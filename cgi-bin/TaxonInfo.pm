package TaxonInfo;

use PBDBUtil;

$DEBUG = 1;

## startTaxonInfo
#
##
sub startTaxonInfo{
	my $html = "<form method=post action=\"/cgi-bin/bridge.pl\">".
		   "<input id=\"action\" type=hidden name=\"action\"".
		   " value=\"checkTaxonInfo\">".
		   "<table width=\"100%\"><tr><td valign=top align=\"middle\">".
		   "<nobr><select name=\"taxon_rank\"><option>Genus".
		   "<option>Genus and species<option>species".
		   "</select></td><td>name:</nobr><td>".
		   "<input name=\"genus_name\" type=\"text\" size=25></td><td>".
		   "<input id=\"submit\" name=\"submit\" value=\"Get info\"".
		   " type=\"submit\"></tr></table></form>";

	# Spit out the HTML
	print main::stdIncludes( "std_page_top" );
	print "<center><h3>Taxon Information</h3></center>";
	print $html;
	print main::stdIncludes("std_page_bottom");
}

## checkStartForm
#
##
sub checkStartForm{
	my $q = shift;
	my $dbh = shift;
	my $s = shift;
	my $dbt = shift;
	my $taxon_type = $q->param("taxon_rank");
	my $taxon_name = $q->param("genus_name");
	my $results = "";
	my $genus = "";
	my $species = "";
	my $sql =""; 
	my @results;

	# if we got here because we had to relogin (old session), we could
	# have a query string that looks like 
	# "action=displayLogin&destination=checkTaxonInfo"
	# which would fall into the 'else' clause below and hammer the server
	# printing a "choose one" page with ALL the genus/species in the db!
	# NOTE: we could also check that we got at least one of genus/species
	# non-empty before running any selects...
	if($taxon_type eq "" or $taxon_name eq ""){
	    print $q->redirect(-url=>$BRIDGE_HOME."?action=beginTaxonInfo");
	    exit;
	}

	# Exact match search for 'Genus and species'
	if($taxon_type eq "Genus and species"){
		($genus,$species) = split(/\s+/,$taxon_name);
		$sql ="select distinct(species_name),genus_name ".
		      "from occurrences ".
		      "where genus_name ='$genus' ".
		      "and species_name = '$species' ".
		      "and species_name!='indet.' ".
		      "and species_name!='sp.'"; 
	
	}
	# 'Genus' or 'species.'  Check for genus and species in either case.
	else{
		# Did we get a genus and species?
		if($taxon_name =~ /\s+/){
			($genus,$species) = split(/\s+/,$taxon_name);
			$sql ="select distinct(species_name),genus_name ".
			      "from occurrences ".
			      "where genus_name like '\%$genus\%' ".
			      "and species_name like '\%$species\%' ".
			      "and species_name!='indet.' ".
			      "and species_name!='sp.'"; 
		}
		# or just a genus or a species?
		else{
			$sql ="select distinct(species_name),genus_name ".
			      "from occurrences ".
			      "where species_name like '\%$taxon_name\%' ".
			      "or genus_name like '\%$taxon_name\%' ".
			      "and species_name!='indet.' ".
			      "and species_name!='sp.'"; 
		}
	}
	@results = @{$dbt->getData($sql)};
	if(@results){
		if(scalar @results < 1 ){
			print main::stdIncludes("std_page_top");
			print "<center><h3>No results found</h3>".
			      "genus: \&lt;<i>$genus</i>\&gt; and ".
			      "species: \&lt;<i>$species</i>\&gt;.";
			print "<br><br><p><b><a href=\"/cgi-bin/bridge.pl?action=".
			      "beginTaxonInfo\">Search Again</a></b></center>";
			print main::stdIncludes("std_page_bottom");
		}
		# if we got just one result, we assume they chose 
		# 'Genus and species' and got an exact match.
		elsif(scalar @results == 1){
			displayTaxonInfoResults($q, $dbh, $s, $dbt);
		}
		# Show 'em their choices (radio buttons for genus-species)
		else{
			print main::stdIncludes( "std_page_top" );
			print "<center><h3>Please select a taxon</h3>";
			print "<form method=post action=\"/cgi-bin/bridge.pl\">".
			      "<input id=\"action\" type=\"hidden\"".
			      " name=\"action\"".
			      " value=\"displayTaxonInfoResults\">".
			      "<input type=\"hidden\" name=\"taxon_rank\" ".
			      "value=\"$taxon_type\">".
			      "<table width=\"100%\">";
			my $newrow = 0;
			foreach $hash (@results){
			    print "<tr>" if($newrow % 3 == 0);
			    print "<td><input type=\"radio\" ".
				  "name=\"genus_name\" value=\"".
			          "$hash->{genus_name} $hash->{species_name}\">".
			    	  "\&nbsp;<i>$hash->{genus_name}\&nbsp;".
			    	  "$hash->{species_name}</i></td>";
			    $newrow++;
			    print "</tr>" if($newrow % 3 == 0);
			}
			print "<tr><td align=\"middle\" colspan=3>";
			print "<input type=\"submit\" value=\"Get taxon info\">";
			print "</td></tr></table></form></center>";

			print main::stdIncludes("std_page_bottom");
		}
	}
}

## displayTaxonInfoResults
#
##
sub displayTaxonInfoResults{
	my $q = shift;
	my $dbh = shift;
	my $s = shift;
	my $dbt = shift;

	require Map;

	print main::stdIncludes("std_page_top");
	print "<center><h2>".$q->param("genus_name")."</h2></center>";

	my $m = Map->new( $dbh, $q, $s );
	$q->param(-name=>"taxon_info_script",-value=>"yes");
	my @map_params = ('projection', 'maptime', 'mapbgcolor', 'gridsize', 'gridcolor', 'coastlinecolor', 'borderlinecolor', 'usalinecolor', 'pointsize', 'pointshape', 'dotcolor', 'dotborder');
	my %user_prefs = main::getPreferences($s->get('enterer'));
	foreach my $pref (@map_params){
		if($user_prefs{$pref}){
			$q->param($pref => $user_prefs{$pref});
		}
	}
	# Not covered by prefs:
	$q->param('mapresolution'=>'medium');
	$q->param('mapscale'=>'X 1');

	# NOTE: ERROR: need to change/remove the "search again" link generated
	# at the bottom of this output.
	print "<table width=\"100%\">".
		  "<tr><td align=\"middle\"><h3>Classification</h3></td><td align=\"middle\"><h3>Collection Map</h3></td></tr>";
	print "<tr><td width=\"40%\" valign=\"top\" align=\"middle\">";

	# These could be either from the above method or via the radio choices,
	# so we have to parse again.
	my ($genus, $species) = split /\s+/, $q->param("genus_name");

	# If the split gave us only one word (in $genus) and it doesn't begin
	# with a capital letter, then we got a species (only).
	unless($species){
		if($genus =~ /^[a-z]/){
			$species = $genus;	
			$genus = "";
		}
	}

	displayTaxonClassification($dbt, $genus, $species);

	print"</td><td width=\"60%\" align=\"middle\">";
	$m->buildMap();
	print "</td></tr></table>";
	print "<hr>";

	# NOTE:  we also have the search results and it'd be more efficient
	# to just process them rather than searching again (as is done below)
	# but I think that's a future project requiring reengineering i
	# (modularization) of the following method.

	# Number of results displayed on page
	# PM: 09/10/02 I have this set to a crazy-high value because I 
	# currently don't know the mechanism for linking to subsequent
	# pages of results.  Perhaps they could be brought up in a frame?
	$q->param(-name=>"limit",-value=>1000000);

	# Get all the data from the database
	@data = @{main::displayCollResults()};	
	#my @sorted = sort {$a->{country} cmp $b->{country}} @data;
	require Collections;

	# Process the data:  group all the collection numbers with the same
	# time-place string together as a hash.
	%time_place_coll;
	foreach my $row (@data){
	    $res = Collections::createTimePlaceString($row);
	    if(exists $time_place_coll{$res}){
			push(@{$time_place_coll{$res}}, $row->{"collection_no"});
	    }
	    else{
			$time_place_coll{$res} = [$row->{"collection_no"}];
			push(@order,$res);
	    }
	}

	my @sorted = sort by_time_place_string (keys %time_place_coll);

	print "<h3>Collections</h3>";
	print "<table width=\"100%\"><tr bgcolor=\"white\">";
	print "<th align=\"middle\">Time - place</th>";
	print "<th align=\"middle\">Collections</th></tr>";
	my $row_color = 0;
	foreach my $key (@sorted){
		if($row_color % 2 == 0){
		    print "<tr bgcolor=\"E0E0E0\">";
		} 
		else{
		    print "<tr bgcolor=\"white\">";
		}
		print "<td align=\"middle\" valign=\"top\">".
			  "<span class=tiny>$key</span></td><td align=\"left\">";
		foreach  my $val (@{$time_place_coll{$key}}){
		    my $link=Collections::createCollectionDetailLink($exec_url,$val,$val);
		    print "$link ";
		}
		print "</td></tr>\n";
		$row_color++;
	}
	print "</table><hr>";
	
	#subroutine to do the synonymy
	displayTaxonSynonymy($dbt, $genus, $species);

	print "<br><br><center><p><b><a href=\"/cgi-bin/bridge.pl?action=".
		  "beginTaxonInfo\">Search Again</a></b></center>";

	print main::stdIncludes("std_page_bottom");
}

sub by_time_place_string{
	$a =~ /.*?-\s*(.*)/;
	my $first = $1;
	$b =~ /.*?-\s*(.*)/;
	my $second = $1;

	return $first cmp $second;
}

## displayTaxonClassification
#
##
sub displayTaxonClassification{
	my $dbt = shift;
	my $genus = shift or "";
	my $species = shift or "";
	my $species_only = 0;
	my $taxon_rank;
	my $taxon_name;

	# The authorities table has "Genus species" for taxon_rank='species', so
	# we set the rank to genus iff we only got a genus. These values 
	# (taxon name and rank) will be used for the initial select into the
	# authorities table, though the passed in genus and species values will
	# be used as given for the classification table (otherwise we often will
	# get genus and species on the same line in the table). 
	if($genus && $species eq ""){
		$taxon_rank = "genus";
		$taxon_name = $genus;
	}
	else{
		$taxon_name = $genus;
		if($genus eq ""){
			$species_only = 1;
			$taxon_name = $species;
		}
		else{
			$taxon_name .= " $species";
		}
		$taxon_rank = "species";
	}

	my %classification = ();
	# Initialize our classification hash with the info
	# that came in as an argument to this method.
	$classification{"species"} = $species;
	$classification{"genus"} = $genus;

	# default to a number that doesn't exist in the database.
	my $child_no = -1;
	my $parent_no = -1;
	my %parent_no_visits = ();
	my %child_no_visits = ();

	my $status = "";
	# Loop at least once, but as long as it takes to get full classification
	while($parent_no){
		my @results = ();
		# We know the taxon_rank and taxon_name, so get its number
		my $sql_auth_inv = "SELECT taxon_no ".
				   "FROM authorities ".
				   "WHERE taxon_rank = '$taxon_rank' ".
				   "AND taxon_name = '$taxon_name'";
	# NOTE: ABOVE: should species also be included in taxon_name???
		PBDBUtil::debug(1,"authorities inv: $sql_auth_inv<br>");
		@results = @{$dbt->getData($sql_auth_inv)};
		if(@results){
			# Keep $child_no at -1 if no results are returned.
			if(defined $results[0]){
				# Save the taxon_no for keying into the opinions table.
				$child_no = $results[0]->{taxon_no};

				# Insurance for self referential / bad data in database.
				# NOTE: can't use the tertiary operator with hashes...
				# How strange...
				if($child_no_visits{$child_no}){
					$child_no_visits{$child_no} += 1; 
				}
				else{
					$child_no_visits{$child_no} = 1;
				}
				PBDBUtil::debug(1,"child_no_visits{$child_no}:$child_no_visits{$child_no}.<br>");
				last if($child_no_visits{$child_no}>1); 

			}
			else{ # no taxon number? no farther!
				last;
			}
		}
		else{ # bad select?
			# No results might not be an error: it might just be lack of data
		    # print "ERROR: no results for $sql_auth_inv<br>";
		    last;
		}
		
		# Now see if the opinions table has a parent for this child
		my $sql_opin =  "SELECT status, parent_no, pubyr, reference_no ".
						"FROM opinions ".
						"WHERE child_no=$child_no";
		PBDBUtil::debug(1,"opinions: $sql_opin<br>");
		@results = @{$dbt->getData($sql_opin)};
		if(scalar @results){
			($status,$parent_no)=selectMostRecentParentOpinion($dbt,\@results);

			# Insurance for self referential or otherwise bad data in database.
			if($parent_no_visits{$parent_no}){
				$parent_no_visits{$parent_no} += 1; 
			}
			else{
				$parent_no_visits{$parent_no}=1;
			}
			PBDBUtil::debug(1,"parent_no_visits{$parent_no}:$parent_no_visits{$parent_no}.<br>");
			last if($parent_no_visits{$parent_no}>1); 

			if($status eq "belongs to" && $parent_no){
				# Get the name and rank for the parent
				my $sql_auth = "SELECT taxon_name, taxon_rank ".
					       "FROM authorities ".
					       "WHERE taxon_no=$parent_no";
				PBDBUtil::debug(1,"authorities: $sql_auth<br>");
				@results = @{$dbt->getData($sql_auth)};
				if(scalar @results){
					$auth_hash_ref = $results[0];
					# reset name and rank for next loop pass
					$taxon_rank = $auth_hash_ref->{"taxon_rank"};
					$taxon_name = $auth_hash_ref->{"taxon_name"};
					print "ADDING $taxon_rank of $taxon_name<br>";
					$classification{$taxon_rank} = $taxon_name;
				}
				else{
					# No results might not be an error: 
					# it might just be lack of data
				    # print "ERROR in sql: $sql_auth<br>";
				    last;
				}
			}
			# If we didn't get a parent or status ne 'belongs to'
			else{
				$parent_no = 0;
			}
		}
		else{
			# No results might not be an error: it might just be lack of data
			# print "ERROR in sql: $sql_opin<br>";
			last;
		}
	}

	print "<table width=\"50%\"><tr><th>Rank</th><th>Name</th></tr>";
	my $counter = 0;
	# Print these out in correct order
	my @taxon_rank_order = ('superkingdom','kingdom','subkingdom','superphylum','phylum','subphylum','superclass','class','superorder','order','suborder','infraorder','superfamily','family','subfamily','tribe','subtribe','genus','subgenus','species','subspecies');
	#foreach my $class (keys %classification){
	foreach my $rank (@taxon_rank_order){
	  if(exists $classification{$rank}){
	    if($counter % 2 == 0){
		print "<tr bgcolor=\"#E0E0E0\"><td align=\"middle\">$rank</td>".
		      "<td align=\"middle\">$classification{$rank}</td></tr>";
	    }
	    else{
		print "<tr><td align=\"middle\">$rank</td>".
		      "<td align=\"middle\">$classification{$rank}</td></tr>";
	    }
	    $counter++;
	  }
	}

	print "</table>";
}

## displayTaxonSynonymy
#
##
sub displayTaxonSynonymy{
	my $dbt = shift;
	my $genus = shift or "";
	my $species = shift or "";
	my $taxon_rank;
	my $taxon_name;
	my $species_only = 0;

	my %synmap = ( 'recombined as' => 'recombined as',
				   'reassigned to' => 'recombined as',
				   'subjective synonym of' => 'synonymized subjectively with',
				   'objective synonym of' => 'synonymized objectively with');

	if($genus && $species eq ""){
		$taxon_rank = "genus";
		$taxon_name = $genus;
	}
	else{
		$taxon_name = $genus;
		if($genus eq ""){
			$species_only = 1;
			$taxon_name = $species;
		}
		else{
			$taxon_name .= " $species";
		}
		$taxon_rank = "species";
	}

	my $sql = "SELECT taxon_no, reference_no, author1last, pubyr, ref_is_authority ".
			  "FROM authorities ".
			  "WHERE taxon_rank='$taxon_rank' AND taxon_name='$taxon_name'";
	my @results = @{$dbt->getData($sql)};
	my $child_no = $results[0]->{taxon_no};
	unless($child_no){
		#print "no taxonomic history found.<br>";
		return 0;
		#exit;
	}

	print "<h3>Taxonomic history</h3>";
	# Do the "was named by" part first:
	print "<ul>";

	# Get ref info from refs if 'ref_is_authority' is set
	if($results[0]->{ref_is_authority} =~ /YES/i){
		PBDBUtil::debug(1,"author and year from refs<br>");
		# If we didn't get an author and pubyr and also didn't get a 
		# reference_no, we're at a wall and can go no further.
		if(!$results[0]->{reference_no}){
			return;	
		}
		$sql = "SELECT author1last, pubyr FROM refs ".
			   "WHERE reference_no=$results[0]->{reference_no}";
		@results = @{$dbt->getData($sql)};
		print "<li><i>$genus $species</i> was named by ".
			  "$results[0]->{author1last} ($results[0]->{pubyr})";
	}
	# If ref_is_authority is not set, use the authorname and pubyr in this
	# record.
	elsif($results[0]->{author1last} && $results[0]->{pubyr}){
		PBDBUtil::debug(1,"author and year from authorities<br>");
		print "<li><i>$genus $species</i> was named by ".
			  "$results[0]->{author1last} ($results[0]->{pubyr})";
	}
	# if there's nothing from above, give up.
	else{
		return 0;
	}

	# Now, go get synonymies for the taxon as child
	my $syn_html = getSynonymyParagraph($dbt, $child_no);
	print "<li>$syn_html" if($syn_html ne "");

	# and for the taxon as parent
	$sql = "SELECT child_no FROM opinions WHERE parent_no=$child_no";
	@results = @{$dbt->getData($sql)};
	foreach my $child (@results){
		my $other_paras = "";
		# Need to print out "[taxon_name] was named by [author] ([pubyr])".
		# - select taxon_name, author1last, pubyr, reference_no from authorities
		$sql = "SELECT taxon_name, author1last, pubyr, reference_no ".
			   "FROM opinions WHERE taxon_no=$child->{child_no}";
		my @auth_rec = @{$dbt->getData($sql)};
		# - if not pubyr and author1last, get same from refs
		unless($auth_rec[0]->{author1last} && $auth_rec[0]->{pubyr}){
			$sql = "SELECT author1last, pubyr ".
				   "FROM refs WHERE reference_no=$auth_rec[0]->{reference_no}";
			my @ref_rec = @{$dbt->getData($sql)};
			$auth_rec[0]->{author1last} = $ref_rec[0]->{author1last};
			$auth_rec[0]->{pubyr} = $ref_rec[0]->{pubyr};
		}
		$other_paras .= "<i>$auth_rec[0]->{taxon_name}</i> was named by ".
						"$auth_rec[0]->{author1last} ($auth_rec[0]->{pubyr})";
		$other_paras .= getSynonymyParagraph($dbt, $child->{child_no});
		print "<li>$other_paras" if($other_paras ne "");
	}

	print "</ul>";
	print "<hr>";
}

##
#
##
sub getSynonymyParagraph{
	my $dbt = shift;
	my $child_no = shift;

	$sql = "SELECT parent_no, status, reference_no, pubyr, author1last ".
		   "FROM opinions WHERE child_no=$child_no";
	@results = @{$dbt->getData($sql)};
	# I can't remember why this is here, commented out...
	#($status, $parent_no) = selectMostRecentParentOpinion($dbt, \@results);
	my %synonymies = ();
	# check for synonymies - status' of anything other than "belongs to"
	foreach my $row (@results){
		if($row->{status} ne "belongs to"){
			# get the proper reference (record first, refs table second)
			if(!$row->{author1last} || !$row->{pubyr}){
				# select into the refs table.
				$sql = "SELECT author1last, pubyr FROM refs ".
					   "WHERE reference_no=$row->{reference_no}";
				my @real_ref = @{$dbt->getData($sql)};
				$row->{author1last} = $real_ref[0]->{author1last};
				$row->{pubyr} = $real_ref[0]->{pubyr};
			}
			# put all syn's referring to the same taxon_name together
			if(exists $synonymies{$row->{parent_no}}){
				push(@{$synonymies{$row->{parent_no}}}, $row);
			}	
			else{
				$synonymies{$row->{parent_no}} = [$row];
			}
		}
	}
	# Now print it all out
	my @syn_keys = keys %synonymies;
	my $syn_html = "";
	for(my $index = 0; $index < @syn_keys; $index++){
		$syn_html .= "; it was $synmap{$synonymies{$syn_keys[$index]}[0]->{status}}";
		$sql = "SELECT taxon_name FROM authorities ".
			   "WHERE taxon_no=$synonymies{$syn_keys[$index]}[0]->{parent_no}";
		@results = @{$dbt->getData($sql)};
		$syn_html .= "<i>$results[0]->{taxon_name}</i> ";
		my @key_list = @{$synonymies{$syn_keys[$index]}};
		for(my $j = 0; $j < @key_list; $j++){
			$syn_html .= "$key_list[$j]->{author1last} ($key_list[$j]->{pubyr}), ";
		}
		# remove the last comma
		$syn_html =~ s/, $//;
		# replace the last comma-space sequence with ' and '
		$syn_html =~ s/(, (.*))$/ and $2/;
		# put a semi-colon on the end to separate from any following syns.
	}
	# remove the last semi-colon.
	$syn_html =~ s/;$//;

	return $syn_html;
}

## selectMostRecentParentOpinion
#
##
sub selectMostRecentParentOpinion{
	my $dbt = shift;
	my $array_ref = shift;
	my @array_of_hash_refs = @{$array_ref};
	
	if(scalar @array_of_hash_refs == 1){
		return ($array_of_hash_refs[0]->{status},
				$array_of_hash_refs[0]->{parent_no});
	}
	elsif(scalar @array_of_hash_refs > 1){
		PBDBUtil::debug(1,"FOUND MORE THAN ONE PARENT<br>");
		# find the most recent opinion
		# Algorithm: For each opinion (parent), if opinions.pubyr exists, 
		# use it.  If not, get the pubyr from the reference and use it.
		# Finally, compare across parents to find the most recent year.

		# Use $years[0] for the index, and $years[1] for the pubyr.
		# Store only the greatest value
		my @years = (0,0);
		for(my $index = 0; $index < @array_of_hash_refs; $index++){
			if($array_of_hash_refs[$index]->{pubyr} &&
					$array_of_hash_refs[$index]->{pubyr} > $years[1]){
				$years[0] = $index;
				$years[1] = $array_of_hash_refs[$index]->{pubyr};
			}
			else{
				# get the year from the refs table
				my $sql = "SELECT pubyr FROM refs WHERE reference_no=".
						  "$array_of_hash_refs[$index]->{reference_no}";
				my @ref_ref = @{$dbt->getData($sql)};
				if($ref_ref[0]->{pubyr} && $ref_ref[0]->{pubyr} > $years[1]){
					$years[0] = $index;
					$years[1] = $ref_ref[0]->{pubyr};
				}
			}	
		}
		return ($array_of_hash_refs[$years[0]]->{"status"},$years[1]);
	}
	else{
		return (undef, undef);
	}
}
