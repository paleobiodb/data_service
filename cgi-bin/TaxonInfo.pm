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
		   "<input id=\"user\" type=hidden name=\"user\"".
		   " value=\"Contributor\">".
		   "<table width=\"100%\"><tr><td valign=top align=\"middle\">".
		   "<nobr><select name=\"taxon_rank\"><option>Higher taxon".
		   "<option>Genus<option>Genus and species<option>species".
		   "</select></td><td align=\"right\">name:</nobr><td>".
		   "<input name=\"taxon_name\" type=\"text\" size=25></td><td>".
		   "<input id=\"submit\" name=\"submit\" value=\"Get info\"".
		   " type=\"submit\"></tr></table></form>";

	# Spit out the HTML
	print main::stdIncludes( "std_page_top" );
	print "<center><h3>Taxon search form</h3></center>";
	print $html;
	print main::stdIncludes("std_page_bottom");
}

## checkStartForm
#
#	Description:	Get the data to the display routine.
##
sub checkStartForm{
	my $q = shift;
	my $dbh = shift;
	my $s = shift;
	my $dbt = shift;
	my $taxon_type = $q->param("taxon_rank");
	my $taxon_name = $q->param("taxon_name");
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
	    print $q->redirect(-url=>$BRIDGE_HOME."?action=beginTaxonInfo&user=Contributor");
	    exit;
	}

	# Higher taxon
	if($taxon_type eq "Higher taxon"){
		$q->param("genus_name" => $taxon_name);
		$sql = "SELECT taxon_name, taxon_no FROM authorities ".
			   "WHERE taxon_name='$taxon_name'";
		@results = @{$dbt->getData($sql)};
		# Check for homonyms
		if(scalar @results > 1){
## SOMETHING WRONG WITH THIS (first of all, check for dupes and don't even
## call it if there aren't any...
			#@results = @{deal_with_homonyms($dbt, \@results)};
		}
		# reset 'taxon_name' key to 'genus_name' for uniformity down the line
		foreach my $ref (@results){
			$ref->{genus_name} = $ref->{taxon_name};
 			delete $ref->{taxon_name};
		}
		# if nothing from authorities, go to occurrencs and reidentifications
		if(scalar @results < 1){
			$q->param("no_classification" => "true");
			$sql = "SELECT DISTINCT(genus_name) FROM occurrences ".
				   "WHERE genus_name like '$taxon_name'";
			@results = @{$dbt->getData($sql)};
			$sql = "SELECT DISTINCT(genus_name) FROM reidentifications ".
				   "WHERE genus_name like '$taxon_name'";
			# collapse duplicates
			@results = @{array_push_unique(\@results, $dbt->getData($sql),
											"genus_name")};
		}
	}
	# Exact match search for 'Genus and species'
	elsif($taxon_type eq "Genus and species"){
		$sql = "SELECT taxon_name, taxon_no FROM authorities ".
			   "WHERE taxon_name = '$taxon_name' AND taxon_rank='species'";
		@results = @{$dbt->getData($sql)};
		# Check for homonyms
		if(scalar @results > 1){
			#@results = @{deal_with_homonyms($dbt, \@results)};
		}
		# reset 'taxon_name' key to 'genus_name' for uniformity
		foreach my $ref (@results){
			my ($genus,$species) = split(/\s+/,$ref->{taxon_name});
			$ref->{genus_name} = $genus;
			$ref->{species_name} = $species;
			delete $ref->{taxon_name};
		}
		if(scalar @results < 1){
			($genus,$species) = split(/\s+/,$taxon_name);
			$sql ="select distinct(species_name),genus_name ".
				  "from occurrences ".
				  "where genus_name ='$genus' ".
				  "and species_name = '$species'";
			@results = @{$dbt->getData($sql)};
			$sql ="select distinct(species_name),genus_name ".
				  "from reidentifications ".
				  "where genus_name ='$genus' ".
				  "and species_name = '$species'";
			@results = @{array_push_unique(\@results, $dbt->getData($sql),
											"genus_name", "species_name")};
		}
	}
	# 'Genus'
	elsif($taxon_type eq "Genus"){
		$sql = "SELECT taxon_name, taxon_no FROM authorities WHERE ".
			   "taxon_name='$taxon_name' AND taxon_rank='Genus'";
		@results = @{$dbt->getData($sql)};
		# Check for homonyms
		if(scalar @results > 1){
			@results = @{deal_with_homonyms($dbt, \@results)};
		}
		# reset 'taxon_name' key to 'genus_name' for uniformity
		foreach my $ref (@results){
			my ($genus,$species) = split(/\s+/,$ref->{taxon_name});
			$ref->{genus_name} = $genus;
			$ref->{species_name} = $species;
			delete $ref->{taxon_name};
		}
		if(scalar @results < 1){
			$sql ="select distinct(genus_name),species_name ".
				  "from occurrences ".
				  "where genus_name = '$taxon_name'";
			@results = @{$dbt->getData($sql)};
			$sql ="select distinct(genus_name),species_name ".
				  "from reidentifications ".
				  "where genus_name = '$taxon_name'";
			@results = @{array_push_unique(\@results, $dbt->getData($sql),
											"genus_name","species_name")};
		}
	}
	# or a species
	else{
		$sql = "SELECT taxon_name, taxon_no FROM authorities WHERE ".
			   "taxon_name like '% $taxon_name' AND taxon_rank='species'";
		@results = @{$dbt->getData($sql)};
		# Check for homonyms
		if(scalar @results > 1){
			#@results = @{deal_with_homonyms($dbt, \@results)};
		}
		# reset 'taxon_name' key to 'genus_name' for uniformity
		foreach my $ref (@results){
			my ($genus,$species) = split(/\s+/,$ref->{taxon_name});
			$ref->{genus_name} = $genus;
			$ref->{species_name} = $species;
			delete $ref->{taxon_name};
		}
		if(scalar @results < 1){
			$sql ="select distinct(species_name),genus_name ".
				  "from occurrences ".
				  "where species_name = '$taxon_name'";
			@results = @{$dbt->getData($sql)};
			$sql ="select distinct(species_name),genus_name ".
				  "from reidentifications ".
				  "where species_name = '$taxon_name'";
			@results = @{array_push_unique(\@results, $dbt->getData($sql),
											"genus_name","species_name")};
		}
	}
	# now deal with results:
	if(scalar @results < 1 ){
		print main::stdIncludes("std_page_top");
		print "<center><h3>No results found</h3>";
		#	  "genus: \&lt;<i>$genus</i>\&gt; and ".
		#	  "species: \&lt;<i>$species</i>\&gt;.";
		print "<br><br><p><b><a href=\"/cgi-bin/bridge.pl?action=".
			  "beginTaxonInfo\">Search Again</a></b></center>";
		print main::stdIncludes("std_page_bottom");
	}
	# if we got just one result, it could be higher taxon, or an exact
	# 'Genus and species' match.
	elsif(scalar @results == 1){
		$q->param("genus_name" => $results[0]->{genus_name}." ".$results[0]->{species_name});
		displayTaxonInfoResults($q, $dbh, $s, $dbt);
	}
	# Show 'em their choices (radio buttons for genus-species)
	else{
		# REWRITE NO_MAP OR NO_CLASSIFICATION AS HIDDENS
		print main::stdIncludes( "std_page_top" );
		print "<center><h3>Please select a taxon</h3>";
		print "<form method=post action=\"/cgi-bin/bridge.pl\">".
			  "<input id=\"action\" type=\"hidden\"".
			  " name=\"action\"".
			  " value=\"displayTaxonInfoResults\">".
			  "<table width=\"100%\">";
		my $newrow = 0;
		foreach $hash (@results){
			print "<tr>" if($newrow % 3 == 0);
			print "<td><input type=\"radio\" name=\"genus_name\" value=\"";
			print $hash->{genus_name}." ". $hash->{species_name};
			if($hash->{clarification_info}){
				print " (".$hash->{taxon_no}.")";
			}
			print "\">&nbsp;<i>".$hash->{genus_name}."\&nbsp;".
				  $hash->{species_name};
			if($hash->{clarification_info}){
				print " (".$hash->{clarification_info}.")";
			}
			print "</i></td>";
			$newrow++;
			print "</tr>" if($newrow % 3 == 0);
		}
		print "<tr><td align=\"middle\" colspan=3>";
		print "<input type=\"submit\" value=\"Get taxon info\">";
		print "</td></tr></table></form></center>";

		print main::stdIncludes("std_page_bottom");
	}
}

## displayTaxonInfoResults
#
#	Description:	by the time we get here, the occs table has been queried.
#					ALSO by the time we're here, we have either a single name
#					higher taxon, or a "Genus species" combination.
#
##
sub displayTaxonInfoResults{
	my $q = shift;
	my $dbh = shift;
	my $s = shift;
	my $dbt = shift;
	my $genus_name = $q->param("genus_name");
	my $taxon_no = 0;

	require Map;

	# Looking for "Genus (23456)" or something like that.	
	$genus_name =~ /(.*?)\((\d+)\)/;
	$taxon_no = $2;

	if($taxon_no){
		$genus_name = $1;
		$genus_name =~ s/\s+$//; # remove trailing spaces.
		$q->param("genus_name" => $genus_name);
	}
	print "genus_name: $genus_name, taxon_no: $taxon_no<br>";

## DEAL WITH param NO_MAP AND param NO_CLASSIFICATION

	print main::stdIncludes("std_page_top");
	print "<center><h2>$genus_name</h2></center>";

	# MAP USES $q->param("genus_name") to determine what it's doing.
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
		  "<tr><td align=\"middle\"><h3>Classification</h3></td><td align=\"middle\"><h3>Distribution</h3></td></tr>";
	print "<tr><td width=\"40%\" valign=\"top\" align=\"middle\">";

	# If $q->param("genus_name") has a space, it's a "Genus species" combo,
	# otherwise it's a "Higher taxon."
	my ($genus, $species) = split /\s+/, $q->param("genus_name");

	displayTaxonClassification($dbt, $genus, $species, $taxon_no);

	print"</td><td width=\"60%\" align=\"middle\">";
	# $m->buildMap();
	# INSTEAD OF THIS CALL DO:
	#  - build a similar method that does all but mapDrawMap, and returns
	#    the number of collections.  Then, if it's greater than 0, we can
	#    call mapDrawMap on the object ourselves.
	print "</td></tr></table>";
	print "<hr>";

	$q->param(-name=>"limit",-value=>1000000);

	# Get all the data from the database
	@data = @{main::displayCollResults()};	

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

	print "<center><h3>Collections</h3></center>";
	print "<table width=\"100%\"><tr bgcolor=\"white\">";
	print "<th align=\"middle\">Time - place</th>";
	print "<th align=\"middle\">PBDB collection number</th></tr>";
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

	print "<p><center><p><b><a href=\"/cgi-bin/bridge.pl?action=".
		  "beginTaxonInfo\">Get info on another taxon</a></b></p></center>";

	print main::stdIncludes("std_page_bottom");
}

##
#
#
##
sub by_time_place_string{
	$a =~ /.*?-\s*(.*)/;
	my $first = $1;
	$b =~ /.*?-\s*(.*)/;
	my $second = $1;

	return $first cmp $second;
}

## displayTaxonClassification
#
# SEND IN GENUS OR HIGHER TO GENUS_NAME, ONLY SET SPECIES IF THERE'S A SPECIES.
##
sub displayTaxonClassification{
	my $dbt = shift;
	my $genus = (shift or "");
	my $species = (shift or "");
	my $easy_number = (shift or "");
	my $taxon_rank;
	my $taxon_name;
	my %classification = ();

	# The authorities table has "Genus species" for taxon_rank='species', so
	# we set the rank to genus iff we only got a genus. These values 
	# (taxon name and rank) will be used for the initial select into the
	# authorities table, though the passed in genus and species values will
	# be used as given for the classification table (otherwise we often will
	# get genus and species on the same line in the table). 
	if($genus && $species eq ""){
		$taxon_name = $genus;
		# Initialize the classification hash:
		my $sql="SELECT taxon_rank FROM authorities WHERE taxon_name='$genus'";
		my @results = @{$dbt->getData($sql)};
		$taxon_rank = $results[0]->{taxon_rank};
		$classification{$results[0]->{taxon_rank}} = $genus;
	}
	else{
		$taxon_name = $genus;
		$taxon_name .= " $species";
		$taxon_rank = "species";
		# Initialize our classification hash with the info
		# that came in as an argument to this method.
		$classification{"species"} = $species;
		$classification{"genus"} = $genus;
	}

	# default to a number that doesn't exist in the database.
	my $child_no = -1;
	my $parent_no = -1;
	my %parent_no_visits = ();
	my %child_no_visits = ();

	my $status = "";
	my $first_time = 1;
	# Loop at least once, but as long as it takes to get full classification
	while($parent_no){
		my @results = ();
		# We know the taxon_rank and taxon_name, so get its number
		unless($easy_number){
			my $sql_auth_inv = "SELECT taxon_no ".
					   "FROM authorities ".
					   "WHERE taxon_rank = '$taxon_rank' ".
					   "AND taxon_name = '$taxon_name'";
			PBDBUtil::debug(1,"authorities inv: $sql_auth_inv<br>");
			@results = @{$dbt->getData($sql_auth_inv)};
		}
		else{
			$results[0] = {"taxon_no" => $easy_number};
			# reset to zero so we don't try to use this on successive loops.
			$easy_number = 0;
		}
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
		# no taxon number: if we're doing "Genus species", try to find a parent
		# for just the Genus, otherwise give up.
		else{ 
			if($genus && $species){
				$sql_auth_inv = "SELECT taxon_no ".
                   "FROM authorities ".
                   "WHERE taxon_rank = 'Genus' ".
                   "AND taxon_name = '$genus'";
				@results = @{$dbt->getData($sql_auth_inv)};
				# THIS IS LOOKING IDENTICAL TO ABOVE...
				# COULD CALL SELF WITH EMPTY SPECIES NAME AND AN EXIT...
				if(defined $results[0]){
					$child_no = $results[0]->{taxon_no};

					if($child_no_visits{$child_no}){
						$child_no_visits{$child_no} += 1; 
					}
					else{
						$child_no_visits{$child_no} = 1;
					}
					PBDBUtil::debug(1,"child_no_visits{$child_no}:$child_no_visits{$child_no}.<br>");
					last if($child_no_visits{$child_no}>1); 
				}
			}
			else{
				last;
			}
		}
		
		# Now see if the opinions table has a parent for this child
		my $sql_opin =  "SELECT status, parent_no, pubyr, reference_no ".
						"FROM opinions ".
						"WHERE child_no=$child_no AND status='belongs to'";
		PBDBUtil::debug(1,"opinions: $sql_opin<br>");
		@results = @{$dbt->getData($sql_opin)};

		if($first_time && $taxon_rank eq "species" && scalar @results < 1){
			my ($genus, $species) = split(/\s+/,$taxon_name);
            my $last_ditch_sql = "SELECT taxon_no ".
							     "FROM authorities ".
							     "WHERE taxon_rank = 'Genus' ".
							     "AND taxon_name = '$genus'";
            @results = @{$dbt->getData($last_ditch_sql)};
			my $child_no = $results[0]->{taxon_no};
			if($child_no > 0){
				$last_ditch_sql = "SELECT status, parent_no, pubyr, ".
								  "reference_no FROM opinions ".
								  "WHERE child_no=$child_no AND ".
								  "status='belongs to'";
				@results = @{$dbt->getData($last_ditch_sql)};
			}
		}
		$first_time = 0;

		if(scalar @results){
			$parent_no=selectMostRecentParentOpinion($dbt,\@results);

			# Insurance for self referential or otherwise bad data in database.
			if($parent_no_visits{$parent_no}){
				$parent_no_visits{$parent_no} += 1; 
			}
			else{
				$parent_no_visits{$parent_no}=1;
			}
			PBDBUtil::debug(1,"parent_no_visits{$parent_no}:$parent_no_visits{$parent_no}.<br>");
			last if($parent_no_visits{$parent_no}>1); 

			if($parent_no){
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
					#print "ADDING $taxon_rank of $taxon_name<br>";
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

	if($genus && $species eq ""){
		$taxon_name = $genus;
		# Initialize the classification hash:
		my $sql="SELECT taxon_rank FROM authorities WHERE taxon_name='$genus'";
		my @results = @{$dbt->getData($sql)};
		$taxon_rank = $results[0]->{taxon_rank};
	}
	else{
		$taxon_name = $genus;
		if($genus eq ""){
			$taxon_name = $species;
		}
		else{
			$taxon_name .= " $species";
		}
		$taxon_rank = "species";
	}

	my $sql = "SELECT taxon_no, reference_no, author1last, pubyr, ".
			  "ref_is_authority FROM authorities ".
			  "WHERE taxon_rank='$taxon_rank' AND taxon_name='$taxon_name'";
	my @results = @{$dbt->getData($sql)};
	my $child_no = $results[0]->{taxon_no};
	PBDBUtil::debug(1,"taxon rank: $taxon_rank");
	PBDBUtil::debug(1,"taxon name: $taxon_name");
	PBDBUtil::debug(1,"taxon number from authorities: $child_no");
	unless($child_no){
		print "<i>No taxonomic history is available for $taxon_rank $taxon_name.</i><br>";
		return 0;
	}

	print "<center><h3>Taxonomic history</h3></center>";
	# Do the "was named by" part first:
	print "<ul>";

	# Get ref info from refs if 'ref_is_authority' is set
	if($results[0]->{ref_is_authority} =~ /YES/i){
		PBDBUtil::debug(1,"author and year from refs<br>");
		# If we didn't get an author and pubyr and also didn't get a 
		# reference_no, we're at a wall and can go no further.
		if(!$results[0]->{reference_no}){
			print "Cannot determine taxonomic history for $taxon_name<br>.";
			return;	
		}
		$sql = "SELECT author1last, pubyr FROM refs ".
			   "WHERE reference_no=".$results[0]->{reference_no};
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
	# if there's nothing from above, give up or try the reference?
	else{
		print "</ul>";
		print "<i>No taxonomic history is available for $taxon_rank $taxon_name.</i><br>";
		return 0;
	}

	# Now, go get synonymies for the taxon as child
	my $syn_html = getSynonymyParagraph($dbt, $child_no);
	print "<br><br><li>$syn_html" if($syn_html ne "");

	# ... and for the taxon as parent
	# We only want unique results: (The db has 8 records of parent 46303 with
	# child 50471 as a 'subjective synonym of'.
	$sql = "SELECT DISTINCT(child_no), status FROM opinions ".
		   "WHERE parent_no=$child_no";
	@results = @{$dbt->getData($sql)};
	foreach my $child (@results){
		my $other_paras = "";
		# Need to print out "[taxon_name] was named by [author] ([pubyr])".
		# - select taxon_name, author1last, pubyr, reference_no from authorities
		$sql = "SELECT taxon_name, author1last, pubyr, reference_no ".
			   "FROM authorities WHERE taxon_no=".$child->{child_no};
		my @auth_rec = @{$dbt->getData($sql)};
		# - if not pubyr and author1last, get same from refs
		unless($auth_rec[0]->{author1last} && $auth_rec[0]->{pubyr}){
			$sql = "SELECT author1last, pubyr ".
				   "FROM refs WHERE reference_no=".$auth_rec[0]->{reference_no};
			my @ref_rec = @{$dbt->getData($sql)};
			$auth_rec[0]->{author1last} = $ref_rec[0]->{author1last};
			$auth_rec[0]->{pubyr} = $ref_rec[0]->{pubyr};
		}
		$other_paras .= "<i>".$auth_rec[0]->{taxon_name}."</i> was named by ".
						$auth_rec[0]->{author1last}." (".
						$auth_rec[0]->{pubyr}."). ";
		$other_paras .= getSynonymyParagraph($dbt, $child->{child_no});
		print "<br><br><li>$other_paras" if($other_paras ne "");
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
	my %synmap = ( 'recombined as' => 'recombined as ',
				   'reassigned to' => 'recombined as ',
				   'subjective synonym of' => 'synonymized subjectively with ',
				   'objective synonym of' => 'synonymized objectively with ');

	# NOTE / QUESTION:  WE'RE NOT DOING ANYTHING WITH 'nomen *', RIGHT?????????
	$sql = "SELECT parent_no, status, reference_no, pubyr, author1last ".
		   "FROM opinions WHERE child_no=$child_no AND status != 'belongs to'".
		   " AND status NOT LIKE 'nomen%'";
	@results = @{$dbt->getData($sql)};
	# I can't remember why this is here, commented out...
	#$parent_no = selectMostRecentParentOpinion($dbt, \@results);
	my %synonymies = ();
	# check for synonymies - status' of anything other than "belongs to"
	foreach my $row (@results){
		# get the proper reference (record first, refs table second)
		if(!$row->{author1last} || !$row->{pubyr}){
			# select into the refs table.
			$sql = "SELECT author1last, pubyr FROM refs ".
				   "WHERE reference_no=$row->{reference_no}";
			my @real_ref = @{$dbt->getData($sql)};
			$row->{author1last} = $real_ref[0]->{author1last};
			$row->{pubyr} = $real_ref[0]->{pubyr};
		}
	# NOTE/QUESTION: IS IT TRUE THAT ALL RECORDS THAT HAVE THIS CHILD [status]ED
	# TO THE SAME PARENT_NO *NECESSARILY* HAVE THE SAME STATUS????????
		# put all syn's referring to the same taxon_name together
		if(exists $synonymies{$row->{parent_no}}){
			push(@{$synonymies{$row->{parent_no}}}, $row);
		}	
		else{
			$synonymies{$row->{parent_no}} = [$row];
		}
	}
	# Now print it all out
	my @syn_keys = keys %synonymies;
	my $syn_html = "";
	# Loop through unique parent numbers from the opinions table.
	# Each parent number is a hash key whose value is an array ref of records.
	for(my $index = 0; $index < @syn_keys; $index++){
		# $syn_keys[$index] is a parent_no, so $synonymies{$syn_keys[$index]
		# is a record from the immediately preceeding 'opinions' select.
		$syn_html .= "; it was ".$synmap{$synonymies{$syn_keys[$index]}[0]->{status}};
		$sql = "SELECT taxon_name FROM authorities ".
			   "WHERE taxon_no=".
			   $syn_keys[$index];
		@results = @{$dbt->getData($sql)};
		$syn_html .= "<i>".$results[0]->{taxon_name}."</i> by ";
		# Dereference the hash value (array ref of opinions recs), so we can
		# write out all of the authors/years for this synonymy.
		my @key_list = @{$synonymies{$syn_keys[$index]}};
		# sort the list by date (ascending)
		@key_list = sort {$a->{pubyr} <=> $b->{pubyr}} @key_list;
		for(my $j = 0; $j < @key_list; $j++){
			$syn_html .= $key_list[$j]->{author1last}.
						 " (".$key_list[$j]->{pubyr}."), ";
		}
		if($syn_html =~ /,\s+$/){
			# remove the last comma
			$syn_html =~ s/,\s+$//;
			# replace the last comma-space sequence with ' and '
			$syn_html =~ s/(, ([\w_']+\s\(\d{4}\)))$/ and $2/;
			# put a semi-colon on the end to separate from any following syns.
		}
	}
	# Add a period at the end.
	$syn_html .= '.';
	# remove a leading semi-colon and any space.
	$syn_html =~ s/^;\s+//;
	# capitalize the first 'I' in the first word (It).
	$syn_html =~ s/^i/I/;

	return $syn_html;
}

## selectMostRecentParentOpinion
#
##
sub selectMostRecentParentOpinion{
	my $dbt = shift;
	my $array_ref = shift;
	my $return_index = (shift or 0); # return index in array (or parent_no)
	my @array_of_hash_refs = @{$array_ref};
	
	if(scalar @array_of_hash_refs == 1){
		if($return_index == 1){
			return 0;
		}
		else{
			return $array_of_hash_refs[0]->{parent_no};
		}
	}
	elsif(scalar @array_of_hash_refs > 1){
		PBDBUtil::debug(1,"FOUND MORE THAN ONE PARENT<br>");
		# find the most recent opinion
		# Algorithm: For each opinion (parent), if opinions.pubyr exists, 
		# use it.  If not, get the pubyr from the reference and use it.
		# Finally, compare across parents to find the most recent year.

		# Store only the greatest value in $years
		my $years = 0;
		my $index_winner = -1;
		for(my $index = 0; $index < @array_of_hash_refs; $index++){
			if($array_of_hash_refs[$index]->{pubyr} &&
					$array_of_hash_refs[$index]->{pubyr} > $years){
				$years = $array_of_hash_refs[$index]->{pubyr};
				$index_winner = $index;
			}
			else{
				# get the year from the refs table
				my $sql = "SELECT pubyr FROM refs WHERE reference_no=".
						  $array_of_hash_refs[$index]->{reference_no};
				my @ref_ref = @{$dbt->getData($sql)};
				if($ref_ref[0]->{pubyr} && $ref_ref[0]->{pubyr} > $years){
					$years = $ref_ref[0]->{pubyr};
					$index_winner = $index;
				}
			}	
		}
		if($return_index == 1){
			return $index_winner;
		}
		else{
			return $array_of_hash_refs[$index_winner]->{parent_no};
		}
	}
	# nothing was passed to us in the first place, return the favor.
	else{
		return undef;
	}
}

## Deal with homonyms
sub deal_with_homonyms{
	my $dbt = shift;
	my $array_ref = shift;
	my @array_of_hash_refs = @{$array_ref};

	if(scalar @array_of_hash_refs == 1){
		return $array_ref;
	}
	else{
		# for each child, find its parent
		foreach my $ref (@array_of_hash_refs){
			my $sql = "SELECT parent_no, status, pubyr, reference_no ".
					  "FROM opinions WHERE child_no=".$ref->{taxon_no}.
					  " AND status='belongs to'";	
			my @ref_ref = @{$dbt->getData($sql)};
			# if it has no parent, find the child that's either 
			# "corrected as", "recombined as" or "replaced by" and use 
			# that as the focal taxon.
			if(scalar @ref_ref < 1){
				$sql = "SELECT child_no FROM opinions WHERE parent_no=".
					   $ref->{taxon_no}." AND status IN ('recombined as',".
					   "'replaced by','corrected as')";
				@ref_ref = @{$dbt->getData($sql)};
				# try above again:
				$sql = "SELECT parent_no, status, pubyr, reference_no ".
					   "FROM opinions WHERE child_no=".$ref_ref[0]->{child_no};	
				@ref_ref = @{$dbt->getData($sql)};
			}
			# get the most recent parent
			#	if it's not "belongs to", get most recent grandparent, etc.
			#	until we get a "belongs to."
			my $index = selectMostRecentParentOpinion($dbt, \@ref_ref, 1);
			while($ref_ref[$index]->{status} ne "belongs to"){
				my $child_no = $ref_ref[$index]->{parent_no};
				# HIT UP THE NEXT GENERATION OF PARENTS:
				#	-start over with the first select in this method.
				$sql = "SELECT parent_no, status, pubyr, reference_no ".
					   "FROM opinions WHERE child_no=$child_no";	
				@ref_ref = @{$dbt->getData($sql)};
				$index = selectMostRecentParentOpinion($dbt, \@ref_ref, 1);
			}
			# Then, add another key to the hash for "clarification_info"
			$sql = "SELECT taxon_name FROM authorities WHERE taxon_no=".
				   $ref_ref[$index]->{parent_no};
			@ref_ref = @{$dbt->getData($sql)};
			$ref->{clarification_info} = $ref_ref[0]->{taxon_name};
		}
	}
	return \@array_of_hash_refs;
}


## Add items from new array to old array iff they are not duplicates
sub array_push_unique{
	my $orig_ref = shift;
	my $new_ref = shift;
	my $field = shift;
	my $field2 = shift;
	my @orig = @{$orig_ref};
	my @new = @{$new_ref};
	my $duplicate = 0;

	foreach my $item (@new){
		my $duplicate = 0;
		foreach my $old (@orig){
			if($field2){
				if($item->{$field} eq $old->{$field} && 
						$item->{$field2} eq $old->{$field2}){
					$duplicate = 1;
					last;
				}
			}
			elsif($item->{$field} eq $old->{$field}){
				$duplicate = 1;
				last;
			}
		}
		unless($duplicate){
			push(@orig, $old);
		}
	}
	return \@orig;
}
