package TaxonInfo;

use PBDBUtil;

$DEBUG = 0;

## startTaxonInfo
#
##
sub startTaxonInfo{
	my $q = shift;

	print main::stdIncludes( "std_page_top" );
	print searchForm($q);
	print main::stdIncludes("std_page_bottom");
}

## searchForm
#
##
sub searchForm{
	my $q = shift;
	my $search_again = (shift or 0);

	my $html = "";

	unless($search_again){
		$html .= "<center><h3>Taxon search form</h3></center>";
	}

	$html .= "\n<script language=\"Javascript\">\n".
			 "function checkInput(){\n".
			 "	var rank = document.forms[0].taxon_rank.value;\n".
			 "	var value = document.forms[0].taxon_name.value;\n".
			 "	if(value == \"\"){\n".
			 "		alert('Please enter a taxon name.');\n".
			 "		return false;\n".
			 "	}\n".
			 "	if(value.match(/[A-Za-z]+\\s+[A-Za-z]+/) && rank != 'Genus and species'){\n".
			 "		alert('Please choose rank \"Genus and species\" for this search');\n".
			 "		return false;\n".
			 "	}\n".
			 "	if(rank == 'Genus and species' && value.match(/^[A-Z]{1}[a-z]+\\s+[a-z]+\$/) == null){\n".
			 "		alert('Please enter a name in the form \"Genus species\"');\n".
			 "		return false;\n".
			 "	}\n".
			 "	else{\n".
			 "		return true;\n".
			 "	}\n".
			 "}\n".
			 "</script>\n";
	$html .= "<form method=post action=\"/cgi-bin/bridge.pl\" ".
			 "onSubmit=\"return checkInput();\">".
		   "<input id=\"action\" type=hidden name=\"action\"".
		   " value=\"checkTaxonInfo\">".
		   "<input id=\"user\" type=hidden name=\"user\"".
		   " value=\"".$q->param("user")."\">".
		   "<center><table width=\"75%\">".
		   "<tr><td valign=\"middle\" align=\"right\" width=\"5%\">".
		   "<nobr><select name=\"taxon_rank\"><option>Higher taxon".
		   "<option selected>Genus<option>Genus and species<option>species".
		   "</select></td>".
		   "<td valign=\"middle\" align=\"left\" width=\"5%\">name:</td>".
		   "<td width=\"5%\"><input name=\"taxon_name\" type=\"text\" size=25>".
		   "</td>".
		   "<td align=\"left\" width=\"5%\">".
		   "<input id=\"submit\" name=\"submit\" value=\"Get info\"".
		   " type=\"submit\"></td></tr></table></center></form>";

	return $html;
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
	$taxon_type =~ s/\+/ /g;
	if($taxon_type ne "Genus" && $taxon_type ne "Genus and species" && 
			$taxon_type ne "species"){
		$taxon_type = "Higher taxon";
	}
	my $taxon_name = $q->param("taxon_name");
	$taxon_name =~ s/\+/ /g;
	my $results = "";
	my $genus = "";
	my $species = "";
	my $sql =""; 
	my @results;


	# If they gave us nothing, start again.
	if($taxon_type eq "" or $taxon_name eq ""){
	    print $q->redirect(-url=>$BRIDGE_HOME."?action=beginTaxonInfo");
	    exit;
	}

	# Higher taxon
	if($taxon_type eq "Higher taxon"){
		$q->param("genus_name" => $taxon_name);
		$sql = "SELECT taxon_name, taxon_no, pubyr, author1last, ".
			   "ref_is_authority, reference_no FROM authorities ".
			   "WHERE taxon_name='$taxon_name'";
		@results = @{$dbt->getData($sql)};
		# Check for homonyms
		if(scalar @results > 1){
			@results = @{deal_with_homonyms($dbt, \@results)};
		}
		# reset 'taxon_name' key to 'genus_name' for uniformity down the line
		foreach my $ref (@results){
			$ref->{genus_name} = $ref->{taxon_name};
 			delete $ref->{taxon_name};
		}
		# if nothing from authorities, go to occurrences and reidentifications
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
		$sql = "SELECT taxon_name, taxon_no, pubyr, author1last, ".
			   "ref_is_authority, reference_no FROM authorities ".
			   "WHERE taxon_name = '$taxon_name' AND taxon_rank='species'";
		@results = @{$dbt->getData($sql)};
		# NOTE: this may not be necessary, but we'll leave it in for now.
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
		$sql = "SELECT taxon_name, taxon_no, pubyr, author1last, ".
			   "ref_is_authority, reference_no FROM authorities WHERE ".
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
		$sql = "SELECT taxon_name, taxon_no, pubyr, author1last, ".
			   "ref_is_authority, reference_no FROM authorities WHERE ".
			   "taxon_name like '% $taxon_name' AND taxon_rank='species'";
		@results = @{$dbt->getData($sql)};
		# DON'T NEED TO DO THIS WITH SPECIES, METHINKS...
		# Check for homonyms
		#if(scalar @results > 1){
		#	@results = @{deal_with_homonyms($dbt, \@results)};
		#}
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
		print "<p><b>Please search again</b></center>";
		print searchForm($q, 1); # param for not printing header with form
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
			  "<input name=\"taxon_rank\" type=\"hidden\" value=\"$taxon_type\">".
			  "<table width=\"100%\">";
		my $newrow = 0;
		my $choices = @results;
		my $NUMCOLS = 3;
		my $numrows = int($choices / $NUMCOLS);
		if($numrows == 0){
			$numrows = 1;
		}
		elsif($choices % $NUMCOLS > 0){
			$numrows++;
		}
		for(my $index=0; $index<$numrows; $index++){
			print "<tr>";
			for(my $counter=0; $counter<$NUMCOLS; $counter++){
				last if($index+($counter*$numrows) >= @results);
				print "<td><input type=\"radio\" name=\"genus_name\" value=\"";
				print $results[$index+($counter*$numrows)]->{genus_name}.
					  " ". $results[$index+($counter*$numrows)]->{species_name};
				if($results[$index+($counter*$numrows)]->{clarification_info}){
					print " (".$results[$index+($counter*$numrows)]->{taxon_no}.")";
				}
				print "\"><i>&nbsp;".$results[$index+($counter*$numrows)]->{genus_name}."&nbsp;".
					  $results[$index+($counter*$numrows)]->{species_name};
				if($results[$index+($counter*$numrows)]->{clarification_info}){
					print " </i><small>".$results[$index+($counter*$numrows)]->{clarification_info}."</small></td>";
				}
				else{
					print "</i></td>";
				}
			}
			print "</tr>";
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
	my $taxon_type = $q->param("taxon_rank");
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
	else{
		# just do this
		$genus_name =~ s/\s+$//; # remove trailing spaces.
	}

	# Keep track of entered name for link at bottom of page
	my $entered_name = $genus_name;
	if(!$taxon_no){
		my $sql = "SELECT taxon_no FROM authorities WHERE taxon_name='".
				  $entered_name."'";
		$taxon_no = ${$dbt->getData($sql)}[0]->{taxon_no};
		# urlencode name in case we got a 'Genus species' combo
		$entered_name =~ s/\s+/ /g;
	}
	my $entered_no = $taxon_no;

	# Verify taxon:  If what was entered's most recent parent is a "belongs to"
	# or a "nomen *" relationship, then do the display page on what was entered.
	# If any other relationship exists for the most recent parent, display info 
	# on that parent.  
	my @verified = verify_chosen_taxon($genus_name, $taxon_no, $dbt);
	$genus_name = $verified[0];
	$q->param("genus_name" => $genus_name);
	$taxon_no = $verified[1];
	
	# Get the sql IN list for a Higher taxon:
	my $in_list = "";
	if($taxon_type eq "Higher taxon"){
		$in_list = PBDBUtil::taxonomic_search($q->param('genus_name'), $dbt, $taxon_no);
	}

	print main::stdIncludes("std_page_top");
	print "<center><h2>$genus_name</h2></center>";

	$q->param(-name=>"taxon_info_script",-value=>"yes");
	my @map_params = ('projection', 'maptime', 'mapbgcolor', 'gridsize', 'gridcolor', 'coastlinecolor', 'borderlinecolor', 'usalinecolor', 'pointshape', 'dotcolor', 'dotborder');
	my %user_prefs = main::getPreferences($s->get('enterer'));
	foreach my $pref (@map_params){
		if($user_prefs{$pref}){
			$q->param($pref => $user_prefs{$pref});
		}
	}
	# Not covered by prefs:
	if(!$q->param('pointshape')){
		$q->param('pointshape' => 'circles');
	}
	if(!$q->param('dotcolor')){
		$q->param('dotcolor' => 'red');
	}
	if(!$q->param('coastlinecolor')){
		$q->param('coastlinecolor' => 'black');
	}
	$q->param('mapresolution'=>'medium');
	$q->param('mapscale'=>'X 1');
	$q->param('pointsize'=>'tiny');
	if(!$q->param('projection') or $q->param('projection') eq ""){
		$q->param('projection'=>'rectilinear');
	}

	print "<table width=\"100%\">".
		  "<tr><td align=\"middle\"><h3>Classification</h3></td><td align=\"middle\"><h3>Distribution</h3></td></tr>";
	print "<tr><td width=\"40%\" valign=\"top\" align=\"middle\">";

	# If $q->param("genus_name") has a space, it's a "Genus species" combo,
	# otherwise it's a "Higher taxon."
	my ($genus, $species) = split /\s+/, $q->param("genus_name");

	displayTaxonClassification($dbt, $genus, $species, $taxon_no);

	print"</td><td width=\"60%\" align=\"middle\" valign=\"top\">";
	# MAP USES $q->param("genus_name") to determine what it's doing.
	my $m = Map->new( $dbh, $q, $s, $dbt );
	my $perm_rows = $m->buildMapOnly($in_list);
	my @perm_rows = @{$perm_rows};
	if(@perm_rows > 0){
		$m->mapDrawMap($perm_rows);
	}
	else{
		print "<i>No distribution data available.</i>";
	}
	print "</td></tr></table>";
	print "<hr>";

	$q->param(-name=>"limit",-value=>1000000);

	# Get all the data from the database
	@data = @{main::displayCollResults($in_list)};	

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

	if(scalar @sorted > 0){
		print "<center><h3>Collections</h3></center>";
		print "<table width=\"100%\"><tr bgcolor=\"white\">";
		print "<th align=\"middle\">Time - place</th>";
		print "<th align=\"left\">PBDB collection number</th></tr>";
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
	}
	
	#subroutine to do the synonymy
	displayTaxonSynonymy($dbt, $genus, $species);

	# Entered Taxon
	print "<p><center><p><b><a href=\"/cgi-bin/bridge.pl?action=".
		  "startTaxonomy&taxon_name=$entered_name";
	if($entered_no){
		  print "&taxon_no=$entered_no";
	}
	print "\">Edit taxonomic data for $entered_name</a>".
		  "</b></p></center>\n";
	
	unless($entered_name eq $genus_name){
		# Verified Taxon
		print "<p><center><p><b><a href=\"/cgi-bin/bridge.pl?action=".
			  "startTaxonomy&taxon_name=$genus_name";
		if($taxon_no){
			  print "&taxon_no=$taxon_no";
		}
		print "\">Edit taxonomic data for $genus_name</a>".
			  "</b></p></center>\n";
	}

	print "<p><center><p><b><a href=\"/cgi-bin/bridge.pl?action=".
		  "beginTaxonInfo\">Get info on another taxon</a>".
		  "</b></p></center>\n";

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

	# if we didn't get a species, figure out what rank we got
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
					   "WHERE taxon_name = '$taxon_name' ".
					   "AND taxon_rank = '$taxon_rank'";
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
                   "WHERE taxon_name = '$genus' ".
                   "AND taxon_rank = 'Genus'";
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
							     "WHERE taxon_name = '$genus' ".
							     "AND taxon_rank = 'Genus'";
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
	my @taxon_rank_order = ('superkingdom','kingdom','subkingdom','superphylum','phylum','subphylum','superclass','class','subclass','infraclass','superorder','order','suborder','infraorder','superfamily','family','subfamily','tribe','subtribe','genus','subgenus','species','subspecies');
	my %taxon_rank_order = ('superkingdom'=>0,'kingdom'=>1,'subkingdom'=>2,'superphylum'=>3,'phylum'=>4,'subphylum'=>5,'superclass'=>6,'class'=>7,'subclass'=>8,'infraclass'=>9,'superorder'=>10,'order'=>11,'suborder'=>12,'infraorder'=>13,'superfamily'=>14,'family'=>15,'subfamily'=>16,'tribe'=>17,'subtribe'=>18,'genus'=>19,'subgenus'=>20,'species'=>21,'subspecies'=>22);
	my $lastgood;
	my $lastrank;
	foreach my $rank (@taxon_rank_order){
		# Don't provide links for any rank higher than 'order'
		if(exists $classification{$rank}){
		  if($taxon_rank_order{$rank} < 11){
			if($counter % 2 == 0){
				print "<tr bgcolor=\"E0E0E0\"><td align=\"middle\">$rank</td>".
					  "<td align=\"middle\">$classification{$rank}</td></tr>\n";
			}
			else{
				print "<tr><td align=\"middle\">$rank</td>".
					  "<td align=\"middle\">$classification{$rank}</td></tr>\n";
			}
		  }
		  else{
			# URL encoding
			my $temp_rank = $rank;
			if($temp_rank eq "genus"){
				$temp_rank = "Genus";
			}
			if($temp_rank ne "Genus" && $temp_rank ne "Genus and species" &&
					$temp_rank ne "species"){
				$temp_rank = "Higher taxon";
			}
			$temp_rank =~ s/\s/+/g;
			$classification{$rank} =~ s/\s/+/g;
			if($counter % 2 == 0){
				print "<tr bgcolor=\"E0E0E0\"><td align=\"middle\">$rank</td>".
					  "<td align=\"middle\"><a href=\"/cgi-bin/bridge.pl?".
					  "action=checkTaxonInfo&taxon_rank=$temp_rank&taxon_name=".
					  "$classification{$rank}\">".
					  "$classification{$rank}</a></td></tr>\n";
			}
			else{
				print "<tr><td align=\"middle\">$rank</td>".
					  "<td align=\"middle\"><a href=\"/cgi-bin/bridge.pl?".
					  "action=checkTaxonInfo&taxon_rank=$temp_rank&taxon_name=".
					  "$classification{$rank}\">".
					  "$classification{$rank}</a></td></tr>\n";
			}
		  }
			$counter++;
			# Keep track of the last successful item  and rank so we have the 
			# lowest on the chain when we're all done.
			$lastgood = $classification{$rank};
			$lastrank = $rank;
		}
	}
	print "</table>";
	# Now, print out a hyperlinked list of all taxa below the one at the
	# bottom of the Classification section.
	if($counter <1){
		print "<i>No classification data available for this taxon</i><br>";
	}
	my $index;
	for($index=0; $index<@taxon_rank_order; $index++){
		last if($lastrank eq $taxon_rank_order[$index]);
	}
	# NOTE: Don't do this if the last rank was 'species.'
	CHILDREN:{
	if($index < 21){ # species is position 21
		#$lastrank = $taxon_rank_order[$index+1];
		#if($lastrank eq "genus"){
		#	$lastrank = "Genus";
		#}
		# genus is position 19.
		#elsif($index < 19){
		#	$lastrank = "Higher taxon";
		#}
		my $sql = "SELECT taxon_no FROM authorities ".
				  "WHERE taxon_name='$lastgood'";
		PBDBUtil::debug(1,"lastgood sql: $sql");
		my @quickie = @{$dbt->getData($sql)};
		if(scalar @quickie < 1){
			# BAIL
			last CHILDREN;
		}
		$sql = "SELECT DISTINCT(child_no),taxon_name, taxon_rank ".
			   "FROM opinions,authorities ".
			   "WHERE parent_no=".$quickie[0]->{taxon_no}.
			   " AND status='belongs to' AND child_no=taxon_no ".
			   "ORDER BY taxon_name";
		PBDBUtil::debug(1,"children sql: $sql");
		@quickie = @{$dbt->getData($sql)};
		if(scalar @quickie < 1){
			# BAIL
			last CHILDREN;
		}
		print "<p><i>This taxon includes:</i> ";
		my $output="";
		foreach my $item (@quickie){
			# Need to do some URL encoding:
			my $taxon_name = $item->{taxon_name};
			$taxon_name =~ s/\s/+/g;
			my $taxon_rank = $item->{taxon_rank};
			if($taxon_rank eq "species" && $taxon_name =~ /\w+\+\w+/){
				$taxon_rank = "Genus and species";
			}
			elsif($taxon_rank eq "genus"){
				$taxon_rank = "Genus";
			}
			if($taxon_rank ne "Genus" && $taxon_rank ne "Genus and species" &&
					$taxon_rank ne "species"){
				$taxon_rank = "Higher taxon";
			}
			$taxon_rank =~ s/\s/+/g;
			$output .= qq|<a href="/cgi-bin/bridge.pl?action=checkTaxonInfo|;
			$output.="&taxon_name=".$taxon_name."&taxon_rank=$taxon_rank\">";
			$output .= $item->{taxon_name}."</a>,\n";
		}
		$output =~ s/,\s*$//;
		print $output;
	}
	} # CHILDREN block
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
			  "WHERE taxon_name='$taxon_name' AND taxon_rank='$taxon_rank'";
	my @results = @{$dbt->getData($sql)};
	my $taxon_no = $results[0]->{taxon_no};
	PBDBUtil::debug(1,"taxon rank: $taxon_rank");
	PBDBUtil::debug(1,"taxon name: $taxon_name");
	PBDBUtil::debug(1,"taxon number from authorities: $taxon_no");
	unless($taxon_no){
		print "<i>No taxonomic history is available for $taxon_rank $taxon_name.</i><br>";
		return 0;
	}

	print "<center><h3>Taxonomic history</h3></center>";
	print "<ul>";

	# Get the original combination (of the verified name, not the focal name)
	my $original_combination_no = getOriginalCombination($dbt, $taxon_no);
	PBDBUtil::debug(1,"original combination_no: $original_combination_no");
	
	# Select all parents of the original combination whose status' are
	# either 'recombined as' or 'corrected as'
	$sql = "SELECT DISTINCT(parent_no), status FROM opinions ".
		   "WHERE child_no=$original_combination_no ".	
		   "AND (status='recombined as' OR status='corrected as')";
	@results = @{$dbt->getData($sql)};

	# Combine parent numbers from above for the next select below. If nothing
	# was returned from above, use the original combination number.
	my @parent_list = ();
	if(scalar @results <1){
		push(@parent_list, $original_combination_no);
	}
	else{
		foreach my $rec (@results){
			push(@parent_list,$rec->{parent_no});
		}
		# don't forget the original (verified) here, either: the focal taxon	
		# should be one of its children so it will be included below.
		push(@parent_list, $original_combination_no);
	}

	# Select all synonymies for the above list of taxa.
	$sql = "SELECT DISTINCT(child_no), status FROM opinions ".
		   "WHERE parent_no IN (".
			join(',',@parent_list).") ".
		   "AND (status like '%synonym%' OR status='homonym of' OR status='replaced by')";
	@results = @{$dbt->getData($sql)};

	# Reduce these results to original combinations:
	foreach my $rec (@results){
		$rec = getOriginalCombination($dbt, $rec->{child_no});	
	}

	# NOTE: "corrected as" could also occur at higher taxonomic levels.

	# Get synonymies for all of these original combinations
	foreach my $child (@results){
		my $list_item = getSynonymyParagraph($dbt, $child);
		push(@paragraphs, "<br><br>$list_item\n") if($list_item ne "");
	}

	# Print the info for the original combination of the passed in taxon first.
	print getSynonymyParagraph($dbt, $original_combination_no);

	# Now alphabetize the rest:
	@paragraphs = sort {lc($a) cmp lc($b)} @paragraphs;
	foreach my $rec (@paragraphs){
		print $rec;
	}

	print "</ul>";
	print "<hr>";
}

##
#
##
sub getSynonymyParagraph{
	my $dbt = shift;
	my $taxon_no = shift;
	my %synmap = ( 'recombined as' => 'recombined as ',
				   'corrected as' => 'corrected as ',
				   'belongs to' => 'revalidated by ',
				   'nomen dubium' => 'considered a nomen dubium ',
				   'nomen nudum' => 'considered a nomen nudum ',
				   'nomen vanum' => 'considered a nomen vanum ',
				   'nomen oblitem' => 'considered a nomen oblitem ',
				   'subjective synonym of' => 'synonymized subjectively with ',
				   'objective synonym of' => 'synonymized objectively with ');
	my $text = "";

	# "Named by" part first:
	# Need to print out "[taxon_name] was named by [author] ([pubyr])".
	# - select taxon_name, author1last, pubyr, reference_no from authorities
	my $sql = "SELECT taxon_name, author1last, pubyr, reference_no, ".
			  "ref_is_authority FROM authorities WHERE taxon_no=$taxon_no";
	my @auth_rec = @{$dbt->getData($sql)};
	# Get ref info from refs if 'ref_is_authority' is set
	if($auth_rec[0]->{ref_is_authority} =~ /YES/i){
		PBDBUtil::debug(1,"author and year from refs<br>");
		# If we didn't get an author and pubyr and also didn't get a 
		# reference_no, we're at a wall and can go no further.
		if(!$auth_rec[0]->{reference_no}){
			$text .= "Cannot determine taxonomic history for ".
					 $auth_rec[0]->{taxon_name}."<br>.";
			return $text;	
		}
		$sql = "SELECT author1last,author2last,otherauthors,pubyr FROM refs ".
			   "WHERE reference_no=".$auth_rec[0]->{reference_no};
		@results = @{$dbt->getData($sql)};
		$text .= "<li><i>".$auth_rec[0]->{taxon_name}."</i> was named by ".
			  	 $results[0]->{author1last};
		if($results[0]->{otherauthors} ne ""){
			$text .= " et al. ";
		}
		elsif($results[0]->{author2last} ne ""){
			# We have at least 120 refs where the author2last is 'et al.'
			if($key_list[$j]->{author2last} eq "et al."){
				$text .= " et al. ";
			}
			else{
				$text .= " and ".$results[0]->{author2last}." ";
			}
		}
		$text .= " (".$results[0]->{pubyr}.")";
	}
	# If ref_is_authority is not set, use the authorname and pubyr in this
	# record.
	elsif($auth_rec[0]->{author1last} && $auth_rec[0]->{pubyr}){
		PBDBUtil::debug(1,"author and year from authorities<br>");
		$text .= "<li><i>".$auth_rec[0]->{taxon_name}."</i> was named by ".
			  	 $auth_rec[0]->{author1last}." (".$auth_rec[0]->{pubyr}.")";
	}
	# if there's nothing from above, give up.
	else{
		$text .= "</ul>";
		$text .= "<i>The author of ".
				 $auth_rec[0]->{taxon_rank}." ".
				 $auth_rec[0]->{taxon_name}." is not known.</i><br>";
		return $text;
	}

	# Now, synonymies:
	$sql = "SELECT parent_no, status, reference_no, pubyr, author1last, ".
		   "author2last, otherauthors ".
		   "FROM opinions WHERE child_no=$taxon_no AND status != 'belongs to'".
		   " AND status NOT LIKE 'nomen%'";
	@results = @{$dbt->getData($sql)};

	my %synonymies = ();
	my @syn_years = ();
	# check for synonymies - status' of anything other than "belongs to"
	foreach my $row (@results){
		# get the proper reference (record first, refs table second)
		if(!$row->{author1last} || !$row->{pubyr}){
			# select into the refs table.
			$sql = "SELECT author1last,author2last,otherauthors,pubyr ".
				   "FROM refs ".
				   "WHERE reference_no=".$row->{reference_no};
			my @real_ref = @{$dbt->getData($sql)};
			$row->{author1last} = $real_ref[0]->{author1last};
			$row->{author2last} = $real_ref[0]->{author2last};
			$row->{otherauthors} = $real_ref[0]->{otherauthors};
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
	
	# Sort the items in each synonymy value by pubyr
	foreach my $key (keys %synonymies){
		my @years = @{$synonymies{$key}};
		@years = sort{$a->{pubyr} cmp $b->{pubyr}} @years;
		$synonymies{$key} = \@years;
		push(@syn_years, $years[0]->{pubyr});
	}

	# sort the list of beginning syn_years
	@syn_years = sort{$a cmp $b} @syn_years;

	# Revalidations and nomen*'s
	$sql = "SELECT parent_no, status, reference_no, pubyr, author1last, ".
		   "author2last, otherauthors, opinion_no ".
		   "FROM opinions WHERE child_no=$taxon_no AND (status='belongs to' ".
		   "OR status like 'nomen%')";
	@results = @{$dbt->getData($sql)};
	my %nomen_or_reval = ();
	my @nomen_or_reval_numbers = ();
	my $has_nomen = 0;
	foreach my $row (@results){
		# get the proper reference (record first, refs table second)
		if(!($row->{author1last} && $row->{pubyr})){
			# select into the refs table.
			$sql = "SELECT author1last,author2last,otherauthors,pubyr ".
				   "FROM refs ".
				   "WHERE reference_no=".$row->{reference_no};
			my @real_ref = @{$dbt->getData($sql)};
			$row->{author1last} = $real_ref[0]->{author1last};
			$row->{author2last} = $real_ref[0]->{author2last};
			$row->{otherauthors} = $real_ref[0]->{otherauthors};
			$row->{pubyr} = $real_ref[0]->{pubyr};
		}
		# use opinion numbers to keep recs separate for now
		$nomen_or_reval{$row->{opinion_no}} = $row;
		push(@nomen_or_reval_numbers, $row->{opinion_no});
		if($row->{status} =~ /nomen/){
			$has_nomen = 1 
		}
	}
	@nomen_or_reval_numbers = sort{$nomen_or_reval{$a}->{pubyr} <=> $nomen_or_reval{$b}->{pubyr}} @nomen_or_reval_numbers;	
	# Since these are arranged numerically now chop of any leading "belongs to"
	# recs whose pubyr is not newer than the oldest synonymy. Keep the whole 
	# list if the oldest thing going is a nomen* record.
	for(my $index = 0; $index < @nomen_or_reval_numbers; $index++){
		# Because of the redo, below:
		last if(scalar @nomen_or_reval_numbers < 1);
		last if($nomen_or_reval{$nomen_or_reval_numbers[$index]}->{status} =~ /nomen/);
		# otherwise, it's a 'belongs to' status
		if(scalar @syn_years < 1 || $nomen_or_reval{$nomen_or_reval_numbers[$index]}->{pubyr} < $syn_years[0]){
			delete $nomen_or_reval{$nomen_or_reval_numbers[$index]};
			shift @nomen_or_reval_numbers;
			redo;
		}
		else{
			last;
		}
	}
	# Combine all adjacent like status types from %nomen_or_reval.
	# (They're chronological: nomen, reval, reval, nomen, nomen, reval, etc.)
	my %additional = ();
	my $last_status;
	my $last_key;
	for(my $index = 0; $index < @nomen_or_reval_numbers; $index++){
		if($last_status && $last_status eq $nomen_or_reval{$nomen_or_reval_numbers[$index]}->{status}){
			push(@{$additional{$last_key}}, $nomen_or_reval{$nomen_or_reval_numbers[$index]});
		}
		else{
			$last_key = "nomen_reval$index";
			$additional{$last_key} = [$nomen_or_reval{$nomen_or_reval_numbers[$index]}];
		}
		$last_status = $nomen_or_reval{$nomen_or_reval_numbers[$index]}->{status};
	}

	# Put revalidations and nomen*'s in synonymies hash.
	if(scalar(keys %synonymies) or $has_nomen){
		foreach my $key (keys %additional){
			$synonymies{$key} = $additional{$key};
		}
	}

	# Now print it all out
	my @syn_keys = keys %synonymies;

	# Order by ascending pubyr of first record in each synonymy list.
	@syn_keys = sort{$synonymies{$a}[0]->{pubyr} cmp $synonymies{$b}[0]->{pubyr}} @syn_keys;
	
	# Exception to above:  the most recent opinion should appear last.
	# Splice it
	if(scalar @syn_keys > 1){
		my $oldest = 0;
		my $oldest_index = 0;
		# The rows are already ordered by pubyr, so find the most recent last
		# element in any row
		for(my $index = 0; $index < @syn_keys; $index++){
			my $date = ${$synonymies{$syn_keys[$index]}}[-1]->{pubyr};	
			if($date > $oldest){
				$oldest = $date;
				$oldest_index = $index;
			}
		}	
		my $new_oldest_key = splice(@syn_keys, $oldest_index, 1);
		# And put it at the end.
		push(@syn_keys, $new_oldest_key);
	}

	# Loop through unique parent number from the opinions table.
	# Each parent number is a hash key whose value is an array ref of records.
	for(my $index = 0; $index < @syn_keys; $index++){
		# $syn_keys[$index] is a parent_no, so $synonymies{$syn_keys[$index]}
		# is a record from the immediately preceeding 'opinions' select.
		$text .= "; it was ".$synmap{$synonymies{$syn_keys[$index]}[0]->{status}};
		$sql = "SELECT taxon_name FROM authorities ".
			   "WHERE taxon_no=";
		if($synonymies{$syn_keys[$index]}[0]->{status} =~ /nomen/ or
		   $synonymies{$syn_keys[$index]}[0]->{status} =~ /belongs/){
			$sql .= $synonymies{$syn_keys[$index]}[0]->{parent_no};
		}
		else{	
			$sql .=  $syn_keys[$index];
		}
		@results = @{$dbt->getData($sql)};
		unless($synmap{$synonymies{$syn_keys[$index]}[0]->{status}} eq "revalidated by "){
			$text .= "<i>".$results[0]->{taxon_name}."</i> by ";
		}
		# Dereference the hash value (array ref of opinions recs), so we can
		# write out all of the authors/years for this synonymy.
		my @key_list = @{$synonymies{$syn_keys[$index]}};
		# sort the list by date (ascending)
		@key_list = sort {$a->{pubyr} <=> $b->{pubyr}} @key_list;
		for(my $j = 0; $j < @key_list; $j++){
			$text .= $key_list[$j]->{author1last};
			if($key_list[$j]->{otherauthors} ne ""){
				$text .= " et al. ";
			}
			elsif($key_list[$j]->{author2last} ne ""){
				# We have at least 120 refs where the author2last is 'et al.'
				if($key_list[$j]->{author2last} eq "et al."){
					$text .= " et al. ";
				}
				else{
					$text .= " and ".$key_list[$j]->{author2last}." ";
				}
			}
			$text .= " (".$key_list[$j]->{pubyr}."), ";
		}
		if($text =~ /,\s+$/){
			# remove the last comma
			$text =~ s/,\s+$//;
			# replace the last comma-space sequence with ' and '
			$text =~ s/(,\s+([a-zA-Z\-']+\s+(and\s+[a-zA-Z\-']+\s+|et al.\s+){0,1}\(\d{4}\)))$/ and $2/;
			# put a semi-colon on the end to separate from any following syns.
		}
	}
	if($text ne ""){
		# Add a period at the end.
		$text .= '.';
		# remove a leading semi-colon and any space.
		$text =~ s/^;\s+//;
		# capitalize the first 'I' in the first word (It).
		$text =~ s/^i/I/;
	}

	return $text;
}

## getOriginalCombination
#
##
sub getOriginalCombination{
	my $dbt = shift;
	my $taxon_no = shift;
	# You know you're an original combination when you have no children
	# that have recombined or corrected as relations to you.
	my $sql = "SELECT DISTINCT(child_no), status FROM opinions ".
			  "WHERE parent_no=$taxon_no AND (status='recombined as' OR ".
			  "status='corrected as')";
	my @results = @{$dbt->getData($sql)};

	my $has_status = 0;
	foreach my $rec (@results){
		$sql = "SELECT DISTINCT(child_no), status FROM opinions ".
			   "WHERE parent_no=".$rec->{child_no}.
			   " AND (status='recombined as' OR status='corrected as')";	
		my @second_level = @{$dbt->getData($sql)};
		if(scalar @second_level < 1){
			return $rec->{child_no};
		}
		# else
		$has_status = 1;
	}
	# If all results in the loop above gave results, follow the first one
	# down (assuming they all point to the same place eventually) recursively.
	if($has_status){
		$taxon_no = getOriginialCombination($dbt,$results[0]->{child_no});
	}

	# If we fall through above but $has_status was not set, we just
	# return the original $taxon_no passed in.
	return $taxon_no;
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
			# first, use the pubyr/author for clarification:
			if($ref->{pubyr} && $ref->{author1last}){
				$ref->{clarification_info} = $ref->{author1last}." ".$ref->{pubyr}." ";
			}
			elsif($ref->{ref_is_authority} && $ref->{reference_no}){
				my $sql = "SELECT pubyr, author1last FROM refs ".
						  "WHERE reference_no=".$ref->{reference_no};
				my @auth_ref = @{$dbt->getData($sql)};
				$ref->{clarification_info} = $auth_ref[0]->{author1last}." ".$auth_ref[0]->{pubyr}." ";
			}
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
				if(scalar @ref_ref < 1){
					# Dead end: clarification_info?
					return $array_ref;	
				}
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
			$ref->{clarification_info} .= "(".$ref_ref[0]->{taxon_name}.")";
		}
	}
	return \@array_of_hash_refs;
}


## Add items from new array to old array iff they are not duplicates
## Where arrays are db record sets (arrays of hashes)
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
			push(@orig, $item);
		}
	}
	return \@orig;
}

# Verify taxon:  If what was entered's most recent parent is a "belongs to"
# or a "nomen *" relationship, then do the display page on what was entered.
# If any other relationship exists for the most recent parent, display info 
# on that parent.  
sub verify_chosen_taxon{
	my $taxon = shift;
	my $num = shift;
	my $dbt = shift;
	my $sql = "";
 	my @results = ();
#	my $temp_num=0;

	# First, see if this name has any recombinations:
	if(!$num){
		$sql = "SELECT taxon_no FROM authorities WHERE taxon_name='$taxon'";
		@results = @{$dbt->getData($sql)};
		# Elephas maximus won't return anything, but the script will still
		# work for it because we have one occurrence in the db. (so don't
		# let it choke out here)
		if(@results > 0){
			$num = $results[0]->{taxon_no};
			#$temp_num = $results[0]->{taxon_no};
		}
	}
	#if($temp_num){
	if($num){
		$sql = "SELECT child_no,taxon_name FROM opinions,authorities ".
			   "WHERE parent_no=$num AND taxon_no=child_no ".
			   #"WHERE parent_no=$temp_num AND taxon_no=child_no ".
			   "AND status='recombined as'";
		@results = @{$dbt->getData($sql)};
		# might not get any recombinations; that's fine - keep going either way.
		if(scalar @results > 0){
			$num = $results[0]->{child_no};
			$taxon = $results[0]->{taxon_name};
		}
	}

	# Now, get the senior synonym or most current combination:
	$sql = "SELECT authorities.taxon_no,opinions.parent_no,opinions.pubyr, ".
			  "opinions.status, opinions.reference_no ".
			  "FROM authorities, opinions ".
			  "WHERE authorities.taxon_name='$taxon' ".
			  "AND authorities.taxon_no = opinions.child_no";

	if($num){
		$sql = "SELECT parent_no, pubyr, reference_no, status ".
			   "FROM opinions WHERE child_no=$num";
	}
	@results = @{$dbt->getData($sql)};
	
	# Elephas maximus won't return anything for the same reason mentioned above.
	if(@results <1){
		return ($taxon, $num);
	}
	
	my $index = selectMostRecentParentOpinion($dbt, \@results, 1);

	# if the most recent parent is a 'belongs to' relationship, just return
	# what we were given (or found in recombinations).
	if($results[$index]->{status} eq "belongs to" ||
	  							$results[$index]->{status} =~ /nomen/i){
		return ($taxon, $num);
	}
	# Otherwise, return the name and number of the parent.
	else{
		my $parent_no = $results[$index]->{parent_no};
		$sql = "SELECT taxon_name FROM authorities WHERE taxon_no=".
			   $results[$index]->{parent_no};
		@results = @{$dbt->getData($sql)};
		return ($results[0]->{taxon_name},$parent_no);
	}
}


1;
