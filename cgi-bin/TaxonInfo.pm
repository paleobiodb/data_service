package TaxonInfo;

use PBDBUtil;

$DEBUG = 1;

# WARNING: HIGHLY EXPERIMENTAL
# TAXON INFORMATION 
sub startTaxonInfo{
	# NOTE:  gonna have to get prefs for map scale, etc.
	# NOTE:  need to intercept query to see which continents are returned.
	# NOTE:  The above (immediate) should probably be done via defining my
	# own 'displayMapResults' method so I can get back the collection
	# data (numbers and all else from that "select *" in Map.pm)
	# for the collection search display.

	my $html = "<form method=post action=\"/cgi-bin/bridge.pl\">".
		   "<input id=\"action\" type=hidden name=\"action\"".
		   " value=\"checkTaxonInfo\">".
		   "<table width=\"100%\"><tr><td valign=top align=\"middle\">".
		   "<nobr><select name=\"taxon_rank\"><option>Genus".
		   "<option>Genus and species<option>species".
		   "</select></td><td>name:</nobr><td>".
		   "<input name=\"genus_name\" type=\"text\" size=25></td><td>".
		   "<input id=\"submit\" name=\"submit\" value=\"Draw Map\"".
		   " type=\"submit\"></tr></table></form>";

	# Spit out the HTML
	# THIS IS PRETTY BARE BONES...
	print main::stdIncludes( "std_page_top" );
	print "<center><h3>Taxon Information</h3></center>";
	print $html;
	print main::stdIncludes("std_page_bottom");
}

sub checkStartForm{
	my $q = shift;
	my $dbh = shift;
	my $s = shift;
	my $taxon_type = $q->param("taxon_rank");
	my $taxon_name = $q->param("genus_name");
	my $sth = "";
	my $results = "";
	my $genus = "";
	my $species = "";
	my $sql =""; 

	# if we got here because we had to relogin (old session), we could
	# have a query string that looks like 
	# "action=displayLogin&destination=checkTaxonInfo"
	# which would fall into the 'else' clause below and hammer the server
	# printing a "choose one" page with ALL the genus/species in the db!
	# NOTE: we could also check that we got at least one of genus/species
	# non-empty before running any selects...
	if($taxon_type eq "" or $taxon_name eq ""){
	    print $q->redirect(-url=>$BRIDGE_HOME."?action=displayPaulsTest");
	    exit;
	}

	# Change +'s to spaces from CGI (insurance)
	#$taxon_name =~ s/\+/ /g;
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
	$sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	# execute returns true if the statement was successfully executed.
	if($sth->execute()){
		# Returns results as a reference to an array which contains
		# references to hashes, representing rows of data.
		$results = $sth->fetchall_arrayref({});
		print "ERROR: $sth->err<br>" if($sth->err);
		if(scalar(@{$results}) < 1 ){
			print main::stdIncludes("std_page_top");
			print "<center><h3>No results found.</h3>".
			      "genus: \&lt;<i>$genus</i>\&gt; and ".
			      "species: \&lt;<i>$species</i>\&gt;.";
			print "<br><br><a href=\"/cgi-bin/bridge.pl?action=".
			      "displayPaulsTest\">Search Again</a></center>";
			print main::stdIncludes("std_page_bottom");
		}
		# if we got just one result, we assume they chose 
		# 'Genus and species' and got an exact match.
		elsif(scalar(@{$results}) == 1){
			displayTaxonInfoResults($q, $dbh, $s);
		}
		# Show 'em their choices (radio buttons for genus-species)
		else{
			print main::stdIncludes( "std_page_top" );
			print "<center><h3>Please Select One:</h3>";
			print "<form method=post action=\"/cgi-bin/bridge.pl\">".
			      "<input id=\"action\" type=\"hidden\"".
			      " name=\"action\"".
			      " value=\"displayTaxonInfoResults\">".
			      "<input type=\"hidden\" name=\"taxon_rank\" ".
			      "value=\"$taxon_type\">".
			      "<table width=\"100%\">";
			my $newrow = 0;
			foreach $hash (@{$results}){
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
			print "<input type=\"submit\" value=\"Get Taxon Info\">";
			print "</td></tr></table></form></center>";

			print main::stdIncludes("std_page_bottom");
		}
	}
	print "ERROR: $sth->errstr<br>" if($sth->errstr);
}

sub displayTaxonInfoResults{
	my $q = shift;
	my $dbh = shift;
	my $s = shift;

	require Map;

	print main::stdIncludes("std_page_top");

	my $m = Map->new( $dbh, $q, $s );
	$q->param(-name=>"taxon_info_script",-value=>"yes");
	# NOTE: THIS needs to be a single continent if it comes up on just one!
	$q->param(-name=>"mapcontinent",-value=>"global");
	$q->param(-name=>"mapresolution",-value=>"medium");
	$q->param(-name=>"mapscale",-value=>"X 3");
	$q->param(-name=>"mapbgcolor",-value=>"white");
	$q->param(-name=>"gridsize",-value=>"30 degrees");
	$q->param(-name=>"gridcolor",-value=>"gray");
	$q->param(-name=>"coastlinecolor",-value=>"black");
	$q->param(-name=>"pointshape",-value=>"medium circles");
	$q->param(-name=>"dotcolor",-value=>"blue");
	$q->param(-name=>"dotborder",-value=>"without");

	# NOTE: ERROR: need to change/remove the "search again" link generated
	# at the bottom of this output.
	# NOTE: DO THIS STEPWISE: instead of this wrapper, do the mapQueryDb
	# and then figure out the number of continents, then call the other
	# routines.
	# NOTE: FIGURING OUT CONTINENTS:  do I do a backwards Map::mapGetScale
	# or just compare collections.country names? THE BEST THING would be
	# to add a CONTINENT column to the collections table (actually, a
	# CONTINENT table with a reference to it from collections).
	$m->buildMap();
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
	    }
	}
	print "<h3>Collections</h3>";
	print "<table width=\"100%\"><tr bgcolor=\"white\">";
	print "<th align=\"middle\">Time - Place</th>";
	print "<th align=\"middle\">Collections</th></tr>";
	my $row_color = 0;
	foreach my $key (keys %time_place_coll){
		if($row_color % 2 == 0){
		    print "<tr bgcolor=\"E0E0E0\">";
		} 
		else{
		    print "<tr bgcolor=\"white\">";
		}
		print "<td align=\"middle\"><span class=tiny>$key</span></td><td align=\"left\">";
		foreach  my $val (@{$time_place_coll{$key}}){
		    my $link=Collections::createCollectionDetailLink($exec_url,$val,$val);
		    print "$link ";
		    #print "$val\&nbsp;";
		}
		print "</td></tr>";
		$row_color++;
	}
	print "</table><hr>";
	
	#subroutine to do the classification
	my ($genus, $species) = split /\s+/, $q->param("genus_name");
	displayTaxonClassification($genus, $species, $dbh);
	
	#subroutine to do the synonymy
	displayTaxonSynonymy($genus,$species);

	print main::stdIncludes("std_page_bottom");
}

sub displayTaxonClassification{
	my $genus = shift or "";
	my $species = shift or "";
	my $dbh = shift;

	my $sth = "";
	my $taxon_rank = "genus";
	my $taxon_name = "$genus";

	my %classification;
	# Initialize our classification hash with the info
	# that came in as an argument to this method.
	$classification{species} = $species;
	$classification{genus} = $genus;

	# default to a number that doesn't exist in the database.
	my $child_no = -1;
	my $parent_no = -1;
	my %parent_no_visits = ();
	my %child_no_visits = ();

	my $status = "";
	# Loop at least once, but as long as it takes to get full classification
	while($parent_no){
		# We know the taxon_rank and taxon_name, so get its number
		my $sql_auth_inv = "SELECT taxon_no ".
				   "FROM authorities ".
				   "WHERE taxon_rank = '$taxon_rank' ".
				   "AND taxon_name = '$taxon_name'";
		PBDBUtil::debug(1,"authorities inv: $sql_auth_inv<br>");
		$sth = $dbh->prepare($sql_auth_inv) || die ("$sql_auth_inv<hr>$!");
		if($sth->execute()){
			my @tmp_array = $sth->fetchrow_array();
			# Keep $child_no at -1 if no results are returned.
			if(defined $tmp_array[0]){
				$child_no = $tmp_array[0];

				# Insurance for self referential / bad data in database.
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
		    print "ERROR: $sth->errstr<br>" if($sth->errstr);
		    last;
		}
		
		# Now see if the opinions table has a parent for this child
		my $sql_opin =  "SELECT status, parent_no ".
				"FROM opinions ".
				"WHERE child_no=$child_no";
		PBDBUtil::debug(1,"opinions: $sql_opin<br>");
		$sth = $dbh->prepare($sql_opin) || die ("$sql_opin<hr>$!");
		if($sth->execute()){
			# NOTE: THIS IS NOT COMPLETE. a child could have
			# multiple parents.  Get all parents, and use the one
			# that has the most recent opinion. ALSO NOTE: the 
			# pubyr of the opinion could be blank... if so, use
			# the reference_no to key into refs.pubyr.
			#******************************************************
			($status,$parent_no) = $sth->fetchrow_array();

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
				$sth = $dbh->prepare($sql_auth) || 
						die ("$sql_auth<hr>$!");
				if($sth->execute()){
					$auth_hash_ref = $sth->fetchrow_hashref;
					# reset name and rank for next loop pass
					$taxon_rank = $auth_hash_ref->{"taxon_rank"};
					$taxon_name = $auth_hash_ref->{"taxon_name"};
					$classification{$taxon_rank} = $taxon_name;
				}
				else{
				    print "ERROR: $sth->errstr<br>" if($sth->errstr);
				    last;
				}
			}
			# If we didn't get a parent or status ne 'belongs to'
			else{
				$parent_no = 0;
			}
		}
		else{
			print "ERROR: $sth->errstr<br>" if($sth->errstr);
			last;
		}
	# This gets set to zero, 8 lines above, when we run into the ceiling.
	}

	print "<h3>Classification</h3>";
	print "<center><table width=\"50%\"><tr><th>Rank</th><th>Name</th></tr>";
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

	print "</table></center><hr>";
}

sub displayTaxonSynonymy{
	my $genus = shift or "";
	my $species = shift or "";

	# NOTE: may need an algorithm similar to above. Get author and pubyr
	# from opinions (if pubyr missing, key into refs table). 
	# NOW, if the status is anything but 'belongs to' , use the parent_no to
	# key back to the child_no and tack on to synonymy until we get a
	# status of 'belongs to'.

	print "<h3>Synonymy</h3>";
	print "<i>$genus $species</i> was named by ...<br>";

	print "<hr>";
}

# Shows the form for requesting a map
sub displayMapForm {

	# defaults
	my @row = ('global', 'X 3', 'white', '30 degrees', 'gray', 'black', 'medium circles', 'blue', 'without');
	my @fieldNames = ('mapcontinent', 'mapscale', 'mapbgcolor', 'gridsize', 'gridcolor', 'coastlinecolor', 'pointshape', 'dotcolor', 'dotborder');
	
	# Read preferences if there are any JA 8.7.02
	%pref = &getPreferences($s->get('enterer'));
	# Get the enterer's preferences
	my ($setFieldNames,$cleanSetFieldNames,$shownFormParts) = &getPrefFields();
	for $p (@{$setFieldNames})	{
		if ($pref{$p} ne "")	{
			unshift @row,$pref{$p};
			unshift @fieldNames,$p;
		}
	}

	%pref = &getPreferences($s->get('enterer'));
	my @prefkeys = keys %pref;
    my $html = $b->populateHTML ('map_form', \@row, \@fieldNames, \@prefkeys);
	buildAuthorizerPulldown ( \$html );

	my $authorizer = $s->get("authorizer");
	$html =~ s/%%authorizer%%/$authorizer/;

	# Spit out the HTML
	print main::stdIncludes("std_page_top");
	print $html;
	print main::stdIncludes("std_page_bottom");
}

1;
