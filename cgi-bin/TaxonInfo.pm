package TaxonInfo;

use PBDBUtil;
use Classification;
use Debug;
use HTMLBuilder;
use DBTransactionManager;
use Taxon;
use TimeLookup;
use Data::Dumper;

use POSIX qw(ceil floor);


$DEBUG = 0;


# JA: rjp did a big complicated reformatting of the following and I can't
#  confirm that no damage was done in the course of it
# him being an idiot, I think it's his fault that all the HTML originally was
#  embedded instead of being in a template HTML file that's passed to
#  populateHTML
# possibly Muhl's fault because he uses populateHTML nowhere in this module
# I fixed this 21.10.04
sub searchForm {
	my $hbo = shift;
	my $q = shift;
	my $search_again = (shift or 0);

	if ($search_again){
        $page_title = "<h2>No results found</h2> (please search again)";
    } else {
        $page_title = "<h2>Taxon search form</h2>";
	}
	print $hbo->populateHTML('search_taxoninfo_form' , [$page_title,$page_subtitle], ['page_title','page_subtitle']);

}

# This is the front end for displayTaxonInfoResults - always use this instead if you want to 
# call from another script.  Can pass it a taxon_no or a taxon_name
sub checkTaxonInfo {
	my $q = shift;
	my $dbh = shift;
	my $s = shift;
	my $dbt = shift;
    my $hbo = shift;
	
    if ($q->param('taxon_no')) {
        # If we have is a taxon_no, use that:
        displayTaxonInfoResults($dbt,$s,$q);
    } elsif (!$q->param('taxon_name') && !($q->param('pubyr')) & !$q->param('author')) {
        searchForm($hbo,$q);
    } else {
        my @results = ();
        my $sql = "SELECT a.taxon_no,a.taxon_rank,a.taxon_name,a.pages,a.figures,a.comments, ".
               " IF (a.ref_is_authority='YES',r.pubyr,a.pubyr) pubyr,".
               " IF (a.ref_is_authority='YES',r.author1init,a.author1init) author1init,".
               " IF (a.ref_is_authority='YES',r.author1last,a.author1last) author1last,".
               " IF (a.ref_is_authority='YES',r.author2init,a.author2init) author2init,".
               " IF (a.ref_is_authority='YES',r.author2last,a.author2last) author2last,".
               " IF (a.ref_is_authority='YES',r.otherauthors,a.otherauthors) otherauthors".
               " FROM authorities a LEFT JOIN refs r ON a.reference_no=r.reference_no".
               " WHERE taxon_name LIKE ".$dbh->quote($q->param('taxon_name'));
        
        # Handle pubyr and author fields
        my $having_sql = '';
        $authorlast = $q->param('author');
        if ($q->param('author')) {
            $having_sql .= " AND (author1last like ".$dbh->quote($authorlast)." OR author2last like ".$dbh->quote($authorlast)." OR otherauthors like ".$dbh->quote('%'.$authorlast.'%').")";
        }
        if ($q->param('pubyr')) {
            $having_sql .= " AND pubyr like ".$dbh->quote($q->param('pubyr'));
        }
        $having_sql =~ s/^ AND/ HAVING/;
        $sql .= $having_sql;
        @results = @{$dbt->getData($sql)};

        # now deal with results:
        if(scalar @results < 1 ){
            # If nothing from authorities, go to occs + reids
            my ($genus,$species) = split(/ /,$q->param('taxon_name'));
            my $where = "WHERE genus_name LIKE ".$dbh->quote($genus);
            if ($species) {
                $where .= " AND species_name LIKE ".$dbh->quote($species);
            }
            $sql = "(SELECT genus_name FROM occurrences $where GROUP BY genus_name)".
                   " UNION ".
                   "(SELECT genus_name FROM reidentifications $where GROUP BY genus_name)";
            my @genera = @{$dbt->getData($sql)};
            if (scalar(@genera) > 1) {
                # now create a table of choices and display that to the user
                print "<div align=\"center\"><h2>Please select a taxon</h2><br>";

                print qq|<form method="POST" action="bridge.pl">|;
                print qq|<input type="hidden" name="action" value="checkTaxonInfo">|;
                
                print "<table>\n";
                print "<tr>";
                for(my $i=0; $i<scalar(@genera); $i++) {
                    my $checked = ($i == 0) ? "CHECKED" : "";
                    my $taxon_name = "$genera[$i]->{genus_name} $species";
                    print qq|<td><input type="radio" name="taxon_name" value="$taxon_name" $checked>$taxon_name</td>|;
                    print "</tr><tr>";
                }
                print "</tr>";
                print "<tr><td align=\"middle\" colspan=3><br>";
                print "<input type=\"submit\" value=\"Get taxon info\">";
                print "</td></tr></table></form></div>";
            } elsif (scalar(@genera) == 1) {
                my $taxon_name = $genera[0]->{'genus_name'};
                if ($species) {
                    $taxon_name .= " $species";
                }    
                $q->param('taxon_name'=>$taxon_name);
                displayTaxonInfoResults($dbt,$s,$q);
            } else {
                # If nothing, print out an error message
                searchForm($hbo, $q, 1); # param for not printing header with form
                if($s->isDBMember()) {
                    print "<center><p><a href=\"bridge.pl?action=submitTaxonSearch?goal=authority&taxon_name=".$q->param('taxon_name')."\"><b>Add taxonomic information</b></a></center>";
                }
            }
        } elsif(scalar @results == 1){
            $q->param('taxon_no'=>$results[0]->{'taxon_no'});
            displayTaxonInfoResults($dbt,$s,$q);
        } else{
            # now create a table of choices and display that to the user
            print "<div align=\"center\"><h2>Please select a taxon</h2><br>";

            print qq|<form method="POST" action="bridge.pl">|;
            print qq|<input type="hidden" name="action" value="checkTaxonInfo">|;
            
            print "<table>\n";
            print "<tr>";
            for(my $i=0; $i<scalar(@results); $i++) {
                my $authorityLine = Taxon::formatAuthorityLine($dbt,$results[$i]);
                my $checked = ($i == 0) ? "CHECKED" : "";
                print qq|<td><input type="radio" name="taxon_no" value="$results[$i]->{taxon_no}" $checked> $authorityLine</td>|;
                print "</tr><tr>";
            }
            print "</tr>";
            print "<tr><td align=\"middle\" colspan=3><br>";
            print "<input type=\"submit\" value=\"Get taxon info\">";
            print "</td></tr></table></form></div>";

        }
    }

}

# By the time we're here, we're gone through checkTaxonInfo and one of these scenarios has happened
#   1: taxon_no is set: taxon is in the authorities table
#   2: taxon_name is set: NOT in the authorities table, but in the occs/reids table
# If neither is set, bomb out, we shouldn't be here
#   entered_name could also be set, for link display purposes. entered_name may not correspond 
#   to taxon_no, depending on if we follow a synonym or go to an original combination
sub displayTaxonInfoResults {
	my $dbt = shift;
	my $s = shift;
	my $q = shift;

    my $dbh = $dbt->dbh;

    my $taxon_no = $q->param('taxon_no');

	# Verify taxon: If what was entered is a synonym for something else, use the
    # synonym. Else use the taxon
    my ($taxon_name,$taxon_rank);
    if ($taxon_no) {
        my $orig_taxon_no = getOriginalCombination($dbt,$taxon_no);
        $taxon_no = getSeniorSynonym($dbt,$orig_taxon_no);
        # This actually gets the most correct name
        my $correct_row = getMostRecentParentOpinion($dbt,$taxon_no,1);
        if ($correct_row) {
            $taxon_name = $correct_row->{'child_name'};
            $taxon_rank = $correct_row->{'child_rank'};
        } else {
            my $t = getTaxon($dbt,'taxon_no'=>$orig_no);
            $child_spelling_no = $t->{'taxon_no'};
            $taxon_name = $t->{'taxon_name'};
            $taxon_rank = $t->{'taxon_rank'};
        }  
    } else {
        $taxon_name = $q->param('taxon_name');
    }

	# Get the sql IN list for a Higher taxon:
	my $in_list;
	if($taxon_no) {
    	# JA: replaced recurse call with taxonomic_search call 7.5.04 because
    	#  I am not maintaining recurse
        if ($taxon_rank =~ /class|phylum|kingdom/) {
            # Don't do this for class or higher, too many results can be returned PS 08/22/2005
            $in_list = [-1];
        } else {
            @in_list=PBDBUtil::taxonomic_search($dbt,$taxon_no);
            $in_list=\@in_list;
        }
	} else {
        # We just got to search the occ/reid tables directly
		$in_list = [$taxon_name];
	} 

    ($genus,$species) = split(/ /,$taxon_name);


    print "<div class=\"float_box\">";
    print "<p>&nbsp;</p>";
    ($modules_to_display,$thumbs) = doNavBox($dbt,$q,$s,$in_list);
    print "</div>";
	print "<div align=\"center\"><h2>$taxon_name</h2></div>";
	# Go through the list
	foreach my $module (@$modules_to_display){
        print "<div align=\"center\">";
		doModules($dbt,$dbh,$q,$s,$exec_url,$module,$genus,$species,$in_list,$taxon_no);
        print "</div>";
		print "<hr>"; 
	}
	# images are last
	my @selected_images = $q->param('image_thumbs');
	if(@selected_images){
		print "<center><h3>Images</h3></center>";
		foreach my $image (@selected_images){
			foreach my $res (@$thumbs){
				if($image == $res->{image_no}){
					print "<center><table><tr><td>";
					print "<center><img src=\"".$res->{path_to_image}.
						  "\" border=1></center><br>\n";
					print "<i>".$res->{caption}."</i><p>\n";
					if ( $res->{taxon_no} != $taxon_no )	{
						print "<div class=\"small\"><b>Original identification:</b> ".$res->{taxon_name}."</div>\n";
					}
					print "<div class=\"small\"><b>Original name of image:</b> ".$res->{original_filename}."</div>\n";
					if ( $res->{reference_no} > 0 )	{
						$sql = "SELECT author1last, author2last, otherauthors, pubyr FROM refs WHERE reference_no=" . $res->{reference_no};
						my @refresults = @{$dbt->getData($sql)};
						my $refstring = $refresults[0]->{author1last};
						if ( $refresults[0]->{otherauthors} )	{
							$refstring .= " et al.";
						} elsif ( $refresults[0]->{author2last} )	{
							$refstring .= " and " . $refresults[0]->{author2last};
						}
						$refstring =~ s/and et al\./et al./;
						print "<div class=\"small\"><b>Reference:</b> 
<a href=$exec_url?action=displayRefResults&reference_no=".$res->{reference_no}.">".$refstring." ".$refresults[0]->{pubyr}."</a></div>\n";
					}
					print "</td></tr></table></center>";
					print "<hr>";
					last;
				}
			}
		}
	}

    my $entered_name = $q->param('entered_name') || $q->param('taxon_name') || $taxon_name;
    my $entered_no   = $q->param('entered_no') || $q->param('taxon_no');
	print "<div style=\"font-family : Arial, Verdana, Helvetica; font-size : 14px;\">";
	if($s->isDBMember()) {
		# Entered Taxon
        if ($entered_no) {
		    print "<center><a href=\"/cgi-bin/bridge.pl?action=displayAuthorityForm&taxon_no=$entered_no\">";
		    print "<b>Edit taxonomic data for $entered_name</b></a> - ";
        } else {
		    print "<center><a href=\"/cgi-bin/bridge.pl?action=submitTaxonSearch&goal=authority&taxon_no=-1&taxon_name=$entered_name\">";
		    print "<b>Enter taxonomic data for $entered_name</b></a> - ";
        }

        if ($entered_no) {
		    print "<a href=\"/cgi-bin/bridge.pl?action=displayOpinionChoiceForm&taxon_no=$entered_no\"><b>Edit taxonomic opinions about $entered_name</b></a> - ";
            print "<a href=\"bridge.pl?action=startPopulateEcologyForm&taxon_no=$taxon_no\"><b>Add/edit ecological/taphonomic data</b></a> - ";
        }
		
		print "<a href=\"/cgi-bin/bridge.pl?action=startImage\">".
			  "<b>Enter an image</b></a> - \n";
	} else {
		print "<center>";
	}

	print "<a href=\"/cgi-bin/bridge.pl?action=beginTaxonInfo\">".
		  "<b>Get info on another taxon</b></a></center></div>\n";

	print "</form><p>";
}

sub doNavBox {
    my $dbt = shift;
    my $q = shift;
    my $s = shift;
    my $in_list = shift;

    my $entered_name = $q->param('entered_name') || $q->param('taxon_name');
    my $entered_no = $q->param('taxon_no');
	# Write out a hidden with the 'genus_name' and 'taxon_rank' for subsequent
	# hits to this page
	print "<form name=\"module_nav_form\" method=\"POST\" action=\"bridge.pl\">";
    print "<input type=hidden name=\"action\" value=\"displayTaxonInfoResults\">";
    print "<input type=hidden name=\"taxon_name\" value=\"".$entered_name."\">";
    print "<input type=hidden name=\"taxon_no\" value=\"".$entered_no."\">";
	# Now, the checkboxes and submit button, 
	my @modules_to_display = $q->param('modules');
	my %module_num_to_name = (1 => "classification",
							  2 => "related taxa",
							  3 => "taxonomic history",
							  4 => "synonymy",
							  5 => "ecology/taphonomy",
							  6 => "measurements",
							  7 => "map",
							  8 => "age range/collections");

	# if the modules are known and the user is not a guest,
	#  set the module preferences in the person table
	# if the modules are not known try to pull them from the person table
	# of course, we don't have to reset the preferences in that case
	# JA 21.10.04
	if ( $s->isDBMember()) {
		if ( @modules_to_display )	{
			my $pref = join ' ', @modules_to_display;
			my $prefsql = "UPDATE person SET taxon_info_modules='$pref',last_action=last_action WHERE person_no='" . $s->get("enterer_no") . "'";
			$dbt->getData($prefsql);
		}
		elsif ( ! @modules_to_display )	{
			my $prefsql = "SELECT taxon_info_modules FROM person WHERE person_no=".$s->get("enterer_no");
			$pref = ${$dbt->getData($prefsql)}[0]->{'taxon_info_modules'};
			@modules_to_display = split / /,$pref;
		}
	}

	# if that didn't work, set the default
	if ( ! @modules_to_display )	{
		@modules_to_display = (1,2,3,4);
	}
	
	# Put in order
	@modules_to_display = sort {$a <=> $b} @modules_to_display;

	# First module has the checkboxes on the side.
	print "<table class=\"navtable\" cellspacing=0 cellpadding=0>".
          "<tr><td valign=\"top\" align=\"center\"><b><div class=\"large\">Display</div></b></td></tr>";
	
	foreach my $key (sort keys %module_num_to_name){
	    print "<tr><td align=left valign=top nowrap>";
		print "<input type=checkbox name=modules value=$key";
		foreach my $checked (@modules_to_display){
			if($key == $checked){
				print " checked";
				last;
			}
		}
		print ">$module_num_to_name{$key}";
        print "</td></tr>";
	}

	# image thumbs:
	require Images;

	my @thumbs = Images::processViewImages($dbt, $q, $s, $in_list);
	my @selected_images = $q->param('image_thumbs');
	foreach my $thumb (@thumbs){
        print "<tr><td>";
		print "<input type=checkbox name=image_thumbs value=";
		print $thumb->{image_no};
		foreach my $image_num (@selected_images){
			if($image_num == $thumb->{image_no}){
				print " checked";
				last;
			}
		}
		print ">";
		my $thumb_path = $thumb->{path_to_image};
		$thumb_path =~ s/(.*)?(\d+)(.*)$/$1$2_thumb$3/;
		print "<img align=middle src=\"$thumb_path\" border=1 vspace=3>";
        print "</td></tr>";
	}

    print "<tr><td align=\"center\"><br><input type=submit value=\"update\"></td></tr>";
    print "</table>";

    return (\@modules_to_display,\@thumbs);
} 

sub doModules{
	my $dbt = shift;
	my $dbh = shift;
	my $q = shift;
	my $s = shift;
	my $exec_url = shift;
	my $module = shift;
	my $genus = shift;
	my $species = shift;
	my $in_list = shift;
	my $taxon_no = shift;

	
	# If $q->param("taxon_name") has a space, it's a "Genus species" combo,
	# otherwise it's a "Higher taxon."
	#($genus, $species) = split /\s+/, $q->param("taxon_name");

	# classification
	if($module == 1){
		print "<table>".
			  "<tr><td align=\"middle\"><h3>Classification</h3></td></tr>".
			  "<tr><td valign=\"top\" align=\"middle\">";

		print displayTaxonClassification($dbt, $genus, $species, $taxon_no);
		print "</td></tr></table>";

	}
	# sister and child taxa
	elsif($module == 2){
		print "<table>".
			  "<tr><td align=\"middle\"><h3>Related taxa</h3></td></tr>".
			  "<tr><td valign=\"top\" align=\"middle\">";

		print displayRelatedTaxa($dbt, $genus, $species, $taxon_no);
		print "</td></tr></table>";

	}
	# synonymy
	elsif($module == 3){
        if ($taxon_no) {
    		print displayTaxonSynonymy($dbt, $genus, $species, $taxon_no);
        } else {
            print "<table>".
                  "<tr><td align=\"middle\"><h3>Taxonomic history</h3></td></tr>".
                  "<tr><td valign=\"top\" align=\"middle\">".
                  "<i>No taxonomic history data are available</i>".
                  "</td></tr></table>\n";
        }
	}
	elsif ( $module == 4 )	{
        if ($taxon_no) {
    		print displaySynonymyList($dbt, $q, $genus, $species, $taxon_no);
        } else {
            print "<table width=\"100%\">".
                  "<tr><td align=\"middle\"><h3>Synonymy</h3></td></tr>".
                  "<tr><td valign=\"top\" align=\"middle\">".
                  "<i>No synonymy data are available</i>".
                  "</td></tr></table>\n";
        }
	}
	# ecology
	elsif ( $module == 5 )	{
		print displayEcology($dbt,$taxon_no,$genus,$species);
    }
    # specimen measurements
    elsif ($module == 6) {
		print displayMeasurements($dbt,$taxon_no,$genus,$species,$in_list);
	}
	# map
	elsif($module == 7){
		print "<center><table><tr><td align=\"middle\"><h3>Distribution</h3></td></tr>".
			  "<tr><td align=\"middle\" valign=\"top\">";
		# MAP USES $q->param("taxon_name") to determine what it's doing.
		my $map_html_path = doMap($dbh, $dbt, $q, $s, $in_list);
		if ( $map_html_path )	{
			if($map_html_path =~ /^\/public/){
				# reconstruct the full path the image.
				$map_html_path = $ENV{DOCUMENT_ROOT}.$map_html_path;
			}
			open(MAP, $map_html_path) or die "couldn't open $map_html_path ($!)";
			while(<MAP>){
				print;
			}
			close MAP;
		} else {
		    print "<i>No distribution data are available</i>";
        }
		print "</td></tr></table></center>";
		# trim the path down beyond apache's root so we don't have a full
		# server path in our html.
		if ( $map_html_path )	{
			$map_html_path =~ s/.*?(\/public.*)/$1/;
			print "<input type=hidden name=\"map_num\" value=\"$map_html_path\">";
		}
	}
	# collections
	elsif($module == 8){
		print doCollections($exec_url, $q, $dbt, $dbh, $in_list);
	}
}


# PASS this a reference to the collection list array and it
# should figure out the min/max/center lat/lon 
# RETURNS an array of parameters (see end of routine for order)
# written 12/11/2003 by rjp.
sub calculateCollectionBounds {
	my $collections = shift;  #collections to plot

	# calculate the min and max latitude and 
	# longitude with 1 degree resolution
	my $latMin = 360;
	my $latMax = -360;
	my $lonMin = 360;
	my $lonMax = -360;

	foreach (@$collections) {
		%coll = %$_;

		# note, this is *assuming* that latdeg and lngdeg are 
		# always populated, even if the user set the lat/lon with 
		# decimal degrees instead.  So if this isn't the case, then
		# we need to check both of them.  
		my $latDeg = $coll{'latdeg'};
		if ($coll{'latdir'} eq "South") {
			$latDeg = -1*$latDeg;
		}

		my $lonDeg = $coll{'lngdeg'};
		if ($coll{'lngdir'} eq "West") {
			$lonDeg = -1* $lonDeg;
		}

		#print "lat = $latDeg<BR>";
		#print "lon = $lonDeg<BR>";

		if ($latDeg > $latMax) { $latMax = $latDeg; }
		if ($latDeg < $latMin) { $latMin = $latDeg; }
		if ($lonDeg > $lonMax) { $lonMax = $lonDeg; }
		if ($lonDeg < $lonMin) { $lonMin = $lonDeg; }
	}

    # If its spread out over more than 75% of the earth, than just zoom out fully and center the map
    if (abs($lonMax - $lonMin) >= 270) {
	    $latCenter = 0;
	    $lonCenter = 0;
        $lonMin= -180;
        $lonMax = 180;
        $latMin = -90;
        $latMax = 90;
    } else {
	    $latCenter = (($latMax - $latMin)/2) + $latMin;
	    $lonCenter = (($lonMax - $lonMin)/2) + $lonMin;
    }

	#print "latCenter = $latCenter<BR>";
	#print "lonCenter = $lonCenter<BR>";
	#print "latMin = $latMin<BR>";
	#print "latMax = $latMax<BR>";
	#print "lonMin = $lonMin<BR>";
	#print "lonMax = $lonMax<BR>";

	return ($latCenter, $lonCenter, $latMin, $latMax, $lonMin, $lonMax);
}





sub doMap{
	my $dbh = shift;
	my $dbt = shift;
	my $q = shift;
	my $s = shift;
	my $in_list = shift;
	my $map_num = $q->param('map_num');

	if($q->param('map_num')){
		return $q->param('map_num');
	}

	$q->param(-name=>"taxon_info_script",-value=>"yes");
	my @map_params = ('projection', 'maptime', 'mapbgcolor', 'gridsize', 'gridcolor', 'coastlinecolor', 'borderlinecolor', 'usalinecolor', 'pointshape1', 'dotcolor1', 'dotborder1');
	my %user_prefs = main::getPreferences($s->get('enterer_no'));
	foreach my $pref (@map_params){
		if($user_prefs{$pref}){
			$q->param($pref => $user_prefs{$pref});
		}
	}
	# Not covered by prefs:
	if(!$q->param('pointshape1')){
		$q->param('pointshape1' => 'circles');
	}
	if(!$q->param('dotcolor1')){
		$q->param('dotcolor1' => 'red');
	}
	if(!$q->param('coastlinecolor')){
		$q->param('coastlinecolor' => 'black');
	}
	$q->param('mapresolution'=>'medium');

	# note, we need to leave this in here even though it's 
	# redunant (since we scale below).. taking it out will
	# cause a division by zero error in Map.pm.
	$q->param('mapscale'=>'X 1');


	$q->param('pointsize1'=>'tiny');

	if(!$q->param('projection') or $q->param('projection') eq ""){
		$q->param('projection'=>'rectilinear');
	}

	require Map;
	my $m = Map->new( $dbh, $q, $s, $dbt );
	my $dataRowsRef = $m->buildMapOnly($in_list);

	if(scalar(@{$dataRowsRef}) > 0) {
		# this section added by rjp on 12/11/2003
		# at this point, we need to figure out the bounds 
		# of the collections and the center point.  
		my @bounds = calculateCollectionBounds($dataRowsRef);

		$q->param('maplat' => shift(@bounds));
		$q->param('maplng' => shift(@bounds));

		# note, we must constrain the map size to be in a ratio
		# of 360 wide by 180 high, so figure out what ratio to use
		my $latMin = shift(@bounds);	my $latMax = shift(@bounds);
		my $lonMin = shift(@bounds);	my $lonMax = shift(@bounds);

		my $latWidth = abs($latMax - $latMin);
		my $lonWidth = abs($lonMax - $lonMin);

		my $scale = 8;  # default scale value
		if (not (($latWidth == 0) and ($lonWidth == 0))) {
			# only do this if they're not both zero...
		
			if ($latWidth == 0) { $latWidth = 1; } #to prevent divide by zero
			if ($lonWidth == 0) { $lonWidth = 1; }
		
			# multiply by 0.9 to give a slight boundary around the zoom.
			my $latRatio = (0.9 * 180) / $latWidth;
			my $lonRatio = (0.9 * 360) / $lonWidth;

			#print "latRatio = $latRatio\n";
			#print "lonRatio = $lonRatio\n";

			if ($latRatio < $lonRatio) {
				$scale = $latRatio;
			} else { 
				$scale = $lonRatio;
			}
		}

		if ($scale > 8) { $scale = 8; } # don't let it zoom too far in!
		$q->param('mapscale' => "X $scale");
		

		# note, we have already set $q in the map object,
		# so we have to set it again with the new values.
		# this is not the ideal way to do it, so perhaps change
		# this at a future date.
		$m->setQAndUpdateScale($q);
		
	
		# now actually draw the map
		return $m->drawMapOnly($dataRowsRef);
	}  else {
        return;
	}
}



sub doCollections{
	my $exec_url = shift;
	my $q = shift;
	my $dbt = shift;
	my $dbh = shift;
	my $in_list = shift;
    # age_range_format changes appearance html formatting of age/range information
    # used by the strata module
    my $age_range_format = shift;  
	my $output = "";

	# get a lookup of the boundary ages for all intervals JA 25.6.04
	# the boundary age hashes are keyed by interval nos
    @_ = TimeLookup::findBoundaries($dbh,$dbt);
    my %upperbound = %{$_[0]};
    my %lowerbound = %{$_[1]};

	# get all the interval names because we need them to print the
	#  total age range below
	my $isql = "SELECT interval_no,eml_interval,interval_name FROM intervals";
	my @intrefs =  @{$dbt->getData($isql)};
	my %interval_name;
	for my $ir ( @intrefs )	{
		$interval_name{$ir->{'interval_no'}} = $ir->{'interval_name'};
		if ( $ir->{'eml_interval'} )	{
			$interval_name{$ir->{'interval_no'}} = $ir->{'eml_interval'} . " " . $ir->{'interval_name'};
		}
	}

    # Pull the colls from the DB;
    my %options = ();
    $options{'permission_type'} = 'read';
    $options{'most_recent'} = 1;
    $options{'calling_script'} = "TaxonInfo";
    $options{'taxon_list'} = $in_list if (@$in_list);
    # This field passed from strata module
    $options{'group_formation_member'} = $q->param('group_formation_member') if (defined($q->param('group_formation_member')));
    my $fields = ["country", "state", "max_interval_no", "min_interval_no"];  
    my ($dataRows,$ofRows) = main::processCollectionsSearch($dbt,\%options,$fields);
    my @data = @$dataRows;

	# figure out which intervals are too vague to use to set limits on
	#  the joint upper and lower boundaries
	# "vague" means there's some other interval falling entirely within
	#  this one JA 26.1.05
	my %seeninterval;
	my %toovague;
	for my $row ( @data )	{
		if ( ! $seeninterval{$row->{'max_interval_no'}." ".$row->{'min_interval_no'}} )	{
			$max1 = $lowerbound{$row->{'max_interval_no'}};
			$min1 = $upperbound{$row->{'min_interval_no'}};
			if ( $min1 == 0 )	{
				$min1 = $upperbound{$row->{'max_interval_no'}};
			}
			for $intervalkey ( keys %seeninterval )	{
				my ($maxno,$minno) = split / /,$intervalkey;
				$max2 = $lowerbound{$maxno};
				$min2 = $upperbound{$minno};
				if ( $min2 == 0 )	{
					$min2 = $upperbound{$maxno};
				}
				if ( $max1 < $max2 && $max1 > 0 && $min1 > $min2 && $min2 > 0 )	{
					$toovague{$intervalkey}++;
				}
				elsif ( $max1 > $max2 && $max2 > 0 && $min1 < $min2 && $min1 > 0 )	{
					$toovague{$row->{'max_interval_no'}." ".$row->{'min_interval_no'}}++;
				}
			}
			$seeninterval{$row->{'max_interval_no'}." ".$row->{'min_interval_no'}}++;
		}
	}

	# Process the data:  group all the collection numbers with the same
	# time-place string together as a hash.
	%time_place_coll = ();
	my $oldestlowerbound;
	my $oldestlowername;
	my $youngestupperbound = 9999;
	my $youngestuppername;
	foreach my $row (@data){
        my $interval1 = $interval_name{$row->{'max_interval_no'}};
        my $interval2 = $interval_name{$row->{'min_interval_no'}};
        my $res = "<span class=\"tiny\">$interval1";
        if ( $interval2 && $row->{'max_interval_no'} != $row->{'min_interval_no'}) {
            $res .= " - " . $interval2;
        }
        $res .= "</span></td><td align=\"middle\" valign=\"top\"><span class=\"tiny\">";

        $row->{"country"} =~ s/ /&nbsp;/;
        $res .= $row->{"country"};
        if($row->{"state"}){
            $row->{"state"} =~ s/ /&nbsp;/;
            $res .= " (" . $row->{"state"} . ")";
        }
        $res .= "</span>\n";

	    if(exists $time_place_coll{$res}){
			push(@{$time_place_coll{$res}}, $row->{"collection_no"});
	    }
	    else{
			$time_place_coll{$res} = [$row->{"collection_no"}];
			push(@order,$res);
            # create a hash array where the keys are the time-place strings
            #  and each value is a number recording the min and max
            #  boundary estimates for the temporal bins JA 25.6.04
            # this is kind of tricky because we want bigger bins to come
            #  before the bins they include, so the second part of the
            #  number recording the upper boundary has to be reversed
            my $upper = $upperbound{$row->{'max_interval_no'}};
            $max_interval_name{$res} = $interval_name{$row->{'max_interval_no'}};
            $min_interval_name{$res} = $max_interval_name{$res};
            if ( $row->{'max_interval_no'} != $row->{'min_interval_no'} &&
                $row->{'min_interval_no'} > 0 )	{
                $upper = $upperbound{$row->{'min_interval_no'}};
                $min_interval_name{$res} = $interval_name{$row->{'min_interval_no'}};
            }
            # also store the overall lower and upper bounds for
            #  printing below JA 26.1.05
            # don't do this if the interval is too vague (see above)
            if ( ! $toovague{$row->{'max_interval_no'}." ".$row->{'min_interval_no'}} )	{
                if ( $lowerbound{$row->{'max_interval_no'}} > $oldestlowerbound )	{
                    $oldestlowerbound = $lowerbound{$row->{'max_interval_no'}};
                    $oldestlowername = $max_interval_name{$res};
                }
                if ( $upper < $youngestupperbound )	{
                    $youngestupperbound = $upper;
                    $youngestuppername = $min_interval_name{$res};
                }
            }
            # WARNING: we're assuming upper boundary ages will never be
            #  greater than 999 million years
            # also, we're just going to ignore fractions of m.y. estimates
            #  because those would screw up the sort below
            $upper = int($upper);
            $upper = 999 - $upper;
            if ( $upper < 10 )	{
                $upper = "00" . $upper;
            } elsif ( $upper < 100 )	{
                $upper = "0" . $upper;
            }
            $bounds_coll{$res} = int($lowerbound{$row->{'max_interval_no'}}) . $upper;
	    }
	}

	# a little cleanup
	$oldestlowerbound =~ s/00$//;
	$youngestupperbound =~ s/00$//;


	# sort the time-place strings temporally or by geographic location
	my @sorted = sort { $bounds_coll{$b} <=> $bounds_coll{$a} || $a cmp $b } keys %bounds_coll;

	# legacy: originally the sorting was just on the key
#	my @sorted = sort (keys %time_place_coll);

	if(scalar @sorted > 0){
		# Do this locally because the module never gets exec_url
		#   from bridge.pl
		my $exec_url = $q->url();

		# print the first and last appearance (i.e., the age range)
		#  JA 25.6.04
		if ($age_range_format eq "for_strata_module") {
			$output .= "<p><b>Age range:</b> ";
			$output .= $oldestlowername;
			if ( $oldestlowername ne $youngestuppername )	{
				$output .= " to " . $youngestuppername;
			}
			$output .= " <i>or</i> " . $oldestlowerbound . " to " . $youngestupperbound . " Ma";
			$output .= "</p><br>\n<hr>\n";
		} else {
			$output .= "<center><h3>Age range</h3>\n";
			$output .= $oldestlowername;
			if ( $oldestlowername ne $youngestuppername )	{
	 			$output .= " to " . $youngestuppername;
			}
			$output .= " <i>or</i> " . $oldestlowerbound . " to " . $youngestupperbound . " Ma";
			$output .= "<center><p>\n<hr>\n";
		}

		$output .= "<center><h3>Collections</h3></center>\n";

		$output .= "<table width=\"100%\"><tr>";
		$output .= "<th align=\"middle\">Time interval</th>";
		$output .= "<th align=\"middle\">Country or state</th>";
		$output .= "<th align=\"left\">PBDB collection number</th></tr>";
		my $row_color = 0;
		foreach my $key (@sorted){
			if($row_color % 2 == 0){
				$output .= "<tr class='darkList'>";
			} 
			else{
				$output .= "<tr>";
			}
			$output .= "<td align=\"middle\" valign=\"top\">".
				  "<span class=tiny>$key</span></td><td align=\"left\">";
			foreach  my $collection_no (@{$time_place_coll{$key}}){
				$output .= "<a href=\"$exec_url?action=displayCollectionDetails&collection_no=$collection_no\">$collection_no</a> ";
			}
			$output .= "</td></tr>\n";
			$row_color++;
		}
		$output .= "</table>";
	} 
	return $output;
}



## displayTaxonClassification
#
# SEND IN GENUS OR HIGHER TO GENUS_NAME, ONLY SET SPECIES IF THERE'S A SPECIES.
##
sub displayTaxonClassification{
	my $dbt = shift;
    my $dbh = $dbt->dbh;
	my $genus = (shift or "");
	my $species = (shift or "");
	my $orig_no = (shift or ""); #Pass in original combination no

    my $output; # the html actually returned by the function

    my ($child_spelling_no,$taxon_no,$taxon_name,$taxon_rank,$genus_no);
    if ($orig_no) {
        my $correct_row = getMostRecentParentOpinion($dbt,$orig_no,1);
        # First, find the rank, name, of the focal taxon
        if ($correct_row) {
            $child_spelling_no = $correct_row->{'child_spelling_no'};    
            $taxon_name = $correct_row->{'child_name'};    
            $taxon_rank = $correct_row->{'child_rank'};    
        } else {
            my $t = getTaxon($dbt,'taxon_no'=>$orig_no);
            $child_spelling_no = $t->{'taxon_no'};
            $taxon_name = $t->{'taxon_name'};    
            $taxon_rank = $t->{'taxon_rank'};    
        }
        if (!$genus) {
            ($genus,$species) = split(/ /,$taxon_name);
        }
    } elsif ($genus && $species) {
        # Theres one case where we might want to do upward classification when theres no taxon_no:
        #  The Genus+species isn't in authorities, but the genus is
        my @results = getTaxon($dbt,'taxon_name'=>$genus);
        if (@results == 1) {
            $child_spelling_no = $results[0]->{'taxon_no'};
            $taxon_name = $results[0]->{'taxon_name'};    
            $taxon_rank = $results[0]->{'taxon_rank'};    
            $genus_no=$results[0]->{'taxon_no'}; 
            $append_species=1;
        }
    }

    #
    # Do the classification
    #
    #my ($taxon_no,$taxon_rank,$taxon_name,$child_spelling_no);
    my @table_rows = ();
    if ($orig_no || $genus_no) {
        # format of table_rows: taxon_rank,taxon_name,taxon_no(original combination),taxon_no(recombination, if recombined)
        # This will happen if the genus has a taxon_no but not the species
        if ($append_species) {
            push @table_rows, ['species',"$genus $species",0,0];
        }

        # Is the classification based on the taxon itself, or is a species thats classified using the genus?
        my $classify_no = ($orig_no) ? $orig_no : $genus_no;
        push @table_rows, [$taxon_rank,$taxon_name,$classify_no,$child_spelling_no];

        # Now find the rank,name, and publication of all its parents
        my $parent_hash = Classification::get_classification_hash($dbt,'all',[$classify_no],'array');
        my $parent_array = $parent_hash->{$classify_no}; 
        if (@$parent_array) {
            foreach $row (@$parent_array) {
                push (@table_rows,[$row->{'taxon_rank'},$row->{'taxon_name'},$row->{'taxon_no'},$row->{'taxon_spelling_no'}]);
                last if ($row->{'taxon_rank'} eq 'kingdom');
            }

            #
            # Print out the table in the reverse order that we initially made it
            #
            $output .= "<table><tr valign=top><th>Rank</th><th>Name</th><th>Author</th></tr>";
            my $class = '';
            for($i = scalar(@table_rows)-1;$i>=0;$i--) {
                $class = $class eq '' ? 'class="darkList"' : '';
                $output .= "<tr $class>";
                my($taxon_rank,$taxon_name,$taxon_no,$child_spelling_no) = @{$table_rows[$i]};
                if ($taxon_rank =~ /species/) {
                    @taxon_name = split(/\s+/,$taxon_name);
                    $taxon_name = $taxon_name[-1];
                }
                my %auth_yr;
                if ($taxon_no) {
                    %auth_yr = %{PBDBUtil::authorAndPubyrFromTaxonNo($dbt,$taxon_no)}
                }
                my $pub_info = $auth_yr{author1last}.' '.$auth_yr{pubyr};
                if ($child_spelling_no != $taxon_no) {
                    $pub_info = "(".$pub_info.")" if $pub_info !~ /^\s*$/;
                } 
                if ($taxon_no) {
                    #if ($species !~ /^sp(\.)*$|^indet(\.)*$/) 
                    $link = qq|<a href="/cgi-bin/bridge.pl?action=checkTaxonInfo&taxon_no=$taxon_no">$taxon_name</a>|;
                } else {
                    my $show_rank = ($taxon_rank eq 'species') ? 'Genus and species' : 
                                    ($taxon_rank eq 'genus')   ? 'Genus' : 
                                                                 'Higher taxon'; 
                    $link = qq|<a href="/cgi-bin/bridge.pl?action=checkTaxonInfo&taxon_name=$table_rows[$i][1]&taxon_rank=$show_rank">$taxon_name</a>|;
                }
                $output .= qq|<td align="middle">$taxon_rank</td>|.
                           qq|<td align="middle">$link</td>|.
                           qq|<td align="middle" style="white-space: nowrap">$pub_info</td>|; 
                $output .= '</tr>';
            }
            $output .= "</table>";
        } else {
            $output .= "<i>No classification data are available</i>";
        }
    } else {
        $output .= "<i>No classification data are available</i>";
    }

    return $output;
}

# Separated out from classification section PS 09/22/2005
sub displayRelatedTaxa {
	my $dbt = shift;
    my $dbh = $dbt->dbh;
	my $genus = (shift or "");
	my $species = (shift or "");
	my $orig_no = (shift or ""); #Pass in original combination no

    #
    # Begin getting sister/child taxa
    # PS 01/20/2004 - rewrite: Use getChildren function
    # First get the children
    #
    my $focal_taxon_no   = $orig_no;
    my ($focal_taxon_rank,$parent_taxon_no);
    if ($orig_no) {
        my $taxon = getTaxon($dbt,'taxon_no'=>$orig_no);
        $focal_taxon_rank = $taxon->{'taxon_rank'};
    } elsif ($genus && $species) {
        $focal_taxon_rank = 'species';
    }

    if ($orig_no) {
        my $class_hash = Classification::get_classification_hash($dbt,'parent',[$orig_no],'numbers');
        if ($class_hash->{$orig_no}) {
            $parent_taxon_no = $class_hash->{$orig_no};
        }
    } else {
        my @genus_nos = getTaxonNos($dbt,$genus,'genus');
        if ($genus && scalar(@genus_nos) <= 1) {
            $parent_taxon_no=$genus_nos[0];
        }
    }

    my $taxon_records = [];
    my @child_taxa_links;
    # This section generates links for children if we have a taxon_no (in authorities table)
    $taxon_records = PBDBUtil::getChildren($dbt,$focal_taxon_no,1,'sort_alphabetical') if ($focal_taxon_no);
    if (@{$taxon_records}) {
        my $sql = "SELECT type_taxon_no FROM authorities WHERE taxon_no=$focal_taxon_no";
        my $type_taxon_no = ${$dbt->getData($sql)}[0]->{'type_taxon_no'};
        foreach $record (@{$taxon_records}) {
            my @syn_links;                                                         
            my @synonyms = @{$record->{'synonyms'}};
            push @syn_links, $_->{'taxon_name'} for @synonyms;
            my $link = qq|<a href="bridge.pl?action=checkTaxonInfo&taxon_no=$record->{taxon_no}">$record->{taxon_name}|;
            $link .= " (syn. ".join(", ",@syn_links).")" if @syn_links;
            $link .= "</a>";
            if ($type_taxon_no && $type_taxon_no == $record->{'taxon_no'}) {
                $link .= " <small>(type $record->{taxon_rank})</small>";
            }
            push @child_taxa_links, $link;
        }
    }    

    # Get sister taxa as well
    # PS 01/20/2004
    $taxon_records = [];
    my @sister_taxa_links;
    # This section generates links for sister if we have a taxon_no (in authorities table)
    $taxon_records = PBDBUtil::getChildren($dbt,$parent_taxon_no,1,1) if ($parent_taxon_no);
    if (@{$taxon_records}) {
        foreach $record (@{$taxon_records}) {
            next if ($record->{'taxon_no'} == $orig_no);
            if ($focal_taxon_rank ne $record->{'taxon_rank'}) {
                PBDBUtil::debug(1,"rank mismatch $focal_taxon_rank -- $record->{taxon_rank} for sister $record->{taxon_name}");
            } else {
                my @syn_links;
                my @synonyms = @{$record->{'synonyms'}};
                push @syn_links, $_->{'taxon_name'} for @synonyms;
                my $link = qq|<a href="bridge.pl?action=checkTaxonInfo&taxon_no=$record->{taxon_no}">$record->{taxon_name}|;
                $link .= " (syn. ".join(", ",@syn_links).")" if @syn_links;
                $link .= "</a>";
                push @sister_taxa_links, $link;
            }
        }
    }
    # This generates links if all we have is occurrences records
    my @genus_nos = getTaxonNos($dbt,$genus,'genus');
    my (@possible_sister_taxa_links,@possible_child_taxa_links);
    if ($genus && scalar(@genus_nos) <= 1) {
        my ($sql,$whereClause,@results);
        my $genus_sql = $dbh->quote($genus);
        my ($occ_genus_no_sql,$reid_genus_no_sql);
        $occ_genus_no_sql = " OR o.taxon_no=$genus_nos[0]" if (@genus_nos);
        $reid_genus_no_sql = " OR re.taxon_no=$genus_nos[0]" if (@genus_nos);
        $sql  = "(SELECT o.genus_name,o.species_name FROM occurrences o LEFT JOIN reidentifications re ON re.occurrence_no=o.occurrence_no WHERE re.reid_no IS NULL AND o.genus_name like $genus_sql AND (o.taxon_no=0 OR o.taxon_no IS NULL $occ_genus_no_sql))";
        $sql .= " UNION ";
        $sql .= "(SELECT re.genus_name,re.species_name FROM occurrences o, reidentifications re WHERE re.occurrence_no=o.occurrence_no AND re.most_recent=1 AND re.genus_name like $genus_sql AND (re.taxon_no=0 OR re.taxon_no IS NULL $reid_genus_no_sql))"; 
        $sql .= "ORDER BY genus_name,species_name";
        main::dbg("Get from occ table: $sql");
        @results = @{$dbt->getData($sql)};
        foreach $row (@results) {
            next if ($row->{'species_name'} =~ /^sp(p)*\.|^indet\.|s\.\s*l\./);
            if ($species) {
                if ($species ne $row->{'species_name'}) {
                    my $link = qq|<a href="bridge.pl?action=checkTaxonInfo&taxon_name=$row->{genus_name} $row->{species_name}">$row->{genus_name} $row->{species_name}</a>|;
                    push @possible_sister_taxa_links, $link;
                }
            } else {
                my $link = qq|<a href="bridge.pl?action=checkTaxonInfo&taxon_name=$row->{genus_name} $row->{species_name}">$row->{genus_name} $row->{species_name}</a>|;
                push @possible_child_taxa_links, $link;
            }
        }
    }
   
    # Print em out
    if (@child_taxa_links) {
        $output .= "<p><i>This taxon includes:</i><br>"; 
        $output .= "<small>" if (scalar(@child_taxa_links)>10);
        $output .= join(", ",@child_taxa_links);
        $output .= "</small>" if (scalar(@child_taxa_links)>10);
        $output .= "</p>";
    }

    if (@possible_child_taxa_links) {
        $output .= "<p><i>This genus may include these species, but they have not been formally classified into it:</i><br>"; 
        $output .= "<small>" if (scalar(@possible_child_taxa_links)>10);
        $output .= join(", ",@possible_child_taxa_links);
        $output .= "</small>" if (scalar(@possible_child_taxa_links)>10);
        $output .= "</p>";
    }

    if (@sister_taxa_links) {
        my $rank = ($focal_taxon_rank eq 'species') ? 'species' :
                   ($focal_taxon_rank eq 'genus') ? 'genera' :
                                                    'taxa';
        $output .= "<p><i>Sister $rank include:</i><br>"; 
        $output .= "<small>" if (scalar(@sister_taxa_links)>10);
        $output .= join(", ",@sister_taxa_links);
        $output .= "</small>" if (scalar(@sister_taxa_links)>10);
        $output .= "</p>";
    }
    
    if (@possible_sister_taxa_links) {
        $output .= "<p><i>These species have not been formally classified into the genus:</i><br>"; 
        $output .= "<small>" if (scalar(@possible_sister_taxa_links)>10);
        $output .= join(", ",@possible_sister_taxa_links);
        $output .= "</small>" if (scalar(@possible_sister_taxa_links)>10);
        $output .= "</p>";
    }
	return $output;
}

# Handle the 'Taxonomic history' section
sub displayTaxonSynonymy{
	my $dbt = shift;
	my $genus = (shift or "");
	my $species = (shift or "");
	my $taxon_no = (shift or "");
	
	my $output = "";  # html output...
	
	$output .= "<center><h3>Taxonomic history</h3></center>";

	my $sql = "SELECT taxon_no, reference_no, author1last, pubyr, ".
			  "ref_is_authority FROM authorities ".
			  "WHERE taxon_no=$taxon_no";
	my @results = @{$dbt->getData($sql)};
	
	unless($taxon_no) {
		return ($output .= "<i>No taxonomic history is available for $genus $species.</i><br>");
	}

	$output .= "<ul>";
	my $original_combination_no = getOriginalCombination($dbt, $taxon_no);
	
	# Select all parents of the original combination whose status' are
	# either 'recombined as,' 'corrected as,' or 'rank changed as'
	$sql = "SELECT DISTINCT(child_spelling_no), status FROM opinions ".
		   "WHERE child_no=$original_combination_no ".	
		   "AND (status='recombined as' OR status='corrected as' OR status='rank changed as')";
	@results = @{$dbt->getData($sql)};

	# Combine parent numbers from above for the next select below. If nothing
	# was returned from above, use the original combination number.
	my @parent_list = ();
    foreach my $rec (@results) {
        push(@parent_list,$rec->{'child_spelling_no'});
    }
    # don't forget the original (verified) here, either: the focal taxon	
    # should be one of its children so it will be included below.
    push(@parent_list, $original_combination_no);

	# Select all synonymies for the above list of taxa.
	$sql = "SELECT DISTINCT(child_no), status FROM opinions ".
		   "WHERE parent_no IN (".join(',',@parent_list).") ".
		   "AND status IN ('subjective synonym of','objective synonym of','homonym of','replaced by')";
	@results = @{$dbt->getData($sql)};

	# Reduce these results to original combinations:
	foreach my $rec (@results) {
		$rec = getOriginalCombination($dbt, $rec->{child_no});	
	}

	# NOTE: "corrected as" could also occur at higher taxonomic levels.

    # Remove duplicates
    my %results_no_dupes;
    @results_no_dupes{@results} = ();
    @results = keys %results_no_dupes;

	# Get synonymies for all of these original combinations
	foreach my $child (@results) {
		my $list_item = getSynonymyParagraph($dbt, $child);
		push(@paragraphs, "<br><br>$list_item\n") if($list_item ne "");
	}
	
	
	# Print the info for the original combination of the passed in taxon first.
	$output .= getSynonymyParagraph($dbt, $original_combination_no);

	# Now alphabetize the rest:
	@paragraphs = sort {lc($a) cmp lc($b)} @paragraphs;
	foreach my $rec (@paragraphs) {
		$output .= $rec;
	}

	$output .= "</ul>";
	return $output;
}


# updated by rjp, 1/22/2004
# gets paragraph displayed in places like the
# taxonomic history, for example, if you search for a particular taxon
# and then check the taxonomic history box at the left.
#
sub getSynonymyParagraph{
	my $dbt = shift;
	my $taxon_no = shift;
	
	my %synmap = ( 'recombined as' => 'recombined as ',
				   'replaced by' => 'replaced with ',
				   'corrected as' => 'corrected as ',
				   'rank changed as' => 'changed to another rank and altered to ',
				   'belongs to' => 'revalidated ',
				   'nomen dubium' => 'considered a nomen dubium ',
				   'nomen nudum' => 'considered a nomen nudum ',
				   'nomen vanum' => 'considered a nomen vanum ',
				   'nomen oblitum' => 'considered a nomen oblitum ',
				   'subjective synonym of' => 'synonymized subjectively with ',
				   'objective synonym of' => 'synonymized objectively with ');
	my $text = "";

	# "Named by" part first:
	# Need to print out "[taxon_name] was named by [author] ([pubyr])".
	# - select taxon_name, author1last, pubyr, reference_no, comments from authorities
	
	my $sql = "SELECT taxon_name, author1last, pubyr, reference_no, comments, ".
			  "ref_is_authority FROM authorities WHERE taxon_no=$taxon_no";
	my @auth_rec = @{$dbt->getData($sql)};
	
	# Get ref info from refs if 'ref_is_authority' is set
	if($auth_rec[0]->{ref_is_authority} =~ /YES/i){
		# If we didn't get an author and pubyr and also didn't get a 
		# reference_no, we're at a wall and can go no further.
		if(!$auth_rec[0]->{reference_no}){
			$text .= "Cannot determine taxonomic history for ".
					 $auth_rec[0]->{taxon_name}."<br>.";
			return $text;	
		}
		$sql = "SELECT author1last,author2last,otherauthors,pubyr,comments FROM refs ".
			   "WHERE reference_no=".$auth_rec[0]->{reference_no};
		@results = @{$dbt->getData($sql)};
		
		$text .= "<li><i>".$auth_rec[0]->{taxon_name}."</i> was named by ".
        $text .= Reference::formatShortRef($results[0],'alt_pubyr'=>1,'show_comments'=>1);
	} elsif($auth_rec[0]->{author1last}){
    #	elsif($auth_rec[0]->{author1last} && $auth_rec[0]->{pubyr}){
		$text .= "<li><i>".$auth_rec[0]->{taxon_name}."</i> was named by ".
        $text .= Reference::formatShortRef($auth_rec[0],'alt_pubyr'=>1,'show_comments'=>1);
	}
	# if there's nothing from above, give up.
	else{
		$text .= "<li><i>The author of $auth_rec[0]->{taxon_rank} $auth_rec[0]->{taxon_name} is not known</i>";
	}

	# Get all things this taxon as been. Note we don't use ref_has_opinion but rather the 
    # pubyr cause of a number of broken records (ref_has_opinion is blank, but no pub info)
    # Transparently insert in the right pubyr and sort by it
    $sql = "(SELECT o.status,o.figures,o.pages, o.parent_no, o.parent_spelling_no, o.child_spelling_no,o.opinion_no, o.reference_no,".
           " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000',o.pubyr,r.pubyr) pubyr,".
           " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000',o.author1last,r.author1last) author1last,".
           " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000',o.author2last,r.author2last) author2last,".
           " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000',o.otherauthors,r.otherauthors) otherauthors".
           " FROM opinions o LEFT JOIN refs r ON o.reference_no=r.reference_no" . 
           " WHERE child_no=$taxon_no) ORDER BY pubyr";
	@results = @{$dbt->getData($sql)};

    # We want to group opinions together that have the same spelling/parent
    # We do this by creating a double array - $syns[$group_index][$child_index]
    # where all children having the same parent/spelling will have the same group index
    # the hashs %(syn|rc)_group_index keep track of what the $group_index is for each clump
    my (@syns,@nomens,%syn_group_index,%rc_group_index);
    my $list_revalidations = 0;
	# If something
	foreach my $row (@results){
		# put all syn's referring to the same taxon_name together
        if ($row->{'status'} =~ /corrected|rank changed|recombined/) {
            if (!exists $rc_group_index{$row->{'child_spelling_no'}}) {
                $rc_group_index{$row->{'child_spelling_no'}} = scalar(@syns);
            }
            $index = $rc_group_index{$row->{'child_spelling_no'}};
            push @{$syns[$index]},$row;
            $list_revalidations = 1;
        } elsif ($row->{'status'} =~ /synonym|homonym|replaced/) {
            if (!exists $syn_group_index{$row->{'parent_spelling_no'}}) {
                $syn_group_index{$row->{'parent_spelling_no'}} = scalar(@syns);
            }
            $index = $syn_group_index{$row->{'parent_spelling_no'}};
            push @{$syns[$index]},$row;
            $list_revalidations = 1;
        } elsif ($row->{'status'} =~ /nomen/) {
	        # Combine all adjacent like status types @nomens
	        # (They're chronological: nomen, reval, reval, nomen, nomen, reval, etc.)
            if (!@nomens) {
                $index = 0;
            } elsif ($nomens[$#nomens][0]->{'status'} eq $row->{'status'}) {
                $index = $#nomens;
            } else {
                $index = scalar(@nomens);
            }
            push @{$nomens[$index]},$row;
            $list_revalidations = 1;
        } elsif ($row->{'status'} =~ /belongs/ && $list_revalidations) {    
            # Belongs to's are only considered revalidations if they come
            # after a recombined as, synonym, or nomen *
            if (!@nomens) {
                $index = 0;
            } elsif ($nomens[$last_index][0]->{'status'} eq $row->{'status'}) {
                $index = $#nomens;
            } else {
                $index = scalar(@nomens);
            }
            push @{$nomens[$index]},$row;
        }
	}
   
    # Now combine the synonyms and nomen/revalidation arrays, with the nomen/revalidation coming last
    @synonyms = (@syns,@nomens);
	
	# Exception to above:  the most recent opinion should appear last. Splice it to the end
    if (@synonyms) {
        my $oldest_pubyr = 0;
        my $oldest_group = 0; 
        for($i=0;$i<scalar(@synonyms);$i++){
            my @group = @{$synonyms[$i]};
            if ($group[$#group]->{'pubyr'} > $oldest_pubyr) {
                $oldest_group = $i; 
                $oldest_pubyr = $group[$#group]->{'pubyr'};
            }
        }
        $most_recent_group = splice(@synonyms,$oldest_group,1);
        push @synonyms,$most_recent_group;
    }
	
	# Loop through unique parent number from the opinions table.
	# Each parent number is a hash key whose value is an array ref of records.
    foreach my $group (@synonyms) {
        $first_row = ${$group}[0];
		$text .= "; it was ".$synmap{$first_row->{'status'}};
        if ($first_row->{'status'} !~ /belongs|nomen/) {
            if ($first_row->{'status'} =~ /corrected|recombined|rank/) {
                $taxon_no = $first_row->{'child_spelling_no'};
            } elsif ($first_row->{'status'} =~ /synonym|replaced|homonym/) {
                $taxon_no = $first_row->{'parent_spelling_no'};
            }
            if ($taxon_no) {
                my $taxon = getTaxon($dbt,'taxon_no'=>$taxon_no);
			    $text .= "<i>".$taxon->{'taxon_name'}."</i>";
            }
        }
        $text .= " by ";
        for(my $i=0;$i<@$group;$i++) {
            if ($i == scalar(@$group) - 2) {
                # replace the final comma with ' and '
                $comma = ' and ';
            } elsif ($i < scalar(@$group) - 2) {
                # otherwise if its before the and, just use commas
                $comma = ', ';
            } else {
                $comma = "";
            }
            $text .= Reference::formatShortRef(${$group}[$i],'alt_pubyr'=>1,'show_comments'=>1) . $comma;
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


sub getOriginalCombination{
	my $dbt = shift;
	my $taxon_no = shift;
	# You know you're an original combination when you have no children
	# that have recombined or corrected as relations to you.
	my $sql = "SELECT DISTINCT(child_no), status FROM opinions".
			  " WHERE child_spelling_no=$taxon_no";
	my @results = @{$dbt->getData($sql)};

    if (@results) {
        return $results[0]->{'child_no'};
    } else {
        return $taxon_no;
    }
}

# PS, used to be selectMostRecentParentOpinion, changed to this to simplify code 
# and consolidate bug fixes 04/20/2005
sub getMostRecentParentOpinion {
	my $dbt = shift;
	my $child_no = shift;
    my $include_child_taxon_info = (shift || 0);
    my $include_reference = (shift || 0);
    my $reference_no = (shift || '');
    return if (!$child_no);
    return if ($reference_no eq '0');

    # The child_name is the correct spelling of the child passed in, not the name of the parent, which 
    # is sort of a bad abstraction on my part PS 09/14/2005. Use this to get the corrected name of a taxon
    my $child_fields = '';
    my $child_join = '';
    if ($include_child_taxon_info) {
        $child_fields .= "a1.taxon_name child_name, a1.taxon_rank child_rank, ";
        $child_join .= " LEFT JOIN authorities a1 ON o.child_spelling_no=a1.taxon_no";
    }

    my $reference_clause = '';
    if ($reference_no) {
        $reference_clause = " AND o.reference_no=$reference_no";
    }

    # This will return the most recent parent opinions. its a bit tricky cause: 
    # we're sorting by aliased fields. So surround the query in parens () to do this:
    # All values of the enum taxonomic_reliability get recast as integers for easy sorting
    # Lowest should appear at top of list (authoritative) and highest at bottom (compendium) so sort ASC
    # and want to use opinions pubyr if it exists, else ref pubyr as second choice - PS
    my $sql = "(SELECT ${child_fields}o.child_no, o.child_spelling_no, o.status, o.parent_no, o.parent_spelling_no,"
            . " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000', o.pubyr, r.pubyr) as pubyr,"
            . " (CASE r.taxonomic_reliability WHEN 'authoritative' THEN 0 WHEN 'standard' THEN 1 WHEN 'second hand' THEN 2 WHEN 'compendium' THEN 3 ELSE 3 END) reliability_index " 
            . " FROM opinions o" 
            . $child_join
            . " LEFT JOIN refs r ON r.reference_no=o.reference_no" 
            . " WHERE o.child_no=$child_no" 
            . $reference_clause
            . ") ORDER BY reliability_index ASC, pubyr DESC LIMIT 1";

    my @rows = @{$dbt->getData($sql)};
    if (scalar(@rows)) {
        return $rows[0];
    }
}

# JA 1.8.03
sub displayEcology	{
	my $dbt = shift;
	my $taxon_no = shift;
	my $genus = shift;
	my $species = shift;
    my $in_list = shift;

	print "<center><h3>Ecology</h3></center>\n";

	if ( ! $taxon_no )	{
		print "<i>No ecological data are available</i>";
		return;
	}

	# get the field names from the ecotaph table
    my @ecotaphFields = $dbt->tableColumns('ecotaph');
    # also get values for ancestors
    my $class_hash = Classification::get_classification_hash($dbt,'all',[$taxon_no],'array');
    my $eco_hash = Ecology::getEcology($dbt,$class_hash,\@ecotaphFields);
    my $ecotaphVals = $eco_hash->{$taxon_no};

    # Convert units for display
    foreach ('minimum_body_mass','maximum_body_mass','body_mass_estimate') {
        if ($ecotaphVals->{$_}) {
            if ($ecotaphVals->{$_} < 1) {
                $ecotaphVals->{$_} = Ecology::kgToGrams($ecotaphVals->{$_});
                $ecotaphVals->{$_} .= ' g';
            } else {
                $ecotaphVals->{$_} .= ' kg';
            }
        }
    } 
    
    my @references = @{$ecotaphVals->{'references'}};     


	if ( ! $ecotaphVals )	{
		print "<i>No ecological data are available</i>";
		return;
	} else	{
		print "<table cellspacing=5 width=600>";
        print "<tr><td colspan=2>";
        if (scalar(@references) == 1) {
            print "<b>Reference:</b> ";
        } elsif (scalar(@references) > 1) {
            print "<b>References:</b> ";
        }
        for(my $i=0;$i<scalar(@references);$i++) {
            my $sql = "SELECT reference_no,author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=$references[$i]";
            my $ref = ${$dbt->getData($sql)}[0];
            $references[$i] = Reference::formatShortRef($ref,'link_id'=>1);
        }
        print join(", ",@references);
        print "</td></tr>";
        print "<tr>";
		my $cols = 0;
		for my $i (0..$#ecotaphFields)	{
			my $name = $ecotaphFields[$i];
			my $nextname = $ecotaphFields[$i+1];
			my $n = $name;
			my @letts = split //,$n;
			$letts[0] =~ tr/[a-z]/[A-Z]/;
			$n = join '',@letts;
			$n =~ s/_/ /g;
			$n =~ s/1$/&nbsp;1/g;
			$n =~ s/2$/&nbsp;2/g;
			if ( $ecotaphVals->{$name} && $name !~ /_no$/ )	{
				my $v = $ecotaphVals->{$name};
				$v =~ s/,/, /g;
                if ( $cols == 2 || $name =~ /^comments$/ || $name =~ /^created$/ || $name =~ /^size_value$/ || $name =~ /1$/ )	{
                    print "</tr>\n<tr>\n";
                    $cols = 0;
                }
				$cols++;
                my $colspan = ($name =~ /comments/) ? "colspan=2" : "";
				print "<td $colspan valign=\"top\"><table><tr><td align=\"left\" valign=\"top\"><b>$n:</b></td><td valign=\"top\">$v</td></tr></table></td> \n";
			}
		}
		if ( $cols > 0 )	{
			print "</tr></table>\n";
		} else	{
			print "</table>\n";
		}
	}

	return $text;

}

# PS 6/27/2005
sub displayMeasurements {
    my $dbt = shift;
    my $taxon_no = shift;
    my $genus = shift;
    my $species = shift;
    my $in_list = shift;    


    # Specimen level data:
    my @specimens;
    my $specimen_count;
    if ($taxon_no) {
        my $t = getTaxon($dbt,'taxon_no'=>$taxon_no);
        if ($t->{'taxon_rank'} =~ /genus|species/) {
            # If the rank is genus or lower we want the big aggregate list of all taxa
            @specimens = Measurement::getMeasurements($dbt,'taxon_list'=>$in_list,'get_global_specimens'=>1);
        } else {
            # I fthe rank is higher than genus, then that rank is too big to be meaningful.  
            # In that case we only want the taxon itself (and its synonyms and alternate names), not the big recursively generated list
            # i.e. If they entered Nasellaria, get Nasellaria indet., or Nasellaria sp. or whatever.
            # get alternate spellings of focal taxon. 
            my @syns = getJuniorSynonyms($dbt,$taxon_no); 
            push @syns,$taxon_no;
            my $sql = "SELECT child_spelling_no FROM opinions WHERE status IN ('recombined as','corrected as','rank changed as') AND child_no IN (".join(",",@syns).")";
            my @results = @{$dbt->getData($sql)};

            # Use the hash to get only unique taxon_nos
            my %all_taxa;
            @all_taxa{@syns} = ();
            foreach my $row (@results) {
               $all_taxa{$row->{'child_spelling_no'}} = 1 if ($row->{'child_spelling_no'});
            }    
            my @small_in_list = keys %all_taxa;
            main::dbg("Passing small_in_list to getMeasurements".Dumper(\@small_in_list));
            @specimens = Measurement::getMeasurements($dbt,'taxon_list'=>\@small_in_list,'get_global_specimens'=>1);
        }
    } else {
        my $taxon_name = $genus;
        if ($species) {
            $taxon_name .= " ".$species;
        }
        @specimens = Measurement::getMeasurements($dbt,'taxon_name'=>$taxon_name,'get_global_specimens'=>1);
    }

    # Returns a triple index hash with index <part><dimension type><whats measured>
    #  Where part can be leg, valve, etc, dimension type can be length,width,height,diagonal,inflation 
    #   and whats measured can be average, min,max,median,error
    my $p_table = Measurement::getMeasurementTable(\@specimens);

    my $str = "";
    $str .= "<div align=\"center\"><h3>Specimen measurements</h3></div>";
    if (@specimens) {
        while (my($part,$m_table)=each %$p_table) {
            my $part_str = ($part) ? "<b>Part: </b>$part<br>" : "";
            $str .= "<table><tr><td colspan=5 style=\"padding-bottom: .75em;\">$part_str<b>Specimens measured:</b> $m_table->{specimens_measured}</td></tr>".
                    "<tr><th></th><th>Mean</th><th>Minimum</th><th>Maximum</th><th>Median</th><th>Error</th><th></th></tr>";

            foreach my $type (('length','width','height','diagonal','inflation')) {
                if (exists ($m_table->{$type})) {
                    $str .= "<tr><td><b>$type</b></td>";
                    foreach my $column (('average','min','max','median','error')) {
                        my $value = $m_table->{$type}{$column};
                        if ($value <= 0) {
                            $str .= "<td align=\"center\">-</td>";
                        } else {
                            $value = sprintf("%.4f",$value);
                            $value =~ s/0+$//;
                            $value =~ s/\.$//;
                            $str .= "<td align=\"center\">$value</td>";
                        }
                    }
                    if ($m_table->{$type}{'error'}) {
                        $str .= "<td align=\"center\">($m_table->{$type}{error_unit})</td>";
                    }
                    $str .= '</tr>';
                }
            }
            $str .= "</table><br>";
        }
    } else {
        $str .= "<div align=\"center\"><i>No measurement data are available</i></div>";
    }

    return $str;

    return "";
}


# JA 11-12,14.9.03
sub displaySynonymyList	{
	my $dbt = shift;
	my $q = shift;
	my $genus = (shift or "");
	my $species = (shift or "");
    # taxon_no must be an original combination
	my $taxon_no = (shift or "");
	my $taxon_name;
	my $output = "";


	if ( $genus && $species ne "" )	{
		$taxon_name = $genus . " " . $species;
	}
	else{
		$taxon_name = $genus;
	}

	print "<center><h3>Synonymy</h3></center>";

    # Find synonyms
    @syns = getJuniorSynonyms($dbt,$taxon_no);

    # Push the focal taxon onto the list as well
    push @syns, $taxon_no;

    # go through list finding all "recombined as" something else cases for each
    # need to do this because synonyms might have their own recombinations, and
    #  the original combination might have alternative combinations
    # don't do this if the parent is actually the focal taxon
	my @synparents;
	for my $syn (@syns)	{
		$sql = "SELECT child_spelling_no FROM opinions WHERE status IN ('recombined as','rank changed as','corrected as') AND child_no=" . $syn . " AND child_spelling_no != " . $taxon_no;
		@synparentrefs = @{$dbt->getData($sql)};
		for $synparentref ( @synparentrefs )	{
			push @synparents, $synparentref->{'child_spelling_no'};
		}
	}

# save each "recombined as" taxon to the list
	push @syns, @synparents;


# now we have a list of the focal taxon, its alternative combinations, its
#  synonyms, and their alternative combinations
# go through the list finding all instances of each name's use as a parent
#  in a recombination or synonymy
# so, we are getting the INSTANCES of opinions and not just the alternative names,
#  which we already know
	for my $syn (@syns)	{
		$sql = "(SELECT author1last,author2last,otherauthors,pubyr,pages,figures,ref_has_opinion,reference_no FROM opinions WHERE status IN ('subjective synonym of','objective synonym of','homonym of','replaced by') AND parent_spelling_no=$syn)";
        $sql .= " UNION ";
		$sql = "(SELECT author1last,author2last,otherauthors,pubyr,pages,figures,ref_has_opinion,reference_no FROM opinions WHERE status IN ('subjective synonym of','objective synonym of','homonym of','replaced by') AND parent_no=$syn)";
        $sql .= " UNION ";
		$sql .= "(SELECT author1last,author2last,otherauthors,pubyr,pages,figures,ref_has_opinion,reference_no FROM opinions WHERE child_spelling_no=$syn AND status IN ('recombined as','rank changed as','corrected as'))";
		my @userefs =  @{$dbt->getData($sql)};


        my $parent = getTaxon($dbt,'taxon_no'=>$syn);

		my $parent_name = $parent->{'taxon_name'};
		my $parent_rank = $parent->{'taxon_rank'};
		if ( $parent_rank =~ /genus|species/ )	{
			$parent_name = "<i>" . $parent_name . "</i>";
		} 
		for $useref ( @userefs )	{
			if ( $useref->{pubyr} )	{
				$synkey = "<td>" . $useref->{pubyr} . "</td><td>" . $parent_name . " " . $useref->{author1last};
				if ( $useref->{otherauthors} )	{
					$synkey .= " et al.";
				} elsif ( $useref->{author2last} )	{
					$synkey .= " and " . $useref->{author2last};
				}
				$mypubyr = $useref->{pubyr};
		# no pub data, get it from the refs table
			} else	{
				$sql = "SELECT author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=" . $useref->{reference_no};
				$refref = @{$dbt->getData($sql)}[0];
				$synkey = "<td>" . $refref->{pubyr} . "</td><td>" . $parent_name . " " . $refref->{author1last};
				if ( $refref->{otherauthors} )	{
					$synkey .= " et al.";
				} elsif ( $refref->{author2last} )	{
					$synkey .= " and " . $refref->{author2last};
				}
				$mypubyr = $refref->{pubyr};
			}
			if ( $useref->{pages} )	{
				if ( $useref->{pages} =~ /[ -]/ )	{
					$synkey .= " pp. " . $useref->{pages};
				} else	{
					$synkey .= " p. " . $useref->{pages};
				}
			}
			if ( $useref->{figures} )	{
				if ( $useref->{figures} =~ /[ -]/ )	{
					$synkey .= " figs. " . $useref->{figures};
				} else	{
					$synkey .= " fig. " . $useref->{figures};
				}
			}
			$synline{$synkey} = $mypubyr;
		}
	}

# go through all the alternative names and mark the original combinations
#  (or really "never recombinations")
# Authorities for later recombinations and corrections have the authority data for the original combination in them (some taxonomic rule or something)
# Thus if we print out the use of an authority, it'll have the original combination author info, not its own. So only print out original combination
# authorities, not recomined authorities, which are just duplicates of the original
	for $syn (@syns)	{
		$sql = "SELECT count(*) AS c FROM opinions WHERE child_spelling_no=$syn AND status IN ('recombined as','rank changed as','corrected as')";
		 $timesrecombined = ${$dbt->getData($sql)}[0]->{c};
		if ( ! $timesrecombined )	{
			$isoriginal{$syn} = "YES";
		}
	}

# likewise appearances in the authority table
	for my $syn (@syns)	{
        if ( $isoriginal{$syn} eq "YES" )	{
            $sql = "SELECT taxon_name,taxon_rank,author1last,author2last,otherauthors,pubyr,pages,figures,ref_is_authority,reference_no FROM authorities WHERE taxon_no=" . $syn;
            @userefs = @{$dbt->getData($sql)};
        # save the instance as a key with pubyr as a value
        # note that @userefs only should have one value because taxon_no
        #  is the primary key
            for $useref ( @userefs )	{
                my $auth_taxon_name = $useref->{taxon_name};
                my $auth_taxon_rank = $useref->{taxon_rank};
                if ( $auth_taxon_rank =~ /genus|species/ )	{
                    $auth_taxon_name = "<i>" . $auth_taxon_name . "</i>";
                }
                if ( $useref->{pubyr} )	{
                    $synkey = "<td>" . $useref->{pubyr} . "</td><td>" . $auth_taxon_name . " " . $useref->{author1last};
                    if ( $useref->{otherauthors} )	{
                        $synkey .= " et al.";
                    } elsif ( $useref->{author2last} )	{
                        $synkey .= " and " . $useref->{author2last};
                    }
                    $mypubyr = $useref->{pubyr};
            # no pub data, get it from the refs table
                } else	{
                    $sql = "SELECT author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=" . $useref->{reference_no};
                    $refref = @{$dbt->getData($sql)}[0];
                    $synkey = "<td>" . $refref->{pubyr} . "</td><td>" . $auth_taxon_name . " " . $refref->{author1last};
                    if ( $refref->{otherauthors} )	{
                        $synkey .= " et al.";
                    } elsif ( $refref->{author2last} )	{
                        $synkey .= " and " . $refref->{author2last};
                    }
                    $mypubyr = $refref->{pubyr};
                }
                if ( $useref->{pages} )	{
                    if ( $useref->{pages} =~ /[ -]/ )	{
                        $synkey .= " pp. " . $useref->{pages};
                    } else	{
                        $synkey .= " p. " . $useref->{pages};
                    }
                }
                if ( $useref->{figures} )	{
                    if ( $useref->{figures} =~ /[ -]/ )	{
                        $synkey .= " figs. " . $useref->{figures};
                    } else	{
                        $synkey .= " fig. " . $useref->{figures};
                    }
                }
                $synline{$synkey} = $mypubyr;
            }
        }
	}

# sort the synonymy list by pubyr
	@synlinekeys = keys %synline;
	@synlinekeys = sort { $synline{$a} <=> $synline{$b} } @synlinekeys;

# print each line of the synonymy list
	print "<table cellspacing=5>\n";
	print "<tr><td><b>Year</b></td><td><b>Name and author</b></td></tr>\n";
	for $synline ( @synlinekeys )	{
		print "<tr>$synline</td></tr>\n";
	}
	print "</table>\n";

	return "";

}

# Small utility function, added 01/06/2005
sub getTaxonNos {
    my $dbt = shift;
    my $name = shift;
    my $rank = shift;
    my @taxon_nos = ();
    if ($dbt && $name)  {
        my $sql = "SELECT taxon_no FROM authorities WHERE taxon_name=".$dbt->dbh->quote($name);
        if ($rank) {
            $sql .= " AND taxon_rank=".$dbt->dbh->quote($rank);
        }
        @results = @{$dbt->getData($sql)};
        push @taxon_nos, $_->{'taxon_no'} for @results;
    }
                                                                                                                                                         
    return @taxon_nos;
}

# Small utility function, added 04/26/2005
# pass it a hash of options
# Returns a array of hashrefs, like getData
# valid hash keys: reference_no, taxon_no, taxon_name, get_reference
# the first three should be self-explanatory.  get_reference set to 1 
# means to include pubyr, author1, etc directly into the hashref.  this can be
# used to distinguish between multiple records of the same name
# Example usage: 
#   @results = getTaxon($dbt,'reference_no'=>345); get all taxa attached to this ref.
#   @results = getTaxon($dbt,'taxon_name'=>'Chelonia', 'get_reference'=>1);  get all records named chelonia, and 
#       transparently include pub info (author1last, pubyr, etc) directly in the 
sub getTaxon {
    my $dbt = shift;
    my %options = @_;

    my @results = ();
    if ($dbt && %options) {
        my $sql;
        if ($options{'get_reference'}) {
            $sql = "SELECT a.taxon_no,a.taxon_rank,a.taxon_name,a.pages,a.figures,a.comments, ".
                   " IF (a.ref_is_authority='YES',r.pubyr,a.pubyr) pubyr,".
                   " IF (a.ref_is_authority='YES',r.author1init,a.author1init) author1init,".
                   " IF (a.ref_is_authority='YES',r.author1last,a.author1last) author1last,".
                   " IF (a.ref_is_authority='YES',r.author2init,a.author2init) author2init,".
                   " IF (a.ref_is_authority='YES',r.author2last,a.author2last) author2last,".
                   " IF (a.ref_is_authority='YES',r.otherauthors,a.otherauthors) otherauthors".
                   " FROM authorities a LEFT JOIN refs r ON a.reference_no=r.reference_no";
        } else {
            $sql = "SELECT * FROM authorities a";
        }
        my @terms = ();
        if ($options{'taxon_no'}) {
            push @terms, 'a.taxon_no='.$dbt->dbh->quote($options{'taxon_no'});
        }
        if ($options{'taxon_name'}) {
            push @terms, 'a.taxon_name like '.$dbt->dbh->quote($options{'taxon_name'});
        }
        if ($options{'reference_no'}) {
            push @terms, 'a.reference_no='.$dbt->dbh->quote($options{'reference_no'});
        }
        if (@terms) {
            $sql .= " WHERE ".join(" AND ",@terms); 
            $sql .= " ORDER BY taxon_name" if ($options{'reference_no'});
            @results = @{$dbt->getData($sql)};
        }
    }
    if (wantarray) {
        return @results;
    } else {
        return $results[0];
    }
}

# Keep going until we hit a belongs to, recombined, corrected as, or nome *
# relationship
sub getSeniorSynonym {
    my $dbt = shift;
    my $taxon_no = shift;

    # Limit this to 10 iterations, in case we a have some weird loop
    for($i=0;$i<10;$i++) {
        $parent = getMostRecentParentOpinion($dbt,$taxon_no);
        if ($parent->{'status'} =~ /synonym|replaced|homonym/) {
            $taxon_no = $parent->{'parent_no'};
        } else {
            return $taxon_no;
        }
    }
}

# They may potentialy be chained, so keep going till we're done. Use a queue isntead of recursion to simplify things slightly
# and original combination must be passed in
sub getJuniorSynonyms {
    my $dbt = shift;
    my $taxon_no = shift;

    my @queue = ($taxon_no);
    my @synonyms = ();
    for($i = 0;$i<20;$i++) {
        my $taxon_no;
        if (@queue) {
            $taxon_no = pop @queue;
        } else {
            last;
        }
        my $sql = "SELECT DISTINCT child_no FROM opinions WHERE parent_no=$taxon_no";
        my @results = @{$dbt->getData($sql)};
        foreach $row (@results) {
            $parent = getMostRecentParentOpinion($dbt,$row->{'child_no'});
            if ($parent->{'parent_no'} == $taxon_no && $parent->{'status'} =~ /synonym|homonym|replaced/) {
                push @queue, $row->{'child_no'};
                push @synonyms, $row->{'child_no'};
            }
        }
    }
    return @synonyms;
}



1;
