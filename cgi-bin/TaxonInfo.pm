package TaxonInfo;

use PBDBUtil;
use Classification;
use Globals;
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
               " FROM authorities a LEFT JOIN refs r ON a.reference_no=r.reference_no";
        if ($q->param('taxon_name') =~ /^[a-z]+$/) {
            # We treat species as special case and do an open ended search
            $sql .= " WHERE taxon_name LIKE ".$dbh->quote("% ".$q->param('taxon_name'));
        } elsif ($q->param('taxon_name')) {
            $sql .= " WHERE taxon_name LIKE ".$dbh->quote($q->param('taxon_name'));
        }
        
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
            $sql1 = "SELECT count(*) c FROM occurrences $where";
            $sql2 = "SELECT count(*) c FROM reidentifications $where";
            my $count = ${$dbt->getData($sql1)}[0]->{'c'};
            $count += ${$dbt->getData($sql2)}[0]->{'c'};
            if ($count > 0) {
                my $taxon_name = $genus;
                if ($species) {
                    $taxon_name .= " $species";
                }    
                $q->param('taxon_name'=>$taxon_name);
                displayTaxonInfoResults($dbt,$s,$q);
            } else {
                # If nothing, print out an error message
                searchForm($hbo, $q, 1); # param for not printing header with form
                if($s->get("enterer") ne "Guest" && $s->get("enterer") ne ""){
                    print "<center><p><a href=\"bridge.pl?action=submitAuthorityTaxonSearch?taxon_name=".$q->param('taxon_name')."\"><b>Add taxonomic information</b></a></center>";
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
    my $taxon_name;
    if ($taxon_no) {
        my $orig_taxon_no = getOriginalCombination($dbt,$taxon_no);
        $taxon_no = getSeniorSynonym($dbt,$orig_taxon_no);
        # This actually gets the most correct name
        my $correct_row = getMostRecentParentOpinion($dbt,$taxon_no,1);
        $taxon_name = $correct_row->{'child_name'};
    } else {
        $taxon_name = $q->param('taxon_name');
    }

	# Get the sql IN list for a Higher taxon:
	my $in_list = "";

	if($taxon_no) {
    	# JA: replaced recurse call with taxonomic_search call 7.5.04 because
    	#  I am not maintaining recurse
        @in_list=PBDBUtil::taxonomic_search($dbt,$taxon_no);
        $in_list=\@in_list;
	} else {
        # We just got to search the occ/reid tables directly
		$in_list = [$taxon_name];
	} 

    ($genus,$species) = split(/ /,$taxon_name);


    print "<div class=\"float_box\">";
    print "<p>&nbsp;</p>";
    @modules_to_display = doNavBox($dbt,$q,$s,$in_list);
    print "</div>";
	print "<div align=\"center\"><h2>$taxon_name</h2></div>";
	# Go through the list
	foreach my $module (@modules_to_display){
        print "<div align=\"center\">";
		doModules($dbt,$dbh,$q,$s,$exec_url,$module,$genus,$species,$in_list,$taxon_no);
        print "</div>";
		print "<hr>"; 
	}
	# images are last
	if(@selected_images){
		print "<center><h3>Images</h3></center>";
		foreach my $image (@selected_images){
			foreach my $res (@thumbs){
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

    my $entered_name = $q->param('entered_name') || $q->param('taxon_name');
    my $entered_no   = $q->param('entered_no') || $q->param('taxon_no');
	print "<div style=\"font-family : Arial, Verdana, Helvetica; font-size : 14px;\">";
	if($s->get("enterer") ne "Guest" && $s->get("enterer") ne ""){
		# Entered Taxon
        if ($entered_no) {
		    print "<center><a href=\"/cgi-bin/bridge.pl?action=submitAuthorityTaxonSearch&taxon_no=$entered_no";
        } else {
		    print "<center><a href=\"/cgi-bin/bridge.pl?action=submitAuthorityTaxonSearch&taxon_no=-1&taxon_name=$entered_name";
        }
		print "\"><b>Edit taxonomic data for $entered_name</b></a> - ";
		
		unless($entered_name eq $genus_name){
			# Verified Taxon
			print "<a href=\"/cgi-bin/bridge.pl?action=startTaxonomy&taxon_name=$genus_name";
			if($taxon_no){
				  print "&taxon_no=$taxon_no";
			}
			print "\"><b>Edit taxonomic data for $genus_name</b></a> - \n";
		}
		print "<a href=\"/cgi-bin/bridge.pl?action=startImage\">".
			  "<b>Enter an image</b></a> - \n";
	}
	else{
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
							  2 => "taxonomic history",
							  3 => "synonymy",
							  4 => "ecology/taphonomy",
							  5 => "map",
							  6 => "age range/collections");

	# if the modules are known and the user is not a guest,
	#  set the module preferences in the person table
	# if the modules are not known try to pull them from the person table
	# of course, we don't have to reset the preferences in that case
	# JA 21.10.04
	if ( $s->get("enterer") ne "Guest" && $s->get("enterer") ne "" )	{
		if ( @modules_to_display )	{
			my $pref = join ' ', @modules_to_display;
			my $prefsql = "UPDATE person SET taxon_info_modules='$pref',last_action=last_action WHERE name='" . $s->get("enterer") . "'";
			$dbt->getData($prefsql);
		}
		elsif ( ! @modules_to_display )	{
			my $prefsql = "SELECT taxon_info_modules FROM person WHERE person_no='" . $s->get("enterer_no") . "'";
			$pref = @{$dbt->getData($prefsql)}[0]->{taxon_info_modules};
			@modules_to_display = split / /,$pref;
		}
	}

	# if that didn't work, set the default
	if ( ! @modules_to_display )	{
		@modules_to_display = (1,2,3);
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
	my @selected_images = $q->param('image_thumbs');
	require Images;

	my @thumbs = Images::processViewImages($dbt, $q, $s, $in_list);
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

    return @modules_to_display;
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
	($genus, $species) = split /\s+/, $q->param("taxon_name");

	# classification
	if($module == 1){
		print "<table>".
			  "<tr><td align=\"middle\"><h3>Classification</h3></td></tr>".
			  "<tr><td valign=\"top\" align=\"middle\">";

		print displayTaxonClassification($dbt, $genus, $species, $taxon_no);
		print "</td></tr></table>";

	}
	# synonymy
	elsif($module == 2){
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
	elsif ( $module == 3 )	{
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
	elsif ( $module == 4 )	{
		print displayEcology($dbt,$taxon_no,$genus,$species);
	}
	# map
	elsif($module == 5){
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
	elsif($module == 6){
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

	$latCenter = (($latMax - $latMin)/2) + $latMin;
	$lonCenter = (($lonMax - $lonMin)/2) + $lonMin;

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

	$q->param(-name=>"limit",-value=>1000000);
	$q->param(-name=>"taxon_info_script",-value=>"yes");

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

	# Get all the data from the database, bypassing most of the normal behavior
	# of displayCollResults
	@data = @{main::displayCollResults($in_list)};	
	require Collections;

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
	    $res = Collections::createTimePlaceString($row,$dbt);
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
			foreach  my $val (@{$time_place_coll{$key}}){
				my $link=Collections::createCollectionDetailLink($exec_url,$val,$val);
				$output .= "$link ";
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

    # Theres one case where we might want to do upward classification when theres no taxon_no:
    #  The Genus+species isn't in authorities, but the genus is
    my $genus_no = 0;
    if ($genus && $species && !$orig_no) {
        my @results = getTaxonNos($dbt,$genus);
        if (@results == 1) {
            $orig_no=$results[0];
            $genus_no=$results[0]; 
            $append_species=1;
        }
    }

    #
    # Do the classification
    #
    my ($taxon_no,$taxon_rank,$taxon_name,$child_spelling_no);
    my @table_rows = ();
    if ($orig_no) {
        # format of table_rows: taxon_rank,taxon_name,taxon_no(original combination),taxon_no(recombination, if recombined)
        # This will happen if the genus has a taxon_no but not the species
        if ($append_species) {
            push @table_rows, ['species',"$genus $species",0,0];
        }

        # First, find the rank, name, of the focal taxon
        my $correct_row = getMostRecentParentOpinion($dbt,$orig_no,1);
        $child_spelling_no = $correct_row->{'child_spelling_no'};    
        $taxon_name = $correct_row->{'child_name'};    
        $taxon_rank = $correct_row->{'child_rank'};    
#        $is_recomb = ($orig_no != $child_spelling_no) ? 1:0;

        push @table_rows, [$taxon_rank,$taxon_name,$orig_no,$child_spelling_no];

        # Now find the rank,name, and publication of all its parents
        $first_link = Classification::get_classification_hash($dbt,'all',[$orig_no],'linked_list');
        $next_link = $first_link->{$orig_no};
        for(my $counter = 0;%$next_link && $counter < 30;$counter++) {
            push @table_rows, [$next_link->{'taxon_rank'},$next_link->{'taxon_name'},$next_link->{'child_no'},$next_link->{'child_spelling_no'}];
            last if ($next_link->{'rank'} eq 'kingdom');
            $next_link = $next_link->{'next_link'};
		}

    } else {
        # This block is if no taxon no is found - go off the occurrences table
        push @table_rows,['species',"$genus $species",0,0] if $species;
        push @table_rows,['genus',$genus,$genus_no,0];
    }

    #
    # Print out the table in the reverse order that we initially made it
    #
    my $output = "<table><tr valign=top><th>Rank</th><th>Name</th><th>Author</th></tr>";
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
            #if ($species !~ /^sp(\.)*$|^indet(\.)*$/) {
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

    #
    # Begin getting sister/child taxa
    # PS 01/20/2004 - rewrite: Use getChildren function
    # First get the children
    #
    $focal_taxon_rank = $table_rows[0][0];
    $focal_taxon_no   = $table_rows[0][2];
    $parent_taxon_no  = $table_rows[1][2];

    my $taxon_records = [];
    my @child_taxa_links;
    # This section generates links for children if we have a taxon_no (in authorities table)
    $taxon_records = PBDBUtil::getChildren($dbt,$focal_taxon_no,1,'sort_alphabetical') if ($focal_taxon_no);
    if (@{$taxon_records}) {
        foreach $record (@{$taxon_records}) {
            my @syn_links;                                                         
            my @synonyms = @{$record->{'synonyms'}};
            push @syn_links, $_->{'taxon_name'} for @synonyms;
            my $link = qq|<a href="bridge.pl?action=checkTaxonInfo&taxon_no=$record->{taxon_no}">$record->{taxon_name}|;
            $link .= " (syn. ".join(", ",@syn_links).")" if @syn_links;
            $link .= "</a>";
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
    if ($genus && !$orig_no) {
        my ($sql,$whereClause,@results);
        $whereClause = "genus_name like ".$dbh->quote($genus);
        $sql  = "(SELECT genus_name,species_name FROM occurrences WHERE $whereClause)";
        $sql .= " UNION ";
        $sql .= "(SELECT genus_name,species_name FROM reidentifications WHERE $whereClause)"; 
        $sql .= "ORDER BY genus_name,species_name";
        @results = @{$dbt->getData($sql)};
        foreach $row (@results) {
            if ($species) {
                if ($species ne $row->{'species_name'}) {
                    my $link = qq|<a href="bridge.pl?action=checkTaxonInfo&taxon_name=$row->{genus_name} $row->{species_name}">$row->{genus_name} $row->{species_name}</a>|;
                    push @sister_taxa_links, $link;
                }
            } else {
                my $link = qq|<a href="bridge.pl?action=checkTaxonInfo&taxon_name=$row->{genus_name} $row->{species_name}">$row->{genus_name} $row->{species_name}</a>|;
                push @child_taxa_links, $link;
            }
        }
    }
   
    # Print em out
    if (@child_taxa_links) {
        $output .= "<p><i>This taxon includes:</i><br>"; 
        $output .= join(", ",@child_taxa_links);
        $output .= "</p>";
    }

    if (@sister_taxa_links) {
        $output .= "<p><i>Sister taxa include:</i><br>"; 
        $output .= join(", ",@sister_taxa_links);
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
		$sql = "SELECT author1last,author2last,otherauthors,pubyr FROM refs ".
			   "WHERE reference_no=".$auth_rec[0]->{reference_no};
		@results = @{$dbt->getData($sql)};
		
		$text .= "<li><i>".$auth_rec[0]->{taxon_name}."</i> was named by ".
        $text .= Reference::formatRef($results[0]);
	} elsif($auth_rec[0]->{author1last}){
    #	elsif($auth_rec[0]->{author1last} && $auth_rec[0]->{pubyr}){
		$text .= "<li><i>".$auth_rec[0]->{taxon_name}."</i> was named by ".
        $text .= Reference::formatRef($auth_rec[0]);
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
                @results = getTaxon($dbt,'taxon_no'=>$taxon_no);
			    $text .= "<i>".$results[0]->{'taxon_name'}."</i>";
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
            $text .= Reference::formatRef(${$group}[$i]) . $comma;
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
			  " WHERE opinions.child_spelling_no=$taxon_no".
              " AND (status='recombined as' OR status='corrected as' OR status='rank changed as')";
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
    return if (!$child_no);

    my $child_fields = '';
    my $child_join = '';
    if ($include_child_taxon_info) {
        $child_fields .= "a1.taxon_name child_name, a1.taxon_rank child_rank, ";
        $child_join .= " LEFT JOIN authorities a1 ON o.child_spelling_no=a1.taxon_no";
    }

    # This will return the most recent parent opinions. its a bit tricky cause: 
    # we're sorting by aliased fields. So surround the query in parens () to do this:
    # compendium types come last: sort asc. so the boolean is_compendium will 
    #  cause compendium entries to come up last in the list
    # and want to use opinions pubyr if it exists, else ref pubyr as second choice
    #  surround the select in () so aliased fields can be used in order by.
    # The taxon_name is the correct spelling of the child passed in, not the name of the parent, which is weird
    my $sql = "(SELECT ${child_fields}o.child_no, o.child_spelling_no, o.status, o.parent_no, o.parent_spelling_no, "
            . " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000', o.pubyr, r.pubyr) as pubyr, "
            . " (r.publication_type='compendium') AS is_compendium" 
            . " FROM opinions o " 
            . $child_join
            . " LEFT JOIN refs r ON r.reference_no=o.reference_no " 
            . " WHERE o.child_no=$child_no) " 
            . " ORDER BY is_compendium ASC, pubyr DESC LIMIT 1";

    my @rows = @{$dbt->getData($sql)};
    if (scalar(@rows)) {
        return $rows[0];
    }
}

# Deprecrated, use getMostRecentParentOpinion, which does pretty much the same
# thing but slightly more stuff in the function and in sql so its simpler to use and a bit more efficient and works with the new table structure
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
		PBDBUtil::debug(2,"FOUND MORE THAN ONE PARENT");
		# find the most recent opinion
		# Algorithm: For each opinion (parent), if opinions.pubyr exists, 
		# use it.  If not, get the pubyr from the reference and use it.
		# Finally, compare across parents to find the most recent year.

		# Store only the greatest value in $years
		my $years = 0;
		my $index_winner = -1;
		for(my $index = 0; $index < @array_of_hash_refs; $index++){
			# pubyr is recorded directly in the opinion record,
			#  so use it
			if ( $array_of_hash_refs[$index]->{pubyr} )	{
				if ( $array_of_hash_refs[$index]->{pubyr} > $years)	{
					$years = $array_of_hash_refs[$index]->{pubyr};
					$index_winner = $index;
				}
			}
			else{
				# get the year from the refs table
				# JA: also get the publication type because
				#  everything else is preferred to compendia
				my $sql = "SELECT pubyr,publication_type AS pt FROM refs WHERE reference_no=".
						  $array_of_hash_refs[$index]->{reference_no};
				my @ref_ref = @{$dbt->getData($sql)};
		# this is kind of ugly: use pubyr if it's the first one
		#  encountered, or if the winner's pub type is compendium and
		#  the current ref's is not, or if both are type compendium and
		#  the current ref is more recent, or if neither are type
		#  compendium and the current ref is more recent
				if($ref_ref[0]->{pubyr} &&
					( ! $years ||
					( $ref_ref[0]->{pt} ne "compendium" &&
					  $winner_pt eq "compendium" ) ||
					( $ref_ref[0]->{pubyr} > $years &&
					  $ref_ref[0]->{pt} eq "compendium" &&
					  $winner_pt eq "compendium" ) ||
					( $ref_ref[0]->{pubyr} > $years &&
					  $ref_ref[0]->{pt} ne "compendium" &&
					  $winner_pt ne "compendium" ) ) )	{
					$years = $ref_ref[0]->{pubyr};
					$index_winner = $index;
					$winner_pt = $ref_ref[0]->{pt};
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

## Deal with homonyms - DEPRECATED
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
			# "corrected as", "recombined as", "rank changed as", or "replaced by" and use 
			# that as the focal taxon.
			if(scalar @ref_ref < 1){
				$sql = "SELECT child_no FROM opinions WHERE parent_no=".
					   $ref->{taxon_no}." AND status IN ('recombined as',".
					   "'replaced by','corrected as','rank changed as')";
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

# JA 1.8.03
sub displayEcology	{
	my $dbt = shift;
	my $taxon_no = shift;
	my $genus = shift;
	my $species = shift;

	print "<center><h3>Ecology</h3></center>\n";

	if ( ! $taxon_no )	{
		print "<i>No ecological data are available</i>";
		return;
	}

	my $taxon_name;
	if ( $species )	{
		$taxon_name = $genus . " " . $species;
	} else	{
		$taxon_name = $genus;
	}

	# get the field names from the ecotaph table
	my %attrs = ("NAME"=>'');
	my $sql = "SELECT * FROM ecotaph WHERE taxon_no=0";
	%ecotaphRow = %{@{$dbt->getData($sql,\%attrs)}[0]};
	for my $name (@{$attrs{"NAME"}})	{
		$ecotaphRow{$name} = "";
		push @ecotaphFields,$name;
	}

	# grab all the data for this taxon from the ecotaph table
	$sql = "SELECT * FROM ecotaph WHERE taxon_no=" . $taxon_no;
	$ecotaphVals = @{$dbt->getData($sql)}[0];

	# also get values for ancestors (always do this because data for the
	#   taxon could be partial)
	# WARNING: this will completely screw up if the name has homonyms
	# JA: changed this on 4.4.04 to use taxon_no instead of taxon_name,
	#  avoiding homonym problem
	my $ancestor_ref = Classification::get_classification_hash($dbt,'class,order,family',[$taxon_no],'numbers');
    my @ancestors = split(/,/,$ancestor_ref->{$taxon_no},-1);

	my $tempVals;
	if ( @ancestors)	{
		for my $a ( @ancestors )	{
            if ($a) {
                $sql = "SELECT * FROM ecotaph WHERE taxon_no=" . $a;
                main::dbg($sql);
                
                $tempVals = @{$dbt->getData($sql)}[0];
                if ( $tempVals )	{
                    for my $field ( @ecotaphFields )	{
                        if ( $tempVals->{$field} && ! $ecotaphVals->{$field} && $field ne "created" && $field ne "modified" )	{
                            $ecotaphVals->{$field} = $tempVals->{$field};
                        }
                    }
                }
            }
		}
	}

	if ( ! $ecotaphVals )	{
		print "<i>No ecological data are available</i>";
		return;
	} else	{
		print "<table cellspacing=5><tr>";
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
				$cols++;
				my $v = $ecotaphVals->{$name};
				$v =~ s/,/, /g;
				print "<td valign=\"top\"><table><tr><td align=\"left\" valign=\"top\"><b>$n:</b></td><td align=\"right\" valign=\"top\">$v</td></tr></table></td> \n";
			}
			if ( $cols == 2 || $nextname =~ /^created$/ || $nextname =~ /^size_value$/ || $nextname =~ /1$/ )	{
				print "</tr>\n<tr>\n";
				$cols = 0;
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


        my @parents = getTaxon($dbt,'taxon_no'=>$syn);

		my $parent_name = $parents[0]->{'taxon_name'};
		my $parent_rank = $parents[0]->{'taxon_rank'};
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
                   " IF (a.ref_is_authority='YES',r.pubyr,r.pubyr) pubyr,".
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
        while (($field,$value)=each %options) {
            if ($field =~ /taxon_no|reference_no|taxon_name/) {
                push @terms, "a.$field=".$dbt->dbh->quote($value);
            }
        }
        if (@terms) {
            $sql .= " WHERE ".join(" AND ",@terms); 
            @results = @{$dbt->getData($sql)};
        }
    }
    return @results;
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
        my $sql = "SELECT child_no FROM opinions WHERE parent_no=$taxon_no";
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
