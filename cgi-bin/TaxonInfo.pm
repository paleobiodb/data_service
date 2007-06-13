package TaxonInfo;

use Taxon;
use TimeLookup;
use Data::Dumper;
use Collection;
use Reference;
use TaxaCache;
use Ecology;
use Images;
use Measurement;
use Debug qw(dbg);
use PBDBUtil;
use Constants qw($READ_URL $WRITE_URL $IS_FOSSIL_RECORD $HTML_DIR);

use strict;

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

    my $page_title = "Taxonomic name search form"; 
    
	if ($search_again){
        $page_title = "<p class=\"medium\">No results found (please search again)</p>";
    } 
	print $hbo->populateHTML('search_taxoninfo_form' , [$page_title,''], ['page_title','page_subtitle']);
}

# This is the front end for displayTaxonInfoResults - always use this instead if you want to 
# call from another script.  Can pass it a taxon_no or a taxon_name
sub checkTaxonInfo {
	my $q = shift;
	my $s = shift;
	my $dbt = shift;
    my $hbo = shift;
    my $dbh = $dbt->dbh;

    if (!$q->param("taxon_no") && !$q->param("taxon_name") && !$q->param("common_name") && !$q->param("author") && !$q->param("pubyr")) {
        searchForm($hbo, $q, 1); # param for not printing header with form
        return;
    }
	
    if ($q->param('taxon_no')) {
        # If we have is a taxon_no, use that:
        displayTaxonInfoResults($dbt,$s,$q);
    } elsif (!$q->param('taxon_name') && !($q->param('common_name')) && !($q->param('pubyr')) && !$q->param('author')) {
        searchForm($hbo,$q);
    } else {
        my $temp = $q->param('taxon_name');
        $temp =~ s/ sp\.//;
        $q->param('taxon_name' => $temp);
        my $options = {'match_subgenera'=>1,'remove_rank_change'=>1};
        foreach ('taxon_name','common_name','author','pubyr') {
            if ($q->param($_)) {
                $options->{$_} = $q->param($_);
            }
        }

        my @results = getTaxa($dbt,$options,['taxon_no','taxon_rank','taxon_name','common_name','author1last','author2last','otherauthors','pubyr','pages','figures','comments']);   

        if(scalar @results < 1 && $q->param('taxon_name')){
            # If nothing from authorities, go to occs + reids
            my ($genus,$subgenus,$species,$subspecies) = Taxon::splitTaxon($q->param('taxon_name'));
            my $where = "WHERE genus_name LIKE ".$dbh->quote($genus);
            if ($subgenus) {
                $where .= " AND subgenus_name LIKE ".$dbh->quote($subgenus);
            }
            if ($species) {
                $where .= " AND species_name LIKE ".$dbh->quote($species);
            }
            my $sql = "(SELECT genus_name FROM occurrences $where GROUP BY genus_name)".
                   " UNION ".
                   "(SELECT genus_name FROM reidentifications $where GROUP BY genus_name)";
            my @occs = @{$dbt->getData($sql)};
            if (scalar(@occs) >= 1) {
                #my $taxon_name = $genera[0]->{'genus_name'};
                #if ($species) {
                #    $taxon_name .= " $species";
                #}    
                #$q->param('taxon_name'=>$taxon_name);
                displayTaxonInfoResults($dbt,$s,$q);
            } else {
                # If nothing, print out an error message
                searchForm($hbo, $q, 1); # param for not printing header with form
                if($s->isDBMember()) {
                    print "<center><p><a href=\"$WRITE_URL?action=submitTaxonSearch&amp;goal=authority&amp;taxon_name=".$q->param('taxon_name')."\"><b>Add taxonomic information</b></a></center>";
                }
            }
        } elsif(scalar @results < 1 && ! $q->param('taxon_name')){
            searchForm($hbo, $q, 1); # param for not printing header with form
            if($s->isDBMember()) {
                print "<center><p><a href=\"$WRITE_URL?action=submitTaxonSearch&amp;goal=authority&amp;taxon_name=".$q->param('taxon_name')."\"><b>Add taxonomic information</b></a></center>";
            }
        } elsif(scalar @results == 1){
            $q->param('taxon_no'=>$results[0]->{'taxon_no'});
            displayTaxonInfoResults($dbt,$s,$q);
        } else{
            # now create a table of choices and display that to the user
            print "<div align=\"center\"><h3>Please select a taxon</h3><br>";
            print qq|<div class="displayPanel" align="center" style="width: 30em; padding-top: 1.5em;">
<div class="displayPanelContent">
|;

            print qq|<form method="POST" action="$READ_URL">|;
            print qq|<input type="hidden" name="action" value="checkTaxonInfo">|;
            
            print "<table>\n";
            print "<tr>";
            for(my $i=0; $i<scalar(@results); $i++) {
                my $authorityLine = Taxon::formatTaxon($dbt,$results[$i]);
                my $checked = ($i == 0) ? "CHECKED" : "";
                print qq|<td><input type="radio" name="taxon_no" value="$results[$i]->{taxon_no}" $checked> $authorityLine</td>|;
                print "</tr><tr>";
            }
            print "</tr>";
            print "<tr><td align=\"center\" colspan=3><br>";
            print "<input type=\"submit\" value=\"Get taxon info\">";
            print qq|</td></tr></table></form></div>
</div>
</div>
|;
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

    my $is_real_user = 0;
    if ($q->request_method() eq 'POST' || $q->param('is_real_user') || $s->isDBMember()) {
        $is_real_user = 1;
    }
    if (PBDBUtil::checkForBot()) {
        $is_real_user = 0;
    }


    # Get most recently used name of taxon
    my ($taxon_name,$common_name,$taxon_rank);
    if ($taxon_no) {
        my $orig_taxon_no = getOriginalCombination($dbt,$taxon_no);
        $taxon_no = getSeniorSynonym($dbt,$orig_taxon_no);
        # This actually gets the most correct name
        my $taxon = getMostRecentSpelling($dbt,$taxon_no);
        $taxon_name = $taxon->{'taxon_name'};
        $common_name = $taxon->{'common_name'};
        $taxon_rank = $taxon->{'taxon_rank'};
    } else {
        $taxon_name = $q->param('taxon_name');
    }

	# Get the sql IN list for a Higher taxon:
	my $in_list;
    my $quick = 0;;
	if($taxon_no) {
        my $sql = "SELECT (rgt-lft) diff FROM taxa_tree_cache WHERE taxon_no=$taxon_no";
        my $diff = ${$dbt->getData($sql)}[0]->{'diff'};
        if (!$is_real_user && $diff > 1000) {
            $quick = 1;
            $in_list = [-1];
        } else {
            my @in_list=TaxaCache::getChildren($dbt,$taxon_no);
            $in_list=\@in_list;
        }
	} 

    print "<div class=\"small\">";
    my @modules_to_display = (1,2,3,4,5,6,7,8);

    my $display_name = $taxon_name;
    if ( $common_name =~ /[A-Za-z]/ )	{
        $display_name .= " ($common_name)";
    } 
    if ($taxon_no && $common_name !~ /[A-Za-z]/) {
        my $orig_ss = getOriginalCombination($dbt,$taxon_no);
        my $mrpo = getMostRecentClassification($dbt,$orig_ss);
        my $last_status = $mrpo->{'status'};

        my %disused;
        my $sql = "SELECT synonym_no FROM taxa_tree_cache WHERE taxon_no=$taxon_no";
        my $ss_no = ${$dbt->getData($sql)}[0]->{'synonym_no'};
        if ($taxon_rank !~ /genus|species/) {
            %disused = %{disusedNames($dbt,$ss_no)};
        }

        if ($disused{$ss_no}) {
            $display_name .= " (disused)";
        } elsif ($last_status =~ /nomen/) {
            $display_name .= " ($last_status)";
        }
    }

    print '
<script src="/JavaScripts/tabs.js" language="JavaScript" type="text/javascript"></script>                                                                                       
<div align=center>
  <table cellpadding=0 cellspacing=0 border=0 width=700>
  <tr>
    <td id="tab1" class="tabOff" style="white-space: nowrap;"
      onClick="showPanel(1);" 
      onMouseOver="hover(this);" 
      onMouseOut="setState(1)">Classification</td>
    <td id="tab2" class="tabOff" style="white-space: nowrap;"
      onClick="showPanel(2);" 
      onMouseOver="hover(this);" 
      onMouseOut="setState(2)">Taxonomic history</td>
    <td id="tab3" class="tabOff" style="white-space: nowrap;"
      onClick = "showPanel(3);" 
      onMouseOver="hover(this);" 
      onMouseOut="setState(3)">Synonymy</td>
    <td id="tab4" class="tabOff" style="white-space: nowrap;"
      onClick = "showPanel(4);" 
      onMouseOver="hover(this);" 
      onMouseOut="setState(4)">Morphology</td>
  </tr>
  <tr>
    <td id="tab5" class="tabOff" style="white-space: nowrap;"
      onClick="showPanel(5);" 
      onMouseOver="hover(this);" 
      onMouseOut="setState(5)">Ecology and taphonomy</td>
    <td id="tab6" class="tabOff" style="white-space: nowrap;"
      onClick = "showPanel(6);" 
      onMouseOver="hover(this);" 
      onMouseOut="setState(6)">Map</td>
    <td id="tab7" class="tabOff" style="white-space: nowrap;"
      onClick = "showPanel(7);" 
      onMouseOver="hover(this);" 
      onMouseOut="setState(7)">Age range and collections</td>
    <td id="tab8" class="tabOff" style="white-space: nowrap;"
      onClick = "showPanel(8);" 
      onMouseOver="hover(this);" 
      onMouseOut="setState(8)">Images</td>
  </tr>
  </table>
</div>
';

    print "<div align=\"center\" style=\"margin-bottom: -1.5em;\"><h2>$display_name</h2></div>\n";

    
    print '<script language="JavaScript" type="text/javascript">
    hideTabText(2);
    hideTabText(3);
    hideTabText(4);
    hideTabText(5);
    hideTabText(6);
    hideTabText(7);
    hideTabText(8);
</script>';

    my %modules = ();
    $modules{$_} = 1 foreach @modules_to_display;


	# classification
	if($modules{1}) {
        print '<div id="panel1" class="panel">';
		print '<div align="center"><h3>Classification</h3></div>';
        #print '<div align="center" style=\"border-bottom-width: 1px; border-bottom-color: red; border-bottom-style: solid;\">';
        print '<div align="center">';
		print displayTaxonClassification($dbt, $taxon_no, $taxon_name,$is_real_user);

        my $entered_name = $q->param('entered_name') || $q->param('taxon_name') || $taxon_name;
        my $entered_no   = $q->param('entered_no') || $q->param('taxon_no');
        print "<p>";
        print "<div>";
        print "<center>";
	print displayRelatedTaxa($dbt, $taxon_no, $taxon_name,$is_real_user);
    	print "<a href=\"$READ_URL?action=beginTaxonInfo\">".
	    	  "<b>Get info on another taxon</b></a></center>\n";
        if($s->isDBMember()) {
            # Entered Taxon
            if ($entered_no) {
                print "<a href=\"$WRITE_URL?action=displayAuthorityForm&amp;taxon_no=$entered_no\">";
                print "<b>Edit taxonomic data for $entered_name</b></a> - ";
            } else {
                print "<a href=\"$WRITE_URL?action=submitTaxonSearch&amp;goal=authority&amp;taxon_no=-1&amp;taxon_name=$entered_name\">";
                print "<b>Enter taxonomic data for $entered_name</b></a> - ";
            }

            if ($entered_no) {
                print "<a href=\"$WRITE_URL?action=displayOpinionChoiceForm&amp;taxon_no=$entered_no\"><b>Edit taxonomic opinions about $entered_name</b></a> - ";
                print "<a href=\"$WRITE_URL?action=startPopulateEcologyForm&amp;taxon_no=$taxon_no\"><b>Add/edit ecological/taphonomic data</b></a> - ";
            }
            
            print "<a href=\"$WRITE_URL?action=startImage\">".
                  "<b>Enter an image</b></a>\n";
        }

        print "</div>\n";
        print "</p>";
        print "</div>\n";
        print "</div>\n";
	}

    print '<script language="JavaScript" type="text/javascript">
    showPanel(1);
</script>';

	# synonymy
	if($modules{2}) {
        print '<div id="panel2" class="panel">';
		print "<div align=\"center\"><h3>Taxonomic history</h3></div>\n";
        print '<div align="center">';
        print displayTaxonHistory($dbt, $taxon_no, $is_real_user);
        print "See something missing? <a href=\"$READ_URL?user=Guest&amp;action=displayPage&amp;page=join_us\">Join the Paleobiology Database</a>\n";
        print "</div>\n";
        print "</div>\n";
        print '<script language="JavaScript" type="text/javascript"> showTabText(2); </script>';
	}

	if ($modules{3}) {
        print '<div id="panel3" class="panel">';
		print "<div align=\"center\"><h3>Synonymy</h3></div>\n";
        print '<div align="center">';
    	print displaySynonymyList($dbt, $taxon_no);
        print "</div>\n";
        print "</div>\n";
        print '<script language="JavaScript" type="text/javascript"> showTabText(3); </script>';
	}
    
    if ($modules{4}) {
        print '<div id="panel4" class="panel">';
		print "<div align=\"center\"><h3>Morphology</h3></div>\n";
        print '<div align="center">';
        print displayDiagnoses($dbt,$taxon_no);
        print "<br>\n";
        unless ($quick) {
		    print displayMeasurements($dbt,$taxon_no,$taxon_name,$in_list);
        }
        print "</div>\n";
        print "</div>\n";
        print '<script language="JavaScript" type="text/javascript"> showTabText(4); </script>';
    }
    if ($modules{5}) {
        print '<div id="panel5" class="panel">';
		print "<div align=\"center\"><h3>Ecology and taphonomy</h3></div>\n";
        print '<div align="center">';
        unless ($quick) {
		    print displayEcology($dbt,$taxon_no,$in_list);
        }
        print "</div>\n";
        print "</div>\n";
        print '<script language="JavaScript" type="text/javascript"> showTabText(5); </script>';
    }
   
    my $collectionsSet;
    if ($is_real_user) {
        $collectionsSet = getCollectionsSet($dbt,$q,$s,$in_list,$taxon_name);
    }

	# map
    if ($modules{6}) {
        print '<div id="panel6" class="panel">';
		print "<div align=\"center\"><h3>Map</h3></div>\n";
        print '<div align="center">';

        if ($is_real_user) {
            displayMap($dbt,$q,$s,$collectionsSet);
        } else {
            print qq|<form method="POST" action="$READ_URL">|;
            foreach my $f ($q->param()) {
                print "<input type=\"hidden\" name=\"$f\" value=\"".$q->param($f)."\">\n";
            }
            print "<input type=\"submit\" name=\"submit\" value=\"Show map\">";
            print "</form>\n";
        }
        print "</div>\n";
        print "</div>\n";
        print '<script language="JavaScript" type="text/javascript"> showTabText(6); </script>';
	}
	# collections
    if ($modules{7}) {
        print '<div id="panel7" class="panel">';
        if ($is_real_user) {
		    print doCollections($dbt, $s, $collectionsSet, $display_name, $taxon_no, $in_list, '', $is_real_user);
        } else {
            print '<div align="center">';
            print qq|<form method="POST" action="$READ_URL">|;
            foreach my $f ($q->param()) {
                print "<input type=\"hidden\" name=\"$f\" value=\"".$q->param($f)."\">\n";
            }
            print "<input type=\"submit\" name=\"submit\" value=\"Show age range and collections\">";
            print "</form>\n";
            print "</div>\n";
        }
        print "</div>\n";
        print '<script language="JavaScript" type="text/javascript"> showTabText(7); </script>';
	}

    if ($modules{8}) {
        print '<div id="panel8" class="panel">';
		print "<div align=\"center\"><h3>Images</h3></div>\n";
        print '<div align="center">';
        doThumbs($dbt,$in_list);
        print "</div>\n";
        print "</div>\n";
        print '<script language="JavaScript" type="text/javascript"> showTabText(8); </script>';
    }


    print "</div>"; # Ends div class="small" declared at start
}


sub getCollectionsSet {
    my ($dbt,$q,$s,$in_list,$taxon_name) = @_;

    my $fields = ['country', 'state', 'max_interval_no', 'min_interval_no','latdeg','latdec','latmin','latsec','latdir','lngdeg','lngdec','lngmin','lngsec','lngdir'];

    # Pull the colls from the DB;
    my %options = ();
    $options{'permission_type'} = 'read';
    $options{'calling_script'} = 'TaxonInfo';
    if ($in_list && @$in_list) {
        $options{'taxon_list'} = $in_list;
    } elsif ($taxon_name) {
        $options{'taxon_name'} = $taxon_name;
    }
    
    # These fields passed from strata module,etc
    #foreach ('group_formation_member','formation','geological_group','member','taxon_name') {
    #    if (defined($q->param($_))) {
    #        $options{$_} = $q->param($_);
    #    }
    #}
    my ($dataRows) = Collection::getCollections($dbt,$s,\%options,$fields);
    return $dataRows;
}

sub doThumbs {
    my ($dbt,$in_list) = @_;
    my $images_per_row = 6;
    my $thumbnail_size = 100;
	my @thumbs = Images::getImageList($dbt, $in_list);

    if (@thumbs) {
        print "<table border=0 cellspacing=8 cellpadding=0>";
        for (my $i = 0;$i < scalar(@thumbs);$i+= $images_per_row) {
            print "<tr>";
            for( my $j = $i;$j<$i+$images_per_row;$j++) {
                print "<td>";
                my $thumb = $thumbs[$j];
                if ($thumb) {
                    my $thumb_path = $thumb->{path_to_image};
                    $thumb_path =~ s/(.*)?(\d+)(.*)$/$1$2_thumb$3/;
                    my $caption = $thumb->{'caption'};
                    my $width = ($thumb->{'width'}  || 300);
                    my $height = ($thumb->{'height'}  || 400);
                    my $maxwidth = $width;
                    my $maxheight = $height;
                    if ( $maxwidth > 300 || $maxheight > 400 )	{
                        $maxheight = 400;
                        $maxwidth = 400 * $width / $height;
                    }
                    $height =  $maxheight + 130;
                    $width = $maxwidth;
                    if ( $width < 300 )	{
                        $width = 300;
                    }
                    $width += 50;
                    my $t_width=$thumbnail_size;
                    my $t_height=$thumbnail_size;
                    if ($thumb->{'width'} && $thumb->{'height'}) {
                        if ($thumb->{'width'} > $thumb->{'height'}) {
                            $t_width = $thumbnail_size;
                            $t_height = int($thumb->{'height'}/$thumb->{'width'}*$thumbnail_size);
                        } else {
                            $t_width = int($thumb->{'width'}/$thumb->{'height'}*$thumbnail_size);
                            $t_height = $thumbnail_size;
                        }
                    }

                    print "<a href=\"javascript: imagePopup('$READ_URL?action=displayImage&amp;image_no=$thumb->{image_no}&amp;maxheight=$maxheight&amp;maxwidth=$maxwidth&amp;display_header=NO',$width,$height)\">";
                    print "<img src=\"$thumb_path\" border=1 vspace=3 width=$t_width height=$t_height alt=\"$caption\">";
                    print "</a>";
                } else {
                    print "&nbsp;";
                }
                print "</td>";
            }
            print "</tr>";
        }
        print "</td></tr></table>";
    } else {
        print "<i>No images are available</i>";
    }
} 

sub displayMap {
    my ($dbt,$q,$s,$collectionsSet)  = @_;
    require Map;
    
	my @map_params = ('projection', 'maptime', 'mapbgcolor', 'gridsize', 'gridcolor', 'coastlinecolor', 'borderlinecolor', 'usalinecolor', 'pointshape1', 'dotcolor1', 'dotborder1');
	my %user_prefs = $s->getPreferences();
	foreach my $pref (@map_params){
		if($user_prefs{$pref}){
			$q->param($pref => $user_prefs{$pref});
		}
	}

	# we need to get the number of collections out of dataRowsRef
	#  before figuring out the point size
    my ($map_html_path,$errors,$warnings);
#    if (ref $in_list && @$in_list) {
        $q->param("simple_map"=>'YES');
        $q->param('mapscale'=>'auto');
        $q->param('autoborders'=>'yes');
        $q->param('pointsize1'=>'auto');
        my $m = Map->new($q,$dbt,$s);
        ($map_html_path,$errors,$warnings) = $m->buildMap('dataSet'=>$collectionsSet);
#    }

    # MAP USES $q->param("taxon_name") to determine what it's doing.
    if ( $map_html_path )	{
        if($map_html_path =~ /^\/public/){
            # reconstruct the full path the image.
            $map_html_path = $HTML_DIR.$map_html_path;
        }
        open(MAP, $map_html_path) or die "couldn't open $map_html_path ($!)";
        while(<MAP>){
            print;
        }
        close MAP;
    } else {
        print "<i>No distribution data are available</i>";
    }
}



# age_range_format changes appearance html formatting of age/range information, used by the strata module
sub doCollections{
    my ($dbt,$s,$colls,$display_name,$taxon_no,$in_list,$age_range_format,$is_real_user) = @_;
    my $dbh = $dbt->dbh;
    

    if (!@$colls) {
        print "<div align=\"center\"><h3>Collections</h3><i> No collection or age range data are available</i></div>";
        return;
    }

    my $interval_hash = getIntervalsData($dbt,$colls);
    my ($lb,$ub,$minfirst,$max,$min) = calculateAgeRange($dbt,$colls,$interval_hash);


#    print "MAX".Dumper($max);
#    print "MIN".Dumper($min);

	my $output = "";
    my $range = "";
    # simplified this because the users will understand the basic range,
    #  and it clutters the form JA 28.8.06
#    my $max = join (" or ",map {$interval_name{$_}} @$max);
#    my $min = join (" or ",map {$interval_name{$_}} @$min);
    my $max_no = $max->[0];
    my $min_no = $min->[0];
    $max = ($max_no) ? $interval_hash->{$max_no}->{interval_name} : "";
    $min = ($min_no) ? $interval_hash->{$min_no}->{interval_name} : ""; 
    if ($max ne $min) {
        $range .= "<a href=\"$READ_URL?action=displayInterval&interval_no=$max_no\">$max</a> to <a href=\"$READ_URL?action=displayInterval&interval_no=$min_no\">$min</a>";
    } else {
        $range .= "<a href=\"$READ_URL?action=displayInterval&interval_no=$max_no\">$max</a>";
    }
    $range .= " <i>or</i> $lb to $ub Ma";

    # I hate to hit another table, but we need to know whether ANY of the
    #  included taxa are extant JA 15.12.06
    my $extant;
    my $mincrownfirst;
    my %iscrown;
    if ( $in_list && @$in_list )	{
        my $taxon_row = ${$dbt->getData("SELECT lft,synonym_no FROM taxa_tree_cache WHERE taxon_no=$taxon_no")}[0];
        my $sql = "SELECT a.taxon_no taxon_no,lft,rgt FROM authorities a,taxa_tree_cache t WHERE synonym_no != $taxon_row->{synonym_no} AND lft != $taxon_row->{lft} AND extant='YES' AND a.taxon_no in (" . join (',',@$in_list) . ") AND a.taxon_no=t.taxon_no";
        my @extantchildren = @{$dbt->getData($sql)};
        # now for a big waste of time: the minimum age of the crown group
        #  must only involve extant, immediate children, so you need to know
        #  which of the children are extant JA 2.3.07
        if ( @extantchildren )	{
            # build some SQL that will be needed below
            my $lrsql;
            for my $ec ( @extantchildren )	{
                $lrsql .= " OR (lft<=".$ec->{'lft'}." AND rgt>=".$ec->{'rgt'}.")";
            }
            $lrsql =~ s/^ OR//;

            # get immediate child taxa
            my $taxon_records = TaxaCache::getChildren($dbt,$taxon_no,'immediate_children');
            my @children = @{$taxon_records};
            my @childnos;
            for my $c ( @children )	{
                push @childnos, $c->{'taxon_no'};
            }

            # get immediate children that include extant children
            if (@childnos && $lrsql) {
                $extant = 1;
                my $sql = "SELECT taxon_no,lft,rgt FROM taxa_tree_cache WHERE taxon_no IN (" . join (',',@childnos) . ") AND (" . $lrsql . ")";
                my @extantimmediates = @{$dbt->getData($sql)};

                # get children of immediate children that include extant children
                my $sql = "SELECT taxon_no FROM taxa_tree_cache WHERE";
                for my $ei ( @extantimmediates )	{
                    $sql .= " (lft>=".$ei->{'lft'}." AND rgt<=".$ei->{'rgt'}.") OR ";
                }
                $sql =~ s/ OR $//;
                my @extantcladechildren = @{$dbt->getData($sql)};

                # get collections including the living immediate children
                # another annoying table hit!
                my $extant_list;
                for my $ecc ( @extantcladechildren )	{
                    $extant_list .= "$ecc->{'taxon_no'},";
                }
                $extant_list =~ s/,$//;

                # Pull the colls from the DB;
                my %options = ();
                $options{'permission_type'} = 'read';
                $options{'calling_script'} = "TaxonInfo";
                $options{'taxon_list'} = $extant_list;
                # These fields passed from strata module,etc 
                #foreach ('group_formation_member','formation','geological_group','member','taxon_name') {
                #    if (defined($q->param($_))) {
                #        $options{$_} = $q->param($_);                                                                               
                #    }                                                                                                               
                #}                                                                                                                   
                my $fields = ["country", "state", "max_interval_no", "min_interval_no"];

                my ($dataRows,$ofRows) = Collection::getCollections($dbt,$s,\%options,$fields);
                my ($lb,$ub,$minfirst,$max,$min) = calculateAgeRange($dbt,$dataRows,$interval_hash);
                for my $coll ( @$dataRows )	{
                    $iscrown{$coll->{'collection_no'}}++;
                }
                $mincrownfirst = $minfirst;
            }
        }
    }

    if ( $minfirst && $extant && $age_range_format ne 'for_strata_module' )	{
        $range = "<div style=\"width: 40em; margin-left: auto; margin-right: auto; text-align: left; white-space: nowrap;\">Maximum range based only on fossils: " . $range . "<br>\n";
        $range .= "Minimum age of oldest fossil (stem group age): $minfirst Ma<br>\n";
        $range .= "Minimum age of oldest fossil in any extant subgroup (crown group age): $mincrownfirst Ma<br>";
        $range .= "<span class=\"verysmall\" style=\"padding-left: 2em;\"><i>Collections with crown group taxa are in <b>bold</b>.</i></span></div><br>\n";
    }

    if ($age_range_format eq 'for_strata_module') {
        print "<b>Age range:</b> $range <br><hr><br>"; 
    } else {
        print "<div align=\"center\"><h3><b>Age range</b></h3></div>\n $range<br><hr>";
    }

    
	# figure out which intervals are too vague to use to set limits on
	#  the joint upper and lower boundaries
	# "vague" means there's some other interval falling entirely within
	#  this one JA 26.1.05
    # Don't do it this way, not reliable

    # sort the collections by taxon name so the names can be printed just once
    #  per set of collections sharing the same taxon
    @{$colls} = sort { $a->{genera} cmp $b->{genera} } @{$colls};

	# Process the data:  group all the collection numbers with the same
	# time-place string together as a hash.
	my %time_place_coll = ();
    my (%max_interval_name,%min_interval_name,%bounds_coll,%max_interval_no);
    my %lastgenus = ();
    my %intervals = ();
	foreach my $row (@$colls) {
        my $max = $row->{'max_interval_no'};
        my $min = $row->{'min_interval_no'};
        if (!$min) {
            $min = $max;
        }
        my $res = "<span class=\"small\"><a href=\"$READ_URL?action=displayInterval&interval_no=$row->{max_interval_no}\">$interval_hash->{$max}->{interval_name}</a>";
        if ( $max != $min ) {
            $res .= " - " . "<a href=\"$READ_URL?action=displayInterval&interval_no=$row->{min_interval_no}\">$interval_hash->{$min}->{interval_name}</a>";
        }
        $res .= "</span></td><td align=\"center\" valign=\"top\"><span class=\"small\"><nobr>";
        $res .= $interval_hash->{$max}->{lower_boundary} . " - ";
        $res =~ s/0+ / /;
        $res =~ s/\. /.0 /;
        if ( $max == $min )	{
            $res .= $interval_hash->{$max}->{upper_boundary};
        } else	{
            $res .= $interval_hash->{$min}->{upper_boundary};
        }
            $res .= "</nobr></span></td><td align=\"center\" valign=\"top\"><span class=\"small\">";
        $res =~ s/0+</</;
        $res =~ s/\.</.0</;

        $row->{"country"} =~ s/ /&nbsp;/;
        $res .= $row->{"country"};
        if($row->{"state"}){
            $row->{"state"} =~ s/ /&nbsp;/;
            $res .= " (" . $row->{"state"} . ")";
        }
        $res .= "</span>\n";

            my @letts = split //,$display_name;
            $row->{'genera'} =~ s/$display_name /$letts[0]\. /g;
            $row->{'genera'} =~ s/[A-Z]\. indet/$display_name indet/g;
	    if (exists $time_place_coll{$res})	{
                if ( $lastgenus{$res} ne $row->{'genera'} )	{
                    ${$time_place_coll{$res}}[$#{$time_place_coll{$res}}] .= ") ";
                    push(@{$time_place_coll{$res}}, $row->{'genera'} . " (" . $row->{'collection_no'});
                } else	{
                    push(@{$time_place_coll{$res}}, " " . $row->{'collection_no'});
                }
                $lastgenus{$res} = $row->{'genera'};
	    }
	    else	{
                $time_place_coll{$res}[0] = $row->{'genera'} . " (" . $row->{'collection_no'};
                $lastgenus{$res} = $row->{'genera'};
                #push(@order,$res);
            if ($interval_hash->{$min}->{'min_no'} == $max) {
                $max = $min;
            }
            if ($interval_hash->{$max}->{'max_no'} == $min) {
                $min = $max;
            }
            # create a hash array where the keys are the time-place strings
            #  and each value is a number recording the min and max
            #  boundary estimates for the temporal bins JA 25.6.04
            # this is kind of tricky because we want bigger bins to come
            #  before the bins they include, so the second part of the
            #  number recording the upper boundary has to be reversed
            my $upper = $interval_hash->{$max}->{upper_boundary};
            $max_interval_no{$res} = $max;
            $max_interval_name{$res} = $interval_hash->{$max}->{interval_name};
            $min_interval_name{$res} = $max_interval_name{$res};
            if ( $max != $min ) {
                $upper = $interval_hash->{$min}->{upper_boundary};
                $min_interval_name{$res} = $interval_hash->{$min}->{interval_name};
            }
            #if ( ! $toovague{$max." ".$min} && ! $seeninterval{$max." ".$min})	
            # WARNING: we're assuming upper boundary ages will never be
            #  greater than 999 million years
            my $lower = int($interval_hash->{$max}->{lower_boundary} * 1000);
            $upper = $upper * 1000;
            $upper = int(999000 - $upper);
            if ( $lower < 1000 )	{
                $lower = "000" . $lower;
            }
            elsif ( $lower < 10000 )	{
                $lower = "00" . $lower;
            }
            elsif ( $lower < 100000 )	{
                $lower = "0" . $lower;
            }
            $bounds_coll{$res} = $lower . $upper;
            $intervals{$max} = 1 if ($max);
            $intervals{$min} = 1 if ($min);
	    }
	}

    my %parents;
    my %best_correlation;
    foreach my $interval (keys %intervals) {
        $parents{$interval} = join(" ",reverse getParentIntervals($interval,$interval_hash));
        my $sql = "SELECT c.correlation_no FROM correlations c, scales s, refs r WHERE c.scale_no=s.scale_no AND s.reference_no=r.reference_no AND c.interval_no=$interval ORDER by r.pubyr DESC LIMIT 1";  
        $best_correlation{$interval} = ${$dbt->getData($sql)}[0]->{'correlation_no'};
    }

#    use Data::Dumper; print Dumper(\%parents);
#    print Dumper(\%best_correlation);

	# sort the time-place strings temporally or by geographic location
	my @sorted = sort { 
        $bounds_coll{$b} <=> $bounds_coll{$a} || 
        $parents{$max_interval_no{$a}} cmp $parents{$max_interval_no{$b}} ||
        $best_correlation{$max_interval_no{$b}} <=> $best_correlation{$max_interval_no{$a}} ||
        $a cmp $b 
    } keys %bounds_coll;

#    foreach my $s (@sorted) {
#        print "$bounds_coll{$s}:$parents{$max_interval_no{$s}}:$best_correlation{$max_interval_no{$s}}:$s<BR>";
#    }

	# legacy: originally the sorting was just on the key
#	my @sorted = sort (keys %time_place_coll);

	if(scalar @sorted > 0){
		$output .= "<div align=\"center\"><h3 style=\"margin-bottom: .4em;\">Collections</h3>\n";
        my $collTxt = (scalar(@$colls)== 0) ? "None found"
                    : (scalar(@$colls) == 1) ? "One found"
                    : scalar(@$colls)." total";
        $output .= "($collTxt)</div>\n";
		if ( $#sorted <= 100 )	{
			$output .= "<br>\n";
		}

		$output .= "<table>\n";
		if ( $#sorted > 100 )	{
			$output .= qq|<tr>
<td colspan="3"><p class=\"large\"><b>Oldest occurrences</b></p>
</tr>|;
		}
		$output .= qq|<tr>
<th align="center">Time interval</th>
<th align="center">Ma</th>
<th align="center">Country or state</th>
<th align="left">PBDB collection number</th></tr>
|;

	# overload rule: if there are more than 100 rows, print only the
	#  first and last 10 for an extinct taxon, and the oldest 20 for
	#   an extant taxon JA 6.5.07
		if ( $#sorted > 100 )	{
			my @temp = @sorted;
			if ( $extant == 0 )	{
				@sorted = splice @temp , 0 , 10;
				push @sorted , ( splice @temp , $#temp - 9 , 10 );
			} else	{
				@sorted = splice @temp , 0 , 20;
			}
		}
		my $row_color = 0;
		foreach my $key (@sorted){
			if($row_color % 2 == 0){
				$output .= "<tr class='darkList'>";
			} 
			else{
				$output .= "<tr>";
			}
			$output .= "<td align=\"center\" valign=\"top\">$key</td>".
                       " <td align=\"left\"><span class=\"small\">";
			foreach my $collection_no (@{$time_place_coll{$key}}){
				my $formatted_no = $collection_no;
				if ( $iscrown{$collection_no} > 0 )	{
					$formatted_no = "<b>".$formatted_no."</b>";
				}
                                my $no = $collection_no;
                                $no =~ s/[^0-9]//g;
				$formatted_no =~ s/([0-9])/<a href=\"$READ_URL?action=displayCollectionDetails&amp;collection_no=$no&amp;is_real_user=$is_real_user\">$1/;
                                $output .= $formatted_no . "</a> ";
				#$output .= "<a href=\"$READ_URL?action=displayCollectionDetails&amp;collection_no=$no&amp;is_real_user=$is_real_user\">$formatted_no</a> ";
			}
			$output =~ s/([0-9])(<\/a>)( )$/$1\)$2/g;
			$output .= "</span></td></tr>\n";
			$row_color++;
			if ( $row_color == 10 && $output =~ /Oldest/ && $extant == 0 )	{
				$output .= qq|
<tr>
<td colspan="3"><p class="large" style="padding-top: 0.5em;"><b>Youngest occurrences</b></p></td>
</tr>
<tr>
<th align="center">Time interval</th>
<th align="center">Ma</th>
<th align="center">Country or state</th>
<th align="left">PBDB collection number</th></tr>
|;
			}
		}
		$output .= "</table>";
	} 
	return $output;
}

# Utility function.  This will get boundary information as well as max and min interval and next interval
# all max interval and min interval nos (as well as higher order time terms) for use in passing
# to calculateAgeRange
sub getIntervalsData {
    my ($dbt,$data) = @_;

	# get a lookup of the boundary ages for all intervals JA 25.6.04
	# the boundary age hashes are keyed by interval nos
    my $t = new TimeLookup($dbt);
 
    my $interval_hash = {};

    my $get_all_data = 0;
    my @itvs = ();
    if (ref($data) eq 'ARRAY') {
        my %seen_itv = ();
        foreach my $row (@$data) {
            $seen_itv{$row->{'max_interval_no'}} = 1 if ($row->{max_interval_no});
            $seen_itv{$row->{'min_interval_no'}} = 1 if ($row->{min_interval_no});
        }
        @itvs = keys %seen_itv;
    } elsif ($data eq 'all') {
        $get_all_data = 1;
    }
  
    # this look gets boundaries for all intervals the in the collection set, as 
    # well as all parents (broader) of those intervals, which is used below
    # in calculateAgeRange for pruning purposes
    for(my $i=0;$i<20;$i++) {
        my %get_itv = ();
        my $where = "";
        if (!$get_all_data) {
            last if (!@itvs);
            $where = " AND i.interval_no IN (".join(",",@itvs).")";
        }
        my $sql = "SELECT i.interval_no,TRIM(CONCAT(i.eml_interval,' ',i.interval_name)) AS interval_name,il.interval_hash,il.lower_boundary,il.upper_boundary FROM interval_lookup il, intervals i WHERE il.interval_no=i.interval_no$where";
        my @data = @{$dbt->getData($sql)};
        foreach my $row (@data) {
            my $itv = $t->deserializeItv($row->{'interval_hash'});
            $interval_hash->{$row->{interval_no}} = $itv; 
            if ($itv->{'max_no'}) {
                $get_itv{$itv->{'max_no'}} = 1 
            }
            if ($itv->{'min_no'}) {
                $get_itv{$itv->{'min_no'}} = 1 
            }
        }
        last if ($get_all_data);
        my @not_yet_fetched = ();
        foreach my $i (keys %get_itv) {
            if (!$interval_hash->{$i}) {
                push @not_yet_fetched, $i;
            }
        }
        @itvs = @not_yet_fetched;
    }
    return $interval_hash;
}

sub getParentIntervals {
    my ($i,$hash) = @_;
    my @intervals = ();
    my @q = ($i);
    my %seen = ();
    while (my $i = pop @q) {
        my $itv = $hash->{$i};
        if ($itv->{'max_no'} && !$seen{$itv->{'max_no'}}) {
            $seen{$itv->{'max_no'}} = 1;
            push @q, $itv->{'max_no'};
            push @intervals, $itv->{'max_no'};
        }
        if ($itv->{'min_no'} && $itv->{'min_no'} != $itv->{'max_no'} && !$seen{$itv->{'min_no'}}) {
            $seen{$itv->{'min_no'}} = 1;
            push @q, $itv->{'min_no'};
            push @intervals, $itv->{'min_no'};
        }
    } 
    return @intervals;
}


# This goes through all collections and finds a "minimal cover". Each collection corresponds to a time range
# i.e. 40-50 m.y. ago, 10-45 m.y. ago.  The time range that should be reported back is the minimally sized
# range that can inserts with the range of every single collection
#   Algorithm to do this is straightfoward. Imagine a set of ranges with overlap, "minimal cover" shown at bottom
#           A[-- -- --]
#       B[------] C[--]
#    D[-- --- --]
#            |========|
#   The lower end of the "minimal cover" must be lower than the upper end of _all_ intervals
#   Conversely, the upper end of the minimal cover must be greater than the lower end of all intervals.  
#   So, just find the ub and lb that satisfy this condition, then lookup the intervals(s) that correspond
#   to the lb and ub and return the best ones;
#   No longer throw out intervals beforehand.  If we throw out A and B for being too vague, then 
#   the cover will be from D --> C.  So don't throw anything out till the very end
sub calculateAgeRange {
    my ($dbt,$data,$interval_hash)  = @_;
    my %all_ints = (); 
    my %seen_range = ();

    foreach my $row (@$data)	{
        # First cast max/min into these variables.
        my $max = $row->{'max_interval_no'};
        my $min = $row->{'min_interval_no'};
        $all_ints{$max} = 1 if ($max);
        $all_ints{$min} = 1 if ($min);
        # If min is blank, set it to be the same as max (always) so we have a canonical representation
        if (!$min) {
            $min = $max;
        }
        my $lb_max = $interval_hash->{$max}->{lower_boundary};
        my $ub_max = $interval_hash->{$max}->{upper_boundary};
        my $lb_min = $interval_hash->{$min}->{lower_boundary};
        my $ub_min = $interval_hash->{$min}->{upper_boundary};
        dbg("MAX $max MIN $min LB $lb_max $lb_min UB $ub_max $ub_min");
        if ($ub_min !~ /\d/) {
            $lb_min = $interval_hash->{$max}->{lower_boundary};
            $ub_min = $interval_hash->{$max}->{upper_boundary};
        }
        my $range = "$max $min";
        if ($lb_max && $ub_max && $ub_max > $lb_max) {
            dbg("FLIPPING");
            my $tmp1 = $ub_max;
            my $tmp2 = $ub_min;
            $ub_max = $lb_max;
            $ub_min = $lb_min;
            $lb_max = $tmp1;
            $lb_min = $tmp2;
            $range = "$min $max";
        } 
        $seen_range{$range} = [$lb_max,$lb_min,$ub_max,$ub_min];
    }
    my @intervals = keys %all_ints;

    # First step in finding the minimal cover.  Find the bounds
    # that we have to cover.  Any range must at least be younger 
    # then the oldest upperbound ($oldest_ub) and older than
    # the youngest lowerbound($youngest_ub). So first find these two values, skipping
    # over "vague" interval ranges
    my $oldest_ub = -1;
    my $youngest_lb = 999999;
    while (my ($range,$bounds) = each %seen_range) {
        my ($max,$min) = split(/ /,$range);
        my ($lb,$x1,$x2,$ub) = @$bounds;

        if ($ub =~ /\d/ && $ub > $oldest_ub) {
            $oldest_ub = $ub;
        }
        if ($lb && $lb < $youngest_lb) {
            $youngest_lb = $lb;
        }
    }
    dbg("OLDEST_UB $oldest_ub - YOUNGEST_LB $youngest_lb");


    # Next step in finding minimal cover
    my $best_lb = 999999;
    my $best_ub = -1;
    while (my ($range,$bounds) = each %seen_range) {
        my ($max,$min) = split(/ /,$range);
        my ($lb_max,$ub_max,$lb_min,$ub_min) = @$bounds;
        if ($lb_max && $lb_max > $oldest_ub && $lb_max < $best_lb) {
            if ($lb_min < $youngest_lb && $youngest_lb < $oldest_ub) {
                # See calippus for the purpose for this - if we don't have this caluse upper 
                # boundary is set to Miocenes upper bound, which is bad since Miocenes lower bound
                # extends BEYOND what we're using for the lower bound
                dbg("THREW OUT LB $lb_max -- MAX $max, $lb_max-$lb_min MIN $min $ub_max-$ub_min");
            } else {
                dbg("BEST_LB SET TO $lb_max -- MAX $max, $lb_max-$lb_min MIN $min $ub_max-$ub_min");
                $best_lb = $lb_max;
            }
        }
        if ($ub_min =~ /\d/ && $ub_min < $youngest_lb && $ub_min > $best_ub) {
            if ($ub_max > $oldest_ub && $youngest_lb < $oldest_ub) {
                dbg("THREW OUT UB $ub_min -- MAX $max, $lb_max-$lb_min MIN $min $ub_max-$ub_min");
            } else {
                dbg("BEST_UB SET TO $ub_min -- MAX $max, $lb_max-$lb_min MIN $min $ub_max-$ub_min");
                $best_ub = $ub_min;
            }
        }
    }
    dbg("BEST_LB $best_lb - BEST_UB $best_ub");

    # We've found our minimal cover now but there may be multiple
    # intervals which can satisfy it.  Store all potential candidates
    # in the best_lb_ints (best lower boundary intervals) and best_ub_ints
    # hashs.
    my %best_lb_ints = ();
    my %best_ub_ints = ();
    while (my ($range,$bounds) = each %seen_range) {
        my ($max,$min) = split(/ /,$range);
        my ($lb,$x1,$x2,$ub) = @$bounds;
        if ($lb == $best_lb) {
            $best_lb_ints{$max} = 1;
        }
        if ($ub == $best_ub) {
            $best_ub_ints{$min} = 1;
        }
    }
    my @max = keys %best_lb_ints;
    my @min = keys %best_ub_ints;

    # Some dirty hacks for when we have multiple intervals that can be printed as a upper or lower boundary
    # We want to throw out the intervals we can.  Sometimes having multiple intervals
    # is legitimate and we should return both .  Sometimes it isn't and its just
    # weird data.  Examples of weird data:  1. Quaternary/Tertiary is "orphaned" since its not a 
    # legit time term, so manually remove that if we have to.  2. Early Cenomanian,Middle Cenomanian, Cenomanian
    # have the same bounds since they have no lowerbound entered: early and middle inherit it from Cenomanian 
    # Three ways of throwing out intervals: 1. Check is one is parent of other.  I.E. throw out Cenomanian is we
    # have early cenomanian.  2. Are in same scale.  Throw out the later or earlier interval in the scale for
    # max and min respectively.  3. Orphaned junk like Teritiary.  Just call a function to check for this

    my %parent = ();
    my %precedes = ();
    foreach my $i (@max,@min) {
        
        my @p = getParentIntervals($i,$interval_hash);
        foreach my $p (@p) {
            $parent{$i}{$p} = 1 unless $i == $p;
        }
#        my @q = ($ig->{$i});
#        while (my $j = pop @q) {
#            $parent{$i}{$j->{'interval_no'}} = 1 unless $i == $j->{'interval_no'};
#            push @q, $j->{'max'} if ($j->{'max'});
#            push @q, $j->{'min'} if ($j->{'min'} && $j->{'min'} != $j->{'max'});
#        }
        my @q = ($i);
        while (my $j = pop @q) {
            $precedes{$i}{$j} = 1 unless $i == $j;
            my $itv_j = $interval_hash->{$j};
            foreach my $n (@{$itv_j->{'all_next_nos'}}) {
                unless ($precedes{$i}{$n}) {
                    push @q, $n;
                }
            }
        }
    }
    # Deal with older term first. Try to get down to only 1 interval by throwing
    # out broader (parent) intervals and younger intervals
    my %all_max = (); 
    $all_max{$_} = 1 for @max;
    foreach my $i1 (@max) {
        # If we're down to 1, exit
        last if scalar keys %all_max == 1;
        foreach my $i2 (@max) {
            if ($parent{$i2}{$i1}) {
                dbg("REMOVING $i1, its a parent of $i2");
                delete $all_max{$i1};
            } elsif ($precedes{$i2}{$i1}) {
                dbg("REMOVING $i1, its is younger than $i2");
                delete $all_max{$i1};
            }
        }
        if ($interval_hash->{$i1}->{is_obsolete}) {
            dbg("REMOVING $i1, it is obsolete");
            delete $all_max{$i1};
        }
    }

    # Deal with younger term. Try to get down to only 1 interval by throwing
    # out broader (parent) intervals and older intervals
    my %all_min= (); 
    $all_min{$_} = 1 for @min;
    foreach my $i1 (@min) {
        # If we're down to 1, exit
        last if scalar keys %all_min == 1;
        foreach my $i2 (@min) {
            if ($parent{$i2}{$i1}) {
                dbg("REMOVING $i1, its a parent of $i2");
                delete $all_min{$i1};
            } elsif ($precedes{$i1}{$i2}) {
                dbg("REMOVING $i1, its is older than $i2");
                delete $all_min{$i1};
            }
        }
        if ($interval_hash->{$i1}->{is_obsolete}) {
            dbg("REMOVING $i1, it is obsolete");
            delete $all_min{$i1};
        }
    }

    @max = keys %all_max;
    @min = keys %all_min;
    $best_lb =~ s/00$//;
    $best_ub =~ s/00$//;
    if ($best_lb == 999999) {
        $best_lb = '';
    }
    if ($best_ub == -1) {
        $best_ub = '';
    }
    return ($best_lb,$best_ub,$oldest_ub,\@max,\@min);
}


## displayTaxonClassification
#
# SEND IN GENUS OR HIGHER TO GENUS_NAME, ONLY SET SPECIES IF THERE'S A SPECIES.
##
sub displayTaxonClassification {
    my ($dbt,$orig_no,$taxon_name,$is_real_user) = @_;
    my $dbh = $dbt->dbh;

    my $output; # the html actually returned by the function

    # These variables will reflect the name as currently used
    my ($taxon_no,$taxon_rank) = (0,"");
    # the classification variables refer to the taxa derived from the taxon_no we're using for classification
    # purposes.  If we found an exact match in the authorities table this classification_no wil
    # be the same as the original combination taxon_no for an authority. If we passed in a Genus+species
    # type combo but only the genus is in the authorities table, the classification_no will refer
    # to the genus
    my ($classification_no,$classification_name,$classification_rank);

    if ($orig_no) {
        my $taxon = getMostRecentSpelling($dbt,$orig_no);
        $taxon_no = $taxon->{'taxon_no'};    
        $taxon_name = $taxon->{'taxon_name'};    
        $taxon_rank = $taxon->{'taxon_rank'};    
        $classification_no = $orig_no;
        $classification_name = $taxon_name;
        $classification_rank = $taxon_rank;
        
    } else {
        # Theres are some case where we might want to do upward classification when theres no taxon_no:
        #  The Genus+species isn't in authorities, but the genus is
        #  The exact taxa isn't in the authorities, but something close is (i.e. the Genus+species matches 
        #  The Subgenus+species of some taxon
        my ($genus,$subgenus,$species,$subspecies) = Taxon::splitTaxon($taxon_name);
        $classification_no = Taxon::getBestClassification($dbt,'',$genus,'',$subgenus,'',$species);
        if ($classification_no) {
            my $taxon = getTaxa($dbt,{'taxon_no'=>$classification_no});
            $classification_name = $taxon->{'taxon_name'};
            $classification_rank = $taxon->{'taxon_rank'};
        }
    }
   
    my ($genus,$subgenus,$species,$subspecies) = Taxon::splitTaxon($taxon_name);
    my ($c_genus,$c_subgenus,$c_species,$c_subspecies) = Taxon::splitTaxon($classification_name);


    # Do the classification
    my @table_rows = ();
    if ($classification_no) {
        # Now find the rank,name, and publication of all its parents
        my $orig_classification_no = getOriginalCombination($dbt,$classification_no);
        my $parent_hash = TaxaCache::getParents($dbt,[$orig_classification_no],'array_full');
        my @parent_array = @{$parent_hash->{$orig_classification_no}}; 
        
        if (@parent_array) {
            my ($subspecies_no,$species_no,$subgenus_no,$genus_no) = (0,0,0,0);
            # Set for focal taxon
            $subspecies_no = $taxon_no if ($taxon_rank eq 'subspecies');
            $species_no = $taxon_no if ($taxon_rank eq 'species');
            $subgenus_no = $taxon_no if ($taxon_rank eq 'subgenus');
            $genus_no = $taxon_no if ($taxon_rank eq 'genus');
            foreach my $row (@parent_array) {
                # Set for all possible higher taxa
                # Handle species/genus separately below.  The reason for this is the "loose" classification that
                # the PBDB does.  Taxon::getBestClassification will find a proximate match if we can't
                # find an exact match in the database.  Because of this, some of the lower level names
                # (genus,subgenus,species,subspecies) may not match up exactly from what the user entered
                $subspecies_no = $row->{'taxon_no'} if ($row->{'taxon_rank'} eq 'subspecies');
                $species_no = $row->{'taxon_no'} if ($row->{'taxon_rank'} eq 'species');
                $subgenus_no = $row->{'taxon_no'} if ($row->{'taxon_rank'} eq 'subgenus');
                $genus_no = $row->{'taxon_no'} if ($row->{'taxon_rank'} eq 'genus');
             
                if ($row->{'taxon_rank'} !~ /species|genus/) {
                    push (@table_rows,[$row->{'taxon_rank'},$row->{'taxon_name'},$row->{'taxon_name'},$row->{'taxon_no'}]);
                }
                last if ($row->{'taxon_rank'} eq 'kingdom');
            }
            if ($genus_no) {
                unshift @table_rows, ['genus',$genus,$genus,$genus_no];
            } elsif ($classification_no) {
                unshift @table_rows, [$classification_rank,$classification_name,$classification_name,$classification_no];
            }
            if ($subgenus) {
                unshift @table_rows, ['subgenus',"$genus ($subgenus)",$subgenus,$subgenus_no];
            }
            if ($species) {
                my $species_name = "$genus $species";
                if ($subgenus) {
                    $species_name = "$genus ($subgenus) $species";
                } 
                unshift @table_rows, ['species',"$species_name",$species,$species_no];
            }
            if ($subspecies) {
                unshift @table_rows, ['subspecies',"$taxon_name",$subspecies,$subspecies_no];
            }

            #
            # Print out the table in the reverse order that we initially made it
            #
            $output .= "<table><tr><td valign=\"top\">\n";
            $output .= "<table><tr valign=top><th>Rank</th><th>Name</th><th>Author</th></tr>";
            my $class = '';
            for(my $i = scalar(@table_rows)-1;$i>=0;$i--) {
                if ( $i == int((scalar(@table_rows) - 2) / 2) )	{
                    $output .= "\n</td></tr></table>\n\n";
                    $output .= "\n</td><td valign=\"top\" style=\"width: 2em;\"></td><td valign=\"top\">\n\n";
                    $output .= "<table><tr valign=top><th>Rank</th><th>Name</th><th>Author</th></tr>";
                }
                $class = $class eq '' ? 'class="darkList"' : '';
                $output .= "<tr $class>";
                my($taxon_rank,$taxon_name,$show_name,$taxon_no) = @{$table_rows[$i]};
                if ($taxon_rank eq 'unranked clade') {
                    $taxon_rank = "&mdash;";
                }
                my $authority;
                if ($taxon_no) {
                    $authority = getTaxa($dbt,{'taxon_no'=>$taxon_no},['author1last','author2last','otherauthors','pubyr','reference_no','ref_is_authority']);
                }
                my $pub_info = Reference::formatShortRef($authority);
                if ($authority->{'ref_is_authority'} =~ /yes/i) {
                    $pub_info = "<a href=\"$READ_URL?action=displayReference&amp;reference_no=$authority->{reference_no}&amp;is_real_user=$is_real_user\">$pub_info</a>";
                }
                my $orig_no = getOriginalCombination($dbt,$taxon_no);
                if ($orig_no != $taxon_no) {
                    $pub_info = "(".$pub_info.")" if $pub_info !~ /^\s*$/;
                } 
                my $link;
                if ($taxon_no) {
                    $link = qq|<a href="$READ_URL?action=checkTaxonInfo&amp;taxon_no=$taxon_no&amp;is_real_user=$is_real_user">$show_name</a>|;
                } else {
                    $link = qq|<a href="$READ_URL?action=checkTaxonInfo&amp;taxon_name=$taxon_name&amp;is_real_user=$is_real_user">$show_name</a>|;
                }
                $output .= qq|<td align="center">$taxon_rank</td>|.
                           qq|<td align="center">$link</td>|.
                           qq|<td align="center" style="white-space: nowrap">$pub_info</td>|; 
                $output .= '</tr>';
            }
            $output .= "</table>";
            $output .= "</td></tr></table>";
           
            if ($classification_no != $orig_no) {
                $output .= "<div class=\"warning\" style=\"width: 600px;\">";
                $output .= "A formal classification for '$taxon_name' could not be found.  An approximate match to '$classification_name' was used to create this classification.";
                $output .= "</div>";
            }
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
	my $orig_no = (shift or ""); #Pass in original combination no
    my $taxon_name = shift;
    my $is_real_user = shift;
    my ($genus,$subgenus,$species,$subspecies) = Taxon::splitTaxon($taxon_name);

    my $output = "";

    #
    # Begin getting sister/child taxa
    # PS 01/20/2004 - rewrite: Use getChildren function
    # First get the children
    #
    my $focal_taxon_no = $orig_no;
    my ($focal_taxon_rank,$parent_taxon_no);
    $focal_taxon_rank = Taxon::guessTaxonRank($taxon_name);
    if (!$focal_taxon_rank && $orig_no) {
        my $taxon = getTaxa($dbt,{'taxon_no'=>$orig_no});
        $focal_taxon_rank = $taxon->{'taxon_rank'};
    } 

    if ($orig_no) {
        my $parent = TaxaCache::getParent($dbt,$orig_no);
        if ($parent) {
            $parent_taxon_no = $parent->{'taxon_no'};
        }
    } else {
        my @bits = split(/ /,$taxon_name);
        pop @bits;
        my $taxon_parent = join(" ",@bits);
        my @parents = ();
        my $taxon_parent_rank = "";
        if ($taxon_parent) {
            $taxon_parent_rank = Taxon::guessTaxonRank($taxon_parent);
            #$taxon_parent_rank = 'genus' if (!$taxon_parent_rank);
            @parents = getTaxa($dbt,{'taxon_name'=>$taxon_parent});
        }
       
        if ($taxon_parent && scalar(@parents) == 1) {
            $parent_taxon_no=getOriginalCombination($dbt,$parents[0]->{'taxon_no'});
        }
    }

    my @child_taxa_links;
    # This section generates links for children if we have a taxon_no (in authorities table)
    if ($focal_taxon_no) {
	my $taxon_records = TaxaCache::getChildren($dbt,$focal_taxon_no,'immediate_children');
	my @children = @{$taxon_records};
#        my @syns = @{$tree->{'synonyms'}};
#        foreach my $syn (@syns) {
#            if ($syn->{'children'}) {
#                push @children, @{$syn->{'children'}};
#            }
#        }
	@children = sort {$a->{'taxon_name'} cmp $b->{'taxon_name'}} @children;
	if (@children) {
	    my $sql = "SELECT type_taxon_no FROM authorities WHERE taxon_no=$focal_taxon_no";
	    my $type_taxon_no = ${$dbt->getData($sql)}[0]->{'type_taxon_no'};
	    foreach my $record (@children) {
		my @syn_links;                                                         
		my @synonyms = @{$record->{'synonyms'}};
		push @syn_links, $_->{'taxon_name'} for @synonyms;
		my $link = qq|<a href="$READ_URL?action=checkTaxonInfo&amp;taxon_no=$record->{taxon_no}&amp;is_real_user=$is_real_user">$record->{taxon_name}|;
		$link .= " (syn. ".join(", ",@syn_links).")" if @syn_links;
		$link .= "</a>";
		if ($type_taxon_no && $type_taxon_no == $record->{'taxon_no'}) {
		    $link .= " <small>(type $record->{taxon_rank})</small>";
		}
		push @child_taxa_links, $link;
	    }
	}
    }

    # Get sister taxa as well
    # PS 01/20/2004
    my @sister_taxa_links;
    # This section generates links for sister if we have a taxon_no (in authorities table)
    if ($parent_taxon_no) {
	my @sisters = @{TaxaCache::getChildren($dbt,$parent_taxon_no,'immediate_children')};
        @sisters = sort {$a->{'taxon_name'} cmp $b->{'taxon_name'}} @sisters;
        if (@sisters) {
            foreach my $record (@sisters) {
                next if ($record->{'taxon_no'} == $focal_taxon_no);
                if ($focal_taxon_rank ne $record->{'taxon_rank'}) {
#                    PBDBUtil::debug(1,"rank mismatch $focal_taxon_rank -- $record->{taxon_rank} for sister $record->{taxon_name}");
                } else {
                    my @syn_links;
                    my @synonyms = @{$record->{'synonyms'}};
                    push @syn_links, $_->{'taxon_name'} for @synonyms;
                    my $link = qq|<a href="$READ_URL?action=checkTaxonInfo&amp;taxon_no=$record->{taxon_no}&amp;is_real_user=$is_real_user">$record->{taxon_name}|;
                    $link .= " (syn. ".join(", ",@syn_links).")" if @syn_links;
                    $link .= "</a>";
                    push @sister_taxa_links, $link;
                }
            }
        }
    }
    # This generates links if all we have is occurrences records
    my (@possible_sister_taxa_links,@possible_child_taxa_links);
    if ($taxon_name) {
        my ($sql,$whereClause,@results);
        my ($genus,$subgenus,$species,$subspecies) = Taxon::splitTaxon($taxon_name);
        my @names = ();
        if ($genus) {
            push @names, $dbh->quote($genus);
        }
        if ($subgenus) {
            push @names, $dbh->quote($subgenus);
        }
        if (@names) {
            my $genus_sql = "a.genus_name IN (".join(",",@names).")";
            my $subgenus_sql = " a.subgenus_name  IN (".join(",",@names).")";
            my ($occ_genus_no_sql,$reid_genus_no_sql) = ("","");
            #$occ_genus_no_sql = " OR a.taxon_no=$parents[0]->{taxon_no}" if (@parents);
            #$reid_genus_no_sql = " OR a.taxon_no=$parents[0]->{taxon_no}" if (@parents);
            # Note that the table aliased to "a" and "b" is switched up.  The table we want to dislay names for and do matches
            # against is "a" and the non-important table is "b"
            my $sql  = "(SELECT a.genus_name,a.subgenus_name,a.species_name,c.taxon_name FROM occurrences a LEFT JOIN reidentifications b ON a.occurrence_no=b.occurrence_no LEFT JOIN authorities c ON a.taxon_no=c.taxon_no WHERE b.reid_no IS NULL AND $genus_sql AND (a.species_reso IS NOT NULL AND a.species_reso NOT LIKE '%informal%'))";
            $sql .= " UNION ";
            $sql .= "(SELECT a.genus_name,a.subgenus_name,a.species_name,c.taxon_name FROM occurrences b, reidentifications a LEFT JOIN authorities c ON a.taxon_no=c.taxon_no WHERE a.occurrence_no=b.occurrence_no AND a.most_recent='YES' AND $genus_sql AND (a.species_reso IS NOT NULL AND a.species_reso NOT LIKE '%informal%'))";
            $sql .= " UNION ";
            $sql .= "(SELECT a.genus_name,a.subgenus_name,a.species_name,c.taxon_name FROM occurrences a LEFT JOIN reidentifications b ON a.occurrence_no=b.occurrence_no LEFT JOIN authorities c ON a.taxon_no=c.taxon_no WHERE b.reid_no IS NULL AND $subgenus_sql AND (a.species_reso IS NOT NULL AND a.species_reso NOT LIKE '%informal%'))";
            $sql .= " UNION ";
            $sql .= "(SELECT a.genus_name,a.subgenus_name,a.species_name,c.taxon_name FROM occurrences b, reidentifications a LEFT JOIN authorities c ON a.taxon_no=c.taxon_no WHERE a.occurrence_no=b.occurrence_no AND a.most_recent='YES' AND $subgenus_sql AND (a.species_reso IS NOT NULL AND a.species_reso NOT LIKE '%informal%'))";
            $sql .= " ORDER BY genus_name,subgenus_name,species_name";
            dbg("Get from occ table: $sql");
            @results = @{$dbt->getData($sql)};
            foreach my $row (@results) {
                next if ($row->{'species_name'} =~ /^sp(p)*\.|^indet\.|s\.\s*l\./);
                my ($g,$sg,$sp) = Taxon::splitTaxon($row->{'taxon_name'});
                my $match_level = 0;
                if ($row->{'taxon_name'}) {
                    $match_level = Taxon::computeMatchLevel($row->{'genus_name'},$row->{'subgenus_name'},$row->{'species_name'},$g,$sg,$sp);
                }
                if ($match_level < 20) { # For occs with only a genus level match, or worse
                    my $occ_name = $row->{'genus_name'};
                    if ($row->{'subgenus'}) {
                        $occ_name .= " ($row->{subgenus})";
                    }
                    $occ_name .= " ".$row->{'species_name'};
                    if ($species) {
                        if ($species ne $row->{'species_name'}) {
                            my $link = qq|<a href="$READ_URL?action=checkTaxonInfo&amp;taxon_name=$occ_name&amp;is_real_user=$is_real_user">$occ_name</a>|;
                            push @possible_sister_taxa_links, $link;
                        }
                    } else {
                        my $link = qq|<a href="$READ_URL?action=checkTaxonInfo&amp;taxon_name=$occ_name&amp;is_real_user=$is_real_user">$occ_name</a>|;
                        push @possible_child_taxa_links, $link;
                    }
                }
            }
        }
    }
   
    # Print em out
    if (@child_taxa_links) {
        $output .= "<p><i>This taxon includes:</i><br>"; 
        $output .= join(", ",@child_taxa_links);
        $output .= "</p>";
    }

    if (@possible_child_taxa_links) {
        $output .= "<p><i>This genus may include these species, but they have not been formally classified into it:</i><br>"; 
        $output .= join(", ",@possible_child_taxa_links);
        $output .= "</p>";
    }

    if (@sister_taxa_links) {
        my $rank = ($focal_taxon_rank eq 'species') ? 'species' :
                   ($focal_taxon_rank eq 'genus') ? 'genera' :
                                                    'taxa';
        $output .= "<p><i>Sister $rank include:</i><br>"; 
        $output .= join(", ",@sister_taxa_links);
        $output .= "</p>";
    }
    
    if (@possible_sister_taxa_links) {
        $output .= "<p><i>These species have not been formally classified into the genus:</i><br>"; 
        $output .= join(", ",@possible_sister_taxa_links);
        $output .= "</p>";
    }

    if (!$output) {
        $output = "<i> No related taxa found </i>";
    } 

    if ($orig_no) {
        $output .= '<p><b><a href=# onClick="javascript: document.doDownloadTaxonomy.submit()">Download authority and opinion data</a></b> - <b><a href=# onClick="javascript: document.doViewClassification.submit()">View classification of included taxa</a></b>';
        $output .= "<form method=\"POST\" action=\"$READ_URL\" name=\"doDownloadTaxonomy\">";
        $output .= '<input type="hidden" name="action" value="displayDownloadTaxonomyResults">';
        $output .= '<input type="hidden" name="taxon_no" value="'.$orig_no.'">';
        $output .= "</form>\n";
        $output .= "<form method=\"POST\" action=\"$READ_URL\" name=\"doViewClassification\">";
        $output .= '<input type="hidden" name="action" value="startProcessPrintHierarchy">';
        $output .= '<input type="hidden" name="maximum_levels" value="99">';
        $output .= '<input type="hidden" name="taxon_no" value="'.$orig_no.'">';
        $output .= "</form>\n";
        
    }
	return $output;
}

# Handle the 'Taxonomic history' section
sub displayTaxonHistory {
	my $dbt = shift;
	my $taxon_no = (shift or "");
    my $is_real_user = shift;
	
	my $output = "";  # html output...
	
	unless($taxon_no) {
		return "<i>No taxonomic history is available</i>";
	}

    # Surrounding able prevents display bug in firefox
	$output .= "<table><tr><td><ul>"; 
	my $original_combination_no = getOriginalCombination($dbt, $taxon_no);
	
	# Select all parents of the original combination whose status' are
	# either 'recombined as,' 'corrected as,' or 'rank changed as'
	my $sql = "SELECT DISTINCT(child_spelling_no), status FROM opinions WHERE child_no=$original_combination_no ";
	my @results = @{$dbt->getData($sql)};

	# Combine parent numbers from above for the next select below. If nothing
	# was returned from above, use the original combination number. Shouldn't be necessary but just in case
	my @parent_list = ();
    foreach my $rec (@results) {
        push(@parent_list,$rec->{'child_spelling_no'});
    }
    # don't forget the original (verified) here, either: the focal taxon	
    # should be one of its children so it will be included below.
    push(@parent_list, $original_combination_no);
	my @synonyms = getJuniorSynonyms($dbt,@parent_list);

	# Reduce these results to original combinations: shouldn't be necessary to do this
	#foreach my $rec (@synonyms) {
#		$rec = getOriginalCombination($dbt, $rec->{child_no});	
	#}

	# Get alternate "original" combinations, usually lapsus calami type cases.  Shouldn't exist, 
    # exists cause of sort of buggy data.
	$sql = "SELECT DISTINCT child_no FROM opinions ".
		   "WHERE child_spelling_no IN (".join(',',@parent_list).") ".
		   "AND child_no != $original_combination_no";
	my @more_orig = map {$_->{'child_no'}} @{$dbt->getData($sql)};

    # Remove duplicates
    my %results_no_dupes;
    @results_no_dupes{@synonyms} = ();
    @results_no_dupes{@more_orig} = ();
    @results = keys %results_no_dupes;

	
	
	# Print the info for the original combination of the passed in taxon first.
	$output .= getSynonymyParagraph($dbt, $original_combination_no,$is_real_user);

	# Get synonymies for all of these original combinations
    my @paragraphs = ();
	foreach my $child (@results) {
		my $list_item = getSynonymyParagraph($dbt, $child, $is_real_user);
		push(@paragraphs, "<br><br>$list_item\n") if($list_item ne "");
	}

	# Now alphabetize the rest:
	@paragraphs = sort {lc($a) cmp lc($b)} @paragraphs;
	foreach my $rec (@paragraphs) {
		$output .= $rec;
	}

	$output .= "</ul></td></tr></table>";
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
    my $is_real_user = shift;

    return unless $taxon_no;
	
	my %synmap1 = ('original spelling' => 'revalidated',
                   'recombination' => 'recombined as ',
				   'correction' => 'corrected as ',
				   'rank change' => 'reranked as ',
				   'reassigment' => 'reassigned as ');
                   
	my %synmap2 = ('belongs to' => 'revalidated ',
				   'replaced by' => 'replaced with ',
				   'nomen dubium' => 'considered a nomen dubium ',
				   'nomen nudum' => 'considered a nomen nudum ',
				   'nomen vanum' => 'considered a nomen vanum ',
				   'nomen oblitum' => 'considered a nomen oblitum ',
				   'homonym of' => ' considered a homonym of ',
				   'misspelling of' => 'misspelled as ',
				   'invalid subgroup of' => 'considered an invalid subgroup of ',
				   'subjective synonym of' => 'synonymized subjectively with ',
				   'objective synonym of' => 'synonymized objectively with ');
	my $text = "";

    # got rid of direct hit on opinions table and replaced it with a call
    #  to getMostRecentClassification because (1) the code was redundant, and
    #  (2) this way you can identify the opinion actually used in the taxon's
    #   classification, so it can be bolded in the history paragraph JA 17.4.07
    # whoops, need to get original combination first JA 12.6.07
    my $orig = getOriginalCombination($dbt, $taxon_no);	
    my @results = getMostRecentClassification($dbt,$orig);

    my $best_opinion;
    if (@results) {
        # save the best opinion no
        $best_opinion = $results[0]->{opinion_no};
        # getMostRecentClassification returns the opinions in reliability_index
        #  order, so now they need to be resorted based on pubyr
        @results = sort { $a->{pubyr} <=> $b->{pubyr} } @results;
    }

	# "Named by" part first:
	# Need to print out "[taxon_name] was named by [author] ([pubyr])".
	# - select taxon_name, author1last, pubyr, reference_no, comments from authorities

    my $taxon = getTaxa($dbt,{'taxon_no'=>$taxon_no},['taxon_no','taxon_name','taxon_rank','author1last','author2last','otherauthors','pubyr','reference_no','ref_is_authority','extant','preservation','type_taxon_no','type_specimen','comments']);
	
	# Get ref info from refs if 'ref_is_authority' is set
	if ( ! $taxon->{'author1last'} )	{
		my $rank = $taxon->{taxon_rank};
		my $article = "a";
		if ( $rank =~ /^[aeiou]/ )	{
			$article = "an";
		}
		my $rankchanged;
		for my $row ( @results )	{
			if ( $row->{'spelling_reason'} =~ /rank/ )	{
			# rank was changed at some point
				$text .= "<li><a href=\"$READ_URL?action=checkTaxonInfo&amp;taxon_no=$taxon->{taxon_no}&amp;is_real_user=$is_real_user\">$taxon->{taxon_name}</a> was named as $article $rank. ";
				$rankchanged++;
				last;
			}
		}
		# rank was never changed
		if ( ! $rankchanged )	{
			$text .= "<li><a href=\"$READ_URL?action=checkTaxonInfo&amp;taxon_no=$taxon->{taxon_no}&amp;is_real_user=$is_real_user\">$taxon->{taxon_name}</a> is $article $rank. ";
		}
	} else	{
		$text .= "<li><i><a href=\"$READ_URL?action=checkTaxonInfo&amp;taxon_no=$taxon->{taxon_no}&amp;is_real_user=$is_real_user\">$taxon->{taxon_name}</a></i> was named by ";
	        if ($taxon->{'ref_is_authority'}) {
			$text .= Reference::formatShortRef($taxon,'alt_pubyr'=>1,'show_comments'=>1,'link_id'=>1);
		} else {
			$text .= Reference::formatShortRef($taxon,'alt_pubyr'=>1,'show_comments'=>1);
		}
		$text .= ". ";
	}

    if ($taxon->{'extant'} =~ /YES/i) {
        $text .= "It is extant. ";
    } elsif ($taxon->{'extant'} =~ /NO/i) {
        $text .= "It is not extant. ";
    }

    # don't report preservation for extant taxa, because they are regular
    #  taxa by definition
    if ($taxon->{'preservation'} && $taxon->{'extant'} !~ /Y/i) {
        my $preservation = $taxon->{'preservation'};
        if ($preservation eq 'regular taxon') {
            $preservation = "not an ichnofossil or a form taxon";
        } elsif ($preservation =~ /^[aieou]/) {
            $preservation = "an $preservation";
        } else {
            $preservation = "a $preservation";
        }
        if ( $taxon->{'extant'} =~ /N/i )	{
            $text =~ s/\. $/ /;
            $text .= "and is $preservation. ";
        } else	{
            $text .= "It is $preservation. ";
        }
    }

    my @spellings = getAllSpellings($dbt,$taxon->{'taxon_no'});

    if ($taxon->{'taxon_rank'} =~ /species/) {
        my $sql = "SELECT taxon_no,type_specimen,type_body_part,part_details FROM authorities WHERE ((type_specimen IS NOT NULL and type_specimen != '') OR (type_body_part IS NOT NULL AND type_body_part != '') OR (part_details IS NOT NULL AND part_details != '')) AND taxon_no IN (".join(",",@spellings).")";
        my $specimen_row = ${$dbt->getData($sql)}[0];
        if ($specimen_row) {
            $text .= "Its type is $specimen_row->{type_specimen}";
            if ($specimen_row->{'type_body_part'}) {
                my $an = ($specimen_row->{'type_body_part'} =~ /^[aeiou]/) ? "an" : "a";
                $text .= ", " if ($specimen_row->{'type_specimen'});
                if ( $specimen_row->{type_body_part} =~ /teeth|postcrania|vertebrae|limb elements|appendages|ossicles/ )	{
                    $an = "a set of";
                }
                $text .= "$an $specimen_row->{type_body_part}";
            }
            if ($specimen_row->{'part_details'}) {
                $text .= " ($specimen_row->{part_details})";
            }
            $text .= ". ";
        }
    } else {
        my $sql = "SELECT taxon_no,type_taxon_no FROM authorities WHERE type_taxon_no != 0 AND taxon_no IN (".join(",",@spellings).")";
        my $tt_row = ${$dbt->getData($sql)}[0];
        if ($tt_row) {
            my $type_taxon = getTaxa($dbt,{'taxon_no'=>$tt_row->{'type_taxon_no'}});
            my $type_taxon_name = $type_taxon->{'taxon_name'};
            if ($type_taxon->{'taxon_rank'} =~ /genus|species/) {
                $type_taxon_name = "<i>".$type_taxon_name."</i>";
            }
            $text .= "Its type is <a href=\"$READ_URL?action=checkTaxonInfo&amp;taxon_no=$type_taxon->{taxon_no}&amp;is_real_user=$is_real_user\">$type_taxon_name</a>. ";  
        }
    }
    
    my $sql = "SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE type_taxon_no IN (".join(",",@spellings).")";
    my @type_for = @{$dbt->getData($sql)};
    if (@type_for) {
        $text .= "It is the type for ";
        foreach my $row (@type_for) {
            my $taxon_name = $row->{'taxon_name'};
            if ($row->{'taxon_rank'} =~ /genus|species/) {
                $taxon_name = "<i>".$taxon_name."</i>";
            }
            $text .= "<a href=\"$READ_URL?action=checkTaxonInfo&amp;taxon_no=$row->{taxon_no}&amp;is_real_user=$is_real_user\">$taxon_name</a>, ";
        }
        $text =~ s/, $/. /;
    }
    
   my %phyly = ();
    foreach my $row (@results) {
        if ($row->{'phylogenetic_status'}) {
            push @{$phyly{$row->{'phylogenetic_status'}}},$row
        }
    }
    my @phyly_list = keys %phyly;
    if (@phyly_list) {
        my $para_text = " It was considered ";
        @phyly_list = sort {$phyly{$a}->[-1]->{'pubyr'} <=> $phyly{$b}->[-1]->{'pubyr'}} @phyly_list;
        foreach my $phylogenetic_status (@phyly_list) {
            $para_text .= " $phylogenetic_status by ";
            my $parent_block = $phyly{$phylogenetic_status};
            $para_text .= printReferenceList($parent_block,$best_opinion);
            $para_text .= ", ";
        }
        $para_text =~ s/, $/\./;
        my $last_comma = rindex($para_text,",");
        if ($last_comma >= 0) {
            substr($para_text,$last_comma,1," and ");
        }
        $text .= $para_text;
    }

    $text .= "<br><br>";


    # We want to group opinions together that have the same spelling/parent
    # We do this by creating a double array - $syns[$group_index][$child_index]
    # where all children having the same parent/spelling will have the same group index
    # the hashs %(syn|rc)_group_index keep track of what the $group_index is for each clump
    my (@syns,@nomens,%syn_group_index,%rc_group_index);
    my $list_revalidations = 0;
	# If something
	foreach my $row (@results) {
		# put all syn's referring to the same taxon_name together
        if ($row->{'status'} =~ /subgroup|synonym|homonym|replaced|misspell/) {
            if (!exists $syn_group_index{$row->{'parent_spelling_no'}}) {
                $syn_group_index{$row->{'parent_spelling_no'}} = scalar(@syns);
            }
            my $index = $syn_group_index{$row->{'parent_spelling_no'}};
            push @{$syns[$index]},$row;
            $list_revalidations = 1;
        } elsif ($row->{'status'} =~ /nomen/) {
	        # Combine all adjacent like status types @nomens
	        # (They're chronological: nomen, reval, reval, nomen, nomen, reval, etc.)
            my $index;
            if (!@nomens) {
                $index = 0;
            } elsif ($nomens[$#nomens][0]->{'status'} eq $row->{'status'}) {
                $index = $#nomens;
            } else {
                $index = scalar(@nomens);
            }
            push @{$nomens[$index]},$row;
            $list_revalidations = 1;
        } elsif ($row->{'status'} =~ /corr|rank|recomb/ || $row->{'spelling_reason'} =~ /^corr|^rank|^recomb|^reass/) {
            if (!exists $rc_group_index{$row->{'child_spelling_no'}}) {
                $rc_group_index{$row->{'child_spelling_no'}} = scalar(@syns);
            }
            my $index = $rc_group_index{$row->{'child_spelling_no'}};
            push @{$syns[$index]},$row;
            $list_revalidations = 1;
        } elsif (($row->{'status'} =~ /belongs/ && $list_revalidations && $row->{'spelling_reason'} !~ /^recomb|^corr|^rank|^reass/)) {
            # Belongs to's are only considered revalidations if they come
            # after a recombined as, synonym, or nomen *
            my $index;
            if (!@nomens) {
                $index = 0;
            } elsif ($nomens[$#nomens][0]->{'status'} eq $row->{'status'}) {
                $index = $#nomens;
            } else {
                $index = scalar(@nomens);
            }
            push @{$nomens[$index]},$row;
        }
	}
   
    # Now combine the synonyms and nomen/revalidation arrays, with the nomen/revalidation coming last
    my @synonyms = (@syns,@nomens);
	
	# Exception to above:  the most recent opinion should appear last. Splice it to the end
    if (@synonyms) {
        my $oldest_pubyr = 0;
        my $oldest_group = 0; 
        for(my $i=0;$i<scalar(@synonyms);$i++){
            my @group = @{$synonyms[$i]};
            if ($group[$#group]->{'pubyr'} > $oldest_pubyr) {
                $oldest_group = $i; 
                $oldest_pubyr = $group[$#group]->{'pubyr'};
            }
        }
        my $most_recent_group = splice(@synonyms,$oldest_group,1);
        push @synonyms,$most_recent_group;
    }
	
	# Loop through unique parent number from the opinions table.
	# Each parent number is a hash key whose value is an array ref of records.
    foreach my $group (@synonyms) {
        my $first_row = ${$group}[0];
        if ($first_row->{'status'} =~ /belongs/) {
            if ($first_row->{'spelling_reason'} eq 'rank change') {
                my $child = getTaxa($dbt,{'taxon_no'=>$first_row->{'child_no'}});
                my $spelling = getTaxa($dbt,{'taxon_no'=>$first_row->{'child_spelling_no'}});
                if ($child->{'taxon_rank'} =~ /genus/) {
		            $text .= "; it was reranked as ";
                } else {
                    $text .= "; it was reranked as the $spelling->{taxon_rank} ";
                }
            } else {
		        $text .= "; it was ".$synmap1{$first_row->{'spelling_reason'}};
            }
        } else {
		    $text .= "; it was ".$synmap2{$first_row->{'status'}};
        }
        if ($first_row->{'status'} !~ /nomen/) {
            my $taxon_no;
            if ($first_row->{'status'} =~ /subgroup|synonym|replaced|homonym|misspelled/) {
                $taxon_no = $first_row->{'parent_spelling_no'};
            } elsif ($first_row->{'status'} =~ /misspell/) {
                $taxon_no = $first_row->{'child_spelling_no'};
            } elsif ($first_row->{'spelling_reason'} =~ /correct|recomb|rank|reass/) {
                $taxon_no = $first_row->{'child_spelling_no'};
            }
            if ($taxon_no) {
                my $taxon = getTaxa($dbt,{'taxon_no'=>$taxon_no},['taxon_no','taxon_name','taxon_rank','author1last','author2last','otherauthors','pubyr']);
                if ($taxon->{'taxon_rank'} =~ /genus|species/) {
			        $text .= "<i>".$taxon->{'taxon_name'}."</i>";
                } else {
			        $text .= $taxon->{'taxon_name'};
                }
                if ($first_row->{'status'} eq 'homonym of') {
                    my $pub_info = Reference::formatShortRef($taxon);
                    $text .= ", $pub_info";
                }
            }
        }
        if ($first_row->{'status'} eq 'misspelling of') {
            $text .= " according to ";
        } else {
            $text .= " by ";
        }
        $text .= printReferenceList($group,$best_opinion);
	}
	if($text ne ""){
        if ($text !~ /\.\s*$/) {
            $text .= ".";
        }
        # Capitalize first it. 
		$text =~ s/;\s+it/It/;
	}

    my %parents = ();
    foreach my $row (@results) {
        if ($row->{'status'} =~ /belongs/) {
            if ($row->{'parent_spelling_no'}) { # Fix for bad opinions. See Asinus, Equus some of the horses
                push @{$parents{$row->{'parent_spelling_no'}}},$row;
            }
        }
    }
    $text =~ s/<br><br>\s*\.\s*$//i;
    my @parents_ordered = sort {$parents{$a}[-1]->{'pubyr'} <=> $parents{$b}[-1]->{'pubyr'} } keys %parents;
    if (@parents_ordered) {
        $text .= "<br><br>";
        #my $taxon_name = $taxon->{'taxon_name'};
        #if ($taxon->{'taxon_rank'} =~ /genus|species/) {
        #    $taxon_name = "<i>$taxon_name</i>";
        #}
        #$text .= "<a href=\"$READ_URL?action=checkTaxonInfo&amp;taxon_no=$taxon->{taxon_no}&amp;is_real_user=$is_real_user\">$taxon_name</a> was assigned ";
        $text .= "It was assigned";
        for(my $j=0;$j<@parents_ordered;$j++) {
            my $parent_no = $parents_ordered[$j];
            my $parent = getTaxa($dbt,{'taxon_no'=>$parent_no});
            my @parent_array = @{$parents{$parent_no}};
            $text .= " and " if ($j==$#parents_ordered && @parents_ordered > 1);
            my $parent_name = $parent->{'taxon_name'};
            if ($parent->{'taxon_rank'} =~ /genus|species/) {
                $parent_name = "<i>$parent_name</i>";
            }
            $text .= " to <a href=\"$READ_URL?action=checkTaxonInfo&amp;taxon_no=$parent->{taxon_no}&amp;is_real_user=$is_real_user\">$parent_name</a> by ";
            $text .= printReferenceList(\@parent_array,$best_opinion);
            $text .= "; ";
        }
        $text =~ s/; $/\./;
    }

    if ($taxon->{'first_occurrence'} && $IS_FOSSIL_RECORD) {
        $text .= "<br><br>";
        my $andlast = ($taxon->{'last_occurrence'} eq '') ? " and last" : "";
        $text .= "First$andlast occurrence: ".$taxon->{'first_occurrence'}."<br>";
        if ($taxon->{'last_occurrence'} ne '') {
            $text .= "Last occurrence: ".$taxon->{'last_occurrence'}."<br>";
        }
    }
    
    $text .= "</li>";
	return $text;

    # Only used in this function, just a simple utility to print out a formatted list of references
    sub printReferenceList {
        my @ref_array = @{$_[0]};
        my $best_opinion = $_[1];
        my $text = " ";
        foreach my $ref (@ref_array) {
            if ($ref->{'ref_has_opinion'} =~ /yes/i) {
                if ( $ref->{'opinion_no'} eq $best_opinion )	{
                    $text .= "<b>";
                }
                $text .= Reference::formatShortRef($ref,'alt_pubyr'=>1,'show_comments'=>1, 'link_id'=>1);
                if ( $ref->{'opinion_no'} eq $best_opinion )	{
                    $text .= "</b>";
                }
                $text .= ", ";
            } else {
                if ( $ref->{'opinion_no'} eq $best_opinion )	{
                    $text .= "<b>";
                }
                $text .= Reference::formatShortRef($ref,'alt_pubyr'=>1,'show_comments'=>1);
                if ( $ref->{'opinion_no'} eq $best_opinion )	{
                    $text .= "</b>";
                }
                $text .= ", ";
            }
        }
        $text =~ s/, $//;
        my $last_comma = rindex($text,",");
        if ($last_comma >= 0) {
            substr($text,$last_comma,1," and ");
        }
        
        return $text;
    }

}


sub getOriginalCombination{
	my $dbt = shift;
	my $taxon_no = shift;
    my $restrict_to_ref = shift;

	my $sql = "SELECT DISTINCT o.child_no FROM opinions o WHERE o.child_spelling_no=$taxon_no";
    if ($restrict_to_ref) {
        $sql .= " AND o.reference_no=".$restrict_to_ref;
    }
	my @results = @{$dbt->getData($sql)};

    if (@results == 0) {
        $sql = "SELECT DISTINCT o.child_no FROM opinions o WHERE o.parent_spelling_no=$taxon_no AND o.status='misspelling of'";
        if ($restrict_to_ref) {
            $sql .= " AND o.reference_no=".$restrict_to_ref;
        }
	    @results = @{$dbt->getData($sql)};
        if (@results == 0) {
            return $taxon_no;
        } else {
            return $results[0]->{'child_no'};
        }
    } elsif (@results == 1) {
        return $results[0]->{'child_no'};
    } else {
        # Weird case causes by bad data: two original combinations numbers.  In that case use
        # the combination with the oldest record.  The other "original" name is probably a misspelling or such
        # and falls by he wayside
        my $sql = "(SELECT o.child_no, o.opinion_no,"
                . " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000', o.pubyr, r.pubyr) as pubyr"
                . " FROM opinions o"
                . " LEFT JOIN refs r ON r.reference_no=o.reference_no"
                . " WHERE o.child_no IN (".join(",",map {$_->{'child_no'}} @results).")"
                . ") ORDER BY pubyr ASC, opinion_no ASC LIMIT 1"; 
	    @results = @{$dbt->getData($sql)};
        return $results[0]->{'child_no'};
    }
}

# See _getMostRecentParenetOpinion
sub getMostRecentClassification {
    my $dbt = shift;
    my $child_no = int(shift);
    my $options = shift || {};

    return if (!$child_no);
    return if ($options->{reference_no} eq '0');

    # This will return the most recent parent opinions. its a bit tricky cause: 
    # we're sorting by aliased fields. So surround the query in parens () to do this:
    # All values of the enum classification_quality get recast as integers for easy sorting
    # Lowest should appear at top of list (authoritative) and highest at bottom (compendium) so sort DESC
    # and want to use opinions pubyr if it exists, else ref pubyr as second choice - PS
    my $reliability = 
        "(IF (o.classification_quality != '',".
            "CASE o.classification_quality WHEN 'second hand' THEN 1 WHEN 'standard' THEN 2 WHEN 'implied' THEN 2 WHEN 'authoritative' THEN 3 END,".
            #"CASE o.classification_quality WHEN 'second hand' THEN 1 WHEN 'standard' THEN 2 WHEN 'implied' THEN 2 WHEN 'authoritative' THEN 3 ELSE 0 END,".
        # ELSE:
            "IF(r.reference_no = 6930,".
                "0,".# is compendium, then 0 (lowest priority)
            # ELSE:
                " CASE r.classification_quality WHEN 'compendium' THEN 1 WHEN 'standard' THEN 2 WHEN 'authoritative' THEN 3 END".
                #" CASE r.classification_quality WHEN 'compendium' THEN 1 WHEN 'standard' THEN 2 WHEN 'authoritative' THEN 3 ELSE 0 END".
            ")".
         ")) AS reliability_index ";
    my $fossil_record_sort;
    my $fossil_record_field;
    if ($IS_FOSSIL_RECORD) {
        $fossil_record_field = "FIND_IN_SET('fossil record',r.project_name) is_fossil_record, ";
        $fossil_record_sort = "is_fossil_record DESC, ";
    }
    my $strat_fields;
    if ($options->{strat_range}) {
        $strat_fields = 'max_interval_no,min_interval_no,';
    }
    my $sql = "(SELECT o.status,o.spelling_reason, o.figures,o.pages, o.parent_no, o.parent_spelling_no, o.child_no, o.child_spelling_no,o.opinion_no, o.reference_no, o.ref_has_opinion, o.phylogenetic_status, ".
            " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000', o.pubyr, r.pubyr) as pubyr, "
            . " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000', o.author1last, r.author1last) as author1last, "
            . " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000', o.author2last, r.author2last) as author2last, "
            . " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000', o.otherauthors, r.otherauthors) as otherauthors, "
            . $fossil_record_field
            . $strat_fields
            . $reliability
            . " FROM opinions o" 
            . " LEFT JOIN refs r ON r.reference_no=o.reference_no" 
            . " WHERE o.child_no=$child_no"
            . " AND o.child_no != o.parent_no AND o.status NOT IN ('misspelling of','homonym of')";
    if ($options->{reference_no}) {
        $sql .= " AND o.reference_no=$options->{reference_no}";
    }
    if ($options->{exclude_nomen}) {
        $sql .= " AND o.status NOT LIKE '%nomen%'";
    }
    if ($options->{strat_range}) {
        $sql .= " AND o.max_interval_no IS NOT NULL and o.max_interval_no != 0";
    }
    $sql .= ") ORDER BY $fossil_record_sort reliability_index DESC, pubyr DESC, opinion_no DESC";
    if ( ! wantarray ) {
        $sql .= " LIMIT 1";
    }
#   print $sql;

    my @rows = @{$dbt->getData($sql)};
    if (scalar(@rows)) {
        if ( wantarray ) {
            return @rows;
        } else	{
            return $rows[0];
        }
    } else {
        return undef;
    }
}

sub getMostRecentSpelling {
    my $dbt = shift;
    my $child_no = int(shift);
    my $options = shift || {};
    return if (!$child_no);
    return if ($options->{reference_no} eq '0');
    my $dbh = $dbt->dbh;

    # Get a list of misspellings and exclude them - do this is a subselect in the future
    my $sql = "SELECT DISTINCT child_spelling_no FROM opinions WHERE child_no=$child_no AND status='misspelling of'";
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my @misspellings;
    while (my @row = $sth->fetchrow_array()) {
        push @misspellings, $row[0];
    }
    # This will return the most recent parent opinions. its a bit tricky cause: 
    # we're sorting by aliased fields. So surround the query in parens () to do this:
    # All values of the enum classification_quality get recast as integers for easy sorting
    # Lowest should appear at top of list (authoritative) and highest at bottom (compendium) so sort ASC
    # and want to use opinions pubyr if it exists, else ref pubyr as second choice - PS
    my $reliability = 
        "(IF (o.classification_quality != '',".
            "CASE o.classification_quality WHEN 'second hand' THEN 1 WHEN 'standard' THEN 2 WHEN 'implied' THEN 2 WHEN 'authoritative' THEN 3 END,".
            #"CASE o.classification_quality WHEN 'second hand' THEN 1 WHEN 'standard' THEN 2 WHEN 'implied' THEN 2 WHEN 'authoritative' THEN 3 ELSE 0 END,".
        # ELSE:
            "IF(r.reference_no = 6930,".
                "0,".# is compendium, then 0 (lowest priority)
            # ELSE:
                " CASE r.classification_quality WHEN 'compendium' THEN 1 WHEN 'standard' THEN 2 WHEN 'authoritative' THEN 3 END".
                #" CASE r.classification_quality WHEN 'compendium' THEN 1 WHEN 'standard' THEN 2 WHEN 'authoritative' THEN 3 ELSE 0 END".
            ")".
         ")) AS reliability_index ";
    my $fossil_record_sort;
    my $fossil_record_field;
    if ($IS_FOSSIL_RECORD) {
        $fossil_record_field = "FIND_IN_SET('fossil record',r.project_name) is_fossil_record, ";
        $fossil_record_sort = "is_fossil_record DESC, ";
    }
    $sql = "(SELECT a2.taxon_name original_name, o.spelling_reason, a.taxon_no, a.taxon_name, a.common_name, a.taxon_rank, o.opinion_no, $reliability, $fossil_record_field"
         . " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000', o.pubyr, r.pubyr) AS pubyr"
         . " FROM opinions o" 
         . " LEFT JOIN authorities a ON o.child_spelling_no=a.taxon_no"
         . " LEFT JOIN authorities a2 ON o.child_no=a2.taxon_no"
         . " LEFT JOIN refs r ON r.reference_no=o.reference_no" 
         . " WHERE o.child_no=$child_no"
         . " AND o.child_no != o.parent_no AND o.status != 'misspelling of'";
    if ($options->{reference_no}) {
        $sql .= " AND o.reference_no=$options->{reference_no}";
    }
    if (@misspellings) {
        $sql .= " AND o.child_spelling_no NOT IN (".join(",",@misspellings).")";
    }
    $sql .= ") ";
    if (@misspellings) {
        $sql .= " UNION ";
        $sql .= "(SELECT a2.taxon_name original_name, o.spelling_reason, a.taxon_no, a.taxon_name, a.common_name, a.taxon_rank, o.opinion_no, $reliability, $fossil_record_field"
              . " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000', o.pubyr, r.pubyr) as pubyr"
              . " FROM opinions o" 
              . " LEFT JOIN authorities a ON o.parent_spelling_no=a.taxon_no"
              . " LEFT JOIN authorities a2 ON o.parent_no=a2.taxon_no"
              . " LEFT JOIN refs r ON r.reference_no=o.reference_no" 
              . " WHERE o.child_no=$child_no"
              . " AND o.status = 'misspelling of' AND o.child_no=o.parent_no";
        if ($options->{reference_no}) {
            $sql .= " AND o.reference_no=$options->{reference_no}";
        }
        $sql .= ") ";
    }
    $sql .= " ORDER BY $fossil_record_sort reliability_index DESC, pubyr DESC, opinion_no DESC LIMIT 1";
    my @rows = @{$dbt->getData($sql)};

    if (scalar(@rows)) {
        return $rows[0];
    } else {
        my $taxon = getTaxa($dbt,{'taxon_no'=>$child_no});
        $taxon->{'spelling_reason'} = "original spelling";
        return $taxon;
    }
}

sub isMisspelling {
    my ($dbt,$taxon_no) = @_;
    my $answer = 0;
    my $sql = "SELECT count(*) cnt FROM opinions WHERE child_spelling_no=$taxon_no AND status='misspelling of'";
    my $row = ${$dbt->getData($sql)}[0];
    return $row->{'cnt'};
}

# PS, used to be selectMostRecentParentOpinion, changed to this to simplify code 
# PS, changed from getMostRecentParentOpinion to _getMostRecentParentOpinion, to denote
# this is an interval function not to be called directly.  call getMostRecentClassification
# or getMostRecentSpelling instead, depending on whats wanted.  Because
# of lapsus calami (misspelling of) cases, these functions will differ occassionally, since a lapsus is a 
# special case that affects the spelling but doesn't affect the classification
# and consolidate bug fixes 04/20/2005

# JA 1.8.03
sub displayEcology	{
	my $dbt = shift;
	my $taxon_no = shift;

	unless ($taxon_no){
		return "<i>No ecological data are available</i>";
	}

    my $output = "";

	# get the field names from the ecotaph table
    my @ecotaphFields = $dbt->getTableColumns('ecotaph');
    # also get values for ancestors
    my $class_hash = TaxaCache::getParents($dbt,[$taxon_no],'array_full');
    my $eco_hash = Ecology::getEcology($dbt,$class_hash,\@ecotaphFields,'get_basis');
    my $ecotaphVals = $eco_hash->{$taxon_no};



	if ( ! $ecotaphVals )	{
		return "<i>No ecological data are available</i>";
	} else	{
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

        my @ranks = ('subspecies', 'species', 'subgenus', 'genus', 'subtribe', 'tribe', 'subfamily', 'family', 'superfamily', 'infraorder', 'suborder', 'order', 'superorder', 'infraclass', 'subclass', 'class', 'superclass', 'subphylum', 'phylum', 'superphylum', 'subkingdom', 'kingdom', 'superkingdom', 'unranked clade');
        my %rankToKey = ();
        foreach my $rank (@ranks) {
            my $rank_abbrev = $rank;
            $rank_abbrev =~ s/species/s/;
            $rank_abbrev =~ s/genus/g/;
            $rank_abbrev =~ s/tribe/t/;
            $rank_abbrev =~ s/family/f/;
            $rank_abbrev =~ s/order/o/;
            $rank_abbrev =~ s/class/c/;
            $rank_abbrev =~ s/phylum/p/;
            $rank_abbrev =~ s/kingdom/f/;
            $rank_abbrev =~ s/unranked clade/uc/;
            $rankToKey{$rank} = $rank_abbrev;
        }   
        my %all_ranks = ();

		$output .= "<table cellpadding=4 width=600>";
        $output .= "<tr><td colspan=2>";
        if (scalar(@references) == 1) {
            $output .= "<b>Reference:</b> ";
        } elsif (scalar(@references) > 1) {
            $output .= "<b>References:</b> ";
        }
        for(my $i=0;$i<scalar(@references);$i++) {
            my $sql = "SELECT reference_no,author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=$references[$i]";
            my $ref = ${$dbt->getData($sql)}[0];
            $references[$i] = Reference::formatShortRef($ref,'link_id'=>1);
        }
        $output .= join(", ",@references);
        $output .= "</td></tr>";
        $output .= "<tr>";
		my $cols = 0;
		foreach my $i (0..$#ecotaphFields)	{
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
                my $rank = $ecotaphVals->{$name."basis"};
                $all_ranks{$rank} = 1; 
				$v =~ s/,/, /g;
                if ( $cols == 2 || $name =~ /^comments$/ || $name =~ /^created$/ || $name =~ /^size_value$/ || $name =~ /1$/ )	{
                    $output .= "</tr>\n<tr>\n";
                    $cols = 0;
                }
				$cols++;
                my $colspan = ($name =~ /comments/) ? "colspan=2" : "";
                my $rank_note = "<span class=\"superscript\">$rankToKey{$rank}</span>";
                if ($name =~ /created|modified/) {
                    $rank_note = "";
                }
				$output .= "<td $colspan valign=\"top\"><table cellpadding=0 cellspacing=0 border=0><tr><td align=\"left\" valign=\"top\"><b>$n:</b>&nbsp;</td><td valign=\"top\">${v}${rank_note}</td></tr></table></td> \n";
			}
		}
        $output .= "</tr>" if ( $cols > 0 );
        # now print out keys for superscripts above
        $output .= "<tr><td colspan=2>";
        my $html = "<b>Source:</b> ";
        foreach my $rank (@ranks) {
            if ($all_ranks{$rank}) {
                $html .= "$rankToKey{$rank} = $rank, ";
            }
        }
        $html =~ s/, $//;
        $output .= $html;
        $output .= "</td></tr>"; 
		$output .= "</table>\n";
	}

	return $output;

}

# PS 6/27/2005
sub displayMeasurements {
    my ($dbt,$taxon_no,$taxon_name,$in_list) = @_;

    # Specimen level data:
    my @specimens;
    my $specimen_count;
    if ($taxon_no) {
        my $t = getTaxa($dbt,{'taxon_no'=>$taxon_no});
        if ($t->{'taxon_rank'} =~ /genus|species/) {
            # If the rank is genus or lower we want the big aggregate list of all taxa
            @specimens = Measurement::getMeasurements($dbt,'taxon_list'=>$in_list,'get_global_specimens'=>1);
        } else {
            # If the rank is higher than genus, then that rank is too big to be meaningful.  
            # In that case we only want the taxon itself (and its synonyms and alternate names), not the big recursively generated list
            # i.e. If they entered Nasellaria, get Nasellaria indet., or Nasellaria sp. or whatever.
            # get alternate spellings of focal taxon. 
            my @small_in_list = TaxonInfo::getAllSynonyms($dbt,$taxon_no);
            dbg("Passing small_in_list to getMeasurements".Dumper(\@small_in_list));
            @specimens = Measurement::getMeasurements($dbt,'taxon_list'=>\@small_in_list,'get_global_specimens'=>1);
        }
    } else {
        @specimens = Measurement::getMeasurements($dbt,'taxon_name'=>$taxon_name,'get_global_specimens'=>1);
    }

    # Returns a triple index hash with index <part><dimension type><whats measured>
    #  Where part can be leg, valve, etc, dimension type can be length,width,height,diagonal,inflation 
    #   and whats measured can be average, min,max,median,error
    my $p_table = Measurement::getMeasurementTable(\@specimens);

    my $str = "";
    if (@specimens) {
        my $temp;
        while (my($part,$m_table)=each %$p_table) {
            $temp++;
            my $part_str = ($part) ? "<p><b>Part: </b>$part</p>" : "";
            if ( $temp > 1 )	{
              $part_str = "<hr>\n" . $part_str;
            }
            $str .= "<table><tr><td colspan=6 style=\"padding-bottom: .75em;\">$part_str<b>Specimens measured:</b> $m_table->{specimens_measured}</td></tr>".
                    "<tr><th></th><th>mean</th><th>minimum</th><th>maximum</th><th>median</th><th>error</th><th></th></tr>";

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
        $str .= "<i>No measurement data are available</i>";
    }

    return $str;
}

sub displayDiagnoses {
    my ($dbt,$taxon_no) = @_;
    my $str = "";
    if ($taxon_no) {
        my @diagnoses = getDiagnoses($dbt,$taxon_no);

        if (@diagnoses) {
            $str .= "<table cellspacing=5>\n";
            $str .= "<tr><td><b>Reference</b></td><td><b>Diagnosis</b></td></tr>\n";
            foreach my $row (@diagnoses) {
                $str .= "<tr><td valign=top><span style=\"white-space: nowrap\">$row->{reference}</span>";
                if ($row->{'is_synonym'}) {
                    if ($row->{'taxon_rank'} =~ /species|genus/) {
                        $str .= " (<i>$row->{taxon_name}</i>)";
                    } else {
                        $str .= " ($row->{taxon_name})";
                    }
                } 
                $row->{diagnosis} =~ s/\n/<br>/g;
                $str .= "</td><td>$row->{diagnosis}<td></tr>";
            }
            $str .= "</table>\n";
        } 
    } 
    if (!$str) {
        $str .= "<div align=\"center\"><i>No diagnosis data are available</i></div>";
    }
    return $str;
}


# JA 11-12,14.9.03
sub displaySynonymyList	{
	my $dbt = shift;
    # taxon_no must be an original combination
	my $taxon_no = (shift or "");
    my $is_real_user = shift;
	my $output = "";

    unless ($taxon_no) {
        return "<i>No synonymy data are available</i>";
    }

    # Find synonyms
    my @syns = getJuniorSynonyms($dbt,$taxon_no);

    # Push the focal taxon onto the list as well
    push @syns, $taxon_no;

    # go through list finding all "recombined as" something else cases for each
    # need to do this because synonyms might have their own recombinations, and
    #  the original combination might have alternative combinations
    # don't do this if the parent is actually the focal taxon
	my @synparents;
	foreach my $syn (@syns)	{
		my $sql = "SELECT child_spelling_no FROM opinions WHERE child_no=" . $syn . " AND child_spelling_no != " . $taxon_no;
		my @synparentrefs = @{$dbt->getData($sql)};
		foreach my $synparentref ( @synparentrefs )	{
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
    my %synline = ();
	foreach my $syn (@syns)	{
		my $sql = "(SELECT author1last,author2last,otherauthors,pubyr,pages,figures,ref_has_opinion,reference_no FROM opinions WHERE status IN ('subjective synonym of','objective synonym of','replaced by','invalid subgroup of','misspelling of') AND parent_spelling_no=$syn)";
        $sql .= " UNION ";
		$sql .= "(SELECT author1last,author2last,otherauthors,pubyr,pages,figures,ref_has_opinion,reference_no FROM opinions WHERE status IN ('subjective synonym of','objective synonym of','replaced by','invalid subgroup of','misspelling of') AND parent_no=$syn)";
        $sql .= " UNION ";
		$sql .= "(SELECT author1last,author2last,otherauthors,pubyr,pages,figures,ref_has_opinion,reference_no FROM opinions WHERE child_spelling_no=$syn AND status IN ('belongs to','recombined as','rank changed as','corrected as'))";
		my @userefs =  @{$dbt->getData($sql)};

        my $parent = getTaxa($dbt,{'taxon_no'=>$syn});

		my $parent_name = $parent->{'taxon_name'};
		my $parent_rank = $parent->{'taxon_rank'};
		if ( $parent_rank =~ /genus|species/ )	{
			$parent_name = "<i>" . $parent_name . "</i>";
		} 
		foreach my $useref ( @userefs )	{
            my $synkey = "";
            my $mypubyr = "";
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
				my $sql = "SELECT author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=" . $useref->{reference_no};
				my $refref = @{$dbt->getData($sql)}[0];
				$synkey = "<td>" . $refref->{pubyr} . "</td><td>" . $parent_name . "<a href=\"$READ_URL?action=displayReference&amp;reference_no=$useref->{reference_no}&amp;is_real_user=$is_real_user\"> " . $refref->{author1last};
				if ( $refref->{otherauthors} )	{
					$synkey .= " et al.";
				} elsif ( $refref->{author2last} )	{
					$synkey .= " and " . $refref->{author2last};
				}
                $synkey .= "</a>";
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
    my %isoriginal;
	foreach my $syn (@syns)	{
		my $sql = "SELECT count(*) AS c FROM opinions WHERE child_spelling_no=$syn AND child_no != child_spelling_no";
		my $timesrecombined = ${$dbt->getData($sql)}[0]->{'c'};
		if ( ! $timesrecombined )	{
			$isoriginal{$syn} = "YES";
		}
	}

# likewise appearances in the authority table
	foreach my $syn (@syns)	{
        if ( $isoriginal{$syn} eq "YES" )	{
            my $sql = "SELECT taxon_name,taxon_rank,author1last,author2last,otherauthors,pubyr,pages,figures,ref_is_authority,reference_no FROM authorities WHERE taxon_no=" . $syn;
            my @userefs = @{$dbt->getData($sql)};
        # save the instance as a key with pubyr as a value
        # note that @userefs only should have one value because taxon_no
        #  is the primary key
            foreach my $useref ( @userefs )	{
                my $auth_taxon_name = $useref->{taxon_name};
                my $auth_taxon_rank = $useref->{taxon_rank};
                if ( $auth_taxon_rank =~ /genus|species/ )	{
                    $auth_taxon_name = "<i>" . $auth_taxon_name . "</i>";
                }
                my $synkey = "";
                my $mypubyr;
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
                    my $sql = "SELECT author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=" . $useref->{reference_no};
                    my $refref = @{$dbt->getData($sql)}[0];
                    $synkey = "<td>" . $refref->{pubyr} . "</td><td>" . $auth_taxon_name . "<a href=\"$READ_URL?action=displayReference&amp;reference_no=$useref->{reference_no}&amp;is_real_user=$is_real_user\"> " . $refref->{author1last};
                    if ( $refref->{otherauthors} )	{
                        $synkey .= " et al.";
                    } elsif ( $refref->{author2last} )	{
                        $synkey .= " and " . $refref->{author2last};
                    }
                    $synkey .= "</a>";
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
	my @synlinekeys = sort { $synline{$a} <=> $synline{$b} } keys %synline;

# print each line of the synonymy list
	$output .= "<table cellspacing=5>\n";
	$output .= "<tr><td><b>Year</b></td><td><b>Name and author</b></td></tr>\n";
	foreach my $synline ( @synlinekeys )	{
		$output .= "<tr>$synline</td></tr>\n";
	}
	$output .= "</table>\n";

    return $output;
}

# Small utility function, added 01/06/2005
# Lump_ranks will cause taxa with the same name but diff rank (same taxa) to only pass
# back one taxon_no (it doesn't really matter which)
sub getTaxonNos {
    my $dbt = shift;
    my $name = shift;
    my $rank = shift;
    my $lump_ranks = shift;
    my @taxon_nos = ();
    if ($dbt && $name)  {
        my $sql = "SELECT a.taxon_no taxon_no FROM authorities a,taxa_tree_cache t WHERE a.taxon_no=t.taxon_no AND taxon_name=".$dbt->dbh->quote($name);
        if ($rank) {
            $sql .= " AND taxon_rank=".$dbt->dbh->quote($rank);
        }
        if ($lump_ranks) {
            $sql .= " GROUP BY lft,rgt";
        }
        my @results = @{$dbt->getData($sql)};
        push @taxon_nos, $_->{'taxon_no'} for @results;
    }
                                                                                                                                                         
    return @taxon_nos;
}

# Now a large centralized function, PS 5/3/2006
# @taxa_rows = getTaxa($dbt,\%options,\@fields)
# Pass it a $dbt object first, a hashref of options, an arrayref of fields. See examples.
# arrayref of fields is optional, default fields returned at taxon_no,taxon_name,taxon_rank
# arrayref of fields can all be values ['*'] and ['all'] to get all fields. Note that is fields
# requested are any of the pubyr or author fields (author1init, etc) then this function will
# do a join with the references table automatically and pull the data from the ref if it is the
# authority for you. So no need to hit the refs table separately afterwords; 
#
# Returns a array of hashrefs, like getData
#
# valid options: 
#  reference_no - Get taxa with reference_no as their reference
#  taxon_no - Get taxon with taxon-no
#  taxon_name - Get all taxa with taxon_name
#  taxon_rank - Restrict search to certain ranks
#  authorizer_no - restrict to authorizeer
#  match_subgenera - the taxon_name can either match the genus or subgenus.  Note taht
#    this is very slow since it has to do a full table scan
#  pubyr - Match the pubyr
#  authorlast - Match against author1last, author2last, and otherauthors
#  created - get records created before or after a date
#  created_before_after: whether to get records created before or after the created date.  Valid values
#  are 'before' and 'after'.  Default is 'after'
#
# Example usage: 
#   Example 1: get all taxa attached to a reference. fields returned are taxon_no,taxon_name,taxon_rank
#     @results = TaxonInfo::getTaxa($dbt,{'reference_no'=>345}); 
#     my $first_taxon_name = $results[0]->{taxon_name}
#   Example 2: get all records named chelonia, and transparently include pub info (author1last, pubyr, etc) directly in the records
#     even if that pub. info is stored in the reference and ref_is_authority=YES
#     @results = TaxonInfo::getTaxa($dbt,{'taxon_name'=>'Chelonia'},['taxon_name','taxon_rank','author1last','author2last','pubyr');  
#   Example 3: get all records where the genus or subgenus is Clymene, get all fields
#     my %options;
#     $options{taxon_name}='Clymene';
#     $options{match_subgenera}=1;
#     @results = getTaxa($dbt,\%options,['*']);
#   Example 4: get record where taxon_no is 555.  Note that we don't pass back an array, he get the (first) hash record directly
#     $taxon = getTaxa($dbt,'taxon_no'=>555);

sub getTaxa {
    my $dbt = shift;
    my $options = shift;
    my $fields = shift;
    my $dbh = $dbt->dbh;

    my $join_refs = 0;
    my @where = ();
    if ($options->{'taxon_no'}) {
        push @where, "a.taxon_no=".int($options->{'taxon_no'});
    } else {
        if ($options->{'common_name'}) {
            push @where, "common_name=".$dbh->quote($options->{'common_name'});
        }
        if ($options->{'taxon_rank'}) {
            push @where, "taxon_rank=".$dbh->quote($options->{'taxon_rank'});
        }
        if ($options->{'reference_no'}) {
            push @where, "a.reference_no=".int($options->{'reference_no'});
        }
        if ($options->{'authorizer_no'}) {
            push @where, "a.authorizer_no=".int($options->{'authorizer_no'});
        }
        if ($options->{'created'}) {
            my $sign = ($options->{'created_before_after'} eq 'before') ? '<=' : '>=';
            push @where, "a.created $sign ".$dbh->quote($options->{'created'});
        }
        if ($options->{'pubyr'}) {
            my $pubyr = $dbh->quote($options->{'pubyr'});
            push @where,"((a.ref_is_authority NOT LIKE 'YES' AND a.pubyr LIKE $pubyr) OR (a.ref_is_authority LIKE 'YES' AND r.pubyr LIKE $pubyr))";
            $join_refs = 1;
        }
        if ($options->{'author'}) {
            my $author = $dbh->quote($options->{'author'});
            my $authorWild = $dbh->quote('%'.$options->{'author'}.'%');
            push @where,"((a.ref_is_authority NOT LIKE 'YES' AND (a.author1last LIKE $author OR a.author2last LIKE $author OR a.otherauthors LIKE $authorWild)) OR".
                        "(a.ref_is_authority LIKE 'YES' AND (r.author1last LIKE $author OR r.author2last LIKE $author OR r.otherauthors LIKE $authorWild)))";
            $join_refs = 1;
        }
    }

#    all_fields = (authorizer_no,enterer_no,modifier_no,taxon_no,reference_no,taxon_rank,taxon_name,type_taxon_no,type_specimen,extant,preservation,ref_is_authority,author1init,author1last,author2init,author2last,otherauthors,pubyr,pages,figures,comments,created,modified);
    my @fields;
    if ($fields) {
        @fields = @$fields;
        if  ($fields[0] =~ /\*|all/) {
            @fields = ('taxon_no','reference_no','taxon_rank','taxon_name','common_name','type_taxon_no','type_specimen','type_body_part','part_details','extant','preservation','ref_is_authority','author1init','author1last','author2init','author2last','otherauthors','pubyr','pages','figures','comments');
        }
        foreach my $f (@fields) {
            if ($f =~ /^author(1|2)(last|init)$|otherauthors|pubyr$/) {
                $f = "IF (a.ref_is_authority LIKE 'YES',r.$f,a.$f) $f";
                $join_refs = 1;
            } else {
                $f = "a.$f";
            }
        }
    } else {
        @fields = ('a.taxon_no','a.taxon_name','a.common_name','a.taxon_rank');
    }
    my $base_sql = "SELECT ".join(",",@fields)." FROM authorities a";
    if ($join_refs) {
        $base_sql .= " LEFT JOIN refs r ON a.reference_no=r.reference_no";
    }

    my @results = ();
    if ($options->{'match_subgenera'} && $options->{'taxon_name'}) {
        my ($genus,$subgenus,$species,$subspecies) = Taxon::splitTaxon($options->{'taxon_name'});
        my $species_sql = "";
        if ($species =~ /[a-z]/) {
            $species_sql .= " $species";
        }
        if ($subspecies =~ /[a-z]/) {
            $species_sql .= " $subspecies";
        }
        my $taxon1_sql = "taxon_name LIKE '$options->{taxon_name}'";
        
        my $sql = "($base_sql WHERE ".join(" AND ",@where,$taxon1_sql).")";
        if ($subgenus) {
            # Only exact matches for now, may have to rethink this
            my $taxon3_sql = "taxon_name LIKE '$subgenus$species_sql'";
            #my $taxon4_sql = "taxon_name LIKE '% ($subgenus)$species_sql'";
            $sql .= " UNION ";
            $sql .= "($base_sql WHERE ".join(" AND ",@where,$taxon3_sql).")";
            #$sql .= "($base_sql WHERE ".join(" AND ",@where,$taxon4_sql).")";
        } else {
            $sql .= " UNION ";
            my $taxon2_sql = "taxon_name LIKE '% ($genus)$species_sql'";
            $sql .= "($base_sql WHERE ".join(" AND ",@where,$taxon2_sql).")";
        }
#        print $sql,"\n";
        @results = @{$dbt->getData($sql)};
    } else {
        if ($options->{'taxon_name'}) {
            push @where,"a.taxon_name LIKE ".$dbh->quote($options->{'taxon_name'});
        }
        if (@where) {
            my $sql = $base_sql." WHERE ".join(" AND ",@where); 
            $sql .= " ORDER BY taxon_name" if ($options->{'reference_no'});
            #print $sql,"\n";
            @results = @{$dbt->getData($sql)};
        }
    }

    if ($options->{'remove_rank_change'}) {
        if (@results > 1) {
            my %seen_orig = ();
            my %is_orig = ();
            foreach my $row (@results) {
                my $orig = getOriginalCombination($dbt,$row->{'taxon_no'});
                $seen_orig{$orig} = $row;
                if ($orig == $row->{'taxon_no'}) {
                    $is_orig{$orig} = $row;
                }
            }
            if (scalar keys %seen_orig == 1) {
                if (%is_orig) {
                    @results = values %is_orig;
                } else {
                    @results = values %seen_orig;
                }
            }
        }
    }

    if (wantarray) {
        return @results;
    } else {
        return $results[0];
    }
}

# Keep going until we hit a belongs to, recombined, corrected as, or nome *
# relationship. Note that invalid subgroup is technically not a synoym, but treated computationally the same
sub getSeniorSynonym {
    my $dbt = shift;
    my $taxon_no = shift;
    my $restrict_to_reference_no = shift;

    my %seen = ();
    # Limit this to 10 iterations, in case we a have some weird loop
    my $options = {};
    if ($restrict_to_reference_no =~ /\d/) {
        $options->{'reference_no'} = $restrict_to_reference_no;
    }
    for(my $i=0;$i<10;$i++) {
        my $parent = getMostRecentClassification($dbt,$taxon_no,$options);
        last if (!$parent || !$parent->{'child_no'});
        if ($seen{$parent->{'child_no'}}) {
            # If we have a loop, disambiguate using last entered
            my @rows = sort {$b->{'opinion_no'} <=> $a->{'opinion_no'}} values %seen;
            #my @rows = sort {$b->{'reliability_index'} <=> $a->{'reliability_index'} || 
            #                 $b->{'pubyr'} <=> $a->{'pubyr'} || 
            #                 $b->{'opinion_no'} <=> $a->{'opinion_no'}} values %seen;
            $taxon_no = $rows[0]->{'parent_no'};
            last;
        } else {
            $seen{$parent->{'child_no'}} = $parent;
            if ($parent->{'status'} =~ /synonym|replaced|subgroup/) {
                $taxon_no = $parent->{'parent_no'};
            } else {
                last;
            }
        } 
    }

    return $taxon_no;
}

# They may potentialy be chained, so keep going till we're done. Use a queue isntead of recursion to simplify things slightly
# and original combination must be passed in. Use a hash to keep track to avoid duplicate and recursion
# Note that invalid subgroup is technically not a synoym, but treated computationally the same
sub getJuniorSynonyms {
    my $dbt = shift;
    my @taxon_nos = @_;

    my @queue = ();
    push @queue, $_ for (@taxon_nos);
    my %seen_syn = ();
    for(my $i = 0;$i<50;$i++) {
        my $taxon_no;
        if (@queue) {
            $taxon_no = pop @queue;
        } else {
            last;
        }
        my $sql = "SELECT DISTINCT child_no FROM opinions WHERE parent_no=$taxon_no AND child_no != parent_no";
        my @results = @{$dbt->getData($sql)};
        foreach my $row (@results) {
            my $parent = getMostRecentClassification($dbt,$row->{'child_no'});
            if ($parent->{'parent_no'} == $taxon_no && $parent->{'status'} =~ /synonym|replaced|subgroup/) {
                if (!$seen_syn{$row->{'child_no'}}) {
                    push @queue, $row->{'child_no'};
                }
                $seen_syn{$row->{'child_no'}} = 1;
            }
        }
    }
    return (keys %seen_syn);
}


# Get all recombinations and corrections a taxon_no could be, but not junior synonyms
# Assume that the taxon_no passed in is already an original combination
sub getAllSpellings {
    my $dbt = shift;
    my @taxon_nos = @_;
    my %all;
    for (@taxon_nos) {
        $all{int($_)} = 1 if int($_);
    }

    if (%all) {
        my $sql = "SELECT DISTINCT child_spelling_no FROM opinions WHERE child_no IN (".join(",",keys %all).")";
        my @results = @{$dbt->getData($sql)};
        $all{$_->{'child_spelling_no'}} = 1 for @results;

        $sql = "SELECT DISTINCT child_no FROM opinions WHERE child_spelling_no IN (".join(",",keys %all).")";
        @results = @{$dbt->getData($sql)};
        $all{$_->{'child_no'}} = 1 for @results;

        # Bug fix: bad records with multiple original combinations
        $sql = "SELECT DISTINCT child_spelling_no FROM opinions WHERE child_no IN (".join(",",keys(%all)).")";
        @results = @{$dbt->getData($sql)};
        $all{$_->{'child_spelling_no'}} = 1 for @results;

        $sql = "SELECT DISTINCT parent_spelling_no FROM opinions WHERE status='misspelling of' AND child_no IN (".join(",",keys %all).")";
        @results = @{$dbt->getData($sql)};
        $all{$_->{'parent_spelling_no'}} = 1 for @results;
    }
    delete $all{''};
    delete $all{'0'};
    return keys %all;
}

# Get all synonyms/recombinations and corrections a taxon_no could be
# Assume that the taxon_no passed in is already an original combination
sub getAllSynonyms {
    my $dbt = shift;
    my $taxon_no = shift;
    if ($taxon_no) {
        $taxon_no = getSeniorSynonym($dbt,$taxon_no); 
        my @js = getJuniorSynonyms($dbt,$taxon_no); 
        return getAllSpellings($dbt,@js,$taxon_no);
    } else {
        return ();
    }
}

# This will return all diagnoses for a particular taxon, for all its spellings, and
# for all its junior synonyms. The diagnoses are passed back as a sorted array of hashrefs ordered by
# pubyr of the opinion.  Each hashref has the following keys:
#  taxon_no: spelling_no for the opinion for which the diagnosis exists
#  reference: formated reference for the diagnosis
#  diagnosis: text of the diagnosis field
#  is_synonym: boolean denoting whether this is a 
#  taxon_name: spelling_name for the opinion fo rwhich the diagnosis exists
# Example usage:
#   $taxon = getTaxa($dbt,{'taxon_name'=>'Calippus'});
#   @diagnoses = getDiagnoses($dbt,$taxon->{taxon_no});
#   foreach $d (@diagnoses) {
#       print "$d->{reference}: $d->{diagnosis}";
#   }
sub getDiagnoses {
    my $dbt = shift;
    my $taxon_no = shift;
    $taxon_no = int($taxon_no);

    my @diagnoses = ();
    my %is_synonym = ();
    if ($taxon_no) {
        # Tricky part is the is_synonym, which will be set to a boolean if the taxon_no passed back is a 
        # synonym (either junior or senior, doesn't make that distiction) or not.  The spelling_no is the
        # most recently uses spelling for the current taxon, so this will be a constant for all the different
        # spellings of the current synonym, and different for all its synonyms
        my $sql = "SELECT t2.taxon_no,IF(t2.spelling_no = t1.spelling_no,0,1) is_synonym FROM taxa_tree_cache t1, taxa_tree_cache t2 WHERE t1.taxon_no=$taxon_no and t1.synonym_no=t2.synonym_no";
        my @results = @{$dbt->getData($sql)};
        my @children;
        foreach my $row (@results) {
            push @children, $row->{'taxon_no'};
            $is_synonym{$row->{'taxon_no'}} = $row->{'is_synonym'};
        }
        if (@children) {
            # Uses the taxa_tree_cache to get opinions for all various spellings, including synonyms
            $sql = "SELECT o.opinion_no,o.child_no, o.child_spelling_no, a.taxon_name, a.taxon_rank, o.diagnosis, o.ref_has_opinion,o.author1init,o.author1last,o.author2init,o.author2last,o.otherauthors,o.pubyr,o.reference_no FROM opinions o, authorities a WHERE o.child_spelling_no=a.taxon_no AND o.child_no IN (".join(",",@children).") AND o.diagnosis IS NOT NULL AND o.diagnosis != ''";
            my @results = @{$dbt->getData($sql)};
            foreach my $row (@results) {
                my $reference = "";
                my $pubyr = "";
                if ($row->{'ref_has_opinion'}) {
                    if ($row->{'reference_no'}) {
                        $sql = "SELECT author1init,author1last,author2init,author2last,otherauthors,pubyr,reference_no FROM refs WHERE reference_no=$row->{reference_no}";
                        my $refData = ${$dbt->getData($sql)}[0];
                        $reference = Reference::formatShortRef($refData,'link_id'=>1);
                        $pubyr = $refData->{'pubyr'};
                    }
                } else {
                    $reference = Reference::formatShortRef($row);
                    $pubyr = $row->{'pubyr'};
                }
                my %diagnosis = (
                    'taxon_no'  =>$row->{'child_spelling_no'},
                    'taxon_name'=>$row->{'taxon_name'},
                    'taxon_rank'=>$row->{'taxon_rank'},
                    'reference' =>$reference,
                    'pubyr'     =>$pubyr,
                    'opinion_no'=>$row->{'opinion_no'},
                    'diagnosis' =>$row->{'diagnosis'},
                    'is_synonym'=>$is_synonym{$row->{'child_no'}}
                );
                push @diagnoses, \%diagnosis;
            }
        }
    }
    @diagnoses = sort {if ($a->{'pubyr'} && $b->{'pubyr'}) {$a->{'pubyr'} <=> $b->{'pubyr'}}
                       else {$a->{'opinion_no'} <=> $b->{'opinion_no'}}} @diagnoses;
    return @diagnoses;
}


# Returns (higher) order taxonomic names that are no longer considered valid (disused)
# These higher order names most be the most recent spelling of the most senior
# synonym, since thats what the taxa_list_cache stores.  Taxonomic names
# that don't fall into this category aren't even valid in the first place
# so there is no point in passing them in.
# This is figured out algorithmically.  If a higher order name used to have
# children assinged into it but now no longer does, then its considered "disused"
# You may pass in a scalar (taxon_no) or a reference to an array of scalars (array of taxon_nos)
# as the sole argument and the program will figure out what you're doing
# Returns a hash reference where they keys are equal all the taxon_nos that 
# it considered no longer valid
sub disusedNames {
    my $dbt = shift;
    my $arg = shift;
    my @taxon_nos = ();
    if (UNIVERSAL::isa($arg,'ARRAY')) {
        @taxon_nos = @$arg;
    } else {
        @taxon_nos = ($arg);
    }


    my %disused = ();
    if (@taxon_nos) {
        my %has_children = ();
        my %taxon_nos = ();
        my %had_children = ();
        my %map_orig = ();
        my ($sql,@results);


        my $taxon_nos_sql = join(",",map{int($_)} @taxon_nos);

        # Since children will be linked to the original combination taxon no, get those and append them to the list
        # The map_orig array refers the original combinations 
        $sql = "SELECT DISTINCT child_no,child_spelling_no FROM opinions WHERE child_spelling_no IN ($taxon_nos_sql) AND child_no != child_spelling_no";
        @results = @{$dbt->getData($sql)};
        foreach my $row (@results) {
            $map_orig{$row->{'child_no'}} = $row->{'child_spelling_no'};
#            print "MAP SP. $row->{child_spelling_no} TO ORIG. $row->{child_no}<BR>";
#            push @taxon_nos, $row->{'child_no'};
            $taxon_nos_sql .= ",$row->{child_no}";
        }


        # Parents with any children will be put into the array. Junior synonyms not counted
        $sql = "SELECT parent_no FROM taxa_list_cache WHERE parent_no IN ($taxon_nos_sql) GROUP BY parent_no";
        @results = @{$dbt->getData($sql)};
        foreach my $row (@results) {
            $has_children{$row->{'parent_no'}} = 1;
#            print "$row->{parent_no} HAS children<BR>\n";
        }

        $sql = "SELECT parent_no FROM opinions WHERE status IN ('belongs to','recombined as','corrected as','rank changed as') AND parent_no IN ($taxon_nos_sql) GROUP BY parent_no";
        @results = @{$dbt->getData($sql)};
        foreach my $row (@results) {
            my $parent_no = $row->{'parent_no'};
            if ($map_orig{$parent_no}) {
                $parent_no = $map_orig{$parent_no};
            }
            $had_children{$parent_no} = 1;
#            print "$row->{parent_no} (spelled $parent_no) HAD children<BR>\n";
        }

#        $sql = "SELECT parent_spelling_no FROM opinions WHERE status IN ('belongs to','recombined as','corrected as','rank changed as') AND parent_spelling_no IN ($taxon_nos_sql) GROUP BY parent_spelling_no";
#        @results = @{$dbt->getData($sql)};
#        foreach my $row (@results) {
#            $had_children{$row->{'parent_spelling_no'}} = 1;
#            print "$row->{parent_spelling_no} HAD children<BR>\n";
#        }

        foreach my $taxon_no (@taxon_nos) {
            if ($had_children{$taxon_no} && !$has_children{$taxon_no}) {
                #if ($map_orig{$taxon_no}) {
                #    print "Map $taxon_no to $map_orig{$taxon_no}<BR>";
                #    $disused{$map_orig{$taxon_no}} = 1;
                #} else {
                $disused{$taxon_no} = 1;
                #}
            }
        }
    }
    return \%disused;
}

# This will get orphaned nomen * children for a list of a taxon_nos or a single taxon_no passed in.
# returns a hash reference where the keys are parent_nos and the values are arrays of child taxon objects
# The child taxon objects are just hashrefs where the hashes have the following keys:
# taxon_no,taxon_name,taxon_rank,status.  Status is nomen dubium etc, and rest of the fields are standard.
sub nomenChildren {
    my $dbt = shift;
    my $arg = shift;
    my @taxon_nos = ();
    if (UNIVERSAL::isa($arg,'ARRAY')) {
        @taxon_nos = @$arg;
    } else {
        @taxon_nos = ($arg);
    }

    my %nomen = ();
    if (@taxon_nos) {
        my $sql = "SELECT DISTINCT o2.child_no,o1.parent_no FROM opinions o1, opinions o2 WHERE o1.child_no=o2.child_no AND o2.status LIKE '%nomen%' AND o1.parent_no IN (".join(",",@taxon_nos).")";
        my @results = @{$dbt->getData($sql)};
        foreach my $row (@results) {
            my $mrpo = getMostRecentClassification($dbt,$row->{'child_no'});
            if ($mrpo->{'status'} =~ /nomen/) {
                #print "child $row->{child_no} IS NOMEN<BR>";
                # This will get the most recent parent opinion where it is not classified as a %nomen%
                my $mrpo_no_nomen = getMostRecentClassification($dbt,$row->{'child_no'},{'exclude_nomen'=>1});
                if ($mrpo_no_nomen->{'parent_no'} == $row->{'parent_no'}) {
                    #print "child $row->{child_no} LAST PARENT IS PARENT $row->{parent_no} <BR>";
                    my $taxon = getTaxa($dbt,{'taxon_no'=>$row->{'child_no'}});
                    $taxon->{'status'} = $mrpo->{'status'};
                    push @{$nomen{$mrpo_no_nomen->{'parent_no'}}}, $taxon;
                } else {
                    #print "child $row->{child_no} LAST PARENT IS NOT PARENT $row->{parent_no} BUT $mrpo_no_nomen->{parent_no}<BR>";
                }
            }
        }
    }
    return \%nomen;
}

sub rankOrder {
    my %rankToNum = ('subspecies' => 1, 'species' => 2, 'subgenus' => 3,
        'genus' => 4, 'subtribe' => 5, 'tribe' => 6,
        'subfamily' => 7, 'family' => 8, 'superfamily' => 9,
        'infraorder' => 10, 'suborder' => 11, 'order' => 12, 'superorder' => 13, 
        'infraclass' => 14, 'subclass' => 15, 'class' => 16, 'superclass' => 17,
        'subphylum' => 18, 'phylum' => 19, 'superphylum' => 20,
        'subkingdom' => 21, 'kingdom' => 22, 'superkingdom' => 23,
        'unranked clade' => 24, 'informal' => 25 ); 
    return %rankToNum;
}
                                                                                                                                                                         

1;
