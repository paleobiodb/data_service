#!/usr/bin/perl

package Confidence;

use DBTransactionManager;
use Debug;
use URI::Escape;
use Data::Dumper; 
use GD;

$IMAGE_DIR = $ENV{'BRIDGE_HTML_DIR'} . "/public/confidence";

# written 03.31.04 by Josh Madin as final product
# Still doesn't allow choice of time-scale when examining the conf ints of taxa

# Deals with homonyms that may exist by presenting an option to select either one
# and passing on the taxon_no.  All non-homonym taxa get passed as hidden fields
# PS 02/08/2004
sub displayHomonymForm {
    $q=shift;
    $s=shift;
    $dbt=shift;
    $dbh=$dbt->dbh;
    $homonym_names = shift;
    $splist=shift;

    %splist = %$splist;
    @homonym_names = @$homonym_names;

    my $pl1 = scalar(@homonym_names) > 1 ? "s" : "";
    my $pl2 = scalar(@homonym_names) > 1 ? "" : "s";
    print "<CENTER><H3>The following taxonomic name$pl1 belong$pl2 to multiple taxonomic <br>hierarchies.  Please choose the one$pl1 you want.</H3>";
    print "<FORM ACTION=\"bridge.pl\" METHOD=\"post\"><INPUT TYPE=\"hidden\" NAME=\"action\" VALUE=\"buildListForm\">";
    print "<INPUT TYPE=\"hidden\" NAME=\"split_taxon\" VALUE=\"".$q->param('split_taxon')."\">\n";
    print "<INPUT TYPE=\"hidden\" NAME=\"input_type\" VALUE=\"taxon\">\n";
                       

    my $i=0;
    foreach $homonym_name (@homonym_names) {
        my @taxon_nos = TaxonInfo::getTaxonNos($dbt,$homonym_name);

        # Find the parent taxon and use that to clarify the choice
        my %parents = %{Classification::get_classification_hash($dbt,'parent',\@taxon_nos,'names')};

        print '<TABLE BORDER=0 CELLSPACING=3 CELLPADDING=3>'."\n";
        print "<TR>";
        foreach $taxon_no (@taxon_nos) {
            print "<TD><INPUT TYPE='radio' CHECKED NAME='speciesname$i' VALUE='$taxon_no'>$homonym_name [$parents{$taxon_no}]</TD>";
        }
        print "<INPUT TYPE='hidden' NAME='keepspecies$i' VALUE='$homonym_name'>\n";
        print "</TR>";
        print "</TABLE>";
        $i++;
    }
    while(($taxon,$taxon_list) = each %splist) {
        print "<INPUT TYPE='hidden' NAME='speciesname$i' VALUE='$taxon_list'>";
        print "<INPUT TYPE='hidden' NAME='keepspecies$i' VALUE='$taxon'>\n";
        $i++;
    }
    print "<BR><INPUT TYPE='submit' NAME='submit' VALUE='Submit'></CENTER></FORM><BR><BR>";
}

# Displays a search page modeled after the collections search to search for local/regional sections
# PS 02/04/2005
sub displaySearchSectionForm{
    my $q = shift;
    my $s = shift;
    my $dbt = shift;
    my $hbo = shift;
    # Show the "search collections" form
    %pref = main::getPreferences($s->get('enterer_no'));
    my @prefkeys = keys %pref;
    my $html = $hbo->populateHTML('search_section_form', [ '', '', '', '', '', '','' ], [ 'research_group', 'eml_max_interval', 'eml_min_interval', 'lithadj', 'lithology1', 'lithadj2', 'lithology2', 'environment'], \@prefkeys);

    HTMLBuilder::buildAuthorizerPulldown($dbt, \$html );
    HTMLBuilder::buildEntererPulldown($dbt, \$html );
                                                                                                                                                             
    # Set the Enterer
    my $enterer = $s->get("enterer");
    $html =~ s/%%enterer%%/$enterer/;
    my $authorizer = $s->get("authorizer");
    $html =~ s/%%authorizer%%/$authorizer/;

    # Spit out the HTML
    print $html;
}

# Handles processing of the output from displaySectionSearchForm similar to displayCollResults
# Goes to next step if 1 result returned, else displays a list of matches
sub displaySearchSectionResults{
    my $q = shift;
    my $s = shift;
    my $dbt = shift;
    my $hbo = shift;

    my $limit = $q->param('limit') || 30;
    $limit = $limit*2; # two columns
    my $rowOffset = $q->param('rowOffset') || 0;

    # limit passed to permissions module
    my $perm_limit = 100000;

    my $ofRows = 0;
    my $method = "getReadRows";         # Default is readable rows
    my $p = Permissions->new( $s );

    # Build the SQL
    # which function to use depends on whether the user is adding a collection
    $q->param('sortby'=>'section_name');
    #if ($q->param('localsection') eq '') { $q->param('localsection'=>'NOT_NULL_OR_EMPTY'); }
    my $sql = main::processCollectionsSearch();
    my $sth = $dbt->dbh->prepare($sql);
    $sth->execute();    # run the query

    $action = "displayCollectionDetails"; $method = "getReadRows";

    # Get rows okayed by permissions module
    my @dataRows;
    $p->$method( $sth, \@dataRows, $perm_limit, \$ofRows );

    # get the enterer's preferences (needed to determine the number
    # of displayed blanks) JA 1.8.02

    local @period_order = TimeLookup::getScaleOrder($dbt,'2');
    # Convert max_interval_no to a period like 'Quaternary'
    my %int2period = %{TimeLookup::processScaleLookup($dbh,$dbt,'2','intervalToScale')};

    local $lastsection = '';
    local $lastregion  = '';
    local $found_localbed = 0;
    local $found_regionalbed = 0;
    local (%period_list,%country_list);
    my @tableRows = ();
    my $rowCount = scalar(@dataRows);
    my $row;
    local $taxon_resolution = $q->param('taxon_resolution') || 'species';
    local $show_taxon_list = $q->param('show_taxon_list') || 'NO';

    # Only used below, in a couple places.  Print contents of a table row
    sub formatSectionLine {
        my ($time_str, $place_str);
        foreach my $period (@period_order) {
            $time_str .= ", ".$period if ($period_list{$period});
        }
        foreach $country (keys %country_list) {
            $place_str .= ", ".$country; 
        }
        $time_str =~ s/^,//;
        $place_str =~ s/^,//;
        my $link = '';
        if ($lastregion && $found_regionalbed) {
            $link .= "<a href='bridge.pl?action=displayStratTaxaForm&taxon_resolution=$taxon_resolution&show_taxon_list=$show_taxon_list&input=".uri_escape($lastregion)."&input_type=regional'>$lastregion</a>";
            if ($lastsection) { $link .= " / "};
        }    
        if ($lastsection && $found_localbed) {
            $link .= "<a href='bridge.pl?action=displayStratTaxaForm&taxon_resolution=$taxon_resolution&show_taxon_list=$show_taxon_list&input=".uri_escape($lastsection)."&input_type=local'>$lastsection</a>";
        }    
            
        $link .= "<span class='tiny'> - $time_str - $place_str</span>";
            
    }

    # We need to group the collections here in the code rather than SQL so that
    # we can get a list of max_interval_nos.  There should generaly be only 1 country.
    # Assumes be do an order by localsection in the SQL and there are no null or empty localsections
    if ($rowCount > 0) {
        for($i=0;$i<$rowCount;$i++) {
            $row = $dataRows[$i];
            if ($i != 0 && (($row->{'localsection'} ne $lastsection) || ($row->{'regionalsection'} ne $lastregion))) {
                push @tableRows, formatSectionLine();
                %period_list = ();
                %country_list = ();
                $found_regionalbed = 0;
                $found_localbed = 0;
            }
            if ($row->{'regionalbed'}) {
                $found_regionalbed = 1;
            }    
            if ($row->{'localbed'}) {
                $found_localbed = 1;
            }    
            $lastsection = $row->{'localsection'};
            $lastregion  = $row->{'regionalsection'};
            $period_list{$int2period{$row->{'max_interval_no'}}} = 1;
            $country_list{$row->{'country'}} = 1;
        }
        push @tableRows, formatSectionLine();
    }

    $ofRows = scalar(@tableRows);
    if ($ofRows > 1) {       
        # Display header link that says which collections we're currently viewing
        print "<center>";
        print "<h3>Your search produced $ofRows matches</h3>\n";
        if ($ofRows > $limit) {
            print "<h4>Here are";
            if ($rowOffset > 0) {
                print " rows ".($rowOffset+1)." to ";
                $printRows = ($ofRows < $rowOffset + $limit) ? $ofRows : $rowOffset + $limit;
                print $printRows;
                print "</h4>\n";
            } else {
                print " the first ";
                $printRows = ($ofRows < $rowOffset + $limit) ? $ofRows : $rowOffset + $limit;
                print $printRows;
                print " rows</h4>\n";
            }
        }
        print "</center>\n";
        print "<br>\n";
        print "<table width='100%' border=0 cellpadding=4 cellspacing=0>\n";

        # print columns header
        print '<tr><th align=left nowrap>Section name</th>';
        if ($rowOffset + $limit/2 < $ofRows) { 
            print '<th align=left nowrap>Section name</th>';
        }    
        print '</tr>';
   
        # print each of the rows generated above
        for($i=$rowOffset;$i<$ofRows && $i < $rowOffset+$limit/2;$i++) {
            # should it be a dark row, or a light row?  Alternate them...
            if ( $i % 2 == 0 ) {
                print "<tr class=\"darkList\">";
            } else {
                print "<tr>";
            }
            print "<td>$tableRows[$i]</td>";
            if ($i+$limit/2 < $ofRows) {
                print "<td>".$tableRows[$i+$limit/2]."</td>";
            } else {
                print "<td>&nbsp;</td>";
            }
            print "</tr>\n";
        }
 
        print "</table>\n";
    } elsif ($ofRows == 1 ) { # if only one row to display, cut to next page in chain
        my $section;
        if (!$lastsection || $q->param('section_name') eq $lastregion) {
            $section = $lastregion;
            $section_type='regional';
        } else {    
            $section = $lastsection;
            $section_type='local';    
        }
        print "<center>\n<h3>Your search produced exactly one match ($section)</h3></center>";

        $my_q = new CGI({'show_taxon_list'=>$show_taxon_list,
                         'taxon_resolution'=>$taxon_resolution,
                         'input'=>$section,
                         'input_type'=>$section_type});
        displayStratTaxa($my_q,$s,$dbt);
        return;
    } else {
        print "<center>\n<h3>Your search produced no matches</h3>";
        print "<p>Please try again with fewer search terms.</p>\n</center>\n";
    }
 
    ###
    # Display the footer links
    ###
    print "<center><p>";
 
    # this q2  var is necessary because the processCollectionSearch
    # method alters the CGI object's internals above, and deletes some fields
    # so, we create a new CGI object with everything intact
    my $q2 = new CGI;
    my @params = $q2->param;
    my $getString = "rowOffset=".($rowOffset+$limit);
    foreach $param_key (@params) {
        if ($param_key ne "rowOffset") {
            if ($q2->param($param_key) ne "" || $param_key eq 'section_name') {
                $getString .= "&".uri_escape($param_key)."=".uri_escape($q2->param($param_key));
            }
        }
    }
 
    if (($rowOffset + $limit) < $ofRows) {
        my $numLeft;
        if (($rowOffset + $limit + $limit) > $ofRows) {
            $numLeft = "the last " . ($ofRows - $rowOffset - $limit);
        } else {
            $numLeft = "the next " . $limit;
        }
        print "<a href='$exec_url?$getString'><b>Get $numLeft sections</b></a> - ";
    }
    print "<a href='$exec_url?action=displaySearchSectionForm'><b>Do another search</b></a>";

    print "</center></p>";
    # End footer links

}
#----------------------FIRST-PAGE-----------------------------------------------

sub displayTaxaIntervalsForm {
    my $q = shift;
    my $s = shift;
    my $dbt = shift;
    my $hbo = shift;
    # Show the "search collections" form
    %pref = main::getPreferences($s->get('enterer_no'));
    my @prefkeys = keys %pref;
    my $html = $hbo->populateHTML('taxa_intervals_form', [], [], \@prefkeys);
                                                                                                                                                             
    # Spit out the HTML
    print $html;
}

sub displayTaxaIntervalsResults{
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $hbo=shift; 
    my $dbh=$dbt->dbh;
# ----------------------REMAKE SPECIES LIST-----ALSO REMOVE UNCHECKED--------------

    # if homonyms found, display homonym chooser form
    # if no homonyms: 
    #   buildList:
    #     if 'split_taxon' (aka analyze species separately) is 'yes'
    #        display a list of taxa to choose (buildList)
    #     else 
    #       display options from
    if ($q->param('input')) {
        my @taxa = split(/\s*[, \t\n-:;]{1}\s*/,$q->param('input'));

        my %splist;
        my @homonyms;
        
        foreach $taxon (@taxa) {
            @taxon_nos = TaxonInfo::getTaxonNos($dbt,$taxon);
            if (scalar(@taxon_nos) > 1) {
                push @homonyms, $taxon;
            } elsif (scalar(@taxon_nos) == 1) {
                $splist{$taxon} = $taxon_nos[0];
            } else {
                $splist{$taxon} = $taxon;
            }
        }

        if (scalar(@homonyms) > 0) {
            displayHomonymForm($q,$s,$dbt,\@homonyms,\%splist);
        } else {
            buildList($q, $s, $dbt, $hbo,\%splist);
        }
    } else {
        displayTaxaIntervalsForm($q, $s, $dbt,$hbo);
    }
    main::dbg("Species list: ".Dumper(\%splist));
}
#--------------------------TAXON LIST BUILDER------------------------------------

sub buildList    {
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $hbo=shift;
    my $dbh=$dbt->dbh;
    my $splist_base=shift;
    my %splist_base=%$splist_base;
    my %splist;
    my @notfound;

    # Set from homonym form
    if (!%splist_base) {
        for (my $i=0;$q->param("speciesname$i");$i++)  {
            $splist_base{$q->param("keepspecies$i")} = $q->param("speciesname$i");
        }    
    }

    # Use taxonomic search to build up a list of taxon_nos that are 
    # children of the potentially higher order taxonomic names entered in by the user
    # splist_base is the list of higher order names that haven't been
    # passed thru taxonomic_search
    while(($taxon_name,$no_or_name)=each(%splist_base)) {
        my $found = 0;
        if ($no_or_name =~ /^\d+$/) {
            my @children = PBDBUtil::taxonomic_search($dbt,$no_or_name);
            # Found the taxon in the authorities table, get its children
#            my @children = PBDBUtil::getChildren($dbt,$taxon_no);

            # Get alternate spellings
#            my $sql = "SELECT DISTINCT taxon_no, taxon_name, taxon_rank FROM opinions, authorities ".
#                      "WHERE opinions.child_spelling_no=authorities.taxon_no ".
#                      "AND status IN  ('rank changed as','recombined as','corrected as') ".
#                      "AND child_no=$taxon_no ".
#                      "AND child_spelling_no!=$taxon_no".
#                      "ORDER BY taxon_name";
#            my $spellings= $dbt->getData($sql);

            foreach $taxon_no (@children) {
                # Make sure its in the occurrences table first
                $sql = "SELECT taxon_name,genus_name,species_name FROM occurrences ".
                       " LEFT JOIN authorities ON occurrences.taxon_no=authorities.taxon_no ".
                       " WHERE occurrences.taxon_no IN ($taxon_no) LIMIT 1";
                @results = @{$dbt->getData($sql)};
                if (scalar(@results) > 0) {
                    $found = 1;
                    # split the children up into separate checkboxes by having different entries in %splist
                    if ($q->param('split_taxon') eq 'yes') {
                        #if ($taxon_name) {
                        #my $taxon = TaxonInfo::getTaxon($dbt,'taxon_no'=>$taxon_no);
                        #$splist{$taxon->{'taxon_name'}} = $taxon_no;
                        #} else {
                        $splist{$results[0]->{'genus_name'}.' '.$results[0]->{'species_name'}} = $taxon_no;
                        #}
                    # or clumped together under the same checkbox by having a comma separated list associated with that higher name
                    } else {
                        $splist{$taxon_name} .= ",".$taxon_no;
                    }
                } 
            }
            if (exists $splist{$taxon_name}) {
                $splist{$taxon_name} =~ s/^,//;
            }
        } else {
            $sql = "SELECT collection_no FROM occurrences WHERE genus_name=".$dbh->quote($no_or_name);
            main::dbg("genus sql: $sql");
            $found = scalar(@{$dbt->getData($sql)});
            if ($found) {
                $splist{$q->param('input')} = $q->param('input');
            }
        }    

        if (!$found) {
            push @not_found, $taxon_name; 
        #    print "<center><table><tr><th><font color=\"red\">Sorry, </font><font color=\"blue\">".
        #          "<i>$taxon_name</i></font><font color=\"red\"> is not in the database</font>".
        #          "</th></tr></table></CENTER><BR><BR>";
        } 
    } 

    # Now print out the list generated above so the user can select potential species to exclude
    # if they selected 'analyze taxa separately'. Otherwise skip to the options form
    if (!scalar keys %splist) {
        print "<center><h3><div class='warning'>Sorry, no occurrences of the taxa entered were found in the database.</div></h3></center>";
        displayTaxaIntervalsForm($q,$s,$dbt,$hbo);
    } else {
        if ($q->param('split_taxon') eq 'yes') {
            print "<DIV CLASS=\"title\">Confidence interval taxon list</DIV><BR>";
            print "<FORM ACTION=\"bridge.pl\" METHOD=\"post\"><INPUT TYPE=\"hidden\" NAME=\"action\" VALUE=\"showOptionsForm\">";
            print "<INPUT TYPE=\"hidden\" NAME=\"input_type\" VALUE=\"taxon\">";
            print "<CENTER>";

            # Print out a list of taxa 3 columns wide
            print "<TABLE CELLPADDING=5 BORDER=0>";
            my @sortList = sort {$a cmp $b} keys(%splist);
            my $columns = int(scalar(@sortList)/3)+1;
            for($i=0;$i<$columns;$i++) {
                print "<TR>";
                for($j=$i;$j<scalar(@sortList);$j=$j+$columns) {
                    $splist{$sortList[$j]} =~ s/,$//; 
                    print "<TD><INPUT TYPE=checkbox NAME=keepspecies$j VALUE='$sortList[$j]' CHECKED=checked>" . 
                          "<i>".$sortList[$j] . "</i><INPUT TYPE=hidden NAME=\"speciesname$j\" VALUE=\"$splist{$sortList[$j]}\"></TD>\n";
                }
                print "</TR>";
            }
            print "</TABLE>";

            print "<TABLE CELLPADDING=5 BORDER=0>";
            print "</TABLE><BR>"; 
            print "<INPUT TYPE=\"submit\" VALUE=\"Submit\">";
            print "</FORM></CENTER><BR><BR>";
        } else {
            $q->param('input_type'=>'taxon');
            optionsForm($q, $s, $dbt, \%splist);
        }
    }
}
#--------------DISPLAYS TAXA IN STRATIGRAPHIC SECTION FOR EDITING BY USER-------

sub displayStratTaxa{
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $dbh=$dbt->dbh;
    my %splist;
    my $section_name = $q->param("input");
    my $section_type = ($q->param("input_type") eq 'regional') ? 'regional' : 'local';
    my $sql = "SELECT occurrence_no, occurrences.taxon_no, genus_name, species_name, taxon_name, taxon_rank".
              " FROM collections, occurrences ".
              " LEFT JOIN authorities ON occurrences.taxon_no=authorities.taxon_no".
              " WHERE occurrences.collection_no=collections.collection_no ".
              " AND ${section_type}section = " . $dbh->quote($section_name) . " AND ${section_type}bed REGEXP '^[0-9]+\$'".
              " GROUP BY taxon_no,genus_name,species_name ";
    main::dbg($sql);
    my @strat_taxa_list= @{$dbt->getData($sql)};
    my %taxonList;
    # We build a comma separated list of taxon_nos to pass in. If the taxon_resolution is species,
    # the list will always have one taxon_no in it, if its genus, it may have more. If theres no
    # taxon_no, use the genus+species name
    foreach my $row (@strat_taxa_list) {
        if ($row->{'taxon_no'}) {
            if ($q->param('taxon_resolution') eq 'genus') {
                $splist{$row->{'genus_name'}} .= $row->{'taxon_no'}.",";
            } else { #species resolution
                $splist{$row->{'genus_name'}.' '.$row->{'species_name'}} .= $row->{'taxon_no'}.",";
            }
        } else {
            if ($q->param('taxon_resolution') eq 'genus') {
                $splist{$row->{'genus_name'}} .= $row->{'genus_name'}.",";
            } else { #species resolution
                $splist{$row->{'genus_name'}.' '.$row->{'species_name'}} .= $row->{'genus_name'}.' '.$row->{'species_name'}.",";
            }
        }
    }

    if ($q->param('show_taxon_list') eq 'NO') {
        optionsForm($q, $s, $dbt, \%splist);
    } else {
        print "<DIV CLASS=\"title\">Stratigraphic section taxon list</DIV><BR>";
        print "<CENTER><TABLE CELLPADDING=5 BORDER=0>";
        print "<FORM ACTION=\"bridge.pl\" METHOD=\"post\"><INPUT TYPE=\"hidden\" NAME=\"action\" VALUE=\"showOptionsForm\">";
        print "<INPUT TYPE=\"hidden\" NAME=\"input\" VALUE=\"".uri_escape($section_name)."\">";
        print "<INPUT TYPE=\"hidden\" NAME=\"taxon_resolution\" VALUE=\"".$q->param("taxon_resolution")."\">";
        print "<INPUT TYPE=\"hidden\" NAME=\"input_type\" VALUE=\"".$q->param('input_type')."\">\n";
        my @sortList = sort alphabetically keys(%splist);
        my $columns = int(scalar(@sortList)/3)+1;
        for($i=0;$i<$columns;$i++) {
            print "<TR>";
            for($j=$i;$j<scalar(@sortList);$j=$j+$columns) {
                $splist{$sortList[$j]} =~ s/,$//; 
                print "<TD><INPUT TYPE=checkbox NAME=keepspecies$j VALUE='$sortList[$j]' CHECKED=checked>" . 
                      "<i>".$sortList[$j] . "</i><INPUT TYPE=hidden NAME=\"speciesname$j\" VALUE=\"$splist{$sortList[$j]}\"></TD>\n";
            }
            print "</TR>";
        }
        print "</CENTER></TABLE><BR>";
        print "<CENTER><SPAN CLASS=\"tiny\">(To remove taxon from list for analysis, uncheck before pressing 'Submit')</SPAN><BR><BR>";
        print "<INPUT TYPE=\"submit\" VALUE=\"Submit\">";
        #print "<A HREF=\"/cgi-bin/bridge.pl?action=displayFirstForm\"><INPUT TYPE=\"button\" VALUE=\"Start again\"></A>";
        print "</CENTER><BR><BR></FORM>";
    } 
    return;
}
#---------------OPTIONS FORM FOR CALCULATING CONFIDENCE INTERVALS---------------

sub optionsForm    {
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $splist=shift;
    my %splist = %$splist;
    # A large form is meant to be displayed on a page by itself, before the chart is drawn
    # A small form is meant to displayed alongside a chart, so must be tall and skinny, and use different styles
    my $form_type = (shift || "large");
    
    my $section_name = uri_unescape($q->param("input"));
    my $type = $q->param("input_type");

# -----------------REMAKE STRAT LIST-----------(REMOVES UNCHECKED)-----------
    if (!%splist) {
        my $testspe =0;
        my $testyes =0;
        while ($q->param("speciesname$testspe"))  {
            if ($q->param("keepspecies$testspe"))   {
                $splist{$q->param("keepspecies$testspe")} = $q->param("speciesname$testspe");
                $testyes++;
            }
            $testspe++;
        }    
    } 
#----------------------BUILD LIST OF SCALES TO CHOOSE FROM--------------------
    my $sql = "SELECT authorizer_no,scale_no,scale_name,reference_no FROM scales";
    my @results = @{$dbt->getData($sql)};
    my %scale_strings;
    for my $scaleref (@results)     {
        my $option;
        if ($scaleref->{scale_no} == 6)	{
            $option = "<option VALUE=\"" . $scaleref->{scale_no} . "\" selected>";
        } else {
            $option = "<option VALUE=\"" . $scaleref->{scale_no} . "\">";
        }
        my $name = $scaleref->{scale_name};
        my $sql2 = "SELECT author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=" . $scaleref->{reference_no};
        my @results2 = @{$dbt->getData($sql2)};
        my $auth = $results2[0]->{author1last};
        if ( $results2[0]->{otherauthors} || $results2[0]->{author2last} eq "et al." )  {
                $auth .= " et al.";
        } elsif ( $results2[0]->{author2last} ) {
                $auth .= " and " . $results2[0]->{author2last};
        }
        $auth .= " " . $results2[0]->{pubyr};
        $auth = " [" . $auth . "]\n";
        $scale_strings{$name} = $option . $name . $auth;
    }
        
#------------------------------OPTIONS FORM----------------------------------

    if ($type eq 'taxon')   {
        print "<FORM ACTION=\"bridge.pl\" METHOD=\"post\"><INPUT TYPE=\"hidden\" NAME=\"action\" VALUE=\"calculateTaxaInterval\">";
    } else  {
        print "<FORM ACTION=\"bridge.pl\" METHOD=\"post\"><INPUT TYPE=\"hidden\" NAME=\"action\" VALUE=\"calculateStratInterval\">";
        print "<INPUT TYPE=\"hidden\" NAME=\"input\" VALUE=\"".uri_escape($section_name)."\">";
        print "<INPUT TYPE=\"hidden\" NAME=\"taxon_resolution\" VALUE=\"".$q->param("taxon_resolution")."\">";
    }    
    print "<INPUT TYPE=\"hidden\" NAME=\"input_type\" VALUE=\"".$q->param('input_type')."\">";
    for(my $i=0;($taxon_name,$taxon_list) = each(%splist);$i++){
        print "<INPUT TYPE=hidden NAME=keepspecies$i VALUE='$taxon_name' CHECKED=checked><INPUT TYPE=hidden NAME=\"speciesname$i\" VALUE=\"$taxon_list\">\n";
    }     

    if ($form_type eq 'large') {
        print "<DIV CLASS=\"title\">Confidence interval options</DIV>";
        print '<CENTER><TABLE CELLPADDING=5 BORDER=0>';
        
        if ($type eq 'taxon')   {    
            print "<TR><TH align=\"right\"> Time-scale: </TH><TD><SELECT NAME=\"scale\">";
            my @sorted = sort keys %scale_strings;
            for my $string (@sorted)        {
                print $scale_strings{$string};
            }       
            print "</SELECT></TD></TR>";
        } 
        
        print "<TR><TH></TH><TD><SPAN CLASS=\"tiny\">(Please select a time-scale that is appropriate for the taxa you have chosen)</SPAN></TD></TR>";
        print "<TR><TH align=\"right\"> Confidence interval method: </TH><TD> <SELECT NAME=\"conftype\"><OPTION>Strauss and Sadler (1989)<OPTION>Marshall (1994)<OPTION>Solow (1996)</SELECT><A HREF=\"javascript: tipsPopup('/public/tips/confidencetips1.html')\">   Help</A></TD></TR>";
        #<OPTION>Marshall (1990)<OPTION>Marshall (1997)<OPTION>Solow (2003)<OPTION>Holland (2003)
    #    print "<TR><TH></TH><TD><SPAN CLASS=\"tiny\">(Warning: Know thine assumptions)</SPAN></TD></TR>";
        print "<TR><TH align=\"right\"> Estimate: </TH><TD> <SELECT NAME=\"conffor\"><OPTION>total duration<OPTION>first appearance<OPTION>last appearance<OPTION>no confidence intervals</SELECT></TD><TR>";
        print "<TR><TH align=\"right\"> Confidence level: </TH><TD> <SELECT NAME=\"alpha\"><OPTION>0.99<OPTION SELECTED>0.95<OPTION>0.8<OPTION>0.5<OPTION>0.25</SELECT></TD></TR>";
        print "<TR><TH align=\"right\"> Order taxa by: </TH><TD> <SELECT NAME=\"order\"><OPTION>name<OPTION SELECTED>first appearance<OPTION>last appearance<OPTION>stratigraphic range</SELECT></TD><TR></TABLE><BR>";
        print "<INPUT NAME=\"full\" TYPE=\"submit\" VALUE=\"Submit\">";
    #    print "<A HREF=\"/cgi-bin/bridge.pl?action=displayFirstForm\"><INPUT TYPE=\"button\" VALUE=\"Start again\" STYLE=\"color:red\"></A>
        print "</FORM></CENTER><BR><BR>";
    } else {
        print '<CENTER><TABLE CLASS="darkList" CELLPADDING=5 BORDER=0 style="border: 1px #000000 solid">';
        print '<TR><TH ALIGN="CENTER" COLSPAN=4><DIV CLASS="large">Options</DIV></TH><TR>';
        
        if ($type eq 'taxon')   {    
            print "<TR><TH align=\"right\"> Time-scale: </TH><TD COLSPAN=3><SELECT NAME=\"scale\">";
            my @sorted = sort keys %scale_strings;
            for my $string (@sorted)        {
                print $scale_strings{$string};
            }       
            print "</SELECT></TD>";
        } 

        print "<TR><TH align=\"right\"> Confidence interval method: </TD><TD><SELECT NAME=\"conftype\"><OPTION>Strauss and Sadler (1989)<OPTION>Marshall (1994)<OPTION>Solow (1996)</SELECT><A HREF=\"javascript: tipsPopup('/public/tips/confidencetips1.html')\">   Help</A></TD>";
        print "<TH align=\"right\"> Confidence level: </TH><TD><SELECT NAME=\"alpha\"><OPTION>0.99<OPTION SELECTED>0.95<OPTION>0.8<OPTION>0.5<OPTION>0.25</SELECT></TD></TR>";
        print "<TR><TH align=\"right\"> Estimate: </TH><TD><SELECT NAME=\"conffor\"><OPTION>total duration<OPTION>first appearance<OPTION>last appearance<OPTION>no confidence intervals</SELECT></TD>";
        print "<TH ALIGN=\"right\">Order taxa by: </TH><TD><SELECT NAME=\"order\"><OPTION>name<OPTION SELECTED>first appearance<OPTION>last appearance<OPTION>stratigraphic range</SELECT></TD><TR>";
        print "</TABLE><BR>";
        print "<INPUT NAME=\"full\" TYPE=\"submit\" VALUE=\"Submit\">";
        print "</FORM></CENTER><BR><BR>";
    }
    return;
}

#----------------------------------CALCULATE-INTERVALS-----------------------------------------

sub calculateTaxaInterval {
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $dbh=$dbt->dbh;
    my $i=0;
    my %splist;
    my $testspe =0;
    my $testyes =0;
    while ($q->param("speciesname$testspe"))  {
        if ($q->param("keepspecies$testspe"))   {
            $splist{$q->param("keepspecies$testspe")} = $q->param("speciesname$testspe");
            $testyes++;
        }
        $testspe++;
    }
    
    my $scale = $q->param("scale");
    my $C = $q->param("alpha");
#    my $C = 1 - $CC;
    my $conffor = $q->param("conffor");
    my $conftype = $q->param("conftype");

    if (scalar(keys(%splist)) == 1 && $conftype eq "Solow (1996)")   {

#    my $i = 0;


#    foreach my $speciesname (@list)   {
#        print "<INPUT TYPE=hidden NAME=\"speciesname$i\" VALUE=\"$speciesname\"></TD>";
#        $i++;
#    }     

        optionsForm($q, $s, $dbt, \%splist);
        print "<center><table><tr><th><font color=\"red\">The Solow (1996) method requires more than one taxon</font></th></tr></table></CENTER><br>";
        return;
    }

    my $order = $q->param("order");
    my %theHash;
    my %namescale;
    my @intervalnumber;
    my @not_in_scale;
    my($AILEFT,$AITOP,$fig_width,$fig_lenth);
    my $aifig_size = 500;

    $_ = TimeLookup::processScaleLookup($dbh,$dbt,$scale);
    my %timeHash = %{$_};

    @_ = TimeLookup::findBoundaries($dbh,$dbt,$scale);
    my %upperbound = %{$_[0]};
    my %lowerbound = %{$_[1]};

#    print "testing getScaleOrder ";
#    @a = TimeLookup::getScaleOrder($dbt,$scale,'number',1);
   #print Dumper(\@a);
#    die;
   #print "findboundaries ".Dumper(\%upperbound,\%lowerbound);
#        foreach my $keycounter (sort numerically keys(%upperbound)) {
#            print "$keycounter: ";
#                print "$upperbound{$keycounter} -- $lowerbound{$keycounter}";
#            print "<BR>";
#        }

    # Get all the necessary stuff to create a scale
    my $sql = "SELECT correlations.interval_no, interval_name, eml_interval,next_interval_no, lower_boundary FROM correlations,intervals ".
              " WHERE correlations.interval_no=intervals.interval_no" 
              . " AND scale_no = " . $scale;
    my @results = @{$dbt->getData($sql)};
    my $last_lower_boundary=0;
    foreach my $row (@results) {
        if ($row->{'eml_interval'} ne "")	{
            $namescale{$row->{'interval_no'}} = $row->{'eml_interval'} . " " . $row->{'interval_name'};
        } else {
            $namescale{$row->{'interval_no'}} = $row->{'interval_name'};
        }
        push @intervalnumber, $row->{interval_no};
        #if ($row->{'lower_boundary'}) {
        #    $lower_boundary = $row->{'lower_boundary'};
        #} else {
        #    $lower_boundary = $lowerbound{$row->{'interval_no'}};
        #}
        #$lowerbound{$row->{'interval_no'}} = $lower_boundary;
        #$upperbound{$row->{'interval_no'}} = $last_lower_boundary;
        #$last_lower_boundary = $lower_boundary;
    }
    #foreach $row (@results) {
    #    $interval_no = $row->{'interval_no'};
    #    print "$interval_no $namescale{$interval_no} $upperbound{$interval_no} $lowerbound{$interval_no}<BR>";
    #}
#    print "<BR>LB:".Dumper(\%lowerbound);
#    print "<BR>UB:".Dumper(\%upperbound);
#    print "<BR>NS:".Dumper(\%namescale);
#    foreach $interval_no (sort {$a <=> $b} keys %namescale) {
#        print "$interval_no $upperbound{$interval_no} $lowerbound{$interval_no}<BR>";
#    }
#    die;
    
    my %solowHash;
    my %masterHash;
    foreach my $keycounter (keys(%namescale)) {
        push @{$masterHash{$keycounter}}, $namescale{$keycounter};
        push @{$masterHash{$keycounter}}, $upperbound{$keycounter};
        push @{$masterHash{$keycounter}}, $lowerbound{$keycounter};
    }

# ----------------------------------------------------------------------------
    my $rusty = 1;
#    my $upper_crosser = 0;
    while(($taxon_name,$taxon_list)=each(%splist)){
        my @maxintervals;
        my @subsetinterval;
        my @nextinterval;
        my @lowerbound;
        my %taxaHash;
        my $sql;
# ---------------------START TAXON CHECKER----------------------------------
        my @taxon_list = split(",",$taxon_list);
        # We have a list of one of more numbers and we know $taxon_name exists in the authorities table
        if ($taxon_list[0] =~ /^\d+$/) {
            $sql = "SELECT collection_no FROM occurrences WHERE taxon_no IN (".join(",",map {$dbh->quote($_)} @taxon_list).")";
            my $taxon_name_link = $taxon_name;
            $taxon_name_link =~ s/\s*(indet(\.)*|sp\.)//;
            $links{$taxon_name} = "bridge.pl?action=checkTaxonInfo&taxon_name=$taxon_name_link";
        } else {
        # No taxon_no for it, match against occurrences table
            my @taxon = split(/\s+/,$taxon_list[0]);
            if (scalar(@taxon) == 2)        {
                $sql = "SELECT collection_no FROM occurrences WHERE genus_name=" . $dbh->quote($taxon[0]);
                $sql .= " AND species_name=" . $dbh->quote($taxon[1]);
                $taxon_rank = 'Genus and species';
            } elsif (scalar(@taxon) ==1) {
                if (!$row->{'species_name'}) {
                    $taxon_rank = 'Genus';
                } else {
                    $taxon_rank = 'Genus and species';
                }
                $sql = "SELECT collection_no FROM occurrences WHERE genus_name=" . $dbh->quote($taxon[0]);
            } 
            $links{$taxon_name} = "bridge.pl?action=checkTaxonInfo&taxon_name=$taxon_name&taxon_rank=$taxon_rank";
        }
        my @col_nos = @{$dbt->getData($sql)};
        my @collnums;
        for my $col_n (@col_nos)        {
            push @collnums, $col_n->{collection_no};
        }    
        #All the collection numbers containing a certain taxon
        #need to find max_inteval for each collection in chosen scale.
        my %anotherHash;
        my $count;
        foreach my $counter (keys(%timeHash)) {
            foreach my $arraycounter (@collnums) {
                if ($arraycounter == $counter) {
                    push @{$anotherHash{$timeHash{$counter}}}, $arraycounter;
                    $count++;
                } 
            }
        }
       
        main::dbg("anotherHash for $taxon_name:".Dumper(\%anotherHash));
       
        if (! scalar keys %anotherHash) {
            push @not_in_scale, $taxon_name;
        } else {
            push @masterHashOccMatrixOrder, $taxon_name;
            while (my ($interval_no,$interval_name) = each %namescale) {
                push @{$masterHash{$interval_no}}, scalar(@{$anotherHash{$interval_name}});
            }
            
            
            #------------FIND FIRST AND LAST OCCURRENCES---------------
            my $firstint;
            my $lastint;
            my @gaplist;
            my $temp = 0;
            my $temptime;
            my $m = 0;
            
            foreach my $interval_no (sort numerically keys(%masterHash)) {
                if (@{$masterHash{$interval_no}}[$rusty + 2] ne "" && $temp == 0) {
                    $lastint = $interval_no;
                    $temptime = @{$masterHash{$interval_no}}[2];
                    $temp = 1;
                }
                if (@{$masterHash{$interval_no}}[$rusty + 2] ne "" && $temp == 1) {
                    $firstint = $interval_no;
                    push @gaplist, sprintf("%.1f", @{$masterHash{$interval_no}}[2] - $temptime);  #round to 1 decimal precision
                    $temptime = @{$masterHash{$interval_no}}[2];
                    $m++;
                }
            }
            
            my $first = @{$masterHash{$firstint}}[2];
            my $last = @{$masterHash{$lastint}}[1];
            my $lastbottom = @{$masterHash{$lastint}}[2];
            my $intervallength = $first - $last;
            my $N = scalar(@gaplist);

            shift @gaplist;
            transpositionTest(\@gaplist, $N);   # TRANSPOSITION TEST
            @gaplist = sort numerically @gaplist;
            
            if ($conftype eq "Strauss and Sadler (1989)") {
                straussSadler($count,$C,$conffor,$intervallength);
            }
            if ($conftype eq "Marshall (1994)") {
                distributionFree(\@gaplist,$N,$C);
            }
            
            if ($conftype eq "Solow (1996)") {  
                $solowHash{$taxon_name} = [$m,$last,$first];
            }

            my $upper = $last;
            my $lower = $first;
        
            if ($conffor eq "last appearance" || $conffor eq "total duration")	{
                $upper = $last - $lastconfidencelengthlong;
                $uppershort = $last - $lastconfidencelengthshort;
                if ($upper > $uppershort) {$upper = $uppershort}
                if ($upper < 0) {
                	$upper = 0;
                	$upper_crosser = 1;
                }
                if ($uppershort < 0) {$uppershort = 0};
            }
        
            if ($conffor eq "first appearance" || $conffor eq "total duration")   {
                $lower = $first + $firstconfidencelengthlong;
                $lowershort = $first + $firstconfidencelengthshort;
                if ($lower < $lowershort) {$lower = $lowershort}
            }
        
            $theHash{$taxon_name} = [$upper, $lower, $first, $last, $firstconfidencelengthlong, $intervallength, $uppershort, $lowershort, $correlation,$N,$firstconfidencelengthshort,$lastconfidencelengthlong,0];
            
#IMPORTANT: UNLIKE OTHER METHODS, CAN"T CALCULATE SOLOW UNTIL FULL LIST IS BUILT
#    print "theHash ".Dumper(\%theHash)."<br>";
#    print "anotherHash ".Dumper(\%anotherHash)."<br>";
            $rusty++;
        }
    }
    



    my @mx;
    foreach my $keycounter (keys(%solowHash)) {
        push @mx, $solowHash{$keycounter}[2];
        push @mx, $solowHash{$keycounter}[1];
    }  #there must be an easier may to find the maximum horizon!!
    @mx = sort numerically @mx;
    my $Smax = $mx[scalar(@mx) - 1];
    my $Smin = $mx[0];

    if ($conftype eq "Solow (1996)") {  
        commonEndPoint(\%solowHash,$C,$conffor);
    
        foreach my $keycounter (keys(%theHash)) {
            if ($firstconfidencelengthlong != -999) {
                $theHash{$keycounter}[1] = $Smax + $firstconfidencelengthlong;
                $theHash{$keycounter}[4] = $firstconfidencelengthlong;
            }
            
            if ($lastconfidencelengthlong != -999) {
                $theHash{$keycounter}[0] = $Smin - $lastconfidencelengthlong;
                $theHash{$keycounter}[11] = $lastconfidencelengthlong;
            }        
        }
    } 
       
#--------------------------------GD---------------------------------------------

    my $fig_wide = scalar(keys(%theHash));
    $fig_width = 170 + (16 * $fig_wide);
    $fig_lenth = 250 + 400;
    $AILEFT = 0;
    $AITOP = 580;   

    $image_count = getImageCount();
                                                                                                                                                             
    my $imagenamejpg = "confimage$image_count.jpg";
    my $imagenamepng = "confimage$image_count.png";
    my $imagenameai = "confimage$image_count.ai";
    my $image_map = "<map name='ConfidenceMap'>";
    open(IMAGEJ, ">$IMAGE_DIR/$imagenamejpg");
    open(IMAGEP, ">$IMAGE_DIR/$imagenamepng");
    open(AI,">$IMAGE_DIR/$imagenameai");
    open(AIHEAD,"<./data/AI.header");
    while (<AIHEAD>)	{
        print AI $_;
    }
    close AIHEAD;
    my $gd = GD::Image->new($fig_width,$fig_lenth);       
    my $poly = GD::Polygon->new();    
    my $white  = $gd->colorAllocate(255, 255, 255);
    my $aiwhite = "0.00 0.00 0.00 0.00 K"; 
    my $grey= $gd->colorAllocate(  192,   192,   192);
    my $aigrey = "0.33 0.33 0.33 0.12 k";
    my $black  = $gd->colorAllocate(  0,   0,   0);
    my $aiblack = "0.00 0.00 0.00 1.00 K";
#    $gd->rectangle(0,0,$fig_width - 1,$fig_lenth - 1,$black);
#-------------------------MAKING SCALE----------------
    my $upperlim = 1000;
    my $lowerlim = 0;

    my %periodinclude;
    my $mintemp;
    my $maxtemp;
    foreach my $count (keys(%theHash)) {
        my $tempupp = 0;
        my $templow = 0;
        foreach my $counter (sort numerically keys(%masterHash))	{
            if ($mintemp == 0) {
                $mintemp = $counter;
            }
            if ($masterHash{$counter}[2] > @{$theHash{$count}}[0] && $tempupp == 0)	{
	            $tempupp = $counter - 2;
            }
            if ($masterHash{$counter}[1] > @{$theHash{$count}}[1] && $templow == 0)	{
                $templow = $counter;
            }
            $maxtemp = $counter;
        }
        if ($tempupp < $mintemp) {
        	push @{$masterHash{$mintemp - 1}}, ('', 0, @{$masterHash{$mintemp}}[1], 0);
        	push @{$masterHash{$mintemp - 2}}, ('', 0, 0, 0);
        	$periodinclude{$tempupp - 1} = $masterHash{$tempupp - 1}[2];
        }

  #      $periodinclude{$tempupp} = $masterHash{$tempupp + 1}[1];

        if ($templow > $maxtemp) {$templow = $maxtemp};
        if ($tempupp < $upperlim) {$upperlim = $tempupp};
        if ($templow > $lowerlim) {$lowerlim = $templow};
    }

	foreach my $counter ($upperlim..$lowerlim)	{
        $periodinclude{$counter} = $masterHash{$counter}[2];
    }
#    print "<br><br>periodinclude".Dumper(\%periodinclude);
#    print "<br><br>masterHash ".Dumper(\%masterHash)."<br>";
#    print "<br><br>theHash ".Dumper(\%theHash)."<br>";
    
    
#    print "<br>periodinclude " . Dumper(\%periodinclude) . "<br>";
#	print "<br> mintemp: $mintemp    $upperlim     $lowerlim";
	    
    my $lowerval = $masterHash{$lowerlim}[2];
    my $upperval = $masterHash{$upperlim}[1];
    my $totalval = $lowerval - $upperval;
    
#	print "<br> upperval: $upperval    lowerval: $lowerval    totalval: $totalval";
    
    my $millionyr = 400 / $totalval;
    my $marker = 150;
    my $Smarker = 150;
    my $aimarker = 150;
    my $tempn = 0;
#    my $first_rep = 0;
    my $leng;
    foreach my $key (sort numerically keys(%periodinclude))	{

#        print "$key $periodinclude{$key}"; 
        my $temp = 230 + (($periodinclude{$key} - $upperval) * $millionyr);
	
        if (($temp - $tempn) > 17 && $tempn > 0)	{
            my $interval_name = @{$masterHash{$key}}[0];
            $interval_name =~ s/Early\/Lower/Early/;
            $interval_name =~ s/Late\/Upper/Late/;
            $leng = length($interval_name);
            $gd->string(gdTinyFont, $marker - 35 - ($leng * 5), ((($temp - $tempn)/2) + $tempn - 3) , $interval_name, $black);
            aiText("null",$aimarker - 140, ((($temp - $tempn)/2) + $tempn + 3) ,$interval_name,$aiblack);       #AI
#            $leng = length(@{$masterHash{$key}}[0]);
#            print "<br> length is $leng";
        }
        if (($temp - $tempn) > 10)	{
            $leng = length(sprintf("%.1f", $masterHash{$key}[2]));
            $gd->string(gdTinyFont, $marker - 48  - ($leng * 5), $temp - 3, sprintf("%.1f", $masterHash{$key}[2]), $black);
            aiText("null", $aimarker - 75, $temp + 3, sprintf("%.1f", $masterHash{$key}[2]),$aiblack);       #AI
        }
        if ($tempn != 0) {
            my @interval_array = split(/ /,@{$masterHash{$key}}[0]);
            my ($eml,$name);
            if (scalar(@interval_array) > 1) {($eml,$name) = @interval_array; }
            else {($name)=@interval_array;}
            $image_map .= "<area shape=rect coords=\"10,".int($temp).",".($marker-30).",".int($tempn)."\" HREF=\"bridge.pl?action=displayCollResults&eml_max_interval=$eml&max_interval=$name&eml_min_interval=$eml&min_interval=$name&taxon_list=".join(",",values %splist)."\" ALT=\"".@{$masterHash{$key}}[0]."\">";
        }
        $gd->line($marker - 35, $temp, $marker - 30, $temp, $black);
        aiLine($aimarker - 35, $temp, $aimarker - 30, $temp,$aiblack);    #AI
        $tempn += $temp;
    }
    $gd->line($marker - 30, 230, $marker - 30, $fig_lenth - 20, $black);
    aiLine($aimarker - 30, 230,$aimarker - 30, 530, $aiblack);    #AI
    
# -------------------------------SORT OUTPUT----------------------------
    sub sortHashLasti {$theHash{$b}[2] <=> $theHash{$a}[2]};
    sub sortHashFirsti {$theHash{$b}[3] <=> $theHash{$a}[3]};
    sub sortHashLenthi {$theHash{$a}[5] <=> $theHash{$b}[5]};
    my @sortedKeys = keys(%theHash);
    if ($order eq "first appearance")   {
        @sortedKeys = sort sortHashLasti sort sortHashFirsti sort alphabetically (@sortedKeys);
    } elsif ($order eq "last appearance") {
        @sortedKeys = sort sortHashFirsti sort sortHashLasti sort alphabetically (@sortedKeys);
    } elsif ($order eq "name")   {
        @sortedKeys = sort alphabetically (@sortedKeys);
    } else  {
        @sortedKeys = sort sortHashLenthi sort sortHashFirsti sort alphabetically (@sortedKeys);    
    }
#---------------------------BARS---------------------------
    my $dotmarkerfirst;
    my $dotmarkerlast;
    my $dottemp = 0;
    my $barup;
    my $bardn;
    foreach my $something (@sortedKeys) {
        $barup = 230 + (@{$theHash{$something}}[3] - $upperval) * $millionyr;
        $bardn = 230 + (@{$theHash{$something}}[2] - $upperval) * $millionyr;
      
        #-------------------- GREY BOXES (PS) ------------------------ 
        my $tempn =0;
        my $idx=-1;
        for ($i=0;$i<scalar(@masterHashOccMatrixOrder);$i++) { if ($something eq $masterHashOccMatrixOrder[$i]) {$idx=3+$i} }
                        
        foreach my $key (sort numerically keys(%periodinclude))	{
            my $temp = 230 + (($periodinclude{$key} - $upperval) * $millionyr);
           
            if (@{$masterHash{$key}}[$idx]) {
                $gd->filledRectangle($marker+1,int($tempn),$marker+4,int($temp),$grey);
                aiFilledRectangle($aimarker,int($tempn),$aimarker+5,int($temp),$aigrey);
                # use the taxon_no if possible
                my $taxon_list = $splist{$something};
                my @interval_array = split(/ /,@{$masterHash{$key}}[0]);
                my ($eml,$name);
                if (scalar(@interval_array) > 1) {($eml,$name) = @interval_array; } 
                else {($name)=@interval_array;} 
                $image_map .= "<area shape=rect coords=\"$marker,".int($temp).",".($marker+5).",".int($tempn)."\" HREF=\"bridge.pl?action=displayCollResults&eml_max_interval=$eml&max_interval=$name&eml_min_interval=$eml&min_interval=$name&taxon_list=$taxon_list\" ALT=\"".@{$masterHash{$key}}[0]."\">";
            }
            $tempn += $temp;
        }
 
        if ($dottemp == 0) { 
            $dotmarkerfirst = $bardn;
            $dotmarkerlast = $barup;
            $dottemp = 1;
        }
        
#        print "upper:" . @{$theHash{$something}}[0] . "<BR>";
#        print "lower:" . @{$theHash{$something}}[1] . "<BR>";
#        print "first:" . @{$theHash{$something}}[2] . "<BR>";
#        print "last:" . @{$theHash{$something}}[3] . "<BR>";
#        print "uppershort:" . @{$theHash{$something}}[6] . "<BR>";
#        print "lowershort:" . @{$theHash{$something}}[7] . "<BR>";

        my $limup;
        my $limdn;
        my $triangle = 0;
        if (@{$theHash{$something}}[7] == @{$theHash{$something}}[1]) {
            $limup = 230 + (@{$theHash{$something}}[6] - $upperval) * $millionyr;
            $limdn = 230 + (@{$theHash{$something}}[7] - $upperval) * $millionyr;
            $triangle = 1;
        } else {
            $limup = 230 + (@{$theHash{$something}}[0] - $upperval) * $millionyr;
            $limdn = 230 + (@{$theHash{$something}}[1] - $upperval) * $millionyr;
        }
#        print "<br><br>limup $limup   limdn $limdn";
        my $limupshort = 230 + (@{$theHash{$something}}[6] - $upperval) * $millionyr;
        my $limdnshort = 230 + (@{$theHash{$something}}[7] - $upperval) * $millionyr;
        
        # Draw confidence bar
        $gd->line($marker,$barup,$marker,$bardn, $black);
        $gd->line($marker + 5,$barup,$marker + 5,$bardn, $black);
        $gd->line($marker,$barup,$marker + 5,$barup, $black);
        $gd->line($marker,$bardn,$marker + 5,$bardn, $black);
        
        aiLine($aimarker,$barup,$aimarker,$bardn, $aiblack);    #AI
        aiLine($aimarker + 5,$barup,$aimarker + 5,$bardn, $aiblack);    #AI
        aiLine($aimarker,$barup,$aimarker + 5,$barup, $aiblack);    #AI
        aiLine($aimarker,$bardn,$aimarker + 5,$bardn, $aiblack);    #AI
        my $recent = 0;
        if ($barup == 50)   {
            $recent = 1;  
        } elsif ($limup <= 50) {
            $recent = 2;
        } else  {
            if (($conffor eq "last appearance" || $conffor eq "total duration") && $conftype ne "Solow (1996)")	{
                $gd->rectangle($marker + 2,$barup,$marker + 3,$limup, $black);
                $gd->line($marker + 0,$limup,$marker + 5,$limup, $black); 
                $gd->line($marker + 0,$limupshort,$marker + 5,$limupshort, $black);
                aiRectangle($aimarker + 2,$barup,$aimarker + 3,$limup, $aiblack);
                aiLine($aimarker + 0,$limup,$aimarker + 5, $limup, $aiblack);
                aiLine($aimarker + 0,$limupshort,$aimarker + 5, $limupshort, $aiblack);
                if ($triangle == 1 && @{$theHash{$something}}[6] > 0) {
                	#if ($upper_crosser == 1) {$limupshort = $limup};
                    $gd->line($marker + 1,$limupshort - 1,$marker + 4,$limupshort - 1, $black);
                    $gd->line($marker + 1,$limupshort - 2,$marker + 4,$limupshort - 2, $black);
                    $gd->line($marker + 2,$limupshort - 3,$marker + 3,$limupshort - 3, $black);
                    $gd->line($marker + 2,$limupshort - 4,$marker + 3,$limupshort - 4, $black);
                    aiLine($aimarker + 1,$limupshort - 1,$aimarker + 4,$limupshort - 1, $aiblack);
                    aiLine($aimarker + 1,$limupshort - 2,$aimarker + 4,$limupshort - 2, $aiblack);
                    aiLine($aimarker + 2,$limupshort - 3,$aimarker + 3,$limupshort - 3, $aiblack);
                    aiLine($aimarker + 2,$limupshort - 4,$aimarker + 3,$limupshort - 4, $aiblack);
                }
            }
        }
        if (($conffor eq "first appearance" || $conffor eq "total duration") && $conftype ne "Solow (1996)")   {
            $gd->rectangle($marker + 2,$bardn,$marker + 3,$limdn, $black);
            $gd->line($marker + 0,$limdn,$marker + 5,$limdn, $black); 
            $gd->line($marker + 0,$limdnshort,$marker + 5,$limdnshort, $black); 
            aiRectangle($aimarker + 2,$bardn,$aimarker + 3,$limdn, $aiblack);
            aiLine($aimarker + 0,$limdn,$aimarker + 5, $limdn, $aiblack);
            aiLine($aimarker + 0,$limdnshort,$aimarker + 5, $limdnshort, $aiblack);
                if ($triangle == 1) {
                    $gd->line($marker + 1,$limdnshort + 1,$marker + 4,$limdnshort + 1, $black);
                    $gd->line($marker + 1,$limdnshort + 2,$marker + 4,$limdnshort + 2, $black);
                    $gd->line($marker + 2,$limdnshort + 3,$marker + 3,$limdnshort + 3, $black);
                    $gd->line($marker + 2,$limdnshort + 4,$marker + 3,$limdnshort + 4, $black);
                    aiLine($aimarker + 1,$limdnshort + 1,$aimarker + 4,$limdnshort + 1, $aiblack);
                    aiLine($aimarker + 1,$limdnshort + 2,$aimarker + 4,$limdnshort + 2, $aiblack);
                    aiLine($aimarker + 2,$limdnshort + 3,$aimarker + 3,$limdnshort + 3, $aiblack);
                    aiLine($aimarker + 2,$limdnshort + 4,$aimarker + 3,$limdnshort + 4, $aiblack);
                }
        }
        $gd->stringUp(gdSmallFont, $marker-5, 200, "$something", $black);

        $image_map .= "<area shape=rect coords=\"".($marker-5).",205,".($marker+7).",".(200-length($something)*6)."\" HREF=\"$links{$something}\" ALT=\"$something\">";
        if (@{$theHash{$something}}[8] == 1) {
            $gd->stringUp(gdTinyFont, $marker-1, 206, "*", $black);
            aiTextVert(       "null", $aimarker-1,206, "*", $aiblack);
        }
        $gd->string(gdSmallFont, 90, 200, 'Ma', $black);
        $gd->string(gdTinyFont, $fig_width - 70,$fig_lenth - 10, "J. Madin 2004", $black);
        aiTextVert(        "null", $aimarker+7, 200, "$something", $aiblack);      #AI
        $marker = $marker + 16;
        $aimarker = $aimarker + 16;
    }
    
    my $center = ((($marker - 11) - $Smarker) / 2) + $Smarker;
    
    if ($conftype eq "Solow (1996)" && $lastconfidencelengthlong != -999) {
        $color = $black;
        for (my $counter = $Smarker; $counter <= ($marker - 11); $counter=$counter + 2) {
            $gd->line($counter,230 + ($Smin - $upperval) * $millionyr,$counter,230 + ($Smin - $upperval) * $millionyr, $black);
        }
#        my $temptemp = (230 + ($Smax - $upperval) * $millionyr);
#        print "barup: $temptemp <BR>";
#        my $temptemp = (230 + ($Smin - $upperval) * $millionyr);
#        print "barup: $temptemp <BR>";       

#        print "dotmarkerlast: $dotmarkerlast <BR>";              
        for (my $counter = (230 + ($Smin - $upperval) * $millionyr); $counter <= $dotmarkerlast; $counter=$counter + 2) {
            $gd->line($Smarker, $counter,$Smarker,$counter, $black);
#        print "counter: $counter <BR>";

        }
        
        for (my $counter = (230 + ($Smin - $upperval) * $millionyr); $counter <= $barup; $counter=$counter + 2) {
            $gd->line($marker - 11, $counter,$marker - 11,$counter, $black);
#        print "counter: $counter <BR>";

        }
        my $conftemp = $lastconfidencelengthlong;
#        print "lastconfidencelengthlong: $lastconfidencelengthlong <BR>";
        
        if (($Smin - $lastconfidencelengthlong) < 0) {
            $conftemp = $Smin;
            $gd->line($center -2,230 + 0,$center + 3,230 +0, $black);
            $gd->line($center -1,230 -1,$center + 2,230 -1, $black);
            $gd->line($center -1,230 -2,$center + 2,230 -2, $black);
            $gd->line($center ,230 -3,$center + 1,230 -3, $black);
            $gd->line($center ,230 -4,$center + 1,230 -4, $black);  
        } else {
#        print "conftemp: $conftemp <BR>";
            $gd->line($Smarker,(230 + ($Smin - $conftemp - $upperval) * $millionyr),$marker - 11,(230 + ($Smin - $conftemp - $upperval) * $millionyr), $black); 
        }
        $gd->rectangle($center , (230 + ($Smin - $conftemp - $upperval) * $millionyr), $center + 1,(230 + ($Smin - $upperval) * $millionyr), $black);
        

        
    }
    

    
    if ($conftype eq "Solow (1996)" && $firstconfidencelengthlong != -999) {
        $color = $black;
        for (my $counter = $Smarker; $counter <= ($marker - 11); $counter=$counter + 2) {
            $gd->line($counter,230 + ($Smax - $upperval) * $millionyr,$counter,230 + ($Smax - $upperval) * $millionyr, $black);
        }
        for (my $counter = $dotmarkerfirst; $counter <= (230 + ($Smax - $upperval) * $millionyr); $counter=$counter + 2) {
            $gd->line($Smarker, $counter,$Smarker,$counter, $black);
#        print "counter: $counter <BR>";

        }
        for (my $counter = $bardn; $counter <= (230 + ($Smax - $upperval) * $millionyr); $counter=$counter + 2) {
            $gd->line($marker - 11, $counter,$marker - 11,$counter, $black);
#        print "counter: $counter <BR>";

        }
        $gd->rectangle($center , (230 + ($Smax + $firstconfidencelengthlong - $upperval) * $millionyr), $center + 1,(230 + ($Smax - $upperval) * $millionyr), $black);
        $gd->line($Smarker,(230 + ($Smax + $firstconfidencelengthlong - $upperval) * $millionyr),$marker - 11,(230 + ($Smax + $firstconfidencelengthlong - $upperval) * $millionyr), $black); 

    }

    aiText("null", 90, 200, 'Ma', $aiblack);                                     #AI
#    aiText("null", $fig_width - 50,$fig_lenth - 0, "J. Madin 2004", $aiblack);      #AI
    open AIFOOT,"<./data/AI.footer";
    while (<AIFOOT>) {print AI $_};
    close AIFOOT;
    print IMAGEJ $gd->jpeg;
    print IMAGEP $gd->png;
    close IMAGEJ;
    close IMAGEP;
    $image_map .= "</map>";
# ---------------------------------RESULTS-PAGE----------------------------------------
    print "<DIV CLASS=\"title\">Confidence interval results</DIV>";

    print "<CENTER><A HREF=\"javascript: tipsPopup('/public/tips/confidencetips1.html')\">Help</A></CENTER>";

    
    if ($recent == 1)    {
        print "<center><table><tr><th><font color=\"red\">
            This taxon may not be extinct</font></th></tr></table></CENTER><BR><BR>";
    }
    if ($recent == 2)    {
        print "<center><table><tr><th><font color=\"red\">
            The upper confidence interval extends into the future and is therefore unreliable
            </font></th></tr></table></CENTER><BR><BR>";
    }
    
    print $image_map;
    print "<CENTER><TABLE><TD><IMG SRC=\"/public/confidence/$imagenamepng\"  USEMAP=\"#ConfidenceMap\" ISMAP BORDER=0></TD><TD WIDTH=40></TD>";
  
    print "<TD ALIGN=\"center\">";
    if($conftype eq "Strauss and Sadler (1989)") {
        my (@tableRowHeader, @tableColHeader, @table);
        @tableRowHeader = ('last occurrence (Ma)','first occurrence (Ma)','confidence interval (Ma)', 'number of horizons', 'transposition test');
        for($i=0;$i<scalar(@sortedKeys);$i++){
            push @tableColHeader, $sortedKeys[$i]; #taxon name
            my @confVals = @{$theHash{$sortedKeys[$i]}};
            $table[$i] = [$confVals[3],$confVals[2],$confVals[4],$confVals[9],$confVals[8]];
        }
        my $transpose = (scalar(@sortedKeys) > 5) ? 1 : 0;
        printResultTable('',\@tableRowHeader,\@tableColHeader,\@table,$transpose);
    } elsif($conftype eq "Marshall (1994)") {
        my (@tableRowHeader, @tableColHeader, @table);
        @tableRowHeader = ('last occurrence (Ma)','first occurrence (Ma)','lower confidence interval (Ma)', 'upper confidence interval (Ma)','number of horizons', 'transposition test');
        for($i=0;$i<scalar(@sortedKeys);$i++){
            push @tableColHeader, $sortedKeys[$i]; #taxon name
            my @confVals = @{$theHash{$sortedKeys[$i]}};
            $table[$i] = [$confVals[3],$confVals[2],$confVals[10],$confVals[4],$confVals[9],$confVals[8]];
        }
        my $transpose = (scalar(@sortedKeys) > 5) ? 1 : 0;
        printResultTable('',\@tableRowHeader,\@tableColHeader,\@table,$transpose);
    } elsif($conftype eq "Solow (1996)") {
        my (@tableRowHeader, @tableColHeader, @table);
        @tableRowHeader = ('last occurrence (Ma)','first occurrence (Ma)','number of horizons', 'transposition test');
        for($i=0;$i<scalar(@sortedKeys);$i++){
            push @tableColHeader, $sortedKeys[$i]; #taxon name
            my @confVals = @{$theHash{$sortedKeys[$i]}};
            $table[$i] = [$confVals[3],$confVals[2],$confVals[9],$confVals[8]];
        }
        my $transpose = (scalar(@sortedKeys) > 5) ? 1 : 0;
        printResultTable('table 1',\@tableRowHeader,\@tableColHeader,\@table,$transpose);
        print "<BR><BR>";
        
        my $temp1;
        my $temp2;
        if ($firstconfidencelengthlong == -999) {
            $temp1 = "NA";
        } else {$temp1 = sprintf("%.3f", $firstconfidencelengthlong)}
        
        if ($lastconfidencelengthlong == -999) {
            $temp2 = "NA";
        } else {$temp2 = sprintf("%.3f", $lastconfidencelengthlong)}
      
        # print table 2 
        print "<TABLE CELLSPACING=1 BGCOLOR=\"black\" CELLPADDING=5><TR BGCOLOR=\"white\" ALIGN=\"CENTER\"><TD>table 2</TD><TD WIH=70><B>significance level</B></TD><TD WIH=70><B>confidence limit (Ma)</B></TD></TR>";
        if ($firstsig != -999) {
        print "<TR  BGCOLOR=\"white\" ALIGN=\"CENTER\"><TD WIH=70><B>common first occurrence</B></TD><TD>$firstsig</TD><TD>$temp1</TD></TR>";
        }
        if ($lastsig != -999) {
        print "<TR  BGCOLOR=\"white\" ALIGN=\"CENTER\"><TD WIH=70><B>common last occurrence</B></TD><TD>$lastsig</TD><TD>$temp2</TD></TR>";
        }
        print "</TABLE>";
    }
    if (scalar @not_in_scale) {
        if (scalar(@not_in_scale) > 1) {
            print "<p></p><div style='border: 1px #000000 solid; font-weight: bold; text-align: center;'>Warning: The following taxa were excluded from the chart because they could not be mapped to the time scale specified:<br>";
        } else {
            print "<p></p><div style='border: 1px #000000 solid; font-weight: bold; text-align: center;'>Warning: The following taxon was excluded from the chart because it could not be mapped to the time scale specified:<br>";
        }
        print join(", ",@not_in_scale);
        print "</div>";
    }
    print "</TD></TR>";
        
    print "</TABLE></TD>";

    print "</TABLE></CENTER><BR><BR>";
    
    print "<CENTER><TABLE><TR><TH>Click on a taxon, section level, or gray box to get more info</TH></TR></TABLE></CENTER><BR>";
    print "<CENTER><b>Download figure as: <a href=\"/public/confidence/$imagenamepng\" TARGET=\"xy\">PNG</a>";
    print ", <a href=\"/public/confidence/$imagenamejpg\" TARGET=\"xy\">JPEG</a>";
    print ", <a href=\"/public/confidence/$imagenameai\">AI</a><BR><BR></b>";
    #print "<INPUT TYPE=submit VALUE=\"Start again\"><BR><BR><BR>";
    optionsForm($q, $s, $dbt, \%splist, 'small');
    print " <a href='bridge.pl?action=displayFirstForm'>Start again</a></b><p></center><BR><BR><BR>";

    return;
} #End Subroutine CalculateIntervals

# Used in CalculateTaxaInterval, print HTML table
sub printResultTable { 
    $tableName = $_[0];
    $tableRowHeader = $_[1];
    $tableColHeader = $_[2];
    @table = @{$_[3]};
    $transpose = ($_[4] || 0);

    if ($transpose) {
        $temp = $tableRowHeader;
        $tableRowHeader = $tableColHeader;
        $tableColHeader = $temp;
    }
    @tableRowHeader = @$tableRowHeader;
    @tableColHeader = @$tableColHeader;

    # RESULTS TABLE HEADER
    print "<TABLE CELLSPACING=1 BGCOLOR=\"black\" CELLPADDING=5><TR BGCOLOR=\"white\" ALIGN=\"CENTER\">";
    print "<TD BGCOLOR=\"white\" ALIGN=\"CENTER\"><span style='font-size: 10pt;'><B>$tableName</B></span></TD>";
    foreach $col (@tableColHeader) { 
        print "<TD BGCOLOR=\"white\" ALIGN=\"center\"><span style='font-size: 9pt;'><B><I>$col</I></B></span></TD>";
    }    
    print "</TR>";
    # RESULTS TABLE BODY
    for(my $rowNum=0;$rowNum<scalar(@tableRowHeader);$rowNum++){
        print "<TR><TD BGCOLOR=\"white\" ALIGN=\"center\"><span style='font-size: 9pt;'><B>$tableRowHeader[$rowNum]</B></span></TD>";
        for(my $colNum=0;$colNum<scalar(@tableColHeader);$colNum++){
            if ($transpose) {
                print "<TD BGCOLOR=\"white\" ALIGN=\"center\"><span style='font-size: 9pt;'>".$table[$rowNum][$colNum]."</span></TD>";
            } else {
                print "<TD BGCOLOR=\"white\" ALIGN=\"center\"><span style='font-size: 9pt'>".$table[$colNum][$rowNum]."</span></TD>";
            }
        }
        print "</TR>";
    }
    print "</TABLE>";
}

#--------------CALCULATE STRATIGRAPHIC RELATIVE CONFIDENCE INTERVALS----------------
sub calculateStratInterval	{
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $dbh=$dbt->dbh;
    my $marker = 100;
    my ($AILEFT,$AITOP,$fig_width,$fig_lenth);
    my $aifig_size = 500;
    my $section_name = uri_unescape($q->param("input"));
    my $section_type = ($q->param("input_type") eq 'regional') ? 'regional' : 'local';
    my $alpha = $q->param("alpha");
#   $alpha = 1 - $alpha;
    my $conffor = $q->param("conffor");
    my $conftype = $q->param("conftype");
    my $stratres = $q->param("stratres");
    my $order = $q->param("order");

    my ($taxon_nos_string,$genus_species_sql);
    for(my $i=0;$q->param("speciesname$i");$i++) {
        my @taxon_nos = split(",",$q->param("speciesname$i"));
        foreach $taxon_no_or_name (@taxon_nos) {
            if ($taxon_no_or_name =~ /^\d+$/) {
                $taxon_nos_string .= $taxon_no_or_name . ",";
            } else {
                my ($genus,$species) = split(/ /,$taxon_no_or_name);
                if ($genus) {
                    $genus_species_sql .= " OR (occurrences.genus_name=".$dbh->quote($genus);
                    if ($species) {
                        $genus_species_sql .= "AND occurrences.species_name=".$dbh->quote($species);
                    }
                    $genus_species_sql .= ")";
                }
            }
        }
    }
    $taxon_nos_string =~ s/,$//;
    $genus_species_sql =~ s/^ OR//;

    my $sql = "SELECT ${section_type}bed, ${section_type}order, occurrences.collection_no, genus_name, species_name, taxon_name, taxon_rank FROM collections, occurrences". 
              " LEFT JOIN authorities ON occurrences.taxon_no=authorities.taxon_no".
              " WHERE collections.collection_no=occurrences.collection_no".
              " AND ${section_type}section=".$dbh->quote($section_name).
              " AND ${section_type}bed REGEXP '^[0-9]+\$'";
    if ($taxon_nos_string && $genus_species_sql) {
        # Doing this as a union is much faster since it uses the indexes properly.  Otherwise doesn't know which index to use
        $sql = "($sql AND occurrences.taxon_no IN ($taxon_nos_string)) UNION ($sql AND ($genus_species_sql))";
    } elsif ($taxon_nos_string) {
        $sql = "$sql AND occurrences.taxon_no IN ($taxon_nos_string)";
    } elsif ($genus_species_sql) {
        $sql = "$sql AND ($genus_species_sql)";
    } else {
        return;
    }
    main::dbg("sql to get beds from species list: $sql");
    my @beds_and_colls = @{$dbt->getData($sql)};
    foreach my $row (@beds_and_colls) {
        my $genus_species;
        if ($q->param('taxon_resolution') eq 'genus') {
            $genus_species = $row->{'genus_name'};
        } else {
            $genus_species = join ' ',$row->{'genus_name'},$row->{'species_name'};
        }
        $sectionbed{$row->{'collection_no'}}=$row->{$section_type.'bed'};
        push @{$mainHash{$genus_species}}, $row->{$section_type.'bed'};
        if ($row->{'taxon_name'}) {
            $taxon_rank='Higher taxon';
            $taxon_rank='species' if ($row->{'taxon_rank'} eq 'species');
            $taxon_rank='Genus' if ($row->{'taxon_rank'} eq 'genus');

            $links{$genus_species} = "bridge.pl?action=checkTaxonInfo&taxon_name=$row->{taxon_name}&taxon_rank=$taxon_rank";
        } else {
            if ($q->param('taxon_resolution') eq 'genus' ||
                !$row->{'species_name'}) {
                $taxon_rank = 'Genus';
            } else {
                $taxon_rank = 'Genus and species';
            }
            $links{$genus_species} = "bridge.pl?action=checkTaxonInfo&taxon_name=$row->{taxon_name}&taxon_rank=$taxon_rank";
        }
    }


# ----------------------------GENERAL FIGURE DIMENSIONS---------------------------
    my @tempp = sort numerically values %sectionbed;                        
    main::dbg("sorted sectionbed values:".Dumper(\@tempp));
    main::dbg("mainHash (genus->beds array):".Dumper(\%mainHash));
    my $number_horizons = scalar(@tempp);            # how many horizons for whole section
    my $maxhorizon = $tempp[$number_horizons-1];     # the maximum horizon number, e.g., 17 (+1, for upper bound)
    my $minhorizon = $tempp[0] - 1;                      # thw minimum horizon number, e.g., 3
# -------------------CALCULATE CONFIEDENCE INTS------------------------------
    my %stratHash;
    my $upper_lim = $maxhorizon;
    my $lower_lim = $minhorizon;
    my $upper_horizon_lim = $upper_lim;
    my $lower_horizon_lim = $lower_lim;
# ---------------------------STRAUSS AND SADLER----------------------------------
    foreach my $counter (sort alphabetically keys(%mainHash))  {
        my $conf = 0;
        my @array = sort numerically @{$mainHash{$counter}};
        my $count = scalar(@array);         # number of horizons
    #        print "$counter: $count<BR>";
        my $lower = $array[0] - 1;              # lower species horizon, say 6
        my $upper = $array[$count - 1]; # upper species horizon, say 10 (+ 1, for upper bound of interval)
        my $lenth = $upper - $lower;        # total number of horizons for species, therefore 5;
        my $limit = 0;
        # -----------------------------------------
        if ($conffor eq 'last appearance' || $conffor eq 'first appearance' || $conffor eq 'total duration')  {

            if ($count > 2) {                   # make sure that enough horizons for analysis
                my $iterate;
                if ($conffor eq 'last appearance' || $conffor eq 'first appearance')	{    # one tailed
                    $conf = ((1 - $alpha)**(-1/($count - 1))) - 1;
                } else	{                                                        # two tailed
                    foreach my $scounter (1..30000)	{
                        $conf = $scounter/1000;
                        $iterate = (1 - (2 * ((1 + $conf)**( - ($count - 1)) ) ) + ((1 + 2 * $conf)**( - ($count - 1))));
                        if ($iterate > $alpha)	{
                            last;
                        }
                    }
                }
            }
        }
        # -----------------------------------------
        $limit = ($lenth * $conf)/$lenth;   # length of conf interval as number of horizons
        $upper_horizon_lim = $upper + $limit;
        $lower_horizon_lim = $lower - $limit;
        if ($upper_horizon_lim > $upper_lim)   {$upper_lim = $upper_horizon_lim;}
        if ($lower_horizon_lim < $lower_lim)   {$lower_lim = $lower_horizon_lim;}
        $stratHash{$counter} = [$lower, $upper, $limit, $lenth];  # fill in Hash array with necessary info
    }
# ----------------------------THE MAX AND MIN CONFIDENCE RANGES-------------------

    if ($conffor eq "last appearance" || $conffor eq "total duration")	{
        $upper_lim = (int $upper_lim) + 1;
    } else {
        $upper_lim = $maxhorizon;
    }
    if ($conffor eq "first appearance" || $conffor eq "total duration")	{    
        if ($lower_lim < 0) {
            $lower_lim = (int $lower_lim) - 3;
        } else  {
            $lower_lim = (int $lower_lim) - 2;
        }
    } else  {
        $lower_lim = $minhorizon - 3;
    }    
    my $fig_wide = scalar(keys(%mainHash));
    my $fig_long = $upper_lim - $lower_lim;
    my $horizon_unit = 400/$fig_long;
    my $lateral_unit = 16;
    $fig_width = 120 + ($lateral_unit * $fig_wide);
    $fig_lenth = 250 + 400;#($horizon_unit * $fig_long);
    $AILEFT = 0;
    $AITOP = 580;    
    my $image_map = "<map name='ConfidenceMap'>";
# ------------------------------------GD------------------------
    $image_count = getImageCount();
                                                                                                                                                             
    my $imagenamejpg = "confimage$image_count.jpg";
    my $imagenamepng = "confimage$image_count.png";
    my $imagenameai = "confimage$image_count.ai";

    open(IMAGEJ, ">$IMAGE_DIR/$imagenamejpg");
    open(IMAGEP, ">$IMAGE_DIR/$imagenamepng");
    open(AI,">$IMAGE_DIR/$imagenameai");
    open(AIHEAD,"<./data/AI.header");
    while (<AIHEAD>)	{
        print AI $_;
    }
    close AIHEAD;
    my $gd = GD::Image->new($fig_width,$fig_lenth);   
    my $poly = GD::Polygon->new();    
    my $white  = $gd->colorAllocate(255, 255, 255);
    my $aiwhite = "0.00 0.00 0.00 0.00 K"; 
    my $grey= $gd->colorAllocate(  192,   192,   192);
    my $aigrey = "0.33 0.33 0.33 0.12 k";
    my $black  = $gd->colorAllocate(  0,   0,   0);
    my $aiblack = "0.00 0.00 0.00 1.00 K";
    $gd->rectangle(0,0,$fig_width-1,$fig_lenth - 1,$black);
# ---------------------------------SCALE BAR---------------------
    my $i = 0;
    my $j = 0;
    print AI "u\n";                                                     # AI start the group 
    foreach my $counter (($lower_lim)..($upper_lim+1))  {
        if ($i > $j)    {
            $gd->line(65,($fig_lenth - 20) - $i,70,($fig_lenth - 20) - $i,$black);   #GD
            $gd->string(gdTinyFont,55-length($counter)*5,($fig_lenth - 20) - ($i) - 4,$counter,$black);      #GD
            aiLine(65,($fig_lenth - 20) - $i,70,($fig_lenth - 20) - $i,$aiblack);    #AI
            aiText("null",55-length($counter)*6,(($fig_lenth - 20) - $i) + 2,$counter,$aiblack);       #AI
            if ($counter > $minhorizon && $counter <= $maxhorizon) {
                $image_map .= "<area shape=rect coords=\"".(55-length($counter)*6).",".int($fig_lenth - $i - 15).",55,".int($fig_lenth - $i - 30)."\" HREF=\"bridge.pl?action=displayCollResults&${section_type}section=$section_name&${section_type}bed=$counter\" ALT=\"$section_type bed $counter of $section_name\">";
            }
            $j = $j + 8;
        }
        if ($counter == $maxhorizon || $counter == $minhorizon) {
            $gd->dashedLine(70,($fig_lenth - 20) - $i,$fig_width - 20,($fig_lenth - 20) - $i,$black);   #GD
            aiLineDash(70,($fig_lenth - 20) - $i,$fig_width - 20,($fig_lenth - 20) - $i,$aiblack);      #AI
        }
        $i = $i + $horizon_unit;
    }
    print AI "U\n";                                                     # AI terminate the group 
    $gd->line(70,$fig_lenth - 20 - $horizon_unit,70,$fig_lenth - (($fig_long + 1)*$horizon_unit) - 20,$black);   #GD    
    $gd->stringUp(gdMediumBoldFont, 13,(250 + (($fig_lenth - 220)/2)), "Section: $section_name", $black);
    #$image_map .= "<area shape=rect coords=\"12,".int(260 + ($fig_lenth - 220)/2).",28,".int(260 + ($fig_lenth-380)/2-length($section_name)*7)."\" HREF=\"bridge.pl?action=displayStrataSearch&localsection=$section_name\" ALT=\"section $section_name\">";
    $gd->string(gdTinyFont, $fig_width - 70,$fig_lenth - 10, "J. Madin 2004", $black);
    aiLine(70,$fig_lenth - 20 - $horizon_unit,70,$fig_lenth - (($fig_long + 1)*$horizon_unit) - 20,$aiblack);   #AI    
    aiTextVert("null", 13,(250 + (($fig_lenth - 220)/2)), "Section: $section_name", $aiblack);
    aiText("null", $fig_width - 70,$fig_lenth - 10, "J. Madin 2004", $aiblack);    
# -------------------------------SORT OUTPUT----------------------------
    sub sortHashLast {$stratHash{$a}[0] <=> $stratHash{$b}[0]};
    sub sortHashFirst {$stratHash{$a}[1] <=> $stratHash{$b}[1]};
    sub sortHashLenth {$stratHash{$a}[3] <=> $stratHash{$b}[3]};
            
    my @sortedKeys = keys(%stratHash);
    if ($order eq "first appearance")   {
        @sortedKeys = sort sortHashLast sort sortHashFirst sort alphabetically (@sortedKeys);
    } elsif ($order eq "last appearance") {
        @sortedKeys = sort sortHashFirst sort sortHashLast sort alphabetically (@sortedKeys);
    } elsif ($order eq "name")   {
        @sortedKeys = sort alphabetically (@sortedKeys);
    } else  {
        @sortedKeys = sort sortHashLenth sort sortHashFirst sort alphabetically (@sortedKeys);    
    }
# -------------------------------SPECIES BARS----------------------------
    foreach my $counter (@sortedKeys) {
        # -----------------GREY BOXES IN BAR (PS)--------------------------
        my @sectionbeds = @{$mainHash{$counter}};
        my %seenBeds = ();
        foreach $bed (@sectionbeds) {
            if (!$seenBeds{$bed}) {
                $gd->filledRectangle($marker+1,$fig_lenth-20-(($bed-$lower_lim)*$horizon_unit),$marker+4,$fig_lenth-20-(($bed-1-$lower_lim)*$horizon_unit),$grey);
                aiFilledRectangle($marker,$fig_lenth-20-(($bed-$lower_lim)*$horizon_unit),$marker+5,$fig_lenth-20-(($bed-1-$lower_lim)*$horizon_unit),$aigrey);
                $image_map .= "<area shape=rect coords=\"".$marker.",".int($fig_lenth-20-(($bed-$lower_lim)*$horizon_unit)).",".($marker+5).",".int($fig_lenth-20-(($bed-1-$lower_lim)*$horizon_unit))."\" HREF=\"bridge.pl?action=displayCollResults&${section_type}section=$section_name&${section_type}bed=$bed&genus_name=$counter\" ALT=\"$counter in $section_type bed $bed of section $section_name\">";
            }
            $seenBeds{$bed} = 1;
        }
        $gd->rectangle($marker, ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[0]) * $horizon_unit), $marker + 5, ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[1]) * $horizon_unit) , $black);
        aiRectangle(   $marker, ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[0]) * $horizon_unit), $marker + 5, ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[1]) * $horizon_unit) , $aiblack);
# -----------------CONFIDENCE BARS--------------------------
        if ($conffor eq "first appearance" || $conffor eq "total duration")	{
            $color = $black;
            $gd->rectangle(($marker + 2), ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[0]) * $horizon_unit), ($marker + 3), ($fig_lenth - 20) - ((-$lower_lim + ($stratHash{$counter}[0] - $stratHash{$counter}[2])) * $horizon_unit), $color);
            aiRectangle(($marker + 2), ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[0]) * $horizon_unit), ($marker + 3), ($fig_lenth - 20) - ((-$lower_lim + ($stratHash{$counter}[0] - $stratHash{$counter}[2])) * $horizon_unit), $aicolor);            
        }
        if ($conffor eq "last appearance" || $conffor eq "total duration")   {
            $color = $black;
            $gd->rectangle(($marker + 2), ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[1]) * $horizon_unit), ($marker + 3), ($fig_lenth - 20) - ((-$lower_lim + ($stratHash{$counter}[1] + $stratHash{$counter}[2])) * $horizon_unit), $color);
            aiRectangle((   $marker + 2), ($fig_lenth - 20) - ((-$lower_lim + $stratHash{$counter}[1]) * $horizon_unit), ($marker + 3), ($fig_lenth - 20) - ((-$lower_lim + ($stratHash{$counter}[1] + $stratHash{$counter}[2])) * $horizon_unit), $aicolor);
        }    
        $gd->stringUp(gdSmallFont, $marker-5, 200, "$counter", $black);
        aiTextVert(        "null", $marker+7, 200, "$counter", $aiblack);
#        $image_map .= "<area shape=rect coords=\"".($marker-5).",205,".($marker+7).",".(200-length($counter)*6)."\" HREF=\"bridge.pl?action=checkTaxonInfo&taxon_name=$counter\" ALT=\"$counter\">";
        $image_map .= "<area shape=rect coords=\"".($marker-5).",205,".($marker+7).",".(200-length($counter)*6)."\" HREF=\"$links{$counter}\" ALT=\"$counter\">";

        
        $marker = $marker + $lateral_unit;
    }
    #$gd->stringFT($black,'/usr/X11R6/lib/X11/fonts/TTF/luximb.ttf',12,0,50,50,'Hello World');
# ------------------------------------MAKE FIGURE------------------------------
    open AIFOOT,"<./data/AI.footer";
    while (<AIFOOT>) {print AI $_};
    close AIFOOT;
    print IMAGEJ $gd->jpeg;
    print IMAGEP $gd->png;
    close IMAGEJ;
    close IMAGEP;
    $image_map .= "</map>";
# ---------------------------------RESULTS-PAGE----------------------------------------
    print "<DIV CLASS=\"title\">Confidence interval results for the <i>$section_name</i> stratigraphic section</DIV><BR>";
    #if ($fig_width > 750)  {
    #   print "<CENTER><IMG WIDTH=750 SRC=\"/public/confidence/$imagenamepng\"></CENTER><BR><BR>";
    #} else {
    
    print $image_map;
    print "<CENTER><IMG SRC=\"/public/confidence/$imagenamepng\" USEMAP=\"#ConfidenceMap\" ISMAP BORDER=0></CENTER><BR><BR>";
    #}
    
    print "<CENTER><TABLE><TR><TH>Click on a taxon, section level, or gray box to get more info</TH></TR></TABLE></CENTER><BR>";
    print "<CENTER><b>Download figure as: <a href=\"/public/confidence/$imagenamepng\" TARGET=\"xy\">PNG</a>";
    print ", <a href=\"/public/confidence/$imagenamejpg\" TARGET=\"xy\">JPEG</a>";
    print ", <a href=\"/public/confidence/$imagenameai\">AI</a><BR><BR></b>";

    optionsForm($q, $s, $dbt, \%splist, 'small');
    print " <a href='bridge.pl?action=displaySearchSectionForm'>Do another search</a></b><p></center><BR><BR><BR>";

    return;
} 

sub alphabetically {lc($a) cmp lc($b)};
sub numerically {$a <=> $b};

sub aiLine {
    my $x1=shift;
    my $y1=shift;
    my $x2=shift;
    my $y2=shift;
    my $color=shift;
    print AI "$color\n";
    print AI "[]0 d\n";
    printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
    printf AI "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y2;
    print AI "S\n";
}

sub aiLineDash {
    my $x1=shift;
    my $y1=shift;
    my $x2=shift;
    my $y2=shift;
    my $color=shift;
    print AI "$color\n";
    print AI "[6 ]0 d\n";
    printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
    printf AI "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y2;
    print AI "S\n";
}

sub aiFilledRectangle {
    my $x1=shift;
    my $y1=shift;
    my $x2=shift;
    my $y2=shift;
    my $color=shift;
    print AI "0 O\n";
    print AI "$color\n";
    print AI "0.5 g\n";
    print AI "4 M\n";
    printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
    printf AI "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y1;
    printf AI "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y2;
    printf AI "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y2;
    printf AI "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y1;
    print AI "f\n";
}

sub aiRectangle {
    my $x1=shift;
    my $y1=shift;
    my $x2=shift;
    my $y2=shift;
    my $color=shift;
    print AI "$color\n";
    print AI "[]0 d\n";
    printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
    printf AI "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y1;
    printf AI "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y2;
    printf AI "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y2;
    printf AI "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y1;
    print AI "S\n";
}

sub aiTextVert {
    my $font=shift;
    my $x=shift;
    my $y=shift;
    my $text=shift;
    my $color=shift;
    print AI "0 To\n";
    printf AI "0 1 -1 0 %.1f %.1f 0 Tp\nTP\n",$AILEFT+$x,$AITOP-$y;
    printf AI "0 Tr\n0 O\n%s\n",$color;
    printf AI "/_Courier %.1f Tf\n",12; 
    printf AI "0 Tw\n";
    print AI "($text) Tx 1 0 Tk\n";
    print AI "(\r) Tx 1 0 Tk\nTO\n";
}

sub aiText {
    my $font=shift;
    my $x=shift;
    my $y=shift;
    my $text=shift;
    my $color=shift;
    print AI "0 To\n";
    printf AI "1 0 0 1 %.1f %.1f 0 Tp\nTP\n",$AILEFT+$x,$AITOP-$y;
    printf AI "0 Tr\n0 O\n%s\n",$color;
    printf AI "/_Courier %.1f Tf\n",12; 
    printf AI "0 Tw\n";
    print AI "($text) Tx 1 0 Tk\n";
    print AI "(\r) Tx 1 0 Tk\nTO\n";
}

sub transpositionTest {
    my $gaplist = shift;
    my @gaplist = @$gaplist;
    my $N = shift;
    my $Tmax = ($N * ($N - 1))/2;
    foreach my $counter (1 .. 2) {    
        my $T = 0;
        my $Tequal = 0;
        my @done;
        foreach my $i (0 .. ($N - 2)) {
            my $temp = 1;
            foreach my $j (($i + 1) .. ($N - 1)) {
                if ($gaplist[$j] <  $gaplist[$i]) {
                    $T++;
                }
                my $pmet = 0;
                foreach my $counter (@done) {
                    if ($i == $counter) {
                        $pmet = 1;
                    }
                }
                if (($gaplist[$i] == $gaplist[$j]) && ($pmet == 0)) {
                    $temp++;
                }
            }    
            push @done, $gaplist[$i];
            $Tequal = $Tequal + ($temp*($temp - 1))/4;
        }
        $Total = $T + $Tequal;
        if($Total > ($Tmax/2)) {
            @gaplist = reverse(@gaplist);
        } else {
            last;
        }
    }
    $correlation = 0;
    #-----------------p(0.95) ONLY------------------
    if ($Total <= ((0.215*($N**2))-(1.15*$N)+0.375)) {
        print "*Significant correlation at alpha = 0.95  <BR>";
        $correlation = 1;
    } else {
#        print "No correlataion  <BR>";
    }
    return $correlation;
}

sub factorial {
    if ($_[0] == 0) {
        1;
    }
    else {
        $_[0] * factorial($_[0] - 1);
    }
}

sub straussSadler {
    my $count = shift;
    my $C = shift;
    my $conffor = shift;
    my $intervallength = shift;
    
    my $alpha = 0;      # calculate intervals Strauss and Saddler (1989)
    my $iterate;
    if ($count > 2) {
        if ($conffor eq 'last appearance' || $conffor eq 'first appearance')	{
            $alpha = ((1 - $C)**(-1/($count - 1))) - 1;
        } else	{
            foreach my $counter (1..3000)	{
                $alpha = $counter/100;
                $iterate = (1 - (2 * ((1 + $alpha)**( - ($count - 1)) ) ) + ((1 + 2 * $alpha)**( - ($count - 1))));
                if ($iterate > $C)	{
                    last;
                }
            }			
        }
    }
    $firstconfidencelengthlong = $intervallength * $alpha;
    $lastconfidencelengthlong = $firstconfidencelengthlong;
    $confidencelengthshort = 0;
    $lastconfidencelengthshort = $firstconfidencelengthshort;
    
    return $firstconfidencelengthlong, $lastconfidencelengthlong, $firstconfidencelengthshort, $lastconfidencelengthshort;          
}

sub distributionFree {          #FOR MARSHALL 1994
    my $gaplist = shift;
    my @gaplist = @$gaplist;
    my $N = shift;
    my $C = shift;
    @gaplist = sort numerically @gaplist;
            
    my $alpha = 0.95;       #STANDARD FOR CONFIDENCE PROBABILITIES OF CONFIDENCE INTERVALS ($C)
    my $gamma = (1 - $alpha)/2;
    my $ll = 0;
    my $uu = 0;
#        print "N: $N<BR>";
    if ($N > 5) {
        foreach my $x (1 .. $N) {
            my $sum = 0;
            foreach my $i (1 .. $x) {
                $Nx = factorial($N) / (factorial($i) * factorial($N - ($i)));
                $sum = $sum + ($Nx * ($C**$i)) * ((1 - $C)**($N - $i));
            }
            if (($sum > $gamma) && ($ll == 0)) {
                $ll = 1;
                $low = $x;
            }
            if (($sum > (1 - $gamma)) && ($uu == 0)) {
                $uu = 1;
                $upp = $x + 1;
            }
        }
#        print "upp: $upp<BR>";
#        print "low: $low<BR>";
        
        if ($upp > $N) {
            $firstconfidencelengthlong = 0;
            $firstconfidencelengthshort = $gaplist[$low - 1];        
        } else {
            $firstconfidencelengthlong = $gaplist[$upp - 1];
            $firstconfidencelengthshort = $gaplist[$low - 1];        
        }
        
    } else {
        $firstconfidencelengthlong = 0;
        $firstconfidencelengthshort = 0;            
    }
    $lastconfidencelengthlong = $firstconfidencelengthlong;
    $lastconfidencelengthshort = $firstconfidencelengthshort;

    return $firstconfidencelengthlong, $lastconfidencelengthlong, $firstconfidencelengthshort, $lastconfidencelengthshort;           
}

sub commonEndPoint {        #FOR SOLOW (1996)
    my $solowHash = shift;
    my %solowHash = %$solowHash;
    my $alpha = shift;
    my $conffor = shift;
    $alpha = 1 - $alpha;
#    my $lastsig;
#    my $firstsig;
#    my $lastconfidencelengthlong;
#    my $firstconfidencelengthlong;
    
    if ($conffor eq 'total duration' || $conffor eq 'first appearance')	{
        my @mx;
        foreach my $keycounter (keys(%solowHash)) {
            push @mx, $solowHash{$keycounter}[2];
        }  #there must be an easier may to find the maximum horizon!!
        @mx = sort numerically @mx;
        my $max = $mx[scalar(@mx) - 1];

        #-------------------------SIGNIFICANCE FINDER-----------------------------

        my $df = 2 * (length(%solowHash) - 1);
#        print "df: $df<BR>";
#        print "alpha: $alpha<BR>";
    
        my $lambda = 1;
        foreach my $i (keys(%solowHash)) {
            $lambda = $lambda * ((($solowHash{$i}[2] - $solowHash{$i}[1]) / ($max - $solowHash{$i}[1])) ** ($solowHash{$i}[0] - 1));
        }
#        print "lambda: $lambda<BR>";

        $firstsig = chiSquaredDensity($df,0,-2*log($lambda));

#        print "Significance: $lastsig<BR>";
        #----------------------UPPER CONFIDENCE FINDER-----------------------------
        
        if ($firstsig > 0.05) {
            my $df = 2 * scalar(%solowHash);
            my $tester = chiSquaredDensity($df,$alpha,0);
#            print "tester: $tester<BR>";
            my $j;
            for ($j = $max; $j <= $max + 1000; $j = $j + 0.1) {
                my $Rstore = 1;
                foreach my $i (keys(%solowHash)) {
                    $Rstore = $Rstore * ((($solowHash{$i}[2] - $solowHash{$i}[1])/($j - $solowHash{$i}[1]))**($solowHash{$i}[0] - 1));
                }
                if ((-2 * log($Rstore)) > $tester) {last}
            }
            $firstconfidencelengthlong = sprintf("%.3f", $j - $max);
        } else {$firstconfidencelengthlong = -999}
         
    } else {
        $firstsig = -999;
        $firstconfidencelengthlong = -999;
    }
#    print "first SIG: $firstsig, first CONF: $firstconfidencelengthlong<BR>";

    my %tempsolowHash = %solowHash;
    
    if ($conffor eq 'total duration' || $conffor eq 'last appearance')	{
        foreach my $keycounter (keys(%tempsolowHash)) {
            my $temp = $tempsolowHash{$keycounter}[1];
            $tempsolowHash{$keycounter}[1] = -$tempsolowHash{$keycounter}[2];
            $tempsolowHash{$keycounter}[2] = -$temp;
        }

        my @mx;
        foreach my $keycounter (keys(%tempsolowHash)) {
            push @mx, $tempsolowHash{$keycounter}[2];
        }  #there must be an easier may to find the maximum horizon!!
        @mx = sort numerically @mx;
        my $max = $mx[scalar(@mx) - 1];

        #-------------------------SIGNIFICANCE FINDER-----------------------------

        my $df = 2 * (length(%tempsolowHash) - 1);
#        print "df: $df<BR>";
#        print "alpha: $alpha<BR>";
    
        my $lambda = 1;
        foreach my $i (keys(%tempsolowHash)) {
            $lambda = $lambda * ((($tempsolowHash{$i}[2] - $tempsolowHash{$i}[1]) / ($max - $tempsolowHash{$i}[1])) ** ($tempsolowHash{$i}[0] - 1));
        }
#        print "lambda: $lambda<BR>";

        $lastsig = chiSquaredDensity($df,0,-2*log($lambda));

#        print "Significance: $firstsig<BR>";
        #----------------------LOWER CONFIDENCE FINDER-----------------------------
        
        if ($lastsig > 0.05) {
            my $df = 2 * scalar(%tempsolowHash);
            my $tester = chiSquaredDensity($df,$alpha,0);
            my $j;
            for ($j = $max; $j <= $max + 1000; $j = $j + 0.1) {
                my $Rstore = 1;
                foreach my $i (keys(%tempsolowHash)) {
                    $Rstore = $Rstore * ((($tempsolowHash{$i}[2] - $tempsolowHash{$i}[1])/($j - $tempsolowHash{$i}[1]))**($tempsolowHash{$i}[0] - 1));
                }
                if ((-2 * log($Rstore)) > $tester) {last}
            }
            $lastconfidencelengthlong = sprintf("%.3f", $j - $max);
        } else {$lastconfidencelengthlong = -999}
         
    } else {
        $lastsig = -999;
        $lastconfidencelengthlong = -999;
    }
#    print "last SIG: $lastsig, last CONF: $lastconfidencelengthlong<BR>";
    return $firstsig, $firstconfidencelengthlong, $lastsig, $lastconfidencelengthlong;
}

sub chiSquaredDensity { #for calculating both types, either alpha or lower must be zero to work
    my $df = shift;
    my $alpha = shift;      #ONE TAILED
    my $lower = shift;
    
    my $resolution = 0.01;      #WILL GET ACCURACY TO APRROX 3 DECIMAL PLACES
    my $upper = 1000;           #MY SURROGATE FOR INFINITE!!
    my $chi = 0;
    my $x;
    
    if ($lower == 0) {
        for ($x = $upper; $x >= 0; $x = $x - $resolution) {
            $chi = $chi + $resolution * ((($x**(($df/2) - 1)) * exp(-$x/2)) / (gamma($df/2) * (2 ** ($df/2))));
            if($chi >= $alpha) {last}
        }
        return sprintf("%.3f", $x);
    } elsif ($alpha == 0 ) {
        for ($x = $upper; $x >= $lower; $x = $x - $resolution) {
            $chi = $chi + $resolution * ((($x**(($df/2) - 1)) * exp(-$x/2)) / (gamma($df/2) * (2 ** ($df/2))));
        }
        return sprintf("%.3f", $chi);    
    }
}

sub gamma {             #THE GAMMA FUNCTION
    my $x = shift;
    my ($y1, $res, $z, $i);
    my $sqrtpi = 0.9189385332046727417803297e0;     # log(sqrt(2*pi))
    my $pi     = 3.1415926535897932384626434e0;
    my $xbig   = 171.624e0;
    my $xminin = 2.23e-308;
    my $eps    = 2.22e-16;
    my $xinf   = 1.79e308;
    
#---Numerator and denominator coefficients for rational minimax approximation over (1,2).
    my @P = (-1.71618513886549492533811e+0, 2.47656508055759199108314e+1, -3.79804256470945635097577e+2, 6.29331155312818442661052e+2, 8.66966202790413211295064e+2, -3.14512729688483675254357e+4, -3.61444134186911729807069e+4, 6.64561438202405440627855e+4);
    my @Q = (-3.08402300119738975254353e+1, 3.15350626979604161529144e+2, -1.01515636749021914166146e+3, -3.10777167157231109440444e+3, 2.25381184209801510330112e+4, 4.75584627752788110767815e+3, -1.34659959864969306392456e+5, -1.15132259675553483497211e+5);
#------------Coefficients for minimax approximation over (12, INF).
    my @C = (-1.910444077728e-03, 8.4171387781295e-04, -5.952379913043012e-04, 7.93650793500350248e-04, -2.777777777777681622553e-03, 8.333333333333333331554247e-02, 5.7083835261e-03);

    my $parity = 0;  
    my $fact = 1;
    my $n = 0;
    my $y = $x;
    if ($y <= 0) {
        $y = -$x;
        $y1 = int($y);
        $res = $y - $y1;
        if ($res != 0) {
            if ($y1 != int($y1*0.5)*2) { $parity = 1 }
            $fact = -$pi / sin($pi*$res);
            $y = $y + 1;
        } else {
            $res = $xinf;
            return $res;
        }
    }
    if ($y < $eps) {
        if ($y >= $xminin) {
            $res = 1 / $y;
        } else {
            $res = $xinf;
            return $res;
        }
    } elsif ($y < 12) {
        $y1 = $y;
        if ($y < 1) {
            $z = $y;
            $y = $y + 1;
        } else {
            $n = int($y) - 1;
            $y = $y - $n;
            $z = $y - 1;
        }
        my $xnum = 0;
        my $xden = 1;
        foreach my $i (0 .. 7) {
            $xnum = ($xnum + $P[$i]) * $z;
            $xden = $xden * $z + $Q[$i];
        }
        $res = $xnum / $xden + 1;
        if ($y1 < $y) {
            $res = $res / $y1;
        } elsif ($y1 > $y) {
            foreach $i ( 1 .. $n ) {
                $res = $res * $y;
                $y = $y + 1;
            }
        }
    } else {
        if ($y <= $xbig) {
            my $ysq = $y * $y;
            my $sum = $C[6];
            foreach my $i (0 .. 5) {
                $sum = $sum / $ysq + $C[$i];
            }
            $sum = $sum/$y - $y + $sqrtpi;
            $sum = $sum + ($y-0.5)*log($y);
            $res = exp($sum);
        } else {
            $res = $xinf;
            return $res;
        }
    }
    if ($parity) { $res = -$res }
    if ($fact != 1) { $res = $fact / $res }
    return $res;
}

# Cleans out files created more than 1 day ago, increments the image counter and passes back the number
# corresponding to a new image 
sub getImageCount {
    # erase all files that haven't been accessed in more than a day
    opendir(DIR,"$IMAGE_DIR") or die "couldn't open $IMAGE_DIR ($!)";
    # grab only files with extensions;  not subdirs or . or ..
    my @filenames = grep { /.*?\.(\w+)/ } readdir(DIR);
    closedir(DIR);
                                                                                                                                                             
    foreach my $file (@filenames){
        if((-M "$IMAGE_DIR/$file") > 1){
            unlink "$IMAGE_DIR/$file";
        }
    }
                                                                                                                                                             
    # get the next number for file creation.
    if ( ! open IMAGECOUNT,"<$IMAGE_DIR/imagecount" ) {
        $self->htmlError ( "Couldn't open [$IMAGE_DIR/imagecount]: $!" );
    }
    $image_count = <IMAGECOUNT>;
    chomp($imagecount);
    close IMAGECOUNT;
                                                                                                                                                             
    $image_count++;
    if ( ! open IMAGECOUNT,">$IMAGE_DIR/imagecount" ) {
          $self->htmlError ( "Couldn't open [$IMAGE_DIR/imagecount]: $!" );
    }
    print IMAGECOUNT "$image_count";
    close IMAGECOUNT;
                                                                                                                                                             
    $image_count++;
    return $image_count;
}

1;
