package Confidence;

use strict;
use Data::Dumper; 
use TaxaCache;
use Classification;
use Collection;
use Person;
use TaxonInfo;
use HTMLBuilder;
use PBDBUtil;
use URI::Escape;
use Memoize;
use Reference;
use Debug qw(dbg);
use Constants qw($READ_URL);

memoize('chiSquaredDensity');
memoize('factorial');
memoize('gamma');

# written 03.31.04 by Josh Madin as final product
# Still doesn't allow choice of time scale when examining the conf ints of taxa

# Deals with homonyms that may exist by presenting an option to select either one
# and passing on the taxon_no.  All non-homonym taxa get passed as hidden fields
# PS 02/08/2004
sub displayHomonymForm {
    my ($q,$s,$dbt,$homonym_names,$occ_list) = @_;
    my $dbh=$dbt->dbh;

    my %occ_list = %$occ_list;
    my @homonym_names = @$homonym_names;

    my $pl1 = scalar(@homonym_names) > 1 ? "s" : "";
    my $pl2 = scalar(@homonym_names) > 1 ? "" : "s";
    print "<center><h3>The following taxonomic name$pl1 belong$pl2 to multiple taxonomic <br>hierarchies.  Please choose the one$pl1 you want.</h3>";
    print "<form action=\"$READ_URL\" method=\"post\"><input type=\"hidden\" name=\"action\" value=\"buildListForm\">";
    print "<input type=\"hidden\" name=\"taxon_resolution\" value=\"".$q->param('taxon_resolution')."\">\n";
    print "<input type=\"hidden\" name=\"input_type\" value=\"taxon\">\n";
                       

    my $i=0;
    foreach my $homonym_name (@homonym_names) {
        my @taxon_nos = TaxonInfo::getTaxonNos($dbt,$homonym_name, undef, 1);

        # Find the parent taxon and use that to clarify the choice

        print '<table border=0 cellspacing=3 cellpadding=3>'."\n";
        print "<tr>";
        foreach my $taxon_no (@taxon_nos) {
            my $parent = TaxaCache::getParent($dbt,$taxon_no);
            print "<td><input type='radio' checked name='occurrence_list_$i' value='$taxon_no'>$homonym_name [$parent->{taxon_name}]</td>";
        }
        print "<input type='hidden' name='taxon_name_$i' value='$homonym_name'>\n";
        print "</tr>";
        print "</table>";
        $i++;
    }
    while(my ($taxon,$occurrence_list) = each %occ_list) {
        print "<input type='hidden' name='occurrence_list_$i' value='$occurrence_list'>";
        print "<input type='hidden' name='taxon_name_$i' value='$taxon'>\n";
        $i++;
    }
    print "<br><input type='submit' name='submit' value='Submit'></center></form><br><br>";
}

# Displays a search page modeled after the collections search to search for local/regional sections
# PS 02/04/2005
sub displaySearchSectionForm {
    my ($q,$s,$dbt,$hbo) = @_;

    my %vars; 
    $vars{'enterer_me'} = $s->get('enterer_reversed');
    $vars{'page_title'} = "Section search form";
    $vars{'action'} = "displaySearchSectionResults";
    $vars{'submit'} = "Search sections";
    print PBDBUtil::printIntervalsJava($dbt,1);
    print Person::makeAuthEntJavascript($dbt);
    print $hbo->populateHTML('search_collections_form',\%vars) ;
}

# Handles processing of the output from displaySectionSearchForm similar to displayCollResults
# Goes to next step if 1 result returned, else displays a list of matches
sub displaySearchSectionResults{
    my ($q,$s,$dbt,$hbo) = @_;

    my $limit = $q->param('limit') || 30;
    $limit = $limit*2; # two columns
    my $rowOffset = $q->param('rowOffset') || 0;

    # Build the SQL

    my $fields = ['max_interval_no','min_interval_no','state','country','localbed','localsection','localbedunit','regionalbed','regionalsection','regionalbedunit'];
    my %options = $q->Vars();
    $options{'permission_type'} = 'read';
    $options{'limit'} = 10000000;
    $options{'calling_script'} = 'Confidence';
#    $options{'lithologies'} = $options{'lithology1'}; delete $options{'lithology1'};
#    $options{'lithadjs'} = $options{'lithadj'}; delete $options{'lithadj'}; 
    my ($dataRows) = Collection::getCollections($dbt,$s,\%options,$fields);
    my @dataRows = sort {$a->{regionalsection} cmp $b->{regionalsection} ||
                      $a->{localsection} cmp $b->{localsection}} @$dataRows;

    # get the enterer's preferences (needed to determine the number
    # of displayed blanks) JA 1.8.02

    my $t = new TimeLookup($dbt);
    my @period_order = $t->getScaleOrder($dbt,'69');
    # Convert max_interval_no to a period like 'Quaternary'
    my $int2period = $t->getScaleMapping('69','names');

    my $lastsection = '';
    my $lastregion  = '';
    my $found_localbed = 0;
    my $found_regionalbed = 0;
    my (%period_list,%country_list);
    my @tableRows = ();
    my $rowCount = scalar(@dataRows);
    my $row;
    my $taxon_resolution = $q->param('taxon_resolution') || 'species';
    my $show_taxon_list = $q->param('show_taxon_list') || 'NO';

    # Only used below, in a couple places.  Print contents of a table row
    sub formatSectionLine {
        my ($lastsection,$lastregion,$found_regionalbed,$found_localbed,$period_list,$country_list,$period_order,$taxon_resolution,$show_taxon_list) = @_;
        my ($time_str, $place_str);
        foreach my $period (@$period_order) {
            $time_str .= ", ".$period if ($period_list->{$period});
        }
        foreach my $country (keys %$country_list) {
            $place_str .= ", ".$country; 
        }
        $time_str =~ s/^,//;
        $place_str =~ s/^,//;
        my $link = '';
        if ($lastregion && $found_regionalbed) {
            $link .= "<a href='$READ_URL?action=displayStratTaxaForm&amp;taxon_resolution=$taxon_resolution&amp;show_taxon_list=$show_taxon_list&amp;input=".uri_escape($lastregion)."&amp;input_type=regional'>$lastregion</a>";
            if ($lastsection) { $link .= " / "};
        }    
        if ($lastsection && $found_localbed) {
            $link .= "<a href='$READ_URL?action=displayStratTaxaForm&amp;taxon_resolution=$taxon_resolution&amp;show_taxon_list=$show_taxon_list&amp;input=".uri_escape($lastsection)."&amp;input_type=local'>$lastsection</a>";
        }    
            
        $link .= "<span class='tiny'> - $time_str - $place_str</span>";
        return $link;
    }

    # We need to group the collections here in the code rather than SQL so that
    # we can get a list of max_interval_nos.  There should generaly be only 1 country.
    # Assumes be do an order by localsection in the SQL and there are no null or empty localsections
    if ($rowCount > 0) {
        for(my $i=0;$i<$rowCount;$i++) {
            $row = $dataRows[$i];
            if ($i != 0 && (($row->{'localsection'} ne $lastsection) || ($row->{'regionalsection'} ne $lastregion))) {
                push @tableRows, formatSectionLine($lastsection,$lastregion,$found_regionalbed,$found_localbed,\%period_list,\%country_list,\@period_order,$taxon_resolution,$show_taxon_list);
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
            $period_list{$int2period->{$row->{'max_interval_no'}}} = 1;
            $country_list{$row->{'country'}} = 1;
        }
        push @tableRows, formatSectionLine($lastsection,$lastregion,$found_regionalbed,$found_localbed,\%period_list,\%country_list,\@period_order,$taxon_resolution,$show_taxon_list);
    }

    my $ofRows = scalar(@tableRows);
    if ($ofRows > 1) {       
        # Display header link that says which collections we're currently viewing
        print "<center>";
        print "<h3>Your search produced $ofRows matches</h3>\n";
        if ($ofRows > $limit) {
            print "<h4>Here are";
            if ($rowOffset > 0) {
                print " rows ".($rowOffset+1)." to ";
                my $printRows = ($ofRows < $rowOffset + $limit) ? $ofRows : $rowOffset + $limit;
                print $printRows;
                print "</h4>\n";
            } else {
                print " the first ";
                my $printRows = ($ofRows < $rowOffset + $limit) ? $ofRows : $rowOffset + $limit;
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
        for(my $i=$rowOffset;$i<$ofRows && $i < $rowOffset+$limit/2;$i++) {
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
        my ($section,$section_type);
        if (!$lastsection || $q->param('section_name') eq $lastregion) {
            $section = $lastregion;
            $section_type='regional';
        } else {    
            $section = $lastsection;
            $section_type='local';    
        }
        print "<center>\n<h3>Your search produced exactly one match ($section)</h3></center>";

        my $my_q = new CGI({'show_taxon_list'=>$show_taxon_list,
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
    foreach my $param_key (@params) {
        if ($param_key ne "rowOffset") {
            if ($q2->param($param_key) ne "" || $param_key eq 'section_name') {
                $getString .= "&amp;".uri_escape($param_key)."=".uri_escape($q2->param($param_key));
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
        print "<a href='$READ_URL?$getString'><b>Get $numLeft sections</b></a> - ";
    }
    print "<a href='$READ_URL?action=displaySearchSectionForm'><b>Do another search</b></a>";

    print "</center></p>";
    # End footer links

}

# FIRST-PAGE
sub displayTaxaIntervalsForm {
    my ($q,$s,$dbt,$hbo) = @_;
    # Show the "search collections" form
    my %pref = $s->getPreferences();
    my @prefkeys = keys %pref;
    my $html = $hbo->populateHTML('taxa_intervals_form', [], [], \@prefkeys);
    # Spit out the HTML
    print $html;
}

# REMAKE SPECIES LIST
sub displayTaxaIntervalsResults {
    my ($q,$s,$dbt,$hbo) = @_;
    my $dbh=$dbt->dbh;

    # if homonyms found, display homonym chooser form
    # if no homonyms: 
    #   buildList:
    #     if 'taxon_resolution' (aka analyze species separately) is 'yes'
    #        display a list of taxa to choose (buildList)
    #     else 
    #       display options from
    if ($q->param('input')) {
        my @taxa = split(/\s*[, \t\n-:;]{1}\s*/,$q->param('input'));

        my %occ_list;
        my @homonyms;
        
        foreach my $taxon (@taxa) {
            my @taxon_nos = TaxonInfo::getTaxonNos($dbt,$taxon, undef, 1);
            if (scalar(@taxon_nos) > 1) {
                push @homonyms, $taxon;
            } elsif (scalar(@taxon_nos) == 1) {
                $occ_list{$taxon} = $taxon_nos[0];
            } else {
                $occ_list{$taxon} = $taxon;
            }
        }

        if (scalar(@homonyms) > 0) {
            displayHomonymForm($q,$s,$dbt,\@homonyms,\%occ_list);
        } else {
            buildList($q,$s,$dbt,$hbo,\%occ_list);
        }
    } else {
        displayTaxaIntervalsForm($q,$s,$dbt,$hbo);
    }
}
#--------------------------TAXON LIST BUILDER------------------------------------

sub buildList    {
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $hbo=shift;
    my $dbh=$dbt->dbh;
    my $occ_list_base=shift;
    my %occ_list_base=%$occ_list_base;
    my %occ_list;

    # Set from homonym form
    if (!%occ_list_base) {
        for (my $i=0;$q->param("occurrence_list_$i");$i++)  {
            $occ_list_base{$q->param("taxon_name_$i")} = $q->param("occurrence_list_$i");
        }    
    }

    # Use taxonomic search to build up a list of taxon_nos that are 
    # children of the potentially higher order taxonomic names entered in by the user
    # occ_list_base is the list of higher order names that haven't been
    my $fields = 'o.occurrence_no,o.taxon_no,o.genus_name,o.subgenus_name,o.species_name';
    while(my ($taxon_name,$no_or_name) = each %occ_list_base) {
        if ($no_or_name =~ /^\d+$/) {
            if ($q->param('taxon_resolution') eq 'as_is') {
                my @taxon_nos = TaxaCache::getChildren($dbt,$no_or_name);
                my $taxon_list = join(",",@taxon_nos);
                my $results = getOccurrenceData($dbt,'taxon_list'=>$taxon_list,'fields'=>$fields);
                if (ref $results && @$results) {
                    $occ_list{$taxon_name} = join(",",map {$_->{'occurrence_no'}} @$results);
                }
            } else {
                my @taxon_nos = TaxaCache::getChildren($dbt,$no_or_name);
                my $taxon_list = join(",",@taxon_nos);
                my $results = getOccurrenceData($dbt,'taxon_list'=>$taxon_list,'fields'=>$fields);

                foreach my $row (@$results) {
                    my $taxon_no = $row->{'taxon_no'};
                    my $best_name_ref = TaxaCache::getSeniorSynonym($dbt,$taxon_no);
                    my $genus_ref;
                    my $subgenus_ref;
                    if ($best_name_ref->{'taxon_rank'} =~ /genus|species/) {
                        if ($best_name_ref->{'taxon_rank'} =~ /species/) {
                            $subgenus_ref = TaxaCache::getParent($dbt,$taxon_no,'subgenus');
                            if (!$subgenus_ref) {
                                $genus_ref= TaxaCache::getParent($dbt,$taxon_no,'genus');
                            }
                        }
                        my ($genus,$subgenus,$species);
                        my ($best_genus,$best_subgenus,$best_species) = Taxon::splitTaxon($best_name_ref->{'taxon_name'});
                        if ($subgenus_ref) {
                            ($genus,$subgenus) = Taxon::splitTaxon($subgenus_ref->{'taxon_name'});
                        } elsif ($genus_ref) {
                            ($genus) = $genus_ref->{'taxon_name'};
                        } else {
                            $genus = $best_genus;
                        }
                        if ($best_species) {
                            $species = $best_species;
                        } else {
                            $species = $row->{'species_name'};
                        }
                        
                        my $name = $genus;
                        if ($q->param('taxon_resolution') ne 'genus') {
                            if ($subgenus) {
                                $name .= " ($subgenus)";
                            }
                            $name .= " ".$species;
                        }
                        $occ_list{$name} .= ",".$row->{'occurrence_no'};
                        $occ_list{$name} =~ s/^,//;
                    }
                }
            }
        } else {
            my @results = getOccurrenceData($dbt,'taxon_name'=>$taxon_name,'fields'=>$fields);
            if (@results) {
                if ($q->param('taxon_resolution') eq 'species') {
                    foreach my $row (@results) {
                        my $name = $row->{'genus_name'};
                        if ($row->{'subgenus_name'}) {
                            $name .= " ($row->{'subgenus_name'})";
                        }
                        $name .= " $row->{species_name}";
                        $occ_list{$name} = join(",",map {$_->{'occurrence_no'}} @results);
                    }
                } else {
                    $occ_list{$taxon_name} = $taxon_name;
                }
            }
        }
    }
    foreach (values %occ_list) {
        s/^,// 
    }

    # Now print out the list generated above so the user can select potential species to exclude
    # if they selected 'analyze taxa separately'. Otherwise skip to the options form
    if (!scalar keys %occ_list) {
        print "<center><h3><div class='warning'>Sorry, no occurrences of the taxa entered were found in the database.</div></h3></center>";
        displayTaxaIntervalsForm($q,$s,$dbt,$hbo);
    } else {
        if ($q->param('taxon_resolution') =~/genus|species/) {
            print "<div align=\"center\"><h2>Confidence interval taxon list</h2></div><br>";
            print "<form action=\"$READ_URL\" method=\"post\"><input type=\"hidden\" name=\"action\" value=\"showOptionsForm\">";
            print "<input type=\"hidden\" name=\"input_type\" value=\"taxon\">";
            print "<center>";

            # Print out a list of taxa 3 columns wide
            print "<table cellpadding=5 border=0>";
            my @sortList = sort {$a cmp $b} keys(%occ_list);
            my $columns = int(scalar(@sortList)/3)+1;
            for(my $i=0;$i<$columns;$i++) {
                print "<TR>";
                for(my $j=$i;$j<scalar(@sortList);$j=$j+$columns) {
                    $occ_list{$sortList[$j]} =~ s/,$//; 
                    print "<TD><INPUT TYPE=checkbox NAME=taxon_name_$j VALUE='$sortList[$j]' CHECKED=checked>" . 
                          "<i>".$sortList[$j] . "</i><INPUT TYPE=hidden NAME=\"occurrence_list_$j\" VALUE=\"$occ_list{$sortList[$j]}\"></TD>\n";
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
            optionsForm($q, $s, $dbt, \%occ_list);
        }
    }
}
#--------------DISPLAYS TAXA IN STRATIGRAPHIC SECTION FOR EDITING BY USER-------

sub displayStratTaxa {
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $dbh=$dbt->dbh;
    my %occ_list;
    my $section_name = $q->param("input");
    my $section_type = ($q->param("input_type") eq 'regional') ? 'regional' : 'local';

    # This will get all the genera in a particular regional/localsection, automatically
    # getting the most recent reids of a an occurrence as well.
    my $taxa_list = getOccurrenceData($dbt,
        section_type=>$section_type,
        section_name=>$section_name,
        fields=>"o.occurrence_no,o.genus_name, o.species_name, o.taxon_no"
    );
    my %taxonList;
    # We build a comma separated list of taxon_nos to pass in. If the taxon_resolution is species,
    # the list will always have one taxon_no in it, if its genus, it may have more. If theres no
    # taxon_no, use the genus+species name
    foreach my $row (@$taxa_list) {
        if ($row->{'taxon_no'}) {
            my $best_name_ref = TaxaCache::getSeniorSynonym($dbt,$row->{'taxon_no'});
            my $genus_ref;
            my $subgenus_ref;
            if ($best_name_ref->{'taxon_rank'} =~ /genus|species/) {
                if ($best_name_ref->{'taxon_rank'} =~ /species/) {
                    $subgenus_ref = TaxaCache::getParent($dbt,$row->{taxon_no},'subgenus');
                    if (!$subgenus_ref) {
                        $genus_ref= TaxaCache::getParent($dbt,$row->{taxon_no},'genus');
                    }
                }
                my ($genus,$subgenus,$species);
                my ($best_genus,$best_subgenus,$best_species) = Taxon::splitTaxon($best_name_ref->{'taxon_name'});
                if ($subgenus_ref) {
                    ($genus,$subgenus) = Taxon::splitTaxon($subgenus_ref->{'taxon_name'});
                } elsif ($genus_ref) {
                    ($genus) = $genus_ref->{'taxon_name'};
                } else {
                    $genus = $best_genus;
                }
                if ($best_species) {
                    $species = $best_species;
                } else {
                    $species = $row->{'species_name'};
                }
                
                my $name = $genus;
                if ($q->param('taxon_resolution') ne 'genus') {
                    if ($subgenus) {
                        $name .= " ($subgenus)";
                    }
                    $name .= " ".$species;
                }
                $occ_list{$name} .= ",".$row->{'occurrence_no'};
                $occ_list{$name} =~ s/^,//;
            }
        } else {
            my $name = $row->{'genus_name'};
            if ($q->param('taxon_resolution') eq 'species') {
                if ($row->{'subgenus_name'}) {
                    $name .= " ($row->{'subgenus_name'})";
                }
                $name .= " $row->{species_name}";
            }
            $occ_list{$name} .= ",".$row->{'occurrence_no'};
            $occ_list{$name} =~ s/^,//;
        }
    }

    foreach (values %occ_list) {
        s/,$//;
    } 

    if ($q->param('show_taxon_list') eq 'NO') {
        optionsForm($q, $s, $dbt, \%occ_list);
    } else {
        print "<div align=\"center\"><h2>Stratigraphic section taxon list</h2></div><br>";
        print "<form action=\"$READ_URL\" method=\"post\"><input type=\"hidden\" name=\"action\" value=\"showOptionsForm\">";
        print "<center><table cellpadding=5 border=0>";
        print "<tr><td><input type=checkbox checked=checked onClick=\"checkAll(this,'sp_checkbox');\"> Check all</td></tr>";
        print "<input type=\"hidden\" name=\"input\" value=\"".uri_escape($section_name)."\">";
        print "<input type=\"hidden\" name=\"taxon_resolution\" value=\"".$q->param("taxon_resolution")."\">";
        print "<input type=\"hidden\" name=\"input_type\" value=\"".$q->param('input_type')."\">\n";
        my @sortList = sort {$a cmp $b} keys(%occ_list);
        my $columns = int(scalar(@sortList)/3)+1;
        for(my $i=0;$i<$columns;$i++) {
            print "<tr>";
            for(my $j=$i;$j<scalar(@sortList);$j=$j+$columns) {
                $occ_list{$sortList[$j]} =~ s/,$//; 
                print "<td><input type=checkbox name=taxon_name_$j value='$sortList[$j]' checked=checked class=\"sp_checkbox\">" . 
                      "<i>".$sortList[$j] . "</i><input type=hidden name=\"occurrence_list_$j\" value=\"$occ_list{$sortList[$j]}\"></td>\n";
            }
            print "</tr>";
        }
        print "</center></table><br>";
        print "<center><span class=\"tiny\">(To remove taxon from list for analysis, uncheck before pressing 'Submit')</span><br><br>";
        print "<input type=\"submit\" value=\"Submit\">";
        #print "<A HREF=\"/cgi-bin/$READ_URL?action=displayFirstForm\"><INPUT TYPE=\"button\" VALUE=\"Start again\"></A>";
        print "</center><br><br></form>";
    } 
    return;
}
#---------------OPTIONS FORM FOR CALCULATING CONFIDENCE INTERVALS---------------

sub optionsForm    {
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $occ_list=shift;
    my %occ_list;
    %occ_list= %$occ_list if ref ($occ_list);
    # A large form is meant to be displayed on a page by itself, before the chart is drawn
    # A small form is meant to displayed alongside a chart, so must be tall and skinny, and use different styles
    my $form_type = (shift || "large");
    
    my $section_name = uri_unescape($q->param("input"));
    my $type = $q->param("input_type");

# -----------------REMAKE STRAT LIST-----------(REMOVES UNCHECKED)-----------
    if (!%occ_list) {
        my $testspe =0;
        while ($q->param("occurrence_list_$testspe"))  {
            if ($q->param("taxon_name_$testspe"))   {
                $occ_list{$q->param("taxon_name_$testspe")} = $q->param("occurrence_list_$testspe");
            }
            $testspe++;
        }    
    } 
#----------------------BUILD LIST OF SCALES TO CHOOSE FROM--------------------
    my $scale_select;
    if ($type eq 'taxon')   {    
        my $sql = "SELECT r.author1last,r.author2last,r.otherauthors,r.pubyr,s.scale_no,s.scale_name,s.reference_no FROM scales s LEFT JOIN refs r ON s.reference_no=r.reference_no ORDER BY s.scale_name";
        my @results = @{$dbt->getData($sql)};
        my (@keys,@values);
        for my $row (@results) {
            my $author_string = Reference::formatShortRef($row);
            $author_string =~ s/^\s*//;
            push @keys,"$row->{scale_name} [$author_string]";
            push @values,$row->{'scale_no'};
        }
        my $scale_selected = ($q->param('scale') || 73); 
        $scale_select = HTMLBuilder::htmlSelect('scale',\@keys,\@values,$scale_selected);
    }

#------------------------------OPTIONS FORM----------------------------------

    if ($type eq 'taxon')   {
        print "<FORM ACTION=\"$READ_URL\" METHOD=\"post\"><INPUT TYPE=\"hidden\" NAME=\"action\" VALUE=\"calculateTaxaInterval\">";
    } else  {
        print "<FORM ACTION=\"$READ_URL\" METHOD=\"post\"><INPUT TYPE=\"hidden\" NAME=\"action\" VALUE=\"calculateStratInterval\">";
        print "<INPUT TYPE=\"hidden\" NAME=\"input\" VALUE=\"".uri_escape($section_name)."\">";
        print "<INPUT TYPE=\"hidden\" NAME=\"taxon_resolution\" VALUE=\"".$q->param("taxon_resolution")."\">";
    }    
    print "<INPUT TYPE=\"hidden\" NAME=\"input_type\" VALUE=\"".$q->param('input_type')."\">";
    for(my $i=0;my ($taxon_name,$occurrence_list) = each(%occ_list);$i++){
        print "<INPUT TYPE=hidden NAME=taxon_name_$i VALUE='$taxon_name' CHECKED=checked><INPUT TYPE=hidden NAME=\"occurrence_list_$i\" VALUE=\"$occurrence_list\">\n";
    }     

    my $methods = ['Strauss and Sadler (1989)','Marshall (1994)','Solow (1996)'];
    my $method_select = HTMLBuilder::htmlSelect('conf_method',$methods,$methods,$q->param('conf_method'));

    my $estimates = ['total duration','first appearance','last appearance','no confidence intervals'];
    my $estimate_select = HTMLBuilder::htmlSelect('conf_type',$estimates,$estimates,$q->param('conf_type'));

    my $confidences = ['0.99','0.95','0.8','0.5','0.25'];
    my $alpha = $q->param('alpha') || '0.95';
    my $confidence_select = HTMLBuilder::htmlSelect('alpha',$confidences,$confidences,$alpha);

    my $order_by = ['name','first appearance','last appearance','stratigraphic range'];
    my $order_by_select = HTMLBuilder::htmlSelect('order',$order_by,$order_by,$q->param('order'));

    my $colors = ['grey','black','red','blue','yellow','green','orange','purple'];
    my $color_select = HTMLBuilder::htmlSelect('color',$colors,$colors,$q->param('color'));

    my $glyph_types = ['boxes','circles','hollow circles','squares','hollow squares','none'];
    my $glyph_type = $q->param('glyph_type') || 'squares';
    my $glyph_type_select = HTMLBuilder::htmlSelect('glyph_type',$glyph_types,$glyph_types,$glyph_type);
    
    if ($form_type eq 'large') {
        print "<div align=\"center\"><H2>Confidence interval options</H2></div>";
        print '<center><table cellpadding=5 border=0>';
        
        if ($type eq 'taxon')   {    
            print "<tr><th align=\"right\"> Time scale: </th><td>$scale_select</td></tr>";
        } 
        
        print "<tr><th></th><td><span class=\"tiny\">(Please select a time scale that is appropriate for the taxa you have chosen)</span></td></tr>";
        print "<tr><th align=\"right\"> Confidence interval Method: </th><td> $method_select<a href=\"javascript: tipsPopup('/public/tips/confidencetips1.html')\">   Help</a></td></tr>";
        print "<tr><th align=\"right\"> Estimate: </th><td> $estimate_select</td><tr>";
        print "<tr><th align=\"right\"> Confidence level: </th><td>$confidence_select</td></tr>";
        print "<tr><th align=\"right\"> Order taxa by: </th><td> $order_by_select</td><tr>";
        print "<tr><th align=\"right\"> Draw occurrences with: </th><td> $color_select $glyph_type_select</td><tr>";
        print "</table><br>";
        print "<input name=\"full\" type=\"submit\" value=\"Submit\">";
        print "</form></center><br><br>";
    } else {
        print '<center><table class="darkList" cellpadding=5 border=0 style="border: 1px #000000 solid">';
        print '<tr><th align="CENTER" colspan=4><div class="large">Options</div></th><tr>';
        
        if ($type eq 'taxon')   {    
            print "<tr><th align=\"right\"> Time scale: </th><td colspan=3>$scale_select</td></tr>";
        } 

        print "<tr><th align=\"right\"> Confidence interval Method: </td><td> $method_select <a href=\"javascript: tipsPopup('/public/tips/confidencetips1.html')\">   Help</a></td>";
        print "<TH align=\"right\"> Confidence level: </TH><TD>$confidence_select</td></tr>";
        print "<TR><TH align=\"right\"> Estimate: </TH><TD>$estimate_select</td>";
        print "<TH ALIGN=\"right\">Order taxa by: </TH><TD>$order_by_select</td><tr>";
        print "<TR><TH align=\"right\">Draw occurrences with: </TH><TD COLSPAN=3>$color_select $glyph_type_select</td></tr>";
        print "</table><br>";
        print "<input name=\"full\" type=\"submit\" value=\"Submit\">";
        print "</form></center><br><br>";
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
    my %occ_list;
    my $testspe =0;
    while ($q->param("occurrence_list_$testspe"))  {
        if ($q->param("taxon_name_$testspe"))   {
            $occ_list{$q->param("taxon_name_$testspe")} = $q->param("occurrence_list_$testspe");
        }
        $testspe++;
    }
    
    my $scale = $q->param("scale");
    my $C = $q->param("alpha");
    my $conf_type = $q->param("conf_type");
    my $conf_method = $q->param("conf_method");

    if (scalar(keys(%occ_list)) <= 1 && $conf_method eq "Solow (1996)")   {
        optionsForm($q, $s, $dbt, \%occ_list);
        print "<center><table><tr><th><font color=\"red\">The Solow (1996) method requires more than one taxon</font></th></tr></table></CENTER><br>";
        return;
    }

#    print $q->query_string."<BR>";

    my %namescale;
    my @intervalnumber;
    my @not_in_scale;

    my $t = new TimeLookup($dbt);
    my $mapping  = $t->getScaleMapping($scale);
    my ($upper_boundary,$lower_boundary) = $t->getBoundaries;

    # Returns ordered array of interval_nos, ordered from youngest to oldest
    my @scale = $t->getScaleOrder($scale,'number');

    if ($scale == 6 || $scale == 73) {
        foreach my $interval_no (32,33) { # Holocene Pleistocene tackd onto gradstein and hardland stages
            foreach my $sub_itv ($t->mapIntervals($interval_no)) {
                $mapping->{$sub_itv} = $interval_no;
            }
        }
        unshift @scale,33;
        unshift @scale,32;
    }
    

    my $ig = $t->getIntervalGraph;
    my $interval_names;
    foreach my $itv (values %$ig) {
        $interval_names->{$itv->{interval_no}} = $itv->{name};
    }

    my %taxa_hash;

    while(my ($taxon_name,$occurrence_list) = each(%occ_list)){

        my $occs = getOccurrenceData($dbt,'occurrence_list'=>$occurrence_list);

        my %mismappings;
        my %mappings;
        foreach my $occ (@$occs) {
            my $max = $occ->{'max_interval_no'};
            my $min = $occ->{'max_interval_no'} || $max;
            if ($mapping->{$max} && $mapping->{$max} == $mapping->{$min}) {
                my $mapped_interval_no = $mapping->{$max};
                push @{$mappings{$mapped_interval_no}}, $occ->{'collection_no'};
            } else {
                push @{$mismappings{$max."_".$min}}, $occ->{'collection_no'};
            }
        } 

       
        if (! %mappings) {
            push @not_in_scale, $taxon_name;
        } else {
            my $bar_data = {};
            $bar_data->{'mappings'} = \%mappings;
            $bar_data->{'mismappings'} = \%mismappings;
            $taxa_hash{$taxon_name} = $bar_data;

            #------------FIND FIRST AND LAST OCCURRENCES---------------
            my $first_interval;
            my $last_interval;
            my $last_lower;
            my @gaplist;
           
            for(my $i=0;$i<@scale;$i++) {
                my $interval_no = $scale[$i];
                if (!$last_interval && $bar_data->{'mappings'}->{$interval_no}) {
                    $last_interval = $interval_no;    
                    my $lower = $lower_boundary->{$interval_no};
                    $last_lower = $lower;
                }
                if ($bar_data->{'mappings'}->{$interval_no}) {
                    my $map_count = scalar(@{ $bar_data->{'mappings'}->{$interval_no} });
                    $first_interval = $interval_no;
                    my $lower = $lower_boundary->{$interval_no};
                    push @gaplist, sprintf("%.1f", $lower - $last_lower); #round to 1 decimal precision
                    if ($map_count > 1) {
                        foreach (2 .. $map_count) {
                            push @gaplist, 0;
                        }
                    }
                    
                    $last_lower = $lower;
                }
            }
          
            my ($C1,$C2) = (0,0);
            while (my ($i,$c) = each %{$bar_data->{'mappings'}}) {
                $C1 += scalar(@$c);
            }
            while (my ($i,$c) = each %{$bar_data->{'mismappings'}}) {
                $C2 += scalar(@$c);
            }
#            print "FOR $taxon_name: $C1 MAPPED, $C2 MISMAPPED<BR>";
           
            my $first = $lower_boundary->{$first_interval};
            my $last = $upper_boundary->{$last_interval};

            my $length = $first - $last;
            my $N = scalar(@gaplist);
            #my $occ_count = scalar(@$occs);

            shift @gaplist;
            $bar_data->{correlation} = transpositionTest(reverse @gaplist);
            @gaplist = sort {$a <=> $b} @gaplist;
            
            my ($first_c_long, $last_c_long, $first_c_short, $last_c_short);
            if ($conf_method eq "Strauss and Sadler (1989)") {
                my $conf_length = StraussSadler1989($N,$C,$conf_type,$length);
                $first_c_long = $conf_length;
#                $conf_length /= $length;
                $last_c_long = $conf_length;
            } elsif ($conf_method eq "Marshall (1994)") {
                ($first_c_long, $first_c_short) = Marshall1994(\@gaplist,$C);
                ($last_c_long , $last_c_short)  = ($first_c_long, $first_c_short);
            }
           
            $bar_data->{'first'} = $first; 
            $bar_data->{'last'} = $last; 
            $bar_data->{'length'} = $length; 
            $bar_data->{'occurrence_count'} = $N;
            $bar_data->{'last_short'} = $last - $last_c_short if ($last_c_short =~ /\d/);
            $bar_data->{'last_long'} = $last - $last_c_long if ($last_c_long =~ /\d/);
            $bar_data->{'first_short'} = $first + $first_c_short if ($first_c_short =~ /\d/);
            $bar_data->{'first_long'} = $first + $first_c_long if ($first_c_long =~ /\d/);
        
        }
    }

    if (scalar(@not_in_scale) == scalar(keys(%occ_list))) {
        print "<p></p><div class=\"warning\">Warning: Could not map any of the taxa to the timescale requested. <br></div>";
        optionsForm($q, $s, $dbt, \%occ_list, 'small');
        return;
    }

    if ($conf_method eq "Solow (1996)") { 
        # Min and max
        my @mx;
        foreach my $taxon (keys(%taxa_hash)) {
            push @mx, $taxa_hash{$taxon}{'first'};
            push @mx, $taxa_hash{$taxon}{'last'};
        }
        @mx = sort {$a <=> $b} @mx;
        my $Smax = $mx[$#mx];
        my $Smin = $mx[0];
        my ($firstsig, $first_c_long, $lastsig, $last_c_long) = Solow1996(\%taxa_hash,$C,$conf_type);
    
        foreach my $taxon (keys %taxa_hash) {
            $taxa_hash{$taxon}{'first_long'} = $first_c_long if ($first_c_long =~ /\d/);
            $taxa_hash{$taxon}{'first_sig'} = $firstsig;
            $taxa_hash{$taxon}{'last_long'} = $last_c_long if ($last_c_long =~ /\d/);
            $taxa_hash{$taxon}{'last_sig'} = $lastsig;
        }
    } 

    # SORT OUTPUT
    my @sortedTaxa = keys %taxa_hash;
    if ($q->param('order') eq "first appearance")   {
        @sortedTaxa = sort {$taxa_hash{$b}{'first'} <=> $taxa_hash{$a}{'first'} ||
                            $taxa_hash{$b}{'last'} <=> $taxa_hash{$a}{'last'} ||
                            $a cmp $b} @sortedTaxa;
    } elsif ($q->param('order') eq "last appearance") {
        @sortedTaxa = sort {$taxa_hash{$b}{'last'} <=> $taxa_hash{$a}{'last'} ||
                            $taxa_hash{$b}{'first'} <=> $taxa_hash{$a}{'first'} ||
                            $a cmp $b} @sortedTaxa;
    } elsif ($q->param('order') eq "name")   {
        @sortedTaxa = sort {$a cmp $b} @sortedTaxa;
    } else  {
        @sortedTaxa = sort {$taxa_hash{$b}{'length'} <=> $taxa_hash{$a}{'length'} ||
                            $taxa_hash{$b}{'first'} <=> $taxa_hash{$a}{'first'} ||
                            $a cmp $b} @sortedTaxa;
    }

    my $cg = new ConfidenceGraph(
        $q->Vars,
       'y_axis_unit'=>"Ma",
       'y_axis_max'=>0,
       'y_axis_type'=>'continuous');


    foreach my $interval_no (@scale) {
        my $max = $lower_boundary->{$interval_no};
        my $min = $upper_boundary->{$interval_no};
        my $interval_name = $interval_names->{$interval_no};
        my %collections = ();
        while (my ($label,$taxon_data) = each %taxa_hash) {
            if (exists $taxon_data->{'mappings'}->{$interval_no}) {
                foreach (@{$taxon_data->{'mappings'}->{$interval_no}}) {
                    $collections{$_} = 1;
                }
            }
        }
        my $link;
        if (scalar keys %collections) {
            $link = "$READ_URL?action=displayCollResults&amp;collection_list=".join(",",keys %collections);
        }
        my $short_interval_name = $interval_name;
        $short_interval_name =~ s/^early/e./;
        $short_interval_name =~ s/^middle/m./;
        $short_interval_name =~ s/^late/l./;
        $short_interval_name =~ s/Early\/Lower/E./;
        $short_interval_name =~ s/Middle/M./;
        $short_interval_name =~ s/Late\/Upper/L./;
        $cg->addRangeLabel($short_interval_name,$max,$min,$link,$interval_name);
        $cg->addTick($max,$max);
        $cg->addTick($min,$min);
    }

    foreach my $taxon (@sortedTaxa) {
        my $taxon_label = $taxon;
        my $taxon_data = $taxa_hash{$taxon};
        my %collections = ();
        foreach my $interval_no (keys %{$taxon_data->{mappings}}){
            foreach (@{$taxon_data->{'mappings'}->{$interval_no}}) {
                $collections{$_} = 1;
            }
        }
        my $link;
        if (scalar keys %collections) {
            $link = "$READ_URL?action=displayCollResults&amp;collection_list=".join(",",keys %collections);
        }
        $cg->addBar($taxon_label,$taxon_data,$link,$taxon);

        foreach my $interval_no (keys %{$taxon_data->{mappings}}){
            my $max = $lower_boundary->{$interval_no};
            my $min = $upper_boundary->{$interval_no};
            my $interval_name = $interval_names->{$interval_no};
            my %collections = ();
            foreach (@{$taxon_data->{'mappings'}->{$interval_no}}) {
                $collections{$_} = 1;
            }
            my $link;
            if (scalar keys %collections) {
                $link = "$READ_URL?action=displayCollResults&amp;collection_list=".join(",",keys %collections);
            }
            $cg->addPoint($taxon_label,[$max,$min],$link,"$taxon at $interval_name");
        }
    }

    my ($image_map,$image_name) = $cg->drawGraph();

    print printResultsPage($q,'Confidence interval results',$image_map,$image_name,\%taxa_hash,\@sortedTaxa,"Ma",\@not_in_scale);

    optionsForm($q, $s, $dbt, \%occ_list, 'small');
    print " <b><a href=\"$READ_URL?action=displayTaxaInteralsForm\">Start again</a></b><p></center><br><br><br>";

}


sub printResultsPage {
    my ($q,$title,$image_map,$image_name,$taxa_data,$sorted_keys,$unit,$not_in_scale) = @_;
    my @not_in_scale = @$not_in_scale;
    my %taxa_hash = %$taxa_data;
    my @sortedTaxa = @$sorted_keys;
    my @warnings;

    my $any_correlation = 0;
    my $any_extant = 0;
    my $any_future = 0;

    my $conf_method = $q->param('conf_method');
    $unit = " ($unit)" if ($unit);

    my $image_count = $image_name;
    $image_count =~ s/[^0-9]//g;

    # RESULTS-PAGE
    print "<div align=\"center\"><H2>$title</H2></div>";
    
    print $image_map;
    print "<div align=\"center\"><table><tr><td valign=\"top\" align=\"center\"><img src=\"/public/confidence/$image_name.png\"  usemap=\"#ConfidenceMap\" ismap border=0><br>";
    print "<br><b>Download image as: <a href=\"/public/confidence/$image_name.png\">PNG</a>";
    print ", <a href=\"/public/confidence/$image_name.jpg\">JPEG</a>";
    print ", <a href=\"/public/confidence/$image_name.ai\">AI</a></b>";
    print "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;";
    print "<b>Download data as: <a href=\"/public/confidence/confidence$image_count.csv\">CSV</a></b>";
    print "<br><br><b>Click on a taxon, section level, or gray box to get more info</b><br><br></td></tr>";
  
    print "<tr><td align=\"center\">";
    my $html;
    my $csv;
    if($conf_method eq "Strauss and Sadler (1989)") {
        my @table = ();
        push @table, ["","first occurrence$unit","last occurrence$unit","confidence interval$unit", 'number of horizons', 'transposition test'];
        for(my $i=0;$i<scalar(@sortedTaxa);$i++){
            my @row = ();
            push @row, $sortedTaxa[$i]; #taxon name
            my $data = $taxa_hash{$sortedTaxa[$i]};
            my $conf;
            if ($data->{'first_long'} =~ /\d/) {
                $conf = sprintf("%.3f",abs($data->{'first_long'} - $data->{'first'}));
            } else {
                $conf = "N/A";
            }
            push @row, ($data->{'first'},$data->{'last'},$conf,$data->{'occurrence_count'},$data->{'correlation'});
            push @table,\@row;
        }
        $html .= printResultTable(\@table);
        $csv .= printCSV(\@table);
    } elsif($conf_method eq "Marshall (1994)") {
        my @table = ();
        push @table,["","first occurrence$unit","last occurrence$unit","lower confidence interval$unit", "upper confidence interval$unit",'number of horizons', 'transposition test'];
        for(my $i=0;$i<scalar(@sortedTaxa);$i++){
            my @row = ();
            push @row, $sortedTaxa[$i]; #taxon name
            my $data = $taxa_hash{$sortedTaxa[$i]};
            my $short;
            if ($data->{'first_short'} =~ /\d/) {
                $short = sprintf("%.3f",abs($data->{'first_short'} - $data->{'first'}));
            } else {
                $short = "N/A";
            }
            my $long; 
            if ($data->{'first_long'} =~ /\d/) {
                $long = sprintf("%.3f",abs($data->{'first_long'} - $data->{'first'}));
            } else {
                $long = "N/A";
            }
            push @row, ($data->{'first'},$data->{'last'},$short,$long,$data->{'occurrence_count'},$data->{'correlation'});
            push @table,\@row;
        }
        $html .= printResultTable(\@table);
        $csv .= printCSV(\@table);
    } elsif($conf_method eq "Solow (1996)") {
        my @table = ();
        push @table, ["","first occurrence$unit","last occurrence$unit",'number of horizons', 'transposition test'];
        my $data;
        for(my $i=0;$i<scalar(@sortedTaxa);$i++){
            my @row;
            push @row, $sortedTaxa[$i]; #taxon name
            $data = $taxa_hash{$sortedTaxa[$i]};
            push @row, ($data->{'first'},$data->{'last'},$data->{'occurrence_count'},$data->{'correlation'});
            push @table,\@row;
        }
        $html .= printResultTable(\@table);
        $csv  .= printCSV(\@table);

        $html .= "<br><br>";
        $csv  .= ",\n";
       
        my $first_long = ($data->{'first_long'} =~ /\d/) ? sprintf("%.3f", $data->{'first_long'}) : "NA";
        my $first_sig  = $data->{'first_sig'};
        my $last_long  = ($data->{'last_long'}  =~ /\d/) ? sprintf("%.3f", $data->{'last_long'})  : "NA";
        my $last_sig   = $data->{'last_sig'};
      
        # print table 2 
        @table = ();
        push @table,["","significance level","confidence limit$unit"];
        if ($first_sig =~ /\d/) {
            push @table,['common first occurrence',$first_sig,$first_long]; 
        }
        if ($last_sig =~ /\d/) {
            push @table,['common last occurrence',$last_sig,$last_long];
        }
        if (@table) {
            $html .= printResultTable(\@table);
            $csv .= printCSV(\@table);
        }
    }

    my $file = "$ENV{BRIDGE_HTML_DIR}/public/confidence/confidence$image_count.csv";
    open CSV,">$file";
    print CSV $csv;

    print $html;

    print "<br></td></tr></table>";


    if ($any_correlation)    {
        push @warnings, '* This taxon fails the tranposition test';
    } 

    if ($any_extant)    {
        push @warnings, '** This taxon may not be extinct';
    }

    if ($any_future) {
        push @warnings, '*** The upper confidence interval extends into the future and is therefore unreliable';
    }
    if (scalar @not_in_scale) {
        push @warnings, "The following taxa were excluded from the chart because they could not be mapped to the time scale specified:<br>".join(", ",@not_in_scale);
    }
        
    if (@warnings) {
        print "<div align=\"center\">".Debug::printWarnings(\@warnings)."</div><br>";
    }

    return;
}


sub printCSV {
    require Text::CSV_XS;
    my $csv = Text::CSV_XS->new();

    my @table = @{$_[0]};
    my $rows = scalar(@table);
    my $cols = scalar(@{$table[0]});

    my $txt = "";
    for(my $i=0;$i<$rows;$i++) {
        my @row = @{$table[$i]};
        if ($csv->combine(@row)) {
            $txt .= $csv->string."\n";
        }
    }
    return $txt;
}

# Used in CalculateTaxaInterval, print HTML table
sub printResultTable { 
    my @table = @{$_[0]};
    my $rows = scalar(@table);
    my $cols = scalar(@{$table[0]});

    my $html = '<table><tr><td>';
    $html .= '<table class="simpleTable">';
    for(my $j=0;$j<$cols;$j++) {
        $html .= qq|<td class="simpleTableHeader">$table[0][$j]</td>|;
    }
    $html .= "</tr>";
    # RESULTS TABLE BODY
    for(my $i=1;$i<$rows;$i++) {
        for(my $j=0;$j<$cols;$j++) {
            $html .= qq|<td class="simpleTableCell">$table[$i][$j]</td>|;
        }
        $html .= "</tr>";
    }
    $html .= "</table>";
#    $html .= "<b>Download as: <a href=\"/public/confidence/confidence$tableNo.csv\">CSV</a></b>";
    $html .= "</td></tr></table>";
    return $html;
}

#--------------CALCULATE STRATIGRAPHIC RELATIVE CONFIDENCE INTERVALS----------------
sub calculateStratInterval	{
    my $q=shift;
    my $s=shift;
    my $dbt=shift;
    my $dbh=$dbt->dbh;
    my $section_name = uri_unescape($q->param("input"));
    my $section_type = ($q->param("input_type") eq 'regional') ? 'regional' : 'local';
    my $alpha = $q->param("alpha");
    my $conf_type = $q->param("conf_type");
    my $conf_method = $q->param("conf_method");
    my $stratres = $q->param("stratres");

    my %taxa_hash;
    my %sectionbed;
    my %sectionorder;
    my %bed_unit;
    my %occ_list;

    for(my $i=0;$q->param("occurrence_list_$i");$i++) {
        my $taxon_name = $q->param("taxon_name_$i");
        my $occurrence_list = $q->param("occurrence_list_$i");
        $occ_list{$taxon_name}=$occurrence_list;

        my $occs = getOccurrenceData($dbt,
            occurrence_list=>$occurrence_list,
            fields=>"${section_type}bed, ${section_type}order, ${section_type}bedunit, c.collection_no, o.genus_name, o.species_name, o.taxon_no"
        );

        foreach my $row (@$occs) {
            $bed_unit{$row->{$section_type.'bedunit'}}++;
            $sectionbed{$row->{'collection_no'}} = $row->{$section_type.'bed'};
            $sectionorder{$row->{$section_type.'order'}}++;
            push @{$taxa_hash{$taxon_name}{'beds'}}, $row->{$section_type.'bed'};
            push @{$taxa_hash{$taxon_name}{'collections'}}, $row->{collection_no};
        }
    }

    
    my @orders = sort {$sectionorder{$b} <=> $sectionorder{$a}} keys %sectionorder;
    my $common_order = $orders[0];
    my $y_axis_order = 1;
    if ($common_order =~ /top to bottom/) {
        $y_axis_order = 0;
    }

    my @units = sort {$bed_unit{$b} <=> $bed_unit{$a}} keys %bed_unit;
    my $common_unit = $units[0];


    # Build and display graph
    my $y_axis_type = 'discrete';
    if ($common_unit) {
        $y_axis_type = 'continuous';
    }

    my @all_beds = ($y_axis_order) 
        ? sort {$b <=> $a} values %sectionbed
        : sort {$a <=> $b} values %sectionbed;                        
    my $number_horizons = scalar(@all_beds);    # how many horizons for whole section
    my $minhorizon = $all_beds[0];              # youngest horizon, e.g. 3
    my $maxhorizon = $all_beds[$#all_beds];     # the oldest horizon number, e.g., 17 (+1, for upper bound)
    
    my @all_values = @all_beds;
    foreach my $taxon_name (keys(%taxa_hash)) {
        my $bar_data = $taxa_hash{$taxon_name};
            
        my @horizons = ($y_axis_order) 
            ? sort {$b <=> $a} @{$bar_data->{'beds'}}
            : sort {$a <=> $b} @{$bar_data->{'beds'}};
        my $count = scalar(@horizons);
        my @gaplist;
        for(my $i=1;$i<@horizons;$i++) {
            push @gaplist, abs($horizons[$i] - $horizons[$i-1]);
        }
        my $first  = $horizons[$#horizons]; # upper species horizon, say 10 (+ 1, for upper bound of interval)
        my $last   = $horizons[0];              # lower species horizon, say 6
        my $length = ($y_axis_type =~ /continuous/) 
            ? abs($first - $last)
            : abs($first - $last) + 1;        # total number of horizons for species, therefore 5;
            
        $bar_data->{correlation} = transpositionTest(reverse @gaplist);
        
        my ($first_c_long, $last_c_long, $first_c_short, $last_c_short);
        if ($conf_method eq "Strauss and Sadler (1989)") {
            my $conf_length = StraussSadler1989($count,$alpha,$conf_type,$length);
#            my $conf_horizons = $conf_length/$length;   # length of conf interval as number of horizons
            ($first_c_long,$last_c_long) = ($conf_length,$conf_length);
        } elsif ($conf_method eq "Marshall (1994)") {
            @gaplist = sort {$a <=> $b} @gaplist;
            ($first_c_long, $first_c_short) = Marshall1994(\@gaplist,$alpha);
            ($last_c_long , $last_c_short)  = ($first_c_long, $first_c_short); 
        }
        $bar_data->{'occurrence_count'} = $count; 
        $bar_data->{'first'}  = $first;
        $bar_data->{'last'}   = $last;
        $bar_data->{'length'} = $length;
        if ($y_axis_order)  {
            $bar_data->{'first_long'} = $first - $first_c_long if ($first_c_long =~ /\d/);
            $bar_data->{'first_short'} = $first - $first_c_short if ($first_c_short =~ /\d/);
            $bar_data->{'last_long'} = $last + $last_c_long if ($last_c_long =~ /\d/);
            $bar_data->{'last_short'} = $last + $last_c_short if ($last_c_short =~ /\d/);
        } else {
            $bar_data->{'first_long'} = $first + $first_c_long if ($first_c_long =~ /\d/);
            $bar_data->{'first_short'} = $first + $first_c_short if ($first_c_short =~ /\d/);
            $bar_data->{'last_long'} = $last - $last_c_long if ($last_c_long =~ /\d/);
            $bar_data->{'last_short'} = $last - $last_c_short if ($last_c_short =~ /\d/);
        }
        push @all_values, $bar_data->{'first_long'} if ($bar_data->{'first_long'} =~ /\d/);
        push @all_values, $bar_data->{'first_short'} if ($bar_data->{'first_short'} =~ /\d/);
        push @all_values, $bar_data->{'last_long'} if ($bar_data->{'last_long'} =~ /\d/);
        push @all_values, $bar_data->{'last_short'} if ($bar_data->{'last_short'} =~ /\d/);
    }

    if ($conf_method eq "Solow (1996)") {
        # Min and max
        my @mx;
        foreach my $taxon (keys(%taxa_hash)) {
            push @mx, $taxa_hash{$taxon}{'first'};
            push @mx, $taxa_hash{$taxon}{'last'};
        }
        @mx = sort {$a <=> $b} @mx;
        my $Smax = $mx[$#mx];
        my $Smin = $mx[0];
        my ($firstsig, $first_c_long, $lastsig, $last_c_long) = Solow1996(\%taxa_hash,$alpha,$conf_type,$y_axis_order);
    
        foreach my $taxon (keys %taxa_hash) {
            $taxa_hash{$taxon}{'first_long'} = $first_c_long if ($first_c_long =~ /\d/);
            $taxa_hash{$taxon}{'first_sig'} = $firstsig;
            $taxa_hash{$taxon}{'last_long'} = $last_c_long if ($last_c_long =~ /\d/);
            $taxa_hash{$taxon}{'last_sig'} = $lastsig;
        }
    }

    # Sort taxa
    my @sortedTaxa = keys %taxa_hash;
    if ($q->param('order') eq "first appearance") {
        @sortedTaxa = sort {$taxa_hash{$a}{'first'} <=> $taxa_hash{$b}{'first'} ||
                            $taxa_hash{$a}{'last'}  <=> $taxa_hash{$b}{'last'} ||
                            $a cmp $b} @sortedTaxa;
    } elsif ($q->param('order') eq "last appearance") {
        @sortedTaxa = sort {$taxa_hash{$a}{'first'} <=> $taxa_hash{$b}{'first'} ||
                            $taxa_hash{$a}{'last'} <=> $taxa_hash{$b}{'last'} ||
                            $a cmp $b} @sortedTaxa;
    } elsif ($q->param('order') eq "name")   {
        @sortedTaxa = sort {$a cmp $b} @sortedTaxa;
    } else  {
        @sortedTaxa = sort {$taxa_hash{$a}{'length'} <=> $taxa_hash{$b}{'length'} ||
                            $taxa_hash{$a}{'last'} <=> $taxa_hash{$b}{'last'} ||
                            $a cmp $b} @sortedTaxa;
    }

    my $common_unit_full_name;
    if ($common_unit eq 'm') {
        $common_unit_full_name = "Meters";
    } elsif ($common_unit eq 'cm') {
        $common_unit_full_name = "Centimeters";
    } elsif ($common_unit eq 'ft') {
        $common_unit_full_name = "Feet";
    } else {
        $common_unit_full_name = "Beds";
    }

    my $cg = new ConfidenceGraph(
        $q->Vars,
        'y_axis_order'=>$y_axis_order,
        'y_axis_unit'=>$common_unit,
        'y_axis_label'=>$common_unit_full_name,
        'y_axis_type'=>$y_axis_type);

    @all_values = sort {$a <=> $b} @all_values;
    foreach my $bed (int($all_values[0]-1) .. int($all_values[$#all_values]+1)) {
        my $link = "";

        my @collections;
        while (my($collection_no,$bed_value) = each %sectionbed) {
            if ($bed_value == $bed) {
                push @collections,$collection_no;
            }
        }
        if (@collections) {
            $link = "$READ_URL?action=displayCollResults&amp;collection_list=".join(",",@collections);
        }
        $cg->addTick($bed,$bed,$link,$bed);
    }

    foreach my $taxon (@sortedTaxa) {
        my $taxon_label = $taxon;
        my $taxon_data = $taxa_hash{$taxon};
        my $collection_list = join(",",@{$taxon_data->{'collections'}});
        my $link = "$READ_URL?action=displayCollResults&amp;collection_list=$collection_list";
        $cg->addBar($taxon_label,$taxon_data,$link,$taxon);

        foreach my $bed (@{$taxon_data->{beds}}){
            my $link = "";

            my @collections;
            for(my $i=0;$i<scalar(@{$taxon_data->{beds}});$i++) {
                if ($bed == $taxon_data->{beds}->[$i]) {
                    push @collections, $taxon_data->{collections}->[$i];
                }
            }
            if (@collections) {
                $link = "$READ_URL?action=displayCollResults&amp;collection_list=".join(",",@collections);
            }
            $cg->addPoint($taxon_label,$bed,$link,"$taxon at $bed");
        }
    }

    my ($image_map,$image_name) = $cg->drawGraph();

    print printResultsPage($q,"<i>$section_name</i> stratigraphic section",$image_map,$image_name,\%taxa_hash,\@sortedTaxa,$common_unit,[]);

    optionsForm($q, $s, $dbt, \%occ_list, 'small');
    print " <b><a href=\"$READ_URL?action=displaySearchSectionForm\">Start again</a><b><p></center><br><br><br>";

    return;
} 


sub transpositionTest {
    my @gaplist = @_;
    my $N = scalar(@gaplist);

    my $Tmax = ($N * ($N - 1))/2;
    my $Total;
    foreach my $h (1 .. 2) {    
        my $T = 0;
        my $Tequal = 0;
        my @done;
        foreach my $i (0 .. ($N - 2)) {
            my $temp = 1;
            foreach my $j (($i + 1) .. ($N - 1)) {
                if ($gaplist[$j] <  $gaplist[$i]) {
                    $T++;
                }
            }   
        }
        $Total = $T;
#        print "H$h TOTAL: $Total\n";
        my %same_value;
        foreach (@gaplist) {
            $same_value{$_}++;
        }
        while (my ($gap,$N) = each %same_value) {
            if ($N> 1) {
                $Total += $N*($N-1)/4;
#                print "ADDING ".($N*($N-1)/4). " TO TOTAL FOR $gap\n";
            }
        }
#        print "H$h TOTAL: $Total\n";
        if($Total > ($Tmax/2)) {
            @gaplist = reverse(@gaplist);
        } else {
            last;
        }
    }
    my $correlation = 0;
    #---------------- equaation for p(0.95) ONLY------------------
    if ($Total <= ((0.215*($N**2))-(1.15*$N)+0.375)) {
#        print "*Significant correlation at alpha = 0.95  <BR>";
        $correlation = 1;
    } else {
#        print "No correlataion  <BR>";
    }
    return $correlation;
}

sub factorial {
    if ($_[0] == 0) {
        1;
    } else {
        $_[0] * factorial($_[0] - 1);
    }
}

sub StraussSadler1989 {
    my $count = shift;
    my $C = shift;
    my $conf_type = shift;
    my $intervallength = shift;
         
    my $alpha = 0;      # calculate intervals Strauss and Saddler (1989)
    my $iterate;
    if ($count > 2) {
        if ($conf_type eq 'last appearance' || $conf_type eq 'first appearance')	{
            $alpha = ((1 - $C)**(-1/($count - 1))) - 1;
        } else	{
            my $end =  30000;
            my $divisor = 1000;
            foreach my $i (1..$end)	{
                $alpha = $i/$divisor;
                $iterate = (1 - (2 * ((1 + $alpha)**( - ($count - 1)) ) ) + ((1 + 2 * $alpha)**( - ($count - 1))));
                if ($iterate > $C)	{
                    last;
                }
            }			
        }
    }
    my $conf_length = $intervallength * $alpha;
    return $conf_length;
}

#FOR MARSHALL 1994
sub Marshall1994{
    my $gaplist = shift;
    my @gaplist = @$gaplist;
    my $C = shift;
    @gaplist = sort {$a <=> $b} @gaplist;
    my $N = scalar(@gaplist);
            
    my $alpha = 0.95;       #STANDARD FOR CONFIDENCE PROBABILITIES OF CONFIDENCE INTERVALS ($C)
    my $gamma = (1 - $alpha)/2;
    my $ll = 0;
    my $uu = 0;
    my ($short,$long);
#        print "N: $N GAMMA:$gamma\n";
    if ($N > 5) {
        my @sumtable;
        my $sum = 0;
        foreach my $i (0 .. $N) {
            my $Nx = factorial($N) / (factorial($i) * factorial($N - ($i)));
            $sum = $sum + ($Nx * ($C**$i)) * ((1 - $C)**($N - $i));
            $sumtable[$i] = $sum;
        }
#        print join(", ",map {sprintf("%.3f",$_)} @sumtable),"\n";
        my ($low,$upp);
        foreach my $x (0 .. $N) {
            my $sum = $sumtable[$x];
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
#            $first_c_long = 0;
            $short = $gaplist[$low - 1];        
#            $short = $gaplist[$low];
        } else {
#            $long = $gaplist[$upp];
#            $short = $gaplist[$low];        
            $long = $gaplist[$upp - 1];
            $short = $gaplist[$low - 1];        
        }
        
    } else {
#        $first_c_long = 0;
#        $first_c_short = 0;            
    }

    return ($long,$short);
}

# FOR SOLOW (1996)
sub Solow1996{
    my $taxa_hash = shift;
    my $alpha = shift;
    my $conf_type = shift;
    my $scale_order = shift;

    $alpha = 1 - $alpha;
    my ($lastsig,$firstsig,$last_c_long,$first_c_long);
  
    # Taxa is the filtered version - must have at least 2 unique values
    my %taxa;
    while (my ($taxon,$data) = each %$taxa_hash) {
        if ($data->{'first'} != $data->{'last'}) {
            $taxa{$taxon} = $data;
        }
    }

    return unless scalar keys %taxa > 1;

    if ($conf_type eq 'total duration' || $conf_type eq 'first appearance')	{
        #find the maximum horizon
        my @mx;
        while (my ($taxon,$data) = each %taxa) {
            if ($scale_order) {
                push @mx, -1*$data->{'first'};
            } else {
                push @mx, $data->{'first'};
            }
        }
        @mx = sort {$a <=> $b} @mx;
        my $max = $mx[$#mx];
#        print "first appearance: $max<BR>";

        #-------------------------SIGNIFICANCE FINDER-----------------------------

# TBD DOUBLE CHECK THIS
        my $df = 2*(scalar(keys(%taxa)) - 1);
#        print "df: $df<BR>";
#        print "alpha: $alpha<BR>";
    
        my $lambda = 1;
        while (my ($taxon,$data) = each %taxa) {
            my $occurrence_count = $data->{'occurrence_count'};
            my $last  = ($scale_order) ? -1*$data->{'last'} : $data->{'last'};
            my $first = ($scale_order) ? -1*$data->{'first'}: $data->{'first'};
            $lambda = $lambda * ((abs($first - $last) / abs($max - $last)) ** ($occurrence_count - 1));
            if (abs($first - $last) == 0) {
                die "Zero length segment encountered";
            }
        }
        if ($lambda == 0) {
            $firstsig = 0;
        } else {
            $firstsig = chiSquaredDensity($df,0,-2*log($lambda),0);
        }
#        print "lambda: $lambda<BR>";


#        print "Significance first: $firstsig<BR>";
        #----------------------UPPER CONFIDENCE FINDER-----------------------------
        
        if ($firstsig > $alpha) {
            my $df = 2 * (scalar(keys(%taxa)));
            my $tester = chiSquaredDensity($df,$alpha,0,1);
#            print "tester: $tester<BR>";
            my $j;
            for ($j = $max; $j <= $max + 1000; $j = $j + 0.1) {
                my $Rstore = 1;
                while (my ($taxon,$data) = each %taxa) {
                    my $occurrence_count = $data->{'occurrence_count'};
                    my $last  = ($scale_order) ? -1*$data->{'last'} : $data->{'last'};
                    my $first = ($scale_order) ? -1*$data->{'first'}: $data->{'first'};
                    $Rstore = $Rstore * ((abs($first-$last)/abs($j - $last))**($occurrence_count - 1));
                }
                if ((-2 * log($Rstore)) > $tester) {
                    last;
                }
            }
            $first_c_long = sprintf("%.3f", $j);
            if ($scale_order) {
                $first_c_long *= -1;
            }
        } 
    } 
#    print "first SIG: $firstsig, first CONF: $first_c_long<BR>";

    if ($conf_type =~ /total duration|last appearance/i) {
        my @mx;
        # Max horizon
        while (my ($taxon,$data) = each %taxa) {
            if ($scale_order) {
                push @mx, $data->{'last'};
            } else {
                push @mx, -1*$data->{'last'};
            }
        }
        @mx = sort {$a <=> $b} @mx;
# TBD SHOULD THIS BE MAX OR MIN?
        my $min = $mx[$#mx];
#        print "last appearance: $min<BR>";

        #-------------------------SIGNIFICANCE FINDER-----------------------------

        my $df = 2 * (scalar(keys(%taxa)) - 1);
#        print "df: $df<BR>";
#        print "alpha: $alpha<BR>";
    
        my $lambda = 1;
        while (my ($taxon,$data) = each %taxa) {
            my $occurrence_count = $data->{'occurrence_count'};
# TBD: can this be right?
            my $last  = ($scale_order) ? $data->{'last'} : -1*$data->{'last'};
            my $first = ($scale_order) ? $data->{'first'}: -1*$data->{'first'};
            $lambda = $lambda * ((abs($first - $last) / abs($min - $first)) ** ($occurrence_count - 1));
            if (abs($first - $last) == 0) {
                die "Zero length segment encountered";
            }
        }
        if ($lambda == 0) {
            $lastsig = 0;
        } else {
            $lastsig = chiSquaredDensity($df,0,-2*log($lambda),0);
        }
#        print "lambda: $lambda<BR>";


#        print "Significance last: $lastsig<BR>";
        #----------------------LOWER CONFIDENCE FINDER-----------------------------
        
        if ($lastsig > $alpha) {
            my $df = 2 * scalar(keys(%taxa));
            my $tester = chiSquaredDensity($df,$alpha,0,1);
#            print "tester: $tester<BR>";
            my $j;
            for ($j = $min; $j <= $min+ 1000; $j = $j + .1) {
                my $Rstore = 1;
                while (my ($taxon,$data) = each %taxa) {
                    my $occurrence_count = $data->{'occurrence_count'};
                    my $last  = ($scale_order) ? $data->{'last'} : -1*$data->{'last'};
                    my $first = ($scale_order) ? $data->{'first'}: -1*$data->{'first'};
                    $Rstore = $Rstore* ((abs($first - $last) / abs($j - $first)) ** ($occurrence_count - 1));
                }
                if ((-2 * log($Rstore)) > $tester) {last}
            }
            $last_c_long = sprintf("%.3f", $j);
            if (! $scale_order) {
                $last_c_long *= -1;
            }
        } 
    }
#    print "last SIG: $lastsig, last CONF: $last_c_long<BR>";
    return ($firstsig, $first_c_long, $lastsig, $last_c_long);
}

# for calculating both types, either alpha or lower must be zero to work
sub chiSquaredDensity { 
    my $df = shift;
    my $alpha = shift;      #ONE TAILED
    my $lower = shift;
    my $type = shift;
    
    my $resolution = 0.01;      #WILL GET ACCURACY TO APRROX 3 DECIMAL PLACES
    my $upper = 1000;           #MY SURROGATE FOR INFINITE!!
    my $chi = 0;
    my $x;
    
    if ($type) {
        for ($x = $upper; $x >= 0; $x = $x - $resolution) {
            $chi = $chi + $resolution * ((($x**(($df/2) - 1)) * exp(-$x/2)) / (gamma($df/2) * (2 ** ($df/2))));
            if($chi >= $alpha) {last}
        }
        return sprintf("%.3f", $x);
    } else {
        for ($x = $upper; $x >= $lower; $x = $x - $resolution) {
            $chi = $chi + $resolution * ((($x**(($df/2) - 1)) * exp(-$x/2)) / (gamma($df/2) * (2 ** ($df/2))));
        }
        return sprintf("%.3f", $chi);    
    }
}

#THE GAMMA FUNCTION
sub gamma {
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
            foreach my $i ( 1 .. $n ) {
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


sub getOccurrenceData {
    my ($dbt,%options) = @_;
    my $dbh = $dbt->dbh;

    my @where;
    if (exists $options{'occurrence_list'}) {
        my @occs = split(/\s*,\s*/,$options{'occurrence_list'});
        if (@occs) {
            push @where, "o.occurrence_no IN (".join(",",map {int($_)} @occs).")";
        } else {
            push @where, "1=0";
        }
    }

    if (exists $options{'taxon_list'}) {
        my @taxa = split(/\s*,\s*/,$options{'taxon_list'});

        if (@taxa) {
            if ($taxa[0] =~ /^\d+$/) {
                push @where, "o.taxon_no IN (".join(",",map {int($_)} @taxa).")";
            } else {
                my $taxon_name = $taxa[0];
                my ($genus,$subgenus,$species) = Taxon::splitTaxon($taxon_name);

                if ($species) {
                    push @where, "o.genus_name LIKE ".$dbh->quote($genus). " AND o.species_name LIKE ".$dbh->quote($species);
                } else {
                    push @where, "o.genus_name LIKE ".$dbh->quote($genus);
                }
            }
        } else {
            push @where, "1=0";
        }
    }

    if (exists $options{'section_type'}) {
        if ($options{'section_type'} eq 'regional') {
            push @where, "c.regionalsection=".$dbh->quote($options{'section_name'});
            push @where, "c.regionalbed REGEXP '^(-)?[0-9.]+\$'";
        } elsif ($options{'section_type'} eq 'local') {
            push @where, "c.localsection=".$dbh->quote($options{'section_name'});
            push @where, "c.localbed REGEXP '^(-)?[0-9.]+\$'";
        }
    }
 
    if (@where) {
        my $where = join " AND ",@where;
        my $fields = "c.collection_no, c.max_interval_no, c.min_interval_no";
        if ($options{'fields'}) {
            $fields = $options{'fields'};
        }
        my $sql = "(SELECT $fields FROM occurrences o, collections c "
                . " LEFT JOIN reidentifications re ON o.occurrence_no=re.occurrence_no"
                . " WHERE o.collection_no=c.collection_no AND $where AND re.reid_no IS NULL)"
                . " UNION "
                . "(SELECT $fields FROM reidentifications o, occurrences o2, collections c"
                . " WHERE o2.collection_no=c.collection_no AND o2.occurrence_no=o.occurrence_no AND o.most_recent='YES' AND $where)";
        if ($options{'limit'}) {
            $sql .= " LIMIT 1"
        }

        dbg("getOccurrenceData called: $sql");
        my @data = @{$dbt->getData($sql)};
        return \@data;
    } else {
        die("NO occurrence data to fetch");
    }
}

sub _dumpTaxonObject {
    my $taxon = shift;
    my $scale = shift;

    my $txt = "";
    foreach ('upper','lower','first','last','first_c_long','first_c_short','last_c_long','last_c_short','length','uppershort','lowershort','correlation','occurrence_count'){
        if (exists $taxon->{$_}) {
            $txt .= "  $_: $taxon->{$_}";
        }
    }

    $txt .= "Intervals:";
    foreach my $itv (@$scale) {
        if ($taxon->{'mappings'}->{$itv}) {
            $txt .= " $itv:".scalar(@{ $taxon->{'mappings'}->{$itv} });
        }
    }

    return $txt;
}


package ConfidenceGraph;

use GD;
use Debug qw(dbg);

my $AILEFT = 100;
my $AITOP = 450;   
my $IMAGE_DIR = $ENV{'BRIDGE_HTML_DIR'}."/public/confidence";

sub new {
    my ($class,%options) = @_;

    my $self = {
        bars=>[],
        y_labels=>{},
        x_labels=>{},
        ai=>"",
        gd=>undef
    }; 

    # Set up default values
    foreach ('y_axis_label','x_axis_label','y_axis_unit') {
        $self->{$_} = $options{$_} || "";
    }
    $self->{y_axis_order}= $options{y_axis_order} || 0;
    $self->{y_axis_type} = $options{y_axis_type} || 'continuous';
    $self->{conf_type}   = $options{conf_type} || 'total duration';
    $self->{conf_method} = $options{conf_method} || 'Strauss and Sadler (1989)';
    $self->{glyph_type}  = $options{glyph_type} || 'squares';
    $self->{color}       = $options{color} || 'black';

    bless $self,$class;
}

sub initCanvas {
    my ($self,$width,$length) = @_;

    my $gd = GD::Image->new($width,$length);

    my $gd_colors = {
        'white'=> $gd->colorAllocate(255, 255, 255),
        'grey'=> $gd->colorAllocate(122, 122, 122),
        'black'=> $gd->colorAllocate(0,0,0),
        'red'=>$gd->colorAllocate(255,0,0),
        'blue'=>$gd->colorAllocate(63,63,255),
        'yellow'=>$gd->colorAllocate(255,255,0),
        'green'=>$gd->colorAllocate(0,143,63),
        'orange'=>$gd->colorAllocate(255,127,0),
        'purple'=>$gd->colorAllocate(223,0,255)
    };
    my $ai_colors = {
        'white'=>"0.00 0.00 0.00 0.00 K",
        'grey'=> "0.20 0.20 0.20 0.52 K",
        'black'=> "0.00 0.00 0.00 1.00 K",
        'red'=> "0.01 0.96 0.91 0.0 K",
        'blue'=>"0.80 0.68 0.00 0.00 K",
        'yellow'=>"0.03 0.02 0.91 0.00 K",
        'green'=>"0.93 0.05 0.91 0.01 K",
        'orange'=>"0.02 0.50 0.93 0.00 K",
        'purple'=>"0.06 0.93 0.00 0.00 K"
    };
    my $gd_fonts = {
        'small'=>gdSmallFont,  
        'tiny'=>gdTinyFont,
        'large'=>gdLargeFont
    };
    my $ai_fonts = {
        'small'=>10,
        'tiny'=>8,
        'large'=>12
    };

    # Create a brush at an angle
    my $dotted_brush = new GD::Image(2,1);
    my $white = $dotted_brush->colorAllocate(255,255,255);
    my $black = $dotted_brush->colorAllocate(0,0,0);
    $dotted_brush->transparent($white);
    $dotted_brush->line(0,0,1,0,$black); # horiz dotted

    # Set the brush
    $gd->setBrush($dotted_brush);

    $self->{gd} = $gd;
    $self->{ai} = "";
    $self->{gd_colors} = $gd_colors;
    $self->{gd_fonts} = $gd_fonts;
    $self->{ai_colors} = $ai_colors;
    $self->{ai_fonts} = $ai_fonts;
}

sub addBar {
    my ($self,$label,$bar,$link,$link_alt) = @_;
    push @{$self->{bars}},[$label,$bar,$link,$link_alt];
    $self->{x_labels}->{$label} = $bar;
}

sub addPoint {
    my ($self,$label,$value,$link,$link_alt) = @_;
    my $bar = $self->{x_labels}->{$label};
    if (!$bar) {
        die "ERROR: Trying to add point to non existant taxon";
    }
    push @{$bar->{points}}, [$value,$link,$link_alt];
}

# Accepted options:
# range => array ref, i.e. [$y1,$y2]
# link => image map link
# link_alt => image map link alternate text
sub addRangeLabel {
    my ($self,$label,$max,$min,$link,$link_alt) = @_;
    $self->{y_labels}->{$label} = [$max,$min,$link,$link_alt];
}

# Accepted options:
# value => floating point number
# link => image map link
# link_alt => image map link alternate text
sub addTick {
    my ($self,$label,$value,$link,$link_alt) = @_;
    $self->{y_ticks}->{$label} = [$value,$link,$link_alt];
}

sub getRange {
    my $self = shift;
    my $y_axis_order = shift;
    if (! exists $self->{'oldest'}) { 
        my @ages = ();
        foreach my $bar (@{$self->{bars}}) {
            foreach ('first','last','first_short','last_short','first_long','last_long') {
                if ($bar->[1]->{$_} =~ /\d/) {
                    push @ages, $bar->[1]->{$_};
                }
            }
        }
        @ages = ($y_axis_order) 
            ? sort {$b <=> $a} @ages
            : sort {$a <=> $b} @ages;
        $self->{'oldest'} = $ages[$#ages];
        $self->{'youngest'} = $ages[0];

        # This is a bit tricky - We want the labels on the left hand side of the scale to extend beyond the 
        # bars by the minimum possible - to do this we expand the "range" given by oldest-youngest by the
        # minimum amount possible.  So for a timescale based scale, we find the oldest tick mark (age) thats
        # younger than the minimum, and vice versa at the other end
        my @tick_values = 
            map {$_->[0]}
            values %{$self->{'y_ticks'}};
        if ($y_axis_order) {
            my @older_values   = sort {$a <=> $b} grep {$_ < $self->{'oldest'}}   @tick_values;
            my @younger_values = sort {$a <=> $b} grep {$_ > $self->{'youngest'}} @tick_values;
            $self->{'oldest'} = $older_values[$#older_values] if (@older_values);
            $self->{'youngest'} = $younger_values[0] if (@younger_values);
        } else {
            my @older_values   = sort {$a <=> $b} grep {$_ > $self->{'oldest'}}   @tick_values;
            my @younger_values = sort {$a <=> $b} grep {$_ < $self->{'youngest'}} @tick_values;
            $self->{'oldest'} = $older_values[0] if (@older_values);
            $self->{'youngest'} = $younger_values[$#younger_values] if (@younger_values);
        }
    }
    return ($self->{'oldest'},$self->{'youngest'});
}

sub makePixelMapper {
    my ($y_offset,$graph_height,$oldest,$youngest,$y_axis_order) = @_;
    my $range = abs($oldest - $youngest);
    my $pixel_ratio = $graph_height / $range;

    return sub {
        if ($y_axis_order) {
            return ($y_offset + abs($youngest - $_[0])*$pixel_ratio);
        } else {
            return ($y_offset + abs($_[0] - $youngest)*$pixel_ratio);
        }
    }
}

sub stripHTML {
    my $h = shift;
    $h =~ s/<.*?>//g;
    return $h;
}


sub drawGraph {
    my $self = shift;

    my $char_width = 5;
    my $char_height = 8;

    my $bar_spacing = $char_height + 6;
    my $bar_width = 8;
    my $image_map = '<map name="ConfidenceMap">'."\n";

    my $edge_spacing = 20;
    my $label_height = 15;
    my $graph_height = 400;

    my $conf_type = $self->{'conf_type'};
    my $conf_method = $self->{'conf_method'};
    my $y_axis_order = $self->{'y_axis_order'};

    # Get size of Y labels - two types of labels
    # labels of "ranges", defined by two values (i.e. a time interval)
    # and labels of points, defined by one value, add them separately
    my $y_label_size = 1;
    foreach my $label (keys %{$self->{'y_labels'}}) {
        my $text = stripHTML($label);
        my $size = length($text) + 1;
        if ($size > $y_label_size) {
            $y_label_size = $size;
        }
    }

    my $y_tick_size = 1;
    foreach my $label (keys %{$self->{'y_ticks'}}) {
        my $text = $label;
        my $size = length($text) + 1;
        if ($size > $y_tick_size) {
            $y_tick_size = $size;
        }
    }
    
    # Get size of X labels
    my $x_label_size = 0;
    foreach my $label (keys %{$self->{'x_labels'}}) {
        my $text = stripHTML($label);
        my $size = length($text) + 1;
        if ($size > $x_label_size) {
            $x_label_size = $size;
        }
    }

    # Calculate width: Y_labels + 20*bars
    # Calculate height: X labels + fixed
    my $num_bars = scalar(@{$self->{'bars'}});
    my $y_offset = $x_label_size*$char_width + 2*$edge_spacing;
    my $x_offset = $y_label_size*$char_width + $y_tick_size*$char_width + $edge_spacing + $label_height;
    my $fig_width = $x_offset + ($num_bars + 1)*$bar_spacing + $edge_spacing;
    my $fig_height = $y_offset + $graph_height + $edge_spacing;

    $self->initCanvas($fig_width,$fig_height);

    # First Major labels
    if ($self->{'y_axis_label'}) {
        $self->stringUp('large', $edge_spacing - 8, $y_offset + int(($fig_height-$y_offset)/2),$self->{'y_axis_label'},'black');
    }
    if ($self->{'y_axis_unit'}) {
        my $unit =  $self->{"y_axis_unit"};
        $self->string('small', $x_offset - $char_width*length($unit), $y_offset - 2*$char_height,$self->{'y_axis_unit'},'black');
    }

    # Initialize bounds and constants
    my ($oldest,$youngest) = $self->getRange($y_axis_order);
	dbg("ORDER:$y_axis_order RANGE:$oldest to $youngest X_LABELS2ZE:$x_label_size Y_LABELSIZE:$y_label_size:$y_tick_size Y_OFFSET:$y_offset X_OFFSET:$x_offset");
    my $toPixel = makePixelMapper($y_offset,$graph_height,$oldest,$youngest,$y_axis_order);

    # Draw total border
    $self->rectangle(0,0,$fig_width - 1,$fig_height- 1,'black');
    
    # Draw y axis
    $self->line($x_offset,int($toPixel->($youngest)),$x_offset,int($toPixel->($oldest)),'black');
   
    # And draw tickmarks as well as labels for them
    my @y_ticks = keys %{$self->{'y_ticks'}};
    @y_ticks = ($y_axis_order) 
        ? sort {$self->{'y_ticks'}->{$b}->[0] <=> $self->{'y_ticks'}->{$a}->[0]} @y_ticks
        : sort {$self->{'y_ticks'}->{$a}->[0] <=> $self->{'y_ticks'}->{$b}->[0]} @y_ticks;
    my $last_bottom = -1;
    my $last_tick   = -1;
    my $tick_width = $y_tick_size*$char_width;
    foreach my $label (@y_ticks) {
        my ($value,$link,$link_alt) = @{$self->{y_ticks}->{$label}};


        next unless (($value <= $oldest && $value >= $youngest) || 
                     ($value >= $oldest && $value <= $youngest));

       
        # Draw label if theres room
        my $text_top = round($toPixel->($value) - .5*$char_height);
        if ($text_top < $last_bottom) {
            next;
        }

        # Draw tick mark
        my $tick_top = round($toPixel->($value));
        $self->line($x_offset - 3,$tick_top,$x_offset,$tick_top,'black');

        my $length = length($label);
        $self->string('tiny',$x_offset - ($length+1)*$char_width,$text_top,$label,'black');

        if ($link) {
            my $x1 = $x_offset - ($length+1)*$char_width;
            my $y1 = $text_top;
            my $x2 = $x_offset;
            my $y2 = $text_top + $char_height;
            $image_map .= qq|<area shape=rect coords="$x1,$y1,$x2,$y2" href="$link" alt="$link_alt">\n|;
        }
        $last_bottom = $text_top + $char_height;
    }

    # Draw Y labels
    my @y_labels = keys %{$self->{'y_labels'}};
    @y_labels = ($y_axis_order)
        ? sort {$self->{'y_labels'}->{$b}->[0] <=> $self->{'y_labels'}->{$a}->[0]} @y_labels
        : sort {$self->{'y_labels'}->{$a}->[0] <=> $self->{'y_labels'}->{$b}->[0]} @y_labels;
    $last_bottom = -1;
    foreach my $label (@y_labels) {
        my @values = @{$self->{y_labels}->{$label}};
        unless ($values[0] <= $oldest && $values[1] >= $youngest) {
            next;
        }
        my $mid_point = ($values[0] + $values[1])/2;
        my $top = round($toPixel->($mid_point) - .5*$char_height);
        if ($top < $last_bottom) {
            next;
        }
        my $length = length($label);
        $self->string('tiny',$x_offset - $tick_width - $length*$char_width,$top,$label,'black');

        my $link = $values[2];
        my $link_alt = $values[3];
        if ($link) {
            my $x1 = $x_offset - $tick_width - $length*$char_width;
            my $y1 = $top;
            my $x2 = $x_offset - $tick_width;
            my $y2 = $top + $char_width;
            $image_map .= qq|<area shape=rect coords="$x1,$y1,$x2,$y2" href="$link" alt="$link_alt">\n|;
        }
        $last_bottom = $top + $char_height;
    }

    # Draw X Labels
    my @bars = @{$self->{'bars'}};
    for(my $i=0;$i<@bars;$i++) {
        my $label = $bars[$i]->[0];
        my $bar   = $bars[$i]->[1];
        my $link  = $bars[$i]->[2];
        my $link_alt= $bars[$i]->[3];

        my $bar_left    = $x_offset + ($i+1)*$bar_spacing - 3;
        my $bar_right   = $bar_left + $bar_width;

        if ($bar->{'correlation'}) {
            $label = $label;
        }
        $self->stringUp('small', $bar_left, $y_offset - $edge_spacing, $label, 'black');

        if ($link) {
            $image_map .= qq|<area shape=rect coords="$bar_left,15,$bar_right,$y_offset" href="$link" alt="$link_alt">\n|;
        }
    }

    # Draw bars & occurrences within them
    my $half_unit_size = round(abs($toPixel->(1) - $toPixel->(0))/2);
    my $min_bar_top = 999999;
    my $max_bar_bottom = 0;
    for(my $i=0;$i<@bars;$i++) {
        my $label = $bars[$i]->[0];
        my $bar   = $bars[$i]->[1];
        
        my ($last,$first,$last_long,$first_long,$last_short,$first_short) = 
            ($bar->{'last'},$bar->{'first'},$bar->{'last_long'},$bar->{'first_long'},$bar->{'last_short'},$bar->{'first_short'});
        if ($self->{'y_axis_type'} =~ /discrete/) {
            if ($y_axis_order) {
                $last += .5;
                $last_short += .5 if ($last_short);
                $last_long += .5 if ($last_long);
                $first -= .5;
                $first_short -= .5 if ($first_short);
                $first_long -= .5 if ($first_long);
            } else {
                $last -= .5;
                $last_short -= .5 if ($last_short);
                $last_long -= .5 if ($last_long);
                $first += .5;
                $first_short += .5 if ($first_short);
                $first_long += .5 if ($first_long);
            }
        }
        my $bar_top     = round($toPixel->($last));
        my $bar_bottom  = round($toPixel->($first));
        my $bar_left    = $x_offset + ($i+1)*$bar_spacing;
        my $bar_right   = $bar_left + $bar_width;

        $min_bar_top = $bar_top if ($bar_top < $min_bar_top);
        $max_bar_bottom = $bar_bottom if ($bar_bottom > $max_bar_bottom);

#        print "TOP $bar_top BOTTOM $bar_bottom LEFT $bar_left RIGHT $bar_right ";
#        foreach my $k (sort keys %$bar) {
#            if (! ref $bar->{$k}) {
#                my $v = $bar->{$k};
#                print uc($k)."=$bar->{$k}";
#                if (($v <= $oldest && $v >= $youngest) ||
#                    ($v >= $oldest && $v <= $youngest)) {
#                    print ":".round($toPixel->($v));
#                }
#                print " ";
#            }            
#        }
#        print "<BR>";
      
        # Draw the glyphs
        foreach my $point (@{$bar->{'points'}}) {
            my ($value,$link,$link_alt)= @$point;
            my ($min,$avg,$max);
            if (ref $value) {
                ($max,$min) = ($value->[0],$value->[1]);
                $avg = ($max+$min)/2;
                $max = round($toPixel->($max));
                $min = round($toPixel->($min));
                $avg = round($toPixel->($avg));
            } else {
                $value = round($toPixel->($value)); 
                ($min,$avg,$max) = ($value+$bar_width/2,$value,$value-$bar_width/2);
            }

            if ($self->{'glyph_type'} eq 'circles') {
                $self->filledCircle(($bar_right+$bar_left)/2,$avg,$bar_width,$self->{'color'});
            } elsif ($self->{'glyph_type'} eq 'hollow circles') {
                $self->circle(($bar_right+$bar_left)/2,$avg,$bar_width,$self->{'color'});
            } elsif ($self->{'glyph_type'} eq 'squares') {
                $self->filledRectangle($bar_left,$avg-$bar_width/2,$bar_right,$avg+$bar_width/2,$self->{'color'});
            } elsif ($self->{'glyph_type'} eq 'hollow squares') {
                $self->rectangle($bar_left,$avg-$bar_width/2,$bar_right,$avg+$bar_width/2,$self->{'color'});
            } else {
                $self->filledRectangle($bar_left,$min,$bar_right,$max,$self->{'color'});
            }

            if ($link) {
                if ($self->{'glyph_type'} =~ /circles|squares/) {
                    my $top     = $avg + $bar_width/2;
                    my $bottom  = $avg - $bar_width/2;
                    $image_map .= qq|<area shape="rect" coords="$bar_left,$top,$bar_right,$bottom" href="$link" alt="$link_alt">\n|;
                } else {
                    $image_map .= qq|<area shape="rect" coords="$bar_left,$max,$bar_right,$min" href="$link" alt="$link_alt">\n|;
                }
            }
        }

        # Draw actual range
        $self->rectangle($bar_left,$bar_top,$bar_right,$bar_bottom,'black');

        # Draw bars
        if ($conf_method ne "Solow (1996)") {
            my $c_bottom       = round($toPixel->($first_long));
            my $c_short_bottom = round($toPixel->($first_short));
            my $c_top          = round($toPixel->($last_long));
            my $c_short_top    = round($toPixel->($last_short));
            
            my $triangle_first = ($first_long !~ /\d/ && $first_short =~ /\d/ && $conf_method eq 'Marshall (1994)') ? 1 : 0;
            my $triangle_last  = ($last_long  !~ /\d/ && $last_short  =~ /\d/ && $conf_method eq 'Marshall (1994)') ? 1 : 0;

    #        if ($barup == 50)   {
    #            $recent = 1;  
    #        } elsif ($limup <= 50) {
    #            $recent = 2;
    #        } else  {
                if ($conf_type =~ /last appearance|total duration/) {
#                    print "RECTANGLE: ".($bar_left+int($bar_width/2)).",".$c_top.",".($bar_left+int($bar_width/2)+1).",".$bar_top."<BR>";
                    if ($last_long =~ /\d/) {
                        $self->rectangle($bar_left + int($bar_width/2),$c_top,$bar_left + int($bar_width/2) + 1,$bar_top, 'black');
                        $self->line($bar_left,$c_top,$bar_right,$c_top, 'black'); 
                    }
                    if ($last_short =~ /\d/) {
                        unless ($last_long =~ /\d/) {
                            $self->rectangle($bar_left + int($bar_width/2),$c_short_top,$bar_left + int($bar_width/2) + 1,$bar_top, 'black');
                        }
                        $self->line($bar_left,$c_short_top,$bar_right,$c_short_top, 'black') if ($c_short_top);
                    }
                    if ($triangle_last) {
                        $self->filledTriangle(
                            $bar_left,$c_short_top,
                            $bar_right,$c_short_top,
                            ($bar_left+$bar_right)/2,$c_short_top-abs(int($bar_left-$bar_right)/2),
                            'black');
                    }
                }
    #        }
            if ($conf_type =~ /first appearance|total duration/) {
                if ($first_long =~ /\d/) {
                    $self->rectangle($bar_left + int($bar_width/2),$bar_bottom,$bar_left+ int($bar_width/2)+1,$c_bottom, 'black');
                    $self->line($bar_left,$c_bottom,$bar_right,$c_bottom, 'black'); 
                }
                if ($first_short =~ /\d/) {
                    unless ($first_long =~ /\d/) {
                        $self->rectangle($bar_left + int($bar_width/2),$bar_bottom,$bar_left+ int($bar_width/2)+1,$c_short_bottom, 'black');
                    }
                    $self->line($bar_left,$c_short_bottom,$bar_right,$c_short_bottom, 'black') if ($c_short_bottom);
                }
                if ($triangle_first) {
                    $self->filledTriangle(
                        $bar_left,$c_short_bottom,
                        $bar_right,$c_short_bottom,
                        ($bar_left+$bar_right)/2,$c_short_bottom+abs(int($bar_left-$bar_right)/2),
                        'black');
                }
            }
        }
    }
    
    # Draw confidence bars
    if ($conf_method eq "Solow (1996)") {
        my $box_left    = $x_offset + $bar_spacing;
        my $box_right   = $x_offset + (scalar(@{$self->{'bars'}}))*$bar_spacing + $bar_width;
        my $box_bottom  = $max_bar_bottom;
        my $box_top     = $min_bar_top;

        my $first_long = $self->{'bars'}->[0]->[1]->{'first_long'};
        my $last_long  = $self->{'bars'}->[0]->[1]->{'last_long'};
        my $center = int(($box_left + $box_right)/2);

        if ($last_long =~ /\d/) {
            my $c_top = round($toPixel->($last_long));
            my $first_top = round($toPixel->($self->{'bars'}->[0]->[1]->{'last'}));
            my $last_top  = round($toPixel->($self->{'bars'}->[-1]->[1]->{'last'}));
            $self->dashedLine($box_left,$box_top,$box_right,$box_top,'black');
            $self->dashedLine($box_left,$box_top,$box_left,$first_top,'black');
            $self->dashedLine($box_right,$box_top,$box_right,$last_top,'black');

            if (($box_top - $c_top) < 0) {
                $self->filledTriangle($center -2,$box_top,$center + 3,$box_top,$center,$box_top-3,'black');
            } else {
                $self->line($center-3,$c_top,$center+3,$c_top,'black'); 
            }
            $self->line($center-3,$c_top,$center+3,$c_top,'black'); 
            $self->rectangle($center, $c_top, $center + 1,$box_top,'black');
            
        }
    
        if ($first_long =~ /\d/) {
            my $c_bottom     = round($toPixel->($first_long));
            my $first_bottom = round($toPixel->($self->{'bars'}->[0]->[1]->{'first'}));
            my $last_bottom  = round($toPixel->($self->{'bars'}->[-1]->[1]->{'first'}));

            $self->dashedLine($box_left,$box_bottom,$box_right,$box_bottom,'black');
            $self->dashedLine($box_left,$first_bottom,$box_left,$box_bottom,'black');
            $self->dashedLine($box_right,$last_bottom,$box_right,$box_bottom,'black');

            $self->line($center-3,$c_bottom,$center+3,$c_bottom,'black'); 
            $self->rectangle($center, $box_bottom, $center + 1,$c_bottom,'black');
        }
    } 

    # Finish up
#    $self->string('small', 90, 200, 'Ma', 'black');
    $self->string('tiny', $fig_width - 70,$fig_height - 10, "J. Madin 2004", 'black');
    

    # Export and move on
    my $image_count = getImageCount();

    my $image_name = "confimage$image_count";
    open(AI,">$IMAGE_DIR/$image_name.ai");
    open(AIHEAD,"<./data/AI.header");
    while (<AIHEAD>)	{
        print AI $_;
    }
    close AIHEAD;
    print AI $self->{ai};
    open AIFOOT,"<./data/AI.footer";
    while (<AIFOOT>) {
        print AI $_;
    };
    close AIFOOT;

    my $gd = $self->{'gd'};
    open(IMAGEJ, ">$IMAGE_DIR/$image_name.jpg");
    print IMAGEJ $gd->jpeg;
    close IMAGEJ;
    
    open(IMAGEP, ">$IMAGE_DIR/$image_name.png");
    print IMAGEP $gd->png;
    close IMAGEP;

    $image_map .= "</map>\n";

    return ($image_map,$image_name);
}

sub line {
    my $self = shift;
    my ($x1,$y1,$x2,$y2,$color) = @_;

    $self->{gd}->line($x1,$y1,$x2,$y2,$self->{gd_colors}->{$color});
    
    $self->{ai} .= "$self->{ai_colors}->{$color}\n";
    $self->{ai} .= "[]0 d\n";
    $self->{ai} .= sprintf "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
    $self->{ai} .= sprintf "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y2;
    $self->{ai} .= "S\n";
}

sub dashedLine {
    my $self = shift;
    my ($x1,$y1,$x2,$y2,$color) = @_;
    $self->{gd}->setStyle($self->{gd_colors}->{$color},$self->{gd_colors}->{'white'});
    $self->{gd}->line($x1,$y1,$x2,$y2,gdStyled);
    $self->{ai} .= "$self->{ai_colors}->{$color}\n";
    $self->{ai} .= "[6 ]0 d\n";
    $self->{ai} .= sprintf "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
    $self->{ai} .= sprintf "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y2;
    $self->{ai} .= "S\n";
}


sub circle {
    my $self = shift;
    my ($x,$y,$diam,$color) = @_;

    $self->{gd}->arc($x,$y,$diam,$diam,0,360,$self->{gd_colors}->{$color});

    my $rad = $diam / 2;
    my $aix = $AILEFT+$x+$rad;
    my $aiy = $AITOP-$y;
    my $obl = $diam * 0.27612;
    $self->{ai} .= "$self->{ai_colors}->{$color}\n";
    $self->{ai} .= sprintf "%.1f %.1f m\n",$aix,$aiy;
    $self->{ai} .= sprintf "%.1f %.1f %.1f %.1f %.1f %.1f c\n",$aix,$aiy-$obl,$aix-$rad+$obl,$aiy-$rad,$aix-$rad,$aiy-$rad;
    $self->{ai} .= sprintf "%.1f %.1f %.1f %.1f %.1f %.1f c\n",$aix-$rad-$obl,$aiy-$rad,$aix-$diam,$aiy-$obl,$aix-$diam,$aiy;
    $self->{ai} .= sprintf "%.1f %.1f %.1f %.1f %.1f %.1f c\n",$aix-$diam,$aiy+$obl,$aix-$rad-$obl,$aiy+$rad,$aix-$rad,$aiy+$rad;
    $self->{ai} .= sprintf "%.1f %.1f %.1f %.1f %.1f %.1f c\n",$aix-$rad+$obl,$aiy+$rad,$aix,$aiy+$obl,$aix,$aiy;
    $self->{ai} .= "S\n";
}

sub filledCircle {
    my $self = shift;
    my ($x,$y,$diam,$color) = @_;

    $self->{gd}->filledArc($x,$y,$diam,$diam,0,360,$self->{gd_colors}->{$color});

    my $rad = $diam / 2;
    my $aix = $AILEFT+$x+$rad;
    my $aiy = $AITOP-$y;
    my $obl = $diam * 0.27612;
    my $aiStrokeColor = $self->{ai_colors}->{$color};
    my $aiFillColor   = $self->{ai_colors}->{$color};
    $aiFillColor =~ s/K/k/;
    $self->{ai} .= "$aiStrokeColor\n";
    $self->{ai} .= "$aiFillColor\n";
    $self->{ai} .= sprintf "%.1f %.1f m\n",$aix,$aiy;
    $self->{ai} .= sprintf "%.1f %.1f %.1f %.1f %.1f %.1f c\n",$aix,$aiy-$obl,$aix-$rad+$obl,$aiy-$rad,$aix-$rad,$aiy-$rad;
    $self->{ai} .= sprintf "%.1f %.1f %.1f %.1f %.1f %.1f c\n",$aix-$rad-$obl,$aiy-$rad,$aix-$diam,$aiy-$obl,$aix-$diam,$aiy;
    $self->{ai} .= sprintf "%.1f %.1f %.1f %.1f %.1f %.1f c\n",$aix-$diam,$aiy+$obl,$aix-$rad-$obl,$aiy+$rad,$aix-$rad,$aiy+$rad;
    $self->{ai} .= sprintf "%.1f %.1f %.1f %.1f %.1f %.1f c\n",$aix-$rad+$obl,$aiy+$rad,$aix,$aiy+$obl,$aix,$aiy;
    $self->{ai} .= "b\n";
}

sub rectangle {
    my $self = shift;
    my ($x1,$y1,$x2,$y2,$color) = @_;

    $self->{gd}->rectangle($x1,$y1,$x2,$y2,$self->{gd_colors}->{$color});

    $self->{ai} .= "$self->{ai_colors}->{$color}\n";
    $self->{ai} .= sprintf "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
    $self->{ai} .= sprintf "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y1;
    $self->{ai} .= sprintf "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y2;
    $self->{ai} .= sprintf "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y2;
    $self->{ai} .= sprintf "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y1;
    $self->{ai} .= "S\n";
}

sub filledRectangle {
    my $self = shift;
    my ($x1,$y1,$x2,$y2,$color) = @_;

    $self->{gd}->filledRectangle($x1,$y1,$x2,$y2,$self->{gd_colors}->{$color});

    $self->{ai} .= "0 O\n";
    my $aiStrokeColor = $self->{ai_colors}->{$color};
    my $aiFillColor   = $self->{ai_colors}->{$color};
    $aiFillColor =~ s/K/k/;
    $self->{ai} .= "$aiStrokeColor\n";
    $self->{ai} .= "$aiFillColor\n";
    $self->{ai} .= "4 M\n";
    $self->{ai} .= sprintf "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
    $self->{ai} .= sprintf "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y1;
    $self->{ai} .= sprintf "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y2;
    $self->{ai} .= sprintf "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y2;
    $self->{ai} .= sprintf "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y1;
    $self->{ai} .= "b\n";
}

sub filledTriangle {
    my $self = shift;
    my ($x1,$y1,$x2,$y2,$x3,$y3,$color) = @_;
    my $poly = new GD::Polygon;
    $poly->addPt($x1,$y1);
    $poly->addPt($x2,$y2);
    $poly->addPt($x3,$y3);

    # draw the polygon, filling it with a color
    $self->{gd}->filledPolygon($poly,$self->{gd_colors}->{$color});

    $self->{ai} .= "0 O\n";
    my $aiStrokeColor = $self->{ai_colors}->{$color};
    my $aiFillColor   = $self->{ai_colors}->{$color};
    $aiFillColor =~ s/K/k/;
    $self->{ai} .= "$aiStrokeColor\n";
    $self->{ai} .= "$aiFillColor\n";
    $self->{ai} .= "4 M\n";
    $self->{ai} .= sprintf "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
    $self->{ai} .= sprintf "%.1f %.1f L\n",$AILEFT+$x2,$AITOP-$y2;
    $self->{ai} .= sprintf "%.1f %.1f L\n",$AILEFT+$x3,$AITOP-$y3;
    $self->{ai} .= sprintf "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y1;
    $self->{ai} .= "b\n";
}

sub stringUp {
    my $self = shift;
    my ($font,$x,$y,$text,$color) = @_;

    my $aiFillColor   = $self->{ai_colors}->{$color};
    $aiFillColor =~ s/K/k/;
    $self->{gd}->stringUp($self->{gd_fonts}->{$font},$x,$y,$text,$self->{gd_colors}->{$color});
    $self->{ai} .= "0 To\n";
    $self->{ai} .= sprintf "0 1 -1 0 %.1f %.1f 0 Tp\nTP\n",$AILEFT+$x+10,$AITOP-$y;
    $self->{ai} .= sprintf "0 Tr\n0 O\n%s\n",$aiFillColor;
    $self->{ai} .= sprintf "/_Courier %.1f Tf\n",$self->{ai_fonts}->{$font}; 
    $self->{ai} .= sprintf "0 Tw\n";
    $self->{ai} .= "($text) Tx 1 0 Tk\n";
    $self->{ai} .= "(\r) Tx 1 0 Tk\nTO\n";
}

sub string {
    my $self = shift;
    my ($font,$x,$y,$text,$color) = @_;

    $self->{gd}->string($self->{gd_fonts}->{$font},$x,$y,$text,$self->{gd_colors}->{$color});

    my $aiFillColor   = $self->{ai_colors}->{$color};
    $aiFillColor =~ s/K/k/;
    $self->{ai} .= "0 To\n";
    $self->{ai} .= sprintf "1 0 0 1 %.1f %.1f 0 Tp\nTP\n",$AILEFT+$x,$AITOP-$y;
    $self->{ai} .= sprintf "0 Tr\n0 O\n%s\n",$aiFillColor;
    $self->{ai} .= sprintf "/_Courier %.1f Tf\n",$self->{ai_fonts}->{$font}; 
    $self->{ai} .= sprintf "0 Tw\n";
    $self->{ai} .= "($text) Tx 1 0 Tk\n";
    $self->{ai} .= "(\r) Tx 1 0 Tk\nTO\n";
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
        die( "Couldn't open [$IMAGE_DIR/imagecount]: $!" );
    }
    my $image_count = <IMAGECOUNT>;
    chomp($image_count);
    close IMAGECOUNT;
                                                                                                                                                             
    $image_count++;
    if ( ! open IMAGECOUNT,">$IMAGE_DIR/imagecount" ) {
          die( "Couldn't open [$IMAGE_DIR/imagecount]: $!" );
    }
    print IMAGECOUNT "$image_count";
    close IMAGECOUNT;
    
    $image_count++;
    return $image_count;
}

sub round {
    return int($_[0]+.5)
}


1;
