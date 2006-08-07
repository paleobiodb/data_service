package TypoChecker;

use Debug;
use Data::Dumper;
use strict;


# start the process to get to the reclassify occurrences page
# modelled after startAddEditOccurrences
sub searchOccurrenceMisspellingForm {
	my ($dbt,$q,$s,$hbo,$message,$no_header) = @_;
    my $dbh = $dbt->dbh;

	if (!$s->isDBMember()) {
	    # have to be logged in
		$s->enqueue( $dbh, "action=searchOccurrenceMisspellingForm" );
		main::displayLoginPage( "Please log in first." );
        return;
	} 

    my %vars = $q->Vars();
    $vars{'enterer_me'} = $s->get('enterer_reversed');
    $vars{'submit'} = "Search for reidentifications";
    unless ($no_header) {
        $vars{'page_title'} = "Misspelled occurrence search form";
    }
    $vars{'action'} = "occurrenceMisspellingForm";
    $vars{'page_subtitle'} = $message;


    # Spit out the HTML
    print main::makeAuthEntJavaScript();
    main::printIntervalsJava(1);
    my $html = $hbo->populateHTML('search_occurrences_form',\%vars);
    if ($no_header) {
        $html =~ s/forms\[0\]/forms[1]/g;
    }
    print $html;
}

#TODO? filter out where taxon_no is 0 bc of homonym

sub getNameData {
    my ($dbt,$name,$period_lookup,$period_order,$can_modify) = @_;
    my $dbh = $dbt->dbh;
    my ($g,$sg,$sp,$ssp) = Taxon::splitTaxon($name);
    my $where = 'o.genus_name='.$dbh->quote($g); 
    if ($sg || $sp) {
        if ($sg) {
            $where .= ' AND o.subgenus_name='.$dbh->quote($sg) 
        } else {
            $where .= " AND (o.subgenus_name IS NULL or o.subgenus_name='')";
        }
    }
    if ($sp) {
        $where .= ' AND o.species_name='.$dbh->quote($sp);
    }

    my $fields = 'o.collection_no,o.authorizer_no,o.occurrence_no,o.genus_name,o.subgenus_name,o.species_name,o.taxon_no,c.max_interval_no,c.min_interval_no,c.country,c.state';
    my $sql = "(SELECT $fields,0 reid_no FROM (occurrences o, collections c) LEFT JOIN authorities a ON (o.taxon_no=a.taxon_no) WHERE (a.taxon_rank IS NULL OR a.taxon_rank NOT LIKE '%species%') AND o.collection_no=c.collection_no AND $where)";
    $sql .= " UNION ";
    $sql .=  "(SELECT $fields, o.reid_no FROM (reidentifications o, collections c) LEFT JOIN authorities a ON (o.taxon_no=a.taxon_no) WHERE (a.taxon_rank IS NULL OR a.taxon_rank NOT LIKE '%species%') AND o.collection_no=c.collection_no AND $where)";
#    main::dbg("SQL: ".$sql);
    my @results = @{$dbt->getData($sql)};

    my %periods;
    my %countries;
    my %collections;
    my %collections_no_edit;
    my @occs;
    my @reids;

    foreach my $row (@results) {
        my $permission_to_edit = 0;
        if ($can_modify->{$row->{'authorizer_no'}}) {
            $permission_to_edit = 1;
        }

        if ($permission_to_edit) {
            if ($row->{'reid_no'}) {
                push @reids,$row->{'reid_no'};
            } else {
                push @occs, $row->{'occurrence_no'};
            }
            $collections{$row->{'collection_no'}} = 1;
        } else {
            $collections_no_edit{$row->{'collection_no'}} = 1;
        }
        my $period = $period_lookup->{$row->{'max_interval_no'}};
        $periods{$period} = 1;
        $countries{$row->{'country'}} = 1 if ($row->{'country'});
    }

    my %name = ();
    if (@occs || @reids) {
        $name{'permission_to_edit'} = 1;
    }
    $name{'countries'} = join(", ",sort keys %countries);
    my @periods;
    foreach my $p (@$period_order) {
        push @periods, $p if ($periods{$p});
    }
    $name{'periods'} = join(", ",@periods);
    $name{'occurrence_list'} = join(",",@occs);
    $name{'reid_list'} = join(",",@reids);
    my @collections;
    foreach (sort{$a <=> $b} keys %collections) {
        my $link = "<b><a target=\"_COLWINDOW\" href=\"bridge.pl?action=displayCollectionDetails&collection_no=$_\">$_</a></b>";
        push @collections, $link;
    }
    $name{'collections'} = join(", ",@collections);

    @collections = ();
    foreach (sort{$a <=> $b} keys %collections_no_edit) {
        my $link = "<a target=\"_COLWINDOW\" href=\"bridge.pl?action=displayCollectionDetails&collection_no=$_\">$_</a>";
        push @collections, $link;
    }
    $name{'collections_no_edit'} = join(", ",@collections);
    return \%name;
}

# Provides a list of misspellings associated with a group of collections or a taxonomic name.
# In the future maybe expand functionalit of the names to be more than simple proximate names - i.e.
# "Find anything that is vaguely like a a gastropod".
# The program has two main loops - first loop gets a list of taxonomic names - don't do ANY filtering
# at this point other than filtering out things tied at specie level (data is considered good at that point).
# We will want to get occurrences that fall outside the filter, and we'll have to do a separate query at a later date
# Also, much of the filtering options can't be applied until we get a list of suggesstions
sub occurrenceMisspellingForm {
	my ($dbt,$q,$s,$hbo) = @_;
    my $dbh = $dbt->dbh;

    my $show_detail = $q->param('show_detail');

    # First block - get a simple list of potential names at the maxium resolution we can (filter out informals and
    # indet and sp)
    my @names;
    if ($q->param('taxon_name')) {
        my $names_hash = taxonTypoCheck($dbt,$q->param("taxon_name"));
        @names = keys(%{$names_hash});
        push @names, $q->param('taxon_name');
    } else {
        my %options = $q->Vars();
        # Do a looser match against occurrences/reids tables only
        $options{'limit'} = 10000000;
        my ($dataRows,$ofRows,$warnings) = main::processCollectionsSearch($dbt,\%options,['collection_no']);  
        my @collection_nos = map {$_->{'collection_no'}} @$dataRows;

        my $fields = 'a.taxon_rank,a.taxon_name,o.genus_reso,o.genus_name,o.subgenus_reso,o.subgenus_name,o.species_reso,o.species_name,o.taxon_no';
        if (@collection_nos) {
            my $sql = "(SELECT $fields FROM collections c, occurrences o LEFT JOIN authorities a ON (o.taxon_no=a.taxon_no) WHERE (a.taxon_rank IS NULL OR a.taxon_rank NOT LIKE '%species%') AND o.collection_no=c.collection_no AND o.genus_name != '' AND c.collection_no IN (".join(",",@collection_nos).") GROUP BY genus_name,subgenus_name,species_name)";
            $sql .= " UNION ";
            $sql .=  "(SELECT $fields FROM collections c, reidentifications o LEFT JOIN authorities a ON (o.taxon_no=a.taxon_no) WHERE (a.taxon_rank IS NULL OR a.taxon_rank NOT LIKE '%species%') AND o.collection_no=c.collection_no AND o.genus_name != '' AND c.collection_no IN (".join(",",@collection_nos).") GROUP BY genus_name,subgenus_name,species_name)";
            main::dbg("SQL: ".$sql);
            my %seen;
            foreach my $row (@{$dbt->getData($sql)}) {
                next if ($row->{'genus_reso'} =~ /informal/);
                my $name = $row->{'genus_name'};
                if ($row->{'subgenus_name'} && $row->{'subgenus_reso'} !~ /informal/) {
                    $name .= " ($row->{'subgenus_name'})";
                }
                if ($row->{'species_name'} !~ /^sp\.|^sp$|^indet\.|^indet$/ && $row->{'species_reso'} !~ /informal/) {
                    $name .= " $row->{'species_name'}";
                } else {
                    # The occ is tied at the genus or higher level, but its a sp. or indet. species, so this is as good as it
                    # gets... we can safely skip this one
                    if ($row->{'taxon_rank'}) {
                        next;
                    }
                }
                push @names, $name if (!$seen{$name});
                $seen{$name} = 1;
            }
        } else {
            @names = ();
        }
    }
    @names = sort {$a cmp $b} @names;
    main::dbg("FOUND ".scalar(@names).' unique names');

    # Some static lookup tables are generated first
    my @period_order = TimeLookup::getScaleOrder($dbt,'69');
    my $period_lookup= TimeLookup::processScaleLookup($dbh,$dbt,'69','intervalToScale');
    my $p = Permissions->new($s,$dbt);
    my $can_modify = $p->getModifierList();
    my $authorizer_no = $s->get('authorizer_no');
    $can_modify->{$authorizer_no} = 1;
   
    # Now the second loop - we're going to get suggestions for each name and then determine whether
    # or not to proceed.  If we do proceed, print out all the relevant information formatted nicely. 
    # No matter what we always want to display 15 rows if we can, and may have to skip 100 rows
    # that don't apply in the meantime.  This can be a bit slow, maybe speed this up in the future
    my $offset = (int($q->param("offset"))) ? int($q->param('offset')) : 0;
    my $limit = (int($q->param("limit"))) ? int($q->param('limit')) : 15;
    my $name_count = scalar(@names);
    if (@names) {
        print '<div align="center">';
        if ($show_detail eq 'typos') {
            print '<h2>Possibly misspelled occurrences</h2>';
        } elsif ($show_detail eq 'unclassified') {
            print '<h2>Possibly misspelled/unclassified occurrences</h2>';
        } else {
            print '<h2>Possibly misspelled/partially classified occurrences</h2>';
        }
        print '<form action="bridge.pl" method="POST">';
        print '<input type="hidden" name="action" value="submitOccurrenceMisspelling">';
        my $page_no = (int($q->param('page_no'))) ? int($q->param('page_no')) : 0;
        print '<input type="hidden" name="page_no" value="'.($page_no+1).'">';
        print '<table cellpadding="2" cellspacing="0" width="100%">';
        my $class = '';
        my $skip_unclassified = 0;
        my $skip_genus_classified = 0;
        my $skip_other = 0;
        my $displayed_results = 0;
        for (my $i = $offset; $i < ($offset+$limit+$skip_other+$skip_unclassified+$skip_genus_classified) && $i < $name_count; $i++) {
            my $name = $names[$i];
            my ($g,$sg,$sp) = Taxon::splitTaxon($name);
           
            # Useful below
            my @taxa = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$g},['taxon_no']);
            my $genus_is_classified = (@taxa) ? 1 : 0;

            my $suggest_hash = taxonTypoCheck($dbt,$name,$genus_is_classified);

            # Grab the suggesstion list now and determine if we want to continue
            my @suggestions = keys(%{$suggest_hash});
            if (!@suggestions) {
                if ($show_detail =~ /unclassified|typos/) {
                    if ($genus_is_classified) {
                        main::dbg("Skipping $name, genus is classified, no suggestions");
                        $skip_unclassified++;
                        next;
                    }
                }
                if ($show_detail eq 'typos') {
                    main::dbg("Skipping $name, no suggesstions");
                    $skip_genus_classified++;
                    next;
                }
            }

            # Grab and compile all the data, also a few more skip conditions below
            my $name_data = getNameData($dbt,$name,$period_lookup,\@period_order,$can_modify);
            if ($q->param('edit_only') && !$name_data->{'permission_to_edit'}) {
                main::dbg("Skipping $name, no permission to edit");
                $skip_other++;
                next;
            }

            $class = ($class) ? '' : 'class="darkList"';

            # Handle printing of summary row
            my $row = "<tr $class>";
            $row .= "<td> $name ";
            $row .= " <small> ";
            if ($genus_is_classified) {
                $row .= "$g is classified - ";
            }
            $row .= $name_data->{'countries'}.' - ';
            $row .= $name_data->{'periods'}.' - ';
            
            if ($genus_is_classified) {
                if ($sp) {
                    $row .= "<a target=\"ADDWINDOW\" href=\"bridge.pl?action=displayAuthorityForm&taxon_no=-1&taxon_name=$name\">Add an authority record</a>";
                } else {
                    # Skipping because its a genus_name only, and the genus exists in teh authorities table
                        if (!@suggestions) {
                        $skip_other++;
                        main::dbg("Skipping $name, genus is classified and no species part");
                        # Undo this action
                        $class = ($class) ? '' : 'class="darkList"';
                        next;
                    }
                }
            } else {
                $row .= "<a target=\"ADDWINDOW\" href=\"bridge.pl?action=displayAuthorityForm&taxon_no=-1&taxon_name=$g\">Add an authority record</a>";
            }
            $row .= "</small></td></tr>";
            $row .= "<tr $class><td>Suggestions: ";
            if (!@suggestions) {
                $row .= "No similar records found."; 
            } else {
                if ($name_data->{'permission_to_edit'}) {
                    my @radios;
                    foreach (@suggestions) {
                        my $radio = "<span style=\"white-space: nowrap\">"
                                  . "<input type=\"radio\" name=\"new_taxon_name_$i\" value=\"$_\">";
                        $radio .= "<a target=\"_TAXONPOPUP\" href=\"bridge.pl?action=checkTaxonInfo&taxon_name=$_\">$_</a>";
                        $radio .= "</span>&nbsp; ";
                        if ($suggest_hash->{$_}{'match_quality'} == 3) {
                            $radio = "<b>$radio</b>";
                        } elsif ($suggest_hash->{$_}{'match_quality'} == 2) {
                            $radio =~ s/>(\w+)\b/><b>$1<\/b>/;
                        }
                        push @radios, $radio;
                    }
                    $row .= "<span style=\"white-space: nowrap\"><input type=\"radio\" name=\"new_taxon_name_$i\" value=\"$name\" checked>leave unchanged &nbsp;</span>";
                    $row .= join(" ",@radios);
                    $row .= "<input type=\"hidden\" name=\"old_taxon_name_$i\" value=\"$name\">";
                    $row .= "<input type=\"hidden\" name=\"occurrence_list_$i\" value=\"$name_data->{occurrence_list}\">";
                    $row .= "<input type=\"hidden\" name=\"reid_list_$i\" value=\"$name_data->{reid_list}\">";
                    $row .= "<input type=\"hidden\" name=\"execute_list\" value=\"$i\">";
                } else {
                    my @links; 
                    foreach (@suggestions) {
                        my $link = "<span style=\"white-space: nowrap\">"
                                 . "<a target=\"_TAXONPOPUP\" href=\"bridge.pl?action=checkTaxonInfo&taxon_name=$_\">$_</a>"
                                 . "</span>&nbsp;";
                        if ($suggest_hash->{$_}{'match_quality'} == 3) {
                            $link= "<b>$link</b>";
                        } elsif ($suggest_hash->{$_}{'match_quality'} == 2) {
                            $link=~ s/>(\w+)\b/<b>$1<\/b>/;
                        }
                        push @links, $link;
                    }
                    $row .= join(" ",@links);
                }
            }
            $row .= "</td></tr>";

            # Handle printing of collection row;
            my $collections = $name_data->{'collections'}.", ".$name_data->{'collections_no_edit'};
            $collections =~ s/^\s*,\s*|\s*,\s*$//g;
            if ($collections) {
                $row .= "<tr $class><td>Collections: $collections</td></tr>";
            } else {
                $row .= "<tr $class><td>Collections: none</td></tr>";
            }
            $displayed_results++;
            print $row;
        }
        print '</table>';
        if ($displayed_results == 0 && $page_no == 0) {
            my $message = "<div align=\"center\"><h4>No results to display, please search again</h4></div>";
            print "</form>";
            searchOccurrenceMisspellingForm($dbt,$q,$s,$hbo,$message,1);
            return;
        } else {
            print '<br><br><input type="submit" name="submit" value="Fix misspellings">';
        }
        my $upper_limit = $offset+$limit;
        my $skip_count = ($skip_unclassified+$skip_genus_classified+$skip_other);
        if ($upper_limit > ($name_count-$skip_count)) {
            $upper_limit = ($name_count-$skip_count);
        }
        if ($upper_limit > $offset) {
            #print '<h4> Here are rows '.($offset+1).' to '.$upper_limit;
            my $lower = ($page_no*$limit+1);
            my $upper = ($page_no*$limit+$limit);
            if ($displayed_results < $limit) {
                $upper = ($page_no*$limit+$displayed_results);
            }
            print "<h4> Here are rows $lower to $upper";
        }
        my %v = $q->Vars();
        $v{'offset'} = ($offset+$limit+$skip_count);
        my @notes;
        if ($v{'offset'} < $name_count) {
            # We want to save previous form values of note until the user presses "submit misspellings"
            # Note that the condition here for saving values must be the same as the condition of submitting
            # to the database in submitOccurrenceMisspellings
            my $saved_search = "";

            my @exec_list = $q->param('execute_list');
            foreach my $i (@exec_list) {
                my $old_name = $q->param("old_taxon_name_$i");
                my $new_name = $q->param("new_taxon_name_$i");
                if ($old_name && $new_name && $old_name ne $new_name) {   
                    $saved_search .= "<input type=\"hidden\" name=\"execute_list\" value=\"$i\">\n";
                    foreach ("old_taxon_name_$i","new_taxon_name_$i","occurrence_list_$i","reid_list_$i") {
                        $saved_search .= "<input type=\"hidden\" name=\"$_\" value=\"".$q->param($_)."\">\n";
                    }
                }
            }

            # We also have to save search parameter to regenerate the search correctly!
            foreach my $f (sort keys %v) {
                if ($f !~ /^action|^new_taxon_name|^old_taxon_name|^reid_list|^occurrence_list|^execute_list|^submit|^skipped/) {
                    $saved_search .= "<input type=\"hidden\" name=\"$f\" value=\"$v{$f}\">\n";
                }
            }
            print $saved_search;
            print " - <input type=\"submit\" name=\"submit\" value=\"Get next $limit\">";
            push @notes, "Any changes made on this page and previous pages will be carried over when you click \"Get next 15,\" but will not be committed to the database until you click \"Fix misspellings.\"";
        }
        print "</h4>";
        print '</div>';

        push @notes, 'If only a genus name is listed in the suggestions, the genus name will be changed but the species left the same. Bolded taxon names are names that exist in both the the occurrences and authority tables. Bolded collection names have occurrences which you have permission to edit, while unbolded names won\'t be changed. Suggestions are generated from existing occurrences and authorities records, so if you see a typo there you can click the name to see the source of the suggestion and track down the authorizer. ';
        if (@notes) {
            print '<div align="left"><div class=small>'.
                join("<br><br>",@notes).
                '</div></div>'
        }
        print '</form><br><br>';
    } else {
            my $message = "<div align=\"center\"><h4>No results to display, please search again</h4></div>";
            searchOccurrenceMisspellingForm($dbt,$q,$s,$hbo,$message);
    }
}

#
# Handle the actual occurrence changes. If the user hits the get next xxx button
# then we have to feed right back into the form though
#
sub submitOccurrenceMisspelling {
	my ($dbt,$q,$s,$hbo) = @_;
    my $dbh = $dbt->dbh;

    if ($q->param('submit') =~ /get next/i) {
        occurrenceMisspellingForm($dbt,$q,$s,$hbo);
    } else {
        my @exec_list = $q->param('execute_list');
        my $authorizer_no = $s->get('authorizer_no');
        my $enterer_no = $s->get('enterer_no');
        my $p = Permissions->new($s,$dbt);
        my $is_modifier_for = $p->getModifierList();
        my @permission_list = keys %{$is_modifier_for};
        push @permission_list, $authorizer_no;
        my $authorizer_list = join(",",@permission_list);
        
        print "<div align=\"center\"><h2>Spellings corrected</h2></div>";

        print "<div align=\"center\">";
        print "<div><ul style=\"text-align: left\">";
        my $count = 0;
        foreach my $i (@exec_list) {
            my $old_name = $q->param("old_taxon_name_$i");
            my $new_name = $q->param("new_taxon_name_$i");
            my $occ_list = $q->param("occurrence_list_$i");
            my $reid_list= $q->param("reid_list_$i");
            next if ($occ_list !~ /^[, \d]*$/);
            next if ($reid_list !~ /^[, \d]*$/);
            if ($old_name && $new_name && $old_name ne $new_name) {
                $count++;
                my ($g1,$sg1,$sp1,$ssp1) = Taxon::splitTaxon($old_name);
                my ($g2,$sg2,$sp2,$ssp2) = Taxon::splitTaxon($new_name);
                my ($g1_q,$sg1_q,$sp1_q) = ($dbh->quote($g1),$dbh->quote($sg1),$dbh->quote($sp1));
                my ($g2_q,$sg2_q,$sp2_q) = ($dbh->quote($g2),$dbh->quote($sg2),$dbh->quote($sp2));

                my @set_fields = ();

                # Paranoia about updates is high, so pass any changes through a filter - "REPLACE" will
                # make sure the string remains unchanged unless the you find an exact match on what you want
                # to replace. If the db field is null, just replace it, we can't do our filter on it.
                # One thing to note is that the number of affected rows returned equals the number of rows
                # that match the WHERE condition of the update, which may or may not be equal to the
                # number of rows actually changed
                push @set_fields, "genus_name=IF(genus_name IS NULL,$g2_q,REPLACE(genus_name,$g1_q,$g2_q))";
                my $new_actual_name = $g2;

                if ($sg1 && $sg2) {
                    push @set_fields, "subgenus_name=IF(subgenus_name IS NULL,$sg2_q,REPLACE(subgenus_name,$sg1_q,$sg2_q))";
                    $new_actual_name .= " ($sg2)";
                }
                if ($sp1 && $sp2) {
                    push @set_fields, "species_name=IF(species_name IS NULL,$sp2_q,REPLACE(species_name,$sp1_q,$sp2_q))";
                    $new_actual_name .= " $sp2";
                }
                my $best_taxon_no = Taxon::getBestClassification($dbt,'',$g2,'',$sg2,'',$sp2);
                push @set_fields,"modifier_no=$enterer_no","modifier=".$dbh->quote($s->get("enterer")),"taxon_no=$best_taxon_no";
                my $mod_count = 0;
                if ($occ_list) {
                    my @occs = split(/\s*,\s*/,$occ_list);
                    foreach my $occurrence_no (@occs) {
                        my $occ_sql = "SELECT comments,genus_name,subgenus_name,species_name FROM occurrences WHERE occurrence_no=$occurrence_no";
                        my $occ = ${$dbt->getData($occ_sql)}[0];
                        my $old_name_note = '[entered as '.$occ->{'genus_name'};
                        $old_name_note .= " ($occ->{subgenus_name})" if ($occ->{'subgenus_name'}); 
                        $old_name_note .= " $occ->{species_name}".']';
                        $old_name_note = $dbh->quote($old_name_note);
                        my $comment = "comments=IF(comments IS NULL,$old_name_note,concat(comments,' ',$old_name_note)),";
                        if ($occ->{'comments'} =~ (/\[entered as/)) {
                            $comment = "";
                        }
                        my $sql = "UPDATE occurrences SET $comment".join(", ",@set_fields)
                                . " WHERE occurrence_no=$occurrence_no"
                                . " AND authorizer_no IN ($authorizer_list)";
                        main::dbg("Occ sql $sql");
                        my $cnt = $dbh->do($sql);
                        $mod_count += $cnt;
                        # Some additional cleanup -- if we changed genus only recheck classification
                    }
                } 
                if ($reid_list) {
                    my @reids = split(/\s*,\s*/,$reid_list);
                    foreach my $reid_no (@reids) {
                        my $reid_sql = "SELECT comments,genus_name,subgenus_name,species_name FROM reidentifications WHERE reid_no=$reid_no";
                        my $reid = ${$dbt->getData($reid_sql)}[0];
                        my $old_name_note = '[entered as '.$reid->{'genus_name'};
                        $old_name_note .= " ($reid->{subgenus_name})" if ($reid->{'subgenus_name'}); 
                        $old_name_note .= " $reid->{species_name}".']';
                        $old_name_note = $dbh->quote($old_name_note);
                        my $comment = "comments=IF(comments IS NULL,$old_name_note,concat(comments,' ',$old_name_note)),";
                        if ($reid->{'comments'} =~ (/\[entered as/)) {
                            $comment = "";
                        }
                        my $sql = "UPDATE reidentifications SET $comment,".join(", ",@set_fields)
                                . " WHERE reid_no=$reid_no"
                                . " AND authorizer_no IN ($authorizer_list)";
                        main::dbg("Reid sql $sql");
                        my $cnt = $dbh->do($sql);
                        $mod_count += $cnt;
                    }
                }
                my $s = ($mod_count == 1) ? "" : "s";
                print "<li>$mod_count record$s of '$old_name' changed to '$new_actual_name'<br>";
            }
        }

        print "</ul></div>";
        if (!$count) {
            print "<h4>No changes were made</h4>";
        }
        print "</div>";
        print "<div align=\"center\"><b><a href=\"bridge.pl?action=searchOccurrenceMisspellingForm\">Search for more misspellings</a></b></div>";
    }
}


sub taxonTypoCheck {
    my ($dbt,$name,$genus_is_classified) = @_;
    my $dbh = $dbt->dbh;
    return () if (!$name);
    $name =~ s/^\s*//;

    my ($g,$sg,$sp,$ssp) = Taxon::splitTaxon($name);

    my %names = ();
    foreach my $table ('occurrences','reidentifications') {
        my @matches;
        if ($genus_is_classified) {
            @matches = ({'genus_name'=>$g});
        } else {
            @matches = typoCheck($dbt,$table,'genus_name','genus_name',"AND (genus_reso IS NULL OR genus_reso NOT LIKE '%informal%')",$g);
        }
        my $base_quality = ($genus_is_classified) ? 2 : 1;
        foreach my $row (@matches) {
            if ($sp) {
                my $where = "AND (species_reso IS NULL OR species_reso NOT LIKE '%informal%') AND genus_name=".$dbh->quote($g);
                if ($sg) {
                    $where .= ' AND subgenus_name='.$dbh->quote($sg);
                }
                my @matches_sp = typoCheck($dbt,$table,'species_name','genus_name,subgenus_name,species_name',$where,$sp);
                if (@matches_sp) {
                    foreach my $row_sp (@matches_sp) {
                        if ($row_sp->{'subgenus_name'}) {
                            $names{$row_sp->{'genus_name'}." ($row_sp->{subgenus_name}) ".$row_sp->{'species_name'}} = {'match_quality'=>$base_quality};
                        } else {
                            $names{$row_sp->{'genus_name'}.' '.$row_sp->{'species_name'}} = {'match_quality'=>$base_quality};
                        }
                    }
                } else {
                    $names{$row->{'genus_name'}} =  {'match_quality'=>$base_quality};
                }
            } else {
                $names{$row->{'genus_name'}} = {'match_quality'=>$base_quality};
            }
        }
    }
    my (@matches1,@matches2);
    if ($name ne $g) {
        @matches1 = typoCheck($dbt,'authorities','taxon_name','taxon_name,taxon_rank','',$name);
    }
    if (!$genus_is_classified) {
        @matches2 = typoCheck($dbt,'authorities','taxon_name','taxon_name','',$g);
    }
    foreach (@matches1,@matches2) {
        if ($names{$_->{'taxon_name'}}) {
            $names{$_->{'taxon_name'}}{'match_quality'} = 2;
            if ($_->{'taxon_rank'} =~ /species/) {
                $names{$_->{'taxon_name'}}{'match_quality'} = 3;
            }
        } else {
            my ($tg,$tsg,$tsp) = Taxon::splitTaxon($_->{'taxon_name'});
            if ($names{$tg}) {
                $names{$_->{'taxon_name'}} = {'match_quality'=>2};
            } else {
                $names{$_->{'taxon_name'}} = {'match_quality'=>1};
            }
        }
    }

    # Delete exact matches after the fact
    delete $names{$name};
    delete $names{$g};
    delete $names{"$g ($sg)"};
    delete $names{"$g ($sg) $sp"};
    delete $names{"$g $sp"};

    return \%names;
}

# PS 6/17/2006
# generic typoChecking function.  Originally planned to use a Metaphone/edit distance type
# algorithm, but generally typos are more prominent, not gross misspellings, so just
# restricting things to the distance metric.  Two step approach. First is a lineaer
# time pass counting up occurrenes of letters and comparing the counts to hone stuff
# down to a small list.  Second use the standard "levenshtein" distance to filter
# out anagram type cases (of which there are MANY).
# liner_pass: M+N speed. Counts the occurrences of each letter in a word
# The sums up the difference in the occurrence count of each letter
# i.e.  Jybbah and Jabba would be 3: 
# 1 from the y (1 y in Jybbah, 0 in Jabba)
# 1 from the a (1 a in Jybbah, 2 in Jabba)
# 1 from the h (1 h in Jybbah, 0 in Jabba)
sub typoCheck {
    my ($dbt,$table,$field,$return_fields,$where,$value) = @_;
    my $dbh = $dbt->dbh;
    my $first = substr($value,0,1);
    my $second = substr($value,1,1);
    my $next = substr($value,2);
    if ($second !~ /^[aieoul]$/) {
        $first .= $second;
    }
    if ($next =~ /([^aeiouylvwbmn])/) { # skip these since  they're most prevalent in typos
        $next = $1;
    } else {
        $next = '';
    }
    my $max_length = length($value) + 2;
    my $min_length = length($value) - 2;
    my $sql = "SELECT $return_fields FROM $table WHERE $field LIKE '$first%$next%' AND (LENGTH($field) BETWEEN $min_length AND $max_length) ";
    if ($where) {
        $sql .= " $where ";
    }
    $sql .= " GROUP BY $field";
    my @results = @{$dbt->getData("$sql")};
#    main::dbg($sql);
    
    my $offset = ord('a');
    my @values;

    my ($w1,@w1v);
    $w1= lc($value); 
    $w1=~ s/[^a-z]//g;
    $w1v[$_] = 0 for (0..25);
    @values = map {ord($_) - $offset} split(//,$w1);
    foreach my $v (@values) {
        $w1v[$v]++;
    }

    my @matches;
    #print "NUM MATCHES: ".scalar(@results);
    my ($w2,@w2v);
    foreach my $row (@results) {
        $w2= lc($row->{$field});
        $w2=~ s/[^a-z]//g;
        $w2v[$_] = 0 for (0..25);
        @values = map {ord($_) - $offset} split(//,$w2);
        foreach my $v (@values) {
            $w2v[$v]++;
        }
        my $distance = 0;
        for (0..25) {
            $distance += abs($w1v[$_] - $w2v[$_]);
        }
        if ($distance < 4) {
            # Also returns exact matches, filter later
            if (editDistance($w1,$w2) < 3) {
                push @matches, $row;
            }
        } 
    }
    #foreach my $row (@matches) {
    #    $row->{'metaphone'} = double_metaphone($row->{'taxon_name'})
    #}
    return @matches;
}

# This is the levenshtein distance algorithm. Google it.
# Downside is that its M*N speed, don't use it against a ten thousand records or somthing, it'll be slow
# PS 6/17/2006
sub editDistance {
    my @s = split(//,$_[0]);
    my @t = split(//,$_[1]);
    my $s_len = scalar(@s);
    my $t_len = scalar(@t);
    if ($s_len == 0) { 
        return $t_len
    } elsif ($t_len == 0) { 
        return $s_len; 
    }

    my @d; # cost array
    for(my $i = 0;$i<$s_len;$i++) {
        $d[$i][0] = $i;
    }
    for(my $j = 0;$j<$t_len;$j++) {
        $d[0][$j] = $j;
    }
    my ($x1,$x2,$x3,$sub_cost,$min_cost);
    for(my $i=1;$i<$s_len;$i++) {
        for(my $j=1;$j<$t_len;$j++) {
            $sub_cost = ($s[$i] eq $t[$j])  ? 0 : 1;
            $x1 = $d[$i-1][$j] + 1;
            $x2 = $d[$i][$j-1] + 1;
            $x3 = $d[$i-1][$j-1] + $sub_cost;
            $min_cost  = ($x1 < $x2 && $x1 < $x3) ? $x1 :
                         ($x2 < $x3 ) ? $x2 :
                         $x3;
            $d[$i][$j] = $min_cost;
        }
    }

    return $d[$s_len-1][$t_len-1];
}

## sub newTaxonNames
#	Description:	checks whether each of the names given to it are
#					currently in the database, returning an array of those
#					that aren't.
#
#	Arguments:		$dbh		database handle
#					$names		reference to an array of genus_names
#					$type		'genus_name', 'species_name' or 'subgenus_name'
#
#	Returns:		Array of names NOT currently in the database.
#
##
sub newTaxonNames {
	my $dbt = shift;
    my $dbh = $dbt->dbh;
	my $names = shift;
    my $type = shift;

	my @names = @{$names};
    my @taxa = ();
	my @result = ();
    my @res;
	
    foreach my $name (@names) {
        push @taxa, $dbh->quote($name) if ($name);
    }
    if (@taxa) {
    	my $sql = "SELECT $type FROM occurrences WHERE $type IN (".join(',',@taxa).") GROUP BY $type";
        @res = @{$dbt->getData($sql)};
	}
	
	NAME:
	foreach my $check (@names){
        next if (!$check);
		foreach my $check_res (@res){ 
			next NAME if(uc($check_res->{$type}) eq uc($check));
		}
		push(@result, $check); 
	}
	
	return @result;
}
1;

