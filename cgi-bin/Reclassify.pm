package Reclassify;

use strict;
use URI::Escape;

# written by JA 31.3, 1.4.04
# in memory of our dearly departed Ryan Poling

# start the process to get to the reclassify occurrences page
# modelled after startAddEditOccurrences
sub startReclassifyOccurrences	{
	my ($q,$s,$dbh,$dbt,$hbo) = @_;

	if (!$s->isDBMember()) {
	    # have to be logged in
		$s->enqueue( $dbh, "action=startStartReclassifyOccurrences" );
		main::displayLoginPage( "Please log in first." );
	} elsif ( $q->param("collection_no") )	{
        # if they have the collection number, they'll immediately go to the
        #  reclassify page
		&displayOccurrenceReclassify($q,$s,$dbh,$dbt);
	} else	{
        my $html = $hbo->populateHTML('search_reclassify_form', [ '', '', '',$s->get('authorizer_no')], [ 'research_group', 'eml_max_interval', 'eml_min_interval','authorizer_no'], []);

        my $javaScript = main::makeAuthEntJavaScript();
        $html =~ s/%%NOESCAPE_enterer_authorizer_lists%%/$javaScript/;
        my $authorizer_reversed = $s->get("authorizer_reversed");
        $html =~ s/%%authorizer_reversed%%/$authorizer_reversed/;
        my $enterer_reversed = $s->get("enterer_reversed");
        $html =~ s/%%enterer_reversed%%/$enterer_reversed/;

        # Spit out the HTML
        print main::stdIncludes( "std_page_top" );
        main::printIntervalsJava(1);
        print $html;
        print main::stdIncludes("std_page_bottom");  
    }
}

# print a list of the taxa in the collection with pulldowns indicating
#  alternative classifications
sub displayOccurrenceReclassify	{

	my $q = shift;
	my $s = shift;
	my $dbh = shift;
	my $dbt = shift;
    my $collections_ref = shift;
    my @collections = ();
    @collections = @$collections_ref if ($collections_ref);

	print main::stdIncludes("std_page_top");

    my @occrefs;
    if (@collections) {
	    print "<center><h3>Classification of ".$q->param('taxon_name')."</h3>";
        my ($genus,$subgenus,$species,$subspecies) = Taxon::splitTaxon($q->param('taxon_name'));
        my @names = ($dbh->quote($genus));
        if ($subgenus) {
            push @names, $dbh->quote($subgenus);
        }
        my $names = join(", ",@names);
        my $sql = "(SELECT 0 reid_no, o.authorizer_no, o.occurrence_no,o.taxon_no, o.genus_reso, o.genus_name, o.subgenus_reso, o.subgenus_name, o.species_reso, o.species_name, c.collection_no, c.collection_name, c.country, c.state, c.max_interval_no, c.min_interval_no FROM occurrences o, collections c WHERE o.collection_no=c.collection_no AND c.collection_no IN (".join(", ",@collections).") AND (o.genus_name IN ($names) OR o.subgenus_name IN ($names))";
        if ($species) {
            $sql .= " AND o.species_name LIKE ".$dbh->quote($species);
        }
        if ($q->param('occurrences_authorizer_no') =~ /^[\d,]+$/) {
            $sql .= " AND o.authorizer_no IN (".$q->param('occurrences_authorizer_no').")";
        }
        $sql .= ")";
        $sql .= " UNION ";
        $sql .= "( SELECT re.reid_no, re.authorizer_no,re.occurrence_no,re.taxon_no, re.genus_reso, re.genus_name, re.subgenus_reso, re.subgenus_name, re.species_reso, re.species_name, c.collection_no, c.collection_name, c.country, c.state, c.max_interval_no, c.min_interval_no FROM reidentifications re, occurrences o, collections c WHERE re.occurrence_no=o.occurrence_no AND o.collection_no=c.collection_no AND c.collection_no IN (".join(", ",@collections).") AND (re.genus_name IN ($names) OR re.subgenus_name IN ($names))";
        if ($species) {
            $sql .= " AND re.species_name LIKE ".$dbh->quote($species);
        }
        if ($q->param('occurrences_authorizer_no') =~ /^[\d,]+$/) {
            $sql .= " AND re.authorizer_no IN (".$q->param('occurrences_authorizer_no').")";
        }
        $sql .= ") ORDER BY occurrence_no ASC, reid_no ASC";
        main::dbg("Reclassify sql:".$sql);
        @occrefs = @{$dbt->getData($sql)};
    } else {
	    my $sql = "SELECT collection_name FROM collections WHERE collection_no=" . $q->param('collection_no');
	    my $coll_name = ${$dbt->getData($sql)}[0]->{collection_name};
	    print "<center><h3>Classification of taxa in collection ",$q->param('collection_no')," ($coll_name)</h3>";
       
        my $authorizer_where = "";
        if ($q->param('occurrences_authorizer_no') =~ /^[\d,]+$/) {
            $authorizer_where = " AND authorizer_no IN (".$q->param('occurrences_authorizer_no').")";
        }

        # get all the occurrences
        my $collection_no = int($q->param('collection_no'));
        $sql = "(SELECT 0 reid_no,authorizer_no, occurrence_no,taxon_no,genus_reso,genus_name,subgenus_reso,subgenus_name,species_reso,species_name FROM occurrences WHERE collection_no=$collection_no $authorizer_where)".
               " UNION ".
               "(SELECT reid_no,authorizer_no, occurrence_no,taxon_no,genus_reso,genus_name,subgenus_reso,subgenus_name,species_reso,species_name FROM reidentifications WHERE collection_no=$collection_no $authorizer_where)".
               " ORDER BY occurrence_no ASC,reid_no ASC";
        main::dbg("Reclassify sql:".$sql);
        @occrefs = @{$dbt->getData($sql)};
    }

	# tick through the occurrences
	# NOTE: the list will be in data entry order, nothing fancy here
	if ( @occrefs )	{
		print "<form action=\"bridge.pl\" method=\"post\">\n";
		print "<input id=\"action\" type=\"hidden\" name=\"action\" value=\"startProcessReclassifyForm\">\n";
		print "<input name=\"occurrences_authorizer_no\" type=\"hidden\" value=\"".$q->param('occurrences_authorizer_no')."\">\n";
        if (@collections) {
            print "<input type=\"hidden\" name=\"taxon_name\" value=\"".$q->param('taxon_name')."\">";
		    print "<table border=0 cellpadding=0 cellspacing=0>\n";
            print "<tr><th colspan=2>Collection</th><th>Classificaton based on</th></tr>";
        } else {
            print "<input type=\"hidden\" name=\"collection_no\" value=\"".$q->param('collection_no')."\">";
		    print "<table border=0 cellpadding=0 cellspacing=0>\n";
            print "<tr><th>Taxon name</th><th>Classification based on</th></tr>";
        }
	}

    # Make non-editable links not changeable
    my $p = Permissions->new($s,$dbt);
    my %is_modifier_for = %{$p->getModifierList()};

	my $rowcolor = 0;
    my $nonEditableCount = 0;
    my @badoccrefs;
    my $nonExact = 0;
	for my $o ( @occrefs )	{
        my $editable = ($s->get("superuser") || $is_modifier_for{$o->{'authorizer_no'}} || $o->{'authorizer_no'} == $s->get('authorizer_no')) ? 1 : 0;
        my $authorizer = ($editable) ? '' : '(<b>Authorizer:</b> '.Person::getPersonName($dbt,$o->{'authorizer_no'}).')';
        $nonEditableCount++ if (!$editable);

		# if the name is informal, add it to the list of
		#  unclassifiable names
		if ( $o->{genus_reso} =~ /informal/ )	{
			push @badoccrefs , $o;
		}
		# otherwise print it
		else	{
			# compose the taxon name
			my $taxon_name = $o->{genus_name};
			if ( $o->{species_reso} !~ /informal/ && $o->{species_name} !~ /^sp\./ && $o->{species_name} !~ /^indet\./)	{
				$taxon_name .= " " . $o->{species_name};
			}
            # Give these default values, don't want to pass in possibly undef values to any function or PERL might screw it up
            my ($genus_reso,$genus_name,$subgenus_reso,$subgenus_name,$species_reso,$species_name) = ("","","","","","");
            $genus_name = $o->{'genus_name'} if ($o->{'genus_name'});
            $genus_reso = $o->{'genus_reso'} if ($o->{'genus_reso'});
            $subgenus_reso = $o->{'subgenus_reso'} if ($o->{'subgenus_reso'});
            $subgenus_name = $o->{'subgenus_name'} if ($o->{'subgenus_name'});
            $species_reso = $o->{'species_reso'} if ($o->{'species_reso'});
            $species_name = $o->{'species_name'} if ($o->{'species_name'});
            my @all_matches = Taxon::getBestClassification($dbt,$genus_reso,$genus_name,$subgenus_reso,$subgenus_name,$species_reso,$species_name);

			# now print the name and the pulldown of authorities
			if ( @all_matches )	{
                foreach my $m (@all_matches) {
                    if ($m->{'match_level'} < 30)	{
                        $nonExact++;
                    }
                }
				if ( $rowcolor % 2 )	{
					print "<tr>";
				} else	{
					print "<tr class='darkList'>";
				}

                my $collection_string = "";
                if ($o->{'collection_no'}) {
                    my $tsql = "SELECT interval_name FROM intervals WHERE interval_no=" . $o->{max_interval_no};
                    my $maxintname = @{$dbt->getData($tsql)}[0];
                    $collection_string = "<b>".$o->{'collection_name'}."</b> ";
                    $collection_string .= "<span class=\"tiny\">"; 
                    $collection_string .= $maxintname->{interval_name};
                    if ( $o->{min_interval_no} > 0 )  {
                        $tsql = "SELECT interval_name FROM intervals WHERE interval_no=" . $o->{min_interval_no};
                        my $minintname = @{$dbt->getData($tsql)}[0];
                        $collection_string .= "/" . $minintname->{interval_name};
                    }

                    $collection_string .= " - ";
                    if ( $o->{"state"} )  {
                        $collection_string .= $o->{"state"};
                    } else  {
                        $collection_string .= $o->{"country"};
                    }
                    $collection_string .= "</span>";
                    $collection_string .= " <span class=\"tiny\" style=\"white-space: nowrap;\">$authorizer</span>";

                    print "<td style=\"padding-right: 1.5em; padding-left: 1.5em;\"><a href=\"bridge.pl?action=displayCollectionDetails&collection_no=$o->{collection_no}\">$o->{collection_no}</a></td><td>$collection_string</td>";
                }
				print "<td><span style=\"white-space:nowrap;\">&nbsp;&nbsp;\n";

				# here's the name
				my $formatted = "";
				if ( $o->{'species_name'} !~ /^indet\./ )	{
					$formatted .= "<i>";
				}
				$formatted .= "$o->{genus_reso} $o->{genus_name}";
                if ($o->{'subgenus_name'}) {
                    $formatted .= " $o->{subgenus_reso} ($o->{subgenus_name})";
                }
                $formatted .= " $o->{species_reso} $o->{species_name}";
				if ( $o->{'species_name'} !~ /^indet\./ )	{
					$formatted .= "</i>";
				}
                $formatted .= " </span>";
                if (!$collection_string) {
                    $formatted .= " <span class=\"tiny\" style=\"white-space: nowrap;\">$authorizer</span>";
                }

				# need a hidden recording the old taxon number
                $collection_string .= ": " if ($collection_string);
                 
				if ( $o->{reid_no} )	{
					print "&nbsp;&nbsp;<span class='small'><b>reID =</b></span>&nbsp;";
                }

                my $description = "$collection_string $formatted";

				print $formatted;
				print "</td>\n";

				# start the select list
				# the name depends on whether this is
				#  an occurrence or reID
				print "<td>&nbsp;&nbsp;\n";
                if ($o->{reid_no}) {
                    print classificationSelect($dbt,$o->{reid_no},1,$editable,\@all_matches,$o->{taxon_no},$description);
                } else {
                    print classificationSelect($dbt,$o->{occurrence_no},0,$editable,\@all_matches,$o->{taxon_no},$description);
                }
                print "</td>\n";
				print "</tr>\n";
				$rowcolor++;
			} else	{
				push @badoccrefs , $o;
			}
		}
	}
	if ( @occrefs )	{
		print "</table>\n";
		print "<p><input type=submit value='Reclassify'></p>\n";
		print "</form>\n";
	}
	print "<p>\n";
    my @warnings;
	if ( $nonExact)	{
		push @warnings, "Exact formal classifications for some taxa could not be found, so approximate matches were used.  For example, a species might not be formally classified but its genus is.";
	}
    if ( $nonEditableCount) {
        push @warnings, "Some occurrences can't be reclassified because they have a different authorizer.";
    }
    if (@warnings) {
        print Debug::printWarnings(\@warnings);
    }

	# print the informal and otherwise unclassifiable names
	if ( @badoccrefs )	{
		print "<hr>\n";
		print "<h4>Taxa that cannot be classified</h4>";
		print "<p><i>Check these names for typos and/or create new taxonomic authority records for them</i></p>\n";
		print "<table border=0 cellpadding=0 cellspacing=0>\n";
	}
	$rowcolor = 0;
	for my $b ( @badoccrefs )	{
		if ( $rowcolor % 2 )	{
			print "<tr>";
		} else	{
			print "<tr class='darkList'>";
		}
		print "<td align='left'>&nbsp;&nbsp;";
		if ( $b->{'species_name'} !~ /^indet\./)	{
			print "<i>";
		}
		print "$b->{genus_reso} $b->{genus_name}";
        if ($b->{'subgenus_name'}) {
            print " $b->{subgenus_reso} ($b->{subgenus_name})";
        }
        print " $b->{species_reso} $b->{species_name}\n";
		if ( $b->{'species_name'} !~ /^indet\./)	{
			print "</i>";
		}
		print "&nbsp;&nbsp;</td></tr>\n";
		$rowcolor++;
	}
	if ( @badoccrefs )	{
		print "</table>\n";
	}

	print "<p>\n";
	print "</center>\n";

	print main::stdIncludes("std_page_bottom");

}

sub processReclassifyForm	{

	my $q = shift;
	my $s = shift;
	my $dbh = shift;
	my $dbt = shift;
	my $exec_url = shift;

    print "<BR>";
	print main::stdIncludes("std_page_top");

	print "<center>\n\n";

    if ($q->param('collection_no')) {
        my $sql = "SELECT collection_name FROM collections WHERE collection_no=" . $q->param('collection_no');
        my $coll_name = ${$dbt->getData($sql)}[0]->{collection_name};
        print "<h3>Taxa reclassified in collection " , $q->param('collection_no') ," (" , $coll_name , ")</h3>\n\n";
	    print "<table border=0 cellpadding=2 cellspacing=0>\n";
        print "<tr><th>Taxon</th><th>Classification based on</th></tr>";
    } elsif ($q->param('taxon_name')) {
        print "<h3>Taxa reclassified for " , $q->param('taxon_name') ,"</h3>\n\n";
	    print "<table border=0 cellpadding=2 cellspacing=0>\n";
        print "<tr><th>Collection</th><th>Classification based on</th></tr>";
    } else {
        print "<h3>Taxa reclassified</h3>";
	    print "<table border=0 cellpadding=2 cellspacing=0>\n";
        print "<tr><th>Taxon</th><th>Classification based on</th></tr>";
    }

	# get lists of old and new taxon numbers
	# WARNING: taxon names are stashed in old numbers and authority info
	#  is stashed in new numbers
	my @old_taxa = $q->param('old_taxon_no');
	my @new_taxa = $q->param('taxon_no');
	my @occurrences = $q->param('occurrence_no');
	my @occurrence_descriptions = $q->param('occurrence_description');
	my @reid_descriptions = $q->param('reid_description');
	my @old_reid_taxa = $q->param('old_reid_taxon_no');
	my @new_reid_taxa = $q->param('reid_taxon_no');
	my @reids = $q->param('reid_no');

	my $rowcolor = 0;

	# first tick through the occurrence taxa and update as appropriate
    my $seen_reclassification = 0;
	foreach my $i (0..$#old_taxa)	{
		my $old_taxon_no = $old_taxa[$i];
        my $occurrence_description = uri_unescape($occurrence_descriptions[$i]);
		my ($new_taxon_no,$authority) = split /\+/,$new_taxa[$i];
		if ( $old_taxa[$i] != $new_taxa[$i] )	{
            $seen_reclassification++;

		# update the occurrences table
			my $sql = "UPDATE occurrences SET taxon_no=".$new_taxon_no.
                   ", modifier=".$dbh->quote($s->get('enterer')).
			       ", modifier_no=".$s->get('enterer_no');
			if ( $old_taxon_no > 0 )	{
				$sql .= " WHERE taxon_no=" . $old_taxon_no;
			} else	{
				$sql .= " WHERE taxon_no=0";
			}
			$sql .= " AND occurrence_no=" . $occurrences[$i];
            main::dbg($sql);
			$dbt->getData($sql);

		# print the taxon's info
			if ( $rowcolor % 2 )	{
				print "<tr>";
			} else	{
				print "<tr class='darkList'>";
			}
			print "<td>&nbsp;&nbsp;$occurrence_description</td><td style=\"padding-left: 1em;\"> $authority&nbsp;&nbsp;</td>\n";
			print "</tr>\n";
			$rowcolor++;
		}
	}

	# then tick through the reidentification taxa and update as appropriate
	# WARNING: this isn't very slick; all the reIDs always come after
	#  all the occurrences
	foreach my $i (0..$#old_reid_taxa)	{
		my $old_taxon_no = $old_reid_taxa[$i];
        my $reid_description = uri_unescape($reid_descriptions[$i]);
		my ($new_taxon_no,$authority) = split /\+/,$new_reid_taxa[$i];
		if ( $old_reid_taxa[$i] != $new_reid_taxa[$i] )	{
            $seen_reclassification++;

		# update the reidentifications table
			my $sql = "UPDATE reidentifications SET taxon_no=".$new_taxon_no.
                   ", modifier=".$dbh->quote($s->get('enterer')).
			       ", modifier_no=".$s->get('enterer_no');
			if ( $old_taxon_no > 0 )	{
				$sql .= " WHERE taxon_no=" . $old_taxon_no;
			} else	{
				$sql .= " WHERE taxon_no=0";
			}
			$sql .= " AND reid_no=" . $reids[$i];
            main::dbg($sql);
			$dbt->getData($sql);

		# print the taxon's info
			if ( $rowcolor % 2 )	{
				print "<tr>";
			} else	{
				print "<tr class='darkList'>";
			}
			print "<td>&nbsp;&nbsp;$reid_description</td><td style=\"padding-left: 1em;\"> $authority&nbsp;&nbsp;</td>\n";
			print "</tr>\n";
			$rowcolor++;
		}
	}

	print "</table>\n\n";
    if (!$seen_reclassification) {
        print "<div align=\"center\">No taxa reclassified</div>";
    }

	print "<p>";
   
    if ($q->param('show_links')) {
        print uri_unescape($q->param("show_links"));
    } else { 
        if ($q->param('collection_no')) {
            print "<a href=\"$exec_url?action=startStartReclassifyOccurrences&occurrences_authorizer_no=".$q->param('occurrences_authorizer_no')."&collection_no=";
            print $q->param('collection_no');
            print "\"><b>Reclassify this collection</b></a> - ";
        } else {
            print "<a href=\"$exec_url?action=displayCollResults&type=reclassify_occurrence&occurrences_authorizer_no=".$q->param('occurrences_authorizer_no')."&taxon_name=";
            print $q->param('taxon_name');
            print "\"><b>Reclassify ".$q->param('taxon_name')."</b></a> - ";
        }
    	print "<a href=\"$exec_url?action=startStartReclassifyOccurrences\"><b>Reclassify another collection or taxon</b></a></p>\n\n";
    }

	print main::stdIncludes("std_page_bottom");

}

sub classificationSelect {
    my ($dbt,$key_no,$is_reid,$editable,$matches,$taxon_no,$description) = @_;

    my $disabled = ($editable) ?  '' : 'DISABLED';

    my $html = "";
    if ($is_reid) {
        $html .= "<input type=\"hidden\" $disabled name=\"old_reid_taxon_no\" value=\"$taxon_no\">\n";
        $html .= "<input type=\"hidden\" $disabled name=\"reid_description\" value=\"".uri_escape($description)."\">\n";
        $html .= "<input type=\"hidden\" $disabled name=\"reid_no\" value=\"$key_no\">\n";
        $html .= "<select $disabled name=\"reid_taxon_no\">";
    } else {
		$html .= "<input type=\"hidden\" $disabled name=\"old_taxon_no\" value=\"$taxon_no\">\n";
        $html .= "<input type=\"hidden\" $disabled name=\"occurrence_description\" value=\"".uri_escape($description)."\">\n";
		$html .= "<input type=\"hidden\" $disabled name=\"occurrence_no\" value=\"$key_no\">\n";
        $html .= "<select $disabled name=\"taxon_no\">";
    }
                 
    # populate the select list of authorities
    foreach my $m (@$matches)	{
        my $t = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$m->{'taxon_no'}},['taxon_no','taxon_name','taxon_rank','author1last','author2last','otherauthors','pubyr']);
        # have to format the authority data
        my $authority = "$t->{taxon_name}";
        my $pub_info = Reference::formatShortRef($t);
        if ($pub_info =~ /[A-Za-z0-9]/) {
            $authority .= ", $pub_info";
        }
        # needed by Classification
        my %master_class=%{TaxaCache::getParents($dbt, [$t->{'taxon_no'}],'array_full')};

        my @parents = @{$master_class{$t->{'taxon_no'}}};
        if (@parents) {
            $authority .= " [";
            my $foundParent = 0;
            foreach (@parents) {
                if ($_->{'taxon_rank'} =~ /^(?:family|order|class)$/) {
                    $foundParent = 1;
                    $authority .= $_->{'taxon_name'}.", ";
                    last;
                }
            }
            $authority =~ s/, $//;
            if (!$foundParent) {
                $authority .= $parents[0]->{'taxon_name'};
            }
            $authority .= "]";
        }
        if ( $authority !~ /[A-Za-z]/ )	{
            $authority = "taxon number " . $t->{taxon_no};
        }
        # clean up in case there's a
        #  classification but no author
        $authority =~ s/^ //;

        $html .= "<option value=\"" . $t->{taxon_no} . "+" . $authority . "\"";
        if ($t->{taxon_no} eq $taxon_no) {
            $html .= " selected";
        }
        $html .= ">$authority</option>\n";
    }
    if ($taxon_no) {
        $html .= "<option value=\"0+unclassified\">leave unclassified</option>\n";
    } else {
        $html .= "<option value=\"0+unclassified\" selected>leave unclassified</option>\n";
    }
    $html .= "</select>";
    return $html;
}


1;
