package Reclassify;

use strict;
use URI::Escape;
use CGI qw(escapeHTML);
use Debug qw(dbg);
use Constants qw($READ_URL $WRITE_URL $DB $COLLECTIONS $COLLECTION_NO $OCCURRENCES $OCCURRENCE_NO $PAGE_TOP $PAGE_BOTTOM);

# in memory of our dearly departed Ryan Poling

# start the process to get to the reclassify occurrences page
# modelled after startAddEditOccurrences
sub startReclassifyOccurrences	{
	my ($q,$s,$dbt,$hbo) = @_;
    my $dbh = $dbt->dbh;

	if (!$s->isDBMember()) {
	    # have to be logged in
		$s->enqueue($q->query_string());
		main::displayLoginPage( "Please log in first." );
	} elsif ( $q->param("collection_no") )	{
        # if they have the collection number, they'll immediately go to the
        #  reclassify page
		displayOccurrenceReclassify($q,$s,$dbt,$hbo);
	} else	{
        my %vars = $q->Vars();
        $vars{'enterer_me'} = $s->get('enterer_reversed');
        $vars{'submit'} = "Search for occurrences";
        $vars{'page_title'} = "Reclassification search form";
        $vars{'action'} = "displayCollResults";
        $vars{'type'} = "reclassify_occurrence";
        $vars{'page_subtitle'} = "You may now reclassify either a set of occurrences matching a genus or higher taxon name, or all the occurrences in one collection.";

        # Spit out the HTML
        print $hbo->stdIncludes( "std_page_top" );
        print PBDBUtil::printIntervalsJava($dbt,1);
        print Person::makeAuthEntJavascript($dbt);
        print $hbo->populateHTML('search_occurrences_form',\%vars);
        print $hbo->stdIncludes("std_page_bottom");  
    }
}

# print a list of the taxa in the collection with pulldowns indicating
#  alternative classifications
sub displayOccurrenceReclassify	{
    my ($q,$s,$dbt,$hbo,$collections_ref) = @_;
    my $dbh = $dbt->dbh;
    my @collections = ();
    @collections = @$collections_ref if ($collections_ref);
    if ($q->param("collection_list")) {
        @collections = split(/\s*,\s*/,$q->param('collection_list'));
    }

	print $hbo->stdIncludes("std_page_top");

    my @occrefs;
    if (@collections) {
	    print "<center><p class=\"pageTitle\">Classification of \"".$q->param('taxon_name')."\" occurrences</p>";
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
        if ($q->param('authorizer_only') =~ /yes/i) {
            $sql .= " AND o.authorizer_no=".$s->get('authorizer_no');
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
        if ($q->param('authorizer_only') =~ /yes/i) {
            $sql .= " AND re.authorizer_no=".$s->get('authorizer_no');
        }
        if ($q->param('occurrences_authorizer_no') =~ /^[\d,]+$/) {
            $sql .= " AND re.authorizer_no IN (".$q->param('occurrences_authorizer_no').")";
        }
        $sql .= ") ORDER BY occurrence_no ASC, reid_no ASC";
        dbg("Reclassify sql:".$sql);
        @occrefs = @{$dbt->getData($sql)};
    } else {
	    my $sql = "SELECT collection_name FROM collections WHERE collection_no=" . $q->param('collection_no');
	    my $coll_name = ${$dbt->getData($sql)}[0]->{collection_name};
	    print "<center><p class=\"pageTitle\">Classification of taxa in collection ",$q->param('collection_no')," ($coll_name)</p>";
       
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
        dbg("Reclassify sql:".$sql);
        @occrefs = @{$dbt->getData($sql)};
    }

	# tick through the occurrences
	# NOTE: the list will be in data entry order, nothing fancy here
	if ( @occrefs )	{
		print "<form action=\"$WRITE_URL\" method=\"post\">\n";
		print "<input id=\"action\" type=\"hidden\" name=\"action\" value=\"startProcessReclassifyForm\">\n";
		print "<input name=\"occurrences_authorizer_no\" type=\"hidden\" value=\"".$q->param('occurrences_authorizer_no')."\">\n";
        if (@collections) {
            print "<input type=\"hidden\" name=\"taxon_name\" value=\"".$q->param('taxon_name')."\">";
		    print "<table border=0 cellpadding=0 cellspacing=0 class=\"small\">\n";
            print "<tr><th class=\"large\" colspan=2>Collection</th><th class=\"large\" colspan=2 style=\"text-align: left; padding-left: 2em;\">Classification based on</th></tr>";
        } else {
            print "<input type=\"hidden\" name=\"collection_no\" value=\"".$q->param('collection_no')."\">";
		    print "<table border=0 cellpadding=0 cellspacing=0 class=\"small\">\n";
            print "<tr><th class=\"large\">Taxon name</th><th colspan=2 class=\"large\" style=\"text-align: left; padding-left: 2em;\">Classification based on</th></tr>";
        }
	}

    # Make non-editable links not changeable
# knocked this out 28.2.08 because it's unclear why anyone would care if
#  someone else fixed the classification of their occurrence JA
#    my $p = Permissions->new($s,$dbt);
#    my %is_modifier_for = %{$p->getModifierList()};

	my $rowcolor = 0;
    my $nonEditableCount = 0;
    my @badoccrefs;
    my $nonExact = 0;
	for my $o ( @occrefs )	{
#        my $editable = ($s->get("superuser") || $is_modifier_for{$o->{'authorizer_no'}} || $o->{'authorizer_no'} == $s->get('authorizer_no')) ? 1 : 0;
my $editable = 1;
        my $authorizer = ($editable) ? '' : '(Authorizer: '.Person::getPersonName($dbt,$o->{'authorizer_no'}).')';
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
            my @all_matches = Taxon::getBestClassification($dbt,$o);

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
                    $collection_string = $o->{'collection_name'}." ";
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

                    print "<td style=\"padding-right: 1.5em; padding-left: 1.5em;\"><a href=\"$READ_URL?action=displayCollectionDetails&collection_no=$o->{collection_no}\">$o->{collection_no}</a></td><td>$collection_string</td>";
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
					print "&nbsp;&nbsp;<span class='small'>reID: </span>&nbsp;";
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
                    print classificationSelect($dbt,$o->{$OCCURRENCE_NO},0,$editable,\@all_matches,$o->{taxon_no},$description);
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
		print "<p class=\"large\">Occurrences that cannot be classified</p>";
		print "<p><i>Check these names for typos and/or create new taxonomic authority records for them</i></p>\n";
		print "<table border=0 cellpadding=0 cellspacing=0 class=\"small\">\n";
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

	print $hbo->stdIncludes("std_page_bottom");

}

sub processReclassifyForm	{
	my ($q,$s,$dbt,$hbo) = @_;
	my $dbh = $dbt->dbh;

	# get lists of old and new taxon numbers
	# WARNING: taxon names are stashed in old numbers and authority info
	#  is stashed in new numbers
	my @old_taxa = $q->param('old_taxon_no');
	my @new_taxa = $q->param('taxon_no');
	my @occurrences = $q->param($OCCURRENCE_NO);
	my @occurrence_lists = $q->param("occurrence_list");
	my @occurrence_descriptions = $q->param('occurrence_description');
	my @reid_descriptions = $q->param('reid_description');
	my @old_reid_taxa = $q->param('old_reid_taxon_no');
	my @new_reid_taxa = $q->param('reid_taxon_no');
	my @reids = $q->param('reid_no');

	# nothing fancy for the eco database: update and bomb out
	if ( $DB eq "eco" )	{
		foreach my $i (0..$#old_taxa)	{
			my $old_taxon_no = $old_taxa[$i];
			my ($new_taxon_no,$authority) = split /\+/,$new_taxa[$i];
			if ( $old_taxa[$i] != $new_taxa[$i] )	{
            			my $dbh_r = $dbt->dbh;
				my $sql = "UPDATE $OCCURRENCES SET taxon_no=".$new_taxon_no.", modifier_no=".$s->get('enterer_no');
				if ( $old_taxon_no > 0 )	{
					$sql .= " WHERE taxon_no=" . $old_taxon_no;
				} else	{
					$sql .= " WHERE taxon_no=0";
				}
				if ($occurrences[$i] =~ /^\d+$/) {
					$sql .= " AND $OCCURRENCE_NO=" . $occurrences[$i];
					$dbh_r->do($sql);
				} elsif ($occurrence_lists[$i] =~ /^[\d, ]+$/) {
					$sql .= " AND $OCCURRENCE_NO IN (".$occurrence_lists[$i].")";
					$dbh_r->do($sql);
				}
			}
		}
		Collection::inventoryInfo($dbt,$q,$s,$hbo,Collection::inventoryEditLinks($q->param('inventory_no')));
		return;
	}

	print $hbo->stdIncludes($PAGE_TOP);

	print "<center>\n\n";

    if ($q->param('collection_no')) {
        my $sql = "SELECT collection_name FROM collections WHERE collection_no=" . $q->param('collection_no');
        my $coll_name = ${$dbt->getData($sql)}[0]->{collection_name};
        print "<p class=\"pageTitle\">Reclassified taxa in collection " , $q->param('collection_no') ," (" , $coll_name , ")</p>\n\n";
	    print "<table border=0 cellpadding=2 cellspacing=0 class=\"small\">\n";
        print "<tr><th class=\"large\">Taxon</th><th class=\"large\">Classification based on</th></tr>";
    } elsif ($q->param('taxon_name')) {
        print "<p class=\"pageTitle\">Reclassified occurrences of " , $q->param('taxon_name') ,"</p>\n\n";
	    print "<table border=0 cellpadding=2 cellspacing=0 class=\"small\">\n";
        print "<tr><th class=\"large\">Collection</th><th class=\"large\">Classification based on</th></tr>";
    } else {
        print "<p class=\"pageTitle\">Reclassified occurrences</p>";
	    print "<table border=0 cellpadding=2 cellspacing=0 class=\"small\">\n";
        print "<tr><th class=\"large\">Taxon</th><th class=\"large\">Classification based on</th></tr>";
    }

	my $rowcolor = 0;

	# first tick through the occurrence taxa and update as appropriate
	my $seen_reclassification = 0;
	foreach my $i (0..$#old_taxa)	{
		my $old_taxon_no = $old_taxa[$i];
		my $occurrence_description = "<span>".uri_unescape($occurrence_descriptions[$i]);
		my ($new_taxon_no,$authority) = split /\+/,$new_taxa[$i];
		if ( $old_taxa[$i] != $new_taxa[$i] )	{
		$seen_reclassification++;

		# update the occurrences table
            my $dbh_r = $dbt->dbh;
			my $sql = "UPDATE occurrences SET taxon_no=".$new_taxon_no.", modifier=".$dbh->quote($s->get('enterer')).", modifier_no=".$s->get('enterer_no');
			if ( $old_taxon_no > 0 )	{
				$sql .= " WHERE taxon_no=" . $old_taxon_no;
			} else	{
				$sql .= " WHERE taxon_no=0";
			}
			if ($occurrences[$i] =~ /^\d+$/) {
				$sql .= " AND occurrence_no=" . $occurrences[$i];
				$dbh_r->do($sql);
			} elsif ($occurrence_lists[$i] =~ /^[\d, ]+$/) {
				$sql .= " AND occurrence_no IN (".$occurrence_lists[$i].")";
				$dbh_r->do($sql);
			} else {
				die ("Error: No occurrence number found for $occurrence_description");
			}
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
			my $dbh_r = $dbt->dbh;
			my $sql = "UPDATE reidentifications SET taxon_no=".$new_taxon_no.
                   ", modifier=".$dbh->quote($s->get('enterer')).
			       ", modifier_no=".$s->get('enterer_no');
			if ( $old_taxon_no > 0 )	{
				$sql .= " WHERE taxon_no=" . $old_taxon_no;
			} else	{
				$sql .= " WHERE taxon_no=0";
			}
			$sql .= " AND reid_no=" . $reids[$i];
			$dbh_r->do($sql);

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
            print "<a href=\"$WRITE_URL?action=startStartReclassifyOccurrences&occurrences_authorizer_no=".$q->param('occurrences_authorizer_no')."&collection_no=";
            print $q->param('collection_no');
            print "\">Reclassify this collection</a> - ";
        } else {
            print "<a href=\"$WRITE_URL?action=displayCollResults&type=reclassify_occurrence&occurrences_authorizer_no=".$q->param('occurrences_authorizer_no')."&taxon_name=";
            print $q->param('taxon_name');
            print "\">Reclassify ".$q->param('taxon_name')."</a> - ";
        }
    	print "<a href=\"$WRITE_URL?action=startStartReclassifyOccurrences\">Reclassify another collection or taxon</a></p>\n\n";
    }

	print "</center>\n\n";
	print $hbo->stdIncludes($PAGE_BOTTOM);

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
		$html .= "<input type=\"hidden\" $disabled name=\"$OCCURRENCE_NO\" value=\"$key_no\">\n";
        $html .= "<select $disabled name=\"taxon_no\">";
    }
                 
    # populate the select list of authorities
    foreach my $m (@$matches) {
        my $t = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$m->{'taxon_no'}},['taxon_no','taxon_name','taxon_rank','author1last','author2last','otherauthors','pubyr']);
        # have to format the authority data
        my $authority = Taxon::formatTaxon($dbt,$t);

        $html .= "<option value=\"" . $t->{taxon_no} . "+" . escapeHTML($authority) . "\"";
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
