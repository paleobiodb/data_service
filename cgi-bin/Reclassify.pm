package Reclassify;

# written by JA 31.3, 1.4.04
# in memory of our dearly departed Ryan Poling

# start the process to get to the reclassify occurrences page
# modelled after startAddEditOccurrences
sub startReclassifyOccurrences	{

	my $q = shift;
	my $s = shift;
	my $dbh = shift;
	my $dbt = shift;


	# have to be logged in
	if (!$s->isDBMember()) {
		$s->enqueue( $dbh, "action=startStartReclassifyOccurrences" );
		main::displayLoginPage( "Please log in first." );
		exit;
	}
	# if they have the collection number, they'll immediately go to the
	#  reclassify page
	elsif ( $q->param("collection_no") )	{
		&displayOccurrenceReclassify($q,$s,$dbh,$dbt);
		exit;
	}
	# otherwise, they'll need to search for the collection
	# the "type" will be passed along by displaySearchColls and
	#  used by displayCollResults
	else	{
		$q->param(type => 'reclassify_occurrence');
		main::displaySearchColls();
		exit;
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
    my @collections = @$collections_ref;

	print main::stdIncludes("std_page_top");

    my @occrefs;
    if (@collections) {
	    print "<center><h3>Classification of ".$q->param('taxon_name')."</h3>";
        my ($genus,$species) = split(/\s+/,$q->param('taxon_name'));
        my $sql = "(SELECT 0 reid_no, o.authorizer_no, o.occurrence_no,o.taxon_no, o.genus_reso, o.genus_name, o.species_reso, o.species_name, c.collection_no, c.collection_name, c.country, c.state, c.max_interval_no, c.min_interval_no FROM occurrences o, collections c WHERE o.collection_no=c.collection_no AND c.collection_no IN (".join(", ",@collections).") AND o.genus_name LIKE ".$dbh->quote($genus);
        if ($species) {
            $sql .= " AND o.species_name LIKE ".$dbh->quote($species);
        }
        $sql .= ")";
        $sql .= " UNION ";
        $sql .= "( SELECT re.reid_no, re.authorizer_no,re.occurrence_no,re.taxon_no, re.genus_reso, re.genus_name, re.species_reso, re.species_name, c.collection_no, c.collection_name, c.country, c.state, c.max_interval_no, c.min_interval_no FROM reidentifications re, occurrences o, collections c WHERE re.occurrence_no=o.occurrence_no AND o.collection_no=c.collection_no AND c.collection_no IN (".join(", ",@collections).") AND re.genus_name LIKE ".$dbh->quote($genus);
        if ($species) {
            $sql .= " AND re.species_name LIKE ".$dbh->quote($species);
        }
        $sql .= ") ORDER BY occurrence_no ASC, reid_no ASC";
        main::dbg("Reclassify sql:".$sql);
        @occrefs = @{$dbt->getData($sql)};
    } else {
	    my $sql = "SELECT collection_name FROM collections WHERE collection_no=" . $q->param('collection_no');
	    my $coll_name = ${$dbt->getData($sql)}[0]->{collection_name};
	    print "<center><h3>Classification of taxa in collection ",$q->param('collection_no')," ($coll_name)</h3>";

        # get all the occurrences
        my $collection_no = int($q->param('collection_no'));
        $sql = "(SELECT 0 reid_no,authorizer_no, occurrence_no,taxon_no,genus_reso,genus_name,species_reso,species_name FROM occurrences WHERE collection_no=$collection_no)".
               " UNION ".
               "(SELECT reid_no,authorizer_no, occurrence_no,taxon_no,genus_reso,genus_name,species_reso,species_name FROM reidentifications WHERE collection_no=$collection_no)".
               " ORDER BY occurrence_no ASC,reid_no ASC";
        main::dbg("Reclassify sql:".$sql);
        @occrefs = @{$dbt->getData($sql)};
    }

	# tick through the occurrences
	# NOTE: the list will be in data entry order, nothing fancy here
	if ( @occrefs )	{
		print "<hr>\n";
		print "<form method=\"post\">\n";
		print "<input id=\"action\" type=\"hidden\" name=\"action\" value=\"startProcessReclassifyForm\">\n";
        if (@collections) {
            print "<input type=\"hidden\" name=\"taxon_name\" value=\"".$q->param('taxon_name')."\">";
        } else {
            print "<input type=\"hidden\" name=\"collection_no\" value=\"".$q->param('collection_no')."\">";
        }
		print "<table border=0 cellpadding=0 cellspacing=0>\n";
	}

    # Make non-editable links not changeable
    my $p = Permissions->new($s,$dbt);
    my %is_modifier_for = %{$p->getModifierList()};

	my $rowcolor = 0;
    my $nonEditableCount = 0;
	for my $o ( @occrefs )	{
        my $editable = ($s->get("superuser") || $is_modifier_for{$o->{'authorizer_no'}} || $o->{'authorizer_no'} == $s->get('authorizer_no')) ? 1 : 0;
        my $disabled = ($editable) ?  '' : 'DISABLED';
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
			$taxon_name = $o->{genus_name};
			if ( $o->{species_reso} !~ /informal/ && $o->{species_name} ne "sp." && $o->{species_name} ne "indet." )	{
				$taxon_name .= " " . $o->{species_name};
			}

			# find taxon names that match
			$sql = "SELECT taxon_no,ref_is_authority,authorities.author1last as aa1,authorities.author2last as aa2,authorities.otherauthors as aoa,authorities.pubyr as ayr,refs.author1last as ra1,refs.author2last as ra2,refs.otherauthors as roa,refs.pubyr as ryr FROM authorities,refs WHERE refs.reference_no=authorities.reference_no AND taxon_name='$taxon_name'";
			@taxnorefs = @{$dbt->getData($sql)};

			# didn't work? maybe the genus is known even though
			#  the species isn't
			$usedGenus = "";
			if ( ! @taxnorefs && $taxon_name =~ / / )	{
				($taxon_name) = split / /,$taxon_name;
				$sql = "SELECT taxon_no,ref_is_authority,authorities.author1last as aa1,authorities.author2last as aa2,authorities.otherauthors as aoa,authorities.pubyr as ayr,refs.author1last as ra1,refs.author2last as ra2,refs.otherauthors as roa,refs.pubyr as ryr FROM authorities,refs WHERE refs.reference_no=authorities.reference_no AND taxon_name='$taxon_name'";
				@taxnorefs = @{$dbt->getData($sql)};
				$usedGenus = "YES";
			}

			# now print the name and the pulldown of authorities
			if ( @taxnorefs )	{
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
                    $collection_string .= " $authorizer";
                    $collection_string .= "</span>";

                    print "<td style=\"padding-right: 1.5em; padding-left: 1.5em;\"><a href=\"bridge.pl?action=displayCollectionDetails&collection_no=$o->{collection_no}\">$o->{collection_no}</td><td>$collection_string</a></td>";
                }
				print "<td nowrap>&nbsp;&nbsp;\n";

				# here's the name
				my $formatted = "";
				if ( $o->{species_name} ne "indet." )	{
					$formatted .= "<i>";
				}
				$formatted .= "$o->{genus_reso} $o->{genus_name} $o->{species_reso} $o->{species_name}";
				if ( $o->{species_name} ne "indet." )	{
					$formatted .= "</i>";
				}

				# need a hidden recording the old taxon number
                $collection_string .= ": " if ($collection_string);
                $collection_string =~ s/'//g;
                 
				if ( ! $o->{reid_no} )	{
					print "<input type='hidden' $disabled name='old_taxon_no' value='" , $o->{taxon_no}, "+" , $collection_string, $formatted , "'>\n";
				} else	{
					print "<input type='hidden' $disabled name='old_reid_taxon_no' value='" , $o->{taxon_no}, "+" , $collection_string, $formatted , "'>\n";
				}

				# need a hidden recording the occurrence number
				#  or reID number as appropriate
				if ( ! $o->{reid_no} )	{
					print "<input type='hidden' $disabled name='occurrence_no' value='" , $o->{occurrence_no}, "'>\n";
				} else	{
					print "<input type='hidden' $disabled name='reid_no' value='" , $o->{reid_no}, "'>\n";
					print "&nbsp;&nbsp;<span class='small'><b>reID =</b></span>&nbsp;";
				}

				print $formatted;
				print "</td>\n";

				# start the select list
				# the name depends on whether this is
				#  an occurrence or reID
				if ( ! $o->{reid_no} )	{
					print "<td>&nbsp;&nbsp;\n<select $disabled name='taxon_no'>\n";
				} else	{
					print "<td>&nbsp;&nbsp;\n<select $disabled name='reid_taxon_no'>\n";
				}
				# populate the select list of authorities
				for my $t ( @taxnorefs )	{
					# have to format the authority data
					my $authority = "";
					if ( $usedGenus )	{
						$authority = "genus: ";
						$genusOnly++;
					}
					# first try getting ref data from the
					#  the reference for the record
					if ( $t->{ref_is_authority} eq "YES" )	{ 
						$authority .= $t->{ra1};
						if ( $t->{roa} )	{
							$authority .= " et al.";
						} elsif ( $t->{ra2} )	{
							$authority .= " and " . $t->{ra2};
						}
						$authority .= " " . $t->{ryr};
					}
					# failing that, use the directly
					#  recorded values
					elsif ( $t->{aa1} )	{
						$authority .= $t->{aa1};
						if ( $t->{aoa} )	{
							$authority .= " et al.";
						} elsif ( $t->{aa2} )	{
							$authority .= " and " . $t->{aa2};
						}
						$authority .= " " . $t->{ayr};
					}

                	# needed by Classification
                	$levels = "class,order,family";
                    my %master_class=%{Classification::get_classification_hash($dbt, $levels, [ $t->{taxon_no} ] )};

					my @parents = split(/,/,$master_class{$t->{taxon_no}},-1);
					if ( @parents )	{
						$authority .= " (";
						if ( $parents[2] ) { $authority .= $parents[2] . ", "; }
						if ( $parents[1] ) { $authority .= $parents[1] . ", "; }
						if ( $parents[0] ) { $authority .= $parents[0]; }
						$authority =~ s/, $//;
						$authority .= ")";
					}
					if ( $authority !~ /[A-Za-z]/ )	{
						$authority = "taxon number " . $t->{taxon_no};
					}
					# clean up in case there's a
					#  classification but no author
					$authority =~ s/^ //;

					print "<option value='" , $t->{taxon_no} , "+" , $authority , "'";
					if ( $t->{taxon_no} eq $o->{taxon_no} )	{
						print " selected";
					}
					print ">";
					print $authority , "\n";
				}
				if ( $o->{taxon_no} )	{
					print "<option value='0'>leave unclassified\n";
				} else	{
					print "<option value='0' selected>leave unclassified\n";
				}
				print "</select>&nbsp;&nbsp;</td>\n";
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
	if ( $genusOnly )	{
		print "<p><i>\"genus\" means that only the genus could be classified</i></p>\n";
	}
    if ( $nonEditableCount) {
        print "<p><i>Some occurrences can't be reclassified because they have a different authorizer</i></p>\n";
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
		if ( $b->{species_name} ne "indet." )	{
			print "<i>";
		}
		print "$b->{genus_reso} $b->{genus_name} $b->{species_reso} $b->{species_name}\n";
		if ( $b->{species_name} ne "indet." )	{
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

	print main::stdIncludes("std_page_top");

	print "<center>\n\n";

    if ($q->param('collection_no')) {
        my $sql = "SELECT collection_name FROM collections WHERE collection_no=" . $q->param('collection_no');
        my $coll_name = ${$dbt->getData($sql)}[0]->{collection_name};
        print "<h3>Taxa reclassified in collection " , $q->param('collection_no') ," (" , $coll_name , ")</h3>\n\n";
    } else {
        print "<h3>Taxa reclassified for " , $q->param('taxon_name') ,"</h3>\n\n";
    }

	# get lists of old and new taxon numbers
	# WARNING: taxon names are stashed in old numbers and authority info
	#  is stashed in new numbers
	@old_taxa = $q->param('old_taxon_no');
	@new_taxa = $q->param('taxon_no');
	@occurrences = $q->param('occurrence_no');
	@old_reid_taxa = $q->param('old_reid_taxon_no');
	@new_reid_taxa = $q->param('reid_taxon_no');
	@reids = $q->param('reid_no');

	print "<table border=0 cellpadding=2 cellspacing=0>\n";
	my $rowcolor = 0;

	# first tick through the occurrence taxa and update as appropriate
	for $i (0..$#old_taxa)	{
		my ($old_taxon_no,$taxon_name) = split /\+/,$old_taxa[$i];
		my ($new_taxon_no,$authority) = split /\+/,$new_taxa[$i];
		if ( $old_taxa[$i] != $new_taxa[$i] )	{

		# update the occurrences table
			$sql = "UPDATE occurrences SET taxon_no=".$new_taxon_no.
                   ", modifier=".$dbh->quote($s->get('enterer')).
			       ", modifier_no=".$s->get('enterer_no');
			if ( $old_taxon_no > 0 )	{
				$sql .= " WHERE taxon_no=" . $old_taxon_no;
			} else	{
				$sql .= " WHERE taxon_no=0";
			}
			$sql .= " AND occurrence_no=" . $occurrences[$i];
			$dbt->getData($sql);

		# print the taxon's info
			if ( $rowcolor % 2 )	{
				print "<tr>";
			} else	{
				print "<tr class='darkList'>";
			}
			print "<td>&nbsp;&nbsp;$taxon_name $authority&nbsp;&nbsp;</td>\n";
			print "</tr>\n";
			$rowcolor++;
		}
	}

	# then tick through the reidentification taxa and update as appropriate
	# WARNING: this isn't very slick; all the reIDs always come after
	#  all the occurrences
	for $i (0..$#old_reid_taxa)	{
		my ($old_taxon_no,$taxon_name) = split /\+/,$old_reid_taxa[$i];
		my ($new_taxon_no,$authority) = split /\+/,$new_reid_taxa[$i];
		if ( $old_reid_taxa[$i] != $new_reid_taxa[$i] )	{

		# update the reidentifications table
			$sql = "UPDATE reidentifications SET taxon_no=".$new_taxon_no.
                   ", modifier=".$dbh->quote($s->get('enterer')).
			       ", modifier_no=".$s->get('enterer_no');
			if ( $old_taxon_no > 0 )	{
				$sql .= "' WHERE taxon_no=" . $old_taxon_no;
			} else	{
				$sql .= "' WHERE taxon_no=0";
			}
			$sql .= " AND reid_no=" . $reids[$i];
			$dbt->getData($sql);

		# print the taxon's info
			if ( $rowcolor % 2 )	{
				print "<tr>";
			} else	{
				print "<tr class='darkList'>";
			}
			print "<td>&nbsp;&nbsp;$taxon_name $authority&nbsp;&nbsp;</td>\n";
			print "</tr>\n";
			$rowcolor++;
		}
	}

	print "</table>\n\n";

	print "<p><a href=\"$exec_url?action=startStartReclassifyOccurrences&collection_no=";
	print $q->param('collection_no');
	print "\"><b>Reclassify this collection</b></a> - ";
	print "<a href=\"$exec_url?action=startStartReclassifyOccurrences\"><b>Reclassify another collection</b></a></p>\n\n";

	print "<center>\n\n";

	print main::stdIncludes("std_page_bottom");

}


1;
