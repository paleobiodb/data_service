package PrintHierarchy;

use TaxonInfo;
use TaxaCache;
use Classification;
use strict;

sub startPrintHierarchy	{
	my $hbo = shift;
	print main::stdIncludes("std_page_top");
    print $hbo->populateHTML('print_hierarchy_form');
	print main::stdIncludes("std_page_bottom");
	return;
}

sub processPrintHierarchy	{
	my $dbh = shift;
	my $q = shift;
	my $dbt = shift;

	my $OUT_HTTP_DIR = "/paleodb/data";
	my $OUT_FILE_DIR = $ENV{DOWNLOAD_OUTFILE_DIR};

	my %shortranks = ("subspecies"=>"","species" => "", 
            "subgenus" => "Subg.", "genus" => "G.",
			"subtribe"=> "Subtr.", "tribe" => "Tr.", 
            "subfamily" => "Subfm.", "family" => "Fm.","superfamily" => "Superfm." ,
			"infraorder" => "Infraor.", "suborder" => "Subor.", "order" => "Or.", "superorder" => "Superor.",
			"infraclass" => "Infracl.", "subclass" => "Subcl.", "class" => "Cl.", "superclass" => "Supercl.",
			"subphylum" => "Subph.", "phylum" => "Ph.");

    my %rank_order = TaxonInfo::rankOrder();

	print main::stdIncludes( "std_page_top" );

    # get focal taxon name from query parameters, then figure out taxon number
    my @taxa = ();
    my ($taxon_no,$taxon_name,$taxon_rank) = (undef,"","");
    my $reference_no = int($q->param('reference_no'));
    $reference_no = undef unless $reference_no;
    if (int($q->param('taxon_no'))) {
        @taxa = TaxonInfo::getTaxa($dbt,{'taxon_no'=>int($q->param('taxon_no'))});
        if (@taxa) {
            $taxon_no   = $taxa[0]->{'taxon_no'};
            $taxon_name = $taxa[0]->{'taxon_name'};
            $taxon_rank = $taxa[0]->{'taxon_rank'};
        }
    } elsif ($q->param('taxon_name') =~ /^([A-Za-z -]+)$/) {
        @taxa = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$1});
        if (@taxa) {
            $taxon_no   = $taxa[0]->{'taxon_no'};
            $taxon_name = $taxa[0]->{'taxon_name'};
            $taxon_rank = $taxa[0]->{'taxon_rank'};
        }
    } elsif ($reference_no) {
        @taxa = getRootTaxa($dbt,$reference_no);
    }

	if (! @taxa) {
        if (int($q->param('taxon_no')) || $q->param('taxon_name') =~ /^([A-Za-z -]+)$/) {
		    print "<center><h3>No classification is available for this taxon</h3>\n";
        } elsif ($reference_no) {
		    print "<center><h3>This reference has no taxonomic opinions</h3>\n";
        } else {
		    print "<center><h3>No query was entered</h3>\n";
        }
		print "<p>You may want to <a href=\"bridge.pl?action=startStartPrintHierarchy\">try again</a></p></center>\n";
		print main::stdIncludes( "std_page_bottom" );
		exit;
	}

    if ($taxon_name) {
        print '<div align="center"><h3>Classification of ';
        if ( $taxon_rank !~ /genus|species/) {
            print "the ";
        }
        print $taxon_name;
    } else {
        print '<div align="center"><h3>Classification';
    }
    if ($reference_no) {
        my $shortref = Reference::formatShortRef($dbt,$reference_no);
        print " from $shortref";
    }
    print "</h3></div>";

    print "<div align=\"center\"><table>";
    open OUT, ">$OUT_FILE_DIR/classification.csv";
    @taxa = sort {$a->{'taxon_name'} cmp $b->{'taxon_name'}} @taxa;
    foreach my $taxon (@taxa) {
        my $taxon_no = $taxon->{'taxon_no'};
        print "<tr><td align=\"left\">";
        my $MAX = $q->param('maximum_levels');
        my $MAX_SEEN = 0;

        my $orig_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no,$reference_no);
        my $tree;
        if ($reference_no) {
            $tree = Classification::getChildren($dbt,$orig_no,'tree',999,$reference_no);
        } else {
            $tree = TaxaCache::getChildren($dbt,$orig_no,'tree');
        }
        
        $tree->{'depth'} = 0;
        my $root_status = getStatus($dbt,$orig_no,$reference_no);
        $tree->{'status'} = $root_status if ($root_status =~ /nomen/);
        my @node_stack = ($tree);
        # mark higher level taxa that had all their children removed as "invalid"
        my %not_found_type_taxon = ();
        my %found_type_for = ();
        my %not_found_type_for = ();
        my @check_is_disused = ();
        my @check_has_nomen = ();
        while (@node_stack) {
            my $node = shift @node_stack;
            if ($node->{'depth'} < $MAX) {
                foreach my $child (@{$node->{'children'}}) {
                    $child->{'depth'} = $node->{'depth'} + 1;
                    if ($child->{'depth'} > $MAX_SEEN) {
                        $MAX_SEEN = $child->{'depth'};
                    }
                }
                unshift @node_stack,@{$node->{'children'}};
                unshift @node_stack,@{$node->{'synonyms'}};
            }
            if ($node->{'taxon_rank'} !~ /species|genus/ && !scalar(@{$node->{'children'}})) {
                push @check_is_disused,$node->{'taxon_no'};
            }
            if ($node->{'taxon_rank'} !~ /species/) {
                push @check_has_nomen,$node->{'taxon_no'};
            }
            
            if ($node->{'type_taxon_no'}) {
                my $type_taxon_no = $node->{'type_taxon_no'};
                my $found_type = 0;
                my @child_queue = ();
                push @child_queue,$_ foreach (@{$node->{'children'}});
                # Recursively search all children for the type taxon. We don't check
                # synonyms though, expect the author to reclassify the taxa into the senior 
                # synonym instead
                while (@child_queue) {
                    my $child = shift @child_queue;
                    foreach (@{$child->{'synonyms'}}) {
                        push @child_queue,$_;
                    }
                    foreach (@{$child->{'children'}}) {
                        push @child_queue,$_;
                    }
                    
                    if ($child->{'taxon_no'} == $type_taxon_no) {
    #                    print "<span style=\"color: red;\">FOUND TYPE $type_taxon_no for $node->{taxon_no},$node->{taxon_name}</span><BR>";
                        $found_type = $child->{'taxon_no'};
                        @child_queue = ();
                        last;
                    }
                    foreach my $spelling (@{$child->{'spellings'}}) {
                        if ($spelling->{'taxon_no'} == $type_taxon_no) {
    #                        print "<span style=\"color: red;\">FOUND TYPE $type_taxon_no SPELLED $child->{taxon_no},$child->{taxon_name} for $node->{taxon_no},$node->{taxon_name}</span><BR>";
                            $found_type = $child->{'taxon_no'};
                            @child_queue = ();
                            last;
                        }
                    }
                }
                if (!$found_type) {
    #                print "<span style=\"color: red;\"> COULD NOT FIND TYPE $type_taxon_no for $node->{taxon_no},$node->{taxon_name}</span><BR>";
                    $not_found_type_taxon{$node->{'taxon_no'}} = $type_taxon_no;
                    $not_found_type_for{$type_taxon_no} = $node->{'taxon_no'};
                } else {
                    #$found_type_taxon{$node->{'taxon_no'}} = $type_taxon_no;
                    # Note that $found_type will not equal to $type_taxon_no if $type_taxon_no
                    # is the original combination no -- it will be the most current spelling_no for that combination
                    $found_type_for{$found_type} = $node->{'taxon_no'};
                }
            }
        } 

        my %disused = %{TaxonInfo::disusedNames($dbt,\@check_is_disused)};
        my %nomen = %{TaxonInfo::nomenChildren($dbt,\@check_has_nomen)};

        # Tricky part: integrate in synonyms and nomen dubiums seamlessly into tree so they
        # get printed out correctly below
        @node_stack = ($tree);
        my @nodes_to_print = ();
        my %syn_status = ();
        while (@node_stack) {
            my $node = shift @node_stack;
            push @nodes_to_print, $node;
            if ($node->{'depth'} < $MAX) {
                # This block is a little redundant but sometimes needed if the synonym has children
                foreach my $child (@{$node->{'children'}}) {
                    $child->{'depth'} = $node->{'depth'} + 1;
                    if ($child->{'depth'} > $MAX_SEEN) {
                        $MAX_SEEN = $child->{'depth'};
                    }
                }
                my @children = @{$node->{'children'}};
                
                foreach (@{$nomen{$node->{'taxon_no'}}}) {
                    $_->{'status'} = getStatus($dbt,$_->{'taxon_no'},$reference_no);
                    $_->{'depth'} = $node->{'depth'} + 1;
                }
                push @children, @{$nomen{$node->{'taxon_no'}}};

                foreach (@{$node->{'synonyms'}}) {
                    $_->{'status'} = getStatus($dbt,$_->{'taxon_no'},$reference_no);
                    $_->{'depth'} = $node->{'depth'} + 1;
                }
                push @children, @{$node->{'synonyms'}};

                @children = sort {$rank_order{$b->{'taxon_rank'}} <=> $rank_order{$a->{'taxon_rank'}} ||
                                  $a->{'taxon_name'} cmp $b->{'taxon_name'}} @children;
                unshift @node_stack, @children;
            }
        }

        # now print out the data
        print "<table>\n";
        print "<tr>";
        for (my $i=0;$i<=$MAX+5 && $i <= $MAX_SEEN+5;$i++) {
               print "<td style=\"width:20;\">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>"; 
        }
        print "</tr>";
        #my %type_taxon_nos = ();
        #while (my ($type,$for) = each %found_type_taxon) {
        #    $type_taxon_nos{$for} = $type;
        #}
        foreach my $record (@nodes_to_print)	{
            print "<tr>";
            for (my $i=0;$i<$record->{'depth'};$i++) {
                print "<td></td>";
            }
            print "<td style=\"white-space: nowrap;\" colspan=".($MAX + 5 - $record->{'depth'}).">";
            my $shortrank = $shortranks{$record->{'taxon_rank'}};
            my $title = ($shortrank) ? "<b>$shortrank</b> " : "";
            my $taxon_name = $record->{'taxon_name'};

            if ($record->{'type_taxon_no'} && $not_found_type_taxon{$record->{'taxon_no'}}) {
                $taxon_name = '"'.$taxon_name.'"';
            }
            
            my $link = "<a href=\"bridge.pl?action=checkTaxonInfo&amp;taxon_no=$record->{taxon_no}\">$taxon_name</a>";
            if ( $record->{'taxon_rank'} =~ /species|genus/ ) {
                $title .= "<i>".$link."</i>";
            } else {
                $title .= $link;
            }
            print $title;

            if ($disused{$record->{'taxon_no'}}) {
                print " <small>(disused)</small>";
            }
            if ($record->{'status'}) {
                print " <small>($record->{status})</small>";
            }
    #        my $type_taxon_for = $type_taxon_nos{$record->{'taxon_no'}};
    #        if ($type_taxon_for) {
                if ($found_type_for{$record->{'taxon_no'}}) {
                    print " <small>(type)</small>";
                } 
                #elsif ($not_found_type_for{$record->{'taxon_no'}}) {
                #    my $type_taxon_for = $not_found_type_for{$record->{'taxon_no'}};
                #    my $t = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$type_taxon_for});
                #    print " <small>(excludes $t->{taxon_name}, the type)</small>";
                #}
    #        }
            if ($record->{'type_taxon_no'} && $not_found_type_taxon{$record->{'taxon_no'}}) {
                my $t = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$record->{'type_taxon_no'}});
                if ($t) {
                    my $link = "<a href=\"bridge.pl?action=checkTaxonInfo&amp;taxon_no=$t->{taxon_no}\">$t->{taxon_name}</a>";
                    if ( $t->{'taxon_rank'} =~ /species|genus/ ) {
                        $link = "<i>".$link."</i>";
                    } 
                    print "<small>";
                    print " (excludes $link, the type)";
                    print "</small>";
                }
            }

            #if (@{$record->{'spellings'}}) {
            #    print " [";
            #    print "=$_->{taxon_name}, " for (@{$record->{'spellings'}});
            #    print "]";
            #}
            
            print OUT $record->{'taxon_rank'}.",".$record->{'taxon_name'}."\n";
        }
        print OUT "\n";
	    print "</table>\n";
        print "</td></tr>";
    }
    print "</table></div>";
	close OUT;

	chmod 0664, "$OUT_FILE_DIR/classification.csv";

	print "<hr><p><b><a href=\"$OUT_HTTP_DIR/classification.csv\">Download</a></b> this list of taxonomic names</p>";
    print '<p><b><a href=# onClick="javascript: document.doDownloadTaxonomy.submit()">Download</a></b> authority and opinion data for these taxa</p>';
    print '<form method="POST" action="bridge.pl" name="doDownloadTaxonomy">';
    print '<input type="hidden" name="action" value="displayDownloadTaxonomyResults">';
    if ($taxon_no) {
        print qq|<input type="hidden" name="taxon_no" value="$taxon_no">|;
    }
    if ($reference_no) {
        print qq|<input type="hidden" name="reference_no" value="$reference_no">|;
    }
    print '</form>'; 

	print "<p>You may <b><a href=\"bridge.pl?action=startStartPrintHierarchy\">classify another taxon</a></b></p>";
	print main::stdIncludes( "std_page_bottom" );

	return;
}

sub getStatus {
    my ($dbt,$taxon_no,$reference_no) = @_;
    # Not very efficient, but no better way to get the status I don't think
    my $orig_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no,$reference_no);
    my $status = "";
    if ($orig_no) {
        my $mrpo = TaxonInfo::getMostRecentClassification($dbt,$orig_no,$reference_no);
        if ($mrpo) {
            if ($mrpo->{'status'} =~ /subjective/) { $status = 'subjective synonym'; }
            elsif ($mrpo->{'status'} =~ /objective/) { $status = 'objective synonym'; }
            elsif ($mrpo->{'status'} =~ /subgroup/) { $status = 'invalid subgroup'; }
            elsif ($mrpo->{'status'} =~ /replaced/) { $status = 'replacement'; }
            else { $status = "$mrpo->{status}";}
        } else {
            $status = 'synonym';
        }
    } else {
        $status = 'synonym';
    }
    return $status;
}

sub getRootTaxa {
    my ($dbt,$ref) = @_;
    my $sql = "SELECT DISTINCT child_no FROM opinions WHERE reference_no=".int($ref); 
    my @results = @{$dbt->getData($sql)};
    my %all_child_nos = ();
    foreach my $row (@results) {
        my $mrpo = TaxonInfo::getMostRecentClassification($dbt,$row->{child_no},$ref);
        if ($mrpo) {
            $all_child_nos{$row->{'child_no'}} = $mrpo;
        }
    }
    my @root = ();
    while (my ($child_no,$mrpo) = each %all_child_nos) {
        my $parent_no = $mrpo->{'parent_no'};
        unless ($all_child_nos{$parent_no}) {
            push @root, $parent_no if ($parent_no);
        }
    }
    my %all_parent_nos = ();
    foreach my $parent_no (@root) {
        $all_parent_nos{$parent_no} = 1;
    }
    my @taxon_nos = sort keys %all_parent_nos;
    my @taxa = ();
    foreach my $t (@taxon_nos) {
        push @taxa, (TaxonInfo::getTaxa($dbt,{'taxon_no'=>$t}));
    }
    return @taxa;
}


1;
