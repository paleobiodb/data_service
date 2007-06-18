package PrintHierarchy;

use TaxonInfo;
use Taxon;
use TaxaCache;
use Classification;
use Constants qw($READ_URL $WRITE_URL $HTML_DIR);
use strict;

sub startPrintHierarchy	{
	my $hbo = shift;
	my $s = shift;
	my $error = shift;
	my %refno;
	$refno{'current_ref'} = $s->get('reference_no');
	if ( $s->get('enterer_no') > 0 )	{
		$refno{'not_guest'} = 1;
	}
	$refno{'error_message'} = $error;
	print $hbo->populateHTML('print_hierarchy_form',\%refno);
}

sub processPrintHierarchy	{
    my ($q,$s,$dbt,$hbo) = @_;

    my $classification_file = "classification.csv";

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
        @taxa = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$1,'remove_rank_change'=>1});
        if (@taxa) {
            $taxon_no   = $taxa[0]->{'taxon_no'};
            $taxon_name = $taxa[0]->{'taxon_name'};
            $taxon_rank = $taxa[0]->{'taxon_rank'};
        }
    } elsif ($reference_no) {
        @taxa = getRootTaxa($dbt,$reference_no);
    }

	if (! @taxa)	{
		my $error = qq|<p class="medium"><i>|;
		if (int($q->param('taxon_no')) || $q->param('taxon_name') =~ /^([A-Za-z -]+)$/)	{
			$error .= "No classification is available for this taxon";
		} elsif ($reference_no)	{
			$error .= "This reference has no taxonomic opinions";
		} else	{
			$error .= "No query was entered";
		}
		$error .= ". Please try again.</i></p>\n\n";
		startPrintHierarchy($hbo,$s,$error);
		return;
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
        print " of $shortref";
    }
    print "</h3></div>";


    print "<div align=\"center\"><table>";
    @taxa = sort {$a->{'taxon_name'} cmp $b->{'taxon_name'}} @taxa;
    PBDBUtil::autoCreateDir("$HTML_DIR/public/classification");
    my $OUT;
    open $OUT, ">$HTML_DIR/public/classification/$classification_file";
    foreach my $taxon (@taxa) {
        my $taxon_no = $taxon->{'taxon_no'};

        my $orig_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no,$reference_no);
        my $tree;
        if ($reference_no) {
            $tree = Classification::getChildren($dbt,$orig_no,'tree',999,$reference_no);
        } else {
            $tree = TaxaCache::getChildren($dbt,$orig_no,'tree');
        }
        $tree->{'taxon_no'}=>$orig_no;
        print "<tr><td align=\"left\">";
        my $options = {
            'max_levels'=>$q->param('maximum_levels'),
            'outfile'=>$OUT,
            'reference_no'=>$reference_no
        };
        print htmlTaxaTree($dbt,$tree,$options);
        print "</td></tr>";
        print $OUT "\n";
    }
    close $OUT;
	chmod 0664, "$HTML_DIR/public/classification/$classification_file";
    print "</table></div>";

	print "<hr><div style=\"padding-left: 2em;\"><p><b><a href=\"/public/classification/$classification_file\">Download</a></b> this list of taxonomic names</p>";
    print '<p><b><a href=# onClick="javascript: document.doDownloadTaxonomy.submit()">Download</a></b> authority and opinion data for these taxa</p>';
    print qq|<form method="POST" action="$READ_URL" name="doDownloadTaxonomy">|;
    print '<input type="hidden" name="action" value="displayDownloadTaxonomyResults">';
    if ($taxon_no) {
        print qq|<input type="hidden" name="taxon_no" value="$taxon_no">|;
    }
    if ($reference_no) {
        print qq|<input type="hidden" name="reference_no" value="$reference_no">|;
    }
    print '</form>'; 

	print "<p><b><a href=\"$READ_URL?action=startStartPrintHierarchy\">See another classification</a></b></p>";
	print "</div>\n";
}

sub getStatus {
    my ($dbt,$taxon_no,$reference_no) = @_;
    # Not very efficient, but no better way to get the status I don't think
    my $orig_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no,$reference_no);
    my $status = "";
    if ($orig_no) {
        my $mrpo = TaxonInfo::getMostRecentClassification($dbt,$orig_no,{'reference_no'=>$reference_no});
        if ($mrpo) {
            if ($mrpo->{'status'} =~ /subjective/) { $status = 'subjective synonym'; }
            elsif ($mrpo->{'status'} =~ /objective/) { $status = 'objective synonym'; }
            elsif ($mrpo->{'status'} =~ /subgroup/) { $status = 'invalid subgroup'; }
            elsif ($mrpo->{'status'} =~ /replaced/) { $status = 'replaced homonym'; }
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
        my $mrpo = TaxonInfo::getMostRecentClassification($dbt,$row->{child_no},{'reference_no'=>$ref});
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


sub htmlTaxaTree {
    my ($dbt,$tree,$options) = @_;

    my $html = "";
    my $reference_no = $options->{'reference_no'};
    my $MAX = $options->{'max_levels'} || 99;
    my $SIMPLE = $options->{'simple'};
    my $OUT = $options->{'outfile'};

	my %shortranks = ("subspecies"=>"","species" => "", 
            "subgenus" => "Subg.", "genus" => "G.",
			"subtribe"=> "Subtr.", "tribe" => "Tr.", 
            "subfamily" => "Subfm.", "family" => "Fm.","superfamily" => "Superfm." ,
			"infraorder" => "Infraor.", "suborder" => "Subor.", "order" => "Or.", "superorder" => "Superor.",
			"infraclass" => "Infracl.", "subclass" => "Subcl.", "class" => "Cl.", "superclass" => "Supercl.",
			"subphylum" => "Subph.", "phylum" => "Ph.");

    my %rank_order = %Taxon::rankToNum;

    $html .= "<table>";
    my $MAX_SEEN = 0;

    $tree->{'depth'} = 0;
    my $root_status = getStatus($dbt,$tree->{'taxon_no'},$reference_no);
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
#                    $html .= "<span style=\"color: red;\">FOUND TYPE $type_taxon_no for $node->{taxon_no},$node->{taxon_name}</span><BR>";
                    $found_type = $child->{'taxon_no'};
                    @child_queue = ();
                    last;
                }
                foreach my $spelling (@{$child->{'spellings'}}) {
                    if ($spelling->{'taxon_no'} == $type_taxon_no) {
#                        $html .= "<span style=\"color: red;\">FOUND TYPE $type_taxon_no SPELLED $child->{taxon_no},$child->{taxon_name} for $node->{taxon_no},$node->{taxon_name}</span><BR>";
                        $found_type = $child->{'taxon_no'};
                        @child_queue = ();
                        last;
                    }
                }
            }
            if (!$found_type) {
#                $html .= "<span style=\"color: red;\"> COULD NOT FIND TYPE $type_taxon_no for $node->{taxon_no},$node->{taxon_name}</span><BR>";
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

    my %disused = (); 
    my %nomen = (); 
    unless ($reference_no || $SIMPLE) {
        %disused = %{TaxonInfo::disusedNames($dbt,\@check_is_disused)};
        %nomen = %{TaxonInfo::nomenChildren($dbt,\@check_has_nomen)};
    }

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
    $html .= "<table>\n";
    $html .= "<tr>";
    for (my $i=0;$i<=$MAX+5 && $i <= $MAX_SEEN+5;$i++) {
           $html .= "<td style=\"width:20;\">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>"; 
    }
    $html .= "</tr>";
    #my %type_taxon_nos = ();
    #while (my ($type,$for) = each %found_type_taxon) {
    #    $type_taxon_nos{$for} = $type;
    #}
    my @taxon_nos;
    foreach my $record (@nodes_to_print)	{
        push @taxon_nos , $record->{'taxon_no'};
    }
    my $sql = "SELECT taxon_no,ref_is_authority,";
    for my $f ( 'reference_no','author1last','author2last','otherauthors','pubyr' )	{
        $sql .= "IF (a.ref_is_authority='YES',r.$f,a.$f) $f,";
    }
    $sql =~ s/,$/ /;
    $sql .= "FROM authorities a,refs r WHERE a.reference_no=r.reference_no AND taxon_no in (" . join(',',@taxon_nos) . ")";
    my @results = @{$dbt->getData($sql)};
    my %authority;
    $authority{$_->{'taxon_no'}}->{IS} = $_->{'ref_is_authority'} foreach ( @results );
    $authority{$_->{'taxon_no'}}->{REF} = $_->{'reference_no'} foreach ( @results );
    $authority{$_->{'taxon_no'}}->{A1} = $_->{'author1last'} foreach ( @results );
    $authority{$_->{'taxon_no'}}->{A2} = $_->{'author2last'} foreach ( @results );
    $authority{$_->{'taxon_no'}}->{OTHER} = $_->{'otherauthors'} foreach ( @results );
    $authority{$_->{'taxon_no'}}->{YR} = $_->{'pubyr'} foreach ( @results );
    foreach my $record (@nodes_to_print)	{
        $html .= "<tr>";
        for (my $i=0;$i<$record->{'depth'};$i++) {
            $html .= "<td></td>";
        }
        $html .= "<td style=\"white-space: nowrap;\" colspan=".($MAX + 5 - $record->{'depth'}).">";
        my $shortrank = $shortranks{$record->{'taxon_rank'}};
        my $title = ($shortrank) ? "<b>$shortrank</b> " : "";
        my $taxon_name = $record->{'taxon_name'};

        if ($record->{'type_taxon_no'} && $not_found_type_taxon{$record->{'taxon_no'}}) {
            $taxon_name = '"'.$taxon_name.'"';
        }
        
        my $link = "<a href=\"$READ_URL?action=checkTaxonInfo&amp;taxon_no=$record->{taxon_no}\">$taxon_name</a>";
        if ( $record->{'taxon_rank'} =~ /species|genus/ ) {
            $title .= "<i>".$link."</i>";
        } else {
            $title .= $link;
        }
        $html .= $title;
        if ( $authority{$record->{'taxon_no'}}->{A1} )	{
            if ( $authority{$record->{'taxon_no'}}->{IS} =~ /Y/ )	{
                $html .= qq|<a href="$READ_URL?action=displayReference&amp;reference_no=$authority{$record->{'taxon_no'}}->{REF}">|;
            }
            $html .= " <span class=\"small\">$authority{$record->{'taxon_no'}}->{A1}";
            if ( $authority{$record->{'taxon_no'}}->{OTHER} )	{
                $html .= " et al.";
            } elsif ( $authority{$record->{'taxon_no'}}->{A2} )	{
                $html .= " and " . $authority{$record->{'taxon_no'}}->{A2};
            }
            $html .= " $authority{$record->{'taxon_no'}}->{YR}</span>";
            if ( $authority{$record->{'taxon_no'}}->{IS} =~ /Y/ )	{
                $html .= "</a>";
            }
        }

        unless ($SIMPLE) {
            if ($disused{$record->{'taxon_no'}}) {
                $html .= " <small>(disused)</small>";
            } elsif ($record->{'status'}) {
                $html .= " <small>($record->{status})</small>";
            }
    #        my $type_taxon_for = $type_taxon_nos{$record->{'taxon_no'}};
    #        if ($type_taxon_for) {
                if ($found_type_for{$record->{'taxon_no'}}) {
                    $html .= " <small>(type)</small>";
                } 
                #elsif ($not_found_type_for{$record->{'taxon_no'}}) {
                #    my $type_taxon_for = $not_found_type_for{$record->{'taxon_no'}};
                #    my $t = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$type_taxon_for});
                #    $html .= " <small>(excludes $t->{taxon_name}, the type)</small>";
                #}
    #        }
            if ($record->{'type_taxon_no'} && $not_found_type_taxon{$record->{'taxon_no'}}) {
                my $t = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$record->{'type_taxon_no'}});
                if ($t) {
                    my $link = "<a href=\"$READ_URL?action=checkTaxonInfo&amp;taxon_no=$t->{taxon_no}\">$t->{taxon_name}</a>";
                    if ( $t->{'taxon_rank'} =~ /species|genus/ ) {
                        $link = "<i>".$link."</i>";
                    } 
                    $html .= "<small>";
                    $html .= " (excludes $link, the type)";
                    $html .= "</small>";
                }
            }
        }

        #if (@{$record->{'spellings'}}) {
        #    $html .= " [";
        #    $html .= "=$_->{taxon_name}, " for (@{$record->{'spellings'}});
        #    $html .= "]";
        #}
        
        if ($OUT) {
            print $OUT $record->{'taxon_rank'}.",".$record->{'taxon_name'}."\n";
        }
    }
    $html .= "</table></div>";
    return $html;
}
1;
