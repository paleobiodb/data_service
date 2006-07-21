package PrintHierarchy;

use TaxonInfo;
use PBDBUtil;
use Classification;
use strict;

# 21.9.03 JA

sub startPrintHierarchy	{

	my $dbh = shift;
	my $hbo = shift;

	print main::stdIncludes( "std_page_top" );
    print $hbo->populateHTML('print_hierarchy_form', [],[]);
	print main::stdIncludes("std_page_bottom");

	return;
}

sub processPrintHierarchy	{
	my $dbh = shift;
	my $q = shift;
	my $dbt = shift;
	my $exec_url = shift;

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
    my $ref;
    if ($q->param('taxon_no')) {
    	my $sql = "SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE taxon_no='" . $q->param('taxon_no') . "'";
	    $ref = @{$dbt->getData($sql)}[0];
    } elsif ($q->param('taxon_name')) {
    	my $sql = "SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE taxon_name='" . $q->param('taxon_name') . "'";
	    $ref = @{$dbt->getData($sql)}[0];
    }

	if ( ! $ref )	{
		print "<center><h3>Taxon not found</h3>\n";
		print "<p>You may want to <a href=\"$exec_url?action=startStartPrintHierarchy\">try again</a></p></center>\n";
		print main::stdIncludes( "std_page_bottom" );
		exit;
	}

	print "<center><h3>Classification of ";
	if ( $ref->{taxon_rank} ne "genus" )	{
		print "the ";
	}
	print $ref->{'taxon_name'} . "</h3></center>";

	my $MAX = $q->param('maximum_levels');
    my $MAX_SEEN = 0;

    my $orig_no = TaxonInfo::getOriginalCombination($dbt,$ref->{taxon_no});        
    my $tree = TaxaCache::getChildren($dbt,$orig_no,'tree');
    $tree->{'depth'} = 0;
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
                $_->{'depth'} = $node->{'depth'} + 1;
            }
            push @children, @{$nomen{$node->{'taxon_no'}}};

            foreach (@{$node->{'synonyms'}}) {
                # Not very efficient, but no better way to get the status I don't think
                my $orig_no = TaxonInfo::getOriginalCombination($dbt,$_->{'taxon_no'});
                if ($orig_no) {
                    my $mrpo = TaxonInfo::getMostRecentClassification($dbt,$orig_no);
                    if ($mrpo) {
                        if ($mrpo->{'status'} =~ /subjective/) { $_->{'status'} = 'subjective synonym'; }
                        elsif ($mrpo->{'status'} =~ /objective/) { $_->{'status'} = 'objective synonym'; }
                        elsif ($mrpo->{'status'} =~ /replaced/) { $_->{'status'} = 'replacement'; }
                        else { $_->{'status'} = "$mrpo->{status}";}
                    } else {
                        $_->{'status'} = 'synonym';
                    }
                } else {
                    $_->{'status'} = 'synonym';
                }
                $_->{'depth'} = $node->{'depth'} + 1;
            }
            push @children, @{$node->{'synonyms'}};

            @children = sort {$rank_order{$b->{'taxon_rank'}} <=> $rank_order{$a->{'taxon_rank'}} ||
                              $a->{'taxon_name'} cmp $b->{'taxon_name'}} @children;
            unshift @node_stack, @children;
        }
    }

    # now print out the data
	open OUT, ">$OUT_FILE_DIR/classification.csv";
	print "<center><table>\n";
    print "<tr>";
    for (my $i=0;$i<=$MAX+1 && $i <= $MAX_SEEN+1;$i++) {
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
		print "<td style=\"white-space: nowrap;\" colspan=".($MAX + 1 - $record->{'depth'}).">";
        my $shortrank = $shortranks{$record->{'taxon_rank'}};
        my $title = "<b>$shortrank</b> ";
        my $taxon_name = $record->{'taxon_name'};

        if ($record->{'type_taxon_no'} && $not_found_type_taxon{$record->{'taxon_no'}}) {
            $taxon_name = '"'.$taxon_name.'"';
        }
        
        my $link = "<a href=bridge.pl?action=checkTaxonInfo&taxon_no=$record->{taxon_no}>$taxon_name</a>";
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
                my $link = "<a href=bridge.pl?action=checkTaxonInfo&taxon_no=$t->{taxon_no}>$t->{taxon_name}</a>";
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
	print "</table></center><p>\n";
	close OUT;

	chmod 0664, "$OUT_FILE_DIR/classification.csv";

	print "<hr><p><b><a href=\"$OUT_HTTP_DIR/classification.csv\">Download</a></b> this list of taxonomic names</p>";
    print '<p><b><a href=# onClick="javascript: document.doDownloadTaxonomy.submit()">Download</a></b> authority and opinion data for these taxa</p>';
    print '<form method="POST" action="bridge.pl" name="doDownloadTaxonomy">';
    print '<input type="hidden" name="action" value="displayDownloadTaxonomyResults">';
    print '<input type="hidden" name="taxon_no" value="'.$ref->{'taxon_no'}.'">';
    print '</form>'; 

	print "<p>You may <b><a href=\"$exec_url?action=startStartPrintHierarchy\">classify another taxon</a></b></p>";
	print main::stdIncludes( "std_page_bottom" );

	return;
}

1;
