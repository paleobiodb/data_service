package PrintHierarchy;

use PBDBUtil;
use Classification;

$DEBUG = 0;

# 21.9.03 JA

sub startPrintHierarchy	{

	my $dbh = shift;
	my $hbo = shift;

	$|=1;

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

	%shortranks = ("subspecies"=>"","species" => "", 
            "subgenus" => "Subg.", "genus" => "G.",
			"subtribe"=> "Subtr.", "tribe" => "Tr.", 
            "subfamily" => "Subfm", "family" => "Fm.","superfamily" => "Superfm." ,
			"infraorder" => "Infraor.", "suborder" => "Subor.", "order" => "Or.", "superorder" => "Superor.",
			"infraclass" => "Infracl.", "subclass" => "Subcl.", "class" => "Cl.", "superclass" => "Supercl.",
			"subphylum" => "Subph.", "phylum" => "Ph.");

	print main::stdIncludes( "std_page_top" );

# get focal taxon name from query parameters, then figure out taxon number
    my $ref;
    if ($q->param('taxon_no')) {
    	$sql = "SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE taxon_no='" . $q->param('taxon_no') . "'";
	    $ref = @{$dbt->getData($sql)}[0];
    } elsif ($q->param('taxon_name')) {
    	$sql = "SELECT taxon_no,taxon_name,taxon_rank FROM authorities WHERE taxon_name='" . $q->param('taxon_name') . "'";
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

	$MAX = $q->param('maximum_levels');
    $MAX_SEEN = 0;

    my $tree = TaxaCache::getChildren($dbt,$ref->{'taxon_no'},'tree');
    $tree->{'depth'} = 0;
    my @nodes_to_print = ();
    my @node_stack = ($tree);
    # mark higher level taxa that had all their children removed as "invalid"
    my @check_is_disused;
    my %not_found_type_taxon = ();
    my %found_type_for = ();
    my %not_found_type_for = ();
    while (@node_stack) {
        my $node = shift @node_stack;
        push @nodes_to_print, $node;
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
        
        if ($node->{'type_taxon_no'}) {
            my $type_taxon_no = $node->{'type_taxon_no'};
#                $type_taxon_nos{$type_taxon_no} = $node->{'taxon_no'};
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
    #my ($taxon_records) = PBDBUtil::getChildren($dbt,$ref->{'taxon_no'},$MAX);

    # prepend the query stuff to the array so it gets printed out as well
    #unshift @{$taxon_records}, {'taxon_no'=>$ref->{'taxon_no'},
    #                        'taxon_name'=>$q->param('taxon_name'),
    #                        'taxon_rank'=>$ref->{'taxon_rank'},
    #                        'depth'=>0};

    # now print out the data
	open OUT, ">$OUT_FILE_DIR/classification.csv";
	print "<center><table>\n";
    print "<tr>";
    for ($i=0;$i<=$MAX && $i <= $MAX_SEEN;$i++) {
           print "<td style=\"width:20;\">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>"; 
    }
    print "</tr>";
    my %type_taxon_nos = ();
    while (my ($type,$for) = each %found_type_taxon) {
        $type_taxon_nos{$for} = $type;
    }
	foreach $record (@nodes_to_print)	{
		print "<tr>";
		for ($i=0;$i<$record->{'depth'};$i++) {
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
        if ( $record->{'taxon_rank'} =~ /(species)|(genus)/ ) {
            $title .= "<i>".$link."</i>";
        } else {
            $title .= $link;
        }
        print $title;

        if ($disused{$record->{'taxon_no'}}) {
            print " <small>(disused)</small>";
        }
#        my $type_taxon_for = $type_taxon_nos{$record->{'taxon_no'}};
#        if ($type_taxon_for) {
            if ($found_type_for{$record->{'taxon_no'}}) {
                print " <small>(type taxon)</small>";
            } elsif ($not_found_type_for{$record->{'taxon_no'}}) {
                my $type_taxon_for = $not_found_type_for{$record->{'taxon_no'}};
                my $t = TaxonInfo::getTaxon($dbt,'taxon_no'=>$type_taxon_for);
#                print "<span style=\"color: red;\">";
#                print " - type taxon for $type_taxon_for,$t->{taxon_name}, but not classified into it";
#                print "<span style=\"color: red;\">";
            }
#        }
        if ($record->{'type_taxon_no'} && $not_found_type_taxon{$record->{'taxon_no'}}) {
            my $t = TaxonInfo::getTaxon($dbt,'taxon_no'=>$record->{'type_taxon_no'});
#            print "<span style=\"color: red;\">";
#            print " - type taxon $record->{type_taxon_no},$t->{taxon_name} not classified into it";
#            print "</span>";
        }

        #if (@{$record->{'spellings'}}) {
        #    print " [";
        #    print "=$_->{taxon_name}, " for (@{$record->{'spellings'}});
        #    print "]";
        #}
        
		print "</td>";
		print "</tr>\n";
        
		print OUT $record->{'taxon_rank'}.",".$record->{'taxon_name'}."\n";
	}
	print "</table></center><p>\n";
	close OUT;

	chmod 0664, "$OUT_FILE_DIR/classification.csv";

	print "<hr><p><b><a href=\"$OUT_HTTP_DIR/classification.csv\">Download</a></b> this list of taxonomic names</p>";
	print "<p><b><a href=\"bridge.pl?action=displayDownloadTaxonomyResults&taxon_no=".$ref->{"taxon_no"}."\">Download</a></b> authority and opinion data for these taxa</p>";

	print "<p>You may <b><a href=\"$exec_url?action=startStartPrintHierarchy\">classify another taxon</a></b></p>";
	print main::stdIncludes( "std_page_bottom" );

	return;
}

1;
