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

    my $tree = TaxaCache::getChildren($dbt,$ref->{'taxon_no'},'tree',$MAX);
    $tree->{'depth'} = 0;
    my @nodes_to_print = ();
    my @node_stack = ($tree);
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
    } 

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
	foreach $record (@nodes_to_print)	{
		print "<tr>";
		for ($i=0;$i<$record->{'depth'};$i++) {
			print "<td></td>";
		}
		print "<td style=\"white-space: nowrap;\" colspan=".($MAX + 1 - $record->{'depth'}).">";
        $shortrank = $shortranks{$record->{'taxon_rank'}};
        $title = "<b>$shortrank</b> ";
        if ( $record->{'taxon_rank'} =~ /(species)|(genus)/ ) {
            $title .= "<i>".$record->{'taxon_name'}."</i>";
        } else {
            $title .= $record->{'taxon_name'};
        }
        print $title;

        #if (@{$record->{'spellings'}}) {
        #    print " [";
        #    print "=$_->{taxon_name}, " for (@{$record->{'spellings'}});
        #    print "]";
        #}
        
		print "</td>";
		print "</tr>\n";
        
		print OUT $record->{'taxon_rank'}.",".$record->{'taxon_name'}."\n";
		$nrecords++;
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
