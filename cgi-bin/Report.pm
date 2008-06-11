# Report.pm
# Rewritten 12/27/2004 PS to use GROUP BY and be faster

package Report;

use Class::Date qw(date localdate gmdate now);

use Text::CSV_XS;
use PBDBUtil;
use TimeLookup;
use Data::Dumper;
use Debug qw(dbg);
use Constants qw($READ_URL $WRITE_URL $HOST_URL $IS_FOSSIL_RECORD $DATA_DIR $HTML_DIR $TAXA_TREE_CACHE $TAXA_LIST_CACHE);

use strict;

sub new {
	my ($class,$dbt,$q,$s) = @_;
	my $self = {dbt=>$dbt,q=>$q,s=>$s};
	bless $self, $class;
}

sub buildReport {
	my $self = shift;

	# flush all output immediately to stdout
	$| =1;

    # class variables, used across functions
    $self->{'dataTable'} = {}; # 1 or 2 level hash for 1 or 2 searchterms
    $self->{'totals1'} = {}; # Totals for searchfield1
    $self->{'totals2'} = {}; # Totals for searchfield2
    $self->{'sortKeys1'} = []; # Sorted keys for each index of the hash, pruned down to a size < max_rows and max_cols, respectively
    $self->{'sortKeys2'} = []; #   
    $self->{'warnings'} = [];
    #$self->{'grandTotal1'} = 0;
    #$self->{'grandTotal2'} = 0;
    # the above vars calculate rows output in the printed table
    # the below calculate unique rows in the DB
    # they'll only differ when tabulating SET/Lithology types, where one collection can be counted twice
    # if it has multiple set members
    $self->{'grandTotalCollections'} = 0;
    $self->{'grandTotalOccurrences'} = 0;
    $self->{'searchFields'} = []; # Double array: First index is searchfield# (can be 1 or 2).  
                                  # Second index can be either 0, 1 or 2 corresponding to no
                                  # search term, a single (like country) or double (like lithology1, lithology2) respectively 
                                # Set in reportQueryDB

    my $sth = $self->reportQueryDB();
    if ($sth) {
        $self->reportBuildDataTables($sth);
        $self->reportPrintOutfile();
        $self->reportDisplayHTML();
    }
}

##
# Prints out the HTML version of the data tables and closely mirrors reportPrintOutfile
##
sub reportDisplayHTML {
    my $self = shift;
    my $q = $self->{q};
    my $s = $self->{s};

    # Print Title of Page
	print "<center>\n";
	print "<p class=\"pageTitle\">Paleobiology Database report";
	if ($q->param('taxon_name')){
		print ": ".$q->param('taxon_name');
	}
	print "</p>\n";

    # Print Warnings
    my $msg = Debug::printWarnings($self->{'warnings'});
    if ($msg) { print $msg. "<br>"; }
    
    # Print Header
    my $isDoubleArray = scalar @{$self->{'sortKeys2'}};
    my $totalKeyword = ($q->param('output') eq 'average occurrences') ? 'AVERAGE' : 'TOTAL';
    my $header1 = $q->param('searchfield1'); $header1 =~ s/\s+\([a-z ]+\)//i;
    my $header2 = $q->param('searchfield2'); $header2 =~ s/\s+\([a-z ]+\)//i;
    print "<table border=0 class=dataTable cellspacing=0>";
    if ($isDoubleArray) {
        my $numCols = scalar(@{$self->{'sortKeys2'}});
        print "<tr><td class=dataTableTopULCorner>&nbsp;</td>";
        print "<td class=dataTableTop colspan=$numCols align=center>$header2</td>";
        print "<td>&nbsp;</td></tr>";
    }
    print "<tr>";
    print "<td class=dataTableULCorner align=center>$header1</td>";
    if ($isDoubleArray) { 
        foreach my $key2 (@{$self->{'sortKeys2'}}) {
            my $cleankey2 = $key2;
            if ($key2 =~ /,[^ ]/)	{
                $cleankey2 =~ s/,/, /g;
            }
            print "<td class=dataTableColumn>$cleankey2</td>";
        }
        print "<td class=dataTableColumnTotal>$totalKeyword</td>";
    } else {
        print "<td class=dataTableColumn>".$q->param('output')."</td>";
        if ($q->param('output') ne 'average occurrences') {
            print "<td class=dataTableColumn>percent</td>";
        }
    }
    print "</tr>\n";

    # Print Table
    my $grandTotal = ($q->param('output') eq 'collections') ? $self->{'grandTotalCollections'} : $self->{'grandTotalOccurrences'};
    foreach my $key1 (@{$self->{'sortKeys1'}}) {
        my $cleankey1 = $key1;
        if ($key1 =~ /,[^ ]/)	{
            $cleankey1 =~ s/,/, /g;
        }
        print "<tr>";
        print "<td class=dataTableRow>$cleankey1</td>";
        if ($isDoubleArray) { 
            foreach my $key2 (@{$self->{'sortKeys2'}}) {
                print "<td class=dataTableCell align=right>".$self->{'dataTable'}{$key1}{$key2}."</td>";
            }
            print "<td class=dataTableCellTotal align=right>".$self->{'totals1'}{$key1}."</td>";
        } else {
            print "<td class=dataTableCell align=right>".$self->{'dataTable'}{$key1}."</td>";
            if ($q->param('output') ne 'average occurrences') {
                print "<td class=dataTableCell align=right>".sprintf("%.1f",$self->{'dataTable'}{$key1}*100/$grandTotal)."</td>";
            }
        }    
        print "</tr>\n";
    }    

    # Print Final Totals Line
    if ($isDoubleArray) { 
        print "<tr><td class=dataTableRowTotal>$totalKeyword</td>";
        foreach my $key2 (@{$self->{'sortKeys2'}}) {
            print "<td class=dataTableCellTotal align=right>".$self->{'totals2'}{$key2}."</td>";
        }
        if ($q->param('output') ne 'average occurrences') {
            print "<td align=right class=dataTableCellTotal>$grandTotal</td>";
        } else {
            print "<td align=right class=dataTableCellTotal>".sprintf("%.1f",$self->{'grandTotalOccurrences'}/$self->{'grandTotalCollections'})."</td>";
        }
        print "</tr>";
    }
    print "</table>\n";

    print "<p>";
    #if ($q->param('output') ne 'collections') {
    #    if ($self->{'grandTotal1'} ne $self->{'grandTotalOccurrences'}) { 
    #        print "<b>".$self->{'grandTotal1'} . "</b> entries in "
    #    }
    #} else {
    #    if ($self->{'grandTotal1'} ne $self->{'grandTotalCollections'}) { 
    #        print "<b>".$self->{'grandTotal1'} . "</b> entries in "
    #    }
    #}
    print $self->{'grandTotalOccurrences'} . " occurrences and " if ($q->param('output') ne 'collections');
    print "".$self->{'grandTotalCollections'} . " collections";
    print ", ".sprintf("%.1f",$self->{'grandTotalOccurrences'}/$self->{'grandTotalCollections'}) . " occurrences per collection" if ($q->param('output') eq 'average occurrences');
    print " were tabulated</p>";

    # Link to report
    my $authorizer = $s->get("authorizer");
    if ( ! $authorizer ) { $authorizer = "unknown"; }
    $authorizer =~ s/(\s|\.)//g;
    my $reportFileName = $authorizer . "-report.csv";

	print qq|<p>The report data have been saved as "<a href="$HOST_URL/public/reports/$reportFileName">$reportFileName</a>"</p>|;
    print "</center>\n";
    print "<p>&nbsp;</p>";
my $date = now();
print "GOT HTML $date ";
}

##
# Simple function that outputs a CSV file based on the data table given
##
sub reportPrintOutfile{
    my $self = shift;
    my $q = $self->{q};
    my $s = $self->{s};

    #
    # Setup output
    #
    my $csv = Text::CSV_XS->new({
                    'quote_char'  => '"',
                    'escape_char' => '"',
                    'sep_char'    => ",",
                    'binary'      => 1});
     
    my $authorizer = $s->get("authorizer");
    if ( ! $authorizer ) { $authorizer = "unknown"; }
    $authorizer =~ s/(\s|\.)//g;
    my $reportFileName = $authorizer . "-report.csv";
    PBDBUtil::autoCreateDir("$HTML_DIR/public/reports");
    open(OUTFILE, ">$HTML_DIR/public/reports/$reportFileName") 
        or die ( "Could not open output file: $HTML_DIR/public/reports/$reportFileName($!) <br>\n" );

    chmod 0664, "$HTML_DIR/public/reports/$reportFileName";

    # Now print to file
    my @line;    
    # Print Header
    my $isDoubleArray = scalar @{$self->{'sortKeys2'}}; #var is true for two search terms, false for one
    my $totalKeyword = ($q->param('output') eq 'average occurrences') ? 'AVERAGE' : 'TOTAL';
    my $header1 = $q->param('searchfield1'); $header1 =~ s/\s+\([a-z ]+\)//i;
    my $header2 = $q->param('searchfield2'); $header2 =~ s/\s+\([a-z ]+\)//i;
    if ($isDoubleArray) {
        @line = ("$header1 / $header2");
        foreach my $key2 (@{$self->{'sortKeys2'}}) {
            push @line, $key2;
        }
        push @line, $totalKeyword;
    } else {
        @line = ($header1);
        push @line, $q->param('output');
        if ($q->param('output') ne 'average occurrences') {
            push @line, 'percent';
        }
    }
    print OUTFILE $csv->string()."\n" if ( $csv->combine ( @line ) );

    # Print Table
    my $grandTotal = ($q->param('output') eq 'collections') ? $self->{'grandTotalCollections'} : $self->{'grandTotalOccurrences'};
    foreach my $key1 (@{$self->{'sortKeys1'}}) {
        @line = ($key1);
        if ($isDoubleArray) {
            foreach my $key2 (@{$self->{'sortKeys2'}}) {
                push @line, $self->{'dataTable'}{$key1}{$key2};
            }
            push @line, $self->{'totals1'}{$key1};
        } else {
            push @line, $self->{'dataTable'}{$key1};
            if ($q->param('output') ne 'average occurrences') {
                push @line, sprintf("%.1f",$self->{'dataTable'}{$key1}*100/$grandTotal);
            }
        }     
        print OUTFILE $csv->string()."\n" if ( $csv->combine ( @line ) );
    }       
        
    # Print Final Totals Line
    @line = ($totalKeyword);
    if ($isDoubleArray) { 
        foreach my $key2 (@{$self->{'sortKeys2'}}) {
            push @line, $self->{'totals2'}{$key2};
        }
    } 
    push @line, $self->{'grandTotalOccurrences'} if ($q->param('output') eq 'occurrences');
    push @line, $self->{'grandTotalCollections'} if ($q->param('output') eq 'collections');
    push @line, sprintf("%.1f",$self->{'grandTotalOccurrences'}/$self->{'grandTotalCollections'}) if ($q->param('output') eq 'average occurrences');
    if (!$isDoubleArray) { push @line, ''; }
    print OUTFILE $csv->string()."\n" if ( $csv->combine ( @line ) );

    # Finish up
    close OUTFILE;
}    

##
# This function gets raw data returned from a SQL query into a standard form. Uses 
# a statement handle to the data (var $sth) and populates the %$self->dataTable, %$self->totals1, and %$self->totals2 if applicable
#  dataTable is a single or doubly index'd hash for 1 and 2 search fields, respectively
# Also populates @$self->sortKeys1 and @$self->sortKeys2, which are arrays of keys for the 3 above tables
#  where the arrays are sorted in descending order of count and truncated to the correct length based
#  on $q->param(max_rows) and $q->param(max_cols)
##
sub reportBuildDataTables {
    my $self = shift;
    my $q = $self->{q};
    my $dbt = $self->{dbt};

    my $sth = shift;
    return unless $sth;

    my $t1FieldCnt = scalar @{$self->{'searchFields'}[1]}; #searchfield1
    my $t2FieldCnt = scalar @{$self->{'searchFields'}[2]}; #searchfield2
    my @t1Fields = @{$self->{'searchFields'}[1]};
    my @t2Fields = @{$self->{'searchFields'}[2]};
    # Translations tables to tranlate country -> continent, interval_no->period,etc
    my %t1TranslationTable = %{$self->getTranslationTable($q->param('searchfield1'))};
    my %t2TranslationTable = %{$self->getTranslationTable($q->param('searchfield2'))};
# removed assemblage components from set list because there are few combinations
#   11.4.08 JA
    my $setFields = 'research group|tectonic setting|preservation mode|list coverage';
    my $weightedFields = 'lithology - weighted|lithification';
    my $t1Type = ($q->param('searchfield1') =~ /$weightedFields/) ? 'weighted' 
               : ($q->param('searchfield1') =~ /$setFields/)      ? 'set' 
               : '';
    my $t2Type = ($q->param('searchfield2') =~ /$weightedFields/) ? 'weighted' 
               : ($q->param('searchfield2') =~ /$setFields/)      ? 'set' 
               : '';
  
    # If we select 'average occurrences', we divide by occs/coll, else we use a ref to occs or colls appropriately
    my %coll_totals1= ();
    my %coll_totals2= ();
    my %coll_dataTable=();
    my %occs_totals1 = ();
    my %occs_totals2 = ();
    my %occs_dataTable = ();

    dbg("t1FieldCnt $t1FieldCnt t2FieldCnt $t2FieldCnt t1Type $t1Type t2Type $t2Type t1Fields "
              .join(",",@t1Fields)." t2Fields ".join(",",@t2Fields));

    while(my $row = $sth->fetchrow_hashref()) {
        my @term1Keys = ();
        my @term2Keys = ();
        if (%t1TranslationTable) {
            foreach (@t1Fields) {
                if ($_ =~ /min_interval_no|latdeg|latdir|lngdir/) { 
                    # Skip these  for now, we'll use them though when lngdeg and max_interval_no come around respectively for grabbing the plate id and higher order time terms 
                    next;
                } elsif ($_ eq 'lngdeg') { # This will happen when grabbing plate_id
                    my $table_key = "";
                    $table_key .= "-" if ($row->{'lngdir'} eq 'West' && $row->{'lngdeg'} != 0);
                    $table_key .= $row->{'lngdeg'};
                    $table_key .= "_";
                    $table_key .= "-" if ($row->{'latdir'} eq 'South' && $row->{'latdeg'} != 0);
                    $table_key .= $row->{'latdeg'};
                    if (exists $t1TranslationTable{$table_key}) {
                        push @term1Keys, $t1TranslationTable{$table_key};
                    }
                } elsif ($_ eq 'max_interval_no') { # When grabbing a higher order time term
                    if ($t1TranslationTable{$row->{'max_interval_no'}} && 
                       ($t1TranslationTable{$row->{'max_interval_no'}} eq $t1TranslationTable{$row->{'min_interval_no'}} ||
                        $row->{'min_interval_no'} == 0)) {
                            push @term1Keys, $t1TranslationTable{$row->{'max_interval_no'}};
                    } 
                } else {
                    if ($t1TranslationTable{$row->{$_}}) {
                        push @term1Keys, $t1TranslationTable{$row->{$_}};
                    } 
                }
            }
        } else {
            foreach (@t1Fields) {
                if ($row->{$_}) {
                    push @term1Keys, $row->{$_}; 
                } 
            }
        }
        
        if (%t2TranslationTable) {
            foreach (@t2Fields) {
                if ($_ =~ /min_interval_no|latdeg|latdir|lngdir/) {
                    next;
                } elsif ($_ eq 'lngdeg') {
                    my $table_key = "";
                    $table_key .= "-" if ($row->{'lngdir'} eq 'West');
                    $table_key .= $row->{'lngdeg'};
                    $table_key .= "_";
                    $table_key .= "-" if ($row->{'latdir'} eq 'South');
                    $table_key .= $row->{'latdeg'};
                    if (exists $t2TranslationTable{$table_key}) {
                        push @term2Keys, $t2TranslationTable{$table_key};
                    }
                } elsif ($_ eq 'max_interval_no') {
                    if ($t2TranslationTable{$row->{'max_interval_no'}} && 
                       ($t2TranslationTable{$row->{'max_interval_no'}} eq $t2TranslationTable{$row->{'min_interval_no'}} ||
                        $row->{'min_interval_no'} == 0)) {
                            push @term2Keys, $t2TranslationTable{$row->{'max_interval_no'}};
                    } 
                } else {
                    if ($t2TranslationTable{$row->{$_}}) {
                        push @term2Keys, $t2TranslationTable{$row->{$_}};
                    } 
                }
            }
        } else {
            foreach (@t2Fields) {
                if ($row->{$_}) {
                    push @term2Keys, $row->{$_}; 
                } 
            }
        }
        if (!@term1Keys) { @term1Keys = ('(no term entered)');}
        if (!@term2Keys) { @term2Keys = ('(no term entered)');}

        # Set up which values we're going to tally 
        #  For weighted multiple values, divide cnt by two if we have two terms
        #  For unweighted multiple values, glob them together in order
        #  For a set, split on the comma
        #  For anything else, do nothing
        my $coll_cnt = $row->{'collections_cnt'};
        my $occs_cnt = $row->{'occurrences_cnt'};
        if ($t1FieldCnt == 2) {
            if ($t1Type eq 'weighted') {
                if ($term1Keys[0] && $term1Keys[1]) {
                    $coll_cnt = $coll_cnt/2;
                    $occs_cnt = $occs_cnt/2;
                }    
            } else { #unweighted
                my $globStr = join ('+',sort (@term1Keys));
                @term1Keys = ($globStr) if ($globStr);
            }
        } elsif ($t1FieldCnt == 1) {
            if ($t1Type eq 'set') {
                @term1Keys = split(/,/,$term1Keys[0]);   
            }
        }
        if ($t2FieldCnt == 2) {
            if ($t2Type eq 'weighted')  {
                if ($term2Keys[0] && $term2Keys[1]) {
                    $coll_cnt = $coll_cnt/2;
                    $occs_cnt = $occs_cnt/2;
                }    
            } else { #unweighted
                my $globStr = join ('+',sort (@term2Keys));
                @term2Keys = ($globStr) if ($globStr);
            }
        } elsif ($t2FieldCnt == 1) {
            if ($t2Type eq 'set') {
                @term2Keys = split(/,/,$term2Keys[0]);   
            }
        }

        # Add in the counts
        foreach my $key1 (@term1Keys) { 
            if ($t2FieldCnt > 0) {
                foreach my $key2 (@term2Keys) {
                    $coll_dataTable{$key1}{$key2} += $coll_cnt;
                    $occs_dataTable{$key1}{$key2} += $occs_cnt;
                }
            } else {
                $coll_dataTable{$key1} += $coll_cnt;
                $occs_dataTable{$key1} += $occs_cnt;
            }
        }    

        # Calculate totals
        foreach my $key1 (@term1Keys) {
            $coll_totals1{$key1} += $coll_cnt;
            $occs_totals1{$key1} += $occs_cnt;
        }
        if ($t2FieldCnt > 0) {
            foreach my $key2 (@term2Keys) {
                $coll_totals2{$key2} += $coll_cnt;
                $occs_totals2{$key2} += $occs_cnt;
            }
        }

        $self->{'grandTotalCollections'} += $row->{'collections_cnt'};
        $self->{'grandTotalOccurrences'} += $row->{'occurrences_cnt'};
    }
    $sth->finish();    

    # Calculate row/column totals, only differ from grandTotal if row/column is SET type
    #my ($coll_grandTotal1, $occs_grandTotal1, $coll_grandTotal2, $occs_grandTotal2);
    #$coll_grandTotal1 += $_ for values %coll_totals1;
    #$coll_grandTotal2 += $_ for values %coll_totasl2;
    #$occs_grandTotal1 += $_ for values %occs_totals1;
    #$occs_grandTotal2 += $_ for values %occs_totals2;

    # Set the totals variables.  For average occs, divide occurrence totals by collection totals
    if ($q->param('output') eq 'average occurrences') {
        while(my ($key1,$val1) = each %occs_totals1) { 
            $self->{'totals1'}{$key1} = sprintf("%.1f",$val1/$coll_totals1{$key1});
        }    
        if ($t2FieldCnt > 0) {
            while(my ($key2,$val2) = each %occs_totals2) { 
                $self->{'totals2'}{$key2} = sprintf("%.1f",$val2/$coll_totals2{$key2});
            }
        } 
    } elsif ($q->param('output') eq 'occurrences') {
        $self->{'totals1'} = \%occs_totals1;
        $self->{'totals2'} = \%occs_totals2;
        $self->{'dataTable'} = \%occs_dataTable;
    } else { # eq 'collections'
        $self->{'totals1'} = \%coll_totals1;
        $self->{'totals2'} = \%coll_totals2;
        $self->{'dataTable'} = \%coll_dataTable;
    }

    my $t = new TimeLookup($dbt);
    # Provide arrays of sorted keys for the two totals with which
    # to index into the hashes
    if ($q->param('searchfield1') eq "10 m.y. bins (standard order)") {
        $self->{'sortKeys1'} = [$t->getBins()];
    } elsif ($q->param('searchfield1') eq "Gradstein 3: Periods (standard order)") {
        $self->{'sortKeys1'} = [$t->getScaleOrder(69)];
    } elsif ($q->param('searchfield1') eq "Gradstein 5: Epochs (standard order)") {
        $self->{'sortKeys1'} = [$t->getScaleOrder(71)];
    } elsif ($q->param('searchfield1') eq "Gradstein 7: Stages (standard order)") {
        $self->{'sortKeys1'} = [$t->getScaleOrder(73)];
    } else {
        $self->{'sortKeys1'} = [sort {$self->{'totals1'}{$b} <=> $self->{'totals1'}{$a}} keys %{$self->{'totals1'}}];
    }    
    # Remove keys with colls/occs in the DB. Have to go in reverse so 
    # we splice the right thing
    for(my $i=scalar(@{$self->{'sortKeys1'}})-1;$i>=0;$i--) {
        if (!exists $self->{'totals1'}{${$self->{'sortKeys1'}}[$i]}) {
            splice @{$self->{'sortKeys1'}},$i,1;
        }
    }    
    if ($q->param('searchfield2') eq "10 m.y. bins (standard order)") {
        $self->{'sortKeys2'} = [$t->getBins()];
    } elsif ($q->param('searchfield2') eq "Gradstein 3: Periods (standard order)") {
        $self->{'sortKeys2'} = [$t->getScaleOrder(69)];
    } elsif ($q->param('searchfield2') eq "Gradstein 5: Epochs (standard order)") {
        $self->{'sortKeys2'} = [$t->getScaleOrder(71)];
    } elsif ($q->param('searchfield2') eq "Gradstein 7: Stages (standard order)") {
        $self->{'sortKeys2'} = [$t->getScaleOrder(73)];
    } else {
        $self->{'sortKeys2'} = [sort {$self->{'totals2'}{$b} <=> $self->{'totals2'}{$a}} keys %{$self->{'totals2'}}];
    }    
    for(my $i=scalar(@{$self->{'sortKeys2'}})-1;$i>=0;$i--) {
        if (!exists $self->{'totals2'}{${$self->{'sortKeys2'}}[$i]}) {
            splice @{$self->{'sortKeys2'}},$i,1;
        }
    }    

    # Only return the X most prevalent rows or columns
    # By splicing away the ends of the sortKeys arrays. the actual dataTable isn't touched.
    my (@key1_left, @key2_left, $new_key1, $new_key2);
    if ($q->param('max_rows') < scalar(@{$self->{'sortKeys1'}})) {
        @key1_left = splice(@{$self->{'sortKeys1'}}, $q->param('max_rows'));
        $new_key1 = "remaining ".scalar(@key1_left)." rows";
        @{$self->{'sortKeys1'}}[$q->param('max_rows')] = $new_key1;
    }
        
    if ($q->param('max_cols') < scalar(@{$self->{'sortKeys2'}})) {
        @key2_left = splice(@{$self->{'sortKeys2'}}, $q->param('max_cols'));
        $new_key2 = "remaining ".scalar(@key2_left)." columns";
        @{$self->{'sortKeys2'}}[$q->param('max_cols')] = $new_key2;
    }


    # if max_rows or max_cols is set and less the the total rows/cols returned, we lump
    # together all the rows/cols that got cut out into a single set of values called "Remaining X rows" and "Remaining X cols"
    if ($t2FieldCnt > 0) {
        #the upper left quadrant is displayed and is the set of rows*cols not cut off
        # by max_rows or max_cols

        #upper right quandrant, the area of the table cut off by 'max_rows'
        # -1 in for header to not include the last key (=$new_key1="Remaining X rows")
        # reduce grid to a single column
        for(my $i=0;$i<scalar(@{$self->{'sortKeys1'}})-1;$i++) { 
            my $sort_key1 = @{$self->{'sortKeys1'}}[$i];
            foreach my $key2 (@key2_left) {
                $coll_dataTable{$sort_key1}{$new_key2} += $coll_dataTable{$sort_key1}{$key2};
                $coll_totals2{$new_key2} += $coll_dataTable{$sort_key1}{$key2};
                $occs_dataTable{$sort_key1}{$new_key2} += $occs_dataTable{$sort_key1}{$key2};
                $occs_totals2{$new_key2} += $occs_dataTable{$sort_key1}{$key2};
            }
        }
        #lower left quadrant, cut off by 'max_cols'. reduce grid to a single row
        for(my $i=0;$i<scalar(@{$self->{'sortKeys2'}})-1;$i++) { 
            my $sort_key2 = @{$self->{'sortKeys2'}}[$i];
            foreach my $key1 (@key1_left) {
                $coll_dataTable{$new_key1}{$sort_key2} += $coll_dataTable{$key1}{$sort_key2};
                $coll_totals1{$new_key1} += $coll_dataTable{$key1}{$sort_key2};
                $occs_dataTable{$new_key1}{$sort_key2} += $occs_dataTable{$key1}{$sort_key2};
                $occs_totals1{$new_key1} += $occs_dataTable{$key1}{$sort_key2};
            }
        }
        #lower right, cut off by 'max_cols' OR 'max_rows'. reduce grid to single square
        foreach my $key1 (@key1_left) {
            foreach my $key2 (@key2_left) {
                $coll_dataTable{$new_key1}{$new_key2} += $coll_dataTable{$key1}{$key2};
                $coll_totals2{$new_key2} += $coll_dataTable{$key1}{$key2};
                $coll_totals1{$new_key1} += $coll_dataTable{$key1}{$key2};
                $occs_dataTable{$new_key1}{$new_key2} += $occs_dataTable{$key1}{$key2};
                $occs_totals2{$new_key2} += $occs_dataTable{$key1}{$key2};
                $occs_totals1{$new_key1} += $occs_dataTable{$key1}{$key2};
            }
        }    
    } else {
        foreach my $key1 (@key1_left) {
            $coll_totals1{$new_key1} += $coll_dataTable{$key1};
            $coll_dataTable{$new_key1} += $coll_dataTable{$key1};
            $occs_totals1{$new_key1} += $occs_dataTable{$key1};
            $occs_dataTable{$new_key1} += $occs_dataTable{$key1};
        }    
    }
   
    # For average occurrences type, we didn't set the $self->{dataTable} to be equal to 
    # a reference to $occ_dataTable or $coll_dataTable above, so calculate it now
    if ($q->param('output') eq 'average occurrences') {
        if ($t2FieldCnt > 0) {
            foreach my $key1 (keys %occs_dataTable ) {
                foreach my $key2 (keys %{$occs_dataTable{$key1}}) {
                    if ($occs_dataTable{$key1}{$key2}) {
                        $self->{'dataTable'}{$key1}{$key2} = sprintf("%.1f",$occs_dataTable{$key1}{$key2} / $coll_dataTable{$key1}{$key2});
                    } 
                }
            }
            if ($new_key1) {
                $self->{'totals1'}{$new_key1} = sprintf("%.1f",$occs_totals1{$new_key1} / $coll_totals1{$new_key1}); 
            }
            if ($new_key2) {
                $self->{'totals2'}{$new_key2} = sprintf("%.1f",$occs_totals2{$new_key2} / $coll_totals2{$new_key2});
            }
        } else {
            foreach my $key1 (keys %occs_dataTable ) {
                if ($occs_dataTable{$key1}) {
                    $self->{'dataTable'}{$key1} = sprintf("%.1f",$occs_dataTable{$key1} / $coll_dataTable{$key1});
                } 
            }
            if ($new_key1) {
                $self->{'totals1'}{$new_key1} = sprintf("%.1f",$occs_totals1{$new_key1} / $coll_totals1{$new_key1});
            }    
        }    
    }
    
    # Bit of cleanup, fill in empty dataTable entries with '-' if they don't exist
    if ($t2FieldCnt > 0) {
        foreach my $key1 (@{$self->{'sortKeys1'}}) { 
            foreach my $key2 (@{$self->{'sortKeys2'}}) {
                $self->{'dataTable'}{$key1}{$key2} = '-' if (! $self->{'dataTable'}{$key1}{$key2})
            }
        }    
    } 
my $date = now();
print "GOT TABLE $date ";
    #    print "<pre>".Dumper($self->{'dataTable'}) . "</pre>";
}


##
# This function builds the SQL statement to pull data out of the database, then executes that SQL, 
# returning the variable $sth, a statement handle to the data
##
sub reportQueryDB{
	my $self = shift;
    my $q = $self->{q};
    my $dbt = $self->{dbt};
    my $dbh = $dbt->dbh;
    my @whereTerms = ();
    my $fromSQL = 'collections c';
    my $leftJoinSQL = '';

    # Build terms/conditionals for collections

    if ($q->param('searchfield1') eq $q->param('searchfield2')) {
        $q->param('searchfield2'=>'');
    }        
    
    # How choices in HTML map to database fields
    my %sqlFields = (
        'authorizer'=>'authorizer_no', 'enterer'=>'enterer_no', 'research group'=>'research_group',
        'country'=>'country', 'state'=>'state', 'interval name'=>'max_interval_no,min_interval_no', 'formation'=>'formation',
        'paleoenvironment'=>'environment', 'scale of geographic resolution'=>'geogscale', 
        'scale of stratigraphic resolution'=>'stratscale',
        'tectonic setting'=>'tectonic_setting', 'preservation mode'=>'pres_mode', 
        'assemblage components'=>'assembl_comps', 'reason for describing collection'=>'collection_type',
        'list coverage'=>'collection_coverage', 'lithification'=>'lithification,lithification2',
        'lithology - all combinations'=>'lithology1,lithology2', 'lithology - weighted'=>'lithology1,lithology2',
        'continent'=>'country', '10 m.y. bins (most common order)'=>'max_interval_no,min_interval_no', 'Gradstein 3: Periods (most common order)'=>'max_interval_no,min_interval_no', 'Gradstein 5: Epochs (most common order)'=>'max_interval_no,min_interval_no', 'Gradstein 7: Stages (most common order)'=>'max_interval_no,min_interval_no', '10 m.y. bins (standard order)'=>'max_interval_no,min_interval_no','Gradstein 3: Periods (standard order)'=>'max_interval_no,min_interval_no', 'Gradstein 5: Epochs (standard order)'=>'max_interval_no,min_interval_no', 'Gradstein 7: Stages (standard order)'=>'max_interval_no,min_interval_no','tectonic plate ID'=>'latdeg,latdir,lngdeg,lngdir');
    foreach my $i (1..2) {
        if ($sqlFields{$q->param("searchfield$i")}) {
            push @{$self->{'searchFields'}[$i]}, split(/,/,$sqlFields{$q->param("searchfield$i")});
        }    
    }

    my $research_group_sql = PBDBUtil::getResearchGroupSQL($dbt,$q->param('research_group'));
    push @whereTerms, $research_group_sql if ($research_group_sql);
    if($q->param('research_group') =~ /^(?:decapod|divergence|ETE|5%|1%|PACED|PGAP)$/) {
        $leftJoinSQL .= " LEFT JOIN secondary_refs sr ON sr.collection_no=c.collection_no";
    }                               

    # Permissions conditionals, since we can't use Permissions Module
    # ((release date < NOW and is public) OR is authorizer OR research_group in mygroups)
    # push @whereTerms, "(access_level='the public' AND NOW() > release_date) OR authorizer=".$dbh->quote($s->get('authorizer'));
    # No permissions conditionals, since this is non-specific data.
 
    my $createTable = ($q->param('output') eq 'collections') ? "c" : "o";
    if($q->param("year_begin")){
        my $creationDate = $dbh->quote(sprintf("%d-%02d-%02d 00:00:00",$q->param('year_begin'),$q->param('month_begin'),$q->param('day_begin')));
		push @whereTerms,"$createTable.created >= $creationDate";
    }    
    if($q->param("year_end")){
        my $creationDate = $dbh->quote(sprintf("%d-%02d-%02d 23:59:59",$q->param('year_end'),$q->param('month_end'),$q->param('day_end')));
		push @whereTerms,"$createTable.created <= $creationDate";
    }    

        
    # Construct the final SQL query and execute
    my ($selectSQL, $groupSQL);
    foreach (@{$self->{'searchFields'}[1]}, @{$self->{'searchFields'}[2]}) {
        $groupSQL .= ",c.".$_;
    }
    $groupSQL =~ s/^,//;
    if ($q->param('taxon_name') =~ /[^\s\w, \t\n-:;]/) {
        print "<div align=\"center\">".Debug::printErrors(["Invalid taxon name"])."</div>";
        exit;
    }
	if (($q->param('output') eq "collections" && ($q->param('Sepkoski') eq "Yes" || $q->param('taxon_name'))) || 
        ($q->param('output') eq "occurrences") ||
        ($q->param('output') eq "average occurrences")) {

        $selectSQL = 'COUNT(DISTINCT o.occurrence_no) AS occurrences_cnt, COUNT(DISTINCT c.collection_no) AS collections_cnt';
        $selectSQL .= ",".$groupSQL;

        $fromSQL .= ", occurrences o ";
        $leftJoinSQL = " LEFT JOIN reidentifications re ON re.occurrence_no = o.occurrence_no ".$leftJoinSQL;
        push @whereTerms,"o.collection_no = c.collection_no";

        # get a list of Sepkoski's genera, if needed JA 28.2.03
        if ( $q->param('Sepkoski') eq "Yes" )	{
            my $sepkoskiGenera = $self->getSepkoskiGenera();
            if ( $sepkoskiGenera) {
                push @whereTerms,"((re.reid_no IS NULL AND o.taxon_no IN ($sepkoskiGenera)) OR (re.most_recent='YES' AND re.taxon_no IN ($sepkoskiGenera)))";
            }
        }

        # handle taxon names
        # Changed PBDBUtil funct to optionally use taxon_nos and used those
        my $genus_names_string;
		if($q->param('taxon_name')){
	        my @taxa = split(/\s*[, \t\n-:;]{1}\s*/,$q->param('taxon_name'));

            my %taxon_nos_unique = ();
            foreach my $taxon (@taxa) {
                my @taxon_nos = TaxonInfo::getTaxonNos($dbt, $taxon, undef, 1);
                dbg("Found ".scalar(@taxon_nos)." taxon_nos for $taxon");
                if (scalar(@taxon_nos) == 0) {
                    $genus_names_string .= ", ".$dbh->quote($taxon);
                } elsif (scalar(@taxon_nos) == 1) {
                    my @all_taxon_nos = TaxaCache::getChildren($dbt,$taxon_nos[0]);
                    # Uses hash slices to set the keys to be equal to unique taxon_nos.  Like a mathematical UNION.
                    @taxon_nos_unique{@all_taxon_nos} = ();
                } else { #result > 1
                    push @{$self->{'warnings'}}, "The counts are not restricted to '$taxon' because more than one taxon has that name. If this is a problem email <a href='mailto: alroy\@nceas.ucsb.edu'>John Alroy</a>.";
                }
            }
            my $taxon_nos_string = join(", ", keys %taxon_nos_unique);
            $genus_names_string =~ s/^,//;
         
            my $sql;
            if ($taxon_nos_string) {
                $sql .= " OR ((re.reid_no IS NULL AND o.taxon_no IN ($taxon_nos_string)) OR (re.most_recent='YES' AND re.taxon_no IN ($taxon_nos_string)))";
            }
            if ($genus_names_string) {
                $sql .= " OR ((re.reid_no IS NULL AND o.genus_name IN ($genus_names_string)) OR (re.most_recent='YES' AND re.genus_name IN ($genus_names_string)))";
            }
            $sql =~ s/^ OR //g;

            if ($sql) { push @whereTerms, "(".$sql.")"; }
        }
    } else {
        $selectSQL = 'COUNT(DISTINCT c.collection_no) AS collections_cnt';
        $selectSQL .= ",".$groupSQL;
    }


    my $sql = "SELECT ".$selectSQL." FROM (".$fromSQL.") ".$leftJoinSQL;
    if (@whereTerms) {
        $sql .= " WHERE ".join(' AND ',@whereTerms);
    }
    $sql .= " GROUP BY ".$groupSQL;

my $date = now();
print "$date ";
    if ($groupSQL) {
        dbg("SQL:".$sql);
       
        my $sth = $dbh->prepare($sql) || die "Prepare query failed\n";
        $sth->execute() || die "Execute query failed\n";
$date = now();
print "GOT DATA $date ";
        return $sth;
    } else {
        return undef;
    }
}


sub getSepkoskiGenera {
    my $self = shift;
    my $dbt = $self->{dbt};
    my $dbh = $dbt->dbh;

    my ($sql, $sth, @jackrefs, $jacklist);
    
    $sql = "SELECT taxon_no FROM authorities WHERE authorizer_no=48 AND taxon_rank='genus'";
    $sth = $dbh->prepare($sql) 
        or die "Prepare query failed\n";
    $sth->execute() 
        or die "Execute query failed\n";
    @jackrefs = @{$sth->fetchall_arrayref()};
    $sth->finish();
    foreach my $jackref (@jackrefs)  {
        $jacklist .= ', '.${$jackref}[0];
    }
    $jacklist =~ s/^,//;
    return $jacklist;
}

##
# This function maps a field in an SQL table to the type of data the user requested
# For example, if the user selected continent, then it must map the country names
# in the collections table to the names of continents. If no map exists, returns
# an empty hash reference.
##
sub getTranslationTable {
    my $self = shift;
    my $param = shift;
    my %table = ();
    my $dbt = $self->{'dbt'};
    my $t = new TimeLookup($dbt);
    if ($param eq "interval name") {
        my $intervals = $self->getIntervalNames();
        %table = %{$intervals};
    } elsif ($param =~ /10 m\.y\. bins/) { 
        my $binning = $t->getScaleMapping('bins');
        %table = %$binning;
    } elsif ($param =~ /Gradstein 3: Periods/) { 
		my $intervalInScaleRef = $t->getScaleMapping(69,'names');
		%table = %{$intervalInScaleRef};
    } elsif ($param =~ /Gradstein 5: Epochs/) { 
		my $intervalInScaleRef = $t->getScaleMapping(71,'names');
		%table = %{$intervalInScaleRef};
    } elsif ($param =~ /Gradstein 7: Stages/) { 
		my $intervalInScaleRef = $t->getScaleMapping(73,'names');
		%table = %{$intervalInScaleRef};
    } elsif ($param eq "continent") {
        my $regions = $self->getRegions();
        %table = %{$regions};
	} elsif ($param eq "enterer") {
        my $sql = "SELECT person_no,name FROM person";
        my @results = @{$dbt->getData($sql)};
        foreach my $row (@results) {
            $table{$row->{'person_no'}} = $row->{'name'};
        }
	} elsif ($param eq "authorizer") {
        my $sql = "SELECT person_no,name FROM person";
        my @results = @{$dbt->getData($sql)};
        foreach my $row (@results) {
            $table{$row->{'person_no'}} = $row->{'name'};
        }
    } elsif ($param eq 'tectonic plate ID') {
        if ( ! open ( PLATES, "$DATA_DIR/plateidsv2.lst" ) ) {
            print "<font color='red'>Skipping plates.</font> Error message is $!<br><br>\n";
        } else {
            <PLATES>;

            while (my $line = <PLATES>) {
                chomp $line;
                my ($lng,$lat,$plate_id) = split /,/,$line;
                $table{$lng."_".$lat}=$plate_id;
            }
        }
    }
    dbg("get translation table called with param $param. table:");
    dbg("<pre>".Dumper(\%table)."</pre>") if (scalar keys %table);

    return \%table;
}

# Returns a hash reference filled with all interval names
sub getIntervalNames {
    my $self = shift;
    my $dbt = $self->{dbt};

    my %interval_names;
 
    # get the names of time intervals
    my $sql = "SELECT interval_no,eml_interval,interval_name FROM intervals";
    my @intnorefs = @{$dbt->getData($sql)};
    for my $intnoref ( @intnorefs )        {
        if ( $intnoref->{eml_interval} )        {
            $interval_names{$intnoref->{interval_no}} = $intnoref->{eml_interval} . " " . $intnoref->{interval_name};
        } else  {
            $interval_names{$intnoref->{interval_no}} = $intnoref->{interval_name};
        }
    }
    return \%interval_names;
}

# Returns a hash reference that maps a country to a continent
sub getRegions	{
	my $self = shift;
    my %regions;

	if ( ! open REGIONS,"<$DATA_DIR/PBDB.regions" ) {
		$self->htmlError ( "$0:Couldn't open $DATA_DIR/PBDB.regions<br>$!" );
	}
	while (<REGIONS>)	{
		s/\n//;
		my ($continent,$country_list) = split /:/, $_, 2;
		my @countries = split /\t/,$country_list;
		foreach my $country (@countries)	{
			$regions{$country} = $continent;
		}
	}
    return \%regions;
	close REGIONS;
}

# This only shown for internal errors
sub htmlError {
	my $self = shift;
    my $message = shift;

    print $message;
    exit 1;
}


# JA 10.6.08
# stuck this module here for lack of any better ideas
sub findMostCommonTaxa	{

	my $self = shift;
	my $dataRowsRef = shift;
	my $q = $self->{q};
	my $dbt = $self->{dbt};

	my @dataRows = @{$dataRowsRef};
	my @collection_nos = map {$_->{'collection_no'}} @dataRows;

	print "<center><p class=\"pageTitle\">Most common genera in these collections</p></center>\n\n";

	my $sql = "SELECT genus_name,taxon_no,occurrence_no FROM occurrences WHERE taxon_no>0 AND collection_no IN (" . join(',',@collection_nos) . ")";
	my $sql2 = "SELECT genus_name,taxon_no,occurrence_no FROM reidentifications WHERE most_recent='YES' AND taxon_no>0 AND collection_no IN (" . join(',',@collection_nos) . ")";
	# WARNING: will fail if there are homonyms
	if ( $q->param('taxon_name') )	{
		$sql = "SELECT lft,rgt FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND taxon_name='".$q->param('taxon_name')."'";
		my $row = ${$dbt->getData($sql)}[0];
		$sql = "SELECT t.taxon_no FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND lft>=".$row->{'lft'}." AND rgt<=".$row->{'rgt'};
		my @rows = @{$dbt->getData($sql)};
		my $taxon_list;
		for my $r ( @rows )	{
			$taxon_list .= "," . $r->{'taxon_no'};
		}
		$taxon_list =~ s/^,//;
		$sql = "SELECT genus_name,taxon_no,occurrence_no FROM occurrences WHERE taxon_no IN (".$taxon_list.") AND collection_no IN (" . join(',',@collection_nos) . ")";
		$sql2 = "SELECT genus_name,taxon_no,occurrence_no FROM reidentifications WHERE most_recent='YES' AND taxon_no IN (".$taxon_list.") AND collection_no IN (" . join(',',@collection_nos) . ")";
	}
 
	my @rows = @{$dbt->getData($sql)};
	my @rows2 = @{$dbt->getData($sql2)};

	my %hasno;
	my %seen;
	for my $r ( @rows2 )	{
		$hasno{$r->{'taxon_no'}}++;
		$seen{$r->{'occurrence_no'}}++;
	}
	for my $r ( @rows )	{
		if ( ! $seen{$r->{'occurrence_no'}} )	{
			$hasno{$r->{'taxon_no'}}++;
		}
	}
	%seen = ();

	my @nos = keys %hasno;
	%hasno = ();

	# get the name and rank of each taxon's synonym
	$sql = "SELECT t.taxon_no,taxon_rank,taxon_name,synonym_no FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=synonym_no AND t.taxon_no IN (". join(',',@nos) .")";
	my @norows = @{$dbt->getData($sql)};

	my %parent;
	my %genus_no;
	for my $r ( @norows )	{
		if ( $r->{'taxon_rank'} eq "genus" )	{
			$parent{$r->{'taxon_no'}}{'genus'} = $r->{'taxon_name'};
			$genus_no{$r->{'taxon_name'}} = $r->{'taxon_no'};
		}
	}

	@norows = ();

	$sql = "SELECT child_no,taxon_rank parent_rank,taxon_name parent FROM authorities a,$TAXA_TREE_CACHE t,$TAXA_LIST_CACHE l WHERE child_no IN (". join(',',@nos) . ") AND parent_no=a.taxon_no AND a.taxon_rank IN ('class','order','family','genus') AND a.taxon_no=t.taxon_no ORDER BY lft";
	my @parentrows = @{$dbt->getData($sql)};

	for my $r ( @parentrows )	{
		$parent{$r->{'child_no'}}{$r->{'parent_rank'}} = $r->{'parent'};
		if ( $r->{'parent_rank'} eq "genus" )	{
			$genus_no{$r->{'parent'}} = $r->{'child_no'};
		}
	}
	@parentrows = ();

	my %count;
	for my $r ( @rows2 )	{
		if ( $parent{$r->{'taxon_no'}}{'genus'} )	{
			$count{$parent{$r->{'taxon_no'}}{'genus'}}++;
			$seen{$r->{'occurrence_no'}}++;
		}
	}
	for my $r ( @rows )	{
		if ( $parent{$r->{'taxon_no'}}{'genus'} && ! $seen{$r->{'occurrence_no'}} )	{
			$count{$parent{$r->{'taxon_no'}}{'genus'}}++;
		}
	}
	%seen = ();

	my @genera = keys %count;
	@genera = sort { $count{$b} <=> $count{$a} } @genera;

	print "<div class=\"displayPanel\" style=\"width: 50em; margin-left: 1em;\">\n";
	print "<div class=\"displayPanelContent\">\n";
	print "<table class=\"small\" style=\"margin-left: 1em; margin-top: 1em; margin-bottom: 1em;\">\n";
	print "<tr align=\"center\"><td style=\"font-size: 1.15em;\">rank</td><td align=\"left\" style=\"font-size: 1.15em; padding-left: 1em;\">class</td><td align=\"left\" style=\"font-size: 1.15em; padding-left: 1em;\">order</td><td align=\"left\" style=\"font-size: 1.15em; padding-left: 1em;\">family</td><td align=\"left\" style=\"font-size: 1.15em; padding-left: 1em;\">genus</td><td style=\"font-size: 1.15em;\">count</td><td style=\"font-size: 1.15em;\">&nbsp;%</td></tr>\n";
	my $sum = 0;
	for my $g ( @genera )	{
		my $n = $genus_no{$g};
		if ( $parent{$n}{'class'} || $parent{$n}{'order'} ||  $parent{$n}{'family'} )	{
			$sum += $count{$g};
		}
	}
	my $printed = 0;
	for my $g ( @genera )	{
		my $n = $genus_no{$g};
		if ( $parent{$n}{'class'} || $parent{$n}{'order'} ||  $parent{$n}{'family'} )	{
			$printed++;
			my $class = "";
			if ( $printed % 2 == 1 )	{
				$class = qq|class="darkList"|;
			}
			print "<tr $class>\n";
			print "<td align=\"center\" style=\"padding-left: 1em; padding-right: 1em;\">&nbsp;$printed</td>\n";
			for my $rank ( 'class','order','family' )	{
				print "<td style=\"font-size: 0.9em; padding-left: 1em; padding-right: 1em;\">$parent{$n}{$rank}</td>\n";
			}
			print qq|<td style=\"padding-left: 1em; padding-right: 1em;\"><i><a href="$READ_URL?action=checkTaxonInfo&amp;taxon_name=$g&amp;is_real_user=1">$g</a></i></td>|;
			print "\n<td align=\"center\" style=\"padding-left: 1em; padding-right: 1em;\">&nbsp;&nbsp;$count{$g}</td>\n";
			printf "<td align=\"center\" style=\"padding-left: 1em; padding-right: 1em;\">%.1f</td>\n",$count{$g}/$sum*100;
			print "</tr>\n";
		}
		if ( $printed == $q->param('rows') )	{
			last;
		}
	}
	print "</table>\n\n";
	print "</div>\n";
	print "</div>\n";
	print "<center><p class=\"large\">Total number of occurrences: $sum</p></center>\n";

}

1;
