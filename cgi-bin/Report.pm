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
    my ($class, $taxonomy, $dbt, $q, $s) = @_;
    my $self = { 
		dbt => $dbt,
		q => $q,
		s => $s,
		taxonomy => $taxonomy
	       };
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
    #    print "<pre>".Dumper($self->{'dataTable'}) . "</pre>";
}


##
# This function builds the SQL statement to pull data out of the database, then executes that SQL, 
# returning the variable $sth, a statement handle to the data
##
sub reportQueryDB{
    my ($self) = @_;
    my $q = $self->{q};
    my $taxonomy = $self->{taxonomy};
    my $dbt = $self->{dbt};
    my $dbh = $dbt->{dbh};
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
        'country'=>'country', 'state'=>'state', 'interval name'=>'max_interval_no,min_interval_no', 'formation'=>'formation', 'geological group'=>'geological_group',
        'paleoenvironment'=>'environment', 'scale of geographic resolution'=>'geogscale', 
        'scale of stratigraphic resolution'=>'stratscale',
        'tectonic setting'=>'tectonic_setting', 'preservation mode'=>'pres_mode', 
        'assemblage components'=>'assembl_comps', 'reason for describing collection'=>'collection_type',
        'list coverage'=>'collection_coverage', 'lithification'=>'lithification,lithification2',
        'lithology - all combinations'=>'lithology1,lithology2', 'lithology - weighted'=>'lithology1,lithology2',
        'continent'=>'country', '10 m.y. bins (most common order)'=>'max_interval_no,min_interval_no', 'Gradstein 3: Periods (most common order)'=>'max_interval_no,min_interval_no', 'Gradstein 5: Epochs (most common order)'=>'max_interval_no,min_interval_no', 'Gradstein 7: Stages (most common order)'=>'max_interval_no,min_interval_no', '10 m.y. bins (standard order)'=>'max_interval_no,min_interval_no','Gradstein 3: Periods (standard order)'=>'max_interval_no,min_interval_no', 'Gradstein 5: Epochs (standard order)'=>'max_interval_no,min_interval_no', 'Gradstein 7: Stages (standard order)'=>'max_interval_no,min_interval_no','tectonic plate ID'=>'latdeg,latdir,lngdeg,lngdir','paleocontinent'=>'plate');
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
            foreach my $taxon_name (@taxa) {
                my @taxon_nos = $taxonomy->getTaxaByName($taxon_name, { common => 1, id => 1 });
                dbg("Found ".scalar(@taxon_nos)." taxon_nos for $taxon_name");
                if (scalar(@taxon_nos) == 0) {
                    $genus_names_string .= ", ".$dbh->quote($taxon_name);
                } elsif (scalar(@taxon_nos) == 1) {
                    my @all_taxon_nos = $taxonomy->getRelatedTaxa($taxon_nos[0], 'all_children',
								  { id => 1 } );
                    # Uses hash slices to set the keys to be equal to unique taxon_nos.  Like a mathematical UNION.
                    @taxon_nos_unique{@all_taxon_nos} = ();
                } else { #result > 1
                    push @{$self->{'warnings'}}, "The counts are not restricted to '$taxon_name' because more than one taxon has that name. If this is a problem email <a href='mailto: alroy\@nceas.ucsb.edu'>John Alroy</a>.";
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

    if ($groupSQL) {
        dbg("SQL:".$sql);
       
        my $sth = $dbh->prepare($sql) || die "Prepare query failed\n";
        $sth->execute() || die "Execute query failed\n";
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
    } elsif ($param eq 'paleocontinent') {
        my $pcontinents = $self->getPaleocontinents();
        %table = %{$pcontinents};
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

# JA 4.9.09
sub getPaleocontinents	{
	my $self = shift;
	my $dbt = $self->{dbt};
	my %pcontinents;
	my $sql = "SELECT plate,paleocontinent FROM plates";
	my @refs = @{$dbt->getData($sql)};
	for my $r ( @refs )	{
		$pcontinents{$r->{'plate'}} = $r->{'paleocontinent'};
		if ( ! $r->{'paleocontinent'} )	{
			$pcontinents{$r->{'plate'}} = "plate ".$r->{'plate'};
		}
	}
	return \%pcontinents;
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
	my $s = $self->{s};
	my $taxonomy = $self->{taxonomy};
	my $dbt = $self->{dbt};

	my @dataRows = @{$dataRowsRef};
	my @collection_nos = map {$_->{'collection_no'}} @dataRows;

	my $atrank = $q->param('rank');

	my $names;
	if ( $atrank eq "species" )	{
		$names = "genus_name,species_name,";
	}
	my $sql = "SELECT $names taxon_no,occurrence_no,collection_no FROM occurrences WHERE taxon_no>0 AND collection_no IN (" . join(',',@collection_nos) . ")";
	my $sql2 = "SELECT $names taxon_no,occurrence_no,collection_no FROM reidentifications WHERE most_recent='YES' AND taxon_no>0 AND collection_no IN (" . join(',',@collection_nos) . ")";
	# WARNING: will take largest taxon if there are homonyms
	if ( $q->param('taxon_name') )
	{
	    # Get all matching taxa, and then iterate through them to find the largest
	    my $largest_taxon = $taxonomy->getTaxaByName($q->param('taxon_name'), 
						{ common => 1, fields => 'lft', order => 'size.desc' } );
	    my @taxon_nos = $taxonomy->getRelatedTaxa($largest_taxon, 'all_children',
							  { id => 1 });
	    my $taxon_list = join(',', @taxon_nos);
	    $sql = "SELECT $names taxon_no,occurrence_no,collection_no FROM occurrences WHERE taxon_no IN (".$taxon_list.") AND collection_no IN (" . join(',',@collection_nos) . ")";
	    $sql2 = "SELECT $names taxon_no,occurrence_no,collection_no FROM reidentifications WHERE most_recent='YES' AND taxon_no IN (".$taxon_list.") AND collection_no IN (" . join(',',@collection_nos) . ")";
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

	# get the name and rank of each taxon's synonym or (if valid)
	#  current spelling
	my @taxa2 = $taxonomy->getRelatedTaxa(\%hasno, 'self', { fields => 'link' });
	
	my %synonym;
	for my $r ( @taxa2 )	{
		$synonym{$r->{'taxon_no'}} = $r->{'synonym_no'};
	}
	
	# get parents at higher ranks
	my @ranks = "class";
	if ( $atrank =~ /order|family|genus|species/ )	{
		push @ranks , "order";
	}
	if ( $atrank =~ /family|genus|species/ )	{
		push @ranks , "family";
	}
	if ( $atrank =~ /genus|species/ )	{
		push @ranks , "genus";
	}
	if ( $atrank =~ /species/ )	{
		push @ranks , "species";
	}
	my $rank_list = join(',', @ranks);
	my @parentrows = $taxonomy->getRelatedTaxa(\%hasno, 'all_parents', { rank => $rank_list });
	
	my %parent;
	for my $r ( @parentrows )	{
		$parent{$r->{'child_no'}}{$r->{'parent_rank'}} = $r->{'parent'};
	}
	@parentrows = ();

	for my $r ( @taxa2 )	{
		if ( $r->{'taxon_rank'} eq $atrank )	{
			$parent{$r->{'synonym_no'}}{$atrank} = $r->{'taxon_name'};
		} elsif ( $atrank eq "species" && $r->{'taxon_rank'} eq "genus" )	{
			$parent{$r->{'synonym_no'}}{$atrank} = $r->{'taxon_name'};
		}
	}

	# count occurrences
	my %count;
	my %child_no;
	my %collseen;
	for my $r ( @rows2 )	{
		my $name = $parent{$synonym{$r->{'taxon_no'}}}{$atrank};
		if ( $atrank eq "species" && $r->{'species_name'} =~ /^[a-z]*$/ && $name !~ / / )	{
			$name = '"'.$name." ".$r->{'species_name'}.'"';
		}
		if ( ( $atrank ne "species" && $name =~ /^[A-Z][a-z]*$/ ) || ( $atrank eq "species" && $name =~ /[A-Z][a-z]* [a-z]*/ ) )	{
			$count{$name}++;
			$child_no{$name} = $synonym{$r->{'taxon_no'}};
			$collseen{$r->{'collection_no'}}++;
		}
		$seen{$r->{'occurrence_no'}}++;
	}
	for my $r ( @rows )	{
		if ( ! $seen{$r->{'occurrence_no'}} )	{
			my $name = $parent{$synonym{$r->{'taxon_no'}}}{$atrank};
			if ( $atrank eq "species" && $r->{'species_name'} =~ /^[a-z]*$/ && $name !~ / / )	{
				$name = '"'.$name." ".$r->{'species_name'}.'"';
			}
			if ( ( $atrank ne "species" && $name =~ /^[A-Z][a-z]*$/ ) || ( $atrank eq "species" && $name =~ /[A-Z][a-z]* [a-z]*/ ) )	{
				$count{$name}++;
				$child_no{$name} = $synonym{$r->{'taxon_no'}};
				$collseen{$r->{'collection_no'}}++;
			}
		}
	}
	%seen = ();
	my @temp = keys %collseen;
	my $collections = $#temp + 1;
	@temp = ();
	%collseen = ();

	my @taxa = keys %count;
	@taxa = sort { $count{$b} <=> $count{$a} } @taxa;

	my $username = ($s->get("enterer")) ? $s->get("enterer") : "";
	my $filename = PBDBUtil::getFilename($username);


	my $csv = Text::CSV_XS->new({'binary'=>1});
	PBDBUtil::autoCreateDir("$HTML_DIR/public/taxa");
	open OUT,">$HTML_DIR/public/taxa/${filename}_taxa.csv";

	my $plural;
	if ( $atrank eq "class" )	{
		$plural = "classes";
	} elsif ( $atrank eq "order" )	{
		$plural = "orders";
	} elsif ( $atrank eq "family" )	{
		$plural = "families";
	} elsif ( $atrank eq "genus" )	{
		$plural = "genera";
	} elsif ( $atrank eq "species" )	{
		$plural = "species";
	}
	print "<center><p class=\"pageTitle\">The most common $plural in these collections</p></center>\n\n";

	print "<div class=\"displayPanel\" style=\"width: 50em; margin-left: 1em;\">\n";
	print "<div class=\"displayPanelContent\">\n";
	print "<table class=\"small\" style=\"margin-left: 1em; margin-top: 1em; margin-bottom: 1em;\">\n";
	print "<tr align=\"center\"><td style=\"font-size: 1.15em;\">rank</td>";
	print OUT "rank,";
	print "<td align=\"left\" style=\"font-size: 1.15em; padding-left: 1em;\">class</td>";
	print OUT "class,";
	if ( $atrank =~ /family|genus|species/ )	{
		print "<td align=\"left\" style=\"font-size: 1.15em; padding-left: 1em;\">order</td>";
		print OUT "order,";
	}
	if ( $atrank =~ /genus|species/ )	{
		print "<td align=\"left\" style=\"font-size: 1.15em; padding-left: 1em;\">family</td>";
		print OUT "family,";
	}
	print "<td align=\"left\" style=\"font-size: 1.15em; padding-left: 1em;\">$atrank</td>";
	print OUT "$atrank,";
	print "<td style=\"font-size: 1.15em;\">count</td>";
	print OUT "count,";
	print "<td style=\"font-size: 1.15em;\">&nbsp;%</td></tr>\n";
	print OUT "percent\n";
	my $sum = 0;
	for my $t ( @taxa )	{
		my $n = $child_no{$t};
		if ( $parent{$n}{'class'} || $parent{$n}{'order'} ||  $parent{$n}{'family'} )	{
			$sum += $count{$t};
		}
	}
	my $printed = 0;
	if ( $atrank eq "species" )	{
		pop @ranks;
	}
	pop @ranks;
	my $quoted;
	for my $t ( @taxa )	{
		my $n = $child_no{$t};
		if ( $parent{$n}{'class'} || $parent{$n}{'order'} ||  $parent{$n}{'family'} )	{
			$printed++;
			my $class = "";
			if ( $printed % 2 == 1 )	{
				$class = qq|class="darkList"|;
			}
			print "<tr $class>\n";
			print "<td align=\"center\" style=\"padding-left: 1em; padding-right: 1em;\">&nbsp;$printed</td>\n";
			for my $rank ( @ranks )	{
				print "<td style=\"font-size: 0.9em; padding-left: 1em; padding-right: 1em;\">$parent{$n}{$rank}</td>\n";
			}
			my $linkname = $t;
			if ( $t =~ /"/ )	{
				$linkname =~ s/"//g;
				$quoted++;
			}
			my $displayname = $t;
			if ( $atrank =~ /genus|species/ )	{
				$displayname = "<i>" . $t . "</i>";
			}
			print qq|<td style=\"padding-left: 1em; padding-right: 1em;\"><a href="$READ_URL?action=checkTaxonInfo&amp;taxon_name=$linkname&amp;is_real_user=1">$displayname</a></td>|;
			print "\n<td align=\"center\" style=\"padding-left: 1em; padding-right: 1em;\">&nbsp;&nbsp;$count{$t}</td>\n";
			printf "<td align=\"center\" style=\"padding-left: 1em; padding-right: 1em;\">%.1f</td>\n",$count{$t}/$sum*100;
			print "</tr>\n";
		}
		if ( $printed == $q->param('rows') )	{
			last;
		}
	}
	$printed = 0;
	for my $t ( @taxa )	{
		my $n = $child_no{$t};
		if ( $parent{$n}{'class'} || $parent{$n}{'order'} ||  $parent{$n}{'family'} )	{
			$printed++;
			print OUT "$printed,";
			for my $rank ( @ranks )	{
				print OUT "$parent{$n}{$rank},";
			}
			my $printedname = $t;
			if ( $printedname =~ / / && $printedname !~ /"/ )	{
				$printedname = '"'.$printedname.'"';
			}
			print OUT "$printedname,";
			print OUT "$count{$t},";
			printf OUT "%.2f\n",$count{$t}/$sum*100;
		}
	}
	print "</table>\n\n";
	print "</div>\n";
	print "</div>\n";
	if ( $printed == 1 )	{
		$plural = $atrank;
	}
	printf "<center><p class=\"large\">In total there are $sum occurrences of %d $plural that come from $collections collections.</p></center>\n",$printed;
	print "<center><p class=\"large\">You can <a href=\"/public/taxa/${filename}_taxa.csv\">download</a> a comma-delimited version of this table listing all of the $plural.</p>\n";
	if ( $quoted > 0 )	{
		print "<center><p class=\"small\">We have no formal taxonomic data for names in quotes, so they may be invalid.</p></center>\n";
	}
	close OUT;

}

# JA 23.10.10
sub fastTaxonCount {
    
    my ($dbt, $taxonomy, $q, $s, $hbo) = @_;

	print qq|
<div align="center">
  <p class="pageTitle">Taxon counts</p>
</div>

<div class="displayPanel">
|;

	my $sql;
	my $errors;
	my @qs;
	push @qs , "taxon name = ".$q->param('taxon_name');
	if ( $q->param('period') )	{
		push @qs , "time interval = ".$q->param('period');
		$sql = "SELECT interval_no FROM intervals WHERE interval_name='".$q->param('period')."'";
		my $no = ${$dbt->getData($sql)}[0]->{interval_no};
		$q->param('period' => $no);
		$q->param('interval' => '');
	}
	my $interval_nos;
	if ( $q->param('interval') )	{
		push @qs , "time interval = ".$q->param('interval');
		require TimeLookup;
		my $t = new TimeLookup($dbt);
		my $interval = $q->param('interval');
		my $eml;
		if ( $interval =~ /early |lower /i )	{
			$eml = 'early/lower';
			$interval =~ s/early //i;
			$interval =~ s/lower //i;
		} elsif ( $interval =~ /middle/i )	{
			$eml = 'middle';
			$interval =~ s/middle //i;
		} elsif ( $interval =~ /late |upper /i )	{
			$eml = 'late/upper';
			$interval =~ s/late //i;
			$interval =~ s/upper //i;
		}
		my ($intervals,$errs,$warnings) = $t->getRange($eml,$interval,'','');
		$interval_nos = join(',',@$intervals);
		if ( $interval_nos eq "" )	{
			$errors = "Sorry, there is no time interval called '".$q->param('interval')."'";
		}
	}
	if ( $q->param('strat_unit') )	{
		push @qs , "stratigraphic unit = ".$q->param('strat_unit');
	}
	if ( $q->param('continent') )	{
		push @qs , "continent = ".$q->param('continent');
		$q->param('country' => '');
	}
	elsif ( $q->param('country') )	{
		push @qs , "country/state = ".$q->param('country');
	}
	if ( $q->param('author') )	{
		push @qs , "author of name = ".$q->param('author');
	}
	if ( $q->param('after') && $q->param('before') )	{
		push @qs , "name published between ".$q->param('after'). " and ".$q->param('before');
	}
	elsif ( $q->param('after') )	{
		push @qs , "name published in or after ".$q->param('after');
	}
	elsif ( $q->param('before') )	{
		push @qs , "name published in or before ".$q->param('before');
	}

	my $enterer;
	if ( $q->param('enterer_reversed') )	{
		push @qs , "data enterer = ".$q->param('enterer_reversed');
		$sql = "SELECT person_no FROM person WHERE reversed_name='".$q->param('enterer_reversed')."'";
		$enterer = ${$dbt->getData($sql)}[0]->{person_no};
	}
	print "<p style=\"margin-left: 2em; text-indent: -0.5em;\">Your query was: <i>".join(', ',@qs).".</i>";

	my @tables = ("authorities a,$taxonomy->{tree_table} t,$taxonomy->{tree_table} t2");
	my @and;

	if ( $q->param('period') || $interval_nos || $q->param('strat_unit') || $q->param('continent') || $q->param('country') )	{
		push @tables , "collections c,occurrences o";
		push @and , "c.collection_no=o.collection_no AND o.taxon_no=t.taxon_no";
	}

	if ( $q->param('period') )	{
		push @tables , "interval_lookup i";
		push @and , "c.max_interval_no=i.interval_no AND i.period_no=".$q->param('period');
	} elsif ( $interval_nos )	{
		push @and , "c.max_interval_no IN ($interval_nos)";
	}

	if ( $q->param('strat_unit') )	{
		push @and , "(geological_group='".$q->param('strat_unit')."' OR formation='".$q->param('strat_unit')."' OR member='".$q->param('strat_unit')."')";
	}

	if ( $q->param('continent') )	{
		# ugly but effective
		my $c = $q->param('continent');
		my $countries = `grep '$c' data/PBDB.regions`;
		$countries =~ s/.*://;
		$countries =~ s/'/\\'/;
		$countries =~ s/\t/','/g;
		push @and , "country IN ('".$countries."')";
	} elsif ( $q->param('country') )	{
		push @and , "(country LIKE '".$q->param('country')."%' OR state LIKE '".$q->param('country')."%')";
	}

	if ( $q->param('author') || $q->param('before') > 0 || $q->param('after') > 0 )	{
		push @tables , "refs r";
		push @and , "r.reference_no=a.reference_no";
	}

	if ( $q->param('author') )	{
		if ( $q->param('author') =~ /[^A-Za-z ']/ )	{
			$errors = "Sorry, the author name you entered is misformatted.";
		}
		push @and , "(((r.author1last='".$q->param('author')."' AND ref_is_authority='YES') OR a.author1last='".$q->param('author')."') OR ((r.author2last='".$q->param('author')."' AND ref_is_authority='YES') OR a.author2last='".$q->param('author')."'))";
	}

	if ( $q->param('before') > 0 || $q->param('after') > 0 )	{
		if ( $q->param('after') && ( $q->param('after') =~ /[^0-9]/ || $q->param('after') < 1700 || $q->param('after') > 2100 ) )	{
			$errors = "Sorry, the 'after' year you entered is misformatted.";
		} elsif ( $q->param('before') && ( $q->param('before') =~ /[^0-9]/ || $q->param('before') < 1700 || $q->param('before') > 2100 ) )	{
			$errors = "Sorry, the 'before' year you entered is misformatted.";
		}
		if ( $q->param('before') > 0 )	{
			push @and , "((r.pubyr<=".$q->param('before')." AND ref_is_authority='YES') OR (a.pubyr<=".$q->param('before')." AND a.pubyr>1700))";
		}
		if ( $q->param('after') > 0 )	{
			push @and , "((r.pubyr>=".$q->param('after')." AND ref_is_authority='YES') OR a.pubyr>=".$q->param('after').")";
		}
	}

	if ( $enterer )	{
		push @and , "a.enterer_no=".$enterer;
	}

	if ( $q->param('taxon_name') )
	{
	    my $largest_taxon = $taxonomy->getTaxaByName($q->param('taxon_name'), 
						{ common => 1, fields => 'lft', order => 'size' });
	    if ( defined $largest_taxon )
	    {
		push @and, "t2.lft>$largest_taxon->{lft} AND t2.rgt<$largest_taxon->{rgt}";
	    } 
	    else
	    {
		$errors = "Sorry, there is no valid taxon in the database called \"$q->param('taxon_name')\".";
	    }
	} 
	else
	{
	    $errors = "Sorry, you must enter a taxon name.";
	}
	
	# query excludes higher-order names lacking any genera or species
	# we need a much more complicated join because senior synonyms can be invalid
	#  (especially in nomen dubium cases)
	# t.taxon_no (possibly used in an occurrences join) is the junior synonym,
	#  t.synonym_no and t2.taxon_no are its senior synonym, and t2.synonym_no is the
	#   senior senior synonym
	$sql = "SELECT t2.lft,t2.rgt,taxon_rank,taxon_name,extant FROM ".join(',',@tables)." WHERE t.synonym_no=t2.taxon_no AND t2.synonym_no=a.taxon_no AND (taxon_rank IN ('species','genus') OR t2.rgt>t2.lft+1)";
	my ($sql2,@and2);
	if ( $sql =~ /occurrences/ )	{
		$sql2 = $sql;
		$sql2 =~ s/(occurrences o,)/$1reidentifications re,/;
		$sql2 =~ s/(occurrences o)( )/$1,reidentifications re /;
		@and2 = @and;
		push @and2 , "o.occurrence_no=re.occurrence_no AND re.most_recent='YES'";
		$sql =~ s/(occurrences o)/$1 LEFT JOIN reidentifications re ON o.occurrence_no=re.occurrence_no/;
		push @and , "re.reid_no IS NULL";
	}

	if ( @and )	{
		$sql .= " AND ".join(' AND ',@and);
	}
	if ( @and2 )	{
		$sql2 .= " AND ".join(' AND ',@and2);
	}
	my $group = " GROUP BY t2.synonym_no ORDER BY t2.lft";
	if ( $sql2 )	{
		$sql = "($sql $group) UNION ($sql2 $group)";
	} else	{
		$sql .= $group;
	}

	my @subtaxa = @{$dbt->getData($sql)};

	if ( ! @subtaxa )	{
		$errors = "Sorry, the query seems fine but nothing matched it.";
	}

	if ( $errors )	{
		print "</p>\n\n";
		print "<div align=\"center\" style=\"padding-bottom: 1em;\">&bull; $errors</div>\n\n";
		print "</div>\n\n";
		print "<div align=\"center\"><a href=\"$READ_URL?action=fastTaxonCount\">Count more taxa</a></div>\n\n";
		return;
	}
	print " The counts are:</p>\n\n";


	# a faster method could be used if queries were only ever by taxon name, but
	#  it's needed to integrate occurrence data
	my (%count,%list,%empty);
	for my $s ( @subtaxa )	{
		$count{$s->{taxon_rank}}++;
		push @{$list{$s->{taxon_rank}}} , $s->{taxon_name};
		if ( $s->{'lft'} + 1 == $s->{'rgt'} && $s->{'extant'} !~ /y/i && $s->{'taxon_rank'} !~ /species/ )	{
			$empty{$s->{'taxon_rank'}}++;
		}
	}
	my %plural = ('order'=>'orders','family'=>'families','genus'=>'genera','species'=>'species' );
	for my $r ( 'order','family','genus','species' )	{
		print "<div style=\"margin-left: 2em; text-indent: -0.5em;\">\n";
		if ( $count{$r} > 0 )	{
			my $name = $plural{$r};
			if ( $count{$r} == 1 )	{
				$name = $r;
			}
			print "<p>",$count{$r}," $name";
			if ( $#{$list{$r}} <= 100 )	{
				@{$list{$r}} = sort @{$list{$r}};
				print " <span class=\"small\">(".join(', ',@{$list{$r}}).")</span>";
			}
			print "</p>\n";
		}
		print "</div>\n\n";
	}
	my @empties;
	for my $r ( 'order','family','genus' )	{
		if ( $empty{$r} )	{
			push @empties , $empty{$r}." ".$plural{$r};
		}
	}
	if ( @empties )	{
		if ( $#empties > 0 )	{
			push @empties , " and ".pop @empties;
		}
		my $warning = "<div align=\"center\"><p><i>Warning: ";
		$warning .= join(', ',@empties);
		my $subtaxa = "subtaxa";
		if ( $#empties == 0 && $empties[0] =~ / gen/ )	{
			$subtaxa = "species";
		}
		$warning .= " do not include $subtaxa with taxonomic classification data.</i></p>\n";
		$warning =~ s/ 1 families/ one family/;
		$warning =~ s/ 1 genera/ one genus/;
		if ( $#empties == 0 && $warning =~ / one / )	{
			$warning =~ s/do not/does not/;
		}
		print $warning,"</div>\n\n";
	}

	print "</div>\n\n";
	print "<div align=\"center\"><a href=\"$READ_URL?action=displayCountForm&page=taxon_count_form\"><b>Count more taxa</b></a></div>\n\n";
}


1;
