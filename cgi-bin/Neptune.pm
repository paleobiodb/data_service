#!/usr/bin/perl

# Connects to the (postgresql) database currently hosted by CHRONOS at
# cdb0.geol.iastate.edu.  Provides forms to download their data as well, which may
# then be fed into the Curve script for diversity analyses

package Neptune;
use strict;
use DBI;
use Debug;

# Flags and constants
my $DEBUG=0; # The debug level of the calling program
my $OUT_FILE_DIR = $ENV{DOWNLOAD_OUTFILE_DIR};                                                                                                                    
$|=1; #free flowing data

my @fields_summary = ('resolved_fossil_name','number of samples','number of samples (pacman)','FO','LO','FO (pacman)','LO (pacman)','FO (literature)','LO (literature)','FO (diff)','LO (diff)','Range','Range (pacman)');
my @fields_all = ('leg','site','hole','hole_id','ocean_code','longitude','latitude','paleo_latitude','paleo_longitude','sample_id','sample_group_abundance','sample_age_ma','sample_preservation','sample_depth_mbsf','taxon_abundance','taxon_id','fossil_name','genus_name','species_name','taxon_status','resolved_taxon_id','resolved_fossil_name','resolved_genus_name','resolved_specise_name','resolved_taxon_status');
my %fields_from_db = (
'leg' => 'a.leg',
'site' => 'a.site',
'hole' => 'a.hole',
'hole_id' => 'a.hole_id',
'sample_id' => 'b.sample_id',
'ocean_code' => 'a.ocean_code',
'longitude' => 'a.longitude',
'latitude' => 'a.latitude',
'paleo_longitude' => 'b.paleo_longitude',
'paleo_latitude' => 'b.paleo_latitude',
'sample_group_abundance' => 'b.sample_group_abundance',
'sample_age_ma' => 'b.sample_age_ma',
'sample_preservation' => 'b.sample_preservation',
'sample_depth_mbsf' => 'b.sample_depth_mbsf',
'taxon_abundance'=> 'c.taxon_abundance',
'taxon_id' => 'c.taxon_id',
'fossil_name' => "trim(initcap(d.genus)||' '||lower(d.species)||' '||lower(coalesce(d.subspecies,' '))) AS fossil_name",
'genus_name' => "initcap(d.genus) AS genus_name",
'species_name' => "lower(d.species) AS species_name",
'taxon_status' => 'd.taxon_status',
'resolved_taxon_id' => 'e.taxsyn_id AS resolved_taxon_id',
'resolved_fossil_name' => "trim(initcap(f.genus)||' '||lower(f.species)||' '||lower(coalesce(f.subspecies,' '))) AS resolved_fossil_name",
'resolved_genus_name' => "initcap(f.genus) AS genus_name",
'resolved_species_name' => "lower(f.species) AS species_name",
'resolved_taxon_status' => 'f.taxon_status AS resolved_taxon_status'
);

              
                            

# Sets up the three files we're going to write to:
#   summary - file that contains summary information 
sub setupOutput {
    my ($q,$s) = @_;

    my $sepChar = ",";
    my $ext = "csv";
    if ($q->param("output_format") eq 'tab') {
        $sepChar = "\t";
        $ext = "tab";
    }
    my $csv = Text::CSV_XS->new({
        'quote_char'  => '"',
        'escape_char' => '"',
        'sep_char'    => $sepChar,
        'binary'      => 1
    });

    my $name = ($s->get("enterer")) ? $s->get("enterer") : $q->param("yourname");
    my $filename = PBDBUtil::getFilename($name); 

    my $summary_file = "$OUT_FILE_DIR/$filename-neptune_summary.$ext";
    my $results_file = "$OUT_FILE_DIR/$filename-neptune_pacman.$ext";
    my $results_all_file = "$OUT_FILE_DIR/$filename-neptune.$ext";
    if ($q->param('data_source') =~ /both/i) {
        $summary_file = "$OUT_FILE_DIR/$filename-neptune_pbdb_summary.$ext";
        $results_file = "$OUT_FILE_DIR/$filename-neptune_pbdb_pacman.$ext";
        $results_all_file = "$OUT_FILE_DIR/$filename-neptune_pbdb.$ext";
    }

    # Open pacman summary file
    open(PS, ">$summary_file") 
        or die ("Could not open output file: $summary_file ($!)");
    # Open pacman results file
    open(PR, ">$results_file") 
        or die ("Could not open output file: $results_file ($!)");
    # Open all results file
    open(PA, ">$results_all_file") 
        or die ("Could not open output file: $results_all_file ($!)");


    return ($csv,$filename);
}

sub displayNeptuneDownloadResults {
    my $q = shift;
    my $s = shift;
    my $hbo = shift;
    my $dbt = shift;
    
    my $dbh_neptune = Neptune::connect();
    my @form_notes;

    if ($q->param('data_source') =~ /both/) {
        print "<div align=\"center\"><h2>Neptune/Paleobiology Database download results</h2></div>";
    } else {
        print "<div align=\"center\"><h2>Neptune download results</h2></div>";
    }

    my $pbdb_results;
    if ($q->param('data_source') =~ /both/) {
        push @form_notes, "The summary file only contains taxa and information pertaining to Neptune data.  Paleobiology Database data are not included in the summary";
        push @form_notes, "The Pacman algorithm only applies to Neptune data, not Paleobiology Database data";
        my $q2 = new CGI;
        my $taxon_name = $q->param("taxon_name"); 
        my @taxa = parseTaxa($taxon_name);
        my @genera = ();
        foreach my $taxon (@taxa) {
            my ($genus,$species,$subspecies) = split(/ /,$taxon);
            push @genera, $genus;
            if ($species) {
                push @form_notes, "Paleobiology Database data are only referenced at the genus level.  Using '$genus' instead of '$genus $species'";
            }
        } 
        $taxon_name = join(" ",@genera);
        # Translate fossil groups into names
        if ($q->param("taxon_name")) {
            # Don't get general group if we alreayd have more specific group
            $q2->param("fossil_group"=>"");
        }
        my @fossil_groups = $q2->param("fossil_group");
        foreach my $g (@fossil_groups) {
            if ($g =~ /^R$/) {
                $taxon_name .= " Radiolaria";
            } elsif ($g =~ /^F$/) {
                $taxon_name .= " Foraminiferida";
                push @form_notes, "For Paleobiology Databse data, mapping informal group 'planktic foraminifera' to 'Foraminiferida'";
            } elsif ($g =~ /^D$/) {
                $taxon_name .= " Bacillariophyceae";
                push @form_notes, "For Paleobiology Datbaase data, mapping informal group 'diatoms' to 'Bacillariophyceae'";
            } elsif ($g =~ /^N$/) {
                $taxon_name .= " Haptophyta";
                push @form_notes, "For Paleobiology Database data, mapping informal group 'nannoplankton' to 'Haptophyta'";
            }
        }

        $q2->param("taxon_name"=>$taxon_name);

        # Transfer over lat/lng ranges - rest will use defaults
        foreach ('latmax','latmin','lngmax','lngmin') {
            $q2->param($_."1"=>$q->param($_));
        }

        $q2->param('max_interval_name'=>$q->param("max_age"));
        $q2->param('min_interval_name'=>$q->param("min_age"));

        $q2->param('use_midpoints'=>'YES');
        $q2->param('collections_coords'=>'YES');
        $q2->param('collections_coords_format'=>'decimal');
        $q2->param('collections_paleocoords'=>'YES');
        $q2->param('collections_paleocoords_format'=>'decimal');
        $q2->param('collections_ma_mid'=>'YES');
        $q2->param('occurrences_species_name'=>'YES');
        $q2->param('occurrences_taxon_no'=>'YES');
        $q2->param('occurrences_abund_value'=>'YES');
        $q2->param('occurrences_abund_unit'=>'YES');

        my $d = new Download($dbt,$q2,$s,$hbo);
        ($pbdb_results) = $d->queryDatabase();
    }
    my $results = queryNeptuneDB($q,$dbh_neptune);
    my ($csv,$filename) = setupOutput($q,$s);

    my %rows_by_taxon = ();
    my %taxon_names = ();
    foreach my $row (@$results) {
        push @{$rows_by_taxon{$row->{'resolved_taxon_id'}}},$row;
        $taxon_names{$row->{'resolved_taxon_id'}} = $row->{'resolved_fossil_name'};
    }

    my %pbdb_rows_by_taxon;
    my %pbdb_taxon_names;
    foreach my $row (@$pbdb_results) {
        $row->{'resolved_taxon_id'} = "$row->{'o.taxon_no'}_$row->{'o.genus_name'}_$row->{'o.species_name'}";
        $row->{'resolved_fossil_name'} = $row->{'o.genus_name'}." ".$row->{'o.species_name'};
        
        $row->{'sample_id'} = $row->{'collection_no'};
        if ($row->{'or.genus_name'}) {
            $row->{'fossil_taxon_id'} = "$row->{'or.taxon_no'}_$row->{'or.genus_name'}_$row->{'or.species_name'}"; 
            $row->{'fossil_name'} = $row->{'or.genus_name'}." ".$row->{'or.species_name'};
        } else {
            $row->{'fossil_taxon_id'} = "$row->{'o.taxon_no'}_$row->{'o.genus_name'}_$row->{'o.species_name'}"; 
            $row->{'fossil_name'} = $row->{'o.genus_name'}." ".$row->{'o.species_name'};
        }

        $row->{'latitude'} = $row->{'c.latdec'};
        $row->{'longitude'} = $row->{'c.lngdec'};
        $row->{'paleo_latitude'} = $row->{'c.paleolatdec'};
        $row->{'paleo_longitude'} = $row->{'c.paleolngdec'};
        $row->{'taxon_abundance'} = $row->{'o.abund_value'};
        
        push @{$pbdb_rows_by_taxon{$row->{'resolved_taxon_id'}}},$row;
        $pbdb_taxon_names{$row->{'resolved_taxon_id'}} = "$row->{'o.genus_name'} $row->{'o.species_name'}";
    }

    my %literature_fo = ();
    my %literature_lo = ();

    my $sql = "SELECT MAX(datum_age_min_ma) AS max1,MIN(datum_age_min_ma) AS min1,".
           " MAX(datum_age_max_ma) AS max2,MIN(datum_age_max_ma) AS min2".
           " FROM neptune_datum_def ".
           " WHERE taxon_id LIKE ?"; 
    my $sth = $dbh_neptune->prepare($sql);

    foreach my $taxon_id (keys %taxon_names) {
        $sth->execute($taxon_id);
        if (my $row = $sth->fetchrow_hashref()) {
            if ($row->{min2} !~ /\d/) {
                $literature_lo{$taxon_id} = $row->{min1};
            } elsif ($row->{min1} !~ /\d/) {
                $literature_lo{$taxon_id} = $row->{min2};
            } elsif ($row->{min1} < $row->{min2}) {
                $literature_lo{$taxon_id} = $row->{min1};
            } else {
                $literature_lo{$taxon_id} = $row->{min2};
            }
            if ($row->{max2} > $row->{max1}) {
                $literature_fo{$taxon_id} = $row->{max2};
            } else {
                $literature_fo{$taxon_id} = $row->{max1};
            }
        }
    }

    my %all_names = ();
    while(my ($k,$v) = each %taxon_names) { $all_names{$k}=$v;}
    while(my ($k,$v) = each %pbdb_taxon_names) { $all_names{$k}=$v;}
    my @sorted_taxa = sort {$all_names{$a} cmp $all_names{$b}} keys %all_names;
    
    if ($DEBUG) {
        print "TAXA: ".join(", ",@sorted_taxa)."<BR>";
    }

    my $top_removal_percent = ($q->param("top_removal_percent") || 0);
    my $base_removal_percent = ($q->param("base_removal_percent") || 0);

    my @fields_all_selected = ();
    foreach my $field (@fields_all) {
        if ($q->param("neptune_$field")) {
            push @fields_all_selected, $field;
        }
    }
    print "ALL FIELDS HEADER: ".join(",",@fields_all_selected)."\n" if ($DEBUG);
    print "SUMMARY HEADER: ".join(",",@fields_summary)."\n" if ($DEBUG);

    # Print out headers first
    $csv->combine(@fields_summary);
    print PS $csv->string(),"\n";

    $csv->combine(@fields_all_selected);
    print PR $csv->string(),"\n";
    print PA $csv->string(),"\n";

    my ($neptune_count,$neptune_pacman_count,$pbdb_count,$pbdb_taxa_count,$neptune_taxa_count) = (0,0,0,0,0);
    foreach my $taxon_id (@sorted_taxa) {
        my @samples;
        my $from_neptune = 0;
        my ($pacman_base_index,$pacman_top_index);
        if ($pbdb_rows_by_taxon{$taxon_id}) {
            @samples = @{$pbdb_rows_by_taxon{$taxon_id}};
            if ($q->param("min_samples_pbdb") =~ /^\d+$/) {
                next if (scalar(@samples) < $q->param("min_samples_pbdb"));
            }
            $pbdb_taxa_count++;
        } else {
            $from_neptune = 1;
            @samples = @{$rows_by_taxon{$taxon_id}};
            
            my $raw_fo = $samples[-1]->{'sample_age_ma'};
            my $raw_lo = $samples[0]->{'sample_age_ma'};

            $pacman_top_index  = int(scalar(@samples) - .0000001 - scalar(@samples)*$top_removal_percent/100);
            $pacman_base_index = int(scalar(@samples)*$base_removal_percent/100);
            print "PACMAN INDEXES: $pacman_top_index AND $pacman_base_index<BR>" if ($DEBUG);
            my $pacman_fo = $samples[$pacman_top_index]->{'sample_age_ma'};
            my $pacman_lo = $samples[$pacman_base_index]->{'sample_age_ma'};

            my $literature_fo = ($literature_fo{$taxon_id} || "");
            my $literature_lo = ($literature_lo{$taxon_id} || "");
            
            my ($diff_fo,$diff_lo) = ('','');
            if ($literature_fo) {
                $diff_fo = abs($literature_fo-$pacman_fo);
            }
            if ($literature_lo) {
                $diff_lo = abs($literature_lo-$pacman_lo),
            }
        
            # First print out summary file line
            my @line = (
                $taxon_names{$taxon_id},
                scalar(@samples),
                (1+$pacman_top_index-$pacman_base_index),
                $raw_fo,
                $raw_lo,
                $pacman_fo,
                $pacman_lo,
                $literature_fo,
                $literature_lo,
                $diff_fo, 
                $diff_lo,
                ($raw_fo-$raw_lo),
                ($pacman_fo-$pacman_lo)
            );
            $csv->combine(@line);
            print PS $csv->string()."\n";
            $neptune_taxa_count++;
        }

        # Next print out all results file && pacman results file
        for(my $i=0;$i<scalar(@samples);$i++) {
            my @line = ();
            foreach my $field (@fields_all_selected) {
                my $value = $samples[$i]->{$field};
                if ($value =~ /^$/) {
                   $value = ""; 
                }
                push @line,$value;
            }
            
            $csv->combine(@line);
            my $line = $csv->string();
            $line =~ s/\n|\r//g;
            print PA $line,"\n";
            if ($from_neptune) {
                if ($i >= $pacman_base_index && $i <= $pacman_top_index)  {
                    print PR $line,"\n";
                    $neptune_pacman_count++;
                }
                $neptune_count++;
            } else {
                print PR $line,"\n";
                $pbdb_count++;
            }
        }
    }
    close PS;
    close PR;
    close PA;

    my $yourname = "";
    if ( $q->param('yourname') ne "" )  {
        $yourname = "&yourname=".PBDBUtil::getFilename($q->param('yourname')); 
    }
    my $ext = "csv";
    if ($q->param("output_format") eq 'tab') {
        $ext = "tab";
    }


    print "<p><div align=\"center\"><div style=\"width: 600;\" align=\"left\">";
    print "<h3 class=\"darkList\" style=\"margin-bottom: 0em;\">Results</h3>";
    if ($q->param('data_source') =~ /both/) {
        print "<p>$neptune_taxa_count Neptune taxa were printed to the <a href=\"/paleodb/data/$filename-neptune_pbdb_summary.$ext\">summary file</a></p>";
        print "<p>$pbdb_taxa_count additional Paleobiology Database taxa were downloaded</p>";
        print "<p>$neptune_pacman_count Neptune occurrences and $pbdb_count Paleobiology Database occurrences were printed to the <a href=\"/paleodb/data/$filename-neptune_pbdb_pacman.$ext\">Pacman results file</a></p>";
        print "<p>$neptune_count Neptune occurrences and $pbdb_count Paleobiology Database occurrences were printed to the <a href=\"/paleodb/data/$filename-neptune_pbdb.$ext\">results file</a></p>";
    } else {
        print "<p>$neptune_taxa_count taxa were printed to the <a href=\"/paleodb/data/$filename-neptune_summary.$ext\">summary file</a></p>";
        print "<p>$neptune_pacman_count occurrences were printed to the <a href=\"/paleodb/data/$filename-neptune_pacman.$ext\">Pacman results file</a></p>";
        print "<p>$neptune_count occurrences were printed to the <a href=\"/paleodb/data/$filename-neptune.$ext\">results file</a></p>";
    }
    print "</div></div></p>";

    if (@form_notes) {
        my $plural = (scalar(@form_notes) > 1) ? "s" : "";
        print "<p><br><div align=\"center\"><div style=\"width: 600;\" align=\"left\">";
        print "<h3 class=\"darkList\" style=\"margin-bottom: 0em;\">Note$plural</h3>";
        print "<p><li class=\"medium\">$_</li></p>" for (@form_notes);
        print "</div></div></p>";
    } 

    print "<p><div align=\"center\">";
    print "<a href=\"bridge.pl?action=displayDownloadNeptuneForm\"><b>Do another download</b></a> - ";
    if ($q->param('data_source') =~ /both/i) {
        print "<a href=\"bridge.pl?action=displayCurveForm&input_data=neptune_pbdb$yourname\"><b>Generate diversity curves</b></a>";
    } else {
        print "<a href=\"bridge.pl?action=displayCurveForm&input_data=neptune$yourname\"><b>Generate diversity curves</b></a>";
    }
    print "</div></p>";
}


sub queryNeptuneDB {
    my $q = shift;
    my $dbh = shift;

    my @errors = ();
    # If entered in reverse order swap them
    if ($q->param("min_age") && $q->param("max_age") && $q->param("min_age") > $q->param("max_age")) {
        my $tmp = $q->param("max_age");
        $q->param("max_age"=>$q->param("min_age"));
        $q->param("min_age"=>$tmp);
    }
  
    my @both_where = (); # clauses that appear in both the main and subselect clauses
    my @fields_all_selected = ();
    foreach my $field (@fields_all) {
        if ($q->param("neptune_".$field) || $field =~ /resolved_fossil_name|resolved_taxon_id|hole_id|sample_age_ma/) {
            push @fields_all_selected,$fields_from_db{$field};
        }
    }
    my $sql = "SELECT ".join(",",@fields_all_selected).
              " FROM neptune_hole_summary a, neptune_sample b, neptune_sample_taxa c, neptune_taxonomy d, neptune_taxonomy_synonym e,neptune_taxonomy f".
              " WHERE a.hole_id = b.hole_id".
              " AND b.sample_id = c.sample_id".
              " AND c.taxon_id = d.taxon_id".
              " AND d.taxon_id = e.taxon_id".
              " AND f.taxon_id = e.taxsyn_id";

    my @fossil_groups = $q->param("fossil_group");
    my @group_codes = ();
    foreach my $fossil_group (@fossil_groups) {
        if ($fossil_group =~ /^R|F|D|N$/) {
            push @group_codes, "'$fossil_group'";
        }
    }
    if (!@group_codes && !$q->param("taxon_name")) {
        push @errors, "Please select at least one fossil group or at least one taxonomic names";
    } elsif (@group_codes) {
        push @both_where, "b.fossil_group IN (".join (",",@group_codes).")";
    }

    if ($q->param("min_age") =~ /^\d+(\.\d+)?$/) {
        push @both_where, "b.sample_age_ma >= ".$q->param("min_age");
    } elsif ($q->param("min_age") !~ /^$/) {
        push @errors, "Minimum sample age must be a positive integer or decimal value";
    } else {
        push @both_where, "b.sample_age_ma IS NOT NULL";
    }

    if ($q->param("max_age") =~ /^\d+(\.\d+)?$/) {
        push @both_where, "b.sample_age_ma <= ".$q->param("max_age");
    } elsif ($q->param("max_age") !~ /^$/) {
        push @errors, "Maximum sample age must be a positive integer or decimal value";
    }


    my $use_coords = 0; 
    if ($q->param("latmin") =~ /^-?\d+(\.\d+)?$/ && $q->param("latmin") > -90) {
        $use_coords = 1;
        push @both_where, "a.latitude >= ".$q->param("latmin");
    }
    if ($q->param("latmax") =~ /^-?\d+(\.\d+)?$/ && $q->param("latmax") < 90) {
        $use_coords = 1;
        push @both_where, " a.latitude <= ".$q->param("latmax");
    }
    if ($q->param("lngmin") =~ /^-?\d+(\.\d+)?$/ && $q->param("lngmin") > -180) {
        $use_coords = 1;
        push @both_where, "a.longitude >= ".$q->param("lngmin");
    }
    if ($q->param("lngmax") =~ /^-?\d+(\.\d+)?$/ && $q->param("lngmax") < 180) {
        $use_coords = 1;
        push @both_where, "a.longitude <= ".$q->param("lngmax");
    }

    

    if ($q->param("min_samples_neptune") =~ /^\d+$/) {
        $sql .= " AND e.taxsyn_id IN (".
          "SELECT DISTINCT taxsyn_id".
          " FROM neptune_sample b, neptune_sample_taxa c,".
          " neptune_taxonomy_synonym e";
        if ($use_coords) {
            $sql .= ", neptune_hole_summary a";
        }
        $sql .= " WHERE b.sample_id = c.sample_id".
          " AND c.taxon_id = e.taxon_id";
        if ($use_coords) {
            $sql .= " AND a.hole_id=b.hole_id";
        }
        $sql .= " AND ".join(" AND ",@both_where);
        $sql .= " GROUP BY e.taxsyn_id".
          " HAVING COUNT(b.sample_age_ma) >= ".int($q->param("min_samples_neptune")).")";
    } elsif ($q->param("min_samples_neptune") !~ /^$/) {
        push @errors, "Minimum # samples per taxon must be a positive integer";
    }

    if ($q->param("taxon_name") =~ /[a-zA-Z]/) {
        my @taxa = parseTaxa($q->param("taxon_name"));
        my @clauses;
        foreach my $taxon (@taxa) {
            my ($genus,$species,$subspecies) = split(/ /,$taxon);
            my $taxa_clause = "f.genus ILIKE ".$dbh->quote($genus);
            $taxa_clause .= " AND f.species ILIKE ".$dbh->quote($species) if ($species);
            $taxa_clause .= " AND f.subspecies ILIKE ".$dbh->quote($species) if ($subspecies);
            $taxa_clause = "($taxa_clause)";
            push @clauses, $taxa_clause;
        }
        #$sql .= " AND e.taxsyn_id IN (SELECT DISTINCT b.taxsyn_id FROM neptune_taxonomy a, neptune_taxonomy_synonym b WHERE a.taxon_id=b.taxon_id AND ".join(" OR ",@clauses).")";
        #$sql = " AND f.taxsyn_id IN (SELECT DISTINCT a.taxon_id FROM neptune_taxonomy a, neptune_taxonomy_synonym b, neptune_taxonomy c WHERE a.taxon_id=b.taxon_id and b.taxsyn_id=c.taxon_id AND ".join(" OR ",@clauses);
        $sql .= " AND (".join(" OR ",@clauses).")";
    }

    # Handle taxon status checkboxes
    my @taxon_status= $q->param("taxon_status");
    my @status_codes = ();
    my $has_q = 0;
    foreach my $taxon_status (@taxon_status) {
        if ($taxon_status =~ /^[A-Z]$/) {
            if ($taxon_status =~ /Q/) {
                $has_q = 1;
            }
            push @status_codes, "'$taxon_status'";
        }
    }
    if (!@status_codes) {
        push @errors, "Please select at least one taxon status";
    }
    $sql .= " AND d.taxon_status IN (".join(",",@status_codes).")";
    if (!$has_q) {
        $sql .= " AND (f.subspecies IS NULL OR f.subspecies NOT ILIKE '%(Q)%')";
    }
    if ($q->param("sp") eq "NO") {
        $sql .= " AND f.species NOT ILIKE 'sp.%'";
    }


    $sql .= " AND ".join(" AND ",@both_where);
    $sql .= " ORDER BY e.taxsyn_id,b.sample_age_ma";
    print $sql if ($DEBUG);

    if (@errors) {
        print Debug::printErrors(\@errors);
        exit;
    }

    my $sth = $dbh->prepare($sql);
    $sth->execute() or
        die "Err with SQL ($sql): ".$sth->errstr;

    my @results;
    while (my $row = $sth->fetchrow_hashref()) {
#        $row->{'resolved_fossil_name'} =~ s/ \(q\)$//i;
#        $row->{'fossil_name'} =~ s/ \(q\)$//i;
        push @results,$row;
    }
    return \@results;
}


sub parseTaxa {
    my $text = shift;

    my @tokens = split /[\s,:;\/|]/,$text;
    my @taxa = ();
    my $taxon_name = "";

    foreach my $token (@tokens) {
        if ($token !~ /^\s+$/) {
            if ($token =~ /^[A-Z]/) {
                push @taxa, $taxon_name if ($taxon_name);
                $taxon_name = $token;
            } else {
                $taxon_name .= " $token";
            }
        }
    }
    push @taxa, $taxon_name if ($taxon_name);
    return @taxa;
}

# return a handle to the database (often called $dbh)
sub connect {
    my $driver =   "Pg";
    my $hostName = "cdb0.geol.iastate.edu";
    my $userName = "pbdbnep";
    my $dbName = "neptune";
    my $password = `cat /home/paleodbpasswd/neptune_passwd`;
    chomp($password);  #remove the newline!  Very important!

    # Make sure a symbolic link to this file always exists;
    #my $password = `cat /home/paleodbpasswd/passwd`;
    #chomp($password);  #remove the newline!  Very important!
    my $dsn = "DBI:$driver:database=$dbName;host=$hostName";

	my $dbh = DBI->connect($dsn, $userName, $password, {RaiseError=>1});	
    if (!$dbh) { die ($dbh->errstr); }
    return $dbh;
}
 
1;

