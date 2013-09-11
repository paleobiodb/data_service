# the following functions were moved into MeasurementEntry.pm by JA 4.6.13:
# submitSpecimenSearch, displaySpecimenList, populateMeasurementForm,
#  processMeasurementForm, syncWithAuthorities

package Measurement;
use TaxaCache;
use Ecology;
use Reference;
use Constants qw($READ_URL $HTML_DIR $TAXA_TREE_CACHE);

my @specimen_fields   =('specimens_measured','specimen_coverage','specimen_id','specimen_side','sex','specimen_part','measurement_source','magnification','is_type','comments');
my @measurement_types =('mass','length','width','height','circumference','diagonal','diameter','inflation','d13C','d18O');
my @measurement_fields=('average','median','min','max','error','error_unit');


# General purpose function for getting occurrences with data.  Pass in 2 arguments:
#   Argument 1 is $dbt object
#   Argument 2 is hash array of options
#   i.e. getSpecimens($dbt,'collection_no'=>1111,'taxon_name'=>'Calippus')
#   Possible values for options:
#      taxon_no: a taxon_no.  Will call getChildren on this no
#      taxon_name: Will find all taxon_nos this corresponds to, and combine the 
#          getChildren calls for all of them.  If a taxon_no is not found, then
#          search against the occurrences/reids table
#      taxon_list: an array ref of taxon_nos, like $in_list in TaxonInfo
#      collection_no: a collection_no
#      get_global_specimens: include measurements for which the occurrence is not known and
#          only the taxon_no is known. used in TaxonInfo and in limited cases in Download
#   Returns a straight array of what the DB results
sub getMeasurements	{
    my $dbt = $_[0];
    my $dbh = $dbt->dbh;
    my %options = %{$_[1]};

    my ($sql1,$sql2,$sql3,$where) = ("","","");
    my @fields = ('s.*','m.*');
    my @tables = ('specimens s','measurements m');
    my @where;

    if ( $options{'refs'} )	{
        push @fields , 'r.author1last,r.author2last,r.otherauthors,r.pubyr';
        push @tables , 'refs r';
        push @where , ('s.reference_no=r.reference_no');
    }

    if ( join('',@{$options{'lengths'}}) =~ /unknown/ )	{
        push @where , qq|(position IN ('|.join("','",@{$options{'lengths'}}).qq|') OR position IS NULL OR measurement_type!='length')|;
    } elsif ( $options{'lengths'} )	{
        push @where , qq|(position IN ('|.join("','",@{$options{'lengths'}}).qq|') OR measurement_type!='length')|;
    }
    if ( join('',@{$options{'widths'}}) =~ /unknown/ )	{
        push @where , qq|(position IN ('|.join("','",@{$options{'widths'}}).qq|') OR position IS NULL OR measurement_type!='width')|;
    } elsif ( $options{'widths'} )	{
        push @where , qq|(position IN ('|.join("','",@{$options{'widths'}}).qq|') OR measurement_type!='width')|;
    }

    my @part_list;
    if ( $options{'part_list'} ne "" && $options{'part_list'} ne "no" )	{
        if ( $options{'part_list'} =~ /[^A-Za-z0-9 ]/ )	{
          @part_list = split /[^A-Za-z0-9 ]/,$options{'part_list'};
        } else	{
          @part_list = split /[^A-Za-z0-9]/,$options{'part_list'};
        }
        s/^[ ]+// foreach @part_list;
        s/[ ]+$// foreach @part_list;
        push @where , "BINARY specimen_part IN ('".join("','",@part_list)."')";
    }
    if ( $options{'sex'} =~ /male|unknown/i )	{
        if ( $options{'sex'} eq "female only" )	{
            push @where , "sex='female'";
        } elsif ( $options{'sex'} eq "male only" )	{
            push @where , "sex='male'";
        } elsif ( $options{'sex'} eq "unknown only" )	{
            push @where , "(sex='' OR sex IS NULL OR sex='both')";
        } elsif ( $options{'sex'} eq "exclude females" )	{
            push @where , "(sex='male' OR sex='both' OR sex IS NULL)";
        } elsif ( $options{'sex'} eq "exclude males" )	{
            push @where , "(sex='female' OR sex='both' OR sex IS NULL)";
        }
    }

    $sql1 = "SELECT ".join(',',@fields).",o.taxon_no FROM ".join(', ',@tables).", occurrences o";
    $sql2 = "SELECT ".join(',',@fields).",re.taxon_no FROM ".join(', ',@tables).", occurrences o, reidentifications re";
    $sql3 = "SELECT ".join(',',@fields).",a.taxon_no FROM ".join(', ',@tables).", authorities a";

    $sql1 .= " LEFT JOIN reidentifications re ON re.occurrence_no=o.occurrence_no WHERE s.occurrence_no=o.occurrence_no AND s.specimen_no=m.specimen_no AND re.reid_no IS NULL";
    $sql2 .= " WHERE s.occurrence_no=o.occurrence_no AND s.specimen_no=m.specimen_no AND re.occurrence_no=o.occurrence_no AND re.most_recent='YES'";
    $sql3 .= " WHERE a.taxon_no=s.taxon_no AND s.specimen_no=m.specimen_no";

    if ( @where )	{
        $sql1 .= " AND ".join(' AND ',@where);
        $sql2 .= " AND ".join(' AND ',@where);
        $sql3 .= " AND ".join(' AND ',@where);
    }

    my $clause_found = 0;
    if ($options{'taxon_list'}) {
        my $taxon_nos = join(",",@{$options{'taxon_list'}});
        $sql1 .= " AND o.taxon_no IN ($taxon_nos)";
        $sql2 .= " AND re.taxon_no IN ($taxon_nos)";
        $sql3 .= " AND a.taxon_no IN ($taxon_nos)";
        $clause_found = 1;
    } elsif ($options{'taxon_name'} || $options{'taxon_no'}) {
        my @taxa;
        if ($options{'taxon_name'}) {
            @taxa = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$options{'taxon_name'}},['taxon_no']);
        } else {
            @taxa = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$options{'taxon_no'}},['taxon_no']);
        }
        if (@taxa) {
            my (@taxon_nos,%all_taxa);
            foreach (@taxa) {
                @taxon_nos = TaxaCache::getChildren($dbt,$_->{'taxon_no'});
                @all_taxa{@taxon_nos} = ();
            }
            @taxon_nos = keys %all_taxa;
            my $taxon_nos = join(",",@taxon_nos);
            $sql1 .= " AND o.taxon_no IN ($taxon_nos)";
            $sql2 .= " AND re.taxon_no IN ($taxon_nos)";
            $sql3 .= " AND a.taxon_no IN ($taxon_nos)";
        } elsif ($options{'taxon_name'}) {
            my @taxon_bits = split(/\s+/,$options{'taxon_name'});
            $sql1 .= " AND o.genus_name LIKE ".$dbh->quote($taxon_bits[0]);
            $sql2 .= " AND re.genus_name LIKE ".$dbh->quote($taxon_bits[0]);
            if (scalar(@taxon_bits) > 1) {
                $sql1 .= "AND o.species_name LIKE ".$dbh->quote($taxon_bits[1]);
                $sql2 .= "AND re.species_name LIKE ".$dbh->quote($taxon_bits[1]);
            }
        }
        $clause_found = 1;
    } elsif ($options{'collection_no'}) {
        $sql1 .= " AND o.collection_no=".int($options{'collection_no'});
        $sql2 .= " AND o.collection_no=".int($options{'collection_no'});
        $clause_found = 1;
    } elsif ($options{'occurrence_list'}) {
        $sql1 .= " AND o.occurrence_no IN (".join(",",@{$options{'occurrence_list'}}).")";
        $sql2 .= " AND o.occurrence_no IN (".join(",",@{$options{'occurrence_list'}}).")";
        $clause_found = 1;
    } elsif ($options{'occurrence_no'}) {
        $sql1 .= " AND o.occurrence_no=".int($options{'occurrence_no'});
        $sql2 .= " AND o.occurrence_no=".int($options{'occurrence_no'});
        $clause_found = 1;
    }

    if ($options{'get_global_specimens'} && $sql3 =~ /taxon_no IN/) {
        $sql = "($sql1) UNION ($sql2) UNION ($sql3)";
    } else {
        $sql = "($sql1) UNION ($sql2)";
    }

    if ($clause_found) {
        my @results = @{$dbt->getData($sql)};
        # good luck coding this in SQL JA 12.5.12
        if ( $options{'precision'} )	{
            my @precise;
            if ( $options{'precision'} =~ /^[0-9]+(|\.[0-9]+) mm$/ )	{
                my $min_digits = $options{'precision'};
                $min_digits =~ s/^([0-9]+)(|\.)(|[0-9]+)( mm)/$3/;
                $min_digits = length($min_digits);
                for my $r ( @results )	{
                    my $value = $r->{'average'};
                    $value =~ s/^[0-9]+//;
                    $value =~ s/^\.//;
                    if ( length($value) >= $min_digits )	{
                        push @precise , $r;
                    }
                }
            } else	{
                my $min_pct = $options{'precision'};
                $min_pct =~ s/[^0-9\.]//g;
                $min_pct /= 100;
                for my $r ( @results )	{
                    my $value = $r->{'average'};
                    $value =~ s/^[0-9]+//;
                    $value =~ s/^\.//;
                    if ( 1 / 10**length($value) / $r->{'average'} <= $min_pct )	{
                        push @precise , $r;
                    }
                }
            }
            @results = @precise;
        }
        return @results;
    } else {
        return ();
    }
}

# Pass in a joined specimen/measurement table, as returned by the getMeasurements function above.
# This will give back a triple hash of aggregate data, in the form:
# $table{part}{what is measured}{stat type} where stat_type can be : min,max,a_mean,average,median,error
#  and what is measured can be: width,length,height,diagonal,inflation and part can be leg or arm,etc
# By convention, the triple hash ref is called p_table (parts table) and the double sub-hash is m_table (measurements table)
# See TaxonInfo or Download for examples of this function being called. i.e.:
# @results = getMeasurements($dbt,{'collection_no'=>1234});
# $p_table = getMeasurementTable(\@results);
# $m_table = $p_table->{'leg'};
# $total_measured = $m_table->{'specimens_measured'}
# $average_width_leg = $m_table->{'width'}{'average'}
sub getMeasurementTable {
    my @measurements = @{$_[0]};

    my %p_table;
    my $sp_count = 0;
    my %types = ();
    my (%seen_specimens,%seen_ref,%seen_part_ref);
    my %unique_specimen_nos = ();

    # Do a simple reorganization of flat database data into triple indexed hash described above
    foreach my $row (@measurements) {
        if (!$seen_specimens{$row->{'specimen_no'}}) {
            $p_table{$row->{'specimen_part'}}{'specimens_measured'} += $row->{'specimens_measured'};
            $unique_specimen_nos{$row->{'specimen_part'}}++;
            $seen_specimens{$row->{'specimen_no'}} = 1;
            # references are passed back in two ways because they are grouped
            #  differently depending on how the data are displayed
            $seen_ref{Reference::formatShortRef($row)}++;
            $seen_part_ref{$row->{'specimen_part'}}{Reference::formatShortRef($row)}++;
        }
        # needed to credit data contributors JA 8.9.11
        $p_table{$row->{'specimen_part'}}{'authorizer '.$row->{'authorizer_no'}}++;
        $p_table{$row->{'specimen_part'}}{'enterer '.$row->{'enterer_no'}}++;
        $p_table{$row->{'specimen_part'}}{'part_refs'} = join(', ',sort keys %{$seen_part_ref{$row->{'specimen_part'}}});
        $types{$row->{'measurement_type'}}++;
        my $part_type;
        if (! exists $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}) {
            $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}} = {};
        } 
        $part_type = $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}};
        # note that "average" is the geometric mean
	$row->{'position'} = ( $row->{'position'} ne "" ) ? $row->{'position'} : "unknown";
        if ( ! $part_type->{'position'} )	{
            $part_type->{'position'} = $row->{'position'};
        } elsif ( $part_type->{'position'} !~ /\b$row->{'position'}\b/ )	{
            $part_type->{'position'} .= ", ".$row->{'position'};
        }
        $part_type->{'specimens_measured'} += $row->{'specimens_measured'};
        $part_type->{'average'} += $row->{'specimens_measured'} * log($row->{'real_average'});
        if ($row->{'specimens_measured'} == 1) {
            unless ($part_type->{'average_only'}) {
                if ( $part_type->{'min'} == 0  || ( $row->{'real_average'} > 0 && $row->{'real_average'} < $part_type->{'min'} ) )	{
                    $part_type->{'min'} = $row->{'real_average'};
                }
                if ( $row->{'real_average'} > $part_type->{'max'} )	{
                    $part_type->{'max'} = $row->{'real_average'};
                }
            }
        } else {
            if ( $part_type->{'min'} == 0 || ( $row->{'real_min'} > 0 && $row->{'real_min'} < $part_type->{'min'} ) )	{
                $part_type->{'min'} = $row->{'real_min'};
            }
            if ( $row->{'real_max'} > $part_type->{'max'} )	{
                $part_type->{'max'} = $row->{'real_max'};
            }
            if ($row->{'real_average'} =~ /\d/ && $row->{'real_min'} !~ /\d/ && $row->{'real_max'} !~ /\d/) {
                $part_type->{'average_only'} = 1;
            }
        }
        my $digits = $row->{'average'};
        if ( $digits =~ /\./ )	{
            $digits =~ s/^.*\.//;
        } else	{
            $digits = "";
        }
        if ( 1 / 10**length($digits) > $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'precision'} || ! $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'precision'} )	{
            $p_table{$row->{'specimen_part'}}{$row->{'measurement_type'}}{'precision'} = 1 / 10**length($digits);
        }
    }
    $p_table{'all_refs'} = join(', ',sort keys %seen_ref);

    for my $part ( keys %p_table )	{
        my %m_table = %{$p_table{$part}};
        foreach my $type (keys %types) {
            if ($m_table{$type}{'specimens_measured'}) {
                $m_table{$type}{'average'} = exp($m_table{$type}{'average'}/$m_table{$type}{'specimens_measured'});
                # if any averages were used in finding the min and max, the
                #  values are statistically bogus and should be erased
                # likewise if the sample size is 1
                if ( $m_table{$type}{'average_only'} == 1 || $m_table{$type}{'specimens_measured'} == 1 )	{
                    $m_table{$type}{'min'} = "";
                    $m_table{$type}{'max'} = "";
                }
            }
        }
   
        my @values = ();
        my $can_compute = 0; # Can compute median, and error (std dev)
        my $is_group = 0; # Is it aggregate group data or a bunch of singles?
        if ($unique_specimen_nos{$part} == 1) {
            if ($m_table{'specimens_measured'} > 1) {
                $can_compute = 1;
                $is_group = 1;
            }
        } elsif ($unique_specimen_nos{$part} >= 1 && $unique_specimen_nos{$part} == $m_table{'specimens_measured'}) {
            # This will only happen if the specimens_measured for each row is 1 above
            $can_compute = 1;
        }
    
        if ($can_compute) {
            my @measurements_for_part = ();
            foreach my $row (@measurements) {
                if ($row->{'specimen_part'} eq $part) {
                    push @measurements_for_part,$row;
                }
            }
            if ($is_group) {
                foreach my $row (@measurements_for_part) {
                    $m_table{$row->{'measurement_type'}}{'median'} = $row->{'real_median'};
                    $m_table{$row->{'measurement_type'}}{'error'} = $row->{'real_error'};
                    $m_table{$row->{'measurement_type'}}{'error_unit'} = $row->{'error_unit'};
                }
            } else {
                my %values_by_type;
                foreach my $row (@measurements_for_part) {
                    push @{$values_by_type{$row->{'measurement_type'}}},$row->{'real_average'};
                }
                while (my ($type,$values_array_ref) = each %values_by_type) {
                    @values = sort {$a <=> $b} @$values_array_ref;
                    if (@values) {
                        if (scalar(@values) % 2 == 0) {
                            my $middle_index = int(scalar(@values)/2);
                            my $median = ($values[$middle_index] + $values[$middle_index-1])/2;
                            $m_table{$type}{'median'} = $median;
                        } else {
                            my $middle_index = int(scalar(@values/2));
                            $m_table{$type}{'median'} = $values[$middle_index];
                        }
                    }
                    if (scalar(@values) > 1) {
                        $m_table{$type}{'error'} = std_dev(@values);
                        $m_table{$type}{'error_unit'} = "1 s.d.";
                    }
                }
            }
        }   
    }

    return \%p_table;
}

sub std_dev {
    my @set = @_;

    my $var = variance(@set);
    return ($var**(1/2));
}

sub variance {
    my @set = @_;

    my $mean = avg(@set);
    my $sum = 0;
    $sum += (($_ - $mean)**2) for @set;
    $sum = $sum/(scalar(@set)-1);
    return $sum;
}

sub avg {
    my @set = @_;
    my $sum = 0;
    $sum += $_ for @set;
    $sum = $sum/(scalar(@set));
    return $sum;
}

# JA 7.12.10
# stolen from TaxonInfo::displayMeasurements, but greatly simplified with double join on
#  taxa_tree_cache, also includes diameter and circumference, deals with multiple parent and/or
#  "minus" taxa in equations table, and also computes mean
sub getMassEstimates	{
	my ($dbt,$taxon_no,$tableref,$skip_area) = @_;
	my %p_table = %{$tableref};
	my %distinct_parts = ();

	for my $part ( keys %p_table )	{
		if ( $part !~ /^(p|m)(1|2|3|4)$/i )	{
			$distinct_parts{$part}++;
		}
	}
	my @part_list = keys %distinct_parts;
	@part_list = sort { $a cmp $b } @part_list;
	# mammal tooth measurements should always be listed in this fixed order
	unshift @part_list , ("P1","P2","P3","P4","M1","M2","M3","M4","p1","p2","p3","p4","m1","m2","m3","m4");

	# first get equations
	# join on taxa_tree_cache because we need to know which parents are
	#  the least inclusive
	# don't do this with a join on taxa_list_cache because that table
	#  is nightmarishly large
	# note that we are finding all equations including $taxon_no based either on
	#  taxon_no or minus_taxon_no, then finding the least inclusive of these groups
	#  (based on ORDER BY) and determining whether this group is a "minus" or not
	#  (based on SELECT FIND_IN_SET...)
	my $sql = "SELECT FIND_IN_SET(t2.taxon_no,e.minus_taxon_no) minus,taxon_name,t2.lft,e.reference_no,part,length,width,area,diameter,circumference,intercept FROM authorities a,equations e,refs r,$TAXA_TREE_CACHE t,$TAXA_TREE_CACHE t2 WHERE t.taxon_no=$taxon_no AND a.taxon_no=t2.taxon_no AND e.reference_no=r.reference_no AND t.lft>t2.lft AND t.rgt<t2.rgt AND (FIND_IN_SET(t2.taxon_no,e.taxon_no) OR FIND_IN_SET(t2.taxon_no,e.minus_taxon_no)) GROUP BY eqn_no ORDER BY t2.lft DESC,r.pubyr DESC";
	my @eqn_refs = @{$dbt->getData($sql)};

	my (@values,@masses,@eqns,@refs);
	my (%mean,%estimates);
	for my $part ( @part_list )	{
		my %m_table = %{$p_table{$part}};
		if ( ! %m_table )	{
			next;
		}
		foreach my $type (('length','width','area','diameter','circumference')) {
			if ( $type eq "area" && $m_table{length}{average} && $m_table{width}{average} && $part =~ /^[PMpm][1234]$/ && ! $skip_area )	{
				$m_table{area}{average} = $m_table{length}{average} * $m_table{width}{average};
			}
			if ( $m_table{$type}{'average'} > 0 ) {
				if ( $type ne "width" || ! $m_table{'length'}{'average'} || ! $m_table{'width'}{'average'} )	{
					my $value = $m_table{$type}{'average'};
					my $digits;
					if ( $value < 1 )	{
						$digits = "3f";
					} elsif ( $value < 10 )	{
						$digits = "2f";
					} else	{
						$digits = "1f";
					}
					$value = sprintf("%.$digits",$value);
					if ( $type ne "length" || ! $m_table{'width'}{'average'} )	{
						push @values , "$part $type $value";
					} else	{
						push @values , sprintf("$part $value x %.$digits",$m_table{'width'}{'average'});
					}
				}
				my $last_lft = "";
				foreach my $eqn ( @eqn_refs )	{
					if ( $part eq $eqn->{'part'} && $eqn->{$type} && ! $eqn->{'minus'} )	{
						if ( $eqn->{'lft'} < $last_lft && $last_lft )	{
							last;
						}
						$last_lft = $eqn->{'lft'};
						my $mass = exp( ( log($m_table{$type}{average}) * $eqn->{$type} ) + $eqn->{intercept} );
						$mean{$type.$part} += log($mass);
						$estimates{$type.$part}++;
						push @masses , $mass;
						push @eqns , "$eqn->{taxon_name} $part $type";
						push @refs , $eqn->{'reference_no'};
					}
				}
			}
		}
	}
	my ($grandmean,$grandestimates);
	for my $m ( keys %mean )	{
		$grandmean += $mean{$m} / $estimates{$m};
		$grandestimates++;
	}
	return (\@part_list,\@values,\@masses,\@eqns,\@refs,$grandmean,$grandestimates);
}
			
# JA 25-29.7.08
sub displayDownloadMeasurementsResults  {
	my ($q,$s,$dbt,$hbo) = @_;
	my $dbh = $dbt->dbh;

	if ( ! $q->param('taxon_name') ) 	{
		my $errorMessage = '<center><p class="medium"><i>You must enter the name of a taxonomic group.</i></p></center>';
		print PBDBUtil::printIntervalsJava($dbt,1);
		main::displayDownloadMeasurementsForm($errorMessage);
		return;
	}

	# who needs Text::CSV_XS? JA 18.6.12
	sub csv	{
		$_ = shift;
		$_ =~ s/^$/NA/;
		if ( $_ =~ /"/ )	{
			$_ =~ s/"/""/g;
		}
		if ( $_ =~ /[" ,]/ )	{
			$_ = '"'.$_.'"';
		}
		return $_;
	}

	my $sep;
	if ( $q->param('output_format') eq "csv" )	{
		$sep = ",";
	} else	{
		$sep = "\t";
	}

	# set up stuff needed to output collection data JA 18.6.12
	my @collection_fields = ('collection_no','collection_name','authorizer','enterer','country','state','county','latitude','longitude','paleolatitude','paleolongitude','period','epoch','stage','10_my_bin','formation','member','lithology','environment');

	if ( $q->param('coll_coord') )	{
		$q->param('coll_coord' => '');
		$q->param('coll_latitude' => 'YES');
		$q->param('coll_longitude' => 'YES');
	}
	if ( $q->param('coll_paleocoord') )	{
		$q->param('coll_paleocoord' => '');
		$q->param('coll_paleolatitude' => 'YES');
		$q->param('coll_paleolongitude' => 'YES');
	}

	my $names = $q->param('taxon_name');
	$names =~ s/[^A-Za-z ]//g;
	$names =~ s/  / /g;
	$names = join("','",split / /,$names);

	# if there are multiple matches, we hope to get the right one by
	#  assuming the larger taxon is the legitimate one
	my $sql = "SELECT t.taxon_no,lft,rgt,rgt-lft width FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND (taxon_name IN ('".$names."') OR common_name IN ('".$names."')) ORDER BY width DESC";
	my @parents = @{$dbt->getData($sql)};
	if ( ! @parents ) 	{
		my $errorMessage = '<center><p class="medium"><i>The taxon '.$q->param('taxon_name').' is not in our database. Please try another name.</i></p></center>';
		print PBDBUtil::printIntervalsJava($dbt,1);
		main::displayDownloadMeasurementsForm($errorMessage);
		return;
	}

	# same for "exclude" taxon JA 8.9.11
	# it doesn't matter if excluded taxa are extant
	my $exclude_clause;
	if ( $q->param('exclude') )	{
		$sql = "SELECT t.taxon_no,lft,rgt,rgt-lft width FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND (taxon_name='".$q->param('exclude')."' OR common_name ='".$q->param('exclude')."') ORDER BY width DESC LIMIT 1"; 
		my $exclude = ${$dbt->getData($sql)}[0];
		if ( ! $exclude ) 	{
			my $errorMessage = '<center><p class="medium"><i>The taxon '.$q->param('exclude').' is not in our database. Please try another name.</i></p></center>';
			print PBDBUtil::printIntervalsJava($dbt,1);
			main::displayDownloadMeasurementsForm($errorMessage);
			return;
		}
		$exclude_clause = "AND (lft<$exclude->{'lft'} OR rgt>$exclude->{'rgt'})";
	}

	my @fields = ('synonym_no','spelling_no','a.taxon_no','taxon_name','taxon_rank');
	if ( $q->param('authors') =~ /y/i )	{
		push @fields , "IF(ref_is_authority='YES',r.author1last,a.author1last) a1";
		push @fields , "IF(ref_is_authority='YES',r.author2last,a.author2last) a2";
		push @fields , "IF(ref_is_authority='YES',r.otherauthors,a.otherauthors) oa";
	}
	if ( $q->param('year') =~ /y/i )	{
		push @fields , "IF(ref_is_authority='YES',r.pubyr,a.pubyr) pubyr";
	}
	for my $f ( 'type_specimen','type_body_part','type_locality','extant')	{
		if ( $q->param($f) =~ /y/i )	{
			push @fields , $f;
		}
	}

	# now extract, average, and output the measurements as follows:
	#  (1) find species in the taxonomic group
	#  (2) get measurements of these species (getMeasurements)
	#  (3) save measurements of species found in a subset of collections
	#  (4) get collection data to be printed to the raw measurements file
	#  (5) compose output file headers
	#  (6) take averages (getMeasurementTable)
	#  (7) print data to raw measurements file
	#  (8) print data to the average measurements file
	#  (9) print data to the table file

	# step 1

	my $extant_clause;
	if ( $q->param('extant_extinct') =~ /extant/i )	{
		$extant_clause = " AND extant='yes'";
	} elsif ( $q->param('extant_extinct') =~ /extinct/i )	{
		$extant_clause = " AND (extant='no' OR extant IS NULL)";
	}

	# grabs all synonyms and spellings
	my @brackets;
	push @brackets, "(lft>=$_->{lft} AND rgt<=$_->{rgt})" foreach @parents;
	$sql = "SELECT ".join(',',@fields)." FROM authorities a,$TAXA_TREE_CACHE t WHERE taxon_rank IN ('species','subspecies') AND a.taxon_no=t.taxon_no AND (".join(" OR ",@brackets).") $exclude_clause $extant_clause ORDER BY taxon_name ASC";
	if ( $q->param('authors') =~ /y/i || $q->param('year') =~ /y/i )	{
		$sql = "SELECT ".join(',',@fields)." FROM authorities a,refs r,$TAXA_TREE_CACHE t WHERE taxon_rank IN ('species','subspecies') AND a.reference_no=r.reference_no AND a.taxon_no=t.taxon_no AND (".join(" OR ",@brackets).") $exclude_clause $extant_clause ORDER BY taxon_name ASC";
	}

	my @taxa = @{$dbt->getData($sql)};
	if ( ! @taxa ) 	{
		my $errorMessage = '<center><p class="medium"><i>We have no measurement data for species belonging to '.$q->param('taxon_name').'. Please try another taxon.</i></p></center>';
		print PBDBUtil::printIntervalsJava($dbt,1);
		main::displayDownloadMeasurementsForm($errorMessage);
		return;
	}

	# get the life habits so some categories can be excluded
	my $habits_checked;
	for my $p ( $q->param('life_habit') )	{
		$habits_checked++;
	}
	if ( $habits_checked < 9 )	{
		my %eco_lookup;
		for my $p ( @parents )	{
			my %temp_lookup = %{Ecology::fastEcologyLookup($dbt,'life_habit',$p->{lft},$p->{rgt})};
			$eco_lookup{$_} = $temp_lookup{$_} foreach keys %temp_lookup;
		}
		my (%use_habit,@with_habits);
		$use_habit{$_}++ foreach $q->param('life_habit');
		for my $t ( @taxa )	{
			if ( $use_habit{$eco_lookup{$t->{'taxon_no'}}} > 0 )	{
				push @with_habits , $t;
			}
		}
		if ( ! @with_habits ) 	{
			my $errorMessage = '<center><p class="medium"><i>We have no measurement data for species in the selected life habit categories. Adding more categories might help.</i></p></center>';
			print PBDBUtil::printIntervalsJava($dbt,1);
			main::displayDownloadMeasurementsForm($errorMessage);
			return;
		}
		@taxa = @with_habits;
	}

	# group synonyms for each valid species
	my %valid_no;
	if ( $q->param('replace_with_ss') =~ /y/i )	{
		$valid_no{$_->{taxon_no}} = $_->{synonym_no} foreach @taxa;
	# or else don't, but make sure only one spelling is used per synonym
	} else	{
		$valid_no{$_->{taxon_no}} = $_->{spelling_no} foreach @taxa;
	}

	# special handling for subspecies JA 1.5.12
	# this is a very minor issue only really relevant to Recent mammals
	my @subspp;
	for my $t ( @taxa )	{
		if ( $t->{'taxon_rank'} eq "subspecies" )	{
			push @subspp , $t->{'taxon_no'};
		}
	}
	if ( @subspp )	{
		$sql = "SELECT child_no,parent_no FROM authorities,taxa_list_cache WHERE taxon_no=parent_no AND child_no IN (".join(',',@subspp).") AND taxon_rank='species'";
		$valid_no{$_->{child_no}} = $_->{parent_no} foreach @{$dbt->getData($sql)};
	}

	my @taxon_list = keys %valid_no;

	# step 2: get measurements of the included species (getMeasurements)

	my %options;
	$options{'get_global_specimens'} = 1;
	if ( @taxon_list )	{
		$options{'taxon_list'} = \@taxon_list;
	}

	# recover desired position values from the length and width
	#  "measurements" checkboxes
	my @params = $q->param;
	my (@lengths,@widths);
	for my $p ( @params )	{
		if ( $p =~ /_length/ && $q->param('length') )	{
			my ($pos,$dim) = split /_/,$p;
			push @lengths , $pos;
		} elsif ( $p =~ /_width/ && $q->param('width') )	{
			my ($pos,$dim) = split /_/,$p;
			push @widths , $pos;
		}
	}
	if ( @lengths )	{
		$options{'lengths'} = \@lengths;
	}
	if ( @widths )	{
		$options{'widths'} = \@widths;
	}

	for my $p ( 'refs','part_list','precision' )	{
		if( $q->param($p) )	{
			$options{$p} = $q->param($p);
		}
	}
	if ( $q->param('sex') && $q->param('sex') ne "both" )	{
		$options{'sex'} = $q->param('sex');
	}

	my @measurements = getMeasurements($dbt,\%options);
	if ( ! @measurements ) 	{
		my $errorMessage = '<center><p class="medium"><i>We have data records for this taxon but your options exclude them. Try broadening your search criteria.</i></p></center>';
		print PBDBUtil::printIntervalsJava($dbt,1);
		main::displayDownloadMeasurementsForm($errorMessage);
		return;
	}

	# step 3: save measurements of species found in a subset of collections

	my ($resos,$collections,$countries,$continent_list,$continents_checked,$interval_nos,$strat_unit);
	my $download = Download->new($dbt,$q,$s,$hbo);
	# JA 18.6.12
	if ( $q->param('taxonomic_resolution') && $q->param('taxonomic_resolution') ne "all" )	{
		if ( $q->param('taxonomic_resolution') eq "identified to genus" )	{
			$resos = "species_name!='indet.' AND (genus_reso!='informal' OR genus_reso IS NULL)";
		} elsif ( $q->param('taxonomic_resolution') eq "certainly identified to genus" )	{
			$resos = "species_name!='indet.' AND (genus_reso NOT IN ('aff.','cf.','ex gr.','sensu lato','?','\"','informal') or genus_reso IS NULL)";
		} elsif ( $q->param('taxonomic_resolution') eq "identified to species" )	{
			$resos = "species_name NOT IN ('indet','sp.','spp.') AND (species_reso!='informal' OR species_reso IS NULL) AND (genus_reso NOT IN ('aff.','cf.','ex gr.','sensu lato','?','\"','informal') or genus_reso IS NULL)";
		} elsif ( $q->param('taxonomic_resolution') eq "certainly identified to species" )	{
			$resos = "species_name NOT IN ('indet','sp.','spp.') AND (species_reso NOT IN ('aff.','cf.','ex gr.','sensu lato','?','\"','informal') OR species_reso IS NULL) AND (genus_reso NOT IN ('aff.','cf.','ex gr.','sensu lato','?','\"','informal') or genus_reso IS NULL)";
		}
	}
	if ( $q->param('collection_names') =~ /^[A-Za-z0-9]/i )	{
		$collections = $q->param('collection_names');
		$collections =~ s/[^A-Za-z0-9 :;,\.\-\(\)\'"]//g;
		if ( $collections =~ /^[0-9 ,]+$/ )	{
			$collections =~ s/ /,/g;
			while ( $collections =~ /,,/ )	{
				$collections =~ s/,,/,/g;
			}
			$collections = "c.collection_no IN (".$collections.")";
		} else	{
			$collections =~ s/\'/\\\'/g;
			$collections = "(collection_name LIKE ('%".$collections."%') OR collection_aka LIKE ('%".$collections."%') )";
		}
	}
	my @continents = ( 'Africa','Antarctica','Asia','Australia','Europe','North America','South America' );
	for my $c ( @continents )	{
		if ( $q->param($c) !~ /y/i )	{
			$countries = $download->getCountryString();
			last;
		}
	}
	for my $c ( @continents )	{
		if ( $q->param($c) =~ /y/i )	{
			$continent_list .= ", ".$c;
			$continents_checked++;
		}
	}
	$continent_list =~ s/^, //;
	if ( $continents_checked == $#continents + 1 )	{
		($continent_list,$countries) = ("","");
	}
	if ( $q->param('max_interval') =~ /^[A-Z][a-z]/i )	{
		require TimeLookup;
		my $t = new TimeLookup($dbt);
	# eml_max and min aren't on the form yet
		my ($intervals,$errors,$warnings) = $t->getRange('',$q->param('max_interval'),'',$q->param('min_interval'));
		$interval_nos = join(',',@$intervals);
	}
	if ( $q->param('group_formation_member') =~ /^[A-Z]/i )	{
		$strat_unit = $q->param('group_formation_member');
		$strat_unit =~ s/\'/\\\'/g;
		$strat_unit = "(geological_group='".$strat_unit."' OR formation='".$strat_unit."' OR member='".$strat_unit."')";
	}
	# JA 18.6.12
	my $env_sql = $download->getEnvironmentString();
	my $pres_sql = $download->getPreservationModeString();

	my (%by_valid,%occ_used,%taxon_used);

	if ( $resos || $collections || $countries || $interval_nos || $strat_unit || $env_sql || $pres_sql )	{
	# it's actually faster to get the occurrences and reIDs separately
	#  from the measurements instead of doing a nightmare five-table
	#  join in getMeasurements
	# it's also faster get the reIDs and then hit occurrences with an out
	#  list instead of using a left join to get only the occurrences without
	#  reIDs, as done by getCollections
	# thank goodness we store collection_no in reidentifications, even
	#  though technically it's redundant
		my $sql1 = "SELECT occurrence_no,taxon_no FROM collections c,reidentifications re WHERE c.collection_no=re.collection_no AND taxon_no IN (".join(',',@taxon_list).")";
		my $sql2;
		if ( $interval_nos )	{
			$sql2 .= " AND max_interval_no IN (".$interval_nos.") AND min_interval_no IN (".$interval_nos.",0)";
		}
		$sql2 .= ( $resos ) ? " AND ".$resos : "";
		$sql2 .= ( $collections ) ? " AND ".$collections : "";
		$sql2 .= ( $countries ) ? " AND ".$countries : "";
		$sql2 .= ( $strat_unit ) ? " AND ".$strat_unit : "";
		$sql2 .= ( $env_sql ) ? " AND ".$env_sql : "";
		$sql2 .= ( $pres_sql ) ? " AND ".$pres_sql : "";
		my @with_occs = @{$dbt->getData($sql1.$sql2)};
		my $sql1 = "SELECT occurrence_no,taxon_no FROM collections c,occurrences o WHERE c.collection_no=o.collection_no AND taxon_no IN (".join(',',@taxon_list).")";
		my %temp;
		$temp{$_->{'occurrence_no'}}++ foreach @with_occs;
		if ( %temp )	{
			$sql1 .= "AND occurrence_no NOT IN (".join(',',keys %temp).")";
		}
		push @with_occs , @{$dbt->getData($sql1.$sql2)};
		if ( ! @with_occs )	{
			my $errorMessage = '<center><p class="medium"><i>None of the collections include data for '.$q->param('taxon_name').'. Please try another name or broaden your search criteria.</i></p></center>';
			print PBDBUtil::printIntervalsJava($dbt,1);
			main::displayDownloadMeasurementsForm($errorMessage);
			return;
		}
		my %avail;
		$avail{$_->{'taxon_no'}}++ foreach @with_occs;
		$occ_used{$_->{'occurrence_no'}}++ foreach @with_occs;
		undef @with_occs;
	# which measured species are sampled anywhere in this collection set?
		for my $m ( @measurements )	{
			if ( $avail{$m->{'taxon_no'}} )	{
				$taxon_used{$valid_no{$m->{'taxon_no'}}}++;
			}
		}
		undef %avail;
	# go through it again because many measurements are not tied to any
	#  collection at all
	# we end up with all measurements grouped by valid species name
		for my $m ( @measurements )	{
			if ( $taxon_used{$valid_no{$m->{'taxon_no'}}} )	{
				my $vn = $valid_no{$m->{'taxon_no'}};
				$vn = ( $valid_no{$vn} > 0 ) ? $valid_no{$vn} : $vn;
				$m->{'valid_no'} = $vn;
				push @{$by_valid{$vn}} , $m;
			}
		}
	} else	{
		for my $m ( @measurements )	{
			# simple fix for extremely rare chaining case (could
			#  be created by lumping of subspecies, not only by
			#  conventional synonymies)
			my $vn = $valid_no{$m->{'taxon_no'}};
			$vn = ( $valid_no{$vn} > 0 ) ? $valid_no{$vn} : $vn;
			# needed to print valid names to raw measurements file
			$m->{'valid_no'} = $vn;
			push @{$by_valid{$vn}} , $m;
			$occ_used{$m->{'occurrence_no'}} = ( $m->{'occurrence_no'} ) ? 1 : "";
			$taxon_used{$valid_no{$m->{'taxon_no'}}} = ( $m->{'taxon_no'} ) ? 1 : "";
		}
	}

	# create faux "data records" for length x width etc. JA 19.6.12 
	# grouping is by unique specimen identifier (taxon_no or occurrence_no)
	#  plus specimen_id, if one exists
	# we have to compute only one LxW or LxWxH per thing even though things
	#  may have multiple lengths or widths
	if ( $q->param('area') || $q->param('volume') )	{
		my (%lengths,%widths,%heights);
		for my $m ( @measurements )	{
			my $id = ( $m->{'occurrence_no'} > 0 ) ? $m->{'occurrence_no'} : $m->{'taxon_no'};
			$id .= ( $m->{'specimen_id'} ) ? $m->{'specimen_id'} : "";
			if ( $m->{'measurement_type'} eq "length" )	{
				push @{$lengths{$id}} , $m;
			} elsif ( $m->{'measurement_type'} eq "width" )	{
				push @{$widths{$id}} , $m;
			} elsif ( $m->{'measurement_type'} eq "height" )	{
				push @{$heights{$id}} , $m;
			}
		}
		for my $id ( keys %lengths )	{
			my $faux = { };
			$faux->{$_} = $lengths{$id}[0]->{$_} foreach ( 'taxon_no','valid_no','occurrence_no','specimen_id','specimen_part','specimens_measured','authorizer_no','enterer_no' );
			my ($sum,$meanW,$meanH);
			$sum += log($_->{'average'}) foreach @{$lengths{$id}};
			my $meanL = $sum / scalar(@{$lengths{$id}});
			if ( $widths{$id} )	{
				$sum = 0;
				$sum += log($_->{'average'}) foreach @{$widths{$id}};
				$meanW = $sum / scalar(@{$widths{$id}});
				if ( $q->param('area') )	{
					if ( $faux->{'specimens_measured'} < $widths{$id}[0]->{'specimens_measured'} )	{
						$faux->{'specimens_measured'} = $widths{$id}[0]->{'specimens_measured'};
					}
					$faux->{'measurement_no'} = "NA";
					$faux->{'measurement_type'} = "area";
					$faux->{'average'} = sprintf "%.1f",exp($meanL + $meanW);
					$faux->{'real_average'} = $faux->{'average'};
					push @{$by_valid{$faux->{'valid_no'}}} , $faux;
					push @measurements , $faux;
				}
			}
			if ( $widths{$id} && $heights{$id} && $q->param('volume') )	{
				$sum = 0;
				$sum += log($_->{'average'}) foreach @{$heights{$id}};
				$meanH = $sum / scalar(@{$heights{$id}});
				# don't try to recycle this, faux is a pointer
				my $faux = { };
				$faux->{$_} = $lengths{$id}[0]->{$_} foreach ( 'taxon_no','valid_no','occurrence_no','specimen_id','specimen_part','specimens_measured','authorizer_no','enterer_no' );
				if ( $faux->{'specimens_measured'} < $heights{$id}[0]->{'specimens_measured'} )	{
					$faux->{'specimens_measured'} = $heights{$id}[0]->{'specimens_measured'};
				}
				$faux->{'measurement_no'} = "NA";
				$faux->{'measurement_type'} = "volume";
				$faux->{'average'} = sprintf "%.1f",exp($meanL + $meanW + $meanH);
				$faux->{'real_average'} = $faux->{'average'};
				push @{$by_valid{$faux->{'valid_no'}}} , $faux;
				push @measurements , $faux;
			}
		}
	}

	# step 4: get collection data to be printed to the raw measurements file
	# JA 18.6.12

	my (@raw_collection_fields,%collection_data);
	for my $p ( @collection_fields )	{
		if ( $q->param('coll_'.$p) )	{
			my $field = $p;
			$field =~ s/part_//;
			if ( $field =~ /period|epoch|stage|10_my_bin/ )	{
				next;
			}
			if ( $field =~ /collection_no|authorizer|enterer/ )	{
				push @raw_collection_fields , "c.".$field;
			} elsif ( $field eq "latitude" )	{
				push @raw_collection_fields , "lat AS latitude";
			} elsif ( $field eq "longitude" )	{
				push @raw_collection_fields , "lng AS longitude";
			} elsif ( $field eq "paleolatitude" )	{
				push @raw_collection_fields , "paleolat AS paleolatitude";
			} elsif ( $field eq "paleolongitude" )	{
				push @raw_collection_fields , "paleolng AS paleolongitude";
			} elsif ( $field eq "lithology" )	{
				push @raw_collection_fields , "IF(lithology2 IS NOT NULL AND lithology2!='',CONCAT(lithology1,'/',lithology2),lithology1) AS lithology";
			} else	{
				push @raw_collection_fields , $field;
			}
		}
	}
	if ( $q->param('coll_period') || $q->param('coll_epoch') || $q->param('coll_stage') || $q->param('coll_10_my_bin') )	{
		push @raw_collection_fields , ('max_interval_no','min_interval_no');
	}
	delete $occ_used{''};
	delete $occ_used{0};
	delete $taxon_used{''};
	delete $taxon_used{0};
	if ( @raw_collection_fields && keys %occ_used )	{
		my @tables = ('collections c','occurrences o');
		my (%interval_names,%lookups);
		if ( $q->param('coll_period') || $q->param('coll_epoch') || $q->param('coll_stage') || $q->param('coll_10_my_bin') )	{
			%interval_names = map { ( $_->{'interval_no'} , $_->{'interval_name'} ) } @{$dbt->getData("SELECT interval_no,IF(eml_interval!='' AND eml_interval IS NOT NULL,CONCAT(eml_interval,' ',interval_name),interval_name) AS interval_name FROM intervals")};
			%lookups = map { ( $_->{'interval_no'} , $_ ) } @{$dbt->getData("SELECT interval_no,period_no,epoch_no,stage_no,ten_my_bin FROM interval_lookup")};
		}
		$sql = "SELECT occurrence_no,".join(',',@raw_collection_fields)." FROM ".join(',',@tables)." WHERE c.collection_no=o.collection_no AND occurrence_no IN (".join(',',keys %occ_used).")";
		$collection_data{$_->{'occurrence_no'}} = $_ foreach @{$dbt->getData($sql)};
		for my $no ( keys %collection_data )	{
			my $max = $collection_data{$no}->{'max_interval_no'};
			my $min = $collection_data{$no}->{'min_interval_no'};
			$collection_data{$no}->{'period'} = $interval_names{$lookups{$max}->{period_no}};
			$collection_data{$no}->{'epoch'} = $interval_names{$lookups{$max}->{epoch_no}};
			$collection_data{$no}->{'stage'} = $interval_names{$lookups{$max}->{stage_no}};
			$collection_data{$no}->{'10_my_bin'} = $lookups{$max}->{ten_my_bin};
			if ( $min > 0 )	{
				$collection_data{$no}->{'period'} .= ( $collection_data{$no}->{'period'} ne $interval_names{$lookups{$min}->{period_no}} ) ? "/".$interval_names{$lookups{$min}->{period_no}} : "";
				$collection_data{$no}->{'epoch'} .= ( $collection_data{$no}->{'epoch'} ne $interval_names{$lookups{$min}->{epoch_no}} ) ? "/".$interval_names{$lookups{$min}->{epoch_no}} : "";
				$collection_data{$no}->{'stage'} .= ( $collection_data{$no}->{'stage'} ne $interval_names{$lookups{$min}->{stage_no}} ) ? "/".$interval_names{$lookups{$min}->{stage_no}} : "";
				$collection_data{$no}->{'10_my_bin'} = ( $collection_data{$no}->{'10_my_bin'} ne $lookups{$min}->{ten_my_bin} ) ? "" : $collection_data{$no}->{'10_my_bin'};
			}
		}
	}

	# step 5: compose output file headers
	# rewritten to deal with raw_header_fields JA 18.6.12

	my @header_fields = ('species');
	my @columns = ('average');
	my %authors;
	for my $param ( 'order','family' )	{
		if ( $q->param($param) )	{
			push @header_fields , $param;
		}
	}
	if ( $q->param('authors') =~ /y/i )	{
		for my $t ( @taxa )	{
			$t->{'a1'} =~ s/,.*//;
			$t->{'a2'} =~ s/,.*//;
			if ( $t->{'oa'} ) { $t->{a2} = " et al."; }
			else { $t->{'a2'} =~ s/^([A-Za-z])/ and $1/; }
			$authors{$t->{'taxon_no'}} = $t->{'a1'}.$t->{'a2'};
		}
		push @header_fields , "authors";
	}
	my %year;
	if ( $q->param('year') =~ /y/i )	{
		$year{$_->{'taxon_no'}} = $_->{'pubyr'} foreach @taxa;
		push @header_fields , "year published";
	}
	my %type;
	if ( $q->param('type_specimen') =~ /y/i )	{
		$type{$_->{'taxon_no'}} = $_->{'type_specimen'} foreach @taxa;
		push @header_fields , "type specimen";
	}
	my %type_part;
	if ( $q->param('type_body_part') =~ /y/i )	{
		$type_part{$_->{'taxon_no'}} = $_->{'type_body_part'} foreach @taxa;
		push @header_fields , "type body part";
	}
	my %locality;
	if ( $q->param('type_locality') =~ /y/i )	{
		$locality{$_->{'taxon_no'}} = $_->{'type_locality'} foreach @taxa;
		push @header_fields , "type locality number";
	}
	my %extant;
	if ( $q->param('extant') =~ /y/i )	{
		$extant{$_->{'taxon_no'}} = $_->{'extant'} foreach @taxa;
		push @header_fields , "extant";
	}

	# all the fields in the raw and averaged files are identical
	#  up to this point
	my @raw_header_fields = ('measurement_no',@header_fields);
	# collection attributes are only printed to the raw data file
	for my $p ( @collection_fields )	{
		if ( $q->param('coll_'.$p) )	{
			push @raw_header_fields , $p;
			if ( $q->param('authorizer') || $q->param('enterer') )	{
				$raw_header_fields[$#raw_header_fields] =~ s/(authorizer|enterer)/collection.$1/;
			}
		}
	}
	if ( $q->param('specimen_id') =~ /y/i )	{
		push @raw_header_fields , "specimen ID";
        }

	my @stat_fields;
	push @stat_fields , 'part';
	if ( $q->param('position') =~ /y/i )	{
		push @stat_fields , "position";
        }
	push @stat_fields, 'measurement';
	if ( $q->param('specimens_measured') =~ /y/i )	{
		push @stat_fields , "specimens measured";
	}
	push @stat_fields , "mean";
	for my $c ('min','max','median','error')	{
		if ( $q->param($c) =~ /y/i )	{
			push @stat_fields , $c;
			push @columns , $c;
		}
	}
	if ( $q->param('error') =~ /y/i )	{
		push @stat_fields , "error unit";
		push @columns , "error_unit";
	}

	my @meta_fields;
	for my $c ( 'authorizer','enterer' )	{
		if ( $q->param($c) =~ /y/i && ( $q->param('coll_authorizer') || $q->param('coll_enterer') ) )	{
			push @meta_fields , "measurement.".$c;
		} elsif ( $q->param($c) )	{
			push @meta_fields , $c;
		}
	}
	if ( $q->param('refs') =~ /y/i )	{
		push @meta_fields , "references";
	}
	push @header_fields , @stat_fields;
	push @raw_header_fields , @stat_fields;
	# figure out if any of the measurements have been rescaled using
	#  the magnification field JA 3.12.12
	for my $m ( @measurements )	{
		if ( $m->{'magnification'} != 1 && $m->{'magnification'} > 0 )	{
			push @raw_header_fields , 'magnification';
			last;
		}
	}
	push @header_fields , @meta_fields;
	push @raw_header_fields , @meta_fields;

	# step 6: take averages (getMeasurementTable)

	my %tables;
	my @with_data;
	foreach my $t ( @taxa )	{
		if ( ( $q->param('replace_with_ss') =~ /y/i && $t->{taxon_no} == $t->{synonym_no} ) || ( $q->param('replace_with_ss') !~ /y/i && $t->{taxon_no} == $t->{spelling_no} ) )	{
			my $vn = $valid_no{$t->{taxon_no}};
			if ( ! $by_valid{$vn} || $tables{$vn} )	{
				next;
			}
			my $p_table = getMeasurementTable(\@{$by_valid{$vn}});
			$tables{$vn} = $p_table;
			push @with_data , $vn;
		}
	}

	my %name;
	$name{$_->{'taxon_no'}} = $_->{'taxon_name'} foreach @taxa;
	@with_data = sort { $name{$a} cmp $name{$b} } @with_data;

	# this is pretty slow, but it's simple and reliable JA 30.5.12
	my (%order,%family);
	if ( $q->param('order') )	{
		$sql = "SELECT taxon_name,t1.taxon_no FROM $TAXA_TREE_CACHE t1,$TAXA_TREE_CACHE t2,authorities a WHERE t1.taxon_no IN (".join(',',@with_data).") AND t2.lft<t1.lft AND t2.rgt>t1.rgt AND a.taxon_no=t2.taxon_no AND taxon_rank='order' AND t2.taxon_no=t2.synonym_no";
		$order{$_->{taxon_no}} = $_->{taxon_name} foreach @{$dbt->getData($sql)};
	}
	if ( $q->param('family') )	{
		$sql = "SELECT taxon_name,t1.taxon_no FROM $TAXA_TREE_CACHE t1,$TAXA_TREE_CACHE t2,authorities a WHERE t1.taxon_no IN (".join(',',@with_data).") AND t2.lft<t1.lft AND t2.rgt>t1.rgt AND a.taxon_no=t2.taxon_no AND taxon_rank='family' AND t2.taxon_no=t2.synonym_no";
		$family{$_->{taxon_no}} = $_->{taxon_name} foreach @{$dbt->getData($sql)};
	}

	# much of this section is lifted from TaxonInfo::displayMeasurements
	# however, rewriting it would be a pain because that version focuses on
	#  creating an HTML table for exactly one taxon at a time
	my %records;
	my %specimens;
	my $rows;
	my @part_list;
	my %distinct_parts = ();
	for my $taxon_no ( @with_data )	{
		my %p_table = %{$tables{$taxon_no}};
		for my $part ( keys %p_table )	{
			if ( $part !~ /^(p|m)(1|2|3|4)$/i )	{
				$distinct_parts{$part}++;
			}
		}
	}

#FOO
#	if ( $q->param('part_list') )	{
#		@part_list = split /[^A-Za-z0-9 ]/,$q->param('part_list');
#		s/^[ ]+// foreach @part_list;
#		s/[ ]+$// foreach @part_list;
#	} else	{
		@part_list = keys %distinct_parts;
		@part_list = sort { $a cmp $b } @part_list;
#	if ( ! $q->param('part_list') )	{
		unshift @part_list , ("P1","P2","P3","P4","M1","M2","M3","M4","p1","p2","p3","p4","m1","m2","m3","m4");
#}
#	}
#print join(' ',@part_list);

	my $types;
	for my $type ( @measurement_types,'area','volume' )	{
		if ( $q->param($type) =~ /y/i )	{
			$types++;
		}
	}
	my (%measured_parts,%measured_types);
	for my $taxon_no ( @with_data )	{
		my $measured_parts = 0;
		my %p_table = %{$tables{$taxon_no}};
		for my $part ( @part_list )	{
			my %m_table = %{$p_table{$part}};
			if ( %m_table )	{
				for my $type ( @measurement_types,'area','volume' )	{
					if ( $m_table{$type} && $q->param($type) =~ /y/i && $m_table{$type}{'average'} > 0 )	{
						$measured_parts{$taxon_no}++;
						$measured_types{$taxon_no}{$part}++;
					}
				}
			}
		}
	}

	# needed to print credit lines
	my %person;
	my $sql = "SELECT name,person_no FROM person";
	$person{$_->{'person_no'}} = $_->{'name'} foreach @{$dbt->getData($sql)};

	# step 7: print data to raw measurements file JA 18.6.12

	my $OUT_HTTP_DIR = "/public/downloads";
	my $OUT_FILE_DIR = $HTML_DIR.$OUT_HTTP_DIR;
	my $person = ($s->get("enterer")) ? $s->get("enterer") : $q->param("yourname");
	my $outfile = PBDBUtil::getFilename($person)."_raw_measurements.txt";
	my $outfile2 = PBDBUtil::getFilename($person)."_average_measurements.txt";
	my $outfile3 = PBDBUtil::getFilename($person)."_measurement_table.txt";


	@measurements = sort { $name{$a->{'valid_no'}} cmp $name{$b->{'valid_no'}} } @measurements;
	open OUT,">$OUT_FILE_DIR/$outfile";
	$_ = csv($_) foreach @raw_header_fields;
	my $header = join($sep,@raw_header_fields);
	print OUT $header,"\n";
	for my $m ( @measurements )	{
		if ( ! ( $occ_used{$m->{'occurrence_no'}} || $taxon_used{$valid_no{$m->{'taxon_no'}}} ) )	{
			next;
		}
		print OUT $m->{'measurement_no'},$sep,csv($name{$m->{'valid_no'}});
		if ( $q->param('order') ) { print OUT $sep,csv($order{$m->{'valid_no'}}); }
		if ( $q->param('family') ) { print OUT $sep,csv($family{$m->{'valid_no'}}); }
		if ( $q->param('authors') ) { print OUT $sep,csv($authors{$m->{'valid_no'}}); }
		if ( $q->param('year') ) { print OUT $sep,csv($year{$m->{'valid_no'}}); }
		if ( $q->param('type_specimen') ) { print OUT $sep,csv($type{$m->{'valid_no'}}); }
		if ( $q->param('type_body_part') ) { print OUT $sep,csv($type_part{$m->{'valid_no'}}); }
		if ( $q->param('type_locality') ) { print OUT $sep,csv($locality{$m->{'valid_no'}}); }
		if ( $q->param('extant') ) { print OUT $sep,csv($extant{$m->{'valid_no'}}); }
		for my $field ( @collection_fields )	{
			if ( $q->param('coll_'.$field) && $field =~ /latitude|longitude/ )	{
				print OUT $sep,csv(sprintf("%.1f",$collection_data{$m->{'occurrence_no'}}->{$field}));
			} elsif ( $q->param('coll_'.$field) )	{
				print OUT $sep,csv($collection_data{$m->{'occurrence_no'}}->{$field});
			}
		}
		if ( $q->param('specimen_id') ) { print OUT $sep,csv($m->{'specimen_id'}); }
		print OUT $sep,csv($m->{'specimen_part'});
		if ( $q->param('position') ) { print OUT $sep,csv($m->{'position'}); }
		print OUT $sep,csv($m->{'measurement_type'});
		if ( $q->param('specimens_measured') ) { print OUT $sep,csv($m->{'specimens_measured'}); }
		print OUT $sep,csv($m->{'average'});
		for my $stat ( 'median','min','max','error' )	{
			if ( $q->param($stat) )	{
				print OUT $sep,csv($m->{$stat});
			}
		}
		if ( $q->param('error') ) { print OUT $sep,csv($m->{'error_unit'}); }
		if ( join('',@raw_header_fields) =~ /magnif/ )	{
			my $magnif = ( $m->{'magnification'} > 0 ) ? $m->{'magnification'} : 1;
			print OUT $sep,$magnif;
		}
		print OUT "\n";
	}
	close OUT;

	# step 8: print data to the average measurements file

	open OUT,">$OUT_FILE_DIR/$outfile2";
	$_ = csv($_) foreach @header_fields;
	my $header = join($sep,@header_fields);
	print OUT $header,"\n";

	my (%printed_parts,%total_authorized,%total_entered,%hasType,%matrix);
	for my $taxon_no ( @with_data )	{
		if ( $q->param('all_parts') =~ /y/i && $measured_parts{$taxon_no} < ( $#part_list + 1 ) * $types )	{
			next;
		}
		my %p_table = %{$tables{$taxon_no}};
		for my $part ( @part_list )	{
			my (%authorized,%entered);
			for my $k ( keys %{$p_table{$part}} )	{
				if ( $k =~ /^auth/ )	{
					my $n = $k;
					$n =~ s/[^0-9]//g;
					$authorized{$person{$n}}++;
					$total_authorized{$person{$n}}++;
				} elsif ( $k =~ /^enter/ )	{
					my $n = $k;
					$n =~ s/[^0-9]//g;
					$entered{$person{$n}}++;
					$total_entered{$person{$n}}++;
				}
			}
			my %m_table = %{$p_table{$part}};
			if ( %m_table )	{
				$printed_part = $part;
				if ( $part eq "" )	{
					$printed_part = "unknown";
				}
				my $part_used;
				for my $type ( @measurement_types,'area','volume' )	{
					if ( $m_table{$type} && $q->param($type) =~ /y/i && $m_table{$type}{'average'} > 0 )	{
						$part_used++;
						$printed_parts{$taxon_no}++;
						print OUT csv($name{$taxon_no});
						if ( $q->param('order') )	{
							print OUT $sep,csv($order{$taxon_no});
						}
						if ( $q->param('family') )	{
							print OUT $sep,csv($family{$taxon_no});
						}
						if ( $q->param('authors') =~ /y/i )	{
							print OUT $sep,csv($authors{$taxon_no});
						}
						if ( $q->param('year') =~ /y/i )	{
							print OUT $sep,csv($year{$taxon_no});
						}
						if ( $q->param('type_specimen') =~ /y/i )	{
							print OUT $sep,csv($type{$taxon_no});
						}
						if ( $q->param('type_body_part') =~ /y/i )	{
							print OUT $sep,csv($type_part{$taxon_no});
						}
						if ( $q->param('type_locality') =~ /y/i )	{
							print OUT $sep,csv($locality{$taxon_no});
						}
						if ( $q->param('extant') =~ /y/i )	{
							print OUT $sep,csv($extant{$taxon_no});
						}
						if ( $q->param('position') =~ /y/i )	{
							print OUT $sep,csv($m_table{$type}{'position'});
						}
						print OUT $sep,$printed_part,$sep,$type;
						if ( $q->param('specimens_measured') =~ /y/i )	{
							print OUT $sep,$m_table{'specimens_measured'};
						}
						foreach my $column ( @columns )	{
							my $value = $m_table{$type}{$column};
							if ( $column eq "error_unit" )	{
							} elsif ( $m_table{$type}{'precision'} <= 0 )	{
								$value = "NaN";
							} elsif ( $m_table{$type}{'precision'} <= 0.001 )	{
								$value = sprintf "%.3f",$value;
							} elsif ( $m_table{$type}{'precision'} == 0.01 )	{
								$value = sprintf "%.2f",$value;
							} elsif ( $m_table{$type}{'precision'} == 0.1 )	{
								$value = sprintf "%.1f",$value;
							} elsif ( $value > 0 )	{
								$value = sprintf "%d",$value;
							}
							print OUT $sep,csv($value);
							if ( $column eq "average" && $value > 0 )	{
								$hasType{$type." ".$part}++;
								$matrix{$taxon_no}{$type}{$part} = $value;
							}
						}
						if ( $q->param('authorizer') =~ /y/i )	{
							my @names = keys %authorized;
							@names = sort @names;
							print OUT $sep,csv(join(', ',@names));
						}
						if ( $q->param('enterer') =~ /y/i )	{
							my @names = keys %entered;
							@names = sort @names;
							print OUT $sep,csv(join(', ',@names));
						}
						if ( $q->param('refs') =~ /y/i )	{
							print OUT $sep,csv($m_table{'part_refs'});
						}
						print OUT "\n";
					}
				}
				if ( $part_used )	{
					$records{$part}++;
					$specimens{$part} += $m_table{'specimens_measured'};
					$rows++;
				}
			}
		}
	}
	close OUT;

	# step 9: finally, print data to the table file

	open OUT,">$OUT_FILE_DIR/$outfile3";
	print OUT "species";
	for my $param ( 'order','family' )	{
		if ( $q->param($param) )	{
			print OUT "\t$param";
		}
	}
	for my $type ( @measurement_types,'area','volume' )	{
		for my $part ( @part_list )	{
			if ( $hasType{$type." ".$part} > 0 )	{
				my $column = $part." ".$type;
				$column =~ s/mass/mass (g)/;
				print OUT "\t",csv($column);
			}
		}
	}
	if ( $q->param('refs') =~ /y/i )	{
		print OUT "\treferences";
	}
	print OUT "\n";
	@with_data = keys %matrix;
	for my $taxon_no ( sort { $name{$a} cmp $name{$b} } @with_data )	{
		print OUT "$name{$taxon_no}";
		if ( $q->param('order') )	{
			print OUT "\t$order{$taxon_no}";
		}
		if ( $q->param('family') )	{
			print OUT "\t$family{$taxon_no}";
		}
		for my $type ( @measurement_types,'area','volume' )	{
			for my $part ( @part_list )	{
				if ( $hasType{$type." ".$part} > 0 )	{
					my $value = ( $matrix{$taxon_no}{$type}{$part} ) ? $matrix{$taxon_no}{$type}{$part} : "NA";
					print OUT "\t$value";
				}
			}
		}
		if ( $q->param('refs') =~ /y/i )	{
			my %p_table = %{$tables{$taxon_no}};
			print OUT "\t",csv($p_table{'all_refs'});
		}
		print OUT "\n";
	}
	close OUT;

	if ( $rows < 1 )	{
		my $errorMessage = '<center><p class="medium"><i>None of the collections include data for '.$q->param('taxon_name').'. Please try another name or broaden your search criteria.</i></p></center>';
		print PBDBUtil::printIntervalsJava($dbt,1);
		main::displayDownloadMeasurementsForm($errorMessage);
		return;
	}
	print "<div style=\"margin-left: 10em; margin-bottom: 5em; width: 35em;\">\n\n";
	print "<p class=\"pageTitle\" style=\"margin-left: 8em;\">Download results</p>\n";
	print "<p class=\"darkList\" style=\"width: 30em; padding: 0.1em; padding-left: 3em;\">Summary</p>\n";
	print "<div style=\"margin-left: 3em;\">\n\n";
	print "<p style=\"width: 26em; margin-left: 1em; text-indent: -1em;\">Search: taxon = ",$q->param('taxon_name');
	if ( $q->param('collection_names') )	{
		print "; collection = ",$q->param('collection_names');
	}
	if ( $countries )	{
		if ( $continent_list =~ /, / )	{
			print "; continents = ",$continent_list;
		} else	{
			print "; continent = ",$continent_list;
		}
	}
	if ( $q->param('max_interval') )	{
		print "; interval = ",$q->param('max_interval');
	}
	if ( $q->param('min_interval') )	{
		print " to ",$q->param('min_interval');
	}
	if ( $q->param('group_formation_member') )	{
		print "; strat unit = ",$q->param('group_formation_member');
	}
	print "</p>\n";
	my @temp = keys %records;
	if ( $#temp == 0 )	{
		printf "<p>%d kind of body part</p>\n",$#temp+1;
	} else	{
		printf "<p>%d kinds of body parts</p>\n",$#temp+1;
	}
	my @temp = keys %printed_parts;
	printf "<p>%d species</p>\n",$#temp+1;
	if ( $rows == 1 )	{
		print "<p>$rows data record</p>\n";
	} else	{
		print "<p>$rows data records</p>\n";
	}
	print qq|<div>Output data files:
	<div style="margin-top: 0.5em; margin-left: 1em;"><a href="$OUT_HTTP_DIR/$outfile">$outfile</a><br>
	<a href="$OUT_HTTP_DIR/$outfile2">$outfile2</a><br>
	<a href="$OUT_HTTP_DIR/$outfile3">$outfile3</a>
	</div>
	</div>
|;

	print "<p style=\"margin-left: 1em; text-indent: -1em;\">Authorizers: ";
	my (@names,@bits) = (keys %total_authorized,());
	@names = sort { $total_authorized{$b} <=> $total_authorized{$a} } @names;
	push @bits , "$_ ($total_authorized{$_}&nbsp;records)" foreach @names;
	$_ =~ s/\(1&nbsp;records\)/(1&nbsp;record)/ foreach @bits;
	$_ =~ s/([A-Z]\.) /$1&nbsp;/ foreach @bits;
	print join(', ',@bits);
	print "</p>\n";

	print "<p style=\"margin-left: 1em; text-indent: -1em;\">Enterers: ";
	(@names,@bits) = (keys %total_entered,());
	@names = sort { $total_entered{$b} <=> $total_entered{$a} } @names;
	push @bits , "$_ ($total_entered{$_}&nbsp;records)" foreach @names;
	$_ =~ s/\(1&nbsp;records\)/(1&nbsp;record)/ foreach @bits;
	$_ =~ s/([A-Z]\.) /$1&nbsp;/ foreach @bits;
	print join(', ',@bits);
	print "</p>\n";
	print "</p>\n";

	print "</div>\n\n";
	print "<p class=\"darkList\" style=\"width: 30em; margin-top: 3em; padding: 0.1em; padding-left: 3em;\">Data totals for each body part</p>\n";
	print "<table cellpadding=\"4\" style=\"margin-left: 6em;\">\n";
	print "<tr><td align=\"center\">part</td><td>species</td><td>specimens</td></tr>\n";
	for my $part ( @part_list )	{
		if ( $records{$part} )	{
			my $printed_part = $part;
			if ( $part eq "" )	{
				$printed_part = "unknown";
			}
			print "<tr><td style=\"padding-left: 1em;\">$printed_part</td> <td align=\"center\">$records{$part}</td> <td align=\"center\">$specimens{$part}</td></tr>\n";
		}
	}
	print "</table>\n";
	print "</div>\n";

}   


1;
