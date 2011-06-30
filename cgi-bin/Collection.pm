package Collection;
use strict;

use HTMLBuilder;
use PBDBUtil;
use Validation;
use Taxon;
use TaxonInfo;
use TimeLookup;
use TaxaCache;
use Person;
use Permissions;
use Class::Date qw(now date);
use Debug qw(dbg);
use URI::Escape;
use Debug;
use Map;    
use Constants qw($READ_URL $WRITE_URL $HTML_DIR $HOST_URL $TAXA_TREE_CACHE $DB $COLLECTIONS $COLLECTION_NO $OCCURRENCES $OCCURRENCE_NO $PAGE_TOP $PAGE_BOTTOM);

# This function has been generalized to use by a number of different modules
# as a generic way of getting back collection results, including maps, collection search, confidence, and taxon_info
# These are simple options, corresponding to database fields, that can be passed in:
# These are more complicated options that can be passed in:
#   taxon_list: A list of taxon_nos to filter by (i.e. as passed by TaxonInfo)
#   include_old_ids: default behavior is to only match a taxon_name/list against the most recent reid. if this flag
#       is set to 1, then also match taxon_name against origianal id and old ids
#   include_occurrences: normally if we have an authority match, only match based off that. if this flag is set,
#       we'll also just do a straight text match of the occurrences table
#   no_authority_lookup: Don't hit the authorities table when lookup up a taxon name , only the occurrences/reids tables
#   calling_script: Name of the script which called this function, only used for error message generation
# This function will die on error, so call it in an eval loop
# PS 08/11/2005
sub getCollections {
	my $dbt = $_[0];
	my $s = $_[1];
	my $dbh = $dbt->dbh;
	my %options = %{$_[2]};
	my @fields = @{$_[3]};

	# Set up initial values
	my (@where,@occ_where,@reid_where,@tables,@from,@left_joins,@groupby,@having,@errors,@warnings);
	@tables = ("collections c");
	# There fields must always be here
	@from = ("c.authorizer_no","c.collection_no","c.collection_name","c.access_level","c.release_date","c.reference_no","DATE_FORMAT(release_date, '%Y%m%d') rd_short","c.research_group");
    

	if ( $DB eq "eco" )	{
		@tables = ("inventories c");
		@from = ("c.inventory_no","c.inventory_name","c.reference_no");
	}


    # Now add on any requested fields
    foreach my $field (@fields) {
        if ($field eq 'enterer') {
            push @from, "c.enterer_no"; 
        } elsif ($field eq 'modifier') {
            push @from, "c.modifier_no"; 
        } else {
            push @from, "c.$field";
        }
    }


	# 9.4.08
	if ( $options{'field_name'} =~ /[a-z]/ && $options{'field_includes'} =~ /[A-Za-z0-9]/ )	{
		$options{$options{'field_name'}} = $options{'field_includes'};
	}


    # the next two are mutually exclusive
    if ($options{'count_occurrences'} || $options{'sortby'} eq 'occurrences')	{
        push @from, "taxon_no,count(*) AS c";
        push @tables, "occurrences o";
        push @where, "o.$COLLECTION_NO=c.$COLLECTION_NO";
    # Handle specimen count for analyze abundance function
    # The groupby is added separately below
    } elsif (int($options{'specimen_count'})) {
        my $specimen_count = int($options{'specimen_count'});
        push @from, "sum(abund_value) as specimen_count";
        push @tables, "occurrences o";
        push @where, "o.$COLLECTION_NO=c.$COLLECTION_NO AND abund_unit IN ('specimens','individuals')";
        push @having, "sum(abund_value)>=$specimen_count";
    }

    # Reworked PS  08/15/2005
    # Instead of just doing a left join on the reids table, we achieve the close to the same effect
    # with a union of the (occurrences left join reids) UNION (occurrences,reids).
    # but for the first SQL in the union, we use o.taxon_no, while in the second we use re.taxon_no
    # This has the advantage in that it can use indexes in each case, thus is super fast (rather than taking ~5-8s for a full table scan)
    # Just doing a simple left join does the full table scan because an OR is needed (o.taxon_no IN () OR re.taxon_no IN ())
    # and because you can't use indexes for tables that have been LEFT JOINED as well
    # By hitting the occ/reids tables separately, it also has the advantage in that we can add filters so that we can only
    # get the most recent reid.
    # We hit the tables separately instead of doing a join and group by so we can populate the old_id virtual field, which signifies
    # that a collection only containts old identifications, not new ones
    my %old_ids;
    my %genera;
    my @results;
    if ($options{'taxon_list'} || $options{'taxon_name'} || $options{'taxon_no'}) {
        my %collections = (-1=>1); #default value, in case we don't find anything else, sql doesn't error out
        my ($sql1,$sql2);
        if ( $DB eq "eco" )	{
            $sql1 = "SELECT DISTINCT o.genus_name, o.species_name, o.$COLLECTION_NO, o.taxon_no FROM $OCCURRENCES o WHERE ";
        } elsif ($options{'include_old_ids'}) {
            $sql1 = "SELECT DISTINCT o.genus_name, o.species_name, o.$COLLECTION_NO, o.taxon_no, re.taxon_no re_taxon_no, (re.reid_no IS NOT NULL) is_old_id FROM occurrences o LEFT JOIN reidentifications re ON re.occurrence_no=o.occurrence_no WHERE ";
            $sql2 = "SELECT DISTINCT o.genus_name, o.species_name, o.$COLLECTION_NO, o.taxon_no, re.taxon_no re_taxon_no, (re.most_recent != 'YES') is_old_id  FROM occurrences o, reidentifications re WHERE re.occurrence_no=o.occurrence_no AND ";
        } else {
            $sql1 = "SELECT DISTINCT o.genus_name, o.species_name, o.$COLLECTION_NO, o.taxon_no, re.taxon_no re_taxon_no FROM occurrences o LEFT JOIN reidentifications re ON re.occurrence_no=o.occurrence_no WHERE re.reid_no IS NULL AND ";
            $sql2 = "SELECT DISTINCT o.genus_name, o.species_name, o.$COLLECTION_NO, o.taxon_no, re.taxon_no re_taxon_no FROM occurrences o, reidentifications re WHERE re.occurrence_no=o.occurrence_no AND re.most_recent='YES' AND ";
        }
	if ( $options{'species_reso'} )	{
		$sql1 .= "(o.species_reso IN ('".join("','",@{$options{'species_reso'}})."') OR re.species_reso IN ('".join("','",@{$options{'species_reso'}})."')) AND ";
		$sql2 .= "(o.species_reso IN ('".join("','",@{$options{'species_reso'}})."') OR re.species_reso IN ('".join("','",@{$options{'species_reso'}})."')) AND ";
	}
        # taxon_list an array reference to a list of taxon_no's
        my %all_taxon_nos;
        if ($options{'taxon_list'}) {
            my $taxon_nos;
            if (ref $options{'taxon_list'}) {
                $taxon_nos = join(",",@{$options{'taxon_list'}});
            } else {
                $taxon_nos = $options{'taxon_list'};
            }
            $taxon_nos =~ s/[^0-9,]//g;
            $taxon_nos = "-1" if (!$taxon_nos);
            $sql1 .= "o.taxon_no IN ($taxon_nos)";
            $sql2 .= "re.taxon_no IN ($taxon_nos)";
            @results = @{$dbt->getData($sql1)}; 
            push @results, @{$dbt->getData($sql2)}; 
        } elsif ($options{'taxon_name'} || $options{'taxon_no'}) {
            # Parse these values regardless
            my (@taxon_nos,%status);

            if ($options{'taxon_no'}) {
                my $sql = "SELECT taxon_name FROM authorities WHERE taxon_no=".$dbh->quote($options{'taxon_no'});
                $options{'taxon_name'} = ${$dbt->getData($sql)}[0]->{'taxon_name'};
                @taxon_nos = (int($options{'taxon_no'}))
            } else {
                if (! $options{'no_authority_lookup'}) {
                # get all variants of a name and current status but not
                #  related synonyms JA 7.1.10
                    $options{'taxon_name'} =~ s/\./%/g;
                    my $sql = "SELECT t.taxon_no,status FROM authorities a,$TAXA_TREE_CACHE t,opinions o WHERE a.taxon_no=t.taxon_no AND t.opinion_no=o.opinion_no AND taxon_name LIKE '".$options{'taxon_name'}."'";
                # if that didn't work and the name is not a species, see if
                #  it appears as a subgenus
                    my @taxa = @{$dbt->getData($sql)};
                    if ( ! @taxa )	{
                        $sql = "SELECT t.taxon_no,status FROM authorities a,$TAXA_TREE_CACHE t,opinions o WHERE a.taxon_no=t.taxon_no AND t.opinion_no=o.opinion_no AND taxon_name LIKE '% (".$options{'taxon_name'}.")'";
                        @taxa = @{$dbt->getData($sql)};
                    }
                    if ( @taxa )	{
                        $status{$_->{'taxon_no'}} = $_->{'status'} foreach @taxa;
                        push @taxon_nos , $_->{'taxon_no'} foreach @taxa;
                    }
                }
            }

            # Fix up the genus name and set the species name if there is a space 
            my ($genus,$subgenus,$species) = Taxon::splitTaxon($options{'taxon_name'});

            if (@taxon_nos) {
                # if taxon is a homonym... make sure we get all versions of the homonym
                foreach my $taxon_no (@taxon_nos) {
                    my $ignore_senior = "";
                    if ( $status{$taxon_no} =~ /nomen/ )	{
                        $ignore_senior = 1;
                    }
                    my @t = TaxaCache::getChildren($dbt,$taxon_no,'',$ignore_senior);
                    # Uses hash slices to set the keys to be equal to unique taxon_nos.  Like a mathematical UNION.
                    @all_taxon_nos{@t} = ();
                }
                my $taxon_nos_string = join(", ", keys %all_taxon_nos);
                if (!$taxon_nos_string) {
                    $taxon_nos_string = '-1';
                    push @errors, "Could not find any collections matching taxononomic name entered.";
                }
                                                    
                my $sql1a = $sql1."o.taxon_no IN ($taxon_nos_string)";
                push @results, @{$dbt->getData($sql1a)}; 
                if ( $sql2 )	{
                    my $sql2a = $sql2."re.taxon_no IN ($taxon_nos_string)";
                    push @results, @{$dbt->getData($sql2a)}; 
                }
            }
            
            if (!@taxon_nos || $options{'include_occurrences'}) {
                # It doesn't exist in the authorities table, so now hit the occurrences table directly 
                if ($options{'match_subgenera'}) {
                    my $sql1a = $sql1;
                    my $sql1b = $sql1;
                    my $sql2a = $sql2;
                    my $sql2b = $sql2;
                    my $names;
                    if ($genus)	{
                        $names .= ",".$dbh->quote($genus);
                    }
                    if ($subgenus)	{
                        $names .= ",".$dbh->quote($subgenus);
                    }
                    $names =~ s/^,//;
                    $sql1a .= " o.genus_name IN ($names)";
                    $sql1b .= " o.subgenus_name IN ($names)";
                    $sql2a .= " re.genus_name IN ($names)";
                    $sql2b .= " re.subgenus_name IN ($names)";
                    if ($species )	{
                        $sql1a .= " AND o.species_name LIKE ".$dbh->quote($species);
                        $sql1b .= " AND o.species_name LIKE ".$dbh->quote($species);
                        $sql2a .= " AND re.species_name LIKE ".$dbh->quote($species);
                        $sql2b .= " AND re.species_name LIKE ".$dbh->quote($species);
                    }
                    if ($genus || $subgenus || $species) {
                        push @results, @{$dbt->getData($sql1a)}; 
                        push @results, @{$dbt->getData($sql1b)}; 
                        push @results, @{$dbt->getData($sql2a)}; 
                        push @results, @{$dbt->getData($sql2b)}; 
                    }
                } else {
                    my $sql1b = $sql1;
                    my $sql2b = $sql2;
                    if ($genus)	{
                        $sql1b .= "o.genus_name LIKE ".$dbh->quote($genus);
                        $sql2b .= "re.genus_name LIKE ".$dbh->quote($genus);
                    }
                    if ($subgenus)	{
                        $sql1b .= " AND o.subgenus_name LIKE ".$dbh->quote($subgenus);
                        $sql2b .= " AND re.subgenus_name LIKE ".$dbh->quote($subgenus);
                    }
                    if ($species)	{
                        $sql1b .= " AND o.species_name LIKE ".$dbh->quote($species);
                        $sql2b .= " AND re.species_name LIKE ".$dbh->quote($species);
                    }
                    if ($genus || $subgenus || $species) {
                        push @results, @{$dbt->getData($sql1b)}; 
                        if ( $sql2 )	{
                            push @results, @{$dbt->getData($sql2b)}; 
                        }
                    }
                }
            }
        }

        # A bit of tricky logic - if something is matched but it isn't in the list of valid taxa (all_taxon_nos), then
        # we assume its a nomen dubium, so its considered an old id
        foreach my $row (@results) {
            $collections{$row->{$COLLECTION_NO}} = 1;
            if ( ! $genera{$row->{$COLLECTION_NO}} )	{
                $genera{$row->{$COLLECTION_NO}} = $row->{genus_name} . " " . $row->{species_name};
            } else	{
                $genera{$row->{$COLLECTION_NO}} .= ", " . $row->{genus_name} . " " . $row->{species_name};
            }
            if ($options{'include_old_ids'}) {
                if (($row->{'is_old_id'} || ($options{'taxon_name'} && %all_taxon_nos && ! exists $all_taxon_nos{$row->{'taxon_no'}})) && 
                    $old_ids{$row->{$COLLECTION_NO}} ne 'N') {
                    $old_ids{$row->{$COLLECTION_NO}} = 'Y';
                } else {
                    $old_ids{$row->{$COLLECTION_NO}} = 'N';
                }
            }
        }
        push @where, " c.$COLLECTION_NO IN (".join(", ",keys(%collections)).")";
    }

    # Handle time terms
	if ( $options{'max_interval'} || $options{'min_interval'} || $options{'max_interval_no'} || $options{'min_interval_no'}) {

        #These seeminly pointless four lines are necessary if this script is called from Download or whatever.
        # if the $q->param($var) is not set (undef), the parameters array passed into processLookup doesn't get
        # set properly, so make sure they can't be undef PS 04/10/2005
        my $eml_max = ($options{'eml_max_interval'} || '');
        my $max = ($options{'max_interval'} || '');
        my $eml_min = ($options{'eml_min_interval'} || '');
        my $min = ($options{'min_interval'} || '');
        if ($max =~ /[a-zA-Z]/ && !Validation::checkInterval($dbt,$eml_max,$max)) {
            push @errors, "There is no record of $eml_max $max in the database";
        }
        if ($min =~ /[a-z][A-Z]/ && !Validation::checkInterval($dbt,$eml_min,$min)) {
            push @errors, "There is no record of $eml_min $min in the database";
        }
        my $t = new TimeLookup($dbt);
 		my ($intervals,$errors,$warnings);
        if ($options{'max_interval_no'} =~ /^\d+$/) {
 		    ($intervals,$errors,$warnings) = $t->getRangeByInterval('',$options{'max_interval_no'},'',$options{'min_interval_no'});
        } else {
 		    ($intervals,$errors,$warnings) = $t->getRange($eml_max,$max,$eml_min,$min);
        }
        push @errors, @$errors;
        push @warnings, @$warnings;
        my $val = join(",",@$intervals);
        if ( ! $val )	{
            $val = "-1";
            if ( $options{'max_interval'} =~ /[^0-9.]/ || $options{'min_interval'} =~ /[^0-9.]/ ) {
                push @errors, "Please enter a valid time term or broader time range";
            }
            # otherwise they must have entered numerical values, so there
            #  are no worries
        }

        # need to know the boundaries of the interval to make use of the
        #  direct estimates JA 5.4.07
        my ($ub,$lb) = $t->getBoundaries();
        my $upper = 999999;
        my $lower;
        my %lowerbounds = %{$lb};
        my %upperbounds = %{$ub};
        for my $intvno ( @$intervals )  {
            if ( $upperbounds{$intvno} < $upper )   {                                                                  
                $upper = $upperbounds{$intvno};
            }
            if ( $lowerbounds{$intvno} > $lower )   {
                $lower = $lowerbounds{$intvno};
            }
        }
        # if the search terms were Ma values, you don't care what the
        #  boundaries of what are for purposes of getting collections with
        #  direct age estimates JA 15.5.07
        if ( $options{'max_interval'} =~ /^[0-9.]+$/ || $options{'min_interval'} =~ /^[0-9.]+$/ )	{
            $lower = $options{'max_interval'};
            $upper = $options{'min_interval'};
        }
        # added 1600 yr fudge factor to prevent uncalibrated 14C dates from
        #  putting Pleistocene collections in the Holocene; there is only a
        #  tiny chance that it might mess up a numerical Holocene search
        #  JA 24.1.10
        $lower -= 0.0016;
        $upper -= 0.0016;

        # only use the interval names if there is no direct estimate
        # added ma_unit and direct_ma support (egads!) 24.1.10
        push @where , "((c.max_interval_no IN ($val) AND c.min_interval_no IN (0,$val) AND c.direct_ma IS NULL AND c.max_ma IS NULL AND c.min_ma IS NULL) OR (c.max_ma_unit='YBP' AND c.max_ma IS NOT NULL AND c.max_ma/1000000<=$lower AND c.min_ma/1000000>=$upper) OR (c.max_ma_unit='Ka' AND c.max_ma IS NOT NULL AND c.max_ma/1000<=$lower AND c.min_ma/1000>=$upper) OR (c.max_ma_unit='Ma' AND c.max_ma IS NOT NULL AND c.max_ma<=$lower AND c.min_ma>=$upper) OR (c.direct_ma_unit='YBP' AND c.direct_ma/1000000<=$lower AND c.direct_ma/1000000>=$upper) OR (c.direct_ma_unit='Ka' AND c.direct_ma/1000<=$lower AND c.direct_ma/1000>=$upper AND c.direct_ma) OR (c.direct_ma_unit='Ma' AND c.direct_ma<=$lower AND c.direct_ma>=$upper))";
	}
                                        
	# Handle half/quarter degrees for long/lat respectively passed by Map.pm PS 11/23/2004
    if ( $options{"coordres"} eq "half") {
		if ($options{"latdec_range"} eq "00") {
			push @where, "((latmin >= 0 AND latmin <15) OR " 
 						. "(latdec regexp '^(0|1|2\$|(2(0|1|2|3|4)))') OR "
                        . "(latmin IS NULL AND latdec IS NULL))";
		} elsif($options{"latdec_range"} eq "25") {
			push @where, "((latmin >= 15 AND latmin <30) OR "
 						. "(latdec regexp '^(4|3|(2(5|6|7|8|9)))'))";
		} elsif($options{"latdec_range"} eq "50") {
			push @where, "((latmin >= 30 AND latmin <45) OR "
 						. "(latdec regexp '^(5|6|7\$|(7(0|1|2|3|4)))'))";
		} elsif ($options{'latdec_range'} eq "75") {
			push @where, "(latmin >= 45 OR (latdec regexp '^(9|8|(7(5|6|7|8|9)))'))";
		}

		if ( $options{'lngdec_range'} eq "50" )	{
			push @where, "(lngmin>=30 OR (lngdec regexp '^(5|6|7|8|9)'))";
		} elsif ($options{'lngdec_range'} eq "00") {
			push @where, "(lngmin<30 OR (lngdec regexp '^(0|1|2|3|4)') OR (lngmin IS NULL AND lngdec
IS NULL))";
		}
    # assume coordinate resolution is 'full', which means full/half degress for long/lat
    # respectively 
	} else {
		if ( $options{'latdec_range'} eq "50" )	{
			push @where, "(latmin>=30 OR (latdec regexp '^(5|6|7|8|9)'))";
		} elsif ($options{'latdec_range'} eq "00") {
			push @where, "(latmin<30 OR (latdec regexp '^(0|1|2|3|4)') OR (latmin IS NULL AND latdec
IS NULL))";
		}
	}

    # Handle period - legacy
	if ($options{'period'}) {
		my $periodName = $dbh->quote($options{'period'});
		push @where, "(period_min LIKE " . $periodName . " OR period_max LIKE " . $periodName . ")";
	}
	
	# Handle intage - legacy
	if ($options{'intage'}) {
		my $intageName = $dbh->quote($options{'intage'});
		push @where, "(intage_min LIKE " . $intageName . " OR intage_max LIKE " . $intageName . ")";
	}
	
	# Handle locage - legacy
	if ($options{'locage'}) {
		my $locageName = $dbh->quote($options{'locage'});
		push @where, "(locage_min LIKE " . $locageName . " OR locage_max LIKE " . $locageName . ")";
	}
	
	# Handle epoch - legacy
	if ($options{'epoch'}) {
		my $epochName = $dbh->quote($options{'epoch'});
		push @where, "(epoch_min LIKE " . $epochName . " OR epoch_max LIKE " . $epochName . ")";
	}

    # Handle authorizer/enterer/modifier - mostly legacy except for person
    if ($options{'person_reversed'}) {
        my $sql = "SELECT person_no FROM person WHERE name like ".$dbh->quote(Person::reverseName($options{'person_reversed'}));
        my $person_no = ${$dbt->getData($sql)}[0]->{'person_no'};
        if (!$person_no) {
            push @errors, "$options{person_reversed} is not a valid database member. Format like 'Sepkoski, J.'";
        } else {
            if ($options{'person_type'} eq 'any') {
                push @where, "(c.authorizer_no=$person_no OR c.enterer_no=$person_no OR c.modifier_no=$person_no)";
            } elsif ($options{'person_type'} eq 'modifier') {
                $options{'modifier_no'} = $person_no;
            } elsif ($options{'person_type'} eq 'enterer') {
                $options{'enterer_no'} = $person_no;
            } else { #default authorizer
                $options{'authorizer_no'} = $person_no;
            }
        }
    }
    if ($options{'authorizer_reversed'}) {
        my $sql = "SELECT person_no FROM person WHERE name like ".$dbh->quote(Person::reverseName($options{'authorizer_reversed'}));
        $options{'authorizer_no'} = ${$dbt->getData($sql)}[0]->{'person_no'};
        push @errors, "$options{authorizer_reversed} is not a valid authorizer. Format like 'Sepkoski, J.'" if (!$options{'authorizer_no'});
    }

    if ($options{'enterer_reversed'}) {
        my $sql = "SELECT person_no FROM person WHERE name like ".$dbh->quote(Person::reverseName($options{'enterer_reversed'}));
        $options{'enterer_no'} = ${$dbt->getData($sql)}[0]->{'person_no'};
        push @errors, "$options{enterer_reversed} is not a valid enterer. Format like 'Sepkoski, J.'" if (!$options{'enterer_no'});
        
    }

    if ($options{'modifier_reversed'}) {
        my $sql = "SELECT person_no FROM person WHERE name like ".$dbh->quote(Person::reverseName($options{'modifier_reversed'}));
        $options{'modifier_no'} = ${$dbt->getData($sql)}[0]->{'person_no'};
        push @errors, "$options{modifier_reversed} is not a valid modifier. Format like 'Sepkoski, J.'" if (!$options{'modifier_no'});
    }

	# Handle modified date
	if ($options{'modified_since'} || $options{'year'})	{
        my ($yyyy,$mm,$dd);
        if ($options{'modified_since'}) {
            my $nowDate = now();
            if ( "yesterday" eq $options{'modified_since'}) {
                $nowDate = $nowDate-'1D';
            } elsif ( "two days ago" eq $options{'modified_since'}) {
                $nowDate = $nowDate-'2D';
            } elsif ( "three days ago" eq $options{'modified_since'}) {
                $nowDate = $nowDate-'3D';
            } elsif ( "last week" eq $options{'modified_since'}) {
                $nowDate = $nowDate-'7D';
            } elsif ( "two weeks ago" eq $options{'modified_since'}) {
                $nowDate = $nowDate-'14D';
            } elsif ( "three weeks ago" eq $options{'modified_since'}) {
                $nowDate = $nowDate-'21D';
            } elsif ( "last month" eq $options{'modified_since'}) {
                $nowDate = $nowDate-'1M';
            }
            my ($date,$time) = split / /,$nowDate;
            ($yyyy,$mm,$dd) = split /-/,$date,3;
        } elsif ($options{'year'}) {
            $yyyy = $options{'year'};
            $mm = $options{'month'};
            # caught a major error here in which months passed as strings
            #  (as normal) were not converted to numbers JA 4.5.06
            my @months = ( "","January","February","March","April","May","June","July","August","September","October","November","December" );
            for my $m ( 0..$#months )	{
                if ( $mm eq $months[$m] )	{
                    $mm = $m;
                    last;
                }
            }
            if ( $mm !~ /(10)|(11)|(12)/ )	{
                $mm = "0" . $mm;
            }
            $dd = $options{'day_of_month'};
            if ( $dd < 10 )	{
                $dd = "0" . $dd;
            }
        }  

        my $val = $dbh->quote(sprintf("%d-%02d-%02d 00:00:00",$yyyy,$mm,$dd));
        if ( $options{'beforeafter'} eq "created after" )  {
            push @where, "created > $val";
        } elsif ( $options{"beforeafter"} eq "created before" )    {
            push @where, "created < $val";
        } elsif ( $options{"beforeafter"} eq "modified after" )    {
            push @where, "modified > $val";
        } elsif ( $options{"beforeafter"} eq "modified before" )   {
            push @where, "modified < $val";
        } 
	}
	
	# Handle collection name (must also search collection_aka field) JA 7.3.02
	if ($options{'collection_list'} && $options{'collection_list'} =~ /^[\d ,]+$/) {
		push @where, "c.$COLLECTION_NO IN ($options{collection_list})";
	}
	if ( ( $DB ne "eco" && $options{'collection_names'} ) || ( $DB eq "eco" && $options{'inventory_name'} ) ) {
		my $OPTION = $options{'collection_names'};
		( $DB eq "eco" ) ? $OPTION = $options{'inventory_name'} : "";
		# only match entire numbers within names, not parts
		my $word = $dbh->quote('%'.$OPTION.'%');
		my $integer = $dbh->quote('.*[^0-9]'.$OPTION.'(([^0-9]+)|($))');
		# interpret plain integers as either names, collection years,
		#  or collection_nos
		if ($OPTION =~ /^\d+$/) {
			if ( $DB ne "eco" )	{
				push @where, "(c.collection_name REGEXP $integer OR c.collection_aka REGEXP $integer OR c.collection_dates REGEXP $integer OR c.$COLLECTION_NO=$OPTION)";
			} else	{
				push @where, "(c.inventory_name REGEXP $integer OR c.inventory_aka REGEXP $integer OR c.years REGEXP $integer OR c.$COLLECTION_NO=$OPTION)";
			}
		}
		# comma-separated lists of numbers are collection_nos, period
		elsif ($OPTION =~ /^[0-9, \-]+$/) {
			my @collection_nos;
			my @ranges = split(/\s*,\s*/,$OPTION);
			foreach my $range (@ranges) {
				if ($range =~ /-/) {
					my ($min,$max) = split(/\s*-\s*/,$range);
					if ($min < $max) {
						push @collection_nos, ($min .. $max);
					} else {
						push @collection_nos, ($max .. $min);
					}
				} else {
					push @collection_nos , $range;
				}
			}
			push @where, "c.$COLLECTION_NO IN (".join(",",@collection_nos).")";
		}
		# interpret non-integers/non-lists of integers as names or
		#  collectors
		# assume that collectors field has names and collection_dates
		#  doesn't (because non-year values are not interesting)
		else {
			if ( $DB ne "eco" )	{
				push @where, "(c.collection_name LIKE $word OR c.collection_aka LIKE $word OR c.collectors LIKE $word)";
			} else	{
				push @where, "(c.inventory_name LIKE $word OR c.inventory_aka LIKE $word OR c.inventoried_by LIKE $word)";
			}
		}
	}
	
    # Handle localbed, regionalbed
    if ($options{'regionalbed'} && $options{'regionalbed'} =~ /^[0-9.]+$/) {
        my $min = int($options{'regionalbed'});
        my $max = $min + 1;
        push @where,"regionalbed >= $min","regionalbed <= $max";
    }
    if ($options{'localbed'} && $options{'localbed'} =~ /^[0-9.]+$/) {
        my $min = int($options{'localbed'});
        my $max = $min + 1;
        push @where ,"localbed >= $min","localbed <= $max";
    }

    # Maybe special environment terms
    if ( $options{'environment'}) {
        my $environment;
        if ($options{'environment'} =~ /general/i) {
            $environment = join(",", map {"'".$_."'"} @{$HTMLBuilder::hard_lists{'environment_general'}});
        } elsif ($options{'environment'} =~ /terrestrial/i) {
            $environment = join(",", map {"'".$_."'"} @{$HTMLBuilder::hard_lists{'environment_terrestrial'}});
        } elsif ($options{'environment'} =~ /^marine/i) {
            $environment = join(",", map {"'".$_."'"} @{$HTMLBuilder::hard_lists{'environment_siliciclastic'}});
            $environment .= "," . join(",", map {"'".$_."'"} @{$HTMLBuilder::hard_lists{'environment_carbonate'}});
        } elsif ($options{'environment'} =~ /siliciclastic/i) {
            $environment = join(",", map {"'".$_."'"} @{$HTMLBuilder::hard_lists{'environment_siliciclastic'}});
        } elsif ($options{'environment'} =~ /carbonate/i) {
            $environment = join(",", map {"'".$_."'"} @{$HTMLBuilder::hard_lists{'environment_carbonate'}});
        } elsif ($options{'environment'} =~ /^(lacustrine|fluvial|karst|marginal.marine|reef|shallow.subtidal|deep.subtidal|offshore|slope.basin)$/i) {
            for my $z ( 'lacustrine','fluvial','karst','other_terrestrial','marginal_marine','reef','shallow_subtidal','deep_subtidal','offshore','slope_basin' )	{
                if ($options{'environment'} =~ $z)	{
                    $environment = join(",", map {"'".$_."'"} @{$HTMLBuilder::hard_lists{"zone_$z"}});
                    last;
                }
            }
        } else {
            $environment = $dbh->quote($options{'environment'});
        }
        if ($environment) {
            $environment =~ s/,'',/,/g;
            push @where, "c.environment IN ($environment)";
        }
    }
		
	# research_group is now a set -- tone 7 jun 2002
	if($options{'research_group'}) {
        my $research_group_sql = PBDBUtil::getResearchGroupSQL($dbt,$options{'research_group'});
        push @where, $research_group_sql if ($research_group_sql);
	}
    
	if ( int($options{'reference_no'}) && $DB ne "eco" )	{
		push @where, " (c.reference_no=".int($options{'reference_no'})." OR sr.reference_no=".int($options{'reference_no'}).") ";
	} elsif ( int($options{'reference_no'}) )	{
		push @where, " c.reference_no=".int($options{'reference_no'});
	}

	if ( $options{'citation'} =~ /^[A-Za-z'\-]* [12][0-9][0-9][0-9]$/ )	{
		my ($auth,$yr) = split / /,$options{'citation'};
		my $sql = "SELECT reference_no FROM refs WHERE (author1last LIKE '$auth' OR author2last LIKE '$auth') AND pubyr=$yr";
		my @refs = @{$dbt->getData($sql)};
		my @ref_nos = map {$_->{'reference_no'}} @refs;
		push @where , "c.reference_no IN (".join(',',@ref_nos).")";
	}

    # Do a left join on secondary refs if we have to
    # PS 11/29/2004
    if ( ($options{'research_group'} =~ /^(?:decapod|divergence|ETE|5%|1%|PACED|PGAP)$/ || int($options{'reference_no'})) && $DB ne "eco" ) {
        push @left_joins, "LEFT JOIN secondary_refs sr ON sr.$COLLECTION_NO=c.$COLLECTION_NO";
    }

	# note, we have one field in the collection search form which is unique because it can
	# either be geological_group, formation, or member.  Therefore, it has a special name, 
	# group_formation_member, and we'll have to deal with it separately.
	# added by rjp on 1/13/2004
	if ($options{"group_formation_member"}) {
        if ($options{"group_formation_member"} eq 'NOT_NULL_OR_EMPTY') {
		    push(@where, "((c.geological_group IS NOT NULL AND c.geological_group !='') OR (c.formation IS NOT NULL AND c.formation !=''))");
        } else {
            my $val = $dbh->quote('%'.$options{"group_formation_member"}.'%');
		    push(@where, "(c.geological_group LIKE $val OR c.formation LIKE $val OR c.member LIKE $val)");
        }
	}

    # This field is only passed by section search form PS 12/01/2004
    if (exists $options{"section_name"} && $options{"section_name"} eq '') {
        push @where, "((c.regionalsection IS NOT NULL AND c.regionalsection != '' AND c.regionalbed REGEXP '^(-)?[0-9.]+\$') OR (c.localsection IS NOT NULL AND c.localsection != '' AND c.localbed REGEXP '^(-)?[0-9.]+\$'))";
    } elsif ($options{"section_name"}) {
        my $val = $dbh->quote('%'.$options{"section_name"}.'%');
        push @where, "((c.regionalsection  LIKE  $val AND c.regionalbed REGEXP '^(-)?[0-9.]+\$') OR (c.localsection  LIKE  $val AND c.localbed REGEXP '^(-)?[0-9.]+\$'))"; 
    }                

    # This field is only passed by links created in the Strata module PS 12/01/2004
	if ($options{"lithologies"}) {
		my $val = $dbh->quote($options{"lithologies"});
		push @where, "(c.lithology1=$val OR c.lithology2=$val)"; 
	}
	if ($options{"lithadjs"}) {
		my $val = $dbh->quote('%'.$options{"lithadjs"}.'%');
		push @where, "(c.lithadj LIKE $val OR c.lithadj2 LIKE $val)";
    }

    # This can be country or continent. If its country just treat it like normal, else
    # do a lookup of all the countries in the continent
    if ($options{"country"}) {
        if ($options{"country"} =~ /^(North America|South America|Europe|Africa|Antarctica|Asia|Australia)/) {
            if ( ! open ( REGIONS, "data/PBDB.regions" ) ) {
                my $error_message = $!;
                die($error_message);
            }

            my %REGIONS;
            while (<REGIONS>)
            {
                chomp();
                my ($region, $countries) = split(/:/, $_, 2);
                $countries =~ s/'/\\'/g;
                $REGIONS{$region} = $countries;
            }
            my @countries;
            for my $r ( split(/[^A-Za-z ]/,$options{"country"}) )	{
                push @countries , split(/\t/,$REGIONS{$r});
            }
            foreach my $country (@countries) {
                $country = "'".$country."'";
            }
            my $in_str = join(",", @countries);
            push @where, "c.country IN ($in_str)";
        } else {
            push @where, "c.country LIKE ".$dbh->quote($options{'country'});
        }
    }
    if ($options{'plate'}) {
        $options{'plate'} =~ s/[^0-9,]/,/g;
        while ( $options{'plate'} =~ /,,/ )	{
            $options{'plate'} =~ s/,,/,/g;
        }
        push @where, "c.plate IN ($options{'plate'})";
    }

    # get the column info from the table
    my $sth = $dbh->column_info(undef,'pbdb',$COLLECTIONS,'%');

	# Compose the WHERE clause
	# loop through all of the possible fields checking if each one has a value in it
    my %all_fields = ();
    while (my $row = $sth->fetchrow_hashref()) {
        my $field = $row->{'COLUMN_NAME'};
        $all_fields{$field} = 1;
        my $type = $row->{'TYPE_NAME'};
        my $is_nullable = ($row->{'IS_NULLABLE'} eq 'YES') ? 1 : 0;
        my $is_primary =  $row->{'mysql_is_pri_key'};

        # These are special cases handled above in code, so skip them
        next if ($field =~ /^(?:environment|localbed|regionalbed|research_group|reference_no|max_interval_no|min_interval_no|country|plate|inventory_name)$/);

		if (exists $options{$field} && $options{$field} ne '') {
			my $value = $options{$field};
			my ($null,$endnull);
		# special handling if user passes a list with NULL_OR_EMPTY
			if ( $value =~ /(^NULL_OR_EMPTY)|(,NULL_OR_EMPTY)/ )	{
				$value =~ s/(|,)(NULL_OR_EMPTY)(|,)//;
				$null = "(c.$field IS NULL OR c.$field='' OR ";
				$endnull = ")";
			}

			if ( $value eq "NOT_NULL_OR_EMPTY" )	{
				push @where , "(c.$field IS NOT NULL AND c.$field !='')";
			} elsif ($value eq "NULL_OR_EMPTY" ) {
				push @where ,"(c.$field IS NULL OR c.$field ='')";
			} elsif ( $type =~ /ENUM/i ) {
				# It is in a pulldown... no wildcards
				push @where, "$null c.$field IN ('".join("','",split(/,/,$value))."')$endnull";
			} elsif ( $type =~ /SET/i ) {
                # Its a set, use the special set syntax
				push @where, "$null FIND_IN_SET(".$dbh->quote($value).", c.$field)$endnull";
			} elsif ( $type =~ /INT/i ) {
                # Don't need to quote ints, however cast them to int a security measure
				push @where, "$null c.$field=".int($value).$endnull;
			} else {
                # Assuming character, datetime, etc. 
				push @where, "$null c.$field LIKE ".$dbh->quote('%'.$value.'%').$endnull;
			}
		}
	}

    # Print out an errors that may have happened.
    # htmlError print header/footer and quits as well
    if (!scalar(@where)) {
        push @errors, "No search terms were entered";
    }
    
	if (@errors) {
		my $message = "<div align=\"center\">".Debug::printErrors(\@errors)."<br>";
		if ( $options{"calling_script"} eq "displayCollResults" )	{
			return;
		} elsif ( $options{"calling_script"} eq "Review" )	{
			return;
		} elsif ( $options{"calling_script"} eq "Map" )	{
			$message .= "<a href=\"$READ_URL?action=displayMapForm\"><b>Try again</b></a>";
		} elsif ( $options{"calling_script"} eq "Confidence" )	{
			$message .= "<a href=\"$READ_URL?action=displaySearchSectionForm\"><b>Try again</b></a>";
		} elsif ( $options{"type"} eq "add" )	{
			$message .= "<a href=\"$WRITE_URL?action=displaySearchCollsForAdd&type=add\"><b>Try again</b></a>";
		} else	{
			$message .= "<a href=\"$READ_URL?action=displaySearchColls&type=$options{type}\"><b>Try again</b></a>";
		}
		$message .= "</div><br>";
		main::displayCollectionForm($message);
		exit;
		die($message);
	}

    if ($options{'count_occurrences'})	{
        push @groupby,"taxon_no";
    # Cover all our bases
    } elsif (scalar(@left_joins) || scalar(@tables) > 1 || $options{'taxon_list'} || $options{'taxon_name'}) {
        push @groupby,"c.$COLLECTION_NO";
    }

	# Handle sort order

    # Only necessary if we're doing a union
    my $sortby = "";
    if ($options{'sortby'}) {
        if ($all_fields{$options{'sortby'}}) {
            $sortby .= "c.$options{sortby}";
        } elsif ($options{'sortby'} eq 'interval_name') {
            push @left_joins, "LEFT JOIN intervals si ON si.interval_no=c.max_interval_no";
            $sortby .= "si.interval_name";
        } elsif ($options{'sortby'} eq 'geography') {
            $sortby .= "IF(c.state IS NOT NULL AND c.state != '',c.state,c.country)";
        } elsif ($options{'sortby'} eq 'occurrences') {
            $sortby .= "c";
        }

        if ($sortby) {
            if ($options{'sortorder'} =~ /desc/i) {
                $sortby.= " DESC";
            } else {
                $sortby.= " ASC";
            }
        }
    }

    my $sql = "SELECT ".join(",",@from).
           " FROM (" .join(",",@tables).") ".join (" ",@left_joins).
           " WHERE ".join(" AND ",@where);
    $sql .= " GROUP BY ".join(",",@groupby) if (@groupby);  
    $sql .= " HAVING ".join(",",@having) if (@having);  
    $sql .= " ORDER BY ".$sortby if ($sortby);

    dbg("Collections sql: $sql");

    $sth = $dbh->prepare($sql);
    $sth->execute();
    my $p = Permissions->new($s,$dbt); 

    # See if rows okay by permissions module
    my @dataRows = ();
    my $limit = (int($options{'limit'})) ? int($options{'limit'}) : 10000000;
    my $totalRows = 0;
    if ( $DB ne "eco" )	{
        $p->getReadRows ( $sth, \@dataRows, $limit, \$totalRows);
    } else	{
        while ( my $row = $sth->fetchrow_hashref ( ) )	{
            push @dataRows, $row;
        }
        $totalRows = $#dataRows + 1;
    }

    if ($options{'include_old_ids'}) {
        foreach my $row (@dataRows) {
            if ($old_ids{$row->{$COLLECTION_NO}} eq 'Y') {
                $row->{'old_id'} = 1;
            }
        }
    }
    if ($options{'enterer'} || $options{'modifier'}) {
        my %lookup = %{PBDBUtil::getPersonLookup($dbt)};
        if ($options{'enterer'})	{
            for my $row (@dataRows) {
                $row->{'enterer'} = $lookup{$row->{'enterer'}};
            }
        }
        if ($options{'modifier'})	{
            for my $row (@dataRows) {
                $row->{'modifier'} = $lookup{$row->{'modifier'}};
            }
        }
    }
    for my $row (@dataRows) {
        if ( $genera{$row->{$COLLECTION_NO}} )	{
            $row->{genera} = $genera{$row->{$COLLECTION_NO}};
        }
    }
    if ($options{'count_occurrences'})	{
        return (\@dataRows,$totalRows,\@warnings,\@results);
    } else	{
        return (\@dataRows,$totalRows,\@warnings);
    }
}


# This is a multi step process: 
# First populate our page variables with prefs, these have the lowest priority
# TBD CHeck for reerence no
sub displayCollectionForm {
    my ($dbt,$q,$s,$hbo) = @_;
    my $dbh = $dbt->dbh;

    my $isNewEntry = ($q->param('collection_no') =~ /^\d+$/) ? 0 : 1;
    my $reSubmission = ($q->param('action') =~ /processCollectionForm/) ? 1 : 0;

    # First check to nake sure they have a reference no for new entries
    my $session_ref = $s->get('reference_no');
    if ($isNewEntry) {
        if (!$session_ref) {
            $s->enqueue($q->query_string() );
            main::displaySearchRefs( "Please choose a reference first" );
            exit;
        }  
    }

    # First get all three sources of data: form submision (%form), prefs (%prefs), and database (%row)
    my %vars = ();

    my %row = ();
    if (!$isNewEntry) {
        my $collection_no = int($q->param('collection_no'));
        my $sql = "SELECT * FROM collections WHERE collection_no=$collection_no";
        my $c_row = ${$dbt->getData($sql)}[0] or die "invalid collection no";
        %row = %{$c_row};
    }
    my %prefs =  $s->getPreferences();
    my %form = $q->Vars();


    if ($reSubmission) {
        %vars = %form;
    } if ($isNewEntry && int($q->param('prefill_collection_no'))) {
        my $collection_no = int($q->param('prefill_collection_no'));
        my $sql = "SELECT * FROM collections WHERE collection_no=$collection_no";
        my $row = ${$dbt->getData($sql)}[0] or die "invalid collection no";
        foreach my $field (keys(%$row)) {
            if ($field =~ /^(authorizer|enterer|modifier|authorizer_no|enterer_no|modifier_no|created|modified|collection_no)/) {
                delete $row->{$field};
            }
        }
        %vars = %$row;
        $vars{'reference_no'} = $s->get('reference_no');
    } elsif ($isNewEntry) {
        %vars = %prefs; 
        # carry over the lat/long coordinates the user entered while doing
        #  the mandatory collection search JA 6.4.04
        my @coordfields = ("latdeg","latmin","latsec","latdec","latdir","lngdeg","lngmin","lngsec","lngdec","lngdir");
        foreach my $cf (@coordfields) {
            $vars{$cf} = $form{$cf};
        }
        $vars{'reference_no'} = $s->get('reference_no');
    } else {
        %vars = %row;
    }
    
    # always carry over optional fields
    $vars{'taphonomy'} = $prefs{'taphonomy'};
    $vars{'use_primary'} = $q->param('use_primary');

    my $ref = Reference::getReference($dbt,$vars{'reference_no'});
    my $formatted_primary = Reference::formatLongRef($ref);

    $vars{'ref_string'} = '<table cellspacing="0" cellpadding="2" width="100%"><tr>'.
    "<td valign=\"top\"><a href=\"$READ_URL?action=displayReference&reference_no=$vars{reference_no}\">".$vars{'reference_no'}."</a></b>&nbsp;</td>".
    "<td valign=\"top\"><span class=red>$ref->{project_name} $ref->{project_ref_no}</span></td>".
    "<td>$formatted_primary</td>".
    "</tr></table>";      

    if (!$isNewEntry) {
        my $collection_no = $row{'collection_no'};
        # We need to take some additional steps for an edit
        my $p = Permissions->new($s,$dbt);
        my $can_modify = $p->getModifierList();
        $can_modify->{$s->get('authorizer_no')} = 1;
        unless ($can_modify->{$row{'authorizer_no'}} || $s->isSuperUser) {
            my $authorizer = Person::getPersonName($dbt,$row{'authorizer_no'});
            print qq|<p class="warning">You may not edit this collection because you are not on the editing permission list of the authorizer ($authorizer)<br>
<a href="$WRITE_URL?action=displaySearchColls&type=edit"><br>Edit another collection</b></a>
|;
            exit;
        }

        # translate the release date field to populate the pulldown
        # I'm not sure if we never did this at all, or if something got
        #  broken at some point, but it was causing big problems JA 10.5.07

        if ( date($vars{'created'}) != date($vars{'release_date'}) )	{
            $vars{'release_date'} = getReleaseString($vars{'created'},$vars{'release_date'});
        }

        # Secondary refs, followed by current ref
        my @secondary_refs = Reference::getSecondaryRefs($dbt,$collection_no);
        if (@secondary_refs) {
            my $table = '<table cellspacing="0" cellpadding="2" width="100%">';
            for(my $i=0;$i < @secondary_refs;$i++) {
                my $sr = $secondary_refs[$i];
                my $ref = Reference::getReference($dbt,$sr);
                my $formatted_secondary = Reference::formatLongRef($ref);
                my $class = ($i % 2 == 0) ? 'class="darkList"' : '';
                $table .= "<tr $class>".
                  "<td valign=\"top\"><input type=\"radio\" name=\"secondary_reference_no\" value=\"$sr\">".
                  "</td><td valign=\"top\" style=\"text-indent: -1em; padding-left: 2em;\"><b>$sr</b> ".
                  "$formatted_secondary <span style=\"color: red;\">$ref->{project_name} $ref->{project_ref_no}</span>";
                if(refIsDeleteable($dbt,$collection_no,$sr)) {
                    $table .= " <nobr>&nbsp;<input type=\"checkbox\" name=\"delete_ref\" value=$sr> remove<nobr>";
                }
                $table .= "</td></tr>";
            }
            $table .= "</table>";
            $vars{'secondary_reference_string'} = $table;
        }   

        # Check if current session ref is at all associated with the collection
        # If not, list it beneath the sec. refs. (with radio button for selecting
        # as the primary ref, as with the secondary refs below).
        if ($session_ref) {
            unless(isRefPrimaryOrSecondary($dbt,$collection_no,$session_ref)){
                my $ref = Reference::getReference($dbt,$session_ref);
                my $sr = Reference::formatLongRef($ref);
                my $table = '<table cellspacing="0" cellpadding="2" width="100%">'
                          . "<tr class=\"darkList\"><td valign=top><input type=radio name=secondary_reference_no value=$session_ref></td>";
                $table .= "<td valign=top><b>$ref->{reference_no}</b></td>";
                $table .= "<td>$sr</td></tr>";
                # Now, set up the current session ref to be added as a secondary even
                # if it's not picked as a primary (it's currently neither).
                $table .= "<tr class=\"darkList\"><td></td><td colspan=2><input type=checkbox name=add_session_ref value=\"YES\"> Add session reference as secondary reference</td></tr>\n";
                $table .= "</table>";
                $vars{'session_reference_string'} = $table;
            }
        }
    }

    # Get back the names for these
	if ( $vars{'max_interval_no'} )	{
		my $sql = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=".$vars{'max_interval_no'};
        my $interval = ${$dbt->getData($sql)}[0];
		$vars{'eml_max_interval'} = $interval->{eml_interval};
		$vars{'max_interval'} = $interval->{interval_name};
	}
	if ( $vars{'min_interval_no'} )	{
		my $sql = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=".$vars{'min_interval_no'};
        my $interval = ${$dbt->getData($sql)}[0];
		$vars{'eml_min_interval'} = $interval->{eml_interval};
		$vars{'min_interval'} = $interval->{interval_name};
	}

    $ref = Reference::getReference($dbt,$vars{'reference_no'});
    $formatted_primary = Reference::formatLongRef($ref);

	print PBDBUtil::printIntervalsJava($dbt);

    if ($isNewEntry) {
        $vars{'page_title'} =  "Collection entry form";
        $vars{'page_submit_button'} = '<input type=submit name="enter_button" value="Enter collection and exit">';
    } else {
        $vars{'page_title'} =  "Collection number ".$vars{'collection_no'};
        $vars{'page_submit_button'} = '<input type=submit name="edit_button" value="Edit collection and exit">';
        if ( $vars{'art_whole_bodies'} || $vars{'disart_assoc_maj_elems'} || $vars{'disassoc_maj_elems'} || $vars{'disassoc_minor_elems'} )	{
            $vars{'elements'} = 1;
        }
    }

    # Output the main part of the page
    print $hbo->populateHTML("collection_form", \%vars);
}


#  * User submits completed collection entry form
#  * System commits data to database and thanks the nice user
#    (or displays an error message if something goes terribly wrong)
sub processCollectionForm {
	my ($dbt,$q,$s,$hbo) = @_;
	my $dbh = $dbt->dbh;

	my $reference_no = $q->param("reference_no");
	my $secondary = $q->param('secondary_reference_no');

	my $collection_no = $q->param($COLLECTION_NO);

	my $isNewEntry = ($collection_no > 0) ? 0 : 1;
    
	# If a radio button was checked, we're changing a secondary to the primary
	if ($secondary)	{
		$q->param(reference_no => $secondary);
	}

	# change interval names into numbers by querying the intervals table
	# JA 11-12.7.03
	if ( $q->param('max_interval') )	{
		my $sql = "SELECT interval_no FROM intervals WHERE interval_name='" . $q->param('max_interval') . "'";
		if ( $q->param('eml_max_interval') )	{
			$sql .= " AND eml_interval='" . $q->param('eml_max_interval') . "'";
		} else	{
			$sql .= " AND eml_interval=''";
		}
		my $no = ${$dbt->getData($sql)}[0]->{interval_no};
		$q->param(max_interval_no => $no);
	}
	if ( $q->param('min_interval') )	{
		my $sql = "SELECT interval_no FROM intervals WHERE interval_name='" . $q->param('min_interval') . "'";
		if ( $q->param('eml_min_interval') )	{
			$sql .= " AND eml_interval='" . $q->param('eml_min_interval') . "'";
		} else	{
			$sql .= " AND eml_interval=''";
		}
		my $no = ${$dbt->getData($sql)}[0]->{interval_no};
		$q->param(min_interval_no => $no);
	} elsif ( $DB ne "eco" )	{
		$q->param(min_interval_no => 0);
	}

	# bomb out if no such interval exists JA 28.7.03
	if ( $q->param('max_interval_no') < 1 && $DB ne "eco" )	{
		print "<center><p>You can't enter an unknown time interval name</p>\n<p>Please go back, check the time scales, and enter a valid name</p></center>";
		return;
	}

	# the inventory form submits lat-min-sec in a single field JA 25.5.11
	if ( $q->param('lat') =~ / / )	{
		my ($lat,$format) = fromMinSec(split / /,$q->param('lat'));
		$q->param('lat' => $lat);
		$q->param('latlng_format' => $format);
	} elsif ( $q->param('lat') )	{
		my ($left,$right) = split /\./,$q->param('lat');
		( ! $right ) ? $right = 1 : "";
		$q->param('latlng_format' => length($right));
	}
	if ( $q->param('lng') =~ / / )	{
		my ($lng,$format) = fromMinSec(split / /,$q->param('lng'));
		$q->param('lng' => $lng);
	}

    unless($q->param('fossilsfrom1')) {
      $q->param(fossilsfrom1=>'');
    }
    unless($q->param('fossilsfrom2')) {
      $q->param(fossilsfrom2=>'');
    }


    my $is_valid = validateCollectionForm($dbt,$q,$s);

    if ($is_valid) {

        #set paleolat, paleolng if we can PS 11/07/2004
        my ($paleolat, $paleolng, $pid);
        if ($q->param('lngdeg') >= 0 && $q->param('lngdeg') =~ /\d+/ &&
            $q->param('latdeg') >= 0 && $q->param('latdeg') =~ /\d+/)
        {
            my ($f_latdeg, $f_lngdeg);
            if ($q->param('lngmin') =~ /\d+/ && $q->param('lngmin') >= 0 && $q->param('lngmin') < 60)  {
                $f_lngdeg = $q->param('lngdeg') + ($q->param('lngmin')/60) + ($q->param('lngsec')/3600);
            } else {
                $f_lngdeg = $q->param('lngdeg') . "." .  int($q->param('lngdec'));
            }
            if ($q->param('latmin') =~ /\d+/ && $q->param('latmin') >= 0 && $q->param('latmin') < 60)  {
                $f_latdeg = $q->param('latdeg') + ($q->param('latmin')/60) + ($q->param('latsec')/3600);
            } else {
                $f_latdeg = $q->param('latdeg') . "." .  int($q->param('latdec'));
            }
            dbg("f_lngdeg $f_lngdeg f_latdeg $f_latdeg");
            if ($q->param('lngdir') =~ /West/)  {
                    $f_lngdeg = $f_lngdeg * -1;
            }
            if ($q->param('latdir') =~ /South/) {
                    $f_latdeg = $f_latdeg * -1;
            }

            my $max_interval_no = ($q->param('max_interval_no')) ? $q->param('max_interval_no') : 0;
            my $min_interval_no = ($q->param('min_interval_no')) ? $q->param('min_interval_no') : 0;
            ($paleolng, $paleolat, $pid) = getPaleoCoords($dbt,$max_interval_no,$min_interval_no,$f_lngdeg,$f_latdeg);
            dbg("have paleocoords paleolat: $paleolat paleolng $paleolng");
            if ($paleolat ne "" && $paleolng ne "") {
                $q->param("paleolng"=>$paleolng);
                $q->param("paleolat"=>$paleolat);
                $q->param("plate"=>$pid);
            }
        }


        # figure out the release date, enterer, and authorizer
        my $created = now();
        if (!$isNewEntry) {
            my $sql = "SELECT created FROM $COLLECTIONS WHERE $COLLECTION_NO=$collection_no";
            my $row = ${$dbt->getData($sql)}[0];
            die "Could not fetch collection $collection_no from the database" unless $row;
            $created = $row->{created};
        }
        my $release_date = getReleaseDate($created, $q->param('release_date'));
        $q->param('release_date'=>$release_date);

        # Now final checking
        my %vars = $q->Vars;

        my ($dupe,$matches) = (0,0);
        if ($isNewEntry) {
            $dupe = $dbt->checkDuplicates($COLLECTIONS,\%vars);
#          $matches = $dbt->checkNearMatch($COLLECTIONS,$COLLECTION_NO,$q,99,"something=something?");
        }

        if ($dupe) {
            $collection_no = $dupe;
        } elsif ($matches) {
            # Nothing to do, page generation and form processing handled
            # in the checkNearMatch function
        } else {
            if ($isNewEntry) {
                my ($status,$coll_id) = $dbt->insertRecord($s,$COLLECTIONS, \%vars);
                $collection_no = $coll_id;
                # needed by inventoryInfo
                $q->param($COLLECTION_NO => $coll_id);
            } else {
                my $status = $dbt->updateRecord($s,$COLLECTIONS,$COLLECTION_NO,$collection_no,\%vars);
            }
        }

	# if numerical dates were entered, set the best-matching interval no
	my $ma;
	if ( $q->param('direct_ma') > 0 )	{
		my $no = setMaIntervalNo($dbt,$dbh,$collection_no,$q->param('direct_ma'),$q->param('direct_ma_unit'),$q->param('direct_ma'),$q->param('direct_ma_unit'));
	}
	elsif ( $q->param('max_ma') > 0 || $q->param('min_ma')> 0 )	{
		my $no = setMaIntervalNo($dbt,$dbh,$collection_no,$q->param('max_ma'),$q->param('max_ma_unit'),$q->param('min_ma'),$q->param('min_ma_unit'));
	} elsif ( $DB ne "eco" )	{
		setMaIntervalNo($dbt,$dbh,$collection_no);
	}
            
        # Secondary ref handling.  Handle this after updating the collection or it'll mess up
        if ($secondary) {
            # The updateRecord() logic will take care of putting in the new primary
            # reference for the collection
            # Now, put the old primary ref into the secondary ref table
            setSecondaryRef($dbt, $collection_no, $reference_no);
            # and remove the new primary from the secondary table
            deleteRefAssociation($dbt, $collection_no, $secondary);
        }
        # If the current session ref isn't being made the primary, and it's not
        # currently a secondary, add it as a secondary ref for the collection 
        # (this query param doesn't show up if session ref is already a 2ndary.)
        if($q->param('add_session_ref') eq 'YES'){
            my $session_ref = $s->get("reference_no");
            if($session_ref != $secondary) {
                setSecondaryRef($dbt, $collection_no, $session_ref);
            }
        }
        # Delete secondary ref associations
        my @refs_to_delete = $q->param("delete_ref");
        dbg("secondary ref associations to delete: @refs_to_delete<br>");
        if(scalar @refs_to_delete > 0){
            foreach my $ref_no (@refs_to_delete){
                # check if any occurrences with this ref are tied to the collection
                if(refIsDeleteable($dbt, $collection_no, $ref_no)){
                    # removes secondary_refs association between the numbers.
                    dbg("removing secondary ref association (col,ref): $collection_no, $ref_no<br>");
                    deleteRefAssociation($dbt, $collection_no, $ref_no);
                }
            }
        }

        my $record_type = ($DB ne "eco") ? "Collection" : "Inventory";
        my $verb = ($isNewEntry) ? "added" : "updated";
        print "<center><p class=\"pageTitle\" style=\"margin-bottom: -0.5em;\"><font color='red'>$record_type record $verb</font></p><p class=\"medium\"><i>Do not hit the back button!</i></p></center>";

	my $coll;
	if ( $DB ne "eco" )	{
        	my ($colls_ref) = getCollections($dbt,$s,{$COLLECTION_NO=>$collection_no},['authorizer','enterer','modifier','*']);
        	$coll = $colls_ref->[0];
	} else	{
        	my ($colls_ref) = getCollections($dbt,$s,{$COLLECTION_NO=>$collection_no},['*']);
        	$coll = $colls_ref->[0];
	}

        if ($coll && $DB ne "eco") {
            
            # If the viewer is the authorizer (or it's me), display the record with edit buttons
            my $links = '<p><div align="center"><table><tr><td>';
            my $p = Permissions->new($s,$dbt);
            my $can_modify = $p->getModifierList();
            $can_modify->{$s->get('authorizer_no')} = 1;
            
            if ($can_modify->{$coll->{'authorizer_no'}} || $s->isSuperUser) {
                $links .= qq|<li><a href="$WRITE_URL?action=displayCollectionForm&collection_no=$collection_no">Edit this collection</a></li>|;
            }
            $links .= qq|<li><a href="$WRITE_URL?action=displayCollectionForm&prefill_collection_no=$collection_no">Add a collection copied from this one</a></li>|;
            if ($isNewEntry) {
                $links .= qq|<li><a href="$WRITE_URL?action=displaySearchCollsForAdd&type=add">Add another collection with the same reference</a></li>|;
            } else {
                $links .= qq|<li><a href="$WRITE_URL?action=displaySearchCollsForAdd&type=add">Add a collection with the same reference</a></li>|;
                $links .= qq|<li><a href="$WRITE_URL?action=displaySearchColls&type=edit">Edit another collection with the same reference</a></li>|;
                $links .= qq|<li><a href="$WRITE_URL?action=displaySearchColls&type=edit&use_primary=yes">Edit another collection using its own reference</a></li>|;
            }
            $links .= qq|<li><a href="$WRITE_URL?action=displayOccurrenceAddEdit&collection_no=$collection_no">Edit taxonomic list</a></li>|;
            $links .= qq|<li><a href="$WRITE_URL?action=displayOccurrenceListForm&collection_no=$collection_no">Paste in taxonomic list</a></li>|;
            $links .= qq|<li><a href="$WRITE_URL?action=displayCollResults&type=occurrence_table&reference_no=$coll->{reference_no}">Edit occurrence table for collections from the same reference</a></li>|;
            if ( $s->get('role') =~ /authorizer|student|technician/ )	{
                $links .= qq|<li><a href="$WRITE_URL?action=displayOccsForReID&collection_no=$collection_no">Reidentify taxa</a></li>|;
            }
            $links .= "</td></tr></table></div></p>";

            $coll->{'collection_links'} = $links;

            displayCollectionDetailsPage($dbt,$hbo,$q,$s,$coll);

        } elsif ($coll && $DB eq "eco") {
		$q->param('inventory_no' => $coll->{'inventory_no'});
		inventoryInfo($dbt,$q,$s,$hbo,inventoryEditLinks($collection_no));
        }
    }
}

sub inventoryEditLinks	{
	my $no = $_[0];
	my $links = qq|<div class="medium"><ul>
	<li><a href="$WRITE_URL?action=menu">Go to the data entry menu</a></li>
	<li><a href="$WRITE_URL?action=inventoryForm&inventory_no=$no">Edit this inventory</a></li>
	<li><a href="$WRITE_URL?action=inventoryForm&type=add">Add another inventory</a></li>
	<li><a href="$WRITE_URL?action=inventoryForm&prefill_inventory_no=$no">Add an inventory copied from this one</a></li>
	<li><a href="$WRITE_URL?action=displayOccurrenceAddEdit&inventory_no=$no">Edit species list</a></li>
	<li><a href="$WRITE_URL?action=displayOccurrenceListForm&inventory_no=$no">Paste in species list</a></li>
	</ul></div>
|;
	return $links;
}

# Set the release date
# originally written by Ederer; made a separate function by JA 26.6.02
sub getReleaseDate	{
	my ($createdDate,$releaseDateString) = @_;
	my $releaseDate = date($createdDate);

	if ( $releaseDateString eq 'three months')	{
		$releaseDate = $releaseDate+'3M';
	} elsif ( $releaseDateString eq 'six months')	{
		$releaseDate = $releaseDate+'6M';
	} elsif ( $releaseDateString eq 'one year')	{
		$releaseDate = $releaseDate+'1Y';
	} elsif ( $releaseDateString eq 'two years') {
		$releaseDate = $releaseDate+'2Y';
	} elsif ( $releaseDateString eq 'three years')	{
		$releaseDate = $releaseDate+'3Y';
	} elsif ( $releaseDateString eq 'four years')	{
        	$releaseDate = $releaseDate+'4Y';
	} elsif ( $releaseDateString eq 'five years')	{
		$releaseDate = $releaseDate+'5Y';
	}
	# Else immediate release
	return $releaseDate;
}

sub getReleaseString	{
	my ($created_date,$releaseDate) = @_;
	my $createdDate = date($created_date);
	my $releaseDate = date($releaseDate);
	my $releaseDateString = "immediate";

	if ( $releaseDate == $createdDate+'3M' )	{
		$releaseDateString = 'three months';
	} elsif ( $releaseDate == $createdDate+'6M' )	{
		$releaseDateString = 'six months';
	} elsif ( $releaseDate == $createdDate+'1Y' )	{
		$releaseDateString = 'one year';
	} elsif ( $releaseDate == $createdDate+'2Y' )	{
		$releaseDateString = 'two years';
	} elsif ( $releaseDate == $createdDate+'3Y' )	{
		$releaseDateString = 'three years';
        } elsif ( $releaseDate == $createdDate+'4Y' )	{
		$releaseDateString = 'four years';
	} elsif ( $releaseDate == $createdDate+'5Y' )	{
		$releaseDateString = 'five years';
	}
	# Else immediate release
	return $releaseDateString;
}

# Make this more thorough in the future
sub validateCollectionForm {
	my ($dbt,$q,$s) = @_;
	my $is_valid = 1;
	unless($q->param('max_interval') || $DB eq "eco")	{
		print "<center><p>The time interval field is required!</p>\n<p>Please go back and specify the time interval for this collection</p></center>";
		print "<br><br>";
		$is_valid = 0;
	}
	return $is_valid;
}


# JA 15.11.10
# records the narrowest interval that includes the direct Ma values entered on the collection form
# it's useful to know this because the enterer may have put in interval names that are either more
#  broad than necessary or in outright conflict with the numerical values
sub setMaIntervalNo	{
	my ($dbt,$dbh,$coll,$max,$max_unit,$min,$min_unit) = @_;
	my $sql;
	if ( $max < $min || ! $max || ! $min )	{
		$sql = "UPDATE collections SET modified=modified,ma_interval_no=NULL WHERE collection_no=$coll";
		$dbh->do($sql);
		return 0;
	}

	# units matter! JA 25.3.11
	if ( $max_unit =~ /ka/i )	{
		$max /= 1000;
	} elsif ( $max_unit =~ /ybp/i )	{
		$max /= 1000000;
	}
	if ( $min_unit =~ /ka/i )	{
		$min /= 1000;
	} elsif ( $min_unit =~ /ybp/i )	{
		$min /= 1000000;
	}

	# users will want a stage name if possible
	$sql = "SELECT interval_no FROM interval_lookup WHERE lower_boundary>$max AND upper_boundary<$min AND stage_no>0 ORDER BY lower_boundary-upper_boundary";
	my $no = ${$dbt->getData($sql)}[0]->{'interval_no'};
	if ( $no == 0 )	{
		$sql = "SELECT interval_no FROM interval_lookup WHERE lower_boundary>$max AND upper_boundary<$min AND subepoch_no>0 ORDER BY lower_boundary-upper_boundary";
		$no = ${$dbt->getData($sql)}[0]->{'interval_no'};
	}
	if ( $no == 0 )	{
		$sql = "SELECT interval_no FROM interval_lookup WHERE lower_boundary>$max AND upper_boundary<$min AND epoch_no>0 ORDER BY lower_boundary-upper_boundary";
		$no = ${$dbt->getData($sql)}[0]->{'interval_no'};
	}
	if ( $no == 0 )	{
		$sql = "SELECT interval_no FROM interval_lookup WHERE lower_boundary>$max AND upper_boundary<$min ORDER BY lower_boundary-upper_boundary";
		$no = ${$dbt->getData($sql)}[0]->{'interval_no'};
	}
	if ( $no > 0 )	{
		$sql = "UPDATE collections SET modified=modified,ma_interval_no=$no WHERE collection_no=$coll";
		$dbh->do($sql);
		return 1;
	} else	{
		$sql = "UPDATE collections SET modified=modified,ma_interval_no=NULL WHERE collection_no=$coll";
		$dbh->do($sql);
		return 0;
	}
}


#  * User selects a collection from the displayed list
#  * System displays selected collection
sub displayCollectionDetails {
	my ($dbt,$q,$s,$hbo) = @_;
	my $dbh = $dbt->dbh;
	if ( PBDBUtil::checkForBot() )	{
		basicCollectionInfo($dbt,$q,$s,$hbo,'',1);
	}

	my $collection_no = int($q->param('collection_no'));

    # Handles the meat of displaying information about the colleciton
    # Separated out so it can be reused in enter/edit collection confirmation forms
    # PS 2/19/2006
    if ($collection_no !~ /^\d+$/) {
        print Debug::printErrors(["Invalid collection number $collection_no"]);
        return;
    }

	# grab the entire person table and work with a lookup hash because
	#  person is tiny JA 2.10.09
	my %name = %{PBDBUtil::getPersonLookup($dbt)};

	my $sql = "SELECT * FROM collections WHERE collection_no=" . $collection_no;
	my @rs = @{$dbt->getData($sql)};
	my $coll = $rs[0];
	$coll->{authorizer} = $name{$coll->{authorizer_no}};
	$coll->{enterer} = $name{$coll->{enterer_no}};
	$coll->{modifier} = $name{$coll->{modifier_no}};
	if (!$coll ) {
		print Debug::printErrors(["No collection with collection number $collection_no"]);
		return;
	}

    my $page_vars = {};
    if ( $coll->{'research_group'} =~ /ETE/ && $q->param('guest') eq '' )	{
        $page_vars->{ete_banner} = "<div style=\"padding-left: 0em; padding-right: 2em; float: left;\"><a href=\"http://www.mnh.si.edu/ETE\"><img alt=\"ETE\" src=\"/public/bannerimages/ete_logo.jpg\"></a></div>";
    }
    print $hbo->stdIncludes($PAGE_TOP, $page_vars);

    $coll = formatCoordinate($s,$coll);

    # Handle display of taxonomic list now
    # don't even let bots see the lists because they will index the taxon
    #  pages returned by TaxonInfo anyway JA 2.10.09
    my $taxa_list = buildTaxonomicList($dbt,$hbo,$s,{'collection_no'=>$coll->{'collection_no'},'hide_reference_no'=>$coll->{'reference_no'}});
    $coll->{'taxa_list'} = $taxa_list;

    my $links = "<div class=\"verysmall\">";

    # Links at bottom
    if ($s->isDBMember()) {
        $links .= '<p><div align="center">';
        my $p = Permissions->new($s,$dbt);
        my $can_modify = $p->getModifierList();
        $can_modify->{$s->get('authorizer_no')} = 1;

        if ($can_modify->{$coll->{'authorizer_no'}} || $s->isSuperUser) {  
            $links .= qq|<a href="$WRITE_URL?action=displayCollectionForm&collection_no=$collection_no">Edit collection</a> - |;
        }
        $links .=  qq|<a href="$WRITE_URL?action=displayCollectionForm&prefill_collection_no=$collection_no">Add a collection copied from this one</a>|;  
        $links .= "</div></p>";
    }
    $links .= "</div>\n";

    $coll->{'collection_links'} = $links;

    displayCollectionDetailsPage($dbt,$hbo,$q,$s,$coll);

	print $hbo->stdIncludes($PAGE_BOTTOM);
}

# split out of displayCollectionDetails JA 6.11.09
sub formatCoordinate	{

    my ($s,$coll) = @_;

    # if the user is not logged in, round off the degrees
    # DO NOT mess with this routine, because Joe Public must not be
    #  able to locate a collection in the field and pillage it
    # JA 10.5.07
    if ( ! $s->isDBMember() )	{
        if ( ! $coll->{'lngdec'} && $coll->{'lngmin'} )	{
            $coll->{'lngdec'} = ( $coll->{'lngmin'} / 60 ) + ( $coll->{'lngsec'}  / 3600 );
        } else	{
            $coll->{'lngdec'} = "0." . $coll->{'lngdec'};
        }
        if ( ! $coll->{'latdec'} && $coll->{'latmin'} )	{
            $coll->{'latdec'} = ( $coll->{'latmin'} / 60 ) + ( $coll->{'latsec'}  / 3600 );
        } else	{
            $coll->{'latdec'} = "0." . $coll->{'latdec'};
        }
        $coll->{'lngdec'} = int ( ( $coll->{'lngdec'} + 0.05 ) * 10 );
        $coll->{'latdec'} = int ( ( $coll->{'latdec'} + 0.05 ) * 10 );
        if ( $coll->{'lngdec'} == 10 )	{
            $coll->{'lngdeg'}++;
            $coll->{'lngdec'} = 0;
        }
        if ( $coll->{'latdec'} == 10 )	{
            $coll->{'latdeg'}++;
            $coll->{'latdec'} = 0;
        }
        $coll->{'lngmin'} = '';
        $coll->{'lngsec'} = '';
        $coll->{'latmin'} = '';
        $coll->{'latsec'} = '';
        $coll->{'geogcomments'} = '';
    }
    $coll->{'paleolatdir'} = "North";
    if ( $coll->{'paleolat'} < 0 )	{
        $coll->{'paleolatdir'} = "South";
    }
    $coll->{'paleolngdir'} = "East";
    if ( $coll->{'paleolng'} < 0 )	{
        $coll->{'paleolngdir'} = "West";
    }
    $coll->{'paleolat'} = sprintf "%.1f&deg;",abs($coll->{'paleolat'});
    $coll->{'paleolng'} = sprintf "%.1f&deg;",abs($coll->{'paleolng'});

    return $coll;
}

# JA 25.5.11
sub fromMinSec	{
	my ($deg,$min,$sec) = @_;
	$deg =~ s/[^0-9]//g;
	$min =~ s/[^0-9]//g;
	$sec =~ s/[^0-9]//g;
	my $dec = $deg + $min/60 + $sec/3600;
	my $format = "minutes";
	if ( $sec ne "" )	{
		$format = "seconds";
	}
	return ($dec,$format);
}

# JA 25.5.11
sub toMinSec	{
	my ($deg,$dec) = split /\./,$_[0];
	$dec = ".".$dec;
	my $min = int($dec * 60);
	my $sec = int($dec *3600 - $min * 60);
	return ($deg,$min,$sec);
}

sub displayCollectionDetailsPage {
    my ($dbt,$hbo,$q,$s,$row) = @_;
    my $dbh = $dbt->dbh;
    my $collection_no = $row->{'collection_no'};
    return if (!$collection_no);

    # Get the reference
    if ($row->{'reference_no'}) {
        $row->{'reference_string'} = '';
        my $ref = Reference::getReference($dbt,$row->{'reference_no'});
        my $formatted_primary = Reference::formatLongRef($ref);
        $row->{'reference_string'} = '<table cellspacing="0" cellpadding="2" width="100%"><tr>'.
            "<td valign=\"top\"><a href=\"$READ_URL?action=displayReference&reference_no=$row->{reference_no}\">".$row->{'reference_no'}."</a></td>".
            "<td valign=\"top\"><span class=red>$ref->{project_name} $ref->{project_ref_no}</span></td>".
            "<td>$formatted_primary</td>".
            "</tr></table>";
        
        $row->{'secondary_reference_string'} = '';
        my @secondary_refs = Reference::getSecondaryRefs($dbt,$collection_no);
        if (@secondary_refs) {
            my $table = "";
            $table .= '<table cellspacing="0" cellpadding="2" width="100%">';
            for(my $i=0;$i < @secondary_refs;$i++) {
                my $sr = $secondary_refs[$i];
                my $ref = Reference::getReference($dbt,$sr);
                my $formatted_secondary = Reference::formatLongRef($ref);
                my $class = ($i % 2 == 0) ? 'class="darkList"' : '';
                $table .= "<tr $class>".
                    "<td valign=\"top\"><a href=\"$READ_URL?action=displayReference&reference_no=$sr\">$sr</a></td>".
                    "<td valign=\"top\"><span class=red>$ref->{project_name} $ref->{project_ref_no}</span></td>".
                    "<td>$formatted_secondary</td>".
                    "</tr>";
            }
            $table .= "</table>";
            $row->{'secondary_reference_string'} = $table;
        }
    }


        my $sql;

	# Get any subset collections JA 25.6.02
	$sql = "SELECT collection_no FROM collections where collection_subset=" . $collection_no;
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
    $sth->execute();
    my @subrowrefs = @{$sth->fetchall_arrayref()};
    $sth->finish();
    my @links = ();
    foreach my $ref (@subrowrefs)	{
      push @links, "<a href=\"$READ_URL?action=displayCollectionDetails&collection_no=$ref->[0]\">$ref->[0]</a>";
    }
    my $subString = join(", ",@links);
    $row->{'subset_string'} = $subString;

    my $sql1 = "SELECT DISTINCT authorizer_no, enterer_no, modifier_no FROM occurrences WHERE collection_no=" . $collection_no;
    my $sql2 = "SELECT DISTINCT authorizer_no, enterer_no, modifier_no FROM reidentifications WHERE collection_no=" . $collection_no;
    my @names = (@{$dbt->getData($sql1)},@{$dbt->getData($sql2)});
    my %lookup = %{PBDBUtil::getPersonLookup($dbt)};
    if (@names) {
        my %unique_auth = ();
        my %unique_ent = ();
        my %unique_mod = ();
        foreach (@names) {
            $unique_auth{$lookup{$_->{'authorizer_no'}}}++;
            $unique_ent{$lookup{$_->{'enterer_no'}}}++;
            $unique_mod{$lookup{$_->{'modifier_no'}}}++ if ($_->{'modifier'});
        }
        delete $unique_auth{$row->{'authorizer'}};
        delete $unique_ent{$row->{'enterer'}};
        delete $unique_mod{$row->{'modifier'}};
        $row->{'authorizer'} .= ", $_" for (keys %unique_auth);
        $row->{'enterer'} .= ", $_" for (keys %unique_ent);
        $row->{'modifier'} .= ", $_" for (keys %unique_mod);
        # many collections have no modifier, so the initial comma needs to be
        #  stripped off
        $row->{'modifier'} =~ s/^, //;
    }

	# get the max/min interval names
	$row->{'interval'} = '';
	if ( $row->{'max_interval_no'} ) {
		$sql = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $row->{'max_interval_no'};
        my $max_row = ${$dbt->getData($sql)}[0];
        $row->{'interval'} .= qq|<a href="$READ_URL?action=displayInterval&interval_no=$row->{max_interval_no}">|;
        $row->{'interval'} .= $max_row->{'eml_interval'}." " if ($max_row->{'eml_interval'});
        $row->{'interval'} .= $max_row->{'interval_name'};
        $row->{'interval'} .= '</a>';
	} 

	if ( $row->{'min_interval_no'}) {
		$sql = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $row->{'min_interval_no'};
        my $min_row = ${$dbt->getData($sql)}[0];
        $row->{'interval'} .= " - ";
        $row->{'interval'} .= qq|<a href="$READ_URL?action=displayInterval&interval_no=$row->{min_interval_no}">|;
        $row->{'interval'} .= $min_row->{'eml_interval'}." " if ($min_row->{'eml_interval'});
        $row->{'interval'} .= $min_row->{'interval_name'};
        $row->{'interval'} .= '</a>';

        if (!$row->{'max_interval_no'}) {
            $row->{'interval'} .= " <span class=small>(minimum)</span>";
        }
	} 
    my $time_place = $row->{'collection_name'}.": ";
    $time_place .= "$row->{interval}";
    if ($row->{'state'} && $row->{country} eq "United States") {
        $time_place .= ", $row->{state}";
    } elsif ($row->{'country'}) {
        $time_place .= ", $row->{country}";
    }
    if ( $row->{'collectors'} || $row->{'collection_dates'} ) {
        $time_place .= "<br><small>collected ";
        if ( $row->{'collectors'} ) {
            my $collectors = $row->{'collectors'};
            $time_place .= " by " .$collectors . " ";
        }
        if ( $row->{'collection_dates'} ) {
            my $years = $row->{'collection_dates'};
            $years =~ s/[A-Za-z\.]//g;
            $years =~ s/\b[0-9]([0-9]|)\b//g;
            $years =~ s/^( |),//;
            $time_place .= $years;
        }
        $time_place .= "</small>";
    }
    $row->{'collection_name'} = $time_place;

    my @intervals = ();
    push @intervals, $row->{'max_interval_no'} if ($row->{'max_interval_no'});
    push @intervals, $row->{'min_interval_no'} if ($row->{'min_interval_no'} && $row->{'min_interval_no'} != $row->{'max_interval_no'});
    my $max_lookup;
    my $min_lookup;
    if (@intervals) { 
        my $t = new TimeLookup($dbt);
        my $lookup = $t->lookupIntervals(\@intervals);
        $max_lookup = $lookup->{$row->{'max_interval_no'}};
        if ($row->{'min_interval_no'}) { 
            $min_lookup = $lookup->{$row->{'min_interval_no'}};
        } else {
            $min_lookup=$max_lookup;
        }
    }
    if ($max_lookup->{'lower_boundary'} && $min_lookup->{'upper_boundary'}) {
        my @boundaries = ($max_lookup->{'lower_boundary'},$max_lookup->{'upper_boundary'},$min_lookup->{'lower_boundary'},$min_lookup->{'upper_boundary'});
        @boundaries = sort {$b <=> $a} @boundaries;
        # Get rid of extra trailing zeros
        $boundaries[0] =~ s/(\.0|[1-9])(0)*$/$1/;
        $boundaries[-1] =~ s/(\.0|[1-9])(0)*$/$1/;
        $row->{'age_range'} = $boundaries[0]." - ".$boundaries[-1]." m.y. ago";
    } else {
        $row->{'age_range'} = "";
    }
    if ( $row->{'direct_ma'} )	{
        $row->{'age_estimate'} .= $row->{'direct_ma'};
        if ( $row->{'direct_ma_error'} )	{
            $row->{'age_estimate'} .= " &plusmn; " . $row->{'direct_ma_error'};
        }
        $row->{'age_estimate'} .= " ".$row->{'direct_ma_unit'}." (" . $row->{'direct_ma_method'} . ")";
    }
    my $link;
    my $endlink;
    if ( $row->{'max_ma'} )	{
        if ( ! $row->{'min_ma'} )	{
            $row->{'age_estimate'} .= "maximum ";
        }
        $row->{'age_estimate'} .= $row->{'max_ma'};
        if ( $row->{'max_ma_error'} )	{
            $row->{'age_estimate'} .= " &plusmn; " . $row->{'max_ma_error'};
        }
	if ( $row->{'max_ma_method'} eq "AEO" )	{
            $link = qq|<a href="$READ_URL?a=explainAEOestimate&amp;collection_no=$row->{'collection_no'}">|;
            $endlink = "</a>";
        }
        if ( $row->{'min_ma'} && $row->{'max_ma_method'} ne $row->{'min_ma_method'} )	{
            $row->{'age_estimate'} .= " ".$row->{'max_ma_unit'}." ($link" . $row->{'max_ma_method'} . "$endlink)";
        }
    }
    if ( $row->{'min_ma'} && ( ! $row->{'max_ma'} || $row->{'min_ma'} ne $row->{'max_ma'} || $row->{'min_ma_method'} ne $row->{'max_ma_method'} ) )	{
        if ( ! $row->{'max_ma'} )	{
            $row->{'age_estimate'} .= "minimum ";
        } else	{
            $row->{'age_estimate'} .= " to ";
        }
        $row->{'age_estimate'} .= $row->{'min_ma'};
        if ( $row->{'min_ma_error'} )	{
            $row->{'age_estimate'} .= " &plusmn; " . $row->{'min_ma_error'};
        }
        $row->{'age_estimate'} .= " ".$row->{'min_ma_unit'}." ($link" . $row->{'min_ma_method'} . "$endlink)";
    } elsif ( $row->{'age_estimate'} && $row->{'max_ma_method'} ne "" )	{
        $row->{'age_estimate'} .= " ".$row->{'max_ma_unit'}." ($link" . $row->{'max_ma_method'} . "$endlink)";
    }
    foreach my $term ("period","epoch","stage") {
        $row->{$term} = "";
        if ($max_lookup->{$term."_name"} &&
            $max_lookup->{$term."_name"} eq $min_lookup->{$term."_name"}) {
            $row->{$term} = $max_lookup->{$term."_name"};
        }
    }
    if ($max_lookup->{"ten_my_bin"} &&
        $max_lookup->{"ten_my_bin"} eq $min_lookup->{"ten_my_bin"}) {
        $row->{"ten_my_bin"} = $max_lookup->{"ten_my_bin"};
    } else {
        $row->{"ten_my_bin"} = "";
    }

    $row->{"zone_type"} =~ s/(^.)/\u$1/;

	# check whether we have period/epoch/locage/intage max AND/OR min:
    if ($s->isDBMember()) {
        foreach my $term ("epoch","intage","locage","period"){
            $row->{'legacy_'.$term} = '';
            if ($row->{$term."_max"}) {
                if ($row->{'eml'.$term.'_max'}) {
                    $row->{'legacy_'.$term} .= $row->{'eml'.$term.'_max'}." ";
                }
                $row->{'legacy_'.$term} .= $row->{$term."_max"};
            }
            if ($row->{$term."_min"}) {
                if ($row->{$term."_max"}) {
                    $row->{'legacy_'.$term} .= " - ";
                }
                if ($row->{'eml'.$term.'_min'}) {
                    $row->{'legacy_'.$term} .= $row->{'eml'.$term.'_min'}." ";
                }
                $row->{'legacy_'.$term} .= $row->{$term."_min"};
                if (!$row->{$term."_max"}) {
                    $row->{'legacy_'.$term} .= " <span class=small>(minimum)</span>";
                }
            }
        }
    }
    if ($row->{'legacy_period'} eq $row->{'period'}) {
        $row->{'legacy_period'} = '';
    }
    if ($row->{'legacy_epoch'} eq $row->{'epoch'}) {
        $row->{'legacy_epoch'} = '';
    }
    if ($row->{'legacy_locage'} eq $row->{'stage'}) {
        $row->{'legacy_locage'} = '';
    }
    if ($row->{'legacy_intage'} eq $row->{'stage'}) {
        $row->{'legacy_intage'} = '';
    }
    if ($row->{'legacy_epoch'} ||
        $row->{'legacy_period'} ||
        $row->{'legacy_intage'} ||
        $row->{'legacy_locage'}) {
        $row->{'legacy_message'} = 1;
    } else {
        $row->{'legacy_message'} = '';
    }

    if ($row->{'interval'} eq $row->{'period'} ||
        $row->{'interval'} eq $row->{'epoch'} ||
        $row->{'interval'} eq $row->{'stage'}) {
        $row->{'interval'} = '';
    }


    if ($row->{'collection_subset'}) {
        $row->{'collection_subset'} =  "<a href=\"$READ_URL?action=displayCollectionDetails&collection_no=$row->{collection_subset}\">$row->{collection_subset}</a>";
    }

    if ($row->{'regionalsection'}) {
        $row->{'regionalsection'} = "<a href=\"$READ_URL?action=displayStratTaxaForm&taxon_resolution=species&skip_taxon_list=YES&input_type=regional&input=".uri_escape($row->{'regionalsection'})."\">$row->{regionalsection}</a>";
    }

    if ($row->{'localsection'}) {
        $row->{'localsection'} = "<a href=\"$READ_URL?action=displayStratTaxaForm&taxon_resolution=species&skip_taxon_list=YES&input_type=local&input=".uri_escape($row->{'localsection'})."\">$row->{localsection}</a>";
    }
    if ($row->{'member'}) {
        $row->{'member'} = "<a href=\"$READ_URL?action=displayStrata&group_hint=".uri_escape($row->{'geological_group'})."&formation_hint=".uri_escape($row->{'formation'})."&group_formation_member=".uri_escape($row->{'member'})."\">$row->{member}</a>";
    }
    if ($row->{'formation'}) {
        $row->{'formation'} = "<a href=\"$READ_URL?action=displayStrata&group_hint=".uri_escape($row->{'geological_group'})."&group_formation_member=".uri_escape($row->{'formation'})."\">$row->{formation}</a>";
    }
    if ($row->{'geological_group'}) {
        $row->{'geological_group'} = "<a href=\"$READ_URL?action=displayStrata&group_formation_member=".uri_escape($row->{'geological_group'})."\">$row->{geological_group}</a>";
    }

    $row->{'modified'} = date($row->{'modified'});

    # textarea values often have returns that need to be rendered
    #  as <br>s JA 20.8.06
    for my $r ( keys %$row )	{
        if ( $r !~ /taxa_list/ && $r =~ /comment/ )	{
            $row->{$r} =~ s/\n/<br>/g;
        }
    }
    print $hbo->populateHTML('collection_display_fields', $row);

} # end sub displayCollectionDetails()


# builds the list of occurrences shown in places such as the collections form
# must pass it the collection_no
# reference_no (optional or not?? - not sure).
#
# optional arguments:
#
# gnew_names	:	reference to array of new genus names the user is entering (from the form)
# subgnew_names	:	reference to array of new subgenus names the user is entering
# snew_names	:	reference to array of new species names the user is entering
sub buildTaxonomicList {
	my ($dbt,$hbo,$s,$options) = @_;
	my %options = ();
	if ($options)	{
		%options = %{$options};
	}

	# dereference arrays.
	my @gnew_names = @{$options{'new_genera'}} if ($options{'new_genera'});
	my @subgnew_names = @{$options{'new_subgenera'}} if ($options{'new_subgenera'}) ;
	my @snew_names = @{$options{'new_species'}} if ($options{'new_species'});
	
	my $new_found = 0;		# have we found new taxa?  (ie, not in the database)
	my $return = "";

	# This is the taxonomic list part
	# join with taxa_tree_cache because lft and rgt will be used to
	#  order the list JA 13.1.07
	my $treefields = ", lft, rgt";
	my $sqlstart = "SELECT abund_value, abund_unit, genus_name, genus_reso, subgenus_name, subgenus_reso, plant_organ, plant_organ2, species_name, species_reso, comments, reference_no, occurrence_no, o.taxon_no taxon_no, collection_no";

	my $sqlmiddle;
	my $sqlend;
	if ($options{'collection_no'}) {
		$sqlmiddle = " FROM occurrences o ";
		$sqlend .= "AND collection_no=$options{'collection_no'}";
	} elsif ($options{'inventory_no'}) {
		$sqlstart = "SELECT reference_no,inventory_no,entry_no,o.taxon_no,genus_name,species_name,abund_value,o.mass,comments";
		$sqlmiddle = " FROM inventory_entries o ";
		$sqlend .= "AND inventory_no=$options{'inventory_no'}";
	} elsif ($options{'occurrence_list'} && @{$options{'occurrence_list'}}) {
		$sqlend .= "AND occurrence_no IN (".join(', ',@{$options{'occurrence_list'}}).") ORDER BY occurrence_no";
	} else	{
		$sqlend = "";
	}
	my $sql = $sqlstart . ", lft, rgt" . $sqlmiddle . ", $TAXA_TREE_CACHE t WHERE o.taxon_no=t.taxon_no " . $sqlend;
	my $sql2 = $sqlstart . $sqlmiddle . "WHERE taxon_no=0 " . $sqlend;

	my @warnings;
	if ($options{'warnings'}) {
		@warnings = @{$options{'warnings'}};
	}

	dbg("buildTaxonomicList sql: $sql");

	my @rowrefs;
	if ($sql) {
		@rowrefs = @{$dbt->getData($sql)};
		push @rowrefs , @{$dbt->getData($sql2)};
	}

	if (@rowrefs) {
		my @grand_master_list = ();
		my $are_reclassifications = 0;

		# loop through each row returned by the query
		foreach my $rowref (@rowrefs) {
			my $output = '';
			my %classification = ();


			# If we have specimens
			if ( $rowref->{'occurrence_no'} )	{
				my $sql_s = "SELECT count(*) c FROM specimens WHERE occurrence_no=$rowref->{occurrence_no}";
				my $specimens_measured = ${$dbt->getData($sql_s)}[0]->{'c'};
				if ($specimens_measured) {
    					my $s = ($specimens_measured > 1) ? 's' : '';
    					$rowref->{comments} .= " (<a href=\"$READ_URL?action=displaySpecimenList&occurrence_no=$rowref->{occurrence_no}\">$specimens_measured measurement$s</a>)";
				}
			}
			
			# if the user submitted a form such as adding a new occurrence or 
			# editing an existing occurrence, then we'll bold face any of the
			# new taxa which we don't already have in the database.
            # Bad bug: rewriting the data directly here fucked up all kinds of operations
            # below which expect the taxonomic names to be pure, just set some flags
            # and have stuff interpret them below PS 2006
			
			# check for unrecognized genus names
			foreach my $nn (@gnew_names){
				if ($rowref->{genus_name} eq  $nn) {
					$rowref->{new_genus_name} = 1;
					$new_found++;
				}
			}

			# check for unrecognized subgenus names
			foreach my $nn (@subgnew_names){
				if($rowref->{subgenus_name} eq $nn){
					$rowref->{new_subgenus_name} = 1;
					$new_found++;
				}
			}

			# check for unrecognized species names
			foreach my $nn (@snew_names){
				if($rowref->{species_name} eq $nn){
					$rowref->{new_species_name} = 1;
					$new_found++;
				}
			}

			# tack on the author and year if the taxon number exists
			# JA 19.4.04
			if ( $rowref->{taxon_no} )	{
				my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$rowref->{'taxon_no'}},['taxon_no','taxon_name','common_name','taxon_rank','author1last','author2last','otherauthors','pubyr','reference_no','ref_is_authority']);

				if ($taxon->{'taxon_rank'} =~ /species/ || $rowref->{'species_name'} =~ /^indet\.|^sp\./) {

					my $orig_no = TaxonInfo::getOriginalCombination($dbt,$taxon->{'taxon_no'});
					my $is_recomb = ($orig_no == $taxon->{'taxon_no'}) ? 0 : 1;
					$rowref->{'authority'} = Reference::formatShortRef($taxon,'no_inits'=>1,'link_id'=>$taxon->{'ref_is_authority'},'is_recombination'=>$is_recomb);
				}
			}

			my $formatted_reference = '';

			# if the occurrence's reference differs from the collection's, print it
			my $newrefno = $rowref->{'reference_no'};
			if ($newrefno != $options{'hide_reference_no'})	{
				$rowref->{reference_no} = Reference::formatShortRef($dbt,$newrefno,'no_inits'=>1,'link_id'=>1);
			} else {
				$rowref->{reference_no} = '';
			}
			
			# put all keys and values from the current occurrence
			# into two separate arrays.
			$rowref->{'taxon_name'} = formatOccurrenceTaxonName($rowref);
			$rowref->{'hide_collection_no'} = $options{'collection_no'};
	
			# get the most recent reidentification
			my $mostRecentReID;
			if ( $rowref->{'occurrence_no'} )	{
				$mostRecentReID = PBDBUtil::getMostRecentReIDforOcc($dbt,$rowref->{$OCCURRENCE_NO},1);
			}
			
			# if the occurrence has been reidentified at least once
			#  display the original and reidentifications.
			if ($mostRecentReID) {
				
				# rjp, 1/2004, change this so it displays *all* reidentifications, not just
				# the last one.
                # JA 2.4.04: this was never implemented by Poling, who instead
                #  went renegade and wrote the entirely redundant
		#  HTMLFormattedTaxonomicList; the correct way to do it was
		#  to pass in $rowref->{occurrence_no} and isReidNo = 0
                #  instead of $mostRecentReID and isReidNo = 1
	
				my $show_collection = '';
				my ($table,$classification,$reid_are_reclassifications) = getReidHTMLTableByOccNum($dbt,$hbo,$s,$rowref->{$OCCURRENCE_NO}, 0, $options{'do_reclassify'});
				$are_reclassifications = 1 if ($reid_are_reclassifications);
				$rowref->{'class'} = $classification->{'class'}{'taxon_name'};
				$rowref->{'order'} = $classification->{'order'}{'taxon_name'};
				$rowref->{'family'} = $classification->{'family'}{'taxon_name'};
				$rowref->{'common_name'} = ($classification->{'common_name'}{'taxon_no'});
				if ( ! $rowref->{'class'} && ! $rowref->{'order'} && ! $rowref->{'family'} )	{
					if ( $options{'do_reclassify'} )	{
						$rowref->{'class'} = qq|<span style="color: red;">unclassified</span>|;
					} else	{
						$rowref->{'class'} = "unclassified";
					}
				}
				if ( $rowref->{'class'} && $rowref->{'order'} )	{
					$rowref->{'order'} = "- " . $rowref->{'order'};
				}
				if ( $rowref->{'family'} && ( $rowref->{'class'} || $rowref->{'order'} ) )	{
					$rowref->{'family'} = "- " . $rowref->{'family'};
				}
				$rowref->{'parents'} = $hbo->populateHTML("parent_display_row", $rowref);
				$output = qq|<tr><td colspan="5" style="border-top: 1px solid #E0E0E0;"></td></tr>|;
				$output .= $hbo->populateHTML("taxa_display_row", $rowref);
				$output .= $table;
				
				$rowref->{'class_no'}  = ($classification->{'class'}{'taxon_no'} or 1000000);
				$rowref->{'order_no'}  = ($classification->{'order'}{'taxon_no'} or 1000000);
				$rowref->{'family_no'} = ($classification->{'family'}{'taxon_no'} or 1000000);
				$rowref->{'lft'} = ($classification->{'lft'}{'taxon_no'} or 1000000);
				$rowref->{'rgt'} = ($classification->{'rgt'}{'taxon_no'} or 1000000);
			}
    		# otherwise this occurrence has never been reidentified
			else {
	        	# get the classification (by PM): changed 2.4.04 by JA to
		        #  use the occurrence number instead of the taxon name
                if ($rowref->{'taxon_no'}) {
                    # Get parents
				    my $class_hash = TaxaCache::getParents($dbt,[$rowref->{'taxon_no'}],'array_full');
                    my @class_array = @{$class_hash->{$rowref->{'taxon_no'}}};
                    # Get Self as well, in case we're a family indet.
                    my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$rowref->{'taxon_no'}},['taxon_name','common_name','taxon_rank','pubyr']);
                    unshift @class_array , $taxon;
                    $rowref = getClassOrderFamily(\$rowref,\@class_array);
                    if ( ! $rowref->{'class'} && ! $rowref->{'order'} && ! $rowref->{'family'} )	{
                        $rowref->{'class'} = "unclassified";
                    }
                    $rowref->{'synonym_name'} = getSynonymName($dbt,$rowref->{'taxon_no'},$taxon->{'taxon_name'});
                } else {
                    if ($options{'do_reclassify'}) {
                        $rowref->{'show_classification_select'} = 1;
                        # Give these default values, don't want to pass in possibly undef values to any function or PERL might screw it up
                        my $taxon_name = $rowref->{'genus_name'}; 
                        $taxon_name .= " ($rowref->{'subgenus_name'})" if ($rowref->{'subgenus_name'});
                        $taxon_name .= " $rowref->{'species_name'}";
                        my @all_matches = Taxon::getBestClassification($dbt,$rowref);
                        if (@all_matches) {
                            $are_reclassifications = 1;
                            $rowref->{'classification_select'} = Reclassify::classificationSelect($dbt, $rowref->{$OCCURRENCE_NO},0,1,\@all_matches,$rowref->{'taxon_no'},$taxon_name);
                        }
                    }
                }
				$rowref->{'class_no'} ||= 1000000;
				$rowref->{'order_no'} ||= 1000000;
				$rowref->{'family_no'} ||= 1000000;
				$rowref->{'lft'} ||= 1000000;

				if ( ! $rowref->{'class'} && ! $rowref->{'order'} && ! $rowref->{'family'} )	{
					if ( $options{'do_reclassify'} )	{
						$rowref->{'class'} = qq|<span style="color: red;">unclassified</span>|;
					} else	{
						$rowref->{'class'} = "unclassified";
					}
				}
				if ( $rowref->{'class'} && $rowref->{'order'} )	{
					$rowref->{'order'} = "- " . $rowref->{'order'};
				}
				if ( $rowref->{'family'} && ( $rowref->{'class'} || $rowref->{'order'} ) )	{
					$rowref->{'family'} = "- " . $rowref->{'family'};
				}
				$rowref->{'parents'} = $hbo->populateHTML("parent_display_row", $rowref);
				$output = qq|<tr><td colspan="5" style="border-top: 1px solid #E0E0E0;"></td></tr>|;
				$output .= $hbo->populateHTML("taxa_display_row", $rowref);
			}

	# Clean up abundance values (somewhat messy, but works, and better
	#   here than in populateHTML) JA 10.6.02
			$output =~ s/(>1 specimen)s|(>1 individual)s|(>1 element)s|(>1 fragment)s/$1$2$3$4/g;
	
			$rowref->{'html'} = $output;
			push(@grand_master_list, $rowref);
		}

		# Look at @grand_master_list to see every record has class_no, order_no,
		# family_no,  reference_no, abundance_unit and comments. 
		# If ALL records are missing any of those, don't print the header
		# for it.
		my ($class_nos, $order_nos, $family_nos, $common_names, $lft_nos,
			$reference_nos, $abund_values, $comments) = (0,0,0,0,0,0,0);
		foreach my $row (@grand_master_list) {
			$class_nos++ if($row->{class_no} && $row->{class_no} != 1000000);
			$order_nos++ if($row->{order_no} && $row->{order_no} != 1000000);
			$family_nos++ if($row->{family_no} && $row->{family_no} != 1000000);
			$common_names++ if($row->{common_name});
			$lft_nos++ if($row->{lft} && $row->{lft} != 1000000);
			$reference_nos++ if($row->{reference_no} && $row->{reference_no} != $options{'hide_reference_no'});
			$abund_values++ if($row->{abund_value});
			$comments++ if($row->{comments});
		}
	
        if ($options{'collection_no'}) {
            my $sql = "SELECT c.collection_name,c.country,c.state,concat(i1.eml_interval,' ',i1.interval_name) max_interval, concat(i2.eml_interval,' ',i2.interval_name) min_interval " 
                    . " FROM collections c "
                    . " LEFT JOIN intervals i1 ON c.max_interval_no=i1.interval_no"
                    . " LEFT JOIN intervals i2 ON c.min_interval_no=i2.interval_no"
                    . " WHERE c.collection_no=$options{'collection_no'}";

            my $coll = ${$dbt->getData($sql)}[0];

            # get the max/min interval names
            my $time_place = $coll->{'collection_name'}.": ";
            if ($coll->{'max_interval'} ne $coll->{'min_interval'} && $coll->{'min_interval'}) {
                $time_place .= "$coll->{max_interval} - $coll->{min_interval}";
            } else {
                $time_place .= "$coll->{max_interval}";
            } 
            if ($coll->{'state'} && $coll->{country} eq "United States") {
                $time_place .= ", $coll->{state}";
            } elsif ($coll->{'country'}) {
                $time_place .= ", $coll->{country}";
            } 

        }

	my $list_title = ( $options{'inventory_no'} ) ? "Species list" : "Taxonomic list";

        # Taxonomic list header
        $return = "<div class=\"displayPanel\" align=\"left\">\n" .
                  "  <span class=\"displayPanelHeader\">$list_title</span>\n" .
                  "  <div class=\"displayPanelContent\">\n" ;

		if ($new_found) {
            push @warnings, "Taxon names in <b>bold</b> are new to the occurrences table. Please make sure there aren't any typos. If there are, DON'T hit the back button; click the edit link below.";
		}
        if  ($are_reclassifications) {
            push @warnings, "Some taxa could not be classified because multiple versions of the names (such as homonyms) exist in the database.  Please choose which versions you mean and hit \"Classify taxa.\"";
        }

        if (@warnings) {
            $return .= "<div style=\"margin-left: auto; margin-right: auto; text-align: left;\">";
            $return .= Debug::printWarnings(\@warnings);
            $return .= "<br>";
            $return .= "</div>";
        }

        if ($are_reclassifications) {
            $return .= "<form action=\"$WRITE_URL\" method=\"post\">\n";
            $return .= "<input type=\"hidden\" name=\"action\" value=\"startProcessReclassifyForm\">\n"; 
            if ($options{$COLLECTION_NO}) {
                $return .= "<input type=\"hidden\" name=\"$COLLECTION_NO\" value=\"$options{$COLLECTION_NO}\">\n"; 
            }
        }

	my $table_size= ( $options{'inventory_no'} ) ? "medium" : "tiny";

	$return .= "<table border=\"0\" cellpadding=\"0\" cellspacing=\"0\" class=\"$table_size\"><tr>";

	# Sort:
        my @sorted = ();
        if ($options{'occurrence_list'} && @{$options{'occurrence_list'}}) {
            # Should be sorted in SQL using the same criteria as was made to
            # build the occurrence list (in displayOccsForReID)  Right now this is by occurrence_no, which is being done in sql;
            @sorted = @grand_master_list;
        } else {
            # switched from sorting by taxon nos to sorting by lft rgt
            #  JA 13.1.07
            @sorted = sort{ $a->{lft} <=> $b->{lft} ||
                               $a->{rgt} <=> $b->{rgt} ||
                               $a->{$OCCURRENCE_NO} <=> $b->{$OCCURRENCE_NO} } @grand_master_list;
            #@sorted = sort{ $a->{class_no} <=> $b->{class_no} ||
            #                   $a->{order_no} <=> $b->{order_no} ||
            #                   $a->{family_no} <=> $b->{family_no} ||
            #                   $a->{occurrence_no} <=> $b->{occurrence_no} } @grand_master_list;
            unless ( $lft_nos == 0 )	{
            #unless($class_nos == 0 && $order_nos == 0 && $family_nos == 0 )
                # Now sort the ones that had no taxon_no by occ_no.
                my @occs_to_sort = ();
                while ( $sorted[-1]->{lft} == 1000000 )	{
                    push(@occs_to_sort, pop @sorted);
                }

            # Put occs in order, AFTER the sorted occ with the closest smaller
            # number.  First check if our occ number is one greater than any 
            # existing sorted occ number.  If so, place after it.  If not, find
            # the distance between it and all other occs less than it and then
            # place it after the one with the smallest distance.
                while(my $single = pop @occs_to_sort){
                    my $slot_found = 0;
                    my @variances = ();
                    # First, look for the "easy out" at the endpoints.
                    # Beginning?
                # HMM, if $single is less than $sorted[0] we don't want to put
                # it at the front unless it's less than ALL $sorted[$x].
                    #if($single->{occurrence_no} < $sorted[0]->{occurrence_no} && 
                    #	$sorted[0]->{occurrence_no} - $single->{occurrence_no} == 1){
                    #	unshift @sorted, $single;
                    #}
                    # Can I just stick it at the end?
                    if(($single->{$OCCURRENCE_NO} > $sorted[-1]->{$OCCURRENCE_NO}) &&
                       ($single->{$OCCURRENCE_NO} - $sorted[-1]->{$OCCURRENCE_NO} == 1)){
                        push @sorted, $single;
                    }
                    # Somewhere in the middle
                    else{
                        for(my $index = 0; $index < @sorted-1; $index++){
                            if($single->{$OCCURRENCE_NO} > 
                                            $sorted[$index]->{$OCCURRENCE_NO}){ 
                                # if we find a variance of 1, bingo!
                                if($single->{$OCCURRENCE_NO} -
                                        $sorted[$index]->{$OCCURRENCE_NO} == 1){
                                    splice @sorted, $index+1, 0, $single;
                                    $slot_found=1;
                                    last;
                                }
                                else{
                                    # store the (positive) variance
                                    push(@variances, $single->{$OCCURRENCE_NO}-$sorted[$index]->{$OCCURRENCE_NO});
                                }
                            }
                            else{ # negative variance
                                push(@variances, 1000000);
                            }
                        }
                        # if we didn't find a variance of 1, place after smallest
                        # variance.
                        if(!$slot_found){
                            # end variance:
                            if($sorted[-1]->{$OCCURRENCE_NO}-$single->{$OCCURRENCE_NO}>0){
                                push(@variances,$sorted[-1]->{$OCCURRENCE_NO}-$single->{$OCCURRENCE_NO});
                            }
                            else{ # negative variance
                                push(@variances, 1000000);
                            }
                            # insert where the variance is the least
                            my $smallest = 1000000;
                            my $smallest_index = 0;
                            for(my $counter=0; $counter<@variances; $counter++){
                                if($variances[$counter] < $smallest){
                                    $smallest = $variances[$counter];
                                    $smallest_index = $counter;
                                }
                            }
                            # NOTE: besides inserting according to the position
                            # found above, this will insert an occ less than all other
                            # occ numbers at the very front of the list (the condition
                            # in the loop above will never be met, so $smallest_index
                            # will remain zero.
                            splice @sorted, $smallest_index+1, 0, $single;
                        }
                    }
                }
            }
        }

		my $sorted_html = '';
		my $rows = $#sorted + 2;
		$sorted_html .= qq|
<script language="JavaScript" type="text/javascript">
<!-- Begin

window.onload = hideName;

function addLink(link_id,link_action,taxon_name)	{
	if ( ! /href/.test( document.getElementById(link_id).innerHTML ) )	{
		document.getElementById(link_id).innerHTML = '<a href="$READ_URL?action=basicTaxonInfo' + link_action + '&amp;is_real_user=1">' + taxon_name + '</a>';
	}
}

function hideName()	{
	for (var rowNum=1; rowNum<$rows; rowNum++)	{
		document.getElementById('commonRow'+rowNum).style.visibility = 'hidden';
	}
}

function showName()	{
	document.getElementById('commonClick').style.visibility = 'hidden';
	var commonName = document.getElementsByName("commonName");
	for ( i = 0; i<= commonName.length; i++ )       {
		commonName[i].style.visibility = "visible";
	}
	for (var rowNum=1; rowNum<$rows; rowNum++)	{
		document.getElementById('commonRow'+rowNum).style.visibility = 'visible';
	}
}

-->
</script>
|;
		my $lastparents;
		for(my $index = 0; $index < @sorted; $index++){
			# only the last row needs to have the rowNum inserted
			my $rowNum = $index + 1;
			my @parts = split /commonRow/,$sorted[$index]->{html};
			$parts[$#parts] = $rowNum . $parts[$#parts];
			$sorted[$index]->{html} = join 'commonRow',@parts;

#            $sorted[$index]->{html} =~ s/<td align="center"><\/td>/<td>$sorted[$index]->{occurrence_no}<\/td>/; DEBUG
			if ( $sorted[$index]->{'class'} . $sorted[$index]->{'order'} . $sorted[$index]->{'family'} ne $lastparents )	{
				$sorted_html .= $sorted[$index]->{'parents'};
				$lastparents = $sorted[$index]->{'class'} . $sorted[$index]->{'order'} . $sorted[$index]->{'family'};
			}
			$sorted_html .= $sorted[$index]->{html};
            
		}
		$return .= $sorted_html;

		$return .= qq|<tr><td colspan="5" align="right"><span onClick="showName();" id="commonClick" class="small">see common names</span></td>|;

		$return .= "</table>";
        if ($are_reclassifications) {
            $return .= "<br><input type=\"submit\" name=\"submit\" value=\"Classify taxa\">";
            $return .= "</form>"; 
        }

	$return .= "<div class=\"verysmall\">";
	$return .= '<p><div align="center">';

	if ( $options{'collection_no'} > 0 && ! $options{'save_links'} )	{
	# have to have at least three taxa
		if ( $abund_values > 2 )	{
			$return .= qq|<a href="$READ_URL?action=rarefyAbundances&collection_no=$options{'collection_no'}">Analyze abundance data</a> - |;
		}

		$return .= qq|<a href="$READ_URL?action=displayCollectionEcology&collection_no=$options{'collection_no'}">Tabulate ecology data</a> - |;

		if ($s->isDBMember()) {
			$return .= qq|<a href="$WRITE_URL?action=displayOccurrenceAddEdit&collection_no=$options{'collection_no'}">Edit taxonomic list</a>|;
			if ( $s->get('role') =~ /authorizer|student|technician/ )	{
				$return .= qq| - <a href="$WRITE_URL?action=displayOccsForReID&collection_no=$options{'collection_no'}">Reidentify taxa</a>|;
			}
		}
	} elsif ($s->isDBMember()) {
		$return .= $options{'save_links'};
	}
	$return .= "</div></p>\n</div>\n";

        $return .= "</div>";
        $return .= "</div>";
	} else {
        if (@warnings) {
            $return .= "<div align=\"center\">";
            $return .= Debug::printWarnings(\@warnings);
            $return .= "<br>";
            $return .= "</div>";
        }
    }


    # This replaces blank cells with blank cells that have no padding, so the don't take up
    # space - this way the comments field lines is indented correctly if theres a bunch of empty
    # class/order/family columns sort of an hack but works - PS
    $return =~ s/<td([^>]*?)>\s*<\/td>/<td$1 style=\"padding: 0\"><\/td>/g;
    #$return =~ s/<td(.*?)>\s*<\/td>/<td$1 style=\"padding: 0\"><\/td>/g;
	return $return;
} # end sub buildTaxonomicList()

sub formatOccurrenceTaxonName {
    my $row = shift;
    my $taxon_name = "";

    # Generate the link first
    my $link_id = $row->{'occurrence_no'};
    if ( $row->{'reid_no'} )	{
        $link_id = "R" . $row->{'reid_no'};
    }
    my $link_action;
    if ( $row->{'taxon_no'} > 0 )	{
        $link_action = $row->{'taxon_no'};
        $link_action = "&amp;taxon_no=" . uri_escape($link_action);
    } elsif ($row->{'genus_name'} && $row->{'genus_reso'} !~ /informal/) {
        $link_action = $row->{'genus_name'};

        if ($row->{'subgenus_name'} && $row->{'subgenus_reso'} !~ /informal/) {
            $link_action .= " ($row->{'subgenus_name'})";
        }
        if ($row->{'species_name'} && $row->{'species_reso'} !~ /informal/ && $row->{'species_name'} !~ /^indet\.|^sp\./) {
            $link_action .= " $row->{'species_name'}";
        }
        $link_action = "&amp;taxon_name=" . uri_escape($link_action);
    }


    if ($row->{'species_name'} !~ /^indet/ && $row->{'genus_reso'} !~ /informal/) {
        $taxon_name .= "<i>";
    }

    my $genus_name = $row->{'genus_name'};
    if ($row->{'new_genus_name'}) {
        $genus_name = "<b>".$genus_name."</b>";
    }
    # n. gen., n. subgen., n. sp. come afterwards
    # sensu lato always goes at the very end no matter what JA 3.3.07
    if ($row->{'genus_reso'} eq 'n. gen.' && $row->{'species_reso'} ne 'n. sp.') {
        $taxon_name .= "$genus_name n. gen.";
    } elsif ($row->{'genus_reso'} eq '"') {
        $taxon_name .= '"'.$genus_name;
        $taxon_name .= '"' unless ($row->{'subgenus_reso'} eq '"' || $row->{'species_reso'} eq '"');
    } elsif ($row->{'genus_reso'} && $row->{'genus_reso'} ne 'n. gen.' && $row->{'genus_reso'} ne 'sensu lato') {
        $taxon_name .= $row->{'genus_reso'}." ".$genus_name;
    } else {
        $taxon_name .= $genus_name;
    }

    if ($row->{'subgenus_name'}) {
        my $subgenus_name = $row->{'subgenus_name'};
        if ($row->{'new_subgenus_name'}) {
            $subgenus_name = "<b>".$subgenus_name."</b>";
        }
        $taxon_name .= " (";
        if ($row->{'subgenus_reso'} eq 'n. subgen.') {
            $taxon_name .= "$subgenus_name n. subgen.";
        } elsif ($row->{'subgenus_reso'} eq '"') {
            $taxon_name .= '"' unless ($row->{'genus_reso'} eq '"');
            $taxon_name .= $subgenus_name;
            $taxon_name .= '"' unless ($row->{'species_reso'} eq '"');
        } elsif ($row->{'subgenus_reso'}) {
            $taxon_name .= $row->{'subgenus_reso'}." ".$subgenus_name;
        } else {
            $taxon_name .= $subgenus_name;
        }
        $taxon_name .= ")";
    }

    $taxon_name .= " ";
    my $species_name = $row->{'species_name'};
    if ($row->{'new_species_name'}) {
        $species_name = "<b>".$species_name."</b>";
    }
    if ($row->{'species_reso'} eq '"') {
        $taxon_name .= '"' unless ($row->{'genus_reso'} eq '"' || $row->{'subgenus_reso'} eq '"');
        $taxon_name .= $species_name.'"';
    } elsif ($row->{'species_reso'} && $row->{'species_reso'} ne 'n. sp.' && $row->{'species_reso'} ne 'sensu lato') {
        $taxon_name .= $row->{'species_reso'}." ".$species_name;
    } else {
        $taxon_name .= $species_name;
    }
    #if ($row->{'species_reso'} ne 'n. sp.' && $row->{'species_reso'}) {
    #    $taxon_name .= " ".$row->{'species_reso'};
    #}
    #$taxon_name .= " ".$row->{'species_name'};

    if ($row->{'species_name'} !~ /^indet/ && $row->{'genus_reso'} !~ /informal/) {
        $taxon_name .= "</i>";
    }
    if ($link_id) {
        $taxon_name =~ s/"/&quot;/g;
        $taxon_name = qq|<span class="mockLink" id="$link_id" onMouseOver="addLink('$link_id','$link_action','$taxon_name')">$taxon_name</span>|;
    }
    
    if ($row->{'genus_reso'} eq 'sensu lato' || $row->{'species_reso'} eq 'sensu lato') {
        $taxon_name .= " sensu lato";
    }
    if ($row->{'species_reso'} eq 'n. sp.') {
        if ($row->{'genus_reso'} eq 'n. gen.') {
            $taxon_name .= " n. gen.,";
        }
        $taxon_name .= " n. sp.";
    }
    if ($row->{'plant_organ'} && $row->{'plant_organ'} ne 'unassigned') {
        $taxon_name .= " $row->{plant_organ}";
    }
    if ($row->{'plant_organ2'} && $row->{'plant_organ2'} ne 'unassigned') {
        $taxon_name .= ", " if ($row->{'plant_organ'} && $row->{'plant_organ'} ne 'unassigned');
        $taxon_name .= " $row->{plant_organ2}";
    }

    return $taxon_name;
}

# This is pretty much just used in a couple places above
sub getSynonymName {
    my ($dbt,$taxon_no,$current_taxon_name) = @_;
    return "" unless $taxon_no;

    my $synonym_name = "";

    my $orig_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no);
    my ($ss_taxon_no,$status) = TaxonInfo::getSeniorSynonym($dbt,$orig_no,'','yes');
    my $is_synonym = ($ss_taxon_no != $orig_no && $status =~ /synonym/) ? 1 : 0;
    my $is_spelling = 0;
    my $spelling_reason = "";

    my $spelling = TaxonInfo::getMostRecentSpelling($dbt,$ss_taxon_no,{'get_spelling_reason'=>1});
    if ($spelling->{'taxon_no'} != $taxon_no && $current_taxon_name ne $spelling->{'taxon_name'}) {
        $is_spelling = 1;
        $spelling_reason = $spelling->{'spelling_reason'};
        $spelling_reason = 'original and current combination' if $spelling_reason eq 'original spelling';
        $spelling_reason = 'recombined as' if $spelling_reason eq 'recombination';
        $spelling_reason = 'corrected as' if $spelling_reason eq 'correction';
        $spelling_reason = 'spelled with current rank as' if $spelling_reason eq 'rank change';
        $spelling_reason = 'reassigned as' if $spelling_reason eq 'reassignment';
        if ( $status =~ /replaced|subgroup|nomen/ )	{
            $spelling_reason = $status;
            if ( $status =~ /nomen/ )	{
                $spelling_reason .= ' belonging to';
            }
        }
    }
    my $taxon_name = $spelling->{'taxon_name'};
    my $taxon_rank = $spelling->{'taxon_rank'};
    if ($is_synonym || $is_spelling) {
        if ($taxon_rank =~ /species|genus/) {
            $synonym_name = "<em>$taxon_name</em>";
        } else { 
            $synonym_name = $taxon_name;
        }
        $synonym_name =~ s/"/&quot;/g;
        if ($is_synonym) {
            $synonym_name = "synonym of <span class=\"mockLink\" id=\"syn$ss_taxon_no\" onMouseOver=\"addLink('syn$ss_taxon_no','&amp;taxon_no=$ss_taxon_no','$synonym_name')\">$synonym_name</span>";
        } else {
            $synonym_name = "$spelling_reason <span class=\"mockLink\" id=\"syn$ss_taxon_no\" onMouseOver=\"addLink('syn$ss_taxon_no','&amp;taxon_no=$ss_taxon_no','$synonym_name')\">$synonym_name</span>";
        }
    }
    return $synonym_name;
}


# Gets an HTML formatted table of reidentifications for a particular taxon
# pass it an occurrence number or reid_no
# the second parameter tells whether it's a reid_no (true) or occurrence_no (false).
sub getReidHTMLTableByOccNum {
	my ($dbt,$hbo,$s,$occNum,$isReidNo,$doReclassify) = @_;

	my $sql = "SELECT genus_reso, genus_name, subgenus_reso, subgenus_name, species_reso, species_name, plant_organ, re.comments as comments, re.reference_no as reference_no,  pubyr, taxon_no, occurrence_no, reid_no, collection_no FROM reidentifications re"
            . " LEFT JOIN refs r ON re.reference_no=r.reference_no ";
	if ($isReidNo) {
		$sql .= " WHERE reid_no = $occNum";
	} else {
		$sql .= " WHERE occurrence_no = $occNum";
	}
    $sql .= " ORDER BY r.pubyr ASC, re.reid_no ASC";
    my @results = @{$dbt->getData($sql)};
	my $html = "";
    my $classification = {};
    my $are_reclassifications = 0;

    # We always get all of them PS
	foreach my $row ( @results ) {
		$row->{'taxon_name'} = "&nbsp;&nbsp;&nbsp;&nbsp;= ".formatOccurrenceTaxonName($row);
        
		# format the reference (PM)
		$row->{'reference_no'} = Reference::formatShortRef($dbt,$row->{'reference_no'},'no_inits'=>1,'link_id'=>1);
       
		# get the taxonomic authority JA 19.4.04
		my $taxon;
		if ($row->{'taxon_no'}) {
			$taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'taxon_no'}},['taxon_no','taxon_name','common_name','taxon_rank','author1last','author2last','otherauthors','pubyr','reference_no','ref_is_authority']);

			if ($taxon->{'taxon_rank'} =~ /species/ || $row->{'species_name'} =~ /^indet\.|^sp\./) {
				$row->{'authority'} = Reference::formatShortRef($taxon,'no_inits'=>1,'link_id'=>$taxon->{'ref_is_authority'});
			}
		}

        # Just a default value, so form looks correct
        # JA 2.4.04: changed this so it only works on the most recently published reID
        if ( $row == $results[$#results] )	{
            if ($row->{'taxon_no'}) {
                my $class_hash = TaxaCache::getParents($dbt,[$row->{'taxon_no'}],'array_full');
                my @class_array = @{$class_hash->{$row->{'taxon_no'}}};
                my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$row->{'taxon_no'}},['taxon_name','taxon_rank','pubyr']);

		unshift @class_array , $taxon;
                $row = getClassOrderFamily(\$row,\@class_array);

		# row has the classification now, so stash it
		$classification->{'class'}{'taxon_name'} = $row->{'class'};
		$classification->{'order'}{'taxon_name'} = $row->{'order'};
		$classification->{'family'}{'taxon_name'} = $row->{'family'};

                # Include the taxon as well, it my be a family and be an indet.
                $classification->{$taxon->{'taxon_rank'}} = $taxon;

                $row->{'synonym_name'} = getSynonymName($dbt,$row->{'taxon_no'},$taxon->{'taxon_name'});
                # only $classification is being returned, so piggyback lft and
                #  rgt on it
                # I hate having to hit taxa_tree_cache with a separate SELECT,
                #  but you can't hit it until you already know there's a
                #  taxon_no you can use JA 23.1.07
                my $sql = "SELECT lft,rgt FROM $TAXA_TREE_CACHE WHERE taxon_no=" . $row->{'taxon_no'};
                my $lftrgtref = ${$dbt->getData($sql)}[0];
                $classification->{'lft'}{'taxon_no'} = $lftrgtref->{'lft'};
                $classification->{'rgt'}{'taxon_no'} = $lftrgtref->{'rgt'};
            } else {
                if ($doReclassify) {
                    $row->{'show_classification_select'} = 'YES';
                    my $taxon_name = $row->{'genus_name'}; 
                    $taxon_name .= " ($row->{'subgenus_name'})" if ($row->{'subgenus_name'});
                    $taxon_name .= " $row->{'species_name'}";
                    my @all_matches = Taxon::getBestClassification($dbt,$row);
                    if (@all_matches) {
                        $are_reclassifications = 1;
                        $row->{'classification_select'} = Reclassify::classificationSelect($dbt, $row->{$OCCURRENCE_NO},0,1,\@all_matches,$row->{'taxon_no'},$taxon_name);
                    }
                }
            }
		}
    
		$row->{'hide_collection_no'} = 1;
		$html .= $hbo->populateHTML("taxa_display_row", $row);
	}

	return ($html,$classification,$are_reclassifications);
}


sub getClassOrderFamily	{
	my $rowref_ref = shift;
	my $rowref;
	if ( $rowref_ref )	{
		$rowref = ${$rowref_ref};
	}
	my $class_array_ref = shift;
	my @class_array = @{$class_array_ref};

	my ($toplowlevel,$maxyr,$toplevel) = (-1,'',$#class_array);
	# common name and family are easy
	for my $i ( 0..$#class_array ) {
		my $t = $class_array[$i];
		if ( $t->{'taxon_rank'} =~ /superclass|phylum|kingdom/ )	{
			last;
		}
		if ( ! $rowref->{'common_name'} && $t->{'common_name'} )	{
			$rowref->{'common_name'} = $t->{'common_name'};
		}
		if ( $t->{'taxon_rank'} eq "family" && ! $t->{'family'} )	{
			$rowref->{'family'} = $t->{'taxon_name'};
			$rowref->{'family_no'} = $t->{'taxon_no'};
		}
		if ( $t->{'taxon_rank'} =~ /family|tribe|genus|species/ && $t->{'taxon_rank'} ne "superfamily" )	{
			$toplowlevel = $i;
		}
	}

	# makes it possible for a higher-order name to be returned as its own
	#  "order" or "class" (because toplowlevel is 0)
	if ( $toplowlevel >= 0 )	{
		$toplowlevel++;
	} else	{
		$toplowlevel = 0;
	}

	# find a plausible class name
	for my $i ( $toplowlevel..$#class_array ) {
		my $t = $class_array[$i];
		if ( $t->{'taxon_rank'} =~ /superclass|phylum|kingdom/ )	{
			last;
		}
		# take a class if you can get it
		if ( $t->{'taxon_rank'} eq "class" )	{
			$maxyr = $t->{'pubyr'};
			$rowref->{'class'} = $t->{'taxon_name'};
			$rowref->{'class_no'} = $t->{'taxon_no'};
			$toplevel = $i;
			last;
		}
		# fish for a useable unranked clade
		if ( ( ! $maxyr || $t->{'pubyr'} <= $maxyr ) && $t->{'taxon_rank'} eq "unranked clade" )	{
			$maxyr = $t->{'pubyr'};
			$rowref->{'class'} = $t->{'taxon_name'};
			$rowref->{'class_no'} = $t->{'taxon_no'};
			$toplevel = $i;
		}
	}

	# find a plausible ordinal name
	for my $i ( $toplowlevel..$toplevel-1 ) {
		my $t = $class_array[$i];
		# something is seriously wrong if a superordinal name has been
		#  encountered, but if we already have a good "class" we might
		#  want to use it anyway, so allow it to be checked later
		if ( $t->{'taxon_rank'} =~ /class|phylum|kingdom/ )	{
			$toplevel = $i+1;
			last;
		}
		if ( $t->{'taxon_rank'} eq "order" )	{
			$rowref->{'order'} = $t->{'taxon_name'};
			$rowref->{'order_no'} = $t->{'taxon_no'};
			last;
		}
	}

	# otherwise extract the oldest unranked intermediate-level name
	if ( ! $rowref->{'order'} )	{
		$maxyr = "";
		#  has been encountered, but if we already have a good "class"
		#  we might want to use it anyway
		for my $i ( $toplowlevel..$toplevel-1 ) {
			my $t = $class_array[$i];
			if ( ( ! $maxyr || $t->{'pubyr'} < $maxyr ) && $t->{'taxon_rank'} ne "superfamily" )	{
				$maxyr = $t->{'pubyr'};
				$rowref->{'order'} = $t->{'taxon_name'};
				$rowref->{'order_no'} = $t->{'taxon_no'};
			}
		}
	}

	return $rowref;
}


# JA 6-9.11.09
# routes to displayCollResults, like a lot of things
sub basicCollectionSearch	{

	my ($dbt,$q,$s,$hbo,$taxa_skipped) = @_;
	my $dbh = $dbt->dbh;

	my $sql;
	my $fields = "collection_no,collection_name,collection_aka,authorizer,authorizer_no,reference_no,country,state,max_interval_no,min_interval_no,collectors,collection_dates";
	my ($NAME_FIELD,$AKA_FIELD,$TIME) = ('collection_name','collection_aka','collection_dates');
	if ( $DB eq "eco" )	{
		$fields = "inventory_no,inventory_name,inventory_aka,authorizer_no,reference_no,country,state,inventoried_by,years";
		($NAME_FIELD,$AKA_FIELD,$TIME) = ('inventory_name','inventory_aka','years');
	}
	my $NO = $q->param($COLLECTION_NO);
	my $NAME = $q->param($NAME_FIELD);

	if ( $NAME =~ /^[0-9]+$/ )	{
		$NO = $NAME;
		$NAME = "";
	}

	if ( ! $q->param($NO) && ! $q->param($NAME_FIELD) && $q->param('quick_search') )	{
		if ( $q->param('quick_search') =~ /^[0-9]+$/ )	{
			$NO = $q->param('quick_search');
		} else	{
			$NAME = $q->param('quick_search');
		}
	}

	if ( $q->param('collection_list') && $q->param('collection_list') =~ /^[\d ,]+$/ ) {
		if ( $q->param('collection_list') =~ /,/ )	{
			$sql = "SELECT $fields FROM $COLLECTIONS WHERE $COLLECTION_NO IN (".$q->param('collection_list').")";
			my @colls = @{$dbt->getData($sql)};
			$q->param('type' => 'view');
			$q->param('basic' => 'yes');
			main::displayCollResults(\@colls);
			exit;
		} else	{
			$q->param('collection_no' => $q->param('collection_list') );
			basicCollectionInfo($dbt,$q,$s,$hbo);
			return 1;
		}
	}

	# paranoia check (all searches should be by name or number)
	if ( ( ! $NO || ( $NO && $NO == 0 ) ) && ! $NAME )	{
		$q->param('type' => 'view');
		$q->param('basic' => 'yes');
		main::displaySearchColls('<center><p style="margin-top: -1em;">Your search produced no matches: please try again</p></center>');
		exit;
	}

	if ( $NO )	{
		$sql = "SELECT $fields FROM $COLLECTIONS WHERE $COLLECTION_NO=".$NO;
		my $coll = ${$dbt->getData($sql)}[0];
		if ( $coll && $DB ne "eco" )	{
			$q->param($COLLECTION_NO => $NO);
			basicCollectionInfo($dbt,$q,$s,$hbo);
			return 1;
		} elsif ( $coll )	{
			$q->param($COLLECTION_NO => $NO);
			inventoryInfo($dbt,$q,$s,$hbo);
			return 1;
		} else	{
			$q->param('type' => 'basic');
			main::displaySearchColls('<center><p style="margin-top: -1em;">Your search produced no matches: please try again</p></center>');
			exit;
		}
	}

	# search is by name of something that could be any of several fields,
	#  so check them in plausibility order

	$NAME =~ s/'/\\'/g;

	# this really looks like a strat unit search, so try that first
	if ( $DB ne "eco" && $NAME =~ / (group|grp|formation|fm|member|mbr|)$/i )	{
		$NAME =~ s/ [A-Za-z]+$//;
		$sql = "SELECT $fields FROM $COLLECTIONS WHERE geological_group='".$NAME."' OR formation='".$NAME."' OR member='".$NAME."'";
	}

	# try literal collection name next
	# exact with no numbers first (could also be a country)
	elsif ( $NAME !~ /[^A-Za-z ]/ )	{
		$sql = "SELECT $fields FROM $COLLECTIONS WHERE $NAME_FIELD='".$NAME."' OR country='".$NAME."'";
	} elsif ( $NAME =~ /[^0-9]/ )	{
		$sql = "SELECT $fields FROM $COLLECTIONS WHERE $NAME_FIELD='".$NAME."'";
	}

	# special handling for plain integers
	else	{
		my $integer = $dbh->quote('.*[^0-9]'.$NAME.'(([^0-9]+)|($))');
		$sql = "SELECT $fields FROM $COLLECTIONS WHERE $COLLECTION_NO=".$NAME." OR $NAME_FIELD REGEXP $integer OR $AKA_FIELD REGEXP $integer OR $TIME REGEXP $integer";
	}

	my @colls = @{$dbt->getData($sql)};
	if ( @colls )	{
		route();
		return 1;
	}

	# a clean string might be a taxon name passed through by quickSearch
	#  in cases where basicTaxonInfo searches were skipped JA 27.5.11
	# note that if a species name is unknown the user won't get matches
	#  based only on the genus name (users seem to prefer this)
	if ( $NAME =~ /^([A-Za-z][a-z]+)(| [a-z]+)$/ && ! $taxa_skipped )	{
		$sql = "SELECT taxon_no FROM authorities WHERE (taxon_name='$NAME'";
		# also look for species of an apparent genus
		if ( $NAME !~ / / )	{
			$sql .= " OR taxon_name LIKE '$NAME %'";
		}
		$sql .= ")";
		my @taxa = @{$dbt->getData($sql)};
		if ( $#taxa > 0 )	{
			my @names;
			for my $taxon ( @taxa )	{
				my $orig = TaxonInfo::getOriginalCombination($dbt,$taxon->{'taxon_no'});
				my $ss = TaxonInfo::getSeniorSynonym($dbt,$orig);
				my @subnames = TaxonInfo::getAllSynonyms($dbt,$ss);
				@subnames ? push @names , @subnames : "";
			}
			my $cfields = $fields;
			$cfields =~ s/,/,c./g;
			$sql = "SELECT c.$cfields FROM $COLLECTIONS c,$OCCURRENCES o WHERE c.$COLLECTION_NO=o.$COLLECTION_NO AND taxon_no IN (".join(',',@names).")";
			@colls = @{$dbt->getData($sql)};
			if ( @colls )	{
				route();
				return 1;
			}
		}
	}

	# partial collection name
	$sql = "SELECT $fields FROM $COLLECTIONS WHERE $NAME_FIELD LIKE '%".$NAME."%'";
	@colls = @{$dbt->getData($sql)};
	if ( @colls )	{
		route();
		return 1;
	}

	# try alternative collection name
	$sql = "SELECT $fields FROM $COLLECTIONS WHERE $AKA_FIELD LIKE '%".$NAME."%'";
	@colls = @{$dbt->getData($sql)};
	if ( @colls )	{
		route();
		return 1;
	}

	# try strat unit
	if ( $DB ne "eco" )	{
		$sql = "SELECT $fields FROM $COLLECTIONS WHERE (geological_group LIKE '%".$NAME."%' OR formation LIKE '%".$NAME."%' OR member LIKE '%".$NAME."%')";
		@colls = @{$dbt->getData($sql)};
		if ( @colls )	{
			route();
			return 1;
		}
	}

	sub route()	{
		if ( scalar(@colls) == 0 )	{
			return;
		} elsif ( $#colls == 0 )	{
			$q->param($COLLECTION_NO => $colls[0]->{$COLLECTION_NO} );
			if ( $DB ne "eco" )	{
				basicCollectionInfo($dbt,$q,$s,$hbo);
			} else	{
				inventoryInfo($dbt,$q,$s,$hbo);
			}
			exit;
		} else	{
			$q->param('type' => 'view');
			$q->param('basic' => 'yes');
			main::displayCollResults(\@colls);
			exit;
		}
	}


	if ( ! @colls )	{
		# function was called by quickSearch, which will try
		#  taxon name next
		if ( ! @colls && $q->param('quick_search') )	{
			return 0;
		} else	{
			$q->param('collection_no' => $q->param('last_collection') );
			$q->param('type' => 'view');
			$q->param('basic' => 'yes');
			main::displaySearchColls('Your search produced no matches: please try again');
			exit;
		}
	}
	return 0;

}

# JA 6-9.11.09
sub basicCollectionInfo	{

	my ($dbt,$q,$s,$hbo,$error,$is_bot) = @_;
	my $dbh = $dbt->dbh;

	my ($is_real_user,$not_bot) = (0,0);
	if ( ! $is_bot )	{
		($is_real_user,$not_bot) = (1,1);
		if (! $q->request_method() eq 'POST' && ! $q->param('is_real_user') && ! $s->isDBMember())	{
			$is_real_user = 0;
			$not_bot = 0;
		} elsif (PBDBUtil::checkForBot())	{
			$is_real_user = 0;
			$not_bot = 0;
		}
		if ( $is_real_user > 0 )	{
			main::logRequest($s,$q);
		}
	}

	my $sql = "SELECT *,DATE_FORMAT(release_date, '%Y%m%d') AS rd_short FROM collections WHERE collection_no=".$q->param('collection_no');
	my $c = ${$dbt->getData($sql)}[0];

	my $p = Permissions->new($s,$dbt);
	my $okToRead = $p->readPermission($c);
	# if the collection is protected, pretend the search failed
	if ( ! $okToRead )	{
		$q->param('type' => 'view');
		main::displaySearchColls('Your search produced no matches: please try again');
		exit;
	}

	my $mockLI = 'class="verysmall" style="margin-top: -1em; margin-left: 2em; text-indent: -1em;"> &bull;';
	my $indent = 'style="padding-left: 1em; text-indent: -1em;"';

	for my $field ( 'geogcomments','stratcomments','geology_comments','lithdescript','component_comments','taphonomy_comments','collection_comments','taxonomy_comments' )	{
		while ( $c->{$field} =~ /\n$/ )	{
			$c->{$field} =~ s/\n$//;
		}
		$c->{$field} =~ s/\n\n/\n/g;
		$c->{$field} =~ s/\n/<\/p>\n<p $mockLI/g;
	}

	my $page_vars = {};
	if ( $c->{'research_group'} =~ /ETE/ && $q->param('guest') eq '' )	{
		$page_vars->{ete_banner} = "<div style=\"padding-left: 3em; float: left;\"><img alt=\"ETE\" src=\"/public/bannerimages/ete_logo.jpg\"></div>";
	}

	print $hbo->stdIncludes($PAGE_TOP, $page_vars);

	my $header = $c->{'collection_name'};

	for my $f ( 'lithadj','lithadj2','pres_mode','assembl_comps','common_body_parts','rare_body_parts','coll_meth','museum' )	{
		$c->{$f} =~ s/,/, /g;
	}


	print qq|
<div align="center" class="medium" style="margin-left: 1em; margin-top: 3em;">
<div class="displayPanel" style="margin-top: -1em; margin-bottom: 2em; text-align: left; width: 54em;">
<span class="displayPanelHeader">$header</span>
<div align="left" class="small displayPanelContent" style="padding-left: 1em; padding-bottom: 1em;">
|;

	if ( $c->{'collection_aka'} )	{
		print "<p>Also known as $c->{'collection_aka'}</p>\n\n";
	}
	print "<p>Where: ";
	if ( $c->{'country'} eq "United States" )	{
		if ( $c->{'county'} )	{
			print $c->{'county'}." County, ";
		}
		print $c->{'state'};
	} else	{
		if ( $c->{'state'} )	{
			print $c->{'state'}.", ";
		}
		print $c->{'country'};
	}

	$c = formatCoordinate($s,$c);
	$c->{'latdir'} =~ s/(N|S).*/$1/;
	$c->{'lngdir'} =~ s/(E|W).*/$1/;
	$c->{'paleolatdir'} =~ s/(N|S).*/$1/;
	$c->{'paleolngdir'} =~ s/(E|W).*/$1/;

	if ( $s->isDBMember() && $c->{'latmin'} )	{
		print " (".$c->{'latdeg'}."&deg;".$c->{'latmin'}."'";
		if ( $c->{'latsec'} )	{
			print $c->{'latsec'}.'"';
		}
		print " ".$c->{'latdir'};
		print " ".$c->{'lngdeg'}."&deg;".$c->{'lngmin'}."'";
		if ( $c->{'lngsec'} )	{
			print $c->{'lngsec'}.'"';
		}
		print " ".$c->{'lngdir'};
	} else	{
		print " (".$c->{'latdeg'}.".".$c->{'latdec'}."&deg; ".$c->{'latdir'};
		print ", ".$c->{'lngdeg'}.".".$c->{'lngdec'}."&deg; ".$c->{'lngdir'};
	}
	if ( $c->{'paleolat'} && $c->{'paleolng'} )	{
		print ": paleocoordinates ".$c->{'paleolat'}." ".$c->{'paleolatdir'};
		print ", ".$c->{'paleolng'}." ".$c->{'paleolngdir'};
	}

	print ")";
	print "</p>\n\n";

	if ( $s->isDBMember() && $c->{'geogcomments'} )	{
		print "<p $mockLI $c->{'geogcomments'}</p>\n\n";
	}

	print "<p $indent>When: ";
	if ( $c->{'zone'} )	{
		print $c->{'zone'}." ".$c->{'zone_type'}." zone, ";
	}
	if ( $c->{'member'} )	{
		print $c->{'member'}." Member";
		if ( $c->{'formation'} )	{
			print " (".$c->{'formation'}." Formation)";
		}
		print ", ";
	} elsif ( $c->{'formation'} )	{
		print $c->{'formation'}." Formation";
		if ( $c->{'geological_group'} )	{
			print " (".$c->{'geological_group'}." Group)";
		}
		print ", ";
	} elsif ( $c->{'geological_group'} )	{
		print $c->{'geological_group'}." Group, ";
	}

	my ($max,$min);
	if ( $c->{'max_interval_no'} > 0 )	{
		$sql = "SELECT eml_interval,interval_name,lower_boundary,upper_boundary FROM intervals i,interval_lookup l WHERE i.interval_no=".$c->{'max_interval_no'}." AND i.interval_no=l.interval_no";
		$max = ${$dbt->getData($sql)}[0];
		if ( $max->{'eml_interval'} )	{
			print $max->{'eml_interval'}." ";
		}
		print $max->{'interval_name'}." ";
	}
	if ( $c->{'min_interval_no'} > 0 )	{
		$sql = "SELECT eml_interval,interval_name,lower_boundary,upper_boundary FROM intervals i,interval_lookup l WHERE i.interval_no=".$c->{'min_interval_no'}." AND i.interval_no=l.interval_no";
		$min = ${$dbt->getData($sql)}[0];
		print " to ";
		if ( $min->{'eml_interval'} )	{
			print $min->{'eml_interval'}." ";
		}
		print $min->{'interval_name'}." ";
	}
	if ( $max->{'lower_boundary'} )	{
		printf "(%.1f - ",$max->{'lower_boundary'};
		if ( ! $min->{'upper_boundary'} )	{
			printf "%.1f",$max->{'upper_boundary'};
		} else	{
			printf "%.1f",$min->{'upper_boundary'};
		}
		print " Ma)";
	}
	print "</p>\n\n";

	if ( $c->{'stratcomments'} )	{
		print "<p $mockLI $c->{'stratcomments'}</p>\n\n";
	}

	print "<p $indent>Environment/lithology: ";
	my $env = $c->{'environment'};
	$env =~ s/ indet.//;
	$env =~ s/(carbonate|siliciclastic)//;
	$env =~ s/\// or /;
	print $env;

	my @terms;
	if ( $c->{'lithification'} )	{
		push @terms , $c->{'lithification'};
	}
	$c->{'lithadj'} =~ s/(fine|medium|coarse)/$1-grained/;
	$c->{'lithadj'} =~ s/dunes(,|)//;
	$c->{'lithadj'} =~ s/grading/graded/;
	$c->{'lithadj'} =~ s/burrows/burrowed/;
	$c->{'lithadj'} =~ s/bioturbation/bioturbated/;
	my @adjectives = split /, /,$c->{'lithadj'};
	for my $adj ( @adjectives )	{
	# I can't be bothered with most of the sed structure values
		if ( $adj !~ / / )	{
			push @terms , $adj;
		}
	}
	if ( $c->{'minor_lithology'} )	{
		push @terms , split /,/,$c->{'minor_lithology'};
	}
	$c->{'lithology1'} =~ s/"//g;
	$c->{'lithology1'} =~ s/clastic/clastic sediments/g;
	$c->{'lithology1'} =~ s/not reported/lithology not reported/g;
	push @terms , $c->{'lithology1'};
	my $last = pop @terms;
	if ( $env && $last )	{
		print "; ";
	}
	print join(', ',@terms)." ".$last;

	if ( $c->{'lithology2'} )	{
		my @terms;
		if ( $c->{'lithification2'} )	{
			push @terms , $c->{'lithification2'};
		}
		$c->{'lithadj2'} =~ s/(fine|medium|coarse)/$1-grained/;
		$c->{'lithadj2'} =~ s/dunes(,|)//;
		$c->{'lithadj2'} =~ s/grading/graded/;
		$c->{'lithadj2'} =~ s/burrows/burrowed/;
		$c->{'lithadj2'} =~ s/bioturbation/bioturbated/;
		my @adjectives = split /, /,$c->{'lithadj2'};
		for my $adj ( @adjectives )	{
			if ( $adj !~ / / )	{
				push @terms , $adj;
			}
		}
		if ( $c->{'minor_lithology2'} )	{
			push @terms , split /,/,$c->{'minor_lithology2'};
		}
		$c->{'lithology2'} =~ s/"//g;
		push @terms , $c->{'lithology2'};
		my $last = pop @terms;
		print " and ".join(', ',@terms)." ".$last;
	}
	print "</p>\n\n";

	if ( $c->{'geology_comments'} || $c->{'lithdescript'} )	{
		print "<div class=\"verysmall\" style=\"margin-top: -1em;\">\n";
		if ( $c->{'geology_comments'} )	{
			print "<div style=\"margin-left: 2em; text-indent: -1em;\">&bull; $c->{'geology_comments'}</div>\n";
		}
		if ( $c->{'lithdescript'} )	{
			print "<div style=\"margin-left: 2em; text-indent: -1em;\">&bull; $c->{'lithdescript'}</div>\n";
		}
		print "</div>\n\n";
	}

	if ( $c->{'assembl_comps'} )	{
		if ( $c->{'assembl_comps'} =~ /,/ )	{
			print "<p>Size classes: ";
		} else	{
			print "<p>Size class: ";
		}
		print $c->{'assembl_comps'};
		print "</p>\n\n";
	}

	if ( $c->{'assembl_comps'} && $c->{'component_comments'} )	{
		print "<p $mockLI $c->{'component_comments'}</p>\n\n";
	}

	$c->{'pres_mode'} =~ s/body(,|)//;
	if ( $c->{'pres_mode'} )	{
		print "<p>Preservation: $c->{'pres_mode'}</p>\n\n";
	}

	if ( $c->{'pres_mode'} && $c->{'taphonomy_comments'} )	{
		print "<p $mockLI $c->{'taphonomy_comments'}</p>\n\n";
	}

	# remove leading day of month (probably)
	$c->{'collection_dates'} =~ s/^[0-9]([0-9]|) //;
	# remove all leading verbiage
	while ( $c->{'collection_dates'} =~ /^[A-Za-z]* / )	{
		$c->{'collection_dates'} =~ s/^[A-Za-z]* //;
	}
	# fix up something like 1980s
	$c->{'collection_dates'} =~ s/(.*)([0-9]s)$/the $1$2/;
	# extract year from a string like 11.11.2011
	$c->{'collection_dates'} =~ s/([0-9]+\.)([0-9]+\.)([1-2][0-9])/$3/g;
	if ( $c->{'collectors'} || $c->{'collection_dates'} )	{
		print "<p>Collected";
		if ( $c->{'collectors'} )	{
			print " by ".$c->{'collectors'};
		}
		if ( $c->{'collection_dates'} )	{
			print " in ".$c->{'collection_dates'};
		}
		if ( $c->{'museum'} )	{
			print "; reposited in the ".$c->{'museum'};
		}
		print "</p>\n\n";
	} elsif ( $c->{'museum'} )	{
		print "<p>Reposited in the $c->{'museum'}</p>\n\n";
	}

	$c->{'coll_meth'} =~ s/(field collection|survey of museum collection|observed .not collected.|selective )//g;
	$c->{'coll_meth'} =~ s/, ,/,/g;
	$c->{'coll_meth'} =~ s/^, //g;
	if ( $c->{'coll_meth'} )	{
		print "<p>Collection methods: $c->{'coll_meth'}</p>\n\n";
	}

	if ( ( $c->{'collectors'} || $c->{'collection_dates'} || $c->{'museum'} || $c->{'coll_meth'} ) && $c->{'collection_comments'} )	{
		print "<p $mockLI $c->{'collection_comments'}</p>\n\n";
	}

	$sql = "SELECT * FROM refs WHERE reference_no=".$c->{'reference_no'};
	my $ref = ${$dbt->getData($sql)}[0];
	print "<p $indent>Primary reference: ".Reference::formatLongRef($ref,'link_id'=>1)." <a class=\"verysmall\" href=\"$READ_URL?action=displayReference&reference_no=$c->{reference_no}\">more details</a>";
	if ( $s->isDBMember() ) {
		print " - <a class=\"verysmall\" href=\"$WRITE_URL?action=displayRefResults&amp;type=edit&amp;reference_no=$c->{reference_no}\">edit</a>";
	}
	print "</p>\n\n";

	$sql = "SELECT r.reference_no,author1last,author2last,otherauthors,pubyr FROM refs r,secondary_refs s WHERE r.reference_no=s.reference_no AND collection_no=".$c->{'collection_no'}." ORDER BY author1last,author2last,pubyr";
	my @refs = @{$dbt->getData($sql)};
	if ( @refs )	{
		my @formatted;
		for my $r ( @refs )	{
			push @formatted , Reference::formatShortRef($r,'link_id'=>1);
		}
		print "<p $indent>See also ".join(', ',@formatted)."</p>\n\n";
	}

	if ( $c->{''} )	{
		print "<p>: ";
		print $c->{''};
		print "</p>\n\n";
	}

	$c->{'created'} =~ s/ .*//;
	my ($y,$m,$d) = split /-/,$c->{'created'};
	print "<p $indent>PaleoDB collection $c->{'collection_no'}: authorized by $c->{'authorizer'}, entered by $c->{'enterer'} on $d.$m.$y";

	$sql = "(SELECT distinct(enterer) FROM occurrences WHERE collection_no=$c->{'collection_no'} AND enterer!=".$dbh->quote($c->{'enterer'}).") UNION (SELECT distinct(enterer) FROM reidentifications WHERE collection_no=$c->{'collection_no'} AND enterer!=".$dbh->quote($c->{'enterer'}).")";
	my @enterers = @{$dbt->getData($sql)};
	if ( @enterers )	{
		print ", edited by ";
		my @names;
		push @names, $_->{'enterer'} foreach @enterers;
		my $last = pop @names;
		if ( @names )	{
			print join(', ',@names)." and ".$last;
		} else	{
			print $last;
		}
	}
	print "</p>\n\n";

	if ( $is_real_user == 0 || $not_bot == 0 )	{
		print $hbo->stdIncludes($PAGE_BOTTOM);
		return;
	}

	print "<a href=\"$READ_URL?action=displayCollectionDetails&collection_no=$c->{'collection_no'}\">See full details</a>\n\n";

	# the following is basically a complete rewrite of buildTaxonomicList
	# so what?

	$sql = "(SELECT lft,o.genus_reso,o.genus_name,o.subgenus_reso,o.subgenus_name,o.species_reso,o.species_name,o.taxon_no,synonym_no FROM occurrences o LEFT JOIN reidentifications re ON (o.occurrence_no=re.occurrence_no) LEFT JOIN $TAXA_TREE_CACHE t ON o.taxon_no=t.taxon_no WHERE o.collection_no=$c->{'collection_no'} AND re.reid_no IS NULL AND lft>0) UNION (SELECT lft,re.genus_reso,re.genus_name,re.subgenus_reso,re.subgenus_name,re.species_reso,re.species_name,re.taxon_no,synonym_no FROM reidentifications re,$TAXA_TREE_CACHE t WHERE collection_no=$c->{'collection_no'} AND re.most_recent='YES' AND re.taxon_no=t.taxon_no AND lft>0) UNION (SELECT 999999,o.genus_reso,o.genus_name,o.subgenus_reso,o.subgenus_name,o.species_reso,o.species_name,o.taxon_no,0 FROM occurrences o WHERE collection_no=$c->{'collection_no'} AND taxon_no=0) ORDER BY lft";
	my @occs = @{$dbt->getData($sql)};
	my (%bad,%lookup);
	for my $o ( @occs )	{
		if ( $o->{'taxon_no'} != $o->{'synonym_no'} )	{
			$bad{$o->{'taxon_no'}} = $o->{'synonym_no'};
		}
	}
	if ( %bad )	{
		$sql = "SELECT a.taxon_no,a.taxon_name bad,a.taxon_rank,synonym_no,a2.taxon_name good FROM authorities a,authorities a2,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND t.synonym_no=a2.taxon_no AND a.taxon_no IN (".join(',',keys %bad).")";
		my @seniors = @{$dbt->getData($sql)};
		for my $s ( @seniors )	{
		# ignore rank changes that don't change spellings
			if ( $s->{'bad'} ne $s->{'good'} )	{
				if ( $s->{'taxon_rank'} =~ /genus|species/ )	{
					$s->{'good'} = "<i>".$s->{'good'}."</i>";
				}
				$s->{'good'} = "<a href=\"$READ_URL?action=basicTaxonInfo&amp;taxon_no=$s->{'synonym_no'}\">".$s->{'good'}."</a>";
				$lookup{$s->{'synonym_no'}} = $s->{'good'};
			}
		}
	}
	print "<div style=\"margin-left: 0em; margin-right: 1em; border-top: 1px solid darkgray;\">\n\n";
	print "<p class=\"large\" style=\"margin-top: 0.5em; margin-bottom: 1.5em;\">Taxonomic list</p>\n\n";
	if ( $c->{'taxonomy_comments'} )	{
		print "<p $mockLI $c->{'taxonomy_comments'}</p>\n\n";
	}
	print "<table class=\"small\" cellpadding=\"4\" class=\"taxonomicList\" style=\"margin-top: -0.5em;\">\n\n";
	my ($lastclass,$lastorder,$lastfamily,$class);
	for my $o ( @occs )	{
		my ($ital,$ital2,$postfix) = ('<i>','</i>','');
		if ( $o->{'species_name'} eq "indet." )	{
			($ital,$ital2) = ('','');
		}
		if ( $o->{'genus_reso'} eq "n. gen." )	{
			$postfix = $o->{'genus_reso'};
			$o->{'genus_reso'} = "";
		}
		if ( $o->{'subgenus_reso'} eq "n. subgen." )	{
			$postfix .= " ".$o->{'subgenus_reso'};
			$o->{'subgenus_reso'} = "";
		}
		if ( $o->{'species_reso'} eq "n. sp." )	{
			$postfix .= " ".$o->{'species_reso'};
			$o->{'species_reso'} = "";
		}
		if ( $o->{'genus_reso'} =~ /informal|"/ )	{
			$o->{'genus_reso'} =~ s/informal.*|"//;
			$o->{'genus_name'} = '"'.$o->{'genus_name'}.'"';
		}
		if ( $o->{'subgenus_reso'} =~ /informal|"/ )	{
			$o->{'subgenus_reso'} =~ s/informal.*|"//;
			$o->{'subgenus_name'} = '"'.$o->{'subgenus_name'}.'"';
		}
		if ( $o->{'species_reso'} =~ /informal|"/ )	{
			$o->{'species_reso'} =~ s/informal.*|"//;
			$o->{'species_name'} = '"'.$o->{'species_name'}.'"';
		}
		if ( $o->{'subgenus_reso'} && $o->{'subgenus_name'} )	{
			$o->{'subgenus_reso'} = "(".$o->{'subgenus_reso'};
			$o->{'subgenus_name'} .= ")";
		} elsif ( $o->{'subgenus_name'} )	{
			$o->{'subgenus_name'} = "(".$o->{'subgenus_name'}.")";
		}
		$o->{'formatted'} = "$o->{'genus_reso'} $o->{'genus_name'} $o->{'subgenus_reso'} $o->{'subgenus_name'} $o->{'species_reso'} $o->{'species_name'}";
		$o->{'formatted'} =~ s/  / /g;
		$o->{'formatted'} =~ s/ $//g;
		$o->{'formatted'} =~ s/^ //g;
		$o->{'formatted'} = $ital.$o->{'formatted'}.$ital2;
		if ( ! $lookup{$o->{'synonym_no'}} && $o->{'taxon_no'} )	{
			$o->{'formatted'} = "<a href=\"$READ_URL?action=basicTaxonInfo&amp;taxon_no=$o->{'taxon_no'}\">".$o->{'formatted'}."</a>";
		} elsif ( ! $o->{'taxon_no'} )	{
			my $name = $o->{'genus_name'};
			if ( $o->{'species_name'} !~ /(sp|spp|indet)\./ )	{
				$name .= " ".$o->{'species_name'};
			}
			$o->{'formatted'} = "<a href=\"$READ_URL?action=basicTaxonInfo&amp;taxon_name=$name\">".$o->{'formatted'}."</a>";
		}
		if ( $postfix )	{
			$o->{'formatted'} .= " ".$postfix;
		}
		if ( $lookup{$o->{'synonym_no'}} )	{
			$o->{'formatted'} = '"'.$o->{'formatted'}.'" = '.$lookup{$o->{'synonym_no'}};
		}
		if ( $o->{'abund_value'} )	{
			$o->{'formatted'} .= "[".$o->{'abund_value'}."]";
		}
		my $class_hash = TaxaCache::getParents($dbt,[$o->{'taxon_no'}],'array_full');
		my @class_array = @{$class_hash->{$o->{'taxon_no'}}};
		my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$o->{'taxon_no'}},['taxon_name','taxon_rank','pubyr']);
		unshift @class_array , $taxon;
		$o = getClassOrderFamily(\$o,\@class_array);
		if ( ! $o->{'class'} && ! $o->{'order'} && ! $o->{'family'} )	{
			$o->{'class'} = "unclassified";
		}
		if ( $o->{'class'} ne $lastclass || $o->{'order'} ne $lastorder || $o->{'family'} ne $lastfamily )	{
			if ( $lastclass || $lastorder || $lastfamily )	{
				print "</tr>\n";
			}
			my @parents;
			for my $p ( 'class','order','family' )	{
				if ( $o->{$p} )	{
					push @parents , $o->{$p};
				}
			}
			my $parentlist = join(' - ',@parents);
			if ( $class =~ /dark/ )	{
				$class = '';
			} elsif ( $#occs > 0 )	{
				$class = ' class="darkList"';
			}
			print "<tr$class>\n<td valign=\"top\"><nobr>$parentlist</nobr></td>\n";
			print "<td valign=\"top\">$o->{'formatted'}";
		} else	{
			print ", $o->{'formatted'}";
		}
		$lastclass = $o->{'class'};
		$lastorder = $o->{'order'};
		$lastfamily = $o->{'family'};
	}
	print "</tr>\n";
	print "</table>\n\n";

	print "</div>\n</div>\n</div>\n\n";

	if ( $error )	{
		print "<center><p style=\"margin-top: -1em;\"><i>$error</i></p></center>\n\n";
	}

	if ($s->isDBMember()) {
		print "<div class=\"medium\" style=\"margin-top: -1em; margin-bottom: 1em;\">\n";
		my $p = Permissions->new($s,$dbt);
		my $can_modify = $p->getModifierList();
		$can_modify->{$s->get('authorizer_no')} = 1;
		if ($can_modify->{$c->{'authorizer_no'}} || $s->isSuperUser) {  
			 print qq|<a href="$WRITE_URL?action=displayCollectionForm&collection_no=$c->{'collection_no'}">Edit collection</a> - |;
		}
		print qq|<a href="$WRITE_URL?action=displayCollectionForm&prefill_collection_no=$c->{'collection_no'}">Add a collection copied from this one</a> - |;
		if ($can_modify->{$c->{'authorizer_no'}} || $s->isSuperUser) {  
			print qq|<a href="$WRITE_URL?action=displayOccurrenceAddEdit&collection_no=$c->{'collection_no'}">Edit taxonomic list</a>|;
		}
		if ( $s->get('role') =~ /authorizer|student|technician/ )	{
			print qq| - <a href="$WRITE_URL?action=displayOccsForReID&collection_no=$c->{'collection_no'}">Reidentify taxa</a>|;
		}
		print "\n</div>\n\n";
	}

        print qq|
<form method="POST" action="$READ_URL">
<input type="hidden" name="action" value="basicCollectionSearch">
<input type="hidden" name="last_collection" value="$c->{'collection_no'}">
<span class="small">
<input type="text" name="collection_name" value="Search again" size="24" onFocus="textClear(collection_name);" onBlur="textRestore(collection_name);" style="font-size: 1.0em;">
</span>
</form>

|;

	print "<br>\n\n";
	print "</div>\n\n";

	print $hbo->stdIncludes($PAGE_BOTTOM);

}

sub inventoryForm	{

	my ($dbt,$q,$s,$hbo) = @_;
	my $dbh = $dbt->dbh;

	return if PBDBUtil::checkForBot();
	if (!$s->isDBMember()) {
		login( "Please log in first.",'inventoryForm');
		exit;
	}
	if ( ! $s->get('reference_no') ) {
		$s->enqueue($q->query_string());
		main::displaySearchRefs( "Please choose a reference first" );
		exit;
	}

	print $hbo->populateHTML($PAGE_TOP);

	my %vars = $q->Vars();
	$vars{'latmin'} eq "" ? delete $vars{'latmin'} : "";
	$vars{'latsec'} eq "" ? delete $vars{'latsec'} : "";
	( ! $vars{'latmin'} && ! $vars{'latsec'} && ! $vars{'latdec'} ) ? $vars{'latdec'} = "9" : "";
	my $no;
	if ( $q->param('inventory_no') > 0 )	{
		$no = $q->param('inventory_no');
	} elsif ( $q->param('prefill_inventory_no') > 0 )	{
		$no = $q->param('prefill_inventory_no');
	}
	if ( $no > 0 )	{
		my $sql = "SELECT * FROM inventories WHERE inventory_no=".$no;
		my $row = ${$dbt->getData($sql)}[0];
		%vars = %$row;
		delete $vars{'inventory_no'};
		if ( $row->{'latlng_format'} eq "minutes" )	{
			my ($deg,$min,$sec) = toMinSec($row->{'lat'});
			$vars{'lat'} = sprintf "%d%c %d'",$deg,186,60*$min;
			my ($deg,$min,$sec) = toMinSec($row->{'lng'});
			$vars{'lng'} = sprintf "%d%c %d'",$deg,186,60*$min;
		} elsif ( $row->{'latlng_format'} eq "seconds" )	{
			my ($deg,$min,$sec) = toMinSec($row->{'lat'});
			$vars{'lat'} = sprintf "%d%c %d' %d\"",$deg,186,$min,$sec;
			my ($deg,$min,$sec) = toMinSec($row->{'lng'});
			$vars{'lng'} = sprintf "%d%c %d' %d\"",$deg,186,$min,$sec;
		} else	{
			$vars{'lat'} = sprintf "%.$row->{'latlng_format'}f",$row->{'lat'};
			$vars{'lng'} = sprintf "%.$row->{'latlng_format'}f",$row->{'lng'};
		}
	}
	my $sql = "SELECT inventory_name FROM inventories WHERE reference_no=".$s->get('reference_no');
	my @inventory_refs = @{$dbt->getData($sql)};
	if ( @inventory_refs )	{
		my @inventories = map {$_->{'inventory_name'}} @inventory_refs;
		$vars{'already_entered'} = join(', ',@inventories);
		$vars{'already_entered'} = "<p>Inventories already tied to this reference: ".$vars{'already_entered'}."</p>\n\n";
	}
	$vars{'title'} = "<p class=\"pageTitle\">New inventory entry form</p>\n\n";
	if ( $q->param('inventory_no') > 0 )	{
		$vars{'inventory_no'} = $no;
		$vars{'title'} = "<p class=\"pageTitle\">Inventory editing form</p>\n\n";
		$vars{'printed_inventory_no'} = "<p>\nEcological Register inventory number: $vars{'inventory_no'}</p>\n\n";
	}
	$vars{'reference_no'} = $s->get('reference_no');
	$vars{'ref_string'} = Reference::formatLongRef( Reference::getReference($dbt,$vars{'reference_no'}) );

	print $hbo->populateHTML('inventory_form',\%vars);
	print $hbo->populateHTML($PAGE_BOTTOM);

}

# JA 21.5.11
# copied in large part from basicCollectionInfo, but that function is far too
#  specialized to be reused for this purpose
sub inventoryInfo	{
	my ($dbt,$q,$s,$hbo,$links) = @_;
	my $dbh = $dbt->dbh;

	my ($is_real_user,$not_bot) = (1,1);
	if (! $q->request_method() eq 'POST' && ! $q->param('is_real_user') && ! $s->isDBMember())	{
		$is_real_user = 0;
		$not_bot = 0;
	} elsif (PBDBUtil::checkForBot())	{
		$is_real_user = 0;
		$not_bot = 0;
	}
	if ( $is_real_user > 0 )	{
		main::logRequest($s,$q);
	}

	my $sql = "SELECT i.*,DATE_FORMAT(i.created,'%e %M %Y') AS day,concat(p1.first_name,' ',p1.last_name) AS authorizer,concat(p2.first_name,' ',p2.last_name) AS enterer FROM inventories i,person p1,person p2 WHERE inventory_no=".$q->param('inventory_no')." AND i.authorizer_no=p1.person_no AND i.enterer_no=p2.person_no";
	my $i = ${$dbt->getData($sql)}[0];

	my $mockLI = 'class="verysmall" style="margin-top: -1em; margin-left: 2em; text-indent: -1em;"> &bull;';
	my $indent = 'style="padding-left: 1em; text-indent: -1em;"';

	print $hbo->stdIncludes($PAGE_TOP);

	for my $field ( 'geography_comments','habitat_comments','inventory_comments' )	{
		while ( $i->{$field} =~ /\n$/ )	{
			$i->{$field} =~ s/\n$//;
		}
		$i->{$field} =~ s/\n\n/\n/g;
		$i->{$field} =~ s/\n/<\/p>\n<p $mockLI/g;
	}

	$i->{'inventory_method'} =~ s/,/, /g;
	if ( $i->{'inventory_method'} =~ /.*, .*, / )	{
		my @methods = split/,/,$i->{'inventory_method'};
		$methods[$#methods] = " and ".$methods[$#methods];
		$i->{'inventory_method'} = join(',',@methods);
	} elsif ( $i->{'inventory_method'} =~ /.*, / )	{
		$i->{'inventory_method'} =~ s/, / and /;
	}

	print qq|
<div align="center" class="medium" style="margin-left: 1em; margin-top: 3em; width: 58em;">

<div class="displayPanel" style="margin-top: -1em; margin-bottom: 2em; text-align: left;">
<span class="displayPanelHeader">$i->{'inventory_name'}</span>
<div align="left" class="small displayPanelContent" style="padding-left: 1em; padding-bottom: 1em;">
|;

	if ( $i->{'inventory_aka'} )	{
		print "<p $indent>Also known as $i->{'inventory_aka'}</p>\n\n";
	}
	print "<p $indent>Where: ";
	if ( $i->{'country'} eq "United States" )	{
		$i->{'county'} ? print $i->{'county'}." County, " : "";
		print $i->{'state'};
	} else	{
		$i->{'county'} ? print $i->{'county'}." County, " : "";
		$i->{'state'} ? print $i->{'state'}.", " : "";
		print $i->{'country'};
	}

	my ($latdir,$lngdir) = ("N","E");
	$i->{'lat'} < 0 ? $latdir = "S" : "";
	$i->{'lng'} < 0 ? $latdir = "W" : "";
	$i->{'latdir'} =~ s/(N|S).*/$1/;
	$i->{'lngdir'} =~ s/(E|W).*/$1/;
	printf " (%.1f&deg; $latdir",$i->{'lat'};
	printf ", %.1f&deg; $lngdir",$i->{'lng'};
	$i->{'gps_datum'} ? print ": ".$i->{'gps_datum'} : "";
	print ")";
	if ( $i->{'altitude_value'} )	{
		print "; altitude $i->{'altitude_value'} $i->{'altitude_unit'}";
	}
	print "</p>\n\n";

	if ( $i->{'geography_comments'} )	{
		print "<p $mockLI $i->{'geography_comments'}</p>\n\n";
	}

	print "<p>Environment: $i->{'habitat'}";
	if ( $i->{'MAT'} || $i->{'CMT'} || $i->{'WMT'} || $i->{'MART'} || $i->{'MAP'} || $i->{'dry_months'} )	{
		print "; ";
		my (@weather,%deg);
		$deg{$_} = "&deg;" foreach ( 'MAT','CMT','WMT','MART' );
		$deg{'MAP'} = " mm";
		$i->{$_} ? push @weather , sprintf("%s %.1f%s",$_,$i->{$_},$deg{$_}) : "" foreach ( 'MAT','CMT','WMT','MART','MAP');
		$i->{'dry_months'} ? push @weather , "$i->{'dry_months'} dry months" : "";
		print join(', ',@weather);
	}
	print "</p>\n\n";

	$i->{'habitat_comments'} ? print "<p $mockLI $i->{'habitat_comments'}</p>\n\n" : "";

	if ( $i->{'sites'} || $i->{'site_area'} || $i->{'site_length'} || $i->{'site_width'} )	{
		my @stuff;
		$i->{'sites'} ? push @stuff , "$i->{'sites'} sites" : "";
		if ( $i->{'site_area'} )	{
			push @stuff , sprintf("site area %.2f ha",$i->{'site_area'});
		}
		push @stuff , sprintf("%s %.0f m",$_,$i->{$_}) foreach ( 'site_length','site_width' );
		$_ =~ s/_/ / foreach @stuff;
		print "<p $indent>Site description: ".join(', ',@stuff)."</p>\n\n";
	}

	print "<p $indent>Survey methods: ";
	$i->{'inventory_size_unit'} eq "captures" ? print "$i->{'inventory_size'} $i->{'inventory_size_unit'} made" : "";
	$i->{'inventory_size_unit'} eq "individuals" ? print "$i->{'inventory_size'} $i->{'inventory_size_unit'} inventoried" : "";
	! $i->{'inventory_size_unit'} ? print "inventoried" : "";
	$i->{'inventoried_by'} ? print " by $i->{'inventoried_by'}" : "";
	$i->{'years'} ? print " in $i->{'years'}" : "";
	$i->{'days'} ? print " over $i->{'days'} days" : "";
	$i->{'inventory_method'} ? print " using ".$i->{'inventory_method'} : "";
	print "</p>\n\n";

	$i->{'inventory_comments'} ? print "<p $mockLI $i->{'inventory_comments'}</p>\n\n" : "";

	print "<p $indent>Ecological Register inventory #".$i->{'inventory_no'}.", contributed by ".$i->{'authorizer'}." and entered by ".$i->{'enterer'}." on ".$i->{'day'}."</p>\n\n";

	print "<p>Reference: " . Reference::formatLongRef( Reference::getReference($dbt,$i->{'reference_no'}) ) . "</p>\n\n";

	$sql = "SELECT inventory_no,inventory_name FROM inventories WHERE reference_no=".$i->{'reference_no'}." AND inventory_no!=".$i->{'inventory_no'}." ORDER BY inventory_name";
	my @sibs = @{$dbt->getData($sql)};
	if ( @sibs )	{
		my @siblinks;
		push @siblinks , "<a href=\"$READ_URL?a=inventoryInfo&amp;inventory_no=$_->{'inventory_no'}\">$_->{'inventory_name'}</a>" foreach @sibs;
		print "Other inventories from this reference: ".join(', ',@siblinks)."</p>\n\n";
	}

	print $links;
	print "</div></div>\n\n";
 
	if ( $is_real_user == 0 || $not_bot == 0 )	{
		print $hbo->stdIncludes($PAGE_BOTTOM);
		return;
	}

	print buildTaxonomicList($dbt,$hbo,$s,{'inventory_no'=>$i->{'inventory_no'},'hide_reference_no'=>$i->{'reference_no'}});

	if ($s->isDBMember()) {
		print "<div class=\"medium\" style=\"margin-top: 0em; margin-bottom: 1em;\">\n";
		print qq|<a href="$WRITE_URL?action=inventoryForm&inventory_no=$i->{'inventory_no'}">Edit inventory</a> - |;
		print qq|<a href="$WRITE_URL?action=inventoryForm&prefill_inventory_no=$i->{'inventory_no'}">Add an inventory copied from this one</a> - |;
		print qq|<a href="$WRITE_URL?action=displayOccurrenceAddEdit&inventory_no=$i->{'inventory_no'}">Edit species list</a>|;
		print "\n</div>\n\n";
	}

        print qq|
<form method="POST" action="$READ_URL">
<input type="hidden" name="action" value="basicCollectionSearch">
<input type="hidden" name="last_inventory" value="$i->{'inventory_name'}">
<span class="small">
<input type="text" name="inventory_name" value="Search again" size="24" onFocus="textClear(inventory_name);" onBlur="textRestore(inventory_name);" style="font-size: 0.8em;">
</span>
</form>

|;

	print "</div>\n\n";

	print $hbo->stdIncludes($PAGE_BOTTOM);

}

# JA 21.2.03
sub rarefyAbundances	{
    my ($dbt,$q,$s,$hbo) = @_;
    my $dbh = $dbt->dbh;

    my $collection_no = int($q->param('collection_no'));
    my $sql = "SELECT collection_name FROM collections WHERE collection_no=$collection_no";
    my $collection_name=${$dbt->getData($sql)}[0]->{'collection_name'};

	$sql = "SELECT abund_value FROM occurrences WHERE collection_no=$collection_no and abund_value>0";
	
	my $sth = $dbh->prepare( $sql ) || die ( "$sql<hr>$!" );
	$sth->execute();
	my @ids = ();
	my $abundsum;
	my $abundmax;
	my $ntaxa;
	my @abund;
	while ( my @abundrow = $sth->fetchrow_array() )	{
		push @abund , $abundrow[0];
		$abundsum = $abundsum + $abundrow[0];
		if ( $abundrow[0] > $abundmax )	{
			$abundmax = $abundrow[0];
		}
		$ntaxa++;
		foreach my $i (1 .. $abundrow[0]) {
			push @ids , $ntaxa;
        }
	}
	$sth->finish();

	if ( $ntaxa < 2 ) 	{
		my $reason = "it includes no abundance data";
		if ( $ntaxa == 1 )	{
			$reason = "only one taxon has abundance data";
		}	
		print "<center><p>Diversity statistics not available</p>\n<p class=\"medium\">Statistics for $collection_name (PBDB collection <a href=\"$READ_URL?action=basicCollectionSearch&collection_no=$collection_no\">$collection_no</a>) cannot<br>be computed because $reason</p></center>\n\n";
    		print "<p><div align=\"center\"><b><a href=\"$READ_URL?action=displaySearchColls&type=analyze_abundance\">Search again</a></b></div></p>";
		return;
	}

	# compute Berger-Parker, Shannon-Wiener, and PIE indices
	my $bpd = $abundmax / $abundsum;
	my $swh;
	my $pie;
	for my $ab ( @abund )	{
		my $p = $ab / $abundsum;
		$swh = $swh + ( $p * log($p) );
		$pie += $p**2;
	}
	$swh = $swh * -1;
	$pie = 1 - $pie;
	# Hurlbert's sample size correction (identical to Lande 1996's
	# correction of 1 - Simpson's concentration)
	# WARNING: this line was wrong through 28.4.11 because it used ntaxa
	#  instead of abundsum
	$pie = $pie * $abundsum / ( $abundsum - 1 );
	# compute Fisher's alpha using May 1975 eqns. 3.12 and F.13
	my $alpha = 100;
	my $lastalpha;
	while ( abs($alpha - $lastalpha) > 0.001 )	{
		$lastalpha = $alpha;
		$alpha = $ntaxa / log(1 + ($abundsum / $alpha));
	}
	# compute PIelou's J index
	my $pj = $swh / log($ntaxa);
	# compute Buzas-Gibson index
	my $bge = exp($swh) / $ntaxa;

	# abundances have to be sorted and transformed to frequencies
	#  in order to test the distribution against the log series JA 14.5.04
	@abund = sort { $b <=> $a } @abund;
	my @freq;
	for my $i (0..$ntaxa-1)	{
		$freq[$i] = $abund[$i] / $abundsum;
	}

	# now we need to get freq i out of alpha and gamma (Euler's constant)
	# start with May 1975 eqn. F.10
	#  i = -a log(a * freq i) - gamma, so
	#  (i + gamma)/-a = log(a * freq i), so
	#  exp((i +gamma)/-a) / a = freq i
	my $gamma = 0.577215664901532860606512090082;

	# note that we only get the right estimates if we start i at 0
	my $estfreq;
	my $sumestfreq;
	my $sumfreq;
	my $logseriesksd;
	for my $i (0..$ntaxa-1)	{
		my $estfreq = ($i + $gamma) / (-1 * $alpha);
		$estfreq = exp($estfreq) / $alpha;
		$sumestfreq = $sumestfreq + $estfreq;
		$sumfreq = $sumfreq + $freq[$i];
		my $freqdiff = abs($sumfreq - $sumestfreq);
		if ( $freqdiff > $logseriesksd )	{
			$logseriesksd = $freqdiff;
		}
	}


	print "<center>\n";
	print "<div class=\"displayPanel\" style=\"width: 38em; margin-top: 2em;\">\n";
	print "<span class=\"displayPanelHeader\"><span class=\"large\">Diversity statistics for <a href=\"$READ_URL?action=basicCollectionSearch&collection_no=$collection_no\">$collection_name</a></span></span>\n\n";
	print "<div class=\"displayPanelContent\" style=\"width: 38em; padding-top: 1em;\">\n";
	print "<table><tr><td align=\"left\">\n";
	print "<div>Total richness: $ntaxa taxa<br>\n";
	print "Total number of specimens: $abundsum taxa<br>\n";
	print "<div style=\"margin-left: 1em; text-indent: -1em;\">Abundances: <span class=\"verysmall\">".join(', ',@abund)."</span></div>\n";
	printf "Frequency of most common taxon (Berger-Parker <i>d</i>): %.3f<br>\n",$bpd;
	printf "Shannon's <i>H</i>: %.3f<br>\n",$swh;
	printf "Hurlbert's <i>PIE</i>: %.3f<br>\n",$pie;
	printf "Fisher's <i>alpha</i>*: %.2f<br>\n",$alpha;
	printf "Kolmogorov-Smirnov <i>D</i>, data vs. log series**: %.3f",$logseriesksd;
	if ( $logseriesksd > 1.031 / $ntaxa**0.5 )	{
		print " (<i>p</i> < 0.01)<br>\n";
	} elsif ( $logseriesksd > 0.886 / $ntaxa**0.5 )	{
		print " (<i>p</i> < 0.05)<br>\n";
	} else	{
		print " (not significant)<br>\n";
	}
	printf "Pielou's <i>J</i> (evenness): %.3f<br>\n",$pj;
	printf "Buzas-Gibson <i>E</i> (evenness): %.3f</p>\n",$bge;
	print "<div class=small><p>* = solved recursively based on richness and total abundance<br>\n** = test of whether the distribution differs from a log series</div></div></center>\n";
	print "</td></tr></table>\n</div>\n</div>\n\n";

	# rarefy the abundances
	my $maxtrials = 200;
    my @sampledTaxa;
    my @richnesses;
	for my $trial (1..$maxtrials)	{
		my @tempids = @ids;
		my @seen = ();
		my $running = 0;
		for my $n (0..$#ids)	{
			my $x = int(rand() * ($#tempids + 1));
			my $id = splice @tempids, $x, 1;
			$sampledTaxa[$n] = $sampledTaxa[$n] + $running;
			if ( $seen[$id] < $trial )	{
				$sampledTaxa[$n]++;
				$running++;
			}
			push @{$richnesses[$n]} , $running;
			$seen[$id] = $trial;
		}
	}

	my @slevels = (1,2,3,4,5,7,10,15,20,25,30,35,40,45,50,
	      55,60,65,70,75,80,85,90,95,100,
	      150,200,250,300,350,400,450,500,550,600,650,
	      700,750,800,850,900,950,1000,
	      1500,2000,2500,3000,3500,4000,4500,5000,5500,
	      6000,6500,7000,7500,8000,8500,9000,9500,10000);
    my %isalevel;
	for my $sl (@slevels)	{
		$isalevel{$sl} = "Y";
	}

	print "<div class=\"displayPanel\" style=\"width: 38em; margin-top: 2em;\">\n";
	print "<span class=\"displayPanelHeader\"><span class=\"large\" >Rarefaction curve for <a href=\"$READ_URL?action=basicCollectionSearch&collection_no=$collection_no\">$collection_name</a></span></span>\n\n";
	print "<div class=\"displayPanelContent\">\n";

    PBDBUtil::autoCreateDir("$HTML_DIR/public/rarefaction");
	open OUT,">$HTML_DIR/public/rarefaction/rarefaction.csv";
	print "<center><table>\n";
	print "<tr class=\"small\"><td><u>Specimens</u></td><td><u>Species (mean)</u></td><td><u>Species (median)</u></td><td><u>95% confidence limits</u></td></tr>\n";
	print OUT "Specimens\tSpecies (mean)\tSpecies (median)\tLower CI\tUpper CI\n";
	for my $n (0..$#ids)	{
		if ( $n == $#ids || $isalevel{$n+1} eq "Y" )	{
			my @distrib = sort { $a <=> $b } @{$richnesses[$n]};
			printf "<tr class=\"small\"><td align=center>%d</td> <td align=center>%.1f</td> <td align=center>%d</td> <td align=center>%d - %d</td></tr>\n",$n + 1,$sampledTaxa[$n] / $maxtrials,$distrib[99],$distrib[4],$distrib[195];
			printf OUT "%d\t%.1f\t%d\t%d\t%d\n",$n + 1,$sampledTaxa[$n] / $maxtrials,$distrib[99],$distrib[4],$distrib[195];
		}
	}
	close OUT;
	print "</table></center>\n</div>\n</div>\n<p>\n\n";
	print "<p><i>Results are based on 200 random sampling trials.<br>\n";
	print "The data can be downloaded from a <a href=\"$HOST_URL/public/rarefaction/rarefaction.csv\">tab-delimited text file</a>.</i></p></center>\n\n";

    print "<p><div align=\"center\"><b><a href=\"$READ_URL?action=displaySearchColls&type=analyze_abundance\">Search again</a></b></div></p>";
}

# JA 6.1.08
# WARNING: clumsy pseudo-join of coll and occ lists is needed because
#  getCollections separately finds useable colls and occs
# WARNING: this just will not work if taxon name is not queried
# WARNING: this is a very slow algorithm
sub countOccurrences	{
	my ($dbt,$hbo,$collRowsRef,$occRowsRef) = @_;
	my @colls = @{$collRowsRef};
	my @occs = @{$occRowsRef};
	my %lookup = ();
	my @coll_nos = map {$_->{'collection_no'}} @colls;

	print $hbo->stdIncludes($PAGE_TOP);
	print "<div class=\"pageTitle\" style=\"margin-left: 4em;\">Occurrence counts</div>\n\n";

	for my $c ( @coll_nos )	{
		$lookup{$c} = 1;
	}
	my %count = ();
	for  my $o ( @occs )	{
		if ( $lookup{$o->{'collection_no'}} )	{
			if ( $o->{'re_taxon_no'} )	{
				$count{$o->{'re_taxon_no'}}++;
			} elsif ( $o->{'taxon_no'} )	{
				$count{$o->{'taxon_no'}}++;
			}
		}
	}
	my @taxon_nos = keys %count;

	my $sql = "SELECT t.taxon_no,parent_no,taxon_name,common_name,t2.lft,t2.rgt FROM authorities a,taxa_tree_cache t,taxa_list_cache l,taxa_tree_cache t2 WHERE t.synonym_no=child_no AND parent_no=a.taxon_no AND a.taxon_no=t2.taxon_no AND t.taxon_no IN (" . join(',',@taxon_nos) . ") AND common_name IS NOT NULL ORDER BY lft,rgt";
	my @taxa = @{$dbt->getData($sql)};
	my %higher = ();
	for my $t ( @taxa )	{
		$higher{$t->{'parent_no'}}->{'count'} += $count{$t->{'taxon_no'}};
		if ( $higher{$t->{'parent_no'}}->{'count'} > 1 )	{
			if ( $t->{'common_name'} =~ /mouse$/ )	{
				$t->{'common_name'} =~ s/mouse$/mice/;
			} elsif ( $t->{'common_name'} =~ /y$/ )	{
				$t->{'common_name'} =~ s/y$/ies/;
			} elsif ( $t->{'common_name'} !~ /s$/ )	{
				$t->{'common_name'} .= "s";
			}
		}
		$higher{$t->{'parent_no'}}->{'lft'} = $t->{'lft'};
		$higher{$t->{'parent_no'}}->{'rgt'} = $t->{'rgt'};
		$higher{$t->{'parent_no'}}->{'taxon_name'} = $t->{'taxon_name'};
		$higher{$t->{'parent_no'}}->{'common_name'} = $t->{'common_name'};
	}

	print "<div class=\"displayPanel\" style=\"margin-left: 2em; margin-bottom: 2em; padding: 1em;\">\n";
	my @higher_nos = keys %higher;
	@higher_nos = sort { $higher{$a}->{'lft'} <=> $higher{$b}->{'lft'} || $higher{$a}->{'rgt'} <=> $higher{$b}->{'rgt'} } @higher_nos;
	my $total =  0;
	for my $i ( 0..$#higher_nos )	{
		if ( $total < $higher{$higher_nos[$i]}->{'count'} )	{
			$total = $higher{$higher_nos[$i]}->{'count'};
		}
	}
	print "<div style=\"float: none; clear: none; position: relative;\">\n";

	for my $i ( 0..$#higher_nos )	{
		my $h = $higher_nos[$i];
		if ( $total > $higher{$h}->{'count'} )	{
			my $depth = 0;
			for my $j ( 0..$i-1 )	{
				if ( $higher{$h}->{'lft'} > $higher{$higher_nos[$j]}->{'lft'} && $higher{$h}->{'rgt'} < $higher{$higher_nos[$j]}->{'rgt'} && $total > $higher{$higher_nos[$j]}->{'count'} )	{
					$depth++;
				}
			}
			if ( $depth == 1)	{
				print "<div style=\"height: 0.1em; width: 100%; background-color: #C0C0C0; margin-top: 0.5em; margin-bottom: 0.5em;\"></div>\n";
			}
			print "<div style=\"clear: right; padding-left: $depth em;\">$higher{$h}->{'count'} $higher{$h}->{taxon_name}";
			if ( $higher{$h}->{'common_name'} )	{
				print " ($higher{$h}->{'common_name'})";
			}
			print "</div>\n";
		}
	}
	print "</div>\n";
	print "</div>\n";

	print $hbo->stdIncludes($PAGE_BOTTOM);

}

# JA 20,21,28.9.04
# shows counts of taxa within ecological categories for an individual
#  collection
# WARNING: assumes you only care about life habit and diet
# Download.pm uses some similar calculations but I see no easy way to
#  use a common function
sub displayCollectionEcology	{
    my ($dbt,$q,$s,$hbo) = @_;
    my @ranks = $hbo->getList('taxon_rank');
    my %rankToKey = ();
    foreach my $rank (@ranks) {
        my $rank_abbrev = $rank;
        $rank_abbrev =~ s/species/s/;
        $rank_abbrev =~ s/genus/g/;
        $rank_abbrev =~ s/tribe/t/;
        $rank_abbrev =~ s/family/f/;
        $rank_abbrev =~ s/order/o/;
        $rank_abbrev =~ s/class/c/;
        $rank_abbrev =~ s/phylum/p/;
        $rank_abbrev =~ s/kingdom/f/;
        $rank_abbrev =~ s/unranked clade/uc/;
        $rankToKey{$rank} = $rank_abbrev;
    }

    # Get all occurrences for the collection using the most currently reid'd name
    my $collection_no = int($q->param('collection_no'));
    my $collection_name = $q->param('collection_name');

    print "<div align=center><p class=\"pageTitle\">$collection_name (collection number $collection_no)</p></div>";

	my $sql = "(SELECT o.genus_name,o.species_name,o.taxon_no FROM occurrences o LEFT JOIN reidentifications re ON o.occurrence_no=re.occurrence_no WHERE o.collection_no=$collection_no AND re.reid_no IS NULL)".
           " UNION ".
	       "(SELECT re.genus_name,re.species_name,o.taxon_no FROM occurrences o,reidentifications re WHERE o.occurrence_no=re.occurrence_no AND o.collection_no=$collection_no AND re.most_recent='YES')";
    
	my @occurrences = @{$dbt->getData($sql)};

    # First get a list of all the parent taxon nos
	my @taxon_nos = map {$_->{'taxon_no'}} @occurrences;
	my $parents = TaxaCache::getParents($dbt,\@taxon_nos,'array_full');
    # We only look at these categories for now
	my @categories = ("life_habit", "diet1", "diet2","minimum_body_mass","maximum_body_mass","body_mass_estimate");
    my $ecology = Ecology::getEcology($dbt,$parents,\@categories,'get_basis');

	if (!%$ecology) {
		print "<center><p>Sorry, there are no ecological data for any of the taxa</p></center>\n\n";
		print "<center><p><b><a href=\"$READ_URL?action=basicCollectionSearch&collection_no=" . $q->param('collection_no') . "\">Return to the collection record</a></b></p></center>\n\n";
		print $hbo->stdIncludes($PAGE_BOTTOM);
		return;
	} 

    # Convert units for display
    foreach my $taxon_no (keys %$ecology) {
        foreach ('minimum_body_mass','maximum_body_mass','body_mass_estimate') {
            if ($ecology->{$taxon_no}{$_}) {
                if ($ecology->{$taxon_no}{$_} < 1) {
                    $ecology->{$taxon_no}{$_} = Ecology::kgToGrams($ecology->{$taxon_no}{$_});
                    $ecology->{$taxon_no}{$_} .= ' g';
                } else {
                    $ecology->{$taxon_no}{$_} .= ' kg';
                }
            }
        } 
    }
   
	# count up species in each category and combined categories
    my (%cellsum,%colsum,%rowsum);
	for my $row (@occurrences)	{
        my ($col_key,$row_key);
		if ( $ecology->{$row->{'taxon_no'}}{'life_habit'}) {
            $col_key = $ecology->{$row->{'taxon_no'}}{'life_habit'};
        } else {
            $col_key = "?";
        }
        
		if ( $ecology->{$row->{'taxon_no'}}{'diet2'})	{
            $row_key = $ecology->{$row->{'taxon_no'}}{'diet1'}.'/'.$ecology->{$row->{'taxon_no'}}{'diet2'};
		} elsif ( $ecology->{$row->{'taxon_no'}}{'diet1'})	{
            $row_key = $ecology->{$row->{'taxon_no'}}{'diet1'};
        } else {
            $row_key = "?";
        }

        $cellsum{$col_key}{$row_key}++;
		$colsum{$col_key}++;
        $rowsum{$row_key}++;
	}

	print "<div align=\"center\"><p class=\"pageTitle\">Assignments of taxa to categories</p>";
	print "<table cellspacing=0 border=0 cellpadding=4 class=dataTable>";

    # Header generation
	print "<tr><th class=dataTableColumnLeft>Taxon</th>";
	print "<th class=dataTableColumn>Diet</th>";
	print "<th class=dataTableColumn>Life habit</th>";
	print "<th class=dataTableColumn>Body mass</th>";
	print "</tr>\n";

    # Table body
    my %all_rank_keys = ();
	for my $row (@occurrences) {
		print "<tr>";
        if (($row->{'taxon_rank'} && $row->{'taxon_rank'} !~ /species/) ||
            ($row->{'species_name'} =~ /indet/)) {
            print "<td class=dataTableCellLeft>$row->{genus_name} $row->{species_name}</td>";
        } else {
            print "<td class=dataTableCellLeft><i>$row->{genus_name} $row->{species_name}</i></td>";
        }

        # Basis is the rank of the taxon where this data came from. i.e. family/class/etc.
        # See Ecology::getEcology for further explanation
        my ($value,$basis);

        # Handle diet first
        if ($ecology->{$row->{'taxon_no'}}{'diet2'}) {
            $value = $ecology->{$row->{'taxon_no'}}{'diet1'}."/".$ecology->{$row->{'taxon_no'}}{'diet2'};
            $basis = $ecology->{$row->{'taxon_no'}}{'diet1'.'basis'}
        } elsif ($ecology->{$row->{'taxon_no'}}{'diet1'}) {
            $value = $ecology->{$row->{'taxon_no'}}{'diet1'};
            $basis = $ecology->{$row->{'taxon_no'}}{'diet1'.'basis'}
        } else {
            ($value,$basis) = ("?","");
        }
        $all_rank_keys{$basis} = 1;
        print "<td class=dataTableCell>$value<span class='superscript'>$rankToKey{$basis}</span></td>";

        # Then life habit
        if ($ecology->{$row->{'taxon_no'}}{'life_habit'}) {
            $value = $ecology->{$row->{'taxon_no'}}{'life_habit'};
            $basis = $ecology->{$row->{'taxon_no'}}{'life_habit'.'basis'}
        } else {
            ($value,$basis) = ("?","");
        }
        $all_rank_keys{$basis} = 1;
        print "<td class=dataTableCell>$value<span class='superscript'>$rankToKey{$basis}</span></td>";

        # Now body mass
        my ($value1,$basis1,$value2,$basis2) = ("?","","","");
        if ($ecology->{$row->{'taxon_no'}}{'body_mass_estimate'}) {
            $value1 = $ecology->{$row->{'taxon_no'}}{'body_mass_estimate'};
            $basis1 = $ecology->{$row->{'taxon_no'}}{'body_mass_estimate'.'basis'};
            $value2 = "";
            $basis2 = "";
        } elsif ($ecology->{$row->{'taxon_no'}}{'minimum_body_mass'}) {
            $value1 = $ecology->{$row->{'taxon_no'}}{'minimum_body_mass'};
            $basis1 = $ecology->{$row->{'taxon_no'}}{'minimum_body_mass'.'basis'};
            $value2 = $ecology->{$row->{'taxon_no'}}{'maximum_body_mass'};
            $basis2 = $ecology->{$row->{'taxon_no'}}{'maximum_body_mass'.'basis'};
        } 
        $all_rank_keys{$basis1} = 1;
        $all_rank_keys{$basis2} = 1; 
        print "<td class=dataTableCell>$value1<span class='superscript'>$rankToKey{$basis1}</span>";
        print " - $value2<span class='superscript'>$rankToKey{$basis2}</span>" if ($value2);
        print "</td>";

		print "</tr>\n";
	}
    # now print out keys for superscripts above
    print "<tr><td colspan=4>";
    my $html = "Source: ";
    foreach my $rank (@ranks) {
        if ($all_rank_keys{$rank}) {
            $html .= "$rankToKey{$rank} = $rank, ";
        }
    }
    $html =~ s/, $//;
    print $html;
    print "</td></tr>";
	print "</table>";
    print "</div>";

    # Summary information
	print "<p>";
	print "<div align=\"center\"><p class=\"pageTitle\">Counts within categories</p>";
	print "<table border=0 cellspacing=0 cellpadding=4 class=dataTable>";
    print "<tr><td class=dataTableTopULCorner>&nbsp;</td><th class=dataTableTop colspan=".scalar(keys %colsum).">Life Habit</th></tr>";
    print "<tr><th class=dataTableULCorner>Diet</th>";
	for my $habit (sort keys %colsum) {
        print "<td class=dataTableRow align=center>$habit</td>";
	}
	print "<td class=dataTableRow><b>Total<b></tr>";

	for my $diet (sort keys %rowsum) {
		print "<tr>";
		print "<td class=dataTableRow>$diet</td>";
		for my $habit ( sort keys %colsum ) {
			print "<td class=dataTableCell align=right>";
			if ( $cellsum{$habit}{$diet} ) {
				printf("%d",$cellsum{$habit}{$diet});
			} else {
                print "&nbsp;";
            }
			print "</td>";
		}
        print "<td class=dataTableCell align=right><b>$rowsum{$diet}</b></td>";
		print "</tr>\n";
	}
	print "<tr><td class=dataTableColumn><b>Total</b></td>";
	for my $habit (sort keys %colsum) {
		print "<td class=dataTableCell align=right>";
		if ($colsum{$habit}) {
			print "<b>$colsum{$habit}</b>";
		} else {
            print "&nbsp;";
        }
		print "</td>";
	}
	print "<td class=dataTableCell align=right><b>".scalar(@occurrences)."</b></td></tr>\n";
	print "</table>\n";
    print "</div>";

	print "<div align=\"center\"><p><b><a href=\"$READ_URL?action=basicCollectionSearch&collection_no=".$q->param('collection_no')."\">Return to the collection record</a></b> - ";
	print "<b><a href=\"$READ_URL?action=displaySearchColls&type=view\">Search for other collections</a></b></p></div>\n\n";
	print $hbo->stdIncludes($PAGE_BOTTOM);

}

## sub getPaleoCoords
#	Description: Converts a set of floating point coordinates + min/max interval numbers.
#	             determines the age from the interval numbers and returns the paleocoords.
#	Arguments:   $dbh - database handle
#				 $dbt - database transaction object	
#				 $max_interval_no,$min_interval_no - max/min interval no
#				 $f_lngdeg, $f_latdeg - decimal lontitude and latitude
#	Returns:	 $paleolng, $paleolat - decimal paleo longitude and latitutde, or undefined
#                variables if a paleolng/lat can't be found 
#
##
sub getPaleoCoords {
    my $dbt = shift;
    my $max_interval_no = shift;
    my $min_interval_no = shift;
    my $f_lngdeg = shift;
    my $f_latdeg = shift;

    my $dbh = $dbt->dbh;


    # Get time interval information
    my $t = new TimeLookup($dbt);
    my @itvs; 
    push @itvs, $max_interval_no if ($max_interval_no);
    push @itvs, $min_interval_no if ($min_interval_no && $max_interval_no != $min_interval_no);
    my $h = $t->lookupIntervals(\@itvs);

    my ($paleolat, $paleolng,$plng,$plat,$lngdeg,$latdeg,$pid); 
    if ($f_latdeg <= 90 && $f_latdeg >= -90  && $f_lngdeg <= 180 && $f_lngdeg >= -180 ) {
        my $colllowerbound =  $h->{$max_interval_no}{'lower_boundary'};
        my $collupperbound;
        if ($min_interval_no)  {
            $collupperbound = $h->{$min_interval_no}{'upper_boundary'};
        } else {        
            $collupperbound = $h->{$max_interval_no}{'upper_boundary'};
        }
        my $collage = ( $colllowerbound + $collupperbound ) / 2;
        $collage = int($collage+0.5);
        if ($collage <= 600 && $collage >= 0) {
            dbg("collage $collage max_i $max_interval_no min_i $min_interval_no colllowerbound $colllowerbound collupperbound $collupperbound ");

            # Get Map rotation information - needs maptime to be set (to collage)
            # rotx, roty, rotdeg get set by the function, needed by projectPoints below
            my $map_o = new Map;
            $map_o->{maptime} = $collage;
            $map_o->readPlateIDs();
            $map_o->mapGetRotations();

            ($plng,$plat,$lngdeg,$latdeg,$pid) = $map_o->projectPoints($f_lngdeg,$f_latdeg);
            dbg("lngdeg: $lngdeg latdeg $latdeg");
            if ( $lngdeg !~ /NaN/ && $latdeg !~ /NaN/ )       {
                $paleolng = $lngdeg;
                $paleolat = $latdeg;
            } 
        }
    }

    dbg("Paleolng: $paleolng Paleolat $paleolat fx $f_lngdeg fy $f_latdeg plat $plat plng $plng pid $pid");
    return ($paleolng, $paleolat, $pid);
}

# prints AEO age ranges of taxa in a collection so users can understand the
#  collection's age estimate 13.4.08 JA
sub explainAEOestimate	{
	my ($dbt,$q,$s,$hbo) = @_;
	my $proj = "11Nov07_tcdm";
	my $maxevent = 999;

	# get age ranges
	my $taxa = 0;
	my @range = ();
	my %no;
	open IN,"<./data/$proj.ageranges";
	while (<IN>)	{
		$taxa++;
		s/\n//;
		my @data = split /\t/,$_;
		$no{$data[0]} = $taxa;
		$range[$taxa]->{'name'} = $data[0];
		$range[$taxa]->{'occs'} = $data[1];
		(my $z,$range[$taxa]->{'max'}) = split / \(/,$data[3];
		$range[$taxa]->{'max'} =~ s/[^0-9\.]//g;
		$range[$taxa]->{'max'} = sprintf("%.1f",$range[$taxa]->{'max'});
		(my $z,$range[$taxa]->{'min'}) = split / \(/,$data[4];
		$range[$taxa]->{'min'} =~ s/[^0-9\.]//g;
		$range[$taxa]->{'min'} = sprintf("%.1f",$range[$taxa]->{'min'});
		# weird Equus alaskae/crinidens cases
		if ( $range[$taxa]->{'max'} - $range[$taxa]->{'min'} > 40 )	{
			$range[$taxa]->{'max'} = "";
			$range[$taxa]->{'min'} = "";
		}
		# WARNING: there is an error in the computation of .ageranges
		#  files that causes genera to have infinite age ranges if
		#  any of their included species do, so fix the data if you can
		# this works only because genera always come before species
		if ( $data[0] =~ / / && $range[$taxa]->{'max'} ne "" && $range[$taxa]->{'max'} < $maxevent )	{
			my ($g,$s) = split / /,$data[0];
			if ( $range[$no{$g}]->{'max'} > $maxevent )	{
				$range[$no{$g}]->{'max'} = $range[$taxa]->{'max'};
				$range[$no{$g}]->{'min'} = $range[$taxa]->{'min'};
			}
			if ( $range[$taxa]->{'max'} > $range[$no{$g}]->{'max'} )	{
				$range[$no{$g}]->{'max'} = $range[$taxa]->{'max'};
			}
			if ( $range[$taxa]->{'min'} < $range[$no{$g}]->{'min'} || $range[$no{$g}]->{'min'} eq "" )	{
				$range[$no{$g}]->{'min'} = $range[$taxa]->{'min'};
			}
		}
	}
	close IN;

	my $max;
	my $min;
	my $name;
	my $colls = 0;
	my $collno;
	open IN,"<./data/$proj.collnoages";
	while (<IN>)	{
		s/\n//;
		$colls++;
		my @data = split /\t/,$_;
		if ( $data[0] == $q->param('collection_no') )	{
			$max = $data[1];
			$min = $data[2];
			$name = $data[3];
			$collno = $colls;
		}
	}
	close IN;

	open IN,"<./data/$proj.nam";
	# skip the collection names
	for my $i ( 1..$colls )	{
		$_ = <IN>;
		s/\n//;
	}
	$taxa = 0;
	my @taxon;
	while (<IN>)	{
		s/\n//;
		# stop once the section names are encountered
		# this won't work 100%, but close to it
		if ( $_ !~ /^[A-Z]([a-z]*)|([a-z]* [a-z]*)$/ )	{
			last;
		}
		$taxa++;
		$taxon[$taxa] = $_;
	}
	close IN;

	# get the list of taxon numbers for this collection
	open IN,"<./data/$proj.mat";
	for my $i ( 1..$collno )	{
		$_ = <IN>;
	}
	close IN;
	s/ \.\n//;
	my @nos = split / /,$_;
	# delete redundant genus names
	# the genus names are always before the species names
	my %seen;
	for my $n ( @nos )	{
		if ( $range[$n]->{'name'} =~ / / )	{
			my ($g,$s) = split / /,$range[$n]->{'name'};
			$seen{$g}++;
		}
	}
	my @cleannos;
	my $collmax;
	my $collmin = 0;
	for my $n ( @nos )	{
		if ( ! $seen{$range[$n]->{'name'}} )	{
			push @cleannos , $n;
		}
		if ( ! $seen{$range[$n]->{'name'}} && $range[$n]->{'occs'} > 1 )	{
			if ( $range[$n]->{'max'} < $collmax || ! $collmax )	{
				$collmax = $range[$n]->{'max'};
			}
			if ( $range[$n]->{'min'} > $collmin )	{
				$collmin = $range[$n]->{'min'};
			}
		}
	}
	@nos = @cleannos;
	@nos = sort { $range[$b]->{'max'} <=> $range[$a]->{'max'} || $range[$b]->{'min'} <=> $range[$a]->{'min'} || $range[$a]->{'name'} cmp $range[$b]->{'name'} } @nos;
	my %ages;
	$ages{'collection_age'} = $collmax;
	if ( $collmax != $collmin )	{
		$ages{'collection_age'} .= " to " . $collmin;
	}

	$ages{'taxon_ages'} = "<table class=\"small\" style=\"border: 1px solid #909090; padding: 0.75em; margin-left: 1em;\">\n";
	$ages{'taxon_ages'} .= "<tr>\n<td>Genus or species</td>\n<td colspan=\"2\">Age range in Ma</td>\n</tr>\n";
	my @singletons;
	for my $n ( @nos )	{
		if ( $range[$n]->{'occs'} > 1 && $range[$n]->{'max'} ne "" )	{
			$ages{'taxon_ages'} .= "<tr>\n";
			$ages{'taxon_ages'} .= "<td>$range[$n]->{'name'}</td>\n";
			if ( $collmax != $range[$n]->{'max'} )	{
				$ages{'taxon_ages'} .= "<td align=\"right\" style=\"padding-left: 0.75em;\">$range[$n]->{'max'}</td>\n";
			} else	{
				$ages{'taxon_ages'} .= "<td align=\"right\" style=\"padding-left: 0.75em;\"><b>$range[$n]->{'max'}</b></td>\n";
			}
			if ( $collmin != $range[$n]->{'min'} )	{
				$ages{'taxon_ages'} .= "<td align=\"left\">to $range[$n]->{'min'}</td>\n";
			} else	{
				$ages{'taxon_ages'} .= "<td align=\"left\">to <b>$range[$n]->{'min'}</b></td>\n";
			}
			$ages{'taxon_ages'} .= "</tr>\n";
		} elsif ( $range[$n]->{'occs'} == 1 )	{
			push @singletons , $range[$n]->{'name'};
		}
	}
	$ages{'taxon_ages'} .= "</table>\n";
	if ( $#singletons == 0 )	{
		$ages{'note'} = "$singletons[0] is also present, but is not biochronologically informative because it is only found in this collection";
	} elsif ( $#singletons == 1 )	{
		$ages{'note'} = "$singletons[0] and $singletons[1] are also present, but are not biochronologically informative because they are only found in this collection";
	} elsif ( $#singletons > 1 )	{
		$singletons[$#singletons] = "and " . $singletons[$#singletons];
		my $temp = join ', ',@singletons;
		$ages{'note'} = "$temp are also present, but are not biochronologically informative because they are only found in this collection";
	}
	if ( $ages{'note'} )	{
		$ages{'note'} = "<p class=\"small\">" . $ages{'note'} . ".</p>";
	}
	print $hbo->populateHTML('aeo_info', \%ages);
}


## setSecondaryRef($dbt, $collection_no, $reference_no)
# 	Description:	Checks if reference_no is the primary reference or a 
#					secondary reference	for this collection.  If yes to either
#					of those, nothing is done, and the method returns.
#					If the ref exists in neither place, it is added as a
#					secondary reference for the collection.
#
#	Parameters:		$dbh			the database handle
#					$collection_no	the collection being added or edited or the
#									collection to which the occurrence or ReID
#									being added or edited belongs.
#					$reference_no	the reference for the occ, reid, or coll
#									being updated or inserted.	
#
#	Returns:		boolean for running to completion.	
##
sub setSecondaryRef{
	my $dbt = shift;
	my $collection_no = shift;
	my $reference_no = shift;

    unless ($collection_no =~ /^\d+$/ && $reference_no =~ /^\d+$/) {
        return;
    }

	return if(isRefPrimaryOrSecondary($dbt, $collection_no, $reference_no));

	# If we got this far, the ref is not associated with the collection,
	# so add it to the secondary_refs table.
	my $sql = "INSERT IGNORE INTO secondary_refs (collection_no, reference_no) ".
		   "VALUES ($collection_no, $reference_no)";	

    my $dbh_r = $dbt->dbh;
    my $return = $dbh_r->do($sql);
	dbg("ref $reference_no added as secondary for collection $collection_no");
	return 1;
}

## refIsDeleteable($dbt, $collection_no, $reference_no)
#
#	Description		determines whether a reference may be disassociated from
#					a collection based on whether the reference has any
#					occurrences tied to the collection
#
#	Parameters		$dbh			database handle
#					$collection_no	collection to which ref is tied
#					$reference_no	reference in question
#
#	Returns			boolean
#
##
sub refIsDeleteable {
	my $dbt = shift;
	my $collection_no = shift;
	my $reference_no = shift;

    unless ($collection_no =~ /^\d+$/ && $reference_no =~ /^\d+$/) {
        return;
    }
	
	my $sql = "SELECT count(occurrence_no) cnt FROM occurrences ".
			  "WHERE collection_no=$collection_no ".
			  "AND reference_no=$reference_no";
    my $cnt = ${$dbt->getData($sql)}[0]->{'cnt'};

	if($cnt >= 1){
		dbg("Reference $reference_no has $cnt occurrences and is not deletable");
		return 0;
	} else {
		dbg("Reference $reference_no has $cnt occurrences and is deletable");
		return 1;
	}
}

## deleteRefAssociation($dbt, $collection_no, $reference_no)
#
#	Description		Removes association between collection_no and reference_no
#					in the secondary_refs table.
#
#	Parameters		$dbh			database handle
#					$collection_no	collection to which ref is tied
#					$reference_no	reference in question
#
#	Returns			boolean
#
##
sub deleteRefAssociation {
	my $dbt = shift;
	my $collection_no = shift;
	my $reference_no = shift;

    unless ($collection_no =~ /^\d+$/ && $reference_no =~ /^\d+$/) {
        return;
    }

	my $sql = "DELETE FROM secondary_refs where collection_no=$collection_no AND reference_no=$reference_no";
    dbg("Deleting secondary ref association $reference_no from collection $collection_no");
    my $dbh_r = $dbt->dbh;
    my $return = $dbh_r->do($sql);
	return 1;
}

## isRefPrimaryOrSecondary($dbt, $collection_no, $reference_no)
#
#	Description	Checks the collections and secondary_refs tables to see if
#				$reference_no is either the primary or secondary reference
#				for $collection
#
#	Parameters	$dbh			database handle
#				$collection_no	collection with which ref may be associated
#				$reference_no	reference to check for association.
#
#	Returns		positive value if association exists (1 for primary, 2 for
#				secondary), or zero if no association currently exists.
##	
sub isRefPrimaryOrSecondary{
	my $dbt = shift;
	my $collection_no = shift;
	my $reference_no = shift;

    my $dbh = $dbt->dbh;

	# First, see if the ref is the primary.
	my $sql = "SELECT reference_no from collections WHERE collection_no=$collection_no";

    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my %results = %{$sth->fetchrow_hashref()};
    $sth->finish();

	# If the ref is the primary, nothing need be done.
	if($results{reference_no} == $reference_no){
		dbg("ref $reference_no exists as primary for collection $collection_no");
		return 1;
	}

	# Next, see if the ref is listed as a secondary
	$sql = "SELECT reference_no from secondary_refs ".
			  "WHERE collection_no=$collection_no";

    $sth = $dbh->prepare($sql);
    $sth->execute();
    my @results = @{$sth->fetchall_arrayref({})};
    $sth->finish();

	# Check the refs for a match
	foreach my $ref (@results){
		if($ref->{reference_no} == $reference_no){
		    dbg("ref $reference_no exists as secondary for collection $collection_no");
			return 2;
		}
	}

	# If we got this far, the ref is neither primary nor secondary
	return 0;
}


1;

