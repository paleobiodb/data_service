# the following functions were moved into CollectionEntry.pm by JA 4.6.13:
# displayCollectionForm, processCollectionForm, getReleaseDate,
#  getReleaseString, validateCollectionForm, setMaIntervalNo,
#  displayCollectionDetails, fromMinSec, toMinSec, displayCollectionDetailsPage,
#  buildTaxonomicList, formatOccurrenceTaxonName, getSynonymName,
#  getReidHTMLTableByOccNum, getPaleoCoords, setSecondaryRef, refIsDeleteable,
#  deleteRefAssociation, isRefPrimaryOrSecondary
# formatCoordinate appears in both functions because it is used by
#  CollectionEntry::displayCollectionDetails and
#  Collection::basicCollectionInfo

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
use Constants qw($WRITE_URL $HTML_DIR $HOST_URL $TAXA_TREE_CACHE $DB $COLLECTIONS $COLLECTION_NO $OCCURRENCES $OCCURRENCE_NO $PAGE_TOP $PAGE_BOTTOM);

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
        if ($options{'include_old_ids'}) {
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
        my ($yyyy,$mm,$dd) = ($options{'year'},$options{'month'},$options{'day_of_month'});
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
	if ( $options{'collection_names'} ) {
		my $OPTION = $options{'collection_names'};
		# only match entire numbers within names, not parts
		my $word = $dbh->quote('%'.$OPTION.'%');
		my $integer = $dbh->quote('.*[^0-9]'.$OPTION.'(([^0-9]+)|($))');
		# interpret plain integers as either names, collection years,
		#  or collection_nos
		if ($OPTION =~ /^\d+$/) {
			push @where, "(c.collection_name REGEXP $integer OR c.collection_aka REGEXP $integer OR c.collection_dates REGEXP $integer OR c.$COLLECTION_NO=$OPTION)";
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
			push @where, "(c.collection_name LIKE $word OR c.collection_aka LIKE $word OR c.collectors LIKE $word)";
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
    
	if ( int($options{'reference_no'}) )	{
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
    if ( ($options{'research_group'} =~ /^(?:decapod|ETE|5%|1%|PACED|PGAP)$/ || int($options{'reference_no'})) ) {
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
		$val =~ s/,/','/g;
		push @where, "(c.lithology1 IN ($val) OR c.lithology2 IN ($val))"; 
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

    # JA 27.9.11
    if ( $options{'min_lat'} && $options{'min_lat'} !~ /[^\-0-9]/ && $options{'min_lat'} > -90 && $options{'min_lat'} < 90 ) {
        if ( $options{'min_lat'} > $options{'max_lat'} )	{
            my $foo = $options{'min_lat'};
            $options{'min_lat'} = $options{'max_lat'};
            $options{'max_lat'} = $foo;
        }
        push @where , "IF(c.latdir='south',concat(\"-\",c.latdeg),c.latdeg)>=".$options{'min_lat'};
    }
    if ( $options{'max_lat'} && $options{'max_lat'} !~ /[^\-0-9]/ && $options{'max_lat'} > -90 && $options{'max_lat'} < 90 ) {
        push @where , "IF(c.latdir='south',concat(\"-\",c.latdeg),c.latdeg)<=".$options{'max_lat'};
    }
    if ( $options{'min_lng'} && $options{'min_lng'} !~ /[^\-0-9]/ && $options{'min_lng'} > -180 && $options{'min_lng'} < 180 ) {
        if ( $options{'min_lng'} > $options{'max_lng'} )	{
            my $foo = $options{'min_lng'};
            $options{'min_lng'} = $options{'max_lng'};
            $options{'max_lng'} = $foo;
        }
        push @where , "IF(c.lngdir='west',concat(\"-\",c.lngdeg),c.lngdeg)>=".$options{'min_lng'};
    }
    if ( $options{'max_lng'} && $options{'max_lng'} !~ /[^\-0-9]/ && $options{'max_lng'} > -180 && $options{'max_lng'} < 180 ) {
        push @where , "IF(c.lngdir='west',concat(\"-\",c.lngdeg),c.lngdeg)<=".$options{'max_lng'};
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
        next if ($field =~ /^(?:environment|localbed|regionalbed|research_group|reference_no|max_interval_no|min_interval_no|country|min_lat|max_lat|min_lng|max_lng|plate)$/);

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
			$message .= "<a href=\"?a=mapForm\"><b>Try again</b></a>";
		} elsif ( $options{"calling_script"} eq "Confidence" )	{
			$message .= "<a href=\"?a=displaySearchSectionForm\"><b>Try again</b></a>";
		} elsif ( $options{"type"} eq "add" )	{
			$message .= "<a href=\"$WRITE_URL?a=displaySearchCollsForAdd&type=add\"><b>Try again</b></a>";
		} else	{
			$message .= "<a href=\"?a=displaySearchColls&type=$options{type}\"><b>Try again</b></a>";
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
    $p->getReadRows ( $sth, \@dataRows, $limit, \$totalRows);

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


# split out of CollectionEntry::displayCollectionDetails JA 6.11.09
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

# split off from basicCollectionInfo JA 28.6.12
sub getTaxonomicList	{
	my ($dbt,$collNos) = @_;
	my $sql = "(SELECT o.collection_no,lft,o.reference_no,o.genus_reso,o.genus_name,o.subgenus_reso,o.subgenus_name,o.species_reso,o.species_name,o.taxon_no,synonym_no,o.comments,'' FROM occurrences o LEFT JOIN reidentifications re ON (o.occurrence_no=re.occurrence_no) LEFT JOIN $TAXA_TREE_CACHE t ON o.taxon_no=t.taxon_no WHERE o.collection_no IN (".join(',',@$collNos).") AND re.reid_no IS NULL AND lft>0) UNION (SELECT o.collection_no,lft,re.reference_no,re.genus_reso,re.genus_name,re.subgenus_reso,re.subgenus_name,re.species_reso,re.species_name,re.taxon_no,synonym_no,o.comments,re.comments FROM occurrences o,reidentifications re,$TAXA_TREE_CACHE t WHERE o.occurrence_no=re.occurrence_no AND re.collection_no IN (".join(',',@$collNos).") AND re.most_recent='YES' AND re.taxon_no=t.taxon_no AND lft>0) UNION (SELECT o.collection_no,999999,o.reference_no,o.genus_reso,o.genus_name,o.subgenus_reso,o.subgenus_name,o.species_reso,o.species_name,o.taxon_no,0,o.comments,'' FROM occurrences o WHERE collection_no IN (".join(',',@$collNos).") AND taxon_no=0) ORDER BY lft";
	return \@{$dbt->getData($sql)};
}

# started another heavy rewrite 26.9.11, finished it 25.10.11
sub getClassOrderFamily	{
	my $dbt = shift;
	my $rowref_ref = shift;
	my $rowref;
	if ( $rowref_ref )	{
		$rowref = ${$rowref_ref};
	}
	my $class_array_ref = shift;
	my @class_array = @{$class_array_ref};
	if ( $#class_array == 0 )	{
		return $rowref;
	}

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
		if ( ( $t->{'taxon_rank'} eq "family" || $t->{'taxon_name'} =~ /idae$/ ) && ! $t->{'family'} )	{
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

	# we need to know which parents have ever been ranked as either a class
	#  or an order
	my (@other_parent_nos,%wasClass,%wasntClass,%wasOrder,%wasntOrder);
	# first mark names currently ranked at these levels
	for my $i ( $toplowlevel..$#class_array ) {
		my $no = $class_array[$i]->{'taxon_no'};
		# used by jsonCollection 30.6.12
		if ( ! $rowref->{'category'} )	{
			if ( $class_array[$i]->{'taxon_name'} =~ /Vertebrata|Chordata/ )	{
				$rowref->{'category'} = "vertebrate";
			} elsif ( $class_array[$i]->{'taxon_name'} =~ /Insecta/ )	{
				$rowref->{'category'} = "insect";
			} elsif ( $class_array[$i]->{'taxon_name'} =~ /Animalia|Metazoa/ )	{
				$rowref->{'category'} = "invertebrate";
			} elsif ( $class_array[$i]->{'taxon_name'} eq "Plantae" )	{
				$rowref->{'category'} = "plant";
			}
		}
		if ( $class_array[$i]->{'taxon_rank'} eq "class" )	{
			$wasClass{$no} = 9999;
		} elsif ( $class_array[$i]->{'taxon_rank'} eq "order" )	{
			$wasOrder{$no} = 9999;
		} elsif ( $no )	{
			push @other_parent_nos , $no;
		}
	}
	# find other names previously ranked at these levels
	if ( @other_parent_nos )	{
		my $sql = "SELECT taxon_rank,spelling_no as parent_no,count(*) c FROM authorities a,opinions o,$TAXA_TREE_CACHE t WHERE a.taxon_no=child_spelling_no AND child_no=t.taxon_no AND spelling_no IN (".join(',',@other_parent_nos).") GROUP BY taxon_rank,child_spelling_no";
		for my $p ( @{$dbt->getData($sql)} )	{
			if ( $p->{'taxon_rank'} eq "class" )	{
				$wasClass{$p->{'parent_no'}} += $p->{'c'};
			} else	{
				$wasntClass{$p->{'parent_no'}} += $p->{'c'};
			}
			if ( $p->{'taxon_rank'} eq "order" )	{
				$wasOrder{$p->{'parent_no'}} += $p->{'c'};
			} else	{
				$wasntOrder{$p->{'parent_no'}} += $p->{'c'};
			}
		}
	}

	# find the oldest parent most frequently ranked an order
	# use publication year as a tie breaker
	my ($maxyr,$mostoften,$orderlevel) = ('',-9999,'');
	for my $i ( $toplowlevel..$#class_array ) {
		my $t = $class_array[$i];
		if ( $wasClass{$t->{'taxon_no'}} > 0 || $t->{'taxon_rank'} =~ /phylum|kingdom/ )	{
			last;
		}
		if ( ( $wasOrder{$t->{'taxon_no'}} - $wasntOrder{$t->{'taxon_no'}} > $mostoften && $wasOrder{$t->{'taxon_no'}} > 0 ) || ( $wasOrder{$t->{'taxon_no'}} - $wasntOrder{$t->{'taxon_no'}} == $mostoften && $wasOrder{$t->{'taxon_no'}} > 0 && $t->{'pubyr'} < $maxyr ) )	{
			$mostoften = $wasOrder{$t->{'taxon_no'}} - $wasntOrder{$t->{'taxon_no'}};
			$maxyr = $t->{'pubyr'};
			$rowref->{'order'} = $t->{'taxon_name'};
			$rowref->{'order_no'} = $t->{'taxon_no'};
			$orderlevel = $i + 1;
		}
	}
	# if that fails then none of the parents have ever been orders,
	#  so use the oldest name between the levels of family and
	#  at-least-once class
	if ( $rowref->{'order_no'} == 0 )	{
		for my $i ( $toplowlevel..$#class_array ) {
			my $t = $class_array[$i];
			if ( $wasClass{$t->{'taxon_no'}} > 0 || $t->{'taxon_rank'} =~ /phylum|kingdom/ )	{
				last;
			}
			if ( ! $maxyr || $t->{'pubyr'} < $maxyr )	{
				$maxyr = $t->{'pubyr'};
				$rowref->{'order'} = $t->{'taxon_name'};
				$rowref->{'order_no'} = $t->{'taxon_no'};
				$orderlevel = $i + 1;
			}
		}
	}

	# find the oldest parent ever ranked as a class
	my ($maxyr,$mostoften) = ('',-9999);
	for my $i ( $orderlevel..$#class_array ) {
		my $t = $class_array[$i];
		if ( ( $wasClass{$t->{'taxon_no'}} - $wasntClass{$t->{'taxon_no'}} > $mostoften && $wasClass{$t->{'taxon_no'}} > 0 ) || ( $wasClass{$t->{'taxon_no'}} - $wasntClass{$t->{'taxon_no'}} == $mostoften && $wasClass{$t->{'taxon_no'}} > 0 && $t->{'pubyr'} < $maxyr ) )	{
			$mostoften = $wasClass{$t->{'taxon_no'}} - $wasntClass{$t->{'taxon_no'}};
			$maxyr = $t->{'pubyr'};
			$rowref->{'class'} = $t->{'taxon_name'};
			$rowref->{'class_no'} = $t->{'taxon_no'};
		}
	}
	# otherwise we're really in trouble, so use the oldest name available
	if ( $rowref->{'class_no'} == 0 )	{
		for my $i ( $orderlevel..$#class_array ) {
			my $t = $class_array[$i];
			if ( $t->{'taxon_rank'} =~ /phylum|kingdom/ )	{
				last;
			}
			if ( ! $maxyr || $t->{'pubyr'} < $maxyr )	{
				$maxyr = $t->{'pubyr'};
				$rowref->{'class'} = $t->{'taxon_name'};
				$rowref->{'class_no'} = $t->{'taxon_no'};
			}
		}
	}
	if ( ! $rowref->{'category'} )	{
		$rowref->{'category'} = "microfossil";
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
		if ( $coll )	{
			$q->param($COLLECTION_NO => $NO);
			basicCollectionInfo($dbt,$q,$s,$hbo);
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
	if ( $NAME =~ / (group|grp|formation|fm|member|mbr|)$/i )	{
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
	$sql = "SELECT $fields FROM $COLLECTIONS WHERE (geological_group LIKE '%".$NAME."%' OR formation LIKE '%".$NAME."%' OR member LIKE '%".$NAME."%')";
	@colls = @{$dbt->getData($sql)};
	if ( @colls )	{
		route();
		return 1;
	}

	sub route()	{
		if ( scalar(@colls) == 0 )	{
			return;
		} elsif ( $#colls == 0 )	{
			$q->param($COLLECTION_NO => $colls[0]->{$COLLECTION_NO} );
			basicCollectionInfo($dbt,$q,$s,$hbo);
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
		if (! $q->request_method() =~ /GET|POST/i && ! $q->param('is_real_user') && ! $s->isDBMember())	{
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

	my $sql = "SELECT c.*,DATE_FORMAT(release_date, '%Y%m%d') AS rd_short,CONCAT(p.first_name,' ',p.last_name) AS authorizer,CONCAT(p2.first_name,' ',p2.last_name) AS enterer FROM collections c,person p,person p2 WHERE authorizer_no=p.person_no AND enterer_no=p2.person_no AND collection_no=".$q->param('collection_no');
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

	my ($max,$min);
	# this is a lot easier than a quadruple plus join and some unions,
	#  etc., etc. and the table is tiny
	$sql = "SELECT interval_no,TRIM(CONCAT(eml_interval,' ',interval_name)) AS interval_name FROM intervals";
	my %interval;
	$interval{$_->{'interval_no'}} = $_->{'interval_name'} foreach @{$dbt->getData($sql)};
	$sql = "SELECT base_age,top_age,epoch_no,period_no FROM interval_lookup WHERE interval_no=".$c->{'max_interval_no'};
	$max = ${$dbt->getData($sql)}[0];
	my $maxName .= ( $interval{$max->{'period_no'}} =~ /Paleogene|Neogene|Quaternary/ && $max->{'epoch_no'} > 0 ) ? $interval{$max->{'epoch_no'}} : $interval{$max->{'period_no'}};
	$header .= " (".$maxName;
	if ( $c->{'min_interval_no'} > 0 )	{
		$sql = "SELECT TRIM(CONCAT(i.eml_interval,' ',i.interval_name)) AS interval_name,base_age,top_age,i2.interval_name AS epoch,i3.interval_name AS period FROM intervals i,intervals i2,intervals i3,interval_lookup l WHERE i.interval_no=".$c->{'min_interval_no'}." AND i.interval_no=l.interval_no AND i2.interval_no=l.epoch_no AND i3.interval_no=period_no";
		$min = ${$dbt->getData($sql)}[0];
		my $minName .= ( $interval{$min->{'period_no'}} =~ /Paleogene|Neogene|Quaternary/ && $min->{'epoch_no'} > 0 ) ? $interval{$min->{'epoch_no'}} : $interval{$min->{'period_no'}};
		$header .= ( $maxName ne $minName ) ? " to ".$minName : "";
	}
	$c->{'country'} =~ s/^United/the United/;

	# I'm forced to do this by an iPhone bug
	my $marginLeft = ( $ENV{'HTTP_USER_AGENT'} =~ /Mobile/i && $ENV{'HTTP_USER_AGENT'} !~ /iPad/i ) ? "-4em" : "0em";

	print qq|

<script language="JavaScript" type="text/javascript">
<!-- Begin
function showAuthors()	{
	alldivs = document.getElementsByTagName('div');
	for ( i = 0; i < alldivs.length; i++ )	{
	//for ( i = 0; i < 33; i++ )	{
//alert(alldivs[i].class);
		if ( alldivs[i].className == 'noAuthors' )	{
			alldivs[i].style.display = 'none';
		} else if ( alldivs[i].className == 'withAuthors' )	{
			alldivs[i].style.display = 'block';
		}
	}
}
//  End -->
</script>

<center>
<div class="displayPanel" style="margin-left: $marginLeft; margin-top: 2em; margin-bottom: 2em; text-align: left; width: 80%;">
<span class="displayPanelHeader">$header of $c->{'country'})</span>
<div align="left" class="small displayPanelContent" style="padding-left: 1em; padding-bottom: 1em;">

|;
	$c->{'country'} =~ s/^the United/United/;

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

	if ( $c->{'latlng_basis'} )	{
		$c->{'latlng_basis'} =~ s/(unpublished)/based on $1/;
		print "<p $mockLI coordinate $c->{'latlng_basis'}</p>\n\n";
	}
	$c->{'geogscale'} ? print "<p $mockLI $c->{'geogscale'}-level geographic resolution</p>\n\n" : "";
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

	print $interval{$c->{'max_interval_no'}}." ";
	if ( $c->{'min_interval_no'} > 0 )	{
		print " to ";
		print $interval{$c->{'max_interval_no'}}." ";
	}
	if ( $max->{'base_age'} )	{
		printf "(%.1f - ",$max->{'base_age'};
		if ( ! $min->{'top_age'} )	{
			printf "%.1f",$max->{'top_age'};
		} else	{
			printf "%.1f",$min->{'top_age'};
		}
		print " Ma)";
	}
	print "</p>\n\n";

	$c->{'stratcomments'} ? print "<p $mockLI $c->{'stratcomments'}</p>\n\n" : "";
	$c->{'stratscale'} ? print "<p $mockLI $c->{'stratscale'}-level stratigraphic resolution</p>\n\n" : "";

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
		$c->{'collectors'} ? print " by ".$c->{'collectors'} : "";
		$c->{'collection_dates'} ? print " in ".$c->{'collection_dates'} : "";
		$c->{'museum'} ? print "; reposited in the ".$c->{'museum'} : "";
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
	} elsif ( $c->{'collection_comments'} )	{
		print "<p>Collection methods: $c->{'collection_comments'}</p>\n\n";
	}

	$sql = "SELECT * FROM refs WHERE reference_no=".$c->{'reference_no'};
	my $ref = ${$dbt->getData($sql)}[0];
	print "<p $indent>Primary reference: ".Reference::formatLongRef($ref,'link_id'=>1)." <a class=\"verysmall\" href=\"?a=displayReference&reference_no=$c->{reference_no}\">more details</a>";
	if ( $s->isDBMember() ) {
		print " - <a class=\"verysmall\" href=\"$WRITE_URL?a=displayRefResults&amp;type=edit&amp;reference_no=$c->{reference_no}\">edit</a>";
	}
	print "</p>\n\n";

	$c->{'collection_type'} ? print "<p $indent>Purpose of describing collection: $c->{'collection_type'} analysis<p>\n\n" : "";

	if ( $c->{''} )	{
		print "<p>: ";
		print $c->{''};
		print "</p>\n\n";
	}

	$c->{'created'} =~ s/ .*//;
	my ($y,$m,$d) = split /-/,$c->{'created'};
	print "<p $indent>PaleoDB collection $c->{'collection_no'}: authorized by $c->{'authorizer'}, entered by $c->{'enterer'} on $d.$m.$y";

	$sql = "(SELECT distinct(concat(first_name,' ',last_name)) AS enterer FROM occurrences o,person p WHERE enterer_no=person_no AND collection_no=$c->{'collection_no'} AND enterer_no!=".$c->{'enterer_no'}.") UNION (SELECT distinct(concat(first_name,' ',last_name)) AS enterer FROM reidentifications r,person p WHERE enterer_no=person_no AND collection_no=$c->{'collection_no'} AND enterer_no!=".$c->{'enterer_no'}.")";
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

	if ( $c->{'license'} )	{
		my $full_license = $c->{'license'};
		$full_license =~ s/(CC BY)(|-)/attribution$2/;
		$full_license =~ s/SA/sharealike/;
		$full_license =~ s/NC/noncommercial/;
		$full_license =~ s/ND/no derivatives/;
		print "<p $indent>Creative Commons license: $c->{'license'} ($full_license)</p>\n";
	}

	if ( $is_real_user == 0 || $not_bot == 0 )	{
		print $hbo->stdIncludes($PAGE_BOTTOM);
		return;
	}

	# the following is basically a complete rewrite of buildTaxonomicList
	# so what?

	my @occs = @{getTaxonomicList($dbt,[$c->{'collection_no'}])};
	my (%bad,%lookup,@need_authors,%authors,%rankOfNo);
	for my $o ( @occs )	{
		if ( $o->{'taxon_no'} != $o->{'synonym_no'} )	{
			$bad{$o->{'taxon_no'}} = $o->{'synonym_no'};
		} elsif ( $o->{'taxon_no'} > 0 )	{
			push @need_authors , $o->{'taxon_no'};
		}
	}
	if ( %bad )	{
		$sql = "SELECT a.taxon_no,a.taxon_name bad,a.taxon_rank,synonym_no,a2.taxon_name good FROM authorities a,authorities a2,$TAXA_TREE_CACHE t,refs r WHERE a.taxon_no=t.taxon_no AND t.synonym_no=a2.taxon_no AND a2.reference_no=r.reference_no AND a.taxon_no IN (".join(',',keys %bad).")";
		my @seniors = @{$dbt->getData($sql)};
		for my $s ( @seniors )	{
		# ignore rank changes that don't change spellings
			if ( $s->{'bad'} ne $s->{'good'} )	{
				if ( $s->{'taxon_rank'} =~ /genus|species/ )	{
					$s->{'good'} = "<i>".$s->{'good'}."</i>";
				}
				$s->{'good'} = "<a href=\"?a=basicTaxonInfo&amp;taxon_no=$s->{'synonym_no'}\">".$s->{'good'}."</a>";
				$lookup{$s->{'synonym_no'}} = $s->{'good'};
				push @need_authors , $s->{'synonym_no'};
			}
		}
	}
	if ( @need_authors )	{
		$sql = "SELECT taxon_no,taxon_rank,IF(ref_is_authority='YES',r.author1last,a.author1last) author1last,IF(ref_is_authority='YES',r.author2last,a.author2last) author2last,IF(ref_is_authority='YES',r.otherauthors,a.otherauthors) otherauthors,IF(ref_is_authority='YES',r.pubyr,a.pubyr) pubyr FROM authorities a,refs r WHERE a.reference_no=r.reference_no AND taxon_no IN (".join(',',@need_authors).")";
		my @ref_info = @{$dbt->getData($sql)};
		$authors{$_->{'taxon_no'}} = Reference::formatShortRef($_) foreach @ref_info;
		$rankOfNo{$_->{'taxon_no'}} = $_->{'taxon_rank'} foreach @ref_info;
	}
	my (%isRef,@refs,$refList,%refCiteNo);
	$isRef{$_->{'reference_no'}}++ foreach @occs;
	if ( %isRef )	{
		@refs = @{$dbt->getData("SELECT reference_no,author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no IN (".join(',',keys %isRef).") AND reference_no!=$c->{'reference_no'} ORDER BY author1last,author2last,otherauthors,pubyr")};
	}
	if ( $#refs > 0 )	{
		
		for my $i ( 0..$#refs )	{
			$refList .= sprintf("; <sup>%d</sup>".Reference::formatShortRef($refs[$i],'link_id'=>1),$i+1);
			$refCiteNo{$refs[$i]->{'reference_no'}} = $i + 1;
		}
		$refList =~ s/^; //;
	}

	print "<div style=\"margin-left: 0em; margin-right: 1em; border-top: 1px solid darkgray;\">\n\n";
	print "<p class=\"large\" style=\"margin-top: 0.5em; margin-bottom: 0em;\">Taxonomic list</p>\n\n";
	if ( $c->{'taxonomy_comments'} )	{
		print qq|<div class="verysmall" style="margin-left: 2em; margin-top: 0.5em; text-indent: -1em;"> &bull; $c->{'taxonomy_comments'}</div>\n\n|;
	}
	print qq|<div class="mockLink" onClick="showAuthors();" style="margin-left: 1em; margin-top: 0.5em; margin-bottom: 0.5em;"> Show authors, comments, and common names</div>\n\n|;
	print "<table class=\"small\" cellspacing=\"0\" cellpadding=\"4\" class=\"taxonomicList\">\n\n";
	my ($lastclass,$lastorder,$lastfamily,$class,@with_authors);
	for my $o ( @occs )	{
		# format taxon names
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
			$o->{'formatted'} = "<a href=\"?a=basicTaxonInfo&amp;taxon_no=$o->{'taxon_no'}\">".$o->{'formatted'}."</a>";
		} elsif ( ! $o->{'taxon_no'} )	{
			my $name = $o->{'genus_name'};
			if ( $o->{'species_name'} !~ /(sp|spp|indet)\./ )	{
				$name .= " ".$o->{'species_name'};
			}
			$o->{'formatted'} = "<a href=\"?a=basicTaxonInfo&amp;taxon_name=$name\">".$o->{'formatted'}."</a>";
		}
		if ( $postfix )	{
			$o->{'formatted'} .= " ".$postfix;
		}
		if ( $lookup{$o->{'synonym_no'}} )	{
			$o->{'formatted'} = '"'.$o->{'formatted'}.'" = '.$lookup{$o->{'synonym_no'}};
		}
$o->{'formatted'} .= qq|<sup><span class="tiny">$refCiteNo{$o->{'reference_no'}}</span></sup>|;
		if ( $o->{'abund_value'} )	{
			$o->{'formatted'} .= "[".$o->{'abund_value'}."]";
		}

		# get author/year info
		my $author = $authors{$o->{'synonym_no'}};
		# erase author if the classified taxon isn't a species but
		#  the name looks like a proper species (= no funny characters)
		if ( $rankOfNo{$o->{'synonym_no'}} !~ /species/ && $o->{'species_name'} !~ /[^a-z]/ && $o->{'species_reso'} !~ /"/ )	{
			$author = "";
		}

		# get class/order/family names
		my $class_hash = TaxaCache::getParents($dbt,[$o->{'taxon_no'}],'array_full');
		my @class_array = @{$class_hash->{$o->{'taxon_no'}}};
		my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$o->{'taxon_no'}},['taxon_name','taxon_rank','pubyr','common_name']);
		unshift @class_array , $taxon;
		$o = getClassOrderFamily($dbt,\$o,\@class_array);
		if ( ! $o->{'class'} && ! $o->{'order'} && ! $o->{'family'} )	{
			$o->{'class'} = "unclassified";
		}

		# put everything together
#$o->{formatted} .= $rankOfNo{$o->{'synonym_no'}};
		if ( $o->{'class'} ne $lastclass || $o->{'order'} ne $lastorder || $o->{'family'} ne $lastfamily )	{
			if ( $lastclass || $lastorder || $lastfamily )	{
				print "\n</div>\n<div class=\"withAuthors\">\n<div style=\"padding-bottom: 0.2em;\">".join("</div><div style=\"padding-bottom: 0.2em;\">\n",@with_authors)."</div></div>\n";
				print "</tr>\n";
				@with_authors = ();
			}
			my @parents;
			if ( $class =~ /dark/ )	{
				$class = '';
			} elsif ( $#occs > 0 )	{
				$class = ' class="darkList"';
			}
			if ( $o->{'class'} ne $lastclass )	{
				my $style = ( $o->{'class'} ne $occs[0]->{'class'} ) ? ' style="padding-top: 1.5em;"' : "";
				print "<tr><td class=\"large\" colspan=\"2\"$style>".$o->{'class'}."</td></tr>\n";
				$class = ' class="darkList"';
			}
			print "<tr$class>\n<td valign=\"top\"><nobr>";
			print "&nbsp;".join(' - ',$o->{'order'},$o->{'family'})."</nobr></td>\n";
			print "<td valign=\"top\"><div class=\"noAuthors\">$o->{'formatted'}";
			push @with_authors , $o->{'formatted'}." ".$author." <span style=\"float: right; clear: right; padding-left: 2em;\">$o->{'common_name'}</span>";
			$with_authors[$#with_authors] .= ( $o->{'comments'} ) ? "<br>\n<div class=\"verysmall\" style=\"padding-left: 0.75em; padding-top: 0.2em; padding-bottom: 0.3em;\">$o->{'comments'}</div>\n" : "";
		} else	{
			print ", $o->{'formatted'}";
			push @with_authors , $o->{'formatted'}." ".$author." <span style=\"float: right; clear: right; padding-left: 2em;\">$o->{'common_name'}</span>";
			$with_authors[$#with_authors] .= ( $o->{'comments'} ) ? "<br>\n<div class=\"verysmall\" style=\"padding-left: 0.75em; padding-top: 0.2em; padding-bottom: 0.3em;\">$o->{'comments'}</div>\n" : "";
		}
		$lastclass = $o->{'class'};
		$lastorder = $o->{'order'};
		$lastfamily = $o->{'family'};
	}
	print "\n</div>\n<div class=\"withAuthors\">\n<div style=\"padding-bottom: 0.2em;\">".join("</div><div style=\"padding-bottom: 0.2em;\">\n",@with_authors)."</div></div>\n";
	print "</tr>\n";
	print "</table>\n\n";
	print "<div class=\"verysmall\" style=\"margin-top: 0.5em;\">$refList</div>\n";

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
			 print qq|<a href="$WRITE_URL?a=displayCollectionForm&collection_no=$c->{'collection_no'}">Edit collection</a> - |;
		}
		print qq|<a href="$WRITE_URL?a=displayCollectionForm&prefill_collection_no=$c->{'collection_no'}">Add a collection copied from this one</a> - |;
		if ($can_modify->{$c->{'authorizer_no'}} || $s->isSuperUser) {  
			print qq|<a href="$WRITE_URL?a=displayOccurrenceAddEdit&collection_no=$c->{'collection_no'}">Edit taxonomic list</a>|;
		}
		if ( $s->get('role') =~ /authorizer|student|technician/ )	{
			print qq| - <a href="$WRITE_URL?a=displayOccsForReID&collection_no=$c->{'collection_no'}">Reidentify taxa</a>|;
		}
		print "\n</div>\n\n";
	}

        print qq|
<form method="GET" action="">
<input type="hidden" name="a" value="basicCollectionSearch">
<input type="hidden" name="last_collection" value="$c->{'collection_no'}">
<span class="small">
<input type="text" name="collection_name" value="Search again" size="24" onFocus="textClear(collection_name);" onBlur="textRestore(collection_name);" style="font-size: 1.0em;">
</span>
</form>

|;

	print "<br>\n\n";
	print "</div>\n\n";
print "</center>";

	print $hbo->stdIncludes($PAGE_BOTTOM);

}

# JA 26-28.6.12
sub jsonCollection	{
	my ($dbt,$q,$s) = @_;
	my %options;
	$options{$_} = $q->param($_) foreach $q->param();
	my ($colls_ref) = getCollections($dbt,$s,\%options,['*']);

	my %intervalInSet;
	for my $c ( @$colls_ref )	{
		$intervalInSet{$c->{'max_interval_no'}}++;
		$intervalInSet{$c->{'min_interval_no'}}++ if ( $c->{'min_interval_no'} > 0 );
	}
	my $t = new TimeLookup($dbt);
	my @intervals = keys %intervalInSet;
	my $lookup = $t->lookupIntervals(\@intervals);
	my (%occs,%seenTaxa,%cof);
	my @coll_nos = map { $_->{collection_no} } @$colls_ref;
	for my $c ( @{getTaxonomicList($dbt,\@coll_nos)} )	{
		push @{$occs{$c->{'collection_no'}}} , $c;
		$seenTaxa{$c->{'taxon_no'}}++;
	}
	for my $no ( keys %seenTaxa )	{
		my $class_hash = TaxaCache::getParents($dbt,[$no],'array_full');
		my @class_array = @{$class_hash->{$no}};
		my $child = { 'taxon_no' => $no } ;
		unshift @class_array , $child;
		my $child = getClassOrderFamily($dbt,\$child,\@class_array);
		$cof{$no}{$_} = $child->{$_} foreach ('category','common_name','class','order','family');
	}

	print qq|{ "collections": [ { |;
	my @colls;
	for my $c ( @$colls_ref )	{
		my @attributes;
		my $coll_string;

		for my $f ( 'lithadj','minor_lithology','lithadj2','minor_lithology2','museum' )	{
			$c->{$f} =~ s/,/ /g;
		}

		$c->{'reference'} = Reference::formatLongRef($dbt,$c->{'reference_no'});
		# strip out HTML plus the authorizer/enterer info
		$c->{'reference'} =~ s/<span.*span>//;
		$c->{'reference'} =~ s/<(\/|)(b|i|u)>//g;

		$c->{'lat'} = sprintf("%.1f",$c->{'lat'});
		$c->{'lng'} = sprintf("%.1f",$c->{'lng'});

		my $max_lookup = $lookup->{$c->{'max_interval_no'}};
		my $min_lookup = $lookup->{$c->{'min_interval_no'}};
		$c->{'max_interval'} = $max_lookup->{'interval_name'};
		$c->{'min_interval'} = $min_lookup->{'interval_name'};

		$c->{'lithology'} = $c->{'lithology1'};
		$c->{'lithadj'} =~ s/ication/ied/g;
		$c->{'lithadj2'} =~ s/ication/ied/g;
		for my $term ( 'minor_lithology','lithadj','lithification' )	{
			$c->{'lithology'} = ( $c->{$term} ne "" ) ? $c->{$term}.' '.$c->{'lithology'} : $c->{'lithology'};
		}
		for my $term ( 'minor_lithology2','lithadj2','lithification2' )	{
			$c->{'lithology2'} = ( $c->{$term} ne "" ) ? $c->{$term}.' '.$c->{'lithology2'} : $c->{'lithology2'};
		}
		$c->{'lithology'} .= ( $c->{'lithology2'} ne "" ) ? ' and '.$c->{'lithology2'} : "";

		for my $f ( 'collection_name','lithology','lithology2','environment' )	{
			$c->{$f} =~ s/"//g;
		}

		for my $field ( 'collection_no' , 'collection_name', 'reference', 'country', 'state', 'county', 'lat', 'lng', 'max_interval', 'min_interval', 'geological_group', 'formation', 'member', 'lithology', 'environment', 'museum', 'authorizer', 'enterer', 'created' )	{
			$c->{$field} =~ s/"/\\"/g;
			# this shouldn't happen, but it does...
			$c->{$field} =~ s/\t/ /g;
			my $f = $field;
			$f =~ s/collection_no/PaleoDB_collection/;
			$f =~ s/lat/latitude/;
			$f =~ s/lng/longitude/;
			$f =~ s/geological_group/group/;
			push @attributes , qq|"$f": "$c->{$field}"|;
		}

		my $list = qq|"taxa": [ |;
		$list .= qq|{ "category": "$cof{$_->{taxon_no}}{category}", "common_name": "$cof{$_->{taxon_no}}{common_name}", "class": "$cof{$_->{taxon_no}}{class}", "order": "$cof{$_->{taxon_no}}{order}", "family": "$cof{$_->{taxon_no}}{family}", "genus": "$_->{genus_name}", "species": "$_->{species_name}" }, | foreach @{$occs{$c->{'collection_no'}}};
		$list =~ s/, $//;
		$list .= " ]";
		push @attributes , $list;

		push @colls , join(', ',@attributes);
	}
	print join('}, { ',@colls);
	print " } ] }";
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
		print "<center><p>Diversity statistics not available</p>\n<p class=\"medium\">Statistics for $collection_name (PBDB collection <a href=\"?a=basicCollectionSearch&collection_no=$collection_no\">$collection_no</a>) cannot<br>be computed because $reason</p></center>\n\n";
    		print "<p><div align=\"center\"><b><a href=\"?a=displaySearchColls&type=analyze_abundance\">Search again</a></b></div></p>";
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
	print "<span class=\"displayPanelHeader\"><span class=\"large\">Diversity statistics for <a href=\"?a=basicCollectionSearch&collection_no=$collection_no\">$collection_name</a></span></span>\n\n";
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
	print "<span class=\"displayPanelHeader\"><span class=\"large\" >Rarefaction curve for <a href=\"?a=basicCollectionSearch&collection_no=$collection_no\">$collection_name</a></span></span>\n\n";
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

    print "<p><div align=\"center\"><b><a href=\"?a=displaySearchColls&type=analyze_abundance\">Search again</a></b></div></p>";
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
		print "<center><p><b><a href=\"?a=basicCollectionSearch&collection_no=" . $q->param('collection_no') . "\">Return to the collection record</a></b></p></center>\n\n";
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

	print "<div align=\"center\"><p><b><a href=\"?a=basicCollectionSearch&collection_no=".$q->param('collection_no')."\">Return to the collection record</a></b> - ";
	print "<b><a href=\"?a=displaySearchColls&type=view\">Search for other collections</a></b></p></div>\n\n";
	print $hbo->stdIncludes($PAGE_BOTTOM);

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

1;
