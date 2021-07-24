# 
# The Paleobiology Database
# 
#   DiversityTables.pm
# 

package DiversityTables;

use strict;

# Modules needed

use Carp qw(carp croak);
use Try::Tiny;

use TaxonDefs qw(@TREE_TABLE_LIST %TAXON_TABLE %TAXON_RANK);

use CoreFunction qw(activateTables doStmt);
use ConsoleLog qw(initMessages logMessage);
use TableDefs qw(%TABLE);
use CoreTableDefs;
use DiversityDefs;

use base 'Exporter';

our (@EXPORT_OK) = qw(buildDiversityTables buildTotalDiversityTables buildPrevalenceTables);

our $DIV_MATRIX_WORK = 'dmn';
our $DIV_GLOBAL_WORK = 'dgn';
our $DIV_ADVANCED_WORK = 'dvn';

our $PVL_MATRIX_WORK = 'pvn';
our $PVL_GLOBAL_WORK = 'pgn';



# buildDiversityTables ( dbh )
# 
# Build the taxonomic diversity tables, so that we can quickly retrieve a list
# of classes and orders (with counts) for geographic and temporal summary
# queries (such as those called from Navigator).

sub buildDiversityTables {

    my ($dbh, $tree_table, $options) = @_;
    
    $options ||= {};
    
    my ($sql, $result);
    
    my $TREE_TABLE = $tree_table;
    my $ATTRS_TABLE = $TAXON_TABLE{$tree_table}{attrs};
    my $LOWER_TABLE = $TAXON_TABLE{$tree_table}{lower};
    
    logMessage(1, "Building diversity tables");
    
    my ($MBL) = $dbh->selectrow_array("SELECT max(bin_level) FROM $TABLE{COLLECTION_BIN_DATA}");
    
    logMessage(2, "    generating occurrence interval map...");
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{OCCURRENCE_MAJOR_MAP}");
    
    $dbh->do("
    	CREATE TABLE $TABLE{OCCURRENCE_MAJOR_MAP} (
    		scale_no smallint unsigned not null,
    		scale_level smallint unsigned not null,
    		early_age decimal(9,5),
    		late_age decimal(9,5),
    		interval_no int unsigned not null,
    		PRIMARY KEY (early_age, late_age)) Engine=MyISAM");
    
    $sql = "
    	INSERT INTO $TABLE{OCCURRENCE_MAJOR_MAP} (scale_no, scale_level, early_age, late_age, interval_no)
    	SELECT m.scale_no, m.scale_level, i.early_age, i.late_age, m.interval_no
    	FROM (SELECT distinct early_age, late_age FROM $TABLE{OCCURRENCE_MATRIX}) as i
    		JOIN (SELECT scale_no, scale_level, early_age, late_age, interval_no
    		      FROM $TABLE{SCALE_MAP} JOIN $TABLE{INTERVAL_DATA} using (interval_no)) as m
    	WHERE i.early_age > i.late_age and
    		if(i.late_age >= m.late_age,
    			if(i.early_age <= m.early_age, i.early_age - i.late_age, m.early_age - i.late_age),
    			if(i.early_age > m.early_age, m.early_age - m.late_age, i.early_age - m.late_age)) / (i.early_age - i.late_age) >= 0.5";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      generated $result rows with majority rule");
    
    logMessage(2, "    generating diversity matrix by geographic cluster, interval, and taxonomy...");
    
    $dbh->do("DROP TABLE IF EXISTS $DIV_MATRIX_WORK");

    $dbh->do("CREATE TABLE $DIV_MATRIX_WORK (
		bin_id int unsigned not null,
		interval_no int unsigned not null,
		ints_no int unsigned not null,
		genus_no int unsigned not null,
		n_occs int unsigned not null,
		not_trace tinyint unsigned not null,
		PRIMARY KEY (bin_id, interval_no, ints_no, genus_no)) Engine=MyISAM");
    
    $sql = "INSERT INTO $DIV_MATRIX_WORK (bin_id, interval_no, ints_no, genus_no, n_occs, not_trace)
		SELECT SQL_NO_CACHE c.bin_id_${MBL}, m.interval_no, ta.ints_no, pl.genus_no, 1, not(is_trace)
		FROM $TABLE{OCCURRENCE_MATRIX} as o
			JOIN $TABLE{OCCURRENCE_MAJOR_MAP} as m using (early_age, late_age)
			JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			JOIN $TREE_TABLE as t using (orig_no)
			JOIN $TREE_TABLE as ta on ta.orig_no = t.accepted_no
			JOIN $ATTRS_TABLE as v on v.orig_no = t.accepted_no
			LEFT JOIN $LOWER_TABLE as pl on pl.orig_no = t.accepted_no
		WHERE latest_ident and c.access_level = 0
		ON DUPLICATE KEY UPDATE $DIV_MATRIX_WORK.n_occs = $DIV_MATRIX_WORK.n_occs + 1";

    $result = $dbh->do($sql);

    logMessage(2, "      generated $result rows");
    
    logMessage(2, "    generating global diversity matrix by interval and taxonomy...");
    
    $dbh->do("DROP TABLE IF EXISTS $DIV_GLOBAL_WORK");

    $dbh->do("CREATE TABLE $DIV_GLOBAL_WORK (
		interval_no int unsigned not null,
		ints_no int unsigned not null,
		genus_no int unsigned not null,
		n_occs int unsigned not null,
		not_trace tinyint unsigned not null,
		PRIMARY KEY (interval_no, ints_no, genus_no)) Engine=MyISAM");
    
    $sql = "INSERT INTO $DIV_GLOBAL_WORK (interval_no, ints_no, genus_no, n_occs, not_trace)
		SELECT SQL_NO_CACHE m.interval_no, ta.ints_no, pl.genus_no, 1, not(is_trace)
		FROM $TABLE{OCCURRENCE_MATRIX} as o
			JOIN $TABLE{OCCURRENCE_MAJOR_MAP} as m using (early_age, late_age)
			JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			JOIN $TREE_TABLE as t using (orig_no)
			JOIN $TREE_TABLE as ta on ta.orig_no = t.accepted_no
			JOIN $ATTRS_TABLE as v on v.orig_no = t.accepted_no
			LEFT JOIN $LOWER_TABLE as pl on pl.orig_no = t.accepted_no
		WHERE latest_ident and c.access_level = 0
		ON DUPLICATE KEY UPDATE $DIV_GLOBAL_WORK.n_occs = $DIV_GLOBAL_WORK.n_occs + 1";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      generated $result rows");
    
    logMessage(2, "    indexing tables...");
    
    $dbh->do("ALTER TABLE $DIV_MATRIX_WORK ADD KEY (ints_no)");
    $dbh->do("ALTER TABLE $DIV_MATRIX_WORK ADD KEY (bin_id)");
    $dbh->do("ALTER TABLE $DIV_GLOBAL_WORK ADD KEY (ints_no)");
    
    activateTables($dbh, $DIV_MATRIX_WORK => $TABLE{DIVERSITY_MATRIX},
		         $DIV_GLOBAL_WORK => $TABLE{DIVERSITY_GLOBAL_MATRIX});
    
    buildAdvancedDiversity($dbh, $tree_table, $options);
    
    my $a = 1;	# we can stop here when debugging
}


# buildTotalDiversityTables ( dbh, tree_table, options )
# 
# Create a table containing information that can be used to generate an "advanced" diversity curve
# for the entire globe, for any range of intervals, at any of the available taxonomic
# levels. Include separate records for marine and terrestrial diversity. This table will only be
# used when no taxonomic or geographic bounds are specified.

sub buildTotalDiversityTables {
    
    my ($dbh, $tree_table, $options) = @_;
    
    $options ||= { };
    
    my $TREE_TABLE = $tree_table;
    my $ATTRS_TABLE = $TAXON_TABLE{$tree_table}{attrs};
    my $ECOTAPH_TABLE = $TAXON_TABLE{$tree_table}{ecotaph};
    my $LOWER_TABLE = $TAXON_TABLE{$tree_table}{lower};
    my $INTS_TABLE = $TAXON_TABLE{$tree_table}{ints};
    
    my ($sql, $sth, $result);
    
    doStmt($dbh, "DROP TABLE IF EXISTS $DIV_ADVANCED_WORK", $options->{debug});
    
    doStmt($dbh, "CREATE TABLE $DIV_ADVANCED_WORK (
		slice varchar(20) not null,
		base_no int unsigned not null,
		timescale_no int unsigned not null,
		interval_no int unsigned not null,
		rank tinyint unsigned not null,
		n_itime int unsigned not null default 0,
		n_itaxon int unsigned not null default 0,
		n_occs int unsigned not null default 0,
		n_taxa int unsigned not null default 0,
		n_implied int unsigned not null default 0,
		n_canceled int unsigned not null default 0,
		n_origin int unsigned not null default 0,
		n_range int unsigned not null default 0,
		n_extinct int unsigned not null default 0,
		n_single int unsigned not null default 0,
		x_extinct int unsigned not null default 0,
		x_single int unsigned not null default 0,
		# t_occs int unsigned not null default 0,
		# t_taxa int unsigned not null default 0,
		# t_implied int unsigned not null default 0,
		# t_origin int unsigned not null default 0,
		# t_range int unsigned not null default 0,
		# t_extinct int unsigned not null default 0,
		# t_single int unsigned not null default 0,
		PRIMARY KEY (slice, base_no, timescale_no, interval_no, rank)) Engine=MyISAM",
	  $options->{debug});
    
    logMessage(2, "    generating advanced diversity table...");
    
    # Read the interval and timescale data for the standard timescales. In the following
    # documentation, "level" means "interval type". Level 3 corresponds to eras, level 4 to
    # periods, and level 5 to stages.
    
    logMessage(2, "      loading interval data...");
    
    $sql = "SELECT interval_no, scale_level, early_age
	FROM $TABLE{SCALE_MAP} join $TABLE{INTERVAL_DATA} using (interval_no)
	WHERE scale_no = 1 and scale_level in (3,4,5) and early_age < 550
	ORDER BY early_age desc";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $sth = $dbh->prepare($sql);
    $sth->execute();
    
    my (%interval_list, %bounds_list, %bounds_interval, @base_list, $root_no);
    
    my $min_level = 3;
    my $max_level = 5;
    my $found_bounds;
    
    while ( my ($interval_no, $scale_level, $early_age) = $sth->fetchrow_array() )
    {
	my $bound = $early_age + 0;
	push $bounds_list{$scale_level}->@*, $bound;
	push $interval_list{$scale_level}->@*, $interval_no;
	$bounds_interval{$bound}{$scale_level} = $interval_no;
    }
    
    foreach my $i ( $min_level .. $max_level )
    {
	next unless $bounds_list{$i};
	push $bounds_list{$i}->@*, 0;
	$found_bounds = 1;
    }
    
    # If we weren't able to load interval data for any of the specified levels, abort.
    
    unless ( $found_bounds )
    {
	logMessage(2, "      ERROR: could not load interval data");
	return;
    }
    
    # Then load a set of high-level taxa for which to pre-compute these diversity statistics. We
    # will start with all classes or above which have at least 10000 occurrences. This might be
    # adjusted later.
    
    logMessage(2, "      loading taxon data...");
    
    $sql = "SELECT orig_no, min_rank, lft, rgt
	FROM $TREE_TABLE as t join $ATTRS_TABLE as v using (orig_no)
	WHERE min_rank >= 16.0 and taxon_size >= 1000 ORDER BY lft asc";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    if ( $result && ref $result eq 'ARRAY' )
    {
	@base_list = @$result;
    }
    
    else
    {
	logMessage(2, "      WARNING: could not load taxon data");
    }
    
    # Now construct the necessary SQL statement to retrieve all occurrences from the
    # Phanerozoic. If a diagnostic filter has been provided in the options hash, add that.
    
    my $filter = $options->{diagnostic} ? "and $options->{diagnostic}" : "";
    
    logMessage(2, "      selecting occurrences from the Phanerozoic...");
    
    my $sql = "SELECT o.occurrence_no, o.early_age, o.late_age, o.envtype,
		t.orig_no, t.lft, t.rgt, v.is_extant, v.is_trace,
		ph.class_no, ph.order_no, ph.family_no, ph.ints_no, ph.ints_rank,
		pl.genus_no, pl.subgenus_no, pl.species_no
	FROM $TABLE{OCCURRENCE_MATRIX} as o
		JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
		LEFT JOIN $TREE_TABLE as t1 using (orig_no)
		LEFT JOIN $TREE_TABLE as t on t.orig_no = t1.accepted_no
		LEFT JOIN $ATTRS_TABLE as v on v.orig_no = t.orig_no
		LEFT JOIN $ECOTAPH_TABLE as et on et.orig_no = t.orig_no
		LEFT JOIN $LOWER_TABLE as pl on pl.orig_no = t.orig_no
		LEFT JOIN $INTS_TABLE as ph on ph.ints_no = t.ints_no
	WHERE o.latest_ident and c.access_level = 0 and t.orig_no > 0 and o.early_age < 550 $filter
	GROUP BY o.occurrence_no";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    # Then prepare and execute it.
    
    $sth = $dbh->prepare($sql);
    $sth->execute();
    
    # Now scan through the result set, and determine how (and whether) to count each occurrence by
    # time, taxon, and environment (marine/terrestrial).
    
    logMessage(2, "      processing those occurrences...");
    
    my (%n_occs, %interval_cache, %interval_overlap, %itime_in_interval, %itime_in_bin);
    my (%sampled_in_bin, %implied_in_bin, %real_in_bin, %nontrace_in_bin);
    my (%itaxon_in_bin, %itaxon_in_interval, %taxon_firstbin, %taxon_lastbin);
    my (%lft_taxon, %extant_taxon, %trace_taxon, %taxon_tree);
    # my (%trace_tree);
    
    my @counted_ranks = (13, 9, 5, 4, 3);
    my %rank_field = (13 => 'order_no', 9 => 'family_no', 5 => 'genus_no',
		      4 => 'subgenus_no', 3 => 'species_no');
    my %implied_field = (9 => 'order_no', 5 => 'family_no', 4 => 'family_no', 3 => 'genus_no');
    my %level_label = (3 => 'period', 4 => 'epoch', 5 => 'stage');
    my %rank_label = (13 => 'order', 9 => 'family', 5 => 'genus', 4 => 'subgenus', 3 => 'species');
    
 OCCURRENCE:
    while ( my $r = $sth->fetchrow_hashref )
    {
	# If this occurrence is counted as coming from one or more particular environments, it
	# will be counted both in the global total and under those environments. An occurrence
	# labeled as 'marine_x' is one where the organism is one that is recorded as living in a
	# terrestrial environment even though the site is from a marine environment. The label
	# 'terrestrial_x' marks the opposite situation.
	
	my @slice = ('global');
	
	if ( ! $options->{global_only} )
	{	
	    if ( $r->{envtype} eq 'marine' )
	    {
		push @slice, 'marine_env', 'marine_taxa';
	    }
	    
	    elsif ( $r->{envtype} eq 'marine_x' )
	    {
		push @slice, 'marine_env', 'terr_taxa';
	    }
	    
	    elsif ( $r->{envtype} eq 'terrestrial' )
	    {
	        push @slice, 'terr_env', 'terr_taxa';
	    }
	    
	    elsif ( $r->{envtype} eq 'terrestrial_x' )
	    {
		push @slice, 'terr_env', 'marine_taxa';
	    }
	}

	unless ( $r->{is_trace} )
	{
	    push @slice, map { "$_:notrace" } @slice;
	}
	
	# Keep a count of all occurrences by slice and by ints_no.
	
	$n_occs{$_}{all}++ foreach @slice;

	if ( $r->{ints_no} )
	{
	    $n_occs{$_}{taxon}{$r->{ints_no}}++ foreach @slice;
	    $lft_taxon{$r->{ints_no}} //= $r->{lft};
	}
	
	# Next figure out the intervals in which to bin this occurrence using the "major"
	# timerule at each level.
	
	my $interval_key = "$r->{early_age}-$r->{late_age}";
	
	# If we have already computed the matching intervals for the specified age range, then we
	# can just use the cached results. Otherwise, we need to go through each list of intervals
	# at each level and pick the one into which this occurrence should be binned.
	
	unless ( $interval_cache{$interval_key} )
	{
	    $interval_cache{$interval_key} = { };
	    
	    # Turn the bounds into numbers for comparison.
	    
	    my $early_occ = $r->{early_age} + 0;
	    my $late_occ = $r->{late_age} + 0;
	    
	    # Scan the bounds list corresponding to each interval type that we have data for.
	    
	    foreach my $level ( $min_level .. $max_level )
	    {
		next unless $bounds_list{$level};
		
		my @overlap_bins;
		
		# Go through all the intervals at this level, looking for one that has a majority
		# overlap with the specified age range. The entries in bounds_list correspond to
		# the early bounds of the entries in intervals_list.
		
	      INTERVAL:
		foreach my $i ( 0 .. $bounds_list{$level}->$#* - 1 )
		{
		    # Skip all intervals that do not overlap with the occurrence range, and stop
		    # the scan when we have passed that range.
		    
		    my $late_bound = $bounds_list{$level}[$i+1];
		    
		    next INTERVAL if $late_bound >= $early_occ;
		    
		    my $early_bound = $bounds_list{$level}[$i];
		    
		    last INTERVAL if $early_bound <= $late_occ;
		    
		    # If we find an interval which covers half or more of the age range we are
		    # evaluating, select that interval for the age range at the current level.
		    
		    my $overlap;
		    
		    if ( $late_occ >= $late_bound )
		    {
			if ( $early_occ <= $early_bound )
			{
			    $overlap = $early_occ - $late_occ;
			}
			
			else
			{
			    $overlap = $early_bound - $late_occ;
			}
		    }
		    
		    elsif ( $early_occ > $early_bound )
		    {
			$overlap = $early_bound - $late_bound;
		    }
		    
		    else
		    {
			$overlap = $early_occ - $late_bound;
		    }
		    
		    if ( $early_occ != $late_occ && $overlap / ($early_occ - $late_occ) >= 0.5 )
		    {
			$interval_cache{$interval_key}{$level} = $i;
			last INTERVAL;
		    }
		    
		    # If this interval overlaps the age range but does not cover at least half of
		    # it, add it to the overlap list.
		    
		    else
		    {
			push @overlap_bins, $i;
		    }
		}
		
		# If we were not able to find a matching interval for the age range at this level,
		# keep the overlap list so that we can later distribute imprecise occurrences
		# among the overlapping intervals.
		
		unless ( defined $interval_cache{$interval_key}{$level} )
		{
		    $interval_overlap{$interval_key}{$level} = \@overlap_bins;
		}
	    }
	}
	
	# Now count this occurrence for each possible combination of environment, interval type,
	# and taxonomic rank. Start by iterating over the interval types.
	
      LEVEL:
	foreach my $level ( $min_level .. $max_level )
	{
	    # For each level/interval type, check whether an interval of this type has been
	    # selected for the occurrence's age range.
	    
	    my $bin_index = $interval_cache{$interval_key}{$level};
	    
	    # If not, count this occurrence under "imprecise time" and skip to the next level.
	    
	    unless ( defined $bin_index )
	    {
		my $count_no = $r->{order_no} || $r->{family_no} || $r->{ints_no};
		
		$lft_taxon{$count_no} //= $r->{lft};
		
		$itime_in_interval{$_}{$level}{$interval_key}{$count_no}++ foreach @slice;
		
		$itime_in_interval{$_}{$level}{$interval_key}{total}++ foreach @slice;
		
		next LEVEL;
	    }
	    
	    # If so, get the interval_no in which this occurrence is binned and proceed to count
	    # it taxonomically.
	    
	    my $interval_no = $interval_list{$level}[$bin_index];
	    
	    # Count this occurrence at the taxonomic rank to which it is identified and at
	    # selected higher ranks. For example, an occurrence that is identified to the genus
	    # level will have its genus, family, and order counted. An occurrence that is
	    # identified to the species level will have its species, subgenus, genus, family, and
	    # order counted.
	    
	    foreach my $rank ( @counted_ranks )
	    {
		# At each rank, extract the taxon identifier (if any) for the occurrence at the
		# given rank. So the taxon identifier for rank 13, for example, is found
		# in the field 'order_no'.
		
		my $taxon_no = $r->{$rank_field{$rank}};
		my $taxon_implied;
		
		# When we are counting at the rank of genus or below, a missing genus means that
		# this is a nomen vanum, nomen dubium, or otherwise invalid. We count this
		# occurrence as an implied_taxon using the family_no if defined or else the
		# ints_no value, which corresponds to the most precise classification of the
		# occurrence above the genus level.
		
		if ( $rank <= 5 && ! $r->{genus_no} )
		{
		    $taxon_no = $r->{family_no} || $r->{ints_no};
		    $taxon_implied = 1;
		}
		
		# When we are counting subgenera, default to the genus if there is no subgenus
		# listed. This is counted as a real taxon, not an implied one.
		
		elsif ( $rank == 4 )
		{
		    $taxon_no ||= $r->{genus_no};
		}
		
		# When we are counting species, count an occurrence not identified to the species
		# level as an implied taxon using genus_no.
		
		elsif ( $rank == 3 && ! $taxon_no )
		{
		    $taxon_no = $r->{genus_no};
		    $taxon_implied = 1;
		}
		
		# When we are counting families, count this as an implied taxon using either the
		# order_no value if defined or else the ints_no value if ints_rank is 9 or higher.
		
		elsif ( $rank == 9 && ! $taxon_no )
		{
		    $taxon_no = $r->{order_no} || ($r->{ints_rank} >= 9 ? $r->{ints_no} : undef);
		    $taxon_implied = 1;
		}
		
		# When we are counting orders, count this as an implied taxon using the class_no
		# value if defined or else the ints_no value if ints_rank is 13 or higher.
		
		elsif ( $rank == 13 && ! $taxon_no )
		{
		    $taxon_no = $r->{class_no} || ($r->{ints_rank} >= 13 ? $r->{ints_no} : undef);
		    $taxon_implied = 1;
		}
		
		# Now count this occurrence as fully binned if we have found a taxon identifier
		# (either real or implied) for this occurrence at this rank.
		
		if ( $taxon_no )
		{
		    # Count the occurrences of each unique taxon of this rank found in this
		    # interval, which also has the effect of counting the number of unique taxa.
		    
		    $sampled_in_bin{$_}{$interval_no}{$rank}{$taxon_no}++ foreach @slice;
		    $sampled_in_bin{$_}{$interval_no}{$rank}{total}++ foreach @slice;
		    
		    # Also keep track of the first and last occurrence this taxon, using the index from
		    # the list of intervals of this type.
		    
		    foreach my $slice ( @slice )
		    {
			unless ( defined $taxon_firstbin{$slice}{$level}{$rank}{$taxon_no} &&
				 $taxon_firstbin{$slice}{$level}{$rank}{$taxon_no} <= $bin_index )
			{
			    $taxon_firstbin{$slice}{$level}{$rank}{$taxon_no} = $bin_index;
			}
			
			unless ( defined $taxon_lastbin{$slice}{$level}{$rank}{$taxon_no} &&
				 $taxon_lastbin{$slice}{$level}{$rank}{$taxon_no} >= $bin_index )
			{
			    $taxon_lastbin{$slice}{$level}{$rank}{$taxon_no} = $bin_index;
			}
		    }
		    
		    # Record information about this taxon necessary to properly report it. The
		    # %lft_taxon hash records the 'lft' value for each taxon, which allows us to
		    # generate diversity records for each of the base taxa retrieved above. The
		    # 'is_extant' and 'is_trace' information will be used to generate correction
		    # records.
		    
		    $lft_taxon{$taxon_no} //= $r->{lft};
		    $extant_taxon{$taxon_no} = 1 if $r->{is_extant};
		    $trace_taxon{$taxon_no} = 1 if $r->{is_trace};
		    
		    # If the counted taxon is implied, mark it as such. This count is kept
		    # separately for each slice/interval combination.
		    
		    if ( $taxon_implied )
		    {
			$implied_in_bin{$_}{$interval_no}{$rank}{$taxon_no} = 1 foreach @slice;
		    }
		    
		    # # If the next higher rank is specified, mark that taxon with a 2 to indicate
		    # # that it does not count as an implied taxon because we have found an actual
		    # # taxon that is a member of that group in this bin. We don't have to worry
		    # # about setting %extant_taxon and %trace_taxon for these, because those will
		    # # have been processed at their own rank.
		    
		    # if ( $implied_field{$rank} && $r->{$implied_field{$rank}} )
		    # {
		    # 	my $implied_no = $r->{$implied_field{$rank}};
		    # 	$implied_in_bin{$_}{$interval_no}{$rank}{$implied_no} = 2 foreach @slice;
		    # }
		}
		
		# Otherwise, count this occurrence as being imprecise at this rank. We count the
		# imprecise occurrences both globally and by taxon, so that we can generate
		# correct statistics both globally and by individual base taxa.
		
		else
		{
		    my $count_no = $r->{ints_no};
		    
		    $lft_taxon{$count_no} //= $r->{lft};
		    
		    $itaxon_in_bin{$_}{$interval_no}{$rank}{$count_no}++ foreach @slice;
		    
		    $itaxon_in_bin{$_}{$interval_no}{$rank}{total}++ foreach @slice;
		    
		    # # If the next higher rank is specified, and that taxon has not yet been seen
		    # # in this bin, mark that taxon with a 1 to indicate that it should be counted
		    # # as an applied taxon at the lower rank. For example, when we are counting at
		    # # the genus level, each unique family that is represented by an occurrence
		    # # with no genus identification should be counted as an implied genus. That
		    # # occurrence implies that at least one more genus was found in that bin, even
		    # # though we don't know exactly what genus it was.
		    
		    # if ( $implied_field{$rank} && $r->{$implied_field{$rank}} )
		    # {
		    # 	my $implied_no = $r->{$implied_field{$rank}};
		    # 	$implied_in_bin{$_}{$interval_no}{$rank}{$implied_no} //= 1 foreach @slice;
		    # }
		}
	    }
	    
	    # After we have counted this occurrence at all of the ranks to which it is identified,
	    # we need to mark all of those taxa as actually being found in this interval in order
	    # to avoid double-counting implied taxa. These are tracked separately for each
	    # slice/interval combination.
	    
	    foreach my $s ( @slice )
	    {
		# Create a hash for this slice/interval combination if it doesn't already exist.
		
		$real_in_bin{$s}{$interval_no} ||= { };
		my $real_in_bin = $real_in_bin{$s}{$interval_no};
		
		# For each taxonomic rank (genus or above) to which this occurrence is identified,
		# count that taxon as being found for real in this interval.
		
		if ( $r->{genus_no} )
		{
		    $real_in_bin->{$r->{genus_no}} = 1;
		}
		
		if ( $r->{family_no} )
		{
		    $real_in_bin->{$r->{family_no}} = 1;
		}
		
		if ( $r->{order_no} )
		{
		    $real_in_bin->{$r->{order_no}} = 1;
		}
		
		# Count class_no as well if any of these is defined.
		
		if ( $r->{class_no} && ! defined $real_in_bin->{$r->{class_no}} )
		{
		    if ( $r->{genus_no} || $r->{family_no} || $r->{order_no} )
		    {
			$real_in_bin->{$r->{class_no}} = 1;
		    }
		}
		
		# If this occurrence is defined at any taxonomic rank at or below its ints_rank,
		# count its ints_no value as being real too. This is necessary because we use
		# ints_no as an implied value for family or order under some circumstances.
		
		if ( $r->{ints_no} && ! defined $real_in_bin->{$r->{ints_no}} )
		{
		    if ( $r->{genus_no} ||
			 $r->{family_no} && $r->{ints_rank} >= 9 ||
			 $r->{order_no} && $r->{ints_rank} >= 13 )
		    {
			$real_in_bin->{$r->{ints_no}} = 0;
		    }
		}
	    }
	}
    }
    
    # For each unique taxon we have found, run through the set of base taxa and record which ones
    # it is contained in. This information is stored in %taxon_tree.

    logMessage(2, "      recording taxon inclusion...");
    
    $taxon_tree{0} = \%lft_taxon;
    
    foreach my $taxon_no ( keys %lft_taxon )
    {
	foreach my $b ( @base_list )
	{
	    if ( $lft_taxon{$taxon_no} >= $b->{lft} && $lft_taxon{$taxon_no} <= $b->{rgt} )
	    {
		$taxon_tree{$b->{orig_no}}{$taxon_no} = 1;
		# $trace_tree{$b->{orig_no}}{$taxon_no} = 1 if $trace_taxon{$taxon_no};
	    }
	}
    }
    
    # This may be over-elaboration, but for every age range for which there were one or more
    # occurrences with that range that were unable to be binned because they do not precisely
    # correspond to any interval, distribute those occurrences as evenly as possible among all of
    # the overlapping intervals and record the counts in %itime_in_bin by interval.
    
    logMessage(2, "      distributing imprecise occurrences over intervals...");
    
    my %missing_overlap;
    # my %bin_count;
    # my %fraction_count;
    # my %fraction_count_taxa;
    
    foreach my $slice ( keys %n_occs )
    {
	foreach my $level ( $min_level .. $max_level )
	{
	    foreach my $interval_key ( keys $itime_in_interval{$slice}{$level}->%* )
	    {
		# my $occs = $n_imprecise_time{$slice}{$level}{$interval_key};
		my $bins = $interval_overlap{$interval_key}{$level};
		
		# # If there aren't any overlapping intervals recorded for this age range, note the
		# # error.
		
		unless ( @$bins )
		{
		    $missing_overlap{$interval_key}++;
		    next;
		}

		# # if ( @$bins > 20 )
		# # {
		# #     $bin_count{$interval_key}{$level} = 1;
		# # }
		
		# # Otherwise, distribute the imprecise occurrence count among the intervals as
		# # evenly as possible until we run out, from late to early. This direction prevents
		# # counting too many of these in early bins.
		
		# my $fraction = $occs/@$bins;
		# my $skip = 0;
		# my $amt = 1;
		
		# if ( $fraction <= 0.5 )
		# {
		#     $skip = int(@$bins/$occs) - 1;
		# }
		
		# else
		# {
		#     $amt = int($fraction+0.5);
		# }
		
		# my $k = $skip;
		# my $interval_no;
		
		# foreach my $bin_index ( reverse @$bins )
		# {
		#     $interval_no = $interval_list{$level}[$bin_index];
		    
		#     if ( $k-- ) { next; }
		#     else { $k = $skip; }
		    
		#     my $drop = $amt > $occs ? $amt : $occs;
		    
		#     $itime_in_bin{$slice}{$interval_no}{total} += $drop;
		#     $occs -= $drop;
		    
		#     last unless $occs;
		# }
		
		# $itime_in_bin{$slice}{$interval_no}{total} += $occs if $occs;
		# $occs = 0;
		
		# # Now do the same for all the taxa in %itime_in_interval.
		
		my $interval_occs = $itime_in_interval{$slice}{$level}{$interval_key};
		
		next unless $interval_occs && ref $interval_occs eq 'HASH';
		
		foreach my $taxon_no ( keys $interval_occs->%* )
		{
		    my $occs = $interval_occs->{$taxon_no};
		    
		    my $fraction = $occs/@$bins;
		    my $skip = 0;
		    my $amt = 1;
		    
		    if ( $fraction <= 0.5 )
		    {
			$skip = int(@$bins/$occs) - 1;
		    }
		    
		    else
		    {
			$amt = int($fraction+0.5);
		    }
		    
		    my $k = $skip;
		    my $interval_no;
		    
		    foreach my $bin_index ( reverse @$bins )
		    {
			$interval_no = $interval_list{$level}[$bin_index];
			
			if ( $k-- ) { next; }
			else { $k = $skip; }
			
			my $drop = $amt > $occs ? $amt : $occs;
			
			$itime_in_bin{$slice}{$interval_no}{$taxon_no} += $drop;
			$occs -= $drop;
			
			last unless $occs;
		    }
		    
		    $itime_in_bin{$slice}{$interval_no}{$taxon_no} += $occs if $occs;
		}
	    }
	}
    }
    
    # logMessage(2, "    Bins:");
    
    # foreach my $f ( sort { $a <=> $b } keys %bin_count )
    # {
    # 	logMessage(2, "      $f: $bin_count{$f}");
    # }
    
    # logMessage(2, "    Fractions:");

    # foreach my $f ( sort { $a <=> $b } keys %fraction_count )
    # {
    # 	logMessage(2, "      $f: $fraction_count{$f}");
    # }

    # logMessage(2, "    For taxa:");
    
    # foreach my $f ( sort { $a <=> $b } keys %fraction_count_taxa )
    # {
    # 	logMessage(2, "      $f: $fraction_count_taxa{$f}");
    # }
	
    # Our goal is to generate the usual diversity output including the four diversity statistics
    # defined by Foote: n_origin = XFt, n_single = XFL, n_extinct = XbL, n_range = Xbt. In
    # addition, the fields x_single and x_extinct provide corrections for the middle two
    # statistics for taxa that are known to be extant.
    
    # Start by creating a statement to insert the statistics for one environment/interval/rank
    # combination into the database.
    
    # $sql = "INSERT INTO $DIV_ADVANCED_WORK (slice, base_no, timescale_no, interval_no, rank, 
    # 	n_itime, n_itaxon, n_occs, n_taxa, n_implied, n_canceled,
    # 	n_origin, n_range, n_extinct, n_single, x_extinct, x_single,
    # 	t_occs, t_taxa, t_implied, t_origin, t_range, t_extinct, t_single)
    # 	values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    $sql = "INSERT INTO $DIV_ADVANCED_WORK (slice, base_no, timescale_no, interval_no, rank,
    	n_itime, n_itaxon, n_occs, n_taxa, n_implied, n_canceled,
    	n_origin, n_range, n_extinct, n_single, x_extinct, x_single)
    	values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    my $insert_sth = $dbh->prepare($sql);
    
    # Then run through all combinations of slice/base-taxon/interval-type/rank. For each such
    # combination, run through all the taxa we have counted and accumulate the Foote statistics
    # for the corresponding intervals of that type. When we have done this, insert a row into the
    # advanced diversity table for each of these intervals.
    
    my (%missing_taxa, $n_rows);
    
    foreach my $slice ( keys %n_occs )
    {
	logMessage(2, "      counting diversity for slice '$slice'...");
	
	foreach my $level ( $min_level .. $max_level )
	{
	    logMessage(2, "          by $level_label{$level}");
	    
	    foreach my $rank ( @counted_ranks )
	    {
		# logMessage(2, "            - $rank");
		
		# The following variables accumulate the Foote statistics and corrections for each
		# interval at the current level (interval type).
		
		my (%n_origin, %n_range, %n_extinct, %n_single, %x_extinct, %x_single);
		
		# Check the first and last appearance of each counted taxon, and increment the
		# statistics for the indicated range of intervals.
		
		foreach my $taxon_no ( keys $taxon_firstbin{$slice}{$level}{$rank}->%* )
		{
		    my $first_index = $taxon_firstbin{$slice}{$level}{$rank}{$taxon_no};
		    my $last_index = $taxon_lastbin{$slice}{$level}{$rank}{$taxon_no};
		    
		    unless ( defined $first_index && defined $last_index )
		    {
			$missing_taxa{$taxon_no} = 1;
			next;
		    }
		    
		    my $first_interval = $interval_list{$level}[$first_index];
		    my $last_interval = $interval_list{$level}[$last_index];
		    
		    # If this taxon is implied and also found for real in its first interval, bump
		    # up the first interval until we find one where it doesn't have that status.

		    while ( $first_interval &&
			    $implied_in_bin{$slice}{$first_interval}{$rank}{$taxon_no} &&
			    $real_in_bin{$slice}{$first_interval}{$taxon_no} )
		    {
			$first_index++;
			$first_interval = $interval_list{$level}[$first_index];
		    }
		    
		    # Make the same adjustment to the end of the range. If this results in no
		    # range at all, that means the taxon won't be counted at all in the code
		    # below.
		    
		    while ( $last_interval && $last_index > $first_index &&
			    $implied_in_bin{$slice}{$last_interval}{$rank}{$taxon_no} &&
			    $real_in_bin{$slice}{$last_interval}{$taxon_no} )
		    {
			$last_index--;
			$last_interval = $interval_list{$level}[$last_index];
		    }
		    
		    # If the interval of first appearance is the same as the interval of last
		    # appearance, then this is a singleton. If the taxon is extant and not implied,
		    # include it in the x_single correction statistic because that means it isn't
		    # really a singleton.
		    
		    if ( $first_index == $last_index )
		    {
			$n_single{$first_interval}{$taxon_no} = 1;
			$x_single{$first_interval}{$taxon_no} = 1 if $extant_taxon{$taxon_no} &&
			    ! $implied_in_bin{$slice}{$first_interval}{$rank}{$taxon_no};
		    }
		    
		    # Otherwise, we count the origin as the first interval where the taxon appears
		    # and the extinction as the last, ranging through the intervals between. If
		    # the taxon is extant and not implied, include it in the x_extinct correction
		    # statistic for the ending bin because that isn't really the last appearance
		    # of this taxon.
		    
		    elsif ( $first_interval && $last_interval && $last_index > $first_index )
		    {
			$n_origin{$first_interval}{$taxon_no} = 1;
			$n_range{$_}{$taxon_no} = 1
			    foreach $interval_list{$level}->@[$first_index+1..$last_index-1];
			$n_extinct{$last_interval}{$taxon_no} = 1;
			$x_extinct{$last_interval}{$taxon_no} = 1 if $extant_taxon{$taxon_no} &&
			    ! $implied_in_bin{$slice}{$last_interval}{$rank}{$taxon_no};
		    }
		}
		
		# Then run through all of the intervals at this level from start to end. For each
		# interval, count the unique taxa (both sampled and implied) for this
		# environment/interval/rank combination and then add a row into the advanced
		# diversity table.
		
		foreach my $interval_no ( $interval_list{$level}->@* )
		{
		    my $itime_occs = $itime_in_bin{$slice}{$interval_no};
		    my $itaxon_occs = $itaxon_in_bin{$slice}{$interval_no}{$rank};
		    my $sampled_occs = $sampled_in_bin{$slice}{$interval_no}{$rank};
		    my $implied_taxon = $implied_in_bin{$slice}{$interval_no}{$rank};
		    my $real_taxon = $real_in_bin{$slice}{$interval_no};
		    my %canceled_taxon;
		    
		    # For each of the implied taxa in the interval, cancel the ones that appear in
		    # the actual counts so that we don't double count them. Correct the occurrence
		    # counts so that the occurrences of the implied taxa are instead counted as
		    # imprecise.
		    
		    foreach my $taxon_no ( keys $implied_taxon->%* )
		    {
			if ( $real_taxon->{$taxon_no} )
			{
			    my $occ_count = $sampled_occs->{$taxon_no};
			    
			    $itaxon_occs->{$taxon_no} += $occ_count;
			    $itaxon_occs->{total} += $occ_count;
			    $sampled_occs->{total} -= $occ_count;
			    delete $sampled_occs->{$taxon_no};
			    delete $implied_taxon->{$taxon_no};
			    $canceled_taxon{$taxon_no} = 1;
			}
		    }
		    
		    # foreach my $taxon_no ( keys $implied_taxon->%* )
		    # {
		    # 	delete $implied_taxon->{$taxon_no} if $implied_taxon->{$taxon_no} == 2;
		    # }
		    
		    # my $n_occs = $n_occs{$slice}{$interval_no}{$rank} // 0;
		    
		    my $n_itime = $itime_occs->{total} // 0;
		    my $n_itaxon = $itaxon_occs->{total} // 0;
		    
		    my $n_occs = $sampled_occs->{total} // 0;
		    my $n_taxa = scalar keys $sampled_occs->%*;
		    my $n_implied = scalar keys $implied_taxon->%*;
		    my $n_canceled = scalar keys %canceled_taxon;
		    
		    my $n_origin = scalar keys $n_origin{$interval_no}->%*;
		    my $n_range = scalar keys $n_range{$interval_no}->%*;
		    my $n_extinct = scalar keys $n_extinct{$interval_no}->%*;
		    my $n_single = scalar keys $n_single{$interval_no}->%*;
		    my $x_extinct = scalar keys $x_extinct{$interval_no}->%*;
		    my $x_single = scalar keys $x_single{$interval_no}->%*;
		    
		    # my $t_occs = 0;
		    # my $t_taxa = 0;
		    # my $t_implied = 0;
		    # my $t_canceled = 0;
		    
		    # my $t_origin = 0;
		    # my $t_range = 0;
		    # my $t_extinct = 0;
		    # my $t_single = 0;
		    # my $tx_extinct = 0;
		    # my $tx_single = 0;
		    
		    # foreach ( grep { $trace_taxon{$_} } keys $sampled_occs->%* )
		    # {
		    # 	$t_occs += $sampled_occs->{$_};
		    # 	$t_taxa++;
		    # 	$t_implied++ if $implied_taxon->{$_};
		    # 	$t_canceled++ if $canceled_taxon{$_};
			
		    # 	$t_origin++ if $n_origin{$interval_no}{$_};
		    # 	$t_extinct++ if $n_extinct{$interval_no}{$_};
		    # 	$t_single++ if $n_single{$interval_no}{$_};
		    # 	$tx_extinct++ if $x_extinct{$interval_no}{$_};
		    # 	$tx_single++ if $x_single{$interval_no}{$_};
		    # }
		    
		    # $t_range = scalar grep { $trace_taxon{$_} } keys $n_range{$interval_no}->%*;
		    
		    $insert_sth->execute($slice, 0, $level, $interval_no, $rank,
			     $n_itime, $n_itaxon, $n_occs, $n_taxa, $n_implied, $n_canceled,
			     $n_origin, $n_range, $n_extinct, $n_single, $x_extinct, $x_single);
		    $n_rows++;
		    
		    # if ( $t_taxa > 0 || $t_range > 0 )
		    # {
		    # 	$insert_sth->execute($slice, 0, $level, $interval_no, $rank, 'trace',
		    # 	     $n_itime, $n_itaxon, $t_occs, $t_taxa, $t_implied, $t_canceled,
		    # 	     $t_origin, $t_range, $t_extinct, $t_single, $tx_extinct, $tx_single);
		    # 	$n_rows++;
		    # }
		    
		    # $insert_sth->execute($slice, 0, $level, $interval_no, $rank, 'all',
		    # 	     $n_itime, $n_itaxon, $n_occs, $n_taxa, $n_implied, $n_canceled,
		    # 	     $n_origin, $n_range, $n_extinct, $n_single, $x_extinct, $x_single,
		    # 	     $t_occs, $t_taxa, $t_implied, $t_origin, $t_range, $t_extinct, $t_single);

		    # For each interval run through all of the base taxa, and generate a row for
		    # each one.
		    
		    foreach my $b ( @base_list )
		    {
			my $base_no = $b->{orig_no};
			my $subtaxon = $taxon_tree{$base_no};
			
			my $n_itime = 0;
			my $t_itime = 0;
			
			foreach ( grep { $subtaxon->{$_} } keys $itime_occs->%* )
			{
			    $n_itime += $itime_occs->{$_};
			    # $t_itime += $itime_occs->{$_} if $trace_taxon{$_};
			}
			
			my $n_itaxon = 0;
			$n_itaxon += $itaxon_occs->{$_} foreach grep { $subtaxon->{$_} }
			    keys $itaxon_occs->%*;
			
			my $n_occs = 0;
			my $n_taxa = 0;
			my $n_implied = 0;
			my $n_canceled = 0;

			my $n_origin = 0;
			my $n_range = 0;
			my $n_extinct = 0;
			my $n_single = 0;
			my $x_extinct = 0;
			my $x_single = 0;
			
			# my $t_occs = 0;
			# my $t_taxa = 0;
			# my $t_implied = 0;
			# my $t_canceled = 0;
			
			# my $t_origin = 0;
			# my $t_range = 0;
			# my $t_extinct = 0;
			# my $t_single = 0;
			# my $tx_extinct = 0;
			# my $tx_single = 0;
			
			foreach ( grep { $subtaxon->{$_} } keys $sampled_occs->%* )
			{
			    $n_occs += $sampled_occs->{$_};
			    $n_taxa++;
			    $n_implied++ if $implied_taxon->{$_};
			    # $n_canceled++ if $canceled_taxon{$_};
			}
			
			my $n_canceled = scalar grep { $subtaxon->{$_} } keys %canceled_taxon;
			
			$n_origin = scalar grep { $subtaxon->{$_} } keys $n_origin{$interval_no}->%*;
			$n_range = scalar grep { $subtaxon->{$_} } keys $n_range{$interval_no}->%*;
			$n_extinct = scalar grep { $subtaxon->{$_} } keys $n_extinct{$interval_no}->%*;
			$n_single = scalar grep { $subtaxon->{$_} } keys $n_single{$interval_no}->%*;
			$x_extinct = scalar grep { $subtaxon->{$_} } keys $x_extinct{$interval_no}->%*;
			$x_single = scalar grep { $subtaxon->{$_} } keys $x_single{$interval_no}->%*;
			    
			    # if ( $trace_taxon{$_} )
			    # {
			    # 	$t_occs += $sampled_occs->{$_};
			    # 	$t_taxa++;
			    # 	$t_implied++ if $implied_taxon->{$_};
			    # 	$t_canceled++ if $canceled_taxon{$_};
				
			    # 	$t_origin++ if $n_origin{$interval_no}{$_};
			    # 	$t_extinct++ if $n_extinct{$interval_no}{$_};
			    # 	$t_single++ if $n_single{$interval_no}{$_};
			    # 	$tx_extinct++ if $x_extinct{$interval_no}{$_};
			    # 	$tx_single++ if $x_single{$interval_no}{$_};
			    # }
			# }
		    
			# foreach ( grep { $subtaxon->{$_} } keys $n_range{$interval_no}->%* )
			# {
			#     $n_range++;
			#     # $t_range++ if $trace_taxon{$_};
			# }
			
			# my $n_implied = scalar grep { $taxon_tree{$base_no}{$_} }
			#     keys $implied_taxon->%*;
			
			# my $n_origin = scalar grep { $taxon_tree{$base_no}{$_} }
			#     keys $n_origin{$interval_no}->%*;
			
			# my $n_range = scalar grep { $taxon_tree{$base_no}{$_} }
			#     keys $n_range{$interval_no}->%*;
			
			# my $n_extinct = scalar grep { $taxon_tree{$base_no}{$_} }
			#     keys $n_extinct{$interval_no}->%*;
			
			# my $n_single = scalar grep { $taxon_tree{$base_no}{$_} }
			#     keys $n_single{$interval_no}->%*;
			
			# my $x_extinct = scalar grep { $taxon_tree{$base_no}{$_} }
			#     keys $x_extinct{$interval_no}->%*;
			
			# my $x_single = scalar grep { $taxon_tree{$base_no}{$_} }
			#     keys $x_single{$interval_no}->%*;

			# my $t_implied = scalar grep { $trace_tree{$base_no}{$_} }
			#     keys $implied_taxon->%*;
			
			# my $t_origin = scalar grep { $trace_tree{$base_no}{$_} }
			#     keys $n_origin{$interval_no}->%*;
			
			# my $t_range = scalar grep { $trace_tree{$base_no}{$_} }
			#     keys $n_range{$interval_no}->%*;
			
			# my $t_extinct = scalar grep { $trace_tree{$base_no}{$_} }
			#     keys $n_extinct{$interval_no}->%*;
			
			# my $t_single = scalar grep { $trace_tree{$base_no}{$_} }
			#     keys $n_single{$interval_no}->%*;
			
			# $insert_sth->execute($slice, $base_no, $level, $interval_no, $rank,
			# 	$n_itime, $n_itaxon, $n_occs, $n_taxa, $n_implied, $n_canceled,
			# 	$n_origin, $n_range, $n_extinct, $n_single, $x_extinct, $x_single,
			# 	$t_occs, $t_taxa, $t_implied, $t_origin, $t_range, $t_extinct, $t_single);
			# $n_rows++;
			
			$insert_sth->execute($slice, $base_no, $level, $interval_no, $rank,
				$n_itime, $n_itaxon, $n_occs, $n_taxa, $n_implied, $n_canceled,
				$n_origin, $n_range, $n_extinct, $n_single, $x_extinct, $x_single);
			$n_rows++;

			# if ( $t_taxa > 0 || $t_range > 0 )
			# {
			#     $insert_sth->execute($slice, $base_no, $level, $interval_no, $rank, 'trace',
			# 	$n_itime, $n_itaxon, $t_occs, $t_taxa, $t_implied, $t_canceled,
			# 	$t_origin, $t_range, $t_extinct, $t_single, $tx_extinct, $tx_single);
			#     $n_rows++;
			# }
		    }
		}
	    }
	}
	
	# Then add a separate row giving the total number of occurrences processed for this slice.
	
	logMessage(2, "          by base taxon");
	
	my $quoted_slice = $dbh->quote($slice);
	my $quoted_occs = $dbh->quote($n_occs{$slice}{all});
	
	$sql = "INSERT INTO $DIV_ADVANCED_WORK
		       (slice, base_no, timescale_no, interval_no, rank, n_occs)
		VALUES ($quoted_slice, 0, 0, 0, 0, $quoted_occs)";
	
	doStmt($dbh, $sql, $options->{debug});
	
	$n_rows++;
	
	# And the same for the total number of occurrences for each base taxon.
	    
	foreach my $b ( @base_list )
	{
	    my $base_no = $b->{orig_no};
	    my $subtaxon = $taxon_tree{$base_no};
	    
	    my $n_occs = 0;
	    
	    foreach ( grep { $subtaxon->{$_} } keys $n_occs{$slice}{taxon}->%* )
	    {
		$n_occs += $n_occs{$slice}{taxon}{$_};
	    }

	    my $quoted_base = $dbh->quote($base_no);
	    my $quoted_occs = $dbh->quote($n_occs);
	    
	    $sql = "INSERT INTO $DIV_ADVANCED_WORK
		       (slice, base_no, timescale_no, interval_no, rank, n_occs)
		VALUES ($quoted_slice, $quoted_base, 0, 0, 0, $quoted_occs)";

	    doStmt($dbh, $sql, $options->{debug});

	    $n_rows++;
	}
    }
    
    # Report the number of occurrences processed, the number of table rows inserted, and any
    # errors we found.
    
    logMessage(2, "    processed $n_occs{global}{all} occurrences");
    logMessage(2, "       $n_occs{$_}{all} for $_") foreach grep { $_ ne 'global' } keys %n_occs;
    logMessage(2, "    inserted $n_rows rows into the advanced diversity table");
    
    logMessage(2, "    indexing table...");
    
    doStmt($dbh, "ALTER TABLE $DIV_ADVANCED_WORK ADD KEY (base_no)", $options->{debug});
    
    activateTables($dbh, $DIV_ADVANCED_WORK => $TABLE{DIVERSITY_STATS});
    
    if ( %missing_taxa )
    {
	my $n_missing = scalar keys %missing_taxa;

	logMessage(2, "ERROR: missing first/last appearance for $n_missing taxa.");
    }

    if ( %missing_overlap )
    {
	my $n_missing = scalar keys %missing_overlap;

	logMessage(2, "ERROR: missing overlap bins for $n_missing interval ranges.");
    }
}


sub buildPrevalenceTables {

    my ($dbh, $tree_table, $options) = @_;
    
    $options ||= {};
    
    my ($sql, $result);
    
    my $TREE_TABLE = $tree_table;
    my $INTS_TABLE = $TAXON_TABLE{$tree_table}{ints};
    my $ATTRS_TABLE = $TAXON_TABLE{$tree_table}{attrs};
    my $LOWER_TABLE = $TAXON_TABLE{$tree_table}{lower};
    
    logMessage(1, "Building prevalence tables");
    
    my ($MBL) = $dbh->selectrow_array("SELECT max(bin_level) FROM $TABLE{COLLECTION_BIN_DATA}");
    
    # Create the prevalence matrix, which tabulates taxonomic diversity across
    # space and time.
    
    logMessage(2, "    generating prevalence matrix by geographic cluster, interval, and taxonomy...");
    
    $dbh->do("DROP TABLE IF EXISTS $PVL_MATRIX_WORK");
    
    $dbh->do("CREATE TABLE $PVL_MATRIX_WORK (
		bin_id int unsigned not null,
		interval_no int unsigned not null,
		order_no int unsigned,
		class_no int unsigned,
		phylum_no int unsigned,
		n_occs int unsigned not null) Engine=MyISAM");
    
    $sql = "
    	INSERT INTO $PVL_MATRIX_WORK (bin_id, interval_no, order_no, class_no, phylum_no, n_occs)
    	SELECT bin_id_${MBL}, interval_no, order_no, class_no, phylum_no, count(*)
    	FROM $TABLE{OCCURRENCE_MATRIX} as o
		JOIN $TABLE{OCCURRENCE_MAJOR_MAP} as i using (early_age, late_age)
		JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
    		JOIN $TREE_TABLE as t using (orig_no)
    		JOIN $INTS_TABLE as ph using (ints_no)
    	WHERE o.latest_ident = 1 and c.access_level = 0 and bin_id_${MBL} > 0 
		and (order_no <> 0 or class_no <> 0 or phylum_no <> 0)
    	GROUP BY bin_id_${MBL}, interval_no, order_no, class_no, phylum_no";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      generated $result rows.");
    
    # Now sum over the entire table ignoring intervals to get counts over all
    # time.  These are stored with interval_no = 0.
    
    # $sql = "
    # 	INSERT INTO $PVL_MATRIX_WORK (bin_id, interval_no, order_no, class_no, phylum_no, n_occs)
    # 	SELECT bin_id, 0, order_no, class_no, phylum_no, sum(n_occs)
    # 	FROM $PVL_MATRIX_WORK
    # 	GROUP BY bin_id, order_no, class_no, phylum_no";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      generated $result rows for all time");
    
    # Then do the same for worldwide taxonomic prevalence.
    
    logMessage(2, "    generating global prevalence matrix by interval, and taxonomy...");
    
    $dbh->do("DROP TABLE IF EXISTS $PVL_GLOBAL_WORK");
    
    $dbh->do("CREATE TABLE $PVL_GLOBAL_WORK (
		interval_no int unsigned not null,
		order_no int unsigned,
		class_no int unsigned,
		phylum_no int unsigned,
		n_occs int unsigned not null) Engine=MyISAM");
    
    $sql = "
	INSERT INTO $PVL_GLOBAL_WORK (interval_no, order_no, class_no, phylum_no, n_occs)
	SELECT interval_no, order_no, class_no, phylum_no, count(*)
      	FROM $TABLE{OCCURRENCE_MATRIX} as o
		JOIN $TABLE{OCCURRENCE_MAJOR_MAP} as i using (early_age, late_age)
    		JOIN $TREE_TABLE as t using (orig_no)
    		JOIN $INTS_TABLE as ph using (ints_no)
    	WHERE o.latest_ident = 1 and (order_no <> 0 or class_no <> 0 or phylum_no <> 0)
    	GROUP BY interval_no, order_no, class_no, phylum_no";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      generated $result rows.");
    
    logMessage(2, "    indexing tables...");
    
    $dbh->do("ALTER TABLE $PVL_MATRIX_WORK ADD KEY (bin_id, interval_no)");
    $dbh->do("ALTER TABLE $PVL_GLOBAL_WORK ADD KEY (interval_no)");
    
    # Finally, we swap in the new tables for the old ones.
    
    activateTables($dbh, $PVL_MATRIX_WORK => $TABLE{PREVALENCE_MATRIX},
			 $PVL_GLOBAL_WORK => $TABLE{PREVALENCE_GLOBAL_MATRIX});
    
    my $a = 1;		# We can stop here when debugging.
}


