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

    # Start by creating a fresh, empty table to work with.
    
    doStmt($dbh, "DROP TABLE IF EXISTS $DIV_ADVANCED_WORK", $options->{debug});
    
    doStmt($dbh, "CREATE TABLE $DIV_ADVANCED_WORK (
		slice varchar(20) not null,
		base_no int unsigned not null,
		timescale_no int unsigned not null,
		interval_no int unsigned not null,
		rank tinyint unsigned not null,
		n_itime int unsigned not null default 0,
		n_itaxon int unsigned not null default 0,
		n_mtaxon int unsigned not null default 0,
		n_occs int unsigned not null default 0,
		n_taxa int unsigned not null default 0,
		n_implied int unsigned not null default 0,
		n_notcounted int unsigned not null default 0,
		n_origin int unsigned not null default 0,
		n_range int unsigned not null default 0,
		n_extinct int unsigned not null default 0,
		n_single int unsigned not null default 0,
		x_extinct int unsigned not null default 0,
		x_single int unsigned not null default 0,
		PRIMARY KEY (slice, base_no, interval_no, rank)) Engine=MyISAM",
	  $options->{debug});
    
    logMessage(2, "    generating advanced diversity table...");
    
    # Step 1: load auxiliary data
    # ---------------------------
    
    # Retrieve the set of intervals contained in the standard International timescales. In the
    # documentation below, "timescale" means "interval type". Timescale 3 corresponds to periods,
    # Timescale 4 to epochs, and Timescale 5 to stages. We are computing this table for the
    # Phanerozoic only, so we have no need for intervals older than that.
    
    logMessage(2, "      loading timescale data...");
    
    my (%interval_list, %bounds_list);
    
    my $min_timescale = 3;
    my $max_timescale = 5;
    
    $sql = "SELECT interval_no, scale_level, early_age
	FROM $TABLE{SCALE_MAP} join $TABLE{INTERVAL_DATA} using (interval_no)
	WHERE scale_no = 1 and scale_level in (3,4,5) and early_age < 550
	ORDER BY early_age desc";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $sth = $dbh->prepare($sql);
    $sth->execute();
    
    while ( my ($interval_no, $timescale_no, $early_age) = $sth->fetchrow_array() )
    {
	push $bounds_list{$timescale_no}->@*, $early_age + 0;
	push $interval_list{$timescale_no}->@*, $interval_no;
    }
    
    # If we weren't able to load interval data for at least one of these timescales, there is no
    # point in continuing.
    
    unless ( $bounds_list{3} || $bounds_list{4} || $bounds_list{5} )
    {
	logMessage(2, "      ERROR: could not load interval data for timescale 3") if ! $bounds_list{3};
	logMessage(2, "      ERROR: could not load interval data for timescale 4") if ! $bounds_list{4};
	logMessage(2, "      ERROR: could not load interval data for timescale 5") if ! $bounds_list{5};
	return;
    }
    
    # Finish the bounds list for each timescale by adding a 0 at the end.
    
    foreach my $ts ( $min_timescale .. $max_timescale )
    {
	next unless $bounds_list{$ts};
	push $bounds_list{$ts}->@*, 0;
    }
    
    # Then select a set of high-level taxa for which to pre-compute these diversity
    # statistics. These are referred to in the documentation below as "base taxa". For now, this
    # set includes any taxon ranking as a subclass or higher which has at least 1000 subtaxa in
    # the database.
    
    logMessage(2, "      loading base taxon data...");
    
    my (@base_list);
    
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
	logMessage(2, "      WARNING: could not load base taxon data");
    }
    
    # Step 2: retrieve occurrences
    # ----------------------------
    
    # Construct the necessary SQL statement to retrieve all occurrences from the Phanerozoic. If a
    # diagnostic filter has been provided in the options hash, add that. This is available primarily to
    # facilitate debugging of the treatment of particular subsets of occurrences if necessary.
    
    logMessage(2, "      selecting occurrences from the Phanerozoic...");
    
    my $filter = $options->{diagnostic} ? "and $options->{diagnostic}" : "";
    
    logMessage(2, "      USING FILTER: $filter") if $filter;
    
    my $sql = "SELECT o.occurrence_no, o.early_age, o.late_age, o.envtype, t.orig_no,
		t.lft, t.rgt, t.rank as ident_rank, v.is_extant, v.is_trace, v.is_form,
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
    
    # Then prepare and execute this query.
    
    $sth = $dbh->prepare($sql);
    $sth->execute();
    
    # Step 3: scan occurrences
    # ------------------------
    
    # Now scan through the result set, determining how to count each occurrence by time, taxon,
    # and slice. Slices represent subsets of the occurrences for which diversity statistics are
    # desired. For example, we generate a slice for occurrences from terrestrial environments, and
    # another for occurrences from marine environments. We also generate a slice for terrestrial
    # taxa, and another for marine taxa. We also generate slices for occurrences that are not
    # identified as trace or form taxa. Finally, we also include all occurrences in a 'global'
    # slice.
    
    logMessage(2, "      processing those occurrences...");
    
    my (%n_occs, %interval_cache, %interval_overlap, %itime_in_interval, %itime_in_bin);
    my (%sampled_in_bin, %implied_in_bin, %real_in_bin, %itaxon_in_bin, %mtaxon_in_bin);
    my (%taxon_lft, %taxon_extant, %taxon_firstbin, %taxon_lastbin, %taxon_contains);
    
    my @counted_ranks = (13, 9, 5, 4, 3);
    my %rank_field = (13 => 'order_no', 9 => 'family_no', 5 => 'genus_no',
		      4 => 'subgenus_no', 3 => 'species_no');
    my %timescale_label = (3 => 'period', 4 => 'epoch', 5 => 'stage');
    my %rank_label = (13 => 'order', 9 => 'family', 5 => 'genus', 4 => 'subgenus', 3 => 'species');
    
 OCCURRENCE:
    while ( my $r = $sth->fetchrow_hashref )
    {
	# We start by selecting the set of slices in which the occurrence will be counted. If this
	# occurrence is marked as coming from one or more particular environments, it will be
	# counted both in the global total and under those environments. An occurrence labeled as
	# 'marine_x' is one where the organism is one that is recorded as living in a terrestrial
	# environment even though the fossil was recovered from a marine environment. The label
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
	
	# For each slice, we add an additional slice suffixed with :notf that counts just
	# occurrences that are identified as neither trace nor form taxa.
	
	unless ( $r->{is_trace} || $r->{is_form} )
	{
	    push @slice, map { "$_:notf" } @slice;
	}
	
	# Keep a count of all occurrences by slice and by one of the taxa under which it is
	# identified. The latter allows us to count the total number of occurrences identified
	# under each base taxon. We use ints_no whenever it is defined, because that keeps the
	# number of separate entries in the hash reasonably small.
	
	$n_occs{$_}{all}++ foreach @slice;
	
	my $count_no = $r->{ints_no} || $r->{orig_no} || 0;
	
	if ( $count_no )
	{
	    $n_occs{$_}{taxon}{$count_no}++ foreach @slice;
	    $taxon_lft{$count_no} //= $r->{lft};
	}
	
	# Step 3A: bin by interval
	# ------------------------
	
	# Next figure out the intervals in which to bin this occurrence, using the "major"
	# timerule separately for each timescale. The "major" timerule selects for each timescale
	# the interval (if any) that includes more than half of the age range associated with the
	# occurrence. Occurrences whose age range is so broad that every overlapping interval
	# covers less than half of it will be counted under "imprecise time" for that timescale.
	
	# This process starts with the age range recorded for this occurrence.
	
	my $interval_key = "$r->{early_age}-$r->{late_age}";
	
	# If we have already computed the matching intervals for this age range, then we can just
	# use the cached results. Otherwise, we need to go through each list of intervals at each
	# level and pick the one into which this occurrence should be binned.
	
	unless ( $interval_cache{$interval_key} )
	{
	    $interval_cache{$interval_key} = { };
	    
	    # Turn the bounds into numbers for comparison.
	    
	    my $early_occ = $r->{early_age} + 0;
	    my $late_occ = $r->{late_age} + 0;
	    
	    # Scan the bounds list corresponding to each interval type that we have data for.
	    
	    foreach my $level ( $min_timescale .. $max_timescale )
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
		    # it, add it to the overlap list. We will use this list below to distribute
		    # the counts of "imprecise time" occurrences as evenly as possible into the
		    # overlapping intervals.
		    
		    else
		    {
			push @overlap_bins, $i;
		    }
		}
		
		# If we were not able to find a matching interval for the age range at this level,
		# keep the overlap list for use below.
		
		unless ( defined $interval_cache{$interval_key}{$level} )
		{
		    $interval_overlap{$interval_key}{$level} = \@overlap_bins;
		}
	    }
	}
	
	# Now count this occurrence for each possible combination of slice, timescale, and
	# taxonomic rank. Start by iterating over the timescales.
	
      TIMESCALE:
	foreach my $ts ( $min_timescale .. $max_timescale )
	{
	    # For each timescale, check whether an interval from that timescale has been selected
	    # for this occurrence's age range.
	    
	    my $bin_index = $interval_cache{$interval_key}{$ts};
	    
	    # If not, count this occurrence under "imprecise time" and skip to the next timescale.
	    
	    unless ( defined $bin_index )
	    {
		$itime_in_interval{$_}{$ts}{$interval_key}{$count_no}++ foreach @slice;
		
		next TIMESCALE;
	    }
	    
	    # If an interval has been selected, get the interval_no in which this occurrence is
	    # binned and proceed to count the occurrence taxonomically.
	    
	    my $interval_no = $interval_list{$ts}[$bin_index];
	    
	    # Step 3B: bin by taxon
	    # ---------------------
	    
	    # Count this occurrence at selected taxonomic ranks, down to the specific rank at
	    # which it was identified. For example, an occurrence that is identified to the genus
	    # level will have its genus, family, and order counted. An occurrence that is
	    # identified to the species level will have its species, subgenus, genus, family, and
	    # order counted.
	    
	    foreach my $rank ( @counted_ranks )
	    {
		# For each rank at which we are counting, extract the taxon identifier (if any)
		# for the occurrence at this rank. The taxon identifier for rank 13, for example,
		# is found in the field 'order_no'.
		
		my $taxon_no = $r->{$rank_field{$rank}};
		
		# When we are counting at a rank below genus, a missing genus means that this is a
		# nomen vanum, nomen dubium, or otherwise invalid.
		
		if ( $rank < 5 && ! $r->{genus_no} )
		{
		    $taxon_no = undef;
		}
		
		# When we are counting subgenera, default to the genus if there is no subgenus
		# listed. This is counted as a real taxon, not an implied one.
		
		elsif ( $rank == 4 )
		{
		    $taxon_no ||= $r->{genus_no};
		}
		
		# If we have found a taxon identifier for this occurrence at this rank, count the
		# occurrence as fully binned. We count the number of occurrences for each taxon
		# separately, so that we can sum them for each of the base taxa and for the
		# overall count as well.
		
		if ( $taxon_no )
		{
		    $sampled_in_bin{$_}{$interval_no}{$rank}{$taxon_no}++ foreach @slice;
		    
		    # Also keep track of the first and last occurrence of this taxon, using the
		    # index from the list of intervals for this timescale. This information will
		    # be used below to generate the Foote statistics.
		    
		    foreach my $slice ( @slice )
		    {
			unless ( defined $taxon_firstbin{$slice}{$ts}{$rank}{$taxon_no} &&
				 $taxon_firstbin{$slice}{$ts}{$rank}{$taxon_no} <= $bin_index )
			{
			    $taxon_firstbin{$slice}{$ts}{$rank}{$taxon_no} = $bin_index;
			}
			
			unless ( defined $taxon_lastbin{$slice}{$ts}{$rank}{$taxon_no} &&
				 $taxon_lastbin{$slice}{$ts}{$rank}{$taxon_no} >= $bin_index )
			{
			    $taxon_lastbin{$slice}{$ts}{$rank}{$taxon_no} = $bin_index;
			}
		    }
		    
		    # Record information about this taxon necessary to properly report it. The
		    # %taxon_lft hash records, for each counted taxon, the 'lft' value for one of
		    # its subtaxa. This allows us to sum up the counts for each of the base taxa,
		    # and for this purpose it doesn't matter which subtaxon is chosen. The
		    # 'is_extant' information will be used to generate correction statistics for
		    # singletons and extinctions, noting how many of those actually correspond to
		    # extant taxa.
		    
		    $taxon_lft{$taxon_no} //= $r->{lft};
		    $taxon_extant{$taxon_no} = 1 if $r->{is_extant};
		}
		
		# Step 3C: count implied taxa
		# ---------------------------
		
		# If this occurrence cannot be identified at this rank, count it as taxonomically
		# imprecise. Where possible, we identify an *implied* taxon and count that. For
		# example, if we are counting genera and this occurrence is identified as
		# belonging to a family or order which is not otherwise found in this interval, we
		# can assume one additional unknown genus. We don't know which genus it might be,
		# but we know there must be at least one in addition to the genera that have been
		# specifically identified. Doing this properly involves keeping track of all the
		# actually identified taxa below. We do not keep track of origination and
		# extinction for implied taxa, because we cannot infer this information.
		
		else
		{
		    # If the rank of the occurrence's taxonomic identification is higher than the
		    # rank at which we are counting, count this occurrence as having an imprecise
		    # taxonomic identification.
		    
		    if ( $r->{ident_rank} > $rank )
		    {
			$itaxon_in_bin{$_}{$interval_no}{$rank}{$count_no}++ foreach @slice;
		    }
		    
		    # If the rank of this occurrence's taxonomic identification is lower than the
		    # rank at which we are counting, we count this occurrence as having a missing
		    # taxonomic identification at the rank being counted. This is most often
		    # caused by incomplete data entry. For example, there are many genera that are
		    # not assigned to families in our database, so occurrences identified to these
		    # genera cannot be counted at the family level.
		    
		    else
		    {
			$mtaxon_in_bin{$_}{$interval_no}{$rank}{$count_no}++ foreach @slice;
		    }
		    
		    # Where possible, we identify a higher ranking taxon that implies an unknown
		    # taxon at the rank being counted.
		    
		    my $implied_no;
		    
		    # When counting orders, we check the class_no if it is defined. If not, we
		    # check the ints_no if it ranks higher than order. If no fully binned
		    # occurrence is identified within this taxon, it implies an additional order.
		    
		    if ( $rank == 13 )
		    {
			$implied_no = $r->{class_no} || ($r->{ints_rank} >= 13 ? $r->{ints_no} : undef);
		    }
		    
		    # When counting families, we check the order_no if it is defined. If not, we
		    # check the ints_no if its rank is higher than family. If no fully binned
		    # occurrence is identified within this taxon, it implies an additional family.
		    
		    elsif ( $rank == 9 )
		    {
			$implied_no = $r->{order_no} || ($r->{ints_rank} >= 9 ? $r->{ints_no} : undef);
		    }

		    # When counting genera and subgenera, we check the ints_no if it is
		    # defined. For each genus, the ints_no represents the lowest ranking
		    # containing taxon above the genus level. If no fully binned occurrence is
		    # identified within this taxon, it implies an additional genus.
		    
		    elsif ( $rank == 5 || $rank == 4 )
		    {
			$implied_no = $r->{ints_no};
		    }
		    
		    # When counting species, we check the genus_no if it is defined. If not, it is
		    # usually because the genus is marked as a nomen dubium or other invalid
		    # subtaxon. In that case, we fall back to the ints_no as in the case above. If
		    # no fully binned occurrence is identified within this taxon, it implies an
		    # additional species.
		    
		    elsif ( $rank == 3 )
		    {
			$implied_no = $r->{genus_no} || $r->{ints_no};
		    }
		    
		    # If we have found an implied taxon, count it. Set $taxon_lft for the implied
		    # taxon if it isn't already set.
		    
		    if ( $implied_no )
		    {
			$implied_in_bin{$_}{$interval_no}{$rank}{$implied_no} = 1 foreach @slice;
			
			$taxon_lft{$implied_no} //= $r->{lft};
		    }
		}
	    }
	    
	    # Step 3D: count real taxa
	    # ------------------------
	    
	    # After we have counted this occurrence at all of the ranks to which it is identified,
	    # we need to mark all of the counted taxa as actually being found in this interval in
	    # order to avoid double-counting implied taxa. These are tracked separately for each
	    # slice/interval combination.
	    
	    foreach my $s ( @slice )
	    {
		# Create a hash for this slice/interval combination if it doesn't already exist.
		
		$real_in_bin{$s}{$interval_no} ||= { };
		my $real_in_bin = $real_in_bin{$s}{$interval_no};
		
		# For each taxonomic rank (genus or above) under which this occurrence has been
		# counted, note that taxon as being found for real in this interval.
		
		if ( $r->{genus_no} )
		{
		    $real_in_bin->{$r->{genus_no}} //= 1;
		}
		
		if ( $r->{family_no} )
		{
		    $real_in_bin->{$r->{family_no}} //= 1;
		}
		
		if ( $r->{order_no} )
		{
		    $real_in_bin->{$r->{order_no}} //= 1;
		}
		
		# If the occurrence is identified to at least one of these ranks, note its class
		# as being found as well. The convoluted way this code is written minimizes
		# execution time for the common case in which numerous occurrences belong to the
		# same class.
		
		if ( $r->{class_no} && ! defined $real_in_bin->{$r->{class_no}} )
		{
		    if ( $r->{genus_no} || $r->{family_no} || $r->{order_no} )
		    {
			$real_in_bin->{$r->{class_no}} = 1;
		    }
		}
		
		# If this occurrence is counted at any taxonomic rank at or below its ints_rank,
		# count its ints_no value as being real too. This is necessary because we use
		# ints_no to represent implied taxa under some circumstances (see above).
		
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
    
    # Step 4: compute taxon incusion
    # ------------------------------
    
    # For each unique taxon we have found, run through the set of base taxa and record which ones
    # it is contained in. This information is recorded in %taxon_contains. Users will thus be
    # able to immediately fetch full diversity statistics for those base taxa from the table being
    # generated by this subroutine.
    
    # NOTE: the value stored in %taxon_lft for a given taxon_no value is not usually the lft value
    # for that particular taxon. Rather, it is the lft value for some subtaxon that falls within
    # it. This is sufficient for the purpose, i.e. to record which counted taxa are contained
    # within each selected base taxon.
    
    logMessage(2, "      recording base taxon inclusion...");
    
    foreach my $taxon_no ( keys %taxon_lft )
    {
	foreach my $b ( @base_list )
	{
	    if ( $taxon_lft{$taxon_no} >= $b->{lft} && $taxon_lft{$taxon_no} <= $b->{rgt} )
	    {
		$taxon_contains{$b->{orig_no}}{$taxon_no} = 1;
	    }
	}
    }
    
    # Step 5: distribute imprecise occurrences
    # ----------------------------------------
    
    # This step may represent over-elaboration, but I think it important to report counts of
    # time-imprecise occurrences for each time interval in detail. Earlier versions of this
    # algorithm reported them only in aggregate. The reason for this is to allow a user to see
    # (roughly) the number of occurrences overlapping each interval that were not counted because
    # they were not identified with sufficient temporal precision. This allows one to gauge the
    # extent to which temporal imprecision has impacted the diversity statistics for a given time
    # period, slice, and base taxon.
    
    # At the same time, we want the sum of imprecise occurrence counts reported for the intervals
    # in a given timescale to remain accurate. This sum can then be added to the sum of binned
    # occurrence counts for that timescale and the result checked against the number of
    # occurrences known to be in the database. If they match, that provides confirmation that all
    # of the occurrences were properly accounted for.
    
    # In order to meet both of these criteria, we distribute the count of unbinned occurrences for
    # each imprecise age range as evenly as possible across every bin that overlaps the
    # range. That way, the sum will remain accurate and it will also be possible to see which
    # intervals have a lot of imprecise occurrences overlapping them and which do not.
    
    logMessage(2, "      distributing imprecise occurrences over intervals...");
    
    my %missing_overlap;
    
    # This process requires iterating over each combination of slice, timescale, and un-binnable
    # time interval that has been found.
    
    foreach my $slice ( keys %n_occs )
    {
	foreach my $ts ( $min_timescale .. $max_timescale )
	{
	    foreach my $interval_key ( keys $itime_in_interval{$slice}{$ts}->%* )
	    {
		my $bins = $interval_overlap{$interval_key}{$ts};
		
		# If there aren't any overlapping intervals recorded for this age range, an error
		# has occurred. Note it.
		
		unless ( @$bins )
		{
		    $missing_overlap{$interval_key}++;
		    next;
		}
		
		my $interval_occs = $itime_in_interval{$slice}{$ts}{$interval_key};
		
		next unless $interval_occs && ref $interval_occs eq 'HASH';
		
		# We have to do this distribution for separately for each taxon (count_no) we have
		# counted, so that statistics for the various base taxa can each include a correct
		# imprecise-time-occurrence count for each interval for all of the taxa contained
		# in that base group. Yes, this is probably more elaborate than strictly
		# necessary, but I think there will be situations in which these statistics will
		# be useful. The result has to add up to the already recorded count, but it
		# doesn't really matter exactly how they are distributed as long as it is
		# somewhere close to even.
		
		foreach my $taxon_no ( keys $interval_occs->%* )
		{
		    my $occs = $interval_occs->{$taxon_no};
		    
		    # We start with the number of occurrences divided by the number of intervals
		    # into which they must be distributed.
		    
		    my $fraction = $occs/@$bins;
		    my ($skip, $drop);
		    
		    # If the number of occurrences is half or less of the number of intervals,
		    # compute a skip count. One occurrence will be dropped into every other
		    # interval, or every third, etc.
		    
		    if ( $fraction <= 0.5 )
		    {
			$skip = int(@$bins/$occs) - 1;
			$drop = 1;
		    }
		    
		    # Otherwise, round up the fraction and drop that number of occurrences into
		    # every interval.
		    
		    else
		    {
			$skip = 0;
			$drop = int($fraction+0.5);
		    }
		    
		    my $k = $skip;
		    my $interval_no;
		    
		    # Run through the list of intervals overlapping the age range we are dealing
		    # with, younger to older. Into each interval that isn't skipped, drop the
		    # specified number of occurrences or the remaining count if it is smaller.
		    
		    foreach my $bin_index ( reverse @$bins )
		    {
			$interval_no = $interval_list{$ts}[$bin_index];
			
			if ( $k-- ) { next; }
			else { $k = $skip; }
			
			my $d = $occs > $drop ? $drop : $occs;
			
			$itime_in_bin{$slice}{$interval_no}{$taxon_no} += $d;
			$occs -= $d;
			
			last unless $occs;
		    }

		    # If any occurrences are left over, just drop them into the last interval we
		    # considered. The most important thing is to keep the sum correct.
		    
		    $itime_in_bin{$slice}{$interval_no}{$taxon_no} += $occs if $occs;
		}
	    }
	}
    }
    
    # Step 6: generate diversity statistics
    # -------------------------------------
    
    # Our goal is to generate the usual diversity output including the four diversity statistics
    # defined by Foote: n_origin = XFt, n_range = Xbt, n_extinct = XbL, n_single = XFL. In
    # addition, the fields x_extinct and x_single provide corrections for the last two statistics
    # for taxa that are known to be extant.
    
    # Create a statement to insert the statistics for one slice/base-taxon/interval/rank
    # combination into the database table we are generating.
    
    $sql = "INSERT INTO $DIV_ADVANCED_WORK (slice, base_no, timescale_no, interval_no, rank,
    	n_itime, n_itaxon, n_mtaxon, n_occs, n_taxa, n_implied, n_notcounted,
    	n_origin, n_range, n_extinct, n_single, x_extinct, x_single)
    	values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    my $insert_sth = $dbh->prepare($sql);
    
    # Iterate through all combinations of slice/timescale/rank. For each such combination, run
    # through all the taxa we have counted at that rank and accumulate the Foote statistics for
    # the intervals in that timescale. For each interval, insert one row into the table for the
    # slice as a whole, and one row for each base taxon.
    
    my (%missing_taxa, $inserted_rows);
    
    foreach my $slice ( keys %n_occs )
    {
	logMessage(2, "      counting diversity for slice '$slice'...");
	
	foreach my $ts ( $min_timescale .. $max_timescale )
	{
	    logMessage(2, "          by $timescale_label{$ts}");
	    
	    foreach my $rank ( @counted_ranks )
	    {
		# The following variables accumulate the Foote statistics and corrections for each
		# interval in the timescale being processed.
		
		my (%n_origin, %n_range, %n_extinct, %n_single, %x_extinct, %x_single);
		
		# Check the first and last appearance of each counted taxon, and increment the
		# statistics for the indicated range of intervals.
		
		foreach my $taxon_no ( keys $taxon_firstbin{$slice}{$ts}{$rank}->%* )
		{
		    my $first_index = $taxon_firstbin{$slice}{$ts}{$rank}{$taxon_no};
		    my $last_index = $taxon_lastbin{$slice}{$ts}{$rank}{$taxon_no};
		    
		    # If we don't have both a first and last index into the interval list, an
		    # error has occurred. Note it.
		    
		    unless ( defined $first_index && defined $last_index )
		    {
			$missing_taxa{$taxon_no} = 1;
			next;
		    }
		    
		    my $first_interval = $interval_list{$ts}[$first_index];
		    my $last_interval = $interval_list{$ts}[$last_index];
		    
		    # # If this taxon is implied and also found for real in its first interval, bump
		    # # up the first interval until we find one where it doesn't have that status.

		    # while ( $first_interval &&
		    # 	    $implied_in_bin{$slice}{$first_interval}{$rank}{$taxon_no} &&
		    # 	    $real_in_bin{$slice}{$first_interval}{$taxon_no} )
		    # {
		    # 	$first_index++;
		    # 	$first_interval = $interval_list{$ts}[$first_index];
		    # }
		    
		    # # Make the same adjustment to the end of the range. If this results in no
		    # # range at all, that means the taxon won't be counted at all in the code
		    # # below.
		    
		    # while ( $last_interval && $last_index > $first_index &&
		    # 	    $implied_in_bin{$slice}{$last_interval}{$rank}{$taxon_no} &&
		    # 	    $real_in_bin{$slice}{$last_interval}{$taxon_no} )
		    # {
		    # 	$last_index--;
		    # 	$last_interval = $interval_list{$ts}[$last_index];
		    # }
		    
		    # If the interval of first appearance is the same as the interval of last
		    # appearance, then this is a singleton. If the taxon is extant, include it in
		    # the x_single correction statistic because that means it isn't really a
		    # singleton if we are taking extancy into account.
		    
		    if ( $first_index == $last_index )
		    {
			$n_single{$first_interval}{$taxon_no} = 1;
			$x_single{$first_interval}{$taxon_no} = 1 if $taxon_extant{$taxon_no};
			    # && ! $implied_in_bin{$slice}{$first_interval}{$rank}{$taxon_no};
		    }
		    
		    # Otherwise, we count the origin as the first interval where the taxon appears
		    # and the extinction as the last, ranging through the intervals in between. If
		    # the taxon is extant, include it in the x_extinct correction statistic for
		    # the ending bin because that isn't really the last appearance of this taxon
		    # if we are taking extancy into account.
		    
		    elsif ( $first_interval && $last_interval && $last_index > $first_index )
		    {
			$n_origin{$first_interval}{$taxon_no} = 1;
			$n_range{$_}{$taxon_no} = 1
			    foreach $interval_list{$ts}->@[$first_index+1..$last_index-1];
			$n_extinct{$last_interval}{$taxon_no} = 1;
			$x_extinct{$last_interval}{$taxon_no} = 1 if $taxon_extant{$taxon_no};
			   # && ! $implied_in_bin{$slice}{$last_interval}{$rank}{$taxon_no};
		    }
		}
		
		# Then run through all of the intervals in this timescale from start to end. For
		# each interval, count the unique taxa (both sampled and implied) for this
		# slice/interval/rank combination and sum the occurrences across all the taxa. Do
		# the same for each of the base taxa, counting only the taxa contained in each
		# one. Add a row to the table with base_no = 0 containing statistics for the slice
		# as a whole, and one for each base taxon as well.
		
		foreach my $interval_no ( $interval_list{$ts}->@* )
		{
		    my $itime_occs = $itime_in_bin{$slice}{$interval_no};
		    my $itaxon_occs = $itaxon_in_bin{$slice}{$interval_no}{$rank};
		    my $mtaxon_occs = $mtaxon_in_bin{$slice}{$interval_no}{$rank};
		    my $sampled_occs = $sampled_in_bin{$slice}{$interval_no}{$rank};
		    my $implied_taxon = $implied_in_bin{$slice}{$interval_no}{$rank};
		    my $real_taxon = $real_in_bin{$slice}{$interval_no};
		    
		    # # For each of the implied taxa in the interval, cancel the ones that appear in
		    # # the actual counts so that we don't double count them. Correct the occurrence
		    # # counts so that the occurrences of the implied taxa are instead counted as
		    # # imprecise.
		    
		    # foreach my $taxon_no ( keys $implied_taxon->%* )
		    # {
		    # 	if ( $real_taxon->{$taxon_no} )
		    # 	{
		    # 	    my $occ_count = $sampled_occs->{$taxon_no};
			    
		    # 	    $itaxon_occs->{$taxon_no} += $occ_count;
		    # 	    $itaxon_occs->{total} += $occ_count;
		    # 	    $sampled_occs->{total} -= $occ_count;
		    # 	    delete $sampled_occs->{$taxon_no};
		    # 	    delete $implied_taxon->{$taxon_no};
		    # 	    $canceled_taxon{$taxon_no} = 1;
		    # 	}
		    # }

		    my $n_itime = 0; $n_itime += $itime_occs->{$_} foreach
			keys $itime_occs->%*;
		    my $n_itaxon = 0; $n_itaxon += $itaxon_occs->{$_} foreach
			keys $itaxon_occs->%*;
		    my $n_mtaxon = 0; $n_mtaxon += $mtaxon_occs->{$_} foreach
			keys $mtaxon_occs->%*;
		    my $n_occs = 0; $n_occs += $sampled_occs->{$_} foreach
			keys $sampled_occs->%*;
		    		    
		    # my $n_itime = $itime_occs->{total} // 0;
		    # my $n_itaxon = $itaxon_occs->{total} // 0;		    
		    # my $n_occs = $sampled_occs->{total} // 0;
		    
		    my $n_taxa = scalar keys $sampled_occs->%*;

		    my $n_implied = 0;
		    my $n_notcounted = 0;
		    
		    foreach ( keys $implied_taxon->%* )
		    {
			my $dummy = $real_taxon->{$_} ? $n_notcounted++ : $n_implied++;
		    }
		    
		    # my $n_implied = scalar keys $implied_taxon->%*;
		    # my $n_canceled = scalar keys %canceled_taxon;
		    
		    my $n_origin = scalar keys $n_origin{$interval_no}->%*;
		    my $n_range = scalar keys $n_range{$interval_no}->%*;
		    my $n_extinct = scalar keys $n_extinct{$interval_no}->%*;
		    my $n_single = scalar keys $n_single{$interval_no}->%*;
		    my $x_extinct = scalar keys $x_extinct{$interval_no}->%*;
		    my $x_single = scalar keys $x_single{$interval_no}->%*;
		    
		    $insert_sth->execute($slice, 0, $ts, $interval_no, $rank,
					 $n_itime, $n_itaxon, $n_mtaxon, $n_occs,
					 $n_taxa, $n_implied, $n_notcounted,
					 $n_origin, $n_range, $n_extinct, $n_single,
					 $x_extinct, $x_single);
		    $inserted_rows++;
		    
		    # Now run through all of the base taxa, and generate a row for each one.
		    
		    foreach my $b ( @base_list )
		    {
			my $base_no = $b->{orig_no};
			my $contains = $taxon_contains{$base_no};
			
			my $n_itime = 0; $n_itime += $itime_occs->{$_} foreach
			    grep { $contains->{$_} } keys $itime_occs->%*;
			
			my $n_itaxon = 0; $n_itaxon += $itaxon_occs->{$_} foreach
			    grep { $contains->{$_} } keys $itaxon_occs->%*;
			
			my $n_mtaxon = 0; $n_mtaxon += $mtaxon_occs->{$_} foreach
			    grep { $contains->{$_} } keys $mtaxon_occs->%*;
			
			my $n_occs = 0;
			my $n_taxa = 0;
			my $n_implied = 0;
			my $n_notcounted = 0;
			
			foreach ( grep { $contains->{$_} } keys $sampled_occs->%* )
			{
			    $n_occs += $sampled_occs->{$_};
			    $n_taxa++;
			}
			
			foreach ( grep { $contains->{$_} } keys $implied_taxon->%* )
			{
			    my $dummy = $real_taxon->{$_} ? $n_notcounted++ : $n_implied++;
			}
			
			# my $n_canceled = scalar grep { $contains->{$_} } keys %canceled_taxon;
			
			my $n_origin = scalar grep { $contains->{$_} }
			    keys $n_origin{$interval_no}->%*;
			my $n_range = scalar grep { $contains->{$_} }
			    keys $n_range{$interval_no}->%*;
			my $n_extinct = scalar grep { $contains->{$_} }
			    keys $n_extinct{$interval_no}->%*;
			my $n_single = scalar grep { $contains->{$_} }
			    keys $n_single{$interval_no}->%*;
			my $x_extinct = scalar grep { $contains->{$_} }
			    keys $x_extinct{$interval_no}->%*;
			my $x_single = scalar grep { $contains->{$_} }
			    keys $x_single{$interval_no}->%*;
			
			$insert_sth->execute($slice, $base_no, $ts, $interval_no, $rank,
					     $n_itime, $n_itaxon, $n_mtaxon, $n_occs,
					     $n_taxa, $n_implied, $n_notcounted,
					     $n_origin, $n_range, $n_extinct, $n_single,
					     $x_extinct, $x_single);
			
			$inserted_rows++;
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
	
	$inserted_rows++;
	
	# Also add a row for the total number of occurrences for each base taxon.
	
	foreach my $b ( @base_list )
	{
	    my $base_no = $b->{orig_no};
	    my $contains = $taxon_contains{$base_no};
	    
	    my $n_occs = 0; $n_occs += $n_occs{$slice}{taxon}{$_} foreach
		grep { $contains->{$_} } keys $n_occs{$slice}{taxon}->%*;
	    
	    my $quoted_base = $dbh->quote($base_no);
	    my $quoted_occs = $dbh->quote($n_occs);
	    
	    $sql = "INSERT INTO $DIV_ADVANCED_WORK
		       (slice, base_no, timescale_no, interval_no, rank, n_occs)
		VALUES ($quoted_slice, $quoted_base, 0, 0, 0, $quoted_occs)";
	    
	    doStmt($dbh, $sql, $options->{debug});

	    $inserted_rows++;
	}
    }
    
    # Report the number of occurrences processed, the number of table rows inserted, and any
    # errors we found.
    
    logMessage(2, "    processed $n_occs{global}{all} occurrences");
    logMessage(2, "       $n_occs{$_}{all} for $_") foreach grep { $_ ne 'global' } keys %n_occs;
    logMessage(2, "    inserted $inserted_rows rows into the advanced diversity table");
    
    logMessage(2, "    indexing table...");
    
    doStmt($dbh, "ALTER TABLE $DIV_ADVANCED_WORK ADD KEY (base_no, slice, timescale_no, rank)",
	   $options->{debug});
    
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


