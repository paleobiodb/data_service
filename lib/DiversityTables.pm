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
    
    my ($sql, $sth);
    
    doStmt($dbh, "DROP TABLE IF EXISTS $DIV_ADVANCED_WORK", $options->{debug});
    
    doStmt($dbh, "CREATE TABLE $DIV_ADVANCED_WORK (
		slice enum('global', 'marine_env', 'terr_env', 'marine_taxa', 'terr_taxa') not null,
		timescale_no int unsigned not null,
		interval_no int unsigned not null,
		rank tinyint unsigned not null,
		n_itime int unsigned not null default 0,
		n_itaxon int unsigned not null default 0,
		n_occs int unsigned not null default 0,
		n_taxa int unsigned not null default 0,
		n_implied int unsigned not null default 0,
		n_origin int unsigned not null default 0,
		n_range int unsigned not null default 0,
		n_extinct int unsigned not null default 0,
		n_single int unsigned not null default 0,
		x_extinct int unsigned not null default 0,
		x_single int unsigned not null default 0,
		PRIMARY KEY (slice, rank, timescale_no, interval_no),
		KEY (interval_no)) Engine=MyISAM",
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
    
    my (%interval_list, %bounds_list, %bounds_interval);
    
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
    
    # Now construct the necessary SQL statement to retrieve all occurrences from the
    # Phanerozoic. If a diagnostic filter has been provided in the options hash, add that.
    
    my $filter = $options->{diagnostic} ? "and $options->{diagnostic}" : "";
    
    logMessage(2, "      selecting occurrences from the Phanerozoic...");
    
    my $sql = "SELECT o.occurrence_no, o.early_age, o.late_age,
		v.is_extant, v.is_trace, o.envtype, et.taxon_environment,
		pl.species_no, pl.subgenus_no, pl.genus_no, ph.family_no, ph.order_no
	FROM $TABLE{OCCURRENCE_MATRIX} as o
		JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
		LEFT JOIN $TREE_TABLE as t using (orig_no)
		LEFT JOIN $TREE_TABLE as ta on ta.orig_no = t.accepted_no
		LEFT JOIN $ATTRS_TABLE as v on v.orig_no = ta.orig_no
		LEFT JOIN $ECOTAPH_TABLE as et on et.orig_no = ta.orig_no
		LEFT JOIN $LOWER_TABLE as pl on pl.orig_no = ta.orig_no
		LEFT JOIN $INTS_TABLE as ph on ph.ints_no = ta.ints_no
	WHERE o.latest_ident and c.access_level = 0 and ta.orig_no > 0 and o.early_age < 550 $filter
	GROUP BY o.occurrence_no";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    # Then prepare and execute it.
    
    $sth = $dbh->prepare($sql);
    $sth->execute();
    
    # Now scan through the result set, and determine how (and whether) to count each occurrence by
    # time, taxon, and environment (marine/terrestrial).
    
    logMessage(2, "      processing those occurrences...");
    
    my (%n_occs, %n_imprecise_time, %n_imprecise_distributed, %n_imprecise_rank);
    my (%interval_cache, %interval_overlap);
    my (%sampled_in_bin, %implied_in_bin, %extant_taxon, %taxon_firstbin, %taxon_lastbin);
    
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
	
	my @env;
	
	if ( $r->{envtype} eq 'marine' )
	{
	    push @env, 'marine_env', 'marine_taxa';
	}

	elsif ( $r->{envtype} eq 'marine_x' )
	{
	    push @env, 'marine_env', 'terr_taxa';
	}
	
	elsif ( $r->{envtype} eq 'terrestrial' )
	{
	    push @env, 'terr_env', 'terr_taxa';
	}
	
	elsif ( $r->{envtype} eq 'terrestrial_x' )
	{
	    push @env, 'terr_env', 'marine_taxa';
	}
	
	# Keep a global count of all occurrences, and all occurrences by environment.
	
	$n_occs{global}{all}++;
	$n_occs{$_}{all}++ foreach @env;
	
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
		$n_imprecise_time{global}{$level}{$interval_key}++;
		$n_imprecise_time{$_}{$level}{$interval_key}++ foreach @env;
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
		
		# When we are counting subgenera, default to the genus if there is no subgenus
		# listed.
		
		$taxon_no ||= $r->{genus_no} if $rank == 4;
		
		# If we have found a taxon identifier for this occurrence at this rank, count it
		# as fully binned.
		
		if ( $taxon_no )
		{
		    $n_occs{global}{$interval_no}{$rank}++;
		    $n_occs{$_}{$interval_no}{$rank}++ foreach @env;
		    
		    # Keep track of the number of unique taxa of this rank found in each interval.
		    
		    $sampled_in_bin{global}{$interval_no}{$rank}{$taxon_no}++;
		    $sampled_in_bin{$_}{$interval_no}{$rank}{$taxon_no}++ foreach @env;
		    
		    # If this is the oldest occurrence of the taxon that we have found so far at
		    # this level, mark it as originating in the selected interval.
		    
		    unless ( defined $taxon_firstbin{global}{$level}{$rank}{$taxon_no} &&
			     $taxon_firstbin{global}{$level}{$rank}{$taxon_no} <= $bin_index )
		    {
			$taxon_firstbin{global}{$level}{$rank}{$taxon_no} = $bin_index;
		    }
		    
		    # If this is the youngest occurrence of the taxon that we have found so far at
		    # this level, mark it as ending in the selected interval.
		    
		    unless ( defined $taxon_lastbin{global}{$level}{$rank}{$taxon_no} &&
			     $taxon_lastbin{global}{$level}{$rank}{$taxon_no} >= $bin_index )
		    {
			$taxon_lastbin{global}{$level}{$rank}{$taxon_no} = $bin_index;
		    }
		    
		    # If the occurrence falls into one or more of the defined environments,
		    # count it separately under of them as well.
		    
		    foreach my $env ( @env )
		    {
			unless ( defined $taxon_firstbin{$env}{$level}{$rank}{$taxon_no} &&
				 $taxon_firstbin{$env}{$level}{$rank}{$taxon_no} <= $bin_index )
			{
			    $taxon_firstbin{$env}{$level}{$rank}{$taxon_no} = $bin_index;
			}
			
			unless ( defined $taxon_lastbin{$env}{$level}{$rank}{$taxon_no} &&
				 $taxon_lastbin{$env}{$level}{$rank}{$taxon_no} >= $bin_index )
			{
			    $taxon_lastbin{$env}{$level}{$rank}{$taxon_no} = $bin_index;
			}
		    }
		    
		    # If the identified taxon is extant, record that fact.
		    
		    $extant_taxon{$taxon_no} = 1 if $r->{is_extant};
		    
		    # If the next higher rank is specified, mark that taxon with a 2 to indicate
		    # that it does not count as an implied taxon because we have found an actual
		    # taxon that is a member of that group in this bin.
		    
		    if ( $implied_field{$rank} && $r->{$implied_field{$rank}} )
		    {
			my $implied_no = $r->{$implied_field{$rank}};
			$implied_in_bin{global}{$interval_no}{$rank}{$implied_no} = 2;
			$implied_in_bin{$_}{$interval_no}{$rank}{$implied_no} = 2 foreach @env;
		    }
		}
		
		# Otherwise, count this occurrence as being imprecise at this rank.
		
		else
		{
		    $n_imprecise_rank{global}{$interval_no}{$rank}++;
		    $n_imprecise_rank{$_}{$interval_no}{$rank}++ foreach @env;
		    
		    # If the next higher rank is specified, and that taxon has not yet been seen
		    # in this bin, mark that taxon with a 1 to indicate that it should be counted
		    # as an applied taxon at the lower rank. For example, when we are counting at
		    # the genus level, each unique family that is represented by an occurrence
		    # with no genus identification should be counted as an implied genus. That
		    # occurrence implies that at least one more genus was found in that bin, even
		    # though we don't know exactly what genus it was.
		    
		    if ( $implied_field{$rank} && $r->{$implied_field{$rank}} )
		    {
			my $implied_no = $r->{$implied_field{$rank}};
			$implied_in_bin{global}{$interval_no}{$rank}{$implied_no} //= 1;
			$implied_in_bin{$_}{$interval_no}{$rank}{$implied_no} //= 1 foreach @env;
		    }
		}
	    }
	}
    }
    
    # This may be over-elaboration, but for every age range for which there were one or more
    # occurrences with that range that were unable to be binned because they do not precisely
    # correspond to any interval, distribute those occurrences as evenly as possible among all of
    # the overlapping intervals and record the counts in %n_imprecise_distributed by interval.
    
    my %missing_overlap;
    
    foreach my $env ( keys %n_occs )
    {
	foreach my $level ( $min_level .. $max_level )
	{
	    foreach my $interval_key ( keys $n_imprecise_time{$env}{$level}->%* )
	    {
		my $occs = $n_imprecise_time{$env}{$level}{$interval_key};
		my $bins = $interval_overlap{$interval_key}{$level};

		# If there aren't any overlapping intervals recorded for this age range, note the
		# error.
		
		unless ( @$bins )
		{
		    $missing_overlap{$interval_key}++;
		    next;
		}
		
		# Otherwise, distribute the imprecise occurrence count among the intervals as
		# evenly as possible until we run out.
		
		my $fraction = int($occs/@$bins) || 1;
		
		foreach my $bin_index ( @$bins )
		{
		    my $interval_no = $interval_list{$level}[$bin_index];
		    
		    if ( $bin_index == $bins->[-1] || $occs < $fraction )
		    {
			$n_imprecise_distributed{$env}{$interval_no} += $occs;
			$occs = 0;
		    }

		    else
		    {
			$n_imprecise_distributed{$env}{$interval_no} += $fraction;
			$occs -= $fraction;
		    }
		}
	    }
	}
    }
    
    # Our goal is to generate the usual diversity output including the four diversity statistics
    # defined by Foote: n_origin = XFt, n_single = XFL, n_extinct = XbL, n_range = Xbt. In
    # addition, the fields x_single and x_extinct provide corrections for the middle two
    # statistics for taxa that are known to be extant.
    
    # Start by creating a statement to insert the statistics for one environment/interval/rank
    # combination into the database.
    
    $sql = "INSERT INTO $DIV_ADVANCED_WORK (slice, timescale_no, interval_no, rank, 
	n_itime, n_itaxon, n_occs, n_taxa, n_implied,
	n_origin, n_range, n_extinct, n_single, x_extinct, x_single)
	values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    my $insert_sth = $dbh->prepare($sql);
    
    # Then run through all combinations of environment/interval-type/rank. For each such
    # combination, run through all the taxa we have counted and accumulate the Foote statistics
    # for the corresponding intervals of that type. When we have done this, insert a row into the
    # advanced diversity table for each of these intervals.
    
    my (%missing_taxa, $n_rows);
    
    foreach my $env ( keys %n_occs )
    {
	logMessage(2, "      counting diversity for environment '$env'...");
	
	foreach my $level ( $min_level .. $max_level )
	{
	    logMessage(2, "          by $level_label{$level}");
	    
	    foreach my $rank ( @counted_ranks )
	    {
		# The following variables accumulate the Foote statistics and corrections for each
		# interval at the current level (interval type).
		
		my (%n_origin, %n_range, %n_extinct, %n_single, %x_extinct, %x_single);
		
		# Check the first and last appearance of each counted taxon, and increment the
		# statistics for the indicated range of intervals.
		
		foreach my $taxon_no ( keys $taxon_firstbin{$env}{$level}{$rank}->%* )
		{
		    my $first_index = $taxon_firstbin{$env}{$level}{$rank}{$taxon_no};
		    my $last_index = $taxon_lastbin{$env}{$level}{$rank}{$taxon_no};
		    
		    unless ( defined $first_index && defined $last_index )
		    {
			$missing_taxa{$taxon_no} = 1;
			next;
		    }
		    
		    my $first_bin = $interval_list{$level}[$first_index];
		    my $last_bin = $interval_list{$level}[$last_index];
		    
		    # If the interval of first appearance is the same as the interval of last
		    # appearance, then this is a singleton. If the taxon is extant, include it in
		    # the x_single correction statistic because that means it isn't really a singleton.
		    
		    if ( $first_index == $last_index )
		    {
			$n_single{$first_bin}++;
			$x_single{$first_bin}++ if $extant_taxon{$taxon_no};
		    }
		    
		    # Otherwise, we count the bin where the taxon starts and the bin where it
		    # ends, and rangethroughs in the bins between. If the taxon is extant, include
		    # it in the x_extinct correction statistic for the ending bin because that
		    # isn't really the last appearance of this taxon.
		    
		    else
		    {
			$n_origin{$first_bin}++;
			$n_range{$_}++ foreach $interval_list{$level}->@[$first_index+1..$last_index-1];
			$n_extinct{$last_bin}++;
			$x_extinct{$last_bin}++ if $extant_taxon{$taxon_no};
		    }
		}
		
		# Then run through all of the intervals at this level from start to end. For each
		# interval, count the unique taxa (both sampled and implied) for this
		# environment/interval/rank combination and then add a row into the advanced
		# diversity table. If any errors occur during insertion, catch them and print them
		# instead of halting.
		
		foreach my $interval_no ( $interval_list{$level}->@* )
		{
		    my $n_taxa = scalar keys $sampled_in_bin{$env}{$interval_no}{$rank}->%*;
		    my $n_implied = scalar grep { $_ == 1 }
					       values $implied_in_bin{$env}{$interval_no}{$rank}->%*;

		    my $n_itime = $n_imprecise_distributed{$env}{$interval_no} // 0;
		    my $n_itaxon = $n_imprecise_rank{$env}{$interval_no}{$rank} // 0;
		    my $n_occs = $n_occs{$env}{$interval_no}{$rank} // 0;

		    eval {
			$insert_sth->execute($env, $level, $interval_no, $rank,
					     $n_itime, $n_itaxon, $n_occs, $n_taxa, $n_implied,
					     $n_origin{$interval_no} // 0, $n_range{$interval_no} // 0,
					     $n_extinct{$interval_no} // 0, $n_single{$interval_no} // 0,
					     $x_extinct{$interval_no} // 0, $x_single{$interval_no} // 0);
			$n_rows++;
		    };
		    
		    if ( $@ )
		    {
			print $@;
		    }
		}
	    }
	}

	# Then add a separate row giving the total number of occurrences processed for this slice.

	my $quoted_env = $dbh->quote($env);
	my $quoted_occs = $dbh->quote($n_occs{$env}{all});
	
	$sql = "INSERT INTO $DIV_ADVANCED_WORK (slice, timescale_no, interval_no, rank, n_occs)
		VALUES ($quoted_env, 0, 0, 0, $quoted_occs)";
	
	doStmt($dbh, $sql, $options->{debug});
    }
    
    # Report the number of occurrences processed, the number of table rows inserted, and any
    # errors we found.
    
    logMessage(2, "    processed $n_occs{global}{all} occurrences");
    logMessage(2, "       $n_occs{$_}{all} for $_") foreach grep { $_ ne 'global' } keys %n_occs;
    logMessage(2, "    inserted $n_rows rows into the advanced diversity table");
    
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


