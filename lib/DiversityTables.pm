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

use CoreFunction qw(activateTables);
use ConsoleLog qw(initMessages logMessage);
use TableDefs qw($COLL_MATRIX $COLL_BINS $COLL_INTS $BIN_KEY $OCC_MATRIX $OCC_TAXON
		 $DIV_MATRIX $DIV_GLOBAL $PVL_SUMMARY $PVL_GLOBAL
		 $OCC_BUFFER_MAP $OCC_MAJOR_MAP
		 $INTERVAL_DATA $INTERVAL_BUFFER $SCALE_LEVEL_DATA $SCALE_DATA $SCALE_MAP $INTERVAL_MAP);

use base 'Exporter';

our (@EXPORT_OK) = qw(buildDiversityTables $DIV_SAMPLED);

our $PVL_SUMMARY_WORK = 'pvn';
our $PVL_GLOBAL_WORK = 'pgn';

our $DIV_AUX = 'div_aux';
our $TAXA_AUX = 'taxa_aux';
our $COLL_AUX = 'coll_aux';

our $DIV_SAMPLED_WORK = 'divsn';
our $DIV_SAMPLED = 'div_sampled';

# buildDiversityTablesNew ( dbh )
# 
# A new approach to building the diversity tables, using hashing to
# approximate the count of unique genera across geographical bins.

# sub buildDiversityTablesNew {

#     my ($dbh, $tree_table, $options) = @_;
    
#     $options ||= {};
    
#     my ($sql, $result);
    
#     my $TREE_TABLE = $tree_table;
#     my $INTS_TABLE = $TAXON_TABLE{$tree_table}{ints};
#     my $LOWER_TABLE = $TAXON_TABLE{$tree_table}{lower};
    
#     logMessage(1, "Building diversity tables");
    
#     # First rebuild the diversity digest table unless we are requested not to.
    
#     unless ( $options->{no_rebuild_div} )
#     {
# 	buildOccurrenceDigest($dbh, $tree_table);
#     }
    
#     # Then create the diversity matrix.
    
#     $dbh->do("DROP TABLE IF EXISTS $DIV_MATRIX");
    
#     $dbh->do("CREATE TABLE $DIV_MATRIX (
# 		bin_id int unsigned not null,
# 		major_no int unsigned not null,
# 		interval_no int unsigned not null,
# 		gmap0 bigint unsigned not null,
# 		n_genera int unsigned not null,
# 		n_occs int unsigned not null,
# 		n_colls int unsigned not null) Engine=MyISAM");
    
#     # Then fill it in.
    
#     logMessage(2, "      counting genera globally for all life over time...");
    
#     $sql = "INSERT INTO $DIV_SAMPLED_WORK (bin_id, major_no, interval_no, n_genera, n_occs)
# 		SELECT 0, 0, interval_no, count(distinct genus_no), sum(n_occs)
# 		FROM $DIV_AUX
# 		WHERE interval_no > 0
# 		GROUP BY interval_no";
    
#     $result = $dbh->do($sql);
    
#     logMessage(2, "        generated $result rows");
    
#     logMessage(2, "      counting genera by geographic cluster for all life over time...");
    
#     $sql = "INSERT INTO $DIV_SAMPLED_WORK (bin_id, major_no, interval_no, n_genera, n_occs)
# 		SELECT SQL_NO_CACHE bin_id, 0, interval_no, count(distinct genus_no), sum(n_occs)
# 		FROM $DIV_AUX
# 		WHERE bin_id > 0 and interval_no > 0
# 		GROUP BY bin_id, interval_no";
    
#     $result = $dbh->do($sql);
    
#     logMessage(2, "        generated $result rows");
    
#     logMessage(2, "      estimating distinct genera by geographic cluster for all life over time...");
    
#     $sql = "INSERT INTO $DIV_SAMPLED_WORK (bin_id, major_no, interval_no, n_genera, n_occs)
# 		SELECT SQL_NO_CACHE bin_id, 0, interval_no, 999999, 0
# 		FROM $DIV_AUX
# 		WHERE bin_id > 0 and interval_no > 0 and genus_no > 0
# 		ON DUPLIATE KEY UPDATE gmap0 = gmap0 | (1 << ($DIV_AUX % 64))";
    
#     $result = $dbh->do($sql);
#     $result /= 2;
    
#     logMessage(2, "        updated $result rows");
# }


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
    my $INTS_TABLE = $TAXON_TABLE{$tree_table}{ints};
    my $ATTRS_TABLE = $TAXON_TABLE{$tree_table}{attrs};
    my $LOWER_TABLE = $TAXON_TABLE{$tree_table}{lower};
    
    logMessage(1, "Building diversity tables");
    
    # First create an auxiliary table that maps each ints_no value to all of
    # the major_no values.
    
    # logMessage(2, "    matching major taxa...");
    
    # $dbh->do("DROP TABLE IF EXISTS $TAXA_AUX");
    
    # $dbh->do("CREATE TABLE $TAXA_AUX (
    # 		major_no int unsigned not null,
    # 		ints_no int unsigned not null,
    # 		PRIMARY KEY (ints_no, major_no)) Engine=MyISAM");
    
    # $sql = "    INSERT IGNORE INTO $TAXA_AUX
    # 		SELECT major_no, ints_no FROM $INTS_TABLE";
    
    # $result = $dbh->do($sql);
    
    # $sql = "    SELECT max(depth)
    # 		FROM $INTS_TABLE JOIN $TREE_TABLE using (taxon_ints)
    # 		WHERE major_no > 0 and major_no <> ints_no";
    
    # my ($max_depth) = $dbh->selectrow_array($sql);
    
    # while ( $result )
    # {
    # 	$sql = "INSERT IGNORE INTO $TAXA_AUX
    # 		SELECT p.major_no, c.ints_no
    # 		FROM $INTS_TABLE as c JOIN $INTS_TABLE as p on c.major_no = c.ints_no";
	
    # 	$result = $dbh->do($sql);
	
    # 	$iterations++;
    # }
    
    # logMessage(2, "      did $iterations iterations");
    
    # Then create an auxiliary table for keeping track of the distinct taxa
    # found in each interval.  We then scan through the entire set of
    # occurrences and fill in this table.  We use the interval buffer map
    # created above to make sure that each occurrence is counted in every
    # interval that it falls into under the "buffer rule".
    
    logMessage(2, "    generating diversity table by geographic cluster, interval, and taxonomy...");
    
    $dbh->do("DROP TABLE IF EXISTS $DIV_MATRIX");

    $dbh->do("CREATE TABLE $DIV_MATRIX (
		bin_id int unsigned not null,
		interval_no int unsigned not null,
		ints_no int unsigned not null,
		genus_no int unsigned not null,
		n_occs int unsigned not null,
		not_trace tinyint unsigned not null,
		PRIMARY KEY (bin_id, interval_no, ints_no, genus_no)) Engine=MyISAM");
    
    $sql = "INSERT INTO $DIV_MATRIX (bin_id, interval_no, ints_no, genus_no, n_occs, not_trace)
		SELECT SQL_NO_CACHE c.bin_id_3, m.interval_no, ta.ints_no, pl.genus_no, 1, not_trace
		FROM occ_matrix as o JOIN $OCC_BUFFER_MAP as m using (early_age, late_age)
			JOIN coll_matrix as c using (collection_no)
			JOIN $TREE_TABLE as t using (orig_no)
			JOIN $TREE_TABLE as ta on ta.orig_no = t.accepted_no
			JOIN $ATTRS_TABLE as v on v.orig_no = t.accepted_no
			LEFT JOIN $LOWER_TABLE as pl on pl.orig_no = t.accepted_no
		WHERE latest_ident and c.access_level = 0
		ON DUPLICATE KEY UPDATE $DIV_MATRIX.n_occs = $DIV_MATRIX.n_occs + 1";

    $result = $dbh->do($sql);

    logMessage(2, "      generated $result rows");
    
    logMessage(2, "    generating diversity table worldwide by interval and taxonomy...");
    
    $dbh->do("DROP TABLE IF EXISTS $DIV_GLOBAL");

    $dbh->do("CREATE TABLE $DIV_GLOBAL (
		interval_no int unsigned not null,
		ints_no int unsigned not null,
		genus_no int unsigned not null,
		n_occs int unsigned not null,
		not_trace tinyint unsigned not null,
		PRIMARY KEY (interval_no, ints_no, genus_no)) Engine=MyISAM");
    
    $sql = "INSERT INTO $DIV_GLOBAL (interval_no, ints_no, genus_no, n_occs, not_trace)
		SELECT SQL_NO_CACHE m.interval_no, ta.ints_no, pl.genus_no, 1, not_trace
		FROM occ_matrix as o JOIN $OCC_BUFFER_MAP as m using (early_age, late_age)
			JOIN coll_matrix as c using (collection_no)
			JOIN $TREE_TABLE as t using (orig_no)
			JOIN $TREE_TABLE as ta on ta.orig_no = t.accepted_no
			JOIN $ATTRS_TABLE as v on v.orig_no = t.accepted_no
			LEFT JOIN $LOWER_TABLE as pl on pl.orig_no = t.accepted_no
		WHERE latest_ident and c.access_level = 0
		ON DUPLICATE KEY UPDATE $DIV_GLOBAL.n_occs = $DIV_GLOBAL.n_occs + 1";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      generated $result rows");
    
    logMessage(2, "    indexing tables...");
    
    $dbh->do("ALTER TABLE $DIV_MATRIX ADD KEY (ints_no)");
    $dbh->do("ALTER TABLE $DIV_MATRIX ADD KEY (bin_id)");
    $dbh->do("ALTER TABLE $DIV_GLOBAL ADD KEY (ints_no)");
    
    # logMessage(2, "    computing global diversity table...");
    
    # $dbh->do("DROP TABLE IF EXISTS $DIV_SAMPLED_WORK");
    
    # $dbh->do("CREATE TABLE $DIV_SAMPLED_WORK (
    # 		bin_id int unsigned not null,
    # 		major_no int unsigned not null,
    # 		interval_no int unsigned not null,
    # 		n_orders int unsigned not null,
    # 		n_families int unsigned not null,
    # 		n_genera int unsigned not null,
    # 		n_occs int unsigned not null,
    # 		n_colls int unsigned not null) Engine=MyISAM");
    
    # logMessage(2, "      counting genera globally for all life over time...");
    
    # $sql = "INSERT INTO $DIV_SAMPLED_WORK (bin_id, major_no, interval_no, n_genera, n_occs)
    # 		SELECT 0, 0, interval_no, count(distinct genus_no), sum(n_occs)
    # 		FROM $DIV_AUX
    # 		WHERE interval_no > 0
    # 		GROUP BY interval_no";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      generated $result rows");
    
    # logMessage(2, "      counting genera by geographic cluster for all life over time...");
    
    # $sql = "INSERT INTO $DIV_SAMPLED_WORK (bin_id, major_no, interval_no, n_genera, n_occs)
    # 		SELECT SQL_NO_CACHE bin_id, 0, interval_no, count(distinct genus_no), sum(n_occs)
    # 		FROM $DIV_AUX
    # 		WHERE bin_id > 0 and interval_no > 0
    # 		GROUP BY bin_id, interval_no";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      generated $result rows");
    
    # logMessage(2, "      counting genera globally by major taxa over time...");
    
    # $sql = "INSERT INTO $DIV_SAMPLED_WORK (bin_id, major_no, interval_no, n_genera, n_occs)
    # 		SELECT SQL_NO_CACHE 0, major_no, interval_no, count(distinct genus_no), sum(n_occs)
    # 		FROM $DIV_AUX JOIN $TAXA_AUX using (ints_no)
    # 		WHERE major_no > 0 and interval_no > 0
    # 		GROUP BY major_no, interval_no";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      generated $result rows");
    
    # logMessage(2, "      counting genera by geographic cluster by major taxa...");
    
    # $sql = "INSERT INTO $DIV_SAMPLED_WORK (bin_id, major_no, interval_no, n_genera, n_occs)
    # 		SELECT SQL_NO_CACHE bin_id, major_no, 0, count(distinct genus_no), sum(n_occs) as n_occs
    # 		FROM $DIV_AUX JOIN $TAXA_AUX using (ints_no) JOIN $SCALE_MAP using (interval_no)
    # 		WHERE bin_id > 0 and major_no > 0 and scale_no = 1 and level = 1
    # 		GROUP BY bin_id, major_no";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      generated $result rows");
    
    # logMessage(2, "      counting genera by geographic cluster, major taxa, and interval...");
    
    # $sql = "INSERT INTO $DIV_SAMPLED_WORK (bin_id, major_no, interval_no, n_genera, n_occs)
    # 		SELECT bin_id, major_no, interval_no, count(distinct genus_no) as n_genera, sum(n_occs) as n_occs
    # 		FROM $DIV_AUX JOIN $TAXA_AUX using (ints_no)
    # 		WHERE major_no > 0 and bin_id > 0 and interval_no > 0
    # 		GROUP BY bin_id, major_no, interval_no";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      generated $result lines");
    
    # logMessage(2, "    indexing table...");
    
    # $dbh->do("ALTER TABLE $DIV_SAMPLED_WORK ADD PRIMARY KEY (bin_id, major_no, interval_no)");
    
    # logMessage(2, "    DONE FOR NOW");
    # return;
    
    # logMessage(2, "      counting families...");
    
    # $sql = "INSERT INTO $DIV_SAMPLED_WORK (interval_no, timerule, n_families)
    # 		SELECT interval_no, 1, count(distinct family_no) as n_families
    # 		FROM $DIV_AUX
    # 		GROUP BY interval_no
    # 	    ON DUPLICATE KEY UPDATE n_families = values(n_families)";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      counting orders...");
    
    # $sql = "INSERT INTO $DIV_SAMPLED_WORK (interval_no, timerule, n_orders)
    # 		SELECT interval_no, 1, count(distinct order_no) as n_orders
    # 		FROM $DIV_AUX
    # 		GROUP BY interval_no
    # 	    ON DUPLICATE KEY UPDATE n_orders = values(n_orders)";
    
    # $result = $dbh->do($sql);
    
    # Start with sampled diversity on the standard set of epochs and ages.  We
    # need to first create auxiliary tables to count up the species, genera
    # and families.
    
    # First create an auxiliary table for use below.
    
    # $dbh->do("DROP TABLE IF EXISTS $DIV_AUX");
    
    # $dbh->do("
    # 	CREATE TABLE $DIV_AUX (
    # 		interval_no int unsigned not null,
    # 		higher_no int unsigned not null,
    # 		PRIMARY KEY (interval_no, higher_no))");
    
    # logMessage(1, "Building diversity tables");

    # unless ( $skip_raw )
    # {
    # 	logMessage(2, "    tabulating occurrences by interval...");
	
    # 	$dbh->do("DROP TABLE IF EXISTS $DIV_RAW_WORK");
	
    # 	$dbh->do("
    # 	CREATE TABLE $DIV_RAW_WORK (
    # 		interval_no int unsigned not null,
    # 		occurrence_no int unsigned not null,
    # 		species_no int unsigned not null,
    # 		genus_no int unsigned not null,
    # 		ints_no int unsigned not null,
    # 		prob float,
    # 		early_prob float,
    # 		late_prob float)");
	
    # 	$result = $dbh->do("
    # 	INSERT INTO $DIV_RAW_WORK
    # 	SELECT i.interval_no, m.occurrence_no,
    # 	       t.species_no, t.genus_no, t.ints_no,
    # 	       -- sample prob
    # 	       if(m.early_age <= i.early_age and m.late_age >= i.late_age, 1.0,
    # 	       if(m.early_age <= i.early_age, (m.early_age-i.late_age)/(m.early_age-m.late_age),
    # 	       if(m.late_age >= i.late_age, (i.early_age-m.late_age)/(m.early_age-m.late_age),
    # 					  (i.early_age-i.late_age)/(m.early_age-m.late_age)))) as prob,
    # 	       -- early prob
    # 	       if(m.late_age >= i.late_age, 1.0, 
    # 			(m.early_age-i.late_age)/(m.early_age-m.late_age)) as early_prob,
    # 	       -- late prob
    # 	       if(m.early_age <= i.early_age, 1.0,
    # 			(i.early_age-m.late_age)/(m.early_age-m.late_age))
    # 	FROM occ_matrix as m JOIN $INTERVAL_DATA as i on m.late_age < i.early_age and m.early_age > i.late_age
    # 		JOIN $SCALE_MAP as sm using (interval_no)
    # 		JOIN $SCALE_LEVEL_DATA as s using (scale_no, level)
    # 		JOIN $TREE_TABLE as t using (orig_no)
    # 	WHERE t.ints_no > 0 and s.sample");
	
    # 	logMessage(2, "    found $result rows.");
	
    # 	logMessage(2, "    indexing by interval_no...");
	
    # 	$dbh->do("ALTER TABLE $DIV_RAW_WORK ADD INDEX (interval_no)");
    # }
    
    # logMessage(2, "    summarizing occurrences by interval for sampled statistics...");
    
    # $dbh->do("DROP TABLE IF EXISTS $DIV_SAMPLED_WORK");
    
    # $dbh->do("
    # 	CREATE TABLE $DIV_SAMPLED_WORK (
    # 		interval_no int unsigned not null,
    # 		orig_no int unsigned not null,
    # 		lft int unsigned not null,
    # 		rank tinyint unsigned not null,
    # 		occurrence_no int unsigned not null,
    # 		prob float,
    # 		PRIMARY KEY (interval_no, orig_no))");
    
    # $result = $dbh->do("
    # 	INSERT INTO $DIV_SAMPLED_WORK (interval_no, orig_no, rank, occurrence_no, prob)
    # 	SELECT interval_no, ints_no, $TAXON_RANK{unranked}, coalesce(occurrence_no),
    # 	       if(max(prob) = 1.0, 1.0, 1.0 - exp(sum(log(1.0-prob)))) as prob
    # 	FROM $DIV_RAW_WORK
    # 	WHERE ints_no > 0
    # 	GROUP BY interval_no, ints_no
    # 	ORDER BY NULL");
    
    # logMessage(2, "      found $result identifications of higher taxa in specific intervals.");
    
    # $result = $dbh->do("
    # 	INSERT INTO $DIV_SAMPLED_WORK (interval_no, orig_no, rank, occurrence_no, prob)
    # 	SELECT interval_no, genus_no, $TAXON_RANK{genus}, coalesce(occurrence_no),
    # 	       if(max(prob) = 1.0, 1.0, 1.0 - exp(sum(log(1.0-prob)))) as prob
    # 	FROM $DIV_RAW_WORK
    # 	WHERE genus_no > 0
    # 	GROUP BY interval_no, genus_no
    # 	ORDER BY NULL");
    
    # logMessage(2, "      found $result identifications of genera in specific intervals.");
    
    # $result = $dbh->do("
    # 	INSERT INTO $DIV_SAMPLED_WORK (interval_no, orig_no, rank, occurrence_no, prob)
    # 	SELECT interval_no, species_no, $TAXON_RANK{species}, coalesce(occurrence_no),
    # 	       if(max(prob) = 1.0, 1.0, 1.0 - exp(sum(log(1.0-prob)))) as prob
    # 	FROM $DIV_RAW_WORK
    # 	WHERE species_no > 0
    # 	GROUP BY interval_no, species_no
    # 	ORDER BY NULL");
    
    # logMessage(2, "      found $result identifications of species in specific intervals.");
    
    # logMessage(2, "    setting tree sequence numbers...");
    
    # $result = $dbh->do("
    # 	UPDATE $DIV_SAMPLED_WORK as d JOIN $TREE_TABLE as t using (orig_no)
    # 	SET d.lft = t.lft");
    
    # # Now range-through.
    
    # logMessage(2, "    summarizing occurrences by interval for range-through statistics...");
    
    # $dbh->do("DROP TABLE IF EXISTS $DIV_RANGE_WORK");
    
    # $dbh->do("
    # 	CREATE TABLE $DIV_RANGE_WORK (
    # 		scale_no smallint unsigned not null,
    # 		level smallint unsigned not null,
    # 		orig_no int unsigned not null,
    # 		lft int unsigned not null,
    # 		rank tinyint unsigned not null,
    # 		early_age decimal(9,5),
    # 		late_age decimal(9,5),
    # 		early_age_08 decimal(9,5),
    # 		late_age_08 decimal(9,5),
    # 		early_age_05 decimal(9,5),
    # 		late_age_05 decimal(9,5),
    # 		-- early_prob_age decimal(9,5),
    # 		-- min_prob float,
    # 		-- late_prob_age decimal(9,5),
    # 		-- max_prob float,
    # 		early_occ_no int unsigned not null,
    # 		late_occ_no int unsigned not null,
    # 		PRIMARY KEY (scale_no, level, orig_no))");
    
    # $result = $dbh->do("
    # 	INSERT INTO $DIV_RANGE_WORK (scale_no, level, orig_no, rank,
    # 				     early_age, late_age,
    # 				     early_age_08, late_age_08,
    # 				     early_age_05, late_age_05,
    # 				     early_occ_no, late_occ_no)
    # 	SELECT sm.scale_no, sm.level, genus_no, $TAXON_RANK{genus},
    # 	       if(prob >= 1.0, i.early_age, null), if(early_prob >= 1.0, i.late_age, null),
    # 	       if(prob >= 0.8, i.early_age, null), if(early_prob >= 0.8, i.late_age, null),
    # 	       i.early_age, i.late_age,
    # 	       occurrence_no, occurrence_no
    # 	FROM $DIV_RAW_WORK as w JOIN $INTERVAL_DATA as i using (interval_no) JOIN $SCALE_DATA as sm using (interval_no)
    # 	WHERE genus_no > 0 and (prob >= 0.5 or early_prob >= 0.8 or late_prob >= 0.5)
    # 	ORDER BY i.early_age desc
    # 	ON DUPLICATE KEY UPDATE
    # 		early_age = if(w.early_prob >= 1.0 and early_age is null, i.early_age, early_age),
    # 		late_age = if(w.late_prob >= 1.0, i.late_age, late_age),
    # 		early_age_08 = if(w.early_prob >= 0.8 and early_age_08 is null, i.early_age, early_age_08),
    # 		late_age_08 = if(w.late_prob >= 0.8, i.late_age, late_age_08),
    # 		late_age_05 = if(w.late_prob >= 0.5, i.late_age, late_age_05),
    # 		late_occ_no = w.occurrence_no");
    
    # logMessage(2, "    found $result rows.");
    
    # Then create the prevalence summary table.
    logMessage(2, "    DONE FOR NOW");
    return;
    logMessage(2, "    creating taxonomic prevalence summary table...");
    
    $dbh->do("DROP TABLE IF EXISTS $PVL_SUMMARY_WORK");
    
    $dbh->do("CREATE TABLE $PVL_SUMMARY_WORK (
		bin_id int unsigned not null,
		interval_no int unsigned not null,
		order_no int unsigned,
		class_no int unsigned,
		phylum_no int unsigned,
		n_occs int unsigned not null) Engine=MyISAM");
    
    # Fill in this table from the occurrences.  Start with orders, then fill
    # in classes, then phyla.
    
    logMessage(2, "      adding global rows...");
    
    $sql = "
	INSERT INTO $PVL_SUMMARY_WORK (bin_id, interval_no, order_no, class_no, phylum_no, n_occs)
	SELECT 0, interval_no, order_no, class_no, phylum_no, count(*)
      	FROM $OCC_MATRIX as o JOIN $COLL_INTS as c using (collection_no)
    		JOIN $TREE_TABLE as t using (orig_no)
    		JOIN $INTS_TABLE as ph using (ints_no)
    	WHERE o.latest_ident = 1 and (order_no <> 0 or class_no <> 0 or phylum_no <> 0)
    	GROUP BY interval_no, order_no, class_no, phylum_no";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      generated $result rows for worldwide occurrence.");
    
    # Now add rows for each separate bin, at the maximum binning level.  If
    # this cannot be determined, assume it is 1.
    
    my ($level) = $dbh->selectrow_array("
		SELECT max(bin_id) FROM $COLL_BINS WHERE interval_no = $BIN_KEY");
    
    $level ||= 1;
    
    logMessage(2, "      adding rows for geographic bins...");
    
    $sql = "
    	INSERT INTO $PVL_SUMMARY_WORK (bin_id, interval_no, order_no, class_no, phylum_no, n_occs)
    	SELECT bin_id_$level, interval_no, order_no, class_no, phylum_no, count(*)
    	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c using (collection_no)
		JOIN $COLL_INTS as i using (collection_no)
    		JOIN $TREE_TABLE as t using (orig_no)
    		JOIN $INTS_TABLE as ph using (ints_no)
    	WHERE o.latest_ident = 1 and bin_id_$level > 0 
		and (order_no <> 0 or class_no <> 0 or phylum_no <> 0)
    	GROUP BY bin_id_$level, interval_no, order_no, class_no, phylum_no";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      generated $result rows for geographic bins.");
    
    # Now sum over the entire table ignoring intervals to get counts over all
    # time.  These are stored with interval_no = 0.
    
    $sql = "
	INSERT INTO $PVL_SUMMARY_WORK (bin_id, interval_no, order_no, class_no, phylum_no, n_occs)
	SELECT bin_id, 0, order_no, class_no, phylum_no, sum(n_occs)
	FROM $PVL_SUMMARY_WORK
	GROUP BY bin_id, order_no, class_no, phylum_no";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      generated $result rows for all time");
    
    logMessage(2, "    indexing table...");
    
    $dbh->do("ALTER TABLE $PVL_SUMMARY_WORK ADD KEY (bin_id, interval_no)");
    
    # Finally, we swap in the new tables for the old ones.
    
    activateTables($dbh, $PVL_SUMMARY_WORK => $PVL_SUMMARY);
    
    my $a = 1;		# We can stop here when debugging.
}


sub buildOccurrenceDigest {
    
    my ($dbh, $tree_table) = @_;
    
    my ($sql, $result);
    
    my $TREE_TABLE = $tree_table;
    my $INTS_TABLE = $TAXON_TABLE{$tree_table}{ints};
    my $LOWER_TABLE = $TAXON_TABLE{$tree_table}{lower};
    
    logMessage(2, "    generating occurrence digest by geographic cluster, interval, and taxonomy...");
    
    $dbh->do("DROP TABLE IF EXISTS $DIV_AUX");

    $dbh->do("CREATE TABLE $DIV_AUX (
		bin_id int unsigned not null,
		interval_no int unsigned not null,
		ints_no int unsigned not null,
		genus_no int unsigned not null,
		n_occs int unsigned not null,
		PRIMARY KEY (bin_id, interval_no, ints_no, genus_no),
		KEY (ints_no)) Engine=MyISAM");

    $sql = "INSERT INTO $DIV_AUX (bin_id, interval_no, ints_no, genus_no, n_occs)
		SELECT SQL_NO_CACHE c.bin_id_3, m.interval_no, ta.ints_no, pl.genus_no, 1
		FROM occ_matrix as o JOIN $OCC_BUFFER_MAP as m using (early_age, late_age)
			JOIN coll_matrix as c using (collection_no)
			JOIN $TREE_TABLE as t using (orig_no)
			JOIN $TREE_TABLE as ta on ta.orig_no = t.accepted_no
			LEFT JOIN $LOWER_TABLE as pl on pl.orig_no = t.accepted_no
		WHERE latest_ident
		ON DUPLICATE KEY UPDATE $DIV_AUX.n_occs = $DIV_AUX.n_occs + 1";

    $result = $dbh->do($sql);

    logMessage(2, "      generated $result rows");
}
