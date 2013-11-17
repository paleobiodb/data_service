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
use CollectionTables qw($COLL_MATRIX $COUNTRY_MAP);
use OccurrenceTables qw($OCC_MATRIX $OCC_TAXON);
use IntervalTables qw($INTERVAL_DATA $INTERVAL_MAP $TEN_MY_BINS);

use base 'Exporter';

our (@EXPORT_OK) = qw(buildDiversityTables $DIV_SAMPLED_STD $DIV_SAMPLED_10);

our $DIV_INT_RAW = 'div_int_raw';
our $DIV_10_RAW = 'div_10_raw';

our $DIV_INT_MATRIX = 'div_int_matrix';
our $DIV_10_MATRIX = 'div_10_matrix';

our $DIV_SAMPLED = 'div_sampled';
our $DIV_SAMPLED_10MY = 'div_sampled_10';

our $DIV_INT_RAW_WORK = 'dirn';
our $DIV_10_RAW_WORK = 'dtrn';

our $DIV_INT_MATRIX_WORK = 'dimn';
our $DIV_10_MATRIX_WORK = 'dtmn';

our $DIV_SAMPLED_WORK = 'dssn';

our $DIV_AUX = 'div_aux';
our $SPECIES_AUX = 'species_aux';
our $GENUS_AUX = 'genus_aux';
our $FAMILY_AUX = 'family_aux';

my $TREE_TABLE = $TREE_TABLE_LIST[0];
my $INTS_TABLE = $TAXON_TABLE{$TREE_TABLE}{ints};

# buildDiversityTables ( dbh )
# 
# Build the taxonomic diversity tables.

sub buildDiversityTables {

    my ($dbh, $tree_table) = @_;
    
    my ($sql, $result);
    
    my $TREE_TABLE = $tree_table;
    my $INTS_TABLE = $TAXON_TABLE{$tree_table}{ints};
    
    # Start with sampled diversity on the standard set of epochs and ages.  We
    # need to first create auxiliary tables to count up the species, genera
    # and families.
    
    # First create an auxiliary table for use below.
    
    $dbh->do("DROP TABLE IF EXISTS $DIV_AUX");
    
    $dbh->do("
	CREATE TABLE $DIV_AUX (
		interval_no int unsigned not null,
		higher_no int unsigned not null,
		PRIMARY KEY (interval_no, higher_no))");
    
    logMessage(1, "Building diversity tables");
    
    logMessage(2, "    tabulating occurrences by interval...");
    
    $dbh->do("DROP TABLE IF EXISTS $DIV_INT_RAW_WORK");
    
    $dbh->do("
	CREATE TABLE $DIV_INT_RAW_WORK (
		interval_no int unsigned not null,
		occurrence_no int unsigned not null,
		species_no int unsigned not null,
		genus_no int unsigned not null,
		family_no int unsigned not null,
		ints_no int unsigned not null,
		prob float)");
    
    $result = $dbh->do("
	INSERT INTO $DIV_INT_RAW_WORK
	SELECT i.interval_no, m.occurrence_no,
	       t.species_no, t.genus_no, g.family_no, t.ints_no,
	       if(m.base_age <= i.base_age and m.top_age >= i.top_age, 1.0,
	       if(m.base_age <= i.base_age, (m.base_age-i.top_age)/(m.base_age-m.top_age),
	       if(m.top_age >= i.top_age, (i.base_age-m.top_age)/(m.base_age-m.top_age),
					  (i.base_age-i.top_age)/(m.base_age-m.top_age)))) as prob
	FROM occ_matrix as m JOIN interval_data as i on m.top_age < i.base_age and m.base_age > i.top_age
		JOIN taxon_trees as t using (orig_no)
		JOIN taxon_ints as g using (ints_no)
	WHERE t.ints_no > 0 and i.scale_no > 0");
    
    logMessage(2, "    found $result rows.");
    
    logMessage(2, "    summarizing occurrences by interval...");
    
    $dbh->do("DROP TABLE IF EXISTS $DIV_INT_MATRIX_WORK");
    
    $dbh->do("
	CREATE TABLE $DIV_INT_MATRIX_WORK (
		interval_no int unsigned not null,
		orig_no int unsigned not null,
		higher_no int unsigned not null,
		rank tinyint unsigned not null,
		lft int unsigned not null,
		occurrence_no int unsigned not null,
		prob float,
		PRIMARY KEY (interval_no, orig_no, higher_no))");
    
    $result = $dbh->do("
	INSERT INTO $DIV_INT_MATRIX_WORK (interval_no, orig_no, higher_no, rank, occurrence_no, prob)
	SELECT interval_no, family_no, 0, $TAXON_RANK{family}, coalesce(occurrence_no),
	       if(max(prob) = 1.0, 1.0, 1.0 - exp(sum(log(1.0-prob)))) as prob
	FROM $DIV_INT_RAW_WORK
	WHERE family_no > 0
	GROUP BY interval_no, family_no
	ORDER BY NULL");
    
    logMessage(2, "      found $result occurrences of families.");
    
    $result = $dbh->do("
	INSERT INTO $DIV_INT_MATRIX_WORK (interval_no, orig_no, higher_no, rank, occurrence_no, prob)
	SELECT interval_no, genus_no, ints_no, $TAXON_RANK{genus}, coalesce(occurrence_no),
	       if(max(prob) = 1.0, 1.0, 1.0 - exp(sum(log(1.0-prob)))) as prob
	FROM $DIV_INT_RAW_WORK
	WHERE genus_no > 0
	GROUP BY interval_no, genus_no
	ORDER BY NULL");
    
    logMessage(2, "      found $result occurrences of genera.");
    
    $result = $dbh->do("DELETE FROM $DIV_AUX");
    
    $result = $dbh->do("
	INSERT IGNORE INTO $DIV_AUX
	SELECT interval_no, higher_no
	FROM $DIV_INT_MATRIX_WORK as d
	WHERE rank = $TAXON_RANK{genus}");
    
    $result = $dbh->do("
	INSERT INTO $DIV_INT_MATRIX_WORK (interval_no, orig_no, higher_no, rank, occurrence_no, prob)
	SELECT d.interval_no, 0, ints_no, $TAXON_RANK{genus}, coalesce(occurrence_no), 
	       if(max(prob) = 1.0, 1.0, 1.0 - exp(sum(log(1.0-prob)))) as prob
	FROM $DIV_INT_RAW_WORK as d
		LEFT JOIN $DIV_AUX as a on a.interval_no = d.interval_no and a.higher_no = d.ints_no
	WHERE a.higher_no is null
	GROUP BY d.interval_no, d.ints_no
	ORDER BY NULL");
    
    logMessage(2, "      found $result implied occurrences of genera.");
    
    $result = $dbh->do("
	INSERT INTO $DIV_INT_MATRIX_WORK (interval_no, orig_no, higher_no, rank, occurrence_no, prob)
	SELECT interval_no, species_no, genus_no, $TAXON_RANK{species}, coalesce(occurrence_no),
	       if(max(prob) = 1.0, 1.0, 1.0 - exp(sum(log(1.0-prob)))) as prob
	FROM $DIV_INT_RAW_WORK
	WHERE species_no > 0
	GROUP BY interval_no, species_no
	ORDER BY NULL");
    
    logMessage(2, "      found $result occurrences of species.");
    
    $result = $dbh->do("DELETE FROM $DIV_AUX");
    
    $result = $dbh->do("
	INSERT IGNORE INTO $DIV_AUX
	SELECT interval_no, higher_no
	FROM $DIV_INT_MATRIX_WORK as d
	WHERE rank = $TAXON_RANK{species}");
    
    $result = $dbh->do("
	INSERT INTO $DIV_INT_MATRIX_WORK (interval_no, orig_no, higher_no, rank, occurrence_no, prob)
	SELECT d.interval_no, 0, genus_no, $TAXON_RANK{species}, coalesce(occurrence_no), 
	       if(max(prob) = 1.0, 1.0, 1.0 - exp(sum(log(1.0-prob)))) as prob
	FROM $DIV_INT_RAW_WORK as d
		LEFT JOIN $DIV_AUX as a on a.interval_no = d.interval_no and a.higher_no = d.genus_no
	WHERE a.higher_no is null
	GROUP BY d.interval_no, d.genus_no
	ORDER BY NULL");
    
    logMessage(2, "      found $result implied occurrences of species.");
    
    logMessage(2, "    setting tree sequence numbers...");
    
    $result = $dbh->do("
	UPDATE $DIV_INT_MATRIX_WORK as d JOIN $TREE_TABLE as t using (orig_no)
	SET d.lft = t.lft");
    
    
    
    # logMessage(2, "    sampled diversity by interval");
    
    # $dbh->do("DROP TABLE IF EXISTS $DIV_INT_WORK");
    
    # $dbh->do("
    # 	CREATE TABLE $DIV_INT_WORK (
    # 		interval_no int unsigned not null,
    # 		ints_no int unsigned not null,
    # 		lft int unsigned not null,
    # 		n_species int unsigned not null,
    # 		n_sp_prob decimal(18,9),
    # 		n_genera int unsigned not null,
    # 		n_gn_prob decimal(18,9),
    # 		PRIMARY KEY (interval_no, ints_no)");
    
    # $result = $dbh->do("
    # 	INSERT INTO $DIV_INT_WORK (m.interval_no, m.ints_no, t.lft, n_genera)
    # 	SELECT interval_no, sum(distinct species_no), sum(distinct genus_no)
    # 	FROM $DIV_INT_MATRIX_WORK as m JOIN $TREE_TABLE as t on t.orig_no = m.ints_no
    # 	WHERE prob = 1.0 and genus_no > 0
    # 	GROUP BY interval_no
    # 	ORDER BY NULL");
    
    # # logMessage(2, "    indexing table...");
    
    # # $dbh->do("ALTER TABLE $DIV_INT_RAW_WORK ADD KEY (interval_no)");
    # # $dbh->do("ALTER TABLE $DIV_INT_RAW_WORK ADD KEY (interval_no)");
    # # $dbh->do("ALTER TABLE $DIV_INT_RAW_WORK ADD KEY (interval_no)");
    # # $dbh->do("ALTER TABLE $DIV_INT_RAW_WORK ADD KEY (interval_no)");
    
    # logMessage(2, "    tabulating occurrences by 10 My bin...");
    
    # $dbh->do("DROP TABLE IF EXISTS $DIV_10_RAW_WORK");
    
    # $dbh->do("CREATE TABLE $DIV_10_RAW_WORK (
    # 		bin_age int unsigned not null,
    # 		occurrence_no int unsigned not null,
    # 		species_no int unsigned not null,
    # 		genus_no int unsigned not null,
    # 		ints_no int unsigned not null,
    # 		coded_prob float)");
    
    # $result = $dbh->do("
    # 	INSERT INTO $DIV_10_RAW_WORK
    # 	SELECT i.base_age, m.occurrence_no,
    # 	       t.species_no, t.genus_no, t.ints_no,
    # 	       if(m.base_age <= i.base_age and m.top_age >= i.top_age, 1e9,
    # 	       if(m.base_age <= i.base_age, log((i.top_age-m.top_age)/(m.base_age-m.top_age)),
    # 	       if(m.top_age >= i.top_age, log((m.base_age-i.base_age)/(m.base_age-m.top_age)),
    # 	       log((m.base_age-i.base_age+i.top_age-m.top_age)/(m.base_age-m.top_age))))) as coded_prob
    # 	FROM occ_matrix as m JOIN ten_my_bins as i on m.top_age < i.base_age and m.base_age > i.top_age
    # 		JOIN taxon_trees as t using (orig_no)
    # 	WHERE t.ints_no > 0");
    
    # logMessage(2, "    found $result rows.");
    
    
    
    # logMessage(2, "    sampled, standard epochs and ages...");
    
    # $dbh->do("DROP TABLE IF EXISTS $SAMPLED_STD_WORK");
    
    # $dbh->do("CREATE TABLE $SAMPLED_STD_WORK (
    # 		orig_no int unsigned not null,
    # 		lft int unsigned not null,
    # 		interval_no int unsigned not null,
    # 		continent char(2),
    # 		n_species int unsigned not null,
    # 		n_genera int unsigned not null,
    # 		PRIMARY KEY (orig_no, interval_no, continent),
    # 		KEY (lft, interval_no))");
    
    # $sql = "	INSERT INTO $SAMPLED_STD_WORK (orig_no, interval_no, continent, int_frac, n_species, n_genera)
    # 		SELECT  t.ints_no, i.interval_no, cm.continent,
    # 			count(distinct species_no) as n_species,
    # 			count(distinct genus_no) as n_genera,
    # 		FROM $OCC_MATRIX as m JOIN $INTERVAL_DATA as i on m.top_age <= i.top_age and m.base_age >= i.base_age
    # 			JOIN $TREE_TABLE as t using (orig_no)
    # 			JOIN $COLL_MATRIX as c using (collection_no)
    # 			LEFT JOIN $COUNTRY_MAP as cm using (cc)
    # 		WHERE i.scale_no = 1 and i.level in (4, 5) and t.ints_no > 0
    # 		GROUP BY t.ints_no, i.interval_no, cm.continent";    
    
    # $result = $dbh->do($sql);
    
    # #		FROM $OCC_MATRIX as m JOIN $INTERVAL_DATA as i on m.base_age <= i.base_age and m.top_age >= i.top_age

    # logMessage(2, "      $result rows");
    
    # logMessage(2, "    indexing by tree sequence...");
    
    # $sql = "	UPDATE $SAMPLED_STD_WORK as w JOIN $TREE_TABLE as t using (orig_no)
    # 		SET w.lft = t.lft";
    
    # $result = $dbh->do($sql);
    
    # $sql = "    INSERT INTO $SAMPLED_STD_WORK (base_no, interval_no, n_species, n_genera, n_families)
    # 		SELECT cl.order_no, i.interval_no,
    # 			count(distinct species_no) as n_species,
    # 			count(distinct genus_no) as n_genera,
    # 			count(distinct family_no) as n_families
    # 		FROM $OCC_MATRIX as m JOIN $INTERVAL_DATA as i on m.base_age <= i.base_age and m.top_age >= i.top_age
    # 			JOIN $TREE_TABLE as t using (orig_no)
    # 			JOIN $INTS_TABLE as cl using (ints_no)
    # 		WHERE i.scale_no = 1 and i.level in (4, 5) and cl.order_no is not null
    # 		GROUP BY cl.order_no, i.interval_no";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      $result order/interval rows");
    
    # $sql = "    INSERT INTO $SAMPLED_STD_WORK (base_no, interval_no, n_species, n_genera, n_families)
    # 		SELECT cl.class_no, i.interval_no,
    # 			count(distinct species_no) as n_species,
    # 			count(distinct genus_no) as n_genera,
    # 			count(distinct family_no) as n_families
    # 		FROM $OCC_MATRIX as m JOIN $INTERVAL_DATA as i on m.base_age <= i.base_age and m.top_age >= i.top_age
    # 			JOIN $TREE_TABLE as t using (orig_no)
    # 			JOIN $INTS_TABLE as cl using (ints_no)
    # 		WHERE i.scale_no = 1 and i.level in (4, 5) and cl.order_no is not null
    # 		GROUP BY cl.class_no, i.interval_no";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      $result class/interval rows");
    
    my $a = 1;		# We can stop here when debugging.
}
