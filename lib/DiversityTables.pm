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
use TableDefs qw($COLL_MATRIX $COUNTRY_MAP $OCC_MATRIX $OCC_TAXON
		 $INTERVAL_DATA $SCALE_LEVEL_DATA $SCALE_DATA $SCALE_MAP $INTERVAL_MAP);

use base 'Exporter';

our (@EXPORT_OK) = qw(buildDiversityTables $DIV_SAMPLED_STD $DIV_SAMPLED_10);

our $DIV_RAW = 'div_raw';
our $DIV_SAMPLED = 'div_sampled';
our $DIV_RANGE = 'div_range';

our $DIV_RAW_WORK = 'dwn';
our $DIV_SAMPLED_WORK = 'dsn';
our $DIV_RANGE_WORK = 'drn';

our $DIV_AUX = 'div_aux';


# buildDiversityTables ( dbh )
# 
# Build the taxonomic diversity tables.

sub buildDiversityTables {

    my ($dbh, $tree_table, $skip_raw) = @_;
    
    my ($sql, $result);
    
    my $TREE_TABLE = $tree_table;
    my $INTS_TABLE = $TAXON_TABLE{$tree_table}{ints};
    
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
    
    logMessage(1, "Building diversity tables");

    unless ( $skip_raw )
    {
	logMessage(2, "    tabulating occurrences by interval...");
	
	$dbh->do("DROP TABLE IF EXISTS $DIV_RAW_WORK");
	
	$dbh->do("
    	CREATE TABLE $DIV_RAW_WORK (
    		interval_no int unsigned not null,
    		occurrence_no int unsigned not null,
    		species_no int unsigned not null,
    		genus_no int unsigned not null,
    		ints_no int unsigned not null,
    		prob float,
		early_prob float,
		late_prob float)");
	
	$result = $dbh->do("
    	INSERT INTO $DIV_RAW_WORK
    	SELECT i.interval_no, m.occurrence_no,
	       t.species_no, t.genus_no, t.ints_no,
	       -- sample prob
    	       if(m.early_age <= i.early_age and m.late_age >= i.late_age, 1.0,
    	       if(m.early_age <= i.early_age, (m.early_age-i.late_age)/(m.early_age-m.late_age),
    	       if(m.late_age >= i.late_age, (i.early_age-m.late_age)/(m.early_age-m.late_age),
    					  (i.early_age-i.late_age)/(m.early_age-m.late_age)))) as prob,
	       -- early prob
	       if(m.late_age >= i.late_age, 1.0, 
			(m.early_age-i.late_age)/(m.early_age-m.late_age)) as early_prob,
	       -- late prob
	       if(m.early_age <= i.early_age, 1.0,
			(i.early_age-m.late_age)/(m.early_age-m.late_age))
    	FROM occ_matrix as m JOIN $INTERVAL_DATA as i on m.late_age < i.early_age and m.early_age > i.late_age
		JOIN $SCALE_MAP as sm using (interval_no)
		JOIN $SCALE_LEVEL_DATA as s using (scale_no, level)
    		JOIN $TREE_TABLE as t using (orig_no)
    	WHERE t.ints_no > 0 and s.sample");
	
	logMessage(2, "    found $result rows.");
	
	logMessage(2, "    indexing by interval_no...");
	
	$dbh->do("ALTER TABLE $DIV_RAW_WORK ADD INDEX (interval_no)");
    }
    
    logMessage(2, "    summarizing occurrences by interval for sampled statistics...");
    
    $dbh->do("DROP TABLE IF EXISTS $DIV_SAMPLED_WORK");
    
    $dbh->do("
    	CREATE TABLE $DIV_SAMPLED_WORK (
    		interval_no int unsigned not null,
    		orig_no int unsigned not null,
    		lft int unsigned not null,
    		rank tinyint unsigned not null,
    		occurrence_no int unsigned not null,
    		prob float,
    		PRIMARY KEY (interval_no, orig_no))");
    
    $result = $dbh->do("
    	INSERT INTO $DIV_SAMPLED_WORK (interval_no, orig_no, rank, occurrence_no, prob)
    	SELECT interval_no, ints_no, $TAXON_RANK{unranked}, coalesce(occurrence_no),
    	       if(max(prob) = 1.0, 1.0, 1.0 - exp(sum(log(1.0-prob)))) as prob
    	FROM $DIV_RAW_WORK
    	WHERE ints_no > 0
    	GROUP BY interval_no, ints_no
    	ORDER BY NULL");
    
    logMessage(2, "      found $result identifications of higher taxa in specific intervals.");
    
    $result = $dbh->do("
    	INSERT INTO $DIV_SAMPLED_WORK (interval_no, orig_no, rank, occurrence_no, prob)
    	SELECT interval_no, genus_no, $TAXON_RANK{genus}, coalesce(occurrence_no),
    	       if(max(prob) = 1.0, 1.0, 1.0 - exp(sum(log(1.0-prob)))) as prob
    	FROM $DIV_RAW_WORK
    	WHERE genus_no > 0
    	GROUP BY interval_no, genus_no
    	ORDER BY NULL");
    
    logMessage(2, "      found $result identifications of genera in specific intervals.");
    
    $result = $dbh->do("
    	INSERT INTO $DIV_SAMPLED_WORK (interval_no, orig_no, rank, occurrence_no, prob)
    	SELECT interval_no, species_no, $TAXON_RANK{species}, coalesce(occurrence_no),
    	       if(max(prob) = 1.0, 1.0, 1.0 - exp(sum(log(1.0-prob)))) as prob
    	FROM $DIV_RAW_WORK
    	WHERE species_no > 0
    	GROUP BY interval_no, species_no
    	ORDER BY NULL");
    
    logMessage(2, "      found $result identifications of species in specific intervals.");
    
    logMessage(2, "    setting tree sequence numbers...");
    
    $result = $dbh->do("
    	UPDATE $DIV_SAMPLED_WORK as d JOIN $TREE_TABLE as t using (orig_no)
    	SET d.lft = t.lft");
    
    # Now range-through.
    
    logMessage(2, "    summarizing occurrences by interval for range-through statistics...");
    
    $dbh->do("DROP TABLE IF EXISTS $DIV_RANGE_WORK");
    
    $dbh->do("
	CREATE TABLE $DIV_RANGE_WORK (
		scale_no smallint unsigned not null,
		level smallint unsigned not null,
		orig_no int unsigned not null,
		lft int unsigned not null,
		rank tinyint unsigned not null,
		early_age decimal(9,5),
		late_age decimal(9,5),
		early_age_08 decimal(9,5),
		late_age_08 decimal(9,5),
		early_age_05 decimal(9,5),
		late_age_05 decimal(9,5),
		-- early_prob_age decimal(9,5),
		-- min_prob float,
		-- late_prob_age decimal(9,5),
		-- max_prob float,
		early_occ_no int unsigned not null,
		late_occ_no int unsigned not null,
		PRIMARY KEY (scale_no, level, orig_no))");
    
    $result = $dbh->do("
	INSERT INTO $DIV_RANGE_WORK (scale_no, level, orig_no, rank,
				     early_age, late_age,
				     early_age_08, late_age_08,
				     early_age_05, late_age_05,
				     early_occ_no, late_occ_no)
	SELECT sm.scale_no, sm.level, genus_no, $TAXON_RANK{genus},
	       if(prob >= 1.0, i.early_age, null), if(early_prob >= 1.0, i.late_age, null),
	       if(prob >= 0.8, i.early_age, null), if(early_prob >= 0.8, i.late_age, null),
	       i.early_age, i.late_age,
	       occurrence_no, occurrence_no
	FROM $DIV_RAW_WORK as w JOIN $INTERVAL_DATA as i using (interval_no) JOIN $SCALE_DATA as sm using (interval_no)
	WHERE genus_no > 0 and (prob >= 0.5 or early_prob >= 0.8 or late_prob >= 0.5)
	ORDER BY i.early_age desc
	ON DUPLICATE KEY UPDATE
		early_age = if(w.early_prob >= 1.0 and early_age is null, i.early_age, early_age),
		late_age = if(w.late_prob >= 1.0, i.late_age, late_age),
		early_age_08 = if(w.early_prob >= 0.8 and early_age_08 is null, i.early_age, early_age_08),
		late_age_08 = if(w.late_prob >= 0.8, i.late_age, late_age_08),
		late_age_05 = if(w.late_prob >= 0.5, i.late_age, late_age_05),
		late_occ_no = w.occurrence_no");
    
    logMessage(2, "    found $result rows.");
    
    my $a = 1;		# We can stop here when debugging.
}
