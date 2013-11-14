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

use TaxonDefs qw(@TREE_TABLE_LIST %TAXON_TABLE $CLASSIC_TREE_CACHE $CLASSIC_LIST_CACHE);

use CoreFunction qw(activateTables);
use ConsoleLog qw(initMessages logMessage);
use OccurrenceTables qw($OCC_MATRIX $OCC_TAXON);
use IntervalTables qw($INTERVAL_DATA $INTERVAL_MAP);

use base 'Exporter';

our (@EXPORT_OK) = qw(buildDiversityTables $DIV_SAMPLED_STD $DIV_SAMPLED_10);


our $DIV_SAMPLED_STD = 'div_sampled_std';
our $DIV_SAMPLED_10MY = 'div_sampled_10';

our $SAMPLED_STD_WORK = 'dssn';
our $SAMPLED_10MY_WORK = 'dstn';

our $SPECIES_AUX = 'species_aux';
our $GENUS_AUX = 'genus_aux';
our $FAMILY_AUX = 'family_aux';


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
    
    logMessage(1, "Building diversity tables");
    
    logMessage(2, "    sampled, standard epochs and ages...");
    
    $dbh->do("DROP TABLE IF EXISTS $SAMPLED_STD_WORK");
    
    $dbh->do("CREATE TABLE $SAMPLED_STD_WORK (
		base_no int unsigned not null,
		lft int unsigned not null,
		interval_no int unsigned not null,
		n_species int unsigned not null,
		n_genera int unsigned not null,
		n_families int unsigned not null,
		PRIMARY KEY (base_no, interval_no),
		KEY (lft, interval_no))");
    
    $sql = "	INSERT INTO $SAMPLED_STD_WORK (base_no, interval_no, n_species, n_genera)
		SELECT cl.family_no, i.interval_no,
			count(distinct species_no) as n_species,
			count(distinct genus_no) as n_genera
		FROM $OCC_MATRIX as m JOIN $INTERVAL_DATA as i on m.base_age <= i.base_age and m.top_age >= i.top_age
			JOIN $TREE_TABLE as t using (orig_no)
			JOIN $INTS_TABLE as cl using (ints_no)
		WHERE i.scale_no = 1 and i.level in (4, 5) and cl.family_no is not null
		GROUP BY cl.family_no, i.interval_no";    
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      $result family/interval rows");
    
    $sql = "    INSERT INTO $SAMPLED_STD_WORK (base_no, interval_no, n_species, n_genera, n_families)
		SELECT cl.order_no, i.interval_no,
			count(distinct species_no) as n_species,
			count(distinct genus_no) as n_genera,
			count(distinct family_no) as n_families
		FROM $OCC_MATRIX as m JOIN $INTERVAL_DATA as i on m.base_age <= i.base_age and m.top_age >= i.top_age
			JOIN $TREE_TABLE as t using (orig_no)
			JOIN $INTS_TABLE as cl using (ints_no)
		WHERE i.scale_no = 1 and i.level in (4, 5) and cl.order_no is not null
		GROUP BY cl.order_no, i.interval_no";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      $result order/interval rows");
    
    $sql = "    INSERT INTO $SAMPLED_STD_WORK (base_no, interval_no, n_species, n_genera, n_families)
		SELECT cl.class_no, i.interval_no,
			count(distinct species_no) as n_species,
			count(distinct genus_no) as n_genera,
			count(distinct family_no) as n_families
		FROM $OCC_MATRIX as m JOIN $INTERVAL_DATA as i on m.base_age <= i.base_age and m.top_age >= i.top_age
			JOIN $TREE_TABLE as t using (orig_no)
			JOIN $INTS_TABLE as cl using (ints_no)
		WHERE i.scale_no = 1 and i.level in (4, 5) and cl.order_no is not null
		GROUP BY cl.class_no, i.interval_no";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      $result class/interval rows");
    
    my $a = 1;		# We can stop here when debugging.
}
