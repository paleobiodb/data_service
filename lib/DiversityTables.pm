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
use TableDefs qw(%TABLE);
use IntervalBase qw(INTL_SCALE BIN_SCALE);
use base 'Exporter';

our (@EXPORT_OK) = qw(buildDiversityTables buildPrevalenceTables);

our $DIV_MATRIX_WORK = 'dmn';
our $DIV_GLOBAL_WORK = 'dgn';

our $PVL_MATRIX_WORK = 'pvn';
our $PVL_GLOBAL_WORK = 'pgn';
our $PVL_COLLS_WORK = 'pcn';



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

    my ($MBL) = $dbh->selectrow_array("SELECT max(bin_level) FROM $TABLE{SUMMARY_BINS}");

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
		SELECT SQL_NO_CACHE c.bin_id_${MBL}, mm.interval_no, ta.ints_no, pl.genus_no, 1, not(is_trace)
		FROM occ_matrix as o JOIN $TABLE{INTERVAL_MAJOR_MAP} as mm using (early_age, late_age)
			JOIN coll_matrix as c using (collection_no)
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
		SELECT SQL_NO_CACHE mm.interval_no, ta.ints_no, pl.genus_no, 1, not(is_trace)
		FROM occ_matrix as o JOIN $TABLE{INTERVAL_MAJOR_MAP} as mm using (early_age, late_age)
			JOIN coll_matrix as c using (collection_no)
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
			 $DIV_GLOBAL_WORK => $TABLE{DIVERSITY_GLOBAL});

    my $a = 1;	# we can stop here when debugging
}


sub buildPrevalenceTables {

    my ($dbh, $tree_table, $options) = @_;

    $options ||= {};

    my ($sql, $result);

    my $TREE_TABLE = $tree_table;
    my $INTS_TABLE = $TAXON_TABLE{$tree_table}{ints};
    my $ATTRS_TABLE = $TAXON_TABLE{$tree_table}{attrs};
    my $LOWER_TABLE = $TAXON_TABLE{$tree_table}{lower};

    my $scale_list = INTL_SCALE . ',' . BIN_SCALE;

    logMessage(1, "Building prevalence tables");

    my ($MBL) = $dbh->selectrow_array("SELECT max(bin_level) FROM $TABLE{SUMMARY_BINS}");

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

    $sql = "INSERT INTO $PVL_MATRIX_WORK (bin_id, interval_no, order_no, class_no, phylum_no, n_occs)
		SELECT bin_id_${MBL}, interval_no, order_no, class_no, phylum_no, count(*)
		FROM $TABLE{OCCURRENCE_MATRIX} as o JOIN $TABLE{INTERVAL_MAJOR_MAP} as mm
				using (early_age, late_age)
			JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			JOIN $TREE_TABLE as t using (orig_no)
			JOIN $INTS_TABLE as ph using (ints_no)
		WHERE o.latest_ident = 1 and c.access_level = 0 and bin_id_${MBL} > 0
			and (order_no <> 0 or class_no <> 0 or phylum_no <> 0)
		GROUP BY bin_id_${MBL}, interval_no, order_no, class_no, phylum_no";

    $result = $dbh->do($sql);

    logMessage(2, "      generated $result rows.");

    logMessage(2, "    generating prevalence matrix by geographic cluster and taxonomy...");

    $sql = "
	INSERT INTO $PVL_MATRIX_WORK (bin_id, interval_no, order_no, class_no, phylum_no, n_occs)
	SELECT bin_id_${MBL}, 0, order_no, class_no, phylum_no, count(*)
	FROM $TABLE{OCCURRENCE_MATRIX} as o JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
		JOIN $TREE_TABLE as t using (orig_no)
		JOIN $INTS_TABLE as ph using (ints_no)
	WHERE o.latest_ident = 1 and c.access_level = 0 and bin_id_${MBL} > 0
		and (order_no <> 0 or class_no <> 0 or phylum_no <> 0)
	GROUP BY bin_id_${MBL}, order_no, class_no, phylum_no";

    $result = $dbh->do($sql);

    logMessage(2, "      generated $result rows.");

    # Now sum over the entire table ignoring intervals to get counts over all
    # time.  These are stored with interval_no = 0.

    # $sql = "
    #	INSERT INTO $PVL_MATRIX_WORK (bin_id, interval_no, order_no, class_no, phylum_no, n_occs)
    #	SELECT bin_id, 0, order_no, class_no, phylum_no, sum(n_occs)
    #	FROM $PVL_MATRIX_WORK
    #	GROUP BY bin_id, order_no, class_no, phylum_no";

    # $result = $dbh->do($sql);

    # logMessage(2, "      generated $result rows for all time");

    # Then do the same for worldwide taxonomic prevalence.

    logMessage(2, "    generating global prevalence matrix by interval and taxonomy...");

    $dbh->do("DROP TABLE IF EXISTS $PVL_GLOBAL_WORK");

    $dbh->do("CREATE TABLE $PVL_GLOBAL_WORK (
		interval_no int unsigned not null default '0',
		order_no int unsigned,
		class_no int unsigned,
		phylum_no int unsigned,
		n_occs int unsigned not null default '0') Engine=MyISAM");

    $sql = "INSERT INTO $PVL_GLOBAL_WORK (interval_no, order_no, class_no, phylum_no, n_occs)
		SELECT interval_no, order_no, class_no, phylum_no, count(*)
		FROM $TABLE{OCCURRENCE_MATRIX} as o JOIN $TABLE{INTERVAL_MAJOR_MAP}
				using (early_age, late_age)
			JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			JOIN $TREE_TABLE as t using (orig_no)
			JOIN $INTS_TABLE as ph using (ints_no)
		WHERE o.latest_ident = 1 and c.access_level = 0
			and (order_no <> 0 or class_no <> 0 or phylum_no <> 0)
		GROUP BY interval_no, order_no, class_no, phylum_no";

    $result = $dbh->do($sql);

    logMessage(2, "      generated $result rows.");

    logMessage(2, "    generating global prevalence matrix by taxonomy alone...");

    $sql = "
	INSERT INTO $PVL_GLOBAL_WORK (interval_no, order_no, class_no, phylum_no, n_occs)
	SELECT 0 as interval_no, order_no, class_no, phylum_no, count(*)
	FROM $TABLE{OCCURRENCE_MATRIX} as o JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
		JOIN $TREE_TABLE as t using (orig_no)
		JOIN $INTS_TABLE as ph using (ints_no)
	WHERE o.latest_ident = 1 and c.access_level = 0
		and (order_no <> 0 or class_no <> 0 or phylum_no <> 0)
	GROUP BY order_no, class_no, phylum_no";

    $result = $dbh->do($sql);

    logMessage(2, "      generated $result rows.");



    # Then create a table to be used with filters such as stratum and research group.

    logMessage(2, "   generating collection prevalence matrix...");

    $dbh->do("CREATE TABLE $PVL_COLLS_WORK (
		collection_no int unsigned not null default '0',
		order_no int unsigned,
		class_no int unsigned,
		phylum_no int unsigned,
		n_occs int unsigned not null default '0') Engine=MyISAM");

    $sql = "
	INSERT INTO $PVL_COLLS_WORK (collection_no, order_no, class_no, phylum_no, n_occs)
	SELECT collection_no, order_no, class_no, phylum_no, count(*)
	FROM $TABLE{OCCURRENCE_MATRIX} as o JOIN $TREE_TABLE as t using (orig_no)
		JOIN $INTS_TABLE as ph using (ints_no)
	WHERE o.latest_ident = 1 and (order_no <> 0 or class_no <> 0 or phylum_no <> 0)
	GROUP BY collection_no, order_no, class_no, phylum_no";

    $result = $dbh->do($sql);

    logMessage(2, "      generated $result rows.");

    logMessage(2, "    indexing tables...");

    $dbh->do("ALTER TABLE $PVL_MATRIX_WORK ADD KEY (bin_id, interval_no)");
    $dbh->do("ALTER TABLE $PVL_GLOBAL_WORK ADD KEY (interval_no)");
    $dbh->do("ALTER TABLE $PVL_COLLS_WORK ADD KEY (collection_no)");

    # Finally, we swap in the new tables for the old ones.

    activateTables($dbh, $PVL_MATRIX_WORK => $TABLE{PREVALENCE_MATRIX},
		   $PVL_GLOBAL_WORK => $TABLE{PREVALENCE_GLOBAL},
		   $PVL_COLLS_WORK => $TABLE{PREVALENCE_COLLS});

    my $a = 1;		# We can stop here when debugging.
}
