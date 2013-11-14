# 
# The Paleobiology Database
# 
#   OccurrenceTables.pm
# 

package OccurrenceTables;

use strict;

use base 'Exporter';

use Carp qw(carp croak);
use Try::Tiny;

use CoreFunction qw(activateTables);
use CollectionTables qw($COLL_MATRIX);
use IntervalTables qw($INTERVAL_DATA);
use TaxonDefs qw(@TREE_TABLE_LIST);
use ConsoleLog qw(logMessage);

our (@EXPORT_OK) = qw(buildOccurrenceTables buildDiversityTables updateOccLft
		      $OCC_MATRIX $OCC_TAXON $OCC_REF $DIV_SAMPLE);

our $OCC_MATRIX = "occ_matrix";
our $OCC_TAXON = "occ_taxon";
our $OCC_REF = "occ_ref";

our $OCC_MATRIX_WORK = "omn";
our $OCC_TAXON_WORK = "otn";
our $OCC_REF_WORK = "orn";


# buildOccurrenceTables ( dbh )
# 
# Build the occurrence matrix, recording which taxonomic concepts are
# associated with which collections in which geological and chronological
# locations.  This table is used to satisfy the bulk of the queries from the
# front-end application.  This function also builds an occurrence summary
# table, summarizing occurrence information by taxon.

sub buildOccurrenceTables {
    
    my ($dbh, $options) = @_;
    
    my ($sql, $result, $count);
    
    # Create a clean working table which will become the new occurrence
    # matrix.
    
    logMessage(1, "Building occurrence tables");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $OCC_MATRIX_WORK");
    $result = $dbh->do("CREATE TABLE $OCC_MATRIX_WORK (
				occurrence_no int unsigned primary key,
				collection_no int unsigned not null,
				reid_no int unsigned not null,
				taxon_no int unsigned not null,
				orig_no int unsigned not null,
				base_age decimal(9,5),
				top_age decimal(9,5),
				reference_no int unsigned not null,
				authorizer_no int unsigned not null,
				enterer_no int unsigned not null) ENGINE=MyISAM");
    
    # Add one row for every occurrence in the database.
    
    logMessage(2, "    inserting occurrences...");
    
    $sql = "	INSERT INTO $OCC_MATRIX_WORK
		       (occurrence_no, collection_no, taxon_no, orig_no, base_age, top_age, reference_no,
			authorizer_no, enterer_no)
		SELECT o.occurrence_no, o.collection_no, o.taxon_no, a.orig_no, ei.base_age, li.top_age,
			if(o.reference_no > 0, o.reference_no, c.reference_no),
			o.authorizer_no, o.enterer_no
		FROM occurrences as o JOIN coll_matrix as c using (collection_no)
			LEFT JOIN $INTERVAL_DATA as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $INTERVAL_DATA as li on li.interval_no = c.late_int_no
			LEFT JOIN authorities as a using (taxon_no)";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count occurrences");
    
    # Update each occurrence entry as necessary to take into account the latest
    # reidentification if any.
    
    $sql = "	UPDATE $OCC_MATRIX_WORK as m
			JOIN reidentifications as re on re.occurrence_no = m.occurrence_no 
				and re.most_recent = 'YES'
			JOIN authorities as a on a.taxon_no = re.taxon_no
		SET m.reid_no = re.reid_no,
		    m.taxon_no = re.taxon_no,
		    m.orig_no = a.orig_no,
		    m.reference_no = if(re.reference_no > 0, re.reference_no, m.reference_no)";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count re-identifications");
    
    # Add some indices to the main occurrence relation, which is more
    # efficient to do now that the table is populated.
    
    logMessage(2, "    indexing by collection...");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (collection_no)");
    
    logMessage(2, "    indexing by taxonomic concept...");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (orig_no)");
    
    logMessage(2, "    indexing by age boundaries...");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (base_age)");
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (top_age)");
    
    logMessage(2, "    indexing by reference_no...");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (reference_no)");
    
    # We now summarize the occurrence matrix by taxon.  We use the older_seq and
    # younger_seq interval identifications instead of interval_no, in order
    # that we can use the min() function to find the temporal bounds for each taxon.
    
    logMessage(2, "    summarizing by taxon...");
    
    # Then create working tables which will become the new taxon summary
    # table and reference summary table.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $OCC_TAXON_WORK");
    $result = $dbh->do("CREATE TABLE $OCC_TAXON_WORK (
				orig_no int unsigned primary key,
				lft int unsigned not null,
				n_occs int unsigned not null,
				n_colls int unsigned not null,
				first_early_age decimal(9,5),
				first_late_age decimal(9,5),
				last_early_age decimal(9,5),
				last_late_age decimal(9,5),
				early_occ int unsigned,
				late_occ int unsigned) ENGINE=MyISAM");
    
    # If the taxonomy tables have already been created, use the main one as
    # the source for the 'lft' field.
    
    my $main_tree_table = $TREE_TABLE_LIST[0];
    my $tree_result;
    my $tree_field = '';
    my $tree_select = '';
    my $tree_join = '';
    
    try {
	$tree_result = $dbh->do("SELECT COUNT(*) FROM $main_tree_table");
    };
    
    if ( $tree_result )
    {
	$tree_field = 'lft, ';
	$tree_select = 't.lft, ';
	$tree_join = "LEFT JOIN $main_tree_table as t using (orig_no)";
    }
    
    # Look for the lower and upper bounds for the interval range in which each taxon
    # occurs.  But ignore intervals at the period level and above (except for
    # precambrian and quaternary).  They are not just specific enough.
    
    $sql = "	INSERT INTO $OCC_TAXON_WORK (orig_no, $tree_field n_occs, n_colls,
			first_early_age, first_late_age, last_early_age, last_late_age)
		SELECT m.orig_no, $tree_select count(*), count(distinct collection_no),
			max(ei.base_age), max(li.top_age), min(ei.base_age), min(li.top_age)
		FROM $OCC_MATRIX_WORK as m JOIN $COLL_MATRIX as c using (collection_no)
			$tree_join
			JOIN $INTERVAL_DATA as ei on ei.interval_no = c.early_int_no
			JOIN $INTERVAL_DATA as li on li.interval_no = c.late_int_no
		WHERE (ei.level is null or ei.level > 3 or
				(ei.level = 3 and (ei.top_age >= 541.0 or ei.base_age <= 2.6))) and
		      (li.level is null or li.level > 3 or 
				(li.level = 3 and (li.top_age >= 541.0 or li.base_age <= 2.6)))
		GROUP BY m.orig_no
		HAVING m.orig_no > 0";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count taxa");
    
    # Now that we have the age bounds for the first and last occurrence, we
    # can select a candidate first and last occurrence for each taxon (from
    # among all of the occurrences in the earliest/latest time interval in
    # which that taxon is recorded).
    
    logMessage(2, "      finding first and last occurrences...");
    
    $sql = "	UPDATE $OCC_TAXON_WORK as s JOIN $OCC_MATRIX_WORK as o using (orig_no)
		SET s.early_occ = o.occurrence_no WHERE o.top_age >= s.first_late_age";
    
    $count = $dbh->do($sql);
    
    $sql = "	UPDATE $OCC_TAXON_WORK as s JOIN $OCC_MATRIX_WORK as o using (orig_no)
		SET s.late_occ = o.occurrence_no WHERE o.base_age <= s.last_early_age";
    
    $count = $dbh->do($sql);
    
    # Then index the symmary table by earliest and latest interval number, so
    # that we can quickly query for which taxa began or ended at a particular
    # time.
    
    logMessage(2, "      indexing the summary table...");
    
    $dbh->do("ALTER TABLE $OCC_TAXON_WORK ADD INDEX (lft)");
    $dbh->do("ALTER TABLE $OCC_TAXON_WORK ADD INDEX (first_early_age)");
    $dbh->do("ALTER TABLE $OCC_TAXON_WORK ADD INDEX (first_late_age)");
    $dbh->do("ALTER TABLE $OCC_TAXON_WORK ADD INDEX (last_early_age)");
    $dbh->do("ALTER TABLE $OCC_TAXON_WORK ADD INDEX (last_late_age)");
    
    # We now summarize the occurrence matrix by reference_no.  For each
    # reference, we record the range of time periods it covers, plus the
    # number of occurrences and collections that refer to it.
    
    logMessage(2, "      summarizing by reference_no...");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $OCC_REF_WORK");
    $result = $dbh->do("CREATE TABLE $OCC_REF_WORK (
				reference_no int unsigned primary key,
				n_occs int unsigned not null,
				n_colls int unsigned not null,
				early_age decimal(9,5),
				late_age decimal(9,5)) ENGINE=MyISAM");
    
    $sql = "	INSERT INTO $OCC_REF_WORK (reference_no, n_occs, n_colls,
			early_age, late_age)
		SELECT m.reference_no, count(*), count(distinct collection_no),
			max(ei.base_age), min(li.top_age)
		FROM $OCC_MATRIX_WORK as m JOIN $COLL_MATRIX as c using (collection_no)
			JOIN $INTERVAL_DATA as ei on ei.interval_no = c.early_int_no
			JOIN $INTERVAL_DATA as li on li.interval_no = c.late_int_no
		GROUP BY m.reference_no";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count references");
    
    # Then index the reference summary table by numbers of collections and
    # occurrences, so that we can quickly query for the most heavily used ones.
    
    logMessage(2, "      indexing the summary table...");
    
    $result = $dbh->do("ALTER TABLE $OCC_REF_WORK ADD INDEX (n_occs)");
    $result = $dbh->do("ALTER TABLE $OCC_REF_WORK ADD INDEX (n_colls)");
    $result = $dbh->do("ALTER TABLE $OCC_REF_WORK ADD INDEX (early_age)");
    $result = $dbh->do("ALTER TABLE $OCC_REF_WORK ADD INDEX (late_age)");
    
    # Now swap in the new tables.
    
    activateTables($dbh, $OCC_MATRIX_WORK => $OCC_MATRIX,
		         $OCC_TAXON_WORK => $OCC_TAXON,
			 $OCC_REF_WORK => $OCC_REF);
    
    
    my $a = 1;		# we can stop here when debugging.
}


# updateOccLft ( dbh, low, high )
# 
# Update the 'lft' numbers in the $OCC_TAXON table.  This should be called
# whenever the main taxon table is modified.  The parameter $low and $high, if
# specified, should be bounds indicating the range of values which need to be
# recomputed.

sub updateOccLft {
    
    my ($dbh, $low, $high) = @_;
    
    my ($sql, $result);
    
    # First make sure we have a table to update.
    
    try {
	$result = $dbh->do("SELECT count(*) FROM $OCC_TAXON");
    };
    
    return unless $result;
    
    my $TREE_TABLE = $TREE_TABLE_LIST[0];
    my $bound_clause = '';
    
    logMessage(2, "    updating 'lft' field of '$OCC_TAXON'");
    
    my $low_bound = $low + 0;
    my $high_bound = $high + 0;
    
    if ( $low > 0 && $high > 0 )
    {
	$bound_clause = "WHERE o.lft between $low and $high";
    }
    
    $sql = "	UPDATE $OCC_TAXON as o JOIN $TREE_TABLE as t using (orig_no)
		SET o.lft = t.lft
		$bound_clause";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      $result entries updated");
    
    my $a = 1;		# we can stop here when debugging;
}


1;
