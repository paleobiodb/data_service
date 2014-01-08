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
use IntervalTables qw($INTERVAL_DATA $SCALE_MAP);
use TaxonDefs qw(@TREE_TABLE_LIST);
use ConsoleLog qw(logMessage);

our (@EXPORT_OK) = qw(buildOccurrenceTables buildDiversityTables
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
				early_age decimal(9,5),
				late_age decimal(9,5),
				reference_no int unsigned not null,
				authorizer_no int unsigned not null,
				enterer_no int unsigned not null) ENGINE=MyISAM");
    
    # Add one row for every occurrence in the database.
    
    logMessage(2, "    inserting occurrences...");
    
    $sql = "	INSERT INTO $OCC_MATRIX_WORK
		       (occurrence_no, collection_no, taxon_no, orig_no, early_age, late_age, reference_no,
			authorizer_no, enterer_no)
		SELECT o.occurrence_no, o.collection_no, o.taxon_no, a.orig_no, ei.early_age, li.late_age,
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
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (early_age)");
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (late_age)");
    
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
				n_occs int unsigned not null,
				n_colls int unsigned not null,
				first_early_age decimal(9,5),
				last_early_age decimal(9,5),
				first_late_age decimal(9,5),
				last_late_age decimal(9,5),
				precise_age boolean default true,
				early_occ int unsigned,
				late_occ int unsigned) ENGINE=MyISAM");
    
    # Look for the lower and upper bounds for the interval range in which each
    # taxon occurs.  Start by ignoring occurrences dated to an epoch, era or
    # period (except for Ediacaran and Quaternary) or to an interval that
    # spans more than 30 million years, because these are not precise enough for
    # first/last appearance calculations.
    
    # This is not the approach we ultimately want to take: we will need to
    # revisit this procedure, and figure out a better way to determine
    # first/last appearance ranges (as probability curves, perhaps?)
    
    $sql = "	INSERT INTO $OCC_TAXON_WORK (orig_no, n_occs, n_colls,
			first_early_age, last_early_age, first_late_age, last_late_age,
			precise_age)
		SELECT m.orig_no, count(*), count(distinct collection_no),
			max(ei.early_age), min(ei.early_age), max(li.late_age), min(li.late_age),
			true
		FROM $OCC_MATRIX_WORK as m JOIN $COLL_MATRIX as c using (collection_no)
			JOIN $INTERVAL_DATA as ei on ei.interval_no = c.early_int_no
			JOIN $INTERVAL_DATA as li on li.interval_no = c.late_int_no
			LEFT JOIN $SCALE_MAP as es on es.interval_no = ei.interval_no
			LEFT JOIN $SCALE_MAP as ls on ls.interval_no = li.interval_no
		WHERE (ei.early_age - li.late_age <= 30 and li.late_age >= 20) or
		      (ei.early_age - li.late_age <= 20 and li.late_age < 20) or
		      (es.scale_no = 1 and es.level in (4,5) and ei.early_age - li.late_age <= 50) or
		      (ls.scale_no = 1 and ls.level in (4,5) and ei.early_age - li.late_age <= 50) or
		      (es.scale_no = 1 and es.level = 3 and ei.early_age < 3) or
		      (ls.scale_no = 1 and ls.level = 3 and li.late_age >= 540)
		GROUP BY m.orig_no
		HAVING m.orig_no > 0";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count taxa");
    
    # Then we need to go back and add in the taxa that are only known from
    # occurrences with non-precise ages (i.e. range > 40 my).
    
    $sql = "	INSERT IGNORE INTO $OCC_TAXON_WORK (orig_no, n_occs, n_colls,
			first_early_age, first_late_age, last_early_age, last_late_age,
			precise_age)
		SELECT m.orig_no, count(*), count(distinct collection_no),
			max(ei.early_age), max(li.late_age), min(ei.early_age), min(li.late_age),
			false
		FROM $OCC_MATRIX_WORK as m JOIN $COLL_MATRIX as c using (collection_no)
			JOIN $INTERVAL_DATA as ei on ei.interval_no = c.early_int_no
			JOIN $INTERVAL_DATA as li on li.interval_no = c.late_int_no
		GROUP BY m.orig_no
		HAVING m.orig_no > 0";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count taxa without highly specific ages");
    
    # Now that we have the age bounds for the first and last occurrence, we
    # can select a candidate first and last occurrence for each taxon (from
    # among all of the occurrences in the earliest/latest time interval in
    # which that taxon is recorded).
    
    logMessage(2, "      finding first and last occurrences...");
    
    $sql = "	UPDATE $OCC_TAXON_WORK as s JOIN $OCC_MATRIX_WORK as o using (orig_no)
		SET s.early_occ = o.occurrence_no WHERE o.late_age >= s.first_late_age";
    
    $count = $dbh->do($sql);
    
    $sql = "	UPDATE $OCC_TAXON_WORK as s JOIN $OCC_MATRIX_WORK as o using (orig_no)
		SET s.late_occ = o.occurrence_no WHERE o.early_age <= s.last_early_age";
    
    $count = $dbh->do($sql);
    
    # Then index the summary table by earliest and latest interval number, so
    # that we can quickly query for which taxa began or ended at a particular
    # time.
    
    logMessage(2, "      indexing the summary table...");
    
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
			max(ei.early_age), min(li.late_age)
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


1;
