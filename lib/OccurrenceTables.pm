# 
# The Paleobiology Database
# 
#   OccurrenceTables.pm
# 
# Build the tables needed by the data service for satisfying queries about
# occurrences.

package OccurrenceTables;

use strict;

use base 'Exporter';

use Carp qw(carp croak);
use Try::Tiny;

use CoreFunction qw(activateTables doStmt);
use TableDefs qw(%TABLE);
use CoreTableDefs;
use TaxonDefs qw(@TREE_TABLE_LIST %TAXON_TABLE %TAXON_RANK);
use ConsoleLog qw(logMessage logTimestamp);

our (@EXPORT_OK) = qw(buildOccurrenceTables buildTaxonSummaryTable updateOccurrenceMatrix
		      buildTaxonCollectionsTable);

our $OCC_MATRIX_WORK = "omn";
our $OCC_TAXON_WORK = "otn";
our $REF_SUMMARY_WORK = "orn";
our $TAXON_COLLS_WORK = "tcn";
our $TCN2 = "tcn2";


# buildOccurrenceTables ( dbh )
# 
# Build the occurrence matrix, recording which taxonomic concepts are
# associated with which collections in which geological and chronological
# locations.  This table is used to satisfy the bulk of the queries from the
# front-end application.  This function also builds an occurrence summary
# table, summarizing occurrence information by taxon.

sub buildOccurrenceTables {
    
    my ($dbh, $tree_table, $options) = @_;
    
    my ($sql, $result, $count, $extra);
    
    $options ||= { };
    
    # Create a clean working table which will become the new occurrence
    # matrix.
    
    logTimestamp();
    
    logMessage(1, "Building occurrence tables");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $OCC_MATRIX_WORK");
    $result = $dbh->do("CREATE TABLE $OCC_MATRIX_WORK (
				occurrence_no int unsigned not null,
				reid_no int unsigned not null,
				collection_no int unsigned not null,
				taxon_no int unsigned not null,
				orig_no int unsigned not null,
				latest_ident boolean not null,
				genus_name varchar(255) not null,
				genus_reso varchar(255) not null,
				subgenus_name varchar(255) not null,
				subgenus_reso varchar(255) not null,
				species_name varchar(255) not null,
				species_reso varchar(255) not null,
				plant_organ varchar(255),
				plant_organ2 varchar(255),
				early_age decimal(9,5),
				late_age decimal(9,5),
				reference_no int unsigned not null,
				authorizer_no int unsigned not null,
				enterer_no int unsigned not null,
				modifier_no int unsigned not null,
				created timestamp null,
				modified timestamp null,
				primary key (occurrence_no, reid_no),
				key (reid_no)) ENGINE=MyISAM");
    
    # Add one row for every occurrence in the database.
    
    logMessage(2, "    inserting occurrences...");
    
    $sql = "	INSERT INTO $OCC_MATRIX_WORK
		       (occurrence_no, reid_no, latest_ident, collection_no, taxon_no, orig_no,
			genus_name, genus_reso, subgenus_name, subgenus_reso, 
			species_name, species_reso, plant_organ, plant_organ2,
			early_age, late_age, reference_no, 
			authorizer_no, enterer_no, modifier_no, created, modified)
		SELECT o.occurrence_no, 0, true, o.collection_no, o.taxon_no, a.orig_no, 
			o.genus_name, o.genus_reso, o.subgenus_name, o.subgenus_reso,
			o.species_name, o.species_reso, o.plant_organ, o.plant_organ2,
			ei.early_age, li.late_age,
			if(o.reference_no > 0, o.reference_no, c.reference_no),
			o.authorizer_no, o.enterer_no, o.modifier_no, o.created, o.modified
		FROM $TABLE{OCCURRENCE_DATA} as o 
			JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
			LEFT JOIN $TABLE{AUTHORITY_DATA} as a using (taxon_no)";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count occurrences");
    
    # Then add one row for every reidentification in the database.
    
    $sql = "	INSERT INTO $OCC_MATRIX_WORK
		       (occurrence_no, reid_no, latest_ident, collection_no, taxon_no, orig_no,
			genus_name, genus_reso, subgenus_name, subgenus_reso,
			species_name, species_reso, plant_organ,
			early_age, late_age, reference_no, access_level,
			authorizer_no, enterer_no, modifier_no, created, modified)
		SELECT re.occurrence_no, re.reid_no, if(re.most_recent = 'YES', 1, 0),
			re.collection_no, re.taxon_no, a.orig_no, 
			re.genus_name, re.genus_reso, re.subgenus_name, re.subgenus_reso,
			re.species_name, re.species_reso, re.plant_organ,
			ei.early_age, li.late_age, re.reference_no, c.access_level,
			re.authorizer_no, re.enterer_no, re.modifier_no, re.created, re.modified
		FROM reidentifications as re
			JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
			LEFT JOIN $TABLE{AUTHORITY_DATA} as a using (taxon_no)";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count re-identifications");
    
    # For each reidentification, the corresponding record in occ_matrix drawn
    # from the original occurrence record needs to be "de-selected".
    
    logMessage(2, "    marking superceded identifications...");
    
    $sql = "	UPDATE $OCC_MATRIX_WORK as m
			JOIN reidentifications as re on re.occurrence_no = m.occurrence_no 
				and re.most_recent = 'YES'
		SET m.latest_ident = false WHERE m.reid_no = 0";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count superceded identifications");
    
    # Now add some indices to the main occurrence relation, which is more
    # efficient to do now that the table is populated.
    
    logMessage(2, "    indexing by selection...");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX selection (occurrence_no, latest_ident)");
    
    logMessage(2, "    indexing by collection...");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (collection_no)");
    
    logMessage(2, "    indexing by taxonomic identification...");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (genus_name)");
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (subgenus_name)");
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (species_name)");
    
    logMessage(2, "    indexing by taxonomic concept...");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (orig_no)");
    
    logMessage(2, "    indexing by age boundaries...");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (early_age)");
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (late_age)");
    
    logMessage(2, "    indexing by reference_no...");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (reference_no)");
    
    logMessage(2, "    indexing by person...");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (authorizer_no)");
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (enterer_no)");
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (modifier_no)");
    
    logMessage(2, "    indexing by timestamp...");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (created)");
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (modified)");
    
    # Then activate the new tables.
    
    activateTables($dbh, $OCC_MATRIX_WORK => $TABLE{OCCURRENCE_MATRIX});
    
    # Create tables summarizing the occurrences by taxon and reference.
    
    buildTaxonSummaryTable($dbh, $tree_table, $options);
    buildReferenceSummaryTable($dbh, $tree_table, $options);
    # buildTaxonCollectionsTable($dbh, $tree_table, $options);
    # buildOccIntervalMaps($dbh, $options);
    
    $dbh->do("REPLACE INTO last_build (name) values ('occurrences')");
    
    my $a = 1;	# we can stop here when debugging
}


# updateOccurrenceMatrix ( dbh, occurrence_no )
# 
# Update one entry in the occurrence matrix, to reflect any changes in the
# specified occurrence.

sub updateOccurrenceMatrix {

    my ($dbh, $occurrence_no, $options) = @_;
    
    my ($sql, $count, $extra);
    
    $options ||= { };
    
    # First replace the main occurrence record
    
    logMessage(2, "    updating occurrence...");
    
    $sql = "	REPLACE INTO $TABLE{OCCURRENCE_MATRIX}
		       (occurrence_no, reid_no, latest_ident, collection_no, taxon_no, orig_no,
			genus_name, genus_reso, subgenus_name, subgenus_reso, 
			species_name, species_reso, plant_organ, plant_organ2,
			early_age, late_age, reference_no, access_level,
			authorizer_no, enterer_no, modifier_no, created, modified)
		SELECT o.occurrence_no, 0, true, o.collection_no, o.taxon_no, a.orig_no, 
			o.genus_name, o.genus_reso, o.subgenus_name, o.subgenus_reso,
			o.species_name, o.species_reso, o.plant_organ, o.plant_organ2,
			ei.early_age, li.late_age,
			if(o.reference_no > 0, o.reference_no, c.reference_no), c.access_level,
			o.authorizer_no, o.enterer_no, o.modifier_no, o.created, o.modified
		FROM $TABLE{OCCURRENCE_DATA} as o
			JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
			LEFT JOIN $TABLE{AUTHORITY_DATA} as a using (taxon_no)
		WHERE occurrence_no = $occurrence_no";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $count = $dbh->do($sql);
    
    # Then replace any reidentifications
    
    $sql = "	REPLACE INTO $TABLE{OCCURRENCE_MATRIX}
		       (occurrence_no, reid_no, latest_ident, collection_no, taxon_no, orig_no,
			genus_name, genus_reso, subgenus_name, subgenus_reso,
			species_name, species_reso, plant_organ,
			early_age, late_age, reference_no, access_level,
			authorizer_no, enterer_no, modifier_no, created, modified)
		SELECT re.occurrence_no, re.reid_no, if(re.most_recent = 'YES', 1, 0), re.collection_no, re.taxon_no, a.orig_no, 
			re.genus_name, re.genus_reso, re.subgenus_name, re.subgenus_reso,
			re.species_name, re.species_reso, re.plant_organ,
			ei.early_age, li.late_age, re.reference_no, c.access_level,
			re.authorizer_no, re.enterer_no, re.modifier_no, re.created, re.modified
		FROM reidentifications as re
			JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
			LEFT JOIN $TABLE{AUTHORITY_DATA} as a using (taxon_no)
		WHERE occurrence_no = $occurrence_no and reid_no > 0";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $count = $dbh->do($sql);
    
    # Now make sure that superceded identifications are marked

    $sql = "	UPDATE $TABLE{OCCURRENCE_MATRIX} as m
			JOIN reidentifications as re on re.occurrence_no = m.occurrence_no 
				and re.most_recent = 'YES'
		SET m.latest_ident = false WHERE m.occurrence_no = $occurrence_no and m.reid_no = 0";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $count = $dbh->do($sql);
    
    my $a = 1;	# we can stop here when debugging
}


# Buildtaxonsummarytable ( dbh, options )
# 
# Create a table to summarize the occurrences by taxon.

sub buildTaxonSummaryTable {

    my ($dbh, $tree_table, $options) = @_;
    
    my ($sql, $result, $count);
    
    $options ||= { };
    
    # First make sure that the scale_map table has field 'scale_level' instead
    # of just 'level'. Also make sure it has an index called `interval_no`.
    
    my ($table_name, $table_definition) = $dbh->selectrow_array("SHOW CREATE TABLE $TABLE{SCALE_MAP}"); 
    
    unless ( $table_definition =~ /`scale_level`/i )
    {
	$dbh->do("ALTER TABLE $TABLE{SCALE_MAP} change column `level` `scale_level` smallint unsigned not null");
    }

    unless ( $table_definition =~ /key `interval_no`/i )
    {
	$dbh->do("ALTER TABLE $TABLE{SCALE_MAP} ADD KEY interval_no (interval_no)");
    }
    
    # Then create working tables which will become the new taxon summary
    # table and reference summary table.
        
    logMessage(2, "    summarizing by taxonomic concept...");
    
    $result = doStmt($dbh, "DROP TABLE IF EXISTS $OCC_TAXON_WORK", $options->{debug});
    
    $result = doStmt($dbh, "CREATE TABLE $OCC_TAXON_WORK (
				orig_no int unsigned PRIMARY KEY,
				n_occs int unsigned not null,
				n_occs_mar int unsigned not null,
				n_occs_ter int unsigned not null,
				n_colls int unsigned not null,
				n_colls_mar int unsigned not null,
				n_colls_ter int unsigned not null,
				first_early_age decimal(9,5),
				first_late_age decimal(9,5),
				last_early_age decimal(9,5),
				last_late_age decimal(9,5),
				precise_age boolean default false,
				early_occ int unsigned,
				late_occ int unsigned) ENGINE=MyISAM", $options->{debug});
    
    # Fill in this table with numbers of occurrences, and age bounds for first
    # and last appearance crudely calculated as the minimum and maximum of the
    # age ranges of the individual occurrences of each taxon.
    
    $sql = "	INSERT INTO $OCC_TAXON_WORK (orig_no, n_occs, n_occs_mar, n_occs_ter,
			n_colls, n_colls_mar, n_colls_ter,
			first_early_age, first_late_age, last_early_age, last_late_age,
			precise_age)
		SELECT orig_no, count(*), count(if(marine=1,1,null)), count(if(marine=0,1,null)),
			count(distinct collection_no),
			count(distinct if(marine=1,collection_no,null)),
			count(distinct if(marine=0,collection_no,null)),
			max(ei.early_age), max(li.late_age), min(ei.early_age), min(li.late_age),
			false
		FROM $TABLE{OCCURRENCE_MATRIX} as m
			JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
		WHERE latest_ident and c.access_level = 0 and orig_no > 0
		GROUP BY orig_no";

    print STDERR "$sql\n\n" if $options->{debug};
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      found $count unique taxa");
    
    # We then try to tighten up these bounds by recalculating them ignoring
    # occurrences dated to an eon, era or period (except for Ediacaran and
    # Quaternary) or to an interval that spans more than 30 million years.
    # Any taxon having occurrences that are dated more precisely than this
    # will be substituted with more precise bounds based just on those
    # occurrences.
    
    # The age thresholds can be adjusted by means of the $options hash.
    
    logMessage(2, "    computing precise age bounds where possible...");
    
    logMessage(2, "      a \"precise\" age is counted as:");
    
    my $epoch_bound = $options->{epoch_bound} || 50;
    my $interval_bound = $options->{interval_bound} || 30;
    my $levels = "4,5";
    
    if ( $options->{accept_periods} )
    {
	$levels = "3,4,5";
	logMessage(2, "      - any period (or range of periods) not greater than $epoch_bound My");
    }
    
    logMessage(2,"      - any epoch/stage (or range of epochs/stages) not greater than $epoch_bound My");
    logMessage(2,"      - any other interval range not greater than $interval_bound My");
    logMessage(2,"      - the Quaternary period");
    logMessage(2,"      - any Precambrian period, epoch or stage");
    
    # This is not the approach we ultimately want to take: we will need to
    # revisit this procedure, and figure out a better way to determine
    # first/last appearance ranges (as probability curves, perhaps?)
    
    # $sql = "	INSERT INTO $OCC_TAXON_WORK (orig_no, n_occs, n_colls,
    # 			first_early_age, first_late_age, last_early_age, last_late_age,
    # 			precise_age)
    # 		SELECT m.orig_no, count(*), 0,
    # 			max(ei.early_age) as fea, max(li.late_age) as lea,
    # 			min(ei.early_age) as fla, min(li.late_age) as lla,
    # 			true
    # 		FROM $TABLE{OCCURRENCE_MATRIX} as m
    # 			JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
    # 			JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
    # 			JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
    # 			LEFT JOIN $TABLE{SCALE_MAP} as es on es.interval_no = ei.interval_no
    # 			LEFT JOIN $TABLE{SCALE_MAP} as ls on ls.interval_no = li.interval_no
    # 		WHERE m.latest_ident and m.orig_no > 0 and m.access_level = 0 and
    # 		      ((ei.early_age - li.late_age <= $interval_bound and li.late_age >= 20) or
    # 		      (ei.early_age - li.late_age <= 20 and li.late_age < 20) or
    # 		      (es.scale_no = 1 and es.scale_level in ($levels) and ei.early_age - li.late_age <= $epoch_bound) or
    # 		      (ls.scale_no = 1 and ls.scale_level in ($levels) and ei.early_age - li.late_age <= $epoch_bound) or
    # 		      (es.scale_no = 1 and es.scale_level = 3 and ei.early_age < 3) or
    # 		      (ls.scale_no = 1 and ls.scale_level = 3 and li.late_age >= 540))
    # 		GROUP BY m.orig_no
    # 		ON DUPLICATE KEY UPDATE
    # 			first_early_age = values(first_early_age),
    # 			first_late_age = values(first_late_age),
    # 			last_early_age = values(last_early_age),
    # 			last_late_age = values(last_late_age),
    # 			precise_age = true";

    $sql = "UPDATE $OCC_TAXON_WORK as s JOIN
		(SELECT orig_no, max(ei.early_age) as fea, max(li.late_age) as lea,
    			min(ei.early_age) as fla, min(li.late_age) as lla
    		FROM $TABLE{OCCURRENCE_MATRIX} as m
    			JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
    			JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
    			JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
    			LEFT JOIN $TABLE{SCALE_MAP} as es on es.interval_no = ei.interval_no
    			LEFT JOIN $TABLE{SCALE_MAP} as ls on ls.interval_no = li.interval_no
    		WHERE latest_ident and orig_no > 0 and c.access_level = 0 and
    		      ((ei.early_age - li.late_age <= $interval_bound and li.late_age >= 20) or
    		      (ei.early_age - li.late_age <= 20 and li.late_age < 20) or
    		      (es.scale_no = 1 and es.scale_level in ($levels) and ei.early_age - li.late_age <= $epoch_bound) or
    		      (ls.scale_no = 1 and ls.scale_level in ($levels) and ei.early_age - li.late_age <= $epoch_bound) or
    		      (es.scale_no = 1 and es.scale_level = 3 and ei.early_age < 3) or
    		      (ls.scale_no = 1 and ls.scale_level = 3 and li.late_age >= 540))
    		GROUP BY orig_no) as u using (orig_no)
	SET first_early_age = u.fea,
	    first_late_age = u.fla,
	    last_early_age = u.lea,
	    last_late_age = u.lla";
    
    $count = $dbh->do($sql);
    
    # $count /= 2;	# we must divide by two to get the count of updated
    #                     # rows, see documentation for "ON DUPLICATE KEY UPDATE".
    
    logMessage(2, "      substituted more precise ages for $count taxa");
    
    # Then we need to go back and add in the taxa that are only known from
    # occurrences with non-precise ages (those for which all of the
    # occurrences were skipped by the above SQL statement).  For those which
    # already have the attributes filled in, we just increment the occurrence
    # count.
    
    # $sql = "	INSERT INTO $OCC_TAXON_WORK (orig_no, n_occs, n_colls,
    # 			first_early_age, first_late_age, last_early_age, last_late_age,
    # 			precise_age)
    # 		SELECT m.orig_no, count(*), 0,
    # 			max(ei.early_age), max(li.late_age), min(ei.early_age), min(li.late_age),
    # 			false
    # 		FROM $TABLE{OCCURRENCE_MATRIX} as m JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
    # 			JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
    # 			JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
    # 		WHERE m.latest_ident and m.orig_no > 0
    # 		GROUP BY m.orig_no
    # 		ON DUPLICATE KEY UPDATE n_occs = n_occs + 1";
    
    # $count = $dbh->do($sql);
    
    # logMessage(2, "      found $count taxa without precise ages");
    
    # Now that we have the age bounds for the first and last occurrence, we
    # can select a candidate first and last occurrence for each taxon (from
    # among all of the occurrences in the earliest/latest time interval in
    # which that taxon is recorded).
    
    logMessage(2, "    finding first and last occurrences...");
    
    $sql = "	UPDATE $OCC_TAXON_WORK as s JOIN $TABLE{OCCURRENCE_MATRIX} as o using (orig_no)
		SET s.early_occ = o.occurrence_no
		WHERE o.late_age >= s.first_late_age and latest_ident and access_level = 0";
    
    $count = doStmt($dbh, $sql, $options->{debug});
    
    $sql = "	UPDATE $OCC_TAXON_WORK as s JOIN $TABLE{OCCURRENCE_MATRIX} as o using (orig_no)
		SET s.late_occ = o.occurrence_no
		WHERE o.early_age <= s.last_early_age and latest_ident and access_level = 0";
    
    $count = doStmt($dbh, $sql, $options->{debug});
    
    # Then index the summary table by earliest and latest interval number, so
    # that we can quickly query for which taxa began or ended at a particular
    # time.
    
    # logMessage(2, "    indexing the summary table...");
    
    # $dbh->do("ALTER TABLE $OCC_TAXON_WORK ADD INDEX (first_early_age)");
    # $dbh->do("ALTER TABLE $OCC_TAXON_WORK ADD INDEX (first_late_age)");
    # $dbh->do("ALTER TABLE $OCC_TAXON_WORK ADD INDEX (last_early_age)");
    # $dbh->do("ALTER TABLE $OCC_TAXON_WORK ADD INDEX (last_late_age)");
    
    # Now swap in the new table.
    
    activateTables($dbh, $OCC_TAXON_WORK => $TABLE{OCCURRENCE_TAXON_SUMMARY});
    
    my $a = 1;	# we can stop here when debugging
}


# buildReferenceSummaryTable ( dbh, options )
# 
# Create a table summarizing the occurences by reference.

sub buildReferenceSummaryTable {
    
    my ($dbh, $tree_table, $options) = @_;
    
    my ($sql, $result, $count);
    
    $options ||= {};
    
    # We now summarize the occurrence matrix by reference_no.  For each
    # reference, we record the range of time periods it covers, plus the
    # number of occurrences and collections that refer to it.
    
    logMessage(2, "    summarizing by reference_no...");
    
    $result = doStmt($dbh, "DROP TABLE IF EXISTS $REF_SUMMARY_WORK", $options->{debug});
    
    $result = doStmt($dbh, "CREATE TABLE $REF_SUMMARY_WORK (
				reference_no int unsigned primary key,
				n_taxa int unsigned not null,
				n_class int unsigned not null,
				n_opinions int unsigned not null,
				n_occs int unsigned not null,
				n_colls int unsigned not null,
				n_prim int unsigned not null,
				early_age decimal(9,5),
				late_age decimal(9,5)) ENGINE=MyISAM", $options->{debug});
    
    $sql = "	INSERT INTO $REF_SUMMARY_WORK (reference_no, n_occs, n_colls,
			early_age, late_age)
		SELECT m.reference_no, count(*), count(distinct collection_no),
			max(ei.early_age), min(li.late_age)
		FROM $TABLE{OCCURRENCE_MATRIX} as m
			JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
		WHERE latest_ident and c.access_level = 0
		GROUP BY m.reference_no";
    
    $count = doStmt($dbh, $sql, $options->{debug});
    
    logMessage(2, "      $count references with occurrences");
    
    $sql = "	INSERT IGNORE INTO $REF_SUMMARY_WORK (reference_no)
		SELECT reference_no FROM refs";
    
    $count = doStmt($dbh, $sql, $options->{debug});
    
    logMessage(2, "      $count references without occurrences");
    
    # Count primary references from collections.
    
    logMessage(2, "    counting primary references...");
    
    $sql = "	UPDATE $REF_SUMMARY_WORK as rs join
		       (SELECT reference_no, count(*) as count from $TABLE{COLLECTION_MATRIX}
			GROUP BY reference_no) as m using (reference_no)
		SET rs.n_prim = m.count";
    
    $count = doStmt($dbh, $sql, $options->{debug});
    
    # Then index the reference summary table by numbers of collections and
    # occurrences, so that we can quickly query for the most heavily used ones.
    
    # logMessage(2, "    indexing the summary table...");
    
    # $result = $dbh->do("ALTER TABLE $REF_SUMMARY_WORK ADD INDEX (early_age, late_age)");
    
    # Now swap in the new table.
    
    activateTables($dbh, $REF_SUMMARY_WORK => $TABLE{OCCURRENCE_REF_SUMMARY});
    
    my $a = 1;		# we can stop here when debugging.
}


# buildTaxonCollectionsTable ( dbh, tree_table, options )
#
# Create a table that counts the number of collections corresponding to a subset of taxa. The ones
# that are counted are those of rank genus, family, order, class, phylum.

sub buildTaxonCollectionsTable {

    my ($dbh, $tree_table, $options) = @_;
    
    $options ||= { };

    my $TREE_TABLE = $tree_table;
    my $INTS_TABLE = $TAXON_TABLE{$tree_table}{ints};
    my $LOWER_TABLE = $TAXON_TABLE{$tree_table}{lower};
    
    my ($sql, $result);
    
    logMessage(2, "    counting collections for taxa up to rank 23...");
    
    $result = doStmt($dbh, "DROP TABLE IF EXISTS $TAXON_COLLS_WORK", $options->{debug});
    
    $result = doStmt($dbh, "CREATE TABLE $TAXON_COLLS_WORK (
    				orig_no int unsigned PRIMARY KEY,
    				rank tinyint unsigned not null,
    				n_taxa int unsigned not null default 0,
    				n_occs int unsigned not null default 0,
				n_occs_mar int unsigned not null default 0,
				n_occs_ter int unsigned not null default 0,
    				n_colls int unsigned not null default 0,
				n_colls_mar int unsigned not null default 0,
				n_colls_ter int unsigned not null default 0) ENGINE=MyISAM",
    		     $options->{debug});
    
    $sql = "INSERT IGNORE INTO $TAXON_COLLS_WORK (orig_no, rank, n_taxa, 
		n_occs, n_occs_mar, n_occs_ter, n_colls, n_colls_mar, n_colls_ter)
    	SELECT base.orig_no, base.rank, count(distinct t.orig_no), 
		count(*), count(if(marine=1,1,null)), count(if(marine=0,1,null)),
    		count(distinct collection_no), 
		count(distinct if(marine=1,collection_no,null)),
		count(distinct if(marine=0,collection_no,null))
    	FROM $TREE_TABLE as base 
		STRAIGHT_JOIN $TREE_TABLE as t on t.lft between base.lft and base.rgt
    		STRAIGHT_JOIN $TABLE{OCCURRENCE_MATRIX} as o on o.orig_no = t.orig_no
    		JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
    	WHERE latest_ident and c.access_level = 0 and base.max_rank <= 23
    	GROUP by base.orig_no";
    
    $result = doStmt($dbh, $sql, $options->{debug});
    
    logMessage(2, "      added $result taxa");
    
    logMessage(2, "    counting collections for taxa of rank 26...");
    
    $sql = "INSERT IGNORE INTO $TAXON_COLLS_WORK (orig_no, rank, n_taxa, 
		n_occs, n_occs_mar, n_occs_ter, n_colls, n_colls_mar, n_colls_ter)
    	SELECT base.orig_no, base.rank, count(distinct t.orig_no), 
		count(*), count(if(marine=1,1,null)), count(if(marine=0,1,null)),
    		count(distinct collection_no), 
		count(distinct if(marine=1,collection_no,null)),
		count(distinct if(marine=0,collection_no,null)),
    	FROM $TREE_TABLE as base 
		STRAIGHT_JOIN $TREE_TABLE as t on t.lft between base.lft and base.rgt
    		STRAIGHT_JOIN $TABLE{OCCURRENCE_MATRIX} as o on o.orig_no = t.orig_no
    		JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
     	WHERE latest_ident and c.access_level = 0 and base.rank = 26
    	GROUP by base.orig_no";
    
    $result = doStmt($dbh, $sql, $options->{debug});
    
    logMessage(2, "      added $result taxa");
    
    activateTables($dbh, $TAXON_COLLS_WORK => $TABLE{TAXON_COLLECTION_COUNTS});
    
    # $sql = "INSERT INTO $TAXON_COLLS_WORK (orig_no, rank, n_taxa, n_occs, n_colls)
    # 	SELECT base.orig_no, base.rank, count(distinct t.orig_no), count(*),
    # 		count(distinct collection_no)
    # 	FROM $TREE_TABLE as base JOIN $TREE_TABLE as t on t.lft between base.lft and base.rgt
    # 		STRAIGHT_JOIN $TABLE{OCCURRENCE_MATRIX} as o on o.orig_no = t.orig_no
    # 		JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
    # 	WHERE latest_ident and access_level = 0 and base.rank = $TAXON_RANK{genus}
    # 	GROUP by base.orig_no";
    
    # $result = doStmt($dbh, $sql, $options->{debug});
    
    # logMessage(2, "      added $result genera");
    
    # logMessage(2, "      counting distinct collections by family...");
    
    # # Start by inserting records for all families
    
    # $sql = "INSERT INTO $TAXON_COLLS_WORK (orig_no, rank, n_taxa, n_occs, n_colls)
    # 	SELECT base.orig_no, base.rank, count(distinct t.orig_no), count(*),
    # 		count(distinct collection_no)
    # 	FROM $TREE_TABLE as base JOIN $TREE_TABLE as t on t.lft between base.lft and base.rgt
    # 		STRAIGHT_JOIN $TABLE{OCCURRENCE_MATRIX} as o on o.orig_no = t.orig_no
    # 		JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
    # 	WHERE latest_ident and access_level = 0 and base.rank = $TAXON_RANK{family}
    # 	GROUP by base.orig_no";
    
    # $result = doStmt($dbh, $sql, $options->{debug});
    
    # logMessage(2, "      added $result families");
    
    # # Add other taxa that are counted as families because of their name but are not ranked as
    # # families.
    
    # $sql = "INSERT INTO $TAXON_COLLS_WORK (orig_no, rank, n_taxa, n_occs, n_colls)
    # 	SELECT base.orig_no, base.rank, count(distinct t.orig_no), count(*),
    # 		count(distinct collection_no)
    # 	FROM $INTS_TABLE as ph JOIN $TREE_TABLE as base on base.orig_no = ph.family_no
    # 			and base.rank <> $TAXON_RANK{family}
    # 		JOIN $TREE_TABLE as t on t.lft between base.lft and base.rgt
    # 		STRAIGHT_JOIN $TABLE{OCCURRENCE_MATRIX} as o on o.orig_no = t.orig_no
    # 		JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
    # 	WHERE latest_ident and access_level = 0
    # 	GROUP by base.orig_no";
    
    # $result = doStmt($dbh, $sql, $options->{debug});
    
    # logMessage(2, "      added $result other taxa");
    
    # # logMessage(2, "        alternate method...");
    
    # # $sql = "INSERT INTO $TCN2 (orig_no, rank, n_taxa, n_occs, n_colls)
    # # 	SELECT base.orig_no, base.rank, count(distinct t.orig_no), count(*),
    # # 		count(distinct collection_no)
    # # 	FROM $TREE_TABLE as base JOIN $INTS_TABLE as ph on ph.family_no = base.orig_no
    # # 		JOIN $TREE_TABLE as t on t.ints_no = ph.ints_no
    # # 		JOIN $TABLE{OCCURRENCE_MATRIX} as o on o.orig_no = t.orig_no
    # # 		JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
    # # 	WHERE latest_ident and access_level = 0 and base.rank = $TAXON_RANK{family} and
    # # 		base.orig_no = base.accepted_no
    # # 	GROUP by base.orig_no";
    
    # # doStmt($dbh, $sql, $options->{debug});
    
    # logMessage(2, "      counting distinct collections by order...");

    # $sql = "INSERT INTO $TAXON_COLLS_WORK (orig_no, rank, n_taxa, n_occs, n_colls)
    # 	SELECT base.orig_no, base.rank, count(distinct t.orig_no), count(*),
    # 		count(distinct collection_no)
    # 	FROM $TREE_TABLE as base JOIN $TREE_TABLE as t on t.lft between base.lft and base.rgt
    # 		STRAIGHT_JOIN $TABLE{OCCURRENCE_MATRIX} as o on o.orig_no = t.orig_no
    # 		JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
    # 	WHERE latest_ident and access_level = 0 and base.rank = $TAXON_RANK{order}
    # 	GROUP by base.orig_no";
    
    # $result = doStmt($dbh, $sql, $options->{debug});
    
    # logMessage(2, "      added $result orders");
    
    # logMessage(2, "      counting distinct collections by class...");
    
    # $sql = "INSERT INTO $TAXON_COLLS_WORK (orig_no, rank, n_taxa, n_occs, n_colls)
    # 	SELECT base.orig_no, base.rank, count(distinct t.orig_no), count(*),
    # 		count(distinct collection_no)
    # 	FROM $TREE_TABLE as base JOIN $TREE_TABLE as t on t.lft between base.lft and base.rgt
    # 		STRAIGHT_JOIN $TABLE{OCCURRENCE_MATRIX} as o on o.orig_no = t.orig_no
    # 		JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
    # 	WHERE latest_ident and access_level = 0 and base.rank = $TAXON_RANK{class}
    # 	GROUP by base.orig_no";
    
    # $result = doStmt($dbh, $sql, $options->{debug});
    
    # logMessage(2, "      added $result classes");
    
    # logMessage(2, "      counting distinct collections by phylum...");
    
    # $sql = "INSERT INTO $TAXON_COLLS_WORK (orig_no, rank, n_taxa, n_occs, n_colls)
    # 	SELECT base.orig_no, base.rank, count(distinct t.orig_no), count(*),
    # 		count(distinct collection_no)
    # 	FROM $TREE_TABLE as base JOIN $TREE_TABLE as t on t.lft between base.lft and base.rgt
    # 		STRAIGHT_JOIN $TABLE{OCCURRENCE_MATRIX} as o on o.orig_no = t.orig_no
    # 		JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
    # 	WHERE latest_ident and access_level = 0 and base.rank = $TAXON_RANK{phylum}
    # 	GROUP by base.orig_no";
    
    # $result = doStmt($dbh, $sql, $options->{debug});
    
    # logMessage(2, "      added $result phyla");
    
}


# buildOccIntervalMaps ( dbh, options )
# 
# Create tables that map each distinct age range from the occurrence table to
# the set of intervals from the various scales that encompass it.  This will
# be useful for diversity calculations, among other things.

sub buildOccIntervalMaps {
    
    my ($dbh) = @_;
    
    my ($sql, $result, $count);
    
    logMessage(2, "    creating occurrence interval maps...");
    
    # $dbh->do("DROP TABLE IF EXISTS $OCC_CONTAINED_MAP");
    
    # $dbh->do("
    # 	CREATE TABLE $OCC_CONTAINED_MAP (
    # 		scale_no smallint unsigned not null,
    # 		scale_level smallint unsigned not null,
    # 		early_age decimal(9,5),
    # 		late_age decimal(9,5),
    # 		interval_no int unsigned not null,
    # 		PRIMARY KEY (early_age, late_age, scale_no, scale_level, interval_no)) Engine=MyISAM");
    
    # $sql = "
    # 	INSERT INTO $OCC_CONTAINED_MAP (scale_no, scale_level, early_age, late_age, interval_no)
    # 	SELECT m.scale_no, m.scale_level, i.early_age, i.late_age, m.interval_no
    # 	FROM (SELECT distinct early_age, late_age FROM $TABLE{OCCURRENCE_MATRIX}) as i
    # 		JOIN (SELECT scale_no, scale_level, early_age, late_age, interval_no
    # 		      FROM $TABLE{SCALE_MAP} JOIN $TABLE{INTERVAL_DATA} using (interval_no)) as m
    # 	WHERE m.late_age >= i.late_age and m.early_age <= i.early_age";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      generated $result rows with container rule");
    
    # $dbh->do("DROP TABLE IF EXISTS $OCC_MAJOR_MAP");
    
    # $dbh->do("
    # 	CREATE TABLE $OCC_MAJOR_MAP (
    # 		scale_no smallint unsigned not null,
    # 		scale_level smallint unsigned not null,
    # 		early_age decimal(9,5),
    # 		late_age decimal(9,5),
    # 		interval_no int unsigned not null,
    # 		PRIMARY KEY (early_age, late_age, scale_no, scale_level, interval_no)) Engine=MyISAM");
    
    # $sql = "
    # 	INSERT INTO $OCC_MAJOR_MAP (scale_no, scale_level, early_age, late_age, interval_no)
    # 	SELECT m.scale_no, m.scale_level, i.early_age, i.late_age, m.interval_no
    # 	FROM (SELECT distinct early_age, late_age FROM $TABLE{OCCURRENCE_MATRIX}) as i
    # 		JOIN (SELECT scale_no, scale_level, early_age, late_age, interval_no
    # 		      FROM $TABLE{SCALE_MAP} JOIN $TABLE{INTERVAL_DATA} using (interval_no)) as m
    # 	WHERE i.early_age > i.late_age and
    # 		if(i.late_age >= m.late_age,
    # 			if(i.early_age <= m.early_age, i.early_age - i.late_age, m.early_age - i.late_age),
    # 			if(i.early_age > m.early_age, m.early_age - m.late_age, i.early_age - m.late_age)) / (i.early_age - i.late_age) >= 0.5";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      generated $result rows with majority rule");
    
    # $dbh->do("DROP TABLE IF EXISTS $OCC_BUFFER_MAP");
    
    # $dbh->do("
    # 	CREATE TABLE $OCC_BUFFER_MAP (
    # 		scale_no smallint unsigned not null,
    # 		scale_level smallint unsigned not null,
    # 		early_age decimal(9,5),
    # 		late_age decimal(9,5),
    # 		interval_no int unsigned not null,
    # 		PRIMARY KEY (early_age, late_age, scale_no, scale_level, interval_no)) Engine=MyISAM");
    
    # $sql = "
    # 	INSERT INTO $OCC_BUFFER_MAP (scale_no, scale_level, early_age, late_age, interval_no)
    # 	SELECT m.scale_no, m.scale_level, i.early_age, i.late_age, m.interval_no
    # 	FROM (SELECT distinct early_age, late_age FROM $TABLE{OCCURRENCE_MATRIX}) as i
    # 		JOIN (SELECT scale_no, scale_level, early_age, late_age, interval_no
    # 		      FROM $TABLE{SCALE_MAP} JOIN $TABLE{INTERVAL_DATA} using (interval_no)) as m
    # 	WHERE m.late_age < i.early_age and m.early_age > i.late_age and
    # 		i.early_age <= m.early_age + if(i.early_age > 66, 12, 5) and
    # 		i.late_age >= m.late_age - if(i.late_age >= 66, 12, 5)";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      generated $result rows with buffer rule");
    
    # $dbh->do("DROP TABLE IF EXISTS $OCC_OVERLAP_MAP");
    
    # $dbh->do("
    # 	CREATE TABLE $OCC_OVERLAP_MAP (
    # 		scale_no smallint unsigned not null,
    # 		scale_level smallint unsigned not null,
    # 		early_age decimal(9,5),
    # 		late_age decimal(9,5),
    # 		interval_no int unsigned not null,
    # 		PRIMARY KEY (early_age, late_age, scale_no, scale_level, interval_no)) Engine=MyISAM");
    
    # $sql = "
    # 	INSERT INTO $OCC_OVERLAP_MAP (scale_no, scale_level, early_age, late_age, interval_no)
    # 	SELECT m.scale_no, m.scale_level, i.early_age, i.late_age, m.interval_no
    # 	FROM (SELECT distinct early_age, late_age FROM $TABLE{OCCURRENCE_MATRIX}) as i
    # 		JOIN (SELECT scale_no, scale_level, early_age, late_age, interval_no
    # 		      FROM $TABLE{SCALE_MAP} JOIN $TABLE{INTERVAL_DATA} using (interval_no)) as m
    # 	WHERE m.late_age < i.early_age and m.early_age > i.late_age";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      generated $result rows with overlap rule");
}


1;
