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

use CoreFunction qw(activateTables);
use TableDefs qw(%TABLE);
use IntervalBase qw(INTL_SCALE BIN_SCALE);
use TaxonDefs qw(@TREE_TABLE_LIST %TAXON_TABLE %TAXON_RANK);

use CoreTableDefs;
use TaxonDefs qw(@TREE_TABLE_LIST);
use ConsoleLog qw(logMessage logTimestamp);

our (@EXPORT_OK) = qw(buildOccurrenceTables buildTaxonSummaryTable buildDiversityTables 
		      updateOccurrenceMatrix buildOccIntervalMaps);
		      
our $OCC_MATRIX_WORK = "omn";
our $OCC_TAXON_WORK = "otn";
our $PRECISE_AGE_AUX = "pan";
our $REF_SUMMARY_WORK = "orn";
our $INT_SUMMARY_WORK = "oin";
our $INT_SUMMARY_AUX = "oia";
our $TS_SUMMARY_WORK = "otsn";
our $PVL_COLLS_WORK = "pvc";


# buildOccurrenceTables ( dbh )
# 
# Build the occurrence matrix, recording which taxonomic concepts are
# associated with which collections in which geological and chronological
# locations.  This table is used to satisfy the bulk of the queries from the
# front-end application.  This function also builds an occurrence summary
# table, summarizing occurrence information by taxon.

sub buildOccurrenceTables {
    
    my ($dbh, $options) = @_;
    
    my ($sql, $result, $count, $extra);
    
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
				subgenus_reso varchar(255) not null default '',
				species_name varchar(255) not null default '',
				species_reso varchar(255) not null default '',
				subspecies_name varchar(255) not null default '',
				subspecies_reso varchar(255) not null default '',
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
			species_name, species_reso, subspecies_name, subspecies_reso,
			plant_organ, plant_organ2,
			early_age, late_age, reference_no,
			authorizer_no, enterer_no, modifier_no, created, modified)
		SELECT o.occurrence_no, 0, true, o.collection_no, o.taxon_no, a.orig_no, 
			o.genus_name, o.genus_reso, o.subgenus_name, o.subgenus_reso,
			o.species_name, o.species_reso, o.subspecies_name, o.subspecies_reso,
			o.plant_organ, o.plant_organ2,
			ei.early_age, li.late_age,
			if(o.reference_no > 0, o.reference_no, c.reference_no),
			o.authorizer_no, o.enterer_no, o.modifier_no, o.created, o.modified
		FROM occurrences as o JOIN coll_matrix as c using (collection_no)
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
			LEFT JOIN authorities as a using (taxon_no)";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count occurrences");
    
    # Then add one row for every reidentification in the database.
    
    $sql = "	INSERT INTO $OCC_MATRIX_WORK
		       (occurrence_no, reid_no, latest_ident, collection_no, taxon_no, orig_no,
			genus_name, genus_reso, subgenus_name, subgenus_reso,
			species_name, species_reso, plant_organ,
			early_age, late_age, reference_no,
			authorizer_no, enterer_no, modifier_no, created, modified)
		SELECT re.occurrence_no, re.reid_no, if(re.most_recent = 'YES', 1, 0), re.collection_no, re.taxon_no, a.orig_no, 
			re.genus_name, re.genus_reso, re.subgenus_name, re.subgenus_reso,
			re.species_name, re.species_reso, re.plant_organ,
			ei.early_age, li.late_age, re.reference_no,
			re.authorizer_no, re.enterer_no, re.modifier_no, re.created, re.modified
		FROM reidentifications as re JOIN coll_matrix as c using (collection_no)
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
			LEFT JOIN authorities as a using (taxon_no)";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count re-identifications");
    
    # For each reidentification, the corresponding record in occ_matrix drawn
    # from the original occurrence record needs to be "de-selected".
    
    logMessage(2, "    marking superceded identifications...");
    
    $sql = "	UPDATE $OCC_MATRIX_WORK as m
			JOIN reidentifications as re on re.occurrence_no = m.occurrence_no 
				and re.most_recent = 'YES'
		SET m.latest_ident = false WHERE m.reid_no = 0";
    
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
    
    buildTaxonSummaryTable($dbh, $options);
    buildReferenceSummaryTable($dbh, $options);
    buildOccIntervalMaps($dbh, $options);
    
    $dbh->do("REPLACE INTO last_build (name) values ('occurrences')");
    
    my $a = 1;	# we can stop here when debugging
}


# updateOccurrenceMatrix ( dbh, occurrence_no )
# 
# Update one entry in the occurrence matrix, to reflect any changes in the
# specified occurrence.

sub updateOccurrenceMatrix {

    my ($dbh, $occurrence_no) = @_;
    
    my ($sql, $count, $extra);
    
    # First replace the main occurrence record
    
    logMessage(2, "    updating occurrence...");
    
    $sql = "	REPLACE INTO $TABLE{OCCURRENCE_MATRIX}
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
		FROM occurrences as o JOIN coll_matrix as c using (collection_no)
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
			LEFT JOIN authorities as a using (taxon_no)
		WHERE occurrence_no = $occurrence_no";
    
    $count = $dbh->do($sql);
    
    # Then replace any reidentifications
    
    $sql = "	REPLACE INTO $TABLE{OCCURRENCE_MATRIX}
		       (occurrence_no, reid_no, latest_ident, collection_no, taxon_no, orig_no,
			genus_name, genus_reso, subgenus_name, subgenus_reso,
			species_name, species_reso, plant_organ,
			early_age, late_age, reference_no,
			authorizer_no, enterer_no, modifier_no, created, modified)
		SELECT re.occurrence_no, re.reid_no, if(re.most_recent = 'YES', 1, 0), re.collection_no, re.taxon_no, a.orig_no, 
			re.genus_name, re.genus_reso, re.subgenus_name, re.subgenus_reso,
			re.species_name, re.species_reso, re.plant_organ,
			ei.early_age, li.late_age, re.reference_no,
			re.authorizer_no, re.enterer_no, re.modifier_no, re.created, re.modified
		FROM reidentifications as re JOIN coll_matrix as c using (collection_no)
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
			LEFT JOIN authorities as a using (taxon_no)
		WHERE occurrence_no = $occurrence_no and reid_no > 0";
    
    $count = $dbh->do($sql);
    
    # Now make sure that superceded identifications are marked

    $sql = "	UPDATE $TABLE{OCCURRENCE_MATRIX} as m
			JOIN reidentifications as re on re.occurrence_no = m.occurrence_no 
				and re.most_recent = 'YES'
		SET m.latest_ident = false WHERE m.occurrence_no = $occurrence_no and m.reid_no = 0";
    
    $count = $dbh->do($sql);
    
    my $a = 1;	# we can stop here when debugging
}


# Buildtaxonsummarytable ( dbh, options )
# 
# Create a table to summarize the occurrences by taxon.

sub buildTaxonSummaryTable {

    my ($dbh, $options) = @_;
    
    my ($sql, $result, $count);
    
    $options ||= {};
    
    # Create working tables which will become the new taxon summary
    # table and reference summary table.
    
    logMessage(2, "    summarizing by taxonomic concept...");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $OCC_TAXON_WORK");
    $result = $dbh->do("CREATE TABLE $OCC_TAXON_WORK (
				orig_no int unsigned primary key,
				n_occs int unsigned not null,
				n_colls int unsigned not null,
				first_early_age decimal(9,5),
				first_late_age decimal(9,5),
				last_early_age decimal(9,5),
				last_late_age decimal(9,5),
				precise_age boolean default false,
				early_occ int unsigned,
				late_occ int unsigned) ENGINE=MyISAM");
    
    # Fill in this table with numbers of occurrences, and age bounds for first
    # and last appearance crudely calculated as the minimum and maximum of the
    # age ranges of the individual occurrences of each taxon.
    
    $sql = "	INSERT INTO $OCC_TAXON_WORK (orig_no, n_occs, n_colls,
			first_early_age, first_late_age, last_early_age, last_late_age,
			precise_age)
		SELECT orig_no, count(*), 0,
			max(ei.early_age), max(li.late_age), min(ei.early_age), min(li.late_age),
			false
		FROM $TABLE{OCCURRENCE_MATRIX} as m 
			JOIN $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
		WHERE latest_ident and access_level = 0 and orig_no > 0
		GROUP BY orig_no";
    
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
    
    $result = $dbh->do("DROP TABLE IF EXISTS $PRECISE_AGE_AUX");
    $result = $dbh->do("CREATE TABLE $PRECISE_AGE_AUX (
				early_int_no int unsigned not null,
				late_int_no int unsigned not null,
				early_age decimal(9,5),
				late_age decimal(9,5),
				precise boolean,
				KEY (early_int_no, late_int_no)) ENGINE=MyISAM");
    
    $sql = "	INSERT INTO $PRECISE_AGE_AUX (early_int_no, late_int_no, early_age, late_age, precise)
		SELECT distinct early_int_no, late_int_no, ei.early_age, li.late_age,
		    ((ei.early_age <= 20 and ei.early_age - li.late_age <= 5) or 
		     (ei.early_age <= 66 and ei.early_age - li.late_age <= 20) or 
		     (ei.early_age <= 252 and ei.early_age - li.late_age <= 25) or
		     (ei.early_age <= 540 and ei.early_age - li.late_age <= 30) or 
		     (ei.early_age > 540))
		FROM $TABLE{OCCURRENCE_MATRIX} as m 
			join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			join $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			join $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "       computed precise age table");
    
    $sql = "	INSERT INTO $OCC_TAXON_WORK (orig_no, n_occs, n_colls,
			first_early_age, first_late_age, last_early_age, last_late_age,
			precise_age)
		SELECT m.orig_no, count(*), 0,
			max(pa.early_age) as fea, max(pa.late_age) as lea,
			min(pa.early_age) as fla, min(pa.late_age) as lla,
			true
		FROM $TABLE{OCCURRENCE_MATRIX} as m 
			join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			join $PRECISE_AGE_AUX as pa using (early_int_no, late_int_no)
		WHERE m.latest_ident and m.orig_no > 0 and pa.precise
		GROUP BY m.orig_no
		ON DUPLICATE KEY UPDATE
			first_early_age = values(first_early_age),
			first_late_age = values(first_late_age),
			last_early_age = values(last_early_age),
			last_late_age = values(last_late_age),
			precise_age = true";
    
    $count = $dbh->do($sql);
    
    $count /= 2;	# we must divide by two to get the count of updated
                        # rows, see documentation for "ON DUPLICATE KEY UPDATE".
    
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
		SET s.early_occ = o.occurrence_no WHERE o.late_age >= s.first_late_age and latest_ident";
    
    $count = $dbh->do($sql);
    
    $sql = "	UPDATE $OCC_TAXON_WORK as s JOIN $TABLE{OCCURRENCE_MATRIX} as o using (orig_no)
		SET s.late_occ = o.occurrence_no WHERE o.early_age <= s.last_early_age and latest_ident";
    
    $count = $dbh->do($sql);
    
    # Then index the summary table by earliest and latest interval number, so
    # that we can quickly query for which taxa began or ended at a particular
    # time.
    
    logMessage(2, "    indexing the summary table...");
    
    $dbh->do("ALTER TABLE $OCC_TAXON_WORK ADD INDEX (first_early_age)");
    $dbh->do("ALTER TABLE $OCC_TAXON_WORK ADD INDEX (first_late_age)");
    $dbh->do("ALTER TABLE $OCC_TAXON_WORK ADD INDEX (last_early_age)");
    $dbh->do("ALTER TABLE $OCC_TAXON_WORK ADD INDEX (last_late_age)");
    
    # Now swap in the new table.
    
    activateTables($dbh, $OCC_TAXON_WORK => $TABLE{OCC_TAXON_SUMMARY});
    
    # # Now create a prevalence table for collections.
    
    # my $TREE_TABLE = 'taxon_trees';
    # my $INTS_TABLE = $TAXON_TABLE{$TREE_TABLE}{ints};
    
    # logMessage(2, "   generating collection prevalence matrix...");
    
    # $dbh->do("CREATE TABLE $PVL_COLLS_WORK (
    #            collection_no int unsigned not null default '0',
    #            order_no int unsigned,
    #            class_no int unsigned,
    #            phylum_no int unsigned,
    #            n_occs int unsigned not null default '0') Engine=MyISAM");
    
    # $sql = "
    #     INSERT INTO $PVL_COLLS_WORK (collection_no, order_no, class_no, phylum_no, n_occs)
    #     SELECT collection_no, order_no, class_no, phylum_no, count(*)
    #     FROM $OCC_MATRIX as o JOIN $TREE_TABLE as t using (orig_no)
    #             JOIN $INTS_TABLE as ph using (ints_no)
    #     WHERE o.latest_ident = 1 and (order_no <> 0 or class_no <> 0 or phylum_no <> 0)
    #     GROUP BY collection_no, order_no, class_no, phylum_no";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      generated $result rows.");
    
    # activateTables($dbh, $PVL_COLLS_WORK => $PVL_COLLS);
    
    my $a = 1;	# we can stop here when debugging
}


# buildReferenceSummaryTable ( dbh, options )
# 
# Create a table summarizing the occurences by reference.

sub buildReferenceSummaryTable {
    
    my ($dbh, $options) = @_;
    
    my ($sql, $result, $count);
    
    $options ||= {};
    
    # We now summarize the occurrence matrix by reference_no.  For each
    # reference, we record the range of time periods it covers, plus the
    # number of occurrences and collections that refer to it.
    
    logMessage(2, "    summarizing by reference_no...");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $REF_SUMMARY_WORK");
    $result = $dbh->do("CREATE TABLE $REF_SUMMARY_WORK (
				reference_no int unsigned primary key,
				n_taxa int unsigned not null,
				n_class int unsigned not null,
				n_opinions int unsigned not null,
				n_occs int unsigned not null,
				n_colls int unsigned not null,
				n_prim int unsigned not null,
				early_age decimal(9,5),
				late_age decimal(9,5)) ENGINE=MyISAM");
    
    $sql = "	INSERT INTO $REF_SUMMARY_WORK (reference_no, n_occs, n_colls,
			early_age, late_age)
		SELECT m.reference_no, count(*), count(distinct collection_no),
			max(ei.early_age), min(li.late_age)
		FROM $TABLE{OCCURRENCE_MATRIX} as m 
			join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
			join $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			join $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
		WHERE latest_ident
		GROUP BY m.reference_no";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count references with occurrences");
    
    $sql = "	INSERT IGNORE INTO $REF_SUMMARY_WORK (reference_no)
		SELECT reference_no FROM refs";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count references without occurrences");
    
    # Count primary references from collections.
    
    logMessage(2, "    counting primary references...");
    
    $sql = "	UPDATE $REF_SUMMARY_WORK as rs join
		       (SELECT reference_no, count(*) as count from $TABLE{COLLECTION_MATRIX}
			GROUP BY reference_no) as m using (reference_no)
		SET rs.n_prim = m.count";
    
    $result = $dbh->do($sql);
    
    # Then index the reference summary table by numbers of collections and
    # occurrences, so that we can quickly query for the most heavily used ones.
    
    logMessage(2, "    indexing the summary table...");
    
    $result = $dbh->do("ALTER TABLE $REF_SUMMARY_WORK ADD INDEX (early_age, late_age)");
    
    # Now swap in the new table.
    
    activateTables($dbh, $REF_SUMMARY_WORK => $TABLE{OCC_REF_SUMMARY});
    
    my $a = 1;		# we can stop here when debugging.
}


# buildOccIntervalMaps ( dbh, options )
# 
# Create tables that map each distinct age range from the occurrence table to
# the set of intervals from the various scales that encompass it.  This will
# be useful for diversity calculations, among other things.

sub buildOccIntervalMaps {
    
    my ($dbh) = @_;
    
    my ($sql, $result, $count);
    
    # logMessage(2, "    creating occurrence interval maps...");
    
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
    # 		JOIN (SELECT sm.scale_no, scale_level, early_age, late_age, interval_no
    # 		      FROM $TABLE{SCALE_MAP} as sm JOIN $TABLE{INTERVAL_DATA} using (interval_no)) as m
    # 	WHERE m.late_age >= i.late_age and m.early_age <= i.early_age";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      generated $result rows with container rule");
    
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
    # 		JOIN (SELECT sm.scale_no, scale_level, early_age, late_age, interval_no
    # 		      FROM $TABLE{SCALE_MAP} as sm JOIN $TABLE{INTERVAL_DATA} using (interval_no)
    # 		      WHERE sm.scale_no = 1) as m
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
    
    $dbh->do("DROP TABLE IF EXISTS $INT_SUMMARY_AUX");
    
    $dbh->do("CREATE TABLE $INT_SUMMARY_AUX (
		early_age decimal(9,5) not null,
		late_age decimal(9,5) not null,
		n_colls int unsigned not null default '0',
		n_occs int unsigned not null default '0') Engine=Aria");
    
    logMessage(2, "    creating int summary...");
    
    $sql = "INSERT INTO $INT_SUMMARY_AUX
	    SELECT early_age, late_age, count(*), sum(n_occs)
	    FROM $TABLE{COLLECTION_MATRIX}
	    GROUP BY early_age, late_age";
    
    $result = $dbh->do($sql);

    $dbh->do("DROP TABLE IF EXISTS $INT_SUMMARY_WORK");
    
    $dbh->do("CREATE TABLE $INT_SUMMARY_WORK (
		interval_no int unsigned PRIMARY KEY,
		scale_no int unsigned not null,
		colls_defined int unsigned not null default '0',
		occs_defined int unsigned not null default '0',
		colls_contained int unsigned not null default '0',
		occs_contained int unsigned not null default '0',
		colls_major int unsigned not null default '0',
		occs_major int unsigned not null default '0',
		KEY (scale_no)) Engine=Aria");
    
    $sql = "INSERT INTO $INT_SUMMARY_WORK
	    SELECT interval_no, scale_no, 0, 0, 0, 0, 0, 0
	    FROM $TABLE{INTERVAL_DATA}";
    
    $result = $dbh->do($sql);
    
    $sql = "UPDATE $INT_SUMMARY_WORK as s
	      join (SELECT interval_no, sum(n_colls) as n_colls, sum(n_occs) as n_occs
		    FROM $INT_SUMMARY_AUX as m join $TABLE{INTERVAL_DATA} as i 
		    WHERE m.early_age <= i.early_age and m.late_age >= i.late_age
		    GROUP BY interval_no) as u
	      using (interval_no)
	    SET s.colls_contained = u.n_colls,
		s.occs_contained = u.n_occs";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      found $result intervals with timerule=contained collections");
    
    $sql = "UPDATE $INT_SUMMARY_WORK as s
	      join (SELECT interval_no, sum(n_colls) as n_colls, sum(n_occs) as n_occs
		    FROM $INT_SUMMARY_AUX as m join $TABLE{INTERVAL_DATA} as i
		    WHERE m.early_age > m.late_age and if(m.late_age >= i.late_age, 
		      if(m.early_age <= i.early_age, m.early_age - m.late_age, i.early_age - m.late_age), 
		      if(m.early_age > i.early_age, i.early_age - i.late_age, m.early_age - i.late_age)) / 
			(m.early_age - m.late_age) >= 0.5
		    GROUP BY interval_no) as u
	      using (interval_no)
	    SET s.colls_major = u.n_colls,
		s.occs_major = u.n_occs";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      found $result intervals with timerule=major collections");
    
    $sql = "UPDATE $INT_SUMMARY_WORK as s
	      join (SELECT interval_no, count(distinct collection_no) as n_colls
		    FROM $TABLE{COLLECTION_DATA} as cc 
			join $TABLE{INTERVAL_DATA} as i on
			    cc.max_interval_no = i.interval_no or
			    cc.min_interval_no = i.interval_no or
			    cc.ma_interval_no = i.interval_no
		    GROUP BY interval_no) as u
	      using (interval_no)
	    SET s.colls_defined = u.n_colls";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      found $result intervals with defined collections");
    
    $sql = "UPDATE $INT_SUMMARY_WORK as s
	      join (SELECT interval_no, count(distinct collection_no) as n_colls
		    FROM $TABLE{COLLECTION_DATA} as cc 
			join $TABLE{INTERVAL_DATA} as i on
			    cc.max_interval_no = i.interval_no or
			    cc.min_interval_no = i.interval_no or
			    cc.ma_interval_no = i.interval_no
		    GROUP BY interval_no) as u
	      using (interval_no)
	    SET s.colls_defined = u.n_colls";
    
    $result = $dbh->do($sql);
    
    $dbh->do("DROP TABLE IF EXISTS $TS_SUMMARY_WORK");
    
    $dbh->do("CREATE TABLE $TS_SUMMARY_WORK (
		scale_no int unsigned PRIMARY KEY,
		colls_defined int unsigned not null default '0',
		occs_defined int unsigned not null default '0') Engine=Aria");
    
    $sql = "INSERT INTO $TS_SUMMARY_WORK (scale_no, colls_defined)
	    SELECT scale_no, count(distinct collection_no) as colls_defined
	    FROM $TABLE{COLLECTION_DATA} as cc 
		join $TABLE{INTERVAL_DATA} as i on
		    cc.max_interval_no = i.interval_no or
		    cc.min_interval_no = i.interval_no or
		    cc.ma_interval_no = i.interval_no
	    GROUP BY scale_no";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      found $result timescales with defined collections");    
    
    # Now swap in the new tables.
    
    activateTables($dbh, $INT_SUMMARY_WORK => $TABLE{OCC_INT_SUMMARY},
		  $TS_SUMMARY_WORK => $TABLE{OCC_TS_SUMMARY});
}

1;
