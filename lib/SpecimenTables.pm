# 
# The Paleobiology Database
# 
#   SpecimenTables.pm
# 
# Build the tables needed by the data service for satisfying queries about
# specimens.


package SpecimenTables;

use strict;

use base 'Exporter';

use Carp qw(carp croak);
use Try::Tiny;

use CoreFunction qw(activateTables);
use TableDefs qw($OCC_MATRIX $SPEC_MATRIX);
use TaxonDefs qw(@TREE_TABLE_LIST);
use ConsoleLog qw(logMessage);

our (@EXPORT_OK) = qw(buildSpecimenTables buildMeasurementTables);

our $SPEC_MATRIX_WORK = "smw";

our $SPEC_ELT_WORK = "seltw";
our $SPEC_ELT_EXCLUSIONS_WORK = "sexw";
our $SPEC_ELT_MAP_WORK = "semw";


# buildSpecimenTables ( dbh )
# 
# Build the specimen matrix, recording which the necessary information for
# efficiently satisfying queries about specimens.

sub buildSpecimenTables {
    
    my ($dbh, $options) = @_;
    
    my ($sql, $result, $count, $extra);
    
    # Create a clean working table which will become the new specimen
    # matrix.
    
    logMessage(1, "Building specimen tables");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SPEC_MATRIX_WORK");
    $result = $dbh->do("CREATE TABLE $SPEC_MATRIX_WORK (
				specimen_no int unsigned not null,
				occurrence_no int unsigned not null,
				reid_no int unsigned not null,
				latest_ident boolean not null,
				taxon_no int unsigned not null,
				orig_no int unsigned not null,
				reference_no int unsigned not null,
				authorizer_no int unsigned not null,
				enterer_no int unsigned not null,
				modifier_no int unsigned not null,
				created timestamp null,
				modified timestamp null,
				primary key (specimen_no, reid_no)) ENGINE=MyISAM");
    
    # Add one row for every specimen in the database.  For specimens tied to
    # occurrences that have multiple identifications, we create a separate row
    # for each identification.
    
    logMessage(2, "    inserting specimens...");
    
    $sql = "	INSERT INTO $SPEC_MATRIX_WORK
		       (specimen_no, occurrence_no, reid_no, latest_ident, taxon_no, orig_no,
			reference_no, authorizer_no, enterer_no, modifier_no, created, modified)
		SELECT s.specimen_no, s.occurrence_no, o.reid_no, ifnull(o.latest_ident, 1), 
		       if(s.taxon_no is not null and s.taxon_no > 0, s.taxon_no, o.taxon_no),
		       if(a.orig_no is not null and a.orig_no > 0, a.orig_no, o.orig_no),
		       s.reference_no, s.authorizer_no, s.enterer_no, s.modifier_no,
		       s.created, s.modified
		FROM specimens as s LEFT JOIN authorities as a using (taxon_no)
			LEFT JOIN $OCC_MATRIX as o on o.occurrence_no = s.occurrence_no";
    
    $count = $dbh->do($sql);
    
        # Now add some indices to the main occurrence relation, which is more
    # efficient to do now that the table is populated.
    
    logMessage(2, "    indexing by occurrence and reid...");
    
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX selection (occurrence_no, reid_no)");
    
    logMessage(2, "    indexing by taxon...");
    
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (taxon_no)");
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (orig_no)");
    
    logMessage(2, "    indexing by reference...");
    
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (reference_no)");
    
    logMessage(2, "    indexing by person...");
    
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (authorizer_no)");
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (enterer_no)");
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (modifier_no)");
    
    logMessage(2, "    indexing by timestamp...");
    
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (created)");
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (modified)");
    
    # Then activate the new tables.
    
    activateTables($dbh, $SPEC_MATRIX_WORK => $SPEC_MATRIX);
    
    my $a = 1;	# we can stop here when debugging
}


# buildMeasurementTables ( dbh )
# 
# Build the measurement matrix, recording which the necessary information for
# efficiently satisfying queries about measurements.

sub buildMeasurementTables {
    
}


# init_specimen_element_tables ( dbh )
# 
# Create the tables for specimen elements.

sub init_specimen_element_tables {
    
    my ($dbh) = @_;
    
    my ($sql, $result);
    
    $dbh->do("DROP TABLE IF EXISTS $SPEC_ELT_WORK");
    
    $dbh->do("CREATE TABLE $SPEC_ELT_WORK (
		spec_elt_no int unsigned PRIMARY KEY,
		element_name varchar(80) not null,
		parent_elt_no int unsigned not null,
		base_no int unsigned not null,
		neotoma_element_id int unsigned not null,
		neotoma_element_type_id int unsigned not null,
		KEY (element_name),
		KEY (neotoma_element_id),
		KEY (neotoma_element_type_id))");
    
    $dbh->do("DROP TABLE IF EXISTS $SPEC_ELT_EXCLUSIONS_WORK");
    
    $dbh->do("CREATE TABLE IF EXISTS $SPEC_ELT_EXCLUSIONS_WORK (
		spec_elt_no int unsigned not null,
		taxon_no int unsigned not null,
		KEY (spec_elt_no)");
    
    $dbh->do("DROP TABLE IF EXISTS $SPEC_ELT_MAP_WORK");
    
    $dbh->do("CREATE TABLE $SPEC_ELT_MAP_WORK (
		spec_elt_no int unsigned not null,
		lft int unsigned not null,
		rgt int unsigned not null,
		KEY (lft, rgt))");
    
    
}
