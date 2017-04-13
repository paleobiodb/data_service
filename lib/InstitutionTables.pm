#
# InstitutionTables.pm
# 
# Create and manage tables for recording information about database contributors.
# 


package InstitutionTables;

use strict;

use base 'Exporter';

our (@EXPORT_OK) = qw(init_institution_tables compute_institution_codes);

use Carp qw(carp croak);
use Try::Tiny;

use TableDefs qw($INSTITUTIONS $INST_ALTNAMES $INST_COLLS $INST_COLL_ALTNAMES $INST_CODES);

use CoreFunction qw(activateTables);
use ConsoleLog qw(logMessage);

our $INSTITUTIONS_WORK = "instw";
our $INST_ALTNAMES_WORK = "ianw";
our $INST_COLLS_WORK = "icollw";
our $INST_COLL_ALTNAMES_WORK = "iacw";
our $INST_CODES_WORK = "icodew";

sub init_institution_tables {

    my ($dbh) = @_;
    
    my ($result, $sql);
    
    logMessage(2, "Creating institution tables...");

    my $table_name;
    
    # First create new working tables.
    
    try {

	$table_name = $INSTITUTIONS_WORK;
	
	$dbh->do("DROP TABLE IF EXISTS $INSTITUTIONS_WORK");
	$dbh->do("CREATE TABLE $INSTITUTIONS_WORK (
		institution_no int unsigned PRIMARY KEY auto_increment not null,
		institution_code varchar(20) not null,
		institution_name varchar(100) not null,
		main_url varchar(255) not null,
		websvc_url varchar(255) not null,
		last_updated datetime null,
		institution_lsid varchar(255) not null,
		KEY (institution_code),
		KEY (institution_name))");
	
	$dbh->do("DROP TABLE IF EXISTS $INST_ALTNAMES_WORK");
	$dbh->do("CREATE TABLE $INST_ALTNAMES_WORK (
		institution_no int unsigned not null,
		institution_code varchar(20) not null,
		institution_name varchar(100) not null,
		KEY (institution_no),
		KEY (institution_code),
		KEY (institution_name))");
	
	$dbh->do("DROP TABLE IF EXISTS $INST_COLLS_WORK");
	$dbh->do("CREATE TABLE $INST_COLLS_WORK (
		instcoll_no int unsigned PRIMARY KEY auto_increment not null,
		institution_no int unsigned not null,
		instcoll_code varchar(20) not null,
		instcoll_name varchar(255) not null,
		instcoll_status enum('active', 'inactive') null,
		has_ih_record boolean null,
		instcoll_url varchar(255) not null,
		catalog_url varchar(255) not null,
		last_updated datetime null,
		mailing_address varchar(255) not null,
		mailing_city varchar(80) not null,
		mailing_state varchar(80) not null,
		mailing_postcode varchar(20) not null,
		mailing_country varchar(80) not null,
		mailing_cc varchar(2) not null,
		physical_address varchar(255) not null,
		physical_city varchar(80) not null,
		physical_state varchar(80) not null,
		physical_postcode varchar(20) not null,
		physical_country varchar(80) not null,
		physical_cc varchar(2) not null,
		lon decimal(9,3),
		lat decimal(9,3),
		instcoll_contact varchar(80) not null,
		contact_role varchar(80) not null,
		contact_email varchar(80) not null,
		instcoll_lsid varchar(100) not null,
		KEY (institution_no),
		KEY (instcoll_code),
		KEY (instcoll_name),
		KEY (lon, lat),
		KEY (physical_cc))");
	
	$dbh->do("DROP TABLE IF EXISTS $INST_COLL_ALTNAMES_WORK");
	$dbh->do("CREATE TABLE $INST_COLL_ALTNAMES_WORK (
		instcoll_no int unsigned not null,
		instcoll_code varchar(20) not null,
		instcoll_name varchar(255) not null,
		KEY (instcoll_no),
		KEY (instcoll_code),
		KEY (instcoll_name))");
	
	$dbh->do("DROP TABLE IF EXISTS $INST_CODES_WORK");
	$dbh->do("CREATE TABLE $INST_CODES_WORK (
		instcoll_code varchar(20) not null,
		instcoll_no int unsigned not null,
		institution_no int unsigned not null,
		is_inst_code boolean not null,
		KEY (collection_code),
		KEY (instcoll_no),
		KEY (institution_no))");
	
    } catch {
	
	logMessage(1, "ABORTING");
	return;
    };

    # Then activate them.

    try {
	
	activateTables($dbh, $INSTITUTIONS_WORK => $INSTITUTIONS,
		       $INST_ALTNAMES_WORK => $INST_ALTNAMES,
		       $INST_COLLS_WORK => $INST_COLLS,
		       $INST_COLL_ALTNAMES_WORK => $INST_COLL_ALTNAMES,
		       $INST_CODES_WORK => $INST_CODES);

	# $dbh->do("ALTER TABLE $INST_NAMES ADD FOREIGN KEY (institution_no) REFERENCES $INSTITUTIONS (institution_no) on delete cascade on update cascade");
	# $dbh->do("ALTER TABLE $INST_COLLS ADD FOREIGN KEY (institution_no) REFERENCES $INSTITUTIONS (institution_no) on delete cascade on update cascade");

	logMessage(2, "Created institution tables.");
	
    } catch {

	logMessage(1, "ABORTING");
	return;
	
    };
}


# compute_institution_codes (dbh)
# 
# Generate (or regenerate) the $INST_CODES table.  This involves copying codes, id numbers, and
# names from both $INSTITUTIONS and $INST_COLLS, plus their associated ALTNAMES tables.

# Perhaps we don't actually need this...

# sub compute_institution_codes {
    
#     my ($dbh) = @_;
    
#     my ($sql, $result);
    
#     $dbh->do("START TRANSACTION");
    
#     $dbh->do("TRUNCATE TABLE $INST_CODES");
    
#     $dbh->do("INSERT INTO $INST_CODES (code, collection_no, institution_no)
# 		SELECT collection_code, collection_no, institution_no
# 		FROM $INST_COLLS");
    
#     $dbh->do("INSERT INTO $INST ...");
# }

1;
