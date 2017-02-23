#
# InstitutionTables.pm
# 
# Create and manage tables for recording information about database contributors.
# 


package InstitutionTables;

use strict;

use base 'Exporter';

our (@EXPORT_OK) = qw(createInstitutionTables);

use Carp qw(carp croak);
use Try::Tiny;

use TableDefs qw($INSTITUTIONS $INST_NAMES $INST_COLLS);

use CoreFunction qw(activateTables);
use ConsoleLog qw(logMessage);

our $INSTITUTIONS_WORK = "instw";
our $INST_NAMES_WORK = "instnw";
our $INST_COLLS_WORK = "instcw";



sub createInstitutionTables {

    my ($dbh) = @_;
    
    my ($result, $sql);
    
    logMessage(2, "Creating institution tables...");

    my $table_name;
    
    # First create new working tables.
    
    try {

	$table_name = $INSTITUTIONS_WORK;
	
	$dbh->do("DROP TABLE IF EXISTS $INSTITUTIONS_WORK");
	$dbh->do("CREATE TABLE $INSTITUTIONS_WORK (
			institution_no int unsigned PRIMARY KEY,
			status enum('active', 'inactive'),
			main_url varchar(80),
			websvc_url varchar(80),
			last_updated datetime,
			mailing_address varchar(100),
			mailing_city varchar(80),
			mailing_state varchar(80),
			mailing_postcode varchar(20),
			mailing_country varchar(2),
			institution_lsid varchar(100),
			lon decimal(9,3),
			lat decimal(9,3))");

	$dbh->do("DROP TABLE IF EXISTS $INST_NAMES_WORK");
	$dbh->do("CREATE TABLE $INST_NAMES_WORK (
			institution_no int unsigned not null,
			institution_name varchar(100) not null,
			institution_code varchar(20) not null,
			has_ih_record boolean,
			UNIQUE KEY (institution_no, institution_code))");
	
	$dbh->do("DROP TABLE IF EXISTS $INST_COLLS_WORK");
	$dbh->do("CREATE TABLE $INST_COLLS_WORK (
			institution_no int unsigned not null,
			collection_code varchar(20),
			collection_name varchar(100),
			collection_url varchar(80),
			catalog_url varchar(80),
			last_updated datetime,
			physical_address varchar(100),
			physical_city varchar(80),
			physical_state varchar(80),
			physical_postcode varchar(20),
			physical_country varchar(2),
			collection_contact varchar(80),
			contact_role varchar(80),
			contact_email varchar(80),
			collection_lsid varchar(100))");
	
	
    } catch {
	
	logMessage(1, "ABORTING");
	return;
    };

    # Then activate them.

    try {
	
	activateTables($dbh, $INSTITUTIONS_WORK => $INSTITUTIONS,
		       $INST_NAMES_WORK => $INST_NAMES,
		       $INST_COLLS_WORK => $INST_COLLS);

	$dbh->do("ALTER TABLE $INST_NAMES ADD FOREIGN KEY (institution_no) REFERENCES $INSTITUTIONS (institution_no) on delete cascade on update cascade");
	$dbh->do("ALTER TABLE $INST_COLLS ADD FOREIGN KEY (institution_no) REFERENCES $INSTITUTIONS (institution_no) on delete cascade on update cascade");

	logMessage(2, "Created institution tables.");
	
    } catch {

	logMessage(1, "ABORTING");
	return;
	
    };
}

1;
