# 
# EditTransaction project
# 
#   EDTPBTest.pm - a class for use in testing EditTransaction::Mod::PaleoBioDB
#   
#   This class is a subclass of EditTransaction which includes the PaleoBioDB module, and is
#   used by the unit tests for that module (edtpb-*.t). It defines a set of test tables with
#   the columns that the Paleobiology Database uses to record who created, authorized, and
#   modified each record.
#   
#   Added by Claude under the guidance of Michael McClennen.

use strict;

package EDTPBTest;

use EditTransaction;

use parent 'EditTransaction';

use TableDefs qw(%TABLE $TEST_DB set_table_name set_table_group set_table_property
		 set_column_property enable_test_mode change_table_db);

use CoreTableDefs;

use Carp qw(carp croak);

use Role::Tiny::With;

with 'EditTransaction::Mod::MariaDB';
with 'EditTransaction::Mod::PaleoBioDB';

use namespace::clean;


# Define the test tables and their properties
# -------------------------------------------

{
    # EDTPB_MAIN has the authorization columns as person numbers, the lock columns, and
    # timestamps, including the updater and update time that are set in FIXUP_MODE. Records can be added by any user with an authorizer. Its primary key is
    # given an external identifier type, so that keys can be given as external
    # identifiers. Its column reference_no gets the type 'REF' from PBDBFields.
    
    set_table_name(EDTPB_MAIN => 'edtpb_main');
    set_table_property(EDTPB_MAIN => PRIMARY_KEY => 'main_no');
    set_table_property(EDTPB_MAIN => CAN_POST => 'AUTHORIZED');
    set_column_property(EDTPB_MAIN => main_no => EXTID_TYPE => 'COL');
    
    # EDTPB_SUB is subordinate to EDTPB_MAIN. Its link column has the same external identifier
    # type as the primary key of EDTPB_MAIN, as the link columns of the real tables do.
    
    set_table_name(EDTPB_SUB => 'edtpb_sub');
    set_table_property(EDTPB_SUB => PRIMARY_KEY => 'sub_no');
    set_table_property(EDTPB_SUB => AUTH_TABLE => 'EDTPB_MAIN');
    set_table_property(EDTPB_SUB => CAN_POST => 'AUTHORIZED');
    set_column_property(EDTPB_SUB => main_no => EXTID_TYPE => 'COL');
    
    # EDTPB_BYAUTH is like EDTPB_MAIN, except that records are owned by everyone with the
    # same authorizer, and they can be deleted by their owners.
    
    set_table_name(EDTPB_BYAUTH => 'edtpb_byauth');
    set_table_property(EDTPB_BYAUTH => PRIMARY_KEY => 'byauth_no');
    set_table_property(EDTPB_BYAUTH => CAN_POST => 'AUTHORIZED');
    set_table_property(EDTPB_BYAUTH => BY_AUTHORIZER => 1);
    set_table_property(EDTPB_BYAUTH => CAN_DELETE => 'OWNER');
    
    # EDTPB_UID records its creator and modifier by user id rather than by person number.
    # Records can be added by anyone who is logged in. Its column approver_id is given the
    # authorizer directive, under a name that is not one of the standard ones.
    
    set_table_name(EDTPB_UID => 'edtpb_uid');
    set_table_property(EDTPB_UID => PRIMARY_KEY => 'uid_no');
    set_table_property(EDTPB_UID => CAN_POST => 'LOGGED_IN');
    set_table_property(EDTPB_UID => CAN_DELETE => 'AUTHORIZED');
    set_column_property(EDTPB_UID => approver_id => DIRECTIVE => 'auth_authorizer');
    
    # Records in EDTPB_KEEP cannot be deleted by anybody.
    
    set_table_name(EDTPB_KEEP => 'edtpb_keep');
    set_table_property(EDTPB_KEEP => PRIMARY_KEY => 'keep_no');
    set_table_property(EDTPB_KEEP => CAN_POST => 'AUTHORIZED');
    set_table_property(EDTPB_KEEP => CAN_DELETE => 'NONE');
    
    # EDTPB_OPEN has no authorization properties, so anybody can operate on it.
    
    set_table_name(EDTPB_OPEN => 'edtpb_open');
    set_table_property(EDTPB_OPEN => PRIMARY_KEY => 'open_no');
    
    set_table_group('edtpb_test' => 'EDTPB_MAIN', 'EDTPB_SUB', 'EDTPB_BYAUTH', 'EDTPB_UID',
		    'EDTPB_KEEP', 'EDTPB_OPEN');
}


# select_test_tables ( )
# 
# Switch the test tables over to the test database, along with the collection, occurrence,
# and reidentification tables, which this module treats specially. This must be called
# after the EditTester instance has been created, because that is when the database
# configuration is read. Throw an exception if the switch did not succeed.

sub select_test_tables {
    
    enable_test_mode('edtpb_test');
    enable_test_mode('collection_data');
    enable_test_mode('occurrence_data');
    
    # REID_DATA is not part of any table group.
    
    change_table_db('REID_DATA', $TEST_DB) unless $TABLE{REID_DATA} =~ /^$TEST_DB[.]/;
    
    foreach my $t ( 'EDTPB_MAIN', 'COLLECTION_DATA', 'OCCURRENCE_DATA', 'REID_DATA' )
    {
	croak "could not select the test database for the table '$t'"
	    unless $TEST_DB && $TABLE{$t} =~ /^$TEST_DB[.]/;
    }
    
    return 1;
}


# establish_test_tables ( dbh )
# 
# Create, or re-create, the test tables.

sub establish_test_tables {
    
    my ($class, $dbh) = @_;
    
    croak "the test tables have not been switched over to the test database"
	unless $TEST_DB && $TABLE{EDTPB_MAIN} =~ /^$TEST_DB[.]/;
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDTPB_MAIN}");
    
    $dbh->do("CREATE TABLE $TABLE{EDTPB_MAIN} (
		main_no int unsigned primary key auto_increment,
		authorizer_no int unsigned not null default 0,
		enterer_no int unsigned not null default 0,
		modifier_no int unsigned not null default 0,
		string_req varchar(40) not null default '',
		string_val varchar(40) not null default '',
		reference_no int unsigned not null default 0,
		int_val int unsigned not null default 0,
		admin_lock tinyint(1) not null default 0,
		owner_lock tinyint(1) not null default 0,
		updater_no int unsigned not null default 0,
		created timestamp not null default current_timestamp,
		modified timestamp not null default current_timestamp on update current_timestamp,
		updated timestamp null default null)
		default charset utf8");
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDTPB_SUB}");
    
    $dbh->do("CREATE TABLE $TABLE{EDTPB_SUB} (
		sub_no int unsigned primary key auto_increment,
		main_no int unsigned not null default 0,
		authorizer_no int unsigned not null default 0,
		enterer_no int unsigned not null default 0,
		modifier_no int unsigned not null default 0,
		name varchar(40) not null default '',
		created timestamp not null default current_timestamp,
		modified timestamp not null default current_timestamp on update current_timestamp)
		default charset utf8");
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDTPB_BYAUTH}");
    
    $dbh->do("CREATE TABLE $TABLE{EDTPB_BYAUTH} (
		byauth_no int unsigned primary key auto_increment,
		authorizer_no int unsigned not null default 0,
		enterer_no int unsigned not null default 0,
		modifier_no int unsigned not null default 0,
		string_val varchar(40) not null default '',
		created timestamp not null default current_timestamp,
		modified timestamp not null default current_timestamp on update current_timestamp)
		default charset utf8");
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDTPB_UID}");
    
    $dbh->do("CREATE TABLE $TABLE{EDTPB_UID} (
		uid_no int unsigned primary key auto_increment,
		approver_id char(36) not null default '',
		enterer_id char(36) not null default '',
		modifier_id char(36) not null default '',
		string_val varchar(40) not null default '',
		created timestamp not null default current_timestamp,
		modified timestamp not null default current_timestamp on update current_timestamp)
		default charset utf8");
    
    # The collection, occurrence, and reidentification tables are copies of the structure
    # of the real ones.
    
    foreach my $t ( 'COLLECTION_DATA', 'OCCURRENCE_DATA', 'REID_DATA' )
    {
	my $test_name = $TABLE{$t};
	my ($real_name) = TableDefs::original_table_name($test_name);
	
	croak "could not find the name of the real table for '$t'" unless $real_name;
	
	$dbh->do("DROP TABLE IF EXISTS $test_name");
	$dbh->do("CREATE TABLE $test_name LIKE $real_name");
    }
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDTPB_KEEP}");
    
    $dbh->do("CREATE TABLE $TABLE{EDTPB_KEEP} (
		keep_no int unsigned primary key auto_increment,
		authorizer_no int unsigned not null default 0,
		enterer_no int unsigned not null default 0,
		modifier_no int unsigned not null default 0,
		string_val varchar(40) not null default '')
		default charset utf8");
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDTPB_OPEN}");
    
    $dbh->do("CREATE TABLE $TABLE{EDTPB_OPEN} (
		open_no int unsigned primary key auto_increment,
		string_val varchar(40) not null default '')
		default charset utf8");
    
    return 1;
}


1;
