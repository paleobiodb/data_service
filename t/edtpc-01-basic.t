#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the EditTransaction client modules, the subclasses of
# EditTransaction that handle edits to particular kinds of Paleobiology Database records.
#
# edtpc-01-basic.t : Set up the test users and the test tables used by the other edtpc-*.t
#                    files, and check that each client module can carry out a transaction
#                    with them. This file must be run before any of the others.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 6;

use TableDefs qw(%TABLE $TEST_DB original_table_name);

use EDTPCTest;
use PBDBTester;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_has_condition);

use EditTransaction::Mod::PaleoBioDB;


my $T;


# Create the tester, and switch the session tables and the tables used by the client
# modules over to the test database.

subtest 'initialize tester' => sub {
    
    $T = PBDBTester->new('PBDBEdit');
    
    ok( $T, "created PBDBTester instance" );
    
    ok( eval { EDTPCTest->select_test_tables }, "selected the test tables" ) ||
	BAIL_OUT "could not select the test tables: $@";
    
    is_deeply( [ EDTPCTest->tables_not_in_test_db ], [ ],
	       "every table used by the client modules is in the test database" ) ||
	BAIL_OUT "some tables are not in the test database";
    
    like( $TABLE{SESSION_DATA}, qr/^$TEST_DB[.]/, "the session table is in the test database" );
};


# Establish the test users and their login sessions. This is the same set of users that is
# used by the edtpb-*.t and pe2-*.t files.

subtest 'establish session data' => sub {
    
    $T->establish_session_data;
    
    my $perm = $T->new_perm('SESSION-ENTERER', 'ARCHIVES');
    
    is_deeply( [ $perm->authorizer_no, $perm->enterer_no ], [ 39998, 39997 ],
	       "the session data is in place" );
};


# Create the test tables, and check that each one is empty and has the same columns as the
# real table.

subtest 'establish test tables' => sub {
    
    ok( eval { EDTPCTest->establish_test_tables($T->dbh) }, "created the test tables" ) ||
	BAIL_OUT "could not create the test tables: $@";
    
    my $dbh = $T->dbh;
    
    foreach my $t ( EDTPCTest->module_tables )
    {
	my $test_name = $TABLE{$t};
	my $real_name = original_table_name($test_name);
	
	my ($count) = $dbh->selectrow_array("SELECT count(*) FROM $test_name");
	
	is( $count, 0, "the test copy of $t is empty" );
	
	my $test_cols = $dbh->selectcol_arrayref("SHOW COLUMNS FROM $test_name");
	my $real_cols = $dbh->selectcol_arrayref("SHOW COLUMNS FROM $real_name");
	
	is_deeply( $test_cols, $real_cols, "the test copy of $t has the columns of $real_name" );
    }
};


# Check that each client module loads, includes the PaleoBioDB role, and requires a
# Permissions object.

subtest 'client modules' => sub {
    
    foreach my $module ( EDTPCTest->client_modules )
    {
	ok( $module->isa('EditTransaction'), "$module is a subclass of EditTransaction" );
	
	is( $module->can('check_row_permission'),
	    EditTransaction::Mod::PaleoBioDB->can('check_row_permission'),
	    "$module includes the PaleoBioDB role" );
	
	my $perm = $T->new_perm('SESSION-ENTERER', $EDTPCTest::MODULE_TABLES{$module}[0]);
	
	my $edt = ok_new_edt(class => $module, permission => $perm, "created a $module transaction");
	
	isa_ok( $edt, $module, "the transaction" );
	
	eval { $module->new($T->dbh, { permission => { enterer_no => 39997 } }) };
	
	like( $@, qr/object of class 'Permissions'/, "$module requires a Permissions object" );
    }
};


# Check that a client module can carry out a transaction on its test table, and that the
# authorization columns are filled in.

subtest 'transaction' => sub {
    
    my $edt = ok_new_edt(class => 'ArchiveEdit',
			 permission => $T->new_perm('SESSION-ENTERER', 'ARCHIVES'));
    
    $edt->insert_record('ARCHIVES', { title => 'edtpc-01 archive' });
    
    ok_commit("an archive was added");
    
    my $record = $T->dbh->selectrow_hashref("
		SELECT * FROM $TABLE{ARCHIVES} WHERE title = 'edtpc-01 archive'");
    
    is_deeply( [ @{$record}{qw(authorizer_no enterer_no)} ], [ 39998, 39997 ],
	       "the authorizer and enterer were filled in" );
    
    my ($real_count) = $T->dbh->selectrow_array("
		SELECT count(*) FROM " . original_table_name($TABLE{ARCHIVES}) . "
		WHERE title = 'edtpc-01 archive'");
    
    is( $real_count, 0, "the real table was not changed" );
};


# Check that a guest cannot carry out a transaction that requires a database member.

subtest 'guest' => sub {
    
    my $edt = ok_new_edt(class => 'ArchiveEdit',
			 permission => $T->new_perm('SESSION-GUEST', 'ARCHIVES'));
    
    $edt->insert_record('ARCHIVES', { title => 'edtpc-01 guest archive' });
    
    ok_failed_commit("a guest cannot add an archive");
    
    ok_has_condition('all', 'E_PERMISSION', "the refusal is reported");
};
