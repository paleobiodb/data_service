#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for EditTransaction::Mod::PaleoBioDB, the module that adapts
# EditTransaction to the Paleobiology Database.
#
# edtpb-01-basic.t : Set up the test data and tables used by the other edtpb-*.t files, and
#                    check that a transaction can be carried out with them. This file must
#                    be run before any of the others.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use TableDefs qw(%TABLE $TEST_DB);

use EDTPBTest;
use PBDBTester;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_has_condition clear_table);


my $T;


# Create the tester, and switch the session tables and the test tables over to the test
# database.

subtest 'initialize tester' => sub {
    
    $T = PBDBTester->new('EDTPBTest');
    
    ok( $T, "created PBDBTester instance" );
    
    ok( eval { EDTPBTest->select_test_tables }, "selected the test tables" ) ||
	BAIL_OUT "could not select the test tables: $@";
    
    like( $TABLE{SESSION_DATA}, qr/^$TEST_DB[.]/, "the session table is in the test database" );
    like( $TABLE{WING_USERS}, qr/^$TEST_DB[.]/, "the users table is in the test database" );
};


# Establish the test users and their login sessions. This is the same set of users that is
# used by the pe2-*.t files.

subtest 'establish session data' => sub {
    
    $T->establish_session_data;
    
    my %expected = ( 'SESSION-AUTHORIZER' => [ 39998, 39998, 'authorizer', '' ],
		     'SESSION-ENTERER' => [ 39998, 39997, 'enterer', '' ],
		     'SESSION-STUDENT' => [ 39999, 39996, 'student', '' ],
		     'SESSION-OTHER' => [ 39999, 39995, 'enterer', '' ],
		     'SESSION-SUPERUSER' => [ 39999, 39999, 'authorizer', 1 ],
		     'SESSION-WITH-ADMIN' => [ 39999, 39991, 'enterer', '' ],
		     'SESSION-GUEST' => [ 0, 0, 'guest', '' ] );
    
    foreach my $session ( sort keys %expected )
    {
	my $perm = $T->new_perm($session, 'EDTPB_MAIN');
	
	is_deeply( [ $perm->authorizer_no, $perm->enterer_no, $perm->role, $perm->is_superuser ? 1 : '' ],
		   $expected{$session}, "permission object for '$session'" );
    }
};


subtest 'establish test tables' => sub {
    
    $T->establish_test_tables;
    
    ok( $T->dbh->selectrow_array("SHOW TABLES FROM $TEST_DB LIKE 'edtpb_main'"),
	"test tables established" );
};


# Check that records can be inserted, and that the authorization columns are filled in from
# the permission object.

subtest 'insert records' => sub {
    
    my $perm = $T->new_perm('SESSION-ENTERER', 'EDTPB_MAIN');
    
    my $edt = ok_new_edt(permission => $perm);
    
    $edt->insert_record('EDTPB_MAIN', { string_req => 'by enterer' });
    $edt->insert_record('EDTPB_UID', { string_val => 'by enterer' });
    $edt->insert_record('EDTPB_OPEN', { string_val => 'by enterer' });
    
    ok_commit;
    
    my $main = $T->dbh->selectrow_hashref("SELECT * FROM $TABLE{EDTPB_MAIN} WHERE string_req = 'by enterer'");
    
    is( $main->{authorizer_no}, 39998, "the authorizer is filled in" );
    is( $main->{enterer_no}, 39997, "the enterer is filled in" );
    is( $main->{modifier_no}, 0, "the modifier is not set on insert" );
    
    my $uid = $T->dbh->selectrow_hashref("SELECT * FROM $TABLE{EDTPB_UID} WHERE string_val = 'by enterer'");
    
    is( $uid->{enterer_id}, 'USERID-ENTERER', "the enterer is filled in as a user id" );
};


# Check that a guest cannot add records to a table that requires an authorizer.

subtest 'guest' => sub {
    
    my $perm = $T->new_perm('SESSION-GUEST', 'EDTPB_MAIN');
    
    my $edt = ok_new_edt(permission => $perm);
    
    $edt->insert_record('EDTPB_MAIN', { string_req => 'by guest' });
    
    ok_failed_commit("a guest cannot add a record to EDTPB_MAIN");
    
    ok_has_condition('all', 'E_PERMISSION', "the refusal is reported");
};
