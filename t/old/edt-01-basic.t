#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the EditTransaction class.
#
# edt-01-basic.t : Test that an EditTransaction object can be created, and that basic operations
# work.
#
# This test file should be run before any of the others, the first time that testing is done. Its
# first task is to establish the database tables needed for the rest of the tests. After that, the
# tests can all be run independently and in any order.
#



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 8;

# use ETBasicTest;
use EditTester;


$DB::single = 1;

# The following calls establish a connection to the database, then create or re-create the
# necessary tables.

my $T = EditTester->new('ETBasicTest');

# $T->establish_session_data;
$T->establish_test_tables;

# Test creation of both Permissions objects and an EditTest object. The Permissions objects are
# made available to subsequent tests. If we cannot create EditTest objects without an error
# occurring, we bail out. There is no point in running any more tests.

my $primary_key;
# my ($perm_a, $perm_g, $primary_key);

subtest 'create objects' => sub {

    # eval {
    # 	$perm_a = $T->new_perm('SESSION-AUTHORIZER');
    # 	$perm_g = $T->new_perm('SESSION-GUEST');
    # };
    
    # unless ( ok( !$@, "permissions did not throw exception" ) )
    # {
    # 	diag("message was: $@");
    # 	BAIL_OUT;
    # }
    
    # ok( $perm_a && $perm_a->{role} eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;
    # ok( $perm_g && $perm_g->{role} eq 'guest', "found guest permission" ) || BAIL_OUT;
    
    my $edt = $T->new_edt('NO_CREATE');
    
    ok( $edt, "created EditTest object" ) || BAIL_OUT;
    
    my $edt2 = $T->new_edt();
    
    ok( $edt2, "created EditTest object with CREATE") || BAIL_OUT;
    
    $primary_key = $edt2->get_table_property('EDT_TEST', 'PRIMARY_KEY');
    ok( $primary_key, "found primary key" ) || BAIL_OUT;
};


# Test inserting and deleting objects. Once again, if we cannot do this without error then we bail
# out.

subtest 'insert and delete' => sub {

    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    
    # Check that we can insert records.

    my $edt = $T->new_edt();
    
    $edt->insert_record('EDT_TEST', { string_req => 'abc' });
    
    $T->ok_commit( "insert record transaction succeeded" ) || BAIL_OUT;
    
    my @inserted = $edt->inserted_keys;
    
    is( @inserted, 1, "inserted one record" ) || BAIL_OUT;
    
    my ($r) = $T->fetch_records_by_key('EDT_TEST', $inserted[0]);
    
    ok( $r, "record was in the table" ) &&
	cmp_ok( $r->{string_req}, 'eq', 'abc', "record had proper string value" );
    
    # Check that we can delete records.
    
    $edt = $T->new_edt();
    
    $edt->delete_record('EDT_TEST', $inserted[0]);
    
    $T->ok_commit( "delete record transaction succeeded" ) || BAIL_OUT;
    
    # unless ( ok( $edt->commit, "delete record transaction succeeded" ) )
    # {
    # 	$T->diag_errors('all');
    # 	$T->diag_warnings('all');
    # 	BAIL_OUT;
    # }
    
    my @deleted = $edt->deleted_keys;
    
    cmp_ok( @deleted, '==', 1, "deleted one record" ) &&
	cmp_ok( $deleted[0], 'eq', $inserted[0], "deleted same record that was inserted" );
    
    $T->ok_no_record('EDT_TEST', "$primary_key=$inserted[0]");
};


# And likewise, test the update and replace operations and bail out if either of these is not
# functional.

subtest 'update and replace' => sub {

    my $edt = $T->new_edt();
    
    $edt->insert_record('EDT_TEST', { signed_val => 456, string_req => 'test for update' });
    
    $T->ok_commit( "insert record transaction succeeded" ) || BAIL_OUT;
    
    # ok( $edt->commit, "insert record transaction succeeded" ) || BAIL_OUT;
    
    my ($test_key) = $edt->inserted_keys;
    
    my ($r1) = $T->fetch_records_by_key('EDT_TEST', $test_key);
    
    ok( $r1, "record was in the table" ) &&
	cmp_ok( $r1->{string_req}, 'eq', 'test for update', "record had proper string value" );
    
    # Check that we can update records.
    
    $edt = $T->new_edt('IMMEDIATE_MODE');
    
    my $result2 = $edt->update_record('EDT_TEST',
				 { test_no => $test_key, string_req => 'updated' });
    
    $T->ok_action || BAIL_OUT;
    
    my ($r2) = $T->fetch_records_by_key('EDT_TEST', $test_key);
    
    if ( ok( $r2, "updated record was in the table" ) )
    {
	cmp_ok( $r2->{string_req}, 'eq', 'updated', "string value was updated" );
	cmp_ok( $r2->{signed_val}, 'eq', '456', "int value was not changed" );
    }
    
    # Check that we can replace records.
    
    my $result3 = $edt->replace_record('EDT_TEST', { test_no => $test_key, string_req => 'replaced' });
    
    $T->ok_action || BAIL_OUT;
    
    my ($r3) = $T->fetch_records_by_key('EDT_TEST', $test_key);
    
    if ( ok( $r3, "replaced record was in the table" ) )
    {
	cmp_ok( $r3->{string_req}, 'eq', 'replaced', "string value was changed" );
	ok( ! defined $r3->{signed_val}, "int value was replaced by nothing" );
    }

    $T->ok_commit( "update and replace transaction committed" );
    
    # ok( $edt->commit, "transaction committed" );
};


# Now deliberately generate exceptions and errors and check for them.

subtest 'test exceptions' => sub {

    my $edt;

    eval {
	local $EditTransaction::TEST_PROBLEM{no_connect} = 1;
	
	$edt = $T->_new_edt('EditTransaction');
    };
    
    ok( $@ && $@ =~ /TEST NO CONNECT/, "test exception for no database connection" );
    
    {
	local $EditTransaction::TEST_PROBLEM{sql_error} = 1;
	
	$edt = $T->_new_edt('EditTransaction', { SILENT_MODE => 1, IMMEDIATE_MODE => 1 });
	
	$edt->insert_record('EDT_TEST', { string_req => 'should not be inserted' });
    }
    
    $T->ok_has_error( 'all', 'E_EXECUTE' );
    $T->clear_edt;
    
    eval {
	local $EditTransaction::TEST_PROBLEM{validate} = 1;
	
	$edt = $T->_new_edt({ SILENT_MODE => 1, IMMEDIATE_MODE => 1 });
	
	$edt->insert_record('EDT_TEST', { string_req => 'should not be inserted' });
    };
    
    $T->ok_has_error( 'all', 'E_EXECUTE' );
};


# Check that the standard allowances are accepted, and that bad ones generate warnings.

subtest 'allowances' => sub {
    
    my $edt;
    
    ok( $edt = $T->new_edt(), "new edt with no options" );
    ok( $edt && $edt->allows('CREATE'), "no options allows CREATE" );
    $T->ok_no_warnings("no warnings");
    
    ok( $edt = $T->new_edt('NO_CREATE'), "new edt with no CREATE" );
    ok( $edt && ! $edt->allows('CREATE'), "does not allow CREATE" );
    $T->ok_no_warnings("no warnings with CREATE");
    
    ok( $edt = $T->new_edt(subclass => 'ETBasicTest',
		       { CREATE => 1,
			 LOCKED => 1,
			 NOT_FOUND => 1,
			 NOT_PERMITTED => 1,
			 PROCEED => 1,
			 MOVE_SUBORDINATES => 1,
			 BAD_FIELDS => 1,
			 DEBUG_MODE => 1,
			 SILENT_MODE => 1,
			 IMMEDIATE_MODE => 1,
			 FIXUP_MODE => 1,
			 ALTER_TRAIL => 1,
			 SKIP_LOGGING => 1,
			 VALIDATION_ONLY => 1,
			 TEST_DEBUG => 1,
			 UNKNOWN_MODE => 1 }), "new edt with many allowances" );
    
    if ( $edt )
    {
	ok( $edt->allows('CREATE'), "allowance CREATE accepted" );
	ok( $edt->allows('LOCKED'), "allowance LOCKED accepted" );
	ok( $edt->allows('ALTER_TRAIL'), "allowance ALTER_TRAIL accepted" );
	ok( $edt->allows('NOT_FOUND'), "allowance NOT_FOUND accepted" );
	ok( $edt->allows('PROCEED'), "allowance PROCEED accepted" );
	ok( $edt->allows('BAD_FIELDS'), "allowance BAD_FIELDS accepted" );
	ok( $edt->allows('DEBUG_MODE'), "allowance DEBUG_MODE accepted" );
	ok( $edt->allows('SILENT_MODE'), "allowance SILENT_MODE accepted" );
	ok( $edt->allows('IMMEDIATE_MODE'), "allowance IMMEDIATE_MODE accepted" );
	ok( $edt->allows('TEST_DEBUG'), "allowance TEST_DEBUG accepted" );
	ok( ! $edt->allows('UNKNOWN_MODE'), "allowance UNKNOWN_MODE not accepted" );
	ok( ! $edt->allows('NO_ALLOW'), "allowance NO_ALLOW not accepted" );
	
	cmp_ok( $edt->warnings, '==', 1, "got one warning" ) || $T->diag_warnings;
	$T->ok_has_warning( qr/UNKNOWN_MODE/, "warning about UNKNOWN_MODE" );
    }

    # Now try to create an EditTransaction using the array form for
    # allowances.
    
    $edt = $T->_new_edt('ETBasicTest', $T->dbh, [ 'CREATE', 'NOT_FOUND', 'TEST_DEBUG' ]);
    
    ok( $edt, "EditTransaction was created" );
    ok( $edt->allows('CREATE'), "allows 'CREATE'" );
    ok( $edt->allows('NOT_FOUND'), "allows 'NOT_FOUND'" );
    ok( $edt->allows('TEST_DEBUG'), "allows 'TEST_DEBUG'" );
    
    # And then try again using the string form.
    
    $edt = $T->_new_edt('ETBasicTest', $T->dbh, 'EDT_TEST', 'TEST_DEBUG , ,,CREATE');
    
    ok( $edt, "EditTransaction was created" );
    ok( $edt->allows('TEST_DEBUG'), "allows 'TEST_DEBUG'" );
    ok( $edt->allows('CREATE'), "allows 'CREATE'" );
};


# Now test the other basic accessor methods of EditTransaction.

subtest 'accessors' => sub {

    my $edt = $T->new_edt('DEBUG_MODE') || return;
    
    if ( can_ok( 'EditTransaction', 'dbh', 'permission', 'debug_mode' ) )
    {
	is( $edt->dbh, $T->dbh, "fetch dbh" );
	is( $edt->permission, 'unrestricted', "fetch permission" );
	ok( $edt->debug_mode, "fetch debug mode" );
    }
    
    $edt = $T->new_edt();
    
    if ( can_ok( 'EditTransaction', 'transaction', 'status', 'can_proceed',
		 'has_started', 'has_finished', 'is_active', 'has_committed' ) )
    {
	is( $edt->transaction, '', "transaction before start" );
	is( $edt->status, 'init', "status before start" );
	ok( ! $edt->has_started, "transaction has not started" );
	ok( ! $edt->has_finished, "transaction has not finished" );
	ok( ! $edt->is_active, "transaction is not active" );
	ok( $edt->can_proceed, "transaction can proceed" );
	ok( $T->debug_mode || ! $edt->debug_mode, "fetch debug 2" );
	
	$edt->start_transaction;
	
	is( $edt->transaction, 'active', "transaction status is 'active'" );
	ok( $edt->is_active, "transaction is active" );
	
	$edt->rollback;
	
	is( $edt->transaction, 'aborted', "transaction is aborted" );
	ok( $edt->has_finished, "transaction has finished" );
    }
    
    can_ok( 'EditTransaction', 'inserted_keys', 'updated_keys', 'replaced_keys', 'deleted_keys',
	    'failed_keys', 'key_labels', 'action_count', 'fail_count' );
};


# Now test capturing debug output

subtest 'debug output' => sub {

    # First try a transaction with DEBUG_MODE on and check that we get some debugging messages.
    
    my $edt = $T->new_edt('DEBUG_MODE', 'TEST_DEBUG');
    
    $edt->start_transaction;
    
    ok( $edt->has_debug_output( qr/START TRANSACTION/ ), "captured start from debug output" );
    
    $edt->rollback;
    
    ok( $edt->has_debug_output( qr/ROLLBACK TRANSACTION/ ), "captured rollback from debug output" );
    
    # Then try a transaction with DEBUG_MODE off and SILENT_MODE on check that we get none.
    
    $edt = $T->new_edt({ DEBUG_MODE => 0, SILENT_MODE => 1, TEST_DEBUG => 1, PROCEED => 1 });
    
    $edt->start_execution;
    
    ok( ! $edt->has_debug_output( qr/START TRANSACTION/ ),
	"did not capture debug output without DEBUG_MODE" );
    
    {
	local $EditTransaction::TEST_PROBLEM{sql_error} = 1;
	
	$edt->insert_record('EDT_TEST', { string_req => 'abc' });
	
	ok( ! $edt->has_debug_output( qr/do failed/i ),
	    "did not capture exception because of SILENT_MODE" );

	# Then turn silent mode off and check that we get exceptions again.
	
	$edt->silent_mode(0);
	$edt->clear_debug_output;
	
	$edt->insert_record('EDT_TEST', { string_req => 'abc' });
	
	ok( $edt->has_debug_output( qr/do failed/i ), "silent mode has been turned off" );
    }
    
    # Then try one with SILENT_MODE off and make sure that we get exceptions
    # but not debugging output.
    
    $edt = $T->new_edt({ SILENT_MODE => 0, TEST_DEBUG => 1, IMMEDIATE_MODE => 1 });

    ok ( ! $edt->has_debug_output( qr/START TRANSACTION/ ),
	 "did not capture debug output without DEBUG_MODE" );
    
    {
	local $EditTransaction::TEST_PROBLEM{sql_error} = 1;
	
	$edt->insert_record('EDT_TEST', { string_req => 'abc' });
	
	ok( $edt->has_debug_output( qr/do failed/i ), "captured exception because SILENT_MODE was off" );
    }

    # Now turn debug mode on and make sure we get debugging output once more.
    
    $edt->debug_mode(1);
    
    $edt->rollback;

    ok( $edt->has_debug_output( qr/ROLLBACK/i ), "captured debugging output again" );    
};


# Test that the transaction is rolled back when an EditTransaction goes out of scope, unless
# explicitly committed first.

subtest 'out of scope' => sub {
    
    my ($t1, $t2);
    
    # clear any pending transaction just in case
    
    $T->dbh->do("ROLLBACK");
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    
    # First try a transaction that is committed and then goes out of scope.

    {
	my $edt = EditTransaction->new($T->dbh);
	
	$edt->start_execution;
	
	$edt->insert_record('EDT_TEST', { signed_val => 222, string_req => 'insert this' });
	
	my ($active) = $T->dbh->selectrow_array("SELECT \@\@in_transaction");
	
	ok( $active, "transaction 1 is active while edt is in scope" );

	$edt->commit;
    }
    
    my ($still_active) = $T->dbh->selectrow_array("SELECT \@\@in_transaction");
    
    ok( ! $still_active, "transaction 1 is no longer active" );
    
    $T->ok_found_record('EDT_TEST', "signed_val = '222'", "transaction 1 was committed");
    
    {
	my $edt = EditTransaction->new($T->dbh);
	
	$edt->start_execution;
	
	$edt->insert_record('EDT_TEST', { signed_val => 223, string_req => 'do not insert' });
	
	my ($active) = $T->dbh->selectrow_array("SELECT \@\@in_transaction");
	
	ok( $active, "in transaction 2 while edt is in scope" );
    }
    
    ($still_active) = $T->dbh->selectrow_array("SELECT \@\@in_transaction");
    
    ok( ! $still_active, "transaction 2 is no longer active" );
    
    $T->ok_no_record('EDT_TEST', "signed_val = '223'", "transaction 2 was rolled back");
};


