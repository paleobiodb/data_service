#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-01-basic.t : Test that an EditTransaction object can be created, and that basic operations work.
#



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 8;

use TableDefs qw(get_table_property $EDT_TEST);

use EditTest;
use EditTester;


# The following calls establish a connection to the database, then create or re-create the
# necessary tables.

my $T = EditTester->new;

$T->create_tables;


# Test creation of both Permissions objects and an EditTest object. The Permissions objects are
# made available to subsequent tests. If we cannot create EditTest objects without an error
# occurring, we bail out. There is no point in running any more tests.

my ($perm_a, $perm_g, $primary_key);

subtest 'create objects' => sub {

    eval {
	$perm_a = $T->new_perm('SESSION-AUTHORIZER');
	$perm_g = $T->new_perm('SESSION-GUEST');
    };
    
    unless ( ok( !$@, "permissions did not throw exception" ) )
    {
	diag("message was: $@");
	BAIL_OUT;
    }
    
    ok( $perm_a && $perm_a->{role} eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;
    ok( $perm_g && $perm_g->{role} eq 'guest', "found guest permission" ) || BAIL_OUT;
    
    my $edt = $T->new_edt($perm_a, { CREATE => 0 });

    ok( $edt, "created EditTest object" ) || BAIL_OUT;
    
    my $edt2 = $T->new_edt($perm_a);
    
    ok( $edt2, "created EditTest object with CREATE") || BAIL_OUT;

    $primary_key = get_table_property($EDT_TEST, 'PRIMARY_KEY');
    ok( $primary_key, "found primary key" ) || BAIL_OUT;
};


# Test inserting and deleting objects. Once again, if we cannot do this without error then we bail
# out.

subtest 'insert and delete' => sub {

    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table($EDT_TEST);
    
    # Check that we can insert records.
    
    my $result1 = $T->do_insert_records($perm_a, undef, "insert record",
					$EDT_TEST, { signed_req => 123, string_req => 'abc' })
	|| BAIL_OUT;
    
    my @inserted = $T->inserted_keys;
    
    unless ( cmp_ok( @inserted, '==', 1, "inserted one record" ) )
    {
	$T->diag_errors;
	$T->diag_warnings;
	BAIL_OUT;
    }
    
    my ($r) = $T->fetch_records_by_key($EDT_TEST, $inserted[0]);

    ok( $r, "record was in the table" ) &&
	cmp_ok( $r->{string_req}, 'eq', 'abc', "record had proper string value" );

    # Check that we can delete records.
    
    my $result2 = $T->do_delete_records($perm_a, undef, "delete record",
    					$EDT_TEST, $inserted[0])
    	|| BAIL_OUT;
    
    my @deleted = $T->deleted_keys;
    
    cmp_ok( @deleted, '==', 1, "deleted one record" ) &&
	cmp_ok( $deleted[0], 'eq', $inserted[0], "deleted same record that was inserted" );
    
    $T->ok_no_record($EDT_TEST, "$primary_key=$inserted[0]");
};


# And likewise, test the update and replace operations and bail out if either of these is not
# functional.

subtest 'update and replace' => sub {

    my $result1 = $T->do_insert_records($perm_a, undef, "insert record", $EDT_TEST,
					{ signed_req => 123, signed_val => 456,
					  string_req => 'test for update' })
	|| BAIL_OUT;
    
    my ($test_key) = $T->inserted_keys;

    my ($r1) = $T->fetch_records_by_key($EDT_TEST, $test_key);
    
    ok( $r1, "record was in the table" ) &&
	cmp_ok( $r1->{string_req}, 'eq', 'test for update', "record had proper string value" );

    # Check that we can update records.
    
    my $result2 = $T->do_update_records($perm_a, undef, "update record", $EDT_TEST,
					{ test_id => $test_key, string_req => 'updated' })
	|| BAIL_OUT;
    
    my ($r2) = $T->fetch_records_by_key($EDT_TEST, $test_key);
    
    if ( ok( $r2, "updated record was in the table" ) )
    {
	cmp_ok( $r2->{string_req}, 'eq', 'updated', "string value was updated" );
	cmp_ok( $r2->{signed_val}, 'eq', '456', "int value was not changed" );
    }
    
    # Check that we can replace records.
    
    my $result3 = $T->do_replace_records($perm_a, undef, "replace record", $EDT_TEST,
					 { test_id => $test_key, string_req => 'replaced',
					   signed_req => 789 })
	|| BAIL_OUT;
    
    my ($r3) = $T->fetch_records_by_key($EDT_TEST, $test_key);
    
    if ( ok( $r3, "replaced record was in the table" ) )
    {
	cmp_ok( $r3->{string_req}, 'eq', 'replaced', "string value was changed" );
	ok( ! defined $r3->{signed_val}, "int value was replaced by nothing" );
    }
};


# Now deliberately generate exceptions and errors and check for them.

subtest 'test exceptions' => sub {

    my $edt;

    eval {
	local $EditTransaction::TEST_PROBLEM{no_connect} = 1;
	
	$edt = $T->new_edt($perm_a);
    };

    ok( $@ && $@ =~ /TEST NO CONNECT/, "test exception for no database connection" );
    
    {
	local $EditTransaction::TEST_PROBLEM{insert_sql} = 1;
	
	$T->do_insert_records($perm_a, { nocheck => 1 }, "insert record exception", $EDT_TEST,
			      { signed_req => 999, string_req => 'should not be inserted' });
    }
    
    $T->ok_has_error( qr/E_EXECUTE/, "test insert sql error" );
    $T->clear_edt;
    
    eval {
	local $EditTransaction::TEST_PROBLEM{validate} = 1;
	
    	$T->do_insert_records($perm_a, { nocheck => 1 }, "insert record", $EDT_TEST,
    			      { signed_req => 999, string_req => 'should not be inserted' });
    };
    
    $T->ok_has_error( qr/E_EXECUTE/, "test validate error" );
};


# Check that the standard allowances are accepted, and that bad ones generate warnings.

subtest 'allowances' => sub {
    
    my $edt;
    
    ok( $edt = $T->new_edt($perm_a), "new edt with no options" );
    ok( $edt && $edt->allows('CREATE'), "no options allows CREATE" );
    $T->ok_no_warnings("no warnings");
    
    ok( $edt = $T->new_edt($perm_a, { CREATE => 0 }), "new edt with no CREATE" );
    ok( $edt && ! $edt->allows('CREATE'), "does not allow CREATE" );
    $T->ok_no_warnings("no warnings with CREATE");
    
    ok( $edt = $T->new_edt($perm_a, { CREATE => 1,
				      MULTI_DELETE => 1,
				      NO_RECORDS => 1,
				      NOT_FOUND => 1,
				      ALTER_TRAIL => 1,
				      DEBUG_MODE => 1,
				      SILENT_MODE => 1,
				      IMMEDIATE_MODE => 1,
				      PROCEED_MODE => 1,
				      TEST_DEBUG => 1,
				      BAD_ALLOW => 1,
				      NO_ALLOW => 0 }), "new edt with many allowances" );
    if ( $edt )
    {
	ok( $edt->allows('CREATE'), "allowance CREATE accepted" );
	ok( $edt->allows('MULTI_DELETE'), "allowance MULTI_DELETE accepted" );
	ok( $edt->allows('NO_RECORDS'), "allowance NO_RECORDS accepted" );
	ok( $edt->allows('NOT_FOUND'), "allowance NOT_FOUND accepted" );
	ok( $edt->allows('ALTER_TRAIL'), "allowance ALTER_TRAIL accepted" );
	ok( $edt->allows('DEBUG_MODE'), "allowance DEBUG_MODE accepted" );
	ok( $edt->allows('SILENT_MODE'), "allowance SILENT_MODE accepted" );
	ok( $edt->allows('IMMEDIATE_MODE'), "allowance DEBUG_MODE accepted" );
	ok( $edt->allows('PROCEED_MODE'), "allowance PROCEED_MODE accepted" );
	ok( $edt->allows('TEST_DEBUG'), "allowance TEST_DEBUG accepted" );
	ok( ! $edt->allows('BAD_ALLOW'), "allowance BAD_ALLOW not accepted" );
	ok( ! $edt->allows('NO_ALLOW'), "allowance NO_ALLOW not accepted" );
	
	cmp_ok( $edt->warnings, '==', 1, "return one warning" );
	$T->ok_has_warning( qr/BAD_ALLOW/, "warning about BAD_ALLOW" );
    }

    # Now try to create an EditTransaction using the array form for
    # allowances.

    $edt = EditTest->new($T->dbh, $perm_a, $EDT_TEST, [ 'CREATE', 'NOT_FOUND', 'TEST_DEBUG' ]);

    ok( $edt, "EditTransaction was created" );
    ok( $edt->allows('CREATE'), "allows 'CREATE'" );
    ok( $edt->allows('NOT_FOUND'), "allows 'NOT_FOUND'" );
    ok( $edt->allows('TEST_DEBUG'), "allows 'TEST_DEBUG'" );
    
    # And then try again using the string form.
    
    $edt = EditTest->new($T->dbh, $perm_a, $EDT_TEST, 'TEST_DEBUG , ,,CREATE');
    
    ok( $edt, "EditTransaction was created" );
    ok( $edt->allows('TEST_DEBUG'), "allows 'TEST_DEBUG'" );
    ok( $edt->allows('CREATE'), "allows 'CREATE'" );
};


# Now test the other basic accessor methods of EditTransaction.

subtest 'accessors' => sub {

    my $edt = $T->new_edt($perm_a, { DEBUG_MODE => 1 });
    
    ok( $edt, "created edt" ) || return;

    if ( can_ok( 'EditTransaction', 'dbh', 'perms', 'role', 'debug' ) )
    {    
	is( $edt->dbh, $T->dbh, "fetch dbh" );
	is( $edt->perms, $perm_a, "fetch perm_a" );
	is( $edt->role, 'authorizer', "fetch role a" );
	ok( $edt->debug, "fetch debug" );
    }
    
    $edt = $T->new_edt($perm_g);

    if ( can_ok( 'EditTransaction', 'transaction', 'has_started', 'has_finished', 'is_active',
	         'has_committed', 'can_proceed' ) )
    {
	is( $edt->transaction, '', "fetch transaction before start" );
	ok( ! $edt->has_started, "transaction has not started" );
	ok( ! $edt->has_finished, "transaction has not finished" );
	ok( ! $edt->is_active, "transaction is not active" );
	ok( $edt->can_proceed, "transaction can proceed" );
	is( $edt->perms, $perm_g, "fetch perm_g" );
	is( $edt->role, 'guest', "fetch role g" );
	ok( $T->debug || ! $edt->debug, "fetch debug 2" );
	
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
    
    my $edt = $T->new_edt($perm_a, { DEBUG_MODE => 1, TEST_DEBUG => 1 });
    
    $edt->start_transaction;

    ok( $edt->has_debug_output( qr/START TRANSACTION/ ), "captured start from debug output" );
    
    $edt->rollback;
    
    ok( $edt->has_debug_output( qr/ROLLBACK TRANSACTION/ ), "captured rollback from debug output" );

    # Then try a transaction with DEBUG_MODE off and check that we get none.
    
    my $edt2 = $T->new_edt($perm_a, { DEBUG_MODE => 0, TEST_DEBUG => 1 });
    
    $edt2->start_execution;
    
    ok( ! $edt2->has_debug_output( qr/START TRANSACTION/ ) || $T->debug,
	"did not capture debug output without DEBUG_MODE" );
    
    {
	local $EditTransaction::TEST_PROBLEM{insert_sql} = 1;

	$edt2->insert_record($EDT_TEST, { signed_req => 'abc' });
    }
    
    ok( ! $edt2->has_debug_output( qr/XXXX/ ), "did not capture exception because of default SILENT_MODE" );
    
    $edt2->rollback;

    # Then try one with SILENT_MODE off (it is on by default) and make sure that we get exceptions
    # but not debugging output.
    
    my $edt3 = $T->new_edt($perm_a, { SILENT_MODE => 0, TEST_DEBUG => 1, IMMEDIATE_MODE => 1 });
    
    ok ( ! $edt3->has_debug_output( qr/START TRANSACTION/ ) || $T->debug,
	 "did not capture debug output without DEBUG_MODE" );

    {
	local $EditTransaction::TEST_PROBLEM{insert_sql} = 1;

	$edt3->insert_record($EDT_TEST, { string_req => 'abc' });
    }
    
    ok( $edt3->has_debug_output( qr/do failed/ ), "captured exception because SILENT_MODE was off" );
    $T->diag_errors;
};


# Test that the transaction is rolled back when an EditTransaction goes out of scope, unless
# explicitly committed first.

subtest 'out of scope' => sub {
    
    my ($t1, $t2);

    # clear any pending transaction just in case
    
    $T->dbh->do("ROLLBACK");
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table($EDT_TEST);
    
    # First try a transaction that is committed and then goes out of scope.

    {
	my $edt = $T->new_edt($perm_a);
	
	$edt->start_execution;
	
	$edt->insert_record($EDT_TEST, { signed_val => 222, string_req => 'insert this' });
	
	my ($active) = $T->dbh->selectrow_array("SELECT \@\@in_transaction");
	
	ok( $active, "transaction 1 is active while edt is in scope" );

	$edt->commit;
	
	$T->new_edt($perm_a);	# must remove saved reference from $T to previous $edt by
                                # generating a new one
    }
    
    my ($still_active) = $T->dbh->selectrow_array("SELECT \@\@in_transaction");

    ok( ! $still_active, "transaction 1 is no longer active" );
    
    $T->ok_found_record($EDT_TEST, "signed_val = '222'", "transaction 1 was committed");
    
    {
	my $edt = $T->new_edt($perm_a);
	
	$edt->start_execution;
	
	$edt->insert_record($EDT_TEST, { signed_val => 223, string_req => 'do not insert' });
	
	my ($active) = $T->dbh->selectrow_array("SELECT \@\@in_transaction");
	
	ok( $active, "in transaction 2 while edt is in scope" );

	$T->new_edt($perm_a);	# must remove saved reference from $T to previous $edt by
                                # generating a new one
    }
    
    ($still_active) = $T->dbh->selectrow_array("SELECT \@\@in_transaction");
    
    ok( ! $still_active, "transaction 2 is no longer active" );

    $T->ok_no_record($EDT_TEST, "signed_val = '223'", "transaction 2 was rolled back");
};


