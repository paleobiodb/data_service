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

use TableDefs qw(init_table_names select_test_tables $EDT_TEST);

use EditTest;
use EditTester;


# The following calls establish a connection to the database, then create or re-create the
# necessary tables.

my $T = EditTester->new;

$T->create_tables;


# Test creation of both Permissions objects and an EditTest object. The Permissions objects are
# made available to subsequent tests. If we cannot create EditTest objects without an error
# occurring, we bail out. There is no point in running any more tests.

my ($perm_a, $perm_g);

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
};


# Test inserting and deleting objects. Once again, if we cannot do this without error then we bail
# out.

subtest 'insert and delete' => sub {

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
    
    my $result2 = $T->do_delete_records($perm_a, undef, "delete record",
					$EDT_TEST, $inserted[0])
	|| BAIL_OUT;
    
    my @deleted = $T->deleted_keys;
    
    cmp_ok( @deleted, '==', 1, "deleted one record" ) &&
	cmp_ok( $deleted[0], 'eq', $inserted[0], "deleted same record that was inserted" );

    ($r) = $T->fetch_records_by_key($EDT_TEST, $inserted[0]);

    ok( !$r, "record was removed from the table" );
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

    my $result2 = $T->do_update_records($perm_a, undef, "update record", $EDT_TEST,
					{ test_id => $test_key, string_req => 'updated' })
	|| BAIL_OUT;
    
    my ($r2) = $T->fetch_records_by_key($EDT_TEST, $test_key);
    
    if ( ok( $r2, "updated record was in the table" ) )
    {
	cmp_ok( $r2->{string_req}, 'eq', 'updated', "string value was updated" );
	cmp_ok( $r2->{signed_val}, 'eq', '456', "int value was not changed" );
    }
    
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
    
    # eval {
    # 	local $EditTransaction::TEST_PROBLEM{execute} = 1;
	
    # 	$T->do_insert_records($perm_a, { nocheck => 1 }, "insert record", $EDT_TEST,
    # 			      { signed_req => 999, string_req => 'should not be inserted' });
    # };
    
    # $T->ok_last_exception( qr/TEST EXECUTE/, "test exception on execution" );
	
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
				      PROCEED => 1, 
				      KEY_INSERT => 1,
				      MULTI_DELETE => 1,
				      NO_RECORDS => 1,
				      ALTER_TRAIL => 1,
				      DEBUG_MODE => 1,
				      TEST_DEBUG => 1,
				      BAD_ALLOW => 1,
				      NO_ALLOW => 0 }), "new edt with many allowances" );
    if ( $edt )
    {
	ok( $edt->allows('CREATE'), "allowance CREATE accepted" );
	ok( $edt->allows('PROCEED'), "allowance PROCEED accepted" );
	ok( $edt->allows('KEY_INSERT'), "allowance KEY_INSERT accepted" );
	ok( $edt->allows('MULTI_DELETE'), "allowance MULTI_DELETE accepted" );
	ok( $edt->allows('NO_RECORDS'), "allowance NO_RECORDS accepted" );
	ok( $edt->allows('ALTER_TRAIL'), "allowance ALTER_TRAIL accepted" );
	ok( $edt->allows('DEBUG_MODE'), "allowance DEBUG_MODE accepted" );
	ok( $edt->allows('TEST_DEBUG'), "allowance TEST_DEBUG accepted" );
	ok( ! $edt->allows('BAD_ALLOW'), "allowance BAD_ALLOW not accepted" );
	ok( ! $edt->allows('NO_ALLOW'), "allowance NO_ALLOW not accepted" );
	
	cmp_ok( $edt->warnings, '==', 1, "return one warning" );
	$T->ok_has_warning( qr/BAD_ALLOW/, "warning about BAD_ALLOW" );
    }
};


# Now test the other basic accessor methods of EditTransaction.

subtest 'accessors' => sub {

    my $edt1 = $T->new_edt($perm_a, { DEBUG_MODE => 1 });
    my $edt2 = $T->new_edt($perm_g);
    
    ok( $edt1 && $edt2, "created both objects" ) || return;
    
    cmp_ok( $edt1->dbh, '==', $T->dbh, "fetch dbh" );
    cmp_ok( $edt1->transaction, 'eq', '', "fetch transaction before start" );
    cmp_ok( $edt1->perms, '==', $perm_a, "fetch perm_a" );
    cmp_ok( $edt1->role, 'eq', 'authorizer', "fetch role a" );
    cmp_ok( $edt2->perms, '==', $perm_g, "fetch perm_g" );
    cmp_ok( $edt2->role, 'eq', 'guest', "fetch role g" );
    
    ok( $edt1->debug, "fetch debug" );
    ok( ! $edt2->debug || $T->debug, "fetch debug 2" );
    
    $edt2->start_transaction;
    
    cmp_ok( $edt2->transaction, 'eq', 'active', "transaction is active" );

    $edt2->rollback;

    cmp_ok( $edt2->transaction, 'eq', 'aborted', "transaction is aborted" );
};


# Now test capturing debug output

subtest 'debug output' => sub {

    my $edt = $T->new_edt($perm_a, { DEBUG_MODE => 1, TEST_DEBUG => 1 });
    
    $edt->start_transaction;

    ok( $edt->has_debug_output( qr/START TRANSACTION/ ), "captured start from debug output" );
    
    $edt->rollback;

    ok( $edt->has_debug_output( qr/ROLLBACK TRANSACTION/ ), "captured rollback from debug output" );

    my $edt2 = $T->new_edt($perm_a, { DEBUG_MODE => 0, TEST_DEBUG => 1 });

    $edt2->start_transaction;
    
    ok( ! $edt2->has_debug_output( qr/START TRANSACTION/ ) || $T->debug ,
	"did not capture debug output without DEBUG_MODE" );
};


# Test that the transaction is rolled back when an EditTransaction goes out of scope, unless
# explicitly committed first.

subtest 'out of scope' => sub {
    
    my ($t1, $t2);
    
    $T->dbh->do("ROLLBACK");  # clear any pending transaction just in case
    
    # First try a transaction that is committed and then goes out of scope.

    {
	my $edt = $T->new_edt($perm_a);
	
	$edt->start_execution;
	
	$edt->insert_record($EDT_TEST, { signed_req => 222, string_req => 'insert this' });
	
	my ($active) = $T->dbh->selectrow_array("SELECT \@\@in_transaction");
	
	ok( $active, "transaction 1 is active while edt is in scope" );

	$edt->commit;
	
	$T->new_edt($perm_a);	# must remove saved reference from $T to previous $edt by
                                # generating a new one
    }
    
    my ($still_active) = $T->dbh->selectrow_array("SELECT \@\@in_transaction");

    ok( ! $still_active, "transaction 1 is no longer active" );
    
    my ($r) = $T->fetch_records_by_expr($EDT_TEST, "signed_req = '222'");

    ok( $r, "found record from transaction 1" );

    {
	my $edt = $T->new_edt($perm_a);
	
	$edt->start_execution;
	
	$edt->insert_record($EDT_TEST, { signed_req => 223, string_req => 'do not insert' });
	
	my ($active) = $T->dbh->selectrow_array("SELECT \@\@in_transaction");
	
	ok( $active, "in transaction 2 while edt is in scope" );

	$T->new_edt($perm_a);	# must remove saved reference from $T to previous $edt by
                                # generating a new one
    }
    
    ($still_active) = $T->dbh->selectrow_array("SELECT \@\@in_transaction");
    
    ok( ! $still_active, "transaction 2 is no longer active" );

    my ($r) = $T->fetch_records_by_expr($EDT_TEST, "signed_req = '223'");
    
    ok( ! $r, "transaction 2 was rolled back" );
};


