#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-10-transaction.t : Test that transaction execution, commit, and rollback work properly, and
# that the proper results and keys are returned from the relevant calls.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 7;

use TableDefs qw($EDT_TEST get_table_property);

use EditTest;
use EditTester;


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;



my ($perm_a, $primary);


subtest 'setup' => sub {
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');

    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;

    $primary = get_table_property($EDT_TEST, 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || BAIL_OUT;
};


# Check that executing a transaction works properly, and that the proper results are returned by
# each of the relevant calls. We test all of the four operations together in the same transaction.

subtest 'execute' => sub {

    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table($EDT_TEST);
    
    # Then run through a transaction that creates two records. We need to execute this one first so
    # that we can find out the keys of the newly inserted records.

    $edt = $T->new_edt($perm_a);
    
    $edt->insert_record($EDT_TEST, { string_req => 'execute test', signed_val => '3' });
    $edt->insert_record($EDT_TEST, { string_req => 'update test', signed_val => '3' });
    $edt->insert_record($EDT_TEST, { string_req => 'delete test' });
    $edt->execute;
    
    my ($key1, $key2, $key3) = $edt->inserted_keys;
    
    ok( $key1 && $key2 && $key3, "inserted three records" ) || return;
    is( $edt->inserted_keys, 3, "inserted only three records" );
    is( $edt->action_count, 3, "performed three actions" );
    
    # Then we run through a transaction that includes all four operations, and check that the
    # various status calls work properly.
    
    $edt = $T->new_edt($perm_a);
    
    is( $edt->transaction, '', "transaction status init" );
    ok( ! $edt->has_started, "transaction has not started" );
    ok( ! $edt->is_active, "transaction is not active" );
    ok( ! $edt->has_finished, "transaction has not finished" );
    ok( $edt->can_proceed, "transaction can proceed" );
    ok( ! $edt->{save_init_count}, "initialize_transaction has not yet run" );
    ok( ! $edt->{save_final_count}, "finalize_transaction has not yet run" );
    
    ok( $edt->insert_record($EDT_TEST, { string_req => 'insert test' }), "insert succeeded" );
    ok( $edt->replace_record($EDT_TEST, { $primary => $key1, string_req => 'execute updated' }), "replace succeeded" );
    ok( $edt->update_record($EDT_TEST, { $primary => $key2, string_req => 'also updated' }), "update succeeded" );
    ok( $edt->delete_record($EDT_TEST, $key3), "delete succeeded" );
    
    ok( ! $edt->inserted_keys, "insert not yet done" );
    ok( ! $edt->replaced_keys, "replace not yet done" );
    ok( ! $edt->updated_keys, "update not yet done" );
    ok( ! $edt->deleted_keys, "delete not yet done" );
    
    is( $edt->transaction, '', "transaction status init" );
    ok( ! $edt->has_started, "transaction has not started" );
    ok( ! $edt->is_active, "transaction is not active" );
    ok( ! $edt->has_finished, "transaction has not finished" );

    ok( $edt->commit, "execution succeeded" );
    
    is( $edt->transaction, 'committed', "transaction committed" );
    ok( $edt->has_started, "transaction has started" );
    ok( ! $edt->is_active, "transaction is not active" );
    ok( $edt->has_finished, "transaction has finished" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    is( $edt->action_count, 4, "performed three actions" );
    is( $edt->fail_count, 0, "no actions failed" );
    is( $edt->skip_count, 0, "no actions were skipped" );
    is( $edt->{save_init_count}, 1, "initialize_transaction has run once" );
    is( $edt->{save_final_count}, 1, "finalize_transaction has run once" );
    ok( ! $edt->{save_cleanup_count}, "cleanup_transaction did not run" );
    
    # Check that the proper keys are reported and that the proper changes have been made to the
    # database.
    
    my @ikeys = $edt->inserted_keys;
    my @rkeys = $edt->replaced_keys;
    my @ukeys = $edt->updated_keys;
    my @dkeys = $edt->deleted_keys;
    
    is( @ikeys, 1, "inserted one key" );
    is( @rkeys, 1, "replaced one key" ) && is( $rkeys[0], $key1, "replaced proper key" );
    is( @ukeys, 1, "updated one key" ) && is( $ukeys[0], $key2, "updated proper key" );
    is( @dkeys, 1, "deleted one key" ) && is( $dkeys[0], $key3, "deleted proper key" );
    
    my( $check1 ) = $edt->dbh->selectrow_array("
	SELECT $primary FROM $EDT_TEST WHERE string_req='insert test'");

    is( $check1, $ikeys[0], "found inserted record" );
    $T->ok_found_record($EDT_TEST, "string_req='execute updated'");
    $T->ok_no_record($EDT_TEST, "string_req='execute test'");
    $T->ok_found_record($EDT_TEST, "string_req='also updated'");
    $T->ok_no_record($EDT_TEST, "string_req='update test'");
    $T->ok_no_record($EDT_TEST, "string_req='delete test'");

    my ($r1) = $T->fetch_records_by_key($EDT_TEST, $key1);
    my ($r2) = $T->fetch_records_by_key($EDT_TEST, $key2);

    # Now check that 'replace' and 'update' each do their proper thing.
    
    is( $r1->{string_req}, 'execute updated', "replace check 1" );
    is( $r1->{signed_val}, undef, "replace check 2" );
    is( $r2->{string_req}, 'also updated', "update check 1" );
    is( $r2->{signed_val}, 3, "update check 2" );

    # Now check that we cannot execute record operations on an EditTransaction after it has
    # committed.
    
    eval {
	$edt->insert_record($EDT_TEST, { string_req => 'good record' });
    };

    like( $@, qr/committed/, "error message after commit" );

    # Check that we cannot call start_transaction either.

    eval {
	$edt->start_transaction;
    };

    like( $@, qr/committed/, "error message after commit" );

    # But we should be able to call 'commit' again, and also 'rollback'.
    
    my ($commit, $rollback);

    eval {
	$commit = $edt->commit;
	$rollback = $edt->rollback;
    };

    ok( !$@, "no errors on commit and rollback" ) || diag("message was: $@");
    
    ok( $commit, "second commit returns true, because transaction did succeed" );
    ok( ! $rollback, "rollback returns false, because it is too late to roll back" );
    is( $edt->transaction, 'committed', "transaction status is still 'committed'" );

    # Now check that we can call 'commit' before a transaction starts.

    $edt = $T->new_edt($perm_a);

    ok( ! $edt->commit, "commit returns false if called before transaction start" );
    ok( $edt->has_finished, 'transaction has finished' );
    is( $edt->transaction, 'finished', "status is 'finished'" );
};


# Check that IMMEDIATE_MODE works properly as well.

subtest 'immediate' => sub {

    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table($EDT_TEST);
    
    # Then create a transaction in IMMEDIATE_MODE, which causes all operations to be executed immediately.

    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    is( $edt->transaction, 'active', "transaction status active" );
    ok( $edt->has_started, "transaction has started" );
    ok( $edt->is_active, "transaction is active" );
    ok( ! $edt->has_finished, "transaction has not finished" );
    ok( $edt->can_proceed, "transaction can proceed" );
    is( $edt->{save_init_count}, 1, "initialize_transaction has run once" );
    ok( ! $edt->{save_final_count}, "finalize_transaction has not yet run" );
    
    $edt->insert_record($EDT_TEST, { string_req => 'first record' });

    # The following key will not be 1, because it is not the first record inserted.
    
    my $key1 = $edt->insert_record($EDT_TEST, { string_req => 'insert immediate' });
    
    if ( cmp_ok( $key1, '>', 1, "insert_record returned a valid key" ) )
    {
	my ($r1) = $T->fetch_records_by_key($EDT_TEST, $key1);
	$r1 && is( $r1->{string_req}, 'insert immediate', "found proper record" );
	
	$result = $edt->replace_record($EDT_TEST, { $primary => $key1, string_req => 'new value',
						    signed_val => '-8' });
	
	ok( $result eq '1' || $result eq '2', "replace_record returned 1 or 2 for success" ) ||
	    diag("result was " . $result // 'undef');
	
	$result = $edt->update_record($EDT_TEST, { $primary => $key1, signed_val => '-7' });
	
	is( $result, 1, "update_record returned 1 for success" ) || $T->diag_errors;

	$result = $edt->delete_record($EDT_TEST, { $primary => $key1 });

	is( $result, 1, "delete_record returnd 1 for success" ) || $T->diag_errors;

	$T->ok_no_record($EDT_TEST, "$primary=$key1", "record was deleted");
    }

    is( $edt->transaction, 'active', "transaction status is still active" );
    ok( $edt->has_started, "transaction has started" );
    ok( $edt->is_active, "transaction is active" );
    ok( $edt->can_proceed, "transaction can proceed" );
    ok( ! $edt->has_finished, "transaction not finished" );
    ok( ! $edt->{save_final_count}, "finalize_transaction has not yet run" );

    $result = $edt->commit;
    
    ok( $result, "transaction succeeded" );
    is( $edt->{transaction}, 'committed', "transaction has committed" );
    ok( $edt->has_started, "transaction has started" );
    ok( ! $edt->is_active, "transaction is not active" );
    ok( $edt->has_finished, "transaction has finished" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    is( $edt->{save_final_count}, 1, "finalize_transaction has run once" );
};


# Now test that 'start_transaction' and 'start_execution' work as they are supposed to.

subtest 'start' => sub {
    
    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table($EDT_TEST);
    
    # Then create a transaction, and check its initial conditions.
    
    $edt = $T->new_edt($perm_a);
    
    is( $edt->transaction, '', "transaction status init" );
    ok( ! $edt->{save_init_count}, "initialize_transaction has not run" );
    
    # Start the transaction, and make sure that all the appropriate calls return the proper
    # values.
    
    ok( $edt->start_transaction, "start_transaction returns true" );
    
    is( $edt->transaction, 'active', "transaction status is 'active'" );
    ok( $edt->has_started, "transaction has started" );
    ok( $edt->is_active, "transaction is active" );
    ok( ! $edt->has_finished, "transaction has not finished" );
    ok( $edt->can_proceed, "transaction can proceed" );
    is( $edt->{save_init_count}, 1, "initialize_transaction ran once" );

    # Check that calling start_transaction again is harmless.

    eval {
	$result = $edt->start_transaction;
    };
    
    ok( !$@, "no exception when start_transaction is called again" );
    ok( $result, "start_transaction returned true when called again" );
    
    # Try inserting a record.
    
    $edt->insert_record($EDT_TEST, { string_req => 'insert test' });
    
    # The record should not have been inserted yet, because we have started the transaction but
    # have not started execution of operations.
    
    $T->ok_no_record($EDT_TEST, "string_req='insert test'");
    
    ok( $edt->start_execution, "start_execution returns true" );

    ok( $edt->has_started, "transaction is still started" );
    ok( $edt->is_active, "transaction is still active" );
    
    # Now the inserted record should actually be in the table.
    
    $T->ok_found_record($EDT_TEST, "string_req='insert test'");
    
    # Insert another record, and check that it goes into the table as well.
    
    $edt->insert_record($EDT_TEST, { string_req => 'second insert' });
    
    $T->ok_found_record($EDT_TEST, "string_req='second insert'");
    
    # Check that calling start_execution again is harmless.

    eval {
	$result = $edt->start_execution;
    };
    
    ok( !$@, "no exception when start_execution is called again" );
    ok( $result, "start_execution returned true when called again" );
    
    # Now commit the transaction.

    $result = $edt->execute;

    ok( $result, "transaction succeeded" );
    ok( $edt->has_started, "transaction is still started" );
    ok( $edt->has_finished, "transaction has finished" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    is( $edt->transaction, 'committed', "transaction has committed" );
    $T->ok_found_record($EDT_TEST, "string_req='second insert'");
    
    is( $edt->{save_init_count}, 1, "initialize_transaction ran once" );
    is( $edt->{save_final_count}, 1, "finalize_transaction ran once" );
    ok( ! $edt->{save_cleanup_count}, "cleanup_transaction didn't run" );
};


# Now test what happens when errors occur during the transaction.

subtest 'errors' => sub {
    
    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table($EDT_TEST);
    
    # Then create a transaction, and generate an error on the second
    # insertion. This error will be there before the transaction even
    # executes.
    
    $edt = $T->new_edt($perm_a);
    
    ok( $edt->insert_record($EDT_TEST, { string_req => 'insert okay' }), "insert succeeded" );
    ok( ! $edt->insert_record($EDT_TEST, { string_val => 'string_req is empty' }), "insert failed" );
    ok( ! $edt->insert_record($EDT_TEST, { string_req => 'after error' }), "insert failed" );
    
    is( $edt->errors, 1, "one error was generated" );
    ok( ! $edt->has_started, "transaction has not started" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    
    # Check that start_transaction does not generate an exception but returns false, and the same
    # for commit.

    eval {
	$result = $edt->start_transaction;
	ok( ! $result, "start_transaction failed" );
	$result = $edt->commit;
	ok( ! $result, "transaction failed" );
    };

    ok( !$@, "no exception on start_transaction or commit" );
    
    ok( $edt->has_started, "transaction has started" );
    ok( $edt->has_finished, "transaction has finished" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    is( $edt->transaction, 'finished', "transaction neither committed nor aborted" );
    ok( ! $edt->{save_init_count}, "initialize_transaction did not run" );
    ok( ! $edt->{save_init_count}, "finalize_transaction did not run" );
    ok( ! $edt->{save_init_count}, "cleanup_transaction did not run" );
    ok( ! $edt->failed_keys, "insertion did not leave a failed key" );
    is( $edt->record_count, 3, "received 3 records" );
    is( $edt->fail_count, 1, "one action failed" );
    is( $edt->action_count, 0, "no actions succeeded" );
    $T->ok_no_record($EDT_TEST, "string_req='insert okay'");
    
    # Check that we can continue to do operations on a transaction that has failed. Records that
    # fail validation will be added to 'fail_count', but those that pass validation will not.

    eval {
	ok( ! $edt->insert_record($EDT_TEST, { string_req => 'good record' }), "insert failed" );
	is( $edt->fail_count, 1, "fail count was not incremented" );
	ok( ! $edt->insert_record($EDT_TEST, { string_val => 'bad record' }), "insert failed" );
	is( $edt->fail_count, 2, "fail count was incremented" );
	is( $edt->errors, 2, "two errors were generated" );
	is( $edt->record_count, 5, "received 5 records in total" );
    };

    ok( !$@, "no errors on inserts after failure" ) || diag("message was: $@");    
    
    # Check that we can call 'commit' and 'rollback' on this failed transaction. Both should
    # return false.

    my ($commit, $rollback);

    eval {
	$commit = $edt->commit;
	$rollback = $edt->rollback;
    };
    
    ok( !$@, "no errors on commit and rollback" ) || diag("message was: $@");
    
    ok( ! $commit, "commit returns false, because transaction did succeed" );
    ok( ! $rollback, "rollback returns false, because it is too late to roll back" );
    is( $edt->transaction, 'finished', "transaction status is still 'finished'" );
    
    # Now create another transaction, but this time start execution. Do the same sequence of
    # operations. The third insertion should not actually be done, since the second one already
    # failed. Therefore we should have only one error. But we have two failed actions.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    ok( $edt->insert_record($EDT_TEST, { string_req => 'insert okay' }), "insert succeeded" );
    ok( ! $edt->insert_record($EDT_TEST, { string_val => 'string_req is empty' }), "insert failed" );
    ok( ! $edt->insert_record($EDT_TEST, { string_req => 'after error' }), "insert failed" );
    
    is( $edt->transaction, 'active', "transaction is active" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    is( $edt->errors, 1, "one error was generated" );
    
    $result = $edt->execute;
    
    ok( ! $result, "transaction failed" );
    ok( $edt->has_finished, "transaction has finished" );
    is( $edt->transaction, 'aborted', "transaction aborted" );
    is( $edt->{save_init_count}, 1, "initialize_transaction ran once" );
    ok( ! $edt->{save_final_count}, "finalize_transaction did not run" );
    is( $edt->{save_cleanup_count}, 1, "cleanup_transaction ran once" );
    is( $edt->errors, 1, "error count did not change" );
    is( $edt->record_count, 3, "received 3 records" );
    is( $edt->action_count, 1, "one action succeeded although transaction failed" );
    is( $edt->fail_count, 1, "one action failed" );
    is( $edt->skip_count, 1, "one action was skipped" );
    $T->ok_no_record($EDT_TEST, "string_req='insert okay'");
    
    # Now we try again, but reverse the order of the final two inserts.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    ok( $edt->insert_record($EDT_TEST, { string_req => 'insert okay' }), "insert succeeded" );
    ok( ! $edt->insert_record($EDT_TEST, { string_req => 'after error' }), "insert failed" );
    ok( ! $edt->insert_record($EDT_TEST, { string_val => 'string_req is empty' }), "insert failed" );
    
    is( $edt->transaction, 'active', "transaction is active" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    is( $edt->errors, 2, "two errors were generated" );
    
    ok( ! $edt->{save_cleanup_count}, "cleanup_transaction has not executed" );
    
    $result = $edt->execute;
    
    is( $edt->transaction, 'aborted', "transaction aborted" );
    is( $edt->errors, 2, "error count did not change" );
    is( $edt->record_count, 3, "received 3 records" );
    is( $edt->action_count, 1, "one action succeeded although transaction failed" );
    is( $edt->fail_count, 2, "two actions failed" );
    is( $edt->skip_count, 0, "no actions were skipped" );
    is( $edt->{save_cleanup_count}, 1, "cleanup_transaction executed once" );
    $T->ok_no_record($EDT_TEST, "string_req='insert okay'");
    
    # Check that we can call 'commit' and 'rollback' on this failed transaction. The first should
    # return false, the second true since the transaction was in fact rolled back.
    
    my ($commit, $rollback);
    
    eval {
	$commit = $edt->commit;
	$rollback = $edt->rollback;
    };
    
    ok( !$@, "no errors on commit and rollback" ) || diag("message was: $@");
    
    ok( ! $commit, "commit returns false, because transaction did not succeed" );
    ok( $rollback, "rollback returns true, because the transaction was rolled back" );
    is( $edt->transaction, 'aborted', "transaction status is still 'aborted'" );
};


# Now test what happens when we explicitly roll back a transaction.

subtest 'rollback' => sub {

    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table($EDT_TEST);
    
    # Then create a transaction, and add some records.
    
    $edt = $T->new_edt($perm_a);
    
    $edt->insert_record($EDT_TEST, { string_req => 'test a1' });
    $edt->insert_record($EDT_TEST, { string_req => 'test a2' });

    # Check the status, and then roll it back.

    ok( ! $edt->is_active, "transaction is not yet active" );
    ok( $edt->can_proceed, "transaction can proceed" );
    
    $result = $edt->rollback;
    
    ok( ! $result, "rollback returned false because the transaction was not yet active" );
    ok( ! $edt->is_active, "transaction is still not active" );
    ok( ! $edt->can_proceed, "transaction can not proceed" );
    ok( $edt->has_finished, "transaction has finished" );
    is( $edt->transaction, 'finished', "transaction status is 'finished'" );
    ok( ! $edt->errors, "no errors on this transaction" );
    
    # Check that the transaction really was rolled back.
    
    $T->ok_no_record($EDT_TEST, "string_req in ('test a1', 'test a2')");
    
    # Check that calling rollback a second time is okay, and that commit is
    # okay too. Both should return false.
    
    eval {
	ok( ! $edt->rollback, "rollback returned false a second time" );
	ok( ! $edt->commit, "commit returned false after rollback" );
    };
    
    ok( !$@, "no error message on commit or rollback" ) || diag("message was: $@");
    is( $edt->transaction, 'finished', "transaction status is unchanged" );
    
    # Now try the same thing under IMMEDIATE_MODE.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt->insert_record($EDT_TEST, { string_req => 'test b1' });
    $edt->insert_record($EDT_TEST, { string_req => 'test b2' });

    ok( $edt->is_active, "transaction is active" );
    ok( $edt->can_proceed, "transaction can proceed" );

    # Check that the records are in the table.

    $T->ok_found_record($EDT_TEST, "string_req in ('test b1', 'test b2')");
    
    # Then roll back the transaction.
    
    $result = $edt->rollback;
    
    ok( $result, "rollback returned true because an active transaction was rolled back" );
    ok( ! $edt->is_active, "transaction is no longer active" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    ok( $edt->has_finished, "transaction has finished" );
    is( $edt->transaction, 'aborted', "transaction status is 'aborted'" );
    ok( ! $edt->errors, "no errors on this transaction" );

    # Check that the transaction really was rolled back.
    
    $T->ok_no_record($EDT_TEST, "string_req in ('test b1', 'test b2')");
    
    # Now try the same thing after an error condition has been added.

    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt->insert_record($EDT_TEST, { string_req => 'test c1' });
    $edt->insert_record($EDT_TEST, { string_req => 'test c2' });
    $edt->add_condition(undef, 'E_EXECUTE', 'test error');
    
    ok( $edt->is_active, "transaction is active" );
    ok( ! $edt->can_proceed, "transaction can not proceed" );
    
    # Check that the records are in the table.

    $T->ok_found_record($EDT_TEST, "string_req in ('test c1', 'test c2')");
    
    # Then roll back the transaction.
    
    $result = $edt->rollback;
    
    ok( $result, "rollback returned true because an active transaction was rolled back" );
    ok( ! $edt->is_active, "transaction is no longer active" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    ok( $edt->has_finished, "transaction has finished" );
    is( $edt->transaction, 'aborted', "transaction status is 'aborted'" );
    is( $edt->errors, 1, "one error on this transaction" );
    
    # Check that the transaction really was rolled back.
    
    $T->ok_no_record($EDT_TEST, "string_req in ('test c1', 'test c2')");
    
    # Check that calling rollback a second time is okay, and that commit is
    # okay too. Both should return false.
    
    eval {
	ok( $edt->rollback, "rollback returned true a second time" );
	ok( ! $edt->commit, "commit returned false after rollback" );
    };
    
    ok( !$@, "no error message on commit or rollback" ) || diag("message was: $@");
    is( $edt->transaction, 'aborted', "transaction status is unchanged" );
};


# And again under both IMMEDIATE_MODE and PROCEED_MODE.

subtest 'immediate and proceed' => sub {

    pass("placeholder");






};

