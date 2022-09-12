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
use Test::More tests => 8;

use TableDefs qw(%TABLE get_table_property);

use EditTest;
use EditTester;


$DB::single = 1;

# The following call establishes a connection to the database, using EditTester.pm and selecting
# EditTest as the subclass of EditTransaction to instantiate.

my $T = EditTester->new({ subclass => 'EditTest' });

my ($perm_a, $primary);


subtest 'setup' => sub {
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');

    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;

    $primary = get_table_property('EDT_TEST', 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || BAIL_OUT;
};


# Check that executing a transaction works properly, and that the proper results are returned by
# each of the relevant calls. Run through these checks twice, once with execution off and once
# with it on.

subtest 'transaction results' => sub {

    my ($edt, $result);
    
    foreach my $mode ( 'regular', 'immediate' )
    {
	my $ms = $mode eq 'regular' ? "" : " (immediate mode)";
	my $astatus = $mode eq 'regular' ? 'pending' : 'executed';
	my ($insert_refstring, $insert_keyval);
	
	# Clear the table so we can check for proper record insertion.
	
	$T->clear_table('EDT_TEST');
	
	# First execute a transaction that creates three records, and get the keys of the newly
	# inserted records.
	
	$edt = $T->new_edt($perm_a);
	
	$edt->insert_record('EDT_TEST', { string_req => 'execute test', signed_val => '3' });
	$edt->insert_record('EDT_TEST', { string_req => 'update test', signed_val => '3' });
	$edt->insert_record('EDT_TEST', { string_req => 'delete test' });
	
	$T->ok_commit || return;
	
	my ($key1, $key2, $key3) = $edt->inserted_keys;
	
	ok( $key1 && $key2 && $key3, "got three keys$ms" ) || diag "aborting test" && return;
	
	# Then execute a transaction that includes all four of the basic operations, and check
	# that the various status calls work properly. The second time through, call
	# 'start_execution' which will cause each action to be executed immediately.
	
	if ( $mode eq 'regular' )
	{
	    $edt = $T->new_edt($perm_a);
	}
	
	else
	{
	    $edt = $T->new_edt($perm_a);
	    $edt->start_execution;
	}
	
	if ( $mode eq 'regular' )
	{
	    is( $edt->transaction, '', "transaction has not initialized$ms" );
	    ok( ! $edt->has_started, "transaction has not started$ms" );
	    ok( ! $edt->is_active, "transaction is not active$ms" );
	    ok( ! $edt->is_executing, "transaction is not executing$ms" );
	    ok( ! $edt->{save_init_count}, "initialize_transaction has not run$ms" );
	}
	
	else
	{
	    is( $edt->transaction, 'active', "transaction status is 'active'$ms" );
	    ok( $edt->has_started, "transaction has started$ms" );
	    ok( $edt->is_active, "transaction is active$ms" );
	    ok( $edt->is_executing, "transaction is executing$ms" );
	    ok( $edt->{save_init_count}, "initialize_transaction has run$ms" );
	}
	
	ok( ! $edt->has_finished, "transaction has not finished$ms" );
	ok( ! $edt->has_committed, "transaction has not committed$ms" );
	ok( ! $edt->has_failed, "transaction has not failed$ms" );
	ok( $edt->can_proceed, "transaction can proceed$ms" );
	ok( ! $edt->{save_final_count}, "finalize_transaction has not run$ms" );
	
	$edt->insert_record('EDT_TEST', { string_req => 'insert test' });
	
	$insert_refstring = $edt->current_action;
	$insert_keyval = $edt->get_keyval;
	
	if ( $mode eq 'regular' )
	{
	    is( $edt->exec_count, 0, "action was not executed$ms" );
	    is ( $edt->action_status, 'pending', "action status is 'pending'$ms" );
	    ok( ! defined $insert_keyval, "insert did not generate a key value" );
	}

	else
	{
	    is( $edt->exec_count, 1, "action was executed$ms" );
	    is( $edt->action_status, 'executed', "action status is 'executed'$ms" );
	    ok( $insert_keyval, "insert generated a key value" );
	}
	
	$edt->replace_record('EDT_TEST', { $primary => $key1, string_req => 'execute updated' });
	    
	is( $edt->action_status, $astatus, "replace status is '$astatus'$ms");
	is( $edt->action_keyval, $key1, "replace keyval is correct" );
	
	$edt->update_record('EDT_TEST', { $primary => $key2, string_req => 'also updated' });
	
	is( $edt->action_status, $astatus, "update status is '$astatus'$ms");
	is( $edt->action_keyval, $key2, "update keyval is correct" );
	
	$edt->delete_record('EDT_TEST', $key3);
	
	is( $edt->action_status, $astatus, "delete status is '$astatus'$ms");
	is( $edt->action_keyval, $key3, "delete keyval is correct" );
	
	if ( $mode eq 'regular' )
	{
	    is( $edt->record_count, 4, "4 actions have been submitted" );
	    is( $edt->exec_count, 0, "0 actions have been executed$ms" );
	    
	    ok( ! $edt->inserted_keys, "insert not yet done$ms" );
	    ok( ! $edt->replaced_keys, "replace not yet done$ms" );
	    ok( ! $edt->updated_keys, "update not yet done$ms" );
	    ok( ! $edt->deleted_keys, "delete not yet done$ms" );
	    
	    is( $edt->transaction, '', "transaction has not initialized$ms" );
	    ok( ! $edt->has_started, "transaction has not started$ms" );
	    ok( ! $edt->is_active, "transaction is not active$ms" );
	    ok( ! $edt->is_executing, "transaction is not executing$ms" );
	    ok( ! $edt->{save_init_count}, "initialize_transaction has not run" );
	}
	
	else
	{
	    is( $edt->record_count, 4, "4 actions have been submitted" );
	    is( $edt->exec_count, 4, "4 actions have been executed$ms" );
	    
	    is( $edt->inserted_keys, 1, "one record was inserted$ms" );
	    is( $edt->replaced_keys, 1, "one record was replaced$ms" );
	    is( $edt->updated_keys, 1, "one record was updated$ms" );
	    is( $edt->deleted_keys, 1, "one record was deleted$ms" );
	    
	    is( $edt->transaction, 'active', "transaction status is 'active'$ms" );
	    ok( $edt->has_started, "transaction has started$ms" );
	    ok( $edt->is_active, "transaction is active$ms" );
	    ok( $edt->is_executing, "transaction is executing$ms" );
	}
	
	ok( ! $edt->has_finished, "transaction has not finished$ms" );
	ok( ! $edt->has_committed, "transaction has not committed$ms" );
	ok( ! $edt->has_failed, "transaction has not failed$ms" );
	ok( $edt->can_proceed, "transaction can proceed$ms" );
	ok( ! $edt->{save_final_count}, "finalize_transaction has not yet run$ms" );
	
	$T->ok_commit("test transaction has committed$ms");
	
	is( $edt->transaction, 'committed', "transaction status is 'committed'$ms" );
	ok( $edt->has_started, "transaction has started$ms" );
	ok( ! $edt->is_active, "transaction is not active$ms" );
	ok( ! $edt->is_executing, "transaction is not executing$ms" );
	ok( $edt->has_finished, "transaction has finished$ms" );
	ok( ! $edt->can_proceed, "transaction cannot proceed$ms" );
	ok( $edt->has_committed, "transaction has committed$ms" );
	ok( ! $edt->has_failed, "transaction has not failed$ms" );
	
	is( $edt->action_status($insert_refstring), 'executed', "action status is 'executed'$ms");
	is( $edt->record_count, 4, "four actions submitted$ms" );
	is( $edt->action_count, 4, "four actions recorded$ms" );
	is( $edt->exec_count, 4, "four actions performed$ms" );
	is( $edt->fail_count, 0, "no actions failed$ms" );
	is( $edt->skip_count, 0, "no actions skipped$ms" );
	is( $edt->{save_init_count}, 1, "initialize_transaction has run once$ms" );
	is( $edt->{save_final_count}, 1, "finalize_transaction has run once$ms" );
	ok( ! $edt->{save_cleanup_count}, "cleanup_transaction did not run$ms" );
	
	# Check that the proper keys are reported and that the proper changes have been made to the
	# database.
	
	my @ikeys = $edt->inserted_keys;
	my @rkeys = $edt->replaced_keys;
	my @ukeys = $edt->updated_keys;
	my @dkeys = $edt->deleted_keys;
	
	is( @ikeys, 1, "inserted one key$ms" );
	is( @rkeys, 1, "replaced one key$ms" ) && is( $rkeys[0], $key1, "replaced proper key$ms" );
	is( @ukeys, 1, "updated one key$ms" ) && is( $ukeys[0], $key2, "updated proper key$ms" );
	is( @dkeys, 1, "deleted one key$ms" ) && is( $dkeys[0], $key3, "deleted proper key$ms" );
	
	my ($check1) = $T->find_record_values('EDT_TEST', $primary, "string_req='insert test'");
	
	is( $check1, $ikeys[0], "insertion executed properly$ms" );
	$T->ok_found_record('EDT_TEST', "string_req='execute updated'", "replace executed properly a$ms");
	$T->ok_no_record('EDT_TEST', "string_req='execute test'", "replace executed properly b$ms");
	$T->ok_found_record('EDT_TEST', "string_req='also updated'", "update executed properly a$ms");
	$T->ok_no_record('EDT_TEST', "string_req='update test'", "update executed properly b$ms");
	$T->ok_no_record('EDT_TEST', "string_req='delete test'", "delete executed properly$ms");
	
	# Additional checks to make sure that 'replace' and 'update' each functioned properly.
	
	my ($r1) = $T->fetch_records_by_key('EDT_TEST', $key1);
	my ($r2) = $T->fetch_records_by_key('EDT_TEST', $key2);
	
	is( $r1->{string_req}, 'execute updated', "replace executed properly c$ms" );
	is( $r1->{signed_val}, undef, "replace executed properly d$ms" );
	is( $r2->{string_req}, 'also updated', "update executed properly c$ms" );
	is( $r2->{signed_val}, 3, "update executed properly d$ms" );
    }
    
    # Now check that we cannot execute record operations on an EditTransaction after it has
    # committed.
    
    eval {
	$edt->insert_record('EDT_TEST', { string_req => 'good record' });
    };
    
    like( $@, qr/finished|committed/, "exception on insert_record after commit" );
    
    # Check that we cannot call start_transaction either.
    
    eval {
	$edt->start_transaction;
    };
    
    like( $@, qr/finished/, "exception on start_transaction after commit" );
    
    # But we should be able to call 'commit' again, and also 'execute' and 'rollback'.
    
    my ($commit, $execute, $rollback);
    
    eval {
	$commit = $edt->commit;
	$execute = $edt->execute;
	$rollback = $edt->rollback;
    };
    
    ok( !$@, "no exception on commit, execute, and rollback" ) || diag("exception was: $@");
    
    ok( $commit, "second commit returns true, because transaction succeeded" );
    ok( ! $execute, "execute returns false, because transaction can no longer proceed");
    ok( ! $rollback, "rollback returns false, because it is too late to roll back" );
    ok( $edt->has_committed, "has_committed still returns true" );
    ok( ! $edt->has_failed, "has_failed still returns false" );
    is( $edt->transaction, 'committed', "transaction status is still 'committed'" );
    
    # Now check that we can call 'commit' before a transaction starts.
    
    $edt = $T->new_edt($perm_a);
    
    ok( $edt->commit, "commit returns true if called before transaction start" );
    ok( $edt->has_finished, 'transaction has finished' );
    ok( $edt->has_committed, 'transaction has committed' );
    is( $edt->transaction, 'committed', "status is 'committed'" );

    eval {
	$edt->insert_record('EDT_TEST', { string_req => 'exception expected' });
    };

    ok( $@, "exception thrown by insert_record after commit" );
};


# Additional tests to make sure that actions are executed immediately if IMMEDIATE_MODE is specified.

subtest 'immediate_mode' => sub {

    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    
    # Then create a transaction in IMMEDIATE_MODE, which causes all operations to be executed immediately.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    is( $edt->transaction, 'active', "transaction status active" );
    ok( $edt->has_started, "transaction has started" );
    ok( $edt->is_active, "transaction is active" );
    ok( ! $edt->has_finished, "transaction has not finished" );
    ok( $edt->can_proceed, "transaction can proceed" );
    is( $edt->{save_init_count}, 1, "initialize_transaction has run once" );
    ok( ! $edt->{save_final_count}, "finalize_transaction has not yet run" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'first record' });
    
    # The following key will not be 1, because it is not the first record inserted.
    
    my $key1 = $edt->insert_record('EDT_TEST', { string_req => 'insert immediate' }, 'keyval')
	&& $edt->get_keyval;
    
    if ( cmp_ok( $key1, '>', 1, "insert_record returned a valid key" ) )
    {
	my ($r1) = $T->fetch_records_by_key('EDT_TEST', $key1);
	$r1 && is( $r1->{string_req}, 'insert immediate', "found proper record" );
	
	$result = $edt->replace_record('EDT_TEST', { $primary => $key1, string_req => 'new value',
						     signed_val => '-8' })
	    && $edt->action_status;
	
	ok( $result eq 'executed', "replace_record executed successfully" ) ||
	    diag("result was " . $result // 'undef');
	
	$result = $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => '-7' })
	    && $edt->action_status;
	
	is( $result, 'executed', "update_record executed successfully" ) || $T->diag_errors;
	
	$result = $edt->delete_record('EDT_TEST', { $primary => $key1 }) && $edt->action_status;
	
	is( $result, 'executed', "delete_record executed successfully" ) || $T->diag_errors;
	
	$T->ok_no_record('EDT_TEST', "$primary=$key1", "record was deleted");
    }

    $T->ok_commit;
};


# Test that 'start_transaction', 'start_execution', 'pause_execution', and 'execute' work as they
# are supposed to.

subtest 'execution control' => sub {
    
    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    
    # Then create a transaction, and check its initial conditions.
    
    $edt = $T->new_edt($perm_a);
    
    is( $edt->transaction, '', "transaction not yet initialized" );
    ok( ! $edt->{save_init_count}, "initialize_transaction has not run" );
    
    # Start the transaction, and make sure that all the appropriate calls return the proper
    # values.
    
    ok( $edt->start_transaction, "start_transaction returns true" );
    
    is( $edt->transaction, 'active', "transaction status is 'active'" );
    ok( $edt->has_started, "transaction has started" );
    ok( $edt->is_active, "transaction is active" );
    ok( ! $edt->has_finished, "transaction has not finished" );
    ok( $edt->can_proceed, "transaction can proceed" );
    ok( ! $edt->is_executing, "transaction is not executing" );
    is( $edt->{save_init_count}, 1, "initialize_transaction ran once" );
    
    # Check that calling start_transaction again is harmless.
    
    eval {
	$result = $edt->start_transaction;
    };
    
    ok( !$@, "no exception when start_transaction is called again" );
    ok( $result, "start_transaction returned true when called again" );
    ok( ! $edt->is_executing, "transaction is still not executing" );
    
    # Try inserting a record.
    
    $edt->insert_record('EDT_TEST', { string_req => 'insert test' });
    my $action1 = $edt->current_action;
    my $key1 = $edt->get_keyval;
    
    # The record should not have been inserted yet, because we have started the transaction but
    # have not started execution of operations.
    
    $T->ok_no_record('EDT_TEST', "string_req='insert test'");
    
    ok( ! defined $key1, "no inserted key value" );
    is( $edt->action_status($action1), 'pending', "status of first action is 'pending'" );
    
    ok( $edt->start_execution, "start_execution returns true" );
    
    ok( $edt->has_started, "transaction is still started" );
    ok( $edt->is_active, "transaction is still active" );
    ok( $edt->is_executing, "actions now execute immediately" );
    
    # The key value for the last action should now be available.
    
    $key1 = $edt->get_keyval;
    ok( $key1, "got inserted key value" );
    is( $edt->action_status, 'executed', "status of previous action is 'executed'" );
    
    # The inserted record should actually be in the table.
    
    $T->ok_found_record('EDT_TEST', "string_req='insert test'");
    
    # Insert another record, and check that it goes into the table as well.
    
    $edt->insert_record('EDT_TEST', { string_req => 'second insert' });
    
    my $key2 = $edt->get_keyval;
    ok( $key2, "got second inserted key value" );
    is( $edt->action_status, 'executed', "status of second action is 'executed'" );
    
    $T->ok_found_record('EDT_TEST', "string_req='second insert'");
    
    is( $edt->action_count, 2, "2 actions submitted" );
    is( $edt->exec_count, 2, "2 actions executed" );
    
    is( $edt->action_status($action1), 'executed', "status of first action is now 'executed'" );
    
    # Check that calling start_execution again is harmless.
    
    eval {
	$result = $edt->start_execution;
    };
    
    ok( !$@, "no exception when start_execution is called again" );
    ok( $result, "start_execution returned true when called again" );
    
    # Pause execution, then do two more inserts, an update and a delete.
    
    ok( $edt->pause_execution, "pause_execution returns true" );
    ok( ! $edt->is_executing, "is_executing returns false" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'extra aaa' });
    $edt->insert_record('EDT_TEST', { string_req => 'extra bbb' });
    $edt->update_record('EDT_TEST', { _primary => $key2, string_req => 'extra ccc' });
    $edt->delete_record('EDT_TEST', { _primary => $key1 });
    
    is( $edt->action_count, 6, "6 actions submitted" );
    is( $edt->exec_count, 2, "2 actions executed" );
    
    # Call execute, which should execute all pending actions but not turn on execution for
    # subsequent actions.

    ok( $edt->execute, "execute returns true" );

    is( $edt->action_count, 6, "still 6 actions submitted" );
    is( $edt->exec_count, 6,  "now 6 actions executed" );
    
    $T->ok_count_records(3, 'EDT_TEST', "string_req like 'extra %'", "actions are reflected in databae" );
    $T->ok_no_record('EDT_TEST', "string_req='insert_test'", "delete is reflected in database");
    
    ok( ! $edt->is_executing, "is_executing still returns false" );
    
    # Add another action, and verify that it doesn't execute.
    
    $edt->update_record('EDT_TEST', { _primary => $key2, string_val => 'updated xiflx' });
    
    is( $edt->action_count, 7, "now 7 actions submitted" );
    is( $edt->exec_count, 6, "still 6 actions executed" );
    $T->ok_no_record('EDT_TEST', "string_val='updated xiflx'", "update not reflected in database");
    
    # Turn execution on again, and verify that it has.
    
    ok( $edt->start_execution, "start_execution returns true" );
    
    is( $edt->exec_count, 7, "now 7 actions executed" );
    $T->ok_found_record('EDT_TEST', "string_val='updated xiflx'", "update is now reflected in database");
    
    # Turn it off again, and add one more action.
    
    ok( $edt->pause_execution, "pause_execution returns true" );

    $edt->update_record('EDT_TEST', { _primary => $key2, string_val => 'final xiflx' });

    is( $edt->action_count, 8, "now 8 actions submitted" );
    is( $edt->exec_count, 7, "still 7 actions executed" );
    $T->ok_no_record('EDT_TEST', "string_val='final xiflx'", "final update has not occurred");
    
    # Now commit the transaction.
    
    $T->ok_commit;
    
    $T->ok_found_record('EDT_TEST', "string_val='final xiflx'", "final update has occurred");
    is( $edt->exec_count, 8, "now 8 actions executed" );
    
    ok( $edt->has_started, "transaction is still started" );
    ok( $edt->has_finished, "transaction has finished" );
    ok( $edt->has_committed, "transaction has committed" );
    ok( ! $edt->has_failed, "transaction has not failed" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    is( $edt->transaction, 'committed', "transaction has committed" );
    
    is( $edt->{save_init_count}, 1, "initialize_transaction ran once" );
    is( $edt->{save_final_count}, 1, "finalize_transaction ran once" );
    ok( ! $edt->{save_cleanup_count}, "cleanup_transaction didn't run" );
};


# Now test what happens when errors occur during the transaction.

subtest 'errors' => sub {
    
    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    
    # Then create a transaction, and generate an error on the second
    # insertion. This error will be there before the transaction even
    # executes.
    
    $edt = $T->new_edt($perm_a);

    $edt->insert_record('EDT_TEST', { string_req => 'insert okay' });
    my $action1 = $edt->current_action;
    $T->ok_action;
    is( $edt->action_status($action1), 'pending', "action1 status is 'pending'" );
    
    $edt->insert_record('EDT_TEST', { string_val => 'string_req is empty' });
    $T->ok_failed_action('failed', "insert status is 'failed' with bad parameters");
    
    is( $edt->action_status($action1), 'aborted', "status of first insertion is now 'aborted");
    
    $edt->insert_record('EDT_TEST', { string_req => 'not executed' });
    $T->ok_failed_action('aborted', "insert status is 'aborted' after prior error");
    
    $edt->insert_record('EDT_TEST', { string_req => 'foobar',
				      _errwarn => ['E_PARAM', "parameter error"] });
    $T->ok_failed_action('failed', "insert status is 'failed' with imported error");
    
    is( $edt->errors, 2, "two error conditions" );
    ok( ! $edt->has_started, "transaction has not started" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    
    # Check that start_transaction does not generate an exception but returns false, and the same
    # for execute.
    
    eval {
	$result = $edt->start_transaction;
	ok( ! $result, "start_transaction failed" );
	$result = $edt->execute;
	ok( ! $result, "execution failed" );
    };
    
    ok( !$@, "no exception on start_transaction or commit" );
    
    ok( ! $edt->has_started, "transaction has not started" );
    ok( ! $edt->has_finished, "transaction has not finished" );
    ok( ! $edt->has_committed, "transaction has not committed" );
    ok( ! $edt->has_failed, "transaction has not failed" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    ok( ! $edt->{save_init_count}, "initialize_transaction did not run" );
    ok( ! $edt->{save_final_count}, "finalize_transaction did not run" );
    ok( ! $edt->{save_cleanup_count}, "cleanup_transaction did not run" );
    ok( ! $edt->failed_keys, "insertion did not leave a failed key" );
    is( $edt->record_count, 4, "received four actions" );
    is( $edt->action_count, 4, "recorded four actions" );
    is( $edt->fail_count, 2, "two actions failed" );
    is( $edt->skip_count, 2, "two actions skipped" );
    is( $edt->exec_count, 0, "no actions succeeded" );
    
    $T->ok_no_record('EDT_TEST', "string_req='insert okay'");
    
    # Now attempt to commit the transaction, and check that the result is false.
    
    ok( ! $edt->commit, "commit failed" );
    ok( $edt->has_finished, "transaction has now finished" );
    ok( $edt->has_committed, "transaction has not committed" );
    ok( $edt->has_failed, "transactio has failed" );
    is( $edt->transaction, 'failed', "transaction status is 'failed'" );
    
    # Check that we can call 'commit' and 'rollback' on this failed transaction. The first should
    # return false, the second true.
    
    my ($commit, $rollback);
    
    eval {
	$commit = $edt->commit;
	$rollback = $edt->rollback;
    };
    
    ok( !$@, "no errors on commit and rollback" ) || diag("message was: $@");
    
    ok( ! $commit, "commit returns false, because transaction did not succeed" );
    ok( $rollback, "rollback returns true, because the transaction has been rolled back" );
    is( $edt->transaction, 'failed', "transaction status is still 'failed'" );
    
    # Now create another transaction, but this time start execution. Do the same sequence of
    # operations. The third insertion should not actually be executed, since the second one
    # already failed. Therefore we should have only one error. But we have two failed actions.
    
    $edt = $T->new_edt($perm_a);
    $edt->start_execution;
    
    $edt->insert_record('EDT_TEST', { string_req => 'insert okay' });
    $T->ok_action;
    
    $edt->insert_record('EDT_TEST', { string_val => 'string_req is empty' });
    $T->ok_failed_action;
    
    $edt->insert_record('EDT_TEST', { string_req => 'after error' });
    $T->ok_failed_action('aborted');
    
    is( $edt->transaction, 'active', "transaction is active" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    is( $edt->errors, 1, "one error was generated" );
    
    $T->ok_found_record('EDT_TEST', "string_req='insert okay'",
			"first insertion is reflected in the database");

    ok( ! $edt->commit, "commit returned false" );
    
    $T->ok_no_record('EDT_TEST', "string_req='insert okay'",
		     "first insertion has been rolled back");
    
    ok( $edt->has_finished, "transaction has finished" );
    is( $edt->transaction, 'failed', "transaction failed" );
    ok( $edt->has_failed, "transaction has failed" );
    ok( ! $edt->has_committed, "transaction did not commit" ); 
    is( $edt->{save_init_count}, 1, "initialize_transaction ran once" );
    ok( ! $edt->{save_final_count}, "finalize_transaction did not run" );
    is( $edt->{save_cleanup_count}, 1, "cleanup_transaction ran once" );
    is( $edt->errors, 1, "still 1 error condition" );
    is( $edt->action_count, 3, "3 actions submitted" );
    is( $edt->exec_count, 1, "1 action succeeded although transaction failed" );
    is( $edt->fail_count, 1, "1 action failed" );
    is( $edt->skip_count, 1, "1 action skipped" );
    
    # Now we try again, but add the extra error condition with the second action.
    
    $edt = $T->new_edt($perm_a);
    $edt->start_execution;
    
    $edt->insert_record('EDT_TEST', { string_req => 'insert okay' });
    $T->ok_action;
    
    $edt->insert_record('EDT_TEST', { string_req => 'after error' });
    $T->ok_action;
    
    is( $edt->errors, 1, "error condition was added after insert" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    
    $T->ok_found_record('EDT_TEST', "string_req='after error'", "found inserted record");
    
    $edt->insert_record('EDT_TEST', { string_val => 'string_req is empty' });
    $T->ok_failed_action('failed');
    
    $edt->insert_record('EDT_TEST', { string_req => 'should not succeed' });
    $T->ok_failed_action('aborted');
    
    is( $edt->is_active, "transaction is active" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    is( $edt->errors, 1, "1 error was generated" );
    
    ok( ! $edt->commit, "commit failed" );
    
    ok( $edt->has_failed, "transaction has failed" );
    ok( ! $edt->has_committed, "transaction has not committed" );
    
    is( $edt->errors, 2, "2 error conditions" );
    is( $edt->action_count, 4, "4 actions submitted" );
    is( $edt->exec_count, 2, "2 actions succeeded although transaction failed" );
    is( $edt->fail_count, 1, "1 action failed" );
    is( $edt->skip_count, 1, "1 action skipped" );
    is( $edt->{save_cleanup_count}, 1, "cleanup_transaction executed once" );
    ok( ! $edt->{save_final_count}, "finalize_transaction did not execute" );
    
    $T->ok_no_record('EDT_TEST', "string_req='after error'", "insert was rolled back");
    
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
    
    ok( $edt->has_failed, "transaction has still failed" );
    ok( ! $edt->has_committed, "transaction was still not committed" );
};


# Now test what happens when we explicitly roll back a transaction.

subtest 'rollback' => sub {

    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    
    # Then create a transaction, and add some records.
    
    $edt = $T->new_edt($perm_a);
    
    $edt->insert_record('EDT_TEST', { string_req => 'test a1' });
    $edt->insert_record('EDT_TEST', { string_req => 'test a2' });

    # Check the status, and then roll it back.

    ok( ! $edt->is_active, "transaction is not yet active" );
    ok( $edt->can_proceed, "transaction can proceed" );
    
    $result = $edt->rollback;
    
    ok( $result, "rollback returned true although the transaction was not yet active" );
    ok( ! $edt->is_active, "transaction is still not active" );
    ok( ! $edt->can_proceed, "transaction can not proceed" );
    ok( $edt->has_finished, "transaction has finished" );
    is( $edt->transaction, 'aborted', "transaction status is 'aborted'" );
    ok( ! $edt->errors, "no errors on this transaction" );
    
    # Check that the transaction really was rolled back.
    
    $T->ok_no_record('EDT_TEST', "string_req in ('test a1', 'test a2')");
    
    # Check that calling rollback a second time is okay, and that commit is
    # okay too. Both should return false.
    
    eval {
	ok( $edt->rollback, "rollback returned true a second time" );
	ok( ! $edt->commit, "commit returned false after rollback" );
    };
    
    ok( !$@, "no error message on commit or rollback" ) || diag("message was: $@");
    is( $edt->transaction, 'aborted', "transaction status is unchanged" );
    
    # Now try the same thing under IMMEDIATE_MODE.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'test b1' });
    $edt->insert_record('EDT_TEST', { string_req => 'test b2' });

    ok( $edt->is_active, "transaction is active" );
    ok( $edt->can_proceed, "transaction can proceed" );

    # Check that the records are in the table.

    $T->ok_found_record('EDT_TEST', "string_req in ('test b1', 'test b2')");
    
    # Then roll back the transaction.
    
    $result = $edt->rollback;
    
    ok( $result, "rollback returned true because an active transaction was rolled back" );
    ok( ! $edt->is_active, "transaction is no longer active" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    ok( $edt->has_finished, "transaction has finished" );
    is( $edt->transaction, 'aborted', "transaction status is 'aborted'" );
    ok( ! $edt->errors, "no errors on this transaction" );

    # Check that the transaction really was rolled back.
    
    $T->ok_no_record('EDT_TEST', "string_req in ('test b1', 'test b2')");
    
    # Now try the same thing after an error condition has been added.

    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'test c1' });
    $edt->insert_record('EDT_TEST', { string_req => 'test c2' });
    $edt->add_condition(undef, 'E_EXECUTE', 'test error');
    
    ok( $edt->is_active, "transaction is active" );
    ok( ! $edt->can_proceed, "transaction can not proceed" );
    
    # Check that the records are in the table.

    $T->ok_found_record('EDT_TEST', "string_req in ('test c1', 'test c2')");
    
    # Then roll back the transaction.
    
    $result = $edt->rollback;
    
    ok( $result, "rollback returned true because an active transaction was rolled back" );
    ok( ! $edt->is_active, "transaction is no longer active" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    ok( $edt->has_finished, "transaction has finished" );
    is( $edt->transaction, 'aborted', "transaction status is 'aborted'" );
    is( $edt->errors, 1, "one error on this transaction" );
    
    # Check that the transaction really was rolled back.
    
    $T->ok_no_record('EDT_TEST', "string_req in ('test c1', 'test c2')");
    
    # Check that calling rollback a second time is okay, and that commit is
    # okay too. Both should return false.
    
    eval {
	ok( $edt->rollback, "rollback returned true a second time" );
	ok( ! $edt->commit, "commit returned false after rollback" );
    };
    
    ok( !$@, "no error message on commit or rollback" ) || diag("message was: $@");
    is( $edt->transaction, 'aborted', "transaction status is unchanged" );
};


# Now check that 'abort_action' works properly.

subtest 'abort_action' => sub {

    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    
    # Then create a transaction, and add some records.
    
    $edt = $T->new_edt($perm_a);
    
    $result = $edt->abort_action;

    ok( ! $result, "early call to 'abort_action' failed" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'test a1' });
    $edt->insert_record('EDT_TEST', { string_req => 'test a2' });
    
    ok( $edt->can_proceed, "transaction can proceed" );
    
    # Now try adding an error, then abandoning the record.
    
    $edt->insert_record('EDT_TEST', { string_req => 'abandon a1' });
    $edt->add_condition('E_TEST');
    $edt->add_condition('W_TEST');
    
    ok( ! $edt->can_proceed, "transaction cannot proceed because of error" );
    
    $result = $edt->abort_action;
    
    ok( $result, "record was abandoned" );
    ok( $edt->can_proceed, "transaction can now proceed" );
    
    $result = $edt->commit;
    
    ok( $result, "transaction succeeded" ) || $T->diag_errors;
    
    $T->ok_found_record('EDT_TEST', "string_req='test a1'");
    $T->ok_no_record('EDT_TEST', "string_req='abandon a1'");
    is( $edt->exec_count, 2, "two actions succeeded" );
    is( $edt->fail_count, 0, "no actions failed" );
    $T->ok_no_warnings("warning from abandoned record was removed");
    is( $edt->warnings, 0, "warning count was decremented" );
    
    # Now try the same, but call 'abort_action' from 'before_action'.

    $edt = $T->new_edt($perm_a);

    $edt->insert_record('EDT_TEST', { string_req => 'test b1' });
    $edt->insert_record('EDT_TEST', { string_req => 'before abandon' });

    $result = $edt->commit;
    
    ok( $result, "transaction succeeded" ) || $T->diag_errors;
    
    $T->ok_found_record('EDT_TEST', "string_req='test b1'");
    $T->ok_no_record('EDT_TEST', "string_req='before abandon'");
    is( $edt->exec_count, 1, "one action succeeded" );
    is( $edt->fail_count, 1, "one action failed" );

    # Finally, try this under IMMEDIATE_MODE.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'test c1' });
    $edt->insert_record('EDT_TEST', { string_req => 'test c2' });

    $result = $edt->abort_action;

    ok( ! $result, "abort_action failed because action was already executed" );

    $result = $edt->commit;
    
    ok( $result, "transaction succeeded" ) || $T->diag_errors;
    $T->ok_found_record('EDT_TEST', "string_req='test c1'");
    $T->ok_found_record('EDT_TEST', "string_req='test c2'");

    # And once again to test that errors added to an already executed record will not be removed
    # by 'abort_action'.

    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'test d1' });
    $edt->insert_record('EDT_TEST', { string_req => 'test d2' });
    $edt->add_condition('E_TEST');
    
    $result = $edt->abort_action;
    
    ok( ! $result, "abort_action failed because action was already executed" );

    $result = $edt->commit;
    
    ok( ! $result, "transaction failed" );
    $T->ok_no_record('EDT_TEST', "string_req='test d1'");
    $T->ok_no_record('EDT_TEST', "string_req='test d2'");
};


# Check that a single transaction can affect more than one table.

subtest 'multiple tables' => sub {
    
    pass('placeholder');

    # This is actually tested in edt-30-insert.t.
};
