#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# transaction.t :
# 
#         Test the transaction process from start to finish. Check that the
#         transaction status and action status methods return the proper values.
#         Also check the allowances NOT_FOUND, PROCEED, and IMMEDIATE_EXECUTION.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 9;

use ETBasicTest;
use EditTester qw(ok_eval ok_exception ok_new_edt ok_can_proceed ok_cannot_proceed
		  clear_captured_output ok_captured_output capture_mode
		  ok_has_condition ok_no_conditions ok_has_one_condition ok_condition_count
		  clear_table ok_commit ok_failed_commit ok_rollback ok_action
		  ok_found_record ok_no_record ok_record_count);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest', 'EDT_TEST');

my @keyvals;


# Check that we have the required methods.

subtest 'required methods' => sub {
    
    can_ok('EditTransaction', 'status', 'has_started', 'has_finished', 'is_active',
	   'is_executing', 'can_accept', 'can_proceed', 'action_status',
	   'action_count', 'exec_count', 'skip_count', 'fail_count' )
	
	|| BAIL_OUT "EditTransaction is missing some required methods";
};


# Run a simple transaction, and check the status method responses.

subtest 'basic transaction' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'a1' });

    is( $edt->{save_init_count}, 0, "initialize_transaction has not executed" );
    is( $edt->{save_final_count}, 0, "finalize_transaction has not executed" );    
    
    # Check the status responses while the transaction is still being initialized.
    
    is( $edt->action_status, 'pending', "insert status is 'pending'" );
    is( $edt->status, 'init', "transaction status is 'init'" );
    is( $edt->has_started, '', "transaction has not started" );
    is( $edt->has_finished, '', "transaction has not finished" );
    is( $edt->is_active, '', "transaction is not active" );
    is( $edt->is_executing, '', "transaction is not executing" );
    is( $edt->can_accept, 1, "transaction can accept new actions" );
    is( $edt->can_proceed, 1, "transaction can proceed" );
    
    ok_commit;
    
    is( $edt->action_status, 'executed', "insert status is now 'executed'" );
    is( $edt->status, 'committed', "transaction status is now 'committed'" );
    is( $edt->has_started, 1, "transaction has started" );
    is( $edt->has_finished, 1, "transaction has finished" );
    is( $edt->has_committed, 1, "transaction has committed" );
    is( $edt->has_failed, '', "transaction has not failed" );
    is( $edt->is_active, '', "transaction is not active" );
    is( $edt->is_executing, '', "transaction is not executing" );
    is( $edt->can_accept, '', "transaction cannot accept new actions" );
    is( $edt->can_proceed, '', "transaction cannot proceed" );
    is( $edt->{commit_count}, 1, "commit was executed" );
    
    # Now check the instrumentation results added by methods in ETBasicTest.pm
    
    is( $edt->{save_init_count}, 1, "initialize_transaction was executed once" );
    is( $edt->{save_init_table}, 'EDT_TEST', "default table was passed to initialize" );
    is( $edt->{save_init_status}, 'active', "status was active for initialize" );
    is( $edt->{save_init_action}, '', "no current action for initialize" );
    is( $edt->{save_final_count}, 1, "finalize_transaction was executed once" );
    is( $edt->{save_final_table}, 'EDT_TEST', "default table was passed to finalize" );
    is( $edt->{save_final_status}, 'active', "status was active for finalize" );
    is( $edt->{save_final_action}, '', "no current action for finalize" );
    is( $edt->{save_cleanup_count}, 0, "cleanup_transaction was not called" );
};


# Now check that 'start_transaction' and 'start_execution' work as expected.

subtest 'start_transaction and start_execution' => sub {
    
    # Clear the table to give us a known initial state.
    
    clear_table;
    
    # Create a new EditTransaction.
    
    my $edt = ok_new_edt;
    
    is( $edt->status, 'init', "transaction status is 'init'" );
    is( $edt->has_started, '', "transaction has not started" );
    is( $edt->is_active, '', "transaction is not active" );
    is( $edt->is_executing, '', "execution has not started" );
    
    is( $edt->{save_init_count}, 0, "initialize_transaction has not been called" );
    
    my $a1 = $edt->insert_record('EDT_TEST', { string_req => 'tt1' });
    
    ok_action('pending', "action status is 'pending'" );
    
    ok( $edt->start_transaction, "start_transaction returned true" );
    
    is( $edt->has_started, 1, "transaction has started" );
    is( $edt->is_active, 1, "transaction is now active" );
    is( $edt->is_executing, '', "execution has not started" );
    
    is( $edt->{save_init_count}, 1, "initialize_transaction has been called" );
    is( $edt->{save_init_action}, '', "no current action for initialize" );
    is( $edt->current_action, $a1, "current action has been preserved" );
    
    ok_action('pending', "action status is still 'pending'" );
    
    my $a2 = $edt->insert_record('EDT_TEST', { string_req => 'tt2' });
    
    ok_action('pending', "second action status is 'pending'" );
    ok_record_count( 0, 'EDT_TEST' );
    
    ok( $edt->start_execution, "start_execution returned true" );
    
    is( $edt->is_executing, 1, "execution has started" );
    
    ok_action($a1, 'executed', "action 1 has now executed" );
    ok_action($a2, 'executed', "action 2 has now executed" );
    ok_record_count( 2, 'EDT_TEST' );
    
    is( $edt->start_transaction, 1, "start_transaction returns true when repeated" );
    is( $edt->start_execution, 1, "start_execution returns true when repeated" );
    
    my $a3 = $edt->insert_record('EDT_TEST', { string_req => 'tt3' });
    
    ok_action($a2, 'executed', "action 3 has now executed" );
    
    is( $edt->{save_final_count}, 0, "finalize_transaction has not been called" );
    is( $edt->current_action, $a3, "current action" );
    
    ok_commit;
    
    is( $edt->{save_final_count}, 1, "finalize_transaction has been called" );
    is( $edt->current_action, '', "no current action any more" );
    ok_record_count( 3, 'EDT_TEST' );
    
    is( $edt->has_started, 1, "has_started" );
    is( $edt->is_active, '', "transaction no longer active" );
    is( $edt->is_executing, '', "transaction no longer executing" );
    
    ok( ! $edt->start_execution, "start_execution returns false after commit" );
    ok( ! $edt->start_transaction, "start_transaction returns false after commit" );
    
    # Now create a second transaction, and check that start_execution also
    # starts the transaction.
    
    $edt = ok_new_edt;
    
    $edt->start_execution;
    
    ok( $edt->is_active, "transaction is now active" );
    ok( $edt->is_executing, "transaction is now executing" );
    is( $edt->{save_init_count}, 1, "initialize_transaction has run" );
};


# Check the result of calling start_transaction and start_execution in
# situations where they do not succceed.

subtest 'start_transaction not valid' => sub {
    
    # Try adding an error condition before start_transaction.
    
    my $edt = ok_new_edt;
    
    $edt->add_condition('E_BAD_TABLE', 'foobar');
    
    is( $edt->can_proceed, '', "transaction cannot proceed" );
    
    is( $edt->start_transaction, '', "start_transaction fails" );
    is( $edt->start_execution, '', "start_execution fails" );
    
    is( $edt->has_started, '', "has_started returns false" );
    is( $edt->{save_init_count}, 0, "initialize_transaction has not run" );
    
    is( $edt->commit, '', "commit returns false" );
    is( $edt->status, 'failed', "status is 'failed'" );
    is( $edt->rollback, 1, "rollback returns true" );
    is( $edt->{rollback_count}, 0, "no rollback occurred" );
    
    # Then add an error in initialize_transaction.
    
    $edt = ok_new_edt;
    
    $edt->set_attr("initialize error", 1);
    
    $edt->insert_record('EDT_TEST', { string_req => 'tt4' });
    
    is( $edt->start_transaction, '', "start_transaction fails" );
    is( $edt->{rollback_count}, 1, "transaction started and was rolled back" );
    
    ok_has_condition( 'main', 'E_TEST', "found condition" );
    
    # Now throw an exception in initialize_transaction.
    
    $edt = ok_new_edt;
    
    $edt->set_attr("initialize exception", 1);
    
    $edt->insert_record('EDT_TEST', { string_req => 'tt4' });
    
    clear_captured_output;
    
    capture_mode(1);
    
    is( $edt->start_transaction, '', "start_transaction fails 2" );
    
    capture_mode(0);
    
    is( $edt->{rollback_count}, 1, "transaction started and was rolled back 2" );
    is( $edt->is_active, '', "transaction is not active" );
    is( $edt->start_execution, '', "start_execution fails" );
    is( $edt->has_failed, 1, "transaction has failed" );
    
    ok_has_condition( 'main', 'E_EXECUTE', "found condition 2" );
    
    ok_captured_output( qr/initialize exception/, "exception was listed to error stream" );
};


# Check the semantics of 'execute' and 'pause_execution'.

subtest 'execution control' => sub {
    
    clear_table;
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'ec1' });
    
    $edt->insert_record('EDT_TEST', { string_req => 'ec2' });
    
    ok_record_count(0, 'EDT_TEST');
    
    ok( ! $edt->is_executing, "transaction is not yet executing" );
    is( $edt->{save_init_count}, 0, "transaction has not yet started" );
    
    $edt->execute;
    
    ok_record_count(2, 'EDT_TEST');
    
    is( $edt->{save_init_count}, 1, "transaction has now started" );
    ok( ! $edt->is_executing, "execution mode is still off" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'ec3' });
    
    ok_record_count(2, 'EDT_TEST' );
    
    $edt->execute;
    
    ok_record_count(3, 'EDT_TEST' );
    
    $edt->start_execution;
    
    ok( $edt->is_executing, "execution has started" );
    
    ok_record_count(3, 'EDT_TEST' );
    
    $edt->insert_record('EDT_TEST', { string_req => 'ec4' });
    
    ok_record_count(4, 'EDT_TEST');
    
    $edt->pause_execution;
    
    ok( ! $edt->is_executing, "execution is paused" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'ec5' });
    
    ok_record_count(4, 'EDT_TEST');
    
    $edt->start_execution;
    
    ok( $edt->is_executing, "execution is unpaused" );
    
    ok_record_count(5, 'EDT_TEST');
    
    ok_rollback;
    
    is( $edt->{save_init_count}, 1, "initialize_transaction was run" );
    is( $edt->{save_final_count}, 0, "finalize_transaction was not run" );
    is( $edt->{save_cleanup_count}, 1, "cleanup_transaction was run" );
};


# Do some more checks on initialize_transaction, and add finalize_transaction
# and cleanup_transaction as well.

subtest 'initialize, finalize, and cleanup' => sub {
    
    pass('placeholder');
};


# Check the NOT_FOUND and PROCEED allowances. This requires first clearing the
# table and adding some records.

subtest 'set up table' => sub {
    
    # Clear the table and add some records.
    
    clear_table;
    
    my $edt = ok_new_edt;
    
    $edt->insert_record({ string_req => 'aaa1' });
    $edt->insert_record({ string_req => 'aaa2' });
    $edt->insert_record({ string_req => 'aaa3' });
    $edt->insert_record({ string_req => 'aaa4' });
    
    ok_commit;
    
    @keyvals = $edt->inserted_keys;
};


subtest 'NOT_FOUND and PROCEED' => sub {
    
    # Attempt to update some records that exist and some that do not.
    
    my $edt = ok_new_edt;
    
    ok_can_proceed;
    
    $edt->update_record({ _primary => $keyvals[0], string_val => 'updated' });
    $edt->update_record({ _primary => 455, string_val => 'not updated' });
    $edt->update_record({ _primary => $keyvals[1], string_val => 'updated' });
    
    ok_cannot_proceed;
    
    is( $edt->action_status, 'unexecuted', "update status is 'unexecuted'" );
    is( $edt->status, 'init', "transaction status is 'init'" );
    
    # The commit should fail with two 'E_NOT_FOUND' conditions.
    
    ok_failed_commit;
    
    is( $edt->action_status, 'unexecuted', "update status is now 'unexecuted'" );
    is( $edt->status, 'failed', "transaction status is now 'failed'" );
    is( $edt->has_started, 1, "transaction has started" );
    is( $edt->is_active, '', "transaction is not active" );
    is( $edt->is_executing, '', "transaction is not yet executing" );
    is( $edt->has_finished, 1, "transaction has finished" );
    is( $edt->has_failed, 1, "transaction has failed" );
    is( $edt->has_committed, '', "transaction has not committed" );
    
    is( $edt->action_count, 3, "total actions: 3" );
    is( $edt->exec_count, 0, "executed actions: 0" );
    is( $edt->fail_count, 1, "failed actions: 1" );
    is( $edt->skip_count, 2, "skipped actions: 2" );
    
    ok_condition_count( 1, 'all', 'E_NOT_FOUND', "generated one E_NOT_FOUND" );
    
    ok_no_record('default', "string_val='updated'");
    
    # Now repeat with the NOT_FOUND allowance. This time the commit should go
    # through.
    
    $edt = ok_new_edt('NOT_FOUND');
    
    ok_can_proceed;
    
    $edt->update_record({ _primary => $keyvals[0], string_val => 'updated' });
    $edt->update_record({ _primary => 455, string_val => 'not updated' });
    $edt->update_record({ _primary => $keyvals[1], string_val => 'updated' });
    
    ok_can_proceed;
    
    # Check that the updates have not been done yet, and then commit.
    
    ok_no_record('default', "string_val='updated'");
    
    is( $edt->exec_count, 0, "executed actions before commit: 0" );
    is( $edt->fail_count, 1, "failed actions before commit: 1" );
    is( $edt->action_status, 'pending', "last action status is 'pending'" );
    is( $edt->status, 'init', "transaction status is 'init'" );
    
    ok_commit;
    
    is( $edt->action_status, 'executed', "last action status is now 'executed'" );
    is( $edt->status, 'committed', "transaction status is now 'committed'" );
    is( $edt->has_failed, '', "transaction has not failed" );
    is( $edt->has_committed, 1, "transaction has committed" );
    
    ok_cannot_proceed( "transaction cannot proceed because it has finished" );
    
    is( $edt->action_count, 3, "total actions: 3" );
    is( $edt->exec_count, 2, "executed actions: 2" );
    is( $edt->fail_count, 1, "failed actions: 1" );
    is( $edt->skip_count, 0, "skipped actions: 0" );
    
    ok_condition_count( 1, 'all', 'F_NOT_FOUND', "demoted E_NOT_FOUND to F_NOT_FOUND" );
    
    ok_record_count( 2, 'default', "string_val='updated'", "two records were changed" );
    ok_no_record( 'default', "string_val='not updated'", "bad update didn't go through" );
    
    # Now check that NOT_FOUND does not protect against other errors. This
    # transaction should fail with an E_FORMAT condition.
    
    $edt = ok_new_edt('NOT_FOUND');
    
    $edt->update_record({ _primary => $keyvals[2], string_val => 'updated' });
    $edt->update_record({ _primary => $keyvals[3], string_val => 'not updated', signed_val => 'abc' });
    
    ok_failed_commit;
    
    is( $edt->action_count, 2, "total actions: 2" );
    is( $edt->exec_count, 0, "executed actions: 0" );
    is( $edt->fail_count, 1, "failed actions: 1" );
    is( $edt->skip_count, 1, "skipped actions: 1" );
    
    ok_condition_count( 1, 'all', 'E_FORMAT', "generated E_FORMAT from 'abc'" );
    
    ok_record_count( 2, 'default', "string_val='updated'", "nothing was changed" );
    ok_no_record( 'default', "string_val='not updated'", "bad update didn't go through" );
    
    # Check that PROCEED allows the same sequence of actions to go through.
    
    $edt = ok_new_edt('PROCEED');
    
    $edt->update_record({ _primary => $keyvals[2], string_val => 'updated' });
    $edt->update_record({ _primary => 456, string_val => 'not updated' });
    $edt->update_record({ _primary => $keyvals[3], string_val => 'not updated', signed_val => 'abc' });
    
    is( $edt->fail_count, 2, "failed actions before commit: 2" );
    
    ok_commit;
    
    is( $edt->action_count, 3, "total actions: 3" );
    is( $edt->exec_count, 1, "executed actions: 1" );
    is( $edt->fail_count, 2, "failed actions: 2" );
    is( $edt->skip_count, 0, "skipped actions: 0" );
    
    ok_condition_count( 1, 'all', 'F_NOT_FOUND', "demoted E_NOT_FOUND to F_NOT_FOUND" );
    ok_condition_count( 0, 'all', 'E_NOT_FOUND', "it really was demoted" );
    ok_condition_count( 1, 'all', 'F_FORMAT', "demoted E_FORMAT to F_FORMAT" );
    
    ok_record_count( 3, 'default', "string_val='updated'", "additional record was changed" );
    ok_no_record( 'default', "string_val='not updated'", "bad update didn't go through" );
};


# Then add IMMEDIATE_EXECUTION into the mix.

subtest 'IMMEDIATE_EXECUTION' => sub {
    
    # First try a transaction with IMMEDIATE_EXECUTION and some errors.
    
    my $edt = ok_new_edt('IMMEDIATE_EXECUTION');
    
    is( $edt->status, 'active', "status is 'active'" );
    is( $edt->is_active, 1, "transaction is active" );
    is( $edt->is_executing, 1, "transaction is executing" );
    is( $edt->{save_init_count}, 1, "initialize_transaction has been called" );
    
    $edt->update_record({ _primary => $keyvals[0], string_val => 'aaa' });
    
    is( $edt->action_status, 'executed', "first action was executed" );
    ok_record_count( 1, 'default', "string_val='aaa'", "first update was executed" );
    ok_can_proceed;
    
    $edt->update_record({ _primary => 457, string_val => 'bbb' });
    
    is( $edt->action_status, 'failed', "second action failed" );
    is( $edt->status, 'active', "transaction is still active" );
    ok_has_condition('E_NOT_FOUND', "bad update was not executed");
    ok_cannot_proceed;
    
    $edt->update_record({ _primary => $keyvals[1], string_val => 'aaa' });
    
    is( $edt->action_status, 'unexecuted', "third action was not executed" );
    ok_record_count( 1, 'default', "string_val='aaa'", "second update was not executed" );
    ok_cannot_proceed;
    
    is( $edt->action_count, 3, "total actions: 3" );
    is( $edt->exec_count, 1, "executed actions: 1" );
    is( $edt->fail_count, 1, "failed actions: 1" );
    is( $edt->skip_count, 1, "skipped actions: 1" );
    
    # The transaction will be rolled back because of errors.
    
    ok_failed_commit;
    
    is( $edt->status, 'failed', "transaction has failed" );
    
    ok_record_count( 0, 'default', "string_val='aaa'", "first update was rolled back" );
    
    # Then try again with IMMEDIATE_EXECUTION and NOT_FOUND.
    
    $edt = ok_new_edt('IMMEDIATE_EXECUTION', 'NOT_FOUND');
    
    is( $edt->transaction, 'active', "transaction is active from the start" );
    
    $edt->update_record({ _primary => $keyvals[0], string_val => 'aaa' });
    
    is( $edt->action_status, 'executed', "first action was executed" );
    ok_record_count( 1, 'default', "string_val='aaa'", "first update was executed" );
    ok_can_proceed;
    
    $edt->update_record({ _primary => 457, string_val => 'bbb' });
    
    is( $edt->action_status, 'failed', "second action failed" );
    is( $edt->status, 'active', "transaction is still active" );
    ok_has_condition('F_NOT_FOUND', "bad update was not executed");
    ok_no_conditions('E_NOT_FOUND', "E_NOT_FOUND was demoted");
    ok_can_proceed;
    
    $edt->update_record({ _primary => $keyvals[1], string_val => 'aaa' });
    
    is( $edt->action_status, 'executed', "third action was executed" );
    ok_record_count( 2, 'default', "string_val='aaa'", "both updates are reflected in the table" );
    ok_can_proceed;
    
    is( $edt->action_count, 3, "total actions: 3" );
    is( $edt->exec_count, 2, "executed actions: 2" );
    is( $edt->fail_count, 1, "failed actions: 1" );
    is( $edt->skip_count, 0, "skipped actions: 0" );
    
    ok_commit;
    
    ok_record_count( 2, 'default', "string_val='aaa'", "both updates were committed" );
};
