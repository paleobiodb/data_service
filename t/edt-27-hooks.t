#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
#
# edt-27-hooks.t : Test the methods that subclasses override to take part in a transaction:
#                  initialize_transaction, finalize_transaction, cleanup_transaction,
#                  initialize_action, before_action, after_action, and cleanup_action.
#                  The overrides in ETBasicTest.pm are triggered by transaction attributes
#                  and by special values of 'string_req'.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 10;

use ETBasicTest;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_no_conditions
		  ok_has_condition ok_has_one_condition ok_no_errors
		  clear_table ok_found_record ok_no_record);


# Establish an EditTester instance.

my $T = EditTester->new('ETBasicTest');


# Check that the necessary methods are defined.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'initialize_transaction', 'finalize_transaction', 'cleanup_transaction',
	   'initialize_action', 'before_action', 'after_action', 'cleanup_action')
	
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check the transaction hooks for a transaction that succeeds.

subtest 'transaction hooks on success' => sub {
    
    clear_table('EDT_TEST');
    
    my $edt = ok_new_edt( table => 'EDT_TEST' );
    
    $edt->insert_record({ string_req => 'success' });
    
    ok_commit;
    
    is( $edt->{save_init_count}, 1, "initialize_transaction was called once" );
    is( $edt->{save_final_count}, 1, "finalize_transaction was called once" );
    is( $edt->{save_cleanup_count}, 0, "cleanup_transaction was not called" );
    
    is( $edt->{save_init_table}, 'EDT_TEST', "initialize_transaction was passed the default table" );
    is( $edt->{save_final_table}, 'EDT_TEST', "finalize_transaction was passed the default table" );
};


# Check errors, warnings, and exceptions from initialize_transaction.

subtest 'initialize_transaction conditions' => sub {
    
    clear_table('EDT_TEST');
    
    my $edt = ok_new_edt;
    
    $edt->set_attr('initialize warning' => 1);
    $edt->insert_record('EDT_TEST', { string_req => 'init warning' });
    
    ok_commit("transaction with a warning from initialize_transaction commits");
    
    ok_has_condition('all', 'W_TEST', "warning from initialize_transaction is reported");
    
    ok_found_record('EDT_TEST', "string_req = 'init warning'");
    
    foreach my $kind ( 'error', 'exception' )
    {
	$edt = ok_new_edt;
	
	$edt->set_attr("initialize $kind" => 1);
	$edt->insert_record('EDT_TEST', { string_req => "init $kind" });
	
	ok_failed_commit("transaction with an $kind from initialize_transaction fails");
	
	ok_has_condition('all', $kind eq 'error' ? 'E_TEST' : 'E_EXECUTE',
			 "$kind from initialize_transaction is reported");
	
	ok_no_record('EDT_TEST', "string_req = 'init $kind'", "record was not stored");
	
	is( $edt->{save_final_count}, 0, "finalize_transaction was not called" );
	
	ok( $edt->{save_cleanup_count}, "cleanup_transaction was called" );
	
	is( $edt->{save_cleanup_count}, 1, "cleanup_transaction was called only once" );
    }
};


# Check errors, warnings, and exceptions from finalize_transaction.

subtest 'finalize_transaction conditions' => sub {
    
    clear_table('EDT_TEST');
    
    my $edt = ok_new_edt;
    
    $edt->set_attr('finalize warning' => 1);
    $edt->insert_record('EDT_TEST', { string_req => 'final warning' });
    
    ok_commit("transaction with a warning from finalize_transaction commits");
    
    ok_has_condition('all', 'W_TEST', "warning from finalize_transaction is reported");
    
    ok_found_record('EDT_TEST', "string_req = 'final warning'");
    
    foreach my $kind ( 'error', 'exception' )
    {
	$edt = ok_new_edt;
	
	$edt->set_attr("finalize $kind" => 1);
	$edt->insert_record('EDT_TEST', { string_req => "final $kind" });
	
	ok_failed_commit("transaction with an $kind from finalize_transaction fails");
	
	ok_has_condition('all', $kind eq 'error' ? 'E_TEST' : 'E_EXECUTE',
			 "$kind from finalize_transaction is reported");
	
	ok_no_record('EDT_TEST', "string_req = 'final $kind'", "executed action was rolled back");
	
	is( $edt->{save_cleanup_count}, 1, "cleanup_transaction was called once" );
    }
    
    $edt = ok_new_edt;
    
    $edt->set_attr('finalize add' => 'added by finalize');
    $edt->insert_record('EDT_TEST', { string_req => 'final add' });
    
    ok_commit;
    
    ok_found_record('EDT_TEST', "string_req = 'added by finalize'",
		    "change made by finalize_transaction was committed");
};


# Check cleanup_transaction, which is called when a transaction fails.

subtest 'cleanup_transaction' => sub {
    
    clear_table('EDT_TEST');
    
    # A transaction that fails during execution is cleaned up.
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'before error' });
    
    ok_failed_commit;
    
    is( $edt->{save_init_count}, 1, "initialize_transaction was called" );
    is( $edt->{save_cleanup_count}, 1, "cleanup_transaction was called once" );
    is( $edt->{save_final_count}, 0, "finalize_transaction was not called" );
    
    # A transaction that fails before execution starts was never initialized, so it is
    # not cleaned up either.
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'cleanup', signed_val => 'not a number' });
    
    ok_failed_commit;
    
    is( $edt->{save_init_count}, 0, "initialize_transaction was not called" );
    is( $edt->{save_cleanup_count}, 0, "cleanup_transaction was not called" );
    
    $edt = ok_new_edt;
    
    $edt->set_attr('cleanup exception' => 1);
    $edt->insert_record('EDT_TEST', { string_req => 'before error' });
    
    ok_failed_commit;
    
    ok_has_condition('all', 'E_EXECUTE', qr/cleanup_transaction/,
		     "exception from cleanup_transaction is reported");
};


# Check that initialize_action is called for each new action, before authorization
# and validation, and that a condition it adds blocks the action.

subtest 'initialize_action' => sub {
    
    clear_table('EDT_TEST');
    
    my @calls;
    
    no warnings 'redefine';
    
    local *ETBasicTest::initialize_action = sub {
	
	my ($edt, $action, $operation, $table_specifier) = @_;
	
	push @calls, [ $operation, $table_specifier ];
	
	$edt->add_condition('E_TEST', 'initialize_action')
	    if ref $action->record eq 'HASH' && $action->record->{string_req} eq 'init action error';
    };
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'init action' });
    $edt->update_record('EDT_TEST', { test_no => 1, string_req => 'init action' });
    
    is_deeply( \@calls, [ [ 'insert', 'EDT_TEST' ], [ 'update', 'EDT_TEST' ] ],
	       "initialize_action was called for each action with its operation and table" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'init action error' });
    
    ok_has_one_condition('&_', 'E_TEST', "condition added by initialize_action is reported");
    
    is( $edt->action_status('&_'), 'failed', "action with an initialize_action error has failed" );
};


# Check the arguments passed to before_action and after_action.

subtest 'before_action and after_action arguments' => sub {
    
    clear_table('EDT_TEST');
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'hook args' });
    
    ok_commit;
    
    my ($new_key) = $edt->inserted_keys('EDT_TEST');
    
    is( $edt->{save_before_count}, 1, "before_action was called once" );
    is( $edt->{save_after_count}, 1, "after_action was called once" );
    
    is( $edt->{save_before_operation}, 'insert', "before_action was passed the operation" );
    is( $edt->{save_before_table}, 'EDT_TEST', "before_action was passed the table" );
    is( $edt->{save_after_operation}, 'insert', "after_action was passed the operation" );
    is( $edt->{save_after_result}, $new_key, "after_action for insert was passed the new key" );
    
    $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { test_no => $new_key, string_req => 'hook args 2' });
    $edt->delete_record('EDT_TEST', $new_key);
    
    ok_commit;
    
    is( $edt->{save_before_count}, 2, "before_action was called for each action" );
    is( $edt->{save_after_operation}, 'delete', "after_action was passed the last operation" );
    is( $edt->{save_after_result}, 1, "after_action for delete was passed the statement result" );
};


# Check errors, warnings, and exceptions from before_action and after_action.

subtest 'before_action and after_action conditions' => sub {
    
    clear_table('EDT_TEST');
    
    foreach my $hook ( 'before', 'after' )
    {
	my $edt = ok_new_edt;
	
	$edt->insert_record('EDT_TEST', { string_req => "$hook warning" });
	
	ok_commit("warning from ${hook}_action does not block the transaction");
	
	ok_has_condition('all', 'W_TEST', "warning from ${hook}_action is reported");
	
	ok_found_record('EDT_TEST', "string_req = '$hook warning'");
	
	foreach my $kind ( 'error', 'exception' )
	{
	    $edt = ok_new_edt;
	    
	    $edt->insert_record('EDT_TEST', { string_req => "$hook $kind" });
	    $edt->insert_record('EDT_TEST', { string_req => "after $hook $kind" });
	    
	    ok_failed_commit("$kind from ${hook}_action causes the transaction to fail");
	    
	    ok_has_condition('all', $kind eq 'error' ? 'E_TEST' : 'E_EXECUTE',
			     "$kind from ${hook}_action is reported");
	    
	    is( $edt->action_status('&#1'), $hook eq 'before' ? 'failed' : $kind eq 'error' ?
		'unexecuted' : 'failed', "action status after a ${hook}_action $kind" );
	    
	    is( $edt->action_status('&#2'), 'unexecuted', "later action was not executed" );
	    
	    ok_no_record('EDT_TEST', "string_req = '$hook $kind'", "record was not stored");
	}
    }
    
    my $edt = ok_new_edt('PROCEED');
    
    $edt->insert_record('EDT_TEST', { string_req => 'before error' });
    $edt->insert_record('EDT_TEST', { string_req => 'proceed after before error' });
    
    ok_commit("with PROCEED, an error from before_action does not block the transaction");
    
    ok_found_record('EDT_TEST', "string_req = 'proceed after before error'",
		    "later action was executed");
    
    ok_no_record('EDT_TEST', "string_req = 'before error'", "failed action was not executed");
};


# Check that abort_action called from before_action keeps the action from being
# executed without blocking the rest of the transaction.

subtest 'abort_action from before_action' => sub {
    
    clear_table('EDT_TEST');
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'before abandon' });
    $edt->insert_record('EDT_TEST', { string_req => 'after abandon' });
    
    ok_commit("transaction with an aborted action commits");
    
    ok_no_record('EDT_TEST', "string_req = 'before abandon'", "aborted action was not executed");
    ok_found_record('EDT_TEST', "string_req = 'after abandon'", "later action was executed");
    
    is( $edt->action_status('&#1'), 'skipped', "aborted action status is 'skipped'" );
    is( $edt->fail_count, 0, "aborted action is not counted as failed" );
    is( $edt->skip_count, 1, "aborted action is counted as skipped" );
};


# Check that cleanup_action is called for an action that fails to execute.

subtest 'cleanup_action' => sub {
    
    clear_table('EDT_TEST');
    
    my $edt = ok_new_edt('PROCEED');
    
    $edt->insert_record('EDT_TEST', { string_req => 'before error' });
    
    ok_commit;
    
    is( $edt->{save_cleanup_action_count}, 1, "cleanup_action was called for the failed action" );
    is( $edt->{save_cleanup_action_operation}, 'insert', "cleanup_action was passed the operation" );
};
