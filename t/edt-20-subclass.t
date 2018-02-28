#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-20-subclass.t : Test that subclassing EditTransaction works properly. Check all of the
# override methods and also additional error and warning templates. Conditions defined in
# subclasses were already checked in edt-01-basic.t.
#



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use TableDefs qw($EDT_TEST);

use EditTest;
use EditTester;


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;



my ($perm_a);


subtest 'setup' => sub {
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');

    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;
};


# Check that exceptions in the 'authorize_action' routine are properly caught, and that we can add
# specific error and warning conditions. Also check that 'authorize_action' is passed the proper
# arguments.

subtest 'authorize' => sub {

    my $edt = $T->new_edt($perm_a) || return;
    
    $edt->insert_record($EDT_TEST, { string_req => 'authorize exception', signed_val => '1' });
    
    $T->ok_has_error($edt, qr/E_EXECUTE.*validation/, "error from exception");
    
    $edt->insert_record($EDT_TEST, { string_req => 'authorize error', signed_val => '2' });
    
    $T->ok_has_error($edt, qr/E_TEST.*xyzzy/, "specific error");
    
    $edt->insert_record($EDT_TEST, { string_req => 'authorize warning', signed_val => 'abc' });
    
    $T->ok_has_warning($edt, qr/W_TEST.*xyzzy/, "specific warning");
    $T->ok_has_error($edt, qr/E_PARAM.*abc/, "bad value error");
    
    $edt->delete_record($EDT_TEST, { string_req => 'authorize save', test_no => '423' });
    $T->ok_has_error($edt, qr/E_NOT_FOUND/, "not found error");
    
    ok( $edt->{save_authorize_action} && $edt->{save_authorize_action}->isa('EditTransaction::Action'),
	"first argument is a valid action" );
    ok( $edt->{save_authorize_operation} && $edt->{save_authorize_operation} eq 'delete',
	"second argument is the operation" );
    ok( $edt->{save_authorize_table} && $edt->{save_authorize_table} eq $EDT_TEST,
	"third argument is the table" );
    ok( $edt->{save_authorize_keyexpr} && $edt->{save_authorize_keyexpr} eq "test_no='423'",
	"fourth argument is the keyexpr" );

    is( $edt->errors, 4, "only 4 errors were generated" );
};


# Check that exceptions in the 'validate_action' routine are properly caught, and that we can add
# specific error and warning conditions. Also check that 'validate_action' is passed the proper
# arguments.

subtest 'validate' => sub {
    
    my $edt = $T->new_edt($perm_a) || return;
    
    $edt->insert_record($EDT_TEST, { string_req => 'validate exception', signed_val => '1' });
    
    $T->ok_has_error($edt, qr/E_EXECUTE.*validation/, "error from exception");
    
    $edt->insert_record($EDT_TEST, { string_req => 'validate error', signed_val => '2' });
    
    $T->ok_has_error($edt, qr/E_TEST.*xyzzy/, "specific error");
    
    $edt->insert_record($EDT_TEST, { string_req => 'validate warning', signed_val => 'abc' });
    
    $T->ok_has_warning($edt, qr/W_TEST.*xyzzy/, "specific warning");
    $T->ok_has_error($edt, qr/E_PARAM/, "found error from base class validate routine");

    $edt->delete_record($EDT_TEST, { string_req => 'validate save', test_no => '999' });

    ok( $edt->{save_validate_action} && $edt->{save_validate_action}->isa('EditTransaction::Action'),
	"first argument is a valid action" );
    ok( $edt->{save_validate_operation} && $edt->{save_validate_operation} eq 'delete',
	"second argument is the operation" );
    ok( $edt->{save_validate_table} && $edt->{save_validate_table} eq $EDT_TEST,
	"third argument is the table" );
    ok( $edt->{save_validate_keyexpr} && $edt->{save_validate_keyexpr} eq "test_no='999'",
	"fourth argument is the keyexpr" );
    ok( $edt->{save_validate_errors}, "action had errors" );
    
    is( $edt->errors, 4, "found four errors" );
};


# Now check 'initialize_transaction' and 'finalize_transaction'. The base class
# methods do nothing, so we don't need to test those.

subtest 'initialize and finalize' => sub {

    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table($EDT_TEST);
    
    # Check that an exception in intialize_transaction is properly turned into
    # E_EXECUTE.
    
    my $edt = $T->new_edt($perm_a, { DEBUG_MODE => 0 }) || return;
    
    $edt->set_attr('initialize exception' => 1);
    $edt->start_execution;

    $T->ok_has_error(qr/E_EXECUTE/, "error from exception");
    is( $edt->errors, 1, "found just one error" );
    
    # Check that initialize_transaction can add an error condition.
    
    $edt = $T->new_edt($perm_a, { DEBUG_MODE => 0 });
    
    $edt->set_attr('initialize error' => 1);
    $edt->start_execution;
    
    $T->ok_has_error(qr/E_TEST.*initialize/, "specific error");

    # Check that this error condition prevents the transaction from
    # proceeding.

    $edt->insert_record($EDT_TEST, { string_req => 'do not add' });
    my $result = $edt->execute;

    ok( ! $result, "execution failed" );
    is( $edt->transaction, 'aborted', "transaction was aborted" );
    $T->ok_no_record($EDT_TEST, "string_req='do not add'");
    
    # Now check that initialize_transaction can actually modify the database,
    # and also that the $table argument is provided.

    $edt = $T->new_edt($perm_a, { DEBUG_MODE => 0 });
    
    my $random_string = 'init test ' . time;
    
    $edt->set_attr('initialize add' => $random_string);
    $edt->start_execution;
    
    $T->ok_no_errors("no errors on initialize add");
    $T->ok_found_record($EDT_TEST, "string_req='$random_string'");
    is($edt->{save_init_table}, $EDT_TEST, "table argument was correct" );
    
    # Check that initialize_transaction is only called once, even if we call
    # both 'start_execution' and 'execute' and also use 'IMMEDIATE_MODE'. Also
    # check that finalize_transaction is able to add warnings.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    is( $edt->{save_init_count}, 1, "called from IMMEDIATE_MODE" );
    
    $edt->set_attr('finalize warning' => 1);
    $edt->start_execution;
    
    is( $edt->{save_init_count}, 1, "not called again from start_execution" );
    
    $edt->insert_record($EDT_TEST, { string_req => 'init test 1' });
    
    $result = $edt->execute;
    
    ok( $result, "transaction succeeded" );
    is( $edt->{save_init_count}, 1, "not called again from execute" );
    is( $edt->{save_final_count}, 1, "finalized on commit" );
    ok( ! $edt->{save_final_rollback}, "finalization second argument was false" );
    $T->ok_has_warning(qr/W_TEST/, "finalization warning" );
    $T->ok_no_errors("no errors on finalization");
    $T->ok_found_record($EDT_TEST, "string_req='init test 1'");
    is( $edt->transaction, 'committed', "transaction was committed" );
    
    # Check that finalize_transaction can abort the transaction by adding an
    # error condition.
    
    $edt = $T->new_edt($perm_a);
    
    $edt->set_attr('finalize error' => 1);
    $edt->insert_record($EDT_TEST, { string_req => 'final test 1' });
    
    $result = $edt->execute;
    
    ok( ! $result, "transaction was aborted" );
    $T->ok_has_error(qr/E_TEST/, "finalization error");
    $T->ok_no_record($EDT_TEST, "string_req='final test 1'");
    is( $edt->{save_final_count}, 1, "finalization completed" );
    ok( ! $edt->{save_final_rollback}, "finalization second argument was false" );
    is( $edt->transaction, 'aborted', "transaction was aborted" );
    
    # Check that finalize_transaction can abort the transaction by throwing an
    # exception.
    
    $edt = $T->new_edt($perm_a);
    $edt->set_attr('finalize exception' => 1);
    $edt->insert_record($EDT_TEST, { string_req => 'final test 2' });
    
    $result = $edt->execute;
    
    ok( ! $result, "finalization error caused transaction to fail" );
    $T->ok_has_error(qr/E_EXECUTE/, "finalization error");
    $T->ok_no_record($EDT_TEST, "string_req='final test 2'");
    is( $edt->transaction, 'aborted', "transaction was aborted" );
    
    # Check that cleanup_transaction gets called instead of finalize_transaction if the transaction
    # has been aborted before its call.
    
    $edt = $T->new_edt($perm_a);
    $edt->start_transaction;
    $edt->insert_record($EDT_TEST, { string_req => 'final test 3' });
    $edt->add_condition(undef, 'E_EXECUTE', 'deliberate error');
    
    $result = $edt->execute;
    
    ok( ! $result, "deliberate error caused transaction to fail" );
    $T->ok_has_error(qr/E_EXECUTE/, "found error");
    $T->ok_no_record($EDT_TEST, "string_req='final test 3'");
    is( $edt->transaction, 'aborted', "transaction was aborted" );
    ok( ! $edt->{save_final_count}, "no finalization" );
    is( $edt->{save_cleanup_count}, 1, "cleanup completed" );
    is( $edt->{save_cleanup_table}, $EDT_TEST, "cleanup was given proper table" );

    # Check that a separate error condition is generated if cleanup_transaction throws an
    # exception as well.

    $edt = $T->new_edt($perm_a);
    $edt->set_attr('cleanup exception' => 1);
    $edt->start_transaction;
    $edt->insert_record($EDT_TEST, { string_req => 'final test 3a' });
    $edt->add_condition(undef, 'E_EXECUTE', 'deliberate error');
    
    $result = $edt->execute;
    
    ok( ! $result, "deliberate error caused transaction to fail" );
    is( $edt->errors, 2, "two errors were generated" );
    $T->ok_has_error(qr/E_EXECUTE.*deliberate error/, "found deliberate error");
    $T->ok_has_error(qr/E_EXECUTE.*cleanup/, "found error from cleanup exception");
    $T->ok_no_record($EDT_TEST, "string_req='final test 3a'");
    ok( ! $edt->{save_cleanup_count}, "cleanup was not completed" );
    
    
    # Check that finalize_transaction can modify the database, and that the first argument is
    # correct.
    
    $edt = $T->new_edt($perm_a);
    $edt->insert_record($EDT_TEST, { string_req => 'main record' });
    $edt->set_attr('finalize add', 'final record');
    $result = $edt->execute;
    
    ok( $result, "transaction succeeded" );
    is( $edt->transaction, 'committed', "transaction committed" );
    $T->ok_found_record($EDT_TEST, "string_req='main record'", "found main record");
    $T->ok_found_record($EDT_TEST, "string_req='final record'", "found final record");
    is( $edt->{save_final_table}, $EDT_TEST, "second argument was correct" );
    
    # Check that neither initialize_transaction nor finalize_transaction gets called if an error
    # occurs before the transaction is actually started.

    $edt = $T->new_edt($perm_a);
    $edt->insert_record($EDT_TEST, { string_req => 'final test 4' });
    $edt->add_condition(undef, 'E_EXECUTE', 'deliberate error');

    $result = $edt->execute;
    
    ok( ! $result, "deliberate error caused transaction to fail" );
    $T->ok_has_error(qr/E_EXECUTE/, "found error");
    $T->ok_no_record($EDT_TEST, "string_req='final test 4'");
    is( $edt->transaction, 'finished', "transaction was never started" );
    ok( ! $edt->{save_init_count}, "initialize_transaction was not called" );
    ok( ! $edt->{save_final_count}, "finalize_transaction was not called" );
};


# Now check the before_action and after_action methods. The base class methods
# don't do anything, so we don't need to check those.

subtest 'before and after' => sub {

    my $result;
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table($EDT_TEST);
    
    # Check that before_action is called at the proper time, and that an exception in
    # before_action is properly turned into E_EXECUTE.
    
    my $edt = $T->new_edt($perm_a) || return;
    
    $edt->insert_record($EDT_TEST, { string_req => 'before exception', string_val => 'test 1' });
    
    $T->ok_no_errors("no error before execution");
    
    $result = $edt->execute;
    
    $T->ok_has_error(qr/E_EXECUTE/, "error from exception");
    ok( ! $result, "transaction failed" );
    is( $edt->errors, 1, "found just one error" );
    $T->ok_no_record($EDT_TEST, "string_val='test 1'");
    ok( ! $edt->{save_before_count}, "before_action was interrupted" );
    
    # Check that before_action can add a warning and modify the database, and that these
    # operations do not affect the progress of the transaction.

    $edt = $T->new_edt($perm_a);
    $edt->set_attr('before add' => 'test record before');
    
    $edt->insert_record($EDT_TEST, { string_req => 'before warning' });
    
    $result = $edt->execute;

    ok( $result, "transaction executed properly" );
    is( $edt->transaction, 'committed', "transaction committed" );
    $T->ok_found_record($EDT_TEST, "string_req='before warning'");
    $T->ok_found_record($EDT_TEST, "string_req='test record before'");
    $T->ok_no_errors("no errors were generated");
    $T->ok_has_warning(qr/W_TEST.*before/, "found test warning");
    is( $edt->{save_before_count}, 1, "before_action finished" );
    isa_ok( $edt->{save_before_action}, 'EditTransaction::Action', "action record" ) &&
	is( $edt->{save_before_action}->record->{string_req}, 'before warning', "correct action record" );
    is( $edt->{save_before_table}, $EDT_TEST, "before_action passed proper table name" );
    is( $edt->{save_before_operation}, 'insert', "before_action passed proper operation" );
    
    # Check that after_action is called at the proper time.

    $edt = $T->new_edt($perm_a);
    $edt->start_execution;
    
    $edt->insert_record($EDT_TEST, { string_req => 'test after' });
    
    if ( is( $edt->{save_after_count}, 1, "after_action was called once" ) )
    {
	isa_ok( $edt->{save_after_action}, 'EditTransaction::Action', "action record" ) &&
	    is( $edt->{save_after_action}->record->{string_req}, 'test after', "correct action record" );
	is( $edt->{save_after_table}, $EDT_TEST, "after_action passed proper table" );
	is( $edt->{save_after_operation}, 'insert', "after_action passed proper operation" );
    }
    
    $T->ok_found_record($EDT_TEST,  "string_req='test after'");
    
    {
	local $EditTransaction::TEST_PROBLEM{insert_sql} = 1;
	
	$edt->insert_record($EDT_TEST, { string_req => 'test after 2' });
    }
    
    is( $edt->transaction, 'active', "transaction is still active" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
    is( $edt->{save_after_count}, 1, "after_action was not called again" );
    is( $edt->{save_cleanup_action_count}, 1, "cleanup_action was called" );
    $T->ok_has_error(qr/E_EXECUTE/, "found error" );
    
    # Check that an exception thrown by after_action will abort the transaction, but that a
    # warning will be passed along.
    
    $edt = $T->new_edt($perm_a);
    
    $edt->insert_record($EDT_TEST, { string_req => 'after warning' });
    $edt->insert_record($EDT_TEST, { string_req => 'after exception' });
    $result = $edt->execute;
    
    ok( ! $result, "transaction failed" );
    is( $edt->transaction, 'aborted', "transaction was aborted" );
    $T->ok_no_record($EDT_TEST, "string_req='after exception'");
    $T->ok_has_warning(qr/W_TEST.*after/, "found warning" );
    $T->ok_has_error(qr/E_EXECUTE/, "found error" );
    is( $edt->errors, 1, "found just one error" );
    if ( is( $edt->{save_cleanup_action_count}, 1, "cleanup routine was called" ) )
    {
	isa_ok( $edt->{save_cleanup_action_action}, 'EditTransaction::Action', "cleanup was passed an action" );
	is( $edt->{save_cleanup_action_operation}, 'insert', "cleanup was passed proper operation" );
	is( $edt->{save_cleanup_action_table}, $EDT_TEST, "cleanup was passed proper table name" );
    }
    
    # Check that an exception thrown by cleanup_action will be caught and passed along as
    # E_EXECUTE.
    
    $edt = $T->new_edt($perm_a);
    $edt->set_attr('cleanup action exception' => 1);
    
    $edt->insert_record($EDT_TEST, { string_req => 'after exception' });
    $result = $edt->execute;
    
    ok( ! $result, "transaction failed" );
    is( $edt->transaction, 'aborted', "transaction was aborted" );
    is( $edt->errors, 2, "found two errors" );
    $T->ok_has_error(qr/E_EXECUTE.*execution/, "found execution error");
    $T->ok_has_error(qr/E_EXECUTE.*cleanup/, "found cleanup error");
    $T->ok_has_warning(qr/W_TEST.*cleanup/, "found warning");

    # Now make sure that before_action and after_action are all passed the proper operation and
    # action with all four operation types. We set IMMEDIATE_MODE so that each action will be
    # immediately reflected in the database.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    my $new_keyval = $edt->insert_record($EDT_TEST, { string_req => 'op test 1' });
    
    if ( ok( $new_keyval, "got key for newly inserted record" ) )
    {
	is( $edt->{save_before_operation}, 'insert', "before insert operation" );
	is( $edt->{save_after_operation}, 'insert', "after insert operation" );
	isa_ok( $edt->{save_before_action}, 'EditTransaction::Action', "before insert action" ) &&
	    is( $edt->{save_before_action}->record_value('string_req'), 'op test 1', "before insert record" );
	isa_ok( $edt->{save_after_action}, 'EditTransaction::Action', "after insert action" ) &&
	    is( $edt->{save_after_action}->record_value('string_req'), 'op test 1', "after insert record" );
	is( $edt->{save_before_table}, $EDT_TEST, "before insert table" );
	is( $edt->{save_after_table}, $EDT_TEST, "after insert table" );
	is( $edt->{save_after_result}, $new_keyval, "after insert keyval" );
	
	$edt->replace_record($EDT_TEST, { test_no => $new_keyval, string_req => 'op test 2' });
	
	is( $edt->{save_before_operation}, 'replace', "before replace operation" );
	is( $edt->{save_after_operation}, 'replace', "after replace operation" );
	isa_ok( $edt->{save_before_action}, 'EditTransaction::Action', "before replace action" ) &&
	    is( $edt->{save_before_action}->record_value('string_req'), 'op test 2', "before replace record" );
	isa_ok( $edt->{save_after_action}, 'EditTransaction::Action', "after replace action" ) &&
	    is( $edt->{save_after_action}->record_value('string_req'), 'op test 2', "after replace record" );
	is( $edt->{save_before_table}, $EDT_TEST, "before replace table" );
	is( $edt->{save_after_table}, $EDT_TEST, "after replace table" );
	is( $edt->{save_after_result}, undef, "after replace keyval" );
	
	$edt->update_record($EDT_TEST, { test_no => $new_keyval, unsigned_val => 3 });
	
	is( $edt->{save_before_operation}, 'update', "before update operation" );
	is( $edt->{save_after_operation}, 'update', "after update operation" );
	isa_ok( $edt->{save_before_action}, 'EditTransaction::Action', "before update action" ) &&
	    is( $edt->{save_before_action}->record_value('unsigned_val'), '3', "before update record" );
	isa_ok( $edt->{save_after_action}, 'EditTransaction::Action', "after update action" ) &&
	    is( $edt->{save_after_action}->record_value('unsigned_val'), '3', "after update record" );
	is( $edt->{save_before_table}, $EDT_TEST, "before update table" );
	is( $edt->{save_after_table}, $EDT_TEST, "after update table" );
	is( $edt->{save_after_result}, undef, "after update keyval" );
	
	$edt->delete_record($EDT_TEST, $new_keyval);
	
	is( $edt->{save_before_operation}, 'delete', "before delete operation" );
	is( $edt->{save_after_operation}, 'delete', "after delete operation" );
	isa_ok( $edt->{save_before_action}, 'EditTransaction::Action', "before delete action" ) &&
	    is( $edt->{save_before_action}->record_value('test_no'), $new_keyval, "before delete record" );
	isa_ok( $edt->{save_after_action}, 'EditTransaction::Action', "after delete action" ) &&
	    is( $edt->{save_after_action}->record_value('test_no'), $new_keyval, "after delete record" );
	is( $edt->{save_before_table}, $EDT_TEST, "before delete table" );
	is( $edt->{save_after_table}, $EDT_TEST, "after delete table" );
	is( $edt->{save_after_result}, undef, "after delete keyval" );
    }
    
    $result = $edt->commit;

    ok( $result, "transaction succeeded" );
    $T->ok_no_errors("no errors were found");
    $T->ok_no_warnings("no warnings were found");
    is( $edt->transaction, 'committed', "transaction committed" );
    
    # Now test the same for the routine cleanup_action. We need to set special variables to
    # deliberately trigger an exception on each operation, so that the cleanup routine will be
    # called. We also need to set PROCEED_MODE flag so that errors in one operation will not
    # prevent the subsequent ones from being executed.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED_MODE => 1 });
    
    my $new_keyval = $edt->insert_record($EDT_TEST, { string_req => 'cleanup test 1' });
    
    if ( ok( $new_keyval, "got key for newly inserted record" ) )
    {
	local $EditTransaction::TEST_PROBLEM{insert_sql} = 1;
	local $EditTransaction::TEST_PROBLEM{replace_sql} = 1;
	local $EditTransaction::TEST_PROBLEM{update_sql} = 1;
	local $EditTransaction::TEST_PROBLEM{delete_sql} = 1;

	my $second_keyval = $edt->insert_record($EDT_TEST, { string_req => 'cleanup test 2' });
	
	is( $edt->{save_cleanup_action_operation}, 'insert', "cleanup insert operation" );
	isa_ok( $edt->{save_cleanup_action_action}, 'EditTransaction::Action', "cleanup insert action" ) &&
	    is( $edt->{save_cleanup_action_action}->record_value('string_req'), 'cleanup test 2', "cleanup insert record" );
	is( $edt->{save_cleanup_action_table}, $EDT_TEST, "cleanup insert table" );

	$edt->replace_record($EDT_TEST, { test_no => $new_keyval, string_req => 'cleanup test 3' });
	
	is( $edt->{save_cleanup_action_operation}, 'replace', "cleanup replace operation" );
	isa_ok( $edt->{save_cleanup_action_action}, 'EditTransaction::Action', "cleanup replace action" ) &&
	    is( $edt->{save_cleanup_action_action}->record_value('string_req'), 'cleanup test 3', "cleanup replace record" );
	is( $edt->{save_cleanup_action_table}, $EDT_TEST, "cleanup replace table" );

	$edt->update_record($EDT_TEST, { test_no => $new_keyval, unsigned_val => 4 });
	
	is( $edt->{save_cleanup_action_operation}, 'update', "cleanup update operation" );
	isa_ok( $edt->{save_cleanup_action_action}, 'EditTransaction::Action', "cleanup update action" ) &&
	    is( $edt->{save_cleanup_action_action}->record_value('unsigned_val'), '4', "cleanup update record" );
	is( $edt->{save_cleanup_action_table}, $EDT_TEST, "cleanup update table" );

	$edt->delete_record($EDT_TEST, { test_no => $new_keyval });
	
	is( $edt->{save_cleanup_action_operation}, 'delete', "cleanup delete operation" );
	isa_ok( $edt->{save_cleanup_action_action}, 'EditTransaction::Action', "cleanup delete action" ) &&
	    is( $edt->{save_cleanup_action_action}->record_value('test_no'), $new_keyval, "cleanup delete record" );
	is( $edt->{save_cleanup_action_table}, $EDT_TEST, "cleanup delete table" );	
    }

    $result = $edt->commit;

    ok( $result, "transaction succeeded" );
    is( $edt->action_count, 1, "one action succeeded" );
    is( $edt->fail_count, 4, "four actions failed" );
    $T->ok_no_errors("no errors were found");
    $T->ok_no_errors("all errors were converted to warnings");
    is( $edt->warnings, 4, "found 4 warnings" );
    is( $edt->transaction, 'committed', "transaction committed" );
    
};
