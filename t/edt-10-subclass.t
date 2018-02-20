#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-10-subclass.t : Test that subclassing EditTransaction works properly. Check all of the
# override methods and also additional error and warning templates. Additional conditions were
# already checked in edt-01-basic.t.
#



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use TableDefs qw(init_table_names select_test_tables $EDT_TEST);

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
    
    ok( $edt->{save_authorize_action} && $edt->{save_authorize_action}->isa('EditAction'),
	"first argument is a valid action" );
    ok( $edt->{save_authorize_operation} && $edt->{save_authorize_operation} eq 'delete',
	"second argument is the operation" );
    ok( $edt->{save_authorize_table} && $edt->{save_authorize_table} eq $EDT_TEST,
	"third argument is the table" );
    ok( $edt->{save_authorize_keyexpr} && $edt->{save_authorize_keyexpr} eq "test_no='423'",
	"fourth argument is the keyexpr" );

    cmp_ok( $edt->errors, '==', 4, "only 4 errors were generated" );
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

    ok( $edt->{save_validate_action} && $edt->{save_validate_action}->isa('EditAction'),
	"first argument is a valid action" );
    ok( $edt->{save_validate_operation} && $edt->{save_validate_operation} eq 'delete',
	"second argument is the operation" );
    ok( $edt->{save_validate_table} && $edt->{save_validate_table} eq $EDT_TEST,
	"third argument is the table" );
    ok( $edt->{save_validate_keyexpr} && $edt->{save_validate_keyexpr} eq "test_no='999'",
	"fourth argument is the keyexpr" );
    ok( $edt->{save_validate_errors}, "action had errors" );
    
    cmp_ok( $edt->errors, '==', 4, "found four errors" );
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

    # $T->diag_errors($edt);
    $T->ok_has_error(qr/E_EXECUTE/, "error from exception");
    cmp_ok( $edt->errors, '==', 1, "found just one error" );
    
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
    
    cmp_ok( $edt->{save_init_count}, '==', 1, "called from IMMEDIATE_MODE" );
    
    $edt->set_attr('finalize warning' => 1);
    $edt->start_execution;
    
    cmp_ok( $edt->{save_init_count}, '==', 1, "not called again from start_execution" );
    
    $edt->insert_record($EDT_TEST, { string_req => 'init test 1' });
    
    $result = $edt->execute;
    
    ok( $result, "transaction succeeded" );
    cmp_ok( $edt->{save_init_count}, '==', 1, "not called again from execute" );
    cmp_ok( $edt->{save_final_count}, '==', 1, "finalized on commit" );
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
    cmp_ok( $edt->{save_final_count}, '==', 1, "finalization completed" );
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
    
    # Check that finalize_transaction gets called with a second argument of 1 if the transaction
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
    cmp_ok( $edt->{save_final_count}, '==', 1, "finalization completed" );
    is( $edt->{save_final_rollback}, '1', "finalization second argument was '1'" );

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

    pass("placeholder");
    
    
};
