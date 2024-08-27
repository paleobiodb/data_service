#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-11-proceed.t : Test that PROCEED and NO_RECORDS allowances work
# properly. These allow a transaction to proceed even if errors have occurred.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 4;

use TableDefs qw(%TABLE get_table_property);

use EditTest;
use EditTester;

$DB::single = 1;

# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new({ subclass => 'EditTest' });


# Start by getting the variable values that we need to execute the remainder of the test. If the
# session key 'SESSION-AUTHORIZER' does not appear in the session_data table, then run the test
# 'edt-01-basic.t' first.

my ($perm_a, $perm_b, $primary);

subtest 'setup' => sub {
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;
    
    $primary = get_table_property('EDT_TEST', 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || BAIL_OUT;

    $T->clear_specific_permissions;
};


# Test that an EditTransaction with PROCEED works the way it is supposed to. Errors of all
# kinds are supposed to be converted to warnings.

subtest 'proceed_mode' => sub {
    
    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    
    # Then create a transaction and add some records.
    
    $edt = $T->new_edt($perm_a, { PROCEED => 1, SILENT_MODE => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'proceed test a1' });
    $edt->insert_record('EDT_TEST', { string_req => 'proceed test a2' });
    $edt->insert_record('EDT_TEST', { string_req => 'proceed test delete' });
    
    # Add another one with an explicit error, and one with a warning.
    
    $edt->insert_record('EDT_TEST', { string_req => 'validate error' });
    $edt->insert_record('EDT_TEST', { string_req => 'validate warning' });

    # Add one that will generate an error when executed.

    $edt->insert_record('EDT_TEST', { string_req => 'before exception' });
    
    # Now check that we have the proper errors and warnings.
    
    is( $edt->errors, 1, "transaction has one error" );
    is( $edt->warnings, 1, "transaction has one warning" );
    is( $edt->conditions('fatal'), 0, "transaction has no fatal conditions" );
    is( $edt->conditions('nonfatal'), 2, "transaction has two non-fatal conditions" );
    ok( $edt->can_proceed, "transaction can proceed" );
    
    $T->ok_has_condition('W_TEST', "found warning W_TEST");
    $T->ok_has_condition('F_TEST', "found demoted error F_TEST");
    
    ok( $edt->can_proceed, "transaction can proceed" );
    
    ok( $edt->commit, "transaction succeeded" );
    
    is( $edt->errors, 2, "transaction has two errors" );
    $T->ok_has_condition('F_EXECUTE', "found condition F_EXECUTE");
    
    # Check that the transaction actually committed and that the record with a warning was
    # inserted while the record with an error was not.
    
    $T->ok_found_record('EDT_TEST', "string_req='proceed test a1'");
    $T->ok_found_record('EDT_TEST', "string_req='validate warning'");
    $T->ok_no_record('EDT_TEST', "string_req='validate error'");

    # Now we create a second transaction and do replace, update, and delete operations to make
    # sure that errors are properly changed into warnings for these operations as well. We also
    # set IMMEDIATE_MODE to check that this doesn't interfere with PROCEED.
    
    $edt = $T->new_edt($perm_a, { PROCEED => 1, IMMEDIATE_MODE => 1, SILENT_MODE => 1 });
    
    my ($k1, $k2) = $T->fetch_keys_by_expr('EDT_TEST', "string_req like 'proceed test %'");
    my ($k3) = $T->fetch_keys_by_expr('EDT_TEST', "string_req='proceed test delete'");
    
    ok( $k1, "found first inserted record" );
    ok( $k2, "found second inserted record" );
    ok( $k3, "found third inserted record" );

    if ( $k1 && $k2 && $k3 )
    {
	$edt->update_record('EDT_TEST', { $primary => $k1, signed_val => 8 });
	$edt->update_record('EDT_TEST', { $primary => 99999, signed_val => 9 });
	$edt->replace_record('EDT_TEST', { $primary => $k2, string_req => 'proceed upated',
					  signed_val => 10 });
	$edt->replace_record('EDT_TEST', { $primary => 99998, string_req => 'not updated',
					  signed_val => 11 });
	$edt->delete_record('EDT_TEST', $k3);
	$edt->delete_record('EDT_TEST', 99997);
	
	local $EditTransaction::TEST_PROBLEM{insert_sql} = 1;
	
	$edt->insert_record('EDT_TEST', { string_req => 'not inserted' });
    }
    
    ok( $edt->can_proceed, "transaction can proceed" );
    
    my $result = $edt->commit;
    
    ok( $result, "transaction succeeded in PROCEED" ) || $T->diag_errors;
    is( $T->conditions('nonfatal'), 4, "got 4 demoted errors" ) || $T->diag_warnings;
    is( $edt->record_count, 7, "record_count of 7 with PROCEED" );
    is( $edt->exec_count, 3, "action_count of 3 with PROCEED" );
    is( $edt->fail_count, 4, "fail_count of 4 with PROCEED" );
    is( $edt->actions('executed'), 3, "got 3 executed action records with PROCEED" );
    is( $edt->actions('notex'), 4, "got 4 failed action records with PROCEED" );
    is( $edt->actions('all'), 7, "got 7 total action records with PROCEED" );
    
    $T->ok_has_error('F_NOT_FOUND', "got 'F_NOT_FOUND'");
    $T->ok_has_error('F_EXECUTE', "got 'F_EXECUTE'");
    $T->ok_has_error('F_PERM', "got 'F_PERM'");
    
    $T->ok_found_record('EDT_TEST', "signed_val=8");
    $T->ok_no_record('EDT_TEST', "signed_val=9");
    $T->ok_found_record('EDT_TEST', "signed_val=10");
    $T->ok_no_record('EDT_TEST', "signed_val=11");
    
    $T->ok_no_record('EDT_TEST', "string_req='proceed test delete'");
};


# Now test the NOT_FOUND allowance. This should allow a transaction to proceed one or more actions
# refer to nonexistent records, with these simply being skipped. But all other errors should stop
# the transaction.

subtest 'not_found' => sub {

    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    
    # Then create a transaction and add some records.
    
    $edt = $T->new_edt($perm_a);
    
    $edt->insert_record('EDT_TEST', { string_req => 'notfound test a1' });
    $edt->insert_record('EDT_TEST', { string_req => 'notfound test a2' });
    $edt->insert_record('EDT_TEST', { string_req => 'notfound test delete' });
    
    ok( $edt->commit, "initial transaction succeeded" ) || $T->diag_errors;
    
    # Now try a transaction with some "not found" errors.
    
    $edt = $T->new_edt($perm_a, { NOT_FOUND => 1 });

    my ($k1, $k2) = $T->fetch_keys_by_expr('EDT_TEST', "string_req like 'notfound test %'");
    my ($k3) = $T->fetch_keys_by_expr('EDT_TEST', "string_req='notfound test delete'");
    
    ok( $k1, "found first inserted record" );
    ok( $k2, "found second inserted record" );
    ok( $k3, "found third inserted record" );

    return unless $k1 && $k2 && $k3;
    
    $edt->update_record('EDT_TEST', { $primary => $k1, signed_val => 21 });
    $edt->update_record('EDT_TEST', { $primary => 99999, signed_val => 22 });
    $edt->replace_record('EDT_TEST', { $primary => 99999, string_req => 'cannot update',
				      signed_val => 23 });
    $edt->replace_record('EDT_TEST', { $primary => $k2, string_req => 'notfound updated',
				      signed_val => 24 });
    $edt->delete_record('EDT_TEST', { $primary => $k3 });
    $edt->delete_record('EDT_TEST', { $primary => 99999 });
    
    ok( $edt->can_proceed, "transaction can proceed" );
    
    $result = $edt->commit;
    
    ok( $result, "transaction succeeded with NOT_FOUND" ) || $T->diag_errors;
    is( $T->conditions('nonfatal'), 3, "got 3 demoted errors" ) || $T->diag_warnings;
    is( $edt->record_count, 6, "record_count of 6 with NOT_FOUND" );
    is( $edt->exec_count, 3, "action_count of 3 with NOT_FOUND" );
    is( $edt->fail_count, 3, "fail_count of 3 with NOT_FOUND" );
    is( $edt->actions('executed'), 3, "got 3 executed action records with NOT_FOUND" );
    is( $edt->actions('notex'), 3, "got 3 failed action records with NOT_FOUND" );
    is( $edt->actions('all'), 6, "got 6 total action records with NOT_FOUND" );
    $T->ok_has_error('F_NOT_FOUND', "got 'F_NOT_FOUND'");
    
    $T->ok_found_record('EDT_TEST', "signed_val=21");
    $T->ok_no_record('EDT_TEST', "signed_val=22");
    $T->ok_no_record('EDT_TEST', "signed_val=23");
    $T->ok_found_record('EDT_TEST', "signed_val=24");
    
    $T->ok_no_record('EDT_TEST', "string_req='notfound test delete'");
    
    # Now check that other errors do abort the transaction.

    $edt = $T->new_edt($perm_a, { NOT_FOUND => 1 });
    
    $edt->update_record('EDT_TEST', { $primary => 99998, string_req => 'cannot update' });
    ok( $edt->can_proceed, "not found error is okay" );
    $edt->update_record('EDT_TEST', { $primary => $k1, signed_val => 'abc' });
    $T->ok_has_error( 'E_FORMAT' );
    ok( ! $edt->can_proceed, "parameter error is not okay" );

    # And same with IMMEDIATE_MODE.

    $edt = $T->new_edt($perm_a, { NOT_FOUND => 1, IMMEDIATE_MODE => 1, SILENT_MODE => 1 });

    $edt->update_record('EDT_TEST', { $primary => 99997, string_req => 'cannot update' });
    ok( $edt->can_proceed, "not found error is okay" );
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'after exception' });
    ok( ! $edt->can_proceed, "execute error is not okay" );

    $result = $edt->commit;
    
    ok( ! $result, "transaction failed" );
    is( $edt->transaction, 'failed', "transaction was rolled back" );
};


# Then test the NOT_PERMITTED allowance. This requires the use of a different set of permissions
# from the usual 'SESSION-AUTHORIZER'.

subtest 'not_permitted' => sub {

    my ($edt);
    
    # Make sure we have a record in the table created by 'SESSION-AUTHORIZER'. If we can't find
    # the one created by subtest 'not_found' above, create one now.
    
    $perm_b = $T->new_perm('SESSION-OTHER');
    
    ok( $perm_b && $perm_b->role eq 'enterer', "found other permission" ) || return;
    
    my ($test_no) = $T->fetch_keys_by_expr('EDT_TEST', "string_req='notfound test a1'");
    
    unless ( $test_no )
    {
	$edt = $T->new_edt($perm_a);
	$edt->insert_record('EDT_TEST', { string_req => 'notfound test a1' });
	ok( $edt->commit, "successfully inserted test record" );
	$test_no = $edt->action_keyval;
    }

    # Then create a transaction with the 'SESSION-OTHER' permission. This set of permissions does
    # not allow updating of records created by 'SESSION-AUTHORIZER'. This transaction is created
    # with the allowance 'NOT_PERMITTED', which causes permission errors to be demoted.
    
    $edt = $T->new_edt($perm_b, { NOT_PERMITTED => 1 });
    
    # Attempt to update the record, which should generate an F_PERM condition.
    
    $edt->update_record('EDT_TEST', { $primary => $test_no, signed_val => 23 });

    ok( $edt->has_condition('F_PERM'), "found F_PERM with NOT_PERMITTED" );
    
    # Check that the transaction can proceed, then insert a record and commit.
    
    if ( ok( $edt->can_proceed, "transaction can proceed" ) )
    {
	$edt->insert_record('EDT_TEST', { string_req => 'insert test not_permitted' });

	ok( $edt->commit, "transaction committed" );

	$T->ok_found_record('EDT_TEST', "string_req = 'insert test not_permitted'");
    }
    
    # Try the same thing again, but make sure that NOT_FOUND and NOT_PERMITTED each work properly
    # when both are specified.

    $edt = $T->new_edt($perm_b, { NOT_FOUND => 1, NOT_PERMITTED => 1 });

    # Attempt to update a nonexistent record, which should generate an F_NOT_FOUND condition.

    $edt->update_record('EDT_TEST', { $primary => 999999, string_req => 'not found' });

    # Attempt to update the same record as above, which should generate an F_PERM condition.

    $edt->update_record('EDT_TEST', { $primary => $test_no, signed_val => 24 });

    ok( $edt->can_proceed, "transaction can proceed" );

    # Now add a record that is lacking the required field. This should generate an E_REQUIRED
    # error which is not demoted.

    $edt->insert_record('EDT_TEST', { signed_val => 25 });

    ok( ! $edt->can_proceed, "transaction cannot proceed after error" );

    # Make sure it has the proper condition codes and counts.
    
    ok( $edt->has_condition('F_NOT_FOUND'), "found F_NOT_FOUND" );
    ok( $edt->has_condition('F_PERM'), "found F_PERM" );
    ok( $edt->has_condition('E_REQUIRED'), "found E_REQUIRED" );
    
    is( $edt->fatals, 1, "one fatal condition" );
    is( $edt->nonfatals, 2, "two nonfatal conditions" );
    is( $edt->errors, 3, "three errors total" );
    is( $edt->warnings, 0, "no warnings" );
};
    

# # Then test the NO_RECORDS allowance. This allows a transaction to be committed even if there are
# # no valid completed actions.

# subtest 'no_records' => sub {
    
#     my ($edt, $result);
    
#     # Clear the table so we can check for proper record insertion.
    
#     $T->clear_table('EDT_TEST');
    
#     # Then create a transaction and execute it without any actions.

#     $edt = $T->new_edt($perm_a);
#     $edt->start_transaction;
    
#     $result = $edt->execute;
    
#     ok( ! $result, "transaction failed" );
#     is( $edt->transaction, 'aborted', "transaction was rolled back" );
#     $T->ok_has_error( 'C_NO_RECORDS', "found no records caution" );
    
#     is( $edt->{save_init_count}, 1, "initialize_transaction was called" );
#     ok( ! $edt->{save_final_count}, "finalize_transaction was not called" );
#     is( $edt->{save_cleanup_count}, 1, "cleanup_transaction was called" );
    
#     # Then create a transaction with NO_RECORDS mode and start it immediately. Check that it
#     # commits properly.
    
#     $edt = $T->new_edt($perm_a, { NO_RECORDS => 1 });
#     $edt->start_transaction;
    
#     $result = $edt->execute;
    
#     ok( $result, "transaction succeeded with NO_RECORDS" ) || $T->diag_errors;
#     is( $edt->transaction, 'committed', "transaction committed" );
#     ok( ! $edt->errors, "no errors were generated" );
    
#     is( $edt->{save_init_count}, 1, "initialize_transaction was called" );
#     is( $edt->{save_final_count}, 1, "finalize_transaction was called" );
# };
