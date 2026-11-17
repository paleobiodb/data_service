#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
#
# edt-32-execution.t : Test the results of executing actions: duplicate keys, updates and
#                      deletes that match fewer records than expected, the methods that
#                      report the keys of executed actions, rollback, and the destruction
#                      of an active transaction.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 9;

use ETBasicTest;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_no_conditions ok_no_errors
		  ok_has_condition ok_has_one_condition ok_exception
		  clear_table sql_command ok_found_record ok_no_record ok_record_count);

use TableDefs qw(%TABLE);


# Establish an EditTester instance.

my $T = EditTester->new('ETBasicTest');


# Clear the test tables and add records with known keys. The 'name' column of EDT_SUB
# has a unique key.

sub reset_tables {
    
    clear_table('EDT_TEST');
    clear_table('EDT_SUB');
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req) VALUES (5, 'five'), (6, 'six')");
    sql_command("INSERT INTO <<EDT_SUB>> (aux_no, test_no, name) VALUES (1, 5, 'taken')");
}


# Check that the necessary methods are defined.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'inserted_keys', 'updated_keys', 'replaced_keys', 'deleted_keys',
	   'other_keys', 'failed_keys', 'tables', 'exec_count', 'fail_count', 'skip_count',
	   'rollback')
	
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check that a statement which violates a unique key produces E_DUPLICATE.

subtest 'duplicate keys' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_SUB', { test_no => 5, name => 'taken' });
    
    ok_failed_commit("insert with a duplicate unique key fails");
    
    ok_has_one_condition('all', 'E_DUPLICATE', qr/taken/, "duplicate insert is reported");
    
    is( $edt->action_status('&#1'), 'failed', "action status is 'failed'" );
    
    $edt = ok_new_edt('PROCEED');
    
    $edt->insert_record('EDT_SUB', { test_no => 5, name => 'taken' });
    $edt->insert_record('EDT_TEST', { string_req => 'after duplicate' });
    
    ok_commit("with PROCEED, a duplicate insert does not block the transaction");
    
    ok_found_record('EDT_TEST', "string_req = 'after duplicate'", "later action was executed");
    
    is( $edt->fail_count, 1, "duplicate insert is counted as failed" );
    is( $edt->exec_count, 1, "later action is counted as executed" );
    
    $edt = ok_new_edt;
    
    $edt->update_record('EDT_SUB', { aux_no => 1, name => 'renamed' });
    $edt->insert_record('EDT_SUB', { test_no => 5, name => 'taken' });
    
    ok_commit("record can take a unique value released earlier in the transaction");
    
    $edt = ok_new_edt;
    
    $edt->update_record('EDT_SUB', { aux_no => 1, name => 'taken' });
    
    ok_failed_commit("update to a duplicate unique key fails");
    
    ok_has_one_condition('all', 'E_DUPLICATE', "duplicate update is reported");
};


# Check updates and deletes that change fewer records than were selected.

subtest 'unchanged and missing records' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { test_no => 5 });
    
    ok_commit;
    
    is( $edt->action_status('&#1'), 'skipped', "update with no field values is skipped" );
    is( $edt->skip_count, 1, "update with no field values is counted as skipped" );
    
    $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { test_no => 5, string_req => 'five' });
    
    ok_commit("update that changes nothing commits");
    
    ok_has_one_condition('&#1', 'W_UNCHANGED', "update that changes nothing adds W_UNCHANGED");
    
    is( $edt->action_status('&#1'), 'executed', "unchanged update is still executed" );
    
    $edt = ok_new_edt;
    
    $edt->delete_record('EDT_TEST', 6);
    
    sql_command("DELETE FROM <<EDT_TEST>> WHERE test_no = 6");
    
    ok_commit("delete of a record removed before execution commits");
    
    ok_has_one_condition('&#1', 'W_NOT_FOUND', "delete of a missing record adds W_NOT_FOUND");
    
    reset_tables();
    
    $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { test_no => 6, string_req => 'gone' });
    
    sql_command("DELETE FROM <<EDT_TEST>> WHERE test_no = 6");
    
    ok_commit("update of a record removed before execution commits");
    
    ok_has_one_condition('&#1', 'W_NOT_FOUND', "update of a missing record adds W_NOT_FOUND");
    
    ok( ! (grep { /^W_UNCHANGED/ } $edt->conditions), "update of a missing record does not add W_UNCHANGED" );
    
    reset_tables();
    
    $edt = ok_new_edt;
    
    $edt->delete_record('EDT_TEST', '5,6');
    
    sql_command("DELETE FROM <<EDT_TEST>> WHERE test_no = 6");
    
    ok_commit;
    
    ok_has_one_condition('&#1', 'W_NOT_FOUND', qr/\b1 key/, "delete reports the number of missing keys");
    
    reset_tables();
    
    $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { _primary => '5,6', string_req => 'both' });
    
    sql_command("DELETE FROM <<EDT_TEST>> WHERE test_no = 6");
    
    ok_commit;
    
    ok_has_one_condition('&#1', 'W_NOT_FOUND', qr/\b1 key/, "update reports the number of missing keys");
    
    my ($condition) = grep { /W_NOT_FOUND/ } $edt->conditions;
    
    unlike( $condition, qr/W_NOT_FOUND\S* .*W_NOT_FOUND/, "W_NOT_FOUND message does not repeat the code" );
    
    reset_tables();
    
    $edt = ok_new_edt;
    
    $edt->delete_record('EDT_TEST', '5,6,7');
    
    ok_commit;
    
    ok_has_one_condition('&#1', 'W_NOT_FOUND', qr/1 key value\(s\) were/,
			 "delete message for missing keys is spelled correctly");
};


# Check the methods that report the keys of executed actions.

subtest 'result keys' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'inserted' });
    $edt->update_record('EDT_TEST', { test_no => 5, string_req => 'updated' });
    $edt->replace_record('EDT_TEST', { test_no => 6, string_req => 'replaced' });
    $edt->delete_record('EDT_SUB', 1);
    
    ok_commit;
    
    my ($new_key) = $edt->inserted_keys;
    
    ok( $new_key && $new_key > 6, "inserted_keys returns the new key" );
    
    is( scalar($edt->inserted_keys), 1, "inserted_keys in scalar context returns the count" );
    
    is_deeply( [ $edt->updated_keys ], [ 5 ], "updated_keys" );
    is_deeply( [ $edt->replaced_keys ], [ 6 ], "replaced_keys" );
    is_deeply( [ $edt->deleted_keys ], [ 1 ], "deleted_keys" );
    
    is_deeply( [ $edt->deleted_keys('EDT_SUB') ], [ 1 ], "deleted_keys for one table" );
    is_deeply( [ $edt->deleted_keys('EDT_TEST') ], [ ], "deleted_keys for another table" );
    
    is_deeply( [ sort $edt->tables ], [ 'EDT_SUB', 'EDT_TEST' ], "tables lists the tables acted on" );
    
    $edt = ok_new_edt('PROCEED');
    
    $edt->update_record('EDT_TEST', { test_no => 5, string_req => 'ok' });
    $edt->update_record('EDT_TEST', { test_no => 6, signed_val => 'not a number' });
    
    ok_commit;
    
    is_deeply( [ $edt->updated_keys ], [ 5 ], "updated_keys omits the failed action" );
    is_deeply( [ $edt->failed_keys ], [ 6 ], "failed_keys lists the failed action" );
    
    reset_tables();
    
    $edt = ok_new_edt('IMMEDIATE_MODE');
    
    $edt->insert_record('EDT_TEST', { string_req => 'rolled back' });
    $edt->insert_record('EDT_TEST', { string_req => 'bad', signed_val => 'not a number' });
    
    ok_failed_commit;
    
    ok_no_record('EDT_TEST', "string_req = 'rolled back'", "the first insert was rolled back");
    
    is_deeply( [ $edt->inserted_keys ], [ ], "inserted_keys is empty after a rollback" );
};


# Check that a record_action is reported by other_keys.

subtest 'other keys' => sub {
    
    reset_tables();
    
    no warnings 'once';
    
    local *ETBasicTest::test_record_method = sub { return 'done' };
    
    my $edt = ok_new_edt;
    
    $edt->record_action('test_record_method', 'EDT_TEST', { test_no => 5 });
    
    ok_commit;
    
    is_deeply( [ $edt->other_keys ], [ 5 ], "other_keys lists the record_action key" );
};


# Check rollback.

subtest 'rollback' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'to be rolled back' });
    
    $edt->start_execution;
    
    ok_found_record('EDT_TEST', "string_req = 'to be rolled back'", "record was inserted");
    
    ok( $edt->rollback, "rollback returns true" );
    
    is( $edt->status, 'aborted', "transaction status is 'aborted'" );
    
    ok_no_record('EDT_TEST', "string_req = 'to be rolled back'", "insert was rolled back");
    
    is( $edt->{save_cleanup_count}, 1, "cleanup_transaction was called once" );
    
    is( $edt->exec_count, 1, "exec_count still counts the action that was rolled back" );
    
    is_deeply( [ $edt->inserted_keys ], [ ], "inserted_keys reports nothing after rollback" );
    
    ok( ! $edt->commit, "commit after rollback returns false" );
    
    ok_exception( sub { $edt->insert_record('EDT_TEST', { string_req => 'too late' }) },
		  qr/transaction that has completed/, "no actions may be added after rollback" );
    
    $edt = ok_new_edt;
    
    ok( $edt->rollback, "rollback before the transaction starts returns true" );
    
    is( $edt->status, 'aborted', "transaction status is 'aborted'" );
    
    is( $edt->{save_cleanup_count}, 0, "cleanup_transaction was not called" );
};


# Check that a transaction that goes out of scope while active is rolled back. The
# transaction is created directly, because EditTester keeps a reference to the
# transactions it creates.

subtest 'destroy an active transaction' => sub {
    
    reset_tables();
    
    my $cleanup_reason;
    
    no warnings 'redefine';
    
    local *ETBasicTest::cleanup_transaction = sub { $cleanup_reason = $_[2] };
    
    {
	my $edt = ETBasicTest->new($T->dbh, { table => 'EDT_TEST' });
	
	$edt->insert_record({ string_req => 'destroyed' });
	$edt->start_execution;
	
	ok_found_record('EDT_TEST', "string_req = 'destroyed'", "record was inserted");
    }
    
    ok_no_record('EDT_TEST', "string_req = 'destroyed'", "insert was rolled back when destroyed");
    
    is( $cleanup_reason, 'destroy', "cleanup_transaction was called with reason 'destroy'" );
    
    my ($in_transaction) = $T->dbh->selectrow_array('SELECT @@in_transaction');
    
    ok( ! $in_transaction, "no database transaction remains open" );
};


# Check commit with no actions.

subtest 'commit with no actions' => sub {
    
    my $edt = ok_new_edt;
    
    ok( $edt->commit, "commit with no actions returns true" );
    
    is( $edt->status, 'committed', "transaction status is 'committed'" );
    
    ok( $edt->has_committed, "has_committed returns true" );
    
    is( $edt->{save_init_count}, 0, "initialize_transaction was not called" );
    
    $edt = ok_new_edt;
    
    $edt->add_condition('E_TEST', 'error with no actions');
    
    ok( ! $edt->commit, "commit with no actions but an error returns false" );
    
    is( $edt->status, 'failed', "transaction status is 'failed'" );
};


# Check the action counts.

subtest 'action counts' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt('PROCEED');
    
    $edt->insert_record('EDT_TEST', { string_req => 'counted' });
    $edt->update_record('EDT_TEST', { test_no => 5, signed_val => 'not a number' });
    $edt->skip_record('EDT_TEST', { string_req => 'skipped' });
    
    ok_commit;
    
    is( $edt->record_count, 3, "record_count" );
    is( $edt->action_count, 3, "action_count" );
    is( $edt->exec_count, 1, "exec_count" );
    is( $edt->fail_count, 1, "fail_count" );
    is( $edt->skip_count, 1, "skip_count" );
};
