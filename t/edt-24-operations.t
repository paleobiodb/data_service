#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
#
# edt-24-operations.t : Test the operations other than the basic insert, update, replace,
#                       and delete: insert_update_record, process_record, skip_record,
#                       null_action, table_action, and record_action. Also test the
#                       action references generated from key values.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 8;

use ETBasicTest;
use EditTester qw(ok_eval ok_exception ok_new_edt
		  ok_no_conditions ok_no_errors ok_has_condition ok_has_one_condition
		  ok_action ok_commit ok_failed_commit
		  clear_table sql_command ok_found_record ok_no_record);

use TableDefs qw(%TABLE);


# Establish an EditTester instance.

my $T = EditTester->new('ETBasicTest');


# The following method is called by 'table_action' and 'record_action' tests. It records
# its arguments, and throws an exception if the record contains 'die'.

our (@METHOD_CALLS);

sub ETBasicTest::test_method {
    
    my ($edt, $action, $table_specifier, $record) = @_;
    
    push @METHOD_CALLS, { table => $table_specifier, keyval => $action->keyval,
			  record => $record };
    
    die "test_method exception\n" if ref $record eq 'HASH' && $record->{die};
    
    return 'test result';
}


# Store a record in EDT_TEST with a known key, for use by the subtests below.

sub reset_test_record {
    
    clear_table('EDT_TEST');
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req) VALUES (5, 'five')");
}


# Check that the necessary methods are defined.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'insert_update_record', 'process_record', 'skip_record',
	   'null_action', 'table_action', 'record_action', 'key_labels',
	   'skip_count', 'exec_count', 'record_count')
	
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check that insert_update_record inserts a record if no key value is given, and
# updates the existing record if a key value is given.

subtest 'insert_update_record' => sub {
    
    reset_test_record();
    
    my $edt = ok_new_edt('CREATE');
    
    $edt->insert_update_record('EDT_TEST', { string_req => 'iu new' });
    
    is( $edt->action_operation('&_'), 'insert', "operation without key is 'insert'" );
    
    ok_commit;
    
    ok_found_record('EDT_TEST', "string_req = 'iu new'", "record without key was inserted");
    
    $edt = ok_new_edt;
    
    $edt->insert_update_record('EDT_TEST', { test_no => 5, string_req => 'iu updated' });
    
    is( $edt->action_operation('&_'), 'update', "operation with key is 'update'" );
    
    ok_commit;
    
    ok_found_record('EDT_TEST', "test_no = 5 and string_req = 'iu updated'",
		    "record with key was updated");
    
    $edt = ok_new_edt;
    
    $edt = ok_new_edt('CREATE');
    
    $edt->insert_update_record('EDT_TEST', { test_no => 99, string_req => 'iu missing' });
    
    ok_has_one_condition('&_', 'E_BAD_KEY', qr/99/, "nonexistent key value is rejected with E_BAD_KEY");
    
    $edt = ok_new_edt;
    
    ok_exception( sub { $edt->insert_update_record({ string_req => "x" }) },
		  qr/you must specify a table/, "exception if no table and no default table" );
};


# Check that process_record dispatches to the proper operation.

subtest 'process_record' => sub {
    
    reset_test_record();
    
    my $edt = ok_new_edt('CREATE', table => 'EDT_TEST');
    
    $edt->process_record({ string_req => 'pr default' });
    
    is( $edt->action_operation('&_'), 'insert', "record without key uses the default table" );
    
    $edt->process_record('EDT_TEST', { test_no => 5, string_req => 'pr update' });
    
    is( $edt->action_operation('&_'), 'update', "record with key is an update" );
    
    $edt->process_record('EDT_TEST', { _skip => 1, string_req => 'pr skipped' });
    
    is( $edt->action_status('&_'), 'skipped', "record with _skip is skipped" );
    
    ok_commit;
    
    ok_found_record('EDT_TEST', "string_req = 'pr default'", "inserted record was stored");
    ok_found_record('EDT_TEST', "test_no = 5 and string_req = 'pr update'",
		    "updated record was stored");
    ok_no_record('EDT_TEST', "string_req = 'pr skipped'", "skipped record was not stored");
    
    $edt = ok_new_edt;
    
    $edt->process_record('EDT_TEST', { _operation => 'update', test_no => 5,
				       string_req => 'pr op update' });
    
    is( $edt->action_operation('&_'), 'update', "_operation 'update'" );
    
    $edt->process_record('EDT_TEST', { _operation => 'replace', test_no => 5,
				       string_req => 'pr op replace' });
    
    is( $edt->action_operation('&_'), 'replace', "_operation 'replace'" );
    
    $edt->process_record('EDT_TEST', { _operation => 'delete', test_no => 5 });
    
    is( $edt->action_operation('&_'), 'delete', "_operation 'delete'" );
    
    ok_no_errors('all', "no errors from _operation dispatch");
    
    $edt->process_record('EDT_TEST', { _operation => 'frob' });
    
    ok_has_one_condition('&_', 'E_BAD_OPERATION', qr/frob/, "unknown _operation is rejected");
  
    $edt = ok_new_edt('CREATE');
    
    $edt->process_record('EDT_TEST', { _operation => 'insert', string_req => 'pr op insert' });
    
    is( $edt->action_operation('&_'), 'insert', "_operation 'insert'" );
    
    ok_no_errors('all', "_operation 'insert' without a key value is accepted");
  
    @METHOD_CALLS = ();
    
    $edt = ok_new_edt;
    
    ok_eval( sub { $edt->process_record('EDT_TEST', { _action => 'test_method',
						      test_no => 5 }) },
	     "_action does not throw an exception" );
    
    ok_commit;
    
    is( scalar(@METHOD_CALLS), 1, "_action method was called" );
};


# Check skip_record.

subtest 'skip_record' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->skip_record('EDT_TEST', { string_req => 'skipped 1' });
    
    is( $edt->action_status('&_'), 'skipped', "action status is 'skipped'" );
    is( $edt->action_operation('&_'), 'skip', "action operation is 'skip'" );
    
    $edt->skip_record({ string_req => 'skipped 2' });
    
    is( $edt->action_status('&_'), 'skipped', "skip_record without a table is allowed" );
    
    $edt->skip_record('delete', 'EDT_TEST', { test_no => 5 });
    
    is( $edt->action_operation('&_'), 'delete', "operation given as the first argument" );
    
    $edt->skip_record('EDT_TEST', { _operation => 'update', test_no => 5 });
    
    is( $edt->action_operation('&_'), 'update', "operation given as _operation" );
    
    ok_no_conditions('all');
    
    is( $edt->skip_count, 4, "skip_count" );
    is( $edt->record_count, 4, "record_count" );
    
    ok_commit("transaction with only skipped records commits");
    
    is( $edt->exec_count, 0, "no actions were executed" );
    
    $edt = ok_new_edt;
    
    $edt->skip_record('EDT_TEST', { _operation => 'bogus' });
    
    ok_has_one_condition('&_', 'E_BAD_OPERATION', qr/bogus/, "bad _operation is rejected");
};


# Check null_action.

subtest 'null_action' => sub {
    
    reset_test_record();
    
    my $edt = ok_new_edt;
    
    $edt->null_action('EDT_TEST', { test_no => 5, string_req => 'null' });
    
    ok_no_conditions('all');
    
    ok_commit("transaction with a null action commits");
    
    ok_found_record('EDT_TEST', "test_no = 5 and string_req = 'five'",
		    "null action does not change the database");
    
    is( $edt->action_status('&_'), 'executed', "null action status is 'executed'" );
    is( $edt->exec_count, 1, "null action is counted as executed" );
};


# Check table_action, which calls a method defined by a subclass without authorizing
# against any table row.

subtest 'table_action' => sub {
    
    @METHOD_CALLS = ();
    
    my $edt = ok_new_edt;
    
    $edt->table_action('test_method', 'EDT_TEST', { abc => 1 });
    
    ok_no_conditions('all');
    
    ok_commit;
    
    is( scalar(@METHOD_CALLS), 1, "method was called once" );
    is( $METHOD_CALLS[0]{table}, 'EDT_TEST', "method was passed the table" );
    is( $METHOD_CALLS[0]{record}{abc}, 1, "method was passed the record" );
    
    is( $edt->action_status('&#1'), 'executed', "action status is 'executed'" );
    is( $edt->action_result('&#1'), 'test result', "action result is the method's return value" );
    is( $edt->exec_count, 1, "action is counted as executed" );
    
    $edt = ok_new_edt;
    
    $edt->table_action('test_method', 'EDT_TEST', { die => 1 });
    
    ok_failed_commit("transaction with a method exception fails");
    
    ok_has_condition('all', 'E_EXECUTE', "method exception is reported as E_EXECUTE");
};


# Check record_action, which calls a method defined by a subclass after authorizing
# against the record selected by the key value.

subtest 'record_action' => sub {
    
    reset_test_record();
  
    @METHOD_CALLS = ();
    
    my $edt = ok_new_edt;
    
    ok_eval( sub { $edt->record_action('test_method', 'EDT_TEST', { test_no => 5 }) },
	     "record_action with a key value does not throw an exception" );
    
    ok_commit;
    
    is( scalar(@METHOD_CALLS), 1, "method was called once" );
    is( $METHOD_CALLS[0]{keyval}, 5, "method was called for the selected record" );
    
    $edt = ok_new_edt;
    
    ok_eval( sub { $edt->record_action('test_method', 'EDT_TEST', { abc => 1 }) },
	     "record_action without a key value does not throw an exception" );
    
    ok_has_one_condition('&_', 'E_NO_KEY', "record_action without a key value is rejected");
};


# Check the action references generated from key values and labels.

subtest 'key references and labels' => sub {
    
    reset_test_record();
    
    my $edt = ok_new_edt;
    
    my $ref1 = $edt->update_record('EDT_TEST', { test_no => 5, string_req => 'first' });
    my $ref2 = $edt->update_record('EDT_TEST', { test_no => 5, string_req => 'second' });
    
    ok( $edt->has_action('&5'), "action is referenced by its key value" );
    ok( $edt->has_action('&5-2'), "second action with the same key gets a numbered reference" );
    
    is( $edt->action_ref('&5'), $edt->action_ref($ref1), "key reference names the first action" );
    is( $edt->action_ref('&5-2'), $edt->action_ref($ref2), "numbered reference names the second" );
    
    $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { test_no => 5, _label => 'lab', string_req => 'labeled' });
    
    ok( $edt->has_action('&lab'), "labeled action is referenced by its label" );
    
    is_deeply( $edt->key_labels('EDT_TEST'), { 5 => 'lab' },
	       "key_labels associates the key value with the label" );
};
