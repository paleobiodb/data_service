#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# update.t :
# 
#         Test the 'update_record' method.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 7;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception ok_new_edt capture_mode ok_captured_output
		  ok_has_condition ok_has_one_condition ok_no_conditions
		  ok_has_one_warning ok_diag diag_conditions
		  clear_table ok_commit ok_failed_commit ok_rollback
		  ok_found_record ok_no_record ok_record_count fetch_records);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest', 'EDT_TEST');

my $inserted_key;


# Check that the 'update_record' method exists.

subtest 'required methods' => sub {
    
    can_ok('EditTransaction', 'update_record')
	|| BAIL_OUT "EditTransaction is missing a required method";
};


# Check that we can insert and update records and commit the changes. If
# update_record fails, bail out because the test cannot continue.

subtest 'basic update' => sub {
    
    clear_table;
    
    my $edt = ok_new_edt;
    
    $edt->start_execution;
    
    $edt->insert_record({ string_req => 'abcd', string_val => 'flip', signed_val => -27 });
    
    ok_no_conditions;
    
    ok_diag( $inserted_key = $edt->action_keyval, "retrieved inserted key value" );
    
    ok_found_record('EDT_TEST', "string_val='flip'");
    
    ok_eval( sub { $edt->update_record({ _primary => $inserted_key, string_val => 'flop' }); },
	     "update_record succeeded" )
	
	|| BAIL_OUT "update_record failed";
    
    ok_no_conditions;
    
    ok_eval( sub { $edt->commit; }, "commit succeeded" )
	
	|| BAIL_OUT "commit after insert_record and update_record failed";
    
    is( $edt->inserted_keys, 1, "one inserted key" );
    is( $edt->updated_keys, 1, "one updated key" );
    is( $edt->deleted_keys, 0, "no deleted keys" );
    is( $edt->replaced_keys, 0, "no replaced keys" );
    
    my @inserted = $edt->inserted_keys;
    my @updated = $edt->updated_keys;
    
    is( $inserted[0], $updated[0], "updated key and inserted key are the same" );
    
    is( $edt->action_result, 1, "action_result indicates 1 record was changed" );
    
    is( $edt->action_matched, 1, "action_affected indicates 1 record was matched" );
    
    ok_found_record( 'EDT_TEST', "string_val = 'flop' and signed_val = -27",
		     "update preserves column not mentioned" );
};


subtest 'update_record with multiple keys' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->start_execution;
    
    $edt->insert_record({ string_req => 'hhhj' });
    
    my $second_key;
    
    ok_diag( $second_key = $edt->action_keyval, "retrieved inserted key value" );
    
    $edt->update_record({ string_val => 'multi key', _primary => [$inserted_key, $second_key, '883'] });
    
    is( $edt->action_result, 2, "action_result indicates 2 records were changed" );
    
    is( $edt->action_matched, 2, "action_matched indicates 2 records were matched" );
    
    ok_has_one_warning( 'W_NOT_FOUND', qr/1\b.*not found/, "got warning indicating 1 keyval not found" );
    
    $edt->update_record({ signed_val => 10000, _primary => "$inserted_key,,$second_key" });
    
    is( $edt->action_result, 2, "action_result indicates 2 records were changed" );
    
    is( $edt->action_matched, 2, "action_matched indicates 2 records were matched" );
    
    ok_no_conditions;
    
    ok_commit;
    
    ok_record_count( 2, 'EDT_TEST', "string_val='multi key'", "found both updated records" );
    ok_record_count( 2, 'EDT_TEST', "signed_val=10000", "found both updated records 2" );
};


subtest 'update_record with where clause' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->update_record({ signed_val => 3, _where => "signed_val = '10000'" });
    
    ok_commit;
    
    is( $edt->action_result, 2, "action_result indicates 2 rows were changed" );
    
    is( $edt->action_matched, 2, "action_matched indicates 2 rows were matched" );
    
    ok_record_count( 2, 'EDT_TEST', "signed_val=3", "found 2 updated rows" );
};


subtest 'update_record unchanged' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->update_record({ signed_val => 3, _where => "signed_val=3" });
    
    ok_commit;
    
    is( $edt->action_result, 0, "action_result indicates no rows were changed" );
    
    is( $edt->action_matched, 2, "action_matched indicates 2 rows were matched" );
    
    ok_has_one_warning( 'W_UNCHANGED', qr/2\b.*unchanged/, 
			"got warning that 2 rows were unchanged" );
    
    $edt = ok_new_edt;
    
    $edt->start_execution;
    
    $edt->insert_record({ string_req => 'ddd1'});
    
    my $keyval = $edt->action_keyval;
    
    $edt->update_record({ _primary => $keyval, string_req => 'ddd1' });
    
    is( $edt->action_result, 0, "action_result indicates no rows were changed" );
    
    is( $edt->action_matched, 1, "action_matched indicates 1 row was matched" );
    
    $edt->insert_record({ string_req => 'ddd2'});
    
    $edt->insert_record({ string_req => 'ddd3'});
    
    $edt->insert_record({ string_req => 'ddd4'});
    
    my @keyvals = $edt->inserted_keys;
    
    $edt->update_record({ _primary => \@keyvals, string_req => 'ddd3' });
    
    is( $edt->action_result, 3, "action_result indicates 3 rows were changed" );
    
    is( $edt->action_matched, 4, "action_matched indicates 4 rows were matched" );
};


subtest 'update_record bad arguments' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { string_req => 'qxyz' });
    
    ok_has_one_condition('E_NO_KEY');
    
    $edt->update_record('EDT_TEST', { _primary => $inserted_key, string_val => '25', string_req => '' });
    
    ok_has_one_condition('E_REQUIRED');
    
    $edt->update_record('EDT_TESTT', { _primary => $inserted_key, signed_val => 5 });
    
    ok_has_one_condition('E_BAD_TABLE');
    
    $edt->update_record('EDT_TEST', { test_no => $inserted_key, signed_val => 'not a number' });
    
    ok_has_one_condition('E_FORMAT');
    
    $edt->update_record('EDT_TEST', { _primary => $inserted_key, string_req => '*',
				      _errwarn => "E_RANGE: test error" });
    
    ok_has_one_condition('E_RANGE');
    
    $edt->update_record('EDT_TEST', { _where => "string_va='bar'", string_val => 'baz' });
    
    ok_has_one_condition('E_EXECUTE');
    
    ok_failed_commit( "commit failed due to error condition" );
    
    $edt = ok_new_edt;
    
    $edt->start_execution;
    
    $edt->update_record('EDT_TEST', { _primary => $inserted_key });
    
    ok_has_one_condition('W_EMPTY_RECORD');
    
    is( $edt->skip_count, 1, "skipped empty record" );
};


subtest 'update_record exceptions' => sub {
    
    my $edt = ok_new_edt;
    
    capture_mode(1);
    
    $edt->update_record('EDT_TEST', { string_req => 'authorize exception' });
    
    capture_mode(0);
    
    ok_captured_output(qr/authorization.*ETBasicTest/i);
    
    ok_has_condition('E_EXECUTE');
};
