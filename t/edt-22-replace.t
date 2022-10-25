#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# replace.t :
# 
#         Test the 'replace_record' method.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use TableDefs qw(set_table_property);

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception ok_new_edt capture_mode ok_captured_output
		  ok_has_condition ok_has_one_condition ok_no_conditions ok_only_conditions
		  ok_diag diag_conditions
		  clear_table ok_commit ok_failed_commit ok_rollback
		  ok_found_record ok_no_record ok_record_count fetch_records);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest', 'EDT_TEST');

my $inserted_key;


# Check that the 'replace_record' method exists.

subtest 'required methods' => sub {
    
    can_ok('EditTransaction', 'replace_record')
	|| BAIL_OUT "EditTransaction is missing a required method";
};


# Check that we can insert and replace records and commit the changes. If
# replace_record fails, bail out because the test cannot continue.

subtest 'basic replace' => sub {
    
    clear_table;
    
    my $edt = ok_new_edt;
    
    $edt->start_execution;
    
    $edt->insert_record({ string_req => 'abcd', string_val => 'flip', signed_val => -27 });
    
    ok_no_conditions;
    
    ok_diag( $inserted_key = $edt->action_keyval, "retrieved inserted key value" );
    
    ok_found_record('EDT_TEST', "string_val='flip'");
    
    ok_eval( sub { $edt->replace_record({ _primary => $inserted_key, string_req => 'replacement' }); },
	     "replace_record succeeded" )
	
	|| BAIL_OUT "replace_record failed";
    
    ok_no_conditions;
    
    ok_eval( sub { $edt->commit; }, "commit succeeded" )
	
	|| BAIL_OUT "commit after insert_record and replace_record failed";
    
    is( $edt->inserted_keys, 1, "one inserted key" );
    is( $edt->updated_keys, 0, "no updated keys" );
    is( $edt->deleted_keys, 0, "no deleted keys" );
    is( $edt->replaced_keys, 1, "one replaced key" );
    
    my @inserted = $edt->inserted_keys;
    my @replaced = $edt->replaced_keys;
    
    is( $inserted[0], $replaced[0], "replaced key and inserted key are the same" );
    
    ok_found_record( 'EDT_TEST', "string_req = 'replacement' and signed_val is null",
		     "replace_record replaces the record rather than updating it" );
};


subtest 'replace_record with nonexistent key value' => sub {
    
    # First try without the CREATE allowance
    
    my $edt = ok_new_edt('NO_CREATE');;
    
    ok_record_count( 1, 'EDT_TEST', '*', "table starts with one record" );
    
    $edt->replace_record({ _primary => '801', string_req => 'flap' });
    
    ok_has_condition('C_CREATE');
    
    ok_failed_commit;
    
    ok_only_conditions('all', 'C_CREATE');
    
    # Then again with the CREATE allowance.
    
    $edt = ok_new_edt;
    
    ok_record_count( 1, 'EDT_TEST', '*', "table starts with one record" );
    
    $edt->replace_record({ _primary => '801', string_req => 'flap' });
    
    ok_no_conditions;
    
    ok_commit;
    
    ok_record_count( 2, 'EDT_TEST', '*', "replace_record generated an insertion" );
    
    ok_found_record( 'EDT_TEST', "string_req='flap'", "found inserted record" );
    
    # Now try setting the CAN_INSERT table property to 'none', which disables
    # inserting records with specified keys.
    
    $edt = ok_new_edt;
    
    set_table_property('EDT_TEST', CAN_INSERT => 'none');
    
    $edt->clear_table_cache('EDT_TEST');
    
    $edt->replace_record({ test_no => '852', string_req => 'this should fail' });
    
    ok_has_one_condition( 'E_PERM', "got permission error on insert with specified key" );
    
    $edt->abort_action;
    
    $edt->replace_record({ test_no => $inserted_key, string_req => 'successful replacement' });
    
    ok_no_conditions;
    
    ok_commit;
    
    ok_found_record( 'EDT_TEST', "string_req='successful replacement'", "found replaced record" );
    
    set_table_property('EDT_TEST', CAN_INSERT => 'unrestricted');
    
    $edt->clear_table_cache('EDT_TEST');
};


subtest 'replace_record bad arguments' => sub {
    
    my $edt = ok_new_edt('NO_CREATE');
    
    $edt->replace_record('EDT_TEST', { test_no => 901, string_req => 'vxyyz' });
    
    ok_has_one_condition('C_CREATE');
    
    $edt->replace_record('EDT_TEST', { _primary => $inserted_key, string_val => '25', string_req => '' });
    
    ok_has_one_condition('E_REQUIRED');
    
    $edt->replace_record('EDT_TEST', { _primary => $inserted_key, signed_val => 'not a number' });
    
    ok_has_condition('E_FORMAT');
    ok_has_condition('E_REQUIRED');
    ok_only_conditions('E_FORMAT', 'E_REQUIRED');
    
    $edt->replace_record('EDT_TEST', { _primary => $inserted_key, string_req => '*',
				      _errwarn => "E_RANGE: test error" });
    
    ok_has_one_condition('E_RANGE');
    
    $edt->replace_record('EDT_TEST', { _where => "string_va='bar'", string_val => 'baz' });
    
    ok_has_condition('E_HAS_WHERE');
    
    ok_failed_commit( "commit failed due to error condition" );
};


subtest 'replace_record exceptions' => sub {
    
    my $edt = ok_new_edt;
    
    capture_mode(1);
    
    $edt->replace_record('EDT_TEST', { string_req => 'authorize exception' });
    
    capture_mode(0);
    
    ok_captured_output(qr/authorization.*ETBasicTest/i);
    
    ok_has_condition('E_EXECUTE');
};
