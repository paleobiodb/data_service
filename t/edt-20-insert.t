#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# insert.t :
# 
#         Test the 'insert_record', 'start_execution', 'commit', and 'rollback'
#         methods.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 8;

use TableDefs qw(set_table_property);
use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception ok_new_edt diag_conditions
		  capture_mode ok_captured_output clear_captured_output
		  ok_has_condition ok_has_error ok_no_conditions ok_has_one_condition
		  ok_only_conditions
		  clear_table ok_commit ok_failed_commit ok_rollback
		  ok_found_record ok_no_record ok_record_count);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest', 'EDT_TEST');


# Make sure that the required methods are present.

subtest 'required methods' => sub {
    
    can_ok('EditTransaction', 'insert_record', 'start_execution')
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
};

# Check that we can insert records and commit the insertions. If insert_record
# fails, bail out. There is no point in continuing until that is fixed.

subtest 'basic insert' => sub {
    
    clear_table('EDT_TEST');
    
    my $edt = ok_new_edt;
    
    ok_eval( sub { $edt->insert_record({ string_req => 'abcd' }); },
	     "insert_record succeeded" )
	
	|| BAIL_OUT "insert_record failed";
    
    ok_no_record( 'EDT_TEST', "string_req='abcd'", "action not yet executed" );
    
    is( $edt->action_keyval, '', "insert action has no key value yet" );
    
    $edt->insert_record({ string_req => 'mnop' });
    
    is( $edt->action_count, 2, "action count is now 2" );
    is( $edt->record_count, 2, "record count is now 2" );
    
    ok_no_conditions;
    
    ok_eval( sub { $edt->commit; }, "commit succeeded" )
	
	|| BAIL_OUT "commit after insert_record failed";
    
    is( $edt->inserted_keys, 2, "two inserted keys" );
    is( $edt->updated_keys, 0, "no updated keys" );
    is( $edt->deleted_keys, 0, "no deleted keys" );
    is( $edt->replaced_keys, 0, "no replaced keys" );
    
    my @keys = $edt->inserted_keys;
    
    is( $keys[0], 1, "first inserted key is 1" );
    is( $keys[1], 2, "second inserted key is 2" );
    
    is( $keys[0], $edt->action_keyval('&#1'), "first inserted key matches action_keyval &#1" );
    is( $keys[1], $edt->action_keyval('&#2'), "second inserted key matches actin_keyval &#2" );
    
    ok_found_record( 'EDT_TEST', "string_req='abcd'", "record 1 was inserted" );
    ok_found_record( 'EDT_TEST', "string_req='mnop'", "record 2 was inserted" );
    
    ok_exception( sub { $edt->insert_record('EDT_TEST', { string_req => 'defg' }) },
		  qr/completed|finished/i, "insert_record exception after commit" );
};


subtest 'insert with rollback' => sub {
    
    ok_record_count( 2, 'EDT_TEST' );
    
    my $edt = ok_new_edt;
    
    $edt->insert_record({ string_req => 'third' });
    
    ok_eval( sub { $edt->rollback; }, "rollback succeeded" )
	
	|| BAIL_OUT "rollback failed";
    
    ok_no_record( 'EDT_TEST', "string_req='third'" );
    
    ok( ! $edt->commit, "commit returns false after rollback" );
    
    ok_no_record( 'EDT_TEST', "string_req='third'" );
    
    ok_record_count( 2, 'EDT_TEST' );
};


subtest 'insert with key' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->insert_record({ test_no => 8, string_req => 'something' });
    
    ok_commit;
    
    ok_record_count( 3, 'EDT_TEST', '*', "table now contains 3 records" );
    
    ok_found_record( 'EDT_TEST', "test_no=8", "found inserted record" );
    
    # now try again while causing 'check_table_permission' to return 'none' when
    # asked for 'insert' permission.
    
    $edt = ok_new_edt;
    
    set_table_property('EDT_TEST', CAN_INSERT => 'none');
    
    $edt->clear_table_cache('EDT_TEST');
    
    $edt->insert_record({ test_no => '85', string_req => 'some other' });
    
    ok_has_condition( 'E_PERM', "got permission error on insert with specified key" );
    
    $edt->abort_action;
    
    $edt->insert_record('EDT_TEST', { string_req => 'success!' });
    
    ok_commit;
    
    ok_record_count( 4, 'EDT_TEST', '*', "table now contains 4 records" );
    
    set_table_property('EDT_TEST', CAN_INSERT => 'unrestricted');
    
    $edt->clear_table_cache('EDT_TEST');
};


subtest 'insert_record with failed commit' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'qxyz' });
    
    $edt->insert_record('EDT_TEST', { signed_val => 'not a number' });
    
    # There should already be an error condition on this record, but if not
    # we'll add another one for the purpose of continuing the test.
    
    unless ( ok_has_error( "insert_record with parameter error produces condition" ) )
    {
	$edt->add_condition('E_FORMAT', "string_val", "test condition");
    }
    
    ok_failed_commit( "commit failed due to error condition" );
};


subtest 'insert_record with immediate execution' => sub {
    
    my $edt = ok_new_edt;
    
    ok( $edt->start_execution, "start_execution succeeded" );
    
    $edt->insert_record({ string_req => 'immediate', string_val => 'first' });
    
    ok_found_record('EDT_TEST', "string_req='immediate'" );
    
    $edt->insert_record({ string_req => 'immediate', string_val => 'second' });
    
    ok_found_record('EDT_TEST', "string_req='immediate' and string_val='second'");
    
    ok_commit;
    
    ok_record_count(2, 'EDT_TEST', "string_req='immediate'");
    
    $edt = ok_new_edt;
    
    $edt->start_execution;
    
    $edt->insert_record({ string_req => 'apples to apples' });
    
    ok_found_record('EDT_TEST', "string_req='apples to apples'", 
		    "record exists while transaction is active");
    
    ok_rollback;
    
    ok_no_record('EDT_TEST', "string_req='apples to apples'",
		 "record vanishes after transaction is rolled back");
};


subtest 'insert_record bad arguments' => sub {
    
    my $edt = ok_new_edt('NO_CREATE');
    
    $edt->insert_record('EDT_TEST', { string_req => 'glax' });
    
    ok_has_one_condition('C_CREATE');
    
    $edt->insert_record('EDT_TESTT', { string_req => 'flux' });
    
    ok_has_condition('E_BAD_TABLE');
    
    $edt->insert_record('EDT_TEST', { string_val => 'flipt' });
    
    ok_has_condition('E_REQUIRED');
    ok_has_condition('C_CREATE');
    ok_only_conditions('E_REQUIRED', 'C_CREATE');
    
    $edt->insert_record('EDT_TEST', { string_req => 'q1', signed_val => 'xyz' });
    
    ok_has_condition('E_FORMAT');
    ok_has_condition('C_CREATE');
    
    $edt->insert_record('EDT_TEST', { string_req => 'flap', _errwarn => "E_TEST: test error" });
    
    ok_has_condition('E_TEST');
    ok_has_condition('C_CREATE');
    
    $edt = ok_new_edt;
    
    $edt->start_execution;
    
    $edt->handle_column('EDT_TEST', string_req => 'pass');
    $edt->insert_record('EDT_TEST', { });
    
    ok_has_condition('W_EMPTY_RECORD');
    
    is( $edt->skip_count, 1, "skipped empty insert" );
    
    $edt->insert_record({ test_no => "86,87", string_req => 'qrs' });
    
    ok_has_condition('E_MULTI_KEY');
    
    $edt->insert_record({ _where => "test_no=3" });
    
    ok_has_condition('E_HAS_WHERE');
};


subtest 'insert_record exceptions' => sub {
    
    my $edt = ok_new_edt;
    
    clear_captured_output;
    
    capture_mode(1);
    
    $edt->insert_record('EDT_TEST', { string_req => 'authorize exception' });
    
    ok_captured_output(qr/authorization.*ETBasicTest/i);
    
    ok_has_condition('E_EXECUTE');
    
    clear_captured_output;
    
    $edt->insert_record('EDT_TEST', { string_req => 'validate exception' });
    
    ok_captured_output(qr/validation.*ETBasicTest/i);
    
    capture_mode(0);
    
    ok_exception( sub { $edt->insert_record('EDT_TEST') }, qr/parameter/,
		  "exception thrown if no parameters are given" );
};
