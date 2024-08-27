#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# delete.t :
# 
#         Test the 'delete_record' method.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 6;

use TableDefs qw(set_table_property);

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception ok_new_edt capture_mode ok_captured_output
		  ok_has_condition ok_has_one_condition ok_no_conditions ok_only_conditions
		  ok_no_errors ok_has_one_warning ok_diag diag_conditions ok_can_proceed
		  clear_table ok_commit ok_failed_commit ok_rollback
		  ok_found_record ok_no_record ok_record_count fetch_records);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest', 'EDT_TEST');

my @keyvals;
my $inserted_key;

# Check that the 'delete_record' method exists.

subtest 'required methods' => sub {
    
    can_ok('EditTransaction', 'delete_record')
	|| BAIL_OUT "EditTransaction is missing a required method";
};


# Check that we can insert records and delete them again, and then commit the
# changes. If delete_record or the subsequent commit fails, abort the test
# because a vital function is missing.

subtest 'basic delete' => sub {
    
    clear_table;
    
    my $edt = ok_new_edt;
    
    $edt->start_execution;
    
    $edt->insert_record({ string_req => 'aaa1' });
    
    $edt->insert_record({ string_req => 'aaa2' });
    
    $edt->insert_record({ string_req => 'aaa3' });
    
    $edt->insert_record({ string_req => 'aaa4' });
    
    ok_can_proceed( "no error conditions so far" );
    
    ok( @keyvals = $edt->inserted_keys, "retrieved inserted key values" );
    
    ok_commit;
    
    $edt = ok_new_edt;
    
    $edt->start_execution;
    
    $edt->insert_record({ string_req => 'aaa5' });
    
    push @keyvals, $edt->inserted_keys;
    
    ok_record_count( 5, 'EDT_TEST', '*', "five records were inserted" );
    
    ok_eval( sub { $edt->delete_record({ _primary => $keyvals[1] }) },
	     "delete_record succeeded" )
	
	|| BAIL_OUT "delete_record failed";
    
    ok_eval( sub { $edt->commit; }, "commit succeeded" )
	
	|| BAIL_OUT "commit after delete_record failed";
    
    is( $edt->action_result, 1, "action_result indicates 1 record was deleted" );
    
    is( $edt->inserted_keys, 1, "one inserted key" );
    is( $edt->updated_keys, 0, "no updated keys" );
    is( $edt->deleted_keys, 1, "one deleted key" );
    is( $edt->replaced_keys, 0, "no replaced keys" );
    
    ok_record_count( 4, 'EDT_TEST', '*', "four records remain" );
    ok_no_record( 'EDT_TEST', "string_req='aaa2'", "proper record was deleted" );
    
    splice(@keyvals, 1, 1);
};


# Now try deleting multiple records at once, using both an arrayref and a
# comma-separated string list. Note that one of the records has already been
# deleted. Consequently, we are also testing that deletion proceeds even if some
# of the records are absent.

subtest 'delete with multiple keys' => sub {
    
    my @testvals = ($keyvals[0], $keyvals[2], '88', '65', '999');
    
    ok_record_count( 4, 'EDT_TEST', '*', "start with four records" );
    
    # First try to delete the records using an arrayref of key values.
    
    my $edt = ok_new_edt;
    
    $edt->start_execution;
    
    $edt->delete_record(\@testvals);
    
    ok_no_errors;
    ok_has_one_warning( 'W_NOT_FOUND', qr/3/, "got warning about the 3 nonexistent key values" );
    
    ok_can_proceed;
    
    is( $edt->action_result, 2, "action_result indicates two records were deleted" );
    
    ok_record_count( 2, 'EDT_TEST', '*', "two records remain" );
    
    ok_rollback;
    
    ok_record_count( 4, 'EDT_TEST', '*', "delete was rolled back, 4 records again" );
    
    # Then do the same with a string list.
    
    $edt = ok_new_edt;
    
    $edt->start_execution;
    
    my $list = join(", ,", @keyvals) . "  ,,,,  ";
    
    $edt->delete_record($list);
    
    ok_no_conditions;
    
    is( $edt->action_result, 4, "action_result indicates two records were deleted" );
    
    ok_no_record( 'EDT_TEST', '*', "no records remain" );
    
    ok_rollback;
    
    ok_record_count( 4, 'EDT_TEST', '*', "deletion was rolled back 2nd, 4 records again" );
    
    $edt = ok_new_edt;
    
    $edt->delete_record('EDT_TEST', { test_no => \@keyvals });
    
    ok_no_conditions;
    
    ok_commit;
    
    ok_no_record( 'EDT_TEST', '*', "all records were deleted and the deletion committed" );
};


subtest 'delete_record with selection expression' => sub {

    my $edt = ok_new_edt;
    
    $edt->insert_record({ string_req => 'aaa1' });
    
    $edt->insert_record({ string_req => 'aaa2' });
    
    $edt->insert_record({ string_req => 'aaa3' });
    
    $edt->insert_record({ string_req => 'bbb4' });
    
    ok_commit;
    
    ok_record_count( 4, 'EDT_TEST', '*', "four new records were inserted" );
    
    @keyvals = $edt->inserted_keys;
    
    $edt = ok_new_edt;
    
    $edt->delete_record({ _where => "string_req like 'aaa%'" });
    
    ok_no_conditions;
    
    ok_commit;
    
    is( $edt->action_result, 3, "action_result returns 3 indicating that 3 records were deleted" );
    
    ok_record_count(1, 'EDT_TEST', '*', "one record remains" );
};


subtest 'delete_record with bad args' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->delete_record('EDT_TEST', { });
    
    ok_has_one_condition('E_NO_KEY');
    
    $edt->delete_record('EDT_TEST', "abc is not a number");
    
    ok_has_condition('E_BAD_KEY');
    
    $edt->delete_record({ string_req => 'abc' });
    
    ok_has_condition('E_NO_KEY');
    
    my $list = join(",", @keyvals);
    
    $edt->delete_record({ _primary => $list, string_req => '*',
			  _errwarn => "E_RANGE: test error" });
    
    ok_has_one_condition('E_RANGE');
    
    ok_failed_commit( "commit failed due to error condition" );
    
    $edt = ok_new_edt;
    
    $edt->delete_record("271", "272");
    
    ok_has_one_condition( 'E_NOT_FOUND', "got error because none of the specified keys were found" );
    
    $edt = ok_new_edt('NOT_FOUND');
    
    $edt->delete_record("273", "274");
    
    ok_can_proceed;
    
    ok_commit;
};


subtest 'delete_record exceptions' => sub {
    
    my $edt = ok_new_edt;
    
    capture_mode(1);
    
    $edt->delete_record('EDT_TEST', { string_req => 'authorize exception' });
    
    capture_mode(0);
    
    ok_captured_output(qr/authorization.*ETBasicTest/i);
    
    ok_has_condition('E_EXECUTE');
    
    ok_exception( sub { $edt->delete_record('EDT_TEST') }, qr/parameter/,
		  "exception thrown if parameters are missing" );
};


