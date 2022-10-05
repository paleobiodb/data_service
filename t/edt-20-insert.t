#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# insert.t :
# 
#         Test the 'insert_record', 'commit', and 'rollback' methods.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception ok_new_edt clear_table
		  ok_has_condition ok_has_error
		  ok_commit ok_failed_commit ok_rollback
		  ok_found_record ok_no_record ok_count_records fetch_records);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest');


# Check that we can insert records and commit the insertions. If insert_record
# fails, bail out. There is no point in continuing until that is fixed.

subtest 'basic insert' => sub {
    
    clear_table('EDT_TEST');
    
    my $edt = ok_new_edt;
    
    ok_eval( sub { $edt->insert_record('EDT_TEST', { string_req => 'abcd' }); },
	     "insert_record succeeded" )
	
	|| BAIL_OUT "insert_record failed";
    
    $edt->insert_record('EDT_TEST', { string_req => 'mnop' });
    
    is( $edt->action_count, 2, "action count is now 2" );
    is( $edt->record_count, 2, "record count is now 2" );
    
    ok_eval( sub { $edt->commit; }, "commit succeeded" )
	
	|| BAIL_OUT "commit failed";
    
    is( $edt->inserted_keys, 2, "two inserted keys" );
    is( $edt->updated_keys, 0, "no updated keys" );
    is( $edt->deleted_keys, 0, "no deleted keys" );
    is( $edt->replaced_keys, 0, "no replaced keys" );
    
    my @keys = $edt->inserted_keys;
    
    is( $keys[0], 1, "first inserted key is 1" );
    is( $keys[1], 2, "second inserted key is 2" );
    
    ok_found_record( 'EDT_TEST', "string_req='abcd'", "record 1 was inserted" );
    ok_found_record( 'EDT_TEST', "string_req='mnop'", "record 2 was inserted" );
    
    ok_exception( sub { $edt->insert_record('EDT_TEST', { string_req => 'defg' }) },
		  qr/completed|finished/i, "insert_record exception after commit" );
};


subtest 'insert with rollback' => sub {
    
    ok_count_records( 2, 'EDT_TEST' );
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'third' });
    
    ok_eval( sub { $edt->rollback; }, "rollback succeeded" )
	
	|| BAIL_OUT "rollback failed";
    
    ok_no_record( 'EDT_TEST', "string_req='third'" );
    
    ok( ! $edt->commit, "commit returns false after rollback" );
    
    ok_no_record( 'EDT_TEST', "string_req='third'" );
    
    ok_count_records( 2, 'EDT_TEST' );
};


subtest 'insert_record with errors' => sub {
    
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


subtest 'insert_record bad arguments' => sub {
    
    my $edt = ok_new_edt('NO_CREATE');
    
    $edt->insert_record('EDT_TEST', { string_req => 'glax' });
    
    ok_has_condition('C_CREATE');
    
    $edt->insert_record('EDT_TESTT', { string_req => 'flux' });
    
    ok_has_condition('E_BAD_TABLE');
    
    $edt->insert_record('EDT_TEST', { string_val => 'flipt' });
    
    ok_has_condition('E_REQUIRED');
    
    $edt->insert_record('EDT_TEST', { string_req => 'flap', _errwarn => "E_FORMAT: test error" });
    
    ok_has_condition('E_FORMAT');
};


subtest 'insert_record exceptions' => sub {
    
    my $edt = ok_new_edt;
    
    local $EditTransaction::TEST_PROBLEM{authorize_action} = 1;
    
    $edt->insert_record('EDT_TEST', { string_req => 'apple' });
    
    ok_has_condition('E_EXECUTE');
};
