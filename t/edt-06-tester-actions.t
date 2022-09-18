#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest and EditTester classes. They
# must be tested together, because the test methods of EditTester are designed
# to report problems with calls to ETBasicTest and other subclasses of
# EditTransaction. This module makes calls with known problems to check that the
# error-checking and condition-reporting functionality works properly.
# 
# The test methods implemented by EditTester all start with 'ok_' or 'is_'.
# 
# tester-conditions.t :
#
#         Check that the methods implemented by EditTester for testing
#         conditions work properly, by using them on EditTransaction instances
#         with simple arguments that should work properly, and with other sets
#         of arguments that should fail in known ways.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 7;

use ETBasicTest;
use ETTrivialClass;

use EditTester qw(ok_new_edt last_edt clear_edt test_mode ok_test_output
		  ok_condition_count ok_no_conditions ok_has_condition ok_has_one_condition
		  ok_no_errors ok_no_warnings ok_has_one_error ok_has_one_warning
		  ok_eval ok_exception ok_diag is_diag
		  select_tester current_tester target_class
		  default_table set_default_table);


$DB::single = 1;

# Check that we get the proper exceptions when trying to do tests without
# creating an EditTester and/or EditTransaction.

subtest 'no tester yet' => sub {
    
    ok_exception( sub { ok_action( 'E_FOOBAR' ); }, qr/EditTester/,
		  "no tester yet for ok_action" );
    
    ok_exception( sub { default_table; }, qr/EditTester/,
		  "no tester yet for default_table" );
};


my $T = EditTester->new('ETBasicTest');





    
    ok_exception( sub { ok_diag(2, "test"); },
		  qr/EditTransaction/, "ok_diag with missing edt throws an exception" );
    
    ok_exception( sub { is_diag(2, 2, "test"); },
		  qr/EditTransaction/, "is_diag with missing edt throws an exception" );
    


subtest 'ok_diag bad arguments' => sub {

    ok_new_edt;
    
    # Then test bad arguments to ok_diag and is_diag.
    
    ok_exception( sub { ok_diag(1); }, qr/result/, 
		  "ok_diag with fewer than two arguments throws an exception" );
    
    ok_exception( sub { is_diag("abc", "abc"); }, qr/compare/,
		  "is_diag with fewer than three arguments throws an exception" );
};


