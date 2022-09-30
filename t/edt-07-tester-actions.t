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
# tester-actions.t :
#
#         Check that the methods implemented by EditTester for testing actions
#         and commit status work properly, by using them on EditTransaction
#         instances with simple arguments that should work properly, and with
#         other sets of arguments that should fail in known ways.

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 7;

use ETBasicTest;
use ETTrivialClass;

use EditTester qw(ok_new_edt last_edt clear_edt
		  invert_mode diag_mode ok_output ok_no_output clear_output
		  ok_action ok_failed_action ok_commit ok_failed_commit ok_rollback
		  ok_eval ok_exception);


$DB::single = 1;

my $T = EditTester->new('ETBasicTest');


subtest 'no edt yet' => sub {
    
    ok_exception( sub { ok_action; }, qr/EditTransaction/,
		  "no edt yet" );
    
    is( last_edt, undef, "last_edt returns undef" );
};


subtest 'successful action' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->_test_action('insert', 'EDT_TEST', { string_req => 'abc' });
    
    diag_mode(1);
    
    ok_action;
    
    ok_action( "ok_action with label" );
    
    ok_action( 'pending', "ok_action with status code" );
    
    ok_action( '&#1', "ok_action with action reference" );
    
    ok_action( '&#1', 'pending', "ok_action with reference and status code" );
    
    ok_no_output;
    
    invert_mode(1);
    
    ok_failed_action;
    
    ok_output( qr/'pending'/, "invert: diag status code" );
    
    ok_failed_action( "inverted: ok_failed_action" );
    
    clear_output;
    
    ok_failed_action( 'skipped', "inverted: ok_failed_action with status code" );
    
    ok_output( qr/'pending'/, "inverted: diag status code" );
    
    ok_failed_action( '&#1', "inverted: ok_failed_action with action reference" );
    
    ok_failed_action( '&#1', 'skipped', "inverted: ok_failed_action with reference and status code" );
    
    clear_output;
    
    ok_action( 'executed', "inverted: ok_action with wrong status code" );
    
    ok_output( qr/'pending'/, "inverted: diag status code" );
    
    clear_output;
    
    ok_action( '&#2', " inverted: ok_action with invalid reference" );
    
    ok_output( qr/matching action/i, "inverted: diag no matching action" );
    
    invert_mode(0);
};


subtest 'failed action' => sub {
    
    diag_mode(1);
    
    clear_output;
    
    my $edt = ok_new_edt;
    
    $edt->_test_action('insert', 'EDT_TEST', { string_req => 'abc' });
    
    $edt->add_condition('E_FORMAT', "foobar");
    
    ok_failed_action;
    
    ok_failed_action( "ok_failed_action passes from condition code" );
    
    ok_failed_action( 'aborted', "ok_action with status code" );
    
    ok_failed_action( '&#1', "ok_action with action reference" );
    
    ok_failed_action( '&#1', 'aborted', "ok_action with reference and status code" );
    
    ok_no_output( "no diagnostic output from ok_failed_action" );;
    
    invert_mode(1);
    
    clear_output;
    
    ok_action;
    
    ok_output( qr/'aborted'/, "inverted: diag status code 'aborted'");
    
    ok_output( qr/E_FORMAT.*foobar/, "inverted: ok_action diag condition message 'E_FORMAT.*foobar'" );
    
    clear_output;
    
    ok_failed_action( 'skipped', "inverted: ok_failed_action with wrong status code" );
    
    ok_output( qr/'aborted'/, "diag status code 'aborted" );
    
    clear_output;
    
    ok_failed_action( '&#99', "inverted: ok_failed_action with invalid reference" );
    
    ok_output( qr/matching action/i, "inverted: diag no matching action" );
    
    clear_output;
    
    ok_action( 'executed', "inverted: ok_action with wrong status code" );
    
    ok_output( qr/'aborted'/, "inverted: diag status code 'aborted'" );
    
    invert_mode(0);
    
    $edt->skip_record('EDT_TEST', { string_req => "abc" });
    
    ok_failed_action( "ok_failed_action passes from skip" );
    
    invert_mode(1);
    
    ok_action( "ok_action fails from skip" );
    
    invert_mode(0);
};


subtest 'ok_action bad arguments' => sub {
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { ok_action('executd', "misspelled status code" ); },
		  qr/unknown status code/i, "misspelled status code throws exception" );
};


subtest 'successful commit' => sub {
    
    diag_mode(1);
    
    clear_output;
    
    my $edt = ok_new_edt;
    
    $edt->skip_record({ abc => 1 });
    
    ok_commit "ok_commit";
    
    ok_no_output;
    
    is( $edt->status, 'committed', "transaction status is 'committed'" );
    
    $edt = ok_new_edt;
    
    $edt->skip_record({ abc => 1 });
    
    invert_mode(1);
    
    ok_failed_commit "invert: ok_failed_commit";
    
    ok_output( qr/'committed'/, "invert: diag transaction status 'committed'" );
    
    invert_mode(0);
};


subtest 'unsuccessful commit' => sub {
    
    diag_mode(1);
    
    clear_output;
    
    my $edt = ok_new_edt;
    
    $edt->add_condition('E_EXECUTE', "test exception");
    
    ok_failed_commit "ok_failed_commit";
    
    ok_no_output;
    
    is( $edt->status, 'aborted', "transaction status is 'aborted'" );
    
    $edt = ok_new_edt;
    
    $edt->add_condition('E_EXECUTE', "test exception");
    
    invert_mode(1);
    
    ok_commit "invert: ok_commit";
    
    ok_output( qr/'aborted'/, "invert: diag transaction status 'aborted'" );
    
    invert_mode(0);
};


subtest 'successfull rollback' => sub {
    
    diag_mode(1);
    
    clear_output;
    
    my $edt = ok_new_edt;
    
    ok_rollback "ok_rollback";
    
    ok_no_output;
    
    is( $edt->status, 'aborted', "transaction status is 'aborted'" );
};


# subtest 'ok_diag bad arguments' => sub {

#     ok_new_edt;
    
#     # Then test bad arguments to ok_diag and is_diag.
    
#     ok_exception( sub { ok_diag(1); }, qr/result/, 
# 		  "ok_diag with fewer than two arguments throws an exception" );
    
#     ok_exception( sub { is_diag("abc", "abc"); }, qr/compare/,
# 		  "is_diag with fewer than three arguments throws an exception" );
# };


