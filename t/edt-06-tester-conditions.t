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
use Test::More tests => 9;

use ETBasicTest;
use ETTrivialClass;

use EditTester qw(ok_new_edt last_edt clear_edt invert_mode diag_mode
		  ok_output ok_no_output clear_output
		  ok_condition_count ok_no_conditions ok_has_condition ok_has_one_condition
		  ok_no_errors ok_no_warnings ok_has_one_error ok_has_one_warning
		  ok_eval ok_exception ok_diag is_diag
		  select_tester current_tester target_class
		  default_table set_default_table);


$DB::single = 1;


# Check that we get the proper exceptions when trying to do tests without
# creating an EditTester and/or EditTransaction.

subtest 'no tester yet' => sub {
    
    ok_exception( sub { ok_has_condition( 'E_FOOBAR' ); }, qr/EditTester/,
		  "no tester yet for ok_has_condition" );
    
    ok_exception( sub { default_table; }, qr/EditTester/,
		  "no tester yet for default_table" );
};


my $T = EditTester->new('ETBasicTest');


subtest 'no edt yet' => sub {
    
    ok_exception( sub { ok_has_condition( 'E_FOOBAR' ); }, qr/EditTransaction/,
		  "no edt yet" );
    
    is( last_edt, undef, "last_edt returns undef" );
};


# Create a new ETBasicTest instance and add some conditions. Check that the test
# methods implemented by EditTester.pm work properly.

subtest 'main conditions' => sub {
    
    my $edt = ok_new_edt();
    
    is( last_edt, $edt, "last_edt returns newly created instance" );
    
    ok_no_conditions( "ok_no_conditions with no arguments" );
    ok_condition_count( 0, "ok_condition_count with 0" );
    ok_no_errors( "ok_no_errors with no arguments" );
    ok_no_warnings( "ok_no_warnings with no arguments" );
    
    $T->ok_no_conditions( "ok_no_conditions called as a method" );
    
    {
	invert_mode(1);
	
	ok_has_condition( "ok_has_condition fails with no conditions" );
	ok_has_one_condition( "ok_has_one_condition fails with no conditions" );
	ok_condition_count( 2, "ok_condition count fails with '2'" );
	
	invert_mode(0);
    }
    
    $edt->add_condition( 'E_EXECUTE', 'foobar' );
    
    ok_has_condition( "ok_has_condition with no arguments" );
    ok_has_condition( 'E_EXECUTE', "ok_has_condition with code" );
    ok_has_condition( qr/foobar$/, "ok_has_condition with regexp" );
    ok_has_condition( 'errors', "ok_has_condition with 'errors'" );
    ok_has_condition( 'fatal', "ok_has_condition with 'fatal'" );
    ok_has_condition( '_', "ok_has_condition with '_'" );
    ok_has_condition( 'main', "ok_has_condition with 'main'" );
    ok_has_condition( 'main', 'errors', "ok_has_condition with 'main' and 'errors'" );
    ok_has_condition( 'errors', 'main', "ok_has_condition with 'errors' and 'main'" );
    ok_no_conditions( 'main', 'warnings', "ok_no_conditions with 'main' and 'warnings" );
    ok_no_conditions( 'warnings', 'main', "ok_no_conditions with 'warnings' and 'main'" );
    
    ok_has_condition( 'E_EXECUTE', qr/foobar/, "ok_has_condition with second filter" );
    $T->ok_has_condition( 'E_EXECUTE', qr/foobar/, "same called as a method" );
    
    ok_has_one_condition( "ok_has_one_condition with no arguments" );
    ok_has_one_condition( 'E_EXECUTE', "ok_has_one_condition with code" );
    
    ok_no_conditions( 'nonfatal', "ok_no_conditions with 'nonfatal'" );
    
    ok_has_one_error( 'E_EXECUTE', "ok_has_one_error with code" );
    ok_no_warnings( "ok_no_warnings with no arguments" );
    
    {
	invert_mode(1);
	
	ok_has_condition( 'warnings', "ok_has_condition properly fails with 'warnings'" );
	ok_has_condition( 'nonfatal', "ok_has_condition properly fails with 'nonfatal'" );
	ok_has_condition( 'E_BAD_TABLE', "ok_has_condition fails with non-matching code" );
	ok_has_condition( qr/xyzzy/, "ok_has_condition fails with non-matching regexp" );
	ok_has_one_warning( "ok_has_one_warning with no arguments" );
	
	ok_has_condition( 'E_EXECUTE', qr/bazz/, "ok_has_condition with second non-matching filter" );
	
	invert_mode(0);
    }
    
    $edt->add_condition( 'E_EXECUTE', "biffbaff" );
    
    ok_has_condition( 'E_EXECUTE', "ok_has_condition, two conditions with same code" );
    ok_has_condition( qr/fba/, "ok_has_condition with no selector defaults to latest" );
    ok_condition_count( 2, "ok_condition_count with '2'" );
    ok_condition_count( 2, 'main', 'errors', "ok_condition_count with '2', 'main', 'errors'" );
    ok_condition_count( 2, 'E_EXECUTE', "ok_condition_count with '2' and code" );
    
    {
	invert_mode(1);
	
	ok_has_one_condition( 'E_EXECUTE', "ok_has_one_condition fails, two with same code" );
	
	invert_mode(0);
    }
    
    $edt->add_condition( 'W_EXECUTE', "foobar" );
    
    ok_has_condition( "ok_has_condition with no arguments" );
    ok_has_condition( 'W_EXECUTE', "ok_has_condition with code" );
    ok_has_condition( qr/foobar$/, "ok_has_condition with regexp" );
    ok_has_condition( 'warnings', "ok_has_condition with 'errors'" );
    ok_has_condition( 'nonfatal', "ok_has_condition with 'fatal'" );
    
    ok_condition_count( 2, qr/foobar/, "ok_condition_count with regexp" );
    
    ok_has_one_condition( 'warnings', 'W_EXECUTE', "ok_has_one_condition with 'warnings' and code" );
    ok_has_one_condition( 'warnings', "ok_has_one_condition with code" );
    ok_has_one_warning( 'W_EXECUTE', "ok_has_one_warning with code" );
    ok_has_one_warning( "ok_has_one_warning with no arguments" );
    
    {
	invert_mode(1);
	
	ok_has_one_condition( 'W_EXECUTE', "ok_has_one_condition fails with multiple conditions" );
	
	invert_mode(0);
    }
    
    clear_edt;
    
    is( last_edt, undef, "last_edt once again returns undef" );
    
    ok_exception( sub { ok_has_condition( 'E_FOOBAR' ); }, qr/EditTransaction/,
		  "no edt yet" );
};


subtest 'action conditions' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->_test_action('insert', 'EDT_TEST', { string_req => "abc" });
    
    $edt->add_condition('E_FORMAT', "string_req", "foobar");
    
    ok_has_one_condition( '_', qr/foobar/, "found condition attached to action" );
    ok_has_one_condition( 'all', "one condition in total" );
    
    ok_no_conditions( 'main', "no main conditions" );
    
    $edt->add_condition('C_LOCKED');
    
    ok_condition_count( 2, "two conditions total" );
    ok_condition_count( 2, '_', "two conditions on latest action" );
    
    invert_mode(1);
    
    ok_has_one_condition( qr/foobar/, "no longer the only condition" );
    
    invert_mode(0);
    
    $edt->_test_action('insert', 'EDT_TEST', { signed_val => 82, string_req => '23' });
    
    $edt->add_condition('E_FORMAT', "signed_val", "xyzzy");
    
    ok_has_condition( 'all', 'E_FORMAT', "ok_has_condition" );
    ok_condition_count( 2, 'all', 'E_FORMAT', "two E_FORMAT conditions in all" );
    ok_no_conditions( 'main', 'E_FORMAT', "none of them on main" );
    
    $edt->add_condition('main', 'C_CREATE');
    
    ok_has_one_condition( 'main', 'C_CREATE', "C_CREATE is the only condition on main" );
    ok_has_one_condition( '_', "just one condition on latest action" );
    ok_has_condition( '&#2', qr/xyzzy/, "found condition by action reference" );
};


subtest 'condition bad arguments' => sub {

    clear_edt;
    
    # Start with bad arguments to ok_has_condition.
    
    ok_exception( sub { ok_has_condition( 'E_FOOBAR' ); }, qr/EditTransaction/,
		  "test call before edt creation throws an exception" );
    
    my $edt = ok_new_edt();
    
    $edt->add_condition('main', 'C_LOCKED', "some message" );
    
    ok_exception( sub { ok_has_condition('main', '&#1', 'C_LOCKED'); },
		  qr/conflicting selector/, "selector conflict throws an exception" );
    
    ok_exception( sub { ok_has_condition('errors', '_', 'nonfatal', 'C_LOCKED'); },
		  qr/conflicting type/, "type conflict throws an exception" );
    
    ok_exception( sub { ok_has_condition('mainn', 'C_LOCKED'); },
		  qr/unknown selector/, "misspelled selector throws an exception" );
    
    ok_exception( sub { ok_has_condition('_', 'errs', 'C_LOCKED'); },
		  qr/unknown selector/, "misspelled type throws an exception" );
    
    clear_output;
    
    invert_mode(1);
    
    ok_has_condition( '&#1', "bad action reference" );
    
    invert_mode(0);
    
    ok_output( qr/action.*[&][#]/, "ok_has_condition diag no matching action");
    
    ok_exception( sub { ok_has_condition( 'errors', 'CLOCKED'); },
		  qr/unrecognized filter/, "misspelled filter throws an exception" );
    
    ok_exception( sub { ok_has_condition( 'errors', [1, 2, 3]); },
		  qr/unrecognized filter/, "non-regex reference throws an exception" );
    
    # A single space is enough for the following argument to be interpreted as a label.
    
    ok_has_condition( "simple " );
    
    # Now test bad arguments to ok_condition_count.
    
    ok_exception( sub { ok_condition_count('errors', "test"); },
		  qr/expected condition/, "missing count throws an exception" );
    
    clear_edt;
    
    ok_exception( sub { ok_condition_count(2, 'errors'); },
		  qr/EditTransaction/, "ok_condition_count with missing edt throws an exception" );
    
    ok_exception( sub { ok_diag(2, "test"); },
		  qr/EditTransaction/, "ok_diag with missing edt throws an exception" );
    
    ok_exception( sub { is_diag(2, 2, "test"); },
		  qr/EditTransaction/, "is_diag with missing edt throws an exception" );
    
};


# The tests 'ok_diag' and 'is_diag' mirror the corresponding tests from
# Test::More, but when they fail they also list all conditions from the latest
# (or specified) EditTransaction that match the specified type, selector, and/or
# filter. If none are given, the selector defaults to '_'.

subtest 'ok_diag and is_diag' => sub {
    
    diag_mode(1);
    
    clear_output;
    
    my $edt = ok_new_edt;
    
    $edt->add_condition('E_EXECUTE');

    my $test_A = 1;
    my $test_B = "abc";
    
    ok_diag( $test_A, "test value true" );
    is_diag( $test_B, "abc", "test value is expected value" );
    
    ok_no_output;
    
    invert_mode(1);
    
    my $test_A1 = undef;
    my $test_B1 = "def";
    
    ok_diag( $test_A1, "test value false" );
    
    ok_output( qr/E_EXECUTE/, "first condition was listed");
    
    $edt->add_condition('C_LOCKED');
    
    is_diag( $test_B1, "abc", "test value is not the expected value" );
    
    ok_output( qr/C_LOCKED/, "second condition was listed" );
    
    clear_output;
    
    $edt->_test_action( 'skip', 'EDT_TEST', { abc => 'def' } );
    
    $edt->add_condition( 'W_PARAM', "foobar" );
    
    ok_diag( $test_A1, "test value false 2" );
    
    ok_output( qr/W_PARAM/, "most recent condition was listed" );
    
    invert_mode(0);
};


subtest 'ok_diag bad arguments' => sub {

    ok_new_edt;
    
    # Then test bad arguments to ok_diag and is_diag.
    
    ok_exception( sub { ok_diag(1); }, qr/result/, 
		  "ok_diag with fewer than two arguments throws an exception" );
    
    ok_exception( sub { is_diag("abc", "abc"); }, qr/compare/,
		  "is_diag with fewer than three arguments throws an exception" );
};


# Test that we can have multiple edts active at once and choose between them. If
# no edt is explicitly specified, it defaults to the last one created.

subtest 'multiple EditTransaction instances' => sub {
    
    my $edt1 = ok_new_edt();
    
    $edt1->add_condition('main', 'E_BAD_REFERENCE', 'foobar');
    
    my $edt2 = ok_new_edt();
    
    $edt2->add_condition('main', 'C_CREATE', 'biffbaff');
    
    ok_has_condition( 'C_CREATE', "ok_has_condition defaults to last edt created" );
    $T->ok_has_condition( 'C_CREATE', "same when called as a method" );
    
    ok_condition_count( 1, 'C_CREATE', "ok_condition_count" );
    
    invert_mode(1);
    
    ok_has_condition( 'E_BAD_REFERENCE', "confirm default to last edt" );
    
    invert_mode(0);
    
    ok_has_condition( $edt1, 'E_BAD_REFERENCE', "accepts explicit edt argument" );
    $T->ok_has_condition( $edt1, 'E_BAD_REFERENCE', "same when called as a method" );
    ok_has_condition( $edt2, 'C_CREATE', "confirm accepts explicit edt argument" );
    
    ok_condition_count( 1, $edt1, 'E_BAD_REFERENCE', "ok_condition_count with count first" );
    ok_condition_count( $edt1, 1, 'E_BAD_REFERENCE', "ok_condition_count with cound second" );
    
    clear_edt;
};


subtest 'multiple EditTester instances' => sub {
    
    my $T2 = EditTester->new('ETTrivialClass', 'EDT_TEST');
    
    is ( last_edt, undef, "no edt yet for second EditTester" );
    
    select_tester($T);
    
    my $edt1 = ok_new_edt();
    
    is ( last_edt, $edt1, "last_edt returned first edt" );
    
    $edt1->add_condition('main', 'C_CREATE');
    
    is ( default_table, undef, "default_table" );
    
    select_tester($T2);
    
    is ( current_tester, $T2, "current_tester" );
    
    is ( default_table, 'EDT_TEST', "default_table 2" );
    
    my $edt2 = ok_new_edt();
    
    $edt2->add_condition('main', 'C_LOCKED');
    
    is ( last_edt, $edt2, "last_edt returned second edt" );
    
    select_tester($T);
    
    is( target_class, 'ETBasicTest', "target_class with no arguments" );
    
    is( $T2->target_class, 'ETTrivialClass', "target_class with one argument" );
    
    is( last_edt, $edt1, "last_edt returned first edt again" );
    
    ok_has_condition( 'C_CREATE', "ok_has_condition used proper edt" );
    
    clear_edt;
    
    is ( last_edt, undef, "last_edt returned undef" );
    
    my $edt3 = ok_new_edt();
    
    $edt3->add_condition('main', 'C_ALTER_TRAIL');
    
    is ( last_edt, $edt3, "last_edt returned third edt" );
    
    select_tester($T2);
    
    is ( last_edt, $edt2, "last_edt returned second edt again" );
    
    is ( default_table, 'EDT_TEST', "default_table returned proper value for second tester" );
    
    set_default_table('EDT_TYPES');
    
    is( default_table, 'EDT_TYPES', "default_table reflects set_default_table" );
    
    ok_has_condition( 'C_LOCKED', "ok_has_condition used proper edt" );
    
    $T->ok_has_condition( 'C_ALTER_TRAIL', "ok_has_condition with tester argument used default edt" );
    
    $T2->ok_has_condition( 'C_LOCKED', "again with second tester" );
    
    $T2->ok_has_condition( $edt1, 'C_CREATE', "explicit edt argument overrides tester default" );
};


