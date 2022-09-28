#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# conditions.t : Test the methods for dealing with error and warning conditions.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 6;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception ok_new_edt);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest');


# Check the method for registering conditions.

subtest 'register_conditions' => sub {
    
    can_ok('ETBasicTest', 'register_conditions', 'is_valid_condition') || return;
    
    ok( ! ETBasicTest->is_valid_condition('W_TESTXYZ'), 
	"baseline: no directive 'W_TESTXYZ'" );
    
    ok_eval( sub { ETBasicTest->register_conditions(W_TESTXYZ => "warning about xyz: $1"); },
	     "register condition with nonempty template" )
	|| return;
    
    ok_eval( sub { ETBasicTest->register_conditions(C_FOOBAR => ""); },
	     "register condition with empty template" );
    
    is( ETBasicTest->register_conditions(C_BIFF => "$1", E_BAFF => "abc"), 2,
	"register_conditions returns number of conditions registered" );
    
    ok( ETBasicTest->is_valid_condition('W_TESTXYZ'), 
	"directive 'W_TESTXYZ' is now registered" );
    
    ok( ! EditTransaction->is_valid_condition('W_TESTXYZ'), 
	"base class does not have new directive" );
    
    if ( can_ok('ETTrivialClass', 'copy_from') )
    {
	ok( ! ETTrivialClass->is_valid_condition('W_TESTXYZ'), 
	    "baseline: other class does not have 'W_TESTXYZ'" );
	
	ETTrivialClass->copy_from('ETBasicTest');
	
	ok( ETTrivialClass->is_valid_condition('W_TESTXYZ'), 
	    "directive 'W_TESTXYZ' copied" );
    }
};


subtest 'register_conditions bad arguments' => sub {
    
    can_ok('ETBasicTest', 'register_conditions', 'is_valid_condition') || return;
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { $edt->register_conditions(foobar => ''); }, qr/class/i,
		  "register_conditions exception when called on an instance" );
    
    ok_exception( sub { ETBasicTest->register_conditions(E_FOOBAR => undef); }, 
		  qr/template/i, 
		  "register_conditions exception with undefined template" );
    
    ok_exception( sub { ETBasicTest->register_conditions('abc:def' => "ghi"); }, 
		  qr/condition code/i,
		  "register_conditions exception when invalid condition name is supplied" );
    
    ok_exception( sub { ETBasicTest->register_conditions('E_FUZZBUFF'); }, 
		  qr/\beven\b|\bodd\b/, 
		  "register_conditions exception with single argument" );
    
    ok_exception( sub { ETBasicTest->register_conditions(E_FUZZBUF => "foo", 'W_BIFFBAFF'); }, 
		  qr/\beven\b|\bodd\b/, 
		  "register_conditions exception with three arguments" );
    
    ok_eval( sub { ETBasicTest->register_conditions('C_--23B' => "23b"); },
	     "condition names can uppercase word characters and hyphens" );
    
    ok_eval( sub { ! $edt->is_valid_condition('abc:def'); },
	     "is_valid_condition returns undef with an invalid name, no exception" );
};


# Check the methods for adding and querying conditions.

subtest 'add_condition, has_condition' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->add_condition('E_EXECUTE', "test condition");
    
    ok( $edt->has_condition('E_EXECUTE'), "has_condition ok immediate" );
    
    $edt->add_condition('E_EXECUTE', "secondary", "condition");
    
    ok( $edt->has_condition('E_EXECUTE', "test condition"), 
	"has_condition with first argument" );
    
    ok( $edt->has_condition('E_EXECUTE', "secondary", "condition"), 
	"has_condition with second arg list" );
    
    ok( ! $edt->has_condition('E_EXECUTE', "foobar"),
	"has_condition with nonexistent argument" );
    
    is( $edt->conditions, 2, "condition count is 2" );
    
    $edt->add_condition('E_EXECUTE', "test condition");
    
    is( $edt->conditions, 2, "duplicate condition not counted" );
    
    $edt->add_condition('W_BAD_FIELD', 'abcd');
    
    my $action1 = $edt->_test_action('insert', 'EDT_TEST', { string_req => 'xyz' });
    
    ok_eval( sub { $edt->add_condition($action1, 'W_FORMAT', 'string_req'); },
	     "add_condition with action perlref" );
    
    ok( $edt->has_condition('W_FORMAT'), "has_condition action warning no selector" );
    
    ok( $edt->has_condition('E_EXECUTE'), "has_condition main error no selector" );
    
    ok( $edt->has_condition('main', 'W_BAD_FIELD'), "has_condition main warning 'main'" );
    
    ok( ! $edt->has_condition('main', 'W_FORMAT'), "has_condition not action warning 'main'" );
    
    ok( $edt->has_condition('all', 'W_BAD_FIELD'), "has_condition main warning 'all'" );
    
    ok( $edt->has_condition('all', 'W_FORMAT'), "has_condition not action warning 'all'" );
    
    ok( $edt->has_condition('_', 'W_FORMAT'), "has_condition action warning '_'" );
    
    ok( ! $edt->has_condition('_', 'W_BAD_FIELD'), "has_condition main warning '_'" );
    
    ok( $edt->has_condition('&#1', 'W_FORMAT'), "has_condition action warning '&#1'" );
    
    ok( ! $edt->has_condition('&#1', 'W_BAD_FIELD'), "has_condition main warning '&#1'" );
    
    ok( ! $edt->has_condition('&#22', 'W_BAD_FIELD'), "has_condition bad refstring null" );
    
    ok( $edt->has_condition('all', qr/W_FORMAT|W_BAD_.*/), "has_condition with regexp" );
    
    $edt->_test_action('insert', 'EDT_TEST', { string_req => 'ghi' });
    
    $edt->add_condition('F_BAD_KEY');
    
    ok( $edt->has_condition('E_BAD_KEY'), "add_condition F_ => E_" );
    
};


subtest 'add_condition, has_condition bad arguments' => sub {
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { $edt->add_condition; }, qr/condition code/i,
		  "exception add_condition no args" );
    
    ok_exception( sub { $edt->add_condition('foobar'); }, qr/condition code/i,
		  "exception add_condition invalid arg" );
    
    ok_exception( sub { $edt->add_condition('main'); }, qr/condition code/i,
		  "exception add_condition selector no code" );
    
    ok_exception( sub { $edt->add_condition('&#8', 'E_BAD_FIELD'); },
		  qr/no matching action/i, "exception add_condition bad refstring" );
    
    ok_exception( sub { $edt->add_condition([ 3, 4, 5], 'E_BAD_FIELD'); },
		  qr/not an action/i, "exception add_condition bad perlref" );
    
    ok_exception( sub { $edt->has_condition( 'mainn', 'E_BAD_FIELD'); },
		  qr/condition code/i, "exception add_condition misspelled selector" );
    
    ok_exception( sub { $edt->has_condition( '_', 'EBAD_FIELD'); },
		  qr/condition code/i, "exception add_condition misspelled code" );
};


subtest 'conditions' => sub {
    
    pass('placeholder');
};


subtest 'conditions bad arguments' => sub {
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { $edt->conditions('mainn'); }, qr/valid selector/i,
		  "exception conditions misspelled selector" );
    
    ok_exception( sub { $edt->conditions('main', 'errs'); }, qr/valid selector/i,
		  "exception conditions misspelled type" );
};
