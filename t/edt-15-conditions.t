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
use Test::More tests => 13;  # plan updated by Claude under the guidance of Michael McClennen

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception ok_new_edt ok_has_condition ok_has_one_condition
		  clear_table);  # imports added by Claude under the guidance of Michael McClennen


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest');


# Check that all required methods are present.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'add_condition', 'has_condition', 'conditions') ||
	
	BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


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
    
    ok( $edt->has_condition('&_', 'W_FORMAT'), "has_condition action warning '&_'" );
    
    ok( ! $edt->has_condition('&_', 'W_BAD_FIELD'), "has_condition main warning '&_'" );
    
    ok( $edt->has_condition('&#1', 'W_FORMAT'), "has_condition action warning '&#1'" );
    
    ok( ! $edt->has_condition('&#1', 'W_BAD_FIELD'), "has_condition main warning '&#1'" );
    
    ok( ! $edt->has_condition('&#22', 'W_BAD_FIELD'), "has_condition bad refstring null" );
    
    ok( $edt->has_condition('all', qr/W_FORMAT|W_BAD_.*/), "has_condition with regexp" );
    
    $edt->_test_action('insert', 'EDT_TEST', { string_req => 'ghi' });
    
    $edt->add_condition('D_BAD_KEY');
    
    ok( $edt->has_condition('E_BAD_KEY'), "add_condition D_ => E_" );
    
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
    
    ok_exception( sub { $edt->has_condition( '&_', 'EBAD_FIELD'); },
		  qr/condition code/i, "exception add_condition misspelled code" );
};


subtest 'conditions' => sub {
    
    # This subtest was filled in by Claude under the guidance of Michael McClennen.
    
    clear_table('EDT_TEST');
    
    my $edt = ok_new_edt;
    
    $edt->add_condition('main', 'W_TEST', 'main warning');
    $edt->add_condition('main', 'E_TEST', 'main error');
    $edt->add_condition('main', 'E_TEST', 'main error');
    
    $edt->insert_record('EDT_TEST', { string_req => 'a', signed_val => 'not a number' });
    $edt->insert_record('EDT_TEST', { string_req => 'b', signed_val => 'not a number' });
    $edt->insert_record('EDT_TEST', { string_req => 'c', signed_val => 'not a number', _label => 'lab' });
    
    my @all = $edt->conditions;
    
    is( scalar(@all), 3, "identical conditions are listed once" );
    
    is( $all[0], "W_TEST: TEST WARNING 'main warning'", "main conditions are listed first" );
    
    like( $all[2], qr/^E_FORMAT \(#1, #2, lab\): Field 'signed_val'/,
	  "a condition shared by several actions lists their labels" );
    
    is( scalar($edt->conditions), 5, "in scalar context, each condition is counted" );
    
    is_deeply( [ $edt->conditions('main') ], [ "W_TEST: TEST WARNING 'main warning'",
					       "E_TEST: TEST ERROR 'main error'" ],
	       "selector 'main' lists the transaction's own conditions" );
    
    is_deeply( [ $edt->conditions('&lab') ],
	       [ "E_FORMAT (lab): Field 'signed_val': value must be an integer" ],
	       "an action reference lists that action's conditions" );
    
    is_deeply( [ $edt->conditions('main', 'warnings') ], [ "W_TEST: TEST WARNING 'main warning'" ],
	       "selector and type together" );
    
    is_deeply( [ $edt->conditions('errors', 'main') ], [ "E_TEST: TEST ERROR 'main error'" ],
	       "type and selector in the other order" );
    
    is( scalar($edt->errors), 4, "errors in scalar context counts the errors" );
    is( scalar($edt->warnings), 1, "warnings in scalar context counts the warnings" );
    is( scalar($edt->errors('main')), 1, "errors with a selector counts that selector's errors" );
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => "g$_", signed_val => 'not a number' }) foreach 1..22;
    
    like( ($edt->conditions)[0], qr/^E_FORMAT \(22 records, including #1, #2, #3, \.\.\., #22\)/,
	  "a condition shared by more than 20 actions lists a summary of the labels" );
};


subtest 'conditions bad arguments' => sub {
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { $edt->conditions('mainn'); }, qr/valid selector/i,
		  "exception conditions misspelled selector" );
    
    ok_exception( sub { $edt->conditions('main', 'errs'); }, qr/valid selector/i,
		  "exception conditions misspelled type" );
};


subtest 'import_conditions' => sub {
    
    # This subtest was filled in by Claude under the guidance of Michael McClennen.
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'imported',
				      _errwarn => [ [ 'E_FORMAT', 'string_req', 'bad value' ],
						    'W_TEST (5): imported line' ] });
    
    ok_has_condition('&_', 'E_FORMAT', qr/Field 'string_req': bad value/,
		     "a condition given as a list is imported");
    
    ok_has_condition('&_', 'W_TEST', qr/imported line/, "a condition given as a string is imported");
    
    $edt->insert_record('EDT_TEST', { string_req => 'imported', _errwarn => 'not a condition' });
    
    ok_has_one_condition('&_', 'E_BAD_CONDITION', qr/not/, "a string that is not a condition is rejected");
    
    $edt->insert_record('EDT_TEST', { string_req => 'imported', _errwarn => [ { } ] });
    
    ok_has_one_condition('&_', 'E_BAD_CONDITION', qr/Unrecognized data format/,
			 "a value of the wrong type is rejected");
    
    $edt->insert_record('EDT_TEST', { string_req => 'imported',
				      _errwarn => [ [ 'E_UNREGISTERED', 'abc' ],
						    [ 'W_UNREGISTERED', 'def' ] ] });
    
    ok_has_condition('&_', 'E_IMPORTED', qr/E_UNREGISTERED/,
		     "an error with an unregistered code becomes E_IMPORTED");
    
    ok_has_condition('&_', 'W_IMPORTED', qr/W_UNREGISTERED/,
		     "a warning with an unregistered code becomes W_IMPORTED");
    
    $edt->insert_record('EDT_TEST', { string_req => 'imported',
				      _errwarn => [ [ 'D_NOT_FOUND' ] ] });
    
    ok_has_condition('&_', 'E_NOT_FOUND', "an imported demoted error becomes an error");
    
    ok( ! (grep { /^C_/ } $edt->conditions('&_')), "an imported demoted error is not a caution" );
};


subtest 'condition messages' => sub {
    
    # This subtest was filled in by Claude under the guidance of Michael McClennen.
    
    my $edt = ok_new_edt;
    
    is( $edt->condition_message('E_PARAM', 'abc', 'def'), "Field 'abc': def",
	"the first template with all of its parameters is used" );
    
    is( $edt->condition_message('E_PARAM', 'abc'), "abc",
	"a later template is used if parameters are missing" );
    
    is( $edt->condition_message('E_PARAM', 'abc', undef), "abc",
	"trailing undefined parameters are ignored" );
    
    is( $edt->condition_message('E_EXECUTE'), "Unknown", "a template with no parameters" );
    
    is( $edt->condition_message('E_PERMISSION', 'update'), "You do not have permission to update this record",
	"a template selected by the first parameter" );
    
    is( $edt->condition_message('E_PERMISSION', 'frobnicate'), "You do not have permission for this operation",
	"the default template is used for an unknown selector" );
    
    is( $edt->condition_message('W_PARAM', 'abc', 'def'), "abc def",
	"with an empty template, the parameters are joined" );
    
    like( $edt->condition_message('E_NO_SUCH_CODE', 'abc'), qr/No template found for 'E_NO_SUCH_CODE'/,
	  "a code with no template" );
    
    my $long = 'x' x 150;
    
    is( $edt->condition_message('E_PARAM', 'abc', $long), "Field 'abc': " . ('x' x 100) . '...',
	"a long parameter is truncated" );
    
    is( $edt->condition_string('#3', 'E_PARAM', 'abc', 'def'), "E_PARAM (#3): Field 'abc': def",
	"condition_string includes the label" );
    
    is( $edt->condition_nolabel('#3', 'E_PARAM', 'abc', 'def'), "E_PARAM: Field 'abc': def",
	"condition_nolabel omits the label" );
    
    is( $edt->condition_string('#3', 'D_PARAM', 'abc', 'def'), "E_PARAM (#3): Field 'abc': def",
	"a demoted error is reported with its original code" );
};


# Check errors that are demoted to warnings by the allowances PROCEED, NOT_FOUND, and
# NOT_PERMITTED. This subtest was added by Claude under the guidance of Michael McClennen.

subtest 'demoted conditions' => sub {
    
    clear_table('EDT_TEST');
    
    my $edt = ok_new_edt('PROCEED');
    
    $edt->insert_record('EDT_TEST', { string_req => 'a', signed_val => 'not a number' });
    $edt->add_condition('main', 'E_TEST', 'main error');
    
    is( scalar($edt->fatals), 1, "with PROCEED, only the main error is fatal" );
    is( scalar($edt->nonfatals), 1, "with PROCEED, the action error is nonfatal" );
    is( scalar($edt->errors), 2, "errors counts both" );
    
    like( ($edt->nonfatals)[0], qr/^E_FORMAT/, "a demoted error is listed with its original code" );
    
    ok( $edt->action_ref('&#1')->has_errors, "a demoted error still counts as an error for its action" );
    
    $edt = ok_new_edt('NOT_FOUND');
    
    $edt->update_record('EDT_TEST', { test_no => 99, string_req => 'missing' });
    $edt->insert_record('EDT_TEST', { string_req => 'b', signed_val => 'not a number' });
    
    is( scalar($edt->fatals), 1, "with NOT_FOUND, the other error is still fatal" );
    is( scalar($edt->nonfatals), 1, "with NOT_FOUND, E_NOT_FOUND is nonfatal" );
    
    ok( $edt->has_condition('E_NOT_FOUND'), "has_condition finds a demoted E_NOT_FOUND" );
    ok( $edt->has_condition('&#1', 'E_NOT_FOUND'), "has_condition finds it on the action" );
};


# Check has_condition_code. This subtest was added by Claude under the guidance of
# Michael McClennen.

subtest 'has_condition_code' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->add_condition('main', 'E_TEST', 'abc');
    
    ok( eval { $edt->has_condition_code('W_NOTHING', 'E_TEST') }, "has_condition_code finds a code" );
    ok( eval { ! $edt->has_condition_code('W_NOTHING') }, "has_condition_code returns false otherwise" );
};


# Check that add_condition rejects codes that have no template. This subtest was added by
# Claude under the guidance of Michael McClennen.

subtest 'unregistered condition codes' => sub {
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { $edt->add_condition('main', 'E_NEVER_REGISTERED', 'abc') },
		  qr/no template for condition code 'E_NEVER_REGISTERED'/,
		  "an unregistered code is rejected for the transaction" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'unregistered' });
    
    ok_exception( sub { $edt->add_condition('&_', 'W_NEVER_REGISTERED') },
		  qr/no template for condition code 'W_NEVER_REGISTERED'/,
		  "an unregistered code is rejected for an action" );
    
    ok_exception( sub { $edt->add_condition('main', 'D_NEVER_REGISTERED') },
		  qr/no template for condition code 'E_NEVER_REGISTERED'/,
		  "a demoted code is checked under its original code" );
    
    ETBasicTest->register_conditions(E_REGISTERED_LATER => "Registered '&1'");
    
    ok_eval( sub { $edt->add_condition('main', 'E_REGISTERED_LATER', 'abc') },
	     "a code is accepted once it is registered" );
    
    ok_eval( sub { $edt->add_condition('main', 'W_PARAM', 'abc') },
	     "a code with an empty template is accepted" );
    
    $edt = ok_new_edt('BAD_FIELDS');
    
    $edt->insert_record('EDT_TEST', { string_req => 'bad field', no_such_field => 1 });
    
    ok_has_one_condition('&_', 'W_BAD_FIELD', qr/Field 'no_such_field' does not correspond/,
			 "with BAD_FIELDS, an unknown field produces W_BAD_FIELD" );
};


# Check conditions whose message is given literally, by the reserved first parameter
# '_literal_'. Imported conditions given as strings keep their text this way. This subtest
# was added by Claude under the guidance of Michael McClennen.

subtest 'literal conditions' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->add_condition('main', 'W_PARAM', '_literal_', "Field 'x': this text is used as is");
    
    is_deeply( [ $edt->conditions('main') ], [ "W_PARAM: Field 'x': this text is used as is" ],
	       "add_condition with '_literal_' uses the text as the message" );
    
    $edt->add_condition('main', 'W_PARAM', '_literal_', "Field 'x': this text is used as is");
    
    is( scalar($edt->conditions('main')), 1, "an identical literal condition is not added twice" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'imported',
				      _errwarn => [ "W_TRUNC (5): value was shortened: 'abc' to 'ab'",
						    "E_FORMAT: Field 'q' is wrong" ] });
    
    is_deeply( [ $edt->conditions('&_') ],
	       [ "W_TRUNC (#1): value was shortened: 'abc' to 'ab'",
		 "E_FORMAT (#1): Field 'q' is wrong" ],
	       "imported strings keep their text, with the label of the action they belong to" );
    
    ok( $edt->has_condition('W_TRUNC'), "an imported string keeps its code" );
    is( scalar($edt->conditions('&_', 'warnings')), 1, "an imported warning counts as a warning" );
    is( scalar($edt->conditions('&_', 'errors')), 1, "an imported error counts as an error" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'imported',
				      _errwarn => [ "E_SOMETHING_UNREGISTERED: some text",
						    "W_SOMETHING_UNREGISTERED: other text" ] });
    
    is_deeply( [ $edt->conditions('&_') ],
	       [ "E_IMPORTED (#2): E_SOMETHING_UNREGISTERED: some text",
		 "W_IMPORTED (#2): W_SOMETHING_UNREGISTERED: other text" ],
	       "an imported string with an unregistered code keeps the code in front of its text" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'imported',
				      _errwarn => [ [ 'E_SOMETHING_UNREGISTERED', 'a', 'b' ] ] });
    
    is_deeply( [ $edt->conditions('&_') ], [ "E_IMPORTED (#3): E_SOMETHING_UNREGISTERED: a b" ],
	       "an imported list with an unregistered code joins its parameters" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'imported',
				      _errwarn => [ [ 'E_FORMAT', 'string_req', 'bad value' ] ] });
    
    is_deeply( [ $edt->conditions('&_') ], [ "E_FORMAT (#4): Field 'string_req': bad value" ],
	       "an imported list with a registered code still uses the template" );
    
    $edt = ok_new_edt('PROCEED');
    
    $edt->insert_record('EDT_TEST', { string_req => 'imported',
				      _errwarn => [ "E_FORMAT: Field 'q' is wrong" ] });
    
    is( scalar($edt->fatals), 0, "with PROCEED, an imported error is demoted" );
    
    is_deeply( [ $edt->nonfatals ], [ "E_FORMAT (#1): Field 'q' is wrong" ],
	       "a demoted literal condition keeps its text and original code" );
    
    $edt = ok_new_edt;
    
    my $errwarn = [ [ 'E_FORMAT', 'string_req', 'bad value' ], [ "W_TRUNC: shortened" ] ];
    
    $edt->insert_record('EDT_TEST', { string_req => 'imported', _errwarn => $errwarn });
    
    is_deeply( [ $edt->conditions('&_') ], [ "E_FORMAT (#1): Field 'string_req': bad value",
					      "W_TRUNC (#1): shortened" ],
	       "a list containing only a condition line is imported as text" );
    
    is_deeply( $errwarn, [ [ 'E_FORMAT', 'string_req', 'bad value' ], [ "W_TRUNC: shortened" ] ],
	       "importing does not change the data that was imported" );
};
