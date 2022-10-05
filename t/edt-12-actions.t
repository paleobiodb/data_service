#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# actions.t : Test the methods for adding new actions, listing them, and
#             retrieving their attributes.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 12;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception ok_new_edt ok_action 
		  ok_has_condition ok_no_conditions);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest');


# Check for required methods.

subtest 'required methods' => sub {
    
    can_ok( 'ETBasicTest', '_test_action', 'action_ref', 'has_action', 
	    'action_operation', 'action_status', 'action_ok' )
		
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check the _test_action method and the basic accessor methods.

subtest '_test_action with insert' => sub {

    my $edt = ok_new_edt( table => 'EDT_TEST' );
    
    my $record = { string_req => 'test' };
    
    if ( ok_eval( sub { $edt->_test_action('insert', $record); },
		  "_test_action executed successfully with 'insert'" ) )
    {
	ok( $edt->action_ok, "action_ok" );
	
	is( $edt->action_status, 'pending', "action_status" );
	
	is( ref $edt->action_ref, 'EditTransaction::Action', "action_ref" );
	
	is( $edt->action_operation, 'insert', "action_operation" );
	
	can_ok( $edt, 'action_table' ) && 
	    is( $edt->action_table, 'EDT_TEST', "action_table returns default value" );
	
	can_ok( $edt, 'action_params' ) &&
	    is( $edt->action_params, $record, "action_params" );
	
	if ( can_ok( $edt, 'action_parameter' ) )
	{
	    is( $edt->action_parameter('string_req'), 'test', "action_parameter" );
	    is( $edt->action_parameter('&_', 'string_req'), 'test', "action_parameter with '&_'" );
	    is( $edt->action_parameter('&#1', 'string_req'), 'test', "action_parameter with refstring" );
	}
	
	can_ok( $edt, 'action_keyval' ) &&
	    is( $edt->action_keyval, '', "action_keyval" );
    }
    
    else
    {
	BAIL_OUT "_test_action failed";
    }
};


# Check that _test_action works properly with the 'delete' operation and accepts
# the different ways of specifying key values.

subtest '_test_action with delete'  => sub {
    
    my $edt = ok_new_edt( table => 'EDT_TEST' );
    
    ok_eval( sub { $edt->_test_action('delete', "18"); },
	     "_test_action executed successfully with 'delete' and a single key value" );
    
    ok_eval( sub { $edt->_test_action('delete', [3, 4, 5]); },
	     "_test_action executed successfully with 'delete' and a list of key values" );
    
    ok_eval( sub { $edt->_test_action('delete', { test_no => "458" }); },
	     "_test_action executed successfully with 'delete', 'test_no', single" );
    
    ok_eval( sub { $edt->_test_action('delete', { test_no => ["12521", "58002", "77000003"] }); },
	     "_test_action executed successfully with 'delete', 'test_no', 'list" );
    
    ok_eval( sub { $edt->_test_action('delete', { _primary => "23521" }); },
	     "_test_action executed successfully with 'delete', '_primary', single" );
    
    ok_eval( sub { $edt->_test_action('delete', { _primary => ["12522", "8207"] }); },
	     "_test_action executed successfully with 'delete', '_primary', list" );
    
    ok_eval( sub { $edt->_test_action('delete_cleanup', 'EDT_SUB', "18"); },
	     "_test_action executed successfully with 'delete_cleanup'" );
    
    ok_eval( sub { $edt->_test_action('delete_cleanup', 'EDT_SUB', ["18", 19]); },
	     "_test_action executed successfully with 'delete_cleanup', list" );
};


# Check that _test_action works properly with other operations as well.

subtest 'other actions' => sub {
    
    my $edt = ok_new_edt( table => 'EDT_TEST' );
    
    ok_eval( sub { $edt->_test_action('update', { test_no => 74, abc => 1}); }, 
		  "_test_action executed successfully with 'update'" );
    
    ok_eval( sub { $edt->_test_action('replace', { abc => 1}); }, 
	     "_test_action executed successfully with 'replace'" );
    
    ok_eval( sub { $edt->_test_action('other', { abc => 1}); }, 
	     "_test_action executed successfully with 'other'" );
};


# Check _test_action without a default table.

subtest 'default table' => sub {
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { $edt->_test_action('insert', { abc => 1 }); },
		  qr/table/i, "_test_action exception with no table argument and no default" );
    
    ok_eval( sub { $edt->_test_action('insert', 'EDT_TEST', { abc => 1 }); },
		  "_test_action succeeds when table is specified" ) || return;
	
    if ( can_ok( $edt, 'action_table' ) )
    {
	is( $edt->action_table, 'EDT_TEST', "table defaults to proper value" );
	
	$edt = ok_new_edt( table => 'EDT_TEST', "create new edt with default table" );;
	
	$edt->_test_action('insert', 'EDT_TYPES', { abc => 1 });
	
	is( $edt->action_table, 'EDT_TYPES', "table override succeeds" );
    }
};


# Check the response to bad arguments passed to the action attribute getters.

subtest 'action methods bad arguments' => sub {
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { $edt->action_ref({ }) }, qr/action reference/,
		  "exception action_ref not EditTransaction::Action" );
    
    ok_exception( sub { $edt->action_parameter }, qr/parameter name/,
		  "exception action_ref no arguments" );
    
    ok_exception( sub { $edt->action_parameter('&_') }, qr/parameter name/,
		  "exception action_ref one argument" );
    
    $edt->_test_action('insert', 'EDT_TEST', { def => 3 });
    
    my $action1 = $edt->action_ref;
    
    is( ref $action1, "EditTransaction::Action", "got Perl reference to action" );
    
    is( $edt->action_operation($action1), 'insert',
	"action_operation with Perl reference" );
    
    my $edt2 = ok_new_edt;
    
    is( $edt2->action_operation($action1), undef, 
	"action_operation undef with action from different transaction" );
};

# Check the response to bad arguments other than lack of a default table.

subtest 'create action bad arguments' => sub {
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { $edt->_test_action(); }, 1, 
		  "_test_action exception if no arguments given" );
    
    ok_exception( sub { $edt->_test_action('insert', 'EDT_TEST'); }, 
		  qr/parameters|record/, "_test_action exception if no parameters given" );
    
    ok_exception( sub { $edt->_test_action('delete', 'EDT_TEST') },
		  qr/key.*values/, "_test_action exception with 'delete' and no key values" );
    
    ok_exception( sub { $edt->_test_action('insert', '', { abc => 1 }); },
		  qr/table/, "_test_action exception with empty table argument" );
    
    ok_eval( sub { $edt->_test_action('insert', 'NOT_A_TABLE_QQQZ', { abc => 1 }); },
	     "_test_action succeeds with unknown table" );
    
    ok_has_condition('E_BAD_TABLE', "E_BAD_TABLE _test_action with unknown table" );
    
    ok_eval( sub { $edt->_test_action('skip', 'NOT_A_TABLE_QQQZ', { abc => 1 }); },
	     "_test_action succeeds with 'skip' and invalid table" );
    
    ok_no_conditions('E_BAD_TABLE', "no E_BAD_TABLE _test_action with 'skip'" );
    
    ok_exception( sub { $edt->_test_action('insrt', 'EDT_TEST', { abc => 1 }); },
		  qr/operation/, "_test_action exception with unknown operation" );
};


subtest 'create action bad arguments with default table' => sub {
    
    my $edt = ok_new_edt( table => 'EDT_TEST' );
    
    ok_exception( sub { $edt->_test_action(); }, 1, 
		  "_test_action exception with default table and no arguments given" );
    
    ok_exception( sub { $edt->_test_action('insert'); }, qr/parameters|record/,
		  "_test_action exception with default table and no parameters given" );
    
    ok_exception( sub { $edt->_test_action('delete') }, qr/key.*values/, 
		  "_test_action exception with 'delete', default table, no key values" );
    
    ok_eval( sub { $edt->_test_action('insert', '', { abc => 1 }); },
	     "_test_action succeeds with empty table argument and default table" );
    
    ok_eval( sub { $edt->_test_action('insert', 'NOT_A_TABLE_QQQZ', { abc => 1 }); },
	     "_test_action succeeds with invalid table and default table" );
    
    ok_has_condition('E_BAD_TABLE', "E_BAD_TABLE _test_action with unknown table" );
    
    ok_exception( sub { $edt->_test_action('insrt', { abc => 1 }); }, qr/operation/, 
		  "_test_action exception with unknown operation and default table" );
};


# Check the response when an attempt is made to create an action a completed transaction.

subtest 'create action after completion' => sub {
    
    my $edt = ok_new_edt( table => 'EDT_TEST' );
    
    if ( can_ok($edt, 'rollback') )
    {
	$edt->rollback;
	
	ok_exception( sub { $edt->_test_action('insert', { abc => 1 }) },
		      qr/finished|completed/, "_test_action exception after rollback" );
    }
    
    $edt = ok_new_edt( table => 'EDT_TEST' );
    
    if ( can_ok($edt, 'commit') )
    {
	$edt->commit;
	
	ok_exception( sub { $edt->_test_action('insert', { abc => 1 }) },
		      qr/finished|completed/, "_test_action exception after commit" );
    }
};


# Check that action labels work properly.

subtest 'action labels' => sub {
    
    my $edt = ok_new_edt( table => 'EDT_TEST' );
    
    if ( can_ok($edt, 'has_action', 'action_ref') )
    {
	my $action1 = $edt->_test_action('insert', { abc => 3, _label => 'foo' });
	my $action2 = $edt->_test_action('update', { def => 4 });
	
	ok( $edt->has_action('&foo'), "action label &foo" );
	ok( $edt->has_action('&#1'), "action label &#1" );
	ok( $edt->has_action('&#2'), "action label &#2" );
	ok( ! $edt->has_action('&#3'), "no action label &#3" );
	ok( ! $edt->has_action('&bar'), "no action label &bar" );
	
	is( $edt->action_ref('&foo'), $action1, "action internal 1" );
	is( $edt->action_ref('&#1'), $action1, "action internal 1a" );
	
	my $strange = "  3 ë\n,x ";
	
	$edt->_test_action('insert', { foo => 2, _label => $strange } );
	
	ok( $edt->has_action("&$strange"), "action found with strange label" );
    }
};


# Check the method for listing actions once they have been created.

subtest 'action list' => sub {
    
    my $edt = ok_new_edt( table => 'EDT_TEST' );
    
    if ( can_ok($edt, 'actions') )
    {
	$edt->_test_action('insert', { abc => 1, def => 1 });
    
	$edt->_test_action('update', { ghi => 1, _label => 'foo' });
    
	my @actions = $edt->actions;
    
	is( scalar @actions, 2, "query retrieved 2 actions" );
    
	is( $actions[0]{status}, 'pending', "action 1 status" );
	is( $actions[1]{status}, 'pending', "action 2 status" );
	is( $actions[0]{table}, 'EDT_TEST', "action 1 table" );
	is( $actions[1]{table}, 'EDT_TEST', "action 2 table" );
	is( $actions[0]{operation}, 'insert', "action 1 operation" );
	is( $actions[1]{operation}, 'update', "action 2 operation" );
	is( $actions[0]{params}{abc}, 1, "action 1 params" );
	is( $actions[1]{params}{ghi}, 1, "action 2 params" );
	is( $actions[0]{refstring}, "&#1", "action 1 refstring" );
	is( $actions[1]{refstring}, "&foo", "action 2 refstring" );
    
	is( scalar $edt->actions('pending'), 2, "query for 'pending' retrieved 2 actions" );
	is( scalar $edt->actions('all'), 2, "query for 'pending' retrieved 2 actions" );
	is( scalar $edt->actions('completed'), 0, "query for 'completed' retrieved no actions" );
    
	ok_exception( sub { $edt->actions('foobar'); }, qr/unknown|invalid/, "unknown selector" );
    }
    
    diag("TO DO: check action selectors");
};


# Check the action_ref method for obtaining and validating action references.

subtest 'action references' => sub {
    
    my $edt = ok_new_edt( table => 'EDT_TEST' );
    
    can_ok($edt, 'action_ref') || return;
    
    my $action1 = $edt->_test_action('insert', { abc => 1, def => 1 });
    
    my $action2 = $edt->_test_action('update', { ghi => 1, _label => 'foo' });
    
    is( $edt->action_ref('&#1'), $action1, "action_ref with &#1" );
    is( $edt->action_ref($action1), $action1, "action_ref with perl reference" );
    is( $edt->action_ref('&foo'), $action2, "action_ref with label refstring" );
    is( $edt->action_ref('&_'), $action2, "action_ref with '&_'");
    is( $edt->action_ref, $action2, "action_ref no arg" );
    
    is( $edt->action_ref('foobar'), undef, "action_ref not found" );
    is( $edt->action_ref(''), undef, "action_ref empty" );
    
    ok_exception( sub { $edt->action_ref([1, 2, 3]); }, qr/not an action ref/,
		      "action_ref bad reference" );
    
};



