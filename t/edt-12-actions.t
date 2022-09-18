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
use Test::More tests => 11;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest');


# Check that the necessary methods are defined.

subtest 'methods' => sub {
    
    ok( ETBasicTest->can('new_action'), "new_action" ) || 
	BAIL_OUT "missing method 'new_action'";
    
    ok( ETBasicTest->can('has_action'), "has_action" ) || 
	BAIL_OUT "missing method 'has_action'";
    
    ok( ETBasicTest->can('action_ref'), "action_ref" ) || 
	BAIL_OUT "missing method 'action_ref'";
    
    ok( ETBasicTest->can('action_status'), "action_status" ) || 
	BAIL_OUT "missing method 'action_status'";
    
    ok( ETBasicTest->can('action_table'), "action_table" ) || 
	BAIL_OUT "missing method 'action_table'";
    
    ok( ETBasicTest->can('action_operation'), "action_operation" ) || 
	BAIL_OUT "missing method 'action_operation'";
    
    ok( ETBasicTest->can('action_ok'), "action_ok" ) || 
	BAIL_OUT "missing method 'action_ok'";
    
    ok( ETBasicTest->can('action_param'), "action_param" ) || 
	BAIL_OUT "missing method 'action_param'";    
};

# Check the create action routine (new_action) and basic accessor methods.

subtest 'insert action' => sub {

    my $edt = $T->new_edt;
    
    my $record = { string_req => 'test' };
    
    my $action = $edt->new_action('insert', 'EDT_TEST', $record);
    
    ok( $action, "action created" ) || return;
    
    is( $edt->action_ref('&#1'), $action, "action_ref" );
    is( $edt->action_table, 'EDT_TEST', "action_table" );
    is( $edt->action_operation, 'insert', "action_operation" );
    is( $edt->action_param('string_req'), 'test', "action_param" );
    is( $edt->action_status, 'pending', "action_status" );
    
    $T->ok_action;
};


subtest 'delete action' => sub {
    
    my $edt = $T->new_edt;
    
    my $action = $edt->new_action('delete', 'EDT_TEST', "18");
    
    ok( $action, "delete action created with scalar key value" ) || return;
    
    $T->ok_action;
    
    $action = $edt->new_action('delete', 'EDT_TEST', [3, 4, 5] );
    
    ok( $action, "delete action created with list of key values" ) || return;
    
    $T->ok_action;
    
    $action = $edt->new_action('delete', 'EDT_TEST', { _primary => "23521" });
    
    $T->ok_action;
};


subtest 'other actions' => sub {
    
    my $edt = $T->new_edt;
    
    ok( $edt->new_action('update', 'EDT_TEST', { abc => 1}), "update action created" );
    ok( $edt->new_action('replace', 'EDT_TEST', { abc => 1}), "replace action created" );
    ok( $edt->new_action('other', 'EDT_TEST', { abc => 1}), "other action created" );
};


# Check the create action routine with a default table.

subtest 'action with default table' => sub {
    
    my $edt = $T->new_edt(table => 'EDT_TYPES');
    
    my $action = $edt->new_action('insert', { abc => 1 });
    
    ok( $action, "insert action created" ) &&
	is( $edt->action_table, 'EDT_TYPES', "table defaults to proper value" ) &&
	$T->ok_action;
    
    $action = $edt->new_action('replace', 'EDT_TEST', { abc => 1 } );
    
    ok( $action, "action created with different table" ) &&
	is( $edt->action_table, 'EDT_TEST', "action has specified table" ) &&
	$T->ok_action;
    
    ok( $edt->new_action('delete', "12"), "delete action created" ) && $T->ok_action;
    ok( $edt->new_action('update', { abc => 1}), "update action created" ) && $T->ok_action;
    ok( $edt->new_action('replace', { abc => 1}), "replace action created" ) && $T->ok_action;
    ok( $edt->new_action('other', { abc => 1}), "other action created" ) && $T->ok_action;
};


# Check the response to bad arguments

subtest 'create action bad arguments' => sub {
    
    my $edt = $T->new_edt;
    
    my $action;
    
    ok_exception( sub { $action = $edt->new_action(); }, 1, 
		  "no arguments" );
    
    ok_exception( sub { $action = $edt->new_action('insert', 'EDT_TEST'); }, 
		  qr/parameters|record/, "no record argument" );
    
    ok_exception( sub { $action = $edt->new_action('delete', 'EDT_TEST') },
		  qr/key.*values/, "delete with no key values" );
    
    ok_exception( sub { $action = $edt->new_action('insert', { abc => 1 }); },
		  qr/table/, "no table argument" );
    
    ok_exception( sub { $action = $edt->new_action('insert', '', { abc => 1 }); },
		  qr/table/, "empty table argument" );
    
    ok_exception( sub { $action = $edt->new_action('insert', 'NOT_A_TABLE_QQQZ',
						   { string_req => 'abc' }); },
		  qr/unknown/, "unknown table argument" );
    
    ok_exception( sub { $action = $edt->new_action('xxyy', 'EDT_TEST', { abc => 1 }); },
		  qr/operation/, "unknown operation argument" );
};


subtest 'create action bad arguments with default table' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    my $action;
    
    ok_exception( sub { $action = $edt->new_action(); }, 1, 
		  "no arguments" );
    
    ok_exception( sub { $action = $edt->new_action('insert'); }, 
		  qr/parameters|record/, "no record argument" );
    
    ok_exception( sub { $action = $edt->new_action('delete') },
		  qr/key.*values/, "delete with no key values" );
    
    ok_eval( sub { $action = $edt->new_action('insert', '', { abc => 1 }); },
	     "empty table argument overridden by default" );
    
    ok_exception( sub { $action = $edt->new_action('insert', 'NOT_A_TABLE_QQQZ',
						   { string_req => 'abc' }); },
		  qr/unknown/, "unknown table argument" );
    
    ok_exception( sub { $action = $edt->new_action('xxyy', { abc => 1 }); },
		  qr/operation/, "unknown operation argument" );
    
};


# Check the response when an attempt is made to create an action a completed transaction.

subtest 'create action after completion' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    my $action;
    
    $edt->rollback;
    
    ok_exception( sub { $action = $edt->new_action('insert', { abc => 1 }) },
		  qr/finished|completed/, "new action after rollback" );
    
    $edt = $T->new_edt( table => 'EDT_TEST' );
    
    $edt->commit;
    
    ok_exception( sub { $action = $edt->new_action('insert', { abc => 1 }) },
		  qr/finished|completed/, "new action after commit" );
};



# Check that action labels work properly.

subtest 'action labels' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    my $action1 = $edt->new_action('insert', { abc => 3, _label => 'foo' });
    my $action2 = $edt->new_action('update', { def => 4 });
    
    ok( $edt->has_action('&foo'), "action label &foo" );
    ok( $edt->has_action('&#1'), "action label &#1" );
    ok( $edt->has_action('&#2'), "action label &#2" );
    ok( ! $edt->has_action('&#3'), "no action label &#3" );
    ok( ! $edt->has_action('&bar'), "no action label &bar" );
    
    is( $edt->action_ref('&foo'), $action1, "action internal 1" );
    is( $edt->action_ref('&#1'), $action1, "action internal 1a" );
    
    my $strange = "  3 ë\n,x ";
    
    my $action3 = $edt->new_action('insert', { foo => 2, _label => $strange } );
    
    ok( $action3, "action created with strange label" ) && 
	ok( $edt->has_action("&$strange"), "action found with strange label" );
};


# Check the method for listing actions once they have been created.

subtest 'action list' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    my $action1 = $edt->new_action('insert', { abc => 1, def => 1 });
    ok( $action1, "new action 1" ) || return;
    
    my $action2 = $edt->new_action('update', { ghi => 1, _label => 'foo' });
    ok( $action2, "new action 2" ) || return;
    
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
    
    diag("subsequent test should check action selectors");
};


# Check the action_ref method for obtaining and validating action references.

subtest 'action references' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    my $action1 = $edt->new_action('insert', { abc => 1, def => 1 });
    ok( $action1, "new action 1" ) || return;
    
    my $action2 = $edt->new_action('update', { ghi => 1, _label => 'foo' });
    ok( $action2, "new action 2" ) || return;
    
    is( $edt->action_ref('&#1'), $action1, "action_ref 1" );
    is( $edt->action_ref($action1), $action1, "action_ref 1a" );
    is( $edt->action_ref('&foo'), $action2, "action_ref 2" );
    
    is( $edt->action_ref('foobar'), undef, "action_ref not found" );
    is( $edt->action_ref(''), undef, "action_ref empty" );
    
    ok_exception( sub { $edt->action_ref([1, 2, 3]); }, qr/not an action ref/,
		  "action_ref bad reference" );
    
    is( $edt->action_ref, $action2, "action_ref no arg" );
    is( $edt->action_ref('latest'), $action2, "action_ref latest" );
};


# The other methods for query action attributes all depend on action_ref.

subtest 'action attributes' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    is( $edt->current_action, '', "current_action before any actions" );
    
    my $action1 = $edt->new_action('insert', { test_no => "15", abc => "def" });
    ok( $action1, "new action 1" ) || return;
    
    my $action2 = $edt->new_action('update', 'EDT_TYPES', 
				   { test_no => "15", ghi => "a", _label => "foo" });
    ok( $action2, "new action 2" ) || return;
    
    is( $edt->action_status, 'pending', "action_status no argument" );
    is( $edt->action_ok, 1, "action_ok no argument" );
    is( $edt->action_keyval, "15", "action_keyval no argument" );
    is( $edt->action_keyvalues, 1, "action_keyvalues no argument" );
    is( $edt->action_keymult, '', "action_keymult no argument" );
    is( $edt->action_table, 'EDT_TYPES', "action_table no argument" );
    is( $edt->action_operation, 'update', "action_operation no argument" );
    is( $edt->action_param('latest', 'ghi'), "a", "action_param with 'latest'" );
    
    is( $edt->current_action, '&foo', "current_action" );
    
    is( $edt->action_table('latest'), 'EDT_TYPES', "action_table with 'latest'" );
    
    is( $edt->action_table($action1), 'EDT_TEST', "action_table with object ref" );
    is( $edt->action_table("&#1"), 'EDT_TEST', "action_table with refstring" );
    is( $edt->action_table("&foo"), 'EDT_TYPES', "action_table with refstring 2" );
    
    is( $edt->action_operation($action2), 'update', "action_operation with object ref" );
};


