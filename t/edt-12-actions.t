#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# allowances.t : Test the methods and arguments for registering, setting and querying allowances.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 7;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest');



# Check the create action routine (new_action) and basic accessor methods.

subtest 'new action' => sub {

    my $edt = $T->new_edt;
    
    my $record = { string_req => 'test' };
    
    my $action = $edt->new_action('insert', 'EDT_TEST', $record);
    
    ok( $action, "action created" ) || return;
    
    is( $action->refstring, '&#1', "action refstring" );
    is( $action->label, '#1', "action label" );
    is( $action->table, 'EDT_TEST', "action table" );
    is( $action->operation, 'insert', "action operation" );
    is( $action->status, '', "action status" );
    ok( ! $action->parent, "action parent" );
    ok( ! $action->has_errors, "action has no errors" );
    ok( ! $action->has_warnings, "action has no warnings" );
    ok( $action->can_proceed, "action can proceed" );
    ok( ! $action->has_completed, "action has not completed" );
    ok( ! $action->has_executed, "action has not executed" );
    
    is( $action->record, $record, "action record" );
    is( $action->record_value('string_req'), 'test', "action record value" );
};


subtest 'new delete action' => sub {
    
    my $edt = $T->new_edt;
    
    my $action = $edt->new_action('delete', 'EDT_TEST', "18");
    
    ok( $action, "delete action created with scalar key value" ) || return;
    is( $action->keyval, "18", "key recognized" );
    
    $action = $edt->new_action('delete', 'EDT_TEST', [3, 4, 5] );
    
    ok( $action, "delete action created with list of key values" ) || return;
    is( ref $action->keyval, 'ARRAY', "key list recognized" ) &&
    is( scalar $action->keyval->@*, '3', "key list has three elements" );
    
    $action = $edt->new_action('delete', 'EDT_TEST', { _primary => "23521" });
    
    ok( $action, "delete action created with parameter hash" ) &&
    is( $action->keyval, "23521", "scalar key value recognized" );
};


subtest 'other actions' => sub {
    
    my $edt = $T->new_edt;
    
    ok( $edt->new_action('update', 'EDT_TEST', { abc => 1}), "update action created" );
    ok( $edt->new_action('replace', 'EDT_TEST', { abc => 1}), "replace action created" );
    ok( $edt->new_action('other', 'EDT_TEST', { abc => 1}), "other action created" );
};


# Check the create action routine with a default table.

subtest 'create action with default table' => sub {
    
    my $edt = $T->new_edt(table => 'EDT_TYPES');
    
    my $action = $edt->new_action('insert', { abc => 1 });
    
    ok( $action, "action created" );
    is( $action->table, 'EDT_TYPES', "default table" );
    
    $action = $edt->new_action('replace', 'EDT_TEST', { abc => 1 } );
    
    ok( $action, "action with different table" ) &&
    is( $action->table, 'EDT_TEST', "action has specified table" );
    
    $action = $edt->new_action('delete', "12,34") || return;
    
    ok( $action, "delete action created with scalar key value" );
    is( $action->table, 'EDT_TYPES', "delete action default table" );
    is( ref $action->keyval, 'ARRAY', "delete action key recognized" ) &&
    is( scalar $action->keyval->@*, '2', "delete action key multiplicity" );
    
    ok( $edt->new_action('update', { abc => 1}), "update action created" );
    ok( $edt->new_action('replace', { abc => 1}), "replace action created" );
    ok( $edt->new_action('other', { abc => 1}), "other action created" );
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
    
    ok_exception( sub { $action = $edt->new_action('xxx', 'EDT_TEST', { abc => 1 }); },
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
    
    ok_exception( sub { $action = $edt->new_action('xxx', { abc => 1 }); },
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


