#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
#
# edt-28-child-actions.t : Test child actions, which are created by 'add_child_action'
#                          from subclass methods such as validate_action, before_action,
#                          and after_action.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 7;

use ETBasicTest;
use EditTester qw(ok_eval ok_exception ok_new_edt ok_commit ok_failed_commit
		  ok_no_errors ok_has_condition clear_table sql_command
		  ok_found_record ok_no_record);


# Establish an EditTester instance.

my $T = EditTester->new('ETBasicTest');


# Wrap the methods validate_action, before_action, and after_action so that a test can
# make any of them add a child action. If %CHILD_HOOK has an entry for the method, it is
# called for every action whose record includes '_child' and which is not itself a child.

our (%CHILD_HOOK);

{
    no warnings 'redefine';
    no strict 'refs';
    
    foreach my $hook ( qw(validate_action before_action after_action) )
    {
	my $orig = ETBasicTest->can($hook);
	
	*{"ETBasicTest::$hook"} = sub {
	    
	    my @result = $orig->(@_);
	    my ($edt, $action) = @_;
	    
	    if ( my $code = $CHILD_HOOK{$hook} )
	    {
		$code->($edt, $action) if ref $action->record eq 'HASH' &&
		    $action->record->{_child} && ! $action->parent;
	    }
	    
	    return wantarray ? @result : $result[0];
	};
    }
}


# Clear the test tables and add a record to EDT_TEST that child records in EDT_SUB can
# link to.

sub reset_tables {
    
    clear_table('EDT_TEST');
    clear_table('EDT_SUB');
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req) VALUES (5, 'five')");
}


# Check that the necessary methods are defined.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'add_child_action', 'action_parent', 'abort_action')
	
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check a child action added from validate_action, before the parent is executed.

subtest 'child action from validate_action' => sub {
    
    reset_tables();
    
    my $child_ref;
    
    local $CHILD_HOOK{validate_action} = sub {
	my ($edt, $action) = @_;
	$child_ref = $edt->add_child_action('insert', 'EDT_SUB', { name => 'child v', test_no => 5 });
    };
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'parent v', _child => 1 });
    
    is( $child_ref, '&#1-1', "add_child_action returns a reference based on the parent label" );
    
    ok_no_errors('all');
    
    ok_commit;
    
    ok_found_record('EDT_TEST', "string_req = 'parent v'", "parent record was stored");
    ok_found_record('EDT_SUB', "name = 'child v'", "child record was stored");
    
    is( $edt->exec_count, 2, "both actions were executed" );
    
    my @labels = map { $_->label } $edt->{action_list}->@*;
    
    is_deeply( \@labels, [ '#1', '#1-1' ], "child action follows its parent in the action list" );
    
    ok( $edt->has_action($child_ref), "child action can be found by its reference" );
    
    is( $edt->action_parent($child_ref), $edt->action_ref('&#1'),
	"action_parent returns the parent action" );
};


# Check that an error in a child action blocks its parent and the transaction.

subtest 'child action errors' => sub {
    
    reset_tables();
    
    local $CHILD_HOOK{validate_action} = sub {
	my ($edt, $action) = @_;
	$edt->add_child_action('insert', 'EDT_SUB', { name => 'bad child', test_no => 5,
						      aux_no => 'not a key' });
    };
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'parent of bad child', _child => 1 });
    
    ok_has_condition('all', 'E_BAD_KEY', "error in the child action is reported");
    
    ok( $edt->action_ref('&#1')->has_errors, "parent action counts the child's error" );
    
    ok_failed_commit("transaction with a child error fails");
    
    is( $edt->action_status('&#1'), 'failed', "parent action has failed" );
    
    ok_no_record('EDT_TEST', "string_req = 'parent of bad child'", "parent record was not stored");
    
    my ($condition) = grep { /E_BAD_KEY/ } $edt->conditions;
    
    like( $condition, qr/\(#1-1\)/, "condition is reported under the child's label" );
};


# Check that aborting a child action removes its errors from the parent.

subtest 'abort a child action' => sub {
    
    reset_tables();
    
    my $result;
    
    local $CHILD_HOOK{validate_action} = sub {
	my ($edt, $action) = @_;
	my $child_ref = $edt->add_child_action('insert', 'EDT_SUB', { name => 'aborted child',
								      test_no => 5,
								      aux_no => 'not a key' });
	$result = $edt->abort_action($child_ref);
    };
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'parent of aborted child', _child => 1 });
    
    ok( $result, "abort_action returns true for the child action" );
    
    ok_commit("transaction with an aborted child commits");
    
    ok_found_record('EDT_TEST', "string_req = 'parent of aborted child'",
		    "parent record was stored");
};


# Check child actions added from before_action and after_action, while the parent is
# being executed.

subtest 'child action during execution' => sub {
    
    foreach my $hook ( 'before_action', 'after_action' )
    {
	reset_tables();
	
	local $CHILD_HOOK{$hook} = sub {
	    my ($edt, $action) = @_;
	    $edt->add_child_action('insert', 'EDT_SUB', { name => "child of $hook", test_no => 5 });
	};
	
	my $edt = ok_new_edt;
	
	$edt->insert_record('EDT_TEST', { string_req => "parent $hook", _child => 1 });
	
	ok_commit;
	
	ok_found_record('EDT_SUB', "name = 'child of $hook'", "child added by $hook was stored");
	
	is( $edt->action_status('&#1-1'), 'executed', "child added by $hook was executed" );
    }
};


# Check that a child action is treated as authorized because its parent is.

subtest 'child action authorization' => sub {
    
    reset_tables();
    
    no warnings 'redefine';
    
    # Deny permission on all existing rows. Inserting into EDT_TEST needs only table
    # permission, but inserting into EDT_SUB needs permission on the linked EDT_TEST row.
    
    local *ETBasicTest::check_row_permission = sub { return ('none', 1) };
    
    local $CHILD_HOOK{validate_action} = sub {
	my ($edt, $action) = @_;
	$edt->add_child_action('insert', 'EDT_SUB', { name => 'authorized child', test_no => 5 });
    };
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_SUB', { name => 'not authorized', test_no => 5 });
    
    ok_has_condition('&_', 'E_PERMISSION', "a direct insert into EDT_SUB is not authorized");
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'authorized parent', _child => 1 });
    
    ok_commit("child of an authorized action does not need its own permission");
    
    ok_found_record('EDT_SUB', "name = 'authorized child'", "child record was stored");
};


# Check the exceptions thrown by add_child_action for bad arguments.

subtest 'add_child_action bad arguments' => sub {
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { $edt->add_child_action('insert', 'EDT_SUB', { name => 'x' }) },
		  qr/no current action/, "exception if there is no current action" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'parent' });
    
    ok_exception( sub { $edt->add_child_action('frob', 'EDT_SUB', { name => 'x' }) },
		  qr/invalid operation/, "exception for an invalid operation" );
    
    ok_exception( sub { $edt->add_child_action('&nosuch', 'insert', 'EDT_SUB', { name => 'x' }) },
		  qr/no matching action/, "exception for an unknown parent reference" );
    
    ok_exception( sub { $edt->add_child_action({ }, 'insert', 'EDT_SUB', { name => 'x' }) },
		  qr/must be of class EditTransaction::Action/,
		  "exception for a parent that is not an action" );
};
