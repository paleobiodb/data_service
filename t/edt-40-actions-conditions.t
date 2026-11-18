#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
#
# edt-40-actions-conditions.t : Test the parts of EditTransaction::IActions and
#                               EditTransaction::Conditions that are not covered elsewhere:
#                               aborting actions, child actions with a named parent, the
#                               'actions' method, condition counts and lists, condition
#                               templates, and importing conditions in bad formats.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 9;

use ETBasicTest;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_no_conditions ok_no_errors
		  ok_has_condition ok_has_one_condition clear_table sql_command
		  ok_found_record ok_no_record);


# Establish an EditTester instance.

my $T = EditTester->new('ETBasicTest');


# Clear the test tables and add a record with a known key.

sub reset_tables {
    
    clear_table('EDT_TEST');
    clear_table('EDT_SUB');
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req) VALUES (5, 'five')");
}


# Check that the necessary methods are defined.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'abort_action', 'add_child_action', 'actions', 'import_conditions',
	   'condition_nolabel', 'errors', 'warnings', 'fatals', 'nonfatals')
	
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check abort_action in the cases not covered by edt-27 and edt-28.

subtest 'abort_action' => sub {
    
    reset_tables();
    
    # Under PROCEED, the error on an aborted action has been demoted. Aborting the action
    # removes it from the count.
    
    my $edt = ok_new_edt('PROCEED');
    
    $edt->insert_record('EDT_TEST', { string_req => 'bad', signed_val => 'not a number' });
    
    is( $edt->errors + 0, 1, "the demoted error is counted" );
    
    ok( $edt->abort_action, "abort_action returns true" );
    
    is( $edt->errors + 0, 0, "the demoted error is no longer counted" );
    
    ok( $edt->abort_action, "aborting the same action again returns true" );
    
    ok( ! defined $edt->abort_action('&nosuch'), "abort_action returns undef for an unknown reference" );
    
    # An action that has already been executed cannot be aborted.
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'executed' });
    
    ok_commit;
    
    ok( ! $edt->abort_action('&#1'), "an executed action cannot be aborted" );
};


# Check add_child_action with a parent given by reference, and an exception while the
# child is being validated.

subtest 'add_child_action' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { _label => 'p', string_req => 'parent' });
    
    my $child = $edt->add_child_action('&p', 'insert', 'EDT_SUB', { name => 'named parent', test_no => 5 });
    
    is( $child, '&p-1', "a child can be added to a parent given by reference" );
    
    is( $edt->action_parent($child), $edt->action_ref('&p'), "the child belongs to that parent" );
    
    {
	no warnings 'redefine';
	
	my $orig = ETBasicTest->can('validate_action');
	
	local *ETBasicTest::validate_action = sub {
	    die "child validation exception\n" if $_[1]->parent;
	    return $orig->(@_);
	};
	
	$edt->add_child_action('&p', 'insert', 'EDT_SUB', { name => 'exception', test_no => 5 });
    }
    
    ok_has_one_condition('&p-2', 'E_EXECUTE', qr/validation of a child action/,
			 "an exception during validation of the child is reported");
    
    ok_failed_commit("the transaction fails");
};


# Check the 'actions' method with a table and with each selector.

subtest 'actions' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt('PROCEED');
    
    $edt->insert_record('EDT_TEST', { string_req => 'done' });
    $edt->insert_record('EDT_TEST', { string_req => 'bad', signed_val => 'not a number' });
    $edt->delete_record('EDT_TEST', 5);
    
    ok_commit;
    
    is_deeply( [ $edt->actions('all', 'EDT_NOSUCH') ], [ ], "no actions for a table that was not used" );
    
    is( scalar(my @all = $edt->actions('all', 'EDT_TEST')), 3, "all actions on a table" );
    
    is_deeply( [ map { $_->{status} } $edt->actions('executed') ], [ 'executed', 'executed' ],
	       "the executed actions" );
    
    is_deeply( [ map { $_->{status} } $edt->actions('failed') ], [ 'failed' ], "the failed actions" );
    
    my ($delete) = grep { $_->{operation} eq 'delete' } $edt->actions;
    
    is_deeply( $delete->{params}, { _primary => 5 }, "the parameters of a delete action list its key" );
    
    my ($failed) = $edt->actions('failed');
    
    like( $failed->{conditions}[0], qr/^E_FORMAT: Field 'signed_val'/,
	  "the conditions of an action are listed without labels" );
    
    is_deeply( [ map { $_->{status} } $edt->actions('unexecuted') ], [ 'failed' ],
	       "the selector 'unexecuted' lists the actions that were not executed" );
};


# Check the counts and lists of conditions.

subtest 'condition counts and lists' => sub {
    
    my $edt = ok_new_edt('PROCEED');
    
    $edt->add_condition('main', 'W_TEST', 'warning');
    $edt->add_condition('main', 'E_TEST', 'error');
    
    $edt->insert_record('EDT_TEST', { string_req => 'bad', signed_val => 'not a number' });
    
    is( scalar($edt->conditions('all', 'errors')), 2, "the errors include demoted errors" );
    is( scalar($edt->conditions('all', 'fatal')), 1, "the fatal errors exclude demoted errors" );
    is( scalar($edt->conditions('all', 'warnings')), 1, "the warnings exclude demoted errors" );
    is( scalar($edt->conditions('all', 'nonfatal')), 2, "the nonfatal conditions include demoted errors" );
    
    is_deeply( [ $edt->warnings ], [ "W_TEST: TEST WARNING 'warning'" ], "warnings lists the warnings" );
    is_deeply( [ $edt->warning_strings ], [ $edt->warnings ], "warning_strings is an alias for warnings" );
    is_deeply( [ $edt->error_strings ], [ $edt->errors ], "error_strings is an alias for errors" );
    is_deeply( [ $edt->fatals ], [ "E_TEST: TEST ERROR 'error'" ], "fatals lists the fatal errors" );
    
    is( $edt->condition_nolabel('#1', 'W_PARAM'), 'W_PARAM',
	"a condition whose template is empty is shown as just its code" );
};


# Check that a subclass may override some of the selectors of a condition template, and
# that the rest are taken from EditTransaction.

subtest 'condition templates' => sub {
    
    {
	package ETTemplateClass;
	our @ISA = ('ETBasicTest');
    }
    
    ETTemplateClass->register_conditions(E_NOT_FOUND => { custom2 => "Custom message '&2'" });
    
    my $edt = ETTemplateClass->new($T);
    
    $edt->add_condition('main', 'E_NOT_FOUND', 'custom2', 'abc');
    $edt->add_condition('main', 'E_NOT_FOUND', 'link', 'def');
    
    ok_has_condition($edt, 'main', 'E_NOT_FOUND', qr/Custom message 'abc'/,
		     "the selector defined by the subclass is used");
    
    ok_has_condition($edt, 'main', 'E_NOT_FOUND', qr/Linked record 'def'/,
		     "a selector not defined by the subclass is taken from EditTransaction");
    
    $edt->add_condition('main', 'E_NOT_FOUND', 'ghi');
    
    ok_has_condition($edt, 'main', 'E_NOT_FOUND', qr/No record was found with key 'ghi'/,
		     "the default template is taken from EditTransaction");
};


# Check import_conditions with data in formats it cannot use.

subtest 'import bad formats' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'a', _errwarn => [ 'not a code', 'x' ] });
    
    ok_has_one_condition('&_', 'E_BAD_CONDITION', qr/Unrecognized data format/,
			 "a list that does not start with a condition code is rejected");
    
    $edt->insert_record('EDT_TEST', { string_req => 'a2', _errwarn => { E_TEST => 'x' } });
    
    ok_has_one_condition('&_', 'E_BAD_CONDITION', qr/Unrecognized data format/,
			 "a hash is rejected");
    
    $edt->insert_record('EDT_TEST', { string_req => 'b', _errwarn => [ [ 'E_badcode', 'x' ] ] });
    
    ok_has_one_condition('&_', 'E_BAD_CONDITION', qr/Invalid condition code 'E_badcode'/,
			 "a condition with an invalid code is rejected");
    
    $edt->insert_record('EDT_TEST', { string_req => 'c', _errwarn => 'E_lowercase text' });
    
    ok_has_one_condition('&_', 'E_BAD_CONDITION', qr/Invalid condition code 'E_lowercase'/,
			 "a condition line with an invalid code is rejected");
};


# Check _test_action, which imports conditions from the record.

subtest '_test_action' => sub {
    
    my $edt = ok_new_edt;
    
    my $action = $edt->_test_action('insert', 'EDT_TEST', { string_req => 'test',
							    _errwarn => 'W_TEST: imported' });
    
    is( ref $action, 'EditTransaction::Action', "_test_action returns the action itself" );
    
    ok_has_one_condition('&_', 'W_TEST', qr/imported/, "conditions in the record are imported");
};


# Check the accessors that take an action reference, when there is no such action.

subtest 'accessors without an action' => sub {
    
    my $edt = ok_new_edt;
    
    is_deeply( [ $edt->action_keyvalues('&nosuch') ], [ ], "action_keyvalues returns nothing" );
    
    ok( ! defined $edt->get_keyval('&nosuch'), "get_keyval returns nothing" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'value', _label => 'v' });
    
    ok( ! defined $edt->action_parameter('&nosuch', 'string_req'), "action_parameter returns undef" );
};
