#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-12-conditions.t : Test that error and warning conditions work properly.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 7;

use TableDefs qw($EDT_TEST get_table_property);

use EditTest;
use EditTester;


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;


# Start by getting the variable values that we need to execute the remainder of the test. If the
# session key 'SESSION-AUTHORIZER' does not appear in the session_data table, then run the test
# 'edt-01-basic.t' first.

my ($perm_a, $primary);

subtest 'setup' => sub {
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;
    
    $primary = get_table_property($EDT_TEST, 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || BAIL_OUT;
};


# Create some errors, warnings, and cautions, and check that the routines for querying them return
# the right values.

subtest 'basic' => sub {
    
    my ($edt, $result);

    # Start by creating a transaction to which these conditions can be attached.
    
    $edt = $T->new_edt($perm_a) || return;
    
    # Add a condition, and test that the error message is properly formatted.
    
    $edt->add_condition('E_FORMAT', 'AAA', 'BBB');
    
    my ($msg) = $edt->error_strings;
    
    like( $msg, qr/E_FORMAT.*'AAA'.*BBB/, "message was generated with proper parmaeters" );
    
    my ($e) = $edt->errors;
    
    unless ( isa_ok($e, 'EditTransaction::Condition', "error has proper class" ) &&
	     can_ok($e, 'code', 'label', 'table') )
    {
	return;
    }
    
    is( $e->code, 'E_FORMAT', "condition has proper code" );
    is( $e->label, '', "condition has empty label" );
    is( $e->table, '', "condition has empty table" );
    
    my (@data) = $e->data;
    
    cmp_ok( @data, '==', 2, "both data elements were saved" );
    is( $data[0], 'AAA', "element 1 is ok");
    is( $data[1], 'BBB', "element 2 is ok");
    
    # Now make sure that 'specific_errors' returns the same error condition record.
    
    my ($s) = $edt->specific_errors;

    is( $s, $e, "specific_errors returns the same record" );
    
    # Add a warning, and make sure that the error count doesn't change.

    $edt->add_condition('W_EXECUTE');
    
    is( $edt->errors, 1, "still only one error" );
    is( $edt->warnings, 1, "also has one warning" );
    is( $edt->specific_warnings, 1, "specific_warnings also returns 1" );
    
    # Make sure that warnings also have the proper class and return the proper values.
    
    my (@w) = $edt->warnings;
    my (@sw) = $edt->specific_warnings;
    
    is( $w[0], $sw[0], "warnings and specific_warnings return the same record" );
    
    unless ( isa_ok( $w[0], 'EditTransaction::Condition', "warning has proper class" ) )
    {
	return;
    }
    
    is( $w[0]->code, 'W_EXECUTE', "warning has proper code" );
    is( $w[0]->label, '', "warning has empty label" );
    is( $w[0]->table, '', "warning has empty table" );
    ok( ! $w[0]->data, "warning has no parameters");
    
    ($msg) = $edt->warning_strings;

    is( $msg, 'W_EXECUTE: UNKNOWN', "warning message has proper format with no parameters" );
    
    # Now add a condition after an action, ane make sure that it is properly attached to that
    # action.
    
    $edt->insert_record($EDT_TEST, { record_label => 'abc1', string_req => 'def' });
    $edt->add_condition('E_EXECUTE', 'ghi');
    
    is( $edt->errors, 2, "now there are two errors" );
    is( $edt->specific_errors, 1, "one error for the most recent action" );

    my @e = $edt->errors;
    my @s = $edt->specific_errors;

    is( $e[1], $s[0], "most recent error record is in both lists" );

    unless ( isa_ok( $s[0], 'EditTransaction::Condition', "error has proper class" ) )
    {
	return;
    }
    
    is( $s[0]->code, 'E_EXECUTE', "condition has proper code" );
    is( $s[0]->label, 'abc1', "condition has proper label" );
    is( $s[0]->table, $EDT_TEST, "condition has proper table" );
    
    my @d = $s[0]->data;
    
    is( $d[0], 'ghi', "condition has proper parameter" );
    
    $edt->add_condition('main', 'E_EXECUTE', 'qxyz');

    is( $edt->errors, 3, "now there are three errors" );
    is( $edt->specific_errors, 1, "still only one error for the most recent action" );
    is( $edt->specific_errors('main'), 2, "two errors not associated with any action" );
    is( $edt->specific_warnings('main'), 1, "one warning not associated with any action" );
    
    @e = $edt->errors;
    @s = $edt->specific_errors('main');

    is( $e[-1], $s[-1], "last condition is the same on both lists" );

    is( $s[-1]->code, 'E_EXECUTE', "last condition has proper code" );
    is( $s[-1]->label, '', "last condition has empty label" );
    is( $s[-1]->table, '', "last condition has empty table" );

    # Check that we can get ahold of the current action.

    my $save_action = $edt->current_action;

    ok( $save_action, "got current action" );
    
    # Now add another action, another error, and a warning, and check that the counts add up.

    $edt->insert_record($EDT_TEST, { record_label => 'abc2', string_req => 'jkl' });
    $edt->add_condition('E_TEST');
    $edt->add_condition('W_TEST');
    
    (@s) = $edt->specific_errors;

    is( @s, 1, "one error for this action" ) &&
	is( $s[0]->code, 'E_TEST', "error has proper code" );
    
    (@sw) = $edt->specific_warnings;

    is ( @sw, 1, "one warning for this action" ) &&
	is( $sw[0]->code, 'W_TEST', "warning has proper code" );

    is( $edt->errors, 4, "now there are four errors" );
    is( $edt->warnings, 2, "now there are two warnings" );
    is( $edt->specific_errors('main'), 2, "still two errors on 'main'" );
    is( $edt->specific_warnings('main'), 1, "still one warning on 'main'");

    # Check that we can add a condition to the previously saved action, and the counts still add
    # up.

    if ( $save_action )
    {
	$edt->add_condition($save_action, 'E_REQUIRED', 'foo');
	is( $edt->specific_errors($save_action), 2, "now two errors on saved action" );
	is( $edt->errors, 5, "now there are five errors" );
	is( $edt->specific_errors('main'), 2, "still two errors on 'main'" );
    }
};


# Test the registration of condition codes by subclasses.

subtest 'register' => sub {
    
    # Check that a subclass can properly register a condition code. Also check that a condition
    # code is allowed to contain numbers and underscores.
    
    eval {
	EditTest->register_conditions(E_TEST_2 => 'this is a test error',
				      C_TEST_2 => { abc => 'test caution %2', default => 'xxx' },
				      W_TEST_2 => 'this is a test warning');
    };
    
    ok( !$@, "register subclass conditions" ) || diag("message was: $@");
    
    # Check that an exception is thrown when invalid codes are registered, or when a valid
    # template is not provided.
    
    eval {
	EditTest->register_conditions(F_TEST => 'this is invalid');
    };
    
    ok( $@, "exception from invalid condition prefix" );
    
    eval {
	EditTest->register_conditions(E_testx => 'this is invalid');
    };

    ok( $@, "exception from lowercase condition" );

    eval {
	EditTest->register_conditions('E_TEST_BAD');
    };

    ok( $@, "exception from missing template" );

    eval {
	EditTest->register_conditions(E_TEST_BAD => '');
    };
    
    ok( $@, "exception from empty template" );

    # Check that registered codes can be used.

    my $edt = $T->new_edt($perm_a) || return;
    
    eval {
	$edt->add_condition('E_TEST_2');
	$edt->add_condition('C_TEST_2');
	$edt->add_condition('W_TEST_2');
    };

    unless ( ok( ! $@, "use subclass conditions" ) )
    {
	diag("message was: $@");
	return;
    }
    
    $T->ok_has_error( 'any', 'E_TEST_2' );
    $T->ok_has_error( 'any', qr/C_TEST_2: xxx/, "found caution with default" );
    $T->ok_has_warning( qr/W_TEST_2/, "found warning" );

    $edt->add_condition('C_TEST_2', 'abc', 'def');

    $T->ok_has_error( 'any', qr/C_TEST_2: test caution def/, "found caution with template" );

    $edt->add_condition('C_TEST_2', 'qrs');

    $T->ok_has_error( 'any', qr/C_TEST_2: xxx/, "found caution with default 2" );
};


# Check that invalid calls to  generate errors.

subtest 'invalid' => sub {

    my $edt = $T->new_edt($perm_a) || return;
    
    # Check that starting the argument list with a reference that is not an action is also caught.
    
    eval {
	$edt->add_condition( { }, 'E_EXECUTE' );
    };

    ok( $@, "exception on non-action reference" );

    # Check that starting the argument list with undef is okay.

    eval {
	$edt->add_condition( undef, 'E_EXECUTE' );
    };

    ok( ! $@, "first undefined is okay" );
    is( $edt->specific_errors('main'), 1, "undefined is the same as 'main'" );

    # Check that missing parameters are filled in with UNKNOWN.

    $edt->add_condition('E_FORMAT');

    $T->ok_has_error( 'any', qr/E_FORMAT.*UNKNOWN.*UNKNOWN/, "found both UNKNOWN substitutions" );
};


# Now make sure that conditions demoted by PROCEED_MODE and NOT_FOUND are counted properly. Errors
# should still be errors as far as specific_errors and specific_warnings are concerned.

subtest 'proceed' => sub {
    
    my ($edt, $result);
    
    # Start by creating a transaction to which these conditions can be attached.
    
    $edt = $T->new_edt($perm_a, { PROCEED_MODE => 1 }) || return;
    
    # Add a condition without an action, and test that it is still counted as an error.
    
    $edt->add_condition('E_EXECUTE');
    is( $edt->errors, 1, "error is counted" );
    
    # Now add a record with two errors, and test that they are demoted to warnings.
    
    $edt->insert_record($EDT_TEST, { signed_val => 'abc' });
    
    is( $edt->errors, 1, "still one error" );
    is( $edt->warnings, 2, "two errors demoted to warnings" );
    is( $edt->specific_errors, 2, "errors still count when checked for this action" );
    ok( ! $edt->specific_warnings, "no specific warnings for this action" );
    
    my ($e) = $edt->specific_errors;

    ok( $e, "found one specific error" ) &&
	like( $e->code, qr/^F_/, "code starts with F_" );
    
    # Now add a record with a warning, and test that the counts add up.

    $edt->insert_record($EDT_TEST, { string_req => 'validate warning' });
    
    is( $edt->warnings, 3, "now there are three warnings" );
    is( $edt->specific_errors, 0, "no specific errors" );
    is( $edt->specific_warnings, 1, "one specific warning" );
    
    # Now try a caution.
    
    $edt = $T->new_edt($perm_a, { CREATE => 0, PROCEED_MODE => 1 });
    
    $edt->insert_record($EDT_TEST, { string_req => 'will trigger caution' });
    
    is( $edt->warnings, 1, "caution was demoted to warning" );

    ($e) = $edt->specific_errors;

    ok( $e, "found one specific error" ) &&
	like( $e->code, qr/^D_/, "code starts with D_" );
};


# Now test the allowance NOT_FOUND, which only demotes E_NOT_FOUND errors and leaves the rest.

subtest 'notfound' => sub {
    
    my ($edt, $result);
    
    # Start by creating a transaction to test.
    
    $edt = $T->new_edt($perm_a, { NOT_FOUND => 1 }) || return;
    
    # Add an action that will generate E_NOT_FOUND.

    $edt->update_record($EDT_TEST, { record_label => 'f1', $primary => 99999, signed_val => 32 });

    is( $edt->specific_errors, 1, "one specific error" );
    is( $edt->errors, 0, "no general errors" );
    is( $edt->specific_warnings, 0, "no specific warnings" );
    is( $edt->warnings, 1, "one general warning" );
    ok( $edt->can_proceed, "transaction can proceed" );
    
    my ($e) = $edt->specific_errors;
    
    if ( ok( $e, "found error" ) )
    {
	is( $e->code, 'F_NOT_FOUND', "error had proper code" );
	is( $e->label, 'f1', "error had proper label" );
	my ($d) = $e->data;
	is( $d, 99999, "error had proper parameter" );
    }
    
    $edt->insert_record($EDT_TEST, { record_label => 'f2', signed_val => 'abc' });
    
    is( $edt->specific_errors, 2, "two specific errors" );
    is( $edt->errors, 2, "two general errors" );
    is( $edt->specific_warnings, 0, "no specific warnings" );
    is( $edt->warnings, 1, "one general warning" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
};


# Test the 'generate_msg' method.

subtest 'generate_msg' => sub {
    
    my $edt = $T->new_edt($perm_a) || return;

    $edt->add_condition('E_FORMAT', 'abc', 'xyz');

    my ($e) = $edt->errors;

    my $msg = $edt->generate_msg($e);

    like( $msg, qr/abc.*xyz/, "generated proper message" );
};
