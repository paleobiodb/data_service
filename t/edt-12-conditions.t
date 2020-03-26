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
use Test::More tests => 8;

use TableDefs qw(get_table_property set_column_property);
use TableData qw(reset_cached_column_properties);

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
    
    $primary = get_table_property('EDT_TEST', 'PRIMARY_KEY');
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
    
    # Add a warning, and make sure that the error count doesn't change.
    
    $edt->add_condition('W_EXECUTE');
    
    is( $edt->errors, 1, "still only one error" );
    is( $edt->warnings, 1, "also has one warning" );
    is( $edt->conditions, 2, "two conditions" );
    is( $edt->conditions('main', 'errors'), 1, "one error with 'main'");
    is( $edt->conditions('main', 'warnings'), 1, "one warning with 'main'");
    
    # Make sure that warnings also have the proper class and return the proper values.
    
    my (@w) = $edt->warnings;
    my (@sw) = $edt->conditions('all', 'warnings');

    is( @w, @sw, "warnings and conditions return same number of entries");
    is( $w[0], $sw[0], "warnings and conditions return the same record" );
    
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
    
    $edt->insert_record('EDT_TEST', { _label => 'abc1', string_req => 'def' });
    $edt->add_condition('E_EXECUTE', 'ghi');
    
    is( $edt->errors, 2, "now there are two errors" );
    is( $edt->conditions('latest', 'errors'), 1, "one error for the most recent action" );
    
    my @e = $edt->errors;
    my @s = $edt->conditions('latest', 'errors');
    
    is( $e[1], $s[0], "most recent error record is in both lists" );
    
    unless ( isa_ok( $s[0], 'EditTransaction::Condition', "error has proper class" ) )
    {
	return;
    }
    
    is( $s[0]->code, 'E_EXECUTE', "condition has proper code" );
    is( $s[0]->label, 'abc1', "condition has proper label" );
    is( $s[0]->table, 'EDT_TEST', "condition has proper table" );
    
    my @d = $s[0]->data;
    
    is( $d[0], 'ghi', "condition has proper parameter" );
    
    $edt->add_condition('main', 'E_EXECUTE', 'qxyz');
    
    is( $edt->errors, 3, "now there are three errors" );
    is( $edt->conditions('latest', 'errors'), 1, "still only one error for the most recent action" );
    is( $edt->conditions('main', 'errors'), 2, "two errors not associated with any action" );
    is( $edt->conditions, 4, "four conditions in total");
    is( $edt->conditions('all'), 4, "four conditions also with argument 'all'" );
    is( $edt->warnings('main'), 1, "one warning not associated with any action" );
    
    @e = $edt->errors;
    @s = $edt->conditions('main', 'errors');
    
    is( $e[-1], $s[-1], "last condition is the same on both lists" );
    
    is( $s[-1]->code, 'E_EXECUTE', "last condition has proper code" );
    is( $s[-1]->label, '', "last condition has empty label" );
    is( $s[-1]->table, '', "last condition has empty table" );
    
    is( @s, 2, "conditions method returned a proper list" );
    
    # Check that we can get ahold of the current action.
    
    my $save_action = $edt->current_action;
    
    ok( $save_action, "got current action" );
    
    # Now add another action, another error, and a warning, and check that the counts add up.

    $edt->insert_record('EDT_TEST', { _label => 'abc2', string_req => 'jkl' });
    $edt->add_condition('E_TEST');
    $edt->add_condition('W_TEST');
    
    (@s) = $edt->conditions('latest', 'errors');
    
    is( @s, 1, "one error for this action" ) &&
	isa_ok( $s[0], 'EditTransaction::Condition' ) &&
	is( $s[0]->code, 'E_TEST', "error has proper code" );
    
    (@sw) = $edt->conditions('latest', 'warnings');
    
    is ( @sw, 1, "one warning for this action" ) &&
	isa_ok( $s[0], 'EditTransaction::Condition' ) &&
	is( $sw[0]->code, 'W_TEST', "warning has proper code" );
    
    is( $edt->errors, 4, "now there are four errors" );
    is( $edt->warnings, 2, "now there are two warnings" );
    is( $edt->conditions('main', 'errors'), 2, "still two errors on 'main'" );
    is( $edt->conditions('main', 'warnings'), 1, "still one warning on 'main'");
    
    my $action2 = $edt->current_action;
    
    # Check that we can grab the conditions associated with the saved action.

    my @sa = $edt->conditions($save_action);

    if ( is(@sa, 1, "found one entry from saved action" ) &&
	 isa_ok($sa[0], 'EditTransaction::Condition', "entry was a condition object") )
    {
	is( $sa[0]->code, 'E_EXECUTE', "condition has proper code" );
	is( $sa[0]->label, 'abc1', "condition has proper label" );
	is( $sa[0]->table, 'EDT_TEST', "condition has proper table" );
    }

    my @wa = $edt->conditions($save_action, 'warnings');
    
    is( @wa, 0, "no warnings from saved action" );

    my @wa2 = $edt->conditions($action2, 'warnings');

    is( @wa2, 1, "found one warning from second saved action" ) &&
	isa_ok( $wa2[0], 'EditTransaction::Condition', "found proper condition object" );
    
    # Check that we can add a condition to the previously saved action, and the counts still add
    # up.
    
    if ( $save_action )
    {
	$edt->add_condition($save_action, 'E_REQUIRED', 'foo');
	is( $edt->conditions($save_action, 'errors'), 2, "now two errors on saved action" );
	is( $edt->errors, 5, "now there are five errors" );
	is( $edt->conditions('main', 'errors'), 2, "still two errors on 'main'" );
    }
};


# Now test the routines that return error and warning strings.

subtest 'strings' => sub {
    
    set_column_property('EDT_TEST', 'string_req', ALLOW_TRUNCATE => 1);
    reset_cached_column_properties('EDT_TEST', 'string_req');
    
    my $edt = $T->new_edt($perm_a) || return;
    
    $edt->add_condition('E_EXECUTE', 'foobar');
    $edt->add_condition('C_CREATE');
    $edt->add_condition('W_EXECUTE', 'grex');
    
    $edt->insert_record('EDT_TEST', { string_req => 'abc', _label => 'a1', signed_val => 'def' });
    $edt->insert_record('EDT_TEST', { string_req => 'def' x 100, _label => 'a2' });
    
    is( $edt->error_strings, 3, "got three error strings" );
    
    my @errors = $edt->error_strings;
    
    if ( is( @errors, 3, "got three error strings in list context" ) )
    {
	is( $errors[0], "E_EXECUTE: foobar", "error message 1" );
	like( $errors[1], qr{^C_CREATE: .*}, "error message 2" );
	like( $errors[2], qr{^E_FORMAT \(a1\): .*integer}, "error message 3" );
    }
    
    else
    {
	$T->diag_errors;
    }
    
    is( $edt->warning_strings, 2, "got two warning strings" );

    my @warnings = $edt->warning_strings;

    if ( is( @warnings, 2, "got three warning strings in list context" ) )
    {
	is( $warnings[0], 'W_EXECUTE: grex', "warning message 1" );
	like( $warnings[1], qr{W_TRUNC \(a2\): .*truncated}, "warning message 2" );
    }

    # Now test that demoted errors are reported as warning strings instead of error strings.
    
    $edt = $T->new_edt($perm_a, { NOT_FOUND => 1 });
    
    $edt->update_record('EDT_TEST', { test_id => 9999, string_val => 'abc', _label => 'def:2' });
    $edt->add_condition('E_EXECUTE', 'foobar');
    $edt->add_condition('C_CREATE');
    
    is( $edt->error_strings, 2, "got two error strings" ) || $T->diag_errors('latest');
    is( $edt->warning_strings, 1, "got one warning string" ) || $T->diag_warnings('latest');
    
    my ($w) = $edt->warning_strings;
    
    like( $w, qr{F_NOT_FOUND \(def:2\): .*9999}, "warning message 1" );
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
    
    $T->ok_has_error( 'E_TEST_2' );
    $T->ok_has_error( qr/C_TEST_2: xxx/, "found caution with default" );
    $T->ok_has_warning( qr/W_TEST_2/, "found warning" );

    $edt->add_condition('C_TEST_2', 'abc', 'def');

    $T->ok_has_error( qr/C_TEST_2: test caution def/, "found caution with template" );

    $edt->add_condition('C_TEST_2', 'qrs');

    $T->ok_has_error( qr/C_TEST_2: xxx/, "found caution with default 2" );
};


# Check that invalid calls to add_condition throw exceptions, and also invalid calls to the
# conditions method.

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
    is( $edt->conditions('main', 'errors'), 1, "undefined is the same as 'main'" );
    
    # Check that missing parameters are filled in with UNKNOWN.
    
    $edt->add_condition('E_FORMAT');

    $T->ok_has_error( qr/E_FORMAT.*UNKNOWN.*UNKNOWN/, "found both UNKNOWN substitutions" );

    # Now check that invalid calls to 'conditions' throw exceptions.

    eval {
	$edt->conditions('ack');
    };

    ok( $@, "exception on bad first argument to 'conditions'" ) &&
	like( $@, qr{selector}, "exception contained word 'selector'" );
    
    eval {
	$edt->conditions('main', 'ack');
    };
    
    ok( $@, "exception on bad second argument to 'conditions'" ) &&
	like( $@, qr{type}, "exception contained word 'type'" );
};


# Now make sure that conditions demoted by PROCEED and NOT_FOUND are counted properly. Errors
# should still be errors as far as specific_errors and specific_warnings are concerned.

subtest 'proceed' => sub {
    
    my ($edt, $result);
    
    # Start by creating a transaction to which these conditions can be attached.
    
    $edt = $T->new_edt($perm_a, { PROCEED => 1 }) || return;
    
    # Add a condition without an action, and test that it is still counted as an error.
    
    $edt->add_condition('E_EXECUTE');
    is( $edt->errors, 1, "error is counted" );
    
    # Now add a record with two errors, and test that they are demoted to warnings.
    
    $edt->insert_record('EDT_TEST', { signed_val => 'abc' });
    
    is( $edt->errors, 1, "still one error" ) || $T->diag_errors;
    is( $edt->warnings, 2, "two errors demoted to warnings" ) || $T->diag_warnings;
    is( $edt->conditions('latest', 'errors'), 2, "errors still count when checked for this action" ) || $T->diag_errors;
    ok( ! $edt->conditions('latest', 'warnings'), "no specific warnings for this action" ) || $T->diag_warnings;
    
    my ($e) = $edt->conditions('latest', 'errors');
    
    ok( $e, "found one specific error" ) &&
	like( $e->code, qr/^F_/, "code starts with F_" );
    
    # Now add a record with a warning, and test that the counts add up.
    
    $edt->insert_record('EDT_TEST', { string_req => 'validate warning' });
    
    is( $edt->warnings, 3, "now there are three warnings" );
    is( $edt->conditions('latest', 'errors'), 0, "no specific errors" );
    is( $edt->conditions('latest', 'warnings'), 1, "one specific warning" );
    
    # Now try a caution.
    
    $edt = $T->new_edt($perm_a, { CREATE => 0, PROCEED => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'will trigger caution' });
    
    is( $edt->warnings, 1, "caution was demoted to warning" );

    ($e) = $edt->conditions('latest', 'errors');
    
    ok( $e, "found one specific error" ) &&
	like( $e->code, qr/^D_/, "code starts with D_" );
};


# Now test the allowance NOT_FOUND, which only demotes E_NOT_FOUND errors and leaves the rest.

subtest 'notfound' => sub {
    
    my ($edt, $result);
    
    # Start by creating a transaction to test.
    
    $edt = $T->new_edt($perm_a, { NOT_FOUND => 1 }) || return;
    
    # Add an action that will generate E_NOT_FOUND.

    $edt->update_record('EDT_TEST', { _label => 'f1', $primary => 99999, signed_val => 32 });

    is( $edt->conditions('latest', 'errors'), 1, "one specific error" );
    is( $edt->errors, 0, "no general errors" );
    is( $edt->conditions('latest', 'warnings'), 0, "no specific warnings" );
    is( $edt->warnings, 1, "one general warning" );
    ok( $edt->can_proceed, "transaction can proceed" );
    
    my ($e) = $edt->conditions('latest', 'errors');
    
    if ( isa_ok( $e, 'EditTransaction::Condition', "found error" ) )
    {
	is( $e->code, 'F_NOT_FOUND', "error had proper code" );
	is( $e->label, 'f1', "error had proper label" );
	my ($p, $v) = $e->data;
	is( $p, 'test_no', "error had proper parameter name" );
	is( $v, 99999, "error had proper parameter value" );
    }
    
    $edt->insert_record('EDT_TEST', { _label => 'f2', signed_val => 'abc' });
    
    is( $edt->conditions('latest', 'errors'), 2, "two specific errors" );
    is( $edt->errors, 2, "two general errors" );
    is( $edt->conditions('latest', 'warnings'), 0, "no specific warnings" );
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
