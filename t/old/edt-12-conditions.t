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

use TableDefs qw(get_table_property set_column_property);
use TableData qw(reset_cached_column_properties);

use EditTest;
use EditTester;


$DB::single = 1;

# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new({ subclass => 'EditTest' });


# Start by getting the variable values that we need to execute the remainder of the test. If the
# session key 'SESSION-AUTHORIZER' does not appear in the session_data table, then run the test
# 'edt-01-basic.t' first.

my ($perm_a, $primary);

subtest 'setup' => sub {
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) ||
	BAIL_OUT "You must run edt-01-basic.t first, to create the proper entries in the session_data table";
    
    $primary = get_table_property('EDT_TEST', 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || BAIL_OUT;

    $T->clear_table('EDT_TEST');
};


# Create some errors, warnings, and cautions, and check that the routines for querying them return
# the right values.

subtest 'basic' => sub {
    
    my ($edt, $result);

    # Start by creating a transaction to which these conditions can be attached.
    
    $edt = $T->new_edt($perm_a) || return;
    
    # Add a condition, and test that the error message is properly formatted.
    
    $edt->add_condition('E_EXECUTE', 'some error');
    
    my ($msg) = $edt->conditions;
    
    like( $msg, qr/^E_EXECUTE: .*\bsome error\b/, "error message contains code and parameter" );
    
    my ($err) = $edt->errors;
    
    is($msg, $err, "conditions and errors return the same string");
    
    # Add a warning, and make sure that the error count doesn't change.
    
    $edt->add_condition('W_EXECUTE');
    
    my ($dummy, $msg1) = $edt->conditions;

    my ($warn) = $edt->warnings;

    is( $msg1, $warn, "conditions and warnings return the same string, and warning is second");
    
    like( $warn, qr/^W_EXECUTE: .*\bunknown\b|^W_EXECUTE$/i, "warning message contains code and 'unknown'" );
    
    is( $edt->errors, 1, "still only one error" );
    is( $edt->warnings, 1, "also has one warning" );
    is( $edt->conditions, 2, "two conditions" );
    is( $edt->conditions('main', 'errors'), 1, "one error with 'main'");
    is( $edt->conditions('main', 'warnings'), 1, "one warning with 'main'");
    is( $edt->conditions('main', 'fatal'), 1, "one fatal with 'main'");
    is( $edt->conditions('main', 'nonfatal'), 1, "one nonfatal with 'main'");
    
    # Check that leaving off the 'main' argument doesn't change the answers.

    is( $edt->conditions('errors'), 1, "one error with no selector");
    is( $edt->conditions('warnings'), 1, "one warning with no selector");
    is( $edt->conditions('fatal'), 1, "one fatal with no selector");
    is( $edt->conditions('nonfatal'), 1, "one nonfatal with no selector");
    
    # Now add some conditions after an action, ane make sure that they are properly attached to
    # that action. Also check that the last condition is ignored since it duplicates one that was
    # already added.
    
    $edt->insert_record('EDT_TEST', { _label => 'abc1', string_req => 'def' });
    $edt->add_condition('E_FORMAT', 'AAA', 'BBB');
    $edt->add_condition('W_PARAM', 'CCC');
    $edt->add_condition('W_PARAM', 'DDD');
    $edt->add_condition('W_PARAM', 'CCC');
    
    my @err_all = $edt->errors;
    my @err_latest = $edt->errors('latest');
    my @warn_all = $edt->warnings;
    my @warn_latest = $edt->conditions('latest', 'warnings');
    
    is( @err_all, 2, "now there are two errors" );
    is( @err_latest, 1, "one error for the most recent action" );
    is( $edt->conditions('main', 'errors'), 1, "still one main error" );
    
    is( $err_all[-1], $err_latest[-1], "most recent error condition is in both lists" );
    
    like( $err_latest[-1], qr/ ^ E_FORMAT \s \(abc1\): \s (?: .* \bAAA\b .* \bBBB\b | .* \bBBB\b .* \bAAA\b )/x,
	  "error message contains code and parameters" );
    
    is( @warn_all, 3, "three warnings in total" );
    is( @warn_latest, 2, "two warnings with 'latest'" );

    is( $warn_all[-1], $warn_latest[-1], "most recent warning condition is in both lists" );
    
    # Add another error condition not associated with any action.
    
    $edt->add_condition('main', 'E_EXECUTE', 'qxyz');
    
    is( $edt->errors, 3, "now there are three errors" );
    is( $edt->conditions('latest', 'errors'), 1, "still only one error for the most recent action" );
    is( $edt->conditions('main', 'errors'), 2, "two errors not associated with any action" );
    is( $edt->warnings('main'), 1, "one warning not associated with any action" );
    is( $edt->conditions, 6, "six conditions in total");
    is( $edt->conditions('all'), 6, "six conditions also with argument 'all'" );
    
    # Check that we can get ahold of the current action.
    
    my $action1 = $edt->current_action;
    
    is( $action1, ':abc1', "current action reference is ':abc1'" );
    
    # Now add another action, another error, and a warning, and check that the counts add up.

    $edt->insert_record('EDT_TEST', { _label => 'abc2', string_req => 'jkl' });
    $edt->add_condition('C_LOCKED');
    $edt->add_condition('W_PARAM');
    
    is( $edt->errors('latest'), 1, "one error for this action" );
    is( $edt->warnings('latest'), 1, "one warning for this action" );
    is( $edt->errors('main'), 2, "still two errors on 'main'" );
    is( $edt->warnings('main'), 1, "still one warning on 'main'");
    
    is( $edt->conditions, 8, "now eight conditions" );
    
    my $action2 = $edt->current_action;
    
    # Add an error and a warning to a previous action.
    
    $edt->add_condition($action1, 'E_RANGE', 'CCC', 'DDD');
    $edt->add_condition(':#1', 'W_PARAM', 'EEE');
    is( $edt->conditions('latest', 'errors'), 1, "still only one error for the most recent action" );
    is( $edt->conditions('latest', 'warnings'), 1, "still only one warning for the most recent action" );
    
    my @err = $edt->errors;

    is( @err, 5, "found 5 errors total" );
    
    like( $err[0], qr/^E_EXECUTE: .*some error/, "err[0] is correct" );
    like( $err[1], qr/^E_EXECUTE: .*qxyz/, "err[1] is correct" );
    like( $err[2], qr/^E_FORMAT \(abc1\): .*AAA/, "err[2] is correct" );
    like( $err[3], qr/^E_RANGE \(abc1\): .*CCC/, "err[3] is correct" );
    like( $err[4], qr/^C_LOCKED \(abc2\)/, "err[4]is correct" ); 

    # $T->diag_errors;
    # $T->diag_warnings;
    
    my @warn = $edt->warnings;
    
    is( @warn, 5, "found 5 warnings total" );
    
    like( $warn[0], qr/^W_EXECUTE: .*unknown/i, "warn[0] is correct" );
    like( $warn[1], qr/^W_PARAM \(abc1\): CCC$/, "warn[1] is correct" );
    like( $warn[2], qr/^W_PARAM \(abc1\): DDD$/, "warn[2] is correct" );
    like( $warn[3], qr/^W_PARAM \(abc1\): EEE$/, "warn[3] is correct" );
    like( $warn[4], qr/^W_PARAM \(abc2\)$/, "warn[4] is correct" );
    
    # Check that we can grab the conditions associated with the saved action.
    
    my @sa = $edt->conditions($action1);

    # diag($_) foreach @sa;
    
    is( @sa, 5, "found 5 conditions from action 1" );
    
    is( $edt->conditions($action1, 'errors'), 2, "found two errors from action 1" );
    is( $edt->conditions($action1, 'warnings'), 3, "found three warnings from action 1" );
    
    is( $sa[0], $err[2], "sa[0] matches" );
    is( $sa[1], $warn[1], "sa[1] matches" );
    is( $sa[2], $warn[2], "sa[2] matches" );
    is( $sa[3], $err[3], "sa[3] matches" );
    is( $sa[4], $warn[3], "sa[4] matches" );

    # Check that the has_condition method works properly.

    ok( $edt->has_condition('C_LOCKED'), "has condition C_LOCKED" );
    ok( $edt->has_condition('C_LOCKED', undef), "has condition C_LOCKED with undef" );
    ok( ! $edt->has_condition('C_LOCKED', 'foo'), "no condition C_LOCKED with 'foo'" );
    ok( $edt->has_condition('E_EXECUTE'), "has condition E_EXECUTE" );
    ok( $edt->has_condition('E_EXECUTE', 'qxyz'), "has condition E_EXECUTE with 'qxyz'" );
    ok( ! $edt->has_condition('E_EXECUTE', 'foo_x1'), "no condition E_EXECUTE with 'foo_x1'" );
    ok( $edt->has_condition('W_PARAM'), "has condition W_PARAM" );
    ok( $edt->has_condition('W_PARAM', undef), "has condition W_PARAM with undef" );
    ok( $edt->has_condition('W_PARAM', 'EEE'), "has condition W_PARAM with 'EEE'" );
    ok( ! $edt->has_condition('W_PARAM', 'FFF'), "no condition W_PARAM with 'FFF'" );
    ok( ! $edt->has_condition('E_HAS_KEY'), "no condition E_HAS_KEY" );
    ok( ! $edt->has_condition('W_UNCHANGED'), "no condition W_UNCHANGED" );
    ok( ! $edt->has_condition('E_FOOBAR'), "no condition E_FOOBAR" );
    
    if ( ok( $edt->has_condition('main', 'E_EXECUTE'), "has condition E_EXECUTE with 'main'" ) )
    {
	ok( $edt->has_condition('main', 'E_EXECUTE', 'qxyz'),
	    "has condition E_EXECUTE with 'main' and 'qxyz'" );
	ok( ! $edt->has_condition('main', 'E_EXECUTE', 'foo_x2'),
	    "no condition E_EXECUTE with 'main' and 'foo_x2'" );
	ok( ! $edt->has_condition('main', 'C_LOCKED'), "no condition C_LOCKED with 'main'" );
	ok( ! $edt->has_condition('main', 'E_FOOBAR'), "no condition E_FOOBAR with 'main'" );
	ok( $edt->has_condition('all', 'E_EXECUTE'), "has condition E_EXECUTE with 'all'" );
	ok( $edt->has_condition('all', 'C_LOCKED'), "has condition C_LOCKED with 'all'" );
	ok( ! $edt->has_condition('all', 'E_FOOBAR'), "no condition E_FOOBAR with 'all'" );
    }
    
    if ( ok( $edt->has_condition('latest', 'W_PARAM'), "has condition W_PARAM with 'latest'" ) )
    {
	ok( ! $edt->has_condition('latest', 'E_EXECUTE'), "no condition E_EXECUTE with 'latest'" );
	ok( ! $edt->has_condition('latest', 'E_FORMAT'), "no condition E_FORMAT with 'latest'" );
	ok( ! $edt->has_condition('latest', 'E_FOOBAR'), "no condition E_FOOBAR with 'latest'" );
    }
    
    if ( ok( $edt->has_action(':abc1') && $edt->has_condition(':abc1', 'E_FORMAT'),
	     "has condition E_FORMAT with :abc1") )
    {
	ok( $edt->has_condition(':abc1', 'E_FORMAT', 'AAA', 'BBB'),
	    "has condition E_FORMAT with 'AAA' and 'BBB'" );
	ok( $edt->has_condition(':abc1', 'E_FORMAT', undef, 'BBB'),
	    "has condition E_FORMAT with undef and 'BBB'" );
	ok( $edt->has_condition(':abc1', 'E_FORMAT', 'AAA', undef),
	    "has condition E_FORMAT with 'AAA' and undef" );
	ok( ! $edt->has_condition(':abc1', 'E_FORMAT', undef, 'CCC'),
	    "no condition E_FORMAT with undef and 'CCC'" );
	ok( ! $edt->has_condition(':abc1', 'E_FORMAT', 'DDD'),
	    "no condition E_FORMAT with 'DDD'" );
	ok( ! $edt->has_condition(':abc1', 'C_LOCKED'), "no condition C_LOCKED with :abc1" );
	ok( ! $edt->has_condition(':abc1', 'W_FOOBAR'), "no condition W_FOOBAR with :abc1" );
    }
};


# Now test the routines that return error and warning strings.

# subtest 'strings' => sub {
    
#     set_column_property('EDT_TEST', 'string_req', ALLOW_TRUNCATE => 1);
#     reset_cached_column_properties('EDT_TEST', 'string_req');
    
#     my $edt = $T->new_edt($perm_a) || return;
    
#     $edt->add_condition('E_EXECUTE', 'foobar');
#     $edt->add_condition('C_CREATE');
#     $edt->add_condition('W_EXECUTE', 'grex');
    
#     $edt->insert_record('EDT_TEST', { string_req => 'abc', _label => 'a1', signed_val => 'def' });
#     $edt->insert_record('EDT_TEST', { string_req => 'def' x 100, _label => 'a2' });
    
#     is( $edt->error_strings, 3, "got three error strings" );
    
#     my @errors = $edt->error_strings;
    
#     if ( is( @errors, 3, "got three error strings in list context" ) )
#     {
# 	is( $errors[0], "E_EXECUTE: foobar", "error message 1" );
# 	like( $errors[1], qr{^C_CREATE: .*}, "error message 2" );
# 	like( $errors[2], qr{^E_FORMAT \(a1\): .*integer}, "error message 3" );
#     }
    
#     else
#     {
# 	$T->diag_errors;
#     }
    
#     is( $edt->warning_strings, 2, "got two warning strings" );

#     my @warnings = $edt->warning_strings;

#     if ( is( @warnings, 2, "got three warning strings in list context" ) )
#     {
# 	is( $warnings[0], 'W_EXECUTE: grex', "warning message 1" );
# 	like( $warnings[1], qr{W_TRUNC \(a2\): .*truncated}, "warning message 2" );
#     }

#     # Now test that demoted errors are reported as warning strings instead of error strings.
    
#     $edt = $T->new_edt($perm_a, { NOT_FOUND => 1 });
    
#     $edt->update_record('EDT_TEST', { test_id => 9999, string_val => 'abc', _label => 'def:2' });
#     $edt->add_condition('E_EXECUTE', 'foobar');
#     $edt->add_condition('C_CREATE');
    
#     is( $edt->error_strings, 2, "got two error strings" ) || $T->diag_errors('latest');
#     is( $edt->warning_strings, 1, "got one warning string" ) || $T->diag_warnings('latest');
    
#     my ($w) = $edt->warning_strings;
    
#     like( $w, qr{F_NOT_FOUND \(def:2\): .*9999}, "warning message 1" );
# };


# Test the registration of condition codes by subclasses.

subtest 'register' => sub {
    
    # Check that a subclass can properly register a condition code. Also check that a condition
    # code is allowed to contain numbers and underscores.
    
    eval {
	EditTest->register_conditions(E_TEST_2 => "this is a test error",
				      C_TEST_2 => { abc => ["test caution &2 - &1", "test caution &1"],
						    default => 'xxx' },
				      W_TEST_2 => ["test warning: &1", "this is a test warning"],
				      W_TEST_3 => "test warning '&1'");
    };
    
    ok( !$@, "register_conditions" ) || diag("message was: $@");
    
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
    
    # Check that registered codes can be used.
    
    my $edt = $T->new_edt($perm_a) || return;
    
    eval {
	$edt->add_condition('E_TEST_2', 'bar');
	$edt->add_condition('C_TEST_2');
	$edt->add_condition('C_TEST_2', 'abc');
	$edt->add_condition('C_TEST_2', 'abc', 'def');
	$edt->add_condition('W_TEST_2');
	$edt->add_condition('W_TEST_2', 'foo');
	$edt->add_condition('W_TEST_3');
    };

    unless ( ok( ! $@, "use subclass conditions" ) )
    {
	diag("message was: $@");
	return;
    }
    
    ok( $edt->has_condition('E_TEST_2'), "found E_TEST_2" );
    ok( $edt->has_condition('C_TEST_2'), "found C_TEST_2" );
    ok( $edt->has_condition('W_TEST_2'), "found W_TEST_2" );

    $T->ok_has_condition( qr/^E_TEST_2: this is a test error$/, "E_TEST_2 has template message" );
    $T->ok_has_condition( qr/^C_TEST_2: xxx$/, "found C_TEST_2 with default" );
    $T->ok_has_condition( qr/^C_TEST_2: test caution abc$/, "found C_TEST_2 with one parameter" );
    $T->ok_has_condition( qr/^C_TEST_2: test caution def - abc$/, "found C_TEST_2 with two parameters" );
    $T->ok_has_condition( qr/^W_TEST_2: test warning: foo$/, "found W_TEST_2 with one parameter" );
    $T->ok_has_condition( qr/^W_TEST_2: this is a test warning$/, "found W_TEST_2 with no parameters" );
    $T->ok_has_condition( qr/^W_TEST_3$/i, "found W_TEST_3 with missing parameter" );
};


# Check that invalid calls to add_condition, has_condition, and conditions throw exceptions.

subtest 'invalid' => sub {

    my $edt = $T->new_edt($perm_a) || return;
    
    # Check that starting the argument list with a reference that is not an action is also caught.
    
    eval {
	$edt->add_condition( { }, 'E_EXECUTE');
    };
    
    ok( $@, "exception on non-action perl reference" );
    
    # Check that starting the argument list with undef is okay.

    eval {
	$edt->add_condition(undef, 'E_EXECUTE');
    };
    
    ok( ! $@, "first undefined is okay" );
    ok( $edt->has_condition('main', 'E_EXECUTE'), "undefined means the same as 'main'" );
    
    # Check that an invalid action reference throws an exception as well.

    eval {
	$edt->add_condition(':aaa', 'E_PARAM');
    };
    
    ok( $@, "exception on 'add_condition' with bad action reference" ) &&
	like( $@, qr{action.*match|match.*action}, "exception contained 'action' and 'match'" );
    
    # Check that invalid calls to 'conditions' throw exceptions.

    eval {
	$edt->conditions('ack');
    };

    ok( $@, "exception on bad first argument to 'conditions'" ) &&
	like( $@, qr{selector}, "exception contained the word 'selector'" );
    
    eval {
	$edt->conditions('main', 'ack');
    };
    
    ok( $@, "exception on bad second argument to 'conditions'" ) &&
	like( $@, qr{type}, "exception contained the word 'type'" );
    
    eval {
	$edt->conditions(':aaa', 'errors');
    };

    ok( $@, "exception on 'conditions' with bad action reference" ) &&
	like( $@, qr{action.*match|match.*action}, "exception contained 'action' and 'match'" );
    
    # Check that invalid calls to 'has_condition' throw exceptions.
    
    eval {
	$edt->has_condition('ack');
    };
    
    ok( $@, "exception on bad first argument to 'has_condition'" ) &&
	like( $@, qr{selector}, "exception contained the word 'selector'" );
    
    eval {
	$edt->has_condition('main');
    };
    
    ok( $@, "exception on 'has_condition' with selector but no code" ) &&
	like( $@, qr{specify}, "exception contained the word 'specify'" );
    
    eval {
	$edt->has_condition;
    };
    
    ok( $@, "exception on 'has_condition' with no arguments" ) &&
	like( $@, qr{specify}, "exception contained the word 'specify'" );
    
    eval {
	$edt->has_condition(':abc', 'E_EXECUTE');
    };
    
    ok( $@, "exception for 'has_condition' with bad action reference" ) &&
	like( $@, qr{action.*match|match.*action}, "exception contained 'action' and 'match'" );
};


# Now make sure that conditions demoted by PROCEED, NOT_FOUND, and NOT_PERMITTED are counted
# properly. Errors should still be errors as far as specific_errors and specific_warnings are
# concerned.

subtest 'proceed' => sub {
    
    my ($edt, $result);
    
    # Start by creating a transaction to which these conditions can be attached.
    
    $edt = $T->new_edt($perm_a, { PROCEED => 1 }) || return;
    
    # Add a condition without an action, and test that it is still counted as an error.
    
    $edt->add_condition('E_EXECUTE');
    is( $edt->errors, 1, "error is counted" );
    
    # Now add a record with two errors, and test that they are demoted to warnings.
    
    $edt->insert_record('EDT_TEST', { signed_val => 'abc' });

    is( $edt->fatals, 1, "still one fatal error" ) || $T->diag_errors;
    is( $edt->errors, 3, "three errors total" ) || $T->diag_errors;
    is( $edt->nonfatals, 2, "two errors demoted to warnings" ) || $T->diag_warnings;
    is( $edt->warnings, 0, "no original warnings" ) || $T->diag_warnings;
    
    is( $edt->errors('latest'), 2, "errors still count when checked for this action" ) || $T->diag_errors;
    is( $edt->fatals('latest'), 0, "errors on latest action are not fatal" ) || $T->diag_errors;
    
    ok( $edt->has_condition('F_REQUIRED'), "E_REQUIRED was properly demoted" );
    ok( ! $edt->has_condition('E_REQUIRED'), "E_REQUIRED was properly demoted 2" );
    ok( $edt->has_condition('F_FORMAT'), "E_FORMAT was properly demoted" );
    ok( ! $edt->has_condition('E_FORMAT'), "E_FORMAT was properly demoted 2" );
    
    my (@errs) = $edt->conditions('latest');
    
    like( $errs[0], qr{^F_REQUIRED}, "demoted error codes start with F_" );
    
    # Now add a record with a warning, and test that the counts add up.
    
    $edt->insert_record('EDT_TEST', { string_req => 'validate warning' });

    is( $edt->nonfatals, 3, "now there are three nonfatal conditions" );
    is( $edt->warnings, 1, "now there is one warning" );
    
    # Now add another error, and check that it gets demoted.

    $edt->add_condition('E_PARAM', 'foo1');

    is( $edt->nonfatals, 4, "now there are four nonfatal conditions" );
    is( $edt->errors, 4, "now there are four error conditions" );

    # Add another caution, and check that it is NOT demoted.

    $edt->add_condition('C_LOCKED');

    is( $edt->nonfatals, 4, "still four nonfatal conditions" );
    is( $edt->fatals, 2, "now there are two fatal conditions" );
    is( $edt->errors, 5, "four errors in total" );
    ok( $edt->has_condition('C_LOCKED'), "caution code was not altered" );
};


# Now test the effect of NOT_FOUND and NOT_PERMITTED on error conditions.

subtest 'notfound and notpermitted' => sub {
    
    my ($edt, $result);
    
    # Start by creating a transaction to test.
    
    $edt = $T->new_edt($perm_a, { NOT_FOUND => 1, NOT_PERMITTED => 1 }) || return;
    
    # Add an action that will generate E_NOT_FOUND.

    $edt->update_record('EDT_TEST', { _label => 'f1', $primary => 99999, signed_val => 32 });

    is( $edt->errors, 1, "one error" );
    is( $edt->fatals, 0, "no fatal conditions" );
    is( $edt->nonfatals, 1, "one nonfatal condition" );
    ok( $edt->has_condition('F_NOT_FOUND'), "E_NOT_FOUND was demoted" );
    ok( ! $edt->has_condition('E_NOT_FOUND'), "E_NOT_FOUND was demoted 2" );
    ok( $edt->can_proceed, "transaction can proceed" );
    
    # Add an action that will succeed, but then add E_PERM.
    
    $edt->insert_record('EDT_TEST', { _label => 'f2', string_req => 'aaa' });
    $edt->add_condition('E_PERM', 'insert');

    is( $edt->errors, 2, "two errors" );
    is( $edt->fatals, 0, "still no fatal conditions" );
    is( $edt->nonfatals, 2, "now there are two nonfatal conditions" );
    ok( $edt->has_condition('F_PERM'), "F_PERM was demoted" );
    ok( ! $edt->has_condition('E_PERM'), "E_PERM was demoted 2" );
    ok( $edt->can_proceed, "transaction can proceed" );
    
    # Add an action that will generate errors that are not covered.
    
    $edt->insert_record('EDT_TEST', { _label => 'f3', signed_val => 'abc' });
    
    is( $edt->errors, 4, "added two error conditions" );
    is( $edt->fatals, 2, "both are fatal" );
    ok( $edt->has_condition('E_REQUIRED'), "E_REQUIRED was not demoted" );
    ok( $edt->has_condition('E_FORMAT'), "E_FORMAT was not demoted" );
    ok( ! $edt->can_proceed, "transaction cannot proceed" );
};


# Test the 'condition_message' method.

subtest 'condition_message' => sub {
    
    my $edt = $T->new_edt($perm_a) || return;

    my $message = $edt->condition_message('E_FORMAT', 'abc', 'xyz');
    
    like( $message, qr/ \babc\b .* \bxyz\b | \bxyz\b .* \babc\b /x, "generated proper message" );
};
