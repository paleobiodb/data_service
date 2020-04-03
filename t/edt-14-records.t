#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-14-records.t : Test the methods for dealing with records and record labels.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use TableDefs qw(get_table_property set_table_property);

use EditTest;
use EditTester;


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new({ subclass => 'EditTest' });


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


# Do some record operations, and make sure that the record counts and labels match up properly.

subtest 'basic' => sub {
    
    my ($edt, $result);
    
    # Start by creating a transaction, and then some actions. If the transaction cannot be
    # created, abort this test.
    
    $edt = $T->new_edt($perm_a) || return;
    
    # Add some records, with and without record labels. Use 'ignore_record' to skip some.
    
    $edt->insert_record('EDT_TEST', { string_req => 'abc', _label => 'a1' });
    
    is( $edt->current_action && $edt->current_action->label, 'a1',
	"first action has proper label" );

    $edt->add_condition('E_TEST');
    
    $T->ok_has_error( qr/^E_TEST \(a1\):/, "first condition has proper label" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'no label' });

    is( $edt->current_action && $edt->current_action->label, '#2',
	"second action has proper label" );
    
    $edt->ignore_record;
    
    $edt->insert_record('EDT_TEST', { string_req => 'no label' });
    
    is( $edt->current_action && $edt->current_action->label, '#4',
	"third action has proper label" );
    
    $edt->add_condition('W_TEST');
    
    $T->ok_has_warning( qr/^W_TEST \(#4\):/, "second condition has proper label");

    # Now call 'abort_action' and check that the status has changed.

    my $action = $edt->current_action;
    
    is ( $action && $action->status, '', "record status is empty" );
    
    $edt->abort_action;

    is( $action && $action->status, 'abandoned', "record has been marked as abandoned" );

    # Add another action and make sure it has the proper label.

    $edt->insert_record('EDT_TEST', { string_req => 'test action' });

    $action = $edt->current_action;

    is( $action && $action->label, '#5', "action has the proper label" );
};


# Test the PRIMARY_FIELD table property, by setting it and then using the new field name.

subtest 'primary_attr' => sub {

    # Clear the table so that we can check for record updating.
    
    $T->clear_table('EDT_TEST');
    
    # First check that we can update records using the primary key field name for the table, as a
    # control.
    
    my $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    my $key = $edt->insert_record('EDT_TEST', { string_req => 'primary attr test' });
    
    $T->ok_result( $key, "test record was properly inserted" ) || return;
    
    ok( $edt->update_record('EDT_TEST', { $primary => $key, signed_val => 3 }),
	"record was updated using primary key field name" );

    # Now try a different field name, and see that it fails.
    
    ok( ! $edt->update_record('EDT_TEST', { not_the_key => $key, signed_val => 4 }),
	"record was not updated using field name 'not_the_key'" );
    
    $T->ok_has_error( 'F_NO_KEY', "got F_NO_KEY warning" );
    
    # Then set this field name as the PRIMARY_FIELD, and check that it succeeds.
    
    set_table_property('EDT_TEST', PRIMARY_FIELD => 'not_the_key');
    
    ok( $edt->update_record('EDT_TEST', { not_the_key => $key, signed_val => 5 }),
	"record was updated using field name 'not_the_key'" ) ||
	    $T->diag_errors('latest');
    
    # Make sure that the record was in fact updated in the table.
    
    $T->ok_found_record('EDT_TEST', "signed_val=5");

    # Then check that we can still use the primary key name.

    ok( $edt->update_record('EDT_TEST', { $primary => $key, signed_val => 6 }),
	"record was updated again using primary key field name" ) ||
	    $T->diag_errors('latest');
    
    $T->ok_found_record('EDT_TEST', "signed_val=6");
};


# Now test the ALTERNATE_NAME column property, which has the same effect for arbitrary columns.

subtest 'alternate_name' => sub {
    
    # Clear the table so that we can check for record updating.
    
    $T->clear_table('EDT_TEST');
    
    # First check that we can update records using the primary key field name for the table, as a
    # control.
    
    my $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    my $key1 = $edt->insert_record('EDT_TEST', { string_req => 'alternate name test' });
    
    $T->ok_result( $key1, "test record was properly inserted" ) || return;
    
    $edt->update_record('EDT_TEST', { $primary => $key1, alt_val => 'abc' });
    
    $T->ok_no_conditions;
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and string_val = 'abc'");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, string_val => 'def' });

    $T->ok_no_conditions;
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and string_val = 'def'");

    $edt->update_record('EDT_TEST', { $primary => $key1, alt_val => undef });

    $T->ok_no_conditions;
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and string_val = ''");
};


# Then test that bad fields are properly recognized.

subtest 'bad fields' => sub {

    # First check that bad fields are recognized and that an error is thrown for each one.
    
    my $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    my $key = $edt->insert_record('EDT_TEST', { string_req => 'bad field test',
						abc => 1,
						def => 1 });
    
    ok(!$key, "record with bad fields was not inserted");
    $T->ok_has_error(qr/F_BAD_FIELD.*abc/, "got F_BAD_FIELD for 'abc'");
    $T->ok_has_error(qr/F_BAD_FIELD.*def/, "got F_BAD_FIELD for 'def'");
    
    is($edt->conditions, 2, "got exactly 2 errors");
    
    # Then check that fields beginning with an underscore do not throw an error.
    
    $key = $edt->insert_record('EDT_TEST', { string_req => 'good field test',
					     _abc => 1,
					     _def => 1 });
    
    ok($key, "record with _abc and _def inserted correctly");
    
    # Now create another transaction which allows BAD_FIELDS, and check that insertion happens
    # correctly with warnings.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, BAD_FIELDS => 1 });

    $key = $edt->insert_record('EDT_TEST', { string_req => 'bad field warnings',
					     abc => 1,
					     def => 1 });

    ok($key, "record with bad fields inserted correctly under BAD_FIELDS allowance");
    $T->ok_has_warning(qr/W_BAD_FIELD.*abc/, "got W_BAD_FIELD for 'abc'");
    $T->ok_has_warning(qr/W_BAD_FIELD.*def/, "got W_BAD_FIELD for 'def'");
    
    is($edt->conditions, 2, "got exactly 2 warnings");

    # Check that fields beginning with an underscore do not throw warnings.

    $key = $edt->insert_record('EDT_TEST', { string_req => 'good field warnings',
					     _abc => 1,
					     _def => 1 });

    ok($key, "record with _abc and _def inserted correctly");
    $T->ok_no_warnings('latest');
    $T->ok_no_errors('latest');
};

