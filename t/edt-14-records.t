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
use Test::More tests => 2;

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


# Do some record operations, and make sure that the record counts and labels match up properly.

subtest 'basic' => sub {
    
    my ($edt, $result);
    
    # Start by creating a transaction, and then some actions. If the transaction cannot be
    # created, abort this test.
    
    $edt = $T->new_edt($perm_a) || return;
    
    # Add some records, with and without record labels. Use 'ignore_record' to skip some.
    
    $edt->insert_record($EDT_TEST, { string_req => 'abc', record_label => 'a1' });
    
    is( $edt->current_action && $edt->current_action->label, 'a1',
	"first action has proper label" );

    $edt->add_condition('E_TEST');
    
    $T->ok_has_error( qr/^E_TEST \(a1\):/, "first condition has proper label" );
    
    $edt->insert_record($EDT_TEST, { string_req => 'no label' });

    is( $edt->current_action && $edt->current_action->label, '#2',
	"second action has proper label" );
    
    $edt->ignore_record;
    
    $edt->insert_record($EDT_TEST, { string_req => 'no label' });
    
    is( $edt->current_action && $edt->current_action->label, '#4',
	"third action has proper label" );
    
    $edt->add_condition('W_TEST');
    
    $T->ok_has_warning( qr/^W_TEST \(#4\):/, "second condition has proper label" );

    # Now call 'abort_action' and check that the status has changed.

    my $action = $edt->current_action;
    
    is ( $action && $action->status, '', "record status is empty" );
    
    $edt->abort_action;

    is( $action && $action->status, 'abandoned', "record has been marked as abandoned" );
};

