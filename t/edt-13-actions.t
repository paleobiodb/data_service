#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-13-actions.t : Test the auxiliary class EditTransaction::Action. Make
# sure that it has the proper methods, and that they return the proper values.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

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

    # Start by creating a transaction, and then some actions. If the transaction cannot be
    # created, abort this test.
    
    $edt = $T->new_edt($perm_a) || return;
    
    # Add an action, then retrieve a reference to it.
    
    $edt->insert_record($EDT_TEST, { string_req => 'abc', record_label => 'a1' });
    
    my $a1 = $edt->current_action;
    
    # Unless we get an object of the proper class, abort this subtest because there's no point in
    # continuing.
    
    isa_ok( $a1, 'EditTransaction::Action', "action has the proper class" ) || return;
    
    # Check that all of the accessors for EditTransaction::Action work correctly.
    
    if ( can_ok( $a1, 'table', 'operation', 'label' ) )
    {
	is( $a1->table, $EDT_TEST, "check table" );
	is( $a1->operation, 'insert', "check operation" );
	is( $a1->label, 'a1', "check label" );
    }

    if ( can_ok( $a1, 'record', 'record_value', 'has_field' ) )
    {	
	my $r1 = $a1->record;
	
	is( ref $r1, 'HASH', "record returns a hash ref" ) &&
	    is( $r1->{string_req}, 'abc', "record returns proper hash" );
	is( $a1->record_value('record_label'), 'a1', "check record_value" );
	is( $a1->record_value('xxx'), undef, "check record_value with bad field" );
	ok( $a1->has_field('record_label'), "check has_field" );
	ok( ! $a1->has_field('xxx'), "check has_field with bad field" );
    }

    if ( can_ok( $a1, 'root', 'is_aux', 'is_multiple', 'action_count' ) )
    {
	ok( ! $a1->root, "check root" );
	ok( ! $a1->is_aux, "check is_aux" );
	ok( ! $a1->is_multiple, "check is_multiple" );
	is( $a1->action_count, 1, "check multiple_count" );
    }

    if ( can_ok( $a1, 'permission', 'keycol', 'keyval', 'has_errors', 'has_warnings' ) )
    {
	is( $a1->permission, 'post', "check permission" );
	is( $a1->keycol, $primary, "check keycol" );
	is( $a1->keyval, undef, "check keyval before execution" );
	
	ok( ! $a1->has_errors, "check has_errors" );
	ok( ! $a1->has_warnings, "check has_warnings" );
    }

    if ( can_ok( $a1, 'column_list', 'value_list' ) )
    {
	my $cols = $a1->column_list;
	my $vals = $a1->value_list;
	
	cmp_ok( @$cols, '>', 2, "at least two columns" );
	cmp_ok( @$vals, '>', 2, "at least two values" );
    }

    if ( can_ok( $a1, 'get_attr', 'set_attr' ) )
    {
	$a1->set_attr(foo => 'bar');
	is( $a1->get_attr('foo'), 'bar', "set and get" );
	is( $a1->get_attr('xxx'), undef, "get undefined" );
    }

    # Now add an error and a warning, and make sure that the counts are updated.
    
    $edt->add_condition('E_TEST');
    $edt->add_condition('E_PERM');
    $edt->add_condition('W_TEST');
    $edt->add_condition('W_EXECUTE');
    
    is( $a1->has_errors, 2, "counted two errors" );
    is( $a1->has_warnings, 2, "counted two warnings" );

    # Execute the action, ane make sure the important stuff hasn't changed.

    $edt->execute;
    
    is( $a1->has_errors, 2, "still counted two errors" );
    is( $a1->has_warnings, 2, "still counted two warnings" );
    
    # Specifically bump up the error and warning counts.
    
    $a1->add_error;
    is( $a1->has_errors, 3, "error count is now three" );
    
    $a1->add_warning;
    is( $a1->has_warnings, 3, "warning count is now three" );
    
    # Now check get_attr and set_attr.
    
    is( $a1->get_attr('foo'), 'bar', "attribute value still set" );

    $a1->set_attr('foo', undef);
    is( $a1->get_attr('foo'), undef, "attribute value has been unset" );

    eval {
	$a1->set_attr(undef, 1);
    };

    ok( $@, "exception thrown by set_attr with undefined attribute name" );

    eval {
	$a1->get_attr();
    };

    ok( $@, "exception thrown by get_attr with undefined attribute name" );
};
    

# Test that an attribute set by the before_action method is still available when after_action is
# called.

subtest 'attrs' => sub {
    
    my $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 }) || return;

    $edt->insert_record($EDT_TEST, { string_req => 'before set_attr' });
    
    is( $edt->{save_after_attr}, 'abc',
	"attribute value preserved between before_action and after_action" );
};


# Now check that methods called on 'delete' actions return the proper values. This is necessary
# because those actions in particular can take a single key value instead of a record.

subtest 'delete' => sub {
    
    my $edt = $T->new_edt($perm_a) || return;

    # First try a delete action with a record.
    
    $edt->delete_record($EDT_TEST, { $primary => 99999, string_req => 'abc' });
    
    my $a1 = $edt->current_action;
    
    is( $a1->keyval, 99999, "got proper key value" );
    is( $a1->keycol, $primary, "got proper key column" );
    is( $a1->record_value($primary), 99999, "got proper key value from record_value" );
    is( $a1->has_errors, 1, "action has one error" );
    $T->ok_has_error( qr/E_NOT_FOUND/, "found proper error" );
    
    # Then try a delete action with a record but no primary key.
    
    $edt->delete_record($EDT_TEST, { string_req => 'abc', record_label => 'r1' });
    
    my $a2 = $edt->current_action;

    is( $a2->keyval, undef, "got undefined key value" );
    is( $a2->keycol, $primary, "got proper key column" );
    is( $a2->has_errors, 1, "action has one error" );
    $T->ok_has_error( qr/E_NO_KEY/, "found proper error" );
    
    # Then try a delete action with a bare key value.

    $edt->delete_record($EDT_TEST, 99998);

    my $a3 = $edt->current_action;

    is( $a3->keyval, 99998, "got proper key value" );
    is( $a3->keycol, $primary, "got proper key column" );
    is( $a3->record_value($primary), 99998, "got proper key value from record_value" );
    is( $a3->has_errors, 1, "action has one error" );
};


# Now check that when multiple deletes are coalesced into a single one, the action accessor
# methods return the proper values.

subtest 'multiple' => sub {

    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table($EDT_TEST);
    
    # Add some records.

    my $edt = $T->new_edt($perm_a) || return;

    $edt->insert_record($EDT_TEST, { string_req => 'abc' });
    $edt->insert_record($EDT_TEST, { string_req => 'def' });
    $edt->insert_record($EDT_TEST, { string_req => 'ghi' });

    $edt->execute;

    my (@keys) = $edt->inserted_keys;

    # Now delete those records, with MULTI_DELETE allowed.
    
    my $edt = $T->new_edt($perm_a, { MULTI_DELETE => 1 }) || return;
    
    foreach my $k (@keys)
    {
	$edt->delete_record($EDT_TEST, $k);
    }
    
    $edt->execute;

    # Now the last saved action should be the only one executed.

    my $a1 = $edt->{save_after_action};

    if ( ok( $a1, "found last action" ) )
    {
	is( $a1->is_multiple, 1, "this action is multiple" );
	is( $a1->action_count, 3, "included a total of three action" );
	
	my @action_keys = $a1->all_keys;
	my @action_labels = $a1->all_labels;
	
	is( @action_keys, 3, "action_keys returns three keys" );
	is( @action_labels, 3, "action_labels returns three labels" );
	
	like( $action_labels[0], qr/^#\d/, "action label starts with '\#'" );
    }
};
