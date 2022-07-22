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
use Test::More tests => 4;

use TableDefs qw(get_table_property);

use EditTest;
use EditTester;


$DB::single = 1;

# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new('EditTest');


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


# Create some actions, and check that the routines for querying them give the proper responses.

subtest 'basic' => sub {
    
    my ($edt, $result);

    # Start by creating a transaction, and then some actions. If the transaction cannot be
    # created, abort this test.
    
    $edt = $T->new_edt($perm_a) || return;
    
    # Check that the action routines are all implemented, and that they all return undef since no
    # actions have been added.

    if ( can_ok( $edt, 'has_action', 'action_status', 'action_can_proceed', 'action_table',
		 'action_operation', 'action_internal') )
    {
	is( $edt->has_action, undef, "has_action returns undef with no actions" );
	is( $edt->action_status, undef, "action_status returns undef with no actions" );
	is( $edt->action_can_proceed, undef, "action_can_proceed returns undef with no actions" );
	is( $edt->action_table, undef, "action_table returns undef with no actions" );
	is( $edt->action_operation, undef, "action_operation returns undef with no actions" );
	is( $edt->action_internal, undef, "action_internal returns undef with no actions" );
    }
    
    else
    {
	diag "The rest of this test is skipped.";
	return;
    }
    
    # Add an action, then retrieve a reference to it.
    
    my $a1 = $edt->insert_record('EDT_TEST', { string_req => 'abc', _label => 'a1' });
    
    is( $a1, ':a1', "returned action reference is correct" );
    
    is( $edt->current_action, ':a1', "current_action returns correct reference" );
    
    ok( $edt->has_action, "has_action returns true after insert_record" );
    ok( $edt->action_can_proceed, "action_can_proceed returns true after insert_record" );
    is( $edt->action_status, 'pending', "action_status returns pending after insert_record" );
    is( $edt->action_table, 'EDT_TEST', "action_table returns EDT_TEST after insert_record" );
    is( $edt->action_operation, 'insert', "action_operation returns insert after insert_record" );
    ok( ref $edt->action_internal, "action_internal returns an object ref after insert_record" );
    
    ok( $edt->has_action('latest'), "has_action returns true with latest" );
    ok( $edt->action_can_proceed('latest'), "action_can_proceed returns true with latest" );
    is( $edt->action_status('latest'), 'pending', "action_status returns pending with latest" );
    is( $edt->action_table('latest'), 'EDT_TEST', "action_table returns EDT_TEST with latest" );
    is( $edt->action_operation('latest'), 'insert', "action_operation returns insert with latest" );
    ok( ref $edt->action_internal('latest'), "action_internal returns an object ref with latest" );
    
    my $a2 = $edt->insert_record('EDT_TEST', { string_req => 'def' });
    
    is( $a2, ':#2', "returned action reference is correct" );

    is( $edt->current_action, ':#2', "current_action returns correct reference" );
    
    # Check the has_action method.

    ok( $edt->has_action(':a1'), "has_action returns true for :a1" );
    ok( $edt->has_action(':#1'), "has_action returns true for :#1" );
    ok( $edt->has_action(':#2'), "has_action returns true for :#2" );
    ok( ! $edt->has_action(':foo'), "has_action returns false with nonexistent ref" );
    ok( ! $edt->has_action('***'), "has_action returns false with nonsense argument" );
    ok( $edt->has_action('latest'), "has_action properly returns true with latest" );
    ok( $edt->has_action, "has_action properly returns true with no argument" );

    # Same for the action_status method.
    
    is( $edt->action_status(':a1'), 'pending', "action_status returns pending for :a1" );
    is( $edt->action_status(':#1'), 'pending', "action_status returns pending for :#1" );
    is( $edt->action_status(':#2'), 'pending', "action_status returns pending for :#2" );
    is( $edt->action_status(':#5'), undef, "action_status returns undef with nonexistent ref" );
    is( $edt->action_status('***'), undef, "action_status returns undef with nonsense argument" );
    is( $edt->action_status('latest'), 'pending', "action_status returns pending with latest" );
    is( $edt->action_status, 'pending', "action_status returns pending with no argument" );
    
    # Check the action_keyval and action_keyvals methods.

    my $kv = $edt->action_keyval(':a1');
    my @kv = $edt->action_keyvalues(':a1');
    
    is( $kv, undef, "action_keyval returns undef for :a1" );
    is( @kv, 0, "action_keyvals returns empty list for :a1" );

    # Check action_table and action_operation.

    is( $edt->action_table(':a1'), 'EDT_TEST', "action_table returns proper value for :a1" );
    is( $edt->action_table, 'EDT_TEST', "action_table returns proper value with no argument" );
    
    is( $edt->action_operation(':a1'), 'insert', "action_operation returns proper value for :a1" );
    is( $edt->action_operation, 'insert', "action_operation returns proper value with no argument" );
    
    # Now commit the transaction and check that the status of both actions changes to
    # 'executed'. Also check the response from keyval and keyvalues.

    if ( ok( $edt->commit, "transaction committed" ) )
    {
	is( $edt->has_action, undef, "has_action returns undef with no args after commit" );
	is( $edt->action_status, undef, "action_status returns undef with no args after commit" );
	is( $edt->action_can_proceed, undef, "action_can_proceed returns undef with no args after commit" );
	is( $edt->action_table, undef, "action_table returns undef with no args after commit" );
	is( $edt->action_operation, undef, "action_operation returns undef with no args after commit" );
	
	is( $edt->action_status('latest'), undef, "action_status returns undef with latest after commit" );
	ok( ! $edt->has_action('latest'), "has_action returns false with latest after commit" );
	
	ok( $edt->has_action(':a1'), "has_action with arg true after commit" );
	is( $edt->action_status(':a1'), 'executed', "action_status with arg returns executed after commit" );
	is( $edt->action_can_proceed(':a1'), 0, "action_can_proceed with arg returns 0 after commit" );
	is( $edt->action_table(':a1'), 'EDT_TEST', "action_table with arg returns EDT_TEST after commit" );
	is( $edt->action_operation(':a1'), 'insert', "action_operation with arg returns insert after commit" );

	$kv = $edt->action_keyval(':#2');
	@kv = $edt->action_keyvalues(':#2');

	cmp_ok( $kv, '>', 0, "action_keyval returns a non-zero result for :#2" );
	cmp_ok( $kv[0], '>', 0, "action_keyvalues returns a non-zero element for :#2" );
	is( @kv, 1, "action_keyvalues returns a list of one element for :#2" );
    }

    # Finally, check the action list.

    my @actions = $edt->actions;

    is( @actions, 2, "transaction had two actions" );

};


# Now check the methods of the internal action object class. These should only be used inside
# EditTransaction and its subclasses.

subtest 'internal' => sub {

    my ($edt, $result);

    # Start by creating a transaction, and then some actions. If the transaction cannot be
    # created, abort this test. We set the PROCEED allowance, so that we can include some actions
    # with errors and still commit the transaction.
    
    $edt = $T->new_edt($perm_a, 'PROCEED') || return;
    
    my $ref1 = $edt->insert_record('EDT_TEST', { string_req => 'testit', _label => "  |\n\ré \$" });
    
    is( $ref1, ":  |\n\ré \$", "labels with non-alphabetic characters work fine" );
    
    can_ok($edt, 'action_internal') || return;
    
    my $action1 = $edt->action_internal;
    
    my $action1a = $edt->action_internal($ref1);

    is( $action1, $action1a, "same action object was retrieved twice" );
    
    # Unless we get an object of the proper class, abort this subtest because there's no point in
    # continuing.
    
    isa_ok( $action1, 'EditTransaction::Action' ) || return;
    
    # Check that all of the accessors for EditTransaction::Action work correctly.
    
    if ( can_ok( $action1, 'table', 'operation', 'label', 'action_ref', 'status', 'parent' ) )
    {
	is( $action1->table, 'EDT_TEST', "table" );
	is( $action1->operation, 'insert', "operation" );
	is( ':' . $action1->label, $ref1, "label" );
	is( $action1->action_ref, $ref1, "action_ref" );
	is( $action1->status, '', "status" );
	is( $action1->parent, undef, "parent" );
    }
    
    if ( can_ok( $action1, 'record', 'record_value', 'has_field' ) )
    {	
	my $r1 = $action1->record;
	
	if ( is( ref $r1, 'HASH', "record returns a hash ref" ) )
	{
	    is( $r1->{string_req}, 'testit', "record hash string_req" );
	    is( ':' . $r1->{_label}, $ref1, "record hash _label" );
	    is( keys $r1->%*, 2, "record hash has no additional keys" );
	}
	
	is( ':' . $action1->record_value('_label'), $ref1, "record_value _label" );
	is( $action1->record_value('string_req'), 'testit', "record_value string_req" );
	is( $action1->record_value('xxx'), undef, "record_value with bad field" );
	ok( $action1->has_field('_label'), "has_field _label" );
	ok( $action1->has_field('string_req'), "has_field string_req" );
	ok( ! $action1->has_field('xxx'), "has_field with bad field" );
    }

    if ( can_ok( $action1, 'has_errors', 'has_warnings', 'can_proceed',
		 'has_completed', 'has_executed' ) )
    {
	ok( ! $action1->has_errors, "has_errors" );
	ok( ! $action1->has_warnings, "has_warnings" );
 	ok( $action1->can_proceed, "can_proceed" );
	ok( ! $action1->has_completed, "has_completed" );
	ok( ! $action1->has_executed, "has_executed" );
    }
    
    # if ( can_ok( $action1, 'root', 'is_aux', 'is_multiple', 'action_count' ) )
    # {
    # 	ok( ! $action1->root, "check root" );
    # 	ok( ! $action1->is_aux, "check is_aux" );
    # 	ok( ! $action1->is_multiple, "check is_multiple" );
    # 	is( $action1->action_count, 1, "check multiple_count" );
    # }

    if ( can_ok( $action1, 'permission', 'keycol', 'keyval', 'column_list', 'value_list' ) )
    {
	is( $action1->permission, 'post', "permission" );
	is( $action1->keycol, $primary, "keycol" );
	is( $action1->keyval, undef, "keyval before execution" );
	is( $action1->keyvalues, 0, "keyvalues before execution" );
	
	my $cols = $action1->column_list;
	my $vals = $action1->value_list;
	
	cmp_ok( @$cols, '>', 2, "at least two columns" );
	cmp_ok( @$vals, '>', 2, "at least two values" );
    }
    
    if ( can_ok( $action1, 'get_attr', 'set_attr' ) )
    {
	$action1->set_attr(foo => 'bar');
	is( $action1->get_attr('foo'), 'bar', "set and get" );
	is( $action1->get_attr('xxx'), undef, "get bad attribute" );

	eval {
	    $action1->set_attr;
	};

	ok( $@, "got exception for set_attr with no arguments" );

	eval {
	    $action1->get_attr;
	};

	ok( $@, "got exception for get_attr with no arguments" );
    }
    
    # Add two more records, for later updating.
    
    my $ref2 = $edt->insert_record('EDT_TEST', { string_req => 'test record 2' });
    my $ref3 = $edt->insert_record('EDT_TEST', { string_req => 'test record 3' });
    
    # Add another record with an error and a warning and make sure that the counts are updated.
    
    my $ref4 = $edt->insert_record('EDT_TEST', { string_val => 'invalid record',
						 _errwarn => ['W_TEST'] });

    my $action4 = $edt->action_internal;

    if ( can_ok( $action4, 'has_errors', 'has_warnings', 'can_proceed') )
    {
	is( $action4->has_errors, 1, "counted one error" );
	is( $action4->has_warnings, 1, "counted one warning" );
	
	$edt->add_condition('E_TEST');
	$edt->add_condition('W_EXECUTE');
	
	is( $action4->has_errors, 2, "counted two errors" );
	is( $action4->has_warnings, 2, "counted two warnings" );

	ok( ! $action4->can_proceed, "action cannot proceed" );
    }
    
    # Now add an update action that references the second and third inserts.
    
    my $ref5 = $edt->update_record('EDT_TEST', { string_val => 'this record was updated',
						 _primary => [$ref2] });
    
    my $action5 = $edt->action_internal($ref5);

    if ( can_ok( $action5, 'keyvalues', 'can_proceed', 'has_executed' ) )
    {
	ok( ref $action5->keyval eq 'ARRAY', "keyval returns an array" ) ||
	    diag "keyval returned: " . $action5->keyval;
	
	my @keys = $action5->keyvalues;
	
	is( @keys, 1, "got one key value" );
	is( $keys[0], $ref2, "key matches ref2" );
	ok( $action5->can_proceed, "action5 can proceed" );
	ok( ! $action5->has_executed, "action5 has not executed" );
	# is( @keys, 2, "got two key values" );
	# is( $keys[0], $ref2, "first matches ref2" );
	# is( $keys[1], $ref3, "second matches ref3" );
    }
    
    # Execute the action, check what has changed and what has not.
    
    ok( $edt->commit, "transaction committed" );

    is( $edt->action_count, 5, "action_count" );
    is( $edt->record_count, 5, "record_count" );
    is( $edt->exec_count, 4, "exec_count" );
    is( $edt->skip_count, 0, "skip_count" );
    is( $edt->fail_count, 1, "fail_count" );
    
    is( $action4->has_errors, 2, "action4 still has two errors" );
    is( $action4->has_warnings, 2, "action4 still has two warnings" );
    
    cmp_ok( $action5->keyval, '>', 0, "action5 keyval is now a proper key value" );
    
    # Now check get_attr and set_attr.
    
    is( $action1->get_attr('foo'), 'bar', "attribute value still set" );

    $action1->set_attr('foo', undef);
    is( $action1->get_attr('foo'), undef, "attribute value has been unset" );
};
    

# Test that an attribute set by the before_action method is still available when after_action is
# called.

subtest 'attrs' => sub {
    
    my $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 }) || return;

    $edt->insert_record('EDT_TEST', { string_req => 'before set_attr' });
    
    is( $edt->{save_after_attr}, 'abc',
	"attribute value preserved between before_action and after_action" );
};


# Now check that methods called on 'delete' actions return the proper values. This is necessary
# because those actions in particular can take a single key value instead of a record.

# subtest 'delete' => sub {
    
#     my $edt = $T->new_edt($perm_a) || return;

#     # First try a delete action with a record.
    
#     $edt->delete_record('EDT_TEST', { $primary => 99999, string_req => 'abc' });
    
#     my $a1 = $edt->current_action;
    
#     is( $a1->keyval, 99999, "got proper key value" );
#     is( $a1->keycol, $primary, "got proper key column" );
#     is( $a1->record_value($primary), 99999, "got proper key value from record_value" );
#     is( $a1->has_errors, 1, "action has one error" );
#     $T->ok_has_error( 'E_NOT_FOUND', "found proper error" );
    
#     # Then try a delete action with a record but no primary key.
    
#     $edt->delete_record('EDT_TEST', { string_req => 'abc', _label => 'r1' });
    
#     my $a2 = $edt->current_action;

#     is( $a2->keyval, undef, "got undefined key value" );
#     is( $a2->keycol, $primary, "got proper key column" );
#     is( $a2->has_errors, 1, "action has one error" );
#     $T->ok_has_error( 'E_NO_KEY', "found proper error" );
    
#     # Then try a delete action with a bare key value.

#     $edt->delete_record('EDT_TEST', 99998);

#     my $a3 = $edt->current_action;

#     is( $a3->keyval, 99998, "got proper key value" );
#     is( $a3->keycol, $primary, "got proper key column" );
#     is( $a3->record_value($primary), 99998, "got proper key value from record_value" );
#     is( $a3->has_errors, 1, "action has one error" );
# };


# # Now check that when multiple deletes are coalesced into a single one, the action accessor
# # methods return the proper values.

# subtest 'multiple' => sub {

#     # Clear the table so we can check for proper record insertion.
    
#     $T->clear_table('EDT_TEST');
    
#     # Add some records.

#     my $edt = $T->new_edt($perm_a) || return;

#     $edt->insert_record('EDT_TEST', { string_req => 'abc' });
#     $edt->insert_record('EDT_TEST', { string_req => 'def' });
#     $edt->insert_record('EDT_TEST', { string_req => 'ghi' });

#     $edt->execute;

#     my (@keys) = $edt->inserted_keys;

#     # Now delete those records, with MULTI_DELETE allowed.
    
#     my $edt = $T->new_edt($perm_a, { MULTI_DELETE => 1 }) || return;
    
#     foreach my $k (@keys)
#     {
# 	$edt->delete_record('EDT_TEST', $k);
#     }
    
#     $edt->execute;

#     # Now the last saved action should be the only one executed.

#     my $a1 = $edt->{save_after_action};

#     if ( ok( $a1, "found last action" ) )
#     {
# 	is( $a1->is_multiple, 1, "this action is multiple" );
# 	is( $a1->action_count, 3, "included a total of three action" );
	
# 	my @action_keys = $a1->all_keys;
# 	my @action_labels = $a1->all_labels;
	
# 	is( @action_keys, 3, "action_keys returns three keys" );
# 	is( @action_labels, 3, "action_labels returns three labels" );
	
# 	like( $action_labels[0], qr/^#\d/, "action label starts with '\#'" );
#     }
# };
