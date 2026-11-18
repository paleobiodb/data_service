#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
#
# edt-35-action-object.t : Test the methods of EditTransaction::Action objects that are
#                          provided for use by subclasses: the record, the old record,
#                          attributes, directives, flags, validate_later, and the
#                          condition counts of child actions.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 11;

use ETBasicTest;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_no_conditions ok_exception
		  ok_has_condition clear_table sql_command ok_found_record ok_no_record);


# Establish an EditTester instance.

my $T = EditTester->new('ETBasicTest');


# Clear EDT_TEST and add a record with a known key.

sub reset_test_record {
    
    clear_table('EDT_TEST');
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req) VALUES (5, 'five')");
}


# Return the action object for the specified reference in the specified transaction.

sub action_of {
    
    my ($edt, $ref) = @_;
    
    return $edt->action_ref($ref || '&_');
}


# Check that the necessary methods are defined.

subtest 'required methods' => sub {
    
    can_ok('EditTransaction::Action', 'record', 'record_value', 'field_specified',
	   'set_record_value', 'delete_record_value', 'record_value_alt', 'set_old_record',
	   'old_record', 'old_record_value', 'set_attr', 'get_attr', 'set_attr_key',
	   'get_attr_key', 'handle_column', 'ignore_field', 'directive', 'all_directives',
	   'validate_admin', 'requires_unlock', 'row_count', 'validate_later', 'pin_errors')
	
	|| BAIL_OUT "EditTransaction::Action is missing some required methods";
};


# Check the methods for reading and changing the action record.

subtest 'record values' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'abc', string_val => undef, signed_val => 3 });
    
    my $action = action_of($edt);
    
    is( $action->record_value('string_req'), 'abc', "record_value" );
    is( $action->record_value('no_such_field'), undef, "record_value of a missing field" );
    
    ok( $action->field_specified('string_val'), "field_specified is true for a field with an undefined value" );
    ok( ! $action->field_specified('no_such_field'), "field_specified is false for a missing field" );
    
    $action->set_record_value('string_val', 'set later');
    
    is( $action->record->{string_val}, 'set later', "set_record_value changes the record" );
    
    $action->delete_record_value('signed_val');
    
    ok( ! exists $action->record->{signed_val}, "delete_record_value removes the field" );
    
    is( $action->record_value_alt('no_such_field', 'string_val', 'string_req'), 'set later',
	"record_value_alt returns the first defined value" );
    
    is( $action->record_value_alt('no_such_field'), undef,
	"record_value_alt returns undef if no field has a value" );
    
    my $other = EditTransaction::Action->new($edt, 'EDT_TEST', 'skip', 'x1', 'not a hash');
    
    is( $other->record_value('a'), undef, "record_value with a record that is not a hash" );
    ok( ! $other->field_specified('a'), "field_specified with a record that is not a hash" );
    is( $other->set_record_value('a', 1), undef, "set_record_value with a record that is not a hash" );
    is( $other->record_value_alt('a'), undef, "record_value_alt with a record that is not a hash" );
};


# Check the old record, which holds the column values fetched before an action is executed.

subtest 'old record' => sub {
    
    reset_test_record();
    
    my $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { test_no => 5, string_req => 'new value' });
    
    my $action = action_of($edt);
    
    is( $action->old_record, undef, "no old record before it is fetched" );
    is( $action->old_record_value('string_req'), undef, "no old record value before it is fetched" );
    
    my $old = $edt->fetch_old_record($action);
    
    is( $old->{string_req}, 'five', "fetch_old_record returns the current values" );
    is( $action->old_record_value('string_req'), 'five', "old_record_value" );
    is( $edt->fetch_old_record($action), $old, "a second fetch returns the same record" );
    
    $action->set_old_record({ string_req => 'replaced' });
    
    is( $action->old_record_value('string_req'), 'replaced', "set_old_record" );
};


# Check the methods for attaching arbitrary attributes to an action.

subtest 'attributes' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'attrs' });
    
    my $action = action_of($edt);
    
    is( $action->set_attr('color', 'blue'), 'blue', "set_attr returns the value" );
    is( $action->get_attr('color'), 'blue', "get_attr" );
    is( $action->get_attr('no_such_attr'), undef, "get_attr of a missing attribute" );
    
    $action->set_attr_key('sizes', 'small', 1);
    $action->set_attr_key('sizes', 'large', 3);
    
    is( $action->get_attr_key('sizes', 'large'), 3, "set_attr_key and get_attr_key" );
    is( $action->get_attr_key('no_such_attr', 'large'), undef, "get_attr_key of a missing attribute" );
    
    ok_exception( sub { $action->set_attr('', 1) }, qr/attribute name/, "set_attr requires a name" );
    ok_exception( sub { $action->get_attr() }, qr/attribute name/, "get_attr requires a name" );
    ok_exception( sub { $action->set_attr_key('sizes', '', 1) }, qr/specify a key/,
		  "set_attr_key requires a key" );
    ok_exception( sub { $action->get_attr_key('color', 'x') }, qr/is not a hash/,
		  "get_attr_key on an attribute that is not a hash" );
    
    ok_exception( sub { $action->set_attr_key('color', 'x', 1) }, qr/is not a hash/,
		  "set_attr_key on an attribute that is not a hash" );
};


# Check the methods for setting directives on a single action.

subtest 'action directives' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'directives' });
    
    my $action = action_of($edt);
    
    $action->handle_column('string_val', 'ignore');
    
    is( $action->directive('string_val'), 'ignore', "handle_column and directive" );
    is( $action->directive('string_req'), undef, "directive for a column without one" );
    
    $action->ignore_field('extra_field');
    
    is( $action->directive('FIELD:extra_field'), 'ignore', "ignore_field" );
    
    is_deeply( { $action->all_directives }, { string_val => 'ignore', 'FIELD:extra_field' => 'ignore' },
	       "all_directives" );
    
    ok_exception( sub { $action->handle_column('', 'ignore') }, qr/column name/,
		  "handle_column requires a column name" );
    ok_exception( sub { $action->ignore_field('') }, qr/field name/,
		  "ignore_field requires a field name" );
    
    # A field ignored by initialize_action, before validation, does not cause an error.
    
    no warnings 'redefine';
    
    local *ETBasicTest::initialize_action = sub {
	my ($edt, $action) = @_;
	$action->ignore_field('extra_field');
    };
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'extra', extra_field => 'not a column' });
    
    ok_no_conditions('&_', "a field ignored before validation is not reported");
};


# Check the flags and simple accessors.

subtest 'flags and accessors' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { _primary => '5,6', string_req => 'flags' });
    
    my $action = action_of($edt);
    
    is( $action->table, 'EDT_TEST', "table" );
    is( $action->operation, 'update', "operation" );
    is( $action->refstring, '&' . $action->label, "refstring" );
    is( $action->keylist, "'5','6'", "keylist" );
    ok( $action->keymult, "keymult is true for more than one key" );
    
    is( $action->validate_admin, '', "validate_admin is initially false" );
    is( $action->validate_admin(1), 1, "validate_admin can be set" );
    
    is( $action->requires_unlock, '', "requires_unlock is initially false" );
    $action->requires_unlock(1);
    is( $action->requires_unlock, 1, "requires_unlock can be set" );
    $action->requires_unlock(0);
    is( $action->requires_unlock, '', "requires_unlock can be cleared" );
    
    is( $action->row_count, '', "row_count is initially empty" );
    is( $action->row_count(2), 2, "row_count can be set" );
    
    is( $action->status('custom'), 'custom', "status can be set" );
    is( $action->table('EDT_SUB'), 'EDT_SUB', "table can be set" );
    is( $action->permission('granted'), 'granted', "permission can be set" );
};


# Check the warning counts of a parent action with a child action.

subtest 'child warnings' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'parent' });
    
    my $parent = action_of($edt);
    
    my $child_ref = $edt->add_child_action('insert', 'EDT_TEST', { string_req => 'child' });
    my $child = $edt->action_ref($child_ref);
    
    $edt->add_condition($child_ref, 'W_TEST', 'child warning');
    
    is( $child->has_warnings, 1, "child has the warning" );
    is( $parent->has_warnings, 1, "parent counts the child's warning" );
    
    $edt->abort_action($child_ref);
    
    is( $parent->has_warnings, 0, "aborting the child removes its warning from the parent" );
    
    ok_exception( sub { $parent->add_child('not an action') }, qr/invalid action reference/,
		  "add_child requires an action" );
};


# Check validate_later, which a subclass calls from validate_action to put off validation
# until the action is executed.

subtest 'validate_later' => sub {
    
    clear_table('EDT_TEST');
    
    my $original = ETBasicTest->can('validate_action');
    my @calls;
    
    no warnings 'redefine';
    
    local *ETBasicTest::validate_action = sub {
	my ($edt, $action) = @_;
	push @calls, $action->validation_status || 'none';
	return $action->validate_later if $action->record_value('_later') && @calls == 1;
	return $original->(@_);
    };
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'validated later', _later => 1 });
    
    is( action_of($edt)->validation_status, 'PENDING', "validation is put off" );
    
    ok_commit;
    
    is( scalar(@calls), 2, "validate_action is called again at execution" );
    
    ok_found_record('EDT_TEST', "string_req = 'validated later'", "the record was stored");
};


# Check that an exception after an action's statement has been executed is fatal, even
# with PROCEED, because the statement cannot be undone on its own.

subtest 'errors after execution' => sub {
    
    clear_table('EDT_TEST');
    
    my $edt = ok_new_edt('PROCEED');
    
    $edt->insert_record('EDT_TEST', { string_req => 'after exception' });
    $edt->insert_record('EDT_TEST', { string_req => 'second' });
    
    ok_failed_commit("an exception from after_action fails the transaction, even with PROCEED");
    
    ok_no_record('EDT_TEST', "string_req = 'after exception'",
		 "the record whose after_action failed is not stored");
};


# Check pin_errors directly.

subtest 'pin_errors' => sub {
    
    my $edt = ok_new_edt('PROCEED');
    
    $edt->insert_record('EDT_TEST', { string_req => 'pinned' });
    
    my $action = action_of($edt);
    
    $action->pin_errors;
    
    $edt->add_condition('&_', 'E_TEST', 'pinned error');
    
    is( scalar($edt->fatals), 1, "an error added after pin_errors is fatal even with PROCEED" );
};


# Check the attribute methods of the transaction itself, which have the same structure as
# those of an action.

subtest 'transaction attributes' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->set_attr('color', 'blue');
    
    is( $edt->get_attr('color'), 'blue', "set_attr and get_attr" );
    
    $edt->set_attr_key('sizes', 'small', 1);
    $edt->set_attr_key('sizes', 'large', 3);
    
    is( $edt->get_attr_key('sizes', 'small'), 1, "set_attr_key and get_attr_key" );
    is_deeply( [ sort $edt->get_attr_keys('sizes') ], [ 'large', 'small' ], "get_attr_keys" );
    is_deeply( { $edt->get_attr_hash('sizes') }, { small => 1, large => 3 }, "get_attr_hash" );
    
    $edt->delete_attr_key('sizes', 'small');
    
    is_deeply( [ $edt->get_attr_keys('sizes') ], [ 'large' ], "delete_attr_key" );
    
    $edt->set_attr_2key('grid', 'a', 'b', 'ab');
    
    is( $edt->get_attr_2key('grid', 'a', 'b'), 'ab', "set_attr_2key and get_attr_2key" );
    
    is( $edt->get_attr_key('no_such_attr', 'x'), undef, "get_attr_key of a missing attribute" );
    is( $edt->get_attr_2key('no_such_attr', 'x', 'y'), undef, "get_attr_2key of a missing attribute" );
    is_deeply( [ $edt->get_attr_keys('no_such_attr') ], [ ], "get_attr_keys of a missing attribute" );
    is_deeply( [ $edt->get_attr_hash('no_such_attr') ], [ ], "get_attr_hash of a missing attribute" );
    
    ok_exception( sub { $edt->set_attr_key('color', 'x', 1) }, qr/is not a hash/,
		  "set_attr_key on an attribute that is not a hash" );
    ok_exception( sub { $edt->set_attr_2key('color', 'x', 'y', 1) }, qr/is not a hash/,
		  "set_attr_2key on an attribute that is not a hash" );
    ok_exception( sub { $edt->get_attr_keys('color') }, qr/is not a hash/,
		  "get_attr_keys on an attribute that is not a hash" );
    ok_exception( sub { $edt->delete_attr_key('color', 'x') }, qr/is not a hash/,
		  "delete_attr_key on an attribute that is not a hash" );
    ok_exception( sub { $edt->set_attr_2key('grid', 'a', '', 1) }, qr/two keys/,
		  "set_attr_2key requires two keys" );
};
