#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-30-insert.t : Test the operation of the 'insert_records' method.
# 


use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use TableDefs qw(get_table_property %TABLE);

use EditTest;
use EditTester;


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new({ subclass => 'EditTest' });


# Start by getting the variable values that we need to execute the remainder of the test. If the
# session key 'SESSION-AUTHORIZER' does not appear in the session_data table, then run the test
# 'edt-01-basic.t' first.

my ($perm_a, $perm_e, $primary);

subtest 'setup' => sub {
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;
    
    $perm_e = $T->new_perm('SESSION-ENTERER');
    
    ok( $perm_e && $perm_e->role eq 'enterer', "found enterer permission" ) or die;
    
    $primary = get_table_property('EDT_TEST', 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || BAIL_OUT;
};


# Check that insert_records fails properly if the necessary conditions are not there.

subtest 'errors' => sub {
    
    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    
    # Then create an edt without CREATE mode and try to insert.

    $edt = $T->new_edt($perm_a, { CREATE => 0 });
    
    $result = $edt->insert_record('EDT_TEST', { string_req => 'test a1' });

    ok( ! $result, "insert_record returned false without CREATE" );
    $T->ok_has_error( qr/C_CREATE/, "found caution C_CREATE" );
    
    # Check for E_HAS_KEY if a key is given.
    
    $edt = $T->new_edt($perm_a);
    
    $result = $edt->insert_record('EDT_TEST', { string_req => 'test a1', test_no => 1 });
    
    ok( ! $result, "insert_record returned false with test_no specified" );
    $T->ok_has_error( qr/E_HAS_KEY/, "found condition E_HAS_KEY" );

    # Check for the proper error condition if an invalid table is specified.

    eval {
	$edt->insert_record('BAD TABLE', { string_req => 'test_a2' });
    };
    
    ok( $@ && $@ =~ /unknown table/i, "insert_record threw an 'unknown table' exception with bad table name" );

    # Check keys and counts.
    
    cmp_ok( $edt->action_count, '==', 0, "found proper action_count" );
    cmp_ok( $edt->record_count, '==', 1, "found proper record_count" );
    cmp_ok( $edt->fail_count, '==', 1, "fail_count returns 0" );
    cmp_ok( $edt->skip_count, '==', 0, "skip_count returns 0" );
    
    cmp_ok( ($edt->inserted_keys), '==', 0, "inserted_keys returns empty list" );
    cmp_ok( ($edt->updated_keys), '==', 0, "replaced_keys returns empty list" );
    cmp_ok( ($edt->replaced_keys), '==', 0, "replaced_keys returns proper count for EDT_TEST" );
    cmp_ok( ($edt->deleted_keys), '==', 0, "deleted_keys returns empty list" );
    # The following test compares to 1 because a key value was erroneously specified in the first insertion.
    cmp_ok( ( $edt->failed_keys), '==', 1, "failed_keys returns empty list" );
};


# Check the methods available to subclasses, specifically those whose effects vary according to
# the action type.

subtest 'subclass' => sub {
    
    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');

    # Then insert a new record.

    $edt = $T->new_edt($perm_a);

    $result = $edt->insert_record('EDT_TEST', { string_req => 'authorize methods', string_val => 'abc' });

    ok( $result, "insert_record succeeded" );
    
    is( $edt->{save_method_keyexpr}, undef, "get_keyexpr returned undef" );
    ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
	cmp_ok( @{$edt->{save_method_keylist}}, '==', 0, "get_keylist returned empty" );
    ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
	cmp_ok( @{$edt->{save_method_values}}, '==', 0, "get_old_values returned empty" );
    
    $edt = $T->new_edt($perm_a);

    $result = $edt->insert_record('EDT_TEST', { string_req => 'validate methods', string_val => 'abc' });

    is( $edt->{save_method_keyexpr}, undef, "get_keyexpr returned undef" );
    ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
	cmp_ok( @{$edt->{save_method_keylist}}, '==', 0, "get_keylist returned empty" );
    ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
	cmp_ok( @{$edt->{save_method_values}}, '==', 0, "get_old_values returned empty" );

    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $result = $edt->insert_record('EDT_TEST', { string_req => 'before methods', string_val => 'abc' });
    
    is( $edt->{save_method_keyexpr}, undef, "get_keyexpr returned undef" );
    ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
	cmp_ok( @{$edt->{save_method_keylist}}, '==', 0, "get_keylist returned empty" );
    ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
	cmp_ok( @{$edt->{save_method_values}}, '==', 0, "get_old_values returned empty" );
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $result = $edt->insert_record('EDT_TEST', { string_req => 'after methods', string_val => 'abc' });

    like( $edt->{save_method_keyexpr}, qr{test_no='?\d'?}, "get_keyexpr returned a proper value" ) &&
	$edt->{save_method_keyexpr} =~ /test_no='?(\d+)/;
    like( $edt->{save_method_keyval}, qr{^\d+$}, "saved keyval had a proper value" );
    my $val = $1;
    ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
	cmp_ok( @{$edt->{save_method_keylist}}, '==', 1, "get_keylist returned one key" ) &&
	is( $edt->{save_method_keylist}[0], $val, "get_keylist returned the proper key" );
    
};


subtest 'insert with labels' => sub {
    
    my ($edt, $result);
    
    # Clear both tables so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    $T->clear_table('EDT_AUX');
    
    # Then insert a new record.
    
    $edt = $T->new_edt($perm_a);
    
    $result = $edt->insert_record('EDT_TEST', { string_req => 'abc', _label => 'foo' });
    
    ok( $result, "first record valid" ) || $T->diag_errors('latest');
    
    # Now insert two into the auxiliary table, using the label from the first.
    
    $result = $edt->insert_record('EDT_AUX', { name => 'def', test_no => '@foo', _label => 'bar' });
    
    ok( $result, "second record valid" ) || $T->diag_errors('latest');
    
    $result = $edt->insert_record('EDT_AUX', { name => 'validate label foo', test_no => '@foo', _label => 'baz' });
    
    ok( $result, "third record valid") || $T->diag_errors('latest');
    
    is( $edt->{save_validate_label}, 'EDT_TEST', "method 'label_table' works from validation" );
    
    # Then execute this transaction, and check that the three records were properly linked up.
    
    $T->ok_result( $edt->execute, "transaction executed successfully" ) || return;
    
    my ($count) = $edt->dbh->selectrow_array("
		SELECT count(*) FROM $TABLE{EDT_TEST} join $TABLE{EDT_AUX} using (test_no)");
    
    cmp_ok( $count, '==', 2, "found two joined table rows" );
    
    # Check keys and counts.

    cmp_ok( $edt->record_count, '==', 3, "record_count returns proper value" );
    cmp_ok( $edt->action_count, '==', 3, "action_count returns proper value" );
    cmp_ok( $edt->fail_count, '==', 0, "fail_count returns 0" );
    cmp_ok( $edt->skip_count, '==', 0, "skip_count returns 0" );
    
    my @inserted_test = $edt->inserted_keys('EDT_TEST');
    my @inserted_aux = $edt->inserted_keys('EDT_AUX');
    my @inserted_all = $edt->inserted_keys;
    my @updated_keys = $edt->updated_keys('EDT_TEST');
    my @replaced_keys = $edt->replaced_keys('EDT_TEST');
    my @deleted_keys = $edt->deleted_keys('EDT_TEST');
    my @failed_keys = $edt->failed_keys('EDT_TEST');
    my @failed_all = $edt->failed_keys;
    
    cmp_ok( @inserted_test, '==', 1, "proper count of inserted keys from EDT_TEST" );
    cmp_ok( @inserted_aux, '==', 2, "proper count of inserted keys from EDT_AUX" );
    cmp_ok( @inserted_all, '==', 3, "proper count of inserted keys from all tables" );
    cmp_ok( @updated_keys, '==', 0, "updated_keys returns empty list" );
    cmp_ok( @replaced_keys, '==', 0, "updated_keys returns empty list" );
    cmp_ok( @deleted_keys, '==', 0, "updated_keys returns empty list" );
    cmp_ok( @failed_keys, '==', 0, "failed_keys returns empty list" );
    cmp_ok( @failed_all, '==', 0, "failed_keys returns empty list from all tables" );
    
    # Check that the inserted key lists match up.
    
    my %check_1 = map { $_ => 1 } @inserted_all;
    my %check_2 = map { $_ => 1 } @inserted_test, @inserted_aux;
    
    foreach my $key ( keys %check_1 )
    {
	fail("could not match key '$key' in table-specific list") unless exists $check_2{$key};
    }
    
    foreach my $key ( keys %check_2 )
    {
	fail("could not match key '$key' in all-key list") unless exists $check_1{$key};
    }
    
    # Now make sure that keys and labels are properly linked up.
    
    my $test_labels = $edt->key_labels('EDT_TEST');
    my $aux_labels = $edt->key_labels('EDT_AUX');
    my $label_keys = $edt->label_keys;
    
    if ( ok( $test_labels && ref $test_labels eq 'HASH', "key_labels from EDT_TEST returns a hash" ) )
    {
	my @values = values %$test_labels;
	cmp_ok( @values, '==', 1, "one label from EDT_TEST" );
	is( $values[0], 'foo', "found label 'foo' from EDT_TEST" );
    }
    
    if ( ok( $aux_labels && ref $aux_labels eq 'HASH', "key_labels from EDT_AUX returns a hash" ) )
    {
	my @values = values %$aux_labels;
	cmp_ok( @values, '==', 2, "two labels from EDT_AUX" );
	my %found = map { $_ => 1 } @values;
	ok( $found{bar}, "found label 'bar' from EDT_AUX" );
	ok( $found{baz}, "found label 'baz' from EDT_AUX" );
    }

    if ( ok( $label_keys && ref $label_keys eq 'HASH', "label_keys returns a hash" ) )
    {
	cmp_ok( keys %$label_keys, '==', 3, "label_keys returns proper number of entries" );
	is( $label_keys->{baz}, $inserted_aux[1], "found key corresponding to label 'baz'" );
	is( $edt->label_key('baz'), $inserted_aux[1], "label_key returned proper key for 'baz'" );
    }

    # Finally, make sure that the records properly made it into EDT_AUX with the proper test_no
    # value.

    if ( $inserted_test[0] )
    {
	my $insert_count = $T->count_records('EDT_AUX', "test_no=$inserted_test[0]");

	is($insert_count, '2', "found two inserted records in EDT_AUX");
    }
};


# Now check that a series of insert statements that cause an error will actually fail.

subtest 'execution errors' => sub {
    
    my ($edt, $result);
    
    # Clear both tables so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    $T->clear_table('EDT_AUX');
    
    # Then insert some records that will cause an index conflict.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, SILENT_MODE => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'test b1', _label => 'b1' });
    $edt->insert_record('EDT_AUX', { name => 'abc', test_no => '@b1' });

    $T->ok_no_errors("no errors on initial inserts");
    
    $edt->insert_record('EDT_AUX', { name => 'abc', test_no => '@b1' });

    $T->ok_has_error('E_DUPLICATE', "got duplicate key value error");
    $T->ok_has_error(qr{'abc'.*'name'}, "error contained proper value and key name");

    # Now try to insert a record into EDT_TEST in a way that will fail.

    $edt->insert_record('EDT_TEST', { name => 'def' });
    
    # Check that the action_count and fail_count are both 2, and that 2 records were inserted.

    is( $edt->action_count, 2, "action_count returns proper value." );
    is( ($edt->inserted_keys), 2, "inserted_keys returns proper number of entries" );
    is( $edt->fail_count, 2, "fail_count returns proper value." );
};


