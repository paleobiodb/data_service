#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-31-update.t : Test the operation of the 'update_records' method.
# 


use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use TableDefs qw(get_table_property %TABLE);

use EditTest;
use EditTester;


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;


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


# Check that update_records works.

subtest 'basic' => sub {
    
    my ($edt, $result);
    
    # Clear the table and then insert some test records.
    
    $T->clear_table('EDT_TEST');
    $T->clear_table('EDT_AUX');
    
    $edt = $T->new_edt($perm_a);
    
    $edt->insert_record('EDT_TEST', { string_req => 'test a1', _label => 'a1' });
    $edt->insert_record('EDT_TEST', { string_req => 'test a2', _label => 'a2' });
    $edt->insert_record('EDT_TEST', { string_req => 'test a3', _label => 'a3' });
    
    $edt->insert_record('EDT_AUX', { name => 'abc', test_no => '@a1' });
    $edt->insert_record('EDT_AUX', { name => 'def', test_no => '@a2' });
    $edt->insert_record('EDT_AUX', { name => 'ghi', test_no => '@a3' });
    
    $T->ok_result( $edt->execute, "insertion executed successfully" ) || return;
    
    my @test_keys = $edt->inserted_keys('EDT_TEST');
    my @aux_keys = $edt->inserted_keys('EDT_AUX');
    
    my ($count) = $edt->dbh->selectrow_array("
		SELECT count(*) FROM $TABLE{EDT_TEST} join $TABLE{EDT_AUX} using (test_no)");
    
    cmp_ok( $count, '==', 3, "found three joined table rows" );
    
    # Now try some updates. Check for E_NO_KEY if no key is given.
    
    $edt = $T->new_edt($perm_a);
    
    $result = $edt->update_record('EDT_TEST', { string_req => 'test a1' });
    
    ok( ! $result, "update_record returned false with test_no missing" );
    $T->ok_has_error( qr/E_NO_KEY/, "found condition E_NO_KEY" );
    
    $T->ok_found_record('EDT_AUX', "name = 'ghi'", "found record with name 'ghi'");
    
    $result = $edt->update_record('EDT_TEST', { string_req => 'test a2', test_no => '9999' });

    ok( ! $result, "update_record returned false with test_no not found" );
    $T->ok_has_error( qr/E_NOT_FOUND/, "found condition E_NOT_FOUND" );
    
    # Now try an update transaction, with record keys but without the CREATE allowance (to check
    # that it is not required).
    
    $edt = $T->new_edt($perm_a, { CREATE => 0 });
    
    $result = $edt->update_record('EDT_TEST', { test_no => $test_keys[0], string_req => 'test x1', _label => 'x1' }); 
    
    ok( $result, "first update was valid" );
    
    $result = $edt->update_record('EDT_AUX', { aux_no => $aux_keys[1], test_no => '@x1' });
    
    ok( $result, "second update was valid" );

    $result = $edt->update_record('EDT_AUX', { aux_no => $aux_keys[2], name => 'updated' });
    
    ok( $result, "third update was valid" );
    
    $T->ok_result( $edt->execute, "update executed successfully" );
    
    my ($count1) = $edt->dbh->selectrow_array("
		SELECT count(*) FROM $TABLE{EDT_AUX} WHERE test_no = $test_keys[0]");

    cmp_ok( $count1, '==', 2, "found two records pointing to first test record" );    
    
    $T->ok_found_record('EDT_AUX', "name = 'updated'", "found record with updated name");
    $T->ok_no_record('EDT_AUX', "name = 'ghi'", "no record with name 'ghi'");
    
    # Check keys and counts.
    
    cmp_ok( $edt->action_count, '==', 3, "found proper action_count" );
    cmp_ok( $edt->record_count, '==', 3, "found proper record_count" );
    cmp_ok( $edt->fail_count, '==', 0, "fail_count returns 0" );
    cmp_ok( $edt->skip_count, '==', 0, "skip_count returns 0" );
    
    cmp_ok( ($edt->inserted_keys), '==', 0, "inserted_keys returns empty list" );
    cmp_ok( ($edt->updated_keys('EDT_TEST')), '==', 1, "updated_keys returns proper count for EDT_TEST" );
    cmp_ok( ($edt->updated_keys('EDT_AUX')), '==', 2, "updated_keys returns proper count for EDT_AUX" );
    cmp_ok( ($edt->updated_keys), '==', 3, "updated_keys returns proper count for all tables" );
    cmp_ok( ($edt->replaced_keys), '==', 0, "replaced_keys returns empty list" );
    cmp_ok( ($edt->deleted_keys), '==', 0, "deleted_keys returns empty list" );
    cmp_ok( ($edt->failed_keys), '==', 0, "failed_keys returns empty list" );
    
    # Check that the updated key lists match up.
    
    my %check_1 = map { $_ => 1 } $edt->updated_keys;
    my %check_2 = map { $_ => 1 } $edt->updated_keys('EDT_TEST'), $edt->updated_keys('EDT_AUX');
    
    foreach my $key ( keys %check_1 )
    {
	fail("could not match key '$key' in table-specific list") unless exists $check_2{$key};
    }
    
    foreach my $key ( keys %check_2 )
    {
	fail("could not match key '$key' in all-key list") unless exists $check_1{$key};
    }
    
    # Now make sure that keys and labels are properly linked up.
    
    my $check_label_key = $edt->label_key('x1');
    
    is( $check_label_key, $test_keys[0], "label_key returned proper value for 'x1'" );
    
    my $test_labels = $edt->key_labels('EDT_TEST');
    my $aux_labels = $edt->key_labels('EDT_AUX');
    my $bad_keys = $edt->key_labels('EDT_ANY');
    my $label_keys = $edt->label_keys;
    
    if ( ok( $test_labels && ref $test_labels eq 'HASH', "key_labels from EDT_TEST returns a hash" ) )
    {
	my @values = values %$test_labels;
	cmp_ok( @values, '==', 1, "one label from EDT_TEST" );
	is( $values[0], 'x1', "found label 'x1' from EDT_TEST" );
    }
    
    if ( ok( $aux_labels && ref $aux_labels eq 'HASH', "key_labels from EDT_AUX returns a hash" ) )
    {
	cmp_ok( keys %$aux_labels, '==', 2, "two labels from EDT_AUX" );
    }
    
    if ( ok( $label_keys && ref $label_keys eq 'HASH', "label_keys returns a hash" ) )
    {
	cmp_ok( keys %$label_keys, '==', 3, "label_keys returns proper number of entries" );
	is( $label_keys->{x1}, $test_keys[0], "found key corresponding to label 'x1'" );
	is( $label_keys->{'#2'}, $aux_keys[1], "found key corresponding to label '#2'" );
	is( $label_keys->{'#3'}, $aux_keys[2], "found key corresponding to label '#3'" );
    }

    is( $bad_keys, undef, "label_keys returns undefined for unused table name" );
};


# Check that various errors are reported properly.

subtest 'bad' => sub {
    
    my ($edt, $result);
    
    $edt = $T->new_edt($perm_a);
    
    # Check for the proper error condition if an invalid table is specified.
    
    eval {
	$edt->update_record('BAD TABLE', { test_no => '1', string_req => 'test x2' });
    };
    
    ok( $@ && $@ =~ /unknown table/i, "update_record threw an 'unknown table' exception with bad table name" );
};


# Check the methods available to subclasses, specifically those whose effects vary according to
# the action type.

subtest 'subclass' => sub {
    
    my ($edt, $result);
    
    # Clear the tables so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    $T->clear_table('EDT_AUX');

    # Then insert some records.
    
    $edt = $T->new_edt($perm_a);
    
    $edt->insert_record('EDT_TEST', { string_req => 'test a1', _label => 'a1' });
    $edt->insert_record('EDT_TEST', { string_req => 'test a2', _label => 'a2' });
    $edt->insert_record('EDT_TEST', { string_req => 'test a3', _label => 'a3' });
    
    $edt->insert_record('EDT_AUX', { name => 'abc', test_no => '@a1' });
    $edt->insert_record('EDT_AUX', { name => 'def', test_no => '@a2' });
    $edt->insert_record('EDT_AUX', { name => 'ghi', test_no => '@a3' });
    
    $T->ok_result( $edt->execute, "insertion executed successfully" ) || return;

    my (@keys) = $edt->inserted_keys('EDT_TEST');
    
    # Now do some updates.
    
    $edt = $T->new_edt($perm_a);
    
    $result = $edt->update_record('EDT_TEST', { test_no => $keys[0], string_req => 'authorize methods',
						string_val => 'abc' });
    
    ok( $result, "update_record succeeded" );
    
    like( $edt->{save_method_keyexpr}, qr{^test_no='?$keys[0]'?}, "get_keyexpr returned proper value" );
    ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
	cmp_ok( @{$edt->{save_method_keylist}}, '==', 1, "get_keylist returned one element" );
    ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
	cmp_ok( @{$edt->{save_method_values}}, '==', 2, "get_old_values returned one element" ) &&
	is( $edt->{save_method_values}[0], 'test a1', "get_old_values returned proper value" );

    $result = $edt->update_record('EDT_TEST', { test_no => $keys[0], string_req => 'validate methods',
						string_val => 'def' });
    
    ok( $result, "update_record succeeded" );
    
    like( $edt->{save_method_keyexpr}, qr{^test_no='?$keys[0]'?}, "get_keyexpr returned proper value" );
    ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
	cmp_ok( @{$edt->{save_method_keylist}}, '==', 1, "get_keylist returned one element" );
    ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
	cmp_ok( @{$edt->{save_method_values}}, '==', 2, "get_old_values returned one element" ) &&
	is( $edt->{save_method_values}[0], 'test a1', "get_old_values returned proper value" );
    
    # Now start execution, so that we can test that the result of get_old_values reflects the updates that
    # have been processed so far.

    $edt->start_execution;
    
    $result = $edt->update_record('EDT_TEST', { test_no => $keys[0], string_req => 'before methods',
						string_val => 'ghi' });
    
    like( $edt->{save_method_keyexpr}, qr{^test_no='?$keys[0]'?}, "get_keyexpr returned proper value" );
    ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
	cmp_ok( @{$edt->{save_method_keylist}}, '==', 1, "get_keylist returned one element" );
    ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
	cmp_ok( @{$edt->{save_method_values}}, '==', 2, "get_old_values returned one element" ) &&
	is( $edt->{save_method_values}[0], 'validate methods', "get_old_values returned proper value" );
    
    $result = $edt->update_record('EDT_TEST', { test_no => $keys[0], string_req => 'after methods',
						string_val => 'ghi' });
    
    like( $edt->{save_method_keyexpr}, qr{^test_no='?$keys[0]'?}, "get_keyexpr returned proper value" );
    ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
	cmp_ok( @{$edt->{save_method_keylist}}, '==', 1, "get_keylist returned one element" );
    ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
	cmp_ok( @{$edt->{save_method_values}}, '==', 2, "get_old_values returned one element" ) &&
	is( $edt->{save_method_values}[0], 'after methods', "get_old_values returned proper value" );
    
};


# Now check that a series of insert statements that cause an error will actually fail.

subtest 'execution errors' => sub {
    
    my ($edt, $result);
    
    # Clear both tables so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    $T->clear_table('EDT_AUX');
    
    # Then insert some records that are not in conflict.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, SILENT_MODE => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'test b1', _label => 'b1' });
    $edt->insert_record('EDT_AUX', { name => 'abc', test_no => '@b1' });
    $edt->insert_record('EDT_AUX', { name => 'def', test_no => '@b1', _label => 'x1' });
    
    $T->ok_no_errors('any', "no errors on initial inserts");
    
    $edt->update_record('EDT_AUX', { name => 'abc', aux_no => '@x1' });
    
    $T->ok_has_error('E_DUPLICATE', "got duplicate key value error");
    $T->ok_has_error(qr{'abc'.*'name'}, "error contained proper value and key name");

    {
	local $EditTransaction::TEST_PROBLEM{update_sql} = 1;
	
	$edt = $T->new_edt($perm_a, { SILENT_MODE => 1, IMMEDIATE_MODE => 1 });
	
	$edt->insert_record('EDT_TEST', { string_req => 'ok to insert' });
	
	my ($key) = $edt->inserted_keys('EDT_TEST');
	
	$T->ok_no_errors("no errors so far");
	
	$edt->update_record('EDT_TEST', { test_no => $key, string_req => 'updated' });

	$T->ok_has_error('any', 'E_EXECUTE');
    }

};
