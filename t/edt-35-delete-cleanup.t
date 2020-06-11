#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-34-delete.t : Test the operation of the 'delete_records' method.
# 


use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 4;

use TableDefs qw(get_table_property set_table_property %TABLE);

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


# Check that delete_cleanup works properly.

subtest 'basic' => sub {
    
    my ($edt, $result);
    
    # Clear the table and then insert some test records.
    
    $T->clear_table('EDT_TEST');
    $T->clear_table('EDT_AUX');
    
    my $edt = $T->new_edt($perm_a) || return;
    
    $edt->insert_record('EDT_TEST', { string_req => 'test a1', _label => 'a1' });
   
    $edt->insert_record('EDT_AUX', { name => 'abc', test_no => '@a1' });
    $edt->insert_record('EDT_AUX', { name => 'def', test_no => '@a1' });
    $edt->insert_record('EDT_AUX', { name => 'ghi', test_no => '@a1' });
    
    $T->ok_result( $edt->commit, "insertion committed" ) || return;
    
    my @test_keys = $edt->inserted_keys('EDT_TEST');
    my @aux_keys = $edt->inserted_keys('EDT_AUX');

    my ($count) = $edt->dbh->selectrow_array("
		SELECT count(*) FROM $TABLE{EDT_TEST} join $TABLE{EDT_AUX} using (test_no)");
    
    cmp_ok( $count, '==', 3, "found three joined table rows" );
    
    # Now try updating one record and inserting another in EDT_AUX, and finish up by calling
    # delete_cleanup. We want to make sure that the two records not touched get deleted.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt->update_record('EDT_AUX', { aux_id => $aux_keys[0], name => 'updated a' });
    $edt->insert_record('EDT_AUX', { name => 'jkl', test_no => $test_keys[0] });
    $edt->delete_cleanup('EDT_AUX', "test_no=$test_keys[0]");
    
    $T->ok_result($edt->commit, "update committed") || return;

    # Check that the before_action and after_action methods were called, and that they were able
    # to get the proper keyexpr.

    if ( ok( $edt->{save_before_keyexpr}, "found before_action keyexpr" ) )
    {
	ok( $edt->{save_after_keyexpr}, "found after_action keyexpr" ) &&
	    is( $edt->{save_before_keyexpr}, $edt->{save_after_keyexpr}, "keyexpr was equal before and after" ) &&
	    like( $edt->{save_before_keyexpr}, qr{test_no=$test_keys[0].*aux_no in \(}, "keyexpr matched pattern" );
    }
    
    # Then check that action counts and record counts are correct.
    
    my ($count) = $edt->dbh->selectrow_array("
		SELECT count(*) FROM $TABLE{EDT_AUX}");
    
    is($count, 2, "two records remain after deletions");
    
    my ($join_count) = $edt->dbh->selectrow_array("
		SELECT count(*) FROM $TABLE{EDT_TEST} join $TABLE{EDT_AUX} using (test_no)");
    
    is($join_count, 2, "found two joined table rows");
    
    is($edt->action_count, 3, "three actions were done");
    
    is( $edt->action_count, 3, "found proper action_count" );
    is( $edt->record_count, 3, "found proper record_count" );
    is( $edt->fail_count, 0, "fail_count returns 0" );
    is( $edt->skip_count, 0, "skip_count returns 0" );
    
    is( $edt->inserted_keys, 1, "inserted_keys returns one element" );
    is( $edt->updated_keys, 1, "updated_keys returns one element" );
    is( $edt->replaced_keys, 0, "replaced_keys returns empty list" );
    is( $edt->deleted_keys, 2, "deleted_keys returns two elements" );
    is( $edt->failed_keys, 0, "failed_keys returns empty list" );
};


# Now check various error conditions.

subtest 'errors' => sub {

    my ($edt, $result);

    # Clear the table and then insert some test records.
    
    $T->clear_table('EDT_TEST');
    $T->clear_table('EDT_AUX');
    
    my $edt = $T->new_edt($perm_a) || return;
    
    $edt->insert_record('EDT_TEST', { string_req => 'test a1', _label => 'a1' });
   
    $edt->insert_record('EDT_AUX', { name => 'abc', test_no => '@a1' });
    $edt->insert_record('EDT_AUX', { name => 'def', test_no => '@a1' });
    $edt->insert_record('EDT_AUX', { name => 'ghi', test_no => '@a1' });
    
    $T->ok_result( $edt->commit, "insertion committed" ) || return;

    my @sup = $edt->inserted_keys('EDT_TEST');
    my @key = $edt->inserted_keys('EDT_AUX');

    is( @key, 3, "inserted three records" );
    
    # Now try delete_cleanup with a bad selector.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, SILENT_MODE => 1 });
    
    eval {
	$edt->delete_cleanup('EDT_AUX');
    };
    
    ok( $@ && $@ =~ /selector/, "delete_cleanup with no selector threw an exception" );
    
    eval {
	$edt->delete_cleanup('EDT_AUX', 1);
    };
    
    ok( $@ && $@ =~ /test_no/, "delete_cleanup with selector not mentioning link column threw an exception" );
    
    eval {
	$edt->delete_cleanup('EDT_AUX', "test_no=");
    };
    
    ok( !$@, "delete_cleanup with bad syntax on selector did not throw an exception" );

    $T->ok_has_error('latest', 'E_EXECUTE', "got E_EXECUTE exception on bad selector");

    # Now try delete_cleanup with a different user that does not have modify permission on the
    # record. Make sure of this by setting the table property.

    set_table_property('EDT_TEST', CAN_MODIFY => 'NOBODY');

    $perm_e->clear_cached_permissions;
    
    $edt = $T->new_edt($perm_e, { IMMEDIATE_MODE => 1 });
    
    $edt->delete_cleanup('EDT_AUX', "test_no=$sup[0]");
    
    $T->ok_has_error('latest', 'E_PERM', "got E_PERM exception on different user");
    
    # Then, make a valid call and check that it deletes all of the subordinate records.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt->delete_cleanup('EDT_AUX', "test_no=$sup[0]");
    
    my @deleted = $edt->deleted_keys('EDT_AUX');
    
    is( @deleted, 3, "deleted 3 records" );
    
    $T->ok_no_record('EDT_AUX', 1, "no records remain in subordinate table");
    
    # Now try a call with a nonexistent superior record and make sure that we get E_NOT_FOUND
    # back.

    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });

    eval {
	$edt->delete_cleanup('EDT_AUX', "test_no=9990");
    };

    ok( !$@, "no exception on nonexistent superior record" );
    
    $T->ok_has_error('latest', 'E_NOT_FOUND', "got E_NOT_FOUND on nonexistent superior record");
};


# Check that subclass method overrides get proper results 

subtest 'subclass' => sub {
    
    pass('placeholder');
    
    # my ($edt, $result);
    
    # # Clear the table and then insert some test records.
    
    # $edt = &setup_records || return;
    
    # my @test_keys = $edt->inserted_keys('EDT_TEST');
    
    # my ($count) = $edt->dbh->selectrow_array("
    # 		SELECT count(*) FROM $TABLE{EDT_TEST} join $TABLE{EDT_AUX} using (test_no)");
    
    # cmp_ok( $count, '==', 3, "found three joined table rows" );
    
    # # Now do some deletes.
    
    # $edt = $T->new_edt($perm_a);
    
    # $result = $edt->delete_record('EDT_TEST', { test_no => $test_keys[0], string_req => 'authorize methods' });
    
    # ok( $result, "delete_record succeeded" );
    
    # like( $edt->{save_method_keyexpr}, qr{^test_no='?$test_keys[0]'?}, "get_keyexpr returned proper value" );
    # ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
    # 	cmp_ok( @{$edt->{save_method_keylist}}, '==', 1, "get_keylist returned one element" );
    # ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
    # 	cmp_ok( @{$edt->{save_method_values}}, '==', 2, "get_old_values returned one element" ) &&
    # 	is( $edt->{save_method_values}[0], 'test a1', "get_old_values returned proper value" );

    # $result = $edt->delete_record('EDT_TEST', { test_no => $test_keys[1], string_req => 'validate methods' });
    
    # ok( $result, "delete_record succeeded" );
    
    # like( $edt->{save_method_keyexpr}, qr{^test_no='?$test_keys[1]'?}, "get_keyexpr returned proper value" );
    # ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
    # 	cmp_ok( @{$edt->{save_method_keylist}}, '==', 1, "get_keylist returned one element" );
    # ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
    # 	cmp_ok( @{$edt->{save_method_values}}, '==', 2, "get_old_values returned one element" ) &&
    # 	is( $edt->{save_method_values}[0], 'test a2', "get_old_values returned proper value" );
    
    # # Now start execution, so that we can test that the result of get_old_values reflects the replaces that
    # # have been processed so far.

    # $edt->start_execution;
    
    # $result = $edt->delete_record('EDT_TEST', { test_no => $test_keys[2], string_req => 'before methods' });
    
    # like( $edt->{save_method_keyexpr}, qr{^test_no='?$test_keys[2]'?}, "get_keyexpr returned proper value" );
    # ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
    # 	cmp_ok( @{$edt->{save_method_keylist}}, '==', 1, "get_keylist returned one element" );
    # ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
    # 	cmp_ok( @{$edt->{save_method_values}}, '==', 2, "get_old_values returned one element" ) &&
    # 	is( $edt->{save_method_values}[0], 'test a3', "get_old_values returned proper value" );

    # $edt->commit;

    # # Now we need to reset the table contents, since we only put three records in EDT_TEST.

    # $edt = &setup_records || return;
    
    # my @test_keys = $edt->inserted_keys('EDT_TEST');
    
    # $edt = $T->new_edt($perm_a);
    
    # $result = $edt->delete_record('EDT_TEST', { test_no => $test_keys[0], string_req => 'after methods' });
    
    # $T->ok_result($edt->commit, "transaction committed");
    
    # like( $edt->{save_method_keyexpr}, qr{^test_no='?$test_keys[0]'?}, "get_keyexpr returned proper value" );
    # ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
    # 	cmp_ok( @{$edt->{save_method_keylist}}, '==', 1, "get_keylist returned one element" );
    # ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
    # 	cmp_ok( @{$edt->{save_method_values}}, '==', 0, "get_old_values returned nothing" );
};


# Set up records for deletion testing.

sub setup_records {

    $T->clear_table('EDT_TEST');
    $T->clear_table('EDT_AUX');
    
    my $edt = $T->new_edt($perm_a);
    
    $edt->insert_record('EDT_TEST', { string_req => 'test a1', _label => 'a1' });
    $edt->insert_record('EDT_TEST', { string_req => 'test a2', _label => 'a2' });
    $edt->insert_record('EDT_TEST', { string_req => 'test a3', _label => 'a3' });
    
    $edt->insert_record('EDT_AUX', { name => 'abc', test_no => '@a1' });
    $edt->insert_record('EDT_AUX', { name => 'def', test_no => '@a2' });
    $edt->insert_record('EDT_AUX', { name => 'ghi', test_no => '@a3' });
    
    return $T->ok_result( $edt->execute, "insertion executed successfully" ) && $edt;
}



