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


# Check that delete_records works, in both single delete and multi-delete modes and with deletions
# from multiple tables in one transaction.

subtest 'basic' => sub {
    
    my ($edt, $result);
    
    # Clear the table and then insert some test records.
    
    $edt = &setup_records || return;
    
    my @test_keys = $edt->inserted_keys('EDT_TEST');
    
    my ($count) = $edt->dbh->selectrow_array("
		SELECT count(*) FROM $TABLE{EDT_TEST} join $TABLE{EDT_AUX} using (test_no)");
    
    cmp_ok( $count, '==', 3, "found three joined table rows" );
    
    # Now try some deletes. Use both a hashref and a single key value.
    
    $edt = $T->new_edt($perm_a);
    
    $result = $edt->delete_record('EDT_TEST', { test_id => $test_keys[0] });
    $result = $edt->delete_record('EDT_TEST', $test_keys[1]);
    
    if ( $T->ok_result($edt->commit, "deletion transaction committed") )
    {
	my ($count) = $edt->dbh->selectrow_array("
		SELECT count(*) FROM $TABLE{EDT_TEST}");
	
	is($count, 1, "one record remains after deletions");
	
	my ($aux_count) = $edt->dbh->selectrow_array("
		SELECT count(*) FROM $TABLE{EDT_AUX}");
	
	is($aux_count, 1, "auxiliary records were deleted by after_action subroutine");

	is($edt->action_count, 2, "two delete actions were done");
    }

    # Check keys and counts.
    
    cmp_ok( $edt->action_count, '==', 2, "found proper action_count" );
    cmp_ok( $edt->record_count, '==', 2, "found proper record_count" );
    cmp_ok( $edt->fail_count, '==', 0, "fail_count returns 0" );
    cmp_ok( $edt->skip_count, '==', 0, "skip_count returns 0" );
    
    cmp_ok( ($edt->inserted_keys), '==', 0, "inserted_keys returns empty list" );
    cmp_ok( ($edt->updated_keys), '==', 0, "updated_keys returns empty list" );
    cmp_ok( ($edt->replaced_keys), '==', 0, "replaced_keys returns empty list" );
    cmp_ok( ($edt->deleted_keys), '==', 2, "deleted_keys returns proper count for all tables" );
    cmp_ok( ($edt->failed_keys), '==', 0, "failed_keys returns empty list" );
    
    # Now check that multiple deletes are coalesced into a single action if the MULTI_DELETE flag
    # is set.
    
    my $edt = &setup_records || return;
    
    my @test_keys = $edt->inserted_keys('EDT_TEST');
    my @aux_keys = $edt->inserted_keys('EDT_AUX');
    
    my ($count) = $edt->dbh->selectrow_array("
		SELECT count(*) FROM $TABLE{EDT_TEST} join $TABLE{EDT_AUX} using (test_no)");
    
    cmp_ok( $count, '==', 3, "found three joined table rows" );
    
    my $edt = $T->new_edt($perm_a, { MULTI_DELETE => 1 });
    
    $result = $edt->delete_record('EDT_TEST', $test_keys[0]);
    $result = $edt->delete_record('EDT_TEST', $test_keys[1]);
    $result = $edt->delete_record('EDT_AUX', $aux_keys[2]);
    
    if ( $T->ok_result($edt->commit, "deletion transaction committed") )
    {
	my ($count) = $edt->dbh->selectrow_array("
		SELECT count(*) FROM $TABLE{EDT_TEST}");
	
	is($count, 1, "one record remains in $TABLE{EDT_TEST} after deletions");
	
	my ($aux_count) = $edt->dbh->selectrow_array("
		SELECT count(*) FROM $TABLE{EDT_AUX}");
	
	is($aux_count, 0, "no records remain in $TABLE{EDT_AUX} after deletions including after_action subroutine");	
	like($edt->{save_delete_aux}, qr{ \d+ ' , \s* ' \d+ }xs, "keyexpr returned two values");
    }
    
    # Check keys and counts.
    
    cmp_ok( $edt->action_count, '==', 2, "two multi-delete actions were done" );
    cmp_ok( $edt->record_count, '==', 3, "three records were processed" );
    cmp_ok( $edt->fail_count, '==', 0, "fail_count returns 0" );
    cmp_ok( $edt->skip_count, '==', 0, "skip_count returns 0" );
    
    cmp_ok( ($edt->inserted_keys), '==', 0, "inserted_keys returns empty list" );
    cmp_ok( ($edt->updated_keys), '==', 0, "updated_keys returns empty list" );
    cmp_ok( ($edt->replaced_keys), '==', 0, "replaced_keys returns empty list" );
    cmp_ok( ($edt->deleted_keys('EDT_TEST')), '==', 2, "deleted keys returns 2 for EDT_TEST");
    cmp_ok( ($edt->deleted_keys('EDT_AUX')), '==', 1, "deleted keys returns 1 for EDT_AUX");
    cmp_ok( ($edt->deleted_keys), '==', 3, "deleted_keys returns proper count for all tables" );
    cmp_ok( ($edt->failed_keys), '==', 0, "failed_keys returns empty list" );
};


# Now check various error conditions.

subtest 'errors' => sub {

    my ($edt, $result);
    
    # Clear the table and then insert some test records.
    
    $edt = &setup_records || return;
    
    my @test_keys = $edt->inserted_keys('EDT_TEST');
    
    my ($count) = $edt->dbh->selectrow_array("
		SELECT count(*) FROM $TABLE{EDT_TEST} join $TABLE{EDT_AUX} using (test_no)");
    
    cmp_ok( $count, '==', 3, "found three joined table rows" );
    
    # Now try some good and bad deletes.
    
    $edt = $T->new_edt($perm_a, { PROCEED => 1, IMMEDIATE_MODE => 1 });
    
    $result = $edt->delete_record('EDT_TEST', $test_keys[0]);
    
    ok( $result, "first deletion succeeded" );
    
    $result = $edt->delete_record('EDT_TEST', { test_no => 9999 });
    
    ok( !$result, "second deletion failed" );
    $T->ok_has_error('latest', 'F_NOT_FOUND');
    
    $result = $edt->delete_record('EDT_TEST', { test_no => $test_keys[0] });
    
    ok( !$result, "third deletion failed" );
    $T->ok_has_error('latest', 'F_NOT_FOUND');
    
    $result = $edt->delete_record('EDT_TEST', { });
    
    ok( !$result, "fourth deletion failed" );
    $T->ok_has_error('latest', 'F_NO_KEY');
    
    $result = $edt->delete_record('EDT_TEST', 0);
    
    ok( !$result, "fifth deletion failed" );
    $T->ok_has_error('latest', 'F_NO_KEY');

    $result = $edt->delete_record('EDT_AUX', 9999);
    
    ok( !$result, "sixth deletion failed" );
    $T->ok_has_error('latest', 'F_NOT_FOUND');
    
    # Check keys and counts.
    
    cmp_ok( $edt->action_count, '==', 1, "found proper action_count" );
    cmp_ok( $edt->record_count, '==', 6, "found proper record_count" );
    cmp_ok( $edt->fail_count, '==', 5, "fail_count returns 5" );
    cmp_ok( $edt->skip_count, '==', 0, "skip_count returns 0" );
    
    cmp_ok( ($edt->inserted_keys), '==', 0, "inserted_keys returns empty list" );
    cmp_ok( ($edt->updated_keys), '==', 0, "replaced_keys returns empty list" );
    cmp_ok( ($edt->replaced_keys('EDT_TEST')), '==', 0, "replaced_keys returns proper count for EDT_TEST" );
    cmp_ok( ($edt->replaced_keys('EDT_AUX')), '==', 0, "replaced_keys returns proper count for EDT_AUX" );
    cmp_ok( ($edt->replaced_keys), '==', 0, "replaced_keys returns proper count for all tables" );
    cmp_ok( ($edt->deleted_keys), '==', 1, "deleted_keys returns one entry" );
    cmp_ok( ($edt->failed_keys), '==', 3, "failed_keys returns three entries" );
    
    # Check for the proper exception if an invalid table is specified, or if no record is given.
    
    $edt = $T->new_edt($perm_a);
    
    eval {
	$edt->delete_record('BAD TABLE', { string_req => 'test_a2' });
    };
    
    ok( $@ && $@ =~ /unknown table/i, "insert_record threw an 'unknown table' exception with bad table name" );

    eval {
	$edt->delete_record('EDT_TEST');
    };
    
    ok( $@  && $@ =~ /no record specified/i, "insert_record threw a 'no record specified' exception with no record argument" );

    eval {
	$edt->delete_record('EDT_TEST', '');
    };

    ok( $@  && $@ =~ /no record specified/i, "insert_record threw a 'no record specified' exception with an empty string" );

    # Now check for an execution error if the generated SQL string is incorrect.
    
    {
	local $EditTransaction::TEST_PROBLEM{delete_sql} = 1;
	
	$edt = $T->new_edt($perm_a, { SILENT_MODE => 1, IMMEDIATE_MODE => 1 });
	
	$edt->delete_record('EDT_TEST', { test_no => $test_keys[1] });
	
	$T->ok_has_error('latest', 'E_EXECUTE');
    }
};


# Check the methods available to subclasses, specifically those whose effects vary according to
# the action type.

subtest 'subclass' => sub {
    
    my ($edt, $result);
    
    # Clear the table and then insert some test records.
    
    $edt = &setup_records || return;
    
    my @test_keys = $edt->inserted_keys('EDT_TEST');
    
    my ($count) = $edt->dbh->selectrow_array("
		SELECT count(*) FROM $TABLE{EDT_TEST} join $TABLE{EDT_AUX} using (test_no)");
    
    cmp_ok( $count, '==', 3, "found three joined table rows" );
    
    # Now do some deletes.
    
    $edt = $T->new_edt($perm_a);
    
    $result = $edt->delete_record('EDT_TEST', { test_no => $test_keys[0], string_req => 'authorize methods' });
    
    ok( $result, "delete_record succeeded" );
    
    like( $edt->{save_method_keyexpr}, qr{^test_no='?$test_keys[0]'?}, "get_keyexpr returned proper value" );
    ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
	cmp_ok( @{$edt->{save_method_keylist}}, '==', 1, "get_keylist returned one element" );
    ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
	cmp_ok( @{$edt->{save_method_values}}, '==', 2, "get_old_values returned one element" ) &&
	is( $edt->{save_method_values}[0], 'test a1', "get_old_values returned proper value" );

    $result = $edt->delete_record('EDT_TEST', { test_no => $test_keys[1], string_req => 'validate methods' });
    
    ok( $result, "delete_record succeeded" );
    
    like( $edt->{save_method_keyexpr}, qr{^test_no='?$test_keys[1]'?}, "get_keyexpr returned proper value" );
    ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
	cmp_ok( @{$edt->{save_method_keylist}}, '==', 1, "get_keylist returned one element" );
    ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
	cmp_ok( @{$edt->{save_method_values}}, '==', 2, "get_old_values returned one element" ) &&
	is( $edt->{save_method_values}[0], 'test a2', "get_old_values returned proper value" );
    
    # Now start execution, so that we can test that the result of get_old_values reflects the replaces that
    # have been processed so far.

    $edt->start_execution;
    
    $result = $edt->delete_record('EDT_TEST', { test_no => $test_keys[2], string_req => 'before methods' });
    
    like( $edt->{save_method_keyexpr}, qr{^test_no='?$test_keys[2]'?}, "get_keyexpr returned proper value" );
    ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
	cmp_ok( @{$edt->{save_method_keylist}}, '==', 1, "get_keylist returned one element" );
    ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
	cmp_ok( @{$edt->{save_method_values}}, '==', 2, "get_old_values returned one element" ) &&
	is( $edt->{save_method_values}[0], 'test a3', "get_old_values returned proper value" );

    $edt->commit;

    # Now we need to reset the table contents, since we only put three records in EDT_TEST.

    $edt = &setup_records || return;
    
    my @test_keys = $edt->inserted_keys('EDT_TEST');
    
    $edt = $T->new_edt($perm_a);
    
    $result = $edt->delete_record('EDT_TEST', { test_no => $test_keys[0], string_req => 'after methods' });
    
    $T->ok_result($edt->commit, "transaction committed");
    
    like( $edt->{save_method_keyexpr}, qr{^test_no='?$test_keys[0]'?}, "get_keyexpr returned proper value" );
    ok( $edt->{save_method_keylist} && ref $edt->{save_method_keylist} eq 'ARRAY', "get_keylist returned a list" ) &&
    	cmp_ok( @{$edt->{save_method_keylist}}, '==', 1, "get_keylist returned one element" );
    ok( $edt->{save_method_values} && ref $edt->{save_method_values} eq 'ARRAY', "get_old_values returned a list" ) &&
    	cmp_ok( @{$edt->{save_method_values}}, '==', 0, "get_old_values returned nothing" );
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



