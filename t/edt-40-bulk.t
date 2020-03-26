#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-40-bulk.t : Execute some bulk inserts, updates, replaces and deletes, each operation
# handling several thousand records.
# 


use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use TableDefs qw(get_table_property %TABLE);

use EditTest;
use EditTester;


our ($BASE_COUNT) = 10000;

# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;


# Start by getting the variable values that we need to execute the remainder of the test. If the
# session key 'SESSION-AUTHORIZER' does not appear in the session_data table, then run the test
# 'edt-01-basic.t' first.

my ($perm_a, $perm_e, $primary);
my (@keys);

subtest 'setup' => sub {
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;
    
    $perm_e = $T->new_perm('SESSION-ENTERER');
    
    ok( $perm_e && $perm_e->role eq 'enterer', "found enterer permission" ) or die;
    
    $primary = get_table_property('EDT_TEST', 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || BAIL_OUT;
};


# Check that we can insert thousands of records at once, and report how long the operation takes.

subtest 'bulk insert' => sub {
    
    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table('EDT_TEST');
    
    # Create a transaction for the insertions.

    my $starttime = time;
    
    $edt = $T->new_edt($perm_a);
    
    my $short_string = "This is a string less than 40 chars...";
    my $long_string = $short_string x 500;
    
    for ( my $i = 1; $i <= $BASE_COUNT; $i++ )
    {
	$result = $edt->insert_record('EDT_TEST', { string_req => "insert test $i",
						    string_val => $short_string,
						    text_val => $long_string,
						    signed_val => -12145,
						    decimal_val => 999.23 });
	last unless $result;
    }
    
    $T->ok_result($edt->commit, "transaction committed");
    
    my $elapsed = time - $starttime;

    diag("$BASE_COUNT insertions were carried out in $elapsed seconds.");
    
    (@keys) = $edt->inserted_keys;
    
    cmp_ok(@keys, '==', $BASE_COUNT, "inserted proper number of records");

    $T->ok_count_records($BASE_COUNT, 'EDT_TEST', '1', "check number of records inserted");
};


# Check that we can update thousands of records at once, and report how long the operation takes.

subtest 'bulk update' => sub {
    
    my ($edt, $result);
    
    my $starttime = time;
    
    $edt = $T->new_edt($perm_a);
    
    for ( my $i = 0; $i < $BASE_COUNT; $i++ )
    {
	$result = $edt->update_record('EDT_TEST', { test_no => $keys[$i], string_val => 'updated' });
	last unless $result;
    }
    
    $T->ok_result($edt->commit, "transaction committed");

    my $elapsed = time - $starttime;

    diag("$BASE_COUNT updates were carried out in $elapsed seconds.");
    
    my (@updated) = $edt->updated_keys;

    cmp_ok(@updated, '==', $BASE_COUNT, "updated proper number of records");

    $T->ok_count_records($BASE_COUNT, 'EDT_TEST', "string_val = 'updated'",
			 "check number of records updated");

};



# Check that we can replace thousands of records at once, and report how long the operation takes.

subtest 'bulk replace' => sub {
    
    my ($edt, $result);
    
    my $starttime = time;
    
    $edt = $T->new_edt($perm_a);
    
    for ( my $i = 0; $i < $BASE_COUNT; $i++ )
    {
	$result = $edt->replace_record('EDT_TEST', { test_no => $keys[$i], string_req => "replace test $i",
						     signed_val => 3 });
	last unless $result;
    }
    
    $T->ok_result($edt->commit, "transaction committed");
    
    my $elapsed = time - $starttime;
    
    diag("$BASE_COUNT replacements were carried out in $elapsed seconds.");
    
    my (@replaced) = $edt->replaced_keys;

    cmp_ok(@replaced, '==', $BASE_COUNT, "replaced proper number of records");

    $T->ok_count_records($BASE_COUNT, 'EDT_TEST', "signed_val = '3'",
			 "check number of records updated");

    $T->ok_count_records(0, 'EDT_TEST', "string_val = 'updated'",
			 "check that records were replaced and not updated");

};


# Check that we can delete thousands of records at once, and report how long the operation takes.

subtest 'bulk delete' => sub {
    
    my ($edt, $result);
    
    my $starttime = time;
    
    $edt = $T->new_edt($perm_a);
    
    for ( my $i = 0; $i < $BASE_COUNT; $i++ )
    {
	$result = $edt->delete_record('EDT_TEST', $keys[$i]);
	last unless $result;
    }
    
    $T->ok_result($edt->commit, "transaction committed");
    
    my $elapsed = time - $starttime;
    
    diag("$BASE_COUNT deletions were carried out in $elapsed seconds.");
    
    my (@deleted) = $edt->deleted_keys;
    
    cmp_ok(@deleted, '==', $BASE_COUNT, "replaced proper number of records");
    
    $T->ok_count_records(0, 'EDT_TEST', "1", "check number of records deleted");
};

