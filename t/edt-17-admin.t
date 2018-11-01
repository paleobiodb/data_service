#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-17-admin.t : Test the 'admin_lock' field and the 'ADMIN_SET' attribute.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 6;

use TableDefs qw(get_table_property set_table_property);

use EditTest;
use EditTester;


# Make sure the following table property change is set for the remainder of this test file.

set_table_property('EDT_TEST', CAN_MODIFY => 'AUTHORIZED');

# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;


# Start by getting the variable values that we need to execute the remainder of the test. If the
# session key 'SESSION-AUTHORIZER' does not appear in the session_data table, then run the test
# 'edt-01-basic.t' first.

my ($perm_a, $perm_e, $perm_s, $perm_m, $primary);

subtest 'setup' => sub {
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;
    
    $perm_e = $T->new_perm('SESSION-ENTERER');
    
    ok( $perm_e && $perm_e->role eq 'enterer', "found enterer permission" ) || BAIL_OUT;
    
    $perm_s = $T->new_perm('SESSION-SUPERUSER');
    
    ok( $perm_s && $perm_s->is_superuser, "found superuser permission" ) || BAIL_OUT;
    
    $perm_m = $T->new_perm('SESSION-WITH-ADMIN');
    ok( $perm_m && $perm_m->role eq 'enterer', "found admin permission" ) || BAIL_OUT;
    
    $primary = get_table_property('EDT_TEST', 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || BAIL_OUT;
};


# Insert some records that we can use in subsequent subtests, and clear both the test table and
# the permissions table.

our (@key);

subtest 'insert records' => sub {
    
    my ($edt, $result);
    
    # Start by clearing the table, and also any specific permissions that have been added.
    
    $T->clear_table('EDT_TEST');
    $T->clear_specific_permissions;
    
    # Then insert some records.
    
    $edt = $T->new_edt($perm_e);
    
    $edt->insert_record('EDT_TEST', { string_req => 'abc', unsigned_val => 3 });
    $edt->insert_record('EDT_TEST', { string_req => 'def', unsigned_val => 4 });
    $edt->insert_record('EDT_TEST', { string_req => 'ghi', unsigned_val => 5 });
    $edt->insert_record('EDT_TEST', { string_req => 'jkl', unsigned_val => 6 });
    $edt->insert_record('EDT_TEST', { string_req => 'mno', unsigned_val => 7 });
    $edt->insert_record('EDT_TEST', { string_req => 'pqr', unsigned_val => 8 });
    
    $T->ok_result( $edt->commit, "insert records committed" ) || return;
    
    (@key) = $edt->inserted_keys;
};


# Check the function of the 'admin_lock' field.

subtest 'admin_lock non-admin' => sub {
    
    my ($edt, $result);
    
    # Try to set the 'admin_lock' fields without adminitrative permission, and check that we get the proper
    # errors.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $edt->update_record('EDT_TEST', { test_id => $key[0], admin_lock => 1 });
    
    $T->ok_has_error('latest', 'F_PERM_COL', "got error setting admin_lock");
    
    $edt->update_record('EDT_TEST', { test_id => $key[0], admin_lock => 0 });
    
    $T->ok_has_error('latest', 'F_PERM_COL', "got error clearing admin_lock");
    
    $edt->update_record('EDT_TEST', { test_id => $key[0], admin_lock => undef });
    
    $T->ok_no_errors('latest', "no error setting admin_lock to undef");
    
    $edt->update_record('EDT_TEST', { test_id => $key[0], admin_lock => 'abc' });
    
    $T->ok_has_error('latest', 'F_PERM_COL', "got permission error setting admin_lock to invalid value");
    $T->ok_has_error('latest', 'F_FORMAT', "got format error setting admin_lock to invalid value");
    
    $edt->insert_record('EDT_TEST', { string_req => 'test', admin_lock => 1 });
    
    $T->ok_has_error('latest', 'F_PERM_COL', "got error inserting a record with admin_lock set");
    
    $edt->insert_record('EDT_TEST', { string_req => 'test', admin_lock => undef });

    $T->ok_no_errors('latest', "no error inserting a record with admin_lock undefined");
};


# Now try the same thing with admin privileges.

subtest 'admin_lock admin' => sub {

    my ($edt, $result);

    # Try setting admin_lock with a user that has admin privileges.
    
    $T->set_specific_permission('EDT_TEST', $perm_m, 'admin');
    
    $edt = $T->new_edt($perm_m, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $edt->update_record('EDT_TEST', { test_id => $key[0], admin_lock => 0 });
    
    $T->ok_no_errors('latest', "no error clearing admin_lock that was already zero");
    
    $edt->update_record('EDT_TEST', { test_id => $key[0], admin_lock => 1 });
    
    $T->ok_no_errors('latest', "no error setting admin_lock");
    
    $T->ok_found_record('EDT_TEST', "test_no = $key[0] and admin_lock = 1", "lock was actually set");
    
    $edt->update_record('EDT_TEST', { test_id => $key[0], admin_lock => '' });
    
    $T->ok_no_errors('latest', "no error setting admin_lock to empty");

    $T->ok_found_record('EDT_TEST', "test_no = $key[0] and admin_lock = 1", "lock is unchanged");
    
    $edt->update_record('EDT_TEST', { test_id => $key[0], admin_lock => 0 });
    
    $T->ok_no_errors('latest', "no error clearing admin_lock");
    
    $T->ok_found_record('EDT_TEST', "test_no = $key[0] and admin_lock = 0", "lock was actually cleared");
    
    $edt->update_record('EDT_TEST', { test_id => $key[0], admin_lock => 'abc' });
    
    $T->ok_has_one_error('latest', 'E_FORMAT', "format error setting admin_lock to an invalid value");

    $edt->insert_record('EDT_TEST', { string_req => 'test locked', admin_lock => 1 });
    
    $T->ok_no_errors('latest', "no error inserting a record with admin_lock set");

    $T->ok_found_record('EDT_TEST', "string_req = 'test locked' and admin_lock = 1", "record was inserted with lock");
    
    $edt->update_record('EDT_TEST', { test_id => $key[0], admin_lock => '1' });

    $T->ok_no_errors('latest', "no error setting admin_lock again");

    $T->ok_found_record('EDT_TEST', "test_no = $key[0] and admin_lock = 1", "lock is now set again");    
    
    $T->ok_result( $edt->commit, "committed admin changes okay" );
    
    # Try again with superuser privileges.
    
    $edt = $T->new_edt($perm_s, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $edt->update_record('EDT_TEST', { test_id => $key[0], admin_lock => 1 });
    
    $T->ok_has_error('latest', 'C_LOCKED', "got caution setting admin_lock by superuser");
    
    $edt = $T->new_edt($perm_s, { IMMEDIATE_MODE => 1, PROCEED => 1, LOCKED => 1 });

    $edt->update_record('EDT_TEST', { test_id => $key[0], admin_lock => 1 });
    
    $T->ok_no_errors('latest', "caution removed by LOCKED for setting admin_lock by superuser");
        
    $T->ok_found_record('EDT_TEST', "test_no = $key[0] and admin_lock = 1", "lock was actually set");

    $edt->update_record('EDT_TEST', { test_id => $key[0], admin_lock => 0 });
    
    $T->ok_no_errors('latest', "no error clearing admin_lock by superuser");
    
    $T->ok_found_record('EDT_TEST', "test_no = $key[0] and admin_lock = 0", "lock was actually cleared");
    
    $edt->update_record('EDT_TEST', { test_id => $key[0], admin_lock => 'abc' });
    
    $T->ok_has_one_error('latest', 'E_FORMAT', "format error setting admin_lock to an invalid value");
    
    $edt->insert_record('EDT_TEST', { string_req => 'test locked', admin_lock => 1 });
    
    $T->ok_no_errors('latest', "no error inserting a record with admin_lock set");

    $T->ok_found_record('EDT_TEST', "string_req = 'test locked' and admin_lock = 1", "record was inserted with lock");

    $T->ok_result( $edt->commit, "committed superuser changes okay" );
};


# Now we insert some locked records and check that nobody without admin permission can modify them
# but an administrator can do so with allow=LOCKED.

subtest 'admin_lock operation ' => sub {
    
    my ($edt, $result);

    # Set the table property 'CAN_MODIFY' to 'AUTHORIZED', which lets authorized users edit
    # entries created (and thus owned) by other users.
    
    $T->clear_table('EDT_TEST');
    
    # Insert two records under a non-admin user.

    $edt = $T->new_edt($perm_e);

    $edt->insert_record('EDT_TEST', { string_req => 'test admin 0' });
    $edt->insert_record('EDT_TEST', { string_req => 'test admin 1' });
    
    $T->ok_result( $edt->commit, "record insertion committed" );
    
    my @test = $edt->inserted_keys;
    
    # Insert a locked record and then lock another using admin permission.
    
    $edt = $T->new_edt($perm_m, { IMMEDIATE_MODE => 1 });
    
    $edt->insert_record('EDT_TEST', { _label => 'a1', string_req => 'test admin 2', admin_lock => 1 });
    $edt->insert_record('EDT_TEST', { _label => 'a2', string_req => 'test admin 3' });
    $edt->update_record('EDT_TEST', { test_no => '@a2', admin_lock => 1 });
    
    # Then insert an unlocked record as a control, and lock one of the previously added ones.
    
    $edt->insert_record('EDT_TEST', { string_req => 'test admin 4', admin_lock => 0 });
    $edt->update_record('EDT_TEST', { test_no => $test[0], admin_lock => 1 });
    
    $T->ok_result( $edt->commit, "record insertion and update committed" );
    
    # Make sure that the locks are as intended.
    
    $T->ok_found_record('EDT_TEST', "string_req = 'test admin 0' and admin_lock = 1", "record was inserted with lock");
    $T->ok_found_record('EDT_TEST', "string_req = 'test admin 1' and admin_lock = 0", "record was updated with lock");
    $T->ok_found_record('EDT_TEST', "string_req = 'test admin 2' and admin_lock = 1", "record was inserted with lock");
    $T->ok_found_record('EDT_TEST', "string_req = 'test admin 3' and admin_lock = 1", "record was updated with lock");
    $T->ok_found_record('EDT_TEST', "string_req = 'test admin 4' and admin_lock = 0", "record was inserted without lock");

    push @test, $edt->inserted_keys;
    
    # Now try to modify the records as the non-admin user.
    
    $edt = $T->new_edt($perm_e, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $edt->update_record('EDT_TEST', { test_no => $test[0], string_val => 'mod e' });
    
    $T->ok_has_error('latest', 'E_LOCKED', "could not update locked record 0");
    
    $edt->update_record('EDT_TEST', { test_no => $test[1], string_val => 'mod e' });
    
    $T->ok_no_errors('latest', "could update unlocked record 1");
    
    $edt->update_record('EDT_TEST', { test_no => $test[2], string_val => 'mod e' });
    
    $T->ok_has_error('latest', 'E_LOCKED', "could not update locked record 2");
    
    $edt->update_record('EDT_TEST', { test_no => $test[3], string_val => 'mod e' });
    
    $T->ok_has_error('latest', 'E_LOCKED', "could not update locked record 3");
    
    $edt->update_record('EDT_TEST', { test_no => $test[4], string_val => 'mod e' });
    
    $T->ok_no_errors('latest', "could update unlocked record 4");
    
    # Then try the same as the admin user. We should get C_LOCKED rather than E_LOCKED, but
    # otherwise we should get the same results.

    $edt = $T->new_edt($perm_m, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $edt->update_record('EDT_TEST', { test_no => $test[0], string_val => 'mod m' });
    
    $T->ok_has_error('latest', 'C_LOCKED', "could not update locked record 0");
    
    $edt->update_record('EDT_TEST', { test_no => $test[1], string_val => 'mod m' });
    
    $T->ok_no_errors('latest', "could update unlocked record 1");
    
    $edt->update_record('EDT_TEST', { test_no => $test[2], string_val => 'mod m' });
    
    $T->ok_has_error('latest', 'C_LOCKED', "could not update locked record 2");
    
    $edt->update_record('EDT_TEST', { test_no => $test[3], string_val => 'mod m' });
    
    $T->ok_has_error('latest', 'C_LOCKED', "could not update locked record 3");
    
    $edt->update_record('EDT_TEST', { test_no => $test[4], string_val => 'mod m' });
    
    $T->ok_no_errors('latest', "could update unlocked record 4");
    
    # Check that we get the same results as superuser.

    $edt = $T->new_edt($perm_s, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $edt->update_record('EDT_TEST', { test_no => $test[0], string_val => 'mod s' });
    
    $T->ok_has_error('latest', 'C_LOCKED', "could not update locked record 0");
    
    $edt->update_record('EDT_TEST', { test_no => $test[1], string_val => 'mod s' });
    
    $T->ok_no_errors('latest', "could update unlocked record 1");
    
    $edt->update_record('EDT_TEST', { test_no => $test[2], string_val => 'mod s' });
    
    $T->ok_has_error('latest', 'C_LOCKED', "could not update locked record 2");
    
    $edt->update_record('EDT_TEST', { test_no => $test[3], string_val => 'mod s' });
    
    $T->ok_has_error('latest', 'C_LOCKED', "could not update locked record 3");
    
    $edt->update_record('EDT_TEST', { test_no => $test[4], string_val => 'mod s' });
    
    $T->ok_no_errors('latest', "could update unlocked record 4");

    $T->ok_count_records(2, 'EDT_TEST', "string_val='mod s'", "modified two records");

    # Then try again with the admin user with the LOCKED condition.

    $edt = $T->new_edt($perm_m, { IMMEDIATE_MODE => 1, PROCEED => 1, LOCKED => 1 });
    
    $edt->update_record('EDT_TEST', { test_no => $test[0], string_val => 'mod m1' });
    
    $T->ok_no_errors('latest', "could update record 0");
    
    $edt->update_record('EDT_TEST', { test_no => $test[1], string_val => 'mod m1' });
    
    $T->ok_no_errors('latest', "could update record 1");
    
    $edt->update_record('EDT_TEST', { test_no => $test[2], string_val => 'mod m1' });
    
    $T->ok_no_errors('latest', "could update record 2");
    
    $edt->update_record('EDT_TEST', { test_no => $test[3], string_val => 'mod m1' });
    
    $T->ok_no_errors('latest', "could update record 3");
    
    $edt->update_record('EDT_TEST', { test_no => $test[4], string_val => 'mod m1' });
    
    $T->ok_no_errors('latest', "could update record 4");

    $T->ok_count_records(5, 'EDT_TEST', "string_val = 'mod m1'", "modified five records with LOCKED");
    
    # Then try again with the superuser with the LOCKED condition.

    $edt = $T->new_edt($perm_s, { IMMEDIATE_MODE => 1, PROCEED => 1, LOCKED => 1 });
    
    $edt->update_record('EDT_TEST', { test_no => $test[0], string_val => 'mod s1' });
    
    $T->ok_no_errors('latest', "could update record 0");
    
    $edt->update_record('EDT_TEST', { test_no => $test[1], string_val => 'mod s1' });
    
    $T->ok_no_errors('latest', "could update record 1");
    
    $edt->update_record('EDT_TEST', { test_no => $test[2], string_val => 'mod s1' });
    
    $T->ok_no_errors('latest', "could update record 2");
    
    $edt->update_record('EDT_TEST', { test_no => $test[3], string_val => 'mod s1' });
    
    $T->ok_no_errors('latest', "could update record 3");
    
    $edt->update_record('EDT_TEST', { test_no => $test[4], string_val => 'mod s1' });
    
    $T->ok_no_errors('latest', "could update record 4");

    $T->ok_count_records(5, 'EDT_TEST', "string_val = 'mod s1'", "modified five records with LOCKED");
    
    # Then try again without LOCKED but with explicitly unlocking the record. Check that
    # explicitly setting admin_lock to 0 allows a modification, but not setting it to 1.

    $edt = $T->new_edt($perm_m, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $edt->update_record('EDT_TEST', { test_no => $test[0], admin_lock => 0, string_val => 'mod m2' });
    
    $T->ok_no_errors('latest', "could update record 0");
    
    $edt->update_record('EDT_TEST', { test_no => $test[1], admin_lock => 0, string_val => 'mod m2' });
    
    $T->ok_no_errors('latest', "could update record 1");
    
    $edt->update_record('EDT_TEST', { test_no => $test[2], admin_lock => 0, string_val => 'mod m2' });
    
    $T->ok_no_errors('latest', "could update record 2");
    
    $edt->update_record('EDT_TEST', { test_no => $test[3], admin_lock => 1, string_val => 'mod m2' });
    
    $T->ok_has_error('latest', 'C_LOCKED', "could not update record 3 while setting admin_lock to 1, already locked");
    
    $edt->update_record('EDT_TEST', { test_no => $test[4], admin_lock => 1, string_val => 'mod m2' });
    
    $T->ok_no_errors('latest', "could update record 4 while setting admin_lock to 1, not already locked");
    
    $T->ok_count_records(4, 'EDT_TEST', "string_val = 'mod m2'", "modified four records while setting admin_lock to 0");
    
    # Now we let the admin unlock a record, and check that the non-admin user can now edit it.

    $edt = $T->new_edt($perm_m, { IMMEDIATE_MODE => 1, PROCEED => 1 });

    $edt->update_record('EDT_TEST', { test_no => $test[0], admin_lock => 0 });

    $T->ok_result( $edt->commit, "unlock record 0 committed" );

    $edt = $T->new_edt($perm_e, { PROCEED => 1 });
    
    $edt->update_record('EDT_TEST', { test_no => $test[0], string_val => 'mod e' });
    
    $T->ok_no_errors('latest', "modification of newly unlocked record worked fine");

    $edt->update_record('EDT_TEST', { test_no => $test[2], string_val => 'mod e' });
    
    $T->ok_has_error('latest', 'E_LOCKED', "modification of still locked record got E_LOCKED");
    
    $T->ok_result( $edt->commit, "modification of newly unlocked record committed" );
    
    $T->ok_count_records(1, 'EDT_TEST', "string_val='mod e'", "found one modified record");
};


# Now test a field that has the 'ADMIN_SET' property. This should be settable only by
# administrators, and only if the record is not locked.

subtest 'admin_set' => sub {
    
    my ($edt, $result);

    $T->clear_table('EDT_TEST');
    
    # Try inserting and updating records with a value for 'admin_str', as a non-admin user.
    
    $edt = $T->new_edt($perm_e, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'abc', _label => 'a1' });
    
    $T->ok_no_errors('latest', "no error inserting a record");
    
    $edt->insert_record('EDT_TEST', { string_req => 'def', admin_str => 'test 1' });
    
    $T->ok_has_error('latest', 'E_PERM_COL', "got permission error inserting value for 'admin_str'");
    
    $edt->update_record('EDT_TEST', { test_no => '@a1', admin_str => 'test 2' });

    $T->ok_has_error('latest', 'E_PERM_COL', "got permission error updating value for 'admin_str'");
    
    $edt->update_record('EDT_TEST', { test_no => '@a1', admin_str => '' });
    
    $T->ok_has_error('latest', 'E_PERM_COL', "got permission error setting 'admin_str' to empty");

    $edt->update_record('EDT_TEST', { test_no => '@a1', admin_str => undef });

    $T->ok_no_errors('latest', "no error setting admin_str to undef");

    $T->ok_result( $edt->commit, "non-admin changes committed" );

    my @keys = $edt->inserted_keys;

    # Now try inserting and updating records with a value for 'admin_str' as an admin user.

    $edt = $T->new_edt($perm_m, { IMMEDIATE_MODE => 1, PROCEED => 1 });

    $edt->insert_record('EDT_TEST', { string_req => 'admin 3', admin_str => 'test 3', _label => 'b1' });

    $T->ok_no_errors('latest', "no error inserting a record");

    $edt->update_record('EDT_TEST', { test_no => '@b1', admin_str => 'test 3 updated' });

    $T->ok_no_errors('latest', "no error updating own record");

    $edt->update_record('EDT_TEST', { test_no => $keys[0], admin_str => 'test 4 updated' });
    
    $T->ok_no_errors('latest', "no error updating another's record");
    
    $T->ok_result( $edt->commit, "admin changes committed" );
};
