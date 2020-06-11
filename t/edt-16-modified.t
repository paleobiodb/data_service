#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-16-modified.t : Test that insertions and updates set the authorizer_no, enterer_no,
# modifier_no, created, and modified properties correctly.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 9;

use TableDefs qw(get_table_property set_table_property);

use EditTest;
use EditTester;


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new({ subclass => 'EditTest' });


# Start by getting the variable values that we need to execute the remainder of the test. If the
# session key 'SESSION-AUTHORIZER' does not appear in the session_data table, then run the test
# 'edt-01-basic.t' first.

my ($perm_a, $perm_e, $perm_s, $primary);
my ($person_a, $person_e, $person_s);

subtest 'setup' => sub {

    # We need to set this property in order to test what happens when a new record is inserted
    # using a 'replace' operation.
    
    set_table_property('EDT_TEST', ALLOW_INSERT_KEY => 1 );

    # Clear any special permissions that may have been set.
    
    $T->clear_specific_permissions;
    
    # Retrieve the necessary permissions. If we can't find any of them, then bail.
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;
    
    $perm_e = $T->new_perm('SESSION-ENTERER');
    
    ok( $perm_e && $perm_e->role eq 'enterer', "found enterer permission" ) || BAIL_OUT;
    
    $perm_s = $T->new_perm('SESSION-SUPERUSER');
    
    ok( $perm_s && $perm_s->is_superuser, "found superuser permission" ) || BAIL_OUT;
    
    $person_a = $perm_a->enterer_no;
    $person_e = $perm_e->enterer_no;
    $person_s = $perm_s->enterer_no;
    
    $primary = get_table_property('EDT_TEST', 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || BAIL_OUT;
};


# Check that record operations set and update the created and modified dates properly. Also check
# that these can be specifically set only with administrator or superuser permission.

subtest 'crmod' => sub {
    
    my ($edt, $result);
    
    # Start by clearing the table.

    $T->clear_table('EDT_TEST');

    # Then insert some records and check the created and modified timestamps.
    
    $edt = $T->new_edt($perm_e);
    
    $edt->insert_record('EDT_TEST', { string_req => 'abc', unsigned_val => 3 });
    $edt->insert_record('EDT_TEST', { string_req => 'def', unsigned_val => 4 });
    $edt->insert_record('EDT_TEST', { string_req => 'ghi', unsigned_val => 5 });
    $edt->insert_record('EDT_TEST', { string_req => 'jkl', unsigned_val => 6 });
    $edt->insert_record('EDT_TEST', { string_req => 'mno', unsigned_val => 7 });
    $edt->insert_record('EDT_TEST', { string_req => 'pqr', unsigned_val => 8 });
    
    my ($now) = $T->dbh->selectrow_array("SELECT NOW()");
    
    ok( $edt->commit, "insert records committed" ) || return;
    ok( $now, "fetched current timestamp" ) || return;
    
    my ($k1, $k2, $k4, $k5) = $edt->inserted_keys;
    my $k3 = 99999;
    
    my ($r1) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $r1->{created}, $now, "created timestamp is current" );
    is( $r1->{modified}, $now, "modified timestamp is current" );
    
    # Now update one record and replace another, and check that the modified timestamp is different. We
    # need to wait for at least 1 second to make sure of this. Also use 'replace' to insert a new record.
    
    sleep(1);
    
    my ($orig_stamp) = $now;
    
    $edt = $T->new_edt($perm_a);
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated' });
    $edt->replace_record('EDT_TEST', { $primary => $k2, string_req => 'replaced' });
    $edt->replace_record('EDT_TEST', { $primary => $k3, string_req => 'new' });
    
    my ($now) = $T->dbh->selectrow_array("SELECT NOW()");
    
    $T->ok_result( $edt->commit, "update records committed" ) || return;
    
    isnt($now, $orig_stamp, "timestamps differ");
    
    my ($u1) = $T->fetch_records_by_key('EDT_TEST', $k1);
    my ($u2) = $T->fetch_records_by_key('EDT_TEST', $k2);
    my ($u3) = $T->fetch_records_by_key('EDT_TEST', $k3);
    
    is( $u1->{created}, $orig_stamp, "created timestamp has not changed on update" );
    is( $u1->{modified}, $now, "modified timestamp is current on update" );

    is( $u2->{created}, $orig_stamp, "created timestamp has not changed on replace" );
    is( $u2->{modified}, $now, "modified timestamp is current on replace" );
    
    is( $u3->{created}, $now, "created timestamp is current on insert key" );
    is( $u3->{modified}, $now, "modified timestamp is current on insert key" );
    
    isnt( $now, $orig_stamp, "at least one second has elapsed" );

    is( $u1->{unsigned_val}, $r1->{unsigned_val}, "update did not change other field" );
    is( $u2->{unsigned_val}, '0', "replace removed other field" );

    # Now have somebody else (the corresponding authorizer) update the record, and check that the
    # crmod fields are still updated properly. Check that modifier_no is also set properly.
    
    $edt = $T->new_edt($perm_a);
    
    $edt->update_record('EDT_TEST', { $primary => $k4, string_req => 'updated by other' });
    $edt->replace_record('EDT_TEST', { $primary => $k5, string_req => 'replaced by other' });
    
    ($now) = $T->dbh->selectrow_array("SELECT NOW()");
    
    $T->ok_result( $edt->commit, "update other committed" ) || return;
    
    my ($u4) = $T->fetch_records_by_key('EDT_TEST', $k4);
    my ($u5) = $T->fetch_records_by_key('EDT_TEST', $k5);
    
    is( $u4->{created}, $orig_stamp, "created timestamp has not changed on update" );
    is( $u4->{modified}, $now, "modified timestamp is current on update" );
    is( $u4->{modifier_no}, $person_a, "modifier_no is set on update" );
    
    is( $u5->{created}, $orig_stamp, "created timestamp has not changed on replace" );
    is( $u5->{modified}, $now, "modified timestamp is current on replace" );
    is( $u5->{modifier_no}, $person_a, "modifier_no is set on update" );
};


# Now check that crmod fields are not allowed to be messed with unless 'admin' permission on the
# table is held.

subtest 'crmod non-admin' => sub {
    
    # Try using 'fixup_mode' with the regular permissions. This should produce an error.
    
    my ($edt, $k1);

    ($k1) = $T->fetch_keys_by_expr('EDT_TEST', "string_req='mno'");
    
    $edt = $T->new_edt($perm_a, { FIXUP_MODE => 1, IMMEDIATE_MODE => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'insert test' });
    
    $T->ok_no_errors("no errors on insert_record with FIXUP_MODE");
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_val => 'updated' });
    
    $T->ok_has_error('E_PERM_COL', "got permission violation on update_record with FIXUP_MODE");
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'updated' });
    
    $T->ok_has_error('E_PERM_COL', "got permission violation on replace_record with FIXUP_MODE");
    
    # Make sure that neither update nor replacement went through.
    
    $T->ok_no_record('EDT_TEST', "$primary = $k1 and string_req = 'updated'");
    
    # Now try the same thing without FIXUP_MODE but explicitly setting modified to UNCHANGED.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, ALTER_TRAIL => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'insert test', modified => 'UNCHANGED' });
    
    $T->ok_has_error( qr{E_PERM_COL.*modified}, "got permission violation on insert_record with modified UNCHANGED");
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_val => 'updated',
				     modified => 'UNCHANGED' });
    
    $T->ok_has_error( qr{E_PERM_COL.*modified}, "got permission violation on update_record with modified UNCHANGED");
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_val => 'updated',
				     modifier_no => 'UNCHANGED' });
    
    $T->ok_has_error( qr{E_PERM_COL.*modifier_no}, "got permission violation on update_record with modified UNCHANGED");
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'updated',
				      modified => 'UNCHANGED', modifier_no => 'UNCHANGED' });
    
    $T->ok_has_error( qr{E_PERM_COL.*modified}, "got permission violation on replace_record with modified UNCHANGED");
    $T->ok_has_error( qr{E_PERM_COL.*modifier_no}, "got permission violation on replace_record with modifier_no UNCHANGED");
    
    # Make sure that neither update nor replacement went through.
    
    $T->ok_no_record('EDT_TEST', "$primary = $k1 and string_req = 'updated'");
    
    # Now try to explicitly set values for created and modified. This should also give an error.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, ALTER_TRAIL => 1 });
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated',
				      modified => '2010-01-01 12:51:42' });
    
    $T->ok_has_error('latest', qr{E_PERM_COL.*modified}, "got permission violation on update_record setting modified");
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'updated',
				       modified => '2010-02-03 12:51:08' });
    
    $T->ok_has_error('latest', qr{E_PERM_COL.*modified}, "got permission violation on replace_record setting modified" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'inserted 2', modified => '2010-03-04 01:01:02' });
    
    $T->ok_has_error('latest', qr{E_PERM_COL.*modified}, "got permission violation on insert_record setting modified" );
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated',
				      created => '2010-01-01 abc' });
    
    $T->ok_has_error('latest', qr{E_PERM_COL.*created}, "got permission violation on update_record setting created" );
    $T->ok_has_error('latest', qr{E_FORMAT.*created}, "got format error on update_record setting created" );
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'updated',
				       created => '2010-01-01 def' });
    
    $T->ok_has_error('latest', qr{E_PERM_COL.*created}, "got permission violation on replace_record setting created" );
    $T->ok_has_error('latest', qr{E_FORMAT.*created}, "got format error on replace_record setting created" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'inserted 2', created => '2010-01-01 ghi' });
    
    $T->ok_has_error('latest', qr{E_PERM_COL.*created}, "got permission violation on insert_record setting created" );
    $T->ok_has_error('latest', qr{E_FORMAT.*created}, "got format error on insert_record setting created" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'inserted 3', created => '2011-01-01',
				      modified => '2011-01-01' });
    
    $T->ok_has_error('latest', qr{E_PERM_COL.*created}, "got permission violation on insert_record setting created" );
    $T->ok_has_error('latest', qr{E_PERM_COL.*modified}, "got permission violation on insert_record setting modified" );

    # Now check that empty values don't work either.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, ALTER_TRAIL => 1 });
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated a2', modified => '' });
    $T->ok_no_errors( 'latest', "no permission violation on update_record setting modified to empty string" );
    
    $edt->update_record('EDT_TEST', { $primary =>$ k1, string_req => 'updated a3', modified => undef });
    $T->ok_no_errors( 'latest', "no permission violation on update_record setting modified to undef" );
    
    $edt->update_record('EDT_TEST', { $primary =>$ k1, string_req => 'updated a4', modified => 0 });
    $T->ok_has_error('latest', 'E_PERM_COL', "permission violation on update_record setting modified to 0" );
    $T->ok_has_error('latest', 'E_FORMAT', "format error on update_record setting modified to 0" );
    
    # $edt->update_record
    
    # Make sure that neither update nor replacement went through. Also make sure that insertion
    # didn't go through.
    
    $T->ok_no_record('EDT_TEST', "$primary = $k1 and string_req = 'updated'");
    $T->ok_no_record('EDT_TEST', "string_req='inserted 3'");
};


# Then check that with 'admin' permission these restrictions are lifted.

subtest 'crmod admin' => sub {

    # This time we give the authorizer 'admin' privilege on the table.
    
    my ($edt, $k1, $k2, $orig, $new, $now);
    
    $T->set_specific_permission('EDT_TEST', $perm_a, 'admin');
    $perm_a->clear_cached_permissions;
    
    $edt = $T->new_edt($perm_a);	# now with 'admin' !!!
    
    ($k1) = $T->fetch_keys_by_expr('EDT_TEST', "string_req='mno'");
    ($orig) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    $edt = $T->new_edt($perm_a, { FIXUP_MODE => 1, IMMEDIATE_MODE => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'insert test admin' });
    
    $T->ok_no_errors("no errors on insert_record with FIXUP_MODE");
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated admin' });
    
    $T->ok_no_errors("no permission violation on update_record with FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'updated admin', "update went through" );
    is( $new->{created}, $orig->{created}, "update did not change created date" );
    is( $new->{modified}, $orig->{modified}, "update did not change modified date" );
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'replaced admin' });
    
    $T->ok_no_errors("no permission violation on replace_record with FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'replaced admin', "replace went through" );
    is( $new->{created}, $orig->{created}, "replace did not change created date" );
    is( $new->{modified}, $orig->{modified}, "replace did not change modified date" );

    # Now do a double-check by creating another transaction without FIXUP_MODE and checking that
    # the modification date does change. Note that every time we create a new transaction the
    # previous one is automatically rolled back since it was never explicitly committed.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated admin 2' });
    
    $T->ok_no_errors("no permission violation on update_record without FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'updated admin 2', "update went through" );
    is( $new->{created}, $orig->{created}, "update did not change created date" );
    isnt( $new->{modified}, $orig->{modified}, "update DID change modified date" );
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'replaced admin 2' });
    
    $T->ok_no_errors("no permission violation on replace_record without FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'replaced admin 2', "replace went through" );
    is( $new->{created}, $orig->{created}, "replace did not change created date" );
    isnt( $new->{modified}, $orig->{modified}, "replace DID change modified date" );
    
    # Now try with explicitly setting modified => UNCHANGED.

    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, ALTER_TRAIL => 1 });

    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated admin 3',
				     modified => 'UNCHANGED' });
    
    $T->ok_no_errors("no permission violation on update_record");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'updated admin 3', "update went through" );
    is( $new->{created}, $orig->{created}, "update did not change created date" );
    is( $new->{modified}, $orig->{modified}, "update did not change modified date" );

    # Now we can test setting the created and modified timestamps explicitly.
    
    $k2 = $edt->insert_record('EDT_TEST', { string_req => 'explicit set', created => '2011-06-22',
				     modified => '2011-06-23 22:15:18' });

    $T->ok_no_conditions;
    $T->ok_found_record('EDT_TEST', "created = '2011-06-22' and modified = '2011-06-23 22:15:18'");

    # Now try modifying the inserted record with empty values for created and modified. This
    # should leave the created date unchanged and update the modified date, precisely as if
    # 'NORMAL' had been the value.
    
    if ( ok( $k2, "insert succeeded" ) )
    {
	($orig) = $T->fetch_records_by_key('EDT_TEST', $k2);
	$edt->update_record('EDT_TEST', { $primary => $k2, string_val => 'abc', modified => '', created => '' });
	($new) = $T->fetch_records_by_key('EDT_TEST', $k2);
	($now) = $T->dbh->selectrow_array("SELECT NOW()");
	
	$T->ok_no_errors('latest', "no errors on update_record setting created and modified to empty");
	is( $new->{created}, $orig->{created}, "created date was not changed by update");
	is( $new->{modified}, $now, "modified date was changed to current by update");
    }
    
    # Redo the modifications without ALTER_TRAIL, and check that we get a caution.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'explicit set without alter_trail',
				     created => '2011-06-24', modified => '2011-06-25' });
    
    $T->ok_has_error('latest', 'D_ALTER_TRAIL', "found ALTER_TRAIL caution");
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated no alter_trail',
				     created => '2011-06-26', modified => '2011-06-27' });
    
    $T->ok_has_error('latest', 'D_ALTER_TRAIL', "found ALTER_TRAIL caution");
};


# Check that a superuser can also change created and modified dates without restriction.

subtest 'crmod superuser' => sub {
    
    my ($edt, $k1, $k2, $orig, $new);
    
    $edt = $T->new_edt($perm_s);	# superuser
    
    ($k1) = $T->fetch_keys_by_expr('EDT_TEST', "string_req='mno'");
    ($orig) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    $edt = $T->new_edt($perm_s, { FIXUP_MODE => 1, IMMEDIATE_MODE => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'insert test superuser' });
    
    $T->ok_no_errors("no errors on insert_record with FIXUP_MODE");
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated superuser' });
    
    $T->ok_no_errors("no permission violation on update_record with FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'updated superuser', "update went through" );
    is( $new->{created}, $orig->{created}, "update did not change created date" );
    is( $new->{modified}, $orig->{modified}, "update did not change modified date" );
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'replaced superuser' });
    
    $T->ok_no_errors("no permission violation on replace_record with FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'replaced superuser', "replace went through" );
    is( $new->{created}, $orig->{created}, "replace did not change created date" );
    is( $new->{modified}, $orig->{modified}, "replace did not change modified date" );

    # Now do a double-check by creating another transaction without FIXUP_MODE and checking that
    # the modification date does change. Note that every time we create a new transaction the
    # previous one is automatically rolled back since it was never explicitly committed.
    
    $edt = $T->new_edt($perm_s, { IMMEDIATE_MODE => 1 });
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated superuser 2' });
    
    $T->ok_no_errors("no permission violation on update_record without FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'updated superuser 2', "update went through" );
    is( $new->{created}, $orig->{created}, "update did not change created date" );
    isnt( $new->{modified}, $orig->{modified}, "update DID change modified date" );
    
    $edt = $T->new_edt($perm_s, { IMMEDIATE_MODE => 1 });
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'replaced superuser 2' });
    
    $T->ok_no_errors("no permission violation on replace_record without FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'replaced superuser 2', "replace went through" );
    is( $new->{created}, $orig->{created}, "replace did not change created date" );
    isnt( $new->{modified}, $orig->{modified}, "replace DID change modified date" );
    
    # Now try with explicitly setting modified => UNCHANGED.

    $edt = $T->new_edt($perm_s, { IMMEDIATE_MODE => 1, ALTER_TRAIL => 1 });

    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated superuser 3',
				     modified => 'UNCHANGED' });
    
    $T->ok_no_errors("no permission violation on update_record");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'updated superuser 3', "update went through" );
    is( $new->{created}, $orig->{created}, "update did not change created date" );
    is( $new->{modified}, $orig->{modified}, "update did not change modified date" );

    # Now we can test setting the created and modified timestamps explicitly
    
    $edt->insert_record('EDT_TEST', { string_req => 'explicit set', created => '2011-06-22',
				     modified => '2011-06-23 22:15:18' });
    
    $T->ok_no_conditions;
    $T->ok_found_record('EDT_TEST', "created = '2011-06-22' and modified = '2011-06-23 22:15:18'");

    # Redo the modifications without ALTER_TRAIL, and check that we get a caution.
    
    $edt = $T->new_edt($perm_s, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'explicit set without alter_trail',
				     created => '2011-06-24', modified => '2011-06-25' });
    
    $T->ok_has_error('latest', 'D_ALTER_TRAIL', "found ALTER_TRAIL caution");
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated no alter_trail',
				     created => '2011-06-26', modified => '2011-06-27' });
    
    $T->ok_has_error('latest', 'D_ALTER_TRAIL', "found ALTER_TRAIL caution");
};


subtest 'authent' => sub {
    
    my ($edt, $result);
    
    # Start by clearing the table, and the 'admin' permission set by one of the previous tests.
    
    $T->clear_table('EDT_TEST');
    
    $T->clear_specific_permissions;
    $perm_a->clear_cached_permissions;
    
    # Then insert some records and check the authorizer_no and enterer_no values.
    
    $edt = $T->new_edt($perm_e);
    
    $edt->insert_record('EDT_TEST', { string_req => 'abc', unsigned_val => 3 });
    $edt->insert_record('EDT_TEST', { string_req => 'def', unsigned_val => 4 });
    $edt->insert_record('EDT_TEST', { string_req => 'ghi', unsigned_val => 5 });
    $edt->insert_record('EDT_TEST', { string_req => 'jkl', unsigned_val => 6 });
    $edt->insert_record('EDT_TEST', { string_req => 'mno', unsigned_val => 7 });
    $edt->insert_record('EDT_TEST', { string_req => 'pqr', unsigned_val => 8 });
    
    ok( $edt->commit, "insert records committed" ) || return;
    
    my ($k1, $k2, $k4, $k5) = $edt->inserted_keys;
    my $k3 = 99999;

    my ($r1) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $r1->{authorizer_no}, $person_a, "authorizer_no is correct" );
    is( $r1->{enterer_no}, $person_e, "enterer_no is correct" );
    is( $r1->{modifier_no}, 0, "modifier_no is correct" );
    
    # Now update one record and replace another, and check that the modifier_no value is
    # different. We also use 'replace' to insert a new record with a specified key.
    
    $edt = $T->new_edt($perm_s);
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated' });
    $edt->replace_record('EDT_TEST', { $primary => $k2, string_req => 'replaced' });
    $edt->replace_record('EDT_TEST', { $primary => $k3, string_req => 'new' });

    $T->ok_result( $edt->commit, "update records committed" ) || return;
    
    my ($u1) = $T->fetch_records_by_key('EDT_TEST', $k1);
    my ($u2) = $T->fetch_records_by_key('EDT_TEST', $k2);
    my ($u3) = $T->fetch_records_by_key('EDT_TEST', $k3);
    
    is( $u1->{authorizer_no}, $person_a, "authorizer_no has not changed on update" );
    is( $u1->{enterer_no}, $person_e, "enterer_no has not changed on update" );
    is( $u1->{modifier_no}, $person_s, "modifier_no was set properly on update" );
    
    is( $u2->{authorizer_no}, $person_a, "authorizer_no has not changed on replace" );
    is( $u2->{enterer_no}, $person_e, "enterer_no has not changed on replace" );
    is( $u2->{modifier_no}, $person_s, "modifier_no was set properly on replace" );
    
    is( $u3->{authorizer_no}, $person_s, "authorizer_no was set on new record" );
    is( $u3->{enterer_no}, $person_s, "enterer_no was set on new record" );
    is( $u3->{modifier_no}, 0, "modifier_no was set properly on new record" );
    
    is( $u1->{unsigned_val}, $r1->{unsigned_val}, "update did not change other field" );
    is( $u2->{unsigned_val}, '0', "replace removed other field" );
    
    $T->ok_result( $edt->commit, "transaction committed" );
    
    # Now update and replace again by the person who originally entered the records, and check
    # that modifier_no is changed again.
    
    $edt = $T->new_edt($perm_e);
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated 2' });
    $edt->replace_record('EDT_TEST', { $primary => $k2, string_req => 'replaced 2' });
    
    $T->ok_result( $edt->commit, "transaction committed" );

    my ($u1a) = $T->fetch_records_by_key('EDT_TEST', $k1);
    my ($u2a) = $T->fetch_records_by_key('EDT_TEST', $k2);
    
    is( $u1a->{authorizer_no}, $person_a, "authorizer_no has not changed on update" );
    is( $u1a->{enterer_no}, $person_e, "enterer_no has not changed on update" );
    is( $u1a->{modifier_no}, $person_e, "modifier_no was set properly on update" );
    is( $u1a->{string_req}, 'updated 2', "string_req was set properly on update" );
    
    is( $u2a->{authorizer_no}, $person_a, "authorizer_no has not changed on replace" );
    is( $u2a->{enterer_no}, $person_e, "enterer_no has not changed on replace" );
    is( $u2a->{modifier_no}, $person_e, "modifier_no was set properly on replace" );
    is( $u2a->{string_req}, 'replaced 2', "string_req was set properly on replace" );
};


subtest 'authent non-admin' => sub {
    
    # Try using 'fixup_mode' with the regular permissions. This should produce an error.
    
    my ($edt, $k1);
    
    ($k1) = $T->fetch_keys_by_expr('EDT_TEST', "string_req='mno'");
    
    $edt = $T->new_edt($perm_a, { FIXUP_MODE => 1, IMMEDIATE_MODE => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'insert test' });
    
    $T->ok_no_errors("no errors on insert_record with FIXUP_MODE");
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_val => 'updated' });
    
    $T->ok_has_error('E_PERM_COL', "got permission violation on update_record with FIXUP_MODE");
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'updated' });
    
    $T->ok_has_error('E_PERM_COL', "got permission violation on replace_record with FIXUP_MODE");
    
    # Make sure that neither update nor replacement went through.
    
    $T->ok_no_record('EDT_TEST', "$primary = $k1 and string_req = 'updated'");
    
    # Now try the same thing without FIXUP_MODE but explicitly setting modifier_no to UNCHANGED.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, ALTER_TRAIL => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'insert test', modifier_no => 'UNCHANGED' });
    
    $T->ok_has_error( qr{E_PERM_COL.*modifier_no}, "got permission violation on insert_record with modifier_no UNCHANGED");
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_val => 'updated',
				     modifier_no => 'UNCHANGED' });
    
    $T->ok_has_error( qr{E_PERM_COL.*modifier_no}, "got permission violation on update_record with modifier_no UNCHANGED");
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_val => 'updated',
				     modifier_no => 'UNCHANGED' });
    
    $T->ok_has_error( qr{E_PERM_COL.*modifier_no}, "got permission violation on update_record with modifier_no UNCHANGED");
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'updated',
				      modifier_no => 'UNCHANGED', modifier_no => 'UNCHANGED' });
    
    $T->ok_has_error( qr{E_PERM_COL.*modifier_no}, "got permission violation on replace_record with modifier_no UNCHANGED");
    $T->ok_has_error( qr{E_PERM_COL.*modifier_no}, "got permission violation on replace_record with modifier_no UNCHANGED");
    
    # Make sure that neither update nor replacement went through.
    
    $T->ok_no_record('EDT_TEST', "$primary = $k1 and string_req = 'updated'");
    
    # Now try to explicitly set values for authorizer_no, enterer_no and modifier_no. This should
    # also give an error.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, ALTER_TRAIL => 1 });
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated',
				     modifier_no => $person_a });
    
    $T->ok_has_error( qr{E_PERM_COL.*modifier_no}, "got permission violation on update_record setting modifier_no" );
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'updated',
				     modifier_no => 99999 });
    
    $T->ok_has_error( qr{E_PERM_COL.*modifier_no}, "got permission violation on replace_record setting modifier_no" );
    $T->ok_has_error( qr{E_KEY_NOT_FOUND.*modifier_no}, "got key not found on replace_record setting modifier_no" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'inserted 2', modifier_no => $person_a });
    
    $T->ok_has_error( qr{E_PERM_COL.*modifier_no}, "got permission violation on insert_record setting modifier_no" );
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated', authorizer_no => $person_a,
				     enterer_no => $person_a });
    
    $T->ok_has_error( qr{E_PERM_COL.*authorizer_no}, "got permission violation on update_record setting authorizer_no" );
    $T->ok_has_error( qr{E_PERM_COL.*enterer_no}, "got permission violation on update_record setting enterer_no" );
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'updated', authorizer_no => 'abc',
				      enterer_no => 'abc' });
    
    $T->ok_has_error( qr{E_PERM_COL.*authorizer_no}, "got permission violation on replace_record setting authorizer_no" );
    $T->ok_has_error( qr{E_FORMAT.*authorizer_no}, "got format error on replace_record setting authorizer_no" );
    $T->ok_has_error( qr{E_PERM_COL.*enterer_no}, "got permission violation on replace_record setting enterer_no" );
    $T->ok_has_error( qr{E_FORMAT.*enterer_no}, "got format error on replace_record setting enterer_no" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'inserted 2', enterer_no => 'int:2' });
    
    $T->ok_has_error( qr{E_PERM_COL.*enterer_no}, "got permission violation on insert_record setting enterer_no" );
    $T->ok_has_error( qr{E_EXTTYPE.*enterer_no}, "got external type error on insert_record setting enterer_no" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'inserted 3', authorizer_no => $perm_a,
				     modifier_no => $perm_a });
    
    $T->ok_has_error( qr{E_PERM_COL.*authorizer_no}, "got permission violation on insert_record setting authorizer_no" );
    $T->ok_has_error( qr{E_PERM_COL.*modifier_no}, "got permission violation on insert_record setting modifier_no" );
    
    # Make sure that neither update nor replacement went through. Also make sure that insertion
    # didn't go through.
    
    $T->ok_no_record('EDT_TEST', "$primary = $k1 and string_req = 'updated'");
    $T->ok_no_record('EDT_TEST', "string_req='inserted 3'");
};


subtest 'authent admin' => sub {
    
    # Now we give the authorizer 'admin' privilege on the table again.
    
    my ($edt, $k1, $k2, $orig, $new);
    
    $T->set_specific_permission('EDT_TEST', $perm_a, 'admin');
    $perm_a->clear_cached_permissions;
    
    # Now create a transaction and start inserting and updating.
    
    $edt = $T->new_edt($perm_a);	# now with 'admin' !!!
    
    ($k1) = $T->fetch_keys_by_expr('EDT_TEST', "string_req='mno'");
    ($orig) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    $edt = $T->new_edt($perm_a, { FIXUP_MODE => 1, IMMEDIATE_MODE => 1 });
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated admin' });
    
    $T->ok_no_errors("no permission violation on update_record with FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'updated admin', "update went through" );
    is( $new->{authorizer_no}, $orig->{authorizer_no}, "update did not change authorizer_no" );
    is( $new->{enterer_no}, $orig->{enterer_no}, "update did not change enterer_no" );
    is( $new->{modifier_no}, $orig->{modifier_no}, "update did not change modifier_no" );
    is( $new->{modified}, $orig->{modified}, "update did not change modified date" );
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'replaced admin' });
    
    $T->ok_no_errors("no permission violation on replace_record with FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'replaced admin', "replace went through" );
    is( $new->{authorizer_no}, $orig->{authorizer_no}, "replace did not change authorizer_no" );
    is( $new->{enterer_no}, $orig->{enterer_no}, "replace did not change enterer_no" );
    is( $new->{modifier_no}, $orig->{modifier_no}, "replace did not change modifier_no" );
    is( $new->{modified}, $orig->{modified}, "replace did not change modified date" );
    
    # Now do a double-check by creating another transaction without FIXUP_MODE and checking that
    # the modifier_no does change. Note that every time we create a new transaction the
    # previous one is automatically rolled back since it was never explicitly committed.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated admin 2' });
    
    $T->ok_no_errors("no permission violation on update_record without FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'updated admin 2', "update went through" );
    is( $new->{authorizer_no}, $orig->{authorizer_no}, "update did not change authorizer_no" );
    is( $new->{enterer_no}, $orig->{enterer_no}, "update did not change enterer_no" );
    isnt( $new->{modifier_no}, $orig->{modifier_no}, "update DID change modifier_no" );
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'replaced admin 2' });
    
    $T->ok_no_errors("no permission violation on replace_record without FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'replaced admin 2', "replace went through" );
    is( $new->{authorizer_no}, $orig->{authorizer_no}, "replace did not change authorizer_no" );
    is( $new->{enterer_no}, $orig->{enterer_no}, "replace did not change enterer_no" );
    isnt( $new->{modifier_no}, $orig->{modifier_no}, "replace DID change modifier_no" );
    
    # Now try with explicitly setting modified => UNCHANGED.

    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, ALTER_TRAIL => 1 });

    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated admin 3',
				     modifier_no => 'UNCHANGED' });
    
    $T->ok_no_errors("no permission violation on update_record");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'updated admin 3', "update went through" );
    is( $new->{authorizer_no}, $orig->{authorizer_no}, "replace did not change authorizer_no" );
    is( $new->{enterer_no}, $orig->{enterer_no}, "replace did not change enterer_no" );
    is( $new->{modifier_no}, $orig->{modifier_no}, "replace did not change modifier_no" );
    
    # Now we can test the various fields explicitly. In fact, test that we can set them using all
    # three possible formats: an unsigned integer, a external identifier string, and an external
    # identifier object.
    
    my $ext_s = PBDB::ExtIdent->new('prs', $person_s);
    
    $edt->insert_record('EDT_TEST', { string_req => 'explicit set', authorizer_no => $person_s,
				     enterer_no => "prs:$person_s", modifier_no => $ext_s });
    
    $T->ok_no_conditions;
    $T->ok_found_record('EDT_TEST', "string_req='explicit set' and authorizer_no=$person_s
		and enterer_no=$person_s and modifier_no=$person_s");
    
    # Try again without ALTER_TRAIL and check that we get a caution.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'explicit set without alter_trail',
				     authorizer_no => $person_s, enterer_no => $person_s });
    
    $T->ok_has_error('latest', 'D_ALTER_TRAIL', "found ALTER_TRAIL caution");
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated no alter_trail',
				     modifier_no => 'UNCHANGED' });
    
    $T->ok_has_error('latest', 'D_ALTER_TRAIL', "found ALTER_TRAIL caution");

    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated 2 no altera_trail',
				     authorizer_no => 'UNCHANGED', enterer_no => 'UNCHANGED' });

    $T->ok_no_conditions('latest', "no ALTER_TRAIl caution with 'UNCHANGED' for authorizer_no, enterer_no");
};


subtest 'authent superuser' => sub {
    
    my ($edt, $k1, $k2, $orig, $new);
    
    $edt = $T->new_edt($perm_s);	# superuser
    
    ($k1) = $T->fetch_keys_by_expr('EDT_TEST', "string_req='mno'");
    ($orig) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    $edt = $T->new_edt($perm_s, { FIXUP_MODE => 1, IMMEDIATE_MODE => 1 });
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated superuser' });
    
    $T->ok_no_errors("no permission violation on update_record with FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'updated superuser', "update went through" );
    is( $new->{authorizer_no}, $orig->{authorizer_no}, "update did not change authorizer_no" );
    is( $new->{enterer_no}, $orig->{enterer_no}, "update did not change enterer_no" );
    is( $new->{modifier_no}, $orig->{modifier_no}, "update did not change modifier_no" );
    is( $new->{modified}, $orig->{modified}, "update did not change modified date" );
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'replaced superuser' });
    
    $T->ok_no_errors("no permission violation on replace_record with FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'replaced superuser', "replace went through" );
    is( $new->{authorizer_no}, $orig->{authorizer_no}, "replace did not change authorizer_no" );
    is( $new->{enterer_no}, $orig->{enterer_no}, "replace did not change enterer_no" );
    is( $new->{modifier_no}, $orig->{modifier_no}, "replace did not change modifier_no" );
    is( $new->{modified}, $orig->{modified}, "replace did not change modified date" );
    
    # Now do a double-check by creating another transaction without FIXUP_MODE and checking that
    # the modifier_no does change. Note that every time we create a new transaction the
    # previous one is automatically rolled back since it was never explicitly committed.
    
    $edt = $T->new_edt($perm_s, { IMMEDIATE_MODE => 1 });
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated superuser 2' });
    
    $T->ok_no_errors("no permission violation on update_record without FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'updated superuser 2', "update went through" );
    is( $new->{authorizer_no}, $orig->{authorizer_no}, "update did not change authorizer_no" );
    is( $new->{enterer_no}, $orig->{enterer_no}, "update did not change enterer_no" );
    isnt( $new->{modifier_no}, $orig->{modifier_no}, "update DID change modifier_no" );
    
    $edt = $T->new_edt($perm_s, { IMMEDIATE_MODE => 1 });
    
    $edt->replace_record('EDT_TEST', { $primary => $k1, string_req => 'replaced superuser 2' });
    
    $T->ok_no_errors("no permission violation on replace_record without FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'replaced superuser 2', "replace went through" );
    is( $new->{authorizer_no}, $orig->{authorizer_no}, "replace did not change authorizer_no" );
    is( $new->{enterer_no}, $orig->{enterer_no}, "replace did not change enterer_no" );
    isnt( $new->{modifier_no}, $orig->{modifier_no}, "replace DID change modifier_no" );
    
    # Now try with explicitly setting modified => UNCHANGED.

    $edt = $T->new_edt($perm_s, { IMMEDIATE_MODE => 1, ALTER_TRAIL => 1 });

    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated superuser 3',
				     modifier_no => 'UNCHANGED' });
    
    $T->ok_no_errors("no permission violation on update_record");
    
    ($new) = $T->fetch_records_by_key('EDT_TEST', $k1);
    
    is( $new->{string_req}, 'updated superuser 3', "update went through" );
    is( $new->{authorizer_no}, $orig->{authorizer_no}, "replace did not change authorizer_no" );
    is( $new->{enterer_no}, $orig->{enterer_no}, "replace did not change enterer_no" );
    is( $new->{modifier_no}, $orig->{modifier_no}, "replace did not change modifier_no" );
    
    # Now we can test setting the various fields explicitly.
    
    my $ext_s = PBDB::ExtIdent->new('prs', $person_s);
    
    $edt->insert_record('EDT_TEST', { string_req => 'explicit set', authorizer_no => $person_s,
				     enterer_no => "prs:$person_s", modifier_no => $ext_s });
    
    $T->ok_no_conditions;
    $T->ok_found_record('EDT_TEST', "string_req='explicit set' and authorizer_no=$person_s
		and enterer_no=$person_s and modifier_no=$person_s");
    
    # Try again without ALTER_TRAIL an check that we get a caution.
    
    $edt = $T->new_edt($perm_s, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $edt->insert_record('EDT_TEST', { string_req => 'explicit set without alter_trail',
				     authorizer_no => $person_s, enterer_no => $person_s });
    
    $T->ok_has_error('latest', 'D_ALTER_TRAIL', "found ALTER_TRAIL caution");
    
    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated no alter_trail',
				     modifier_no => 'UNCHANGED' });
    
    $T->ok_has_error('latest', 'D_ALTER_TRAIL', "found ALTER_TRAIL caution");

    $edt->update_record('EDT_TEST', { $primary => $k1, string_req => 'updated 2 no altera_trail',
				     authorizer_no => 'UNCHANGED', enterer_no => 'UNCHANGED' });

    $T->ok_no_conditions('latest', "no ALTER_TRAIL caution with 'UNCHANGED' for authorizer_no, enterer_no");
};


