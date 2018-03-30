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
use Test::More tests => 6;

use TableDefs qw($EDT_TEST get_table_property set_table_property);

use EditTest;
use EditTester;


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;


# Start by getting the variable values that we need to execute the remainder of the test. If the
# session key 'SESSION-AUTHORIZER' does not appear in the session_data table, then run the test
# 'edt-01-basic.t' first.

my ($perm_a, $perm_e, $perm_s, $primary);

subtest 'setup' => sub {

    # We need to set this property in order to test what happens when a new record is inserted
    # using a 'replace' operation.
    
    set_table_property($EDT_TEST, ALLOW_INSERT_KEY => 1 );

    # Clear any special permissions that may have been set.
    
    $T->clear_specific_permissions;
    
    # Retrieve the necessary permissions. If we can't find any of them, then bail.
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;
    
    $perm_e = $T->new_perm('SESSION-ENTERER');
    
    ok( $perm_e && $perm_e->role eq 'enterer', "found enterer permission" ) || BAIL_OUT;
    
    $perm_s = $T->new_perm('SESSION-SUPERUSER');
    
    ok( $perm_s && $perm_s->is_superuser, "found superuser permission" ) || BAIL_OUT;
    
    $primary = get_table_property($EDT_TEST, 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || BAIL_OUT;
};


# Check that record operations set and update the created and modified dates properly. Also check
# that these can be specifically set only with administrator or superuser permission.

subtest 'crmod' => sub {
    
    my ($edt, $result);
    
    # Start by clearing the table.

    $T->clear_table($EDT_TEST);

    # Then insert some records and check the created and modified timestamps.
    
    $edt = $T->new_edt($perm_e);
    
    $edt->insert_record($EDT_TEST, { string_req => 'abc', unsigned_val => 3 });
    $edt->insert_record($EDT_TEST, { string_req => 'def', unsigned_val => 4 });
    $edt->insert_record($EDT_TEST, { string_req => 'ghi', unsigned_val => 5 });
    $edt->insert_record($EDT_TEST, { string_req => 'jkl', unsigned_val => 6 });
    $edt->insert_record($EDT_TEST, { string_req => 'mno', unsigned_val => 7 });
    $edt->insert_record($EDT_TEST, { string_req => 'pqr', unsigned_val => 8 });
    
    my ($now) = $T->dbh->selectrow_array("SELECT NOW()");
    
    ok( $edt->commit, "insert records committed" ) || return;
    ok( $now, "fetched current timestamp" ) || return;
    
    my ($k1, $k2, $k4, $k5) = $edt->inserted_keys;
    my $k3 = 99999;
    
    my ($r1) = $T->fetch_records_by_key($EDT_TEST, $k1);
    my ($r2) = $T->fetch_records_by_key($EDT_TEST, $k2);
    
    is( $r1->{created}, $now, "created timestamp is current" );
    is( $r1->{modified}, $now, "modified timestamp is current" );
    
    # Now update one record and replace another, and check that the modified timestamp is different. We
    # need to wait for at least 1 second to make sure of this. Also use 'replace' to insert a new record.
    
    sleep(1);
    
    my ($orig_stamp) = $now;
    
    $edt = $T->new_edt($perm_a);
    
    $edt->update_record($EDT_TEST, { $primary => $k1, string_req => 'updated' });
    $edt->replace_record($EDT_TEST, { $primary => $k2, string_req => 'replaced' });
    $edt->replace_record($EDT_TEST, { $primary => $k3, string_req => 'new' });
    
    ($now) = $T->dbh->selectrow_array("SELECT NOW()");
    
    $T->ok_result( $edt->commit, 'any', "update records committed" ) || return;
    
    my ($u1) = $T->fetch_records_by_key($EDT_TEST, $k1);
    my ($u2) = $T->fetch_records_by_key($EDT_TEST, $k2);
    my ($u3) = $T->fetch_records_by_key($EDT_TEST, $k3);
    
    is( $u1->{created}, $orig_stamp, "created timestamp has not changed on update" );
    is( $u1->{modified}, $now, "modified timestamp is current on update" );

    is( $u2->{created}, $orig_stamp, "created timestamp has not changed on replace" );
    is( $u2->{modified}, $now, "modified timestamp is current on replace" );
    
    is( $u3->{created}, $now, "created timestamp has not changed on replace" );
    is( $u3->{modified}, $now, "modified timestamp is current on replace" );
    
    isnt( $now, $orig_stamp, "at least one second has elapsed" );

    is( $u1->{unsigned_val}, $r1->{unsigned_val}, "update did not change other field" );
    is( $u2->{unsigned_val}, '0', "replace removed other field" );

    # Now have somebody else (the corresponding authorizer) update the record, and check that the
    # crmod fields are still updated properly.

    $edt = $T->new_edt($perm_a);

    $edt->update_record($EDT_TEST, { $primary => $k4, string_req => 'updated by other' });
    $edt->replace_record($EDT_TEST, { $primary => $k5, string_req => 'replaced by other' });
    
    ($now) = $T->dbh->selectrow_array("SELECT NOW()");
    
    $T->ok_result( $edt->commit, 'any', "update other committed" ) || return;
    
    my ($u4) = $T->fetch_records_by_key($EDT_TEST, $k4);
    my ($u5) = $T->fetch_records_by_key($EDT_TEST, $k5);

    is( $u4->{created}, $orig_stamp, "created timestamp has not changed on update" );
    is( $u4->{modified}, $now, "modified timestamp is current on update" );
    
    is( $u5->{created}, $orig_stamp, "created timestamp has not changed on replace" );
    is( $u5->{modified}, $now, "modified timestamp is current on replace" );
};


# Now check that crmod fields are not allowed to be messed with unless 'admin' permission on the
# table is held.

subtest 'crmod non-admin' => sub {
    
    # Try using 'fixup_mode' with the regular permissions. This should produce an error.
    
    my ($edt, $k1);

    ($k1) = $T->fetch_keys_by_expr($EDT_TEST, "string_req='mno'");
    
    $edt = $T->new_edt($perm_a, { FIXUP_MODE => 1, IMMEDIATE_MODE => 1 });
    
    $edt->insert_record($EDT_TEST, { string_req => 'insert test' });
    
    $T->ok_no_errors("no errors on insert_record with FIXUP_MODE");
    
    $edt->update_record($EDT_TEST, { $primary => $k1, string_val => 'updated' });
    
    $T->ok_has_error('E_PERM_COL', "got permission violation on update_record with FIXUP_MODE");
    
    $edt->replace_record($EDT_TEST, { $primary => $k1, string_req => 'updated' });
    
    $T->ok_has_error('E_PERM_COL', "got permission violation on replace_record with FIXUP_MODE");
    
    # Make sure that neither update nor replacement went through.
    
    $T->ok_no_record($EDT_TEST, "$primary = $k1 and string_req = 'updated'");
    
    # Now try the same thing without FIXUP_MODE but explicitly setting modified to UNCHANGED.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, ALTER_TRAIL => 1 });
    
    $edt->insert_record($EDT_TEST, { string_req => 'insert test', modified => 'UNCHANGED' });
    
    $T->ok_has_error( qr{E_PERM_COL.*modified}, "got permission violation on insert_record with modified UNCHANGED");
    
    $edt->update_record($EDT_TEST, { $primary => $k1, string_val => 'updated',
				     modified => 'UNCHANGED' });
    
    $T->ok_has_error( qr{E_PERM_COL.*modified}, "got permission violation on update_record with modified UNCHANGED");
    
    $edt->update_record($EDT_TEST, { $primary => $k1, string_val => 'updated',
				     modifier_no => 'UNCHANGED' });
    
    $T->ok_has_error( qr{E_PERM_COL.*modifier_no}, "got permission violation on update_record with modified UNCHANGED");
    
    $edt->replace_record($EDT_TEST, { $primary => $k1, string_req => 'updated',
				      modified => 'UNCHANGED', modifier_no => 'UNCHANGED' });
    
    $T->ok_has_error( qr{E_PERM_COL.*modified}, "got permission violation on replace_record with modified UNCHANGED");
    $T->ok_has_error( qr{E_PERM_COL.*modifier_no}, "got permission violation on replace_record with modifier_no UNCHANGED");
    
    # Make sure that neither update nor replacement went through.
    
    $T->ok_no_record($EDT_TEST, "$primary = $k1 and string_req = 'updated'");
    
    # Now try to explicitly set values for created and modified. This should also give an error.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, ALTER_TRAIL => 1 });
    
    $edt->update_record($EDT_TEST, { $primary => $k1, string_req => 'updated',
				     modified => '2010-01-01 12:51:42' });
    
    $T->ok_has_error( qr{E_PERM_COL.*modified}, "got permission violation on update_record setting modified" );
    
    $edt->replace_record($EDT_TEST, { $primary => $k1, string_req => 'updated',
				     modified => '2010-02-03 12:51:08' });
    
    $T->ok_has_error( qr{E_PERM_COL.*modified}, "got permission violation on replace_record setting modified" );

    $edt->insert_record($EDT_TEST, { string_req => 'inserted 2', modified => '2010-03-04 01:01:02' });

    $T->ok_has_error( qr{E_PERM_COL.*modified}, "got permission violation on insert_record setting modified" );

    $edt->update_record($EDT_TEST, { $primary => $k1, string_req => 'updated',
				     created => '2010-01-01 abc' });
    
    $T->ok_has_error( qr{E_PERM_COL.*created}, "got permission violation on update_record setting created" );
    $T->ok_has_error( qr{E_FORMAT.*created}, "got format error on update_record setting created" );

     $edt->replace_record($EDT_TEST, { $primary => $k1, string_req => 'updated',
				     created => '2010-01-01 def' });
    
    $T->ok_has_error( qr{E_PERM_COL.*created}, "got permission violation on replace_record setting created" );
    $T->ok_has_error( qr{E_FORMAT.*created}, "got format error on replace_record setting created" );
    
    $edt->insert_record($EDT_TEST, { string_req => 'inserted 2', created => '2010-01-01 ghi' });
    
    $T->ok_has_error( qr{E_PERM_COL.*created}, "got permission violation on insert_record setting created" );
    $T->ok_has_error( qr{E_FORMAT.*created}, "got format error on insert_record setting created" );

    $edt->insert_record($EDT_TEST, { string_req => 'inserted 2', created => '2011-01-01',
				     modified => '2011-01-01' });
    
    $T->ok_has_error( qr{E_PERM_COL.*created}, "got permission violation on insert_record setting created" );
    $T->ok_has_error( qr{E_PERM_COL.*modified}, "got permission violation on insert_record setting modified" );
    
    # Make sure that neither update nor replacement went through. Also make sure that insertion
    # didn't go through.
    
    $T->ok_no_record($EDT_TEST, "$primary = $k1 and string_req = 'updated'");
    $T->ok_no_record($EDT_TEST, "string_req='inserted 3'");
};


# Then check that with 'admin' permission these restrictions are lifted.

subtest 'crmod admin' => sub {

    # This time we give the authorizer 'admin' privilege on the table.
    
    my ($edt, $k1, $k2, $orig, $new);
    
    $T->set_specific_permission($EDT_TEST, $perm_a, 'admin');
    $perm_a->clear_cached_permissions;
    
    $edt = $T->new_edt($perm_a);	# now with 'admin' !!!
    
    ($k1) = $T->fetch_keys_by_expr($EDT_TEST, "string_req='mno'");
    ($orig) = $T->fetch_records_by_key($EDT_TEST, $k1);
    
    $edt = $T->new_edt($perm_a, { FIXUP_MODE => 1, IMMEDIATE_MODE => 1 });
    
    $edt->insert_record($EDT_TEST, { string_req => 'insert test admin' });
    
    $T->ok_no_errors("no errors on insert_record with FIXUP_MODE");
    
    $edt->update_record($EDT_TEST, { $primary => $k1, string_req => 'updated admin' });
    
    $T->ok_no_errors("no permission violation on update_record with FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key($EDT_TEST, $k1);
    
    is( $new->{string_req}, 'updated admin', "update went through" );
    is( $new->{created}, $orig->{created}, "update did not change created date" );
    is( $new->{modified}, $orig->{modified}, "update did not change modified date" );
    
    $edt->replace_record($EDT_TEST, { $primary => $k1, string_req => 'replaced admin' });
    
    $T->ok_no_errors("no permission violation on replace_record with FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key($EDT_TEST, $k1);
    
    is( $new->{string_req}, 'replaced admin', "replace went through" );
    is( $new->{created}, $orig->{created}, "replace did not change created date" );
    is( $new->{modified}, $orig->{modified}, "replace did not change modified date" );

    # Now do a double-check by creating another transaction without FIXUP_MODE and checking that
    # the modification date does change. Note that every time we create a new transaction the
    # previous one is automatically rolled back since it was never explicitly committed.
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt->update_record($EDT_TEST, { $primary => $k1, string_req => 'updated admin 2' });
    
    $T->ok_no_errors("no permission violation on update_record without FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key($EDT_TEST, $k1);
    
    is( $new->{string_req}, 'updated admin 2', "update went through" );
    is( $new->{created}, $orig->{created}, "update did not change created date" );
    isnt( $new->{modified}, $orig->{modified}, "update DID change modified date" );
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt->replace_record($EDT_TEST, { $primary => $k1, string_req => 'replaced admin 2' });
    
    $T->ok_no_errors("no permission violation on replace_record without FIXUP_MODE");
    
    ($new) = $T->fetch_records_by_key($EDT_TEST, $k1);
    
    is( $new->{string_req}, 'replaced admin 2', "replace went through" );
    is( $new->{created}, $orig->{created}, "replace did not change created date" );
    isnt( $new->{modified}, $orig->{modified}, "replace DID change modified date" );
    
    # Now try with explicitly setting modified => UNCHANGED.

    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, ALTER_TRAIL => 1 });

    $edt->update_record($EDT_TEST, { $primary => $k1, string_req => 'updated admin 3',
				     modified => 'UNCHANGED' });
    
    $T->ok_no_errors("no permission violation on update_record");
    
    ($new) = $T->fetch_records_by_key($EDT_TEST, $k1);
    
    is( $new->{string_req}, 'updated admin 3', "update went through" );
    is( $new->{created}, $orig->{created}, "update did not change created date" );
    is( $new->{modified}, $orig->{modified}, "update did not change modified date" );

    # Now we can test setting the created and modified timestamps explicitly. We get a caution if
    # 'ALTER_TRAIL' is not allowed.
    
    $edt->insert_record($EDT_TEST, { string_req => 'explicit set', created => '2011-23-05',
				     modified => '2011-23-06' });

    $T->ok_has_error('D_ALTER_TRAIL', "found ALTER_TRAIL caution");
    $T->diag_errors;
    
    # $T->ok_no_errors("both created and modified were set properly");
    # $T->ok_found_record($EDT_TEST, "created = '2011-23-05' and modified = '2011-23-06'");
    
    # $edt->commit;
};


subtest 'crmod superuser' => sub {
    
    pass('placeholder');
    
    
};


subtest 'authent' => sub {
    
    pass('placeholder');
    
};
