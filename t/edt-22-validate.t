#
# PBDB Data Service
# -----------------
# 
# This file contains unit tests for the EditTransaction class.
# 
# edt-22-validate.t : Test that validation of a request against the database schema works
# properly: the only values which are let through are those which can be stored in the table, and
# errors are generated for others.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use TableDefs qw(get_table_property set_table_property set_column_property);

use TableData qw(reset_cached_column_properties);
use ExternalIdent;

use EditTest;
use EditTester;

use Carp qw(croak);
use Encode;


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;

$T->set_table('EDT_TEST');


my ($perm_a, $perm_e, $primary);


subtest 'setup' => sub {
    
    # Grab the permissions that we will need for testing.
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) or die;
    
    $perm_e = $T->new_perm('SESSION-ENTERER');
    
    # Grab the name of the primary key of our test table.
    
    $primary = get_table_property('EDT_TEST', 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || die;
    
    # Clear any specific table permissions.
    
    $T->clear_specific_permissions;
};


# Check that required column values are actually required.

subtest 'required' => sub {
    
    # Clear the table, so that we can track record insertions.
    
    $T->clear_table('EDT_TEST');
    
    # Then try inserting some records.
    
    my ($edt, $result, $key1);
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $key1 = $edt->insert_record('EDT_TEST', { string_req => 'abc' });
    
    $T->ok_result( $key1, "inserted record with required field" ) || return;
    
    $result = $edt->insert_record('EDT_TEST', { string_val => 'def' });
    
    ok( ! $result, "could not insert record with required field missing" );
    
    $result = $edt->insert_record('EDT_TEST', { string_req => '', string_val => 'def' });
    
    ok( ! $result, "could not insert record with empty value for required field" );
    
    $result = $edt->insert_record('EDT_TEST', { string_req => undef, string_val => 'def' });
    
    ok( ! $result, "could not insert record with undefined value for required field" );
    
    $result = $edt->update_record('EDT_TEST', { $primary => $key1, string_req => 'def' });
    
    ok( $result, "updated record with required field" ) || $T->diag_errors;
    
    $result = $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => '5' });
    
    ok( $result, "updated record with required field missing" ) || $T->diag_errors;
    
    $result = $edt->update_record('EDT_TEST', { $primary => $key1, string_req => '' });
    
    ok( ! $result, "could not update record with required field set to empty" );
    
    $result = $edt->update_record('EDT_TEST', { $primary => $key1, string_req => undef });
    
    ok( ! $result, "could not update record with required field set to undef" );
    
    $result = $edt->replace_record('EDT_TEST', { $primary => $key1, string_req => 'ghi' });
    
    ok( $result, "replaced record with required field" ) || $T->diag_errors;
    
    $result = $edt->replace_record('EDT_TEST', { $primary => $key1, signed_val => 6 });
    
    ok( ! $result, "could not replace record with required field missing" );
    
    $result = $edt->replace_record('EDT_TEST', { $primary => $key1, string_req => '' });
    
    ok( ! $result, "could not replace record with required field empty" );
    
    $result = $edt->replace_record('EDT_TEST', { $primary => $key1, string_req => undef });
    
    ok( ! $result, "could not replace record with required field undefined" );
    
    $T->ok_found_record( 'EDT_TEST', "string_req='ghi'", "found replaced record as a check" );
    
};


# Now test that foreign key data is properly checked and entered.

subtest 'foreign keys' => sub {
    
    # Clear the table, so that we can track record insertions.
    
    $T->clear_table('EDT_TEST');
    
    # Find a good foreign key value and a non-existent one.
    
    my ($int_good) = $T->fetch_row_by_expr('INTERVAL_DATA', 'interval_no',
					   "interval_name='Cretaceous'");

    my ($int_alt) = $T->fetch_row_by_expr('INTERVAL_DATA', 'interval_no',
					  "interval_name='Permian'");
    
    my ($int_bad) = $T->fetch_row_by_expr('INTERVAL_DATA', 'max(interval_no) + 1');
    
    unless ( $int_good && $int_alt && $int_bad )
    {
	diag("aborting subtest: could not fetch necessary keys");
	return;
    }
    
    # Then try inserting some records with good and bad keys.
    
    my ($edt, $result, $key1, $key2, $key3);
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $key1 = $edt->insert_record('EDT_TEST', { string_req => 'foreign key test', interval_no => $int_good });
    
    $T->ok_result( $key1, "inserted record with valid foreign key" ) || return;
    
    $edt->insert_record('EDT_TEST', { string_req => 'foreign key test', interval_no => $int_bad });
    
    $T->ok_has_one_error('latest', 'F_KEY_NOT_FOUND', "bad key is recognized");
    $T->ok_no_warnings('latest');
    
    # Now try generating PBDB:ExtIdent objects instead of integer keys.
    
    my $ext_good = PBDB::ExtIdent->new('int', $int_good);
    
    ok( $ext_good, "generated good external ident for interval" );
    
    my $ext_bad = PBDB::ExtIdent->new('int', $int_bad);
    
    ok( $ext_bad, "generated bad external ident for interval" );

    $key2 = $edt->insert_record('EDT_TEST', { string_req => 'external ident test', interval_no => $ext_good });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key2 and interval_no = $int_good") || return;
    
    $edt->update_record('EDT_TEST', { $primary => $key2, interval_no => $ext_bad });
    
    $T->ok_has_one_error('latest', 'F_KEY_NOT_FOUND', "bad external identifier is recognized");
    $T->ok_no_warnings('latest');
    
    # Try a PBDB::ExtIdent with the wrong type.
    
    my $ext_wrong = PBDB::ExtIdent->new('prs', $int_good);
    
    ok( $ext_wrong, "generated external ident of wrong type" );
    
    $edt->update_record('EDT_TEST', { $primary => $key2, interval_no => $ext_wrong });

    $T->ok_has_one_error('latest', 'F_EXTTYPE', "bad external identifier type is recognized");
    $T->ok_no_warnings('latest');
    
    # Now do the same with unparsed external identifiers.

    my $urn_good = "paleobiodb.org:int:$int_good";
    my $urn_bad = "paleobiodb.org:int:$int_bad";
    my $urn_ugly = "paleobiodb.org:blargh:23";
    my $urn_hideous = "xxxz";
    
    $edt->update_record('EDT_TEST', { $primary => $key2, interval_no => $urn_good });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key2 and interval_no = $int_good");
    
    $edt->update_record('EDT_TEST', { $primary => $key2, interval_no => $urn_bad });
    
    $T->ok_has_one_error('latest', 'F_KEY_NOT_FOUND', "bad urn key value is recognized");
    $T->ok_found_record('EDT_TEST', "$primary = $key2 and interval_no = $int_good");
    
    $edt->update_record('EDT_TEST', { $primary => $key2, interval_no => $urn_ugly });
    
    $T->ok_has_one_error('latest', 'F_EXTTYPE', "bad urn type is recognized");
    $T->ok_found_record('EDT_TEST', "$primary = $key2 and interval_no = $int_good");
    
    $edt->update_record('EDT_TEST', { $primary => $key2, interval_no => $urn_hideous });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "bad urn format is recognized");
    $T->ok_found_record('EDT_TEST', "$primary = $key2 and interval_no = $int_good");
    
    # Now try overriding the identifier type.
    
    set_column_property('EDT_TEST', 'interval_no', EXTID_TYPE => 'TXN');
    reset_cached_column_properties('EDT_TEST', 'interval_no');
    
    # Test that the new identifier type works and the old one does not.
    
    $key3 = $edt->insert_record('EDT_TEST', { string_req => 'external ident override',
					     interval_no => "txn:$int_good" });
    
    
    $T->ok_result( $key3, "inserted record with overridden external ident type" ) || return;
    
    $edt->update_record('EDT_TEST', { $primary => $key3, interval_no => "int:$int_alt" });
    
    $T->ok_has_one_error('latest', 'F_EXTTYPE', "override of external ident type is recognized");
    $T->ok_found_record('EDT_TEST', "$primary = $key3 and interval_no = $int_good");
    
    # Now test that we can set field values using _id in place of _no.
    
    $edt->update_record('EDT_TEST', { $primary => $key3, interval_id => "txn:$int_alt" });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key3 and interval_no = $int_alt");
    
    # Now test that we can set the field to zero, and to null.
    
    $edt->update_record('EDT_TEST', { $primary => $key3, interval_id => 0 });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key3 and interval_no = 0");
    
    $edt->update_record('EDT_TEST', { $primary => $key3, interval_id => $int_alt });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key3 and interval_no = $int_alt");
    
    $edt->update_record('EDT_TEST', { $primary => $key3, interval_id => undef });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key3 and interval_no = 0");
};


# Then test foreign keys that are specifically indicated by the FOREIGN_TABLE and FOREIGN_KEY
# properties.

subtest 'foreign_table' => sub {
    
    # Clear the table, so that we can track record insertions.
    
    $T->clear_table('EDT_TEST');
    $T->clear_table('EDT_AUX');
    
    # Find a good foreign key value and a non-existent one.
    
    my ($int_good) = $T->fetch_row_by_expr('INTERVAL_DATA', 'interval_no',
					   "interval_name='Ordovician'");
    
    # my ($int_bad) = $T->fetch_row_by_expr('INTERVAL_DATA', 'max(interval_no) + 1');
    
    unless ( $int_good )
    {
	diag("aborting subtest: could not fetch necessary keys");
	return;
    }
    
    # Then try inserting some records with good and bad keys.
    
    my ($edt, $result, $key1, $key2, $key3);
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $key1 = $edt->insert_record('EDT_TEST', { string_req => 'foreign table a' });
    
    $T->ok_result($key1, "inserted test record") || return;
    
    $key2 = $edt->insert_record('EDT_TEST', { string_req => 'foreign table b' });
    
    $T->ok_no_conditions;
    
    $key3 = $edt->insert_record('EDT_AUX', { name => 'good', test_id => $key2 });
    
    $T->ok_no_conditions;
    $T->ok_found_record('EDT_AUX', "aux_no = $key3 and test_no = $key2");
    
    $edt->update_record('EDT_AUX', { aux_no => $key3, test_id => $int_good });
    
    $T->ok_has_one_error('latest', 'F_BAD_UPDATE', "error condition for link value change" );
    $T->ok_no_warnings('latest');
    $T->ok_no_record('EDT_AUX', "aux_no = $key3 and test_no = $int_good");
    
    # Now specifically redirect this column to a different table/key combination. The same key
    # value should now work.
    
    set_column_property('EDT_AUX', 'test_no', FOREIGN_TABLE => 'INTERVAL_DATA');
    set_column_property('EDT_AUX', 'test_no', FOREIGN_KEY => 'interval_no');
    set_table_property('EDT_AUX', PERMISSION_TABLE => undef);
    
    reset_cached_column_properties('EDT_AUX', 'test_no');
    
    $edt->update_record('EDT_AUX', { aux_no => $key3, test_id => $int_good });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_AUX', "aux_no = $key3 and test_no = $int_good");
};


# Now test that a validator subroutine specified by column properties is properly called.

subtest 'validators' => sub {
    
    # Clear the table, so that we can track record insertions.
    
    $T->clear_table('EDT_TEST');
    
    # Then try inserting some records.
    
    my ($edt, $result, $key1);
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $key1 = $edt->insert_record('EDT_TEST', { string_req => 'abc' });
    
    $T->ok_result($key1, "inserted test record") || return;
    
    $edt->update_record('EDT_TEST', { $primary => $key1, string_req => 'validator test',
				     string_val => 'abcdefghij' });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "found error condition from validator");
    $T->ok_has_one_error('latest',  qr{string_val.*EditTransaction::Action}xs, "error condition had proper info" );
};
