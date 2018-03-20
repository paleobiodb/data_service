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
use Test::More tests => 2;

use TableDefs qw($EDT_TEST $EDT_AUX $EDT_ANY
		 get_table_property set_table_property set_column_property);

use TableData qw(reset_cached_column_properties);

use EditTest;
use EditTester;

use Carp qw(croak);
use Encode;


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;



my ($perm_a, $perm_e, $primary);


subtest 'setup' => sub {
    
    # Grab the permissions that we will need for testing.
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) or die;
    
    $perm_e = $T->new_perm('SESSION-ENTERER');
    
    # Grab the name of the primary key of our test table.
    
    $primary = get_table_property($EDT_TEST, 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || die;
    
    # Clear any specific table permissions.
    
    $T->clear_specific_permissions;
};


# Check that required column values are actually required.

subtest 'required' => sub {
    
    # Clear the table, so that we can track record insertions.
    
    $T->clear_table($EDT_TEST);
    
    # Then try inserting some records.
    
    my ($edt, $result, $key1);
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED_MODE => 1 });
    
    $key1 = $edt->insert_record($EDT_TEST, { string_req => 'abc' });

    $T->ok_result( $key1, "inserted record with required field" ) || return;
    
    $result = $edt->insert_record($EDT_TEST, { string_val => 'def' });

    ok( ! $result, "could not insert record with required field missing" );
    
    $result = $edt->insert_record($EDT_TEST, { string_req => '', string_val => 'def' });
    
    ok( ! $result, "could not insert record with empty value for required field" );
    
    $result = $edt->insert_record($EDT_TEST, { string_req => undef, string_val => 'def' });

    ok( ! $result, "could not insert record with undefined value for required field" );
    
    $result = $edt->update_record($EDT_TEST, { $primary => $key1, string_req => 'def' });

    ok( $result, "updated record with required field" ) || $T->last_errors;

    $result = $edt->update_record($EDT_TEST, { $primary => $key1, signed_val => '5' });

    ok( $result, "updated record with required field missing" ) || $T->last_errors;
    
    $result = $edt->update_record($EDT_TEST, { $primary => $key1, string_req => '' });
    
    ok( ! $result, "could not update record with required field set to empty" );

    $result = $edt->update_record($EDT_TEST, { $primary => $key1, string_req => undef });

    ok( ! $result, "could not update record with required field set to undef" );
    
    $result = $edt->replace_record($EDT_TEST, { $primary => $key1, string_req => 'ghi' });

    ok( $result, "replaced record with required field" ) || $T->last_errors;;

    $result = $edt->replace_record($EDT_TEST, { $primary => $key1, signed_val => 6 });

    ok( ! $result, "could not replace record with required field missing" );

    $result = $edt->replace_record($EDT_TEST, { $primary => $key1, string_req => '' });

    ok( ! $result, "could not replace record with required field empty" );

    $result = $edt->replace_record($EDT_TEST, { $primary => $key1, string_req => undef });

    ok( ! $result, "could not replace record with required field undefined" );

    $T->ok_found_record( $EDT_TEST, "string_req='ghi'", "found replaced record as a check" );
    
};


subtest 'foreign keys' => sub {
    
    
    
    
};
