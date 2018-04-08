#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-20-insert.t : Test the operation of the 'insert_records' method.
# 


use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 2;

use TableDefs qw(get_table_property);

use EditTest qw($EDT_TEST);
use EditTester;


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;


# Start by getting the variable values that we need to execute the remainder of the test. If the
# session key 'SESSION-AUTHORIZER' does not appear in the session_data table, then run the test
# 'edt-01-basic.t' first.

my ($perm_a, $primary);

subtest 'setup' => sub {
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;
    
    $primary = get_table_property($EDT_TEST, 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || BAIL_OUT;
};


# Check that insert_records works only with the allowance CREATE.

subtest 'create' => sub {
    
    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table($EDT_TEST);
    
    # Then create an edt without CREATE mode and try to insert.

    $edt = $T->new_edt($perm_a, { CREATE => 0 });
    
    $result = $edt->insert_record($EDT_TEST, { string_req => 'test a1' });

    ok( ! $result, "insert_record returned false" );
    $T->ok_has_error( qr/C_CREATE/, "found caution C_CREATE" );

    
};
