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


# Do some record operations, and make sure that the record counts and labels match up properly.

subtest 'basic' => sub {
    
    my ($edt, $result);
    
    pass('placeholder');
};


