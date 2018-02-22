#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-10-transaction.t : Test that transaction execution, commit, and rollback work properly, and
# that the proper results and keys are returned from the relevant calls.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use TableDefs qw($EDT_TEST);

use EditTest;
use EditTester;


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;



my ($perm_a);


subtest 'setup' => sub {
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');

    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;
};


# Check that executing a transaction works properly, and that the proper results are returned by
# each of the relevant calls.

subtest 'execute' => sub {

    my ($edt, $result);
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table($EDT_TEST);
    
    # Then run through a transaction that creates a record. We need to execute this one first so
    # that we can find out the key of the newly inserted record.

    $edt = $T->new_edt($perm_a);
    
    $edt->insert_record($EDT_TEST, { string_req => 'execute test' });
    $edt->execute;
    
    my ($keyval) = $edt->inserted_keys;

    ok( $keyval, "inserted initial record" ) || return;
    
    # Then we run through a transaction that includes all four operations, and check that the
    # various status calls work properly.
    
    $edt = $T->new_edt($perm_a);

    is( $edt->transaction, '', "transaction status init" );
    ok( ! $edt->has_started, "transaction has not started" );
    ok( ! $edt->is_active, "transaction is not active" );
    ok( ! $edt->has_finished, "transaction has not finished" );
};


# Do the same checks under IMMEDIATE_MODE.

subtest 'immediate' => sub {

    pass("placeholder");






};


# Do the same checks under PROCEED_MODE.

subtest 'proceed' => sub {

    pass("placeholder");






};


# And again under both IMMEDIATE_MODE and PROCEED_MODE.

subtest 'immediate and proceed' => sub {

    pass("placeholder");






};

