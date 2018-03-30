#
# PBDB Data Service
# -----------------
# 
# This file contains unit tests for the EditTransaction class.
# 
# edt-15-interlock.t : Test that starting an EditTransaction while another one is already active
# will abort the first one.
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


# check that starting a new EditTransaction while a previous one is active will abort it.

subtest 'interlock' => sub {
    
    # Clear the table, so that we can track record insertions.
    
    $T->clear_table($EDT_TEST);
    
    # Then initiate one transaction. We use IMMEDIATE_MODE to make sure that the database manager
    # has actually been told to start the transaction.
    
    my ($edt1, $edt2, $key1, $key2);
    
    $edt1 = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED_MODE => 1 });
    
    $key1 = $edt1->insert_record($EDT_TEST, { string_req => 'interlock 1' });
    
    $T->ok_result($key1, "inserted test record 1") || return;
    $T->ok_found_record($EDT_TEST, "$primary = $key1 and string_req = 'interlock 1'");
    
    # Now initiate a second one, and test that the first one was rolled back.
    
    $edt2 = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED_MODE => 1 });
    
    $key2 = $edt1->insert_record($EDT_TEST, { string_req => 'interlock 2' });
    
    $T->ok_result($key2, "inserted test record 2") || return;
    $T->ok_found_record($EDT_TEST, "$primary = $key2 and string_req = 'interlock 2'");
    $T->ok_no_record($EDT_TEST, "$primary = $key1 and string_req = 'interlock 1'");
    
    is( $edt1->transaction, 'aborted', "transaction 1 was aborted");
    is( $edt2->transaction, 'active', "transation 2 is active");
    
    # Now roll back the second transaction, and test that.
    
    $edt2->rollback;
    
    $T->ok_no_record($EDT_TEST, "$primary = $key2 and string_req = 'interlock 2'");
    
    # Start one transaction, but without IMMEDIATE_MODE so that the actions are checked but not
    # actually executed yet.

    my ($edt3, $edt4);
    
    $edt3 = $T->new_edt($perm_a);
    
    $edt3->insert_record($EDT_TEST, { string_req => 'interlock 3' });
    
    $T->ok_no_record($EDT_TEST, "string_req = 'interlock 3'");
    
    # Now execute another transaction, and commit it. Since the first one hasn't actually been
    # sent to the database yet, it shouldn't be affected.
    
    $edt4 = $T->new_edt($perm_a);

    $edt4->insert_record($EDT_TEST, { string_req => 'interlock 4' });

    $edt4->commit;

    $T->ok_found_record($EDT_TEST, "string_req = 'interlock 4'");
    $T->ok_no_record($EDT_TEST, "string_req = 'interlock 3'");

    is( $edt3->transaction, '', "transaction 3 has not started" );
    is( $edt4->transaction, 'committed', "transaction 4 was committed" );

    # Now commit the third transaction. This should go through just fine.

    $edt3->commit;

    $T->ok_found_record($EDT_TEST, "string_req = 'interlock 4'");
    $T->ok_found_record($EDT_TEST, "string_req = 'interlock 3'");
    
    is( $edt3->transaction, 'committed', "transaction 3 was committed" );
};
