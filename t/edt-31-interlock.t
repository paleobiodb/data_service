#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# interlock.t :
# 
#         Check that starting a new EditTransaction while another one is still
#         active will abort the first one.
# 


use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 3;

use ETBasicTest;
use EditTester qw(ok_new_edt clear_edt clear_table ok_commit ok_rollback
		  ok_found_record ok_no_record ok_record_count);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest', 'EDT_TEST');


# check that starting a new EditTransaction while a previous one is active will abort it.

subtest 'interlock a' => sub {
    
    # Clear the default table, so we are in a known state. The table EDT_TEST
    # was specified above.
    
    clear_table;
    
    # Then initiate one transaction and start execution.
    
    my $edt1 = ok_new_edt;
    
    $edt1->start_execution;
    
    $edt1->insert_record({ string_req => 'interlock 1' });
    
    ok_found_record('default', "string_req = 'interlock 1'", "edt1 record was inserted");
    
    # Now initiate a second one, and test that the first one was rolled back.
    
    my $edt2 = ok_new_edt;
    
    $edt2->start_execution;
    
    $edt2->insert_record({ string_req => 'interlock 2' });
    
    ok_found_record('default', "string_req = 'interlock 2'", "edt2 record was inserted");
    ok_no_record('default', "string_req = 'interlock 1'", "edt1 record is no longer in the table");
    
    is( $edt1->transaction, 'aborted', "transaction 1 was aborted");
    is( $edt2->transaction, 'active', "transation 2 is active");
    
    # Check that the second transaction commits successfully.
    
    ok_commit;
    
    ok_found_record('default', "string_req = 'interlock 2'", "edt2 record was committed");
};


# Check that a transaction that hasn't yet started execution is not aborted.

subtest 'interlock b' => sub {
    
    # Start a new EditTransaction, but do not start the associated database transaction.
    
    my $edt1 = ok_new_edt;
    
    $edt1->insert_record({ string_req => 'interlock 3' });
    
    ok_no_record('default', "string_req = 'interlock 3'");
    
    # Now execute another transaction, and commit it. Since the first one hasn't actually been
    # sent to the database yet, it shouldn't be affected.
    
    my $edt2 = ok_new_edt;
    
    $edt2->insert_record({ string_req => 'interlock 4' });
    
    $edt2->commit;
    
    ok_found_record('default', "string_req = 'interlock 4'");
    ok_no_record('default', "string_req = 'interlock 3'");
    
    is( $edt1->transaction, '', "transaction 1 has not started" );
    is( $edt2->transaction, 'committed', "transaction 2 was committed" );
    
    # Now commit the first transaction. This should go through just fine.
    
    $edt1->commit;
    
    ok_found_record('default', "string_req = 'interlock 4'");
    ok_found_record('default', "string_req = 'interlock 3'");
    
    is( $edt1->transaction, 'committed', "transaction 1 was committed" );
};


# Now check that a transaction is automatically rolled back when it goes out of
# scope. 

subtest 'out of scope' => sub {
    
    # Create the EditTransaction in a nested scope. We must use 'clear_edt' to
    # remove the reference kept in the EditTester instance.
    
    {
	my $edt = ok_new_edt;
	
	clear_edt;
	
	$edt->start_execution;
	
	$edt->insert_record({ string_req => 'scope' });
	
	ok_found_record('default', "string_req = 'scope'");
    }
    
    ok_no_record('default', "string_req = 'scope'");
};
