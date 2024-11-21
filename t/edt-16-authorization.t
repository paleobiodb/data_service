#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# authorization.t :
# 
#         Test the methods used for checking record authorization. The
#         functionality tested here is limited, because the default
#         authorization is 'unrestricted'. More sophisticated authorization
#         requires a plug-in module.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception ok_new_edt clear_table sql_command
		  last_result last_result_list ok_has_condition ok_has_error);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest');


# Check the scaffolding. These methods are designed primarily for internal use,
# except for 'check_table_permission' and 'check_row_permission'.

subtest 'required methods' => sub {
    
    can_ok( 'ETBasicTest', 'check_table_permission', 'check_row_permission', 'authorize_action' )
	
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
    
    my $edt = ok_new_edt;
    
    my $perm = bless { abc => 1}, 'TestPermission';
    
    can_ok( 'ETBasicTest', 'check_instance_permission' ) &&
	is( $edt->check_instance_permission($perm), '', 
	    "default configuration: check_instance_permission always returns false" );
    
    can_ok( 'ETBasicTest', 'validate_instance_permission' ) &&
	is( $edt->validate_instance_permission($perm), 'unrestricted',
	    "default configuration: validate_instance_permission always returns 'unrestricted'" );
    
    is( $edt->check_table_permission('EDT_TEST', 'post'), 'unrestricted',
        "validate_instance_permission returns 'unrestricted'" );
    
    clear_table('EDT_TEST');
};


subtest 'check_row_permission' => sub {
    
    my $edt = ok_new_edt('SILENT_MODE');
    
    ok_eval( sub { $edt->check_row_permission('EDT_TEST', 'modify', "test_no=3") },
	     "check_row_permission executes successfully" )
    
	|| BAIL_OUT "check_row_permission failed";
    
    is( last_result, 'notfound', "check_row_permission returns 'notfound'" );
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req) VALUES (3, 'abc')");
    
    my @result = $edt->check_row_permission('EDT_TEST', 'modify', "test_no=3");
    
    is( $result[0], 'unrestricted', "check_row_permission returns 'unrestricted'" );
    is( $result[1], '1', "check_row_permission returns '1'" );
    
    ok_eval( sub { $edt->check_row_permission('EDT_TEST', 'modify', "not an expression xxx") },
	     "check_row_permission sql error does not throw an exception" );
    
    is( ref(last_result), 'ARRAY', "check_row_permission sql error returns listref" ) &&
	is( last_result->[0], 'E_EXECUTE', "check_row_permission sql error returns E_EXECUTE" );
};


subtest 'authorize_action' => sub {
    
    my $edt = ok_new_edt;
    
    my $action = $edt->_test_action('insert', 'EDT_TEST', { abc => 1} );
    
    ok_eval( sub { $edt->authorize_action($action, 'insert', 'EDT_TEST') },
	     "authorize_action succeeded" )
    
	|| return;
    
    is( last_result, 'unrestricted', "authorize_action insert primary unrestricted" );
    
    $action->set_permission(undef);
    
    ok_eval( sub { $edt->authorize_action($action, 'insert', 'NOT_A_TABLE_XXX') },
	     "ok_eval authorize_action with bad table" );
    
    is( last_result, 'none', "bad table returns 'none'" );
    
    is( $action->permission, 'none', "action permission set to 'none'" );
    
    $action = $edt->_test_action('update', 'EDT_TEST', 
				 { _primary => 3, string_req => 'def' });
    
    my @result = $edt->authorize_action($action);
    
    is( $result[0], 'unrestricted', "authorize_action update primary unrestricted" );
    is( $result[1], 1, "authorize_action update count 1" );
    
    $action = $edt->_test_action('update', 'EDT_TEST', 
				 { _primary => [3, 4, 5], string_req => 'ghi' });
    
    @result = $edt->authorize_action($action);
    
    is( $result[0], 'unrestricted', "authorize_action update primary unrestricted" );
    is( $result[1], 1, "authorize_action update count 1" );
    
    $action = $edt->_test_action('update', 'EDT_TEST', 
				 { _primary => [4, 5, 6], string_req => 'ghi' });
    
    @result = $edt->authorize_action($action);
    
    is( $result[0], 'notfound', "authorize_action update primary notfound" );
    is( $result[1], undef, "authorize_action update count undef" );
    
    $action = $edt->_test_action('update', 'EDT_TEST', { _primary => 19 });
    
    @result = $edt->authorize_action($action);
    
    is( $result[0], 'notfound', "authorize_action update single primary notfound" );
    
    $action = $edt->_test_action('delete', 'EDT_TEST', "3");
    
    @result = $edt->authorize_action($action);
    
    is( $result[0], 'unrestricted', "authorize_action delete single primary" );
    is( $result[1], 1, "authorize_action delete count 1" );
    
    $action = $edt->_test_action('other', 'EDT_TEST', "3");
    
    @result = $edt->authorize_action($action);
    
    is( $result[0], 'unrestricted', "authorize_action other single primary" );
    is( $result[1], 1, "authorize_action other count 1" );
    
    $action = $edt->_test_action('other', 'EDT_TEST');
    
    @result = $edt->authorize_action($action);
    
    is( $result[0], 'unrestricted', "authorize_action other no key" );
    # is( $result[1], undef, "authorize_action other count undef" );
};


subtest 'authorize_action change operation' => sub {
    
    my $edt = ok_new_edt;
    
    my $action = $edt->_test_action('insupdate', 'EDT_TEST', 
				    { _primary => 8, string_req => 'hippo' });
    
    my @result = $edt->authorize_action($action);
    
    is( $result[0], 'unrestricted', "authorize_action grants permission" );
    
    is( $action->operation, 'insert', "authorize_action converted insupdate to insert" );
    
    $action = $edt->_test_action('replace', 'EDT_TEST', 
				 { _primary => 9, string_req => 'elephant' });
    
    @result = $edt->authorize_action($action);
    
    is( $result[0], 'unrestricted', "authorize_action grants permission" );
    
    is( $action->operation, 'insert', "authorize_action converts replace to insert" );
    
    $action = $edt->_test_action('insupdate', 'EDT_TEST', { _primary => 3 });
    
    @result = $edt->authorize_action($action);
    
    is( $result[0], 'unrestricted', "authorize_action grants permission" );
    
    is( $action->operation, 'update', "authorize_action converts insupdate to update" );
    
    my $edtnc = ok_new_edt('NO_CREATE');
    
    $action = $edtnc->_test_action('insupdate', 'EDT_TEST', 
				   { _primary => 10, string_req => 'giraffe' });
    
    @result = $edtnc->authorize_action($action);
    
    is( $result[0], 'unrestricted', "authorize_action grants permission" );
    
    is( $action->operation, 'insert', "authorize_action converts insupdate to insert" );
    
    # ok( $action->has_condition('C_CREATE'), "condition C_CREATE added absent CREATE allowance" );
    
    # ok( $edtnc->has_condition('C_CREATE'), "same for edt as a whole" );
};
    

subtest 'authorize_subordinate_action' => sub {
    
    my $edt = ok_new_edt;
};


diag("TODO: pending authorization and 'insupdate', 'replace'");

# When authorization cannot be finalized, we need to proceed at least as far as
# checking whether the records exist or not, so that 'insupdate' and 'replace'
# can be changed to 'insert' if necessary.

