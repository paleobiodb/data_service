#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
#
# edt-29-permissions.t : Test how 'authorize_against_table' responds to the results of
#                        'check_table_permission', 'check_row_permission', and
#                        'authorize_insert_key'. ETBasicTest grants unrestricted permission
#                        everywhere, so these tests replace those methods temporarily with
#                        versions that return chosen results.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 7;

use ETBasicTest;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_no_conditions ok_no_errors
		  ok_has_condition ok_has_one_condition clear_table sql_command
		  ok_found_record ok_no_record);


# Establish an EditTester instance.

my $T = EditTester->new('ETBasicTest');


# Call the specified code with the permission methods replaced. The first argument is a
# hashref which may contain the keys 'row', 'table', and 'insert_key', each with a code
# reference that will be called in place of 'check_row_permission', 'check_table_permission',
# and 'authorize_insert_key' respectively. Methods without an entry keep their normal
# behavior. Each call to any of these methods is recorded in @CALLS.

our (@CALLS);

my %ORIGINAL = ( row => ETBasicTest->can('check_row_permission'),
		 table => ETBasicTest->can('check_table_permission'),
		 insert_key => ETBasicTest->can('authorize_insert_key') );

sub with_permissions {
    
    my ($perms, $code) = @_;
    
    my $row = $perms->{row} || $ORIGINAL{row};
    my $table = $perms->{table} || $ORIGINAL{table};
    my $insert_key = $perms->{insert_key} || $ORIGINAL{insert_key};
    
    no warnings 'redefine';
    
    local *ETBasicTest::check_row_permission = sub {
	push @CALLS, "row $_[1] $_[2] $_[3]";
	return $row->(@_);
    };
    
    local *ETBasicTest::check_table_permission = sub {
	push @CALLS, "table $_[1] $_[2]";
	return $table->(@_);
    };
    
    local *ETBasicTest::authorize_insert_key = sub {
	push @CALLS, "insert_key $_[1] $_[2]";
	return $insert_key->(@_);
    };
    
    @CALLS = ();
    
    return $code->();
}


# Return the permission stored in the latest action of the specified transaction.

sub latest_permission {
    
    my ($edt) = @_;
    
    return $edt->action_ref('&_')->permission;
}


# Clear the test tables and add records with known keys.

sub reset_tables {
    
    clear_table('EDT_TEST');
    clear_table('EDT_SUB');
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req) VALUES (5, 'five'), (6, 'six')");
    sql_command("INSERT INTO <<EDT_SUB>> (aux_no, test_no, name) VALUES (1, 5, 'sub one')");
}


# Check that the necessary methods are defined.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'check_table_permission', 'check_row_permission',
	   'authorize_insert_key', 'authorize_against_table')
	
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check the results of check_row_permission for an update of an existing record.

subtest 'row permissions' => sub {
    
    reset_tables();
    
    foreach my $perm ( 'owned', 'granted', 'admin', 'unrestricted' )
    {
	my $edt = ok_new_edt;
	
	with_permissions({ row => sub { ($perm, 1) } }, sub {
	    $edt->update_record('EDT_TEST', { test_no => 5, string_req => 'u' });
	});
	
	ok_no_conditions('&_', "row permission '$perm' allows the update");
	
	is( latest_permission($edt), $perm, "action permission is '$perm'" );
    }
    
    is_deeply( \@CALLS, [ "row EDT_TEST modify test_no in ('5')" ],
	       "update requests 'modify' permission on the selected row" );
    
    my $edt = ok_new_edt;
    
    with_permissions({ row => sub { ('none', 1) } }, sub {
	$edt->update_record('EDT_TEST', { test_no => 5, string_req => 'u' });
    });
    
    ok_has_one_condition('&_', 'E_PERMISSION', qr/update/, "row permission 'none' is rejected");
    
    is( latest_permission($edt), 'none', "action permission is 'none'" );
    
    with_permissions({ row => sub { ('none', 1) } }, sub {
	$edt->delete_record('EDT_TEST', 5);
    });
    
    is_deeply( \@CALLS, [ "row EDT_TEST delete test_no in ('5')" ],
	       "delete requests 'delete' permission on the selected row" );
    
    ok_has_one_condition('&_', 'E_PERMISSION', qr/delete/, "delete without permission is rejected");
    
    with_permissions({ row => sub { [ 'E_TEST', 'row permission error' ] } }, sub {
	$edt->update_record('EDT_TEST', { test_no => 5, string_req => 'u' });
    });
    
    ok_has_one_condition('&_', 'E_TEST', "error returned by check_row_permission is added");
};


# Check the results of check_row_permission for locked records.

subtest 'locked records' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    with_permissions({ row => sub { ('locked', 1) } }, sub {
	$edt->update_record('EDT_TEST', { test_no => 5, string_req => 'u' });
    });
    
    ok_has_one_condition('&_', 'E_LOCKED', "locked record is rejected");
    
    with_permissions({ row => sub { ('locked', 2) } }, sub {
	$edt->update_record('EDT_TEST', { _primary => '5,6', string_req => 'u' });
    });
    
    ok_has_one_condition('&_', 'E_LOCKED', qr/2 locked/, "two locked records are counted");
    
    $edt = ok_new_edt;
    
    with_permissions({ row => sub { ('owned_unlock', 1) } }, sub {
	$edt->update_record('EDT_TEST', { test_no => 5, string_req => 'u' });
    });
    
    ok_has_condition('all', 'C_LOCKED', "record locked by the owner requires LOCKED");
    ok_has_condition('&_', 'W_LOCKED', "record locked by the owner adds a warning to the action");
    
    ok_has_condition('&_', 'W_LOCKED', qr/permission to unlock/,
		     "W_LOCKED warning has a message");
    
    $edt = ok_new_edt('LOCKED');
    
    with_permissions({ row => sub { ('owned_unlock', 1) } }, sub {
	$edt->update_record('EDT_TEST', { test_no => 5, string_req => 'u' });
    });
    
    ok_no_conditions('all', "with LOCKED, a record locked by the owner can be updated");
    
    is( latest_permission($edt), 'owned', "the '_unlock' suffix is removed from the permission" );
};


# Check the table permissions for inserts.

subtest 'insert permissions' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    with_permissions({ table => sub { 'own' } }, sub {
	$edt->insert_record('EDT_TEST', { string_req => 'i' });
    });
    
    is_deeply( \@CALLS, [ "table EDT_TEST post" ], "insert requests 'post' permission on the table" );
    
    ok_no_conditions('&_', "table permission 'own' allows an insert");
    
    is( latest_permission($edt), 'own', "action permission is 'own'" );
    
    with_permissions({ table => sub { 'none' } }, sub {
	$edt->insert_record('EDT_TEST', { string_req => 'i' });
    });
    
    ok_has_one_condition('&_', 'E_PERMISSION', qr/insert/, "insert without permission is rejected");
    
    with_permissions({ table => sub { $_[2] eq 'insert' ? 'none' : 'unrestricted' } }, sub {
	$edt->insert_record('EDT_TEST', { test_no => 99, string_req => 'i' });
    });
    
    ok( (grep { /^insert_key insert EDT_TEST/ } @CALLS), "insert with a new key calls authorize_insert_key" );
    ok( (grep { /^table EDT_TEST insert/ } @CALLS), "insert with a new key requests 'insert' permission" );
    
    ok_has_one_condition('&_', 'E_PERMISSION', "insert with a new key without 'insert' permission is rejected");
    
    # If authorize_insert_key refuses, an insert or replace with a new key is rejected for
    # lack of permission.
    
    with_permissions({ insert_key => sub { 'none' } }, sub {
	$edt->insert_record('EDT_TEST', { test_no => 99, string_req => 'i' });
    });
    
    ok_has_one_condition('&_', 'E_PERMISSION', qr/insert a record with key '99'/,
			 "insert with a key the user may not choose is rejected");
    
    with_permissions({ insert_key => sub { 'none' } }, sub {
	$edt->replace_record('EDT_TEST', { test_no => 99, string_req => 'r' });
    });
    
    ok_has_one_condition('&_', 'E_PERMISSION', qr/No record was found with key '99'/,
			 "replace with a key the user may not choose is rejected");
};


# Check the permissions on the linked record for actions on a subordinate table.

subtest 'link permissions' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    with_permissions({ row => sub { ('granted', 1) } }, sub {
	$edt->insert_record('EDT_SUB', { test_no => 5, name => 'linked' });
    });
    
    is_deeply( \@CALLS, [ "row EDT_TEST modify test_no in ('5')" ],
	       "insert requests 'modify' permission on the linked record" );
    
    ok_no_conditions('&_', "permission on the linked record allows the insert");
    
    with_permissions({ row => sub { ('none', 1) } }, sub {
	$edt->insert_record('EDT_SUB', { test_no => 5, name => 'linked' });
    });
    
    ok_has_one_condition('&_', 'E_PERMISSION', qr/linked record/,
			 "insert without permission on the linked record is rejected");
    
    with_permissions({ row => sub { ('locked', 1) } }, sub {
	$edt->insert_record('EDT_SUB', { test_no => 5, name => 'linked' });
    });
    
    ok_has_one_condition('&_', 'E_LOCKED', "insert linked to a locked record is rejected");
    
    $edt->insert_record('EDT_SUB', { test_no => 77, name => 'linked' });
    
    ok_has_one_condition('&_', 'E_NOT_FOUND', qr/[Ll]inked record/,
			 "insert linked to a nonexistent record is rejected");
    
    $edt->insert_record('EDT_SUB', { name => 'not linked' });
    
    ok_has_one_condition('&_', 'E_NO_KEY', qr/linking key/, "insert without a link value is rejected");
    
    # An update or delete of a subordinate record that does not exist is reported as not
    # found, and an insert_update_record with a key that does not exist as a bad key.
    
    $edt->update_record('EDT_SUB', { aux_no => 999, name => 'missing' });
    
    ok_has_one_condition('&_', 'E_NOT_FOUND', "update of a nonexistent subordinate record");
    
    $edt->delete_record('EDT_SUB', 999);
    
    ok_has_one_condition('&_', 'E_NOT_FOUND', "delete of a nonexistent subordinate record");
    
    $edt->insert_update_record('EDT_SUB', { aux_no => 999, name => 'missing' });
    
    ok_has_one_condition('&_', 'E_BAD_KEY', qr/999/,
			 "insert_update_record with a nonexistent subordinate key");
    
    with_permissions({ row => sub { $_[1] eq 'EDT_TEST' ? ('granted', 1) : ('none', 1) } }, sub {
	$edt->update_record('EDT_SUB', { aux_no => 1, name => 'updated' });
    });
    
    is_deeply( \@CALLS, [ "row EDT_TEST modify test_no in ('5')",
			  "row EDT_SUB modify aux_no in ('1')" ],
	       "update checks the linked record and then the record itself" );
    
    ok_has_one_condition('&_', 'E_PERMISSION', qr/update/,
			 "update without permission on the record itself is rejected");
    
    with_permissions({ row => sub { [ 'E_TEST', 'link permission error' ] } }, sub {
	$edt->insert_record('EDT_SUB', { test_no => 5, name => 'linked' });
    });
    
    ok_has_condition('&_', 'E_BAD_LINK', "error from the link permission check is reported");
    
    ok_has_condition('&_', 'E_BAD_LINK', qr/Could not check permission/, "E_BAD_LINK has a message");
};


# Check the permission on a new parent record that a subordinate record links to. This
# check can only be made when the subordinate action is executed, after the parent has
# been inserted.

subtest 'link to a new parent record' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    with_permissions({ row => sub { ('none', 1) } }, sub {
	$edt->insert_record('EDT_TEST', { string_req => 'new parent', _label => 'np' });
	$edt->insert_record('EDT_SUB', { test_no => '&np', name => 'child of new parent' });
	
	ok_no_errors('all', "permission on the new parent is not checked before execution");
	
	ok_failed_commit("transaction fails if there is no permission on the new parent");
    });
    
    my ($parent_key) = $edt->inserted_keys('EDT_TEST');
    
    ok( (grep { $_ eq "row EDT_TEST modify test_no in ('$parent_key')" } @CALLS),
	"permission was checked on the new parent record" );
    
    ok_has_condition('all', 'E_PERMISSION', qr/linked record/, "missing permission is reported");
    
    ok( ! (grep { /^E_EXECUTE/ } $edt->conditions), "refusal at execution time adds no E_EXECUTE" );
    
    ok_no_record('EDT_SUB', "name = 'child of new parent'", "child record was not stored");
};


# Check that table_action does not check permissions itself.

subtest 'table_action' => sub {
    
    reset_tables();
    
    # table_action does not authorize the action when it is created. The action is
    # authorized when it is executed, which requires 'admin' permission on the table.
    
    no warnings 'once';
    
    local *ETBasicTest::test_table_method = sub { return 'done' };
    
    my $edt = ok_new_edt;
    
    with_permissions({ table => sub { $_[2] eq 'admin' ? 'none' : 'unrestricted' } }, sub {
	
	$edt->table_action('test_table_method', 'EDT_TEST', { });
	
	is_deeply( \@CALLS, [ ], "table_action calls no permission method when it is created" );
	
	ok_no_errors('&_', "table_action is not rejected when it is created");
	
	ok_failed_commit("table_action without 'admin' permission fails when executed");
    });
    
    ok( (grep { $_ eq 'table EDT_TEST admin' } @CALLS), "'admin' permission was requested" );
    
    ok_has_condition('all', 'E_PERMISSION', "missing 'admin' permission is reported");
    
    ok( ! (grep { /^E_EXECUTE/ } $edt->conditions), "refusal at execution time adds no E_EXECUTE" );
    
    $edt = ok_new_edt;
    
    with_permissions({ table => sub { $_[2] eq 'admin' ? 'admin' : 'unrestricted' } }, sub {
	
	$edt->table_action('test_table_method', 'EDT_TEST', { });
	
	ok_commit("table_action with 'admin' permission is executed");
    });
    
    is( $edt->action_result('&#1'), 'done', "table method was called" );
};
