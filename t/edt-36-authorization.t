#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
#
# edt-36-authorization.t : Test the parts of EditTransaction::Authorization that are not
#                          covered by edt-16 and edt-29: the default permission methods,
#                          bad superior table configurations, keys of subordinate records,
#                          references in key and link values, the before_key_column
#                          hook, subordinate tables with no primary key, and deleting
#                          subordinate records whose superior record does not exist.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 14;

use ETBasicTest;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_no_conditions ok_no_errors
		  ok_has_condition ok_has_one_condition clear_table sql_command
		  ok_found_record ok_no_record);

use TableDefs qw(%TABLE set_table_name set_table_property set_column_property);


# Establish an EditTester instance.

my $T = EditTester->new('ETBasicTest');

my ($TEST_DB) = $TABLE{EDT_TEST} =~ /^(\w+)\./;


# Create a throwaway table, and give it three names with different table properties.
# EDT_PERMS denies every kind of access. EDT_BADAUTH names a superior table that does not
# exist, and EDT_BADKEY names a superior table that has no primary key.

sql_command("DROP TABLE IF EXISTS $TEST_DB.edt_auth_cov");
sql_command("CREATE TABLE $TEST_DB.edt_auth_cov (cov_no int unsigned primary key auto_increment,
	     test_no int unsigned not null default 0, name varchar(40) not null default '')");

set_table_name(EDT_PERMS => "$TEST_DB.edt_auth_cov");
set_table_name(EDT_BADAUTH => "$TEST_DB.edt_auth_cov");
set_table_name(EDT_BADKEY => "$TEST_DB.edt_auth_cov");

set_table_property(EDT_PERMS => $_ => 'none') foreach qw(CAN_VIEW CAN_POST CAN_INSERT
							 CAN_MODIFY CAN_DELETE);
set_table_property(EDT_BADAUTH => AUTH_TABLE => 'EDT_NOSUCH');
set_table_property(EDT_BADKEY => AUTH_TABLE => 'EDT_NOPRIM');


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
	   'authorize_against_table', 'unpack_key_values', 'check_key_values')
	
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check that the default permission methods respect the CAN_ table properties.

subtest 'default permission methods' => sub {
    
    sql_command("DELETE FROM $TEST_DB.edt_auth_cov");
    sql_command("INSERT INTO $TEST_DB.edt_auth_cov (cov_no, name) VALUES (1, 'one')");
    
    my $edt = ok_new_edt;
    
    foreach my $requested ( qw(view post insert modify) )
    {
	is( $edt->check_table_permission('EDT_PERMS', $requested), 'none',
	    "check_table_permission '$requested' is refused by CAN_" . uc $requested );
	
	is( $edt->check_table_permission('EDT_TEST', $requested), 'unrestricted',
	    "check_table_permission '$requested' is allowed by default" );
    }
    
    is( $edt->check_table_permission('EDT_PERMS', 'admin'), 'unrestricted',
	"check_table_permission 'admin' does not depend on any CAN_ property" );
    
    foreach my $requested ( qw(view modify delete) )
    {
	is_deeply( [ $edt->check_row_permission('EDT_PERMS', $requested, 'cov_no = 1') ], [ 'none' ],
		   "check_row_permission '$requested' is refused by CAN_" . uc $requested );
    }
    
    is_deeply( [ $edt->check_row_permission('EDT_TEST', 'view', 'test_no = 99999') ], [ 'notfound' ],
	       "check_row_permission returns 'notfound' if no row matches" );
    
    $edt->insert_record('EDT_PERMS', { name => 'refused' });
    
    ok_has_one_condition('&_', 'E_PERMISSION', "insert is refused by CAN_POST");
    
    $edt->update_record('EDT_PERMS', { cov_no => 1, name => 'refused' });
    
    ok_has_one_condition('&_', 'E_PERMISSION', "update is refused by CAN_MODIFY");
    
    $edt->delete_record('EDT_PERMS', 1);
    
    ok_has_one_condition('&_', 'E_PERMISSION', "delete is refused by CAN_DELETE");
};


# Check that authorize_against_table returns the stored permission of an action that has
# already been authorized, and fills in the operation and table if they are not given.

subtest 'authorize an action again' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { test_no => 5, string_req => 'again' });
    
    my $action = $edt->action_ref('&_');
    
    is( $action->permission, 'unrestricted', "the action was authorized" );
    
    no warnings 'redefine';
    
    my $calls = 0;
    
    local *ETBasicTest::check_row_permission = sub { $calls++; return ('none', 1) };
    
    is( $edt->authorize_against_table($action), 'unrestricted',
	"authorize_against_table returns the stored permission" );
    
    is( $calls, 0, "the permission was not checked again" );
    
    $action->set_permission('PENDING');
    
    $edt->authorize_against_table($action);
    
    is( $calls, 1, "a pending permission is checked using the action's operation and table" );
    
    is( $action->permission, 'none', "the new permission is stored" );
};


# Check what happens if a table names a superior table that cannot be used.

subtest 'bad superior table' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_BADAUTH', { test_no => 5, name => 'bad superior' });
    
    ok_has_one_condition('&_', 'E_BAD_TABLE', qr/EDT_NOSUCH/,
			 "a superior table that does not exist is reported");
    
    is( $edt->action_ref('&_')->permission, 'none', "the action is not authorized" );
    
    $edt->insert_record('EDT_BADKEY', { test_no => 5, name => 'bad superior key' });
    
    ok_has_one_condition('&_', 'E_BAD_TABLE', qr/EDT_BADKEY/,
			 "a superior table with no primary key is reported");
    
    is( $edt->action_ref('&_')->permission, 'none', "the action is not authorized" );
    
    ok_failed_commit("the transaction fails");
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_BADAUTH', { test_no => 5, name => 'bad superior' });
    
    ok_has_condition('&_', 'E_BAD_TABLE', qr/'EDT_NOSUCH'/,
		     "the message quotes the name of the superior table");
    
    $edt->insert_record('EDT_BADKEY', { test_no => 5, name => 'bad superior key' });
    
    ok( ! (grep { /EDT_BADKEY.*does not correspond to any known/ } $edt->conditions('&_')),
	"a table that exists is not reported as unknown" );
};


# Check actions on a subordinate table that specify both a key value and a link value.
# After permission is granted on the linked record, the permission on the record itself
# is checked.

subtest 'subordinate record keys' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_SUB', { aux_no => 1, test_no => 5, name => 'duplicate' });
    
    ok_has_one_condition('&_', 'E_DUPLICATE', "insert with an existing key is rejected");
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_SUB', { aux_no => 50, test_no => 5, name => 'new key' });
    
    ok_no_conditions('&_', "insert with a new key is accepted");
    
    ok_commit;
    
    ok_found_record('EDT_SUB', "aux_no = 50 and name = 'new key'", "record with the new key was stored");
    
    no warnings 'redefine';
    
    {
	local *ETBasicTest::authorize_insert_key = sub { 'none' };
	
	$edt = ok_new_edt;
	
	$edt->insert_record('EDT_SUB', { aux_no => 60, test_no => 5, name => 'chosen key' });
	
	ok_has_one_condition('&_', 'E_PERMISSION', qr/insert a record with key '60'/,
			     "insert with a key the user may not choose is rejected");
	
	$edt->replace_record('EDT_SUB', { aux_no => 61, test_no => 5, name => 'chosen key' });
	
	ok_has_one_condition('&_', 'E_PERMISSION', qr/No record was found with key '61'/,
			     "replace with a key the user may not choose is rejected");
    }
    
    {
	local *ETBasicTest::check_row_permission = sub {
	    return $_[1] eq 'EDT_SUB' ? [ 'E_TEST', 'row error' ] : ('granted', 1);
	};
	
	$edt = ok_new_edt;
	
	$edt->update_record('EDT_SUB', { aux_no => 1, test_no => 5, name => 'error' });
	
	ok_has_one_condition('&_', 'E_TEST', qr/row error/,
			     "error from the permission check on the record itself is added");
	
	is( $edt->action_ref('&_')->permission, 'none', "the action is not authorized" );
    }
    
    $edt = ok_new_edt;
    
    $edt->update_record('EDT_SUB', { aux_no => 999, test_no => 5, name => 'missing' });
    
    ok_has_one_condition('&_', 'E_NOT_FOUND', "update of a nonexistent record with a link value");
    
    ok( ! (grep { /Linked record/ } $edt->conditions('&_')),
	"the linked record, which exists, is not reported as missing" );
};



# Check deleting a subordinate record whose superior record does not exist. This is
# allowed if the permission on the record is 'unrestricted', 'admin', or 'admin_unlock',
# and refused with E_NOT_FOUND naming the missing link value otherwise. Other operations
# on such a record are always refused. [Added by Claude under the guidance of Michael
# McClennen]

subtest 'orphaned subordinate records' => sub {
    
    reset_tables();
    
    sql_command("INSERT INTO <<EDT_SUB>> (aux_no, test_no, name) VALUES
		 (2, 999, 'orphan two'), (3, 999, 'orphan three'), (4, 999, 'orphan four')");
    
    # The default permission method returns 'unrestricted'.
    
    my $edt = ok_new_edt;
    
    $edt->delete_record('EDT_SUB', 2);
    
    ok_no_conditions('&_', "unrestricted delete of an orphaned record has no conditions");
    
    ok_commit("the transaction commits");
    
    ok_no_record('EDT_SUB', "aux_no = 2", "the orphaned record was deleted");
    
    $edt = ok_new_edt;
    
    $edt->update_record('EDT_SUB', { aux_no => 3, name => 'changed' });
    
    ok_has_one_condition('&_', 'E_NOT_FOUND', qr/Linked record '999' was not found/,
			 "update of an orphaned record is refused, naming the link value");
    
    # Override the permission on the subordinate table, keeping the default for the
    # superior table so that its record is still not found.
    
    my $row_perm;
    
    local *ETBasicTest::check_row_permission = sub {
	return $_[1] eq 'EDT_SUB' ? ($row_perm, 1)
	    : EditTransaction::Authorization::check_row_permission(@_);
    };
    
    foreach my $perm ( 'owned', 'granted' )
    {
	$row_perm = $perm;
	
	$edt = ok_new_edt;
	
	$edt->delete_record('EDT_SUB', 3);
	
	ok_has_one_condition('&_', 'E_NOT_FOUND', qr/Linked record '999' was not found/,
			     "delete with permission '$perm' is refused");
	
	is( $edt->action_ref('&_')->permission, 'notfound', "the action is not authorized" );
	
	ok_failed_commit("the transaction fails");
    }
    
    ok_found_record('EDT_SUB', "aux_no = 3", "the orphaned record was not deleted");
    
    foreach my $perm ( 'admin', 'admin_unlock' )
    {
	$row_perm = $perm;
	
	$edt = ok_new_edt;
	
	$edt->delete_record('EDT_SUB', $perm eq 'admin' ? 3 : 4);
	
	ok_no_errors('&_', "delete with permission '$perm' has no errors");
	
	is( $edt->action_ref('&_')->permission, $perm, "the action has permission '$perm'" );
    }
};


# Check that an error returned by check_table_permission is added to the action.

subtest 'table permission errors' => sub {
    
    my $edt = ok_new_edt;
    
    no warnings 'redefine';
    
    local *ETBasicTest::check_table_permission = sub { [ 'E_TEST', 'table error' ] };
    
    $edt->insert_record('EDT_TEST', { string_req => 'table error' });
    
    ok_has_one_condition('&_', 'E_TEST', qr/table error/,
			 "error from check_table_permission is added");
    
    is( $edt->action_ref('&_')->permission, 'none', "the action is not authorized" );
};


# Check a link value that refers to a parent action which was never executed. With
# PROCEED, the parent is skipped because of its error, and the reference cannot be
# resolved when the child is executed.

subtest 'unresolved link reference' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt('PROCEED');
    
    $edt->insert_record('EDT_TEST', { _label => 'p', string_req => 'bad parent',
				      signed_val => 'not a number' });
    
    $edt->insert_record('EDT_SUB', { test_no => '&p', name => 'orphan' });
    
    is( $edt->action_ref('&_')->permission, 'PENDING',
	"the permission on a new parent waits until execution" );
    
    ok_commit("the transaction commits under PROCEED");
    
    ok_has_condition('all', 'E_BAD_REFERENCE', qr/no key value found for '&p'/,
		     "the unresolved reference is reported at execution");
    
    is( $edt->action_status('&#2'), 'failed', "the child action failed" );
    
    ok_no_record('EDT_SUB', "name = 'orphan'", "the child record was not stored");
};


# Check references to other actions in key values and link values.

subtest 'references and link fields' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_SUB', { _label => 's', test_no => 5, name => 'sub ref' });
    
    $edt->update_record('EDT_TEST', { test_no => '&s', string_req => 'wrong table' });
    
    ok_has_one_condition('&_', 'E_BAD_REFERENCE', qr/&s/,
			 "a key value referring to an action on another table is rejected");
    
    # EditTransaction::Mod::PaleoBioDB sets AUTH_FIELD for each subordinate table, as the
    # name of a field that may be used instead of the link column. Simulate that here.
    
    my $tableinfo = $edt->table_info_ref('EDT_SUB');
    
    local $tableinfo->{AUTH_FIELD} = 'parent_id';
    
    $edt->insert_record('EDT_SUB', { test_no => 5, parent_id => 5, name => 'two links' });
    
    ok_has_condition('&_', 'E_BAD_KEY_FIELD', qr/test_no.*parent_id/,
		     "the link column and AUTH_FIELD cannot be given together");
    
    $edt->insert_record('EDT_SUB', { parent_id => 6, name => 'auth field' });
    
    is( $edt->action_ref('&_')->{linkfield}, 'parent_id', "a link value can be given under AUTH_FIELD" );
    
    is_deeply( [ $edt->action_ref('&_')->linkvalues ], [ 6 ], "the link value is taken from AUTH_FIELD" );
};


# Check the before_key_column hook, which an application role may define to check and
# clean key values.

subtest 'before_key_column' => sub {
    
    reset_tables();
    
    no warnings 'once';
    
    local *ETBasicTest::before_key_column = sub {
	my ($edt, $columninfo, $operation, $value, $field) = @_;
	
	return $value eq 'five' ? (1, 5)
	    : $value eq 'bad' ? [ 'E_TEST', 'hook error' ]
	    : $value eq 'seven' ? (undef, undef, [ 'W_TEST', 'hook warning' ])
	    : $value eq 'eight' ? ([ 'E_TEST', 'hook error' ], undef, [ 'W_TEST', 'hook warning' ])
	    : ();
    };
    
    my $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { test_no => 'five', string_req => 'cleaned' });
    
    ok_no_conditions('&_', "a value cleaned by the hook is accepted");
    
    is( $edt->action_keyval('&_'), 5, "the cleaned value is used as the key" );
    
    ok_commit;
    
    ok_found_record('EDT_TEST', "test_no = 5 and string_req = 'cleaned'", "record was updated");
    
    $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { test_no => 'bad', string_req => 'refused' });
    
    ok_has_one_condition('&_', 'E_TEST', qr/hook error/, "an error from the hook is added");
    
    ok( ! $edt->action_keyval('&_'), "the refused value is not used as a key" );
    
    $edt = ok_new_edt;
    
    ok( eval { $edt->update_record('EDT_TEST', { test_no => 'seven', string_req => 'w' }); 1 },
	"an additional condition without an error does not throw an exception" );
    
    ok_has_condition('&_', 'W_TEST', qr/hook warning/, "the additional condition is added");
    
    $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { test_no => 'eight', string_req => 'w' });
    
    ok_has_condition('&_', 'W_TEST', qr/hook warning/,
		     "the additional condition is added along with an error");
};


# Check actions on a subordinate table with no primary key. Inserts are authorized by the
# link value, and updates and deletes use _where. [Added by Claude under the guidance of
# Michael McClennen]

subtest 'keyless subordinate table' => sub {
    
    reset_tables();
    
    sql_command("DROP TABLE IF EXISTS $TEST_DB.edt_auth_nokey");
    sql_command("CREATE TABLE $TEST_DB.edt_auth_nokey (test_no int unsigned not null default 0,
		 name varchar(40) not null default '')");
    
    set_table_name(EDT_NKSUB => "$TEST_DB.edt_auth_nokey");
    set_table_property(EDT_NKSUB => AUTH_TABLE => 'EDT_TEST');
    
    set_table_name(EDT_NKBAD => "$TEST_DB.edt_auth_nokey");
    set_table_property(EDT_NKBAD => AUTH_TABLE => 'EDT_NOSUCH');
    
    my $edt = ok_new_edt;
    
    # The link column is treated as a foreign key to the superior table, without a
    # FOREIGN_KEY property.
    
    my $link_cr = $edt->table_column_ref('EDT_NKSUB', 'test_no');
    
    is( $link_cr->{FOREIGN_KEY}, 'EDT_TEST', "link column is a foreign key to the superior table" );
    is( $link_cr->{FOREIGN_COL}, 'test_no', "link column refers to the superior table's primary key" );
    
    $edt->insert_record('EDT_NKSUB', { test_no => 5, name => 'nk one' });
    
    ok_no_conditions('&_', "insert with a link value is accepted");
    
    is_deeply( [ $edt->action_ref('&_')->linkvalues ], [ 5 ], "the link value is stored" );
    
    ok_commit;
    
    ok_found_record('EDT_NKSUB', "test_no = 5 and name = 'nk one'", "record was stored");
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_NKSUB', { name => 'no link' });
    
    ok_has_one_condition('&_', 'E_NO_KEY', qr/linking key/, "insert without a link value is rejected");
    
    $edt->insert_record('EDT_NKSUB', { test_no => 9999, name => 'bad link' });
    
    ok_has_one_condition('&_', 'E_NOT_FOUND', qr/[Ll]inked record/,
			 "insert linked to a nonexistent record is rejected");
    
    $edt->insert_record('EDT_NKSUB', { _primary => 3, test_no => 5, name => 'primary' });
    
    ok_has_one_condition('&_', 'E_BAD_KEY', qr/no primary key/, "_primary is rejected");
    
    {
	no warnings 'redefine';
	
	my @calls;
	
	local *ETBasicTest::check_row_permission = sub { push @calls, "$_[1] $_[2] $_[3]"; ('none', 1) };
	
	$edt->insert_record('EDT_NKSUB', { test_no => 5, name => 'refused' });
	
	is_deeply( \@calls, [ "EDT_TEST modify test_no in ('5')" ],
		   "insert requests 'modify' permission on the linked record" );
	
	ok_has_one_condition('&_', 'E_PERMISSION', qr/linked record/,
			     "insert without permission on the linked record is rejected");
    }
    
    # Link to a new parent record, and update and delete records using _where.
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { _label => 'p', string_req => 'new parent' });
    $edt->insert_record('EDT_NKSUB', { test_no => '&p', name => 'nk child' });
    $edt->update_record('EDT_NKSUB', { _where => "name = 'nk one'", name => 'nk updated' });
    
    ok_commit;
    
    my $parent_key = $edt->action_keyval('&p');
    
    ok_found_record('EDT_NKSUB', "test_no = '$parent_key' and name = 'nk child'",
		    "record linked to a new parent was stored");
    
    ok_found_record('EDT_NKSUB', "test_no = 5 and name = 'nk updated'", "record was updated");
    
    $edt = ok_new_edt;
    
    $edt->delete_record('EDT_NKSUB', { _where => "name = 'nk updated'" });
    
    ok_commit;
    
    ok_no_record('EDT_NKSUB', "name = 'nk updated'", "record was deleted");
    
    # A bad superior table is reported for a keyless table, and for an action that uses
    # _where.
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_NKBAD', { test_no => 5, name => 'bad superior' });
    
    ok_has_one_condition('&_', 'E_BAD_TABLE', qr/'EDT_NOSUCH'/,
			 "a bad superior table is reported for a keyless table");
    
    $edt->update_record('EDT_BADAUTH', { _where => "name = 'x'", name => 'y' });
    
    ok_has_one_condition('&_', 'E_BAD_TABLE', qr/'EDT_NOSUCH'/,
			 "a bad superior table is reported for an action that uses _where");
    
    is( $edt->action_ref('&_')->permission, 'none', "the action is not authorized" );
    
    sql_command("DROP TABLE $TEST_DB.edt_auth_nokey");
};


# Check that an insert into a table whose primary key is not auto_increment requires a key.

subtest 'primary key required' => sub {
    
    clear_table('EDT_SIGNED');
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_SIGNED', { string_val => 'no key' });
    
    ok( $edt->table_info_ref('EDT_SIGNED')->{PRIMARY_REQUIRED}, "PRIMARY_REQUIRED is set" );
    
    ok( ! $edt->table_info_ref('EDT_TEST')->{PRIMARY_REQUIRED},
	"PRIMARY_REQUIRED is not set for an auto_increment primary key" );
    
    ok_has_condition('&_', 'E_REQUIRED', qr/signed_no/,
		     "an insert without a value for the primary key is rejected");
    
    sql_command("DROP TABLE $TEST_DB.edt_auth_cov");
};
