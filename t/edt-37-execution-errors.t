#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
#
# edt-37-execution-errors.t : Test the error paths in EditTransaction::Execution that are
#                             not covered by edt-32: actions that acquire errors after they
#                             are created, failures of the checks made just before execution,
#                             foreign key prechecks, exceptions from the hooks, replace
#                             statements, and failures while writing the log file.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 12;
use File::Temp qw(tempdir);
use Fcntl qw(:flock);

use ETBasicTest;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_no_conditions ok_no_errors
		  ok_has_condition ok_has_one_condition clear_table sql_command
		  ok_found_record ok_no_record);

use TableDefs qw(%TABLE set_table_name set_column_property get_table_property);


# Establish an EditTester instance.

my $T = EditTester->new('ETBasicTest');

my ($TEST_DB) = $TABLE{EDT_TEST} =~ /^(\w+)\./;


# Create a throwaway table with a foreign key column that refers to EDT_TEST. Unlike
# EDT_SUB, it is not subordinate to EDT_TEST, so actions on it are not authorized against
# the referenced record.

sql_command("DROP TABLE IF EXISTS $TEST_DB.edt_fkey");
sql_command("CREATE TABLE $TEST_DB.edt_fkey (fk_no int unsigned primary key auto_increment,
	     test_no int unsigned not null default 0)");

set_table_name(EDT_FKEY => "$TEST_DB.edt_fkey");
set_column_property(EDT_FKEY => test_no => FOREIGN_KEY => 'EDT_TEST');


# Clear the test tables and add records with known keys.

sub reset_tables {
    
    clear_table('EDT_TEST');
    clear_table('EDT_SUB');
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req) VALUES (5, 'five'), (6, 'six')");
    sql_command("INSERT INTO <<EDT_SUB>> (aux_no, test_no, name) VALUES (1, 5, 'sub one'),
		 (2, 5, 'sub two')");
}


# Check that the necessary methods are defined.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'execute_action_list', 'pre_execution_check', 'check_key',
	   'fetch_old_record', 'write_log')
	
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check an action that acquires an error after it was created. Under PROCEED the error
# does not block the transaction, but the action itself is not executed.

subtest 'error added after an action is created' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt('PROCEED');
    
    $edt->insert_record('EDT_TEST', { string_req => 'late error' });
    
    $edt->add_condition('&_', 'E_TEST', 'late');
    
    $edt->insert_record('EDT_TEST', { string_req => 'no error' });
    
    ok_commit("the transaction commits under PROCEED");
    
    is( $edt->action_status('&#1'), 'failed', "the action with the error failed" );
    is( $edt->action_status('&#2'), 'executed', "the other action was executed" );
    is( $edt->fail_count, 1, "the failed action was counted" );
    
    ok_no_record('EDT_TEST', "string_req = 'late error'", "the action with the error was not executed");
    ok_found_record('EDT_TEST', "string_req = 'no error'", "the other action was executed");
};


# Check the checks made by execute_action_list and pre_execution_check just before an
# action is executed. These guard against subclass code that leaves an action in an
# inconsistent state.

subtest 'pre-execution checks' => sub {
    
    reset_tables();
    
    no warnings 'redefine';
    
    # An action whose operation is not recognized.
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'bad operation' });
    
    $edt->action_ref('&_')->set_operation('bogus');
    
    ok_failed_commit("an action with an unrecognized operation fails");
    
    ok_has_one_condition('all', 'E_EXECUTE', qr/routing/, "the bad operation is reported");
    
    # An action whose permission is still pending at execution time, because
    # authorize_against_table did not set it.
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { _label => 'p', string_req => 'new parent' });
    $edt->insert_record('EDT_SUB', { test_no => '&p', name => 'pending child' });
    
    is( $edt->action_ref('&_')->permission, 'PENDING', "the child's permission is pending" );
    
    {
	local *ETBasicTest::authorize_against_table = sub { return };
	
	ok_failed_commit("an action whose permission is still pending fails");
    }
    
    ok_has_one_condition('all', 'E_EXECUTE', qr/checking authorization/,
			 "the missing permission is reported");
    
    # An action whose validation is still pending at execution time, because
    # validate_against_schema did not complete it.
    
    {
	local *ETBasicTest::validate_action = sub { $_[1]->validate_later };
	local *ETBasicTest::validate_against_schema = sub { return };
	
	$edt = ok_new_edt;
	
	$edt->insert_record('EDT_TEST', { string_req => 'pending validation' });
	
	is( $edt->action_ref('&_')->validation_status, 'PENDING', "the validation is pending" );
	
	ok_failed_commit("an action whose validation is still pending fails");
    }
    
    ok_has_one_condition('all', 'E_EXECUTE', qr/checking validation/,
			 "the incomplete validation is reported");
    
    # An exception thrown while doing the prechecks.
    
    {
	local *ETBasicTest::check_key = sub { die "check_key exception\n" };
	
	$edt = ok_new_edt;
	
	$edt->insert_record('EDT_FKEY', { test_no => 5 });
	
	ok_failed_commit("an exception during the prechecks fails the action");
    }
    
    ok_has_one_condition('all', 'E_EXECUTE', qr/exception/, "the exception is reported");
};


# Check the foreign key prechecks, which are made just before an action is executed.

subtest 'foreign key prechecks' => sub {
    
    reset_tables();
    clear_table('EDT_FKEY');
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { _label => 'p', string_req => 'new parent' });
    $edt->insert_record('EDT_FKEY', { _label => 'f', test_no => '&p' });
    $edt->insert_record('EDT_FKEY', { test_no => 6 });
    
    ok_commit;
    
    my $parent_key = $edt->action_keyval('&p');
    
    ok_found_record('EDT_FKEY', "test_no = '$parent_key'", "reference to a new record was resolved");
    ok_found_record('EDT_FKEY', "test_no = 6", "existing foreign key value was accepted");
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_FKEY', { test_no => 9999 });
    
    ok_failed_commit("a foreign key value that does not exist fails");
    
    ok_has_one_condition('all', 'E_BAD_REFERENCE', qr/test_no/, "the bad value is reported");
    
    # A reference to an action that selected more than one record.
    
    $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { _label => 'm', _primary => '5,6', string_req => 'multiple' });
    $edt->insert_record('EDT_FKEY', { test_no => '&m' });
    
    ok_failed_commit("a reference to an action with multiple keys fails");
    
    ok_has_one_condition('all', 'E_BAD_REFERENCE', qr/multiple keys for '&m'/,
			 "the multiple keys are reported once, naming the reference");
    
    # A reference to an action that was never executed, because under PROCEED it was
    # skipped because of an error.
    
    $edt = ok_new_edt('PROCEED');
    
    $edt->insert_record('EDT_TEST', { _label => 'b', string_req => 'bad', signed_val => 'not a number' });
    $edt->insert_record('EDT_FKEY', { test_no => '&b' });
    
    ok_commit("the transaction commits under PROCEED");
    
    ok_has_condition('all', 'E_BAD_REFERENCE', qr/[Nn]o key value found/,
		     "the unresolved reference is reported");
    
    is( $edt->action_status('&#2'), 'failed', "the action with the reference failed" );
    
    ok_has_condition('all', 'E_BAD_REFERENCE', qr/'&b'/, "the message names the reference");
    
    is( scalar(grep { /E_BAD_REFERENCE/ } $edt->conditions), 1,
	"the unresolved reference is reported once" );
    
    # A foreign key to a table whose primary key is not set as a table property, but is
    # found in the database schema. [Added by Claude under the guidance of Michael
    # McClennen]
    
    clear_table('EDT_TYPES');
    
    sql_command("INSERT INTO <<EDT_TYPES>> (name) VALUES ('type one')");
    sql_command("DROP TABLE IF EXISTS $TEST_DB.edt_fkey_types");
    sql_command("CREATE TABLE $TEST_DB.edt_fkey_types (fk_no int unsigned primary key auto_increment,
		 type_name varchar(40) not null default '')");
    
    set_table_name(EDT_FKEY_TYPES => "$TEST_DB.edt_fkey_types");
    set_column_property(EDT_FKEY_TYPES => type_name => FOREIGN_KEY => 'EDT_TYPES');
    
    ok( ! defined get_table_property('EDT_TYPES', 'PRIMARY_KEY'),
	"EDT_TYPES has no PRIMARY_KEY property" );
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_FKEY_TYPES', { type_name => 'type one' });
    
    ok_commit("a foreign key value is checked against the primary key from the schema");
    
    ok_found_record('EDT_FKEY_TYPES', "type_name = 'type one'", "the record was stored");
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_FKEY_TYPES', { type_name => 'no such type' });
    
    ok_failed_commit("a foreign key value that does not exist in that table fails");
    
    ok_has_one_condition('all', 'E_BAD_REFERENCE', qr/no such type/, "the bad value is reported");
    
    sql_command("DROP TABLE $TEST_DB.edt_fkey_types");
};


# Check exceptions thrown by after_action for update, replace, and delete, and by
# before_action for delete. Each of these fails the action and the transaction.

subtest 'hook exceptions' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { test_no => 5, string_req => 'after exception' });
    
    ok_failed_commit("update fails if after_action throws an exception");
    
    ok_has_one_condition('all', 'E_EXECUTE', qr/exception/, "the exception is reported");
    
    $edt = ok_new_edt;
    
    $edt->replace_record('EDT_TEST', { test_no => 5, string_req => 'after exception' });
    
    ok_failed_commit("replace fails if after_action throws an exception");
    
    ok_has_one_condition('all', 'E_EXECUTE', qr/exception/, "the exception is reported");
    
    no warnings 'redefine';
    
    {
	local *ETBasicTest::after_action = sub { die "after delete\n" if $_[2] eq 'delete' };
	
	$edt = ok_new_edt;
	
	$edt->delete_record('EDT_SUB', 1);
	
	ok_failed_commit("delete fails if after_action throws an exception");
    }
    
    ok_has_one_condition('all', 'E_EXECUTE', qr/exception/, "the exception is reported");
    
    is( $edt->action_status('&#1'), 'failed', "the delete action failed" );
    
    {
	local *ETBasicTest::before_action = sub { die "before delete\n" if $_[2] eq 'delete' };
	
	$edt = ok_new_edt;
	
	$edt->delete_record('EDT_SUB', 1);
	
	ok_failed_commit("delete fails if before_action throws an exception");
    }
    
    is( $edt->{save_cleanup_action_count}, 1, "cleanup_action was called for the failed delete" );
    
    ok_found_record('EDT_SUB', "aux_no = 1", "the record was not deleted");
};


# Check an exception thrown by cleanup_action.

subtest 'cleanup_action exception' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt('PROCEED');
    
    $edt->set_attr('cleanup action exception', 1);
    
    $edt->insert_record('EDT_TEST', { string_req => 'before error' });
    
    ok_commit("the transaction commits under PROCEED");
    
    ok_has_condition('all', 'W_TEST', qr/cleanup exception/, "cleanup_action was called");
    
    ok_has_condition('all', 'E_EXECUTE', qr/cleanup_action/, "the exception is reported");
};


# Check a replace whose new values duplicate a unique key of a different record. REPLACE
# deletes that record too, and a warning reports how many other records were replaced.

subtest 'replace with a duplicate unique key' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    $edt->replace_record('EDT_SUB', { aux_no => 2, test_no => 5, name => 'sub two again' });
    
    ok_commit;
    
    ok_no_conditions('all', "an ordinary replace has no warning");
    
    $edt = ok_new_edt;
    
    $edt->replace_record('EDT_SUB', { aux_no => 2, test_no => 5, name => 'sub one' });
    
    ok_commit("a replace that duplicates a unique key of another record commits");
    
    ok_has_one_condition('all', 'W_REPLACED', qr/1 other record/,
			 "the other record that was replaced is reported");
    
    ok_found_record('EDT_SUB', "aux_no = 2 and name = 'sub one'", "the new record was stored");
    
    ok_no_record('EDT_SUB', "aux_no = 1", "the other record was deleted");
};


# Check that fetch_old_record returns nothing for an action that has no key.

subtest 'fetch_old_record' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt;
    
    $edt->update_record('EDT_TEST', { test_no => 5, string_req => 'old record' });
    
    my $old = $edt->fetch_old_record($edt->action_ref('&_'));
    
    is( ref $old && $old->{string_req}, 'five', "the old record is fetched for an action with a key" );
    
    $edt->insert_record('EDT_TEST', { string_req => 'no key' });
    
    ok( ! defined $edt->fetch_old_record($edt->action_ref('&_')),
	"nothing is returned for an action with no key" );
};


# Check what happens if the log file cannot be written. The transaction still commits, and
# the log lines are written to STDERR so that they are not lost.

sub commit_with_log {
    
    my ($logfile) = @_;
    
    my $save_filename = ETBasicTest->log_filename;
    my $save_all = ETBasicTest->log_all_tables;
    
    ETBasicTest->log_filename($logfile);
    ETBasicTest->log_all_tables(1);
    
    my $stderr = '';
    my $edt;
    
    {
	local *STDERR;
	open(STDERR, '>', \$stderr);
	
	$edt = ok_new_edt;
	
	$edt->insert_record('EDT_TEST', { string_req => 'logged' });
	
	ok_commit;
    }
    
    ETBasicTest->log_filename($save_filename);
    ETBasicTest->log_all_tables($save_all);
    
    return $stderr;
}

subtest 'log file cannot be opened' => sub {
    
    my $logdir = tempdir(CLEANUP => 1);
    
    my $stderr = commit_with_log("$logdir/no_such_dir/edt-37.log");
    
    like( $stderr, qr/DATALOG ERROR: cannot open/, "the error is written to STDERR" );
    
    like( $stderr, qr/INSERT INTO/, "the log lines are written to STDERR" );
};

subtest 'log file cannot be locked' => sub {
    
    my $logdir = tempdir(CLEANUP => 1);
    my $logfile = "$logdir/edt-37.log";
    
    open(my $lockfh, '>>', $logfile) || die "could not open $logfile: $!";
    flock($lockfh, LOCK_EX) || die "could not lock $logfile: $!";
    
    my $stderr = commit_with_log($logfile);
    
    like( $stderr, qr/DATALOG ERROR: cannot lock/, "the error is written to STDERR" );
    
    like( $stderr, qr/INSERT INTO/, "the log lines are written to STDERR" );
    
    close $lockfh;
    
    sql_command("DROP TABLE $TEST_DB.edt_fkey");
};
