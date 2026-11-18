#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
#
# edt-38-transaction-errors.t : Test the parts of EditTransaction.pm that are not covered
#                               elsewhere: the less common forms of the constructor,
#                               failures of the database statements that start, commit, and
#                               roll back a transaction, the allowance methods, and the
#                               default hook methods.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 8;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_no_conditions ok_has_condition
		  ok_has_one_condition ok_exception clear_table sql_command
		  ok_found_record ok_no_record);

use TableDefs qw(%TABLE);


# Establish an EditTester instance.

my $T = EditTester->new('ETBasicTest');

my $dbh = $T->dbh;

my ($TEST_DB) = $TABLE{EDT_TEST} =~ /^(\w+)\./;


# Call the specified code with DBI's 'do' method replaced, so that the statement matching
# the specified pattern throws an exception. If $before is given, it is executed first,
# using the real 'do' method.

my $DBI_DO = \&DBI::db::do;

sub with_failing_statement {
    
    my ($pattern, $before, $code) = @_;
    
    no warnings 'redefine';
    
    local *DBI::db::do = sub {
	if ( $_[1] =~ $pattern )
	{
	    $DBI_DO->($_[0], $before) if $before;
	    die "simulated failure of '$_[1]'\n";
	}
	
	return $DBI_DO->(@_);
    };
    
    return $code->();
}


sub in_transaction {
    
    my ($in_transaction) = $dbh->selectrow_array('SELECT @@in_transaction');
    return $in_transaction;
}


# Check that the necessary methods are defined.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'new', 'commit', 'rollback', 'has_allowance', 'allows', 'key_labels')
	
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check the less common forms of the constructor.

subtest 'constructor' => sub {
    
    # The old form of the constructor takes a permission object, a table name, and a list
    # of allowances.
    
    my $perm = bless { }, 'Permissions';
    
    my $edt = ETBasicTest->new($T, $perm, 'EDT_TEST', 'PROCEED');
    
    ok( $edt, "the old form of the constructor is accepted" );
    
    is( $edt->default_table, 'EDT_TEST', "the table name is taken from the old form" );
    
    ok( $edt->allows('PROCEED'), "the allowances are taken from the old form" );
    
    # A class must include a database role.
    
    {
	package ETNoDatabase;
	our @ISA = ('EditTransaction');
    }
    
    ok_exception( sub { ETNoDatabase->new($T) }, qr/must include a database role/,
		  "a class without a database role is rejected" );
    
    # A database handle that the database role does not accept is reported.
    
    {
	no warnings 'redefine';
	
	local *ETBasicTest::db_validate_dbh = sub { "not the expected database" };
	
	$edt = ETBasicTest->new($T);
    }
    
    ok_has_one_condition($edt, 'main', 'E_BAD_CONNECTION', qr/not the expected database/,
			 "a database handle that is not accepted is reported");
    
    # A request object provides the database handle through 'get_connection', and may
    # provide the debugging mode through 'debug'.
    
    {
	package ETRequest;
	sub new { bless { dbh => $_[1] }, $_[0] }
	sub get_connection { $_[0]{dbh} }
	sub debug { 1 }
	
	package ETBadRequest;
	sub new { bless { }, $_[0] }
	sub get_connection { 'not a handle' }
    }
    
    $edt = ETBasicTest->new(ETRequest->new($dbh));
    
    is( $edt->dbh, $dbh, "the database handle is taken from the request object" );
    
    ok( $edt->debug_mode, "the debugging mode is taken from 'debug'" );
    
    ok_exception( sub { ETBasicTest->new(ETBadRequest->new) }, qr/failed to return a database handle/,
		  "a request object that does not provide a handle is rejected" );
};


# Check the allowance methods.

subtest 'allowances' => sub {
    
    my $edt = ok_new_edt('PROCEED');
    
    ok( $edt->has_allowance('PROCEED'), "has_allowance works as an instance method" );
    
    ok( ! $edt->has_allowance('NO_SUCH_ALLOWANCE'), "has_allowance rejects an unknown name" );
    
    ok( (grep { $_ eq 'PROCEED' } $edt->allows), "allows with no argument lists the allowances" );
    
    is_deeply( [ EditTransaction::parse_allowances('') ], [ ], "an empty allowance string is ignored" );
};


# Check a failure of the statement that starts the transaction.

subtest 'start transaction fails' => sub {
    
    clear_table('EDT_TEST');
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'not started' });
    
    with_failing_statement(qr/^START TRANSACTION/, undef, sub {
	ok_failed_commit("commit fails if the transaction cannot be started");
    });
    
    ok_has_one_condition('main', 'E_EXECUTE', qr/starting the transaction/, "the failure is reported");
    
    is( $edt->status, 'failed', "the transaction status is 'failed'" );
    
    ok( ! in_transaction(), "no database transaction is active" );
    
    ok_no_record('EDT_TEST', "string_req = 'not started'", "the record was not stored");
    
    # If the failure occurs after the database transaction has been started, it is rolled
    # back.
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'started' });
    
    with_failing_statement(qr/^START TRANSACTION/, 'START TRANSACTION', sub {
	ok_failed_commit("commit fails if starting the transaction throws an exception");
    });
    
    ok( ! in_transaction(), "the database transaction was rolled back" );
};


# Check a failure of the COMMIT statement.

subtest 'commit fails' => sub {
    
    clear_table('EDT_TEST');
    
    sql_command("DROP TABLE IF EXISTS $TEST_DB.edt_held");
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'not committed' });
    
    # A DDL statement passed to do_sql is held until the commit, and is skipped if the
    # commit fails.
    
    $edt->do_sql("CREATE TABLE $TEST_DB.edt_held (a int)");
    
    with_failing_statement(qr/^COMMIT$/, undef, sub {
	ok_failed_commit("commit fails if the COMMIT statement fails");
    });
    
    ok_has_one_condition('all', 'E_EXECUTE', qr/committing the transaction/, "the failure is reported");
    
    is( $edt->status, 'failed', "the transaction status is 'failed'" );
    
    ok( ! in_transaction(), "the transaction was rolled back" );
    
    ok_no_record('EDT_TEST', "string_req = 'not committed'", "the record was not stored");
    
    is( $edt->action_status('&#2'), 'skipped', "the held statement was skipped" );
    
    ok( ! $edt->table_exists("$TEST_DB.edt_held"), "the held statement was not executed" );
    
    # If the COMMIT fails because the database has already ended the transaction, for
    # example after a deadlock, there is nothing to roll back.
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'rolled back by the server' });
    
    my $result = with_failing_statement(qr/^COMMIT$/, 'ROLLBACK', sub { $edt->commit });
    
    ok_has_one_condition('all', 'E_EXECUTE', qr/committing the transaction/, "the failure is reported");
    
    ok_no_record('EDT_TEST', "string_req = 'rolled back by the server'", "the record was not stored");
    
    ok( ! $result, "commit returns false" );
    
    is( $edt->status, 'failed', "the transaction status is 'failed'" );
};


# Check a failure of the ROLLBACK statement.

subtest 'rollback fails' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->start_execution;
    
    $edt->insert_record('EDT_TEST', { string_req => 'rollback' });
    
    with_failing_statement(qr/^ROLLBACK$/, undef, sub { $edt->rollback });
    
    ok_has_one_condition('main', 'E_EXECUTE', qr/rolling back the transaction/,
			 "the failure is reported");
    
    is( $edt->status, 'aborted', "the transaction status is 'aborted'" );
    
    $DBI_DO->($dbh, 'ROLLBACK');
};


# Check the default hook methods, by running a transaction in a class that does not
# override any of them.

subtest 'default hooks' => sub {
    
    clear_table('EDT_TEST');
    
    clear_table('EDT_SUB');
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req) VALUES (5, 'five'), (6, 'six')");
    sql_command("INSERT INTO <<EDT_SUB>> (test_no, name) VALUES (5, 'taken')");
    
    my $edt = ETTrivialClass->new($T, { table => 'EDT_TEST', allows => { PROCEED => 1 } });
    
    # This action fails at execution because of a duplicate unique key, so the default
    # cleanup_action method is called.
    
    $edt->insert_record('EDT_SUB', { test_no => 5, name => 'taken' });
    
    $edt->insert_record('EDT_TEST', { string_req => 'trivial', _label => 't' });
    $edt->update_record('EDT_TEST', { test_no => 5, string_req => 'trivial update' });
    $edt->delete_record('EDT_TEST', 6);
    $edt->update_record('EDT_TEST', { test_no => 99, string_req => 'not found' });
    
    ok( $edt->commit, "a transaction in a class with the default hooks commits" );
    
    ok_found_record('EDT_TEST', "string_req = 'trivial'", "the insert was executed");
    ok_found_record('EDT_TEST', "test_no = 5 and string_req = 'trivial update'", "the update was executed");
    ok_no_record('EDT_TEST', "test_no = 6", "the delete was executed");
    
    $edt = ETTrivialClass->new($T);
    
    $edt->insert_record('EDT_TEST', { string_req => 'rolled back' });
    $edt->rollback;
    
    is( $edt->status, 'aborted', "a transaction in a class with the default hooks rolls back" );
    
    # key_labels with no argument returns the labels for every table.
    
    my $edt2 = ok_new_edt;
    
    $edt2->insert_record('EDT_TEST', { string_req => 'labeled', _label => 'lab' });
    
    ok_commit;
    
    my $labels = $edt2->key_labels;
    
    is( ref $labels, 'HASH', "key_labels with no argument returns a hash" );
    
    is_deeply( [ values $labels->{EDT_TEST}->%* ], [ 'lab' ], "the hash contains the labels for each table" );
};


# Check the debugging and error output when no request object is given. It is written
# to STDERR.

subtest 'output without a request object' => sub {
    
    my $stderr = '';
    
    {
	local *STDERR;
	open(STDERR, '>', \$stderr);
	
	my $edt = ETTrivialClass->new($dbh, { allows => 'DEBUG_MODE' });
	
	$edt->start_execution;
	
	$edt->insert_record('EDT_TEST', { string_req => 'debug output' });
	$edt->insert_record('EDT_TEST', { string_req => 'debug output', test_no => 'bad' });
	$edt->error_line("an error line");
	
	$edt->rollback;
    }
    
    like( $stderr, qr/an error line/, "error output is written to STDERR" );
    
    like( $stderr, qr/ROLLBACK TRANSACTION/, "debugging output is written to STDERR" );
};
