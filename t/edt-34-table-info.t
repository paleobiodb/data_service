#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
#
# edt-34-table-info.t : Test the methods that report information about database tables,
#                       and the methods from EditTransaction::DBStatements that execute
#                       SQL statements directly.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 10;

use ETBasicTest;
use EditTester qw(ok_new_edt ok_exception ok_commit clear_table sql_command
		  ok_found_record ok_no_record capture_mode captured_output clear_captured_output);

use File::Temp qw(tempdir);

use TableDefs qw(%TABLE);


# Establish an EditTester instance.

my $T = EditTester->new('ETBasicTest');


# Check that the necessary methods are defined.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'table_exists', 'table_column_list', 'table_has_column',
	   'get_table_property', 'get_column_property', 'get_column_type',
	   'get_table_description', 'table_info_present', 'clear_table_cache',
	   'selectrow_array', 'do_stmt')
	
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check table_exists, table_column_list, and table_has_column.

subtest 'tables and columns' => sub {
    
    my $edt = ok_new_edt;
    
    is( $edt->table_exists('EDT_TEST'), $TABLE{EDT_TEST}, "table_exists returns the table name" );
    is( ETBasicTest->table_exists('EDT_TEST'), $TABLE{EDT_TEST}, "table_exists works as a class method" );
    is( $edt->table_exists($TABLE{EDT_TEST}), $TABLE{EDT_TEST}, "table_exists accepts a table name" );
    is( $edt->table_exists('NO_SUCH_TABLE'), '', "table_exists returns '' for an unknown table" );
    
    ok_exception( sub { $edt->table_exists() }, qr/table name/, "table_exists requires an argument" );
    
    is_deeply( [ $edt->table_column_list('EDT_TEST') ],
	       [ 'test_no', 'string_val', 'string_req', 'signed_val' ],
	       "table_column_list returns the columns in order" );
    
    is_deeply( [ $edt->table_column_list('NO_SUCH_TABLE') ], [ ],
	       "table_column_list returns nothing for an unknown table" );
    
    is( $edt->table_has_column('EDT_TEST', 'string_req'), 'string_req',
	"table_has_column returns the column name" );
    is( $edt->table_has_column('EDT_TEST', 'no_such_column'), '',
	"table_has_column returns '' for an unknown column" );
    
    ok_exception( sub { $edt->table_has_column('EDT_TEST') }, qr/column name/,
		  "table_has_column requires a column name" );
};


# Check get_table_property, get_column_property, and get_column_type.

subtest 'properties and types' => sub {
    
    my $edt = ok_new_edt;
    
    is( $edt->get_table_property('EDT_TEST', 'PRIMARY_KEY'), 'test_no', "table property PRIMARY_KEY" );
    is( $edt->get_table_property('EDT_SUB', 'AUTH_TABLE'), 'EDT_TEST', "table property AUTH_TABLE" );
    
    ok_exception( sub { $edt->get_table_property('EDT_TEST', 'NO_SUCH_PROPERTY') },
		  qr/invalid property name/, "unknown table property" );
    
    ok_exception( sub { $edt->get_table_property('NO_SUCH_TABLE', 'PRIMARY_KEY') },
		  qr/unknown table name/, "table property of an unknown table" );
    
    is( $edt->get_column_property('EDT_SUB', 'name', 'REQUIRED'), 1, "column property REQUIRED" );
    is( $edt->get_column_property('EDT_SUB', 'test_no', 'FOREIGN_KEY'), 'EDT_TEST',
	"column property FOREIGN_KEY" );
    
    ok_exception( sub { $edt->get_column_property('EDT_SUB', 'name', 'NO_SUCH_PROPERTY') },
		  qr/invalid property name/, "unknown column property" );
    
    ok_exception( sub { $edt->get_column_property('EDT_SUB', 'no_such_column', 'REQUIRED') },
		  qr/does not exist/, "property of an unknown column" );
    
    ok_exception( sub { $edt->get_column_property('NO_SUCH_TABLE', 'name', 'REQUIRED') },
		  qr/unknown table name/, "column property of an unknown table" );
    
    is_deeply( [ map { $edt->get_column_type('EDT_TYPES', $_) }
		 qw(signed_val decimal_val string_val dcr enum_val) ],
	       [ 'integer', 'fixed', 'char', 'datetime', 'enum' ],
	       "get_column_type returns the main type of each column" );
    
    ok_exception( sub { $edt->get_column_type('EDT_TYPES', 'no_such_column') },
		  qr/does not exist/, "type of an unknown column" );
};


# Check get_table_description, which returns a copy of the cached table information.

subtest 'get_table_description' => sub {
    
    my $edt = ok_new_edt;
    
    my $description = $edt->get_table_description('EDT_TEST');
    
    is( $description->{TABLE_NAME}, $TABLE{EDT_TEST}, "description includes the table name" );
    is( $description->{PRIMARY_KEY}, 'test_no', "description includes the primary key" );
    
    push $description->{COLUMN_LIST}->@*, 'added_column';
    
    is( scalar(grep { $_ eq 'added_column' } $edt->table_column_list('EDT_TEST')), 0,
	"changing the description does not change the cached information" );
    
    ok_exception( sub { $edt->get_table_description('NO_SUCH_TABLE') },
		  qr/unknown table name/, "description of an unknown table" );
};


# Check the table information cache.

subtest 'table information cache' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->table_info_ref('EDT_TEST');
    
    ok( $edt->table_info_present('EDT_TEST'), "table information is cached once fetched" );
    ok( $edt->table_info_present('EDT_TEST', 'string_req'), "a column of a cached table is present" );
    ok( ! $edt->table_info_present('EDT_TEST', 'no_such_column'), "an unknown column is not present" );
    
    ETBasicTest->clear_table_cache('EDT_TEST');
    
    ok( ! $edt->table_info_present('EDT_TEST'), "clear_table_cache removes the table information" );
    
    ok( $edt->table_exists('EDT_TEST'), "the table information is fetched again when needed" );
    
    ok( $edt->table_info_present('EDT_TEST'), "and is cached again" );
    
    ok( ETBasicTest->alter_column_property('EDT_TEST', 'string_val', 'REQUIRED', 1),
	"alter_column_property returns true for a known column" );
    
    is( $edt->get_column_property('EDT_TEST', 'string_val', 'REQUIRED'), 1,
	"alter_column_property changes the cached property" );
    
    ETBasicTest->alter_column_property('EDT_TEST', 'string_val', 'REQUIRED', undef);
    
    ok( ! defined ETBasicTest->alter_column_property('EDT_TEST', 'no_such_column', 'REQUIRED', 1),
	"alter_column_property returns undef for an unknown column" );
    
    is( $edt->get_table_handling('EDT_TEST', 'string_req'), '',
	"get_table_handling returns '' for a column with no directive" );
    
    is( $edt->get_table_handling('EDT_TEST', 'no_such_column'), undef,
	"get_table_handling returns undef for an unknown column" );
};


# Check the methods from EditTransaction::DBStatements.

subtest 'DBStatements' => sub {
    
    clear_table('EDT_TEST');
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req) VALUES (5, 'five')");
    
    my $edt = ok_new_edt;
    
    is_deeply( [ $edt->selectrow_array("SELECT test_no, string_req FROM $TABLE{EDT_TEST}
					WHERE test_no = ?", undef, 5) ],
	       [ 5, 'five' ], "selectrow_array returns the selected row" );
    
    is( $edt->do_stmt("UPDATE $TABLE{EDT_TEST} SET string_val = 'changed' WHERE test_no = ?",
		      undef, 5), 1, "do_stmt returns the number of rows affected" );
    
    ok_exception( sub { $edt->do_stmt("UPDATE no_such_table SET x = 1") }, qr/no_such_table/,
		  "do_stmt throws an exception if the statement fails" );
};


# Check that the table information methods require their arguments.

subtest 'bad arguments' => sub {
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { $edt->get_table_property('EDT_TEST') }, qr/property name/,
		  "get_table_property requires a property name" );
    
    ok_exception( sub { $edt->get_column_property('EDT_TEST', 'string_req') }, qr/property name/,
		  "get_column_property requires a property name" );
    
    ok_exception( sub { $edt->get_column_type('EDT_TEST') }, qr/column name/,
		  "get_column_type requires a column name" );
    
    ok_exception( sub { $edt->get_table_description() }, qr/table name/,
		  "get_table_description requires a table name" );
    
    ok_exception( sub { $edt->get_table_handling('EDT_TEST') }, qr/column name/,
		  "get_table_handling requires a column name" );
};


# Check the debugging output of the DBStatements methods. This subtest was added by Claude
# under the guidance of Michael McClennen.

subtest 'DBStatements debug output' => sub {
    
    clear_table('EDT_TEST');
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req) VALUES (5, 'five')");
    
    clear_captured_output();
    capture_mode(1);
    
    my $edt = ok_new_edt('DEBUG_MODE');
    
    $edt->selectrow_array("SELECT string_req FROM $TABLE{EDT_TEST} WHERE test_no = ?", undef, 5);
    
    my $output = captured_output();
    
    like( $output, qr/SELECT string_req FROM \Q$TABLE{EDT_TEST}\E WHERE test_no = \?/,
	  "selectrow_array writes the statement" );
    like( $output, qr/\['5'\]/, "selectrow_array writes the bind values" );
    
    clear_captured_output();
    
    $edt->do_stmt("UPDATE $TABLE{EDT_TEST} SET string_val = ? WHERE test_no = ?", undef, 'x', 5);
    
    $output = captured_output();
    
    like( $output, qr/UPDATE \Q$TABLE{EDT_TEST}\E SET string_val = \?/, "do_stmt writes the statement" );
    like( $output, qr/\['x', '5'\]/, "do_stmt writes the bind values" );
    like( $output, qr/This statement affected 1 rows/, "do_stmt writes the number of rows affected" );
    
    clear_captured_output();
    
    $edt = ok_new_edt;
    
    $edt->do_stmt("UPDATE $TABLE{EDT_TEST} SET string_val = 'y' WHERE test_no = ?", undef, 5);
    
    is( captured_output(), '', "nothing is written without debug mode" );
    
    capture_mode(0);
    clear_captured_output();
};


# Check that selectrow_array throws an exception if its statement fails. This subtest was
# added by Claude under the guidance of Michael McClennen.

subtest 'DBStatements selectrow_array failure' => sub {
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { $edt->selectrow_array("SELECT x FROM no_such_table") }, qr/no_such_table/,
		  "selectrow_array throws an exception if the statement fails" );
};


# Check do_logged_stmt, which executes a statement and logs it. It is meant to be called
# while a transaction is active, for example from 'after_action', and the log is written
# when the transaction commits. This subtest was added by Claude under the guidance of
# Michael McClennen.

subtest 'do_logged_stmt' => sub {
    
    clear_table('EDT_TEST');
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req) VALUES (5, 'five'), (6, 'six')");
    
    my $logdir = tempdir(CLEANUP => 1);
    my $logfile = "$logdir/edt-34.log";
    
    my $save_filename = ETBasicTest->log_filename;
    
    ETBasicTest->log_filename($logfile);
    
    my $edt = ok_new_edt;
    
    $edt->start_execution;
    
    is( $edt->do_logged_stmt('update', 'EDT_TEST',
			     "UPDATE $TABLE{EDT_TEST} SET string_val = 'logged' WHERE test_no = 5",
			     "test_no = 5"), 1, "do_logged_stmt returns the number of rows affected" );
    
    ok_commit;
    
    ok_found_record('EDT_TEST', "test_no = 5 and string_val = 'logged'", "the statement was executed");
    
    my $log = '';
    
    if ( open(my $fh, '<', $logfile) )
    {
	local $/;
	$log = <$fh>;
    }
    
    like( $log, qr/^# [^|]+ \| UPDATE \| \Q$TABLE{EDT_TEST}\E \| test_no = 5$/m,
	  "the log header names the operation, table, and key expression" );
    like( $log, qr/^UPDATE \Q$TABLE{EDT_TEST}\E SET string_val = 'logged' WHERE test_no = 5;$/m,
	  "the statement is logged" );
    
    unlink $logfile;
    
    $edt = ok_new_edt;
    
    $edt->start_execution;
    
    $edt->do_logged_stmt('update', 'EDT_TEST',
			 "UPDATE $TABLE{EDT_TEST} SET string_val = 'rolled back' WHERE test_no = 6",
			 "test_no = 6");
    
    $edt->rollback;
    
    ok_no_record('EDT_TEST', "string_val = 'rolled back'", "the statement is rolled back with the transaction");
    ok( ! -e $logfile, "nothing is logged for a transaction that is rolled back" );
    
    $edt = ok_new_edt('NO_LOG_MODE');
    
    $edt->start_execution;
    
    $edt->do_logged_stmt('update', 'EDT_TEST',
			 "UPDATE $TABLE{EDT_TEST} SET string_val = 'not logged' WHERE test_no = 6",
			 "test_no = 6");
    
    ok_commit;
    
    ok( ! -e $logfile, "with NO_LOG_MODE, the statement is not logged" );
    
    $edt = ok_new_edt('DEBUG_MODE');
    
    clear_captured_output();
    capture_mode(1);
    
    $edt->start_execution;
    
    $edt->do_logged_stmt('update', 'EDT_TEST',
			 "UPDATE $TABLE{EDT_TEST} SET string_val = 'debugged' WHERE test_no = 6",
			 "test_no = 6");
    
    like( captured_output(), qr/SET string_val = 'debugged'.*affected 1 rows/s,
	  "do_logged_stmt writes debugging output" );
    
    capture_mode(0);
    clear_captured_output();
    
    ok_exception( sub { $edt->do_logged_stmt('update', 'EDT_TEST', "UPDATE no_such_table SET x = 1",
					      "x = 1") },
		  qr/no_such_table/, "do_logged_stmt throws an exception if the statement fails" );
    
    $edt->rollback;
    
    ETBasicTest->log_filename($save_filename);
};
