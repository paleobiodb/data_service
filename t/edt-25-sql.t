#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
#
# edt-25-sql.t : Test the 'do_sql' method, which creates an action that executes an
#                arbitrary SQL statement. The specification is: the action uses the 'sql'
#                operation, it has no permission requirement, it is logged, and in
#                immediate mode 'do_sql' returns the result of executing the statement
#                while in regular mode it returns nothing.
#
# Added by Claude under the guidance of Michael McClennen. The 'do_sql' method has
# not yet been implemented, so most of these tests are in TODO blocks.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 8;

use File::Temp qw(tempdir);

use ETBasicTest;
use EditTester qw(ok_eval ok_exception ok_new_edt ok_no_errors ok_has_condition
		  ok_commit ok_failed_commit clear_table sql_command
		  ok_found_record ok_no_record);

use TableDefs qw(%TABLE set_table_name);


# Establish an EditTester instance.

my $T = EditTester->new('ETBasicTest');


# Store a record in EDT_TEST with a known key, for use by the subtests below.

sub reset_test_record {
    
    clear_table('EDT_TEST');
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req) VALUES (5, 'five')");
}


# Check that the necessary methods are defined.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'do_sql', 'log_filename', 'log_all_tables')
	
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# In regular mode, the statement is executed when the transaction is committed, and
# 'do_sql' returns nothing.

subtest 'do_sql in regular mode' => sub {
    
    reset_test_record();
    
    my $edt = ok_new_edt;
  
  TODO: {
	local $TODO = "do_sql is not yet implemented";
	
	my @result;
	
	ok_eval( sub { @result = $edt->do_sql("UPDATE $TABLE{EDT_TEST} SET string_req = 'by sql'
					       WHERE test_no = 5") },
		 'IGNORE', "do_sql does not throw an exception" );
	
	is( scalar(@result), 0, "do_sql returns nothing in regular mode" );
	
	is( $edt->action_operation('&_'), 'sql', "do_sql creates an action with operation 'sql'" );
	
	ok_no_errors('all', "do_sql adds no error conditions");
	
	ok_no_record('EDT_TEST', "string_req = 'by sql'", "statement is not executed before commit");
	
	ok_commit;
	
	ok_found_record('EDT_TEST', "test_no = 5 and string_req = 'by sql'",
			"statement was executed at commit");
	
	is( $edt->action_status('&#1'), 'executed', "action status is 'executed'" );
    }
};


# In immediate mode, the statement is executed right away and 'do_sql' returns the
# result of the execution.

subtest 'do_sql in immediate mode' => sub {
    
    reset_test_record();
    
    my $edt = ok_new_edt('IMMEDIATE_MODE');
  
  TODO: {
	local $TODO = "do_sql is not yet implemented";
	
	my $result = $edt->do_sql("UPDATE $TABLE{EDT_TEST} SET string_req = 'immediate'
				   WHERE test_no = 5");
	
	is( $result, 1, "do_sql returns the result of the statement in immediate mode" );
	
	ok_found_record('EDT_TEST', "test_no = 5 and string_req = 'immediate'",
			"statement was executed immediately");
	
	ok_commit;
    }
};


# The statement is executed in order with the other actions in the transaction.

subtest 'do_sql action order' => sub {
    
    clear_table('EDT_TEST');
    
    my $edt = ok_new_edt;
  
  TODO: {
	local $TODO = "do_sql is not yet implemented";
	
	$edt->insert_record('EDT_TEST', { string_req => 'before sql' });
	
	$edt->do_sql("UPDATE $TABLE{EDT_TEST} SET string_val = 'updated'
		      WHERE string_req = 'before sql'");
	
	$edt->insert_record('EDT_TEST', { string_req => 'after sql' });
	
	ok_commit;
	
	ok_found_record('EDT_TEST', "string_req = 'before sql' and string_val = 'updated'",
			"statement affected the record inserted before it");
	
	ok_found_record('EDT_TEST', "string_req = 'after sql' and string_val = ''",
			"statement did not affect the record inserted after it");
    }
};


# A statement that fails causes the transaction to fail and be rolled back.

subtest 'do_sql with a bad statement' => sub {
    
    clear_table('EDT_TEST');
    
    my $edt = ok_new_edt;
  
  TODO: {
	local $TODO = "do_sql is not yet implemented";
	
	$edt->insert_record('EDT_TEST', { string_req => 'rolled back' });
	
	ok_eval( sub { $edt->do_sql("UPDATE $TABLE{EDT_TEST} SET no_such_column = 1") },
		 'IGNORE', "bad statement does not throw an exception" );
	
	ok_failed_commit("transaction with a bad statement is not committed");
	
	ok_has_condition('all', 'E_EXECUTE', "bad statement is reported as E_EXECUTE");
	
	ok_no_record('EDT_TEST', "string_req = 'rolled back'", "other actions were rolled back");
    }
};


# The statement is written to the log. The log format for 'sql' actions has not yet
# been decided, so this only checks that the statement text appears in the log.

subtest 'do_sql logging' => sub {
    
    reset_test_record();
    
    my $logdir = tempdir(CLEANUP => 1);
    my $logfile = "$logdir/edt-25.log";
    
    my $save_filename = ETBasicTest->log_filename;
    my $save_all = ETBasicTest->log_all_tables;
    
    ETBasicTest->log_filename($logfile);
    ETBasicTest->log_all_tables(1);
    
    my $edt = ok_new_edt;
  
  TODO: {
	local $TODO = "do_sql is not yet implemented";
	
	$edt->do_sql("UPDATE $TABLE{EDT_TEST} SET string_req = 'logged' WHERE test_no = 5");
	
	ok_commit;
	
	my $log = '';
	
	if ( open(my $fh, '<', $logfile) )
	{
	    local $/;
	    $log = <$fh>;
	}
	
	like( $log, qr/SET string_req = 'logged' WHERE test_no = 5/, "statement was logged" );
    }
    
    ETBasicTest->log_filename($save_filename);
    ETBasicTest->log_all_tables($save_all);
};


# Check the substitution of table names for <<NAME>> specifiers in SQL statements.
# Specifiers inside quoted values and backquoted identifiers must be left alone.

subtest 'table name substitution' => sub {
    
    my $edt = ok_new_edt;
    
    # Substituted names are enclosed in backquotes, as `db`.`table`.
    
    my $test = join('.', map { "`$_`" } split /[.]/, $TABLE{EDT_TEST}, 2);
    my $sub = join('.', map { "`$_`" } split /[.]/, $TABLE{EDT_SUB}, 2);
    
    like( $test, qr/^`\w+`(\.`\w+`)?$/, "expected form of a quoted table name" );
    
    my $subst = sub { $edt->_substitute_table_names(@_) };
    
    is( $subst->("SELECT 1"), "SELECT 1", "statement without specifiers is unchanged" );
    
    is( $subst->("SELECT * FROM <<EDT_TEST>>"), "SELECT * FROM $test", "one specifier" );
    
    is( $subst->("SELECT * FROM <<EDT_TEST>> JOIN <<EDT_SUB>> USING (test_no)"),
	"SELECT * FROM $test JOIN $sub USING (test_no)", "two specifiers" );
    
    is( $subst->("UPDATE <<EDT_TEST>> SET string_req = '<<EDT_TEST>>'"),
	"UPDATE $test SET string_req = '<<EDT_TEST>>'", "single-quoted value is unchanged" );
    
    is( $subst->(q{UPDATE <<EDT_TEST>> SET string_req = "<<EDT_TEST>>"}),
	qq{UPDATE $test SET string_req = "<<EDT_TEST>>"}, "double-quoted value is unchanged" );
    
    is( $subst->("SELECT `<<EDT_TEST>>` FROM <<EDT_TEST>>"),
	"SELECT `<<EDT_TEST>>` FROM $test", "backquoted identifier is unchanged" );
    
    is( $subst->("SET string_req = 'it''s <<EDT_TEST>>' FROM <<EDT_SUB>>"),
	"SET string_req = 'it''s <<EDT_TEST>>' FROM $sub", "doubled quote inside a quoted value" );
    
    is( $subst->(q{SET string_req = 'it\'s <<EDT_TEST>>' FROM <<EDT_SUB>>}),
	qq{SET string_req = 'it\\'s <<EDT_TEST>>' FROM $sub}, "escaped quote inside a quoted value" );
    
    is( $subst->(q{SET string_req = 'a\\\\' FROM <<EDT_SUB>>}),
	qq{SET string_req = 'a\\\\' FROM $sub}, "escaped backslash at the end of a quoted value" );
    
    is( $subst->("SET string_req = '' FROM <<EDT_SUB>>"),
	"SET string_req = '' FROM $sub", "empty quoted value" );
    
    is( $subst->(q{SET a = "it's", b = '<<EDT_TEST>>' FROM <<EDT_SUB>>}),
	qq{SET a = "it's", b = '<<EDT_TEST>>' FROM $sub}, "one kind of quote inside the other" );
    
    is( $subst->("SET string_req = '<<EDT_TEST>>"), "SET string_req = '<<EDT_TEST>>",
	"unterminated quoted value extends to the end of the statement" );
    
    is( $subst->("SET string_req = '<<NO_SUCH_TABLE>>'"), "SET string_req = '<<NO_SUCH_TABLE>>'",
	"unknown specifier inside a quoted value is allowed" );
    
    ok_exception( sub { $subst->("SELECT * FROM <<NO_SUCH_TABLE>>") },
		  qr/E_BAD_TABLE.*NO_SUCH_TABLE/, "unknown specifier throws an exception" );
    
    ok_exception( sub { $subst->("SELECT * FROM <<>>") },
		  qr/E_BAD_TABLE/, "empty specifier throws an exception" );
    
    ok_exception( sub { $edt->do_sql("SELECT * FROM <<NO_SUCH_TABLE>>") },
		  qr/E_BAD_TABLE.*NO_SUCH_TABLE/, "do_sql throws an exception for an unknown specifier" );
};


# Check quoted_table_name, which is used for the substitution above. The specifiers
# defined here are only used by this subtest.

subtest 'quoted_table_name' => sub {
    
    set_table_name(EDT_QUOTE_PLAIN => 'plain_table');
    set_table_name(EDT_QUOTE_DB => 'some_db.some_table');
    set_table_name(EDT_QUOTE_DONE => '`some_db`.`some_table`');
    
    is( ETBasicTest->quoted_table_name('EDT_QUOTE_PLAIN'), '`plain_table`',
	"name without a database is enclosed in backquotes" );
    
    is( ETBasicTest->quoted_table_name('EDT_QUOTE_DB'), '`some_db`.`some_table`',
	"database and table are each enclosed in backquotes" );
    
    is( ETBasicTest->quoted_table_name('EDT_QUOTE_DONE'), '`some_db`.`some_table`',
	"name that already contains backquotes is unchanged" );
    
    is( ETBasicTest->quoted_table_name('NO_SUCH_TABLE'), undef, "unknown specifier returns undef" );
    
    is( ETBasicTest->quoted_table_name(''), undef, "empty specifier returns undef" );
};
