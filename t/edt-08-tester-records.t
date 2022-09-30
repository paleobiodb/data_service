#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest and EditTester classes. They
# must be tested together, because the test methods of EditTester are designed
# to report problems with calls to ETBasicTest and other subclasses of
# EditTransaction. This module makes calls with known problems to check that the
# error-checking and condition-reporting functionality works properly.
# 
# The test methods implemented by EditTester all start with 'ok_' or 'is_'.
# 
# tester-records.t :
#
#         Check that the methods implemented by EditTester for testing the
#         presence or absence of database records work properly, by using them
#         with simple arguments that should work properly, and with other sets
#         of arguments that should fail in known ways.

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 4;

use List::Util qw(all);

use ETBasicTest;
use ETTrivialClass;

use EditTester qw(clear_table sql_command count_records fetch_records
		  ok_found_record ok_no_record ok_count_records
		  get_table_name sql_command sql_selectrow
		  invert_mode ok_output ok_no_output clear_output
		  ok_last_result ok_eval ok_exception last_result);


$DB::single = 1;


my $T = EditTester->new('ETBasicTest');

# Check that the basic tests for record checking all provide good results.

subtest 'basic tests' => sub {
    
    my $testname;
    
    ok_eval( sub { $testname = get_table_name('EDT_TEST'); },
	     "get_table_name returns a table name" )
	
	|| BAIL_OUT "this test file depends on get_table_name";
    
    like( $testname, qr/`edt_test`/, "get_table_name returns underlying table name" );
    
    clear_table('EDT_TEST');
    
    ok_no_record('EDT_TEST', undef, "ok_no_record no arguments");
    
    ok_no_record('EDT_TEST', '*', "ok_no_record '*'");
    
    sql_command("INSERT INTO <<EDT_TEST>> (string_req) VALUES ('abc')");
    
    ok_last_result("first insert succeeded");
    
    sql_command("INSERT INTO <<EDT_TEST>> (string_req, signed_val) VALUES ('abc', '-48')" );
    
    ok_last_result("second insert succeeded");
    
    my $test_no = sql_selectrow("SELECT test_no FROM <<EDT_TEST>> LIMIT 1");
    
    if ( ok( $test_no, "got inserted key" ) )
    {
	ok_found_record('EDT_TEST', $test_no, "ok_found_record with key value" );
	
	ok_no_record('EDT_TEST', '99', "ok_no_record with key value" );
    }
    
    ok_found_record('EDT_TEST', undef, "ok_found_record no arguments");;
    
    ok_found_record('EDT_TEST', '*', "ok_found_record with '*'");;
    
    ok_found_record( 'EDT_TEST', "string_req='abc'", "ok_found_record with 'abc'" );
    
    ok_no_record( 'EDT_TEST', "string_req='def'", "ok_no_record with 'def'" );
    
    ok_count_records( 2, 'EDT_TEST', "string_req='abc'", "ok_count_records 'abc'" );
    
    ok_count_records( 0, 'EDT_TEST', "string_req='def'", "ok_count_records 'def'" );
    
    ok_count_records( 2, 'EDT_TEST', '*', "ok_count_records '*'" );
    
    my $a = count_records( 'EDT_TEST', "string_req='abc'" );
    
    my $b = count_records( 'EDT_TEST', "string_req='def'" );
    
    my $c = count_records( 'EDT_TEST', undef, "count_records with no expression" );
    
    is( $a, 2, "count_records with 'abc' returned 2" );
    
    is( $b, 0, "count_records with 'def' returned 0" );
    
    is( $c, 2, "count_records with no expression returned 2" );
    
    my @v = sql_selectrow('SELECT @@character_set_client, @@character_set_server');
    
    is( @v, 2, "sql_selectrow returned 2 values" );
    
    my $n_records = sql_command( "UPDATE <<EDT_TEST>> SET signed_val=2 WHERE string_req='abc'",
				 "sql_command update statement succeeded" );
    
    is( $n_records, 2, "sql_command returned number of records updated" );
    
    is( last_result, 2, "last_result contained return value from sql_command" );
    
    invert_mode(1);
    
    ok_found_record( 'EDT_TEST', "string_req='def'" );
    
    ok_no_record( 'EDT_TEST', "string_req='abc'" );
    
    clear_output;
    
    ok_found_record( 'EDT_TEST', "string_req xxx" );
    
    ok_output( qr/EXCEPTION:.*syntax/, "diag exception" );
    
    clear_output;
    
    ok_no_record( 'EDT_TEST', "string_req xxx" );
    
    ok_output( qr/EXCEPTION:.*syntax/, "diag exception" );
    
    clear_output;
    
    ok_count_records( 4, 'EDT_TEST', "string_req='abc'" );
    
    ok_output( qr/got: 2.*expected: 4/s, "diag output" );
    
    clear_output;
    
    ok_count_records( 4, 'EDT_TEST', "string_req xxx" );
    
    ok_output( qr/EXCEPTION:.*syntax/, "diag exception" );
    
    clear_output;
    
    count_records( 'EDT_TEST', "string_req xxx" );
    
    ok_output( qr/EXCEPTION:.*syntax/, "diag exception" );
    
    clear_output;
    
    sql_command( "INSERT INTO <<EDT_TEST>> (string_val)",
		 "sql_command with invalid statement" );
    
    ok_last_result;
    
    ok_output( qr/EXCEPTION:.*syntax/, "diag exception" );
    
    clear_output;
    
    sql_selectrow( "SELECT foo WHERE baz", "sql_selectrow with invalid statement" );;
    
    ok_output( qr/EXCEPTION:.*(syntax|foo)/, "diag exception" );
    
    invert_mode(0);
};


subtest 'fetch_records' => sub {
    
    sql_command("INSERT INTO <<EDT_TEST>> (string_req, string_val) VALUES ('xyz', 'foo')");
    
    ok_last_result("third insert succceeded");
    
    my @result = fetch_records( 'EDT_TEST', "string_req='abc'", 'records', 
				"fetch_records with 'records'" );
    
    is( @result, 2, "fetch_records returned two records" );
    
    @result = fetch_records('EDT_TEST', "string_req='abc'");
    
    is( @result, 2, "fetch_records returned two records with no return argument" );
    
    ok( all( sub { ref($_) eq 'HASH'}, @result ), 
	"fetch_records result is all hashrefs" )
     && ok( all( sub { $_->{string_req} eq 'abc' }, @result ), 
	    "found string_req in each result" )
     && ok( all( sub { $_->{test_no} =~ /^\d+$/ }, @result ), 
	    "found test_no in each result" );
    
    my @keyvals = fetch_records('EDT_TEST', "string_req='abc'", 'keyvalues',
				"fetch_records with 'keyvalues'" );
    
    if ( is( @keyvals, 2, "fetch_records keyvalues returned 2 values" ) &&
	 ok( all( sub { $_ =~ /^\d+$/ }, @keyvals ), "all are positive integers" ) )
    {
	my @columnvals = fetch_records( 'EDT_TEST', \@keyvals, 'column', 'string_req',
					"fetch_records with value list and 'column'" );
	
	is( @columnvals, 2, "fetch_records with value list and 'column' returned 2 values" );
	
	ok( all( sub { $_ eq 'abc' }, @columnvals ), 
	    "fetch_records column values are all 'abc'" );
	
	my @rowvals = fetch_records('EDT_TEST', $keyvals[1], 'row', 'string_req', 'string_val');
	
	is( @rowvals, 2, "fetch_records with single key value and 'row' returned two values" );
	
	is( $rowvals[0], 'abc', "first value is 'abc'" );
    }
    
    my @rowvals = fetch_records( 'EDT_TEST', "string_req='abc'", 'row', ['string_req', 'string_val'],
				 "fetch_records with expression, 'row', and listref of columns" );
    
    is( @rowvals, 2, "fetch_records returned 2 values" );
    
    my @results = fetch_records( 'EDT_TEST', '*' );
    
    is( @results, 3, "fetch_records with '*' returned three records" );
    
    invert_mode(1);
    
    clear_output;
    
    fetch_records( 'EDT_TEST', "xxx invalid sql expression", 'records', 
		   "fetch_records with bad expression" );
    
    ok_output( qr/EXCEPTION:.*syntax/, "diag exception" );
    
    invert_mode(0);
};


# Check that table specifiers in the expression provided to any test will be
# substituted properly.

subtest 'table specifier substitution' => sub {
    
    clear_table('EDT_TYPES');
    
    sql_command( "INSERT INTO <<EDT_TYPES>> (name) VALUES ('abc')",
		 "insert into EDT_TYPES so we can use 'any' below" );
    
    clear_table('EDT_SUB');
    
    sql_command( "INSERT INTO <<EDT_SUB>> (name, test_no)
		  VALUES ('aaa', 1), ('bbb', 2), ('ccc', 3)",
		 "insert into EDT_SUB so we can use 'any' below" );
    
    fetch_records( 'EDT_TEST', "string_req = any(SELECT name FROM <<EDT_TYPES>>) and
				test_no = any(SELECT test_no FROM <<EDT_SUB>>)",
		       'records', "fetch_records with 2 table specifiers" );
    
    my $count = count_records( 'EDT_TEST', "string_req = any(SELECT name FROM <<EDT_TYPES>>) and
				test_no = any(SELECT test_no FROM <<EDT_SUB>>)",
			       'records', "fetch_records with 2 table specifiers" );
    
    ok_count_records( $count, 'EDT_TEST', "string_req = any(SELECT name FROM <<EDT_TYPES>>) and
				test_no = any(SELECT test_no FROM <<EDT_SUB>>)",
		      'records', "fetch_records with 2 table specifiers" );
    
    ok_found_record( 'EDT_TEST', "string_req = any(SELECT name FROM <<EDT_TYPES>>) and
				test_no = any(SELECT test_no FROM <<EDT_SUB>>)",
		     'records', "fetch_records with 2 table specifiers" );
    
    ok_no_record( 'EDT_TEST', "string_req = any(SELECT name FROM <<EDT_TYPES>>) and
				test_no = any(SELECT test_no FROM <<EDT_SUB>>) and 0",
		  'records', "fetch_records with 2 table specifiers" );
};


subtest 'bad arguments' => sub {
    
    ok_exception( sub { get_table_name('XXX_NOT_A_TABLE'); }, qr/unknown table/i,
		  "exception get_table_name nonexistent table" );
    
    ok_exception( sub { sql_command; }, qr/specify/i,
		  "exception sql_command no arguments" );
    
    ok_exception( sub { sql_selectrow; }, qr/specify/i,
		  "exception sql_selectrow no arguments" );
    
    ok_exception( sub { ok_found_record; }, qr/specify/i,
		  "exception ok_found_record no arguments" );
    
    ok_exception( sub { ok_found_record('EDT_NOPRIM', '82'); }, qr/primary key/i,
		  "exception ok_found_record with key value on table with no primary key" );
    
    ok_exception( sub { ok_found_record('EDT_TEST', [1, 2]); }, qr/valid/,
		  "exception ok_found_record with listref" );
    
    ok_exception( sub { ok_no_record; }, qr/specify/i,
		  "exception ok_no_record no arguments" );
    
    ok_exception( sub { ok_no_record('EDT_NOPRIM', '82'); }, qr/primary key/i,
		  "exception ok_found_record with key value on table with no primary key" );
    
    ok_exception( sub { ok_no_record('EDT_TEST', [1, 2]); }, qr/valid/i,
		  "exception ok_no_record with listref" );
    
    ok_exception( sub { ok_count_records; }, qr/specify|invalid count/i,
		  "exception ok_count_records no arguments" );
    
    ok_exception( sub { ok_count_records('abc', 'EDT_TEST'); }, qr/invalid count/i,
		  "exception ok_count_records invalid count" );
    
    ok_exception( sub { ok_count_records(3, 'XXX_NOT_A_TABLE'); }, qr/unknown table/i,
		  "exception ok_count_records nonexistent table" );
    
    ok_exception( sub { ok_count_records(3, 'EDT_TEST', [1, 2]); }, qr/valid/i,
		  "exception ok_count_records with listref" );
    
    ok_exception( sub { count_records; }, qr/specify/i,
		  "exception count_records no arguments" );
    
    ok_exception( sub { count_records('XXX_NOT_A_TABLE'); }, qr/unknown table/i,
		  "exception count_records nonexistent table" );
    
    ok_exception( sub { count_records('EDT_TEST', [1, 2]); }, qr/valid/i,
		  "exception count_records with listref" );
    
    ok_exception( sub { fetch_records; }, qr/specify/i,
		  "exception fetch_records no arguments" );
    
    ok_exception( sub { fetch_records('XXX_NOT_A_TABLE'); }, qr/unknown table/i,
		  "exception fetch_records nonexistent table" );
    
    ok_exception( sub { fetch_records('EDT_TEST', { abc => 1 }); }, qr/valid/i,
		  "exception fetch_records with hashref" );
    
    ok_exception( sub { fetch_records('EDT_TEST', '*', 'column'); }, qr/specify/i,
		  "exception fetch_records with 'column' and no column name" );
    
    ok_exception( sub { fetch_records('EDT_TEST', '*', 'colmn'); }, qr/valid/i,
		  "exception fetch_records with misspelled return type" );
};

