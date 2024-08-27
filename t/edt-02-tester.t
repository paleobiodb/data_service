#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of
# EditTransaction whose purpose is to implement these tests.
# 
# tester.t : Check that the EditTester module can create new instances and edts,
#            and that exported subroutines needed for the other tests work
#            properly.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 8;

use EditTester qw(ok_eval ok_exception last_result last_result_list
		  connect_to_database capture_mode invert_mode diag_lines
		  captured_output ok_captured_output ok_no_captured_output
		  clear_captured_output);


$DB::single = 1;


# Check the 'ok_captured_output' and 'ok_no_captured_output' constructs for checking for the
# presence or absence of diagnostic output.

subtest 'ok_captured_output' => sub {
    
    clear_captured_output;
    
    ok_no_captured_output( "ok_no_captured_output after clear_captured_output" );
    
    capture_mode(1);
    
    diag_lines("foobar", "biffbaff 23");
    
    capture_mode(0);
    
    ok_captured_output( qr/foo/, "ok_captured_output 1" );
    
    ok_captured_output( qr/biff.*23/, "ok_captured_output 2" );
    
    invert_mode('output');
    
    ok_no_captured_output( "invert: ok_no_captured_output, output is present" );
    
    like( captured_output, qr/^\s*captured output was *:.*23/si, 
	  "ok_no_captured_output diag & clears output on failure" );
    
    clear_captured_output;
    
    capture_mode(1);
    
    diag_lines("bananarama");
    
    capture_mode(0);
    
    ok_captured_output( qr/razzamatazz/, "invert: ok_captured_output, output does not match" );
    
    like( captured_output, qr/was *:.*banana.*expected *:.*razz/si, 
	  "invert: ok_captured_output diag" );
    
    invert_mode(0);
};


# Check the 'ok_eval' construct for catching and reporting unexpected
# exceptions, and 'last_result' for examining the result of a successful call.

subtest 'ok_eval' => sub {
    
    ok_eval( sub { 1 + 1 }, "ok_eval true result" );
    
    is( last_result, 2, "last_result produces proper value for 1+1" );
    
    ok_eval( sub { ['xyz', 'foo'] }, "ok_eval listref result" );
    
    is( ref(last_result), 'ARRAY', "last_result produces listref" ) &&
	is( last_result->[0], 'xyz', "last_result list value 1" ) &&
	is( last_result->[1], 'foo', "last_result list value 2" );
    
    ok_eval( sub { ('xyz', 'foo') }, "ok_eval list result" );
    
    is( last_result, 'xyz', "last_result produces first value" );
    
    is( last_result_list, 2, "last_result_list has 2 values" );
    is( last_result_list(0), 'xyz', "last_result_list value 1" );
    is( last_result_list(1), 'foo', "last_result_list value 2" );
    
    ok_eval( sub { 0 }, 'IGNORE', "ok_eval false result with 'IGNORE'" );
    
    my $T = { }; bless $T, 'EditTester';
    
    $T->ok_eval( sub { 'pqr' }, "ok_eval called as method" );
    
    ok_eval( $T, sub { 23 }, "ok_eval called with instance argument" );
    
    invert_mode('eval');
    
    clear_captured_output;
    
    ok_eval( sub { die "test exception" }, "invert: ok_eval exception thrown" );
    
    ok( ! defined last_result, "last_result produces undef after exception" );
    
    ok_captured_output( qr/exception *:.*test exception/si, "invert: ok_eval diag" );
    
    ok_eval( sub { 0 }, "invert: ok_eval false result" );
    
    is( last_result, '0', "last_result produces proper value for '0'");
    
    ok_eval( sub { }, "invert: ok_eval empty subroutine" );
    
    invert_mode(0);
    
    ok( ! defined last_result, "last_result produces undef after empty subroutine" );
};


subtest 'ok_eval bad arguments' => sub {
    
    eval {
	ok_eval( "no subroutine" );
    };
    
    ok( $@ =~ /subroutine reference/i, "ok_eval no subroutine" );
    
    my $B = { }; bless $B, 'XYZ';
    
    eval {
	ok_eval( $B, sub { 4-5 }, "not a subroutine" );
    };
    
    ok( $@ =~ /subroutine reference/i, "ok_eval not a subroutine" );
};


# Check the 'ok_exception' construct for catching and testing expected exceptions.

subtest 'ok_exception' => sub {
    
    ok_exception( sub { die "test exception xyxxy" }, qr/xyxxy/, 
		  "ok_exception with matching exception" );
    
    ok_exception( sub { die "foobar" }, qr/foobar/ );
    
    ok_exception( sub { die "biffbaff" }, '*', "ok_exception with '*'" );
    
    my $T = { }; bless $T, 'EditTester';
    
    $T->ok_exception( sub { die "foobar" }, qr/foobar/, 
		      "ok_exception called as a method" );
    
    ok_exception($T, sub { die "foobar" }, qr/foobar/,
		 "ok_exception called with instance argument" );
    
    invert_mode('eval');
    
    clear_captured_output;
    
    ok_exception( sub { die "test exception xyzzy" }, qr/foobar/, 
		  "invert: ok_exception with non matching exception" );
    
    ok( ! defined last_result, "last_result undefined after exception" );
    
    ok_captured_output( qr/exception *:.*xyzzy.*expected *:.*foobar/si,
	       "invert: ok_exception diag" );
    
    clear_captured_output;
    
    ok_exception( sub { 1 }, qr/foobar/, "invert: ok_exception with no exception" );
    
    is( last_result, 1, "last_result returns subroutine value if no exception" );
    
    ok_captured_output( qr/no exception/i, "invert: ok_exception diag" );
    
    ok_exception( sub { 0 }, qr/foobar/, "ok_exception with no exception, false result" );
    
    is( last_result, 0, "last_result returns 0 if no exception" );
    
    ok_exception( sub { }, qr/foobar/, "ok_exception with no exception, nil result" );
    
    invert_mode(0);
    
    ok( ! defined last_result, "last_result undefined with empty subroutine" );
};


subtest 'ok_exception bad arguments' => sub {
    
    eval {
	ok_exception( "no subroutine" );
    };
    
    ok( $@ =~ /subroutine reference/i, "ok_exception no subroutine" );
    
    eval {
	ok_exception( sub { die "foobar" }, "no regexp" );
    };
    
    ok( $@ =~ /regexp reference/i, "ok_exception no regexp" );
    
    my $B = { }; bless $B, 'XYZ';
    
    eval {
	ok_exception( $B, sub { die "foobar" }, qr/foobar/, "not a subroutine" );
    };
    
    ok( $@ =~ /subroutine reference/i, "ok_exception not a subroutine" );
};


# Make sure the 'connect_to_database' method returns a proper result. If it does
# not, there is no point in continuing any farther.

subtest 'connect_to_database' => sub {
    
    my $dbh = connect_to_database();
    
    like( ref $dbh, qr/DBI::/, "connect_to_database returns a DBI database handle" ) ||
	BAIL_OUT "unable to connect to database";
};


# Check the various ways of creating EditTester instances via the 'new' method.

subtest 'new EditTester' => sub {
    
    ok_eval( sub { EditTester->new('ETBasicTest', 'EDT_TEST') },
	     "new EditTester" ) || BAIL_OUT "new EditTester failed";
    
    my $T = last_result;
    
    is( $T->target_class, 'ETBasicTest', "class assigned correctly" );
    is( $T->default_table, 'EDT_TEST', "table assigned correctly" );
    like( ref $T->dbh, qr/DBI::/, "dbh method returns a DBI database handle" );
    
    my $x = bless { }, 'XYZ';
    
    ok_eval( sub { EditTester->new({ class => 'ETBasicTest', table => 'EDT_TEST', 
				     debug_mode => 1, errlog_mode => 1, dbh => $x }) },
	     "new EditTester with parameter hash" ) || 
		 BAIL_OUT "new EditTester with parameter hash failed";
    
    $T = last_result;
    
    is( $T->target_class, 'ETBasicTest', "class assigned correctly" );
    is( $T->default_table, 'EDT_TEST', "table assigned correctly" );
    is( $T->debug_mode, 1, "debug_mode assigned correctly" );
    is( $T->errlog_mode, 1, "debug_mode assigned correctly" );
    is( $T->dbh, $x, "dbh assigned correctly" );
    
    $T->set_default_table('EDT_AUX');
    
    is( $T->default_table, 'EDT_AUX', "set_table method functions properly" );
    
    $T = EditTester->new('ETBasicTest');
    
    ok( ! $T->default_table, "no table assigned" );
};


subtest 'constructor bad arguments' => sub {
    
    ok_exception( sub { EditTester->new('NOT_A_CLASS_XXX') }, qr/NOT_A_CLASS_XXX.*[@]INC/,
		  "unknown class exception" );
    
    ok_exception( sub { EditTester->new('Test::More') }, qr/EditTransaction/,
		  "not subclass exception" );
};


