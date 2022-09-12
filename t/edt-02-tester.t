#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of
# EditTransaction whose purpose is to implement these tests.
# 
# tester.t : Check that the EditTester module can create new instances and
#            edts, and that exported subs needed for the other tests work
#            properly.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 7;

use EditTester qw(ok_eval ok_exception last_result connect_to_database);


$DB::single = 1;


subtest 'check ok_eval' => sub {
    
    ok_eval( sub { 1 + 1 }, "true result" );
    
    is( last_result, 2, "last_result produces proper value for 1+1" );
    
    ok_eval( sub { 'xyz' } );
    
    is( last_result, 'xyz', "last_result produces proper value for 'xyz'" );
    
    my $T = { }; bless $T, 'EditTester';
    
    $T->ok_eval( sub { 'pqr' }, "called as method" );
    
    ok_eval( $T, sub { 23 }, "called with instance argument" );
    
    local $EditTester::TEST_MODE = 1;
    
    ok_eval( sub { die "test exception" }, "exception result" );
    
    ok( ! defined last_result, "last_result produces undef after exception" );
    
    ok_eval( sub { 0 }, "false result" );
    
    is( last_result, '0', "last_result produces proper value for '0'");
    
    ok_eval( sub { }, "nil result" );
    
    ok( ! defined last_result, "last_result produces undef after empty subroutine" );
};


subtest 'ok_eval bad arguments' => sub {
    
    eval {
	ok_eval( "no subroutine" );
    };
    
    ok( $@ =~ /subroutine reference/i, "no subroutine" );
    
    my $B = { }; bless $B, 'XYZ';
    
    eval {
	ok_eval( $B, sub { 4-5 }, "bad method call" );
    };
    
    ok( $@ =~ /subroutine reference/i, "bad method call" );
};


subtest 'check ok_exception' => sub {
    
    ok_exception( sub { die "test exception xyxxy" }, qr/xyxxy/, "matching exception" );
    
    ok_exception( sub { die "foobar" }, qr/foobar/ );
    
    ok_exception( sub { die "biffbaff" }, 1, "any exception" );
    
    my $T = { }; bless $T, 'EditTester';
    
    $T->ok_exception( sub { die "foobar" }, qr/foobar/, "called as a method" );
    
    ok_exception($T, sub { die "foobar" }, qr/foobar/, "called with an instance argument" );
    
    local $EditTester::TEST_MODE = 1;
    
    ok_exception( sub { die "test exception xyxxy" }, qr/foobar/, "non matching exception" );
    
    ok( ! defined last_result, "last_result undefined after exception" );
    
    ok_exception( sub { 1 }, qr/foobar/, "no exception at all" );
    
    is( last_result, 1, "last_result returns subroutine value if no exception" );
    
    ok_exception( sub { 0 }, qr/foobar/, "no exception, false result" );
    
    is( last_result, 0, "last_result returns 0 if no exception" );
    
    ok_exception( sub { }, qr/foobar/, "no exception, nil result" );
    
    ok( ! defined last_result, "last_result undefined with empty subroutine" );
};


subtest 'ok_exception bad arguments' => sub {
    
    eval {
	ok_exception( "no subroutine" );
    };
    
    ok( $@ =~ /subroutine reference/i, "no subroutine" );
    
    eval {
	ok_exception( sub { die "foobar" }, "no regexp" );
    };
    
    ok( $@ =~ /regexp reference/i, "no regexp" );
    
    my $B = { }; bless $B, 'XYZ';
    
    eval {
	ok_exception( $B, sub { die "foobar" }, qr/foobar/, "bad method call" );
    };
    
    ok( $@ =~ /subroutine reference/i, "bad method call" );
};


subtest 'check connect_to_database' => sub {
    
    my $dbh = connect_to_database();
    
    like( ref $dbh, qr/DBI::/, "connect_to_database returns a DBI database handle" ) ||
	BAIL_OUT "unable to connect to database";
};


subtest 'constructor' => sub {
    
    ok_eval( sub { EditTester->new('ETBasicTest', 'EDT_TEST') },
	     "new EditTester" ) || BAIL_OUT "new EditTester failed";
    
    my $T = last_result;
    
    is( $T->{edt_class}, 'ETBasicTest', "class assigned correctly" );
    is( $T->{edt_table}, 'EDT_TEST', "table assigned correctly" );
    like( ref $T->dbh, qr/DBI::/, "dbh method returns a DBI database handle" );
    
    my $x = bless { }, 'XYZ';
    
    ok_eval( sub { EditTester->new({ class => 'ETBasicTest', table => 'EDT_TEST', 
				     debug_mode => 1, errlog_mode => 1, dbh => $x }) },
	     "new EditTester with parameter hash" ) || 
		 BAIL_OUT "new EditTester with parameter hash failed";
    
    $T = last_result;
    
    is( $T->{edt_class}, 'ETBasicTest', "class assigned correctly" );
    is( $T->{edt_table}, 'EDT_TEST', "table assigned correctly" );
    is( $T->{debug_mode}, 1, "debug_mode assigned correctly" );
    is( $T->{errlog_mode}, 1, "debug_mode assigned correctly" );
    is( $T->dbh, $x, "dbh assigned correctly" );
    
    $T->set_table('EDT_AUX');
    
    is( $T->{edt_table}, 'EDT_AUX', "set_table method" );
    
    $T = EditTester->new('ETBasicTest');
    
    ok( ! $T->{edt_table}, "no table assigned" );
};


subtest 'constructor bad arguments' => sub {
    
    ok_exception( sub { EditTester->new('NOT_A_CLASS_XXX') }, qr/NOT_A_CLASS_XXX.*[@]INC/,
		  "unknown class exception" );
    
    ok_exception( sub { EditTester->new('Test::More') }, qr/EditTransaction/,
		  "not subclass exception" );
};

