#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of
# EditTransaction whose purpose is to implement these tests.
# 
# basic.t : Test that the class loads properly, and that EditTransaction objects
#           can be created.
# 
# This test file should be run before any of the others, the first time that
# testing is done. Its first task is to establish the database tables needed for
# the rest of the tests. After that, the tests can all be run independently and
# in any order.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 3;


use ETBasicTest;
use EditTester;


# Test object creation for classes EditTester and ETBasicTest.

$DB::single = 1;

my $T;

subtest 'initialize tester' => sub {
    
    $T = EditTester->new('ETBasicTest');
    
    ok( $T, "created EditTester instance" );
};


subtest 'establish test tables' => sub {
    
    $T->establish_test_tables;
    
    ok( 1, "test tables established" );
};


subtest 'create ETBasicTest instance' => sub {
    
    my $edt = ETBasicTest->new($T->dbh);
    
    ok( $edt, "created object of class ETBasicTest" );
};


