#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# default-table.t : Test the methods and arguments for setting and querying default tables.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 2;

use ETBasicTest;
use EditTester qw(ok_eval ok_exception);


# Establish two EditTester instances: one with a default table, and one without.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest', 'EDT_TEST');
my $TN = EditTester->new('ETBasicTest');


# Check that setting and modifying the default table in an EditTester instance works properly.

subtest 'EditTester default table' => sub {

    is( $T->default_table, 'EDT_TEST', "default table produces correct result" );
    ok( ! $TN->default_table, "no default table produces correct result");
    
    $TN->set_table('EDT_TEST');
    is( $TN->default_table, 'EDT_TEST', "default table changed correctly");

    $T->set_table('EDT_TYPES');
    is( $T->default_table, 'EDT_TYPES', "default table changed correctly 2");
};


# Check that new ETBasicTest instances inherit the default table.

subtest 'EditTransaction default table' => sub {
    
    $T = EditTester->new('ETBasicTest', 'EDT_TEST');
    
    my $edt = $T->new_edt();
    
    is( $edt->default_table, 'EDT_TEST', "new edt inherits default table");
    
    $TN = EditTester->new('ETBasicTest');
    
    my $edtn = $TN->new_edt();
    
    ok( ! $edtn->default_table, "new edt inherits empty default table");
    
    $T->set_table('EDT_TYPES');
    
    $edt = $T->new_edt;
    
    is( $edt->default_table, 'EDT_TYPES', "new edt inherits changed table");
}


