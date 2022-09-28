#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# allowances.t : Test the methods and arguments for registering, setting and querying allowances.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 3;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest');



# Check registration of new allowances.

subtest 'registration' => sub {
    
    ok( ETBasicTest->has_allowance('TEST_DEBUG'), "has class default allowance" );
    ok( ETBasicTest->has_allowance('CREATE'), "has base default allowance");
    ok( ! ETBasicTest->has_allowance('NOT_AN_ALLOWANCE'), "lacks nonexistent allowance" );
    
    ETBasicTest->register_allowances('A-1', 'A-2');
    
    ok( ETBasicTest->has_allowance('A-1'), "has first registered allowance" );
    ok( ETBasicTest->has_allowance('A-2'), "has second registered allowance" );
    
    ok( ! ETTrivialClass->has_allowance('A2'), "different class lacks registered allowance" );
    ok( ! EditTransaction->has_allowance('A2'), "base class lacks registered allowance");
    
    my $edt = ETBasicTest->new($T->dbh, { allows => 'TEST_DEBUG' });
    
    ok( ! $edt->has_condition('W_BAD_ALLOWANCE'), "edt accepts default allowance" );
    
    ok( $edt->allows('TEST_DEBUG'), "edt has default allowance" );
    
    $edt = ETBasicTest->new($T->dbh, { allows => 'A-1,A-2' });
    
    ok( ! $edt->has_condition('W_BAD_ALLOWANCE'), "edt accepts registered allowances" );
    
    ok( $edt->allows('A-1'), "edt has registered allowance 1");
    ok( $edt->allows('A-2'), "edt has registered allowance 2");
    
    $edt = ETTrivialClass->new($T->dbh, { allows => 'A-1'});
    ok( $edt->has_condition('W_BAD_ALLOWANCE'), "different class does not accept registered allowance");
};


# Check that invalid allowance names are rejected.

subtest 'registration bad input' => sub {

    ok_exception( sub { ETBasicTest->register_allowances('BAD:ALLOWANCE') }, 1,
		  "invalid character in allowance name" );
    
    ok_exception( sub { ETBasicTest->register_allowances('bad_allowance') }, 1,
		  "allowances must be upper case" );
};


# Check the 'copy_from' method.

subtest 'copy allowances' => sub {
    
    ETBasicTest->register_allowances('A-3');
    
    ETTrivialClass->copy_from('ETBasicTest');
    
    ETBasicTest->register_allowances('NEW_ALLOWANCE');
    
    ok( ETTrivialClass->has_allowance('TEST_DEBUG'), "default allowance was copied" );
    ok( ETTrivialClass->has_allowance('A-3'), "registered allowance was copied" );
    ok( ! ETTrivialClass->has_allowance('NEW_ALLOWANCE'), "post-registered allowance was not copied" );
};

