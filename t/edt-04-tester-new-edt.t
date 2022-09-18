#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction
# whose purpose is to implement these tests.
# 
# tester-new-edt : Test the creation of transaction instances by the 'new_edt'
#                  method of EditTester.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 3;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception last_result);


$DB::single = 1;

my $T = EditTester->new('ETBasicTest');


# Test creation of transaction instances

subtest 'new_edt' => sub {
    
    ok_eval( sub { $T->new_edt() }, "created new edt" ) || BAIL_OUT "new_edt failed";
    
    my $edt = last_result;
    
    is( ref $edt, 'ETBasicTest', "new_edt produces instance of default class" );
    is( $edt->dbh, $T->dbh, "new_edt set proper database handle" );
    is( $edt->default_table, '', "new_edt sets no default table" );
    ok( $edt->allows('CREATE'), "new_edt sets CREATE allowance by default" );
    
    is( $T->last_edt, $edt, "last_edt returns the newly created instance" );
};


# Test the various combinations of arguments.

subtest 'new_edt arguments' => sub {
    
    ok_eval( sub { $T->new_edt(class => 'ETTrivialClass', table => 'EDT_TYPES', 
			       permission => 'anything', 'NO_CREATE', 'LOCKED') },
	     "new edt with alternate class and other arguments" );
    
    if ( my $edt = last_result )
    {
	is( ref $edt, 'ETTrivialClass', "new_edt produces instance of specified class" );
	is( $edt->dbh, $T->dbh, "new_edt sets proper database handle" );
	is( $edt->default_table, 'EDT_TYPES', "new_edt sets proper default table" );
	ok( $edt->allows('LOCKED'), "new_edt sets LOCKED" );
	ok( ! $edt->allows('CREATE'), "new_edt with NO_CREATE does not set CREATE" );
	ok( $edt->can_proceed, "new_edt returns a working edt with no errors" );
    }
    
    ok_eval( sub { $T->new_edt({ class => 'ETTrivialClass', table => 'EDT_TYPES',
				 CREATE => 0, PROCEED => 1, BAD_FIELDS => 1, 
				 NOT_AN_ALLOWANCE => 1 }) },
	     "new edt with parameter hash" );
    
    if ( my $edt = last_result )
    {
	is( ref $edt, 'ETTrivialClass', "new_edt produces instance of specified class" );
	is( $edt->default_table, 'EDT_TYPES', "new_edt sets proper default table" );
	ok( $edt->allows('PROCEED'), "new_edt sets PROCEED" );
	ok( $edt->allows('BAD_FIELDS'), "new_edt sets BAD_FIELDS" );
	ok( ! $edt->allows('CREATE'), "new_edt with NO_CREATE does not set CREATE" );
	ok( $edt->has_condition('W_BAD_ALLOWANCE'), "bad allowance warning" );
	is( $edt->warnings, 1, "no other warnings" );
	ok( $edt->can_proceed, "no errors" );
    }
    
    ok_eval( sub { $T->new_edt({ class => 'ETBasicTest' }, ", LOCKED,, PROCEED , NO_BAD_FIELDS") },
	     "new edt with parameter hash and allowance string" );
    
    if ( my $edt = last_result )
    {
	is( ref $edt, 'ETBasicTest', "new_edt produces instance of default class" );
	is( $edt->default_table, '', "no default table" );
	ok( $edt->allows('LOCKED'), "new_edt sets LOCKED" );
	ok( $edt->allows('PROCEED'), "new_edt sets PROCEED" );
	ok( ! $edt->allows('BAD_FIELDS'), "new_edt clears BAD_FIELDS" );
	is( $edt->warnings, 0, "no warnings" );
	ok( $edt->can_proceed, "no errors" );
    }
    
    ok_eval( sub { $T->new_edt({ table => 'EDT_TEST', PROCEED => 1, LOCKED => 0 },
			       'NO_CREATE', 'NO_PROCEED', 'LOCKED' ) },
	     "new edt with default class" ) || BAIL_OUT "new_edt with parameters failed";
    
    if ( my $edt = $T->last_edt )
    {
	is( ref $edt, 'ETBasicTest', "new_edt produces instance of default class" );
	is( $edt->default_table, 'EDT_TEST', "new_edt sets default table" );
	ok( $edt->allows('LOCKED'), "new_edt sets LOCKED" );
	ok( ! $edt->allows('CREATE'), "new_edt clears CREATE" );
	is( $edt->warnings, 0, "no warnings" );
	ok( $edt->can_proceed, "no errors" );
    }    
};


subtest 'new_edt bad arguments' => sub {
    
    ok_exception( sub { $T->new_edt(table => { }) }, qr/invalid table/,
		  "exception when table name is not a string" );
    
    ok_exception( sub { $T->new_edt(class => { }) }, qr/invalid class/,
		  "exception when class name is not a string" );
    
    ok_exception( sub { $T->new_edt(class => 'NOT_A_CLASS') }, qr/invalid class/,
		  "exception when class was not loaded" );
    
    ok_exception( sub { $T->new_edt(class => 'Test::More') }, qr/invalid class/,
		  "exception when class is not a subclass of EditTransaction" );
    
    ok_eval( sub { $T->new_edt(table => 'XYZ') }, "bad table name" );
    
    ok( $T->has_condition('E_BAD_TABLE'), "bad table name generated an error" );
};

