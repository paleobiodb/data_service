#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# directives.t : Test the methods for dealing with column-handling directives.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 6;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception ok_new_edt);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest');


# Check that the default set of directives is present.

subtest 'default directives' => sub {
    
    can_ok('ETBasicTest', 'is_valid_directive') || return;
    
    ok( ETBasicTest->is_valid_directive('validate'), "default directive 'validate' is present" );
    
    ok( ETBasicTest->is_valid_directive('pass'), "default directive 'pass' is present" );
    
    ok( ETBasicTest->is_valid_directive('unquoted'), "default directive 'pass' is present" );
    
    ok( ETBasicTest->is_valid_directive('copy'), "default directive 'copy' is present" );
    
    ok( ETBasicTest->is_valid_directive('ignore'), "default directive 'ignore' is present" );
    
    ok( ETBasicTest->is_valid_directive('none'), "default directive 'none' is present" );
};


# Check the registration of directives. Also check that the default set are present.

subtest 'registering directives' => sub {
    
    can_ok('ETBasicTest', 'register_directives', 'is_valid_directive') || return;
    
    ok( ! ETBasicTest->is_valid_directive('testxyz'), "baseline: no directive 'testxyz'" );
    
    ETBasicTest->register_directives('testxyz');
    
    ok( ETBasicTest->is_valid_directive('testxyz'), "directive 'testxyz' is now registered" );
    
    ok( ! EditTransaction->is_valid_directive('testxyz'), "base class does not have new directive" );
    
    if ( can_ok('ETTrivialClass', 'copy_from') )
    {
	ok( ! ETTrivialClass->is_valid_directive('testxyz'), "baseline: other class does not have 'testxyz'" );
	
	ETTrivialClass->copy_from('ETBasicTest');
	
	ok( ETTrivialClass->is_valid_directive('testxyz'), "directive 'testxyz' copied" );
    }
};


subtest 'bad arguments for registration and query' => sub {
    
    can_ok('ETBasicTest', 'register_directives', 'is_valid_directive') || return;
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { $edt->register_directives('foobar'); }, qr/class/i,
		  "exception when register_directives is called on an instance" );
    
    ok_exception( sub { ETBasicTest->register_directives('abc:def'); }, qr/valid directive/i,
		  "exception when an invalid directive name is supplied" );
    
    ok_eval( sub { ETBasicTest->register_directives('ABC_DEF--23B'); },
	     "directive names can contain word characters and hyphens" );
    
    ok_eval( sub { ! $edt->is_valid_directive('abc:def'); },
	     "is_valid_directive returns undef with an invalid name, no exception" );
};


# Check that we can set column directives and query them on both the class level
# and the transaction level.

subtest 'class and table directives' => sub {
    
    can_ok('ETBasicTest', 'handle_column', 'table_directive') || return;
    
    # Check that we can set a class-level directive.
    
    ETBasicTest->handle_column('EDT_TEST', 'string_val', 'ignore');
    
    is( ETBasicTest->table_directive('EDT_TEST', 'string_val'), 'ignore',
	"class directive was set" );
    
    ok( ! ETTrivialClass->table_directive('EDT_TEST', 'string_val'),
	"different class has different directives" );
    
    my $edt = ok_new_edt;
    
    is( $edt->table_directive('EDT_TEST', 'string_val'), 'ignore', 
	"class directive was copied to transaction" );
    
    # When we change the class directive, an already created transaction doesn't
    # change. 
    
    ETBasicTest->handle_column('EDT_TEST', 'string_val', 'unquoted');
    
    is( $edt->table_directive('EDT_TEST', 'string_val'), 'ignore',
	"transaction directive didn't change" );
    
    # Check that we can set a transaction-level directive.
    
    $edt->handle_column('EDT_TEST', 'signed_val', 'pass');
    
    is( $edt->table_directive('EDT_TEST', 'signed_val'), 'pass', 
	"transaction directive was set" );
    
    is( $edt->table_directive('EDT_TEST', 'string_val'), 'ignore',
	"existing transaction directive is still there" );
    
    # Check that these directives are set on a per-table basis.
    
    ok( ! $edt->table_directive('EDT_TYPES', 'string_val'),
	"different table has different directives");
    
    # Check that we can get all of the directives at once.
    
    my %coldir = $edt->table_directive('EDT_TEST', '*');
    
    is( $coldir{signed_val}, 'pass', "found directive for signed_val" );
    is( $coldir{string_val}, 'ignore', "found directive for string_val" );
    is( $edt->table_directive('EDT_TEST', '*'), 2, "scalar result reports two directives" );
    
    # Create a new transaction and check that it has the class directive but not
    # the one set for the previous transaction.
    
    $edt = ok_new_edt;
    
    is( $edt->table_directive('EDT_TEST', 'string_val'), 'unquoted',
	"class directive was copied to new transaction" );
    
    ok( ! $edt->table_directive('EDT_TEST', 'signed_val'),
	"directive from previous edt was not copied to new one" );
};


subtest 'action directives' => sub {
    
    can_ok('ETBasicTest', 'handle_column', 'action_directive') || return;
    
    ETBasicTest->handle_column('EDT_TEST', 'string_val', 'unquoted');
    
    ETBasicTest->handle_column('EDT_TYPES', 'FIELD:foobar', 'ignore');
    
    my $edt = ok_new_edt;
    
    is( $edt->table_directive('EDT_TEST', '*'), 1, "one directive inherited from the class" );
    
    $edt->handle_column('EDT_TEST', 'string_req', 'pass');
    
    is( $edt->table_directive('EDT_TEST', '*'), 2, "additional directive has been added" );
    
    $edt->_test_action( 'insert', 'EDT_TEST', { string_val => 'abc' } );
    
    is( $edt->action_directive('string_req'), 'pass', "action_directive returns 'pass'" );
    
    is( $edt->action_directive('&#1', 'string_req'), 'pass', "action_directive with refstring" );
    
    $edt->handle_column('_', 'string_req', 'ignore');
    
    is( $edt->table_directive('_', '*'), 3, "action directive has been added" );
    
    my %directives = $edt->table_directive('_', '*');
    
    is( %directives, 2, "two entries in total" );
    is( $directives{string_req}, 'ignore', "string_req => ignore" );
    is( $directives{string_val}, 'unquoted', "string_val => unquoted" );
    
    is( $edt->action_directive('string_req'), 'ignore', "action_directive returns 'ignore'" );
    
    is( $edt->action_directive('string_val'), 'unquoted', "action_directive returns 'unquoted'" );
    
    $edt->_test_action( 'update', 'EDT_TEST', { string_val => 'abc'} );
    
    is( $edt->action_directive('string_req'), 'pass', "action_directive returns 'pass'" );
    
    $edt->handle_column('EDT_TEST', 'string_req', 'none');
    
    is( $edt->table_directive('_', 'string_req'), '', "table_directive returns ''" );
    
    is( $edt->action_directive('string_req'), 'validate', "action_directive returns 'validate'" );
    
    is( $edt->action_directive('signed_val'), 'validate', "action_directive returns 'validate'" );
    
    $edt->_test_action( 'insert', 'EDT_TYPES', { abc => 1 } );
    
    is( $edt->action_directive('FIELD:foobar'), 'ignore', "field 'foobar' is ignored" );
    
    $edt->handle_column('_', 'FIELD:foobar', 'pass');
    
    is( $edt->action_directive('FIELD:foobar'), 'pass', "field 'foobar' is now passed" );
};


subtest 'bad arguments for setting and retrieving' => sub {
    
    can_ok('ETBasicTest', 'handle_column', 'table_directive', 'action_directive' ) || return;
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { $edt->handle_column }, qr/specify/, 
		  "handle_column exception no arguments" );
    
    ok_exception( sub { $edt->handle_column('EDT_TEST', 'foobar') }, qr/specify/,
		  "handle_column exception two arguments" );
    
    ok_exception( sub { $edt->handle_column('NOT_A_TABLE_XXX', 'foobar', 'ignore'); },
		  qr/unknown table/i, "handle_column exception bad table name" );
    
    ok_exception( sub { $edt->handle_column('EDT_TEST', 'foobar', 'ignre'); },
		  qr/invalid directive/i, "handle_column exception bad directive name" );
    
    ok_exception( sub { $edt->table_directive }, qr/specify/,
		  "table_directive exception no arguments" );
    
    ok_exception( sub { $edt->table_directive(undef, 'test_no'); },
		  qr/specify/, "table_directive exception empty argument" );
    
    ok_exception( sub { $edt->table_directive('NOT_A_TABLE_XXX', 'test_no'); },
		qr/unknown table/i, "table_directive exception bad table name" );
    
    
    is( $edt->table_directive('EDT_TEST', 'test_no'), '', "column without a directive returns ''" );
    
    is( $edt->table_directive('EDT_TEST', 'foobar'), undef, "unknown column returns undef" );
    
    is( $edt->table_directive('_', 'foobar'), undef, "no current action results undef" );
    
    my %directives = $edt->table_directive('&#2', '*');
    
    is( %directives, 0, "bad action reference results empty list" );
    
    is( $edt->action_directive('test_no'), undef, "no current action results undef" );
};

