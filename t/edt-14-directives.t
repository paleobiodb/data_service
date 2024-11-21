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
use Test::More tests => 8;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception ok_new_edt);

use TableDefs qw(set_column_property);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest');


subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'handle_column', 'get_handling') ||
	
	BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


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

subtest 'class-level and transaction-level directives' => sub {
    
    can_ok('ETBasicTest', 'handle_column', 'get_handling') || return;
    
    # Check that we can set a class-level directive.
    
    ETBasicTest->handle_column('EDT_TEST', 'string_val', 'ignore');
    
    is( ETBasicTest->get_handling('EDT_TEST', 'string_val'), 'ignore',
	"class directive was set" );
    
    ok( ! ETTrivialClass->get_handling('EDT_TEST', 'string_val'),
	"different class has different directives" );
    
    my $edt = ok_new_edt;
    
    is( $edt->get_handling('EDT_TEST', 'string_val'), 'ignore', 
	"class directive was copied to transaction" );
    
    # When we change the class directive, an already created transaction doesn't
    # change. 
    
    ETBasicTest->handle_column('EDT_TEST', 'string_val', 'unquoted');
    
    is( $edt->get_handling('EDT_TEST', 'string_val'), 'ignore',
	"transaction directive didn't change" );
    
    # Check that we can set a transaction-level directive.
    
    $edt->handle_column('EDT_TEST', 'signed_val', 'pass');
    
    is( $edt->get_handling('EDT_TEST', 'signed_val'), 'pass', 
	"transaction directive was set" );
    
    is( $edt->get_handling('EDT_TEST', 'string_val'), 'ignore',
	"existing transaction directive is still there" );
    
    # Check that we can get all of the directives at once.
    
    my %coldir = $edt->get_handling('EDT_TEST', '*');
    
    is( $coldir{signed_val}, 'pass', "found directive for signed_val" );
    is( $coldir{string_val}, 'ignore', "found directive for string_val" );
    is( $edt->get_handling('EDT_TEST', '*'), 2, "scalar result reports two directives" );
    
    # Check that we can cancel a directive.
    
    $edt->handle_column('EDT_TEST', 'signed_val', 'none');
    
    is( $edt->get_handling('EDT_TEST', 'signed_val'), '', 
	"directive canceled with 'none'" );
    
    # Check that get_handling returns '' if no directive is found and undef for a
    # nonexistent column.
    
    is( $edt->get_handling('EDT_TYPES', 'text_val'), '',
	"get_handling returns '' for a column with no directive" );
    
    is( $edt->get_handling('EDT_TYPES', 'not_a_column'), undef,
	"get_handling returns undef for a nonexistent column" );
    
    # Check that these directives are set on a per-table and per-transaction basis.
    
    ok( ! $edt->get_handling('EDT_TYPES', 'string_val'),
	"different table has different directives");
    
    $edt = ok_new_edt;
    
    ok( ! $edt->get_handling('EDT_TEST', 'signed_val'),
	"new transaction does not get old directives" );
    
    # Create a new transaction and check that it has the current class
    # directives. 
    
    $edt = ok_new_edt;
    
    is( $edt->get_handling('EDT_TEST', 'string_val'), 'unquoted',
	"class directive was copied to new transaction" );
};
    

subtest 'class-level and table-level directives' => sub {
    
    can_ok('ETBasicTest', 'alter_column_property', 'get_table_handling',
	   'class_directives_list', 'table_directives_list') || return;
    
    ETBasicTest->alter_column_property('EDT_TYPES', blob_val => DIRECTIVE => 'unquoted');
    
    is( ETBasicTest->get_table_handling('EDT_TYPES', 'blob_val'), 'unquoted',
	"get_table_handling returns newly set directive" );
    
    my %tabledirs = ETBasicTest->table_directives_list('EDT_TYPES');
    
    is( $tabledirs{blob_val}, 'unquoted', "table_directives_list returns newly set directive" );
    
    my %classdirs = ETBasicTest->class_directives_list('EDT_TYPES');
    
    is( $classdirs{blob_val}, undef, "class_directives_list does not return directive set for table" );
    
    my $edt = ok_new_edt;
    
    is( $edt->get_handling('EDT_TYPES', 'blob_val'), 'unquoted',
	"found newly set table directive in new transaction" );
    
    can_ok('ETBasicTest', 'clear_table_cache') || return;
    
    ETBasicTest->clear_table_cache('EDT_SUB');
    
    set_column_property('EDT_SUB', name => DIRECTIVE => 'pass');
    
    is( $edt->get_table_handling('EDT_SUB', 'name'), 'pass',
	"newly loaded table picks up directive set by 'set_column_property'" );
};


subtest 'action directives' => sub {
    
    ETBasicTest->handle_column('EDT_TEST', 'string_val', 'unquoted');
    
    ETBasicTest->handle_column('EDT_TYPES', 'FIELD:foobar', 'ignore');
    
    my $edt = ok_new_edt;
    
    is( $edt->get_handling('EDT_TEST', '*'), 1, "one directive inherited from the class" );
    
    $edt->handle_column('EDT_TEST', 'string_req', 'pass');
    
    is( $edt->get_handling('EDT_TEST', '*'), 2, "additional directive has been added" );
    
    $edt->_test_action( 'insert', 'EDT_TEST', { string_val => 'abc' } );
    
    is( $edt->get_handling('&_', 'string_req'), 'pass', 
	"action_directive with '&_' returns transaction-level directive" );
    
    is( $edt->get_handling('&#1', 'string_req'), 'pass', 
	"action_directive with '&#1' returns transaction-level directive" );
    
    $edt->handle_column('&_', 'string_req', 'ignore');
    
    is( $edt->get_handling('&_', '*'), 2, "action directive override has been added" );
    
    my %directives = $edt->get_handling('&_', '*');
    
    is( %directives, 2, "two entries in total" );
    is( $directives{string_req}, 'ignore', "action directive overrides transaction directive" );
    is( $directives{string_val}, 'unquoted', "transaction directive is inherited by action" );
    
    $edt->handle_column('&_', 'test_no', 'pass');
    
    is( $edt->get_handling('&_', '*'), 3, "new action directive has been added" );
    
    is( $edt->get_handling('&_', 'string_req'), 'ignore', "get_handling returns 'ignore'" );
    
    is( $edt->get_handling('&_', 'string_val'), 'unquoted', "get_handling returns 'unquoted'" );
    
    $edt->_test_action( 'update', 'EDT_TEST', { string_val => 'abc'} );
    
    is( $edt->get_handling('&#2', 'string_req'), 'pass', "new action inherits transaction directive" );
    
    ok( $edt->handle_column('EDT_TEST', 'string_req', 'none'), "handle_column sets value of 'none'" );
    
    is( $edt->get_handling('&#2', 'string_req'), '', 
	"action inheritance vanishes after transaction directive removed" );
    
    ok( $edt->handle_column('&#2', 'signed_val', 'validate'), 
	"handle_column sets value with refstring other than current action" );
    
    is( $edt->get_handling('&#2', 'signed_val'), 'validate', 
	"get_handling returns 'validate' after that was explicitly set" );
    
    $edt->_test_action( 'insert', 'EDT_TYPES', { abc => 1 } );
    
    is( $edt->get_handling('&_', 'FIELD:foobar'), 'ignore', "ignore field directive is found" );
    
    $edt->handle_column('&_', 'FIELD:foobar', 'pass');
    
    is( $edt->get_handling('&_', 'FIELD:foobar'), 'pass', "field 'ignore field directive is canceled" );
    
    # test handle_column after validation
    
    can_ok('EditTransaction', 'action_ref') || return;
    can_ok('EditTransaction::Action', 'validation_complete') || return;
    
    if ( ok( $edt->action_ref('&#2'), "retrieved action reference" ) )
    {
	my $action = $edt->action_ref('&#2');
	
	$action->validation_complete;
	
	ok( ! $edt->handle_column('&#2', 'string_val', 'validate'),
	    "handle_column fails after validation is complete" );
    }    
};


subtest 'bad arguments for setting and retrieving' => sub {
    
    my $edt = ok_new_edt;
    
    ok_exception( sub { $edt->handle_column }, qr/specify/, 
		  "handle_column exception no arguments" );
    
    ok_exception( sub { $edt->handle_column('EDT_TEST', 'foobar') }, qr/specify/,
		  "handle_column exception two arguments" );
    
    ok_exception( sub { $edt->handle_column('NOT_A_TABLE_XXX', 'foobar', 'ignore'); },
		  qr/unknown table/i, "handle_column exception bad table name" );
    
    ok_exception( sub { $edt->handle_column('EDT_TEST', 'foobar', 'ignre'); },
		  qr/invalid directive/i, "handle_column exception bad directive name" );
    
    ok_exception( sub { $edt->get_handling }, qr/table specifier/,
		  "get_handling exception no arguments" );
    
    ok_exception( sub { $edt->get_handling(undef, 'test_no'); },
		  qr/table specifier/, "get_handling exception empty argument" );
    
    is( $edt->get_handling('NOT_A_TABLE_XXX', 'test_no'), undef,
	"get_handling with bad table name returns undef" );
    
    is( $edt->get_handling('EDT_TEST', 'test_no'), '', "column without a directive returns ''" );
    
    is( $edt->get_handling('EDT_TEST', 'foobar'), undef, "unknown column returns undef" );
    
    # is( $edt->get_handling('_', 'foobar'), undef, "no current action results undef" );
    
    my %directives = $edt->get_handling('&#2', '*');
    
    is( %directives, 0, "bad action reference results empty list" );
    
    is( $edt->get_handling('&_', 'test_no'), undef, "no current action results undef" );
};

