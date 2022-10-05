#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
# 
# keys.t : Test that key values in action parameters are parsed properly and can
#          be queried.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 10;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception ok_new_edt 
		  ok_no_conditions ok_has_condition ok_has_one_condition);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest');


# Check that the necessary methods are defined.

subtest 'required methods' => sub {
    
    can_ok( 'ETBasicTest', 'action_keyval', 'action_keyvalues', 'action_keymult' ) ||
	
	BAIL_OUT "EditTransaction and related modules are missing some required methods";
	
};


# Test key values with 'insert'.

subtest 'insert key values' => sub {
    
    my $edt = ok_new_edt( table => 'EDT_TEST' );
    
    if ( ok_eval( sub { $edt->_test_action('insert', { string_req => 'something' }); },
		  "_test_action with insert and no key value" ) )
    {
	is( $edt->action_keyval, '', "action_keyval empty" );
    }
    
    if ( ok_eval( sub { $edt->_test_action('insert', { string_req => 'else', test_no => '52' }); },
		  "_test_action with insert and one key value" ) )
    {
	is( $edt->action_keyval, '52', "action_keyval not empty" );
    }
    
    if ( ok_eval( sub { $edt->_test_action('insert', { string_req => 'or', _primary => '5' }); },
		  "_test_action with insert and _primary key value" ) )
    {
	is( $edt->action_keyval, '5', "action_keyval not empty with _primary" );
    }
};


# Test key values with 'delete'. This operation accepts different ways of specifying key values.

subtest 'delete key values'  => sub {
    
    my $edt = ok_new_edt( table => 'EDT_TEST' );
    
    if ( ok_eval( sub { $edt->_test_action('delete', "18"); },
		  "_test_action with delete and a single key value" ) )
    {
	is( $edt->action_keyval, "18", "action_keyval returns specified key value" );
	
	my @kv = $edt->action_keyvalues;
	
	is( @kv, 1, "action_keyvalues returns one key value" );
	is( $kv[0], "18", "action_keyvalues returns specified key value" );
    }
    
    if ( ok_eval( sub { $edt->_test_action('delete', [3, 4, 5]); },
		  "_test_action with delete and list of key values" ) )
    {
	my $kv = $edt->action_keyval;
	
	is( ref $kv, 'ARRAY', "action_keyval returns a listref" ) &&
	    is( @$kv, 3, "action_keyval returns three values" ) &&
	    is( $kv->[2], 5, "action_keyval entry 2 is '5'" );
	
	my @kv = $edt->action_keyvalues;
	
	is( @kv, 3, "action_keyvalues returns three values" );
	is( $kv[2], 5, "last key value is correct" );
	
	ok( $edt->action_keymult, "action_keymult returns true for multiple keys" );
    }
    
    if ( ok_eval( sub { $edt->_test_action('delete', "8, 2, 12"); },
		  "_test_action with delete and comma-separated list" ) )
    {
	my $kv = $edt->action_keyval;
	
	is( ref $kv, 'ARRAY', "action_keyval returns a listref" ) &&
	    is( @$kv, 3, "action_keyval returns three values" ) &&
	    is( $kv->[2], 12, "action_keyval entry 2 is '12'" );
	
	my @kv = $edt->action_keyvalues;
	
	is( @kv, 3, "action_keyvalues returns three values" );
	is( $kv[2], 12, "last key value is correct" );
	
	ok( $edt->action_keymult, "action_keymult returns true for multiple keys" );
    }
    
    if ( ok_eval( sub { $edt->_test_action('delete', { _primary => "23521" }); },
		  "_test_action with delete and _primary" ) )
    {
	is( $edt->action_keyval, 23521, "action_keyval returns specified key value" );
    }
    
    if ( ok_eval( sub { $edt->_test_action('delete_cleanup', 'EDT_SUB', "18"); },
		  "_test_action with 'delete_cleanup'" ) )
    {
	can_ok( $edt, 'action_keyval' ) &&
	    is( $edt->action_keyval, "18", "action_keyval returns specified key value" );
    }
};


# Test key values with 'update', 'replace', and 'other'.

subtest 'update replace other key values' => sub {
    
    my $edt = ok_new_edt( table => 'EDT_TEST' );
    
    if ( ok_eval( sub { $edt->_test_action('update', { test_no => 74, abc => 1}); }, 
		  "_test_action executed successfully with 'update'" ) )
    {
	is( $edt->action_keyval, "74", "action_keyval returns specified key value" );
	ok( ! $edt->action_keymult, "action_keymult returns false for a single key" );
    }
    
    if ( ok_eval( sub { $edt->_test_action('update', { _primary => [6, 2, 5], abc => 1}); }, 
		  "_test_action executed successfully with 'update'" ) )
    {
	is( ref $edt->action_keyval, "ARRAY", "action_keyval arrayref" );
	ok( $edt->action_keymult, "action_keymult true" );
	
	my @kv = $edt->action_keyvalues;
	
	is( @kv, 3, "action_keyvalues list of 3" );
	is( $kv[2], 5, "action_keyvalues expected value" );
    }
    
    if ( ok_eval( sub { $edt->_test_action('replace', { test_no => 74, abc => 1}); }, 
		  "_test_action executed successfully with 'update'" ) )
    {
	is( $edt->action_keyval, "74", "action_keyval expected value" );
	ok( ! $edt->action_keymult, "action_keymult false" );
    }    
    
    if ( ok_eval( sub { $edt->_test_action('other', { abc => 1, _primary => 5 }); }, 
		  "_test_action executed successfully with 'other'" ) )
    {
	is( $edt->action_keyval, 5, "action_keyval expected value" );
    }
};


# Test other ways of specifying key values.

subtest 'key values' => sub {
    
    my $edt = ok_new_edt( table => 'EDT_TEST' );
    
    $edt->_test_action( 'replace', { _primary => "15", abc => "def" } );
    
    ok_no_conditions;
    
    my @keyvalues = $edt->action_keyvalues('&#1');
    
    is( @keyvalues, 1, "single key count" );
    is( $keyvalues[0], "15", "single key value" );
    
    is( $edt->action_keyval('&#1'), "15", "single key keyval" );
    is( $edt->action_keymult('&#1'), '', "single key keymult" );
    
    $edt->_test_action('update', { _primary => "18,2" });
    
    ok_no_conditions;
    
    is( ref $edt->action_keyval, 'ARRAY', "comma-separated key values" );
    
    @keyvalues = $edt->action_keyvalues;
    
    is( @keyvalues, 2, "two key values" );
    is( $keyvalues[0], "18", "first key value" );
    is( $keyvalues[1], "2", "second key value" );
    
    is( $edt->action_keymult, 1, "comma-separated keymult" );
    
    my $keyval = $edt->action_keyval;
    
    if ( is( ref $keyval, 'ARRAY', "comma-separated keyval" ) )
    {
	is( $keyval->[0], $keyvalues[0], "keyval first element" );
	is( $keyval->[1], $keyvalues[1], "keyval second element" );
    }
    
    $edt->_test_action('update', { _primary => [30, '45', 0, 8] });
    
    ok_no_conditions;
    
    is( ref $edt->action_keyval, 'ARRAY', "list of key values" ) &&
	is( $edt->action_keyval->[2], 8, "keyval third element" );
    
    @keyvalues = $edt->action_keyvalues;
    
    is( @keyvalues, 3, "three key values" );
    is( $keyvalues[0], 30, "first key value" );
    is( $keyvalues[1], 45, "second key value" );
    is( $keyvalues[2], 8, "third key value" );
    
    is( $edt->action_keymult, 1, "list keymult" );
    
    $edt->_test_action('delete', { _primary => [28] } );
    
    ok_no_conditions;
    
    is( $edt->action_keyval, "28", "solitaire list keyval" );
    is( $edt->action_keymult, '', "solitaire list keymult" );
    
    # is( $action->keyval, "18", "key recognized" );
    # is( ref $action->keyval, 'ARRAY', "key list recognized" ) &&
    # is( scalar $action->keyval->@*, '3', "key list has three elements" );
};


# Now check oddly formatted values.

subtest 'strange values' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    $edt->_test_action('replace', { _primary => " ,,0   ,  ,72,, ," });
    
    ok_no_conditions;
    
    is( $edt->action_keyval, "72", "one valid key value" );
    
    $edt->_test_action('delete', { _primary => ", '53' , \"26\"  ,, '', "});
    
    ok_no_conditions;
    
    is( $edt->action_keyvalues, 2, "two valid quoted key values" );
};


# Check that bad integer values are recognized.

subtest 'bad values' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    $edt->_test_action('update', { _primary => ["abc", "16"] });
    
    ok_has_one_condition('E_BAD_KEY');
    
    $edt->_test_action('delete', { _primary => " 84 , -5, 16 " });
    
    ok_has_one_condition('E_BAD_KEY');
};


# Check the key references are handled properly.

subtest 'key references' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    $edt->_test_action('update', { _primary => "16", ghi => "abc" });
    
    $edt->_test_action('insert', { foo => "baz" });
    
    $edt->_test_action('replace', { _primary => "&#1", ghi => "jkl" });
    
    ok_no_conditions;
    
    is( $edt->action_keyval, "16", "simple reference resolved" );
    
    $edt->_test_action('update', { _primary => "&#2", ghi => 0 });
    
    ok_no_conditions;
    
    my @keyvalues = $edt->action_keyvalues;
    
    is( $keyvalues[0], "&#2", "reference unresolved");
    is( $edt->action_ref->permission, "PENDING", "authorization postponed");
    
    $edt->_test_action('delete', { _primary => "&fazz" });
    
    ok_has_one_condition('E_BAD_REFERENCE');
    
    $edt->_test_action('update', { _primary => "&" });
    
    ok_has_one_condition('E_BAD_REFERENCE');
};


# Check the various field names that can be used. We have already checked
# _primary above. Also check that 'delete' will take a scalar key value or list.

subtest 'key fields' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    $edt->_test_action('update', { test_no => "16", ghi => "abc" });
    
    ok_no_conditions;
    
    is( $edt->action_keyval, "16", "test_no keyval" );
    
    $edt->_test_action('replace', { test_id => "22" });
    
    ok_no_conditions;
    
    is( $edt->action_keyval, "22", "test_id keyval" );
    
    $edt->_test_action('insert', { foo => "bar" });
    
    ok_no_conditions;
    
    is( $edt->action_keyval, '', "no key value" );
    
    $edt->_test_action('delete', "23, 15, 88");
    
    ok_no_conditions;
    
    is( $edt->action_keyvalues, 3, "three key values for delete" );
    
    $edt->_test_action('delete', [15]);
    
    ok_no_conditions;
    
    is( $edt->action_keyval, "15", "one key value for delete" );
};


# Check that multiple key fields are flagged as errors.

subtest 'multiple fields' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    $edt->_test_action('update', { test_no => "3", _primary => "3" });
    
    ok_has_one_condition( qr/E_EXECUTE.*_primary/, "caught test_no and _primary" );
    
    $edt->_test_action('update', { test_no => "4", test_id => [5, 6] });
    
    ok_has_one_condition( qr/E_EXECUTE.*test_id/, "caught test_no and test_id" );
    
    $edt->_test_action('update', { test_id => "66", _primary => "xxx" });
    
    ok_has_condition( qr/E_EXECUTE.*test_id/, "caught test_id and _primary" );
};
