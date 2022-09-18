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
use Test::More tests => 6;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest');


# Check that the necessary methods are defined.

subtest 'methods' => sub {
    
    ok( ETBasicTest->can('action_keyvalues'), "action_keyvalues" ) || 
	BAIL_OUT "missing method 'action_keyvalues'";
    
    ok( ETBasicTest->can('action_keyval'), "action_keyval" ) || 
	BAIL_OUT "missing method 'action_keyval'";
    
    ok( ETBasicTest->can('action_keymult'), "action_keymult" ) || 
	BAIL_OUT "missing method 'action_keymult'";
};


# Test various ways of specifying key values.

subtest 'key values' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    $edt->new_action('replace', { _primary => "15", abc => "def" });
    
    $T->ok_no_conditions('latest');
    
    my @keyvalues = $edt->action_keyvalues('&#1');
    
    is( @keyvalues, 1, "single key count" );
    is( $keyvalues[0], "15", "single key value" );
    
    is( $edt->action_keyval('&#1'), "15", "single key keyval" );
    is( $edt->action_keymult('&#1'), '', "single key keymult" );
    
    $edt->new_action('update', { _primary => "18,2" });
    
    $T->ok_no_conditions('latest');
    
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
    
    $edt->new_action('update', { _primary => [30, '45', 0, 8] });
    
    $T->ok_no_conditions('latest');
    
    is( ref $edt->action_keyval, 'ARRAY', "list of key values" ) &&
	is( $edt->action_keyval->[2], 8, "keyval third element" );
    
    @keyvalues = $edt->action_keyvalues;
    
    is( @keyvalues, 3, "three key values" );
    is( $keyvalues[0], 30, "first key value" );
    is( $keyvalues[1], 45, "second key value" );
    is( $keyvalues[2], 8, "third key value" );
    
    is( $edt->action_keymult, 1, "list keymult" );
    
    $edt->new_action('delete', { _primary => [28] } );
    
    $T->ok_no_conditions('latest');
    
    is( $edt->action_keyval, "28", "solitaire list keyval" );
    is( $edt->action_keymult, '', "solitaire list keymult" );
};


# Now check oddly formatted values.

subtest 'odd values' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    $edt->new_action('replace', { _primary => " ,,0   ,  ,72,, ," });
    
    $T->ok_no_conditions('latest');
    
    is( $edt->action_keyval, "72", "one valid key value" );
    
    $edt->new_action('delete', { _primary => ", '53' , \"26\"  ,, '', "});
    
    $T->ok_no_conditions('latest');
    
    is( $edt->action_keyvalues, 2, "two valid quoted key values" );
};


# Check that bad integer values are recognized.

subtest 'bad values' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    $edt->new_action('update', { _primary => ["abc", "16"] });
    
    $T->ok_has_one_condition('latest', 'E_BAD_KEY');
    
    $edt->new_action('delete', { _primary => " 84 , -5, 16 " });
    
    $T->ok_has_one_condition('latest', 'E_BAD_KEY');
};


# Check the key references are handled properly.

subtest 'key references' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    $edt->new_action('update', { _primary => "16", ghi => "abc" });
    
    $edt->new_action('insert', { foo => "baz" });
    
    $edt->new_action('replace', { _primary => "&#1", ghi => "jkl" });
    
    $T->ok_no_conditions;
    
    is( $edt->action_keyval, "16", "simple reference resolved" );
    
    $edt->new_action('update', { _primary => "&#2", ghi => 0 });
    
    $T->ok_no_conditions;
    
    my @keyvalues = $edt->action_keyvalues;
    
    is( $keyvalues[0], "&#2", "reference unresolved");
    is( $edt->action_ref->permission, "PENDING", "authorization postponed");
    
    $edt->new_action('delete', { _primary => "&fazz" });
    
    $T->ok_has_one_condition('latest', 'E_BAD_REFERENCE');
    
    $edt->new_action('update', { _primary => "&" });
    
    $T->ok_has_one_condition('latest', 'E_BAD_REFERENCE');
};


# Check the various field names that can be used. We have already checked
# _primary above. Also check that 'delete' will take a scalar key value or list.

subtest 'key fields' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    $edt->new_action('update', { test_no => "16", ghi => "abc" });
    
    $T->ok_no_conditions('latest');
    
    is( $edt->action_keyval, "16", "test_no keyval" );
    
    $edt->new_action('replace', { test_id => "22" });
    
    $T->ok_no_conditions('latest');
    
    is( $edt->action_keyval, "22", "test_id keyval" );
    
    $edt->new_action('insert', { foo => "bar" });
    
    $T->ok_no_conditions('latest');
    
    is( $edt->action_keyval, undef, "no key value" );
    
    $edt->new_action('delete', "23, 15, 88");
    
    $T->ok_no_conditions('latest');
    
    is( $edt->action_keyvalues, 3, "three key values for delete" );
    
    $edt->new_action('delete', [15]);
    
    $T->ok_no_conditions('latest');
    
    is( $edt->action_keyval, "15", "one key value for delete" );
};


# Check that multiple key fields are flagged as errors.

subtest 'multiple fields' => sub {
    
    my $edt = $T->new_edt( table => 'EDT_TEST' );
    
    $edt->new_action('update', { test_no => "3", _primary => "3" });
    
    $T->ok_has_one_condition( 'latest', qr/E_EXECUTE.*_primary/, "caught test_no and _primary" );
    
    $edt->new_action('update', { test_no => "4", test_id => [5, 6] });
    
    $T->ok_has_one_condition( 'latest', qr/E_EXECUTE.*test_id/, "caught test_no and test_id" );
    
    $edt->new_action('update', { test_id => "66", _primary => "xxx" });
    
    $T->ok_has_condition( 'latest', qr/E_EXECUTE.*test_id/, "caught test_id and _primary" );
};
