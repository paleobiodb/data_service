#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-01-basic.t : Test that an EditTransaction object can be created.
#



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use ConsoleLog qw(initMessages logMessage logTimestamp);

use TableDefs qw(init_table_names select_test_tables $EDT_TEST);

use EditTest;
use EditTester;


# Initialize the console message system, in case we generate any debugging output.

initMessages(2, 'EditTransaction');


# The following calls establish a connection to the database, then create or re-create the
# necessary tables.

my $T = EditTester->new;

$T->create_tables;


# Test creation of both Permissions objects and an EditTest object. The Permissions objects are
# made available to subsequent tests. If we cannot create EditTest objects without an error
# occurring, we bail out. There is no point in running any more tests.

my ($perm_a, $perm_e, $perm_g);

subtest 'create objects' => sub {

    eval {
	$perm_a = $T->new_perm('SESSION-AUTHORIZER');
	$perm_e = $T->new_perm('SESSION-ENTERER');
	$perm_g = $T->new_perm('SESSION-GUEST');
    };

    unless ( ok( !$@, "permissions established" ) )
    {
	diag("message was: $@");
	BAIL_OUT;
    }
    
    $edt = $T->new_edt_nocreate($perm_a);

    ok( $edt, "created EditTest object" ) || BAIL_OUT;
    
    $edt2 = $T->new_edt($perm_a);
    
    ok( $edt2, "created EditTest object with CREATE") || BAIL_OUT;
};


# Test inserting and deleting objects. Once again, if we cannot do this without error then we bail
# out.

subtest 'insert and delete' => sub {

    my $result1 = $T->do_insert_records($perm_a, undef, "insert record",
					$EDT_TEST, { signed_req => 123, string_req => 'abc' })
	|| BAIL_OUT;
    
    my @inserted = $T->inserted_keys;
    
    unless ( cmp_ok( @inserted, '==', 1, "inserted one record" ) )
    {
	$T->diag_errors;
	$T->diag_warnings;
	BAIL_OUT;
    }
    
    my $result2 = $T->do_delete_records($perm_a, undef, "delete record",
					$EDT_TEST, $inserted[0])
	|| BAIL_OUT;
    
    my @deleted = $T->deleted_keys;
    
    cmp_ok( @deleted, '==', 1, "deleted one record" ) &&
	cmp_ok( $deleted[0], 'eq', $inserted[0], "deleted same record that was inserted" );
};


subtest 'update and replace' => sub {

    my $edt = $T->new_edt($perm_a);
    $edt->


};


subtest 'foo' => sub {

    pass("placeholder");
};


subtest 'bar' => sub {

    pass("placeholder");
};
