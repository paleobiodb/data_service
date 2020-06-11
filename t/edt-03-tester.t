#
# PBDB Data Service
# -----------------
# 
# This file contains unit tests for the EditTransaction class.
# 
# edt-03-tester.t : Test the class EditTester, which is used in all of the other tests. We need to
# make sure that it is actually running the tests properly and not silently flubbing any of htem.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 9;

use TableDefs qw(%TABLE get_table_property set_table_property set_column_property);

use TableData qw(reset_cached_column_properties);

use EditTest;
use EditTester;

use Carp qw(croak);


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new({ subclass => 'EditTest' });

my ($perm_a);

# Test that we can create EditTester objects, and check the basic accessor methods of the class.

subtest 'basic' => sub {
    
    ok( ref $T && $T->isa('EditTester'), "created EditTester object" ) || BAIL_OUT;
    
    my $dbh = $T->dbh;
    
    ok( ref $dbh && ref($dbh) =~ /DBI/, "got proper dbh" ) || BAIL_OUT;
    
    my $debug = $T->debug;	# this will cause the script to fail if the method isn't implemented
};


# Test that we can set and clear specific permissions.

subtest 'permissions' => sub {
    
    my $dbh = $T->dbh;
    
    $T->clear_specific_permissions;
    
    my ($count) = $dbh->selectrow_array("SELECT count(*) FROM $TABLE{TABLE_PERMS}");
    
    is( $count, 0, "specific permissions table was cleared" );

    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( ref $perm_a && $perm_a->isa('Permissions'), "created permission object" );
    
    $T->set_specific_permission('EDT_TEST', $perm_a, 'admin');
    
    ($count) = $dbh->selectrow_array("SELECT count(*) FROM $TABLE{TABLE_PERMS}");
    
    is( $count, 1, "inserted one permission row" );
    
    my $check_table = 'EDT_TEST'; $check_table =~ s/^.*[.]//;
    
    my ($person_no, $table_name, $permission) = $dbh->selectrow_array(
		"SELECT person_no, table_name, permission FROM $TABLE{TABLE_PERMS} LIMIT 1");
    
    is( $person_no, $perm_a->enterer_no, "permission person number matches" );
    is( $table_name, $check_table, "permission table name matches" );
    is( $permission, 'admin', "permission string matches" );

    $T->clear_specific_permissions;
};


# Test the creation and checking of EditTransaction objects.

subtest 'edts' => sub {

    my $edt1 = $T->new_edt($perm_a);

    my $edt2 = $T->new_edt($perm_a);

    is( $T->last_edt, $edt2, "last_edt returns last edt" );

    $T->clear_edt;

    is( $T->last_edt, undef, "last_edt returns undef after clear_edt" );
};


# Now test that the error and caution condition-checking methods work properly

subtest 'errors' => sub {

    # Create a new transaction and check that it has no errors.
    
    my $edt = $T->new_edt($perm_a);
    
    $T->ok_no_errors("ok_no_errors passes test for new object");
    $T->ok_no_errors('all', "ok_no_errors with 'all' passes test for new object");
    $T->ok_no_conditions("ok_no_conditions passes test for new object");
    
    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_has_error('E_EXECUTE', "ok_has_error properly fails test");
	$T->ok_has_error('all', 'E_EXECUTE', "ok_has_error with 'all' properly fails test");
    }
    
    # Insert a record and add a single error condition. Check that the methods for testing error
    # conditions work properly.
    
    $edt->insert_record('EDT_TEST', { string_req => 'abc' });
    
    $edt->add_condition('E_EXECUTE', "test condition");
    
    $T->ok_has_error('E_EXECUTE', "ok_has_error passes test");
    $T->ok_has_error('all', 'E_EXECUTE', "ok_has_error with 'all' passes test");
    $T->ok_has_error('latest', 'E_EXECUTE', "ok_has_error with 'latest' passes test");
    $T->ok_has_one_error('E_EXECUTE', "ok_has_one_error passes test");
    $T->ok_has_one_error('all', 'E_EXECUTE', "ok_has_one_error with 'all' passes test");
    $T->ok_has_one_error('latest', 'E_EXECUTE', "ok_has_one_error with 'latest' passes test");
    $T->ok_no_errors('main', "ok_no_errors with 'main' passes test");
    $T->ok_no_conditions('main', "ok_no_conditions with 'main' passes test");
    $T->ok_no_warnings("ok_no_warnings passes test");
    $T->ok_no_warnings('main', "ok_no_warnings with 'main' passes test");
    $T->ok_no_warnings('latest', "ok_no_warnings with 'latest' passes test");
    $T->ok_no_warnings('all', "ok_no_warnings with 'all' passes test");    
    
    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_no_errors("ok_no_errors properly fails test");
	$T->ok_no_conditions("ok_no_conditions properly fails test");
	$T->ok_no_conditions('all', "ok_no_conditions with 'all' properly fails test");
	$T->ok_no_conditions('latest', "ok_no_conditions with 'latest' properly fails test");
	$T->ok_has_error('main', 'E_EXECUTE', "ok_has_error with 'main' properly fails test");
	$T->ok_has_one_error('main', 'E_EXECUTE', "ok_has_one_error with 'main' properly fails test");
	$T->ok_has_warning('W_FOOBAR', "ok_has_warning properly fails test");
    }
    
    # Now add another record with no error conditions but a single warning. Check that the error
    # condition testing methods with 'latest' properly report no errors. Using 'all' should still
    # indicate that errors are present.
    
    $edt->insert_record('EDT_TEST', { string_req => 'def' });
    
    $edt->add_condition('W_ALLOW', 'foobar');
    
    $T->ok_no_errors('latest', "ok_no_errors passes test");
    $T->ok_no_errors('main', "ok_no_errors with 'main' passes test");
    $T->ok_no_conditions('main', "ok_no_conditions with 'main' passes test");
    $T->ok_has_warning('latest', 'W_ALLOW', "ok_has_warning passes test");
    
    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_no_errors('all', "ok_no_errors with 'all' properly fails test");
	$T->ok_no_conditions('all', "ok_no_conditions with 'all' properly fails test");
	$T->ok_no_conditions('latest', "ok_no_conditions with 'latest' properly fails test");
	$T->ok_has_error('latest', 'E_EXECUTE', "ok_no_errors with 'latest' properly fails test");
    }
    
    $T->ok_has_error('E_EXECUTE', "ok_has_error with no selector passes test");
    $T->ok_has_one_error('E_EXECUTE', "ok_has_one_error with no selector passes test");
    $T->ok_has_error('all', 'E_EXECUTE', "ok_has_error with 'all' passes test");
    $T->ok_has_one_error('all', 'E_EXECUTE', "ok_has_one_error with 'all' passes test");
    
    # Now check again with regexes.
    
    $T->ok_has_error(qr{E_EXECUTE.*test}, "ok_has_error with regex passes test");
    $T->ok_has_one_error(qr{E_EXECUTE.*test}, "ok_has_one_error with regex passes test");
    
    # Now add a third record, with two errors. This time, "has one error" should fail.
    
    $edt->insert_record('EDT_TEST', { string_req => 'ghi' });
    
    $edt->add_condition('E_PERM', 'foobar');
    $edt->add_condition('C_CREATE');
    
    $T->ok_has_error('E_PERM', "found E_PERM condition");
    $T->ok_has_error('C_CREATE', "found C_CREATE condition");
    $T->ok_has_error('latest', 'E_PERM', "found E_PERM condition with 'latest'");
    $T->ok_has_error('latest', 'C_CREATE', "found C_CREATE condition with 'latest'");
    
    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_no_errors("ok_no_errors properly fails");
	$T->ok_has_one_error('E_PERM', "ok_has_one_error properly fails");
	ok( $EditTester::TEST_DIAG =~ /E_PERM/, "found diagnostic for E_PERM" );
	ok( $EditTester::TEST_DIAG =~ /C_CREATE/, "found diagnostic for C_CREATE" );
	$T->ok_has_one_error('all', 'E_PERM', "ok_has_one_error with 'all' properly fails");
	$T->ok_has_one_error('latest', 'E_PERM', "ok_has_one_error with 'all' properly fails");
	$T->ok_has_one_error('main', 'E_PERM', "ok_has_one_error with 'all' properly fails");
	$T->ok_has_error('main', 'E_PERM', "ok_has_error with 'main' properly fails");
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_warnings;
	ok( $EditTester::TEST_DIAG =~ /W_ALLOW/, "found W_ALLOW from diag_errors" );
	$EditTester::TEST_DIAG = '';
	$T->diag_errors('main');
	ok( $EditTester::TEST_DIAG eq '', "diag_errors('main') did not generate any output");
	$T->diag_errors('latest');
	ok( $EditTester::TEST_DIAG =~ /E_PERM/, "found E_PERM from diag_errors ('latest')" );
	ok( $EditTester::TEST_DIAG !~ /E_EXECUTE/, "did not find E_EXECUTE from diag_errors('latest')");
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_errors('all');
	ok( $EditTester::TEST_DIAG =~ /E_EXECUTE/, "found E_EXECUTE from diag_errors('all')");
	ok( $EditTester::TEST_DIAG =~ /E_PERM/, "found E_PERM from diag_errors('all')" );
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_errors();
	ok( $EditTester::TEST_DIAG =~ /E_PERM/, "found E_PERM from diag_errors()" );
	ok( $EditTester::TEST_DIAG =~ /E_EXECUTE/, "found E_EXECUTE from diag_errors()");
    }
    
    # Now create a second edt. Add an error not associated with any action.
    
    my $edt2 = $T->new_edt($perm_a);

    $edt2->add_condition('C_CREATE');

    $T->ok_has_error('C_CREATE', "found C_CREATE condition");
    $T->ok_has_one_error('C_CREATE', "found C_CREATE condition as one error");
    $T->ok_has_error('main', 'C_CREATE', "found C_CREATE condition with 'main'");
    $T->ok_has_one_error('C_CREATE', "found C_CREATE condition as one error");

    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_no_errors("ok_no_errors properly fails");
	$T->ok_no_errors('main', "ok_no_errors properly fails with 'main'");
	$T->ok_no_errors('all', "ok_no_errors properly fails wtih 'all'");
	$T->ok_no_errors('latest', "ok_no_errors properly fails with 'latest'");
	$T->ok_no_conditions($edt2, 'main', "ok_no_conditions properly fails with edt2 and 'main'");
    }
    
    $edt2->insert_record('EDT_TEST', { string_req => 'ghi' });

    $edt2->add_condition('E_REQUIRED', 'foobar');
    
    $T->ok_has_one_error('main', 'C_CREATE', "only one error with 'main'");
    $T->ok_has_one_error('latest', 'E_REQUIRED', "only one error with 'latest'");
    $T->ok_has_error('all', 'C_CREATE', "found C_CREATE with 'all'");
    $T->ok_has_error('all', 'E_REQUIRED', "found E_REQUIRED with 'all'");
    
    # Check that we get the same error if we ask for F_REQUIRED.

    $T->ok_has_error('all', 'F_REQUIRED', "found F_REQUIRED with 'all'");
    
    # Check that we can still test errors from the previous transaction, since it hasn't yet been
    # destroyed.
    
    $T->ok_has_error($edt, 'E_PERM', "found E_PERM from first edt");
    $T->ok_has_error($edt, 'C_CREATE', "found C_CREATE from first edt");
    $T->ok_has_error($edt, 'all', 'E_EXECUTE', "found E_EXECUTE from first edt with 'all'");
    $T->ok_no_conditions($edt, 'main', "no conditions for first edt with 'main'");
    $T->ok_has_one_error($edt2, 'latest', 'E_REQUIRED', "found E_REQUIRED as one error from second edt");
    $T->ok_has_error($edt2, 'main', 'C_CREATE', "found C_CREATE from second edt");
    
    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_has_error($edt, 'main', 'C_CREATE', "properly did not find C_CREATE from first edt with 'main'");
	$T->ok_has_error($edt, 'all', 'E_REQUIRED', "properly did not find E_REQUIRED from first edt with 'all'");

	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_errors($edt2, 'latest');
	ok( $EditTester::TEST_DIAG =~ /E_REQUIRED/, "found E_REQUIRED from diag_errors(\$edt2)" );
	ok( $EditTester::TEST_DIAG !~ /C_CREATE/, "did not find C_CREATE from diag_errors(\$edt2)");
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_errors($edt);
	ok( $EditTester::TEST_DIAG =~ /E_PERM/, "found E_PERM from diag_errors(\$edt)" );
	ok( $EditTester::TEST_DIAG =~ /E_EXECUTE/, "found E_EXECUTE from diag_errors(\$edt)" );
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_errors($edt, 'all');
	ok( $EditTester::TEST_DIAG =~ /E_PERM/, "found E_PERM from diag_errors(\$edt, 'all')" );
	ok( $EditTester::TEST_DIAG =~ /E_EXECUTE/, "found E_EXECUTE from diag_errors(\$edt, 'all')" );
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_errors($edt2, 'main');
	ok( $EditTester::TEST_DIAG =~ /C_CREATE/, "found C_CREATE from diag_errors(\$edt2, 'main')" );
	ok( $EditTester::TEST_DIAG !~ /E_REQUIRED/, "did not find E_REQUIRED from diag_errors(\$edt2, 'main')" );
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->ok_result(0, 'latest', "ok_result properly failed with a false argument");
	ok( $EditTester::TEST_DIAG =~ /E_REQUIRED/, "found E_REQUIRED from ok_result" );	
	ok( $EditTester::TEST_DIAG !~ /C_CREATE/, "did not find C_CREATE from ok_result" );
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->ok_result($edt, 0, "ok_result edt properly failed with a false argument");
	ok( $EditTester::TEST_DIAG =~ /E_PERM/, "found E_PERM from ok_result edt" );	
	ok( $EditTester::TEST_DIAG =~ /E_EXECUTE/, "found E_EXECUTE from ok_result edt" );
    }
};


# Then test that the condition-checking methods work propery with errors that are demoted
# according to 'PROCEED' and 'NOT_FOUND'.

subtest 'demoted errors' => sub {

    # Create a new transaction and check that it has no errors.
    
    my $edt = $T->new_edt($perm_a, { PROCEED => 1 });
    
    $T->ok_no_errors("ok_no_errors passes test for new object");
    $T->ok_no_errors('all', "ok_no_errors with 'all' passes test for new object");
    $T->ok_no_errors('latest', "ok_no_errors with 'latest' passes test for new object");
    $T->ok_no_conditions("ok_no_conditions passes test for new object");
    
    # Now add a record with two errors.
    
    $edt->insert_record('EDT_TEST', { signed_val => 'abc' });
    
    # Check that these conditions are reported correctly, even though they have been demoted to
    # warnings by the PROCEED flag, and regardless of whether they are indicated with E_ or F_.
    
    $T->ok_has_error('E_REQUIRED', "ok_has_error found 'E_REQUIRED'");
    $T->ok_has_error('E_FORMAT', "ok_has_error found 'E_FORMAT'");
    $T->ok_has_error('F_REQUIRED', "ok_has_error found 'F_REQUIRED'");
    $T->ok_has_error('F_FORMAT', "ok_has_error found 'F_FORMAT'");
    $T->ok_has_error(qr{ ^ [EF]_FORMAT .* integer }xs, "ok_has_error found E_FORMAT with regex");

    # Now add a caution and check that we can find it with either C_ or D_.

    $edt->add_condition('C_CREATE');

    $T->ok_has_error('C_CREATE', "ok_has_error found 'C_CREATE'");
    $T->ok_has_error('D_CREATE', "ok_has_error found 'D_CREATE'");
    $T->ok_has_error(qr{ ^ [CD]_CREATE }xs, "ok_has_error found C_CREATE with regex");

    # Now make sure that F_ and D_ work without PROCEED.

    $edt = $T->new_edt($perm_a);

    $edt->add_condition('E_REQUIRED', 'foobar');
    $edt->add_condition('C_CREATE');

    $T->ok_has_error('F_REQUIRED', "ok_has_error found F_REQUIRED");
    $T->ok_has_error('D_CREATE', "ok_has_error found D_CREATE");
};


subtest 'warnings' => sub {

    # Create a new transaction and check that it has no warnings.
    
    my $edt = $T->new_edt($perm_a);
    
    $T->ok_no_warnings("ok_no_warnings passes test for new object");
    $T->ok_no_warnings('all', "ok_no_warnings with 'all' passes test for new object");
    $T->ok_no_warnings('main', "ok_no_warnings with 'main' passes test for new object");
    $T->ok_no_warnings('latest', "ok_no_warnings with 'latest' passes test for new object");
    $T->ok_no_conditions("ok_no_conditions passes test for new object");
    $T->ok_no_conditions('all', "ok_no_conditions with 'all'  passes test for new object");
    $T->ok_no_conditions('main', "ok_no_conditions with 'main' passes test for new object");
    $T->ok_no_conditions('latest', "ok_no_conditions with 'latest' passes test for new object");
    
    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_has_warning('W_TEST', "ok_has_error properly fails test");
	$T->ok_has_error('all', 'C_FOOBAR', "ok_has_error with 'all' properly fails test");
    }
    
    # Insert a record and add a single warning condition. Check that the methods for testing warning
    # conditions work properly.
    
    $edt->insert_record('EDT_TEST', { string_req => 'abc' });
    
    $edt->add_condition('W_TEST', "test condition");
    
    $T->ok_has_warning('W_TEST', "ok_has_warning passes test");
    $T->ok_has_warning('all', 'W_TEST', "ok_has_warning with 'all' passes test");
    $T->ok_has_warning('latest', 'W_TEST', "ok_has_warning with 'latest' passes test");
    $T->ok_no_warnings('main', "ok_no_warnings with 'main' passes test");
    $T->ok_no_conditions('main', "ok_no_conditions with 'main' passes test");
    $T->ok_has_one_warning('W_TEST', "ok_has_one_warning passes test");
    $T->ok_has_one_warning('all', 'W_TEST', "ok_has_one_warning with 'all' passes test");
    $T->ok_has_one_warning('latest', 'W_TEST', "ok_has_one_warning with 'latest' passes test");
    $T->ok_no_errors("ok_no_errors passes test");
    $T->ok_no_errors('main', "ok_no_errors with 'main' passes test");
    $T->ok_no_errors('latest', "ok_no_errors with 'latest' passes test");
    $T->ok_no_errors('all', "ok_no_errors with 'all' passes test");
    
    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_no_warnings("ok_no_warnings properly fails test");
	$T->ok_no_warnings('all', "ok_no_warnings with 'all' properly fails test");
	$T->ok_no_warnings('latest', "ok_no_warnings with 'latest' properly fails test");
	$T->ok_no_conditions("ok_no_conditions properly fails test");
	$T->ok_no_conditions('all', "ok_no_conditions with 'all' properly fails test");
	$T->ok_no_conditions('latest',"ok_no_conditions with 'latest' properly fails test");
	$T->ok_has_warning('main', 'W_TEST', "ok_has_warning with 'main' properly fails test");
	$T->ok_has_one_warning('main', 'W_TEST', "ok_has_one_warning with 'main' properly fails test");
    }
    
    # Now add another record with no warning conditions. Check that the warning condition testing
    # methods still work properly. In particular, the most recent action should show no warnings,
    # but using 'all' should still indicate that warnings are present.
    
    $edt->insert_record('EDT_TEST', { string_req => 'def' });
    
    $T->ok_no_warnings('main', "ok_no_warnings with 'main' passes test");
    $T->ok_no_conditions('main', "ok_no_conditions with 'main' passes test");
    $T->ok_no_warnings('latest', "ok_no_warnings with 'latest' passes test");
    $T->ok_no_conditions('latest', "ok_no_conditions with 'latest' passes test");
    $T->ok_has_warning('W_TEST', "ok_has_warning with no selector passes test");
    $T->ok_has_one_warning('W_TEST', "ok_has_one_warning with no selector passes test");
    
    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_no_warnings("ok_no_warnings properly fails test");
	$T->ok_no_conditions("ok_no_conditions properly fails test");
	$T->ok_no_warnings('all', "ok_no_warnings with 'all' properly fails test");
	$T->ok_no_conditions('all', "ok_no_conditions with 'all' properly fails test");
    }
    
    $T->ok_has_warning('all', 'W_TEST', "ok_has_warning with 'all' passes test");
    $T->ok_has_one_warning('all', 'W_TEST', "ok_has_one_warning with 'all' passes test");
    
    # Now add a second warning, and test the 'ok_has_one_warning' now fails with 'latest'.
    
    $edt->add_condition('W_EXECUTE', 'foobar');

    $T->ok_has_warning('latest', 'W_EXECUTE', "ok_has_warning passes test");
    $T->ok_has_one_warning('latest', 'W_EXECUTE', "ok_has_one_warning passes test");
    
    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_no_warnings('latest', "ok_no_warnings properly fails test");
	$T->ok_has_one_warning('latest', 'W_TEST', "did not find W_TEST without selector");
	$T->ok_has_one_warning('all', 'W_TEST', "ok_has_one_warning fails with 'all'");
	$T->ok_has_warning('latest', 'W_TEST', "ok_has_warning did not find W_TEST with 'latest'");
    }
    
    # Now check again with regexes.
    
    $T->ok_has_warning('all', qr{W_TEST.*test}, "ok_has_warning with regex passes test");
    $T->ok_has_one_warning('latest', qr{W_EXECUTE.*foobar}, "ok_has_one_warning with regex passes test");
    
    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_has_one_warning('latest', 'W_TEST', "ok_has_one_warning properly fails test");
	$T->ok_no_warnings("ok_no_warnings properly fails test");
	$T->ok_no_warnings('latest', "ok_no_warnings with 'latest' properly fails test");

	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_errors;
	ok( $EditTester::TEST_DIAG eq '', "diag_errors did not generate any output" );
	$T->diag_warnings('main');
	ok( $EditTester::TEST_DIAG eq '', "diag_warnings('main') did not generate any output");
	$T->diag_warnings;
	ok( $EditTester::TEST_DIAG =~ /W_EXECUTE/, "found W_EXECUTE from diag_warnings with no selector" );
	ok( $EditTester::TEST_DIAG =~ /W_TEST/, "found W_TEST from diag_warnings with no selector");
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_warnings('all');
	ok( $EditTester::TEST_DIAG =~ /W_EXECUTE/, "found W_EXECUTE from diag_warnings('all')");
	ok( $EditTester::TEST_DIAG =~ /W_TEST/, "found W_TEST from diag_warnings('all')" );
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_warnings('latest');
	ok( $EditTester::TEST_DIAG =~ /W_EXECUTE/, "found W_EXECUTE from diag_warnings('latest')" );
	ok( $EditTester::TEST_DIAG !~ /W_TEST/, "did not find W_TEST from diag_warnings('latest')");
    }
    
    # Now add a second warning to the current action, and check that ok_has_one_warning now fails.
    
    $edt->add_condition('W_TEST', 'second test');
    
    $T->ok_has_warning('latest', 'W_TEST', "ok_has_warning now found W_TEST with 'latest'");
    $T->ok_has_warning('all', 'W_TEST', "ok_has_warning found W_TEST with 'all'");
    $T->ok_has_warning('all', 'W_EXECUTE', "ok_has_warning found W_EXECUTE with 'all'");
    
    # Now create a second edt. Add a warning not associated with any action.
    
    my $edt2 = $T->new_edt($perm_a);
    
    $edt2->add_condition('W_EXECUTE');
    
    $T->ok_has_warning('W_EXECUTE', "found W_EXECUTE condition");
    $T->ok_has_one_warning('W_EXECUTE', "found W_EXECUTE condition as one warning");
    $T->ok_has_warning('main', 'W_EXECUTE', "found W_EXECUTE condition with 'main'");
    $T->ok_has_one_warning('W_EXECUTE', "found W_EXECUTE condition as one warning");
    
    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_no_warnings("ok_no_warnings properly fails");
	$T->ok_no_warnings('main', "ok_no_warnings properly fails with 'main'");
	$T->ok_no_warnings('all', "ok_no_warnings properly fails wtih 'all'");
	$T->ok_no_warnings('latest', "ok_no_warnings properly fails with 'latest'");
	$T->ok_no_conditions($edt2, 'main', "ok_no_conditions properly fails with edt2 and 'main'");
    }
    
    $edt2->insert_record('EDT_TEST', { string_req => 'ghi' });
    
    $edt2->add_condition('W_TRUNC', 'foobar', 'baz');
    
    $T->ok_has_one_warning('main', 'W_EXECUTE', "only one warning with 'main'");
    $T->ok_has_one_warning('latest', 'W_TRUNC', "only one warning with 'latest'");
    $T->ok_has_warning('all', 'W_EXECUTE', "found W_EXECUTE with 'all'");
    $T->ok_has_warning('all', 'W_TRUNC', "found W_TRUNC with 'all'");
    
    # Check that we can still test warnings from the previous transaction, since it hasn't yet been
    # destroyed.
    
    $T->ok_has_warning($edt, 'W_EXECUTE', "found W_EXECUTE from first edt");
    $T->ok_has_warning($edt, 'W_TEST', "found W_TEST from first edt");
    $T->ok_has_warning($edt, 'all', 'W_EXECUTE', "found W_EXECUTE from first edt with 'all'");
    $T->ok_no_conditions($edt, 'main', "no conditions for first edt with 'main'");
    $T->ok_has_one_warning($edt2, 'latest', 'W_TRUNC', "found W_TRUNC as one warning from second edt");
    $T->ok_has_warning($edt2, 'main', 'W_EXECUTE', "found W_EXECUTE from second edt");
    
    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_has_warning($edt, 'main', 'W_TRUNC', "properly did not find W_TRUNC from first edt with 'main'");
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_warnings($edt2, 'latest');
	ok( $EditTester::TEST_DIAG =~ /W_TRUNC/, "found W_TRUNC from diag_warnings(\$edt2)" );
	ok( $EditTester::TEST_DIAG !~ /W_EXECUTE/, "did not find W_EXECUTE from diag_warnings(\$edt2)");
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_warnings($edt);
	ok( $EditTester::TEST_DIAG =~ /W_TEST/, "found W_TEST from diag_warnings(\$edt)" );
	ok( $EditTester::TEST_DIAG !~ /W_TRUNC/, "did not find W_TRUNC from diag_warnings(\$edt)" );
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_warnings($edt, 'all');
	ok( $EditTester::TEST_DIAG =~ /W_TEST/, "found W_TEST from diag_warnings(\$edt, 'all')" );
	ok( $EditTester::TEST_DIAG =~ /W_EXECUTE/, "found W_EXECUTE from diag_warnings(\$edt, 'all')" );
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_warnings($edt2, 'main');
	ok( $EditTester::TEST_DIAG =~ /W_EXECUTE/, "found W_EXECUTE from diag_warnings(\$edt2, 'main')" );
	ok( $EditTester::TEST_DIAG !~ /W_TRUNC/, "did not find W_TRUNC from diag_warnings(\$edt2, 'main')" );
    }

    # Finally, add an error condition to both transactions and make sure that both errors and warnings are
    # properly reported.
    
    $edt->add_condition('E_EXECUTE', 'xyzzy');
    $edt2->add_condition('E_EXECUTE', 'bazbaz');
    
    $T->ok_no_conditions($edt, 'main', "ok_no_conditions edt 'main' passes test");
    $T->ok_no_warnings($edt, 'main', "ok_no_warnings edt 'main' passes test");
    $T->ok_no_errors($edt, 'main', "ok_no_errors edt 'main' passes test");
    $T->ok_has_error($edt, qr{E_EXECUTE.*xyzzy}, "ok_has_error edt finds E_EXECUTE");
    $T->ok_has_one_error($edt, qr{E_EXECUTE.*xyzzy}, "ok_has_one_error edt finds E_EXECUTE");
    $T->ok_has_warning($edt, qr{W_EXECUTE.*foobar}, "ok_has_warning edt finds W_EXECUTE");
    
    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_no_conditions($edt, 'all', "ok_no_conditions edt 'all' properly fails");
	$T->ok_no_conditions($edt, 'latest', "ok_no_conditions edt 'latest' properly fails");
	$T->ok_has_one_warning($edt, qr{W_EXECUTE.*foobar}, "ok_has_one_warning properly fails");
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->diag_errors($edt);
	$T->diag_warnings($edt);
	ok( $EditTester::TEST_DIAG =~ /E_EXECUTE.*xyzzy/, "found E_EXECUTE from diag_errors(\$edt)" );
	ok( $EditTester::TEST_DIAG =~ /W_TEST/, "found W_TEST from diag_warnings(\$edt)" );
	
	$EditTester::TEST_DIAG = '';
	ok( $EditTester::TEST_DIAG eq '', "properly cleared TEST_DIAG" );
	$T->ok_result(0, 'latest', "ok_result properly failed with a false argument");
	ok( $EditTester::TEST_DIAG =~ /E_EXECUTE.*bazbaz/, "found E_EXECUTE from ok_result" );	
	ok( $EditTester::TEST_DIAG =~ /W_TRUNC/, "found W_TRUNC from ok_result" );	
	ok( $EditTester::TEST_DIAG !~ /W_EXECUTE/, "did not find W_EXECUTE from ok_result" );
    }
};


# Now test that the methods for checking the presence or absence of records in tables work properly.

subtest 'records' => sub {

    # This subtest will deliberately insert records "manually" to make sure that we are only
    # testing one particular aspect of the EditTester class.
    
    my $dbh = $T->dbh;
    
    my ($result);
    
    # First clear the table, and insert some records. We make sure that more than one record will
    # be matched by the test expression.
    
    $dbh->do("DELETE FROM $TABLE{EDT_TEST}");
    $dbh->do("ALTER TABLE $TABLE{EDT_TEST} AUTO_INCREMENT = 1");
    
    $dbh->do("INSERT INTO $TABLE{EDT_TEST} (string_req, authorizer_no, enterer_no)
		values ('abc',0,0), ('abc',0,0), ('def',0,0)");
    
    $T->ok_found_record('EDT_TEST', "string_req = 'def'", "found unique record");
    $T->ok_found_record('EDT_TEST', "string_req = 'abc'", "found non-unique record");
    $T->ok_no_record('EDT_TEST', "string_req = 'xyz'", "did not find non-existent record");
    $T->ok_count_records(2, 'EDT_TEST', "string_req = 'abc'", "found 2 matching records");
    
    {
	local($EditTester::TEST_MODE) = 1;
	$T->ok_found_record('EDT_TEST', "string_req = 'xyz'", "ok_found_record properly failed");
	$T->ok_no_record('EDT_TEST', "string_req = 'abc'", "ok_no_record properly failed");
	$T->ok_count_records(1, 'EDT_TEST', "string_req = 'abc'", "ok_count_records properly failed with 'abc'");
	$T->ok_count_records(1, 'EDT_TEST', "string_req = 'jkl'", "ok_count_records properly failed with 'jkl'");
    }
    
    my (@r) = $T->fetch_records_by_expr('EDT_TEST', "string_req = 'abc'");
    
    if ( is( scalar(@r), 2, "fetch_records_by_expr retrieved two records" ) )
    {
	is( $r[0]{string_req}, 'abc', "record 0 had proper value for string_req" );
	is( $r[1]{string_req}, 'abc', "record 1 had proper value for string_req" );
	cmp_ok( $r[0]{test_no}, '>', 0, "record 0 had proper value for test_no" );
	cmp_ok( $r[1]{test_no}, '>', 0, "record 1 had proper value for test_no" );
	
	my (@s) = $T->fetch_records_by_key('EDT_TEST', $r[0]{test_no}, $r[1]{test_no});
	
	is( scalar(@s), 2, "fetch_records_by_key retrieved two records" );

	is( $s[0]{string_req}, 'abc', "record 0 had proper value for string_req" );
	is( $s[1]{string_req}, 'abc', "record 1 had proper value for string_req" );

	$T->ok_found_record('EDT_TEST', "test_no = $r[0]{test_no} and string_req = 'abc'");
    }

    {
	local($EditTester::TEST_MODE) = 1;
	$T->fetch_records_by_expr('EDT_TEST', "string_req = 'xyzzy'");
	$T->fetch_records_by_key('EDT_TEST', 99998, 99997, 99996);
	$T->fetch_row_by_expr('EDT_TEST', 'string_req, string_val', "test_no = 99999");
    }
    
    my ($max, $min) = $T->fetch_row_by_expr('EDT_TEST', 'max(test_no), min(test_no)');
    
    cmp_ok($max, '>', 0, "fetch_row_by_expr found max greater than zero");
    cmp_ok($min, '>', 0, "fetch_row_by_expr found min graeter than zero");

    my ($key) = $T->fetch_row_by_expr('EDT_TEST', 'test_no', "string_req='abc'");

    cmp_ok($key, '>', 0, "fetch_row_by_expr found record with key");

    my ($count) = $T->fetch_row_by_expr('EDT_TEST', 'count(*)');

    cmp_ok($count, '>', 0, "table 'EDT_TEST' has at least one row");

    $T->clear_table('EDT_TEST');

    ($count) = $T->fetch_row_by_expr('EDT_TEST', 'count(*)');

    cmp_ok($count, '==', 0, "table 'EDT_TEST' has no records after clearing");

    $dbh->do("INSERT INTO $TABLE{EDT_TEST} (string_req, authorizer_no, enterer_no)
		values ('foo',0,0)");
    
    ($max) = $T->fetch_row_by_expr('EDT_TEST', 'max(test_no)');
    
    cmp_ok($max, '==', 1, "table 'EDT_TEST' has auto increment = 1 after clearing");
};


# We need to run some kind of test on the 'test_permissions' method, but unfortunately this is too
# complicated to really test separately from the specific test file involved in permission
# checking. We just have to hope that if the test method has an error, some of the tests in that
# file will fail.

subtest 'test_permissions' => sub {

    # Make sure that CAN_POST is set properly for 'EDT_TEST', and then call test_permissions. Set
    # it to NOBODY and then check that test_permissions fails.
    
    set_table_property('EDT_TEST', CAN_POST => 'AUTHORIZED');
    $perm_a->clear_cached_permissions;
    
    $T->test_permissions('EDT_TEST', $perm_a, 'basic', 'succeeds', "authorizer succeeded");
    
    set_table_property('EDT_TEST', CAN_POST => 'NOBODY');
    $perm_a->clear_cached_permissions;
    
    $T->test_permissions('EDT_TEST', $perm_a, 'basic', 'fails', "fails with permission denial");
    
    # Now set the table property back again.

    set_table_property('EDT_TEST', CAN_POST => 'AUTHORIZED');
    $perm_a->clear_cached_permissions;
};


# Now test the convenience methods for returning the sets of inserted, deleted, etc. keys from the
# most recent edt.

subtest 'keys' => sub {

    # First clear the table so that we can track record insertions.

    $T->clear_table('EDT_TEST');

    # Then create a new transaction and insert some records. Check that we can retrieve the
    # inserted keys, and that calling the edt directly and through EditTester produces the same
    # list.
    
    my $edt1 = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt1->insert_record('EDT_TEST', { string_req => 'abc' });
    $edt1->insert_record('EDT_TEST', { string_req => 'def' });
    $edt1->insert_record('EDT_TEST', { string_req => 'ghi' });
    
    $edt1->commit;
    
    my (@k1) = $edt1->inserted_keys;
    my (@k2) = $T->inserted_keys;
    
    is_deeply( \@k1, \@k1, "two calls to inserted_keys returned same list" );
    ok( @k2, "inserted_keys returned at least one value" );
    
    # Now try a second transaction, and check replaced_keys, updated_keys, and deleted_keys.
    
    my $edt2 = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1 });
    
    $edt2->replace_record('EDT_TEST', { test_no => $k1[0], string_req => 'ghi' });
    
    my (@k3) = $edt2->replaced_keys;
    my (@k4) = $T->replaced_keys;
    
    is_deeply( \@k3, \@k4, "two calls to replaced_keys returned same list" );
    ok( @k4, "replaced_keys returned at least one value" );
    
    my (@k5) = $T->inserted_keys;
    
    is( scalar(@k5), 0, "call to inserted_keys returned empty list" );
    
    my (@k6) = $T->inserted_keys($edt1);
    
    is_deeply( \@k6, \@k1, "call to inserted_keys with edt ref returned proper list" );
    
    ok( ! $T->replaced_keys($edt1), "call to replaced_keys with edt ref in scalar context returned false" );
    
    $edt2->update_record('EDT_TEST', { test_no => $k1[1], string_req => 'update1' });
    $edt2->update_record('EDT_TEST', { test_no => $k1[0], string_req => 'update2' });
    
    my (@k7) = $edt2->updated_keys;
    my (@k8) = $T->updated_keys;
    
    is_deeply( \@k7, \@k8, "two calls to updated_keys returned same list" );
    ok( @k8, "updated_keys returned at least one value" );

    $edt2->delete_record('EDT_TEST', $k1[0]);

    my (@k9) = $edt2->deleted_keys;
    my (@k10) = $T->deleted_keys;

    is_deeply( \@k9, \@k10, "two calls to deleted_keys returned same list" );
    ok( @k10, "deleted_keys returned at least one value" );
};


