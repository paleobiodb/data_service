#
# PBDB Data Service
# -----------------
# 
# This file contains unit tests for the EditTransaction class.
# 
# edt-22-validate.t : Test that validation of a request against the database schema works
# properly: the only values which are let through are those which can be stored in the table, and
# errors are generated for others.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 9;

use TableDefs qw(get_table_property set_column_property);

use TableData qw(reset_cached_column_properties);

use EditTest;
use EditTester;

use Carp qw(croak);
use Encode;


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;



my ($perm_a, $perm_e, $primary);


subtest 'setup' => sub {
    
    # Grab the permissions that we will need for testing.
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) or die;
    
    $perm_e = $T->new_perm('SESSION-ENTERER');
    
    # Grab the name of the primary key of our test table.
    
    $primary = get_table_property('EDT_TEST', 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || die;
    
    # Clear any specific table permissions.
    
    $T->clear_specific_permissions;
};


# Check that character data is properly checked and input.

subtest 'text' => sub {

    use utf8;
    
    # Clear the table, so that we can track record insertions.

    $T->clear_table('EDT_TEST');

    # Then try inserting a record which is too long.
    
    my ($edt, $result, $key1);

    my $long_value = "a string which is too long to fit inside the database table into which it must be stored";
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $result = $edt->insert_record('EDT_TEST', { string_req => $long_value });
    
    $T->ok_has_one_error('latest', qr/F_WIDTH.*no more than/, "could not insert record with string value too long");
    
    # Set the table column property ALLOW_TRUNCATE, and try again.
    
    set_column_property('EDT_TEST', 'string_req', ALLOW_TRUNCATE => 1);
    reset_cached_column_properties('EDT_TEST', 'string_req');
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $result = $edt->insert_record('EDT_TEST', { string_req => $long_value });
    
    $T->ok_no_errors("inserted test record with long value");
    $T->ok_has_warning( qr/W_TRUNC.*truncated/ );

    # Make sure that the string value was actually truncated to 40 characters.
    
    my ($r) = $T->fetch_records_by_expr('EDT_TEST', "string_req like 'a string %'");

    if ( ok( $r && $r->{string_req}, "found value for 'string_req'" ) )
    {
	is( length($r->{string_req}), 40, "value of 'string_req' is 40 characters long" );
	is( $r->{string_req}, substr($long_value, 0, 40),
	    "value of 'string_req' is first 40 characters of specified value");
    }
    
    # Now try again with a value that is just under 40 Unicode characters but is more than 40
    # bytes long. We check that this value is not truncated, and that it comes back as the same
    # string that was inserted.
    
    # This test also checks that non-ascii text can be properly inserted and retrieved.
    
    my $wide_value = "wide chars: αβγδεζηθικλμνξοπρςστυφχψω";
    
    cmp_ok( length($wide_value), '<', 40, "wide value is properly encoded" );
    
    $result = $edt->insert_record('EDT_TEST', { string_req => $wide_value });
    
    $T->ok_no_conditions('latest');
    
    ($r) = $T->fetch_records_by_expr('EDT_TEST', "string_req like 'wide chars: %'");
    
    if( ok( $r && $r->{string_req}, "found value for 'string_req'" ) )
    {
	is( length($r->{string_req}), length($wide_value),
	    "value of 'string_req' was not truncated" );
	is( $r->{string_req}, $wide_value, "value was not corrupted" );
    }
    
    # Make sure we can set text columns as well as char columns. The particular column in the test
    # table is also set to allow null values, so we can check this as well.

    $key1 = $edt->insert_record('EDT_TEST', { string_req => 'text test', text_val => 'some text' });
    
    ok( $key1, "inserted one record" ) || return;
    
    $edt->update_record('EDT_TEST', { $primary => $key1, text_val => '' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and text_val = ''");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, text_val => undef });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and text_val is null");
    $T->ok_no_conditions('latest');
    
    ($r) = $T->fetch_records_by_expr('EDT_TEST', "$primary = $key1");
    
    ok( $r && exists $r->{text_val} && $r->{text_val} eq undef,
	"text field exists and is undefined" );

    # Now set a 'not null' character column to null, and check that the value that is inserted is
    # the empty string.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, string_val => undef });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and string_val = ''");
    $T->ok_no_conditions('latest');
    
    ($r) = $T->fetch_records_by_expr('EDT_TEST', "$primary = $key1");
    
    ok( $r && exists $r->{string_val} && $r->{string_val} eq '',
	"text field exists and its value is the empty string" );
    
    # Make sure we had no errors overall.
    
    $T->ok_no_errors;
};


# Test that binary (non-text) character data is properly checked and input.

subtest 'binary' => sub {

    pass('placeholder');
    diag("\$\$\$ need to add binary data tests");
    
};


# Test that boolean data is properly checked and input.

subtest 'boolean' => sub {
    
    # Clear the table, so that we can track record insertions.
    
    $T->clear_table('EDT_TEST');
    
    # Then try inserting a record and setting the boolean column to true, false, and null.
    
    my ($edt, $result, $key1);
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $key1 = $edt->insert_record('EDT_TEST', { string_req => 'boolean test', boolean_val => 1 });

    ok( $key1, "inserted test record" ) || return;

    $T->ok_found_record('EDT_TEST', "$primary = $key1 and boolean_val", "column value is true");    
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, boolean_val => 0 });

    $T->ok_found_record('EDT_TEST', "$primary = $key1 and not boolean_val", "column value is false");
    $T->ok_no_conditions('latest');

    $edt->update_record('EDT_TEST', { $primary => $key1, boolean_val => undef });

    $T->ok_found_record('EDT_TEST', "$primary = $key1 and boolean_val is null", "column value is null");
    $T->ok_no_conditions('latest');
    
    # Now try 'true', 'false', 'yes', 'no'.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, boolean_val => 'TRUE' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and boolean_val", "column value is true");    
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, boolean_val => 'false' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and not boolean_val", "column value is false");
    $T->ok_no_conditions('latest');

    $edt->update_record('EDT_TEST', { $primary => $key1, boolean_val => undef });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and boolean_val is null", "column value is null");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, boolean_val => '  yES' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and boolean_val", "column value is true");    
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, boolean_val =>   'No   ' });

    $T->ok_found_record('EDT_TEST', "$primary = $key1 and not boolean_val", "column value is false");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, boolean_val => '' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and boolean_val is null", "column value is null");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, boolean_val => '   1    ' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and boolean_val = 1", "column value is true");
    $T->ok_no_conditions('latest');
    
    # Check that there are no errors or warnings at all.
    
    $T->ok_no_conditions;
    
    # Now try a bad value and make sure it gets rejected.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, boolean_val => 'true_but_bad' });
    
    $T->ok_has_one_error('latest',  'F_FORMAT', "got parameter error for bad boolean value" );
};


# Test that integer data is properly checked and input.

subtest 'integer' => sub {
    
    # Clear the table, so that we can track record insertions.
    
    $T->clear_table('EDT_TEST');
    
    # Then try inserting a record with an unsigned integer value.
    
    my ($edt, $result, $key1);
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $key1 = $edt->insert_record('EDT_TEST', { string_req => 'boolean test', unsigned_val => 1 });
    
    ok( $key1, "inserted test record" ) || return;
    
    # Now try zero, a string starting with zeros, the empty string, and undefined (null). This
    # column is defined as "not null", so null values are turned into a default value of zero.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsigned_val => 0 });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsigned_val = 0", "column value is 0");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsigned_val => '0005' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsigned_val = 5", "column value is 5");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsigned_val => '' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsigned_val = 0", "column value defaults to 0");
    $T->ok_no_conditions('latest');

    $edt->update_record('EDT_TEST', { $primary => $key1, unsigned_val => '10000  ' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsigned_val = 10000", "column value is 10000");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsigned_val => undef });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsigned_val = 0", "column value is 0 after stored null");
    $T->ok_no_conditions('latest');

    $edt->update_record('EDT_TEST', { $primary => $key1, unsigned_val => "+17" });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsigned_val = 17", "column value is 17");
    $T->ok_no_conditions('latest');

    $edt->update_record('EDT_TEST', { $primary => $key1, unsigned_val => "+ 028  " });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsigned_val = 28", "column value is 28");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsigned_val => "16777215" });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsigned_val = 16777215", "column value is maximum for medium int");
    $T->ok_no_conditions('latest');
    
    # Now try the same with a signed value. This column allows null values, so we check for
    # them. We also check negative values, zero, and the empty string.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => '0030' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and signed_val = 30", "column value is 30");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => '' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and signed_val is null", "column value is null");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => '-024' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and signed_val = -24", "column value is -24");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => undef });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and signed_val is null", "column value is null");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => 0 });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and signed_val = 0", "column value is 0");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => '1' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and signed_val = 1", "column value is 1");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => '-0' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and signed_val = 0", "column value is 0");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => '-    0110   ' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and signed_val = -110", "column value is -110");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => '+22   ' });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and signed_val = 22", "column value is 22");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => "8388607" });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and signed_val = 8388607", "column value is maximum for medium int");
    $T->ok_no_conditions('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => "-8388608" });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and signed_val = -8388608", "column value is maximum for medium int");
    $T->ok_no_conditions('latest');
    
    # Now try a different-sized integer.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, tiny_val => 255 });
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and tiny_val = 255", "column value is maximum for tiny int");
    $T->ok_no_conditions('latest');
    
    # Check that there are no errors or warnings at all.
    
    $T->ok_no_conditions;
    
    # Now try some bad values and check that we get the proper errors. We start with values that
    # are not in the proper format, then check range errors.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => 'abc' });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "parameter error for non-integer value");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => '12a' });

    $T->ok_has_one_error('latest', 'F_FORMAT', "parameter error for non-integer ending");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => '-' });

    $T->ok_has_one_error('latest', 'F_FORMAT', "format error for single minus sign");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => "8388608" });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "signed int exceeds bound");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, signed_val => "-8388609" });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "signed int exceeds negative bound");
    $T->ok_no_warnings('latest');
    
    # Now try for unsigned integers.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsigned_val => "16777216" });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "unsigned int exceeds bound");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsigned_val => "  - 23 " });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "unsigned int must be nonnegative");
    $T->ok_no_warnings('latest');

    $edt->update_record('EDT_TEST', { $primary => $key1, unsigned_val => "  14    b" });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "format error for non-digit suffix");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsigned_val => "-0" });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "value -0 not accepted for unsigned integer");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, tiny_val => 256 });

    $T->ok_has_one_error('latest', 'F_RANGE', "value 256 out of range for tinyint unsigned");
    $T->ok_no_warnings('latest');
};


# Now check that fixed-point data is properly checked and input.

subtest 'fixed' => sub {
    
    # Clear the table, so that we can track record insertions.
    
    $T->clear_table('EDT_TEST');
    
    # Then try inserting a record with an unsigned decimal value.
    
    my ($edt, $result, $key1);
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $key1 = $edt->insert_record('EDT_TEST', { string_req => 'decimal test', decimal_val => 3.14 });
    
    ok( $key1, "inserted test record" ) || return;
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and decimal_val = 3.14", "column value is 3.14");
    
    # Now try various acceptable input values including zero, the empty string, undefined, etc.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => 0 });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and decimal_val = 0", "column value is 0");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '  - 00.0200  ' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and decimal_val = -0.02", "column value is -0.02");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and decimal_val is null", "column value is null");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '+5.04 e2' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and decimal_val = 504", "column value is 504");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => undef });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and decimal_val is null", "column value is null");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => ' - 1.3 e - 1   ' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and decimal_val = -0.13", "column value is -0.13");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '0.' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and decimal_val = 0", "column value is 0");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '999.99' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and decimal_val = 999.99", "column value is max allowed for decimal(5,2)");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '-099.9990 e 1  ' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and decimal_val = -999.99", "column value is min allowed for decimal(5,2)");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '     ' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and decimal_val is null", "column value is null");
    
    # Now check the results with a column that is unsigned decimal not null.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsdecimal_val => '54.030' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsdecimal_val = 54.03", "column value is 54.03");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsdecimal_val => '.0' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsdecimal_val = 0", "column value is 0");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsdecimal_val => ' + 2.0 E + 2 ' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsdecimal_val = 200", "column value is 200");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsdecimal_val => '' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsdecimal_val = 0", "column value is 0");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsdecimal_val => ' + 2.0 E - 2 ' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsdecimal_val = .02", "column value is .02");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsdecimal_val => undef });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsdecimal_val = 0", "column value is 0 after stored null");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsdecimal_val => '999.99' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsdecimal_val = 999.99", "column value is max allowed for decimal(5,2)");
    
    # Now try some bad values ane make sure we get the proper errors.

    $edt->update_record('EDT_TEST', { $primary => $key1, unsdecimal_val => 'abc' });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "format error for non-number");
    $T->ok_no_warnings('latest');
        
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => ' -2.03a5  ' });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "format error for non-numeric suffix");
    $T->ok_no_warnings('latest');
        
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '2. 03' });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "format error for space in the middle of the number");
    $T->ok_no_warnings('latest');
        
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '-' });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "format error for single minus sign");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '.' });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "format error for single decimal point");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '1000.0' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error for exceeding bound");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '-1000.0' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error for exceeding lower bound");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '1000.2345' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error for exceeding bound, even though precision is also too high");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '1.2345' });
    
    $T->ok_has_one_error('latest', 'F_WIDTH', "length error for exceeding precision");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '-1.2345' });
    
    $T->ok_has_one_error('latest', 'F_WIDTH', "length error for exceeding precision");
    $T->ok_no_warnings('latest');
    
    # Now try some incorrect values on the unsigned column.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsdecimal_val => '1e3' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error for value that is too large");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsdecimal_val => '-1.23456' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error for value that is negative");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsdecimal_val => '1.23456' });
    
    $T->ok_has_one_error('latest', 'F_WIDTH', "length error for exceeding precision");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '10000.2345' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error for exceeding bound, even though precision is also too high");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '10000.2345-' });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "format error for non-numeric suffix");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsdecimal_val => '-0' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error for -0");
    $T->ok_no_warnings('latest');
    
    # Now set the column property ALLOW_TRUNCATE to true, and check that we get W_TRUNC warnings
    # instead of E_WIDTH errors.

    set_column_property('EDT_TEST', 'decimal_val', ALLOW_TRUNCATE => 1);
    set_column_property('EDT_TEST', 'unsdecimal_val', ALLOW_TRUNCATE => 1);
    reset_cached_column_properties('EDT_TEST', 'decimal_val');
    reset_cached_column_properties('EDT_TEST', 'unsdecimal_val');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsdecimal_val => '123.456789000' });
    
    $T->ok_has_warning('latest', 'W_TRUNC', "got truncation warning");
    $T->ok_no_errors('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsdecimal_val = 123.45", "column value is 123.45");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => ' -999.002347 ' });
    
    $T->ok_has_warning('latest', 'W_TRUNC', "got truncation warning");
    $T->ok_no_errors('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and decimal_val = -999", "column value is -999");

    # Make sure that range checks still function.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, decimal_val => '-1e5' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error with ALLOW_TRUNCATE");
    $T->ok_no_warnings('latest');
        
};


# Now check that floating-point data is properly checked and input.

subtest 'float' => sub {
    
    # Clear the table, so that we can track record insertions.
    
    $T->clear_table('EDT_TEST');
    
    # Then try inserting a record with an unsigned decimal value.
    
    my ($edt, $result, $key1);
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $key1 = $edt->insert_record('EDT_TEST', { string_req => 'floating point test', double_val => 3.14 });
    
    ok( $key1, "inserted test record" ) || return;
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and double_val = 3.14", "column value is 3.14");
    
    # Now try various acceptable input values including zero, the empty string, undefined, etc.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => 0 });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and double_val = 0", "column value is 0");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '  - 00.0200  ' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and double_val = -0.02", "column value is -0.02");

    $edt->commit;
    return;
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and double_val is null", "column value is null");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '+5.04 e2' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and double_val = 504", "column value is 504");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => undef });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and double_val is null", "column value is null");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => ' - 1.3 e - 1   ' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and double_val = -0.13", "column value is -0.13");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '0.' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and double_val = 0", "column value is 0");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '2.53e+100' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and double_val = 2.53e100", "column value is enormously positive");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '-4.56 E +200' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and double_val = -4.56e200", "column value is enormously negative");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '     ' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and double_val is null", "column value is null");
    
    # Now check the results with a column that is unsigned float not null.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => '54.030' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsfloat_val = 54.03", "column value is 54.03");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => '.0' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsfloat_val = 0", "column value is 0");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => ' + 2.0 E + 2 ' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsfloat_val = 200", "column value is 200");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => '' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsfloat_val = 0", "column value is 0");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => ' + 2.0 E - 2 ' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsfloat_val = .02", "column value is .02");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => undef });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsfloat_val = 0", "column value is 0 after stored null");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => '001e38' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsfloat_val = 1e38", "column value is max we will accept for float");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => '1.2345e-38' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsfloat_val = 0", "large negative exponent gives a value of 0");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => '1.2345e-38' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsfloat_val = 0", "large negative exponent gives a value of 0");

    my $long_digit_string = '1.23456789012345678901234567890123456789000';
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => $long_digit_string });
    
    $T->ok_no_conditions('latest');
    my ($r) = $T->fetch_record_by_key('EDT_TEST', $key1);

    if ( ok( $r && $r->{unsfloat_val}, "found record with proper field" ) )
    {
	is( $r->{unsfloat_val}, substr($long_digit_string, 0, length($r->{unsfloat_val})),
	    "digits were stored with truncation" );
    }
    
    # Now try some bad values ane make sure we get the proper errors.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => 'abc' });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "format error for non-number");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => ' -2.03a5  ' });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "format error for non-numeric suffix");
    $T->ok_no_warnings('latest');
        
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '2. 03' });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "format error for space in the middle of the number");
    $T->ok_no_warnings('latest');
        
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '-' });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "format error for single minus sign");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '.' });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "format error for single decimal point");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '2e500' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error for exceeding bound");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '-2e500' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error for exceeding lower bound");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error for exceeding bound, even though precision is also too high");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '1.2345' });
    
    $T->ok_has_one_error('latest', 'F_WIDTH', "length error for exceeding precision");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '-1.2345' });
    
    $T->ok_has_one_error('latest', 'F_WIDTH', "length error for exceeding precision");
    $T->ok_no_warnings('latest');
    
    # Now try some incorrect values on the unsigned column.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => '1e3' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error for value that is too large");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => '-1.23456' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error for value that is negative");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => '1.23456' });
    
    $T->ok_has_one_error('latest', 'F_WIDTH', "length error for exceeding precision");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '10000.2345' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error for exceeding bound, even though precision is also too high");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '10000.2345-' });
    
    $T->ok_has_one_error('latest', 'F_FORMAT', "format error for non-numeric suffix");
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => '-0' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error for -0");
    $T->ok_no_warnings('latest');
    
    # Now set the column property ALLOW_TRUNCATE to true, and check that we get W_TRUNC warnings
    # instead of E_WIDTH errors.

    set_column_property('EDT_TEST', 'double_val', ALLOW_TRUNCATE => 1);
    set_column_property('EDT_TEST', 'unsfloat_val', ALLOW_TRUNCATE => 1);
    reset_cached_column_properties('EDT_TEST', 'double_val');
    reset_cached_column_properties('EDT_TEST', 'unsfloat_val');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, unsfloat_val => '123.456789000' });
    
    $T->ok_has_warning('W_TRUNC', "got truncation warning");
    $T->ok_no_errors;
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and unsfloat_val = 123.45", "column value is 123.45");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => ' -999.002347 ' });
    
    $T->ok_has_warning('W_TRUNC', "got truncation warning");
    $T->ok_no_errors;
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and double_val = -999", "column value is -999");

    # Make sure that range checks still function.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, double_val => '-1e5' });
    
    $T->ok_has_one_error('latest', 'F_RANGE', "range error with ALLOW_TRUNCATE");
    $T->ok_no_warnings('latest');
        
};


# Test that data is properly checked and stored into enumerated fields.

subtest 'enumerated' => sub {

    # Clear the table, so that we can track record insertions.
    
    $T->clear_table('EDT_TEST');
    
    # Then try inserting a record with an enumerated value.
    
    my ($edt, $result, $key1);
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $key1 = $edt->insert_record('EDT_TEST', { string_req => 'enumerated value test', enum_val => 'abc' });
    
    $T->ok_result( $key1, "inserted test record" ) || return;
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and enum_val = 'abc'",
			"found inserted record");
    
    # Now try inserting test values, values that differ in case, empty values, etc.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, enum_val => 'aBC' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and enum_val = 'abc'",
			"enum value with variant case was inserted successfully");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, enum_val => undef });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and enum_val is null",
			"null enum value was inserted successfully");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, enum_val => "'jkl'" });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and enum_val = '''jkl'''",
			"enum value containing single quotes was inserted successfully");
    
    my $non_ascii_value = "d\N{U+1F10}f";
    my $non_ascii_upcase = "D\N{U+1F18}F";
    
    $edt->update_record('EDT_TEST', { $primary => $key1, enum_val => $non_ascii_value });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and enum_val = '$non_ascii_value'",
			"enum value containing non-ascii characters was inserted successfully");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, enum_val => $non_ascii_upcase });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and enum_val = '$non_ascii_value'",
			"enum value containing non-ascii characters with different case was inserted successfully");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, enum_val => '' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and enum_val is null",
			"empty enum value was inserted as a null");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, enum_val => 'ghi' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and enum_val = 'ghi'",
			"a different allows value was successfully inserted");   
    
    # Now try some bad values and check that they properly generate errors.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, enum_val => 'xxx' });
    
    $T->ok_has_one_error('latest',  'F_RANGE', "got 'E_RANGE' error for unrecognized enum value" );
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, enum_val => 'abc,ghi' });
    
    $T->ok_has_one_error('latest',  'F_RANGE', "got 'E_RANGE' error for combination of correct values" );
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, enum_val => 'abc\'' });
    
    $T->ok_has_one_error('latest',  'F_RANGE', "got 'E_RANGE' error for value containing one quote character" );
    $T->ok_no_warnings('latest');
};


# Test that data is properly checked and stored into set fields.

subtest 'sets' => sub {

    # Clear the table, so that we can track record insertions.
    
    $T->clear_table('EDT_TEST');
    
    # Then try inserting a record with a set value.
    
    my ($edt, $result, $key1);
    
    $edt = $T->new_edt($perm_a, { IMMEDIATE_MODE => 1, PROCEED => 1 });
    
    $key1 = $edt->insert_record('EDT_TEST', { string_req => 'set value test', set_val => 'abc' });
    
    $T->ok_result( $key1, "inserted test record" ) || return;
    
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and set_val = 'abc'",
			"found inserted record");
    
    # Now try inserting test values, values that differ in case, empty values, etc.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, set_val => 'aBC' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and set_val = 'abc'",
			"set value with variant case was inserted successfully");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, set_val => undef });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and set_val is null",
			"set value of null is inserted properly");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, set_val => 'ghi,abc' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and set_val = 'abc,ghi'",
			"set value with multiples was inserted successfully");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, set_val => '' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and set_val is null",
			"empty set value is changed to null");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, set_val => 'abc , ghi, \'jkl\'' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and set_val like 'abc%'",
			"spaces around commas are removed");
    
    my $non_ascii_value = "d\N{U+1F10}f";
    my $non_ascii_upcase = "D\N{U+1F18}F";
    
    $edt->update_record('EDT_TEST', { $primary => $key1, set_val => "GHI,$non_ascii_upcase,ABC" });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and set_val = 'abc,$non_ascii_value,ghi'",
			"set value with multiples was inserted successfully");
    
    $edt->update_record('EDT_TEST', { $primary => $key1, set_val => ' abc,, ghi , ' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and set_val = 'abc,ghi'",
			"extra commas and spaces are ignored");
    
    # Now try some bad values and check that they properly generate errors.
    
    $edt->update_record('EDT_TEST', { $primary => $key1, set_val => 'xxx' });
    
    $T->ok_has_one_error('latest',  'F_RANGE', "got 'E_RANGE' error for unrecognized set value" );
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, set_val => 'abc,xxx,ghi' });
    
    $T->ok_has_one_error('latest',  'F_RANGE', "got 'E_RANGE' error for bad value among good ones" );
    $T->ok_no_warnings('latest');
    
    $edt->update_record('EDT_TEST', { $primary => $key1, set_val => 'abc\'' });
    
    $T->ok_has_one_error('latest',  'F_RANGE', "got 'E_RANGE' error for value containing one quote character" );
    $T->ok_no_warnings('latest');

    # Now try setting the property VALUE_SEPARATOR and check that it is properly applied.

    set_column_property('EDT_TEST', 'set_val', VALUE_SEPARATOR => qr{ / }xs );
    reset_cached_column_properties('EDT_TEST', 'set_val');

    $edt->update_record('EDT_TEST', { $primary => $key1, set_val => 'abc/\'jkl\'' });
    
    $T->ok_no_conditions('latest');
    $T->ok_found_record('EDT_TEST', "$primary = $key1 and set_val = 'abc,''jkl'''");

    $edt->update_record('EDT_TEST', { $primary => $key1, set_val => 'abc,ghi' });

    $T->ok_has_one_error('latest', 'F_RANGE', "got 'E_RANGE' error for improperly separated values");
    $T->ok_no_warnings('latest');
};
