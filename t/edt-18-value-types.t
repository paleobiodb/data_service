#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the EditTransaction class and the
# EditTransaction::Mod::MariaDB plugin module, using the subclass ETBasicTest
# whose sole purpose is to enable these tests.
#
# value-types.t :
#
#         Test the routines that validate individual column values according
#         to their data type: character, text, binary, boolean, integer,
#         fixed-point, floating-point, enumerated/set, and datetime. Each value
#         is inserted into the EDT_TYPES table and committed, and the stored
#         value is checked. Values that should be rejected are checked for the
#         proper error condition, which must be generated during validation
#         rather than by the database at execution time.
#
# This file was added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 13;

use TableDefs qw(%TABLE);

use ETBasicTest;
use EditTester qw(ok_new_edt clear_table
		  ok_has_condition ok_has_one_condition ok_no_conditions);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest', 'EDT_TYPES');

binmode(Test::More->builder->$_, ':utf8') foreach qw(output failure_output todo_output);


# Some values to be used in the tests below.

my $ALPHA = "\N{U+03B1}";	# Greek small letter alpha: 2 bytes in utf8, 1 byte in greek
my $EACUTE = "\N{U+00E9}";	# e with acute accent: 2 bytes in utf8, 1 byte in latin1

my $RECNO = 0;		# used to generate a unique name for each inserted record


# The definitions for 'store_ok', 'reject_ok', 'pending_columns', and
# 'set_truncate' are at the end of this file.

sub store_ok; sub reject_ok; sub pending_columns; sub set_truncate;


# Check the methods that must be present.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'validate_char_value', 'validate_binary_value',
	   'validate_boolean_value', 'validate_integer_value', 'validate_fixed_value',
	   'validate_float_value', 'validate_enum_value', 'validate_datetime_value') ||
	
	BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check integer values. The bugs fixed in commit 9bd5242f caused unsigned
# columns to be validated as signed, so unsigned values were checked against
# the wrong bounds and negative values were passed through to the database.

subtest 'integer values' => sub {
    
    clear_table('EDT_TYPES');
    
    # Signed mediumint: -8388608 .. 8388607
    
    store_ok('signed_val', '0', '0', "signed zero");
    store_ok('signed_val', '-8388608', '-8388608', "signed lower bound");
    store_ok('signed_val', '8388607', '8388607', "signed upper bound");
    store_ok('signed_val', ' 12 ', '12', "signed value with whitespace");
    store_ok('signed_val', '+7', '7', "signed value with plus sign");
    store_ok('signed_val', '', undef, "signed empty value becomes null");
    
    reject_ok('signed_val', '8388608', 'E_RANGE', qr/-8388608 and 8388607/,
	      "signed value above upper bound");
    reject_ok('signed_val', '-8388609', 'E_RANGE', qr/-8388608 and 8388607/,
	      "signed value below lower bound");
    reject_ok('signed_val', 'abc', 'E_FORMAT', qr/must be an integer/,
	      "signed non-numeric value");
    reject_ok('signed_val', '1.5', 'E_FORMAT', qr/must be an integer/,
	      "signed non-integer value");
    
    # Unsigned mediumint: 0 .. 16777215
    
    store_ok('unsigned_val', '0', '0', "unsigned zero");
    store_ok('unsigned_val', '16777215', '16777215', "unsigned upper bound");
    
    reject_ok('unsigned_val', '16777216', 'E_RANGE', qr/less than or equal to 16777215/,
	      "unsigned value above upper bound");
    reject_ok('unsigned_val', '-5', 'E_RANGE', qr/must be an unsigned integer/,
	      "unsigned negative value");
    reject_ok('unsigned_val', 'abc', 'E_FORMAT', qr/must be an unsigned integer/,
	      "unsigned non-numeric value");
    
    # Unsigned tinyint: 0 .. 255
    
    store_ok('tiny_val', '255', '255', "unsigned tinyint upper bound");
    
    reject_ok('tiny_val', '256', 'E_RANGE', qr/less than or equal to 255/,
	      "unsigned tinyint above upper bound");
};


# Check bigint values. The bounds of these columns cannot be checked using
# Perl numeric comparison, because they exceed the precision of a double.

subtest 'bigint values' => sub {
    
    store_ok('bigint_val', '9223372036854775807', '9223372036854775807',
	     "signed bigint upper bound");
    store_ok('bigint_val', '-9223372036854775808', '-9223372036854775808',
	     "signed bigint lower bound");
    store_ok('bigint_val', '0', '0', "signed bigint zero");
    
    reject_ok('bigint_val', '9223372036854775808', 'E_RANGE', qr/lie between/,
	      "signed bigint above upper bound");
    reject_ok('bigint_val', '-9223372036854775809', 'E_RANGE', qr/lie between/,
	      "signed bigint below lower bound");
    
    store_ok('ubigint_val', '18446744073709551615', '18446744073709551615',
	     "unsigned bigint upper bound");
    store_ok('ubigint_val', '0018446744073709551615', '18446744073709551615',
	     "unsigned bigint upper bound with leading zeros");
    
    reject_ok('ubigint_val', '18446744073709551616', 'E_RANGE',
	      qr/less than or equal to 18446744073709551615/,
	      "unsigned bigint above upper bound");
    reject_ok('ubigint_val', '1' . ('0' x 70), 'E_RANGE',
	      qr/less than or equal to 18446744073709551615/,
	      "unsigned bigint with 71 digits");
    reject_ok('ubigint_val', '-1', 'E_RANGE', qr/must be an unsigned integer/,
	      "unsigned bigint negative value");
};


# Check fixed-point values. The bug fixed in commit 9bd5242f caused signed
# decimal columns to be validated as unsigned.

subtest 'fixed-point values' => sub {
    
    # decimal(5,2): at most 3 digits before the decimal point and 2 after.
    
    store_ok('decimal_val', '-12.5', '-12.50', "signed decimal negative value");
    store_ok('decimal_val', '999.99', '999.99', "decimal maximum value");
    store_ok('decimal_val', '7', '7.00', "decimal with no decimal point");
    store_ok('decimal_val', '0', '0.00', "decimal zero");
    store_ok('decimal_val', '1.5e2', '150.00', "decimal with exponent");
    store_ok('decimal_val', '', undef, "decimal empty value becomes null");
    
    reject_ok('decimal_val', '1000', 'E_RANGE', qr/too large for decimal\(5,2\)/,
	      "decimal with too many integer digits");
    reject_ok('decimal_val', '1.234', 'E_WIDTH', qr/too many decimal digits/,
	      "decimal with too many fraction digits");
    reject_ok('decimal_val', '1.2.3', 'E_FORMAT', qr/must be a decimal number/,
	      "decimal bad format");
    
    # With ALLOW_TRUNCATE, excess fraction digits are dropped with a warning.
    
    set_truncate('decimal_val', 1);
    
    store_ok('decimal_val', '1.239', '1.23', "decimal truncated with ALLOW_TRUNCATE",
	     { warning => 'W_TRUNC' });
    
    set_truncate('decimal_val', 0);
    
    # Unsigned decimal(5,2)
    
    store_ok('unsdecimal_val', '3.25', '3.25', "unsigned decimal value");
    
    reject_ok('unsdecimal_val', '-1.5', 'E_RANGE', qr/must be an unsigned decimal/,
	      "unsigned decimal negative value");
};


# Check floating-point values. The bug fixed in commit 8ce14679 caused negative
# values to be rejected for signed columns.

subtest 'floating-point values' => sub {
    
    store_ok('double_val', '-1.5', '-1.5', "double negative value");
    store_ok('double_val', '0', '0', "double zero");
    store_ok('double_val', '2', '2', "double integer value");
    store_ok('double_val', '1e300', qr{^1e\+?300$}, "double large value");
    store_ok('double_val', '', undef, "double empty value becomes null");
    
    reject_ok('double_val', '1e400', 'E_RANGE', qr/too large for double-precision/,
	      "double magnitude too large");
    reject_ok('double_val', '-1e400', 'E_RANGE', qr/too large for double-precision/,
	      "double negative magnitude too large");
    reject_ok('double_val', 'x', 'E_FORMAT', qr/must be a floating point number/,
	      "double bad format");
    
    store_ok('unsfloat_val', '2.5', '2.5', "unsigned float value");
    
    reject_ok('unsfloat_val', '-1.5', 'E_RANGE', qr/must be an unsigned floating point/,
	      "unsigned float negative value");
    reject_ok('unsfloat_val', '1e39', 'E_RANGE', qr/too large for single-precision/,
	      "float magnitude too large");
    reject_ok('unsfloat_val', 'x', 'E_FORMAT', qr/must be an unsigned floating point/,
	      "unsigned float bad format");
};


# Check boolean values. The bug fixed in commit 9bd5242f caused an empty value
# to be passed to the database instead of being turned into a null.

subtest 'boolean values' => sub {
    
    store_ok('boolean_val', '1', '1', "boolean 1");
    store_ok('boolean_val', '0', '0', "boolean 0");
    
    foreach my $v ( 'yes', 'YES', 'true', 'on', 'y' )
    {
	store_ok('boolean_val', $v, '1', "boolean synonym '$v'");
    }
    
    foreach my $v ( 'no', 'False', 'off', 'n' )
    {
	store_ok('boolean_val', $v, '0', "boolean synonym '$v'");
    }
    
    store_ok('boolean_val', '', undef, "boolean empty value becomes null");
    store_ok('boolean_val', '  ', undef, "boolean whitespace value becomes null");
    
    reject_ok('boolean_val', 'maybe', 'E_FORMAT', qr/must be one of/,
	      "boolean bad value");
};


# Check enumerated and set values. Matching is case-insensitive.

subtest 'enum and set values' => sub {
    
    store_ok('enum_val', 'ghi', 'ghi', "enum value");
    store_ok('enum_val', 'ABC', 'abc', "enum value matched without regard to case");
    store_ok('enum_val', "d\N{U+1F10}f", "d\N{U+1F10}f", "enum value with non-ASCII character");
    store_ok('enum_val', "'jkl'", "'jkl'", "enum value containing quotes");
    store_ok('enum_val', '', undef, "enum empty value becomes null");
    
    reject_ok('enum_val', 'xyz', 'E_BAD_VALUE', qr/value 'xyz' is not allowed/,
	      "enum bad value");
    
    store_ok('set_val', 'abc,ghi', 'abc,ghi', "set with two values");
    store_ok('set_val', 'ghi , abc', 'abc,ghi', "set with whitespace and different order");
    
    reject_ok('set_val', 'abc, xyz, qq', 'E_BAD_VALUE', qr/values 'xyz, qq' are not allowed/,
	      "set with two bad values");
};


# Check datetime values. The empty value was previously rejected instead of
# being turned into a null.

subtest 'datetime values' => sub {
    
    store_ok('dcr', '2024-01-02 03:04:05', '2024-01-02 03:04:05', "datetime value");
    store_ok('dcr', '2024-01-02', '2024-01-02 00:00:00', "date value is given a time of midnight");
    store_ok('dcr', 'now', qr/^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/, "datetime 'now'");
    store_ok('dcr', 'NOW()', qr/^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/, "datetime 'NOW()'");
    store_ok('dcr', '1700000000', qr/^2023-11-1[45] /, "datetime from unix timestamp");
    store_ok('dcr', '', undef, "datetime empty value becomes null");
    
    reject_ok('dcr', 'Jan 1', 'E_FORMAT', qr/invalid datetime value/, "datetime bad format");
    reject_ok('dcr', '2024-1-2', 'E_FORMAT', qr/invalid datetime value/,
	      "datetime with single-digit month and day");
  
    # TODO block removed: rejecting a whitespace-only datetime is the intended behavior.
    # [Changed by Claude under the guidance of Michael McClennen]
    reject_ok('dcr', '   ', 'E_FORMAT', qr/invalid datetime value/,
	      "datetime whitespace value is rejected");
};


# Check the length of char/varchar values. The declared size of these columns is a
# number of characters, regardless of the character set.

subtest 'char and varchar values' => sub {
    
    store_ok('string_val', 'a' x 40, 'a' x 40, "utf8 varchar with 40 ASCII characters");
    store_ok('string_val', $ALPHA x 40, $ALPHA x 40, "utf8 varchar with 40 two-byte characters");
    store_ok('latin1_val', $EACUTE x 40, $EACUTE x 40, "latin1 varchar with 40 accented characters");
    store_ok('greek_val', $ALPHA x 40, $ALPHA x 40, "greek varchar with 40 Greek characters");
    
    reject_ok('string_val', 'a' x 41, 'E_WIDTH', qr/40 characters \(was 41\)/,
	      "utf8 varchar with 41 ASCII characters");
    reject_ok('string_val', $ALPHA x 41, 'E_WIDTH', qr/40 characters \(was 41\)/,
	      "utf8 varchar with 41 two-byte characters");
    reject_ok('latin1_val', $EACUTE x 41, 'E_WIDTH', qr/40 characters \(was 41\)/,
	      "latin1 varchar with 41 accented characters");
    reject_ok('greek_val', $ALPHA x 41, 'E_WIDTH', qr/40 characters \(was 41\)/,
	      "greek varchar with 41 Greek characters");
    
    # With ALLOW_TRUNCATE, values are truncated to the column size in characters.
    
    foreach my $col ( 'string_val', 'latin1_val', 'greek_val' )
    {
	set_truncate($col, 1);
	
	my $char = $col eq 'latin1_val' ? $EACUTE : $ALPHA;
	
	store_ok($col, $char x 45, $char x 40, "$col truncated to 40 characters",
		 { warning => 'W_TRUNC', filter => qr/truncated to 40 characters/ });
	
	set_truncate($col, 0);
    }
};


# Check the length of text values. The size limit of these columns is a number of
# bytes in the column's character set.

subtest 'text values' => sub {
    
    # tinytext in utf8: 255 bytes
    
    store_ok('tinytext_val', 'a' x 255, 'a' x 255, "utf8 tinytext with 255 ASCII characters");
    store_ok('tinytext_val', $ALPHA x 127, $ALPHA x 127,
	     "utf8 tinytext with 127 two-byte characters (254 bytes)");
    
    reject_ok('tinytext_val', 'a' x 256, 'E_WIDTH', qr/255 bytes \(was 256\)/,
	      "utf8 tinytext with 256 ASCII characters");
    reject_ok('tinytext_val', $ALPHA x 128, 'E_WIDTH', qr/255 bytes \(was 256\)/,
	      "utf8 tinytext with 128 two-byte characters (256 bytes)");
    
    # tinytext in greek: 255 bytes, one byte per Greek character. This checks
    # the size computation that is done by the database.
    
    store_ok('greek_text', $ALPHA x 255, $ALPHA x 255,
	     "greek tinytext with 255 Greek characters");
    
    reject_ok('greek_text', $ALPHA x 256, 'E_WIDTH', qr/255 bytes \(was 256\)/,
	      "greek tinytext with 256 Greek characters");
    
    # Text values are not truncated even if ALLOW_TRUNCATE is set.
    
    set_truncate('tinytext_val', 1);
    
    reject_ok('tinytext_val', 'a' x 256, 'E_WIDTH', qr/255 bytes/,
	      "utf8 tinytext is not truncated with ALLOW_TRUNCATE");
    
    set_truncate('tinytext_val', 0);
};


# Check binary values. Byte strings must be stored unchanged, and character
# strings are stored as utf8.

subtest 'binary values' => sub {
    
    store_ok('binary_val', "\xe9\x00\xffA", 'E900FF41', "binary bytes stored unchanged",
	     { hex => 1 });
    store_ok('binary_val', "\xe9" x 40, 'E9' x 40, "binary with 40 high bytes",
	     { hex => 1 });
    store_ok('binary_val', "${ALPHA}B", 'CEB142', "binary character string stored as utf8",
	     { hex => 1 });
    
    reject_ok('binary_val', "\xe9" x 41, 'E_WIDTH', qr/40 bytes \(was 41\)/,
	      "binary with 41 bytes");
    reject_ok('binary_val', $ALPHA x 30, 'E_WIDTH', qr/40 bytes \(was 60\)/,
	      "binary character string longer than 40 bytes in utf8");
};


# Check that a column default is substituted only when the column is mentioned
# with a null value, and not when the column is not mentioned at all. This
# behavior was changed in commit f0115eb8.

subtest 'column defaults' => sub {
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TYPES', { name => 'default1' });
    
    my %cols = pending_columns($edt);
    
    ok( ! exists $cols{unsigned_val}, "default not added for column that is not mentioned" );
    
    $edt->insert_record('EDT_TYPES', { name => 'default2', unsigned_val => undef });
    
    %cols = pending_columns($edt);
    
    is( $cols{unsigned_val}, "'0'", "default substituted for column mentioned with null value" );
    
    ok_no_conditions('all', "no conditions from default handling");
};


# Check values that cannot be represented in the column's character set. These
# should be rejected during validation, rather than by the database.

subtest 'unrepresentable characters' => sub {
  
  TODO: {
	local $TODO = "invalid characters are not yet checked during validation";
	
	my $edt = ok_new_edt;
	
	$edt->insert_record('EDT_TYPES', { name => 'charset1', latin1_val => $ALPHA });
	
	ok_has_one_condition('errors', qr/^E_(?!EXECUTE)/,
			     "Greek character in latin1 column rejected during validation");
	
	$edt->insert_record('EDT_TYPES', { name => 'charset2', string_val => "\N{U+1F600}" });
	
	ok_has_one_condition('errors', qr/^E_(?!EXECUTE)/,
			     "four-byte character in utf8mb3 column rejected during validation");
    }
};


# Definitions for the convenience subroutines used in the tests above
# -------------------------------------------------------------------

# store_ok ( colname, value, expected, label, [options] )
#
# Insert a record into EDT_TYPES with the specified value for the specified
# column, and commit it. Pass a test if the commit succeeds without any
# conditions, and another if the stored value matches $expected. An undefined
# value for $expected means the stored value should be null. A Regexp for
# $expected is matched against the stored value.
#
# Options: 'warning' specifies a condition code that must be the only condition
# generated, with an optional 'filter' regexp. 'hex' means the stored value is
# compared in hexadecimal.

sub store_ok {
    
    my ($colname, $value, $expected, $label, $options) = @_;
    
    $options ||= { };
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $name = 'v' . ++$RECNO;
    
    my $edt = $T->new_edt || return;
    
    $edt->insert_record('EDT_TYPES', { name => $name, $colname => $value });
    
    my $committed = $edt->commit;
    
    my @conditions = $edt->conditions;
    
    if ( $options->{warning} )
    {
	my $filter = $options->{filter} || qr/./;
	
	ok( $committed && @conditions == 1 && $conditions[0] =~ /^$options->{warning}\b/ &&
	    $conditions[0] =~ $filter, "$label: committed with $options->{warning}" ) ||
		diag(@conditions ? join("\n", @conditions) : "no conditions");
    }
    
    else
    {
	ok( $committed && ! @conditions, "$label: committed with no conditions" ) ||
	    diag(@conditions ? join("\n", @conditions) : "commit failed with no conditions");
    }
    
    my $colexpr = $options->{hex} ? "hex($colname)" : $colname;
    
    my ($found, $stored) = $T->dbh->selectrow_array("SELECT 1, $colexpr FROM $TABLE{EDT_TYPES}
						     WHERE name = '$name'");
    
    # If the record was not stored, fail the test. Otherwise a missing record
    # would match an expected null value.
    
    if ( ! $found )
    {
	fail( "$label: stored value" );
	diag( "no record was stored" );
    }
    
    elsif ( ref $expected eq 'Regexp' )
    {
	like( $stored, $expected, "$label: stored value" );
    }
    
    else
    {
	is( $stored, $expected, "$label: stored value" );
    }
}


# reject_ok ( colname, value, code, filter, label )
#
# Insert a record into EDT_TYPES with the specified value for the specified
# column. Pass a test if validation generates exactly one condition, and that
# condition matches the specified code and filter. The transaction is not
# committed.

sub reject_ok {
    
    my ($colname, $value, $code, $filter, $label) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $name = 'r' . ++$RECNO;
    
    my $edt = $T->new_edt || return;
    
    $edt->insert_record('EDT_TYPES', { name => $name, $colname => $value });
    
    ok_has_one_condition($edt, $code, $filter, $label);
}


# pending_columns ( edt )
#
# Return a hash of the column names and (quoted) values that will be stored by
# the latest action.

sub pending_columns {
    
    my ($edt) = @_;
    
    my $cols = $edt->action_ref->column_list;
    my $vals = $edt->action_ref->value_list;
    
    my %result;
    
    @result{@$cols} = @$vals if ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY';
    
    return %result;
}


# set_truncate ( colname, flag )
#
# Set or clear the ALLOW_TRUNCATE property of the specified EDT_TYPES column. The
# table schema must already have been fetched and cached, which will have
# happened by the time this is called.

sub set_truncate {
    
    my ($colname, $flag) = @_;
    
    ETBasicTest->alter_column_property('EDT_TYPES', $colname, ALLOW_TRUNCATE => $flag) ||
	BAIL_OUT "could not alter column property for '$colname'";
}
