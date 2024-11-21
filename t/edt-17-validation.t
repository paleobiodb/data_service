#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the EditTransaction class and the
# EditTransaction::Mod::MariaDB plugin module, using the subclass ETBasicTest
# whose sole purpose is to enable these tests.
# 
# validation.t :
# 
#         Test the methods used for validating records. All action parameters
#         are validated against the corresponding database columns. If the
#         parameter value cannot be stored in the database column, an error or
#         warning condition will be generated.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 9;

use TableDefs qw(set_column_property);

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception ok_new_edt capture_mode last_edt last_result
		  capture_mode ok_captured_output
		  clear_table sql_command sql_fetchrow
		  ok_has_condition ok_no_conditions ok_has_one_condition);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest', 'EDT_TEST');

my $dbh = $T->dbh;

clear_table('EDT_TEST');
    

#  Set up some values to be used in the tests below.

my $long_value = "This value is longer than 40 characters and can be used to check length validation";
my $trunc_value = substr($long_value, 0, 40);


# The definitions for 'vtest', 'cname', and 'clist' are at the end of this file.

sub vtest; sub cvalue; sub clist;


# Check the methods that must be present.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'validate_action', 'validate_special_column') ||
	
	BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check that a valid set of parameters validates correctly.

subtest 'valid parameters' => sub {
    
    my $edt = ok_new_edt;
    
    vtest('insert', 'EDT_TEST', { string_req => 'abc' });
    
    ok_no_conditions("valid parameters for EDT_TEST");
    
    is( clist, 1, "only one stored value" );
    is( cvalue('string_req'), "'abc'", "stored value for string_req" );
};


# Check that parameter fields that do not correspond to columns are caught.
# Fields whose names start with an underscore are ignored unless they have a
# special meaning.

subtest 'bad field names' => sub {
    
    my $edt = ok_new_edt;
    
    vtest('insert', 'EDT_TEST', { string_req => 'abc', foo => 'bar' });
    
    ok_has_one_condition('E_BAD_FIELD', 'foo', "E_BAD_FIELD from invalkd field name");
    
    $edt = ok_new_edt('BAD_FIELDS');
    
    vtest('insert', 'EDT_TEST', { string_req => 'abc', biff => 'baff' });
    
    ok_has_one_condition('W_BAD_FIELD', 'biff', "W_BAD_FIELD from invalid field name with 'BAD_FIELDS'");
    
    vtest('insert', 'EDT_TEST', { string_req => 'abc', _fitz => 'something' });
    
    ok_no_conditions("field name starting with underscore is ignored");
    
    vtest('insert', 'EDT_TEST', { string_req => 'abc', hippo => 58 },
	  { 'FIELD:hippo' => 'ignore' });
    
    ok_no_conditions("directive 'ignore' suppresses bad field condition");
};


# Check that validate_action properly handles validation status. We use bad
# field names to tell us whether the validation process is run or not.

subtest 'validation status' => sub {
    
    my $edt = ok_new_edt;
    
    vtest('insert', 'EDT_TEST', { string_req => 'abc', hippo => 1 },
	  { _vstatus => 'COMPLETE' });
    
    ok_no_conditions("validation does not proceed if already complete");
    
    # vtest('insert', 'EDT_TEST', { string_req => 'abc', hippo => 2 },
    # 	  { _vstatus => 'PENDING' });
    
    # ok_no_conditions("validation does not proceed if pending");
    
    vtest('insert', 'EDT_TEST', { string_req => 'abc', hippo => 3 }, {});
    
    ok_has_one_condition('E_BAD_FIELD', "validation proceeds if not complete");
    
    is( $edt->action_ref->validation_status, 'COMPLETE',
	"validation status set to COMPLETE when validation completes" );
};


# Make sure that the basic set of column handling directives have the proper semantics.

subtest 'column handling: ignore, pass, unquoted' => sub {
    
    my $edt = ok_new_edt;
    
    # Start with a baseline test. Check that a validation with the default
    # 'validate' directive produces the expected result.
    
    vtest('update', 'EDT_TEST', { string_req => 'def', signed_val => -48 },
	  { string_req => 'validate', signed_val => 'validate' });
    
    ok_no_conditions;
    
    is( cvalue('string_req'), $dbh->quote('def'), "stored value for 'string_req'" );
    is( cvalue('signed_val'), $dbh->quote('-48'), "stored value for 'signed_val'" );
    
    # Then check that 'ignore' generates a warning and prevents the value from
    # being stored.
    
    vtest('update', 'EDT_TEST', { string_req => 'def', signed_val => -48 },
	  { signed_val => 'ignore' });
    
    ok_has_one_condition('W_DISCARDED');
    
    is( cvalue('signed_val'), undef, "no stored value for 'signed_val' with 'ignore'" );
    is( clist, 1, "only one stored value" );
    
    # If no value is specified for the ignored field, no warning should be
    # generated.
    
    vtest('update', 'EDT_TEST', { string_req => 'def', signed_val => 45 },
	  { string_val => 'ignore' });
    
    ok_no_conditions("no conditions because ignored field was not given a value");
	
    # Now check 'pass' and 'unquoted'. They make use of the value and
    # consequently do not generate any warnings.
    
    vtest('update', 'EDT_TEST', { string_val => $long_value},
	  { string_val => 'pass' });
    
    ok_no_conditions;
    
    is( cvalue('string_val'), $dbh->quote($long_value), "stored value for 'string_val' with 'pass'" );;
    
    vtest('update', 'EDT_TEST', { string_val => $long_value},
	  { string_val => 'unquoted' });
    
    ok_no_conditions;
    
    is( cvalue('string_val'), $long_value, "stored value for 'string_val' with 'unquoted'" );;
};


# The column handling directive 'copy' has different semantics for 'insert',
# 'update', and 'replace'.

subtest 'column handling: copy' => sub {
    
    my $edt = ok_new_edt;
    
    vtest( 'insert', 'EDT_TEST', { string_req => 'apple', string_val => 'banana' },
	   { string_val => 'copy' } );
    
    ok_has_one_condition('W_DISCARDED', "field 'string_val' overridden by copy on 'insert'");
    
    is( cvalue('string_req'), $dbh->quote('apple'), "stored value for string_req" );
    is( cvalue('string_val'), undef, 
	"stored value for string_val is undef with 'copy' and 'insert'" );
    
    vtest( 'update', 'EDT_TEST', { string_req => 'pear', string_val => 'peach' },
	   { string_val => 'copy' } );
    
    ok_has_one_condition('W_DISCARDED', "field 'string_val' overridden by 'copy' on 'update'");
    
    is( cvalue('string_req'), $dbh->quote('pear'), "stored value for string_req" );
    is( cvalue('string_val'), 'string_val', 
	"stored value for 'string_val' is unquoted column name with 'copy' and 'update'" );
    
    # In order to test 'replace' we must have a row in the database first.
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req, string_val, signed_val)
		 VALUES ('8', 'huckleberry', 'orange', '24')");
    
    if ( last_result )
    {
	vtest('replace', 'EDT_TEST', { test_no => 8, string_req => 'mango' },
	      { string_val => 'copy' });
	
	ok_no_conditions( "no warning on 'string_val' => 'copy' because no value was specified" );
	
	is( cvalue('string_req'), $dbh->quote('mango'), "stored value for string_req" );
	is( cvalue('string_val'), $dbh->quote('orange'), 
	    "stored value for 'string_val' is existing record value with 'copy' and 'replace'" );
	is( cvalue('signed_val'), undef, "no stored value for 'signed_val' with 'replace'" );
    }
};


# Check that special handling directives are dealt with properly.

subtest 'column handling: special directives' => sub {
    
    my $edt = ok_new_edt;
    
    # Set the handling directive for the column 'dmd' to 'ts_modified'. The type
    # of this column is timestamp, so this should generate 'NOW()' as the
    # assigned value.
    
    $edt->handle_column('EDT_TYPES', 'dmd', 'ts_modified');
    
    vtest('insert', 'EDT_TYPES', { name => 'glipx' });
    
    ok_no_conditions;
    
    is( cvalue('dmd'), "NOW()", "generated timestamp for field 'dmd' with 'ts_modified'" );
    
    # Now try the same with a column that doesn't have the proper type. Look for
    # E_BAD_DIRECTIVE. We use capture_mode to capture the error output so that
    # it doesn't appear in the test stream.
    
    $edt->handle_column('EDT_TYPES', 'signed_val', 'ts_modified');
    
    capture_mode(1);
    
    $edt->{breakpoint}{colname}{signed_val} = 1;
    $DB::single=1;
    
    vtest('insert', 'EDT_TYPES', { name => 'flaxx' });
    
    capture_mode(0);
    
    ok_has_one_condition( 'all', qr/E_BAD_DIRECTIVE.*signed_val.*type/i,
			  "E_BAD_DIRECTIVE from directive mismatch with column type" );
    
    ok_captured_output(qr/column type/, "error output from directive mismatch with column type" );
    
    # Finally, register a directive that has no handler and then try to use it.
    # Look for E_BAD_DIRECTIVE with a different message.
    
    my $edt = ok_new_edt;
    
    ETBasicTest->register_directives('not_a_valid_directive');
    
    vtest('insert', 'EDT_TEST', { string_req => 'abhq' },
	  { signed_val => 'not_a_valid_directive' });
    
    ok_has_one_condition( 'all', qr/E_BAD_DIRECTIVE.*signed_val.*not_a_valid_directive/,
			  "E_BAD_DIRECTIVE from directive with no handler" );
    
};


# Check that REQUIRED and NOT_NULL columns are properly validated.

subtest 'required and not null' => sub {
    
    my $edt = ok_new_edt;
    
    # ETBasicTest specifically sets the 'REQUIRED' property for 'string_req' in
    # EDT_TEST. This requires a non-empty value.
    
    vtest('insert', 'EDT_TEST', { string_val => 'required field missing' });
    
    ok_has_one_condition('E_REQUIRED');
    
    vtest('insert', 'EDT_TEST', { string_req => '', signed_val => 0 });
    
    ok_has_one_condition('E_REQUIRED');
    
    # After dropping the property 'REQUIRED', the column reverts to 'not null'.
    # The empty string is an acceptable value in this case.
    
    ETBasicTest->alter_column_property('EDT_TEST', string_req => REQUIRED => '');
    
    vtest('insert', 'EDT_TEST', { string_val => 'required field missing' });
    
    ok_has_one_condition('E_REQUIRED');
    
    vtest('insert', 'EDT_TEST', { string_req => '' });
    
    ok_no_conditions;
};




# Definitions for the convenience subroutines used in the tests above
# -------------------------------------------------------------------


# vtest( operation, table, parameters, directives )
# 
# Set up a call to validate_action with the specified parameters, handling
# directives, and validation status.

sub vtest {
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift @_ : $T->last_edt;
    
    my ($operation, $table, $params, $directives) = @_;
    
    my $action = $edt->_new_action($operation, $table, $params);
    
    my $validation_flag;
    
    if ( ref $directives eq 'HASH' )
    {
	foreach my $key ( keys $directives->%* )
	{
	    if ( $key eq '_vstatus' )
	    {
		$action->{validation} = $directives->{$key};
	    }
	    
	    elsif ( $key eq '_vfinal' )
	    {
		$validation_flag = 'FINAL';
	    }
	    
	    else
	    {
		$edt->handle_column('&_', $key, $directives->{$key});
	    }
	}
    }
    
    $edt->validate_against_schema($action, $operation, $table);
}


# cvalue ( colname, [flag] )
# 
# Return the value that will be stored in the database for the specified column.

sub cvalue {
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift @_ : $T->last_edt;
    
    my ($colname, $exists) = @_;
    
    my $cols = $edt->{current_action}->column_list;
    my $vals = $edt->{current_action}->value_list;
    
    if ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' )
    {
	foreach my $i ( 0 .. $cols->$#* )
	{
	    if ( $cols->[$i] eq $colname )
	    {
		return $vals->[$i];
	    }
	}
    }
    
    return undef;
}


# clist ( )
# 
# Return the list of column names for which values will be stored.

sub clist {
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift @_ : $T->last_edt;
    
    my $cols = $edt->{current_action}->column_list;
    
    if ( ref $cols eq 'ARRAY' )
    {
	return $cols->@*;
    }
    
    else
    {
	return ();
    }
}


