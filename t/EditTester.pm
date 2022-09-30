#
# EditTester.pm: a class for running tests on EditTransaction.pm and its subclasses.
# 



use strict;
use feature 'unicode_strings';
use feature 'fc';

package EditTester;

use Scalar::Util qw(blessed);
use Carp qw(croak);
use List::Util qw(any);
use Test::More;
use parent 'Exporter';

use CoreFunction qw(connectDB configData);
use TableDefs qw(%TABLE $TEST_DB init_table_names enable_test_mode get_table_property);
use Permissions;
use EditTransaction;
use TestTables;

use namespace::clean;

our (@EXPORT_OK) = qw(connect_to_database ok_eval ok_exception ok_last_result
		      select_tester current_tester
		      set_default_table default_table target_class
		      invert_mode diag_mode
		      ok_output ok_no_output diag_output diag_lines clear_output
		      last_result last_result_list last_edt clear_edt
		      ok_new_edt ok_condition_count ok_has_condition
		      ok_has_one_condition ok_no_conditions ok_no_errors 
		      ok_no_warnings ok_has_one_error ok_has_one_warning
		      ok_has_error ok_has_warning ok_diag is_diag
		      ok_action ok_failed_action ok_commit ok_failed_commit ok_rollback
		      clear_table ok_found_record ok_no_record ok_count_records
		      get_table_name sql_command sql_selectrow count_records fetch_records);
		      


# If $INVERT_MODE is set to true, the outcome of certain tests is reversed. We
# use this to check that tests will fail under certain circumstances. If either
# $INVERT_MODE or $DIAG_MODE is set to true, $DIAG_OUTPUT collects diagnostic
# output.

our $INVERT_MODE = 0;
our $DIAG_MODE = 0;
our $DIAG_OUTPUT = '';

# Keep track of the last EditTester instance created by this module and the last
# EditTransaction instance created by this module. This allows all of the
# routines below to be called as subroutines instead of methods.

our $LAST_TESTER;


# new ( options, [default_table] )
# 
# Create a new EditTester instance associated with a subclass of EditTransaction.

sub new {
    
    my ($class, $options, $edt_table) = @_;
    
    my ($dbh, $edt_class, $debug_mode, $errlog_mode);
    
    if ( ref $options eq 'HASH' )
    {
	$dbh = $options->{dbh} if $options->{dbh};
	$edt_class = $options->{class} if $options->{class};
	$edt_table = $options->{table} if $options->{table};
	$debug_mode = $options->{debug_mode} if defined $options->{debug_mode};
	$errlog_mode = $options->{errlog_mode} if defined $options->{errlog_mode};
    }
    
    elsif ( $options && ! ref $options )
    {
	$edt_class = $options;
    }
    
    $debug_mode = 1 if @ARGV && $ARGV[0] eq 'debug';
    $errlog_mode = 1 if @ARGV && $ARGV[0] eq 'errlog';
    
    $debug_mode = 1 if $ENV{DEBUG};
    $errlog_mode = 1 if $ENV{PRINTERR};
    
    # If a class was specified, load corresponding module if it was not already loaded.
    
    if ( $edt_class && ! $edt_class->isa('EditTransaction') )
    {
	my $class = $edt_class;
	$class =~ s{::}{/}g;
	
	require "${class}.pm";
    }
    
    # Make sure this class actually is a subclass of EditTransaction.
    
    unless ( $edt_class && $edt_class->isa('EditTransaction') )
    {
	croak "You must specify the name of a class that is a subclass of EditTransaction.";
    }
    
    # Then make sure that we can connect directly to the database. We use the parameters from the
    # file config.yml in the main directory. If this is done successfully, make sure that the
    # connection uses STRICT_TRANS_TABLES mode and also that all communication is done using
    # utf8.
    
    $dbh ||= connect_to_database();
    
    # If we are using the class ETBasicTest, then switch over the edt_test table and related
    # tables to the test database. Then double-check that this has been done.
    
    if ( $edt_class && $edt_class eq 'ETBasicTest' )
    {
	enable_test_mode('edt_test');
	
	unless ( $TEST_DB && $TABLE{EDT_TEST} =~ /$TEST_DB/ )
	{
	    diag("Could not enable test mode for 'EDT_TEST'.");
	    BAIL_OUT;
	}
    }
    
    my $instance = { dbh => $dbh,
		     edt_class => $edt_class,
		     edt_table => $edt_table,
		     debug_mode => $debug_mode,
		     errlog_mode => $errlog_mode,
		   };
    
    bless $instance, $class;
    
    $LAST_TESTER = $instance;
    
    return $instance;
}


# connect_to_database ( )
# 
# Attempt to connect to a database using the attributes in the file "config.yml". If the
# connection succeeds, return the database handle. Otherwise, call BAIL_OUT.
# 
# This subroutine is intended to be exported and called with no arguments, but you can also call
# it as a method.

sub connect_to_database {
    
    my $dbh;
    
    eval {
	$dbh = connectDB("config.yml");
	$dbh->do('SET @@SQL_MODE = CONCAT(@@SQL_MODE, ",STRICT_TRANS_TABLES")');
	$dbh->do('SET CHARACTER SET utf8');
	$dbh->do('SET NAMES utf8');
	init_table_names(configData, 1);
    };
    
    if ( $@ )
    {
	diag("Database connection failed: $@");
	BAIL_OUT;
    }
    
    elsif ( ! $dbh )
    {
	diag("Database connection failed");
	BAIL_OUT;
    }
    
    return $dbh;
}


# dbh ( )
#
# Return the database handle for the current tester.

sub dbh {
    
    return $_[0]->{dbh};
}


# debug_mode ( [value ] )
#
# Return the status of the debug flag on this object. If a true or false value is given, set the
# flag accordingly.

sub debug_mode {

    if ( @_ > 1 )
    {
	$_[0]->{debug_mode} = ( $_[1] ? 1 : 0 );
    }
    
    return $_[0]->{debug_mode};
}


# errlog_mode ( [value ] )
#
# Return the status of the debug flag on this object. If a true or false value is given, set the
# flag accordingly.

sub errlog_mode {

    if ( @_ > 1 )
    {
	$_[0]->{errlog_mode} = ( $_[1] ? 1 : 0 );
    }
    
    return $_[0]->{errlog_mode};
}


# tester_args ( ... )
# 
# Determine which EditTester instance other routines should use.

sub tester_args {
    
    if ( ref $_[0] && $_[0]->isa('EditTester') )
    {
	return @_;
    }
    
    elsif ( $LAST_TESTER )
    {
	return $LAST_TESTER, @_;
    }
    
    else
    {
	croak "You must first create an EditTester instance";
    }
}


# set_table ( table_name )
#
# Set the default table for all new EditTransactions created by this tester object.

sub set_default_table {

    my ($T, $table) = &tester_args;
    
    $T->{edt_table} = $table;
}


sub default_table {

    my ($T) = &tester_args;
    
    return $T->{edt_table};
}


sub target_class {
    
    my ($T) = &tester_args;
    
    return $T->{edt_class};
}


sub trim_exception {

    my ($msg) = @_;
    
    return $msg;
}


sub debug_line {
    
    my ($T, $line) = @_;

    print STDERR " ### $line\n" if $T->{debug_mode};
}


sub debug_skip {

    my ($T) = @_;
    
    print STDERR "\n" if $T->{debug_mode};
}


# new_edt ( perms, options )
#
# Create a new EditTest object. The CREATE allowance is specified by default.

sub new_edt {
    
    my $T = ref $_[0] && $_[0]->isa('EditTester') ? shift @_ : $LAST_TESTER;
    
    croak "You must first create an EditTester instance" unless $T;
    
    # croak "You must specify a permission as the first argument" unless $perm;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my (@allowances);
    my $CREATE = 1;
    
    # If defaults were given when this tester instance was created, fetch them now.
    
    my $edt_table = $T->{edt_table};
    my $edt_class = $T->{edt_class} || croak "EditTester instance has an empty class name";
    my $edt_perm;
    
    # Now iterate through the arguments given.
    
    while ( my $entry = shift @_ )
    {
	if ( $entry eq 'table' )
	{
	    $edt_table = shift @_;
	    croak "invalid table name '$edt_table'" unless $edt_table && ! ref $edt_table;
	}
	
	elsif ( $entry eq 'permission' )
	{
	    $edt_perm = shift @_;
	    croak "you must specify a permission" unless $edt_perm;
	}
	
	elsif ( $entry eq 'class' )
	{
	    $edt_class = shift @_;
	    croak "invalid class '$edt_class'" unless $edt_class && ! ref $edt_class &&
		$edt_class->isa('EditTransaction');
	}
	
	elsif ( ref $entry eq 'HASH' )
	{
	    foreach my $k ( keys $entry->%* )
	    {
		# Special case 'class', 'permission', and 'table'.
		
		if ( $k =~ /^class$|^table$|^permission$/ )
		{
		    $edt_class = $entry->{$k} if $k eq 'class';
		    $edt_table = $entry->{$k} if $k eq 'table';
		    $edt_perm = $entry->{$k} if $k eq 'permission';
		}
		
		# Otherwise, if the hash key has a true value then add that allowance.
		
		elsif ( $entry->{$k} )
		{
		    push @allowances, $k;
		}
		
		# If the hash key has a false value, add the negation or remove the default.
		
		elsif ( $k eq 'CREATE' )
		{
		    $CREATE = 0;
		}
		
		else
		{
		    push @allowances, "NO_$k";
		}
	    }
	}
	
	elsif ( $entry eq 'NO_CREATE' )
	{
	    $CREATE = 0;
	}
	
	elsif ( $entry )
	{
	    push @allowances, $entry;
	}
    }
    
    # Add the default CREATE unless it was turned off.
    
    unshift @allowances, 'CREATE' if $CREATE;
    
    # Turn on debug mode if 'debug' was given as an argument to the entire test. Turn off silent
    # mode if 'errlog' was given as an argument to the entire test.
    
    push @allowances, 'DEBUG_MODE' if $T->{debug_mode};
    push @allowances, 'SILENT_MODE' if defined $T->{errlog_mode} && $T->{errlog_mode} == 0;
    
    # If a table or a permission was specified, start with a hash that specifies those values.
    
    my $options;
    
    if ( $edt_table || $edt_perm || @allowances )
    {
	$options = { };
	$options->{table} = $edt_table if $edt_table;
	$options->{permission} = $edt_perm if $edt_perm;
	$options->{allows} = \@allowances if @allowances;
    }
    
    # If we are able to create a new edt, pass the test. Otherwise, fail it.
    
    my $edt = eval { $T->_new_edt($edt_class, $options) };
    
    if ( $edt )
    {
	pass("created edt");
	return $edt;
    }
    
    else
    {
	diag($@) if $@;
	fail("created edt");
	return;
    }
}


# _new_edt ( perms, options )
#
# Do the work of creating a new EditTest object.

sub _new_edt {
    
    my ($T, $edt_class, @args) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $T->{last_edt} = undef;
    $T->{last_exception} = undef;
    
    my $edt = $edt_class->new($T->dbh, @args);
    
    if ( $edt )
    {
	$T->{last_edt} = $edt;
	return $edt;
    }
    
    else
    {
	$T->{last_edt} = undef;
	return;
    }
}


sub last_edt {
    
    my ($T) = &tester_args;
    
    return ref $T->{last_edt} && $T->{last_edt}->isa('EditTransaction') ? $T->{last_edt} : undef;
}


sub clear_edt {
    
    my ($T) = &tester_args;
    
    $T->{last_edt} = undef;
}


sub select_tester {
    
    ($LAST_TESTER) = &tester_args;
}


sub current_tester {
    
    return $LAST_TESTER;
}


sub ok_new_edt {
    
    goto &new_edt;
}



# Subroutines to make testing more straightforward
# ------------------------------------------------

# ok_eval ( subroutine, [ignore_flag], [label] )
# 
# The first argument is a subroutine (typically anonymous) that will be called
# inside an eval. Any errors that occur will be reported. The second argument is
# the name for this test, which defaults to a generic label. If the result of
# the eval is true, the test will pass. Otherwise it will fail. This subroutine
# can be exported and called directly, or it can be called as a method. 
# 
# If the flag 'IGNORE' is specified, ignore the result and pass the test if no
# exception occurred.

our (@EVAL_RESULT);

sub ok_eval {
    
    # If the first argument is an EditTester instance, ignore it.
    
    shift @_ if ref $_[0] eq 'EditTester';
    
    my $sub = shift @_;
    
    croak "First argument must be a subroutine reference" unless ref $sub eq 'CODE';
    
    # If the next argument is 'IGNORE', then return true regardless of the
    # result. 
    
    my $ignore_result;
    
    if ( $_[0] eq 'IGNORE' )
    {
	$ignore_result = 1;
	shift @_;
    }
    
    my $label = shift @_ || 'eval succeeded';
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    @EVAL_RESULT = ();
    
    eval { @EVAL_RESULT = $sub->() };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag_lines("EXCEPTION : $msg");
	ok( $INVERT_MODE eq 'eval', $label );
    }
    
    elsif ( $ignore_result || $EVAL_RESULT[0] )
    {
	ok( $INVERT_MODE ne 'eval', $label )
    }
    
    else
    {
	ok( $INVERT_MODE eq 'eval', $label );
    }
}


# ok_exception ( subroutine, expected, [label] )
# 
# The first argument is a subroutine (typically anonymous) that will be called inside an eval. The
# second argument is a regexp to be matched against an exception if one is thrown during the eval.
# It may also be '1', which matches any exception. The third argument is the name of this test,
# defaulting to a generic label. If no exception is thrown, the test fails. If an exception is
# thrown, the test passes if the exception matches the regexp and fails otherwise. This subroutine
# can be exported and called directly, or it can be called as a method.

sub ok_exception {
    
    # If the first argument is an EditTester instance, ignore it.
    
    shift @_ if ref $_[0] eq 'EditTester';
    
    my ($sub, $expected, $label) = @_;
    
    croak "First argument must be a subroutine reference" unless ref $sub eq 'CODE';
    croak "Second argument must be a regexp reference or '*'" 
	unless ref $expected eq 'Regexp' || $expected eq '1' || $expected eq '*';
    
    $label ||= 'a matching exception was thrown';
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    @EVAL_RESULT = ();
    
    eval { @EVAL_RESULT = $sub->() };
    
    if ( $@ && ( $expected eq '1' || $expected eq '*' || $@ =~ $expected ) )
    {
	ok( $INVERT_MODE ne 'eval', $label );
    }
    
    elsif ( $@ )
    {
	my $msg = trim_exception($@);
	diag_lines("", "EXCEPTION : $msg", "EXPECTED  : $expected");
	ok( $INVERT_MODE eq 'eval', $label);
    }
    
    else
    {
	diag_lines("NO EXCEPTION WAS THROWN");
	ok( $INVERT_MODE eq 'eval', $label);
    }
}


# last_result ( )
# 
# Return the result from the subroutine executed by the last ok_eval or ok_exception.

sub last_result {
    
    return $EVAL_RESULT[0];
}


sub last_result_list {
    
    shift @_ if ref $_[0] eq 'EditTester';
    
    if ( @_ )
    {
	return $EVAL_RESULT[$_[0]];
    }
    
    else
    {
	return @EVAL_RESULT;
    }
}


# ok_last_result ( [label] )

sub ok_last_result {
    
    # If the first argument is an EditTester instance, ignore it.
    
    shift @_ if ref $_[0] eq 'EditTester';
    
    my $label = shift @_ || "last result was true";
    
    if ( $INVERT_MODE )
    {
	ok( !$EVAL_RESULT[0], $label );
    }
    
    else
    {
	ok( $EVAL_RESULT[0], $label );
    }
}


# Inverting the sense of the tests and capturing diagnostic output
# ----------------------------------------------------------------

# In order to use this module for testing, it is first necessary to check that
# the testing routines themselves work properly. To that end, unit tests of this
# module can set $INVERT_MODE and then call the testing routines with arguments
# that purposely cause the tests to fail and/or generate debugging output.
# 
# All of the routines below this section use 'diag_lines' to generate diagnostic
# output. When $INVERT_MODE is set, this output is captured in the $INVERT_DIAG
# variable, which can then be checked to make sure the expected output was
# produced.
# 
# The other thing that happens when $INVERT_MODE is set is that the results of all
# tests are reversed. A test that would fail succeeds, and a test that would
# succeed fails. This is used by unit tests to check that the routines below
# fail when they should and not when they shouldn't.

# invert_mode ( [value] )
# 
# If a value is specified, set $INVERT_MODE to true or false accordingly.
# Otherwise, just return its current value. Note that $INVERT_MODE is a global
# variable in this module, so it will affect all calls to all instances until
# cleared.

sub invert_mode {
    
    shift if ref $_[0] eq 'EditTester';
    
    if ( @_ )
    {
	$INVERT_MODE = $_[0];
    }
    
    return $INVERT_MODE;
}


# diag_mode ( [value] )
# 
# If a value is specified, set $DIAG_MODE to true or false accordingly.
# Otherwise, just return its current value. This can be used to collect
# diagnostic output when $INVERT_MODE is not active.

sub diag_mode {
    
    shift if ref $_[0] eq 'EditTester';
    
    if ( @_ )
    {
	$DIAG_MODE = ( $_[0] ? 1 : 0 );
    }
    
    return $DIAG_MODE;
}


# diag_lines ( string... )
# 
# If $INVERT_MODE is true, append the given strings to $INVERT_DIAG. Otherwise,
# print them out as diagnostic messages.

sub diag_lines {
    
    shift if ref $_[0] && $_[0]->isa('EditTester');
    
    foreach ( @_ )
    {
	if ( $DIAG_MODE || $INVERT_MODE )
	{ 
	    $DIAG_OUTPUT .= "$_\n"; 
	}
	
	else { 
	    diag $_;
	}
    }
}


# ok_output ( regexp, [label] )
# 
# Match the value of $DIAG_OUTPUT against the specified regexp. If it passes, pass
# a test with the specified label. This method is intended for use only in
# testing the EditTester class.

sub ok_output {
    
    shift if ref $_[0] && $_[0]->isa('EditTester');
    
    my $testre = shift;
    
    croak "You must specify a regular expression" unless ref $testre eq 'Regexp';
    
    my $label = shift || "diagnostic output matches regular expression";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( $DIAG_OUTPUT =~ $testre )
    {
	ok( $INVERT_MODE ne 'output', $label );
    }
    
    else
    {
	my $temp = $DIAG_OUTPUT;
	diag_lines("", "DIAGNOSTIC OUTPUT WAS:", $temp,
		   "EXPECTED : $testre");
	ok( $INVERT_MODE eq 'output', $label );
    }
}


# ok_no_output ( [label] )
# 
# If $DIAG_OUTPUT is empty, pass a test with the specified label. This method is
# intended for us only in testing the EditTester class. This subroutine always
# clears $DIAG_OUTPUT regardless of the result, so that subsequent tests will
# start with an empty baseline.

sub ok_no_output {
    
    shift if ref $_[0] && $_[0]->isa('EditTester');
    
    my $label = shift || "diagnostic output is empty";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( $DIAG_OUTPUT eq '' )
    {
	ok( $INVERT_MODE ne 'output', $label );
    }
    
    else
    {
	my $temp = $DIAG_OUTPUT;
	$DIAG_OUTPUT = '';
	diag_lines("", "DIAGNOSTIC OUTPUT WAS:", $temp);
	ok( $INVERT_MODE eq 'output', $label );
    }
}


# diag_output ( )
# 
# Return the value of $INVERT_DIAG for further testing. This method is intended
# for use only in testing the EditTester class.

sub diag_output {

    return $DIAG_OUTPUT;
}


# clear_output ( )
#
# Clear $INVERT_DIAG and return the value it had. This method is intended for use only in testing
# the EditTester class.

sub clear_output {

    my $retval = $DIAG_OUTPUT;
    $DIAG_OUTPUT = '';
    return $retval;
}


# Methods for testing error and warning conditions
# ------------------------------------------------

# _edt_args ( [tester], [edt] )
# 
# If no EditTester instance is specified, use $LATEST_TESTER. If that is
# undefined, throw an exception. If no EditTransaction instance is specified,
# use the latest EditTransaction created using that EditTester instance. If
# there is none, throw an exception. Return the two instances, followed by any
# remaining arguments.

sub _edt_args {
    
    # Look for an EditTester argument.
    
    my $T = ref $_[0] && $_[0]->isa('EditTester') ? shift @_ : $LAST_TESTER;
    
    croak "You must first create an EditTester instance" unless $T;
    
    # If the first argument is a reference to an EditTransaction instance, use
    # that. Otherwise, use the last instance create.
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift @_ : $T->{last_edt};
    
    croak "You must first create an EditTransaction instance" unless $edt;
    
    # If we get here, we have both an EditTester and an EditTransaction. Return
    # them, followed by any remaining arguments.
    
    return ($T, $edt, @_);
}


# _condition_args ( [selector], [type], [filter], [label] )
# 
# Construct and return a list ($selector, $type, $filter, $label) from the
# arguments, with appropriate defaults. All of the arguments are optional, and
# this method will properly distinguish whichever ones are specified and will
# fill in defaults for any left out. The parameter $selector defaults to
# '_', while $type defaults to 'all'. There is no default for $label.
# 
# Accepted values for $selector are:
# 
#   all        all conditions
#   main       conditions not associated with any action
#   latest     conditions associated with the latest action
#   &...       an action reference string
# 
# Accepted values for $type are:
# 
#   all        all conditions
#   errors     all error conditions
#   warnings   all warning conditions
#   fatal      error conditions that cause the EditTransaction to terminate.
#   nonfatal   warning conditions plus error conditions that were demoted to warnings
#                by PROCEED or NOTFOUND
# 
# Accepted values for $filter are:
# 
# - Any regexp, which will be matched against the entire condition string.
# 
# - A string that looks like 'E_SOME_ERROR', 'F_SOME_ERROR', 'C_SOME_CAUTION',
#   or 'W_SOME_WARNING'.  This selects conditions with a matching code. 
# 
# - A string the looks like 'EF_SOME_ERROR', which will select conditions that
#   contain either 'E_SOME_ERROR' or 'F_SOME_ERROR'.


sub _condition_args {
    
    # Check for selector and/or type arguments. If not given, they default to 'all'.
    
    my $selector = '_';
    my $type = 'all';
    
    while ( $_[0] && $_[0] =~ / ^ (?: all | latest | main | &.* | _ |
				      errors | warnings | fatal | nonfatal ) $ /xsi )
    {
	if ( $_[0] =~ / ^ (?: errors | warnings | fatal | nonfatal ) $ /xsi )
	{
	    croak "conflicting type arguments: '$type', '$_[0]'"
		unless $type eq 'all' || $type eq $_[0];
	    
	    $type = shift @_;
	}
	
	else
	{
	    $selector = '_' if $selector eq 'latest';
	    
	    croak "conflicting selector arguments: '$selector', '$_[0]'" 
		unless $selector eq '_' || $selector eq $_[0];
	    
	    $selector = shift @_;
	}
    }
    
    # If the next argument is a single word in all lower case, assume it is a
    # misspelled selector or type argument and throw an exception.
    
    if ( $_[0] && $_[0] =~ /^[a-z]+$/ )
    {
	croak "unknown selector or type '$_[0]'";
    }
    
    # If the next argument is a regular expression or looks like a condition
    # code, use it as the filter. The default filter is /./, which selects all
    # conditions. 
    
    my $filter;
    
    if ( defined $_[0] && $_[0] ne '' )
    {
	if ( ref $_[0] eq 'Regexp' )
	{
	    $filter = shift @_;
	}
	
	elsif ( $_[0] && $_[0] =~ qr{ ^ ([CEFW]_[A-Z0-9_]+) $ }xs )
	{
	    $filter = qr{ ^ $1 \b }xs;
	    shift @_;
	}
	
	elsif ( $_[0] && $_[0] =~ qr{ ^ EF_ ([A-Z0-9_]+) $ }xs )
	{
	    $filter = qr{ ^ [EF]_ $1 \b }xs;
	    shift @_;
	}
	
	elsif ( ref $_[0] || $_[0] !~ /[a-z ]/ )
	{
	    croak "unrecognized filter '$_[0]': must be a condition code or a regexp";
	}
    }
    
    # If we already have one filter and the next argument is a regexp,
    # add it to the existing filter.
    
    if ( ref $filter eq 'Regexp' && ref $_[0] eq 'Regexp' )
    {
	my $second = shift @_;
	
	$filter = qr{ $filter .* $second }xs;
    }
    
    $filter ||= qr/./;
    
    # Remove any empty arguments from the end.
    
    while ( @_ && ! $_[-1] )
    {
	pop @_;
    }
    
    # The last argument from what remains, if any, will be used as the label
    # (name) for this test.  If not given, a default will be used (see below).
    
    return ($selector, $type, $filter, $_[-1]);
}


# _action_args ( [refstring], [status], [label] )
# 
# Construct and return a list ($refstring, $status, $label) with appropriate
# defaults. 

sub _action_args {
    
    my $refstring;
    my $status;
    
    # If the first argument is an action reference string or '_', extract it.
    
    if ( $_[0] && $_[0] =~ /^&|^_$/ )
    {
	$refstring = shift @_;
    }
    
    # If the first argument is an action status code, extract it.
    
    if ( $_[0] && $_[0] =~ qr{ ^ (?: pending|executed|failed|aborted|skipped) $ }xs )
    {
	$status = shift @_;
    }
    
    # If we didn't find a status code and the next argument is a single
    # lowercase word, it probably represents a misspelling of a status code.
    
    if ( $status eq undef && $_[0] =~ qr{ ^ [a-z]+ $ }xs )
    {
	croak "unknown status code '$_[0]'";
    }
    
    # Remove any empty arguments from the end.
    
    while ( @_ && ! $_[-1] )
    {
	pop @_;
    }
    
    # The last argument from what remains, if any, will be used as the label
    # (name) for this test.  If not given, a default will be used (see below).
    
    return ($refstring, $status, $_[-1])
}


# conditions ( [edt], [selector], [type], [filter] )
# 
# Return a list of conditions matching the arguments.

sub conditions {

    my ($T, $edt, @rest) = &_edt_args;
    
    my ($selector, $type, $filter) = &_condition_args(@rest);
    
    return grep { $_ =~ $filter } $edt->conditions($selector, $type);
}


# ok_condition_count ( expected_count, [edt], [selector], [type], [filter], [label] )
# 
# If if the latest edt (or the specified edt) has exactly $expected_count
# conditions that match $selector, $type and $filter, pass a test labeled by
# $label and return true.  Otherwise, fail the test and return false. In the
# latter case, list the matching conditions to the diagnostic output stream.  If
# the expected count argument is '*', pass the test if there is at least one
# matching condition.
# 
# If you are explicitly specifying an edt instance, it may be placed either
# before or after the expected count.
# 
# If $INVERT_MODE is true, invert the test result. If the expected count does not
# match, pass the test. If it does match, fail the test and list the matching
# conditions from the edt being tested to the diagnostic output stream. This
# allows for unit tests to check situations where the call is supposed to fail.
# All of the tests defined below work the same way.

sub ok_condition_count {
    
    my ($T, $edt, @rest) = &_edt_args;
    
    my $expected_count = shift @rest;
    
    # The expected count must be supplied.
    
    unless ( defined $expected_count && $expected_count =~ /^\d+$|^[*]$/ )
    {
	croak "You must specify the expected condition count as either natural number or '*'";
    }
    
    # If an EditTransaction instance is specified after that, use it instead of
    # the default one.
    
    if ( ref $rest[0] && $rest[0]->isa('EditTransaction') )
    {
	$edt = shift @rest;
    }
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return _ok_condition_count($edt, $expected_count, $selector, $type, $filter,
			       $label || "condition count is $expected_count");
}


sub _ok_condition_count {
    
    my ($edt, $expected_count, $selector, $type, $filter, $label) = @_;
    
    # If the selector is a refstring, first make sure it is valid.
    
    if ( $selector =~ /^&/ && ! $edt->has_action($selector) )
    {
	diag_lines("No action matching '$selector' was found");
	ok($INVERT_MODE, $label);
	return $INVERT_MODE;
    }
    
    # Get a list of all the conditions matching $selector, $type, and $filter.
    
    my @conditions = grep { $_ =~ $filter } $edt->conditions($selector, $type);
    
    # If the number of matching conditions equals the expected count, pass the
    # test and return true. If the expected count is '*', pass the test if
    # there is at least one. Invert this if $INVERT_MODE is true.
    
    if ( $expected_count eq '*' && @conditions )
    {
	ok(!$INVERT_MODE, $label);
	return !$INVERT_MODE;
    }
    
    elsif ( $expected_count ne '*' && @conditions == $expected_count )
    {
	ok(!$INVERT_MODE, $label);
	return !$INVERT_MODE;
    }
    
    # Otherwise, fail the test and return false. Invert this if $INVERT_MODE is
    # true, and additionally print out all conditions (matching or not) to the
    # diagnostic stream. If no conditions are present, print out a message
    # stating that.
    
    else
    {
	ok($INVERT_MODE, $label);
	my @all_conditions = $edt->conditions($selector, $type, $filter);
	push @all_conditions, "No matching conditions were found" unless @all_conditions;
	diag_lines(@all_conditions);
	return $INVERT_MODE;
    }
}


# ok_no_conditions ( [edt], [selector], [type], [filter], [label] )
# 
# If the latest edt (or the specified edt) has no conditions that match
# $selector, $type, and $filter, pass a test with the specified label and return
# true. Otherwise, fail the test and return false. In the latter case, list all
# matching conditions to the diagnostic output stream.

sub ok_no_conditions {

    my ($T, $edt, @rest) = &_edt_args;
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return _ok_condition_count($edt, 0, $selector, $type, $filter,
			       $label || "no matching conditions");
    
    # return $T->_ok_no_conditions($edt, $selector, $type, $filter,
    # 				 $label || "no matching conditions");
}


# sub _ok_no_conditions {

#     my ($T, $edt, $selector, $type, $filter, $label) = @_;
    
#     # If we find any matching conditions then fail the test, print out the conditions that were
#     # found, and return false. Invert this if $INVERT_MODE is true.
    
#     if ( my @conditions = grep { $_ =~ $filter } $edt->conditions($selector, $type) )
#     {
# 	ok($INVERT_MODE, $label);
# 	diag_lines(@conditions);
# 	return $INVERT_MODE;
#     }
    
#     # Otherwise, pass the test and return true. Invert this if $INVERT_MODE is true.

#     else
#     {
# 	ok(!$INVERT_MODE, $label);
# 	return !$INVERT_MODE;
#     }
# }


# ok_has_condition ( [edt], [selector], [type], [filter], [label] )
# 
# If the latest edt (or the specified edt) has at least one condition that
# matches $selector, $type, and $filter, pass a test with the specified label
# and return true. Otherwise, fail the test and return false. In the latter
# case, list all conditions that match $selector and $type to the diagnostic
# output stream.

sub ok_has_condition {
    
    my ($T, $edt, @rest) = &_edt_args;
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
        
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return _ok_condition_count($edt, '*', $selector, $type, $filter,
			       $label || "found matching condition");
    
    # return $T->_ok_has_condition($edt, $selector, $type, $filter,
    # 				 $label || "found matching condition");
}


# sub _ok_has_condition {

#     my ($T, $edt, $selector, $type, $filter, $label) = @_;
    
#     # If we find any matching conditions, pass the test and return true. Invert this if $INVERT_MODE
#     # is true.
    
#     my @conditions = $edt->conditions($selector, $type);
    
#     if ( any { $filter } @conditions )
#     {
# 	ok(!$INVERT_MODE, $label);
# 	return !$INVERT_MODE;
#     }
    
#     # Otherwise, fail the test and return false. Invert this if $INVERT_MODE is true. If any
#     # conditions were found that don't match the filter, print them out as diagnostic messages.
    
#     else
#     {
# 	ok($INVERT_MODE, $label);
# 	diag_lines(@conditions);
# 	return $INVERT_MODE;
#     }
# }


# ok_has_one_condition ( [edt], [selector], [type], [filter], [label] )
# 
# If the latest edt (or the specified edt) has exactly one condition that
# matches $selector and $type, and that condition also matches $filter, pass a
# test with the specified label and return true.  Otherwise, fail the test and
# return false. In the latter case, list all conditions matching $selector and
# $type to the diagnostic output stream.
# 
# To be clear, this test will fail if there is more than one condition that
# matches $selector and $type, even if only one of them matches the filter. The
# intended use for this test is to check that (for example) the latest action
# has only one error condition and it is the one we expect it to have.

sub ok_has_one_condition {
    
    my ($T, $edt, @rest) = &_edt_args;
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
        
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return _ok_has_one_condition($edt, $selector, $type, $filter,
				 $label || "found one matching condition");
}


sub _ok_has_one_condition {
    
    my ($edt, $selector, $type, $filter, $label) = @_;
    
    # If the selector is a refstring, first make sure it is valid.
    
    if ( $selector =~ /^&/ && ! $edt->has_action($selector) )
    {
	diag_lines("No action matching '$selector' was found");
	ok($INVERT_MODE, $label);
	return $INVERT_MODE;
    }
    
    # If we find exactly one condition that matches $selector and $type, and it also matches the
    # filter, pass the test and return true. Invert this if $INVERT_MODE is true.
    
    my @conditions = $edt->conditions($selector, $type);
    
    if ( @conditions == 1 && ref $filter eq 'Regexp' )
    {
	if ( $conditions[0] =~ $filter )
	{
	    ok(!$INVERT_MODE, $label);
	    return !$INVERT_MODE;
	}
    }
    
    # If no filter was given, pass the test and return true if the number of
    # conditions that match $selector and $type is exactly 1. Similarly invert
    # this if $INVERT_MODE is true.
    
    elsif ( @conditions == 1 )
    {
	ok(!$INVERT_MODE, $label);
	return !$INVERT_MODE;
    }
    
    # Otherwise, fail the test and return false. Invert this if $INVERT_MODE is true. If there are
    # any conditions that match $selector and $type, print them out as diagnostic
    # messages. Otherwise, print out a message stating that no matching conditions were found.
    
    else
    {
	ok($INVERT_MODE, $label);
	push @conditions, "No matching conditions were found" unless @conditions;
	diag_lines(@conditions);
	return $INVERT_MODE;
    }
}


# The following test subroutines are shortcuts, specialized for particular
# condition types.

sub ok_no_errors {

    my ($T, $edt, @rest) = &_edt_args;
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return _ok_condition_count($edt, 0, $selector, 'errors', $filter,
			       $label || "no matching errors");
    
    # return $T->_ok_no_conditions($edt, $selector, 'errors', $filter,
    # 				 $label || "no matching errors");
}


sub ok_no_warnings {

    my ($T, $edt, @rest) = &_edt_args;
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return _ok_condition_count($edt, 0, $selector, 'warnings', $filter,
			       $label || "no matching warnings");
    
    # return $T->_ok_no_conditions($edt, $selector, 'warnings', $filter,
    # 				 $label || "no matching warnings");
}


sub ok_has_error {

    my ($T, $edt, @rest) = &_edt_args;
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    _ok_condition_count($edt, '*',  $selector, 'errors', $filter,
			$label || "found matching error");
}


sub ok_has_warning {

    my ($T, $edt, @rest) = &_edt_args;
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    _ok_condition_count($edt, '*', $selector, 'warnings', $filter,
			$label || "found matching warning");
}


sub ok_has_one_error {

    my ($T, $edt, @rest) = &_edt_args;
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return _ok_has_one_condition($edt, $selector, 'errors', $filter,
				 $label || "found one matching error");
}


sub ok_has_one_warning {

    my ($T, $edt, @rest) = &_edt_args;
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return _ok_has_one_condition($edt, $selector, 'warnings', $filter,
				 $label || "found one matching warning");
}


# The following tests work just like the corresponding tests from Test::More,
# except that when they fail, all matching conditions from the edt being tested
# are listed to the diagnostic output stream. This will (hopefully) help the
# person doing the testing to diagnose what is wrong.


# ok_diag ( test_value, [edt], [selector], [type], [filter], [label] )
# 
# This method passes a test with the specified label if $test_value is true, and
# fails it if $test_value is false. In the latter case, all conditions matching
# the rest of the arguments are listed to the diagnostic output stream. Of
# course, the last EditTransaction instance created will be used unless one is
# explicitly specified.
# 
# If you are explicitly specifying an edt instance, it can be placed either before
# or after the test value.

sub ok_diag {

    my ($T, $edt, @rest) = &_edt_args;
    
    croak "You must specify a result to test and at least one other argument" unless @rest  >= 2;
    
    my $test_value = shift @rest;
    
    # if we find an EditTransaction instance after the test value, use it
    # instead of the default one.
    
    if ( ref $rest[0] && $rest[0]->isa('EditTransaction') )
    {
	$edt = shift @rest;
    }
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
    
    $label ||= "test value is true";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( $test_value )
    {
	ok( !$INVERT_MODE, $label );
	return !$INVERT_MODE;
    }
    
    else
    {
	ok( $INVERT_MODE, $label );
	diag_conditions($edt, $selector, $type, $filter);
	return $INVERT_MODE;
    }
}


sub ok_result {

    goto &ok_diag;
}


# is_diag ( test_value, expected, [edt], [selector], [type], [filter], [label] )
# 
# This method passes a test with the specified label if $test_value eq
# $expected, and fails it otherwise. In the latter case, all conditions matching
# the rest of the arguments are listed to the diagnostic output stream. Of
# course, the last EditTransaction instance created will be used unless one is
# explicitly specified.
#
# If you are explicitly specifying an edt instance, it can be placed either
# before or after the test_value, expected pair.

sub is_diag {
    
    my ($T, $edt, @rest) = &_edt_args;
    
    croak "You must specify two arguments to compare and at least one other argument" unless @rest >= 3;
    
    my $test_value = shift @rest;
    my $expected = shift @rest;
    
    # if we find an EditTransaction instance after the test value, use it.
    
    if ( ref $rest[0] && $rest[0]->isa('EditTransaction') )
    {
	$edt = shift @rest;
    }
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
    
    unless ( $label )
    {
	$label = $INVERT_MODE ? "test value is not the expected value" :
	    "test value is the expected value";
    }
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( !$INVERT_MODE && is($test_value, $expected, $label) )
    {
	return 1;
    }
    
    elsif ( !$INVERT_MODE )
    {
	diag_conditions($edt, $selector, $type, $filter);
	return 0;
    }
    
    elsif ( isnt($test_value, $expected, $label) )
    {
	diag_conditions($edt, $selector, $type, $filter);
	return 1;
    }
    
    else
    {
	return 0;
    }
}


# The following subroutines list all matching conditions to the diagnostic
# output stream. They can also be called as methods, just like all the test
# subroutines can.

sub diag_conditions {
    
    my ($T, $edt, @rest) = &_edt_args;
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
    
    my @conditions = grep { $_ =~ $filter } $edt->conditions($selector, $type, $filter);
    
    diag_lines(@conditions);
}


sub diag_errors {

    my ($T, $edt, @rest) = &_edt_args;
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
    
    my @errors = grep { $_ =~ $filter } $edt->conditions($selector, 'errors', $filter);
    
    diag_lines(@errors);
}


sub diag_warnings {

    my ($T, $edt, @rest) = &_edt_args;
    
    my ($selector, $type, $filter, $label) = &_condition_args(@rest);
    
    my @warnings = grep { $_ =~ $filter } $edt->conditions($selector, 'warnings', $filter);
    
    diag_lines(@warnings);
}



# Methods for testing the success or failure of transactions and actions
# ----------------------------------------------------------------------

# ok_action ( [edt], [refstring], [status], [label] )
# 
# If the latest action created for the latest edt (or the specified edt) has
# completed or can proceed, pass a test with the specified label. Otherwise,
# fail the test and list the action status and all associated conditions to the
# diagnostic output stream.
# 
# If a status code is given, the test passes only if the action has the
# specified status. If an action refstring is given, test that action instead of
# the latest one.

sub ok_action {

    my ($T, $edt, @rest) = &_edt_args;
    
    my ($refstring, $status, $label) = &_action_args(@rest);
    
    my $wrong_status;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $refstring ||= '_';
    
    unless( $edt->has_action($refstring) )
    {
	ok($INVERT_MODE, $label);
	diag_lines("No matching action was found");
	return $INVERT_MODE;
    }
    
    if ( $status )
    {
	$wrong_status = $edt->action_status($refstring) ne $status;
    }
    
    unless ( $label )
    {
	my $oplabel = $edt->action_operation($refstring);
	$oplabel = 'action' unless $oplabel =~ qr{ ^ (?: insert|delete|update|replace) $ }xs;
	
	$label = $status ? "$oplabel has status '$status'" : "$oplabel can proceed";
    }
    
    if ( $edt->action_ok($refstring) && ! $wrong_status )
    {
	ok(!$INVERT_MODE, $label);
	return !$INVERT_MODE;
    }
    
    else
    {
	ok($INVERT_MODE, $label);
	my $status = $edt->action_status($refstring);
	diag_lines("action status is '$status'");
	diag_conditions($edt, $refstring);
	return $INVERT_MODE;
    }
}


# ok_has_action ( [edt], [refstring], [label] )
# 
# This subroutine can be used to test if a specified action is present,
# regardless of its status.

sub ok_has_action {
    
    my ($T, $edt, @rest) = &_edt_args;
    
    my ($refstring, $status, $label) = &_action_args(@rest);
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $refstring ||= '_';
    
    if ( $edt->has_action($refstring) )
    {
	ok(!$INVERT_MODE, $label);
	return !$INVERT_MODE;
    }
    
    else
    {
	ok($INVERT_MODE, $label);
	diag_lines("No matching action was found");
	return $INVERT_MODE;
    }
}


# ok_failed_action ( [edt], [refstring], [status], [label] )
# 
# If the latest action created for the latest edt (or the specified edt) has
# failed, skipped, or aborted, pass a test with the specified label. Otherwise,
# fail the test and list the action status and all associated conditions to the
# diagnostic output stream.
# 
# If a status code is given, the test passes only if the action has the
# specified status. If an action refstring is given, test that action instead of
# the latest one.

sub ok_failed_action {
    
    my ($T, $edt, @rest) = &_edt_args;
    
    my ($refstring, $status, $label) = &_action_args(@rest);
    
    my $wrong_status;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $refstring ||= '_';
    
    unless ( $edt->has_action($refstring) )
    {
	ok($INVERT_MODE, $label);
	diag_lines("No matching action was found");
	return $INVERT_MODE;
    }
    
    if ( $status )
    {
	$wrong_status = $edt->action_status($refstring) ne $status;
    }
    
    unless ( $label )
    {
	my $oplabel = $edt->action_operation($refstring);
	$oplabel = 'action' unless $oplabel =~ qr{ ^ (?: insert|delete|update|replace) $ }xs;
	
	$label = $status ? "$oplabel has status '$status'" : "$oplabel can proceed";
    }
    
    if ( ! $edt->action_ok($refstring) && ! $wrong_status )
    {
	ok(!$INVERT_MODE, $label);
	return !$INVERT_MODE;
    }
    
    else
    {
	ok($INVERT_MODE, $label);
	my $status = $edt->action_status;
	diag_lines("action status is '$status'");
	diag_conditions($edt, $refstring);
	return $INVERT_MODE;
    }
}


# ok_commit ( [edt], [label] )
# 
# Attempt to commit the default edt (or the specified edt). If the commit
# succeeds, pass a test with the specified label. Otherwise, fail the test. In
# the latter case, list the transaction status and *all* conditions to the
# diagnostic output stream.

sub ok_commit {

    my ($T, $edt, @rest) = &_edt_args;
    
    my $label = $rest[-1] || "transaction committed successfully";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( $edt->commit )
    {
	ok(!$INVERT_MODE, $label);
	return !$INVERT_MODE;
    }
    
    else
    {
	ok($INVERT_MODE, $label);
	my $status = $edt->status;
	diag_lines("transaction status is '$status'");
	diag_conditions($edt);
	return $INVERT_MODE;
    }
}


# ok_failed_commit ( [edt], [label] )
# 
# Attempt to commit the default edt (or the specified edt). If the commit
# fails, pass a test with the specified label. Otherwise, fail the test. In
# the latter case, list the transaction status and *all* conditions to the
# diagnostic output stream.

sub ok_failed_commit {

    my ($T, $edt, @rest) = &_edt_args;
    
    my $label = $rest[-1] || "transaction committed successfully";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( $edt->commit )
    {
	ok($INVERT_MODE, $label);
	my $status = $edt->status;
	diag_lines("transaction status is '$status'");
	diag_conditions($edt);
	return $INVERT_MODE;
    }
    
    else
    {
	ok(!$INVERT_MODE, $label);
	return !$INVERT_MODE;
    }
}


# ok_rollback ( [edt], [label] )
# 
# Attempt to roll back the default edt (or the specified edt). If the rollback
# succeeds, pass a test with the specified label. Otherwise, fail the test. In
# the latter case, list the transaction status and *all* conditions to the
# diagnostic output stream.

sub ok_rollback {

    my ($T, $edt, @rest) = &_edt_args;
    
    my $label = $rest[-1] || "transaction committed successfully";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( $edt->rollback )
    {
	ok(!$INVERT_MODE, $label);
	return !$INVERT_MODE;
    }
    
    else
    {
	ok($INVERT_MODE, $label);
	my $status = $edt->status;
	diag_lines("transaction status is '$status'");
	diag_conditions($edt);
	return $INVERT_MODE;
    }
}


# Methods for testing the existence or nonexistence of records in the database
# -----------------------------------------------------------------------------


# clear_table ( table )
# 
# Deletes all records from the specified table. If the table has an
# AUTO_INCREMENT key, the sequence is reset to 1. This record throws an
# exception if it fails, bringing the test to a halt.

sub clear_table {
    
    my $T = ref $_[0] && $_[0]->isa('EditTester') ? shift @_ : $LAST_TESTER;
    
    croak "You must first create an EditTester instance" unless $T;
    
    my $table = shift @_;
    
    my $dbh = $T->dbh;
    
    my $tablename = $T->get_table_name($table);
    
    my $label = "cleared table $table";
    
    my $sql = "DELETE FROM $tablename";
    
    $T->debug_line($sql);
    
    my $result = $dbh->do($sql);
    
    $T->debug_line("Deleted $result rows") if $result;
    
    $sql = "ALTER TABLE $tablename AUTO_INCREMENT = 1";
    
    $T->debug_line($sql);
    
    eval {
	$dbh->do($sql);
    };
    
    $T->debug_skip;
    
    pass($label);
    
    return;
}


# get_table_name ( table )
# 
# Return the real name of the database table specified by the argument, or else
# throw an exception.

sub get_table_name {
    
    my $T = ref $_[0] && $_[0]->isa('EditTester') ? shift @_ : $LAST_TESTER;
    
    croak "You must first create an EditTester instance" unless $T;
    
    my $specifier = shift @_ || croak "you must specify a table";
    
    my $tableinfo = $T->{edt_class}->table_info_ref($specifier, $T->{dbh}) ||
	croak "unknown table '$specifier'";
    
    return $tableinfo->{QUOTED_NAME} || croak "could not determine table name";
}


# sql_command ( command, [label] )
# 
# Attempt to execute the specified sql command. If it executes without throwing
# an exception, pass a test with the specified label and return the result.
# Otherwise, fail the test and print out the exception if one occurred. The command
# result can also be retrieved using the 'last_result' method.

sub sql_command {
    
    my $T = ref $_[0] && $_[0]->isa('EditTester') ? shift @_ : $LAST_TESTER;
    
    croak "you must first create an EditTester instance" unless $T;
    
    my $sql = shift @_ || croak "you must specify an SQL command";
    
    croak "you must specify a valid SQL command" if ref $sql;
    
    my $label = shift @_ || "sql command executed successfully";
    
    my $dbh = $T->dbh;
    
    # Substitute any table specifiers with with the corresponding table names.
    
    $sql =~ s/<<([\w_-]+)>>/$T->get_table_name($1)/ge;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $T->debug_line($sql);
    
    @EVAL_RESULT = ();
    
    eval {
	@EVAL_RESULT = $dbh->do($sql);
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	$T->{last_exception} = $msg;
	diag_lines("SQL STMT: $sql");
	diag_lines("EXCEPTION: $msg");
	ok( $INVERT_MODE, $label );
	return undef;
    }
    
    else
    {
	ok( !$INVERT_MODE, $label );
	return $EVAL_RESULT[0];
    }
}


# sql_selectrow ( command, [label] )
# 
# Attempt to execute the specified sql query and fetch the result using
# 'fetchrow_array'. If it executes without throwing an exception, pass a test
# with the specified label and return the resulting values. Otherwise, fail the
# test and print out the exception. The command result can be retrieved using
# the 'last_result' method.

sub sql_selectrow {
    
    my $T = ref $_[0] && $_[0]->isa('EditTester') ? shift @_ : $LAST_TESTER;
    
    croak "You must first create an EditTester instance" unless $T;
    
    my $sql = shift @_ || croak "you must specify a valid SELECT statement";
    
    croak "you must specify a valid SELECT statement" if ref $sql;
    
    my $label = shift @_ || "select statement executed successfully";
    
    my $dbh = $T->dbh;
    
    # Substitute any table specifiers with with the corresponding table names.
    
    $sql =~ s/<<([\w_-]+)>>/$T->get_table_name($1)/ge;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $T->debug_line($sql);
    
    @EVAL_RESULT = ();
    
    eval {
	@EVAL_RESULT = $dbh->selectrow_array($sql);
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag_lines("SQL STMT: $sql");
	diag_lines("EXCEPTION: $msg");
	ok( $INVERT_MODE, $label );
	return ();
    }
    
    else
    {
	ok( !$INVERT_MODE, $label );
	return @EVAL_RESULT;
    }
}


# ok_found_record ( table, expr, [label] )
# 
# If a row is found in the specified table that matches the specified
# expression, pass a test with the specified label. Otherwise, fail the test. If
# the sql expression throws an exception, fail the test and print out the
# exception.

sub ok_found_record {
    
    my $T = ref $_[0] && $_[0]->isa('EditTester') ? shift @_ : $LAST_TESTER;
    
    croak "You must first create an EditTester instance" unless $T;
    
    my ($table, $expr, $label) = @_;
    
    my $dbh = $T->{dbh};
    
    # Check arguments
    
    croak "you must specify a table" unless $table;
    croak "you must specify a valid SQL expression" if ref $expr;
    $label ||= 'found at least one record';
    
    $expr = '1' if !$expr || $expr eq '*';
    
    my $tableinfo = $T->{edt_class}->table_info_ref($table, $dbh) ||
	croak "unknown table '$table'";
    
    my $tablename = $tableinfo->{QUOTED_NAME} || croak "could not determine table name";
    
    # If the given expression is a single decimal number, assume it is a key.
    
    if ( $expr =~ /^\d+$/ )
    {
	my $key_name = $tableinfo->{PRIMARY_KEY} || 
	    croak "could not determine primary key for table '$table'";
	$expr = "$key_name = '$expr'";
    }
    
    # Otherwise, substitute any table specifiers with the corresponding table names.
    
    else
    {
	$expr =~ s/<<([\w_-]+)>>/$T->get_table_name($1)/ge;
    }
    
    # Execute the SQL expression and test the result.
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $sql = "SELECT COUNT(*) FROM $tablename WHERE $expr";
    
    $T->debug_line($sql);
    
    my $count;
    
    eval {
	($count) = $dbh->selectrow_array($sql);
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	$T->{last_exception} = $msg;
	diag_lines("SQL STMT: $sql");
	diag_lines("EXCEPTION: $msg");
    }
    
    elsif ( $T->debug_mode )
    {
	$T->debug_line("Returned $count rows");
	$T->debug_skip;
    }
    
    if ( $INVERT_MODE )
    {
	ok( !$count, $label );
    }
    
    else
    {
	ok( $count, $label);
    }
}


# ok_no_record ( table, expr, [label] )
# 
# If a row is found in the specified table that matches the specified
# expression, fail a test with the specified label. Otherwise, pass the test. If
# the sql expression throws an exception, fail the test and print out the
# exception.

sub ok_no_record {
    
    my $T = ref $_[0] && $_[0]->isa('EditTester') ? shift @_ : $LAST_TESTER;
    
    croak "You must first create an EditTester instance" unless $T;
    
    my ($table, $expr, $label) = @_;
    
    my $dbh = $T->{dbh};
    
    # Check arguments
    
    croak "you must specify a table" unless $table;
    croak "you must specify a valid SQL expression" if ref $expr;
    $label ||= 'record was absent';
    
    $expr = '1' if !$expr || $expr eq '*';
        
    my $tableinfo = $T->{edt_class}->table_info_ref($table, $dbh) ||
	croak "unknown table '$table'";
    
    my $tablename = $tableinfo->{QUOTED_NAME} || croak "could not determine table name";
    
    # If the given expression is a single decimal number, assume it is a key.
    
    if ( $expr =~ /^\d+$/ )
    {
	my $key_name = $tableinfo->{PRIMARY_KEY} || 
	    croak "could not determine primary key for table '$table'";
	$expr = "$key_name = '$expr'";
    }
    
    # Otherwise, substitute any table specifiers with the corresponding table names.
    
    else
    {
	$expr =~ s/<<([\w_-]+)>>/$T->get_table_name($1)/ge;
    }
    
    # Execute the SQL expression and test the result.
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $sql = "SELECT COUNT(*) FROM $tablename WHERE $expr";
    
    $T->debug_line($sql);
    
    my $count;

    eval {
	($count) = $dbh->selectrow_array($sql);
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	$T->{last_exception} = $msg;
	diag_lines("SQL STMT: $sql");
	diag_lines("EXCEPTION: $msg");
	$count = 1;		# ensure test fails, unless in INVERT_MODE
    }
    
    elsif ( $T->debug_mode )
    {
	$T->debug_line("Returned $count rows");
	$T->debug_skip;
    }
    
    if ( $INVERT_MODE )
    {
	ok( $count, $label );
    }
    
    else
    {
	ok( !$count, $label);
    }
}


# ok_count_records ( expected, table, expr, [label] )
# 
# If the count of records in the specified table that match the specified
# expression is the expected number, pass a test with the specified label.
# Otherwise, fail the test.  If the sql expression throws an exception, fail the
# test and print out the exception.

sub ok_count_records {
    
    my $T = ref $_[0] && $_[0]->isa('EditTester') ? shift @_ : $LAST_TESTER;
    
    croak "You must first create an EditTester instance" unless $T;
    
    my ($count, $table, $expr, $label) = @_;
    
    my $dbh = $T->{dbh};
    
    croak "invalid count '$count'" unless defined $count && $count =~ /^\d+$/;
    croak "you must specify a valid SQL expression" if ref $expr;
    
    $label ||= "found expected number of records";
    
    $expr = '1' if !$expr || $expr eq '*';
    
    my $tablename = $T->get_table_name($table);
    
    # my $tableinfo = $T->{edt_class}->table_info_ref($table, $dbh) ||
    # 	croak "unknown table '$table'";
    
    # my $tablename = $tableinfo->{QUOTED_NAME} || croak "could not determine table name";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    # Substitute any table specifiers with the corresponding table names and
    # then generate a SELECT expression.
    
    $expr =~ s/<<([\w_-]+)>>/$T->get_table_name($1)/ge;
    
    my $sql = "SELECT count(*) FROM $tablename WHERE $expr";
    
    $T->debug_line($sql);
    
    my $result;
    
    eval {
	($result) = $dbh->selectrow_array($sql);
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	$T->{last_exception} = $msg;
	diag_lines("SQL STMT: $sql");
	diag_lines("EXCEPTION: $msg");
    }
    
    if ( defined $result && $result == $count )
    {
	ok(!$INVERT_MODE, $label);
    }
    
    else
    {
	$result //= "undef";
	
	ok($INVERT_MODE, $label);
	
	diag_lines("     got: $result");
	diag_lines("expected: $count");
	
	return $INVERT_MODE;
    }
}


# count_records ( expr )
# 
# Return the number of records matching the specified expression. If the sql
# query succeeds, pass a test and return the result which may be zero. If an
# exception occurs, then print out the exception, fail the test, and return
# undef. 

sub count_records {

    my $T = ref $_[0] && $_[0]->isa('EditTester') ? shift @_ : $LAST_TESTER;
    
    croak "You must first create an EditTester instance" unless $T;
    
    my ($table, $expr) = @_;
    
    my $dbh = $T->dbh;
    
    my $tablename = $T->get_table_name($table);
    
    croak "you must specify a valid SQL expression" if ref $expr;
    
    my $label = "sql record count executed successfully";
    
    # my $tableinfo = $T->{edt_class}->table_info_ref($table, $dbh) ||
    # 	croak "unknown table '$table'";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    # Substitute any table specifiers with the corresponding table names and
    # then generate a SELECT expression.
    
    my $where_clause = "";
    
    if ( $expr )
    {
	$expr =~ s/<<([\w_-]+)>>/$T->get_table_name($1)/ge;
	$where_clause = "WHERE $expr";
    }
    
    my $sql = "SELECT count(*) FROM $tablename $where_clause";
    
    $T->debug_line($sql);
    
    my $result;
    
    eval {
	($result) = $dbh->selectrow_array($sql);
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	$T->{last_exception} = $msg;
	diag_lines("SQL STMT: $sql");
	diag_lines("EXCEPTION: $msg");
	ok( $INVERT_MODE, $label );
	return $INVERT_MODE ? 1 : undef;
    }
    
    else
    {
	ok( !$INVERT_MODE, $label );
	return $result;
    }
}


# fetch_records ( table, selector, return... )
# 
# Fetch one or more records or column values from the database. If any return
# values are generated, pass a test and return them. Otherwise, fail the test
# and return the empty list. If an exception occurred, print it to the
# diagnostic output stream.
# 
# Accepted values for $selector include:
# 
# - A single key value
# 
# - A listref containing key values
# 
# - A valid SQL expression
# 
# - The empty string or '*', which will select all records in the table.
# 
# Accepted values for $return include:
# 
#  row        If this is followed by a list of column names and a matching row is
#             found, a list of corresponding column values will be returned. If no
#             column names are specified, the row will be returned as a hashref.
#  
#  column     This must be followed by a single column name. The values of that
#             column in all matching rows will be returned as a single list, to
#             a limit of 50.
#  
#  keyvalues  Like 'column', but returns the primary key value from each record.
#             No additional argument is required.
#  
#  records    Matching records will be returned as a list of hashrefs, to a
#             limit of 50. This is the default if no $return specification is given.

sub fetch_records {
    
    my $T = ref $_[0] && $_[0]->isa('EditTester') ? shift @_ : $LAST_TESTER;
    
    croak "You must first create an EditTester instance" unless $T;
    
    my ($table, $selector, $return, @rest) = @_;
    
    my $dbh = $T->dbh;
    
    croak "you must specify a table" unless $table;
    
    my $tableinfo = $T->{edt_class}->table_info_ref($table, $dbh) ||
	croak "unknown table '$table'";
    
    my $tablename = $tableinfo->{QUOTED_NAME} || croak "could not determine table name";
    
    my $label = "fetch_records had a nonempty result";
    
    my $select_clause = "*";
    my $where_clause = "";
    my $limit_clause = "LIMIT 50";
    my $fetch_method = 'selectall_arrayref';
    my $return_word = "values";
    my @extra;
    
    # Determine which records to fetch. If the second argument is a listref,
    # it should be a list of primary key values.
    
    if ( ref $selector eq 'ARRAY' )
    {
	my $key_name = $tableinfo->{PRIMARY_KEY} || 
	    croak "could not determine primary key for table '$table'";
    
	my @key_list;
	
	foreach my $k ( @$selector )
	{
	    next unless defined $k;
	    #croak "keys cannot be refs" if ref $k;
	    $k =~ s/^\w+[:]//;
	    push @key_list, $dbh->quote($k);
	}
	
	if ( @key_list )
	{
	    my $key_string = join(',', @key_list);
	    
	    $where_clause = "WHERE $key_name in ($key_string)";
	}
	
	else
	{
	    diag_lines("No valid keys were specified");
	    ok( $INVERT_MODE, $label );
	}
    }
    
    # Any other reference is an error.
    
    elsif ( ref $selector )
    {
	croak "invalid selector '$selector'";
    }
    
    # If it is a string consisting only of word characters and hyphens, assume
    # it is a single key value.
    
    elsif ( defined $selector && $selector =~ /^[a-zA-Z0-9_-]+$/ )
    {
	my $key_name = $tableinfo->{PRIMARY_KEY} || 
	    croak "could not determine primary key for table '$table'";
	
	my $quoted = $dbh->quote($selector);
	
	$where_clause = "WHERE $key_name=$quoted";
    }
    
    # If it is a non-empty expression, substitute any table specifiers with the
    # corresponding table names. If it is empty or '*', select the whole table
    # subject to the 50-row limit.
    
    elsif ( defined $selector && $selector ne '' && $selector ne '*' )
    {
	$selector =~ s/<<([\w_-]+)>>/$T->get_table_name($1)/ge;
	
	$where_clause = "WHERE $selector";
    }
    
    # Next, determine what should be returned. If the third argument is 'row',
    # return a list of values from one row using selectrow_array. The next
    # argument should be a list of column names. If not specified, the entire
    # row will be returned.
    
    if ( $return eq 'row' )
    {
	$limit_clause = 'LIMIT 1';
	
	my $column_list;
	
	if ( @rest && ref($rest[0]) eq 'ARRAY' )
	{
	    $label = $rest[1] || "fetch_records row had a nonempty result";
	    $column_list = join ',', grep /\w/, $rest[0]->@*;
	}
	
	else
	{
	    $column_list = join ',', grep /\w/, @rest;
	}
	
	if ( $column_list )
	{
	    $select_clause = $column_list;
	    $fetch_method = 'selectrow_arrayref';
	}
	
	else
	{
	    $fetch_method = 'selectrow_hashref';
	}
    }
    
    # If the third argument is 'column', return a list of values using
    # selectcol_arrayref. The next argument should be a single column name.
    
    elsif ( $return eq 'column' )
    {
	$fetch_method = 'selectcol_arrayref';
	$return_word = 'rows';
	
	if ( $rest[0] )
	{
	    $select_clause = $rest[0];
	    $label = $rest[1] || "fetch_records column had a nonempty result";
	}
	
	else
	{
	    croak "you must specify the name of the column to fetch";
	}
    }
    
    # If the third argument is 'key', return a list of primary key values.
    
    elsif ( $return eq 'keyvalues' )
    {
	my $key_name = $tableinfo->{PRIMARY_KEY} || 
	    croak "could not determine primary key for table '$table'";
	
	$fetch_method = 'selectcol_arrayref';
	$select_clause = $key_name;
	$return_word = 'rows';
	
	$label = $rest[0] || "fetch_records keyvalues had a nonempty result";
    }
    
    # Otherwise, the default is to fetch a list of rows using
    # selectall_arrayref.
    
    else
    {
	croak "invalid return type '$return'" if $return && $return ne 'records';
	
	if ( $return eq 'records' )
	{
	    $label = $rest[0] || "fetch_records records had a nonempty result";
	}
	
	$return_word = 'rows';
    }
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $sql = "SELECT $select_clause FROM $tablename $where_clause $limit_clause";
    
    $T->debug_line($sql);
    
    if ( $fetch_method eq 'selectall_arrayref' || $fetch_method eq 'selectrow_hashref' )
    {
	push @extra, { Slice => { } };
    }
    
    my $results;
    
    eval {
	$results = $dbh->$fetch_method($sql, @extra);
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	$T->{last_exception} = $msg;
	diag_lines("SQL STMT: $sql");
	diag_lines("EXCEPTION: $msg");
	ok( $INVERT_MODE, $label );
	return $INVERT_MODE ? 1 : ();
    }
    
    elsif ( ref $results eq 'ARRAY' && $results->@* )
    {
	$T->debug_line("Returned " . scalar(@$results) . " $return_word");
	$T->debug_skip;
	
	ok( !$INVERT_MODE, $label );
	return $INVERT_MODE ? () : @$results;
    }
    
    elsif ( ref $results eq 'HASH' )
    {
	$T->debug_line("Returned 1 row");
	$T->debug_skip;
	
	ok( !$INVERT_MODE, $label );
	return $INVERT_MODE ? undef : $results;
    }
    
    else
    {
	$T->debug_line("Returned no results");
	$T->debug_skip;
	
	ok( $INVERT_MODE, $label);
	return $INVERT_MODE ? 1 : ();
     }
}


# Methods for keeping track of records that were operated on
# ----------------------------------------------------------

sub inserted_keys {
    
    my ($T, $edt) = @_;

    $edt //= $T->{last_edt};
    
    return unless $edt;
    return $edt->inserted_keys;
}


sub updated_keys {
    
    my ($T, $edt) = @_;

    $edt //= $T->{last_edt};
    
    return unless $edt;
    return $edt->updated_keys;
}


sub replaced_keys {
    
    my ($T, $edt) = @_;

    $edt //= $T->{last_edt};
    
    return unless $edt;
    return $edt->replaced_keys;
}


sub deleted_keys {
    
    my ($T, $edt) = @_;

    $edt //= $T->{last_edt};
    
    return unless $edt;
    return $edt->deleted_keys;
}


# Methods for handling test tables
# --------------------------------

# establish_test_tables ( )
# 
# Create or re-create the tables necessary for the tests we want to run. If the subclass of
# EditTransaction that we are using has an 'establish_test_tables' method, then call
# it. Otherwise, call TestTables::establish_test_tables, which copies the schemas for the
# specified table group from the main database to the test database.

sub establish_test_tables {
    
    my ($T) = shift @_ || $LAST_TESTER;
    
    if ( $T->{edt_class} && $T->{edt_class}->can('establish_test_tables') )
    {
	diag("Establishing test tables for class '$T->{edt_class}'.");
	
	eval {
	    $T->{edt_class}->establish_test_tables($T->dbh);
	};
	
	if ( $@ )
	{
	    my $msg = trim_exception($@);
	    diag("Could not establish tables. Message was: $msg");
	    BAIL_OUT("Cannot proceed without the proper test tables.");
	}
    }
    
    else
    {
	my ($table_group, $debug) = @_;
	
	$debug = 'test' if $T->{debug_mode};
	
	TestTables::establish_test_tables($T->dbh, $table_group, $debug);
    }
}


# fill_test_table ( )
#
# Fill the specified table in the test database with the contents of the same table in the main
# database, optionally filtered by some expression.

sub fill_test_table {

    my ($T, $table_specifier, $expr, $debug) = @_;
    
    $debug = 'test' if $T->{debug_mode};
    
    TestTables::fill_test_table($T->dbh, $table_specifier, $expr, $debug);
}


# complete_test_table ( )
#
# Complete the specified table in the test database (or any database) by calling an the
# complete_table_definition method from a subclass of EditTransaction. This is designed to be used
# for the establishment of database triggers that are necessary for the specified table to
# properly function. But it may be used for other purposes in the future. The $arg argument is
# currently unused, but is put in place in case it might be needed in the future.

sub complete_test_table {
    
    my ($T, $table_specifier, $arg, $debug) = @_;
    
    $debug = 'test' if $T->{debug_mode};
    my $edt_class = $T->{edt_class} || 'EditTransaction';
    
    if ( $edt_class->can('complete_table_definition') )
    {
	$edt_class->complete_table_definition($T->dbh, $table_specifier, $arg, $debug);
    }
    
    else
    {
	diag "Warning: method 'complete_table_definition' not found in class $edt_class";
    }
}


sub start_test_mode {
    
    my ($T, $table_group) = @_;

    return enable_test_mode($table_group);
}


# check_test_schema ( table )
# 
# Make sure that the schema of the specified table in the test database matches that of the
# specified table in the main database. Warn if not.

sub check_test_schema {

    my ($T, $table_specifier) = @_;
    
    # $$$$ we need to fix this!
    
    # my $test_name = exists $TABLE{$table_specifier} && $TABLE{$table_specifier};
    # my $base_name = exists $TABLE{"==$table_specifier"} && $TABLE{"==$table_specifier"};
    
    # unless ( $test_name && $base_name )
    # {
    # 	diag "CANNOT CHECK SCHEMA FOR TABLE '$table_specifier'";
    # 	return;
    # }
    
    # my $test_schema = get_table_schema($T->dbh, $table_specifier);
    # my $base_schema = get_table_schema($T->dbh, "==$table_specifier");
    
    # return is_deeply($test_schema, $base_schema, "schema for '$test_name' matches schema for '$base_name'");
}


1;

