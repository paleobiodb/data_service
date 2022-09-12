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

our $LAST_BANNER = '';

our (@EXPORT_OK) = qw(connect_to_database ok_eval ok_exception last_result);


# If the $TEST_MODE is set to true, the outcome of certain tests is reversed. We use this to check
# that tests will fail under certain circumstances. $TEST_DIAG collects diagnostic output if
# $TEST_MODE is true.

our $TEST_MODE = 0;
our $TEST_DIAG = '';


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


# set_table ( table_name )
#
# Set the default table for all new EditTransactions created by this tester object.

sub set_table {

    my ($T, $table) = @_;

    $T->{edt_table} = $table;
}


sub default_table {

    my ($T) = @_;

    return $T->{edt_table};
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
    
    my ($T, @options) = @_;
    
    # croak "You must specify a permission as the first argument" unless $perm;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my (@allowances);
    my $CREATE = 1;
    
    # If defaults were given when this tester instance was created, fetch them now.
    
    my $edt_table = $T->{edt_table};
    my $edt_class = $T->{edt_class} || croak "EditTester instance has an empty class name";
    my $edt_perm;
    
    # Now iterate through the arguments given.
    
    while ( @options )
    {
	my $entry = shift @options;
	
	if ( $entry eq 'table' )
	{
	    $edt_table = shift @options;
	    croak "invalid table name '$edt_table'" if ref $edt_table;
	}
	
	elsif ( $entry eq 'permission' )
	{
	    $edt_perm = shift @options;
	}
	
	elsif ( $entry eq 'class' )
	{
	    $edt_class = shift @options;
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

    my ($T) = @_;
    
    return ref $T->{last_edt} && $T->{last_edt}->isa('EditTransaction') ? $T->{last_edt} : undef;
}


sub clear_edt {

    my ($T) = @_;

    $T->{last_edt} = undef;
}


# Subroutines to make testing more straightforward
# ------------------------------------------------

# ok_eval ( subroutine, label )
# 
# The first argument is a subroutine (typically anonymous) that will be called inside an eval. Any
# errors that occur will be reported. The second argument is the name for this test, which
# defaults to a generic label. If the result of the eval is true, the test will pass. Otherwise
# it will fail. This subroutine can be exported and called directly, or it can be called as a method.

our ($EVAL_RESULT);

sub ok_eval {
    
    # If the first argument is an EditTester instance, ignore it.
    
    shift if ref $_[0] eq 'EditTester';
    
    my ($sub, $label) = @_;
    
    croak "First argument must be a subroutine reference" unless ref $sub eq 'CODE';
    
    $label ||= 'eval succeeded';
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $EVAL_RESULT = undef;
    
    my $result = eval { $EVAL_RESULT = $sub->() };
    
    if ( $@ )
    {
	unless ( $TEST_MODE )
	{
	    my ($package, $filename, $line) = caller;
	    diag "An exception was thrown from ok_eval at $filename line $line:";
	    diag $@;
	}
	ok( $TEST_MODE, $label );
    }
    
    elsif ( $TEST_MODE )
    {
	ok( !$result , $label );
    }
    
    else
    {
	ok( $result, $label );
    }
}


# ok_exception ( subroutine, expected, label )
# 
# The first argument is a subroutine (typically anonymous) that will be called inside an eval. The
# second argument is a regexp to be matched against an exception if one is thrown during the eval.
# It may also be '1', which matches any exception. The third argument is the name of this test,
# defaulting to a generic label. If no exception is thrown, the test fails. If an exception is
# thrown, the test passes if the exception matches the regexp and fails otherwise. This subroutine
# can be exported and called directly, or it can be called as a method.

sub ok_exception {
    
    # If the first argument is an EditTester instance, ignore it.
    
    shift if ref $_[0] eq 'EditTester';
    
    my ($sub, $expected, $label) = @_;
    
    croak "First argument must be a subroutine reference" unless ref $sub eq 'CODE';
    croak "Second argument must be a regexp reference" 
	unless ref $expected eq 'Regexp' || $expected eq '1';
    
    $label ||= 'a matching exception was thrown';
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $EVAL_RESULT = undef;
    
    eval { $EVAL_RESULT = $sub->() };
    
    if ( $@ && ( $expected eq '1' || $@ =~ $expected ) )
    {
	ok( !$TEST_MODE, $label );
    }
    
    else
    {
	if ( $@ )
	{
	    diag "Expected exception matching '$expected', got '$@'";
	}
	
	ok( $TEST_MODE, $label );
    }
}


# last_result ( )
# 
# Return the result from the subroutine executed by the last ok_eval or ok_exception.

sub last_result {
    
    return $EVAL_RESULT;
}


# Methods for testing error and warning conditions
# ------------------------------------------------

# condition_args ( [edt], [selector], [type], [filter], [label] )
# 
# Construct and return a list ($edt, $selector, $type, $filter, $label) from the arguments, with
# appropriate defaults. All of the arguments are optional, and this method will properly
# distinguish whichever ones are specified and will fill in defaults for any left out. The
# argument $edt defaults to the last edt created by this EditTester instance. The $selector and
# $type arguments default to 'all'. There is no default for $label.
# 
# Accepted values for $selector are:
#
#   all        all conditions
#   main       conditions not associated with any action
#   latest     conditions associated with the latest action
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
# - A string that looks like 'E_SOME_ERROR', 'C_SOME_CAUTION', or 'W_SOME_WARNING'.
#   This selects conditions with a matching code. 


sub condition_args {

    my $T = shift;
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift @_ : $T->{last_edt};
    
    croak "no EditTransaction found" unless $edt;

    my ($selector, $type, $filter, $label);
    
    if ( $_[0] && $_[0] =~ / ^ (?: all | latest | main ) $ /xsi )
    {
	$selector = shift;
    }
    
    if ( $_[0] && $_[0] =~ / ^ (?: errors | warnings | fatal | nonfatal | all ) $ /xsi )
    {
	$type = shift;
    }
    
    if ( $_[0] && $_[0] =~ /^[a-z]+$/ )
    {
	croak "invalid selector or type '$_[0]'";
    }
    
    $selector ||= 'all';
    $type ||= 'all';
    
    if ( defined $_[0] && $_[0] ne '' )
    {
	if ( ref $_[0] eq 'Regexp' )
	{
	    $filter = shift;
	}
	
	elsif ( $_[0] && $_[0] =~ qr{ ^ ([CEFW]_[A-Z0-9_]+) $ }xs )
	{
	    $filter = qr{ ^ $1 \b }xs;
	    shift;
	}
	
	elsif ( $_[0] && $_[0] =~ qr{ ^ EF_ ([A-Z0-9_]+) $ }xs )
	{
	    $filter = qr{ ^ [EF]_ $1 \b }xs;
	    shift;
	}
	
	elsif ( ref $_[0] || $_[0] !~ /[a-z ]/ )
	{
	    croak "unrecognized filter '$_[0]': must be a condition code or a regexp";
	}
    }
    
    shift while defined $_[0] && ! $_[0];
    
    $label = shift;
    
    return ($T, $edt, $selector, $type, $filter, $label);
}


# conditions ( [edt], [selector], [type], [filter] )
#
# Return a list of conditions matching the arguments.

sub conditions {

    my ($T, $edt, $selector, $type, $filter) = &condition_args;

    if ( $filter )
    {
	return grep { $_ =~ $filter } $edt->conditions($selector, $type);
    }

    else
    {
	return $edt->conditions($selector, $type);
    }
}




# diag_lines ( string... )
#
# If $TEST_MODE is true, append the given strings to $TEST_DIAG. Otherwise, print them out as
# diagnostic messages.

sub diag_lines {
    
    foreach ( @_ )
    {
	if ( $TEST_MODE ) { $TEST_DIAG .= "$_\n"; }
	else { diag $_ };
    }
}


# test_output ( )
#
# Return the value of $TEST_DIAG. This method is intended for use only in testing the EditTester
# class.

sub test_output {

    return $TEST_DIAG;
}


# clear_test_output ( )
#
# Clear $TEST_DIAG and return the value it had. This method is intended for use only in testing
# the EditTester class.

sub clear_test_output {

    my $retval = $TEST_DIAG;
    $TEST_DIAG = '';
    return $retval;
}


# ok_no_conditions ( [edt], [selector], [type], [filter], [label] )
#
# If $edt has no conditions that match $selector, $type and/or $filter, pass a test labeled by
# $label and return true. Otherwise, fail the test and return false. If $label is not given, it
# defaults to a generic message.
# 
# If $TEST_MODE is true, the test outcome and return value are reversed. And the same is true for
# all of the following methods as well.

sub ok_no_conditions {

    my ($T, $edt, $selector, $type, $filter, $label) = &condition_args;
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return $T->_ok_no_conditions($edt, $selector, $type, $filter,
				 $label || "no matching conditions");
}


sub _ok_no_conditions {

    my ($T, $edt, $selector, $type, $filter, $label) = @_;
    
    # If we find any matching conditions then fail the test, print out the conditions that were
    # found, and return false. Invert this if $TEST_MODE is true.
    
    $filter ||= qr/./;	# If no filter was specified, use a dummy one.
    
    if ( my @conditions = grep { $_ =~ $filter } $edt->conditions($selector, $type) )
    {
	diag_lines(@conditions);
	ok($TEST_MODE, $label);
	return $TEST_MODE;
    }
    
    # Otherwise, pass the test and return true. Invert this if $TEST_MODE is true.

    else
    {
	ok(!$TEST_MODE, $label);
	return !$TEST_MODE;
    }
}


# ok_has_condition ( [edt], [selector], [type], [filter], [label] )
# 
# If $edt has a condition that matches $selector, $type and/or $filter, pass a test labeled by
# $label and return true. Otherwise, fail the test and return false. If $label is not given, it
# defaults to a generic message.
    
sub ok_has_condition {
    
    my ($T, $edt, $selector, $type, $filter, $label) = &condition_args;
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return $T->_ok_has_condition($edt, $selector, $type, $filter,
				 $label || "found matching condition");
}


sub _ok_has_condition {

    my ($T, $edt, $selector, $type, $filter, $label) = @_;
    
    # If we find any matching conditions, pass the test and return true. Invert this if $TEST_MODE
    # is true.
    
    my @conditions = $edt->conditions($selector, $type);
    
    if ( $filter )
    {
	if ( any { $_ =~ $filter } @conditions )
	{
	    ok(!$TEST_MODE, $label);
	    return !$TEST_MODE;
	}
    }
    
    elsif ( @conditions )
    {
	ok(!$TEST_MODE, $label);
	return !$TEST_MODE;
    }
    
    # Otherwise, fail the test and return false. Invert this if $TEST_MODE is true. If any
    # conditions were found that don't match the filter, print them out as diagnostic messages.
    
    diag_lines(@conditions);
    ok($TEST_MODE, $label);
    return $TEST_MODE;
}


# ok_has_one_condition ( [edt], [selector], [type], [filter], [label] )
#
# This method behaves the same as 'ok_has_condition', except that the test is only passed if
# exactly one condition is found and it matches the specified filter. More or fewer will result in
# a failure.

sub ok_has_one_condition {
    
    my ($T, $edt, $selector, $type, $filter, $label) = &condition_args;
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return $T->_ok_has_one_condition($edt, $selector, $type, $filter,
				     $label || "found one matching condition");
}


sub _ok_has_one_condition {
    
    my ($T, $edt, $selector, $type, $filter, $label) = @_;
    
    # If we find exactly one condition that matches $selector and $type, and it also matches the
    # filter, pass the test and return true. Invert this if $TEST_MODE is true.
    
    my @conditions = $edt->conditions($selector, $type);
    
    if ( @conditions == 1 && $filter )
    {
	if ( $conditions[0] =~ $filter )
	{
	    ok(!$TEST_MODE, $label);
	    return !$TEST_MODE;
	}
    }

    elsif ( @conditions == 1 )
    {
	ok(!$TEST_MODE, $label);
	return !$TEST_MODE;
    }
    
    # Otherwise, fail the test and return false. Invert this if $TEST_MODE is true. If there are
    # any conditions that match $selector and $type, print them out as diagnostic
    # messages. Otherwise, print out a message stating that no matching conditions were found.
    
    push @conditions, "No matching conditions were found" unless @conditions;
    diag_lines(@conditions);
    ok($TEST_MODE, $label);
    return $TEST_MODE;
}


# The following methods are shortcuts, specialized for particular condition types.

sub ok_no_errors {

    my ($T, $edt, $selector, $type, $filter, $label) = &condition_args;
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return $T->_ok_no_conditions($edt, $selector, 'errors', $filter,
				 $label || "no matching errors");
}


sub ok_no_warnings {

    my ($T, $edt, $selector, $type, $filter, $label) = &condition_args;
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return $T->_ok_no_conditions($edt, $selector, 'warnings', $filter,
				 $label || "no matching warnings");
}


sub ok_has_error {

    my ($T, $edt, $selector, $type, $filter, $label) = &condition_args;
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return $T->_ok_has_condition($edt, $selector, 'errors', $filter,
				 $label || "found matching error");
}


sub ok_has_warning {

    my ($T, $edt, $selector, $type, $filter, $label) = &condition_args;
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return $T->_ok_has_condition($edt, $selector, 'warnings', $filter,
				 $label || "found matching warning");
}


sub ok_has_one_error {

    my ($T, $edt, $selector, $type, $filter, $label) = &condition_args;
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return $T->_ok_has_one_condition($edt, $selector, 'errors', $filter,
				     $label || "found one matching error");
}


sub ok_has_one_warning {

    my ($T, $edt, $selector, $type, $filter, $label) = &condition_args;
    
    local $Test::Builder::Level = $Test::Builder::Level + 2;
    
    return $T->_ok_has_one_condition($edt, $selector, 'warnings', $filter,
				     $label || "found one matching warning");
}


sub diag_errors {

    my ($T, $edt, $selector) = &condition_args;
    
    my @errors = $edt->conditions($selector, 'errors');
    
    diag_lines(@errors);
}


sub diag_warnings {

    my ($T, $edt, $selector) = &condition_args;

    my @warnings = $edt->conditions($selector, 'warnings');

    diag_lines(@warnings);
}


# Methods for testing the success or failure of transactions and actions
# ----------------------------------------------------------------------

sub ok_action {

    my $T = shift;
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift @_ : $T->{last_edt};
    
    croak "no EditTransaction found" unless $edt;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $label;

    unless ( $label = shift )
    {
	my $operation = $edt->action_operation;

	$label = $operation ? "$operation succeeded" : "action succeeded";
    }
    
    if ( $edt->action_ok )
    {
	ok(!$TEST_MODE, $label);
	return !$TEST_MODE;
    }
    
    else
    {
	ok($TEST_MODE, $label);
	my $status = $edt->action_status;
	diag_lines("action status is '$status'");
	$T->diag_errors($edt, 'latest');
	$T->diag_warnings($edt, 'latest');
	return $TEST_MODE;
    }
}


sub ok_failed_action {
    
    my $T = shift;
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift @_ : $T->{last_edt};
    
    croak "no EditTransaction found" unless $edt;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $operation = $edt->action_operation || 'action';
    $operation = 'action' if $operation eq 'other';
    
    my $check_status;
    my $label;
    my $result;
    
    if ( $_[0] =~ /^\w+$/ )
    {
	$check_status = shift;
    }
    
    if ( $check_status )
    {
	$label = shift || "$operation status is '$check_status'";
	$result = $edt->action_status eq $check_status;
    }
    
    else
    {
	$label = shift || "$operation failed";
	$result = not $edt->action_ok;
    }
    
    if ( $result )
    {
	ok(!$TEST_MODE, $label);
	return !$TEST_MODE;
    }
    
    else
    {
	ok($TEST_MODE, $label);
	my $status = $edt->action_status;
	diag_lines("action status is '$status'");
	$T->diag_errors($edt, 'latest');
	$T->diag_warnings($edt, 'latest');
	return $TEST_MODE;
    }
}


sub ok_commit {

    my $T = shift;
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift @_ : $T->{last_edt};
    
    croak "no EditTransaction found" unless $edt;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $label = shift || "transaction committed successfully";
    
    if ( $edt->commit )
    {
	ok(!$TEST_MODE, $label);
	return !$TEST_MODE;
    }

    else
    {
	my $status = $edt->status;
	diag_lines("transaction status is '$status'");
	$T->diag_errors($edt);
	$T->diag_warnings($edt);
	ok($TEST_MODE, $label);
	return $TEST_MODE;
    }
}


sub ok_diag {
    
    my $T = shift;
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift : $T->{last_edt};
    
    croak "not enough arguments" unless @_;
    
    my $result = shift;
    
    my $selector = 'all';
    
    if ( $_[0] )
    {
	if ( $_[0] =~ /^latest$|^all$|^main$|^:/ )
	{
	    $selector = shift;
	}

	elsif ( $_[0] =~ /^[a-z]+$/ )
	{
	    croak "invalid selector '$_[0]'";
	}
    }
    
    my $label = shift || "operation succeeded";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( $result )
    {
	ok(!$TEST_MODE, $label);
	return !$TEST_MODE;
    }
    
    else
    {
	$T->diag_errors($edt, $selector);
	$T->diag_warnings($edt, $selector);
	ok($TEST_MODE, $label);
	return $TEST_MODE;
    }
}


sub ok_result {

    goto &ok_diag;
}


sub is_diag {
    
    my $T = shift;
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift : $T->{last_edt};
    
    croak "not enough arguments" unless @_ >= 2;
    
    my $arg1 = shift;
    my $arg2 = shift;
    
    my $selector = 'all';
    
    if ( $_[0] )
    {
	if ( $_[0] =~ /^latest$|^all$|^main$|^:/ )
	{
	    $selector = shift;
	}

	elsif ( $_[0] =~ /^[a-z]+$/ )
	{
	    croak "invalid selector '$_[0]'";
	}
    }
    
    my $label = shift || "operation succeeded";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( $TEST_MODE && isnt($arg1, $arg2, $label) )
    {
	$T->diag_errors($edt, $selector);
	$T->diag_warnings($edt, $selector);
	return 1;
    }
    
    elsif ( $TEST_MODE )
    {
	return 0;
    }
    
    elsif ( is($arg1, $arg2, $label) )
    {
	return 1;
    }
    
    else
    {
	$T->diag_errors($edt, $selector);
	$T->diag_warnings($edt, $selector);
	return 0;
    }
}




# Methods for testing the existence or nonexistence of records in the database
# -----------------------------------------------------------------------------

sub ok_found_record {
    
    my ($T, $table, $expr, $label) = @_;
    
    my $dbh = $T->dbh;
    
    # Check arguments
    
    croak "you must specify an expression" unless defined $expr && ! ref $expr && $expr ne '';
    $label ||= 'found at least one record';

    # If the given expression is a single decimal number, assume it is a key.
    
    if ( $expr =~ /^\d+$/ )
    {
	my $key_name = $T->last_edt->get_table_property($table, 'PRIMARY_KEY') or
	    croak "could not determine primary key for table '$table'";
	$expr = "$key_name = $expr";
    }
    
    # Execute the SQL expression and test the result.
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $sql = "SELECT COUNT(*) FROM $TABLE{$table} WHERE $expr";
    
    $T->debug_line($sql);

    my $count;

    eval {
	($count) = $dbh->selectrow_array($sql);
    };

    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("EXCEPTION: $msg");
	$T->{last_exception} = $msg;
    }
    
    $T->debug_line("Returned $count rows");
    $T->debug_skip;
    
    if ( $TEST_MODE ) { $count = ! $count };
    
    ok( $count, $label );
    
    return $count;
}


sub ok_no_record {
    
    my ($T, $table, $expr, $label) = @_;
    
    my $dbh = $T->dbh;
    
    # Check arguments
    
    croak "you must specify an expression" unless defined $expr && ! ref $expr && $expr ne '';
    $label ||= 'record was absent';

    # If the given expression is a single decimal number, assume it is a key.
    
    if ( $expr =~ /^\d+$/ )
    {
	my $key_name = get_table_property($table, 'PRIMARY_KEY') or
	    croak "could not determine primary key for table '$table'";
	$expr = "$key_name = $expr";
    }
    
    # Execute the SQL expression and test the result.
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $sql = "SELECT COUNT(*) FROM $TABLE{$table} WHERE $expr";
    
    $T->debug_line($sql);
    
    my $count;

    eval {
	($count) = $dbh->selectrow_array($sql);
    };

    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("EXCEPTION: $msg");
	$T->{last_exception} = $msg;
	$count = 1; # to trigger a failure on this test
    }	
    
    $T->debug_line("Returned $count rows");
    $T->debug_skip;

    if ( $TEST_MODE ) { $count = ! $count };
    
    ok( ! $count, $label );
}


sub ok_count_records {
    
    my ($T, $count, $table, $expr, $label) = @_;
    
    my $dbh = $T->dbh;
    
    croak "invalid count '$count'" unless defined $count && $count =~ /^\d+$/;
    croak "you must specify a table" unless $table;
    croak "you must specify a valid SQL expression" unless $expr;

    $label ||= "found proper number of records";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $sql = "SELECT count(*) FROM $TABLE{$table} WHERE $expr";
    
    $T->debug_line($sql);
    
    my $result;

    eval {
	($result) = $dbh->selectrow_array($sql);
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("EXCEPTION: $msg");
	$T->{last_exception} = $msg;
	fail("query failed");
	return;
    }
    

    if ( defined $result && $result == $count )
    {
	ok(!$TEST_MODE, $label);
    }

    else
    {
	$result //= "undefined";
	
	ok($TEST_MODE, $label);

	unless ( $TEST_MODE )
	{
	    diag("     got: $result");
	    diag("expected: $count");
	}
    }
}


sub count_records {

    my ($T, $table, $expr) = @_;
    
    my $dbh = $T->dbh;
    
    croak "you must specify a table" unless $table;
    
    my $where_clause = $expr ? "WHERE $expr" : "";
    
    my $sql = "SELECT count(*) FROM $TABLE{$table} $where_clause";
    
    $T->debug_line($sql) if $T->{debug_mode};
    
    my ($result) = $dbh->selectrow_array($sql);
    
    return $result;
}


sub find_record_values {

    my ($T, $table, $colname, $expr) = @_;
    
    my $dbh = $T->dbh;
    
    croak "you must specify a table" unless $table;
    croak "you must specify a column name" unless $colname;
    
    my $where_clause = $expr ? "WHERE $expr" : "";
    
    my $sql = "SELECT `$colname` FROM $TABLE{$table} $where_clause LIMIT 50";
    
    $T->debug_line($sql) if $T->{debug_mode};

    my $result = $dbh->selectrow_arrayref($sql);

    return ref $result eq 'ARRAY' ? $result->@* : ();
}    


sub clear_table {
    
    my ($T, $table) = @_;
    
    $T->clear_edt;
    
    my $dbh = $T->dbh;
    
    croak "you must specify a table" unless $table;
    
    my $sql = "DELETE FROM $TABLE{$table}";
    
    $T->debug_line($sql);
    
    my $result = $dbh->do($sql);
    
    if ( $result )
    {
	$T->debug_line("Deleted $result rows");
    }
    
    $sql = "ALTER TABLE $TABLE{$table} AUTO_INCREMENT = 1";

    $T->debug_line($sql);
    
    eval {
	$dbh->do($sql);
    };
    
    $T->debug_skip;
    
    return;
}


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


sub fetch_records_by_key {
    
    my ($T, $table, @keys) = @_;
    
    my $dbh = $T->dbh;
    
    croak "you must specify a table" unless $table;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    unless ( @keys && $keys[0] )
    {
	fail("no keys were defined");
	return;
    }
    
    my @key_list;

    foreach my $k ( @keys )
    {
	next unless defined $k;
	croak "keys cannot be refs" if ref $k;
	$k =~ s/^\w+[:]//;
	push @key_list, $dbh->quote($k);
    }
    
    return unless @key_list;
    
    my $key_string = join(',', @key_list);
    my $key_name = $T->last_edt->get_table_property($table, 'PRIMARY_KEY');
    
    croak "could not determine primary key for table '$table'" unless $key_name;
    
    my $sql = "SELECT * FROM $TABLE{$table} WHERE $key_name in ($key_string)";
    
    $T->debug_line($sql);
    
    my $results;
    
    eval {
	$results = $dbh->selectall_arrayref($sql, { Slice => { } });
    };

    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("EXCEPTION: $msg");
	$T->{last_exception} = $msg;
    }
    
    if ( ref $results eq 'ARRAY' )
    {
	$T->debug_line("Returned " . scalar(@$results) . " rows");
	$T->debug_skip;
	
	if ( @$results )
	{
	    ok(!$TEST_MODE, "found records");
	}
	
	else
	{
	    ok($TEST_MODE, "found records");
	}
	
	return @$results;
    }

    else
    {
	$T->debug_line("Returned no results");
	$T->debug_skip;
	ok($TEST_MODE, "found records");
	return;
    }
}


sub fetch_records_by_expr {

    my ($T, $table, $expr) = @_;
    
    my $dbh = $T->dbh;
    
    croak "you must specify a table" unless $table;
    croak "you must specify a valid SQL expression" unless $expr;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $sql = "SELECT * FROM $TABLE{$table} WHERE $expr";
    
    $T->debug_line($sql);
    
    my $results;
    
    eval {
	$results = $dbh->selectall_arrayref($sql, { Slice => { } });
    };

    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("EXCEPTION: $msg");
	$T->{last_exception} = $msg;
    }
    
    if ( ref $results eq 'ARRAY' )
    {
	$T->debug_line("Returned " . scalar(@$results) . " rows");
	$T->debug_skip;
	
	if ( @$results )
	{
	    ok(!$TEST_MODE, "found records");
	}

	else
	{
	    ok($TEST_MODE, "found records");
	}

	return @$results;
    }
    
    else
    {
	$T->debug_line("Returned no results");
	$T->debug_skip;
	ok($TEST_MODE, "found records");
	return;
    }
}


sub fetch_keys_by_expr {

    my ($T, $table, $expr) = @_;
    
    my $dbh = $T->dbh;
    
    croak "you must specify a table" unless $table;
    croak "you must specify a valid SQL expression" unless $expr;
    
    my $key_name = get_table_property($table, 'PRIMARY_KEY');
    
    croak "could not determine primary key for table '$table'" unless $key_name;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $sql = "SELECT $key_name FROM $TABLE{$table} WHERE $expr";
    
    $T->debug_line($sql);
    
    my $results;
    
    eval {
	$results = $dbh->selectcol_arrayref($sql, { Slice => { } });
    };

    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("EXCEPTION: $msg");
	$T->{last_exception} = $msg;
    }
    
    if ( ref $results eq 'ARRAY' )
    {
	$T->debug_line("Returned " . scalar(@$results) . " rows");
	$T->debug_skip;
	
	if ( @$results )
	{
	    ok(!$TEST_MODE, "found keys");
	}

	else
	{
	    ok($TEST_MODE, "found keys");
	}

	return @$results;
    }
    
    else
    {
	$T->debug_line("Returned no results");
	$T->debug_skip;
	ok($TEST_MODE, "found keys");
	return;
    }
}


sub fetch_row_by_expr {

    my ($T, $table, $columns, $expr) = @_;
    
    my $dbh = $T->dbh;
    
    croak "you must specify a table" unless $table;
    croak "you must specify at least one column" unless $columns;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;

    my ($sql, $msg);
    
    if ( $expr )
    {
	$sql = "SELECT $columns FROM $TABLE{$table} WHERE $expr";
	$msg = "found a row matching '$expr'";
    }
    
    else
    {
	$sql = "SELECT $columns FROM $TABLE{$table} LIMIT 1";
	$msg = "found a row for '$columns'";
    }
    
    $T->debug_line($sql);
    
    my @values;
    
    eval {
	@values = $dbh->selectrow_array($sql);
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("EXCEPTION: $msg");
	$T->{last_exception} = $msg;
    }
    
    if ( @values )
    {
	ok(!$TEST_MODE, $msg);
	return @values;
    }
    
    else
    {
	ok($TEST_MODE, $msg);
	return;
    }
}


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
    
    my ($T) = shift;
    
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

1;

