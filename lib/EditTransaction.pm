# 
# The Paleobiology Database
# 
#   EditTransaction.pm - base class for data acquisition and modification
# 

package EditTransaction;

use strict;

use ExternalIdent qw(%IDP %IDRE);
use TableDefs qw(get_table_property $PERSON_DATA %TABLE
		 %COMMON_FIELD_SPECIAL %COMMON_FIELD_IDTYPE %FOREIGN_KEY_TABLE %FOREIGN_KEY_COL);
use TableData qw(get_table_schema);
use EditTransaction::Action;
use EditTransaction::Datalog;
use Permissions;

use Carp qw(carp croak);
use Try::Tiny;
use Scalar::Util qw(weaken blessed reftype);
use Encode qw(encode);

use Switch::Plain;

use feature 'unicode_strings';

use namespace::clean;


# This class is intended to encapsulate the mid-level code necessary for updating records in the
# database in the context of a data service operation or a command-line utility. It handles
# transaction initiation, commitment, and rollback, permission checking, and also error and
# warning conditions.
# 
# This class can be subclassed (see ResourceEdit.pm, TimescaleEdit.pm) in order to provide
# additional logic for checking values and performing auxiliary operations in conjunction with
# database inserts, updates, and deletes.

our ($MULTI_DELETE_LIMIT) = 100;
our ($MULTI_INSERT_LIMIT) = 100;

our (%ALLOW_BY_CLASS) = ( EditTransaction => { 
		CREATE => 1,
		LOCKED => 1,
		MULTI_DELETE => 1,
		ALTER_TRAIL => 1,
		NOT_FOUND => 1,
		NO_RECORDS => 1,
		PROCEED => 1,
		BAD_FIELDS => 1,
		DEBUG_MODE => 1,
		SILENT_MODE => 1,
		IMMEDIATE_MODE => 1,
		FIXUP_MODE => 1,
		NO_LOG_MODE => 1 } );

our (%CONDITION_BY_CLASS) = ( EditTransaction => {		     
		C_CREATE => "Allow 'CREATE' to create records",
		C_LOCKED => "Allow 'LOCKED' to update locked records",
		C_NO_RECORDS => "Allow 'NO_RECORDS' to allow transactions with no records",
		C_ALTER_TRAIL => "Allow 'ALTER_TRAIL' to explicitly set crmod and authent fields",
		E_NO_KEY => "The %1 operation requires a primary key value",
		E_HAS_KEY => "You may not specify a primary key value for the %1 operation",
		E_KEY_NOT_FOUND => "Field '%1': no record was found with key '%2'",
		E_LABEL_NOT_FOUND => "Field '%1': no record of the proper type was found with label '%2'",
		E_NOT_FOUND => "No record was found with value '%2' for key '%1'",
		E_LOCKED => "This record is locked",
		E_PERM => { insert => "You do not have permission to insert a record into this table",
			    update => "You do not have permission to update this record",
			    update_many => "You do not have permission to update records in this table",
			    replace_new => "No record was found with key '%2', ".
				"and you do not have permission to insert one",
			    replace_existing => "You do not have permission to replace this record",
			    delete => "You do not have permission to delete this record",
			    delete_many => "You do not have permission to delete records from this table",
			    delete_cleanup => "You do not have permission to delete these records",
			    default => "You do not have permission for this operation" },
		E_BAD_OPERATION => "Invalid operation '%1'",
		E_BAD_RECORD => "%1",
		E_BAD_SELECTOR => "%1",
		E_BAD_UPDATE => "You cannot change the value of '%1' once a record has been created",
		E_PERM_COL => "You do not have permission to set the value of the field '%1'",
		E_REQUIRED => "Field '%1': must have a nonempty value",
		E_RANGE => "Field '%1': %2",
		E_WIDTH => "Field '%1': %2",
		E_FORMAT => "Field '%1': %2",
		E_EXTTYPE => "Field '%1': %2",
		E_PARAM => "%1",
  		E_EXECUTE => "%1",
		E_DUPLICATE => "Duplicate entry '%1' for key '%2'",
		E_BAD_FIELD => "Field '%1' does not correspond to any column",
		W_ALLOW => "Unknown allowance '%1'",
		W_EXECUTE => "%1",
		W_TRUNC => "Field '%1': %2",
		W_BAD_FIELD => "Field '%1' does not correspond to any column",
		UNKNOWN => "MISSING ERROR MESSAGE" });

our (%SPECIAL_BY_CLASS);

our (%TEST_PROBLEM);	# This variable can be set in order to trigger specific errors, in order
                        # to test the error-response mechanisms.


# The following hash is used to make sure that if one transaction interrupts the other, we will
# know about it. The hash keys are DBI connection handles. In fact, under most circumstances there
# will only be one key in this hash at any given time. The value, if defined, will be a weakened
# reference to an EditTransaction object which may possibly have an active transaction.

our (%TRANSACTION_INTERLOCK);

# Also, just for the heck of it, we keep a count which is incremented with each new transaction
# and can be displayed in debugging messages.

our ($TRANSACTION_COUNT) = 1;


# CONSTRUCTOR and destructor
# --------------------------

# new ( request_or_dbh, perms, table, allows )
# 
# Create a new EditTransaction object, for use in association with the specified request. It is
# also possible to specify a DBI database connection handle, as would typically be done by a
# command-line utility. The second argument should be a Permissions object which has already been
# created, the third a table name, and the fourth a hash of allowed cautions.

sub new {
    
    my ($class, $request_or_dbh, $perms, $table_specifier, $allows) = @_;
    
    # Check the arguments.
    
    croak "new EditTransaction: request or dbh is required"
	unless $request_or_dbh && blessed($request_or_dbh);
    
    croak "new EditTransaction: perms is required"
	unless blessed $perms && $perms->isa('Permissions');

    if ( $table_specifier )
    {
	croak "new EditTransaction: unknown table '$table_specifier'"
	    unless $TABLE{$table_specifier}
    }
    
    # Create a new EditTransaction object, and bless it into the proper class.
    
    my $edt = { perms => $perms,
		main_table => $table_specifier || '',
		unique_id => $TRANSACTION_COUNT++,
		allows => { },
		action_list => [ ],
		bad_list => [ ],
		errors => [ ],
		warnings => [ ],
		demoted => [ ],
		condition_code => { },
		tables => { },
		label_found => { },
		current_action => undef,
		proceed => undef,
		record_count => 0,
		action_count => 0,
		fail_count => 0,
		skip_count => 0,
		commit_count => 0,
		rollback_count => 0,
		transaction => '',
		state => 'ok' };
    
    bless $edt, $class;
    
    # Store the request, dbh, and debug flag as local fields. If we are storing a reference to a
    # request, we must weaken it to ensure that this object from being destroyed when it goes out
    # of scope. Circular references might otherwise prevent this.
    
    if ( $request_or_dbh->can('get_connection') )
    {
	$edt->{request} = $request_or_dbh;
	weaken $edt->{request};
	
	$edt->{dbh} = $request_or_dbh->get_connection;
	$edt->{debug} = $request_or_dbh->debug if $request_or_dbh->can('debug');

	$edt->{debug} = 0 if ref $allows eq 'HASH' &&
	    defined $allows->{DEBUG_MODE} && ! $allows->{DEBUG_MODE};
	
	die "TEST NO CONNECT" if $TEST_PROBLEM{no_connect};
    }
    
    elsif ( ref($request_or_dbh) =~ /DBI::db$/ )
    {
	$edt->{dbh} = $request_or_dbh;
	
	die "TEST NO CONNECT" if $TEST_PROBLEM{no_connect};
    }

    else
    {
	croak "no database handle was provided";
    }
    
    # If we are given either a hash or an array of conditions that should be allowed, store them
    # in the object.
    
    my @allows;
    
    if ( ref $allows eq 'HASH' )
    {
	@allows = grep { $allows->{$_} } keys %$allows;
    }
    
    elsif ( ref $allows eq 'ARRAY' )
    {
	@allows = @$allows;
    }

    elsif ( defined $allows )
    {
	@allows = grep { $_ } split(/\s*,\s*/, $allows);
    }
    
    foreach my $k ( @allows )
    {
	if ( $ALLOW_BY_CLASS{$class}{$k} || $ALLOW_BY_CLASS{EditTransaction}{$k} )
	{
	    $edt->{allows}{$k} = 1;

	    if ( $k eq 'PROCEED' ) { $edt->{proceed} = 1 }
	    elsif ( $k eq 'NOT_FOUND' ) { $edt->{proceed} ||= 2 }
	    elsif ( $k eq 'DEBUG_MODE' ) { $edt->{debug} = 1; $edt->{silent} = undef }
	    elsif ( $k eq 'SILENT_MODE' ) { $edt->{silent} = 1 unless $edt->{debug} }
	    elsif ( $k eq 'IMMEDIATE_MODE' ) { $edt->{execute_immediately} = 1 }
	    elsif ( $k eq 'FIXUP_MODE' ) { $edt->{fixup_mode} = 1; }
	}
	
	else
	{
	    $edt->add_condition('W_ALLOW', $k);
	}
    }
    
    # Throw an exception if we don't have a valid database handle. Otherwise, rollback any
    # uncommitted work, since if there is previous work on THIS DATABASE CONNECTION that qwasn't
    # explicitly committed before creating this new transaction then something is wrong.
    
    croak "missing dbh" unless ref $edt->{dbh};
    
    # Now set the database handle attributes properly.
    
    $edt->{dbh}->{RaiseError} = 1;
    $edt->{dbh}->{PrintError} = 0; # $edt->{debug};
    
    # If IMMEDIATE_MODE was specified, then immediately start a new transaction. The same effect
    # can be provided by calling the method 'start_execution' on this new object.
    
    if ( $edt->{execute_immediately} )
    {
	$edt->_start_transaction;
    }
    
    return $edt;
}


# If this object is destroyed while a transaction is in progress, roll it back.

sub DESTROY {
    
    my ($edt) = @_;
    
    if ( $edt->is_active )
    {
	$edt->_rollback_transaction('destroy');
    }
}


# Basic accessor methods
# ----------------------

# These are all read-only.

sub dbh {
    
    return $_[0]{dbh};
}


sub request {
    
    return $_[0]{request};
}


sub transaction {

    return $_[0]{transaction} || '';
}


sub has_started {

    return $_[0]{transaction} ? $_[0]{transaction} : '';
}


sub is_active {

    return $_[0]{transaction} && $_[0]{transaction} eq 'active' ? 'active' : '';
}


sub has_finished {

    return $_[0]{transaction} && $_[0]{transaction} ne 'active' ? $_[0]{transaction} : '';
}


sub has_committed {

    return $_[0]{transaction} && $_[0]{transaction} eq 'committed' ? 'committed' : '';
}


sub can_proceed {

    return defined $_[0]{transaction} && ($_[0]{transaction} eq '' || $_[0]{transaction} eq 'active') &&
	! ( $_[0]{errors} && @{$_[0]{errors}} )
}


sub perms {
    
    return $_[0]->{perms};
}


sub debug {
    
    return $_[0]->{debug};
}


sub role {
    
    return $_[0]->{perms} ? $_[0]->{perms}->role : '';
}


# Additional attributes
# ---------------------

# The following routines allow getting and setting of arbitrary attributes, which can be used for
# communication between subclass methods and interface code.

sub set_attr {

    my ($edt, $attr, $value) = @_;
    
    croak "you must specify an attribute name" unless $attr;
    $edt->{attrs}{$attr} = $value;
}


sub get_attr {

    my ($edt, $attr) = @_;
    
    croak "you must specify an attribute name" unless $attr;
    return $edt->{attrs} ? $edt->{attrs}{$attr} : undef;
}


sub set_attr_key {
    
    my ($edt, $attr, $key, $value) = @_;
    
    croak "you must specify an attribute name" unless $attr;
    croak "attribute '$attr' is not a hash" if exists $edt->{attrs}{$attr} &&
	! ref $edt->{attrs}{$attr} eq 'HASH';
    $edt->{attrs}{$attr}{$key} = $value;
}


sub delete_attr_key {

    my ($edt, $attr, $key) = @_;
    
    croak "you must specify an attribute name" unless $attr;
    croak "attribute '$attr' is not a hash" if exists $edt->{attrs}{$attr} &&
	! ref $edt->{attrs}{$attr} eq 'HASH';
    delete $edt->{attrs}{$attr}{$key};
}


sub get_attr_keys {
    
    my ($edt, $attr) = @_;
    
    croak "you must specify an attribute name" unless $attr;
    croak "attribute '$attr' is not a hash" if exists $edt->{attrs}{$attr} &&
	! ref $edt->{attrs}{$attr} eq 'HASH';
    return keys %{$edt->{attrs}{$attr}};
}
    

sub get_attr_hash {
    
    my ($edt, $attr) = @_;
    
    croak "you must specify an attribute name" unless $attr;
    croak "attribute '$attr' is not a hash" if exists $edt->{attrs}{$attr} &&
	! ref $edt->{attrs}{$attr} eq 'HASH';
    return %{$edt->{attrs}{$attr}};
}
    

# Debugging and error message display
# -----------------------------------

# error_line ( text )
# 
# This method is called internally to display error messages, including
# exceptions that were generated during the execution of code in this
# module. These messages are suppressed if SILENT_MODE is true. Note that
# setting DEUBG_MODE to true. will turn off SILENT_MODE.

sub error_line {

    return if ref $_[0] && $_[0]->{silent};
    
    my ($edt, $line) = @_;
    
    # if ( $edt->{request} )
    # {
    # 	$edt->{request}->debug_line($line);
    # }

    # else
    # {
	$edt->write_debug_output($line);
    # }
}


# debug_line ( text )
#
# This method is called internally to display extra output for
# debugging. These messages are only shown if DEBUG_MODE is true.
    
sub debug_line {
    
    return unless ref $_[0] && $_[0]->{debug} && ! $_[0]->{silent};
    
    my ($edt, $line) = @_;
    
    if ( $edt->{request} )
    {
	$edt->{request}->debug_line($line);
    }

    else
    {
	$edt->write_debug_output($line);
    }
}


# write_debug_output ( line )
#
# This can be overridden by subclasses, in order to capture debugging output. This is particularly
# useful for unit tests.

sub write_debug_output {

    print STDERR "$_[1]\n";
}


# debug_mode ( value )
#
# Turn debug mode on or off. When this mode is on, all SQL statements are printed to standard
# error along with some extra debugging output.

sub debug_mode {

    my ($edt, $new_value) = @_;

    $edt->{debug} = $new_value;
}


# silent_mode ( value )
#
# Turn silent mode on or off. When this mode is on, exceptions are NOT printed to standard error
# the way they usually are.

sub silent_mode {
    
    my ($edt, $new_value) = @_;

    $edt->{silent} = $new_value;
}


# Error, caution, and warning conditions
# --------------------------------------

# Error and warning conditions are indicated by codes, all composed of upper case word symbols. Those
# that start with 'E_' represent errors, those that start with 'C_' represent cautions, and those
# that start with 'W_' represent warnings. In general, errors cause the operation to be aborted
# while warnings do not. Cautions cause the operation to be aborted unless specifically allowed.
# 
# Codes that start with 'C_' indicate cautions that may be allowed, so that the operation proceeds
# despite them. A canonical example is 'C_CREATE', which is returned if records are to be
# created. If the data service operation method knows that records are to be created, it can
# explicitly allow 'CREATE', which will allow the records to be created. Alternatively, it can
# return 'C_CREATE' as an error code to the client-side application, which can ask the user if
# they really want to create new records. If they answer affirmatively, the operation can be
# re-tried with 'CREATE' specifically allowed. The same can be done with other cautions.
# 
# Codes that start with 'E_' indicate conditions that prevent the operation from proceeding. For
# example, 'E_PERM' indicates that the user does not have permission to operate on the specified
# record or table. 'E_NOT_FOUND' indicates that a record to be updated is not in the
# database. Unlike cautions, these conditions cannot be specifically allowed. However, the special
# allowance 'PROCEED' specifies that whatever parts of the operation are able to succeed
# should be carried out, even if some record operations fail. The special allowance 'NOT_FOUND'
# indicates that E_NOT_FOUND should be demoted to a warning, and that particular record skipped,
# but other errors will still block the operation from proceeding.
# 
# Codes that start with 'W_' indicate warnings that should be passed back to the client but do not
# prevent the operation from proceeding.
# 
# Codes that start with 'D_' and 'F_' indicate conditions that would otherwise have been cautions
# or errors, under the 'PROCEED' or 'NOT_FOUND' allowance. These are treated as warnings.
# 
# Allowed conditions must be specified for each EditTransaction object when it is created.


# register_allows ( condition... )
# 
# Register the names of extra allowances for transactions in a particular subclass. This class
# method is designed to be called at startup from modules that subclasses this one.

sub register_allowances {
    
    my ($class, @names) = @_;
    
    foreach my $n ( @names )
    {
	$ALLOW_BY_CLASS{$class}{$n} = 1;
    }
}


# inherit_from ( from_class )
# 
# Copy all of the allowances and conditions from the specified class to this one.

sub inherit_from {
    
    my ($class, $from_class) = @_;
    
    if ( ref $ALLOW_BY_CLASS{$from_class} eq 'HASH' )
    {
	foreach my $n ( keys %{$ALLOW_BY_CLASS{$from_class}} )
	{
	    $ALLOW_BY_CLASS{$class}{$n} = $ALLOW_BY_CLASS{$from_class}{$n};
	}
    }

    if ( ref $CONDITION_BY_CLASS{$from_class} eq 'HASH' )
    {
	foreach my $n ( keys %{$CONDITION_BY_CLASS{$from_class}} )
	{
	    $CONDITION_BY_CLASS{$class}{$n} = $CONDITION_BY_CLASS{$from_class}{$n};
	}
    }
}


# allows ( condition )
# 
# Returns true if the specified condition is allowed for this EditTransaction, false
# otherwise. The set of allowed conditions was specified when this object was originally created.

sub allows {
    
    return ref $_[0]->{allows} && defined $_[1] && $_[0]->{allows}{$_[1]};
}


# register_conditions ( condition ... )
#
# Register the names and templates of conditions which may be generated by transactions in a
# particular subclass. This is designed to be called at startup from modules which subclass this
# one.

sub register_conditions {

    my $class = shift;
    
    croak "you must call this as a class method" unless $class->isa('EditTransaction') && ! ref $class;
    
    # Process the arguments in pairs.
    
    while ( @_ )
    {
	my $code = shift;
	
	croak "you must specify an even number of arguments" unless @_;
	
	my $template = shift;

	# Make sure the code follows the proper pattern, and the template is not empty.
	
	croak "bad condition code '$code'" unless $code =~ qr{ ^ [ECW] _ [A-Z0-9_]+ $ }xs;
	croak "bad condition template '$template'" unless defined $template && $template ne '';
	
	$CONDITION_BY_CLASS{$class}{$code} = $template;
    }
};


# add_condition ( [action], condition, data... )
# 
# Register a condition (error, caution, or warning) that pertains to the either the entire transaction
# or to a single action. One or more pieces of data will generally also be passed in, which can be
# used later by code in the data service operation module to generate an error or warning message
# to return to the user. Conditions that pertain to an action may be translated to warnings if
# either the PROCEED or the NOT_FOUND allowance was specified.
# 
# If the first parameter is a reference to an action, then the condition will be attached to that
# action. If it is the undefined value or the string 'main', then the condition will apply to the
# transaction as a whole. Otherwise, the condition will be attached to the current action if there
# is one or the transaction as a whole otherwise.

sub add_condition {
    
    my $edt = shift;
    
    my $action;
    
    # If the first parameter is a reference, it must be a reference to an action. 
    
    if ( ref $_[0] )
    {
	$action = shift;
	
	croak "first parameter must be either a code, or an action, or 'main'"
	    unless $action->isa('EditTransaction::Action');
    }
    
    # If it is 'main' or the undefined value, then the action will be empty.
    
    elsif ( ! defined $_[0] || $_[0] eq 'main' )
    {
	shift;	# in this case, $action will be empty
    }
    
    # Otherwise, we use the "current action", which may be empty if this method is called before
    # the first action is handled or after the last one is done.
    
    else
    {
	$action = $edt->{current_action};
    }
    
    # The next parameter must be the condition code. Any subsequent parameters are data elements
    # that will be used as parameters for generating the error message.
    
    my $code = shift || '';

    # Make sure this is a code that we recognize.

    unless ( $CONDITION_BY_CLASS{EditTransaction}{$code} || $CONDITION_BY_CLASS{ref $edt}{$code} )
    {
	croak "unknown condition code '$code'";
    }
    
    # If the code starts with E_ or C_ then it represents an error.
    
    if ( $code =~ qr{ ^ [EC] }xs )
    {
	# If it is attached to an action, then mark that action as having at least one error, and
	# check to see if it needs to be demoted.
	
	if ( $action )
	{
	    $action->add_error;
	    
	    # If this transaction allows either PROCEED or NOT_FOUND, then we demote this
	    # error to a warning. But in the latter case, only if it is E_NOT_FOUND.
	    
	    if ( $edt->{proceed} && ( $edt->{proceed} == 1 || $code eq 'E_NOT_FOUND' ) )
	    {
		substr($code,0,1) =~ tr/CE/DF/;
		
		$edt->{condition_code}{$code}++;
		
		my $condition = EditTransaction::Condition->new($action, $code, @_);
		push @{$edt->{demoted}}, $condition;
		
		return $condition;
	    }
	}
	
	# If we get here, then the condition will be saved as an error.
	
	$edt->{condition_code}{$code}++;
	
	my $condition = EditTransaction::Condition->new($action, $code, @_);
	push @{$edt->{errors}}, $condition;
	
	return $condition;
    }
    
    # if the code starts with W_ then it represents a warning.
    
    elsif ( $code =~ qr{ ^ [W] }xs )
    {
	# If it is attached to an action, mark that action as having at least one warning.
	
	if ( $action )
	{
	    $action->add_warning;
	}

	# This condition will be saved on the warning list.
	
	$edt->{condition_code}{$code}++;
	
	my $condition = EditTransaction::Condition->new($action, $code, @_);
	push @{$edt->{warnings}}, $condition;

	return $condition;
    }

    # If it doesn't match either pattern, throw an exception.
    
    else
    {
	croak "bad condition code '$code'";
    }
}


# errors ( )
# 
# Return the list of error and caution condition records for the current EditTransaction. In
# numeric context, Perl will simply evaluate this as a number. In boolean context, this will be
# evaluated as true if there are any and false if not. This is one of my favorite features of
# Perl.

sub errors {

    return @{$_[0]->{errors}};
}


# warnings ( )
# 
# Return the list of warning condition records for the current EditTransaction. See &errors above.

sub warnings {

    if ( wantarray )
    {
	return @{$_[0]->{demoted}}, @{$_[0]->{warnings}};
    }

    else
    {
	return @{$_[0]->{demoted}} + @{$_[0]->{warnings}};
    }
}


# conditions ( [selector], [type] )
# 
# Returns the list of condition records associated with the current action, or with a specified action, or
# those not associated with any action, or all conditions associated with this transaction.
# 
# The parameter $selector may be any of the following, defaulting to 'all':
# 
# 'all'		returns all conditions associated with this EditTransaction
# 'latest'	returns all conditions associated with the most recent action
# 'main'	returns all conditions not associated with any action
# Action ref	returns all conditions associated with the specified action
#
# The parameter $type may be any of the following, defaulting to 'all':
#
# 'all'		returns all types of conditions
# 'errors'	returns only errors and demoted errors
# 'warnings'	returns only warnings

sub conditions {
    
    my ($edt, $selector, $type) = @_;
    
    # The selector and type default to 'all' if not specified.
    
    $selector ||= 'all';
    $type ||= 'all';
    
    my @conditions;
    
    # If $selector is 'all', then we just need to return the appropriate list(s).
    
    if ( $selector eq 'all' )
    {
	if ( $type eq 'all' )
	{
	    return (@{$edt->{errors}}, @{$edt->{demoted}}, @{$edt->{warnings}}) if wantarray;
	    return (@{$edt->{errors}} + @{$edt->{demoted}} + @{$edt->{warnings}}); # otherwise
	}
	
	elsif ( $type eq 'errors' )
	{
	    return (@{$edt->{errors}}, @{$edt->{demoted}}) if wantarray;
	    return (@{$edt->{errors}} + @{$edt->{demoted}}); # otherwise
	}
	
	elsif ( $type eq 'warnings' )
	{
	    return (@{$edt->{warnings}}, @{$edt->{demoted}}) if wantarray;
	    return (@{$edt->{warnings}} + @{$edt->{demoted}}); # otherwise
	}
	
	else
	{
	    croak "invalid type '$type'";
	}
    }
    
    # Otherwise, we need to search the individual lists.
    
    foreach my $c ( $type eq 'all' ? (@{$edt->{errors}}, @{$edt->{demoted}}, @{$edt->{warnings}}) :
		    $type eq 'errors' ? (@{$edt->{errors}}, @{$edt->{demoted}}) :
		    $type eq 'warnings' ? @{$edt->{warnings}} : croak "invalid type '$type'" )
    {
	if ( $selector eq 'main' )
	{
	    push @conditions, $c unless $c->[0];
	}

	elsif ( $selector eq 'latest' )
	{
	    push @conditions, $c if $c->[0] == $edt->{current_action};
	}
	
	elsif ( ref $selector eq 'EditTransaction::Action' )
	{
	    push @conditions, $c if $c->[0] == $selector;
	}
	
	else
	{
	    croak "bad selector '$selector'";
	}
    }
    
    return @conditions;
}


# has_condition_code ( code... )
#
# Return true if any of the specified codes have been attached to the current transaction.

sub has_condition_code {
    
    my ($edt, @codes) = @_;

    # Return true if any of the following codes are found.
    
    foreach my $code ( @codes )
    {
	return 1 if $edt->{condition_code}{$code};
    }

    # Otherwise, return false.
    
    return;
}


# _remove_conditions ( )
# 
# This routine is designed to be called from 'abort_action', to remove any errors or warnings that have
# been accumulated for the record.

sub _remove_conditions {
    
    my ($edt, $action, $selector) = @_;
    
    my $condition_count;
    
    if ( $selector && $selector eq 'errors' )
    {
	$condition_count = $action->has_errors;
    }
    
    elsif ( $selector && $selector eq 'warnings' )
    {
	$condition_count = $action->has_warnings;
    }
    
    else
    {
	croak "you must specify either 'errors' or 'warnings' as the second parameter";
    }
    
    my $removed_count = 0;
    
    # Start at the end of the list, removing any errors that are associated with this action.
    
    while ( @{$edt->{$selector}} && $edt->{$selector}[-1][0] && $edt->{$selector}[-1][0] == $action )
    {
	pop @{$edt->{$selector}};
	$removed_count++;
    }

    # If our count of conditions to be removed doesn't match the number removed, and there are
    # more errors not yet looked at, scan the list from back to front and remove them. This should
    # never actually happen, unless $action->has_whichever returns an incorrect value.
    
    if ( $condition_count && $condition_count > $removed_count && @{$edt->{$selector}} > 1 )
    {
	my $orig_count = scalar(@{$edt->{$selector}});
	
	foreach ( 2..$orig_count )
	{
	    my $i = $orig_count - $_;
	    
	    if ( $edt->{$selector}[$i][0] && $edt->{$selector}[$i][0] == $action )
	    {
		splice(@{$edt->{$selector}}, $i, 1);
		$removed_count++;
	    }
	}
    }
    
    return $removed_count;
}


# error_strings ( )
#
# Return a list of error strings that can be printed out, returned in a JSON data structure, or
# otherwise displayed to the end user.

sub error_strings {
    
    my ($edt) = @_;
    
    return $edt->_generate_strings(@{$edt->{errors}});
}


sub warning_strings {

    my ($edt) = @_;

    return $edt->_generate_strings(@{$edt->{demoted}}, @{$edt->{warnings}});
}


sub _generate_strings {

    my $edt = shift;
    
    my %message;
    my @messages;
    
    foreach my $e ( @_ )
    {
	my $str = $e->code . ': ' . $edt->generate_msg($e);
	
	push @messages, $str unless $message{$str};
	push @{$message{$str}}, $e->label;
    }
    
    my @strings;
    
    foreach my $m ( @messages )
    {
	my $nolabel;
	my %label;
	my @labels;
	
	foreach my $l ( @{$message{$m}} )
	{
	    if ( defined $l && $l ne '' ) { push @labels, $l unless $label{$l}; $label{$l} = 1; }
	    else { $nolabel = 1; }
	}
	
	if ( $nolabel )
	{
	    push @strings, $m;
	}
	
	if ( @labels > 3 )
	{
	    my $count = scalar(@labels) - 2;
	    my $list = " ($labels[0], $labels[1], and $count more):";
	    push @strings, $m =~ s/:/$list/r;
	}

	elsif ( @labels )
	{
	    my $list = ' (' . join(', ', @labels) . '):';
	    push @strings, $m =~ s/:/$list/r;
	}
    }

    return @strings;
}


# record_errors ( )
# 
# Return the list of errors (not warnings) for the current record. This is used below to test
# whether or not we can proceed with the current record.

# sub record_errors {
    
#     return @{$_[0]->{current_errors}};
# }


# record_warnings ( )
# 
# Return the list of warnings for the current record. This is only here in case it is needed by a
# subroutine defined by some subclass.

# sub record_warnings {

#     return @{$_[0]->{current_warnings}};
# }


# generate_msg ( condition )
#
# This routine generates an error message from a condition record.

sub generate_msg {
    
    my ($edt, $condition) = @_;
    
    # Extract the necessary information from the condition record.
    
    my $code = $condition->code;
    my $table = $condition->table;
    my @params = $condition->data;
    
    # If the code was altered because of the PROCEED allowance, change it back
    # so we can look up the proper template.
    
    my $lookup = $code;
    substr($lookup,0,1) =~ tr/DF/CE/;
    
    # Look up the template according to the specified, code, table, and first
    # parameter. The method called may be overridden by a subclass, in order
    # to handle codes that we do not know about.
    
    my $template = $edt->get_condition_template($lookup, $params[0]);
    
    # Then generate the message.
    
    return $edt->substitute_msg($code, $table, $template, @params);
}


# get_condition_template ( code, table, selector )
#
# Given a code, a table, and an optional selector string, return a message template.  This method
# is designed to be overridden by subclasses, but the override methods must call
# SUPER::get_condition_template if they cannot find a template for their particular class that
# corresponds to the information they are given.

sub get_condition_template {

    my ($edt, $code, $selector) = @_;
    
    my $template = $CONDITION_BY_CLASS{ref $edt}{$code} ||
	           $CONDITION_BY_CLASS{EditTransaction}{$code};
    
    if ( ref $template eq 'HASH' )
    {
	if ( $selector && $template->{$selector} )
	{
	    return $template->{$selector};
	}
	
	elsif ( $CONDITION_BY_CLASS{EditTransaction}{$code}{$selector} )
	{
	    return $CONDITION_BY_CLASS{EditTransaction}{$code}{$selector};
	}
	
	elsif ( $template->{default} )
	{
	    return $template->{default};
	}
	
	elsif ( $CONDITION_BY_CLASS{EditTransaction}{$code}{default} )
	{
	    return $CONDITION_BY_CLASS{EditTransaction}{$code}{default};
	}
	
	else
	{
	    return $selector ? $CONDITION_BY_CLASS{EditTransaction}{'UNKNOWN'} . " for '$selector'"
		: $CONDITION_BY_CLASS{EditTransaction}{'UNKNOWN'};
	}
    }
    
    else
    {
	return $template || $CONDITION_BY_CLASS{EditTransaction}{'UNKNOWN'} . " for '$code'";
    }
}


# substitute_msg ( code, table, template, params... )
#
# Generate a message string using the specified elements. The message template may include any of
# the following symbols:
# 
# %t		substitute the name of the database table that was being operated on
# %1..%9	substitute one of the parameters associated with the error

sub substitute_msg {
    
    my ($edt, $code, $table, $template, @params) = @_;
    
    # If we have a non-empty template, then substitute all of the symbols that appear in it.
    
    if ( defined $template && $template ne '' )
    {
	$template =~ s{ [%]t }{ $table }xs;
	$template =~ s{ [%](\d) }{ &squash_param($params[$1-1]) }xseg;
	
	# if ( $code eq 'E_PARAM' && defined $params[2] && $params[2] ne '' )
	# {
	#     $template .= ", was '$params[2]'";
	# }
	
	return $template;
    }

    # Otherwise, return the empty string.
    
    else
    {
	return '';
    }
}


# squash_param ( param )
#
# Return a value suitable for inclusion into a message template. If the parameter value is longer
# than 40 characters, it is truncated and ellipses are appended. If the value is not defined, then
# 'UNKNOWN PARAMETER' is returned.

sub squash_param {

    if ( defined $_[0] )
    {
	if ( length($_[0]) > 80 )
	{
	    return substr($_[0],0,80) . '...';
	}

	else
	{
	    return $_[0];
	}
    }

    else
    {
	return 'UNKNOWN';
    }
}


# Record sequencing
# -----------------

# _new_record {
# 
# Prepare for a new record operation. This includes moving any errors, cautions, and warnings
# generated for the previous record to the main error and warning lists. It also determines the
# label to be used in reporting any conditions about the new record. This is intended to be a private
# method, called only from within this class.

sub _new_record {
    
    my ($edt, $table, $operation, $record) = @_;
    
    croak "no record specified" unless ref $record eq 'HASH' ||
	$operation =~ /delete|other/ && defined $record && $record ne '';
    
    croak "unknown table '$table'" unless exists $TABLE{$table};
    
    $edt->{tables}{$table} = 1;
    
    # Determine a label for this record. If one is specified, use that. Otherwise, keep count
    # of how many records we have seen so far and use that prepended by '#'. Create an entry
    # in the 'label_found' hash so that we know what table this label refers to.
    
    my $label;
    
    $edt->{record_count}++;
    
    if ( ref $record && defined $record->{_label} && $record->{_label} ne '' )
    {
	$label = $record->{_label};
    }
    
    else
    {
	$label = '#' . $edt->{record_count};
    }
    
    $edt->{label_found}{$label} = $operation eq 'delete' ? '_DELETED_' : $table;
    
    # Then create a new EditTransaction::Action object.
    
    $edt->{current_action} = EditTransaction::Action->new($table, $operation, $record, $label);
    
    # If there are special column instructions already set for this table, copy them in.
    # $$$ this needs to be changed so that these instructions are only accessed if needed.
    
    if ( my $s = $SPECIAL_BY_CLASS{ref $edt}{$table} )
    {
	$edt->{current_action}->column_special($s);
    }
    
    if ( my $s = $edt->{column_special}{$table} )
    {
	$edt->{current_action}->column_special($s);
    }
    
    # Return the new action.
    
    return $edt->{current_action};
}


# Transaction control
# -------------------

# start_transaction ( )
# 
# Start the database transaction associated with this EditTransaction. This is done automatically
# when 'execute' is called, but can be done explicitly at an earlier stage if the checking of
# record values needs to be done inside a transaction. Returns true if the transaction is can proceed,
# false otherwise.

sub start_transaction {
    
    my ($edt) = @_;
    
    # If this transaction has already committed, throw an exception.
    
    if ( $edt->has_committed )
    {
	croak "this transaction has already committed";
    }
    
    # If we have not started the transaction yet, and there are no errors, then start it now.
    
    elsif ( ! $edt->has_started && ! $edt->errors )
    {
	$edt->_start_transaction;
    }
    
    return $edt->can_proceed;
}


# _start_transaction ( )
#
# This method does the actual work of starting a new transaction. If one is already active on this
# database connection, then we assume that something has gone wrong and issue a rollback first.

sub _start_transaction {

    my ($edt) = @_;
    
    my ($result, $save_action);

    # Check to see if there might be an active transaction on this database connection. We cannot
    # be absolutely sure, since a COMMIT or ROLLBACK might have been issued outside the scope of
    # the current module, but just in case we issue a ROLLBACK now.

    my $dbh = $edt->dbh;
    
    if ( ref $TRANSACTION_INTERLOCK{$dbh} && $TRANSACTION_INTERLOCK{$dbh}->isa('EditTransaction') )
    {
	my ($in_transaction) = $dbh->selectrow_array('SELECT @@in_transaction');

	if ( $in_transaction )
	{
	    $TRANSACTION_INTERLOCK{$dbh}->_rollback_transaction('interlock');
	}
    }
    
    $edt->debug_line( " >>> START TRANSACTION $edt->{unique_id}\n" );
    
    try {

	# Start the transaction. This will implicitly commit any uncommitted work.
	
	$edt->dbh->do("START TRANSACTION");
	$edt->{transaction} = 'active';
	
	# Save and clear the 'current action', if any, before calling
	# 'initialize_transaction'. This way, error conditions generated by that method will not
	# be attached to any specific transaction.
	
	$save_action = $edt->{current_action};
	$edt->{current_action} = undef;
	
	# Call the 'initialize_transaction' method, which is designed to be overridden by subclasses. The
	# default method does nothing.
	
	$edt->initialize_transaction($edt->{main_table});

	# Keep a reference to this transaction, so it can be rolled back if the client code tries
	# to start another transaction before this one is committed. But the reference must be a
	# weak one, because the transaction should be automatically rolled back through the
	# DESTROY method if all other references to it are destroyed.
	
	$TRANSACTION_INTERLOCK{$dbh} = $edt;
	weaken $TRANSACTION_INTERLOCK{$dbh};
    }
    
    catch {

	# If an error occurs during the above block, then mark this EditTransaction as 'aborted'.
	
	$edt->{transaction} = 'aborted';

	# If the transaction was actually started, then roll it back.
	
	my ($in_transaction) = $dbh->selectrow_array('SELECT @@in_transaction');
	
	if ( $in_transaction )
	{
	    $edt->_rollback_transaction('errors');
	}
	
	# Add an error condition, and output the exception to standard error so that it can be
	# tracked and (hopefully) debugged.
	
	my $msg = $edt->{transaction} eq 'active' ? 'an exception occurred on initialization' :
	    'an exception occurred while starting the transaction';
	
	$edt->add_condition(undef, 'E_EXECUTE', $msg);
	$edt->error_line($_);
    };

    # Restore the current action, if any.
    
    $edt->{current_action} = $save_action;
}


# commit ( )
# 
# If this EditTransaction has not yet been completed, do so. After this is done, this
# EditTransaction cannot be used for any more actions. If the operation method needs to make more
# changes to the database, a new EditTransaction must be created. This operation returns a true
# value if the transaction succeeded, false otherwise.
# 
# $$$ Perhaps I will later modify this class so that it can be used for multiple transactions in turn.

sub commit {
    
    my ($edt) = @_;

    return $edt->execute;
}


sub _commit_transaction {
    
    my ($edt) = @_;
    
    $edt->{current_action} = undef;    
    
    $edt->debug_line( " <<< COMMIT TRANSACTION $edt->{unique_id}\n" );
    
    try {
	
	$edt->dbh->do("COMMIT");
	$edt->{transaction} = 'committed';
	$edt->{commit_count}++;
    }
	
    catch {
	$edt->add_condition(undef, 'E_EXECUTE', 'an exception occurred on transaction commit');
	$edt->error_line($_);
	$edt->{transaction} = 'aborted';
	$edt->{rollback_count}++;
    };
}


# rollback ( )
#
# If this EditTransaction has not yet been completed, roll back whatever work has been done. If
# the database transaction has not yet been started, just mark the transaction 'finished' and
# return. This operation returns a true value if an active transaction was rolled back, and false
# otherwise.

sub rollback {

    my ($edt) = @_;
    
    if ( $edt->is_active )
    {
	$edt->_rollback_transaction('call');
    }
    
    else
    {
	$edt->{transaction} ||= 'finished';
    }
    
    return $edt->{transaction} eq 'aborted' ? 1 : undef;
}


sub _rollback_transaction {
    
    my ($edt, $reason) = @_;
    
    my $dbh = $edt->dbh;
    
    if ( $reason )
    {
	$reason = uc $reason;
	$edt->debug_line( " <<< ROLLBACK TRANSACTION $edt->{unique_id} FROM $reason\n" );
    }
    
    else
    {
	$edt->debug_line( " <<< ROLLBACK TRANSACTION $edt->{unique_id}\n" );
    }
    
    try {
	
	$TRANSACTION_INTERLOCK{$dbh} = undef;
	$dbh->do("ROLLBACK") if defined $dbh;
    }
    
    catch {
	$edt->add_condition(undef, 'E_EXECUTE', 'an exception occurred on transaction rollback');
	$edt->error_line($_);
    };
    
    $edt->{rollback_count}++;
    $edt->{transaction} = 'aborted';
}


# Record operations
# -----------------

# The operations in this section are called by data service operation methods to insert, update,
# and delete records in the database.


# start_execution
# 
# Call 'start_transaction' and also set the 'execute_immediately' flag. This means that subsequent
# actions will be carried out immediately on the database rather than waiting for a call to
# 'execute'. Returns true if the transaction can proceed, false otherwise.

sub start_execution {
    
    my ($edt) = @_;
    
    $edt->start_transaction;
    $edt->{execute_immediately} = 1;

    # If the transaction is now active, then execute all pending actions before we do anything
    # else.
    
    if ( $edt->is_active )
    {
	$edt->_execute_action_list;
    }
    
    return $edt->can_proceed;
}


# insert_record ( table, record )
# 
# The specified record is to be inserted into the specified table. Depending on the settings of
# this particular EditTransaction, this action may happen immediately or may be executed
# later. The record in question MUST NOT include a primary key value.

sub insert_record {
    
    my ($edt, $table, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main
    # lists, and create a new EditTransaction::Action to represent this insertion.
    
    my $action = $edt->_new_record($table, 'insert', $record);
    
    # We can only create records if specifically allowed. This may be specified by the user as a
    # parameter to the operation being executed, or it may be set by the operation method itself
    # if the operation is specifically designed to create records.
    
    if ( $edt->allows('CREATE') )
    {
	try {

	    # First check to make sure we have permission to insert a record into this table. A
	    # subclass may override this method, if it needs to make different checks than the default
	    # ones.
	    
	    my $permission = $edt->authorize_action($action, 'insert', $table);
	    
	    # If errors occurred during authorization, then we need not check further but can
	    # proceed to the next action.
	    
	    if ( $permission eq 'error' && $action->has_errors )
	    {
		return;
	    }
	    
	    # If the user does not have permission to add a record, add an error condition.
	    
	    elsif ( $permission !~ /post|admin/ )
	    {
		$edt->add_condition($action, 'E_PERM', 'insert');
	    }
	    
	    # A record to be inserted must not have a primary key value specified for it. Records with
	    # primary key values can only be passed to 'update_record' or 'replace_record'.
	    
	    if ( $action->keyval )
	    {
		$edt->add_condition($action, 'E_HAS_KEY', 'insert');
	    }
	    
	    # Then check the actual record to be inserted, to make sure that the column values meet
	    # all of the criteria for this table. If any error or warning conditions are detected,
	    # they are added to the current transaction. A subclass may override this method, if it
	    # needs to make additional checks or perform additional work.
	    
	    $edt->validate_action($action, 'insert', $table);
	    $edt->validate_against_schema($action, 'insert', $table);
	}
	
	catch {
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	    $edt->error_line($_);
	};
    }
    
    # If an attempt is made to add a record without the 'CREATE' allowance, add the appropriate
    # error condition.
    
    else
    {
	$edt->add_condition($action, 'C_CREATE');
    }
    
    # Either execute the action immediately or add it to the appropriate list depending on whether
    # or not any error conditions are found.
    
    return $edt->_handle_action($action);
}


# update_record ( table, record )
# 
# The specified record is to be updated in the specified table. Depending on the settings of this
# particular EditTransaction, this action may happen immediately or may be executed later. The
# record in question MUST include a primary key value, indicating which record to update.

sub update_record {
    
    my ($edt, $table, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being updated.
    
    my $action = $edt->_new_record($table, 'update', $record);
    
    # We can only update a record if a primary key value is specified.
    
    if ( my $keyexpr = $edt->_set_keyexpr($action) )
    {
	try {

	    # First check to make sure we have permission to update this record. A subclass may
	    # override this method, if it needs to make different checks than the default ones.
	    
	    my $permission = $edt->authorize_action($action, 'update', $table, $keyexpr);

	    # If errors occurred during authorization, then we need not check further but can
	    # proceed to the next action.
	    
	    if ( $permission eq 'error' && $action->has_errors )
	    {
		return;
	    }
	    
	    # If no such record is found in the database, add an E_NOT_FOUND condition. If this
	    # EditTransaction has been created with the 'PROCEED' or 'NOT_FOUND' allowance, it
	    # will automatically be turned into a warning and will not cause the transaction to be
	    # aborted.
	    
	    elsif ( $permission eq 'notfound' )
	    {
		$edt->add_condition($action, 'E_NOT_FOUND', $action->keyrec, $action->keyval);
	    }
	    
	    # If the record has been found but is locked, then add an E_LOCKED condition. The user
	    # would have had permission to update this record, except for the lock.
	    
	    elsif ( $permission =~ /locked/ )
	    {
		$edt->add_condition($action, 'E_LOCKED', $action->keyval);
	    }
	    
	    # If the record can be unlocked by the user, then add a C_LOCKED condition UNLESS the
	    # record is actually being unlocked by this operation, or the transaction allows
	    # 'LOCKED'. In either of those cases, we can proceed. A permission of 'unlock' means
	    # that the user does have permission to update the record if the lock is disregarded,
	    # and it implies 'admin' permission. So we proceed as if we had 'admin' permission,
	    # but add a caution unless the abovementioned conditions are met.
	    
	    elsif ( $permission =~ /unlock/ )
	    {
		unless ( $edt->allows('LOCKED') )
		{
		    $edt->add_condition($action, 'C_LOCKED', $action->keyval);
		}
		
		$action->_set_permission('admin');
	    }
	    
	    # If the user does not have permission to edit the record, add an E_PERM condition. 
	    
	    elsif ( $permission !~ /edit|admin/ )
	    {
		$edt->add_condition($action, 'E_PERM', 'update');
	    }
	    
	    # Then check the new record values, to make sure that the column values meet all of the
	    # criteria for this table. If any error or warning conditions are detected, they are added
	    # to the current transaction. A subclass may override this method, if it needs to make
	    # additional checks or perform additional work.
	    
	    $edt->validate_action($action, 'update', $table, $keyexpr);
	    $edt->validate_against_schema($action, 'update', $table);
	}
	
	catch {
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	    $edt->error_line($_);
	};
    }
    
    # If no primary key value was specified for this record, add an error condition. This will, of
    # course, have to be reported under the record label that was passed in as part of the record
    # (if one was in fact given) or else the index of the record in the input set.
    
    else
    {
	$edt->add_condition($action, 'E_NO_KEY', 'update');
    }
    
    # Either execute the action immediately or add it to the appropriate list depending on whether
    # or not any error conditions are found.
    
    return $edt->_handle_action($action);
}


# update_many ( table, selector, record )
# 
# All records matching the selector are to be updated in the specified table. Depending on the
# settings of this particular EditTransaction, this action may happen immediately or may be
# executed later. The selector may indicate a set of keys, or it may include some expression that
# selects all matching records.

sub update_many {
    
    my ($edt, $table, $selector, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being updated.
    
    my $action = $edt->_new_record($table, 'update_many', $record);
    
    $action->_set_selector($selector);
    
    try {
	# First check to make sure we have permission to update records in this table. A subclass
	# may override this method, if it needs to make different checks than the default ones.
	
	my $permission = $edt->authorize_action($action, 'update_many', $table);
	
	# If errors occurred during authorization, then we need not check further but can
	# proceed to the next action.
	
	if ( $permission eq 'error' && $action->has_errors )
	{
	    return;
	}
	
	# If the user does not have permission to edit the record, add an E_PERM condition. 
	
	if ( $permission !~ /edit|admin/ )
	{
	    $edt->add_condition($action, 'E_PERM', 'update_many');
	}
	
	# The update record must not include a key value.
	
	if ( $action->keyval )
	{
	    $edt->add_condition($action, 'E_HAS_KEY', 'update_many');
	}
    }
    
    catch {
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	$edt->error_line($_);
    };
    
    # Either execute the action immediately or add it to the appropriate list depending on whether
    # or not any error conditions are found.
    
    return $edt->_handle_action($action);
}


# replace_record ( table, record )
# 
# The specified record is to be inserted into the specified table, replacing any record that may
# exist with the same primary key value. Depending on the settings of this particular EditTransaction,
# this action may happen immediately or may be executed later. The record in question MUST include
# a primary key value.

sub replace_record {
    
    my ($edt, $table, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being replaced.
    
    my $action = $edt->_new_record($table, 'replace', $record);
    
    # We can only replace a record if a primary key value is specified.
    
    if ( my $keyexpr = $edt->_set_keyexpr($action) )
    {
	try {
	    
	    # First check to make sure we have permission to replace this record. A subclass may
	    # override this method, if it needs to make different checks than the default ones.
	    
	    my $permission = $edt->authorize_action($action, 'replace', $table, $keyexpr);
	    
	    # If errors occurred during authorization, then we need not check further but can
	    # proceed to the next action.
	    
	    if ( $permission eq 'error' && $action->has_errors )
	    {
		return;
	    }
	    
	    # If no such record is found in the database, check to see if this EditTransaction
	    # allows CREATE. If this is the case, and if the user also has 'admin' permission on
	    # this table, or if the user has 'post' and the table property ADMIN_INSERT_KEY is NOT
	    # set, then a new record will be created with the specified primary key
	    # value. Otherwise, an appropriate error condition will be added.
	    
	    elsif ( $permission eq 'notfound' )
	    {
		if ( $edt->allows('CREATE') )
		{
		    $permission = $edt->check_table_permission($table, 'insert_key');
		    
		    if ( $permission eq 'admin' || $permission eq 'insert_key' )
		    {
			$action->_set_permission($permission);
			$action->{_no_modifier} = 1;
		    }
		    
		    elsif ( $edt->allows('NOT_FOUND') )
		    {
			$edt->add_condition($action, 'E_NOT_FOUND', $action->keyrec, $action->keyval);
		    }
		    
		    else
		    {
			$edt->add_condition($action, 'E_PERM', 'replace_new', $action->keyval);
		    }
		}
		
		# If we are not allowed to create new records, add an error condition. If this
		# EditTransaction has been created with the PROCEED or NOT_FOUND allowance, it
		# will automatically be turned into a warning and will not cause the transaction to be
		# aborted.
		
		else
		{
		    $edt->add_condition($action, 'E_NOT_FOUND', $action->keyrec, $action->keyval);
		}
	    }
	    
	    # If the record has been found but is locked, then add an E_LOCKED condition. The user
	    # would have had permission to replace this record, except for the lock.
	    
	    elsif ( $permission eq 'locked' )
	    {
		$edt->add_condition($action, 'E_LOCKED', $action->keyval);
	    }
	    
	    # If the record can be unlocked by the user, then add a C_LOCKED condition UNLESS the
	    # record is actually being unlocked by this operation, or the transaction allows
	    # 'LOCKED'. In either of those cases, we can proceed. A permission of 'unlock' means
	    # that the user does have permission to update the record if the lock is disregarded.
	    
	    elsif ( $permission =~ /unlock/ )
	    {
		unless ( $edt->allows('LOCKED') )
		{
		    $edt->add_condition($action, 'C_LOCKED', $action->keyval);
		}
	    }
	    
	    # If the user does not have permission to edit the record, add an error condition. 
	    
	    elsif ( $permission !~ /edit|admin/ )
	    {
		$edt->add_condition($action, 'E_PERM', 'replace_existing');
	    }
	    
	    # Then check the new record values, to make sure that the replacement record meets all of
	    # the criteria for this table. If any error or warning conditions are detected, they are
	    # added to the current transaction. A subclass may override this method, if it needs to
	    # make additional checks or perform additional work.
	    
	    $edt->validate_action($action, 'replace', $table, $keyexpr);
	    $edt->validate_against_schema($action, 'replace', $table);
	}
	
	catch {
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	    $edt->error_line($_);
	};
    }
    
    # If no primary key value was specified for this record, add an error condition. This will, of
    # course, have to be reported under the record label that was passed in as part of the record
    # (if one was in fact given) or else the index of the record in the input set.
    
    else
    {
	$edt->add_condition($action, 'E_NO_KEY', 'replace');
    }
    
    # Create an action record, and either execute it immediately or add it to the appropriate list
    # depending on whether or not any error conditions are found.
    
    return $edt->_handle_action($action);
}


# delete_record ( table, record )
# 
# The specified record is to be deleted from the specified table. Depending on the settings of
# this particular EditTransaction, this action may happen immediately or may be executed
# later. The record in question must include a primary key value, indicating which record to
# delete. In fact, for this operation only, the $record argument may be a key value rather than a
# hash ref.

sub delete_record {

    my ($edt, $table, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being deleted.
    
    my $action = $edt->_new_record($table, 'delete', $record);
    
    # A record can only be deleted if a primary key value is specified.
    
    if ( my $keyexpr = $edt->_set_keyexpr($action) )
    {
	try {
	    
	    # First check to make sure we have permission to delete this record. A subclass may
	    # override this method, if it needs to make different checks than the default ones.
	    
	    my $permission = $edt->authorize_action($action, 'delete', $table, $keyexpr);
	    
	    # If errors occurred during authorization, then we need not check further but can
	    # proceed to the next action.
	    
	    if ( $permission eq 'error' && $action->has_errors )
	    {
		return;
	    }
	    
	    # If no such record is found in the database, add an error condition. If this
	    # EditTransaction has been created with the 'PROCEED' or 'NOT_FOUND' allowance, it
	    # will automatically be turned into a warning and will not cause the transaction to be
	    # aborted.
	    
	    elsif ( $permission eq 'notfound' )
	    {
		$edt->add_condition($action, 'E_NOT_FOUND', $action->keyrec, $action->keyval);
	    }
	    
	    # If the record has been found but is locked, then add an E_LOCKED condition. The user
	    # would have had permission to delete this record, except for the lock.
	    
	    elsif ( $permission eq 'locked' )
	    {
		$edt->add_condition($action, 'E_LOCKED', $action->keyval);
	    }
	    
	    # If the record can be unlocked by the user, then add a C_LOCKED condition UNLESS the
	    # record is actually being unlocked by this operation, or the transaction allows
	    # 'LOCKED'. In either of those cases, we can proceed. A permission of 'unlock' means
	    # that the user does have permission to update the record if the lock is disregarded.
	    
	    elsif ( $permission eq 'unlock' )
	    {
		unless ( $edt->allows('LOCKED') )
		{
		    $edt->add_condition($action, 'C_LOCKED', $action->keyval);
		}
	    }
	    
	    # If we do not have permission to delete the record, add an error condition.
	    
	    elsif ( $permission !~ /delete|admin/ )
	    {
		$edt->add_condition($action, 'E_PERM', 'delete');
	    }
	    
	    # If a 'validate_delete' method was specified, then call it. This method may abort the
	    # deletion by adding an error condition. Otherwise, we assume that the permission check we
	    # have already done is all that is necessary.
	    
	    $edt->validate_action($action, 'delete', $table, $keyexpr);
	}

	catch {
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	    $edt->error_line($_);
	};
    }
    
    # If no primary key was specified, add an error condition.
    
    else
    {
	$edt->add_condition($action, 'E_NO_KEY', 'delete');
    }
    
    # Create an action record, and then take the appropriate action.
    
    return $edt->_handle_action($action);
}


# delete_many ( table, selector )
# 
# All records matching the selector are to be deleted in the specified table. Depending on the
# settings of this particular EditTransaction, this action may happen immediately or may be
# executed later. The selector may indicate a set of keys, or it may include some expression that
# selects all matching records.

sub delete_many {
    
    my ($edt, $table, $selector) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being updated.
    
    my $action = $edt->_new_record($table, 'delete_many', { });
    
    $action->_set_selector($selector);
    
    try {
	# First check to make sure we have permission to delete records in this table. A subclass
	# may override this method, if it needs to make different checks than the default ones.
	
	my $permission = $edt->authorize_action($action, 'delete_many', $table);
	
	# If errors occurred during authorization, then we need not check further but can
	# proceed to the next action.
	
	if ( $permission eq 'error' && $action->has_errors )
	{
	    return $edt->_handle_action($action);
	}
	
	# If the user does not have permission to edit the record, add an E_PERM condition. 
	
	elsif ( $permission !~ /delete|admin/ )
	{
	    $edt->add_condition($action, 'E_PERM', 'delete_many');
	}
    }
    
    catch {
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	$edt->error_line($_);
    };
    
    # Either execute the action immediately or add it to the appropriate list depending on whether
    # or not any error conditions are found.
    
    return $edt->_handle_action($action);
}


# delete_cleanup ( table, selector )
# 
# Create an action that will cause all records from the specified table that match the specified
# selector to be deleted UNLESS they have been inserted, updated, or replaced during the current
# transaction. This action is designed to be used by a transaction that wishes to completely
# replace the set of records in a subsidiary table that are tied to one or more records in a
# superior table. Of course, it should be called as the last action in the transaction.
#
# This action is invalid on a main table.

sub delete_cleanup {
    
    my ($edt, $table, $selector) = @_;
    
    # Create a new action record for this operation.
    
    my $action = $edt->_new_record($table, 'delete_cleanup', { });

    # Make sure we have a non-empty selector, although we will need to do more checks on it later.
    
    unless ( $selector && ! ref $selector )
    {
	croak "'$selector' is not a valid selector";
    }
    
    $action->_set_selector($selector);
    
    try {
	# First check to make sure we have permission to delete records in this table. A subclass
	# may override this method, if it needs to make different checks than the default ones.
	
	my $permission = $edt->authorize_action($action, 'delete_cleanup', $table, $selector);
	
	# If errors occurred during authorization, then we need not check further but can
	# proceed to the next action.
	
	if ( $permission eq 'error' && $action->has_errors )
	{
	    return $edt->_handle_action($action);
	}
	
	# If the user does not have permission to edit the record, add an E_PERM condition. 
	
	elsif ( $permission !~ /delete|admin/ )
	{
	    $edt->add_condition($action, 'E_PERM', 'delete_cleanup');
	}
    }
    
    catch {
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	$edt->error_line($_);
    };
    
    # If the selector does not mention the linking column, throw an exception rather than adding
    # an error condition. These are static errors that should not be occurring because of bad
    # end-user input.
    
    my $linkcol = $action->linkcol;
    
    unless ( $linkcol )
    {
	croak "could not find linking column for table $table";
    }
    
    unless ( $selector =~ /\b$linkcol\b/ )
    {
	croak "'$selector' is not a valid selector, must mention $linkcol";
    }
    
    # Otherwise, either execute the action immediately or add it to the appropriate list depending on
    # whether or not any error conditions are found.
    
    return $edt->_handle_action($action);
}


# process_record ( table, record )
# 
# Call either 'insert_record' or 'update_record', depending on whether the record has a value for
# the primary key attribute. This is a convenient shortcut for use by operation methods. If the
# record contains the field '_operation', then call the method indicated by the field value.

sub process_record {
    
    my ($edt, $table, $record) = @_;
    
    if ( $record->{_operation} )
    {
	if ( $record->{_operation} eq 'delete' )
	{
	    return $edt->delete_record($table, $record);
	}

	elsif ( $record->{_operation} eq 'replace' )
	{
	    return $edt->replace_record($table, $record);
	}

	elsif ( $record->{_operation} eq 'insert' )
	{
	    return $edt->replace_record($table, $record);
	}

	elsif ( $record->{_operation} eq 'update' )
	{
	    return $edt->update_record($table, $record);
	}
	
	else
	{
	    my $action = $edt->_new_record($table, 'bad', $record);
	    $edt->add_condition($action, 'E_BAD_OPERATION', $record->{_operation});
	}
    }
    
    elsif ( $edt->get_record_key($table, $record) )
    {
	return $edt->update_record($table, $record);
    }
    
    else
    {
	return $edt->insert_record($table, $record);
    }
}


sub insert_update_record {

    my ($edt, $table, $record) = @_;

    $edt->process_record($table, $record);
}


# other_action ( table, method, record )
# 
# An action not defined by this module is to be carried out. The argument $method must be a method
# name defined in a subclass of this module, which will be called to carry out the action. The
# method name will be passed as the operation when calling subclass methods for authoriation and validation.

sub other_action {
    
    my ($edt, $table, $method, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being updated.
    
    my $action = $edt->_new_record($table, 'other', $record);
    
    $action->_set_method($method);
    
    # If we have a primary key value, we can authorize against that record. This may be a
    # limitation in future, but for now the action is authorized if they have 'update' privilege
    # on that record.
    
    if ( my $keyexpr = $edt->_set_keyexpr($action) )
    {
	try {

	    # First check to make sure we have permission to update this record. A subclass may
	    # override this method, if it needs to make different checks than the default ones.
	    
	    my $permission = $edt->authorize_action($action, 'other', $table, $keyexpr);

	    # If errors occurred during authorization, then we need not check further but can
	    # proceed to the next action.
	    
	    if ( $permission eq 'error' && $action->has_errors )
	    {
		return;
	    }
	    
	    # If no such record is found in the database, add an E_NOT_FOUND condition. If this
	    # EditTransaction has been created with the 'PROCEED' or 'NOT_FOUND' allowance, it
	    # will automatically be turned into a warning and will not cause the transaction to be
	    # aborted.
	    
	    elsif ( $permission eq 'notfound' )
	    {
		$edt->add_condition($action, 'E_NOT_FOUND', $action->keyrec, $action->keyval);
	    }

	    # If the record has been found but is locked, then add an E_LOCKED condition. The user
	    # would have had permission to update this record, except for the lock.
	    
	    elsif ( $permission eq 'locked' )
	    {
		$edt->add_condition($action, 'E_LOCKED', $action->keyval);
	    }
	    
	    # If the record can be unlocked by the user, then add a C_LOCKED condition UNLESS the
	    # record is actually being unlocked by this operation, or the transaction allows
	    # 'LOCKED'. In either of those cases, we can proceed. A permission of 'unlock' means
	    # that the user does have permission to update the record if the lock is disregarded,
	    # and it implies 'admin' permission. So we proceed as if we had 'admin' permission,
	    # but add a caution unless the abovementioned conditions are met.
	    
	    elsif ( $permission eq 'unlock' )
	    {
		unless ( $edt->allows('LOCKED') )
		{
		    $edt->add_condition($action, 'C_LOCKED', $action->keyval);
		}
		
		$action->_set_permission('admin');
	    }
	    
	    # If the user does not have permission to edit the record, add an E_PERM condition. 
	    
	    elsif ( $permission !~ /edit|admin/ )
	    {
		$edt->add_condition($action, 'E_PERM', $method);
	    }
	    
	    # Then check the new record values, to make sure that the column values meet all of the
	    # criteria for this table. If any error or warning conditions are detected, they are added
	    # to the current transaction. A subclass may override this method, if it needs to make
	    # additional checks or perform additional work.
	    
	    $edt->validate_action($action, 'other', $table, $keyexpr);
	    # $edt->validate_against_schema($action, 'other', $table);
	}
	
	catch {
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	    $edt->error_line($_);
	};
    }
    
    # If no primary key value was specified for this record, add an error condition. This will, of
    # course, have to be reported under the record label that was passed in as part of the record
    # (if one was in fact given) or else the index of the record in the input set.

    # $$$ At some point in the future, we may add the ability to carry out operations using table
    # rather than record authentication.
    
    else
    {
	$edt->add_condition($action, 'E_NO_KEY', 'update');
    }
    
    # Either execute the action immediately or add it to the appropriate list depending on whether
    # or not any error conditions are found.
    
    return $edt->_handle_action($action);
}


# do_sql ( stmt, options )
# 
# Create an action that will execute the specified SQL statement, and do nothing else. The
# execution is protected by a try block, and an E_EXECUTE condition will be added if it fails. The
# appropriate cleanup methods will be called in this case. If an options hash is provided with the
# key 'result' and a scalar reference, the result code returned by the statement execution will be
# written to that reference. 

sub do_sql {

    my ($edt, $sql, $options) = @_;
    
    # Substitute any table specifiers in the statement for the actual table names.
    
    $sql =~ s{ << (\w+) >> }{ $TABLE{$1} }xseg;
    
    my $record = { sql => $sql };
    $record->{result} = $options->{result} if ref $options eq 'HASH' && $options->{result};
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being updated.
    
    my $action = EditTransaction::Action->new('<SQL>', 'other', $record);
    
    $action->_set_method('_do_sql');
    
    return $edt->_handle_action($action);
}


# bad_record ( record )
#
# Create an action for this record, and immediately attach an error condition to it. This method
# should be called when an input record lacks the proper fields and the client code cannot figure
# out how to process it.

sub bad_record {

    my ($edt, $table, $record, $message) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being updated.
    
    my $action = $edt->_new_record($table, 'update', $record);
    
    # Then attach an error condition to this action.

    $message ||= "Bad record, necessary fields could not be found";
    
    $edt->add_condition($action, 'E_BAD_RECORD', $message);
    
    # Since the action now has an error condition, it will be placed on the 'bad' list.
    
    return $edt->_handle_action($action);
}


# get_record_key ( table, record )
# 
# Return the key value (if any) specified in this record. Look first to see if the table has a
# 'PRIMARY_FIELD' property. If so, check to see if we have a value for the named
# attribute. Otherwise, check to see if the table has a 'PRIMARY_KEY' property and check under
# that name as well. If no non-empty value is found, return undefined.

sub get_record_key {

    my ($edt, $table, $record) = @_;

    return unless ref $record eq 'HASH';
    
    if ( my $key_attr = get_table_property($table, 'PRIMARY_FIELD') )
    {
	if ( defined $record->{$key_attr} && $record->{$key_attr} ne '' )
	{
	    return $record->{$key_attr};
	}
    }
    
    if ( my $key_column = get_table_property($table, 'PRIMARY_KEY') )
    {
	if ( defined $record->{$key_column} && $record->{$key_column} ne '' )
	{
	    return $record->{$key_column};
	}
	
	elsif ( $key_column =~ /(.*)_no$/ )
	{
	    my $check_attr = "$1_id";

	    if ( defined $record->{$check_attr} && $record->{$check_attr} ne '' )
	    {
		return $record->{$check_attr};
	    }
	}
    }
    
    return;
}


# ignore_record ( )
# 
# Indicates that a particular record should be ignored. This will keep the
# record count up-to-date for generating record labels with which to tag
# subsequent error and warning messages.

sub ignore_record {

    my ($edt, $table, $record) = @_;
    
    $edt->{record_count}++;
}


# abort_action ( )
# 
# This method may be called from record validation routines defined in subclasses of
# EditTransaction, if it is determined that a particular record action should be skipped but the
# rest of the transaction should proceed.

sub abort_action {
    
    my ($edt) = @_;

    # Return without doing anything unless there is a current action.
    
    my $action = $edt->{current_action} || return;
    
    # Return also if the current action has already been executed or abandoned.
    
    return if $action->status;
    
    # If the current action has any errors or warnings, remove them from the list.
    
    $edt->_remove_conditions($action, 'errors') if $action->has_errors;
    $edt->_remove_conditions($action, 'warnings') if $action->has_warnings;
    
    # Then remove the action from the action list if it is the last item.
    
    if ( @{$edt->{action_list}} && $edt->{action_list}[-1] == $action )
    {
	pop @{$edt->{action_list}};
    }
    
    # Mark the action as 'abandoned'.
    
    $action->_set_status('abandoned');

    # Return true, to indicate that the current action was abandoned.

    return 1;
}


# aux_action ( table, operation, record )
# 
# This method is called from client code or subclass methods that wish to create auxiliary actions
# to supplement the current one. For example, adding a record to one table may involve also adding
# another record to a different table.

sub aux_action {
    
    my ($edt, $table, $operation, $record) = @_;
    
    # Create a new action, and set it as auxiliary to the current action.
    
    my $action = EditTransaction::Action->new($table, $operation, $record);
    
    $action->_set_auxiliary($edt->{current_action});
    
    # If there are special column attributes already set for this table, copy them in.
    
    if ( my $s = $SPECIAL_BY_CLASS{ref $edt}{$table} )
    {
	$edt->{current_action}->column_special($s);
    }
    
    if ( my $s = $edt->{column_special}{$table} )
    {
	$edt->{current_action}->column_special($s);
    }
    
    # Return the action.
    
    return $action;
}


sub current_action {

    my ($edt) = @_;
    
    return $edt->{current_action};
}


# authorize_action ( action, table, operation, keyexpr )
# 
# Determine whether the current user is authorized to perform the specified action. If so, store
# the indicated permission in the action record and also return it. For any operation but 'insert'
# a key expression is provided. The return value should be one of the following:
# 
# admin		the user has administrative privilege on the table, so the action is authorized
# post		the user is authorized to add new records to the table
# edit		the user is authorized to update or replace the specified record
# delete	the user is authorized to delete the specified record
# none		the user is not authorized to perform this action
# notfound	no record was found corresponding to the specified key expression
# 
# This method may be overridden by subclasses. Override methods should indicate error and warning
# conditions by calling the method 'add_condition'.

sub authorize_action {
    
    my ($edt, $action, $operation, $table, $keyexpr) = @_;
    
    die "TEST AUTHORIZE" if $TEST_PROBLEM{authorize};
    
    my $permission;
    
    # First check whether the SUPERIOR_TABLE property is set for the specified table. If so,
    # then the authorization check needs to be done on this other table first.
    
    if ( my $alt_table = get_table_property($table, 'SUPERIOR_TABLE') )
    {
	$permission = $edt->authorize_subordinate_action($action, $operation, $table, $alt_table, $keyexpr);
    }
    
    # Otherwise carry out the appropriate check for this operation directly on its own table.

    else
    {
	if ( $operation eq 'delete_cleanup' )
	{
	    croak "the action 'delete_cleanup' is only valid on a subordinate table";
	}
	
	sswitch ( $operation )
	{
	    case 'insert': {
		$permission = $edt->check_table_permission($table, 'post');
	    }
	    
	    case 'update':
	    case 'replace': {
		$permission = $edt->check_record_permission($table, 'edit', $keyexpr);
	    }
	    
	    case 'update_many': {
		$permission = $edt->check_many_permission($table, 'edit', $keyexpr);
	    }
	    
	    case 'delete': {
		$permission = $edt->check_record_permission($table, 'delete', $keyexpr);
	    }

	    case 'delete_many': {
		$permission = $edt->check_many_permission($table, 'delete', $keyexpr);
	    }

	    case 'delete_cleanup': {
		croak "the operation '$operation' can only be done on a subordinate table";
	    }

	    case 'other': {
		$permission = $edt->check_record_permission($table, 'edit', $keyexpr);
	    }
	    
	    default: {
		croak "bad operation '$operation'";
	    }
	};
    }
    
    # If any errors have been generated for this action, then set the permission to 'error'. This
    # will keep additional redundant errors from being added to the action.
    
    if ( $action->has_errors )
    {
	$permission = 'error';
    }

    # Store the permission with the action and return it.
    
    return $action->_set_permission($permission);
}


# authorize_subordinate_action ( action, operation, table, alt_table, keyexpr )
#
# Carry out the authorization operation where the actual table to be checked ($alt_table) is
# different from the one on which the action is being executed ($table).

sub authorize_subordinate_action {

    my ($edt, $action, $operation, $table, $sup_table, $keyexpr) = @_;
    
    if ( $operation eq 'update_many' || $operation eq 'delete_many' )
    {
	croak "Operation '$operation' is not yet implemented on subordinate tables.";
    }
    
    # If the action table is subordinate to another table, the action table must contain a column
    # that links records in this table to records in the superior table (permission table). We need to
    # start by determining what that column is.
    
    my $linkcol = get_table_property($table, 'SUPERIOR_KEY');
    my $sup_keycol = get_table_property($sup_table, 'PRIMARY_KEY');
    
    # If no linking column was specified, assume that it is named the same as the primary key
    # of the permission table.
    
    croak "SUPERIOR_TABLE was specified as '$sup_table' but no key column was found"
	unless $linkcol || $sup_keycol;
    
    $linkcol ||= $sup_keycol;

    $action->_set_linkcol($linkcol);
    
    # If we were given a key expression for this record, fetch the current value for the
    # linking column from that row.
    
    my ($keyval, $linkval, $new_linkval, $record_col);
    
    if ( $keyexpr )
    {
	# $$$ This needs to be updated to allow for multiple $linkval keys!!!

	try {
	    ($linkval) = $edt->get_old_values($table, $keyexpr, $linkcol);
	}
	
	catch {
	    $edt->add_condition($action, 'E_EXECUTE', "bad selector '$keyexpr'");
	    $edt->error_line($_);
	};
	
	unless ( $linkval )
	{
	    $edt->add_condition($action, 'E_NOT_FOUND', $action->keyrec || $action->keycol, $action->keyval);
	    return 'error';
	}
    }
    
    # Then fetch the new value, if any, from the action record. But not for a 'delete_cleanup'
    # operation, for which that is not applicable.
    
    if ( $operation eq 'insert' || $operation eq 'update' || $operation eq 'replace' || $operation eq 'other' )
    {
	($new_linkval, $record_col) = $edt->record_col($action, $table, $linkcol);
    }
    
    # If we don't have one or the other value, that is an error.
    
    unless ( $linkval || $new_linkval )
    {
	$edt->add_condition($action, 'E_REQUIRED', $linkcol);
	return 'error';
    }
    
    # If we have both and they differ, that is also an error. It is disallowed to use an 'update'
    # operation to switch the association of a subordinate record to a different superior record.
    
    if ( $linkval && $new_linkval && $linkval ne $new_linkval )
    {
	$edt->add_condition($action, 'E_BAD_UPDATE', $record_col);
	return 'error';
    }
    
    # Now that these two conditions have been checked, we make sure that $linkval has the proper
    # value in it regardless of whether it is new (i.e. for an 'insert') or old (for other operations).
    
    $linkval ||= $new_linkval;
    
    # Now store this value in the action, for later record-keeping.
    
    $action->_set_linkval($linkval);
    
    # If we have a cached permission result for this linkval, then just return that. There is no
    # reason to look up the same superior record multiple times in the course of a single
    # transaction.
    
    my $alt_permission;
    
    if ( $edt->{linkval_cache}{$linkval} )
    {
	$alt_permission = $edt->{linkval_cache}{$linkval};
    }
    
    # If the link value is a label, then it must represent a record either updated or inserted
    # into the proper table by the current user earlier in the transaction. So all we need to do
    # is check that it represents the proper type of record, and if so we return the indicated
    # permission.
    
    elsif ( $linkval =~ /^@(.*)/ )
    {
	my $label = $1;
	
	# If the 'label_found' entry for this label exists and matches $sup_table, then we are
	# okay as far as authorization of this action goes. If the action corresponding to the
	# label failed its own authorization check, then it will never be executed and the
	# corresponding key value will not be recorded. Therefore, execution of the current action
	# will fail with a 'E_LABEL_NOT_FOUND' error. So this has left no security hole (I think.)
	
	if ( $edt->{label_found}{$label} && $edt->{label_found}{$label} eq $sup_table )
	{
	    $alt_permission = $edt->check_table_permission($sup_table, 'post');
	    $edt->{linkval_cache}{$linkval} = $alt_permission;
	    
	    # # If we have 'admin' permission on the superior table, then we return 'admin'.
	    
	    # if (  eq 'admin' )
	    # {
	    # 	$permission = 'admin';
	    # 	$edt->{linkval_cache}{$linkval} = $permission;
	    # }
	    
	    # # Otherwise, we return the proper permission for the operation we are doing.
	    
	    # elsif ( $operation eq 'insert' )
	    # {
	    # 	$permission = 'post';
	    # 	$edt->{linkval_cache}{$linkval} = 'edit';
	    # }
	    
	    # elsif ( $operation eq 'delete' )
	    # {
	    # 	$permission = 'delete';
	    # 	$edt->{linkval_cache}{$linkval} = 'edit';
	    # }
	    
	    # else
	    # {
	    # 	$permission = 'edit';
	    # 	$edt->{linkval_cache}{$linkval} = 'edit';
	    # }
	    
	    # return $permission;
	}
	
	else
	{
	    $edt->add_condition($action, 'E_LABEL_NOT_FOUND', $record_col, $label);
	    return 'error';
	}
    }
    
    # Otherwise, we generate a key expression for the superior record so that we can check permissions
    # on that. If we cannot generate one, then we return with an error. The 'aux_keyexpr'
    # routine will already have added one or more error conditions in this case.
    
    else
    {
	my $alt_keyexpr = $edt->aux_keyexpr($action, $sup_table, $sup_keycol, $linkval, $record_col);
	
	unless ( $alt_keyexpr )
	{
	    return 'error';
	}
	
	# Now we carry out the permission check on the permission table. The permission check is for
	# modifying the superior record, since that is essentially what we are doing. Whether we are
	# inserting, updating, or deleting subordinate records, that essentially counts as modifying
	# the superior record.
	
	$alt_permission = $edt->check_record_permission($sup_table, 'edit', $alt_keyexpr);
	$edt->{linkval_cache}{$linkval} = $alt_permission;
    }
    
    # Now, if the alt permission is 'admin', then the subordinate permission must be as well.
    
    if ( $alt_permission =~ /admin/ )
    {
	return $alt_permission;
    }
    
    # If the alt permission is 'edit', then we need to figure out what subordinate permission we
    # are being asked for and return that.
    
    elsif ( $alt_permission =~ /edit|post/ )
    {
	my $unlock = $alt_permission =~ /unlock/ ? ',unlock' : '';
	
	if ( $operation eq 'insert' )
	{
	    return 'post';
	}
	
	elsif ( $operation eq 'delete' || $operation eq 'delete_many' || $operation eq 'delete_cleanup' )
	{
	    return "delete$unlock";
	}
	
	elsif ( $operation eq 'update' || $operation eq 'update_many' || $operation eq 'replace' || $operation eq 'other' )
	{
	    return "edit$unlock";
	}
	
	else
	{
	    croak "bad subordinate operation '$operation'";
	}
    }
    
    # If the returned permission is 'notfound', then the record that is supposed to be linked to
    # does not exist. This should generate an E_KEY_NOT_FOUND.
    
    elsif ( $alt_permission eq 'notfound' )
    {
	$record_col ||= '';
	$edt->add_condition($action, 'E_KEY_NOT_FOUND', $record_col, $linkval);
	return 'error';
    }
    
    # Otherwise, the permission returned should be 'none'. So return that.
    
    else
    {
	return 'none';
    }
}


# _handle_action ( action )
# 
# Handle the specified action record. If errors were generated for this record, put it on the 'bad
# record' list. Otherwise, either execute it immediately or put it on the action list to be
# executed later.

sub _handle_action {
    
    my ($edt, $action) = @_;
    
    # If this transaction has already committed, throw a real exception. Client code should never
    # try to execute operations on a transaction that has already committed. (One that has been
    # rolled back is different. The status of such a transaction will clearly indicate that the
    # operations will not actually be carried out.)
    
    if ( $edt->{transaction} && $edt->{transaction} eq 'committed' )
    {
	croak "This transaction has already been committed";
    }
    
    # If this action was abandoned, then ignore it. A new action's status is empty, and any
    # non-empty value means that the action should be ignored.
    
    if ( $action->status )
    {
	return;
    }
    
    # If any errors were already generated for the record currently being processed, put this
    # action on the 'bad action' list and update the counts and key lists. We then immediately
    # return without doing anything more.
    
    if ( $action->has_errors )
    {
	push @{$edt->{bad_list}}, $action;
	$edt->{fail_count}++;
	
	my $table = $action->table;
	
	# For a multiple action, all of the failed keys are put on the list.
	
	if ( $action->is_multiple )
	{
	    push @{$edt->{failed_keys}{$table}}, $action->all_keys;
	}
	
	elsif ( my $keyval = $action->keyval )
	{
	    push @{$edt->{failed_keys}{$table}}, $keyval;
	}
	
	return;
    }
    
    # If errors were generated for previous records, then there is no point in proceeding with
    # this action since the edit transaction will either never be started or will be subsequently
    # rolled back. Since we already know that no errors were generated for this particular record,
    # there is nothing more that needs to be done.
    
    elsif ( $edt->errors )
    {
	$edt->{skip_count}++;
	return;
    }
    
    # If we get here, then there is nothing to prevent the action from being executed. If the
    # 'execute immediately' flag has been turned on, then execute this action now.
    
    elsif ( $edt->{execute_immediately} )
    {
	return $edt->_execute_action($action);
    }
    
    # Otherwise, we push it on the action list for later execution. We return 1 in this case, to
    # indicate that the operation has succeeded up to this point.
    
    else
    {
	push @{$edt->{action_list}}, $action;
	return 1;
    }
}


# execute_action ( action )
#
# This method is designed to be called either internally by this class or explicitly by code from
# other classes. It executes a single action, and returns the result.

sub execute_action {
    
    my ($edt, $action) = @_;
    
    # If the action has already been executed or abandoned, throw an exception.

    croak "you must specify an action" unless $action && $action->isa('EditTransaction::Action');
    croak "that action has already been executed or abandoned" if $action->status;
    
    # If errors have already occurred, then do nothing.
    
    if ( $edt->errors )
    {
	$edt->{skip_count}++;
	return;
    }

    # If we haven't already started the transaction in the database, do so now.
    
    elsif ( ! $edt->has_started )
    {
	$edt->_start_transaction;
    }

    # Now execute the action.

    return $edt->_execute_action($action);
}


sub _execute_action {

    my ($edt, $action) = @_;

    my $result;
    
    # Call the appropriate routine to execute this operation.
    
    sswitch ( $action->operation )
    {
	case 'insert': {
	    $result = $edt->_execute_insert($action);
	}
	
	case 'update': {
	    $result = $edt->_execute_update($action);
	}

	case 'update_many': {
	    $result = $edt->_execute_update_many($action);
	}
	
	case 'replace': {
	    $result = $edt->_execute_replace($action);
	}
	
	case 'delete': {
	    $result = $edt->_execute_delete($action);
	}

	case 'delete_cleanup': {
	    $result = $edt->_execute_delete_cleanup($action);
	}

	case 'delete_many': {
	    $result = $edt->_execute_delete_many($action);
	}

	case 'other': {
	    $result = $edt->_execute_other($action);
	}
	
        default: {
	    croak "bad operation '$_'";
	}
    }
    
    # If errors have occurred, then we return false. Otherwise, return the result of the
    # execution.
    
    if ( $edt->errors )
    {
	return undef;
    }
    
    else
    {
	return $result;
    }
}


# execute ( )
# 
# Start a database transaction, if one has not already been started. Then execute all of the
# pending insert/update/delete operations, and then either commit or rollback as
# appropriate. Returns a true value if the transaction succeeded, and false otherwise.

sub execute {
    
    my ($edt) = @_;

    $edt->{current_action} = undef;
    
    # Return immediately if the transaction has already finished.
    
    if ( $edt->has_finished )
    {
	return $edt->{transaction} eq 'committed' ? 1 : undef;
    }
    
    # If there are no actions to do, and none have been done so far, and no errors have already
    # occurred, then add C_NO_RECORDS unless the NO_RECORDS condition is allowed. This will cause
    # the transaction to be immediately aborted.
    
    unless ( @{$edt->{action_list}} || @{$edt->{errors}} || $edt->{action_count} || $edt->allows('NO_RECORDS') )
    {
	$edt->add_condition(undef, 'C_NO_RECORDS');
    }
    
    # If errors have already occurred (i.e. when records were checked for insertion or updating),
    # then return without doing anything. If a transaction is already active, then roll it
    # back. If the transaction has already been initialized and a cleanup_transaction method is
    # defined for this class, call it.
    
    if ( $edt->errors )
    {
	if ( $edt->is_active )
	{
	    try {
		
		$edt->cleanup_transaction($edt->{main_table});
	    }
	    
	    catch {
		$edt->add_condition(undef, 'E_EXECUTE', 'an exception occurred during cleanup');
		$edt->error_line($_);
	    };
	    
	    $edt->_rollback_transaction('errors');
	}
	
	if ( $edt->{action_list} )
	{
	    $edt->{skip_count} += scalar(@{$edt->{action_list}});
	    $edt->{action_list} = [ ];
	}
	
	$edt->{transaction} ||= 'finished';
	return undef;
    }
    
    # Now execute any pending actions. Unless immediate mode has been turned on, these will
    # include all actions that have been done on this transaction.
    
    $edt->_execute_action_list;
    
    # Now we need to finish the transaction. If errors have occurred, then call the
    # 'cleanup_transaction' method. Otherwise, call 'finalize_transaction'.
    
    my ($result, $culprit);
    
    try {
	
	# If errors have occurred, then call the 'cleanup_transaction' method, which is designed to
	# be overridden by subclasses. The default does nothing.
	
	if ( $edt->errors )
	{
	    $culprit = 'cleanup';
	    $edt->cleanup_transaction($edt->{main_table});
	}
	
	# Otherwise, we call 'finalize_transaction'. This too is designed to be overridden by
	# subclasses, and the default does nothing.
	
	else
	{
	    $edt->{current_action} = undef;
	    $culprit = 'execution';
	    $edt->finalize_transaction($edt->{main_table});
	}
    }
    
    # If an error occurs, we add an error condition, which will cause the transaction to be
    # aborted below.
    
    catch {
	
	$edt->add_condition(undef, 'E_EXECUTE', "an exception was thrown during $culprit");
	$edt->error_line($_);
    };
    
    # At this point, we need to do a final check to see if there are any errors accumulated for
    # this transaction. An error condition could have been added explicitly by the
    # finalize or cleanup method, or could have been generated if that method died.
    
    try {
	
	# If there are any errors, then roll back the transaction.
	
	if ( $edt->errors )
	{
	    $culprit = 'rollback';
	    $edt->_rollback_transaction('errors');
	}
	
	# Otherwise, we're good to go! Yay!
	
	else
	{
	    $culprit = 'commit';
	    $edt->_commit_transaction;
	    $result = 1;
	}
    }

    catch {

	$edt->add_condition(undef, 'E_EXECUTE', "an exception was thrown during $culprit");
	$edt->error_line($_);
    };
    
    return $result;
}


# _execute_action_list ( )
#
# Execute any pending actions. This is called one of two ways: either by 'execute' or by 'start_execution'.

sub _execute_action_list {

    my ($edt) = @_;
    
    # The main part of this routine is executed inside a try block, so that we can roll back the
    # transaction if any errors occur.
    
    my $result;
    my $cleanup_called;
    
    try {
	
	# If we haven't already executed 'start_transaction' on the database, do so now.
	
	$edt->_start_transaction unless $edt->has_started;
	
	# Then go through the action list and execute each action in turn. If there are multiple
	# deletes in a row on the same table, these can be handled with a single call for
	# efficiency.
	
	while ( my $action = shift @{$edt->{action_list}} )
	{
	    $edt->{current_action} = $action;
	    
	    # If any errors have accumulated on this transaction, skip all remaining actions. This
	    # includes any errors that may have been generated by the previous action.
	    
	    if ( $edt->errors )
	    {
		$edt->{skip_count}++;				# the one we just shifted
		$edt->{skip_count} += @{$edt->{action_list}};	# any remaining
		$edt->{action_list} = [ ];			# clear the action list
		last;
	    }
	    
	    # If this particular action has been aborted, then skip it.

	    elsif ( $action->status )
	    {
		next;
	    }
	    
	    # If this particular action has any errors, then skip it. We need to do this check
	    # separately, because if PROCEED has been set for this transaction then any
	    # errors that were generated during validation of this action will have been converted
	    # to warnings. But in that case, $action->has_errors will still return true, and this
	    # action should not be executed.
	    
	    elsif ( $action->has_errors )
	    {
		$edt->{fail_count}++;
		next;
	    }
	    
	    # Now execute the appropriate handler for this action's operation.
	    
	    sswitch ( $action->operation )
	    {
		case 'insert': {
		    $edt->_execute_insert($action);
		}
		
		case 'update': {
		    $edt->_execute_update($action);
		}

		case 'update_many': {
		    $edt->_execute_update_many($action);
		}
		
		case 'replace': {
		    $edt->_execute_replace($action);
		}
		
		case 'delete': {
		    
		    # If we are allowing multiple deletion and there are more actions remaining, check
		    # to see if the immediately subsequent ones are also deletes on the same table and
		    # with the same permission. If so, coalesce them all into one action.
		    
		    if ( $edt->allows('MULTI_DELETE') && @{$edt->{action_list}} )
		    {
			my @additional;
			my $table = $action->table;
			my $permission = $action->permission;
			
			while ( my $next = $edt->{action_list}[0] )
			{
			    if ( $next->operation eq 'delete' && 
				 $next->table eq $table &&
				 $next->permission eq $permission )
			    {
				next if $action->has_errors;
				push @additional, shift(@{$edt->{action_list}});
				last unless @additional < $MULTI_DELETE_LIMIT;
			    }
			    
			    else
			    {
				last;
			    }
			}
			
			if ( @additional )
			{
			    $action->_coalesce($edt->{label_keys}, @additional);
			    $edt->_set_keyexpr($action);
			}
		    }
		    
		    # Now execute the action.
		    
		    $edt->_execute_delete($action);
		}
		
		case 'delete_cleanup' : {
		    $edt->_execute_delete_cleanup($action);
		}
		
		case 'delete_many': {
		    $edt->_execute_delete_many($action);
		}

		case 'other': {
		    $edt->_execute_other($action);
		}
		
	        default: {
		    croak "bad operation '$_'";
		}
	    }
	}
    }
    
    # If an exception is caught, we add an error condition. This will stop any further execution.
    
    catch {
	
	$edt->add_condition('E_EXECUTE', 'an exception was thrown during execution');
	$edt->debug_line($_);
	
	# Add to the skip count all remaining actions.
	
	$edt->{skip_count} += scalar(@{$edt->{action_list}});
    };
}


# _set_keyexpr ( action )
# 
# Generate a key expression for the specified action, that will select the particular record being
# acted on. If the action has no key value (i.e. is an 'insert' operation) or if no key column is
# known for this table then return '0'. The reason for returning this value is that it can be
# substituted into an SQL 'WHERE' clause and will be syntactically correct but always false.

sub _set_keyexpr {
    
    my ($edt, $action) = @_;
    
    my $keycol = $action->keycol;
    my $keyval = $action->keyval;
    my $keyexpr;

    # If we have already computed a key expression, even if it is 0, return it.
    
    if ( defined($keyexpr = $action->keyexpr) )
    {
	return $keyexpr;
    }
    
    # Otherwise, if there is no key column then the key expression is just 0.
    
    elsif ( ! $keycol )
    {
	$action->_set_keyexpr('0');
	return '0';
    }

    # If we get here, then we need to compute the key expression.
    
    if ( $action->is_multiple )
    {
	my $dbh = $edt->dbh;
	my @keys = map { $dbh->quote($_) } $action->all_keys;
	
	unless ( @keys )
	{
	    $action->_set_keyexpr('0');
	    return '0';
	}
	
	$keyexpr = "$keycol in (" . join(',', @keys) . ")";
    }
    
    elsif ( defined $keyval && $keyval ne '' && $keyval ne '0' )
    {
	if ( $keyval =~ /^@(.*)/ )
	{
	    my $label = $1;
	    
	    if ( $keyval = $edt->{label_keys}{$label} )
	    {
		$action->_set_keyval($keyval);
	    }
	    
	    else
	    {
		$edt->add_condition($action, 'E_LABEL_NOT_FOUND', $keycol, $label);
	    }
	}

	elsif ( $keyval =~ /^[0-9+]$/ )
	{
	    # do nothing
	}

	elsif ( $keyval =~ $IDRE{LOOSE} )
	{
	    my $type = $1;
	    my $num = $2;
	    
	    my $exttype = $COMMON_FIELD_IDTYPE{$keycol};
	    
	    if ( $exttype )
	    {
		if ( $type eq $exttype )
		{
		    $keyval = $num;
		}

		else
		{
		    $edt->add_condition($action, 'E_EXTTYPE', $keycol, "external identifier must be of type '$exttype', was '$type'");
		}
	    }
	    
	    else
	    {
		$edt->add_condition($action, 'E_EXTTYPE', $keycol, "no external identifier is defined for this primary key");
	    }
	}
	
	$keyexpr = "$keycol=" . $edt->dbh->quote($keyval);
    }
    
    if ( $keyexpr )
    {
	$action->_set_keyexpr($keyexpr);
	return $keyexpr;
    }

    else
    {
	$action->_set_keyexpr('0');
	return '0';
    }
}


# get_keyexpr ( action )
#
# If the action already has a key expression, return it. Otherwise, generate one and return it.

sub get_keyexpr {
    
    my ($edt, $action) = @_;

    if ( my $keyexpr = $action->keyexpr )
    {
	return $keyexpr;
    }

    else
    {
	return $edt->_set_keyexpr($action);
    }
}


# aux_keyexpr ( table, keycol, keyval )
#
# Generate a key expression that will select the indicated record from the table.

sub aux_keyexpr {
    
    my ($edt, $action, $table, $keycol, $keyval, $record_col) = @_;

    if ( $action )
    {
	$table ||= $action->table;
	$keycol ||= $action->keycol;
	$keyval ||= $action->keyval;
	$record_col ||= $action->keyrec;
    }
    
    # If we are given an empty key value, or '0', then we cannot generate a key expression. Add an
    # E_REQUIRED error condition to the action, and return.
    
    if ( ! defined $keyval || $keyval eq '0' || $keyval eq '' )
    {
	$edt->add_condition($action, 'E_REQUIRED', $record_col) if $action;
	return 0;
    }

    # If we are given a positive integer value, we can use that directly.
    
    elsif ( $keyval =~ /^[0-9]+$/ && $keyval > 0 )
    {
	return "$keycol='$keyval'";
    }

    # If we are given a label, we need to look it up and find the key value to which the label
    # refers. We also need to check to make sure that this label refers to a key value in the
    # proper table.
    
    elsif ( $keyval =~ /^@(.*)/ )
    {
	my $label = $1;
	my $lookup_val = $edt->{label_keys}{$label};
	
	if ( $lookup_val && $edt->{label_found}{$label} eq $table )
	{
	    return "$keycol='$lookup_val'";
	}
	
	else
	{
	    $edt->add_condition($action, 'E_LABEL_NOT_FOUND', $record_col, $label) if $action;
	    return 0;
	}
    }

    # Otherwise, check if this column supports external identifiers. If it matches the pattern for
    # an external identifier of the proper type, then we can use the extracted numeric value to
    # generate a key expression.
    
    elsif ( my $exttype = $COMMON_FIELD_IDTYPE{$keycol} || get_column_property($table, $keycol, 'EXTID_TYPE') )
    {
	if ( $keyval =~ $IDRE{$exttype} )
	{
	    if ( defined $2 && $2 > 0 )
	    {
		return "$keycol='$2'";
	    }

	    else
	    {
		$edt->add_condition($action, 'E_RANGE', $record_col,
				    "value does not specify a valid record") if $action;
		
		return 0;
	    }
	}
    }
    
    # Otherwise, we weren't given a valid value.
    
    else
    {
	$edt->add_condition($action, 'E_FORMAT', $record_col,
			    "value does not correspond to a record identifier") if $action;
	return 0;
    }
}


# record_col ( action, table, column )
# 
# Return a list of two values. The first is the value from the specified action's record that
# corresponds to the specified column in the database, or undef if no matching value found. The
# second is the key under which that value was found in the action record.

sub record_col {
    
    my ($edt, $action, $table, $col) = @_;
    
    # First we need to grab the schema for this table, if we don't already have it.
    
    my $schema;
    
    unless ( $schema = $TableData::SCHEMA_CACHE{$table} )
    {
	my $dbh = $edt->dbh;
	$schema = get_table_schema($dbh, $table, $edt->debug);
    }
    
    # If the action record is not a hashref, then we can return a value only if the operation is
    # 'delete' and the column being asked for is the primary key. Otherwise, we must return
    # undef. In either case, we return no second value because there is no column.
    
    my $record = $action->{record};
    
    unless ( ref $record eq 'HASH' )
    {
        if ( $action->{operation} eq 'delete' && $col eq $action->{keycol} &&
	     defined $record )
	{
	    return $record;
	}
	
	else
	{
	    return;
	}
    }
    
    # If the column is the key column, then we need to check both that column name and the primary
    # attribute if any.

    if ( $col eq $action->{keycol} && ! exists $record->{$col} )
    {
	if ( my $alt = get_table_property($table, 'PRIMARY_FIELD') )
	{
	    return ($record->{$alt}, $alt);
	}
    }
    
    # Otherwise, we need to check for a value under the column name. If there isn't one, then we need to
    # check the alternate name if any.

    my $cr = $schema->{$col};

    if ( exists $record->{$col} && ! $cr->{ALTERNATE_ONLY} )
    {
	return ($record->{$col}, $col);
    }
    
    elsif ( my $alt = $cr->{ALTERNATE_NAME} )
    {
	if ( exists $record->{$alt} )
	{
	    (return $record->{$alt}, $alt);
	}
    }
    
    # Otherwise, we must return undefined.

    return;
}


# # record_has_col ( action, table, column )
# #
# # This is like the previous routine, except that it returns true if the specified column is
# # mentioned in the action record, and false otherwise. The value specified for the column is not
# # checked.

# sub record_has_col {
    
#     my ($edt, $action, $table, $col) = @_;
    
#     # First we need to grab the schema for this table, if we don't already have it.
    
#     unless ( my $schema = $TableData::SCHEMA_CACHE{$table} )
#     {
# 	my $dbh = $edt->dbh;
# 	$schema = get_table_schema($dbh, $table, $edt->debug);
#     }
    
#     # If the action record is not a hashref, then we can return a true only if the operation is
#     # 'delete' and the column being asked for is the primary key. Otherwise, we must return undef.
    
#     my $record = $action->{record};
    
#     unless ( ref $record eq 'HASH' )
#     {
#         return $action->{operation} eq 'delete' && $col eq $action->{keycol} && defined $record;
#     }
    
#     # Otherwise, we need to check for the existence of the column name in the record hash. If it
#     # isn't there, then we need to check the alternate name if any.
    
#     my $cr = $schema->{$col};

#     if ( exists $record->{$col} && ! $cr->{ALTERNATE_ONLY} )
#     {
# 	return 1;
#     }

#     elsif ( $cr->{ALTERNATE_NAME} )
#     {
# 	return exists $record->{$cr->{ALTERNATE_NAME}};
#     }
    
#     else
#     {
# 	return;
#     }
# }


# get_old_values ( action, table, fields )
# 
# Fetch the specified columns from the table record whose key is given by the specified action.

sub get_old_values {

    my ($edt, $table, $keyexpr, $fields) = @_;
    
    # croak "get_old_values cannot be called on a multiple action" if $action->is_multiple;
    
    return () unless $keyexpr;
    
    my $sql = "SELECT $fields FROM $TABLE{$table} WHERE $keyexpr";
    
    $edt->debug_line("$sql\n");
    
    return $edt->dbh->selectrow_array($sql);
}


# fetch_old_record ( action, table, key_expr )
# 
# Fetch the old version of the specified record, if it hasn't already been fetched.

sub fetch_old_record {
    
    my ($edt, $action, $table, $keyexpr) = @_;

    if ( my $old = $action->old_record )
    {
	return $old;
    }
    
    $table ||= $action->table;
    $keyexpr ||= $action->keyexpr;
    
    return unless $table && $keyexpr;
    
    my $sql = "SELECT * FROM $TABLE{$table} WHERE $keyexpr";
    
    $edt->debug_line("$sql\n");
    
    return $edt->dbh->selectrow_hashref($sql);
}


# check_permission ( action, key_expr )
# 
# Determine the current user's permission to do the specified action.

# sub check_permission {
    
#     my ($edt, $action, $keyexpr) = @_;
    
#     my $table = $action->table;

#     sswitch ( $action->operation )
#     {
# 	case 'insert': {
# 	    return $action->_set_permission($edt->check_table_permission($table, 'post'))
# 	}
	
# 	case 'update': {
# 	    $keyexpr ||= $action->keyexpr;
# 	    return $action->_set_permission($edt->check_record_permission($table, 'edit', $keyexpr));
# 	}
        
#         case 'replace': {
# 	    $keyexpr ||= $edt->get_keyexpr($action);
# 	    my $permission = $edt->check_record_permission($table, 'edit', $keyexpr);
# 	    if ( $permission eq 'notfound' )
# 	    {
# 		$permission = $edt->check_table_permission($table, 'insert_key');
# 	    }
# 	    return $action->_set_permission($permission);
# 	}
	
# 	case 'delete': {
# 	    $keyexpr ||= $edt->get_keyexpr($action);
# 	    return $action->_set_permission($edt->check_record_permission($table, 'delete', $keyexpr));
# 	}
	
#       default: {
# 	    croak "bad operation '$_'";
# 	}
#     }
# }


# _execute_insert ( action )
# 
# Actually perform an insert operation on the database. The record keys and values should already
# have been checked by 'validate_record' or some other code, and lists of columns and values
# generated.

sub _execute_insert {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    # Set this as the current action.

    $edt->{current_action} = $action;
    
    # If we need to substitute any key values for labels, do that now.
    
    if ( $action->label_sub )
    {
	$edt->_substitute_labels($action) || return;
    }
    
    # Check to make sure that we actually have column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'column/value mismatch on insert');
	return;
    }
    
    # If the following flag is set, deliberately generate an SQL error for
    # testing purposes.
    
    if ( $TEST_PROBLEM{insert_sql} )
    {
	push @$cols, 'XXXX';
    }
    
    # Construct the INSERT statement.
    
    my $dbh = $edt->dbh;
    
    my $column_list = join(',', @$cols);
    my $value_list = join(',', @$vals);
        
    my $sql = "	INSERT INTO $TABLE{$table} ($column_list)
		VALUES ($value_list)";
    
    $edt->debug_line("$sql\n");
    
    my ($result, $cleanup_called, $new_keyval);
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    try {
	
	# Start by calling the 'before_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing.
	
	$edt->before_action($action, 'insert', $table);
	
	# Then execute the insert statement itself, provided there were no errors and the action
	# was not aborted.

	unless ( $action->status )
	{
	    $result = $dbh->do($sql);
	    $action->_set_status('executed');
	    
	    if ( $result )
	    {
		$new_keyval = $dbh->last_insert_id(undef, undef, undef, undef);
	    }
	}
	
	# Finaly, call the 'after_action' method. This is designed to be overridden by subclasses,
	# and can be used to do any necessary auxiliary actions to the database. If the insert
	# failed, then 'cleanup_action' is called instead.
	
	if ( $new_keyval )
	{
	    $action->_set_keyval($new_keyval);
	    $edt->after_action($action, 'insert', $table, $new_keyval);
	}
	
	else
	{
	    $cleanup_called = 1;
	    $edt->add_condition($action, 'W_EXECUTE', 'insert statement failed') unless $action->status eq 'aborted';
	    $edt->cleanup_action($action, 'insert', $table);
	    $result = undef;
	}
    }
    
    catch {
	if ( /duplicate entry '(.*)' for key '(.*)' at/i )
	{
	    my $value = $1;
	    my $key = $2;
	    $edt->add_condition($action, 'E_DUPLICATE', $value, $key);
	}

	else
	{
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	}
	
	$edt->error_line($_);
	
	$action->_set_status('exception') unless $action->status;
	
	unless ( $cleanup_called )
	{
	    try {
		$edt->cleanup_action($action, 'insert', $table);
	    }
	    
	    catch {
		$edt->add_condition($action, 'E_EXECUTE', 'an exception was thrown during cleanup');
		$edt->error_line($_);
	    };
	}
    };
    
    # If the insert succeeded, return the new primary key value. Also record this value so that it
    # can be queried for later. Otherwise, return undefined.
    
    if ( $new_keyval )
    {
	$edt->{action_count}++;
	push @{$edt->{inserted_keys}{$table}}, $new_keyval;
	push @{$edt->{datalog}}, EditTransaction::LogEntry->new($new_keyval, $action);
	
	if ( my $linkval = $action->linkval )
	{
	    if ( $linkval =~ /^[@](.*)/ )
	    {
		$linkval = $edt->{label_keys}{$1};
		$edt->add_condition($action, 'E_EXECUTE', 'link value label was not found') unless $linkval;
	    }
	    
	    $edt->{superior_keys}{$table}{$linkval} = 1;
	}
	
	my $label = $action->label;
	if ( defined $label && $label ne '' )
	{
	    $edt->{label_keys}{$label} = $new_keyval;
	    $edt->{key_labels}{$table}{$new_keyval} = $label;
	}
	
	return $new_keyval;
    }
    
    else
    {
	$edt->{fail_count}++;
	return undef;
    }
}


# _execute_replace ( action )
# 
# Actually perform an replace operation on the database. The record keys and values should already
# have been checked by 'validate_record' or some other code, and lists of columns and values
# generated.

sub _execute_replace {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    # Set this as the current action.

    $edt->{current_action} = $action;
    
    # If we need to substitute any key values for labels, do that now.
    
    if ( $action->label_sub )
    {
	$edt->_substitute_labels($action) || return;
    }
    
    # Check to make sure that we actually have column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'column/value mismatch on replace');
	return;
    }
    
    # If the following flag is set, deliberately generate an SQL error for
    # testing purposes.
    
    if ( $TEST_PROBLEM{replace_sql} )
    {
	push @$cols, 'XXXX';
    }
    
    # Construct the REPLACE statement.
    
    my $dbh = $edt->dbh;
    
    my $column_list = join(',', @$cols);
    my $value_list = join(',', @$vals);
    
    my $sql = "	REPLACE INTO $TABLE{$table} ($column_list)
		VALUES ($value_list)";
    
    $edt->debug_line("$sql\n");
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    my ($result, $cleanup_called);
    
    try {
	
	# If we are logging this action, then fetch the existing record if any.
	
	unless ( $edt->allows('NO_LOG_MODE') || get_table_property($table, 'NO_LOG') )
	{
	    $edt->fetch_old_record($action, $table);
	}
	
	# Start by calling the 'before_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing.
	
	$edt->before_action($action, 'replace', $table);

	# Then execute the replace statement itself, provided there are no errors and the action
	# was not aborted.
	
	unless ( $action->status )
	{
	    $result = $dbh->do($sql);
	    $action->_set_status('executed');
	}
	
        # Finally, call the 'after_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing. If the replace failed, then 'cleanup_action' is called
	# instead.
	
	if ( $result )
	{
	    $edt->after_action($action, 'replace', $table);
	}
	
	else
	{
	    $cleanup_called = 1;
	    $edt->add_condition($action, 'W_EXECUTE', 'replace statement failed');
	    $edt->cleanup_action($action, 'replace', $table);
	}
    }
    
    catch {	
	if ( /duplicate entry '(.*)' for key '(.*)' at/i )
	{
	    my $value = $1;
	    my $key = $2;
	    $edt->add_condition($action, 'E_DUPLICATE', $value, $key);
	}

	else
	{
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	}
	
	$edt->error_line($_);
	$action->_set_status('exception') unless $action->status;
	
	unless ( $cleanup_called )
	{
	    try {
		$edt->cleanup_action($action, 'replace', $table);
	    }
	    
	    catch {
		$edt->add_condition($action, 'E_EXECUTE', 'an exception was thrown during cleanup');
		$edt->error_line($_);
	    };
	}
    };
    
    # If the replace succeeded, return true. Otherwise, return false. In either case, record the
    # mapping between key value and record label.
    
    my $keyval = $action->keyval + 0;
    my $label = $action->label;
    
    if ( defined $label && $label ne '' )
    {
	$edt->{label_keys}{$label} = $keyval;
	$edt->{key_labels}{$table}{$keyval} = $label;
    }
    
    if ( $result && ! $action->has_errors )
    {
	$edt->{action_count}++;
	push @{$edt->{replaced_keys}{$table}}, $keyval;
	push @{$edt->{datalog}}, EditTransaction::LogEntry->new($keyval, $action);
	
	if ( my $linkval = $action->linkval )
	{
	    if ( $linkval =~ /^[@](.*)/ )
	    {
		$linkval = $edt->{label_keys}{$1};
		$edt->add_condition($action, 'E_EXECUTE', 'link value label was not found') unless $linkval;
	    }
	    
	    $edt->{superior_keys}{$table}{$linkval} = 1;
	}
	
	return $result;
    }
    
    else
    {
	$edt->{fail_count}++;
	push @{$edt->{failed_keys}{$table}}, $keyval;
	return undef;
    }
}


# _execute_update ( action )
# 
# Actually perform an update operation on the database. The keys and values have been checked
# previously.

sub _execute_update {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    # Set this as the current action.

    $edt->{current_action} = $action;
    
    # If we need to substitute any key values for labels, do that now.
    
    if ( $action->label_sub )
    {
	$edt->_substitute_labels($action) || return;
    }
    
    # Check to make sure that we actually have column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'column/value mismatch on update');
	return;
    }
    
    # If the following flag is set, deliberately generate an SQL error for
    # testing purposes.
    
    if ( $TEST_PROBLEM{update_sql} )
    {
	push @$cols, 'XXXX';
    }
    
    # Construct the UPDATE statement.
    
    my $dbh = $edt->dbh;
    my $set_list = '';
    
    foreach my $i ( 0..$#$cols )
    {
	$set_list .= ', ' if $set_list;
	$set_list .= "$cols->[$i]=$vals->[$i]";
    }
    
    my $key_expr = $action->keyexpr;
    
    my $sql = "	UPDATE $TABLE{$table} SET $set_list
		WHERE $key_expr";
    
    $edt->debug_line("$sql\n");
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    my ($result, $cleanup_called);
    
    try {
	
	# If we are logging this action, then fetch the existing record.
	
	unless ( $edt->allows('NO_LOG_MODE') || get_table_property($table, 'NO_LOG') )
	{
	    $edt->fetch_old_record($action, $table, $key_expr);
	}
	
	# Start by calling the 'before_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing.
	
	$edt->before_action($action, 'update', $table);

	# Then execute the update statement itself, provided there are no errors and the action
	# has not been aborted.
	
	unless ( $action->status )
	{
	    $result = $dbh->do($sql);
	    $action->_set_status('executed');
	}
	
	# Finally, call the 'after_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing. If the update failed, then 'cleanup_action' is called
	# instead.
	
	if ( $result )
	{
	    $edt->after_action($action, 'update', $table);
	}
	
	else
	{
	    $cleanup_called = 1;
	    $edt->add_condition($action, 'W_EXECUTE', 'update failed');
	    $edt->cleanup_action($action, 'update', $table);
	}
    }
    
    catch {
	if ( /duplicate entry '(.*)' for key '(.*)' at/i )
	{
	    my $value = $1;
	    my $key = $2;
	    $edt->add_condition($action, 'E_DUPLICATE', $value, $key);
	}

	else
	{
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	}
	
	$edt->error_line($_);
	$action->_set_status('exception') unless $action->status;
	
	unless ( $cleanup_called )
	{
	    try {
		$edt->cleanup_action($action, 'update', $table);
	    }
	    
	    catch {
		$edt->add_condition($action, 'E_EXECUTE', 'an exception was thrown during cleanup');
		$edt->debug_line($_);
	    };
	}
    };
    
    # If the update succeeded, return true. Otherwise, return false. In either case, record the
    # mapping between key value and record label.
    
    my $keyval = $action->keyval + 0;
    my $label = $action->label;

    if ( defined $label && $label ne '' )
    {
	$edt->{label_keys}{$label} = $keyval;
	$edt->{key_labels}{$table}{$keyval} = $label;
    }
    
    if ( $result && ! $action->has_errors )
    {
	$edt->{action_count}++;
	push @{$edt->{updated_keys}{$table}}, $keyval;
	push @{$edt->{datalog}}, EditTransaction::LogEntry->new($keyval, $action);
	
	if ( my $linkval = $action->linkval )
	{
	    if ( $linkval =~ /^[@](.*)/ )
	    {
		$linkval = $edt->{label_keys}{$1};
		$edt->add_condition($action, 'E_EXECUTE', 'link value label was not found') unless $linkval;
	    }
	    
	    $edt->{superior_keys}{$table}{$linkval} = 1;
	}
	
	return $result;
    }
    
    else
    {
	$edt->{fail_count}++;
	push @{$edt->{failed_keys}{$table}}, $keyval;
	return undef;
    }
}


# _execute_update_many ( action )
# 
# Actually perform an update_many operation on the database. The keys and values have NOT yet been
# checked.

sub _execute_update_many {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    croak "operation 'update_many' is not yet implemented";
}


# _execute_delete ( action )
# 
# Actually perform a delete operation on the database. The only field that makes any difference
# here is the primary key.

sub _execute_delete {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    my $dbh = $edt->dbh;
    
    my $key_expr = $action->keyexpr;
    
    # Set this as the current action.
    
    $edt->{current_action} = $action;
    
    # If the following flag is set, deliberately generate an SQL error for
    # testing purposes.
    
    if ( $TEST_PROBLEM{delete_sql} )
    {
	$key_expr .= 'XXXX';
    }
    
    # Construct the DELETE statement.
    
    my $sql = "	DELETE FROM $TABLE{$table} WHERE $key_expr";
    
    $edt->debug_line( "$sql\n" );
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    my ($result, $cleanup_called);
    
    try {
	
	# If we are logging this action, then fetch the existing record.
	
	unless ( $edt->allows('NO_LOG_MODE') || get_table_property($table, 'NO_LOG') )
	{
	    $edt->fetch_old_record($action, $table, $key_expr);
	}
	
	# Start by calling the 'before_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing.    
	
	$edt->before_action($action, 'delete', $table);
	
	# Then execute the delete statement itself, provided the action has not been aborted.
	
	unless ( $action->status || $action->has_errors )
	{
	    $result = $dbh->do($sql);
	    $action->_set_status('executed');
	    
	    # Then call the 'after_action' method. This is designed to be overridden by
	    # subclasses, and can be used to do any necessary auxiliary actions to the database. The
	    # default method does nothing. If the delete failed, then 'cleanup_action' is called
	    # instead.
	    
	    if ( $result )
	    {
		$edt->after_action($action, 'delete', $table);
	    }
	    
	    else
	    {
		$cleanup_called = 1;
		$edt->add_condition($action, 'W_EXECUTE', 'delete failed');
		$edt->cleanup_action($action, 'delete', $table);
	    }
	}
    }
    
    catch {
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	$edt->error_line($_);
	$action->_set_status('exception') unless $action->status;	
	
	unless ( $cleanup_called )
	{
	    try {
		$edt->cleanup_action($action, 'delete', $table);
	    }
	    
	    catch {
		$edt->add_condition($action, 'E_EXECUTE', 'an exception was thrown during cleanup');
		$edt->error_line($_);
	    };
	}
    };
    
    # Record the number of records deleted, along with the mapping between key values and record labels.
    
    my ($count, @keys, @labels);
    
    if ( $action->is_multiple )
    {
	$count = $action->action_count;
	
	@keys = $action->all_keys;
	@labels = $action->all_labels;
	
	foreach my $i ( 0..$#keys )
	{
	    $edt->{key_labels}{$table}{$keys[$i]} = $labels[$i] if defined $labels[$i] && $labels[$i] ne '';
	}
    }
    
    else
    {
	$count = 1;
	@keys = $action->keyval + 0;
	my $label = $action->label;
	$edt->{key_labels}{$table}{$keys[0]} = $label if defined $label && $label ne '';
	# There is no need to set label_keys, because the record has now vanished and no longer
	# has a key.
    }
    
    # If the delete succeeded, log it and return true. Otherwise, return false.
    
    if ( $result && ! $action->has_errors )
    {
	$edt->{action_count} += 1;
	push @{$edt->{deleted_keys}{$table}}, @keys;
	push @{$edt->{datalog}}, EditTransaction::LogEntry->new($_, $action) foreach @keys;
	
	if ( my $linkval = $action->linkval )
	{
	    if ( $linkval =~ /^[@](.*)/ )
	    {
		$linkval = $edt->{label_keys}{$1};
		$edt->add_condition($action, 'E_EXECUTE', 'link value label was not found') unless $linkval;
	    }
	    
	    $edt->{superior_keys}{$table}{$linkval} = 1;
	}
	
	return $result;
    }
    
    else
    {
	$edt->{fail_count} += 1;
	push @{$edt->{failed_keys}{$table}}, @keys;
	return undef;
    }
}


# _execute_delete_cleanup ( action )
# 
# Perform a delete operation on the database. The records to be deleted are those that match the
# action selector and were not either inserted, updated, or replaced during this transaction.

sub _execute_delete_cleanup {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    my $dbh = $edt->dbh;
    
    my $selector = $action->selector;
    my $keycol = $action->keycol;
    
    # Set this as the current action.
    
    $edt->{current_action} = $action;
    
    # Come up with the list of keys to preserve. If there aren't any entries, add a 0 to avoid a
    # syntax error. This will not match any records under the Paleobiology Database convention
    # that 0 is never a valid key.
    
    my @preserve;
    
    push @preserve, @{$edt->{inserted_keys}{$table}} if ref $edt->{inserted_keys}{$table} eq 'ARRAY';
    push @preserve, @{$edt->{replaced_keys}{$table}} if ref $edt->{replaced_keys}{$table} eq 'ARRAY';
    push @preserve, @{$edt->{updated_keys}{$table}} if ref $edt->{updated_keys}{$table} eq 'ARRAY';
    
    push @preserve, '0' unless @preserve;
    
    my $key_list = join(',', @preserve);
    
    my $keyexpr = "$selector and not $keycol in ($key_list)";
    
    # Figure out which keys will be deleted, so that we can list them later.

    my $init_sql = "	SELECT $keycol FROM $TABLE{$table} WHERE $keyexpr";
    
    $edt->debug_line( "$init_sql\n" );

    my $deleted_keys = $dbh->selectcol_arrayref($init_sql);
    
    # If the following flag is set, deliberately generate an SQL error for
    # testing purposes.
    
    if ( $TEST_PROBLEM{delete_sql} )
    {
	$keyexpr .= 'XXXX';
    }
    
    # Then construct the DELETE statement.
    
    $action->_set_keyexpr($keyexpr);
    
    my $sql = "	DELETE FROM $TABLE{$table} WHERE $keyexpr";
    
    $edt->debug_line( "$sql\n" );
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    my ($result, $cleanup_called);
    
    try {
	
	# If we are logging this action, then fetch the existing record.
	
	# unless ( $edt->allows('NO_LOG_MODE') || get_table_property($table, 'NO_LOG') )
	# {
	#     $edt->fetch_old_record($action, $table, $keyexpr);
	# }
	
	# Start by calling the 'before_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing.    
	
	$edt->before_action($action, 'delete_cleanup', $table);
	
	# Then execute the delete statement itself, provided there are no errors and the action
	# has not been aborted.
	
	unless ( $action->status )
	{
	    $result = $dbh->do($sql);
	    $action->_set_status('executed');
	}
	
	# Finally, call the 'after_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing. If the delete failed, then 'cleanup_action' is called
	# instead.
	
	if ( $result )
	{
	    $edt->after_action($action, 'delete_cleanup', $table);
	}
	
	else
	{
	    $cleanup_called = 1;
	    $edt->add_condition($action, 'W_EXECUTE', 'delete failed');
	    $edt->cleanup_action($action, 'delete_cleanup', $table);
	}
    }
    
    catch {
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	$edt->error_line($_);
	$action->_set_status('exception') unless $action->status;	
	
	unless ( $cleanup_called )
	{
	    try {
		$edt->cleanup_action($action, 'delete_cleanup', $table);
	    }
	    
	    catch {
		$edt->add_condition($action, 'E_EXECUTE', 'an exception was thrown during cleanup');
		$edt->error_line($_);
	    };
	}
    };
    
    # Record the number of records deleted, along with the mapping between key values and record labels.
    
    if ( ref $deleted_keys eq 'ARRAY' )
    {
	foreach my $i ( 0..$#$deleted_keys )
	{
	    push @{$edt->{datalog}}, EditTransaction::LogEntry->new($deleted_keys->[$i], $action);
	}
    }
    
    # If the delete succeeded, return true. Otherwise, return false.
    
    if ( $result && ! $action->has_errors )
    {
	$edt->{action_count} += 1;
	push @{$edt->{deleted_keys}{$table}}, @$deleted_keys if ref $deleted_keys eq 'ARRAY';
	return $result;
    }
    
    else
    {
	$edt->{fail_count} += 1;
	push @{$edt->{failed_keys}{$table}}, @$deleted_keys if ref $deleted_keys eq 'ARRAY';
	return undef;
    }
}


# _execute_delete_many ( action )
# 
# Actually perform an update_many operation on the database. The keys and values have NOT yet been
# checked.

sub _execute_delete_many {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    croak "operation 'delete_many' is not yet implemented";
}


# _execute_update ( action )
# 
# Actually perform an update operation on the database. The keys and values have been checked
# previously.

sub _execute_other {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    my $record = $action->record;
    
    # Set this as the current action.
    
    $edt->{current_action} = $action;
    
    # Determine the method to be called.
    
    my $method = $action->method;
    
    # Call the specified method inside a try block. If it fails, add either an error or
    # a warning depending on whether this EditTransaction allows PROCEED.
    
    my ($result, $cleanup_called);
    
    try {
	
	# Call the method specified for this action, provided there are no errors and the action
	# has not been aborted.
	
	unless ( $action->status )
	{
	    $result = $edt->$method($action, $table, $record);
	    $action->_set_status('executed');
	}
    }
    
    catch {
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	$edt->error_line($_);
	$action->_set_status('exception') unless $action->status;
	
	unless ( $cleanup_called )
	{
	    try {
		$edt->cleanup_action($action, 'other', $table);
	    }
	    
	    catch {
		$edt->add_condition($action, 'E_EXECUTE', 'an exception was thrown during cleanup');
		$edt->debug_line($_);
	    };
	}
    };
    
    # If the operation succeeded, return true. Otherwise, return false. In either case, record the
    # mapping between key value and record label.
    
    my $keyval = $action->keyval + 0;
    my $label = $action->label;
    
    if ( defined $label && $label ne '' )
    {
	$edt->{label_keys}{$label} = $keyval;
	$edt->{key_labels}{$table}{$keyval} = $label;
    }
    
    if ( $result && ! $action->has_errors )
    {
	$edt->{action_count}++;
	push @{$edt->{other_keys}{$table}}, $keyval;
	# push @{$edt->{datalog}}, EditTransaction::LogEntry->new($keyval, $action);
	
	if ( my $linkval = $action->linkval )
	{
	    if ( $linkval =~ /^[@](.*)/ )
	    {
		$linkval = $edt->{label_keys}{$1};
		$edt->add_condition($action, 'E_EXECUTE', 'link value label was not found') unless $linkval;
	    }
	    
	    $edt->{superior_keys}{$table}{$linkval} = 1;
	}
	
	return $result;
    }
    
    else
    {
	$edt->{fail_count}++;
	push @{$edt->{failed_keys}{$table}}, $keyval;
	return undef;
    }
}


# _substitute_labels ( action )
#
# Substitute the values in the columns marked for substitution with the key value associated with
# the corresponding labels.

sub _substitute_labels {
    
    my ($edt, $action) = @_;
    
    my $has_label = $action->label_sub;
    my $columns = $action->column_list;
    my $values = $action->value_list;
    my $ok = 1;
    
    # Step through the columns, checking to see which ones have labels that must be substituted.
    
    foreach my $index ( 0..$#$columns )
    {
	next unless $columns->[$index] && $has_label->{$columns->[$index]};
	next unless defined $values->[$index];
	
	$values->[$index] =~ /^'\@(.*)'$/;
	
	my $label = $1;
	my $key = defined $1 && $1 ne '' && $edt->{label_keys}{$1};
	
	if ( $key )
	{
	    $values->[$index] = $edt->dbh->quote($key);
	}
	
	else
	{
	    $edt->add_condition($action, 'E_LABEL_NOT_FOUND', $columns->[$index], '@' . $label);
	    $ok = undef;
	}
    }
    
    return $ok;
}


# _do_sql ( action, table, record )
#
# Execute the specified SQL statement.

sub _do_sql {
    
    my ($edt, $action, $table, $record) = @_;
    
    my $result = $edt->{dbh}->do($record->{sql});

    if ( ref $record->{result} eq 'SCALAR' )
    {
	${$record->{$result}} = $result;
    }
}


# Methods to be overridden
# ------------------------

# The following methods do nothing, and exist solely to be overridden by subclasses. This enables
# subclasses to execute auxiliary database operations before and/or after actions and
# transactions.

# initialize_transaction ( table )
#
# This method is passed the name that was designated as the "main table" for this transaction. The
# method is designed to be overridden by subclasses, so that any necessary work can be carried out
# at the beginning of the transaction. The default method defined here does nothing.

sub initialize_transaction {
    
    my ($edt, $table) = @_;
    
    my $a = 1;	# We can stop here when debugging.
}


# finalize_transaction ( table )
#
# This method is called at the end of every successful transaction. It is passed the name that was
# designated as the "main table" for this transaction. The method is designed to be overridden by
# subclasses, so that any necessary work can be carried out at the end of the transaction.

sub finalize_transaction {

    my ($edt, $table) = @_;

    my $a = 1;	# We can stop here when debugging.
}


# cleanup_transaction ( table )
# 
# This method is called instead of 'finalize_transaction' if the transaction is to be rolled back
# instead of being committed. The method is designed to be overridden by subclasses, so that any
# necessary work can be carried out to clean up before the transaction is rolled back.

sub cleanup_transaction {

    my ($edt, $table) = @_;

    my $a = 1;	# We can stop here when debugging.
}


# before_action ( action, operation, table )
#
# This method is called before each action. It is designed to be overridden by subclasses, so that
# any necessary auxiliary work can be carried out. The default method defined here does nothing.

sub before_action {

    my ($edt, $action, $operation, $table) = @_;
    
    my $a = 1;	# We can stop here when debugging.
}


# after_action ( action, operation, table, key )
#
# This method is called after each successfully completed action. It is designed to be overridden
# by subclasses, so that any necessary auxiliary work can be carried out. For insert operations,
# the parameter $key will get the primary key value of the newly inserted record. Otherwise, this
# parameter will be undefined.

sub after_action {

    my ($edt, $action, $operation, $table, $keyval) = @_;
    
    my $a = 1;	# We can stop here when debugging.
}


# cleanup_action ( action, operation, table )
#
# This method is called after each failed action. It is designed to be overridden by subclasses,
# so that any necessary auxiliary work can be carried out.

sub cleanup_action {

    my ($edt, $action, $operation, $table) = @_;
    
    my $a = 1;  # We can stop here when debugging.
}


# Progress and results of actions
# -------------------------------

# The methods in this section can be called from code in subclasses to determine the progress of
# the EditTransaction and carry out auxiliary actions such as inserts to or deletes from other
# tables that are tied to the main one by foreign keys.

sub tables {
    
    return keys %{$_[0]->{tables}};
}


sub inserted_keys {

    my ($edt, $table) = @_;

    if ( $table )
    {
	return $edt->{inserted_keys}{$table} ? @{$edt->{inserted_keys}{$table}} : wantarray ? ( ) : 0;
    }
    
    else
    {
	return map { $edt->{inserted_keys}{$_} ? @{$edt->{inserted_keys}{$_}} : ( ) } keys %{$edt->{tables}};
    }
}


sub updated_keys {

    my ($edt, $table) = @_;
    
    if ( $table )
    {
	return $edt->{updated_keys}{$table} ? @{$edt->{updated_keys}{$table}} : wantarray ? ( ) : 0;
    }
    
    else
    {
	return map { $edt->{updated_keys}{$_} ? @{$edt->{updated_keys}{$_}} : ( ) } keys %{$edt->{tables}};
    }
}


sub replaced_keys {

    my ($edt, $table) = @_;
    
    if ( $table )
    {
	return $edt->{replaced_keys}{$table} ? @{$edt->{replaced_keys}{$table}} : wantarray ? ( ) : 0;
    }
    
    else
    {
	return map { $edt->{replaced_keys}{$_} ? @{$edt->{replaced_keys}{$_}} : ( ) } keys %{$edt->{tables}};
    }
}


sub deleted_keys {

    my ($edt, $table) = @_;
    
    if ( $table )
    {
	return $edt->{deleted_keys}{$table} ? @{$edt->{deleted_keys}{$table}} : wantarray ? ( ) : 0;
    }
    
    else
    {
	return map { $edt->{deleted_keys}{$_} ? @{$edt->{deleted_keys}{$_}} : ( ) } keys %{$edt->{tables}};
    }
}


sub other_keys {

    my ($edt, $table) = @_;
    
    if ( $table )
    {
	return $edt->{other_keys}{$table} ? @{$edt->{other_keys}{$table}} : wantarray ? ( ) : 0;
    }
    
    else
    {
	return map { $edt->{other_keys}{$_} ? @{$edt->{other_keys}{$_}} : ( ) } keys %{$edt->{tables}};
    }
}


sub superior_keys {

    my ($edt, $table) = @_;

    if ( $table )
    {
	return $edt->{superior_keys}{$table} ? keys %{$edt->{superior_keys}{$table}} : wantarray ? ( ) : 0;
    }
    
    elsif ( $edt->{superior_keys} )
    {
	return map { $edt->{superior_keys}{$_} ? keys %{$edt->{superior_keys}{$_}} : ( ) } keys %{$edt->{superior_keys}};
    }
    
    else
    {
	return;
    }
}


sub failed_keys {

    my ($edt, $table) = @_;
    
    if ( $table )
    {
	return $edt->{failed_keys}{$table} ? @{$edt->{failed_keys}{$table}} : ();
    }
    
    else
    {
	return map { $edt->{failed_keys}{$_} ? @{$edt->{failed_keys}{$_}} : () } keys %{$edt->{tables}};
    }
}


sub key_labels {

    my ($edt, $table) = @_;

    if ( $table )
    {
	return $_[0]->{key_labels}{$table} if $table && $_[0]->{key_labels}{$table};
    }
    
    else
    {
	return $_[0]->{key_labels};
    }
}


sub label_keys {

    return $_[0]->{label_keys};
}


sub label_key {

    return $_[0]->{label_keys}{$_[1]};
}


sub label_table {

    return $_[0]->{label_found}{$_[1]};
}


sub record_count {

    return $_[0]->{record_count} || 0;
}


sub action_count {
    
    return $_[0]->{action_count} || 0;
}


sub fail_count {
    
    return $_[0]->{fail_count} || 0;
}


sub skip_count {

    return $_[0]->{skip_count} || 0;
}


# Permission checking
# -------------------

# The methods listed below call the equivalent methods of the Permissions object that
# was used to initialize this EditTransaction.

sub check_table_permission {

    my ($edt, $table, $permission) = @_;
    
    return $edt->{perms}->check_table_permission($table, $permission);
}

sub check_record_permission {
    
    my ($edt, $table, $permission, $key_expr, $record) = @_;
    
    return $edt->{perms}->check_record_permission($table, $permission, $key_expr, $record);
}

sub check_many_permission {

    my ($edt, $table, $permission, $key_expr, $record) = @_;

    return $edt->{perms}->check_many_permission($table, $permission, $key_expr, $record);
}

# Action validation
# -----------------

# The methods in this section provide default validation for records to be inserted and
# updated. This is done by comparing the field values to the types of the corresponding columns
# from the database schema for the table, plus any attributes specifically specified for the
# column using 'set_column_property' such as 'REQUIRED' and 'ADMIN_SET'.
# 
# Subclasses may override this method, to add additional checks. It is recommended that they call
# this method as a SUPER, because it comprehensively checks every field value against the
# corresponding table definition.

our (%SIGNED_BOUND) = ( tiny => 127,
			small => 32767,
			medium => 8388607,
			regular => 2147483647 );

our (%UNSIGNED_BOUND) = ( tiny => 255,
			  small => 65535,
			  medium => 16777215,
			  regular => 4294967295 );
			 


# validate_action ( action, operation, table, keyexpr )
#
# Call the validate_record method to check that the new field values to be added or updated in the
# database are consistent with the corresponding table definition. The $keyexpr parameter is
# not used by this code, since it does not check the current values in the database. However, this
# parameter is provided for every operation except 'insert' in case an override method wants to
# use it.
#
# This method may be overridden by subclasses, in order to provide different checks or additional
# checks. Such methods should indicate error and warning conditions using the method
# 'add_condition'. Override methods will probably want to call SUPER::validate_action as
# well, because it provides comprehensive checks to make sure that all record values can be
# properly stored in the database. Specific columns can be exempted from validation checks by
# calling 'column_skip_validate' on the action object and providing one or more column names.

sub validate_action {
    
    my ($edt, $action, $operation, $table, $keyexpr) = @_;
    
    if ( $TEST_PROBLEM{validate} )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'TEST VALIDATE');
	return;
    }
}


# column_special ( special, column ... )
#
# This is intended to be called as either a class method or an instance. It specifies special
# treatment for certain columns given by name.

sub column_special {
    
    my ($edt, $table_specifier, $special, @columns) = @_;
    
    my $hash;
    
    # If this was called as an instance method, attach the special column information to this
    # instance, stored under the table name as a hash key.
    
    if ( ref $edt )
    {
    	$hash = $edt->{column_special}{$table_specifier} ||= { };
    }
    
    # Otherwise, store it in a global variable using the name of the class and table name as a
    # hash key.
    
    else
    {
    	$hash = $SPECIAL_BY_CLASS{$edt}{$table_specifier} ||= { };
    }
    
    # Now set the specific attribute for non-empty column name.
    
    foreach my $col ( @columns )
    {
	$hash->{$col} = $special if $col;
    }
}


# validate_against_schema ( action, operation, table, special )
# 
# Check the field values to be stored in the database against the corresponding table definition,
# and call 'add_condition' to record any error or warning conditions that are detected. The
# column names and corresponding values to be stored are added to the action record using
# 'set_column_values', for later use by the action execution methods.
# 
# If the argument $special is given, it must be a hash ref whose keys are column names. Currently
# the only accepted value is 'skip', indicating that this field should be skipped. This is
# available for use when this method is called from within a subclass method that overrides
# 'validate_action'.

our $DECIMAL_NUMBER_RE = qr{ ^ \s* ( [+-]? ) \s* (?: ( \d+ ) (?: [.] ( \d* ) )? | [.] ( \d+ ) ) \s*
			     (?: [Ee] \s* ( [+-]? ) \s* ( \d+ ) )? \s* $ }xs;

our %EXTID_CHECK;

sub validate_against_schema {

    my ($edt, $action, $operation, $table) = @_;
    
    $operation ||= $action->operation;
    $table ||= $action->table;
    
    my $record = $action->record;
    my $permission = $action->permission;
    my $keycol = $action->keycol;
    
    my $is_owner;
    
    # Grab the table schema, or throw an exception if it is not available. This information is cached, so
    # the database will only need to be asked for this information once per process per table.
    
    my $dbh = $edt->dbh;
    my $schema = get_table_schema($dbh, $table, $edt->debug);
    
    # If the operation is 'replace', then get the current created/modified timestamps from the old
    # record.
    
    my (@copy_columns);
    
    # if ( $operation eq 'replace' )
    # {
    # 	($old_values{created}, $old_values{modified},
    # 	 $old_values{authorizer_no}, $old_values{enterer_no}, $old_values{modifier_no}) =
    # 	    $edt->get_old_values($action, $table, 'created, modified, authorizer_no, enterer_no, modifier_no');
    # }
    
    # Start by going through the list of field names, and constructing a list of values to be
    # inserted.
    
    my (@columns, @values, %used);

  COLUMN:
    foreach my $col ( @{$schema->{_column_list}} )
    {
	my $cr = $schema->{$col};
	
	# Start by checking to see if there are special handling instructions for this column.
	
	my $special = $action->get_special($col);
	
	# If we are supposed to ignore this column, then do so.
	
	next COLUMN if $special eq 'ignore';

	# If a value for this column is found in the record, then use that.
	
	my $value = $record->{$col};
	my $record_col = $col;
	my $quote_this_value;
	my $is_default;
	
	# Skip the primary key for any operation except 'replace'. For 'replace' operations, we
	# use the cleaned key value without checking it. In all cases, the primary key value has
	# already been validated by the routine that called this one. The primary key may be
	# referred to in the action record under its alternate name, as specified by the
	# PRIMARY_FIELD property of the table. The ALTERNATE_NAME property should not be used for
	# primary keys.
	
	if ( $col eq $keycol )
	{
	    if ( exists $record->{$col} )
	    {
		$used{$col} = 1;
	    }
	    
	    elsif ( my $alt = get_table_property($table, 'PRIMARY_FIELD') )
	    {
		$used{$alt} = 1;
		$record_col = $alt;
	    }
	    
	    next COLUMN unless $operation eq 'replace';
	    
	    $value = $action->keyval;
	    $special = 'pass';
	}
	
	# Otherwise, if the column name is not mentioned in the record but an alternate name is
	# defined, then check that.
	
	elsif ( $cr->{ALTERNATE_ONLY} || ! exists $record->{$col} )
	{
	    my $alt = $cr->{ALTERNATE_NAME};
	    
	    # If an alternate name for this column is known, and this name appears as a key in the
	    # record, use that name and value. But not if we have been told to ignore it.
	    
	    if ( $alt && exists $record->{$alt} && $action->get_special($alt) ne 'ignore' )
	    {
		$record_col = $alt;
		$value = $record->{$alt};
	    }
	    
	    else
	    {
		$record_col = $alt if $cr->{ALTERNATE_ONLY};
		$value = undef;
	    }
	}
	
	# Record the keys that correspond to values from the record. We will use this info later
	# to throw error or warning conditions for any record keys that we do not recognize.
	
	$used{$record_col} = 1 if exists $record->{$record_col};
	
	# Don't check any columns we are directed to ignore. These were presumably checked by code
	# from a subclass that has called this method. Columns that have a type assigned by
	# %COMMON_FIELD_SPECIAL cannot be passed.
	
	my $type = $COMMON_FIELD_SPECIAL{$col};
	
	if ( $type || $special ne 'pass' )
	{
	    # Handle special columns in the appropriate ways.
	    
	    if ( $type )
	    {
		# The 'crmod' fields store the record creation and modification dates. These cannot be
		# specified explicitly except by a user with administrative permission, and then only
		# if this EditTransaction allows the condition 'ALTER_TRAIL'. In that case, check to
		# make sure that they have the proper format. But always ignore empty values.
		
		if ( $type eq 'crmod' )
		{
		    # If the value of 'modified' is 'NORMAL', that overrides everything else. Just
		    # treat all of the 'crmod' and 'authent' fields normally, which means "as if a
		    # null value was given". It is also okay to specify 'UNCHANGED' for 'created'
		    # since this is the normal behavior.
		    
		    if ( $record->{modified} && $record->{modified} eq 'NORMAL' )
		    {
			$value = undef;
		    }
		    
		    elsif ( $col eq 'created' && defined $value && $value eq 'UNCHANGED' )
		    {
			$value = undef;
		    }
		    
		    # Now, if a value is specified for any of the crmod fields, then add an error
		    # condition unless the user has permission to explicitly set these fields.
		    
		    if ( defined $value && $value ne '' )
		    {
			my $error;
			
			unless ( $permission =~ /admin/ )
			{
			    $edt->add_condition($action, 'E_PERM_COL', $record_col);
			    $error = 1;
			}
			
			unless ( $edt->{fixup_mode} || $edt->allows('ALTER_TRAIL') )
			{
			    $edt->add_condition($action, 'C_ALTER_TRAIL');
			    $error = 1;
			}
			
			# If the value is explicitly 'UNCHANGED', then leave it unchanged. This
			# requires copying the old value if the operation is 'replace'.
			
			if ( $value eq 'UNCHANGED' )
			{
			    next COLUMN unless $operation eq 'replace';
			    
			    push @copy_columns, $col;
			    $value = undef;
			}
			
			# Otherwise, check that the value matches the required format.
			
			else
			{
			    ($value, $quote_this_value) =
				$edt->validate_datetime_value($action, $schema->{$col}, $record_col, $value);
			    
			    next if $error || ref $value;
			}
		    }
		    
		    # Otherwise, if we are working under FIXUP_MODE, then leave no record of the
		    # modification. This is only allowed with 'admin' privilege on the table in
		    # question. If 'modified' is specifically a key in the action, with an undefined
		    # value, then skip this section because the user wants it treated normally.
		    
		    elsif ( $operation ne 'insert' && $col eq 'modified' && $edt->{fixup_mode} &&
			    ! exists $record->{$col} )
		    {
			if ( $permission !~ /admin/ )
			{
			    $edt->add_condition($action, 'E_PERM_COL', $col);
			    next;
			}
			
			elsif ( $operation eq 'replace' )
			{
			    push @copy_columns, 'modified';
			    $value = undef;
			}
			
			else
			{
			    next;
			}
		    }
		    
		    # Otherwise, if the operation is 'update' then set the modification time to
		    # the present. This is handled by specifying an explicit null value. The creation
		    # time will be unchanged, unless explicitly specified above.
		    
		    elsif ( $operation eq 'update' && $col eq 'modified' )
		    {
			$value = undef;
		    }
		    
		    # If the operation is 'replace', then copy the creation time from the old
		    # record. The modification time will be null unless specifically specified
		    # above, which will cause it to be set to the current time.
		    
		    elsif ( $operation eq 'replace' && $col eq 'created' )
		    {
			push @copy_columns, $col;
			$value = undef;
		    }
		    
		    # Otherwise, we skip the column. For a newly inserted record, this will cause
		    # the 'created' and 'modified' times to be set to the current timestamp.
		    
		    else
		    {
			next;
		    }
		}
		
		# The 'authent' fields store the identifiers of the record authorizer, enterer, and
		# modifier. These are subject to the same conditions as the 'crmod' fields if
		# specified explicitly. But empty values get filled in according to the values for the
		# current user.
		
		elsif ( $type eq 'authent' )
		{
		    # If the value of 'modified' is 'NORMAL', that overrides everything else. Just
		    # treat all of the 'crmod' and 'authent' fields normally, which means "as if a
		    # null value was given". It is also okay to specify 'UNCHANGED' for
		    # 'authorizer_no' and 'enterer_no' since this is the normal behavior.
		    
		    if ( $record->{modified} && $record->{modified} eq 'NORMAL' )
		    {
			$value = undef;
		    }
		    
		    elsif ( $col =~ /^auth|^ent/ && defined $value && $value eq 'UNCHANGED' )
		    {
			$value = undef;
		    }
		    
		    # Now, If the value is not empty, check to make sure the user has permission
		    # to set a specific value.
		    
		    if ( defined $value && $value ne '' )
		    {
			my $error;
			
			unless ( $permission =~ /admin/ )
			{
			    $edt->add_condition($action, 'E_PERM_COL', $record_col);
			    $error = 1;
			}
			
			unless ( $edt->{fixup_mode} || $edt->allows('ALTER_TRAIL') )
			{
			    $edt->add_condition($action, 'C_ALTER_TRAIL');
			    $error = 1;
			}
			
			# If the value is explicitly 'UNCHANGED', then leave it unchanged. This
			# requires copying the old value if the operation is 'replace'.
			
			if ( $value eq 'UNCHANGED' )
			{
			    next COLUMN unless $operation eq 'replace';
			    
			    push @copy_columns, $col;
			    $value = undef;
			}
						
			# Now check to make sure the value is properly formatted.
			
			if ( ref $value eq 'PBDB::ExtIdent' )
			{
			    unless ( $value->{type} eq $IDP{PRS} )
			    {
				$edt->add_condition($action, 'E_EXTTYPE', $record_col, $value,
						    "must be an external identifier of type '$IDP{PRS}'");
				next;
			    }
			    
			    $value = $value->stringify;
			}
			
			elsif ( $value =~ $IDRE{PRS} )
			{
			    $value = $2;
			    
			    # If the value is 0, or ERROR, or something else not valid, add an error
			    # condition.
			    
			    unless ( $value > 0 )
			    {
				$edt->add_condition($action, 'E_RANGE', $record_col,
						    "value does not specify a valid record");
				next;
			    }
			}
			
			elsif ( $value =~ $IDRE{LOOSE} )
			{
			    $edt->add_condition($action, 'E_EXTTYPE', $record_col,
						"external id type '$1' is not valid for this field");
			    next;
			}
			
			# Otherwise, if it looks like an external identifier but is not of the right
			# type, then add an error condition.
			
			elsif ( ref $value || $value !~ qr{ ^ \d+ $ }xs )
			{
			    $edt->add_condition($action, 'E_FORMAT', $record_col, 
						'must be an external identifier or an unsigned integer');
			    next;
			}
			
			# Now make sure that the specific person actually exists.
			
			unless ( $edt->check_key('PERSON_DATA', $col, $value) )
			{
			    $edt->add_condition($action, 'E_KEY_NOT_FOUND', $record_col, $value);
			    next;
			}
			
			next if $error;
		    }
		    
		    # Otherwise, if we are working under FIXUP_MODE or this action was
		    # specifically directed to leave no record of the modification, then do
		    # that. But this is only allowed with 'admin' privilege on the table in
		    # question. If 'modifier_no' is specifically a key in the action, with an undefined
		    # value, then skip this section because the user wants it treated normally.
		    
		    elsif ( $operation ne 'insert' && $col eq 'modifier_no' && $edt->{fixup_mode} &&
			    ! exists $record->{$col} )
		    {
			if ( $permission !~ /admin/ )
			{
			    $edt->add_condition($action, 'E_PERM_COL', $record_col);
			    next;
			}
			
			elsif ( $operation eq 'replace' )
			{
			    push @copy_columns, $col;
			    $value = undef;
			}
			
			else
			{
			    next;
			}
		    }
		    
		    # If (as is generally supposed to happen) no value is specified for this
		    # column, then fill it in from the known information. The 'authorizer_no',
		    # 'enterer_no', and 'enterer_id' fields are filled in on record insertion, and
		    # 'modifier_no' on record update. If this is a 'replace' operation, then
		    # specify that this value should be replaced by the one in the old record.
		    
		    elsif ( $col eq 'authorizer_no' && $operation ne 'update' )
		    {
			$value = $edt->{perms}->authorizer_no;
			
			push @copy_columns, $col if $operation eq 'replace';
		    }
		    
		    elsif ( $col eq 'enterer_no' && $operation ne 'update' )
		    {
			$value = $edt->{perms}->enterer_no;
			
			push @copy_columns, $col if $operation eq 'replace';
		    }
		    
		    elsif ( $col eq 'enterer_id' && $operation ne 'update' )
		    {
			$value = $edt->{perms}->user_id;
			$quote_this_value = 1;
			
			push @copy_columns, $col if $operation eq 'replace';
			
		    }
		    
		    elsif ( $col eq 'modifier_no' && $operation ne 'insert' )
		    {
			if ( $action->{_no_modifier} )
			{
			    $value = 0;
			}

			else
			{
			    $value = $edt->{perms}->enterer_no;
			}
		    }
		    
		    elsif ( $col eq 'modifier_id' && $operation ne 'insert' )
		    {
			$value = $edt->{perms}->user_id;
		    }
		    
		    # Otherwise, we skip this column.
		    
		    else
		    {
			next;
		    }
		}
		
		# The 'admin' fields specify attributes that can only be controlled by users with
		# administrative privilege. For now, this includes only 'admin_lock'. 
		
		elsif ( $type eq 'admin' )
		{
		    # If the value is empty, skip it and let it be filled in by the database engine.
		    
		    next unless defined $value && $value ne '';
		    
		    # Otherwise, check to make sure the user has permission to set a specific value.

		    unless ( $permission =~ /admin/ )
		    {
			$edt->add_condition($action, 'E_PERM_COL', $col);
		    }
		    
		    # If so, make sure the value is correct.
		    
		    if ( $col eq 'admin_lock' && not ( $value eq '1' || $value eq '0' ) )
		    {
			$edt->add_condition($action, 'E_FORMAT', $col, 'value must be 1 or 0');
		    }
		}

		# The 'owner' fields specify attributes that can only be controlled by owners.
		# For now, this includes only 'owner_lock'.
		
		elsif ( $type eq 'owner' )
		{
		    # If the value is empty, skip it and let it be filled in by the database engine.
		    
		    next unless defined $value && $value ne '';
		    
		    # Otherwise, check to make sure the current user is the owner or administrator.
		    
		    unless ( defined $is_owner )
		    {
			$is_owner = $edt->{perms}->check_if_owner($table, $action->keyexpr);
		    }
		    
		    unless ( $is_owner)
		    {
			$edt->add_condition($action, 'E_PERM_COL', $col);
		    }
		    
		    # If so, make sure the value is correct.
		    
		    if ( $col eq 'owner_lock' && not ( $value eq '1' || $value eq '0' ) )
		    {
			$edt->add_condition($action, 'E_FORMAT', $col, 'value must be 1 or 0');
		    }
		}
		
		else
		{
		    croak "bad internal field type";
		}
	    }
	    
	    # Otherwise, if the value is defined then validate against the column definition.
	    
	    elsif ( defined $value )
	    {
		# If the column allows external identifiers, and if the value is one, then unpack
		# it. If the value is already a PBDB::ExtIdent object, we assume that type checking has
		# already been done.
		
		if ( my $extid_type = $cr->{EXTID_TYPE} || $COMMON_FIELD_IDTYPE{$col} )
		{
		    # If the external identifier has already been parsed, make sure it has the
		    # proper type.
		    
		    if ( ref $value eq 'PBDB::ExtIdent' )
		    {
			$EXTID_CHECK{$extid_type} ||= qr{$IDP{$extid_type}};
			my $type = $value->type;
			
			unless ( $type eq 'unk' || $type =~ $EXTID_CHECK{$extid_type} )
			{
			    $edt->add_condition($action, 'E_EXTTYPE', $record_col,
						"wrong type for external identifier: must be '$IDP{$extid_type}'");
			    next;
			}
			
			$value = $value->stringify;
			$record->{$record_col} = $value;
		    }
		    
		    # If it is a number or a label reference, then leave it alone. We'll have to change
		    # this check if we ever add non-integer keys.
		    
		    elsif ( $value =~ /^\d+$|^@/ )
		    {
			# do nothing
		    }

		    # If it is the empty string, set it to zero.
		    
		    elsif ( $value eq '' )
		    {
			$value = 0;
		    }
		    
		    # If it looks like an external identifier of the proper type, unpack it.
		    
		    elsif ( $value =~ $IDRE{$extid_type} )
		    {
			$value = $2;

			# If the value is a positive integer, do nothing
			
			if ( $value =~ /^\d+$/ )
			{
			    # do nothing
			}
			
			# If the value is ERROR, or something else not valid, add an error
			# condition.
			
			else
			{
			    $edt->add_condition($action, 'E_RANGE', $record_col,
						"value does not specify a valid record");
			    next;
			}
		    }
		    
		    # Otherwise, if it looks like an external identifier but is not of the right
		    # type, then add an error condition.
		    
		    elsif ( $value =~ $IDRE{LOOSE} )
		    {
			$edt->add_condition($action, 'E_EXTTYPE', $record_col,
					    "external id type '$1' is not valid for this field");
			next;
		    }
		    
		    # Otherwise, add an error condition if we are expecting an integer. If we ever
		    # add non-integer keys, we'll have to come up with some other check.
		    
		    elsif ( $cr->{TypeParams}[0] && $cr->{TypeParams}[0] eq 'integer' )
		    {
			$edt->add_condition($action, 'E_FORMAT', $record_col,
					    "value must be an unsigned integer or an external " .
					    "identifier of type '$IDP{$extid_type}'");
			next;
		    }
		}
		
		# At this point, throw an exception (a real one) if we are handed a value which is
		# an anonymous hash or array ref. In fact, the only reference type we accept is a
		# PBDB external identifier.
		
		if ( ref $value && reftype $value ne 'SCALAR' )
		{
		    my $type = ref $value;

		    if ( $type eq 'PBDB::ExtIdent' )
		    {
			$edt->add_condition($action, 'E_EXTTYPE', $record_col,
					    "no external identifier type was defined for this field");
		    }

		    else
		    {
			croak "invalid value type '$type' for col '$col'";
		    }
		}
		
		# Handle references to keys from other PBDB tables by checking them
		# against the specified table.
		
		if ( my $foreign_table = $cr->{FOREIGN_TABLE} || $FOREIGN_KEY_TABLE{$col} )
		{
		    if ( $value =~ /^@(.*)/ )
		    {
			my $check_table = $edt->{label_found}{$1};

			unless ( $check_table && $check_table eq $foreign_table )
			{
			    $edt->add_condition($action, 'E_LABEL_NOT_FOUND', $record_col, $value);
			    next;
			}
			
			$quote_this_value = 1;
			$action->substitute_label($col);
		    }
		    
		    elsif ( $value )
		    {
			no strict 'refs';
			
			# my $f_table = ${$foreign_table};
			my $foreign_col = $cr->{FOREIGN_KEY} || $FOREIGN_KEY_COL{$col} || $col;
			
			unless ( $edt->check_key($foreign_table, $foreign_col, $value) )
			{
			    $edt->add_condition($action, 'E_KEY_NOT_FOUND', $record_col, $value);
			    next;
			}
		    }
		    
		    else
		    {
			$value = undef;
		    }
		}
		
		# Otherwise, check the value according to the column type.
		
		elsif ( ref $cr->{TypeParams} )
		{
		    my ($type, @param) = @{$schema->{$col}{TypeParams}};
		    
		    if ( $type eq 'text' || $type eq 'data' )
		    {
			($value, $quote_this_value) = $edt->validate_character_value($action, $schema->{$col}, $record_col, $value);
			next if ref $value;
		    }
		    		    
		    elsif ( $type eq 'boolean' )
		    {
			$value = $edt->validate_boolean_value($action, $schema->{$col}, $record_col, $value);
			
			next if ref $value;
		    }
		    
		    elsif ( $type eq 'integer' )
		    {
			$value = $edt->validate_integer_value($action, $schema->{$col}, $record_col, $value);

			next if ref $value;
		    }
		    
		    elsif ( $type eq 'fixed' )
		    {
			$value = $edt->validate_fixed_value($action, $schema->{$col}, $record_col, $value);
			
			next if ref $value;
		    }
		    
		    elsif ( $type eq 'floating' )
		    {
			$value = $edt->validate_float_value($action, $schema->{$col}, $record_col, $value);
			
			next if ref $value;
		    }
		    		    
		    elsif ( $type eq 'enum' || $type eq 'set' )
		    {
			$value = $edt->validate_enum_value($action, $schema->{$col}, $record_col, $value);
			$quote_this_value = 1;
			next if ref $value;
		    }

		    elsif ( $type eq 'date' )
		    {
			($value, $quote_this_value) =
			    $edt->validate_datetime_value($action, $schema->{$col}, $record_col, $value);
			
			next if ref $value;
		    }

		    elsif ( $type eq 'geometry' )
		    {
			$value = $edt->validate_geometry_value($action, $schema->{$col}, $record_col, $value);
			next if ref $value;
		    }
		    
		    # If the data type is anything else, we just throw up our hands and accept
		    # whatever they give us. This might not be wise.

		    # Now store the cleaned value back into the record, so that before_action and
		    # after_action routines will have access to it.
		    
		    $record->{$record_col} = $value;
		}
	    }
	    
	    # Now we have to re-check whether we have a defined value or not. Some of the data
	    # types checked above turn whitespace into null, for example. If we have a value, then
	    # if a validator function has been defined for this column, call it. If the function
	    # returns a condition code, then add the specified error or warning condition.
	    
	    if ( defined $value )
	    {
		if ( $cr->{VALIDATOR} )
		{
		    my $v = $cr->{VALIDATOR};
		    
		    my ($code, @error_params) = ref $v eq 'CODE' ?
			&$v($edt, $value, $record_col, $action) :
			$edt->$v($value, $record_col, $action);
		    
		    if ( $code )
		    {
			$error_params[0] ||= 'value is not valid for this field';
			$edt->add_condition($action, $code, $record_col, @error_params);
			next;
		    }
		}
	    }
	    
	    # Otherwise, we don't have a defined value for this column. If the column name is
	    # 'modified' and this is an 'update' or 'replace' operation, or 'created' on a
	    # 'replace' operation, then let it go through as a null. This will cause the current
	    # timestamp to be stored.
	    
	    elsif ( $col eq 'modified' && $operation ne 'insert' )
	    {
		# let this column go through with a value of NULL
	    }
	    
	    elsif ( $col eq 'created' && $operation eq 'replace' )
	    {
		# let this column go through with a value of NULL
	    }
	    
	    # Otherwise, if this column is required to have a value, then throw an exception
	    # unless this is an update operation and the column does not appear in the action
	    # record. Any columns not explicitly given a value in an update operation are left
	    # with whatever value was previously stored in the table.
	    
	    elsif ( ($cr->{REQUIRED} || $cr->{NOT_NULL} ) &&
		    ( $operation ne 'update' || exists $record->{$record_col} ) )
	    {
		my $col_name;
		
		if ( $record_col ne $col ) { $col_name = $record_col; }
		else { $col_name = $cr->{ALTERNATE_NAME} || $record_col; }
		
		$edt->add_condition($action, 'E_REQUIRED', $col_name);
		next;
	    }
	    
	    # If this column does appear in the action record, then it should be explicitly
	    # included in the SQL statement. If it has a default value, we substitute
	    # that. Otherwise, we will let its value be NULL.
	    
	    elsif ( exists $record->{$record_col} )
	    {
		if ( defined $cr->{Default} )
		{
		    $value = $cr->{Default};
		    $quote_this_value = 1;
		    $is_default = 1;
		}
	    }
	    
	    # If we get here, then the column does not appear in the action record, is not
	    # explicitly required, and is not implicitly required for this operation
	    # (i.e. 'modified' with an 'update' or 'replace' operation). So we skip it.
	    
	    else
	    {
		next;
	    }
	}
	
	# If we were directed not to validate this column, we still need to check whether it is
	# mentioned in the record. If not, we skip it.
	
	elsif ( ! exists $record->{$record_col} )
	{
	    next;
	}
	
	# If this column has the ADMIN_SET property, then throw an exception unless
	# the user has 'admin' privilege, or unless the value being set is the default.
	
	if ( $cr->{ADMIN_SET} && ! $is_default && $action->permission ne 'admin' )
	{
	    $edt->add_condition($action, 'E_PERM_COL', $record_col);
	}
	
	# If we get here, then we have a good value! Push the column and value on the respective
	# lists. An undefined value is pushed as NULL, otherwise the value is quoted. The default
	# behavior for mariadb when given the empty string as a value for a numeric column is to
	# store zero. So we'll go with that.
	
	push @columns, $col;
	
	if ( defined $value )
	{
	    $value = $dbh->quote($value) if $quote_this_value;
	    push @values, $value;
	}
	
	else
	{
	    push @values, 'NULL';
	}
    }
    
    # If this is a primary action (not auxiliary) and there are any unrecognized keys in this
    # record, add an error or a warning depending on whether BAD_FIELDS is allowed for this
    # transaction.
    
    unless ( $action->is_aux )
    {
	foreach my $key ( keys %$record )
	{
	    next if $used{$key};
	    next if $key =~ /^_/;
	    next if $action->{ignore_field}{$key};
	    
	    if ( $edt->allows('BAD_FIELDS') )
	    {
		$edt->add_condition($action, 'W_BAD_FIELD', $key);
	    }
	    
	    else
	    {
		$edt->add_condition($action, 'E_BAD_FIELD', $key);
	    }
	}
    }
    
    # If the action has no errors, then we save the column values to it.
    
    unless ( $action->has_errors )
    {    
	# If we were directed to copy any old column values, do this first.
	
	if ( @copy_columns )
	{
	    my (@copy_values) = $edt->get_old_values($table, $action->keyexpr, join(',', @copy_columns));
	    
	    my (%copy_values, $substitution_count);
	    
	    foreach my $i ( 0..$#copy_columns )
	    {
		$copy_values{$copy_columns[$i]} = $dbh->quote($copy_values[$i]) if defined $copy_values[$i];
	    }
	    
	    foreach my $i ( 0..$#columns )
	    {
		if ( defined $copy_values{$columns[$i]} )
		{
		    $values[$i] = $copy_values{$columns[$i]};
		    $substitution_count++;
		}
		
		last if $substitution_count == scalar(keys %copy_values);
	    }
	}
	
	# Now store our column and value lists for subsequent use in constructing SQL statements.
	
	$action->set_column_values(\@columns, \@values);
    }
    
    return;
}


# validate_character_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into a boolean column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_character_value {
    
    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    my ($type, $size, $variable, $charset) = @{$column_defn->{TypeParams}};
    
    my $value_size = length($value);
    my $quote_this_value = 1;
    
    # If the character set of a text/char column is not utf8, then encode it into the proper
    # character set before checking the length.
    
    if ( $type eq 'text' && $charset && $charset ne 'utf8' )
    {
	# If the column is latin1, we can do the conversion in Perl.
	
	if ( $charset eq 'latin1' )
	{
	    $value = encode('cp1252', $value);
	    $value_size = length($value);
	}
	
	# Otherwise, we must let the database do the conversion.
	
	else
	{
	    my $dbh = $edt->dbh;
	    my $quoted = $dbh->quote($value);
	    $value = "convert($quoted using $charset)";
	    ($value_size) = $dbh->selectrow_array("SELECT length($value)");
	    $quote_this_value = 0;
	}
    }
    
    # If the size of the value exceeds the size of the column, then we either truncate the data if
    # the column has the ALLOW_TRUNCATE attribute or else reject the value.
    
    if ( defined $size && $value_size > $size )
    {
	my $word = $type eq 'text' ? 'characters' : 'bytes';
	
	if ( $column_defn->{ALLOW_TRUNCATE} )
	{
	    $value = substr($value, 0, $size);
	    $edt->add_condition($action, 'W_TRUNC', $record_col,
				"value was truncated to a length of $size $word");
	}
	
	else
	{
	    $edt->add_condition($action, 'E_WIDTH', $record_col,
				"value must be no more than $size $word in length, was $value_size");
	    return { };
	}
    }
    
    # If this column is required and the value is empty, add an error condition.
    
    if ( $value eq '' && $column_defn->{REQUIRED} )
    {
	$edt->add_condition($action, 'E_REQUIRED', $record_col);
	return { };
    }
    
    return ($value, $quote_this_value);
}


# validate_boolean_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into a boolean column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_boolean_value {
    
    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    # If the type is boolean, the value must be either 1 or 0. But we allow 'yes', 'no', 'true',
    # and 'false' as synonyms. A string that is empty or has only whitespace is turned into a
    # null.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
    }
    
    else
    {
	unless ( $value =~ qr{ ^ \s* (?: ( 1 | true | yes ) | ( 0 | false | no ) ) \s* $ }xsi )
	{
	    $edt->add_condition($action, 'E_FORMAT', $record_col,
				"value must be one of: 1, 0, true, false, yes, no");
	    return { };
	}
	
	return $1 ? 1 : 0;
    }
}


# validate_integer_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into an integer column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

our (%SIGNED_BOUND) = ( tiny => 127,
			small => 32767,
			medium => 8388607,
			regular => 2147483647,
			big => 9223372036854775807 );

our (%UNSIGNED_BOUND) = ( tiny => 255,
			  small => 65535,
			  medium => 16777215,
			  regular => 4294967295,
			  big => 18446744073709551615 );

sub validate_integer_value {
    
    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    my ($type, $unsigned, $size) = @{$column_defn->{TypeParams}};

    my $max = $unsigned ? $UNSIGNED_BOUND{$size} : $SIGNED_BOUND{$size};
    
    # First make sure that the value is either empty or matches the proper format. A value which
    # is empty or contains only whitespace will be treated as a NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
    }
    
    elsif ( $value !~ qr{ ^ \s* ( [-+]? ) \s* ( \d+ ) \s* $ }xs )
    {
	my $phrase = $unsigned ? 'an unsigned' : 'an';
	
	$edt->add_condition($action, 'E_FORMAT', $record_col,
			    "value must be $phrase integer");
	return { };
    }
    
    elsif ( $unsigned )
    {
	$value = $2;
	
	if ( $1 && $1 eq '-' )
	{
	    $edt->add_condition($action, 'E_RANGE', $record_col, 
				"value must an unsigned decimal number");
	    return { };
	}
	
	elsif ( $value > $max )
	{
	    $edt->add_condition($action, 'E_RANGE', $record_col,
				"value must be less than or equal to $max");
	    return { };
	}
	
	else
	{
	    return $value;
	}
    }
    
    else
    {
	$value = ($1 && $1 eq '-') ? "-$2" : $2;
	
	my $lower = $max + 1;
	
	if ( $value > $max || (-1 * $value) > $lower )
	{
	    $edt->add_condition($action, 'E_RANGE', $record_col, 
				"value must lie between -$lower and $max");
	    return { };
	}
	
	else
	{
	    return $value;
	}
    }
}


# validate_fixed_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into a fixed-point decimal column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_fixed_value {

    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    my ($type, $unsigned, $whole, $precision) = @{$column_defn->{TypeParams}};
    
    # First make sure that the value is either empty or matches the proper format.  A value which
    # is empty or contains only whitespace is turned into NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
    }
    
    elsif ( $value !~ $DECIMAL_NUMBER_RE )
    {
	my $phrase = $unsigned ? 'an unsigned' : 'a';
	
	$edt->add_condition($action, 'E_FORMAT', $record_col,
			    "value must be $phrase decimal number");
	return { };
    }
    
    else
    {
	# If the column is unsigned, make sure there is no minus sign.
	
	if ( $unsigned && defined $1 && $1 eq '-' )
	{
	    $edt->add_condition($action, 'E_RANGE', $record_col,
				"value must be an unsigned decimal number");
	    return { };
	}
	
	# Now put the number back together from the regex captures. If there is an
	# exponent, reformat it as a fixed point.
	
	my $sign = $1 && $1 eq '-' ? '-' : '';
	my $intpart = $2 // '';
	my $fracpart = $3 // $4 // '';
	
	if ( $6 )
	{
	    my $exponent = ($5 && $5 eq '-' ? "-$6" : $6);
	    my $formatted = sprintf("%.10f", "${intpart}.${fracpart}E${exponent}");
	    
	    ($intpart, $fracpart) = split(/[.]/, $formatted);
	}
	
	# Check that the number of digits is not exceeded, either before or after the decimal. In
	# the latter case, we add an error unless the column property ALLOW_TRUNCATE is set in
	# which case we add a warning.
	
	$intpart =~ s/^0+//;
	$fracpart =~ s/0+$//;
	
	if ( $intpart && length($intpart) > $whole )
	{
	    my $total = $whole + $precision;
	    
	    $edt->add_condition($action, 'E_RANGE', $record_col,
				"value is too large for decimal($total,$precision)");
	    return { };
	}
	
	if ( $fracpart && length($fracpart) > $precision )
	{
	    my $total = $whole + $precision;
	    
	    if ( $column_defn->{ALLOW_TRUNCATE} )
	    {
		$edt->add_condition($action, 'W_TRUNC', $record_col,
				    "value has been truncated to decimal($total,$precision)");
	    }
	    
	    else
	    {
		$edt->add_condition($action, 'E_WIDTH', $record_col,
				    "too many decimal digits for decimal($total,$precision)");
		return { };
	    }
	}
	
	# Rebuild the value, with the fracional part trimmed.
	
	$value = $sign;
	$value .= $intpart || '0';
	$value .= '.' . substr($fracpart, 0, $precision);
	
	return $value;
    }
}


# validate_float_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into a floating-point decimal column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_float_value {

    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    my ($type, $unsigned, $precision) = @{$column_defn->{TypeParams}};
    
    # First make sure that the value is either empty or matches the proper format. A value which
    # is empty or contains only whitespace will be treated as a NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
    }
    
    elsif ( $value !~ $DECIMAL_NUMBER_RE )
    {
	my $phrase = $unsigned ? 'an unsigned' : 'a';
	
	$edt->add_condition($action, 'E_FORMAT', $record_col,
			    "value must be $phrase floating point number");
	return { };
    }
    
    else
    {
	my $sign = (defined $1 && $1 eq '-') ? '-' : '';
	
	# If the column is unsigned, make sure there is no minus sign.
	
	if ( $unsigned && $sign eq '-' )
	{
	    $edt->add_condition($action, 'E_RANGE', $record_col,
				"value must be an unsigned floating point number");
	    return { };
	}
	
	# Put the pieces of the value back together.
	
	$value = $sign . ( $2 // '' ) . '.';
	$value .= ( $3 // $4 // '' );
	
	if ( $6 )
	{
	    my $esign = $5 eq '-' ? '-' : '';
	    $value .= 'E' . $esign . $6;
	}
	
	# Then check that the number is not too large to be represented, given the size of the
	# field. We are conservative in the bounds we check. We do not check for the number of
	# decimal places being exceeded, because floating point is naturally inexact. Also, if
	# maximum digits were specified we ignore these.
			    
	my $bound = $precision eq 'double' ? 1E308 : 1E38;
	my $word = $precision eq 'float' ? 'single' : 'double';
	
	if ( $value > $bound || ( $value < 0 && -$value > $bound ) )
	{
	    $edt->add_condition($action, 'E_RANGE', $record_col,
				"magnitude is too large for $word-precision floating point");
	    return { };
	}

	return $value;
    }
}


# validate_enum_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into an enumerated or set valued column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_enum_value {

    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    my ($type, $good_values) = @{$column_defn->{TypeParams}};
    
    # If the data type is either 'set' or 'enum', then we check to make sure that the value is one
    # of the allowable ones. We always match without regard to case, using the Unicode 'fold case'
    # function (fc).
    
    use feature 'fc';
    
    $value =~ s/^\s+//;
    $value =~ s/\s+$//;
    
    my @raw = $value;
    
    if ( $type eq 'set' )
    {
	my $sep = $column_defn->{VALUE_SEPARATOR} || qr{ \s* , \s* }xs;
	@raw = split $sep, $value;
    }
    
    my (@good, @bad);
    
    foreach my $v ( @raw )
    {
	next unless defined $v && $v ne '';
	
	if ( $good_values->{fc $v} )
	{
	    push @good, $v;
	}
	
	else
	{
	    push @bad, $v;
	}
    }
    
    if ( @bad )
    {
	my $value_string = join(', ', @bad);
	my $word = @bad > 1 ? 'values' : 'value';
	my $word2 = @bad > 1 ? 'are' : 'is';
	
	$edt->add_condition($action, 'E_RANGE', $record_col,
			    "$word '$value_string' $word2 not allowed for this table column");
	return { };
    }
    
    if ( @good )
    {
	return join(',', @good);
    }
    
    else
    {
	return undef;
    }
}


# validate_datetime_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into a time or date or datetime valued
# column in the database. If it is not, add an error condition and return a non-scalar value as a
# flag to indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_datetime_value {
    
    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    my ($type, $specific) = @{$column_defn->{TypeParams}};
    
    if ( $value =~ qr{ ^ now (?: [(] [)] ) ? $ }xsi )
    {
	return 'NOW()';
    }

    elsif ( $value =~ qr{ ^ \d\d\d\d\d\d\d\d\d\d+ $ }xs )
    {
	return "FROM_UNIXTIME($value)";
    }
    
    elsif ( $specific eq 'time' )
    {
	if ( $value !~ qr{ ^ \d\d : \d\d : \d\d $ }xs )
	{
	    $edt->add_condition($action, 'E_FORMAT', $record_col, "invalid time format '$value'");
	    return { };
	}
	
	return ($value, 1);
    }
    
    else
    {
	if ( $value !~ qr{ ^ ( \d\d\d\d - \d\d - \d\d ) ( \s+ \d\d : \d\d : \d\d ) ? $ }xs )
	{
	    $edt->add_condition($action, 'E_FORMAT', $record_col, "invalid datetime format '$value'");
	    return { };
	}
	
	unless ( defined $2 && $2 ne '' )
	{
	    $value .= ' 00:00:00';
	}
	
	return ($value, 1);
    }
}


# validate_geometry_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into a geometry valued column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_geometry_value {
    
    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    my ($type, $specific) = @{$column_defn->{TypeParams}};
    
    # $$$ we still need to write some code to validate these.
    
    return $value;
}


# check_key ( table, value )
#
# Make sure that the specified key exists in the specified table.

sub check_key {
    
    my ($edt, $table_specifier, $col, $value) = @_;
    
    if ( $FOREIGN_KEY_COL{$col} )
    {
	$col = $FOREIGN_KEY_COL{$col};
    }
    
    my $quoted = $edt->dbh->quote($value);
    
    my $sql = "SELECT $col FROM $TABLE{$table_specifier} WHERE $col=$quoted";

    $edt->debug_line( "$sql\n" );
    
    my ($found) = $edt->dbh->selectrow_array($sql);

    return $found;
}


# Error and warning conditions
# ----------------------------
# 
# We define a separate package for error and warning conditions.

package EditTransaction::Condition;


# new ( action, code, data... )
#
# Create a new EditTransaction::Condition for the specified action, which may be undef. The second
# argument must be a condition code, i.e. 'E_PERM' or 'W_NOT_FOUND'. The remaining arguments, if
# any, indicate the particulars of the condition and are used in generating a string value from
# the condition record.

sub new {
    
    my $class = shift;
    
    return bless [ @_ ], $class;
}


# code ( )
#
# Return the code associated with this error condition.

sub code {
    
    my ($condition) = @_;
    
    return $condition->[1];
}


# label ( )
#
# Return the label associated with this error condition. If no action was specified, the empty
# string is returned.

sub label {
    
    my ($condition) = @_;
    
    return $condition->[0] && $condition->[0]->isa('EditTransaction::Action') ?
	$condition->[0]->label : '';
}


# table ( )
#
# Return the table associated with this error condition. If no action was specified, the empty
# string is returned.

sub table {

    my ($condition) = @_;

    return $condition->[0] && $condition->[0]->isa('EditTransaction::Action') ?
	$condition->[0]->table : '';
}


# data ( )
#
# Return the data elements, if any, associated with this error condition.

sub data {
    
    my ($condition) = @_;
    
    return @$condition[2..$#$condition];
}

1;
