# 
# The Paleobiology Database
# 
#   EditTransaction.pm - base class for data acquisition and modification
# 

package EditTransaction;

use Moo;

with 'EditTransaction::Validation';

use strict;
no warnings 'uninitialized';

use Carp qw(carp croak);
use Scalar::Util qw(weaken blessed reftype looks_like_number);
use List::Util qw(sum reduce any);
use Hash::Util qw(lock_hash);
use Switch::Plain;

use ExternalIdent qw(%IDP %IDRE);
use TableDefs qw(get_table_property %TABLE %COMMON_FIELD_IDTYPE);
use TableData qw(get_table_schema);
use Permissions;

use feature 'unicode_strings', 'postderef';

use EditTransaction::Condition;
use EditTransaction::Action;
use EditTransaction::Datalog;

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
		MOVE_SUBORDINATES => 1,
		NOT_FOUND => 1,
		NOT_PERMITTED => 1,
    		NO_RECORDS => 1,
		PROCEED => 1,
		BAD_FIELDS => 1,
		DEBUG_MODE => 1,
		SILENT_MODE => 1,
		IMMEDIATE_MODE => 1,
		FIXUP_MODE => 1,
		NO_LOG_MODE => 1,
		VALIDATION_ONLY => 1 } );

our (%CONDITION_BY_CLASS) = ( EditTransaction => {		     
		C_CREATE => "Allow 'CREATE' to create records",
		C_LOCKED => "Allow 'LOCKED' to update locked records",
    		C_NO_RECORDS => "Allow 'NO_RECORDS' to allow transactions with no records",
		C_ALTER_TRAIL => "Allow 'ALTER_TRAIL' to explicitly set crmod and authent fields",
		C_MOVE_SUBORDINATES => "Allow 'MOVE_SUBORDINATES' to allow subordinate link values to be changed",
		E_NO_KEY => "The %1 operation requires a primary key value",
		E_HAS_KEY => "You may not specify a primary key value for the %1 operation",
		E_KEY_NOT_FOUND => "Field '%1': no record of the proper type was found with key '%2'",
		E_BAD_REFERENCE => "Field '%1': no record of the proper type was found with label '%2'",
		E_NOT_FOUND => "No record was found with this key",
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
		E_BAD_CONDITION => "%1 '%2'",
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
		E_UNRECOGNIZED => "Does not match any record type accepted by this operation",
		E_ACTION => "%1",
		W_BAD_ALLOWANCE => "Unknown allowance '%1'",
		W_EXECUTE => "%1",
		W_UNCHANGED => "%1",
		W_PARAM => "%1",
		W_TRUNC => "Field '%1': %2",
		W_BAD_FIELD => "Field '%1' does not correspond to any column",
		W_EMPTY_RECORD => "Item is empty",
		W_UNKNOWN_CONDITION => "Unrecognized warning condition '%1': %2",
		UNKNOWN => "MISSING ERROR MESSAGE" });

our (%SPECIAL_BY_CLASS);

our (@TRANSACTION_STATUS) = qw(active committed aborted);

our (%TEST_PROBLEM);	# This variable can be set in order to trigger specific errors, in order
                        # to test the error-response mechanisms.

# We set @CARP_NOT because we specifically do not want subclasses of EditTransaction to be
# passed over as safe.

our (@CARP_NOT) = qw(EditTransaction::Action);

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
    
    croak "new EditTransaction: permissions object is required"
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
		action_ref => { },
		current_action => undef,
		action_count => 0,
		completed_count => 0,
		exec_count => 0,
		fail_count => 0,
		skip_count => 0,
		errors => [ ],
		warnings => [ ],
		error_count => 0,
		warning_count => 0,
		demoted_count => 0,
		condition_code => { },
		tables => { },
		label_found => { },
		commit_count => 0,
		rollback_count => 0,
		transaction => '',
		debug_mode => 0,
	        errlog_mode => 1 } ;
    
    bless $edt, $class;
    
    # Store the request, dbh, and debug flag as local fields. If we are storing a reference to a
    # request object, we must weaken it to ensure that this object will be destroyed when it goes
    # out of scope. Circular references might otherwise prevent this.
    
    if ( $request_or_dbh->can('get_connection') )
    {
	$edt->{request} = $request_or_dbh;
	weaken $edt->{request};
	
	$edt->{dbh} = $request_or_dbh->get_connection;
	
	$edt->{debug_mode} = $request_or_dbh->debug if $request_or_dbh->can('debug');
	
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
	}
	
	else
	{
	    $edt->add_condition('W_BAD_ALLOWANCE', $k);
	}
    }
    
    # The allowance DEBUG_MODE turns debugging mode on. The allowance SILENT_MODE turns
    # error logging mode off, which is on by default.
    
    if ( $edt->{allows}{DEBUG_MODE} )
    {
	$edt->{debug_mode} = 1;
    }
    
    elsif ( $edt->{allows}{SILENT_MODE} )
    {
	$edt->{errlog_mode} = 0;
    }	
    
    # Throw an exception if we don't have a valid database handle.
    
    croak "missing dbh" unless ref $edt->{dbh};
    
    # Set the database handle attributes properly.
    
    $edt->{dbh}->{RaiseError} = 1;
    $edt->{dbh}->{PrintError} = 0;
    
    # If IMMEDIATE_MODE was specified, then immediately start a new transaction and set the
    # the 'execute_immediately' flag. The same effect can be provided by calling the method
    # 'start_execution' on this new object.
    
    if ( $edt->{allows}{IMMEDIATE_MODE} )
    {
	$edt->_start_transaction;
	$edt->{execute_immediately} = 1;
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


sub status {

    return $_[0]{transaction} // 'init';
}


sub transaction {

    goto &status;
}


sub has_started {

    return $_[0]{transaction} ne '';
}


sub is_active {

    return $_[0]{transaction} eq 'active';
}


sub has_finished {

    return $_[0]{transaction} =~ /^committed|^aborted/;
}


sub has_committed {

    return $_[0]{transaction} eq 'committed';
}


sub can_accept {
    
    return ! $_[0]{transaction} || $_[0]{transaction} eq 'active';
}


sub can_proceed {

    return (! $_[0]{transaction} || $_[0]{transaction} eq 'active') && ! $_[0]{error_count};
}


sub perms {
    
    return $_[0]->{perms};
}


sub role {
    
    return $_[0]->{perms} ? $_[0]->{perms}->role : '';
}


# Debugging and error message display
# -----------------------------------

# error_line ( text )
# 
# This method is called internally to display certain error messages, including exceptions
# that were generated during the execution of code in this module. These messages are
# shown if either debugging mode or error logging mode is on.

sub error_line {
    
    if ( ref $_[0] && ($_[0]->{errlog_mode} || $_[0]->{debug_mode}) )
    {
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
}


# debug_line ( text )
#
# This method is called internally to display extra output for debugging purposes. These messages
# are shown if debugging mode is on.
    
sub debug_line {
    
    if ( ref $_[0] && $_[0]->{debug_mode} )
    {    
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
# Turn debug mode on or off, or return the current value if no argument is given. When
# this mode is on, all SQL statements are printed to standard error along with some extra
# debugging output. The initial line makes queries as efficient as possible.

sub debug_mode {
    
    return $_[0]{debug_mode} unless defined $_[1];
    
    my ($edt, $value) = @_;
    
    if ( $value )
    {
	$edt->{debug_mode} = 1;
    }

    elsif ( defined $value && ! $value )
    {
	$edt->{debug_mode} = 0;
    }
    
    return $edt->{debug_mode};
}


# silent_mode ( value )
#
# Turn error logging mode on or off, or return the current value if no argument is
# given. The given value is inverted: a true value will turn off error logging mode, while
# a false value turns it on. The first line makes queries as efficient as possible.

sub silent_mode {
    
    return $_[0]{errlog_mode} ? 0 : 1 unless defined $_[1];
    
    my ($edt, $value) = @_;
    
    if ( $value )
    {
	$edt->{errlog_mode} = 0;
    }
    
    elsif ( defined $value && ! $value )
    {
	$edt->{errlog_mode} = 1;
    }
    
    return $edt->{errlog_mode} ? 0 : 1;
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
# or errors, under the 'PROCEED', 'NOT_FOUND', or 'NOT_PERMITTED' allowances. These are treated as
# warnings.
# 
# Allowed conditions must be specified for each EditTransaction object when it is created.


# register_allowances ( name... )
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


# has_allowance ( name )
#
# Return true if the specified class or object has the specified allowance.

sub has_allowance {

    my ($class, $name) = @_;

    if ( ref $class )
    {
	return $ALLOW_BY_CLASS{ref $class}{$name} || $ALLOW_BY_CLASS{EditTransaction}{$name};
    }

    else
    {
	return $ALLOW_BY_CLASS{$class}{$name} || $ALLOW_BY_CLASS{EditTransaction}{$name};
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
	
	# Make sure the code follows the proper pattern and the template is not empty.
	
	croak "bad condition code '$code'" unless $code =~ qr{ ^ [CEW]_[A-Z0-9_-]+ $ }xs;
	croak "bad condition template '$template'" unless defined $template && $template ne '';
	
	$CONDITION_BY_CLASS{$class}{$code} = $template;
    }
};


# add_condition ( [action], code, param... )
# 
# Add a condition (error, caution, or warning) that pertains to the either the entire
# transaction or to a single action. The condition is specified by a code, optionally
# followed by one or more parameters which will be used later to generate an error or
# warning message. Conditions that pertain to an action may be demoted to warnings if
# any of the allowances PROCEED, NOT_FOUND, or NOT_PERMITTED was specified.
# 
# If the first parameter is a reference to an action, then the condition will be attached
# to that action. If it is the undefined value or the string 'main', then the condition
# will apply to the transaction as a whole. Otherwise, the condition will be attached to
# the current action if there is one or the transaction as a whole otherwise.
#
# This method either adds the condition, or else throws an exception.

sub add_condition {
    
    my ($edt, @params) = @_;
    
    # Start by determining the action (if any) to which this condition should be attached.
    
    my ($action, $code);

    # If the first parameter is a Perl reference, it must be a reference to an
    # action. This method should only be called in this way from within this class and its
    # subclasses. Outside code should always refer to actions using string references.
    
    if ( ref $params[0] )
    {
	$action = shift @params;
	
	unless ( $action->isa('EditTransaction::Action') )
	{
	    my $ref_string = ref $action;
	    croak "'$ref_string' is not an action reference";
	}
    }
    
    # If the first parameter is either 'main' or the undefined value, then the condition
    # will be attached to the transaction as a whole.
    
    elsif ( ! defined $params[0] || $params[0] eq 'main' )
    {
	shift @params;
    }
    
    # If the first parameter starts with '@', look it up as an action reference. Calls of
    # this kind will always come from outside code.
    
    elsif ( $params[0] =~ /^@/ )
    {
	unless ( $action = $edt->{action_ref}{$params[0]} )
	{
	    croak "no matching action found for '$params[0]'";
	}
	
	shift @params;
    }
    
    # Otherwise, default to the current action. Depending on when this method is called,
    # it may be empty, in which case the condition will be attached to the transaction as
    # a whole. If the first parameter is 'latest', remove it.
    
    else
    {
	$action = $edt->{current_action};

	if ( $action && ! ( ref $action && $action->isa('EditTransaction::Action') ) )
	{
	    croak "current_action is not a valid action reference";
	}
	
	shift @params if $params[0] eq 'latest';
    }
    
    # There must be at least one remaining parameter, and it must match the syntax of a
    # condition code. If it starts with F, change it back to E. If it does not have the
    # form of a condition code, throw an exception. Any subsequent parameters will be kept
    # and used to generate the condition message.

    if ( $params[0] && $params[0] =~ qr{ ^ [CEFW]_[A-Z0-9_-]+ $ }xs )
    {
	$code = shift @params;

	if ( $code =~ /^F/ )
	{
	    substr($code, 0, 1) = 'E';
	}
    }
    
    else
    {
	croak "'$params[0]' is not a valid condition code";
    }
    
    # When an error condition is attached to an action and this transaction allows
    # PROCEED, the error is demoted to a warning. If this transaction allows NOT_FOUND,
    # then an E_NOT_FOUND error is demoted to a warning but others are not.

    if ( $action && $code =~ /^E/ && ref $edt->{allows} eq 'HASH' )
    {
	if ( $edt->{allows}{PROCEED} ||
	     $edt->{allows}{NOT_FOUND} && $code eq 'E_NOT_FOUND' ||
	     $edt->{allows}{NOT_PERMITTED} && $code eq 'E_PERM' )
	{
	    substr($code,0,1) =~ 'F';
	}
    }
    
    # Create a condition object using these parameters, add it to the proper list, and
    # update the relevant counts.  The following code uses guard statements in case some
    # bug has overwritten the count or list variables and they longer have the proper
    # type. It seems safer to add this condition as the only one of its type rather than
    # abandon it and throw an unrelated exception or leave the count with an invalid value.
    
    my $condition = EditTransaction::Condition->new($action, $code, @params);
    
    # If this condition belongs to an action, add it to that action. Adjust the condition
    # counts for the transaction, but only if the action is not marked as skipped.
    
    if ( $action )
    {
	# If the code starts with E or C then it represents an error or caution.
	
	if ( $code =~ /^[EC]/ )
	{
	    $action->_add_error($condition);

	    if ( $action->status ne 'skipped' )
	    {
		$edt->{error_count} = 0 unless looks_like_number $edt->{error_count};
		$edt->{error_count}++;
	    }
	}
	
	# If the code starts with F, then it represents a demoted error. It counts as a
	# warning for the transaction as a whole, but as an error for the action.

	elsif ( $code =~ /^F/ )
	{
	    $action->_add_error($condition);
	    
	    if ( $action->status ne 'skipped' )
	    {
		$edt->{demoted_count} = 0 unless looks_like_number $edt->{demoted_count};
		$edt->{demoted_count}++;
		$edt->{warning_count} = 0 unless looks_like_number $edt->{warning_count};
		$edt->{warning_count}++;
	    }
	}
	
	# Otherwise, it represents a warning.
	
	else
	{
	    $action->_add_warning($condition);

	    if ( $action->status ne 'skipped' )
	    {
		$edt->{warning_count} = 0 unless looks_like_number $edt->{warning_count};
		$edt->{warning_count}++;
	    }
	}
    }
    
    # Otherwise, the condition is attached to the transaction as a whole as either an
    # error/caution or a warning.
    
    elsif ( $code =~ /^[EC]/ )
    {
	$edt->{errors} = [ ] unless ref $edt->{errors} eq 'ARRAY';
	push $edt->{errors}->@*, $condition;
	
	$edt->{error_count} = 0 unless looks_like_number $edt->{error_count};
	$edt->{error_count}++;
    }
    
    else
    {
	$edt->{warnings} = [ ] unless ref $edt->{errors} eq 'ARRAY';
	push $edt->{warnings}->@*, $condition;
	
	$edt->{warning_count} = 0 unless looks_like_number $edt->{warning_count};
	$edt->{warning_count}++;
    }
    
    # Keep track of how many times each individual condition code is generated.
    
    $edt->{condition_code}{$code}++ unless $action && $action->status eq 'skipped';

    # Don't return anything.
    
    return;
}


# action_error ( action, is_fatal, code, param... )
#
# Add an error condition to the specified action. If the second parameter is true, do not
# demote it even if PROCEED or NOT_FOUND are allowed. Unlike add_condition, the action
# parameter is required. It can be either a Perl reference or an action reference. If the
# code is not valid, it is replaced by E_ACTION.

sub action_error {

    my ($edt, $ap, $is_fatal, $code, @params) = @_;
    
    my ($action, $condition);
    
    if ( $ap && $ap =~ /^@/ )
    {
	unless ( $action = $edt->{action_ref}{$ap} )
	{
	    croak "no matching action found for '$ap'";
	}
    }
    
    elsif ( ref $ap )
    {
	$action = $ap if $ap->isa('EditTransaction::Action');
    }

    unless ( $action )
    {
	croak "'$ap' is not an action reference";
    }
    
    unless ( $code =~ qr{ ^ [CEFW]_[A-Z0-9_-]+ $ }xs )
    {
	$code = 'E_ACTION';
    }
    
    if ( $is_fatal )
    {    
	$condition = EditTransaction::Condition->new($action, $code, @params);
	
	$action->_add_error($condition);
	
	if ( $action->status ne 'skipped' )
	{
	    $edt->{error_count} = 0 unless looks_like_number $edt->{error_count};
	    $edt->{error_count}++;
	}
    }

    else
    {
	$edt->add_condition($action, $code, @params);
    }
}


# # add_condition_simple ( code, param... )
# #
# # Attach the specified condition to the current action, or to the transaction as a whole
# # if the current action is undefined. This method is designed to be called only from this
# # class or a subclass. It does not do any argument checking, so it is risky to call it
# # from interface code. However, it is more robust. Unlike add_condition, it should never
# # throw an exception as long as the first parameter is a string.

# sub add_condition_simple {
    
#     my ($edt, $code, @params) = @_;

#     my $action;
    
#     # If current_action is defined and is a reference to an action, attach the condition
#     # to it. If the condition is an error and the transaction allows PROCEED or NOT_FOUND,
#     # demote the error to a warning.
    
#     if ( $edt->{current_action} && ref $edt->{current_action} &&
# 	 $edt->{current_action}->isa('EditTransaction::Action') )
#     {
# 	$action = $edt->{current_action};
	
# 	if ( $code =~ /^E/ && ref $edt->{allows} eq 'HASH' &&
# 	     ( $edt->{allows}{PROCEED} || $edt->{allows}{NOT_FOUND} && $code eq 'E_NOT_FOUND' ) )
# 	{
# 	    substr($code,0,1) =~ 'F';
# 	}
#     }
    
#     # Create a condition object using these parameters, and attach it to the action or to
#     # the transaction as a whole.
    
#     my $condition = EditTransaction::Condition->new($action, $code, @params);
    
#     $edt->_attach_condition($action, $condition, $code);
# }


# conditions ( [selector], [type] )
# 
# In list context, return a list of stringified error and/or warning conditions recorded
# for this transaction. In scalar context, return how many there are. The selector and
# type can be given in either order. The selector can be any of the following, defaulting
# to 'all':
# 
#     main		Return conditions that are attached to the transaction as a whole.
#     latest		Return conditions that are attached to the latest action.
#     @...              Return conditions that are attached to the referenced action.
#     all		Return all conditions.
# 
# The type can be any of the following, also defaulting to 'all':
# 
#     errors		Return only error conditions.
#     fatal		With selector 'all', return error conditions that were not demoted to warnings.
#     nonfatal		With selector 'all', return warning conditions and demoted errors.
#     warnings		Return only warning conditions.
#     all		Return all conditions.
#
# The types 'fatal' and 'nonfatal' are the same as 'errors' and 'warnings' respectively when used
# with any selector other than 'all'. 

my %TYPE_KEYS = ( errors => ['errors'],
		  fatal => ['errors'],
		  nonfatal => ['warnings'],
		  warnings => ['warnings'],
		  all => ['errors', 'warnings'] );

my $csel_pattern = qr{ ^ (?: main$|latest$|all$|[@]. ) }xs;
my $ctyp_pattern = qr{ ^ (?: errors$|fatal$|nonfatal$|warnings$ ) }xs;

sub conditions {
    
    my ($edt, @params) = @_;
    
    # First extract the selector and type from the parameters. They can occur in either
    # order. Both are optional, defaulting to 'all'.
    
    my $selector = 'all';
    my $type = 'all';
    
    if ( $params[0] )
    {
	if ( $params[0] =~ $csel_pattern )
	{
	    $selector = $params[0];
	}

	elsif ( $params[0] =~ $ctyp_pattern )
	{
	    $type = $params[0];
	}

	elsif ( $params[0] ne 'all' )
	{
	    croak "invalid argument '$params[0]'";
	}
    }

    if ( $params[1] )
    {
	if ( $params[1] =~ $csel_pattern && $selector eq 'all' )
	{
	    $selector = $params[1];
	}

	elsif ( $params[1] =~ $ctyp_pattern && $type eq 'all' )
	{
	    $type = $params[1];
	}

	elsif ( $params[1] ne 'all' )
	{
	    croak "invalid argument '$params[1]'";
	}
    }
    
    # Then extract the requested data.
    
    my @keys = $TYPE_KEYS{$type}->@*;
    
    # For 'main', we return either or both of the 'errors' and 'warnings' lists from $edt.
    
    if ( $selector eq 'main' )
    {
	if ( wantarray )
	{
	    return map( $edt->_condition_string($_), map( $edt->{$_} && $edt->{$_}->@*, @keys ));
	}
	
	else
	{
	    return sum( map( scalar($edt->{$_} && $edt->{$_}->@*), @keys ));
	}
    }
    
    # For 'latest', we return either or both of the 'errors' and 'warning' lists from
    # the current action. For an action reference, we use the corresponding action.
    
    elsif ( $selector =~ /^latest|^@/ )
    {
	my $action;
	
	if ( $selector eq 'latest' )
	{
	    $action = $edt->{current_action} || return;
	}
	
	else
	{
	    $action = $edt->{action_ref}{$selector} || croak "no matching action found for '$selector'";
	}
	
	if ( wantarray )
	{
	    return map( $edt->_condition_string($_),
			map( $action->{$_} && $action->{$_}->@*, @keys ));
	}
	
	else
	{
	    return sum( map( scalar($action->{$_} && $action->{$_}->@*), @keys ));
	}
    }
    
    # For 'all', the response depends on the type.
    
    else
    {
	# If we were called in list context with a type of 'all' or 'nonfatal', we must look at
	# the lists stored under both keys.
	
	if ( wantarray && ( $type eq 'all' || $type eq 'nonfatal' ) )
	{
	    my $filter = '!EC' if $type eq 'nonfatal';
	    
	    return map { $edt->_condition_string($_, $filter) }
		$edt->{errors} ? $edt->{errors}->@* : (),
		$edt->{warnings} ? $edt->{warnings}->@* : (),
		map { ( $_->{errors} ? $_->{errors}->@* : () ,
			$_->{warnings} ? $_->{warnings}->@* : () ) }
		grep { $_->{status} ne 'skipped' } $edt->{action_list}->@*;
	}
	
	# With any other type, there is just one key to check.
	
	elsif ( wantarray )
	{
	    my $key = $keys[0];
	    my $filter = 'EC' if $type eq 'fatal';
	    
	    return map { $edt->_condition_string($_, $filter) }
		$edt->{$key} ? $edt->{$key}->@* : (),
		map { $_->{$key} ? $_->{$key}->@* : () }
		grep { $_->{status} ne 'skipped' } $edt->{action_list}->@*;
	}
	
	# If we were called in scalar context, we just return the appropriate counts.
	
	elsif ( $type eq 'errors' )
	{
	    return $edt->{error_count} + $edt->{demoted_count};
	}

	elsif ( $type eq 'fatal' )
	{
	    return $edt->{error_count};
	}

	elsif ( $type eq 'nonfatal' )
	{
	    return $edt->{warning_count};
	}

	elsif ( $type eq 'warnings' )
	{
	    return $edt->{warning_count} - $edt->{demoted_count};
	}

	else
	{
	    return $edt->{error_count} + $edt->{warning_count};
	}
    }
}


# _condition_string ( condition, filter )
#
# Return a stringified version of the specified condition object. If it is attached to an action,
# insert the label right after the code. If a filter is specified, only return the string if it
# matches. Supported filters are 'EC' and '!EC'.

sub _condition_string {

    my ($edt, $condition, $filter) = @_;
    
    return unless $condition;
    
    my $code = $condition->code;
    
    if ( $filter eq 'EC' )
    {
	return unless $code =~ /^[EC]/;
    }
    
    elsif ( $filter eq '!EC' )
    {
	return unless $code !~ /^[EC]/;
    }
    
    my $label = $condition->label;
    $label = " ($label)" if defined $label && $label ne '';

    if ( my $msg = $edt->generate_msg($condition) )
    {
	return "${code}${label}: ${msg}";
    }

    else
    {
	return "${code}${label}";
    }
}


sub _condition_simple {

    my ($edt, $condition) = @_;

    return unless $condition;

    my $code = $condition->code;
    
    if ( my $msg = $edt->generate_msg($condition) )
    {
	return "${code}: ${msg}";
    }

    else
    {
	return $code;
    }
}


# has_errors ( )
#
# If this EditTransaction has accumulated any fatal errors, return the count. Otherwise, return
# false.

sub has_errors {

    return $_[0]{error_count};
}


# errors ( [selector] )
# 
# This is provided for backward compatibility. When called with no arguments in scalar context, it
# efficiently returns the fatal condition count. When called with 'latest', it returns all
# errors. Otherwise, it returns fatal errors.

sub errors {
    
    my ($edt, $selector) = @_;
    
    return $_[0]{error_count} unless wantarray || $selector;
    
    if ( $selector && $selector eq 'latest' )
    {
	return $edt->conditions('latest', 'errors');
    }

    else
    {
	$selector ||= 'all';
	return $edt->conditions($selector, 'fatal');
    }
}


# warnings ( [selector] )
# 
# Like 'errors', this method is provided for backward compatibility. When called with no arguments in
# scalar context, it efficiently returns the nonfatal condition count. When called with 'latest', it
# returns just warnings. Otherwise, it returns nonfatal conditions.

sub warnings {

    my ($edt, $selector) = @_;
    
    return $_[0]{warning_count} unless wantarray || $selector;
    
    if ( $selector && $selector eq 'latest' )
    {
	return $edt->conditions('latest', 'warnings');
    }

    else
    {
	$selector ||= 'all';
	return $edt->conditions($selector, 'nonfatal' );
    }
}


#     if ( wantarray )
#     {
# 	return ($edt->{errors} ? $edt->{errors}->@* : ()),
# 	    grep { $_->code =~ /^[EC]/ }
# 	    map { $_->{errors} ? $_->{errors}->@* : () }
# 	    grep { $_->status ne 'skipped' } $edt->{action_list}->@*;
#     }
    
#     else
#     {
# 	return $edt->{error_count};
#     }
# }


#     if ( wantarray )
#     {
# 	return $edt->{warnings}->@*,
# 	    grep { $_->code !~ /^[EC]/ }
# 	    map { ($_->{errors } ? $_->{errors}->@* : ()), ($_->{warnings} ? $_->{warnings}->@* : ()) }
# 	    grep { $_->status ne 'skipped' } $edt->{action_list}->@*;
#     }

#     else
#     {
# 	return $edt->{warning_count};
#     }
# }


# has_condition ( code... )
#
# Return true if any of the specified codes have been attached to the current transaction.

sub has_condition {
    
    my $edt = shift;

    # Return true if any of the following codes are found.

    return any { 1; $edt->{condition_code}{$_}; } @_;
}


sub has_condition_code {

    goto &has_condition;
}


# _remove_conditions ( )
# 
# This routine is designed to be called from 'abort_action', to remove any errors or warnings that have
# been accumulated for the record.

# sub _remove_conditions {
    
#     my ($edt, $action, $selector) = @_;
    
#     my $condition_count;
    
#     if ( $selector && $selector eq 'errors' )
#     {
# 	$condition_count = $action->errors;
#     }
    
#     elsif ( $selector && $selector eq 'warnings' )
#     {
# 	$condition_count = $action->has_warnings;
#     }
    
#     else
#     {
# 	croak "you must specify either 'errors' or 'warnings' as the second parameter";
#     }
    
#     my $removed_count = 0;
    
#     # Start at the end of the list, removing any errors that are associated with this action.
    
#     while ( @{$edt->{$selector}} && $edt->{$selector}[-1][0] && $edt->{$selector}[-1][0] == $action )
#     {
# 	pop @{$edt->{$selector}};
# 	$removed_count++;
#     }

#     # If our count of conditions to be removed doesn't match the number removed, and there are
#     # more errors not yet looked at, scan the list from back to front and remove them. This should
#     # never actually happen, unless $action->has_whichever returns an incorrect value.
    
#     if ( $condition_count && $condition_count > $removed_count && @{$edt->{$selector}} > 1 )
#     {
# 	my $orig_count = scalar(@{$edt->{$selector}});
	
# 	foreach ( 2..$orig_count )
# 	{
# 	    my $i = $orig_count - $_;
	    
# 	    if ( $edt->{$selector}[$i][0] && $edt->{$selector}[$i][0] == $action )
# 	    {
# 		splice(@{$edt->{$selector}}, $i, 1);
# 		$removed_count++;
# 	    }
# 	}
#     }
    
#     return $removed_count;
# }


# # error_strings ( )
# #
# # Return a list of error strings that can be printed out, returned in a JSON data structure, or
# # otherwise displayed to the end user.

# sub error_strings {
    
#     my ($edt) = @_;
    
#     return $edt->_generate_strings(@{$edt->{errors}});
# }


# sub warning_strings {

#     my ($edt) = @_;

#     return $edt->_generate_strings(@{$edt->{demoted}}, @{$edt->{warnings}});
# }


# sub _generate_strings {

#     my $edt = shift;
    
#     my %message;
#     my @messages;
    
#     foreach my $e ( @_ )
#     {
# 	my $str = $e->code . ': ' . $edt->generate_msg($e);
	
# 	push @messages, $str unless $message{$str};
# 	push @{$message{$str}}, $e->label;
#     }
    
#     my @strings;
    
#     foreach my $m ( @messages )
#     {
# 	my $nolabel;
# 	my %label;
# 	my @labels;
	
# 	foreach my $l ( @{$message{$m}} )
# 	{
# 	    if ( defined $l && $l ne '' ) { push @labels, $l unless $label{$l}; $label{$l} = 1; }
# 	    else { $nolabel = 1; }
# 	}
	
# 	if ( $nolabel )
# 	{
# 	    push @strings, $m;
# 	}
	
# 	if ( @labels > 3 )
# 	{
# 	    my $count = scalar(@labels) - 2;
# 	    my $list = " ($labels[0], $labels[1], and $count more):";
# 	    push @strings, $m =~ s/:/$list/r;
# 	}

# 	elsif ( @labels )
# 	{
# 	    my $list = ' (' . join(', ', @labels) . '):';
# 	    push @strings, $m =~ s/:/$list/r;
# 	}
#     }

#     return @strings;
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


# Actions
# -------
# 
# Every operation carried out on the database as part of this transaction is represented by an
# action object. This object stores the parameters used to generate the SQL command, and also
# keeps track of the result and of any errors or warnings that were generated.
# 
# A typical action inserts, deletes, or updates a single database record, as specified by the
# parameters contained in an input record given as a hashref. The routines in this section are
# responsible for generating and querying action objects.
# 
# Each action may be given a label, which may be subsequently used as a reference to refer to the
# action later. Action references are always prefixed with '@'. All actions labeled or unlabeled
# may also be referenced as '@#n', where n=1 for the first action and increments sequentially.
#
# Each action acts on a particular database table, though it is possible that the SQL command(s)
# responsible for executing it may reference other tables as well. The possible operations are:
#
#   insert            Insert a new record into the specified database table.
#
#   update            Update an existing record in the specified database table.
#
#   update_matching   Update all records in the specified database table that match a
#                     specified SQL expression. *NOT IMPLEMENTED YET*
#
#   replace           Replace an existing record in the specified database table with
#                     a new one that has the same primary key.
#
#   delete            Delete an existing record from the specified database table. For
#                     this operation only, the record may be a primary key value instead
#                     of a hashref.
#
#   delete_matching   Delete all records in the specified database table that match a
#                     specified SQL expression. *NOT IMPLEMENTED YET*
#
#   delete_cleanup    Delete all records in the specified database table that match a
#                     specified SQL expression, except for those records that were
#                     inserted, replaced, or updated during the current transaction.
#                     This can be used to replace an entire set of records with the
#                     ones specified in this transaction.
#
# The table must be specified as a table reference from the Tables module rather than as an actual
# table name.


# _new_action ( table, operation, record )
# 
# Generate a new action object that will act on the specified table using the specified operation
# and the parameters contained in the specified input record. The record which must be a hashref,
# unless the operation is 'delete' in which case it may be a single primary key value or a
# comma-separated list.

sub _new_action {
    
    my ($edt, $table, $operation, $record) = @_;
    
    my ($label, @refs);
    
    croak "no parameters were specified" unless ref $record eq 'HASH' ||
	$operation =~ /delete|other/ && defined $record && $record ne '';
    
    # If this transaction has already finished, throw an exception. Client code should never try
    # to execute operations on a transaction that has already committed or been rolled back.
    
    croak "transaction has completed" if $edt->{transaction} && $edt->has_finished;
    
    # Increment the action sequence number.
    
    $edt->{action_count}++;
    
    # If _label is found among the input parameters with a nonempty value, use it to label the
    # action. Register both this and the sequence number as references for this action.
    
    if ( ref $record && defined $record->{_label} && $record->{_label} ne '' )
    {
	$label = $record->{_label};
	push @refs, '@#' . $edt->{action_count};
 	push @refs, '@' . $label;
    }
    
    # Otherwise, just use the sequence number.
    
    else
    {
	$label = '#' . $edt->{action_count};
	push @refs, '@' . $label;
    }
    
    # Unless this action is to be skipped, check that it refers to a known database table.
    
    unless ( $operation eq 'skip' )
    {
	croak "unknown table '$table'" unless exists $TABLE{$table};
	$edt->{tables}{$table} = 1;
    }
    
    # Create a new action object, and add it to the action list. Store its reference(s) in
    # the action_ref hash, then make it the current action. The object reference will be
    # implicitly returned from this method.
    
    my $action = EditTransaction::Action->new($table, $operation, $record, $label);
    
    push $edt->{action_list}->@*, $action;
    
    foreach my $k (@refs)
    {
	$edt->{action_ref}{$k} = $action;
	weaken $edt->{action_ref}{$k};
    }
    
    $edt->{current_action} = $action;
}


# action_status ( ref )
#
# If the given action reference is defined for this transaction, return the action's status.
# Otherwise, return undefined. This method allows interface code to check whether a given
# action can be or has been executed. It can also be used to verify that an action reference is
# valid before passing it to some other method. If no reference is given, the status of the
# current action is returned.
#
# The status codes as are follows:
#
#   pending      This action has not yet been executed.
#
#   executed     This action has been executed successfully.
#
#   failed       This action could not be executed. In that case, the error condition(s)
#                may be retrieved using the 'conditions' method.
#
#   skipped      This action was not handled at all.
#
#   aborted      This action was not executed because the transaction had fatal errors.

sub action_status {
    
    my ($edt, $ref) = @_;
    
    my $action = ($ref ? $edt->{action_ref}{$ref} : $edt->{current_action}) || return;
    
    my $status = $action->status;
    
    # If the transaction has fatal errors, the action status will be 'aborted' if the action was
    # not failed or skipped.
    
    if ( $edt->has_errors && $status !~ /^failed|^skipped/ )
    {
	return 'aborted';
    }
    
    # An empty action status is returned as 'pending'.
    
    else
    {
	return $status || 'pending';
    }
}


# actions ( selector )
#
# Return a list of records representing completed or pending actions. These are generated from the
# input record together with the action status. Inserted records will have the primary key
# added. Accepted values for selector are:
# 
# all          Return the entire action list [default]
# completed    Return completed actions
# pending      Return all actions that have been registered but not completed
# executed     Return all actions that were executed successfully
# notex        Return all actions that were skipped, aborted, or failed
# failed       Return all actions that failed
# aborted      Return all actions that were aborted
# skipped      Return all actions that were skipped

our (%STATUS_SELECTOR) = (all => 1, completed => 1, pending => 1,
			  executed => 1, notex => 1, failed => 1, skipped => 1);

our (%STATUS_LABEL) = (insert => 'inserted', update => 'updated',
		       replace => 'replaced', delete => 'deleted');

sub actions {
    
    my ($edt, $selector, $table) = @_;
    
    $selector ||= 'all';
    croak "unknown selector '$selector'" unless $STATUS_SELECTOR{$selector};

    if ( $table )
    {
	return unless $edt->{tables}{$table};
    }
    
    return map { $edt->_action_filter($_, $selector, $table) } $edt->{action_list}->@*;
}


sub _action_filter {

    my ($edt, $action, $selector, $table) = @_;
    
    my $status = $action->status;
    my $operation = $action->operation;
    my $result;

    if ( $edt->has_errors && $status !~ /^failed|^skipped/ )
    {
	$status = 'aborted';
    }
    
    # Return the empty list if this action does not match the selector or the table.
    
    if ( $selector eq 'all' )
    {
	return if $table && $table ne $action->table;
    }
    
    elsif ( $selector eq 'completed' && ! $status ||
	    $selector eq 'pending' && $status ||
	    $selector eq 'executed' && $status ne 'executed' ||
	    $selector eq 'notex' && $status !~ /^failed|^skipped|^aborted/ ||
	    $selector eq 'failed' && $status ne 'failed' ||
	    $selector eq 'skipped' && $status ne 'skipped' ||
	    $table && $table ne $action->table )
    {
	return;
    }
    
    # If we have the original input record, start with that.
    
    elsif ( my $orig = $action->record )
    {
	$result = { $orig->%* };
    }
    
    # If no original record exists, try to create one.
    
    elsif ( my $keycol = $action->keycol )
    {
	$result = { $keycol => $action->keyval };
    }
    
    else
    {
	$result = { };
    }
    
    # Add the label if it is not already there. Then add the action status and table.
    
    unless ( $result->{_label} )
    {
	if ( my $label = $action->label )
	{
	    $result->{_label} = $label;
	}
    }
    
    if ( $status eq 'executed' )
    {
	$result->{_status} = $STATUS_LABEL{$operation} || 'executed';
    }

    else
    {
	$result->{_status} = $status;
    }
    
    $result->{_table} = $action->table;
    
    # If this action has conditions attached, add them now.
    
    if ( $action->errors )
    {
	my @errors = map { $edt->_condition_simple($_) } $action->errors;
	$result->{_errors} = \@errors;
    }
    
    if ( $action->warnings )
    {
	my @warnings = map { $edt->_condition_simple($_) } $action->warnings;
	$result->{_warnings} = \@warnings;
    }

    return $result;
}


# Transaction control
# -------------------

# start_transaction ( )
# 
# Start the database transaction associated with this EditTransaction object, if it has not
# already been started. This is done automatically when 'execute' is called, but can be done
# explicitly at an earlier stage if the checking of record values needs to be done inside a
# transaction. Returns true if the transaction is can proceed, false otherwise.

sub start_transaction {
    
    my ($edt) = @_;
    
    # If this transaction has already committed, throw an exception.
    
    if ( $edt->has_committed )
    {
	croak "this transaction has already committed";
    }
    
    # If we have not started the transaction yet and no errors have occurred, then start it
    # now. Preserve the current action.
    
    elsif ( $edt->can_proceed && ! $edt->has_started )
    {
	my $save = $edt->{current_action};
	
	$edt->_start_transaction;
	
	$edt->{current_action} = $save;
    }
    
    return $edt->can_proceed;
}


# start_execution ( )
# 
# Start the database transaction associated with this EditTransaction object, if it has not
# already been started. Then immediately execute every pending action, and set the
# 'execute_immediately' flag. This means that any subsequently specified actions will be executed
# immediately instead of waiting for 'execute' or 'commit' to be called. Return true if the
# transaction can proceed, false otherwise.

sub start_execution {
    
    my ($edt) = @_;
    
    # If the transaction can proceed but has not yet been started, start it now. Then set the
    # 'execute_immediately' flag and execute all pending actions.
    
    if ( $edt->can_proceed )
    {
	$edt->_start_transaction unless $edt->has_started;
	$edt->{execute_immediately} = 1;
	$edt->_execute_action_list;
    }
    
    return $edt->can_proceed;
}


# execute ( )
# 
# Start a database transaction, if one has not already been started. Then execute all of the
# pending actions. Returns a true value if the transaction can proceed, false otherwise. This
# differs from 'start_execution' only in that the execute_immediately flag is not set.

sub execute {
    
    my ($edt) = @_;
    
    # If this transaction can proceed, start the database transaction if it hasn't already been
    # started. Then run through the action list and execute any actions that are pending.
    
    if ( $edt->can_proceed )
    {
	$edt->_start_transaction unless $edt->has_started;
	$edt->_execute_action_list;
    }

    return $edt->can_proceed;
}


# commit ( )
# 
# Execute all pending actions and commit the transaction. Return true if the commit was
# successful, false otherwise.

sub commit {
    
    my ($edt) = @_;

    # If this transaction can proceed, start the database transaction if it hasn't already been
    # started. Then run through the action list and execute any actions that are pending.

    if ( $edt->can_proceed )
    {
	$edt->_start_transaction unless $edt->has_started;
	$edt->_execute_action_list;
    }    
    
    $edt->{current_action} = undef;
    
    # If any fatal errors have accumulated, call the 'cleanup_transaction' method.  Otherwise,
    # call the 'finalize_transaction' method. These do nothing by default, and are designed to be
    # overridden by subclasses.
    
    my $culprit;
    
    eval {
	
	if ( $edt->has_errors )
	{
	    $culprit = 'cleaned up';
	    $edt->cleanup_transaction($edt->{main_table});
	}
	
	else
	{
	    $culprit = 'finalized';
	    $edt->finalize_transaction($edt->{main_table});
	}
    };
    
    # If an exception is thrown during either of these method calls, write it to the error stream
    # and add an error condition.
    
    if ( $@ )
    {
	$edt->error_line($@);
	$edt->add_condition('main', 'E_EXECUTE', "an exception occurred while the transaction was $culprit");
    }
    
    # If there are any errors at this point, roll back the transaction and return false.
    
    if ( $edt->has_errors )
    {
	$edt->_rollback_transaction('errors') if $edt->has_started;
	return 0;
    }
    
    # Otherwise, commit the transaction.
    
    else
    {
	return $edt->_commit_transaction;
    }
}


# rollback ( )
#
# If this EditTransaction has not yet been completed, roll back whatever work has been done. If
# the database transaction has not yet been started, just mark the transaction 'aborted' and
# return. Return true if the transaction was either rolled back or never carried out, false
# otherwise.

sub rollback {

    my ($edt) = @_;
    
    # If the transaction has finished, return true if it was rolled back or aborted, false
    # otherwise.
    
    if ( $edt->has_finished )
    {
	return ! $edt->has_committed;
    }

    # If the transaction has started, then roll it back.
    
    elsif ( $edt->has_started )
    {
	$edt->_rollback_transaction('call');
	return $edt->{transaction} eq 'aborted';
    }
    
    # Otherwise, preemptively set the status to 'aborted' and return true. This will prevent
    # anything else from being done with it.
    
    else
    {
	$edt->{transaction} = 'aborted';
	return 1;
    }
}


# _start_transaction ( )
#
# Start a new database transaction. If one is already active on this database connection, assume
# that something has gone wrong and issue a rollback first.

sub _start_transaction {

    my ($edt) = @_;
    
    my $result;

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
    
    # Clear the current action. If we are in debug mode, print a line to the debugging stream
    # announcing the start of the transaction.
    
    $edt->{current_action} = undef;
    
    $edt->debug_line(" >>> START TRANSACTION $edt->{unique_id}\n") if $edt->{debug_mode};
    
    # Start a new database transaction. If there is an uncommitted transaction on this database
    # connection that was not initiated by this module, it will be implicitly committed. If there
    # was an uncommitted transaction that was initiated by this module, it was rolled back
    # earlier in this subroutine.
    
    eval {
	$dbh->do("START TRANSACTION");
	
	# Store a reference to this transaction, so that the interlock code above can
	# function properly. The reference must be a weak one, so the transaction will be
	# automatically rolled back through the DESTROY method if all other references to
	# it are destroyed.
	
	$TRANSACTION_INTERLOCK{$dbh} = $edt;
	weaken $TRANSACTION_INTERLOCK{$dbh};
	
	# Update the status of this transaction to 'active', and then call the
	# 'initialize_transaction' method. This is designed to be overridden by designed
	# to be overridden by subclasses. The default method does nothing.
	
	$edt->{transaction} = 'active';
	
	$edt->initialize_transaction($edt->{main_table});
    };
    
    # If an exception was thrown, add an error condition. Write the actual error message to the
    # error stream.
    
    if ( $@ )
    {
	$edt->error_line($@);
	
	my $word = $edt->{transaction} eq 'active' ? 'initializing' : 'starting';
	$edt->add_condition('main', 'E_EXECUTE', "an exception occurred while $word the transaction");

	# Mark this transaction as 'aborted'. If a database transaction was actually started,
	# roll it back.
	
	$edt->{transaction} = 'aborted';
	
	my ($in_transaction) = $dbh->selectrow_array('SELECT @@in_transaction');
	
	if ( $in_transaction )
	{
	    $edt->_rollback_transaction('errors');
	}

	return 0;
    }

    else
    {
	return 1;
    }
}


# _commit_transaction ( )
#
# Commit the current transaction.

sub _commit_transaction {
    
    my ($edt) = @_;
    
    $edt->{current_action} = undef;

    my $dbh = $edt->dbh;
    
    # If debug mode is on, print a line to the debugging straem announcing the commit.
    
    $edt->debug_line( " <<< COMMIT TRANSACTION $edt->{unique_id}\n" ) if $edt->{debug_mode};
    
    # Tell the database to commit the transaction.
    
    eval {
	$TRANSACTION_INTERLOCK{$dbh} = undef;
	$dbh->do("COMMIT");
	
	$edt->{transaction} = 'committed';
	$edt->{commit_count} = 0 unless looks_like_number $edt->{commit_count};
	$edt->{commit_count}++;
    };
    
    # If an exception was thrown by the COMMIT statement, try rolling back the transaction. Print
    # the exception to the error stream, add an error condition, and set the transaction status to
    # 'aborted'. If debug mode is on, print an additional message to the debugging stream
    # announcing the rollback.
    
    if ( $@ && $edt->{transaction} ne 'committed')
    {
	$edt->error_line($@);
	$edt->debug_line( " <<< ROLLBACK TRANSACTION $edt->{unique_id} FROM exception\n" );

	$dbh->do("ROLLBACK");
	$edt->add_condition('E_EXECUTE', 'an exception occurred while committing the transaction');

	eval {
	    $edt->{transaction} = 'aborted';
	    $edt->{rollback_count} = 0 unless looks_like_number $edt->{rollback_count};
	    $edt->{rollback_count}++;
	};

	return 0;
    }

    else
    {
	return 1;
    }
}


# _rollback_transaction ( reason )
#
# Roll back the current transaction.

sub _rollback_transaction {
    
    my ($edt, $reason) = @_;
    
    $edt->{current_action} =  undef;
    
    my $dbh = $edt->dbh;
    
    # If debug mode is on, print a message to the debugging stream announcing the rollback. Include
    # the reason given by the argument.
    
    if ( $edt->{debug_mode} )
    {
	my $msg = $reason ? ' FROM ' . uc($reason) : '';
	$edt->debug_line( " <<< ROLLBACK TRANSACTION $edt->{unique_id}$msg\n" );
    }
    
    # Tell the database to roll back the transaction.
    
    eval {
	$TRANSACTION_INTERLOCK{$dbh} = undef;
	$dbh->do("ROLLBACK");
	
	$edt->{transaction} = 'aborted';
	$edt->{rollback_count} = 0 unless looks_like_number $edt->{rollback_count};
	$edt->{rollback_count}++;
    };
    
    # If an exception was thrown by the ROLLBACK statement, print the exception to the error
    # stream and add an error condition.
    
    if ( $@ && $edt->{transaction} ne 'aborted' )
    {
	$edt->error_line($@);
	$edt->add_condition('E_EXECUTE', 'an exception occurred while rolling back the transaction');
    }
    
    return 1;
}


# Record operations
# -----------------

# The methods in this section are called by interface methods to carry out actions such as
# inserting, updating, and deleting records in the database. Each one returns true if the
# operation succeeds or is validated and queued for later execution, false otherwise.


# insert_record ( table, record )
# 
# The specified record is to be inserted into the specified table. Depending on the settings of
# this particular EditTransaction, this action may happen immediately or may be executed
# later. The record in question MUST NOT include a primary key value.

sub insert_record {
    
    my ($edt, $table, $record) = @_;
    
    # Create a new action object to represent this insertion.
    
    my $action = $edt->_new_action($table, 'insert', $record);
    
    # If the record includes any errors or warnings, import them.
    
    $edt->import_conditions($action, $record) if $record->{_errwarn};
    
    # We can only create records if specifically allowed. This may be specified by the user as a
    # parameter to the operation being executed, or it may be set by the operation method itself
    # if the operation is specifically designed to create records.
    
    if ( $edt->allows('CREATE') )
    {
	eval {
	    # First check to make sure we have permission to insert a record into this table. A
	    # subclass may override this method, if it needs to make different checks than the default
	    # ones.
	    
	    my $permission = $edt->authorize_action($action, 'insert', $table);
	    
	    # If the user does not have permission to add a record, add an error condition unless
	    # one was added by authorize_action or unless the authorization will be carried out
	    # later.
	    
	    if ( $permission !~ /post|admin|later|error/ )
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
	};
	
	# If a exception occurred, write the exception to the error stream and add an error
	# condition to this action.
	
	if ( $@ )
        {
	    $edt->error_line($@);
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	}
    }
    
    # If an attempt is made to add a record without the 'CREATE' allowance, add the appropriate
    # caution.
    
    else
    {
	$edt->add_condition($action, 'C_CREATE');
    }
    
    # Either execute the action immediately or add it to the appropriate list depending on whether
    # or not any error conditions are found.
    
    return $edt->_handle_action($action, 'insert');
}


# update_record ( table, record )
# 
# The specified record is to be updated in the specified table. Depending on the settings of this
# particular EditTransaction, this action may happen immediately or may be executed later. The
# record in question MUST include a primary key value, indicating which record to update.

sub update_record {
    
    my ($edt, $table, $record) = @_;
    
    # Create a new action object to represent this update.
    
    my $action = $edt->_new_action($table, 'update', $record);
    
    # If the record includes any errors or warnings, import them.
    
    $edt->import_conditions($action, $record) if $record->{_errwarn};
    
    # We can only update a record if a primary key value is specified.
    
    if ( my $keyexpr = $edt->_set_keyexpr($action) )
    {
	eval {
	    # First check to make sure we have permission to update this record. A subclass may
	    # override this method, if it needs to make different checks than the default ones.
	    
	    my $permission = $edt->authorize_action($action, 'update', $table, $keyexpr);
	    
	    # If no such record is found in the database, add an E_NOT_FOUND condition. If this
	    # EditTransaction has been created with the 'PROCEED' or 'NOT_FOUND' allowance,
	    # it will automatically be turned into a warning and will not cause the transaction to
	    # be aborted.
	    
	    if ( $permission eq 'notfound' )
	    {
		$edt->add_condition($action, 'E_NOT_FOUND');
	    }
	    
	    # If the record has been found but is locked, then add an E_LOCKED condition. The user
	    # would have had permission to update this record, except for the lock.
	    
	    elsif ( $permission =~ /locked/ )
	    {
		$edt->add_condition($action, 'E_LOCKED', $action->keyval);
	    }
	    
	    # If the record can be unlocked by the user, then add a C_LOCKED condition UNLESS the
	    # transaction allows 'LOCKED'. In that case, we can proceed. A permission
	    # of 'unlock' means that the user does have permission to update the record if the
	    # lock is disregarded.
	    
	    elsif ( $permission =~ /,unlock/ )
	    {
		if ( $edt->allows('LOCKED') )
		{
		    $permission =~ s/,unlock//;
		    $action->_set_permission($permission);
		}
		
		else
		{
		    $edt->add_condition($action, 'C_LOCKED', $action->keyval);
		}
	    }
	    
	    # If the user does not have permission to edit the record, add an error condition unless
	    # one was added by authorize_action or unless the authorization will be carried out
	    # later.
	    
	    elsif ( $permission !~ /edit|admin|later|error/ )
	    {
		$edt->add_condition($action, 'E_PERM', 'update');
	    }
	    
	    # Then check the new record values, to make sure that the column values meet all of the
	    # criteria for this table. If any error or warning conditions are detected, they are added
	    # to the current transaction. A subclass may override this method, if it needs to make
	    # additional checks or perform additional work.
	    
	    $edt->validate_action($action, 'update', $table, $keyexpr);
	    $edt->validate_against_schema($action, 'update', $table);
	};
	
	# If a exception occurred, write the exception to the error stream and add an error
	# condition to this action.
	
	if ( $@ )
	{
	    $edt->error_line($@);
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	}
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
    
    return $edt->_handle_action($action, 'update');
}


# update_many ( table, selector, record )
# 
# All records matching the selector are to be updated in the specified table. Depending on the
# settings of this particular EditTransaction, this action may happen immediately or may be
# executed later. The selector may indicate a set of keys, or it may include some expression that
# selects all matching records.

sub update_many {
    
    my ($edt, $table, $keyexpr, $record) = @_;

    # Create a new action object to represent this update.
    
    my $action = $edt->_new_action($table, 'update_many', $record);
    
    # If the record includes any errors or warnings, import them.
    
    $edt->import_conditions($action, $record) if $record->{_errwarn};
    
    # As a failsafe, we only accept an empty selector or a universal selector if the key
    # '_universal' appears in the record with a true value.
    
    if ( ( defined $keyexpr && $keyexpr ne '' && $keyexpr ne '1' ) || $record->{_universal} )
    {
	$action->_set_keyexpr($keyexpr);
	
	eval {
	    # First check to make sure we have permission to update records in this table. A
	    # subclass may override this method, if it needs to make different checks than the
	    # default ones.
	    
	    my $permission = $edt->authorize_action($action, 'update_many', $table);
	    
	    # If the user does not have admin permission on the table, add an error condition. 
	    
	    if ( $permission ne 'admin' )
	    {
		$edt->add_condition($action, 'E_PERM', 'update_many');
	    }
	    
	    # The update record must not include a key value.
	    
	    if ( $action->keyval )
	    {
		$edt->add_condition($action, 'E_HAS_KEY', 'update_many');
	    }
	};
	
	# If a exception occurred, write the exception to the error stream and add an error
	# condition to this action.
	
	if ( $@ )
	{
	    $edt->error_line($@);
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	}
    }

    # If a valid selector was not given, add an error condition.

    else
    {
	$edt->add_condition($action, 'E_PARAM', "invalid selection expression '$keyexpr'");
    }
    
    # Either execute the action immediately or add it to the appropriate list depending on whether
    # or not any error conditions are found.
    
    return $edt->_handle_action($action, 'update_many');
}


# replace_record ( table, record )
# 
# The specified record is to be inserted into the specified table, replacing any record that may
# exist with the same primary key value. Depending on the settings of this particular EditTransaction,
# this action may happen immediately or may be executed later. The record in question MUST include
# a primary key value.

sub replace_record {
    
    my ($edt, $table, $record) = @_;
    
    # Create a new action object to represent this replacement.
    
    my $action = $edt->_new_action($table, 'replace', $record);
    
    # If the record includes any errors or warnings, import them.
    
    $edt->import_conditions($action, $record) if $record->{_errwarn};
    
    # We can only replace a record if a primary key value is specified.
    
    if ( my $keyexpr = $edt->_set_keyexpr($action) )
    {
	eval {
	    
	    # First check to make sure we have permission to replace this record. A subclass may
	    # override this method, if it needs to make different checks than the default ones.
	    
	    my $permission = $edt->authorize_action($action, 'replace', $table, $keyexpr);
	    
	    # If no such record is found in the database, check to see if this EditTransaction
	    # allows CREATE. If this is the case, and if the user also has 'admin' permission on
	    # this table, or if the user has 'post' and the table property ADMIN_INSERT_KEY is NOT
	    # set, then a new record will be created with the specified primary key
	    # value. Otherwise, an appropriate error condition will be added.
	    
	    if ( $permission eq 'notfound' )
	    {
		if ( $edt->{allows}{CREATE} )
		{
		    $permission = $edt->check_table_permission($table, 'insert_key');
		    
		    if ( $permission eq 'admin' || $permission eq 'insert_key' )
		    {
			$action->_set_permission($permission);
			$action->{_no_modifier} = 1;
		    }
		    
		    # If the NOT_FOUND allowance is present, we report this as E_NOT_FOUND instead
		    # of E_PERM.
		    
		    elsif ( $edt->{allows}{NOT_FOUND} )
		    {
			$edt->add_condition($action, 'E_NOT_FOUND');
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
		    $edt->add_condition($action, 'C_CREATE');
		}
	    }
	    
	    # If the record has been found but is locked, then add an E_LOCKED condition. The user
	    # would have had permission to replace this record, except for the lock.
	    
	    elsif ( $permission eq 'locked' )
	    {
		$edt->add_condition($action, 'E_LOCKED', $action->keyval);
	    }
	    
	    # If the record can be unlocked by the user, then add a C_LOCKED condition UNLESS the
	    # transaction allows 'LOCKED'. In this case, we can proceed. A permission of 'unlock'
	    # means that the user does have permission to update the record if the lock is
	    # disregarded.
	    
	    elsif ( $permission =~ /,unlock/ )
	    {
		if ( $edt->allows('LOCKED') )
		{
		    $permission =~ s/,unlock//;
		    $action->_set_permission($permission);
		}
		
		else
		{
		    $edt->add_condition($action, 'C_LOCKED', $action->keyval);
		}
	    }
	    
	    # If the user does not have permission to edit the record, add an error condition unless
	    # one was added by authorize_action or unless the authorization will be carried out
	    # later.
	    
	    elsif ( $permission !~ /edit|admin||later|error/ )
	    {
		$edt->add_condition($action, 'E_PERM', 'replace_existing');
	    }
	    
	    # Then check the new record values, to make sure that the replacement record meets all of
	    # the criteria for this table. If any error or warning conditions are detected, they are
	    # added to the current transaction. A subclass may override this method, if it needs to
	    # make additional checks or perform additional work.
	    
	    $edt->validate_action($action, 'replace', $table, $keyexpr);
	    $edt->validate_against_schema($action, 'replace', $table);
	};
	
	# If a exception occurred, write the exception to the error stream and add an error
	# condition to this action.
	
	if ( $@ )
        {
	    $edt->error_line($@);
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	}
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
    
    return $edt->_handle_action($action, 'replace');
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
    
    # Create a new action object to represent this deletion.
    
    my $action = $edt->_new_action($table, 'delete', $record);
    
    # If the record includes any errors or warnings, import them.
    
    $edt->import_conditions($action, $record) if $record && ref $record eq 'HASH' && $record->{_errwarn};
    
    # A record can only be deleted if a primary key value is specified.
    
    if ( my $keyexpr = $edt->_set_keyexpr($action) )
    {
	eval {
	    
	    # First check to make sure we have permission to delete this record. A subclass may
	    # override this method, if it needs to make different checks than the default ones.
	    
	    my $permission = $edt->authorize_action($action, 'delete', $table, $keyexpr);
	    
	    # If no such record is found in the database, add an error condition. If this
	    # EditTransaction has been created with the 'PROCEED' or 'NOT_FOUND' allowance, it
	    # will automatically be turned into a warning and will not cause the transaction to be
	    # aborted.
	    
	    if ( $permission eq 'notfound' )
	    {
		$edt->add_condition($action, 'E_NOT_FOUND');
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
	    
	    # If the user does not have permission to delete the record, add an error condition
	    # unless one was added by authorize_action.
	    
	    elsif ( $permission !~ /delete|admin|error/ )
	    {
		$edt->add_condition($action, 'E_PERM', 'delete');
	    }
	    
	    # If a 'validate_delete' method was specified, then call it. This method may abort the
	    # deletion by adding an error condition. Otherwise, we assume that the permission check we
	    # have already done is all that is necessary.
	    
	    $edt->validate_action($action, 'delete', $table, $keyexpr);
	};
	
	if ( $@ )
	{
	    $edt->error_line($@);
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	}
    }
    
    # If no primary key was specified, add an error condition.
    
    else
    {
	$edt->add_condition($action, 'E_NO_KEY', 'delete');
    }
    
    # Create an action record, and then take the appropriate action.
    
    return $edt->_handle_action($action, 'delete');
}


# delete_many ( table, selector )
# 
# All records matching the selector are to be deleted in the specified table. Depending on the
# settings of this particular EditTransaction, this action may happen immediately or may be
# executed later. The selector may indicate a set of keys, or it may include some expression that
# selects all matching records.

sub delete_many {
    
    my ($edt, $table, $record) = @_;
    
    # Extract the selector from the specified record.
    
    my $selector = $record->{_where} // $record->{where};
    my $failsafe = $record->{_universal} // $record->{universal};
    
    croak "you must specify a nonempty selector using the key 'where' or '_where'"
	unless $selector && $selector ne '';
    
    # As a failsafe, we only accept a universal selector if the key '_universal' or 'universal' is
    # given a true value.
    
    unless ( $failsafe || $selector ne '1' )
    {
	croak "selector '$selector' is not valid unless _universal is set to a true value";
    }
    
    # Create a new action object to represent this deletion.
    
    my $action = $edt->_new_action($table, 'delete_many');
    
    if ( $selector )
    {
	$action->_set_keyexpr($selector);
	
	eval {
	    
	    # First check to make sure we have permission to delete records in this table. A
	    # subclass may override this method, if it needs to make different checks than the
	    # default ones.
	    
	    my $permission = $edt->authorize_action($action, 'delete_many', $table);
	    
	    # If errors occurred during authorization, then we need not check further but can
	    # proceed to the next action.
	    
	    if ( $permission eq 'error' && $action->errors )
	    {
		return $edt->_handle_action($action, 'delete_many');
	    }
	    
	    # If the user does not have administrative permission on the table, add an error
	    # condition.
	    
	    elsif ( $permission ne 'admin' )
	    {
		$edt->add_condition($action, 'E_PERM', 'delete_many');
	    }
	};
	
	if ( $@ )
	{
	    $edt->error_line($@);
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	}
    }

    # If a valid selector was not given, add an error condition.

    else
    {
	$edt->add_condition($action, 'E_PARAM', "invalid expression '$selector'");
    }
    
    # Either execute the action immediately or add it to the appropriate list depending on whether
    # or not any error conditions are found.
    
    return $edt->_handle_action($action, 'delete_many');
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
    
    my ($edt, $table, $keyexpr) = @_;
    
    # Create a new action object to represent this operation.
    
    my $action = $edt->_new_action($table, 'delete_cleanup', { });
    
    # Make sure we have a non-empty selector, although we will need to do more checks on it later.
    
    unless ( $keyexpr && ! ref $keyexpr ) # $$$ this needs to be fixed
    {
	croak "'$keyexpr' is not a valid selection expression";
    }

    	# if ( $keyexpr =~ qr{ ^ \s* (\w+) \s+ in \s* [(] (.*) [)] \s* $ }xs && $1 eq $supcol )
	# {
	#     $keyexpr = $2;
	# }
	
	# if ( $keyexpr =~ qr{ ^ [0-9,\s]+ $ }xs )
	# {
	#     @linkval = grep { $_ } split /[,\s]+/, $keyexpr;
	# }

	# elsif ( ref $keyexpr eq 'ARRAY' )
	# {
	#     @linkval = grep { $_ } $keyexpr->@*;
	# }

	# else
	# {
	#     croak "invalid key expression '$keyexp'";
	# }

    
    $action->_set_keyexpr($keyexpr);
    
    eval {
	# First check to make sure we have permission to delete records in this table. A subclass
	# may override this method, if it needs to make different checks than the default ones.
	
	my $permission = $edt->authorize_action($action, 'delete_cleanup', $table, $keyexpr);
	
	# If errors occurred during authorization, then we need not check further but can
	# proceed to the next action.
	
	if ( $permission eq 'error' && $action->errors )
	{
	    return; # exit the eval
	}
	
	# If the user does not have permission to edit the record, add an E_PERM condition. 
	
	elsif ( $permission !~ /delete|admin|error/ )
	{
	    $edt->add_condition($action, 'E_PERM', 'delete_cleanup');
	}
    };
    
    if ( $@ )
    {
	$edt->error_line($@);
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
    }
    
    # If the selector does not mention the linking column, throw an exception rather than adding
    # an error condition. These are static errors that should not be occurring because of bad
    # end-user input.
    
    my $linkcol = $action->linkcol;
    
    unless ( $linkcol )
    {
	croak "could not find linking column for table $table";
    }
    
    unless ( $keyexpr =~ /\b$linkcol\b/ )
    {
	croak "'$keyexpr' is not a valid selector, must mention $linkcol";
    }
    
    # Otherwise, either execute the action immediately or add it to the appropriate list depending on
    # whether or not any error conditions are found.
    
    return $edt->_handle_action($action, 'delete_cleanup');
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
    
    my $action = $edt->_new_action($table, 'other', $record);
    
    $action->_set_method($method);
    
    # If we have a primary key value, we can authorize against that record. This may be a
    # limitation in future, but for now the action is authorized if they have 'edit' permission
    # on that record.
    
    if ( my $keyexpr = $edt->_set_keyexpr($action) )
    {
	eval {

	    # First check to make sure we have permission to update this record. A subclass may
	    # override this method, if it needs to make different checks than the default ones.
	    
	    my $permission = $edt->authorize_action($action, 'other', $table, $keyexpr);
	    
	    # If no such record is found in the database, add an E_NOT_FOUND condition. If this
	    # EditTransaction has been created with the 'PROCEED' or 'NOT_FOUND' allowance, it
	    # will automatically be turned into a warning and will not cause the transaction to be
	    # aborted.
	    
	    if ( $permission eq 'notfound' )
	    {
		$edt->add_condition($action, 'E_NOT_FOUND', $action->keyval);
	    }

	    # If the record has been found but is locked, then add an E_LOCKED condition. The user
	    # would have had permission to update this record, except for the lock.
	    
	    elsif ( $permission eq 'locked' )
	    {
		$edt->add_condition($action, 'E_LOCKED', $action->keyval);
	    }
	    
	    # If the record can be unlocked by the user, then add a C_LOCKED condition UNLESS the
	    # transaction allows 'LOCKED'. In that case, we can proceed. A permission of 'unlock'
	    # means that the user does have permission to update the record if the lock is
	    # disregarded.
	    
	    elsif ( $permission =~ /,unlock/ )
	    {
		if ( $edt->allows('LOCKED') )
		{
		    $permission =~ s/,unlock//;
		    $action->_set_permission($permission);
		}

		else
		{
		    $edt->add_condition($action, 'C_LOCKED', $action->keyval);
		}
	    }
	    
	    # If the user does not have permission to edit the record, add an error condition
	    # unless one was added by authorize_action or unless the authorization will be carried
	    # out later.
	    
	    elsif ( $permission !~ /edit|admin|later|error/ )
	    {
		$edt->add_condition($action, 'E_PERM', $method);
	    }
	    
	    # Then check the new record values, to make sure that the column values meet all of the
	    # criteria for this table. If any error or warning conditions are detected, they are added
	    # to the current transaction. A subclass may override this method, if it needs to make
	    # additional checks or perform additional work.
	    
	    $edt->validate_action($action, 'other', $table, $keyexpr);
	    # $edt->validate_against_schema($action, 'other', $table);
	};
	
	if ( $@ )
	{
	    $edt->error_line($@);
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
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
    
    return $edt->_handle_action($action, 'other');
}


# skip_record ( )
# 
# Create a placeholder action that will never be executed. This should be called for input records
# that have been determined to be invalid before they are passed to this module.

sub skip_record {

    my ($edt, $record) = @_;
    
    # Create the placeholder action.
    
    my $action = $edt->_new_action(undef, 'skip', $record);
    
    # If the record includes any errors or warnings, import them.
    
    $edt->import_conditions($action, $record) if $record->{_errwarn};
    
    # Handle this action, which in most cases is a no-op.
    
    return $edt->_handle_action($action, 'skip');
}


# process_record ( table, record )
# 
# If the record contains the key '_operation', then call the method indicated by the key
# value. Otherwise, call either 'update_record' or 'insert_record' depending on whether or not the
# record contains a value for the table's primary key. This is a convenient shortcut for use by
# interface code.

sub process_record {
    
    my ($edt, $table, $record) = @_;
    
    # If the record contains the key _skip with a true value, add a placeholder action and do not
    # process this record.
    
    if ( $record->{_skip} )
    {
	return $edt->skip_record($record);
    }
    
    # If the record contains the key _operation with a nonempty value, call the corresponding
    # method.
    
    elsif ( $record->{_operation} )
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
	    my $action = $edt->_new_action($table, 'skip', $record);
	    $edt->add_condition($action, 'E_BAD_OPERATION', $record->{_operation});
	    return $edt->_handle_action($action, 'skip');
	}
    }
    
    # If the record contains the key _action with a nonempty value, create a special action to
    # carry out the specified operation. This willl typically be a method defined in a subclass of
    # EditTransaction.
    
    elsif ( $record->{_action} )
    {
	return $edt->other_action($table, $record->{_action}, $record);
    }
    
    # Otherwise, call update_record if the record contains a value for the table's primary
    # key. Call insert_record if it does not.
    
    elsif ( $edt->get_record_key($table, $record) )
    {
	return $edt->update_record($table, $record);
    }
    
    else
    {
	return $edt->insert_record($table, $record);
    }
}


# insert_update_record ( table, record )
#
# This is a deprecated alias for process_record.

sub insert_update_record {

    my ($edt, $table, $record) = @_;
    
    goto &process_record;
}


# do_sql ( stmt, options )
# 
# Create an action that will execute the specified SQL statement, and do nothing else. The
# execution is protected by a try block, and an E_EXECUTE condition will be added if it fails. The
# appropriate cleanup methods will be called in this case. If an options hash is provided with the
# key 'result' and a scalar reference, the result code returned by the statement execution will be
# written to that reference. If the value is a code reference, it will be called with the result
# as an argument.

sub do_sql {

    my ($edt, $sql, $options) = @_;
    
    # Substitute any table specifiers in the SQL statement for the actual table names.
    
    $sql =~ s{ << (\w+) >> }{ $TABLE{$1} || "_INVALID_<<$1>>" }xseg;
    
    # If any invalid tables were found, throw an exception.

    if ( $sql =~ qr{ _INVALID_<< (\w+) >> | <<>> }xs )
    {
	my $bad_table = $1 || '';
	croak "E_BAD_TABLE: invalid table name '$bad_table'";
    }
    
    # Otherwise, create a record containing this statement and then create a new action for it.
    
    my $record = { sql => $sql };

    if ( ref $options eq 'HASH' && $options->{store_result} )
    {
	croak "result value must be a scalar or code ref" unless
	    ref $options->{store_result} =~ /^SCALAR|^CODE/;
	
	$record->{store_result} = $options->{store_result};
	weaken $options->{store_result};
    }
    
    my $action = EditTransaction::Action->new('<SQL>', 'other', $record);
    
    $action->_set_method('_execute_sql_action');
    
    return $edt->_handle_action($action, 'other');
}


# import_conditions ( action, record )
#
# If the specified record contains either of the keys _errors or _warnings, convert the contents
# into conditions and add them to this transaction and to the specified action. But don't count
# them if the record is being skipped.

our ($CODE_PATTERN) = qr{^[CDEFW]_};

sub import_conditions {

    my ($edt, $action, $record) = @_;
    
    # Add every condition specified in _errwarn to the current transaction and the current
    # action. The value of _errwarn might be a list of lists, or a single list, or a list of
    # strings, or a single string.
    
    if ( my $arg = $record->{_errwarn} || $record->{_errors} )
    {
	my @conditions;
	
	if ( ref $arg eq 'ARRAY' && $arg->@* )
	{
	    if ( $arg->[0] && $arg->[0] =~ $CODE_PATTERN )
	    {
		if ( $arg->[1] && $arg->[1] =~ $CODE_PATTERN )
		{
		    @conditions = $arg->@*;
		}
		
		else
		{
		    @conditions = $arg;
		}
	    }

	    elsif ( ref $arg->[0] eq 'ARRAY' )
	    {
		@conditions = $arg->@*;
	    }

	    else
	    {
		@conditions = ['E_BAD_CONDITION', "unrecognized data format", ref $arg];
	    }
	}
	
	elsif ( ref $arg )
	{
	    @conditions = ['E_BAD_CONDITION', "unrecognized data format", ref $arg];
	}
	
	elsif ( $arg && $arg =~ $CODE_PATTERN )
	{
	    @conditions = $arg;
	}
	
	else
	{
	    @conditions = ['E_BAD_CONDITION', "unrecognized data format", $arg];
	}

	foreach my $c ( @conditions )
	{
	    if ( ref $c eq 'ARRAY' )
	    {
		my $code = shift $c->@*;

		if ( $code && $code =~ $CODE_PATTERN )
		{
		    $edt->_import_condition($action, $code, $c->@*);
		}

		else
		{
		    $edt->add_condition($action, 'E_BAD_CONDITION', "unrecognized code", $code);
		}
	    }

	    elsif ( $c =~ qr{ ^ ([CDEFW]_[A-Z_-]+) (?: \s* [(] .*? [)] )? (?: \s* : \s* )? (.*) }xs )
	    {
		$edt->_import_condition($action, $1, $2);
	    }
	    
	    elsif ( $c )
	    {
		$c =~ qr{ ^ (\w+) }xs;
		$edt->add_condition($action, 'E_BAD_CONDITION', "unrecognized code", $1);
	    }
	}
    }
    
    delete $record->{_errwarn};
    delete $record->{_skip};
    delete $record->{_errors};
}


sub _import_condition {

    my ($edt, $action, $code, @data) = @_;
    
    substr($code, 0, 1) =~ tr/DF/CE/;
    
    my $template = $CONDITION_BY_CLASS{ref $edt}{$code} || $CONDITION_BY_CLASS{EditTransaction}{$code};
    
    if ( ! $template )
    {
	$edt->add_condition($action, 'E_PARAM', join(' ', "$code:", @data));
    }
    
    elsif ( $template =~ /%2/ )
    {
	if ( @data == 1 && $data[0] && $data[0] =~ qr{ (['] .*? [']) .* (['] .* ['])? }xs )
	{
	    $edt->add_condition($action, $code, $1, $2);
	}
	
	else
	{
	    $edt->add_condition($action, $code, @data);
	}
    }

    elsif ( $data[0] =~ qr{ (['] .*? [']) }xs )
    {
	$edt->add_condition($action, $code, $1);
    }
    
    else
    {
	$edt->add_condition($action, $code, $data[0]);
    }
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


# abort_action ( )
# 
# This method may be called from record validation routines defined in subclasses of
# EditTransaction, if it is determined that a particular record action should be skipped but the
# rest of the transaction should proceed.

sub abort_action {
    
    my ($edt) = @_;
    
    # If the most recent action has not already been executed or abandoned, abort it
    # now and return true. Otherwise, return false.

    if ( $edt->{action_list}->@* )
    {
	my $action = $edt->{action_list}[-1];

	# If the current action has already been processed, return false.
	
	return if $action->status;
	
	# Otherwise, set the status to 'skipped'.
	
	$action->_set_status('skipped');
	
	# If this action has any errors or warnings, remove them from the appropriate counts.
	
	foreach my $c ( $action->errors )
	{
	    if ( $c->code =~ /^[EC]/ )
	    {
		$edt->{error_count}--;
	    }
	    
	    else
	    {
		$edt->{demoted_count}--;
	    }
	}

	foreach my $c ( $action->warnings )
	{
	    $edt->{warning_count}--;
	}

	# Return true, because the action has successfully been aborted.

	return 1;
    }
}


# child_action ( [action], table, operation, record )
# 
# This method is called from client code or subclass methods that wish to create auxiliary actions
# to supplement the current one. For example, adding a record to one table may involve also adding
# another record to a different table. If the first parameter is an action reference (either a
# string or a Perl reference), the new action is attached to that. Otherwise, it is attached to
# the current action.

sub child_action {
    
    my ($edt, @params) = @_;
    
    my $parent;
    
    # Determine which action to attach the child to, or throw an exception if none can be found.
    
    if ( $params[0] =~ /^@/ )
    {
	$parent = $edt->{action_ref}{$params[0]} || croak "no matching action found for '$params[0]'";
	shift @params;
    }
    
    elsif ( ref $params[0] )
    {
	$parent = shift @params;
	
	croak "bad reference $parent: must be of class EditTransaction::Action"
	    unless $parent->isa('EditTransaction::Action');
    }
    
    else
    {
	$parent = $edt->{current_action};

	croak "there is no current action" unless $parent;
    }
    
    # Create the new action using the remaining parameters, and set it as a child of the
    # existing one.
    
    my $child = $edt->_new_action(@params);
    
    $parent->_add_child($child);
    
    # Handle the new action.
    
    return $edt->_handle_action($child, $child->operation);
}


# current_action ( )
#
# Return a reference to the current action, if any.

sub current_action {

    my ($edt) = @_;

    if ( $edt->{current_action} )
    {
	return '@' . $edt->{current_action}->label;
    }
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
    
    if ( my $sup_table = get_table_property($table, 'SUPERIOR_TABLE') )
    {
	$permission = $edt->authorize_subordinate_action($action, $operation, $sup_table, $table, $keyexpr);
    }
    
    # Otherwise carry out the appropriate check for this operation directly on its own table.

    else
    {
	sswitch ( $operation )
	{
	    case 'insert': {
		$permission = $edt->check_table_permission($table, 'post');
	    }
	    
	    case 'update':
	    case 'replace': {
		$permission = $edt->check_record_permission($table, 'edit', $keyexpr);
	    }
	    
	    case 'update_many':
	    case 'delete_many': {
		$permission = $edt->check_table_permission($table, 'admin');
	    }
	    
	    case 'delete': {
		$permission = $edt->check_record_permission($table, 'delete', $keyexpr);
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
    
    # Store the permission with the action and return it.
    
    $action->_set_permission($permission);
}


# authorize_subordinate_action ( action, operation, superior_table, table, keyexpr )
#
# Carry out the authorization operation where the table to be authorized against ($superior_table)
# is different from the one on which the action is being executed ($table).

sub authorize_subordinate_action {

    my ($edt, $action, $operation, $superior_table, $table, $keyexpr) = @_;
    
    # First, check if we have information about the link between the subordinate and superior
    # tables already cached. If not, determine it now.
    
    my ($linkcol, $supcol, $altfield, @linkval, $update_linkval);
    my (%PERM, $perm);
    
    unless ( ($linkcol, $supcol, $altfield) = $edt->{permission_link_cache}{$table}->@* )
    {
	# The subordinate table must contain a column that links records in this table to records
	# in the superior table. If no linking column is specified, assume that it the same as the
	# primary key of the superior table.
	
	$linkcol = get_table_property($table, 'SUPERIOR_KEY');
	$supcol = get_table_property($superior_table, 'PRIMARY_KEY');
	
	$linkcol ||= $supcol;

	$altfield = get_table_property($table, $linkcol, 'ALTERNATE_NAME') ||
	    ($linkcol =~ /(.*)_no$/ && "${1}_id");
	
	croak "SUPERIOR_TABLE was given as '$superior_table' but no key column was found"
	    unless $linkcol;
	
	$edt->{permission_link_cache}{$table} = [$linkcol, $supcol, $altfield];
    }
    
    # For an insert operation, the link value is given in the record to be inserted. If it is not
    # found, the action cannot be authorized.
    
    if ( $operation eq 'insert' )
    {
	@linkval = $edt->input_record_value($action, $linkcol, $altfield);

	unless ( @linkval )
	{
	    $edt->add_condition($action, 'E_REQUIRED', $linkcol);
	    $PERM{error} = 1;
	}
    }
    
    # The operations 'update_many' and 'delete_many' require administrative permission for the
    # table on which they are being authorized. If we have this permission, the operation is
    # authorized. If not, it isn't.
    
    if ( $operation eq 'update_many' || $operation eq 'delete_many' )
    {
	return $edt->check_table_permission($superior_table, 'admin');
    }
    
    # For a 'delete_cleanup' operation, authorization is based on the specified key expression.
    
    elsif ( $operation eq 'delete_cleanup' )
    {
	croak "bad key expression" unless ref $keyexpr eq 'ARRAY' && $keyexpr->@*;
	@linkval = $keyexpr->@*;
    }
    
    # For update, replace, delete, and other operations, the value of this column in the existing
    # database record(s) must be retrieved.
    
    else
    {
	eval
	{
	    @linkval = $edt->db_column_values($table, $keyexpr, $linkcol, { distinct => 1 });
	};
	
	return 'none' unless @linkval;
	
	# If a different value is given in the record, we must check this value too. The link
	# value can only be updated if authorization succeeds for both of them, and if the
	# transaction allows MOVE_SUBORDINATES.
	
	if ( $operation eq 'update' || $operation eq 'replace' )
	{
	    $update_linkval = $action->record_value_alt($linkcol, $altfield);

	    if ( $update_linkval && any { $_ ne $update_linkval } @linkval )
	    {
		push @linkval, $update_linkval;
		$PERM{move} = 1 unless $edt->{allows}{MOVE_SUBORDINATES};
	    }
	}
    }
    
    # Determine the aggregate permission. If any of the link values cannot yet be evaluated, mark
    # this action as needing authorization at execution time. If any of them have a permission of
    # 'none' or 'locked', that will be the aggregate permission. Otherwise, if any of them have a
    # permission of 'edit', that will be the aggregate permission. If all have 'admin', then that
    # will be the aggregate. If we cannot find any permissions at all, the aggregate will be
    # empty.
    
    foreach my $lv ( @linkval )
    {
	# If any of the link values is an action reference, look it up. If no matching action can
	# be found, add an error condition. Otherwise, if the action has a key value then use
	# it. If not, this action will have to be authorized at execution time.
	
	if ( $lv =~ /^@/ )
	{
	    my $ref = $edt->{action_ref}{$lv};
	    
	    if ( $ref && $ref->table eq $superior_table )
	    {
		if ( my $refkey = $ref->keyval )
		{
		    $lv = $refkey;
		}
		
		# If the reference is good but the reference has no key value, it is almost
		# certainly a reference to a new record insertion that hasn't been executed
		# yet. This means we will have to put off the authorization until execution time.
		
		else
		{
		    $PERM{later} = 1;
		    next;
		}
	    }
	    
	    # If we cannot find the reference at all, add an error condition. This is NOT a
	    # permission error, and cannot be demoted.
	    
	    else
	    {
		$edt->add_condition($action, 'E_BAD_REFERENCE', $linkcol, $lv);
		$PERM{error} = 1;
		next;
	    }
	}
	
	# If we get here, then we have an actual key value. If we have a cached permission value,
	# use it. Otherwise, generate one. If the keyexpr is just a key value, we need to turn it
	# into an expression.
	
	unless ( $perm = $edt->{permission_record_edit_cache}{$superior_table}{$lv} )
	{
	    my $keyexpr = "$supcol=$lv";
	    $perm = $edt->check_record_permission($superior_table, 'edit', $keyexpr);
	    $edt->{permission_record_edit_cache}{$superior_table}{$lv} = $perm;
	}
	
	# Keep track of which permissions we have found. If the permission has the unlock
	# attribute, count that as well.

	if ( $perm =~ /^(.*?),(.*)/ )
	{
	    my $main = $1;
	    my $attrs = $2;
	    
	    $PERM{$main} = 1;
	    $PERM{unlock} = 1 if $attrs =~ /unlock/;
	}
	
	else
	{
	    $PERM{$perm} = 1;
	}

	$PERM{bad} = 1 if $perm !~ /^none|^locked|^edit|^admin/;
	
	# Finally, keep track of the superior keys we have authorized against.

	$edt->{superior_auth_keys}{$lv} = 1;
    }
    
    # Now use the %PERM hash to generate an aggregate permission for this action. If any of the
    # linked records had 'none' or 'locked', the action cannot be executed regardless of the other
    # permissions. If any of the linked records have an unrecognized position, the action cannot
    # be executed either.
    
    if ( $PERM{error} )
    {
	return 'error';
    }
    
    elsif ( $PERM{none} || $PERM{bad} )
    {
	return 'none';
    }
    
    elsif ( $PERM{locked} )
    {
	return 'locked';
    }
    
    # Otherwise, if any of the linked records had 'later' then we must put off the authorization
    # until execution time. Save all of the necessary information with the action.
    
    elsif ( $PERM{later} )
    {
	$action->_authorize_later(\@linkval, $PERM{move});
	return 'later';
    }

    # Otherwise, the action can proceed. If all of the linked records had 'admin' permission, that
    # is the aggregate permission. Otherwise, it is 'edit'. If any of the linked records had the
    # 'unlock' attribute, add that to the aggregate permission. Now is the time to add
    # C_MOVE_SUBORDINATES if that is indicated.
    
    elsif ( $PERM{edit} || $PERM{admin} )
    {
	my $aggregate = $PERM{edit} ? 'edit' : 'admin';
	$aggregate .= ',unlock' if $PERM{unlock};
	
	$edt->add_condition($action, 'C_MOVE_SUBORDINATES') if $PERM{move};
	
	return $aggregate;
    }
    
    # As a fallback, just return 'none'.
    
    else
    {
	return 'none';
    }
}


#     # If we were given a key expression for this record, fetch the current value for the
#     # linking column from that row.
    
#     my ($keyval, $linkval, $new_linkval, $record_col);
    
#     if ( $keyexpr )
#     {
# 	# $$$ This needs to be updated to allow for multiple $linkval keys!!!
	
# 	unless ( $linkval )
# 	{
# 	    $edt->add_condition($action, 'E_NOT_FOUND', $action->keyval);
# 	    return 'none';
# 	}
#     }
    
#     # Then fetch the new value, if any, from the action record. But not for a 'delete_cleanup'
#     # operation, for which that is not applicable.
    
#     if ( $operation eq 'insert' || $operation eq 'update' || $operation eq 'replace' || $operation eq 'other' )
#     {
# 	($new_linkval, $record_col) = $edt->record_value($action, $table, $linkcol);
#     }
    
#     # If we don't have one or the other value, that is an error.
    
#     unless ( $linkval || $new_linkval )
#     {
# 	$edt->add_condition($action, 'E_REQUIRED', $linkcol);
# 	return 'none';
#     }
    
#     # If we have both and they differ, that is also an error. It is disallowed to use an 'update'
#     # operation to switch the association of a subordinate record to a different superior record.
    
#     if ( $linkval && $new_linkval && $linkval ne $new_linkval )
#     {
# 	$edt->add_condition($action, 'E_BAD_UPDATE', $record_col);
# 	return 'none';
#     }
    
#     # Now that these two conditions have been checked, we make sure that $linkval has the proper
#     # value in it regardless of whether it is new (i.e. for an 'insert') or old (for other operations).
    
#     $linkval ||= $new_linkval;
    
#     # Now store this value in the action, for later record-keeping.
    
#     $action->_set_linkval($linkval);
    
#     # If we have a cached permission result for this linkval, then just return that. There is no
#     # reason to look up the same superior record multiple times in the course of a single
#     # transaction.
    
#     my $alt_permission;
    
#     if ( $edt->{linkval_cache}{$linkval} )
#     {
# 	$alt_permission = $edt->{linkval_cache}{$linkval};
#     }
    
#     # If the link value is a label, then it must represent an action that either updated
#     # or inserted a record into the proper table earlier in the transaction. If this
#     # operation succeeded, it must have been properly authorized. So all we need to do is
#     # check that this action is associated with the correct table, and if so we return the
#     # indicated permission.

#     # $$$ This is not enough, because of PROCEED. The previous action may have failed but the
#     # transaction may still go through. This will need to be checked during execution.
    
#     elsif ( $linkval =~ /^@/ )
#     {
# 	my $action = $edt->{action_ref}{$linkval};
	
# 	if ( $action && $action->table eq $superior_table )
# 	{
# 	    $alt_permission = $edt->check_table_permission($superior_table, 'edit');
# 	    $edt->{linkval_cache}{$linkval} = $alt_permission;
	    
# 	    # # If we have 'admin' permission on the superior table, then we return 'admin'.
	    
# 	    # if (  eq 'admin' )
# 	    # {
# 	    # 	$permission = 'admin';
# 	    # 	$edt->{linkval_cache}{$linkval} = $permission;
# 	    # }
	    
# 	    # # Otherwise, we return the proper permission for the operation we are doing.
	    
# 	    # elsif ( $operation eq 'insert' )
# 	    # {
# 	    # 	$permission = 'post';
# 	    # 	$edt->{linkval_cache}{$linkval} = 'edit';
# 	    # }
	    
# 	    # elsif ( $operation eq 'delete' )
# 	    # {
# 	    # 	$permission = 'delete';
# 	    # 	$edt->{linkval_cache}{$linkval} = 'edit';
# 	    # }
	    
# 	    # else
# 	    # {
# 	    # 	$permission = 'edit';
# 	    # 	$edt->{linkval_cache}{$linkval} = 'edit';
# 	    # }
	    
# 	    # return $permission;
# 	}
	
# 	else
# 	{
# 	    $edt->add_condition($action, 'E_BAD_REFERENCE', $record_col, $label);
# 	    return 'error';
# 	}
#     }
    
#     # Otherwise, generate a key expression for the superior record so that we can check
#     # permissions on that. If we cannot generate one, then we return with an error. The
#     # 'aux_keyexpr' routine will already have added one or more error conditions in this
#     # case.
    
#     else
#     {
# 	my $alt_keyexpr = $edt->aux_keyexpr($action, $superior_table, $sup_keycol, $linkval, $record_col);
	
# 	return 'error' unless $alt_keyexpr;
	
# 	# Now we carry out the permission check on the permission table. The permission check is for
# 	# modifying the superior record, since that is essentially what we are doing. Whether we are
# 	# inserting, updating, or deleting subordinate records, that essentially counts as modifying
# 	# the superior record.
	
# 	$alt_permission = $edt->check_record_permission($superior_table, 'edit', $alt_keyexpr);
# 	$edt->{linkval_cache}{$linkval} = $alt_permission;
#     }
    
#     # Now, if the alt permission is 'admin', then the subordinate permission must be as well.
    
#     if ( $alt_permission =~ /admin/ )
#     {
# 	return $alt_permission;
#     }
    
#     # If the alt permission is 'edit', then we need to figure out what subordinate permission we
#     # are being asked for and return that.
    
#     elsif ( $alt_permission =~ /edit|post/ )
#     {
# 	my $unlock = $alt_permission =~ /unlock/ ? ',unlock' : '';
	
# 	if ( $operation eq 'insert' )
# 	{
# 	    return 'post';
# 	}
	
# 	elsif ( $operation eq 'delete' || $operation eq 'delete_many' || $operation eq 'delete_cleanup' )
# 	{
# 	    return "delete$unlock";
# 	}
	
# 	elsif ( $operation eq 'update' || $operation eq 'update_many' || $operation eq 'replace' || $operation eq 'other' )
# 	{
# 	    return "edit$unlock";
# 	}
	
# 	else
# 	{
# 	    croak "bad subordinate operation '$operation'";
# 	}
#     }
    
#     # If the returned permission is 'notfound', then the record that is supposed to be linked to
#     # does not exist. This should generate an E_KEY_NOT_FOUND.
    
#     elsif ( $alt_permission eq 'notfound' )
#     {
# 	$record_col ||= '';
# 	$edt->add_condition($action, 'E_KEY_NOT_FOUND', $record_col, $linkval);
# 	return 'error';
#     }
    
#     # Otherwise, the permission returned should be 'none'. So return that.
    
#     else
#     {
# 	return 'none';
#     }
# }


# _handle_action ( action )
# 
# Handle the specified action record. If errors were generated for this record, put it on the 'bad
# record' list. Otherwise, either execute it immediately or put it on the action list to be
# executed later. This method returns true if the action is successfully executed or is queued for
# later execution, false otherwise.

sub _handle_action {
    
    my ($edt, $action, $operation) = @_;
    
    # If the action has accumulated any errors, update fail_count and the action status.
    
    if ( $action->errors )
    {
	$edt->{fail_count}++;
	$action->_set_status('failed');
	return;
    }

    # If the operation is 'skip', update skip_count and the action status.
    
    elsif ( $operation eq 'skip' )
    {
	$edt->{skip_count}++;
	$action->_set_status('skipped');
	return;
    }
    
    # Otherwise, the action is able to be executed. If the 'execute_immediately' flag is set,
    # execute it now. This will also cause any other pending actions to execute.
    
    elsif ( $edt->{execute_immediately} )
    {
	return $edt->_execute_action_list;
    }
    
    # Otherwise return a true value, since the action has been queued for later execution.
    
    else
    {
	return 1;
    }
}


	# my $table = $action->table;
	
	# # For a multiple action, all of the failed keys are put on the list.
	
	# if ( $action->is_multiple )
	# {
	#     push @{$edt->{failed_keys}{$table}}, $action->all_keys;
	# }
	
	# elsif ( my $keyval = $action->keyval )
	# {
	#     push @{$edt->{failed_keys}{$table}}, $keyval;
	# }



# execute_action ( action )
#
# This method is designed to be called either internally by this class or explicitly by code from
# other classes. It executes a single action, and returns the result. $$$ this will need to be
# rewritten if we actually need it.

# sub execute_action {
    
#     my ($edt, $action) = @_;
    
#     # If the action has already been executed or abandoned, throw an exception.

#     croak "you must specify an action" unless $action && $action->isa('EditTransaction::Action');
#     croak "that action has already been executed or abandoned" if $action->status;
    
#     # If errors have already occurred, then do nothing.
    
#     if ( ! $edt->can_proceed )
#     {
# 	$edt->{skip_count}++;
# 	return;
#     }

#     # If we haven't already started the transaction in the database, do so now.
    
#     elsif ( ! $edt->has_started )
#     {
# 	$edt->_start_transaction;
#     }

#     # Now execute the action.

#     return $edt->_execute_action($action);
# }


# sub _execute_action {

#     my ($edt, $action) = @_;

#     my $result;
    
#     # Call the appropriate routine to execute this operation.
    
#     sswitch ( $action->operation )
#     {
# 	case 'insert': {
# 	    $result = $edt->_execute_insert($action);
# 	}
	
# 	case 'update': {
# 	    $result = $edt->_execute_update($action);
# 	}

# 	case 'update_many': {
# 	    $result = $edt->_execute_update_many($action);
# 	}
	
# 	case 'replace': {
# 	    $result = $edt->_execute_replace($action);
# 	}
	
# 	case 'delete': {
# 	    $result = $edt->_execute_delete($action);
# 	}

# 	case 'delete_cleanup': {
# 	    $result = $edt->_execute_delete_cleanup($action);
# 	}

# 	case 'delete_many': {
# 	    $result = $edt->_execute_delete_many($action);
# 	}

# 	case 'other': {
# 	    $result = $edt->_execute_other($action);
# 	}
	
#         default: {
# 	    croak "bad operation '$_'";
# 	}
#     }
    
#     # If errors have occurred, then we return false. Otherwise, return the result of the
#     # execution.
    
#     if ( ! $edt->can_proceed )
#     {
# 	return undef;
#     }
    
#     else
#     {
# 	return $result;
#     }
# }


    # # If there are no actions to do, and none have been done so far, and no errors have already
    # # occurred, then add C_NO_RECORDS unless the NO_RECORDS condition is allowed. This will cause
    # # the transaction to be immediately aborted.
    
    # unless ( @{$edt->{action_list}} || @{$edt->{errors}} || $edt->{exec_count} || $edt->allows('NO_RECORDS') )
    # {
    # 	$edt->add_condition(undef, 'C_NO_RECORDS');
    # }
    



# _execute_action_list ( )
#
# Execute any pending actions. This is called either by 'execute' or by 'start_execution'. If
# $complete is true, then these are all of the remaining actions for this EditTransaction.

sub _execute_action_list {

    my ($edt, $arg) = @_;
    
    my $return_value;
    
    # Iterate through the action list, starting after the last completed action.
    
    my $start_index = $edt->{completed_count};
    my $end_index = $edt->{action_list}->$#*;
    
  ACTION:
    foreach my $i ( $start_index .. $end_index )
    {
	my $action = $edt->{action_list}[$i];
	
	# If any errors have accumulated on this transaction, skip all remaining actions. This
	# includes any errors that may have been generated by the previous action.
	
	last ACTION if $edt->has_errors;
	
	# If this particular action has been executed, aborted, or skipped, then pass over it.
	
	next ACTION if $action->has_completed;
	
	# If this particular action has any errors, then skip it. We need to do this check
	# separately, because if PROCEED has been set for this transaction then any
	# errors that were generated during validation of this action will have been converted
	# to warnings. But in that case, $action->errors will still return true, and this
	# action should not be executed.
	
	if ( $action->errors )
	{
	    $action->_set_status('failed');
	    next ACTION;
	}
	
	# If there are pending deletes and this action is not a delete, execute those now.
	
	if ( $edt->{pending_deletes} && $edt->{pending_deletes}->@* && $action->operation ne 'delete' )
	{
	    $edt->_cleanup_pending_actions;
	    next ACTION if $edt->has_errors;
	}
	
	# Set the current action and then execute the appropriate handler for this
	# action's operation. A child action is counted as a subcomponent of its parent
	# action, and the current action is set accordingly.
	
	$edt->{current_action} = $action->parent || $action;

	sswitch ( $action->operation )
	{
	    case 'insert': {
		$return_value = $edt->_execute_insert($action);
	    }
	    
	    case 'update': {
		$return_value = $edt->_execute_update($action);
	    }
	    
	    case 'update_many': {
		$return_value = $edt->_execute_update_many($action);
	    }
	    
	    case 'replace': {
		$return_value = $edt->_execute_replace($action);
	    }
	    
	    case 'delete': {
		
		# If we are allowing multiple deletion and the next action is also a delete,
		# or if we are at the end of the action list, save this action on the
		# pending_deletes queue and go on to the next. This loop will be repeated
		# until a non-delete operation is encountered.
		
		if ( $edt->allows('MULTI_DELETE') && ! $edt->{execute_immediately} )
		{
		    if ( $i < $end_index && $edt->{action_list}[$i+1]{operation} eq 'delete' ||
			 $i == $end_index && $arg )
		    {
			push $edt->{pending_deletes}->@*, $action;
			next ACTION;
		    }
		    
		    # Otherwise, if we have pending deletes then coalesce them now before
		    # executing.
		    
		    elsif ( $edt->{pending_deletes}->@* )
		    {
			$action->_coalesce($edt->{pending_deletes});
			delete $edt->{pending_deletes};
		    }
		}
		
		# Now execute the action.
		
		$return_value = $edt->_execute_delete($action);
	    }
	    
	    case 'delete_cleanup' : {
		$return_value = $edt->_execute_delete_cleanup($action);
	    }
	    
	    case 'delete_many': {
		$return_value = $edt->_execute_delete_many($action);
	    }
	    
	    case 'other': {
		$return_value = $edt->_execute_other($action);
	    }
	    
	  default: {
		croak "_execute_action_list: bad operation '$_'";
	    }
	}
    }
    
    # If this is the completion of the EditTransaction and no fatal errors have occurred, execute
    # any pending operations.
    
    if ( $arg eq 'complete' && ! $edt->has_errors )
    {
	$edt->_cleanup_pending_actions;
    }
    
    # Update the count of completed actions. If this routine is called again, it will skip
    # over everything on the list as of now.
    
    $edt->{completed_count} = scalar($edt->{action_list}->@*);
    return $return_value;
}


sub _cleanup_pending_actions {

    my ($edt) = @_;

    if ( $edt->{pending_deletes} && $edt->{pending_deletes}->@* )
    {
	my $delete_action = pop $edt->{pending_deletes}->@*;
	$delete_action->_coalesce($edt->{pending_deletes});
	delete $edt->{pending_deletes};
	$edt->_execute_delete($delete_action);
    }
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


# record_value ( action, table, column )
# 
# Return a list of two values. The first is the value from the specified action's record that
# corresponds to the specified column in the database, or undef if no matching value found. The
# second is the key under which that value was found in the action record.

sub record_value {
    
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
    
    $edt->debug_line("$sql\n") if $edt->{debug_mode};
    
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
    
    $edt->debug_line("$sql\n") if $edt->{debug_mode};
    
    return $edt->dbh->selectrow_hashref($sql);
}


# check_permission ( action, key_expr )
# 
# Determine the current user's permission to do the specified action.

# sub checkp_ermission {
    
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
    
    $edt->debug_line("$sql\n") if $edt->{debug_mode};
    
    my ($new_keyval);
    
    # Execute the statement inside an eval block, to catch any exceptions that might be thrown.
    
    eval {
	
	$edt->before_action($action, 'insert', $table);
	
	# Execute the insert statement itself, provided there were no errors and the action
	# was not aborted during the execution of before_action.

	if ( $action->can_proceed )
	{
	    my $result = $dbh->do($sql);
	    
	    $action->_set_status('executed');
	    
	    # If the insert succeeded, get and store the new primary key value. Otherwise, add an
	    # error condition. Unlike update, replace, and delete, if an insert statement fails
	    # that counts as a failure of the action.
	    
	    if ( $result )
	    {
		$new_keyval = $dbh->last_insert_id(undef, undef, undef, undef);
		$action->_set_keyval($new_keyval);
		$edt->{inserted_keys}{$new_keyval}++;
	    }
	    
	    unless ( $new_keyval )
	    {
		$edt->add_condition($action, 'E_EXECUTE', 'insert statement failed');
	    }
	    
	    # Finally, call the 'after_action' method.
	    
	    $edt->after_action($action, 'insert', $table, $new_keyval);
	}
    };
    
    # If an exception occurred, print it to the error stream and add a corresponding error
    # condition. Any exeption that occurs after the database statement is executed counts as a
    # fatal error for the transaction, because there is no way to go back and undo that statement
    # if PROCEED is in force.
    
    if ( $@ )
    {
	$edt->error_line($@);
	
	if ( $@ =~ /duplicate entry '(.*)' for key '(.*)' at/i )
	{
	    my $value = $1;
	    my $key = $2;
	    $edt->add_condition($action, 'E_DUPLICATE', $value, $key);
	}
	
	else
	{
	    $edt->action_error($action, $action->status eq 'executed', 'E_EXECUTE',
			       'an exception occurred during execution');
	}
    };
    
    # If the insert succeeded, return the new primary key value. Also record this value so
    # that it can be queried for later. Otherwise, return undefined.
    
    if ( $action->has_succeeded && $new_keyval )
    {
	$edt->{exec_count}++;
	return $new_keyval;
    }
    
    else
    {
	$action->_set_status('failed') unless $action->status eq 'aborted';
	$edt->{fail_count}++;
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
    
    my $keyexpr = $action->keyexpr;
    my $keyval = $action->keyval
    
    my $sql = "	UPDATE $TABLE{$table} SET $set_list
		WHERE $keyexpr";
    
    $edt->debug_line("$sql\n") if $edt->{debug_mode};
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    my $result;
    
    eval {
	
	# If we are logging this action, then fetch the existing record.
	
	unless ( $edt->allows('NO_LOG_MODE') || get_table_property($table, 'NO_LOG') )
	{
	    $edt->fetch_old_record($action, $table, $keyexpr);
	}
	
	# Start by calling the 'before_action' method.
	
	$edt->before_action($action, 'update', $table);
	
	# Then execute the update statement itself, provided there are no errors and the action
	# has not been aborted. If the update statement returns a zero result and does not throw
	# an exception, that means the updated record was identical to the old one. This is
	# counted as a successful execution, and is marked with a warning.
	
	if ( $action->can_proceed )
	{
	    $result = $dbh->do($sql);
	    
	    $action->_set_status('executed');
	    $action->{updated_keys}{$keyval}++;
	    
	    unless ( $result )
	    {
		$edt->add_condition($action, 'W_UNCHANGED', 'updated record is identical to the old');
	    }
	    
	    $edt->after_action($action, 'replace', $table, $result);
	}
    };
    
    if ( $@ )
    {
	$edt->error_line($@);
	
	if ( $@ =~ /duplicate entry '(.*)' for key '(.*)' at/i )
	{
	    my $value = $1;
	    my $key = $2;
	    $edt->add_condition($action, 'E_DUPLICATE', $value, $key);
	}

	else
	{
	    $edt->action_error($action, $action->status eq 'executed', 'E_EXECUTE',
				   'an exception occurred during execution');
	}
    };
    
    # If no errors occurred, return the result of the database statement. Otherwise, return false
    # and set the action status to 'failed'.
    
    if ( $action->has_succeeded )
    {
	$edt->{exec_count}++;
	return $result;
    }
    
    else
    {
	$action->_set_status('failed') unless $action->status eq 'aborted';
	$edt->{fail_count}++;
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


# _execute_replace ( action )
# 
# Actually perform an replace operation on the database. The record keys and values should already
# have been checked by 'validate_record' or some other code, and lists of columns and values
# generated.

sub _execute_replace {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
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
    
    $edt->debug_line("$sql\n") if $edt->{debug_mode};
    
    my $keyval = $action->keyval;
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    my ($result);
    
    eval {
	
	# If we are logging this action, then fetch the existing record if any.
	
	unless ( $edt->allows('NO_LOG_MODE') || get_table_property($table, 'NO_LOG') )
	{
	    $edt->fetch_old_record($action, $table);
	}
	
	# Start by calling the 'before_action' method.
	
	$edt->before_action($action, 'replace', $table);
	
	# Then execute the replace statement itself, provided there are no errors and the action
	# was not aborted. If the replace statement returns a zero result and does not throw an
	# exception, that means that the new record was identical to the old one. This is counted
	# as a successful execution, and is marked with a warning.
	
	if ( $action->can_proceed )
	{
	    $result = $dbh->do($sql);
	    
	    $action->_set_status('executed');
	    $action->{replaced_keys}{$keyval}++;
	    
	    unless ( $result )
	    {
		$edt->add_condition($action, 'W_UNCHANGED', 'new record is identical to the old');
	    }
	    
	    $edt->after_action($action, 'replace', $table, $result);
	}
    };
    
    # If an exception occurred, print it to the error stream and add a corresponding error
    # condition. Any exeption that occurs after the database statement is executed is
    # automatically a fatal error for the transaction, because there is no way to go back and undo
    # that statement if PROCEED is in force.
    
    if ( $@ )
    {	
	$edt->error_line($@);
	
	if ( $@ =~ /duplicate entry '(.*)' for key '(.*)' at/i )
	{
	    my $value = $1;
	    my $key = $2;
	    $edt->add_condition($action, 'E_DUPLICATE', $value, $key);
	}

	else
	{
	    $edt->action_error($action, $action->status eq 'executed', 'E_EXECUTE',
				   'an exception occurred during execution');
	}
    }
    
    # If no errors occurred, return the result of the database statement. Otherwise, return false
    # and set the action status to 'failed'.
    
    if ( $action->has_succeeded )
    {
	$edt->{exec_count}++;
	return $result;
    }
    
    else
    {
	$action->_set_status('failed') unless $action->status eq 'aborted';
	$edt->{fail_count}++;
	return undef;
    }
}


# _execute_delete ( action )
# 
# Actually perform a delete operation on the database. The only field that makes any difference
# here is the primary key.

sub _execute_delete {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    my $dbh = $edt->dbh;
    
    my $keyexpr = $action->keyexpr;
    my @keyval = $action->keyval;
    
    # If the following flag is set, deliberately generate an SQL error for
    # testing purposes.
    
    if ( $TEST_PROBLEM{delete_sql} )
    {
	$keyexpr .= 'XXXX';
    }
    
    # Construct the DELETE statement.
    
    my $sql = "	DELETE FROM $TABLE{$table} WHERE $keyexpr";
    
    $edt->debug_line( "$sql\n" ) if $edt->{debug_mode};
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    my ($result, $cleanup_called);
    
    eval {
	
	# If we are logging this action, then fetch the existing record.
	
	unless ( $edt->allows('NO_LOG_MODE') || get_table_property($table, 'NO_LOG') )
	{
	    $edt->fetch_old_record($action, $table, $keyexpr);
	}
	
	# Start by calling the 'before_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing.    
	
	$edt->before_action($action, 'delete', $table);
	
	# Then execute the delete statement itself, provided the action has not been aborted.
	
	if ( $action->can_proceed )
	{
	    $result = $dbh->do($sql);
	    
	    $action->_set_status('executed');
	    $action->{deleted_keys}{$_}++ foreach @keyval;
	    
	    unless ( $result )
	    {
		$edt->add_condition($action, 'W_EXECUTE', 'delete statement failed');
	    }
	    
	    $edt->after_action($action, 'delete', $table, $result);
	}
    };
    
    if ( $@ )
    {	
	$edt->error_line($@);
	$edt->action_error($action, $action->status eq 'executed', 'E_EXECUTE',
			       'an exception occurred during execution');
    };
    
    # Record the number of records deleted, along with the mapping between key values and record labels.
    # $$$
    # my ($count, @keys, @labels);
    
    # if ( $action->is_multiple )
    # {
    # 	$count = $action->action_count;
	
    # 	@keys = $action->all_keys;
    # 	@labels = $action->all_labels;
	
    # 	foreach my $i ( 0..$#keys )
    # 	{
    # 	    $edt->{key_labels}{$table}{$keys[$i]} = $labels[$i] if defined $labels[$i] && $labels[$i] ne '';
    # 	}
    # }
    
    # else
    # {
    # 	$count = 1;
    # 	@keys = $action->keyval + 0;
    # 	my $label = $action->label;
    # 	$edt->{key_labels}{$table}{$keys[0]} = $label if defined $label && $label ne '';
    # 	# There is no need to set label_keys, because the record has now vanished and no longer
    # 	# has a key.
    # }
    
    # # If the delete succeeded, log it and return true. Otherwise, return false.
    
    # if ( $result && ! $action->errors )
    # {
    # 	$edt->{exec_count}++;
    # 	push @{$edt->{deleted_keys}{$table}}, @keys;
    # 	push @{$edt->{datalog}}, EditTransaction::LogEntry->new($_, $action) foreach @keys;
	
    # 	if ( my $linkval = $action->linkval )
    # 	{
    # 	    if ( $linkval =~ /^[@](.*)/ )
    # 	    {
    # 		$linkval = $edt->{label_keys}{$1};
    # 		$edt->add_condition($action, 'E_EXECUTE', 'link value label was not found') unless $linkval;
    # 	    }
	    
    # 	    $edt->{superior_keys}{$table}{$linkval} = 1;
    # 	}
	
    # 	return $result;
    # }

    if ( $action->has_succeeded )
    {
	$edt->{exec_count}++;
	return $result;
    }
    
    else
    {
	$action->_set_status('failed') unless $action->status eq 'aborted';
	$edt->{fail_count}++;
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
    
    # Come up with the list of keys to preserve. If there aren't any entries, add a 0 to avoid a
    # syntax error. This will not match any records under the Paleobiology Database convention
    # that 0 is never a valid key.
    
    my @preserve;	# $$$ this needs to be rewritten.
    
    push @preserve, @{$edt->{inserted_keys}{$table}} if ref $edt->{inserted_keys}{$table} eq 'ARRAY';
    push @preserve, @{$edt->{updated_keys}{$table}} if ref $edt->{updated_keys}{$table} eq 'ARRAY';
    push @preserve, @{$edt->{replaced_keys}{$table}} if ref $edt->{replaced_keys}{$table} eq 'ARRAY';
    
    push @preserve, '0' unless @preserve;
    
    my $key_list = join(',', @preserve);
    
    my $keyexpr = "$selector and not $keycol in ($key_list)";
    
    # Figure out which keys will be deleted, so that we can list them later.

    my $init_sql = "	SELECT $keycol FROM $TABLE{$table} WHERE $keyexpr";
    
    $edt->debug_line( "$init_sql\n" ) if $edt->{debug_mode};

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
    
    $edt->debug_line( "$sql\n" ) if $edt->{debug_mode};
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    my ($result, $cleanup_called);
    
    eval {
	
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
	
	if ( $action->can_proceed )
	{
	    $result = $dbh->do($sql);
	    
	    $action->_set_status('executed');
	    $action->_confirm_keyval($deleted_keys);   # $$$ needs to be rewritten
	    
	    $edt->after_action($action, 'delete_cleanup', $table);
	}
    };
    
    if ( $@ )
    {	
	$edt->error_line($@);
	$edt->action_error($action, $action->status eq 'executed', 'E_EXECUTE',
			       'an exception occurred during execution');
    };
    
    # # Record the number of records deleted, along with the mapping between key values and record labels.
    
    # if ( ref $deleted_keys eq 'ARRAY' )
    # {
    # 	foreach my $i ( 0..$#$deleted_keys )
    # 	{
    # 	    push @{$edt->{datalog}}, EditTransaction::LogEntry->new($deleted_keys->[$i], $action);
    # 	}
    # }
    
    # If the delete succeeded, return true. Otherwise, return false.
    
    if ( $action->has_succeeded )
    {
	$edt->{exec_count}++;
	return $result;
    }
    
    else
    {
	$action->_set_status('failed') unless $action->status eq 'aborted';
	$edt->{fail_count}++;
	return undef;
    }
}


# _execute_delete_many ( action )
# 
# Perform an update_many operation on the database. The keys and values have NOT yet been
# checked.

sub _execute_delete_many {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    croak "operation 'delete_many' is not yet implemented";
}


# _execute_other ( action )
# 
# Perform an operation other than insert, replace, update, or delete on the database. The keys
# and values have been checked previously.

sub _execute_other {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    my $record = $action->record;
    
    # Set this as the current action.
    
    $edt->{current_action} = $action->parent || $action;
    
    # Determine the method to be called.
    
    my $method = $action->method;
    
    # Call the specified method inside a try block. If it fails, add either an error or
    # a warning depending on whether this EditTransaction allows PROCEED.
    
    my ($result, $cleanup_called);
    
    # Call the method specified for this action, provided there are no errors and the action
    # has not been aborted. $$$ need to add some method of accounting for inserted keys, etc.
    
    eval {
	
	$edt->before_action($action, 'other', $table);

	if ( $action->can_proceed )
	{
	    $result = $edt->$method($action, $table, $record);
	    
	    $action->_set_status('executed');

	    $edt->after_action($action, 'other', $table);
	}
    };
    
    if ( $@ )
    {
	$edt->error_line($@);
	$edt->add_condition('E_EXECUTE', 'an exception occurred during execution');
    }
    
    # # If the operation succeeded, return true. Otherwise, return false. In either case, record the
    # # mapping between key value and record label.
    
    # my $keyval = $action->keyval + 0;
    # my $label = $action->label;
    
    # if ( defined $label && $label ne '' )
    # {
    # 	$edt->{label_keys}{$label} = $keyval;
    # 	$edt->{key_labels}{$table}{$keyval} = $label;
    # }
    
    # if ( $result && ! $action->errors )
    # {
    # 	$edt->{exec_count}++;
    # 	push @{$edt->{other_keys}{$table}}, $keyval;
    # 	# push @{$edt->{datalog}}, EditTransaction::LogEntry->new($keyval, $action);
	
    # 	if ( my $linkval = $action->linkval )
    # 	{
    # 	    if ( $linkval =~ /^[@](.*)/ )
    # 	    {
    # 		$linkval = $edt->{label_keys}{$1};
    # 		$edt->add_condition($action, 'E_EXECUTE', 'link value label was not found') unless $linkval;
    # 	    }
	    
    # 	    $edt->{superior_keys}{$table}{$linkval} = 1;
    # 	}
	
    # 	return $result;
    # }

    if ( $action->has_succeeded )
    {
	$edt->{exec_count}++;
	return $result;
    }
    
    else
    {
	$action->_set_status('failed') unless $action->status eq 'aborted';
	$edt->{fail_count}++;
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


# _execute_sql_action ( action, table, record )
#
# Execute the specified SQL statement.

sub _execute_sql_action {
    
    my ($edt, $action, $table, $record) = @_;
    
    my $sql_stmt = $record->{sql};
    my $store_it = $record->{store_result};
    
    croak "no sql statement to execute" unless $sql_stmt;

    $edt->debug_line("$sql_stmt\n") if $edt->{debug_mode};
    
    my $result = $edt->{dbh}->do($sql_stmt);

    if ( $store_it )
    {
	if ( ref $store_it eq 'SCALAR' )
	{
	    $store_it->$* = $result;
	}
	
	elsif ( ref $store_it eq 'ARRAY' )
	{
	    push $store_it->@*, $result;
	}
	
	elsif ( ref $store_it eq 'CODE' )
	{
	    $store_it->($result);
	}
    }

    return $result;
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
#
# If overridden, this method should not do anything irrevocable to the database. If an exception
# is thrown during the execution of the database statement(s) generated by the action, then the
# rest of the action processing will be aborted. If the PROCEED allowance is present then the
# transaction as a whole may complete and preserve whatever was done by this method. It is best
# whenever possible to do any auxiliary database modifications in the after_action procedure,
# which will only execute once the SQL statements have successfully completed.

sub before_action {

    my ($edt, $action, $operation, $table) = @_;
    
    my $a = 1;	# We can stop here when debugging.
}


# after_action ( action, operation, table, result )
#
# This method is called after each successfully completed action. It is designed to be overridden
# by subclasses, so that any necessary auxiliary work can be carried out. The default method
# defined here does nothing.
# 
# For insert actions, the parameter $result will get the primary key value of the newly inserted
# record. For update, replace, delete, and do_sql actions, $result will get the result of the
# database statement that was executed. Otherwise, it will get the result returned by the action
# method.

sub after_action {

    my ($edt, $action, $operation, $table, $result) = @_;
    
    my $a = 1;	# We can stop here when debugging.
}


# Progress and results of actions
# -------------------------------

# The methods in this section can be called from code in subclasses to determine the progress of
# the EditTransaction and carry out auxiliary actions such as inserts to or deletes from other
# tables that are tied to the main one by foreign keys.

# tables ( )
#
# Return a list of tables that were affected by this transaction.

sub tables {
    
    my ($edt) = @_;

    return unless $edt->{tables};
    return keys $edt->{tables}->%*;
}


sub deleted_keys {

    my ($edt, $table) = @_;
    return $edt->_result_keys('deleted_keys', $table);
}


sub inserted_keys {

    my ($edt, $table) = @_;
    return $edt->_result_keys('inserted_keys', $table);
}


sub updated_keys {

    my ($edt, $table) = @_;
    return $edt->_result_keys('updated_keys', $table);
}


sub replaced_keys {

    my ($edt, $table) = @_;
    return $edt->_result_keys('replaced_keys', $table);
}


sub other_keys {

    my ($edt, $table) = @_;
    return $edt->_result_keys('other_keys', $table);
}


sub superior_keys {

    my ($edt, $table) = @_;
    return $edt->_result_keys('superior_keys', $table);
}


sub failed_keys {

    my ($edt, $table) = @_;
    return $edt->_result_keys('failed_keys', $table);
}


sub _result_keys {
    
    my ($edt, $type, $table) = @_;
    
    return unless $edt->{$type};
    return unless $edt->{tables} || $table;
    
    if ( wantarray && $table )
    {
	return $edt->{$type}{$table} ? $edt->{$type}{$table}->@* : ( );
    }
    
    elsif ( wantarray )
    {
	my $mt = $edt->{main_table};
	my @tables = grep { $_ ne $mt } keys $edt->{tables}->%*;
	unshift @tables, $mt if $mt;
	
	return map { $edt->{$type}{$_} ? $edt->{$type}{$_}->@* : ( ) } @tables;
    }
    
    elsif ( $table )
    {
	return $edt->{$type}{$table} ? $edt->{$type}{$table}->@* : 0;
    }

    else
    {
	return reduce { $a + ($edt->{$type}{$b} ? $edt->{$type}{$b}->@* : 0) }
	    0, keys $edt->{tables}->%*;
    }
}


sub action_keys {

    my ($edt, $table) = @_;
    
    return unless $edt->{tables} || $table;
    
    my @types = qw(deleted_keys inserted_keys updated_keys replaced_keys other_keys);
    
    if ( wantarray && $table )
    {
	return map { $edt->{$_} && $edt->{$_}{$table} ? $edt->{$_}{$table}->@* : ( ) } @types;
    }
    
    elsif ( wantarray )
    {
	my $mt = $edt->{main_table};
	my @tables = $mt, grep { $_ ne $mt } keys $edt->{tables}->%*;
	
	return map { $edt->_keys_by_table($_) } @tables;
    }
    
    elsif ( $table )
    {
	return reduce { $a + ($edt->{$b}{$table} ? $edt->{$b}{$table}->@* : 0) } 0, @types;
    }
    
    else
    {
	return reduce { $a + $edt->_keys_by_table($b) } 0, keys $edt->{tables}->%*;
    }
}


sub _keys_by_table {

    my ($edt, $table) = @_;
    
    my @types = qw(deleted_keys inserted_keys updated_keys replaced_keys other_keys);
    
    if ( wantarray )
    {
	return map { $edt->{$_}{$table} ? $edt->{$_}{$table}->@* : ( ) } @types;
    }

    else
    {
	return reduce { $a + ($edt->{$b}{$table} ? $edt->{$b}{$table}->@* : 0) } 0, @types;
    }
}


sub count_superior_key {

    my ($edt, $table, $keyval) = @_;
    
    if ( $keyval =~ /^@/ )
    {
	if ( my $action = $edt->{action_ref}{$keyval} )
	{
	    if ( $keyval = $action->keyval )
	    {
		$edt->{superior_keys}{$table}{$keyval} = 1;
	    }
	}
    }
    
    elsif ( $keyval )
    {
	$edt->{superior_keys}{$table}{$keyval} = 1;
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


sub action_count {

    return $_[0]->{action_count} || 0;
}


sub exec_count {
    
    return $_[0]->{exec_count} || 0;
}


sub fail_count {
    
    return $_[0]->{fail_count} || 0;
}


sub skip_count {

    return $_[0]->{skip_count} || 0;
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

1;


# Notes: add 'cannot_demote' operation for action.
