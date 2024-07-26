# 
# EditTransaction project
# -----------------------
# 
#   EditTransaction.pm - base class for data acquisition and modification
# 

use strict;

package EditTransaction;

use Carp qw(carp croak);
use Scalar::Util qw(weaken blessed);
use Switch::Plain;

use feature 'unicode_strings', 'postderef';

use EditTransaction::Action;

use Role::Tiny::With;

with 'EditTransaction::Conditions';
with 'EditTransaction::Directives';
with 'EditTransaction::Authorization';
with 'EditTransaction::Validation';
with 'EditTransaction::IActions';
with 'EditTransaction::Operations';
with 'EditTransaction::Execution';
with 'EditTransaction::TableInfo';

no warnings 'uninitialized';


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
		MOVE_SUBORDINATES => 1,
		NOT_FOUND => 1,
		NOT_PERMITTED => 1,
    		NO_RECORDS => 1, # deprecated
		PROCEED => 1,
		BAD_FIELDS => 1,
		DEBUG_MODE => 1,
		SILENT_MODE => 1,
		FIXUP_MODE => 1,
		ALTER_TRAIL => 1,
		IMMEDIATE_MODE => 1,
		VALIDATION_ONLY => 1 } );

our (%ALLOW_ALIAS) = ( IMMEDIATE_EXECUTION => 'IMMEDIATE_MODE' );

our (%TEST_PROBLEM);	# This variable can be set in order to trigger specific errors, in order
                        # to test the error-response mechanisms.

# We set @CARP_NOT because we specifically do not want subclasses of EditTransaction to be
# passed over as safe.

our (@CARP_NOT) = qw(EditTransaction::Action EditTransaction::Conditions EditTransaction::Validation);

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

# new ( dbh, [options] )
# 
# Creates a new EditTransaction object, for use in modifying any database table(s) that can be
# accessed using the database connection specified by $dbh. The value of this parameter can be
# either a DBI handle or else some other object implementing a `get_connection' method which
# returns a DBI handle. This EditTransaction object can be used to carry out multiple insertions,
# deletions, updates, replacements, and auxiliary operations, which will be all be done as a
# single database transaction.
# 
# The EditTransaction class is designed to be subclassed. A subclass must always include a
# database role, which provides the syntax and semantics appropriate for whichever database is
# being operated on. It may also override certain methods to enforce the semantics of a particular
# application, implement an authorization scheme other than the one provided by the dbms, add
# extra error checks and/or extra processing of submitted values, and may also alter or bypass the
# default checks made by the base class.
# 
# If a second parameter is specified, it must be a hash containing any of the following keys:
# 
#   table           the name of a default table for this transaction
# 
#   permission      an object that encapsulates user identity and permissions
# 
#   allows          a set of flags that may alter how the transaction is checked and executed
# 
# If a default table name is provided, it will be used for all operations except when
# overridden. Otherwise, each individual operation must identify the table to be operated on. The
# 'permission' option is only relevant for subclasses that implement an application-level
# authorization scheme. Otherwise, the transaction will be executed with no restrictions other
# than what is allowed by the database handle. The value of 'allows' can be either a listref or a
# hashref consisting of entries from the table below. Any entry that begins with 'NO_' or has a
# false value in the hash will be taken in the negative sense.
# 
# The default list of allowances and modes is given below. An EditTransaction subclass may
# register additional allowances. The allowances that correspond to cautions are designed to
# operate in the following way: either the allowance is specified with every transaction that
# requires it, or else a mechanism should be implemented to notify the user and request approval
# to proceed. This mechanism should be triggered whenever the corresponding caution is
# returned. If the user gives approval, the transaction should then be repeated with the necessary
# allowance.
# 
#   CREATE            If this allowance is specified, then 'insert' operations are allowed
#                     as part of this transaction. Otherwise, a 'C_CREATE' caution will be
#                     returned if insertion is attempted. The reason behind this is to prevent new
#                     records being added inadvertently due to improper client interface design or
#                     improper user input. In other words, insertion must be explicitly enabled
#                     or else some mechanism must be provided for the end user to confirm that
#                     they are intending to create new records rather than just update existing ones.
# 
#   LOCKED            If this allowance is specified, then locked records which the user has
#                     authorization to unlock may be operated upon without explicitly unlocking
#                     them. Otherwise, a 'C_LOCKED' caution will be returned for any attempt to
#                     operate on a locked record. The reason behind this is to enable the client
#                     interface to verify that the user wants to update or delete a locked record
#                     instead of just carrying it out.
# 
#   CHANGE_PARENT     If this allowance is specified, then records in subordinate tables may be
#                     updated so as to link them to a different parent record than they were
#                     linked to before. Otherwise, a 'C_CHANGE_PARENT' caution will be returned if
#                     this is attempted. As with 'LOCKED' and 'CREATE', the purpose is to require
#                     positive affirmation by either the client interface coder or the end user
#                     that this operation is intended.
# 
#   ALTER_TRAIL       If this allowance is specified, then users who have either unrestricted or
#                     administrative permission on a particular table can explicitly set the values of
#                     special columns such as those recording record creation and modifiation
#                     timestamps. Otherwise, a 'C_ALTER_TRAIL' caution will be returned if this is
#                     attempted. 
# 
#   PROCEED           If this allowance is specified, operations
#                     which fail for any reason will not abort the transaction.
# 
#   NOT_FOUND         If this allowance is specified, operations which refer to nonexistent
#                     records will still fail but will not abort the transaction. This allowance
#                     and the next are more restricted versions of PROCEED.
# 
#   NOT_PERMITTED     If this allowance is specified, operations that are not authorized
#                     by the specified permission object will still fail but will not abort
#                     the transaction.
# 
#   BAD_FIELDS        If this allowance is specified, any keys in an input record that do not
#                     correspond to database columns generate warnings rather than aborting the
#                     transaction.
# 
#   DEBUG_MODE        If this mode is specified, extra output will be written to STDERR to aid in
#                     debugging. This will include the text of all SQL statements executed, along
#                     with other information.
# 
#   SILENT_MODE       By default, database errors and certain exceptions are printed to STDERR. If
#                     this mode is specified, this output will be suppressed.
# 
#   IMMEDIATE_MODE    If this mode is specified, then a database transaction will immediately be
#                     started. Each operation method will cause the operation to be immediately
#                     executed. Otherwise, the operations are accumulated. The transaction will be
#                     started and the operations carried out only when 'commit' or 'execute' is
#                     called.
# 
#   FIXUP_MODE        If this mode is specified, and if the user has either unrestricted or
#                     administrative position on a particular table, then operations done on that
#                     table will not change special column values such as a modification timestamp.
# 
#   VALIDATION_ONLY   If this mode is specified, then no transaction will be started and the
#                     database will not be modified in any way. Each operation will be checked for
#                     errors, and nothing more.
# 

sub new {
    
    my ($class, $dbh_arg, $options, @rest) = @_;
    
    local ($_);
    
    # Check the arguments. The argument $dbh_arg is required, because it provides us with a
    # way to contact the database. A permission argument may be required by an application module
    # if one is included in the current class. The default 'check_permission_argument' method does
    # not require any permission and returns the string 'unrestricted'. An application may override
    # this, and may throw an exception if a valid permission object is not provided.
    
    croak "new EditTransaction: database connection is required"
	unless $dbh_arg && blessed($dbh_arg);
    
    # Now parse the options, if any.
    
    my ($table_specifier, $permission, @allowances);
    
    # If we are called using the old syntax, parse that.
    
    if ( ref $options eq 'Permissions' && @rest == 2 )
    {
	$permission = $options;
	$table_specifier = $rest[0];
	@allowances = parse_allowances($rest[1]);
    }
    
    elsif ( ref $options eq 'HASH' )
    {
	foreach my $k ( keys $options->%* )
	{
	    if ( $k eq 'table' )
	    {
		$table_specifier = $options->{$k};
	    }
	    
	    elsif ( $k eq 'permission' )
	    {
		$permission = $options->{$k};
	    }
	    
	    elsif ( $k eq 'allows' )
	    {
		@allowances = parse_allowances($options->{$k});
	    }
	    
	    else
	    {
		croak "unrecognized option '$k'";
	    }
	}
    }
    
    elsif ( $options )
    {
	croak "invalid parameter hash: '$options'";
    }
    
    # Validate the specified permission, which may be empty.
    
    my $store_permission = $class->validate_instance_permission($permission);
    
    # Create a new EditTransaction object, and bless it into the proper class.
    
    my $edt = { permission => $store_permission,
		default_table => $table_specifier || '',
		unique_id => $TRANSACTION_COUNT++,
		allows => { },
		action_list => [ ],
		action_ref => { },
		action_tables => { },
		exec_tables => { },
		current_action => undef,
		action_count => 0,
		completed_count => 0,
		record_count => 0,
		exec_count => 0,
		fail_count => 0,
		skip_count => 0,
		conditions => [ ],
		error_count => 0,
		warning_count => 0,
		demoted_count => 0,
		condition_code => { },
		commit_count => 0,
		rollback_count => 0,
		transaction => '',
		execution => '',
		debug_mode => 0,
	        errlog_mode => 1 } ;
    
    bless $edt, $class;
    
    # Store the request, dbh, and debug flag as local fields. If we are storing a reference to a
    # request object, we must weaken it to ensure that this object will be destroyed when it goes
    # out of scope. Circular references might otherwise prevent this.
    
    if ( $dbh_arg->can('get_connection') )
    {
	$edt->{request} = $dbh_arg;
	weaken $edt->{request};
	
	$edt->{dbh} = $dbh_arg->get_connection;
	
	my $dbh_class = ref $edt->{dbh};
	
	unless ( $dbh_class =~ /DBI::|Plugin::Database::/ )
	{
	    croak "'get_connection' failed to return a database handle";
	    $edt->{dbh} = undef;
	}
	
	if ( $dbh_arg->can('debug_mode') )
	{
	    $edt->{debug_mode} = $dbh_arg->debug_mode;
	}
	
	elsif ( $dbh_arg->can('debug') )
	{
	    $edt->{debug_mode} = $dbh_arg->debug;
	}
    }
    
    elsif ( ref($dbh_arg) =~ /DBI::/ )
    {
	$edt->{dbh} = $dbh_arg;
    }
    
    else
    {
	croak "'$dbh_arg' is neither a database handle nor an object that can provide one";
    }
    
    # Use the database interface module to evaluate the specified database handle. This call will
    # add an error condition if the database handle does not match the expected DBMS.
    
    unless ( $edt->can('db_validate_dbh') )
    {
	croak "Class '$class' must include a database role, like 'EditTransaction::Mod::xxx'";
    }
    
    if ( $edt->{dbh} )
    {
	my $result = $edt->db_validate_dbh($edt->{dbh});
	
	if ( $result ne 'ok' )
	{
	    $edt->add_condition('main', 'E_BAD_CONNECTION', $result);
	    $edt->{dbh} = undef;
	}
    }
    
    # If a table specifier is given, its value must correspond to a known table.
    # If that is the case, load the directives for this table as specified for
    # this class. Otherwise, add E_BAD_TABLE.
    
    if ( $table_specifier )
    {
	if ( $edt->table_info_ref($table_specifier) )
	{
	    $edt->init_directives($table_specifier);
	}
	
	else
	{
	    $edt->add_condition('main', 'E_BAD_TABLE', $table_specifier);
	}
    }
    
    # Now check the list of allowances, if any were specified. Add a warning for any values that
    # are not recognized as valid allowances.
    
    foreach my $k ( @allowances )
    {
	my $negated;
	
	if ( $k =~ /^NO_(.+)$/ )
	{
	    $k = $1;
	    $negated = 1;
	}
	
	$k = $ALLOW_ALIAS{$k} || $k;
	
	if ( $ALLOW_BY_CLASS{$class}{$k} || $ALLOW_BY_CLASS{EditTransaction}{$k} )
	{
	    if ( $negated )
	    {
		$edt->{allows}{$k} = 0;
	    }

	    else
	    {
		$edt->{allows}{$k} = 1;
	    }
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
    
    if ( $edt->{allows}{SILENT_MODE} )
    {
	$edt->{errlog_mode} = 0;
    }
    
    # Set the database handle attributes properly.
    
    $edt->{dbh}->{RaiseError} = 1;
    $edt->{dbh}->{PrintError} = 0;
    
    # Call 'initialize_instance', which allows subclasses to adjust the instance in any
    # way they deem necessary.
    
    $edt->initialize_instance;
    
    # If IMMEDIATE_MODE was specified, then immediately start a new transaction and turn on
    # execution. The same effect can be provided by calling the method 'start_execution' on this
    # new object.
    
    if ( $edt->{allows}{IMMEDIATE_MODE} )
    {
	$edt->start_transaction;
	$edt->{execution} = 'active';
    }
    
    return $edt;
}


# If this object is destroyed while a transaction is in progress, roll it back.

sub DESTROY {
    
    my ($edt) = @_;
    
    # Delete all direct references to actions, to avoid reference loops.
    
    delete $edt->{current_action};
    delete $edt->{action_list};
    
    # Roll back the transaction if it is still active, and if the database
    # handle hasn't already been destroyed.
    
    if ( $edt->is_active && $edt->{dbh} )
    {
	$edt->_call_cleanup('destroy');
	$edt->_rollback_transaction('destroy');
    }
}


# Parse hashes and lists of allowances

sub parse_allowances {
    
    my ($arg) = @_;
    
    # A hashref or a listref are assumed to specify allowances and/or modes.
    
    if ( ref $arg eq 'HASH' )
    {
	my @allowances;
	
	foreach my $k ( keys $arg->%* )
	{
	    push @allowances, $arg->{$k} ? $k : "NO_$k";
	}
	
	return @allowances;
    }
    
    elsif ( ref $arg eq 'ARRAY' )
    {
	return grep { $_ } map { split /\s*,\s*/ } $arg->@*;
    }
    
    elsif ( ref $arg )
    {
	return $arg;
    }
    
    # A scalar value is split on commas and surrounding whitespace.
    
    elsif ( $arg )
    {
	return grep { $_ } map { split /\s*,\s*/ } $arg;
    }
    
    else
    {
	return;
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

    return $_[0]{transaction} || 'init';
}


sub transaction {

    return $_[0]{transaction} // '';
}


sub has_started {

    return $_[0]{transaction} ne '';
}


sub is_active {

    return $_[0]{transaction} eq 'active';
}


sub is_executing {

    return $_[0]{transaction} eq 'active' && $_[0]{execution} ne '';
}


sub has_finished {

    return $_[0]{has_finished} || '';
}


sub has_committed {

    return $_[0]{transaction} eq 'committed';
}


sub has_failed {

    return $_[0]{transaction} eq 'failed' || $_[0]{transaction} eq 'aborted';
}


sub can_accept {
    
    return ! $_[0]{transaction} || $_[0]{transaction} eq 'active';
}


sub can_proceed {

    return (! $_[0]{transaction} || $_[0]{transaction} eq 'active') && ! $_[0]{error_count};
}


sub permission {
    
    return $_[0]->{permission} // '';
}


sub default_table {
    
    return $_[0]->{default_table};
}


# Debugging and error message display
# -----------------------------------

# error_line ( text )
# 
# This method is called internally to display certain error messages, including exceptions
# that were generated during the execution of code in this module. These messages are
# shown if either debugging mode or error logging mode is on.

sub error_line {
    
    my ($edt, $line) = @_;
    
    if ( ref $edt && ($edt->{errlog_mode} || $edt->{debug_mode}) )
    {
	if ( blessed $edt->{request} && $edt->{request}->can('error_line') )
	{
	    $edt->{request}->error_line($line);
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
	
	if ( blessed $edt->{request} && $edt->{request}->can('debug_line') )
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
    
    if ( @_ > 1 )
    {
	$_[0]{debug_mode} = ($_[1] ? 1 : 0);
    }
    
    return $_[0]{debug_mode};
}


# silent_mode ( value )
#
# Turn error logging mode on or off, or return the current value if no argument is
# given. The given value is inverted: a true value will turn off error logging mode, while
# a false value turns it on. The first line makes queries as efficient as possible.

sub silent_mode {

    if ( @_ > 1 )
    {
	$_[0]{errlog_mode} = ($_[1] ? 0 : 1);
    }

    return $_[0]{errlog_mode} ? 0 : 1;
}


# Class initialization
# --------------------

# The subroutines in this section must be called as class methods.

# copy_from ( from_class )
# 
# Copy all of the registered allowances, conditions, and directives from the
# specified class to this one.  This method is designed to be called at startup
# from subclasses of EditTransaction.

sub copy_from {
    
    my ($class, $from_class) = @_;
    
    $class->copy_allowances_from($from_class);
    
    $class->copy_conditions_from($from_class);
    
    $class->copy_directives_from($from_class);
}


# Allowances
# ----------
#
# Allowances are flags that affect the behavior of an EditTransaction. In general, their effect is
# to allow things that otherwise would not be allowed. Every caution has a corresponding
# allowance. So for example an attempt to modify a locked record would generate the caution
# C_LOCKED. The client user interface can then present the user with a question such as "Update
# locked records?" and if answered in the affirmative the request can be repeated with the
# allowance LOCKED. Other allowances are not associated with cautions.

# register_allowances ( name... )
# 
# Register the names of extra allowances for transactions in a particular subclass. This class
# method is designed to be called at startup from modules that subclasses this one.

sub register_allowances {
    
    my ($class, @names) = @_;
    
    foreach my $n ( @names )
    {
	croak "'$n' is not a valid allowance name" unless $n =~ /^[A-Z0-9_-]+$/;
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


# copy_allowances_from ( from_class )
# 
# This must be called as a class method. It copies all of the allowances
# registered by the specified class to this one.

sub copy_allowances_from {
    
    my ($class, $from_class) = @_;
    
    return if $class eq $from_class;
    
    if ( ref $ALLOW_BY_CLASS{$from_class} eq 'HASH' )
    {
	foreach my $n ( keys %{$ALLOW_BY_CLASS{$from_class}} )
	{
	    $ALLOW_BY_CLASS{$class}{$n} = $ALLOW_BY_CLASS{$from_class}{$n};
	}
    }
    
    my ($class, $from_class) = @_;
}


# allows ( condition )
# 
# Returns true if the specified condition is allowed for this EditTransaction, false
# otherwise. The set of allowed conditions was specified when this object was originally created.

sub allows {

    if ( @_ > 1 )
    {
	return ref $_[0]{allows} eq 'HASH' && defined $_[1] && $_[0]{allows}{$_[1]};
    }
    
    else
    {
	return ref $_[0]{allows} eq 'HASH' ? keys $_[0]{allows}->%* : ();
    }
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
    
    # If this transaction has already finished, return false.
    
    if ( $edt->has_finished )
    {
	return '';
    }
    
    # If the transaction has already started, return true.
    
    elsif ( $edt->has_started )
    {
	return 1;
    }
    
    # If we have not started the transaction yet and no errors have occurred, then start it
    # now. Preserve the current action if any.
    
    elsif ( $edt->can_proceed )
    {
	my $save = $edt->{current_action};
	
	$edt->{current_action} = undef;
	
	# If the transaction is successfully started, call the
	# 'initialize_transaction' method. If an exception occurs, add an error
	# condition and write the exception to the error stream.
	
	if ( $edt->_start_transaction )
	{
	    $edt->_call_initialize;
	}
	
	# If any error conditions have accumulated, mark this transaction as 'failed'. If
	# a database transaction was actually started, roll it back.
	
	if ( $edt->has_errors )
	{
	    $edt->{transaction} = 'failed';
	    
	    if ( $edt->has_started )
	    {    
		$edt->_call_cleanup('errors');
	    }
	    
	    my ($in_transaction) = $edt->{dbh}->selectrow_array('SELECT @@in_transaction');
	    
	    if ( $in_transaction )
	    {
		$edt->_rollback_transaction('errors');
	    }
	}
	
	# Restore the current action, if any.
	
	$edt->{current_action} = $save;
    }
    
    # Return true if the transaction can proceed, false otherwise.
    
    return $edt->can_proceed;
}


# start_execution ( )
# 
# Start the database transaction associated with this EditTransaction object, if it has not
# already been started. Then turn on execution and immediately execute every pending
# action. Return true if the transaction can proceed, false otherwise.

sub start_execution {
    
    my ($edt) = @_;
    
    # If this transaction has already finished, return false.
    
    if ( $edt->has_finished )
    {
	return '';
    }
    
    # If the transaction can proceed but has not yet been started, start it now. Then turn on
    # execution and execute all pending actions.
    
    if ( $edt->can_proceed )
    {
	$edt->start_transaction unless $edt->has_started;
	$edt->{execution} = 'active';
	$edt->execute_action_list;
    }
    
    return $edt->can_proceed;
}


# pause_execution ( )
#
# Pause execution for this transaction. Any subsequent actions will remain unexecuted until either
# 'start_execution', 'execute', or 'commit' is called. Return true if the transaction can proceed,
# false otherwise.

sub pause_execution {

    my ($edt) = @_;

    $edt->{execution} = '';
    return $edt->can_proceed;
}


# execute ( )
# 
# Start a database transaction, if one has not already been started. Then execute all of the
# pending actions. Returns a true value if the transaction can proceed, false otherwise. This
# differs from 'start_execution' only in that execution is not turned on for subsequent actions.

sub execute {
    
    my ($edt) = @_;
    
    # If this transaction can proceed, start the database transaction if it hasn't already been
    # started. Then run through the action list and execute any actions that are pending.
    
    if ( $edt->can_proceed )
    {
	$edt->start_transaction unless $edt->has_started;
	$edt->execute_action_list;
    }

    return $edt->can_proceed;
}


# commit ( )
# 
# Execute all pending actions and commit the transaction. Return true if the commit was
# successful, false otherwise.

sub commit {
    
    my ($edt) = @_;
    
    # If the transaction has already finished, return true if it has committed and false
    # otherwise.
    
    if ( $edt->has_finished )
    {
	return $edt->has_committed;
    }
    
    # If there are no actions and no errors and the transaction has not started,
    # dispose of it now. If there are no errors, mark it as committed and return
    # true. Otherwise, mark it as aborted and return false.
    
    elsif ( ! $edt->has_started && ( ! $edt->{action_list} || $edt->{action_list}->@* == 0 ) )
    {
	if ( $edt->can_proceed )
	{
	    $edt->{transaction} = 'aborted';
	    $edt->{has_finished} = 1;
	    return 1;
	}
	
	else
	{
	    $edt->{transaction} = 'failed';
	    $edt->{has_finished} = 1;
	    return '';
	}
    }
    
    # If this transaction can proceed, start the database transaction if it hasn't already been
    # started.
    
    elsif ( $edt->can_proceed )
    {
	$edt->start_transaction unless $edt->has_started;
    }
    
    # Now run through the action list. If the transaction can proceed, any
    # remaining actions will be executed. Otherwise, they will all be marked as
    # 'skipped'.
    
    $edt->execute_action_list;
    
    # Clear the current action.
    
    $edt->{current_action} = undef;
    
    # If the transaction can still proceed, call the 'finalize_transaction' method.  Otherwise,
    # call the 'cleanup_transaction' method. These do nothing by default, and are designed to be
    # overridden by subclasses.
    
    if ( $edt->can_proceed )
    {
	$edt->_call_finalize;
    }
    
    else
    {
	$edt->_call_cleanup('errors');
    }
        
    # If the transaction can proceed at this point, attempt to commit and then return the
    # result.
    
    if ( $edt->can_proceed )
    {
	$edt->{has_finished} = 1;
	return $edt->_commit_transaction;
    }
    
    # Otherwise, roll back the transaction and return false. Set the status to 'failed'.
    
    else
    {
	$edt->_rollback_transaction('errors') if $edt->has_started;
	$edt->{has_finished} = 1;
	$edt->{transaction} = 'failed';
	return '';
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
	$edt->_call_cleanup('call');
	$edt->_rollback_transaction('call');
	$edt->{has_finished} = 1;
	return 1;
    }
    
    # Otherwise, preemptively set the status to 'aborted' and return true. This will prevent
    # anything else from being done with it.
    
    else
    {
	$edt->{transaction} = 'aborted';
	$edt->{has_finished} = 1;
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
    
    # If we are in debug mode, print a line to the debugging stream
    # announcing the start of the transaction.
    
    $edt->debug_line(" >>> START TRANSACTION $edt->{unique_id}\n") if $edt->{debug_mode};
    
    # Start a new database transaction. If there is an uncommitted transaction on this
    # database connection that was not initiated by this module, it will be implicitly
    # committed. If there was an uncommitted transaction that was initiated by this
    # module, it was rolled back earlier in this subroutine.
    
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
    };
    
    # If an exception was thrown, add an error condition. Write the actual error message
    # to the error stream.
    
    if ( $@ )
    {
	$edt->error_line($@);
	
	$edt->add_condition('main', 'E_EXECUTE',
			    "an exception occurred while starting the transaction");
	
	# Mark this transaction as 'failed'. If a database transaction was actually started,
	# roll it back.
	
	$edt->{transaction} = 'failed';
	
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
	$dbh->do("COMMIT");
	$TRANSACTION_INTERLOCK{$dbh} = undef;
	
	$edt->{transaction} = 'committed';
	$edt->{commit_count}++;
    };
    
    # If an exception was thrown by the COMMIT statement, print the exception to the error
    # stream and add an error condition. If the commit was not carried out, roll back the
    # transaction and set the status to 'failed'. If debug mode is on, print an additional
    # message to the debugging stream announcing the rollback.
    
    if ( $@ )
    {
	$edt->error_line($@);
	$edt->add_condition('E_EXECUTE', 'an exception occurred while committing the transaction');

	my ($in_transaction) = $dbh->selectrow_array('SELECT @@in_transaction');

	if ( $in_transaction )
	{
	    $edt->debug_line( " <<< ROLLBACK TRANSACTION $edt->{unique_id} FROM exception\n" );
	    
	    $dbh->do("ROLLBACK");
	    
	    eval {
		$edt->{transaction} = 'failed';
		$edt->{rollback_count}++;
	    };
	    
	    return '';
	}
    }

    # If we get here, we know that the commit succeeded.
    
    return 1;
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
	$edt->{rollback_count}++;
    };
    
    # If an exception was thrown by the ROLLBACK statement, print the exception to the error
    # stream and add an error condition.
    
    if ( $@ && $edt->{transaction} ne 'aborted' )
    {
	$edt->error_line($@);
	$edt->add_condition('E_EXECUTE', 'an exception occurred while rolling back the transaction');
	$edt->{transaction} = 'aborted';
    }
    
    # If the reason for the rollback was 'errors', correct the transaction status to 'failed'.
    
    if ( $reason eq 'errors' )
    {
	$edt->{transaction} = 'failed';
    }
    
    return 1;
}


sub _call_initialize {
    
    my ($edt) = @_;
    
    local($@);
    
    eval {
	$edt->initialize_transaction($edt->{default_table});
    };
    
    if ( $@ )
    {
	$edt->error_line($@);
	
	$edt->add_condition('main', 'E_EXECUTE', 
			    "an exception occurred during initialize_transaction");
    }
}


sub _call_finalize {
    
    my ($edt) = @_;
    
    local($@);
    
    eval {
	$edt->finalize_transaction($edt->{default_table});
    };
    
    if ( $@ )
    {
	$edt->error_line($@);
	
	$edt->add_condition('main', 'E_EXECUTE', 
			    "an exception occurred during finalize_transaction");
    }
}


sub _call_cleanup {
    
    my ($edt, $reason) = @_;
    
    local($@);
    
    eval {
	$edt->cleanup_transaction($edt->{default_table}, $reason);
    };
    
    if ( $@ )
    {
	$edt->error_line($@);
	
	$edt->add_condition('main', 'E_EXECUTE', 
			    "an exception occurred during cleanup_transaction");
    }
}


# Methods to be overridden
# ------------------------

# The following methods do nothing, and exist solely to be overridden by subclasses. This enables
# subclasses to execute auxiliary database operations before and/or after actions and
# transactions.


# initialize_instance ( )
# 
# This method is called on each new instance.

sub initialize_instance {
    
    my ($edt) = @_;
}


# initialize_transaction ( table_specifier )
#
# This method is passed the name that was designated as the "main table" for this transaction. The
# method is designed to be overridden by subclasses, so that any necessary work can be carried out
# at the beginning of the transaction. The default method defined here does nothing.

sub initialize_transaction {
    
    my ($edt, $table_specifier) = @_;
}


# finalize_transaction ( table )
#
# This method is called at the end of every successful transaction. It is passed the name that was
# designated as the "main table" for this transaction. The method is designed to be overridden by
# subclasses, so that any necessary work can be carried out at the end of the transaction.

sub finalize_transaction {

    my ($edt, $table_specifier) = @_;
}


# cleanup_transaction ( table_specifier )
# 
# This method is called instead of 'finalize_transaction' if the transaction is to be rolled back
# instead of being committed. The method is designed to be overridden by subclasses, so that any
# necessary work can be carried out to clean up before the transaction is rolled back.

sub cleanup_transaction {

    my ($edt, $table_specifier) = @_;
}


# before_action ( action, operation, table_specifier )
# 
# This method is called before each action. It is designed to be overridden by subclasses, so that
# any necessary auxiliary work can be carried out. The default method defined here does nothing.
# 
# If any changes to the database are made by this method, you should be careful
# to provide a 'cleanup_action' method that undoes them. The reason is that if
# the action fails to execute and the PROCEED allowance is present, the
# transaction as a whole may complete and preserve whatever was done by this
# method even though the action it was designed to initialize was not carried
# out.

sub before_action {

    my ($edt, $action, $operation, $table_specifier) = @_;
}


# after_action ( action, operation, table_specifier, result )
# 
# This method is called after each successfully completed action. It is designed to be overridden
# by subclasses, so that any necessary auxiliary work can be carried out. The default method
# defined here does nothing.
# 
# For insert actions, the parameter $result will get the primary key value of
# the newly inserted record. For update, replace, delete, and do_sql actions,
# $result will get the result of the database statement that was executed. For
# other actions, it will get the result returned by the action method.

sub after_action {

    my ($edt, $action, $operation, $table_specifier, $result) = @_;
}


# cleanup_action ( action, operation, table_specifier )
# 
# This method is called after each action that fails to execute. It is designed
# to be overridden by subclasses, so that any changes to the database made by
# 'before_action' can be reversed if necessary.

sub cleanup_action {
    
    my ($edt, $action, $operation, $table_specifier);
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

    return unless $edt->{action_tables};
    return keys $edt->{action_tables}->%*;
}


sub deleted_keys {

    my ($edt, $table_key) = @_;
    return $edt->_result_keys('delete', $table_key);
}


sub inserted_keys {

    my ($edt, $table_key) = @_;
    return $edt->_result_keys('insert', $table_key);
}


sub updated_keys {

    my ($edt, $table_key) = @_;
    return $edt->_result_keys('update', $table_key);
}


sub replaced_keys {

    my ($edt, $table_key) = @_;
    return $edt->_result_keys('replace', $table_key);
}


sub other_keys {

    my ($edt, $table_key) = @_;
    return $edt->_result_keys('other', $table_key);
}


sub superior_keys {

    my ($edt, $table_key) = @_;
    # return $edt->_result_keys('superior_keys', $table_key);
}


sub failed_keys {

    my ($edt, $table_key) = @_;
    return $edt->_result_keys('failed', $table_key);
}


sub _result_keys {
    
    my ($edt, $type, $table_key) = @_;

    # Scan through the action list, collecting all of the keys associated with
    # successfully executed actions of the requested type.
    
    my @result_list;
    my $result_count = 0;

  ACTION:
    foreach my $action ( $edt->{action_list}->@* )
    {
	next unless $action;
	
	my $status = $action->status;
	
	# Based on the requested type, skip actions that do not match according to
	# operation and status.
	
	if ( $type ne 'failed' )
	{
	    next ACTION unless $status eq 'executed';
	    next ACTION unless $type eq 'action' || $type eq $action->operation;
	}

	else
	{
	    next ACTION if $status eq 'executed' && $status ne '';
	}
	
	# If keys associated with a particular table were requested, skip actions
	# associated with other tables.
	
	if ( $table_key )
	{
	    next ACTION if $table_key ne $action->table;
	}
	
	# For selected actions, accumulate either the keys themselves or the count
	# depending on context.
	
	if ( wantarray )
	{
	    push @result_list, $action->keyvalues;
	}

	else
	{
	    $result_count += scalar($action->keyvalues);
	}
    }

    if ( wantarray )
    {
	return @result_list;
    }

    else
    {
	return $result_count;
    }
}


# sub _result_keys {
    
#     my ($edt, $type, $table_key) = @_;
    
#     return unless $edt->{$type};
#     return unless $edt->{action_tables} || $table_key;
    
#     if ( wantarray && $table_key )
#     {
# 	return $edt->{$type}{$table_key} ? $edt->{$type}{$table_key}->@* : ( );
#     }
    
#     elsif ( wantarray )
#     {
# 	my $mt = $edt->{default_table};
# 	my @tables = grep { $_ ne $mt } keys $edt->{action_tables}->%*;
# 	unshift @tables, $mt if $mt;
	
# 	return map { $edt->{$type}{$_} ? $edt->{$type}{$_}->@* : ( ) } @tables;
#     }
    
#     elsif ( $table_key )
#     {
# 	return $edt->{$type}{$table_key} ? $edt->{$type}{$table_key}->@* : 0;
#     }

#     else
#     {
# 	return reduce { $a + ($edt->{$type}{$b} ? $edt->{$type}{$b}->@* : 0) }
# 	    0, keys $edt->{action_tables}->%*;
#     }
# }


# sub action_keys {

#     my ($edt, $table_key) = @_;
    
#     return unless $edt->{action_tables} || $table_key;
    
#     my @types = qw(deleted_keys inserted_keys updated_keys replaced_keys other_keys);
    
#     if ( wantarray && $table_key )
#     {
# 	return map { $edt->{$_} && $edt->{$_}{$table_key} ? $edt->{$_}{$table_key}->@* : ( ) } @types;
#     }
    
#     elsif ( wantarray )
#     {
# 	my $mt = $edt->{default_table};
# 	my @tables = $mt, grep { $_ ne $mt } keys $edt->{action_tables}->%*;
	
# 	return map { $edt->_keys_by_table($_) } @tables;
#     }
    
#     elsif ( $table_key )
#     {
# 	return reduce { $a + ($edt->{$b}{$table_key} ? $edt->{$b}{$table_key}->@* : 0) } 0, @types;
#     }
    
#     else
#     {
# 	return reduce { $a + $edt->_keys_by_table($b) } 0, keys $edt->{action_tables}->%*;
#     }
# }


# sub _keys_by_table {

#     my ($edt, $table_key) = @_;
    
#     my @types = qw(deleted_keys inserted_keys updated_keys replaced_keys other_keys);
    
#     if ( wantarray )
#     {
# 	return map { $edt->{$_}{$table_key} ? $edt->{$_}{$table_key}->@* : ( ) } @types;
#     }

#     else
#     {
# 	return reduce { $a + ($edt->{$b}{$table_key} ? $edt->{$b}{$table_key}->@* : 0) } 0, @types;
#     }
# }


# sub count_superior_key {

#     my ($edt, $table_key, $keyval) = @_;
    
#     if ( $keyval =~ /^@/ )
#     {
# 	if ( my $action = $edt->{action_ref}{$keyval} )
# 	{
# 	    if ( $keyval = $action->keyval )
# 	    {
# 		$edt->{superior_keys}{$table_key}{$keyval} = 1;
# 	    }
# 	}
#     }
    
#     elsif ( $keyval )
#     {
# 	$edt->{superior_keys}{$table_key}{$keyval} = 1;
#     }
# }


sub key_labels {

    my ($edt, $table_key) = @_;

    if ( $table_key )
    {
	return $_[0]->{key_labels}{$table_key} if $table_key && $_[0]->{key_labels}{$table_key};
    }
    
    else
    {
	return $_[0]->{key_labels};
    }
}


# sub label_keys {

#     return $_[0]{label_keys};
# }


# sub label_key {

#     return $_[0]{label_keys}{$_[1]};
# }


# sub label_table {

#     return $_[0]{label_found}{$_[1]};
# }


sub action_count {
    
    return ref $_[0]{action_list} eq 'ARRAY' ? scalar($_[0]{action_list}->@*) : 0;
}


sub record_count {

    return $_[0]{record_count} || 0;
}


sub exec_count {
    
    return $_[0]{exec_count} || 0;
}


sub fail_count {
    
    return $_[0]{fail_count} || 0;
}


sub skip_count {

    return $_[0]{skip_count} || 0;
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
