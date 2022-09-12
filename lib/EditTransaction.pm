# 
# EditTransaction project
# 
#   EditTransaction.pm - base class for data acquisition and modification
# 

use strict;

package EditTransaction;

use Carp qw(carp croak);
use Scalar::Util qw(weaken blessed reftype);
use List::Util qw(reduce any);
use Switch::Plain;

use feature 'unicode_strings', 'postderef';

use parent 'Exporter';

our (@EXPORT_OK) = qw(%ALLOW_BY_CLASS %ALLOW_ALIAS %CONDITION_BY_CLASS);

use EditTransaction::Action;
use EditTransaction::Datalog;

use Role::Tiny::With;

with 'EditTransaction::Conditions';
with 'EditTransaction::Authorization';
with 'EditTransaction::Validation';
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

our (%CONDITION_BY_CLASS) = ( EditTransaction => {		     
		C_CREATE => "Allow 'CREATE' to create records",
		C_LOCKED => "Allow 'LOCKED' to update locked records",
    		C_NO_RECORDS => "Allow 'NO_RECORDS' to allow transactions with no records",
		C_ALTER_TRAIL => "Allow 'ALTER_TRAIL' to explicitly set crmod and authent fields",
		C_CHANGE_PARENT => "Allow 'CHANGE_PARENT' to allow subordinate link values to be changed",
		E_BAD_CONNECTION => ["&1", "Database connection failed"],
		E_BAD_TABLE => "'&1' does not correspond to any known database table",
		E_NO_KEY => "The &1 operation requires a primary key value",
		E_HAS_KEY => "You may not specify a primary key value for the &1 operation",
		E_MULTI_KEY => "You may only specify a single primary key value for the &1 operation",
		E_BAD_KEY => ["Field '&1': Invalid key value(s): &2",
			      "Invalid key value(s): &2",
			      "Invalid key value(s): &1"],
		E_BAD_SELECTOR => ["Field '&1': &2", "&2"],
		E_BAD_REFERENCE => { _multiple_ => ["Field '&2': found multiple keys for '&3'",
						    "Found multiple keys for '&3'",
						    "Found multiple keys for '&2'"],
				     _unresolved_ => ["Field '&2': no key value found for '&3'",
						      "No key value found for '&3'",
						      "No key value found for '&2'"],
				     _mismatch_ => ["Field '&2': the reference '&3' has the wrong type",
						    "Reference '&3' has the wrong type",
						    "Reference '&2' has the wrong type"],
				     default => ["Field '&1': no record with the proper type matches '&2'",
						 "No record with the proper type matches '&2'",
						 "No record with the proper type matches '&1'"] },
		E_NOT_FOUND => ["No record was found with key '&1'", 
				"No record was found with this key"],
		E_LOCKED => { multiple => ["Found &2 locked record(s)",
					   "One or more of these records is locked"],
			      default => "This record is locked" },
		E_PERM_LOCK => { _multiple_ => 
				["You do not have permission to lock/unlock &2 of these records",
				 "You do not have permission to lock/unlock one or more of these records"],
				 default => "You do not have permission to lock/unlock this record" },
		E_PERM => { insert => "You do not have permission to insert a record into this table",
			    update => "You do not have permission to update this record",
			    update_many => "You do not have permission to update records in this table",
			    replace_new => "No record was found with key '&2', ".
				"and you do not have permission to insert one",
			    replace_existing => "You do not have permission to replace this record",
			    delete => "You do not have permission to delete this record",
			    delete_many => "You do not have permission to delete records from this table",
			    delete_cleanup => "You do not have permission to delete these records",
			    fixup_mode => "You do not have permission for fixup mode on this table",
			    default => "You do not have permission for this operation" },
		E_BAD_OPERATION => ["Invalid operation '&1'", "Invalid operation"],
		E_BAD_RECORD => "",
		E_BAD_CONDITION => "&1 '&2'",
		E_PERM_COL => "You do not have permission to set the value of '&1'",
		E_REQUIRED => "Field '&1': must have a nonempty value",
		E_RANGE => ["Field '&1': &2", "&2", "Field '&1'"],
		E_WIDTH => ["Field '&1': &2", "&2", "Field '&1'"],
		E_FORMAT => ["Field '&1': &2", "&2", "Field '&1'"],
		E_EXTID => ["Field '&1': &2", "Field '&1': bad external identifier",
			    "Bad external identifier"],
			    # "Field '&1': external identifier must be of type '&2'",
			    # "External identifier must be of type '&2', was '&3'",
			    # "External identifier must be of type '&2'",
			    # "No external identifier type is defined for field '&1'"],
		E_PARAM => "",
  		E_EXECUTE => ["&1", "Unknown"],
		E_DUPLICATE => "Duplicate entry '&1' for key '&2'",
		E_BAD_FIELD => "Field '&1' does not correspond to any column in '&2'",
		E_UNRECOGNIZED => "This record not match any record type accepted by this operation",
		E_IMPORTED => "",
		W_BAD_ALLOWANCE => "Unknown allowance '&1'",
		W_EXECUTE => ["&1", "Unknown"],
		W_UNCHANGED => "",
		W_NOT_FOUND => "",
		W_PARAM => "",
		W_TRUNC => ["Field '&1': &2", "Field '&1'"],
		W_EXTID => ["Field '&1' : &2", 
			    "Field '&1': column does not accept external identifiers, value looks like one"],
		W_BAD_FIELD => "Field '&1' does not correspond to any column in '&2'",
		W_EMPTY_RECORD => "Item is empty",
		W_IMPORTED => "",
		UNKNOWN => "Unknown condition code" });

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


# Interfaces with other modules
# -----------------------------

our (%DIRECTIVES_BY_CLASS);


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
    
    my ($class, $dbh_arg, $options) = @_;
    
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
    
    if ( ref $options eq 'HASH' )
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
	
	unless ( $dbh_class =~ /DBI::/ )
	{
	    $edt->add_condition('main', 'E_BAD_CONNECTION', 
				"'get_connection' failed to return a database handle");
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
    
    if ( $edt->{dbh} )
    {
	my $result = $edt->validate_dbh($edt->{dbh});
	
	if ( $result ne 'ok' )
	{
	    $edt->add_condition('main', 'E_BAD_CONNECTION', $result);
	    $edt->{dbh} = undef;
	}
    }
    
    # If a table specifier is given, its value must correspond to a known table.
    
    if ( $table_specifier && ! $edt->table_info_ref($table_specifier) )
    {
	$edt->add_condition('main', 'E_BAD_TABLE', $table_specifier);
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
    
    # If IMMEDIATE_MODE was specified, then immediately start a new transaction and turn on
    # execution. The same effect can be provided by calling the method 'start_execution' on this
    # new object.
    
    if ( $edt->{allows}{IMMEDIATE_MODE} )
    {
	$edt->_start_transaction;
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
    
    # Roll back the transaction if it is still active.
    
    if ( $edt->is_active )
    {
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

    return $_[0]{transaction} && $_[0]{transaction} ne 'active';
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
    
    if ( ref $_[0] && ($_[0]->{errlog_mode} || $_[0]->{debug_mode}) )
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


# inherit_from ( from_class )
# 
# Copy all of the allowances and conditions from the specified class to this one. This method is
# designed to be called at startup from subclasses of EditTransaction.

sub copy_allowances_from {
    
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

    if ( @_ > 1 )
    {
	return ref $_[0]{allows} eq 'HASH' && defined $_[1] && $_[0]{allows}{$_[1]};
    }
    
    else
    {
	return ref $_[0]{allows} eq 'HASH' ? keys $_[0]{allows}->%* : ();
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
# action later. Action references are always prefixed with '&'. All actions labeled or unlabeled
# may also be referenced as '&#n', where n=1 for the first action and increments sequentially.
# 
# Each action acts on a particular database table, though it is possible that the SQL command(s)
# responsible for executing it may reference other tables as well. The possible operations are:
#
#   insert            Insert a new record into the specified database table.
#
#   update            Update an existing record in the specified database table.
#
#   replace           Replace an existing record in the specified database table with
#                     a new one that has the same primary key.
#
#   delete            Delete an existing record from the specified database table. For
#                     this operation only, the record may be a primary key value instead
#                     of a hashref.
#   
#   delete_cleanup    Delete all records in the specified database table that match a
#                     specified SQL expression, except for those records that were
#                     inserted, replaced, or updated during the current transaction.
#                     This can be used to replace an entire set of records with the
#                     ones specified in this transaction.
# 
# The table must be specified as a table reference from the Tables module rather than as an actual
# table name.


# new_action ( table, operation, record )
# 
# Generate a new action object that will act on the specified table using the specified operation
# and the parameters contained in the specified input record. The record which must be a hashref,
# unless the operation is 'delete' in which case it may be a single primary key value or a
# comma-separated list of them.

sub new_action {
    
    my ($edt, $operation, $table_specifier, $parameters) = @_;
    
    # If this transaction has already finished, throw an exception. Client code should never try
    # to execute operations on a transaction that has already committed or been rolled back.
    
    croak "this transaction has already finished" if $edt->has_finished;
    
    # Check the arguments, unless the operation is 'skip'.
    
    if ( $operation && $operation ne 'skip' )
    {
	# Check for a valid table name. If the table specifier looks like a
	# record or a list of keys, use the default table for this transaction
	# (if any). But if there is an additional argument, flag the table
	# specifier as invalid.
	
	if ( ref $table_specifier || ( $operation eq 'delete' && $table_specifier =~ /^\d/ ) )
	{
	    if ( $parameters )
	    {
		croak "invalid table '$table_specifier'";
	    }
	    
	    else
	    {
		$parameters = $table_specifier;
		$table_specifier = $edt->{default_table};
	    }
	}
	
	else
	{
	    $table_specifier ||= $edt->{default_table};
	}
	
	# Catch an empty table specifier without any default.
	
	croak "you must specify a table name" 
	    unless $table_specifier;
	
	# Check for operation parameters
	
	if ( $operation =~ /^delete/ )
	{
	    # The delete operations also accept a key value or a list of key values as a scalar or an
	    # array. In that case, create a hashref to hold them.
	    
	    croak "you must provide another argument containing key values"
		unless defined $parameters;
	    
	    unless ( ref $parameters && reftype $parameters eq 'HASH' )
	    {
		$parameters = { _primary => $parameters };
	    }
	}
	
	else
	{
	    croak "you must provide a hashref containing parameters for this operation"
		unless ref $parameters && reftype $parameters eq 'HASH';
	}
    }
    
    else
    {
	croak "you must specify an operation";
    }
    
    # Increment the action sequence number.
    
    $edt->{action_count}++;
    
    # Create one or more reference strings for this action. If _label is found among the input
    # parameters with a nonempty value, use both this and the sequence number as references for
    # this action. Otherwise, use just the sequence number.
    
    my (@refs, $label);
    
    if ( ref $parameters && defined $parameters->{_label} && $parameters->{_label} ne '' )
    {
	$label = $parameters->{_label};
 	push @refs, '&' . $label;
	push @refs, '&#' . $edt->{action_count};
    }
    
    else
    {
	$label = '#' . $edt->{action_count};
	push @refs, '&' . $label;
    }
    
    # Create a new action object. Add it to the action list, and store its string
    # reference(s) in the action_ref hash.
    
    my $action = EditTransaction::Action->new($edt, $table_specifier, $operation, $label, $parameters);
    
    push $edt->{action_list}->@*, $action;
    $edt->{action_tables}{$table_specifier} = 1;
    
    foreach my $k (@refs)
    {
	$edt->{action_ref}{$k} = $action;
	weaken $edt->{action_ref}{$k};
    }
    
    # If the action is not 'skip' and the table specifier is not empty, check it
    # and unpack key values.
    
    if ( $operation ne 'skip' && $table_specifier )
    {
	if ( $edt->table_info_ref($table_specifier) )
	{
	    $edt->_unpack_key_values($action, $table_specifier, $operation, $parameters);
	}
	
	else
	{
	    $edt->add_condition($action, 'E_BAD_TABLE', $table_specifier);
	}
    }
    
    # Finally, make this action the current action. This will cause the object reference
    # to be implicitly returned from this subroutine.
    
    $edt->{current_action} = $action;
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
    
    local ($_);
    
    $selector ||= 'all';
    croak "unknown selector '$selector'" unless $STATUS_SELECTOR{$selector};

    if ( $table )
    {
	return unless $edt->{action_tables}{$table};
    }
    
    return map { $edt->_action_filter($_, $selector, $table) } $edt->{action_list}->@*;
}


sub _action_filter {

    my ($edt, $action, $selector, $table) = @_;
    
    my $status = $action->status;
    my $operation = $action->operation;
    my $result;
    
    # If the transaction has errors, report any uncompleted action as 'blocked'.
    
    if ( $edt->has_errors )
    {
    	$status ||= 'blocked';
    }
    
    # Return nothing if this action does not match the selector or the table.
    
    if ( $selector eq 'all' )
    {
	return if $table && $table ne $action->table;
    }
    
    elsif ( $selector eq 'completed' && ! $status ||
	    $selector eq 'pending' && $status ||
	    $selector eq 'blocked' && $status ne 'blocked' ||
	    $selector eq 'executed' && $status ne 'executed' ||
	    $selector eq 'unexecuted' && $status =~ /^executed|^$/ ||
	    $selector eq 'failed' && $status ne 'failed' ||
	    $selector eq 'skipped' && $status !~ /^skipped|^aborted/ ||
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
	if ( $action->keymult )
	{
	    $result = { $keycol => [ $action->keyvalues ] };
	}

	else
	{
	    $result = { $keycol => $action->keyval };
	}
    }
    
    else
    {
	$result = { };
    }
    
    # Add the action reference, and delete _label if it is there.

    delete $result->{_label};
    $result->{_ref} = $action->refstring;
    
    # Add the operation and the table.
    
    $result->{_operation} = $action->operation;
    $result->{_table} = $action->table;
    
    # Add the status. If empty, it defaults to 'pending'.
    
    $result->{_status} = $status || 'pending';
    
    # if ( $status eq 'executed' )
    # {
    # 	$result->{_status} = $STATUS_LABEL{$operation} || 'executed';
    # }

    # If this action has conditions attached, add them now.
    
    my @conditions = map ref $_ eq 'ARRAY' ? $edt->condition_nolabel($_->@*) : undef,
	$action->conditions;
    
    $result->{_errwarn} = \@conditions if @conditions;
    
    return $result;
}


sub _action_list {

    return ref $_[0]{action_list} eq 'ARRAY' ? $_[0]{action_list}->@* : ();
}


# action_status ( ref )
#
# If the given action reference is defined for this transaction, return the action's status.
# Otherwise, return undefined. This method allows interface code to check whether a given
# action can be or has been executed. It can also be used to verify that an action reference is
# valid before passing it to some other method. If no reference is given, the status of the
# current action is returned.
#
# This method also has the alias 'has_action'.
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
    
    my ($edt, $arg) = @_;
    
    if ( ref $arg )
    {
	croak "not an action reference" unless ref $arg eq 'EditTransaction::Action';
	return unless any { $_ eq $arg } $edt->{action_list};
    }
    
    my $action = ref $arg ? $arg :
	defined $arg && $arg ne 'latest' ? $edt->{action_ref}{$arg} :
	$edt->{current_action};
    
    if ( $action )
    {
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
}


sub has_action {

    my ($edt, $arg) = @_;

    if ( ref $arg )
    {
	croak "not an action reference" unless ref $arg eq 'EditTransaction::Action';
	return unless any { $_ eq $arg } $edt->{action_list};
    }
    
    my $action = ref $arg ? $arg :
	defined $arg && $arg ne 'latest' ? $edt->{action_ref}{$arg} :
	$edt->{current_action};

    return $action ? 1 : undef;
}


sub action_ok {

    my ($edt, $arg) = @_;
    
    if ( ref $arg )
    {
	croak "not an action reference" unless ref $arg eq 'EditTransaction::Action';
	return unless any { $_ eq $arg } $edt->{action_list};
    }
    
    my $action = ref $arg ? $arg :
	defined $arg && $arg ne 'latest' ? $edt->{action_ref}{$arg} :
	$edt->{current_action};

    return $action && ! $action->has_errors && $action->status =~ /^executed$|^$/;
}


sub action_keyval {

    my ($edt, $arg) = @_;
    
    if ( ref $arg )
    {
	croak "not an action reference" unless ref $arg eq 'EditTransaction::Action';
	return unless any { $_ eq $arg } $edt->{action_list};
    }
    
    my $action = ref $arg ? $arg :
	defined $arg && $arg ne 'latest' ? $edt->{action_ref}{$arg} :
	$edt->{current_action};

    return $action && $action->keyval;
}


sub get_keyval {

    my ($edt) = @_;

    return $edt->{current_action} && $edt->{current_action}->keyval;
}


sub action_keyvalues {

    my ($edt, $arg) = @_;
    
    if ( ref $arg )
    {
	croak "not an action reference" unless ref $arg eq 'EditTransaction::Action';
	return unless any { $_ eq $arg } $edt->{action_list};
    }
    
    my $action = ref $arg ? $arg :
	defined $arg && $arg ne 'latest' ? $edt->{action_ref}{$arg} :
	$edt->{current_action};

    return $action ? $action->keyvalues : ();
}


sub action_keymult {

    my ($edt, $arg) = @_;

    if ( ref $arg )
    {
	croak "not an action reference" unless ref $arg eq 'EditTransaction::Action';
	return unless any { $_ eq $arg } $edt->{action_list};
    }
    
    my $action = ref $arg ? $arg :
	defined $arg && $arg ne 'latest' ? $edt->{action_ref}{$arg} :
	$edt->{current_action};

    return $action && $action->keymult;
}


sub action_table {

    my ($edt, $arg) = @_;

    if ( ref $arg )
    {
	croak "not an action reference" unless ref $arg eq 'EditTransaction::Action';
	return unless any { $_ eq $arg } $edt->{action_list};
    }
    
    my $action = ref $arg ? $arg :
	defined $arg && $arg ne 'latest' ? $edt->{action_ref}{$arg} :
	$edt->{current_action};
    
    return $action && $action->table;
}


sub action_operation {

    my ($edt, $arg) = @_;

    if ( ref $arg )
    {
	croak "not an action reference" unless ref $arg eq 'EditTransaction::Action';
	return unless any { $_ eq $arg } $edt->{action_list};
    }
    
    my $action = ref $arg ? $arg :
	defined $arg && $arg ne 'latest' ? $edt->{action_ref}{$arg} :
	$edt->{current_action};

    return $action && $action->operation;
}


sub action_internal {

    my ($edt, $arg) = @_;
    
    if ( ref $arg )
    {
	croak "not an action reference" unless ref $arg eq 'EditTransaction::Action';
	return unless any { $_ eq $arg } $edt->{action_list};
    }
    
    return ref $arg ? $arg :
	defined $arg && $arg ne 'latest' ? $edt->{action_ref}{$arg} :
	$edt->{current_action};
}


# current_action ( )
#
# Return a reference for the current action, if any.

sub current_action {

    my ($edt) = @_;
    
    return $edt->{current_action} && $edt->{current_action}->refstring;
}


# Keys
# ----
#
# Most action objects are associated with one or more primary key values which specify the
# database records being operated on. The following methods are involved in handling them.
# 

# _unpack_key_values ( action, table, operation, record )
# 
# Unpack $record, and construct a canonical list of key values. This argument can be
# either a listref or a scalar. If it is a scalar, it is assumed to contain either a
# single key value, a comma-separated list of key values, or a string of the form "<name>
# = <value>" or "<name> in (<values...>)". In the latter cases, the name must be equal to
# the primary key column for $table.
# 
# If at least one valid key value is found, and none of them are invalid, generate an SQL
# expression using $column_name that will select the corresponding records. This method
# does not check if those key values actually exist in the database. Under most
# circumstances, $column_name should be the primary key column for the table to which the
# key values will apply.
# 
# Store the key value(s) and SQL expression in the action object. If individual key values
# are found to be invalid (not empty or 0, which are ignored), add error condition(s) to
# the action.

sub _unpack_key_values {
    
    my ($edt, $action, $table_specifier, $operation, $record) = @_;
    
    # Get a full description of this table from the database, if we don't already have it. If the
    # table does not have a primary key, add an error condition and return.
    
    my $tableinfo = $edt->table_info_ref($table_specifier);
    my $key_column;
    
    unless ( $key_column = $tableinfo->{PRIMARY_KEY} )
    {
	$edt->add_condition('main', 'E_EXECUTE',
			    "could not determine the primary key for table '$table_specifier'");
	return;
    }
    
    my $columninfo = $edt->table_column_ref($table_specifier, $key_column);
    
    # If the operation is 'delete_cleanup', we require that the original table's
    # SUPERIOR_TABLE property be set. The key column name will be original table's
    # SUPERIOR_KEY if set, or else the PRIMARY_KEY of the superior table. Either way, the
    # specified column must exist in the subordinate table and must be a foreign key
    # linked to the superior table.
    
    if ( $operation eq 'delete_cleanup' )
    {
	my $sup_table_specifier = $tableinfo->{SUPERIOR_TABLE} ||
	    croak "unable to determine the superior table for '$table_specifier'";
	
	my $sup_tableinfo = $edt->table_info_ref($sup_table_specifier);
	
	my $sup_key_column = $tableinfo->{SUPERIOR_KEY} || $tableinfo->{PRIMARY_KEY} ||
	    croak "unable to determine the linking column for '$table_specifier'";
	
	# Now that we have all of the necessary information about the superior table, key values
	# will be looked up in that table instead.
	
	$table_specifier = $sup_table_specifier;
	$tableinfo = $sup_tableinfo;
	$key_column = $sup_key_column;
    }
    
    # Now look for key values. If $record is a hashref, look for primary key values among
    # its value set. They may appear under $key_column, or under some other hash key.
    
    my ($key_field, $raw_values, @key_values, @bad_values, $auth_later);
    
    if ( ref $record )
    {
	# Start by checking if the record contains a value under the key column name.
	
	if ( defined $record->{$key_column} && $record->{$key_column} ne '' )
	{
	    $key_field = $key_column;
	    $raw_values = $record->{$key_column};
	}
	
	# Otherwise, check if the record contains a value under '_primary'.
	
	elsif ( defined $record->{_primary} && $record->{_primary} ne '' )
	{
	    $key_field = '_primary';
	    $raw_values = $record->{_primary};
	}
	
	# If not, check if the key table has a 'PRIMARY_FIELD' property and if so whether
	# the record contains a value under that name.
	
	elsif ( my $alt_name = $tableinfo->{PRIMARY_FIELD} )
	{
	    if ( defined $record->{$alt_name} && $record->{$alt_name} ne '' )
	    {
		$key_field = $alt_name;
		$raw_values = $record->{$alt_name};
	    }
	}
    }
    
    # If we did not find any raw values, or if what we found was something other than a
    # nonempty string or a nonempty array, store undef as the key and '0' as the key
    # expression. This latter expression will select nothing.
    
    unless ( ref $raw_values eq 'ARRAY' && $raw_values->@* || $raw_values && ! ref $raw_values )
    {
	$action->set_keyinfo($key_column, $key_field);
	return;
    }
    
    # # Otherwise, look for particular patterns in $raw_values. If if it matches one of
    # # the expressions "<name> = <value>" or "<name> in (<values...>)", extract the value
    # # string and make sure that the column matches $key_column.
    
    # if ( ! ref $raw_values && $raw_values =~
    # 	 qr{ ^ \s* (\w+) (?: \s* = \s* | \s+ in \s* [(] ) ( [^)]* ) [)]? \s* $ }xsi )
    # {
    # 	my $check_column = $1;
    # 	$raw_values = $2;

    # 	if ( $check_column ne $key_column )
    # 	{
    # 	    $action->add_condition('E_BAD_SELECTOR', $key_field || 'unknown',
    # 				   "invalid key column '$check_column'");
    # 	}
    # }		
    
    # Now iterate through the elements of $raw_values, whether it is a listref or a
    # string. We know it must be one or the other, because of the check above. Collect all
    # of the valid elements in @key_values and the invalid ones in @bad_values.

    my $app_call = $edt->can('before_key_column');

  VALUE:
    foreach my $v ( ref $raw_values ? $raw_values->@* : split /\s*,\s*/, $raw_values )
    {
	# Skip any value that is either empty or zero.

	if ( $v )
	{
	    # A value that is an action label must be looked up. If the action is found but has no
	    # key value, authentication will have to be delayed until execution time.
	    
	    if ( $v =~ /^&./ )
	    {
		my $ref_action = $edt->{action_ref}{$v};
		
		if ( $ref_action && $ref_action->table eq $table_specifier )
		{
		    if ( $ref_action->keyvalues )
		    {
			push @key_values, $ref_action->keyvalues;
		    }
		    
		    else
		    {
			push @key_values, $v;
			$auth_later = 1;
		    }
		}
		
		else
		{
		    $action->add_condition('E_BAD_REFERENCE', $key_field, $v);
		}

		next VALUE;
	    }
	    
	    # Otherwise, the key value must be checked. If this EditTransaction includes an
	    # application role that implements 'before_key_column', call it now. If it returns an
	    # error condition, add that condition and go on to the next value.
	    
	    if ( $app_call )
	    {
		my ($result, $clean_value, $additional) =
		    $edt->before_key_column($columninfo, $operation, $v, $key_field);
		
		if ( ref $result eq 'ARRAY' || ref $additional eq 'ARRAY' )
		{
		    $edt->add_validation_error($action, $result, $additional);
		    next VALUE if ref $result;
		}

		elsif ( $result )
		{
		    $v = $clean_value;
		}
	    }
	    
	    # If the operation is 'replace', check that the key value matches the column type. If
	    # the table has the property 'ALLOW_INSERT_KEY', then this key value may be inserted
	    # into the table if it is not already there. A replace operation allows only a single
	    # key, so we have no need to check any further.
	    
	    if ( $operation eq 'replace' && $tableinfo->{ALLOW_INSERT_KEY} )
	    {
		# $$$ need to write this.
	    }

	    elsif ( $columninfo->{TypeMain} eq 'integer' && $v !~ /^\d+$/ )
	    {
		push @bad_values, $v;
	    }
	    
	    else
	    {
		push @key_values, $v;
	    }
	}
    }
    
    # If any bad values were found, add an error condition for all of them.
    
    if ( @bad_values )
    {
	my $key_string = join ',', @bad_values;
	$edt->add_condition($action, 'E_BAD_KEY', $key_field, $key_string);
    }
    
    # If the action can proceed, store the key values and key expression with the action.
    
    if ( $action->can_proceed )
    {    
	# Start with the case in which there are unresolved key references.
	
	if ( $auth_later )
	{
	    $action->set_keyinfo($key_column, $key_field, \@key_values);
	    $action->authorize_later;
	}
	
	# Next, the case in which there is only a single key value.
	
	elsif ( @key_values == 1 )
	{
	    my $key_expr = "$key_column='$key_values[0]'";
	    
	    $action->set_keyinfo($key_column, $key_field, $key_values[0], $key_expr); 
	}
	
	# Next, the case in which there are multiple key values.
	
	elsif ( @key_values > 1 )
	{
	    my $key_string = join q{','}, @key_values;
	    my $key_expr = "$key_column in ('$key_string')";
	    
	    $action->set_keyinfo($key_column, $key_field, \@key_values, $key_expr);
	}
	
	# If no key values were found, store nothing.
    }
    
    return;
}


# Column directives
# -----------------

# The following methods can be used to control how particular fields are validated. Column
# directives affect the value specified for that column, regardless of what name the value was
# specified under. The 'handle_column' method can be called from 'initialize_transaction', or else
# during class initiation. Directives can be overridden at the action level by calling the
# 'handle_column' method on the action.
# 
# The available column directives are:
# 
# ignore        The column is treated as if it did not exist. No value will be stored to it.
# 
# pass          If a value is assigned to this column, it will be passed directly to the database
#               with no validation. It is the caller's responsibility to ensure that the value
#               is consistent with the database column's type and size.
# 
# unquoted      Same as 'pass', but the value will not be quoted. This can be used to specify an
#               SQL expression as the column value.
# 
# copy          For a 'replace' action, the column value will be copied from the current table
#               row and preserved in the replacement row. For 'update', a 'column=column'
#               clause will be included so any default value 'on update' will be ignored. For
#               an 'insert' action, this directive is ignored.
# 
# validate      This will restore the default validation to this column. Any assigned value will
#               be checked against the column type and attributes.
#
# auth_creater    This column will store user identifiers indicating who created each record.
#
# auth_authorizer This column will store user identifiers indicating who authorized each record.
#
# auth_modifier   This column will store user identifiers indicating who last modified each record.
#
# ts_created    This column will hold the date/time of record creation.
#
# ts_modified   This column will hold the date/time of last modification.
#
# adm_lock       This column will indicate whether a record was locked by an administrator.
#
# own_lock       This column will indicate whether a record was locked by its owner.
#
# In the absence of any explicit directive, special columns will be assigned the special-identity
# directives based on the variable %COMMON_FIELD_SPECIAL in TableDefs.pm.


# handle_column ( class_or_instance, table_specifier, column_name, directive )
#
# Store the specified column directive with this transaction instance. If called as a class
# method, the directive will be supplied by default to every EditTransaction in this class. If the
# specified column does not exist in the specified table, the directive will have no effect.

sub handle_column {

    my ($edt, $table_specifier, $colname, $directive) = @_;
    
    croak "you must specify a table name, a column name, and a handling directive"
	unless $table_specifier && $colname && $directive;
    
    croak "unknown table '$table_specifier'" unless $edt->table_info_ref($table_specifier);
    
    croak "invalid directive '$directive'" unless $edt->has_directive($directive);
    
    # If this was called as an instance method, add the directive locally.
    
    if ( ref $edt )
    {
	# If we have not already done so, initialize this transaction with the globally specified
	# directives for this class and table.
	
	unless ( $edt->{directive}{$table_specifier} )
	{
	    $edt->{directive}{$table_specifier} = { $edt->table_directives_list($table_specifier),
						    $edt->class_directives_list($table_specifier) };
	}
	
	# Then apply the current directive.
	
	$edt->{directive}{$table_specifier}{$colname} = $directive;
    }
    
    # If this was called as a class method, apply the directive to the global directive hash for
    # this class. In this case, the parameter $edt will contain the class name.
    
    else
    {
	$DIRECTIVES_BY_CLASS{$edt}{$table_specifier}{$colname} = $directive;
    }
}


# class_directives ( class, table_specifier )
# 
# This may be called either as an instance method or as a class method. Returns a list of columns
# and directives stored in the global directive cache for the given class and table, suitable for
# assigning to a hash.

sub class_directives_list {

    my ($edt, $table_specifier) = @_;
    
    my $class = ref $edt || $edt;
    
    if ( $DIRECTIVES_BY_CLASS{$class}{$table_specifier} )
    {
	return $DIRECTIVES_BY_CLASS{$class}{$table_specifier}->%*;
    }

    else
    {
	return;
    }
}


# all_directives ( table_specifier )
# 
# The first call to this method for a given table specifier will cause the global
# directives for this table specifier to be initialized if they haven't already been, and then the
# local directives initialized if they haven't already been. Subsequent calls are very cheap.
#
# When called in list context, returns a list of columns and directives suitable for assigning to
# a hash. When called in scalar context, returns a nonzero value if there are any directives and
# zero if there are not. A call in scalar context can be used to ensure initialization.

sub all_directives {

    my ($edt, $table_specifier) = @_;

    # Make sure we have a recognized table specifier.

    croak "unknown table '$table_specifier'" unless $edt->table_info_ref($table_specifier);
    
    # Initialize the directives for this instance and table if they haven't already been
    # initialized. This may involve initializing the global directives for this class and table.
    
    unless ( $edt->{directive}{$table_specifier} )
    {
	$edt->{directive}{$table_specifier} = { $edt->table_directives_list($table_specifier),
						$edt->class_directives_list($table_specifier) };
    }
    
    # Return a list of columns and directives suitable for assigning to a hash.
    
    return $edt->{directive}{$table_specifier}->%*;
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
    
    if ( $edt->has_finished )
    {
	croak "this transaction has already finished";
    }
    
    # If we have not started the transaction yet and no errors have occurred, then start it
    # now. Preserve the current action.
    
    elsif ( $edt->can_proceed && ! $edt->has_started )
    {
	my $save = $edt->{current_action};
	
	$edt->_start_transaction;
	
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
    
    # If the transaction can proceed but has not yet been started, start it now. Then turn on
    # execution and execute all pending actions.
    
    if ( $edt->can_proceed )
    {
	$edt->_start_transaction unless $edt->has_started;
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
	$edt->_start_transaction unless $edt->has_started;
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
    
    # If there are no actions and the transaction has not started, mark it as
    # committed and return true.
    
    elsif ( ! $edt->has_started && ( ! $edt->{action_list} || $edt->{action_list}->@* == 0 ) )
    {
	$edt->{transaction} = 'committed';
	return 1;
    }
    
    # If this transaction can proceed, start the database transaction if it hasn't already been
    # started. Then run through the action list and execute any actions that are pending.
    
    elsif ( $edt->can_proceed )
    {
	$edt->_start_transaction unless $edt->has_started;
	$edt->execute_action_list;
    }    
    
    $edt->{current_action} = undef;
    
    # If the transaction can still proceed, call the 'finalize_transaction' method.  Otherwise,
    # call the 'cleanup_transaction' method. These do nothing by default, and are designed to be
    # overridden by subclasses.
    
    my $culprit;
    
    eval {
	
	if ( $edt->can_proceed )
	{
	    $culprit = 'finalized';
	    $edt->finalize_transaction($edt->{default_table});
	}
	
	else
	{
	    $culprit = 'cleaned up';
	    $edt->cleanup_transaction($edt->{default_table});
	}
    };
    
    # If an exception is thrown during either of these method calls, write it to the error stream
    # and add an error condition.
    
    if ( $@ )
    {
	$edt->error_line($@);
	$edt->add_condition('main', 'E_EXECUTE', "an exception occurred while the transaction was $culprit");
    }
    
    # If the transaction can proceed at this point, attempt to commit and then return the
    # result.
    
    if ( $edt->can_proceed )
    {
	return $edt->_commit_transaction;
    }
    
    # Otherwise, roll back the transaction and return false. Set the status to 'failed'.
    
    else
    {
	$edt->_rollback_transaction('errors') if $edt->has_started;
	$edt->{transaction} = 'failed';
	return 0;
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
	return 1;
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
	
	$edt->initialize_transaction($edt->{default_table});
    };
    
    # If an exception was thrown, add an error condition. Write the actual error message to the
    # error stream.
    
    if ( $@ )
    {
	$edt->error_line($@);
	
	my $word = $edt->{transaction} eq 'active' ? 'initializing' : 'starting';
	$edt->add_condition('main', 'E_EXECUTE', "an exception occurred while $word the transaction");

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
	    
	    return 0;
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
    
    my ($edt, $table_specifier, $record) = @_;
    
    # We cannot proceed unless we have a valid database handle.
    
    return unless $edt->{dbh};
    
    # Create a new action object to represent this insertion.
    
    my $action = $edt->new_action('insert', $table_specifier, $record);
    
    $edt->{record_count}++;
    
    $record = $action->record;
    
    # If the record includes any errors or warnings, import them as conditions.
    
    if ( my $ew = $action->record_value('_errwarn') )
    {
	$edt->import_conditions($action, $ew);
    }
    
    # We can only create records if specifically allowed. This may be specified by the user as a
    # parameter to the operation being executed, or it may be set by the operation method itself
    # if the operation is specifically designed to create records.
    
    if ( $edt->allows('CREATE') )
    {
	eval {
	    # First check to make sure we have permission to insert a record into this table. An
	    # error condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_action($action, 'insert', $table_specifier);
	    
	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
	    # class-specific checks.
	    
	    $edt->validate_action($action, 'insert', $table_specifier);
	    
	    # A record to be inserted must not have a primary key value specified for it. Records
	    # with primary key values can only be passed to 'update_record', 'replace_record', and
	    # 'delete_record'.
	    
	    if ( $action->keyval )
	    {
		$edt->add_condition($action, 'E_HAS_KEY', 'insert');
	    }
	    
	    # Then check the record to be inserted, making sure that the column values meet all of
	    # the criteria for this table. Any discrepancies will cause error and/or warning
	    # conditions to be added.
	    
	    $edt->validate_against_schema($action, 'insert', $table_specifier);
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
    
    # Handle the action and return the action reference.
    
    return $edt->_handle_action($action, 'insert');
}


# update_record ( table, record )
# 
# The specified record is to be updated in the specified table. Depending on the settings of this
# particular EditTransaction, this action may happen immediately or may be executed later. The
# record in question MUST include a primary key value, indicating which record to update.

sub update_record {
    
    my ($edt, $table_specifier, $record) = @_;
    
    # We cannot proceed unless we have a valid database handle.
    
    return unless $edt->{dbh};
    
    # Create a new action object to represent this update.
    
    my $action = $edt->new_action('update', $table_specifier, $record);
    
    $edt->{record_count}++;
    
    $record = $action->record;
    
    # If the record includes any errors or warnings, import them as conditions.
    
    if ( my $ew = $action->record_value('_errwarn') )
    {
	$edt->import_conditions($action, $ew);
    }
    
    # We can only update a record if a primary key value is specified.
    
    if ( $action->keyval )
    {
	eval {
	    # First check to make sure we have permission to update this record. An error
	    # condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_action($action, 'update', $table_specifier);
	    
	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
	    # class-specific checks and substitutions.
	    
	    $edt->validate_action($action, 'update', $table_specifier);
	    
	    # Finally, check the record to be inserted, making sure that the column values meet all of
	    # the criteria for this table. Any discrepancies will cause error and/or warning
	    # conditions to be added.
	    
	    $edt->validate_against_schema($action, 'update', $table_specifier);
	};
	
	# If a exception occurred, write the exception to the error stream and add an error
	# condition to this action.
	
	if ( $@ )
	{
	    $edt->error_line($@);
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	}
    }
    
    # If no primary key value was specified for this record, add an error condition.
    
    else
    {
	$edt->add_condition($action, 'E_NO_KEY', 'update');
    }
    
    # Handle the action and return the action reference.
    
    return $edt->_handle_action($action, 'update');
}


# # update_many ( table, selector, record )
# # 
# # All records matching the selector are to be updated in the specified table. Depending on the
# # settings of this particular EditTransaction, this action may happen immediately or may be
# # executed later. The selector may indicate a set of keys, or it may include some expression that
# # selects all matching records.

# sub update_many {
    
#     my ($edt, $table_specifier, $keyexpr, $record) = @_;

#     # Create a new action object to represent this update.
    
#     my $action = $edt->new_action($table_specifier, 'update_many', $record);
    
#     $edt->{record_count}++;
    
#     # If the record includes any errors or warnings, import them.
    
#     $edt->import_conditions($action, $record) if $record->{_errwarn} || $record->{_errors};
    
#     # As a failsafe, we only accept an empty selector or a universal selector if the key
#     # '_universal' appears in the record with a true value.
    
#     if ( ( defined $keyexpr && $keyexpr ne '' && $keyexpr ne '1' ) || $record->{_universal} )
#     {
# 	$action->set_keyexpr($keyexpr);
	
# 	eval {
# 	    # First check to make sure we have permission to carry out this operation. An
# 	    # error condition will be added if the proper permission cannot be established.
	    
# 	    $edt->authorize_action($action, 'update_many', $table_specifier);
	    
# 	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
# 	    # class-specific checks and substitutions.
	    
# 	    $edt->validate_action($action, 'update_many', $table_specifier);
	    
# 	    # The update record must not include a key value.
	    
# 	    if ( $action->keyval )
# 	    {
# 		$edt->add_condition($action, 'E_HAS_KEY', 'update_many');
# 	    }
	    
# 	    # Finally, check the record to be inserted, making sure that the column values meet all of
# 	    # the criteria for this table. Any discrepancies will cause error and/or warning
# 	    # conditions to be added.
	    
# 	    $edt->validate_against_schema($action, 'update_many', $table_specifier);
# 	};
	
# 	# If a exception occurred, write the exception to the error stream and add an error
# 	# condition to this action.
	
# 	if ( $@ )
# 	{
# 	    $edt->error_line($@);
# 	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
# 	}
#     }

#     # If a valid selector was not given, add an error condition.

#     else
#     {
# 	$edt->add_condition($action, 'E_PARAM', "invalid selection expression '$keyexpr'");
#     }
    
#     # Handle the action and return the action reference.
    
#     return $edt->_handle_action($action, 'update_many');
# }


# replace_record ( table, record )
# 
# The specified record is to be inserted into the specified table, replacing any record that may
# exist with the same primary key value. Depending on the settings of this particular EditTransaction,
# this action may happen immediately or may be executed later. The record in question MUST include
# a primary key value.

sub replace_record {
    
    my ($edt, $table_specifier, $record) = @_;
    
    # We cannot proceed unless we have a valid database handle.
    
    return unless $edt->{dbh};
    
    # Create a new action object to represent this replacement.
    
    my $action = $edt->new_action('replace', $table_specifier, $record);
    
    $edt->{record_count}++;
    
    $record = $action->record;
    
    # If the record includes any errors or warnings, import them as conditions.
    
    if ( my $ew = $action->record_value('_errwarn') )
    {
	$edt->import_conditions($action, $ew);
    }
    
    # We can only replace a record if a single key value is specified.
    
    if ( my $keyval = $action->keyval )
    {
	eval {
	    # First check to make sure we have permission to replace this record. An
	    # error condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_action($action, 'replace', $table_specifier);
	    
	    # If more than one key value was specified, add an error condition.

	    if ( ref $keyval eq 'ARRAY' )
	    {
		$edt->add_condition($action, 'E_MULTI_KEY', 'replace');
	    }
	    
	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
	    # class-specific checks and substitutions.
	    
	    $edt->validate_action($action, 'replace', $table_specifier);
	    
	    # Finally, check the record to be inserted, making sure that the column values meet all of
	    # the criteria for this table. Any discrepancies will cause error and/or warning
	    # conditions to be added.
	    
	    $edt->validate_against_schema($action, 'replace', $table_specifier);
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
    
    # Handle the action and return the action reference.
    
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

    my ($edt, $table_specifier, $record) = @_;
    
    # We cannot proceed unless we have a valid database handle.
    
    return unless $edt->{dbh};
    
    # Create a new action object to represent this deletion.
    
    my $action = $edt->new_action('delete', $table_specifier, $record);
    
    $edt->{record_count}++;
    
    $record = $action->record;
    
    # If the record includes any errors or warnings, import them as conditions.
    
    if ( ref $record eq 'HASH' && (my $ew = $action->record_value('_errwarn')) )
    {
	$edt->import_conditions($action, $ew);
    }
    
    # A record can only be deleted if a primary key value is specified.
    
    if ( my $keyval = $action->keyval )
    {
	eval {
	    # First check to make sure we have permission to delete this record. An
	    # error condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_action($action, 'delete', $table_specifier);
	    
	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
	    # class-specific checks and substitutions.
	    
	    $edt->validate_action($action, 'delete', $table_specifier);
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
    
    # Handle the action and return the action reference.
    
    return $edt->_handle_action($action, 'delete');
}


# # delete_many ( table, selector )
# # 
# # All records matching the selector are to be deleted in the specified table. Depending on the
# # settings of this particular EditTransaction, this action may happen immediately or may be
# # executed later. The selector may indicate a set of keys, or it may include some expression that
# # selects all matching records.

# sub delete_many {
    
#     my ($edt, $table_specifier, $record) = @_;
    
#     # Extract the selector from the specified record.
    
#     my $selector = $record->{_where} // $record->{where};
#     my $failsafe = $record->{_universal} // $record->{universal};
    
#     croak "you must specify a nonempty selector using the key 'where' or '_where'"
# 	unless $selector && $selector ne '';
    
#     # As a failsafe, we only accept a universal selector if the key '_universal' or 'universal' is
#     # given a true value.
    
#     unless ( $failsafe || $selector ne '1' )
#     {
# 	croak "selector '$selector' is not valid unless _universal is set to a true value";
#     }
    
#     # Create a new action object to represent this deletion.
    
#     my $action = $edt->new_action($table_specifier, 'delete_many');
    
#     $edt->{record_count}++;
    
#     if ( $selector )
#     {
# 	$action->set_keyexpr($selector);
	
# 	eval {
# 	    # First check to make sure we have permission to delete this record. An
# 	    # error condition will be added if the proper permission cannot be established.
	    
# 	    $edt->authorize_action($action, 'delete_many', $table_specifier);

# 	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
# 	    # class-specific checks and substitutions.
	    
# 	    $edt->validate_action($action, 'delete_many', $table_specifier);
# 	};
	
# 	if ( $@ )
# 	{
# 	    $edt->error_line($@);
# 	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
# 	}
#     }

#     # If a valid selector was not given, add an error condition.

#     else
#     {
# 	$edt->add_condition($action, 'E_PARAM', "invalid expression '$selector'");
#     }
    
#     # Handle the action and return the action reference.
    
#     return $edt->_handle_action($action, 'delete_many');
# }


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
    
    my ($edt, $table_specifier, $raw_keyexpr) = @_;
    
    # We cannot proceed unless we have a valid database handle.
    
    return unless $edt->{dbh};
    
    # Create a new action object to represent this operation.
    
    my $action = $edt->new_action('delete_cleanup', $table_specifier, $raw_keyexpr);
    
    # Make sure we have a non-empty selector, although we will need to do more checks on it later.
    
    if ( $action->keyval )
    {
	eval {
	    # First check to make sure we have permission to delete records in this table. An
	    # error condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_action($action, 'delete_cleanup', $table_specifier);
	};
	
	# If a exception occurred, write the exception to the error stream and add an error
	# condition to this action.
	
	if ( $@ )
	{
	    $edt->error_line($@);
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	}
	
	# If the selector does not mention the linking column, throw an exception rather than adding
	# an error condition. These are static errors that should not be occurring because of bad
	# end-user input.
	
	if ( my $linkcol = $action->linkcol )
	{
	    my $keyexpr = $action->keyexpr;
	    
	    unless ( $keyexpr =~ /\b$linkcol\b/ )
	    {
		croak "'$keyexpr' is not a valid selector, must mention $linkcol";
	    }
	}
	
	else
	{
	    croak "could not find linking column for table $table_specifier";
	}
    }

    else
    {
	$edt->add_condition($action, 'E_NO_KEY', 'delete_cleanup');
    }
    
    # Handle the action and return the action reference.
    
    return $edt->_handle_action($action, 'delete_cleanup');
}


# other_action ( table, method, record )
# 
# An action not defined by this module is to be carried out. The argument $method must be a method
# name defined in a subclass of this module, which will be called to carry out the action. The
# method name will be passed as the operation when calling subclass methods for authoriation and validation.

sub other_action {
    
    my ($edt, $table_specifier, $method, $record) = @_;
    
    # We cannot proceed unless we have a valid database handle.
    
    return unless $edt->{dbh};
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being updated.
    
    my $action = $edt->new_action('other', $table_specifier, $record);
    
    $action->set_method($method);
    
    $edt->{record_count}++;
    
    $record = $action->record;
    
    # If the record includes any errors or warnings, import them as conditions.
    
    if ( my $ew = $action->record_value('_errwarn') )
    {
	$edt->import_conditions($action, $ew);
    }
    
    # If we have a primary key value, we can authorize against that record. This may be a
    # limitation in future, but for now the action is authorized if they have 'edit' permission
    # on that record.
    
    if ( $edt->keyval )
    {
	eval {
	    # First check to make sure we have permission to update this record. An error
	    # condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_action($action, 'other', $table_specifier);
	    
	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
	    # class-specific checks and substitutions.
	    
	    $edt->validate_action($action, 'other', $table_specifier);
	};
	
	if ( $@ )
	{
	    $edt->error_line($@);
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	};
    }
    
    # If no primary key value was specified for this record, add an error condition.
    
    # $$$ At some point in the future, we may add the ability to carry out operations using table
    # rather than record authentication.
    
    else
    {
	$edt->add_condition($action, 'E_NO_KEY', 'other');
    }
    
    # Handle the action and return the action reference.
    
    return $edt->_handle_action($action, 'other');
}


# skip_record ( )
# 
# Create a placeholder action that will never be executed. This should be called for input records
# that have been determined to be invalid before they are passed to this module.

sub skip_record {

    my ($edt, $table_specifier, $record) = @_;
    
    # We cannot proceed unless we have a valid database handle.
    
    return unless $edt->{dbh};
    
    # Create the placeholder action.
    
    my $action = $edt->new_action('skip', $table_specifier, $record);
    
    $record = $action->record;
    
    # If the record includes any errors or warnings, import them as conditions.
    
    if ( my $ew = $action->record_value('_errwarn') )
    {
	$edt->import_conditions($action, $ew);
    }
    
    # If this record contains a bad operation, add the corresponding condition.
    
    if ( my $op = $action->record_value('_operation') )
    {
	if ( $op !~ /^insert$|^update$|^delete$|^replace$/ )
	{
	    $edt->add_condition($action, 'E_BAD_OPERATION', $op);
	}
    }
    
    # Handle this action, which will be a no-op.
    
    return $edt->_handle_action($action, 'skip');
}


# process_record ( table, record )
# 
# If the record contains the key '_operation', then call the method indicated by the key
# value. Otherwise, call either 'update_record' or 'insert_record' depending on whether or not the
# record contains a value for the table's primary key. This is a convenient shortcut for use by
# interface code.

sub process_record {
    
    my ($edt, $table_specifier, $record) = @_;
    
    # We cannot proceed unless we have a valid database handle.
    
    return unless $edt->{dbh};
    
    # If the table specifier is a hashref, assume that the table specifier was omitted.
    
    if ( ref $table_specifier eq 'HASH' )
    {
	$record = $table_specifier;
	$table_specifier = $edt->{default_table};
    }
    
    # If the record contains the key _skip with a true value, add a placeholder action and do not
    # process this record.
    
    if ( $record->{_skip} )
    {
	return $edt->skip_record($table_specifier, $record);
    }
    
    # If the record contains the key _operation with a nonempty value, call the corresponding
    # method.
    
    elsif ( $record->{_operation} )
    {
	if ( $record->{_operation} eq 'delete' )
	{
	    return $edt->delete_record($table_specifier, $record);
	}
	
	elsif ( $record->{_operation} eq 'replace' )
	{
	    return $edt->replace_record($table_specifier, $record);
	}

	elsif ( $record->{_operation} eq 'insert' )
	{
	    return $edt->replace_record($table_specifier, $record);
	}

	elsif ( $record->{_operation} eq 'update' )
	{
	    return $edt->update_record($table_specifier, $record);
	}
	
	else
	{
	    return $edt->skip_record($table_specifier, $record);
	}
    }
    
    # If the record contains the key _action with a nonempty value, create a special action to
    # carry out the specified operation. This will typically be a method defined in a subclass of
    # EditTransaction.
    
    elsif ( $record->{_action} )
    {
	return $edt->other_action($table_specifier, $record->{_action}, $record);
    }
    
    # Otherwise, call update_record if the record contains a value for the table's primary
    # key. Call insert_record if it does not.
    
    elsif ( $edt->get_record_key($table_specifier, $record) )
    {
	return $edt->update_record($table_specifier, $record);
    }
    
    else
    {
	return $edt->insert_record($table_specifier, $record);
    }
}


# insert_update_record ( table, record )
#
# This is a deprecated alias for process_record.

sub insert_update_record {

    my ($edt, $table_specifier, $record) = @_;
    
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
    
    # $sql =~ s{ << (\w+) >> }{ $TABLE{$1} || "_INVALID_<<$1>>" }xseg;
    
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

    # $$$ need sql statement labels. In fact, we need an SQL statement type.
    
    # my $action = EditTransaction::Action->new($edt, '<SQL>', 'other', ':#s1', $record);
    
    # $action->set_method('_execute_sql_action');
    
    # $edt->_handle_action($action, 'other');

    # return $action->label;
}


# get_record_key ( table, record )
# 
# Return the key value (if any) specified in this record. Look first to see if the table has a
# 'PRIMARY_FIELD' property. If so, check to see if we have a value for the named
# attribute. Otherwise, check to see if the table has a 'PRIMARY_KEY' property and check under
# that name as well. If no non-empty value is found, return undefined.

sub get_record_key {

    my ($edt, $table_specifier, $record) = @_;

    return unless ref $record eq 'HASH';
    
    if ( my $key_attr = get_table_property($table_specifier, 'PRIMARY_FIELD') )
    {
	if ( defined $record->{$key_attr} && $record->{$key_attr} ne '' )
	{
	    return $record->{$key_attr};
	}
    }
    
    if ( my $key_column = get_table_property($table_specifier, 'PRIMARY_KEY') )
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


# abort_action ( action_ref )
# 
# This method may be called from either 'validate_action' or 'before_action', if it is determined
# that a particular action should be skipped but the rest of the transaction should proceed. If no
# action reference is given, the most recent action is aborted if possible. It may also be called
# from client code if the action has not yet been executed, and if we are not in immediate
# execution mode.

sub abort_action {
    
    my ($edt, $arg) = @_;
    
    if ( ref $arg )
    {
	croak "not an action reference" unless ref $arg eq 'EditTransaction::Action';
	return undef unless any { $_ eq $arg } $edt->{action_list};
    }
    
    my $action = ref $arg ? $arg :
	defined $arg && $arg eq 'parent' ? $edt->{current_action}->parent || $edt->{current_action} :
	! defined $arg || $arg eq 'latest' ? $edt->{current_action} :
	$edt->{action_ref}{$arg};
    
    return unless $action;
    
    # If the action has already been aborted, return true. This makes the method idempotent.
    
    if ( $action->status eq 'aborted' )
    {
	return 1;
    }
    
    # If the transaction has not completed and the action has not been executed, this method will
    # wipe the slate clean so to speak. The action status will be set to 'aborted', and its
    # conditions will be removed from the transaction counts. This may allow the transaction to
    # proceed if it was blocked only by those errors and not any others.
    
    unless ( $edt->has_finished || $action->has_executed )
    {
	# If this action had previously failed, decrement the fail count. This method is not
	# allowed to be called on executed actions, so the executed count does not need to be
	# adjusted.
	
	$edt->{fail_count}-- if $action->status eq 'failed';
	
	# Set the action status to 'aborted' and increment the skip count.
	
	$action->set_status('aborted');
	
	$edt->{skip_count}++;
	
	# If this action has any errors or warnings, remove them from the appropriate counts. This
	# may allow the transaction to proceed if it was blocked only by these errors. The
	# conditions themselves are left in place in the action record, so they will be included
	# whenever all actions are listed.
	
	foreach my $c ( $action->conditions )
	{
	    if ( $c->[1] =~ /^[EC]/ )
	    {
		$edt->{error_count}--;
	    }
	    
	    elsif ( $c->[1] =~ /^F/ )
	    {
		$edt->{demoted_count}--;
	    }

	    elsif ( $c->[1] =~ /^W/ )
	    {
		$edt->{warning_count}--;
	    }
	}
	
	# Just in case a bug has occurred, don't let the parent counts go negative.

	$edt->{error_count} = 0 if $edt->{error_count} < 0;
	$edt->{demoted_count} = 0 if $edt->{demoted_count} < 0;
	$edt->{warning_count} = 0 if $edt->{warning_count} < 0;
	
	# If this is a child action, do the same thing for the child counts associated with its
	# parent action.
	
	$action->clear_conditions_from_parent();
	
	# Return true, because the action has successfully been aborted.
	
	return 1;
    }
    
    # Otherwise, return false.
    
    else
    {
	return 0;
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
    
    if ( $params[0] =~ /^&./ )
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
    
    my $child = $edt->new_action(@params);
    
    $parent->add_child($child);
    
    # Handle the new action.
    
    return $edt->_handle_action($child, $child->operation);
}


# _handle_action ( action )
# 
# Handle the specified action record. If errors were generated for this record, put it on the 'bad
# record' list. Otherwise, either execute it immediately or put it on the action list to be
# executed later. This method returns true if the action is successfully executed or is queued for
# later execution, false otherwise.

sub _handle_action {
    
    my ($edt, $action, $operation) = @_;
    
    # If the action has accumulated any errors, update fail_count and the action status.
    
    if ( $action->has_errors )
    {
	$edt->{fail_count}++;
	$action->set_status('failed');
    }
    
    # If the operation is 'skip', update skip_count and the action status.
    
    elsif ( $operation eq 'skip' )
    {
	$edt->{skip_count}++;
	$action->set_status('skipped');
    }

    # If fatal errors have accumulated on this transaction, update the action status.

    elsif ( $edt->{error_count} )
    {
	$action->set_status('aborted');
    }
    
    # Otherwise, the action is able to be executed. If execution is active,
    # execute it now. This will also cause any other pending actions to execute.
    
    elsif ( $edt->{execution} )
    {
	$edt->execute_action_list;
    }
    
    # Then return the action reference. This can be used to track the status of the
    # action before and after it is executed.
    
    return $action->refstring;
}


	# my $table_specifier = $action->table;
	
	# # For a multiple action, all of the failed keys are put on the list.
	
	# if ( $action->is_multiple )
	# {
	#     push @{$edt->{failed_keys}{$table_specifier}}, $action->all_keys;
	# }
	
	# elsif ( my $keyval = $action->keyval )
	# {
	#     push @{$edt->{failed_keys}{$table_specifier}}, $keyval;
	# }




    # # If there are no actions to do, and none have been done so far, and no errors have already
    # # occurred, then add C_NO_RECORDS unless the NO_RECORDS condition is allowed. This will cause
    # # the transaction to be immediately aborted.
    
    # unless ( @{$edt->{action_list}} || @{$edt->{errors}} || $edt->{exec_count} || $edt->allows('NO_RECORDS') )
    # {
    # 	$edt->add_condition(undef, 'C_NO_RECORDS');
    # }
    



# execute_action_list ( )
# 
# Execute any pending actions. This is called either by 'execute' or by 'start_execution'. If
# $complete is true, then these are all of the remaining actions for this EditTransaction.

sub execute_action_list {

    my ($edt, $arg) = @_;
    
    local ($_);
    
    # Iterate through the action list, starting after the last completed action.
    
    my $start_index = $edt->{completed_count};
    my $end_index = $edt->{action_list}->$#*;
    
  ACTION:
    foreach my $i ( $start_index .. $end_index )
    {
	my $action = $edt->{action_list}[$i];
	
	# If any errors have accumulated on this transaction, skip all remaining actions. This
	# includes any errors that may have been generated by the previous action.
	
	last ACTION unless $edt->can_proceed;
	
	# If this particular action has been executed, aborted, or skipped, then pass over it.
	
	next ACTION if $action->has_completed;
	
	# If this particular action has any errors, then skip it. We need to do this check
	# separately, because if PROCEED has been set for this transaction then any errors that
	# were generated during validation of this action will have been demoted.  But in that
	# case, $action->errors will still return true, and this action should not be executed.
	
	if ( $action->has_errors )
	{
	    $edt->{fail_count}++;
	    $action->set_status('failed');
	    next ACTION;
	}
	
	# Set the current action and the current external action, then execute the appropriate
	# handler for this action's operation. If the current action is a child action, keep track
	# of its parent.
	
	$edt->{current_action} = $action;
	
	sswitch ( $action->operation )
	{
	    case 'insert': {
		$edt->pre_execution_check($action) && $edt->execute_insert($action);
	    }
	    
	    case 'update': {
		$edt->pre_execution_check($action) && $edt->execute_update($action);
	    }
	    
	    case 'replace': {
		$edt->pre_execution_check($action) && $edt->execute_replace($action);
	    }
	    
	    case 'delete': {
		$edt->pre_execution_check($action) && $edt->execute_delete($action);
	    }
	    
	    case 'delete_cleanup' : {
		$edt->pre_execution_check($action) && $edt->execute_delete_cleanup($action);
	    }
	    
	    case 'other': {
		$edt->pre_execution_check($action) && $edt->execute_other($action);
	    }
	    
	  default: {
		$edt->add_condition($action, 'E_EXECUTE', "An error occurred while routing this action");
		$edt->error_line("execute_action_list: bad operation '$_'");
	    }
	}
    }
    
    # # If this is the completion of the EditTransaction and no fatal errors have occurred, execute
    # # any pending operations.
    
    # if ( $arg eq 'FINAL' && $edt->can_proceed )
    # {
    # 	$edt->_cleanup_pending_actions;
    # }
    
    # Update the count of completed actions. If this routine is called again, it will skip
    # over everything on the list as of now.
    
    $edt->{completed_count} = scalar($edt->{action_list}->@*);
    
    # If the current action is a child action, reset the current action to its parent. This means
    # that subsequent client calls such as 'add_condition' will affect the parent and not its
    # children.
    
    if ( my $parent_action = $edt->{current_action} && $edt->{current_action}->parent )
    {
	$edt->{current_action} = $parent_action;
    }
}


# _cleanup_pending_actions ( )
#
# If there are any pending deletes, execute them now.

sub _cleanup_pending_actions {

    my ($edt) = @_;

    if ( $edt->{pending_deletes} && $edt->{pending_deletes}->@* )
    {
	my $delete_action = pop $edt->{pending_deletes}->@*;
	$delete_action->_coalesce($edt->{pending_deletes});
	
	delete $edt->{pending_deletes};
	$edt->{current_action} = $delete_action;
	
	$edt->_execute_delete($delete_action);
    }
}


# pre_execution_check ( action, operation )
#
# This method is called immediately before each action is executed. It starts by doing any
# necessary substitutions of action references for key values. If authorization and/or validation
# are still pending, those steps are carried out now. If any errors result, the action is marked
# as 'failed'. Returns true if the action can be executed, false otherwise.

sub pre_execution_check {
    
    my ($edt, $action, $operation, $table_specifier) = @_;
    
    # If the authorization step has not been completed, do so now.
    
    my $permission = $action->permission;
    
    if ( ! $permission || $permission eq 'PENDING' )
    {
	$permission = $edt->authorize_action($action, $operation, $table_specifier, 'FINAL');
    }
    
    # If we don't have a valid permission by this point (including 'none'), something went wrong.
    
    unless ( $permission && $permission ne 'PENDING' )
    {
	$edt->add_condition($action, 'E_EXECUTE', "An error occurred while checking authorization");
	$edt->error_line("Permission for '$operation' was '$permission' at execution time");
    }
    
    # If the validation steps have not yet been completed, do so now. If the validation status is
    # 'PENDING', start by calling 'validate_action'. This only occurs if a previous call to
    # 'validate_action' put off the validation until now by calling 'validate_later'.
    
    if ( $action->validation_status eq 'PENDING' )
    {
	$edt->validate_action($action, $operation, $table_specifier, 'FINAL');
    }
    
    # If the operation is 'insert', 'update', or 'replace', we need to call
    # 'validate_against_schema' if it has not already been called. If the validation is not
    # complete afterward, something went wrong.
    
    if ( $operation =~ /insert|update|replace/ )
    {
	# We have to check the status again, because 'validate_action' might have set it to
	# 'COMPLETE'.
	
	if ( $action->validation_status eq 'PENDING' )
	{
	    $edt->validate_against_schema($action, $operation, $table_specifier, 'FINAL');
	}
	
	unless ( $action->validation_status eq 'COMPLETE' )
	{
	    my $vs = $action->validation_status;
	    $edt->add_condition($action, 'E_EXECUTE', "An error occurred while checking validation");
	    $edt->error_line("Validation for '$operation' was '$vs' at execution time");
	}
    }
    
    # If any prechecks have been defined for this action, do them now.
    
    $edt->_do_all_prechecks($action, $operation, $table_specifier);
    
    # If the action can now proceed, return true. This implies that no error conditions were added
    # during execution of this subroutine. 

    if ( $action->can_proceed )
    {
	return 1;
    }
    
    # Otherwise, return false. If the action has errors and the status isn't already 'failed',
    # then set it and increment the fail count.
    
    elsif ( $action->has_errors && $action->status ne 'failed' )
    {
	$edt->{fail_count}++;
	$action->set_status('failed');
    }
    
    return 0;
}


# _do_all_prechecks ( action, operation, table )
#
# Carry out any prechecks that have been defined for this action.

sub _do_all_prechecks {

    my ($edt, $action, $operation, $table_specifier) = @_;
    
    # Get the lists of column names and values from the action. They are returned as references,
    # so this routine can modify the contents.
    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    # Iterate through all of the prechecks, recording which columns are affected.
    
    my (%fkey_check);
    
    foreach my $check ( $action->all_prechecks )
    {
	# A 'foreign_key' precheck checks the column value against a foreign table.
	
	if ( $check->[0] eq 'foreign_key' )
	{
	    my $colname = $check->[1];
	    
	    $fkey_check{$colname} = $check;
	}
    }
    
    # Then iterate through all of the columns, checking values and modifying them if necessary.
    
    foreach my $i ( 0 .. $#$cols )
    {
	# If this column needs a 'foreign_key' check, do that.
	
	if ( $fkey_check{$cols->[$i]} )
	{
	    my $colname   = $cols->[$i];
	    my $fieldname = $fkey_check{$colname}[2];
	    my $ftable =    $fkey_check{$colname}[3];
	    my $fcolname =  $fkey_check{$colname}[4] || get_table_property($ftable, 'PRIMARY_KEY');
	    my $checkval =  $vals->[$i];
	    
	    # If the column value is still a reference, substitute it with the keyval of the
	    # corresponding action. If an error occurs, set the value to undef instead.
	    
	    if ( $checkval =~ /^&./ )
	    {
		if ( my $ref_action = $edt->{action_ref}{$checkval} )
		{
		    my $refkey = $ref_action->keyval;
		    
		    # If there are too few or too many keys, set the column value to undefined and
		    # add an appropriate error condition.

		    if ( ! $refkey )
		    {
			$checkval = undef;
			$edt->add_condition($action, 'E_BAD_REFERENCE', '_unresolved_',
					    $fieldname, $checkval);
		    }
		    
		    elsif ( ref $refkey eq 'ARRAY' )
		    {
			$checkval = undef;
			$edt->add_condition($action, 'E_BAD_REFERENCE', '_multiple_',
					    $fieldname, $checkval);
		    }

		    else
		    {
			$checkval = $refkey;
		    }
		}
		
		else
		{
		    $checkval = undef;
		    $edt->add_condition($action, 'E_BAD_REFERENCE', '_unresolved_',
					$fieldname, $checkval);
		}
	    }
	    
	    # If we have a nonempty value and it exists in the foreign table, store it back into
	    # the value list.
	    
	    if ( defined $checkval && $edt->check_key($ftable, $fcolname, $checkval) )
	    {
		$vals->[$i] = $checkval;
	    }
	    
	    # Otherwise, set the value to something that will cause an SQL error if executed. This
	    # is a failsafe, because the execution should be aborted before that point.

	    else
	    {
		$vals->[$i] = '_INVALID_';
	    }
	}
    }
}


# # _substitute_labels ( action )
# #
# # Substitute the values in the columns marked for substitution with the key value associated with
# # the corresponding labels.

# sub _substitute_labels {
    
#     my ($edt, $action) = @_;
    
#     my $needs_substitution = $action->label_sub;
#     my $columns = $action->column_list;
#     my $values = $action->value_list;
    
#     # Step through the columns, checking to see which ones have labels that must be substituted.
    
#     foreach my $index ( 0..$#$columns )
#     {
# 	my $col_name = $columns->[$index];
# 	my $raw_value = $values->[$index];
	
# 	next unless $col_name && $needs_substitution->{$col_name};
# 	next unless $raw_value && $raw_value =~ /^&./;
	
# 	# $$$ need to check that the referenced action has the proper table.
	
# 	if ( $edt->{label_ref}{$raw_value} )
# 	{
# 	    my $key_value = $edt->{label_ref}{$raw_value}->keyval;

# 	    if ( ref $key_value eq 'ARRAY' )
# 	    {
# 		$edt->add_condition($action, 'E_BAD_REFERENCE', '_multiple_', $col_name, $raw_value);
# 	    }

# 	    elsif ( $key_value )
# 	    {
# 		$values->[$index] = $key_value;
# 	    }

# 	    else
# 	    {
# 		$edt->add_condition($action, 'E_BAD_REFERENCE', '_unresolved_', $col_name, $raw_value);
# 	    }
# 	}

# 	else
# 	{
# 	    $edt->add_condition($action, 'E_BAD_REFERENCE', $col_name, $raw_value);
# 	}
#     }
# }


# # set_keyexpr ( action )
# # 
# # Generate a key expression for the specified action, that will select the particular record being
# # acted on. If the action has no key value (i.e. is an 'insert' operation) or if no key column is
# # known for this table then return '0'. The reason for returning this value is that it can be
# # substituted into an SQL 'WHERE' clause and will be syntactically correct but always false.

# sub set_keyexpr {
    
#     my ($edt, $action) = @_;
    
#     my $keycol = $action->keycol;
#     my $keyval = $action->keyval;
#     my $keyexpr;

#     # If we have already computed a key expression, even if it is 0, return it.
    
#     if ( defined($keyexpr = $action->keyexpr) )
#     {
# 	return $keyexpr;
#     }
    
#     # Otherwise, if there is no key column then the key expression is just 0.
    
#     elsif ( ! $keycol )
#     {
# 	$action->set_keyexpr('0');
# 	return '0';
#     }

#     # If we get here, then we need to compute the key expression.
    
#     if ( $action->is_multiple )
#     {
# 	my $dbh = $edt->dbh;
# 	my @keys = map { $dbh->quote($_) } $action->all_keys;
	
# 	unless ( @keys )
# 	{
# 	    $action->set_keyexpr('0');
# 	    return '0';
# 	}
	
# 	$keyexpr = "$keycol in (" . join(',', @keys) . ")";
#     }
    
#     elsif ( defined $keyval && $keyval ne '' && $keyval ne '0' )
#     {
# 	if ( $keyval =~ /^@(.*)/ )
# 	{
# 	    my $label = $1;
	    
# 	    if ( $keyval = $edt->{label_keys}{$label} )
# 	    {
# 		$action->set_keyval($keyval);
# 	    }
	    
# 	    else
# 	    {
# 		$edt->add_condition($action, 'E_LABEL_NOT_FOUND', $keycol, $label);
# 	    }
# 	}

# 	elsif ( $keyval =~ /^[0-9+]$/ )
# 	{
# 	    # do nothing
# 	}

# 	elsif ( $keyval =~ $IDRE{LOOSE} )
# 	{
# 	    my $type = $1;
# 	    my $num = $2;
	    
# 	    my $exttype = $COMMON_FIELD_IDTYPE{$keycol};
	    
# 	    if ( $exttype )
# 	    {
# 		if ( $type eq $exttype )
# 		{
# 		    $keyval = $num;
# 		}

# 		else
# 		{
# 		    $edt->add_condition($action, 'E_EXTID', $keycol, "external identifier must be of type '$exttype', was '$type'");
# 		}
# 	    }
	    
# 	    else
# 	    {
# 		$edt->add_condition($action, 'E_EXTID', $keycol, "no external identifier is defined for this primary key");
# 	    }
# 	}
	
# 	$keyexpr = "$keycol=" . $edt->dbh->quote($keyval);
#     }
    
#     if ( $keyexpr )
#     {
# 	$action->set_keyexpr($keyexpr);
# 	return $keyexpr;
#     }

#     else
#     {
# 	$action->set_keyexpr('0');
# 	return '0';
#     }
# }


# # get_keyexpr ( action )
# #
# # If the action already has a key expression, return it. Otherwise, generate one and return it.

# sub get_keyexpr {
    
#     my ($edt, $action) = @_;

#     if ( my $keyexpr = $action->keyexpr )
#     {
# 	return $keyexpr;
#     }

#     else
#     {
# 	return $edt->set_keyexpr($action);
#     }
# }


# # aux_keyexpr ( table, keycol, keyval )
# #
# # Generate a key expression that will select the indicated record from the table.

# sub aux_keyexpr {
    
#     my ($edt, $action, $table_key, $keycol, $keyval, $record_col) = @_;

#     if ( $action )
#     {
# 	$table_key ||= $action->table;
# 	$keycol ||= $action->keycol;
# 	$keyval ||= $action->keyvalues;
# 	$record_col ||= $action->keyrec;
#     }
    
#     # If we are given an empty key value, or '0', then we cannot generate a key expression. Add an
#     # E_REQUIRED error condition to the action, and return.
    
#     if ( ! defined $keyval || $keyval eq '0' || $keyval eq '' )
#     {
# 	$edt->add_condition($action, 'E_REQUIRED', $record_col) if $action;
# 	return 0;
#     }

#     # If we are given a positive integer value, we can use that directly.
    
#     elsif ( $keyval =~ /^[0-9]+$/ && $keyval > 0 )
#     {
# 	return "$keycol='$keyval'";
#     }

#     # If we are given a label, we need to look it up and find the key value to which the label
#     # refers. We also need to check to make sure that this label refers to a key value in the
#     # proper table.
    
#     elsif ( $keyval =~ /^&(.+)/ )
#     {
# 	# my $label = $1;
# 	# my $lookup_val = $edt->{label_keys}{$label};

# 	# $$$ switch to action references
	
# 	# if ( $lookup_val && $edt->{label_found}{$label} eq $table_key )
# 	# {
# 	#     return "$keycol='$lookup_val'";
# 	# }
	
# 	# else
# 	# {
# 	#     $edt->add_condition($action, 'E_LABEL_NOT_FOUND', $record_col, $label) if $action;
# 	#     return 0;
# 	# }
#     }

#     # Otherwise, check if this column supports external identifiers. If it matches the pattern for
#     # an external identifier of the proper type, then we can use the extracted numeric value to
#     # generate a key expression.
    
#     elsif ( my $exttype = $COMMON_FIELD_IDTYPE{$keycol} || get_column_property($table_key, $keycol, 'EXTID_TYPE') )
#     {
# 	# $$$ call validate_extid_value
	
# 	if ( $IDRE{$exttype} && $keyval =~ $IDRE{$exttype} )
# 	{
# 	    if ( defined $2 && $2 > 0 )
# 	    {
# 		return "$keycol='$2'";
# 	    }

# 	    else
# 	    {
# 		$edt->add_condition($action, 'E_RANGE', $record_col,
# 				    "value does not specify a valid record") if $action;
		
# 		return 0;
# 	    }
# 	}

# 	else
# 	{
# 	    $edt->add_condition($action, 'E_EXTID', $keycol);
# 	}
#     }
    
#     # Otherwise, we weren't given a valid value.
    
#     else
#     {
# 	$edt->add_condition($action, 'E_FORMAT', $record_col,
# 			    "value does not correspond to a record identifier") if $action;
# 	return 0;
#     }
# }


# # record_value ( action, table, column )
# # 
# # Return a list of two values. The first is the value from the specified action's record that
# # corresponds to the specified column in the database, or undef if no matching value found. The
# # second is the key under which that value was found in the action record.

# sub record_value {
    
#     my ($edt, $action, $table_key, $col) = @_;
    
#     # First we need to grab the schema for this table, if we don't already have it.
    
#     my $schema;
    
#     # unless ( $schema = $table_keyData::SCHEMA_CACHE{$table_key} )
#     # {
#     # 	my $dbh = $edt->dbh;
#     # 	$schema = get_table_schema($dbh, $table_key, $edt->debug);
#     # }
    
#     # If the action record is not a hashref, then we can return a value only if the operation is
#     # 'delete' and the column being asked for is the primary key. Otherwise, we must return
#     # undef. In either case, we return no second value because there is no column.
    
#     my $record = $action->{record};
    
#     unless ( ref $record eq 'HASH' )
#     {
#         if ( $action->{operation} eq 'delete' && $col eq $action->{keycol} &&
# 	     defined $record )
# 	{
# 	    return $record;
# 	}
	
# 	else
# 	{
# 	    return;
# 	}
#     }
    
#     # If the column is the key column, then we need to check both that column name and the primary
#     # attribute if any.

#     if ( $col eq $action->{keycol} && ! exists $record->{$col} )
#     {
# 	if ( my $alt = get_table_property($table_key, 'PRIMARY_FIELD') )
# 	{
# 	    return ($record->{$alt}, $alt);
# 	}
#     }
    
#     # Otherwise, we need to check for a value under the column name. If there isn't one, then we need to
#     # check the alternate name if any.

#     my $cr = $schema->{$col};

#     if ( exists $record->{$col} && ! $cr->{ALTERNATE_ONLY} )
#     {
# 	return ($record->{$col}, $col);
#     }
    
#     elsif ( my $alt = $cr->{ALTERNATE_NAME} )
#     {
# 	if ( exists $record->{$alt} )
# 	{
# 	    (return $record->{$alt}, $alt);
# 	}
#     }
    
#     # Otherwise, we must return undefined.

#     return;
# }


# # record_has_col ( action, table, column )
# #
# # This is like the previous routine, except that it returns true if the specified column is
# # mentioned in the action record, and false otherwise. The value specified for the column is not
# # checked.

# sub record_has_col {
    
#     my ($edt, $action, $table_key, $col) = @_;
    
#     # First we need to grab the schema for this table, if we don't already have it.
    
#     unless ( my $schema = $TableData::SCHEMA_CACHE{$table_key} )
#     {
# 	my $dbh = $edt->dbh;
# 	$schema = get_table_schema($dbh, $table_key, $edt->debug);
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


# fetch_old_record ( action, table, key_expr )
# 
# Fetch the old version of the specified record, if it hasn't already been fetched.

sub fetch_old_record {
    
    my ($edt, $action, $table_key, $keyexpr) = @_;
    
    if ( my $old = $action->old_record )
    {
	return $old;
    }
    
    $table_key ||= $action->table;
    $keyexpr ||= $action->keyexpr;

    if ( $table_key && $keyexpr )
    {
	return $edt->fetch_record($table_key, $keyexpr);
    }

    else
    {
	return;
    }
}


# execute_delete_cleanup ( action )
# 
# Perform a delete operation on the database. The records to be deleted are those that match the
# action selector and were not either inserted, updated, or replaced during this transaction.

sub execute_delete_cleanup {

    my ($edt, $action) = @_;
    
    # my $table_key = $action->table;
    
    # my $dbh = $edt->dbh;
    
    # my $selector = $action->selector;
    # my $keycol = $action->keycol;
    
    # # Come up with the list of keys to preserve. If there aren't any entries, add a 0 to avoid a
    # # syntax error. This will not match any records under the Paleobiology Database convention
    # # that 0 is never a valid key.
    
    # my @preserve;	# $$$ this needs to be rewritten.
    
    # push @preserve, @{$edt->{inserted_keys}{$table_key}} if ref $edt->{inserted_keys}{$table_key} eq 'ARRAY';
    # push @preserve, @{$edt->{updated_keys}{$table_key}} if ref $edt->{updated_keys}{$table_key} eq 'ARRAY';
    # push @preserve, @{$edt->{replaced_keys}{$table_key}} if ref $edt->{replaced_keys}{$table_key} eq 'ARRAY';
    
    # push @preserve, '0' unless @preserve;
    
    # my $key_list = join(',', @preserve);
    
    # my $keyexpr = "$selector and not $keycol in ($key_list)";
    
    # # Figure out which keys will be deleted, so that we can list them later.

    # # my $init_sql = "	SELECT $keycol FROM $TABLE{$table_key} WHERE $keyexpr";
    
    # # $edt->debug_line( "$init_sql\n" ) if $edt->{debug_mode};

    # # my $deleted_keys = $dbh->selectcol_arrayref($init_sql);
    
    # # If the following flag is set, deliberately generate an SQL error for
    # # testing purposes.
    
    # if ( $TEST_PROBLEM{sql_error} )
    # {
    # 	$keyexpr .= 'XXXX';
    # }
    
    # # Then construct the DELETE statement.
    
    # $action->set_keyexpr($keyexpr);
    
    # # my $sql = "	DELETE FROM $TABLE{$table_key} WHERE $keyexpr";
    
    # # $edt->debug_line( "$sql\n" ) if $edt->{debug_mode};
    
    # # Execute the statement inside a try block. If it fails, add either an error or a warning
    # # depending on whether this EditTransaction allows PROCEED.
    
    # eval {
	
    # 	# If we are logging this action, then fetch the existing record.
	
    # 	# unless ( $edt->allows('NO_LOG_MODE') || get_table_property($table_key, 'NO_LOG') )
    # 	# {
    # 	#     $edt->fetch_old_record($action, $table_key, $keyexpr);
    # 	# }
	
    # 	# Start by calling the 'before_action' method. This is designed to be overridden by
    # 	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
    # 	# default method does nothing.    
	
    # 	$edt->before_action($action, 'delete_cleanup', $table_key);
	
    # 	# Then execute the delete statement itself, provided there are no errors and the action
    # 	# has not been aborted.
	
    # 	if ( $action->can_proceed )
    # 	{
    # 	    # my $result = $dbh->do($sql);
	    
    # 	    $action->set_status('executed');
    # 	    $action->set_result($result);
    # 	    $action->_confirm_keyval($deleted_keys);   # $$$ needs to be rewritten
	    
    # 	    $edt->after_action($action, 'delete_cleanup', $table_key, $result);
    # 	}
    # };
    
    # if ( $@ )
    # {	
    # 	$edt->error_line($@);
    # 	$action->pin_errors if $action->status eq 'executed';
    # 	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
    # };
    
    # # If the SQL statement succeeded, increment the executed count.
    
    # if ( $action->has_executed )
    # {
    # 	$edt->{exec_count}++;
    # }
    
    # # Otherwise, set the action status to 'failed' unless the action was aborted before execution.
    
    # elsif ( $action->status ne 'aborted' )
    # {
    # 	$action->set_status('failed');
    # 	$edt->{fail_count}++;
    # }
    
    return;
}


# execute_other ( action )
# 
# Perform an operation other than insert, replace, update, or delete on the database. The keys
# and values have been checked previously.

sub execute_other {

    my ($edt, $action) = @_;
    
    my $table_key = $action->table;
    
    # If authorization and/or validation for this action are still pending, do those now. Also
    # complete action reference substitution. If the pre-execution check returns false, then
    # the action has failed and cannot proceed.
    
    $edt->_pre_execution_check($action, 'insert', $table_key) || return;
    
    # Determine the method to be called.
    
    my $method = $action->method;
    
    # Call the specified method inside a try block. If it fails, add either an error or
    # a warning depending on whether this EditTransaction allows PROCEED.
    
    eval {
	
	$edt->before_action($action, 'other', $table_key);

	if ( $action->can_proceed )
	{
	    my $result = $edt->$method($action, $table_key, $action->record);
	    
	    $action->set_status('executed');
	    $action->set_result($result);

	    $edt->after_action($action, 'other', $table_key, $result);
	}
    };
    
    if ( $@ )
    {
	$edt->error_line($@);
	$action->pin_errors if $action->status eq 'executed';
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
    }
    
    # If the SQL statement succeeded, increment the executed count.
    
    if ( $action->has_executed )
    {
	$edt->{exec_count}++;
    }
    
    # Otherwise, set the action status to 'failed' unless the action was aborted before execution.
    
    elsif ( $action->status ne 'aborted' )
    {
	$action->set_status('failed');
	$edt->{fail_count}++;
    }
    
    return;
}


# _execute_sql_action ( action, table, record )
#
# Execute the specified SQL statement.

sub _execute_sql_action {
    
    my ($edt, $action, $table_key, $record) = @_;
    
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
    
    my ($edt, $table_key) = @_;
    
    my $a = 1;	# We can stop here when debugging.
}


# finalize_transaction ( table )
#
# This method is called at the end of every successful transaction. It is passed the name that was
# designated as the "main table" for this transaction. The method is designed to be overridden by
# subclasses, so that any necessary work can be carried out at the end of the transaction.

sub finalize_transaction {

    my ($edt, $table_key) = @_;

    my $a = 1;	# We can stop here when debugging.
}


# cleanup_transaction ( table )
# 
# This method is called instead of 'finalize_transaction' if the transaction is to be rolled back
# instead of being committed. The method is designed to be overridden by subclasses, so that any
# necessary work can be carried out to clean up before the transaction is rolled back.

sub cleanup_transaction {

    my ($edt, $table_key) = @_;

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

    my ($edt, $action, $operation, $table_key) = @_;
    
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

    my ($edt, $action, $operation, $table_key, $result) = @_;
    
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

    return $_[0]{action_count} || 0;
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
