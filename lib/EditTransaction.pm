# 
# The Paleobiology Database
# 
#   EditTransaction.pm - base class for data acquisition and modification
# 

package EditTransaction;

use strict;

use Carp qw(carp croak);
use Scalar::Util qw(weaken blessed reftype looks_like_number);
use List::Util qw(sum max reduce any);
use Hash::Util qw(lock_hash);
use Switch::Plain;

use ExternalIdent qw(%IDP %IDRE);
use TableDefs qw(get_table_property specific_column_property %TABLE %COMMON_FIELD_IDTYPE);
use TableData qw(get_table_schema);
use Permissions;

use feature 'unicode_strings', 'postderef';

use EditTransaction::Action;
use EditTransaction::Datalog;

use Moo;

with 'EditTransaction::Validation';
# with 'EditTransaction::Internal';

use namespace::clean;

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
		SKIP_LOGGING => 1,
		IMMEDIATE_MODE => 1,
		VALIDATION_ONLY => 1 } );

our (%ALLOW_ALIAS) = ( IMMEDIATE_EXECUTION => 'IMMEDIATE_MODE' );

our (%CONDITION_BY_CLASS) = ( EditTransaction => {		     
		C_CREATE => "Allow 'CREATE' to create records",
		C_LOCKED => "Allow 'LOCKED' to update locked records",
    		C_NO_RECORDS => "Allow 'NO_RECORDS' to allow transactions with no records",
		C_ALTER_TRAIL => "Allow 'ALTER_TRAIL' to explicitly set crmod and authent fields",
		C_MOVE_SUBORDINATES => "Allow 'MOVE_SUBORDINATES' to allow subordinate link values to be changed",
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


# Interfaces with other modules
# -----------------------------

our (%DBF_PACKAGE, $DBF_NAME, $DBF_PKGNAME);
our (%APP_PACKAGE, $APP_NAME, $APP_PKGNAME);

our (%PACKAGE_HOOK, %DIRECTIVE_HOOK);

our (%DIRECTIVES_BY_CLASS);

our (%VALID_DIRECTIVE) = ( ignore => 1, pass => 1, noquote => 1, copy => 1, validate => 1 );


# CONSTRUCTOR and destructor
# --------------------------

# new ( request_or_dbh, perms, table, allows )
# 
# Create a new EditTransaction object, for use in association with the specified request. It is
# also possible to specify a DBI database connection handle, as would typically be done by a
# command-line utility. The second argument should be a Permissions object which has already been
# created, the third a table name, and the fourth a hash of allowed cautions.

sub new {
    
    my ($class, $request_or_dbh, $perms, $table_specifier, @rest) = @_;
    
    local ($_);
    
    # Check the arguments. The first two, $request_or_dby and $perms, are required.
    
    croak "new EditTransaction: request or dbh is required"
	unless $request_or_dbh && blessed($request_or_dbh);
    
    croak "new EditTransaction: permissions object is required"
	unless blessed $perms && $perms->isa('Permissions');
    
    # Now parse the arguments to extract allowances, if specified. If the third argument is
    # 'allows', no table specifier was given. If it is a listref or hashref, extract allowances
    # from it.
    
    my @allowances;
    
    if ( ref $table_specifier )
    {
	unshift @rest, $table_specifier;
	$table_specifier = undef;
    }
    
    elsif ( $table_specifier eq 'allows' )
    {
	$table_specifier = undef;
    }

    foreach my $arg ( @rest )
    {
	if ( ref $arg eq 'HASH' )
	{
	    push @allowances, $_ foreach grep { $arg->{$_} } keys $arg->%*;
	}

	elsif ( ref $arg eq 'ARRAY' )
	{
	    push @allowances, $arg->@*;
	}

	elsif ( $arg )
	{
	    push @allowances, map { split /\s*,\s*/ } $arg;
	}
    }
    
    # If a table specifier is given, its value must correspond to a known table.
    
    if ( $table_specifier )
    {
	croak "'$table_specifier' does not correspond to any known table"
	    unless $TABLE{$table_specifier}
    }
    
    # Create a new EditTransaction object, and bless it into the proper class.
    
    my $edt = { perms => $perms,
		main_table => $table_specifier || '',
		unique_id => $TRANSACTION_COUNT++,
		dbf => undef,
		app => undef,
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
    
    if ( $request_or_dbh->can('get_connection') )
    {
	$edt->{request} = $request_or_dbh;
	weaken $edt->{request};
	
	$edt->{dbh} = $request_or_dbh->get_connection;
	
	my $classcheck = ref $edt->{dbh};
	
	croak "'get_connection' failed to return a database handle"
	    unless $classcheck =~ /DBI::/;
	
	if ( $request_or_dbh->can('debug_mode') )
	{
	    $edt->{debug_mode} = $request_or_dbh->debug_mode;
	}

	elsif ( $request_or_dbh->can('debug') )
	{
	    $edt->{debug_mode} = $request_or_dbh->debug;
	}
	
	die "TEST NO CONNECT" if $TEST_PROBLEM{no_connect};
    }
    
    elsif ( ref($request_or_dbh) =~ /DBI::/ )
    {
	$edt->{dbh} = $request_or_dbh;
	
	die "TEST NO CONNECT" if $TEST_PROBLEM{no_connect};
    }
    
    else
    {
	croak "'$request_or_dbh' is not a request object or database handle";
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
    
    elsif ( $edt->{allows}{SILENT_MODE} )
    {
	$edt->{errlog_mode} = 0;
    }	
    
    # Set the database handle attributes properly.
    
    $edt->{dbh}->{RaiseError} = 1;
    $edt->{dbh}->{PrintError} = 0;
    
    # Select the DBF module and APP module that will be used by this EditTransaction instance.
    # $$$ need to add syntax for selecting from multiple registered modules.
    
    $edt->{DBF} = $DBF_PKGNAME || '';
    $edt->{APP} = $APP_PKGNAME || '';
    
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


# Registration of database and application plug-ins
# -------------------------------------------------

# The following methods can all be called as class methods. Under most circumstances they will be
# called just after the calling package has compiled.

# register_dbf ( name, package )
# 
# Register the package from which this call originates as a DBF module. A canonical name must be
# specified for this module. That name can be used by clients to select which database interface
# to use for a new EditTransaction, if more than one is DBF module is registered. If only one DBF
# module is registered, then it will be automatically used for every new EditTransaction. The
# package name need only be provided if it is different from the calling package.

sub register_dbf {

    my ($edt, $name, $package) = @_;
    
    $package ||= caller;
    
    $DBF_PACKAGE{$name} = $package;

    # If we have only a single registered DBF module, select that one.
    
    my @names = keys %DBF_PACKAGE;
    
    $DBF_NAME = @names == 1 ? $names[0] : undef;
    $DBF_PKGNAME = $DBF_NAME ? $DBF_PACKAGE{$DBF_NAME} : undef;
}


# register_dbf ( name, package )
#
# Register the package from which this call originates as an APP module. A canonical name must be
# specified for this module. That name can be used by clients to select which application
# semantics to use for a new EditTransaction, if more than one is APP module is registered. If
# only one APP module is registered, then it will be automatically used for every new
# EditTransaction. The package name need only be provided if it is different from the calling
# package.

sub register_app {

    my ($edt, $name, $package) = @_;
    
    $package ||= caller;
    
    $APP_PACKAGE{$name} = $package;
    
    # If we have only a single registered APP module, select that one.
    
    my @names = keys %APP_PACKAGE;
    
    $APP_NAME = @names == 1 ? $names[0] : undef;
    $APP_PKGNAME = $APP_NAME ? $APP_PACKAGE{$APP_NAME} : undef;
}


# register_hook ( name, ref, package )
#
# Register the specified code reference as a hook in the plug-in module (either APP or DBF) from
# which this call originates. The package name need only be provided if it is different from the
# calling package.

sub register_hook {

    my ($edt, $name, $ref, $package) = @_;
    
    $package ||= caller;
    
    $PACKAGE_HOOK{$package}{$name} = $ref;
}


sub register_directive_hook {

    my ($edt, $directive, $ref, $package) = @_;
    
    $package ||= $caller;
    
    $DIRECTIVE_HOOK{$package}{$name} = $ref;
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

    return $_[0]{transaction} ne '' && $_[0]{transaction} ne 'active';
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

    return ($_[0]{transaction} eq '' || $_[0]{transaction} eq 'active') && ! $_[0]{error_count};
}


sub app {
    
    return $_[0]->{APP} // '';
}


sub dbf {

    return $_[0]->{DBF} // '';
}


sub perms {
    
    return $_[0]->{perms} // '';
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

    if ( @_ > 1 )
    {
	return ref $_[0]{allows} eq 'HASH' && defined $_[1] && $_[0]{allows}{$_[1]};
    }

    else
    {
	return ref $_[0]{allows} eq 'HASH' && keys $_[0]{allows}->%*;
    }
}


# Plug-in module hooks
# --------------------

sub has_app_hook {

    my ($edt, $hook_name) = @_;
    
    my $package = $edt->{APP} || $APP_PKGNAME || '';
    
    return $PACKAGE_HOOK{$package}{$hook_name} || $PACKAGE_HOOK{DEFAULT}{$hook_name} || '';
}


sub call_app_hook {
    
    my ($edt, $hook_name) = @_;
    
    my $package = $edt->{APP} || $APP_PKGNAME || '';
    
    if ( my $hook = $PACKAGE_HOOK{$package}{$hook_name} || $PACKAGE_HOOK{DEFAULT}{$hook_name} )
    {
	if ( ref $hook )
	{
	    goto &$hook;
	}
    }
    
    # Otherwise
    
    return;
}


sub has_dbf_hook {

    my ($edt, $hook_name) = @_;
    
    my $package = $edt->{DBF} || $DBF_PKGNAME || '';
    
    return $PACKAGE_HOOK{$package}{$hook_name} || $PACKAGE_HOOK{DEFAULT}{$hook_name} || '';
}


sub call_dbf_hook {

    my ($edt, $hook_name) = @_;

    my $package = $edt->{DBF} || $DBF_PKGNAME || '';
    
    if ( my $hook = $PACKAGE_HOOK{$package}{$hook_name} || $PACKAGE_HOOK{DEFAULT}{$hook_name} )
    {
	if ( ref $hook )
	{
	    goto &$hook;
	}
    }
    
    # Otherwise
    
    return;
}


sub has_directive_hook {

    my ($edt, $directive) = @_;

    my $package = $edt->{APP} || $APP_PKGNAME || '';
    
    return $DIRECTIVE_HOOK{$package}{$directive} || $DIRECTIVE_HOOK{DEFAULT}{$directive} || '';
}


sub call_directive_hook {
    
    my $package = $edt->{APP} || $APP_PKGNAME || '';
    
    if ( my $hook = $PACKAGE_HOOK{$package}{$hook_name} || $PACKAGE_HOOK{DEFAULT}{$hook_name} )
    {
	if ( ref $hook )
	{
	    goto &$hook;
	}
    }
    
    # Otherwise
    
    return;
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

our ($CONDITION_CODE_STRICT) = qr{ ^ [CEW]_ [A-Z0-9_-]+ $ }x;
our ($CONDITION_CODE_LOOSE) =  qr{ ^ [CEFW]_ [A-Z0-9_-]+ $ }x;
our ($CONDITION_CODE_START) =  qr{ ^ [CEFW]_ }x;
our ($CONDITION_LINE_IMPORT) = qr{ ^ ([CEFW]_[A-Z0-9_-]+) (?: \s* [(] .*? [)] )? (?: \s* : \s* )? (.*) }x;

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
	
	# Make sure the code follows the proper pattern and the template is defined. It may be
	# the empty string, but it must be given.
	
	croak "bad condition code '$code'" unless $code =~ $CONDITION_CODE_STRICT;
	croak "bad condition template '$template'" unless defined $template;
	
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
    
    # If the first parameter starts with '&', look it up as an action reference. Calls of
    # this kind will always come from outside code.
    
    elsif ( $params[0] =~ /^&./ )
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
	    die "current_action is not a valid action reference";
	}
	
	shift @params if $params[0] eq 'latest';
    }
    
    # There must be at least one remaining parameter, and it must match the syntax of a
    # condition code. If it starts with F, change it back to E. If it does not have the
    # form of a condition code, throw an exception. Any subsequent parameters will be kept
    # and used to generate the condition message.
    
    if ( $params[0] && $params[0] =~ $CONDITION_CODE_LOOSE )
    {
	$code = shift @params;

	if ( $code =~ /^F/ )
	{
	    substr($code, 0, 1) = 'E';
	}
    }
    
    else
    {
	croak "'$params[0]' is not a valid selector or condition code";
    }
    
    # If this condition belongs to an action, add it to that action. Adjust the condition counts
    # for the transaction, but only if the action is not marked as skipped. If the action already
    # has this exact condition, return without doing anything.
    
    if ( $action )
    {
	# When an error condition is attached to an action and this transaction allows
	# PROCEED, the error is demoted to a warning. If this transaction allows NOT_FOUND,
	# then an E_NOT_FOUND error is demoted to a warning but others are not.
	
	if ( $action && $code =~ /^E/ && ref $edt->{allows} eq 'HASH' )
	{
	    if ( $edt->{allows}{PROCEED} ||
		 $edt->{allows}{NOT_FOUND} && $code eq 'E_NOT_FOUND' ||
		 $edt->{allows}{NOT_PERMITTED} && $code eq 'E_PERM' )
	    {
		substr($code, 0, 1) = 'F';
	    }
	}

	# Try to add this condition to the action. If add_condition fails, that means the
	# condition duplicates one that is already attached to the action. If it succeeds,
	# then update the transaction condition counts unless this is a skipped action.
	
	if ( $action->add_condition($code, @params) && $action->status ne 'skipped' )
	{
	    # This code in this block includes guard statements that reset any invalid
	    # counts to zero before incrementing them. If the guard statement triggers, it
	    # means that any prior count has already been lost. So we start again from zero,
	    # rather than losing the count anyway or throwing an exception that will probably
	    # not be caught.

	    $edt->{condition_code}{$code}++;
	    
	    # If the code starts with E or C then it represents an error or caution.
	    
	    if ( $code =~ /^[EC]/ )
	    {
		$edt->{error_count} = 0 unless looks_like_number $edt->{error_count};
		$edt->{error_count}++;
	    }
	    
	    # If the code starts with F, then it represents a demoted error. It counts as a
	    # warning for the transaction as a whole, but as an error for the action.
	    
	    elsif ( $code =~ /^F/ )
	    {
		$edt->{demoted_count} = 0 unless looks_like_number $edt->{demoted_count};
		$edt->{demoted_count}++;
	    }
	    
	    # Otherwise, it represents a warning.
	    
	    else
	    {
		$edt->{warning_count} = 0 unless looks_like_number $edt->{warning_count};
		$edt->{warning_count}++;
	    }
	    
	    # Return true to indicate that the condition was attached.
	    
	    return 1;
	}
    }
    
    # Otherwise, the condition is to be attached to the transaction as a whole unless it
    # duplicates one that is already there. If the transaction already has this exact
    # condition, return without doing anything. Use the same kind of guard statements as
    # above, and also on the condition list.
    
    elsif ( ! $edt->_has_main_condition($code, @params) )
    {
	$edt->{conditions} = [ ] unless ref $edt->{conditions} eq 'ARRAY';
	push $edt->{conditions}->@*, [undef, $code, @params];
	
	$edt->{condition_code}{$code}++;
	
	# If the code starts with [EC], it represents an error or caution.
	
	if ( $code =~ /^[EC]/ )
	{
	    $edt->{error_count} = 0 unless looks_like_number $edt->{error_count};
	    $edt->{error_count}++;
	}
	
	# Otherwise, it represents a warning.
	
	else
	{
	    $edt->{warning_count} = 0 unless looks_like_number $edt->{warning_count};
	    $edt->{warning_count}++;
	}

	# Return 1 to indicate that the condition was attached.

	return 1;
    }
    
    # If we get here, return false. Either the condition was a duplicate, or the action
    # was skipped or aborted and thus is not counted.
    
    return;
}


# has_condition ( [selector], code, [arg1, arg2, arg3] )
#
# Return true if this transaction contains a condition with the specified code. If 1-3 extra
# arguments are also given, return true only if each argument value matches the
# corresponding condition parameter. The code may be specified either as a string or a regex.
# 
# If the first argument is 'main', the condition lists for the transaction as a whole are
# searched. If it is an action reference, the condition lists for that action are searched. If it
# is 'all', then all condition lists are searched. This is the default.

sub has_condition {
    
    my ($edt, $code, @v) = @_;
    
    my $selector = 'all';
    
    # If the first argument is a valid selector, remap the arguments.
    
    if ( $code =~ /^main|^all|^latest|^&./ )
    {
	($edt, $selector, $code, @v) = @_;
    }
    
    # Make sure that we were given either a regex or a string starting with [CDEFW] as the
    # code to look for.
    
    unless ( $code && (ref $code && reftype $code eq 'REGEXP' || $code =~ $CONDITION_CODE_LOOSE ) )
    {
	croak $code ? "'$code' is not a valid selector or condition code" :
	    "you must specify a condition code";
    }
    
    # If the selector is either 'main' or 'all', check the main condition list.  Return true if we
    # find an entry that has the proper code and also matches any extra values that were given.
    
    if ( $selector eq 'main' || $selector eq 'all' )
    {
	return 1 if $edt->_has_main_condition($code, @v);
	
	# If the selector is 'all', return true if any of the actions has a matching
	# condition. Return false otherwise.
	
	if ( $selector eq 'all' && ref $edt->{action_list} eq 'ARRAY' )
	{
	    foreach my $action ( $edt->{action_list}->@* )
	    {
		return 1 if $action->has_condition($code, @v);
	    }
	}

	return 0;
    }
    
    # If the selector is 'latest', check the current action.
    
    elsif ( $selector eq 'latest' )
    {
	if ( $edt->{current_action} )
	{
	    return $edt->{current_action}->has_condition($code, @v);
	}
    }
    
    # If the selector is an action reference, check that action.
    
    elsif ( $selector =~ /^&./ )
    {
	if ( my $action = $edt->{action_ref}{$selector} )
	{
	    return $action->has_condition($code, @v);
	}

	else
	{
	    croak "no matching action found for '$selector'";
	}
    }

    else
    {
	croak "'$selector' is not a valid selector";
    }
    
    # If we didn't find a matching condition, return false.
    
    return 0;
}


sub _has_main_condition {

    my ($edt, $code, @v) = @_;

    my $is_regexp = ref $code && reftype $code eq 'REGEXP';
    
    if ( ref $edt->{conditions} eq 'ARRAY' )
    {
	foreach my $i ( 0 .. $edt->{conditions}->$#* )
	{
	    my $c = $edt->{conditions}[$i];
	    
	    if ( ref $c eq 'ARRAY' &&
		( $code eq $c->[1] || $is_regexp && $c->[1] =~ $code) &&
		( @v == 0 || ( ! defined $v[0] || $v[0] eq $c->[2] ) &&
			     ( ! defined $v[1] || $v[1] eq $c->[3] ) &&
			     ( ! defined $v[2] || $v[2] eq $c->[4] ) ) )
	    {
		return $i + 1;
	    }
	}
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
#     :...              Return conditions that are attached to the referenced action.
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

my %TYPE_RE = ( errors => qr/^[EFC]/,
		fatal => qr/^[EC]/,
		nonfatal => qr/^[FW]/,
		warnings => qr/^W/,
		all => qr/^[EFCW]/ );

my $csel_pattern = qr{ ^ (?: main$|latest$|all$|:. ) }xs;
my $ctyp_pattern = qr{ ^ (?: errors$|fatal$|nonfatal$|warnings$ ) }xs;

sub conditions {
    
    my ($edt, @params) = @_;
    
    local ($_);
    
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
	    croak "'$params[0]' is not a valid selector or condition type";
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
	    croak "'$params[1]' is not a valid selector or condition type";
	}
    }
    
    # Get the proper regexp to pull out the desired conditions.
    
    my $filter = $TYPE_RE{$type} || $TYPE_RE{all};
    
    # If the selector is 'main', grep through the main conditions list.
    
    if ( $selector eq 'main' )
    {
    	if ( wantarray )
    	{
    	    return map { $edt->condition_string($_->@*) }
		grep { ref $_ eq 'ARRAY' && $_->[1] =~ $filter }
		$edt->_main_conditions;
    	}
	
    	else
    	{
    	    return grep { ref $_ eq 'ARRAY' && $_->[1] =~ $filter } $edt->_main_conditions;
    	}
    }
    
    # my @keys = $TYPE_KEYS{$type}->@*;
    
    # # For 'main', we return either or both of the 'errors' and 'warnings' lists from $edt.
    
    # if ( $selector eq 'main' )
    # {
    # 	if ( wantarray )
    # 	{
    # 	    return map( $edt->condition_string($_), map( $edt->{$_} && $edt->{$_}->@*, @keys ));
    # 	}
	
    # 	else
    # 	{
    # 	    return sum( map( scalar($edt->{$_} && $edt->{$_}->@*), @keys ));
    # 	}
    # }
    
    # For 'latest', we return either or both of the 'errors' and 'warning' lists from
    # the current action. For an action reference, we use the corresponding action.
    
    elsif ( $selector =~ /^latest|^&./ )
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
	    return map { $edt->condition_string($_->@*) }
		grep { ref $_ eq 'ARRAY' && $_->[1] =~ $filter }
		$action->conditions;
	}
	
	else
	{
	    return grep { ref $_ eq 'ARRAY' && $_->[1] =~ $filter } $action->conditions;
	}
    }
    
    # For 'all' in list context, we grep both the main conditions list and the one for
    # every action.
    
    elsif ( wantarray )
    {
	return map { $edt->condition_string($_->@*) }
	    grep { ref $_ eq 'ARRAY' && $_->[1] =~ $filter }
	    $edt->_main_conditions,
	    map { $_->status ne 'skipped' ? $_->conditions : () } $edt->_action_list;
    }
    
	# if ( wantarray && ( $type eq 'all' || $type eq 'nonfatal' ) )
	# {
	#     my $filter = '!EC' if $type eq 'nonfatal';
	    
	#     return map { $edt->condition_string($_, $filter) }
	# 	$edt->{errors} ? $edt->{errors}->@* : (),
	# 	$edt->{warnings} ? $edt->{warnings}->@* : (),
	# 	map { ( $_->{errors} ? $_->{errors}->@* : () ,
	# 		$_->{warnings} ? $_->{warnings}->@* : () ) }
	# 	grep { $_->{status} ne 'skipped' } $edt->{action_list}->@*;
	# }
	
	# # With any other type, there is just one key to check.
	
	# elsif ( wantarray )
	# {
	#     my $key = $keys[0];
	#     my $filter = 'EC' if $type eq 'fatal';
	    
	#     return map { $edt->condition_string($_, $filter) }
	# 	$edt->{$key} ? $edt->{$key}->@* : (),
	# 	map { $_->{$key} ? $_->{$key}->@* : () }
	# 	grep { $_->{status} ne 'skipped' } $edt->{action_list}->@*;
	# }
	
    # For 'all' in scalar context, return the count(s) that correspond to $type.
    
    elsif ( $type eq 'errors' )
    {
	return $edt->{error_count} + $edt->{demoted_count};
    }
    
    elsif ( $type eq 'warnings' )
    {
	return $edt->{warning_count};
    }
    
    elsif ( $type eq 'fatal' )
    {
	return $edt->{error_count};
    }
    
    elsif ( $type eq 'nonfatal' )
    {
	return $edt->{warning_count} + $edt->{demoted_count};
    }
    
    else
    {
	return $edt->{error_count} + $edt->{demoted_count} + $edt->{warning_count};
    }
}


sub _main_conditions {

    return ref $_[0]{conditions} eq 'ARRAY' ? $_[0]{conditions}->@* : ();
}


# condition_string ( condition )
#
# Return a stringified version of the specified condition tuple (action, code, parameters...).

sub condition_string {

    my ($edt, $label, $code, @params) = @_;
    
    # If no code was given, return undefined.
    
    return unless $code;
    
    # If this condition is associated with an action, include the action's label in
    # parentheses.
    
    my $labelstr = defined $label && $label ne '' ? " ($label)" : "";
    
    if ( my $msg = $edt->condition_message($code, @params) )
    {
	return "${code}${labelstr}: ${msg}";
    }
    
    else
    {
	return "${code}${labelstr}";
    }
}


sub condition_nolabel {

    my ($edt, $label, $code, @params) = @_;
    
    return unless $code;
    
    if ( my $msg = $edt->condition_message($code, @params) )
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

    return $_[0]{error_count} || 0;
}


# errors ( [selector] )
# 
# This is provided for backward compatibility. When called with no arguments in scalar
# context, it efficiently returns the error condition count. Otherwise, the argument is
# passed to the 'conditions' method.

sub errors {
    
    my ($edt, $selector) = @_;

    if ( wantarray || $selector )
    {
	return $edt->conditions($selector || 'all', 'errors');
    }

    else
    {
	return $edt->{error_count} + $edt->{demoted_count};
    }
}


# warnings ( [selector] )
# 
# Like 'errors', this method is provided for backward compatibility. When called with no
# arguments in scalar context, it efficiently returns the warning condition
# count. Otherwise, the argument is passed to the 'conditions' method. 

sub warnings {

    my ($edt, $selector) = @_;
    
    if ( wantarray || $selector )
    {
	return $edt->conditions($selector || 'all', 'warnings');
    }
    
    else
    {
	return $edt->{warning_count};
    }
}


# error_strings ( ) and warning_strings ( )
#
# These are deprecated aliases for 'errors' and 'warnings'.

sub error_strings {

    goto &errors;
}


sub warning_strings {

    goto &warnings;
}


# fatals ( [selector] )
#
# When called with no arguments in scalar context, this method efficiently returns the fatal
# condition count. Otherwise, the argument is passed to the 'conditions' method.

sub fatals {

    my ($edt, $selector) = @_;

    if ( wantarray || $selector )
    {
	return $edt->conditions($selector || 'all', 'fatal');
    }

    else
    {
	return $edt->{error_count};
    }
}


# nonfatals ( [selector] )
#
# When called with no arguments in scalar context, this method efficiently returns the nonfatal
# condition count. Otherwise, the argument is passed to the 'conditions' method.

sub nonfatals {

    my ($edt, $selector) = @_;

    if ( wantarray || $selector )
    {
	return $edt->conditions($selector || 'all', 'nonfatal');
    }

    else
    {
	return $edt->{warning_count} + $edt->{demoted_count};
    }
}


# has_condition_code ( code... )
#
# Return true if any of the specified codes have been attached to the current transaction.

sub has_condition_code {
    
    my $edt = shift;
    
    local ($_);
    
    # Return true if any of the following codes are found.
    
    return any { 1; $edt->{condition_code}{$_}; } @_;
}


# condition_message ( code, [parameters...] )
# 
# This routine generates an error message from a condition code and optinal associated
# parameters.

sub condition_message {
    
    my ($edt, $code, @params) = @_;
    
    # If the code was altered because of the PROCEED allowance, change it back
    # so we can look up the proper template.
    
    my $lookup = $code;
    substr($lookup,0,1) =~ tr/F/E/;
    
    # Look up the template according to the specified code and first parameter.  This may
    # return one or more templates.
    
    my @templates = $edt->get_condition_template($lookup, $params[0]);
    
    # Remove any undefined values from the end of the parameter list, so that the proper template
    # will be selected for the parameters given.
    
    pop @params while @params > 0 && ! defined $params[-1];
    
    # Run down the list until we find a template for which all of the required parameters have values.

  TEMPLATE:
    foreach my $tpl ( @templates )
    {
	if ( defined $tpl && $tpl ne '' )
	{
	    my @required = $tpl =~ /[&](\d)/g;
	    
	    foreach my $n ( @required )
	    {
		next TEMPLATE unless defined $params[$n-1] && $params[$n-1] ne '';
	    }
	    
	    $tpl =~ s/ [&](\d) / &_squash_param($params[$1-1]) /xseg;
	    return $tpl;
	}
    }
    
    # If none of the templates are fulfilled, concatenate the parameters with a space
    # between each one.
    
    return join(' ', @params);
}


# _squash_param ( param )
#
# Return a value suitable for inclusion into a message template. If the parameter value is longer
# than 40 characters, it is truncated and ellipses are appended. If the value is not defined, then
# 'UNKNOWN' is returned.

sub _squash_param {

    if ( defined $_[0] && length($_[0]) > 80 )
    {
	return substr($_[0],0,80) . '...';
    }
    
    else
    {
	return $_[0] // 'UNKNOWN';
    }
}


# get_condition_template ( code, selector, param_count )
#
# Given a code, a table, and an optional selector string, return a message template.  This method
# is designed to be overridden by subclasses, but the override methods must call
# SUPER::get_condition_template if they cannot find a template for their particular class that
# corresponds to the information they are given.

sub get_condition_template {

    my ($edt, $code, $selector) = @_;
    
    my $template = $CONDITION_BY_CLASS{ref $edt}{$code} //
	           $CONDITION_BY_CLASS{EditTransaction}{$code};
    
    if ( ref $template eq 'HASH' && $template->{$selector} )
    {
	$template = $template->{$selector};
    }
    
    elsif ( ref $template eq 'HASH' && $template->{default} )
    {
	$template = $template->{default};
    }
    
    # If we have reached a string value, return it. If it is a non-empty list, return
    # the list contents.
    
    if ( $template && ref $template eq 'ARRAY' && $template->@* )
    {
	return $template->@*;
    }
    
    elsif ( defined $template && ! ref $template )
    {
	return $template;
    }
    
    # Otherwise, return the UNKNOWN template.
    
    else
    {
	return $selector ? $CONDITION_BY_CLASS{EditTransaction}{'UNKNOWN'} . " for '$selector'"
	    : $CONDITION_BY_CLASS{EditTransaction}{'UNKNOWN'} . " for 'code'";
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
# comma-separated list of them.

sub _new_action {
    
    my ($edt, $table, $operation, $record) = @_;
    
    # Make sure we have a hashref containing parameters for this operation.
    
    if ( $operation =~ /^delete/ )
    {
	# The delete operations also accept a key value or a list of key values as a scalar or an
	# array. In that case, create a hashref to hold them.
	
	croak "you must provide another argument containing key values"
	    unless defined $record;
	
	unless ( ref $record && reftype $record eq 'HASH' )
	{
	    $record = { _primary => $record };
	}
    }
    
    else
    {
	croak "you must provide a hashref containing parameters for this operation"
	    unless ref $record && reftype $record eq 'HASH';
    }
    
    # If this transaction has already finished, throw an exception. Client code should never try
    # to execute operations on a transaction that has already committed or been rolled back.
    
    croak "this transaction has already finished" if $edt->has_finished;
    
    # Increment the action sequence number.
    
    $edt->{action_count}++;
    
    # Create one or more reference strings for this action. If _label is found among the input
    # parameters with a nonempty value, use both this and the sequence number as references for
    # this action. Otherwise, use just the sequence number.
    
    my (@refs, $label);
    
    if ( ref $record && defined $record->{_label} && $record->{_label} ne '' )
    {
	$label = $record->{_label};
 	push @refs, '&' . $label;
	push @refs, '&#' . $edt->{action_count};
    }

    else
    {
	$label = '#' . $edt->{action_count};
	push @refs, '&' . $label;
    }
    
    # Unless this action is to be skipped, check that it refers to a known database table.
    
    unless ( $operation eq 'skip' )
    {
	croak "unknown table '$table'" unless exists $TABLE{$table};
	$edt->{action_tables}{$table} = 1;
    }
    
    # Create a new action object. Add it to the action list, and store its string
    # reference(s) in the action_ref hash.
    
    my $action = EditTransaction::Action->new($edt, $table, $operation, $label, $record);
    
    push $edt->{action_list}->@*, $action;
    
    foreach my $k (@refs)
    {
	$edt->{action_ref}{$k} = $action;
	weaken $edt->{action_ref}{$k};
    }
    
    # If any key values were provided for this action, decode them and generate a key expression
    # that will select the records to be operated on.
    
    $edt->_unpack_key_values($action, $table, $operation, $record);
    
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
    
    my ($edt, $action, $tablename, $operation, $record) = @_;
    
    # The key table name is determined directly by the $tablename argument, unless the
    # operation is 'delete_cleanup' in which case it is overridden.
    
    my $key_tablename = $tablename;
    my $key_column;
    
    # If the table does not have a primary key, add an error condition and return.
    
    unless ( $key_column = get_table_property($tablename, 'PRIMARY_KEY') )
    {
	$edt->add_condition('main', 'E_EXECUTE',
			    "could not determine the primary key for table '$tablename'");
	return;
    }
    
    # If the operation is 'delete_cleanup', we require that the original table's
    # SUPERIOR_TABLE property be set. The key column name will be original table's
    # SUPERIOR_KEY if set, or else the PRIMARY_KEY of the superior table. Either way, the
    # specified column must exist in the subordinate table and must be a foreign key
    # linked to the superior table.
    
    if ( $operation eq 'delete_cleanup' )
    {
	$key_tablename =
	    get_table_property($tablename, 'SUPERIOR_TABLE') ||
	    croak "unable to determine the superior table for '$tablename'";
	
	$key_column =
	    get_table_property($tablename, 'SUPERIOR_KEY') ||
	    get_table_property($key_tablename, 'PRIMARY_KEY') ||
	    croak "unable to determine the linking column for '$tablename'";
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
	
	elsif ( my $alt_name = get_table_property($key_tablename, 'PRIMARY_FIELD') )
	{
	    if ( defined $record->{$alt_name} && $record->{$alt_name} ne '' )
	    {
		$key_field = $alt_name;
		$raw_values = $record->{$alt_name};
	    }
	}

	# As a fallback, if the key column name ends in _no, change that to _id and check to
	# see if the record contains a value under that name.
	
	elsif ( $key_column =~ /(.*)_no$/ )
	{
	    my $fallback_name = "${1}_id";
	    
	    if ( defined $record->{$fallback_name} && $record->{$fallback_name} ne '' )
	    {
		$key_field = $fallback_name;
		$raw_values = $record->{$fallback_name};
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
    
    foreach my $v ( ref $raw_values ? $raw_values->@* : split /\s*,\s*/, $raw_values )
    {
	# Skip any value that is either empty or zero.
	
	next unless $v;
	
	# $$$ validate extid here, not below
	
	# The most common values will be positive integers. This subroutine does not check
	# whether these values correspond to actual records in the database.
	
	if ( $v =~ /^[0-9]+$/ )
	{
	    push @key_values, $v;
	}
	
	# An action label must be looked up. If the action is found but has no key value,
	# the action will have to be authenticated at execution time.
	
	elsif ( $v =~ /^&./ )
	{
	    my $ref_action = $edt->{action_ref}{$v};
	    
	    if ( $ref_action && $ref_action->table eq $key_tablename )
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
	}
	
	# An external identifier must be parsed and checked against the proper type.
	
	elsif ( $IDRE{LOOSE} && $v =~ $IDRE{LOOSE} )
	{
	    # my $value_type = $1;
	    # my $keyval = $2;
	    
	    # if ( my $expected = $COMMON_FIELD_IDTYPE{$key_column} )
	    # {
	    # 	if ( $type eq $exttype || $exttype =~ /[|]/ && $exttype =~ /\b$type\b/ )
	    # 	{
	    # 	    push @key_values, $keyval;
	    # 	}
		
	    # 	else
	    # 	{
	    # 	    $edt->add_condition($action, 'E_EXTID', $key_column, $exttype, $type);
	    # 	}
	    # }
	    
	    # else
	    # {
	    # 	$edt->add_condition($action, 'E_EXTID', $key_column);
	    # }
	}
	
	# Otherwise, we have an invalid key value.
	
	else
	{
	    push @bad_values, $v;
	}
    }
    
    # If any bad values were found, add an error condition for all of them.

    if ( @bad_values )
    {
	my $key_string = join ',', @bad_values;
	$edt->add_condition($action, 'E_BAD_KEY', $key_field || 'unknown', $key_string);
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
# en_creater    This column will store user identifiers indicating who created each record.
#
# en_authorizer This column will store user identifiers indicating who authorized each record.
#
# en_modifier   This column will store user identifiers indicating who last modified each record.
#
# ts_created    This column will hold the date/time of record creation.
#
# ts_modified   This column will hold the date/time of last modification.
#
# ad_lock       This column will indicate whether a record was locked by an administrator.
#
# ow_lock       This column will indicate whether a record was locked by its owner.
#
# In the absence of any explicit directive, special columns will be assigned the special-identity
# directives based on the variable %COMMON_FIELD_SPECIAL in TableDefs.pm.


# handle_column ( class_or_instance, table_specifier, column_name, directive )
#
# Store the specified column directive with this transaction instance. If called as a class
# method, the directive will be supplied by default to every EditTransaction in this class. If the
# specified column does not exist in the specified table, the directive will have no effect.

sub handle_column {

    my ($edt, $table_specifier, $column_name, $directive) = @_;
    
    croak "you must give a table specifier and a column name" unless $table_specifier && $column_name;

    croak "unknown table '$table_specifier'" unless defined $TABLE{$table_specifier};
    
    croak "invalid directive" unless $VALID_DIRECTIVE{$directive};
    
    # If this was called as an instance method, add the directive locally.
    
    if ( ref $edt )
    {
	# If we have not already done so, initialize this transaction with the globally specified
	# directives for this class and table.
	
	unless ( $edt->{directive}{$table_specifier} )
	{
	    my $class = ref $edt;
	    $edt->{directive}{$table_specifier} = { $edt->class_directives($class, $table_specifier) };
	}
	
	# Then apply the current directive.
	
	$edt->{directive}{$table_specifier}{$column_name} = $directive;
    }
    
    # If this was called as a class method, apply the directive to the global directive hash for
    # this class.
    
    else
    {
	my $class = $edt;
	
	# Call the 'class_directives' method in scalar context, which will initialize the global
	# directive hash for this table specifier for this class.
	
	my $directive_count = $edt->class_directives($class, $table_specifier);
	
	# Then apply the directive.
	
	$DIRECTIVES_BY_CLASS{$class}{$table_specifier}{$column_name} = $directive;
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

    croak "unknown table '$table_specifier'"
	unless $table_specifier && defined $TABLE{$table_specifier};
    
    # Initialize the directives for this instance and table if they haven't already been
    # initialized. This may involve initializing the global directives for this class and table.
    
    unless ( $edt->{directive}{$table_specifier} )
    {
	my $class = ref $edt;
	$edt->{directive}{$table_specifier} = { $edt->class_directives($class, $table_specifier) };
    }
    
    # Return a list of columns and directives suitable for assigning to a hash.
    
    return $edt->{directive}{$table_specifier}->%*;
}


# class_directives ( class, table_specifier )
# 
# This may be called either as an instance method or as a class method. Either way, the global
# directive hash for the specified class and table specifier will be initialized if it hasn't
# already been.
# 
# When called in list context, return a list of columns and directives suitable for assigning to a
# hash. When called in scalar context, return the number of directives. A call in scalar context
# can be used to ensure initialization.

sub class_directives {

    my ($edt, $class, $table_specifier) = @_;
    
    # If we haven't cached the directives for this class, do so now. Note that
    # 'validate_against_schema' relies on this system rather than directly checking the column
    # properties so that the directives can be overridden either at the class level, the
    # transaction instance level, or the action level.
    
    unless ( $DIRECTIVES_BY_CLASS{$class}{$table_specifier} )
    {
	my $cache = $DIRECTIVES_BY_CLASS{$class}{$table_specifier} = { };
	
	my $schema = get_table_schema($edt->{dbh}, $table_specifier, $edt->{debug_mode});
	
	foreach my $colname ( $schema->{_column_list}->@* )
	{
	    if ( $schema->{$colname}{EDT_DIRECTIVE} )
	    {
		$cache->{$colname} = $schema->{$colname}{EDT_DIRECTIVE};
	    }
	}
    }
    
    return $DIRECTIVES_BY_CLASS{$class}{$table_specifier}->%*;
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
	$edt->_execute_action_list;
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
    
    # If the transaction has already finished, return true if it has committed and false
    # otherwise.

    if ( $edt->has_finished )
    {
	return $edt->has_committed;
    }
    
    # If this transaction can proceed, start the database transaction if it hasn't already been
    # started. Then run through the action list and execute any actions that are pending.
    
    elsif ( $edt->can_proceed )
    {
	$edt->_start_transaction unless $edt->has_started;
	$edt->_execute_action_list;
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
	    $edt->finalize_transaction($edt->{main_table});
	}
	
	else
	{
	    $culprit = 'cleaned up';
	    $edt->cleanup_transaction($edt->{main_table});
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
	
	$edt->initialize_transaction($edt->{main_table});
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
	$edt->{commit_count} = 0 unless looks_like_number $edt->{commit_count};
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
		$edt->{rollback_count} = 0 unless looks_like_number $edt->{rollback_count};
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
	$edt->{rollback_count} = 0 unless looks_like_number $edt->{rollback_count};
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
    
    my ($edt, $table, $record) = @_;
    
    # Create a new action object to represent this insertion.
    
    my $action = $edt->_new_action($table, 'insert', $record);
    
    $edt->{record_count}++;
    
    # If the record includes any errors or warnings, import them.
    
    $edt->import_conditions($action, $record) if $record->{_errwarn} || $record->{_errors};
    
    # We can only create records if specifically allowed. This may be specified by the user as a
    # parameter to the operation being executed, or it may be set by the operation method itself
    # if the operation is specifically designed to create records.
    
    if ( $edt->allows('CREATE') )
    {
	eval {
	    # First check to make sure we have permission to insert a record into this table. An
	    # error condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_action($action, 'insert', $table);
	    
	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
	    # class-specific checks.
	    
	    $edt->validate_action($action, 'insert', $table);
	    
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
    
    # Handle the action and return the action reference.
    
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
    
    $edt->{record_count}++;
    
    # If the record includes any errors or warnings, import them.
    
    $edt->import_conditions($action, $record) if $record->{_errwarn} || $record->{_errors};
    
    # We can only update a record if a primary key value is specified.
    
    if ( $action->keyval )
    {
	eval {
	    # First check to make sure we have permission to update this record. An error
	    # condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_action($action, 'update', $table);
	    
	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
	    # class-specific checks and substitutions.
	    
	    $edt->validate_action($action, 'update', $table);
	    
	    # Finally, check the record to be inserted, making sure that the column values meet all of
	    # the criteria for this table. Any discrepancies will cause error and/or warning
	    # conditions to be added.
	    
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
    
    # If no primary key value was specified for this record, add an error condition.
    
    else
    {
	$edt->add_condition($action, 'E_NO_KEY', 'update');
    }
    
    # Handle the action and return the action reference.
    
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
    
    $edt->{record_count}++;
    
    # If the record includes any errors or warnings, import them.
    
    $edt->import_conditions($action, $record) if $record->{_errwarn} || $record->{_errors};
    
    # As a failsafe, we only accept an empty selector or a universal selector if the key
    # '_universal' appears in the record with a true value.
    
    if ( ( defined $keyexpr && $keyexpr ne '' && $keyexpr ne '1' ) || $record->{_universal} )
    {
	$action->set_keyexpr($keyexpr);
	
	eval {
	    # First check to make sure we have permission to carry out this operation. An
	    # error condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_action($action, 'update_many', $table);
	    
	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
	    # class-specific checks and substitutions.
	    
	    $edt->validate_action($action, 'update_many', $table);
	    
	    # The update record must not include a key value.
	    
	    if ( $action->keyval )
	    {
		$edt->add_condition($action, 'E_HAS_KEY', 'update_many');
	    }
	    
	    # Finally, check the record to be inserted, making sure that the column values meet all of
	    # the criteria for this table. Any discrepancies will cause error and/or warning
	    # conditions to be added.
	    
	    $edt->validate_against_schema($action, 'update_many', $table);
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
    
    # Handle the action and return the action reference.
    
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
    
    $edt->{record_count}++;
    
    # If the record includes any errors or warnings, import them.
    
    $edt->import_conditions($action, $record) if $record->{_errwarn} || $record->{_errors};
    
    # We can only replace a record if a single key value is specified.
    
    if ( my $keyval = $action->keyval )
    {
	eval {
	    # First check to make sure we have permission to replace this record. An
	    # error condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_action($action, 'replace', $table);
	    
	    # If more than one key value was specified, add an error condition.

	    if ( ref $keyval eq 'ARRAY' )
	    {
		$edt->add_condition($action, 'E_MULTI_KEY', 'replace');
	    }
	    
	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
	    # class-specific checks and substitutions.
	    
	    $edt->validate_action($action, 'replace', $table);
	    
	    # Finally, check the record to be inserted, making sure that the column values meet all of
	    # the criteria for this table. Any discrepancies will cause error and/or warning
	    # conditions to be added.
	    
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

    my ($edt, $table, $record) = @_;
    
    # Create a new action object to represent this deletion.
    
    my $action = $edt->_new_action($table, 'delete', $record);
    
    $edt->{record_count}++;
    
    # If the record includes any errors or warnings, import them.
    
    $edt->import_conditions($action, $record) if $record && ref $record eq 'HASH' &&
	($record->{_errwarn} || $record->{_errors});
    
    # A record can only be deleted if a primary key value is specified.
    
    if ( my $keyval = $action->keyval )
    {
	eval {
	    # First check to make sure we have permission to delete this record. An
	    # error condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_action($action, 'delete', $table);
	    
	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
	    # class-specific checks and substitutions.
	    
	    $edt->validate_action($action, 'delete', $table);
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
    
    $edt->{record_count}++;
    
    if ( $selector )
    {
	$action->set_keyexpr($selector);
	
	eval {
	    # First check to make sure we have permission to delete this record. An
	    # error condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_action($action, 'delete_many', $table);

	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
	    # class-specific checks and substitutions.
	    
	    $edt->validate_action($action, 'delete_many', $table);
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
    
    # Handle the action and return the action reference.
    
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
    
    my ($edt, $table, $raw_keyexpr) = @_;
    
    # Create a new action object to represent this operation.
    
    my $action = $edt->_new_action($table, 'delete_cleanup', $raw_keyexpr);
    
    # Make sure we have a non-empty selector, although we will need to do more checks on it later.
    
    if ( $action->keyval )
    {
	eval {
	    # First check to make sure we have permission to delete records in this table. An
	    # error condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_action($action, 'delete_cleanup', $table);
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
	    croak "could not find linking column for table $table";
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
    
    my ($edt, $table, $method, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being updated.
    
    my $action = $edt->_new_action($table, 'other', $record);
    
    $action->set_method($method);
    
    $edt->{record_count}++;
    
    # If we have a primary key value, we can authorize against that record. This may be a
    # limitation in future, but for now the action is authorized if they have 'edit' permission
    # on that record.
    
    if ( $edt->keyval )
    {
	eval {
	    # First check to make sure we have permission to update this record. An error
	    # condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_action($action, 'other', $table);
	    
	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
	    # class-specific checks and substitutions.
	    
	    $edt->validate_action($action, 'other', $table);
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

    my ($edt, $record) = @_;
    
    # Create the placeholder action.
    
    my $action = $edt->_new_action(undef, 'skip', $record);
    
    # If the record includes any errors or warnings, import them.
    
    $edt->import_conditions($action, $record) if $record->{_errwarn} || $record->{_errors};
    
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

    # $$$ need sql statement labels. In fact, we need an SQL statement type.
    
    # my $action = EditTransaction::Action->new($edt, '<SQL>', 'other', ':#s1', $record);
    
    # $action->set_method('_execute_sql_action');
    
    # $edt->_handle_action($action, 'other');

    # return $action->label;
}


# import_conditions ( action, record )
#
# If the specified record contains either of the keys _errors or _warnings, convert the contents
# into conditions and add them to this transaction and to the specified action.

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
	    if ( $arg->[0] && $arg->[0] =~ $CONDITION_CODE_START )
	    {
		if ( $arg->[1] && $arg->[1] =~ $CONDITION_CODE_START )
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
		@conditions = ['E_BAD_CONDITION', "Unrecognized data format for import", ref $arg];
	    }
	}
	
	elsif ( ref $arg )
	{
	    @conditions = ['E_BAD_CONDITION', "Unrecognized data format for import", ref $arg];
	}
	
	elsif ( $arg && $arg =~ $CONDITION_CODE_START )
	{
	    @conditions = $arg;
	}
	
	else
	{
	    @conditions = ['E_BAD_CONDITION', "Unrecognized data format for import", $arg];
	}

	foreach my $c ( @conditions )
	{
	    if ( ref $c eq 'ARRAY' )
	    {
		my $code = shift $c->@*;

		if ( $code && $code =~ $CONDITION_CODE_LOOSE )
		{
		    $edt->_import_condition($action, $code, $c->@*);
		}

		else
		{
		    $edt->add_condition($action, 'E_BAD_CONDITION', "Invalid condition code", $code);
		}
	    }

	    elsif ( $c =~ $CONDITION_LINE_IMPORT )
	    {
		$edt->_import_condition($action, $1, $2);
	    }
	    
	    elsif ( $c )
	    {
		$c =~ qr{ ^ (\w+) }xs;
		$edt->add_condition($action, 'E_BAD_CONDITION', "Invalid condition code", $1);
	    }
	}
    }
    
    delete $record->{_errwarn};
    delete $record->{_errors};
}


sub _import_condition {

    my ($edt, $action, $code, $message) = @_;
    
    # Return the code to its canonical version.
    
    substr($code, 0, 1) =~ tr/DF/CE/;

    # If we have a template corresponding to that code, add the condition as is.
    
    if ( $edt->get_condition_template($code) )
    {
	$edt->add_condition($action, $code, $message);
    }
    
    # Otherwise, change any warning condition to 'W_IMPORTED' and all other conditions to
    # 'E_IMPORTED'. A condition that does not have either an 'E' or a 'W' prefix might be
    # an error, so we assume that it is.
    
    else
    {
	my $newcode = $code =~ /^W/ ? 'W_IMPORTED' : 'E_IMPORTED';
	$edt->add_condition($action, $newcode, "$code: $message");
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
    
    my $child = $edt->_new_action(@params);
    
    $parent->add_child($child);
    
    # Handle the new action.
    
    return $edt->_handle_action($child, $child->operation);
}


# authorize_action ( action, table, operation, flag )
# 
# Determine whether the current user is authorized to perform the specified action. If so, store
# the indicated permission in the action record. For any operation but 'insert' a key expression
# must be provided.
# 
# This method may be overridden by subclasses, though that is an iffy thing to do because it will
# circumvent all of the permission checks implemented here. Under most circumstances, an override
# method should make additional checks and then call this one. Override methods should indicate
# error and warning conditions by calling the method 'add_condition'.
#
# If the flag 'FINAL' is given and the authorization has been marked as pending, complete it
# now.

sub authorize_action {
    
    my ($edt, $action, $operation, $table, $flag) = @_;
    
    # The following statement is used for testing purposes.
    
    die "TEST AUTHORIZE" if $TEST_PROBLEM{authorize};
    
    my @permcounts;
    
    # If the authorization cannot be resolved yet, return immediately. In this case, authorization
    # will be completed just before the action is executed. This typically happens when an action
    # reference is provided as a key value. But if this call included the argument 'FINAL', it is
    # time to complete the authorization.

    unless ( $flag && $flag eq 'FINAL' )
    {
	return if $action->permission eq 'PENDING';
    }
    
    # Check whether the SUPERIOR_TABLE property is set for the specified table. If so, then the
    # authorization check needs to be done on this other table instead of the specified one.
    
    if ( my $sup_table = get_table_property($table, 'SUPERIOR_TABLE') )
    {
	@permcounts = $edt->authorize_subordinate_action($action, $operation, $table, $sup_table);
    }
    
    # Otherwise, use the standard authorization for each operation. Some operations are authorized
    # against the table permissions, others against individual record permissions.
    
    else
    {
	my $keyexpr = $action->keyexpr;
	
	sswitch ( $operation )
	{
	    case 'insert': {
		@permcounts = $edt->check_table_permission($table, 'post');
	    }
	    
	    case 'update':
	    case 'replace': {
	        @permcounts = $edt->check_record_permission($table, 'edit', $keyexpr);
	    }
	    
	    case 'update_many':
	    case 'delete_many': {
		@permcounts = $edt->check_table_permission($table, 'admin');
	    }
	    
	    case 'delete': {
		@permcounts = $edt->check_record_permission($table, 'delete', $keyexpr);
	    }
	    
	    case 'delete_cleanup': {
		croak "the operation '$operation' can only be done on a subordinate table";
	    }
	    
	    case 'other': {
		@permcounts = $edt->check_record_permission($table, 'edit', $keyexpr);
	    }
	    
	  default: {
		die "bad operation '$operation' in 'authorize_action'";
	    }
	};
    }

    # In either case, we will get one or more permissions from the call. If there are more than
    # one, each permission except possibly the last will be followed by a count.
    
    my $permission = shift @permcounts;
    my $count = shift @permcounts;
    
    # If the 'notfound' permission is first, that means no records at all were found. The only
    # difficult case is the 'replace' operation, for which 'notfound' is not necessarily fatal.

    if ( $permission eq 'notfound' )
    {
	if ( $operation eq 'replace' )
	{
	    # If this transaction has the CREATE allowance, the operation can proceed if the user
	    # has 'insert_key' permission on this table.
	    
	    if ( $edt->{allows}{CREATE} )
	    {
		$permission = $edt->check_table_permission($table, 'insert_key');

		# Otherwise, report E_PERM unless the NOT_FOUND allowance is present. In that case,
		# report E_NOT_FOUND.
		
		if ( $permission !~ /insert_key|admin/ )
		{
		    if ( $edt->{allows}{NOT_FOUND} )
		    {
			$edt->add_condition($action, 'E_NOT_FOUND');
		    }
		    
		    else
		    {
			$edt->add_condition($action, 'E_PERM', 'replace_new', $action->keyval);
		    }
		}
	    }
	    
	    # If we are not allowed to create new records, add the C_CREATE caution.
	    
	    else
	    {
		$edt->add_condition($action, 'C_CREATE');
	    }
	}

	# For all other operations, report E_NOT_FOUND.
	
	else
	{	
	    $edt->add_condition($action, 'E_NOT_FOUND');
	}
    }
    
    # If the primary permission is 'none', that means there are at least some records the user has
    # no authorization to operate on, or else the user lacks the necessary permission on the table
    # itself. This is reported as E_PERM.
    
    elsif ( $permission eq 'none' )
    {
	# If we have multiple keys and were given were given a count, report that as part of the
	# error condition.
	
	if ( $action->keymult )
	{
	    $edt->add_condition($action, 'E_PERM', $operation, $count);
	}
	
	else
	{
	    $edt->add_condition($action, 'E_PERM', $operation);
	}
    }
    
    # If the primary permission is 'locked', that means the user is authorized to operate on all
    # the records but at least one is either admin_locked or is owner_locked by somebody
    # else. This is reported as E_LOCKED.

    elsif ( $permission eq 'locked' )
    {
	if ( $action->keymult )
	{
	    $edt->add_condition($action, 'E_LOCKED', 'multiple', $count);
	}
	
	else
	{
	    $edt->add_condition($action, 'E_LOCKED');
	}
    }
    
    # If the primary permission includes ',unlock', that means some of the records were locked by
    # the user themselves, or else the user has adminitrative privilege and can unlock
    # anything. If the transaction allows 'LOCKED', we can proceed. Otherwise, add a C_LOCKED
    # caution.
    
    elsif ( $permission =~ /,unlock/ )
    {
	if ( $edt->allows('LOCKED') )
	{
	    $permission =~ s/,unlock//;
	}
	
	else
	{
	    $edt->add_condition($action, 'C_LOCKED');
	}
    }
    
    # If there are any remaining permissions, check if any of them is 'unowned'. If so, add
    # ',unowned' to the overall permission.
    
    if ( @permcounts && grep { $_ eq 'unowned' } @permcounts )
    {
	$permission .= ',unowned';
    }
    
    # If the returned permission is anything except the requested one, add an E_EXECUTE condition.
    
    if ( $permission !~ /admin|post|edit|delete|insert_key/ )
    {
	$edt->add_condition($action, 'E_EXECUTE', "An error occurred while authorizing this action");
	$edt->debug_line("authorize_action: bad permission '$permission'");
    }
    
    # Store the permission with the action and return it.
    
    $action->set_permission($permission);
}


# authorize_subordinate_action ( action, operation, table, suptable, keyexpr )
#
# Carry out the authorization operation where the table to be authorized against ($suptable) is
# different from the one on which the action is being executed ($table). In this situation, the
# "subordinate table" is $table while the "superior table" is $suptable. The former is subordinate
# because authorization for actions taken on it is referred to the superior table.

sub authorize_subordinate_action {

    my ($edt, $action, $operation, $table, $suptable, $keyexpr) = @_;
    
    my ($linkcol, $supcol, $altfield, @linkval, $update_linkval, %PERM, $perm);
    
    local ($_);
    
    # Start by fetching information about the link between the subordinate table and the superior table.

    ($linkcol, $supcol, $altfield) = $edt->get_linkinfo($table, $suptable);
    
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
	return $edt->check_table_permission($suptable, 'admin');
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
		$PERM{c_move} = 1 unless $edt->{allows}{MOVE_SUBORDINATES};
	    }
	}
    }
    
    # $$$ split this off into an 'authorize action' method. 
    
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
	
	if ( $lv =~ /^&./ )
	{
	    my $ref_action = $edt->{action_ref}{$lv};
	    
	    if ( $ref_action && $ref_action->table eq $suptable )
	    {
		my @refkeys = $ref_action->keyvalues;
		
		if ( @refkeys == 1 )
		{
		    $lv = $refkeys[0];
		}

		# If there is more than one key value corresponding to this reference, it
		# cannot be used to authorize a subordinate action.

		elsif ( @refkeys > 1 )
		{
		    $edt->add_condition($action, 'E_BAD_REFERENCE', '_multiple_', $linkcol, $lv);
		    $PERM{error} = 1;
		    next;
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
	
	unless ( $perm = $edt->{permission_record_edit_cache}{$suptable}{$lv} )
	{
	    my $keyexpr = "$supcol=$lv";
	    $perm = $edt->check_record_permission($suptable, 'edit', $keyexpr);
	    $edt->{permission_record_edit_cache}{$suptable}{$lv} = $perm;
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
    
    # If we need to add a C_MOVE_SUBORDINATES condition, do so now.
    
    $edt->add_condition($action, 'C_MOVE_SUBORDINATES') if $PERM{c_move};
    
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
	$action->set_linkinfo($linkcol, \@linkval);
	$action->_authorize_later;
	return 'PENDING';
    }

    # Otherwise, the action can proceed. If all of the linked records had 'admin' permission, that
    # is the aggregate permission. Otherwise, it is 'edit'. If any of the linked records had the
    # 'unlock' attribute, add that to the aggregate permission. Now is the time to add
    # C_MOVE_SUBORDINATES if that is indicated.
    
    elsif ( $PERM{edit} || $PERM{admin} )
    {
	my $aggregate = $PERM{edit} ? 'edit' : 'admin';
	$aggregate .= ',unlock' if $PERM{unlock};
	
	
	return $aggregate;
    }
    
    # As a fallback, just return 'none'.
    
    else
    {
	return 'none';
    }
}

# get_linkinfo ( table, suptable )
#
# Return information about the link between $table and $suptable.

sub get_linkinfo {

    my ($edt, $table, $suptable) = @_;
    
    # If we have this information already cached, return it now.
    
    if ( $edt->{permission_link_cache}{$table} && ref $edt->{permission_link_cache} eq 'ARRAY' &&
	 $edt->{permission_link_cache}{$table}[0] )
    {
	return $edt->{permission_link_cache}{$table}->@*;
    }
    
    # Otherwise, the subordinate table must contain a column that links records in this table to
    # records in the superior table. If no linking column is specified, assume it is the same as
    # the primary key of the superior table.
    
    my $linkcol = get_table_property($table, 'SUPERIOR_KEY');
    my $supcol = get_table_property($suptable, 'PRIMARY_KEY');
    
    $linkcol ||= $supcol;
    
    my $altfield = get_table_property($table, $linkcol, 'ALTERNATE_NAME') ||
	($linkcol =~ /(.*)_no$/ && "${1}_id");
    
    croak "SUPERIOR_TABLE was given as '$suptable' but no key column was found"
	unless $linkcol;
    
    $edt->{permission_link_cache}{$table} = [$linkcol, $supcol, $altfield];
    
    return ($linkcol, $supcol, $altfield);
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
    
#     $action->set_linkval($linkval);
    
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
	
# 	if ( $action && $action->table eq $suptable )
# 	{
# 	    $alt_permission = $edt->check_table_permission($suptable, 'edit');
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
# 	my $alt_keyexpr = $edt->aux_keyexpr($action, $suptable, $sup_keycol, $linkval, $record_col);
	
# 	return 'error' unless $alt_keyexpr;
	
# 	# Now we carry out the permission check on the permission table. The permission check is for
# 	# modifying the superior record, since that is essentially what we are doing. Whether we are
# 	# inserting, updating, or deleting subordinate records, that essentially counts as modifying
# 	# the superior record.
	
# 	$alt_permission = $edt->check_record_permission($suptable, 'edit', $alt_keyexpr);
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
	$edt->_execute_action_list;
    }
    
    # Then return the action reference. This can be used to track the status of the
    # action before and after it is executed.
    
    return $action->refstring;
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
	# separately, because if PROCEED has been set for this transaction then any
	# errors that were generated during validation of this action will have been converted
	# to warnings. But in that case, $action->errors will still return true, and this
	# action should not be executed.
	
	if ( $action->has_errors )
	{
	    $edt->{fail_count}++;
	    $action->set_status('failed');
	    next ACTION;
	}
	
	# If there are pending deletes and this action is not a delete, execute those now.
	
	if ( $edt->{pending_deletes} && $edt->{pending_deletes}->@* && $action->operation ne 'delete' )
	{
	    $edt->_cleanup_pending_actions;
	    
	    last ACTION unless $edt->can_proceed;
	}
	
	# Set the current action and the current external action, then execute the appropriate
	# handler for this action's operation. If the current action is a child action, keep track
	# of its parent.
	
	$edt->{current_action} = $action;
	
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
		
		# # If we are allowing multiple deletion and the next action is also a delete,
		# # or if we are at the end of the action list, save this action on the
		# # pending_deletes queue and go on to the next. This loop will be repeated
		# # until a non-delete operation is encountered.
		
		# if ( $edt->{allows}{MULTI_DELETE} && ! $edt->{execute_immediately} )
		# {
		#     if ( $i < $end_index && $edt->{action_list}[$i+1]{operation} eq 'delete' ||
		# 	 $i == $end_index && $arg )
		#     {
		# 	if ( $action->can_proceed )
		# 	{
		# 	    push $edt->{pending_deletes}->@*, $action;
		# 	}
			
		# 	next ACTION;
		#     }
		    
		#     # Otherwise, if we have pending deletes then coalesce them now before
		#     # executing.
		    
		#     elsif ( $edt->{pending_deletes}->@* )
		#     {
		# 	$action->_coalesce($edt->{pending_deletes});
		# 	delete $edt->{pending_deletes};
		#     }
		# }
		
		# # Now execute the action.
		
		$edt->_execute_delete($action) if $action->can_proceed;
	    }
	    
	    case 'delete_cleanup' : {
		$edt->_execute_delete_cleanup($action) if $action->can_proceed;
	    }
	    
	    case 'delete_many': {
		$edt->_execute_delete_many($action) if $action->can_proceed;
	    }
	    
	    case 'other': {
		$edt->_execute_other($action) if $action->can_proceed;
	    }
	    
	  default: {
		$edt->add_condition($action, 'E_EXECUTE', "An error occurred while routing this action");
		$edt->error_line("_execute_action_list: bad operation '$_'");
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


# _pre_execution_check ( action, operation )
#
# This method is called immediately before each action is executed. It starts by doing any
# necessary substitutions of action references for key values. If authorization and/or validation
# are still pending, those steps are carried out now. If any errors result, the action is marked
# as 'failed'. Returns true if the action can be executed, false otherwise.

sub _pre_execution_check {
    
    my ($edt, $action, $operation, $table) = @_;
    
    # If the authorization step has not been completed, do so now.
    
    my $permission = $action->permission;
    
    if ( ! $permission || $permission eq 'PENDING' )
    {
	$permission = $edt->authorize_action($action, $operation, $table, 'FINAL');
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
	$edt->validate_action($action, $operation, $table, 'FINAL');
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
	    $edt->validate_against_schema($action, $operation, $table, 'FINAL');
	}
	
	unless ( $action->validation_status eq 'COMPLETE' )
	{
	    my $vs = $action->validation_status;
	    $edt->add_condition($action, 'E_EXECUTE', "An error occurred while checking validation");
	    $edt->error_line("Validation for '$operation' was '$vs' at execution time");
	}
    }
    
    # If any prechecks have been defined for this action, do them now.
    
    $edt->_do_all_prechecks($action, $operation, $table);
    
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

    my ($edt, $action, $operation, $table) = @_;
    
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


# aux_keyexpr ( table, keycol, keyval )
#
# Generate a key expression that will select the indicated record from the table.

sub aux_keyexpr {
    
    my ($edt, $action, $table, $keycol, $keyval, $record_col) = @_;

    if ( $action )
    {
	$table ||= $action->table;
	$keycol ||= $action->keycol;
	$keyval ||= $action->keyvalues;
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
    
    elsif ( $keyval =~ /^&(.+)/ )
    {
	# my $label = $1;
	# my $lookup_val = $edt->{label_keys}{$label};

	# $$$ switch to action references
	
	# if ( $lookup_val && $edt->{label_found}{$label} eq $table )
	# {
	#     return "$keycol='$lookup_val'";
	# }
	
	# else
	# {
	#     $edt->add_condition($action, 'E_LABEL_NOT_FOUND', $record_col, $label) if $action;
	#     return 0;
	# }
    }

    # Otherwise, check if this column supports external identifiers. If it matches the pattern for
    # an external identifier of the proper type, then we can use the extracted numeric value to
    # generate a key expression.
    
    elsif ( my $exttype = $COMMON_FIELD_IDTYPE{$keycol} || get_column_property($table, $keycol, 'EXTID_TYPE') )
    {
	# $$$ call validate_extid_value
	
	if ( $IDRE{$exttype} && $keyval =~ $IDRE{$exttype} )
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

	else
	{
	    $edt->add_condition($action, 'E_EXTID', $keycol);
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
# 	    return $action->set_permission($edt->check_table_permission($table, 'post'))
# 	}
	
# 	case 'update': {
# 	    $keyexpr ||= $action->keyexpr;
# 	    return $action->set_permission($edt->check_record_permission($table, 'edit', $keyexpr));
# 	}
        
#         case 'replace': {
# 	    $keyexpr ||= $edt->get_keyexpr($action);
# 	    my $permission = $edt->check_record_permission($table, 'edit', $keyexpr);
# 	    if ( $permission eq 'notfound' )
# 	    {
# 		$permission = $edt->check_table_permission($table, 'insert_key');
# 	    }
# 	    return $action->set_permission($permission);
# 	}
	
# 	case 'delete': {
# 	    $keyexpr ||= $edt->get_keyexpr($action);
# 	    return $action->set_permission($edt->check_record_permission($table, 'delete', $keyexpr));
# 	}
	
#       default: {
# 	    croak "bad operation '$_'";
# 	}
#     }
# }


# _execute_insert ( action )
# 
# Execute this action by performing an insert operation on the database. The keys and values to be
# inserted have been checked by 'validate_against_schema' or some other code.

sub _execute_insert {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    # If authorization and/or validation for this action are still pending, complete those now.
    # If the pre-execution check returns false, then the action has failed and cannot proceed.
    
    $edt->_pre_execution_check($action, 'insert', $table) || return;
    
    # Check to make sure that we have non-empty column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'column/value mismatch or missing on insert');
	return;
    }
    
    # Construct the INSERT statement.
    
    my $dbh = $edt->dbh;
    
    my $column_string = join(',', @$cols);
    my $value_string = join(',', @$vals);
    
    my $sql = "	INSERT INTO $TABLE{$table} ($column_string)
		VALUES ($value_string)";
    
    # If the following flag is set, deliberately generate an SQL error for testing purposes.
    
    if ( $TEST_PROBLEM{sql_error} )
    {
	$sql .= " XXXX";
    }
    
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
	    
	    $action->set_status('executed');
	    
	    # If the insert succeeded, get and store the new primary key value. Otherwise, add an
	    # error condition. Unlike update, replace, and delete, if an insert statement fails
	    # that counts as a failure of the action.
	    
	    if ( $result )
	    {
		$new_keyval = $dbh->last_insert_id(undef, undef, undef, undef);
		$action->set_keyval($new_keyval);
		$action->set_result($new_keyval);
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
	    $action->pin_errors if $action->status eq 'executed';
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	}
    };
    
    # If the SQL statement succeeded, increment the executed count.
    
    if ( $action->has_executed && $new_keyval )
    {
	$edt->{exec_count}++;
    }
    
    # Otherwise, set the action status to 'failed' and increment the fail count unless the action
    # was aborted before execution.
    
    elsif ( $action->status ne 'aborted' )
    {
	$action->set_status('failed');
	$edt->{fail_count}++;
    }
    
    return;
}


# _execute_update ( action )
# 
# Execute this action by performing an update operation on the database. The keys and values to be
# updated have been checked by 'validate_against_schema' or some other code.

sub _execute_update {
    
    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    # If authorization and/or validation for this action are still pending, complete those now.
    # If the pre-execution check returns false, then the action has failed and cannot proceed.
    
    $edt->_pre_execution_check($action, 'update', $table) || return;
    
    # Check to make sure that we actually have column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'column/value mismatch or missing on update');
	return;
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
    
    my $sql = "	UPDATE $TABLE{$table} SET $set_list
		WHERE $keyexpr";
    
    # If the following flag is set, deliberately generate an SQL error for testing purposes.
    
    if ( $TEST_PROBLEM{sql_error} )
    {
	$sql .= " XXXX";
    }
    
    $edt->debug_line("$sql\n") if $edt->{debug_mode};
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    eval {
	
	# If we are logging this action, then fetch the existing record.
	
	# unless ( $edt->allows('NO_LOG_MODE') || get_table_property($table, 'NO_LOG') )
	# {
	#     $edt->fetch_old_record($action, $table, $keyexpr);
	# }
	
	# Start by calling the 'before_action' method.
	
	$edt->before_action($action, 'update', $table);
	
	# Then execute the update statement itself, provided there are no errors and the action
	# has not been aborted. If the update statement returns a result less than the number of
	# matching records, that means at least one updated record was identical to the old
	# one. This is counted as a successful execution, and is marked with a warning.
	
	if ( $action->can_proceed )
	{
	    my $result = $dbh->do($sql);
	    
	    $action->set_status('executed');
	    $action->set_result($result);
	    
	    if ( $action->keymult && $result < $action->keyvalues )
	    {
		$sql = "SELECT count(*) FROM $TABLE{$table} WHERE $keyexpr";
		
		$edt->debug_line("$sql\n") if $edt->{debug_mode};
		
		my ($found) = $dbh->selectrow_array($sql);
		
		my $missing = $action->keyvalues - $found;

		if ( $missing > 0 )
		{
		    $edt->add_condition($action, 'W_NOT_FOUND',
					"$missing key value(s) were not found");
		}
		
		my $unchanged = $found - $result;
		
		if ( $unchanged )
		{
		    $edt->add_condition($action, 'W_UNCHANGED',
					"$unchanged record(s) were unchanged by the update");
		}
	    }
	    
	    elsif ( ! $result )
	    {
		$edt->add_condition($action, 'W_UNCHANGED', 'Record was unchanged by the update');
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
	    $action->pin_errors if $action->status eq 'executed';
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	}
    };
    
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
    
    # If authorization and/or validation for this action are still pending, complete those now.
    # If the pre-execution check returns false, then the action has failed and cannot proceed.
    
    $edt->_pre_execution_check($action, 'replace', $table) || return;
    
    # Check to make sure that we actually have column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'column/value mismatch or missing on replace');
	return;
    }
    
    # Construct the REPLACE statement.
    
    my $dbh = $edt->dbh;
    
    my $column_list = join(',', @$cols);
    my $value_list = join(',', @$vals);
    
    my $sql = "	REPLACE INTO $TABLE{$table} ($column_list)
		VALUES ($value_list)";
    
    # If the following flag is set, deliberately generate an SQL error for testing purposes.
    
    if ( $TEST_PROBLEM{sql_error} )
    {
	$sql .= " XXXX";
    }
    
    $edt->debug_line("$sql\n") if $edt->{debug_mode};
    
    # Execute the statement inside a try block, to catch any exceptions that might be thrown.
    
    eval {
	
	# If we are logging this action, then fetch the existing record if any.
	
	# unless ( $edt->allows('NO_LOG_MODE') || get_table_property($table, 'NO_LOG') )
	# {
	#     $edt->fetch_old_record($action, $table);
	# }
	
	# Start by calling the 'before_action' method.
	
	$edt->before_action($action, 'replace', $table);
	
	# Then execute the replace statement itself, provided there are no errors and the action
	# was not aborted. If the replace statement returns a zero result and does not throw an
	# exception, that means that the new record was identical to the old one. This is counted
	# as a successful execution, and is marked with a warning.
	
	if ( $action->can_proceed )
	{
	    my $result = $dbh->do($sql);
	    
	    $action->set_status('executed');
	    $action->set_result($result);
	    
	    unless ( $result )
	    {
		$edt->add_condition($action, 'W_UNCHANGED', 'New record is identical to the old');
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
	    $action->pin_errors if $action->status eq 'executed';
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	}
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


# _execute_delete ( action )
# 
# Actually perform a delete operation on the database. The only field that makes any difference
# here is the primary key.

sub _execute_delete {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    # If authorization and/or validation for this action are still pending, complete those now.
    # If the pre-execution check returns false, then the action has failed and cannot proceed.
    
    $edt->_pre_execution_check($action, 'delete', $table) || return;
    
    my $dbh = $edt->dbh;
    
    my $keyexpr = $action->keyexpr;
    
    # If the following flag is set, deliberately generate an SQL error for
    # testing purposes.
    
    if ( $TEST_PROBLEM{sql_error} )
    {
	$keyexpr .= ' XXXX';
    }
    
    # Construct the DELETE statement.
    
    my $sql = "	DELETE FROM $TABLE{$table} WHERE $keyexpr";
    
    $edt->debug_line( "$sql\n" ) if $edt->{debug_mode};
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    eval {
	
	# If we are logging this action, then fetch the existing record.
	
	# unless ( $edt->allows('NO_LOG_MODE') || get_table_property($table, 'NO_LOG') )
	# {
	#     $edt->fetch_old_record($action, $table, $keyexpr);
	# }
	
	# Start by calling the 'before_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing.    
	
	$edt->before_action($action, 'delete', $table);
	
	# Then execute the delete statement itself, provided the action has not been aborted.
	
	if ( $action->can_proceed )
	{
	    my $result = $dbh->do($sql);
	    
	    $action->set_status('executed');
	    $action->set_result($result);
	    
	    if ( $action->keymult && $result < $action->keyvalues )
	    {
		my $missing = $action->keyvalues - $result;
		
		$edt->add_condition($action, 'W_NOT_FOUND', "$missing key values(s) were not found");
	    }
	    
	    $edt->after_action($action, 'delete', $table, $result);
	}
    };
    
    if ( $@ )
    {	
	$edt->error_line($@);
	$action->pin_errors if $action->status eq 'executed';
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
    };
    
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


# _execute_delete_cleanup ( action )
# 
# Perform a delete operation on the database. The records to be deleted are those that match the
# action selector and were not either inserted, updated, or replaced during this transaction.

sub _execute_delete_cleanup {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    # If authorization and/or validation for this action are still pending, do those now. Also
    # complete action reference substitution. If the pre-execution check returns false, then
    # the action has failed and cannot proceed.
    
    $edt->_pre_execution_check($action, 'delete_cleanup', $table) || return;
    
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
    
    if ( $TEST_PROBLEM{sql_error} )
    {
	$keyexpr .= 'XXXX';
    }
    
    # Then construct the DELETE statement.
    
    $action->set_keyexpr($keyexpr);
    
    my $sql = "	DELETE FROM $TABLE{$table} WHERE $keyexpr";
    
    $edt->debug_line( "$sql\n" ) if $edt->{debug_mode};
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
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
	    my $result = $dbh->do($sql);
	    
	    $action->set_status('executed');
	    $action->set_result($result);
	    $action->_confirm_keyval($deleted_keys);   # $$$ needs to be rewritten
	    
	    $edt->after_action($action, 'delete_cleanup', $table, $result);
	}
    };
    
    if ( $@ )
    {	
	$edt->error_line($@);
	$action->pin_errors if $action->status eq 'executed';
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
    };
    
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
    
    # If authorization and/or validation for this action are still pending, do those now. Also
    # complete action reference substitution. If the pre-execution check returns false, then
    # the action has failed and cannot proceed.
    
    $edt->_pre_execution_check($action, 'insert', $table) || return;
    
    # Determine the method to be called.
    
    my $method = $action->method;
    
    # Call the specified method inside a try block. If it fails, add either an error or
    # a warning depending on whether this EditTransaction allows PROCEED.
    
    eval {
	
	$edt->before_action($action, 'other', $table);

	if ( $action->can_proceed )
	{
	    my $result = $edt->$method($action, $table, $action->record);
	    
	    $action->set_status('executed');
	    $action->set_result($result);

	    $edt->after_action($action, 'other', $table, $result);
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

    return unless $edt->{action_tables};
    return keys $edt->{action_tables}->%*;
}


sub deleted_keys {

    my ($edt, $table) = @_;
    return $edt->_result_keys('delete', $table);
}


sub inserted_keys {

    my ($edt, $table) = @_;
    return $edt->_result_keys('insert', $table);
}


sub updated_keys {

    my ($edt, $table) = @_;
    return $edt->_result_keys('update', $table);
}


sub replaced_keys {

    my ($edt, $table) = @_;
    return $edt->_result_keys('replace', $table);
}


sub other_keys {

    my ($edt, $table) = @_;
    return $edt->_result_keys('other', $table);
}


sub superior_keys {

    my ($edt, $table) = @_;
    # return $edt->_result_keys('superior_keys', $table);
}


sub failed_keys {

    my ($edt, $table) = @_;
    return $edt->_result_keys('failed', $table);
}


sub _result_keys {
    
    my ($edt, $type, $table) = @_;

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
	
	if ( $table )
	{
	    next ACTION if $table ne $action->table;
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
    
#     my ($edt, $type, $table) = @_;
    
#     return unless $edt->{$type};
#     return unless $edt->{action_tables} || $table;
    
#     if ( wantarray && $table )
#     {
# 	return $edt->{$type}{$table} ? $edt->{$type}{$table}->@* : ( );
#     }
    
#     elsif ( wantarray )
#     {
# 	my $mt = $edt->{main_table};
# 	my @tables = grep { $_ ne $mt } keys $edt->{action_tables}->%*;
# 	unshift @tables, $mt if $mt;
	
# 	return map { $edt->{$type}{$_} ? $edt->{$type}{$_}->@* : ( ) } @tables;
#     }
    
#     elsif ( $table )
#     {
# 	return $edt->{$type}{$table} ? $edt->{$type}{$table}->@* : 0;
#     }

#     else
#     {
# 	return reduce { $a + ($edt->{$type}{$b} ? $edt->{$type}{$b}->@* : 0) }
# 	    0, keys $edt->{action_tables}->%*;
#     }
# }


# sub action_keys {

#     my ($edt, $table) = @_;
    
#     return unless $edt->{action_tables} || $table;
    
#     my @types = qw(deleted_keys inserted_keys updated_keys replaced_keys other_keys);
    
#     if ( wantarray && $table )
#     {
# 	return map { $edt->{$_} && $edt->{$_}{$table} ? $edt->{$_}{$table}->@* : ( ) } @types;
#     }
    
#     elsif ( wantarray )
#     {
# 	my $mt = $edt->{main_table};
# 	my @tables = $mt, grep { $_ ne $mt } keys $edt->{action_tables}->%*;
	
# 	return map { $edt->_keys_by_table($_) } @tables;
#     }
    
#     elsif ( $table )
#     {
# 	return reduce { $a + ($edt->{$b}{$table} ? $edt->{$b}{$table}->@* : 0) } 0, @types;
#     }
    
#     else
#     {
# 	return reduce { $a + $edt->_keys_by_table($b) } 0, keys $edt->{action_tables}->%*;
#     }
# }


# sub _keys_by_table {

#     my ($edt, $table) = @_;
    
#     my @types = qw(deleted_keys inserted_keys updated_keys replaced_keys other_keys);
    
#     if ( wantarray )
#     {
# 	return map { $edt->{$_}{$table} ? $edt->{$_}{$table}->@* : ( ) } @types;
#     }

#     else
#     {
# 	return reduce { $a + ($edt->{$b}{$table} ? $edt->{$b}{$table}->@* : 0) } 0, @types;
#     }
# }


# sub count_superior_key {

#     my ($edt, $table, $keyval) = @_;
    
#     if ( $keyval =~ /^@/ )
#     {
# 	if ( my $action = $edt->{action_ref}{$keyval} )
# 	{
# 	    if ( $keyval = $action->keyval )
# 	    {
# 		$edt->{superior_keys}{$table}{$keyval} = 1;
# 	    }
# 	}
#     }
    
#     elsif ( $keyval )
#     {
# 	$edt->{superior_keys}{$table}{$keyval} = 1;
#     }
# }


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
