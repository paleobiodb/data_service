# 
# The Paleobiology Database
# 
#   EditOperation.pm - base class for database editing
# 

package EditTransaction;

use strict;

use TableDefs qw(get_table_property get_column_properties
		 %COMMON_FIELD_IDTYPE %COMMON_FIELD_OTHER
		 %FOREIGN_KEY_TABLE %FOREIGN_KEY_KEY);
use TableData qw(get_table_schema);

use Carp qw(carp croak);
use Try::Tiny;
use Scalar::Util qw(weaken blessed);


# This class is intended to encapsulate the mid-level code necessary for updating records in the
# database in the context of a data service operation. It handles transaction initiation,
# commitment, and rollback, permission checking, and also error and warning conditions.
# 
# This class can be subclassed (see ResourceEdit.pm, TimescaleEdit.pm) in order to provide
# additional logic for checking values and performing auxiliary operations in conjunction with
# database inserts, updates, and deletes.

our (%HOOK_NAMES) = ( auth_insert => 1, check_insert => 1,
		      auth_update => 1, check_update => 1,
		      auth_delete => 1, check_delete => 1,
		      before_insert => 1, after_insert => 1,
		      before_update => 1, after_update => 1,
		      before_delete => 1, after_delete => 1,
		      begin_transaction => 1, end_transaction => 1 );

our (%HOOKS_BY_CLASS);

our (%ALLOW_BY_CLASS) = ( EditTransaction => { CREATE => 1,
					       PROCEED => 1, 
					       KEY_INSERT => 1,
					       MULTI_INSERT => 1,
					       MULTI_DELETE => 1,
					       NO_RECORDS => 1,
					       ALTER_TRAIL => 1 } );


# Constructor and destructor
# --------------------------

# new ( request, perms, conditions )
# 
# Create a new EditTransaction object, for use in association with the specified request. The
# second argument should be a Permissions object, and the third a hash of allowed conditions.

sub new {
    
    my ($class, $request, $perms, $allows) = @_;
    
    # Check the arguments.
    
    croak "new EditTransaction: request is required"
	unless blessed($request) && $request->can('get_connection');
    
    croak "new EditTransaction: perms is required"
	unless blessed $perms && $perms->isa('Permissions');
    
    # Create a new EditTransaction object, and bless it into the proper class.
    
    my $edt = { perms => $perms,
		allows => { },
		action_list => [ ],
		bad_list => [ ],
		errors => [ ],
		warnings => [ ],
		condition => { },
		current_errors => [ ],
		current_warnings => [ ],
		current_condition => { },
		current_label => undef,
		current_keyexpr => undef,
		inserted_keys => { },
		updated_keys => { },
		deleted_keys => { },
		record_count => 0,
		action_count => 0,
		commit_count => 0,
		rollback_count => 0,
		transaction => '',
		state => 'ok' };
    
    bless $edt, $class;
    
    # Store the request, dbh, and debug flag as local fields. Weaken all references, because
    # otherwise those objects might be prevented from being destroyed when they go out of
    # scope. In particular, the $reference object might subsequently be updated to contain a
    # reference to this EditTransaction object and as we all know, circular references can prevent
    # garbage collection unless one of them is weakened.
    
    $edt->{dbh} = $request->get_connection;
    weaken $edt->{dbh};
    
    $edt->{request} = $request;
    weaken $edt->{request};
    
    $edt->{debug} = $request->debug;
    
    # If we are given either a hash or an array of conditions that should be allowed, store them
    # in the object.
    
    my @allows;
    
    if ( ref $allows eq 'HASH' )
    {
	@allows = keys %$allows;
    }
    
    elsif ( ref $allows eq 'ARRAY' )
    {
	@allows = @$allows;
    }

    elsif ( defined $allows )
    {
	croak "new EditTransaction: bad value '$allows' for 'allows'";
    }
    
    foreach my $k ( @allows )
    {
	if ( $ALLOW_BY_CLASS{$class}{$k} || $ALLOW_BY_CLASS{EditTransaction}{$k} )
	{
	    $edt->{allows}{$k} = 1;
	}
	
	else
	{
	    $edt->add_condition('W_ALLOW', $k);
	}
    }
    
    # Store a reference to the hooks (if any) for the subclass into which this object is
    # blessed. If none were registered, store an empty hash.
    
    $HOOKS_BY_CLASS{$class} ||= { };
    $edt->{hooks} = $HOOKS_BY_CLASS{$class};
    
    return $edt;
}


# If this object is destroyed while a transaction is in progress, roll it back.

sub DESTROY {
    
    my ($edt) = @_;
    
    if ( $edt->{transaction} && $edt->{transaction} eq 'active' )
    {    
	$edt->rollback;
    }
}


# Hooks
# -----

# The following routines implement a hook functionality that can be used by subclasses to
# implement additional behavior around record insertion, deletion, and updating. These hooks are
# selected both by object class and by table name. Note: hooks do not inherit if subclasses are
# further subclassed!

# register_hooks ( table_name, hooks )
# 
# This class method is designed to be called at startup by modules that subclass this one. It
# registers methods that these modules implement, assigning them to hooks implemented by this
# class. The methods must be specified by name, and are stored by class, table, and hook
# name. Multiple methods can be registered for a single hook.

sub register_hooks {

    my ($class, $table, @hooks) = @_;
    
    while ( @hooks )
    {
	# Check the arguments.
	
	my $hook = shift @hooks;
	croak "register_hooks: invalid hook '$hook'" unless $HOOK_NAMES{$hook};
	my $method = shift @hooks || 
	    croak "register_hooks: must provide a table name followed by an even number of arguments";
	
	croak "register_hooks: unknown method '$method'" unless $class->can($method);
	
	# If we have a single hook, it gets stored as a scalar. With multiple hooks, they are
	# stored as an array.
	
	unless ( $HOOKS_BY_CLASS{$class}{$table}{$hook} )
	{
	    $HOOKS_BY_CLASS{$class}{$table}{$hook} = $method;
	}
	
	elsif ( ref $HOOKS_BY_CLASS{$class}{$table}{$hook} eq 'ARRAY' )
	{
	    push @{$HOOKS_BY_CLASS{$class}{$table}{$hook}}, $method;
	}
	
	else
	{
	    my @list = ( $HOOKS_BY_CLASS{$class}{$table}{$hook}, $method );
	    $HOOKS_BY_CLASS{$class}{$table}{$hook} = \@list;
	}
    }
}


# _call_hooks ( table, hook_name, args... )
# 
# Call all of the methods that are registered under the specified hook name, for the specified
# table, for the class that this EditTransaction belongs to. The result of the last one executed
# will be available via the '_hook_result' method defined below. This method returns 1 if one or more
# calls were made, false otherwise.

sub _call_hooks {

    my ($edt, $table, $hook_name, @args) = @_;
    
    if ( my $hooks = $edt->{hooks}{$table}{$hook_name} )
    {
	unless ( ref $hooks )
	{
	    ($edt->{hook_result}) = $edt->$hooks($table, @args);
	}
	
	else
	{
	    foreach my $method ( @$hooks )
	    {
		($edt->{hook_result}) = $edt->$method($table, @args);
	    }
	}
	
	return 1;
    }
    
    else
    {
	return 0;
    }
}


# _hook_result ( )
# 
# Return the result of the last hook routine call that was made. If it was a list result
# containing more than one value, only the first is saved and returned by this method.

sub _hook_result {

    return $_[0]->{hook_result};
}


# Basic accessor methods
# ----------------------

# These are all read-only.

sub dbh {
    
    return $_[0]->{dbh};
}


sub request {
    
    return $_[0]->{request};
}


sub perms {
    
    return $_[0]->{perms};
}


sub debug {
    
    return $_[0]->{debug};
}


sub role {
    
    return $_[0]->{perms}->role;
}


# Error and warning conditions, and allows.
# -----------------------------------------

# Error and warning conditions are indicated by codes, all in upper case word symbols. Those that
# start with 'E_' and 'C_' represent errors, and those that start with 'W_' represent warnings. In
# general, errors cause the operation to be aborted while warnings do not.
# 
# Codes that start with 'C_' indicate conditions that may be allowed, so that the operation
# proceeds despite them. A canonical example is 'C_CREATE', which is returned if records are to be
# created. If the data service operation method knows that records are to be created, it can
# explicitly allow 'CREATE', which will allow the records to be created. Alternatively, it can
# return the 'C_CREATE' error to the user, and allow the operation to be re-tried with the
# 'CREATE' condition specified in the operation parameters. This will allow the operation to
# proceed. The same can be done with other conditions.
# 
# Codes that start with 'E_' indicate conditions that must be remedied for the operation to
# proceed. For example, 'E_PERM' indicates that the user does not have permission to operate on
# the specified record or table. 'E_NOT_FOUND' indicates that the record to be updated is not in
# the database. These conditions can not, in general, be allowed. However, the special allowance
# 'PROCEED' specifies that whatever parts of the operation are able to succeed should be carried
# out, even if some record operations fail.
# 
# Codes that start with 'W_' indicate warnings that should be passed back to the user but do not
# prevent the operation from proceeding.
# 
# Codes that start with 'D_' and 'F_' indicate conditions that would otherwise have been errors,
# under the 'PROCEED' allowance. These are treated as warnings.
# 
# Allowed conditions must be specified for each EditTransaction object when it is created.


# register_allows ( condition... )
# 
# Register the names of extra conditions that can be allowed for transactions in a particular
# subclass. This class method is designed to be called at startup from a module that subclasses
# this one.

sub register_allows {
    
    my ($class, @names) = @_;
    
    foreach my $n ( @names )
    {
	$ALLOW_BY_CLASS{$class}{$n} = 1;
    }
}


# allows ( condition )
# 
# Returns true if the specified condition is allowed for this EditTransaction, false
# otherwise. The set of allowed conditions was specified when this object was originally created.

sub allows {
    
    return $_[0]->{allows}{$_[1]};
}


# add_condition ( condition, data... )
# 
# Add a condition (error or warning) that pertains to the entire transaction rather than a single
# record. One or more pieces of data will generally also be passed in, which can be used later by
# code in the data service operation module to generate an error or warning message to return to
# the user. Since these conditions apply to the transaction as a whole, they cannot be ignored
# with 'PROCEED'.

sub add_condition { 
    
    my ($edt, $code, @data) = @_;
    
    if ( $code =~ qr{ ^ [EC] _ }xs )
    {
	push @{$edt->{errors}}, [$code, '_', @data];
    }
    
    elsif ( $code =~ qr{ ^ W_ }xs )
    {
	push @{$edt->{warnings}}, [$code, '_', @data];
    }
    
    else
    {
	croak "bad condition '$code'";
    }
    
    $edt->{condition}{$code} = 1;
    
    return 1;
}


# errors ( )
# 
# Return the list of errors for the current EditTransaction. In numeric context, Perl will simply
# evaluate this as a number. In boolean context, as true if there are any and false if not. This
# is one of my favorite features of Perl.

sub errors {

    return @{$_[0]->{errors}};
}


# warnings ( )
# 
# Return the list of warnings for the current EditTransaction.

sub warnings {
    
    return @{$_[0]->{warnings}};
}


# add_record_condition ( condition, data... )
# 
# Add a condition (error or warning) that pertains to the current record.

sub add_record_condition {
    
    my ($edt, $code, @data) = @_;
    
    croak "_new_record must be called first" unless defined $edt->{current_label};
    
    if ( $code =~ qr{ ^ [EC] _ }xs )
    {
	push @{$edt->{current_errors}}, [$code, $edt->{current_label}, '_', @data];
    }
    
    elsif ( $code =~ qr{ ^ W_ }xs )
    {
	push @{$edt->{current_warnings}}, [$code, $edt->{current_label}, '_', @data];
    }
    
    else
    {
	croak "bad condition '$code'";
    }
    
    $edt->{current_condition}{$code} = 1;
    
    return $edt;
}


# record_errors ( )
# 
# Return the list of errors (not warnings) for the current record. This is used below to test
# whether or not we can proceed with the current record.

sub record_errors {
    
    return @{$_[0]->{current_errors}};
}


# record_warnings ( )
# 
# Return the list of warnings for the current record. This is only here in case it is needed by a
# subroutine defined by some subclass.

sub record_warnings {

    return @{$_[0]->{current_warnings}};
}

# record_condition_ref ( )
# 
# Return a reference to a list of all the conditions associated with the current record, errors
# and warnings both. This new list will be copied from the current condition lists, so that the
# contents will not disappear after the next call to _finish_record.

sub record_condition_ref {
    
    my @list;
    
    push @list, @{$_[0]->{current_errors}};
    push @list, @{$_[0]->{current_warnings}};
    
    return \@list;
}


# Record sequencing
# -----------------

# _new_record {
# 
# Prepare for a new record operation. This includes moving any record errors and warnings to the
# main error and warning lists. It also determines the label to be used in reporting any
# conditions about this record. This is intended to be a private method, called only from within
# this class.

sub _new_record {
    
    my ($edt, $table, $record) = @_;
    
    croak "no record specified" unless ref $record eq 'HASH';
    
    # If there are any errors and warnings pending from the previous record, move them to the main
    # lists.
    
    $edt->_finish_record;
    
    # If the record has a primary key and a non-empty value, store these as current_keyexpr. This
    # will be used to fetch information about the record, such as the authorization fields.
    
    my ($key_column, $key_attr);
    
    if ( my $key_attr = get_table_property($table, 'PRIMARY_ATTR') )
    {
	if ( defined $record->{$key_attr} && $record->{$key_attr} ne '' )
	{
	    $edt->{current_keyval} = $record->{$key_attr};
	}
    }
    
    else
    {
	$edt->{current_keyval} = undef;
    }
    
    # Then determine a label for this record. If one is specified, use that. Otherwise, keep count
    # of how many records we have seen so far and use that prepended by '#'.
    
    $edt->{record_count}++;
    
    if ( defined $key_value )
    {
	$edt->{current_label} = $record->{record_label};
    }
    
    else
    {
	$edt->{current_label} = '#' . $edt->{record_count};
    }
}


# _finish_record ( )
# 
# Finish processing the current record. All record conditions are moved over to the main lists,
# and the 'current_label' is set to undefined.

sub _finish_record {
    
    my ($edt) = @_;
    
    if ( @{$edt->{current_errors}} )
    {
	if ( $edt->allows('PROCEED') )
	{
	    while ( my $e = shift @{$edt->{current_errors}} )
	    {
		substr($e->[0],0,1) =~ tr/CE/DF/;
		push @{$edt->{warnings}}, $e;
	    }
	}
	
	elsif ( $edt->allows('NOT_FOUND') )
	{
	    while ( my $e = shift @{$edt->{current_errors}} )
	    {
		if ( $e->[0] eq 'E_NOT_FOUND' )
		{
		    $e->[0] = 'F_NOT_FOUND';
		    push @{$edt->{warnings}}, $e;
		}
		
		else
		{
		    push @{$edt->{errprs}}, $e;
		}
	    }
	}
	
	else
	{
	    while ( my $e = shift @{$edt->{current_errors}} )
	    {
		push @{$edt->{errors}}, $e;
	    }
	}
    }
    
    while ( my $w = shift @{$edt->{current_warnings}} )
    {
	push @{$edt->{warnings}}, $w;
    }
    
    $edt->{current_keyval} = undef;
    $edt->{current_label} = undef;
}


# _clear_record ( )
# 
# Clear any error and warning messages generated by the current record, and also the record
# label. This method is called when processing of a record is to be abandoned.

sub _clear_record {
    
    my ($edt) = @_;
    
    @{$edt->{curremt_errors}} = ();
    @{$edt->{current_warnings}} = ();
    $edt->{current_label} =  undef;
}


# record_key ( )
# 
# Return the key value, if any, for the record currently being processed. This is valid both
# during checking and execution.

sub record_key {

    return $_[0]->{current_keyval};
}


# record_keyexpr ( )
# 
# Return the key expression, if any, for the record currently being processed. This is an
# expression that can be used in SQL statements to select the specific record.

sub record_keyexpr {

    # $$$ okay, I need to stop now
}


# record_label ( )
# 
# Return the record label for the record currently being processed. This is valid both during
# checking and execution.

sub record_label {

    return $_[0]->{current_label};
}

# Transaction control
# -------------------

# start_transaction ( )
# 
# Start the database transaction. This is done automatically when 'execute' is called, but can
# be done explicitly at an earlier stage if the checking of record values needs to be done
# inside a transaction.

sub start_transaction {
    
    my ($edt) = @_;
    
    if ( $edt->{transaction} eq 'active' )
    {
	print STDERR " WARNING: transaction already active\n\n" if $edt->debug;
	return;
    }
    
    my $label = $edt->role eq 'guest' ? '(guest) ' : '';
    
    print STDERR " >>> START TRANSACTION $label\n\n" if $edt->debug;
    
    $edt->dbh->do("START TRANSACTION");
    $edt->{transaction} = 'active';
    
    # If we have a 'begin_transaction' hook, call it now.
    
    $edt->_call_hooks($table, 'begin_transaction');
    
    return $edt;
}


# commit ( )
# 
# Commit the database transaction. After this is done, this EditTransaction cannot be used for any
# more actions. If the operation method needs to make more changes to the database, a new
# EditTransaction must be created.
# 
# $$$ Perhaps I will later modify this class so that it can be used for multiple transactions in turn.

sub commit {
    
    my ($edt) = @_;
    
    print STDERR " <<< COMMIT TRANSACTION\n\n" if $edt->debug;
	
    $edt->dbh->do("COMMIT");
    $edt->{transaction} = 'committed';
    $edt->{commit_count}++;
    
    return $edt;
}


sub rollback {

    my ($edt) = @_;
    
    print STDERR " <<< ROLLBACK TRANSACTION\n\n" if $edt->debug;
	
    $edt->dbh->do("ROLLBACK");
    $edt->{transaction} = 'aborted';
    $edt->{rollback_count}++;
    
    return $edt;
}


# Record operations
# -----------------

# The operations in this section are called by data service operation methods to insert, update,
# and delete records in the database.

# $$$ do we need an option to carry out database operations immediately?


# start_execution
# 
# Call 'start_transaction' and also set the 'execute_immediately' flag. This means that subsequent
# actions will be carried out immediately on the database rather than waiting for a call to
# 'execute'.

sub start_execution {
    
    my ($edt) = @_;
    
    $edt->start_transaction;
    $edt->{execute_immediately} = 1;
}


# insert_record ( table, record )
# 
# The specified record is to be inserted into the specified table. Depending on the settings of
# this particular EditTransaction, this action may happen immediately or may be executed
# later. The record in question must NOT include a primary key value.

sub insert_record {
    
    my ($edt, $table, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and initialize
    # the action object for the record being inserted.
    
    my $action = $edt->_new_action($record);
    
    # We can only create records if specifically allowed. This may be specified by the user as a
    # parameter to the operation being executed, or it may be set by the operation method itself
    # if the operation is specifically designed to create records.
    
    if ( $edt->allows('CREATE') )
    {
	# First check to make sure we have permission to edit this record. If an 'auth_insert'
	# method was defined, call it. This method must return a string indicating the permission
	# the user has on this record, or THE EMPTY STRING if none. Otherwise, we call the default
	# 'check_table_permission' method.
	
        if ( $edt->_call_hooks($table, 'auth_insert', 'insert', $action) )
	{
	    $action->permission($edt->_hook_result);
	}
	
	else
	{
	    $action->permission($edt->check_table_permission($table, 'post'));
	}
	
	# If the user does not have permission to add a record, add an error condition.
	
	if ( $permission ne 'post' && $permission ne 'admin' )
	{
	    $edt->add_record_condition('E_PERM', 'insert');
	}
	
	# A record to be inserted must not have a primary key value specified for it. Records with
	# primary key values can only be passed to 'update_record' or 'replace_record'.
	
	if ( $action->key )
	{
	    $edt->add_record_condition('E_HAS_KEY' 'insert');
	}
	
	# Then check the actual record to be inserted, to make sure that the column values meet
	# all of the criteria for this table. If a 'check_insert' method was specified, then call
	# it. Otherwise, call the 'validate_record' method. In either case, the method must add an
	# error condition if any criteria are violated. It may also add warning conditions.
	
	$edt->_call_hooks($table, 'check_insert', 'insert', $action) ||
	    $edt->validate_record($table, 'insert', $action);
    }
    
    # If an attempt is made to add a record without the 'CREATE' allowance, add the appropriate
    # error condition.
    
    else
    {
	$edt->add_record_condition('C_CREATE');
    }
    
    # Either execute the action immediately or add it to the appropriate list depending on whether
    # or not any error conditions are found.
    
    return _handle_action($table, 'insert', $action);
}


# update_record ( table, record )
# 
# The specified record is to be updated in the specified table. Depending on the settings of this
# particular EditTransaction, this action may happen immediately or may be executed later. The
# record in question must include a primary key value, indicating which record to update.

sub update_record {
    
    my ($edt, $table, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being updated.
    
    $edt->_new_record($record);
    
    my $permission;
    
    # We can only update a record if a primary key value is specified.
    
    if ( my $key = $edt->record_key )
    {
	# First check to make sure we have permission to edit this record. If an 'auth_update'
	# method was defined, call it. This method must return a string indicating the permission
	# the user has on this record, or THE EMPTY STRING if none. Otherwise, we call the default
	# 'check_record_permission' method.
	
	if ( $edt->_call_hooks($table, 'auth_update', 'update', $record) )
	{
	    $permission = $edt->_hook_result;
	}
	
	else
	{
	    $permission = $edt->check_record_permission($table, 'edit', $key);
	}
	
	# If no such record is found in the database, add an error condition.
	
	if ( $permission eq 'notfound' )
	{
	    $edt->add_record_condition('E_NOT_FOUND', 'update');
	}
	
	# If the user does not have permission to edit the record, add an error condition. 
	
	elsif ( $permission ne 'edit' && $permission ne 'admin' )
	{
	    $edt->add_record_condition('E_PERM', 'update');
	}
	
	# Then check the actual record, to make sure that the new column values to be stored meet
	# all of the criteria for this table. If a 'check_update' method was specified, then call
	# it. Otherwise, call the 'validate_record' method. In either case, the method must add an
	# error condition if any criteria are violated. It may also add warning conditions.
	
	$edt->_call_hooks($table, 'check_update', 'update', $record, $permission) ||
	    $edt->validate_record($table, 'update', $record, $permission);
    }
    
    # If no primary key value was specified for this record, add an error condition. This will, of
    # course, have to be reported under the record label that was passed in as part of the record
    # (if one was in fact given) or else the index of the record in the input set.
    
    else
    {
	$edt->add_record_condition('E_NO_KEY', 'update');
    }
    
    # Create an action record, and either execute it immediately or add it to the appropriate list
    # depending on whether or not any error conditions are found.
    
    return _handle_record($table, 'update', $record, $permission);
}


# replace_record ( table, record )
# 
# The specified record is to be inserted into the specified table, replacing any record that may
# exist with the same primary key value. Depending on the settings of this particular EditTransaction,
# this action may happen immediately or may be executed later. The record in question must include
# a primary key value.

sub replace_record {
    
    my ($edt, $table, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being replaced.
    
    $edt->_new_record($record);
    
    my $permission;
    
    # We can only replace a record if a primary key value is specified.
    
    if ( my $key = $edt->record_key )
    {
	# First check to make sure we have permission to edit this record. If an 'auth_replace'
	# method was defined, call it. This method must return a string indicating the permission
	# the user has on this record, or THE EMPTY STRING if none. Otherwise, we call the default
	# 'check_record_permission' method.
	
	if ( $edt->_call_hooks($table, 'auth_replace', 'replace', $record) )
	{
	    $permission = $edt->_hook_result;
	}
	
	else
	{
	    $permission = $edt->check_record_permission($table, 'edit', $key);
	}
	
	# If no such record is found in the database, check to see if the user has administrative
	# permission on the table. If so, then the operation can proceed and will result in a new
	# record with the specified primary key.
	
	if ( $permission eq 'notfound' )
	{
	    $permission = $edt->check_table_permission($table, 'admin');
	    
	    unless ( $permission eq 'admin' )
	    {
		$edt->add_record_condition('E_PERM', 'replace_new');
	    }
	}
	
	# If the user does not have permission to edit the record, add an error condition. 
	
	elsif ( $permission ne 'edit' && $permission ne 'admin' )
	{
	    $edt->add_record_condition('E_PERM', 'replace_old');
	}
	
	# Then check the actual record, to make sure that the new column values to be stored meet
	# all of the criteria for this table. If a 'check_replace' method was specified, then call
	# it. Otherwise, call the 'validate_record' method. In either case, the method must add an
	# error condition if any criteria are violated. It may also add warning conditions.
	
	$edt->_call_hooks($table, 'check_replace', 'replace', $record, $permission) ||
	    $edt->validate_record($table, 'replace', $record, $permission);
    }
    
    # If no primary key value was specified for this record, add an error condition. This will, of
    # course, have to be reported under the record label that was passed in as part of the record
    # (if one was in fact given) or else the index of the record in the input set.
    
    else
    {
	$edt->add_record_condition('E_NO_KEY', 'replace');
    }
    
    # Create an action record, and either execute it immediately or add it to the appropriate list
    # depending on whether or not any error conditions are found.
    
    return _handle_record($table, 'replace', $record, $permission);
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
    
    # If the record is given as a straight key value, create a record hash.
    
    if ( defined $record && ! ref $record && $record ne '' )
    {
	$record = { _key => $record };
    }
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being deleted.
    
    $edt->_new_record($record);
    
    my $permission;
    
    # A record can only be deleted if a primary key value is specified.
    
    if ( my $key = $edt->record_key )
    {
	# First check to make sure we have permission to edit this record. If an 'auth_delete'
	# method was defined, call it. This method must return a string indicating the permission
	# the user has on this record, or THE EMPTY STRING if none. Otherwise, we call the default
	# 'check_record_permission' method.
	
	if (  $edt->_call_hooks($table, 'auth_delete', 'delete', $record) )
	{
	    $permission = $edt->_hook_result;
	}
	
	else
	{
	    $permission = $edt->check_record_permission($table, 'delete', $key);
	}
	
	# If no such record is found in the database, add an error condition. If this
	# EditTransaction has been created with the 'PROCEED' or 'NOT_FOUND' allowance, it
	# will automatically be turned into a warning and will not cause the transaction to be
	# aborted.
	
	if ( $permission eq 'notfound' )
	{
	    $edt->add_record_condition('E_NOT_FOUND', 'delete');
	}
	
	# If we do not have permission to delete the record, add an error condition.
	
	elsif ( $permission ne 'delete' && $permission ne 'admin' )
	{
	    $edt->add_record_condition('E_PERM', 'delete');
	}
	
	# If a 'check_delete' method was specified, then call it. This method may abort the
	# deletion by adding an error condition. Otherwise, we assume that the permission check we
	# have already done is all that is necessary.
	
	$edt->_call_hooks($table, 'check_delete', 'delete', $record, $permission);
    }
    
    # If no primary key was specified, add an error condition.
    
    else
    {
	$edt->add_record_condition('E_NO_KEY', 'delete');
    }
    
    # Create an action record, and then take the appropriate action.
    
    return $edt->_handle_record($table, 'delete', $record, $permission);
}


# insert_update_record ( table, record )
# 
# Call either 'insert_record' or 'update_record', depending on whether the record has a value for
# the primary key attribute. This is a convenient shortcut for use by operation methods.

sub insert_update_record {
    
    my ($edt, $table, $record) = @_;
    
    $edt->_new_record($record);
    
    if ( $edt->record_key )
    {
	return $edt->update_record($table, $record);
    }
    
    else
    {
	return $edt->insert_record($table, $record);
    }
}


# ignore_record ( )
# 
# Indicates that a particular record that was sent by the user should be ignored. This will keep
# the record count up-to-date for generating record labels with which to tag subsequent error and
# warning messages.

sub ignore_record {

    my ($edt, $table, $record) = @_;
    
    $edt->{record_count}++;
}


# abandon_record ( )
# 
# This method may be called from record validation routines defined in subclasses of
# EditTransaction, if it is determined that a particular record action should be skipped but the
# rest of the transaction should proceed.

sub abandon_record {
    
    my ($edt) = @_;
    
    $edt->_clear_record;
}


# _handle_record ( action )
# 
# Handle the specified action record. If errors were generated for this record, put it on the 'bad
# record' list. Otherwise, either execute it immediately or put it on the action list to be
# executed later.

sub _handle_record {

    my ($edt, $action) = @_;
    
    # Create an action record.
    
    my $action = [ $operation, $table, $record, $permission, $edt->{current_keyval}, $edt->{current_label},
		   $edt->record_condition_ref ];
    
    # If errors were generated for this record, put it on the 'bad record' list and otherwise do
    # nothing.
    
    if ( $edt->record_errors )
    {
	push @{$edt->{bad_list}}, $action;
	return;
    }
    
    # If no errors have been accumulated from previous records, then we can proceed with this
    # action. We either execute it immediately, or put it on the action list to be executed after
    # all of the records are checked.
    
    unless ( $edt->errors )
    {
	unless ( $edt->{execute_immediately} )
	{
	    push @{$edt->{action_list}}, $action;
	    return;
	}
	
	if ( $action eq 'insert' )
	{
	    return $edt->_execute_insert($action);
	}
	
	elsif ( $action eq 'update' )
	{
	    return $edt->_execute_update($action);
	}
	
	elsif ( $action eq 'replace')
	{
	    return $edt->_execute_replace($action);
	}
	
	elsif ( $action eq 'delete' )
	{
	    return $edt->_execute_delete($action);
	}
	
	else
	{
	    croak "bad action '$action'";
	}
    }
    
    return;
}


# execute ( )
# 
# Start a database transaction, if one has not already been started. Then execute all of the
# pending insert/update/delete operations, and then either commit or rollback as
# appropriate. Returns true on success, false otherwise.

sub execute {
    
    my ($edt) = @_;
    
    # Finish processing of the final record that was added to the action list, if any.
    
    $edt->_finish_record;
    
    # If errors have already occurred (i.e. when records were checked for insertion or updating),
    # then return without doing anything. If a transaction is already active, then roll it back.
    
    if ( $edt->errors )
    {
	$edt->rollback if $edt->{transaction} eq 'active';
	return;
    }
    
    # If there are no actions to do, and none have been done so far, then rollback any transaction
    # and return unless the NO_RECORDS condition is allowed.
    
    unless ( @{$edt->{action_list}} || $edt->{action_count} )
    {
	unless ( $edt->allows('NO_RECORDS' ) )
	{
	    $edt->rollback if $edt->{transaction} eq 'active';
	    $edt->add_condition('C_NO_RECORDS');
	    return;
	}
    }
    
    # The main part of this routine is executed inside a try block, so that we can roll back the
    # transaction if any errors occur.
    
    my $result;
    
    try {
	
	# If we haven't already executed 'start_transaction' on the database, do so now.
	
	$edt->start_transaction unless $edt->{transaction} eq 'active';
	
	# Then go through the action list and execute each action in turn. If there are multiple
	# inserts or deletes in a row on the same table, handle them with a single call for
	# efficiency.
	
	while ( my $action = shift @{$edt->{action_list}} )
	{
	    last if $edt->errors;
	    
	    my $operation = $action->[0];
	    
	    $edt->{current_keyval} = $action->[4];
	    $edt->{current_label} = $action->[5];
	    
	    if ( $operation eq 'insert' )
	    {
		# push @records, $record;
		
		# while ( $edt->{action_list}[0] && $edt->{action_list}[0][0] eq 'insert' &&
		# 	$edt->{action_list}[0][1] eq $table &&
		#         ref $edt->{action_list}[0][2] eq 'HASH' )
		# {
		#     my $next_action = shift @{$edt->{action_list}};
		#     push @records, $next_action->[2];
		# }
		
		$edt->_execute_insert($action);
	    }
	    
	    elsif ( $operation eq 'update' )
	    {
		$edt->_execute_update($action);
	    }
	    
	    elsif ( $operation eq 'replace' )
	    {
		$edt->_execute_replace($action);
	    }
	    
	    elsif ( $operation eq 'delete' )
	    {
		# push @records, $record;
		
		# while ( $edt->{action_list}[0] && $edt->{action_list}[0][0] eq 'delete' &&
		# 	$edt->{action_list}[0][1] eq $table &&
		#         $edt->{action_list}[0][2] )
		# {
		#     my $next_action = shift @{$edt->{action_list}};
		#     push @records, $next_action->[2];
		# }
		
		$edt->_execute_delete($action);
	    }
	}
	
	# If we have an 'end_transaction' hook, call it now. The subroutine will be passed the
	# number of actions that succeeded, and the number that failed.
	
	$edt->_call_hooks($table, 'end_transaction', $edt->{action_count}, $edt->{fail_count});
	
	# If any errors have occurred, we roll back the transaction.
	
	if ( $edt->errors )
	{
	    $edt->rollback;
	}
	
	# Otherwise, we're good to go! Yay!
	
	else
	{
	    $edt->commit;
	    $result = 1;
	}
    }
    
    # If an exception is caught, we roll back the transaction and add an error condition.
    
    catch {

	$edt->rollback;
	print STDERR "$_\n\n";
	$edt->add_condition('E_EXECUTE', 'execute');
    };
    
    return $result;
}


# _execute_insert ( table, record )
# 
# Actually perform an insert operation on the database. The record keys and values have been
# checked by 'validate_record' or some other code, and lists of columns and values generated.

sub _execute_insert {

    my ($edt, $action) = @_;
    
    my ($operation, $table, $record, $permission) = @$action;
    
    # Start by calling the 'before_insert' hook. This can be used to do any necessary auxiliary
    # actions to the database.
    
    $edt->_call_hooks($table, 'before_insert', 'insert', $record, $permission);
    
    # Construct the insert statement.
    
    unless ( ref $record->{_columns} eq 'ARRAY' && ref $record->{_values} eq 'ARRAY' &&
	     @{$record->{_columns}} == @{$record->{_values}} )
    {
	$edt->add_condition('E_EXECUTE', 'internal error: column/value mismatch');
	return;
    }
    
    my $dbh = $edt->dbh;
    
    my $column_list = join(',', @{$record->{_columns}});
    my $value_list = join(',', @{$record->{_values}});
    
    my $sql = "	INSERT INTO $table ($column_list)
		VALUES ($value_list)";
    
    my $new_keyval;
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    try {
	
	my ($result) = $dbh->do($sql);
	
	if ( $result )
	{
	    $new_keyval = $dbh->last_insert_id(undef, undef, undef, undef);
	}
	
	unless ( $result && $new_keyval )
	{
	    $edt->add_record_condition('E_EXECUTE', 'insert');
	}
    }
    
    catch {
	
	$edt->add_record_condition('E_EXECUTE', 'insert');
    };
    
    # Now call the 'after_insert' hook. This can be used to do any necessary auxiliary actions to
    # the database. This is passed an extra argument, which will contain the primary key of the
    # newly inserted record. If the insert failed, it will be undefined.
    
    $edt->_call_hooks($table, 'after_insert', 'insert', $record, $permission, $new_keyval);
    
    # If the insert succeeded, return the new primary key value. Otherwise, return undefined.
    
    if ( $new_keyval )
    {
	$edt->{action_count}++;
	$edt->{inserted_keys}{$new_keyval} = 1;
	return $new_keyval;
    }
    
    else
    {
	$edt->{fail_count}++;
	return undef;
    }
}


# _execute_replace ( table, record )
# 
# Actually perform an replace operation on the database. The record keys and values have been
# checked by 'validate_record' or some other code, and lists of columns and values generated.

sub _execute_replace {

    my ($edt, $action) = @_;
    
    my ($operation, $table, $record, $permission, $keyval) = @$action;
    
    # Start by calling the 'before_replace' hook. This can be used to do any necessary auxiliary
    # actions to the database.
    
    $edt->_call_hooks($table, 'before_replace', 'replace', $record, $permission, $keyval);
    
    # Construct the replace statement.
    
    unless ( ref $record->{_columns} eq 'ARRAY' && ref $record->{_values} eq 'ARRAY' &&
	     @{$record->{_columns}} == @{$record->{_values}} )
    {
	$edt->add_condition('E_EXECUTE', 'internal error: column/value mismatch');
	return;
    }
    
    my $dbh = $edt->dbh;
    
    my $column_list = join(',', @{$record->{_columns}});
    my $value_list = join(',', @{$record->{_values}});
    
    my $sql = "	REPLACE INTO $table ($column_list)
		VALUES ($value_list)";
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    try {
	
	my ($result) = $dbh->do($sql);
	
	unless ( $result )
	{
	    $edt->add_record_condition('E_EXECUTE', 'replace');
	}
    }
    
    catch {
	
	$edt->add_record_condition('E_EXECUTE', 'replace');
    };
    
    # Now call the 'after_replace' hook. This can be used to do any necessary auxiliary actions to
    # the database. This is passed an extra argument, which will contain the primary key of the
    # newly replaced record. If the replace failed, it will be undefined.
    
    $edt->_call_hooks($table, 'after_replace', 'replace', $record, $permission, $keyval);
    
    # If the replace succeeded, return the new primary key value. Otherwise, return undefined.
    
    if ( $result )
    {
	$edt->{action_count}++;
	$edt->{replaced_keys}{$keyval} = 1;
	return $result;
    }
    
    else
    {
	$edt->{fail_count}++;
	return undef;
    }
}


# _execute_update ( table, record )
# 
# Actually perform an update operation on the database. The keys and values have been checked
# previously.

sub _execute_update {

    my ($edt, $action) = @_;
    
    my ($operation, $table, $record, $permission, $keyval) = @$action;
    
    # Start by calling the 'before_update' hook. This can be used to do any necessary auxiliary
    # actions to the database. It is passed the key value of the record to be updated.
    
    $edt->_call_hooks($table, 'before_update', 'update', $record, $permission);
    
    # Construct the update statement.
    
    unless ( ref $record->{_columns} eq 'ARRAY' && ref $record->{_values} eq 'ARRAY' &&
	     @{$record->{_columns}} == @{$record->{_values}} )
    {
	$edt->add_condition('E_EXECUTE', 'internal error: column/value mismatch');
	return;
    }
    
    my $dbh = $edt->dbh;
    
    my $set_list = '';
    
    foreach my $i ( 0..$#{$record->{_columns}} )
    {
	$set_list .= ', ' if $set_list;
	$set_list .= $record->{_columns}[$i] . '=' . $record->{_values}[$i];
    }
    
    my $key_expr = $dbh->record_keyexpr;
    
    my $sql = "	UPDATE $table SET $set_list
		WHERE $key_expr";
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    try {
	
	my ($result) = $dbh->do($sql);
	
	unless ( $result )
	{
	    $edt->add_record_condition('E_EXECUTE', 'insert');
	}
	
	# $$$ we maybe should set RaiseError instead?
    }
    
    catch {
	
	$edt->add_record_condition('E_EXECUTE', 'insert');
    };
    
    # Now call the 'after_update' hook. This can be used to do any necessary auxiliary actions to
    # the database. This is passed two extra arguments. The first contains the key value of the
    # record to be updated, and the second will be true if the update succeeded and false otherwise.
    
    $edt->_call_hooks($table, 'after_update', 'update', $record, $permission, $result);
    
    # If the update succeeded, return true. Otherwise, return false.
    
    if ( $result )
    {
	$edt->{action_count}++;
	$edt->{inserted_keys}{$keyval} = 1;
	return $result;
    }
    
    else
    {
	$edt->{fail_count}++;
	return undef;
    }
}


# execute_delete ( table, record )
# 
# Actually perform a delete operation on the database. The lists of colu

sub execute_delete {

    my ($edt, $action) = @_;
    
    my ($operation, $table, $record, $permission, $keyval) = @$action;
    
    # Start by calling the 'before_delete' hook. This can be used to do any necessary auxiliary
    # actions to the database. It is passed the key value of the record to be deleted.
    
    $edt->_call_hooks($table, 'before_delete', 'delete', $record, $keyval);
    
    # Now delete the record.
    
    my $key_expr = $dbh->record_keyexpr;
    
    my $sql = "	UPDATE $table SET $set_list
		WHERE $key_expr";    
    
    # Now call the 'after_delete' hook. This can be used to do any necessary auxiliary actions to
    # the database. This is passed two extra arguments. The first contains the key value of the
    # record that was deleted, and the second will be true if the delete succeeded and false otherwise.
    
    $edt->_call_hooks($table, 'after_delete', $record, $keyval, $result);
    
    # If the delete succeeded, return true. Otherwise, return false.
    
    if ( $result )
    {
	$edt->{action_count}++;
	$edt->{deleted_keys}{$keyval} = 1;
	return $result;
    }
    
    else
    {
	$edt->{fail_count}++;
	return undef;
    }
}


# Permission checking
# -------------------

# The methods in this section simply call the equivalent methods of the Permissions object that
# was used to initialize this EditTransaction.

sub check_table_permission {

    my ($edt, $table, $permission) = @_;
    
    return $edt->{perms}->check_table_permission($table, $permission);
}

sub check_record_permission {
    
    my ($edt, $table, $permission, $key_expr, $record) = @_;
    
    return $edt->{perms}->check_table_permission($table, $permission, $key_expr, $record) = @_;
}


# Record validation
# -----------------

# The methods in this section provide default validation for records to be inserted and
# updated. This is done by comparing the field values to the types of the corresponding columns
# from the database schema for the table, plus any attributes specifically specified for the
# column using 'set_column_property' such as 'REQUIRED' and 'ADMIN_SET'.
# 
# Subclasses of EditTransaction can also register their own validation methods using the hooks
# 'check_insert', 'check_update', and 'check_delete'. If such registrations are made, then the
# methods specified below will NOT be called. But the subclass methods may call the ones specified
# below, as well as conducting their own checks. In any case, the mechanism by which validation
# routines communicate an error or warning is by calling 'add_record_condition'.

our (%SIGNED_BOUND) = ( tiny => 127,
			small => 32767,
			medium => 8388607,
			regular => 2147483647 );

our (%UNSIGNED_BOUND) = ( tiny => 255,
			  small => 65535,
			  medium => 16777215,
			  regular => 4294967295 );
			 


# validate_record ( table, record, permission, special )
# 
# Check over the field values to be inserted, and use the 'add_record_condition' method to record
# any error or warning conditions that are detected. The column names to be inserted are collected
# up as a list and added to the record under the key '_insert_columns', and the values under
# '_insert_values'.
# 
# The $permission argument provides the record permission that was previously determined, which
# may be either 'post' for ordinary users or 'admin' if the insertion is being done by a user with
# administrative privilege on this table. It is possible to mark certain fields as settable only
# by users with administrative privilege.
# 
# If the argument $special is given, it must be a hash ref whose keys are field names. Currently
# the only accepted value is 'skip', indicating that this field should be skipped. This is
# available for use when this routine is called from within a validation method defined by a
# subclass of EditTransaction.

sub validate_record {

    my ($edt, $table, $operation, $record, $permission, $special) = @_;
    
    croak "bad operation '$operation'" 
	unless $operation eq 'insert' || $operation eq 'update' || $operation eq 'mirror';
    
    # Grab the table schema, or throw an exception. This information is cached, so the database
    # will only need to be asked for this information once per process per table.
    
    my $schema = get_table_schema($table, $edt->debug);
    my $property = get_column_properties($table);
    my $dbh = $edt->dbh;
    
    # Start by going through the list of field names, and constructing a list of values to be
    # inserted.
    
    my (@columns, @values);
    
    foreach my $col ( @{$schema->{_column_list}} )
    {
	# Skip any columns we are directed to ignore. These will presumably handled by the code
	# from a subclass that has called this method.
	
	if ( $special && $special->{$c} && $special->{$c} eq 'skip' )
	{
	    next;
	}
	
	# The name under which the value is stored in the record provided us may not be exactly
	# the same as the database column name. Start with the assumption that it is, but if
	# the column ends in '_no' then also check for a corresponding column ending in '_id'.
	
	my $lookup_col = $col;
	
	unless ( exists $record->{$lookup_col} )
	{
	    if ( $c =~ qr{ ^ (.*) _no $ }xs )
	    {
		$lookup_col = $1 . '_id';
	    }
	}
	
	# Skip any columns that aren't included in the record, unless they are required and this
	# is a non-update operation.
	
	unless ( exists $record->{$lookup_col} )
	{
	    next unless $property->{$col}{REQUIRED} && $operation ne 'update';
	}
	
	# Grab the value specified in the record.
	
	my $value = $record->{$lookup_col};
	
	# Handle special columns in the appropriate ways.
	
	if ( my $type = $COMMON_FIELD_OTHER{$c} )
	{
	    # The 'crmod' fields store the record creation and modification dates. These cannot be
	    # specified explicitly except by a user with administrative permission, and then only
	    # if this EditTransaction allows the condition 'ALTER_TRAIL'. In that case, check to
	    # make sure that they have the proper format. But always ignore empty values.
	    
	    if ( $type eq 'crmod' )
	    {
		# If the value is empty, skip it and let it be filled in by the database engine.
		
		next unless defined $value && $value ne '';
		
		# Otherwise, check to make sure the user has permission to set a specific value.
		
		unless ( $permission eq 'admin' && $edt->allows('ALTER_TRAIL') )
		{
		    $edt->add_record_condition('E_PERM_COL', $lookup_col);
		    next;
		}
		
		# If so, check that the value matches the required format.
		
		unless ( $value =~ qr{ ^ \d\d\d\d - \d\d - \d\d (?: \s+ \d\d : \d\d : \d\d ) $ }xs )
		{
		    $edt->add_record_condition('E_PARAM', $lookup_col, 'invalid format');
		    next;
		}
	    }
	    
	    # The 'authent' fields store the identifiers of the record authorizer, enterer, and
	    # modifier. These are subject to the same conditions as the 'crmod' fields if
	    # specified explicitly. But empty values get filled in according to the values for the
	    # current user.
	    
	    elsif ( $type eq 'authent' )
	    {
		# If the value is not empty, check to make sure the user has permission to set a
		# specific value.
		
		if ( defined $value && $value ne '' )
		{
		    unless ( $permission eq 'admin' && $edt->allows('ALTER_TRAIL') )
		    {
			$edt->add_record_condition('E_PERM_COL', $lookup_col);
			next;
		    }
		    
		    if ( ref $value eq 'PBDB::ExtIdent' )
		    {
			unless ( $value->{type} eq 'PRS' )
			{
			    $edt->add_record_condition('E_PARAM', $lookup_col, 
						       "must be an external identifier of type '$IDP{PRS}'");
			}
			
			$value = $value->stringify;
		    }
		    
		    elsif ( ref $value || $value !~ qr{ ^ \d+ $ }xs )
		    {
			$edt->add_record_condition('E_PARAM', $lookup_col,
						   'must be an external identifier or an unsigned integer');
			next;
		    }
		    
		    unless ( $edt->check_key($PERSON_DATA, $value) )
		    {
			$edt->add_record_condition('E_KEY_NOT_FOUND', $lookup_col, $value);
		    }
		}
		
		# If (as is generally supposed to happen) no value is specified for this column,
		# then fill it in from the known information. The 'authorizer_no', 'enterer_no',
		# and 'enterer_id' fields are filled in on record insertion, and 'modifier_no' on
		# record update.
		
		elsif ( $col eq 'authorizer_no' && $operation ne 'update' )
		{
		    $value = $edt->{perms}->authorizer_no;
		}
		
		elsif ( $col eq 'enterer_no' && $operation ne 'update' )
		{
		    $value = $edt->{perms}->enterer_no;
		}
		
		elsif ( $col eq 'enterer_id' && $operation ne 'update' )
		{
		    $value = $edt->{perms}->user_id;
		}
		
		elsif ( $col eq 'modifier_no' && $operation eq 'update' )
		{
		    $value = $edt->{perms}->enterer_no;
		}
		
		elsif ( $col eq 'modifier_id' && $operation eq 'update' )
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
		
		unless ( $permission eq 'admin' && $edt->allows('ALTER_TRAIL') )
		{
		    $edt->add_record_condition('E_PERM_COL', $col);
		    next;
		}
		
		# If so, make sure the value is correct.
		
		if ( $col eq 'admin_lock' && not ( $value eq '1' || $value eq '0' ) )
		{
		    $edt->add_record_condition('E_PARAM', $col, 'value must be 1 or 0');
		    next;
		}
	    }
	    
	    else
	    {
		croak "bad internal field type";
	    }
	}
	
	# Otherwise, if the value is not empty then validate against the column definition.
	
	elsif ( defined $value && $value ne '' )
	{
	    # Handle references to keys from other PBDB tables by checking them against the
	    # specified table. We use a soft reference because the system of table names is based
	    # on global variables, whose values might change. Yes, I know this is not the best way
	    # to do it.
	    
	    if ( my $foreign_table = $FOREIGN_KEY_TABLE{$col} )
	    {
		if ( $value )
		{
		    no strict 'refs';
		    
		    my $foreign_table_name = ${$foreign_table};
		    
		    unless ( $edt->check_key($foreign_table_name, $value) )
		    {
			$edt->add_record_condition('E_KEY_NOT_FOUND', $lookup_col, $value);
			next;
		    }
		}
	    }
	    
	    # Otherwise, check the column type.
	    
	    elsif ( my $type = $schema->{$col}{Type} )
	    {
		# If the type is char or varchar, we only need to check the maximum length.
		
		if ( $type =~ qr{ ^ (?: var )? char \( ( \d+ ) }xs )
		{
		    if ( length($value) > $1 )
		    {
			$edt->add_record_condition('E_PARAM', $lookup_col, "must be no more than $1 characters");
			next;
		    }
		}
		
		# If the type is text or tinytext, similarly.
		
		elsif ( $type =~ qr{ ^ (tiny)? text }xs )
		{
		    my $max_length = $1 ? 255 : 65535;
		    
		    if ( length($value) > $max_length )
		    {
			$edt->add_record_condition('E_PARAM', $lookup_col, "must be no more than $1 characters");
			next;
		    }
		}
		
		# If the type is integer, do format and bound checking. Special case booleans,
		# which are represented as tinyint(1).
		
		elsif ( $type =~ qr{ ^ (tiny|small|medium|big)? int \( (\d+) \) \s* (unsigned)? }xs )
		{
		    my $size = $1 || 'regular';
		    my $tinyint = $1;
		    my $unsigned = $2;
		    
		    if ( $tinyint eq '1' )
		    {
			if ( $value !~ qr{ ^ [01] $ }xs )
			{
			    $edt->add_record_condition('E_PARAM', $lookup_col, "must be 0 or 1");
			    next;
			}
		    }
		    
		    elsif ( $unsigned )
		    {
			if ( $value !~ qr{ ^ \d+ $ }xs || $value > $UNSIGNED_BOUND{$size} )
			{
			    $edt->add_record_condition('E_PARAM', $lookup_col, 
						       "must be an unsigned integer no greater than $UNSIGNED_BOUND{$size}");
			    next;
			}
		    }
		    
		    else
		    {
			if ( $value !~ qr{ ^ -? \s* \d+ $ }xs )
			{
			    $edt->add_record_condition('E_PARAM', $lookup_col, "must be an integer");
			    next;
			}
			
			elsif ( $value > $SIGNED_BOUND{$size} || -1 * $value > $SIGNED_BOUND{$size} + 1 )
			{
			    my $lower = $SIGNED_BOUND{$size} + 1;
			    $edt->add_record_condition('E_PARAM', $lookup_col,
						       "must be an integer between -$lower and $SIGNED_BOUND{$size}");
			    next;
			}
		    }
		}
		
		# If the type is decimal, do format and bound checking. 
		
		elsif ( $type =~ qr{ ^ decimal \( (\d+) , (\d+) \) \s* (unsigned)? }xs )
		{
		    # $$$ we need to add bounds checking
		    
		    if ( $unsigned )
		    {
			if ( $value !~ qr{ ^ (?: \d+ [.] \d* | \d* [.] \d+ ) }xs )
			{
			    $edt->add_record_condition('E_PARAM', $lookup_col, "must be an unsigned decimal number");
			    next;
			}
		    }
		    
		    else
		    {
			if ( $value !~ qr{ ^ -? (?: \d+ [.] \d* | \d* [.] \d+ ) }xs )
			{
			    $edt->add_record_condition('E_PARAM', $lookup_col, "must be a decimal number");
			    next;
			}
		    }
		}
		
		# $$$ should add float later
		
		# Otherwise, we just throw up our hands and accept whatever they give us. This
		# might not be wise.
	    }
	}
	
	# Now, if the value is required and empty, then add an error. But not if this is an update
	# operation where no value was specified. In that case, we assume that the existing value
	# will be left unchanged whatever it is.
	
	if ( $property->{$col}{REQUIRED} && ! ( defined $value && $value ne '' ) )
	{
	    unless ( $operation eq 'update' && ! exists $record->{$lookup_col} )
	    {
		$edt->add_record_condition('E_REQUIRED', $lookup_col);
	    }
	}
	
	# If we get here, then we have a good value! Push the column and value on the respective
	# lists. An undefined value is pushed as NULL, otherwise the value is quoted. The default
	# behavior for mariadb when given the empty string as a value for a numeric column is to
	# store zero. So we'll go with that.
	
	push @columns, $col;
	
	if ( defined $value )
	{
	    push @values, $dbh->quote($value);
	}
	
	else
	{
	    push @values, 'NULL';
	}
    }
    
    # Now store our column and value lists under the record keys _columns and _values
    # respectively. 
    
    $record->{_columns} = \@columns;
    $record->{_values} = \@values;
    
    return;
}


1;
