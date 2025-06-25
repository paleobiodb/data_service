# 
# EditTransaction::IActions
# 
# This role provides methods for creating and executing the actions that perform
# the work of each transaction. The extra 'I' at the beginning of the name makes
# it easy to differentiate from EditTransaction::Action, which is the class for
# the action objects themselves.
# 


package EditTransaction::IActions;

use strict;

use Switch::Plain;
use Scalar::Util qw(weaken reftype);
use List::Util qw(any);
use Carp qw(carp croak);

use EditTransaction::Operations qw(%VALID_OPERATION);

use Role::Tiny;

no warnings 'uninitialized';



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


# _action_args ( operation, [table_specifier], parameters )
# 
# Parse the arguments that were passed to an action-initiation routine. If the
# values are not acceptable, throw an error or add a condition as appropriate.

sub _action_args {
    
    my ($edt, $operation, @rest) = @_;
    
    # If the first argument is not a parameter hash or a key string, assume it
    # is a table specifier. Otherwise, the table specifier defaults to the one
    # provided when this EditTransaction was created. If no default table was
    # provided, throw an exception unless the operation is 'skip'.
    
    my $table_specifier;
    
    if ( @rest )
    {
	unless ( ref $rest[0] || $operation =~ /^delete/ && $rest[0] =~ /^[^\w]*\d/ )
	{
	    $table_specifier = shift @rest;
	}
    }
    
    $table_specifier ||= $edt->{default_table};
    
    unless ( $table_specifier || $operation eq 'skip' )
    {
	croak "you must specify a table to operate on";
    }
    
    # The next argument should be the parameters.
    
    return ($table_specifier, @rest);
}


# _new_action ( operation, table_specifier, parameters )
# 
# Generate a new action object that will act on the specified table using the specified operation
# and the parameters contained in the specified input record. The record which must be a hashref,
# unless the operation is 'delete' in which case it may be a single primary key value or a
# comma-separated list of them.

sub _new_action {
    
    my ($edt, $operation, $table_specifier, $parameters) = @_;
    
    # We cannot proceed unless we have a valid database handle.
    
    croak "no database connection" unless $edt->{dbh};
    
    # We cannot proceed if the transaction has already finished.
    
    croak "you may not add new actions to a transaction that has completed"
	if $edt->has_finished;
    
    # If the operation is not 'skip', throw an exception if no parameters were
    # given.
    
    if ( $operation ne 'skip' )
    {
	croak "you must specify parameters for this action" 
	    unless defined $parameters || $operation eq 'other';
	
	if ( $operation =~ /^delete|^other$/ )
	{
	    $parameters = { _primary => $parameters } unless ref $parameters eq 'HASH';
	}
	
	else
	{
	    croak "the parameter argument must be a hashref" unless
		ref $parameters eq 'HASH';
	}
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
    
    foreach my $k (@refs)
    {
	$edt->{action_ref}{$k} = $action;
	weaken $edt->{action_ref}{$k};
    }
    
    # Make this the current action. It will be the default action during
    # execution of the methods 'authorize_action' and 'validate_action' in
    # subclasses.
    
    $edt->{current_action} = $action;
    
    # If the action is not 'skip' and the table specifier is not empty, check it
    # and unpack key values from the action parameters. Then load the directives
    # for this table as specified for this class.
    
    if ( $operation ne 'skip' && $table_specifier )
    {
	if ( $edt->table_info_ref($table_specifier) )
	{
	    $edt->unpack_key_values($action, $table_specifier, $operation, $parameters);
	    
	    $edt->{action_tables}{$table_specifier} = 1;
	    $edt->init_directives($table_specifier);
	}
	
	else
	{
	    $edt->add_condition('E_BAD_TABLE', $table_specifier);
	}
    }
    
    # Return a reference to the new action.
    
    return $action;
}


# _test_action ( operation, table_specifier, parameters )
# 
# This routine is intended only for use by the test suite for this module. It
# returns a Perl reference to the action instead of a refstring. It does not do
# authentication nor validation, and it does not call _handle_action. It does,
# however, unpack key values and import conditions.

sub _test_action {
    
    my ($edt, $operation, @rest) = @_;
    
    # Create a new object to represent this action.
    
    my ($table_specifier, $parameters) = $edt->_action_args($operation, @rest);
    
    my $action = $edt->_new_action($operation, $table_specifier, $parameters);
    
    if ( my $ew = $action->record_value('_errwarn') )
    {
	$edt->import_conditions($action, $ew);
    }
    
    return $action;
}


# abort_action ( action_ref )
# 
# This method may be called from either 'validate_action' or 'before_action', if it is determined
# that a particular action should be skipped but the rest of the transaction should proceed. If no
# action reference is given, the most recent action is aborted if possible. It may also be called
# from client code if the action has not yet been executed, and if we are not in immediate
# execution mode.

sub abort_action {
    
    my ($edt) = @_;
    
    if ( my $action = &action_ref )
    {
	# If the action has already been skipped or aborted, return true. This makes
	# the method idempotent.
	
	if ( $action->status eq 'skipped' )
	{
	    return 1;
	}
	
	# If the action has already been executed or the transaction has been
	# completed, return false.
	
	elsif ( $edt->has_finished || $action->has_executed )
	{
	    return '';
	}
	
	# Otherwise, wipe the slate clean with respect to this action. The
	# action status will be set to 'aborted', and its conditions will be
	# removed from the transaction counts. This may allow the transaction to
	# proceed if it was blocked only by those errors and not any others.
	
	else
	{
	    # If this action had previously failed, decrement the fail count.
	    # This method is not allowed to be called on executed actions, so
	    # the executed count does not need to be adjusted.
	    
	    $edt->{fail_count}-- if $action->status eq 'failed';
	    
	    # Set the action status to 'skipped' and increment the skip count.
	    
	    $action->set_status('skipped');
	    
	    $edt->{skip_count}++;
	    
	    # If this action has any errors or warnings, remove them from the
	    # appropriate counts. This may allow the transaction to proceed if
	    # it was blocked only by these errors. The conditions themselves are
	    # left in place in the action record, so they will be included
	    # whenever all actions are listed.
	    
	    $edt->_remove_conditions($action);
	    
	    # If this is a child action, do the same thing for the child counts
	    # associated with its parent action.
	    
	    $action->clear_conditions_from_parent();
	    
	    # Return true, because the action has successfully been aborted.
	    
	    return 1;
	}
    }
    
    # Otherwise, no matching action was found.
    
    else
    {
	return undef;
    }
}


# add_child_action ( [action], table, operation, record )
# 
# This method is called from client code or subclass methods that wish to create auxiliary actions
# to supplement the current one. For example, adding a record to one table may involve also adding
# another record to a different table. If the first parameter is an action reference (either a
# string or a Perl reference), the new action is attached to that. Otherwise, it is attached to
# the current action.

sub add_child_action {
    
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
    
    my $op = shift @params;
    
    croak "invalid operation '$op'" unless $VALID_OPERATION{$op};
    
    my ($table_specifier, $parameters) = $edt->_action_args($op, @params);
    
    my $child = $edt->_new_action($op, $table_specifier, $parameters);
    
    $parent->add_child($child);
    
    # Because this action is a child of an authorized action, it is assumed to
    # be authorized.

    $child->set_permission('modify');
    
    # Validate the new action, unless this is a delete.
    
    if ( $op =~ /^ins|^upd|^rep/ )
    {
	eval {
	    $edt->validate_action($child, $op, $table_specifier);
	    
	    if ( $child->can_proceed )
	    {
		$edt->validate_against_schema($child, $op, $table_specifier);
	    }
	};
	
	if ( $@ )
	{
	    $edt->error_line($@);
	    $edt->add_condition($child, 'E_EXECUTE', 
				'an exception occurred during validation of a child action');
	}
    }
    
    # Regardless of any added conditions, handle the action and return the action
    # reference.
    
    $edt->_handle_action($child);
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
# notex        Return all actions that were skipped, abandoned, or failed
# failed       Return all actions that failed
# skipped      Return all actions that were skipped
# blocked      Return all actions that were blocked

our (%STATUS_SELECTOR) = (all => 1, completed => 1, pending => 1, executed => 1,
			  notex => 1, blocked => 1, failed => 1, skipped => 1);

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
    
    my $status = $action->status || '';
    my $operation = $action->operation;
    my $parameters;
    
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
    
    # If we have the original input record, start with that. Otherwise, try to
    # create one.
    
    unless ( $parameters = $action->record )
    {
	my $keycol = $action->keycol;
	
	if ( $action->keymult && $keycol )
	{
	    $parameters = { $keycol => [ $action->keyvalues ] };
	}

	elsif ( $action->keyval && $keycol )
	{
	    $parameters = { $keycol => $action->keyval };
	}
	
	else
	{
	   $parameters = { };
	}
    }
    
    # Create a hashref to represent the action. If the status is empty, it
    # defaults to 'pending'.
    
    my $result = { refstring => $action->refstring,
		   operation => $action->operation,
		   table => $action->table,
		   params => $parameters,
		   status => ($status || 'pending') };
    
    # If this action has conditions attached, add them now.
    
    my @conditions = map ref $_ eq 'ARRAY' ? $edt->condition_nolabel($_->@*) : undef,
	$action->conditions;
    
    $result->{conditions} = \@conditions if @conditions;
    
    return $result;
}


sub _action_list {

    return ref $_[0]{action_list} eq 'ARRAY' ? $_[0]{action_list}->@* : ();
}


# action_ref ( ref )
# 
# If no argument is given, or if the argument is '&_', return a reference to
# the current action if any. Otherwise, if the argument refers to an action that
# is defined for this EditTransaction, return a reference to the corresponding
# action object. Otherwise, return undefined. The argument may be either a Perl
# reference to an action object or else the reference string associated with an
# action object.
# 
# This method is designed to be used internally, and should be used sparingly
# and with caution by interface code. The structure and interface of the action
# object may change with subsequent releases of this codebase, which limits the
# usefulness of action object references. Whenever possible, use the methods
# defined in this section instead.
# 
# All of the following methods take the same argument, and interpret it the same
# way.

sub action_ref {
    
    my ($edt, $selector) = @_;
    
    if ( ref $selector )
    {
	local($_);
	
	croak "not an action reference" unless ref $selector eq 'EditTransaction::Action';
	
	if ( ref $edt->{action_list} eq 'ARRAY' && any { $_ eq $selector } $edt->{action_list}->@* )
	{
	    return $selector;
	}
    }
    
    elsif ( ! defined $selector || $selector eq '&_' )
    {
	return $edt->{current_action} || 
	    ref $edt->{action_list} eq 'ARRAY' && $edt->{action_list}[-1];
    }
    
    elsif ( $selector =~ /^&/ )
    {
	if ( $edt->{action_ref}{$selector} )
	{
	    return $edt->{action_ref}{$selector};
	}
    }
    
    # If we get here, there is no matching action.
    
    return undef;
}


# sub _action_ref_args {
    
#     my ($edt, $selector, @args) = @_;
    
#     if ( ref $selector )
#     {
# 	local($_);
	
# 	croak "not an action reference" unless ref $selector eq 'EditTransaction::Action';
	
# 	if ( $edt->{action_list} && any { $_ eq $selector } $edt->{action_list}->@* )
# 	{
# 	    unshift @args, $selector;
# 	    return @args;
# 	}
#     }
    
#     elsif ( ! defined $selector || $selector eq '&_' )
#     {
# 	unshift @args, $edt->{current_action};
# 	return @args;
#     }
    
#     elsif ( defined $selector && $selector =~ /^&/ )
#     {
# 	if ( $edt->{action_ref}{$selector} )
# 	{
# 	    unshift @args, $edt->{action_ref}{$selector};
# 	    return @args;
# 	}
#     }
    
#     elsif ( $edt->{current_action} )
#     {
# 	unshift @args, $selector if @_ > 1 && $selector ne '_';
# 	return @args;
#     }
    
#     # If we get here, return the empty list.
    
#     return;
# }


# has_action ( ref )
# 
# If the argument refers to an action that is defined for this EditTransaction,
# return true. If no argument is given, return true if at least one action
# has been defined for this EditTransaction. Return false otherwise.
# 
# This method can be used to verify that an action reference is valid.

sub has_action {

    return &action_ref ? 1 : '';
}


# action_status ( ref )
# 
# If the argument refers to an action which is part of this EditTransaction,
# return the action's status. If no argument is given, return the status of the
# most recent action if any. Otherwise, return undefined.
# 
# If the return value is defined, it will be one of the following:
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
    
    my ($edt) = @_;
    
    if ( my $action = &action_ref )
    {
	my $status = $action->status;
	
	# If the transaction has fatal errors, the action status will be 'unexecuted' if the action was
	# not failed or skipped.
	
	if ( $edt->has_errors && $status !~ /^failed|^skipped/ )
	{
	    return 'unexecuted';
	}
	
	# An empty action status is returned as 'pending'.
	
	else
	{
	    return $status || 'pending';
	}
    }
}


# action_ok ( ref )
# 
# If the argument refers to an action that is part of this EditTransaction,
# return true if it has either executed successfully or is still pending and has
# no error conditions. Return false otherwise. If no argument is given, use the
# most recent action if any. If there is no matching action, return undefined.

sub action_ok {

    if ( my $action = &action_ref )
    {
	return ! $action->has_errors && $action->status =~ /^executed$|^$/;
    }
}


# action_keyval ( ref )
# 
# If the argument refers to an action that is part of this EditTransaction,
# return its key value if any. If no argument is given, use the most recent
# action if any. If there is no matching action, or if the matching action has
# no key value, return undefined.
# 
# The 'get_keyval' method is an alias to this one.

sub action_keyval {

    if ( my $action = &action_ref )
    {
	return $action->keyval || '';
    }
}


sub get_keyval {

    goto &action_keyval;
}


# action_keyvalues ( ref )
# 
# If the argument refers to an action that is part of this EditTransaction,
# return a list of its key values if any. If no argument is given, use the most
# recent action if any. If there is no matching action, or if the matching
# action has no key values, return the empty list.

sub action_keyvalues {
    
    if ( my $action = &action_ref )
    {
	return $action->keyvalues;	
    }
    
    else
    {
	return ();
    }
}


# action_keymult ( ref )
# 
# If the argument refers to an action that is part of this EditTransaction,
# return true if it has multiple key values and false otherwise. If no argument
# is given, use the most recent action if any. If there is no matching action,
# or if the matching action has no key values, return undefined.

sub action_keymult {

    if ( my $action = &action_ref )
    {
	return $action->keymult || '';
    }
}


# action_table ( ref )
# action_operation ( ref )
# action_parent ( ref )
# action_result ( ref )
# action_record ( ref )
# action_parameter ( ref, parameter_name )
# 
# Return the specified action attribute if a matching action is found. Return
# undefined otherwise.

sub action_table {
    
    if ( my $action = &action_ref )
    {
	return $action->table || '';
    }
}


sub action_operation {

    if ( my $action = &action_ref )
    {
	return $action->operation || '';
    }
}


sub action_parent {
    
    if ( my $action = &action_ref )
    {
	return $action->parent || '';
    }
}


sub action_result {
    
    if ( my $action = &action_ref )
    {
	return defined $action->{result} ? $action->{result} : '';
    }
}


sub action_matched {
    
    if ( my $action = &action_ref )
    {
	return defined $action->{matched} ? $action->{matched} : '';
    }
}


sub action_record {
    
    if ( my $action = &action_ref )
    {
	return $action->record || '';
    }
}


sub action_parameter {
    
    my ($edt, $arg1, $arg2) = @_;
    
    my ($action, $parameter) = @_;
    
    if ( $arg1 && $arg1 =~ /^&/ || ref $arg1 )
    {
	$action = &action_ref;
	$parameter = $arg2;
    }
    
    elsif ( $arg1 )
    {
	$action = $edt->{current_action};
	$parameter = $arg1;
    }
    
    if ( $action && $parameter )
    {
	return $action->record_value($parameter);
    }
    
    elsif ( $parameter )
    {
	return undef;
    }
    
    else
    {
	croak "you must specify a parameter name";
    }
}


# current_action ( )
#
# Return the refstring corresponding to the current action, if any. Otherwise,
# return the empty string.

sub current_action {

    my ($edt) = @_;
    
    return $edt->{current_action} && $edt->{current_action}->refstring // '';
}


1;
