# 
# EditTransaction::Directives
# 
# This role provides methods for setting and querying handling directives that
# modify the semantics of individual database columns.
# 


package EditTransaction::Directives;

use strict;

use Switch::Plain;
use List::Util qw(sum);
use Carp qw(carp croak);

use Moo::Role;

no warnings 'uninitialized';


our (%DIRNAME_BY_CLASS) = ( EditTransaction => {
		none => 1,
		ignore => 1,
		pass => 1,
		unquoted => 1,
		copy => 1,
		validate => 1,
		ts_created => 1,
		ts_modified => 1 });


our (%DIRECTIVES_BY_CLASS);


# Column handling directives
# --------------------------

# The following methods can be used to control how particular fields are
# validated. Column directives affect the value specified for that column,
# regardless of what name the value was specified under. The 'handle_column'
# method can be called from 'initialize_transaction', from a 'before' method on
# 'validate_action', or else during class initiation. Directives can be
# overridden at the action level by calling the 'handle_column' method on the
# action.
# 
# The available column directives are:
# 
# validate      This is the default for any column that does not have a
#               different directive assigned to it. Any assigned value will be
#               checked against the column type and attributes, and will be
#               stored to that column in the database if no errors are found.
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
# none	        Remove any previously specified directive.
# 
# ts_created    This column will hold the date/time of record creation.
# 
# ts_modified   This column will hold the date/time of last modification.
# 
# Plug-in application modules such as PaleoBioDB.pm can define extra directives.
# 
# You can cause a particular hash key to be ignored when it occurs in a hash of
# action parameters by using the special form "FIELD:<key>" as the column name
# and 'ignore' as the directive. Such keys will no longer generate E_BAD_FIELD
# or W_BAD_FIELD conditions.


# register_directives ( name... )
# 
# Register the names of extra column directives. This is designed to be called
# from plug-in Application modules, listing any extra directives that the module
# can handle.

sub register_directives {
    
    my ($class, @names) = @_;
    
    croak "'register_directives' must be called as a class method" if ref $class;
    
    foreach my $n ( @names )
    {
	croak "'$n' is not a valid directive name" unless $n =~ /^[\w-]+$/;
	$DIRNAME_BY_CLASS{$class}{$n} = 1;
    }
    
    return scalar(@names);
}


sub copy_directives_from {
    
    my ($class, $from_class) = @_;
    
    return if $class eq $from_class;
    
    if ( ref $DIRNAME_BY_CLASS{$from_class} eq 'HASH' )
    {
	foreach my $n ( keys %{$DIRNAME_BY_CLASS{$from_class}} )
	{
	    $DIRNAME_BY_CLASS{$class}{$n} = $DIRNAME_BY_CLASS{$from_class}{$n};
	}
    }
}


# is_valid_directive ( name )
# 
# This can be called either as a class method or an instance method. It returns
# true if the specified directive is available for this class or instance.

sub is_valid_directive {
    
    my ($class, $name) = @_;
    
    if ( ref $class )
    {
	return $DIRNAME_BY_CLASS{ref $class}{$name} || $DIRNAME_BY_CLASS{EditTransaction}{$name};
    }
    
    else
    {
	return $DIRNAME_BY_CLASS{$class}{$name} || $DIRNAME_BY_CLASS{EditTransaction}{$name};
    }
}


# class_directives_list ( class, table_specifier )
# 
# This may be called either as an instance method or as a class method. Returns a list of columns
# and directives stored in the global directive cache for the given class and table, suitable for
# assigning to a hash.

sub class_directives_list {

    my ($edt, $table_specifier) = @_;
    
    my $class = ref $edt || $edt;
    
    return map { ref $DIRECTIVES_BY_CLASS{$_}{$table_specifier} eq 'HASH' ?
		     $DIRECTIVES_BY_CLASS{$_}{$table_specifier}->%* : () }
	'EditTransaction', $class;
}


# init_directives ( table_specifier )
# 
# The first call to this method for a given table specifier will cause the
# global directives for this table specifier to be initialized if they haven't
# already been, and then the local directives initialized if they haven't
# already been. Returns a reference to the directives hash for this table in the
# specified transaction, or to an empty hash if there are none.
# 
# This must be called as an instance method.

sub init_directives {

    my ($edt, $table_specifier) = @_;
    
    # If we have already initialized the directives for this table, return a
    # reference.
    
    if ( ref $edt->{directives}{$table_specifier} eq 'HASH' )
    {
	return $edt->{directives}{$table_specifier};
    }
    
    # Otherwise, make sure we have a recognized table specifier and initialize
    # the directives.
    
    elsif ( $edt->table_info_ref($table_specifier) )
    {
	$edt->{directives}{$table_specifier} = { $edt->table_directives_list($table_specifier),
						 $edt->class_directives_list($table_specifier) };
	
	return $edt->{directives}{$table_specifier};
    }
    
    # If this isn't a recognized table, add 'E_BAD_TABLE' and return undef.
    
    else
    {
	$edt->add_condition('E_BAD_TABLE', $table_specifier);
	return undef;
    }
}


# handle_column ( class_or_instance, table_or_action, column_name, directive )
# 
# Store the specified column directive with this transaction instance. If called
# as a class method, the directive will be supplied by default to every
# EditTransaction in this class. If the specified column does not exist in the
# specified table, the directive will have no effect. 
# 
# If this is called as an instance method, the second argument may be '&_' which
# will set the directive for the current action only. This is only allowed if
# validation has not yet completed. Otherwise, it will be set as a default for
# all subsequent actions on the specified table.
# 
# Return true if the directive was set, false otherwise.

sub handle_column {

    my ($edt, $table_or_action, $colname, $directive) = @_;
    
    # All three arguments must be provided. The directive must be one that is
    # valid for this class.
    
    croak "you must specify a table or action, a column name, and a handling directive"
	unless $table_or_action && $colname && $directive;
    
    croak "invalid directive '$directive'" unless $edt->is_valid_directive($directive);
    
    my $directives_hash;
    
    # If this was called as a class method, apply the directive to the global
    # directive hash for this class. In this case, the parameter $edt will
    # contain the class name.
    
    if ( ! ref $edt )
    {
	croak "action reference is not valid when this method is called on a class"
	    if ref $table_or_action || $table_or_action =~ /^&/;
	
	$directives_hash = $DIRECTIVES_BY_CLASS{$edt}{$table_or_action} ||= { };
    }
    
    # Otherwise, this is an instance method call. If $table_or_action is a
    # string value that is not an action refstring, apply it to the directives
    # for the instance on which this method was called. If directives for this
    # particular table have not yet been initialized, do that now.
    
    elsif ( ! ref $table_or_action && $table_or_action !~ /^&/ )
    {
	return undef if $edt->has_finished;
	
	$directives_hash = $edt->init_directives($table_or_action) ||
	    croak "unknown table '$table_or_action'";
    }
    
    # Otherwise, assume it is an action reference. If it is valid, apply the
    # directive to the corresponding action unless validation has already been
    # completed for that action.
    
    elsif ( my $action = $edt->action_ref($table_or_action) )
    {
	return undef if $edt->has_finished;
	return undef if $action->{validation} eq 'COMPLETE';
	
	$directives_hash = $action->{directives} ||= { };
    }
    
    # Now, if we have identified a directives hash, apply the directive.
    # Otherwise, return undef.
    
    if ( $directives_hash )
    {
	if ( $directive eq 'none' )
	{
	    $directives_hash->{$colname} = '';
	    return $directive;
	}
	
	else
	{
	    $directives_hash->{$colname} = $directive;
	    return $directive;
	}
    }
    
    else
    {
	return undef;
    }
}


# get_handling ( table_or_action, column_name )
# 
# This subroutine can be called either as a class method or an instance method.
# In the former case, return directive(s) associated with the class. If a column
# name is given, return the class-level directive (if any) assigned to that
# column in the specified table. If the second argument is '*', return a list of
# column names and directives for the specified table, suitable for
# assigning to a hash. If no directive(s) have been assigned, return the empty
# list.
# 
# In the latter case, return directives associated with the specific
# EditTransaction instance on which this method was called. If the first
# argument is an action refstring , return a list of directives and columns
# which will be used by the specified action, or else the directive associated
# with a specific column depending on whether the second argument is a column
# name or '*'.

sub get_handling {
    
    my ($edt, $table_or_action, $colname) = @_;
    
    croak "you must provide a table specifier and a column name" 
	unless $table_or_action && $colname;
    
    # If this was called as a class method, return class-level directives only.
    # In this case, the value of $edt will be a class name.
    
    if ( ! ref $edt )
    {
	croak "action reference is not valid when this method is called on a class"
	    if ref $table_or_action || $table_or_action =~ /^&/;
	
	if ( $colname eq '*' )
	{
	    return $edt->class_directives_list($table_or_action);
	}
	
	else
	{
	    return $DIRECTIVES_BY_CLASS{$edt}{$table_or_action}{$colname} ||
		$DIRECTIVES_BY_CLASS{EditTransaction}{$table_or_action}{$colname};
	}
    }
    
    # Otherwise, this is an instance method call. If $table_or_action is a
    # string value that is not an action refstring, return transaction-level
    # directives associated with the specified table. Initialize the directives
    # for this particular table if that has not yet been done.
    
    elsif ( ! ref $table_or_action && $table_or_action !~ /^&/ )
    {
	if ( my $directives_hash = $edt->init_directives($table_or_action) )
	{
	    if ( $colname eq '*' )
	    {
		return $directives_hash->%*;
	    }
	    
	    elsif ( $directives_hash->{$colname} )
	    {
		return $directives_hash->{$colname};
	    }
	    
	    elsif ( $edt->table_info_present($table_or_action, $colname) )
	    {
		return '';
	    }
	    
	    else
	    {
		return undef;
	    }
	}
    }
    
    # Otherwise, assume it is an action reference. If it is valid, return
    # directives that apply to the corresponding action. If no directive was
    # found for a specified column, return '' if the column actually exists and
    # undef otherwise.
    
    elsif ( my $action = $edt->action_ref($table_or_action) )
    {
	my $action_table = $action->table || '';
	
	if ( $colname eq '*' )
	{
	    my %directives = map { ref $_ eq 'HASH' ? $_->%* : () }
		$edt->{directives}{$action_table}, $action->{directives};
	    
	    return %directives;
	}
	
	elsif ( my $directive = $action->{directives}{$colname} || 
				$edt->{directives}{$action_table}{$colname} )
	{
	    return $directive;
	}
	
	elsif ( $action_table && $edt->table_info_present($action_table, $colname) )
	{
	    return '';
	}
	
	else
	{
	    return undef;
	}
    }
    
    # If the action reference is invalid, return the empty list for '*' and
    # undef otherwise.
    
    elsif ( $colname eq '*' )
    {
	return ();
    }
    
    else
    {
	return undef;
    }
}

    # # Otherwise, return directives from this transaction associated with the
    # # specified table.
    
    # else
    # {
    # 	return ref $edt->{directives}{$table_or_action} eq 'HASH' ?
    # 	    $edt->{directives}{$table_or_action}->%* : ();
    # }
    
    # # Initialize the directives for this transaction if necessary, and return a
    # # reference to the directives hash.
    
    # my $edt_directives = $edt->init_directives($table_or_action);
    
    # # If a column name was specified, return the value of its directive if any.
    # # If the column exists but has no directive, return the empty string.
    # # Otherwise return undef.
    
    # if ( $colname ne '*' )
    # {
    # 	return $action_specific || $edt_directives->{$colname} || 
    # 	    ($edt->table_info_present($table_or_action, $colname) ? '' : undef);
    # }
    
    # # Otherwise, return the directive hash contents in list context or the
    # # number of entries in scalar context. If there are also action directives,
    # # append them to the list. That way, when assigned to a hash, the action-level
    # # value for a particular column will overwrite any previous
    # # transaction-level value.
    
    # elsif ( $action_directives )
    # {
    # 	if ( wantarray )
    # 	{
    # 	    return $edt_directives->%*, $action_directives->%*;
    # 	}
	
    # 	else
    # 	{
    # 	    return $edt_directives->%* + $action_directives->%*;
    # 	}
    # }
    
    # else
    # {
    # 	return $edt_directives->%*;
    # }


1;
