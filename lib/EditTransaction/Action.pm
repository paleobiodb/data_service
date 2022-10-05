# 
# EditAction.pm
# 
# This class encapsulates a single action to be executed on the database, generally a single SQL
# statement or a set of related SQL statements executed sequentially. Under most circumstances,
# each record submitted to a data service operation will generate a single action. Sometimes,
# auxiliary actions will be included to update linking tables and such.
# 
# This class is meant to be used internally by EditTransaction and its subclasses.
# 


package EditTransaction::Action;

use strict;

use EditTransaction;

use Carp qw(carp croak);
use Scalar::Util qw(reftype);

use namespace::clean;


our %OPERATION_TYPE = ( skip => 'record', test => 'record', insupdate => 'record',
			insert => 'record', update => 'record', replace => 'record',
			update_many => 'selector', insupdate => 'record',
			delete => 'keys', delete_cleanup => 'keys',
			delete_many => 'selector', sql => 'selector', other => 'keys' );


# Create a new action object with the specified information.

sub new {
    
    my ($class, $edt, $tablename, $operation, $label, $record) = @_;
    
    # Start by checking that we have the required attributes.
    
    die "unknown operation '$operation'" unless $operation && $OPERATION_TYPE{$operation};
    die "missing action label" unless $label;
    
    # A valid table name is not required for a skipped action, but is required for every other
    # kind of action.
    
    die "missing tablename" unless $tablename || $operation eq 'skip';
    
    # Create a basic object to represent this action.
    
    my $action = { table => $tablename,
		   operation => $operation,
		   record => $record,
		   label => $label,
		   status => '',
		   permission => '',
		   validation => '' };
    
    bless $action, $class;
    
    # # For any operation other than 'skip', 'other', or 'sql', fetch all column directives for the
    # # table we are using. The second call to 'all_directives' is guaranteed to be very cheap once
    # # the first call in scalar context has been made, because the answer is cached in the
    # # transaction instance.
    
    # unless ( $operation =~ /^skip|^other|^sql/ )
    # {
    # 	if ( $edt->all_directives($tablename) )
    # 	{
    # 	    $action->{directives} = { $edt->all_directives($tablename) };
    # 	}
    # }
    
    # Return a reference to the new action object.
    
    return $action;
}


# General accessor methods
# ------------------------

sub table {

    return $_[0]{table};
}


sub operation {
    
    return $_[0]{operation};
}


sub label {

    return $_[0]{label};
}


sub refstring {
    
    return $_[0]{label} ? '&' . $_[0]{label} : undef;
}


sub status {

    return $_[0]{status} // '';
}


sub parent {

    return $_[0]{parent};
}


# Methods for setting the action status and result
# ------------------------------------------------

# These are intended to be called from EditTransaction.pm and associated modules. Be very cautious
# about calling them from any other code.

sub set_status {

    my ($action, $status) = @_;
    
    $action->{status} = $status;
}


sub set_result {

    my ($action, $result) = @_;
    
    $action->{result} = $result;
}


sub set_operation {
    
    my ($action, $operation) = @_;
    
    $action->{operation} = $operation;
}


# Methods for keeping track of the action status.
# -----------------------------------------------

# The methods 'has_errors' and 'has_warnings' return the respective counts, including any
# conditions attached to child actions. The rest return true or false.

sub has_errors {

    return $_[0]{child_errors} ? 0 + $_[0]{child_errors} + $_[0]{error_count} : 0 + $_[0]{error_count};
}


sub has_warnings {

    return $_[0]{child_warnings} ? 0 + $_[0]{child_warnings} + $_[0]{warning_count} : 0 + $_[0]{warning_count};
}


sub can_proceed {

    return $_[0]{status} || $_[0]{error_count} || $_[0]{child_errors} ? 0 : 1;
}


sub has_completed {

    return $_[0]{status} ? 1 : 0;
}


sub has_executed {
    
    return $_[0]{status} eq 'executed' ? 1 : 0;
}


# Methods for attaching actions as children of other actions
# ----------------------------------------------------------

# add_child ( )
#
# Attach the specified action as a child of this one. This is intended to be called from
# EditTransaction.pm and related modules. Use with care if called from any other code.

sub add_child {
    
    my ($parent, $child) = @_;
    
    if ( ref $child && $child->isa('EditTransaction::Action') )
    {
	push $parent->{child}->@*, $child;
	$child->{parent} = $parent;
	$child->{label} = $parent->{label} . '-' . scalar($parent->{child}->@*);
	
	foreach my $c ( $child->conditions )
	{
	    if ( $c->[1] =~ /^[ECF]/ )
	    {
		$parent->{child_errors}++;
	    }
	    
	    elsif ( $c->[1] =~ /^W/ )
	    {
		$parent->{child_warnings}++;
	    }
	}
	
	return $child;
    }
    
    else
    {
	croak "invalid action reference '$child'";
    }
}


# Methods for dealing with record selection.
# ------------------------------------------

# set_keyinfo ( keycol, keyfield, keyval, keyexpr )
# 
# Set the fields that determine which records are operated on. The $keycol argument must be the
# name of the primary key column for the table on which this action will operate. The $keyfield
# argument specifies the key in the input record from which the key values or the selector were
# taken. The $keyvalue argument can be either a single keyval or a listref. The final argument
# gives an SQL expression for selecting the corresponding records. This method and the two
# following are intended to be called from EditTransaction.pm and its related modules, and should
# be used with extreme caution anywhere else.

sub set_keyinfo {

    my ($action, $keycol, $keyfield, $keyval, $keyexpr) = @_;
    
    $action->{keycol} = $keycol;
    $action->{keyfield} = $keyfield;
    $action->{keyval} = $keyval if @_ > 3;
    $action->{keyexpr} = $keyexpr if @_ > 3;
}


sub set_keyexpr {

    my ($action, $keyexpr) = @_;

    $action->{keyexpr} = $keyexpr;
}


sub set_keyval {

    my ($action, $keyval) = @_;
    
    $action->{keyval} = $keyval;
}


# A get method is provided for each of these variables. Note that the key expression defaults to
# '0', which will select nothing if included in an SQL where clause.

sub keycol {
    
    return $_[0]{keycol} // '';
}

sub keyfield {

    return $_[0]{keyfield} // '';
}

sub keyval {

    return $_[0]{keyval};
}

sub keyexpr {

    return $_[0]{keyexpr} // '0';
}


# keyvalues ( )
#
# When called in list context, return a list of the key values that have been set for this
# action. In scalar context, return the count. The variable 'keyval' can hold either a single
# value or a listref.

sub keyvalues {
    
    if ( ref $_[0]{keyval} eq 'ARRAY' )
    {
	return $_[0]{keyval}->@*;
    }

    elsif ( wantarray )
    {
	return defined $_[0]{keyval} ? ($_[0]{keyval}) : ();
    }

    else
    {
	return defined $_[0]{keyval} ? 1 : 0;
    }
}


# keymult ( )
#
# Return true if this action has more than one key value, false otherwise.

sub keymult {

    return (ref $_[0]{keyval} eq 'ARRAY' && $_[0]{keyval}->@* > 1 ? 1 : '');
}


# Methods for handling error, caution and warning conditions.
# -----------------------------------------------------------

# conditions ( )
# 
# This method takes no arguments. Return the list of conditions that are attached to this action,
# as a list of listrefs, or else the return empty list.

sub conditions {

    return ref $_[0]{conditions} eq 'ARRAY' ? $_[0]{conditions}->@* : ();
}


# add_condition ( code, [params...] )
#
# Add the specified condition to this action, unless an identical one has already been
# added. Increment error_count or warning_count as appropriate. If this is a child action,
# increment the parent's child_errors or child_warnings count.

sub add_condition {

    my ($action, $code, @params) = @_;
    
    if ( ! $action->has_condition($code, @params) )
    {
	$action->{conditions} = [ ] unless ref $action->{conditions} eq 'ARRAY';
	push $action->{conditions}->@*, [$action->{label}, $code, @params];
	
	if ( $code =~ /^[CEF]/ )
	{
	    $action->{error_count}++;
	    $action->{parent}{child_errors}++ if $action->{parent};
	}
	
	elsif ( $code =~ /^W/ )
	{
	    $action->{warning_count}++;
	    $action->{parent}{child_warnings}++ if $action->{parent};
	}
	
	else
	{
	    $params[0] = $code;
	    $code = 'E_BAD_CONDITION';
	    $action->{error_count}++;
	    $action->{parent}{child_errors}++ if $action->{parent};
	}
	
	return 1;
    }
}


# has_condition ( code, [arg1, arg2, arg3] )
#
# Return true if this action has a condition with the specified code, false otherwise. If 1-3
# extra arguments are given, a condition only matches if each argument value also matches the
# corresponding condition parameter. This method is intended for interal use only, so it does
# minimal checking and does not throw deliberate exceptions.

sub has_condition {
    
    my ($action, $code, @v) = @_;
    
    # If a valid code was given, scan through the condition list looking for matches.
    
    my $is_regexp = ref $code && reftype $code eq 'REGEXP';
    
    if ( $code && ref $action->{conditions} eq 'ARRAY' )
    {
	# Return true if we find an entry that has the proper code and also matches any extra
	# values that were given.
	    
	foreach my $i ( 0 .. $action->{conditions}->$#* )
	{
	    my $c = $action->{conditions}[$i];
	    
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
    
    # If we haven't found a matching condition, return false.
    
    return '';
}


# pin_errors ( )
# 
# After this method is called, subsequent errors added to this action will be fatal even if this
# transaction allows PROCEED.

sub pin_errors {

    my ($action) = @_;
    
    $action->{pin_errors} = 1;
}


# clear_conditions_from_parent ( )
# 
# This method is called when a child action is aborted. Subtract its error and warning conditions
# from the child counts associated with the parent action.

sub clear_conditions_from_parent {

    my ($action) = @_;
    
    if ( my $parent = $action->{parent} )
    {
	foreach my $c ( $action->conditions )
	{
	    if ( $c->[1] =~ /^[ECF]/ )
	    {
		$parent->{child_errors}--;
	    }
	    
	    elsif ( $c->[1] =~ /^W/ )
	    {
		$parent->{child_warnings}--;
	    }
	}

	# Just in case a bug has occurred, don't let the parent counts go negative.
	
	$parent->{child_errors} = 0 if $parent->{child_errors} < 0;
	$parent->{child_warnings} = 0 if $parent->{child_warnings} < 0;
    }
}


# Methods for examining the action record.
# ----------------------------------------

# record ( )
# 
# Return the record that was specified when the action was created.

sub record {

    return $_[0]{record};
}


# record_value ( fieldname )
# 
# Return the value associated with the specified key in the action record, or undef if the key
# does not exist.

sub record_value {

    my ($action, $fieldname) = @_;
    
    if ( ref $action->{record} && reftype $action->{record} eq 'HASH' && defined $fieldname )
    {
	return $action->{record}{$fieldname};
    }
    
    else
    {
	return undef;
    }
}


# record_value_alt ( fieldname... )
#
# Look up each of the specified fieldnames in action record in turn, and return the first defined
# value found.

sub record_value_alt {
    
    my ($action, @fields) = @_;
    
    if ( ref $action->{record} && reftype $action->{record} eq 'HASH' )
    {
	foreach my $f ( @fields )
	{
	    return $action->{record}{$f} if defined $f && defined $action->{record}{$f};
	}
    }
    
    return undef;
}


# set_old_record ( hashref )
#
# This method is called when the current column values for the record (row) to be operated on are
# fetched before the action is executed. These values must be provided as a hashref, and the
# reference is kept for later use if necessary.

sub set_old_record {

    my ($action, $old_record) = @_;
    
    $action->{old_record} = $old_record;
}


# old_record ( )
#
# Return the old record hashref if present.

sub old_record {
    
    return $_[0]{old_record};
}


# Methods for handling authorization
# ----------------------------------

# permission ( )
# 
# Return a single permission string that has been determined during the authorization process. If
# the value is either empty or 'PENDING', then authorization has not been completed.

sub permission {
    
    return $_[0]{permission} // '';
}


# set_permission ( perm )
#
# Set the action permission to the specified string. Under most circumstances, this should only be
# called from authorization code.

sub set_permission {
    
    my ($action, $permission) = @_;
    
    $action->{permission} = $permission;
}


# authorize_later ( )
#
# Indicate that authorization could not be completed and should be tried again at execution time.

sub authorize_later {

    $_[0]{permission} = 'PENDING';
}


# requires_unlock ( )
#
# If an argument is given, set an unlock requirement or clear it. Otherwise, return true if the
# requirement has been set and false otherwise.

sub requires_unlock {
    
    my ($action, $arg) = @_;

    if ( $arg )
    {
	$action->{require_unlock} = 1;
    }

    elsif ( defined $arg )
    {
	$action->{require_unlock} = 0;
    }
    
    else
    {
	return $action->{require_unlock} || '';
    }
}


# Methods for handling validation
# -------------------------------

# validation_status ( )
#
# Return a string indicating the validation status. If it is empty or 'PENDING', then validation
# has not been completed. A value of 'COMPLETED' indicates that the validation process is done.

sub validation_status {

    return $_[0]{validation} // '';
}


# validate_later ( )
#
# Indicate that validation could not be completed and should be tried again at execution time.

sub validate_later {

    $_[0]{validation} = 'PENDING';
}


# validation_complete ( )
#
# Indicate that the validation process is complete. Under most circumstances, this should only be
# called from validation code. Note that it will prevent 'validate_against_schema' from being
# called thereafter.

sub validation_complete {

    $_[0]{validation} = 'COMPLETE';
}


# The following methods set and retrieve the lists of column names and column values (if any)
# associated with this action. These will be used to construct the SQL statement by which this
# action will be carried out. These are not intended to be called outside of EditTransaction.pm
# and its associated modules.

sub set_column_values {

    my ($action, $cols, $vals) = @_;
    
    croak "first argument must be an array ref" unless ref $cols eq 'ARRAY';
    croak "second argument must be an array ref" unless ref $vals eq 'ARRAY';
    
    $action->{columns} = $cols;
    $action->{values} = $vals;
}


sub column_list {
    
    return $_[0]{columns};
}


sub value_list {

    return $_[0]{values};
}


# # handle_column ( column_name, directive )
# #
# # Apply the specified directive to the specified column, for use in the final validation step. The
# # accepted values are documented with the 'handle_column' method of EditTransaction. If the column
# # name you supply does not appear in the database table associated with this action, the directive
# # will have no effect. This method must be called before validation occurs, or it will have no
# # effect.

# sub handle_column {

#     my ($action, $column_name, $directive) = @_;
    
#     croak "you must specify a column name" unless $column_name;
    
#     # croak "invalid directive" unless $EditTransaction::VALID_DIRECTIVE{$directive};
    
#     $action->{directive}{$column_name} = $directive;
# }


# # directive ( column_name )
# # 
# # Return the directive, if any, for the specified column.

# sub directive {

#     return $_[0]{directive} && $_[1] ? $_[0]{directive}{$_[1]} : undef;
# }


# # all_directives ( )
# #
# # In list context, return a list of column names and directives, suitable for assigning to a
# # hash. In scalar context, return the number of columns that have directives.

# sub all_directives {

#     return $_[0]{directive} ? $_[0]{directive}->%* : ();
# }


# add_precheck ( column_name, check_type, params... )
# 
# Add a pre-execution check to this action. This will be carried out immediately before the action
# is executed, after all validation has taken place.
# 
# Currently recognized checks are:
#
# foreign_key    This check is enabled for columns that are foreign keys, if the key
#                value was unable to be verified at validation time.
#

sub add_precheck {

    my ($action, $column_name, $check_type, @params) = @_;

    croak "invalid precheck '$check_type'" unless $check_type eq 'foreign_key';

    $action->{prechecks} ||= [ ];
    push $action->{prechecks}->@*, [ $column_name, $check_type, @params ];
}


# all_prechecks ( )
#
# Return all prechecks that have been defined for this action.

sub all_prechecks {

    my ($action) = @_;

    return ref $action->{prechecks} eq 'ARRAY' ? $action->{prechecks}->@* : ();
}


# Methods for handling method calls
# ---------------------------------

# Actions whose operation is 'other' are executed by making method calls to the transaction
# object, which will be a subclass of EditTransaction. The following methods set and retrieve the
# method name.

sub set_method {

    my ($action, $method) = @_;

    $action->{method} = $method;
}


sub method {

    return $_[0]{method};
}


# Methods for setting and retrieving arbitrary attributes
# -------------------------------------------------------

# set_attr ( attr, value )
# 
# Attach the specified attribute to this action if it isn't already there. Set the attribute to
# the value provided, even if the value is undefined.

sub set_attr {
    
    my ($action, $attr, $value) = @_;
    
    croak "you must specify an attribute name" unless $attr;
    
    $action->{attrs}{$attr} = $value;
    return $action->{attrs}{$attr};
}


sub get_attr {

    my ($action, $attr) = @_;

    croak "you must specify an attribute name" unless $attr;
    
    return $action->{attrs}{$attr};
}

1;
