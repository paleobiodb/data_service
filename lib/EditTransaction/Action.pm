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

use TableDefs qw(get_table_property);

use EditTransaction;
use Carp qw(carp croak);
use Scalar::Util qw(reftype);

use namespace::clean;


our %OPERATION_TYPE = ( skip => 'record',
			insert => 'record', update => 'record', replace => 'record',
			update_many => 'selector', 
			delete => 'keys', delete_cleanup => 'keys',
			delete_many => 'selector', other => 'keys' );


# Create a new action object with the specified information.

sub new {
    
    my ($class, $table, $operation, $record, $label) = @_;
    
    # Start by checking that we have the required attributes.
    
    unless ( $operation && $OPERATION_TYPE{$operation} )
    {
	$operation ||= '';
	croak "unknown operation '$operation'";
    }
    
    # Create a basic object to represent this action.
    
    my $action = { table => $table,
		   operation => $operation,
		   record => $record,
		   label => $label,
		   status => '' };

    bless $action, $class;
    
    # If the operation is 'skip', return the new object immediately.
    
    if ( $operation eq 'skip' )
    {
	return $action;
    }
    
    # A valid table name is not required for a skipped action, but is required for every other kind of action. It is # important to note that for the 'other' operation, the table name is not required to be the # one actually used to construct database statements.
    
    # If the we are given a key value or a list of key values, store these in the action
    # record. This will be used to fetch information about the record, such as the authorization
    # fields. This step is only taken if the specified table has a non-empty PRIMARY_KEY
    # attribute.

    my ($key_column, $key_value);
    
    if ( $key_column = get_table_property($table, 'PRIMARY_KEY') )
    {
	$action->{keycol} = $key_column;
	
	# If the $record parameter is a hash ref, look for key values there.
	
	if ( ref $record && reftype $record eq 'HASH' )
	{
	    # First check to see if the record contains a value under the key column name.
	    
	    if ( defined $record->{$key_column} && $record->{$key_column} ne '' )
	    {
		$action->{keyval} = $record->{$key_column};
		$action->{keyrec} = $key_column;
	    }

	    # If not, check to see if the table has a 'PRIMARY_FIELD' property and if so whether
	    # the record contains a value under that name.
	    
	    elsif ( my $key_attr = get_table_property($table, 'PRIMARY_FIELD') )
	    {
		if ( defined $record->{$key_attr} && $record->{$key_attr} ne '' )
		{
		    $action->{keyval} = $record->{$key_attr};
		    $action->{keyrec} = $key_attr;
		}
	    }

	    # As a fallback, if the key column name ends in _no, change that to _id and check to
	    # see if the record contains a value under that name.
	    
	    elsif ( $key_column =~ s/_no$/_id/ )
	    {
		if ( defined $record->{$key_column} && $record->{$key_column} ne '' )
		{
		    $action->{keyval} = $record->{$key_column};
		    $action->{keyrec} = $key_column;
		}
	    }
	}
    }
    
    return $action;
}


sub _simple {

    my ($class, $table, $operation, $record, $label) = @_;

}


# General accessor methods

sub table {

    return $_[0]->{table};
}


sub operation {
    
    return $_[0]->{operation};
}


sub record {
    
    unless ( ref $_[0]->{record} eq 'HASH' )
    {
	if ( $_[0]->{operation} eq 'delete' && defined $_[0]->{record} )
	{
	    return { $_[0]->{keycol} => $_[0]->{record} };
	}

	else
	{
	    return;
	}
    }
    
    return $_[0]->{record};
}


sub selector {

    return $_[0]->{selector};
}


sub label {
    
    return $_[0]->{label};
}


sub status {

    return $_[0]{status};
}


sub can_proceed {

    return ! $_[0]{status} && ! ($_[0]{errors} && $_[0]{errors}->@*);
}


sub has_completed {

    return $_[0]{status};
}


sub has_succeeded {
    
    return $_[0]{status} eq 'executed' && ! ($_[0]{errors} && $_[0]{errors}->@*);
}


sub errors {

    return unless $_[0]->{errors};
    return $_[0]->{errors}->@*;
}


sub warnings {

    return unless $_[0]->{warnings};
    return $_[0]->{warnings}->@*;
}


sub parent {

    return $_[0]->{parent};
}


sub is_child {

    return $_[0]->{is_child};
}


sub record_value {
    
    if ( ref $_[0]{record} eq 'HASH' )
    {
	return defined $_[1] ? $_[0]{record}{$_[1]} : undef;
    }

    elsif ( $_[0]{operation} eq 'delete' )
    {
	return $_[1] eq $_[0]{keycol} && defined $_[0]{record} ? $_[0]{record} : undef;
    }

    return;
}


sub record_value_alt {
    
    my ($action, @fields) = @_;
    
    foreach my $f ( @fields )
    {
	return $action->{record}{$f} if defined $action->{record}{$f};
    }

    return;
}


sub old_record {
    
    return $_[0]{old_record};
}


sub has_field {

    return exists $_[0]{record}{$_[1]};
}


sub permission {
    
    return $_[0]{permission};
}


sub keycol {
    
    return $_[0]{keycol};
}


sub keyval {
    
    return $_[0]{keyval};
}


sub keyrec {

    return $_[0]{keyrec} // '';
}


sub keylist {

    my ($action) = @_;
    
    if ( $action->is_multiple )
    {
	return $action->all_keys;
    }
    
    elsif ( defined $action->{keyval} && $action->{keyval} ne '' )
    {
	return $action->{keyval};
    }
    
    else
    {
	return;
    }
}


sub keystring {

    my ($action) = @_;

    if ( $action->is_multiple )
    {
	return "'" . join("','", $action->all_keys) . "'";
    }

    elsif ( defined $action->{keyval} && $action->{keyval} ne '' )
    {
	return "'" . $action->{keyval} . "'";
    }

    else
    {
	return '';
    }
}


sub column_list {
    
    return $_[0]{columns};
}


sub keyexpr {

    return $_[0]{key_expr};
}


sub method {

    return $_[0]{method};
}


sub linkcol {

    return $_[0]{linkcol};
}


sub linkval {

    return $_[0]{linkval};
}


sub value_list {

    return $_[0]{values};
}


sub label_sub {

    return $_[0]{label_sub};
}


sub is_multiple {

    return $_[0]{additional} ? 1 : undef;
}


sub is_single {

    return ! $_[0]{additional};
}


sub action_count {
    
    return $_[0]{additional} ? scalar(@{$_[0]{additional}}) + 1 : 1;
}


sub all_keys {

    return unless $_[0]{all_keys};
    return @{$_[0]{all_keys}};
}


sub all_labels {

    return unless $_[0]{all_labels};
    return @{$_[0]{all_labels}};
}


# Public mutator methods.

# attr ( attr, [value] )
# 
# If a value is provided, attach the specified attribute to this action if it isn't already
# there. Set the attribute to the value provided, including the undefined value. If only the first
# argument is given, return the current value of the attribute if it exists, undefined otherwise.

sub attr {
    
    my ($action, $attr, $value) = @_;
    
    croak "you must specify an attribute name" unless $attr;
    
    if ( @_ == 3 )
    {
	$action->{attrs}{$attr} = $value;
    }
    
    return $action->{attrs}{$attr};
}


# errors_are_fatal ( [value] )
# 
# If a true value is specified, subsequent errors added to this action will be fatal even if this
# transaction allows PROCEED. If a false value is specified, subsequent errors will again be
# eligible for demotion to warnings. If no value is provided, the current value is returned.

sub errors_are_fatal {

    my ($action, $arg) = @_;
    
    if ( defined $arg )
    {
	$action->{errors_are_fatal} = ($arg ? 1 : undef);
    }

    return $action->{errors_are_fatal};
}


# The following methods are non-public. They should only be called from EditTransaction.pm and Validate.pm

sub _set_status {

    my ($action, $status) = @_;
    
    $action->{status} = $status;
}


sub _add_child {
    
    my ($action, $aux_action) = @_;
    
    push $action->{child}->@*, $aux_action;
    $aux_action->{parent} = $action;
    $aux_action->{label} ||= $action->{label};
    $aux_action->{is_child} = 1;

    return $aux_action;
}


sub _set_permission {
    
    my ($action, $permission) = @_;
    
    croak "you must specify a non-empty permission" unless defined $permission;
    $action->{permission} = $permission;
}


sub _authorize_later {

    my ($action, $linkref, $move) = @_;

    $action->{permission} = 'later';
    $action->{linkref} = $linkref;
    $action->{c_move} = 1 if $move;
}


sub _set_keyexpr {

    my ($action, $key_expr) = @_;

    $action->{key_expr} = $key_expr;
}


sub _set_keyval {

    my ($action, $keyval) = @_;
    
    croak "cannot call 'set_keyval' on a multiple action" if $action->{all_keys};
    $action->{keyval} = $keyval;
}


sub _set_linkcol {

    my ($action, $linkcol) = @_;

    $action->{linkcol} = $linkcol;
}


sub _set_linkval {

    my ($action, $linkval) = @_;

    $action->{linkval} = $linkval;
}


sub _set_method {

    my ($action, $method) = @_;

    $action->{method} = $method;
}


sub _set_selector {

    my ($action, $selector) = @_;
    
    $action->{selector} = $selector;
}


sub _set_old_record {

    my ($action, $old_record) = @_;

    $action->{old_record} = $old_record;
}


sub set_column_values {

    my ($action, $cols, $vals) = @_;
    
    croak "columns and values must be specified as array refs" unless
	ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY';
    
    $action->{columns} = $cols;
    $action->{values} = $vals;
}


sub substitute_label {

    my ($action, $col) = @_;
    
    $action->{label_sub}{$col} = 1;
}


sub _add_error {

    my ($action, $condition) = @_;
    
    $action->{error} = [ ] unless ref $action->{errors} eq 'ARRAY';
    push $action->{errors}->@*, $condition;
}


sub _add_warning {

    my ($action, $condition) = @_;

    $action->{warnings} = [ ] unless ref $action->{errors} eq 'ARRAY';
    push $action->{warnings}->@*, $condition;
}


# sub ignore_column {

#     my ($action, $column_name) = @_;

#     $action->{colspec}{$column_name} = 'ignore' if $column_name;
# }


sub column_special {

    return $_[0]->{colspec} && $_[0]->{colspec}{$_[1]};
}


# Finally, we can coalesce multiple actions into one. This method should not be called except by
# EditTransaction.pm. The argument $label_keys should be a map from all record labels that have been
# processed so far into the corresponding keys.

sub _coalesce {
    
    my ($action, $additional) = @_;

    $action->{additional} = $additional;
}
    
    # my ($action, $label_keys, @additional) = @_;
    
#     my $operation = $action->operation;
    
#     if ( $operation eq 'delete' )
#     {
# 	return unless @additional;
	
# 	$action->{all_keys} = [ $action->{keyval} ];
# 	$action->{all_labels} = [ $action->{label} ];
# 	$action->{additional} = [ ];
	
# 	foreach my $a ( @additional )
# 	{
# 	    next unless $a && defined $a->{keyval} && $a->{keyval} ne '';
	    
# 	    if ( $label_keys && $a->{keyval} =~ /^@(.*)/ )
# 	    {
# 		my $label = $1;
		
# 		$a->{keyval} = $label_keys->{$label} if $label_keys->{$label};
# 	    }
	    
# 	    push @{$action->{all_keys}}, $a->{keyval};
# 	    push @{$action->{all_labels}}, $a->{label};
# 	    push @{$action->{additional}}, $a;
# 	}

# 	# Now delete the old key expression. A new one will need to be generated if necessary.
	
# 	$action->{key_expr} = undef;
#     }

#     elsif ( $operation eq 'insert' )
#     {
# 	...
#     }

#     else
#     {
# 	croak "you cannot coalesce a '$operation' operation";
#     }
# }


1;
