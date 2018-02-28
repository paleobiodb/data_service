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

use Carp qw(carp croak);

our %OPERATION_TYPE = ( insert => 'record', update => 'record', replace => 'record', delete => 'single' );


# Create a new action record with the specified information.

sub new {
    
    my ($class, $table, $operation, $record, $label) = @_;
    
    # Start by checking that we have the required attributes.
    
    croak "a non-empty table name is required" unless $table;

    unless ( $operation && $OPERATION_TYPE{$operation} )
    {
	$operation ||= '';
	croak "unknown operation '$operation'";
    }
    
    # Create an action object.
    
    my ($action) = { table => $table, operation => $operation, record => $record, label => $label };
    
    # If the record has a primary key and a non-empty key attribute, store these in the action
    # record. This will be used to fetch information about the record, such as the authorization
    # fields.
    
    my ($key_column, $key_attr);
    
    if ( $key_column = get_table_property($table, 'PRIMARY_KEY') )
    {
	$action->{keycol} = $key_column;
    }
    
    if ( $key_attr = get_table_property($table, 'PRIMARY_ATTR') )
    {
	if ( ref $record eq 'HASH' && defined $record->{$key_attr} && $record->{$key_attr} ne '' )
	{
	    $action->{keyval} = $record->{$key_attr};
	}
    }

    elsif ( $key_attr = $key_column )
    {
	if ( ref $record eq 'HASH' )
	{
	    if ( defined $record->{$key_column} && $record->{$key_column} ne '' )
	    {
		$action->{keyval} = $record->{$key_column};
	    }
	    
	    else
	    {
		$key_attr =~ s/_no$/_id/;

		if ( defined $record->{$key_attr} && $record->{$key_attr} ne '' )
		{
		    $action->{keyval} = $record->{$key_attr};
		}
	    }
	}
    }
    
    # If the operation is 'delete' then we accept a key value in lieu of a hash ref. Otherwise, if
    # we haven't found a key value under the usual attribute then check to see if one is found
    # under the primary key column name.
    
    unless ( $action->{keyval} )
    {
	if ( $operation eq 'delete' && ref $record ne 'HASH' )
	{
	    $action->{keyval} = $record;
	}
	
	elsif ( ref $record eq 'HASH' && defined $record->{$key_column} && $record->{$key_column} ne '' )
	{
	    $action->{keyval} = $record->{$key_column};
	}
    }
    
    return bless $action, $class;
}


# General accessor methods

sub table {

    return $_[0]{table};
}


sub operation {
    
    return $_[0]{operation};
}


sub record {
    
    unless ( ref $_[0]{record} eq 'HASH' )
    {
	return unless $_[0]{operation} eq 'delete' && defined $_[0]{record};
	# croak "no record defined for this action" unless $_[0]->{operation} eq 'delete' && defined $_[0]->{record};
	# croak "record must be a hash ref or scalar" if ref $_[0]->{record};
	return { $_[0]{keycol} => $_[0]{record} };
    }
    
    return $_[0]{record};
}


sub label {
    
    return $_[0]{label};
}


sub root {

    return $_[0]{root};
}


sub is_aux {

    return $_[0]{is_aux};
}


sub record_value {

    unless ( ref $_[0]{record} eq 'HASH' )
    {
	return $_[0]{operation} eq 'delete' && $_[1] eq $_[0]{keycol} && defined $_[0]{record} ? $_[0]{record} : undef;
    }
    
    return defined $_[1] ? $_[0]{record}{$_[1]} : undef;
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


sub column_list {
    
    return $_[0]{columns};
}


sub value_list {

    return $_[0]{values};
}


sub is_multiple {

    return $_[0]{additional} ? 1 : undef;
}


sub count {
    
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


sub has_labels {

    return $_[0]{has_labels};
}


sub has_errors {

    return $_[0]{error_count};
}


sub has_warnings {

    return $_[0]{warning_count};
}


# We have very few mutator methods, because almost all the attributes of an action are immutable.

sub set_auxiliary {
    
    my ($action, $root) = @_;
    
    $action->{root} = $root;
    $action->{label} ||= $root->{label};
    $action->{is_aux} = 1;
}


sub set_permission {

    my ($action, $permission) = @_;
    
    croak "you must specify a non-empty permission" unless defined $permission;
    $action->{permission} = $permission;
}


sub set_column_values {

    my ($action, $cols, $vals) = @_;
    
    croak "columns and values must be specified as array refs" unless
	ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY';
    
    $action->{columns} = $cols;
    $action->{values} = $vals;
}


sub set_keyval {

    my ($action, $keyval) = @_;
    
    croak "cannot call 'set_keyval' on a multiple action" if $action->{all_keys};
    $action->{keyval} = $keyval;
}


sub add_error {

    $_[0]{error_count}++;
}


sub add_warning {

    $_[0]{warning_count}++;
}


# We also have a facility to set and get general action attributes.

sub set_attr {

    my ($action, $attr, $value) = @_;
    
    croak "you must specify an attribute name" unless $attr;
    $action->{attrs}{$attr} = $value;
}


sub get_attr {

    croak "you must specify an attribute name" unless $_[1];
    return $_[0]->{attrs} ? $_[0]->{attrs}{$_[1]} : undef;
}


# The following method can be called from a subclass that overrides the 'validate_action' method
# of EditTransaction.pm. It specifies that the specified column should be ignored during any
# subsequent automatic validation check. Presumably, the override method has already performed
# whatever checks it considers to be appropriate.

sub column_skip_validate {
    
    my ($action, @cols) = @_;

    foreach my $col ( @cols )
    {
	$action->{skip_validate}{$col} = 1 if $col;
    }
}


# Finally, we can coalesce multiple actions into one. This method should not
# be called except by EditTransaction.pm.

sub _coalesce {

    my ($action, @additional) = @_;

    my $operation = $action->operation;
    
    if ( $operation eq 'delete' )
    {
	return unless @additional;
	
	$action->{all_keys} = [ $action->{keyval} ];
	$action->{all_labels} = [ $action->{label} ];
	$action->{has_labels} = undef;
	$action->{additional} = [ ];
	
	foreach my $a ( @additional )
	{
	    next unless $a && defined $a->{keyval} && $a->{keyval} ne '';
	    
	    push @{$action->{all_keys}}, $a->{keyval};
	    push @{$action->{all_labels}}, $a->{label};
	    push @{$action->{additional}}, $a;
	    $action->{has_labels} = 1 if defined $a->{label} && $a->{label} ne '';
	}
    }

    elsif ( $operation eq 'insert' )
    {
	...
    }

    else
    {
	croak "you cannot coalesce a '$operation' operation";
    }
}


1;
