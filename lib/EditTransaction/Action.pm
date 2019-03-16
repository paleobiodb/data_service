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

use namespace::clean;


our %OPERATION_TYPE = ( insert => 'record', update => 'record', replace => 'record',
			update_many => 'selector', 
			delete => 'single', delete_cleanup => 'selector',
			delete_many => 'selector', bad => 'record', other => 'record' );


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
    
    my ($action) = { table => $table,
		     operation => $operation,
		     record => $record,
		     label => $label,
		     status => '' };
    
    # If the record has a primary key and a non-empty key attribute, store these in the action
    # record. This will be used to fetch information about the record, such as the authorization
    # fields. If the operation is 'delete' or 'other' then we accept a single key value in lieu of a
    # hashref representing a record.
    
    if ( my $key_column = get_table_property($table, 'PRIMARY_KEY') )
    {
	$action->{keycol} = $key_column;
	
	# The 'delete' and 'other' operations can accept a single key value rather than a record hash.
	
	if ( ($operation eq 'delete' || $operation eq 'other' ) && ref $record ne 'HASH' )
	{
	    $action->{keyval} = $record;
	}
	
	# In all other cases, there will be no key value unless $record points to a hash.
	
	elsif ( ref $record eq 'HASH' )
	{
	    # First check to see if the record contains a value under the key column name.
	    
	    if ( defined $record->{$key_column} && $record->{$key_column} ne '' )
	    {
		$action->{keyval} = $record->{$key_column};
		$action->{keyrec} = $key_column;
	    }

	    # If not, check to see if the table has a 'PRIMARY_ATTR' property and if so whether
	    # the record contains a value under that name.
	    
	    elsif ( my $key_attr = get_table_property($table, 'PRIMARY_ATTR') )
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


sub selector {

    return $_[0]{selector};
}


sub label {
    
    return $_[0]{label};
}


sub status {

    return $_[0]{status};
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


sub has_errors {

    return $_[0]{error_count};
}


sub has_warnings {

    return $_[0]{warning_count};
}


# We have very few mutator methods, because almost all the attributes of an action are immutable.

sub _set_status {

    my ($action, $status) = @_;
    
    $action->{status} = $status;
}


sub _set_auxiliary {
    
    my ($action, $root) = @_;
    
    $action->{root} = $root;
    $action->{label} ||= $root->{label};
    $action->{is_aux} = 1;
}


sub _set_permission {
    
    my ($action, $permission) = @_;
    
    croak "you must specify a non-empty permission" unless defined $permission;
    $action->{permission} = $permission;
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


sub add_error {

    $_[0]{error_count}++;
}


sub add_warning {

    $_[0]{warning_count}++;
}


# Actions also need to keep track of special instructions for various table columns. The following
# instructions are available for any given column, and specify how any value that may be present
# in the action's record should be treated:
#
#  ignore	Skip this column value entirely, and do not use it to construct SQL statements.
#		This can be used to mark hash keys that exist in the action record for some other
#		purpose and do not correspond to table columns.
#  
#  pass		Use this column value, but do not add the automatic checks. Assume that the value
#		has passed all necessary checks.
#  
#  validate	Run the automatic validation checks on this column value. This is the same as no
#		special instruction at all.
#  
# These instructions can be given both for table column names and for alternate field names.

# column_special ( arg, column_name... )
#
# If the first argument is a hashref, then assume that its keys are column names and its values
# are instructions. Otherwise, record the special instruction given by $arg for each of the given
# column names.

sub column_special {
    
    my ($action, $special, @cols) = @_;

    # If the first argument is a hashref, just copy in the contents.

    if ( ref $special eq 'HASH' )
    {
	foreach my $col ( keys %$special )
	{
	    $action->{column_special}{$col} = $special->{$col};
	}
    }
    
    # Otherwise, set the special treatment of the indicated columns to the indicated value.

    else
    {
	croak "the first argument must be either 'pass' or 'ignore' or 'validate'"
	    unless $special && ($special eq 'pass' || $special eq 'ignore' || $special eq 'validate');
	
	foreach my $col ( @cols )
	{
	    $action->{column_special}{$col} = $special if $col;
	}
    }
}


sub ignore_column {
    
    my ($action, $col) = @_;

    $action->{column_special}{$col} = 'ignore';
}


sub pass_column {

    my ($action, $col) = @_;

    $action->{column_special}{$col} = 'pass';
}


# get_special ( column_name )
# 
# Return the special column instruction for this column name, or the default if there are none.

sub get_special {
    
    my ($action, $column_name) = @_;
    
    return $action->{column_special}{$column_name} || 'validate';
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


# Finally, we can coalesce multiple actions into one. This method should not be called except by
# EditTransaction.pm. The argument $label_keys should be a map from all record labels that have been
# processed so far into the corresponding keys.

sub _coalesce {

    my ($action, $label_keys, @additional) = @_;
    
    my $operation = $action->operation;
    
    if ( $operation eq 'delete' )
    {
	return unless @additional;
	
	$action->{all_keys} = [ $action->{keyval} ];
	$action->{all_labels} = [ $action->{label} ];
	$action->{additional} = [ ];
	
	foreach my $a ( @additional )
	{
	    next unless $a && defined $a->{keyval} && $a->{keyval} ne '';
	    
	    if ( $label_keys && $a->{keyval} =~ /^@(.*)/ )
	    {
		my $label = $1;
		
		$a->{keyval} = $label_keys->{$label} if $label_keys->{$label};
	    }
	    
	    push @{$action->{all_keys}}, $a->{keyval};
	    push @{$action->{all_labels}}, $a->{label};
	    push @{$action->{additional}}, $a;
	}

	# Now delete the old key expression. A new one will need to be generated if necessary.
	
	$action->{key_expr} = undef;
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
