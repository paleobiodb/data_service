#
# Web::DataService::Set
# 
# This module provides a role that is used by 'Web::DataService'.  It implements
# routines for defining and documenting output formats.
# 
# Author: Michael McClennen

use strict;

package Web::DataService::Set;

use Carp 'croak';
use Scalar::Util 'reftype';

use Moo::Role;


our (%SET_DEF) = (value => 'single',
		  insert => 'single',
		  maps_to => 'single',
		  disabled => 'single',
		  undocumented => 'single',
		  doc_string => 'single');

our ($METHODNAME) = 'define_valueset';


# define_valueset ( name, specification... )
# 
# Define a set of values, with optional value map and documentation.  Such
# sets can be used to define and document acceptable parameter values,
# document data values, and many other uses.
# 
# The names of sets must be unique within a single data service.

sub define_set {

    my ($ds, $name, @items) = @_;
    
    # Make sure the name is unique.
    
    croak "$METHODNAME: the first argument must be a valid name"
	unless $ds->valid_name($name);
    
    croak "$METHODNAME: '$name' was already defined at $ds->{set}{$name}{defined_at}"
	if ref $ds->{set}{$name};
    
    # Create a new set object.
    
    my ($package, $filename, $line) = caller;
    
    my $vs = { name => $name,
	       defined_at => "line $line of $filename",
	       value => {},
	       value_list => [] };
    
    bless $vs, 'Web::DataService::Set';
    
    $ds->{set}{$name} = $vs;
    
    # Then add the specified value records and documentation strings to this set.
    
    $ds->add_to_set($name, @items);
}


sub add_to_set {
    
    my ($ds, $name, @items) = @_;
    
    # Make sure the name is defined.
    
    my $vs = $ds->{set}{$name} || croak "add_to_set: unknown set '$name'";
    
    # Then process the records and documentation strings one by one.  Throw an
    # exception if we find an invalid record.
    
    my $doc_node;
    my @doc_lines;
    
    foreach my $item (@items)
    {
	# A scalar is interpreted as a documentation string.
	
	unless ( ref $item )
	{
	    $ds->add_doc($vs, $item) if defined $item;
	    next;
	}
	
	# Any item that is not a record or a scalar is an error.
	
	unless ( ref $item && reftype $item eq 'HASH' )
	{
	    croak "$METHODNAME: arguments must be records (hash refs) and documentation strings";
	}

	# If this item includes the key 'insert', and a valueset exists whose name matches the key
	# value, copy all of its records into the current valueset. If this item also contains
	# 'disabled' and/or 'undocumented', the values of these keys override these attributes in
	# the copied records.
	
	if ( $item->{insert} )
	{
	    croak "$METHODNAME: 'value' is not allowed with 'insert'" if defined $item->{value};
	    croak "$METHODNAME: 'maps_to' is not allowed with 'insert'" if defined $item->{maps_to};
	    
	    my $insert_set = $ds->{set}{$item->{insert}};
	    
	    croak "$METHODNAME: valueset '$item->{insert}' is not defined"
		unless ref $insert_set eq 'Web::DataService::Set';
	    
	    foreach my $value ( $insert_set->{value_list}->@* )
	    {
		croak "$METHODNAME: value '$value' cannot be defined twice in the same valueset"
		    if exists $vs->{value}{$value};
		
		my %newitem = $insert_set->{value}{$value}->%*;

		$newitem{disabled} = $item->{disabled} if defined $item->{disabled};
		$newitem{undocumented} = $item->{undocumented} if defined $item->{undocumented};
		
		$ds->add_doc($vs, \%newitem);
		
		push @{$vs->{value_list}}, $value unless $newitem{disabled};
		$vs->{value}{$value} = \%newitem;
	    }
	}
	
	# Otherwise, this item represents a single value.

	else
	{
	    # Check for invalid attributes.
	    
	    foreach my $k ( keys %$item )
	    {
		croak "$METHODNAME: unknown attribute '$k'"
		    unless defined $SET_DEF{$k};
	    }
	
	    # Check that each reord contains an actual value, and that these
	    # values do not repeat.
	    
	    my $value = $item->{value};
	    
	    croak "$METHODNAME: you must specify a nonempty 'value' key in each record"
		unless defined $value && $value ne '';
	    
	    croak "$METHODNAME: value '$value' cannot be defined twice in the same valueset"
		if exists $vs->{value}{$value};
	    
	    # Add the value to the various lists it belongs to, and to the hash
	    # containing all defined values.

	    $ds->add_doc($vs, $item);
	    
	    push @{$vs->{value_list}}, $value unless $item->{disabled};
	    $vs->{value}{$value} = $item;
	}
    }
    
    # Finish the documentation for this object.
    
    $ds->process_doc($vs);
    
    my $a = 1;	# we can stop here when debugging
}


# define_set ( ... )
#
# This is an alias for define_valueset.

sub define_valueset {

    local($METHODNAME) = 'define_valueset';
    &define_set;
}


# define_map ( ... )
#
# This is an alias for define_valueset.

sub define_map {

    local($METHODNAME) = 'define_map';
    &define_set;
}


# set_defined ( name )
# 
# Return true if the given argument is the name of a set that has been defined
# for the current data service, false otherweise.

sub set_defined {
    
    my ($ds, $name) = @_;
    
    return ref $ds->{set}{$name} eq 'Web::DataService::Set';
}


# valid_set ( name )
# 
# Return a reference to a validator routine (actualy a closure) which will
# accept the list of values defined for the specified set.  If the given name
# does not correspond to any set, the returned routine will reject any value
# it is given.

sub valid_set {

    my ($ds, $set_name) = @_;
    
    my $vs = $ds->{set}{$set_name};
    
    unless ( ref $vs eq 'Web::DataService::Set' )
    {
	unless ( $Web::DataService::QUIET || $ENV{WDS_QUIET} )
	{
	    warn "WARNING: unknown set '$set_name'";
	}
	return \&bad_set_validator;
    }
    
    # If there is at least one enabled value for this set, return the
    # appropriate closure.
    
    if ( ref $vs->{value_list} eq 'ARRAY' && @{$vs->{value_list}} )
    {
	return HTTP::Validate::ENUM_VALUE( @{$vs->{value_list}} );
    }
    
    # Otherwise, return a reference to a routine which will always return an
    # error.
    
    return \&bad_set_validator;
}


sub bad_set_validator {

    return { error => "No valid values have been defined for {param}." };
}


# document_set ( set_name )
# 
# Return a string in Pod format documenting the values that were assigned to
# this set.

sub document_set {

    my ($ds, $set_name) = @_;
    
    # Look up a set object using the given name.  If none could be found,
    # return an explanatory message.
    
    my $vs = $ds->{set}{$set_name};
    
    return "=over\n\n=item I<Could not find the specified set>\n\n=back"
	unless ref $vs eq 'Web::DataService::Set';
    
    my @values; @values = grep { ! $vs->{value}{$_}{undocumented} } @{$vs->{value_list}}
	if ref $vs->{value_list} eq 'ARRAY';
    
    return "=over\n\n=item I<The specified set is empty>\n\n=back"
	unless @values;
    
    # Now return the documentation in Pod format.
    
    my $doc = "=over\n\n";
    
    foreach my $name ( @values )
    {
	my $rec = $vs->{value}{$name};
	
	$doc .= "=item $rec->{value}\n\n";
	$doc .= "$rec->{doc_string}\n\n" if defined $rec->{doc_string} && $rec->{doc_string} ne '';
    }
    
    $doc .= "=back";
    
    return $doc;
}


# list_set_values ( set_name )
# 
# Return a list of the documented values defined for the specified set.

sub list_set_values {
    
    my ($ds, $name) = @_;
    
    return unless defined $name;
    
    my $set = $ds->{set}{$name};
    
    return unless ref $set eq 'Web::DataService::Set';
    return grep { ! $set->{value}{$_}{undocumented} } @{$set->{value_list}};
}


# set_values ( set_name )
# 
# Return a list of records representing the values defined for the specified
# set.

sub set_values {
    
    my ($ds, $name) = @_;
    
    my $set = $ds->{set}{$name};
    
    croak "set_values: set '$name' not found\n"
	unless ref $set eq 'Web::DataService::Set';
    
    my @list;
    
    foreach my $v ( @{$set->{value_list}} )
    {
	next if $set->{value}{$v}{undocumented};
	
	my $sr = $set->{value}{$v};
	my $r = { value => $sr->{value} };
	$r->{maps_to} = $sr->{maps_to} if defined $sr->{maps_to};
	$r->{doc_string} = $sr->{doc_string} if defined $sr->{doc_string};
	
	push @list, $r;
    }
    
    return @list;
}


# map_value ( set_name, value )
# 
# If the given value is a member of the named set, then return the 'maps_to'
# value if any was defined.  Return undef otherwise.

sub map_value {
    
    no warnings 'uninitialized';
    
    my ($ds, $name, $value) = @_;
    
    my $set = $ds->{set}{$name};
    
    croak "set_values: set '$name' not found\n"
	unless ref $set eq 'Web::DataService::Set';
    
    return $set->{value}{$value}{maps_to};
}

1;
