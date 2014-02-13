#
# Web::DataService::Cache.pm
# 
# This module is responsible for the definition of output caches.  These are
# used to store the results of operations, so that they need not be computed
# as long as they are still correct.
# 
# Author: Michael McClennen

use strict;

package Web::DataService;

use Carp qw(carp croak);

our (%CACHE_DEF) = (name => 'single',
		    lifetime => 'single',
		    check_entry => 'single',
		    disabled => 'single');

our ($DEFAULT_INSTANCE);


# define_cache ( attrs... )
# 
# Define one or more cache objects for data service responses.

sub define_cache {

    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    my $self = $_[0]->isa('Web::DataService') ? shift : $DEFAULT_INSTANCE;
    
    my ($last_node);
    
    # Now we go through the rest of the arguments.  Hashrefs define new cache
    # objects, while strings add to the documentation of the cache object
    # whose definition they follow.
    
    foreach my $item (@_)
    {
	# A hashref defines a new cache object.
	
	if ( ref $item eq 'HASH' )
	{
	    # Make sure the attributes include 'name'.
	    
	    my $name = $item->{name}; 
	    
	    croak "define_cache: you must include the attribute 'name'" unless $name;
	    
	    # Make sure this cache object was not already defined by a previous call,
	    # and set the attributes as specified.
	    
	    croak "define_cache: '$name' was already defined" if defined $self->{cache}{$name};
	    
	    # Create a new cache object.
	    
	    my $record = bless { name => $name }, 'Web::DataService::Cache';
	    
	    foreach my $k ( keys %$item )
	    {
		croak "define_cache: invalid attribute '$k'" unless $CACHE_DEF{$k};
		
		$record->{$k} = $item->{$k};
	    }
	    
	    # Now install the new cache object.  But don't add it to the list if
	    # the 'disabled' attribute is set.
	    
	    $self->{cache}{$name} = $record;
	    push @{$self->{cache_list}}, $name unless $record->{disabled};
	    $last_node = $record;
	}
	
	# A scalar is taken to be a documentation string.
	
	elsif ( not ref $item )
	{
	    $self->add_node_doc($last_node, $item);
	}
	
	else
	{
	    croak "define_cache: arguments must be hashrefs and strings";
	}
    }
    
    croak "define_cache: the arguments must include a hashref of attributes"
	unless $last_node;
}


1;
