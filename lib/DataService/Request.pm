#
# DataService::Query
# 
# A base class that implements a data service for the PaleoDB.  This can be
# subclassed to produce any necessary data service.  For examples, see
# TaxonQuery.pm and CollectionQuery.pm. 
# 
# Author: Michael McClennen

use strict;

package Web::DataService::Request;

use Scalar::Util qw(reftype);


# new ( dbh, attrs )
# 
# Generate a new query object, using the given database handle and any other
# attributes that are specified.

sub new {
    
    my ($class, $attrs) = @_;
    
    # Now create a query record.
    
    my $self = { };
    
    if ( ref $attrs eq 'HASH' )
    {
	foreach my $key ( %$attrs )
	{
	    $self->{$key} = $attrs->{$key};
	}
    }
    
    # Bless it into the proper class and return it.
    
    bless $self, $class;
    return $self;
}


# define_output ( ds, section, specification... )
# 
# Define an output section for this class, for the specified data service.  If
# none is specified, use the default instance.  This must be called as a class method!

sub define_output {
    
    my $ds;
    
    # If the first argument is not a data service.
    
    unless ( ref $_[0] and $_[0]->isa('Web::Dataservice') )
    {
	unshift @_, $Web::DataService::DEFAULT_INSTANCE;
    }
    
    goto &Web::DataService::define_output_section;
}


# configure_output ( )
# 
# Determine the list of selection, processing and output rules for this query,
# based on the list of selected output sections, the operation, and the output
# format.

sub configure_output {
    
    my $self = shift;
    my $ds = $self->{ds};
    
    return $ds->configure_query_output($self);
}


# configure_section ( section_name )
# 
# Set up a list of processing and output steps for the given section.

sub configure_section {
    
    my ($self, $section_name) = @_;
    my $ds = $self->{ds};
    
    return $ds->configure_section($self, $section_name);
}


# process_record ( record, steps )
# 
# Process the specified record using the specified steps.

sub process_record {
    
    my ($self, $record, $steps) = @_;
    my $ds = $self->{ds};
    
    return $ds->process_record($self, $record, $steps);
}


1;
