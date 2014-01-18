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


# section_set ( )
# 
# Return a hash of the output sections being shown for this request.

sub section_set {

    my ($self) = @_;
    
    return $self->{section_set};
}


# process_record ( record, steps )
# 
# Process the specified record using the specified steps.

sub process_record {
    
    my ($self, $record, $steps) = @_;
    my $ds = $self->{ds};
    
    return $ds->process_record($self, $record, $steps);
}


# result_limit ( )
#
# Return the result limit specified for this request, or undefined if
# it is 'all'.

sub result_limit {
    
    return $_[0]->{result_limit} ne 'all' && $_[0]->{result_limit};
}


# result_offset ( will_handle )
# 
# Return the result offset specified for this request, or zero if none was
# specified.  If the parameter $will_handle is true, then auto-offset is
# suppressed.

sub result_offset {
    
    my ($self, $will_handle) = @_;
    
    $self->{offset_handled} = 1 if $will_handle;
    
    return $self->{result_offset} || 0;
}


# sql_limit_clause ( will_handle )
# 
# Return a string that can be added to an SQL statement in order to limit the
# results in accordance with the parameters specified for this request.  If
# the parameter $will_handle is true, then auto-offset is suppressed.

sub sql_limit_clause {
    
    my ($self, $will_handle) = @_;
    
    $self->{offset_handled} = 1 if $will_handle;
    
    my $limit = $self->{result_limit};
    my $offset = $self->{result_offset} || 0;
    
    if ( $offset > 0 )
    {
	$offset += 0;
	$limit = $limit eq 'all' ? 100000000 : $limit + 0;
	return "LIMIT $offset,$limit";
    }
    
    elsif ( defined $limit and $limit ne 'all' )
    {
	return "LIMIT " . ($limit + 0);
    }
    
    else
    {
	return '';
    }
}


# sql_count_clause ( )
# 
# Return a string that can be added to an SQL statement to generate a result
# count in accordance with the parameters specified for this request.

sub sql_count_clause {
    
    return $_[0]->{display_counts} ? 'SQL_CALC_FOUND_ROWS' : '';
}


# add_warning ( message )
# 
# Add a warning message to this request object, which will be returned as part
# of the output.

sub add_warning {

    my ($self, $message) = @_;
    
    return unless defined $message and $message ne '';
    
    $self->{warnings} = [] unless defined $self->{warnings};
    push @{$self->{warnings}}, $message;
}


# warnings
# 
# Return any warning messages that have been set for this request object.

sub warnings {

    my ($self) = @_;
    
    return unless ref $self->{warnings} eq 'ARRAY';
    return @{$self->{warnings}};
}


# output_format
# 
# Return the output format for this request

sub output_format {
    
    return $_[0]->{format};    
}


# display_header
# 
# Return true if we should display optional header material, false
# otherwise.  The text formats respect this setting, but JSON does not.

sub display_header {
    
    return $_[0]->{display_header};
}


# display_counts
# 
# Return true if the result count should be displayed along with the data,
# false otherwise.

sub display_counts {

    return $_[0]->{display_counts};
}


# result_counts
# 
# Return a hashref containing the following values:
# 
# found		the total number of records found by the main query
# returned	the number of records actually returned
# offset	the number of records skipped before the first returned one
# 
# These counts reflect any use or the 'limit' and 'offset' parameters in the
# request, or whichever corresponding parameter names were configured for this
# data service.
# 
# If no counts are available, empty strings are returned for all values.

sub result_counts {

    my ($self) = @_;
    
    my $r = { found => $self->{result_count} // '',
	      returned => $self->{result_count} // '',
	      offset => $self->{result_offset} // '' };
    
    if ( defined $self->{result_count} && defined $self->{result_limit} 
	 && $self->{result_limit} ne 'all'
	 && $self->{result_count} > $self->{result_limit} )
    {
	$r->{returned} = $self->{result_limit};
    }
    
    return $r;
}


# linebreak_cr
# 
# Return true if the linebreak sequence should be a single carriage return
# instead of the usual carriage return/linefeed combination.

sub linebreak_cr {

    return $_[0]->{linebreak_cr};
}


1;
