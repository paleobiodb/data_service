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


# CLASS METHODS
# -------------

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


# define_output_map ( specification... )
# 
# Define a map which associates output blocks with values of the 'show'
# parameter.  This can be used to automatically generate a validator function
# and parameter documentation.

sub define_output_map {
    
    goto &Web::DataService::define_output_map;
}


# define_block ( block_name, specification... )
# 
# Define an output block, using the default data service instance.  This must be
# called as a class method!

sub define_block {
    
    my $class = shift;
    
    goto &Web::DataService::define_block;
}


# output_map_validator ( )
# 
# Return a reference to a validator routine (a closure, actually) which will
# accept the list of output sections defined in the output map for this class.

sub output_map_validator {

    goto &Web::DataService::output_map_validator;
}


# document_output_map ( )
# 
# Return a documentation string in POD format, documenting the blocks that are
# included in the output map for this class.

sub document_output_map {

    goto &Web::DataService::document_output_map;
}


# INSTANCE METHODS
# ----------------

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


# configure_block ( section_name )
# 
# Set up a list of processing and output steps for the given section.

sub configure_block {
    
    my ($self, $section_name) = @_;
    my $ds = $self->{ds};
    
    return $ds->configure_section($self, $section_name);
}


# base_block_list ( )
# 
# Return the base list of output blocks for this request (this is an attribute
# of the request path).

sub base_block_list {
    
    my ($self) = @_;
    
    my $ds = $self->{ds};
    my $path = $self->{path};
    
    my $base_output = $self->{path_attrs}{$path}{base_output};
    
    return @$base_output if ref $base_output eq 'ARRAY';
    return;
}


# extra_block_list ( )
# 
# Return the list of output blocks selected by the 'show' parameter (or the
# corresponding parameter name specified for this data service).

sub extra_block_list {
    
    my ($self) = @_;
    
    my $ds = $self->{ds};
    my $path = $self->{path};
    
    my $param = $self->{path_attrs}{$path}{output_param};
    
    my $extra_output = $self->{params}{$param};
    
    return @$extra_output if ref $extra_output eq 'ARRAY';
    return $extra_output if defined $extra_output;
    return;
}


# section_set ( )
# 
# Return a hash of the output sections being shown for this request.

sub section_set {

    my ($self) = @_;
    
    return $self->{section_set};
}


# select_list ( subst )
# 
# Return a list of strings derived from the 'select' records passed to
# define_output.  The parameter $subst, if given, should be a hash of
# substitutions to be made on the resulting strings.

sub select_list {
    
    my ($self, $subst) = @_;
    
    my @fields = @{$self->{select_list}};
    
    return unless @fields && defined $subst && ref $subst eq 'HASH';
    
    foreach my $f (@fields)
    {
	$f =~ s/\$(\w+)/$subst->{$1}/g;
    }
    
    return @fields;
}


# select_string ( subst )
# 
# Return the select list (see above) joined into a comma-separated string.

sub select_string {
    
    my ($self, $subst) = @_;
    
    return join(', ', $self->select_list($subst));    
}


# tables_hash ( )
# 
# Return a hashref whose keys are the values of the 'tables' attributes in
# 'select' records passed to define_output.

sub tables_hash {
    
    my ($self) = @_;
    
    return $self->{tables_hash};
}


# filter_hash ( )
# 
# Return a hashref derived from 'filter' records passed to define_output.

sub filter_hash {
    
    my ($self) = @_;
    
    return $self->{filter_hash};
}


# debug ( )
# 
# Return true if we are in debug mode.

sub debug {
    
    my ($self) = @_;
    
    return $self->{ds}{DEBUG};
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
    
    $self->{offset_handled} = $will_handle ? 1 : 0;
    
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


# sql_count_rows ( )
# 
# If we were asked to get the result count, execute an SQL statement that will
# do so.

sub sql_count_rows {
    
    my ($self) = @_;
    
    if ( $self->{display_counts} )
    {
	($self->{result_count}) = $self->{dbh}->selectrow_array("SELECT FOUND_ROWS()");
    }
    
    return $self->{result_count};
}


# set_result_count ( count )
# 
# This method should be called if the backend database does not implement the
# SQL FOUND_ROWS() function.  The database should be queried as to the result
# count, and the resulting number passed as a parameter to this method.

sub set_result_count {
    
    my ($self, $count) = @_;
    
    $self->{result_count} = $count;
}


# add_warning ( message )
# 
# Add a warning message to this request object, which will be returned as part
# of the output.

sub add_warning {

    my $self = shift;
    
    foreach my $m (@_)
    {
	push @{$self->{warnings}}, $m if defined $m && $m ne '';
    }
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
# These counts reflect the values given for the 'limit' and 'offset' parameters in
# the request, or whichever substitute parameter names were configured for
# this data service.
# 
# If no counts are available, empty strings are returned for all values.

sub result_counts {

    my ($self) = @_;
    
    # Start with a default hashref with empty fields.  This is what will be returned
    # if no information is available.
    
    my $r = { found => $self->{result_count} // '',
	      returned => $self->{result_count} // '',
	      offset => $self->{result_offset} // '' };
    
    # If no result count was given, just return the default hashref.
    
    return $r unless defined $self->{result_count};
    
    # Otherwise, figure out the start and end of the output window.
    
    my $window_start = defined $self->{result_offset} && $self->{result_offset} > 0 ?
	$self->{result_offset} : 0;
    
    my $window_end = $self->{result_count};
    
    # If the offset and limit together don't stretch to the end of the result
    # set, adjust the window end.
    
    if ( defined $self->{result_limit} && $self->{result_limit} ne 'all' &&
	 $window_start + $self->{result_limit} < $window_end )
    {
	$window_end = $window_start + $self->{result_limit};
    }
    
    # The number of records actually returned is the length of the output
    # window. 
    
    $r->{returned} = $window_end - $window_start;
    
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
