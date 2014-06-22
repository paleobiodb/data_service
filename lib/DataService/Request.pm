#
# Web::DataService::Request
# 
# A base class that implements a data service for the PaleoDB.  This can be
# subclassed to produce any necessary data service.  For examples, see
# TaxonQuery.pm and CollectionQuery.pm. 
# 
# Author: Michael McClennen

use strict;

package Web::DataService::Request;

use Scalar::Util qw(reftype);
use Carp qw(carp croak);


# CLASS METHODS
# -------------

# new ( ds, outer, path )
# 
# Generate a new request object.  The parameter $ds is the data service object
# with which this request will be associated; $outer should be an "outer"
# request object generated by the underlying web application framework
# (i.e. Dancer or Mojolicious) or undef if there is none; $path is the
# remainder of the request URL path, after removing the prefix associated
# with the data service.

sub new {
    
    my ($class, $ds, $outer, $path) = @_;
    
    # If the path has a suffix, start by splitting it off.
    
    my $suffix;
    
    if ( $path =~ qr{ ^ (.+) \. (.+) }xs )
    {
	$path = $1;
	$suffix = $2;
    }
    
    # To get the attribute path, we start with the given path minus the
    # suffix.  If it ends in '_doc', remove that first.  This is necessary
    # because the path "abc/def_doc" indicates a request for doumentation
    # about "abc/def".
    
    my $attr_path = $path;
    $attr_path =~ s/_doc$//;
    
    # We then hop off components as necessary until we get to a node that has
    # attributes.  If we reach the empty string, then the attribute path is
    # '/'.
    
    while ( $attr_path ne '' && ! exists $ds->{path_attrs}{$attr_path} )
    {
	if ( $attr_path =~ qr{ ^ (.*) / (.*) }xs )
	{
	    $attr_path = $1;
	}
	
	else
	{
	    $attr_path = '';
	}
    }
    
    if ( $attr_path eq '' )
    {
	$attr_path = '/';
    }
    
    # We save all of the removed components as $rest_path, in case this turns
    # out to be a request for a particular file.
    
    my $rest_path = $attr_path eq '/' ? '' : substr($path, length($attr_path));
    
    # Then create a new request object.  Its initial class is specified in
    # this method call, but that may change later if it is executed as an
    # operation.
    
    my $request = { ds => $ds,
		    outer => $outer,
		    path => $path,
		    attr_path => $attr_path,
		    rest_path => $rest_path,
		    format => $suffix,
		    attrs => $ds->{path_attrs}{$attr_path}
		   };
    
    bless $request, $class;
    
    return $request;
}    


# INSTANCE METHODS
# ----------------

# execute ( )
# 
# Execute this request.  Depending upon the request path, it may either be a
# request for documentation or a request to execute some operation and return
# a result.

sub execute {
    
    my ($request) = @_;
    
    my $ds = $request->{ds};
    my $path = $request->{path};
    my $format = $request->{format};
    my $attrs = $request->{attrs};
    
    $DB::single = 1;
    
    # First check to see if we should be sending a file.  If so, figure out
    # the path and send it.
    
    if ( $attrs->{send_files} )
    {
	my $file_dir = $attrs->{file_dir};
	my $rest_path = $request->{rest_path};
	$rest_path .= ".$format" if defined $format;
	
	croak "you must specify a directory in which to look for files"
	    unless defined $file_dir && $file_dir ne '';
	
	my $file_name = "$file_dir/$rest_path";
	
	$ds->{foundation_plugin}->send_file($request, $file_name);
    }
    
    # Otherwise, check to see if the requested format is '.pod' or '.html' and
    # the path ends in 'index' or '*_doc'.  These URL patterns are interpreted
    # as requests for documentation.
    
    elsif ( defined $format && 
	    ( $format eq 'html' || $format eq 'pod' ) &&
	    $path =~ qr{ (?: index | [^/]+ _doc ) $ }xs )
    {
	# Check to see if we can document this path; if not, return a 404
	# error.
	
	if ( $request->can_document )
	{
	    return $request->document;
	}
	
	else
	{
	    $ds->error_result($request, "404 The resource you requested was not found.");
	}
    }
    
    # Otherwise, any path that ends in a suffix is interpreted as a request to
    # execute an operation.  The suffix specifies the desired format for the
    # result. 
    
    elsif ( defined $format )
    {
	# If the path ends in a number, replace it by 'single'
	# and add the parameter as 'id'.
	
	#if ( $path =~ qr{ (\d+) $ }xs )
	#{
	#    params->{id} = $1;
	#    $path =~ s{\d+$}{single};
	#}
	
	if ( $request->can_do_operation )
	{
	    return $request->do_operation;
	}
	
	else
	{
	    $request->{format} = $format;
	    $ds->error_result($request, "404 The resource you requested was not found.");
	}
    }
    
    # Any other path not ending in a suffix is interpreted as a request for
    # documentation.  If no documentation is available, we return a 404 error.
    
    else
    {
	if ( $request->can_document )
	{
	    return $request->document;
	}
	
	else
	{
	    $ds->error_result($request, "404 The resource you requested was not found.");
	}
    }
}


# can_do_operation ( path, format )
# 
# Return true if the path can be used for a request, i.e. if it has a class
# and operation defined.  Return false otherwise.

sub can_do_operation {
    
    # If we were called as a method, use the object on which we were called.
    # Otherwise, use the globally defined one.
    
    my ($request) = @_;
    
    # Now check whether we have the necessary attributes.
    
    return defined $request->{attrs}{class} &&
	   defined $request->{attrs}{method};
}


# do_operation ( path, format )
# 
# Execute the operation corresponding to the attributes of the given path, and
# return the resulting data in the specified format.

sub do_operation {
    
    my ($request) = @_;
    
    my $ds = $request->{ds};
    my $path = $request->{path};
    my $format = $request->{format};
    my $attrs = $request->{attrs};
    my $class = $attrs->{class};
    my $method = $attrs->{method};
    my $arg = $attrs->{arg};
    
    # Do all of the processing in a try block, so that if an error occurs we
    # can respond with an appropriate error page.
    
    try {
	
	$DB::single = 1;
	
	# Bless the request into the specified class.
	
	bless $request, $class;
	
	# If an init_operation hook was specified for this path, call it now.
	
	$ds->call_hook($attrs->{init_operation_hook}, $request)
	    if $attrs->{init_operation_hook};
	
	# Then check to see that the specified format is valid for the
	# specified path.
	
	unless ( defined $format && ref $ds->{format}{$format} &&
		 ! $ds->{format}{$format}{disabled} &&
		 $attrs->{allow_format}{$format} )
	{
	    return $ds->error_result($request, "415")
	}
	
	# If we are in 'one request' mode, initialize the class plus all of
	# the classes it requires.  If we are not in this mode, then all of
	# the classes will have been previously initialized.
	
	if ( $Web::DataService::ONE_REQUEST )
	{
	    $ds->initialize_class($class);
	}
	
	# Get the raw parameters for this request.
	
	my $params = $ds->{foundation_plugin}->get_params($request);
	
	# Check to see if there is a ruleset corresponding to this path.  If
	# so, then validate the parameters according to that ruleset.
	
	$request->{rs_name} //= $request->determine_ruleset;
	
	if ( $request->{rs_name} )
	{
	    my $context = { ds => $ds, request => $request };
	    
	    my $result = $ds->{validator}->check_params($request->{rs_name}, $context, $params);
	    
	    if ( $result->errors )
	    {
		return $ds->error_result($request, $result);
	    }
	    
	    elsif ( $result->warnings )
	    {
		$request->add_warning($result->warnings);
	    }
	    
	    $request->{valid} = $result;
	    $request->{params} = $result->values;
	    
	    if ( $ds->debug )
	    {
		print STDERR "Params:\n";
		foreach my $p ( $result->keys )
		{
		    my $value = $result->value($p);
		    $value = join(', ', @$value) if ref $value eq 'ARRAY';
		    print STDERR "$p = $value\n";
		}
	    }
	}
	
	# Otherwise, just pass the raw parameters along with no validation or
	# processing.
	
	else
	{
	    print STDERR "No ruleset could be determined for path '$path'" if $ds->debug;
	    $request->{valid} = undef;
	    $request->{params} = $params;
	}
	
	# If a post_params_hook is defined for this path, call it.
	
	$ds->call_hook($attrs->{post_params_hook}, $request)
	    if $attrs->{post_params_hook};
	
	# Now that the parameters have been processed, we can configure all of
	# the settings that might be specified or affected by parameter values:
	
	# Determine the result limit and offset, if any.
	
	$request->{result_limit} = 
	    defined $attrs->{limit_param} &&
	    defined $request->{params}{$attrs->{limit_param}}
		? $request->{params}{$attrs->{limit_param}}
		    : $attrs->{default_limit} || $ds->{default_limit} || 'all';
	
	$request->{result_offset} = 
	    defined $attrs->{offset_param} &&
	    defined $request->{params}{$attrs->{offset_param}}
		? $request->{params}{$attrs->{offset_param}} : 0;
	
	# Determine whether we should show the optional header information in
	# the result.
	
	$request->{display_header} = $request->{params}{$attrs->{nohead_param}} ? 0 : 1;
	$request->{display_source} = $request->{params}{$attrs->{showsource_param}} ? 1 : 0;
	$request->{display_counts} = $request->{params}{$attrs->{count_param}} ? 1 : 0;
	$request->{linebreak_cr} = 
	    $request->{params}{$attrs->{linebreak_param}} &&
		$request->{params}{$attrs->{linebreak_param}} eq 'cr' ? 1 : 0;
	
	# Select a vocabulary.  If no vocabulary was explicitly specified, use
	# the default for the selected format.  As a backup, use the first
	# vocabulary defined for the data service (which may be a default
	# entry if no vocabularies were explicitly defined).
	
	$request->{vocab} = $request->{params}{$attrs->{vocab_param}} || 
	    $ds->{format}{$format}{default_vocab} || $ds->{vocab_list}[0];
	
	# Configure the output.  This involves constructing a list of
	# specifiers that indicate which fields will be included in the output
	# and how they will be processed.  These fields are defined in the
	# context of "output blocks".
	
	$request->configure_output;
	
	# If a post_configure_hook is defined for this path, call it.
	
	$ds->call_hook($attrs->{post_configure_hook}, $request)
	    if $attrs->{post_configure_hook};
	
	# Prepare to time the query operation.
	
	my (@starttime) = Time::HiRes::gettimeofday();
	
	# Now execute the query operation.  This is the central step of this
	# entire routine; everything before and after is in support of this
	# call.
	
	$request->$method($arg);
	
	# Determine how long the query took.
	
	my (@endtime) = Time::HiRes::gettimeofday();
	$request->{elapsed} = Time::HiRes::tv_interval(\@starttime, \@endtime);
	
	# If a post_operation_hook is defined for this path, call it.
	
	$ds->call_hook($attrs->{post_process_hook}, $request)
	    if $attrs->{post_process_hook};
	
	# If a pre_output_hook is defined for this path, save it in the
	# request object so it can be called at the appropriate time.
	
	$request->{pre_output_hook} = $attrs->{pre_output_hook}
	    if $attrs->{pre_output_hook};
	
	# Set the response headers according to the request parameters.
	
	$request->set_response_headers;
	
	# Then we use the output configuration and the result of the query
	# operation to generate the actual output.  How we do this depends
	# upon how the query operation chooses to return its data.  It must
	# set one of the following fields in the request object, as described:
	# 
	# main_data		A scalar, containing data which is to be 
	#			returned as-is without further processing.
	# 
	# main_record		A hashref, representing a single record to be
	#			returned according to the output format.
	# 
	# main_result		A list of hashrefs, representing multiple
	#			records to be returned according to the output
	# 			format.
	# 
	# main_sth		A DBI statement handle, from which all 
	#			records that can be read should be returned
	#			according to the output format.
	# 
	# It is okay for main_result and main_sth to both be set, in which
	# case the records in the former will be sent first and then the
	# latter will be read.
	
	if ( ref $request->{main_record} )
	{
	    return $ds->generate_single_result($request);
	}
	
	elsif ( ref $request->{main_sth} or ref $request->{main_result} )
	{
	    my $threshold = $attrs->{streaming_threshold} || $ds->{streaming_threshold}
		if $ds->{streaming_available} and not $request->{do_not_stream};
	    
	    return $ds->generate_compound_result($request, $threshold);
	}
	
	elsif ( defined $request->{main_data} )
	{
	    return $request->{main_data};
	}
	
	# If none of these fields are set, then the result set is empty.
	
	else
	{
	    return $ds->generate_empty_result($request);
	}
    }
    
    # If an error occurs, return an appropriate error response to the client.
    
    catch {
	
	return $ds->error_result($request, $_);
    };
};



sub set_response_headers {
    
    my ($request) = @_;
    
    # If this is a public-access data service, we add a universal CORS header.
    # At some point we need to add provision for authenticated access.
    
    my $ds = $request->{ds};
    my $path = $request->{path};
    my $attrs = $request->{attrs};
    
    if ( $attrs->{public_access} )
    {
	$ds->{foundation_plugin}->set_cors_header("*");
    }
    
    # If the parameter 'textresult' was given, set the content type to
    # 'text/plain' which will cause the response to be displayed in a browser
    # tab. 
    
    if ( $request->{params}{textresult} )
    {
	$ds->{foundation_plugin}->set_content_type($request, 'text/plain');
    }
    
    # Otherwise, set the content type based on the format.
    
    else
    {
	my $format = $request->{format};
	my $ct = $ds->{format}{$format}{content_type} || 'text/plain';
	my $disp = $ds->{format}{$format}{disposition};
	
	$ds->{foundation_plugin}->set_content_type($request, $ct);
	
	if ( defined $disp && $disp eq 'attachment' )
	{
	    $ds->{foundation_plugin}->set_header($request, 
						 'Content-Disposition' => 
						 qq{attachment; filename="paleobiodb.$format"});
	}
    }
    
    return;
}


# determine_ruleset ( )
# 
# Determine the ruleset that should apply to this request.  If a ruleset name
# was explicitly specified for the request path, then use that if it is
# defined or throw an exception if not.  Otherwise, try the path with slashes
# turned into commas and the optional ruleset_prefix applied.

sub determine_ruleset {
    
    my ($self) = @_;
    
    my $ds = $self->{ds};
    my $validator = $ds->{validator};
    my $path = $self->{attr_path};
    my $ruleset = $self->{attrs}{ruleset};
    
    # If a ruleset name was explicitly given, then use that or throw an
    # exception if not defined.
    
    if ( defined $ruleset && $ruleset ne '' )
    {
	croak "unknown ruleset '$ruleset' for path $path"
	    unless $validator->ruleset_defined($ruleset);
	
	return $ruleset;
    }
    
    # If the ruleset was explicitly specified as '', do not process the
    # parameters for this path.
    
    elsif ( defined $ruleset )
    {
	return;
    }
    
    # Otherwise, try the path with / replaced by :.  If that is not defined,
    # then return empty.  The parameters for this path will not be processed.
    
    else
    {
	$path =~ s{/}{:}g;
	
	$path = $ds->{ruleset_prefix} . $path
	    if defined $ds->{ruleset_prefix} && $ds->{ruleset_prefix} ne '';
	
	return $path if $validator->ruleset_defined($path);
	return; # empty if not defined.
    }
}


# determine_output_names {
# 
# Determine the output block(s) and/or map(s) that should be used for this
# request.  If any output names were explicitly specified for the request
# path, then use them or throw an error if any are undefined.  Otherwise, try
# the path with slashes turned into colons and either ':default' or
# ':default_map' appended.

sub determine_output_names {

    my ($self) = @_;
    
    my $ds = $self->{ds};
    my $path = $self->{path};
    my @output_list = @{$self->{attrs}{output}} if ref $self->{attrs}{output} eq 'ARRAY';
    
    # If any output names were explicitly given, then check to make sure each
    # one corresponds to a known block or set.  Otherwise, throw an exception.
    
    foreach my $output_name ( @output_list )
    {
	croak "the string '$output_name' does not correspond to a defined output block or map"
	    unless ref $ds->{set}{$output_name} eq 'Web::DataService::Set' ||
		ref $ds->{block}{$output_name} eq 'Web::DataService::Block';
    }
    
    # Return the list.
    
    return @output_list;
}


# configure_output ( )
# 
# Determine the list of selection, processing and output rules for this query,
# based on the list of selected output sections, the operation, and the output
# format.

sub configure_output {
    
    my $self = shift;
    my $ds = $self->{ds};
    
    return $ds->configure_output($self);
}


# configure_block ( block_name )
# 
# Set up a list of processing and output steps for the given section.

sub configure_block {
    
    my ($self, $block_name) = @_;
    my $ds = $self->{ds};
    
    return $ds->configure_block($self, $block_name);
}


# add_output_block ( block_name )
# 
# Add the specified block to the output configuration for the current request.

sub add_output_block {
    
    my ($self, $block_name) = @_;
    my $ds = $self->{ds};
    
    return $ds->add_output_block($self, $block_name);
}


# output_key ( key )
# 
# Return true if the specified output key was selected for this request.

sub output_key {

    return $_[0]->{block_keys}{$_[1]};
}


# output_block ( name )
# 
# Return true if the named block is selected for the current request.

sub block_selected {

    return $_[0]->{block_hash}{$_[1]};
}


# select_list ( subst )
# 
# Return a list of strings derived from the 'select' records passed to
# define_output.  The parameter $subst, if given, should be a hash of
# substitutions to be made on the resulting strings.

sub select_list {
    
    my ($self, $subst) = @_;
    
    my @fields = @{$self->{select_list}} if ref $self->{select_list} eq 'ARRAY';
    
    if ( defined $subst && ref $subst eq 'HASH' )
    {
	foreach my $f (@fields)
	{
	    $f =~ s/\$(\w+)/$subst->{$1}/g;
	}
    }
    
    return @fields;
}


# select_hash ( subst )
# 
# Return the same set of strings as select_list, but in the form of a hash.

sub select_hash {

    my ($self, $subst) = @_;
    
    return map { $_ => 1} $self->select_list($subst);
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


# add_table ( name )
# 
# Add the specified name to the table hash.

sub add_table {

    my ($self, $table_name, $real_name) = @_;
    
    if ( defined $real_name )
    {
	if ( $self->{tables_hash}{"\$$table_name"} )
	{
	    $self->{tables_hash}{$real_name} = 1;
	}
    }
    else
    {
	$self->{tables_hash}{$table_name} = 1;
    }
}


# filter_hash ( )
# 
# Return a hashref derived from 'filter' records passed to define_output.

sub filter_hash {
    
    my ($self) = @_;
    
    return $self->{filter_hash};
}


# clean_param ( name )
# 
# Return the cleaned value of the named parameter, or the empty string if it
# doesn't exist.

sub clean_param {
    
    my ($self, $name) = @_;
    
    return '' unless ref $self->{valid};
    return $self->{valid}->value($name) // '';
}


# clean_param_list ( name )
# 
# Return a list of all the cleaned values of the named parameter, or the empty
# list if it doesn't exist.

sub clean_param_list {
    
    my ($self, $name) = @_;
    
    return unless ref $self->{valid};
    my $value = $self->{valid}->value($name);
    return @$value if ref $value eq 'ARRAY';
    return unless defined $value;
    return $value;
}


# output_field_list ( )
# 
# Return the output field list for this request.  This is the actual list, not
# a copy, so it can be manipulated.

sub output_field_list {
    
    my ($self) = @_;
    return $self->{field_list};
}


# debug ( )
# 
# Return true if we are in debug mode.

sub debug {
    
    my ($self) = @_;
    
    return $Web::DataService::DEBUG || $self->{DEBUG};
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


# display_source
# 
# Return true if the data soruce should be displayed, false otherwise.

sub display_source {
    
    return $_[0]->{display_source};    
}


# display_counts
# 
# Return true if the result count should be displayed along with the data,
# false otherwise.

sub display_counts {

    return $_[0]->{display_counts};
}


# get_data_source
# 
# Return the following pieces of information:
# - The name of the data source
# - The license under which the data is made available

sub get_data_source {
    
    return Web::DataService->get_data_source;
}


# get_request_url
# 
# Return the raw (unparsed) request URL

sub get_request_url {

    return Web::DataService->get_request_url;
}


# get_base_url
# 
# Return the base URL for this request

sub get_base_url {

    my ($self) = @_;
    return $self->{ds}->get_base_url;
}


# get_request_path
# 
# Return the URL path for this request (just the path, no format suffix or
# parameters)

sub get_request_path {
    
    my $ds = $_[0]->{ds};
    
    return $ds->{path_prefix} . $_[0]->{path};
}


# get_base_path 
# 
# Return the base path for this request

sub get_base_path {

    my ($self) = @_;
    
    return $self->{ds}->get_base_path;
}


# params_for_display
# 
# Return a list of (parameter, value) pairs for use in constructing response
# headers.  These are the cleaned parameter values, not the raw ones.

sub params_for_display {
    
    my $self = $_[0];
    my $ds = $self->{ds};
    my $validator = $ds->{validator};
    my $rs_name = $self->{rs_name};
    my $path = $self->{path};
    
    # First get the list of all parameters allowed for this result.  We will
    # then go through them in order to ensure a known order of presentation.
    
    my @param_list = list_ruleset_params($validator, $rs_name, {});
    
    # Now filter this list.  For each parameter that has a value, add its name
    # and value to the display list.
    
    my @display;
    
    foreach my $p ( @param_list )
    {
	# Skip parameters that don't have a value.
	
	next unless defined $self->{params}{$p};
	
	# Skip the 'showsource' parameter itself, plus a few more.
	
	next if $p eq $ds->{path_attrs}{$path}{showsource_param} ||
	    $p eq $ds->{path_attrs}{$path}{textresult_param} ||
		$p eq $ds->{path_attrs}{$path}{linebreak_param} ||
		    $p eq $ds->{path_attrs}{$path}{count_param} ||
			$p eq $ds->{path_attrs}{$path}{nohead_param};
	
	# Others get included along with their value(s).
	
	push @display, $p, $self->{params}{$p};
    }
    
    return @display;
}


sub list_ruleset_params {
    
    my ($validator, $rs_name, $uniq) = @_;
    
    return if $uniq->{$rs_name}; $uniq->{$rs_name} = 1;
    
    my $rs = $validator->{RULESETS}{$rs_name};
    return unless ref $rs eq 'HTTP::Validate::Ruleset';
    
    my @params;
    
    foreach my $rule ( @{$rs->{rules}} )
    {
	if ( $rule->{type} eq 'param' )
	{
	    push @params, $rule->{param};
	}
	
	elsif ( $rule->{type} eq 'include' )
	{
	    foreach my $name ( @{$rule->{ruleset}} )
	    {
		push @params, list_ruleset_params($validator, $name, $uniq);
	    }
	}
    }
    
    return @params;
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



# get_config ( )
# 
# Return a hashref providing access to the configuration directives for this
# data service.

sub get_config {
    
    my ($self) = @_;
    
    return $self->{ds}->get_config;
}


# get_connection ( )
# 
# Get a database handle, assuming that the proper directives are present in
# the config.yml file to allow a connection to be made.

sub get_connection {
    
    my ($self) = @_;
    
    return $self->{dbh} if ref $self->{dbh};
    
    $self->{dbh} = $self->{ds}{backend_plugin}->get_connection($self->{ds});
    return $self->{dbh};
}



# set_cors_header ( arg )
# 
# Set the CORS access control header according to the argument.

sub set_cors_header {

    my ($self, $arg) = @_;
    
    $self->{ds}{foundation_plugin}->set_cors_header($self, $arg);
}


# set_content_type ( type )
# 
# Set the content type according to the argument.

sub set_content_type {
    
    my ($self, $type) = @_;
    
    $self->{ds}{foundation_plugin}->set_content_type($self, $type);
}


1;
