# 
# Web::DataService::IRequest
# 
# This is a role whose sole purpose is to be composed into the classes defined
# for the various data service operations.  It defines the public interface 
# to a request object.


package Web::DataService::IRequest;

use Carp 'croak';
use Scalar::Util 'reftype';
use JSON 'decode_json';
use Try::Tiny;

use Moo::Role;


# has_block ( block_key_or_name )
# 
# Return true if the specified block was selected for this request.

sub has_block {
    
    my ($request, $key_or_name) = @_;
    
    return 1 if $request->{block_hash}{$key_or_name};
}


# output_block ( name )
# 
# Return true if the named block is selected for the current request.

sub block_selected {

    return $_[0]->{block_hash}{$_[1]};
}


# substitute_select ( substitutions ... )
# 
# Make the specified substitutions in the select and tables hashes for this
# request.  You can pass either a list such as ( a => 'b', c => 'd' ) or a
# hashref.

sub substitute_select {

    my $request = shift;
    
    my $subst;
    
    # First unpack the arguments.
    
    if ( ref $_[0] eq 'HASH' )
    {
	croak "substitute_select: you must pass either a single hashref or a list of substitutions\n"
	    if @_ > 1;
	
	$subst = shift;
    }
    
    else
    {
	$subst = { @_ };
    }
    
    # Keep a count of the number of substitutions.
    
    my $count = 0;
    
    # Then substitute the field values, if there are any for this request.
    
    if ( ref $request->{select_list} eq 'ARRAY' )
    {
	foreach my $f ( @{$request->{select_list}} )
	{
	    $f =~ s/\$(\w+)/$subst->{$1}||"\$$1"/eog and $count++;
	}
    }
    
    # Then substitute the table keys, if there are any for this request.
    
    if ( ref $request->{tables_hash} eq 'HASH' )
    {
	foreach my $k ( keys %{$request->{tables_hash}} )
	{
	    if ( $k =~ qr{ ^ \$ (\w+) $ }xs )
	    {
		$request->{tables_hash}{$subst->{$1}} = $request->{tables_hash}{$k};
		delete $request->{tables_hash}{$k};
		$count++;
	    }
	}
    }
    
    # Return the number of substitutions made.
    
    return $count;
}


# select_list ( subst )
# 
# Return a list of strings derived from the 'select' records passed to
# define_output.  The parameter $subst, if given, should be a hash of
# substitutions to be made on the resulting strings.

sub select_list {
    
    my ($request, $subst) = @_;
    
    my @fields = @{$request->{select_list}} if ref $request->{select_list} eq 'ARRAY';
    
    if ( defined $subst && ref $subst eq 'HASH' )
    {
	foreach my $f (@fields)
	{
	    $f =~ s/\$(\w+)/$subst->{$1}||"\$$1"/eog;
	}
    }
    
    return @fields;
}


# select_hash ( subst )
# 
# Return the same set of strings as select_list, but in the form of a hash.

sub select_hash {

    my ($request, $subst) = @_;
    
    return map { $_ => 1} $request->select_list($subst);
}


# select_string ( subst )
# 
# Return the select list (see above) joined into a comma-separated string.

sub select_string {
    
    my ($request, $subst) = @_;
    
    return join(', ', $request->select_list($subst));    
}


# tables_hash ( )
# 
# Return a hashref whose keys are the values of the 'tables' attributes in
# 'select' records passed to define_output.

sub tables_hash {
    
    my ($request) = @_;
    
    return $request->{tables_hash};
}


# add_table ( name )
# 
# Add the specified name to the table hash.

sub add_table {

    my ($request, $table_name, $real_name) = @_;
    
    if ( defined $real_name )
    {
	if ( $request->{tables_hash}{"\$$table_name"} )
	{
	    $request->{tables_hash}{$real_name} = 1;
	}
    }
    else
    {
	$request->{tables_hash}{$table_name} = 1;
    }
}


# filter_hash ( )
# 
# Return a hashref derived from 'filter' records passed to define_output.

sub filter_hash {
    
    my ($request) = @_;
    
    return $request->{filter_hash};
}


# param_keys ( )
# 
# Return a list of strings representing the cleaned parameter keys from this
# request.  These will often be the same as the original parameter names, but
# may be different if 'alias' or 'key' was specified in any of the relevant
# validation rules.

sub param_keys {
    
    my ($request) = @_;
    
    return $request->{valid}->keys() if $request->{valid};
    return;
}


# clean_param ( name )
# 
# Return the cleaned value of the named parameter, or the empty string if it
# doesn't exist.

sub clean_param {
    
    my ($request, $name) = @_;
    
    return '' unless ref $request->{valid};
    return $request->{valid}->value($name) // '';
}


# clean_param_list ( name )
# 
# Return a list of all the cleaned values of the named parameter, or the empty
# list if it doesn't exist.

sub clean_param_list {
    
    my ($request, $name) = @_;
    
    return unless ref $request->{valid};
    my $clean = $request->{valid}->value($name);
    return @$clean if ref $clean eq 'ARRAY';
    return unless defined $clean;
    return $clean;
}


# clean_param_hash ( name )
# 
# Return a hashref whose keys are all of the cleaned values of the named
# parameter, or an empty hashref if it doesn't exist.

sub clean_param_hash {
    
    my ($request, $name) = @_;
    
    return {} unless ref $request->{valid};
    
    my $clean = $request->{valid}->value($name);
    
    if ( ref $clean eq 'ARRAY' )
    {
	return { map { $_ => 1 } @$clean };
    }
    
    elsif ( defined $clean && $clean ne '' )
    {
	return { $clean => 1 };
    }
    
    else
    {
	return {};
    }
}


# param_given ( )
# 
# Return true if the specified parameter was included in this request, whether
# or not it was given a valid value.  Return false otherwise.

sub param_given {

    my ($request, $name) = @_;
    
    return unless ref $request->{valid};
    return exists $request->{valid}{clean}{$name};
}


# validate_params ( ruleset, params )
# 
# Pass the given parameters to the validator, to be validated by the specified ruleset.
# Return the validation result object.

sub validate_params {
    
    my ($request, $rs_name, @params) = @_;
    
    my $context = { ds => $request->{ds}, request => $request };
    my $result = $request->{ds}{validator}->check_params($rs_name, $context, @params);
    
    return $result;
}


# raw_body ( )
# 
# Return the request body as an un-decoded string. If there is none, return the empty string.

sub raw_body {
    
    my ($request) = @_;
    
    return $request->{ds}{backend_plugin}->get_request_body() // '';
}


# decode_body ( )
# 
# Determine what format the request body is in, and decode it.

sub decode_body {
    
    my ($request, $section) = @_;
    
    # First grab (and cache) the undecoded request body.
    
    unless ( defined $request->{raw_body} )
    {
	$request->{raw_body} = $request->{ds}{backend_plugin}->get_request_body() // '';
    }
    
    # If this is empty, return the undefined value.
    
    return undef unless defined $request->{raw_body} && $request->{raw_body} ne '';
    
    # Get the submitted content type.
    
    my $content_type = $request->{ds}{backend_plugin}->get_content_type() // '';
    
    # If the body starts with '{' or '[', assume the format is JSON regardless of content type.
    
    if ( $request->{raw_body} =~ / ^ [{\[] /xsi )
    {
	try {
	    unless ( defined $request->{decoded_body} )
	    {
		# print STDERR "About to decode\n";
		$request->{decoded_body} = JSON->new->utf8->relaxed->decode($request->{raw_body});
		# print STDERR "Decoded: " . $request->{decoded_body} . "\n";
	    }
	}
	
	catch {
	    $request->{decoded_body} = '';
	    $request->{body_error} = $_;
	    $request->{body_error} =~ s{ at /.*}{};
	    # print STDERR "Error: $request->{body_error}\n";
	};
	
	return ($request->{decoded_body}, $request->{body_error});
    }
    
    # If the backend already unpacked the body into a hash ref, just return that.
    
    elsif ( ref $request->{raw_body} eq 'HASH' )
    {
	# my @chunks = split(/&/, $request->{raw_body});
	# $request->{decoded_body} = { };
	
	# foreach my $chunk ( @chunks )
	# {
	#     my ($var, $value) = split(/=/, $chunk, 2);

	#     $var =~ s/%([0-9A-Fa-f]{2})/chr(hex($1))/eg;
	#     $value =~ s/%([0-9A-Fa-f]{2})/chr(hex($1))/eg;

	#     $request->{decoded_body}{$var} = $value;
	# }
	
	$request->{decoded_body} = $request->{raw_body};
	
	return $request->{decoded_body};
    }
    
    # Otherwise, split into rows and return.
    
    else
    {
	my @lines = split(/[\r\n]+/, $request->{raw_body});
	$request->{decoded_body} = \@lines;
	
	return $request->{decoded_body};
    }
}


# exception ( code, message )
# 
# Return an exception object with the specified HTTP result code and
# message. This can be used to return an error result.

sub exception {
    
    my ($request, $code, $message) = @_;
    
    croak "Bad exception code '$code', must be an HTTP result code"
	unless defined $code && $code =~ qr{^\d\d\d$};
    
    unless ( $message )
    {
	if ( $code eq '400' )
	{
	    $message = 'Parameter error';
	}
	
	elsif ( $code eq '404' )
	{
	    $message = 'Not found';
	}
	
	else
	{
	    $message = 'Internal error: please contact the website administrator';
	}
    }
    
    my $exception = { code => $code, message => $message };
    return bless $exception, 'Web::DataService::Exception';
}


# output_field_list ( )
# 
# Return the output field list for this request.  This is the actual list, not
# a copy, so it can be manipulated.

sub output_field_list {
    
    my ($request) = @_;
    return $request->{field_list};
}


# delete_output_field ( field_name )
# 
# Delete the named field from the output list.  This can be called from the
# operation method if it becomes clear at some point that certain fields will
# not be needed.  This can be especially useful for text-format output.

sub delete_output_field {
    
    my ($request, $field_name) = @_;
    
    return unless defined $field_name && $field_name ne '';
    
    my $list = $request->{field_list};
    
    foreach my $i ( 0..$#$list )
    {
	no warnings 'uninitialized';
	if ( $request->{field_list}[$i]{field} eq $field_name )
	{
	    splice(@$list, $i, 1);
	    return;
	}
    }
}


# debug ( )
# 
# Return true if we are in debug mode.

sub debug {
    
    my ($request) = @_;
    
    return $Web::DataService::DEBUG;
}


# debug_line ( )
#
# Output the specified line(s) of text for debugging purposes.

sub debug_line {

    print STDERR "$_[1]\n" if $Web::DataService::DEBUG;
}


# _process_record ( record, steps )
# 
# Process the specified record using the specified steps.

sub _process_record {
    
    my ($request, $record, $steps) = @_;
    my $ds = $request->{ds};
    
    return $ds->process_record($request, $record, $steps);
}


# result_limit ( )
#
# Return the result limit specified for this request, or undefined if
# it is 'all'.

sub result_limit {
    
    return defined $_[0]->{result_limit} && $_[0]->{result_limit} ne 'all' && $_[0]->{result_limit};
}


# result_offset ( will_handle )
# 
# Return the result offset specified for this request, or zero if none was
# specified.  If the parameter $will_handle is true, then auto-offset is
# suppressed.

sub result_offset {
    
    my ($request, $will_handle) = @_;
    
    $request->{offset_handled} = 1 if $will_handle;
    
    return $request->{result_offset} || 0;
}


# sql_limit_clause ( will_handle )
# 
# Return a string that can be added to an SQL statement in order to limit the
# results in accordance with the parameters specified for this request.  If
# the parameter $will_handle is true, then auto-offset is suppressed.

sub sql_limit_clause {
    
    my ($request, $will_handle) = @_;
    
    $request->{offset_handled} = $will_handle ? 1 : 0;
    
    my $limit = $request->{result_limit};
    my $offset = $request->{result_offset} || 0;
    
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


# require_preprocess ( arg )
# 
# If the argument is true, then the result set will be processed before
# output. This will mean that the entire result set will be held in the memory
# of the dataservice process before being sent to the client, no matter how
# big it is.
# 
# If the argument is '2', then this will only be done if row counts were
# requested and not otherwise.

sub require_preprocess {
    
    my ($request, $arg) = @_;
    
    croak "you must provide a defined argument, either 0, 1, or 2"
	unless defined $arg && ($arg eq '0' || $arg eq '1' || $arg eq '2');
    
    if ( $arg eq '2' )
    {
	$request->{process_before_count} = 1;
	$request->{preprocess} = 0;
    }
    
    elsif ( $arg eq '1' )
    {
	$request->{preprocess} = 1;
    }
    
    elsif ( $arg eq '0' )
    {
	$request->{process_before_count} = 0;
	$request->{preprocess} = 0;
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
    
    my ($request) = @_;
    
    if ( $request->{display_counts} )
    {
	($request->{result_count}) = $request->{dbh}->selectrow_array("SELECT FOUND_ROWS()");
    }
    
    return $request->{result_count};
}


# set_result_count ( count )
# 
# This method should be called if the backend database does not implement the
# SQL FOUND_ROWS() function.  The database should be queried as to the result
# count, and the resulting number passed as a parameter to this method.

sub set_result_count {
    
    my ($request, $count) = @_;
    
    $request->{result_count} = $count;
}


# add_warning ( message )
# 
# Add a warning message to this request object, which will be returned as part
# of the output.

sub add_warning {

    my $request = shift;
    
    foreach my $m (@_)
    {
	push @{$request->{warnings}}, $m if defined $m && $m ne '';
    }
}


# warnings
# 
# Return any warning messages that have been set for this request object.

sub warnings {

    my ($request) = @_;
    
    return unless ref $request->{warnings} eq 'ARRAY';
    return @{$request->{warnings}};
}


sub add_caution {

    my ($self, $error_msg) = @_;
    
    $self->{cautions} = [] unless ref $self->{cautions} eq 'ARRAY';
    push @{$self->{cautions}}, $error_msg;
}


sub cautions {
    
    my ($self) = @_;
    
    return @{$self->{cautions}} if ref $self->{cautions} eq 'ARRAY';
    return;
}


sub add_error {
    
    my ($self, $error_msg) = @_;
    
    $self->{errors} = [] unless ref $self->{errors} eq 'ARRAY';
    push @{$self->{errors}}, $error_msg;
}


sub errors {

    my ($self) = @_;
    
    return @{$self->{errors}} if ref $self->{errors} eq 'ARRAY';
    return;
}


# display_header
# 
# Return true if we should display optional header material, false
# otherwise.  The text formats respect this setting, but JSON does not.

sub display_header {
    
    return $_[0]->{display_header};
}


# display_datainfo
# 
# Return true if the data soruce should be displayed, false otherwise.

sub display_datainfo {
    
    return $_[0]->{display_datainfo};    
}


# display_counts
# 
# Return true if the result count should be displayed along with the data,
# false otherwise.

sub display_counts {

    return $_[0]->{display_counts};
}


# params_for_display
# 
# Return a list of (parameter, value) pairs for use in constructing response
# headers.  These are the cleaned parameter values, not the raw ones.

sub params_for_display {
    
    my $request = $_[0];
    my $ds = $request->{ds};
    my $validator = $ds->{validator};
    my $rs_name = $request->{ruleset};
    my $path = $request->{path};
    
    # First get the list of all parameters allowed for this result.  We will
    # then go through them in order to ensure a known order of presentation.
    
    my @param_list = $ds->list_ruleset_params($rs_name);
    
    # We skip some of the special parameter names, specifically those that do
    # not affect the content of the result.
    
    my %skip;
    
    $skip{$ds->{special}{datainfo}} = 1 if $ds->{special}{datainfo};
    $skip{$ds->{special}{linebreak}} = 1 if $ds->{special}{linebreak};
    $skip{$ds->{special}{count}} = 1 if $ds->{special}{count};
    $skip{$ds->{special}{header}} = 1 if $ds->{special}{header};
    $skip{$ds->{special}{save}} = 1 if $ds->{special}{save};
    
    # Now filter this list.  For each parameter that has a value, add its name
    # and value to the display list.
    
    my @display;
    
    foreach my $p ( @param_list )
    {
	# Skip parameters that don't have a value, or that we have noted above.
	
	next unless defined $request->{clean_params}{$p};
	next if $skip{$p};
	
	# Others get included along with their value(s).
	
	my @values = $request->clean_param_list($p);
	
	# Go through the values; if any one is an object with a 'regenerate'
	# method, then call it.
	
	foreach my $v (@values)
	{
	    if ( ref $v && $v->can('regenerate' ) )
	    {
		$v = $v->regenerate;
	    }
	}
	
	push @display, $p, join(q{,}, @values);
    }
    
    return @display;
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

    my ($request) = @_;
    
    # Start with a default hashref with empty fields.  This is what will be returned
    # if no information is available.
    
    my $r = { found => $request->{result_count} // '',
	      returned => $request->{result_count} // '',
	      offset => $request->{result_offset} // '' };
    
    # If no result count was given, just return the default hashref.
    
    return $r unless defined $request->{result_count};
    
    # Otherwise, figure out the start and end of the output window.
    
    my $window_start = defined $request->{result_offset} && $request->{result_offset} > 0 ?
	$request->{result_offset} : 0;
    
    my $window_end = $request->{result_count};
    
    # If the offset and limit together don't stretch to the end of the result
    # set, adjust the window end.
    
    if ( defined $request->{result_limit} && $request->{result_limit} ne 'all' &&
	 $window_start + $request->{result_limit} < $window_end )
    {
	$window_end = $window_start + $request->{result_limit};
    }
    
    # The number of records actually returned is the length of the output
    # window. 
    
    $r->{returned} = $window_end - $window_start;
    
    return $r;
}


# set_extra_datainfo ( key, value )
# 
# Register a key, with a corresponding value. This key/value pair will be added to the datainfo
# list, and presented if the user has asked for it to be displayed.  If the output format is JSON,
# the value may be a hashref or arrayref. Otherwise, it should be a scalar. If the value is
# undefined, nothing will be displayed, and any previously set value for this key will be
# removed. Keys will be displayed in the order in which they were first set.

sub set_extra_datainfo {
    
    my ($request, $key, $title, $value) = @_;
    
    $request->{extra_datainfo}{$key} = $value;
    $request->{title_datainfo}{$key} = $title;
    push @{$request->{list_datainfo}}, $key;
}


# linebreak
# 
# Return the linebreak sequence that should be used for the output of this request.

sub linebreak {

    return $_[0]->{output_linebreak} eq 'cr' ? "\r"
	 : $_[0]->{output_linebreak} eq 'lf' ? "\n"
					     : "\r\n";
}



# get_config ( )
# 
# Return a hashref providing access to the configuration directives for this
# data service.

sub get_config {
    
    my ($request) = @_;
    
    return $request->{ds}->get_config;
}


# get_connection ( )
# 
# Get a database handle, assuming that the proper directives are present in
# the config.yml file to allow a connection to be made.

sub get_connection {
    
    my ($request) = @_;
    
    return $request->{dbh} if ref $request->{dbh};
    
    $request->{dbh} = $request->{ds}{backend_plugin}->get_connection($request->{ds});
    return $request->{dbh};
}



# set_cors_header ( arg )
# 
# Set the CORS access control header according to the argument.

sub set_cors_header {

    my ($request, $arg) = @_;
    
    $Web::DataService::FOUNDATION->set_cors_header($request, $arg);
}


# set_content_type ( type )
# 
# Set the content type according to the argument.

sub set_content_type {
    
    my ($request, $type) = @_;
    
    $Web::DataService::FOUNDATION->set_content_type($request, $type);
}


# summary_data ( record )
# 
# Add a set of summary data to the result.  The argument must be a single hashref.

sub summary_data {
    
    my ($request, $summary) = @_;
    
    croak 'summary_data: the argument must be a hashref' unless ref $summary eq 'HASH';
    $request->{summary_data} = $summary;
}


# single_result ( record )
# 
# Set the result of this operation to the single specified record.  Any
# previously specified results will be removed.

sub single_result {

    my ($request, $record) = @_;
    
    $request->clear_result;
    return unless defined $record;
    
    croak "single_result: the argument must be a hashref\n"
	unless ref $record && reftype $record eq 'HASH';
    
    $request->{main_record} = $record;
}


# list_result ( record_list )
# 
# Set the result of this operation to the specified list of results.  Any
# previously specified results will be removed.

sub list_result {
    
    my $request = shift;
    
    $request->clear_result;
    return unless @_;
    
    # If we were given a single listref, just use that.
    
    if ( scalar(@_) == 1 && ref $_[0] && reftype $_[0] eq 'ARRAY' )
    {
	$request->{main_result} = $_[0];
	return;
    }
    
    # Otherwise, go through the arguments one by one.
    
    my @result;
    
    while ( my $item = shift )
    {
	next unless defined $item;
	croak "list_result: arguments must be hashrefs or listrefs\n"
	    unless ref $item && (reftype $item eq 'ARRAY' or reftype $item eq 'HASH');
	
	if ( reftype $item eq 'ARRAY' )
	{
	    push @result, @$item;
	}
	
	else
	{
	    push @result, $item;
	}
    }
    
    $request->{main_result} = \@result;
}


# data_result ( data )
# 
# Set the result of this operation to the value of the specified scalar.  Any
# previously specified results will be removed.

sub data_result {
    
    my ($request, $data) = @_;
    
    $request->clear_result;
    return unless defined $data;
    
    croak "data_result: the argument must be either a scalar or a scalar ref\n"
	if ref $data && reftype $data ne 'SCALAR';
    
    $request->{main_data} = ref $data ? $$data : $data;
}


# values_result ( values_list )
# 
# Set the result of this operation to the specified list of data values.  Each
# value should be a scalar.

sub values_result {
    
    my $request = shift;
    
    $request->clear_result;
    
    if ( ref $_[0] eq 'ARRAY' )
    {
	$request->{main_values} = $_[0];
    }
    
    else
    {
	$request->{main_values} = [ @_ ];
    }
}


# sth_result ( sth )
# 
# Set the result of this operation to the specified DBI statement handle.  Any
# previously specified results will be removed.

sub sth_result {
    
    my ($request, $sth) = @_;
    
    $request->clear_result;
    return unless defined $sth;
    
    croak "sth_result: the argument must be an object that implements 'fetchrow_hashref'\n"
	unless ref $sth && $sth->can('fetchrow_hashref');
    
    $request->{main_sth} = $sth;
}


# add_result ( record... )
# 
# Add the specified record(s) to the list of result records for this operation.
# Any result previously specified by any method other than 'add_result' or
# 'list_result' will be cleared.

sub add_result {
    
    my $request = shift;
    
    $request->clear_result unless ref $request->{main_result} eq 'ARRAY';
    return unless @_;
    
    croak "add_result: arguments must be hashrefs\n"
	unless ref $_[0] && reftype $_[0] eq 'HASH';
    
    push @{$request->{main_result}}, @_;
}


# clear_result
# 
# Clear all results that have been specified for this operation.

sub clear_result {
    
    my ($request) = @_;
    
    delete $request->{main_result};
    delete $request->{main_record};
    delete $request->{main_data};
    delete $request->{main_sth};
}


# skip_output_record
#
# This method should only be called from a before_record_hook. It directs that the record about to
# be output should be skipped.

sub skip_output_record {

    my ($request, $record) = @_;
    
    $record->{_skip_record} = 1 if $record;
}


# alternate_output_block ( block_name )
# 
# Call this method from a before_record_hook routine to select an alternate output block for the
# record.

sub alternate_output_block {

    my ($request, $block_name) = @_;

    croak "'alternate_output_block' is obsolete.";
    
    # croak "unknown block '$block_name'" unless $request->{ds}{block}{$block_name};
    
    # $request->{_alternate_block} = $block_name;

    # unless ( exists $request->{block_field_list}{$block_name} )
    # {
    # 	$request->{ds}->configure_block($request, $block_name);
    # }
}


sub select_output_block {
    
    my ($request, $record, $block_name) = @_;
    
    croak "unknown block '$block_name'" unless $request->{ds}{block}{$block_name};
    
    $record->{_output_block} = $block_name;

    unless ( exists $request->{block_field_list}{$block_name} )
    {
	$request->{ds}->configure_block($request, $block_name);
    }
}


1;
