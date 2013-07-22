#!/opt/local/bin/perl
# 
# Paleobiology Data Services
# 
# This application provides data services that query the Paleobiology Database
# (MySQL version).  It is implemented using the Perl Dancer framework.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>

package PBDB_Data;

use Dancer;
use Dancer::Plugin::Database;
use Dancer::Plugin::StreamData;
use Dancer::Plugin::ValidateParams;
use Try::Tiny;
use Scalar::Util qw(blessed);
use Pod::Simple::HTML;
use Pod::Simple::Text;

use Taxonomy;
use DataQuery;
use TaxonQuery;
use TreeQuery;
use CollectionQuery;


set environment => 'development';
set apphandler => 'Debug';
set log => 'debug';

our(%HELP_TEXT);


# Specify the parameters we will accept, and the acceptable value types for
# each of them.

ruleset '1.1:common_params' => 
    [content_type => 'ct', 'json', 'xml', 'txt=text/tab-separated-values', 'csv', 
		{ key => 'output_format' }],
    [optional => 'limit', POS_ZERO_VALUE, { default => $DataQuery::DEFAULT_LIMIT } ],
    [optional => 'limit', ENUM_VALUE('all'),
      { error => "acceptable values for 'limit' are a positive integer, 0, or 'all'" } ],
    [optional => 'offset', POS_ZERO_VALUE],
    [optional => 'count', FLAG_VALUE];

ruleset '1.1:common_display' =>
    [optional => 'vocab', ENUM_VALUE('dwc', 'com', 'pbdb')],
    # The following are only relevant for .csv and .txt output
    [optional => 'quoted', FLAG_VALUE],
    [optional => 'no_header', FLAG_VALUE],
    [optional => 'linebreak', ENUM_VALUE('cr','crlf'), { default => 'crlf' }];

ruleset '1.1:taxon_specifier' => 
    [param => 'name', \&TaxonQuery::validNameSpec, { alias => 'taxon_name' }],
    [param => 'id', POS_VALUE, { alias => 'taxon_id' }],
    [at_most_one => 'name', 'id', 'taxon_id'],
    [optional => 'spelling', ENUM_VALUE('orig', 'current', 'exact'),
      { default => 'current' } ];

ruleset '1.1:taxon_selector' =>
    [param => 'taxon_name', \&TaxonQuery::validNameSpec, { alias => 'name' }],
    [param => 'taxon_id', INT_LIST_PERMISSIVE(1), { alias => 'id' }],
    [param => 'base_name', \&TaxonQuery::validNameSpec],
    [param => 'base_id', INT_LIST_PERMISSIVE(1)],
    [param => 'leaf_name', \&TaxonQuery::validNameSpec],
    [param => 'leaf_id', INT_LIST_PERMISSIVE(1)],
    [param => 'status', ENUM_VALUE('valid', 'senior', 'invalid', 'all'),
      { default => 'valid' } ],
    [at_most_one => 'name', 'taxon_name', 'id', 'taxon_id', 'base_name', 'base_id'],
    [at_most_one => 'name', 'taxon_name', 'id', 'taxon_id', 'leaf_name', 'leaf_id'],
    [at_most_one => 'leaf_name', 'leaf_id'],
    [optional => 'spelling', ENUM_VALUE('orig', 'current', 'exact', 'all'),
      { default => 'current' } ];

ruleset '1.1:taxon_filter' => 
    [optional => 'rank', \&TaxonQuery::validRankSpec],
    [optional => 'extant', BOOLEAN_VALUE],
    [optional => 'depth', POS_VALUE];

ruleset '1.1:taxon_display' => 
    [optional => 'show', LIST_PERMISSIVE('ref','attr','time','coll','phyl','size','det','all')],
    [optional => 'exact', FLAG_VALUE];

ruleset '1.1:taxa/single' => 
    [require => '1.1:taxon_specifier',
	{ error => "you must specify either 'name' or 'id'" }],
    [allow => '1.1:taxon_display'],
    [allow => '1.1:common_display'],
    [allow => '1.1:common_params'];

ruleset '1.1/taxa/list' => 
    [require => '1.1:taxon_selector',
	{ error => "you must specify one of 'name', 'id', 'status', 'base_name', 'base_id', 'leaf_name', 'leaf_id'" }],
    [allow => '1.1:taxon_filter'],
    [allow => '1.1:taxon_display'],
    [allow => '1.1:common_display'],
    [allow => '1.1:common_params'];

ruleset '1.1:coll_specifier' =>
    [param => 'id', POS_VALUE, { alias => 'coll_id' }];

ruleset '1.1:coll_selector' =>
    [param => 'id', INT_LIST_PERMISSIVE(1), { alias => 'coll_id' }],
    [param => 'bin_id', INT_LIST_PERMISSIVE(1)],
    [param => 'taxon_name', \&TaxonQuery::validNameSpec],
    [param => 'taxon_id', INT_LIST_PERMISSIVE(1)],
    [param => 'base_name', \&TaxonQuery::validNameSpec],
    [param => 'base_id', INT_LIST_PERMISSIVE(1)],
    [at_most_one => 'taxon_name', 'taxon_id', 'base_name', 'base_id'],
    [param => 'lngmin', REAL_VALUE('-180.0','180.0')],
    [param => 'lngmax', REAL_VALUE('-180.0','180.0')],
    [param => 'latmin', REAL_VALUE('-90.0','90.0')],
    [param => 'latmax', REAL_VALUE('-90.0','90.0')],
    [together => 'lngmin', 'lngmax', 'latmin', 'latmax',
	{ error => "you must specify all of 'lngmin', 'lngmax', 'latmin', 'latmax' if you specify any of them" }],
    [param => 'loc', STRING_VALUE],		# This should be a geometry in WKT format
    [param => 'min_ma', REAL_VALUE(0)],
    [param => 'max_ma', REAL_VALUE(0)],
    [param => 'interval', STRING_VALUE],
    [optional => 'time_strict', FLAG_VALUE];

ruleset '1.1:coll_display' =>
    [param => 'show', LIST_PERMISSIVE('bin','ref','sref','loc','time','taxa','occ','det')];

ruleset '1.1:colls/single' => 
    [require => '1.1:coll_specifier', { error => "you must specify a collection identifier, either in the URL or with the 'id' parameter" }],
    [allow => '1.1:coll_display'],
    [allow => '1.1:common_display'],
    [allow => '1.1:common_params'];

ruleset '1.1:colls/list' => 
    [require => '1.1:coll_selector', { error => "you must specify one of: 'id', 'bin_id', 'taxon_name', 'taxon_id', 'base_name', 'base_id', ('lng_min', 'lng_max', 'lat_min' and 'lat_max'), 'loc', 'min_ma', 'max_ma', 'interval'" }],
    [allow => '1.1:coll_display'],
    [allow => '1.1:common_display'],
    [allow => '1.1:common_params'];

ruleset '1.1:summary_display' => 
    [param => 'level', INT_VALUE(1,2), { default => 1 }],
    [param => 'show', LIST_VALUE('ext', 'all')];

ruleset '1.1:colls/summary' => 
    [require => '1.1:coll_selector'],
    [allow => '1.1:summary_display'],
    [allow => '1.1:common_display'],
    [allow => '1.1:common_params'];


# Send app pages

get '/app/:filename' => sub {
    
    my $filename = param "filename";
    return send_file("app/$filename", streaming => 1);
};


# Provide the style sheet for documentation pages

get '/data/css/dsdoc.css' => sub {
    
    $DB::single = 1;
    send_file('/css/dsdoc.css');
};


# Translate old URLs without a version number (we assume they are 1.0).

get qr{/data(/.*)} => sub {
    
    my ($path) = splat;
    forward "/data1.0$path";
};


# If the given URL asks for documentation, provide that as best we can.  If
# the given path does not correspond to any known documentation, we provide
# a page explaining what went wrong and providing the proper URLs.

get qr{/data([^/]*)(/.+)\.(html|pod)} => sub {
    
    $DB::single = 1;
    my ($version, $path, $ct) = @_;
    sendDocumentation($version, $path, ct => $ct);
};

get qr{/data([^/]*)(/[^.]+)?} => sub {
    
    $DB::single = 1;
    my ($version, $path) = @_;
    sendDocumentation($version, $path, ct => 'html');
};


# Now we have the version 1.1 routes

get '/data1.1/taxa/single.:ct' => sub {

    querySingle('TaxonQuery', v => '1.1',
		validation => '1.1:taxa/single',
		op => 'single');
};

get '/data1.1/taxa/:id.:ct' => sub {

    querySingle('TaxonQuery', v => '1.1',
		validation => '1.1:taxa/single',
		op => 'single');
};

get '/data1.1/taxa/list.:ct' => sub {

    queryMultiple('TaxonQuery', v => '1.1',
		  validation => '1.1:taxa/list',
		  op => 'list');
};

get '/data1.1/taxa/all.:ct' => sub {

    queryMultiple('TaxonQuery', v => '1.1',
		  validation => '1.1:taxa/list',
		  op => 'list');
};

get '/data1.1/taxa/hierarchy.:ct' => sub {

    queryMultiple('TaxonQuery', v => '1.1',
		  validation => '1.1:taxa/list',
		  op => 'hierarchy');
};

get '/data1.1/colls/single.:ct' => sub {
    
    querySingle('CollectionQuery', v => '1.1',
		validation => '1.1:colls/single',
		op => 'single');
};

get '/data1.1/colls/list.:ct' => sub {

    queryMultiple('CollectionQuery', v => '1.1',
		  validation => '1.1:colls/list',
		  op => 'list');
};

get '/data1.1/colls/all.:ct' => sub {

    queryMultiple('CollectionQuery', v => '1.1',
		  validation => '1.1:colls/list',
		  op => 'list');
};

get '/data1.1/colls/summary.:ct' => sub {

    queryMultiple('CollectionQuery', v => '1.1',
		  validation => '1.1:colls/summary',
		  op => 'summary');
};

get '/data1.1/colls/:id.:ct' => sub {
    
    returnErrorResult({}, "404 Not found") unless params('id') =~ /^[0-9]+$/;
    querySingle('CollectionQuery', v => '1.1',
		validation => '1.1:colls/single',
		op => 'single');
};

# Any other URL beginning with '/data1.1/' is an error.

get qr{/data1\.1/(.*)} => sub {

    my ($path) = splat;
    $DB::single = 1;
    returnErrorResult({}, "404 Not found");
};


# Now we try the version 1.0 URLs

# require "web_app10.pm";
    

# Anything that falls through to here is a bad request

get qr{(.*)} => sub {

    my ($path) = splat;
    $DB::single = 1;
    returnErrorResult({}, "404 Not found");
};


# querySingle ( class, attrs )
# 
# Execute a single-result query on the given class, using the parameters
# specified by the current request.  Those derived from the URL path are in
# %attrs, those from the URL arguments are in %$params.

sub querySingle {

    my ($class, %attrs) = @_;
    
    my ($query, $result);
    
    try {
	
	$DB::single = 1;
	
	# Create a new query object.
	
	$query = $class->new(database(), %attrs);
	
	# Validate and clean the parameters.  If an error occurs,
	# an error response will be generated automatically.
	
	$query->{params} = validate_request($attrs{validation}, params);
	
	# Determine the output fields and vocabulary.
	
	$query->setOutputList();
	
	# Execute the query and generate the result.
	
	$query->fetchSingle();
	$result = $query->generateSingleResult();
    }
    
    # If an error occurs, return an appropriate error response to the client.
    
    catch {

	$result = returnErrorResult($query, $_);
    };
    
    # Send the result back to the client.
    
    return $result;
}


# queryMultiple ( class, attrs )
# 
# Execute a multiple-result query on the given class, using the parameters
# specified by the current request.  Those derived from the URL path are in
# %attrs, those from the URL arguments are in %$params.

sub queryMultiple {
    
    my ($class, %attrs) = @_;
    
    my ($query, $result);
    
    try {
	
	$DB::single = 1;
	
	# Create a new query object.
	
	$query = $class->new(database(), %attrs);
	
	# Validate and clean the parameters.  If an error occurs,
	# an error response will be generated automatically.
	
	$query->{params} = validate_request($attrs{validation}, params);
	
	# Determine the output fields and vocabulary.
	
	$query->setOutputList();
	
	# Execute the query and generate the result.
	
	$query->fetchMultiple();
	
	# If the server supports streaming, call generateCompoundResult with
	# can_stream => 1 and see if it returns any data or not.  If it does
	# return data, send it back to the client as the response content.  An
	# undefined result is a signal that we should stream the data by
	# calling stream_data (imported from Dancer::Plugin::StreamData).
	
	if ( server_supports_streaming )
	{
	    $result = $query->generateCompoundResult( can_stream => 1 ) or
		stream_data( $query, 'streamResult' );
	}
	
	# If the server does not support streaming, we just generate the result
	# and return it.
	
	else
	{
	    $result = $query->generateCompoundResult();
	}
    }
    
    # If an error occurs, pass it to returnErrorResult.
    
    catch {
	
	$result = returnErrorResult($query, $_);
    };
    
    return $result;
}


# showUsage ( path )
# 
# Show a usage message (i.e. manual page) corresponding to the specified path.

sub showUsage {

    my ($path) = @_;
    
    content_type 'text/html; charset=utf-8';
    
    $DB::single = 1;
    
    my ($v) = param "v";
    $v = $DataQuery::CURRENT_VERSION unless defined $v and $DataQuery::VERSION_ACCEPTED{$v};
    
    my $pod = getHelpText({ v => $v }, $path);
    my $html;
    
    if ( param "pod" )
    {
	$html = $pod;
    }
    
    else
    {
	my $parser = new Pod::Simple::HTML;
	$parser->html_css('/data/css/dsdoc.css');
	$parser->output_string(\$html);
	$parser->parse_string_document($pod);
    }
    
    return $html;
}


# setContentType ( ct )
# 
# If the parameter is one of 'xml' or 'json', set content type of the response
# appropriately.  Otherwise, send back a status of 415 "Unknown Media Type".

sub setContentType {
    
    my ($ct) = @_;
    
    if ( $ct eq 'xml' )
    {
	content_type 'text/xml; charset=utf-8';
    }
    
    elsif ( $ct eq 'json' )
    {
	content_type 'application/json; charset=utf-8';
    }
    
    elsif ( $ct eq 'html' )
    {
	content_type 'text/html; charset=utf-8';
    }
    
    elsif ( $ct eq 'csv' )
    {
	content_type 'text/csv; charset=utf-8';
    }
    
    elsif ( $ct eq 'txt' )
    {
	content_type 'text/plain; charset=utf-8';
    }
    
    else
    {
	content_type 'text/plain';
	status(415);
	halt("Unknown Media Type: '$ct' is not supported by this application; use '.json', '.xml', '.csv' or '.txt' instead");
    }
}


# returnErrorResult ( message )
# 
# This method is called if an exception occurs during execution of a route
# subroutine.  If $exception is a blessed reference, then we pass it on (this
# is necessary for streaming to work properly).
# 
# If it is a string that starts with a 4xx HTTP result code, we nip that off
# and send the rest as the result.
# 
# Otherwise, we log it and send back a generic error message with a result
# code of 500 "server error".
# 
# If the content type is 'json', then we send a JSON object with the field
# "error" giving the error message.  Otherwise, we just send a plain text
# message.

sub returnErrorResult {

    my ($query, $exception) = @_;
    my ($code) = 500;
    
    # If the exception is a blessed reference, pass it on.  It's most likely a
    # signal from one part of Dancer to another.
    
    if ( blessed $exception )
    {
	die $exception;
    }
    
    # Otherwise, if it's a string that starts with 4xx, break that out as the
    # HTTP result code.
    
    if ( defined $exception and $exception =~ /^(4\d+)\s+(.*)/ )
    {
	$code = $1;
	$exception = $2;
    }
    
    # Otherwise, log the message and change the message to a generic one.
    
    else
    {
	print STDERR "\n=============\nCaught an error at " . scalar(gmtime) . ":\n";
	print STDERR $exception;
	print STDERR "=============\n";
	$exception = "A server error occurred during processing of this request.  Please notify the administrator of this website.";
    }
    
    # If the message is 'help', display the help message for this route.
    
    if ( $exception =~ /^help/ )
    {
	my $pod = 'HELP'; #getHelpText($query, $label);
	my $parser;
	
	if ( $query->{show_pod} )
	{
	    $exception = $pod;
	}
	
	elsif ( $query->{ct} eq 'html' )
	{
	    $parser = new Pod::Simple::HTML;
	    $parser->html_css('/data/css/dsdoc.css');
	    $exception = '';
	    $parser->output_string(\$exception);
	    $parser->parse_string_document($pod);
	}
	else
	{
	    $parser = new Pod::Simple::Text;
	    $exception = '';
	    $parser->output_string(\$exception);
	    $parser->parse_string_document($pod);
	}
	
    }
    
    # Send back JSON results as an object with field 'error'.
    
    if ( defined $query->{params}{ct} and $query->{params}{ct} eq 'json' )
    {
	status($code);
	
	my $error_obj = { 'error' => $exception };
	
	if ( defined $query->{warnings} and $query->{warnings} > 0 )
	{
	    $error_obj->{warnings} = $query->{warnings};
	}
	
	return to_json($error_obj) . "\n";
    }
    
    # A content type of HTML means we were asked for some documentation.
    
    elsif ( defined $query->{params}{ct} and $query->{params}{ct} eq 'html' )
    {
	status(200);
	content_type 'text/html';
	return $exception . "\n";
    }
    
    # Everything else gets a plain text message.
    
    else
    {
	content_type 'text/plain';
	status($code);
	return $exception . "\n";
    }
}


# getHelpText ( query, label )
# 
# Assemble a help message using the text stored in $HELP_TEXT{$label} together
# with parameter information retrieved from $query.

sub getHelpText {
    
    my ($query, $label, @args) = @_;
    
    my ($v) = param "v";
    $v = $DataQuery::CURRENT_VERSION unless defined $v and $DataQuery::VERSION_ACCEPTED{$v};
    
    my $text = $HELP_TEXT{$label . $v} || $HELP_TEXT{$label} ||
	"Undefined help message for '$label'.  Please contact the system administrator of this website.";
    
    $text =~ s/\[(.*?)\]/substHelpText($query, $1, \@args)/eg;
    
    return $text;
}


sub substHelpText {
    
    my ($query, $variable, $args) = @_;
    
    if ( $variable eq 'URL' )
    {
	return request->uri;
    }
    
    elsif ( $variable eq 'CT' )
    {
	return param "ct";
    }
    
    elsif ( $variable eq 'V' )
    {
	return $query->{v};
    }
    
    elsif ( $variable =~ /^(\d+)$/ )
    {
	return $args->[$1-1] if ref $args eq 'ARRAY' and defined $args->[$1-1];
	return '';
    }
    
    elsif ( $variable =~ /^URL:(.*)/ )
    {
	my ($path) = $1;
	my ($v) = $query->{v};
	my ($orig) = request->uri;
	my ($hostname) = config->{hostname};
	my ($port) = config->{hostport};
	my ($base) = 'http://' . $hostname;
	$base .= ":$port" if defined $port and $port ne '' and $port ne '80';
	my ($tag) = '';
	
	if ( $path =~ /^([BP])(\d.*)/ )
	{
	    if ( $1 eq 'B' )
	    {
		$orig = '/data1.0';
	    }
	    else
	    {
		$orig = '/data';
	    }
	    $path = '/';
	}
	
	if ( $path =~ /\.CT/ )
	{
	    if ( $orig =~ /(\.json|\.xml|\.html)/ )
	    {
		my $suffix = $1;
		$path =~ s/\.CT/$suffix/;
	    }
	}
	
	if ( $path =~ /(.*?)(\#\w+)$/ )
	{
	    $path = $1;
	    $tag = $2;
	}
	
	if ( $orig =~ m{^/data\d} )
	{
	    return $base . "/data${v}${path}${tag}";
	}
	
	elsif ( $path =~ /\?/ )
	{
	    return $base . "/data${path}&v=$v${tag}";
	}
	
	else
	{
	    return $base . "/data${path}?v=$v${tag}";
	}
    }
    
    elsif ( $variable =~ /^XML:(.*)/ )
    {
	return $1 if params->{ct} eq 'xml';
	return '';
    }
    
    elsif ( $variable =~ /^JSON:(.*)/ )
    {
	return $1 if params->{ct} eq 'json';
	return '';
    }
    
    elsif ( $variable =~ /^PARAMS(?::(.*))?/ )
    {
	my $param_string = $query->describeParameters();
	my $label = $1 || "PARAMETERS";
	
	if ( defined $param_string && $param_string ne '' )
	{
	    return "=head2 $label\n\n$param_string";
	}
	else
	{
	    return '';
	}
    }
    
    elsif ( $variable =~ /^REQS(?::(.*))?/ )
    {
	my $req_string = $query->describeRequirements();
	my $label = $1 || "REQUIREMENTS";
	
	if ( defined $req_string && $req_string ne '' )
	{
	    return "=head2 $label\n\n$req_string";
	}
	else
	{
	    return '';
	}
    }
    
    elsif ($variable eq 'RESPONSE' )
    {
	my $field_string = $query->describeFields();
	my $label = "RESPONSE";
	
	if ( defined $field_string && $field_string ne '' )
	{
	    my $string = getHelpText($query, 'RESPONSE') . $field_string;
	    return $string;
	}
	else
	{
	    return getHelpText($query, 'RESPONSE_NOFIELDS');
	}
    }
    
    elsif ( $variable eq 'HEADER' or $variable eq 'FOOTER' )
    {
	my $string = getHelpText($query, $variable);
	return $string;
    }
    
    elsif ( $variable =~ /^HEADER:(.*)/ )
    {
	my $label = $1;
	
	if ( $label eq 'URL' )
	{
	    $label = substHelpText($query, 'URL');
	}

	$label = ": $label";
	
	my $string = getHelpText($query, 'HEADER', $label);
	return $string;
    }
    
    else
    {
	return "[$variable]";
    }
}


sub debug_msg {
    
    debug(@_);
}

1;


dance;


