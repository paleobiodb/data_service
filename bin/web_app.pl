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
use Try::Tiny;
use Scalar::Util qw(blessed);
use Pod::Simple::HTML;
use Pod::Simple::Text;

use DataQuery;
use TaxonQuery;
use TreeQuery;
use CollectionQuery;


set environment => 'development';
set apphandler => 'Debug';
set log => 'debug';

our(%HELP_TEXT);


# Provide the style sheet

get '/data/css/dsdoc.css' => sub {
    
    $DB::single = 1;
    send_file('/css/dsdoc.css');
};


# Translate URL paths that begin with version numbers

get qr{/data(\d+.\d+)/(.*)} => sub {
    
    $DB::single = 1;
    my ($version, $path) = splat;
    forward "/data/$path", { v => $version };
};


# Translate URL paths without version numbers

get qr{/taxa/(.*)} => sub {
    
    $DB::single = 1;
    my ($path) = splat;
    forward "/data/taxa/$path", { v => '1.0' };
};

get qr{/collections/(.*)} => sub {

    $DB::single = 1;
    my ($path) = splat;
    forward "/data/collections/$path", { v => '1.0' };
};


# Deal with individual pages

get qr{/data/?} => sub {
    
    $DB::single = 1;
    showUsage('/');
};

get qr{/data/(\w+)/?} => sub {
    
    $DB::single = 1;
    my ($path) = splat;
    showUsage("/$path/");
};


$HELP_TEXT{'HEADER'} = "
=head1 PaleoDB Data Service[1]

=head2 VERSION

This page documents L<version [V]|[URL:/]> of the service.
";


$HELP_TEXT{'/1.0'} = "
[HEADER]

=head2 DESCRIPTION

The function of this service is to provide programmatic access to the information stored in the Paleobiology Database.  This information can be retrieved in two different formats: XML and JSON.  The field names are taken for the most part from the L<Darwin Core|http://rs.tdwg.org/dwc/> standard.

=head2 VERSION

This page documents version 1.0 of the data service.

=head2 SCOPE

This service currently provides access to the following information:

=over 4

=item L<Taxonomic entries|[URL:/taxa/]>

The PaleoDB includes over 1,000,000 taxonomic entries organized into a hierarchy that encompasses life past and present.

=item L<Collections|[URL:/collections/]>

The core of the PaleoDB is our list of taxonomic collections
from around the globe.

=back

=head2 USAGE

In general, all requests to this service should conform to the following guidelines:

=over 4

=item 1

The HTTP method must be GET or HEAD.

=item 2

Each request must include a service version number, in order to select which version of the service you wish to use.  Different versions may provide different URL paths, different parameters, different output field names, and so on.  The current version of this service is 1.0.  Prior versions may be supported for an indefinite period of time, so that existing client applications will not break when we upgrade to a new version.

=item 3

All URLs must start with either C</data/> or C</dataE<lt>versionE<gt>> (i.e. C</data1.0/>).  If the version number is not given in the URL path, it must be specified with the parameter 'v' as in C</url/path?v=1.0&other_parameters>.  Information about the currently accepted versions can be found through the following links:

=over 4

=item B<version 1.0>

L<[URL:B1.0]> or L<[URL:P1.0]>

=back

=item 4

The return data type is indicated by the suffix on the URL path.  Any URL path ending in C<.json> will return a JSON object, while paths ending in C<.xml> will return XML data in the Darwin Core format.  Any URL path ending in '.html' will return usage information (not data) as an HTML page.

=item 5

Each URL has a set of major parameters, at least one of which is required in order to return information.  Any request that does not include one of these major parameters will respond with usage information.

=item 6

Documentation about the currently valid URLs can be found through the following links:

=over 4

=item L<[URL:/taxa/list.html]>

=item L<[URL:/taxa/hierarchy.html]>

=item L<[URL:/taxa/details.html]>

=item L<[URL:/collections/list.html]>

=item L<[URL:/collections/details.html]>

=back

=back

=head2 RESPONSE

All data requested through this service can be returned in either of two formats: JSON and XML.  The output format is selected by the suffix on the URL path, which can be .json or .xml respectively.  The following sections provide more information on each format.

Each response is limited by default to 500 records, unless a different limit is specified by the use of the C<limit> parameter.  If you specify C<limit=all>, then all matching records will be returned.  I<Be warned that the result set may exceed one million records for some queries!>  Please be responsible about using such queries, so as not to overwhelm our server.

You can always get a count of the number of records found, by including the C<count> parameter (no value is required).  This will include two additional pieces of information in the response: the number of records found, and the number of records returned (which may be smaller because of the limit noted above).  By using C<count> with C<limit=0>, you can find out how many records would be returned without actually getting any data.

The HTTP response code will indicate the following conditions:

=over 4

=item B<200>

The request was fulfilled.  You must check the contents of the response to determine whether or not any records were found and returned.

=item B<400>

The request could not be fulfilled, because of invalid parameters

=item B<404>

The request could not be fulfilled, because of an invalid URL path.

=item B<500>

The request could not be fulfilled, because a server error occurred.

=back

=head3 JSON responses

All JSON responses will be single objects, which may contain one or more of the following fields:

=over 4

=item B<records>

If the request could be fulfilled, this field will be present.  Its value will be an array (possibly empty) of JSON objects each representing a record in the result set.  The fields of these individual objects are described in the documentation for each URL path.

=item B<records_found>

If the C<count> parameter was specified, this field will be present and will state the number of records found by the query.  This number may exceed the limit on records returned.

=item B<records_returned>

If the C<count> parameter was specified, this field will be present and will state the number of records actually returned.

=item B<error>

If the request could not be fulfilled because of invalid parameters, this field will be present and will contain a diagnostic error message.  If the request could not be fulfilled because of a server error, the message will not be very helpful.  In that case, you will need to contact the server administrator.

=item B<warnings>

If problems occurred which did not prevent the request from being fulfilled, this field will be present.  Its value will be an array of strings, each one providing a diagnostic message.

=back

=head3 XML responses

All XML responses will be formatted as L<Darwin Core|http://rs.tdwg.org/dwc/>
record sets.  Additional information (if any) will be provided via XML comments, which may include one or more of the following:

=over 4

=item C<E<lt>!-- records found: I<n> --E<gt>>

If the C<count> parameter was specified, this comment will be present and will state the number of records found by the query.  This number may exceed the limit on records returned.

=item C<E<lt>!-- records returned: I<n> --E<gt>>

If the C<count> parameter was specified, this comment will be present and will state the number of records actually returned.

=item C<E<lt>!-- warnings: I<messages> --E<gt>>

If problems occurred which did not prevent the request from being fulfilled, this comment will be present.  It will include a list of diagnostic messages, separated by semicolons.

=back

If an error occurs that prevents the request from being fulfilled, the response will simply be the error message as plain text.

[FOOTER]

=cut
";


$HELP_TEXT{RESPONSE} = "
=head2 RESPONSE

See L<here|[URL:/#RESPONSE]> for the structure of the response.  The field names for each record are taken from the Darwin Core standard, with a few necessary additions.  The fields provided by this URL path are as follows:

";


$HELP_TEXT{RESPONSE_NOFIELDS} = "
=head2 RESPONSE

See L<here|[URL:/#RESPONSE]> for the structure of the response.  The field names for each record are taken from the Darwin Core standard, with a few necessary additions.

";


$HELP_TEXT{FOOTER} = "
=head2 AUTHOR

This service is provided by the L<Paleobiology Database|http://www.paleodb.org/cgi-bin/bridge.pl?a=displayPage&page=paleodbFAQ>, a joint project of the L<University of Wisconsin-Madison|http://www.wisc.edu/> and L<Macquarie University|http://www.mq.edu.au/>.

If you have questions about this service, please contact Michael McClennen E<lt>L<mmcclenn\@geology.wisc.edu|mailto:mmcclenn\@geology.wisc.edu>E<gt>.
";

$HELP_TEXT{'/taxa/1.0'} = "
[HEADER:/taxa/]

=head2 DESCRIPTION

The URL paths under this heading provide access to the taxonomic hierarchy stored in the Paleobiology Database.

=head2 USAGE

The currently available paths are as follows:

=over 4

=item L</taxa/list|[URL:/taxa/list.html]>

A query using this path will return a list of records representing taxa that match the criteria specified by the parameters.  It can be used to show the descendants of a given taxon, the ancestors of a given taxon, taxa whose names match a given pattern, taxa of a given rank, or all of the taxa in the database (the latter will be an extremely large result set).

=item L</taxa/details|[URL:/taxa/details.html]>

A query using this path will return a single record representing the given taxon, which can be specified either by name or by identifier (taxon_no).

=item L</taxa/hierarchy|[URL:/taxa/hierarchy.html]>

A query using this path will return a hierarchical list of records representing the portion of the hierarchy rooted at the given taxon.  The base taxon can be specified either by name or by identifier (taxon_no).

=back

[FOOTER]

";


$HELP_TEXT{'/taxa/hierarchy1.0'} = "[HEADER:/taxa/hierarchy]

=head2 DESCRIPTION

The function of this URL path is to retrieve parts of the taxonomic hierarchy stored in the Paleobiology Database.  The records are returned either as a hierarchical list in JSON format or as a L<Darwin Core|http://rs.tdwg.org/dwc/terms/index.html> record set in XML format depending upon the suffix provided.

The records returned by this URL path provide minimal detail.  If you need more extensive data about the returned taxa, use L<[URL:/taxa/list.CT]> instead.  See L<[URL:/]> for more information about the format of the responses.

=head2 USAGE

Here are some usage examples:

=over 4

L<[URL:/taxa/hierarchy.json?base_name=Dascillidae]>

L<[URL:/taxa/hierarchy.json?taxon_no=69296]>

L<[URL:/taxa/hierarchy.xml?base_name=Dascillidae&rank=genus]>

=back

[PARAMS]

[REQS]

[RESPONSE]

[FOOTER]
";

get '/data/taxa/hierarchy.:ct' => sub {

    doQueryMultiple('TreeQuery', '/taxa/hierarchy');
};


$HELP_TEXT{'/taxa/list1.0'} = "[HEADER:/taxa/list]

=head2 DESCRIPTION

The function of this URL path is to retrieve parts of the taxonomic hierarchy stored in the Paleobiology Database.  The records are returned either as a straight list in JSON format or as a L<Darwin Core|http://rs.tdwg.org/dwc/terms/> record set in XML format depending upon the suffix provided.

This URL path provides a variety of options for selecting which taxa to display, and a number of options to select which information to return about them.

=head2 USAGE

Here are some usage examples:

=over 4

L<[URL:/taxa/list.json?base_name=Dascillidae&type=synonyms&extant]>

L<[URL:/taxa/list.json?base_no=69296&show=ref,attr]>

L<[URL:/taxa/list.xml?leaf_name=Dascillidae]>

=back

[PARAMS]

[REQS]

[RESPONSE]

[FOOTER]
";

get '/data/taxa/list.:ct' => sub {
    
    doQueryMultiple('TaxonQuery', '/taxa/list');
};


get '/data/taxa/all.:ct' => sub {

    forward '/taxa/list.' . params->{ct};
};


$HELP_TEXT{'/taxa/details1.0'} = "[HEADER:/taxa/details]

=head2 DESCRIPTION

The function of this URL path is to retrieve detailed information about a single taxon from the Paleobiology Database.  The record is returned as an object in JSON format or as a L<Darwin Core|http://rs.tdwg.org/dwc/> record set in XML format depending upon the suffix provided.

=head2 USAGE

Here are some usage examples:

=over 4

L<[URL:/taxa/details.json?taxon_name=Dascillidae]>

L<[URL:/taxa/details.json?taxon_no=69296&show=ref,attr]>

=back

[PARAMS]

[REQS]

[RESPONSE]

[FOOTER]
";

get '/data/taxa/details.:ct' => sub {

    doQuerySingle('TaxonQuery', '/taxa/details');
};


get qw{/data/taxa/(\d+)\.(\w+)} => sub {
       
    my ($id, $ct) = splat;
    forward "/data/taxa/details.$ct", { id => $id, deprecated => 1 };
};


$HELP_TEXT{'/collections/1.0'} = "[HEADER:/collections/]

=head2 DESCRIPTION

The URL paths under this heading provide access to information about paleontological collections stored in the Paleobiology Database.

=head2 USAGE

The currently available paths are as follows:

=over 4

=item L</collections/list|[URL:/collections/list.html]>

A query using this path will return a list of records representing collections that match the criteria specified by the parameters.

=item L</collections/details|[URL:/collections/details.html]>

A query using this path will return a single record representing a collection, which must be specified by identifier (collection_no).

=back

[FOOTER]
";


$HELP_TEXT{'/collections/list1.0'} = "[HEADER:/collections/list]

The function of this URL is to retrieve information about paleontological collections from the Paleobiology Database.  There are a variety of options to select which collections to display, and a number of options to select which information to display about them.

[PARAMS]

[REQS]

[RESPONSE]

[FOOTER]
";

get '/data/collections/list.:ct' => sub {

    doQueryMultiple('CollectionQuery', '/collections/list');
};


$HELP_TEXT{'/collections/details'} = "[HEADER:/collections/details]

=head2 DESCRIPTION

The function of this URL is to retrieve detailed information about a single paleontological collection from the Paleobiology Database.  There are a number of options to select which information to display about the indicated collection.

[PARAMS]

[REQS]

[RESPONSE]

[FOOTER]
";

get '/data/collections/details.:ct' => sub {

    doQuerySingle('CollectionQuery', '/collections/details');
};


# The following routines are used in the execution of the above routes.


# doQueryMultiple ( class )
# 
# Execute a multiple-result query on the given class, using the URL parameters
# from the current request.

sub doQueryMultiple {
    
    my ($class, $label) = @_;
    
    my ($query, $result);
    my ($params) = scalar(params);
    
    try {
	
	$DB::single = 1;
	
	# Set the content type of the response, or generate an error if
	# $params->{ct} is unrecognized.
	
	setContentType($params->{ct});
	
	# Otherwise, create a new query object, set the parameters, and execute
	# it the query.
	
	$query = $class->new(dbh => database(), version => 'multiple');
	
	$query->checkParameters($params);
	$query->setParameters($params);
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
	
	$result = returnErrorResult($query, $label, $_);
    };
    
    return $result;
}


# doQuerySingle ( )
# 
# Execute a single-result query on the given class, using the URL parameters
# from the current request.

sub doQuerySingle {

    my ($class, $label) = @_;
    
    my ($query, $result);
    my ($params) = scalar(params);
    
    try {
	
	$DB::single = 1;
	
	# Set the content type of the response, or generate an error if
	# $params->{ct} is unrecognized.
	
	setContentType($params->{ct});
	
	# Create a new query object, set the parameters, and execute it the
	# query.
	
	$query = $class->new(dbh => database(), version => 'single');
	
	$query->checkParameters($params);
	$query->setParameters($params);
	$query->fetchSingle();
	
	# Generate the result and return it.
	
	$result = $query->generateSingleResult();
    }
    
    # If an error occurs, pass it to returnErrorResult.
    
    catch {

	$result = returnErrorResult($query, $label, $_);
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
    
    elsif ( $ct eq 'txt' )
    {
	content_type 'text/plain; charset=utf-8';
    }
    
    else
    {
	status(415);
	content_type 'text/plain';
	halt("Unknown Media Type: '$ct' is not supported by this application; use '.json', '.xml', or '.txt' instead");
    }
}


# returnErrorResult ( exception )
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

    my ($query, $label, $exception) = @_;
    my ($code) = 500;
    
    # If the exception is a blessed reference, pass it on.  It's most likely a
    # signal from one part of Dancer to another.
    
    if ( blessed $exception )
    {
	die $exception;
    }
    
    # Otherwise, if it's a string that starts with 4xx, break that out as the
    # HTTP result code.
    
    if ( $exception =~ /^(4\d+)\s+(.*)/ )
    {
	$code = $1;
	$exception = $2;
    }
    
    # Otherwise, log the message and change the message to a generic one.
    
    else
    {
	error "Caught an error at " . scalar(gmtime) . ":\n";
	error $exception;
	$exception = "A server error occurred during processing of this request.  Please notify the administrator of this website.";
    }
    
    # If the message is 'help', display the help message for this route.
    
    if ( $exception =~ /^help/ )
    {
	my $pod = getHelpText($query, $label);
	my $parser;
	
	if ( $query->{show_pod} )
	{
	    $exception = $pod;
	}
	
	elsif ( params->{ct} eq 'html' )
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
    
    if ( params->{ct} eq 'json' )
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
    
    elsif ( params->{ct} eq 'html' )
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


