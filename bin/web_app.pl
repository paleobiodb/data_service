#!/opt/local/bin/perl
# 
# Paleobiology Data Services
# 
# This application provides data services that query the Paleobiology Database
# (MySQL version).  It is implemented using the Perl Dancer framework.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>

use strict;

package PBDB_Data;

use Dancer;
use Dancer::Plugin::Database;
use Dancer::Plugin::StreamData;
use Dancer::Plugin::Validate;
use Template;
use Try::Tiny;
use Scalar::Util qw(blessed);
#use Pod::Simple::HTML;
#use Pod::Simple::Text;

use PodParser;

use Taxonomy;
use DataQuery;
use TaxonQuery;
use TreeQuery;
use CollectionQuery;
use IntervalQuery;
use PersonQuery;
use ConfigQuery;


# If we were called from the command line with 'GET' as the first argument,
# then assume we are running in debug mode.

if ( defined $ARGV[0] and $ARGV[0] eq 'GET' )
{
    set apphandler => 'Debug';
    set logger => 'console';
}


# Map paths to attributes

our (%ROUTE_ATTRS);

use subs qw(setupRoute setupDirectory);

# Specify the parameters we will accept, and the acceptable value types for
# each of them.

ruleset '1.1:common_params' => 
    "The following parameters can be used with all requests:",
    [content_type => 'ct', 'json', 'xml', 'txt=text/tab-separated-values', 'csv', 
	{ key => 'output_format' }],
    [optional => 'limit', POS_ZERO_VALUE, ENUM_VALUE('all'), 
      { error => "acceptable values for 'limit' are a positive integer, 0, or 'all'",
	default => 500 } ],
    "Limits the number of records returned.  The value may be a positive integer, zero, or 'all'.  Defaults to 500.",
    [optional => 'offset', POS_ZERO_VALUE],
    "Returned records start at this offset in the result set.  The value may be a positive integer or zero.",
    [optional => 'count', FLAG_VALUE],
    "If specified, then the response includes the number of records found and the number returned.  This is ignored for CSV and TSV formats.",
    [optional => 'vocab', ENUM_VALUE('dwc', 'com', 'pbdb')],
    "Selects the vocabulary used to name the fields in the response.  Possible values include:", "=over",
    "=item pbdb", "PBDB classic field names.  This is the default for CSV and TSV responses.",
    "=item dwc", "Darwin Core element names, plus a few invented by us.  This is the default for XML responses.",
    "=item com", "3-character abbreviated (\"compact\") field names.  This is the default for JSON responses.",
    "!!The following parameters are only relevant to the CSV and TSV formats:",
    [optional => 'quoted', FLAG_VALUE],
    "If specified, then CSV fields are always quoted.",
    [optional => 'no_header', FLAG_VALUE],
    "If specified, then the header line (which gives the field names) is omitted.",
    [optional => 'linebreak', ENUM_VALUE('cr','crlf'), { default => 'crlf' }],
    "Specifies the linebreak character sequences.  The value may be either 'cr' or 'crlf', and defaults to the latter.";

ruleset '1.1:main_selector' =>
    "The following parameters can be used to specify which records to return.  Except as specified below, you can use these in combination:",
    [param => 'bin_id', INT_VALUE, { list => ',' }],
    "Return only records associated with the specified collection summary (geographic) bin.",
    [param => 'taxon_name', \&TaxonQuery::validNameSpec],
    "Return only records associated with the specified taxonomic name(s).  You may specify multiple names, separated by commas.",
    [param => 'taxon_id', POS_VALUE, { list => ','}],
    "Return only records associated with the specified taxonomic name(s), specified by numeric identifier.",
    "You may specify multiple identifiers, separated by commas.",
    [param => 'base_name', \&TaxonQuery::validNameSpec, { list => ',' }],
    "Return only records associated with the specified taxonomic name(s), or I<any of their children>.",
    "You may specify multiple names, separated by commas.",
    [param => 'base_id', POS_VALUE, { list => ',' }],
    "Return only records associated with the specified taxonomic name(s), specified by numeric identifier, or I<any of their children>.",
    "You may specify multiple identifiers, separated by commas",
    [at_most_one => 'taxon_name', 'taxon_id', 'base_name', 'base_id'],
    "!!Note that you may specify at most one of 'taxon_name', 'taxon_id', 'base_name', 'base_id'.",
    [param => 'exclude_id', POS_VALUE, { list => ','}],
    "Do not return any records whose associated taxonomic name is a child of the given name, specified by numeric identifier.",
    [param => 'person_no', POS_VALUE, { list => ','}],
    "Return only records whose entry was authorized by the given person or people, specified by numeric identifier.",
    [param => 'lngmin', DECI_VALUE('-180.0','180.0')],
    "",
    [param => 'lngmax', DECI_VALUE('-180.0','180.0')],
    "",
    [param => 'latmin', DECI_VALUE('-90.0','90.0')],
    "",
    [param => 'latmax', DECI_VALUE('-90.0','90.0')],
    "Return only records whose geographic location falls within the given bounding box.",
    "Note that if you specify one of these parameters then you must specify all four of them.",
    [together => 'lngmin', 'lngmax', 'latmin', 'latmax',
	{ error => "you must specify all of 'lngmin', 'lngmax', 'latmin', 'latmax' if you specify any of them" }],
    [param => 'loc', ANY_VALUE],		# This should be a geometry in WKT format
    "Return only records whose geographic location falls within the specified region, specified in WKT format.",
    [param => 'min_ma', DECI_VALUE(0)],
    "Return only records whose temporal locality is at least this old, specified in Ma.",
    [param => 'max_ma', DECI_VALUE(0)],
    "Return only records whose temporal locality is at most this old, specified in Ma.",
    [param => 'interval', ANY_VALUE],
    "Return only records whose temporal locality falls within the named geologic time interval.",
    [optional => 'time_strict', FLAG_VALUE],
    "If this parameter is specified, then return only records whose temporal locality falls strictly within the specified interval.",
    "Otherwise, all records whose temporal locality overlaps the specified interval will be returned";

ruleset '1.1:coll_specifier' =>
    [param => 'id', POS_VALUE, { alias => 'coll_id' }];

ruleset '1.1:coll_selector' =>
    [param => 'id', INT_VALUE, { list => ',', alias => 'coll_id' }];

ruleset '1.1:coll_display' =>
    [param => 'level', POS_VALUE],
    [param => 'show', ENUM_VALUE('bin','ref','sref','loc','time','taxa','occ','det'), { list => ',' }];

ruleset '1.1:colls/single' => 
    [require => '1.1:coll_specifier', { error => "you must specify a collection identifier, either in the URL or with the 'id' parameter" }],
    [allow => '1.1:coll_display'],
    [allow => '1.1:common_params'];

ruleset '1.1:colls/list' => 
    [allow => '1.1:coll_selector'],
    [allow => '1.1:main_selector'],
    [allow => '1.1:coll_display'],
    "!> You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request",
    [allow => '1.1:common_params'];

ruleset '1.1:summary_display' => 
    [param => 'level', POS_VALUE, { default => 1 }],
    [param => 'show', ENUM_VALUE('ext','time','all'), { list => ',' }];

ruleset '1.1:colls/summary' => 
    [allow => '1.1:coll_selector'],
    [allow => '1.1:main_selector'],
    [allow => '1.1:summary_display'],
    [allow => '1.1:common_params'];

ruleset '1.1:toprank_selector' =>
    [param => 'show', ENUM_VALUE('formation', 'ref', 'author'), { list => ',' }];

ruleset '1.1:colls/toprank' => 
    [require => '1.1:main_selector'],
    [require => '1.1:toprank_selector'],
    [allow => '1.1:common_params'];

ruleset '1.1:taxon_specifier' => 
    [param => 'name', \&TaxonQuery::validNameSpec, { alias => 'taxon_name' }],
    [param => 'id', POS_VALUE, { alias => 'taxon_id' }],
    [at_most_one => 'name', 'id', 'taxon_id'],
    [optional => 'rank', \&TaxonQuery::validRankSpec],
    [optional => 'spelling', ENUM_VALUE('orig', 'current', 'exact'),
      { default => 'current' } ];

ruleset '1.1:taxon_selector' =>
    [param => 'name', \&TaxonQuery::validNameSpec, { alias => 'taxon_name' }],
    [param => 'id', POS_VALUE, { list => ',', alias => 'base_id' }],
    [param => 'rel', ENUM_VALUE('self', 'synonyms', 'children', 'all_children', 
				'parents', 'all_parents', 'common_ancestor', 'all_taxa'),
      { default => 'self' } ],
    [param => 'status', ENUM_VALUE('valid', 'senior', 'invalid', 'all'),
      { default => 'valid' } ],
    [optional => 'spelling', ENUM_VALUE('orig', 'current', 'exact', 'all'),
      { default => 'current' } ];

ruleset '1.1:taxon_filter' => 
    [optional => 'rank', \&TaxonQuery::validRankSpec],
    [optional => 'extant', BOOLEAN_VALUE],
    [optional => 'depth', POS_VALUE];

ruleset '1.1:taxon_display' => 
    [optional => 'show', ENUM_VALUE('ref','attr','time','app','applong',
				    'appfirst','coll','phyl','size',
				    'nav','det','all'),
	{ list => ','}],
    [optional => 'exact', FLAG_VALUE];

ruleset '1.1/taxa/single' => 
    [require => '1.1:taxon_specifier',
	{ error => "you must specify either 'name' or 'id'" }],
    [allow => '1.1:taxon_display'],
    [allow => '1.1:common_params'];

ruleset '1.1/taxa/list' => 
    [require => '1.1:taxon_selector',
	{ error => "you must specify one of 'name', 'id', 'status', 'base_name', 'base_id', 'leaf_name', 'leaf_id'" }],
    [allow => '1.1:taxon_filter'],
    [allow => '1.1:taxon_display'],
    [allow => '1.1:common_params'];

ruleset '1.1:interval_selector' => 
    [param => 'order', ENUM_VALUE('older', 'younger'), { default => 'younger' }],
    [param => 'min_ma', DECI_VALUE(0)],
    [param => 'max_ma', DECI_VALUE(0)];

ruleset '1.1/intervals' => 
    [allow => '1.1:interval_selector'],
    [allow => '1.1:common_params'];

ruleset '1.1/config' =>
    "!> You can use any of the L<common parameters|/data.1.1/common_doc.html> with this request.", 
    [allow => '1.1:common_params'];

ruleset '1.1:person_selector' => 
    [param => 'name', ANY_VALUE];

ruleset '1.1:person_specifier' => 
    [param => 'id', POS_VALUE, { alias => 'person_id' }];

ruleset '1.1/people/single' => 
    [allow => '1.1:person_specifier'],
    [allow => '1.1:common_params'];

ruleset '1.1/people/list' => 
    [require => '1.1:person_selector'],
    [allow => '1.1:common_params'];

ruleset '1.1:refs_specifier' => 
    [param => 'id', POS_VALUE, { alias => 'ref_id' }];

ruleset '1.1/refs/single' => 
    [require => '1.1:refs_specifier'],
    [allow => '1.1:common_params'];

ruleset '1.1/refs/toprank' => 
    [require => '1.1:main_selector'],
    [allow => '1.1:common_params'];

# Send app pages

get '/testapp/:filename' => sub {
    
    $DB::single = 1;
    my $filename = param "filename";
    return send_file("testapp/$filename", streaming => 1, callbacks => {});
};


get '/app/:filename' => sub {
    
    $DB::single = 1;
    my $filename = param "filename";
    return send_file("app/$filename", streaming => 1, callbacks => {});
};


# Send style sheets

get '/data:v/css/:filename' => sub {
    
    $DB::single = 1;
    my $filename = param "filename";
    send_file("css/$filename", streaming => 1, callbacks => {});
};


# Translate old URLs without a version number (we assume they are 1.0).

get qr{/data(/.*)} => sub {
    
    my ($path) = splat;
    forward "/data1.0$path";
};


# If the given URL asks for documentation, provide that as best we can.  If
# the given path does not correspond to any known documentation, we provide
# a page explaining what went wrong and providing the proper URLs.

# Any path that is not interpreted above might be a request for documentation.

get qr{ ^ /data ( \d+ \. \d+ / (?: [^/.]* / )* )
	  (?: ( index | \w+_doc ) \. ( html | pod ) )? $ }xs => sub {
	    
    my ($path, $last, $ct) = splat;
    
    $DB::single = 1;
    $path .= $last unless !defined $last || $last eq 'index';
    $path =~ s{/$}{};
    $path =~ s{_doc}{};
    
    sendDocumentation({ path => $path, ct => $ct });
};


get qr{ ^ /data ( \d+ \. \d+ (?: / [^/.]+ )* $ ) } => sub {
    
    my ($path) = splat;
    
    $DB::single = 1;
    
    sendDocumentation({ path => $path, ct => 'html' });
};


# get qr{/data1.1(/.+)\.(html?|pod)} => sub {
    
#     $DB::single = 1;
#     my ($path, $ct) = @_;
#     sendDocumentation(v => '1.1', path => $path, ct => $ct);
# };

# get qr{/data1.1(/[^.]+)?} => sub {
    
#     $DB::single = 1;
#     my ($path) = @_;
#     sendDocumentation(v => '1.1', $path, ct => 'html');
# };


# Now we have the version 1.1 routes:

# Base and miscellaneous

get '/data1.1/config.:ct' => sub {
    
    querySingle({ class => 'ConfigQuery', 
		  params => '1.1/config',
		  op => 'single' });
};

setupDirectory '1.1';

setupRoute '1.1/config' =>
   {
    class => 'ConfigQuery',
    op => 'single',
    output => ['single'],
   };

setupDirectory '1.1/taxa';



# get '/data1.1/config_doc.:ct' => sub {
    
#     sendDocumentation({ path => '1.1/config',
# 			file => '1.1/config_doc.tt',
# 			class => 'ConfigQuery',
# 		        output => ['single'] });
# };

# get '/data1.1/common_doc.:ct' => sub {

#     sendDocumentation({ params => '1.1:common_params', 
# 			file => '1.1/common_doc.tt',
# 			title => 'common parameters' });
# };

# Taxa

# get qr{/data1.1/taxa(?:|/|/index.html?)} => sub {

#     sendDocumentation({ class => 'TaxonQuery',
# 			path => '1.1/taxa/index' });
# };

get '/data1.1/taxa/single.:ct' => sub {

    querySingle({ class => 'TaxonQuery',
		  path => '1.1/taxa/single',
		  op => 'single' });
};

get '/data1.1/taxa/single_doc.:ct' => sub {
    
    sendDocumentation({ class => 'TaxonQuery',
			path => '1.1/taxa/single',
		        output => ['single', 'nav'] });
};

get '/data1.1/taxa/list.:ct' => sub {

    queryMultiple({ class => 'TaxonQuery',
		    path => '1.1/taxa/list',
		    op => 'list' });
};

get '/data1.1/taxa/all.:ct' => sub {

    queryMultiple({ class => 'TaxonQuery',
		    path => '1.1/taxa/list',
		    op => 'list' });
};

get '/data1.1/taxa/hierarchy.:ct' => sub {

    queryMultiple({ class => 'TaxonQuery',
		    path => '1.1/taxa/list',
		    op => 'hierarchy' });
};

get '/data1.1/taxa/:id.:ct' => sub {

    querySingle({ class => 'TaxonQuery',
		  path => '1.1/taxa/single',
		  op => 'single' });
};

# Collections

get '/data1.1/colls/single.:ct' => sub {
    
    querySingle('CollectionQuery', v => '1.1',
		validation => '1.1/colls/single',
		op => 'single');
};

get '/data1.1/colls/list.:ct' => sub {

    queryMultiple('CollectionQuery', v => '1.1',
		  validation => '1.1/colls/list',
		  op => 'list');
};

get '/data1.1/colls/list_doc.:ct' => sub {

    sendDocumentation({ class => 'CollectionQuery',
			path => '1.1/colls/list',
			output => ['single', 'attr', 'ref', 'author', 'bin', 'formation'] });
    
};

get '/data1.1/colls/all.:ct' => sub {

    queryMultiple('CollectionQuery', v => '1.1',
		  validation => '1.1/colls/list',
		  op => 'list');
};

get '/data1.1/colls/toprank.:ct' => sub {

    queryMultiple('CollectionQuery', v => '1.1',
		  validation => '1.1/colls/toprank',
		  op => 'toprank');
};

get '/data1.1/colls/summary.:ct' => sub {

    queryMultiple('CollectionQuery', v => '1.1',
		  validation => '1.1/colls/summary',
		  op => 'summary');
};

get '/data1.1/colls/:id.:ct' => sub {
    
    returnErrorResult({}, "404 Not found") unless params('id') =~ /^[0-9]+$/;
    querySingle('CollectionQuery', v => '1.1',
		validation => '1.1/colls/single',
		op => 'single');
};

get '/data1.1/intervals/list.:ct' => sub {

    queryMultiple('IntervalQuery', v => '1.1',
		  validation => '1.1/intervals',
		  op => 'list');
};

get '/data1.1/intervals/hierarchy.:ct' => sub {
    
    queryMultiple('IntervalQuery', v => '1.1',
		  validation => '1.1/intervals',
		  op => 'hierarchy');
};

get '/data1.1/people/list.:ct' => sub {
    
    queryMultiple('PersonQuery', v => '1.1',
		  validation => '1.1/people/list', op => 'list');
};

get '/data1.1/people/single.:ct' => sub {

    querySingle('PersonQuery', v => '1.1',
		  validation => '1.1/people/single', op => 'single');
};

get '/data1.1/people/:id.:ct' => sub {
    
    returnErrorResult({}, "404 Not found") unless params('id') =~ /^[0-9]+$/;
    querySingle('PersonQuery', v => '1.1',
		validation => '1.1/people/single', op => 'single');
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
# specified by the current request.  The attributes provided are used to
# generate a query object.  The attribute 'validation' 

sub querySingle {

    my ($attrs) = @_;
    
    my ($class, $query, $result);
    
    try {
	
	# Queries are safe to access from anywhere, without regard to browser
	# controls, because they do not have any side effects on either server or
	# client.
	
	header "Access-Control-Allow-Origin" => "*";
	
	$DB::single = 1;
	
	# Create a new query object, from the specified class.
	
	$class = $attrs->{class};
	$query = $class->new(database(), $attrs);
	
	# Validate and clean the parameters.  If an error occurs,
	# an error response will be generated automatically.
	
	$query->{params} = validate_request($attrs->{ruleset}, params);
	
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
    
    my ($attrs) = @_;
    
    my ($class, $query, $result);
    
    try {
	
	# Queries are safe to access from anywhere, without regard to browser
	# controls, because they do not have any side effects on either server or
	# client.
	
	header "Access-Control-Allow-Origin" => "*";
	
	$DB::single = 1;
	
	# Create a new query object, from the specified class.
	
	$class = $attrs->{class};
	$query = $class->new(database(), $attrs);
	
	# Validate and clean the parameters.  If an error occurs,
	# an error response will be generated automatically.
	
	$query->{params} = validate_request($attrs->{ruleset}, params);
	
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


sub setupRoute {
    
    my ($path, $attrs) = @_;
    
    $ROUTE_ATTRS{$path} = $attrs || {};
}


sub setupDirectory {
    
    my ($path, $attrs) = @_;
    
    $ROUTE_ATTRS{$path} = $attrs || {};
    $ROUTE_ATTRS{$path}{is_directory} = 1;
}


# sendDocumentation ( attrs )
# 
# Respond with a documentation message corresponding to the specified attributes.

sub sendDocumentation {
    
    my ($attrs) = @_;
    
    my $path = $attrs->{path};
    my $format = $attrs->{ct} || 'html';
    
    my $ruleset = $attrs->{ruleset} || $ROUTE_ATTRS{$path}{ruleset} || $path;
    my $class = $attrs->{class} || $ROUTE_ATTRS{$path}{class};
    my $output = $attrs->{output} || $ROUTE_ATTRS{$path}{output};
    my $docfile = $attrs->{file} || $ROUTE_ATTRS{$path}{docfile};
    my $title = $attrs->{title} || $ROUTE_ATTRS{$path}{title};
    
    header "Access-Control-Allow-Origin" => "*";
    
    my ($version, $display_path);
    
    if ( $path =~ qr{ ^ ( \d+ \. \d+ ) }xs )
    {
	$version = $1;
    }
    
    $DB::single = 1;
    
    # Compute the string that is used to display the path being documented,
    # and the version string.
    
    # !!! send an error response unless we have a version
    
    if ( $ROUTE_ATTRS{$path}{is_directory} )
    {
	$title ||= "/data$path/";
	$docfile ||= "${path}/index.tt";
    }
    
    else
    {
	$title ||= "/data$path";
	$docfile ||= "${path}_doc.tt";
    }

    # Now pull up the documentation file for the given path and assemble it
    # together with elements describing the parameters and output fields.
    
    set layout => 'doc_1_1';
    
    my $viewdir = config->{views};
    
    unless ( -e "$viewdir/$docfile" )
    {
	$docfile = "$version/error.tt";
    }
    
    my $param_doc = document_params($ruleset) if $ruleset;
    my $response_doc = $class->output_pod('pod', $output) if $class && $output;
    
    my $doc_string = template $docfile, { version => $version, 
					  param_doc => $param_doc,
					  response_doc => $response_doc,
					  title => $title };
    
    # If POD format was requested, return the documentation as is.
    
    if ( $format eq 'pod' )
    {
	content_type 'text/plain';
	return $doc_string;
    }
    
    # Otherwise, convert the POD to HTML and return that.
    
    else
    {
	my $doc_html;
    	# my $parser = new Pod::Simple::HTML;
	# $parser->html_css('/data/css/dsdoc.css');
	# $parser->output_string(\$doc_html);
	# $parser->parse_string_document($doc_string);
	
	my $parser = new PodParser;
	$parser->init_doc;
	$parser->parse_pod($doc_string);
	
	$doc_html = $parser->generate_html({ css => '/data/css/dsdoc.css', tables => 1 });
	
	content_type 'text/html';
	return $doc_html;
    }
}


sub processDocumentation {
    
    my ($doc_ref) = @_;
    
    my $output = '';
    my $table_level = 0;
    
    foreach my $line (split(/\n/, $$doc_ref))
    {
	unless ( $line =~ qr{ <d }x )
	{
	    $output .= $line . "\n";
	    next;
	}
	
	if ( $line =~ s{ < dl ( [^>]* ) > }{<table$1>}x )
	{
	    $table_level++;
	}
	
	#$line =~ s{ < dt ( [^>]* ) > }{<tr><td$1>}x;
	#$line =~ s{ < /dt > }{</td>}x;
	#$line =~ s{ < dd ( [^>]* ) > }{<td$1>}x;
	#$line =~ s{ < /dd > }{</td></tr>}x;
	
	if ( $line =~ s{ </dl> }{</table>}x )
	{
	    $table_level--;
	}
	
	$output .= $line . "\n";
    }
    
    return $output;
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
	header "Access-Control-Allow-Origin" => "*";
	
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

# sub getHelpText {
    
#     my ($query, $label, @args) = @_;
    
#     my ($v) = param "v";
#     $v = $DataQuery::CURRENT_VERSION unless defined $v and $DataQuery::VERSION_ACCEPTED{$v};
    
#     my $text = $HELP_TEXT{$label . $v} || $HELP_TEXT{$label} ||
# 	"Undefined help message for '$label'.  Please contact the system administrator of this website.";
    
#     $text =~ s/\[(.*?)\]/substHelpText($query, $1, \@args)/eg;
    
#     return $text;
# }


# sub substHelpText {
    
#     my ($query, $variable, $args) = @_;
    
#     if ( $variable eq 'URL' )
#     {
# 	return request->uri;
#     }
    
#     elsif ( $variable eq 'CT' )
#     {
# 	return param "ct";
#     }
    
#     elsif ( $variable eq 'V' )
#     {
# 	return $query->{v};
#     }
    
#     elsif ( $variable =~ /^(\d+)$/ )
#     {
# 	return $args->[$1-1] if ref $args eq 'ARRAY' and defined $args->[$1-1];
# 	return '';
#     }
    
#     elsif ( $variable =~ /^URL:(.*)/ )
#     {
# 	my ($path) = $1;
# 	my ($v) = $query->{v};
# 	my ($orig) = request->uri;
# 	my ($hostname) = config->{hostname};
# 	my ($port) = config->{hostport};
# 	my ($base) = 'http://' . $hostname;
# 	$base .= ":$port" if defined $port and $port ne '' and $port ne '80';
# 	my ($tag) = '';
	
# 	if ( $path =~ /^([BP])(\d.*)/ )
# 	{
# 	    if ( $1 eq 'B' )
# 	    {
# 		$orig = '/data1.0';
# 	    }
# 	    else
# 	    {
# 		$orig = '/data';
# 	    }
# 	    $path = '/';
# 	}
	
# 	if ( $path =~ /\.CT/ )
# 	{
# 	    if ( $orig =~ /(\.json|\.xml|\.html)/ )
# 	    {
# 		my $suffix = $1;
# 		$path =~ s/\.CT/$suffix/;
# 	    }
# 	}
	
# 	if ( $path =~ /(.*?)(\#\w+)$/ )
# 	{
# 	    $path = $1;
# 	    $tag = $2;
# 	}
	
# 	if ( $orig =~ m{^/data\d} )
# 	{
# 	    return $base . "/data${v}${path}${tag}";
# 	}
	
# 	elsif ( $path =~ /\?/ )
# 	{
# 	    return $base . "/data${path}&v=$v${tag}";
# 	}
	
# 	else
# 	{
# 	    return $base . "/data${path}?v=$v${tag}";
# 	}
#     }
    
#     elsif ( $variable =~ /^XML:(.*)/ )
#     {
# 	return $1 if params->{ct} eq 'xml';
# 	return '';
#     }
    
#     elsif ( $variable =~ /^JSON:(.*)/ )
#     {
# 	return $1 if params->{ct} eq 'json';
# 	return '';
#     }
    
#     elsif ( $variable =~ /^PARAMS(?::(.*))?/ )
#     {
# 	my $param_string = $query->describeParameters();
# 	my $label = $1 || "PARAMETERS";
	
# 	if ( defined $param_string && $param_string ne '' )
# 	{
# 	    return "=head2 $label\n\n$param_string";
# 	}
# 	else
# 	{
# 	    return '';
# 	}
#     }
    
#     elsif ( $variable =~ /^REQS(?::(.*))?/ )
#     {
# 	my $req_string = $query->describeRequirements();
# 	my $label = $1 || "REQUIREMENTS";
	
# 	if ( defined $req_string && $req_string ne '' )
# 	{
# 	    return "=head2 $label\n\n$req_string";
# 	}
# 	else
# 	{
# 	    return '';
# 	}
#     }
    
#     elsif ($variable eq 'RESPONSE' )
#     {
# 	my $field_string = $query->describeFields();
# 	my $label = "RESPONSE";
	
# 	if ( defined $field_string && $field_string ne '' )
# 	{
# 	    my $string = getHelpText($query, 'RESPONSE') . $field_string;
# 	    return $string;
# 	}
# 	else
# 	{
# 	    return getHelpText($query, 'RESPONSE_NOFIELDS');
# 	}
#     }
    
#     elsif ( $variable eq 'HEADER' or $variable eq 'FOOTER' )
#     {
# 	my $string = getHelpText($query, $variable);
# 	return $string;
#     }
    
#     elsif ( $variable =~ /^HEADER:(.*)/ )
#     {
# 	my $label = $1;
	
# 	if ( $label eq 'URL' )
# 	{
# 	    $label = substHelpText($query, 'URL');
# 	}

# 	$label = ": $label";
	
# 	my $string = getHelpText($query, 'HEADER', $label);
# 	return $string;
#     }
    
#     else
#     {
# 	return "[$variable]";
#     }
# }


sub debug_msg {
    
    debug(@_);
}

1;


dance;


