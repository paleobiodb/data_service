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

use DataQuery;
use TaxonQuery;
use TreeQuery;
use CollectionQuery;


set environment => 'development';
set apphandler => 'Debug';
set log => 'debug';

our(%HELP_TEXT);


get qr{/data(\d+.\d+)/(.*)} => sub {
    
    $DB::single = 1;
    my ($version, $path) = splat;
    forward '/' . $path, { v => $version };
};


get qr{/data/(.*)} => sub {
    
    $DB::single = 1;
    my ($path) = splat;
    forward '/' . $path;
};


$HELP_TEXT{'/taxa/hierarchy'} = "PaleoDB Data Service: [URL]

The function of this URL is to retrieve parts of the taxonomic hierarchy stored in the Paleobiology Database.  The records provided contain minimal detail, and [JSON:are returned as a hierarchical list in JSON format][XML:are returned in Darwin Core XML format].  If you need additional information about the returned taxa, use /taxa/list.[CT] instead.

[PARAMS:Available parameters include:]

[REQS:Requirements:]
";

get '/taxa/hierarchy.:ct' => sub {

    doQueryMultiple('TreeQuery', '/taxa/hierarchy');
};


$HELP_TEXT{'/taxa/list'} = "PaleoDB Data Service: [URL]

The function of this URL is to retrieve part or all of the taxonomic hierarchy stored in the Paleobiology Database.  There are a number of different options to select which taxa to display, and a number of options to select which information to display about them.  The information is returned [XML:in Darwin Core XML format (http://rs.tdwg.org/dwc/terms/index.htm).][JSON:as a JSON object.]

[PARAMS:Available parameters include:]

[REQS:Requirements:]
";

get '/taxa/list.:ct' => sub {
    
    doQueryMultiple('TaxonQuery', '/taxa/list');
};


get '/taxa/all.:ct' => sub {

    forward '/taxa/list.' . params->{ct};
};


$HELP_TEXT{'/taxa/details'} = "PaleoDB Data Service: [URL]

The function of this URL is to retrieve detailed information about a single taxon from the Paleobiology Database.  There are a number of options to select which information to display about the indicated taxon.  The information is returned [XML:in Darwin Core XML format (http://rs.tdwg.org/dwc/terms/index.htm).][JSON:as a JSON object.]

[PARAMS:Available parameters include:]

[REQS:Requirements:]
";

get '/taxa/details.:ct' => sub {

    doQuerySingle('TaxonQuery', '/taxa/details');
};


get '/taxa/:id.:ct' => sub {
    
    forward '/taxa/details.' . params->{ct}, { id => params->{id}, deprecated => 1 };
};


$HELP_TEXT{'/collections/list'} = "PaleoDB Data Service: [URL]

The function of this URL is to retrieve information about paleontological collections from the Paleobiology Database.  There are a number of different options to select which collections to display, and a number of options to select which information to display about them.  The information is returned [XML:in Darwin Core XML format (http://rs.tdwg.org/dwc/terms/index.htm).][JSON:as a JSON object.]

[PARAMS:Available parameters include:]

[REQS:Requirements:]
";

get '/collections/list.:ct' => sub {

    doQueryMultiple('CollectionQuery', '/collections/list');
};


$HELP_TEXT{'/collections/details'} = "PaleoDB Data Service: [URL]

The function of this URL is to retrieve detailed information about a single paleontological collection from the Paleobiology Database.  There are a number of options to select which information to display about the indicated collection.  The information is returned [XML:in Darwin Core XML format (http://rs.tdwg.org/dwc/terms/index.htm).][JSON:as a JSON object.]

[PARAMS:Available parameters include:]

[REQS:Requirements:]
";

get '/collections/details.:ct' => sub {

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
	
	# Create a new query object, set the parameters, and execute it the
	# query.
	
	$query = $class->new(database(), 'multiple');
	
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
	
	$query = $class->new(database(), 'single');
	
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
    
    else
    {
	status(415);
	content_type 'text/plain';
	halt("Unknown Media Type: '$ct' is not supported by this application; use '.json' or '.xml' instead");
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
    
    if ( $exception eq 'help' )
    {
	$exception = getHelpText($query, $label);
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
    
    my ($query, $label) = @_;
    
    my $text = $HELP_TEXT{$label} or 
	return "The help message for this URL was not defined.  Please contact the system administrator of this website.";
    
    $text =~ s/\[(.*?)\]/substHelpText($query, $1)/eg;
    
    return $text;
}


sub substHelpText {
    
    my ($query, $variable) = @_;
    
    if ( $variable eq 'URL' )
    {
	return request->uri;
    }
    
    elsif ( $variable eq 'CT' )
    {
	return params->{ct};
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
	
	if ( defined $param_string && $param_string ne '' )
	{
	    return $1 . "\n\n" . $param_string;
	}
	else
	{
	    return '';
	}
    }
    
    elsif ( $variable =~ /^REQS(?::(.*))?/ )
    {
	my $req_string = $query->describeRequirements();
	
	if ( defined $req_string && $req_string ne '' )
	{
	    return $1 . "\n\n" . $req_string;
	}
	else
	{
	    return '';
	}
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


