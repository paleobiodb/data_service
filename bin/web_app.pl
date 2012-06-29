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

use DataQuery;
use TaxonQuery;
use TreeQuery;
use CollectionQuery;


set environment => 'development';
set apphandler => 'Debug';
set log => 'debug';

get '/taxa/list.:ct' => sub {
    
    my ($query, $result);
    my ($params) = params;
    
    $DB::single = 1;
    
    try {
	
	setContentType($params->{ct});
	
	$query = TaxonQuery->new(database());
	
	$query->setParameters($params);
	$query->fetchMultiple();
	$result = returnCompoundResult($query);
    }
	
    catch {

	$result = returnErrorResult($_);
    };
    
    return $result;
};


get '/taxa/all.:ct' => sub {

    forward '/taxa/list.' . params->{ct};
};


get '/taxa/details.:ct' => sub {

    $DB::single = 1;
    
    setContentType(params->{ct});
    
    my $query = TaxonQuery->new(database());
    
    $query->setParameters(scalar(params)) or return $query->reportError();
    
    $query->fetchSingle(params->{id}) or return $query->reportError();
    
    return $query->generateSingleResult();
};


get '/taxa/hierarchy.:ct' => sub {

    my ($query, $result);
    my ($params) = scalar(params);
    
    try {
	
	$DB::single = 1;
	
	setContentType($params->{ct});
	
	$query = TreeQuery->new(database());
	
	$query->setParameters($params);
	$query->fetchMultiple();
	$result = returnCompoundResult($query, 'streamResult');
    }
	
    catch {

	$result = returnErrorResult($query, $_);
    };
    
    return $result;
};


get '/taxa/:id.:ct' => sub {
    
    $DB::single = 1;
    return forward '/taxa/details.' . params->{ct}, { id => params->{id} };
};


get '/collections/list.:ct' => sub {

    $DB::single = 1;
    
    setContentType(params->{ct});
    
    my $query = CollectionQuery->new(database());
    
    $query->setParameters(scalar(params)) or return $query->reportError();
    
    $query->fetchMultiple() or return $query->reportError();
    
    return $query->generateCompoundResult( can_stream => server_supports_streaming );
};


get '/collections/details.:ct' => sub {

    $DB::single = 1;
    
    setContentType(params->{ct});

    my $query = CollectionQuery->new(database());
    
    $query->setParameters(scalar(params)) or return reportError($query);
    
    $query->fetchSingle(params->{id}) or return reportError($query);
    
    return $query->generateSingleResult();
};


# The following routines are used in the execution of the above routes.


# setContentType ( ct )
# 
# If the parameter is one of 'xml' or 'json', set content type of the response
# appropriately.  Otherwise, send back a status of 415 "Unknown Media Type".

sub setContentType {
    
    my ($ct) = @_;
    
    if ( $ct eq 'xml' )
    {
	content_type 'text/xml';
    }
    
    elsif ( $ct eq 'json' )
    {
	content_type 'application/json';
    }
    
    else
    {
	status(415);
	content_type 'text/plain';
	halt("Unknown Media Type: '$ct' is not supported by this application; use '.json' or '.xml' instead");
    }
}


# returnCompoundResult ( query, stream_method )
# 
# Call the proper method to generate a compound result from $query.  If the
# server supports streaming, and the method returns false (indicating that the
# result is large enough for streaming to make sense), we invoke the
# stream_data function from Dancer::Plugin::StreamData.  If the server does
# not support streaming, we just call the plain generateCompoundResult()
# method.

sub returnCompoundResult {

    my ($query, $stream_method) = @_;
    
    if ( server_supports_streaming )
    {
	$query->generateCompoundResult( can_stream => 1 ) or
	    stream_data( $query, $stream_method );
    }
    
    else
    {
	$query->generateCompoundResult();
    }
}


# returnErrorResult ( exception )
# 
# This method is called if an exception occurs during execution of a route
# sub.  If $exception is a blessed reference, then we pass it on (this is
# necessary for streaming to work properly).
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
    
    # If the exception is a blessed reference, pass it on through.  It's
    # a signal from one part of Dancer to another.
    
    if ( ref $exception )
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
    
    # If the message is 'help', substitute the help text.
    
    if ( $exception eq 'help' )
    {
	$exception = $query->helpText(params->{ct});
    }
    
    # Send back JSON results as an object with field 'error'.
    
    if ( params->{ct} eq 'json' )
    {
	status($code);
	return to_json({ 'error' => $exception }) . "\n";
    }
    
    # Everything else gets a plain text message.
    
    else
    {
	content_type 'text/plain';
	status($code);
	return $exception . "\n";
    }
}


sub debug_msg {
    
    debug(@_);
}

1;


dance;
