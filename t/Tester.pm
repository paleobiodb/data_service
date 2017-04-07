#
# Tester.pm: a class for running data service tests
# 


use strict;
use feature 'unicode_strings';
use feature 'fc';

use LWP::UserAgent;
use Text::CSV_XS;
use Encode qw(decode_utf8);
use JSON;

package Tester;

use Scalar::Util qw(looks_like_number reftype);
use Carp qw(croak);
use Test::More;
use base 'Exporter';

use namespace::clean;


our $LAST_BANNER = '';

our (@EXPORT_OK) = qw(cmp_sets_ok ok_checkpoint);

our ($DIAG_URLS);

# new ( server_name )
# 
# Create a new tester instance.  If no server is specified, the value of the
# environment variable PBDB_TEST_SERVER is used instead.  If this is not set,
# the default is "127.0.0.1:3000".

sub new {
    
    my ($class, $options) = @_;
    
    my $ua = LWP::UserAgent->new(agent => "PBDB Tester/0.1")
	or die "Could not create user agent: $!\n";
    
    $options ||= { };
    
    my $server = $options->{server} || $ENV{PBDB_TEST_SERVER} || '127.0.0.1:3000';
    my $prefix = $options->{prefix} || '';
    my $base_url = "http://$server";
    $base_url = "$base_url/$prefix" if $prefix ne '';
    my $timeout = $options->{timeout} || $ENV{PBDB_TEST_TIMEOUT} || 0;
    
    my $instance = { ua => $ua,
		     csv => Text::CSV_XS->new({ binary => 1 }),
		     json => JSON->new->utf8,
		     server => $server,
		     prefix => $prefix,
		     timeout => $timeout,
		     base_url => $base_url };
    
    bless $instance, $class;
    
    # See if we recognize any arguments to the test from which this method was called

    foreach my $arg ( @ARGV )
    {
	if ( $arg =~ /^--subtest=(.*)/si )
	{
	    $instance->{subtest_select} = $1;
	}
    }
    
    return $instance;
}


# set_url_check ( regex )
# 
# The specified regex will be applied to all URLs subsequently tested with
# this object, and an error will be thrown if it does not match.  This can be
# used to catch errors in the test suite, which may be introduced when code is
# copied from a test file intended for one data service version into a test
# file intended for another version.

# sub set_url_check {
    
#     my ($tester, $key, $regex) = @_;
    
#     $tester->{url_key} = $key;
#     $tester->{url_check} = $regex;
# }


# fetch_url ( path_and_args, message )
# 
# Try to carry out the operation given by path_and_args on the server
# associated with this Tester instance.  If it succeeds, return the
# HTTP::Response object that is returned.
# 
# If it fails, then call Test::More::fail with the specified message and
# return undefined.

sub fetch_url {

    my ($tester, $path_and_args, $message, $options) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    # If we haven't yet announced which server we are testing, do so now.
    
    unless ( $tester->{banner_displayed} )
    {
	my $message = $tester->{base_url};
	# $message .= " $tester->{url_key}" if $tester->{url_key};
	
	diag("TESTING SERVER: $message") unless $LAST_BANNER eq $message;
	
	$tester->{message_displayed} = 1;
	$LAST_BANNER = $message;
    }
    
    # If a regex has been supplied for checking URLs, then fail if it doesn't
    # match.
    
    # if ( my $re = $tester->{url_check} )
    # {
    # 	unless ( $path_and_args =~ $re )
    # 	{
    # 	    fail($message);
    # 	    diag("ERROR: URL DID NOT MATCH $tester->{url_key} <===");
    # 	}
    # }
    
    # Process the options.
    
    my $diag = $options->{no_diag} ? 0 : 1;
    
    my $timeout = $options->{timeout} || $tester->{timeout};
    
    # Create the full URL and execute a 'GET' request on the server being tested.
    
    my $url = $tester->make_url($path_and_args);
    
    if ( $DIAG_URLS || $ENV{PBDB_TEST_SHOW_URLS}    )
    {
	diag("Fetching: $url");
    }
    
    my $response;
    
    eval {
	
	local($SIG{ALRM}) = sub { die "timeout\n" };	# \n is required, see perldoc -f alarm
	alarm $timeout if $timeout;
	
	$response = $tester->{ua}->get($url);
	
	alarm 0;
    };
    
    my $errmsg = $@;
    
    alarm 0;	# in case the alarm wasn't canceled inside the eval above
    
    # If the timeout signal was given, then fail with the appropriate message.
    
    if ( $errmsg && $errmsg eq "timeout\n" )
    {
	fail($message);
	diag("    TIMED OUT WAITING FOR SERVER");
	
	 # Clear the saved response, and return false.
	
	$tester->{last_response} = undef;
	return;
    }
    
    # If we did not get a response at all, then fail with the specified
    # message and add appropriate diagnostics.  Return undefined (false) to
    # indicate failure.
    
    if ( ! defined $response )
    {
	# Fail the implicit test, and add an appropriate diagnostic.
	
	fail($message);
	
	$errmsg ||= "no response";
	diag("    $errmsg");
	
	# Clear the saved response, and return false.
	
	$tester->{last_response} = undef;
	return;
    }
    
    # If we get a response and the 'no_check' option was given, just extract
    # any error or warning messages that it contains and return the response
    # object.  Pass the implicit test and return the response object, no
    # matter what the result code is.  This can be used, for example, to test
    # that bad parameter values will result in a failure.
    
    elsif ( $options->{no_check} )
    {
	# Pass the implicit test, regardless of response status.
	
	pass($message);
	
	# Extract any error or warning messages without printing them.  Also
	# save the URL path and arguments used to generate this response.

	$tester->extract_errwarn($response, 0, $message);
	$response->{__URLPATH} = $path_and_args;
	
	# Save the response message for use by subsequent method calls, and
	# also return it as a true value.
	
	$tester->{last_response} = $response;
	return $response;
    }
    
    # Otherwise, if the request succeeded then pass the implicit test and
    # handle the result as follows.
    
    elsif ( $response->is_success )
    {
	pass($message);
	
	# Extract any error or warning messages that the response contained,
	# and print them unless directed not to.  Also save the URL path and
	# arguments.
	
	$tester->extract_errwarn($response, $diag, $message);
	$response->{__URLPATH} = $path_and_args;
	
	# Save the response message for use by subsequent method calls, and
	# also return it as a true value.
	
	$tester->{last_response} = $response;
	return $response;
    }
    
    # Otherwise, fail the implicit test with appropriate diagnostics.  Save
    # the response object for use by subsequent method calls, but return false
    # to indicate failure.
    
    else
    {
	fail($message);
	
	my $status = $response->status_line;
	diag("request was: $url") if $url;
	diag("status was: $status") if $status;
	
	# Extract any error or warning messages that the response contained,
	# and print them unless directed not to.  Also save the URL path and
	# arguments.
	
	$tester->extract_errwarn($response, $diag, $message);
	$response->{__URLPATH} = $path_and_args;
	
	# Save the response object but return false.
	
	$tester->{last_response} = $response;
	return;
    }
    
}


# fetch_nocheck ( path_and_args )
# 
# Works just like fetch_url, but does not check to make sure the response is a
# success. 

sub fetch_nocheck {

    my ($tester, $path_and_args, $message) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    return $tester->fetch_url($path_and_args, $message, { no_check => 1, no_diag => 1 });
    
    # my $url = $tester->make_url($path_and_args);
    
    # my $response;
    
    # eval {
    # 	$response = $tester->{ua}->get($url);
    # };
    
    # if ( $response )
    # {
    # 	$tester->extract_errwarn($response, 0, $message);
    # 	$response->{__URLPATH} = $path_and_args;
    # 	return $response;
    # }
    
    # else
    # {
    # 	fail($message);
    # 	diag("no response");
    # 	diag("request was: $url") if $url;
    # 	return;
    # }
}


# make_url ( path_and_args )
# 
# Put the specified path and arguments together with the base URL for this
# tester object to make a full HTTP URL.

sub make_url {
    
    my ($tester, $path_and_args) = @_;
    
    my $url = $tester->{base_url};
    $url .= '/' unless $path_and_args =~ qr{^/};
    $url .= $path_and_args;
    
    return $url;
}


my %TEXT_HEAD = ( 'elapsed time' => 'elapsed_time',
		  'records found' => 'records_found',
		  'records returned' => 'records_returned',
		  'record offset' => 'record_offset' );

# extract_errwarn ( response, diag )
# 
# Extract the error and/or warning messages from $response.  If $diag is true,
# then output them using diag().  In either case, set keys __WARNINGS and/or
# __ERRORS in the $response object.

sub extract_errwarn {
    
    my ($tester, $response, $diag, $message) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $body = $response->content;
    my (@errors, @warnings, %metadata);
    
    if ( $body =~ qr< ^ [{] >xs )
    {
	my $json = $response->{__JSON};
	
	unless ( $json )
	{
	    eval {
		$json = $response->{__JSON} = $tester->{json}->decode( $body );
	    };
	    
	    if ( $@ )
	    {
		fail("$message extracting content (json)");
		diag("    " . $@);
		return;
	    }
	}
	
	if ( ref $json->{errors} eq 'ARRAY' )
	{
	    @errors = @{$json->{errors}};
	}
	
	if ( ref $json->{warnings} eq 'ARRAY' )
	{
	    @warnings = @{$json->{warnings}};
	}
	
	foreach my $key ( qw(elapsed_time records_found records_returned) )
	{
	    $metadata{$key} = $json->{$key} if defined $json->{$key};
	}
    }
    
    elsif ( $body =~ qr{ ^ [<] }xs )
    {
	my @lines = split qr{[\r\n]+}, $body;
	my $section = 'errors';
	
	foreach my $line (@lines)
	{
	    if ( $line =~ qr{ <li> (.*?) </li> }xs )
	    {
		if ( $section eq 'errors' )
		{
		    push @errors, $1;
		}
		
		elsif ( $section eq 'warnings' )
		{
		    push @warnings, $1;
		}
	    }
	    
	    elsif ( $line =~ qr{ <h.>Warnings }xs )
	    {
		$section = 'warnings';
	    }
	}
	
    }
    
    else	# assume text format response
    {
	my @lines = split qr{[\r\n]+}, $body, 100;
	my @fields;
	
	my $format = $lines[0] =~ qr{^"} ? 'csv' : 'tsv';
	
	foreach my $line (@lines)
	{
	    if ( $format eq 'csv' )
	    {
		$tester->{csv}->parse($line);
		@fields = $tester->{csv}->fields;
		
		if ( $tester->{csv}->error_diag() )
		{
		    fail("$message parsing header (csv)");
		    diag("    " . $tester->{csv}->error_diag());
		}
	    }
	    
	    else
	    {
		@fields = split(qr{\t}, $line);
	    }
	    
	    if ( lc $fields[0] eq 'warning:' )
	    {
		push @warnings, $fields[1];
	    }
	    
	    elsif ( my $item = $TEXT_HEAD{lc $fields[0]} )
	    {
		$metadata{$item} = $fields[1];
	    }
	    
	    elsif ( lc $fields[0] eq 'records:' )
	    {
		last;
	    }
	}
    }
    
    $response->{__ERRORS} = \@errors;
    $response->{__WARNINGS} = \@warnings;
    $response->{__METADATA} = \%metadata;
    
    if ( $diag && @errors )
    {
	foreach my $w ( @errors )
	{
	    diag("ERROR: $w");
	}
    }
    
    if ( $diag && @warnings )
    {
	foreach my $w ( @warnings )
	{
	    diag("WARNING: $w");
	}
    }
}


# get_content_type ( response )
# 
# Return the content type from the specified response object, or from the last
# response fetched if no response object was given.

sub get_content_type {
    
    my ($tester, $response) = @_;

    $response ||= $tester->{last_response};
    
    # If an HTTP response object was given, extract the content type from that.
    
    if ( ref $response && $response->isa('HTTP::Response') )
    {
	return $response->header("Content-Type");
    }
    
    # If no object was provided (or a scalar argument such as 'last'), extract
    # the content type from the last response fetched or return undefined if
    # there isn't one.
    
    elsif ( ! defined $response || lc $response eq 'last' )
    {
	return $tester->{last_response}->header("Content-Type")
	    if defined $tester->{last_response};
	return;
    }
    
    # If some other kind of object was passed to use, throw an exception.
    
    else
    {
	my $type = ref $response;
	croak "First argument must be an HTTP response object, was type '$type'";
    }
}


# ok_content_type ( response, type, charset, message )
# 
# Test whether the given response has the given content type.  Fail if it does
# not.  Ignore character set unless it is specified.

sub ok_content_type {
    
    my $tester = shift @_;
    my $response;
    $response = shift @_ if ref $_[0] && $_[0]->isa('HTTP::Response');
    my ($type, $charset, $message) = @_;
    
    croak "No message specified" unless $message && ! ref $message;
    
    my $ct_header = $tester->get_content_type($response);
    
    unless ( defined $ct_header )
    {
	fail($message);
	diag("    No content type was found");
	return;
    }
    
    if ( ! ok( $ct_header =~ qr{ ^ $type (?: ; | $ ) }xs, $message ) )
    {
	diag "     got: $ct_header";
	diag "expected: $type";
	return;
    }
    
    if ( $charset && ! ok( $ct_header =~ qr{ charset=$charset }xsi, $message) )
    {
	diag "     got: $ct_header";
	diag "expected: charset=$charset";
	return;
    }
    
    return 1;
}


# get_response_code ( response )
# 
# Return the content type from the specified response object, or from the last
# response fetched if no response object was given.

sub get_response_code {

    my ($tester, $response) = @_;
    
    # If an HTTP response object was given, extract the response code from that.
    
    if ( ref $response && $response->isa('HTTP::Response') )
    {
	return $response->code;
    }
    
    # If no object was provided (or a scalar argument such as 'last'), extract
    # the response code from the last response fetched or return undefined if
    # there isn't one.
    
    elsif ( ! defined $response || lc $response eq 'last' )
    {
	return $tester->{last_response}->code
	    if defined $tester->{last_response};
	return;
    }
    
    # If some other kind of object was passed to use, throw an exception.
    
    else
    {
	my $type = ref $response;
	croak "First argument must be an HTTP response object, was type '$type'";
    }
}


# ok_response_code ( response, code, message )
# 
# Test whether the given response has one of the given codes.  Codes must be
# given as a string which may contain a comma-separated list.  Fail if the
# response code is not one of those specified.

sub ok_response_code {
    
    my $tester = shift @_;
    my $response;
    $response = shift @_ if ref $_[0] && $_[0]->isa('HTTP::Response');
    my ($code, $message) = @_;
    
    croak "No message specified" unless $message && ! ref $message;
    
    my %acceptable = map { $_ => 1 } split /\s*,\s*/, $code;
    
    my $rc = $tester->get_response_code($response);
    
    unless ( defined $rc )
    {
	fail($message);
	diag("    No response code was found");
	return;
    }
    
    unless ( ok( $acceptable{$rc}, $message ) )
    {
	my $report = $code;
	$report =~ s/\s*,\s*/, /g;
	diag( "    got: $rc" );
	diag( "    expected: $report" );
    }
}


# get_errors ( response )
# 
# Return the errors, if any, from the given response.  If no response is
# given, use the last one fetched.  We assume that these responses were
# fetched using the 'fetch_url' method of this class, which automatically
# extracts any error and warning messages from the response body.

sub get_errors {

    my ($tester, $response) = @_;

    $response ||= $tester->{last_response};
    
    # If an HTTP response object was given, return any errors from that.
    
    if ( ref $response && $response->isa('HTTP::Response') )
    {
	return @{$response->{__ERRORS}} if $response->{__ERRORS};
	return;	# return an empty list if no errors were found
    }
    
    # If no object was provided (or a scalar argument such as 'last'), extract
    # the response code from the last response fetched or return undefined if
    # there isn't one.
    
    elsif ( ! defined $response || lc $response eq 'last' )
    {
	if ( $response = $tester->{last_response} )
	{
	    return @{$response->{__ERRORS}} if $response->{__ERRORS};
	}
	return; # return an empty list if no errors were found, or if no
                # previously fetched response was found.
    }
    
    # If some other kind of object was passed to use, throw an exception.
    
    else
    {
	my $type = ref $response;
	croak "First argument must be an HTTP response object, was type '$type'";
    }
}


# get_warnings ( response )
# 
# Return the warnings, if any, from the given response.  If no response is
# given, use the last one fetched.  We assume that these responses were
# fetched using the 'fetch_url' method of this class, which automatically
# extracts any error and warning messages from the response body.


sub get_warnings {
    
    my ($tester, $response) = @_;
    
    # If an HTTP response object was given, return any errors from that.
    
    if ( ref $response eq 'HTTP::Response' )
    {
	return @{$response->{__WARNINGS}} if $response->{__WARNINGS};
	return;	# return an empty list if no errors were found
    }
    
    # If no object was provided (or a scalar argument such as 'last'), extract
    # the response code from the last response fetched or return undefined if
    # there isn't one.
    
    elsif ( ! defined $response || lc $response eq 'last' )
    {
	if ( $response = $tester->{last_response} )
	{
	    return @{$response->{__WARNINGS}} if $response->{__WARNINGS};
	}
	return; # return an empty list if no errors were found, or if no
                # previously fetched response was found.
    }
    
    # If some other kind of object was passed to use, throw an exception.
    
    else
    {
	my $type = ref $response;
	croak "First argument must be an HTTP response object, was type '$type'";
    }    
}


# has_error_like ( response, regex )
# 
# Return true if one of the errors in $response matches $regex, false otherwise.

sub has_error_like {
    
    my $tester = shift @_;
    my $response;
    $response = shift @_ if ref $_[0] && $_[0]->isa('HTTP::Response');
    my ($regex) = @_;
    
    croak "You must specify a regular expression" unless ref $regex eq 'Regexp';
    
    foreach my $e ( $tester->get_errors($response) )
    {
	return 1 if $e =~ $regex;
    }
    
    return;
}


# has_warning_like ( response, regex )
# 
# Return true if one of the warnings in $response matches $regex, false otherwise.

sub has_warning_like {
    
    my $tester = shift @_;
    my $response;
    $response = shift @_ if ref $_[0] && $_[0]->isa('HTTP::Response');
    my ($regex) = @_;
    
    croak "You must specify a regular expression" unless ref $regex eq 'Regexp';
    
    foreach my $w ( $tester->get_warnings($response) )
    {
	return 1 if $w =~ $regex;
    }
    
    return;
}


sub ok_error_like {
    
    my $tester = shift @_;
    my $response;
    $response = shift @_ if ref $_[0] && $_[0]->isa('HTTP::Response');
    my ($regex, $message) = @_;
    
    croak "You must specify a regular expression" unless ref $regex eq 'Regexp';
    croak "You must specify a message" unless $message;
    
    $response ||= $tester->{last_response};
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    unless ( reftype $response eq 'HASH' && ref $response->{__ERRORS} eq 'ARRAY' )
    {
	fail($message);
	diag("    No error messages were extracted");
	return;
    }
    
    unless ( ok( $tester->has_error_like($response, $regex), $message ) )
    {
	diag("  expected: $regex");
	$tester->diag_errors($response);
    }
}


sub diag_errors {
    
    my ($tester, $response) = @_;
    
    my @errors = $tester->get_errors($response);
    
    unless ( @errors )
    {
	diag("  got no error messages");
    }
    
    foreach my $e ( @errors )
    {
	diag("  got error: $e");
    }
}
    

sub ok_warning_like {

    my $tester = shift @_;
    my $response;
    $response = shift @_ if ref $_[0] && $_[0]->isa('HTTP::Response');
    my ($regex, $message) = @_;
    
    croak "You must specify a regular expression" unless ref $regex eq 'Regexp';
    croak "You must specify a message" unless $message;
    
    $response ||= $tester->{last_response};
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    unless ( reftype $response eq 'HASH' && ref $response->{__WARNINGS} eq 'ARRAY' )
    {
	fail($message);
	diag("    No warnings were extracted");
	return;
    }
    
    unless ( ok( $tester->has_warning_like($response, $regex), $message ) )
    {
	diag("  expected: $regex");
	$tester->diag_warnings($response);
    }
}
    

sub diag_warnings {
    
    my ($tester, $response) = @_;
    
    my @warnings= $tester->get_warnings($response);
    
    unless ( @warnings )
    {
	diag("  got no warning messages");
    }
    
    foreach my $w ( @warnings )
    {
	diag("  got warning: $w");
    }
}


# cmp_ok_errors ( response, operation, compvalue, message )
# 
# Run 'cmp_ok' on the number of errors in the specified response.  If no
# response is given, use the last one fetched.

sub cmp_ok_errors {
    
    my $tester = shift @_;
    my $response;
    $response = shift @_ if ref $_[0] && $_[0]->isa('HTTP::Response');
    my ($operation, $compvalue, $message) = @_;
    
    croak "You must specify a message" unless defined $message;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;

    $response ||= $tester->{last_response};
    
    unless ( reftype $response eq 'HASH' && ref $response->{__ERRORS} eq 'ARRAY' )
    {
	fail($message);
	diag("    No error messages were extracted");
	return;
    }
    
    cmp_ok( @{$response->{__ERRORS}}, $operation, $compvalue, $message );
}


# cmp_ok_warnings ( response, operation, compvalue, message )
# 
# Run 'cmp_ok' on the number of warnings in the specified response.  If no
# response is given, use the last one fetched.

sub cmp_ok_warnings {
    
    my $tester = shift @_;
    my $response;
    $response = shift @_ if ref $_[0] && $_[0]->isa('HTTP::Response');
    my ($operation, $compvalue, $message) = @_;
    
    croak "You must specify a message" unless defined $message;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $response ||= $tester->{last_response};
    
    unless ( reftype $response eq 'HASH' && ref $response->{__WARNINGS} eq 'ARRAY' )
    {
	fail($message);
	diag("    No warnings were extracted");
	return;
    }
    
    cmp_ok( @{$response->{__WARNINGS}}, $operation, $compvalue, $message );
}


# get_meta ( response, field )
# 
# Return the value of the specified metadata field from the specified
# response, or undefined if it doesn't exist or has no value.  If no response
# is given, use the last one fetched.

sub get_meta {
    
    my ($tester, $response, $field) = @_;
    
    # If an HTTP response object was given, look up the metadata directly.
    # This assumes that &extract_errwarn was already called on this response.
    
    if ( ref $response eq 'HTTP::Response' )
    {
	return $response->{__METADATA}{$field} if defined $response->{__METADATA}{$field};
	return;	# return undefined if no such field was found.
    }
    
    # If no object was provided (or a scalar argument such as 'last'), extract
    # the response code from the last response fetched or return undefined if
    # there isn't one.
    
    elsif ( ! defined $response || lc $response eq 'last' )
    {
	if ( $response = $tester->{last_response} )
	{
	    return $response->{__METADATA}{$field} if defined $response->{__METADATA}{$field};
	}
	
	return; # return undefined if no such field was found, or if no
                # previously fetched response was found.
    }
    
    # If some other kind of object was passed to use, throw an exception.
    
    else
    {
	my $type = ref $response;
	croak "First argument must be an HTTP response object, was type '$type'";
    }
}


# cmp_ok_meta ( response, field, operation, compvalue, message )
# 
# Test that the specified metadata value from the given response compares properly with
# the specified value according to the specified operation.

sub cmp_ok_meta {

    my $tester = shift @_;
    my $response;
    $response = shift @_ if ref $_[0] && $_[0]->isa('HTTP::Response');
    my ($field, $operation, $compvalue, $message) = @_;
    
    croak "You must specify a message" unless defined $message;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $response ||= $tester->{last_response};
    
    my $value = $tester->get_meta($response, $field);
    
    cmp_ok($value, $operation, $compvalue, $message);
}


# ok_no_records ( response, message )
# 
# Test that the given response contains an empty record set.

sub ok_no_records {
    
    my $tester = shift @_;
    my $response;
    $response = shift @_ if ref $_[0] && $_[0]->isa('HTTP::Response');
    my ($message) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $extract_from;
    
    if ( ref $response eq 'HTTP::Response' )
    {
	$extract_from = $response;
    }
    
    elsif ( ! defined $response || lc $response eq 'last' )
    {
	$extract_from = $tester->{last_response};
    }
    
    else
    {
	my $type = ref $response;
	croak "First argument must be an HTTP response object, was type '$type'";
    }
    
    my @r = $tester->extract_records( $extract_from, $message, { no_records_ok => 1 } );
    
    my $count = scalar(@r);
    
    ok( $count == 0, $message ) ||
	diag( "    got: $count records" );
}


# get_metadata ( response )
# 
# Return any metadata that was included with the response. $$$

# sub get_metadata {
    
#     my ($tester, $response) = @_;
    
#     croak "First argument must be a response object" unless ref($response) =~ /^HTTP/;
    
#     return $response->{__METADATA} if ref $response->{__METADATA} eq 'HASH';
#     return {};
# }


# fetch_records ( path_and_args, options, message )
# 
# Call fetch_url and then extract_records.

sub fetch_records {
    
    my ($tester, $path_and_args, $message, $options) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $options ||= { };
    
    my $response = $tester->fetch_url($path_and_args, "$message: response OK", $options);
    return unless $response;
    
    return $tester->extract_records($response, "$message: extract records", $options);
}


# fetch_record_values ( path_and_args, field, message, options )
# 
# Call fetch_url, extract_records, and then extract the values of the named
# field. Return a hash whose keys are the values found, or which contains the
# key 'NO_RECORDS' if no records were found.

sub fetch_record_values {
    
    my ($tester, $path_and_args, $field, $message, $options) = @_;
    
    croak "You must specify a message" unless $message;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $options ||= { };
    
    my $response = $tester->fetch_url($path_and_args, "$message response OK");
    return unless $response;
    
    my (@r) = $tester->extract_records($response, "$message: extract records", $options);
    
    return ( NO_RECORDS => 1 ) unless @r;
    
    return $tester->extract_values( \@r, $field );
    
    # foreach my $r (@r)
    # {
    # 	$found{$r->{$field}} = 1 if defined $r->{$field} && $r->{$field} ne '';
    # }
    
    # return %found;
}


# extract_records ( response, message, $options )
# 
# Decode the specified response.  If this succeeds, and if the response
# contains at least one record, return a list of all the record hashes.
# 
# Otherwise, call Test::More::fail with the specified message and return the
# empty list.

sub extract_records {

    my ($tester, $response, $message, $options) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    unless ( ref $response )
    {
	fail("$message: extract records");
	diag("empty response");
	return;
    }
    
    my $body = $response->content;
    
    # If the body starts with '{', then this is a JSON response.
    
    if ( $body =~ qr< ^ { >xs )
    {
	return $tester->extract_records_json($response, $message, $options);
    }
    
    # If the body contains 'TY - ' within the first 2000 characters, then this is an RIS
    # response.
    
    elsif ( substr($body,0,2000) =~ qr<TY  - > )
    {
	return $tester->extract_records_ris($response, $message, $options);
    }
    
    # Otherwise, assume it is a tsv or csv (including .txt) response.
    
    else
    {
	return $tester->extract_records_text($response, $message, $options);
    }
}


# extract_records_json ( response, message )
# 
# Decode the specified JSON response.  If it succeeds, and if the response
# contains at least one record, return a list of all the record hashes.

sub extract_records_json {
    
    my ($tester, $response, $message, $options) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $options ||= { };
    
    croak "third argument must be an options hash"
	unless ref $options eq 'HASH';
    
    my $body = $response->{__JSON};
    
    unless ( $body )
    {
	eval {
	    $body = $response->{__JSON} = $tester->{json}->decode( $response->content );
	};
	
	if ( $@ )
	{
	    fail("$message (json)");
	    diag("    " . $@);
	    return;
	}
    }
    
    if ( ref $body->{records} eq 'ARRAY' && $options->{no_records_ok} )
    {
	return @{$body->{records}};
    }
    
    elsif ( ref $body->{records} eq 'ARRAY' && @{$body->{records}} && ref $body->{records}[0] eq 'HASH' )
    {
	return @{$body->{records}};
    }
    
    else
    {
	fail($message);
	diag('no records found');
	if ( $response->{__URLPATH} )
	{
	    my $url = $tester->make_url($response->{__URLPATH});
	    diag("request was: $url");
	}
	return;
    }
}


# extract_records_text ( response, message, options )
# 
# Decode the specified text response.  If it succeeds, and if the format
# contains at least one record, return a list of all the record lines.
# 
# Otherwise, call Test::More::Fail with the specified message and return the
# empty list.
# 
# The parameter $options->{type} tells this routine what to expect in terms of how the
# data is organized:
# 
# datainfo	Expect full header material (datainfo/showsource)
# header	Expect a single header line
# records	Expect no header at all

sub extract_records_text {

    my ($tester, $response, $message, $options) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $options ||= { };
    
    # Check the arguments.
    
    croak "third argument must be an options hash"
	unless ref $options eq 'HASH';
    
    my $raw_data = $response->content || '';
    my @records;
    
    my $section = $options->{parse} || 'init';
    my $limit = $options->{limit};
    
    my (@fields, @values);
    my $count = 0;
    my $format;
    
    my @lines = split( qr{[\n\r]+}, $raw_data );
    my (@cells, @fields, $line_no);
    
    # Determine the format of the data.
    
    if ( $options->{format} )
    {
	$format = $options->{format};
    }
    
    elsif ( $lines[0] =~ qr{^"} )
    {
	$format = 'csv';
    }
    
    elsif ( $lines[0] =~ qr{\t} )
    {
	$format = 'tsv';
    }
    
    else
    {
	fail("$message could not determine output format");
	return;
    }
    
    # Now go through the lines one by one.
    
 LINE:
    foreach my $line (@lines)
    {
	$line_no++;
	
	# Extract a list of values from each line.
	
	if ( $format eq 'csv' )
	{
	    $tester->{csv}->parse($line);
	    @cells = $tester->{csv}->fields;
	    
	    if ( $tester->{csv}->error_diag() )
	    {
		fail("$message parse error at line $line_no (csv)");
		diag("    " . $tester->{csv}->error_diag());
	    }
	}
	
	else
	{
	    @cells = split(qr{\t}, $line, -1);
	}
	
	# If we are just starting out, we need to figure out where the records start.
	
	if ( $section eq 'init' )
	{
	    # If the first line starts with "Records:" then the next line will be the header.
	    
	    if ( lc $cells[0] eq 'records:' )
	    {
		$section = 'header';
		next LINE;
	    }
	    
	    # If the first line contains two values, or the first value ends in a colon, then we
	    # are in the metadata section.
	    
	    elsif ( @cells == 2 || $cells[0] =~ qr{[:]$} )
	    {
		$section = 'metadata';
		next LINE;
	    }
	    
	    # Otherwise, we have to assume that the first line is the header."
	    
	    else
	    {
		$section = 'header';
	    }
	}
	
	# If we are in the 'metadata' section, then look for a line starting with "Records:".
	
	if ( $section eq 'metadata' )
	{
	    $section = 'header' if lc $cells[0] eq 'records:';
	    next LINE;
	}
	
	# If we have reached the header line, then capture all of the values as the field names.
	# All subsequent lines will be records.
	
	if ( $section eq 'header' )
	{
	    @fields = @cells;
	    $section = 'records';
	    next LINE;
	}
	
	if ( $section eq 'records' )
	{
	    last LINE if uc $cells[0] =~ / ^ THIS \s REQUEST \s .* RECORDS $ /x;
	    
	    my $r;
	    
	    foreach my $i ( 0..$#fields )
	    {
		$r->{$fields[$i]} ||= $cells[$i];
	    }
	    
	    push @records, $r;
	    $count++;
	    last LINE if $limit && $count > $limit;
	    next LINE; # otherwise
	}
	
	else
	{
	    fail("$message parse error at line $line_no ($format)");
	    return;
	}
    }
    
    if ( @records || $options->{no_records_ok} )
    {
	return @records;
    }
    
    else
    {
	fail($message);
	diag('no records found');
	if ( $response->{__URLPATH} )
	{
	    my $url = $tester->make_url($response->{__URLPATH});
	    diag("request was: $url");
	}
	return;
    }
}


# extract_records_ris ( response, message )
# 
# Decode the specified RIS-format response.  Each entry is take to be a single
# record, and is returned as a hash with the field codes as the hash keys.  If
# it decoding succeeds, and if at least one record is found, return a list of all
# the records.
# 
# Otherwise, call Test::More::Fail with the specified message and return the
# empty list.

sub extract_records_ris {

    my ($tester, $response, $message, $options) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $options ||= { };
    
    # Check the arguments.
    
    croak "third argument must be an options hash"
	unless ref $options eq 'HASH';
    
    my $raw_data = $response->content || '';
    my @records;
    
    my $section = $options->{parse} || 'init';
    my $limit = $options->{limit};
    
    my (@fields, @values);
    my $count = 0;
    my $format;
    
    my @lines = split( qr{\r\n}, $raw_data );
    my $record = { };
    my $line_no;
    
    # Now go through the lines one by one.
    
 LINE:
    foreach my $line (@lines)
    {
	$line_no++;
	
	# If we are just starting out, we need to figure out where the records start.  Each record
	# starts with a 'TY' line, so until we find the first one we process header stuff.
	
	if ( $section eq 'init' && $line !~ /^TY/ )
	{
	    # Since we are just extracting records, we can skip the header.
	    
	    next LINE
	}
	
	elsif ( $section eq 'init' )
	{
	    $section = 'metadata-maybe';
	}
	
	# If we get here, we are processing records.  But the first few may be metadata instead of
	# actual records.  Whenever we find a blank line, that means the end of a record.  So
	# process it, and check to see if it is metadata.
	
	if ( $line eq '' )
	{
	    # If this is a metadata record, discard it.
	    
	    if ( $section eq 'metadata-maybe' && $record->{TY} eq 'GEN' && 
		 (lc $record->{TI} eq 'data source' || 
		  lc $record->{TI} eq 'documentation' ||
		  lc $record->{TI} eq 'record counts') )
	    {
		next;
	    }
	    
	    # Otherwise, save the record and change the state to 'records'.
	    
	    else
	    {
		push @records, $record;
		$record = { };
		$section = 'records';
	    }
	}
	
	# Otherwise, parse the field and value and add them to the current record.
	
	else
	{
	    if ( $line =~ qr{ ^ (\w\w) \s\s - \s (.*) }x )
	    {
		my $key = $1;
		my $value = Encode::decode_utf8($2);

		if ( defined $record->{$key} )
		{
		    $record->{$key} .= "\t$value";
		}

		else
		{
		    $record->{$key} = $value;
		}
	    }
	    
	    else
	    {
		fail("$message parse error at line $line_no");
		my $url = $tester->make_url($response->{__URLPATH});
		diag("request was: $url");
		return;
	    }
	}
    }
    
    # If there is an unclosed record at the end, add it to the result.
    
    if ( ref $record eq 'HASH' && keys %$record )
    {
	push @records, $record;
    }
    
    # Now, if we have found some records (or if none are okay) then return them.  Otherwise, this
    # operation fails.
    
    if ( @records || $options->{no_records_ok} )
    {
	return @records;
    }
    
    else
    {
	fail($message);
	diag('no records found');
	if ( $response->{__URLPATH} )
	{
	    my $url = $tester->make_url($response->{__URLPATH});
	    diag("request was: $url");
	}
	return;
    }
}


# extract_info ( resonse, message )
# 
# Extract the datainfo/showsource header from a response.

sub extract_info {

    my ($tester, $response, $message) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    unless ( ref $response )
    {
	fail($message);
	diag("empty response");
	return;
    }
    
    my $body = $response->content;
    
    # If the body starts with '{', then this is a JSON response.
    
    if ( $body =~ qr< ^ { >xs )
    {
	return $tester->extract_info_json($response, $message);
    }
    
    # Otherwise, assume it is a text response.
    
    else
    {
	return $tester->extract_info_text($response, $message);
    }
}


# extract_info_json ( response, message )

sub extract_info_json {
    
    my ($tester, $response, $message) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $body = $response->{__JSON};
    
    unless ( $body )
    {
	eval {
	    $body = $response->{__JSON} = $tester->{json}->decode( $response->content );
	};
	
	if ( $@ )
	{
	    fail("$message (json)");
	    diag("    " . $@);
	    return;
	}
    }
    
    return $body;
}


# extract_info_text ( response, message )

sub extract_info_text {

    my ($tester, $response, $message) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $raw_data = $response->content || '';
    my $info = {};
    
    my $section = 'info';
    my $format = $raw_data =~ qr{^"} ? 'csv' : 'tsv';
    
 LINE:
    while ( $raw_data =~ qr{ ^ (.*?) (?: \r\n? | \n ) (.*) }xsi )
    {
	$raw_data = $2;
	my $line = $1;
	
	if ( $line =~ qr{^"Parameters:"}xsi )
	{
	    $section = 'parameters';
	    next LINE;
	}
	
	elsif ( $section eq 'parameters' && $line !~ qr{^""} )
	{
	    $section = 'info';
	}
	
	elsif ( $line =~ qr{^"Records:"} )
	{
	    last LINE;
	}
	
	if ( $section eq 'parameters' )
	{
	    if ( $format eq 'csv' )
	    {
		$tester->{csv}->parse($line);
		my ($dummy, $param, $value) = $tester->{csv}->fields;
		$info->{parameters}{$param} = $value;
		
		if ( $tester->{csv}->error_diag() )
		{
		    fail("$message (csv)");
		    diag("    " . $tester->{csv}->error_diag());
		}
	    }
	    
	    else
	    {
		my ($dummy, $param, $value) = split(qr{\t}, $line);
		$info->{parameters}{$param} = $value;
	    }
	    
	    next LINE;
	}
	
	elsif ( $section eq 'info' )
	{
	    if ( $format eq 'csv' )
	    {
		$tester->{csv}->parse($line);
		my ($field, $value) = $tester->{csv}->fields;
		$field =~ s/:$//;	# take off final ':', if one is found
		$info->{$field} = $value;
		
		if ( $tester->{csv}->error_diag() )
		{
		    fail("$message (csv)");
		    diag("    " . $tester->{csv}->error_diag());
		}
	    }
	    
	    else
	    {
		my ($field, $value) = split(qr{\t}, $line);
		$field =~ s/:$//;	# take off final ':', if one is found
		$info->{$field} = $value;
	    }
	}
    }
    
    return $info;
}


sub scan_records {
    
    my ($tester, $response, $field, $message) = @_;
    
    my @r = $tester->extract_records($response, $message);
    
    return $tester->extract_values(\@r, $field);
}


sub extract_values {

    my ($tester, $records_ref, $field, $options) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    croak "first argument must be an arrayref"
	unless ref $records_ref eq 'ARRAY';
    
    $options ||= { };
    
    return ( NO_RECORDS => 1 ) unless @$records_ref;
    
    my %found;
    
    my @field_list = ref $field eq 'ARRAY' ? @$field : $field;

 RECORD:
    foreach my $r (@$records_ref)
    {
	foreach my $f ( @field_list )
	{
	    if ( defined $r->{$f} && $r->{$f} ne '' )
	    {
		$found{$r->{$f}} = 1;
		next RECORD;
	    }
	}
	
	if ( $options->{count_empty} )
	{
	    $found{''} = 1;
	}
    }
    
    unless ( keys %found )
    {
	fail("no values found") unless $options->{no_values_ok};
    }
    
    return %found;
}


sub count_values {

    my ($tester, $records_ref, $field, $options) = @_;
    
    croak "first argument must be an arrayref"
	unless ref $records_ref eq 'ARRAY';
    
    $options ||= { };
    
    return ( NO_RECORDS => 1 ) unless @$records_ref;
    
    my %found;
    
    foreach my $r (@$records_ref)
    {
	if ( defined $r->{$field} && $r->{$field} ne '' )
	{
	    $found{$r->{$field}}++;
	}
	
	elsif ( $options->{count_empty} )
	{
	    $found{''}++;
	}
    }
    
    fail("found no values for '$field'") unless keys %found;
    
    return %found;
}


sub find_max {
    
    my ($tester, $values_ref) = @_;
    
    croak "First argument must be a hashref" unless ref $values_ref eq 'HASH';
    
    my ($max_value, $max_count);
    
    foreach my $k ( keys %$values_ref )
    {
	if ( ! defined $max_count || $values_ref->{$k} > $max_count )
	{
	    $max_value = $k;
	    $max_count = $values_ref->{$k};
	}
    }
    
    return wantarray ? ($max_value, $max_count) : $max_value;
}


sub find_prevalent {
    
    my ($tester, $values_ref, $field) = @_;
    
    croak "First argument must be a listref or hashref"
	unless ref $values_ref eq 'ARRAY' or ref $values_ref eq 'HASH';
    
    if ( ref $values_ref eq 'ARRAY' )
    {
	croak "Second argument must be a field name"
	    unless defined $field && $field eq '';
	
	my %values = $tester->count_values( $values_ref, $field );
	
	return sort { $values{$b} <=> $values{$a} } keys %values;
    }
    
    else
    {
	return sort { $values_ref->{$b} <=> $values_ref->{$a} } keys %$values_ref;
    }
}


sub cmp_sets_ok {
    
    shift if ref $_[0] && ( ref $_[0] eq 'Tester' || $_[0]->isa('Tester') );
    
    my ($ref1, $op, $ref2, $message) = @_;
    
    croak "You must specify a message" unless defined $message && $message ne '';
    croak "First argument must be a hashref" unless ref $ref1 eq 'HASH';
    croak "Third argument must be a hashref" unless ref $ref2 eq 'HASH';
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    # First check to see if we can find a key in the left set that is not in the right, or vice
    # versa.
    
    my ($found_left, $found_right);
    my ($left_only_count, $right_only_count);
    
    foreach my $key ( keys %$ref1 )
    {
	unless ( exists $ref2->{$key} )
	{
	    $found_left //= $key;
	    $left_only_count++;
	}
    }
    
    foreach my $key ( keys %$ref2 )
    {
	unless ( exists $ref1->{$key} )
	{
	    $found_right //= $key;
	    $right_only_count++;
	}
    }
    
    # Then use this info to pass or fail tests accordingly.
    
    my $left_msg = $left_only_count && $left_only_count > 1 ? " ($left_only_count total)" : "";
    my $right_msg = $right_only_count && $right_only_count > 1 ? " ($right_only_count total)" : "";
    
    if ( $op eq '==' )
    {
	fail($message) if defined $found_left || defined $found_right;
	diag("    Found '$found_left' in left, not in right$left_msg") if defined $found_left;
	diag("    Found '$found_right' in right, not in left$right_msg") if defined $found_right;
	pass($message) unless defined $found_left || defined $found_right;
    }
    
    elsif ( $op eq '<=' )
    {
	fail($message) if defined $found_left;
	diag("    Found '$found_left' in left, not in right$left_msg") if defined $found_left; 
	pass($message) unless defined $found_left;
    }
    
    elsif ( $op eq '<' )
    {
	fail($message) if defined $found_left || ! defined $found_right;
	diag("    Found '$found_left' in left, not in right$left_msg") if defined $found_left;
	diag("    Left is not a proper subset of right") unless defined $found_right;
	pass($message) if ! defined $found_left && defined $found_right;
    }
    
    elsif ( $op eq '>=' )
    {
	fail($message) if defined $found_right;
	diag("    Found '$found_right' in right, not in left$right_msg") if defined $found_right; 
	pass($message) unless defined $found_right;
    }
    
    elsif ( $op eq '>' )
    {
	fail($message) if defined $found_right || ! defined $found_left;
	diag("    Found '$found_right' in right, not in left$right_msg") if defined $found_right;
	diag("    Left is not a proper superset of right") unless defined $found_left;
	pass($message) if ! defined $found_right && defined $found_left;
    }
    
    elsif ( $op eq '!=' )
    {
	fail($message) unless defined $found_left || defined $found_right;
	diag("    Left is equal to right") unless defined $found_left || defined $found_right;
	pass($message) if defined $found_left || defined $found_right;
    }
    
    else
    {
	croak "The second argument must be a numeric comparison operator, was '$op'";
    }
}


sub check_values {
    
    my ($tester, $records_ref, $field, $pattern, $message) = @_;
    
    croak "first argument must be arrayref" unless ref $records_ref eq 'ARRAY';
    croak "third argument must be pattern or scalar" unless ref $pattern eq 'Regexp' || ! ref $pattern;
    croak "no message specified" unless $message;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my ($found_value);
    
    foreach my $r (@$records_ref)
    {
	my $v = $r->{$field};
	
	if ( defined $v && $v ne '' )
	{
	    $found_value = 1;
	    
	    if ( ref $pattern )
	    {
		unless ( $v =~ $pattern )
		{
		    fail("$message found '$v' not matching $pattern");
		    return;
		}
	    }
	    
	    else
	    {
		unless ( $v eq $pattern )
		{
		    fail("$message found '$v' not matching '$pattern'");
		    return;
		}
	    }
	}
	
	else
	{
	    fail("$message found '' not matching '$pattern'");
	    return
	}
    }
    
    ok( $found_value, "$message - no values found" );
}


sub cmp_values {
    
    my ($tester, $records_ref, $field, $cmp, $value, $message) = @_;
    
    croak "first argument must be arrayref" unless ref $records_ref eq 'ARRAY';
    croak "no message specified" unless $message;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my ($found_value);
    
    foreach my $r (@$records_ref)
    {
	my $v = $r->{$field};
	cmp_ok( $v, $cmp, $value, $message ) || return;
	$found_value = 1 if defined $v && $v ne '';
    }
    
    ok( $found_value, "$message - no values found" );
}


sub cmp_distinct_count {
    
    my ($tester, $records_ref, $field, $cmp, $value, $message) = @_;
    
    croak "first argument must be arrayref" unless ref $records_ref eq 'ARRAY';
    croak "no message specified" unless $message;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my %values;
    
    foreach my $r (@$records_ref)
    {
	$values{$r->{$field}} = 1 if defined $r->{$field};
    }
    
    cmp_ok( keys %values, $cmp, $value, $message );
}


sub ok_is_subset {
    
    my ($tester, $a_ref, $b_ref, $message) = @_;
    
    croak "first argument must be hashref" unless ref $a_ref eq 'HASH';
    croak "second argument must be hashref" unless ref $b_ref eq 'HASH';
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    foreach my $k ( keys %$a_ref )
    {
	unless ( $b_ref->{$k} )
	{
	    fail($message);
	    diag("    Value '$k' not found in second set");
	    return;
	}
    }
    
    return 1;
}


sub decode_json_response {
    
    my ($tester, $response, $message) = @_;
    
    my $json;
    
    eval {
	$json = $tester->{json}->decode( $response->content );
    };
    
    if ( $@ )
    {
	fail("$message decode json");
	diag("    " . $@);
	return;
    }
    
    return $json;
}


# found_all ( hashref, @probes )
#
# Return true if every entry in @probes has a true value in %$hashref.  Return
# false otherwise.

sub found_all {

    my ($tester, $hashref, @probes) = @_;
    
    foreach my $n (@probes)
    {
	return unless $hashref->{$n};
    }
    
    return 1;
}


# check_field ( value, check, key, message )
# 
# Make sure that $value matches $check.  If the value of $check is a regexp
# then it is matched against $value.  If it is a scalar, it is compared
# either numerically or stringwise as appropriate.  If it is '!' then the
# appropriate special check is run (see code below).

sub check_field {
    
    my ($tester, $value, $check, $message) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( ref $check eq 'Regexp' )
    {
    	like( $value, $check, "$message matches regexp" ) || return;
    }
    
    elsif ( ref $check )
    {
    	cmp_ok( ref $check, 'eq', ref $value, "$message ref is " . ref $check ) || return;
    }
    
    elsif ( $check eq '!empty' )
    {
	ok( !defined $value, "$message does not have a value" );
    }
    
    elsif ( $check eq '!numeric' )
    {
	ok( looks_like_number($value), "$message is numeric" );
    }
    
    elsif ( $check eq '!nonzero' )
    {
	ok( looks_like_number($value), "$message is numeric" ) || return;
	cmp_ok( $value, '!=', 0, "$message is nonzero" );
    }
    
    elsif ( $check eq '!pos_num' )
    {
	ok( looks_like_number($value), "$message is numeric" ) || return;
	cmp_ok( $value, '>', 0, "$message is positive" );
    }
    
    elsif ( $check eq '!integer' )
    {
	ok( $value =~ qr{ ^ -? [1-9][0-9]* $ }xs, "$message is an integer" );
    }
    
    elsif ( $check eq '!pos_int' )
    {
	ok( $value =~ qr{ ^ [1-9][0-9]* $ }xs, "$message is a positive integer" );
    }
    
    elsif ( $check eq '!date' )
    {
	my $ok = ok( $value =~ qr{ ^ \d\d\d\d-\d\d-\d\d \s \d\d:\d\d:\d\d $ }xs, 
		     "$message is a valid date" );
	diag( "    Got: $value" ) unless $ok;
    }
    
    elsif ( $check =~ qr{ ^ !extid [(] ( [^)]+ ) [)] $ }xs )
    {
	my $prefix = $1;
	ok( $value =~ qr{ ^ (?: $prefix ) : [1-9][0-9]* $ }xs, "$message is a valid external id" );
    }
    
    elsif ( $check =~ qr{ ^ ! ( [<=>]+ | eq | [ngl]e | [gl]t ) : (.*) }xsi )
    {
	my $op = $1;
	my $bound = $2;
	
	cmp_ok( $value, $op, $bound, "$message value check" );
    }
    
    elsif ( $check eq '!array' )
    {
	ok( ref $value eq 'ARRAY', "$message is arrayref" ) || return;
    }
    
    elsif ( $check =~ qr{ ^ !id (?: [{] (\w+) [}] )? $ }xs )
    {
	my $prefix = $1 || '\w+';
	my $label = $1 || 'paleobiodb';
	ok( $value =~ qr{ ^ $prefix [:] ( \d+ ) $ }xs, "$message is a valid $label id" ) || return;
    }
    
    elsif ( $check =~ qr{ \| }xs )
    {
	my @possible = split( qr{\|}, $check );
	my $ok;
	
	foreach my $p (@possible)
	{
	    $ok = 1 if $value eq $p;
	}
	
	unless ( ok( $ok, $message ) )
	{
	    diag("     got: '$value'");
	    diag("expected: '$check'");
	    return;
	}
    }
    
    elsif ( $check =~ qr{ ^ ! }xs )
    {
	croak "invalid check '$check'";
    }
    
    else
    {
	$check =~ s{^\\!}{!};
	
    	cmp_ok( $value, 'eq', $check, $message ) || return;
    }
    
    return 1;
}


# check_array ( check_array, value_array, message )
# 
# Call check_field once for each entry in @$value_array

sub check_array {

    my ($tester, $value, $check, $message) = @_;
    
    foreach my $i (0..$#$value)
    {
	$tester->check_field($value->[$i], $check, "$message #$i") || return;
    }
    
    return 1;
}


# check_fields ( value_hash, check_hash, message )
# 
# Call check_field once for each key in %$check_hash.

sub check_fields {
    
    my ($tester, $value, $check, $message) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
 KEY:
    foreach my $key ( keys %$check )
    {
	if ( defined $check->{$key} && $check->{$key} ne '' && $check->{$key} ne '!empty' )
	{
	    ok( defined $value->{$key} && $value->{$key} ne '', "$message '$key' nonempty" ) or next KEY;
	}
	
	next KEY if $check->{$key} eq "!nonempty";
	
	if ( $check->{$key} eq '!empty' )
	{
	    ok( !exists $value->{$key}, "$message '$key' not found" );
	    next KEY;
	}
	
	elsif ( ref $check->{$key} eq 'ARRAY' )
	{
	    ok( ref $value->{$key} eq 'ARRAY', "$message '$key' is array" ) || next KEY;
	    ok( @{$value->{$key}}, "$message '$key' is nonempty" ) || next KEY;
	    
	    if ( defined $check->{$key}[0] )
	    {
		$tester->check_array( $value->{$key}, $check->{$key}[0], "$message '$key'" );
	    }
	    
	    next KEY;
	}
	
	elsif ( ref $check->{$key} eq 'HASH' )
	{
	    ok( ref $value->{$key} eq 'HASH', "$message '$key' is hash" ) || next KEY;
	    
	    if ( keys %{$check->{$key}} )
	    {
		$tester->check_fields($value->{$key}, $check->{$key}, "$message '$key'");
	    }
	    
	    next KEY;
	}
	
	else
	{
	    $tester->check_field($value->{$key}, $check->{$key}, "$message '$key'");
	    next KEY;
	}
    }
}


# check_order ( result_ref, field, op, idfield, message )
# 
# Make sure that the records in $result_ref are in the proper order when field
# $field is compared using $op.  Report the first discrepancy using the value
# of the field $idfield and using $message.  The parameter $result_ref must be
# an arrayref of hashes.

sub check_order {
    
    my ($tester, $result_ref, $field, $op, $idfield, $message) = @_;
    
    croak "first argument must be an arrayref" unless ref $result_ref eq 'ARRAY';
    croak "you must specify a message" unless $message;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my ($last, $bad_record);
    my ($field2, $op2, $last2);
    
    if ( ref $field eq 'ARRAY' )
    {
	($field, $field2) = @$field;
    }
    
    if ( ref $op eq 'ARRAY' )
    {
	($op, $op2) = @$op;
    }
    
    foreach my $r ( @$result_ref )
    {
	next if $r->{SKIP_ORDER};
	
	if ( defined $last )
	{
	    my $is_equal;
	    
	    if ( $op eq '<' )
	    {
		$bad_record ||= $r->{$idfield} unless $last < $r->{$field};
	    }
	    
	    elsif ( $op eq '<=' )
	    {
		$bad_record ||= $r->{$idfield} unless $last <= $r->{$field};
		$is_equal = 1 if $last == $r->{$field};
	    }
	    
	    elsif ( $op eq '>' )
	    {
		$bad_record ||= $r->{$idfield} unless $last > $r->{$field};
	    }
	    
	    elsif ( $op eq '>=' )
	    {
		$bad_record ||= $r->{$idfield} unless $last >= $r->{$field};
		$is_equal = 1 if $last == $r->{$field};
	    }
	    
	    elsif ( $op eq '==' )
	    {
		$bad_record ||= $r->{$idfield} unless $last == $r->{$field};
		$is_equal = 1;
	    }
	    
	    elsif ( $op eq '!=' )
	    {
		$bad_record ||= $r->{$idfield} unless $last != $r->{$field};
	    }
	    
	    elsif ( $op eq 'lt' )
	    {
		$bad_record ||= $r->{$idfield} unless fc($last) lt fc($r->{$field});
	    }
	    
	    elsif ( $op eq 'le' )
	    {
		$bad_record ||= $r->{$idfield} unless fc($last) le fc($r->{$field});
		$is_equal = 1 if fc($last) eq fc($r->{$field});
	    }
	    
	    elsif ( $op eq 'gt' )
	    {
		$bad_record ||= $r->{$idfield} unless fc($last) gt fc($r->{$field});
	    }
	    
	    elsif ( $op eq 'ge' )
	    {
		$bad_record ||= $r->{$idfield} unless fc($last) ge fc($r->{$field});
		$is_equal = 1 if fc($last) eq fc($r->{$field});
	    }
	    
	    elsif ( $op eq 'eq' )
	    {
		$bad_record ||= $r->{$idfield} unless fc($last) eq fc($r->{$field});
		$is_equal = 1;
	    }
	    
	    elsif ( $op eq 'ne' )
	    {
		$bad_record ||= $r->{$idfield} unless fc($last) ne fc($r->{$field});
	    }
	    
	    else
	    {
		die "unknown operation '$op'";
	    }
	    
	    # If there is a second operation, it comes into play only if this
	    # record has an equal value to the last.
	    
	    if ( $is_equal && $op2 && $field2 && defined $last2 )
	    {
		if ( $op2 eq '<' || $op2 eq '<=' )
		{
		    $bad_record ||= $r->{$idfield} unless $last2 <= $r->{$field2};
		}
		
		elsif ( $op2 eq '>' || $op2 eq '>=' )
		{
		    $bad_record ||= $r->{$idfield} unless $last2 >= $r->{$field2};
		}
		
		elsif ( $op2 eq '==' )
		{
		    $bad_record ||= $r->{$idfield} unless $last2 == $r->{$field2};
		}
		
		elsif ( $op2 eq '!=' )
		{
		    $bad_record ||= $r->{$idfield} unless $last2 != $r->{$field2};
		}
		
		elsif ( $op2 eq 'lt' || $op2 eq 'le' )
		{
		    $bad_record ||= $r->{$idfield} unless fc($last2) le fc($r->{$field2});
		}
		
		elsif ( $op2 eq 'gt' || $op2 eq 'ge' )
		{
		    $bad_record ||= $r->{$idfield} unless fc($last2) ge fc($r->{$field2});
		}
		
		elsif ( $op2 eq 'eq' )
		{
		    $bad_record ||= $r->{$idfield} unless fc($last2) eq fc($r->{$field2});
		}
		
		elsif ( $op2 eq 'ne' )
		{
		    $bad_record ||= $r->{$idfield} unless fc($last2) ne fc($r->{$field2});
		}
		
		else
		{
		    die "unknown operation '$op2'";
		}
	    }
	}
	
	$last = $r->{$field};
	
	unless ( defined $last && $last ne '' )
	{
	    fail("$message no value for '$field' ('$r->{$idfield}')");
	    return;
	}
	
	if ( $field2 )
	{
	    $last2 = $r->{$field2};
	    
	    unless ( defined $last2 && $last2 ne '' )
	    {
		fail("$message no value for '$field2' ('$r->{$idfield}')");
		return;
	    }
	}
	
	unless ( defined $r->{$idfield} && $r->{$idfield} ne '' )
	{
	    fail("$message no value for '$idfield'");
	    return;
	}
    }
    
    ok( ! $bad_record, "$message returned proper sequence" ) ||
	diag("    Found: '$bad_record' out of order");
}


# check_messages ( string_list, regex_list )
# 
# Check each one of the specified list of regular expressions against the
# specified list of strings, and return the number that match against at least
# one string.  This can be used to check a list of warning messages to make
# sure that the expected warnings are there, without any expectation as to the
# order in which they appear.  The paramter $regex_list can be either a single
# regular expression or a reference to an array of them.

sub check_messages {

    my ($tester, $message_list, $regex_list ) = @_;
    
    my @regex_list = ref $regex_list eq 'ARRAY' ? @$regex_list : $regex_list;
    my $count = 0;
    
    croak "the first parameter must be a list of messages"
	unless ref $message_list eq 'ARRAY';
    
 REGEXP:
    foreach my $r ( @regex_list )
    {
	croak "the second parameter must be either a single regex or a reference to an array of them"
	    unless ref $r eq 'Regexp';
	
    MESSAGE:	
	foreach my $m ( @$message_list )
	{
	    if ( $m =~ $r )
	    {
		$count++; next REGEXP;
	    }
	}
    }
    
    return $count;
}


# generate_coverage_expr ( hash )
# 
# Generate an SQL expression for checking the coverage (number of non-empty values) for a set of
# fields. 

sub generate_coverage_expr {
    
    shift if ref $_[0] eq 'Tester';
    
    my ($field_ref) = @_;
    
    my @field_list;
    
    if ( ref $field_ref eq 'HASH' )
    {
	@field_list = keys %$field_ref;
    }
    
    elsif ( ref $field_ref eq 'ARRAY' )
    {
	@field_list = @$field_ref;
    }
    
    else
    {
	croak "argument must be either a hash ref whose keys are field names or an array ref of field names\n";
    }
    
    croak "no field names were specified\n" unless @field_list && $field_list[0];
    
    my $list = join(', ', map { "if($_, '1', '')" } @field_list);
    
    return "concat($list)";
}


sub generate_value_expr {

    shift if ref $_[0] eq 'Tester';
    
    my ($field_ref) = @_;
    
    my @field_list;
    
    if ( ref $field_ref eq 'HASH' )
    {
	@field_list = keys %$field_ref;
    }
    
    elsif ( ref $field_ref eq 'ARRAY' )
    {
	@field_list = @$field_ref;
    }
    
    else
    {
	croak "argument must be either a hash ref whose keys are field names or an array ref of field names\n";
    }
    
    croak "no field names were specified\n" unless @field_list && $field_list[0];
    
    return join(', ', @field_list);
}


1;
