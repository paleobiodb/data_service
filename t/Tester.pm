#
# Tester.pm: a class for running data service tests
# 


use strict;

use LWP::UserAgent;
use Text::CSV_XS;
use JSON;

package Tester;

use Scalar::Util qw(looks_like_number);
use Test::More;

use namespace::clean;

our ($TEST_NO_REPORT) = 1;

# new ( server_name )
# 
# Create a new tester instance.  If no server is specified, the value of the
# environment variable PBDB_TEST_SERVER is used instead.  If this is not set,
# the default is "127.0.0.1:3000".

sub new {
    
    my ($class, $server) = @_;
    
    my $ua = LWP::UserAgent->new(agent => "PBDB Tester/0.1")
	or die "Could not create user agent: $!\n";
    
    $server ||= $ENV{PBDB_TEST_SERVER} || '127.0.0.1:3000';
    
    my $instance = { ua => $ua,
		     csv => Text::CSV_XS->new(),
		     json => JSON->new(),
		     server => $server,
		     base_url => "http://$server" };
    
    bless $instance, $class;
    
    return $instance;
}


# set_url_check ( regex )
# 
# The specified regex will be applied to all URLs subsequently tested with
# this object, and an error will be thrown if it does not match.  This can be
# used to catch errors in the test suite, which may be introduced when code is
# copied from a test file intended for one data service version into a test
# file intended for another version.

sub set_url_check {
    
    my ($tester, $key, $regex) = @_;
    
    $tester->{url_key} = $key;
    $tester->{url_check} = $regex;
}


# fetch_url ( path_and_args, message )
# 
# Try to carry out the operation given by path_and_args on the server
# associated with this Tester instance.  If it succeeds, return the
# HTTP::Response object that is returned.
# 
# If it fails, then call Test::More::fail with the specified message and
# return undefined.

sub fetch_url {

    my ($tester, $path_and_args, $message) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    # If we haven't yet announced which server we are testing, do so now.
    
    unless ( $tester->{message_displayed} )
    {
	my $message = $tester->{server};
	$message .= " $tester->{url_key}" if $tester->{url_key};
	
	diag("TESTING SERVER: $message");
	
	$tester->{message_displayed} = 1;
    }
    
    # If a regex has been supplied for checking URLs, then fail if it doesn't
    # match.
    
    if ( my $re = $tester->{url_check} )
    {
	unless ( $path_and_args =~ $re )
	{
	    fail($message);
	    diag("ERROR: URL DID NOT MATCH $tester->{url_key} <===");
	}
    }
    
    # Create the full URL and execute a 'GET' request on the server being tested.
    
    my $url = $tester->make_url($path_and_args);
    
    my $response;
    
    eval {
	$response = $tester->{ua}->get($url);
    };
    
    # If the request succeeds, we are done.
    
    if ( defined $response && $response->is_success )
    {
	return $response;
    }
    
    # Otherwise, fail with the appropriate error message.
    
    else
    {
	fail($message);
	if ( defined $response )
	{
	    my $status = $response->status_line;
	    diag("request was: $url") if $url;
	    diag("status was: $status") if $status;
	}
	else
	{
	    diag("no response or bad response");
	}
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
    
    my $url = $tester->{base_url};
    $url .= '/' unless $path_and_args =~ qr{^/};
    $url .= $path_and_args;
    
    my $response;
    
    eval {
	$response = $tester->{ua}->get($url);
    };
    
    if ( $response )
    {
	return $response;
    }
    
    else
    {
	fail($message);
	diag("no response");
	return;
    }
}


sub make_url {
    
    my ($tester, $path_and_args) = @_;
    
    my $url = $tester->{base_url};
    $url .= '/' unless $path_and_args =~ qr{^/};
    $url .= $path_and_args;
    
    return $url;
}


# extract_records ( response, message )
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
	fail($message);
	diag("empty response");
	return;
    }
    
    my $body = $response->content;
    
    # If the body starts with '{', then this is a JSON response.
    
    if ( $body =~ qr< ^ { >xs )
    {
	return $tester->extract_records_json($response, $message, $options);
    }
    
    # Otherwise, assume it is a text response.
    
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
    
    $options ||= {};
    
    my $body = $response->{__JSON};
    
    unless ( $body )
    {
	eval {
	    $body = $response->{__JSON} = $tester->{json}->decode( $response->content );
	};
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
    
    $options ||= {};
    
    my $raw_data = $response->content || '';
    my @records;
    
    my $section = $options->{type} || 'header';
    my $limit = $options->{limit};
    
    my (@fields, @values);
    my $count = 0;
    
    my $format = $raw_data =~ qr{^"} ? 'csv' : 'tsv';
    
    my @lines = split( qr{[\n\r]+}, $raw_data );
    
 LINE:
    foreach my $line (@lines)
    {
	if ( $section eq 'records' )
	{
	    if ( $format eq 'csv' )
	    {
		$tester->{csv}->parse($line);
		@values = $tester->{csv}->fields;
	    }
	    
	    else
	    {
		@values = split(qr{\t}, $line);
	    }
	    
	    last LINE if $values[0] eq 'THIS REQUEST DID NOT GENERATE ANY OUTPUT RECORDS';
	    
	    my $r;
	    
	    foreach my $i ( 0..$#fields )
	    {
		$r->{$fields[$i]} ||= $values[$i];
	    }
	    
	    push @records, $r;
	    $count++;
	    last LINE if $limit && $count > $limit;
	    next LINE; # otherwise
	}
	
	elsif ( $section eq 'header' )
	{
	    if ( $format eq 'csv' )
	    {
		$tester->{csv}->parse($line);
		@fields = $tester->{csv}->fields;
	    }
	    
	    else
	    {
		@fields = split(qr{\t}, $line);
	    }
	    
	    $section = 'records';
	}
	
	elsif ( $line =~ qr{ ^ "? Records: "? $ }xsi )
	{
	    $section = 'header';
	    next LINE;
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
    }
    
    return $body;
}


# extract_info_text ( response, message )

sub extract_info_text {

    my ($tester, $response) = @_;
    
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
    
    return ( NO_RECORDS => 1 ) unless @r;
    
    my %found;
    
    foreach my $r (@r)
    {
	$found{$r->{$field}} = 1 if defined $r->{$field} && $r->{$field} ne '';
    }
    
    return %found;
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


# check_field ( check_hash, value_hash, key, message )
# 
# Make sure that $check_hash contains the given key, and that the key matches
# the one contained in $value_hash.  If the value in $value_hash is a regexp
# then it is matched against $check_hash.  If it is a scalar, it is compared
# either numerically or stringwise as appropriate.  If it is '!' then the
# field is only checked for existence.

sub check_field {
    
    my ($tester, $check, $value, $key, $message) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    ok( defined $check->{$key} && $check->{$key} ne '', "$message '$key' nonempty" ) or return;
    
    return 1 if $value->{$key} eq '!' || $value->{$key} eq '!nonempty';
    
    my $vmsg = "$message value '$key'";
    
    if ( ref $value->{$key} eq 'Regexp' )
    {
    	like( $check->{$key}, $value->{$key}, $vmsg ) || return;
    }
    
    elsif ( ref $value->{$key} )
    {
    	cmp_ok( ref $check->{$key}, 'eq', ref $value->{$key}, $vmsg ) || return;
    }
    
    elsif ( $value->{$key} eq '!numeric' )
    {
	ok( looks_like_number($check->{$key}), "$vmsg is numeric" ) || return;
    }
    
    elsif ( $value->{$key} eq '!nonzero' )
    {
	ok( looks_like_number($check->{$key}), "$vmsg is numeric" ) || return;
	cmp_ok( $check->{$key}, '>', 0, $vmsg ) || return;
    }
    
    elsif ( $value->{$key} =~ qr{ ^ !id (?: [{] (\w+) [}] )? $ }xs )
    {
	my $prefix = $1 || '\w+';
	my $label = $1 || 'paleobiodb';
	ok( $check->{$key} =~ qr{ ^ $prefix [:] ( \d+ ) $ }xs, "$vmsg is a valid $label id" ) || return;
    }
    
    # elsif ( looks_like_number($value->{$key}) )
    # {
    # 	cmp_ok( $check->{$key}, '==', $value->{$key}, $vmsg ) || return;
    # }
    
    else
    {
    	cmp_ok( $check->{$key}, 'eq', $value->{$key}, $vmsg ) || return;
    }
    
    return 1;
}


# check_fields ( check_hash, value_hash, message )
# 
# Call check_field once for each key in %$value_hash.

sub check_fields {
    
    my ($tester, $check, $value, $message) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    foreach my $key ( keys %$value )
    {
	$tester->check_field($check, $value, $key, $message);
    }
}


1;
