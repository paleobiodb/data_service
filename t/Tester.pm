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
our ($LAST_URL);

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
    
    $LAST_URL = $url;
    
    my $response;
    
    eval {
	$response = $tester->{ua}->get($url);
    };
    
    # If the request succeeds, we are done.
    
    if ( defined $response && $response->is_success )
    {
	$tester->extract_errwarn($response, 1);
	return $response;
    }
    
    # Otherwise, fail with the appropriate error message.
    
    elsif ( defined $response )
    {
	fail($message);
	my $status = $response->status_line;
	diag("request was: $url") if $url;
	diag("status was: $status") if $status;
	$tester->extract_errwarn($response, 1);
	return;
    }
    
    else
    {
	diag("no response or bad response");
	return;
    }
}


# extract_errwarn ( response, diag )
# 
# Extract the error and/or warning messages from $response.  If $diag is true,
# then output them using diag().  In either case, set keys __WARNINGS and/or
# __ERRORS in the $response object.

sub extract_errwarn {
    
    my ($tester, $response, $diag) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $body = $response->content;
    my (@errors, @warnings);
    
    if ( $body =~ qr< ^ [{] >xs )
    {
	my $json = $tester->{json}->decode( $body );
	
	if ( ref $json->{errors} eq 'ARRAY' )
	{
	    @errors = @{$json->{errors}};
	}
	
	if ( ref $json->{warnings} eq 'ARRAY' )
	{
	    @warnings = @{$json->{warnings}};
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
	my @lines = split qr{[\r\n]+}, $body;
	
	foreach my $line (@lines)
	{
	    if ( $line =~ qr{ ^ "Warning:" , " (.*?) " }xs )
	    {
		push @warnings, $1;
	    }
	    
	    elsif ( $line =~ qr{ ^ Warning: \t ( [^\t]+ ) }xs )
	    {
		push @warnings, $1;
	    }
	}
    }
    
    $response->{__ERRORS} = \@errors if @errors;
    $response->{__WARNINGS} = \@warnings if @warnings;
    
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


# fetch_nocheck ( path_and_args )
# 
# Works just like fetch_url, but does not check to make sure the response is a
# success. 

sub fetch_nocheck {

    my ($tester, $path_and_args, $message) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $url = $tester->make_url($path_and_args);
    
    $LAST_URL = $url;
    
    my $response;
    
    eval {
	$response = $tester->{ua}->get($url);
    };
    
    if ( $response )
    {
	$tester->extract_errwarn($response);
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
	fail("$message extract records");
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
    
    elsif ( $check eq '!notfound' )
    {
	ok( !defined $value, "$message does not have a value" );
    }
    
    elsif ( $check eq '!numeric' )
    {
	ok( looks_like_number($value), "$message is numeric" ) || return;
    }
    
    elsif ( $check eq '!nonzero' )
    {
	ok( looks_like_number($value), "$message is numeric" ) || return;
	cmp_ok( $value, '!=', 0, "$message is nonzero" ) || return;
    }
    
    elsif ( $check eq '!pos_num' )
    {
	ok( looks_like_number($value), "$message is numeric" ) || return;
	cmp_ok( $value, '>', 0, "$message is positive" ) || return;
    }
    
    elsif ( $check =~ qr{ ^ !id (?: [{] (\w+) [}] )? $ }xs )
    {
	my $prefix = $1 || '\w+';
	my $label = $1 || 'paleobiodb';
	ok( $value =~ qr{ ^ $prefix [:] ( \d+ ) $ }xs, "$message is a valid $label id" ) || return;
    }
    
    else
    {
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
	if ( defined $check->{$key} && $check->{$key} ne '' && $check->{$key} ne '!notfound' )
	{
	    ok( defined $value->{$key} && $value->{$key} ne '', "$message '$key' nonempty" ) or next KEY;
	}
	
	next KEY if $check->{$key} eq "!nonempty";
	
	if ( $check->{$key} eq '!notfound' )
	{
	    ok( !exists $value->{$key}, "$message '$key' not found" );
	}
	
	elsif ( ref $check->{$key} eq 'ARRAY' )
	{
	    ok( ref $value->{$key} eq 'ARRAY', "$message '$key' is array" ) || next KEY;
	    ok( @{$value->{$key}}, "$message '$key' is nonempty" ) || next KEY;
	    
	    if ( defined $check->{$key}[0] )
	    {
		$tester->check_array( $value->{$key}, $check->{$key}[0], "$message '$key'" );
	    }
	}
	
	elsif ( ref $check->{$key} eq 'HASH' )
	{
	    ok( ref $value->{$key} eq 'HASH', "$message '$key' is hash" ) || next KEY;
	    
	    if ( keys %{$check->{$key}} )
	    {
		$tester->check_fields($value->{$key}, $check->{$key}, "$message '$key'");
	    }
	}
	
	else
	{
	    $tester->check_field($value->{$key}, $check->{$key}, "$message '$key'");
	}
    }
}


1;
