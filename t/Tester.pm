#
# Tester.pm: a class for running data service tests
# 


use strict;

use Test::More;
use Text::CSV_XS;


package Tester;


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
    
    diag("TESTING SERVER: $server");
    
    my $instance = { ua => $ua,
		     csv => Text::CSV_XS->new(),
		     server => $server,
		     base_url => "http://$server" };
    
    bless $instance, $class;
    
    return $instance;
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
    
    my $url = $tester->{base_url};
    $url .= '/' unless $path_and_args =~ qr{^/};
    $url .= $path_and_args;
    
    my $response;
    
    eval {
	$response = $tester->{ua}->get($url);
    };
    
    if ( defined $response && $response->is_success )
    {
	return $response;
    }
    
    else
    {
	fail($message);
	if ( defined $response )
	{
	    my $status = $response->status_line;
	    diag("status was: $status") if $status;
	}
	else
	{
	    diag("status was: no response or bad response");
	}
	diag('skipping subsequent tests for this URL');
	return;
    }
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
    
    my ($tester, $response, $message) = @_;
    
    my $body = $response->{__JSON};
    
    unless ( $body )
    {
	eval {
	    $body = $response->{__JSON} = decode_json( $response->content );
	};
    }
    
    if ( ref $body->{records} eq 'ARRAY' && @{$body->{records}} && ref $body->{records}[0] eq 'HASH' )
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
    
    $options ||= {};
    
    my $raw_data = $response->content || '';
    my @records;
    
    my $section = $options->{type} || 'header';
    my $limit = $options->{limit};
    
    my (@fields, @values);
    my $count = 0;
    
    my $format = $raw_data =~ qr{^"} ? 'csv' : 'tsv';
    
 LINE:
    while ( $raw_data =~ qr{ ^ (.*?) (?: \r\n? | \n ) (.*) }xsi )
    {
	$raw_data = $2;
	my $line = $1;
	
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
	    
	    my $r;
	    
	    foreach my $i ( 0..$#fields )
	    {
		$r->{$fields[$i]} = $values[$i];
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
    
    if ( @records )
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
    
    my $body = $response->{__JSON};
    
    unless ( $body )
    {
	eval {
	    $body = $response->{__JSON} = decode_json( $response->content );
	};
    }
    
    return $body;
}


# extract_info_text ( response, message )

sub extract_info_text {

    my ($tester, $response) = @_;
    
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
	}
	
	elsif ( $line =~ qr{^"Records:"} )
	{
	    last LINE;
	}
	
	elsif ( $section eq 'parameters' )
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
	}
	
	elsif ( $section eq 'info' )
	{
	    if ( $format eq 'csv' )
	    {
		$tester->{csv}->parse($line);
		my ($field, $value) = $tester->{csv}->fields;
		$info->{$field} = $value;
	    }
	    
	    else
	    {
		my ($field, $value) = split(qr{\t}, $line);
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


1;
