#!/usr/bin/env perl
# 
# docdiff.pl
# 
# Execute a diff on matching documentation pages between two different data
# service versions.  This will assist in making a changelog.

use strict;

use Getopt::Std;
use LWP::UserAgent;
use Text::CSV_XS;
use JSON;

use Algorithm::Diff;


my @QUEUE;
my %FOUND_PATH = ( '/' => 1 );



my $t = MyAgent->new();


my $NEW_BASE = shift @ARGV;
my $OLD_BASE = shift @ARGV;

# $$$ options

die "You must specify both a 'new' and an 'old' root.\n" unless $NEW_BASE && $OLD_BASE;

scan_doc();

print_message("DONE.\n");
exit;


sub scan_doc {

    push @QUEUE, { path => '/' };
    
    while ( @QUEUE )
    {
	my $node = shift @QUEUE;
	
	my $path = $node->{path};
	
	if ( $path eq '/' )
	{
	    $path = '/index.pod';
	}
	
	elsif ( $path =~ qr{ (.*) / $ }xs )
	{
	    $path = $1 . '_doc.pod';
	}
	
	else
	{
	    $path .= '_doc.pod';
	}
	
	my $new_path = $NEW_BASE . $path;
	my $old_path = $OLD_BASE . $path;
	
	my $new_response = $t->fetch_url($new_path);
	my $old_response = $t->fetch_url($old_path);
	
	my ($new_content, $old_content);
	
	$new_content = $new_response->content if $new_response;
	$old_content = $old_response->content if $old_response;
	
	process_content($new_content) if $new_content;
	process_content($old_content) if $old_content;
	
	print_message("================");
	
	if ( $new_content && $old_content )
	{
	    print_diff($path, $new_content, $old_content);
	}
	
	elsif ( $new_content )
	{
	    print_new($path, $new_content);
	}
	
	elsif ( $old_content )
	{
	    print_message("OLD --- PATH: $path");
	}
	
	else
	{
	    print_message("BAD xxx PATH: $path");
	}
    }
}
    

sub process_content {
    
    my ($content) = @_;
    
    while ( $content =~ qr{L<}s )
    {
	$content =~ s{^.*?L<}{L<}s;
	my $url;
	
	if ( $content =~ qr{ ^ L<<< (.*?) >>> (.*) }xs )
	{
	    $url = $1;
	    $content = $2;
	}
	
	elsif ( $content =~ qr{ ^ L<< (.*?) >> (.*) }xs )
	{
	    $url = $1;
	    $content = $2;
	}
	
	elsif ( $content =~ qr{ ^ L< (.*?) > (.*) }xs )
	{
	    $url = $1;
	    $content = $2;
	}
	
	else
	{
	    $content =~ s{^L<}{};
	}
	
	if ( defined $url )
	{
	    if ( $url =~ qr{ ^ (.*?) \# (.*) }xs )
	    {
		$url = $1;
	    }
	    
	    if ( $url =~ qr{ ^ (.*?) \| (.*) $ }xs )
	    {
		$url = $2;
	    }
	    
	    $url =~ s{ ^ \s+ }{}xs;
	    $url =~ s{ \s+ $ }{}xs;
	    
	    if ( $url =~ qr{ (.*) [.] ( [^/]+ ) $ }xs )
	    {
		next unless $2 eq 'html' || $2 eq 'pod';
		$url = $1;
	    }
	    
	    $url =~ s{_doc$}{};
	    
	    if ( $url =~ qr{ ^ / ( [^/]+ ) (.*) }xs )
	    {
		next unless $1 eq $NEW_BASE || $1 eq $OLD_BASE;
		
		my $path = $2;
		
		if ( $path && ! $FOUND_PATH{$path} )
		{
		    $FOUND_PATH{$path} = 1;
		    push @QUEUE, { path => $path };
		}
	    }
	}
    }
}


sub print_message {
    
    my ($message) = @_;
    
    print "\n$message\n";
}


sub print_diff {
    
    my ($path, $new_content, $old_content) = @_;
    
    $new_content =~ s{$NEW_BASE}{BASE}gs;
    $old_content =~ s{$OLD_BASE}{BASE}gs;
    
    $new_content =~ s{ ^ =for \s+ wds_nav (.*) }{=for wds_nav ...}xm;
    $old_content =~ s{ ^ =for \s+ wds_nav (.*) }{=for wds_nav ...}xm;
    
    my @new_seq = split(/\n/, $new_content);
    my @old_seq = split(/\n/, $old_content);
    
    my $diff = Algorithm::Diff->new(\@new_seq, \@old_seq);
    
    $diff->Base(1);
    
    my $output = '';
    my $pluses = 0;
    my $minuses = 0;
    
    while ( $diff->Next() )
    {
	my @items1 = $diff->Items(1);
	my @items2 = $diff->Items(2);
	
	if ( $diff->Same() )
	{
	    $output .= "  $_\n" for @items1;
	}
	
	elsif ( ! @items2 )
	{
	    $output .= "+ $_\n" for @items1;
	    $pluses += scalar(@items1);
	}
	
	elsif ( ! @items1 )
	{
	    $output .= "- $_\n" for @items2;
	    $minuses += scalar(@items2);
	}
	
	else
	{
	    $output .= "+ $_\n" for @items1;
	    $pluses += scalar(@items1);
	    $output .= "- $_\n" for @items2;
	    $minuses += scalar(@items2);
	}
    }
    
    if ( $pluses + $minuses )
    {
	my $adjp = int(sqrt($pluses));
	my $adjm = int(sqrt($minuses));
	my $diffs = $pluses + $minuses;
	
	my $pmstring = '';
	$pmstring .= '+' x $adjp;
	$pmstring .= '-' x $adjm;
	
	print_message("DIFF !!! PATH: $path   $diffs $pmstring\n");
	
	print $output;
    }
    
    else
    {
	print_message("SAME ___ PATH $path");
    }
}


sub print_new {
    
    my ($path, $content) = @_;
    
    print_message("NEW +++ PATH: $path");
}



package MyAgent;

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


# fetch_url ( path_and_args, message )
# 
# Try to carry out the operation given by path_and_args on the server
# associated with this Tester instance.  If it succeeds, return the
# HTTP::Response object that is returned.
# 
# If it fails, then return undefined.

sub fetch_url {

    my ($tester, $path_and_args, $message) = @_;
    
    # If we haven't yet announced which server we are testing, do so now.
    
    unless ( $tester->{message_displayed} )
    {
	my $message = $tester->{server};
	$message .= " $tester->{url_key}" if $tester->{url_key};
	
	print STDERR "TESTING SERVER: $message\n";
	
	$tester->{message_displayed} = 1;
    }
    
    # If a regex has been supplied for checking URLs, then fail if it doesn't
    # match.
    
    if ( my $re = $tester->{url_check} )
    {
	unless ( $path_and_args =~ $re )
	{
	    # fail($message);
	    # diag("ERROR: URL DID NOT MATCH $tester->{url_key} <===");
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
    
    # Otherwise, return undefined.  Save the status, in case we want to
    # display it later.
    
    else
    {
	$tester->{last_status} = $response->status_line if $response;
	return;
    }
    
    # else
    # {
    # 	print STDERR "Fetch of '$url' failed.\n";
	
    # 	if ( defined $response )
    # 	{
    # 	    my $status = $response->status_line;
    # 	    print STDERR "Status was: $status\n" if $status;
    # 	}
    # 	else
    # 	{
    # 	    print STDERR "No response or bad response.\n";
    # 	}
	
    # 	return;
    # }
}


sub make_url {
    
    my ($tester, $path_and_args) = @_;
    
    my $url = $tester->{base_url};
    $url .= '/' unless $path_and_args =~ qr{^/};
    $url .= $path_and_args;
    
    return $url;
}


