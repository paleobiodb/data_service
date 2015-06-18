#!/usr/bin/env perl
# 
# docdiff.pl
# 
# Execute a diff on matching documentation pages between two different data
# service versions.  This will assist in making a changelog.

use strict;

use Getopt::Std;
use LWP::UserAgent;
use Storable;

use Algorithm::Diff;

use feature 'say';

my $SEPARATOR = "================";


# Parse command-line options

my %options;

getopts('s:pd', \%options);


# The option -s indicates that we should scan the indicated service starting
# with the base path indicated by the option value.  The data should be stored
# to the indicated filename.

if ( $options{s} )
{
    my $base = $options{s};
    my $filename = shift @ARGV;
    
    my $instance = ScanInstance->new($base);
    my $agent = MyAgent->new();
    
    $instance->scan_doc($agent);
    
    if ( $filename )
    {
	store($instance, $filename);
	exit;
    }
    
    else
    {
	die "No data will be stored, because no filename was specified.\n";
    }
}


# The option -p indicates that we should print out all of the documentation
# pages.  The filename from which to read the data is taken from the first argument.

elsif ( $options{p} )
{
    my $filename = shift @ARGV;
    
    die "No filename was specified.\n" unless $filename;
    
    my $instance;
    
    eval {
	$instance = retrieve($filename);
    };
    
    if ( $@ )
    {
	die "Cannot retrieve data from $filename: $!\n";
    }
    
    unless ( ref $instance eq 'ScanInstance' )
    {
	die "Cannot retrieve data from $filename: unknown error\n";
    }
    
    print_content($instance);
    exit;
}


# The option -d indicates that we should diff two setes of documentation
# pages.  There should be two arguments, giving the two filenames from which
# to read.

elsif ( $options{d} )
{
    my $newfilename = shift @ARGV;
    my $oldfilename = shift @ARGV;
    
    die "You must specify two filenames with -d.\n"
	unless defined $oldfilename && $oldfilename ne '';
    
    my ($new_instance, $old_instance);
    
    eval {
	$new_instance = retrieve($newfilename);
    };
    
    if ( $@ )
    {
	die "Cannot retrieve data from $newfilename: $!\n";
    }
    
    unless ( ref $new_instance eq 'ScanInstance' )
    {
	die "Cannot retrieve data from $newfilename: unknown error\n";
    }
    
    eval {
	$old_instance = retrieve($oldfilename);
    };
    
    if ( $@ )
    {
	die "Cannot retrieve data from $oldfilename: $!\n";
    }

    unless ( ref $old_instance eq 'ScanInstance' )
    {
	die "Cannot retrieve data from $oldfilename: unknown error\n";
    }
    
    diff_content($new_instance, $old_instance);
}


# Otherwise, we have an error.

else
{
    die "Please specify either -s, -d, or -p.\n";
}

exit;


sub print_message {
    
    my ($message) = @_;
    
    print "\n$message\n";
}


sub print_with_flag {
    
    my ($content, $flag) = @_;
    
    my @lines = split(qr{\n}, $content);
    say "$flag $_" foreach @lines;
}


sub print_content {

    my ($instance) = @_;

    foreach my $path ( $instance->path_list )
    {
	my $node = $instance->path_node($path);
	
	print_message($SEPARATOR);
	print_message("DOC PATH: $path");
	
	print "\n";
	print $node->{content};
	print "\n";
    }
}


sub diff_content {
    
    my ($new_instance, $old_instance) = @_;
    
    # Collect all of the keys together and sort them.
    
    my %keys;
    
    $keys{$_} = 1 for $new_instance->path_list;
    $keys{$_} = 1 for $old_instance->path_list;
    
    foreach my $path ( sort keys %keys )
    {
	print_message($SEPARATOR);
	
	my $new_node = $new_instance->path_node($path);
	my $old_node = $old_instance->path_node($path);
	
	if ( $new_node && $old_node )
	{
	    print_diff($new_node, $old_node);
	}
	
	elsif ( $new_node )
	{
	    print_message("NEW +++ PATH: $path");
	    print_with_flag($new_node->{content}, '+');
	}
	
	elsif ( $old_node )
	{
	    print_mesasge("OLD --- PATH: $path");
	    print_with_flag($old_node->{content}, '-');
	}
	
	else
	{
	    print_message("BAD xxx PATH: $path");
	}
    }
}

sub print_diff {

    my ($new_node, $old_node) = @_;
    
    # $new_content =~ s{$new_base}{BASE}gs;
    # $old_content =~ s{$old_base}{BASE}gs;
    
    # $new_content =~ s{ ^ =for \s+ wds_nav (.*) }{=for wds_nav ...}xm;
    # $old_content =~ s{ ^ =for \s+ wds_nav (.*) }{=for wds_nav ...}xm;
    
    my $path = $new_node->{path};
    
    my @new_seq = split(/\n/, $new_node->{content});
    my @old_seq = split(/\n/, $old_node->{content});
    
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


package ScanInstance;

use Carp qw(croak);

sub new {
    
    my ($class, $base) = @_;
    
    croak "You must specify a base as the first argument" unless defined $base && $base ne '';
    
    my $instance = { base => $base,
		     queue => [],
		     path_info => {} };
    
    bless $instance, $class;
    
    return $instance;
}


sub add_path {
    
    my ($instance, $path) = @_;
    
    $path =~ s{ \# .* $ }{}xs;
    $path =~ s{ _doc [.] (pod|html) $ }{}xs;
    
    return if defined $instance->{path_info}{$path};
    
    $instance->{path_info}{$path} = { path => $path };
    push @{$instance->{queue}}, $path;
    
    return 1;
}


sub next_path {

    my ($instance) = @_;
    
    return shift @{$instance->{queue}};
}


sub set_content {
    
    my ($instance, $path, $content) = @_;
    
    my $base = $instance->{base};
    
    $content =~ s{$base}{BASE}gs;
    $content =~ s{ ^ =for \s+ wds_nav (.*) }{=for wds_nav ...}xm;
    
    $instance->{path_info}{$path}{content} = $content;
}


sub path_list {
    
    my ($instance) = @_;
    
    return sort keys %{$instance->{path_info}};
}


sub path_node {
    
    my ($instance, $path) = @_;
    
    if ( $instance->{path_info}{$path} && $instance->{path_info}{$path}{content} )
    {
	return $instance->{path_info}{$path};
    }
    
    else
    {
	return;
    }
}


sub scan_doc {
    
    my ($instance, $agent) = @_;
    
    my $base = $instance->{base};
    
    $instance->add_path('/');
    
    while ( my $path = $instance->next_path )
    {
	my $realpath;
	
	if ( $path eq '/' )
	{
	    $realpath = '/index.pod';
	}
	
	elsif ( $path =~ qr{ (.*) / $ }xs )
	{
	    $realpath = $1 . '_doc.pod';
	}
	
	else
	{
	    $realpath = $path . '_doc.pod';
	}
	
	my $response = $agent->fetch_url($base . $realpath);
	
	if ( $response )
	{
	    my $content = $response->content;
	    
	    $instance->set_content($path, $content);
	    $instance->process_content($path, $content);
	}
    }
}
	
	
	
# 	my $new_path = $NEW_BASE . $path;
# 	my $old_path = $OLD_BASE . $path;
	
# 	my $new_response = $t->fetch_url($new_path);
# 	my $old_response = $t->fetch_url($old_path);
	
# 	my ($new_content, $old_content);
	
# 	$new_content = $new_response->content if $new_response;
# 	$old_content = $old_response->content if $old_response;
	
# 	process_content($new_content) if $new_content;
# 	process_content($old_content) if $old_content;
	
# 	print_message("================");
	
# 	if ( $new_content && $old_content )
# 	{
# 	    print_diff($path, $new_content, $old_content);
# 	}
	
# 	elsif ( $new_content )
# 	{
# 	    print_new($path, $new_content);
# 	}
	
# 	elsif ( $old_content )
# 	{
# 	    print_message("OLD --- PATH: $path");
# 	}
	
# 	else
# 	{
# 	    print_message("BAD xxx PATH: $path");
# 	}
#     }
# }
    

sub process_content {
    
    my ($instance, $docpath, $content) = @_;
    
    $docpath =~ s{[^/]+$}{};
    
    my $base = $instance->{base};
    
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
	    
	    if ( $url =~ qr{ ^ \w+ : // }xs )
	    {
		# ignore external urls
	    }
	    
	    elsif ( $url =~ qr{ ^ / ( [^/]+ ) (.*) }xs )
	    {
		next unless $1 eq $base;
		my $path = $2;
		
		$path = '/' unless defined $path && $path ne '';
		
		$instance->add_path($path);
	    }
	    
	    elsif ( $url =~ qr{ ^ [^/] }xs && $docpath ne '/' )
	    {
		$instance->add_path($docpath . $url);
	    }
	}
    }
}


package MyAgent;

sub new {
    
    my ($class, $server) = @_;
    
    my $ua = LWP::UserAgent->new(agent => "PBDB Tester/0.1")
	or die "Could not create user agent: $!\n";
    
    $server ||= $ENV{PBDB_TEST_SERVER} || '127.0.0.1:3000';
    
    my $instance = { ua => $ua,
		     # csv => Text::CSV_XS->new(),
		     # json => JSON->new(),
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


