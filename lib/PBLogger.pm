# 
# PBLogger.pm - special-purpose logger module for the PBDB data service
# 
# This module generates an extra log that records each request as it comes in.
# This is important, because the main log only records requests as they
# finish.  Requests that hang and do not properly finish therefore will not be
# recorded in the regular access log.  The output of this module will be the
# only record of such requests.

use strict;

package PBLogger;

use Dancer qw(info);
use Dancer::Config qw(setting);
use Dancer::Logger::File;
use Dancer::FileUtils qw(open_file);
use Encode qw(encode_utf8);
use File::Spec;
use Carp qw(carp);
use Fcntl qw(:flock SEEK_END);

use namespace::clean;


sub new {
    
    my ($class) = @_;
    
    my $setting = setting('request_log');
    
    unless ( $setting && $setting ne 'none' )
    {
	print STDERR "Request log disabled\n";
	return;
    }
    
    my $logger = bless { }, $class;
    
    eval {
	$logger->open_logfile;
    };
    
    carp($@) if $@;
    
    info("Request log set to $logger->{logfile}") if $logger->{logfile};
    
    carp("Logfile not found!") unless $logger->{logfile};
    
    return $logger;
}


sub open_logfile {
    
    my ($logger) = @_;
    
    my $logdir = Dancer::Logger::File::logdir;
    return unless $logdir;
    
    my $logfile = setting('request_log_file') || 'request_log';
    
    mkdir($logdir) unless(-d $logdir);
    $logfile = File::Spec->catfile($logdir, $logfile);
    
    my $fh;
    
    unless($fh = open_file('>>', $logfile))
    {
        carp "unable to create or append to $logfile: $!";
        return;
    }
    
    $fh->autoflush;
    
    $logger->{logfile} = $logfile;
    $logger->{fh} = $fh;
}


sub log_request {
    
    my ($logger, $request) = @_;
    
    my $fh = $logger->{fh};
    
    return unless(ref $fh && $fh->opened);
    
    my $remote_addr = $request->headers ? $request->header('X-Real-IP') || $request->env->{REMOTE_ADDR}
	: $request->env->{REMOTE_ADDR};
    my $time_formatted = localtime;
    my $method = $request->method;
    my $uri = $request->uri;
    my $referer = $request->referer || '';
    my $agent = $request->user_agent || '';
    my $post_params = '';

    if ( $method eq 'POST' || $method eq 'PUT' )
    {
	if ( $request->content_type =~ qr{ application/x-www-form-urlencoded }xsi &&
	     $request->params('body') )
	{
	    my %params = $request->params('body');
	    
	    while ( my ($key, $value) = each %params )
	    {
		$post_params .= "  $key=$value\n";
	    }
	}
	
	elsif ( $request->body() )
	{
	    $post_params = $request->body();
	    $post_params .= "\n" unless $post_params =~ /\n$/;
	}
    }
    
    my $line = sprintf(qq{%6d : %s [%s] "%s %s"\n%s}, $$, $remote_addr, $time_formatted,
		       $method, $uri, $post_params);
    
    $fh->print(encode_utf8($line));
}


sub log_event {
    
    my ($logger, $request, $event, $starttime) = @_;
    
    my $fh = $logger->{fh};
    
    return unless(ref $fh && $fh->opened);
    
    my $elapsed = time - $starttime;

    my $line = sprintf(qq{%-6d : %s (%s secs)\n}, $$, $event, $elapsed);
    
    $fh->print(encode_utf8($line));
}

1;
