#!/usr/bin/env perl
#
# longrequests.pl - analyze the request log and print out requests that take a
# lot of system resources
#
# Created 2026-02-26
# by Michael McClennen


use strict;

use GetOpt::Long;
use feature 'say';

Getopt::Long::Configure("bundling");

my $threshold = 5;
my $lines;

GetOptions("t|threshold=d" => \$threshold);


if ( $ARGV[0] =~ /^\d+$/ )
{
    $lines = shift @ARGV;
}

else
{
    die "ERROR: You must specify the number of lines to look back\n";
}


&AnalyzeLog($lines);

exit;



sub AnalyzeLog {

    my ($lines_to_fetch) = @_;
    
    my @data = `tail -$lines_to_fetch /var/paleobiodb/logs/pbapi/request_log`;

    my (%command);

    foreach my $line ( @data )
    {
	if ( $line =~ / (\d+) \s+ : \s+ \d .* " (.*) " /xs )
	{
	    $command{$1} = $2;
	}

	elsif ( $line =~ / (\d+) \s+ : \s+ DONE \s [(] (\d+) /xs )
	{
	    my $pid = $1;
	    my $secs = $2;
	    
	    next unless $secs >= $threshold;
	    next unless $command{$pid};
	    
	    say "$command{$pid} ($secs)";
	    delete $command{$pid};
	}
	
	else
	{
	    say "Bad line: $line";
	}
    }
}

