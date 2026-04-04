#!/usr/bin/env perl

# test_common.pl - process the output of bin/common_queries.pl and run the most common
# queries to see which ones are slow.



use strict;

use feature 'say';
use LWP::UserAgent;

use Getopt::Long;


# Process the options and arguments.

Getopt::Long::Configure("bundling");

my $opt_count = '';
my $min_count = 100;
my $max_count = undef;
my $max_elapsed = 10;
my ($test_selector, $query_url, $query_param, $opt_force, $opt_test_server, $opt_main_server);

my %selected;

GetOptions("c|count=s" => \$opt_count,
	   "e|elapsed=i" => \$max_elapsed,
	   "t|test=s" => \$test_selector,
	   "q|query=s" => \$query_url,
	   "p|param=s" => \$query_param,
	   "t|test" => \$opt_test_server,
	   "m|main" => \$opt_main_server,
	   "f|force" => \$opt_force);

# Set the range (min and max) of the 'count' parameter for selecting tests.

if ( $opt_count =~ /^\d+$/ )
{
    $min_count = $opt_count;
}

elsif ( $opt_count =~ /^(\d+)-(\d+)$/ )
{
    $min_count = $1;
    $max_count = $2;
}

elsif ( $opt_count )
{
    die "Error: bad value '$opt_count' for option --count\n";
}

# If the 'test' option was specified, select just the indicated tests.

if ( $test_selector )
{
    foreach my $arg ( split /\s*,\s*/, $test_selector )
    {
	if ( $arg =~ /^\d+$/ )
	{
	    $selected{$arg + 0} = 1;
	}
	
	else
	{
	    die "Error: invalid argument '$arg'\n";
	}
    }
}

# If the 'main' option was specified, make requests on port 80. Otherwise,
# default to 3999.

my $HOST = $opt_main_server ? 'paleobiodb.org' : 'localhost';
my $PORT = $opt_test_server ? ':3999' : '';

# Unbuffer STDOUT, so we can see the results immediately

$| = 1;

# The diagnostic file is checked after every request to see if it has grown larger. If
# it has, then print out the added characters. This only works if we are using the test
# server and its STDERR has been redirected to this file.

our ($DIAG_FILE) = "test_diagnostic.txt";

# Generate a UserAgent object with which to make requests on the test server. The
# timeout is set to a very high value so that we can test how long slow queries take. We
# also set max_size, so that when we are fetching very long data sets we don't actually
# hold the entire result in memory.

my $ua = LWP::UserAgent->new();
$ua->agent("Paleobiology Database Query Tester");
$ua->timeout(1800);
$ua->max_size(1024 * 1024);


# Main operation
# --------------

# If the 'query' option was specified, just execute the given URL.

if ( $query_url )
{
    %selected = (1 => 1);
    RunTest(1, { count => 0, pattern => "n/a", examples => [$query_url] });
    exit;
}

# Otherwise, process the lines from the input file, which should be the output of
# bin/common_queries.pl. We use a simple state machine to parse the file into a set of
# tests, each one represented by a hashref.

my $state = 'INIT';
my (@tests, $pattern, $count, $exception, @examples);

my $starttest = time;
my $fail_count = 0;

while (<<>>)
{
    if ( /^----/ )
    {
	push @tests, { count => $count, pattern => $pattern, exception => $exception,
		       examples => [@examples] };

	$state = 'PATTERN';
	$count = undef;
	$pattern = undef;
	@examples = ();
    }

    elsif ( $state eq 'PATTERN' )
    {
	($count, $pattern, $exception) = split /\s+/;
	$state = 'EXAMPLES';
    }

    elsif ( $state eq 'EXAMPLES' )
    {
	push @examples, $_;
    }
}


push @tests, { count => $count, pattern => $pattern, exception => $exception,
	       examples => [@examples] };


# If we are selecting a test or tests by number, make sure that the numbers correspond
# to tests in the parsed result.

foreach my $k ( keys %selected )
{
    unless ( $tests[$k] )
    {
	say STDERR "Warning: could not find test $k";
    }
}

# Now iterate through the tests. If %selected is not empty, run only those tests whose
# indices appear as keys. Otherwise, run all tests where the 'count' parameter falls
# into the specified range.

my $total = %selected ? scalar(keys %selected) : scalar(@tests);

say "Total tests: $total\n";

foreach my $i ( 0..$#tests )
{
    my $result;
    
    if ( %selected )
    {
	$result = RunTest($i, $tests[$i]) if $selected{$i};
    }
    
    elsif ( $tests[$i]{pattern} && $tests[$i]{count} >= $min_count &&
	    (!defined $max_count || $tests[$i]{count} <= $max_count) )
    {
	$result = RunTest($i, $tests[$i]);
    }
    
    if ( $result eq 'ABORT' )
    {
	say "\nAborting the run because the server has gone away";
	last;
    }
}

my $elapsedminutes = int((time - $starttest)/60);

say "Test failures: $fail_count";
say "Elapsed Time: $elapsedminutes minutes";

exit;


# RunTest ( index, test )
#
# Run the specified test and print the results to STDOUT.

sub RunTest {

    my ($index, $test) = @_;
    
    my $count = $test->{count};
    my $pattern = $test->{pattern};
    
    my $time_limit = $max_elapsed;
    my $exception = $test->{exception};
    
    if ( $exception )
    {
	return if $exception =~ /SKIP/;
	$time_limit = $1 if $exception =~ /^TIME=(\d+)$/;
    }
    
    foreach my $query ( $test->{examples}->@* )
    {
	chomp $query;
	
	if ( $query =~ qr{data1.2/occs/list|data1.2/colls/list} && $time_limit < 20 )
	{
	    $time_limit = 20;
	}
	
	$query .= "&$query_param" if $query_param;
	
	my $request = HTTP::Request->new(GET => "http://$HOST$PORT$query");
	
	my $starttime = time;
	my $startsize = (-r $DIAG_FILE) && (-s $DIAG_FILE);
	
	my $response = $ua->request($request);
	
	my $elapsed = time - $starttime;
	my $sizediff = (-r $DIAG_FILE) ? (-s $DIAG_FILE) - $startsize : 0;
	
	my $code = $response->code;
	my $status = $response->status_line;
	my $content_type = $response->content_type;
	my $content = $response->content;
	
	my ($fh, @lines);
	
	if ( $sizediff > 100 && ($code ne '200' || $elapsed > $time_limit || $opt_force) )
	{
	    open($fh, "tail -c $sizediff test_diagnostic.txt|");
	    
	    while ( <$fh> )
	    {
		push @lines, $_ unless /^-----/;
	    }
	    
	    close $fh;
	}
	
	if ( $code ne '200' || $elapsed > $time_limit )
	{
	    ReportTest($index, $count, $pattern, $status, $elapsed, $sizediff, $query, \@lines);
	    $fail_count++;
	    return $status =~ /Connection refused/i ? 'ABORT' : '';
	}
	
	elsif ( %selected )
	{
	    ReportTest($index, $count, $pattern, $status, $elapsed, $sizediff, $query, \@lines);
	}
    }
    
    return '';
}



sub ReportTest {
    
    my ($index, $count, $pattern, $code, $elapsed, $sizediff, $query, $lines) = @_;
    
    say "======================";
    say "Test $index of $total";
    say "Pattern: $pattern";
    say "";
    say "Query: $query";
    say "";
    say "Count: $count";
    say "Code: $code";
    say "Elapsed: $elapsed";
    say "";
    
    print @$lines;

    say "";
}


# sub TestSucceeded {

#     my ($index, $count, $pattern, $code, $elapsed, $sizediff, $query) = @_;
        
#     say "======================";
#     say "Test $index of $total";
#     say "Pattern: $pattern";
#     say "";
#     say "Query: $query";
#     say "";
#     say "Count: $count";
#     say "Code: $code";
#     say "Elapsed: $elapsed";
#     say "";
# }
