#!/usr/bin/env perl
# 
# refcheck.pl
# 
# Run some experiments on the refs table.

use strict;

use lib 'lib', '../lib';
use Getopt::Long;

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB configData);
use TableDefs qw(%TABLE);
use ReferenceSources;
use ReferenceMatch qw(ref_similarity);

use Encode;
# use Algorithm::Diff qw(sdiff);
# use Text::Levenshtein::Damerau qw(edistance);
use JSON;


# Initial declarations
# --------------------

my $starttime = time;

STDOUT->autoflush(1);


# Option and argument handling
# ----------------------------

# First parse option switches.  If we were given an argument, then use that as
# the database name overriding what was in the configuration file.

my ($opt_refno, $opt_sourceno, $opt_match, $opt_all, $opt_unchecked, $opt_since,
    $opt_limit, $opt_print, $opt_which, $opt_crossref, $opt_xdd, $opt_debug);

my ($expr_a, $expr_b);

# $opt_fulltitle, $opt_fullpub,
	   # "fulltitle|t" => \$opt_fulltitle,
	   # "fullpub|p" => \$opt_fullpub,

GetOptions("n|refno=i" => \$opt_refno,
	   "s|sourceno=i" => \$opt_sourceno,
	   "m|match=s" => \$opt_match,
	   "u|unchecked" => \$opt_unchecked,
	   "unchecked-since=s" => \$opt_since,
	   "l|limit=i" => \$opt_limit,
	   "p|print" => \$opt_print,
	   "C|crossref" => \$opt_crossref,
	   "X|xdd" => \$opt_xdd,
	   "debug|d" => \$opt_debug);

# Then the remaining command-line arguments. If we don't specify any, the action defaults to
# 'print'.

my $action;

my %ACCEPTED_ACTION = ( print => 1, fetch => 1, test => 1, match => 1, score => 1,
			showscore => 1, history => 1, dump => 1, show => 1,
			recount => 1, compare => 1 );

if ( defined $ARGV[0] && $ACCEPTED_ACTION{$ARGV[0]} )
{
    $action = shift @ARGV;
}

# If the action is 'compare' then we look for two SQL expressions.

if ( $action eq 'compare' )
{
    $expr_a = shift @ARGV;
    $expr_b = shift @ARGV;

    unless ( $expr_a )
    {
	print STDERR "ERROR: 'compare' must be followed by one or two SQL expressions.\n";
	exit 2;
    }
    
    if ( $expr_a !~ /_[sc]/ && $expr_a !~ /\d/ )
    {
	print STDERR "ERROR: the first argument must be a valid SQL expression.\n";
	exit 2;
    }
    
    if ( $expr_b && $expr_b !~ /_[sc]/ && $expr_b !~ /\d/ )
    {
	print STDERR "ERROR: the second argument must be a valid SQL expression.\n";
	exit 2;
    }
}

# For all other actions, if a numeric argument follows, it indicates a reference_no value. If
# prefixed with '@', it indicates a refsource_no value.

else
{
    if ( defined $ARGV[0] && $ARGV[0] =~ /^([@])?(\d+)$/ )
    {
	if ( $opt_refno || $opt_sourceno )
	{
	    print STDERR "WARNING: option --refno or --sourceno overridden by argument\n";
	}
	
	if ( $1 ) { $opt_sourceno = $2; }
	else { $opt_refno = $2; }
	
	shift @ARGV;
    }
    
    # If instead the action is followed by 'all' or 'unchecked' then set the equivalent option.
    
    elsif ( defined $ARGV[0] && $ARGV[0] eq 'all' )
    {
	$opt_all = 1;
	shift @ARGV;
    }
    
    elsif ( defined $ARGV[0] && ($ARGV[0] eq 'unchecked' ||
				 $ARGV[0] eq 'unscored' || $ARGV[0] eq 'unmatched') )
    {
	$opt_unchecked = 1;
	shift @ARGV;
    }
}

# The actions 'dump' and 'show' take an additional argument to specify which of the records in the
# response should be output.

if ( $action eq 'dump' || $action eq 'show' || $action eq 'score' )
{
    if ( defined $ARGV[0] && $ARGV[0] =~ /^all$|^\d+$/ )
    {
	$opt_which = shift @ARGV;
    }
}

# If there are any remaining arguments, print a warning if an action was specified and otherwise
# die with an error.

if ( defined $ARGV[0] )
{
    if ( $action )
    {
	print STDERR "WARNING: invalid argument '$ARGV[0]'\n";
    }

    else
    {
	die "Invalid action '$ARGV[0]'\n";
    }
}

# The default action is 'print'.

$action ||= 'print';

# For the 'print' action, the default limit is 5.

$opt_limit ||= 5 if $action eq 'print';


# Database connection
# -------------------

# Get a database handle. If we don't find the configuration file in the current directory, look
# for it up to two levels up.

my $config = "config.yml";
my $levels = 2;

while ( ! -e $config && $levels-- )
{
    $config = "../$config";
}

my $dbh = connectDB($config, "pbdb");

# Verify to the user the database that we are checking.

if ( $dbh->{Name} =~ /database=([^;]+)/ )
{
    print STDERR "Connected to database: $1\n\n";
}

else
{
    print STDERR "Using connect string: $dbh->{Name}\n\n";
}


# Select references to process
# ----------------------------

# The following variables control processing of references. If a list of references is given
# either by explicitly identifying them or by matching against a set of attributes, the matching
# reference data is stored in @REF_LIST. If we are looping through the entire reference table, the
# $IN_LOOP variable controls that process.

my @REF_LIST;
my $END_LOOP = 'no loop';
my $NO_PRINT;

$SIG{INT} = \&stop_loop;

# If a refsource_no value was specified, either as an option value or an argument, use the
# corresponding reference record with the refsource_no added as the reference to process.

if ( $opt_sourceno )
{
    if ( $opt_sourceno > 0 )
    {
	if ( my $r = ref_from_sourceno($dbh, $opt_sourceno) )
	{
	    @REF_LIST = $r;
	    
	    if ( $opt_refno && $opt_refno ne $r->{reference_no} )
	    {
		die "ERROR: specified refno and sourceno are not consistent\n";
	    }
	}

	else
	{
	    die "ERROR: unknown refsource id '$opt_sourceno'\n";
	}
    }

    else
    {
	die "ERROR: invalid refsource id '$opt_sourceno'\n";
    }
}


# If a reference_no value was specified instead, either as an option value or an argument, use the
# specified paleobiodb reference record as the reference to process.

elsif ( $opt_refno )
{
    if ( $opt_refno > 0 )
    {
	if ( my $r = ref_from_refno($dbh, $opt_refno) )
	{
	    @REF_LIST = $r;
	}
	
	else
	{
	    die "ERROR: unknown reference id '$opt_refno'\n";
	}
    }
    
    else
    {
	die "ERROR: invalid reference id '$opt_refno'.\n";
    }
}


# If --match was specified instead, extract attributes from the option value. If the action is
# 'test', then these attributes will be used to test the remote source. Otherwise, they will be
# matched against the paleobiology database references table.

elsif ( $opt_match )
{
    unless ( $opt_match =~ /^\w+[:]/ )
    {
	die "ERROR: invalid reference attributes '$opt_match'\n";
    }
    
    my $attrs = ref_from_args($opt_match);
    
    # Print an error message and exit unless we have a nonempty value for either the doi or at
    # least two of the following attributes.
    
    my $count = 0;
    $count++ if $attrs->{reftitle};
    $count++ if $attrs->{pubtitle};
    $count++ if $attrs->{author1last};
    $count++ if $attrs->{pubyr};
    
    unless ( $attrs->{doi} || $count > 1 )
    {
	die "ERROR: not enough attributes for a reference match\n";
    }
    
    # If the action is 'test', we add the hashref of attributes to @REF_LIST. These attributes
    # will be used to generate a test query on the specified reference source.
    
    if ( $action eq 'test' )
    {
	push @REF_LIST, $attrs;
    }
    
    # Otherwise, attempt to match the specified attributes against the pbdb database.
    
    else
    {
	@REF_LIST = find_matches($dbh, $attrs);
	
	unless ( @REF_LIST )
	{
	    print STDERR "No matching references found\n";
	    exit;
	}
    }
}


# Main loop
# ---------

# Create a new ReferenceSources object with which we can make queries and interact with the
# REFERENCE_SOURCES table. The reference source defaults to crossref, unless overridden by an option.

my $source = $opt_xdd      ? 'xdd'
           : $opt_crossref ? 'crossref'
                           : 'all';

my $rs = ReferenceSources->new($dbh, $source, { debug => $opt_debug,
						limit => $opt_limit,
					        since => $opt_since });

# If the action is 'fetch', we must have a source selected.

if ( $action eq 'fetch' && ($source eq 'all' || ! $source) )
{
    print STDERR "ERROR: you must specify a source with either --crossref or --xdd\n";
    exit 2;
}

# If the action is 'compare', do a score comparison. First query for differential counts and
# display that info to the user. Ask the user if they want to see the actual lists of reference matches.

if ( $action eq 'compare' )
{
    # First get the counts.
    
    my ($count_a, $count_b) = $rs->compare_scores($expr_a, $expr_b);

    # Print them out.

    if ( $expr_b )
    {
	print "Matched A: $count_a\n\n";

	print "Matched B: $count_b\n\n";
    }

    else
    {
	print "Matched: $count_a\n\n";
    }
    
    # Now loop, asking for a response.
    
    my $answer;
    my $matchlist_a;
    my $matchlist_b;
    
    while (1)
    {
	print "See match list? ";
	my $answer = <STDIN>;
	chomp $answer;
	
	last if $answer =~ /^[nq]/i;
	next unless $answer =~ /^[aby]/i;
	
	my @matches;
	
	if ( $answer =~ /^[ay]/i )
	{
	    if ( ref $matchlist_a eq 'ARRAY' )
	    {
		@matches = @$matchlist_a;
	    }

	    else
	    {
		@matches = $rs->compare_list_scores($expr_a, $expr_b, 50);
		$matchlist_a = \@matches;
	    }
	}
	
	else
	{
	    if ( ref $matchlist_b eq 'ARRAY' )
	    {
		@matches = @$matchlist_b;
	    }

	    else
	    {
		@matches = $rs->compare_list_scores($expr_b, $expr_a, 50);
		$matchlist_b = \@matches;
	    }
	}
	
	open(my $outfile, '>', "/var/tmp/refcheck$$.output");
	
	my $sep = '';
	
	foreach my $m ( @matches )
	{
	    print $outfile $sep;
	    print $outfile "REF $m->{reference_no}    \@$m->{refsource_no}:\n\n";
	    print $outfile $m->{ref_formatted}, "\n\n";
	    print $outfile $m->{formatted}, "\n\n";
	    print $outfile format_scores_horizontal($m), "\n";
	    
	    $sep = "===================================================\n";
	}

	close $outfile;

	system("less", "/var/tmp/refcheck$$.output");
    }
    
    unlink "/var/tmp/refcheck$$.output";
    
    exit;
}

# If any of the mass-selection options were chosen, confirm to the user what we are doing and then
# execute a query for the selected references.

if ( $opt_unchecked || $opt_since )
{
    $NO_PRINT = 1;
    
    if ( $action eq 'fetch' || $action eq 'print' )
    {
	my $verbing = $action eq 'fetch' ? 'Fetching' : 'Printing';
	my $since = $opt_since ? ' since $opt_since' : '';
	print STDERR "$verbing references unchecked $since from $source:\n\n";
	$rs->select_refs('unfetched');
	$END_LOOP = undef;
    }
    
    elsif ( $action eq 'score' )
    {
	print STDERR "Scoring all unscored references from $source:\n\n";
	$rs->select_refs('unscored');
	$END_LOOP = undef;
    }
    
    elsif ( $action eq 'match' )
    {
	print STDERR "Matching all unmatched references from $source:\n\n";
	$rs->select_refs('unmatched');
	$END_LOOP = undef;
    }
    
    else
    {
	my $opt = $opt_since ? '--since' : '--unchecked';
	die "ERROR: option $opt cannot be used with action '$action'\n";
    }
}

elsif ( $opt_all )
{
    $NO_PRINT = 1;
    
    if ( $action eq 'fetch' || $action eq 'print' )
    {
	my $verbing = $action eq 'fetch' ? 'Fetching' : 'Printing';
	print STDERR "$verbing all references from $source:\n\n";
	$rs->select_refs('all');
	$END_LOOP = undef;
    }

    elsif ( $action eq 'score' || $action eq 'match' )
    {
	my $verbing = $action eq 'score' ? 'Scoring' : 'Matching';
	print STDERR "$verbing all references from $source:\n\n";
	$rs->select_refs('fetched');
	$END_LOOP = undef;
    }

    elsif ( $action eq 'recount' )
    {
	$rs->recount_scores('all');
	exit;
    }
    
    else
    {
	die "ERROR: option --all cannot be used with action '$action'\n";
    }
}

elsif ( ! @REF_LIST )
{
    print STDERR "No references selected.\n";
    exit;
}

# Then loop through either the specified references or all unchecked references. We stop either
# when we have handled all of the available references, or when 10 or more of the last 20 queries
# have failed. This allows us to handle some degree of query failure but stop if almost every
# query is failing.

my $action_count = 0;
my $score_count = 0;
my $success_count = 0;
my $error_count = 0;
my @status_queue;

while ( @REF_LIST || ! $END_LOOP )
{
    my $r;
    
    # If specific reference_no or refsource_no values were specified, loop through the
    # corresponding records and stop. Otherwise, loop as long as we have references that have not
    # been checked.
    
    if ( @REF_LIST )
    {
	$r = shift @REF_LIST;
    }
    
    elsif ( ! $END_LOOP )
    {
	$r = $rs->get_next();
    }
    
    # If we have reached the end of the available records, stop the loop.
    
    if ( ! $r )
    {
	$END_LOOP = 'done';
	last;
    }
    
    # If the action is 'print', print the info for the specified reference. If this is not the
    # first print, add an extra newline first to separate the output records from each other.
    
    elsif ( $action eq 'print' )
    {
	print STDOUT "\n" if $action_count;
	printout_ref($r);
    }
    
    # If the action is 'fetch' or 'test', perform a query on the specified reference source. If
    # this is not the first fetch, sleep for 1 second to make sure that we do not
    # overload the source server.
    
    elsif ( $action eq 'fetch' || $action eq 'test' )
    {
	sleep(1) if $action_count;
	my $status = fetch_ref($rs, $r, $action);
	
	# Keep track of the last 20 status codes. If an error occurs, check to see if at least 10
	# of those are errors and abort if that is the case.
	
	push @status_queue, $status;
	shift @status_queue if scalar(@status_queue) > 20;
	
	if ( $status =~ /^400 Invalid/ )
	{
	    # ignore these
	}
	
	elsif ( $status =~ /^[45]/ )
	{
	    $error_count++;
	    
	    if ( scalar(grep /^[45]/, @status_queue) >= 10 )
	    {
		$END_LOOP = 'errors';
		last;
	    }
	}
	
	else
	{
	    $success_count++;
	}
    }
    
    elsif ( $action eq 'history' )
    {
	my @events = $rs->select_events($r, 'history');
	printout_history($r, \@events);
    }
    
    elsif ( $action eq 'dump' )
    {
	my ($event) = $rs->select_events($r, 'latest');
	my @items = get_event_content($r, $event, $opt_which);
	printout_event_attrs($event);
	
	foreach my $i (@items)
	{
	    printout_item_source($i);
	}
    }
    
    elsif ( $action eq 'show' )
    {
	my ($event) = $rs->select_events($r, 'latest');
	my @items = get_event_content($r, $event, $opt_which);
	printout_event_attrs($event);
	
	foreach my $i (@items)
	{
	    printout_item_formatted($i);
	}
    }
    
    elsif ( $action eq 'score' )
    {
	my ($event) = $rs->select_events($r, 'latest');
	my @items = get_event_content($r, $event, $opt_which);
	printout_event_attrs($event) unless $NO_PRINT;
	printout_item_formatted($r) unless $NO_PRINT;
	my $index = $opt_which eq 'all' ? 0 : $opt_which > 0 ? $opt_which-1 : 0;
	
	foreach my $item (@items)
	{
	    my $scores = ref_similarity($r, $item);
	    printout_item_formatted($item) unless $NO_PRINT;
	    printout_item_scores($scores) unless $NO_PRINT;
	    my $result = $rs->store_scores($event, $scores, $index);
	    $index++;
	    if ( $result ) { $score_count++; }
	    else { $error_count++; }
	}
    }

    elsif ( $action eq 'recount' )
    {
	$rs->recount_scores($r->{reference_no});
    }
    
    elsif ( $action eq 'match' )
    {
	my ($event) = $rs->select_events($r, 'latest');
	# my @items = get_event_content($r, $event, $opt_which);
	# if ( my $f = get_eventdata($r, $event, $opt_which) )
	# {
	#     printout_data($r, $f, 'match') unless $NO_PRINT;
	# }
    }
    
    else
    {
	die "Invalid action: $action\n";
    }
    
    # If we have reached the specified limit on the number of actions, then stop.
    
    if ( $opt_limit && ++$action_count >= $opt_limit )
    {
	$END_LOOP = 'limit';
	last;
    }
}


# If we finished because we processed the records we were asked to process, let the user know we
# are done.

if ( $END_LOOP eq 'no loop' || $END_LOOP eq 'done' || $END_LOOP eq 'limit' )
{
    print STDERR "\nDone.\n";
}

# If we finished for some other reason, let the user know what it was.

elsif ( $END_LOOP eq 'errors' )
{
    print STDERR "\nToo many errors in a short time.\n";
    print STDERR "$status_queue[$_]\n" foreach 0..$#status_queue;
}

elsif ( $END_LOOP eq 'interrupt' )
{
    print STDERR "\nTerminating because of interrupt.\n";
    print STDERR "$status_queue[$_]\n" foreach 0..$#status_queue;
}

# Print out totals, and then exit.

my $elapsed = $starttime - time;
my $h = int($elapsed / 3600);
my $minsec = $elapsed % 3600;
my $m = int($minsec / 60);
my $s = $minsec % 60;

my $timestring = "";

if ( $elapsed > 5 )
{
    if ( $elapsed >= 3600 )
    {
	$timestring = " in $h hours $m minutes";
    }

    elsif ( $elapsed >= 60 )
    {
	$timestring = " in $m minutes $s seconds";
    }

    else
    {
	$timestring = " in $s seconds";
    }
}

print STDERR "Executed $success_count successful queries$timestring.\n" if $success_count;
print STDERR "Stored $score_count scores$timestring.\n" if $score_count;
print STDERR "Got $error_count error responses.\n" if $error_count;
print STDERR "\n";

exit;


sub stop_loop {

    $END_LOOP = 'interrupt';
}

# =============================================
#
# reference actions
#
# =============================================

sub printout_ref {
    
    my ($r) = @_;
    
    my $string = encode_utf8($rs->format_ref($r));
    
    my $score = defined $r->{score} ? " [$r->{score}]" : "";
    
    print STDOUT "$r->{reference_no} :$score $string\n";
}


sub fetch_check {

    if ( rand > 0.45 ) { print STDERR "200 OK\n"; return "200 OK"; }
    else { print STDERR "400 Bad request\n"; return "400 Bad Request"; }
}


sub fetch_ref {

    my ($rs, $r, $action) = @_;
    
    my $string = $rs->format_ref($r);
    
    print STDERR "Fetching refno $r->{reference_no} from $source:\n$string\n\n";
    
    my ($status, $query_text, $query_url, $response_data) = $rs->metadata_query($r, 2);
    
    if ( $action eq 'fetch' && $status && $r->{reference_no} )
    {
	my $result = $rs->store_result($r->{reference_no}, $status,
				       $query_text, $query_url, $response_data);
	
	if ( $result )
	{
	    print STDERR "Result: $status; refsource_no = $result\n\n";
	}
	
	else
	{
	    print STDERR "Result $status; DATABASE ERROR, no record inserted.\n\n";
	    return "500 No record inserted";
	}
    }
    
    elsif ( $status && $r->{reference_no} )
    {
	print STDERR "Result: $status\n\n";
    }
    
    elsif ( $status )
    {
	print STDERR "Query text: $query_text\n\n";
	print STDERR "Result: $status\n\n";
    }
    
    else
    {
	print STDERR "FETCH ERROR, no status returned.\n\n";
    }
    
    if ( $opt_print && $response_data )
    {
	print STDOUT $response_data;
	
	unless ( $response_data =~ /\n$/ )
	{
	    print STDOUT "\n";
	}
    }

    return $status || "500 No status returned";
}


sub printout_event_history {

    my ($r, $eventlist) = @_;
    
    # If there aren't any, print an error message and exit.
    
    unless ( ref $eventlist eq 'ARRAY' && @$eventlist )
    {
	if ( ref $r && $r->{reference_no} )
	{
	    print STDERR "No events found for refno $r->{reference_no}";
	    exit;
	}

	else
	{
	    print STDERR "No refno found";
	    exit;
	}
    }
    
    # Otherwise, print out the results.
    
    my @rows = ['id', 'refno', 'source', 'eventtype', 'eventtime', 'status', 'data'];
    
    foreach my $e ( @$eventlist )
    {
	push @rows, [$e->{refsource_no}, $e->{reference_no}, $e->{source},
		     $e->{eventtype}, $e->{eventtime}, $e->{status}, $e->{data} ? 'yes' : 'no'];
    }
    
    print_table(@rows);
}
    

sub get_event_content {
    
    my ($r, $e, $which) = @_;
    
    # If there isn't any data, print an error message and exit.
    
    unless ( $e && ref $e eq 'HASH' )
    {
	if ( ref $r && $r->{reference_no} )
	{
	    print STDERR "No fetched data found for refno $r->{reference_no}";
	    return;
	}

	else
	{
	    print STDERR "No refno found";
	    return;
	}
    }
    
    my ($data, @items);
    
    unless ( $e->{response_data} )
    {
	print STDERR "ERROR: no response data found in event $e->{refsource_no} ($e->{reference_no})\n";
	return;
    }
    
    eval {
	$data = decode_json($e->{response_data});
    };
    
    if ( $@ )
    {
	print STDERR "An error occurred while decoding \@$e->{refsource_no}: $@\n";
	return;
    }
    
    if ( ref $data eq 'HASH' && ref $data->{message}{items} eq 'ARRAY' )
    {
	@items = @{$data->{message}{items}};
    }

    elsif ( ref $data eq 'ARRAY' && ( $data->[0]{deposited} || $data->[0]{title} ) )
    {
	@items = @$data;
    }

    if ( $which eq 'all' )
    {
	return @items;
    }

    elsif ( $which > 0 )
    {
	return $items[$which-1];
    }

    else
    {
	return $items[0];
    }
}


sub printout_event_attrs {
    
    my ($e) = @_;
    
    my @rows = ['id', 'refno', 'source', 'eventtype', 'eventtime', 'status'];
    
    push @rows, [$e->{refsource_no}, $e->{reference_no}, $e->{source},
		 $e->{eventtype}, $e->{eventtime}, $e->{status}];
    
    print_table(@rows);
    
    print "\n";
}


sub printout_item_formatted {
    
    my ($r) = @_;
    
    print encode_utf8($rs->format_ref($r));
    print "\n\n";
}


sub printout_item_source {
    
    my ($i) = @_;
    
    print JSON->new->pretty->utf8->encode($i);
    print "\n";
}


sub printout_item_scores {
    
    my ($scores) = @_;
    
    foreach my $key ( qw(title pub auth1 auth2 pubyr volume pages pblshr) )
    {
	my $key1 = $key . '_s';
	my $key2 = $key . '_c';
	my $line = sprintf("%-15s %5d %5d\n", $key, $scores->{$key1}, $scores->{$key2});
	
	print $line;
    }
    
    print "\n";
}


sub format_scores_horizontal {

    my ($scores) = @_;
    
    my $line1 = "stat      ";
    my $line2 = "similar   ";
    my $line3 = "conflict  ";

    foreach my $key ( qw(complete count sun title pub auth1 auth2 pubyr volume pages pblshr) )
    {
	my $key1 = $key . '_s';
	my $key2 = $key . '_c';
	$line1 .= fixed_width($key, 10);
	$line2 .= fixed_width($scores->{$key1}, 10);
	$line3 .= fixed_width($scores->{$key2}, 10);
    }

    return "$line1\n\n$line2\n$line3\n";
}


sub fixed_width {
    
    return $_[0] . (' ' x ($_[1] - length($_[0])));
}


sub print_table {
    
    my $options = ref $_[0] eq 'HASH' ? shift @_ : { };
    
    my ($header, @body) = @_;
    
    my $columnpad = $options->{pad} // 5;
    
    my $outfh = $options->{outfh} || *STDOUT;
    
    # If the 'noformat' option was given, just print out lines of tab-separated fields.
    
    # if ( $options->{noformat} )
    # {
    # 	foreach my $i ( 0..$linelimit )
    # 	{
    # 	    print join "\t", map { $_->[$i] // '' } @columns;
    # 	    print "\n";
    # 	}

    # 	return;
    # }
    
    # Otherwise, print formatted output.
    
    my (@width, @entrywidth, @separator, $format);
    
    # Start by computing column widths.

    foreach my $c ( 0..$#$header )
    {
	$width[$c] = string_width($header->[$c]);
	$separator[$c] = '-' x $width[$c];
    }
    
    foreach my $row ( $header, @body )
    {
	foreach my $c ( 0..$#$row )
	{
	    my $this_width = string_width($row->[$c]);
	    if ( ! $width[$c] || $this_width > $width[$c] )
	    {
		$width[$c] = $this_width;
	    }
	}
    }
    
    # # Create a format string.
    
    # $format = '%s';
    
    # foreach my $c ( 0..$#columns )
    # {
    #     my $mod = $options->{format}[$c] && $options->{format}[$c] =~ /R/ ? '' : '-';
    #     $format .= "%$mod$width[$c]s  ";
    # }
    
    # $format =~ s/\s*$/\n/;
    
    # print "format: $format\n" if $DEBUG;
    
    # If we were given a header list, print out the header followed by a separator line.
    
    if ( ref $header eq 'ARRAY' && @$header )
    {
	# print sprintf($format, '', @$header);
	# print sprintf($format, '', @separator);
	
	PrintLine(@$header);
	PrintLine(@separator);
    }
    
    # Print out the data lines.
    
    foreach my $row ( @body )
    {
	# print sprintf($format, '', map { $_->[$i] // '' } @columns);
	PrintLine(@$row);
    }
    
    sub PrintLine {

	my (@fields) = @_;
	
	foreach my $j ( 0..$#fields )
	{
	    my $data = $fields[$j];
	    my $fieldwidth = $width[$j];
	    my $datawidth = string_width($data);
	    my $pad = $datawidth < $fieldwidth ? $fieldwidth - $datawidth : 0;
	    $pad += $columnpad if $j < $#fields;

	    print $outfh $data . (" " x $pad);
	}

	print $outfh "\n";
    }
}


sub string_width {
    
    my ($string) = @_;
    
    return 0 unless defined $string && $string ne '';
    $string =~ s/\033\[[\d;]+m//g;
    return length($string);
}


# # If --fulltitle was specified, do a full text search on each nonempty reftitle.

# elsif ( $opt_fulltitle )
# {
#     read_input();
#     fulltitle_proc();
# }

# # If --fullpub was specified, the same on each nonempty pubtitle.

# elsif ( $opt_fullpub )
# {
#     read_input();
#     fullpub_proc();
# }

# exit;


sub ref_from_args {
    
    my ($arg) = @_;
    
    my $ref = { };
    
    while ( $arg =~ qr{ ^ (\w+) [:] \s* (.*?) (?= \w+ [:] | $) (.*) }xs )
    {
	my $field = $1;
	my $value = $2;
	$arg = $3;
	
	$value =~ s/\s+$//;
	
	if ( $field eq 'author' || $field eq 'au' )
	{
	    my $key = $ref->{author1last} ? 'author2last' : 'author1last';
	    $ref->{$key} = $value;
	}
	
	elsif ( $field eq 'author1' || $field eq 'a1' || $field eq 'author1last' )
	{
	    $ref->{author1last} = $value;
	}
	
	elsif ( $field eq 'author2' || $field eq 'a2' || $field eq 'author2last' )
	{
	    $ref->{author2last} = $value;
	}
	
	elsif ( $field eq 'title' || $field eq 'ti' || $field eq 'reftitle' )
	{
	    $ref->{reftitle} = $value;
	}
	
	elsif ( $field eq 'pub' || $field eq 'pu' || $field eq 'pubtitle' || $field eq 'publication' )
	{
	    $ref->{pubtitle} = $value;
	}
	
	elsif ( $field eq 'pubyr' || $field eq 'py' || $field eq 'year' )
	{
	    $ref->{pubyr} = $value;
	}
	
	elsif ( $field eq 'pubtype' || $field eq 'ty' || $field eq 'type' || $field eq 'publication_type' )
	{
	    $ref->{pubtype} = $value;
	}

	elsif ( $field eq 'label' || $field eq 'lb' )
	{
	    $ref->{label} = $value;
	}
    }
    
    if ( $arg )
    {
	print "WARNING: unparsed remainder '$arg'\n\n";
    }
    
    return $ref;
}


sub ref_from_refno {
    
    my ($dbh, $reference_no) = @_;
    
    return unless $reference_no && $reference_no =~ /^\d+$/;
    
    my $sql = "SELECT * FROM $TABLE{REFERENCE_DATA} WHERE reference_no = $reference_no";

    print STDERR "$sql\n\n" if $opt_debug;
    
    my $result = $dbh->selectrow_hashref($sql);
    
    return $result && $result->{reference_no} ? $result : ();
}


sub ref_from_sourceno {

    my ($dbh, $refsource_no) = @_;

    return unless $refsource_no && $refsource_no =~ /^\d+$/;
    
    my $sql = "SELECT r.*, s.refsource_no
	FROM $TABLE{REFERENCE_DATA} as r join $TABLE{REFERENCE_SOURCES} as s using (reference_no)
	WHERE s.refsource_no = $refsource_no";
    
    print STDERR "$sql\n\n" if $opt_debug;
    
    my $result = $dbh->selectrow_hashref($sql);
    
    return $result && $result->{reference_no} ? $result : ();
}
    

# find_matches ( reference_attrs )
# 
# Return a list of matches for the specified reference attributes in the REFERENCE_DATA (refs)
# table. The attributes must be given as a hashref.

sub ref_match {
    
    my ($dbh, $r) = @_;
    
    my @matches;
    
    # If a doi was given, find all references with that doi. Compare them all to the given
    # attributes; if no other attributes were given, each one gets a score of 90 plus the number
    # of important attributes with a non-empty value. The idea is to select the matching reference
    # record that has the greatest amount of information filled in.
    
    if ( $r->{doi} )
    {
	my $quoted = $dbh->quote($r->{doi});
	
	my $sql = "SELECT * FROM refs WHERE doi = $quoted";
	
	print STDERR "$sql\n\n" if $opt_debug;
	
	my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
	
	@matches = @$result if $result && ref $result eq 'ARRAY';
	
	# Assign match scores.
	
	foreach my $m ( @matches )
	{
	    my $score = match_score($r, $m);

	    $m->{score} = $score;
	}	
    }
    
    # If no doi was given or if no references with that doi were found, look for references that
    # match some combination of reftitle, pubtitle, pubyr, author1last, author2last.
    
    unless ( @matches )
    {
	my $base;
	my $having;

	# If we have a reftitle or a pubtitle, use the refsearch table for full-text matching.
	
	if ( $r->{reftitle} )
	{
	    my $quoted = $dbh->quote($r->{reftitle});
	    
	    $base = "SELECT refs.*, match(refsearch.reftitle) against($quoted) as score
		FROM refs join refsearch using (reference_no)";
	    
	    $having = "score > 5";
	}
	
	elsif ( $r->{pubtitle} )
	{
	    my $quoted = $dbh->quote($r->{pubtitle});
	    
	    $base = "SELECT refs.*, match(refsearch.pubtitle) against($quoted) as score
		FROM refs join refsearch using (reference_no)";
	    
	    $having = "score > 0";
	}
	
	else
	{
	    $base = "SELECT * FROM refs";
	}
	
	# Then add clauses to restrict the selection based on pubyr and author names.
	
	my @clauses;
	
	if ( $r->{pubyr} )
	{
	    my $quoted = $dbh->quote($r->{pubyr});
	    push @clauses, "refs.pubyr = $quoted";
	}
	
	if ( $r->{author1last} && $r->{author2last} )
	{
	    my $quoted1 = $dbh->quote($r->{author1last});
	    my $quoted2 = $dbh->quote($r->{author2last});
	    
	    push @clauses, "(refs.author1last sounds like $quoted1 and refs.author2last sounds like $quoted2)";
	}
	
	elsif ( $r->{author1last} )
	{
	    my $quoted1 = $dbh->quote($r->{author1last});
	    
	    push @clauses, "refs.author1last sounds like $quoted1";
	}

	if ( $r->{anyauthor} )
	{
	    my $quoted1 = $dbh->quote($r->{anyauthor});
	    my $quoted2 = $dbh->quote('%' . $r->{anyauthor} . '%');
	    
	    push @clauses, "(refs.author1last sounds like $quoted1 or refs.author2last sounds like $quoted1 or refs.otherauthors like $quoted2)";
	}
	
	# Now put the pieces together into a single SQL statement and execute it.
	
	my $sql = $base;
	
	if ( @clauses )
	{
	    $sql .= "\n\t\tWHERE " . join(' and ', @clauses);
	}
	
	if ( $having )
	{
	    $sql .= "\n\t\tHAVING $having";
	}
	
	print STDERR "$sql\n\n" if $opt_debug;
	
	my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
	
	# If we get results, look through them and keep any that have even a slight chance of
	# matching.
	
	if ( $result && ref $result eq 'ARRAY' )
	{
	    foreach my $m ( @$result )
	    {
		my $score = match_score($r, $m);
		
		if ( $score > 20 )
		{
		    $m->{score} = $score;
		    push @matches, $m;
		}
	    }
	}
    }
    
    # Now sort the matches in descending order by score.
    
    my @sorted = sort { $b->{score} <=> $a->{score} } @matches;
    
    return @sorted;
}


our ($PCHARS);

sub Progress {

    my ($message) = @_;
    
    if ( $PCHARS )
    {
	print STDOUT chr(8) x $PCHARS;
    }
    
    print STDOUT $message;
    $PCHARS = length($message);
}




	# my $refcount;
	
	# while (<>)
	# {
	#     chomp;
	#     next unless $_ =~ /[[:alnum:]]{3}/;
	    
	#     $refcount++;
	    
	#     my $r = ref_from_args($_);
	    
	#     my @matches = ref_match($dbh, $r);

	#     my $matchcount = scalar(@matches);
	#     my $matchphrase = $matchcount == 1 ? "1 match" : "$matchcount matches";
	    
	#     print "Reference $r: $matchphrase\n";
	    
	#     foreach my $i ( 0..$#matches )
	#     {
	# 	my $m = $matches[$i];
	# 	my $r = $m->{label} || $refcount;
	# 	my $n = $i + 1;
	# 	my $s = $m->{score} || 'no score';
		
	# 	print "  Match $n: [$s]\n\n";
		
	# 	print format_ref($m, '    ') . "\n\n";
	#     }
	# }

	# unless ( $refcount )
	# {
	#     print "You must specify at least one reference either on the command line\nor through standard input.\n\n";
	#     exit(2);
	# }



# sub read_input {
    
#     while (<>)
#     {
# 	$_ =~ s/[\n\r]+$//;
# 	my @cols = split /\t/;
	
# 	unless ( $LINE_NO++ )
# 	{
# 	    /reference_no/ || die "The first line must be a list of field names.\n";
# 	    @FIELD_LIST = @cols;
# 	    next;
# 	}
	
# 	my $r = { };
	
# 	foreach my $i ( 0..$#cols )
# 	{
# 	    $r->{$FIELD_LIST[$i]} = $cols[$i];
# 	}
	
# 	my $reference_no = $r->{reference_no};
	
# 	$REF{$reference_no} = $r;
# 	push @REF_LIST, $reference_no;
	
# 	if ( ! ( $LINE_NO % 100 ) )
# 	{
# 	    Progress($LINE_NO);
# 	}
#     }
    
#     Progress('');
#     print STDOUT "Read $LINE_NO lines.\n";
# }


# sub read_table {

#     my $sql = "SELECT * FROM refsearch";
    
#     my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
#     my $lines;
    
#     if ( $result && @$result )
#     {
# 	foreach my $r ( @$result )
# 	{
# 	    my $reference_no = $r->{reference_no};
# 	    $REF{$reference_no} = $r;
# 	    push @REF_LIST, $reference_no;
	    
# 	    if ( ! ( ++$lines % 100 ) )
# 	    {
# 		Progress($lines);
# 	    }
# 	}
	
# 	Progress('');
# 	print STDOUT "Read $lines lines.\n";
#     }

#     else
#     {
# 	print "No data read.\n";
#     }
# }


# sub fulltitle_proc {
    
#     my $reftitle_sth = $dbh->prepare("
# 	SELECT reference_no, reftitle, match(reftitle) against (?) as score
# 	FROM refsearch HAVING score > 0 ORDER BY score desc LIMIT 3");
    
#     my $refupdate_sth = $dbh->prepare("
# 	UPDATE refsearch SET selfmatch = ?, maxmatch = ?, match_no = ?
# 	WHERE reference_no = ? LIMIT 1");
    
#     my $count;
    
#     foreach my $ref_no ( @REF_LIST )
#     {
# 	$count++;
# 	my $reftitle = $REF{$ref_no}{reftitle};

# 	next if $REF{$ref_no}{maxmatch};
	
# 	if ( $reftitle )
# 	{
# 	    $reftitle_sth->execute($reftitle);
	    
# 	    my $result = $reftitle_sth->fetchall_arrayref({ });
	    
# 	    if ( $result && @$result )
# 	    {
# 		$MATCH{$ref_no} = $result;
		
# 		foreach my $r ( @$result )
# 		{
# 		    if ( $r->{reference_no} eq $ref_no )
# 		    {
# 			$REF{$ref_no}{selfmatch} = $r->{score};
# 		    }

# 		    elsif ( $r->{score} && ( ! $REF{$ref_no}{maxmatch} ||
# 					     $r->{score} > $REF{$ref_no}{maxmatch} ) )
# 		    {
# 			$REF{$ref_no}{maxmatch} = $r->{score};
# 			$REF{$ref_no}{match_no} = $r->{reference_no};
# 		    }

# 		    else
# 		    {
# 			last;
# 		    }
# 		}

# 		my $result = $refupdate_sth->execute($REF{$ref_no}{selfmatch}, $REF{$ref_no}{maxmatch},
# 						     $REF{$ref_no}{match_no}, $ref_no);

# 		my $a = 1; # we can stop here when debugging
# 	    }
# 	}
	
# 	Progress($count) unless $count % 100;
#     }

#     Progress('');
#     print STDOUT "Queried for $count entries.\n";
# }


# sub fullpub_proc {
    
#     my $pubtitle_sth = $dbh->prepare("
# 	SELECT match(pubtitle) against (?) as score
# 	FROM refsearch WHERE reference_no = ?");
    
#     my $refupdate_sth = $dbh->prepare("
# 	UPDATE refsearch SET pselfmatch = ? WHERE reference_no = ? LIMIT 1");
    
#     my $count;
    
#     foreach my $ref_no ( @REF_LIST )
#     {
# 	$count++;
# 	my $pubtitle = $REF{$ref_no}{pubtitle};
	
# 	next if $REF{$ref_no}{pselfmatch};
	
# 	if ( $pubtitle )
# 	{
# 	    $pubtitle_sth->execute($pubtitle, $ref_no);
	    
# 	    my ($score) = $pubtitle_sth->fetchrow_array();
	    
# 	    if ( $score )
# 	    {
# 		$REF{$ref_no}{pselfmatch} = $score;
# 	    }
	    
# 	    my $result = $refupdate_sth->execute($score, $ref_no);
	    
# 	    my $a = 1; # we can stop here when debugging
# 	}
	
# 	Progress($count) unless $count % 100;
#     }
    
#     Progress('');
#     print STDOUT "Queried for $count entries.\n";
# }


# sub match_doi {

#     my ($dbh, $r) = @_;
    
#     if ( $r->{doi} )
#     {
# 	my $quoted = $dbh->quote($r->{doi});
	
# 	my $sql = "SELECT * FROM refs WHERE doi = $quoted";
	
# 	print STDERR "$sql\n\n" if $opt_debug;
	
# 	my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
	
# 	return @$result if $result && ref $result eq 'ARRAY';
#     }
    
#     return;	# otherwise
# }


