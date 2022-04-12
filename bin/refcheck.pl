#!/usr/bin/env perl
# 
# refcheck.pl
# 
# Run some experiments on the refs table.

use strict;

use open ':std', ':encoding(UTF-8)';

use lib 'lib', '../lib';
use Getopt::Long;

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB configData);
use TableDefs qw(%TABLE);
use ReferenceSources;
use ReferenceMatch qw(ref_similarity get_reftitle get_authorname get_pubyr);

# Other modules.

use Carp qw(croak);
use JSON;


# Initial declarations
# --------------------

my $starttime = time;

STDOUT->autoflush(1);

our ($DYNAMIC) = '';

# Configure the terminal I/O. If the debugger is running, add the UTF8 encoding layer to its
# output. Why doesn't "use open :std" do this???

if ( $DB::OUT )
{
    binmode($DB::OUT, ':encoding(UTF8)');
}

# Option and argument handling
# ----------------------------

# If the very first argument is -d, restart this program under the perl debugger. Any
# remaining -d arguments will turn on verbose mode.

if ( $ARGV[0] eq '-d' )
{
    shift @ARGV;
    exec('perl', '-d', $0, @ARGV);
}

# Parse option switches, then the remaining command-line arguments.

Getopt::Long::Configure('require_order');

my ($opt_after, $opt_before, $opt_limit, $opt_full, $opt_crossref, $opt_xdd,
    $opt_good, $opt_bad, $opt_sources, $opt_write_scores, $opt_verbose);

GetOptions("a|after=s" => \$opt_after,
	   "b|before=s" => \$opt_before,
	   "l|limit=i" => \$opt_limit,
	   "g|good=s" => \$opt_good,
	   "b|bad=s" => \$opt_bad,
	   "f|full" => \$opt_full,
	   "C|crossref" => \$opt_crossref,
	   "X|xdd" => \$opt_xdd,
	   "w|write-stats=s" => \$opt_write_scores,
	   "verbose|debug|v|d" => \$opt_verbose);

my ($action, $selector, $modifier, $source, $match_attrs);
my @item_list;
my @REF_LIST;
my @REFSOURCE_LIST;

my %ACCEPTED_ACTION = ( print => 'RS', count => 'RS', history => 'RS',
			fetch => 'R', test => 'R', testrefs => 'R', rescore => 'RS',
		        delete => 'S' );

my $opt_source = $opt_crossref && $opt_xdd ? 'all'
    : $opt_crossref ? 'crossref'
    : $opt_xdd ? 'xdd'
    : 'all';

# The first argument must be an action.

if ( defined $ARGV[0] && $ACCEPTED_ACTION{$ARGV[0]} )
{
    $action = shift @ARGV;
}

elsif ( defined $ARGV[0] )
{
    die "Unrecognized action: $action\n";
}

else
{
    print "No action was specified\n";
    exit;
}

# Now process any remaining arguments.

while ( @ARGV )
{
    # An argument that starts with a dash specifies a limit on the number of results to
    # process. This overrides any limit specified with --limit.

    if ( $ARGV[0] =~ /^-(\d+)$/ )
    {
	$opt_limit = $1;
	shift @ARGV;
    }
    
    # A word ending in a colon indicates a set of reference attributes to either match
    # against the refs table or test a source query. This will eat up all of the remaining
    # arguments.
    
    elsif ( $ARGV[0] =~ /^\w+:/ )
    {
	my $attrs = attrs_from_args(join(' ', @ARGV));
	@ARGV = ();
	
	# Print an error message and exit unless we have a nonempty value for either the
	# doi or at least two of the following attributes.
	
	my $count = 0;
	$count++ if $attrs->{reftitle};
	$count++ if $attrs->{pubtitle};
	$count++ if $attrs->{author1last};
	$count++ if $attrs->{pubyr};
	
	unless ( $attrs->{doi} || $count > 1 )
	{
	    die "ERROR: not enough attributes for a reference match\n";
	}

	# If the action is 'test', add these attributes to @REF_LIST. Otherwise, store
	# them in $match_attrs. The action 'testrefs' can be used to select a set of refs
	# and then test them all against the specified source.
	
	if ( $action eq 'test' )
	{
	    push @REF_LIST, $attrs;
	}
	
	else
	{
	    $match_attrs = $attrs;
	}
    }
    
    # Numeric arguments represent reference_no values unless prefixed by @ in
    # which case they represent refsource_no values.
    
    elsif ( $ARGV[0] =~ /^[@]?\d+$/ )
    {
	push @item_list, $ARGV[0];
	shift @ARGV;
    }
    
    # A selector specifies a class of references or sources to scan. Only one is allowed.
    
    elsif ( $ARGV[0] =~ qr { ^ (?: all | (?:un)? (?: fetched|checked|scored|matched )) $ }xsi )
    {
	die "You may only specify one selector: '$selector' or '$ARGV[0]'\n"
	    if $selector && $selector ne lc $ARGV[0];
	
	$selector = lc shift @ARGV;
    }
    
    # A modifier specifies which kinds of records to work on. Only one is allowed.
    
    elsif ( $ARGV[0] =~ qr{ ^ (?: refs?|sources?|latest|both|full ) $ }xsi )
    {
	my $newmod = lc shift @ARGV;
	$newmod =~ s/s$//;
	
	die "You may only specify one modifier: '$modifier' or '$ARGV[0]'\n"
	    if $modifier && $modifier ne $newmod;
	
	$modifier = $newmod;
    }
    
    # A source argument specifies which source(s) to query.

    elsif ( $ARGV[0] =~ /^(crossref|cross|cr)$/ )
    {
	$source = ($source && $source eq 'xdd') ? 'all' : 'crossref';
	shift @ARGV;
    }
    
    elsif ( $ARGV[0] =~ /^(xdd)$/ )
    {
	$source = ($source && $source eq 'crossref') ? 'all' : 'xdd';
	shift @ARGV;
    }
    
    # Warn about any invalid arguments.
    
    else
    {
	print STDERR "WARNING: ignored argument '$ARGV[0]'\n";
	shift @ARGV;
    }
}

# If a source was specified in the arguments, it overrides the options. Otherwise, use the
# source from the options or default to 'all'.

$source ||= $opt_source;

# If a limit was specified, add it to the options to be used for selection. Also add
# 'good' and 'bad' if specified.

my $options = { };
$options->{limit} = $opt_limit if defined $opt_limit && $opt_limit ne '';
$options->{good} = $opt_good if $opt_good;
$options->{bad} = $opt_bad if $opt_bad;

# If no selector was specified and no limit was specified, set a default limit of 5 and
# change the selector to 'all'.

if ( ! $selector && ! defined $options->{limit} )
{
    $options->{limit} = 5;
    $selector = 'all';
}

# If the action was 'testrefs', we can now reset it to 'test' because argument processing
# is complete.

if ( $action eq 'testrefs' )
{
    $action = 'test';
}

# If the action was count, individual reference and source numbers are ignored.

if ( $action eq 'count' && @item_list )
{
    print STDERR "WARNING: individual reference or source identifiers will be ignored\n";
}


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
    print STDERR "Connected to database: $1\n\n" if $opt_verbose;
}

else
{
    print STDERR "Using connect string: $dbh->{Name}\n\n" if $opt_verbose;
}


# Select references to process
# ----------------------------

# Create a new ReferenceSources object with which we can make queries and interact with the
# REFERENCE_SOURCES table.

my $rs = ReferenceSources->new($dbh, $source, { debug => $opt_verbose });

# The following variables control processing of references. If a list of references is given
# either by explicitly identifying them or by matching against a set of attributes, the matching
# reference data is stored in @REF_LIST. If we are looping through the entire reference table, the
# $IN_LOOP variable controls that process.

my $PRINT = 0;
my $END_LOOP = 'no loop';
my $STOP;

$SIG{INT} = \&stop_loop;

# If specific records were selected for processing, add them to the proper lists.

foreach my $item ( @item_list )
{
    # For each refsource_no specified, if the action type is 'R' then fetch the reference
    # attributes from the corresponding entry in the refs table and add an entry to
    # @REF_LIST. If the action type is 'RS' then add the refsource_no value to @REFSOURCE_LIST.
    
    if ( $item =~ /^[@](\d+)$/ )
    {
	my $refsource_no = $1;
	
	if ( $ACCEPTED_ACTION{$action} eq 'R' )
	{
	    push @REF_LIST, $rs->ref_from_sourceno($refsource_no);
	}

	else
	{
	    push @REFSOURCE_LIST, $refsource_no;
	}
    }
    
    # For each reference_no value specified, fetch the reference attributes from the
    # corresponding entry in the refs table and add an entry to @REF_LIST.
    
    elsif ( $ACCEPTED_ACTION{$action} ne 'S' )
    {
	push @REF_LIST, $rs->ref_from_refno($item);
    }
}

# If the action is 'count', then count the matching references or sources instead of
# selecting them. The program terminates here in that case.

if ( $action eq 'count' )
{
    my $count;
    
    if ( $match_attrs )
    {
	
    }

    elsif ( $selector && ($modifier eq 'source' || $modifier eq 'full') )
    {
	$count = $rs->count_sources($selector, '', $options);
	print "$count reference sources\n\n";
    }
    
    elsif ( $selector )
    {
	$count = $rs->count_refs($selector, '', $options);
	print "$count references\n\n";
    }
    
    else
    {
	print "Nothing to count\n\n";
    }

    exit;
}

# Otherwise, if we have a set of attributes to match, do so now.

if ( $match_attrs )
{
    $rs->select_matching_refs($selector, $match_attrs, $options);
    $END_LOOP = undef;
}

# Otherwise, if a selector was specified then select the corresponding references.

elsif ( $selector )
{
    $rs->select_refs($selector, '', $options);
    $END_LOOP = undef;
}

# Make sure we have something to work on.

unless ( $rs->selection_count || @REF_LIST || @REFSOURCE_LIST )
{
    print STDERR "No matching references found\n\n";
    exit;
}


# Main Loop
# ---------

# Now loop through any references or reference source events that were explicitly
# specified, followed by any that were selected.

my $action_count = 0;
my $success_count = 0;
my $insert_count = 0;
my $update_count = 0;
my $error_count = 0;
my $notfound_count = 0;
my $request_count = 0;
my $score_count = 0;
my $scins_count = 0;
my $scupd_count = 0;
my @status_queue;

while ( @REFSOURCE_LIST || @REF_LIST || ! $END_LOOP )
{
    my ($r, $refsource_no);
    
    # If specific reference_no or refsource_no values were specified, loop through the
    # corresponding records and stop. Otherwise, loop as long as we have references that have not
    # been checked.
    
    if ( @REFSOURCE_LIST )
    {
	$refsource_no = shift @REFSOURCE_LIST;
	$PRINT = 1;
    }
    
    elsif ( @REF_LIST )
    {
	$r = shift @REF_LIST;
	$PRINT = 1;
    }
    
    elsif ( ! $END_LOOP )
    {
	unless ( $r = $rs->get_next() )
	{
	    $END_LOOP = 'done';
	    last;
	}
	
	$PRINT = 0;
    }
    
    # If we got an interrupt, drop into the debugger here.
    
    if ( $STOP )
    {
	$STOP = undef;
	$DB::single = 1;
    }
    
    # If the action is 'print', print the specified reference formatted into a text string.
    
    if ( $action eq 'print' )
    {
	print STDOUT "Reference # $r->{reference_no}\n\n";
	print STDOUT $rs->format_ref($r) . "\n\n";
	$action_count++;
    }
    
    # If the action is 'fetch' or 'test', perform a query on the specified reference source. If
    # this is not the first fetch, sleep for 1 second to make sure that we do not
    # overload the source server.
    
    elsif ( $action eq 'fetch' || $action eq 'test' )
    {
	sleep(1) if $action_count && $source ne 'all';
	fetch_ref($rs, $r, $action);
	$action_count++;
	
	if ( $action eq 'fetch' && $action_count % 10 == 0 ) 
    	{
    	    show_dynamic_count($action_count, $error_count, $notfound_count, $rs->selection_count);
    	}
    }
    
    elsif ( $action eq 'history' || $action eq 'dump' )
    {
	my $which = $r || { refsource_no => $refsource_no };
	my $evsel;
	
	my $label = $refsource_no ? "source @ $refsource_no"
	    : $r->{reference_no} ? "ref # $r->{reference_no}"
	    : "ref # unknown";
	
	if ( $action eq 'dump' )
	{
	    $evsel = $opt_full ? 'full' : 'latest';
	}

	else
	{
	    $evsel = 'history';
	}
	
	my @events = $rs->list_events($which, $source, $evsel);
	printout_event_history($label, $evsel, \@events);
    }
}

if ( $DYNAMIC ) { print "\n\n"; }

# If we finished because we processed the records we were asked to process, let the user know we
# are done.

if ( $END_LOOP eq 'done' || $END_LOOP eq 'limit' )
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

my $elapsed = time - $starttime;
my $h = int($elapsed / 3600);
my $minsec = $elapsed % 3600;
my $m = int($minsec / 60);
my $s = $minsec % 60;

my $timestring = " in $elapsed seconds";

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

if ( $success_count )
{
    print STDERR "Executed $success_count successful queries$timestring\n";

    if ( $insert_count || $update_count )
    {
	my @stmts; push @stmts, "inserted: $insert_count" if $insert_count;
	push @stmts, "updated: $update_count" if $update_count;
	
	print STDERR "Records " . join(', ', @stmts) . "\n";
    }

    else
    {
	print STDERR "Records stored: 0\n";
    }
    
    print STDERR "Errors: $error_count\n" if $error_count;
    print STDERR "No match: $notfound_count\n" if $notfound_count;
    print STDERR "Total requests: $request_count\n\n";

    if ( $score_count )
    {
	my @stmts; push @stmts, "$scins_count inserted" if $scins_count;
	push @stmts, "$scupd_count updated" if $scupd_count;
	print STDERR "Generated scores: $score_count   " . join(', ', @stmts) . "\n";
    }
}

elsif ( $notfound_count )
{
    print STDERR "No successful queries, $notfound_count not matched$timestring\n";
    print STDERR "Errors: $error_count\n" if $error_count;
    print STDERR "Total requests: $request_count\n\n";
}

elsif ( $score_count )
{
    my @stmts; push @stmts, "$scins_count inserted" if $scins_count;
    push @stmts, "$scupd_count updated" if $scupd_count;
    print STDERR "Generated scores: $score_count$timestring   " . join(', ', @stmts) . "\n\n";
}

elsif ( $error_count )
{
    print STDERR "Got $error_count errors$timestring\n\n";
    print STDERR "Total requests: $request_count\n\n";
}

if ( $score_count && $opt_write_scores )
{
    write_scores($opt_write_scores) || write_scores('/tmp/score_stats.txt');
}

exit;


# Extra subroutines
# =================

# stop_loop ( )
#
# This is called when an interrupt is received. If the debugger is active, drop into it
# early in the next iteration of the main loop. If a second interrupt is received, drop
# into the debugger right away. If the debugger is not active, terminate the main loop
# after the current iteration.

sub stop_loop {

    if ( $DB::OUT )
    {
	$DB::single = 1 if $STOP;
	$STOP = 1;
    }
    
    else
    {
	exit if $END_LOOP eq 'interrupt';
	$END_LOOP = 'interrupt';
    }
}


# show_dynamic_count ( actions, errors, notfound, total )
#
# Display a running total of the number of actions completed, along with a count of errors
# and records not found.

sub show_dynamic_count {
    
    my ($action_count, $error_count, $notfound_count, $total) = @_;
    
    clear_dynamic_count();
    
    if ( $error_count || $notfound_count )
    {
	$DYNAMIC = "$action_count out of $total    ($error_count errors, $notfound_count not found)";
    }
    
    else
    {
	$DYNAMIC = "$action_count out of $total";
    }
    
    print $DYNAMIC;
}


# clear_dynamic_count
#
# Clear the running total, if any.

sub clear_dynamic_count {
    
    print chr(8) x length($DYNAMIC) if $DYNAMIC;
    $DYNAMIC = '';
}


# write_scores ( filename )
#
# Write debugging statistics about score generation to the specified file.

sub write_scores {

    my ($filename) = @_;

    return if $filename =~ /^-|^1$/;

    my $outfh;

    unless ( open my $outfh, ">", $filename )
    {
	print STDERR "ERROR writing $filename: $!\n";
	return;
    }
    
    print $outfh "Counts for score debugging from $score_count scores:\n\n";
    
    foreach my $k ( sort keys %ReferenceMatch::COUNT )
    {
	print " $k: $ReferenceMatch::COUNT{$k}\n";
    }
    
    print "\n";
    
    if ( close $outfh )
    {
	print STDERR "Wrote score debugging stats to $filename.\n\n";
	return 1;
    }
    
    else
    {
	print STDERR "Error writing $filename: $!\n";
	return;
    }
}


# =============================================
#
# reference actions
#
# =============================================

sub fetch_check {

    if ( rand > 0.45 ) { print STDERR "200 OK\n"; return "200 OK"; }
    else { print STDERR "400 Bad request\n"; return "400 Bad Request"; }
}


# fetch_ref ( rs, reference, action )
#
# If the action is 'fetch', then make a query on the external source associated with $rs
# that tries to match the reference attributes in $r. If the query is successful, store
# the result.
#
# If the action is 'test', fetch the data but print it instead of storing it.

sub fetch_ref {

    my ($rs, $r, $action) = @_;
    
    my $result;
    my $fetches;
    
    my @sources = $rs->source;

    if ( $sources[0] eq 'all' )
    {
	@sources = ('xdd', 'crossref');
    }
    
    foreach my $source ( @sources )
    {    
	if ( $action eq 'fetch' )
	{
	    my $refno = $r->{reference_no};
	    
	    croak "ERROR: no reference_no given for 'fetch_ref'" unless $refno;
	    
	    print STDERR "Fetching ref # $refno from $source:\n" if $opt_verbose;
	    
	    $result = $rs->metadata_query($r, $source);
	    
	    my ($outcome, $res) = $rs->store_source_data($refno, $result);

	    if ( $outcome eq 'inserted' || $outcome eq 'updated' )
	    {
		$success_count++ if $result->{success};
		$insert_count++ if $outcome eq 'inserted';
		$update_count++ if $outcome eq 'updated';
		$error_count++ if $result->{error};
		$notfound_count++ if $result->{notfound};
		$request_count += $result->{request_count};

		if ( $result->{scores} && defined $result->{scores}{sum_s} &&
		     $res > 0 && $refno > 0 && $source )
		{
		    my $formatted_ref = $rs->format_ref($result->{item});

		    my $attrs = { refsource_no => $res,
				  reference_no => $refno,
				  source => $source };
		    
		    my ($outcome2, $res2) = $rs->store_score_data( $attrs, $result->{scores},
								   $formatted_ref );
		    
		    if ( $outcome2 eq 'inserted' )
		    {
			$score_count++;
			$scins_count++;
		    }

		    elsif ( $outcome2 eq 'updated' )
		    {
			$score_count++;
			$scupd_count++;
		    }
		    
		    elsif ( $outcome2 eq 'error' )
		    {
			print STDERR "ERROR: storing scores for ref # $refno: $res2\n\n";
			$error_count++;
		    }
		}
		
		elsif ( $result->{scores} && defined $result->{scores}{sum_s} )
		{
		    print STDERR "ERROR: cannot store scores for ref # $refno: missing attributes\n\n";
		    $error_count++;
		}
	    }
	    
	    else
	    {
		print STDERR "ERROR: storing source data for ref # $refno: $res\n\n";
		$error_count++;
	    }
	}
	
	elsif ( $action eq 'test' )
	{
	    if ( $r->{reference_no} )
	    {
		print "Fetching refno $r->{reference_no} from $source:\n\n";
	    }
	    
	    else
	    {
		my $label = get_reftitle($r) || join(', ', get_authorname($r, 1));
		my $pubyr = get_pubyr($r);
		
		$label .= " ($pubyr)" if $pubyr;
		
		print "Fetching $label from $source:\n\n";
	    }
	    
	    $result = $rs->metadata_query($r, $source);
	    
	    print "Status: $result->{status}\n";
	    print "URL: $result->{query_url}\n" if $result->{query_url};
	    print "Query: $result->{query_text}\n" if $result->{query_text};

	    if ( $result->{item} )
	    {
		my $encoded = $ReferenceSources::JSON_ENCODER->encode($result->{item});
		print "\nMatching record:\n$encoded\n";
	    }

	    if ( $result->{scores} )
	    {
		print "\n" . format_scores_horizontal($result->{scores});
	    }
	    
	    print "\n";
	}
	
	else
	{
	    croak "ERROR: invalid action '$action' for 'fetch_ref'";
	}
	
	$success_count++ if $result->{success};
	$error_count++ if $result->{error};
	$notfound_count++ if $result->{notfound};
	$request_count += $result->{request_count};
	
	# If the result status is 5xx, abort. This means we tried several times and are not
	# getting a good response from the server.
	
	if ( $result->{status} =~ /^5/ )
	{
	    $END_LOOP = 'errors';
	}
    }
}


sub printout_event_history {

    my ($r, $selector, $eventlist) = @_;
    
    # If there aren't any reference source events associated with this reference, print
    # out a message to that effect.
    
    unless ( ref $eventlist eq 'ARRAY' && @$eventlist )
    {
	if ( ref $r && $r->{reference_no} )
	{
	    print STDOUT "No events found for ref # $r->{reference_no}\n\n";
	    return;
	}
	
	else
	{
	    print STDOUT "No ref # found\n\n";
	    return;
	}
    }
    
    # Otherwise, print out the results.
    
    my $encoder = JSON->new->indent->space_after->canonical;
    
    my @rows = ['id', 'refno', 'source', 'eventtype', 'eventtime', 'status', 'data'];
    
    foreach my $e ( @$eventlist )
    {
	push @rows, [$e->{refsource_no}, $e->{reference_no}, $e->{source},
		     $e->{eventtype}, $e->{eventtime}, $e->{status}, $e->{data} ? 'yes' : 'no'];
	
	if ( $selector eq 'latest' || $selector eq 'full' )
	{
	    my $decoded;
	    
	    eval {
		$decoded = from_json($e->{data});
	    };
	    
	    if ( $@ )
	    {
		push @rows, "$e->{data}\n\n$@\n\n"; 
	    }

	    else
	    {
		push @rows, $encoder->encode($decoded);
	    }
	}
    }
    
    print_table(@rows);
    
    print "\n" if ref $rows[-1] eq 'ARRAY';
}


sub format_scores_horizontal {

    my ($scores) = @_;
    
    my $line1 = "stat      ";
    my $line2 = "similar   ";
    my $line3 = "conflict  ";

    foreach my $key ( qw(complete count sum title pub auth1 auth2 pubyr volume pages pblshr) )
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
	next unless ref $row eq 'ARRAY';
	
	foreach my $c ( 0..$#$row )
	{
	    my $this_width = string_width($row->[$c]);
	    if ( ! $width[$c] || $this_width > $width[$c] )
	    {
		$width[$c] = $this_width;
	    }
	}
    }
    
    if ( ref $header eq 'ARRAY' && @$header )
    {
	PrintLine(@$header);
	PrintLine(@separator);
    }
    
    # Print out the data lines. Any rows that are not arrayrefs are printed out if they
    # are strings, or ignored if they are other kinds of refs.
    
    foreach my $row ( @body )
    {
	if ( ref $row eq 'ARRAY' )
	{
	    PrintLine(@$row);
	}

	elsif ( ! ref $row )
	{
	    print $outfh "\n" unless substr($row, 0, 1) eq "\n";
	    print $outfh $row;
	    print $outfh "\n" unless substr($row, -2, 2) eq "\n\n";
	    print $outfh "\n" unless substr($row, -1, 1) eq "\n";
	}
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


sub attrs_from_args {
    
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

	else
	{
	    print STDERR "WARNING: unrecognized field '$field'\n\n";
	}
    }
    
    if ( $arg )
    {
	print STDERR "WARNING: unparsed remainder '$arg'\n\n";
    }
    
    return $ref;
}


