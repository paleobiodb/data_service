# The Paleobiology Database
# 
#   RefCheck.pm
# 
# Subroutines for working with external sources of bibliographic reference data.
# 

package ReferenceSources;

use strict;

use feature 'unicode_strings';

use Carp qw(croak);
use LWP::UserAgent;
use URI::Escape;
use JSON;
use Encode;
use Scalar::Util qw(reftype blessed);

use TableDefs qw(%TABLE);
use CoreTableDefs;
use ReferenceMatch qw(get_reftitle get_pubtitle get_publisher get_authorname
		      get_pubyr get_doi title_words ref_similarity @SCORE_VARS);

our ($UA);

our ($CROSSREF_BASE) = "https://api.crossref.org/works";
our ($XDD_BASE) = "https://xdd.wisc.edu/api/articles";

our ($JSON_ENCODER) = JSON->new->canonical;

our ($GOOD_MATCH_MIN) = 500;
our ($PARTIAL_MATCH_MIN) = 300;

# Constructor
# -----------

# new ( dbh, source, options )
# 
# Generate a new object with which to make reference data queries. The first argument must be a
# database handle, and the second must be the name of one of the sources we know how to query.
#
# available options are:
# 
# debug		if true, print debugging messages

sub new {

    my ($class, $dbh, $source, $options) = @_;
    
    # Create a new object that can be used to fetch data from the specified source. This source
    # must come from the set of sources known to this module. Currently 'crossref' and 'xdd' are the
    # only allowed sources. If no source is specified, it defaults to 'all'.
    
    $source ||= 'all';
    
    croak "unknown reference source '$source'"
	unless $source eq 'crossref' || $source eq 'xdd' || $source eq 'all';
    
    my $datasource = { dbh => $dbh,
		       source => $source };
    
    # Turn on auto_reconnect.
    
    $dbh->{mariadb_auto_reconnect} = 1;
    
    # Create a LWP::UserAgent object to use for making queries.
    
    $datasource->{ua} =
	LWP::UserAgent->new(timeout => 30,
			    agent => 'Paleobiology Database/1.0 (mailto:mmcclenn@geology.wisc.edu); ');
    
    # If a third option was given, it should be a hash of option values.
    
    if ( $options && ref $options eq 'HASH' )
    {
	$datasource->{debug} = 1 if $options->{debug};
	$datasource->{since} = $options->{since} if $options->{since};
	$datasource->{limit} = $options->{limit} if $options->{limit};
    }
    
    bless $datasource, $class;

    return $datasource;
}


# Attribute methods
# -----------------

sub debug {
    
    my ($rs, $debug) = @_;
    
    $rs->{debug} = $debug if defined $debug;
    return $rs->{debug};
}


sub source {
    
    my ($rs, $source) = @_;
    
    $rs->{source} = $source if defined $source;
    return $rs->{source};
}


sub dbh {
    
    my ($rs, $dbh) = @_;
    
    $rs->{dbh} = $dbh if defined $dbh;
    return $rs->{dbh};
}


# Methods for selecting local records
# -----------------------------------

# select_refs ( selector, filter, options )
# 
# Select all references that meet a specified criterion, and store the statement handle
# where it can be read by `get_next`. Available values for $selector are:
# 
#   fetched, unfetched, checked, unchecked, scored, unscored, matched, unmatched
# 
# If $filter is specified, it must be a valid SQL expression. This is conjoined to the WHERE
# clause in the SELECT statement. It may reference the 'refs' table as 'r', the REFERENCE_SOURCES
# table as 's', and the REFERENCE_SCORES table as 'sc'.
# 
# If $options is specified, it must be a hashref. Accepted options are 'limit', 'offset'.

sub select_refs {
    
    my ($ds, $selector, $filter_clause, $options) = @_;
    
    $options ||= { };
    
    $ds->clear_selection;
    
    my $fields = "SQL_CALC_FOUND_ROWS r.*";
    
    my $sql = $ds->generate_ref_base($selector, $fields, $filter_clause, $options);
    
    return $ds->select_records($sql, 'sth', $options);
}


sub count_refs {

    my ($ds, $selector, $filter_clause, $options) = @_;

    $options ||= { };
    
    my $sql = $ds->generate_ref_base($selector, 'count(distinct r.reference_no)',
				     $filter_clause, $options);
    
    return $ds->count_records($selector, $sql, $options);
}


sub select_matching_refs {
    
    my ($ds, $selector, $attrs, $options) = @_;
    
    $ds->clear_selection;

    my $fields = 'r.*';
    my $dbh = $ds->{dbh};
    
    my @matches;
    
    # If a doi was given, find all references with that doi. Compare them all to the given
    # attributes; if no other attributes were given, each one gets a score of 90 plus the number
    # of important attributes with a non-empty value. The idea is to select the matching reference
    # record that has the greatest amount of information filled in.
    
    if ( $attrs->{doi} )
    {
	my $quoted = $dbh->quote($attrs->{doi});
	my $filter = "doi=$quoted";
	$filter .= " and ($attrs->{filter})" if $attrs->{filter};
	
	my $sql = $ds->generate_ref_base($selector, $fields, $filter, $options);

	push @matches, $ds->select_records($sql, 'return', $options);
	
	# Assign match scores.
	
	foreach my $m ( @matches )
	{
	    my $score = 90;
	    $score++ if $m->{reftitle};
	    $score++ if $m->{pubtitle};
	    $score++ if $m->{author1last};
	    $score++ if $m->{author2last};
	    $score++ if $m->{pubvol};
	    $score++ if $m->{pubno};
	    $score++ if $m->{firstpage};
	    $score++ if $m->{lastpage};
	    
	    $m->{score} = $score;
	}
    }
    
    # If no doi was given or if no references with that doi were found, look for references that
    # match some combination of reftitle, pubtitle, pubyr, author1last, author2last.
    
    unless ( @matches )
    {
	my $having;

	# If we have a reftitle or a pubtitle, use the refsearch table for full-text matching.
	
	if ( $attrs->{reftitle} && $attrs->{pubtitle} )
	{
	    my $refquoted = $dbh->quote($attrs->{reftitle});
	    my $pubquoted = $dbh->quote($attrs->{pubtitle});

	    $fields = "r.*, match(refsearch.reftitle) against($refquoted) as score1,
		  match(refsearch.pubtitle) against ($pubquoted) as score2";
	    $having = "score1 > 5 and score2 > 5";
	}
	
	elsif ( $attrs->{reftitle} )
	{
	    my $quoted = $dbh->quote($attrs->{reftitle});

	    $fields = "r.*, match(refsearch.reftitle) against($quoted) as score";
	    $having = "score > 5";
	}
	
	elsif ( $attrs->{pubtitle} )
	{
	    my $quoted = $dbh->quote($attrs->{pubtitle});
	    
	    $fields = "r.*, match(refsearch.pubtitle) against($quoted) as score";
	    $having = "score > 0";
	}
	
	# Then add clauses to restrict the selection based on pubyr and author names.
	
	my @clauses;
	
	if ( $attrs->{pubyr} )
	{
	    my $quoted = $dbh->quote($attrs->{pubyr});
	    push @clauses, "refs.pubyr = $quoted";
	}
	
	if ( $attrs->{author1last} && $attrs->{author2last} )
	{
	    my $quoted1 = $dbh->quote($attrs->{author1last});
	    my $quoted2 = $dbh->quote($attrs->{author2last});
	    
	    push @clauses, "(refs.author1last sounds like $quoted1 and " .
		"refs.author2last sounds like $quoted2)";
	}
	
	elsif ( $attrs->{author1last} )
	{
	    my $quoted1 = $dbh->quote($attrs->{author1last});
	    
	    push @clauses, "refs.author1last sounds like $quoted1";
	}

	if ( $attrs->{anyauthor} )
	{
	    my $quoted1 = $dbh->quote($attrs->{anyauthor});
	    my $quoted2 = $dbh->quote('%' . $attrs->{anyauthor} . '%');
	    
	    push @clauses, "(refs.author1last sounds like $quoted1 or " .
		"refs.author2last sounds like $quoted1 or refs.otherauthors like $quoted2)";
	}
	
	# Now put the pieces together into a single SQL statement and execute it.

	push @clauses, "($attrs->{filter})" if $attrs->{filter};

	my $filter = join(' and ', @clauses);

	my $sql = $ds->generate_ref_base($selector, $fields, $filter, $options);
	
	$sql .= "\n\tHAVING $having" if $having;
	
	my @other, $ds->select_records($sql, 'return', $options);
	
	$ds->{selection_sql} = $sql;
	
	# If we get results, look through them and keep any that have even a slight chance of
	# matching.
	
	foreach my $m ( @other )
	{
	    if ( $m->{score1} || $m->{score2} )
	    {
		$m->{score} = $m->{score1} + $m->{score2};
	    }
	    
	    push @matches, $m;
	}
    }
    
    # Now sort the matches in descending order by score.
    
    my @sorted = sort { $b->{score} <=> $a->{score} } @matches;

    $ds->{selection_list} = \@sorted;
    $ds->{selection_count} = scalar(@sorted);
}


sub generate_ref_base {

    my ($ds, $selector, $fields, $filter_clause, $options) = @_;
    
    my $filter = $ds->generate_recordfilter($filter_clause, $selector, $options);
    my $sourcefilter = $ds->generate_sourcefilter($selector, $options);
    my $scorefilter = $ds->generate_scorefilter($selector, $options);
    
    my $mainjoin = "$TABLE{REFERENCE_DATA} as r";
    $mainjoin .= " join $TABLE{REFERENCE_SEARCH} as ft using (reference_no)"
	if $options->{refsearch};

    my $groupby = "\n\tGROUP BY r.reference_no";
    $groupby = "" if $fields =~ /^count[(]/;
    
    my $sql;
    
    if ( $selector eq 'all' )
    {
	$sql = "SELECT $fields
	FROM $mainjoin
	WHERE 1=1 $filter";
    }
    
    elsif ( $selector eq 'unfetched' )
    {
	$sql = "SELECT $fields
	FROM $mainjoin left join $TABLE{REFERENCE_SOURCES} as s
	    on s.reference_no = r.reference_no $sourcefilter and s.eventtype = 'fetch'
	    and s.status rlike '^2..'
	WHERE s.reference_no is null $filter";
    }
    
    elsif ( $selector eq 'fetched' )
    {
	$sql = "SELECT $fields
	FROM $mainjoin join $TABLE{REFERENCE_SOURCES} as s
	    on s.reference_no = r.reference_no $sourcefilter
	WHERE s.eventtype = 'fetch' and s.status rlike '^2..' $filter $groupby";
    }
    
    elsif ( $selector eq 'unchecked' )
    {
	$sql = "SELECT $fields
	FROM $mainjoin left join $TABLE{REFERENCE_SOURCES} as s
	    on s.reference_no = r.reference_no $sourcefilter and s.eventtype = 'fetch'
	WHERE s.reference_no is null $filter";
    }

    elsif ( $selector eq 'checked' )
    {
	$sql = "SELECT $fields
	FROM $mainjoin join $TABLE{REFERENCE_SOURCES} as s
	    on s.reference_no = r.reference_no $sourcefilter
	WHERE s.eventtype = 'fetch' $filter $groupby";
    }
    
    elsif ( $selector eq 'unscored' || $selector eq 'unmatched' )
    {
	$sql = "SELECT $fields
	FROM $mainjoin left join $TABLE{REFERENCE_SCORES} as sc
	    on sc.reference_no = r.reference_no $sourcefilter
    	WHERE (sc.reference_no is null or not($scorefilter)) $filter $groupby";
    }
    
    elsif ( $selector eq 'scored' || $selector eq 'matched' )
    {
	$sql = "SELECT $fields
	FROM $mainjoin join $TABLE{REFERENCE_SCORES} as sc
	    on sc.reference_no = r.reference_no $sourcefilter
    	WHERE ($scorefilter) $filter $groupby";
    }
    
    else
    {
	croak "ERROR: unknown selector '$selector'\n";
    }

    return $sql;
}


# select_sources ( selector, filter, options )
# 
# Select all the reference source/score records matching the specified SQL expression, and
# store the statement handle where it can be read by `get_next`. If $show includes
# 'response', include the response_data field. Otherwise, leave it out.
#
# If $options is specified, it must be hashref. Accepted options are 'offset', 'limit',
# 'include', 'all'.

sub select_sources {
    
    my ($ds, $selector, $filter_clause, $options) = @_;

    $options ||= { };

    if ( $options->{all} )
    {
	$ds->clear_selection;
    }

    else
    {
	$ds->clear_selection('latest');
    }
    
    my $fields = 'SQL_CALC_FOUND_ROWS s.refsource_no, s.source, s.reference_no, ' .
	's.status, s.eventtype, s.eventtime';
    
    if ( $options->{include} )
    {
	my $include = $options->{include};
	
	if ( $include =~ /query/ )
	{
	    $fields .= ', s.eventtype, s.eventtime, s.query_url, s.query_text';
	}
	
	if ( $include =~ /data/ )
	{
	    $fields .= ', s.response_data';
	}
	
	if ( $include =~ /ref/ )
	{
	    $fields .= ', r.*';
	}
	
	if ( $include =~ /score/ )
	{
	    $fields .= ', sc.*';
	}
    }
    
    my $sql = $ds->generate_source_base($selector, $fields, $filter_clause, $options);
    
    $sql .= " ORDER BY s.reference_no, s.eventtime desc";
    
    return $ds->select_records($sql, 'sth', $options);
}


sub count_sources {

    my ($ds, $selector, $filter_clause, $options) = @_;

    $options ||= { };
    
    my $sql = $ds->generate_source_base($selector, 'count(*)', $filter_clause, $options);
    
    return $ds->count_records($selector, $sql, $options);
}


sub generate_source_base {
    
    my ($ds, $selector, $fields, $filter_clause, $options) = @_;
    
    my $filter = $ds->generate_recordfilter($filter_clause, $selector, $options);
    my $sourcefilter = $ds->generate_sourcefilter($selector, $options);
    my $scorefilter = $ds->generate_scorefilter($selector, $options);
    my $refjoin = '';
    my $scorejoin = '';
    
    if ( $options->{include} =~ /ref/ || $filter_clause =~ /\br[.]/ )
    {
	$refjoin = "left join $TABLE{REFERENCE_DATA} as r using (reference_no)";
    }

    if ( $options->{include} =~ /score/ || $filter_clause =~ /\bsc[.]/ )
    {
	$scorejoin = "left join $TABLE{REFERENCE_SCORES} as sc using (refsource_no)";
    }
    
    my $sql;
    
    if ( $selector eq 'all' || $selector eq 'checked' )
    {
	$sql = "SELECT $fields
	FROM $TABLE{REFERENCE_SOURCES} as s $refjoin $scorejoin
	WHERE 1=1 $sourcefilter $filter";
    }

    elsif ( $selector eq 'unchecked' )
    {
	$sql = "SELECT $fields
	FROM $TABLE{REFERENCE_SOURCES} as s
	WHERE s.status is null";
    }
    
    elsif ( $selector eq 'unfetched' )
    {
	$sql = "SELECT $fields
	FROM $TABLE{REFERENCE_SOURCES} as s $refjoin $scorejoin
	WHERE s.status not rlike '^2..' $sourcefilter $filter";
    }
    
    elsif ( $selector eq 'fetched' )
    {
	$sql = "SELECT $fields
	FROM $TABLE{REFERENCE_SOURCES} as s $refjoin $scorejoin
	WHERE s.status rlike '^2..' $sourcefilter $filter";
    }
    
    elsif ( $selector eq 'unscored' || $selector eq 'unmatched' )
    {
	$sql = "SELECT $fields
	FROM $TABLE{REFERENCE_SOURCES} as s $refjoin
	    left join $TABLE{REFERENCE_SCORES} as sc on s.refsource_no = sc.refsource_no $sourcefilter
    	WHERE (sc.reference_no is null or not($scorefilter)) $filter";
    }
    
    elsif ( $selector eq 'scored' || $selector eq 'matched' )
    {
	my $sql = "SELECT $fields
	FROM $TABLE{REFERENCE_SOURCES} as s $refjoin
	    join $TABLE{REFERENCE_SCORES} as sc on s.refsource_no = sc.refsource_no $sourcefilter
    	WHERE ($scorefilter) $filter";
    }
    
    else
    {
	croak "ERROR: unknown selector '$selector'\n";
    }

    return $sql;
}


sub generate_recordfilter {

    my ($ds, $filter_clause) = @_;
    
    if ( $filter_clause && $filter_clause ne '()' )
    {
	return ($filter_clause =~ /^[(]/) ? " and $filter_clause" : " and ($filter_clause)";
    }

    else
    {
	return '';
    }
}


sub generate_sourcefilter {
    
    my ($ds, $selector, $options) = @_;
    
    my $source = $options->{source} || $ds->{source};

    if ( $source eq 'crossref' || $source eq 'xdd' )
    {
	return " and s.source='$source'";
    }

    else
    {
	return '';
    }
}


sub generate_scorefilter {
    
    my ($ds, $selector, $options) = @_;
    
    if ( $selector eq 'matched' || $selector eq 'unmatched' )
    {
	my @clauses;
	
	if ( $options->{good} )
	{
	    my ($min_sum, $min_count, $min_complete) = split /:/, $options->{good};
	    
	    push @clauses, "sc.sum_s >= $min_sum" if $min_sum;
	}

	if ( $options->{bad} )
	{
	    my ($max_sum, $max_count, $max_complete) = split /:/, $options->{bad};
	    
	    push @clauses, "sc.sum_c < $max_sum" if $max_sum;
	}
	
	if ( @clauses )
	{
	    return join(' and ', @clauses);
	}
	
	else
	{
	    return "sc.sum_s >= $GOOD_MATCH_MIN";
	}
    }
    
    elsif ( $selector eq 'scored' || $selector eq 'unscored' )
    {
	return "sc.sum_s > 0 and sc.sum_c > 0";
    }
    
    else
    {
	return '';
    }
}


# select_records ( selector, sql, options )

sub select_records {
    
    my ($ds, $sql, $disposition, $options) = @_;
    
    my $dbh = $ds->{dbh};
    
    if ( $options->{offset} )
    {
	my $offset = $options->{offset} + 0;
	croak "ERROR: invalid offset '$offset'\n" unless $offset =~ /^\d+$/;
	
	$sql .= " OFFSET $offset";
    }
    
    if ( defined $options->{limit} && $options->{limit} ne '' )
    {
	my $limit = $options->{limit} + 0;
	croak "ERROR: invalid limit '$limit'\n" unless $limit =~ /^\d+$/;
	
	$sql .= " LIMIT $limit";
    }
    
    $ds->{selection_sql} = $sql;
    
    print STDERR "$sql\n\n" if $ds->{debug};

    my $result;
    
    eval {
	if ( $disposition eq 'return' )
	{
	    $result = $dbh->selectall_arrayref($sql, { Slice => { } });
	}

	else
	{
	    my $sth = $dbh->prepare($sql);
	    $sth->execute();
	    $ds->{selection_sth} = $sth;
	    ($ds->{selection_count}) = $dbh->selectrow_array("SELECT FOUND_ROWS()");
	}
    };
    
    if ( $@ )
    {
	print STDERR "$@\n\n";
	return;
    }
    
    elsif ( ref $result eq 'ARRAY' )
    {
	$ds->{selection_count} = scalar(@$result);
	return @$result;
    }
}


# count_records ( selector, sql, options )
#
# Count the number of records that match the specified SQL expression.

sub count_records {

    my ($ds, $selector, $sql, $options) = @_;
    
    print STDERR "$sql\n\n" if $ds->{debug};
    
    my $dbh = $ds->{dbh};
    
    my ($count) = $dbh->selectrow_array($sql);

    return $count;
}


# clear_selection ()
#

sub clear_selection {

    my ($ds, $mode) = @_;

    $ds->{selection_sth} = undef;
    $ds->{selection_sql} = undef;
    $ds->{selection_count} = undef;
    $ds->{selection_mode} = $mode;
    $ds->{selection_list} = undef;
    $ds->{last_refno} = undef;
}


# selection_count ()
#

sub selection_count {

    my ($ds) = @_;

    return $ds->{selection_count};
}


# get_next ( )
#
# If a selection has been made, return the next selected record. This method may be called
# repeatedly to iterate through the selection. If there is no selection, or if all of the
# selected records have been returned, return undef.

sub get_next {
    
    my ($ds) = @_;
    
    if ( $ds->{selection_sth} )
    {
	if ( $ds->{selection_mode} eq 'latest' )
	{
	    my ($r, $reference_no);
	    
	    while ( $r = $ds->{selection_sth}->fetchrow_hashref )
	    {
		$reference_no = $r && $r->{reference_no};
		
		last unless $reference_no && $ds->{last_refno} &&
		    $reference_no eq $ds->{last_refno};
	    }
	    
	    $ds->{last_refno} = $reference_no;
	    
	    return $r;
	}
	
	else
	{
	    return $ds->{selection_sth}->fetchrow_hashref;
	}
    }
    
    elsif ( ref $ds->{selection_list} eq 'ARRAY' )
    {
	return shift $ds->{selection_list}->@*;
    }
}


# metadata_query ( attrs )
# 
# Perform a search on this data source using the specified set of reference attributes. If
# this search is intended to fetch data matching an existing bibliographic reference
# record in the Paleobiology Database, the attribute `reference_no` should be included.

sub metadata_query {
    
    my ($ds, $attrs, $source) = @_;
    
    # If the attrs parameter is a reference, it must be a hashref.
    
    croak "first argument must be a hashref" unless ref($attrs) && reftype($attrs) eq 'HASH';
    
    # If the source isn't specified, use the one from $ds.

    $source ||= $ds->source;
    
    # # The max_items parameter, if specified, must be a positive integer.
    
    # if ( $max_items && $max_items !~ /^\d+$/ )
    # {
    # 	croak "second argument (max_items) must be a positive integer";
    # }
    
    # # The default number of query results to ask for is 2.
    
    # $max_items ||= 2;
    
    # Prepare to generate a query.
    
    my $ua = $ds->{ua};
    
    # This operation will involve one or more requests on the datasource. We may need to
    # try several different requests before we are satisfied that no matching record can
    # be found in the external dataset. In order to coordinate this, we use a hashref that
    # can hold flags and variables.
    
    my $progress = { };
    my $request_count = 0;
    my $code_500_count = 0;
    my ($request_url, $response_code, $response_status, $match_item, $match_score, $abort);
    my @possible_matches;
    
    # Loop until we find a sufficiently similar record, or until the generate_request_url
    # method stops returning new URLs, or until we exceed a set number of requests.
    
  REQUEST:
    while ( $request_url = $ds->generate_request_url($source, $progress, $attrs) )
    {
	# Abort if we exceed a set number of requests.
	
	last REQUEST if ++$request_count > 10;
	
	# If the last request took at least 5 seconds to complete, wait an additional
	# second. If it took at least 10 seconds to complete, wait an additional 5
	# seconds. This helps to keep the server from getting overloaded.
	
	if ( $ds->{last_latency} >= 5 )
	{
	    my $delay = $ds->{last_latency} >= 10 ? 5 : 1;
	    sleep($delay);
	}
	
	# Send the request and wait for the response. Record how long it takes.

	print STDERR "Request Text: $progress->{query_text}\n" if $ds->{debug};
	print STDERR "Request URL: $request_url\n" if $ds->{debug};
	
	my $inittime = time;
	
	my $response = $ua->get($request_url);
	
	$response_code = $response->code;
	$response_status = $response->status_line;
	
	$ds->{last_url} = $request_url;
	$ds->{last_code} = $response_code;
	$ds->{last_latency} = time - $inittime;
	
	print STDERR "Response status: $response_status\n\n" if $ds->{debug};
	
	# If the request is successful, decode the content and score all of the items it
	# contains.
	
	if ( $response_code =~ /^2../ )
	{
	    my $decoded_content = $ds->decode_response_json($response);
	    my @items = $ds->extract_source_records($source, $decoded_content, $attrs);
	    
	    # Loop through the items and score them one by one. Ignore any item with a
	    # conflict sum of 300 or more. If we find an item with a similarity sum of 500
	    # or better, choose it immediately. Otherwise, set aside any items that have a
	    # similarity sum of 300 or better.
	    
	    foreach my $item ( @items )
	    {
		my $score = ref_similarity($attrs, $item, { max_c => $PARTIAL_MATCH_MIN - 1 });
		
		if ( $score->{sum_s} && $score->{sum_s} >= $GOOD_MATCH_MIN )
		{
		    $match_item = $item;
		    $match_score = $score;
		    last REQUEST;
		}
		
		elsif ( $score->{sum_s} && $score->{sum_s} >= $PARTIAL_MATCH_MIN )
		{
		    push @possible_matches, [$item, $score, $request_url,
					     $progress->{query_text}];
		}
	    }
	}

	# If the response indicates a client error, abort the fetch.
	
	elsif ( $response_code =~ /^4../ )
	{
	    $abort = $response->status_line;
	    last REQUEST;
	}
	
	# If the response indicates a server error, abort if we have gotten more than a set
	# number of server errors. Otherwise, wait a few seconds and try again.
	
	elsif ( $response_code =~ /^5../ )
	{
	    if ( ++$code_500_count < 3 )
	    {
		sleep(2 + $code_500_count * 2);
		next REQUEST;
	    }
	    
	    else
	    {
		$abort = $response->status_line;
		last REQUEST;
	    }
	}
	
	# With any other code, abort.
	
	else
	{
	    $abort = $response->status_line;
	    last REQUEST;
	}
    }
    
    # If we aborted, return the error status.
    
    if ( $abort )
    {
	return { status => $abort, error => 1, source => $source }
    }
    
    # If we have a good match, return it.
    
    if ( $match_item )
    {
	return { status => "200 Good Match",
		 success => 1,
		 source => $source,
		 request_count => $request_count,
		 item => $match_item,
		 scores => $match_score,
		 query_url => $request_url,
		 query_text => $progress->{query_text} };
    }
    
    # If we have one or more possible matches, choose the one with the best similarity
    # score.
    
    if ( @possible_matches )
    {
	my $best_similarity = 0;
	my $best_match;
	
	foreach my $i ( 0..$#possible_matches )
	{
	    if ( $possible_matches[$i][1]{sum_s} > $best_similarity )
	    {
		$best_match = $possible_matches[$i];
		$best_similarity = $possible_matches[$i][1]{sum_s};
	    }
	}
	
	if ( $best_match )
	{
	    return { status => "280 Partial match",
		     success => 1,
		     source => $source,
		     request_count => $request_count,
		     item => $best_match->[0],
		     scores => $best_match->[1],
		     query_url => $best_match->[2],
		     query_text => $best_match->[3] };
	}
    }
    
    # Otherwise, return either Not Found or No Valid Requests as appropriate.
    
    if ( $request_count )
    {
	return { status => "404 Not found",
		 notfound => 1,
		 source => $source,
		 request_count => $request_count };
    }
    
    else
    {
	return { status => "480 No valid requests",
		 notfound => 1,
		 source => $source,
		 request_count => 0 };
    }
}


# decode_response_json ( response )
# 
# This method decodes the response content from a data source request into a Perl data
# structure and returns it.

sub decode_response_json {
    
    my ($ds, $response) = @_;
    
    my $content = $response->decoded_content;
    
    return decode_json($content);
}


# extract_source_records ( source, response_data, attrs )
# 
# This method takes a Perl data structure decoded from the response to a data source
# request. It returns a list of items that may (or may not) be a match for the specified
# reference attributes. The source-specific methods may filter the list to exclude obvious
# mismatches, but are not required to do so.

sub extract_source_records {

    my ($ds, $source, $response_data, $attrs) = @_;
    
    if ( $source eq 'crossref' )
    {
	return $ds->extract_crossref_records($response_data, $attrs);
    }
    
    elsif ( $source eq 'xdd' )
    {
	return $ds->extract_xdd_records($response_data, $attrs);
    }
    
    else
    {
	croak "Unrecognized data source: '$source'";
    }
}


# generate_request_url ( source, progress, attrs )
# 
# Generate a request URL that will be used to query this datasource, trying to match the
# given set of reference attributes. The $progress hash is used to keep track of which
# requests have been tried so far.

sub generate_request_url {
    
    my ($ds, $source, $progress, $attrs) = @_;
    
    # If the previous request returned a server error, try the same request again. The
    # calling method is responsible for waiting an appropriate amount of time between
    # requests.
    
    if ( $ds->{last_code} =~ /^5../ )
    {
	return $ds->{last_url};
    }
    
    # Otherwise, call the method appropriate to this datasource.
    
    elsif ( $source eq 'crossref' )
    {
	return $ds->generate_crossref_request($progress, $attrs);
    }
    
    elsif ( $source eq 'xdd' )
    {
	return $ds->generate_xdd_request($progress, $attrs);
    }
    
    else
    {
	croak "Unrecognized data source: '$source'";
    }
}


# generate_crossref_request ( progress, attrs )
# 
# Generate a request URL that will be used to query the Crossref dataset, trying to match
# the given set of reference attributes. The $progress hash is used to keep track of which
# requests have been tried so far.

sub generate_crossref_request {
    
    my ($ds, $progress, $attrs) = @_;

    # If the attributes include a DOI, try that first.
    
    if ( ! $progress->{try_doi} )
    {
	$progress->{try_doi} = 1;
	
	if ( my $doi = get_doi($attrs) )
	{
	    $progress->{query_text} = "doi: $doi";
	    
	    my $encoded = uri_escape_utf8($doi);
	    return "$CROSSREF_BASE/$encoded";
	}

	print STDERR "Attributes lack a doi.\n" if $ds->{debug};
    }
    
    # If the attributes do not include a DOI, or if we have already tried that, then try a
    # bibliographic query.
    
    if ( ! $progress->{try_biblio} )
    {
	$progress->{try_biblio} = 1;
	
	my @query_words;
	my @container_words;
	my @author_words;
	
	# If the attributes include a reference title containing at least 3 letters in a
	# row, chop it up into words. Ignore punctuation, whitespace, and stopwords, and
	# put each word into foldcase.
	
	my $reftitle = get_reftitle($attrs);
	
	if ( $reftitle =~ /\pL{3}/ )
	{
	    push @query_words, title_words($reftitle);
	}
	
	# If the attributes include a publication year, add that too.
	
	my $pubyr = get_pubyr($attrs);
	
	push @query_words, $pubyr if $pubyr;
	
	# If the attributes include a publication title which is different from the reference
	# title, do the same thing.
	
	my $pubtitle = get_pubtitle($attrs);
	
	if ( $pubtitle && $pubtitle ne $reftitle )
	{
	    push @container_words, title_words($pubtitle);
	}
	
	# Add up to 3 author lastnames from the reference attributes.
	
	foreach my $i (1..3)
	{
	    my ($lastname, $firstname) = get_authorname($attrs, $i);
	    
	    if ( $lastname && $lastname =~ /\pL{2}/ )
	    {
		push @author_words, grep { /\pL{2}/ } title_words($lastname);
	    }
	    
	    if ( $firstname && $firstname =~ /\pL{2}/ )
	    {
		push @author_words, grep { /\pL{2}/ } title_words($firstname);
	    }
	}
	
	# If we have enough attributes for a reasonable request, assemble and return it.
	
	if ( @author_words && ( @query_words > 1 || @container_words && $pubyr ) )
	{
	    my (@url_params, @text_params);
	    
	    if ( @query_words )
	    {
		my $value = join ' ', @query_words;
		
		push @url_params, "query.bibliographic=" . uri_escape_utf8($value);
		push @text_params, "biblio: $value";
	    }
	    
	    if ( @container_words )
	    {
		my $value = join ' ', @container_words;
		
		push @url_params, "query.container-title=" . uri_escape_utf8($value);
		push @text_params, "container: $value";
	    }
	    
	    if ( @author_words )
	    {
		my $value = join ' ', @author_words;
		
		push @url_params, "query.contributor=" . uri_escape_utf8($value);
		push @text_params, "author: $value";
	    }
	    
	    $progress->{query_text} = join '; ', @text_params;
	    
	    push @url_params, "rows=2";
	    
	    my $query_string = join '&', @url_params;
	    return "$CROSSREF_BASE?$query_string";
	}

	print STDERR "Attributes do not contain enough bibliographic information.\n"
	    if $ds->{debug};
    }
    
    # If we have tried both of these, return nothing.
    
    return;
}


# extract_crossref_records ( response_data, attrs )
#
# Extract data items from a Crossref response. Remove unnecessary keys from each item,
# so that we won't store information that is unnecessary for our purpose.

sub extract_crossref_records {

    my ($ds, $data, $attrs) = @_;
    
    if ( ref $data eq 'HASH' && ref $data->{message}{items} eq 'ARRAY' )
    {
	return map { $ds->clean_crossref_item($_) } @{$data->{message}{items}};
    }
    
    elsif ( ref $data eq 'HASH' && $data->{message}{deposited} )
    {
	return $ds->clean_crossref_item($data->{message});
    }
    
    elsif ( ref $data eq 'ARRAY' && $data->[0]{deposited} || $data->[0]{indexed} )
    {
	return $ds->clean_crossref_item($data);
    }

    elsif ( $data->{deposited} || $data->{indexed} )
    {
	return $ds->clean_crossref_item($data);
    }

    return;
}


sub clean_crossref_item {
    
    my ($ds, $data) = @_;

    delete $data->{reference};
    delete $data->{license};
    delete $data->{'content-domain'};
    
    foreach my $key ( 'created', 'deposited', 'indexed', 'published' )
    {
	if ( $data->{$key}{'date-time'} )
	{
	    $data->{$key} = $data->{$key}{'date-time'};
	}

	elsif ( ref $data->{$key}{'date-parts'} eq 'ARRAY' &&
		ref $data->{$key}{'date-parts'}[0] eq 'ARRAY' &&
		$data->{$key}{'date-parts'}[0][0] =~ /^\d+$/ )
	{
	    $data->{$key} = join '-', $data->{$key}{'date-parts'}[0]->@*;
	}
    }

    return $data;
}


# generate_xdd_request ( progress, attrs )
# 
# Generate a request URL that will be used to query the XDD dataset, trying to match
# the given set of reference attributes. The $progress hash is used to keep track of which
# requests have been tried so far.

sub generate_xdd_request {
    
    my ($ds, $progress, $attrs) = @_;
    
    # If the attributes include a DOI, try that first.
    
    if ( ! $progress->{try_doi} )
    {
	$progress->{try_doi} = 1;
	
	if ( my $doi = get_doi($attrs) )
	{
	    $progress->{query_text} = "doi: $doi";
	    
	    my $encoded = uri_escape_utf8($doi);
	    return "$XDD_BASE?doi=$encoded";
	}
    }
    
    # If the attributes do not include a DOI, or if we have already tried that, then try a
    # bibliographic query. It may be necessary to try several queries, due to the
    # deficiencies in the XDD api. The first step is to process the attributes.
    
    unless ( defined $progress->{pubyr} )
    {
	$progress->{pubyr} = (get_pubyr($attrs) || '');

	my $minyr = $progress->{pubyr} - 1;
	my $maxyr = $progress->{pubyr} + 1;

	$progress->{pubyr_clause} = $progress->{pubyr} ?
	    "min_published=$minyr&max_published=$maxyr" : '';
	$progress->{pubyr_text} = $progress->{pubyr} ? "pubyr: $progress->{pubyr}" : '';
	
	$progress->{reftitle} = (get_reftitle($attrs) || '');
	$progress->{pubtitle} = (get_pubtitle($attrs) || '');
	
	$progress->{reftitle_has_marks} = 1 if $progress->{reftitle} =~ /\pM/;
	$progress->{pubtitle_has_marks} = 1 if $progress->{pubtitle} =~ /\pM/;
	
	$progress->{refwords} = [ title_words($progress->{reftitle}, 1) ];
	$progress->{pubwords} = [ title_words($progress->{pubtitle}, 1) ];
	
	$progress->{reftitle_okay} = $progress->{reftitle} &&
	    length($progress->{reftitle}) >= 15 &&
	    $progress->{refwords}->@* > 3;
	
	$progress->{pubtitle_okay} = $progress->{pubtitle} &&
	    $progress->{pubwords}->@*;
	
	my ($author1last) = get_authorname($attrs, 1);
	my ($author2last) = get_authorname($attrs, 2);
	
	$progress->{author1last} = ($author1last || '');
	$progress->{author2last} = ($author2last || '');
	
	$progress->{author1_has_marks} = 1 if $author1last =~ /\pM/;
	$progress->{author2_has_marks} = 1 if $author2last =~ /\pM/;
    }
    
    # If we have a reftitle of at least 15 characters and at least 4 words, try that first and
    # see if it's in the top 3 results.
    
    if ( ! $progress->{try_title_only} )
    {
	$progress->{try_title_only} = 1;
	
	if ( $progress->{reftitle_okay} )
	{
	    return xdd_simple_request($progress,
				  { title => $progress->{refwords},
				    pubyr => $progress->{pubyr},
				    limit => 3 });
	}
    }
    
    # If that doesn't work, try the reftitle with author1, then with author2.
    
    if ( ! $progress->{try_title_auth1} )
    {
	$progress->{try_title_auth1} = 1;

	if ( $progress->{reftitle_okay} && $progress->{author1last} )
	{
	    return xdd_simple_request($progress,
				  { title => $progress->{refwords},
				    pubyr => $progress->{pubyr},
				    lastname => $progress->{author1last},
				    limit => 3 });
	}
    }
    
    if ( ! $progress->{try_title_auth2} )
    {
	$progress->{try_title_auth2} = 1;

	if ( $progress->{reftitle_okay} && $progress->{author2last} )
	{
	    return xdd_simple_request($progress,
				  { title => $progress->{refwords},
				    pubyr => $progress->{pubyr},
				    lastname => $progress->{author2last},
				    limit => 3 });
	}
    }
    
    # If we have tried all of these, return nothing.
    
    return;
    
    # # Since the XDD api only allows querying for a single author, the first try is made
    # # using the first author. If that fails, a second try will be made using the second
    # # author. If one or the other is entered correctly, 
    
    # if ( ! $progress->{try_biblio} == 2 )
    # {
    # 	my $author_no;
	
    # 	if ( $progress->{try_biblio} == 1 )
    # 	{
    # 	    $author_no = 2;
    # 	    $progress->{try_biblio} = 2;
    # 	}

    # 	else
    # 	{
    # 	    $author_no = 1;
    # 	    $progress->{try_biblio} = 1;
    # 	}
	
    # 	my $reftitle = get_reftitle($attrs);

    # 	my @title_words = title_words($reftitle);
	
    # 	my $pubyr = get_pubyr($attrs);
	
    # 	my $minyr = $pubyr-1;
    # 	my $maxyr = $pubyr+1;
	
    # 	my ($author_last) = get_authorname($attrs, $author_no);
	
    # 	if ( (@title_words || $pubyr) && $author_last )
    # 	{
    # 	    my (@url_params, @text_params);
	    
    # 	    if ( @title_words )
    # 	    {
    # 		my $value = join ' ', @title_words;
    # 		my $modified = join ',', map "$_*" @title_words;
		
    # 		push @url_params, "term=$modified";
    # 		push @text_params, "title: $value";
    # 	    }
	    
	    
    # 	my $pubyr_select = "&min_published=$minyr&max_published=$maxyr";
	
    # 	$query_text = "title=$title$pubyr_select";
    # 	$query_url = "https://xdd.wisc.edu/api/articles?max=5&" . $query_text;
    # 	}
}


sub xdd_simple_request {

    my ($progress, $params) = @_;
    
    my (@url_params, @text_params);
    
    if ( $params->{title} && ref $params->{title} eq 'ARRAY' )
    {
	my $string = join ' ', $params->{title}->@*;

	push @url_params, "title_like=" . uri_escape_utf8($string);
	push @text_params, "title: $string";
    }

    elsif ( $params->{title} )
    {
	push @url_params, "title_like=" . uri_escape_utf8($params->{title});
	push @text_params, "title: $params->{title}";
    }

    if ( $params->{pubyr} )
    {
	push @url_params, $progress->{pubyr_clause};
	push @text_params, "pubyr: $progress->{pubyr} +-";
    }

    if ( $params->{lastname} )
    {
	push @url_params, "lastname=" . uri_escape_utf8($params->{lastname});
	push @text_params, "lastname: $params->{lastname}";
    }
    
    my $limit = $params->{limit} || 5;
    
    push @url_params, "max=$limit";
    
    $progress->{query_text} = join '; ', @text_params;
    
    my $query_args = join '&', @url_params;
    
    return "$XDD_BASE?$query_args";
}


# extract_xdd_records ( response_data, attrs )
#
# Extract data items from an XDD response. Remove unnecessary keys from each item,
# so that we won't store information that is unnecessary for our purpose.

sub extract_xdd_records {

    my ($ds, $data, $attrs) = @_;

    if ( ref $data->{success}{data} eq 'ARRAY' )
    {
	return $data->{success}{data}->@*;
    }

    elsif ( $data->{success}{title} || $data->{success}{author} )
    {
	return $data->{success};
    }
    
    elsif ( $data->{title} || $data->{author} )
    {
	return $data;
    }

    return;
}


# store_source_data ( reference_no, data )
#
# Store the specified source data in the reference sources table with the specified
# reference_no value. The data must be a hashref returned from a previous call to
# metadata_query, and the reference_no must be a positive integer. This method returns the
# refsource_no of the stored data. If this is an existing record that was updated, a
# second return value of 'updated' is added.

sub store_source_data {
    
    my ($ds, $r, $data) = @_;
    
    my $reference_no;
    
    if ( ref $r eq 'HASH' && $r->{reference_no} && $r->{reference_no} =~ /^\d+$/ )
    {
	$reference_no = $r->{reference_no};
    }
    
    elsif ( $r && $r =~ /^\d+$/ )
    {
	$reference_no = $r;
    }

    else
    {
	croak "reference_no must be a positive integer, either directly or as a hash value";
    }
    
    unless ( ref $data eq 'HASH' && $data->{status} && $data->{source} )
    {
	my $label = $data->{source} ? "for $reference_no ($data->{source})"
	    : "for $reference_no";
	croak "cannot store source data $label: missing attributes\n";
    }
    
    my $dbh = $ds->{dbh};

    my $sql;
    my $result;
    
    # Quote the data fields.
    
    my $quoted_status = $dbh->quote($data->{status});
    my $quoted_source = $dbh->quote($data->{source});
    my $quoted_text = $dbh->quote($data->{query_text} // '');
    my $quoted_url = $dbh->quote($data->{query_url} // '');
    my ($encoded_item, $quoted_item);
    
    if ( $data->{item} )
    {
	$encoded_item = $JSON_ENCODER->encode($data->{item});
	$quoted_item = $dbh->quote($encoded_item);
    }

    else
    {
	$encoded_item = '';
	$quoted_item = "''";
    }
    
    # Check to see if the data to be stored matches an existing reference source event.
    
    $sql = "SELECT refsource_no, response_data FROM $TABLE{REFERENCE_SOURCES} 
	WHERE reference_no = $reference_no and source = $quoted_source and
	  eventtype = 'fetch' and status = $quoted_status and
	  query_url = $quoted_url
	ORDER BY eventtime desc LIMIT 1";
    
    print STDERR "$sql\n\n" if $ds->{debug};
    
    my ($refsource_no, $previous_data) = $dbh->selectrow_array($sql);
    
    # If it does, check previous response_data against the new data. If it differs only in
    # unimportant ways (difference in matching score, difference in indexed date, etc.)
    # then update the record with the new eventtime, query_text, and response_data.

    my $update;

    if ( $refsource_no )
    {
	if ( $encoded_item ne '' && $data->{source} eq 'crossref' )
	{
	    my $check_data = $encoded_item;
	    
	    $check_data =~ s/"score":["'\w\s.-]+//;
	    $previous_data =~ s/"score":["'\w\s.-]+//;
	    
	    $check_data =~ s/"indexed":["'\w\s.-:]+//;
	    $previous_data =~ s/"indexed":["'\w\s.-:]+//;
	    
	    $update = 1 if $check_data eq $previous_data;
	}
	
	elsif ( $encoded_item ne '' && $data->{source} eq 'xdd' )
	{
	    $update = 1 if $encoded_item eq $previous_data;
	}
	
	elsif ( $encoded_item eq '' && $previous_data eq '' )
	{
	    $update = 1;
	}
    }
    
    # If the record is to be updated, do so.
    
    if ( $update )
    {
	$sql = "UPDATE $TABLE{REFERENCE_SOURCES}
		SET eventtime=now(), query_text=$quoted_text
		WHERE refsource_no = $refsource_no LIMIT 1";
	
	print STDERR "$sql\n\n" if $ds->{debug};

	eval {
	    $result = $dbh->do($sql);
	};
	
	if ( $@ )
	{
	    print STDERR "$sql\n\n" unless $ds->{debug};
	    return 'error', $@;
	}
	
	else
	{
	    return 'updated', $refsource_no;
	}
    }
    
    # Otherwise, store a new reference source event record.
    
    else
    {
	$sql = "INSERT INTO $TABLE{REFERENCE_SOURCES}
		(source, reference_no, eventtype, status, query_text, query_url, response_data)
		VALUES ($quoted_source, $reference_no, 'fetch', $quoted_status,
		$quoted_text, $quoted_url, $quoted_item)";
	
	print STDERR "$sql\n\n" if $ds->{debug};

	eval {
	    $result = $dbh->do($sql);
	};
	
	if ( $@ )
	{
	    print STDERR "$sql\n\n" unless $ds->{debug};
	    return 'error', $@;
	}
	
	elsif ( $result )
	{
	    return 'inserted', $dbh->last_insert_id();
	}
	
	else
	{
	    return 'error', "could not insert reference source event record";
	}
    }
}


# store_score_data ( attrs, scores, formatted )
#
# 

sub store_score_data {

    my ($ds, $attrs, $scores, $formatted) = @_;
    
    # Make sure the necessary information was specified.
    
    unless ( ref $attrs eq 'HASH' && $attrs->{refsource_no} &&
	     $attrs->{reference_no} && $attrs->{source} )
    {
	croak "ERROR: cannot store score data: missing attributes\n";
    }
    
    unless ( $attrs->{refsource_no} =~ /^\d+$/ )
    {
	croak "ERROR: bad value for '$attrs->{refsource_no}' for refsource_no\n";
    }
    
    unless ( $attrs->{reference_no} =~ /^\d+$/ )
    {
	croak "ERROR: bad value for '$attrs->{reference_no}' for reference_no\n";
    }
    
    unless ( ref $scores eq 'HASH' && defined $scores->{title_s} )
    {
	croak "ERROR: no score data was provided\n";
    }
    
    # Generate values needed for the SQL statements.
    
    my $dbh = $ds->{dbh};
    my $quoted_sourceno = $dbh->quote($attrs->{refsource_no});
    my $quoted_refno = $dbh->quote($attrs->{reference_no});
    my $quoted_source = $dbh->quote($attrs->{source});
    my $quoted_formatted = $dbh->quote($formatted);
    
    # Check to see if a record is already present.
    
    my $sql = "SELECT sc.* FROM $TABLE{REFERENCE_SOURCES} as sc
	WHERE refsource_no = $quoted_sourceno";
    
    my $row = $dbh->selectrow_hashref($sql);

    # If there is already a score record corresponding to the specified refsource_no, check the
    # existing values against the new ones to see what needs to be updated. Then update
    # the record.
    
    if ( $row && $row->{refsource_no} )
    {
	my (@update_list, %update, $result);
	
	foreach my $name ( @SCORE_VARS )
	{
	    my $name_s = $name . "_s";
	    my $name_c = $name . "_c";
	    
	    if ( defined $scores->{$name_s} &&
		 (! defined $row->{$name_s} || $scores->{$name_s} ne $row->{$name_s}) )
	    {
		push @update_list, "$name_s = " . $dbh->quote($scores->{$name_s});
		$update{$name_s} = $scores->{$name_s};
	    }
	    
	    if ( defined $scores->{$name_c} &&
		 (! defined $row->{$name_c} || $scores->{$name_c} ne $row->{$name_c}) )
	    {
		push @update_list, "$name_c = " . $dbh->quote($scores->{$name_c});
		$update{$name_c} = $scores->{$name_c};
	    }
	}

	if ( $attrs->{source} &&
	     (! defined $row->{source} || $attrs->{source} ne $row->{source} ) )
	{
	    push @update_list, "source = $quoted_source";
	    $update{source} = $attrs->{source};
	}
	
	if ( defined $formatted && ( ! defined $row->{formatted} || $formatted ne $row->{formatted} ) )
	{
	    push @update_list, "formatted = $quoted_formatted";
	    $update{formatted} = $formatted;
	}
	
	if ( defined $scores->{debugstr} &&
	     (! defined $row->{debugstr} || $scores->{debugstr} ne $row->{debugstr} ) )
	{
	    push @update_list, "debugstr = " . $dbh->quote($scores->{debugstr});
	    $update{debugstr} = $scores->{debugstr};
	}
	
	# If there are any fields to update, generate and execute an SQL statement. If it executes
	# successfully, return the number of updated fields.
	
	if ( @update_list )
	{
	    my $update_string = join(', ', @update_list);
	    my $update_count = scalar(@update_list);
	    
	    $sql = "UPDATE $TABLE{REFERENCE_SCORES} SET $update_string
	    WHERE refsource_no = $quoted_sourceno LIMIT 1";
	    
	    print STDERR "$sql\n\n" if $ds->{debug};

	    eval {
		$result = $dbh->do($sql);
	    };

	    if ( $@ )
	    {
		print STDERR "$sql\n\n" unless $ds->{debug};
		return 'error', $@;
	    }

	    elsif ( $result )
	    {
		return 'updated', $update_count, $attrs->{refsource_no};
	    }
	    
	    else
	    {
		return 'unchanged';
	    }
	}

	else
	{
	    return 'unchanged';
	}
    }
    
    # Otherwise, insert a new score record.

    else
    {
	my $debugstr = $dbh->quote($scores->{debugstr});
	my $sum_s = $dbh->quote($scores->{sum_s} || 0);
	my $sum_c = $dbh->quote($scores->{sum_c} || 0);
	my $count_s = $dbh->quote($scores->{count_s} || 0);
	my $count_c = $dbh->quote($scores->{count_c} || 0);
	my $complete_s = $dbh->quote($scores->{complete_s} || 0);
	my $complete_c = $dbh->quote($scores->{complete_c} || 0);
	my $title_s = $dbh->quote($scores->{title_s} || 0);
	my $title_c = $dbh->quote($scores->{title_c} || 0);
	my $pub_s = $dbh->quote($scores->{pub_s} || 0);
	my $pub_c = $dbh->quote($scores->{pub_c} || 0);
	my $pblshr_s = $dbh->quote($scores->{pblshr_s} || 0);
	my $pblshr_c = $dbh->quote($scores->{pblshr_c} || 0);
	my $auth1_s = $dbh->quote($scores->{auth1_s} || 0);
	my $auth1_c = $dbh->quote($scores->{auth1_c} || 0);
	my $auth2_s = $dbh->quote($scores->{auth2_s} || 0);
	my $auth2_c = $dbh->quote($scores->{auth2_c} || 0);
	my $pubyr_s = $dbh->quote($scores->{pubyr_s} || 0);
	my $pubyr_c = $dbh->quote($scores->{pubyr_c} || 0);
	my $volume_s = $dbh->quote($scores->{volume_s} || 0);
	my $volume_c = $dbh->quote($scores->{volume_c} || 0);
	my $pages_s = $dbh->quote($scores->{pages_s} || 0);
	my $pages_c = $dbh->quote($scores->{pages_c} || 0);
	
	$sql = "REPLACE INTO $TABLE{REFERENCE_SCORES} (reference_no, refsource_no,
	source, formatted, debugstr, sum_s, sum_c, count_s, count_c, complete_s, complete_c,
	title_s, title_c, pub_s, pub_c, auth1_s, auth1_c, auth2_s, auth2_c, 
	pubyr_s, pubyr_c, volume_s, volume_c, pages_s, pages_c, pblshr_s, pblshr_c)
	VALUES ($quoted_refno, $quoted_sourceno, $quoted_formatted, $debugstr, $sum_s, $sum_c,
	$count_s, $count_c, $complete_s, $complete_c, $title_s, $title_c, $pub_s, $pub_c, 
	$auth1_s, $auth1_c, $auth2_s, $auth2_c, $pubyr_s, $pubyr_c, $volume_s, $volume_c,
	$pages_s, $pages_c, $pblshr_s, $pblshr_c)";
	
	print STDERR "$sql\n\n" if $ds->{debug};
	
	my $result;
	
	eval {
	    $result = $dbh->do($sql);
	};
	
	if ( $@ )
	{
	    print STDERR "$sql\n\n" unless $ds->{debug};
	    return 'error', $@;
	}

	elsif ( $result )
	{
	    return 'inserted', $attrs->{refsource_no};
	}

	else
	{
	    return 'error', "could not insert score record";
	}
    }
}    


# store_result ( reference_no, status, query_text, query_url, data )
#
# Store the specified query results in the database. If the data is the same as the data stored
# from a previous response, add a record referencing that response.

sub store_result {

    my ($ds, $reference_no, $status, $query_text, $query_url, $data) = @_;
    
    # The reference_no value must be a positive integer.
    
    unless ( $reference_no && $reference_no =~ /^\d+$/ )
    {
	croak "reference_no must be a positive integer";
    }

    my $dbh = $ds->{dbh};
    my $source = $ds->{source};
    
    # Quote the text attributes.
    
    my $quoted_source = $dbh->quote($source);
    my $quoted_status = $dbh->quote($status // '');
    my $quoted_text = $dbh->quote($query_text // '');
    my $quoted_url = $dbh->quote($query_url // '');
    my $quoted_data = $dbh->quote($data // '');
    my $short_data = $dbh->quote($data ? substr($data, 0, 20) . '...' : '');
    
    # If we are given a data string, see if it is already stored from a previous response.
    # If so, store a record that references the old one.

    my $sql;
    my $result;
    
    if ( $data )
    {
	$sql = "SELECT refsource_no FROM $TABLE{REFERENCE_SOURCES} 
		WHERE reference_no = $reference_no and response_data = $quoted_data
		ORDER BY eventtime desc LIMIT 1";

	print STDERR "SELECT refsource_no FROM $TABLE{REFERENCE_SOURCES} 
		WHERE reference_no = $reference_no and response_data = $short_data
		ORDER BY eventtime desc LIMIT 1\n\n" if $ds->{debug};
	
	my ($refsource_no) = $dbh->selectrow_array($sql);
	
	# If there is a previous matching row with the same result data, add a new row that
	# references it.
	
	if ( $refsource_no )
	{
	    $sql = "INSERT INTO $TABLE{REFERENCE_SOURCES}
		(reference_no, eventtype, source, previous_no, status, query_text, query_url)
		VALUES ($reference_no, 'fetch', $quoted_source, $refsource_no, $quoted_status, $quoted_text, $quoted_url)";
	    
	    print STDERR "$sql\n\n"if $ds->{debug};
	    
	    $result = $dbh->do($sql);
	}
	
	# Otherwise, add a new row with the given data.

	else
	{
	    my $items = 'NULL';
	    
	    if ( $source eq 'xdd' )
	    {
		if ( substr($data, 0, 50) =~ /\"data\"\:\[\]/ )
		{
		    $items = "'0'";
		}
	    }
	    
	    $sql = "INSERT INTO $TABLE{REFERENCE_SOURCES}
		(reference_no, eventtype, source, status, query_text, query_url, items, response_data)
		VALUES ($reference_no, 'fetch', $quoted_source, $quoted_status, $quoted_text, $quoted_url, $items, $quoted_data)";
	    
	    print STDERR "INSERT INTO $TABLE{REFERENCE_SOURCES}
		(reference_no, eventtype, source, status, query_text, query_url, items, response_data)
		VALUES ($reference_no, 'fetch', $quoted_source, $quoted_status, 
		$quoted_text, $quoted_url, $items, $short_data)" if $ds->{debug};
	    
	    $result = $dbh->do($sql);
	}
    }
    
    # Otherwise, add a new row for this event.
    
    else
    {
	$sql = "INSERT INTO $TABLE{REFERENCE_SOURCES}
		(reference_no, eventtype, source, status, query_text, query_url)
		VALUES ($reference_no, 'fetch', $quoted_source, $quoted_status,
		$quoted_text, $quoted_url)";

	print STDERR "$sql\n\n" if $ds->{debug};
	
	$result = $dbh->do($sql);
    }
    
    # If the insert operation succeeded, return the identifier of the inserted record. Otherwise,
    # return false.
    
    if ( $result )
    {
	return $dbh->last_insert_id();
    }
}


# list_events ( r, source, selector )
# 
# Return a list of events from the REFERENCE_SOURCES table, selected by either
# reference_no or refsource_no. The second argument may be a valid source name or 'all'.
# The argument $selector may be any of the following:
#
# history	Return a list of records representing all events from the
#		specified source, most recent to least recent. Response
#               data is not included.
#
# full		Return the same list as 'history', but include the response
#		data.
#
# latest	Return only the most recent event, including the response data.

sub list_events {

    my ($ds, $r, $source, $selector) = @_;
    
    my $dbh = $ds->{dbh};
    
    my ($basefilter, @filters);
    
    $selector ||= 'history';
    
    unless ( $selector eq 'history' || $selector eq 'full' || $selector eq 'latest' )
    {
	croak "Unrecognized selector '$selector'\n";
    }
    
    # Generate an SQL expression to select the requested records. If no valid record
    # identifier is provided, return nothing.
    
    if ( ref $r eq 'HASH' && $r->{reference_no} )
    {
	my $quoted = $dbh->quote($r->{reference_no});
	$basefilter = "s.reference_no = $quoted";
    }
    
    elsif ( ref $r eq 'HASH' && $r->{refsource_no} )
    {
	my $quoted = $dbh->quote($r->{refsource_no});
	$basefilter = "s.refsource_no = $quoted";
    }
    
    else
    {
	return;
    }
    
    if ( $source eq 'crossref' || $source eq 'xdd' )
    {
	push @filters, "$basefilter and s.source = '$source'";
    }

    elsif ( $source && $source ne 'all' )
    {
	croak "Unrecognized source '$source'\n";
    }

    else
    {
	push @filters, "$basefilter and s.source = 'crossref'";
	push @filters, "$basefilter and s.source = 'xdd'";
    }
    
    # Generate the list of fields to select.
    
    my $fields = "s.refsource_no, s.source, s.reference_no, eventtype, eventtime, status,
		query_text, query_url, sum_s, sum_c";
    
    if ( $selector eq 'full' || $selector eq 'latest' )
    {
	$fields .= ", response_data as data";
    }
    
    else
    {
	$fields .= ", left(response_data,20) as data";
    }
    
    # For each entry in @filters, make one SQL query. Collect the results together in
    # @results.

    my ($result, @results);
    
    foreach my $expr ( @filters )
    {
	my $sql = "SELECT $fields
	FROM $TABLE{REFERENCE_SOURCES} as s
	    left join $TABLE{REFERENCE_SCORES} as sc using (refsource_no)
	WHERE $expr
	GROUP BY s.refsource_no ORDER BY eventtime desc";
	
	$sql .= " LIMIT 1" if $selector eq 'latest';
	
	print STDERR "$sql\n\n" if $ds->{debug};

	eval {
	    $result = $dbh->selectall_arrayref($sql, { Slice => { } });
	};

	if ( $@ )
	{
	    print STDERR "$@\n\n";
	}
	
	elsif ( ref $result eq 'ARRAY' && @$result)
	{
	    push @results, @$result;
	}
    }
    
    return @results;
}


# store_scores ( event, scores, index )
#
#

sub store_scores {
    
    my ($ds, $e, $s, $index) = @_;
    
    my $dbh = $ds->{dbh};

    my $refno = $dbh->quote($e->{reference_no});
    my $sourceno = $dbh->quote($e->{refsource_no});
    my $idx = $dbh->quote($index);
    
    my $count_s = $dbh->quote($s->{count_s} || 0);
    my $count_c = $dbh->quote($s->{count_c} || 0);
    my $complete_s = $dbh->quote($s->{complete_s} || 0);
    my $complete_c = $dbh->quote($s->{complete_c} || 0);
    my $sum_s = $dbh->quote($s->{sum_s} || 0);
    my $sum_c = $dbh->quote($s->{sum_c} || 0);
    my $title_s = $dbh->quote($s->{title_s} || 0);
    my $title_c = $dbh->quote($s->{title_c} || 0);
    my $pub_s = $dbh->quote($s->{pub_s} || 0);
    my $pub_c = $dbh->quote($s->{pub_c} || 0);
    my $pblshr_s = $dbh->quote($s->{pblshr_s} || 0);
    my $pblshr_c = $dbh->quote($s->{pblshr_c} || 0);
    my $auth1_s = $dbh->quote($s->{auth1_s} || 0);
    my $auth1_c = $dbh->quote($s->{auth1_c} || 0);
    my $auth2_s = $dbh->quote($s->{auth2_s} || 0);
    my $auth2_c = $dbh->quote($s->{auth2_c} || 0);
    my $pubyr_s = $dbh->quote($s->{pubyr_s} || 0);
    my $pubyr_c = $dbh->quote($s->{pubyr_c} || 0);
    my $volume_s = $dbh->quote($s->{volume_s} || 0);
    my $volume_c = $dbh->quote($s->{volume_c} || 0);
    my $pages_s = $dbh->quote($s->{pages_s} || 0);
    my $pages_c = $dbh->quote($s->{pages_c} || 0);
    my $debugstr = $dbh->quote($s->{debugstr});
    
    my $sql = "REPLACE INTO $TABLE{REFERENCE_SCORES} (reference_no, refsource_no,
	rs_index, debugstr, count_s, count_c, complete_s, complete_c, sum_s, sum_c,
	title_s, title_c, pub_s, pub_c, auth1_s, auth1_c, auth2_s, auth2_c, 
	pubyr_s, pubyr_c, volume_s, volume_c, pages_s, pages_c, pblshr_s, pblshr_c)
	VALUES ($refno, $sourceno, $idx, $debugstr, $count_s, $count_c,
	$complete_s, $complete_c, $sum_s, $sum_c, $title_s, $title_c, $pub_s, $pub_c, 
	$auth1_s, $auth1_c, $auth2_s, $auth2_c, $pubyr_s, $pubyr_c, $volume_s, $volume_c,
	$pages_s, $pages_c, $pblshr_s, $pblshr_c)";
    
    print STDERR "$sql\n\n" if $ds->{debug};
    
    my $result;
    
    eval {
	$result = $dbh->do($sql);
    };
    
    if ( $@ )
    {
	print STDERR "$sql\n\n" unless $ds->{debug};
    }
    
    return $result;
}


# update_match_scores ( match_record, new_scores )
#
# Given a match record and a hashref of new scores, check to see if any of the new scores are
# different from the current ones. If so, update the corresponding record in the REFERENCE_SCORES
# table. This method returns the number of updated score variables if an update is successfully
# made, and undef otherwise.

sub update_match_scores {
    
    my ($ds, $m, $scores) = @_;
    
    # Make sure we have a refsource_no.
    
    unless ( $m->{refsource_no} )
    {
	print STDERR "ERROR: no refsource_no found for score update\n";
	return;
    }
    
    # Run through all the possible scores, and accumulate a list of update clauses in each case
    # where $s has a defined value that is different from $m.
    
    my $dbh = $ds->{dbh};
    
    my (@update_list, %update);
    
    foreach my $name ( @SCORE_VARS )
    {
	my $name_s = $name . "_s";
	my $name_c = $name . "_c";
	
	if ( defined $scores->{$name_s} && (! defined $m->{$name_s} ||
					    $scores->{$name_s} ne $m->{$name_s}) )
	{
	    push @update_list, "$name_s = " . $dbh->quote($scores->{$name_s});
	    $update{$name_s} = $scores->{$name_s};
	}
	
	if ( defined $scores->{$name_c} && (! defined $m->{$name_c} ||
					    $scores->{$name_c} ne $m->{$name_c}) )
	{
	    push @update_list, "$name_c = " . $dbh->quote($scores->{$name_c});
	    $update{$name_c} = $scores->{$name_c};
	}
    }

    if ( defined $scores->{debugstr} && $scores->{debugstr} ne $m->{debugstr} )
    {
	push @update_list, "debugstr = " . $dbh->quote($scores->{debugstr});
	$update{debug} = $scores->{debugstr};
    }
    
    # If there are any scores to update, generate and execute an SQL statement. If it executes
    # successfully, update the values in $m and return the number of updated scores.
    
    if ( @update_list )
    {
	my $update_string = join(', ', @update_list);

	my $update_count = scalar(@update_list);
	
	my $quoted = $dbh->quote($m->{refsource_no});
	
	my $sql = "UPDATE $TABLE{REFERENCE_SCORES} SET $update_string
		WHERE refsource_no = $quoted";
	
	print STDERR "$sql\n\n" if $ds->{debug};
	
	local $dbh->{RaiseError};
	local $dbh->{PrintError} = 1;
	
	my $result = $dbh->do($sql);
	
	# If the update succeeded, actually update the corresponding values in the match record
	# and then return the number of scores that were updated.
	
	if ( $result )
	{
	    foreach my $key ( keys %update )
	    {
		$m->{$key} = $update{$key};
	    }
	    
	    return $update_count;
	}
    }
    
    # If no update was made, or if it was not successful, return undef.
    
    return;
}


# update_match_formatted ( match_record, reference_text )
#
# Given a match record and a formatted bibliographic reference text, store the latter in the
# REFERENCE_SCORES record corresponding to the former.

sub update_match_formatted {
    
    my ($ds, $m, $formatted) = @_;
    
    # Make sure we have a refsource_no.
    
    unless ( $m->{refsource_no} )
    {
	print STDERR "ERROR: no refsource_no found for score update\n";
	return;
    }
    
    # If $m->{match_formatted} exists and has the same value as $formatted, nothing needs to be
    # done. Otherwise, construct and execute an UPDATE statement.
    
    if ( $m->{match_formatted} ne $formatted )
    {
	my $dbh = $ds->{dbh};
	
	my $quoted_id = $dbh->quote($m->{refsource_no});
	
	my $quoted_text = $dbh->quote($formatted);
	
	my $sql = "UPDATE $TABLE{REFERENCE_SCORES} SET formatted=$quoted_text
		WHERE refsource_no=$quoted_id";
	
	print STDERR "$sql\n\n" if $ds->{debug};
	
	my $result;
	
	local $dbh->{RaiseError};
	local $dbh->{PrintError} = 1;
	
	$dbh->do($sql);
    }
    
    else
    {
	return;
    }
}


# recount_scores ( )
# 
# Recompute the derived counts from the set of scores.

sub recount_scores {
    
    my ($ds, $selector) = @_;
    
    unless ( $selector =~ qr{ ^ \d+ $ | ^ all $ }xs )
    {
	print STDERR "ERROR: No reference number for recount.\n";
	return;
    }
    
    my $dbh = $ds->{dbh};

    my $sql;

    $sql = "UPDATE $TABLE{REFERENCE_SCORES} SET
	sum_s = title_s + pub_s + auth1_s + auth2_s + pubyr_s + volume_s + pages_s + pblshr_s,
	sum_c = title_c + pub_c + auth1_c + auth2_c + pubyr_c + volume_c + pages_c + pblshr_c";
    
    $sql .= "\nWHERE reference_no = $selector" if $selector ne 'all';
    
    print STDERR "$sql\n\n" if $ds->{debug};

    $dbh->do($sql);

    $sql = "UPDATE $TABLE{REFERENCE_SCORES} SET
	count_s = sign(title_s)+sign(pub_s)+sign(auth1_s)+sign(auth2_s)+
		sign(pubyr_s)+sign(volume_s)+sign(pages_s)+sign(pblshr_s),
	count_c = sign(title_c)+sign(pub_c)+sign(auth1_c)+sign(auth2_c)+
		sign(pubyr_c)+sign(volume_c)+sign(pages_c)+sign(pblshr_c)";
    
    $sql .= "\nWHERE reference_no = $selector" if $selector ne 'all';
    
    print STDERR "$sql\n\n" if $ds->{debug};
    
    $dbh->do($sql);

    $sql = "UPDATE $TABLE{REFERENCE_SCORES} SET
	complete_s = sign(title_s div 100)+sign(pub_s div 100)+sign(auth1_s div 100)+
		sign(auth2_s div 100)+sign(pubyr_s div 100)+sign(volume_s div 100)+
		sign(pages_s div 100)+sign(pblshr_s div 100),
	complete_c = sign(title_c div 100)+sign(pub_c div 100)+sign(auth1_c div 100)+
		sign(auth2_c div 100)+sign(pubyr_c div 100)+sign(volume_c div 100)+
		sign(pages_c div 100)+sign(pblshr_c div 100)";
    
    $sql .= "\nWHERE reference_no = $selector" if $selector ne 'all';
    
    print STDERR "$sql\n\n" if $ds->{debug};

    $dbh->do($sql);

    print STDERR "Updated all score counts.\n\n";
}


# set_manual ( score, is_match )
# 
# Sets the 'manual' field of a reference score record to either true (is a match), false (is not a
# match), or null (no decision made). This method is intended to be called in response to user input
# after viewing the match.

sub set_manual {
    
    my ($ds, $score, $is_match) = @_;
    
    my $dbh = $ds->{dbh};
    my $sql;
    
    my $mark_value = $is_match                        ? '1'
	           : defined $is_match && ! $is_match ? '0'
						      : 'NULL';

    my $score_id = ref $score eq 'HASH'		     ? $score->{refsource_no}
		 : ! ref $score && $score =~ /^\d+$/ ? $score
						     : undef;
    if ( $score_id && $score_id =~ /^\d+$/ )
    {
	$sql = "UPDATE $TABLE{REFERENCE_SCORES} SET manual = $mark_value
		WHERE refsource_no = $score_id";

	print STDERR "$sql\n\n" if $ds->{debug};

	my $result = $dbh->do($sql);

	my ($new) = $dbh->selectrow_array("SELECT manual FROM $TABLE{REFERENCE_SCORES}
		WHERE refsource_no = $score_id");
	
	if ( ref $score eq 'HASH' )
	{
	    $score->{manual} = $new;
	}
	
	return $new;
    }
    
    else
    {
	croak "no refscore_no given";
    }
}


# count_matching_scores ( expr )
#
# Return the count of records in the REFERENCE_SCORES table that match the specified expression.
# Also return the number of known positives and known negatives that match it.

sub count_matching_scores {
    
    my ($ds, $expr) = @_;
    
    unless ( $expr && $expr ne '()' )
    {
	croak "No sql expression specified\n";
    }
    
    unless ( $expr =~ qr{ ^ [(] .* [)] $ }xs )
    {
	$expr = "($expr)";
    }
    
    my $dbh = $ds->{dbh};
    my $source_select = '';
    
    if ( $ds->{source} && $ds->{source} ne 'all' )
    {
	$source_select = " and sc.source = " . $dbh->quote($ds->{source});
    }
    
    my ($sql, $matches, $positive, $negative);
    
    # Generate and execute the relevant SQL expression.
    
    $sql = "SELECT count(*) as matches, count(if(manual=1,1,null)) as positive,
		count(if(manual=0,1,null)) as negative FROM $TABLE{REFERENCE_SCORES} as sc
	WHERE $expr$source_select";
	
    print STDERR "$sql\n\n" if $ds->{debug};
    
    ($matches, $positive, $negative) = $dbh->selectrow_array($sql);
    
    return ($matches, $positive, $negative);
}


# list_matching_scores ( expr, mode, count, limit )
# 
# Return a list of reference score records matching the specified SQL expression. If $mode is
# 'random', select a random segment of the matching records. Otherwise, just list them in the
# order returned by the database. If parameter $count is defined, it should be the number of
# matching records as returned by a previous call to 'count_matching_scores' (see above). The
# parameter $limit specifies the maximum number of records to return.
#
# The given expression should be enclosed in parentheses unless it is okay as-is to conjoin with
# one or more simple clauses.
#
# If no count is given, the records are returned sequentially in the default order regardless of
# the value of $mode. If no limit is given, a default limit of 20 is assumed.

sub list_matching_scores {

    my ($ds, $expr, $mode, $count, $limit) = @_;
    
    unless ( $expr && $expr ne '()' )
    {
	croak "No sql expression specified\n";
    }

    $limit ||= 20;
    
    my $dbh = $ds->{dbh};
    my $source_select = '';
    
    if ( $ds->{source} && $ds->{source} ne 'all' )
    {
	$source_select = " and sc.source = " . $dbh->quote($ds->{source});
    }
    
    my ($sql, $matches, $positive, $negative);
    
    # Generate and execute the relevant SQL expression.
    
    $sql = "SELECT sc.*, r.*, s.items,
	    if(sc.formatted is null, s.response_data, '') as response_data
	FROM $TABLE{REFERENCE_SCORES} as sc
	    join $TABLE{REFERENCE_DATA} as r using (reference_no)
	    join $TABLE{REFERENCE_SOURCES} as s using (refsource_no)
	WHERE $expr$source_select";
    
    # If the mode is 'random' and the count is given and is not more than 50, just do a simple
    # random order.
    
    if ( $mode eq 'random' && $count && $count <= 50 )
    {
	$sql .= " ORDER BY RAND() LIMIT $limit";
    }
    
    # Otherwise, if the count is greater than the limit, pick a random offset (possibly 0) and
    # return rows sequentially from there.
    
    elsif ( $mode eq 'random' && $count && $count > $limit )
    {
	my $offset = int(rand($count - $limit + 1));
	
	$sql .= $offset > 0 ? " LIMIT $limit OFFSET $offset" : "LIMIT $limit";
    }
    
    # If the mode is not 'random' or if no count is given, or if the count is less than or equal
    # to the limit, just return the matching records in their default order with the specified
    # limit.
    
    else
    {
	$sql .= " LIMIT $limit";
    }
    
    print STDERR "$sql\n\n" if $ds->{debug};
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    if ( $result && ref $result eq 'ARRAY' )
    {
	return @$result;
    }
    
    return;
}


# compare_scores ( expr1, expr2, limit )
#
# Count two categories of REFERENCE_SCORES records: those which match expr1 but not expr2, and those which
# match expr2 but not expr1. Return the two counts as a list.

sub compare_scores {
    
    my ($ds, $expr_a, $expr_b) = @_;
    
    unless ( $expr_a )
    {
	croak "No sql expression specified\n";
    }
    
    my $dbh = $ds->{dbh};
    my $source_select = '';
    
    if ( $ds->{source} && $ds->{source} ne 'all' )
    {
	$source_select = " and sc.source = " . $dbh->quote($ds->{source});
    }

    my ($sql_a, $sql_b);
    my $count_a = 0;
    my $count_b = 0;

    # Generate and execute the relevant SQL expressions for a and b. If b is empty, only do a.
    
    if ( $expr_a && $expr_b )
    {
	$sql_a = "SELECT count(*) FROM $TABLE{REFERENCE_SCORES} as sc
	WHERE ($expr_a) and not ($expr_b)$source_select";
	
	print STDERR "$sql_a\n\n" if $ds->{debug};
	
	($count_a) = $dbh->selectrow_array($sql_a);
	
	$sql_b = "SELECT count(*) FROM $TABLE{REFERENCE_SCORES} as sc
	WHERE ($expr_b) and not ($expr_a)$source_select";

	print STDERR "$sql_b\n\n" if $ds->{debug};

	($count_b) = $dbh->selectrow_array($sql_b);
    }
    
    elsif ( $expr_a )
    {
	$sql_a = "SELECT count(*) FROM $TABLE{REFERENCE_SCORES} as sc
	WHERE ($expr_a)$source_select";
	
	print STDERR "$sql_a\n\n" if $ds->{debug};
	
	($count_a) = $dbh->selectrow_array($sql_a);
    }
    
    # Then return the results.
    
    return ($count_a, $count_b);
}


sub compare_list_scores {

    my ($ds, $expr_a, $expr_b, $limit) = @_;
    
    unless ( $limit && $limit =~ /^\d+$/ )
    {
	croak "Invalid limit '$limit'\n";
    }
    
    unless ( $expr_a )
    {
	croak "No SQL expression specified\n";
    }
    
    my $dbh = $ds->{dbh};
    my $source_select = '';
    
    if ( $ds->{source} && $ds->{source} ne 'all' )
    {
	$source_select = " and sc.source = " . $dbh->quote($ds->{source});
    }
    
    my ($sql, $result);

    # Generate and execute the appropriate SQL query.
    
    if ( $expr_a && $expr_b )
    {
	$sql = "SELECT sc.*, r.*, s.items,
	    if(sc.formatted is null, s.response_data, '') as response_data
	FROM $TABLE{REFERENCE_SCORES} as sc
	    join $TABLE{REFERENCE_DATA} as r using (reference_no)
	    join $TABLE{REFERENCE_SOURCES} as s using (refsource_no)
	WHERE ($expr_a) and not ($expr_b)$source_select LIMIT $limit";
	
	print STDERR "$sql\n\n" if $ds->{debug};
	
	$result = $dbh->selectall_arrayref($sql, { Slice => { } });
    }
    
    elsif ( $expr_a )
    {
	$sql = "SELECT sc.*, r.*, s.items,
	    if(sc.formatted is null, s.response_data, '') as response_data
	FROM $TABLE{REFERENCE_SCORES} as sc
	    join $TABLE{REFERENCE_DATA} as r using (reference_no)
	    join $TABLE{REFERENCE_SOURCES} as s using (refsource_no)
	WHERE ($expr_a)$source_select LIMIT $limit";
	
	print STDERR "$sql\n\n" if $ds->{debug};
	
	$result = $dbh->selectall_arrayref($sql, { Slice => { } });
    }
    
    # If there are no results, return the empty list.

    return unless $result && @$result;
    
    # Go through the result list and generate formatted output for each row if we don't already
    # have it.
    
  ROW:
    foreach my $r ( @$result )
    {
	$ds->format_scored_match($r);
    }

    # Then return the list.

    return @$result;
}


# format_match ( match_record )
#
# Given a hashref of attributes representing a potential match for a paleobiodb reference from an
# external source, Generate human-readable bibliographic reference text items representing both
# sets of data: the paleobiodb record and the externally fetched record.
# 
# The hashref passed to this method should have been generated originally by
# 'list_matching_scores', which ensures that it includes all of the necessary fields from the
# paleobiodb REFERENCE_DATA table (as r.* in the SELECT statement from 'list_matching_scores', see
# above). It will be augmented with fields 'ref_formatted' and 'match_formatted'.

sub format_match {
    
    my ($ds, $m) = @_;
    
    # Make sure we have a hashref that includes a refsource_no value and an rs_index value. This
    # identifies exactly which scored match we are working with, and causes processing to be
    # aborted if we have been handed a bad argument.
    
    unless ( ref $m eq 'HASH' && $m->{refsource_no} )
    {
	croak "ERROR: no refsource_no\n";
    }

    unless ( defined $m->{rs_index} && $m->{rs_index} ne '' )
    {
	croak "ERROR: no rs_index\n";
    }
    
    # Generate bibliographic reference text using the attributes from the corresponding record in
    # the paleobiodb REFERENCE_DATA table.
    
    $m->{ref_formatted} = $ds->format_ref($m);
    
    # If bibliographic reference text has already been generated and stored for the external data
    # to be matched, it will be included under the key 'formatted'. In that case, simply rename
    # this field to 'match_formatted'.
    
    if ( $m->{formatted} )
    {
	$m->{match_formatted} = $m->{formatted};
	delete $m->{formatted};
    }

    # If we have already processed this record and already have a value in match_formatted,
    # nothing else needs to be done. Otherwise, decode the response_data associated with this
    # scored match and use the extracted attributes to generate bibliographic reference text.
    
    elsif ( ! $m->{match_formatted} )
    {
	# Start by decoding the response_data for this match and extracting the bibliographic
	# reference items from the it. If $m->{response_data} is empty, the
	# extract_response_items method will make a query for the corresponding data.
	
	my @items = $ds->extract_response_items($m);
	
	# Now that we know the item count, we can store that number in the REFERENCE_SOURCES table if
	# it isn't already there.
	
	my $dbh = $ds->{dbh};
	
	if ( ! defined $m->{items} || $m->{items} != @items )
	{
	    $m->{items} = scalar(@items);
	    
	    my $quoted_id = $dbh->quote($m->{refsource_no});
	    my $quoted_count = $dbh->quote($m->{items});
	    
	    my $sql_u = "UPDATE $TABLE{REFERENCE_SOURCES} SET items=$quoted_count
		WHERE refsource_no=$quoted_id LIMIT 1";
	    
	    print STDERR "$sql_u\n\n" if $ds->{debug};

	    # If an error occurs while storing the data, just print it out and don't throw an
	    # exception.
	    
	    local $dbh->{RaiseError};
	    local $dbh->{PrintError} = 1;
	    
	    $dbh->do($sql_u);
	}
	
	# If we found some items, pick the selected item and generate a formatted bibliographic
	# reference text from the item data.
	
	my $index = $m->{rs_index} > 0 ? $m->{rs_index} : 0;
	
	if ( @items && $index < @items )
	{
	    $m->{match_formatted} = $ds->format_ref($items[$index]);
	    
	    # Store the generated text in the 'formatted' column of the score record.
	    
	    my $quoted_text = $dbh->quote($m->{match_formatted});
	    my $quoted_id = $dbh->quote($m->{refsource_no});
	    
	    my $sql_s = "UPDATE $TABLE{REFERENCE_SCORES} SET formatted=$quoted_text
		WHERE refsource_no=$quoted_id and rs_index=$index LIMIT 1";
	    
	    print STDERR $sql_s . "\n\n" if $ds->{debug};
	    
	    # If an error occurs while storing the data, just print it out and don't throw an
	    # exception.
	    
	    local $dbh->{RaiseError};
	    local $dbh->{PrintError} = 1;
	    
	    $dbh->do($sql_s);
	}
	
	# If there aren't enough items, throw an exception.
	
	else
	{
	    my $item_count = scalar(@items);
	    die "item '$m->{rs_index}' requested for refsource_no='$m->{refsource_no}', $item_count items were found";
	}
    }
}


# get_match_data ( match_record )
#
# Return the bibliographic reference data corresponding to the specified match record. Much of
# this code is quite similar to 'format_match' above.

sub get_match_data {
    
    my ($ds, $m) = @_;
    
    # Make sure we have a hashref that includes a refsource_no value and an rs_index value. This
    # identifies exactly which scored match we are working with, and causes processing to be
    # aborted if we have been handed a bad argument.
    
    unless ( ref $m eq 'HASH' && $m->{refsource_no} )
    {
	croak "ERROR: no refsource_no\n";
    }

    unless ( defined $m->{rs_index} && $m->{rs_index} ne '' )
    {
	croak "ERROR: no rs_index\n";
    }
    
    # Start by decoding the response_data for this match and extracting the bibliographic
    # reference items from the it. If $m->{response_data} is empty, the extract_response_items
    # method will make a query for the corresponding data.
    
    my @items = $ds->extract_response_items($m);
    
    # Now that we know the item count, we can store that number in the REFERENCE_SOURCES table if
    # it isn't already there.
    
    if ( ! defined $m->{items} || $m->{items} != @items )
    {
	$m->{items} = scalar(@items);
	
	my $dbh = $ds->{dbh};
	my $quoted_id = $dbh->quote($m->{refsource_no});
	my $quoted_count = $dbh->quote($m->{items});
	
	my $sql_u = "UPDATE $TABLE{REFERENCE_SOURCES} SET items=$quoted_count
		WHERE refsource_no=$quoted_id LIMIT 1";
	
	print STDERR "$sql_u\n\n" if $ds->{debug};
	
	# If an error occurs while storing the data, just print it out and don't throw an
	# exception.
	
	local $dbh->{RaiseError};
	local $dbh->{PrintError} = 1;
	
	$dbh->do($sql_u);
    }
    
    # If we found some items, return the selected item.
    
    my $index = $m->{rs_index} > 0 ? $m->{rs_index} : 0;
    
    if ( @items && $index < @items )
    {
        return $items[$index];
    }
    
    # If there aren't enough items, throw an exception.
    
    else
    {
	my $item_count = scalar(@items);
	die "item '$m->{rs_index}' requested for refsource_no='$m->{refsource_no}', $item_count items were found";
    }
}


# extract_response_items ( match_record )
# 
# Decode the specified response data and return the list of bibliographic reference items it
# includes. If 'response_data' is not provided, query for it using the value of 'refsource_no'.

sub extract_response_items {

    my ($ds, $m) = @_;
    
    my $response_data = $m->{response_data};
    
    # If we weren't given any value for $response_data, fill it in by querying the database.
    
    unless ( $response_data )
    {
	my $dbh = $ds->{dbh};
	my $quoted_id = $dbh->quote($m->{refsource_no});
	
	my $sql = "SELECT response_data FROM $TABLE{REFERENCE_SOURCES} WHERE refsource_no=$quoted_id";
	
	print STDERR "$sql\n\n" if $ds->{debug};
	
	($response_data) = $dbh->selectrow_array($sql);
	
	unless ( $response_data )
	{
	    die "empty response_data for refsource_no=$quoted_id\n";
	}
    }
    
    # Decode $response_data, whose content should be encoded in JSON. If the raw data includes the
    # sequence \uxxxx where x is a hexadecimal digit, that means the data was stored
    # incorrectly. Those sequences represent non-ascii unicode characters which should have been
    # translated into utf-8 and stored in the database as such. We fix this by substituting each
    # one with the correspondingly numbered character and then running the result through
    # encode_utf8. If we don't find any of those sequences, we assume that the raw data is either
    # already encoded into utf8 or else contains only ASCII characters. In either case, no
    # encoding is needed. The end result will be a decoded data structure with all character data
    # encoded in utf8 or ascii which for our purposes is equivalent. This allows each reference
    # attribute to be properly compared with utf8 character data retrieved from the paleobiodb
    # REFERENCE_DATA table.
    
    if ( $response_data =~ s/\\u([0-9a-zA-Z]{4})/chr(hex($1))/ge )
    {
	utf8::upgrade($response_data);
    }
    
    # If an error is thrown during JSON decoding, it will be caught by the caller.
    
    my $data = from_json($response_data);
    
    # The rest of our task is to identify and return just the list of bibliographic reference
    # items, ignoring the status info and other stuff that isn't useful to us.

    # Responses from the Crossref API contain the items as an array under the key 'items' inside
    # 'message'.
    
    if ( ref $data eq 'HASH' && ref $data->{message}{items} eq 'ARRAY' )
    {
	return @{$data->{message}{items}};
    }
    
    # Responses from the Xdd API contain the items as an array under the key 'data' inside 'success'.
    
    elsif ( ref $data eq 'HASH' && ref $data->{success}{data} eq 'ARRAY' )
    {
	return @{$data->{success}{data}};
    }
    
    # If the entire response is a JSON array, and the first item contains the key 'deposited' or
    # 'title', assume that these are bibliographic reference items and just return the array contents.
    
    elsif ( ref $data eq 'ARRAY' && ( $data->[0]{deposited} || $data->[0]{title} ) )
    {
	return @$data;
    }
    
    # Otherwise, throw an exception.
    
    else
    {
	die "ERROR: unrecognized response format for refsource_no='$m->{refsource_no}'\n";
    }
}


# select_matching_refs ( reference_attrs, selector )
# 
# Select matches for the specified reference attributes from the REFERENCE_DATA (refs)
# table. The attributes must be given as a hashref.

sub select_matching_refs_old {
    
    my ($ds, $selector, $attrs, $options) = @_;
    
    $ds->clear_selection;

    my $dbh = $ds->{dbh};
    my $source = ref $options eq 'HASH' && $options->{source} || $ds->{source};
    
    my @matches;
    
    # If a selector is given, generate the appropriate filter and join clauses.
    
    my $join = '';
    my $filter = '';
    my $group = '';
    
    my $source_selector = $source && $source ne 'all' ? "and s.source = '$source'" : '';
    
    my $limit = ref $options eq 'HASH' && $options->{limit}
	? "LIMIT $options->{limit}" : '';
    
    if ( $selector eq 'unfetched' )
    {
	$join = "left join $TABLE{REFERENCE_SOURCES} as on s.reference_no = refs.reference_no
		  and s.eventtype = 'fetch' and s.status rlike '^2..' $source_selector";
	$filter = "and s.reference_no is null";
    }
    
    elsif ( $selector eq 'fetched' )
    {
	$join = "join $TABLE{REFERENCE_SOURCES} as on s.reference_no = refs.reference_no
		  and s.eventtype = 'fetch' and s.status rlike '^2..' $source_selector";
	$group = "GROUP BY refs.reference_no";
    }
    
    elsif ( $selector eq 'unchecked' )
    {
	$join = "left join $TABLE{REFERENCE_SOURCES} as on s.reference_no = refs.reference_no
		  and s.eventtype = 'fetch' $source_selector";
	$filter = "and s.reference_no is null";
    }
    
    elsif ( $selector eq 'checked' )
    {
	$join = "join $TABLE{REFERENCE_SOURCES} as on s.reference_no = refs.reference_no
		  and s.eventtype = 'fetch' $source_selector";
	$group = "GROUP BY refs.reference_no";
    }
    
    elsif ( $selector eq 'unscored' )
    {
	$join = "left join $TABLE{REFERENCE_SCORES} as sc
		  on sc.reference_no = refs.reference_no $source_selector";
	$filter = "and sc.reference_no is null";
    }
    
    elsif ( $selector eq 'scored' )
    {
	$join = "join $TABLE{REFERENCE_SCORES} as sc using (reference_no)";
	$filter = $source_selector;
	$group = "GROUP BY reference_no";
    }
    
    # If a doi was given, find all references with that doi. Compare them all to the given
    # attributes; if no other attributes were given, each one gets a score of 90 plus the number
    # of important attributes with a non-empty value. The idea is to select the matching reference
    # record that has the greatest amount of information filled in.
    
    if ( $attrs->{doi} )
    {
	my $quoted = $dbh->quote($attrs->{doi});
	
	my $sql = "SELECT * FROM refs $join WHERE doi = $quoted $filter $group $limit";
	
	print STDERR "$sql\n\n" if $ds->{debug};
	
	my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
	
	@matches = @$result if $result && ref $result eq 'ARRAY';

	$ds->{selection_sql} = $sql;
	
	# Assign match scores.
	
	foreach my $m ( @matches )
	{
	    my $score = 90;
	    $score++ if $m->{reftitle};
	    $score++ if $m->{pubtitle};
	    $score++ if $m->{author1last};
	    $score++ if $m->{author2last};
	    $score++ if $m->{pubvol};
	    $score++ if $m->{pubno};
	    $score++ if $m->{firstpage};
	    $score++ if $m->{lastpage};
	    
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
	
	if ( $attrs->{reftitle} && $attrs->{pubtitle} )
	{
	    my $refquoted = $dbh->quote($attrs->{reftitle});
	    my $pubquoted = $dbh->quote($attrs->{pubtitle});

	    $base = "SELECT refs.*, match(refsearch.reftitle) against($refquoted) as score1,
		  match(refsearch.pubtitle) against ($pubquoted) as score2
		FROM refs join refsearch using (reference_no) $join";
	    
	    $having = "score1 > 5 and score2 > 5";
	}
	
	elsif ( $attrs->{reftitle} )
	{
	    my $quoted = $dbh->quote($attrs->{reftitle});
	    
	    $base = "SELECT refs.*, match(refsearch.reftitle) against($quoted) as score
		FROM refs join refsearch using (reference_no) $join";
	    
	    $having = "score > 5";
	}
	
	elsif ( $attrs->{pubtitle} )
	{
	    my $quoted = $dbh->quote($attrs->{pubtitle});
	    
	    $base = "SELECT refs.*, match(refsearch.pubtitle) against($quoted) as score
		FROM refs join refsearch using (reference_no) $join";
	    
	    $having = "score > 0";
	}
	
	else
	{
	    $base = "SELECT * FROM refs $join";
	}
	
	# Then add clauses to restrict the selection based on pubyr and author names.
	
	my @clauses;
	
	if ( $attrs->{pubyr} )
	{
	    my $quoted = $dbh->quote($attrs->{pubyr});
	    push @clauses, "refs.pubyr = $quoted";
	}
	
	if ( $attrs->{author1last} && $attrs->{author2last} )
	{
	    my $quoted1 = $dbh->quote($attrs->{author1last});
	    my $quoted2 = $dbh->quote($attrs->{author2last});
	    
	    push @clauses, "(refs.author1last sounds like $quoted1 and refs.author2last sounds like $quoted2)";
	}
	
	elsif ( $attrs->{author1last} )
	{
	    my $quoted1 = $dbh->quote($attrs->{author1last});
	    
	    push @clauses, "refs.author1last sounds like $quoted1";
	}

	if ( $attrs->{anyauthor} )
	{
	    my $quoted1 = $dbh->quote($attrs->{anyauthor});
	    my $quoted2 = $dbh->quote('%' . $attrs->{anyauthor} . '%');
	    
	    push @clauses, "(refs.author1last sounds like $quoted1 or refs.author2last sounds like $quoted1 or refs.otherauthors like $quoted2)";
	}
	
	# Now put the pieces together into a single SQL statement and execute it.
	
	my $sql = $base;
	
	if ( @clauses )
	{
	    $sql .= "\n\t\tWHERE " . join(' and ', @clauses) . " $filter";
	}

	if ( $group )
	{
	    $sql .= " $group";
	}
	
	if ( $having )
	{
	    $sql .= "\n\t\tHAVING $having";
	}

	if ( $limit )
	{
	    $sql .= " $limit";
	}
	
	print STDERR "$sql\n\n" if $ds->{debug};
	
	my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
	
	$ds->{selection_sql} = $sql;
	
	# If we get results, look through them and keep any that have even a slight chance of
	# matching.
	
	if ( $result && ref $result eq 'ARRAY' )
	{
	    foreach my $m ( @$result )
	    {
		if ( $m->{score1} || $m->{score2} )
		{
		    $m->{score} = $m->{score1} + $m->{score2};
		}
		
		push @matches, $m;
	    }
	}
    }
    
    # Now sort the matches in descending order by score.
    
    my @sorted = sort { $b->{score} <=> $a->{score} } @matches;

    $ds->{selection_list} = \@sorted;
    $ds->{selection_count} = scalar(@sorted);
}


sub ref_from_refno {
    
    my ($ds, $reference_no) = @_;
    
    return unless $reference_no && $reference_no =~ /^\d+$/;
    
    my $dbh = $ds->{dbh};
    
    my $sql = "SELECT * FROM $TABLE{REFERENCE_DATA} WHERE reference_no = $reference_no";
    
    print STDERR "$sql\n\n" if $ds->{debug};
    
    my $result = $dbh->selectrow_hashref($sql);
    
    return $result && $result->{reference_no} ? $result : ();
}


sub ref_from_sourceno {

    my ($ds, $refsource_no) = @_;

    return unless $refsource_no && $refsource_no =~ /^\d+$/;
    
    my $dbh = $ds->{dbh};
    
    my $sql = "SELECT r.*, s.refsource_no
	FROM $TABLE{REFERENCE_DATA} as r join $TABLE{REFERENCE_SOURCES} as s using (reference_no)
	WHERE s.refsource_no = $refsource_no";
    
    print STDERR "$sql\n\n" if $ds->{debug};
    
    my $result = $dbh->selectrow_hashref($sql);
    
    return $result && $result->{reference_no} ? $result : ();
}
    
    
# format_ref ( attrs )
#
# Return a string of text, formatted in Chicago B style, representing the bibliographic reference
# data contained in the given record. This routine handles both the pbdb reference attributes and
# the crossref reference attributes.

sub format_ref {

    my ($ds, $r) = @_;
    
    # First format the author string. If there is an 'author' attribute that is a non-empty list,
    # use that. In Chicago B style, the first author goes "family, given" while all the other
    # authors are "given family". For compactness, we will report given names as initials even if
    # the full name is given.
    
    my $authorstring;
    
    if ( $r->{author} && ref $r->{author} eq 'ARRAY' && @{$r->{author}} )
    {
	$authorstring = format_authorlist($r->{author}, 'author');
    }
    
    # If the 'author' field is a string, just use that.
    
    elsif ( $r->{author} && ! ref $r->{author} )
    {
	$authorstring = $r->{author};
    }
    
    # Otherwise, if this is a PBDB refs record, format the author string using those fields. We
    # present the author names exactly as they appear, instead of trying to canonicalize
    # them. Code that attempted to do that has been commented out.
    
    elsif ( $r->{author1last} )
    {
	my $ai1 = $r->{author1init} || '';
	my $al1 = $r->{author1last};
	
	# $ai1 =~ s/\.//g;
	# $ai1 =~ s/([[:alpha:]])/$1./g;
	
	my $auth1 = $al1;
	$auth1 .= ", $ai1" if $ai1 ne '';
	
	my $ai2 = $r->{author2init} || '';
	my $al2 = $r->{author2last} || '';
	
	# $ai2 =~ s/\.//g;
	# $ai2 =~ s/([[:alpha:]])/$1./g;
	
	my $auth2 = $ai2;
	$auth2 .= ' ' if $ai2 ne '' && $al2 ne '';
	$auth2 .= $al2;
	
	my $auth3 = $r->{otherauthors} || '';
	
	# $auth3 =~ s/\.//g;
	# $auth3 =~ s/\b(\w)\b/$1./g;
	
	# Then construct the author string
	
	$authorstring = $auth1;
	
	if ( $auth2 =~ /et al/ )
	{
	    $authorstring .= " $auth2";
	}
	
	elsif ( $auth2 ne '' && $auth3 ne '' )
	{
	    $authorstring .= ", $auth2, $auth3";
	}
	
	elsif ( $auth2 )
	{
	    $authorstring .= " and $auth2";
	}
    }

    else
    {
	$authorstring = "unknown";
    }
    
    # Next, the publication year.

    my $pubyr = $r->{'published-print'} && extract_json_year($r->{'published-print'}) ||
	$r->{'published-online'} && extract_json_year($r->{'published-online'}) ||
	$r->{'issued'} && extract_json_year($r->{'issued'}) ||
	$r->{'approved'} && extract_json_year($r->{'approved'}) ||
	$r->{year} || $r->{pubyr} || '0000';
    
    # Next, the publication type. The set of publication types is different between BibTex,
    # Crossref, and PBDB. If the field 'type' is present, that could mean this is either a BibJSON
    # entry or a Crossref entry. A pbdb refs record has 'publication_type' instead.
    
    my $rawtype = $r->{type} || $r->{publication_type};
    my $pubtype;
    
    # Map the types into a canonical list.
    
    if ( $rawtype eq 'article' || $rawtype eq 'journal-article' ||
	 $rawtype eq 'journal article' )
    {
	$pubtype = 'journal-article';
    }
    
    elsif ( $rawtype eq 'conference' || $rawtype eq 'inproceedings' ||
	    $rawtype eq 'proceedings-article' )
    {
	$pubtype = 'proceedings-article';
    }
    
    elsif ( $rawtype eq 'book' || $rawtype eq 'edited-book' )
    {
	$pubtype = 'book';
    }
    
    elsif ( $rawtype eq 'incollection' || $rawtype eq 'book-section' || $rawtype eq 'book-track' )
    {
	$pubtype = 'book-section';
    }
    
    elsif ( $rawtype eq 'inbook' || $rawtype eq 'book-chapter' || $rawtype eq 'book chapter' )
    {
	$pubtype = 'book-chapter';
    }
    
    elsif ( $rawtype eq 'techreport' || $rawtype eq 'manual' || $rawtype eq 'monograph' ||
	    $rawtype eq 'report' || $rawtype eq 'serial monograph' ||
	    $rawtype eq 'news article' || $rawtype eq 'compendium' )
    {
	$pubtype = 'serial-monograph';
    }
    
    elsif ( $rawtype eq 'guidebook' )
    {
	$pubtype = 'guidebook';
    }
    
    elsif ( $rawtype eq 'mastersthesis' || $rawtype eq 'M.S. thesis' )
    {
	$pubtype = 'masters-thesis';
    }

    elsif ( $rawtype eq 'phdthesis' || $rawtype eq 'dissertation' || $rawtype eq 'Ph.D. thesis' )
    {
	$pubtype = 'phd-thesis';
    }

    elsif ( $rawtype eq 'unpublished' )
    {
	$pubtype = 'unpublished';
    }

    else
    {
	$pubtype = 'other';
    }

    # Next, format the title string. If there is a 'title' attribute that is a non-empty list, use
    # the first element of that.
    
    my $reftitle;
    
    if ( $r->{title} && ref $r->{title} eq 'ARRAY' && @{$r->{title}} )
    {
	$reftitle = $r->{title}[0];

	if ( $r->{subtitle} && ref $r->{subtitle} eq 'ARRAY' && @{$r->{subtitle}} )
	{
	    $reftitle .= ": $r->{subtitle}[0]";
	}
    }
    
    # If the title attribute is itself a string, use that.
    
    elsif ( $r->{title} && ! ref $r->{title} )
    {
	$reftitle = $r->{title};

	if ( $r->{subtitle} && ! ref $r->{subtitle} )
	{
	    $reftitle .= ": $r->{subtitle}";
	}
    }
    
    # Otherwise, if this is a pbdb refs record with a 'reftitle' attribute, use that. Some pbdb
    # refs have no reftitle, so use the pubtitle in that case. Default to 'unknown' if we can't
    # find a title at all.
    
    else
    {
	$reftitle = $r->{reftitle} || $r->{pubtitle} || 'unknown';
    }

    # If there are editors, format that list as well.

    my $editorstring = '';

    if ( $r->{editor} && ref $r->{editor} eq 'ARRAY' && @{$r->{editor}} )
    {
	$editorstring = format_authorlist($r->{editor}, 'editor');
    }

    # If the 'editor' attribute is a string, use that.

    elsif ( $r->{editor} && ! ref $r->{editor} )
    {
	$editorstring = $r->{editor};
    }
    
    # The pbdb refs field is 'editors', and its value is a simple string.
    
    elsif ( $r->{editors} && ! ref $r->{editors} )
    {
	$editorstring = $r->{editors};
    }
    
    # Next, format the publication title, publisher, volume, issue, and pages.
    
    my $pubtitle = '';
    
    # A Crossref record has the publication title(s) in 'container-title' as an array of strings.
    
    if ( $r->{'container-title'} && ref $r->{'container-title'} eq 'ARRAY' &&
	 @{$r->{'container-title'}} )
    {
	$pubtitle = $r->{'container-title'}[0];
    }
    
    # A BibJSON record has the publication title in either 'booktitle', 'series', or 'journal'. A
    # paleobiodb record has that information under 'pubtitle'.
    
    elsif ( $r->{booktitle} || $r->{series} || $r->{journal} || $r->{pubtitle} )
    {
	$pubtitle = $r->{booktitle} || $r->{series} || $r->{pubtitle};
    }
    
    # The volume, issue
    
    my $volume = $r->{volume} || $r->{pubvol};
    my $issue = $r->{number} || $r->{issue} || $r->{pubno};
    my $pages;
    
    # If this is a paleobiodb record, we need to put the page numbers together from the
    # 'firstpage' and 'lastpage' fields.
    
    if ( $r->{firstpage} || $r->{lastpage} )
    {
	$r->{firstpage} =~ s/-$//;	# Some firstpage values incorrectly end with a dash.
	
	$pages = $r->{firstpage} && $r->{lastpage} ? "$r->{firstpage}--$r->{lastpage}"
	    : $r->{firstpage} || $r->{lastpage};
    }
    
    # A Crossref record has the page numbers in 'page', whereas BibJSON uses 'pages'. 
    
    else
    {
	$pages = $r->{pages} || $r->{page};
	
	# If (as in most cases) the page numbers are two decimal integers separated by a dash,
	# change the dash to a double dash. At the same time, we can remove any extraneous
	# whitespace.
	
	if ( $pages =~ qr{ ^ \s* (\d+) \s* - \s* (\d+) \s* $ }xs )
	{
	    $pages = "$1--$2";
	}
    }
    
    my $volstring = $volume || '';
    
    if ( $issue )
    {
	$volstring .= ', ' if $volstring;
	$volstring .= "no. $issue";
    }

    my $publisher = $r->{publisher};
    my $pubcity = $r->{'publisher-location'} || $r->{address} || $r->{pubcity};

    my $pubstring = $publisher && $pubcity ? "$pubcity: $publisher" :
	$publisher || $pubcity || '';
    
    # If there is a DOI, build that.

    my $doi = $r->{doi} || $r->{DOI} || $r->{URL} || $r->{'published-print'}{DOI} ||
	$r->{'published-online'}{DOI};
    
    if ( $doi && $doi !~ /^http/ )
    {
	$doi = "https://doi.org/$doi";
    }
    
    # Now build the formatted output based on publication type.
    
    my $output;

    $authorstring .= '.' unless $authorstring =~ /[.]$/;
    
    # Article of any kind.
    
    if ( $pubtype =~ /^journal|^proceedings/ )
    {
	$output = "$authorstring $pubyr. \"$reftitle\" $pubtitle $volstring";
	$output .= " ($pubstring)" if $pubstring;
	$output .= ": $pages" if $pages;
    }
    
    # Book.
    
    elsif ( $pubtype eq 'book' )
    {
	$output = "$authorstring $pubyr. $reftitle";
	$output .= ", $volstring" if $volstring;
	$output .= " ed. $editorstring" if $editorstring;
	$output .= " ($pubstring)" if $pubstring;
	$output .= ": $pages" if $pages;
    }
    
    # Book chapter.
    
    elsif ( $pubtype =~ /^book/ )
    {
	$output = "$authorstring $pubyr. \"$reftitle\" in $pubtitle";
	$output .= ", $volstring" if $volstring;
	$output .= " ed. $editorstring" if $editorstring;
	$output .= " ($pubstring)" if $pubstring;
	$output .= ": $pages" if $pages;
    }

    # Thesis.

    elsif ( $pubtype =~ /^phd|^masters/ )
    {
	$output = "$authorstring $pubyr. \"$reftitle\"";
	$output .= $pubtype =~ /^phd/ ? " PhD diss" : " MS thesis";
	$output .= " ($pubstring)" if $pubstring;
	$output .= ": $pages" if $pages;
    }

    # Unpublished.
    
    elsif ( $pubtype eq 'unpublished' )
    {
	$output = "$authorstring $pubyr. \"$reftitle\" Unpublished work";
	$output .= ": $pages" if $pages;
    }

    # Anything else.

    else
    {
	$output = "$authorstring $pubyr. \"$reftitle\"";
	$output .= " in $pubtitle" if $pubtitle;
	$output .= ", $volstring" if $volstring;
	$output .= " ed. $editorstring" if $editorstring;
	$output .= " ($pubstring)" if $pubstring;
	$output .= ": $pages" if $pages;
    }

    # If there is a doi, append it.

    if ( $doi )
    {
	$output .= ". $doi";
    }

    # Add a final period.

    $output .= ".";

    return $output;
}


# format_authorlist ( authorlist, field )
#
# Format the specified list of authors as either authors or editors.

sub format_authorlist {

    my ($authorlist, $field) = @_;

    my @names;

    my $family_field = $authorlist->[0]{family} ? 'family' :
	$authorlist->[0]{last} ? 'last' :
	$authorlist->[0]{lastname} ? 'lastname' : '';
    
    foreach my $a ( @$authorlist )
    {
	my $family = $a->{family} || $a->{last} || $a->{lastname} || '';
	my $given = $a->{given} || $a->{first} || $a->{firstname} || '';
	
	# Some crossref authorlists are formatted incorrectly, interleaving author names with
	# author affiliations as separate items in the list. So we skip any entries that don't
	# have the same family name field as the first entry. Those are assumed to represent author
	# affiliations.
	
	next if $family_field && ! $a->{$family_field};
	
	# In the given name, transform each capitalized word into an initial.
	
	$given =~ s/([[:upper:]])[[:alpha:]]*[.]?/$1./g;
	$given =~ s/\s+$//;
	$given =~ s/[.]([[:alpha:]])/. $1/g;
	
	# The first name in an author list goes: 'family, given' if both those fields are
	# found. If we only have a 'name' field, use it as-is.
	
	unless ( @names || $field ne 'author' )
	{
	    if ( $family && $given )
	    {
		push @names, "$family, $given";
	    }
	    
	    elsif ( $family )
	    {
		push @names, $family;
	    }
	    
	    else
	    {
		push @names, ($a->{name} || 'unknown');
	    }
	}
	
	# The rest go: 'given family' if both those fields are found.
	
	else
	{
	    if ( $family && $given )
	    {
		push @names, "$given $family";
	    }
	    
	    elsif ( $family )
	    {
		push @names, $family;
	    }
	    
	    else
	    {
		push @names, ($a->{name} || 'unknown');
	    }
	}
	
	# Crossref may also provide ORCID (scalar) and affiliation (array with single subfield
	# name)
    }
    
    # If there are two authors, join with 'and'.
    
    if ( @names == 2 )
    {
	return "$names[0] and $names[1]";
    }
    
    # Otherwise, join with commas.
    
    return join(', ', @names);
}


# extract_json_year ( json_value )
#
# Extract a year from the JSON date values returned by crossref and possibly other sources. If the
# value is a nested array, return the first value of the first (generally only)
# subarray. Otherwise, extract the first four digit string found as the date.

sub extract_json_year {
    
    my ($value) = @_;
    
    if ( ref $value eq 'HASH' && $value->{'date-parts'} )
    {
	$value = $value->{'date-parts'};
    }
    
    if ( ref $value eq 'ARRAY' )
    {
	($value) = @$value;
    }
    
    if ( ref $value eq 'ARRAY' )
    {
	return $value->[0];
    }
    
    elsif ( ! ref $value && $value =~ /([12]\d\d\d)/ )
    {
	return $1;
    }
    
    else
    {
	return undef;
    }
}


sub format_scores_horizontal {

    my ($ds, $scores, $color_output) = @_;
    
    my $line1 = "stat      ";
    my $line2 = "similar   ";
    my $line3 = "conflict  ";
    
    foreach my $key ( @SCORE_VARS )
    {
	my $key1 = $key . '_s';
	my $key2 = $key . '_c';
	
	my $pos_on = $color_output && $scores->{$key1} > 0 ? "\033[0;32m" : "";
	my $pos_off = $color_output && $scores->{$key1} > 0 ? "\033[0m" : "";
	my $neg_on = $color_output && $scores->{$key2} > 0 ? "\033[0;31m" : "";
	my $neg_off = $color_output && $scores->{$key2} > 0 ? "\033[0m" : "";
	
	$line1 .= fixed_width($key, 10);
	$line2 .= $pos_on . fixed_width($scores->{$key1}, 10) . $pos_off;
	$line3 .= $neg_on . fixed_width($scores->{$key2}, 10) . $neg_off;
    }

    return "$line1\n\n$line2\n$line3\n";
}


sub fixed_width {
    
    return $_[0] . (' ' x ($_[1] - length($_[0])));
}


sub strcontent {

    my ($string, $start, $end) = @_;
    
    if ( $start =~ /^(\d+)..(\d+)$/ )
    {
	$start = $1;
	$end = $2;
    }

    $start ||= 0;
    $end ||= length($string) - 1;

    my $output = '';

    foreach my $i ( $start .. $end )
    {
	my $c = substr($string, $i, 1);
	my $l = $i < 10 ? " $i" : $i;
	my $o = ord($c);

	$output .= "$l '$c' $o\n";
    }

    return $output;
}


# init_tables ( dbh )
#
# Make sure that the necessary tables exist.

sub init_tables {
    
    my ($dbh) = @_;

    my $sql = "CREATE TABLE IF NOT EXISTS ref_sources



";
    
}

1;

