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
use ReferenceMatch qw(get_reftitle get_pubtitle get_authorname get_authorlist get_pubyr @SCORE_VARS);

our ($UA);



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


# Object methods
# --------------

# set_debug ( boolean )
# 
# Sets or clears the debug flag.

sub set_debug {
    
    my ($rs, $debug) = @_;
    
    $rs->{debug} = $debug;
}


# set_source ( source )
# 
# Changes the 'source' attribute.

sub set_source {
    
    my ($rs, $source) = @_;
    
    $rs->{source} = $source if $source;
}


# set_dbh ( dbh )
#
# Sets a new database handle.

sub set_dbh {

    my ($rs, $dbh) = @_;

    $rs->{dbh} = $dbh if $dbh;
}

# select_refs ( selector, clause )
#
# Selects all references that meet a specified criterion, and stores the statement handle where it
# can be read by 'get_next'. Available values for $selector are:
#
#   fetched, unfetched, scored, unscored, matched, unmatched
#
# If $clause is specified, it must be a valid SQL expression. This is conjoined to the WHERE
# clause in the SELECT statement. It may reference the 'refs' table as 'r', the REFERENCE_SOURCES
# table as 's', and the REFERENCE_SCORES table as 'sc'.

sub select_refs {
    
    my ($datasource, $selector, $clause) = @_;
    
    my $dbh = $datasource->{dbh};
    my $limit = $datasource->{limit};
    my $source_select = '';
    my $score_select = '';
    
    if ( $datasource->{source} && $datasource->{source} ne 'all' )
    {
	$source_select = ' and s.source = ' . $dbh->quote($datasource->{source});
	$score_select = ' and sc.source = ' . $dbh->quote($datasource->{source});
    }
    
    my $sql;

    if ( $selector eq 'all' )
    {
	$sql = "SELECT SQL_CALC_FOUND_ROWS r.* FROM $TABLE{REFERENCE_DATA} as r
	WHERE 1=1";
    }
    
    elsif ( $selector eq 'unfetched' )
    {
	$sql = "SELECT SQL_CALC_FOUND_ROWS r.* FROM $TABLE{REFERENCE_DATA} as r
	    left join $TABLE{REFERENCE_SOURCES} as s on r.reference_no = s.reference_no
		$source_select and s.eventtype = 'fetch' and s.status like '200%'
	WHERE s.reference_no is null";
    }
    
    elsif ( $selector eq 'fetched' )
    {
	$sql = "SELECT SQL_CALC_FOUND_ROWS r.* FROM $TABLE{REFERENCE_DATA} as r
	    join $TABLE{REFERENCE_SOURCES} as s on r.reference_no = s.reference_no
		$source_select and s.eventtype = 'fetch' and s.status like '200%'
	WHERE 1=1";
    }
    
    elsif ( $selector eq 'unscored' )
    {
	$sql = "SELECT r.* FROM $TABLE{REFERENCE_DATA} as r
		left join $TABLE{REFERENCE_SCORES} as sc on r.reference_no = sc.reference_no
	WHERE sc.reference_no is null";
    }
    
    elsif ( $selector eq 'scored' )
    {
	$sql = "SELECT SQL_CALC_FOUND_ROWS r.* FROM $TABLE{REFERENCE_DATA} as r
		join $TABLE{REFERENCE_SCORES} as sc on r.reference_no = sc.reference_no
	WHERE 1=1";
    }
    
    elsif ( $selector eq 'unmatched' )
    {
	$sql = "SELECT SQL_CALC_FOUND_ROWS r.* FROM $TABLE{REFERENCE_DATA} as r
		left join $TABLE{REFERENCE_SCORES} as sc on r.reference_no = sc.reference_no
	WHERE sc.matched is null or sc.matched = 0";
    }
    
    elsif ( $selector eq 'matched' )
    {
	$sql = "SELECT SQL_CALC_FOUND_ROWS r.* FROM $TABLE{REFERENCE_DATA} as r
		join $TABLE{REFERENCE_SCORES} as sc on r.reference_no = sc.reference_no and sc.matched
	WHERE 1=1";
    }
    
    else
    {
	croak "ERROR: unknown selector '$selector'\n";
    }
    
    if ( $clause )
    {
	if ( $clause =~ / or / )
	{
	    $sql .= " and ($clause)";
	}

	else
	{
	    $sql .= " and $clause"
	}
    }
    
    if ( $limit )
    {
	die "ERROR: invalid limit '$limit'\n" unless $limit =~ /^\d+$/;

	$sql .= " LIMIT $limit";
    }
    
    print STDERR "$sql\n\n" if $datasource->{debug};
    
    my $sth = $dbh->prepare($sql);
    
    $sth->execute();
    
    $datasource->{sth} = $sth;
    
    ($datasource->{refs_found}) = $dbh->selectrow_array("SELECT FOUND_ROWS()");
}


sub get_next {
    
    my ($datasource) = @_;
    
    my $sth = $datasource->{sth};

    croak "ERROR: nothing selected\n" unless $sth;
    
    return $sth->fetchrow_hashref();
}


# metadata_query ( attrs, max_items )
# 
# Perform a search on this data source using the specified set of reference attributes.
# 
# The first argument should be either a string consisting of a formatted bibliographic reference
# or some facsimile thereof, or a hashref of attributes using paleobiodb field names. If this
# search is intended to fetch data about an existing bibliographic reference record in the
# Paleobiology Database, the reference_no attribute should be included.
# 
# If a second argument is given, it specifies the maximum number of response items we want. If not
# given, it defaults to 1 which will return the single best match.

sub metadata_query {
    
    my ($datasource, $attrs, $max_items) = @_;
    
    # If the attrs parameter is a reference, it must be a hashref.
    
    if ( ref $attrs )
    {
	croak "first argument (attrs) must be a string or a hashref"
	    unless reftype $attrs eq 'HASH';
    }
    
    # The max_items parameter, if specified, must be a positive integer.
    
    if ( $max_items && $max_items !~ /^\d+$/ )
    {
	croak "second argument (max_items) must be a positive integer";
    }
    
    # The default number of query results to ask for is 1.
    
    $max_items ||= 1;
    
    # Generate a query url.

    my $source = $datasource->{source};
    my $ua = $datasource->{ua};
    
    my ($query_text, $query_url, $response, $status);
    
    if ( $source eq 'crossref' )
    {
	$query_text = ref $attrs ? $datasource->format_ref($attrs) : $attrs;
	
	if ( $query_text =~ /[[:alpha:]]{5}/ )
	{
	    my $encoded = uri_escape_utf8($query_text);
	    $query_url = "https://api.crossref.org/works?" .
		"rows=$max_items&query.bibliographic=$encoded";
	}
	
	else
	{
	    $status = '400 Invalid: no words in the query';
	}
    }
    
    elsif ( $source eq 'xdd' )
    {
	my $title = get_reftitle($attrs);
	my $pubyr = get_pubyr($attrs);
	
	unless ( $title && $title =~ /[[:alpha:]]{5}/ )
	{
	    $status = '400 Invalid: no words in the query';
	}
	
	unless ( $pubyr && $pubyr =~ /^\d\d\d\d$/ )
	{
	    $status = '400 Invalid: no publication year';
	}
	
	$title =~ s/[^[:alnum:][:space:]]+/ /g;
	$title =~ s/[[:space:]]+/+/g;
	
	my $minyr = $pubyr-1;
	my $maxyr = $pubyr+1;
	
	my $pubyr_select = "&min_published=$minyr&max_published=$maxyr";
	
	$query_text = "title=$title$pubyr_select";
	$query_url = "https://xdd.wisc.edu/api/articles?max=5&" . $query_text;
    }
    
    # If we have a query url and don't already have an error status, send off a request.
    
    if ( $query_url && ! $status )
    {
	$response = $ua->get($query_url);
	$status = $response->status_line;
	
	# If the response has a status code of 200, return the status line, the query text and
	# url, the content, and the character set in which the content is encoded. If the content
	# type starts with 'text/', the content will have been automatically decoded into Perl's
	# internal format. Otherwise, it will almost always be UTF-8.
	
	if ( $response->code eq '200' )
	{
	    my $content = $response->decoded_content;
	    my $charset = $response->content_charset;
	    my ($ctype) = $response->content_type;
	    
	    $charset = '' if $ctype && $ctype =~ qr{^text/};
	    
	    return $status, $query_text, $query_url, $content, $charset;
	}
	
	else
	{
	    return $status, $query_text, $query_url;
	}
    }
    
    else
    {
	$status ||= '400 Invalid: an error occurred';
	return $status;
    }
}


# store_result ( reference_no, status, query_text, query_url, data )
#
# Store the specified query results in the database. If the data is the same as the data stored
# from a previous response, add a record referencing that response.

sub store_result {

    my ($datasource, $reference_no, $status, $query_text, $query_url, $data) = @_;
    
    # The reference_no value must be a positive integer.
    
    unless ( $reference_no && $reference_no =~ /^\d+$/ )
    {
	croak "reference_no must be a positive integer";
    }

    my $dbh = $datasource->{dbh};
    my $source = $datasource->{source};
    
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
		ORDER BY eventtime desc LIMIT 1\n\n" if $datasource->{debug};
	
	my ($refsource_no) = $dbh->selectrow_array($sql);
	
	# If there is a previous matching row with the same result data, add a new row that
	# references it.
	
	if ( $refsource_no )
	{
	    $sql = "INSERT INTO $TABLE{REFERENCE_SOURCES}
		(reference_no, eventtype, source, previous_no, status, query_text, query_url)
		VALUES ($reference_no, 'fetch', $quoted_source, $refsource_no, $quoted_status, $quoted_text, $quoted_url)";
	    
	    print STDERR "$sql\n\n"if $datasource->{debug};
	    
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
		$quoted_text, $quoted_url, $items, $short_data)" if $datasource->{debug};
	    
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

	print STDERR "$sql\n\n" if $datasource->{debug};
	
	$result = $dbh->do($sql);
    }
    
    # If the insert operation succeeded, return the identifier of the inserted record. Otherwise,
    # return false.
    
    if ( $result )
    {
	return $dbh->last_insert_id();
    }
}


# select_events ( r, selector )
#
# Select one or more events from the REFERENCE_SOURCES table that come from this source.

sub select_events {

    my ($datasource, $r, $selector, $options) = @_;
    
    my $dbh = $datasource->{dbh};
    my $source = $datasource->{source};
    
    my ($refno, $sourceno);
    
    if ( ref $options && $options->{source} )
    {
	$source = $options->{source};
    }
    
    # First determine a record id.
    
    if ( $r =~ /^\d+$/ )
    {
	$refno = $r;
    }

    elsif ( ref $r && $r->{refsource_no} )
    {
	$sourceno = $dbh->quote($r->{refsource_no});
    }
    
    elsif ( ref $r && $r->{reference_no} )
    {
	$refno = $dbh->quote($r->{reference_no});
    }
    
    else
    {
	return;
    }
    
    # Then return the requested data.
    
    my $sql = "SELECT refsource_no, source, reference_no, eventtype, eventtime, previous_no,
		status, query_text, query_url";

    if ( $selector eq 'data' || $selector eq 'latest' || $selector eq 'all' )
    {
	$sql .= ", response_data";
    }

    else
    {
	$sql .= ", left(response_data,20) as response_data";
    }
    
    $sql .= "\n	FROM $TABLE{REFERENCE_SOURCES}\n";

    if ( $sourceno )
    {
	$sql .= "\n	WHERE refsource_no = $sourceno";
    }

    else
    {
	$sql .= "\n	WHERE reference_no = $refno";
	$sql .= " and source = " . $dbh->quote($source) if $source && $source ne 'all';
	$sql .= " and response_data <> ''" if $selector eq 'data' || $selector eq 'latest' ||
	    $selector eq 'all';
    }
    
    $sql .= "\n	ORDER BY eventtime desc LIMIT 1" if $selector eq 'data' || $selector eq 'latest';
    
    print STDERR "$sql\n\n" if $datasource->{debug};
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    return $result && ref $result eq 'ARRAY' ? @$result : ();
}


# store_scores ( event, scores, index )
#
#

sub store_scores {
    
    my ($datasource, $e, $s, $index) = @_;
    
    my $dbh = $datasource->{dbh};

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
    
    print STDERR "$sql\n\n" if $datasource->{debug};
    
    my $result;
    
    eval {
	$result = $dbh->do($sql);
    };
    
    if ( $@ )
    {
	print STDERR "$sql\n\n" unless $datasource->{debug};
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
    
    my ($datasource, $m, $scores) = @_;
    
    # Make sure we have a refsource_no.
    
    unless ( $m->{refsource_no} )
    {
	print STDERR "ERROR: no refsource_no found for score update\n";
	return;
    }
    
    # Run through all the possible scores, and accumulate a list of update clauses in each case
    # where $s has a defined value that is different from $m.
    
    my $dbh = $datasource->{dbh};
    
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
	
	print STDERR "$sql\n\n" if $datasource->{debug};
	
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
    
    my ($datasource, $m, $formatted) = @_;
    
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
	my $dbh = $datasource->{dbh};
	
	my $quoted_id = $dbh->quote($m->{refsource_no});
	
	my $quoted_text = $dbh->quote($formatted);
	
	my $sql = "UPDATE $TABLE{REFERENCE_SCORES} SET formatted=$quoted_text
		WHERE refsource_no=$quoted_id";
	
	print "$sql\n\n" if $datasource->{debug};
	
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
    
    my ($datasource, $selector) = @_;
    
    unless ( $selector =~ qr{ ^ \d+ $ | ^ all $ }xs )
    {
	print STDERR "ERROR: No reference number for recount.\n";
	return;
    }
    
    my $dbh = $datasource->{dbh};

    my $sql;

    $sql = "UPDATE $TABLE{REFERENCE_SCORES} SET
	sum_s = title_s + pub_s + auth1_s + auth2_s + pubyr_s + volume_s + pages_s + pblshr_s,
	sum_c = title_c + pub_c + auth1_c + auth2_c + pubyr_c + volume_c + pages_c + pblshr_c";
    
    $sql .= "\nWHERE reference_no = $selector" if $selector ne 'all';
    
    print STDERR "$sql\n\n" if $datasource->{debug};

    $dbh->do($sql);

    $sql = "UPDATE $TABLE{REFERENCE_SCORES} SET
	count_s = sign(title_s)+sign(pub_s)+sign(auth1_s)+sign(auth2_s)+
		sign(pubyr_s)+sign(volume_s)+sign(pages_s)+sign(pblshr_s),
	count_c = sign(title_c)+sign(pub_c)+sign(auth1_c)+sign(auth2_c)+
		sign(pubyr_c)+sign(volume_c)+sign(pages_c)+sign(pblshr_c)";
    
    $sql .= "\nWHERE reference_no = $selector" if $selector ne 'all';
    
    print STDERR "$sql\n\n" if $datasource->{debug};
    
    $dbh->do($sql);

    $sql = "UPDATE $TABLE{REFERENCE_SCORES} SET
	complete_s = sign(title_s div 100)+sign(pub_s div 100)+sign(auth1_s div 100)+
		sign(auth2_s div 100)+sign(pubyr_s div 100)+sign(volume_s div 100)+
		sign(pages_s div 100)+sign(pblshr_s div 100),
	complete_c = sign(title_c div 100)+sign(pub_c div 100)+sign(auth1_c div 100)+
		sign(auth2_c div 100)+sign(pubyr_c div 100)+sign(volume_c div 100)+
		sign(pages_c div 100)+sign(pblshr_c div 100)";
    
    $sql .= "\nWHERE reference_no = $selector" if $selector ne 'all';
    
    print STDERR "$sql\n\n" if $datasource->{debug};

    $dbh->do($sql);

    print STDERR "Updated all score counts.\n\n";
}


# set_manual ( score, is_match )
# 
# Sets the 'manual' field of a reference score record to either true (is a match), false (is not a
# match), or null (no decision made). This method is intended to be called in response to user input
# after viewing the match.

sub set_manual {
    
    my ($datasource, $score, $is_match) = @_;
    
    my $dbh = $datasource->{dbh};
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

	print STDERR "$sql\n\n" if $datasource->{debug};

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
    
    my ($datasource, $expr) = @_;
    
    unless ( $expr && $expr ne '()' )
    {
	croak "No sql expression specified\n";
    }
    
    unless ( $expr =~ qr{ ^ [(] .* [)] $ }xs )
    {
	$expr = "($expr)";
    }
    
    my $dbh = $datasource->{dbh};
    my $source_select = '';
    
    if ( $datasource->{source} && $datasource->{source} ne 'all' )
    {
	$source_select = " and sc.source = " . $dbh->quote($datasource->{source});
    }
    
    my ($sql, $matches, $positive, $negative);
    
    # Generate and execute the relevant SQL expression.
    
    $sql = "SELECT count(*) as matches, count(if(manual=1,1,null)) as positive,
		count(if(manual=0,1,null)) as negative FROM $TABLE{REFERENCE_SCORES} as sc
	WHERE $expr$source_select";
	
    print STDERR "$sql\n\n" if $datasource->{debug};
    
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

    my ($datasource, $expr, $mode, $count, $limit) = @_;
    
    unless ( $expr && $expr ne '()' )
    {
	croak "No sql expression specified\n";
    }

    $limit ||= 20;
    
    my $dbh = $datasource->{dbh};
    my $source_select = '';
    
    if ( $datasource->{source} && $datasource->{source} ne 'all' )
    {
	$source_select = " and sc.source = " . $dbh->quote($datasource->{source});
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
    
    print STDERR "$sql\n\n" if $datasource->{debug};
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    if ( $result && ref $result eq 'ARRAY' )
    {
	return @$result;
    }
    
    return;
}


# select_matching_scores ( expr, include_rd )
# 
# Select all the reference score records matching the specified SQL expression, and store the
# statement handle where it can be read by 'get_next'. If $include_rd is true, include the
# response_data field in each record.
# 
# The given expression should be enclosed in parentheses unless it is okay as-is to conjoin with
# one or more simple clauses.

sub select_matching_scores {

    my ($datasource, $expr, $include_rd) = @_;
    
    $datasource->{sth} = undef;
    
    unless ( $expr && $expr ne '()' )
    {
	croak "No sql expression specified\n";
    }
    
    my $dbh = $datasource->{dbh};
    my $source_select = '';
    
    if ( $datasource->{source} && $datasource->{source} ne 'all' )
    {
	$source_select = " and sc.source = " . $dbh->quote($datasource->{source});
    }
    
    # Generate and execute the relevant SQL expression.
    
    my $rd = $include_rd ? ', s.response_data' : '';
    
    my $sql = "SELECT sc.*, r.*, s.items $rd
	FROM $TABLE{REFERENCE_SCORES} as sc
	    join $TABLE{REFERENCE_DATA} as r using (reference_no)
	    join $TABLE{REFERENCE_SOURCES} as s using (refsource_no)
	WHERE $expr$source_select";
    
    print STDERR "$sql\n\n" if $datasource->{debug};

    my $sth = $dbh->prepare($sql);
    
    $sth->execute();
    
    $datasource->{sth} = $sth;
    
    return;
}


# compare_scores ( expr1, expr2, limit )
#
# Count two categories of REFERENCE_SCORES records: those which match expr1 but not expr2, and those which
# match expr2 but not expr1. Return the two counts as a list.

sub compare_scores {
    
    my ($datasource, $expr_a, $expr_b) = @_;
    
    unless ( $expr_a )
    {
	croak "No sql expression specified\n";
    }
    
    my $dbh = $datasource->{dbh};
    my $source_select = '';
    
    if ( $datasource->{source} && $datasource->{source} ne 'all' )
    {
	$source_select = " and sc.source = " . $dbh->quote($datasource->{source});
    }

    my ($sql_a, $sql_b);
    my $count_a = 0;
    my $count_b = 0;

    # Generate and execute the relevant SQL expressions for a and b. If b is empty, only do a.
    
    if ( $expr_a && $expr_b )
    {
	$sql_a = "SELECT count(*) FROM $TABLE{REFERENCE_SCORES} as sc
	WHERE ($expr_a) and not ($expr_b)$source_select";
	
	print STDERR "$sql_a\n\n" if $datasource->{debug};
	
	($count_a) = $dbh->selectrow_array($sql_a);
	
	$sql_b = "SELECT count(*) FROM $TABLE{REFERENCE_SCORES} as sc
	WHERE ($expr_b) and not ($expr_a)$source_select";

	print STDERR "$sql_b\n\n" if $datasource->{debug};

	($count_b) = $dbh->selectrow_array($sql_b);
    }
    
    elsif ( $expr_a )
    {
	$sql_a = "SELECT count(*) FROM $TABLE{REFERENCE_SCORES} as sc
	WHERE ($expr_a)$source_select";
	
	print STDERR "$sql_a\n\n" if $datasource->{debug};
	
	($count_a) = $dbh->selectrow_array($sql_a);
    }
    
    # Then return the results.
    
    return ($count_a, $count_b);
}


sub compare_list_scores {

    my ($datasource, $expr_a, $expr_b, $limit) = @_;
    
    unless ( $limit && $limit =~ /^\d+$/ )
    {
	croak "Invalid limit '$limit'\n";
    }
    
    unless ( $expr_a )
    {
	croak "No SQL expression specified\n";
    }
    
    my $dbh = $datasource->{dbh};
    my $source_select = '';
    
    if ( $datasource->{source} && $datasource->{source} ne 'all' )
    {
	$source_select = " and sc.source = " . $dbh->quote($datasource->{source});
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
	
	print STDERR "$sql\n\n" if $datasource->{debug};
	
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
	
	print STDERR "$sql\n\n" if $datasource->{debug};
	
	$result = $dbh->selectall_arrayref($sql, { Slice => { } });
    }
    
    # If there are no results, return the empty list.

    return unless $result && @$result;
    
    # Go through the result list and generate formatted output for each row if we don't already
    # have it.
    
  ROW:
    foreach my $r ( @$result )
    {
	$datasource->format_scored_match($r);
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
    
    my ($datasource, $m) = @_;
    
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
    
    $m->{ref_formatted} = $datasource->format_ref($m);
    
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
	
	my @items = $datasource->extract_response_items($m);
	
	# Now that we know the item count, we can store that number in the REFERENCE_SOURCES table if
	# it isn't already there.
	
	my $dbh = $datasource->{dbh};
	
	if ( ! defined $m->{items} || $m->{items} != @items )
	{
	    $m->{items} = scalar(@items);
	    
	    my $quoted_id = $dbh->quote($m->{refsource_no});
	    my $quoted_count = $dbh->quote($m->{items});
	    
	    my $sql_u = "UPDATE $TABLE{REFERENCE_SOURCES} SET items=$quoted_count
		WHERE refsource_no=$quoted_id LIMIT 1";
	    
	    print STDERR "$sql_u\n\n" if $datasource->{debug};

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
	    $m->{match_formatted} = $datasource->format_ref($items[$index]);
	    
	    # Store the generated text in the 'formatted' column of the score record.
	    
	    my $quoted_text = $dbh->quote($m->{match_formatted});
	    my $quoted_id = $dbh->quote($m->{refsource_no});
	    
	    my $sql_s = "UPDATE $TABLE{REFERENCE_SCORES} SET formatted=$quoted_text
		WHERE refsource_no=$quoted_id and rs_index=$index LIMIT 1";
	    
	    print STDERR $sql_s . "\n\n" if $datasource->{debug};
	    
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
    
    my ($datasource, $m) = @_;
    
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
    
    my @items = $datasource->extract_response_items($m);
    
    # Now that we know the item count, we can store that number in the REFERENCE_SOURCES table if
    # it isn't already there.
    
    if ( ! defined $m->{items} || $m->{items} != @items )
    {
	$m->{items} = scalar(@items);
	
	my $dbh = $datasource->{dbh};
	my $quoted_id = $dbh->quote($m->{refsource_no});
	my $quoted_count = $dbh->quote($m->{items});
	
	my $sql_u = "UPDATE $TABLE{REFERENCE_SOURCES} SET items=$quoted_count
		WHERE refsource_no=$quoted_id LIMIT 1";
	
	print STDERR "$sql_u\n\n" if $datasource->{debug};
	
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

    my ($datasource, $m) = @_;
    
    my $response_data = $m->{response_data};
    
    # If we weren't given any value for $response_data, fill it in by querying the database.
    
    unless ( $response_data )
    {
	my $dbh = $datasource->{dbh};
	my $quoted_id = $dbh->quote($m->{refsource_no});
	
	my $sql = "SELECT response_data FROM $TABLE{REFERENCE_SOURCES} WHERE refsource_no=$quoted_id";
	
	print STDERR "$sql\n\n" if $datasource->{debug};
	
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

    
# format_ref ( attrs )
#
# Return a string of text, formatted in Chicago B style, representing the bibliographic reference
# data contained in the given record. This routine handles both the pbdb reference attributes and
# the crossref reference attributes.

sub format_ref {

    my ($datasource, $r) = @_;
    
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

    my ($datasource, $scores, $color_output) = @_;
    
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



#     # Now start building the reference with authorstring, publication year,
#     # reference title and publication title
    
#     my $longref = $authorstring;
#     my $pubtype = $r->{pubtype} || $r->{publication_type};
    
#     if ( $authorstring ne '' )
#     {
# 	$longref .= '.' unless $authorstring =~ /\.$/;
# 	$longref .= ' ';
#     }
    
    
#     if ( $pubyr ne '' )
#     {
# 	$longref .= "$pubyr. ";
#     }
    
#     my $reftitle;

#     if ( $r->{title} && ref $r->{title} eq 'ARRAY' && @{$r-{title}} )
#     {
# 	$reftitle = $r->{title}[0];
#     }

#     elsif ( $r->{title} && ! ref $r->{title} )
#     {
# 	$reftitle = $r->{title};
#     }

#     elsif ( $r->{reftitle} )
#     {
# 	$reftitle = $r->{reftitle};
#     }

#     else
#     {
# 	$reftitle = 'unknown';
#     }
    
#     if ( $reftitle ne '' )
#     {
# 	$longref .= $reftitle;

# 	if ( $pubtype && $pubtype eq 'instcoll' )
# 	{
# 	    $longref .= ', ';
# 	}

# 	else
# 	{
# 	    $longref .= '.' unless $reftitle =~ /\.$/;
# 	    $longref .= ' ';
# 	}
#     }
    
#     my $pubtitle = $r->{pubtitle} || '';
#     my $editors = $r->{editors} || '';
    
#     if ( $pubtitle ne '' )
#     {
# 	my $pubstring = $pubtitle;

# 	unless ( $pubtype && $pubtype eq 'instcoll' )
# 	{
# 	    if ( $editors =~ /,| and / )
# 	    {
# 		$pubstring = " In $editors (eds.), $pubstring";
# 	    }
# 	    elsif ( $editors )
# 	    {
# 		$pubstring = " In $editors (ed.), $pubstring";
# 	    }
# 	}
	
# 	$longref .= $pubstring . " ";
#     }
    
#     my $publisher = $r->{publisher};
#     my $pubcity = $r->{pubcity};

#     if ( $pubtype && $pubtype eq 'instcoll' )
#     {
# 	if ( $pubcity )
# 	{
# 	    $pubcity =~ s/^(, )+//;
# 	    $pubcity =~ s/, , /, /;
# 	    $longref =~ s/\s+$//;
# 	    $longref .= ". " unless $longref =~ /[.]$/;
# 	    $longref .= "$pubcity. ";
# 	}
#     }
    
#     elsif ( $publisher )
#     {
# 	$longref =~ s/\s+$//;
# 	$longref .= ". ";
# 	$longref .= "$pubcity: " if $pubcity;
# 	$longref .= $publisher . ". ";
#     }
    
#     # Now add volume and page number information if available
    
#     my $pubvol = $r->{pubvol} || '';
#     my $pubno = $r->{pubno} || '';
    
#     if ( $pubvol ne '' || $pubno ne '' )
#     {
# 	$longref .= $pubvol if $pubvol ne '';
# 	$longref .= "." if $pubvol eq 'PRIVATE';
# 	$longref .= "($pubno)" if $pubno ne '';
#     }
    
#     my $fp = $r->{firstpage} || '';
#     my $lp = $r->{lastpage} || '';
    
#     if ( ($pubvol ne '' || $pubno ne '') && ($fp ne '' || $lp ne '') )
#     {
# 	$longref .= ':';
# 	$longref .= $fp if $fp ne '';
# 	$longref .= '-' if $fp ne '' && $lp ne '';
# 	$longref .= $lp if $lp ne '';
#     }
    
#     $longref =~ s/\s+$//;
    
#     return $longref || 'ERROR';
# }



# # get_eligible ( type )
# # 
# # Return the reference_no of a Paleobiology Database reference record that has not been the subject
# # of a query of the specified type from this source. If there are none, return undefined. Type
# # defaults to 'fetch' if not specified. If a second parameter is given, it must be an array of
# # reference_no values. References with these values are skipped.

# sub get_eligible {
    
#     my ($datasource, $type) = @_;
    
#     my $dbh = $datasource->{dbh};
#     my $quoted_source = $dbh->quote($datasource->{source});
#     my $quoted_type = $dbh->quote($type || 'fetch');
    
#     # If we don't already have one, create a temporary table that lets us return each reference
#     # only once per session.
    
#     unless ( $datasource->{session_table} )
#     {
# 	$datasource->create_session_table;
#     }
    
#     my $sql = "SELECT r.* FROM $TABLE{REFERENCE_DATA} as r left join refcheck using (reference_no)
# 		left join $TABLE{REFERENCE_SOURCES} as s on r.reference_no = s.reference_no 
# 			and s.source = $quoted_source and s.eventtype = $quoted_type
# 		WHERE refcheck.reference_no is null and s.reference_no is null LIMIT 1";
    
#     print STDERR "$sql\n\n" if $datasource->{debug};

#     my ($r) = $dbh->selectrow_hashref($sql);

#     if ( $r && $r->{reference_no} )
#     {
# 	$dbh->do("INSERT IGNORE INTO refcheck (reference_no) VALUES ($r->{reference_no})");
#     }
    
#     return $r;
# }


# # create_session_table ( )

# sub create_session_table {
    
#     my ($datasource) = @_;
    
#     my $dbh = $datasource->{dbh};

#     my $result = $dbh->do("CREATE TEMPORARY TABLE refcheck (
# 		reference_no int unsigned not null primary key ) ENGINE=MEMORY");
    
#     die "ERROR: could not create temporary table\n" unless $result;
#     $datasource->{session_table} = 'refcheck';
# }


