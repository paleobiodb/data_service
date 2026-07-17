# 
# The Paleobiology Database
# 
#   MacrostratMatch.pm
# 
# This module is responsible for generating and updating matches between collections in
# The Paleobiology Database and units/columns in Macrostrat. It does this by querying
# the matching service specified by the entry 'macrostrat_unit_match_uri' in the file
# 'config.yml'.
# 
# Author: Michael McClennen
# 

package MacrostratMatch;

use strict;

use base 'Exporter';

no warnings 'experimental';

our (@EXPORT_OK) = qw(updateMacrostratMatch ensureTables);

use Carp qw(carp croak);
use JSON;
use Encode;
use LWP::UserAgent;

use TableDefs qw(%TABLE);
use CoreTableDefs;
use ConsoleLog qw(logMessage);
use CoreFunction qw(loadConfig configData);


our ($DEFAULT_RETRY_LIMIT) = 3;
our ($DEFAULT_RETRY_INTERVAL) = 5;
our ($DEFAULT_FAIL_LIMIT) = 3;
our ($DEFAULT_BAD_RESPONSE_LIMIT) = 5;
our ($DEFAULT_MAX_ENTRIES) = 500;


# CLASS CONSTRUCTOR
# -----------------

sub new {
    
    my ($class, $dbh, $options) = @_;
    
    $options ||= { };
    
    my $self = { dbh => $dbh,
		 debug => $options->{debug} };
    
    return bless($self, $class);
}


# cancelExisting ( selector, options )
# 
# Clear the update_existing flag from all or selected entries. The value of $selector
# can be either 'new' or 'existing'.

sub cancelUpdate {
    
    my ($self, $selector, $options) = @_;
    
    my ($filter, $desc, @rest) = $self->generateFilter($options);
    
    my $flag_column;
    
    if ( $selector eq 'new' )
    {
	$flag_column = 'update_new';
    }
    
    elsif ( $selector eq 'existing' )
    {
	$flag_column = 'update_existing';
    }
    
    else
    {
	croak "Invalid value '$selector' for first argument";
    }
    
    logMessage(1, "Canceling update of existing entries $desc");
    logMessage(1, $_) foreach @rest;
    
    my $sql = "UPDATE $TABLE{COLLECTION_UNITS_STATIC} as cs
		    join $TABLE{COLLECTION_DATA} as cc using (collection_no)
		    join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
		SET $flag_column = false
		WHERE $filter";
    
    my $count = $self->doSQL($sql);
    
    logMessage(2, "  canceled the update for $count entries");
    
    return;
}


# updateNew ( options )
# 
# Generate column/unit matches for new collections, and update those whose location has
# been modified. The specified options may restrict the selection. The update operation
# uses the settings specified in the configuration file.

sub updateNew {
    
    my ($self, $options) = @_;
    
    # Check if there is already a process doing this step. If so, print a message and
    # exit.

    my $dbh = $self->{dbh};

    my ($lock) = $dbh->selectrow_array("SELECT GET_LOCK('msmatch new', 1)");
    
    unless ( $lock )
    {
	logMessage(1, "Another process is already updating new Macrostrat matches");
	exit;
    }
        
    # Start by loading the relevant configuration settings from the
    # paleobiology database configuration file.
    
    $self->getConfig();
    
    # Generate a filter expression according to the specified options. If no
    # filtering options were given, the filter expression will be "1". The
    # remaining returned values provide a text description of which records will
    # be updated.
    
    my ($filter, $desc, @rest) = $self->generateFilter($options);
    
    if ( $options->{resume} )
    {
	logMessage(1, "Resuming interrupted execution");
    }
    
    else
    {
	logMessage(1, "Updating new columns/units $desc");
	logMessage(1, $_) foreach @rest;
	
	# Create entries in the static table for any collections that aren't already
	# there. But ignore entries associated with the 'eODP' research group, because
	# they already have known unit matches.
	
	my $sql = "INSERT IGNORE INTO $TABLE{COLLECTION_UNITS_STATIC}
		(collection_no, known_match, update_new)
	    SELECT c.collection_no, find_in_set('eODP', cc.research_group),
		not(find_in_set('eODP', cc.research_group))
	    FROM $TABLE{COLLECTION_MATRIX} as c
		join $TABLE{COLLECTION_DATA} as cc using (collection_no)
		left join $TABLE{COLLECTION_UNITS_STATIC} as cs using (collection_no)
	    WHERE cs.collection_no is null and $filter";
	
	print STDERR "> $sql\n\n" if $self->{debug};
	
	my $count = $self->doSQL($sql);
	
	logMessage(2, "    added entries for $count new collections");
	
	# Update entries where the collection has been modified more recently than that
	# match has been updated.
	
	$sql = "UPDATE $TABLE{COLLECTION_UNITS_STATIC} as cs
		join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
		join $TABLE{COLLECTION_DATA} as cc using (collection_no)
	    SET cs.update_new = true
	    WHERE cc.modified > cs.updated and not(cs.known_match) and $filter";
	
	$count = $self->doSQL($sql);
	
	logMessage(2, "    flagged entries for $count collections which were modified " .
		   "since being updated");
    }
    
    $self->updateFlagged('new', $filter, $options);
}


# updateExisting ( options )
# 
# Update existing column/unit matches selected by the specified options, using the settings
# specified in the configuration file.

sub updateExisting {
    
    my ($self, $options) = @_;
    
    # Check if there is already a process doing this step. If so, print a message and
    # exit.

    my $dbh = $self->{dbh};

    my ($lock) = $dbh->selectrow_array("SELECT GET_LOCK('msmatch existing', 1)");
    
    unless ( $lock )
    {
	logMessage(1, "Another process is already updating existing Macrostrat matches");
	exit;
    }
        
    # Start by loading the relevant configuration settings from the
    # paleobiology database configuration file.
    
    $self->getConfig();
    
    # Generate a filter expression according to the specified options. If no
    # filtering options were given, the filter expression will be "1". The
    # remaining returned values provide a text description of which records will
    # be updated.
    
    my ($filter, $desc, @rest) = $self->generateFilter($options);
    
    if ( $options->{resume} )
    {
	logMessage(1, "Resuming interrupted execution");
    }

    else
    {
	logMessage(1, "Updating existing columns/units $desc");
	logMessage(1, $_) foreach @rest;
	
	my $sql = "UPDATE $TABLE{COLLECTION_UNITS_STATIC} as cs
		    join $TABLE{COLLECTION_DATA} as cc using (collection_no)
		    join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
		SET cs.update_existing = true
		WHERE $filter";
	
	my $count = $self->doSQL($sql);
	
	logMessage(2, "    flagged $count existing records to update");
    }
    
    # Now update all of the records that have been flagged, including
    # any flags that were already set when this subroutine was called.
    
    $self->updateFlagged('existing', $filter, $options);
}


# updateFlagged ( selector, filter, options )
# 
# Update all column/unit match records that have been flagged. The first parameter
# specifies whether to use the 'update_new' or 'update_existing' flag. The two flags
# allow for a slow background job to update existing records at the same time as a
# periodic job to update the records corresponding to newly added or modified
# collections. Each record's flag is cleared when it is successfully updated, so if
# one call to this subroutine is interrupted then the next one will complete all
# outstanding updates.

sub updateFlagged {
    
    my ($self, $selector, $filter, $options) = @_;
    
    my $dbh = $self->{dbh};
    
    my $service_uri = $self->{service_uri};
    
    my $opt_verbose = $options->{verbose};
    
    my $flag_column = $selector eq 'new' ? 'update_new' : 'update_existing';
    
    # Prepare the SQL statements that will be used to update entries in the
    # table, and generate a user agent object with which to make requests.
    
    # $self->prepareStatements($options);
    
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database Updater/0.2");
    
    # Count the number of records to be updated.
    
    my $sql = "SELECT count(*) FROM $TABLE{COLLECTION_UNITS_STATIC} as cs
		    join $TABLE{COLLECTION_DATA} as cc using (collection_no)
		    join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
		WHERE cs.$flag_column and not(cs.known_match) and $filter";
    
    print STDERR "> $sql\n\n" if $self->{debug};
    
    my ($update_total) = $dbh->selectrow_array($sql);
    
    logMessage(2, "    updating $update_total column/unit entries...");
    
    my $max_expr = 'if(cc.direct_ma + cc.direct_ma_error < cc.max_ma + cc.max_ma_error, cc.direct_ma + cc.direct_ma_error, coalesce(cc.max_ma + cc.max_ma_error, cc.direct_ma + cc.direct_ma_error))';
    my $min_expr = 'if(cc.direct_ma - cc.direct_ma_error > cc.min_ma - cc.min_ma_error, cc.direct_ma - cc.direct_ma_error, coalesce(cc.min_ma - cc.min_ma_error, cc.direct_ma - cc.direct_ma_error))';
    
    # Fetch the basic information about the records that need updating, in chunks.
    
    my $CHUNK_SIZE = 10000;
    my $REQUEST_SIZE = 50;
    
    my @request_records;
    my $colls_found = 0;
    my $last_found = 0;
    my $colls_matched = 0;
    
    $DB::single = 1;
    
  CHUNK:
    while ($update_total)
    {
	# Fetch up to 10,000 collections that need to be updated.
	
	$sql = "SELECT cs.collection_no, c.lat, c.lng, c.bin_id_2 as bin_id,
		    $max_expr as max_ma,
		    $min_expr as min_ma,
		    ccs.grp, ccs.formation, ccs.member,
		    mmax.interval_name as max_interval, mmin.interval_name as min_interval,
		    imax.early_age as b_age, coalesce(imin.late_age, imax.late_age) as t_age,
		    cc.latlng_basis = 'based on political unit' as bad_coordinates
		FROM $TABLE{COLLECTION_UNITS_STATIC} as cs
		    join $TABLE{COLLECTION_DATA} as cc using (collection_no)
		    join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
		    join $TABLE{COLLECTION_STRATA} as ccs using (collection_no)
		    left join $TABLE{INTERVAL_DATA} as imax on imax.interval_no = cc.max_interval_no
		    left join macrostrat.intervals as mmax on mmax.interval_name = imax.interval_name
		    left join $TABLE{INTERVAL_DATA} as imin on imin.interval_no = cc.min_interval_no
		    left join macrostrat.intervals as mmin on mmin.interval_name = imin.interval_name
		WHERE cs.$flag_column and not(cs.known_match) and $filter
		ORDER By c.bin_id_2
		LIMIT $CHUNK_SIZE";
	
	print STDERR "> $sql\n\n" if $self->{debug};
	
	my $updates = $dbh->selectall_arrayref($sql, { Slice => {} });
	
	# Stop if we have nothing to update.
	
	last CHUNK unless ref $updates eq 'ARRAY' && $updates->@*;
	
	my %points;
	my %matched;
	
	# Group the results by bin_id, because records in the same bin are almost
	# certainly in the same Macrostrat column. Further group them by space/time
	# coordinates, since there will often be multiple collections with the same
	# lat/lng/intervals. For each distinct coordinate key, collect a list of
	# collection_no values.
	
	if ( ref $updates eq 'ARRAY' && @$updates )
	{    
	    foreach my $record ( @$updates )
	    {
		my $max_interval = $record->{max_interval} || '';
		my $min_interval = $record->{min_interval} || '';
		my $max_age = $record->{max_age} || '';
		my $min_age = $record->{min_age} || '';
		my $t_age = $record->{t_age} || '';
		my $b_age = $record->{b_age} || '';
		my @strat_names;
		push @strat_names, $record->{member} if $record->{member};
		push @strat_names, $record->{formation} if $record->{formation};
		push @strat_names, $record->{grp} if $record->{grp};
		my $strat_name = join ';', @strat_names;
		
		my $point_key = "$record->{lat}|$record->{lng}|$max_interval|$min_interval|" .
		    "$b_age|$t_age|$max_age|$min_age|$strat_name|$record->{bad_coordinates}";
		
		my $bin_key = $record->{bin_id};
		
		$points{$bin_key}{$point_key} ||= [ ];
		
		push $points{$bin_key}{$point_key}->@*, $record->{collection_no};
	    }
	}
	
	else
	{
	    last CHUNK;
	}
	
	# Now iterate through all the list of bin_id values found in the set of points
	# we are matching. For each bin, iterate through the coordinate keys and
	# construct a list of records to be passed to the matching service. The reason
	# for this double loop is to make sure that all the matches being carried out in
	# a given request fall into the same bin and thus are almost certain to fall
	# into the same Macrostrat column.
	
      BIN:
	foreach my $bin_id ( sort keys %points )
	{
	  POINT:
	    foreach my $point_key ( keys $points{$bin_id}->%* )
	    {
		my ($lat, $lng, $max_interval, $min_interval, $b_age, $t_age, $max_age, $min_age,
		    $strat_name, $bad_coordinates) = split /[|]/, $point_key;
		
		$colls_found += scalar($points{$bin_id}{$point_key}->@*);
		
		next POINT unless $strat_name;
		
		my $record = { lat => $lat, lng => $lng, identifier => $point_key,
			       strat_name => $strat_name, all => 1 };
		
		if ( $min_interval && $max_interval )
		{
		    $record->{b_interval} = $max_interval;
		    $record->{t_interval} = $min_interval;
		}
		
		elsif ( $max_interval )
		{
		    $record->{interval} = $max_interval;
		}
		
		else
		{
		    $record->{b_age} = $b_age;
		    $record->{t_age} = $t_age;
		}
		
		if ( $max_age )
		{
		    $record->{b_age} = $max_age;
		}
		
		if ( $min_age )
		{
		    $record->{t_age} = $min_age;
		}
		
		if ( $bad_coordinates )
		{
		    $record->{priority} = 'strat_name';
		}
		
		push @request_records, $record;
		
		# Accumulate records until we reach $REQUEST_SIZE. It should be more
		# efficient to match multiple records at once.
		
		if ( @request_records >= $REQUEST_SIZE )
		{
		    my $response = $self->makeMatchRequest($ua, \@request_records);
		    @request_records = ();
		    
		    if ( $response )
		    {
			$self->processResponse($response, $points{$bin_id}, \%matched);
		    }
		    
		    if ( $self->{fail_count} >= $self->{fail_limit} )
		    {
			logMessage(1, "ABORTING due to service error count: $self->{fail_count}");
			print STDERR "Aborting due to service error count: $self->{fail_count}\n";
			last CHUNK;
		    }
		    
		    if ( $self->{bad_count} >= $self->{bad_limit} )
		    {
			logMessage(1, "ABORTING due to database error count: $self->{bad_count}");
			print STDERR "Aborting due to database error count: $self->{bad_count}\n";
			last CHUNK;
		    }
		}
	    }
	    
	    # If there are any records still outstanding, make one more request to take
	    # care of them.
	    
	    if ( @request_records )
	    {
		my $response = $self->makeMatchRequest($ua, \@request_records);
		@request_records = ();
		
		if ( $response )
		{
		    $self->processResponse($response, $points{$bin_id}, \%matched);
		}
		
		if ( $self->{fail_count} >= $self->{fail_limit} )
		{
		    logMessage(1, "ABORTING due to service error count: $self->{fail_count}");
		    print STDERR "Aborting due to service error count: $self->{fail_count}\n";
		    last CHUNK;
		}
		
		if ( $self->{bad_count} >= $self->{bad_limit} )
		{
		    logMessage(1, "ABORTING due to database error count: $self->{bad_count}");
		    print STDERR "Aborting due to database error count: $self->{bad_count}\n";
		    last CHUNK;
		}
	    }
	    		
	    # Now mark all of the collections that didn't get matched as 'invalid' but
	    # updated.
	    
	    my @unmatched;
	    
	    foreach my $point_key ( $points{$bin_id}->%* )
	    {
		if ( $points{$bin_id}{$point_key} )
		{
		    if ( $matched{$point_key} )
		    {
			$colls_matched += $points{$bin_id}{$point_key}->@*;
		    }
		    
		    else
		    {
			push @unmatched, $points{$bin_id}{$point_key}->@*;
		    }
		}
	    }
	    
	    if ( @unmatched )
	    {
		my $unmatched_list = join "','", @unmatched;
		
		my $sql = "UPDATE $TABLE{COLLECTION_UNITS_STATIC}
		SET invalid = true, update_new = false, update_existing = false,
		    updated = now()
		WHERE collection_no in ('$unmatched_list')";
		
		print STDERR "$sql\n\n" if $self->{debug};
		
		my $result = $dbh->do($sql);
	    }
	    
	    my $a = 1;	# we can stop here while debugging
	}
	
	logMessage(2, "    processed $colls_found collections (matched $colls_matched) " .
		   "out of $update_total");
	
	$last_found = $colls_found;
	
	last CHUNK if $colls_found == $update_total;
    }
    
    if ( $colls_found > $last_found )
    {
	logMessage(2, "    processed $colls_found collections (matched $colls_matched) " .
		   "out of $update_total");
    }
    
    my $time = localtime;
    
    logMessage(2, "    finished at $time");
}


# makeMatchRequest ( user_agent, record_list )
#
# Make a match API request whose body is the specified list of records. 

sub makeMatchRequest {

    my ($self, $ua, $record_list) = @_;
    
    my $uri = $self->{service_uri};
    
    my $body = encode_json($record_list);
    my $pretty_body = JSON->new->pretty->utf8->encode($record_list);
    
    # Generate a match request.  The actual request is wrapped inside a
    # while loop so that we can retry it if something goes wrong.
    
    print STDERR "POST $uri\n" if $self->{debug};
    print STDERR "$pretty_body\n" if $self->{debug};
    
    my $request = HTTP::Request->new(POST => $uri, undef, $body);
    
    my ($response, $content_ref, $data);
    my $retry_count = $self->{retry_limit};
    my $retry_interval = $self->{retry_interval};
    
 RETRY:
    while ( $retry_count )
    {
	# $DB::single = 1;
	
	my $start = time;
	my $time1;
	
	$response = $ua->request($request);
	$content_ref = $response->content_ref;
	
	# If the request succeeds, decode the content. If that succeeds, reset fail count
	# and return.
	
	if ( $response->is_success )
	{
	    eval {
		$data = decode_json($$content_ref);
		
		print STDERR encode_utf8($$content_ref) . "\n\n"
		    if $self->{debug} && $content_ref && $$content_ref;
	    };
	    
	    if ( ref $data eq 'HASH' )
	    {
		$self->{fail_count} = 0;
		return $data;
	    }
	    
	    elsif ( $data )
	    {
		logMessage(2, "      Response JSON must be a hash: $request");
		print STDERR "ERROR: response JSON must be a hash: $request\n";
		return;
	    }
	    
	    else
	    {
		logMessage(2, "      Bad JSON from service: $request");
		print STDERR "ERROR: bad JSON from service: $request\n";
		return;
	    }
	}
	
	# Otherwise, check the initial part of the response message body.  If
	# the server didn't give us any response, wait a few seconds and try
	# again.
	
	my $content_start = substr($$content_ref, 0, 1000);
	
	if ( $content_start =~ /server closed connection/i )
	{
	    $retry_count--;
	    
	    if ( $retry_count > 0 )
	    {
		logMessage(2, "      Server closed connection, retrying...");
		print STDERR "SERVER CLOSED CONNECTION, RETRYING...\n";
	    }
	    
	    sleep($retry_interval);
	    $retry_interval *= 2;
	    next RETRY;
	}
	
	# Otherwise, the request failed for some other reason and should not
	# be retried.  If the option $debug is true, write the response
	# content to an error file.
	
	my $code = $response->code;
	logMessage(2, "      Request failed with code '$code': $request");
	print STDERR "REQUEST FAILED WITH CODE '$code': $request\n";
	$self->{fail_count}++;
	
	if ( $self->{debug} )
	{
	    $self->{debug_count}++;
	    open(OUTFILE, ">gpfail.$self->{debug_count}.html");
	    print OUTFILE $response->content;
	    close OUTFILE;
	    print STDERR "DEBUG FILE 'gpfail.$self->{debug_count}.html' created\n";
	}
	
	return;
    }
	
    # If we get here, then we have exceeded the retry count.
    
    logMessage(2, "      ABORTING REQUEST");
    print STDERR "ABORTING REQUEST\n";
    $self->{fail_count}++;
    return;
}


# processResponse ( response_data, point_hash )
#
# Process a response received back from the match API service.

sub processResponse {

    my ($self, $response, $point_hash, $matched_hash) = @_;
    
    if ( ref $response->{results} eq 'ARRAY' && $response->{results}->@* )
    {
	my $dbh = $self->{dbh};
	my ($sql, $result);
	
      RECORD:
	foreach my $record ( $response->{results}->@* )
	{
	    my $point_key = $record->{id};
	    
	    unless ( $point_key )
	    {
		logMessage(2, "    ERROR: identifier was not returned");
		next RECORD;
	    }
	    
	    unless ( $point_hash->{$point_key} )
	    {
		logMessage(2, "    ERROR: could not match key '$point_key'");
		next RECORD;
	    }
	    
	    my @collections = $point_hash->{$point_key}->@*;
	    my $coll_list = join "','", @collections;
	    my @matches = $record->{unit_matches}->@*;
	    my @messages = $record->{messages}->@*;
	    
	    foreach my $m ( @messages )
	    {
		next if $m->{message} eq 'Multiple columns';
		
		my $msg = $m->{details} || $m->{message};
		
		if ( $m->{type} eq 'warning' )
		{
		    logMessage(2, "    WARNING: $msg for collection(s) '$coll_list'");
		}
		
		else
		{
		    logMessage(2, "    ERROR: $msg for collection(s) '$coll_list'");
		}
	    }
	    
	    $sql = "DELETE FROM $TABLE{COLLECTION_UNITS}
		    WHERE collection_no in ('$coll_list')";
	    
	    print STDERR "$sql\n\n" if $self->{debug};
	    
	    $result = $dbh->do($sql);
	    
	    if ( @matches > 1 )
	    {
		@matches = $self->filterMatches($point_key, @matches);
	    }
	    
	    my $insertions = '';
	    
	    foreach my $collection_no ( @collections )
	    {
		$self->{update_count}++;
		
		foreach my $match ( @matches )
		{
		    my $unit_id = $dbh->quote($match->{unit_id} || '0');
		    my $col_id = $dbh->quote($match->{col_id} || '0');
		    my $concept_id = $dbh->quote($match->{concept_id} || '0');
		    my $concept_name = $dbh->quote($match->{concept_name});
		    my $strat_name_id = $dbh->quote($match->{strat_name_id});
		    my $strat_name = $dbh->quote($match->{strat_name});
		    my $strat_rank = $dbh->quote($match->{strat_rank});
		    my $strat_parent_id = $dbh->quote($match->{parent_id});
		    my $t_age = $dbh->quote($match->{t_age});
		    my $b_age = $dbh->quote($match->{b_age});
		    
		    $insertions .= ',' if $insertions;
		    $insertions .= "($collection_no, $unit_id, $col_id, $concept_id, $concept_name, $strat_name_id, $strat_name, $strat_rank, $strat_parent_id, $t_age, $b_age)\n";
		}
	    }
	    
	    if ( $insertions )
	    {
		$sql = "INSERT INTO $TABLE{COLLECTION_UNITS} (collection_no, unit_id, col_id, concept_id, concept_name, strat_name_id, strat_name, strat_rank, strat_parent_id, t_age, b_age) VALUES\n$insertions";
		
		print STDERR "$sql\n\n" if $self->{debug};
		
		$result = $dbh->do($sql);
	    }

	    my $matched_list = join "','", @collections;

	    $sql = "UPDATE $TABLE{COLLECTION_UNITS_STATIC}
		SET invalid = false, update_new = false, update_existing = false,
		    updated = now()
		WHERE collection_no in ('$matched_list')";
	    
	    print STDERR "$sql\n\n" if $self->{debug};
	    
	    $result = $dbh->do($sql);
	    
	    $matched_hash->{$point_key} = 1;
	}
    }
    
    else
    {
	logMessage(2, "    ERROR: no results from API call");
    }
}


sub filterMatches {

    my ($self, $point_key, @matches) = @_;
    
    # For now, we automatically accept the first match. Reject all subsequent matches
    # with the same combination of unit_id, col_id, concept_id.

    my @filtered;
    my %unique_key;

    foreach my $r ( @matches )
    {
	my $unit_id = $r->{unit_id} || '0';
	my $col_id = $r->{col_id} || '0';
	my $concept_id = $r->{concept_id} || '0';
	my $key = "$unit_id|$col_id|$concept_id";
	
	unless ( $unique_key{$key} )
	{
	    $unique_key{$key} = 1;
	    push @filtered, $r;
	}
    }
    
    return @filtered;
}


# generateFilter ( options )
# 
# Return a list whose first element is an SQL expression that will select only entries
# in the COLLECTION_UNITS table that are consistent with $options. The remainder of the
# elements in the list are text descriptions that can be printed out to let the user
# know what is going on.

sub generateFilter {
    
    my ($self, $options) = @_;
    
    my (@clauses, @descriptions);
    
    if ( my $opt_coll = $options->{collection_no} )
    {
	my (@selected_cn, @bad_cn);
	
	foreach my $cn ( $opt_coll->@* )
	{
	    if ( $cn =~ /^(col:)?(\d+)$/ )
	    {
		push @selected_cn, $2;
		$self->{collection_filter}{$2} = 1;
	    }
	    
	    else
	    {
		push @bad_cn, $cn;
	    }
	}
	
	if ( @bad_cn )
	{
	    my $list = join(', ', @bad_cn);
	    die "Invalid collection_no: $list\n";
	}
	
	elsif ( @selected_cn )
	{
	    my $list = join("','", @selected_cn);
	    
	    push @clauses, "c.collection_no in ('$list')";
	    push @descriptions, "for collection(s) $list";
	}
    }
    
    if ( my $opt_bins = $options->{bin_id} )
    {
	my (@selected_1, @selected_2, @selected_3, @bad_cn);
	
	foreach my $cn ( $opt_bins->@* )
	{
	    if ( $cn =~ /^(bin:)?(1\d+)$/ )
	    {
		push @selected_1, $2;
		$self->{bin_filter}{$2} = 1;
	    }
	    
	    elsif ( $cn =~ /^(bin:)?(2\d+)$/ )
	    {
		push @selected_2, $2;
		$self->{bin_filter}{$2} = 1;
	    }
	    
	    elsif ( $cn =~ /^(bin:)?(3\d+)$/ )
	    {
		push @selected_3, $2;
		$self->{bin_filter}{$2} = 1;
	    }
	    
	    else
	    {
		push @bad_cn, $cn;
	    }
	}
	
	if ( @bad_cn )
	{
	    my $list = join(', ', @bad_cn);
	    die "Invalid bin_id: $list\n";
	}
	
	else
	{
	    if ( @selected_1 )
	    {
		my $list = join("','", @selected_1);
		
		push @clauses, "c.bin_id_1 in ('$list')";
		push @descriptions, "for bin $list";
	    }
	    
	    if ( @selected_2 )
	    {
		my $list = join("','", @selected_2);
		
		push @clauses, "c.bin_id_2 in ('$list')";
		push @descriptions, "for bin $list";
	    }
	    
	    if ( @selected_3 )
	    {
		my $list = join("','", @selected_3);
		
		push @clauses, "c.bin_id_3 in ('$list')";
		push @descriptions, "for bin $list";
	    }
	}
    }

    if ( my $opt_cc = $options->{country} )
    {
	my (@good_cc, @bad_cc);
	
	foreach my $cc ( split /\s*,\s*/, $opt_cc )
	{
	    if ( $cc =~ /^[a-z][a-z]$/i )
	    {
		push @good_cc, $cc;
		$self->{cc_filter}{$cc} = 1;
	    }
	    
	    else
	    {
		push @bad_cc, $cc;
	    }
	}
	
	if ( @bad_cc )
	{
	    my $list = join(', ', @bad_cc);
	    die "Invalid country: $list\n";
	}
	
	elsif ( @good_cc )
	{
	    my $list = join("','", @good_cc);
	    
	    push @clauses, "c.cc in ('$list')";
	    push @descriptions, "for countr(ies) $list";
	}
    }
    
    if ( my $opt_resgroup = $options->{resgroup} )
    {
	$self->{resgroup_filter}{$opt_resgroup} = 1;
	push @clauses, "find_in_set('$opt_resgroup', cc.research_group)";
	push @descriptions, "for collections in research group '$opt_resgroup'";
    }
    
    push @clauses, "1" unless @clauses;
    push @descriptions, "for all collections" unless @descriptions;
    
    my $sql_expr = join(' and ', @clauses);
    
    return ($sql_expr, @descriptions);
}


# getConfig ( )
# 
# Load the configuration settings that will be used in the process of making and
# processing requests to the paleocoordinate service.

sub getConfig {
    
    my ($self) = @_;
    
    loadConfig();
    
    $self->{service_uri} = configData('macrostrat_unit_match_uri') ||
	croak "You must specify 'macrostrat_unit_match_uri' in config.yml";
    
    $self->{update_count} = 0;
    $self->{fail_count} = 0;
    $self->{bad_count} = 0;
    $self->{debug_count} = 0;
    
    $self->{fail_limit} = configData('macrostrat_match_fail_limit') || $DEFAULT_FAIL_LIMIT;
    $self->{bad_limit} = configData('macrostrat_match_bad_response_limit') || $DEFAULT_BAD_RESPONSE_LIMIT;
    $self->{retry_limit} = configData('macrostrat_match_retry_limit') || $DEFAULT_RETRY_LIMIT;
    $self->{retry_interval} = configData('macrostrat_match_retry_interval') || $DEFAULT_RETRY_INTERVAL;
    
    $self->{max_points} = configData('macrostrat_match_limit') || $DEFAULT_MAX_ENTRIES;
}


# prepareStatements ( options )
# 
# Prepare the SQL statements that will be used to store paleocoordinate data
# into the database.

sub prepareStatements {
    
    my ($self, $options) = @_;
    
    my $dbh = $self->{dbh};
    
    my $sql = "DELETE FROM $TABLE{COLLECTION_UNITS} WHERE collection_no = ?";
    
    print STDERR "PREPARED> $sql\n\n" if $self->{debug};
    
    $self->{delete_matches_sth} = $dbh->prepare($sql);
    
    $sql = "INSERT IGNORE INTO $TABLE{COLLECTION_UNITS} (collection_no, unit_id, col_id, certainty)
	    VALUES (?, ?, ?, ?)";
    
    print STDERR "PREPARED> $sql\n\n" if $self->{debug};
    
    $self->{insert_match_sth} = $dbh->prepare($sql);
    
    $sql = "UPDATE $TABLE{COLLECTION_UNITS_STATIC}
	    SET update_new = false, updated = now()
	    WHERE collection_no = ?";
    
    print STDERR "PREPARED> $sql\n\n" if $self->{debug};
    
    $self->{updated_new_sth} = $dbh->prepare($sql);
    
    $sql = "UPDATE $TABLE{COLLECTION_UNITS_STATIC}
	    SET update_existing = false, updated = now()
	    WHERE collection_no = ?";
    
    print STDERR "PREPARED> $sql\n\n" if $self->{debug};
    
    $self->{updated_existing_sth} = $dbh->prepare($sql);
    
    return;
}


sub doSQL {
    
    my ($self, $sql) = @_;
    
    my $dbh = $self->{dbh};
    
    print STDERR "> $sql\n\n" if $self->{debug};
    
    my $result;
    
    eval {
	$result = $dbh->do($sql);
    };
    
    if ( $@ )
    {
	my ($package, $filename, $line) = caller;
	
	my $msg = $@;
	
	$msg =~ s/ at \S+ line \d.*//s;
	$msg .= " at $filename line $line.";
	
	die "$msg\n";
    }
    
    print STDERR "Result: $result\n\n" if $self->{debug};
    
    return $result;
}


