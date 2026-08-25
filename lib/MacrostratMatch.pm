# 
# The Paleobiology Database
# 
# MacrostratMatch.pm
# 
# This module is responsible for generating and updating matches between collections in
# The Paleobiology Database and units/strata/columns in Macrostrat. It does this by
# querying the matching services specified by the entry 'macrostrat_unit_match_uri' and
# 'macrostrat_col_match_uri' in the file 'config.yml'.
# 
# Author: Michael McClennen
# 

package MacrostratMatch;

use strict;

no warnings 'experimental';

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

our ($QUIT_NOW, $EXECUTE_MODE);

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
    
    logMessage(1, "Canceling update of $selector entries $desc");
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
    
    my ($lock) = $dbh->selectrow_array("SELECT GET_LOCK('msmatch new', 0)");
    
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
    
    my ($lock) = $dbh->selectrow_array("SELECT get_lock('msmatch existing', 0)");
    
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
		WHERE not(cs.known_match) and $filter";
	
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
    
    my $CHUNK_SIZE = $update_total >= 30000 ? 10000 : 1000;
    my $REQUEST_SIZE = 50;
    
    my @request_records;
    my $colls_found = 0;
    my $last_found = 0;
    my $colls_matched = 0;

    die "Quitting...\n" if $QUIT_NOW;
    
    $DB::single = 1;
    
  CHUNK:
    while ($update_total)
    {
	$SIG{INT} = undef;
	
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
	
	my %points;
	my %matched;
	my %column_cache;
	
	$SIG{INT} = \&handleInterrupt;
	
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
		last CHUNK if $QUIT_NOW;
		
		my ($lat, $lng, $max_interval, $min_interval, $b_age, $t_age, $max_age, $min_age,
		    $strat_name, $bad_coordinates) = split /[|]/, $point_key;
		
		$colls_found += scalar($points{$bin_id}{$point_key}->@*);
		
		unless ( exists $column_cache{$lat}{$lng} )
		{
		    $column_cache{$lat}{$lng} = $self->lookupContainingColumn($ua, $lat, $lng);
		    
		    if ( $self->{fail_count} >= $self->{fail_limit} )
		    {
			logMessage(1, "ABORTING due to service error count: $self->{fail_count}");
			print STDERR "Aborting due to service error count: $self->{fail_count}\n";
			last CHUNK;
		    }
		}
		
		next POINT unless $strat_name;
		
		my $record = { lat => $lat, lng => $lng, identifier => $point_key,
			       strat_name => $strat_name, age_tolerance => 2, all => 1 };
		
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
			$self->processMatchResponse($response, $points{$bin_id},
						    \%matched, \%column_cache);
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
		    $self->processMatchResponse($response, $points{$bin_id},
						\%matched, \%column_cache);
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
	    
	    my (@unmatched_null, %unmatched_col);
	    
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
			my ($lat, $lng) = split /[|]/, $point_key;
			
			my $containing_col = exists $column_cache{$lat}{$lng} ?
			    $column_cache{$lat}{$lng} : undef;
			
			if ( $containing_col )
			{
			    $unmatched_col{$containing_col} ||= [ ];
			    push $unmatched_col{$containing_col}->@*,
				$points{$bin_id}{$point_key}->@*;
			}
			
			else
			{
			    push @unmatched_null, $points{$bin_id}{$point_key}->@*;
			}
		    }
		}
	    }
	    
	    if ( @unmatched_null )
	    {
		my $unmatched_list = join "','", @unmatched_null;
		
		my $result = $self->doSQL(<<~END_SQL);
		    UPDATE $TABLE{COLLECTION_UNITS_STATIC}
		    SET update_new = false, update_existing = false, containing_col = NULL,
		        updated = now()
		    WHERE collection_no in ('$unmatched_list');
		    END_SQL
	    }

	    foreach my $col ( keys %unmatched_col )
	    {
		my $unmatched_list = join "','", $unmatched_col{$col}->@*;
		my $qcol = $dbh->quote($col);
		
		my $result = $self->doSQL(<<~END_SQL);
		    UPDATE $TABLE{COLLECTION_UNITS_STATIC}
		    SET update_new = false, update_existing = false, containing_col = $qcol,
		        updated = now()
		    WHERE collection_no in ('$unmatched_list');
		    END_SQL
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

    die "Quitting...\n" if $QUIT_NOW;
    
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
    
    print STDERR "POST $uri\n" if $self->{debug};
    print STDERR "$pretty_body\n" if $self->{debug};
    
    my $request = HTTP::Request->new(POST => $uri, undef, $body);
    
    return $self->makeRequest($ua, $request);
}


# enumerated values

our (%LOCATION_BASIS) = ('containing column' => 1, 'adjacent column' => 1, 'other' => 1);
our (%NAME_BASIS) = ('exact' => 1, 'concept' => 1, 'rank-up', => 1, 'rank-down' => 1,
		     'synonym' => 1, 'other' => 1);
our (%AGE_BASIS) = ('containing interval' => 1, 'adjacent interval' => 1, 'other' => 1);


# processMatchResponse ( response_data, point_hash )
#
# Process a response received back from the match API service.

sub processMatchResponse {

    my ($self, $response, $point_hash, $matched_hash, $column_cache) = @_;
    
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

	    $result = $self->doSQL($sql);

	    if ( @matches == 1 )
	    {
		$matches[0]{certainty} = 1;
	    }
	    
	    elsif ( @matches > 1 )
	    {
		@matches = $self->filterMatches($point_key, @matches);
	    }
	    
	    my $insertions = '';
	    
	    foreach my $collection_no ( @collections )
	    {
		$self->{update_count}++;
		
		foreach my $match ( @matches )
		{
		    if ( $match->{location_basis} && !$LOCATION_BASIS{$match->{location_basis}} )
		    {
			$match->{location_basis} = 'other';
		    }
		    
		    if ( $match->{name_basis} && !$NAME_BASIS{$match->{name_basis}} )
		    {
			$match->{name_basis} = 'other';
		    }
		    
		    if ( $match->{age_basis} && !$AGE_BASIS{$match->{age_basis}} )
		    {
			$match->{age_basis} = 'other';
		    }
		    
		    my $unit_id = $dbh->quote($match->{unit_id} || '0');
		    my $col_id = $dbh->quote($match->{col_id} || '0');
		    my $location_basis = $dbh->quote($match->{location_basis});
		    my $concept_id = $dbh->quote($match->{concept_id} || '0');
		    my $concept_name = $dbh->quote($match->{concept_name});
		    my $strat_name_id = $dbh->quote($match->{strat_name_id});
		    my $strat_name = $dbh->quote($match->{strat_name});
		    my $strat_rank = $dbh->quote($match->{strat_rank});
		    my $strat_parent_id = $dbh->quote($match->{parent_id});
		    my $name_basis = $dbh->quote($match->{name_basis});
		    my $t_age = $dbh->quote($match->{t_age});
		    my $b_age = $dbh->quote($match->{b_age});
		    my $age_basis = $dbh->quote($match->{age_basis});
		    my $certainty = $dbh->quote($match->{certainty});
		    
		    $insertions .= ',' if $insertions;
		    $insertions .= "($collection_no, $unit_id, $col_id, $location_basis, $concept_id, $concept_name, $strat_name_id, $strat_name, $strat_rank, $strat_parent_id, $name_basis, $t_age, $b_age, $age_basis, $certainty)\n";
		}
	    }
	    
	    if ( $insertions )
	    {
		$sql = "INSERT INTO $TABLE{COLLECTION_UNITS} (collection_no, unit_id, col_id, location_basis, concept_id, concept_name, strat_name_id, strat_name, strat_rank, strat_parent_id, name_basis, t_age, b_age, age_basis, certainty) VALUES\n$insertions";
		
		$result = $self->doSQL($sql);
	    }

	    my $matched_list = join "','", @collections;
	    
	    my ($lat, $lng) = split /[|]/, $point_key;
	    
	    my $containing_col = exists $column_cache->{$lat}{$lng} ?
		$column_cache->{$lat}{$lng} : undef;
	    
	    my $qcol = $dbh->quote($containing_col);
	    
	    $sql = "UPDATE $TABLE{COLLECTION_UNITS_STATIC}
		SET update_new = false, update_existing = false, containing_col = $qcol,
		    updated = now()
		WHERE collection_no in ('$matched_list')";
	    
	    $result = $self->doSQL($sql);
	    
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
    
    # Scan through the returned matches. If we can find one with a unit id and a strat
    # name id where the location_basis is 'containing column', then ignore all the
    # others.
    
    my @filtered;
    my %unique_key;
    
    foreach my $r ( @matches )
    {
	my $unit_id = $r->{unit_id} || '0';
	my $col_id = $r->{col_id} || '0';
	my $concept_id = $r->{concept_id} || '0';
	my $key = "$unit_id|$col_id|$concept_id";
	
	if ( $r->{unit_id} && $r->{strat_name_id} &&
	     $r->{location_basis} && $r->{location_basis} eq 'containing column' )
	{
	    return ($r);
	}
	
	unless ( $unique_key{$key} )
	{
	    $unique_key{$key} = 1;
	    push @filtered, $r;
	}
    }
    
    if ( @filtered )
    {
	my $certainty = int(1000 / scalar(@filtered)) / 1000;
	
	foreach my $r ( @filtered ) { $r->{certainty} = $certainty }
    }
    
    return @filtered;
}


# lookupContainingColumn ( user_agent, lat, lng )
#
# Make a column API request, to determine which Macrostrat column covers the specified
# lat/lng point.

sub lookupContainingColumn {

    my ($self, $ua, $lat, $lng) = @_;
    
    my $uri = $self->{column_uri} . "?lat=$lat&lng=$lng";
    
    # Generate a column request.  The actual request is wrapped inside a
    # while loop so that we can retry it if something goes wrong.
    
    print STDERR "GET $uri\n" if $self->{debug};
    
    my $request = HTTP::Request->new(GET => $uri);
    
    my $response = $self->makeRequest($ua, $request);
    
    if ( $response && ref $response->{success}{data} eq 'ARRAY' &&
	 $response->{success}{data}->@* )
    {
	return $response->{success}{data}[0]{col_id};
    }
    
    else
    {
	return;
    }
}


sub makeRequest {

    my ($self, $ua, $request) = @_;
    
    my ($response, $content_ref, $data);
    my $retry_count = $self->{retry_limit};
    my $retry_interval = $self->{retry_interval};
    
 RETRY:
    while ( $retry_count )
    {
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



# handleInterrupt ( )
#
# Handle an INT signal by setting a global variable indicating that we should quit as
# soon as possible.

sub handleInterrupt {

    # If we are running under the debugger, stop and drop into the debugger.
    
    if ( $DB::VERSION )
    {
	$DB::single = 1;
    }
    
    # Otherwise, set the global variable.
    
    else
    {
	$QUIT_NOW = 1;
    }
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
    
    $self->{column_uri} = configData('macrostrat_col_match_uri') ||
	croak "You must specify 'macrostrat_col_match_uri' in config.yml";
    
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


# initializeTables ( )
#
# Ensure that the proper tables are present in the database to which we have connected.
# If they are not present, create them. If an earlier version of the 'coll_units' table
# is present, preserve its content.

sub initializeTables {

    my ($self) = @_;
    
    my $dbh = $self->{dbh};
    
    unless ( $EXECUTE_MODE )
    {
	print "The following statements would be executed if the 'initialize tables'\n";
	print "subcommand is given:\n\n";
    }
    
    my $changes = 0;
    
    # First establish 'coll_units_static'.
    
    my ($check_table) = $dbh->selectrow_array(<<~END_SQL);
	SHOW TABLES LIKE '$TABLE{COLLECTION_UNITS_STATIC}'
	END_SQL
    
    unless ( $check_table && $check_table eq $TABLE{COLLECTION_UNITS_STATIC} )
    {
	$self->doStmtPrint(<<~END_SQL);
		CREATE TABLE IF NOT EXISTS `$TABLE{COLLECTION_UNITS_STATIC}` (
		  `collection_no` int(10) unsigned NOT NULL,
		  `containing_col` int(10) unsigned DEFAULT NULL,
		  `known_match` tinyint(1) NOT NULL DEFAULT 0,
		  `update_new` tinyint(1) unsigned NOT NULL DEFAULT 0,
		  `update_existing` tinyint(1) unsigned NOT NULL DEFAULT 0,
		  `updated` timestamp NULL DEFAULT NULL,
		  PRIMARY KEY (`collection_no`),
		  KEY `update_new` (`update_new`),
		  KEY `update_existing` (`update_existing`),
		  KEY `containing_col` (`containing_col`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci		    
		END_SQL
	
	$changes++;
    }
    
    # Then check for the existence of 'coll_units';
    
    ($check_table) = $dbh->selectrow_array(<<~END_SQL);
	SHOW TABLES LIKE '$TABLE{COLLECTION_UNITS}'
	END_SQL
    
    # If it exists, check for the column 'concept_id'. If it doesn't exist, rename the
    # table and then create a new one and insert the old contents.
    
    my $check_column;
    
    my $coll_units_create = <<~END_SQL;
	CREATE TABLE `$TABLE{COLLECTION_UNITS}` (
	  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
	  `collection_no` int(10) unsigned NOT NULL,
	  `unit_id` int(10) unsigned NOT NULL DEFAULT 0,
	  `col_id` int(10) unsigned NOT NULL DEFAULT 0,
	  `location_basis` enum('containing column','adjacent column','other') DEFAULT NULL,
	  `concept_id` int(10) unsigned NOT NULL DEFAULT 0,
	  `concept_name` varchar(255) DEFAULT NULL,
	  `strat_name_id` int(10) unsigned DEFAULT NULL,
	  `strat_name` varchar(255) DEFAULT NULL,
	  `strat_rank` enum('','SGp','Gp','SubGp','Fm','Mbr','Bed') DEFAULT NULL,
	  `strat_parent_id` int(10) unsigned DEFAULT NULL,
	  `name_basis` enum('exact','concept','rank-up','rank-down','synonym','other') DEFAULT NULL,
	  `t_age` decimal(9,6) DEFAULT NULL,
	  `b_age` decimal(9,6) DEFAULT NULL,
	  `age_basis` enum('containing interval','adjacent interval','other') DEFAULT NULL,
	  `certainty` tinyint(3) unsigned DEFAULT NULL,
	  PRIMARY KEY (`id`),
	  KEY `collection_no` (`collection_no`),
	  KEY `unit_id` (`unit_id`),
	  KEY `col_id` (`col_id`),
	  KEY `concept_id` (`concept_id`),
	  KEY `strat_name_id` (`strat_name_id`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
	END_SQL
    
    if ( $check_table && $check_table eq $TABLE{COLLECTION_UNITS} )
    {
	($check_column) = $dbh->selectrow_array(<<~END_SQL);
		SHOW COLUMNS FROM `$TABLE{COLLECTION_UNITS}` LIKE 'concept_id'
		END_SQL
	
	unless ( $check_column )
	{
	    $self->doStmtPrint(<<~END_SQL);
		RENAME TABLE `$TABLE{COLLECTION_UNITS}` to `$TABLE{COLLECTION_UNITS}_bak`
		END_SQL
	    
	    $self->doSQL($coll_units_create, 1);

	    $self->doStmtPrint(<<~END_SQL);
		INSERT INTO `$TABLE{COLLECTION_UNITS}` (collection_no, unit_id, col_id, certainty)
		SELECT collection_no, unit_id, col_id, 1 as certainty
		FROM `$TABLE{COLLECTION_UNITS}_bak`
		END_SQL
	    
	    $changes++;
	}
    }
    
    else
    {
	$self->doStmtPrint($coll_units_create, 1);
	$changes++;
    }
    
    unless ( $changes )
    {
	print "All tables are up to date.\n\n";
    }
}


# doSQL ( sql )
#
# Execute the specified SQL statement using the database handle that was established
# when the referenced object was initialized. If we are running in debug mode, print the
# statement to STDERR first. If an error occurs, throw an exception indicating the line
# from which this method was called. Otherwise, return the result.

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
	
	print STDERR "$sql\n\n" unless $self->{debug};
	
	die "$msg\n";
    }
    
    print STDERR "Result: $result\n\n" if $self->{debug};
    
    return $result;
}


# doStmtPrint ( sql )
#
# Print the specified SQL statement. If the variable $EXECUTE_MODE has a true value,
# execute it as well.

sub doStmtPrint {

    my ($self, $sql) = @_;

    my $dbh = $self->{dbh};

    print "> $sql\n\n";

    if ( $EXECUTE_MODE )
    {
	my $result = $dbh->do($sql);
	
	print "Result: $result\n\n";
    }
}

1;
