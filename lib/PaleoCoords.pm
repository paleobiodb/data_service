# 
# The Paleobiology Database
# 
#   PaleoCoords.pm
# 
# This module is responsible for updating the paleocoordinates of collections
# in the Paleobiology Database, by querying the PaleoCoords service at
# caltech.edu.
# 
# Author: Michael McClennen
# 

package PaleoCoords;

use strict;

use base 'Exporter';

our (@EXPORT_OK) = qw(updatePaleocoords ensureTables readPlateData);

use Carp qw(carp croak);
use Try::Tiny;
use JSON;
use LWP::UserAgent;

use TableDefs qw(%TABLE $COLL_MATRIX);
use ConsoleLog qw(logMessage);
use CoreFunction qw(loadConfig configData);


our ($DEFAULT_RETRY_LIMIT) = 3;
our ($DEFAULT_RETRY_INTERVAL) = 5;
our ($DEFAULT_FAIL_LIMIT) = 3;
our ($DEFAULT_BAD_RESPONSE_LIMIT) = 5;
our ($DEFAULT_MAX_POINTS) = 35;


# updateCoords ( dbh, options )
# 
# Update all or some of the paleocoordinates in the database, using the
# PaleoCoords service.

sub updateCoords {
    
    my ($class, $dbh, $options) = @_;
    
    $options ||= {};
    
    # We start by loading the relevant configuration settings from the
    # paleobiology database configuration file.
    
    loadConfig();
    
    my $service_uri = configData('paleocoord_point_uri') ||
	croak "You must specify 'paleocoord_point_uri' in config.yml";
    
    my $retry_limit = configData('paleocoord_retry_limit') || $DEFAULT_RETRY_LIMIT;
    my $retry_interval = configData('paleocoord_retry_interval') || $DEFAULT_RETRY_INTERVAL;
    my $fail_limit = configData('paleocoord_fail_limit') || $DEFAULT_FAIL_LIMIT;
    my $bad_limit = configData('paleocoord_bad_response_limit') || $DEFAULT_BAD_RESPONSE_LIMIT;
    my $max_points = configData('paleocoord_point_limit') || $DEFAULT_MAX_POINTS;
    
    # Then process any command-line options.
    
    my $min_age = 0;
    my $max_age = 600;
    
    $min_age = $options->{min_age} + 0 if defined $options->{min_age} && $options->{min_age} > 0;
    $max_age = $options->{max_age} + 0 if defined $options->{max_age} && $options->{max_age} > 0;
    
    # We then create a control object to manage this process.
    
    my $self = { dbh => $dbh,
		 update_count => 0,
		 debug => $options->{debug},
		 fail_count => 0,
		 fail_limit => $fail_limit,
		 bad_count => 0,
		 bad_limit => $bad_limit,
		 model_count => 0,
		 debug_count => 0,
		 min_age_bound => $min_age,
		 max_age_bound => $max_age,
		 service_uri => $service_uri,
		 retry_limit => $retry_limit,
		 retry_interval => $retry_interval,
		 max_points => $max_points,
		 quiet => $options->{quiet},
		 verbose => $options->{verbose},
	       };
    
    bless $self, 'PaleoCoords';
    
    $self->initMessage($options);
    
    my ($sql, $result, $count, %points);
    
    # If any filters were specified, compute them now.
    
    my $filters = '';
    
    if ( $options->{collection_no} )
    {
	$filters .= " and collection_no in ($options->{collection_no})";
    }
    
    # Get a list of the available paleocoordinate models.
    
    my (@model_list, %min_age, %max_age);
    
    my $model_list = $dbh->selectall_arrayref("SELECT * FROM $TABLE{PCOORD_MODELS}", { Slice => {} });
    
    if ( ref $model_list eq 'ARRAY' && $model_list->@* )
    {
	foreach my $entry ( $model_list->@* )
	{
	    my $name = $entry->{name} || next;
	    
	    push @model_list, $name;
	    $min_age{$name} = $entry->{min_age};
	    $max_age{$name} = $entry->{max_age};
	}
	
	logMessage(1, "Paleocoordinates will be computed for: " . join(', ', @model_list));
    }
    
    else
    {
	logMessage(1, "No paleocoordinate models were found");
	print STDERR "No paleocoordinate models were found.\n";
	return;
    }
    
    # If the option 'clear_all' was specified, then we clear the $PALEOCOORDS table of entries
    # falling into the given age range.  No further action should be taken in this case.
    
    if ( $options->{clear_all} )
    {
	logMessage(2, "    clearing all paleocoords between $max_age Ma and $min_age Ma...");
	
	$sql = "UPDATE $TABLE{PCOORD_DATA}
		SET early_lng = null, early_lat = null
		WHERE early_age between $min_age and $max_age $filters";
	
	$count = $dbh->do($sql);
	
	$sql = "UPDATE $TABLE{PCOORD_DATA}
		SET mid_lng = null, mid_lat = null
		WHERE mid_age between $min_age and $max_age $filters";
	
	$count += $dbh->do($sql);
	
	$sql = "UPDATE $TABLE{PCOORD_DATA}
		SET late_lng = null, late_lat = null
		WHERE late_age between $min_age and $max_age $filters";
	
	$count += $dbh->do($sql);
	
	logMessage(2, "      cleared $count coordinates");
	
	# If 'clear_all' was specified, then we are done.
	
	return;
    }
    
    # If the option 'update_all' was specified, we flag all entries falling into the given age
    # range so that they will be updated below.  Otherwise, only entries that have been added or
    # changed will be updated.
    
    elsif ( $options->{update_all} || $options->{collection_no} )
    {
	if ( $options->{update_all} )
	{
	    logMessage(2, "    updating all paleocoords between $max_age Ma and $min_age Ma...");
	}
	
	else
	{
	    logMessage(2, "    updating all paleocoords for the following collections: " .
		       " $options->{collection_no}");
	}
	
	$sql = "UPDATE $TABLE{PCOORD_DATA}
		SET early_lng = null, early_lat = null, update_early = true
		WHERE early_age between $min_age and $max_age $filters";
	
	$dbh->do($sql);
	
	$sql = "UPDATE $TABLE{PCOORD_DATA}
		SET mid_lng = null, mid_lat = null, update_mid = true
		WHERE mid_age between $min_age and $max_age $filters";
	
	$dbh->do($sql);
	
	$sql = "UPDATE $TABLE{PCOORD_DATA}
		SET late_lng = null, late_lat = null, update_late = true
		WHERE late_age between $min_age and $max_age $filters";
	
	$dbh->do($sql);
	
	$sql = "SELECT count(*) FROM $TABLE{PCOORD_DATA}
		WHERE update_early or update_mid or update_late";
	
	my ($count) = $dbh->selectrow_array($sql);
	
	logMessage(2, "      flagging $count coordinates to update");
    }
    
    # Otherwise, we check for all coordinates within the specified age range
    # that need updating.
    
    else
    {
	# Lock the tables $TABLE{PCOORD_DATA} and $COLL_MATRIX, to avoid deadlock with other processes.
	
	$sql = "LOCK TABLES $TABLE{PCOORD_DATA} as p WRITE, $COLL_MATRIX as c READ";
	
	my $locks = $dbh->do($sql);
	
	# Delete any rows in $TABLE{PCOORD_DATA} corresponding to collections whose
	# coordinates have been made invalid.  This should not happen very often, but is a
	# boundary case that we need to take care of.
	
	$sql = "SELECT count(*) FROM $TABLE{PCOORD_DATA} as p
		    join $COLL_MATRIX as c using (collection_no)
		WHERE c.lat is null or c.lat not between -90 and 90 or
		      c.lng is null or c.lng not between -180 and 180";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( defined $count && $count > 0 )
	{
	    $sql = "DELETE FROM p
		USING $TABLE{PCOORD_DATA} as p join $COLL_MATRIX as c using (collection_no)
		WHERE c.lat is null or c.lat not between -90 and 90 or
		      c.lng is null or c.lng not between -180 and 180";
	    
	    $result = $dbh->do($sql);
	    
	    logMessage(2, "    cleared paleocoords from $count collections without a valid location");
	}
	
	# Then mark for update any rows in $TABLE{PCOORD_DATA} corresponding to collections
	# whose coordinates have been edited.  These will have to be recomputed entirely.
	
	$sql = "SELECT count(distinct collection_no) FROM $TABLE{PCOORD_DATA} as p
		    join $COLL_MATRIX as c using (collection_no)
                WHERE c.lat <> p.present_lat or c.lng <> p.present_lng";
	
	($count) = $dbh->selectrow_array($sql);
	
	if ( defined $count && $count > 0 )
	{
	    $sql = "UPDATE $TABLE{PCOORD_DATA} as p join $COLL_MATRIX as c using (collection_no)
		SET p.update_early = true, p.update_mid = true, p.update_late = true
                WHERE c.lat <> p.present_lat or c.lng <> p.present_lng";
	    
	    $result = $dbh->do($sql);
	    
	    logMessage(2, "    marked paleocoords from $count collections whose location has been changed");
	}
	
	# Now iterate through all available models. For each model, mark any paleocoordinate
	# entries corresponding to collections whose ages have changed and add rows for
	# collections which don't have entries for that model. But only mark entries whose age
	# falls within the age limit for each model.
	
	foreach my $model ( @model_list )
	{
	    my $min_age = $min_age{$model};
	    my $max_age = $max_age{$model};
	    
	    $sql = "UPDATE $TABLE{PCOORD_DATA} as p JOIN $COLL_MATRIX as c using (collection_no)
		SET p.early_age = null, p.early_lng = null, p.early_lat = null, p.update_early =
		    if(round(c.early_age,0) >= $min_age and round(c.early_age,0) <= $max_age
		WHERE round(c.early_age,0) <> p.early_age and
		      (round(c.early_age,0) between $min_age and $max_age or p.early_age is not null)";
	
	$count = $dbh->do($sql);
	
	$sql = "UPDATE $TABLE{PCOORD_DATA} as p JOIN $COLL_MATRIX as c using (collection_no)
		SET p.late_age = null, p.late_lng = null, p.late_lat = null, p.update_late = true
		WHERE round(c.late_age,0) <> p.late_age and
		      (round(c.late_age,0) between $min_age and $max_age or p.late_age is not null)";
	
	$count += $dbh->do($sql);
	
	$sql = "UPDATE $TABLE{PCOORD_DATA} as p JOIN $COLL_MATRIX as c using (collection_no)
		SET p.mid_age = null, p.mid_lng = null, p.mid_lat = null, p.update_mid = true
		WHERE round((c.early_age + c.late_age)/2,0) <> p.mid_age and
		      (round((c.early_age + c.late_age)/2,0) between $min_age and $max_age or 
		       p.mid_age is not null)";
	
	$count += $dbh->do($sql);
	
	if ( defined $count && $count > 0 )
	{
	    $self->initMessage($options);
	    logMessage(2, "    marked $count entries whose ages did not correspond to their collections");
	}
	
	# Then create new rows for collections that do not yet have paleocoords.
	
	    $sql = "INSERT IGNORE INTO $TABLE{PCOORD_DATA}
		(collection_no, model, present_lng, present_lat, update_early, update_mid, update_late)
	    SELECT c.collection_no, '$model', c.lng, c.lat, true, true, true
	    FROM $COLL_MATRIX as c left join $TABLE{PCOORD_DATA} as p using (collection_no)
	    WHERE p.collection_no is null";
	    
	    $count = $dbh->do($sql);
	    
	    logMessage(2, "    created $count new rows for model '$model'");
	    
	    $sql = "UPDATE $TABLE{PCOORD_DATA}
		SET update_early = false"
	}
	
	$dbh->do("UNLOCK TABLES");
    }
    
    # Now query for all collections whose paleocoordinates need updating.  This includes:
    # 
    # - collections without a corresponding valid paleocoords entry
    # - collections where at least one paleocoords entry is null
    # - collections where at least one paleocoords entry is flagged for updating
    
    # logMessage(2, "    looking for collections whose palecoordinates need updating...");
    
    $sql =     "SELECT collection_no, c.lng as present_lng, c.lat as present_lat,
		       'early' as selector, round(c.early_age,0) as age
    		FROM $COLL_MATRIX as c LEFT JOIN $TABLE{PCOORD_DATA} as p using (collection_no)
    		WHERE c.lat between -90.0 and 90.0 and c.lng between -180.0 and 180.0
			and round(c.early_age,0) between $min_age and $max_age
			and p.update_early $filters";
    
    my $early_updates = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    $sql =     "SELECT collection_no, c.lng as present_lng, c.lat as present_lat,
		       'mid' as selector, round((c.early_age + c.late_age)/2,0) as age
    		FROM $COLL_MATRIX as c LEFT JOIN $TABLE{PCOORD_DATA} as p using (collection_no)
    		WHERE c.lat between -90.0 and 90.0 and c.lng between -180.0 and 180.0
			and round((c.early_age + c.late_age)/2,0) between $min_age and $max_age
			and p.update_mid $filters";
    
    my $mid_updates = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    $sql =     "SELECT collection_no, c.lng as present_lng, c.lat as present_lat,
		       'late' as selector, round(c.late_age,0) as age
    		FROM $COLL_MATRIX as c LEFT JOIN $TABLE{PCOORD_DATA} as p using (collection_no)
    		WHERE c.lat between -90.0 and 90.0 and c.lng between -180.0 and 180.0
			and round(c.late_age,0) between $min_age and $max_age
			and p.update_late $filters";
    
    my $late_updates = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    $count = 0;
    
    foreach my $record ( @$early_updates, @$mid_updates, @$late_updates )
    {
	my $collection_no = $record->{collection_no};
	my $lng = $record->{present_lng};
	my $lat = $record->{present_lat};
	my $selector = $record->{selector};
	my $age = $record->{age};
	
	push $points{$age}->@*, [$collection_no, $selector, $lng, $lat];
	$count++;
    }
    
    if ( $count )
    {
	$self->initMessage($options);
	logMessage(2, "    found $count entries to update");
    }
    
    else
    {
	return;
    }
    
    # Get a list of all paleocoordinate ages.
    
    my @age_list = sort { $a <=> $b } keys %points;
    
    # At this point, we need to prepare the SQL statements that will be used
    # to update entries in the table.
    
    $self->prepareStatements();
    
    # Now iterate through all of the available models and all of the ages of
    # paleocoordinates that need to be generated or updated. For each model/age
    # combination, send off as many requests as are necessary to generate all of the
    # paleocoordinates for that model and age.
        
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database Updater/0.1");
    
    $DB::single = 1;
    
  MODEL:
    foreach my $model ( $model_list->@* )
    {
      AGE:
	foreach my $age ( @age_list )
	{
	    # Ignore any ages that fall outside the bounds of this model.
	    
	    next AGE if $max_age{$model} && $age > $max_age{$model};
	    next AGE if $min_age{$model} && $age < $min_age{$model};
	    
	    # Grab the set of points that need to be rotated to this age.
	    
	    my @points = $points{$age}->@*;
	    
	    # Now create as many requests as are necessary to rotate all of these
	    # points. 
	    
	    while ( @points )
	    {
		# Start building a parameter list.
		
		my $request_params = "model=$model&time=$age&include_failures=1&data=";
		my $sep = '';
		
		# Keep track of the selector and collection_no for each point.
		
		my @request_points;
		
		# Add each point, up to the limit for a single request.
	    
	      POINT:
		while ( my $point = shift @points )
		{
		    my ($coll_no, $selector, $lng, $lat) = @$point;
		    
		    # Skip any point with empty coordinates.
		    
		    next unless $lng && $lng ne '' && $lat && $lat ne '';
		    
		    # Add the rest to the end of the parameter list.
		    
		    $request_params .= $sep; $sep = '+';
		    $request_params .= "$lng,$lat";
		    
		    push @request_points, $point;
		    
		    last POINT if @request_points >= $self->{max_points};
		}
		
		# Now if we have at least one point to rotate then fire off the request and
		# process the answer (if any). If processResponse returns false, abort the
		# task. 
		
		if ( @request_points )
		{
		    my $request_uri = "$service_uri?$request_params";
		    
		    my $data = $self->makeRotationRequest($ua, $request_uri);
		    
		    if ( $data )
		    {
			$self->processResponse($model, $age, $data, \@request_points);
		    }
		    
		    if ( $self->{fail_count} > $self->{fail_limit} )
		    {
			logMessage(1, "ABORTING due to service error count: $self->{fail_count}");
			print STDERR "Aborting due to service error count: $self->{fail_count}\n";
			last AGE;
		    }
		    
		    if ( $self->{bad_count} > $self->{bad_limit} )
		    {
			logMessage(1, "ABORTING due to database error count: $self->{bad_count}");
			print STDERR "Aborting due to database error count: $self->{bad_count}\n";
			last AGE;
		    }
		}
	    }
	}
    }
    
    logMessage(2, "    updated $self->{update_count} paleocoordinate entries");
    
    my $a = 1; # we can stop here when debugging
}


sub initMessage {

    my ($self, $options) = @_;
    
    return if $self->{init_message};
    $self->{init_message} = 1;
    
    my $now = localtime;
    
    logMessage(1, "Updating paleocoordinates at $now");
}


sub prepareStatements {
    
    my ($self) = @_;
    
    my $dbh = $self->{dbh};
    my $sql;
    
    $sql = "INSERT IGNORE INTO $TABLE{PCOORD_DATA} 
		(collection_no, model, present_lng, present_lat, update_early, update_mid, update_late)
	    SELECT collection_no, ? as model, lng, lat FROM $COLL_MATRIX
	    WHERE collection_no = ?";
    
    $self->{add_row_sth} = $dbh->prepare($sql);
    
    $sql = "UPDATE $TABLE{PCOORD_DATA}
	    SET early_age = ?, early_lng = ?, early_lat = ?, update_early = 0
	    WHERE collection_no = ? and model = ? LIMIT 1";
    
    $self->{early_sth} = $dbh->prepare($sql);
    
    $sql = "UPDATE $TABLE{PCOORD_DATA}
	    SET mid_age = ?, mid_lng = ?, mid_lat = ?, update_mid = 0
	    WHERE collection_no = ? and model = ? LIMIT 1";
    
    $self->{mid_sth} = $dbh->prepare($sql);
    
    $sql = "UPDATE $TABLE{PCOORD_DATA}
	    SET late_age = ?, late_lng = ?, late_lat = ?, update_late = 0
	    WHERE collection_no = ? and model = ? LIMIT 1";
    
    $self->{late_sth} = $dbh->prepare($sql);

    $sql = "UPDATE $TABLE{PCOORD_DATA}
	    SET plate_no = ?
	    WHERE collection_no = ? and model = ? LIMIT 1";
    
    $self->{set_plate} = $dbh->prepare($sql);
    
    # $sql = "UPDATE $COLL_MATRIX as c JOIN $TABLE{PCOORD_DATA} as pc using (collection_no)
    # 	    SET c.g_plate_no = pc.plate_no
    # 	    WHERE collection_no = ?";
	
    # $self->{coll_matrix_sth} = $dbh->prepare($sql);
    
    return;
}


# makeRotationRequest ( ua, request )
# 
# Make a request to the paleocoordinate rotation service using the specified user agent
# object.

sub makeRotationRequest {

    my ($self, $ua, $request) = @_;
    
    # Generate a rotation request.  The actual request is wrapped inside a
    # while loop so that we can retry it if something goes wrong.
    
    print STDERR "GET $request\n" if $self->{debug};
    
    my $req = HTTP::Request->new(GET => $request);
    
    my ($resp, $content_ref, $data);
    my $retry_count = $self->{retry_limit};
    my $retry_interval = $self->{retry_interval};
    
 RETRY:
    while ( $retry_count )
    {
	# $DB::single = 1;
	
	my $start = time;
	my $time1;
	
	$resp = $ua->request($req);
	$content_ref = $resp->content_ref;
	
	# If the request succeeds, decode the content. If that succeeds, reset fail count
	# and return.
	
	if ( $resp->is_success )
	{
	    eval {
		$data = decode_json($$content_ref);
	    };
	    
	    if ( ref $data eq 'ARRAY' )
	    {
		$self->{fail_count} = 0;
		return $data;
	    }
	    
	    elsif ( $data )
	    {
		logMessage(2, "      Response JSON must be a list: $request");
		print STDERR "ERROR: response JSON must be a list: $request\n";
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
	
	my $code = $resp->code;
	logMessage(2, "      Request failed with code '$code': $request");
	print STDERR "REQUEST FAILED WITH CODE '$code': $request\n";
	$self->{fail_count}++;
	
	if ( $self->{debug} )
	{
	    $self->{debug_count}++;
	    open(OUTFILE, ">gpfail.$self->{debug_count}.html");
	    print OUTFILE $resp->content;
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


# processResponse ( age, content_ref, points_ref )
# 
# Process a response received from the rotation service. Both $content_ref and $points_ref
# must be array references.
# 
# We need the value of $age because the response does not include this information. The
# request MUST include the argument 'include_failures=1', so that points which cannot be
# rotated are returned as null entries.  This enables us to match up the entries in
# $content_ref with the corresponding entries in $points_ref.

sub processResponse {
    
    my ($self, $model, $age, $data, $points) = @_;
    
    my $dbh = $self->{dbh};
    my $response;
    my @bad_list;
    
    # Iterate through the entries in $data.
    
  POINT:
    foreach my $i ( 0..$data->$#* )
    {
	my $entry = $data->[$i];
	
	# Get the corresponding collection identifier and selector (early, mid, late) from
	# the $points_ref array.
	
	my ($coll_no, $selector) = $points->[$i]->@*;
	
	# If the entry is null, it means that the specified rotation model cannot produce
	# a paleocoordinate for this point/age combination. Set the paleocoordinates for
	# the corresponding collection identifier, model, selector, and age to null.
	
	if ( ! $entry )
	{
	    $self->updateOneEntry($coll_no, $model, $selector, $age, undef, undef, undef);
	}
	
	# If the result entry contains a list of coordinates, set the paleocoordinates for
	# the corresponding collection identifier, selector, and age to these values. If
	# there is a third value, it should be the plate identifier.
	
	elsif ( ref $entry eq 'HASH' && ref $entry->{geometry}{coordinates} eq 'ARRAY' )
	{
	    my ($lng, $lat) = $entry->{geometry}{coordinates}->@*;
	    
	    my $plate_id = $entry->{properties}{plate_id};
	    
	    $self->updateOneEntry($coll_no, $model, $selector, $age, $lng, $lat, $plate_id);
	}
	
	else
	{
	    push @bad_list, "$coll_no $age ($selector)";
	}
    }
    
    # If there were any entries that we couldn't parse, report them. If we have exceeded
    # the limit for bad results, return false which will terminate the task.
    
    if ( @bad_list )
    {
	my $count = scalar(@bad_list);
	my $list = join(', ', @bad_list);
	
	logMessage(1, "ERROR: the following $count entries were not updated because the PaleoCoords response was invalid:");
	logMessage(1, $list);
	
	if ( ++$self->{bad_count} > $self->{bad_limit} )
	{
	    logMessage(1, "ABORTING DUE TO BAD RESPONSE COUNT");
	    return;
	}
    }
    
    # Otherwise, return true.
    
    return 1;
}


sub updateOneEntry {
    
    my ($self, $collection_no, $model, $selector, $age, $lng, $lat, $plate_id) = @_;
    
    my $dbh = $self->{dbh};
    
    eval {
	$self->{add_row_sth}->execute($model, $collection_no);
	
	if ( $selector eq 'early' )
	{
	    $self->{early_sth}->execute($age, $lng, $lat, $collection_no, $model);
	}
	
	elsif ( $selector eq 'mid' )
	{
	    $self->{mid_sth}->execute($age, $lng, $lat, $collection_no, $model);
	}
	
	elsif ( $selector eq 'late' )
	{
	    $self->{late_sth}->execute($age, $lng, $lat, $collection_no, $model);
	}
	
	if ( $plate_id )
	{
	    $self->{set_plate}->execute($plate_id, $collection_no, $model);
	    # $self->{coll_matrix_sth}->execute($collection_no);
	}
	
	$self->{update_count}++;
    };
    
    if ( $@ )
    {
	logMessage(1, "ERROR updating the database: $@");
	print STDERR "ERROR updating the database: $@\n";
	$self->{bad_count}++;
    }
}


# initializeTables ( dbh, argument, replace)
# 
# Create the tables used by this module. If $replace is true, drop the existing ones.
# Otherwise, rename them using the extension _bak.

sub initializeTables {
    
    my ($class, $dbh, $argument, $replace) = @_;
    
    unless ( $argument =~ qr{ ^ tables $ | ^ PCOORD_ (DATA|PRESENT|MODELS|PLATES) $ }xs )
    {
	die "Invalid argument '$argument'";
    }
    
    if ( $argument eq 'PCOORD_DATA' || $argument eq 'tables' )
    {
	initOneTable('PCOORD_DATA', $replace, 
		     "CREATE TABLE IF NOT EXISTS $TABLE{PCOORD_DATA} (
			collection_no int unsigned not null,
			model varchar(255) not null,
			selector enum('early', 'mid', 'late') not null,
			age smallint unsigned not null,
			plate_no smallint unsigned not null default '0',
			paleo_lng decimal(5,2) null,
			paleo_lat decimal(5,2) null,
			update_flag boolean not null,
			updated timestamp not null
			PRIMARY KEY (collection_no, model, selector),
			) Engine=MyISAM CHARSET=utf8");
    }
    
    if ( $argument eq 'PCOORD_STATIC' || $argument eq 'tables' )
    {
	if ( $replace )
	{
	    logMessage(1, "Replacing table PCOORD_STATIC as '$TABLE{PCOORD_STATIC}'");
	    $dbh->do("DROP TABLE IF EXISTS $TABLE{PCOORD_STATIC}");
	}
	
	else
	{
	    logMessage(1, "Creating new table PCOORD_STATIC as '$TABLE{PCOORD_STATIC}'");
	    $dbh->do("DROP TABLE IF EXISTS $TABLE{PCOORD_STATIC}_bak");
	    $dbh->do("RENAME TABLE IF EXISTS $TABLE{PCOORD_STATIC} to $TABLE{PCOORD_STATIC}_bak");
	}
	
	$dbh->do("CREATE TABLE IF NOT EXISTS $TABLE{PCOORD_STATIC} (
		collection_no int unsigned not null,
		model varchar(255) not null,
		present_lng decimal(9,6) not null,
		present_lat decimal(9,6) not null,
		plate_no smallint unsigned,
		PRIMARY KEY (collection_no, model)
		) Engine=MyISAM CHARSET=utf8");
    }
    
	# $dbh->do("CREATE TABLE IF NOT EXISTS $TABLE{PCOORD_DATA} (
	# 	collection_no int unsigned not null,
	# 	model varchar(255) not null,
	# 	update_time timestamp not null,
	# 	present_lng decimal(9,6),
	# 	present_lat decimal(9,6),
	# 	plate_no int unsigned,
	# 	early_age int unsigned,
	# 	early_lng decimal(5,2),
	# 	early_lat decimal(5,2),
	# 	mid_age int unsigned,
	# 	mid_lng decimal(5,2),
	# 	mid_lat decimal(5,2),
	# 	late_age int unsigned,
	# 	late_lng decimal(5,2),
	# 	late_lat decimal(5,2),
	# 	update_early boolean not null,
	# 	update_mid boolean not null,
	# 	update_late boolean not null,
	# 	PRIMARY KEY (collection_no, model),
	# 	key (plate_no)
	# 	) Engine=MyISAM");
    
    if ( $argument eq 'PCOORD_MODELS' || $argument eq 'tables' )
    {
	if ( $replace )
	{
	    logMessage(1, "Replacing table PCOORD_MODELS as '$TABLE{PCOORD_MODELS}'");
	    $dbh->do("DROP TABLE IF EXISTS $TABLE{PCOORD_MODELS}");
	}
	
	else
	{
	    logMessage(1, "Creating new table PCOORD_MODELS as '$TABLE{PCOORD_MODELS}'");
	    $dbh->do("DROP TABLE IF EXISTS $TABLE{PCOORD_MODELS}_bak");
	    $dbh->do("RENAME TABLE IF EXISTS $TABLE{PCOORD_MODELS} to $TABLE{PCOORD_MODELS}_bak");
	}
	
	$dbh->do("CREATE TABLE IF NOT EXISTS $TABLE{PCOORD_MODELS} (
		name varchar(255) not null PRIMARY KEY,
		model_id int unsigned not null,
		min_age smallint unsigned not null default '0',
		max_age smallint unsigned not null default '0',
		update_time timestamp not null default current_timestamp()
		) Engine=MyISAM CHARSET=utf8");
    }
    
    if ( $argument eq 'PCOORD_PLATES' || $argument eq 'tables' )
    {
	if ( $replace )
	{
	    logMessage(1, "Replacing table PCOORD_PLATES as '$TABLE{PCOORD_PLATES}'");
	    $dbh->do("DROP TABLE IF EXISTS $TABLE{PCOORD_PLATES}");
	}
	
	else
	{
	    logMessage(1, "Creating new table PCOORD_PLATES as '$TABLE{PCOORD_PLATES}'");
	    $dbh->do("DROP TABLE IF EXISTS $TABLE{PCOORD_PLATES}_bak");
	    $dbh->do("RENAME TABLE IF EXISTS $TABLE{PCOORD_PLATES} to $TABLE{PCOORD_PLATES}_bak");
	}
    
	$dbh->do("CREATE TABLE IF NOT EXISTS $TABLE{PCOORD_PLATES} (
		model varchar(255) not null,
		plate_no int unsigned,
		name varchar(255) not null default '',
		min_age int unsigned null,
		max_age int unsigned null,
		update_time timestamp not null default current_timestamp(),
		primary key (model, plate_no)
		) Engine=MyISAM CHARSET=utf8");
    }
    
    my $a = 1;	# we can stop here when debugging
}


sub initOneTable {
    
    my ($self, $table_specifier, $replace, $create_stmt) = @_;
    
    my $table_name = $TABLE{$table_specifier};
    
    if ( $replace )
    {
	logMessage(1, "Replacing table $table_specifier as '$table_name'");
	$dbh->do("DROP TABLE IF EXISTS $table_name");
    }
    
    else
    {
	logMessage(1, "Creating new table $table_specifier as '$table_name'");
	$dbh->do("DROP TABLE IF EXISTS ${table_name}_bak");
	$dbh->do("RENAME TABLE IF EXISTS $table_name to ${table_name}_bak");
    }
    
    return $dbh->do($create_stmt);
}


# initializeModels ( dbh, argument, replace )
# 
# Initialize the PCOORD_MODELS table.

sub initializeModels {
    
    # If this is called as a method, ignore the first parameter.
    
    shift if $_[0] eq __PACKAGE__ or ref $_[0] eq __PACKAGE__;
    
    my ($dbh, $argument, $replace) = @_;
    
    logMessage(1, "Initializing models");
    
    my $model_uri = configData('paleocoord_model_uri') ||
	die "You must specify 'paleocoord_model_uri' in config.yml";
    
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database Updater/0.1");
    
    my $req = HTTP::Request->new(GET => $model_uri);
    
    my $response = $ua->request($req);
    my $content = $response->content_ref;
    my $code = $response->code;
    my $data;
    
    die "Request failed with code $code: $model_uri" unless $response->is_success;
    
    eval {
	$data = decode_json($$content);
    };
    
    die "Bad JSON response from service: $model_uri" unless $data;
    
    my $insert_count = 0;
    
    if ( ref $data eq 'ARRAY' )
    {
	if ( $replace )
	{
	    my $result = $dbh->do("DELETE FROM $TABLE{PCOORD_MODELS}");
	}
	
	foreach my $entry ( $data->@* )
	{
	    if ( $entry->{id} && $entry->{name} && $entry->{max_age} >= 200 )
	    {
		my $quoted_id = $dbh->quote($entry->{id});
		my $quoted_name = $dbh->quote($entry->{name});
		my $quoted_max = $dbh->quote($entry->{max_age});
		my $quoted_min = $dbh->quote($entry->{min_age});
		
		my $sql = "REPLACE INTO $TABLE{PCOORD_MODELS}
			(name, model_id, min_age, max_age)
			VALUES ($quoted_name, $quoted_id, $quoted_min, $quoted_max)";
		
		my $result = $dbh->do($sql);
		$insert_count++;
	    }
	}
	
	logMessage(1, "Inserted $insert_count models into '$TABLE{PCOORD_MODELS}'");
    }
    
    else
    {
	die "Response from service was not a list: $model_uri\n";
    }
}


sub initializePlates {
    
    # If this is called as a method, ignore the first parameter.
    
    shift if $_[0] eq __PACKAGE__ or ref $_[0] eq __PACKAGE__;
    
    my ($dbh, $argument, $replace) = @_;
    
    my $plate_uri = configData('paleocoord_plate_uri') ||
	die "You must specify 'paleocoord_plate_uri' in config.yml";
    
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database Updater/0.1");
    
    my $model_list = $dbh->selectcol_arrayref("SELECT name FROM $TABLE{PCOORD_MODELS}");
    
    unless ( ref $model_list eq 'ARRAY' && $model_list->@* )
    {
	die "You must initialize the models first";
    }
    
  MODEL:
    foreach my $model ( $model_list->@* )
    {
	logMessage(1, "Loading plates for model '$model'");
	
	my $request_uri = "$plate_uri?model=$model";
	
	my $req = HTTP::Request->new(GET => $request_uri);
	
	my $response = $ua->request($req);
	my $content = $response->content_ref;
	my $code = $response->code;
	my $data;
	
	unless ( $response->is_success )
	{
	    print STDERR "  Request failed with code '$code': $request_uri";
	    next MODEL;
	}
	
	eval {
	    $data = decode_json($$content);
	};
	
	unless ( $data )
	{
	    print STDERR "Bad JSON response from service: $request_uri";
	    next MODEL;
	}
	
	my $insert_count = 0;
	
	if ( ref $data eq 'ARRAY' )
	{
	    if ( $replace )
	    {
		$dbh->do("DELETE FROM $TABLE{PCOORD_PLATES} WHERE model='$model'");
	    }
	    
	  ENTRY:
	    foreach my $entry ( $data->@* )
	    {
		my $properties = $entry->{properties};
		
		my $plate_id = $properties->{plate_id};
		my $name = $properties->{name} || '';
		my $min_age = $properties->{young_lim};
		my $max_age = $properties->{old_lim};
		
		next ENTRY unless $plate_id;
		
		my $quoted_id = $dbh->quote($plate_id);
		my $quoted_name = $dbh->quote($name);
		my $quoted_min = $dbh->quote($min_age);
		my $quoted_max = $dbh->quote($max_age);
		
		my $sql = "REPLACE INTO $TABLE{PCOORD_PLATES} " .
		    "(model, plate_no, name, min_age, max_age)" .
		    "VALUES ('$model', $quoted_id, $quoted_name, $quoted_min, $quoted_max)";
		
		my $result = $dbh->do($sql);
		$insert_count++;
	    }
	    
	    logMessage(1, "Inserted $insert_count plates into '$TABLE{PCOORD_PLATES}' for model '$model'");
	}
	
	else
	{
	    print STDERR "Response from service was not a list: $request_uri\n";
	    next MODEL;
	}
    }
    
    logMessage(1, "Done loading plate data.");
}

1;


=head1 NAME

PaleoCoords - set paleocoordinates for collections in the Paleobiology Database

=head1 SYNOPSIS

    use PaleoCoords qw(ensureTables updatePaleocoords readPlateData);
    
    updatePaleocoords($dbh, $options);

=head1 DESCRIPTION

GPlates computes paleocoordinates by making requests to the GPlates server at
http://gplates.gps.caltech.edu/.  The particular service URL is read from the
paleobiodb configuration file (typically "config.yml") as indicated below.
These paleocoordinates are determined based on the present location of the
collection (longitude and latitude) in conjunction with the beginning, middle
and end points of the collection's stated age range.

In the default mode of operation, any collections which have been added to the
database or have had their location or age range modified will have their
paleocoordinates updated.  This behavior may be modified by specifying one or
more of the options indicated below.

=head1 CONFIGURATION

The following configuration directives are read from the application
configuration file (typically "config.yml"):

=over

=item paleocoord_uri

The value of this directive should be the URL of the GPlates service for
computing paleocoordinates.  This module is designed to work with the
service which at the time of this writing is associated with the URL
"http://gplates.gps.caltech.edu/reconstruct_feature_collection/".

If this directive is not found in the configuration file, a fatal error will
result. 

=item paleocoord_max_age

The value of this directive should be the maximum age for which the GPlates
service will return valid results.  Any collection age range points (early,
middle and/or end) exceeding this threshold will not have paleocoordinates
computed for them.

This directive must be either specified in the configuration file or via the
command line (if the script "paleocoord_update.pl" is used) or else a fatal error
will result.

=item paleocoord_retry_limit

The value of this directive should be the number of times to retry a request
to the GPlates server if the request times out.  If not specified, it defaults
to 3.  A request that returns an HTTP error code will not be retried.

=item paleocoord_retry_interval

The value of this directive should be the number of seconds to wait before
retrying a timed-out request.  This interval will be doubled before each
successive try is made.  If not specified, it defaults to 5.

=item paleocoord_fail_limit

The value of this directive should be the number of request failures that are
tolerated before the update process is aborted.  If not specified, it defaults
to 3.

=item paleocoord_feature_limit

The value of this directive should be the maximum number of features (points) to be included in a
single request to the GPlates service.  If not specified, it defaults to 35.  This is necessary
because the service does not work properly if too many points are included in one request.  As
many requests as necessary will be made so as to update all paleocoordinates that need updating.

=back

=head1 SUBROUTINES

The following subroutines are exported on request:

=head2 updatePaleocoords ( $dbh, $options )

This subroutine updates paleocoordinates in the database, making calls to the
GPlates server in order to obtain the proper values.  The default mode of
operation is to update the coordinates for all collections with a valid
latitude and longitude whose age range is less than the specified maximum age.
All collections will get three sets of palecoordinates, corresponding to the
beginning, middle and end of their age range.  However, age points that fall
outside of the specified age range (see L</max_age> and L</min_age> below) will
not be updated.

The first argument to this subroutine must be a valid DBI database handle.
The second, if given, must be a hash of option values.  Valid options include:

=over

=item update_all

If this option is given a true value, then all coordinates within the
specified age range will be recomputed.

=item clear_all

If this option is given a true value, then all coordinates within the
specified age range will be cleared.  No further action will be taken.

=item min_age

This option specifies the minimum end of the age range to update (in millions
of years ago, or "Ma").  If not given, it defaults to 0.

=item max_age

This option specifies the maximum end of hte age range to update (in millions
of years ago, or "Ma").  If not given, it defaults to the value of the
configuration directive L</paleocoord_max_age>.  If this directive is not found
in the configuration file, a fatal error will be thrown.

=back

=head2 ensureTables ( $dbh, $force )

This subroutine makes sure that the tables used to hold paleocoordinates exist
in the database.  The first argument must be a valid DBI database handle.

If the second argument is true, then the existing tables (if any) will be
dropped and empty ones recreated in their place.  This will, of course, delete
all paleocoordinate information.  A subsequent call to L</updatePaleocoords>
and much traffic to the GPlates server will be required in order to
reconstitute this information.

=head2 readPlateData ( $dbh )

This subroutine will read lines of text from standard input and use them to
populate the table $GEOPLATES (see TableDefs.pm).

The input must be either a geojson feature collection whose features have the
properties C<PLATE_ID> and C<NAME>, or a sequence of plain text lines
containing a 3-digit plate ID followed by an alphanumeric plate abbreviation
followed by a plate description.  These three must be separated by whitespace
and the plate description must start with an alphabetic character.

