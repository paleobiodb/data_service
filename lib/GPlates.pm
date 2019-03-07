# 
# The Paleobiology Database
# 
#   GPlates.pm
# 
# This module is responsible for updating the paleocoordinates of collections
# in the Paleobiology Database, by querying the GPlates service at
# caltech.edu.
# 
# Author: Michael McClennen
# 

package GPlates;

use strict;

use base 'Exporter';

our (@EXPORT_OK) = qw(updatePaleocoords ensureTables readPlateData);

use Carp qw(carp croak);
use Try::Tiny;
use JSON;
use LWP::UserAgent;

use TableDefs qw($PALEOCOORDS $GEOPLATES $COLLECTIONS $COLL_MATRIX $INTERVAL_DATA);
use ConsoleLog qw(logMessage);
use CoreFunction qw(loadConfig configData);


our ($GEOPLATES_WORK) = 'gpn';

our ($DEFAULT_RETRY_LIMIT) = 3;
our ($DEFAULT_RETRY_INTERVAL) = 5;
our ($DEFAULT_FAIL_LIMIT) = 3;

our ($DEFAULT_MAX_FEATURES) = 35;	# This value can be adjusted if necessary; it ensures
                                        # that the length of each request URL won't exceed the
                                        # server's limit.  Since we don't actually know what that
                                        # limit is, we are conservative.


# updatePaleocoords ( dbh, options )
# 
# Update all or some of the paleocoordinates in the database, using the
# GPlates service.

sub updatePaleocoords {
    
    my ($dbh, $options) = @_;
    
    $options ||= {};
    
    # We start by loading the relevant configuration settings from the
    # paleobiology database configuratio nfile.
    
    loadConfig();
    
    my $gplates_uri = configData('gplates_uri');
    my $config_max_age = configData('gplates_max_age');
    my $retry_limit = configData('gplates_retry_limit') || $DEFAULT_RETRY_LIMIT;
    my $retry_interval = configData('gplates_retry_interval') || $DEFAULT_RETRY_INTERVAL;
    my $fail_limit = configData('gplates_fail_limit') || $DEFAULT_FAIL_LIMIT;
    my $max_features = configData('gplates_feature_limit') || $DEFAULT_MAX_FEATURES;
    
    die "You must specify the GPlates URI in the configuration file, as 'gplates_uri'\n"
	unless $gplates_uri;
    
    # Then process any command-line options.
    
    my $min_age = $options->{min_age} + 0 if defined $options->{min_age} && $options->{min_age} > 0;
    my $max_age = $options->{max_age} + 0 if defined $options->{max_age} && $options->{max_age} > 0;
    
    $min_age ||= 0;
    $max_age ||= $config_max_age;
    
    die "You must specify the GPlates maximum age in the configuration file as 'gplates_max_age', or on the command line.\n"
	unless $max_age;
    
    # We then create a control object to manage this process.
    
    my $self = { dbh => $dbh,
		 source_points => {},
		 update_count => 0,
		 debug => $options->{debug},
		 fail_count => 0,
		 debug_count => 0,
		 min_age_bound => $min_age,
		 max_age_bound => $max_age,
		 service_uri => $gplates_uri,
		 retry_limit => $retry_limit,
		 retry_interval => $retry_interval,
		 fail_limit => $fail_limit,
		 max_features => $max_features,
		 quiet => $options->{quiet},
		 verbose => $options->{verbose},
	       };
    
    bless $self, 'GPlates';
    
    my ($sql, $result, $count, @filters);
    
    # We start by making sure that we have the proper tables.
    
    ensureTables($dbh);
    
    # logMessage(1, "Updating paleocoordinates") unless $self->{quiet};

    # If other filters were specified, compute them now.

    my $filters = '';
    
    if ( $options->{collection_no} )
    {
	$filters .= " and collection_no in ($options->{collection_no})";
    }
    
    # If the option 'clear_all' was specified, then we clear the $PALEOCOORDS table of entries
    # falling into the given age range.  No further action should be taken in this case.
    
    if ( $options->{clear_all} )
    {
	$self->initMessage($options);
	logMessage(2, "    clearing all paleocoords between $max_age Ma and $min_age Ma...");
	
	$sql = "UPDATE $PALEOCOORDS
		SET early_lng = null, early_lat = null
		WHERE early_age between $min_age and $max_age $filters";
	
	$count = $dbh->do($sql);
	
	$sql = "UPDATE $PALEOCOORDS
		SET mid_lng = null, mid_lat = null
		WHERE mid_age between $min_age and $max_age $filters";
	
	$count += $dbh->do($sql);
	
	$sql = "UPDATE $PALEOCOORDS
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
	logMessage(2, "    updating all paleocoords between $max_age Ma and $min_age Ma...");
	
	$sql = "UPDATE $PALEOCOORDS
		SET update_early = true
		WHERE early_age between $min_age and $max_age $filters";
	
	$dbh->do($sql);
	
	$sql = "UPDATE $PALEOCOORDS
		SET update_mid = true
		WHERE mid_age between $min_age and $max_age $filters";
	
	$dbh->do($sql);
	
	$sql = "UPDATE $PALEOCOORDS
		SET update_late = true
		WHERE late_age between $min_age and $max_age $filters";
	
	$dbh->do($sql);
	
	$sql = "SELECT count(*) FROM $PALEOCOORDS
		WHERE update_early or update_mid or update_late";
	
	my ($count) = $dbh->selectrow_array($sql);
	
	logMessage(2, "      flagging $count coordinates to update");
    }

    # Otherwise, we check for all coordinates within the specified age range
    # that need updating.

    else
    {
	# Lock the tables $PALEOCOORDS and $COLL_MATRIX, to avoid deadlock with other processes.

	$sql = "LOCK TABLES $PALEOCOORDS as p WRITE, $COLL_MATRIX as c READ";

	$count = $dbh->do($sql);
	
	# Now delete any rows in $PALEOCOORDS corresponding to collections whose
	# coordinates have been made invalid.  This should not happen very often,
	# but is a boundary case that we need to take care of.
	
	$sql = "DELETE FROM p
		USING $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
		WHERE c.lat is null or c.lat not between -90 and 90 or
		      c.lng is null or c.lng not between -180 and 180";
	
	$count = $dbh->do($sql);
	
	if ( defined $count && $count > 0 )
	{
	    $self->initMessage($options);
	    logMessage(2, "    cleared paleocoords from $count collections without a valid location");
	}
	
	# Then delete any rows in $PALEOCOORDS corresponding to collections whose
	# coordinates have been edited.  These will have to be recomputed entirely.
	
	$sql = "DELETE FROM p
                USING $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
                WHERE c.lat <> p.present_lat or c.lng <> p.present_lng";
	
	$count = $dbh->do($sql);
	
	# # Then set the update flags on any rows in $PALEOCOORDS corresponding to collections whose
	# # coordinates have been edited.  These will have to be recomputed entirely.
	
	# $sql =     "UPDATE $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
	# 		SET p.update_early = true, p.update_mid = true, p.update_late = true
	# 		WHERE c.lat <> p.present_lat or c.lng <> p.present_lng";
	
	# $count = $dbh->do($sql);
	
	if ( defined $count && $count > 0 )
	{
	    $self->initMessage($options);
	    logMessage(2, "    cleared paleocoords from $count collections whose location has been changed");
	}
	
	# Then clear any entries in $PALEOCOORDS corresponding to collections whose ages have been
	# changed.
	
	$sql = "UPDATE $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
		SET p.early_age = null, p.early_lng = null, p.early_lat = null, p.update_early = 1
		WHERE round(c.early_age,0) <> p.early_age and
		      (round(c.early_age,0) between $min_age and $max_age or p.early_age is not null)";
	
	$count = $dbh->do($sql);
	
	$sql = "UPDATE $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
		SET p.late_age = null, p.late_lng = null, p.late_lat = null, p.update_late = 1
		WHERE round(c.late_age,0) <> p.late_age and
		      (round(c.late_age,0) between $min_age and $max_age or p.late_age is not null)";
	
	$count += $dbh->do($sql);
	
	$sql = "UPDATE $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
		SET p.mid_age = null, p.mid_lng = null, p.mid_lat = null, p.update_mid = 1
		WHERE round((c.early_age + c.late_age)/2,0) <> p.mid_age and
		      (round((c.early_age + c.late_age)/2,0) between $min_age and $max_age or 
		       p.mid_age is not null)";
	
	$count += $dbh->do($sql);
	
	if ( defined $count && $count > 0 )
	{
	    $self->initMessage($options);
	    logMessage(2, "    cleared $count entries whose ages did not correspond to their collections");
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
    		FROM $COLL_MATRIX as c LEFT JOIN $PALEOCOORDS as p using (collection_no)
    		WHERE c.lat between -90.0 and 90.0 and c.lng between -180.0 and 180.0
			and round(c.early_age,0) between $min_age and $max_age
			and (p.present_lng is null or p.update_early) $filters";
    
    my $early_updates = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    $sql =     "SELECT collection_no, c.lng as present_lng, c.lat as present_lat,
		       'mid' as selector, round((c.early_age + c.late_age)/2,0) as age
    		FROM $COLL_MATRIX as c LEFT JOIN $PALEOCOORDS as p using (collection_no)
    		WHERE c.lat between -90.0 and 90.0 and c.lng between -180.0 and 180.0
			and round((c.early_age + c.late_age)/2,0) between $min_age and $max_age
			and (p.present_lng is null or p.update_mid) $filters";
    
    my $mid_updates = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    $sql =     "SELECT collection_no, c.lng as present_lng, c.lat as present_lat,
		       'late' as selector, round(c.late_age,0) as age
    		FROM $COLL_MATRIX as c LEFT JOIN $PALEOCOORDS as p using (collection_no)
    		WHERE c.lat between -90.0 and 90.0 and c.lng between -180.0 and 180.0
			and round(c.late_age,0) between $min_age and $max_age
			and (p.present_lng is null or p.update_late) $filters";
    
    my $late_updates = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    $count = 0;
    
    foreach my $record ( @$early_updates, @$mid_updates, @$late_updates )
    {
	my $collection_no = $record->{collection_no};
	my $lng = $record->{present_lng};
	my $lat = $record->{present_lat};
	my $selector = $record->{selector};
	my $age = $record->{age};
	
	push @{$self->{source_points}{$age}}, [$collection_no, $selector, $lng, $lat];
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
    
    # At this point, we need to prepare the SQL statements that will be used
    # to update entries in the table.
    
    $self->prepareStatements();
    
    # Then we must step through the keys of $self->{source_points} one by one.
    # These keys are ages (in Ma), and we need to generate a GPlates rotation
    # query for each separate age.  In order to identify each point, we create
    # a feature name using the collection_no field in conjunction with the
    # selector value of 'early', 'mid' or 'late' to identify which
    # paleocoordinate age we are computing.
    
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database Updater/0.1");
    
    # $DB::single = 1;
    
 AGE:
    foreach my $age (sort { $a <=> $b } keys %{$self->{source_points}})
    {
	# If we have received age bounds from the service, then ignore any age
	# that falls outside them.
	
	next AGE if defined $self->{min_age_bound} && $age < $self->{min_age_bound};
	next AGE if defined $self->{max_age_bound} && $age > $self->{max_age_bound};
	
	# Grab the set of points that need to be rotated to this age.
	
	my @points = @{$self->{source_points}{$age}};
	
	# Now create as many requests as are necessary to rotate all of these
	# points. 
	
	while ( @points )
	{
	    # my $request_json = "geologicage=$age&output=geojson&feature_collection={\"type\": \"FeatureCollection\",";
	    # $request_json .= "\"features\": [";
	    # my $comma = '';
	    # my @oid_list;
	    
	    # Start building a parameter list.
	    
	    my $request_params = "points=";
	    my $sep = '';
	    my @oid_list;
	    
	    # Add each point, up to the limit for a single request.
	    
	FEATURE:
	    while ( my $point = shift @points )
	    {
		my ($coll_no, $selector, $lng, $lat) = @$point;
		my $oid = "$selector.$coll_no";
		
		next unless $lng ne '' && $lat ne '';	# skip any point with null coordinates.
		
		# $request_json .= $comma; $comma = ",";
		# $request_json .= $self->generateFeature($lng, $lat, $oid);
		
		$request_params .= $sep; $sep = ' ';
		$request_params .= "$lng,$lat,$oid";
		
		push @oid_list, $oid;
		
		last FEATURE if @oid_list >= $self->{max_features};
	    }
	    
	    # $request_json .= "]}";
	    
	    $request_params .= "&age=$age";
	    
	    # Now if we have at least one point to rotate then fire off the
	    # request and process the answer (if any)
	    
	    my $count = scalar(@oid_list);
	    my $oid_string = join(',', @oid_list);
	    
	    next AGE unless $count;
	    
	    logMessage(2, "    rotating $count points to $age Ma ($oid_string)");
	    
	    # $self->makeGPlatesRequest($ua, \$request_json, $age);
	    
	    $self->makeGPlatesRequest($ua, $request_params, $age);
	    
	    # If we have gotten too many server failures in a row, then abort this
	    # run.
	    
	    if ( $self->{fail_count} >= $self->{fail_limit} )
	    {
		logMessage(2, "    ABORTING RUN DUE TO REPEATED QUERY FAILURE");
		last;
	    }
	}
    }
    
    logMessage(2, "    updated $self->{update_count} paleocoordinate entries");
    
    my $a = 1; # we can stop here when debugging
}


# The following routines are internally used methods, and are not exported:
# =========================================================================

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
    
    $sql = "INSERT IGNORE INTO $PALEOCOORDS (collection_no, present_lng, present_lat)
	    SELECT collection_no, lng, lat FROM $COLL_MATRIX
	    WHERE collection_no = ?";
    
    $self->{add_row_sth} = $dbh->prepare($sql);
    
    $sql = "UPDATE $PALEOCOORDS
	    SET early_age = ?, early_lng = ?, early_lat = ?, 
		update_early = 0
	    WHERE collection_no = ? LIMIT 1";
    
    $self->{early_sth} = $dbh->prepare($sql);
    
    $sql = "UPDATE $PALEOCOORDS
	    SET mid_age = ?, mid_lng = ?, mid_lat = ?, 
		update_mid = 0
	    WHERE collection_no = ? LIMIT 1";
    
    $self->{mid_sth} = $dbh->prepare($sql);
    
    $sql = "UPDATE $PALEOCOORDS
	    SET late_age = ?, late_lng = ?, late_lat = ?, 
		update_late = 0
	    WHERE collection_no = ? LIMIT 1";
    
    $self->{late_sth} = $dbh->prepare($sql);

    $sql = "UPDATE $PALEOCOORDS
	    SET plate_no = ?, early_plate = ?, late_plate = ?
	    WHERE collection_no = ? LIMIT 1";
    
    $self->{set_plate} = $dbh->prepare($sql);
    
    # $sql = "UPDATE $PALEOCOORDS
    # 	    SET late_plate = ?
    # 	    WHERE collection_no = ? LIMIT 1";

    # $self->{late_plate_sth} = $dbh->prepare($sql);
    
    $sql = "UPDATE $COLL_MATRIX as c JOIN $PALEOCOORDS as pc using (collection_no)
	    SET c.g_plate_no = pc.plate_no
	    WHERE collection_no = ?";
	
    $self->{coll_matrix_sth} = $dbh->prepare($sql);
    
    return;
}


sub generateFeature {
    
    my ($self, $lng, $lat, $oid) = @_;
    
    my $output = "{\"type\": \"Feature\",";
    $output .= "\"geometry\": {\"type\": \"Point\", \"coordinates\": [$lng, $lat]},";
    $output .= "\"properties\": {\"name\": \"$oid\", \"feature_type\": \"gpml:UnclassifiedFeature\", ";
    $output .= "\"begin_age\": \"4000.0\", \"end_age\": \"0.0\"}";
    $output .= "}";
    
    return $output;
}


sub makeGPlatesRequest {

    # my ($self, $ua, $request_ref, $age) = @_;
    
    my ($self, $ua, $request_params, $age) = @_;
    
    # Generate a GPlates request.  The actual request is wrapped inside a
    # while loop so that we can retry it if something goes wrong.
    
    my $uri = $self->{service_uri};
    
    # my @headers = ( 'Content-Type' => 'application/x-www-form-urlencoded',
    # 		    'Content-Length' => length($$request_ref) );
    
    # my $req = HTTP::Request->new(POST => $uri, \@headers, $$request_ref);

    my $req = HTTP::Request->new(GET => "$uri?$request_params");
    
    my ($resp, $content_ref);
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
	
	# If the request succeeds, enter the resulting coordinates into
	# the database, reset $fail_count, and return.
	
	if ( $resp->is_success )
	{
	    $time1 = time;
	    my $elapsed = $time1 - $start;
	    
	    logMessage(3, "    response elapsed time: $elapsed");
	    
	    $self->processResponse($age, $content_ref);
	    $self->{fail_count} = 0;

	    $elapsed = time - $time1;
	    
	    logMessage(3, "    process elapsed time: $elapsed");
	    
	    return;
	}
	
	# Otherwise, check the initial part of the response message body.  If
	# the server didn't give us any response, wait a few seconds and try
	# again.
	
	my $content_start = substr($$content_ref, 0, 1000);
	
	if ( $content_start =~ /server closed connection/i )
	{
	    $retry_count--;
	    logMessage(2, "      SERVER CLOSED CONNECTION, RETRYING...") if $retry_count > 0;
	    sleep($retry_interval);
	    $retry_interval *= 2;
	    next RETRY;
	}
	
	# Otherwise, the request failed for some other reason and should not
	# be retried.  If the option $debug is true, write the response
	# content to an error file.
	
	my $code = $resp->code;
	logMessage(2, "      REQUEST FAILED WITH CODE '$code'");
	$self->{fail_count}++;
	
	if ( $self->{debug} )
	{
	    $self->{debug_count}++;
	    open(OUTFILE, ">gpfail.$self->{debug_count}.html");
	    print OUTFILE $resp->content;
	    close OUTFILE;
	    logMessage(2, "      DEBUG FILE 'gpfail.$self->{debug_count}.html' created");
	}
	
	return;
    }
    
    # If we get here, then we have exceeded the retry count.
    
    logMessage(2, "      SERVER CLOSED CONNECTION, ABORTING REQUEST");
    $self->{fail_count}++;
    return;
}


my %is_selector = ( 'early' => 1, 'mid' => 1, 'late' => 1 );


sub processResponse {
    
    my ($self, $age, $content_ref) = @_;

    my $dbh = $self->{dbh};
    my $response;
    my @bad_list;
    
    # Decode the response, trapping any errors that may occur.
    
    try {
	$response = decode_json($$content_ref);
    }
    
    catch {
	logMessage(2, "ERROR: bad json from server");
        $self->{fail_count}++;
	return;
    };
    
    # Check for an error
    
    if ( $response->{error} )
    {
	logMessage(2, "ERROR: $response->{error}");
	$self->{fail_count}++;
	return;
    }
    
    # For each feature (i.e. result point) in the response, update the
    # corresponding entry in the database.

    my @result; @result = @{$response->{result}} if ref $response->{result} eq 'ARRAY';
    my $errmsg_displayed;
    
  POINT:
    foreach my $featcoll ( @result )
    {
	unless ( ref $featcoll->{features} eq 'ARRAY' &&
		 ref $featcoll->{features}[0] eq 'HASH' &&
		 defined $featcoll->{features}[0]{properties}{label} )
	{
	    logMessage(1, "ERROR: found a feature collection with no 'label' property")
		unless $errmsg_displayed;
	    $errmsg_displayed = 1;
	    next;
	}
	
	my $feature = $featcoll->{features}[0];
	my $key = $feature->{properties}{label};
	my ($selector, $collection_no) = split(qr{\.}, $key);
	
	unless ( defined $selector && $is_selector{$selector} && defined $collection_no && $collection_no > 0 )
	{
	    push @bad_list, $key;
	    next POINT;
	}
	
	unless ( ref $feature->{geometry}{coordinates} eq 'ARRAY' )
	{
	    $self->updateOneEntry($collection_no, $selector, $age, undef, undef, undef);
	}
	
	else
	{
	    my ($lng, $lat) = @{$feature->{geometry}{coordinates}};
	    
	    unless ( $lng =~ qr{ ^ -? \d+ (?: \. \d* )? (?: E -? \d+ )? $ }xi and
		     $lat =~ qr{ ^ -? \d+ (?: \. \d* )? (?: E -? \d+ )? $ }xi )
	    {
		push @bad_list, "$key ($lng, $lat)";
		$self->updateOneEntry($collection_no, $selector, $age, undef, undef, undef);
		next POINT;
	    }
	    
	    my $plate_id = $feature->{properties}{plate_id};
	    $plate_id =~ s/[.]0$//;
	    
	    $plate_id = undef if $plate_id eq 'NULL';
	    
	    # my $quoted = defined $plate_id && $plate_id ne '' && $plate_id !~ /null/i ?
	    # 	$dbh->quote($plate_id) : undef;
	    
	    $self->updateOneEntry($collection_no, $selector, $age, $lng, $lat, $plate_id);
	}
    }
    
    if ( @bad_list )
    {
	my $count = scalar(@bad_list);
	my $list = join(', ', @bad_list);
	
	logMessage(1, "ERROR: the following $count entries were not updated because the GPlates response was invalid:");
	logMessage(1, "$list");
    }
}


sub updateOneEntry {
    
    my ($self, $collection_no, $selector, $age, $lng, $lat, $plate_id) = @_;
    
    my $dbh = $self->{dbh};
    
    $self->{add_row_sth}->execute($collection_no);
    
    if ( $selector eq 'early' )
    {
	$self->{early_sth}->execute($age, $lng, $lat, $collection_no);
    }
    
    elsif ( $selector eq 'mid' )
    {
	$self->{mid_sth}->execute($age, $lng, $lat, $collection_no);
    }
    
    elsif ( $selector eq 'late' )
    {
	$self->{late_sth}->execute($age, $lng, $lat, $collection_no);
    }
    
    $self->{set_plate}->execute($plate_id, $plate_id, $plate_id, $collection_no);
    
    # if ( $age > 200 )
    # {
	# $self->{early_plate_sth}->execute($plate_id, $collection_no);
    # }

    # else
    # {
	# $self->{late_plate_sth}->execute($plate_id, $collection_no);
    # }
    
    $self->{coll_matrix_sth}->execute($collection_no);
    
    $self->{update_count}++;
}

# ==============
# end of methods


# ensureTables ( dbh, force )
# 
# Make sure that necessary tables are present.  If $force is true, then drop
# the old tables first.

sub ensureTables {
    
    # If this is called as a method, ignore the first parameter.
    
    shift if $_[0] eq 'GPlates' or ref $_[0] eq 'GPlates';
    
    my ($dbh, $force) = @_;
    
    # If the $force parameter is true, then drop existing tables so that fresh
    # ones can be created.
    
    if ( $force )
    {
	$dbh->do("DROP TABLE IF EXISTS $PALEOCOORDS");
	$dbh->do("DROP TABLE IF EXISTS $GEOPLATES");
    }
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $PALEOCOORDS (
		collection_no int unsigned primary key,
		update_time timestamp not null,
		present_lng decimal(9,6),
		present_lat decimal(9,6),
		plate_no int unsigned,
		early_plate varchar(20),
	        late_plate varchar(20),
		early_age int unsigned,
		early_lng decimal(5,2),
		early_lat decimal(5,2),
		mid_age int unsigned,
		mid_lng decimal(5,2),
		mid_lat decimal(5,2),
		late_age int unsigned,
		late_lng decimal(5,2),
		late_lat decimal(5,2),
		update_early boolean not null,
		update_mid boolean not null,
		update_late boolean not null,
		key (plate_no)) Engine=MyISAM CHARACTER SET utf8 COLLATE utf8_unicode_ci");
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $GEOPLATES (
		plate_no int unsigned primary key,
		abbrev varchar(10) not null,
		name varchar(255) not null,
		key (abbrev),
		key (name)) Engine=MyISAM CHARACTER SET utf8 COLLATE utf8_unicode_ci");
    
    my $a = 1;	# we can stop here when debugging
}


# readPlateData ( dbh )
# 
# Read a data file in JSON format from standard input and parse plate ids and
# names from it.  Replace the contents of $GEOPLATES from this data.

sub readPlateData {

    my ($dbh) = @_;
    
    ensureTables($dbh);
    
    # Prepare an SQL statement to insert property names.
    
    my $insert_sth = $dbh->prepare("
		REPLACE INTO $GEOPLATES (plate_no, abbrev, name)
		VALUES (?, ?, ?)");
    
    # Read data line-by-line from standard input.
    
    my $buffer = "";
    my $plate_count = 0;
    my %found_plate;	
    
    while (<>)
    {
	$buffer .= $_;
    }
    
    # If $buffer starts with '{', then assume it contains geojson.
    
    if ( $buffer =~ /^\s*\{/ )
    {
	my $result = decode_json($buffer);
	
	# Then scan the list of features looking for property lists.  We ignore
	# plates that we have seen once already, because many of the plates are
	# mentioned multiple times.
	
	my $features = $result->{features};
	
	unless ( defined $features )
	{
	    logMessage(1, "    NO FEATURES FOUND: ABORTING");
	    return;
	}
	
	foreach my $f ( @$features )
	{
	    my $prop = $f->{properties};
	    
	    my $id = $prop->{PLATE_ID};
	    my $name = $prop->{NAME};
	    
	    next unless $id ne '' && $name ne '';
	    next if $found_plate{$id};
	    
	    $insert_sth->execute($id, '', $name);
	    $found_plate{$id} = 1;
	}
    }
    
    # Otherwise, assume it is plain text.
    
    else
    {
	while ( $buffer =~ / (\d\d\d) \s+ ([A-Z0-9]+) \s+ ([a-zA-Z][^\r\n]+) /xsg )
	{
	    my $plate_id = $1;
	    my $plate_abbrev = $2;
	    my $plate_name = $3;
	    
	    $insert_sth->execute($plate_id, $plate_abbrev, $plate_name);
	    $found_plate{$plate_id} = 1;
	}
    }
    
    my $plate_count = scalar(keys %found_plate);
    
    logMessage(2, "    found $plate_count plates");
    
    my $a = 1;	# we can stop here when debugging
}

1;


=head1 NAME

GPlates - set paleocoordinates for collections in the Paleobiology Database

=head1 SYNOPSIS

    use GPlates qw(ensureTables updatePaleocoords readPlateData);
    
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

=item gplates_uri

The value of this directive should be the URL of the GPlates service for
computing paleocoordinates.  This module is designed to work with the
service which at the time of this writing is associated with the URL
"http://gplates.gps.caltech.edu/reconstruct_feature_collection/".

If this directive is not found in the configuration file, a fatal error will
result. 

=item gplates_max_age

The value of this directive should be the maximum age for which the GPlates
service will return valid results.  Any collection age range points (early,
middle and/or end) exceeding this threshold will not have paleocoordinates
computed for them.

This directive must be either specified in the configuration file or via the
command line (if the script "gplates_update.pl" is used) or else a fatal error
will result.

=item gplates_retry_limit

The value of this directive should be the number of times to retry a request
to the GPlates server if the request times out.  If not specified, it defaults
to 3.  A request that returns an HTTP error code will not be retried.

=item gplates_retry_interval

The value of this directive should be the number of seconds to wait before
retrying a timed-out request.  This interval will be doubled before each
successive try is made.  If not specified, it defaults to 5.

=item gplates_fail_limit

The value of this directive should be the number of request failures that are
tolerated before the update process is aborted.  If not specified, it defaults
to 3.

=item gplates_feature_limit

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
configuration directive L</gplates_max_age>.  If this directive is not found
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

