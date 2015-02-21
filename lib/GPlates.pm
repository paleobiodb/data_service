# 
# The Paleobiology Database
# 
#   GPlates.pm
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
use ConsoleLog qw(initMessages logMessage);


our ($GEOPLATES_WORK) = 'gpn';
our ($PALEO_AUX) = 'pc_aux';

our ($RETRY_LIMIT) = 3;
our ($RETRY_INTERVAL) = 5;
our ($FAIL_LIMIT) = 3;


# updatePaleocoords ( dbh, options )
# 
# Update all or some of the paleocoordinates in the database, using the
# GPlates service.

sub updatePaleocoords {
    
    my ($dbh, $options) = @_;
    
    $options ||= {};
    
    # We start by creating a control object to manage this process.
    
    my $self = { dbh => $dbh,
		 new_coords => {},
		 source_points => {},
		 update_count => 0,
		 debug => $options->{debug},
		 fail_count => 0,
		 debug_count => 0,
		 max_age_bound => undef,
		 min_age_bound => undef,
	       };
    
    bless $self, 'GPlates';
    
    # Then process the other options.
    
    my $min_age = $options->{min_age} + 0 if defined $options->{min_age} && $options->{min_age} > 0;
    my $max_age = $options->{max_age} + 0 if defined $options->{max_age} && $options->{max_age} > 0;
    
    $min_age ||= 0;
    $max_age ||= 4000;
    
    $self->{min_age_bound} = $min_age;
    $self->{max_age_bound} = $max_age;
    
    my ($sql, $result, $count, @filters);
    
    $DB::single = 1;
    
    # We start by making sure that we have the proper tables.
    
    ensureTables($dbh);
    
    logMessage(1, "Updating paleocoordinates");
    
    # If the option 'update_all' or 'clear_all' was specified, then we start
    # by clearing the $PALEOCOORDS table of entries falling into the given age
    # range.
    
    if ( $options->{update_all} || $options->{clear_all} )
    {
	logMessage(2, "    clearing all paleocoords between $max_age Ma and $min_age Ma...");
	
	$sql = "UPDATE $PALEOCOORDS
		SET early_lng = null, early_lat = null, update_time = now()
		WHERE early_age between $min_age and $max_age";
	
	$count = $dbh->do($sql);
	
	$sql = "UPDATE $PALEOCOORDS
		SET mid_lng = null, mid_lat = null, update_time = now()
		WHERE mid_age between $min_age and $max_age";
	
	$count += $dbh->do($sql);
	
	$sql = "UPDATE $PALEOCOORDS
		SET late_lng = null, late_lat = null, update_time = now()
		WHERE late_age between $min_age and $max_age";
	
	$count += $dbh->do($sql);
	
	logMessage(2, "      cleared $count coordinates");
    }
    
    # If the option 'clear_all' was specified, then we are done.
    
    return if $options->{clear_all};
    
    # Now delete any rows in $PALEOCOORDS corresponding to collections whose
    # coordinates have been made invalid.  This should not happen very often,
    # but is a boundary case that we need to take care of.
    
    $sql =     "DELETE FROM p
		USING $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
		WHERE c.lat is null or c.lat not between -90 and 90 or
		      c.lng is null or c.lng not between -180 and 180";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "    cleared paleocoords from $count collections without a valid location")
	if defined $count && $count > 0;
    
    # Then delete any rows in $PALEOCOORDS corresponding to collections whose
    # coordinates have been edited.  These will have to be recomputed entirely.
    
    $sql =     "DELETE FROM p
		USING $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
		WHERE c.lat <> p.present_lat or c.lng <> p.present_lng";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "    cleared paleocoords from $count collections whose location has been changed")
	if defined $count && $count > 0;
    
    # Then clear any entries in $PALEOCOORDS corresponding to entries whose
    # ages have been changed.
    
    $sql =     "UPDATE $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
		SET p.early_age = null, p.early_lng = null, p.early_lat = null,
		    p.update_time = now()
		WHERE round(c.early_age,0) <> p.early_age and
		      (round(c.early_age,0) between $min_age and $max_age or p.early_age is not null)";
    
    $count = $dbh->do($sql);
    
    $sql =     "UPDATE $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
		SET p.late_age = null, p.late_lng = null, p.late_lat = null,
		    p.update_time = now()
		WHERE round(c.late_age,0) <> p.late_age and
		      (round(c.late_age,0) between $min_age and $max_age or p.late_age is not null)";
    
    $count += $dbh->do($sql);
    
    $sql =     "UPDATE $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
		SET p.mid_age = null, p.mid_lng = null, p.late_lng = null,
		    p.update_time = now()
		WHERE round((c.early_age + c.late_age)/2,0) <> p.mid_age and
		      (round((c.early_age + c.late_age)/2,0) between $min_age and $max_age or 
		       p.mid_age is not null)";
    
    $count += $dbh->do($sql);
    
    logMessage(2, "    cleared $count entries whose ages did not correspond to their collections")
	if defined $count && $count > 0;
    
    # $sql =     "INSERT INTO $PALEO_AUX
    # 		SELECT collection_no
    # 		FROM $COLL_MATRIX as c LEFT JOIN $PALEOCOORDS as p using (collection_no)
    # 		WHERE c.lat between -90.0 and 90.0 and c.lng between -180.0 and 180.0 and
    # 		      (c.lat <> p.present_lat or c.lng <> p.present_lng or 
    # 		       p.present_lat is null or p.present_lng is null or
    # 		       round(c.early_age,0) <> p.early_age or round(c.late_age,0) <> p.late_age)
    # 		       $age_filter";
    
    # print STDERR $sql . "\n\n" if $self->{debug};
    
    # $count = $dbh->do($sql);
    
    # logMessage(2, "    found $count collections to recompute") if $count > 0;
    
    # $sql =     "REPLACE INTO $PALEOCOORDS (collection_no, present_lng, present_lat, early_age, mid_age, late_age)
    # 		SELECT collection_no, c.lng, c.lat, 
    # 		       round(c.early_age,0), round((c.early_age + c.late_age)/2,0),
    # 		       round(c.late_age,0)
    # 		FROM $PALEO_AUX JOIN $COLL_MATRIX as c using (collection_no)";
    
    # $count = $dbh->do($sql);
    
    # If any age fields are not up-to-date, update them and clear the
    # corresponding coordinates.
    
    # $sql =     "UPDATE $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
    #  		SET p.early_age = round(c.early_age,0), early_lat = null, early_lng = null
    # 		WHERE round(c.early_age,0) <> p.early_age";
    
    # $count = $dbh->do($sql);
    
    # logMessage(2, "    updated $count early ages") if $count > 0;
    
    # $sql =     "UPDATE $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
    #  		SET p.mid_age = round((c.early_age + c.late_age)/2,0), mid_lat = null, mid_lng = null
    # 		WHERE round((c.early_age + c.late_age)/2,0) <> p.mid_age";
    
    # $count = $dbh->do($sql);
    
    # logMessage(2, "    updated $count mid ages") if $count > 0;
    
    # $sql =     "UPDATE $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
    #  		SET p.late_age = round(c.late_age,0), late_lat = null, late_lng = null
    # 		WHERE round(c.late_age,0) <> p.late_age";
    
    # $count = $dbh->do($sql);
    
    # logMessage(2, "    updated $count late ages") if $count > 0;
    
    # Next, we need to query for all records in $PALEOCOORDS that have missing
    # entries (including the ones just added).
    
    # $sql =     "SELECT p.collection_no, p.present_lng, p.present_lat,
    # 		       p.early_age, p.mid_age, p.late_age,
    # 		       early_lng, mid_lng, late_lng
    # 		FROM $PALEOCOORDS as p JOIN $COLL_MATRIX as c using (collection_no)
    # 		WHERE early_lng is null or mid_lng is null or late_lng is null
    # 		$age_filter";
    
    # If age limits were specified, then construct an SQL filter for them.
    
    my @age_filter = ();
    push @age_filter, "c.early_age >= $min_age" if defined $min_age;
    push @age_filter, "c.late_age <= $max_age" if defined $max_age;
    
    my $age_filter = '';
    $age_filter = ' and ' . join(' and ', @age_filter) if @age_filter;
    
    # Now query for all collections whose paleocoordinates need updating.
    
    logMessage(2, "    looking for collections whose palecoordinates need updating...");
    
    $sql =     "SELECT collection_no, c.lng as present_lng, c.lat as present_lat,
		       round(c.early_age,0) as early_age,
		       round((c.early_age + c.late_age)/2,0) as mid_age,
		       round(c.late_age,0) as late_age
    		FROM $COLL_MATRIX as c LEFT JOIN $PALEOCOORDS as p using (collection_no)
    		WHERE c.lat between -90.0 and 90.0 and c.lng between -180.0 and 180.0 and
    		      (c.lat <> p.present_lat or c.lng <> p.present_lng or 
    		       p.present_lat is null or p.present_lng is null or
		       p.early_lng is null or p.mid_lng is null or p.late_lng is null or
    		       round(c.early_age,0) <> p.early_age or round(c.late_age,0) <> p.late_age or
		       round((c.early_age + c.late_age)/2,0) <> p.mid_age)
    		       $age_filter";
    
    print STDERR $sql . "\n\n" if $self->{debug};
    
    my $sth = $dbh->prepare($sql);
    
    $sth->execute();
    
    $count = 0;
    
    while ( my $record = $sth->fetchrow_hashref )
    {
	my $coll_no = $record->{collection_no};
	my $lng = $record->{present_lng};
	my $lat = $record->{present_lat};
	my $early_age = $record->{early_age};
	my $mid_age = $record->{mid_age};
	my $late_age = $record->{late_age};
	
	if ( defined $early_age and $early_age >= $min_age and $early_age <= $max_age )
	{
	    push @{$self->{source_points}{$early_age}}, [$coll_no, 'early', $lng, $lat];
	    $count++;
	}
	
	if ( defined $mid_age and $mid_age >= $min_age and $mid_age <= $max_age )
	{
	    push @{$self->{source_points}{$mid_age}}, [$coll_no, 'mid', $lng, $lat];
	    $count++;
	}
	
	if ( defined $late_age and $late_age >= $min_age and $late_age <= $max_age )
	{
	    push @{$self->{source_points}{$late_age}}, [$coll_no, 'late', $lng, $lat];
	    $count++;
	}
    }
    
    logMessage(2, "    found $count entries to update");
    
    # At this point, we need to prepare the SQL statements that will be used
    # to update entries in the table.
    
    $self->prepareStatements();
    
    # Then we must step through the keys of $self->{source_points} one by one.
    # These keys are ages (in Ma), and we need to generate a GPlates rotation
    # query for each separate age.  In order to identify each point, we create
    # a decimal number using the collection_no field in conjunction with "0"
    # for "early", "1" for "mid" and "2" for "late" ages.
    
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database Updater/0.1");
    
    my %age_code = ( 'early' => 0, 'mid' => 1, 'late' => 2 );
    
 AGE:
    foreach my $age (sort { $a <=> $b } keys %{$self->{source_points}})
    {
	# If we have received age bounds from the service, then ignore any age
	# that falls outside them.
	
	next AGE if defined $self->{min_age_bound} && $age < $self->{min_age_bound};
	next AGE if defined $self->{max_age_bound} && $age > $self->{max_age_bound};
	
	# Construct an HTTP request body.
	
	my $request_json = "geologicage=$age&output=geojson&feature_collection={\"type\": \"FeatureCollection\",";
	$request_json .= "\"features\": [";
	my $comma = '';
	my $count = 0;
	
	foreach my $point ( @{$self->{source_points}{$age}} )
	{
	    my ($coll_no, $which, $lng, $lat) = @$point;
	    my $oid = $age_code{$which} . ".$coll_no";
	    
	    next unless $lng ne '' && $lat ne '';	# skip any point with null coordinates.
	    
	    $request_json .= $comma; $comma = ",";
	    $request_json .= $self->generateFeature($lng, $lat, $oid);
	    $count++;
	}
	
	$request_json .= "]}";
	
	# Now if we have at least one point to rotate then fire off the
	# request and process the answer (if any)
	
	next AGE unless $count;
	
	logMessage(2, "    rotating $count points to $age Ma");
	
	$self->makeGPlatesRequest($ua, \$request_json, $age);
	
	# If we have gotten too many server failures in a row, then abort this
	# run.
	
	if ( $self->{fail_count} >= $FAIL_LIMIT )
	{
	    logMessage(2, "    ABORTING RUN DUE TO REPEATED QUERY FAILURE");
	    last;
	}
    }
    
    logMessage(2, "    updated $self->{update_count} paleocoordinate entries");
    logMessage(2, "DONE.");
    
    my $a = 1; # we can stop here when debugging
}


# The following routines are internally used methods, and are not exported:
# =========================================================================

sub prepareStatements {
    
    my ($self) = @_;
    
    my $dbh = $self->{dbh};
    my $sql;
    
    $sql = "INSERT IGNORE INTO $PALEOCOORDS (collection_no, present_lng, present_lat)
	    SELECT collection_no, lng, lat FROM $COLL_MATRIX
	    WHERE collection_no = ?";
    
    $self->{add_row_sth} = $dbh->prepare($sql);
    
    $sql = "UPDATE $PALEOCOORDS
	    SET early_age = ?, early_lng = ?, early_lat = ?, plate_no = ?, update_time = now()
	    WHERE collection_no = ? LIMIT 1";
    
    $self->{early_sth} = $dbh->prepare($sql);
    
    $sql = "UPDATE $PALEOCOORDS
	    SET mid_age = ?, mid_lng = ?, mid_lat = ?, plate_no = ?, update_time = now()
	    WHERE collection_no = ? LIMIT 1";
    
    $self->{mid_sth} = $dbh->prepare($sql);
    
    $sql = "UPDATE $PALEOCOORDS
	    SET late_age = ?, late_lng = ?, late_lat = ?, plate_no = ?, update_time = now()
	    WHERE collection_no = ? LIMIT 1";
    
    $self->{late_sth} = $dbh->prepare($sql);
    
    $sql = "UPDATE $COLL_MATRIX as c JOIN $PALEOCOORDS as pc using (collection_no)
	    SET c.g_plate_no = pc.plate_no
	    WHERE collection_no = ?";
	
    $self->{coll_matrix_sth} = $dbh->prepare($sql);
    
    return;
}


sub generateFeature {
    
    my ($self, $lng, $lat, $oid) = @_;
    
    my $output = "{\"type\": \"Feature\",";
    $output .= "\"geometry\": {\"type\": \"Point\", \"coordinates\": [$lat, $lng]},";
    $output .= "\"properties\": {\"name\": \"$oid\", \"feature_type\": \"gpml:UnclassifiedFeature\", ";
    $output .= "\"begin_age\": \"4000.0\", \"end_age\": \"0.0\"}";
    $output .= "}";
    
    return $output;
}


sub makeGPlatesRequest {

    my ($self, $ua, $request_ref, $age) = @_;
    
    # Generate a GPlates request.  The actual request is wrapped inside a
    # while loop so that we can retry it if something goes wrong.
    
    my $uri = "http://gplates.gps.caltech.edu:8080/reconstruct_feature_collection/";
    my @headers = ( 'Content-Type' => 'application/x-www-form-urlencoded',
		    'Content-Length' => length($$request_ref) );
    
    my $req = HTTP::Request->new(POST => $uri, \@headers, $$request_ref);
    my ($resp, $content_ref);
    my $retry_count = $RETRY_LIMIT;
    my $retry_interval = $RETRY_INTERVAL;
    
 RETRY:
    while ( $retry_count )
    {
	$DB::single = 1;
	
	$resp = $ua->request($req);
	$content_ref = $resp->content_ref;
	
	# If the request succeeds, enter the resulting coordinates into
	# the database, reset $fail_count, and return.
	
	if ( $resp->is_success )
	{
	    $self->processResponse($age, $content_ref);
	    $self->{fail_count} = 0;
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
	    $retry_interval += $RETRY_INTERVAL;
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


sub processResponse {
    
    my ($self, $age, $content_ref) = @_;
    
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
    
    # For each feature (i.e. result point) in the response, update the
    # corresponding entry in the database.
    
 POINT:
    foreach my $feature ( @{$response->{features}} )
    {
	my $key = $feature->{properties}{NAME};
	my ($selector, $collection_no) = split(qr{\.}, $key);
	
	unless ( defined $selector && $selector =~ qr{^[0-2]$} && defined $collection_no && $collection_no > 0 )
	{
	    push @bad_list, $key;
	    next POINT;
	}
	
	my ($lng, $lat) = @{$feature->{geometry}{coordinates}};
	
	unless ( $lng =~ qr{ ^ -? \d+ (?: \. \d* )? $ }x and
		 $lat =~ qr{ ^ -? \d+ (?: \. \d* )? $ }x )
	{
	    push @bad_list, "$key ($lng, $lat)";
	    next POINT;
	}
	
	my $plate_id = $feature->{properties}{PLATE_ID};
	
	$self->updateOneEntry($collection_no, $selector, $age, $lng, $lat, $plate_id);
	
	# If this is the first request, and the response includes age
	# bounds, use them.
	
	# unless ( $self->{bounds_checked} )
	# {
	#     if ( defined $feature->{properties}{FROMAGE} )
	#     {
	# 	$self->{max_age_bound} = $feature->{properties}{FROMAGE};
	# 	logMessage(2, "      max age bound reported as $self->{max_age_bound}") if $self->{debug};
	#     }
	#     if ( defined $feature->{properties}{TOAGE} )
	#     {
	# 	$self->{min_age_bound} = $feature->{properties}{TOAGE};
	# 	logMessage(2, "      min age bound reported as $self->{min_age_bound}") if $self->{debug};
	#     }
	#     $self->{bounds_checked} = 1;
	# }
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
    
    if ( $selector eq '0' )
    {
	$self->{early_sth}->execute($age, $lng, $lat, $plate_id, $collection_no);
    }
    
    elsif ( $selector eq '1' )
    {
	$self->{mid_sth}->execute($age, $lng, $lat, $plate_id, $collection_no);
    }
    
    elsif ( $selector eq '2' )
    {
	$self->{late_sth}->execute($age, $lng, $lat, $plate_id, $collection_no);
    }
    
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
		update_time timestamp,
		present_lng decimal(9,6),
		present_lat decimal(9,6),
		plate_no int unsigned,
		early_age int unsigned,
		early_lng decimal(5,2),
		early_lat decimal(5,2),
		mid_age int unsigned,
		mid_lng decimal(5,2),
		mid_lat decimal(5,2),
		late_age int unsigned,
		late_lng decimal(5,2),
		late_lat decimal(5,2)) Engine=MyISAM CHARACTER SET utf8 COLLATE utf8_unicode_ci");
    
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
