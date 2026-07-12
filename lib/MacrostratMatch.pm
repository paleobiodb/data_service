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
    
    my ($class, $dbh, $opt_debug) = @_;
    
    my $self = { dbh => $dbh,
		 debug => $opt_debug };
    
    my ($lock) = $dbh->selectrow_array("SELECT GET_LOCK('$TABLE{COLLECTION_UNITS}', 5)");
    
    unless ( $lock )
    {
	logMessage(1, "Another process is already updating the '$TABLE{COLLECTION_UNITS}' table");
	exit;
    }
    
    return bless($self, $class);
}


# clearUnits ( options )
# 
# Clear all column/unit entries selected by the options.

sub clearUnits {
    
    my ($self, $options) = @_;
    
    my ($filter, $desc, @rest) = $self->generateFilter($options);
    
    logMessage(1, "Clearing columns/units $desc");
    logMessage(1, $_) foreach @rest;
    
    my $sql = "UPDATE $TABLE{COLLECTION_UNITS} as cu
		    join $TABLE{COLLECTION_DATA} as cc using (collection_no)
		    join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
		SET col_id = null, unit_id = null, update_flag = false, updated = null
		WHERE $filter";
    
    my $count = $self->doSQL($sql);
    
    logMessage(2, "  cleared $count records");
    
    return;
}


# updateExisting ( options )
# 
# Update existing column/unit matches selected by the specified options, using the settings
# specified in the configuration file.

sub updateExisting {
    
    my ($self, $options) = @_;
    
    # Start by loading the relevant configuration settings from the
    # paleobiology database configuration file.
    
    $self->getConfig();
    
    # Generate a filter expression according to the specified options. If no
    # filtering options were given, the filter expression will be "1". The
    # remaining returned values provide a text description of which records will
    # be updated.
    
    my ($filter, $desc, @rest) = $self->generateFilter($options);
    
    logMessage(1, "Updating existing columns/units $desc");
    logMessage(1, $_) foreach @rest;
    
    my $sql = "UPDATE $TABLE{COLLECTION_UNITS} as cu
		    join $TABLE{COLLECTION_DATA} as cc using (collection_no)
		    join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
		SET update_flag = true
		WHERE $filter";
    
    my $count = $self->doSQL($sql);
    
    logMessage(2, "    flagged $count existing records to update");
    
    # Now update all of the records that have been flagged, including
    # any flags that were already set when this subroutine was called.
    
    $self->updateFlagged($filter, $options);
}


# updateNew ( options )
# 
# Generate column/unit matches for new collections, and update those whose location has
# been modified. The specified options may restrict the selection. The update operation
# uses the settings specified in the configuration file.

sub updateNew {
    
    my ($self, $options) = @_;
    
    my $dbh = $self->{dbh};
    
    # Start by loading the relevant configuration settings from the
    # paleobiology database configuration file.
    
    $self->getConfig();
    
    # Generate a filter expression according to the specified options. If no
    # filtering options were given, the filter expression will be "1". The
    # remaining returned values provide a text description of which records will
    # be updated.
    
    my ($filter, $desc, @rest) = $self->generateFilter($options);
    
    logMessage(1, "Updating new columns/units $desc");
    logMessage(1, $_) foreach @rest;
    
    # Create entries in the static table for any collections that aren't already there.
    
    my $sql = "INSERT IGNORE INTO $TABLE{COLLECTION_UNITS_STATIC}
		(collection_no, match_lat, match_lng, bin_id, max_interval_no, min_interval_no,
		 max_ma, min_ma, known_match, political_unit, synchronized, update_new)
	    SELECT c.collection_no, c.lat, c.lng, c.bin_id_2, cc.max_interval_no, cc.min_interval_no,
		min(cc.direct_ma + cc.direct_ma_error, cc.max_ma + cc.max_ma_error),
		max(cc.direct_ma - cc.direct_ma_error, cc.max_ma - cc.max_ma_error),
		find_in_set('eODP', c.research_group), c.latlng_basis = 'political unit', now(), 1
	    FROM $TABLE{COLLECTION_MATRIX} as c join $TABLE{COLLECTION_DATA} as cc using (collection_no)
		left join $TABLE{COLLECTION_UNITS} as cu using (collection_no)
	    WHERE cu.collection_no is null and $filter";
    
    my $count = $self->doSQL($sql);
    
    logMessage(2, "    added entries for $count existing collections");
    
    # Update entries where the latitude and/or longitude of the collection has changed.
    
    $sql = "UPDATE $TABLE{COLLECTION_UNITS_STATIC} as cs
		join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
	    SET cs.match_lat = c.lat, cs.match_lng = c.lng,
		cs.update_new = if(cs.known_match, false, true),
		cs.synchronized = now()
	    WHERE (cs.match_lat <> c.lat or cs.match_lng <> c.lng) and $filter";
    
    $count = $self->doSQL($sql);
    
    logMessage(2, "    updated entries for $count collections whose coordinates have changed");
    
    # Update entries where the max and/or min interval has changed.
    
    $sql = "UPDATE $TABLE{COLLECTION_UNITS_STATIC} as cs
		join $TABLE{COLLECTION_DATA} as cc using (collection_no)
	    SET cs.max_interval_no = cc.max_interval_no, cs.min_interval_no = cc.min_interval_no,
		cs.update_new = if(cs.known_match, false, true),
		cs.synchronized = now()
	    WHERE (cs.max_interval_no <> cc.max_interval_no or
		   cs.min_interval_no <> cc.min_interval_no) and $filter";
    
    $count = $self->doSQL($sql);
    
    logMessage(2, "    updated entries for $count collections whose intervals have changed");
    
    # Update entries where the max_ma, min_ma, or direct_ma has changed.

    my $max_expr = 'min(cc.direct_ma + cc.direct_ma_error, cc.max_ma + cc.max_ma_error)';
    my $min_expr = 'max(cc.direct_ma - cc.direct_ma_error, cc.max_ma - cc.max_ma_error)';
    
    $sql = "UPDATE $TABLE{COLLECTION_UNITS_STATIC} as cs
		join $TABLE{COLLECTION_DATA} as cc using (collection_no)
	    SET cs.max_ma = $max_expr,
		cs.min_ma = $min_expr,
		cs.update_new = if(cs.known_match, false, true),
		cs.update_new = if(cs.known_match, false, true),
		cs.synchronized = now()
	    WHERE cs.synchronized < cc.modified and
		  (coalesce($max_expr, -1) <> coalesce(cs.max_ma, -1) or
		   coalesce($min_expr, -1) <> coalesce(cs.min_ma, -1)) and $filter";
    
    $count = $self->doSQL($sql);
    
    logMessage(2, "    updated entries for $count collections whose dating has changed");
    
    # Now update all flagged entries.
    
    $self->updateFlagged('new', $filter, $options);
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
    
    $self->prepareStatements($options);
    
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
    
    # Fetch the basic information about the records that need updating,
    # in chunks of 10000.
    
    $DB::single = 1;
    
  CHUNK:
    while (1)
    {
	$sql = "SELECT cs.collection_no, cs.match_lat, cs.match_lng, cs.bin_id,
		    cs.max_ma, cs.min_ma, cs.political_unit, 
		    imax.interval_name as max_interval, imin.interval_name as min_interval,
		FROM $TABLE{COLLECTION_UNITS_STATIC} as cs
		    join $TABLE{COLLECTION_DATA} as cc using (collection_no)
		    join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
		    join $TABLE{INTERVAL_DATA} as imax on imax.interval_no = cs.max_interval_no
		    join $TABLE{INTERVAL_DATA} as imin on imin.interval_no = cs.min_interval_no
		WHERE cs.$flag_column and not(cs.known_match) and $filter
		LIMIT 10000";
	
	print STDERR "> $sql\n\n" if $self->{debug};
	
	my $updates = $dbh->selectall_arrayref($sql, { Slice => {} });
    
	my %points;
	
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
		
		my $point_key = "$record->{match_lat}|$record->{match_lng}|$max_interval|" .
		    "$min_interval|$max_age|$min_age|$record->{political_unit}";
		
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
	# construct a list of records to be passed to the matching service.
        
      MODEL:
	foreach my $bin_id ( sort keys %points )
	{
	    my @request_records;
	    
	    foreach my $point_key ( keys $points{$bin_id}->%* )
	    {
		my ($lat, $lng, $max_interval, $min_interval, $max_age, $min_age, $pol_unit) =
		    split /|/, $point_key;
		
		
	    
	    # Sort the list of paleocoordinate ages, and then iterate through it.
	    
	    my @age_list = sort { $a <=> $b } keys $points{$model}->%*;
	    
	  AGE:
	    foreach my $age ( @age_list )
	    {
		# Grab the set of points that need to be rotated to this age.
		
		my @points = $points{$model}{$age}->@*;
		
		# If the age falls outside of the age bounds for this model, set
		# all of the paleocoordinates to null.
		
		if ( $age > $model_max || $age < $model_min )
		{
		    foreach my $p ( @points )
		    {
			my ($key, $selector) = $p->@*;
			
			$self->updateOneEntry($key, $model, $selector, $age, undef, undef, undef);
		    }
		    
		    next AGE;
		}
		
		# Otherwise, create as many requests as are necessary to rotate all of these
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
		    
		    # Now if we have at least one point to rotate then fire off
		    # the request and process the answer (if any). If
		    # processResponse returns false, abort the task. 
		    
		    if ( @request_points )
		    {
			my $request_uri = "$service_uri?$request_params";
			
			if ( $opt_verbose )
			{
			    logMessage(2, "    Service request: $request_uri");
			}
			
			my $data = $self->makePaleocoordRequest($ua, $request_uri);
			
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
	
	my $cumulative = $self->{update_count};
	
	logMessage(2, "    updated $cumulative paleocoordinates out of $update_total");
    }
    
    my $time = localtime;
    
    logMessage(2, "    finished at $time");
    
    my $a = 1; # we can stop here when debugging
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
	
	foreach my $cn ( split /\s*,\s*/, $opt_coll )
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
	    my $list = join(', ', @selected_cn);
	    
	    push @clauses, "c.collection_no in ($list)";
	    push @description, "for collection(s) $list";
	}
    }
    
    if ( my $opt_bins = $options->{bin_id} )
    {
	my (@selected_1, @selected_2, @selected_3, @bad_cn);
	
	foreach my $cn ( split /\s*,\s*/, $opt_bins )
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
		my $list = join(', ', @selected_1);
		
		push @clauses, "c.bin_id_1 in ($list)";
		push @description, "for bin $list";
	    }
	    
	    if ( @selected_2 )
	    {
		my $list = join(', ', @selected_2);
		
		push @clauses, "c.bin_id_2 in ($list)";
		push @description, "for bin $list";
	    }
	    
	    if ( @selected_3 )
	    {
		my $list = join(', ', @selected_3);
		
		push @clauses, "c.bin_id_3 in ($list)";
		push @description, "for bin $list";
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
	    my $list = join(', ', @good_cc);
	    
	    push @clauses, "c.cc in ($list)";
	    push @description, "for countr(ies) $list";
	}
    }
    
    push @clauses, "1" unless @clauses;
    push @description, "for all collections" unless @description;
    
    my $sql_expr = join(' and ', @clauses);
    
    return ($sql_expr, @description);
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


