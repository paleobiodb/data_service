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

use TableDefs qw(%TABLE);
use CoreTableDefs;
use ConsoleLog qw(logMessage);
use CoreFunction qw(loadConfig configData);


our ($DEFAULT_RETRY_LIMIT) = 3;
our ($DEFAULT_RETRY_INTERVAL) = 5;
our ($DEFAULT_FAIL_LIMIT) = 3;
our ($DEFAULT_BAD_RESPONSE_LIMIT) = 5;
our ($DEFAULT_MAX_POINTS) = 35;


# CLASS CONSTRUCTOR
# -----------------

sub new {
    
    my ($class, $dbh, $opt_debug) = @_;
    
    my $self = { dbh => $dbh,
		 debug => $opt_debug };
    
    return bless($self, $class);
}


# clearCoords ( options )
# 
# Clear all paleocoordinates selected by the options.

sub clearCoords {
    
    my ($self, $options) = @_;
    
    my ($coord_filter, $unused, $desc, @rest) = $self->generateFilter($options);
    
    logMessage(1, "Clearing paleocoordinates $desc");
    logMessage(1, $_) foreach @rest;
    
    my $sql;
    
    if ( $options->{bins} )
    {
	$sql = "UPDATE $TABLE{PCOORD_BINS_DATA} as pd JOIN $TABLE{PCOORD_BINS_STATIC} as ps
		    using (bin_id, interval_no)
		SET pd.paleo_lng = null, pd.paleo_lat = null, pd.plate_no = null, 
		    pd.update_flag = false
		WHERE $coord_filter";
    }
    
    else
    {
	$sql = "UPDATE $TABLE{PCOORD_DATA} as pd JOIN $TABLE{PCOORD_STATIC} as ps
		    using (collection_no)
		SET pd.paleo_lng = null, pd.paleo_lat = null, pd.plate_no = null,
		    pd.update_flag = false
		WHERE $coord_filter";
    }
    
    my $count = $self->doSQL($sql);
    
    logMessage(2, "  cleared $count coordinates");
    
    return;
}


# updateExisting ( options )
# 
# Update existing paleocoordinates selected by the specified options, using the settings
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
    
    my ($coord_filter, $unused, $desc, @rest) = $self->generateFilter($options);
    
    logMessage(1, "Updating existing paleocoordinates $desc");
    logMessage(1, $_) foreach @rest;
    
    my $sql;
    
    if ( $options->{bins} )
    {
	$sql = "UPDATE $TABLE{PCOORD_BINS_DATA} as pd JOIN $TABLE{PCOORD_BINS_STATIC} as ps
		    using (bin_id, interval_no)
		SET update_flag = true
		WHERE $coord_filter";
    }
    
    else
    {
	$sql = "UPDATE $TABLE{PCOORD_DATA} as pd JOIN $TABLE{PCOORD_STATIC} as ps
		    using (collection_no)
		SET update_flag = true
		WHERE $coord_filter";
    }
    
    my $count = $self->doSQL($sql);
    
    logMessage(2, "    flagged $count existing coordinates to update");
    
    # Now update all of the paleocoordinates that have been flagged, including
    # any flags that were already set when this subroutine was called.
    
    $self->updateFlagged($coord_filter, $options);
}


# updateNew ( options )
# 
# Generate paleocoordinates for new collections, and update those whose age or modern
# location have been modified. The specified options may restrict the selection. The
# update operation uses the settings specified in the configuration file.

sub updateNew {
    
    my ($self, $options) = @_;
    
    my $dbh = $self->{dbh};
    
    my $word = $options->{bins} ? 'bins' : 'collections';
    
    # Start by loading the relevant configuration settings from the
    # paleobiology database configuration file.
    
    $self->getConfig();
    
    # Generate a filter expression according to the specified options. If no
    # filtering options were given, the filter expression will be "1". The
    # remaining returned values provide a text description of which records will
    # be updated.
    
    my ($coord_filter, $static_filter, $desc, @rest) = $self->generateFilter($options);
    
    logMessage(1, "Generating new paleocoordinates $desc");
    logMessage(1, $_) foreach @rest;
    
    # Get a list of the available paleocoordinate models, restricted by the
    # filter option 'model' if given.
    
    my @model_list = grep { $self->{model_filter}{$_} } $self->getModels();
    
    if ( @model_list )
    {
	logMessage(1, "Paleocoordinates will be computed using " . join(', ', @model_list));
    }
    
    else
    {
	logMessage(1, "No paleocoordinate models were found");
	print STDERR "No paleocoordinate models were found.\n";
	return;
    }
    
    # Add a new entry to PCOORD_STATIC for every new collection, or to PCOORD_BINS_STATIC
    # for every new bin.
    
    my ($sql, $count, $result);
    
    if ( $options->{bins} )
    {
	$static_filter =~ s/ps[.]early_age/i.early_age/g;
	$static_filter =~ s/ps[.]late_age/i.late_age/g;
	
	$sql = "INSERT IGNORE INTO $TABLE{PCOORD_BINS_STATIC}
		(bin_id, interval_no, present_lat, present_lng, early_age, late_age, update_flag)
	    SELECT s.bin_id, s.interval_no, s.lat, s.lng, i.early_age, i.late_age, 1
	    FROM $TABLE{SUMMARY_BINS} as s join $TABLE{INTERVAL_DATA} as i using (interval_no)
		left join $TABLE{PCOORD_BINS_STATIC} as ps using (bin_id, interval_no)
	    WHERE ps.bin_id is null and i.scale_no = 1 and $static_filter";
    }
    
    else
    {
	$static_filter =~ s/ps[.]early_age/c.early_age/g;
	$static_filter =~ s/ps[.]late_age/c.late_age/g;
	
	$sql = "INSERT IGNORE INTO $TABLE{PCOORD_STATIC}
		(collection_no, present_lat, present_lng, early_age, late_age, update_flag)
	    SELECT c.collection_no, c.lat, c.lng, c.early_age, c.late_age, 1
	    FROM $TABLE{COLLECTION_MATRIX} as c
		left join $TABLE{PCOORD_STATIC} as ps using (collection_no)
	    WHERE ps.collection_no is null and $static_filter";
    }
    
    $count = $self->doSQL($sql);
    
    logMessage(2, "    adding pcoords for $count new $word")
	if $count && $count > 0;
    
    # Mark for update any rows in PCOORD_STATIC corresponding to collections or bins
    # whose modern coordinates have been modified.
    
    if ( $options->{bins} )
    {
	$sql = "UPDATE $TABLE{PCOORD_BINS_STATIC} as ps
		join $TABLE{SUMMARY_BINS} as s using (bin_id, interval_no)
	    SET ps.present_lat = s.lat, ps.present_lng = s.lng, 
		ps.update_flag = true, ps.invalid = false
	    WHERE (s.lat <> ps.present_lat or s.lng <> ps.present_lng)
		and $static_filter";
    }
    
    else
    {
	$sql = "UPDATE $TABLE{PCOORD_STATIC} as ps
		join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
	    SET ps.present_lat = c.lat, ps.present_lng = c.lng, 
		ps.update_flag = true, ps.invalid = false
	    WHERE (c.lat <> ps.present_lat or c.lng <> ps.present_lng)
		and $static_filter";
    }
    
    $count = $self->doSQL($sql);
    
    logMessage(2, "    updating pcoords for $count collections with modified locations")
	if $count && $count > 0;
    
    # Mark for update any rows in PCOORD_STATIC corresponding to collections
    # whose age range has been modified.
    
    if ( $options->{bins} )
    {
	$sql = "UPDATE $TABLE{PCOORD_BINS_STATIC} as ps
		join $TABLE{SUMMARY_BINS} as s using (bin_id, interval_no)
		join $TABLE{INTERVAL_DATA} as i using (interval_no)
	    SET ps.early_age = i.early_age, ps.late_age = i.late_age, 
		ps.update_flag = true
            WHERE (i.early_age <> ps.early_age or i.late_age <> ps.late_age)
		and $static_filter";
    }
    
    else
    {
	$sql = "UPDATE $TABLE{PCOORD_STATIC} as ps
		join $TABLE{COLLECTION_MATRIX} as c using (collection_no)
	    SET ps.early_age = c.early_age, ps.late_age = c.late_age, 
		ps.update_flag = true
            WHERE (c.early_age <> ps.early_age or c.late_age <> ps.late_age)
		and $static_filter";
    }
    
    $count = $self->doSQL($sql);
    
    logMessage(2, "    updating pcoords for $count collections with modified ages")
	if $count && $count > 0;
    
    # If the option 'all' was specified, check all entries to see if any need to
    # be updated. Otherwise, check only those for which the update flag has been
    # set. 
    
    my $check = $options->{all} ? '1' : 'ps.update_flag';
    
    # Delete any rows in PCOORD_DATA corresponding to collections
    # whose coordinates have been made invalid.  This should not happen very
    # often, but is a boundary case that we need to take care of.
    
    if ( $options->{bins} )
    {
	$sql = "SELECT count(*) FROM $TABLE{PCOORD_BINS_STATIC} as ps
	    WHERE (ps.present_lat is null or ps.present_lat not between -90 and 90 or
		   ps.present_lng is null or ps.present_lng not between -180 and 180)
		and $check and not(ps.invalid) and $static_filter";
    }
    
    else
    {
	$sql = "SELECT count(*) FROM $TABLE{PCOORD_STATIC} as ps
	    WHERE (ps.present_lat is null or ps.present_lat not between -90 and 90 or
		   ps.present_lng is null or ps.present_lng not between -180 and 180)
		and $check and not(ps.invalid) and $static_filter";
    }
    
    print STDERR "> $sql\n\n" if $self->{debug};
    
    ($count) = $dbh->selectrow_array($sql);
    
    if ( $count && $count > 0 )
    {
	if ( $options->{bins} )
	{
	    $sql = "DELETE FROM pd USING $TABLE{PCOORD_BINS_STATIC} as ps
		    join $TABLE{PCOORD_BINS_DATA} as pd using (bin_id, interval_no)
		WHERE (ps.present_lat is null or ps.present_lat not between -90 and 90 or
		       ps.present_lng is null or ps.present_lng not between -180 and 180)
		    and $check and $coord_filter";
	}
	
	else
	{
	    $sql = "DELETE FROM pd USING $TABLE{PCOORD_STATIC} as ps
		    join $TABLE{PCOORD_DATA} as pd using (collection_no)
		WHERE (ps.present_lat is null or ps.present_lat not between -90 and 90 or
		       ps.present_lng is null or ps.present_lng not between -180 and 180)
		    and $check and $coord_filter";
	}
	
	$result = $self->doSQL($sql);
	
	if ( $options->{bins} )
	{
	    $sql = "UPDATE $TABLE{PCOORD_BINS_STATIC} as ps
		SET ps.update_flag = false, ps.invalid = true
		WHERE (ps.present_lat is null or ps.present_lat not between -90 and 90 or
		       ps.present_lng is null or ps.present_lng not between -180 and 180)
		    and $check and $static_filter";
	}
	
	else
	{
	    $sql = "UPDATE $TABLE{PCOORD_STATIC} as ps
		SET ps.update_flag = false, ps.invalid = true
		WHERE (ps.present_lat is null or ps.present_lat not between -90 and 90 or
		       ps.present_lng is null or ps.present_lng not between -180 and 180)
		    and $check and $static_filter";
	}
	
	$result = $self->doSQL($sql);
	
	logMessage(2, "    cleared paleocoords from $count collections with invalid locations");
    }
    
    # For every entry in PCOORD_STATIC marked for update, mark all of the
    # corresponding rows in PCOORD_DATA for update. 
    
    if ( $options->{bins} )
    {
	$sql = "UPDATE $TABLE{PCOORD_BINS_DATA} as pd join $TABLE{PCOORD_BINS_STATIC} as ps
		using (bin_id, interval_no)
	    SET pd.update_flag = true
	    WHERE ps.update_flag and $coord_filter";
    }
    
    else
    {
	$sql = "UPDATE $TABLE{PCOORD_DATA} as pd join $TABLE{PCOORD_STATIC} as ps
		using (collection_no)
	    SET pd.update_flag = true
	    WHERE ps.update_flag and $coord_filter";
    }
    
    $count = $self->doSQL($sql);
    
    # Now iterate through all available models. For each model, add rows for
    # every collection that doesn't yet have paleocoords for this model and
    # whose age range overlaps the age range of the model.
    
    foreach my $model ( @model_list )
    {
	my $model_min = $self->{min_age}{$model};
	my $model_max = $self->{max_age}{$model};
	my $quoted_model = $dbh->quote($model);
	
	my $count = 0;
	
	if ( $options->{bins} )
	{
	    $sql = "INSERT INTO $TABLE{PCOORD_BINS_DATA}
			(bin_id, interval_no, model, selector, update_flag, age)
		SELECT ps.bin_id, ps.interval_no, $quoted_model, 'early', 1, round(early_age, 0) as age
		FROM $TABLE{PCOORD_BINS_STATIC} as ps left join $TABLE{PCOORD_BINS_DATA} as pd
			on pd.bin_id = ps.bin_id and pd.interval_no = ps.interval_no
			and pd.model = $quoted_model and pd.selector = 'early'
		WHERE $check and pd.bin_id is null and not(ps.invalid) and $static_filter
		HAVING age between $model_min and $model_max";
	
	    $count += $self->doSQL($sql);
	    
	    $sql = "INSERT INTO $TABLE{PCOORD_BINS_DATA}
			(bin_id, interval_no, model, selector, update_flag, age)
		    SELECT ps.bin_id, ps.interval_no, $quoted_model, 'late', 1, round(late_age, 0) as age
		    FROM $TABLE{PCOORD_BINS_STATIC} as ps left join $TABLE{PCOORD_BINS_DATA} as pd
			    on pd.bin_id = ps.bin_id and pd.interval_no = ps.interval_no
			    and pd.model = $quoted_model and pd.selector = 'late'
		    WHERE $check and pd.bin_id is null and not(ps.invalid) and $static_filter
		    HAVING age between $model_min and $model_max";

	    $count += $self->doSQL($sql);

	    $sql = "INSERT INTO $TABLE{PCOORD_BINS_DATA}
			(bin_id, interval_no, model, selector, update_flag, age)
		    SELECT ps.bin_id, ps.interval_no, $quoted_model, 'mid', 1,
			round((ps.early_age + ps.late_age)/2, 0) as age
		    FROM $TABLE{PCOORD_BINS_STATIC} as ps left join $TABLE{PCOORD_BINS_DATA} as pd
			    on pd.bin_id = ps.bin_id and pd.interval_no = ps.interval_no
			    and pd.model = $quoted_model and pd.selector = 'mid'
		    WHERE $check and pd.bin_id is null and not(ps.invalid) and $static_filter
		    HAVING age between $model_min and $model_max";

	    $count += $self->doSQL($sql);
	}
	
	else
	{
	    $sql = "INSERT INTO $TABLE{PCOORD_DATA}
			(collection_no, model, selector, update_flag, age)
		SELECT ps.collection_no, $quoted_model, 'early', 1, round(early_age, 0) as age
		FROM $TABLE{PCOORD_STATIC} as ps left join $TABLE{PCOORD_DATA} as pd
			on pd.collection_no = ps.collection_no 
			and pd.model = $quoted_model and pd.selector = 'early'
		WHERE $check and pd.collection_no is null and not(ps.invalid) and $static_filter
		HAVING age between $model_min and $model_max";
	
	    $count += $self->doSQL($sql);
	    
	    $sql = "INSERT INTO $TABLE{PCOORD_DATA}
			(collection_no, model, selector, update_flag, age)
		    SELECT ps.collection_no, $quoted_model, 'late', 1, round(late_age, 0) as age
		    FROM $TABLE{PCOORD_STATIC} as ps left join $TABLE{PCOORD_DATA} as pd
			    on pd.collection_no = ps.collection_no 
			    and pd.model = $quoted_model and pd.selector = 'late'
		    WHERE $check and pd.collection_no is null and not(ps.invalid) and $static_filter
		    HAVING age between $model_min and $model_max";

	    $count += $self->doSQL($sql);

	    $sql = "INSERT INTO $TABLE{PCOORD_DATA}
			(collection_no, model, selector, update_flag, age)
		    SELECT ps.collection_no, $quoted_model, 'mid', 1,
			round((ps.early_age + ps.late_age)/2, 0) as age
		    FROM $TABLE{PCOORD_STATIC} as ps left join $TABLE{PCOORD_DATA} as pd
			    on pd.collection_no = ps.collection_no 
			    and pd.model = $quoted_model and pd.selector = 'mid'
		    WHERE $check and pd.collection_no is null and not(ps.invalid) and $static_filter
		    HAVING age between $model_min and $model_max";

	    $count += $self->doSQL($sql);
	}
	
	logMessage(2, "    adding $count new paleocoordinates for generation by $model");
    }
    
    # Now update all of the paleocoordinates that have been flagged, including
    # any flags that were already set when this subroutine was called.
    
    $self->updateFlagged($coord_filter, $options);
    
    # After this succeeds, clear all of the update flags on the records
    # that were selected by the filter.
    
    # $sql = "UPDATE $TABLE{PCOORD_DATA} SET update_flag = false
    # 	    WHERE $coord_filter and updated";
    
    # $sql = "UPDATE $TABLE{PCOORD_STATIC} SET update_flag = false
    # 	    WHERE $static_filter and updated";
    
    # $result = $self->doSQL($sql);    
}


# updateFlagged ( )
# 
# Update all paleocoordinates that have been flagged. Each coordinate's flag is
# cleared when it is successfully updated, so if one call to this subroutine is
# interrupted then the next one will complete all outstanding updates.

sub updateFlagged {
    
    my ($self, $coord_filter, $options) = @_;
    
    my $dbh = $self->{dbh};
    
    my $service_uri = $self->{service_uri};
    
    my $opt_verbose = $options->{verbose};
    
    my $sql;
    
    # Prepare the SQL statements that will be used to update entries in the
    # table, and generate a user agent object with which to make requests.
    
    $self->prepareStatements($options);
    
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database Updater/0.1");
    
    # Count the number of paleocoordinates to be updated.
    
    if ( $options->{bins} )
    {
	$sql = "SELECT count(*) FROM $TABLE{PCOORD_BINS_DATA} WHERE update_flag";
    }
    
    else
    {
	$sql = "SELECT count(*) FROM $TABLE{PCOORD_DATA} WHERE update_flag";
    }
    
    my ($update_total) = $dbh->selectrow_array($sql);
    
    logMessage(2, "    updating $update_total paleocoordinate entries...");
    
    # Fetch the basic information about the paleocoordinates that need updating,
    # in chunks of 10000.
    
    $DB::single = 1;
    
  CHUNK:
    while (1)
    {
	if ( $options->{bins} )
	{
	    $sql = "SELECT ps.bin_id, ps.interval_no, ps.present_lng, ps.present_lat,
		   pd.model, pd.selector, pd.age
		FROM $TABLE{PCOORD_BINS_DATA} as pd join $TABLE{PCOORD_BINS_STATIC} as ps
		    using (bin_id, interval_no)
		WHERE pd.update_flag and $coord_filter LIMIT 10000";
	}
	
	else
	{
	    $sql = "SELECT ps.collection_no, ps.present_lng, ps.present_lat,
		   pd.model, pd.selector, pd.age
		FROM $TABLE{PCOORD_DATA} as pd join $TABLE{PCOORD_STATIC} as ps using (collection_no)
		WHERE pd.update_flag and $coord_filter LIMIT 10000";
	}
	
	print STDERR "> $sql\n\n" if $self->{debug};
	
	my $updates = $dbh->selectall_arrayref($sql, { Slice => {} });
    
	my %points;
	
	if ( ref $updates eq 'ARRAY' && @$updates )
	{    
	    foreach my $record ( @$updates )
	    {
		my $key;
		
		if ( $options->{bins} )
		{
		    $key = "$record->{bin_id}-$record->{interval_no}";
		}
		
		else
		{
		    $key = $record->{collection_no};
		}
		
		my $model = $record->{model};
		my $lng = $record->{present_lng};
		my $lat = $record->{present_lat};
		my $selector = $record->{selector};
		my $age = $record->{age};
		
		push $points{$model}{$age}->@*, [$key, $selector, $lng, $lat];
	    }
	}
	
	else
	{
	    last CHUNK;
	}
	
	# Retrieve the list of models that were selected by the options passed to
	# the parent subroutine call.
	
	my @model_list = sort keys %points;
	
	# Now iterate through all of the available models and all of the ages of
	# paleocoordinates that need to be generated or updated. For each
	# model/age combination, send off as many requests as are necessary to
	# generate all of the paleocoordinates for that model and age.
        
      MODEL:
	foreach my $model ( @model_list )
	{
	    my $model_min = $self->{min_age}{$model};
	    my $model_max = $self->{max_age}{$model};
	    
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
# Return two SQL expressions. The first will filter out rows in the
# PALEOCOORD_DATA table that do not match the specified options. If no filtering
# options were given, the result will be "1" which does not filter out any rows.
# 
# The second expression will filter out rows in the PALEOCOORD_STATIC table that
# do not match the specified options. If no filtering options were given, this
# will also be "1".
# 
# All of the filter values are stored under $self using keys such as
# 'model_filter', 'min_age_filter', etc.
# 
# This method returns the two filter expressions followed by one or more text
# strings that describe the generated filter. These can be printed out to
# confirm to the user that the proper filter is being applied.

sub generateFilter {
    
    my ($self, $options) = @_;
    
    my (@coord_clauses, @static_clauses, @description);
    
    my %is_model = map { $_ => 1 } $self->getModels();
    
    if ( my $opt_coll = $options->{collection_no} )
    {
	my (@selected_cn, @bad_cn);
	
	foreach my $cn ( split /\s*,\s*/, $opt_coll )
	{
	    if ( $cn =~ /^\d+$/ )
	    {
		push @selected_cn, $cn;
		$self->{collection_filter}{$cn} = 1;
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
	
	else
	{
	    my $list = join(', ', @selected_cn);
	    
	    push @coord_clauses, "pd.collection_no in ($list)";
	    push @static_clauses, "ps.collection_no in ($list)";
	    push @description, "from collection $list";
	}
    }
    
    if ( my $opt_bins = $options->{bin_id} )
    {
	my (@selected_cn, @bad_cn);
	
	foreach my $cn ( split /\s*,\s*/, $opt_bins )
	{
	    if ( $cn =~ /^\d+$/ )
	    {
		push @selected_cn, $cn;
		$self->{bin_filter}{$cn} = 1;
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
	    my $list = join(', ', @selected_cn);
	    
	    push @coord_clauses, "pd.bin_id in ($list)";
	    push @static_clauses, "ps.bin_id in ($list)";
	    push @description, "from bin_id $list";
	}
    }
    
    if ( my $opt_model = $options->{model} )
    {
	my (@selected_models, @bad_models);
	
	foreach my $name ( split /\s*,\s*/, $opt_model )
	{
	    if ( $name && $is_model{$name} )
	    {
		push @selected_models, $name;
		$self->{model_filter}{$name} = 1;
	    }
	    
	    elsif ( $name )
	    {
		push @bad_models, $name;
	    }
	}
	
	if ( @bad_models )
	{
	    my $list = join("', '", @bad_models);
	    die "Invalid model: '$list'\n";
	}
	
	else
	{
	    my $list = join("', '", @selected_models);
	    
	    push @coord_clauses, "pd.model in ('$list')" if $list;
	    push @description, "from model '$list'" if $list;
	}
    }
    
    else
    {
	$self->{model_filter} = \%is_model;
    }
    
    if ( $options->{min_age} && $options->{min_age} > 0 )
    {
	my $opt_min_age = $options->{min_age};
	
	die "Invalid value for min_age: $opt_min_age\n" unless
	    $opt_min_age =~ / ^ \d+ (?: [.] \d* )? $ /xs;
	
	$self->{min_age_filter} = $opt_min_age;
	
	push @coord_clauses, "pd.age >= $opt_min_age";
	push @static_clauses, "ps.early_age >= $opt_min_age";
	push @description, "at least $opt_min_age Ma";
    }
    
    if ( my $opt_max_age = $options->{max_age} )
    {
	die "Invalid value for max_age: $opt_max_age\n" unless
	    $opt_max_age =~ / ^ \d+ (?: [.] \d* )? $ /xs;
	
	$self->{max_age_filter} = $opt_max_age;
	
	push @coord_clauses, "pd.age <= $opt_max_age";
	push @static_clauses, "ps.early_age <= $opt_max_age";
	push @description, "at most $opt_max_age Ma";
    }
    
    push @coord_clauses, "1" unless @coord_clauses;
    push @static_clauses, "1" unless @static_clauses;
    push @description, "" unless @description;
    
    my $coord_expr = join(' and ', @coord_clauses);
    my $static_expr = join(' and ', @static_clauses);
    
    return ($coord_expr, $static_expr, @description);
}


# getModels ( )
# 
# List the names of the active models from the PCOORD_MODELS table. Store the
# minimum and maximum age associated with each model for later use. If
# $opt_model is specified, if should be a comma-separated list of model names.
# Only names on the list will be returned.

sub getModels {
    
    my ($self) = @_;
    
    # If we have already generated the list of active models, return it.
    
    if ( $self->{model_list} )
    {
	return $self->{model_list}->@*;
    }
    
    # Otherwise, retrieve the contents of the PCOORD_MODELS table.
    
    my $dbh = $self->{dbh};
    
    my $sql = "SELECT * FROM $TABLE{PCOORD_MODELS} WHERE is_active ORDER BY name";
    
    print STDERR "> $sql\n\n" if $self->{debug};
    
    my $model_entries = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    my @model_list;
    
    # Iterate through the retrieved entries, if any were found.
    
    if ( ref $model_entries eq 'ARRAY' && $model_entries->@* )
    {
	foreach my $entry ( $model_entries->@* )
	{
	    # Add the name of this model to the list, if it is active.  Store
	    # the minimum and maximum age for later use.
	    
	    my $name = $entry->{name};
	    
	    if ( $name && $entry->{is_active} )
	    {
		push @model_list, $name;
		$self->{min_age}{$name} = $entry->{min_age} + 0;
		$self->{max_age}{$name} = $entry->{max_age} + 0;
	    }
	}
    }
    
    # Cache the list and return its contents.
    
    $self->{model_list} = \@model_list;
    return @model_list;
}


# getConfig ( )
# 
# Load the configuration settings that will be used in the process of making and
# processing requests to the paleocoordinate service.

sub getConfig {
    
    my ($self) = @_;
    
    loadConfig();
    
    $self->{service_uri} = configData('paleocoord_point_uri') ||
	croak "You must specify 'paleocoord_point_uri' in config.yml";
    
    $self->{update_count} = 0;
    $self->{fail_count} = 0;
    $self->{bad_count} = 0;
    $self->{debug_count} = 0;
    
    $self->{fail_limit} = configData('paleocoord_fail_limit') || $DEFAULT_FAIL_LIMIT;
    $self->{bad_limit} = configData('paleocoord_bad_response_limit') || $DEFAULT_BAD_RESPONSE_LIMIT;
    $self->{retry_limit} = configData('paleocoord_retry_limit') || $DEFAULT_RETRY_LIMIT;
    $self->{retry_interval} = configData('paleocoord_retry_interval') || $DEFAULT_RETRY_INTERVAL;
    
    $self->{max_points} = configData('paleocoord_point_limit') || $DEFAULT_MAX_POINTS;
}
	

# prepareStatements ( options )
# 
# Prepare the SQL statements that will be used to store paleocoordinate data
# into the database.

sub prepareStatements {
    
    my ($self, $options) = @_;
    
    my $dbh = $self->{dbh};
    my $sql;
    
    if ( $options->{bins} )
    {
	$sql = "UPDATE $TABLE{PCOORD_BINS_DATA}
	    SET paleo_lng = ?, paleo_lat = ?, plate_no = ?, 
		update_flag = false, updated = now()
	    WHERE bin_id = ? and interval_no = ? and model = ? and selector = ? LIMIT 1";
    }
    
    else
    {
	$sql = "UPDATE $TABLE{PCOORD_DATA}
	    SET paleo_lng = ?, paleo_lat = ?, plate_no = ?, 
		update_flag = false, updated = now()
	    WHERE collection_no = ? and model = ? and selector = ? LIMIT 1";
    }
    
    print STDERR "> $sql\n\n" if $self->{debug};
    
    $self->{update_sth} = $dbh->prepare($sql);
    
    return;
}


# makePaleocoordRequest ( ua, request )
# 
# Make a request to the paleocoordinate service using the specified user agent
# object. If the request succeeds and the response is valid JSON, decode it and
# return it.

sub makePaleocoordRequest {

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


# processResponse ( model, age, response_data, source_points )
# 
# Process a response received from the rotation service. Both $response_data and
# $source_points must be array references.
# 
# We need the value of $age because the response does not include this
# information. The request MUST include the argument 'include_failures=1', so
# that paleocoordinates which cannot be computed are returned as null entries.
# This enables us to match up the entries in $response_data with the corresponding
# entries in $source_points.

sub processResponse {
    
    my ($self, $model, $age, $response_data, $source_points) = @_;
    
    my $dbh = $self->{dbh};
    my $response;
    my @bad_list;
    
    # Iterate through the entries in $response_data.
    
  POINT:
    foreach my $i ( 0..$response_data->$#* )
    {
	my $entry = $response_data->[$i];
	
	# Get the corresponding collection identifier and selector (early, mid, late) from
	# the $source_points array.
	
	my ($key, $selector) = $source_points->[$i]->@*;
	
	# If the entry is null, it means that the specified rotation model cannot produce
	# a paleocoordinate for this point/age combination. Set the paleocoordinates for
	# the corresponding collection identifier, model, selector, and age to null.
	
	if ( ! $entry )
	{
	    $self->updateOneEntry($key, $model, $selector, $age, undef, undef, undef);
	}
	
	# If the entry contains coordinates, set the paleocoordinates for the
	# corresponding collection identifier, selector, and age to these
	# values. If it contains a plate_id, set that too.
	
	elsif ( ref $entry eq 'HASH' && ref $entry->{geometry}{coordinates} eq 'ARRAY' )
	{
	    my ($lng, $lat) = $entry->{geometry}{coordinates}->@*;
	    
	    my $plate_id = $entry->{properties}{plate_id};
	    
	    $self->updateOneEntry($key, $model, $selector, $age, $lng, $lat, $plate_id);
	}
	
	# Otherwise, keep a list of any bad entries.
	
	else
	{
	    push @bad_list, "$key $age ($selector)";
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
    
    my ($self, $key, $model, $selector, $age, $lng, $lat, $plate_id) = @_;
    
    my $dbh = $self->{dbh};
    
    eval {
	
	if ( $key =~ /(\d+)-(\d+)/ )
	{
	    $self->{update_sth}->execute($lng, $lat, $plate_id, $1, $2, $model, $selector);
	    $self->{update_count}++;
	}
	
	else
	{
	    $self->{update_sth}->execute($lng, $lat, $plate_id, $key, $model, $selector);
	    $self->{update_count}++;
	}
    };
    
    if ( $@ )
    {
	logMessage(1, "ERROR updating table '$TABLE{PCOORD_DATA}': $@");
	print STDERR "ERROR updating table '$TABLE{PCOORD_DATA}': $@\n";
	$self->{bad_count}++;
    }
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


# initializeTables ( dbh, argument, replace)
# 
# Create the tables used by this module. If $replace is true, drop the existing ones.
# Otherwise, rename them using the extension _bak.

sub initializeTables {
    
    my ($self, $argument, $replace) = @_;
    
    unless ( $argument =~ qr{ ^ tables $ | ^ PCOORD_ (BINS_)? (DATA|STATIC|MODELS|PLATES) $ }xs )
    {
	die "Invalid argument '$argument'";
    }
    
    if ( $argument eq 'PCOORD_DATA' || $argument eq 'tables' || 
	 $argument eq $TABLE{PCOORD_DATA} )
    {
	$self->initOneTable('PCOORD_DATA', $replace,
		     "CREATE TABLE IF NOT EXISTS $TABLE{PCOORD_DATA} (
			collection_no int unsigned not null,
			model varchar(255) not null,
			selector enum('early', 'mid', 'late') not null,
			age smallint unsigned not null,
			plate_no smallint unsigned null,
			paleo_lng decimal(5,2) null,
			paleo_lat decimal(5,2) null,
			update_flag boolean not null,
			updated timestamp not null,
			PRIMARY KEY (collection_no, model, selector),
			KEY (update_flag)
			) Engine=MyISAM CHARSET=utf8mb3");
    }
    
    if ( $argument eq 'PCOORD_BINS_DATA' || $argument eq 'tables' || 
	 $argument eq $TABLE{PCOORD_BINS_DATA} )
    {
	$self->initOneTable('PCOORD_BINS_DATA', $replace,
		     "CREATE TABLE IF NOT EXISTS $TABLE{PCOORD_BINS_DATA} (
			`bin_id` int(10) unsigned NOT NULL,
			`interval_no` int(10) unsigned NOT NULL,
			`model` varchar(255) NOT NULL,
			`selector` enum('early','mid','late') NOT NULL,
			`age` smallint(5) unsigned NOT NULL,
			`plate_no` smallint(5) unsigned DEFAULT NULL,
			`paleo_lng` decimal(5,2) DEFAULT NULL,
			`paleo_lat` decimal(5,2) DEFAULT NULL,
			`update_flag` tinyint(1) NOT NULL,
			`updated` timestamp NOT NULL,
			PRIMARY KEY (`bin_id`,`interval_no`,`model`,`selector`),
			KEY `update_flag` (`update_flag`)
			) Engine=MyISAM CHARSET=utf8mb3");
    }
    
    if ( $argument eq 'PCOORD_STATIC' || $argument eq 'tables' || 
	 $argument eq $TABLE{PCOORD_STATIC} )
    {
	$self->initOneTable('PCOORD_STATIC', $replace,
		     "CREATE TABLE IF NOT EXISTS $TABLE{PCOORD_STATIC} (
			collection_no int unsigned not null,
			present_lng decimal(9,6) null,
			present_lat decimal(9,6) null,
			early_age decimal(9,5) null,
			late_age decimal(9,5) null,
			update_flag boolean not null,
			invalid boolean default '0',
			updated timestamp not null,
			PRIMARY KEY (collection_no),
			KEY (update_flag)
			) Engine=MyISAM CHARSET=utf8mb3");
    }
    
    if ( $argument eq 'PCOORD_BINS_STATIC' || $argument eq 'tables' || 
	 $argument eq $TABLE{PCOORD_BINS_STATIC} )
    {
	$self->initOneTable('PCOORD_STATIC', $replace,
		    "CREATE TABLE IF NOT EXISTS $TABLE{PCOORD_BINS_STATIC} (
			`bin_id` int(10) unsigned NOT NULL,
			`interval_no` int(10) unsigned NOT NULL,
			`present_lng` decimal(9,6) DEFAULT NULL,
			`present_lat` decimal(9,6) DEFAULT NULL,
			`early_age` decimal(9,5) DEFAULT NULL,
			`late_age` decimal(9,5) DEFAULT NULL,
			`update_flag` tinyint(1) NOT NULL,
			`invalid` tinyint(1) DEFAULT 0,
			`updated` timestamp NOT NULL,
			PRIMARY KEY (`bin_id`,`interval_no`),
			KEY `update_flag` (`update_flag`)
			) ENGINE=MyISAM CHARSET=utf8mb3");
    }
    
    if ( $argument eq 'PCOORD_MODELS' || $argument eq 'tables' )
    {
	$self->initOneTable('PCOORD_MODELS', $replace,
		     "CREATE TABLE IF NOT EXISTS $TABLE{PCOORD_MODELS} (
			name varchar(255) not null PRIMARY KEY,
			model_id int unsigned not null,
			min_age smallint unsigned not null default '0',
			max_age smallint unsigned not null default '0',
			description varchar(255) not null default '',
			is_active boolean null,
			is_default boolean null,
			updated timestamp not null default current_timestamp()
			) Engine=MyISAM CHARSET=utf8");
    }
    
    if ( $argument eq 'PCOORD_PLATES' || $argument eq 'tables' )
    {
	$self->initOneTable('PCOORD_PLATES', $replace,
		     "CREATE TABLE IF NOT EXISTS $TABLE{PCOORD_PLATES} (
			model varchar(255) not null,
			plate_no int unsigned,
			name varchar(255) not null default '',
			min_age int unsigned null,
			max_age int unsigned null,
			updated timestamp not null default current_timestamp(),
			primary key (model, plate_no)
			) Engine=MyISAM CHARSET=utf8");
    }
    
    my $a = 1;	# we can stop here when debugging
}


# initOneTable ( )
# 
# Create the specified table using the specified create statement. If $replace
# is true, drop any existing table. Otherwise, rename any existing table using
# the suffix _bak.

sub initOneTable {
    
    my ($self, $table_specifier, $replace, $create_stmt) = @_;
    
    my $dbh = $self->{dbh};
    
    my $table_name = $TABLE{$table_specifier};
    
    my ($check) = $dbh->selectrow_array("SHOW TABLES LIKE '$table_name'");
    
    if ( $replace )
    {
	logMessage(1, "Replacing table $table_specifier as '$table_name'");
	$self->doSQL("DROP TABLE IF EXISTS ${table_name}_bak");
	$self->doSQL("RENAME TABLE IF EXISTS $table_name to ${table_name}_bak");
	return $self->doSQL($create_stmt);
    }
    
    elsif ( $check eq $table_name )
    {
	logMessage(1, "Table $table_specifier exists");
    }
    
    else
    {
	logMessage(1, "Creating new table $table_specifier as '$table_name'");
	return $self->doSQL($create_stmt);
    }
    
}


# initializeModels ( )
# 
# Load the table PCOORD_MODELS with a list of models retrieved from the
# paleocoordinate service, if there are existing rows in the table, preserve the
# values of the is_active flag. This allows an administrator to deactivate
# models they do not wish to make available to the end users, and preserve this
# state when the list of models is refreshed.

sub initializeModels {
    
    my ($self) = @_;
    
    my $dbh = $self->{dbh};
    
    # From the configuration file, get the URI for retrieving a list of models
    # from the paleocoordinate service.
    
    loadConfig;
    
    my $model_uri = configData('paleocoord_model_uri') ||
	die "You must specify 'paleocoord_model_uri' in config.yml";
    
    # Make a request using that URI. Throw an exception if it does not succeed.
    
    logMessage(1, "Initializing models using $model_uri");
    
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database Updater/0.1");
    
    my $request = HTTP::Request->new(GET => $model_uri);
    
    my $response = $ua->request($request);
    my $content = $response->content_ref;
    my $code = $response->code;
    my $data;
    
    die "Request failed with code $code: $model_uri" unless $response->is_success;
    
    # Decode the response, and throw an exception if it does not produce an
    # array of hash entries.
    
    eval {
	$data = decode_json($$content);
    };
    
    die "Bad response from service: $model_uri" 
	unless ref $data eq 'ARRAY' && $data->@* && ref $data->[0] eq 'HASH';
    
    # Retrieve a list of models currently in the database.
    
    my $sql = "SELECT name FROM $TABLE{PCOORD_MODELS}";
    
    print STDERR "> $sql\n\n" if $self->{debug};
    
    my $existing_list = $dbh->selectcol_arrayref($sql);
    
    my %exists;
    
    if ( ref $existing_list eq 'ARRAY' )
    {
	$exists{$_} = 1 foreach $existing_list->@*;
    }
    
    # Iterate through the list retrieved from the paleocoordinate service.
    # Update existing models, and add any new ones.
    
    my $update_count = 0;
    my $insert_count = 0;
    
    foreach my $entry ( $data->@* )
    {
	if ( $entry->{id} && $entry->{name} && $entry->{max_age} >= 200 )
	{
	    my $quoted_name = $dbh->quote($entry->{name});
	    my $quoted_id = $dbh->quote($entry->{id});
	    my $quoted_min = $dbh->quote($entry->{min_age} || 0);
	    my $quoted_max = $dbh->quote($entry->{max_age} || 0);
	    
	    if ( $exists{$entry->{name}} )
	    {
		$sql = "UPDATE $TABLE{PCOORD_MODELS}
		SET model_id=$quoted_id, min_age=$quoted_min, max_age=$quoted_max
		WHERE name=$quoted_name";
		
		my $result = $self->doSQL($sql);
		logMessage(1, "  updated model $quoted_name") if $result && $result > 0;
		$update_count += $result;
	    }
	    
	    else
	    {
		$sql = "INSERT INTO $TABLE{PCOORD_MODELS}
		(name, model_id, min_age, max_age, is_active)
		VALUES ($quoted_name, $quoted_id, $quoted_min, $quoted_max, true)";
	    
		my $result = $self->doSQL($sql);
		logMessage(1, "  inserted model $quoted_name") if $result && $result > 0;
		$insert_count += $result;
	    }
	}
    }
    
    unless ( $update_count || $insert_count )
    {
	logMessage(1, "All models are up to date");
    }
}


# initializePlates ( )
# 
# Delelete all entries in PCOORD_PLATES, and reload the table with a list of
# plates retrieved from the paleocoordinate service. One request will be made
# for each model in PCOORD_MODELS, including inactive ones. If any model is
# later made active, we want its plate list to be immediately available.

sub initializePlates {
    
    my ($self) = @_;
    
    my $dbh = $self->{dbh};
    
    loadConfig;
    
    my $plate_uri = configData('paleocoord_plate_uri') ||
	die "You must specify 'paleocoord_plate_uri' in config.yml";
    
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database Updater/0.1");
    
    # Get a list of all models, including inactive ones. If none are found, tell
    # the user that they must initialize the models before initializing the plates.
    
    my $sql = "SELECT name FROM $TABLE{PCOORD_MODELS}";
    
    print STDERR "> $sql\n\n" if $self->{debug};
    
    my $model_list = $dbh->selectcol_arrayref($sql);
    
    unless ( ref $model_list eq 'ARRAY' && $model_list->@* )
    {
	die "You must initialize the models first";
    }
    
    logMessage(1, "Initializing plates using $plate_uri");
    
    # Iterate through the models, fetching and storing a list of plates for each
    # one.
    
  MODEL:
    foreach my $model ( $model_list->@* )
    {
	logMessage(1, "  loading plates for model '$model'...");
	
	my $request_uri = "$plate_uri?model=$model";
	
	my $request = HTTP::Request->new(GET => $request_uri);
	
	my $response = $ua->request($request);
	my $content = $response->content_ref;
	my $code = $response->code;
	my $data;
	
	unless ( $response->is_success )
	{
	    print STDERR "Request failed with code '$code': $request_uri";
	    next MODEL;
	}
	
	eval {
	    $data = decode_json($$content);
	};
	
	unless ( ref $data eq 'ARRAY' && @$data )
	{
	    print STDERR "Bad response from service: $request_uri";
	    next MODEL;
	}
	
	# Delete any existing plate records for this model.
	
	$self->doSQL("DELETE FROM $TABLE{PCOORD_PLATES} WHERE model='$model'");
	
	# Iterate through the response data and insert a row for each plate.
	
	my $quoted_model = $dbh->quote($model);
	my $count = 0;
	
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
	    
	    my $sql = "INSERT IGNORE INTO $TABLE{PCOORD_PLATES} " .
		"(model, plate_no, name, min_age, max_age)" .
		"VALUES ($quoted_model, $quoted_id, $quoted_name, $quoted_min, $quoted_max)";
	    
	    $count += $self->doSQL($sql);
	}
	
	logMessage(1, "  inserted $count plates into '$TABLE{PCOORD_PLATES}'");
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

