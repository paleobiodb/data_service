# 
# The Paleobiology Database
# 
#   IntervalTables.pm
# 

package TimescaleTables;

use strict;

use Carp qw(carp croak);
use Try::Tiny;

use TableDefs qw($TIMESCALE_DATA $TIMESCALE_REFS $TIMESCALE_INTS $TIMESCALE_BOUNDS $TIMESCALE_PERMS
	         $INTERVAL_DATA $INTERVAL_MAP $SCALE_MAP $MACROSTRAT_INTERVALS);
use CoreFunction qw(activateTables loadSQLFile);
use ConsoleLog qw(logMessage);

use base 'Exporter';

our(@EXPORT_OK) = qw(establishTimescaleTables copyOldTimescales);


# Table and file names

our $TIMESCALE_WORK = 'tsw';
our $TS_REFS_WORK = 'tsrw';
our $TS_INTS_WORK = 'tsiw';
our $TS_BOUNDS_WORK = 'tsbw';
our $TS_PERMS_WORK = 'tspw';

=head1 NAME

Timescale tables

=head1 SYNOPSIS

This module builds and maintains the tables for storing the definitions of timescales and
timescale intervals and boundaries.

=head2 TABLES

The following tables are maintained by this module:

=over 4

=item timescales

Lists each timescale represented in the database.

=back

=cut

=head1 INTERFACE

In the following documentation, the parameter C<dbi> refers to a DBI database handle.

=cut


# establishTimescaleTables ( dbh, options )
# 
# This function creates the timescale tables, or replaces the existing ones.  The existing ones,
# if any, are renamed to *_bak.

sub establishTimescaleTables {
    
    my ($dbh, $options) = @_;
    
    $options ||= { };
    
    # First create the table 'timescales'.  This stores information about each timescale.
    
    $dbh->do("DROP TABLE IF EXISTS $TIMESCALE_WORK");
    
    $dbh->do("CREATE TABLE $TIMESCALE_WORK (
		timescale_no int unsigned primary key auto_increment,
		authorizer_no int unsigned not null,
		timescale_name varchar(80) not null,
		source_timescale_no int unsigned not null,
		early_age decimal(9,5),
		late_age decimal(9,5),
		reference_no int unsigned not null,
		interval_type enum('eon', 'era', 'period', 'epoch', 'stage', 'zone'),
		is_active boolean,
		created timestamp default current_timestamp,
		modified timestamp default current_timestamp,
		updated timestamp default current_timestamp on update current_timestamp,
		key (reference_no),
		key (authorizer_no))");
    
    # $dbh->do("DROP TABLE IF EXISTS $TIMESCALE_ARCHIVE");
    
    # $dbh->do("CREATE TABLE $TIMESCALE_ARCHIVE (
    # 		timescale_no int unsigned,
    # 		revision_no int unsigned auto_increment,
    # 		authorizer_no int unsigned not null,
    # 		timescale_name varchar(80) not null,
    # 		source_timescale_no int unsigned,
    # 		early_age decimal(9,5),
    # 		late_age decimal(9,5),
    # 		reference_no int unsigned not null,
    # 		interval_type enum('eon', 'era', 'period', 'epoch', 'stage', 'zone'),
    # 		is_active boolean,
    # 		created timestamp default current_timestamp,
    # 		modified timestamp default current_timestamp,
    # 		key (reference_no),
    # 		key (authorizer_no),
    # 		primary key (timescale_no, revision_no))");
    
    # The table 'timescale_refs' stores secondary references for timescales.
    
    $dbh->do("DROP TABLE IF EXISTS $TS_REFS_WORK");
    
    $dbh->do("CREATE TABLE $TS_REFS_WORK (
		timescale_no int unsigned not null,
		reference_no int unsigned not null,
		primary key (timescale_no, reference_no))");
    
    # The table 'timescale_ints' associates interval names with unique identifiers.
    
    $dbh->do("DROP TABLE IF EXISTS $TS_INTS_WORK");
    
    $dbh->do("CREATE TABLE $TS_INTS_WORK (
		interval_no int unsigned primary key,
		macrostrat_id int unsigned not null,
		interval_name varchar(80) not null,
		early_age decimal(9,5),
		late_age decimal(9,5),
		abbrev varchar(10) not null,
		orig_early decimal(9,5),
		orig_late decimal(9,5),
		orig_color varchar(10) not null,
		orig_refno int unsigned not null,
		macrostrat_color varchar(10) not null,
		KEY (macrostrat_id),
		KEY (interval_name))");
    
    # The table 'timescale_bounds' defines boundaries between intervals.
    
    $dbh->do("DROP TABLE IF EXISTS $TS_BOUNDS_WORK");
    
    $dbh->do("CREATE TABLE $TS_BOUNDS_WORK (
		bound_no int unsigned primary key auto_increment,
		timescale_no int unsigned not null,
		authorizer_no int unsigned not null,
		enterer_no int unsigned not null,
		bound_type enum('absolute', 'spike', 'same', 'percent', 'offset'),
		lower_no int unsigned not null,
		upper_no int unsigned not null,
		source_bound_no int unsigned,
		range_bound_no int unsigned,
		color_bound_no int unsigned,
		age decimal(9,5),
		age_error decimal(9,5),
		offset decimal(9,5),
		offset_error decimal(9,5),
		is_locked boolean not null,
		is_different boolean not null,
		color varchar(10) not null,
		reference_no int unsigned,
		derived_age decimal(9,5),
		derived_age_error decimal(9,5),
		derived_color varchar(10) not null,
		derived_reference_no int unsigned,
		created timestamp default current_timestamp,
		modified timestamp default current_timestamp,
		updated timestamp default current_timestamp on update current_timestamp,
		key (source_bound_no),
		key (range_bound_no),
		key (color_bound_no),
		key (derived_age),
		key (reference_no))");
    
    # The table 'timescale_perms' stores viewing and editing permission for timescales.
    
    $dbh->do("DROP TABLE IF EXISTS $TS_PERMS_WORK");
    
    $dbh->do("CREATE TABLE $TS_PERMS_WORK (
		timescale_no int unsigned not null,
		person_no int unsigned,
		group_no int unsigned,
		access enum ('none', 'view', 'edit'),
		key (timescale_no),
		key (person_no),
		key (group_no))");
    
    activateTables($dbh, $TIMESCALE_WORK => $TIMESCALE_DATA,
		         $TS_REFS_WORK => $TIMESCALE_REFS,
			 $TS_INTS_WORK => $TIMESCALE_INTS,
			 $TS_BOUNDS_WORK => $TIMESCALE_BOUNDS,
			 $TS_PERMS_WORK => $TIMESCALE_PERMS);
}


# initFromOldIntervals ( dbh )
# 
# Initialize the new tables from the old set of timescale intervals.

sub copyOldTimescales {

    my ($dbh, $options) = @_;
    
    $options ||= { };
    
    my $authorizer_no = $options->{authorizer_no} || 0;
    my $auth_quoted = $dbh->quote($authorizer_no);
    my ($sql, $result);
    
    # First establish the international timescales.
    
    $sql = "REPLACE INTO $TIMESCALE_DATA (timescale_no, authorizer_no, timescale_name,
	interval_type, is_active) VALUES
	(5, $auth_quoted, 'International Chronostratigraphic Eons', 'eon', 1),
	(4, $auth_quoted, 'International Chronostratigraphic Eras', 'era', 1),
	(3, $auth_quoted, 'Internatioanl Chronostratigraphic Periods', 'period', 1),
	(2, $auth_quoted, 'International Chronostratigraphic Epochs', 'epoch', 1),
	(1, $auth_quoted, 'International Chronostratigraphic Stages', 'stage', 1)";
    
    $result = $dbh->do($sql);
    
    # Then copy the interval data from the old tables for scale_no 1.
    
    $sql = "REPLACE INTO $TIMESCALE_INTS (interval_no, interval_name, abbrev,
		orig_early, orig_late, orig_color, orig_refno)
	SELECT i.interval_no, i.interval_name, i.abbrev, i.early_age, i.late_age, 
		sm.color, i.reference_no
	FROM interval_data as i join scale_map as sm using (interval_no)
	WHERE scale_no = 1
	GROUP BY interval_no";
    
    $result = $dbh->do($sql);
    
    # Then copy the interval data from macrostrat. Override any intervals that are already
    # in the table (by name) and add any others that are fond.
    
    $sql = "UPDATE $TIMESCALE_INTS as i join $MACROSTRAT_INTERVALS as msi using (interval_name)
	SET i.orig_early = msi.age_bottom, i.orig_late = msi.age_top,
	    i.macrostrat_id = msi.id,
	    i.orig_color = msi.orig_color, i.macrostrat_color = msi.interval_color";
    
    $result = $dbh->do($sql);
    
    $sql = "INSERT INTO $TIMESCALE_INTS (macrostrat_id, interval_name, abbrev, orig_early, orig_late,
	    orig_color, macrostrat_color)
	SELECT msi.id, msi.interval_name, msi.interval_abbrev, msi.age_bottom, msi.age_top,
		msi.interval_color, msi.orig_color
	FROM $MACROSTRAT_INTERVALS as msi join $TIMESCALE_INTS as i using (interval_name)
	WHERE i.interval_name is null";
    
    $result = $dbh->do($sql);
    
    # Then we need to establish the bounds for each timescale.
    
    $sql = "TRUNCATE TABLE $TIMESCALE_BOUNDS";
    
    $result = $dbh->do($sql);
    
    foreach my $level_no (reverse 1..5)
    {
	my $timescale_no = 6 - $level_no;
	
	$sql = "INSERT INTO $TIMESCALE_BOUNDS (timescale_no, authorizer_no, enterer_no, 
			bound_type, lower_no, upper_no, age, color, reference_no)
	SELECT $timescale_no as timescale_no, $auth_quoted as authorizer_no, $auth_quoted as enterer_no,
			'spike' as bound_type, lower_no, upper_no, age, color, orig_refno
	FROM
	((SELECT null as lower_no, null as lower_name, i1.orig_early as age, i1.interval_name as upper_name, 
		i1.interval_no as upper_no, 
		if(i1.macrostrat_color <> '', i1.macrostrat_color, i1.orig_color) as color, i1.orig_refno
	FROM scale_map as sm1 join $TIMESCALE_INTS as i1 using (interval_no)
	WHERE sm1.scale_level = $level_no ORDER BY i1.orig_early desc LIMIT 1)
	UNION
	(SELECT i1.interval_no as lower_no, i1.interval_name as lower_name, i2.orig_early as age, 
		i2.interval_name as upper_name, i2.interval_no as upper_no,
		if(i2.macrostrat_color <> '', i2.macrostrat_color, i2.orig_color) as color, i2.orig_refno
	FROM scale_map as sm1 join scale_map as sm2 on (sm1.scale_no = sm2.scale_no and sm1.scale_level = sm2.scale_level)
		join $TIMESCALE_INTS as i1 on i1.interval_no = sm1.interval_no
		join $TIMESCALE_INTS as i2 on i2.interval_no = sm2.interval_no
	WHERE (i1.orig_late = i2.orig_early) and sm1.scale_level = $level_no GROUP BY i1.interval_no)
	UNION
	(SELECT i1.interval_no as lower_no, i1.interval_name as lower_name, i1.orig_late as age,
		null as upper_name, null as upper_no, null as color, null as orig_refno
	FROM scale_map as sm1 join $TIMESCALE_INTS as i1 using (interval_no)
	WHERE sm1.scale_level = $level_no ORDER BY i1.orig_late asc LIMIT 1)
	ORDER BY age asc) as innerquery";
	
	print "$sql\n\n" if $options->{debug};
	
	$dbh->do($sql);
	
	update_timescale_attrs($dbh, $timescale_no);
    }
    
    # Finally, we knit pieces of these together into a single timescale, for demonstration
    # purposes. 
    
    my $test_timescale_no = 10;
    
    $sql = "REPLACE INTO $TIMESCALE_DATA (timescale_no, authorizer_no, timescale_name,
	is_active) VALUES
	($test_timescale_no, $auth_quoted, 'Test timescale using international intervals', 1)";
    
    $dbh->do($sql);
    
    my @boundaries;
    
    add_timescale_chunk($dbh, \@boundaries, 1);
    add_timescale_chunk($dbh, \@boundaries, 3);
    add_timescale_chunk($dbh, \@boundaries, 4);
    add_timescale_chunk($dbh, \@boundaries, 5);
    
    set_timescale_boundaries($dbh, $test_timescale_no, \@boundaries, $authorizer_no);
    update_timescale_attrs($dbh, $test_timescale_no);
    
    # Now check each of these new timescales to make sure there are no gaps. This will also let us
    # set the bottom and top bounds on each timescale.
    
 TIMESCALE:
    foreach my $timescale_no (1..5, $test_timescale_no)
    {
	$sql = "SELECT bound_no, age, lower_no, lower.interval_name as lower_name,
			upper_no, upper.interval_name as upper_name
		FROM $TIMESCALE_BOUNDS as tsb
			left join $TIMESCALE_INTS as lower on lower.interval_no = tsb.lower_no
			left join $TIMESCALE_INTS as upper on upper.interval_no = tsb.upper_no
		WHERE timescale_no = $timescale_no";
	
	my ($results) = $dbh->selectall_arrayref($sql, { Slice => { } });
	my (@results);
	
	@results = @$results if ref $results eq 'ARRAY';
	
	my (@errors);
	
	# Make sure that we actually have some results.
	
	unless ( @results )
	{
	    push @errors, "No boundaries found";
	    next TIMESCALE;
	}
	
	# Make sure that the first and last intervals have the correct properties.
	
	if ( $results[0]{upper_no} )
	{
	    my $bound_no = $results[0]{bound_no};
	    push @errors, "Error in bound $bound_no: should be upper boundary but has upper_no = $results[0]{upper_no}";
	}
	
	if ( $results[-1]{lower_no} )
	{
	    my $bound_no = $results[-1]{bound_no};
	    push @errors, "Error in bound $bound_no: should be lower boundary but has lower_no = $results[-1]{lower_no}";
	}
	
	# Then check all of the boundaries in sequence.
	
	my ($early_age, $late_age, $last_age, $last_lower_no);
	my $boundary_count = 0;
	
	$results[-1]{last_record} = 1;
	
	foreach my $r (@results)
	{
	    my $bound_no = $r->{bound_no};
	    my $age = $r->{age};
	    my $upper_no = $r->{upper_no};
	    my $lower_no = $r->{lower_no};
	    
	    $boundary_count++;
	    
	    # The first age will be the late end of the scale, the last age will be the early end.
	    
	    $late_age //= $age;
	    $early_age = $age;
	    
	    # Make sure the ages are all defined and monotonic.
	    
	    unless ( defined $age )
	    {
		push @errors, "Error in bound $bound_no: age is not defined";
	    }
	    
	    if ( defined $last_age && $last_age >= $age )
	    {
		push @errors, "Error in bound $bound_no: age ($age) >= last age ($last_age)";
	    }
	    
	    # Make sure that the upper_no matches the lower_no of the previous
	    # record.
	    
	    if ( defined $last_lower_no )
	    {
		unless ( $upper_no )
		{
		    push @errors, "Error in bound $bound_no: upper_no not defined";
		}
		
		elsif ( $upper_no ne $last_lower_no )
		{
		    push @errors, "Error in bound $bound_no: upper_no ($upper_no) does not match upward ($last_lower_no)";
		}
	    }
	    
	    $last_lower_no = $lower_no;
	    
	    unless ( $lower_no || $r->{last_record} )
	    {
		push @errors, "Error in bound $bound_no: lower_no not defined";
	    }
	}
	
	# Now report.
	
	print "\nTimescale $timescale_no: $boundary_count boundaries from $early_age Ma to $late_age Ma\n";
	
	foreach my $e (@errors)
	{
	    print "    $e\n";
	}
	
	if ( $options->{verbose} && $options->{verbose} > 2 )
	{
	    print "\n";
	    
	    foreach my $r (@results)
	    {
		my $name = $r->{upper_name} || "TOP";
		my $interval_no = $r->{upper_no};
		
		printf "  %-20s%s\n", $r->{age}, "$name ($interval_no)";
	    }
	}
	
	# Then set the early and late age for the timescale.
	
	# $sql = "UPDATE $TIMESCALE_DATA SET early_age = $early_age, late_age = $late_age
	# 	WHERE timescale_no = $level_no";
	
	# $result = $dbh->do($sql);
    }
    
    print "\n\n";
}


# add_timescale_chunk ( timescale_dest, timescale_source, last_boundary_age )
# 
# Add boundaries to the destination timescale, which refer to boundaries in the source timescale.
# Add only boundaries earlier than $last_boundary_age, and return the age of the last boundary
# added.
# 
# This routine is meant to be used in sequence to knit together a timescale with chunks from a
# variety of source timescales. It should be called most recent -> least recent.

sub add_timescale_chunk {

    my ($dbh, $boundary_list, $source_no, $early_bound, $late_bound) = @_;
    
    my ($sql, $result);
    
    # First get a list of boundaries from the specified timescale, restricted according to the
    # specified bounds.
    
    my $source_quoted = $dbh->quote($source_no);
    my @filters = "timescale_no = $source_quoted";
    
    if ( $early_bound )
    {
	my $quoted = $dbh->quote($early_bound);
	push @filters, "age <= $quoted";
    }
    
    if ( @$boundary_list && $boundary_list->[-1]{age} )
    {
	$late_bound = $boundary_list->[-1]{age} + 0.1 if ! defined $late_bound || $boundary_list->[-1]{age} >= $late_bound;
    }
    
    if ( $late_bound )
    {
	my $quoted = $dbh->quote($late_bound);
	push @filters, "age >= $quoted";
    }
    
    my $filter = "";
    
    if ( @filters )
    {
	$filter = "WHERE " . join( ' and ', @filters );
    }
    
    $sql = "SELECT bound_no, age, lower_no, upper_no
	    FROM $TIMESCALE_BOUNDS $filter ORDER BY age asc";
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
    my @results;
    
    @results = @$result if ref $result eq 'ARRAY';
    
    # If we have no results, do nothing.
    
    return unless @results;
    
    # If the top boundary has no upper_no, and the list to which we are adding has at least one
    # member, then remove it.
    
    if ( @$boundary_list && ! $results[0]{upper_no} )
    {
	shift @results;
    }
    
    # Now tie the two ranges together.
    
    if ( @$boundary_list )
    {
	$boundary_list->[-1]{lower_no} = $results[0]{upper_no};
    }
    
    # Alter each record so that it is indicated as a copy of the specified bound.
    
    foreach my $b (@results)
    {
	$b->{bound_type} = 'same';
    }
    
    # Then add the new results on to the list.
    
    push @$boundary_list, @results;
}


# set_timescale_boundaries ( dbh, timescale_no, boundary_list )
# 
# 

sub set_timescale_boundaries {
    
    my ($dbh, $timescale_no, $boundary_list, $authorizer_no) = @_;
    
    my $result;
    my $sql = "INSERT INTO $TIMESCALE_BOUNDS (timescale_no, authorizer_no, enterer_no,
		bound_type, lower_no, upper_no, source_bound_no, age) VALUES ";
    
    my @values;
    
    my $ts_quoted = $dbh->quote($timescale_no);
    my $auth_quoted = $dbh->quote($authorizer_no);
    
    foreach my $b (@$boundary_list)
    {
	my $lower_quoted = $dbh->quote($b->{lower_no});
	my $upper_quoted = $dbh->quote($b->{upper_no});
	my $source_quoted = $dbh->quote($b->{source_bound_no} // $b->{bound_no});
	my $age_quoted = $dbh->quote($b->{age});
	
	push @values, "($ts_quoted, $auth_quoted, $auth_quoted, 'same', $lower_quoted, " .
	    "$upper_quoted, $source_quoted, $age_quoted)";
    }
    
    $sql .= join( q{, } , @values );
    
    $result = $dbh->do($sql);
}


# update_timescale_attrs ( dbh, timescale_no )
# 
# Make sure that the attributes of the specified timescale are consistent with the boundaries it
# contains. If no value is given for $timescale_no, then update all timescales. If the value is 0,
# then do nothing.

sub update_timescale_attrs {
    
    my ($dbh, $timescale_no) = @_;
    
    return if defined $timescale_no && $timescale_no == 0;
    
    my $filter = "";
    $filter = "WHERE timescale_no = " . $dbh->quote($timescale_no) if defined $timescale_no;
    
    my $result;
    my $sql = "UPDATE $TIMESCALE_DATA as t join 
		(SELECT timescale_no, max(b.age) as early_age, min(b.age) as late_age FROM timescale_bounds as b
		$filter GROUP BY timescale_no) as bb using (timescale_no)
	SET t.early_age = bb.early_age, t.late_age = bb.late_age";
    
    $result = $dbh->do($sql);
}


# update_boundary_attrs ( dbh, timescale_no )
# 
# Make sure that the attributes of the specified bounaries are consistent with their source
# boundaries, if any. If n o value is given for timescale_no, then update all boundaries. If the
# value is 0, then do nothing. Otherwise, update all boundaries.

sub update_boundary_attrs {
    
    my ($dbh, $timescale_no) = @_;
    
    # If no value is given for $timescale_no, just call propagate_boudary_changes( ) to update any
    # changes to the boundaries.
    
    if ( ! defined $timescale_no )
    {
	return propagate_boundary_changes($dbh);
    }
    
    # If a value of 0 is given, do nothing.
    
    elsif ( $timescale_no == 0 )
    {
	return;
    }
    
    # Otherwise, update just the boundaries in the specified timescale.
    
    my ($result, $sql);
    
    my $age_update_count = 0;
    my $color_update_count = 0;
    my $ts_quoted = $dbh->quote($timescale_no);
    
    # We start by figuring out how many boundaries of various types we have.
    
    $sql = "SELECT min(source_bound_no) as has_source, min(range_bound_no) as has_range,
		min(color_bound_no) as has_color
	FROM $TIMESCALE_BOUNDS WHERE timescale_no = $ts_quoted";
    
    my ($has_source, $has_range, $has_color) = $dbh->selectrow_array($sql);
    
    # Unless at least one of those is greater than zero, there is nothing to do.
    
    unless ( $has_source || $has_range || $has_color )
    {
	return;
    }
    
    # If we get here, then we have some work to do. We start by recomputing ages for 'relative' boundaries.
    
    if ( $has_range )
    {
	# $$$ derived_age_error needs a more sophisticated calculation, taking into account both
	# the source age error and the offset error.
	
	$sql = "UPDATE $TIMESCALE_BOUNDS as b
			join $TIMESCALE_BOUNDS as bottom on base.bound_no = b.source_bound_no
			join $TIMESCALE_BOUNDS as top on top.bound_no = b.range_bound_no
		SET b.derived_age = bottom.age - (bottom.age - top.age) * b.offset,
		    b.derived_age_error = (bottom.age - top.age) * b.offset_error,
		    b.derived_reference_no = null,
		    b.derived_color = bottom.color
		WHERE timescale_no = $ts_quoted and bound_type = 'percent'";
	
	$age_update_count += $dbh->do($sql);
    }
    
    # Then compute ages for 'same' and 'offset' boundaries.
    
    if ( $has_source )
    {
	# $$$ derived_age_error needs a more sophisticated calculation when boundary type is
	# 'offset', taking into account both the source age error and the offset error.
	
	$sql = "UPDATE $TIMESCALE_BOUNDS as b
			join $TIMESCALE_BOUNDS as source on source.bound_no = b.source_bound_no
		SET b.derived_age = source.age - ifnull(b.offset, 0),
		    b.derived_age_error = if(b.bound_type = 'same', source.age_error, b.offset_error),
		    b.derived_reference_no = if(b.bound_type = 'same', source.reference_no, null)
		    b.derived_color = source.color
		WHERE timescale_no = $ts_quoted and bound_type in ('same', 'offset')";
	
	$age_update_count += $dbh->do($sql);
    }
    
    # If we have any boundaries that take their color from a different boundary, update those now.
    
    if ( $has_color )
    {
	$sql = "UPDATE $TIMESCALE_BOUNDS as b
			join $TIMESCALE_BOUNDS as source on source.bound_no = b.color_bound_no
		SET b.derived_color = source.color
		WHERE timescale_no = $ts_quoted";
	
	$color_update_count += $dbh->do($sql);
    }
    
    # Now recompute the ages from the derived ages for any interval that is not locked. For any
    # interval that is locked, set the 'is_different' flag if the derived age is different from
    # the locked age.
    
    if ( $age_update_count )
    {
	$sql = "UPDATE $TIMESCALE_BOUNDS as b
		SET b.age = if(b.is_locked, b.age, b.derived_age),
		    b.age_error = if(b.is_locked, b.age_error, b.derived_age_error),
		    b.is_different = b.is_locked and b.age <> b.derived_age
		WHERE timescale_no = $ts_quoted and bound_type in ('same', 'percent', 'offset')";
	
	$result = $dbh->do($sql);
    }
}

1;
