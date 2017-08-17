# 
# The Paleobiology Database
# 
#   TimescaleTables.pm
# 

package TimescaleTables;

use strict;

use Carp qw(carp croak);
use Try::Tiny;

use TableDefs qw($TIMESCALE_DATA $TIMESCALE_REFS $TIMESCALE_INTS $TIMESCALE_BOUNDS
		 $TIMESCALE_PERMS
	         $INTERVAL_DATA $INTERVAL_MAP $SCALE_MAP $MACROSTRAT_SCALES $MACROSTRAT_INTERVALS
	         $MACROSTRAT_SCALES_INTS $REFERENCES);
use CoreFunction qw(activateTables loadSQLFile);
use ConsoleLog qw(logMessage);

use base 'Exporter';

our(@EXPORT_OK) = qw(establish_timescale_tables copy_international_timescales
		   copy_pbdb_timescales copy_macrostrat_timescales process_one_timescale
		   update_timescale_descriptions create_triggers
		   %TIMESCALE_BOUND_ATTRS %TIMESCALE_ATTRS %TIMESCALE_REFDEF);


our (%TIMESCALE_ATTRS) = 
    ( timescale_id => 'IGNORE',
      timescale_name => 'varchar80',
      timescale_type => { eon => 1, era => 1, period => 1, epoch => 1, stage => 1,
			  substage => 1, zone => 1, other => 1, multi => 1 },
      timescale_extent => 'varchar80',
      timescale_taxon => 'varchar80',
      authority_level => 'range0-255',
      source_timescale_id => 'timescale_no',
      reference_id => 'reference_no',
      is_active => 'boolean' );
    
our (%TIMESCALE_BOUND_ATTRS) = 
    ( bound_id => 'IGNORE',
      bound_type => { absolute => 1, spike => 1, same => 1, range => 1, offset => 1 },
      interval_type => { eon => 1, era => 1, period => 1, epoch => 1, stage => 1,
			 substage => 1, zone => 1, other => 1 },
      interval_extent => 'varchar80',
      interval_taxon => 'varchar80',
      timescale_id => 'timescale_no',
      is_locked => 'boolean',
      interval_id => 'interval_no',
      lower_id => 'interval_no',
      interval_name => 'varchar80',
      lower_name => 'varchar80',
      base_id => 'bound_no',
      range_id => 'bound_no',
      color_id => 'bound_no',
      refsource_id => 'bound_no',
      age => 'pos_decimal',
      age_error => 'pos_decimal',
      offset => 'pos_decimal',
      offset_error => 'pos_decimal',
      color => 'colorhex',
      reference_no => 'reference_no' );

our (%TIMESCALE_REFDEF) = 
    ( timescale_no => [ 'TSC', $TIMESCALE_DATA, 'timescale' ],
      interval_no => [ 'INT', $TIMESCALE_INTS, 'interval' ],
      bound_no => [ 'BND', $TIMESCALE_BOUNDS, 'interval boundary' ],
      reference_no => [ 'REF', $REFERENCES, 'bibliographic reference' ] );
    
    
# Table and file names

our $TIMESCALE_WORK = 'tsw';
our $TS_REFS_WORK = 'tsrw';
our $TS_INTS_WORK = 'tsiw';
our $TS_BOUNDS_WORK = 'tsbw';
our $TS_PERMS_WORK = 'tspw';
our $TS_QUEUE_WORK = 'tsqw';

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

sub establish_timescale_tables {
    
    my ($dbh, $options) = @_;
    
    $options ||= { };
    
    # First create the table 'timescales'.  This stores information about each timescale.
    
    $dbh->do("DROP TABLE IF EXISTS $TIMESCALE_WORK");
    
    $dbh->do("CREATE TABLE $TIMESCALE_WORK (
		timescale_no int unsigned primary key auto_increment,
		authorizer_no int unsigned not null,
		enterer_no int unsigned not null,
		modifier_no int unsigned not null,
		pbdb_id int unsigned not null,
		macrostrat_id int unsigned not null,
		timescale_name varchar(80) not null,
		source_timescale_no int unsigned not null,
		authority_level tinyint unsigned not null,
		min_age decimal(9,5),
		max_age decimal(9,5),
		reference_no int unsigned not null,
		timescale_type enum('eon', 'era', 'period', 'epoch', 'stage', 'substage', 
			'zone', 'other', 'multi') not null,
		timescale_extent varchar(80) not null,
		timescale_taxon varchar(80) not null,
		is_active boolean,
		is_updated boolean,
		is_error boolean,
		is_locked boolean,
		created timestamp default current_timestamp,
		modified timestamp default current_timestamp,
		updated timestamp default current_timestamp on update current_timestamp,
		key (reference_no),
		key (authorizer_no),
		key (is_active),
		key (is_updated))");
    
    # $dbh->do("DROP TABLE IF EXISTS $TIMESCALE_ARCHIVE");
    
    # $dbh->do("CREATE TABLE $TIMESCALE_ARCHIVE (
    # 		timescale_no int unsigned,
    # 		revision_no int unsigned auto_increment,
    # 		authorizer_no int unsigned not null,
    # 		timescale_name varchar(80) not null,
    # 		source_timescale_no int unsigned,
    # 		max_age decimal(9,5),
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
		color varchar(10) not null,
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
		modifier_no int unsigned not null,
		bound_type enum('absolute', 'spike', 'same', 'range', 'offset', 'modeled'),
		interval_extent varchar(80) not null,
		interval_taxon varchar(80) not null,
		interval_type enum('eon', 'era', 'period', 'epoch', 'stage', 'substage', 'zone', 'other') not null,
		interval_no int unsigned not null,
		lower_no int unsigned not null,
		base_no int unsigned,
		range_no int unsigned,
		color_no int unsigned,
		refsource_no int unsigned,
		age decimal(9,5),
		age_error decimal(9,5),
		offset decimal(9,5),
		offset_error decimal(9,5),
		is_error boolean not null,
		is_updated boolean not null,
		is_locked boolean not null,
		is_different boolean not null,
		color varchar(10) not null,
		reference_no int unsigned,
		derived_age decimal(9,5),
		derived_age_error decimal(9,5),
		derived_color varchar(10),
		derived_reference_no int unsigned,
		created timestamp default current_timestamp,
		modified timestamp default current_timestamp,
		updated timestamp default current_timestamp on update current_timestamp,
		key (timescale_no),
		key (base_no),
		key (range_no),
		key (color_no),
		key (age),
		key (reference_no),
		key (is_updated))");
    
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



# copy_international_timescales ( dbh, options )
# 
# Copy into the new tables the old set of timescales and intervals corresponding to the standard
# international timescale.

sub copy_international_timescales {

    my ($dbh, $options) = @_;
    
    $options ||= { };
    
    my $authorizer_no = $options->{authorizer_no} || 0;
    my $auth_quoted = $dbh->quote($authorizer_no);
    my ($sql, $result);
    
    # First establish the international timescales.
    
    $sql = "REPLACE INTO $TIMESCALE_DATA (timescale_no, authorizer_no, enterer_no, timescale_name,
	timescale_type, timescale_extent, authority_level, is_active) VALUES
	(5, $auth_quoted, $auth_quoted, 'International Chronostratigraphic Eons', 'eon', 'international', 5, 1),
	(4, $auth_quoted, $auth_quoted, 'International Chronostratigraphic Eras', 'era', 'international', 5, 1),
	(3, $auth_quoted, $auth_quoted, 'Internatioanl Chronostratigraphic Periods', 'period', 'international', 5, 1),
	(2, $auth_quoted, $auth_quoted, 'International Chronostratigraphic Epochs', 'epoch', 'international', 5, 1),
	(1, $auth_quoted, $auth_quoted, 'International Chronostratigraphic Stages', 'stage', 'international', 5, 1)";
    
    $result = $dbh->do($sql);
    
    # Then copy the interval data from the old tables for scale_no 1.
    
    $sql = "REPLACE INTO $TIMESCALE_INTS (interval_no, interval_name, abbrev,
		orig_early, orig_late, orig_color, orig_refno)
	SELECT i.interval_no, i.interval_name, i.abbrev, i.early_age, i.late_age, 
		sm.color, i.reference_no
	FROM $INTERVAL_DATA as i join scale_map as sm using (interval_no)
	WHERE scale_no = 1 and i.interval_no < 3000
	GROUP BY interval_no";
    
    $result = $dbh->do($sql);
    
    logMessage(1, "Inserted 5 timescales and $result intervals from PBDB");
    
    # For every interval in macrostrat whose name matches one already in the table, overwrite its
    # attributes with the attributes from macrostrat.
    
    $sql = "UPDATE $TIMESCALE_INTS as i join $MACROSTRAT_INTERVALS as msi using (interval_name)
	SET i.orig_early = msi.age_bottom, i.orig_late = msi.age_top,
	    i.macrostrat_id = msi.id,
	    i.orig_color = msi.orig_color, i.macrostrat_color = msi.interval_color";
    
    $result = $dbh->do($sql);
    
    logMessage(1, "Updated $result intervals with data from Macrostrat");
    
    # Then we need to establish the bounds for each timescale.
    
    $sql = "TRUNCATE TABLE $TIMESCALE_BOUNDS";
    
    $result = $dbh->do($sql);
    
    foreach my $level_no (reverse 1..5)
    {
	my $timescale_no = 6 - $level_no;
	
	$sql = "INSERT INTO $TIMESCALE_BOUNDS (timescale_no, authorizer_no, enterer_no, 
			bound_type, lower_no, interval_no, age, derived_age, color, reference_no)
	SELECT $timescale_no as timescale_no, $auth_quoted as authorizer_no, $auth_quoted as enterer_no,
			'spike' as bound_type, lower_no, interval_no, age, age as derived_age, color, orig_refno
	FROM
	((SELECT null as lower_no, null as lower_name, i1.orig_early as age, i1.interval_name, i1.interval_no,
		if(i1.macrostrat_color <> '', i1.macrostrat_color, i1.orig_color) as color, i1.orig_refno
	FROM scale_map as sm1 join $TIMESCALE_INTS as i1 using (interval_no)
	WHERE sm1.scale_level = $level_no ORDER BY i1.orig_early desc LIMIT 1)
	UNION
	(SELECT i1.interval_no as lower_no, i1.interval_name as lower_name, i2.orig_early as age,
		i2.interval_name, i2.interval_no,
		if(i2.macrostrat_color <> '', i2.macrostrat_color, i2.orig_color) as color, i2.orig_refno
	FROM scale_map as sm1 join scale_map as sm2 on (sm1.scale_no = sm2.scale_no and sm1.scale_level = sm2.scale_level)
		join $TIMESCALE_INTS as i1 on i1.interval_no = sm1.interval_no
		join $TIMESCALE_INTS as i2 on i2.interval_no = sm2.interval_no
	WHERE (i1.orig_late = i2.orig_early) and sm1.scale_level = $level_no GROUP BY i1.interval_no)
	UNION
	(SELECT i1.interval_no as lower_no, i1.interval_name as lower_name, i1.orig_late as age,
		null as interval_name, null as interval_no, null as color, null as orig_refno
	FROM scale_map as sm1 join $TIMESCALE_INTS as i1 using (interval_no)
	WHERE sm1.scale_level = $level_no ORDER BY i1.orig_late asc LIMIT 1)
	ORDER BY age asc) as innerquery";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$dbh->do($sql);
	
	update_timescale_attrs($dbh, $timescale_no);
    }
    
    # Correct bad interval numbers from PBDB
    
    $sql = "UPDATE $TIMESCALE_BOUNDS SET lower_no = if(lower_no = 3002, 32, if(lower_no = 3001, 59, lower_no))";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    $sql = "UPDATE $TIMESCALE_BOUNDS SET interval_no = if(interval_no = 3002, 32, if(interval_no = 3001, 59, interval_no))";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $result += $dbh->do($sql);
    
    logMessage(2, "    Updated $result bad interval numbers from PBDB\n") if $result and $result > 0;
    
    # Set the other interval attributes, which are attached to the lower bound
    # record. 
    
    $sql = "UPDATE $TIMESCALE_BOUNDS as tsb join $TIMESCALE_DATA as ts using (timescale_no)
		SET tsb.interval_type = ts.timescale_type, tsb.interval_extent = ts.timescale_extent
		WHERE timescale_no in (1,2,3,4,5)";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(2, "    Initialized the attributes of $result bounds");
    
    # Then we create links between same-age bounds across these different
    # scales. 
    
    my %ages;
    
    foreach my $timescale_no ( 4,3,2,1 )
    {
	my $source_no = $timescale_no + 1;
	
	$sql = "UPDATE $TIMESCALE_BOUNDS as tsb join $TIMESCALE_BOUNDS as source on tsb.age = source.age
		SET tsb.bound_type = 'same', tsb.base_no = source.bound_no,
		    tsb.derived_age = source.age
		WHERE tsb.timescale_no = $timescale_no and source.timescale_no = $source_no";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$result = $dbh->do($sql);
	
	logMessage(2, "    Linked up $result bounds from timescale $timescale_no as 'same'");
    }
    
    # Finally, we knit pieces of these together into a single timescale, for demonstration
    # purposes. 
    
    my $test_timescale_no = 10;
    
    $sql = "REPLACE INTO $TIMESCALE_DATA (timescale_no, authorizer_no, timescale_name,
	is_active, timescale_type, timescale_extent) VALUES
	($test_timescale_no, $auth_quoted, 'Test timescale using international intervals', 1,
	 'multi', 'international')";
    
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
	my @bounds;
	
	# Check the timescale.
	
	my @errors = check_timescale_integrity($dbh, $timescale_no, \@bounds);
	
	# Then report.
	
	my $boundary_count = scalar(@bounds);
	my $early_age = $bounds[0]{age};
	my $late_age = $bounds[-1]{age};
	
	logMessage(1, "");
	logMessage(1, "Timescale $timescale_no: $boundary_count boundaries from $early_age Ma to $late_age Ma");
	
	foreach my $e (@errors)
	{
	    logMessage(2, "    $e");
	}
	
	if ( $options->{verbose} && $options->{verbose} > 2 )
	{
	    logMessage(3, "");
	    
	    foreach my $r (@bounds)
	    {
		my $name = $r->{interval_name} || "TOP";
		my $interval_no = $r->{interval_no};
		
		logMessage(3, sprintf("  %-20s%s", $r->{age}, "$name ($interval_no)"));
	    }
	}
    }
    
    logMessage(1, "done.");
}


# copy_pbdb_timescales ( dbh, options )
# 
# Copy into the new tables the old set of timescales and intervals from the pbdb (other than the
# international ones).

sub copy_pbdb_timescales {

    my ($dbh, $options) = @_;
    
    $options ||= { };
    
    my ($sql, $result);
    
    # First copy the timescales from the PBDB.
    
    $sql = "REPLACE INTO $TIMESCALE_DATA (timescale_no, reference_no, pbdb_id, timescale_name,
		timescale_type, is_active, authorizer_no, enterer_no, modifier_no, created, modified)
	SELECT scale_no + 100, reference_no, scale_no, scale_name, null, 0,
		authorizer_no, enterer_no, modifier_no, created, modified
	FROM scales";
    
    print STDERR "\n$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    # Then get ourselves a list of what we just copied.
    
    $sql = "SELECT scale_no + 100 as timescale_no, scale_no as pbdb_id, scale_name
	FROM scales";
    
    my $timescale_list = $dbh->selectall_arrayref($sql, { Slice => {} });
    my $timescale_count = scalar(@$timescale_list);
    
    logMessage(1, "Inserted $timescale_count timescales from the PBDB");
    
    # Then copy the intervals from the PBDB. But skip the ones from scale 1, which were already
    # copied by 'copy_international_timescales' above.
    
    $sql = "INSERT IGNORE INTO $TIMESCALE_INTS (interval_no, interval_name, abbrev,
		orig_early, orig_late, orig_refno)
	SELECT interval_no, interval_name, abbrev, early_age, late_age, reference_no
	FROM $INTERVAL_DATA as i left join scale_map as sm using (interval_no)
	WHERE scale_no is null";
    
    print STDERR "\n$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(1, "Inserted $result intervals from the PBDB");
    
    # Now go through the timescales we just copied, one by one.
    
 TIMESCALE:
    foreach my $t ( @$timescale_list )
    {
	next unless $t->{timescale_no} && $t->{pbdb_id};
	
	process_one_timescale($dbh, $t->{timescale_no}, $options);
    }
}


sub process_one_timescale {
    
    my ($dbh, $timescale_no, $options) = @_;
    
    $options ||= { };
    
    my ($sql, $result);
    
    my ($timescale_name, $macrostrat_id, $pbdb_id) = $dbh->selectrow_array("
	SELECT timescale_name, macrostrat_id, pbdb_id FROM $TIMESCALE_DATA
	WHERE timescale_no = $timescale_no");
    
    if ( $pbdb_id )
    {
	logMessage(1, "Processing $timescale_name ($timescale_no from PBDB $pbdb_id)");
    }
    
    elsif ( $macrostrat_id )
    {
	logMessage(1, "Processing $timescale_name ($timescale_no from Macrostrat $macrostrat_id)");
    }
    
    else
    {
	logMessage(1, "Processing $timescale_name ($timescale_no source unknown)");
    }
    
    # Delete any bounds that are already in the table.
    
    $sql = "DELETE FROM $TIMESCALE_BOUNDS WHERE timescale_no = $timescale_no";
    
    print STDERR "\n$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(2, "    deleted $result old bounds") if $result && $result > 0;
    
    # Then get all of the intervals for this timescale, in order from oldest to youngest
    
    if ( $pbdb_id )
    {
	$sql = "SELECT interval_no, i.base_age, i.top_age, c.authorizer_no, c.reference_no, ir.interval_name
		FROM interval_lookup as i join correlations as c using (interval_no)
			join intervals as ir using (interval_no)
		WHERE scale_no = $pbdb_id
		ORDER BY i.base_age desc, i.top_age desc";
	
	print STDERR "\n$sql\n\n" if $options->{debug};
    }
    
    elsif ( $macrostrat_id )
    {
	$sql = "SELECT tsi.interval_no, i.age_bottom as base_age, i.age_top as top_age,
			0 as authorizer_no, 0 as reference_no, i.interval_name
		FROM $MACROSTRAT_SCALES_INTS as im
			join $MACROSTRAT_INTERVALS as i on i.id = im.interval_id
			join $TIMESCALE_INTS as tsi on tsi.macrostrat_id = im.interval_id
		WHERE im.timescale_id = $macrostrat_id
		ORDER BY i.age_bottom desc, i.age_top desc";
	
	print STDERR "\n$sql\n\n" if $options->{debug};
    }
    
    else
    {
	logMessage(1, "    no source for this timescale");
	return;
    }
    
    my $interval_list = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    if ( ref $interval_list eq 'ARRAY' && @$interval_list )
    {
	my $interval_count = scalar(@$interval_list);
	logMessage(1, "    found $interval_count intervals");
    }
    
    else
    {
	logMessage(1, "    no intervals found for this timescale");
	return;
    }
    
    my ($first_early, $last_early, $last_late, $last_interval);
    
 INTERVAL:
    foreach my $i ( @$interval_list )
    {
	unless ( $last_early )
	{
	    my $reference_no = $i->{reference_no} || '0';
	    
	    $sql = "
		INSERT INTO $TIMESCALE_BOUNDS (timescale_no, authorizer_no, bound_type,
			lower_no, interval_no, age, derived_age, reference_no)
		VALUES ($timescale_no, $i->{authorizer_no}, 'absolute', 0,
			$i->{interval_no}, $i->{base_age}, $i->{base_age}, $reference_no)";
	    
	    $result = $dbh->do($sql);
	    
	    $first_early = $i->{base_age};
	    $last_early = $i->{base_age};
	    $last_late = $i->{top_age};
	    $last_interval = $i;
	    
	    next INTERVAL;
	}
	
	elsif ( $i->{base_age} == $last_late )
	{
	    $sql = "
		INSERT INTO $TIMESCALE_BOUNDS (timescale_no, authorizer_no, bound_type,
			lower_no, interval_no, age, derived_age, reference_no)
		VALUES ($timescale_no, $i->{authorizer_no}, 'absolute', $last_interval->{interval_no},
			$i->{interval_no}, $i->{base_age}, $i->{base_age}, $i->{reference_no})";
	    
	    $result = $dbh->do($sql);
	    
	    $last_early = $i->{base_age};
	    $last_late = $i->{top_age};
	    $last_interval = $i;
	    
	    next INTERVAL;
	}
	
	elsif ( $i->{base_age} == $last_early )
	{
	    logMessage(2, "  error: $i->{interval_name} ($i->{interval_no}) matches bottom of previous");
	    
	    $sql = "
		INSERT INTO $TIMESCALE_BOUNDS (timescale_no, authorizer_no, bound_type, is_error,
			lower_no, interval_no, age, derived_age, reference_no)
		VALUES ($timescale_no, $i->{authorizer_no}, 'absolute', 1, $last_interval->{interval_no},
			$i->{interval_no}, $i->{base_age}, $i->{base_age}, $i->{reference_no})";
	    
	    $result = $dbh->do($sql);
	    
	    next INTERVAL;
	}
	
	elsif ( $i->{top_age} == $last_late )
	{
	    logMessage(2, "  error: $i->{interval_name} ($i->{interval_no}) matches top of previous");
	    
	    $sql = "
		INSERT INTO $TIMESCALE_BOUNDS (timescale_no, authorizer_no, bound_type, is_error,
			lower_no, interval_no, age, derived_age, reference_no)
		VALUES ($timescale_no, $i->{authorizer_no}, 'absolute', 1, $last_interval->{interval_no},
			$i->{interval_no}, $i->{base_age}, $i->{base_age}, $i->{reference_no})";
	    
	    $result = $dbh->do($sql);
	    
	    next INTERVAL;
	}
	
	elsif ( $i->{base_age} > $last_late )
	{
	    logMessage(2, "  error: $i->{interval_name} ($i->{interval_no}) overlaps top");
	    
	    $sql = "
		INSERT INTO $TIMESCALE_BOUNDS (timescale_no, authorizer_no, bound_type, is_error,
			lower_no, interval_no, age, derived_age, reference_no)
		VALUES ($timescale_no, $i->{authorizer_no}, 'absolute', 1, $last_interval->{interval_no},
			$i->{interval_no}, $i->{base_age}, $i->{base_age}, $i->{reference_no})";
	    
	    $result = $dbh->do($sql);
	    
	    next INTERVAL;
	}
	
	else
	{
	    my $gap = $last_late - $i->{base_age};
	    logMessage(2, "  error: $i->{interval_name} ($i->{interval_no}) has a gap of $gap Ma");
	    
	    $sql = "
		INSERT INTO $TIMESCALE_BOUNDS (timescale_no, authorizer_no, bound_type,
			lower_no, interval_no, age, derived_age, reference_no)
		VALUES ($timescale_no, $i->{authorizer_no}, 'absolute', $last_interval->{interval_no},
			$i->{interval_no}, $i->{base_age}, $i->{base_age}, $i->{reference_no})";
	    
	    # print "\n$sql\n\n" if $options->{debug};
	    
	    $result = $dbh->do($sql);
	    
	    $last_early = $i->{base_age};
	    $last_late = $i->{top_age};
	    $last_interval = $i;	
	}
    }
    
    # Now we need to process the upper boundary of the last interval.
    
    $sql = "INSERT INTO $TIMESCALE_BOUNDS (timescale_no, authorizer_no, bound_type,
			lower_no, interval_no, age, derived_age, reference_no)
		VALUES ($timescale_no, $last_interval->{authorizer_no}, 'absolute',
			$last_interval->{interval_no}, 0, $last_late, $last_late, $last_interval->{reference_no})";
    
    $result = $dbh->do($sql);
    
    # Then update the timescale age range
    
    $sql = "UPDATE $TIMESCALE_DATA
		SET max_age = $first_early, min_age = $last_late
		WHERE timescale_no = $timescale_no";
    
    $result = $dbh->do($sql);
    
    # We can stop here when debugging
    
    my $a = 1;
}


sub copy_macrostrat_timescales {

    my ($dbh, $options) = @_;
    
    $options ||= { };
    
    my $authorizer_no = $options->{authorizer_no} || 0;
    my $auth_quoted = $dbh->quote($authorizer_no);
    
    my ($sql, $result);
    
    # First copy the timescales from Macrostrat. Ignore 1,2,3,13,14, which are the international
    # stages, epochs, etc. and have already been taken care of in 'copy_international_timescales'
    # above.
    
    my $skip_list = "1,2,3,11,13,14";
    
    $sql = "REPLACE INTO $TIMESCALE_DATA (timescale_no, reference_no, macrostrat_id, timescale_name,
		timescale_type, is_active, authorizer_no, enterer_no)
	SELECT id + 50, 0, id, timescale, null, 0, $auth_quoted as authorizer_no, $auth_quoted as enterer_no
	FROM $MACROSTRAT_SCALES
	WHERE id not in ($skip_list)";
    
    print STDERR "\n$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    # Then get ourselves a list of what we just copied.
    
    $sql = "SELECT id + 50 as timescale_no, id as macrostrat_id, timescale as timescale_name
	FROM $MACROSTRAT_SCALES
	WHERE id not in ($skip_list)";
    
    my $timescale_list = $dbh->selectall_arrayref($sql, { Slice => {} });
    my $timescale_count = scalar(@$timescale_list);
    
    logMessage(1, "Inserted $timescale_count timescales from Macrostrat");
    
    # Update any intervals from these timescales that are already in the table.
    
    $sql = "UPDATE $TIMESCALE_INTS as i join $MACROSTRAT_INTERVALS as msi using (interval_name)
		join $MACROSTRAT_SCALES_INTS as im on im.interval_id = msi.id
	SET i.macrostrat_id = msi.id, i.macrostrat_color = msi.interval_color
	WHERE im.timescale_id not in ($skip_list)";
    
    print STDERR "\n$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(1, "Updated $result existing intervals using Macrostrat data");
    
    # Add any new intervals that are not already in the table.
    
    $sql = "INSERT INTO $TIMESCALE_INTS (interval_no, macrostrat_id, interval_name, abbrev, orig_early, orig_late,
    	    orig_color, macrostrat_color)
	SELECT msi.id+2000 as interval_no, msi.id, msi.interval_name, msi.interval_abbrev, msi.age_bottom, msi.age_top,
    		msi.interval_color, msi.orig_color
	FROM $MACROSTRAT_INTERVALS as msi join $MACROSTRAT_SCALES_INTS as im on im.interval_id = msi.id
		left join $TIMESCALE_INTS as tsi using (interval_name)
	WHERE tsi.interval_name is null and im.timescale_id not in ($skip_list)
	GROUP BY msi.id";
    
    print STDERR "\n$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(1, "Inserted $result intervals from Macrostrat");
    
    $result = $dbh->do($sql);
    
    logMessage(1, "Inserted $result intervals from the PBDB");
    
    # Now go through the timescales we just copied, one by one.
    
 TIMESCALE:
    foreach my $t ( @$timescale_list )
    {
	next unless $t->{timescale_no} && $t->{macrostrat_id};
	
	process_one_timescale($dbh, $t->{timescale_no}, $options);
    }

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

    my ($dbh, $boundary_list, $source_no, $max_age, $late_age) = @_;
    
    my ($sql, $result);
    
    # First get a list of boundaries from the specified timescale, restricted according to the
    # specified bounds.
    
    my $source_quoted = $dbh->quote($source_no);
    my @filters = "timescale_no = $source_quoted";
    
    if ( $max_age )
    {
	my $quoted = $dbh->quote($max_age);
	push @filters, "age <= $quoted";
    }
    
    if ( @$boundary_list && $boundary_list->[-1]{age} )
    {
	$late_age = $boundary_list->[-1]{age} + 0.1 if ! defined $late_age || $boundary_list->[-1]{age} >= $late_age;
    }
    
    if ( $late_age )
    {
	my $quoted = $dbh->quote($late_age);
	push @filters, "age >= $quoted";
    }
    
    my $filter = "";
    
    if ( @filters )
    {
	$filter = "WHERE " . join( ' and ', @filters );
    }
    
    $sql = "SELECT bound_no, age, lower_no, interval_no, timescale_type, timescale_extent
	    FROM $TIMESCALE_BOUNDS join $TIMESCALE_DATA using (timescale_no)
	    $filter ORDER BY age asc";
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
    my @results;
    
    @results = @$result if ref $result eq 'ARRAY';
    
    # If we have no results, do nothing.
    
    return unless @results;
    
    # If the top boundary has no interval_no, and the list to which we are adding has at least one
    # member, then remove it.
    
    if ( @$boundary_list && ! $results[0]{interval_no} )
    {
	shift @results;
    }
    
    # Now tie the two ranges together.
    
    if ( @$boundary_list )
    {
	$boundary_list->[-1]{lower_no} = $results[0]{interval_no};
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
		bound_type, lower_no, interval_no, base_no, age, derived_age, interval_type,
		interval_extent, interval_taxon) VALUES ";
    
    my @values;
    
    my $ts_quoted = $dbh->quote($timescale_no);
    my $auth_quoted = $dbh->quote($authorizer_no);
    
    foreach my $b (@$boundary_list)
    {
	my $lower_quoted = $dbh->quote($b->{lower_no});
	my $upper_quoted = $dbh->quote($b->{interval_no});
	my $source_quoted = $dbh->quote($b->{base_no} // $b->{bound_no});
	my $age_quoted = $dbh->quote($b->{age});
	my $type_quoted = $dbh->quote($b->{timescale_type} // '');
	my $extent_quoted = $dbh->quote($b->{timescale_extent} // '');
	my $taxon_quoted = $dbh->quote($b->{timescale_taxon} // '');
	
	push @values, "($ts_quoted, $auth_quoted, $auth_quoted, 'same', $lower_quoted, " .
	    "$upper_quoted, $source_quoted, $age_quoted, $age_quoted, $type_quoted, $extent_quoted, $taxon_quoted)";
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
		(SELECT timescale_no, max(b.age) as max_age, min(b.age) as min_age FROM timescale_bounds as b
		$filter GROUP BY timescale_no) as bb using (timescale_no)
	SET t.max_age = bb.max_age, t.min_age = bb.min_age";
    
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
    
    $sql = "SELECT min(base_no) as has_source, min(range_no) as has_range,
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
			join $TIMESCALE_BOUNDS as bottom on base.bound_no = b.base_no
			join $TIMESCALE_BOUNDS as top on top.bound_no = b.range_no
		SET b.derived_age = bottom.age - (bottom.age - top.age) * b.offset,
		    b.derived_age_error = (bottom.age - top.age) * b.offset_error,
		    b.derived_reference_no = null,
		    b.derived_color = bottom.color
		WHERE timescale_no = $ts_quoted and bound_type = 'range'";
	
	$age_update_count += $dbh->do($sql);
    }
    
    # Then compute ages for 'same' and 'offset' boundaries.
    
    if ( $has_source )
    {
	# $$$ derived_age_error needs a more sophisticated calculation when boundary type is
	# 'offset', taking into account both the source age error and the offset error.
	
	$sql = "UPDATE $TIMESCALE_BOUNDS as b
			join $TIMESCALE_BOUNDS as source on source.bound_no = b.base_no
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
		WHERE timescale_no = $ts_quoted and bound_type in ('same', 'range', 'offset')";
	
	$result = $dbh->do($sql);
    }
}


# check_timescale_integrity ( dbh, timescale_no )
# 
# Check the specified timescale for integrity.  If any errors are found, return a list of them.

sub check_timescale_integrity {
    
    my ($dbh, $timescale_no, $bounds_ref, $options) = @_;
    
    my ($sql);
    
    $sql = "	SELECT bound_no, age, lower_no, lower.interval_name as lower_name,
			upper.interval_no, upper.interval_name
		FROM $TIMESCALE_BOUNDS as tsb
			left join $TIMESCALE_INTS as lower on lower.interval_no = tsb.lower_no
			left join $TIMESCALE_INTS as upper on upper.interval_no = tsb.interval_no
		WHERE timescale_no = $timescale_no";
	
    my ($results) = $dbh->selectall_arrayref($sql, { Slice => { } });
    my (@results);
    
    @results = @$results if ref $results eq 'ARRAY';
    
    my (@errors);
    
    # Make sure that we actually have some results.
    
    unless ( @results )
    {
	push @errors, "No boundaries found";
	return @errors;
    }
    
    # Make sure that the first and last intervals have the correct properties.
    
    if ( $results[0]{interval_no} )
    {
	my $bound_no = $results[0]{bound_no};
	push @errors, "Error in bound $bound_no: should be upper boundary but has interval_no = $results[0]{interval_no}";
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
	my $interval_no = $r->{interval_no};
	my $lower_no = $r->{lower_no};
	
	$boundary_count++;
	
	# The first age will be the late end of the scale, the last age will be the early end.
	
	# $late_age //= $age;
	# $early_age = $age;
	
	# Make sure the ages are all defined and monotonic.
	
	unless ( defined $age )
	{
	    push @errors, "Error in bound $bound_no: age is not defined";
	}
	
	if ( defined $last_age && $last_age >= $age )
	{
	    push @errors, "Error in bound $bound_no: age ($age) >= last age ($last_age)";
	}
	
	# Make sure that the interval_no matches the lower_no of the previous
	# record.
	
	if ( defined $last_lower_no )
	{
	    unless ( $interval_no )
	    {
		push @errors, "Error in bound $bound_no: interval_no not defined";
	    }
	    
	    elsif ( $interval_no ne $last_lower_no )
	    {
		push @errors, "Error in bound $bound_no: interval_no ($interval_no) does not match upward ($last_lower_no)";
	    }
	}
	
	$last_lower_no = $lower_no;
	
	unless ( $lower_no || $r->{last_record} )
	{
	    push @errors, "Error in bound $bound_no: lower_no not defined";
	}
    }
    
    if ( ref $bounds_ref eq 'ARRAY' )
    {
	@$bounds_ref = @results;
    }
    
    return @errors;
}


# update_timescale_descriptions ( dbh, timescale_no, options )
# 
# Update the fields 'timescale_type', 'timescale_extent', and 'timescale_taxon' based on the name
# of each timescale. If $timescale_no is given, just update that timescale. Otherwise, update
# everything.

sub update_timescale_descriptions {
    
    my ($dbh, $timescale_no, $options) = @_;
    
    $options ||= { };
    
    logMessage(1, "Setting timescale descriptions...");
    
    my ($sql, $result);
    
    my $selector = '';
    
    $selector = "and timescale_no = $timescale_no" if $timescale_no && $timescale_no =~ qr{^\d+$} && $timescale_no > 0;
    
    # First clear all existing attributes and re-compute them.
    
    $dbh->do("UPDATE $TIMESCALE_DATA
		SET timescale_type = null, timescale_extent = null, timescale_taxon = null
		WHERE timescale_no > 10 $selector");
    
    # Scan for all of the possible types.
    
    foreach my $type ('stage', 'substage', 'zone', 'epoch', 'period', 'era', 'eon')
    {
	my $regex = $type;
	$regex = 's?t?age' if $type eq 'stage';
	$regex = '(?:subs?t?age|unit)' if $type eq 'substage';
	$regex = '(?:zone|zonation|chron)' if $type eq 'zone';
	
	$sql = "UPDATE $TIMESCALE_DATA
		SET timescale_type = '$type'
		WHERE timescale_name rlike '\\\\b${regex}s?\\\\b' $selector";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$result = $dbh->do($sql);
	
	logMessage(2, "    type = '$type' - $result") if $result;
    }
    
    # Scan for taxa that are known to be used in naming zones
    
    foreach my $taxon ('ammonite', 'conodont', 'mammal', 'faunal')
    {
	my $regex = $taxon;
	
	$sql = "UPDATE $TIMESCALE_DATA
		SET timescale_taxon = '$taxon'
		WHERE timescale_name rlike '$regex' $selector";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$result = $dbh->do($sql);
	
	logMessage(2, "    taxon = '$taxon' - $result") if $result;
    }
    
    # Now scan for geographic extents
    
    my (%extent) = ('china' => 'Chinese',
		    'asia' => 'Asian',
		    'north america' => 'North American',
		    'south america' => 'South American',
		    'africa' => 'African',
		    'europe' => 'European',
		    'laurentia' => 'Laurentian',
		    'western interior' => 'Western interior',
		    'boreal' => 'Boreal');
    
    foreach my $regex ( keys %extent )
    {
	my $extent = $extent{$regex};
	
	$sql = "UPDATE $TIMESCALE_DATA
		SET timescale_extent = '$extent'
		WHERE timescale_name rlike '$regex' $selector";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$result = $dbh->do($sql);
	
	logMessage(2, "    extent = '$extent' - $result") if $result && $result > 0;
    }
    
    # Now copy these to the intervals boundaries.
    
    $sql = "UPDATE $TIMESCALE_BOUNDS as tsb join $TIMESCALE_DATA as ts using (timescale_no)
		SET tsb.interval_type = ts.timescale_type,
		    tsb.interval_extent = ts.timescale_extent,
		    tsb.interval_taxon = ts.timescale_taxon
		WHERE tsb.interval_type = ''";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(2, "Updated attributes of $result boundaries to match their timescales") if $result && $result > 0;
}


sub create_triggers {
    
    my ($dbh) = @_;
    
    $dbh->do("DROP PROCEDURE IF EXISTS complete_bound_updates");
    
    $dbh->do("CREATE PROCEDURE complete_bound_updates ( )
	BEGIN

	SET \@row_count = 1;
	SET \@age_iterations = 0;
	
	# Mark all timescales that have updated bounds as updated, plus all
	# other bounds in those timescales.
	
	UPDATE timescale_bounds as tsb join timescales as ts using (timescale_no)
		join timescale_bounds as base using (timescale_no)
	SET ts.is_updated = 1, tsb.is_updated = 1
	WHERE base.is_updated;
	
	# Now update the ages on all bounds that are marked as is_updated.  If
	# this results in any updated records, repeat the process until no
	# records change. We put a limit of 20 on the number of iterations.
	
	WHILE \@row_count > 0 AND \@age_iterations < 20 DO
	    
	    UPDATE timescale_bounds as tsb
		left join timescale_bounds as base on base.bound_no = tsb.base_no
		left join timescale_bounds as top on top.bound_no = tsb.range_no
	    SET tsb.is_updated = 1,
		tsb.derived_age = case tsb.bound_type
			when 'same' then base.derived_age
			when 'offset' then base.derived_age - tsb.offset
			when 'range' then base.derived_age - (tsb.offset / 100) * ( base.derived_age - top.derived_age )
			else tsb.age
			end,
		tsb.derived_age_error = case tsb.bound_type
			when 'same' then base.age_error
			when 'offset' then base.age_error + tsb.offset_error
			when 'range' then base.age_error + (tsb.offset_error / 100) * ( base.derived_age - top.derived_age )
			else tsb.age_error
			end
	    WHERE base.is_updated or top.is_updated or tsb.is_updated;
	    
	    SET \@row_count = ROW_COUNT();
	    SET \@age_iterations = \@age_iterations + 1;
	    
	    # SET \@debug = concat(\@debug, \@cnt, ' : ');
	    
	END WHILE;
	
	# Then do the same thing for the color and reference_no attributes
	
	SET \@row_count = 1;
	SET \@attr_iterations = 0;

	WHILE \@row_count > 0 AND \@attr_iterations < 20 DO
	
	    UPDATE timescale_bounds as tsb
		join timescales as ts using (timescale_no)
		left join timescale_bounds as csource on csource.bound_no = tsb.color_no
		left join timescale_bounds as rsource on rsource.bound_no = tsb.refsource_no
	    SET tsb.is_updated = 1,
		tsb.derived_color = case
			when csource.color <> '' then csource.color
			else csource.derived_color end,
		tsb.derived_reference_no = case
			when rsource.reference_no > 0 then rsource.reference_no
			when rsource.derived_reference_no > 0 then rsource.derived_reference_no
			else ts.reference_no end
	    WHERE tsb.is_updated or csource.is_updated or rsource.is_updated;
	    
	    SET \@row_count = ROW_COUNT();
	    SET \@attr_iterations = \@attr_iterations + 1;
	    
	END WHILE;
	
	# Now, for locked records we set the is_different flag if any of the derived attributes
	# are different from the active ones.
	
	UPDATE timescale_bounds as tsb
	SET tsb.is_different = tsb.age <> tsb.derived_age or tsb.age_error <> tsb.derived_age_error
		or tsb.color <> tsb.derived_color or tsb.reference_no <> tsb.derived_reference_no
	WHERE tsb.is_updated and tsb.is_locked;
	
	# For unlocked records, we set the active values equal to the derived ones.
	
	UPDATE timescale_bounds as tsb
	SET tsb.is_different = 0,
	    tsb.age = tsb.derived_age,
	    tsb.age_error = tsb.derived_age_error,
	    tsb.color = tsb.derived_color,
	    tsb.reference_no = tsb.derived_reference_no
	WHERE tsb.is_updated and not tsb.is_locked;
	
	# Now clear all of the is_updated flags on the bounds, and at the same time set the
	# is_updated flags on all timescales that had updated bounds.
	
	# UPDATE $TIMESCALE_DATA as ts join timescale_bounds as tsb using (timescale_no)
	# SET ts.is_updated = 1, tsb.is_updated = 0
	# WHERE tsb.is_updated;
	
	# Then check all of the bounds in the specified timescales for errors.
	
	CALL check_updated_bounds;
	
	# Then 
	
	END;");
    
    
    $dbh->do("DROP PROCEDURE IF EXISTS check_updated_bounds");
    
    $dbh->do("CREATE PROCEDURE check_updated_bounds ( )
	BEGIN
	
	UPDATE timescales as ts join
	    (SELECT timescale_no, min(b.age) as min, max(b.age) as max FROM
		timescale_bounds as b join timescale_bounds as base using (timescale_no)
	     WHERE base.is_updated GROUP BY timescale_no) as all_bounds using (timescale_no)
	SET ts.min_age = all_bounds.min,
	    ts.max_age = all_bounds.max;
	
	UPDATE timescale_bounds as tsb join timescales as ts using (timescale_no) join
		(SELECT b1.bound_no,
		    if(b1.is_top, 1, b1.age_this > b1.age_prev) as age_ok,
		    if(b1.is_top, b1.interval_no = 0, b1.interval_no = b1.lower_prev) as bound_ok,
		    (b1.interval_no = 0 or b1.upper_int > 0) as interval_ok,
		    (b1.lower_this = 0 or b1.lower_int > 0) as lower_ok,
		    (b1.duplicate_no is null) as unique_ok
		 FROM
		  (SELECT b0.bound_no, b0.timescale_no, (\@ts_prev:=\@ts_this) as ts_prev, (\@ts_this:=timescale_no) as ts_this,
			(\@ts_prev is null or \@ts_prev <> \@ts_this) as is_top, b0.interval_no, upper_int, lower_int,
			(\@lower_prev:=\@lower_this) as lower_prev, (\@lower_this:=lower_no) as lower_this,
			(\@age_prev:=\@age_this) as age_prev, (\@age_this:=b0.age) as age_this, b0.duplicate_no
		   FROM (SELECT b.bound_no, b.timescale_no, b.age, b.interval_no, b.lower_no,
			upper.interval_no as upper_int, lower.interval_no as lower_int,
			duplicate.bound_no as duplicate_no
		   FROM timescale_bounds as b
			join timescale_bounds as base using (timescale_no)
			left join timescale_bounds as duplicate on duplicate.timescale_no = b.timescale_no and
				duplicate.interval_no = b.interval_no and duplicate.bound_no <> b.bound_no and
				duplicate.bound_no > 0
		        left join timescale_ints as upper on upper.interval_no = b.interval_no
		        left join timescale_ints as lower on lower.interval_no = b.lower_no
			join (SELECT \@lower_this:=null, \@lower_prev:=null, \@age_this:=null, \@age_prev:=null,
				\@ts_prev:=0, \@ts_this:=0) as initializer
		   WHERE base.is_updated GROUP BY b.bound_no ORDER BY b.timescale_no, b.age) as b0) as b1) as b2 using (bound_no)
		 
	      SET tsb.is_error = not(bound_ok and age_ok and interval_ok and lower_ok and unique_ok),
		  ts.is_error = not(bound_ok and age_ok and interval_ok and lower_ok and unique_ok);
	END;");
    
    $dbh->do("DROP PROCEDURE IF EXISTS unmark_updated");
    
    $dbh->do("CREATE PROCEDURE unmark_updated ( )
	BEGIN
	
	UPDATE timescale_bounds SET is_updated = 0;
	
	END;");
    
    $dbh->do("DROP TRIGGER IF EXISTS insert_bound");
    
    $dbh->do("CREATE TRIGGER insert_bound
	BEFORE INSERT ON timescale_bounds FOR EACH ROW
	BEGIN
	    DECLARE ts_interval_type varchar(10);
	    
	    IF NEW.timescale_no > 0 THEN
		SELECT timescale_type INTO ts_interval_type
		FROM timescales WHERE timescale_no = NEW.timescale_no;
		
		IF NEW.interval_type is null or NEW.interval_type = ''
		THEN SET NEW.interval_type = interval_type; END IF;
	    END IF;
	    
	    SET NEW.is_updated = 1;
	END;");
    
    $dbh->do("DROP TRIGGER IF EXISTS update_bound");
    
    $dbh->do("CREATE TRIGGER update_bound
	BEFORE UPDATE ON timescale_bounds FOR EACH ROW
	BEGIN
	    IF OLD.bound_type <> NEW.bound_type or OLD.interval_no <> NEW.interval_no or
		OLD.lower_no <> NEW.lower_no or OLD.base_no <> NEW.base_no or
		OLD.range_no <> NEW.range_no or OLD.color_no <> NEW.color_no or
		OLD.refsource_no <> NEW.refsource_no or OLD.age <> NEW.age or
		OLD.age_error <> NEW.age_error or OLD.offset <> NEW.offset or
		OLD.offset_error <> NEW.offset_error or OLD.color <> NEW.color or
		OLD.derived_age <> NEW.derived_age or OLD.derived_age_error <> NEW.derived_age_error or
		OLD.derived_color <> NEW.derived_color or OLD.derived_reference_no <> NEW.derived_reference_no
		 THEN
	    SET NEW.is_updated = 1; END IF;
	END;");
    
    # my $propagate_bound_routine = "
    # 	BEGIN
    # 	    UPDATE timescale_bounds SET is_propagated = 1
    # 		WHERE base_no = NEW.bound_no or range_no = NEW.bound_no
    # 		    or color_no = NEW.bound_no or refsource_no = NEW.bound_no;
    # 	END;";
    
    # $dbh->do("DROP TRIGGER IF EXISTS propagate_bound");
    
    # $dbh->do("CREATE TRIGGER propagate_bound
    # 	AFTER UPDATE ON timescale_bounds
    # 	FOR EACH ROW $propagate_bound_routine");
}

1;
