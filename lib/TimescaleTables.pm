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
	         $INTERVAL_DATA $INTERVAL_MAP $SCALE_MAP);
use CoreFunction qw(activateTables loadSQLFile);
use ConsoleLog qw(logMessage);

use base 'Exporter';

our(@EXPORT_OK) = qw(establishTimescaleTables);


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
		timescale_no int unsigned primary key,
		authorizer_no int unsigned not null,
		timescale_name varchar(80) not null,
		source_timescale_no int unsigned,
		early_age decimal(9,5),
		late_age decimal(9,5),
		reference_no int unsigned not null,
		interval_type enum('eon', 'era', 'period', 'epoch', 'stage', 'zone'),
		is_active boolean,
		created timestamp default current_timestamp,
		modified timestamp default current_timestamp,
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
		interval_name varchar(80) not null,
		interval_abbrev varchar(10) not null)");
    
    # The table 'timescale_bounds' defines boundaries between intervals.
    
    $dbh->do("DROP TABLE IF EXISTS $TS_BOUNDS_WORK");
    
    $dbh->do("CREATE TABLE $TS_BOUNDS_WORK (
		bound_no int unsigned primary key,
		timescale_no int unsigned not null,
		bound_type enum('absolute', 'spike', 'same', 'relative', 'offset'),
		lower_interval_no int unsigned not null,
		upper_interval_no int unsigned not null,
		source_bound_no int unsigned not null,
		range_bound_no int unsigned not null,
		color_bound_no int unsigned not null,
		age decimal(9,5),
		age_error decimal(9,5),
		percent decimal(7,4),
		percent_error decimal(7,4),
		is_locked boolean not null,
		is_different boolean not null,
		color varchar(10),
		reference_no int unsigned not null,
		derived_age decimal(9,5),
		derived_color varchar(10),
		derived_reference_no int unsigned not null,
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


# sub loadIntervalData {

#     my ($dbh, $force) = @_;
    
#     # Unless $force was specified, check whether the table already exists and
#     # contains data.
    
#     my $result;
    
#     unless ( $force )
#     {
# 	try {
# 	    $result = $dbh->do("SELECT COUNT(*) FROM $INTERVAL_DATA");
# 	};
#     }
    
#     # If the relevant tables were not found, try to read the data in from
#     # files on disk.
    
#     my $update = 0;
    
#     if ( $force or not $result )
#     {
# 	logMessage(2, "loading tables from system/interval_data.sql...");
# 	loadSQLFile($dbh, $INTERVAL_DATA_FILE);
# 	$update = 1;
#     }
    
#     # If new data was loaded, then we need to recompute the interval map table.
    
#     if ( $update )
#     {
# 	buildIntervalMap($dbh);
# 	buildIntervalBufferMap($dbh);
#     }
# }


# =head2 buildIntervalMap ( dbh )

# Generate the tables 'interval_map' and 'interval_bracket'.  The first of these
# maps each possible time range (whose endpoints are the start or end of any
# known intervals) and each known time scale to a single containing interval in
# that time scale plus a starting and ending interval which most precisely
# bracket the starting range.

# The second table just maps single ages to bracketing intervals.  By means of
# these two tables, we can translate any range or age into any of the known time
# scales. 

# =cut

# sub buildIntervalMap {

#     my ($dbh) = @_;
    
#     my ($sql, $result, $count);
    
#     logMessage(2, "computing interval map");
    
#     # Then create a new working map table.  For each possible combination of
#     # start and end ages and each possible scale, insert a row if a containing
#     # interval exists for that age range in that scale.
    
#     $dbh->do("DROP TABLE IF EXISTS $INTERVAL_MAP_WORK");
    
#     $dbh->do("CREATE TABLE $INTERVAL_MAP_WORK (
# 		scale_no smallint unsigned not null,
# 		early_age decimal(9,5),
# 		late_age decimal(9,5),
# 		range_key varchar(20) not null,
# 		cx_int_no int unsigned not null,
# 		cx_interval varchar(80) not null,
# 		early_int_no int unsigned not null,
# 		early_interval varchar(80) not null,
# 		late_int_no int unsigned not null,
# 		late_interval varchar(80) not null,
# 		PRIMARY KEY (early_age, late_age, scale_no)) Engine=MyISAM");
    
#     logMessage(2, "    computing containing intervals...");
    
#     $sql = "	INSERT IGNORE $INTERVAL_MAP_WORK (scale_no, early_age, late_age, cx_int_no)
# 		SELECT i.scale_no, i.early_age, i.late_age, 
# 		       (SELECT cxi.interval_no 
# 			FROM $INTERVAL_DATA as cxi JOIN $SCALE_MAP as sm using (interval_no)
# 			WHERE cxi.early_age >= i.early_age and cxi.late_age <= i.late_age 
# 				and sm.scale_no = i.scale_no
# 			ORDER BY scale_level desc limit 1) as cx_int_no
# 		FROM (SELECT distinct s.scale_no, ei.early_age, li.late_age
# 		      FROM scale_data as s JOIN interval_data as ei JOIN interval_data as li
# 		      WHERE ei.early_age > li.late_age and
# 			ei.early_age <= s.early_age and li.late_age >= s.late_age) as i";
    
#     $result = $dbh->do($sql);
    
#     logMessage(2, "      found $result age start/end pairs");
    
#     $sql = "	INSERT IGNORE INTO $INTERVAL_MAP_WORK (scale_no, early_age, late_age, cx_int_no)
# 		SELECT i.scale_no, i.early_age, i.early_age,
# 		       (SELECT cxi.interval_no 
# 			FROM interval_data as cxi JOIN $SCALE_MAP as sm using (interval_no)
# 			WHERE cxi.early_age > i.early_age and cxi.late_age < i.early_age
# 				and sm.scale_no = i.scale_no
# 			ORDER BY scale_level desc limit 1) as cx_int_no
# 		FROM (SELECT distinct s.scale_no, i.early_age
# 		      FROM scale_data as s JOIN interval_data as i
# 		      WHERE i.early_age <= s.early_age and i.early_age >= s.late_age) as i";
    
#     $result = $dbh->do($sql);
    
#     $sql = "	INSERT IGNORE INTO $INTERVAL_MAP_WORK (scale_no, early_age, late_age, cx_int_no)
# 		SELECT i.scale_no, i.late_age, i.late_age,
# 		       (SELECT cxi.interval_no 
# 			FROM interval_data as cxi JOIN $SCALE_MAP as sm using (interval_no)
# 			WHERE cxi.early_age >= i.late_age and cxi.late_age < i.late_age
# 				and sm.scale_no = i.scale_no
# 			ORDER BY scale_level desc limit 1) as cx_int_no
# 		FROM (SELECT distinct s.scale_no, i.late_age
# 		      FROM scale_data as s JOIN interval_data as i
# 		      WHERE i.late_age <= s.early_age and i.late_age >= s.late_age) as i";
    
#     $result += $dbh->do($sql);
    
#     logMessage(2, "      found $result age boundary points");
    
#     # Now, we fill in the most specific early interval for each age range.
    
#     logMessage(2, "    computing early intervals...");
    
#     $sql = "UPDATE $INTERVAL_MAP_WORK as i
# 	    SET i.early_int_no =
# 	       (SELECT d.interval_no FROM $INTERVAL_DATA as d JOIN $SCALE_MAP as sm using (interval_no)
# 		WHERE d.early_age >= i.early_age and d.late_age < i.early_age and d.late_age >= i.late_age
# 			and sm.scale_no = i.scale_no
# 		ORDER BY early_age asc, scale_level asc limit 1)";
    
#     $result = $dbh->do($sql);
    
#     $sql = "UPDATE $INTERVAL_MAP_WORK as i JOIN $INTERVAL_DATA as d on i.late_age = d.late_age
# 		JOIN $SCALE_MAP as sm using (interval_no)
# 	    SET i.early_int_no =
# 	       (SELECT d.interval_no FROM $INTERVAL_DATA as d JOIN $SCALE_MAP as sm using (interval_no)
# 		WHERE d.late_age = i.late_age
# 		ORDER BY scale_level asc limit 1)
# 	    WHERE i.early_age = i.late_age";
    
#     $result = $dbh->do($sql);
    
#     $sql = "UPDATE $INTERVAL_MAP_WORK
# 	    SET early_int_no = cx_int_no
# 	    WHERE early_int_no = 0 and
# 		(early_age = late_age or late_int_no = 0 or late_int_no = cx_int_no)";
    
#     $result = $dbh->do($sql);
    
#     my ($hadean_no, $early) = $dbh->selectrow_array("
# 	SELECT interval_no, early_age FROM $INTERVAL_DATA
# 	WHERE interval_name = 'Hadean'");
    
#     $hadean_no ||= 0;
#     $early ||= 4600.0;
    
#     $sql = "UPDATE $INTERVAL_MAP_WORK
# 	    SET cx_int_no = $hadean_no, early_int_no = $hadean_no, late_int_no = $hadean_no
# 	    WHERE early_age = $early and late_age = $early";
    
#     $result = $dbh->do($sql);
    
#     logMessage(2, "    computing late intervals...");
    
#     $sql = "UPDATE $INTERVAL_MAP_WORK as i
# 	    SET i.late_int_no =
# 	       (SELECT d.interval_no FROM $INTERVAL_DATA as d JOIN $SCALE_MAP as sm using (interval_no)
# 		WHERE d.early_age <= i.early_age and d.early_age > i.late_age and d.late_age <= i.late_age
# 			and sm.scale_no = i.scale_no
# 		ORDER BY late_age desc, scale_level asc limit 1)";
    
#     $result = $dbh->do($sql);
    
#     $sql = "UPDATE $INTERVAL_MAP_WORK as i JOIN $INTERVAL_DATA as d on i.early_age = d.early_age
# 		JOIN $SCALE_MAP as sm using (interval_no)
# 	    SET i.late_int_no =
# 	       (SELECT d.interval_no FROM $INTERVAL_DATA as d JOIN $SCALE_MAP as sm using (interval_no)
# 		WHERE d.early_age = i.early_age
# 		ORDER BY scale_level asc limit 1)
# 	    WHERE i.early_age = i.late_age";
    
#     $result = $dbh->do($sql);
    
#     $sql = "UPDATE $INTERVAL_MAP_WORK
# 	    SET late_int_no = cx_int_no
# 	    WHERE late_int_no = 0 and
# 		(early_age = late_age or early_int_no = 0 or early_int_no = cx_int_no)";
    
#     $result = $dbh->do($sql);
    
#     my ($holocene_no) = $dbh->selectrow_array("
# 	SELECT interval_no FROM $INTERVAL_DATA
# 	WHERE interval_name = 'Holocene'");
    
#     $holocene_no ||= 0;
    
#     $sql = "UPDATE $INTERVAL_MAP_WORK
# 	    SET cx_int_no = $holocene_no, early_int_no = $holocene_no, late_int_no = $holocene_no
# 	    WHERE early_age = 0 and late_age = 0";
    
#     $result = $dbh->do($sql);
    
#     my ($quaternary_no) = $dbh->selectrow_array("
# 	SELECT interval_no FROM $INTERVAL_DATA
# 	WHERE interval_name = 'Quaternary'");
    
#     my ($pleistocene_no) = $dbh->selectrow_array("
# 	SELECT interval_no FROM $INTERVAL_DATA
# 	WHERE interval_name = 'Pleistocene'");
    
#     $sql = "UPDATE $INTERVAL_MAP_WORK
# 	    SET late_int_no = $holocene_no
# 	    WHERE late_age = 0 and late_int_no = $quaternary_no";
    
#     $result = $dbh->do($sql);
    
#     $sql = "UPDATE $INTERVAL_MAP_WORK
# 	    SET early_int_no = $pleistocene_no
# 	    WHERE early_int_no = $quaternary_no";
    
#     $result = $dbh->do($sql);
    
#     logMessage(2, "    setting interval names...");
    
#     $sql = "UPDATE $INTERVAL_MAP_WORK as i JOIN $INTERVAL_DATA as d on d.interval_no = i.cx_int_no
# 	    SET i.cx_interval = d.interval_name";
    
#     $result = $dbh->do($sql);
    
#     $sql = "UPDATE $INTERVAL_MAP_WORK
# 	    SET cx_interval = 'Geologic Time'
# 	    WHERE cx_int_no = 0";
    
#     $dbh->do($sql);
    
#     $sql = "UPDATE $INTERVAL_MAP_WORK as i JOIN $INTERVAL_DATA as d on d.interval_no = i.late_int_no
# 	    SET i.late_interval = d.interval_name";
    
#     $result = $dbh->do($sql);
    
#     $sql = "UPDATE $INTERVAL_MAP_WORK as i JOIN $INTERVAL_DATA as d on d.interval_no = i.early_int_no
# 	    SET i.early_interval = d.interval_name";
    
#     $result = $dbh->do($sql);
    
#     # Now compute the range_key column, and add an index on it.
    
#     logMessage(2, "    setting range keys...");
    
#     $result = $dbh->do("
# 	UPDATE $INTERVAL_MAP_WORK
# 	SET range_key = concat(cast(early_age as double), '-', cast(late_age as double))");
    
#     $result = $dbh->do("
# 	ALTER TABLE $INTERVAL_MAP_WORK ADD UNIQUE KEY (range_key, scale_no)");
    
#     activateTables($dbh, $INTERVAL_MAP_WORK => $INTERVAL_MAP);
    
#     my $a = 1;		# we can stop here when debugging   
# }


# sub buildIntervalBufferMap {
    
#     my ($dbh) = @_;
    
#     my ($result, $sql);
    
#     logMessage(2, "    setting interval buffer bounds...");
    
#     $dbh->do("DROP TABLE IF EXISTS $INTERVAL_BUFFER_WORK");
    
#     $dbh->do("CREATE TABLE $INTERVAL_BUFFER_WORK (
# 		interval_no int unsigned primary key,
# 		early_bound decimal(9,5),
# 		late_bound decimal(9,5))");
    
#     $sql = "INSERT IGNORE INTO $INTERVAL_BUFFER_WORK (interval_no, early_bound, late_bound)
# 		SELECT interval_no, early_age + 50, if(late_age - 50 > 0, late_age - 50, 0)
# 		FROM $INTERVAL_DATA JOIN $SCALE_MAP using (interval_no)
# 		WHERE scale_no = 1";
    
#     $result = $dbh->do($sql);
    
#     $sql = "UPDATE $INTERVAL_BUFFER_WORK as ib JOIN $INTERVAL_DATA as i1 using (interval_no)
# 		JOIN $INTERVAL_DATA as i2
# 		JOIN $SCALE_MAP as s1 on i1.interval_no = s1.interval_no
# 		JOIN $SCALE_MAP as s2 on s2.interval_no = i2.interval_no
# 	    SET ib.late_bound = if(i1.late_age - 50.0 > i2.late_age, i1.late_age - 50, i2.late_age)
# 	    WHERE s1.scale_no = s2.scale_no and s1.scale_level = s2.scale_level and i1.late_age = i2.early_age";
    
#     $result = $dbh->do($sql);
    
#     $sql = "UPDATE $INTERVAL_BUFFER_WORK as ib JOIN $INTERVAL_DATA as i1 using (interval_no)
# 		JOIN $INTERVAL_DATA as i2
# 		JOIN $SCALE_MAP as s1 on i1.interval_no = s1.interval_no
# 		JOIN $SCALE_MAP as s2 on s2.interval_no = i2.interval_no
# 	    SET ib.early_bound = if(i1.early_age + 50 < i2.early_age, i1.early_age + 50, i2.early_age)
# 	    WHERE s1.scale_no = s2.scale_no and s1.scale_level = s2.scale_level and i1.early_age = i2.late_age";
    
#     $result = $dbh->do($sql);
    
#     activateTables($dbh, $INTERVAL_BUFFER_WORK => $INTERVAL_BUFFER);
    
#     my $a = 1;	# we can stop here when debugging
# }
