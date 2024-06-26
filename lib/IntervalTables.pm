# 
# The Paleobiology Database
# 
#   IntervalTables.pm
# 

package IntervalTables;

use strict;

use Carp qw(carp croak);
use Try::Tiny;

use TableDefs qw(%TABLE $INTERVAL_DATA $INTERVAL_MAP $INTERVAL_BUFFER
		 $SCALE_DATA $SCALE_LEVEL_DATA $SCALE_MAP);
use CoreTableDefs;
use CoreFunction qw(activateTables loadSQLFile);
use ConsoleLog qw(logMessage);

use base 'Exporter';

our(@EXPORT_OK) = qw(loadIntervalData buildIntervalMap
		     $INTERVAL_DATA $INTERVAL_MAP);


our ($holocene_no) = 32;
our ($holocene_stages) = '(1200, 1201, 1202)';

# Table and file names

our $INTERVAL_MAP_WORK = "imn";
our $INTERVAL_BUFFER_WORK = "iun";

# Template files

our $INTERVAL_DATA_FILE = "system/interval_data.sql";


=head1 NAME

IntervalTables

=head1 SYNOPSIS

This module builds and maintains the tables by means of which time interval
computations may be carried out.

=head2 TABLES

The following tables are maintained by this module:

=over 4

=item interval_data

Lists each interval known to the database, one per row.

=item interval_bracket

For any age that is an endpoint of a known interval=item interval_map, lists
the intervals from all the known time scales that contain (bracket) it.

=item interval_map

Maps any time range whose endpoints are the start or end of any known interval
to any of the known time scales.

=back

=cut

=head1 INTERFACE

In the following documentation, the parameter C<dbi> refers to a DBI database handle.

=head2 loadIntervalData ( dbh, force )

Unless the 'interval_data' table exists and has data in it, load it from the
template file on disk.  Unless the 'scale_data' table exists and has data in
it, load it from the template file on disk.  If the parameter C<force> is
true, then load these tables regardless and replace any existing data.

=cut

sub loadIntervalData {

    my ($dbh, $force) = @_;
    
    # Unless $force was specified, check whether the table already exists and
    # contains data.
    
    my $result;
    
    unless ( $force )
    {
	try {
	    $result = $dbh->do("SELECT COUNT(*) FROM $INTERVAL_DATA");
	};
    }
    
    # If the relevant tables were not found, try to read the data in from
    # files on disk.
    
    my $update = 0;
    
    if ( $force or not $result )
    {
	logMessage(2, "loading tables from system/interval_data.sql...");
	loadSQLFile($dbh, $INTERVAL_DATA_FILE);
	$update = 1;
    }
    
    # If new data was loaded, then we need to recompute the interval map table.
    
    if ( $update )
    {
	buildIntervalMap($dbh);
	buildIntervalBufferMap($dbh);
    }
}


=head2 buildIntervalMap ( dbh )

Generate the tables 'interval_map' and 'interval_bracket'.  The first of these
maps each possible time range (whose endpoints are the start or end of any
known intervals) and each known time scale to a single containing interval in
that time scale plus a starting and ending interval which most precisely
bracket the starting range.

The second table just maps single ages to bracketing intervals.  By means of
these two tables, we can translate any range or age into any of the known time
scales. 

=cut

sub buildIntervalMap {

    my ($dbh) = @_;
    
    my ($sql, $result, $count);
    
    logMessage(2, "computing interval map");
    
    # Then create a new working map table.  For each possible combination of
    # start and end ages and each possible scale, insert a row if a containing
    # interval exists for that age range in that scale.
    
    $dbh->do("DROP TABLE IF EXISTS $INTERVAL_MAP_WORK");
    
    $dbh->do("CREATE TABLE $INTERVAL_MAP_WORK (
		scale_no smallint unsigned not null,
		early_age decimal(9,5),
		late_age decimal(9,5),
		range_key varchar(20),
		cx_int_no int unsigned,
		cx_interval varchar(80),
		early_int_no int unsigned not null,
		early_interval varchar(80) not null,
		late_int_no int unsigned not null,
		late_interval varchar(80) not null,
		PRIMARY KEY (early_age, late_age, scale_no)) Engine=MyISAM");
    
    # Generate a table with all possible start/end ages. We use late ages only
    # because we don't care about the Hadean eon.
    
    logMessage(2, "    computing endpoints...");
    
    $sql = "    INSERT IGNORE into $INTERVAL_MAP_WORK (scale_no, early_age, late_age)
		SELECT 1, i.late_age, j.late_age
		FROM $TABLE{INTERVAL_DATA} as i cross join $TABLE{INTERVAL_DATA} as j
		WHERE i.late_age >= j.late_age";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      found $result age start/end pairs");
    
    # Set containing intervals whenever possible.
    
    logMessage(2, "    computing containing intervals...");
    
    $sql = "	UPDATE $INTERVAL_MAP_WORK as im
		SET cx_int_no = (SELECT interval_no FROM $TABLE{INTERVAL_DATA} as i
		      WHERE i.early_age >= im.early_age and i.late_age <= im.late_age and scale_no = 1
		      ORDER by i.early_age - i.late_age LIMIT 1)";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      found $result containing intervals");
    
    # Set the rest to 0.
    
    $sql = "UPDATE $INTERVAL_MAP_WORK SET cx_int_no = 0 WHERE cx_int_no is null";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      found $result ranges with no containing interval");
    
    # Now, we fill in the most specific early interval for each age range.
    
    logMessage(2, "    computing early intervals...");
    
    $sql = "UPDATE $INTERVAL_MAP_WORK as im
	    SET im.early_int_no =
	       (SELECT i.interval_no FROM $TABLE{INTERVAL_DATA} as i
		WHERE i.early_age >= im.early_age and i.late_age < im.early_age and scale_no = 1
		ORDER BY i.early_age - i.late_age limit 1)";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      found $result early intervals");
    
    $sql = "UPDATE $INTERVAL_MAP_WORK
	    SET cx_int_no = 32 WHERE early_age = 0 and late_age = 0";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "    computing late intervals...");
    
    $sql = "UPDATE $INTERVAL_MAP_WORK as im
	    SET im.late_int_no =
	       (SELECT i.interval_no FROM $TABLE{INTERVAL_DATA} as i
		WHERE i.early_age > im.late_age and i.late_age <= im.late_age and scale_no = 1
		ORDER BY i.early_age - i.late_age limit 1)";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      found $result late intervals");
    
    $sql = "UPDATE $INTERVAL_MAP_WORK as im
	    SET early_int_no = $holocene_no WHERE early_int_no in $holocene_stages";
    
    $result = $dbh->do($sql);
    
    $sql = "UPDATE $INTERVAL_MAP_WORK as im
	    SET late_int_no = $holocene_no WHERE late_int_no in $holocene_stages";
    
    $result += $dbh->do($sql);
    
    logMessage(2, "      corrected $result entries from Holocene stages to Holocene");
    
    # $sql = "UPDATE $INTERVAL_MAP_WORK as i JOIN $INTERVAL_DATA as d on i.early_age = d.early_age
    # 		JOIN $SCALE_MAP as sm using (interval_no)
    # 	    SET i.late_int_no =
    # 	       (SELECT d.interval_no FROM $INTERVAL_DATA as d JOIN $SCALE_MAP as sm using (interval_no)
    # 		WHERE d.early_age = i.early_age
    # 		ORDER BY scale_level asc limit 1)
    # 	    WHERE i.early_age = i.late_age";
    
    # $result = $dbh->do($sql);
    
    # $sql = "UPDATE $INTERVAL_MAP_WORK
    # 	    SET late_int_no = cx_int_no
    # 	    WHERE late_int_no = 0 and
    # 		(early_age = late_age or early_int_no = 0 or early_int_no = cx_int_no)";
    
    # $result = $dbh->do($sql);
    
    # my ($holocene_no) = $dbh->selectrow_array("
    # 	SELECT interval_no FROM $INTERVAL_DATA
    # 	WHERE interval_name = 'Holocene'");
    
    # $holocene_no ||= 0;
    
    # $sql = "UPDATE $INTERVAL_MAP_WORK
    # 	    SET cx_int_no = $holocene_no, early_int_no = $holocene_no, late_int_no = $holocene_no
    # 	    WHERE early_age = 0 and late_age = 0";
    
    # $result = $dbh->do($sql);
    
    # my ($quaternary_no) = $dbh->selectrow_array("
    # 	SELECT interval_no FROM $INTERVAL_DATA
    # 	WHERE interval_name = 'Quaternary'");
    
    # my ($pleistocene_no) = $dbh->selectrow_array("
    # 	SELECT interval_no FROM $INTERVAL_DATA
    # 	WHERE interval_name = 'Pleistocene'");
    
    # $sql = "UPDATE $INTERVAL_MAP_WORK
    # 	    SET late_int_no = $holocene_no
    # 	    WHERE late_age = 0 and late_int_no = $quaternary_no";
    
    # $result = $dbh->do($sql);
    
    # $sql = "UPDATE $INTERVAL_MAP_WORK
    # 	    SET early_int_no = $pleistocene_no
    # 	    WHERE early_int_no = $quaternary_no";
    
    # $result = $dbh->do($sql);
    
    logMessage(2, "    setting interval names...");
    
    $sql = "UPDATE $INTERVAL_MAP_WORK as im JOIN $INTERVAL_DATA as i on i.interval_no = im.cx_int_no
	    SET im.cx_interval = i.interval_name";
    
    $result = $dbh->do($sql);
    
    $sql = "UPDATE $INTERVAL_MAP_WORK
	    SET cx_interval = 'Geologic Time'
	    WHERE cx_int_no = 0";
    
    $dbh->do($sql);
    
    $sql = "UPDATE $INTERVAL_MAP_WORK as i JOIN $INTERVAL_DATA as d on d.interval_no = i.late_int_no
	    SET i.late_interval = d.interval_name";
    
    $result = $dbh->do($sql);
    
    $sql = "UPDATE $INTERVAL_MAP_WORK as i JOIN $INTERVAL_DATA as d on d.interval_no = i.early_int_no
	    SET i.early_interval = d.interval_name";
    
    $result = $dbh->do($sql);
    
    # Now compute the range_key column, and add an index on it.
    
    logMessage(2, "    setting range keys...");
    
    $result = $dbh->do("
	UPDATE $INTERVAL_MAP_WORK
	SET range_key = concat(cast(early_age as double), '-', cast(late_age as double))");
    
    $result = $dbh->do("
	ALTER TABLE $INTERVAL_MAP_WORK ADD UNIQUE KEY (range_key, scale_no)");
    
    activateTables($dbh, $INTERVAL_MAP_WORK => $INTERVAL_MAP);
    
    logMessage(2, "    setting interval buffer bounds...");
    
    $dbh->do("DROP TABLE IF EXISTS $INTERVAL_BUFFER_WORK");
    
    $dbh->do("CREATE TABLE $INTERVAL_BUFFER_WORK (
		interval_no int unsigned primary key,
		early_bound decimal(9,5),
		late_bound decimal(9,5))");
    
    $sql = "INSERT IGNORE INTO $INTERVAL_BUFFER_WORK (interval_no, early_bound, late_bound)
		SELECT interval_no, early_age + 50, if(late_age - 50 > 0, late_age - 50, 0)
		FROM $INTERVAL_DATA
		WHERE scale_no = 1";
    
    $result = $dbh->do($sql);
    
    $sql = "UPDATE $INTERVAL_BUFFER_WORK as ib JOIN $INTERVAL_DATA as i1 using (interval_no)
		JOIN $INTERVAL_DATA as i2
		JOIN $SCALE_MAP as s1 on i1.interval_no = s1.interval_no
		JOIN $SCALE_MAP as s2 on s2.interval_no = i2.interval_no
	    SET ib.late_bound = if(i1.late_age - 50.0 > i2.late_age, i1.late_age - 50, i2.late_age)
	    WHERE s1.scale_no = s2.scale_no and s1.scale_level = s2.scale_level and i1.late_age = i2.early_age";
    
    $result = $dbh->do($sql);
    
    $sql = "UPDATE $INTERVAL_BUFFER_WORK as ib JOIN $INTERVAL_DATA as i1 using (interval_no)
		JOIN $INTERVAL_DATA as i2
		JOIN $SCALE_MAP as s1 on i1.interval_no = s1.interval_no
		JOIN $SCALE_MAP as s2 on s2.interval_no = i2.interval_no
	    SET ib.early_bound = if(i1.early_age + 50 < i2.early_age, i1.early_age + 50, i2.early_age)
	    WHERE s1.scale_no = s2.scale_no and s1.scale_level = s2.scale_level and i1.early_age = i2.late_age";
    
    $result = $dbh->do($sql);
    
    activateTables($dbh, $INTERVAL_BUFFER_WORK => $INTERVAL_BUFFER);
    
    my $a = 1;	# we can stop here when debugging
}
