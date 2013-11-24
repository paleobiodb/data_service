# 
# The Paleobiology Database
# 
#   IntervalTables.pm
# 

package IntervalTables;

use strict;

use Carp qw(carp croak);
use Try::Tiny;

use CoreFunction qw(activateTables loadSQLFile);
use ConsoleLog qw(logMessage);

use base 'Exporter';

our(@EXPORT_OK) = qw(loadIntervalData buildIntervalMap $INTERVAL_DATA $INTERVAL_MAP
		     $INTERVAL_BRACKET $SCALE_DATA $SCALE_LEVEL_DATA $SCALE_MAP);


# Table and file names

our $INTERVAL_DATA = "interval_data";
our $SCALE_DATA = "scale_data";
our $SCALE_LEVEL_DATA = "scale_level_data";
our $SCALE_MAP = "scale_map";
our $INTERVAL_BRACKET = "interval_bracket";
our $INTERVAL_MAP = "interval_map";

our $INTERVAL_MAP_WORK = "imn";
our $INTERVAL_BRACKET_WORK = "ibn";

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
		cx_int_no int unsigned not null default 0,
		early_int_no int unsigned,
		late_int_no int unsigned,
		PRIMARY KEY (early_age, late_age, scale_no)) Engine=MyISAM");
    
    logMessage(2, "    computing containing intervals");
    
    $sql = "	INSERT IGNORE $INTERVAL_MAP_WORK (scale_no, early_age, late_age, cx_int_no)
		SELECT i.scale_no, i.base_age, i.top_age, 
		       (SELECT cxi.interval_no 
			FROM $INTERVAL_DATA as cxi JOIN $SCALE_MAP as sm using (interval_no)
			WHERE cxi.base_age >= i.base_age and cxi.top_age <= i.top_age 
				and sm.scale_no = i.scale_no
			ORDER BY level desc limit 1) as cx_int_no
		FROM (SELECT distinct s.scale_no, ei.base_age, li.top_age
		      FROM scale_data as s JOIN interval_data as ei JOIN interval_data as li
		      WHERE ei.base_age > li.top_age and
			ei.base_age <= s.base_age and li.top_age >= s.top_age) as i";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      found $result age start/end pairs");
    
    $sql = "	INSERT IGNORE INTO $INTERVAL_MAP_WORK (scale_no, early_age, late_age, cx_int_no)
		SELECT i.scale_no, i.base_age, i.base_age,
		       (SELECT cxi.interval_no 
			FROM interval_data as cxi JOIN $SCALE_MAP as sm using (interval_no)
			WHERE cxi.base_age >= i.base_age and cxi.top_age <= i.base_age 
				and sm.scale_no = i.scale_no
			ORDER BY level desc limit 1) as cx_int_no
		FROM (SELECT distinct s.scale_no, i.base_age
		      FROM scale_data as s JOIN interval_data as i
		      WHERE i.base_age <= s.base_age and i.base_age >= s.top_age) as i";
    
    $result = $dbh->do($sql);
    
    $sql = "	INSERT IGNORE INTO $INTERVAL_MAP_WORK (scale_no, early_age, late_age, cx_int_no)
		SELECT i.scale_no, i.top_age, i.top_age,
		       (SELECT cxi.interval_no 
			FROM interval_data as cxi JOIN $SCALE_MAP as sm using (interval_no)
			WHERE cxi.base_age >= i.top_age and cxi.top_age <= i.top_age 
				and sm.scale_no = i.scale_no
			ORDER BY level desc limit 1) as cx_int_no
		FROM (SELECT distinct s.scale_no, i.top_age
		      FROM scale_data as s JOIN interval_data as i
		      WHERE i.top_age <= s.base_age and i.top_age >= s.top_age) as i";
    
    $result += $dbh->do($sql);
    
    logMessage(2, "      found $result age boundary points");
    
    # Now, for each of these entries we need to see if a range of intervals in
    # a finer level of the scale can more precisely bracket the age range.  To
    # do this, we will need an auxiliary table which associates each endpoint
    # age with all of the intervals that include it.
    
    logMessage(2, "    computing interval brackets...");
    
    $dbh->do("DROP TABLE IF EXISTS $INTERVAL_BRACKET_WORK");
    
    $dbh->do("CREATE TABLE $INTERVAL_BRACKET_WORK (
		age decimal(9,5),
		interval_no int unsigned not null,
		scale_no smallint unsigned not null,
		level smallint unsigned not null,
		base_age decimal(9,5),
		top_age decimal(9,5),
		PRIMARY KEY (age, interval_no),
		KEY (interval_no)) Engine=MyISAM");
    
    $sql = "INSERT IGNORE INTO $INTERVAL_BRACKET_WORK
	    SELECT a.age, bi.interval_no, sm.scale_no, sm.level, bi.base_age, bi.top_age
	    FROM $INTERVAL_DATA as bi JOIN $SCALE_MAP as sm using (interval_no)
		JOIN (SELECT distinct base_age as age FROM $INTERVAL_DATA UNION
		      SELECT distinct top_age as age FROM $INTERVAL_DATA) as a
	    WHERE a.age between bi.top_age and bi.base_age";
    
    $result = $dbh->do($sql);
    
    # Now we can figure out the best bracket of same-level intervals that
    # covers each possible age range from each possible scale.  Here "best"
    # means "with closest bounds, where ties are broken by lower level".
    
    logMessage(2, "    setting interval brackets...");
    
    $sql = "UPDATE $INTERVAL_MAP_WORK as i JOIN
		(SELECT i.early_age, i.late_age, i.scale_no,
			ei.interval_no as early_int_no, li.interval_no as late_int_no
		 FROM $INTERVAL_MAP_WORK as i
		     JOIN $INTERVAL_BRACKET_WORK as ei on ei.age = i.early_age and ei.scale_no = i.scale_no
		     JOIN $INTERVAL_BRACKET_WORK as li on li.age = i.late_age and li.scale_no = i.scale_no
		 WHERE ei.level = li.level ORDER BY (ei.base_age - li.top_age), ei.level) as b
			using (early_age, late_age, scale_no)
	    SET i.early_int_no = b.early_int_no, i.late_int_no = b.late_int_no";
    
    $result = $dbh->do($sql);
    
    # Now swap in the new tables.
    
    activateTables($dbh, $INTERVAL_MAP_WORK => $INTERVAL_MAP,
			 $INTERVAL_BRACKET_WORK => $INTERVAL_BRACKET);
    
    my $a = 1;		# we can stop here when debugging
}



