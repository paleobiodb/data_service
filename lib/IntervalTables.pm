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
		     $INTERVAL_BRACKET $SCALE_DATA $SCALE_LEVEL_DATA $TEN_MY_BINS);


# Table and file names

our $INTERVAL_DATA = "interval_data";
our $SCALE_DATA = "scale_data";
our $SCALE_LEVEL_DATA = "scale_level_data";
our $INTERVAL_BRACKET = "interval_bracket";
our $INTERVAL_MAP = "interval_map";
our $TEN_MY_BINS = "ten_my_bins";

our $INTERVAL_MAP_WORK = "imn";
our $INTERVAL_BRACKET_WORK = "ibn";

# Template files

our $INTERVAL_DATA_FILE = "system/interval_data.sql";
our $SCALE_DATA_FILE = "system/scale_data.sql";



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
    
    my $scale_result;
    my $interval_result;
    
    unless ( $force )
    {
	try {
	    $scale_result = $dbh->do("SELECT COUNT(*) FROM $SCALE_DATA");
	};
	
	try {
	    $interval_result = $dbh->do("SELECT COUNT(*) FROM $INTERVAL_DATA");
	};
    }
    
    # If the relevant tables were not found, try to read the data in from
    # files on disk.
    
    my $update = 0;
    
    if ( $force or not $scale_result )
    {
	logMessage(2, "loading scale data table from system/scale_data.sql...");
	loadSQLFile($dbh, $SCALE_DATA_FILE);
	$update = 1;
    }
    
    if ( $force or not $interval_result )
    {
	logMessage(2, "loading interval data table from system/interval_data.sql...");
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
		cx_int_no int unsigned not null,
		early_int_range int unsigned not null,
		late_int_range int unsigned not null,
		PRIMARY KEY (early_age, late_age, scale_no)) Engine=MyISAM");
    
    logMessage(2, "    computing containing intervals");
    
    $sql = "INSERT IGNORE INTO $INTERVAL_MAP_WORK (scale_no, early_age, late_age, cx_int_no)
		SELECT s.scale_no, p.early_age, p.late_age, i.interval_no
		FROM (SELECT ei.base_age as early_age, li.top_age as late_age
			FROM $INTERVAL_DATA as ei JOIN $INTERVAL_DATA as li
			WHERE (ei.base_age >= li.base_age or ei.top_age >= li.top_age)) as p
		    JOIN $SCALE_DATA as s
		    LEFT JOIN $INTERVAL_DATA as i on i.base_age >= p.early_age and i.top_age <= p.late_age
		        and i.scale_no = s.scale_no
		ORDER BY i.level desc";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      found $result age start/end pairs");
    
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
	    SELECT a.age, bi.interval_no, bi.scale_no, bi.level, bi.base_age, bi.top_age FROM $INTERVAL_DATA as bi
		JOIN (SELECT distinct base_age as age FROM $INTERVAL_DATA UNION
		      SELECT distinct top_age as age FROM $INTERVAL_DATA) as a
	    WHERE a.age between bi.top_age and bi.base_age and bi.scale_no > 0";
    
    $result = $dbh->do($sql);
    
    # Now we can figure out the best bracket of same-level intervals that
    # covers each possible age range from each possible scale.  Here "best"
    # means "with closest bounds, where ties are broken by lower level".
    
    logMessage(2, "    setting interval brackets...");
    
    $sql = "UPDATE $INTERVAL_MAP_WORK as i JOIN
		(SELECT i.early_age, i.late_age, i.scale_no,
			ei.interval_no as early_int_range, li.interval_no as late_int_range
		 FROM $INTERVAL_MAP_WORK as i
		     JOIN $INTERVAL_BRACKET_WORK as ei on ei.age = i.early_age and ei.scale_no = i.scale_no
		     JOIN $INTERVAL_BRACKET_WORK as li on li.age = i.late_age and li.scale_no = i.scale_no
		 WHERE ei.level = li.level ORDER BY (ei.base_age - li.top_age), ei.level) as b
			using (early_age, late_age, scale_no)
	    SET i.early_int_range = b.early_int_range, i.late_int_range = b.late_int_range";
    
    $result = $dbh->do($sql);
    
    # Now swap in the new tables.
    
    activateTables($dbh, $INTERVAL_MAP_WORK => $INTERVAL_MAP,
			 $INTERVAL_BRACKET_WORK => $INTERVAL_BRACKET);
    
    my $a = 1;		# we can stop here when debugging
}



