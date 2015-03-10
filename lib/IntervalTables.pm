# 
# The Paleobiology Database
# 
#   IntervalTables.pm
# 

package IntervalTables;

use strict;

use Carp qw(carp croak);
use Try::Tiny;

use TableDefs qw($INTERVAL_DATA $INTERVAL_MAP $INTERVAL_BRACKET $INTERVAL_BUFFER
		 $SCALE_DATA $SCALE_LEVEL_DATA $SCALE_MAP);
use CoreFunction qw(activateTables loadSQLFile);
use ConsoleLog qw(logMessage);

use base 'Exporter';

our(@EXPORT_OK) = qw(loadIntervalData buildIntervalMap $INTERVAL_DATA $INTERVAL_MAP
		    );


# Table and file names

our $INTERVAL_MAP_WORK = "imn";
our $INTERVAL_BRACKET_WORK = "ibn";
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
		SELECT i.scale_no, i.early_age, i.late_age, 
		       (SELECT cxi.interval_no 
			FROM $INTERVAL_DATA as cxi JOIN $SCALE_MAP as sm using (interval_no)
			WHERE cxi.early_age >= i.early_age and cxi.late_age <= i.late_age 
				and sm.scale_no = i.scale_no
			ORDER BY scale_level desc limit 1) as cx_int_no
		FROM (SELECT distinct s.scale_no, ei.early_age, li.late_age
		      FROM scale_data as s JOIN interval_data as ei JOIN interval_data as li
		      WHERE ei.early_age > li.late_age and
			ei.early_age <= s.early_age and li.late_age >= s.late_age) as i";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      found $result age start/end pairs");
    
    $sql = "	INSERT IGNORE INTO $INTERVAL_MAP_WORK (scale_no, early_age, late_age, cx_int_no)
		SELECT i.scale_no, i.early_age, i.early_age,
		       (SELECT cxi.interval_no 
			FROM interval_data as cxi JOIN $SCALE_MAP as sm using (interval_no)
			WHERE cxi.early_age >= i.early_age and cxi.late_age <= i.early_age 
				and sm.scale_no = i.scale_no
			ORDER BY scale_level desc limit 1) as cx_int_no
		FROM (SELECT distinct s.scale_no, i.early_age
		      FROM scale_data as s JOIN interval_data as i
		      WHERE i.early_age <= s.early_age and i.early_age >= s.late_age) as i";
    
    $result = $dbh->do($sql);
    
    $sql = "	INSERT IGNORE INTO $INTERVAL_MAP_WORK (scale_no, early_age, late_age, cx_int_no)
		SELECT i.scale_no, i.late_age, i.late_age,
		       (SELECT cxi.interval_no 
			FROM interval_data as cxi JOIN $SCALE_MAP as sm using (interval_no)
			WHERE cxi.early_age >= i.late_age and cxi.late_age <= i.late_age 
				and sm.scale_no = i.scale_no
			ORDER BY scale_level desc limit 1) as cx_int_no
		FROM (SELECT distinct s.scale_no, i.late_age
		      FROM scale_data as s JOIN interval_data as i
		      WHERE i.late_age <= s.early_age and i.late_age >= s.late_age) as i";
    
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
		scale_level smallint unsigned not null,
		early_age decimal(9,5),
		late_age decimal(9,5),
		PRIMARY KEY (age, interval_no),
		KEY (interval_no)) Engine=MyISAM");
    
    $sql = "INSERT IGNORE INTO $INTERVAL_BRACKET_WORK
	    SELECT a.age, bi.interval_no, sm.scale_no, sm.scale_level, bi.early_age, bi.late_age
	    FROM $INTERVAL_DATA as bi JOIN $SCALE_MAP as sm using (interval_no)
		JOIN (SELECT distinct early_age as age FROM $INTERVAL_DATA UNION
		      SELECT distinct late_age as age FROM $INTERVAL_DATA) as a
	    WHERE a.age between bi.late_age and bi.early_age";
    
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
		 WHERE ei.level = li.level ORDER BY (ei.early_age - li.late_age), ei.level) as b
			using (early_age, late_age, scale_no)
	    SET i.early_int_no = b.early_int_no, i.late_int_no = b.late_int_no";
    
    $result = $dbh->do($sql);
    
    # Now compute the interval buffer table, which gives the default criteria
    # for resolving temporal locality via the 'buffer' rule when an interval
    # is specified.
    
    buildIntervalBufferMap($dbh);
    
    # Now swap in the new tables.
    
    activateTables($dbh, $INTERVAL_MAP_WORK => $INTERVAL_MAP,
			 $INTERVAL_BRACKET_WORK => $INTERVAL_BRACKET,
			 $INTERVAL_BUFFER_WORK => $INTERVAL_BUFFER);
    
    my $a = 1;		# we can stop here when debugging
}


sub buildIntervalBufferMap {
    
    my ($dbh) = @_;
    
    my ($result, $sql);
    
    logMessage(2, "    setting interval buffer bounds...");
    
    $dbh->do("DROP TABLE IF EXISTS $INTERVAL_BUFFER_WORK");
    
    $dbh->do("CREATE TABLE $INTERVAL_BUFFER_WORK (
		interval_no int unsigned primary key,
		early_bound decimal(9,5),
		late_bound decimal(9,5))");
    
    $sql = "INSERT IGNORE INTO $INTERVAL_BUFFER_WORK (interval_no, early_bound, late_bound)
		SELECT interval_no, early_age + 50, if(late_age - 50 > 0, late_age - 50, 0)
		FROM $INTERVAL_DATA JOIN $SCALE_MAP using (interval_no)
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
    
    my $a = 1;	# we can stop here when debugging
}
