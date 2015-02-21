#!/opt/local/bin/perl
# 
# gplates_update.pl
# 
# Update paleocoordinates in the paleobiology database using the GPlates
# service.

use lib '../lib', 'lib';
use Getopt::Long qw(:config bundling no_auto_abbrev);

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB);
use ConsoleLog qw(initMessages logMessage logTimestamp);
use GPlates qw(ensureTables updatePaleocoords readPlateData);

# First parse option switches.  If we were given an argument, then use that as
# the database name overriding what was in the configuration file.

my ($replace_table, $update_all, $clear_all, $min_age, $max_age, $read_plates, $debug);

GetOptions("replace-table|R" => \$replace_table,
	   "update-all|a" => \$update_all,
	   "clear-all|x" => \$clear_all,
	   "min-age=i" => \$min_age,
	   "max-age=i" => \$max_age,
	   "read-plate-data" => \$read_plates,
	   "debug" => \$debug) or die;

my $cmd_line_db_name = shift @ARGV;


# Initialize the output-message subsystem

initMessages(2, 'GPlates update');
logTimestamp();

# Get a database handle.

my $dbh = connectDB("config.yml", $cmd_line_db_name);

# Verify the database that we are rebuilding.

if ( $dbh->{Name} =~ /database=([^;]+)/ )
{
    logMessage(1, "Using database: $1");
}
else
{
    logMessage(1, "Using connect string: $dbh->{Name}");
}

# If we are debugging, stop here.

$DB::single = 1;

# Make sure we hvae the proper fields in the collections table.

if ( $replace_table )
{
    logMessage(1, "replacing table 'paleocoords'");
    ensureTables($dbh, 1);
}


# If --read-plate-data was specified, then read and parse plate data in
# geojson format from standard input.

if ( $read_plates )
{
    logMessage(1, "Reading plate data from standard input");
    readPlateData($dbh);
    exit;
}


# Otherwise, update the coordinates.

updatePaleocoords($dbh, { update_all => $update_all,
			  clear_all => $clear_all,
			  min_age => $min_age,
			  max_age => $max_age,
			  debug => $debug });

logTimestamp();

my $a = 1; # we can stop here when debugging.



