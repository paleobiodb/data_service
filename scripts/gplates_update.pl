#!/opt/local/bin/perl
# 
# gplates_update.pl
# 
# Update paleocoordinates in the paleobiology database using the GPlates
# service.

use strict;

use lib '../lib', 'lib';
use Getopt::Long qw(:config bundling no_auto_abbrev);

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB);
use ConsoleLog qw(initMessages logMessage logTimestamp);
use GPlates qw(ensureTables updatePaleocoords readPlateData);

# First parse option switches.  If we were given an argument, then use that as
# the database name overriding what was in the configuration file.

my ($replace_table, $update_all, $update_empty, $clear_all, $min_age, $max_age, $collection_no,
    $read_plates, $quiet, $verbose, $debug);

GetOptions("replace-table|R" => \$replace_table,
	   "update-all|a" => \$update_all,
	   "update-empty|e" => \$update_empty,
	   "clear-all|x" => \$clear_all,
	   "min-age=s" => \$min_age,
	   "max-age=s" => \$max_age,
	   "coll=s" => \$collection_no,
	   "read-plate-data" => \$read_plates,
	   "quiet|q" => \$quiet,
	   "verbose|v" => \$verbose,
	   "debug" => \$debug) or die;

my $cmd_line_db_name = shift @ARGV;

# Make sure we have good values for the switches.

die "Bad arguments: the value of 'min-age' must be a positive integer\n"
    if defined $min_age && $min_age !~ /^\d+$/;

die "Bad arguments: the value of 'max-age' must be a positive integer\n"
    if defined $max_age && $max_age !~ /^\d+$/;

die "Bad arguments: the value of 'coll' must be a comma-separated list of collection_no values\n"
    if defined $collection_no && $collection_no !~ qr{ ^ \d+ (?: \s*,\s* \d+ )* $ }xs;

# Initialize the output-message subsystem

my $level = $verbose ? 3 : 2;

initMessages($level, 'GPlates update');
logTimestamp() if $verbose;

# Make sure output is not buffered

$| = 1;

# Get a database handle.

my $dbh = connectDB("config.yml", $cmd_line_db_name);

# Verify the database that we are rebuilding.

if ( $dbh->{Name} =~ /database=([^;]+)/ )
{
    logMessage(1, "Using database: $1") if $verbose;
}
else
{
    logMessage(1, "Using connect string: $dbh->{Name}") if $verbose;
}

# If we are debugging, stop here.

$DB::single = 1;

# Make sure we have the proper fields in the collections table.

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
			  update_empty => $update_empty,
			  clear_all => $clear_all,
			  min_age => $min_age,
			  max_age => $max_age,
			  collection_no => $collection_no,
			  quiet => $quiet,
			  verbose => $verbose,
			  debug => $debug });

logTimestamp() if $verbose;

my $a = 1; # we can stop here when debugging.



