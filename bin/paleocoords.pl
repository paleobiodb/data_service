#!/usr/bin/env perl
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

my ($opt_logfile, $opt_test, $opt_error, $opt_quiet, $opt_verbose, $opt_debug,
    $opt_replace_table, $opt_update_all, $opt_clear_all, $opt_min_age, $opt_max_age,
    $opt_collection_no, $opt_read_plates, $opt_dbname);

GetOptions("log=s" => \$opt_logfile,
	   "test" => \$opt_test,
	   "error" => \$opt_error,
	   "replace-table|R" => \$opt_replace_table,
	   "update-all|a" => \$opt_update_all,
	   "clear-all|x" => \$opt_clear_all,
	   "min-age=s" => \$opt_min_age,
	   "max-age=s" => \$opt_max_age,
	   "coll=s" => \$opt_collection_no,
	   "read-plate-data" => \$opt_read_plates,
	   "db=s" => \$opt_dbname,
	   "quiet|q" => \$opt_quiet,
	   "verbose|v" => \$opt_verbose,
	   "debug" => \$opt_debug) or die;

# Make sure we have good values for the switches.

die "Bad arguments: the value of 'min-age' must be a positive integer\n"
    if defined $opt_min_age && $opt_min_age !~ /^\d+$/;

die "Bad arguments: the value of 'max-age' must be a positive integer\n"
    if defined $opt_max_age && $opt_max_age !~ /^\d+$/;

die "Bad arguments: the value of 'coll' must be a comma-separated list of collection_no values\n"
    if defined $opt_collection_no && $opt_collection_no !~ qr{ ^ \d+ (?: \s*,\s* \d+ )* $ }xs;

# The argument 'log' specifies that output should be written to the specified file. Otherwise,
# make sure that we are writing to STDOUT. Errors should continue to be written to STDERR, which
# is not directed to the logfile. However, any exception thrown during execution of the processing
# code will be captured and written to the logfile in addition to being written to STDERR.

if ( $opt_logfile )
{
    open(STDOUT, '>>', $opt_logfile) || die "Could not append to $opt_logfile: $!\n";
}

select(STDOUT);
$|=1;

if ( $opt_logfile )
{
    eval {
	DoTask();
    };
    
    if ( $@ )
    {
	print STDOUT $@;
	die $@;
    }
    
    exit;
}

else
{
    DoTask();
    exit;
}


sub DoTask {
    
    # If either option --test or --error was given, just write a test message and stop.
    
    if ( $opt_test || $opt_error )
    {
	initMessages(2, 'PCoords');
	logMessage(1, '------------------------------------------------------------');
	logTimestamp();
	logMessage(1, 'Compute Paleocoords test run');
	die "ERROR: Compute Paleocoords\n" if $opt_error;
	exit;
    }
    
    # Otherwise, initialize the output-message subsystem for the run.
    
    my $level = $opt_verbose ? 3 : 2;
    
    initMessages($level, 'PCoords');
    logMessage(1, '------------------------------------------------------------');
    logTimestamp();
    
    # Get a database handle.
    
    my $dbh = connectDB("config.yml", $opt_dbname || 'pbdb');
    
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
    
    # Make sure we have the proper fields in the collections table.
    
    if ( $opt_replace_table )
    {
	logMessage(1, "replacing table 'paleocoords'");
	ensureTables($dbh, 1);
    }
    
    # If --read-plate-data was specified, then read and parse plate data in
    # geojson format from standard input. Exit afterward.
    
    if ( $opt_read_plates )
    {
	logMessage(1, "Reading plate data from standard input");
	readPlateData($dbh);
	exit;
    }
    
    # Otherwise, update paleocoordinates according to the specified options.
    
    updatePaleocoords($dbh, { update_all => $opt_update_all,
			      clear_all => $opt_clear_all,
			      min_age => $opt_min_age,
			      max_age => $opt_max_age,
			      collection_no => $opt_collection_no,
			      quiet => $opt_quiet,
			      verbose => $opt_verbose,
			      debug => $opt_debug });
    
    logTimestamp() if $opt_verbose;
    logMessage(1, "Done computing paleocoordinates.");
    
    my $a = 1; # we can stop here when debugging.
}


