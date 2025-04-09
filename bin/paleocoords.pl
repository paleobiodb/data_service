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
use PaleoCoords;

# First parse option switches.  If we were given an argument, then use that as
# the database name overriding what was in the configuration file.

my ($opt_logfile, $opt_test, $opt_error, $opt_quiet, $opt_verbose, $opt_debug,
    $opt_min_age, $opt_max_age, $opt_model, $opt_collection_no, $opt_dbname, $opt_bins,
    $opt_all, $opt_help);

GetOptions("log=s" => \$opt_logfile,
	   "test" => \$opt_test,
	   "error" => \$opt_error,
	   "bins" => \$opt_bins, 
	   "min-age=s" => \$opt_min_age,
	   "max-age=s" => \$opt_max_age,
	   "coll=s" => \$opt_collection_no,
	   "model|M=s", \$opt_model,
	   "all|a", \$opt_all,
	   "db=s" => \$opt_dbname,
	   "quiet|q" => \$opt_quiet,
	   "verbose|v" => \$opt_verbose,
	   "help|h" => \$opt_help,
	   "debug|D" => \$opt_debug) or die;

# If --help was specified, display the help message.

if ( $opt_help )
{
    &ShowHelp;
    exit;
}

# Make sure we have good values for the switches.

die "Bad arguments: the value of 'min-age' must be a positive integer\n"
    if defined $opt_min_age && $opt_min_age !~ /^\d+$/;

die "Bad arguments: the value of 'max-age' must be a positive integer\n"
    if defined $opt_max_age && $opt_max_age !~ /^\d+$/;

die "Bad arguments: the value of 'coll' must be a comma-separated " .
    "list of collection_no values\n"
    if defined $opt_collection_no && 
       $opt_collection_no !~ qr{ ^ \d+ (?: \s*,\s* \d+ )* $ }xs;

# The remaining arguments specify the command.

my ($CMD, @PARAMS);

if ( $ARGV[0] eq 'update' )
{
    if ( $ARGV[1] eq 'existing' )
    {
	$CMD = 'update-existing';
    }
    
    elsif ( $ARGV[1] eq 'new' || ! $ARGV[1] )
    {
	$CMD = 'update-new';
    }
    
    else
    {
	die "Invalid argument '$ARGV[1]'";
    }
}

elsif ( $ARGV[0] eq 'clear' )
{
    if ( $ARGV[1] eq 'all' || $ARGV[1] eq 'existing' )
    {
	$CMD = 'clear-existing';
    }
    
    else
    {
	die "You must specify 'clear all' to clear paleocoordinates";
    }
}

elsif ( $ARGV[0] eq 'initialize' || $ARGV[0] eq 'replace' )
{
    if ( $ARGV[1] eq 'tables' || $ARGV[1] =~ /^PCOORD/ )
    {
	$CMD = 'init-tables';
	push @PARAMS, $ARGV[1];
	push @PARAMS, 1 if $ARGV[0] eq 'replace';
    }
    
    elsif ( $ARGV[1] eq 'models' )
    {
	$CMD = 'init-models';
    }
    
    elsif ( $ARGV[1] eq 'plates' )
    {
	$CMD = 'init-plates';
    }
    
    else
    {
	die "Invalid argument '$ARGV[1]'";
    }
}

elsif ( $ARGV[0] && $ARGV[0] ne 'help' )
{
    die "Invalid command '$ARGV[0]'";
}

else
{
    &ShowHelp;
    exit;
}


# The option --logfile specifies that output should be written to the specified
# file. Otherwise, make sure that we are writing to STDOUT. Errors should
# continue to be written to STDERR, which is not directed to the logfile.
# However, any exception thrown during execution of the processing code will be
# captured and written to the logfile in addition to being written to STDERR.

if ( $opt_logfile )
{
    open(STDOUT, '>>', $opt_logfile) || die "Could not append to $opt_logfile: $!\n";
}

select(STDOUT);
$|=1;

if ( $opt_logfile )
{
    eval {
	DoTask($CMD, @PARAMS);
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
    DoTask($CMD, @PARAMS);
    exit;
}


sub DoTask {
    
    my ($CMD, @PARAMS) = @_;
    
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
    
    # Create a new instance of PaleoCoords.
    
    my $pcoords = PaleoCoords->new($dbh, $opt_debug);
    
    # Create an options hash.
    
    my $options = { min_age => $opt_min_age, 
		    max_age => $opt_max_age,
		    collection_no => $opt_collection_no,
		    model => $opt_model,
		    all => $opt_all,
		    verbose => $opt_verbose };
    
    # If we are debugging, stop here.
    
    $DB::single = 1;
    
    # Execute the specified command.
    
    if ( $CMD eq 'update-new' )
    {
	$pcoords->updateNew($options);
    }
    
    elsif ( $CMD eq 'update-existing' )
    {
	$pcoords->updateExisting($options);
    }
    
    elsif ( $CMD eq 'clear-existing' )
    {
	$pcoords->clearCoords($options);
    }
    
    elsif ( $CMD eq 'init-tables' )
    {
	$pcoords->initializeTables(@PARAMS)
    }
    
    elsif ( $CMD eq 'init-models' )
    {
	$pcoords->initializeModels();
    }
    
    elsif ( $CMD eq 'init-plates' )
    {
	$pcoords->initializePlates();
    }
    
    my $a = 1; # we can stop here when debugging.
}


sub ShowHelp {
    
    print <<USAGE;
Usage: $0 [options] [command]

This command generates paleocoordinates for collections in The Paleobiology
Database using a paleocoordinate service. The URIs for the service must
be specified in the configuration file for the paleobiology database API.

Available commands are:

  update new          Generate paleocoordinates for new or modified collections.

  update existing     Regenerate paleocoordinates for existing collections.

  clear all           Clear existing paleocoordinates.

  initialize tables   Create the database tables used by this command. Any
                      existing tables will be renamed using the prefix _bak,
                      so that if there is existing data it can be copied over.

  replace tables      Replace the database tables used by this command. All
                      existing paleocoordinate data will be lost.

  initialize models   Fetch the list of models provided by the service and
                      store those entries in the database.

  initialize plates   Fetch the list of plates identifiers used by each model
                      and store those entries in the database.
 
Available options are:

  --model=model       Update or clear only paleocoordinates associated with
                      the specified model or comma-separated list of model names.

  --coll=collection   Update or clear only paleocoordinates associated with
                      the specified collection_no or comma-separated list of
                      collection_no values.

  --min-age=age       Update only paleocoordinates whose ages are greater than or
                      equal to the specified age.

  --max-age=age       Update only paleocoordinates whose ages are less than or
                      equal to the maximum age.

  --log=filename      Write lines to the specified log file indicating what this
                      invocation of the command is doing. Otherwise, those lines
                      will be written to standard output.

  --db=dbname         Select a database to use. The default is 'pbdb'.

  --debug | -D        Write debugging output to standard error.

  --help | -h         Display this message and exit.

USAGE
}
