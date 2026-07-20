#!/usr/bin/env perl
# 
# msmatch.pl
# 
# Update Macrostrat column/unit matches in the paleobiology database using a remote service.

use strict;

use lib 'lib';
use Getopt::Long qw(:config bundling no_auto_abbrev);

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB);
use ConsoleLog qw(initMessages logMessage logTimestamp);
use MacrostratMatch;

# First parse option switches.

my ($opt_logfile, $opt_test, $opt_error, $opt_verbose, $opt_debug, $opt_bin_id,
    $opt_collection_no, $opt_country, $opt_resgroup, $opt_resume,
    $opt_limit, $opt_dbname, $opt_help);

GetOptions("log=s" => \$opt_logfile,
	   "test" => \$opt_test,
	   "error" => \$opt_error,
	   "verbose|V" => \$opt_verbose,
	   "bin=s" => \$opt_bin_id, 
	   "coll=s" => \$opt_collection_no,
	   "country=s" => \$opt_country,
	   "resgroup=s" => \$opt_resgroup,
	   "resume" => \$opt_resume,
	   "limit=i" => \$opt_limit,
	   "db=s" => \$opt_dbname,
	   "help|h" => \$opt_help,
	   "debug|D" => \$opt_debug) or die "\n";

# If --help was specified, display the help message.

if ( $opt_help )
{
    &ShowHelp;
    exit;
}

# Make sure we have good values for the options.

die "Bad arguments: the value of --country must be a two-letter country code\n"
    if defined $opt_country && $opt_country !~ /^[a-z][a-z]$/i;

if ( $opt_collection_no )
{
    my @colls;
    
    foreach my $c ( split /\s*,\s*/, $opt_collection_no )
    {
	if ( $c =~ /^(col:)?(\d+)$/ )
	{
	    push @colls, $2;
	}

	else
	{
	    die "Bad arguments: the value of --coll must be a comma-separated list of " .
		"collection identifiers\n";
	}
    }
    
    $opt_collection_no = \@colls;
}

if ( $opt_bin_id )
{
    my @bins;
    
    foreach my $b ( split /\s*,\s*/, $opt_bin_id )
    {
	if ( $b =~ /^(bin:)?(\d+)$/ )
	{
	    push @bins, $2;
	}
	
	else
	{
	    die "Bad arguments: the value of --bin must be a comma-separated list of " .
		"bin identifiers\n";
	}
    }
    
    $opt_bin_id = \@bins;
}


# The remaining arguments specify the command.

my ($CMD, @PARAMS);

if ( $ARGV[0] eq 'update' )
{
    if ( $ARGV[1] eq 'existing' )
    {
	$CMD = 'update-existing';
    }
    
    elsif ( $ARGV[1] eq 'columns' )
    {
	$CMD = 'update-columns';
    }
    
    elsif ( $ARGV[1] eq 'new' || ! $ARGV[1] )
    {
	$CMD = 'update-new';
    }
    
    else
    {
	die "Invalid argument '$ARGV[1]'\n";
    }
}

elsif ( $ARGV[0] eq 'cancel' )
{
    if ( $ARGV[1] eq 'existing' )
    {
	$CMD = 'cancel-existing';
    }
    
    elsif ( $ARGV[1] eq 'new' )
    {
	$CMD = 'cancel-new';
    }
    
    else
    {
	die "Invalid argument '$ARGV[1]'\n";
    }
}

elsif ( $ARGV[0] eq 'initialize' )
{
    if ( $ARGV[1] eq 'tables' || $ARGV[1] =~ /^COLLECTION_UNITS/ )
    {
	$CMD = 'init-tables';
	push @PARAMS, $ARGV[1];
    }
    
    else
    {
	die "Invalid argument '$ARGV[1]'\n";
    }
}

elsif ( $ARGV[0] && $ARGV[0] eq 'help' )
{
    &ShowHelp;
    exit;
}

else
{
    die "Invalid command '$ARGV[0]'\n";
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

# Turn off buffering on STDOUT, so that a user can monitor the execution of this command
# as it happens.

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
	initMessages(2, 'Macrostrat Match');
	logMessage(1, '------------------------------------------------------------');
	logTimestamp();
	logMessage(1, 'Macrostrat Match test run');
	die "ERROR: Macrostrat Match\n" if $opt_error;
	exit;
    }
    
    # Otherwise, initialize the output-message subsystem for the run.
    
    my $level = $opt_verbose ? 3 : 2;
    
    initMessages($level, 'Macrostrat Match');
    logMessage(1, '------------------------------------------------------------');
    logTimestamp();
    
    # Get a database handle.
    
    my $dbh = connectDB("config.yml", $opt_dbname || 'pbdb');
    
    # Verify the database that we are working with.
    
    if ( $dbh->{Name} =~ /database=([^;]+)/ )
    {
	logMessage(1, "Using database: $1");
    }
    else
    {
	logMessage(1, "Using connect string: $dbh->{Name}");
    }
    
    # Create an options hash.
    
    my $options = { collection_no => $opt_collection_no,
		    bin_id => $opt_bin_id,
		    country => $opt_country,
		    resgroup => $opt_resgroup,
		    resume => $opt_resume,
		    limit => $opt_limit,
		    verbose => $opt_verbose,
		    debug => $opt_debug };
    
    # Create a new instance of MacrostratMatch.
    
    my $msmatch = MacrostratMatch->new($dbh, $options);
    
    # If we are debugging, stop here.
    
    $DB::single = 1;
    
    # Set up an interrupt handler for the Quit signal.
    
    $SIG{INT} = sub { $MacrostratMatch::QUIT_NOW = 1 };
    
    # Execute the specified command.
    
    if ( $CMD eq 'update-new' )
    {
	$msmatch->updateNew($options);
    }
    
    elsif ( $CMD eq 'update-existing' )
    {
	$msmatch->updateExisting($options);
    }
    
    elsif ( $CMD eq 'update-columns' )
    {
	$msmatch->updateColumns($options);
    }
    
    elsif ( $CMD eq 'cancel-existing' )
    {
	$msmatch->cancelUpdate('existing', $options);
    }
    
    elsif ( $CMD eq 'cancel-new' )
    {
	$msmatch->cancelUpdate('new', $options);
    }
    
    elsif ( $CMD eq 'init-tables' )
    {
	$msmatch->initializeTables(@PARAMS)
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

  update new          Generate matches for new or modified collections.

  update existing     Regenerate the matches for existing collections.

  clear existing      Clear existing paleocoordinates.

  initialize tables   Create or update the database tables used by this command.
 
Available options are:

  --coll=identifier(s)  Update or clear only matches associated with
                        the specified collection identifier or comma-separated list of
                        collection identifier values.

  --bin=identifier(s)   Update or clear only matches associated with the specified
                        geological summary bin identifier or comma-separated list of
                        bin identifier values.

  --country=cc          Update or clear only matches associated with the specified
                        country. The country must be specified as a 2-character code.

  --log=filename        Write lines to the specified log file indicating what this
                        invocation of the command is doing. Otherwise, those lines
                        will be written to standard output.

  --db=dbname           Select a database to use. The default is 'pbdb'.

  --verbose | -V        Emit extra output lines describing what this command is doing.

  --debug | -D          Write debugging output to standard error.

  --help | -h           Display this message and exit.

USAGE
}
