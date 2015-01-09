#!/opt/local/bin/perl
# 
# pbdb_check.pl
# 
# Run some simple consistency checks on the PBDB.

use lib '../lib', 'lib';
use Getopt::Std;

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB
		    configData);
use ConsoleLog qw(initMessages
		  logMessage);
use Taxonomy;


# First parse option switches.  If we were given an argument, then use that as
# the database name overriding what was in the configuration file.

my %options;

getopts('m:', \%options);	# a dummy option, for now

my $cmd_line_db_name = shift;

my $mail_to = $options{m};

# Initialize the output-message subsystem

initMessages(2, 'Check');

# Get a database handle and a taxonomy object.

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

my $t = Taxonomy->new($dbh, 'taxon_trees');

# If we are debugging, stop here.

$DB::single = 1;


# Get the set of checks we are to run, from config.yml.

my $checks = configData('checks');

# Now run some consistency checks.

our ($SQL_STRING, $checks_run);

if ( $checks->{max_synonym_no} )
{
    my $max_no = $checks->{max_synonym_no} + 0;
    my $limit = $checks->{max_synonym_limit} + 0;
    
    $SQL_STRING = "SELECT count(*) FROM taxa_tree_cache where synonym_no = $max_no";
    
    my ($count) = $dbh->selectrow_array($SQL_STRING);
    
    if ( $count > 0 && $count <= $limit )
    {
	logMessage(1, "PASSED max_synonym with value of: $count");
    }
    
    else
    {
	logMessage(1, "*******");
	logMessage(1, "FAILED max_synonym with value of: $count (should be $limit)");
	logMessage(1, "*******");
    }
    
    $checks_run++;
}


unless ( $checks_run )
{
    logMessage(1, "*******");
    logMessage(1, "WARNING: no checks were found");
    logMessage(1, "*******");
}



