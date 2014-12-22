#!/opt/local/bin/perl
# 
# build_tables.pl
# 
# Build (or rebuild) the tables necessary for the "new" version of the
# Paleobiology Database.  These tables all depend upon the contents of the
# "old" tables.

use lib '../lib', 'lib';
use Getopt::Std;

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB
		    configData);
use ConsoleLog qw(initMessages
		  logMessage);
use IntervalTables qw(loadIntervalData
		      buildIntervalMap);
use CollectionTables qw(buildCollectionTables buildStrataTables);
use OccurrenceTables qw(buildOccurrenceTables buildTaxonSummaryTable);
use TaxonTables qw(populateOrig
		   buildTaxonTables rebuildAttrsTable
		   buildTaxaCacheTables computeGenSp);
use TaxonPics qw(getPics selectPics);
use Taxonomy;
use DiversityTables qw(buildDiversityTables);


my $cmd_line_db_name = shift;


# Initialize the output-message subsystem

initMessages(2, 'Taxonomy test');

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


my $a = 1;
my $a = 2;
my $a = 3;

exit;



