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
use CollectionTables qw(buildCollectionTables);
use OccurrenceTables qw(buildOccurrenceTables updateOccLft);
use TaxonTables qw(populateOrig
		   buildTaxonTables
		   buildTaxaCacheTables computeGenSp);
use Taxonomy;
use DiversityTables qw(buildDiversityTables);


# First parse option switches.  If we were given an argument, then use that as
# the database name overriding what was in the configuration file.

my %options;

getopts('tT:mbcKuivryd', \%options);

my $cmd_line_db_name = shift;


# Get a database handle and a taxonomy object.

my $dbh = connectDB("config.yml", $cmd_line_db_name);

my $t = Taxonomy->new($dbh, 'taxon_trees');

# If we are debugging, stop here.

$DB::single = 1;


# Initialize the output-message subsystem

initMessages(2, 'Rebuild');


# Call the routines that build the various caches, depending upon the options
# that were specified.

my $interval_data = $options{i};
my $interval_map = $options{u};
my $rank_map = $options{r};

my $collection_tables = $options{c};
my $occurrence_tables = $options{m};

my $taxon_tables = $options{t} or $options{T};
my $taxon_steps = $options{T};
my $old_taxon_tables = $options{y};
my $diversity_tables = $options{d};

my $options = { taxon_steps => $options{T},
		colls_cluster => $options{k} };


# The option -i causes a forced reload of the interval data from the source
# data files.  Otherwise, do a (non-forced) call to LoadIntervalData if any
# function has been selected that requires it.

if ( $interval_data )
{
    loadIntervalData($dbh, 1);
}

elsif ( $interval_map || $collection_tables || $occurrence_tables || $taxon_tables )
{
    loadIntervalData($dbh);
}

# The option -u causes the interval map tables to be (re)computed.

if ( $interval_map )
{
    buildIntervalMap($dbh);
}

# The option -r causes the taxon rank map to be (re)generated.

if ( $rank_map )
{
    createRankMap($dbh);
}

# The option -c causes the collection tables to be (re)computed.

if ( $collection_tables )
{
    my $bins = configData('bins');
    buildCollectionTables($dbh, $bins, $options);
}

# The option -m causes the occurrence tables to be (re)computed.

if ( $occurrence_tables )
{
    buildOccurrenceTables($dbh, $options);
}

# The option -t or -T causes the taxonomy tables to be (re)computed.  If -T
# was specified, its value should be a sequence of steps (a-h) to be carried
# out. 

if ( $taxon_tables )
{
    populateOrig($dbh);
    buildTaxonTables($dbh, 'taxon_trees', $options);
    updateOccLft($dbh);
}


# The option -y causes the "classic" taxa_tree_cache table to be computed.

if ( $old_taxon_tables )
{
    buildTaxaCacheTables($dbh, 'taxon_trees');
}


# temp

if ( $diversity_tables )
{
    buildDiversityTables($dbh, 'taxon_trees');
}


print "done rebuilding tables\n";

# Done!

exit;



