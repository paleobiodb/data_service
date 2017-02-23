#!/opt/local/bin/perl
# 
# build_tables.pl
# 
# Build (or rebuild) the tables necessary for the "new" version of the
# Paleobiology Database.  These tables all depend upon the contents of the
# "old" tables.

use strict;

use lib '../lib', 'lib';
use Getopt::Std;

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB
		    configData);
use ConsoleLog qw(initMessages
		  logMessage
		  logTimestamp);
use IntervalTables qw(loadIntervalData
		      buildIntervalMap);
use CollectionTables qw(buildCollectionTables buildStrataTables buildLithTables);
use OccurrenceTables qw(buildOccurrenceTables buildTaxonSummaryTable buildOccIntervalMaps);
use SpecimenTables qw(buildSpecimenTables);
use TaxonTables qw(populateOrig
		   buildTaxonTables rebuildAttrsTable
		   buildTaxaCacheTables computeGenSp);
# use TimescaleTables qw(establishTimescaleTables);
use TaxonPics qw(getPics selectPics);
use Taxonomy;
use DiversityTables qw(buildDiversityTables buildPrevalenceTables);
use ContributorTables qw(buildContributorTables);
use InstitutionTables qw(createInstitutionTables);


# First parse option switches.  If we were given an argument, then use that as
# the database name overriding what was in the configuration file.

my %options;

getopts('tT:OmR:bcKIUuivrydqspfAMSLC', \%options);

my $cmd_line_db_name = shift;


# Initialize the output-message subsystem

initMessages(2, 'Rebuild');
logTimestamp();

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

# Call the routines that build the various caches, depending upon the options
# that were specified.

my $force = $options{f};
my $institution_tables = $options{I};
my $interval_map = $options{U};
my $rank_map = $options{r};
my $taxon_pics = $options{p};

my $collection_tables = $options{c};
my $occurrence_tables = $options{m};
my $occurrence_int_maps = $options{M};
my $occurrence_reso = $options{R};
my $diversity_tables = $options{d};
my $prevalence_tables = $options{q};
my $timescale_tables = $options{S};
my $contrib_tables = $options{C};

my $taxon_tables = 1 if $options{t} || $options{T};
my $taxon_steps = $options{T};
my $old_taxon_tables = $options{y};
my $strata_tables = $options{s};
my $lith_tables = $options{L};

my $options = { taxon_steps => $options{T},
		colls_cluster => $options{k},
		no_rebuild_cache => $options{O},
		no_rebuild_div => $options{A} };


# The option -i causes a forced reload of the interval data from the source
# data files.  Otherwise, do a (non-forced) call to LoadIntervalData if any
# function has been selected that requires it.

if ( $institution_tables )
{
    createInstitutionTables($dbh);
}

if ( $interval_map || $collection_tables || $occurrence_tables )
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
    TaxonTables::createRankMap($dbh);
    CollectionTables::createCountryMap($dbh, 1);
}

# The option -c causes the collection tables to be (re)computed.

if ( $collection_tables )
{
    my $bins = configData('bins');
    buildCollectionTables($dbh, $bins, $options);
}

if ( $lith_tables && ! $collection_tables )
{
    buildLithTables($dbh);
}

# The option -m causes the occurrence tables to be (re)computed.  -R also
# triggers this.

my ($occ_options);

if ( $occurrence_reso )
{
    $occurrence_reso //= '';
    
    if ( $occurrence_reso =~ qr{p}x )
    {
	$occ_options->{accept_periods} = 1;
    }
    
    if ( $occurrence_reso =~ qr{^[^/\d]*(\d+)}x )
    {
	$occ_options->{epoch_bound} = $1;
    }
    
    if ( $occurrence_reso =~ qr{/(\d+)}x )
    {
	$occ_options->{interval_bound} = $1;
    }
}

if ( $occurrence_tables )
{
    populateOrig($dbh);
    buildOccurrenceTables($dbh, $occ_options);
}

elsif ( $occurrence_reso )
{
    populateOrig($dbh);
    buildTaxonSummaryTable($dbh, $occ_options);
    rebuildAttrsTable($dbh, 'taxon_trees');
}

elsif ( $occurrence_int_maps )
{
    buildOccIntervalMaps($dbh);
}


# The option -t or -T causes the taxonomy tables to be (re)computed.  If -T
# was specified, its value should be a sequence of steps (a-h) to be carried
# out. 

if ( $taxon_tables )
{
    populateOrig($dbh);
    buildTaxonTables($dbh, 'taxon_trees', $options);
}


# The option -y causes the "classic" taxa_tree_cache table to be computed.

if ( $old_taxon_tables )
{
    # First make sure that nothing else is updating the taxa_tree_cache table
    # while we work.
    
    my $count = 0;
    
    while (1)
    {
	my ($mutex, $created) = $dbh->selectrow_array("SELECT * FROM tc_mutex");
	
	if ( defined $mutex )
	{
	    if ( ++$count > 6 )
	    {
		logMessage(1, "Could not acquire mutex: aborting taxa_tree_cache rebuild");
		exit;
	    }
	    
	    else
	    {
		logMessage(2, "   tc_mutex is locked...");
		sleep(10);
	    }
	}
	
	else
	{
	    $dbh->do("INSERT INTO tc_mutex (mutex_id,created) VALUES (999999,NOW())");
	    logMessage(1, "Acquired lock on tc_mutex");
	    last;
	}
    }
    
    # Then update the cache tables.
    
    eval {
	buildTaxaCacheTables($dbh, 'taxon_trees');
    };
    
    my $error = $@;
    
    $dbh->do("DELETE FROM tc_mutex");
    
    if ( $error )
    {
	logMessage(1, "An error occurred during taxa_tree_cache rebuild: $@");
    }
    
    logMessage(1, "Released lock on tc_mutex");
}


# The option -d causes the diversity tables to be (re)computed.

if ( $diversity_tables )
{
    buildDiversityTables($dbh, 'taxon_trees', $options);
}

if ( $prevalence_tables )
{
    buildPrevalenceTables($dbh, 'taxon_trees', $options);
}

# The option -p causes taxon pictures to be fetched from phylopic.org

if ( $taxon_pics )
{
    getPics($dbh, 'taxon_trees', $force);
    selectPics($dbh, 'taxon_trees');
}

# temp

if ( $strata_tables )
{
    buildStrataTables($dbh);
}

if ( $occurrence_tables )
{
    buildSpecimenTables($dbh);
}

if ( $timescale_tables )
{
    establishTimescaleTables($dbh);
}

if ( $contrib_tables )
{
    my $options = { new_tables => $options{f} };
    buildContributorTables($dbh, $options);
}

print "done rebuilding tables\n";

# Done!

exit;



