#!/usr/bin/env perl
# 
# build_tables.pl
# 
# Build (or rebuild) the tables necessary for the "new" version of the
# Paleobiology Database.  These tables all depend upon the contents of the
# "old" tables.

use strict;

use lib 'lib';
use Getopt::Long;
use Try::Tiny;

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB
		    configData);
use ConsoleLog qw(initMessages
		  logMessage
		  logTimestamp);
use IntervalTables qw(loadIntervalData buildIntervalMap);
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


# First parse option switches.  If we were given an argument, then use that as
# the database name overriding what was in the configuration file.

Getopt::Long::Configure("bundling");

my ($opt_nightly, $opt_logfile, $opt_test, $opt_error,
    $taxon_tables, $collection_tables, $occurrence_tables, $diversity_tables, $interval_map,
    $occurrence_int_maps, $taxon_summary_table, $prevalence_tables, $country_map,
    $old_taxon_tables, $taxon_steps);

GetOptions( "nightly" => \$opt_nightly,
	    "log=s" => \$opt_logfile,
	    "test" => \$opt_test,
	    "error" => \$opt_error,
	    "taxonomy|t" => \$taxon_tables,
	    "collections|c" => \$collection_tables,
	    "occurrences|m" => \$occurrence_tables,
	    "prevalence|P" => \$prevalence_tables,
	    "I" => \$interval_map,
	    "M" => \$occurrence_int_maps,
	    "S" => \$taxon_summary_table,
	    "countries|C" => \$country_map,
	    "listcache|y" => \$old_taxon_tables,
	    "prevalence|p" => \$prevalence_tables,
	    "diversity|d" => \$diversity_tables,
	    "steps|T=s" => \$taxon_steps );

my $cmd_line_db_name = shift;

# The argument 'nightly' is the same as -cmty. If this is being run on
# Sunday, rebuild the diversity tables as well.

if ( $opt_nightly )
{
    $taxon_tables = 1;
    $collection_tables = 1;
    $occurrence_tables = 1;
    $old_taxon_tables = 1;
}

# The argument 'log' specifies that output should be written to the specified file.

if ( $opt_logfile )
{
    open(STDOUT, '>>', $opt_logfile) || die "Could not append to $opt_logfile: $!\n";
}

select(STDOUT);
$|=1;

# If the argument 'log' was specified, run the specified table builds inside an eval. If an error occurs,
# print it to the log and then re-throw it so it gets printed to STDERR as well.

if ( $opt_logfile )
{
    eval {
	BuildTables();
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
    BuildTables();
    exit;
}

# getopts('tT:OmR:bcKUuIivrydqspfAMSL', \%options);


sub BuildTables {
    
    # If either option --test or --error was given, just write a test message and stop.
    
    if ( $opt_test || $opt_error )
    {
	initMessages(2, 'Build test');
	logMessage(1, '------------------------------------------------------------');
	logTimestamp();
	logMessage(1, 'Test log message');
	die "ERROR: Build test\n" if $opt_error;
	exit;
    }
    
    # Initialize the output-message subsystem
    
    initMessages(2, 'Build');
    logMessage(1, '------------------------------------------------------------');
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
    
    my %options;
    
    my $force = $options{f};
    my $interval_data = $options{I};
    # my $interval_map = $options{U};
    my $rank_map = $options{r};
    my $taxon_pics = $options{p};
    
    # my $collection_tables = $options{c};
    # my $occurrence_tables = $options{m};
    # my $occurrence_int_maps = $options{M};
    my $occurrence_reso = $options{R};
    # my $diversity_tables = $options{d};
    # my $prevalence_tables = $options{q};
    my $timescale_tables = $options{S};
    
    # my $taxon_tables = 1 if $options{t} || $options{T};
    # my $taxon_steps = $options{T};
    # my $old_taxon_tables = $options{y};
    my $strata_tables = $options{s};
    my $lith_tables = $options{L};
    
    # my $options = { taxon_steps => $options{T},
    # 		colls_cluster => $options{k},
    # 		no_rebuild_cache => $options{O},
    # 		no_rebuild_div => $options{A} };
    
    my $options = { taxon_steps => $taxon_steps };
    
    # The option -r causes the taxon rank map to be (re)generated.
    
    if ( $rank_map )
    {
	TaxonTables::createRankMap($dbh);
    }
    
    if ( $country_map )
    {
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
    
    if ( $interval_map )
    {
	buildIntervalMap($dbh);
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
    
    elsif ( $taxon_summary_table )
    {
	buildTaxonSummaryTable($dbh);
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
    
    
    # The option -d causes the diversity and prevalence tables to be (re)computed.
    
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
    
    logTimestamp();
    logMessage(1, "Done rebuilding tables.");
    
    # Done!
}

