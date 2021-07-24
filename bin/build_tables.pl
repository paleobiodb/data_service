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

# The following modules are all part of the "new" pbdb.

use CoreFunction qw(connectDB configData);
use ConsoleLog qw(initMessages logMessage logTimestamp);
use IntervalTables qw(loadIntervalData buildIntervalMap);
use CollectionTables qw(buildCollectionTables buildStrataTables buildLithTables);
use OccurrenceTables qw(buildOccurrenceTables buildTaxonSummaryTable buildTaxonCollectionsTable);
use SpecimenTables qw(buildSpecimenTables);
use TaxonTables qw(populateOrig buildTaxonTables rebuildAttrsTable buildTaxaCacheTables);
use TaxonPics qw(getPics selectPics);
use Taxonomy;
use DiversityTables qw(buildDiversityTables buildTotalDiversityTables buildPrevalenceTables);


# First parse option switches.

Getopt::Long::Configure("bundling");

my ($opt_logfile, $opt_test, $opt_error, $opt_debug, $opt_force, $opt_database,
    $opt_nightly, $opt_weekly, $opt_collections, $opt_occurrences, $opt_taxonomy, $opt_taxa_cache,
    $opt_diversity, $opt_prevalence, $opt_occ_summary, $opt_taxon_colls, $opt_taxon_pics,
    $opt_occurrence_reso, $opt_global_only, $opt_taxon_steps, $opt_interval_data, $opt_diagnostic);

GetOptions( "log=s" => \$opt_logfile,
	    "test" => \$opt_test,
	    "error" => \$opt_error,
	    "debug" => \$opt_debug,
	    "database=s" => \$opt_database,
	    "nightly" => \$opt_nightly,
	    "weekly" => \$opt_weekly,
	    "collections|c" => \$opt_collections,
	    "occurrences|m" => \$opt_occurrences,
	    "taxonomy|t" => \$opt_taxonomy,
	    "taxa-cache|y" => \$opt_taxa_cache,
	    "diversity|d" => \$opt_diversity,
	    "prevalence|v" => \$opt_prevalence,
	    "occ-summary" => \$opt_occ_summary,
	    "taxon-colls" => \$opt_taxon_colls,
	    "resolution|R" => \$opt_occurrence_reso,
	    "global-only|G" => \$opt_global_only,
	    "taxon-steps|T=s" => \$opt_taxon_steps,
	    "interval-data|I" => \$opt_interval_data,
	    "diagnostic|D=s" => \$opt_diagnostic,
	    "steps|T=s" => \$opt_taxon_steps );

# Script actions can be selected by arguments of the same name as well as by options.

while ( my $arg = shift @ARGV )
{
    if ( $arg =~ /^[cmtydv]+$/ )
    {
	$opt_collections = 1 if $arg =~ /c/;
	$opt_occurrences = 1 if $arg =~ /m/;
	$opt_taxonomy = 1 if $arg =~ /t/;
	$opt_taxa_cache = 1 if $arg =~ /y/;
	$opt_diversity = 1 if $arg =~ /d/;
	$opt_prevalence = 1 if $arg =~ /v/;
    }
    
    elsif ( $arg =~ /^(collections|colls)$/ )
    {
	$opt_collections = 1;
    }

    elsif ( $arg =~ /^(occurrences|occs)$/ )
    {
	$opt_occurrences = 1;
    }

    elsif ( $arg =~ /^(taxonomy|taxa)$/ )
    {
	$opt_taxonomy = 1;
    }

    elsif ( $arg =~ /^(taxacache|taxa-cache)$/ )
    {
	$opt_taxa_cache = 1;
    }

    elsif ( $arg =~ /^(diversity|div)$/ )
    {
	$opt_diversity = 1;
    }

    elsif ( $arg =~ /^(prevalence|prev)$/ )
    {
	$opt_prevalence = 1;
    }

    elsif ( $arg =~ /^(occsummary|occ-summary)$/ )
    {
	$opt_occ_summary = 1;
    }

    elsif ( $arg =~ /^(taxoncolls|taxon-colls)$/ )
    {
	$opt_taxon_colls = 1;
    }
    
    elsif ( $arg =~ /^(intervaldata|interval-data)$/ )
    {
	$opt_interval_data = 1;
    }
    
    elsif ( $arg =~ /^nightly$/ )
    {
	$opt_collections = 1;
	$opt_occurrences = 1;
	$opt_taxonomy = 1;
	$opt_taxa_cache = 1;
    }
    
    elsif ( $arg =~ /^weekly$/ )
    {
	$opt_diversity = 1;
	$opt_prevalence = 1;
    }
    
    else
    {
	die "Invalid argument '$arg'";
    }
}


# The option 'nightly' is the same as -cmty, and 'weekly' is the same as -dv.

if ( $opt_nightly )
{
    $opt_collections = 1;
    $opt_occurrences = 1;
    $opt_taxonomy = 1;
    $opt_taxa_cache = 1;
}

if ( $opt_weekly )
{
    $opt_diversity = 1;
    $opt_prevalence = 1;
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


# BuildTables ( )
# 
# Do the build actions specified by the options and arguments passed to this script.

sub BuildTables {

    # The table 'taxon_trees' is currently the only tree table there is.
    
    my $tree_table = 'taxon_trees';
    
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
    
    # Get a database handle and a taxonomy object. The database name can be overridden using the
    # '--database' option on the command line.
    
    my $dbh = connectDB("config.yml", $opt_database);
    
    # Verify the database that we are rebuilding.
    
    if ( $dbh->{Name} =~ /database=([^;]+)/ )
    {
	logMessage(1, "Using database: $1");
    }
    else
    {
	logMessage(1, "Using connect string: $dbh->{Name}");
    }
    
    my $t = Taxonomy->new($dbh, $tree_table);
    
    # my $force = $options{f};
    # my $interval_data = $options{I};
    # my $interval_map = $options{U};
    # my $rank_map = $options{r};
    # my $taxon_pics = $options{p};
    # my $occurrence_int_maps = $options{M};
    # my $occurrence_reso = $options{R};
    
    # my $taxon_tables = 1 if $options{t} || $options{T};
    # my $taxon_steps = $options{T};
    
    my $options = { taxon_steps => $opt_taxon_steps,
		    debug => $opt_debug, };
    
    $options->{diagnostic} = $opt_diagnostic if $opt_diagnostic;
    $options->{global_only} = 1 if $opt_global_only;
    
    # The option -i causes a forced reload of the interval data from the source
    # data files.  Otherwise, do a (non-forced) call to LoadIntervalData if any
    # function has been selected that requires it.
    
    if ( $opt_interval_data )
    {
	loadIntervalData($dbh, 1);
    }
    
    elsif ( $opt_collections || $opt_occurrences || $opt_taxonomy )
    {
	loadIntervalData($dbh);
    }
    
    # # The option -r causes the taxon rank map to be (re)generated.
    
    # if ( $rank_map )
    # {
    # 	TaxonTables::createRankMap($dbh);
    # 	CollectionTables::createCountryMap($dbh, 1);
    # }

    # Main build actions
    # ------------------
    
    if ( $opt_collections )
    {
	my $bins = configData('bins');
	buildCollectionTables($dbh, $bins, $options);
    }
    
    # if ( $lith_tables && ! $collection_tables )
    # {
    # 	buildLithTables($dbh);
    # }
    
    if ( $opt_occurrence_reso )
    {
	if ( $opt_occurrence_reso =~ qr{p}x )
	{
	    $options->{accept_periods} = 1;
	}
	
	if ( $opt_occurrence_reso =~ qr{^[^/\d]*(\d+)}x )
	{
	    $options->{epoch_bound} = $1;
	}
	
	if ( $opt_occurrence_reso =~ qr{/(\d+)}x )
	{
	    $options->{interval_bound} = $1;
	}
    }
    
    if ( $opt_occurrences || $opt_occurrence_reso )
    {
	populateOrig($dbh);
	buildOccurrenceTables($dbh, $tree_table, $options);
    }
    
    elsif ( $opt_occ_summary )
    {
	buildTaxonSummaryTable($dbh, $tree_table, $options);
    }
    
    if ( $opt_taxon_colls )
    {
	buildTaxonCollectionsTable($dbh, $tree_table, $options);
    }
    
    # elsif ( $occurrence_int_maps )
    # {
    # 	buildOccIntervalMaps($dbh);
    # }
    
    if ( $opt_taxonomy )
    {
	populateOrig($dbh);
	buildTaxonTables($dbh, $tree_table, $options);
    }
    
    if ( $opt_taxa_cache )
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
	    buildTaxaCacheTables($dbh, $tree_table);
	};
	
	my $error = $@;
	
	$dbh->do("DELETE FROM tc_mutex");
	
	if ( $error )
	{
	    logMessage(1, "An error occurred during taxa_tree_cache rebuild: $@");
	}
	
	logMessage(1, "Released lock on tc_mutex");
    }
    
    if ( $opt_diversity )
    {
	buildTotalDiversityTables($dbh, $tree_table, $options);
    }
    
    if ( $opt_prevalence )
    {
	buildPrevalenceTables($dbh, $tree_table, $options);
    }
    
    if ( $opt_taxon_pics )
    {
	getPics($dbh, $tree_table, $opt_force);
	selectPics($dbh, $tree_table);
    }
    
    # if ( $strata_tables )
    # {
    # 	buildStrataTables($dbh);
    # }
    
    if ( $opt_occurrences )
    {
	buildSpecimenTables($dbh);
    }
    
    # if ( $timescale_tables )
    # {
    # 	establishTimescaleTables($dbh);
    # }
    
    logTimestamp();
    logMessage(1, "Done rebuilding tables.");
    
    # Done!
}

