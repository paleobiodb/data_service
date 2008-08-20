#!/usr/bin/perl

# modified from update_cache_multitaxon.pl by JA 20.8.08
# fast and reliable method for recomputing opinion_no values in taxa_tree_cache

use lib "../cgi-bin";
use DBConnection;
use DBTransactionManager;
use TaxaCache;

my $dbh = DBConnection::connect();
my $dbt = new DBTransactionManager($dbh);

$file = $ARGV[0];
open IN,"<./$file";
$t = <IN>;
close IN;
s/\n//;

if ($t =~ /^[\d,]+$/) {
	if ( $t =~ /,/ )	{
		@taxa = split /,/,$t;
	} else	{
		push @taxa , $t;
	}
	print "Running TaxaCache::updateCache\n";
	for $taxon ( @taxa )	{
		TaxonInfo::getMostRecentClassification($dbt,$taxon,{'recompute'=>'yes'});
	}
}
