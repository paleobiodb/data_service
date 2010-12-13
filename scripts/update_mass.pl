#!/usr/bin/perl

# JA 8.12.10
# updates mass estimates in taxa_tree_cache
# uses data for a name and all its synonyms, but stashes data only for the valid name
#  to minimize confusion and save (a little) space

use lib "../cgi-bin";
use DBConnection;
use DBTransactionManager;
use Measurement;
use TaxonInfo;

my $dbh = DBConnection::connect();
my $dbt = new DBTransactionManager($dbh);

my $sql = "UPDATE taxa_tree_cache SET mass=NULL";
$dbh->do($sql);

my $sql = "(SELECT a.taxon_no FROM authorities a,specimens s WHERE a.taxon_no=s.taxon_no) UNION (SELECT a.taxon_no FROM authorities a,specimens s, occurrences o LEFT JOIN reidentifications r ON r.occurrence_no=o.occurrence_no WHERE a.taxon_no=o.taxon_no AND s.occurrence_no=o.occurrence_no AND r.reid_no IS NULL) UNION (SELECT a.taxon_no FROM authorities a,specimens s,reidentifications r WHERE a.taxon_no=r.taxon_no AND s.occurrence_no=r.occurrence_no AND s.occurrence_no>0 AND r.most_recent='YES' GROUP BY a.taxon_no) ORDER BY taxon_no ASC";
my @taxa = @{$dbt->getData($sql)};

my $updates;
for my $t ( @taxa )	{
	my $orig = TaxonInfo::getOriginalCombination($dbt,$t->{'taxon_no'});
	my $ss = TaxonInfo::getSeniorSynonym($dbt,$orig);
	my @in_list = TaxonInfo::getAllSynonyms($dbt,$ss);
	my @specimens = Measurement::getMeasurements($dbt,'taxon_list'=>\@in_list,'get_global_specimens'=>1);
	my $p_table = Measurement::getMeasurementTable(\@specimens);
	my @m = Measurement::getMassEstimates($dbt,$ss,$p_table);
	if ( $m[5] && $m[6] )       {
		$updates++;
		if ( $updates / 100 == int($updates / 100) )	{
			print "$updates updates so far, up to taxon $t->{'taxon_no'}\r";
		}
		my $mean = $m[5] / $m[6];
		@in_list = TaxonInfo::getAllSpellings($dbt,$ss);
		my $sql = "UPDATE taxa_tree_cache SET mass=$mean WHERE taxon_no IN (".join(',',@in_list).")";
		$dbh->do($sql);
	}
}
print "\n";

