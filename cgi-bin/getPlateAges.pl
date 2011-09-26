#!/usr/local/bin/perl

# JA 8.9.09

use DBI;
use DBConnection;
use DBTransactionManager;

my $dbt = DBTransactionManager->new($dbh);
my $dbh = $dbt->dbh;

my $sql = "SELECT plate,max(top_age) max FROM collections,interval_lookup WHERE max_interval_no>0 AND min_interval_no=0 AND max_interval_no=interval_no AND top_age<600 AND plate>0 GROUP BY plate ORDER BY plate";
my @ms = @{$dbt->getData($sql)};

my %max;
$max{$_->{'plate'}} = $_->{'max'} foreach @ms;

my $sql = "SELECT plate,max(top_age) max FROM collections,interval_lookup WHERE min_interval_no>0 AND min_interval_no=interval_no AND top_age<600 AND plate>0 GROUP BY plate ORDER BY plate";
my @ms = @{$dbt->getData($sql)};

for my $m ( @ms )	{
	if ( $m->{'max'} > $max{$m->{'plate'}} )	{
		$max{$m->{'plate'}} = $m->{'max'};
	}
}

my @plates = keys %max;
@plates = sort { $a <=> $b } @plates;
foreach $p ( @plates )	{
	$i++;
	$sql = "UPDATE plates SET age=".$max{$p}." WHERE plate=".$p;
	$dbh->do( $sql );
}
print "$i plates updated\n";

