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
my $t;
if ($ARGV[0] eq "all")	{
	my $sql = "SELECT taxon_no FROM authorities";
	my @rows = @{$dbt->getData($sql)};
	$t .= ",".$_->{'taxon_no'} foreach @rows;
	$t =~ s/^,//;
} elsif ( $ARGV[0] !~ /[^0-9,]/ )	{
	$t = $ARGV[0];
} else	{
	open IN,"<./$file";
	$t = <IN>;
	close IN;
	s/\n//;
}

$| = 1;
if ($t =~ /^[\d,]+$/) {
	if ( $t =~ /,/ )	{
		@taxa = split /,/,$t;
	} else	{
		push @taxa , $t;
	}
	my %seen;
	for $taxon ( @taxa )	{
		if ( $taxon/1000 == int($taxon/1000) )	{
			print "$taxon = ";
		}
		$orig = TaxonInfo::getOriginalCombination($dbt,$taxon);
		if ( $taxon/1000 == int($taxon/1000) )	{
			print "$orig\n";
		}
		if ( $seen{$orig} )	{
			next;
		}
		$seen{$orig}++;
		TaxonInfo::getMostRecentClassification($dbt,$orig,{'recompute'=>'yes'});
	}
}
