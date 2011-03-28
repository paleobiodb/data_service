#!/opt/local/bin/perl5
# JA 15.11.10

use lib "../cgi-bin";
use DBConnection;
use DBTransactionManager;
use Collection;

my $dbh = DBConnection::connect();
my $dbt = new DBTransactionManager($dbh);

my $sql = "SELECT collection_no,direct_ma,direct_ma_unit FROM collections WHERE direct_ma>0";
my @colls = @{$dbt->getData($sql)};
for my $c ( @colls )	{
	Collection::setMaIntervalNo($dbt,$dbh,$c->{collection_no},$c->{direct_ma},$c->{direct_ma_unit},$c->{direct_ma},$c->{direct_ma_unit});
}

my $sql = "SELECT collection_no,max_ma,max_ma_unit,min_ma,min_ma_unit FROM collections WHERE direct_ma IS NULL AND (max_ma>0 OR min_ma>0)";
my @colls = @{$dbt->getData($sql)};
for my $c ( @colls )	{
	Collection::setMaIntervalNo($dbt,$dbh,$c->{collection_no},$c->{max_ma},$c->{max_ma_unit},$c->{min_ma},$c->{min_ma_unit});
}

