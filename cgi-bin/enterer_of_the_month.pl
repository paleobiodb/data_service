# 13.9.06
# determine which enterer has created the most records over the preceding month

use DBI;
use DBConnection;
use DBTransactionManager;
use Session;
use Class::Date qw(date localdate gmdate now);

my $dbh = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh);

my @tables = ("refs","authorities","opinions","collections","occurrences","reidentifications");

my ($date,$time) = split / /, now();
my ($yyyy,$mm,$dd) = split /-/,$date,3;

my $lastmonth;
if ( $mm == 1 )	{
	$lastmonth = $yyyy - 1 . "1201000000";
} else	{
	$lastmonth = $yyyy . $mm -1 . "01000000";
}
my $thismonth = $yyyy . $mm . "01000000";

my %total;
print "\n";
for my $t ( @tables )	{
	my $sql = "SELECT first_name,last_name,count(*) AS c FROM person,$t WHERE person_no=enterer_no AND $t.created>$lastmonth AND $t.created<$thismonth GROUP BY enterer_no";

	my @countrefs = @{$dbt->getData($sql)};
	my $grandtotal = 0;
	for $c ( @countrefs )	{
		$total{$c->{first_name}." ".$c->{last_name}} += $c->{c};
		$grandtotal += $c->{c};
	}
	print "$grandtotal $t\n";
}

my @names = keys %total;
@names = sort { $total{$b} <=> $total{$a} } @names;

print "\n";
for $n ( @names )	{
	print "$total{$n}\t$n\n";
}
print "\n";

