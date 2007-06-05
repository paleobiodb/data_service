# 21.4.04
# fixes bad opinion records in which enterer no has been replaced with
#  authorizer no due to a Poling bug

use Class::Date qw(date localdate gmdate now);
use DBI;
use DBTransactionManager;
use Session;

# Flags and constants
my $DEBUG = 0;                  # The debug level of the calling program
my $sql;                                # Any SQL string

my $driver = "mysql";
my $db = "pbdb_paul";
my $host = "localhost";
my $user = 'pbdbuser';

open PASSWD,"</Users/paleodbpasswd/passwd";
$password = <PASSWD>;
$password =~ s/\n//;
close PASSWD;

my $dbh = DBI->connect("DBI:$driver:database=$db;host=$host",
                      $user, $password, {RaiseError => 1});

my $s = Session->new();
my $dbt = DBTransactionManager->new($dbh, $s);

$sql = "SELECT taxon_no,to_days(created) AS day,authorizer_no,enterer_no FROM authorities WHERE taxon_rank='species'";
@trefs = @{$dbt->getData($sql)};

print "\n";
for $tr (@trefs)	{
	$sql = "SELECT opinion_no,child_no,parent_no,enterer_no,to_days(created) AS day FROM opinions WHERE child_no=".$tr->{taxon_no}." AND to_days(created)=".$tr->{day}." AND enterer_no!=".$tr->{enterer_no};
	$oref = ${$dbt->getData($sql)}[0];
	if ( $oref )	{
		print $oref->{opinion_no},"\t",$oref->{child_no},"\t",$oref->{parent_no},"\t",$oref->{enterer_no},"\t",$tr->{enterer_no},"\t",$oref->{chid_no},"\t",$oref->{day},"\n";
		$sql = "UPDATE opinions SET modified=modified,enterer_no=".$tr->{enterer_no}." WHERE opinion_no=".$oref->{opinion_no};
		$dbt->getData($sql);
		$count++;
	}
}
print "\n$count\n\n";

