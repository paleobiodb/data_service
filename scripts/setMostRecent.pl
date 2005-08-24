#!/usr/bin/perl

use lib '../cgi-bin';
use DBI;
use DBConnection;
use DBTransactionManager;
use Session;
use Data::Dumper;

my $s = Session->new();
my $dbh = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh, $s);


$doUpdates = 0;
if ($ARGV[0] eq '--do_sql') {
    $doUpdates = 1;
    print "RUNNING SQL\n";
} else {
    print "DRY RUN\n";
}  

#
# This scripts  find (and optionally fixes) problems with opinions that doing POINT to the
# original combination
#

$sql = "SELECT DISTINCT occurrence_no FROM reidentifications";
@results = @{$dbt->getData($sql)};
foreach $row (@results) {
    my $sql = "SELECT re.* FROM reidentifications re, refs r WHERE r.reference_no=re.reference_no AND re.occurrence_no=".$row->{occurrence_no}." ORDER BY r.pubyr DESC, re.reid_no DESC";
    my @o_results = @{$dbt->getData($sql)};
    if (@o_results) {
        $sql = "UPDATE reidentifications SET modified=modified, most_recent='YES' WHERE reid_no=".$o_results[0]->{'reid_no'};
        my $result = $dbh->do($sql) if ($doUpdates);
        print "set most recent: $sql\n";
        if ($doUpdates && !$result) {
            print "Error setting most recent reid to YES for reid_no=$o_results[0]->{reid_no}";
        }

        my @older_reids;
        for($i=1;$i<scalar(@o_results);$i++) {
            push @older_reids, $o_results[$i]->{'reid_no'};
        }
        if (@older_reids) {
            $sql = "UPDATE reidentifications SET modified=modified, most_recent='NO' WHERE reid_no IN (".join(",",@older_reids).")";
            $result = $dbh->do($sql) if ($doUpdates);
            print "set not most recent: $sql\n";
            if ($doUpdates && !$result) {
                print "Error setting most recent reid to NO for reid_no IN (".join(",",@older_reids).")";
            }
        }
    } 


}

