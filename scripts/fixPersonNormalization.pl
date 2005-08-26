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

$sql = "SELECT name,person_no FROM person";
@results = @{$dbt->getData($sql)};
foreach $row (@results) {
    foreach $table ("collections","occurrences","reidentifications","refs") {
        $sql1 = "UPDATE $table SET modified=modified, authorizer_no=$row->{person_no} WHERE authorizer=".$dbh->quote($row->{'name'});
        $sql2 = "UPDATE $table SET modified=modified, enterer_no=$row->{person_no} WHERE enterer=".$dbh->quote($row->{'name'});
        $sql3 = "UPDATE $table SET modified=modified, modifier_no=$row->{person_no} WHERE modifier=".$dbh->quote($row->{'name'});
        print "SQL1: $sql1\n";
        print "SQL2: $sql2\n";
        print "SQL3: $sql3\n";

        if ($doUpdates) {
            $dbh->do($sql1);
            $dbh->do($sql2);
            $dbh->do($sql3);
        }

    }
}
