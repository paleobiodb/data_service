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



$sql = "SELECT * FROM authorities WHERE authorizer_no=48 AND taxon_rank LIKE 'genus'";
@results = @{$dbt->getData($sql)};
foreach $row (@results) {
    if ($row->{comments} =~ /age data/i) {
        if ( $row->{comments} =~ / R / || $row->{comments} =~ / R$/)  {
            print "SETTING extant to YES for $row->{taxon_name}. Comments is $row->{comments}\n";
            $sql = "UPDATE authorities SET modified=modified, extant='YES' WHERE taxon_no=$row->{taxon_no}";
        } else {
            print "SETTING extant to NO for $row->{taxon_name}. Comments is $row->{comments}\n";
            $sql = "UPDATE authorities SET modified=modified, extant='NO' WHERE taxon_no=$row->{taxon_no}";
        }
        print "$sql\n";
        $dbh->do($sql) if ($doUpdates);
    } else {
        print "SKIPPING $row->{taxon_name} NO AGE DATA $row->{comments}\n";
    }
}
