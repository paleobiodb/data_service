#!/usr/bin/perl

use lib '../cgi-bin';
use DBI;
use DBTransactionManager;
use Session;
use Data::Dumper;
use TimeLookup;
use TaxonInfo;
use DBConnection;

my $s = Session->new();
my $dbh = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh, $s);

my $fix = 0;
#
# This scripts  find (and optionally fixes) problems with opinions that doing POINT to the
# original combination
#

$count = 0;
%seen = ();
@results = @{$dbt->getData("SELECT o.child_spelling_no,a1.taxon_name spelling_name FROM opinions o LEFT JOIN authorities a1 ON a1.taxon_no=o.child_spelling_no GROUP by o.child_spelling_no HAVING COUNT(DISTINCT o.child_no) > 1")};
foreach $row (@results) {
    # row->{child_no} is orig comb, row->{parent_no} is recombined  name
    # find other children of the recombined name
    $sql = "SELECT o.reference_no, o.opinion_no, p.name, o.pubyr as opubyr,r.pubyr,o.child_no,a1.taxon_name child_name,o.child_spelling_no,o.status,o.parent_no,a3.taxon_name parent_name FROM opinions o,refs r LEFT JOIN person p ON o.enterer_no=p.person_no LEFT JOIN authorities a1 ON a1.taxon_no=o.child_no LEFT JOIN authorities a3 ON a3.taxon_no=o.parent_no WHERE o.reference_no=r.reference_no AND o.child_spelling_no=$row->{child_spelling_no}"; 
    @results2 = @{$dbt->getData($sql)};
    print "$row->{spelling_name}: \n";
    foreach $row2 (@results2) {
        print "op# $row2->{opinion_no},$row2->{name} child# $row2->{child_no} $row2->{child_name} $row2->{status} --> $row2->{parent_no} $row2->{parent_name}: ref $row2->{reference_no}, $row2->{opubyr}$row2->{pubyr}\n";
    }
    print "\n";
    $count++;
}

print "$count Found\n";
