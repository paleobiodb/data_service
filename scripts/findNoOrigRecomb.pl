#!/usr/bin/perl

use lib '../cgi-bin';
use DBI;
use DBTransactionManager;
use Session;
use Data::Dumper;
use TimeLookup;
use TaxonInfo;
use Benchmark qw(:all) ;

$driver =       "mysql";
$host =         "localhost";
$user =         "pbdbuser";
$db =           "pbdb";

open PASSWD,"</home/paleodbpasswd/passwd";
$password = <PASSWD>;
$password =~ s/\n//;
close PASSWD;

my $dbh = DBI->connect("DBI:$driver:database=$db;host=$host", $user, $password, {RaiseError => 1});

# Make a Global Transaction Manager object
my $s = Session->new();
my $dbt = DBTransactionManager->new($dbh, $s);

my $fix = 1;
#
# This scripts  find (and optionally fixes) problems with opinions that doing POINT to the
# original combination
#

$count = 0;
%seen = ();
@results = @{$dbt->getData("SELECT opinions.reference_no,opinion_no,person.name, opinions.pubyr as opubyr,refs.pubyr,child_no,status,parent_no FROM opinions,refs,person WHERE opinions.reference_no=refs.reference_no AND opinions.authorizer_no=person.person_no AND status IN ('recombined as', 'corrected as', 'rank changed as') AND parent_no IS NOT NULL")};
foreach $row (@results) {
    # row->{child_no} is orig comb, row->{parent_no} is recombined  name
    # find other children of the recombined name
    $sql = "SELECT opinions.reference_no, opinion_no, person.name, opinions.pubyr as opubyr,refs.pubyr,child_no,status,parent_no FROM opinions,refs,person WHERE opinions.reference_no=refs.reference_no AND opinions.authorizer_no=person.person_no AND parent_no=$row->{parent_no} AND child_no != $row->{child_no} AND status IN ('recombined as', 'corrected as', 'rank changed as')";
    @results2 = @{$dbt->getData($sql)};
    foreach $row2 (@results2) {
        if (!$seen{$row->{'opinion_no'}} && !$seen{$row2->{'opinion_no'}}) {
            $seen{$row->{'opinion_no'}} = 1;
            $seen{$row2->{'opinion_no'}} = 1;
            $c_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$row->{child_no}")}[0]->{'taxon_name'};
            $p_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$row->{parent_no}")}[0]->{'taxon_name'};
            $c2_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$row2->{child_no}")}[0]->{'taxon_name'};
            $p2_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$row2->{parent_no}")}[0]->{'taxon_name'};
            print "1 op# $row->{opinion_no} child# $row->{child_no} $c_name $row->{status} --> $row->{parent_no} $p_name \n";
            print "1 auth $row->{name} o.pubyr $row->{opubyr} r.pubyr $row->{pubyr} ref# $row->{reference_no}\n";
            print "2 op# $row2->{opinion_no} child# $row2->{child_no} $c2_name $row2->{status} --> $row2->{parent_no} $p2_name \n"; 
            print "2 auth $row2->{name} o.pubyr $row2->{opubyr} r.pubyr $row2->{pubyr} ref# $row2->{reference_no}\n";
            $count++;
        }
    }
}

print "$count Found\n";
