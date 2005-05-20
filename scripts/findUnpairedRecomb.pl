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

$types = "'recombined as'";
#$types = "'recombined as','corrected as','rank changed as'";

$count = 0;
%seen = ();
@results = @{$dbt->getData("SELECT opinions.created, opinions.reference_no,opinion_no,person.name, opinions.pubyr as opubyr,refs.pubyr,child_no,status,parent_no FROM opinions,refs,person WHERE opinions.reference_no=refs.reference_no AND opinions.authorizer_no=person.person_no AND status IN ($types) AND parent_no IS NOT NULL")};
foreach $row (@results) {
    # row->{child_no} is orig comb, row->{parent_no} is recombined  name
    # find other children of the recombined name
    $sql = "SELECT opinions.created, opinions.reference_no, opinion_no, person.name, opinions.pubyr as opubyr,refs.pubyr,child_no,status,parent_no FROM opinions,refs,person WHERE opinions.reference_no=refs.reference_no AND opinions.authorizer_no=person.person_no AND child_no = $row->{child_no} AND status IN ('belongs to') AND opinions.reference_no=$row->{reference_no}";

    @results2 = @{$dbt->getData($sql)};
    if (@results2) {
#        print printOp($row);
#        print printOp($results2[0]);
    } else {
        print printOp($row);
        print "No Pair\n";
    }
}

print "$count Found\n";

sub printOp {
    my $row = shift;
    $c_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$row->{child_no}")}[0]->{'taxon_name'};
    $p_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$row->{parent_no}")}[0]->{'taxon_name'};
    return "c $row->{created} op# $row->{opinion_no} child# $row->{child_no} $c_name $row->{status} --> $row->{parent_no} $p_name \n";
}


