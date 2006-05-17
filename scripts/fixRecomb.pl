#!/usr/bin/perl

use lib '../cgi-bin';
use DBI;
use DBTransactionManager;
use Session;
use Data::Dumper;
use TimeLookup;
use TaxonInfo;

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

my $fix = 0;
#
# This scripts  find (and optionally fixes) problems with chained reocmbinations
#

$count = 0;
@results = @{$dbt->getData("SELECT DISTINCT o1.child_no,o1.child_spelling_no FROM opinions o1, opinions o2 WHERE o1.child_no != o1.child_spelling_no AND o1.child_spelling_no=o2.child_no")};
foreach $row (@results) {
    # row->{child_no} is orig comb, row->{parent_no} is recombined  name
    # find other children of the recombined name
    if ($row->{'parent_no'}) {
        $sql = "SELECT opinion_no,child_no,status,parent_no FROM opinions WHERE child_no=$row->{parent_no}";
        @results2 = @{$dbt->getData($sql)};
        foreach $row2 (@results2) {
            $c_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$row->{child_no}")}[0]->{'taxon_name'};
            $p_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$row->{parent_no}")}[0]->{'taxon_name'};
            if ($row2->{'parent_no'}) {
                $gp_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$row2->{parent_no}")}[0]->{'taxon_name'};
            }
            print "Child no $row->{child_no} $c_name $row->{status} --> $row->{parent_no} $p_name $row2->{status} -->$row2->{parent_no} $gp_name (op# $row2->{opinion_no})\n";
            $count++;
            if ($fix) {
                $sql = "UPDATE opinions SET modified=modified,child_no=$row->{child_no} WHERE opinion_no=$row2->{opinion_no}";
                if ($row->{'child_no'} != $row->{'parent_no'} && $row->{'child_no'} != $row2->{'parent_no'}) {
                    print $sql."\n";
                    #$dbt->getData($sql);
                } else {
                    print "^--- Loop found?\n";
                }
            }
        }
    }
}

print "$count Found\n";
