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
@results = @{$dbt->getData("SELECT DISTINCT child_no,status,parent_no FROM opinions WHERE status IN ('recombined as', 'corrected as', 'rank changed as') AND parent_no IS NOT NULL")};
foreach $row (@results) {
    # row->{child_no} is orig comb, row->{parent_no} is recombined  name
    # find other children of the recombined name
    $sql = "SELECT opinion_no,child_no,status,parent_no FROM opinions WHERE parent_no=$row->{parent_no} AND child_no != $row->{child_no}";
    @results2 = @{$dbt->getData($sql)};
    foreach $row2 (@results2) {
        #$sql = "SELECT status, parent_no, pubyr, reference_no FROM opinions WHERE child_no=$row2->{child_no}";
        #@results3 = @{$dbt->getData($sql)};
        #my $parent_no = TaxonInfo::selectMostRecentParentOpinion($dbt, \@results3);
        #if ($parent_no == $row->{'parent_no'}) {
        $c_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$row->{child_no}")}[0]->{'taxon_name'};
        $p_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$row->{parent_no}")}[0]->{'taxon_name'};
        $vc_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$row2->{child_no}")}[0]->{'taxon_name'};
        print "Child no $row->{child_no} $c_name $row->{status} --> $row->{parent_no} $p_name <-- $row2->{status} $row2->{child_no} $vc_name (op# $row2->{opinion_no})\n";
        $count++;
        if ($fix) {
            if ($row2->{status} =~ /^(recombined|corrected|rank changed)/) { 
                # dont' run this, manully fix
                #$sql = "UPDATE opinions SET modified=modified, child_no=$row->{child_no}, parent_no=$row2->{child_no} WHERE opinion_no=$row2->{opinion_no}";
                #$dbt->getData($sql);
            } else {
                $sql = "UPDATE opinions SET modified=modified, parent_no=$row->{child_no} WHERE opinion_no=$row2->{opinion_no}";
                #$dbt->getData($sql);
            }    
            print $sql."\n";
        }
        #}
    }
}

print "$count Found\n";
