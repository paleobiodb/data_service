#!/usr/bin/perl

use lib '../cgi-bin';
use DBI;
use DBTransactionManager;
use Session;
use Data::Dumper;
use TimeLookup;
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

@results = @{$dbt->getData("SELECT DISTINCT child_no,parent_no FROM opinions WHERE status IN ('recombined as', 'corrected as', 'rank changed as')")};
foreach $row (@results) {
    if ($row->{'parent_no'}) {
        $c_no = $row->{'child_no'};
        $p_no = $row->{'parent_no'};
        $str2 = rec_gp($p_no);
        if ($str2) {
            $c_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$c_no")}[0]->{'taxon_name'};
            $p_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$p_no")}[0]->{'taxon_name'};
            print "$c_no $c_name --> $p_no $p_name ".$str2."\n" ;
        }
    }
}

sub rec_gp {
    my $child_no = shift;
    my $times = shift || 0;
    if ($times > 10) { return ""; }
    my $str;
    $times++;
    my @results = @{$dbt->getData("SELECT DISTINCT parent_no FROM opinions WHERE status IN ('recombined as', 'corrected as', 'rank changed as') AND child_no=$child_no")};
    if (scalar(@results)) {
        my $p_no = $results[0]->{parent_no};
        if ($p_no) {
            my $p_name = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$p_no")}[0]->{'taxon_name'};
            my $str = "--> $p_no $p_name ";
            $str .= rec_gp($p_no,$times);
            return $str;
        }
    }
    return "";
}



#@scales = TimeLookup::getScaleOrder($dbt,4);
#print Dumper(@scales);
