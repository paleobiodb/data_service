#!/usr/bin/perl

use lib '../cgi-bin';
use DBI;
use DBTransactionManager;
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
my $dbt = DBTransactionManager->new($dbh);

#
# Removes completely redundant opinions
#

$fields = 'child_no,status,parent_no,author1last,author1init,pubyr,reference_no,ref_has_opinion'; 

$sql = "SELECT $fields FROM opinions WHERE child_no != 0 GROUP BY $fields HAVING count(*) > 1";
@results = @{$dbt->getData($sql)};

print "Found ".scalar(@results)." redundant entries";

foreach $row (@results) {
    #try to find pairing 
    $sql = "SELECT * FROM opinions ";
    foreach $field (split/,/,$fields) {
        if (defined $row->{$field}) {
            $field_value = " = ".$dbh->quote($row->{$field});  
        } else {
            $field_value  = "IS NULL";
        }
        $sql .= " AND $field ".$field_value;
    }
    $sql =~ s/opinions  AND/opinions WHERE/;
    #print $sql."\n";
    @results2 = @{$dbt->getData($sql)};
    if (scalar(@results2) > 2) {
        print "WARNING, results2 is > 2\n";
    }
    if (scalar(@results2) < 2) {
        print "WARNING, results2 is < 2\n";
    }
    $delete_idx = miter($results2[0],$results2[1]);
    print "deleting $delete_idx\n";
    $sql = "UPDATE opinions SET comments=CONCAT(comments, '--Was child_no ', child_no, ' parent_no ', parent_no, ' reference_no ',reference_no,' deleted because redundant'),child_no=0,parent_no=0,reference_no=0,modified=modified WHERE opinion_no=$results2[$delete_idx]->{opinion_no}";
    print $sql."\n";
    $dbt->getData($sql);
}


sub miter {
    %a = %{$_[0]};
    %b = %{$_[1]};
    $failed = 0;
    $delete_idx = 1;
    while(($k,$v)=each %a) {
        if ($k !~ /created|modified|opinion_no|modifier_no/) {
            if ($a{$k} ne $b{$k}) {
                $failed =1;
                if ($k =~ /comments/ && $v =~ /m\. kosnik/) {
                    $failed = 0;
                } else {
                    print "A and B not equal for $k: \n  A: $a{$k}\n  B: $b{$k}\n";
                    if ($b{$k}) {
                        $delete_idx = 0;
                    }
                }
            }
        }
    }
    return $delete_idx;
}

