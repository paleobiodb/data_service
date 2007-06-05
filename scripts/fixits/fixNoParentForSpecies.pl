#!/usr/bin/perl

use lib '../cgi-bin';
use DBI;
use DBTransactionManager;
use Session;
use Data::Dumper;
use TimeLookup;
use TaxonInfo;


$doUpdates = 0;
if ($ARGV[0] eq '--do_sql') {
    $doUpdates = 1;
    print "RUNNING SQL\n";
} else {
    print "DRY RUN\n";
}


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

#
# Generates the spelling number when it can
#

$sql = "SELECT opinions.*, taxon_rank, taxon_name FROM opinions, authorities WHERE child_spelling_no=taxon_no AND (parent_no IS NULL or parent_no=0) and taxon_rank='species' and child_no != 0 and status NOT LIKE '%nomen%'";

@results = @{$dbt->getData($sql)};

foreach $row (@results) {
    if ($row->{status} =~ /corrected/) {
        print "$row->{child_no} $row->{child_spelling_no} ($row->{taxon_name} $row->{taxon_rank}) $row->{status} $row->{parent_no} $row->{parent_spelling_no}\n";
        @bits = split / /,$row->{'taxon_name'};
        $parent_name = shift @bits;
        @r = TaxonInfo::getTaxon($dbt,'taxon_name'=>$parent_name);
        if (scalar(@r) == 1) {
            $parent_spelling_no = $r[0]->{'taxon_no'};
            $parent_no = TaxonInfo::getOriginalCombination($dbt,$parent_spelling_no);
            $sql = "UPDATE opinions SET parent_no=$parent_no, parent_spelling_no=$parent_spelling_no, modified=modified WHERE opinion_no=$row->{opinion_no}";
            print $sql."\n";
            $dbh->do($sql) if ($doUpdates);
        } else {
            print "ERROR: r is ".scalar(@r)."\n";
        }
    }
}

