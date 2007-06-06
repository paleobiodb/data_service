#!/usr/bin/perl

use lib '../cgi-bin';
use DBI;
use DBTransactionManager;
use Session;
use Data::Dumper;
use TimeLookup;
use TaxonInfo;

my $dbh = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh);

$doUpdate = 0;
if ($ARGV[0] eq '--do_sql') {
    $doUpdate = 1;
    print "RUNNING SQL\n";
} else {
    print "DRY RUN\n";
}
    
#
# This scripts  find (and optionally fixes) problems with opinions that doing POINT to the
# original combination
#

$count = 0;
#%tables = ('reid_no'=>'reidentifications','occurrence_no'=>'occurrences');
my $sql = "SELECT * FROM authorities GROUP BY taxon_name HAVING count(*) = 1";
my @results = @{$dbt->getData($sql)};

foreach my $row (@results) {
    if ($row->{'taxon_rank'} =~ /species/) {
        my ($g,$s) = split(/\s*/,$row->{'taxon_name'});
        if ($g && $s) {
            $g = $dbh->quote($g);
            $s = $dbh->quote($s);
            $sql1 = "SELECT count(*) c FROM occurrences WHERE species_reso NOT LIKE '%informal%' AND genus_name LIKE $g AND species_name LIKE $s AND taxon_no != $row->{taxon_no}";
            $sql2 = "SELECT count(*) c FROM reidentifications WHERE species_reso NOT LIKE '%informal%' AND genus_name LIKE $g and species_name LIKE $s AND taxon_no != $row->{taxon_no}";
            my $c1 = ${$dbt->getData($sql1)}[0]->{c};
            my $c2 = ${$dbt->getData($sql2)}[0]->{c};
            if ($c1) {
                print "Found $c1 occs for $row->{taxon_no} $row->{taxon_name}\n";
                $usql = "UPDATE occurrences SET taxon_no=$row->{taxon_no},modified=modified WHERE genus_name LIKE $g and species_name LIKE $s";
                print $usql,"\n";
                if ($doUpdate) {
#                    $dbh->do($usql);
                }
            }
            if ($c2) {
                print "Found $c2 reids for $row->{taxon_no} $row->{taxon_name}\n";
                $usql = "UPDATE reidentifications SET taxon_no=$row->{taxon_no},modified=modified WHERE genus_name LIKE $g and species_name LIKE $s";
                print $usql,"\n";
                if ($doUpdate) {
#                    $dbh->do($usql);
                }
            }
        }
    } else {
        $g = $dbh->quote($row->{'taxon_name'});
        $sql1 = "SELECT count(*) c FROM occurrences WHERE species_reso NOT LIKE '%informal%' AND genus_name LIKE $g AND taxon_no=0"; 
        $sql2 = "SELECT count(*) c FROM reidentifications WHERE species_reso NOT LIKE '%informal%' AND genus_name LIKE $g AND taxon_no=0";
        my $c1 = ${$dbt->getData($sql1)}[0]->{c};
        my $c2 = ${$dbt->getData($sql2)}[0]->{c};
        if ($c1) {
            print "Found $c1 occs for $row->{taxon_no} $row->{taxon_name}\n";
            $usql = "UPDATE occurrences SET taxon_no=$row->{taxon_no},modified=modified WHERE genus_name LIKE $g AND taxon_no=0";
            print $usql,"\n";
            if ($doUpdate) {
#                $dbh->do($usql);
            }
        }
        if ($c2) {
            print "Found $c2 reids for $row->{taxon_no} $row->{taxon_name}\n";
            $usql = "UPDATE reidentifications SET taxon_no=$row->{taxon_no},modified=modified WHERE genus_name LIKE $g AND taxon_no=0";
            print $usql,"\n";
            if ($doUpdate) {
#                $dbh->do($usql);
            }
        }
    }
}

