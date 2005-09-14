#!/usr/bin/perl

use lib '../cgi-bin';
use DBI;
use DBConnection;
use DBTransactionManager;
use Session;
use Data::Dumper;
use TaxonInfo;

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
# This scripts finds mismatches between the taxon_no set in the occurrences/reids table and what exists in
# the authorities table (i.e. when its tied to a genus when it should be tied to a species), or it wasn't set for some reason
# Usually its not set cause of some deficiences in teh scripts that should be repaired soon.  When something is recombined or corrected,
# the new name is created on teh fly but not tied to any occs. Also when up update a record to issue a correction, its doesn't retie occs
#

foreach my $table ('reidentifications','occurrences') {
    $pkey = ($table eq 'occurrences') ? 'occurrence_no' : 'reid_no';
    
    $sql = "select $pkey, a.taxon_name,a.taxon_no,t.taxon_no t_taxon_no from $table t, authorities a where concat(t.genus_name,' ',t.species_name) = a.taxon_name and a.taxon_rank like 'species' and t.taxon_no != a.taxon_no group by t.$pkey";

    $sth = $dbh->prepare($sql);
    $sth->execute();
    while($row = $sth->fetchrow_hashref()) {
        @nos = TaxonInfo::getTaxonNos($dbt,$row->{'taxon_name'});
        if (scalar(@nos) > 1) {
            print "WARNING: skipping, multiple possible authorities for $pkey=$row->{$pkey} : $row->{taxon_name}\n";
        } else {
            $sql = "UPDATE $table SET modified=modified,taxon_no=".$row->{taxon_no}." WHERE $pkey=$row->{$pkey}";
            print "$sql\n";
            $dbh->do($sql)  if ($doUpdates);
        }
    }
}

