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
%tables = ('reid_no'=>'reidentifications','occurrence_no'=>'occurrences');
while(($pkey,$table)=each(%tables)) {
    $sql = "SELECT * FROM $table WHERE taxon_no=0";
    @results = @{$dbt->getData($sql)};
    foreach $row (@results) {
        $taxon_no = 0;
        $genus = $row->{'genus_name'};
        $species = $row->{'species_name'};
        next if ($row->{'genus_reso'} =~ /informal/); 

        if ($species != /^sp(\.){0,1}|indet(\.{0,1})/ && $row->{'species_reso'} !~ /informal/) {
            @taxonNos = TaxonInfo::getTaxonNos($dbt,$genus." ".$species);
            if (scalar(@taxonNos)==1) {
                $taxon_no = $taxonNos[0];
                $taxon_created = ${$dbt->getData("select created from authorities where taxon_no=$taxon_no")}[0]->{'created'};
                print "Found match for $genus $species on $pkey=$row->{$pkey} created $row->{created} to taxon no $taxon_no created $taxon_created\n";
                $fnd{"$genus $species"} = $taxonNos[0];
            } elsif (scalar(@taxonNos) > 1) {
                print "ERROR: ambigious: $genus $species\n";
                $ambig{"$genus $species"} =join(', ',@taxonNos);
            }
        }
        if (!$taxon_no) {
            @taxonNos = TaxonInfo::getTaxonNos($dbt,$genus);
            if (scalar(@taxonNos)==1) {
                $taxon_no = $taxonNos[0];
                $taxon_created = ${$dbt->getData("select created from authorities where taxon_no=$taxon_no")}[0]->{'created'};
                print "Found match for $genus on $pkey=$row->{$pkey} created $row->{created} to taxon no $taxon_no created $taxon_created\n";
                $fnd{"$genus"} = $taxonNos[0];
            } elsif (scalar(@taxonNos) > 1) {
               print "ERROR ambigious: $genus\n";
                $ambig{"$genus"} =join(', ',@taxonNos);
            } else {
                #print "ERROR could not find: $genus $species\n";
                $cnf{"$genus $species"} =1;
            }
        }
        if ($taxon_no) {
            $sql = "UPDATE $table SET modified=modified,taxon_no=$taxon_no WHERE $pkey=$row->{$pkey}";
            print $sql."\n";
            if ($doUpdate) {
                my $r = $dbh->do($sql);
            }
        }
    }
}

#print "$count Found\n";
#print "could not find: ".join(', ',sort keys %cnf);
print "\n\nambiguous: ".join(', ',sort keys %ambig);
print "\n\nfound: ".join(',',sort keys %fnd);

