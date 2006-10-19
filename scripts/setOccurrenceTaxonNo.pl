#!/usr/bin/perl

use lib '../cgi-bin';
use DBConnection;
use DBTransactionManager;
use Data::Dumper;
use Class::Date qw(date localdate gmdate now);
use Taxon;

my $dbh = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh);

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
    
    $sql = "select t.$pkey, date_format(t.created,'%m/%d/%Y') tc, date_format(t.modified,'%m/%d/%Y') tm,date_format(a.created,'%m/%d/%Y') ac, date_format(a.modified,'%m/%d/%Y') am,t.created t_created, t.modified t_modified,a.created a_created,t.genus_reso,t.genus_name,t.subgenus_reso,t.subgenus_name,t.species_reso,t.species_name,a.taxon_name,t.taxon_no from $table t LEFT JOIN authorities a ON a.taxon_no=t.taxon_no WHERE genus_name NOT LIKE \"%'%\" and species_name NOT LIKE \"%'%\"";
    print $sql,"\n";

    $sth = $dbh->prepare($sql);
    $sth->execute();
    while($row = $sth->fetchrow_hashref()) {
        if ($row->{'genus_name'} =~ /'/) {
            print "$pkey:$row->{$pkey}: skipping, quote in name $row->{genus_name}";
        }
        if ($row->{'genus_reso'} =~ /informal/) {
            next;
        }
        my $name = $row->{'genus_name'};
        if ($row->{'subgenus_name'}) {
            $name .= " (".$row->{'subgenus_name'}.")";
        }
        if ($row->{'species_name'} !~ /^sp\.|^indet\.|^sp$|^indet$/ && $row->{'species_reso'} !~ 'informal') {
            $name .= " $row->{'species_name'}";
        }
        next if ($row->{'taxon_name'} && $name eq $row->{'taxon_name'});
        @matches = Taxon::getBestClassification($dbt,$row);
        $best_no = Taxon::getBestClassification($dbt,$row);
        if (@matches) {
            if (@matches && !$best_no) {
#                print "Skipping, appears to be a homonym issue for $matches[0]->{taxon_name}. ".join(",",map {$_->{'taxon_no'}} @matches)."\n";
            } else {
                $best_name = $matches[0]->{'taxon_name'};
                if ($best_no && $row->{'taxon_no'} != $best_no) {
                    my $t = TaxonInfo::getTaxa($dbt,{ taxon_no=>$best_no},['taxon_no','taxon_name','taxon_rank','comments','created']);
                    my $taxon_no = $row->{'taxon_no'};
                    my $modified = new Class::Date($row->{"t_modified"});
                    my $created = new Class::Date($row->{"t_created"});
                    my $t_created = new Class::Date($t->{'created'});
                    my $cutoff1 = new Class::Date("2004-01-01 00:00:00");
                    my $cutoff2 = new Class::Date("2006-01-01 00:00:00");
                    my $rank_change = (@matches > 1 && $matches[0]->{'taxon_name'} eq $matches[1]->{'taxon_name'}) ? 1 : 0;
                    my $old_match = 0;
                    my ($t_genus,$t_subgenus,$t_species,$t_subspecies) = Taxon::splitTaxon($t->{taxon_name});
                    if ($t_created < $cutoff2) {
                        if ($t->{'taxon_rank'} eq 'genus') {
                            if ($row->{'genus_name'} eq $t_genus) {
                                $old_match = 1;
                            }
                        } elsif ($t->{'taxon_rank'} =~ /species/) {
                            if ($row->{'genus_name'} eq $t_genus &&
                                $row->{'species_name'} eq $t_species) {
                                $old_match = 1;
                            }
                        }
                    }

                    print "Occ: created $row->{tc} modified $row->{tm} Authority: created $row->{ac} modified $row->{am}\n";
                    print "$pkey:$row->{$pkey} has taxon ($row->{taxon_no}:$row->{taxon_name}) but should be ($best_no:$best_name)\n";
                    if (!$taxon_no && !$rank_change && $old_match && $created != $modified && $modified > $cutoff1) {
                        print "Skipping, taxon_no appears to have been manually set to 0 via reclassification\n";
                        next;
                    }
                    if (!$taxon_no && !$rank_change && $old_match && $row->{'tc'} eq $row->{'tm'} && $created < $cutoff1) {
                        print "Skipping, taxon_no appears to have been manually set to 0 by alroy\n";
                        next;
                    }
                    if ($t->{'comments'} =~ /sepkoski/i && !$row->{'taxon_no'} && $old_match) {
                        print "Skipping, sepkoski compendium but appears to have been manually set to 0\n";
                        next;
                    }
                    
                    $sql = "UPDATE $table SET modified=modified,taxon_no=$best_no WHERE $pkey=$row->{$pkey}";
                    print "$sql\n";
                    print "\n";
                    if ($doUpdates) {
                        $dbh->do($sql);
                    }
                }
            }
        }
    }
}

