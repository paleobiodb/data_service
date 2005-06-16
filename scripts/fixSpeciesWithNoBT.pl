#!/usr/bin/perl

#
# Fixes weird cases where you have an species, and its genus in the DB,
# but no opinions off that species linking it into that genus, even though they're
# almost alwways from the same ref.  weak scripts.  Mostly data keyed under Carrano and Sims it seems
#

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

$doUpdates = 0;
if ($ARGV[0] eq '--do_sql') {
    $doUpdates = 1;
    print "RUNNING SQL\n";
} else {
    print "DRY RUN\n";
}


my $dbh = DBI->connect("DBI:$driver:database=$db;host=$host", $user, $password, {RaiseError => 1});

# Make a Global Transaction Manager object
my $s = Session->new();
my $dbt = DBTransactionManager->new($dbh, $s);

#
# This script fixes species that are missing the opinion: Genus_a species_a 'belongs to' Genus_a
#

%ambig = ('Conolophus'=>54363,
          'Eutemnodus'=>53770,
          'Rhectomyax'=>55265,
          'Paliurus'=>55524,
          'Andromeda'=>55551,
          'Myristica'=>55895,
          'Ficus'=>53873,
          'Kenella'=>56338,
          'Pseudoscalites'=>9166,
          'Acropora'=>6114,
          'Cyclina'=>17545,
          'Ovacuna'=>61534,
          'Protobalaena'=>64783,
          'Tenisia'=>'53405',
          'Mesophyllum'=>'54183',
          'Convexastrea'=>54146);


@ok = ();
@zero = ();
@many = ();

$sql = "SELECT *,DATE_FORMAT(created,'%Y%m') ym FROM authorities WHERE taxon_rank IN('subspecies','species') AND taxon_name NOT LIKE 'ERROR' AND taxon_name NOT LIKE 'DUPLICATE'";

%by_auth = ();
%by_mon = ();

@rs = @{$dbt->getData($sql)};
foreach my $taxon (@rs) {
    @bits = split(/\s+/,$taxon->{'taxon_name'});
    pop @bits;
    $higher_name = join(" ",@bits);
    $sql = "(SELECT o.* FROM opinions o WHERE o.child_spelling_no=$taxon->{taxon_no})";
    $sql .= " UNION ";
    $sql .= "(SELECT o.* FROM opinions o WHERE o.child_no=$taxon->{taxon_no})";
    @rs2 = @{$dbt->getData($sql)};
    $found = 0;
    foreach $row (@rs2) {
        if ($row->{'parent_no'}) {
            $parent_name1 = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$row->{parent_no}")}[0]->{'taxon_name'};
            $parent_name2 = ${$dbt->getData("SELECT taxon_name FROM authorities WHERE taxon_no=$row->{parent_spelling_no}")}[0]->{'taxon_name'};
        } else {
            $parent_name1 = '';
            $parent_name2 = '';
        }
        if ($higher_name eq $parent_name1 || $higher_name eq $parent_name2) {
            $found = 1;
        }
    }

    if (!$found) {
        if ($taxon->{comments} =~ /kosnik taxon/) {
            print "Kosnik taxon\n";
        }

        if ($taxon->{ym} =~ /2005/) {
            print Dumper($taxon);
        }
        print "Could not find $taxon->{taxon_no} $taxon->{taxon_name} belongs to $higher_name\n";
        if (@rs2) {
            foreach $row (@rs2) {
                print "    Found this opinion: $row->{child_no} ($row->{child_spelling_no}) $row->{status} $row->{parent_no} $row->{parent_name} ($row->{parent_spelling_no})\n";
            }
        } else {
            $by_auth{$taxon->{authorizer_no}}++;
            $by_ent{$taxon->{enterer_no}}++;
            $by_mon{$taxon->{ym}}++;
            $taxon_short = $taxon->{taxon_name};
            chop $taxon_short; chop $taxon_short;
            $sql1 = "SELECT * FROM authorities WHERE taxon_name LIKE '".$taxon_short."%'";
            @r1 = @{$dbt->getData($sql1)};
            if (scalar(@r1) > 1) {
                print "Skipping $taxon->{taxon_name}, too similar to ".join (', ',map {"$_->{taxon_no} $_->{taxon_name}"} @r1)."\n";
            }
            @parents = TaxonInfo::getTaxon($dbt,'taxon_name'=>$higher_name); 
            $tt = 0;
            for $p (@parents) {
                if ($p->{'type_taxon_no'} && $p->{'type_taxon_no'} == $taxon->{'taxon_no'}){
                    $tt = $p->{'taxon_no'};
                    print "FOUND type_taxon linking $p->{taxon_no} to $p->{type_taxon_no}\n";
                }
            }
            if (scalar(@parents) == 1 || $ambig{$higher_name} || $tt) { 
                if ($ambig{$higher_name}) {
                    $parent_no = $ambig{$higher_name};
                } elsif ($tt) {
                    $parent_no = $tt;
                } else {
                    $parent_no=$parents[0]->{'taxon_no'};
                }
                if ($parents[0]) {
                    $gp = TaxonInfo::getMostRecentParentOpinion($dbt,$parents[0]->{taxon_no});
                    if ($gp->{status} =~ /synonym|replaced|homonym/) {
                        print "Parent is $gp->{status} of $gp->{parent_no}, not adding BT\n";
                    }
                }
                if ($taxon->{'ref_is_authority'} !~ /YES/i) {
                    if (!$taxon->{'pubyr'}) {
                        print "WARNING, pubyr not set\n";
                    }
                    if (!$taxon->{'author1last'}) {
                        print "WARNING,  author1last not set\n";
                    }
                    if ($parents[0]->{reference_no}) {
                        if ($parents[0]->{reference_no} == $taxon->{reference_no}) {
                            if ($parents[0]->{ref_is_authority} =~ /YES/) {
                                print "Found parent from same ref with ref_is_authority YES\n"; 
                                $sql = "UPDATE authorities SET ref_is_authority='YES' WHERE taxon_no=$taxon->{taxon_no}";
                                print $sql."\n";
                                #$dbh->do($sql) if ($doUpdates); # lay off for now
                                $sql = "INSERT INTO opinions (authorizer_no,enterer_no,reference_no,child_no,child_spelling_no,status,parent_no,parent_spelling_no,created,ref_has_opinion,author1init,author1last,author2init,author2last,otherauthors,pubyr,pages,figures) VALUES ($taxon->{authorizer_no},$taxon->{enterer_no},$taxon->{reference_no},$taxon->{taxon_no},$taxon->{taxon_no},'belongs to',$parent_no,$parent_no,NOW(),'YES','','','','','','','$taxon->{pages}','$taxon->{figures}')";
                                print $sql."\n";
                                $dbh->do($sql) if ($doUpdates);
                                next;
                            } elsif ($parents[0]->{type_taxon_no} == $taxon->{taxon_no}) {
                                print "Found parent from same ref with ref_is_authority NO but linked by type_taxon_no\n"; 
                                # Don't use parnet for authority data - not reliable i.e. Polacantus: Owen 1865, Polacanthus foxii, type taxon: Hulke 1881
                                #if ($parents[0]->{author1last} && !$taxon->{author1last}) {
                                #    print "Using parent for authority data\n";
                                #    $sql = "INSERT INTO opinions (authorizer_no,enterer_no,reference_no,child_no,child_spelling_no,status,parent_no,parent_spelling_no,created,ref_has_opinion,author1init,author1last,author2init,author2last,otherauthors,pubyr,pages,figures) VALUES ($taxon->{authorizer_no},$taxon->{enterer_no},$taxon->{reference_no},$taxon->{taxon_no},$taxon->{taxon_no},'belongs to',$parent_no,$parent_no,NOW(),'$parents[0]->{ref_is_authority}',".$dbh->quote($parents[0]->{author1init}).",".$dbh->quote($parents[0]->{author1last}).",".$dbh->quote($parents[0]->{author2init}).",".$dbh->quote($parents[0]->{author2last}).",".$dbh->quote($parents[0]->{otherauthors}).",'$parents[0]->{pubyr}','$taxon->{pages}','$taxon->{figures}')";
                                #} else {
                                    $sql = "INSERT INTO opinions (authorizer_no,enterer_no,reference_no,child_no,child_spelling_no,status,parent_no,parent_spelling_no,created,ref_has_opinion,author1init,author1last,author2init,author2last,otherauthors,pubyr,pages,figures) VALUES ($taxon->{authorizer_no},$taxon->{enterer_no},$taxon->{reference_no},$taxon->{taxon_no},$taxon->{taxon_no},'belongs to',$parent_no,$parent_no,NOW(),'$taxon->{ref_is_authority}',".$dbh->quote($taxon->{author1init}).",".$dbh->quote($taxon->{author1last}).",".$dbh->quote($taxon->{author2init}).",".$dbh->quote($taxon->{author2last}).",".$dbh->quote($taxon->{otherauthors}).",'$taxon->{pubyr}','$taxon->{pages}','$taxon->{figures}')";
                                #}
                                print $sql."\n";
                                $dbh->do($sql) if ($doUpdates);
                                next;
                            }
                        }
                    }
                    # At this point, screwed cause we have the opinion data will be incomplete (no pubyr, author1last), do it anyways tho, better than nothing
                    $sql = "INSERT INTO opinions (authorizer_no,enterer_no,reference_no,child_no,child_spelling_no,status,parent_no,parent_spelling_no,created,ref_has_opinion,author1init,author1last,author2init,author2last,otherauthors,pubyr,pages,figures) VALUES ($taxon->{authorizer_no},$taxon->{enterer_no},$taxon->{reference_no},$taxon->{taxon_no},$taxon->{taxon_no},'belongs to',$parent_no,$parent_no,NOW(),'$taxon->{ref_is_authority}',".$dbh->quote($taxon->{author1init}).",".$dbh->quote($taxon->{author1last}).",".$dbh->quote($taxon->{author2init}).",".$dbh->quote($taxon->{author2last}).",".$dbh->quote($taxon->{otherauthors}).",'$taxon->{pubyr}','$taxon->{pages}','$taxon->{figures}')";
                    print $sql."\n";
                    $dbh->do($sql) if ($doUpdates);
                } else {
                    if (!$taxon->{'reference_no'}) {
                        print "ERROR, no reference_no\n";
                        next;
                    } 
                    $sql = "INSERT INTO opinions (authorizer_no,enterer_no,reference_no,child_no,child_spelling_no,status,parent_no,parent_spelling_no,created,ref_has_opinion,author1init,author1last,author2init,author2last,otherauthors,pubyr,pages,figures) VALUES ($taxon->{authorizer_no},$taxon->{enterer_no},$taxon->{reference_no},$taxon->{taxon_no},$taxon->{taxon_no},'belongs to',$parent_no,$parent_no,NOW(),'YES','','','','','','','$taxon->{pages}','$taxon->{figures}')";
                    print $sql."\n";
                    $dbh->do($sql) if ($doUpdates);
                }
            } elsif (scalar(@parents) == 0) {
                print "    ERROR, could not find $higher_name in DB\n";
            } else {
                print "    ERROR, ambiguous parent $higher_name\n";
            }
        }
    }
}
@srt = sort {$a <=> $b} keys %by_mon;
print "BY AUTH:\n".Dumper(\%by_auth)."\n";
print "BY ENT:\n".Dumper(\%by_ent)."\n";
print "BY MONTH:\n";
print "$_ => $by_mon{$_}\n" for (@srt);
   
