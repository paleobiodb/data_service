#!/usr/local/bin/perl

#
# Fixes weird cases where you have an species, and its genus in the DB,
# but no opinions off that species linking it into that genus, even though they're
# almost alwways from the same ref.  weak scripts.  Mostly data keyed under Carrano and Sims it seems
#

use lib '../cgi-bin';
use DBI;
use DBConnection;
use DBTransactionManager;
use Session;
use Data::Dumper;
use TimeLookup;
use TaxonInfo;

use strict;
my $dbh = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh);

my $doUpdates = 0;
if ($ARGV[0] eq '--do_sql') {
    $doUpdates = 1;
    print "RUNNING SQL\n";
} else {
    print "DRY RUN\n";
}



#
# This script fixes species that are missing the opinion: Genus_a species_a 'belongs to' Genus_a
#

my %ambig = ('Conolophus'=>54363,
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


my $sql = "SELECT *,DATE_FORMAT(created,'%Y%m') ym,UNIX_TIMESTAMP(created) ut FROM authorities WHERE taxon_rank IN('subspecies','species','subgenus') AND taxon_name NOT LIKE 'ERROR' AND taxon_name NOT LIKE 'DUPLICATE'";

my %by_auth = ();
my %by_ent = ();
my %by_mon = ();

my %skipped = ();
my %processed = ();

my @rs = @{$dbt->getData($sql)};

foreach my $taxon (@rs) {
    my @bits = split(/\s+/,$taxon->{'taxon_name'});
    pop @bits;
    my $higher_name = join(" ",@bits);
    $sql = "(SELECT o.* FROM opinions o WHERE o.child_spelling_no=$taxon->{taxon_no})";
    $sql .= " UNION ";
    $sql .= "(SELECT o.* FROM opinions o WHERE o.child_no=$taxon->{taxon_no})";
    my @rs2 = @{$dbt->getData($sql)};
    my $found = 0;
    foreach my $row (@rs2) {
        my ($parent_name1,$parent_name2);
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
        print "\n";
        if ($taxon->{comments} =~ /kosnik taxon/) {
            print "Kosnik taxon\n";
        }

        print "Could not find $taxon->{taxon_no} $taxon->{taxon_name} belongs to $higher_name -- ref is authority = '$taxon->{ref_is_authority}' -- ref is $taxon->{reference_no}";
        my $ref1 = Reference->new($dbt,$taxon->{reference_no});
        if ($ref1) {
            print $ref1->authors();
        } else {
            print " ERROR: NO REFERENCE";
        }
        print "\n";

        if (@rs2) {
            foreach my $row (@rs2) {
                print "    Found this opinion: $row->{child_no} ($row->{child_spelling_no}) $row->{status} $row->{parent_no} $row->{parent_name} ($row->{parent_spelling_no})\n";
            }
        } else {
            my $sqlt = "SELECT * FROM opinions WHERE enterer_no=$taxon->{enterer_no} AND (UNIX_TIMESTAMP(created) <= ".($taxon->{ut}+2)." AND UNIX_TIMESTAMP(created) >= ".($taxon->{ut}-2).") LIMIT 3";
            my @rst = @{$dbt->getData($sqlt)};
            if (@rst) {
                # We skip these because they're usually just typos that happened on authority autocreation - i.e.
                # Plesiocetopsis burtinii was autocreated but later corrected to Pleiocetus burtinii, leaving the
                # original stranded
                print "ERROR: SKIPPING $taxon->{taxon_no}: found opinions created by $taxon->{enterer_no} within same 2 seconds:\n";
                $skipped{$taxon->{taxon_no}} = 1;
                foreach my $row (@rst) {
                    print "    Found this opinion: $row->{child_no} ($row->{child_spelling_no}) $row->{status} $row->{parent_no} ($row->{parent_spelling_no})\n";
                }
                next;
            }
            $by_auth{$taxon->{authorizer_no}}++;
            $by_ent{$taxon->{enterer_no}}++;
            $by_mon{$taxon->{ym}}++;
            my $taxon_short = $taxon->{taxon_name};
            chop $taxon_short; chop $taxon_short;
            my $sql1 = "SELECT * FROM authorities WHERE taxon_name LIKE '".$taxon_short."%'";
            my @r1 = @{$dbt->getData($sql1)};
            if (scalar(@r1) > 1) {
                print "ERROR: Skipping $taxon->{taxon_name}, too similar to ".join (', ',map {"$_->{taxon_no} $_->{taxon_name}"} @r1)."\n";
                $skipped{$taxon->{taxon_no}} = 1;
                next;
            }
            my @parents = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$higher_name}); 
            my $tt = 0;
            if ($taxon->{authorizer_no} == 4) {
                #print "Alroy $taxon->{taxon_name} missing bt to $higher_name\n";
            }
            foreach my $p (@parents) {
                if ($p->{'type_taxon_no'} && $p->{'type_taxon_no'} == $taxon->{'taxon_no'}){
                    $tt = $p->{'taxon_no'};
                    print "FOUND type_taxon linking $p->{taxon_no} to $p->{type_taxon_no}\n";
                }
            }
            if (!@parents) {
                my ($g,$sg,$sp,$ssp) = Taxon::splitTaxon($higher_name);
                if ($sg) {
                    my $taxon = TaxonInfo::getTaxa($dbt,{taxon_name=>$sg});
                    if ($taxon) {
                        print "FOUND $taxon->{taxon_no}:$taxon->{taxon_name}\n";
                    }
                }
            }
            
            if (scalar(@parents) == 1 || $ambig{$higher_name} || $tt) {
                my $parent_no;
                if ($ambig{$higher_name}) {
                    $parent_no = $ambig{$higher_name};
                } elsif ($tt) {
                    $parent_no = $tt;
                } else {
                    $parent_no= $parents[0]->{'taxon_no'};
                }
                if ($parents[0]) {
                    my $gp = TaxonInfo::getMostRecentClassification($dbt,$parents[0]->{taxon_no});
                    if ($gp->{status} =~ /synonym|replaced|homonym|subgroup/) {
                        print "ERROR: Parent is $gp->{status} of $gp->{parent_no}, not adding BT\n";
                        $skipped{$taxon->{taxon_no}} = 1;
                        next;
                    }
                }
                if ($taxon->{ym} =~ /2005|2006/) {
                    print "Skipping, from 2005 or 20006\n";
                    next;
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
                                if ($doUpdates) {
                                    $dbh->do($sql);
                                }
                                $taxon->{ref_is_authority} = 'YES';
                                addOpinion($dbt,$taxon,$parent_no);
                                next;
                            } elsif ($parents[0]->{type_taxon_no} == $taxon->{taxon_no}) {
                                print "Found parent from same ref with ref_is_authority NO but linked by type_taxon_no\n"; 
                                addOpinion($dbt,$taxon,$parent_no);
                                next;
                            }
                        }
                    }
                    # At this point, screwed cause we have the opinion data will be incomplete (no pubyr, author1last), do it anyways tho, better than nothing
                    addOpinion($dbt,$taxon,$parent_no);
                    next;
                } else {
                    if (!$taxon->{'reference_no'}) {
                        print "ERROR, no reference_no\n";
                        $skipped{$taxon->{taxon_no}} = 1;
                        next;
                    } 
                    addOpinion($dbt,$taxon,$parent_no);
                    next;
                }
            } elsif (scalar(@parents) == 0) {
                print "    ERROR, could not find $higher_name in DB\n";
                $skipped{$taxon->{taxon_no}} = 1;
                next;
            } else {
                print "    ERROR, ambiguous parent $higher_name\n";
                $skipped{$taxon->{taxon_no}} = 1;
                next;
            }
        }
    }
}
my @srt = sort {$a <=> $b} keys %by_mon;
print "BY AUTH:\n".Dumper(\%by_auth)."\n";
print "BY ENT:\n".Dumper(\%by_ent)."\n";
print "BY MONTH:\n";
print "$_ => $by_mon{$_}\n" for (@srt);

print "";
print "SKIPPED: ".join(", ",sort {$a <=> $b} keys %skipped)."\n";
print "PROCESSED: ".join(", ",sort {$a <=> $b} keys %processed)."\n";



sub addOpinion {
    my ($dbt,$taxon,$parent_no)  = @_;
    my $dbh = $dbt->dbh;

    my ($a1i,$a1,$a2i,$a2,$other,$pubyr,$is_authority);
    if ($taxon->{'ref_is_authority'} =~ /yes/i) {
        $a1i = "''"; $a1 = "''"; $a2i = "''"; $a2 = "''"; $other = "''"; $pubyr="''"; $is_authority="'YES'";
    } else {
        $a1i = $dbh->quote($taxon->{'author1init'});
        $a1 = $dbh->quote($taxon->{'author1last'});
        $a2i = $dbh->quote($taxon->{'author2init'});
        $a2 = $dbh->quote($taxon->{'author2last'});
        $other = $dbh->quote($taxon->{'author1init'});
        $is_authority="''";
    }
    my $pages = $dbh->quote($taxon->{'pages'});
    my $figures= $dbh->quote($taxon->{'figures'});

    print "ADDING OPINION FOR $taxon->{taxon_name} AUTHORIZER $taxon->{authorizer_no}\n";
    $sql = "INSERT INTO opinions (authorizer_no,enterer_no,reference_no,child_no,child_spelling_no,status,spelling_reason,parent_no,parent_spelling_no,created,ref_has_opinion,author1init,author1last,author2init,author2last,otherauthors,pubyr,pages,figures) VALUES ($taxon->{authorizer_no},$taxon->{enterer_no},$taxon->{reference_no},$taxon->{taxon_no},$taxon->{taxon_no},'belongs to','original spelling',$parent_no,$parent_no,NOW(),$is_authority,$a1i,$a1,$a2i,$a2,$other,$pubyr,$pages,$figures)";
    print $sql."\n";
    if ($doUpdates) {
        $dbh->do($sql);
    }
    $processed{$taxon->{taxon_no}} = 1;
}
