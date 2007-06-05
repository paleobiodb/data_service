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

# key is child_spelling no, value is opinion no which holds type_taxon opinion
%manualfix = (
    53155=>88384,
    54161=>90356,
    56505=>92580,
    63544=>89281,
    64694=>92528
);

@ok = ();
@zero = ();
@many = ();
@manyt = ();

%screwups = ();

$sql = "SELECT a.*,tt.taxon_name type_taxon_name, tt.authorizer_no tt_authorizer_no, tt.enterer_no tt_enterer_no, tt.author1last tt_author1last, tt.author2last tt_author2last, tt.reference_no tt_reference_no, tt.pubyr tt_pubyr, tt.taxon_rank tt_taxon_rank FROM authorities a LEFT JOIN authorities tt ON a.type_taxon_no=tt.taxon_no WHERE a.type_taxon_no != 0 AND a.type_taxon_no IS NOT NULL";
@rs = @{$dbt->getData($sql)};
foreach my $taxon (@rs) {
    if ($taxon->{tt_taxon_rank} eq 'species') {
        if ($taxon->{taxon_rank} !~ /genus/) {
            #print "ERROR: type taxon $taxon->{type_taxon_no} $taxon->{type_taxon_name} $taxon->{tt_taxon_rank} mismatch for $taxon->{taxon_no} $taxon->{taxon_name} $taxon->{taxon_rank}\n";
            push @{$screwups{$taxon->{tt_authorizer_no}}},$taxon;
            next;
        }
    } elsif ($taxon->{tt_taxon_rank} eq 'subspecies') {
        if ($taxon->{taxon_rank} !~ /species/) {
            #print "ERROR: type taxon $taxon->{type_taxon_no} $taxon->{type_taxon_name} $taxon->{tt_taxon_rank} mismatch for $taxon->{taxon_no} $taxon->{taxon_name} $taxon->{taxon_rank}\n";
            push @{$screwups{$taxon->{tt_authorizer_no}}},$taxon;
            next;
        }
    }
    my $sql = "SELECT opinions.*,refs.pubyr ref_pubyr FROM opinions left join refs ON opinions.reference_no=refs.reference_no WHERE child_no=$taxon->{type_taxon_no} AND (parent_no=$taxon->{taxon_no} OR parent_spelling_no=$taxon->{taxon_no}) AND opinions.status IN ('recombined as','corrected as','belongs to','rank changed as')";
    my @rs2 = @{$dbt->getData($sql)};
    if (scalar(@rs2) == 0) {
        push @zero,$taxon;
    } elsif (scalar(@rs2) == 1) {
        push @ok,$rs2[0];
    } else {
        push @many,\@rs2;
        push @manyt, $taxon;
    }
}

while(($auth_no,$aref)=each %screwups) {
    print "Records for authorizer ".Person::getPersonName($dbt,$auth_no)."\n";
    foreach $row (@$aref) {
        if ($auth_no != 14) {
            print "Enterer: ".Person::getPersonName($dbt,$row->{'enterer_no'});
            print " Modifier: ".Person::getPersonName($dbt,$row->{'modifier_no'})." ";
        }
        print "$row->{type_taxon_name} ($row->{tt_taxon_rank}) --> $row->{taxon_name} ($row->{taxon_rank})\n";
        if ($row->{taxon_rank} =~ /family/) {
            @bits = split(/ /,$row->{'type_taxon_name'});
            $parent = shift @bits;
            @genera = TaxonInfo::getTaxon($dbt,'taxon_name'=>$parent);
            if (scalar(@genera) == 1) {
                $sql = "UPDATE authorities SET type_taxon_no=$genera[0]->{taxon_no},modified=modified WHERE taxon_no=$row->{taxon_no}";
                print "$sql\n";
                $dbh->do($sql) if ($doUpdates);
                $sql = "UPDATE authorities SET type_taxon_no=$row->{type_taxon_no},modified=modified WHERE taxon_no=$genera[0]->{taxon_no}";
                print "$sql\n";
                $dbh->do($sql) if ($doUpdates);
            } else {
                print "ERROR: too many or too for genera for $parent: ".scalar(@genera)."\n";
            }
        }
    }
    print "\n";
}

print "SUMMARY: ".scalar(@ok). " OK ".scalar(@zero). " ZERO ".scalar(@many)." TOO MANY\n";

my %by_auth = ();
$found_count = 0;
foreach my $taxon (@zero) {
    print "COULD NOT find $taxon->{type_taxon_no} ($taxon->{type_taxon_name}) --> $taxon->{taxon_no} ($taxon->{taxon_name}) :: $taxon->{reference_no}\n";
    $sql = "SELECT o.*,r.pubyr ref_pubyr, a1.taxon_name child_name, a2.taxon_name child_spelling_name, a3.taxon_name parent_name FROM opinions o ".
           " LEFT JOIN authorities a1 ON a1.taxon_no=o.child_no".
           " LEFT JOIN authorities a2 ON a2.taxon_no=o.child_spelling_no".
           " LEFT JOIN authorities a3 ON a3.taxon_no=o.parent_no".
           " LEFT JOIN refs r ON r.reference_no=o.reference_no".
           " WHERE o.status IN ('recombined as','corrected as','belongs to','rank changed as')";
    $sql = "(".$sql." AND child_no=$taxon->{type_taxon_no}) ".
           " UNION ".
           "(".$sql." AND child_spelling_no=$taxon->{type_taxon_no})";

    my @rs = @{$dbt->getData($sql)};


    @bits = split(/ /,$taxon->{'type_taxon_name'});
    $parent = shift @bits;
    $recomb_name = $taxon->{'taxon_name'} . " " . join(" ",@bits);
    my $found_ref = 0;
    if (scalar(@rs) > 0) {
        %doit = ();
        print "FOUND these other children tho:\n";
        foreach my $row (@rs) {
            print "    #$row->{opinion_no} $row->{child_no} $row->{child_name} ($row->{child_spelling_no} $row->{child_spelling_name}) --> $row->{parent_spelling_no} $row->{parent_name}\n"; 
            #if ($manualfix{$row->{'child_spelling_no'}}) {
            #    $doit{'op_no'}=$manualfix{$row->{'child_spelling_no'}};
            #    last;
            #}
            if ($taxon->{'taxon_rank'} eq 'genus') {
                @bits2 = split(/ /,$row->{'child_spelling_name'}); 
                $parent2 = shift @bits2;
                if ($parent2 eq $taxon->{'taxon_name'}) {
                    if ($row->{'ref_has_opinion'} eq 'YES') {
                        if ($row->{'status'} !~ /nomen|syn/ && !$doit{'year'} || $row->{'ref_pubyr'} < $doit{'year'}) { 
                            print "Found earlier w/$row->{opinion_no} $row->{ref_pubyr}\n";
                            $doit{'op_no'} = $row->{'opinion_no'};
                            $doit{'year'} = $row->{'ref_pubyr'};
                            $doit{'child_no'} = $row->{'child_no'};
                            $doit{'child_spelling_no'} = $row->{'child_spelling_no'};
                            $doit{'parent_no'} = $row->{'parent_no'};
                        }
                    } else {
                        if ($row->{'status'} !~ /nomen|syn/ && !$doit{'year'} || $row->{'pubyr'} < $doit{'year'}) { 
                            print "Found earlier w/$row->{opinion_no} $row->{pubyr}\n";
                            $doit{'year'} = $row->{'pubyr'};
                            $doit{'op_no'} = $row->{'opinion_no'};
                            $doit{'child_no'} = $row->{'child_no'};
                            $doit{'child_spelling_no'} = $row->{'child_spelling_no'};
                            $doit{'parent_no'} = $row->{'parent_no'};
                        }
                    }
                }
            }
        }
        if (%doit) {
            $sql = "UPDATE authorities SET type_taxon_no=$doit{child_no},modified=modified WHERE taxon_no=$doit{parent_no}";
            print $sql."\n";
            $dbh->do($sql) if ($doUpdates);
            $found_ref++;
        }
    } else {
        $by_auth{$taxon->{'tt_authorizer_no'}}++;
    }
    if ($found_ref) {
        $found_count++;
    } else {
        my %refs;
        foreach $row (@rs) {
            $refs{$row->{reference_no}} = 1;
        }
        my $foundTT = 0;
        print "TRYING refs: ".join(" ",keys %refs)."\n";
        foreach $ref (keys %refs) {
            my @taxa = Taxon::getTypeTaxonList($dbt,$taxon->{'type_taxon_no'},$ref);
            $fields{'type_taxon'} = 0;
            foreach my $row (@taxa) {
                if ($row->{'type_taxon_no'} == $taxon->{type_taxon_no}) {
                    $foundTT = 1;
                    print "FOUND $row->{taxon_no} $row->{taxon_name} linked by ref# $ref\n";
                }
            }
        }
        if ($foundTT) {
            next;
        }
        print "WARNING, could not find opinion to match it to\n";

        $orig_type_taxon_no = TaxonInfo::getOriginalCombination($dbt,$taxon->{type_taxon_no});
        $ttaxon = TaxonInfo::getTaxon($dbt,'taxon_no'=>$orig_type_taxon_no);
        if ($taxon->{'taxon_rank'} ne 'genus' || ($taxon->{'taxon_rank'} eq 'genus' && $parent eq $taxon->{'taxon_name'})) {
            $parent_spelling_no = $taxon->{'taxon_no'};
            $parent_no = TaxonInfo::getOriginalCombination($dbt,$parent_spelling_no);
            print "NOTE: compendium ref.\n" if ($taxon->{reference_no} =~ /6930|4783|7584/ && $taxon->{taxon_rank} !~ /genus/);
            if (!$ttaxon->{'ref_is_authority'}) {
                if (!$ttaxon->{'author1last'}) {
                    print "WARN, ref_is_authority is not set and theres no author1last\n";
                }
                if (!$ttaxon->{'pubyr'}) {
                    print "WARN, ref_is_authority is not set and theres no pubyr\n";
                }
                if (!$ttaxon->{'reference_no'}) {
                    print "ERROR, no reference_no\n";
                } else {
                    if ($taxon->{taxon_rank} !~ /genus/) {
                        print "WARNING: non-species, no ref_is_authority or pubyr\n";
                        #push @{$non_by_ref{$taxon->{reference_no}}},$taxon->{authorizer_no} if ($taxon->{taxon_rank} !~ /genus/);
                        if ($taxon->{modifier_no}) {
                            push @{$highers{$taxon->{modifier_no}}},$taxon;
                        } else {
                            push @{$highers{$taxon->{enterer_no}}},$taxon;
                        }
                    } else {
                        $sql = "INSERT INTO opinions (authorizer_no,enterer_no,reference_no,child_no,child_spelling_no,status,parent_no,parent_spelling_no,created,ref_has_opinion,author1init,author1last,author2init,author2last,otherauthors,pubyr,pages,figures) VALUES ($ttaxon->{authorizer_no},$ttaxon->{enterer_no},$ttaxon->{reference_no},$orig_type_taxon_no,$taxon->{type_taxon_no},'belongs to',$parent_no,$parent_spelling_no,NOW(),'YES','','','','','','','$ttaxon->{pages}','$ttaxon->{figures}')";
                        print $sql."\n";
# Skipping these -- fixSpeciesNoBT script can handle these - in all of these cases the species was recombined or declared a nomen dubium or something
# so another opinoin doens't actually change anything
#                        $dbh->do($sql) if ($doUpdates);
                    }
                }
            } else {
                if ($taxon->{taxon_rank} !~ /genus/) {
                    print "WARNING: non-species ref_is_authority $ttaxon->{ref_is_authority} pubyr $ttaxon->{pubyr}\n";
                    if ($taxon->{modifier_no}) {
                        push @{$highers{$taxon->{modifier_no}}},$taxon;
                    } else {
                        push @{$highers{$taxon->{enterer_no}}},$taxon;
                    }
                } else {
                    $sql = "INSERT INTO opinions (authorizer_no,enterer_no,reference_no,child_no,child_spelling_no,status,parent_no,parent_spelling_no,created,ref_has_opinion,author1init,author1last,author2init,author2last,otherauthors,pubyr,pages,figures) VALUES ($ttaxon->{authorizer_no},$ttaxon->{enterer_no},$ttaxon->{reference_no},$orig_type_taxon_no,$taxon->{type_taxon_no},'belongs to',$parent_no,$parent_spelling_no,NOW(),'$ttaxon->{ref_is_authority}',".$dbh->quote($ttaxon->{author1init}).",".$dbh->quote($ttaxon->{author1last}).",".$dbh->quote($ttaxon->{author2init}).",".$dbh->quote($ttaxon->{author2last}).",".$dbh->quote($ttaxon->{otherauthors}).",'$ttaxon->{pubyr}','$ttaxon->{pages}','$ttaxon->{figures}')";
                    print $sql."\n";
#                    $dbh->do($sql) if ($doUpdates);
                }
            }
        } else {
            print "ERROR: $parent doesn't match $taxon->{taxon_name} AUTH $taxon->{tt_authorizer_no}\n";
            if ($taxon->{modifier_no}) {
                push @{$recombers{$taxon->{modifier_no}}},$taxon;
            } else {
                push @{$recombers{$taxon->{enterer_no}}},$taxon;
            }
        }

    }
} 

print "\nFOUND $found_count IN WHICH A ZERO WAS PAIRED\n\n";
print "BY_AUTH: \n";
print Dumper(\%by_auth);
print "\n";


while (($reference_no,$ar) = each %non_by_ref) {
    print "$reference_no: ".scalar(@$ar)." == ".join(" ",@$ar)."\n";
}

print "\n\nSECTION: higher level type taxon\n\n";
while (my ($no,$aref) = each %highers) {
    print "\nFor ".Person::getPersonName($dbt,$no).":\n";
    foreach my $taxon (@$aref) {
        printf "%-20s%s\n",$taxon->{taxon_name},$taxon->{type_taxon_name};
    }
}

print "\n\nSECTION: type taxon that are recombined\n\n";
while (my ($no,$aref) = each %recombers) {
    print "\nFor ".Person::getPersonName($dbt,$no).":\n";
    foreach my $taxon (@$aref) {
        printf "%-20s%s\n",$taxon->{taxon_name},$taxon->{type_taxon_name};
    }
}

