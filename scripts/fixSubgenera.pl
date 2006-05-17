#!/usr/bin/perl

use lib '../cgi-bin';
use DBI;
use DBConnection;
use DBTransactionManager;
use Data::Dumper;
use TaxonInfo;

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
# This scripts  find (and optionally fixes) problems with opinions that doing POINT to the
# original combination
#

$fake_taxon_no = 9999999;

$sql = "SELECT * FROM authorities where taxon_rank LIKE 'subgenus'";
my @results = @{$dbt->getData($sql)};

$mult_parents = 0;
$mult_parents_genus = 0;
$no_parents = 0;
$no_genus_parent = 0;
$with_children = 0;

%mult_parents_auth = ();

$total = 0;
foreach my $row (@results) {
    if ($row->{'taxon_name'} =~ / /) {
        ($g,$sg) = Taxon::splitTaxon($row->{'taxon_name'});
        if ($g && $sg) {
            print "WARNING: skipping $row->{taxon_name}, name already seems ok\n";
            next;
        } else {
            print "ERROR: don't know what to make of $row->{taxon_name}\n";
            next;
        }
    }
    
    my $child_no = TaxonInfo::getOriginalCombination($dbt,$row->{'taxon_no'});

    my $c = getTaxon($dbt,$child_no);
    if ($c->{'taxon_rank'} !~ /subgenus/) {
        print "WARNING: skipping $row->{'taxon_name'}, orig rank is genus\n";
        next;
    }

    $sql = "SELECT * FROM opinions WHERE child_no=$child_no AND status IN ('belongs to','corrected as','rank changed as') GROUP by child_no,parent_no";
    my @r2 = @{$dbt->getData($sql)};
    print "Processing $row->{taxon_name} \n";
    %seen_spellings = ();
    my $new_name;
    if (@r2 == 0) {
        print "ERROR: No opinions found for $row->{taxon_name}\n";
        $no_parents++;
        $errors++;
        next;
    } elsif (@r2 == 1) {
        my $t = getTaxon($dbt,$r2[0]->{'parent_spelling_no'});
        $new_name = "$t->{taxon_name} ($row->{taxon_name})";
        print "New name should be $new_name for $row->{taxon_no} $row->{taxon_name}\n";
        $usql ="UPDATE authorities SET taxon_name='$new_name',modified=modified WHERE taxon_no=$row->{taxon_no}\n";
        print $usql,"\n";
        if ($doUpdates) {
            $dbh->do($usql);
        }
    } else {
        $sql = "(SELECT o.status,o.figures,o.pages, o.parent_no, o.parent_spelling_no, o.child_spelling_no,o.opinion_no, o.reference_no, o.ref_has_opinion, ".
           " a1.taxon_name child_name, a1.taxon_rank child_rank, ".
           " a2.taxon_name child_spelling_name, a2.taxon_rank child_spelling_rank, ".
           " a3.taxon_name parent_name, a3.taxon_rank parent_rank, ".
           " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000',o.pubyr,r.pubyr) pubyr,".
           " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000',o.author1last,r.author1last) author1last,".
           " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000',o.author2last,r.author2last) author2last,".
           " IF(o.pubyr IS NOT NULL AND o.pubyr != '' AND o.pubyr != '0000',o.otherauthors,r.otherauthors) otherauthors".
           " FROM opinions o LEFT JOIN refs r ON o.reference_no=r.reference_no" .
           " LEFT JOIN authorities a1 ON a1.taxon_no=o.child_no".
           " LEFT JOIN authorities a2 ON a2.taxon_no=o.child_spelling_no".
           " LEFT JOIN authorities a3 ON a3.taxon_no=o.parent_no".
           " WHERE o.child_no=$row->{taxon_no}) ORDER BY pubyr";
        @r3 = @{$dbt->getData($sql)};
        print "Number of opinions for $row->{taxon_name}: ".scalar(@r3)."\n";
        $seen_orig = 0;

        foreach my $rp (@r3) {
            print "Opinions states: $rp->{child_name}/$rp->{child_rank} SP. $rp->{child_spelling_name}/$rp->{child_spelling_rank} $rp->{status} $rp->{parent_name}/$rp->{parent_rank}\n";
            if ($rp->{'status'} =~ /rank/) {
                print "WARNING: skipping opinion $rp->{opinion_no}, already rank change\n";
                next;
            }
            my $sp = getTaxon($dbt,$rp->{'child_spelling_no'});
            if ($rp->{parent_rank} eq 'genus') {
                my $t = getTaxon($dbt,$rp->{'parent_spelling_no'});
                $new_name = "$t->{taxon_name} ($row->{taxon_name})";
                
                if ($seen_spellings{$new_name}) {
                    if ($rp->{'child_spelling_no'} ne $seen_spellings{$new_name}) {
                        $sql = "UPDATE opinions SET modified=modified,child_spelling_no=$seen_spellings{$new_name},spelling_status='reassignment' WHERE opinion_no=$rp->{opinion_no}";
                        print "REASSIGN A GENUS: $sql\n";
                        if ($doUpdates) {
                            $dbh->do($sql);
                        }
                    }
                } elsif (!$seen_orig) {
                    $seen_spellings{$new_name} = $row->{'taxon_no'};
                    $seen_orig = 1;
                    print "New name (Orig) should be $new_name for $row->{taxon_no} $row->{taxon_name}\n";
                    $usql ="UPDATE authorities SET taxon_name='$new_name',modified=modified WHERE taxon_no=$row->{taxon_no}\n";
                    print $usql,"\n";
                    if ($doUpdates) {
                        $dbh->do($usql);
                    }
                } else {
                    my $new_taxon_no;
                    $new_taxon_no = createAuthority($row,$rp->{reference_no},$new_name,'subgenus');
                    $seen_spellings{$new_name} = $new_taxon_no;
                    $sql = "UPDATE opinions SET child_spelling_no=$new_taxon_no,modified=modified,spelling_reason='reassignment' WHERE opinion_no=$rp->{opinion_no}\n";
                    print "REASSIGN A GENUS: $sql\n";
                    if ($doUpdates) {
                        $dbh->do($sql);
                    }
                }
            } else {
                if ($rp->{'child_name'} == $rp->{'child_spelling_name'} || !$rp->{'child_spelling_no'}) {
                    print "Subgenus reranked as genus and classified into $rp->{parent_name} $rp->{parent_rank}?\n";
                    my $new_taxon_no;
                    if ($seen_spelling{$row->{'taxon_name'}."genus"}) {
                        $new_taxon_no = $seen_spelling{$row->{'taxon_name'}."genus"};
                    } else {
                        $new_taxon_no = createAuthority($row,$rp->{reference_no},$row->{taxon_name},'genus');
                        $seen_spelling{$row->{'taxon_name'}."genus"} = $new_taxon_no;
                    }
                    $sql = "UPDATE opinions SET child_spelling_no=$new_taxon_no,modified=modified,spelling_reason='rank change' WHERE opinion_no=$rp->{opinion_no}\n";
                    print "RERANK A GENUS: $sql\n";
                    if ($doUpdates) {
                        $dbh->do($sql);
                    }
                } else {
                    print "WARNING: what to do with #$rp->{opinion_no} $rp->{child_name}/$rp->{child_rank} $rp->{child_spelling_name}/$rp->{child_spelling_rank} $rp->{status} $rp->{parent_name}/$rp->{parent_rank}";
                }
            }
        }
        if (!$seen_orig) {
            print "ERROR: did not find orig name for $row->{taxon_name}\n";
            next;
        }
    }

    $sql = "SELECT o.*,".
           " a1.taxon_name child_name, a1.taxon_rank child_rank, ".
           " a2.taxon_name child_spelling_name, a2.taxon_rank child_spelling_rank, ".
           " a3.taxon_name parent_name, a3.taxon_rank parent_rank ".
           " FROM opinions o ".
           " LEFT JOIN authorities a1 ON a1.taxon_no=o.child_no".
           " LEFT JOIN authorities a2 ON a2.taxon_no=o.child_spelling_no".
           " LEFT JOIN authorities a3 ON a3.taxon_no=o.parent_no".
           " WHERE parent_no=$child_no";
    my @r4 = @{$dbt->getData($sql)};
    foreach my $c_row (@r4) {
        print "CHILD Opinion states: $c_row->{child_name}/$c_row->{child_rank} SP. $c_row->{child_spelling_name}/$c_row->{child_spelling_rank} $c_row->{status} $c_row->{parent_name}/$c_row->{parent_rank}\n";
        if ($c_row->{'child_spelling_name'} =~ /\(/) {
            print "WARNING: skipping $c_row->{child_spelling_name} already seems to be ok\n";
            next;
        }
        if (@r2 > 1 && @r4) {
            print "ERROR: skipping, multiple parents to choose from";
            next;
        } 
        if ($c_row->{'status'} =~ /belongs|recombined|corrected/) {
            my ($g1,$sg1) = Taxon::splitTaxon($new_name);
            my ($sg2,$xxx,$ss2) = Taxon::splitTaxon($c_row->{'child_spelling_name'});

            if ($sg1 ne $sg2) {
                print "ERROR: names dont' seem to match up for $new_name and $c_row->{child_spelling_name}\n";
            }

            my $new_child_name = "$g1 ($sg1) $ss2";
            print "New child name $new_child_name for $c_row->{child_spelling_no} $c_row->{child_spelling_name}\n";
            $usql ="UPDATE authorities SET taxon_name='$new_child_name',modified=modified WHERE taxon_no=$c_row->{child_spelling_no}\n";
            print $usql,"\n";
            if ($doUpdates) {
                $dbh->do($usql);
            }
        } else {
            print "ERROR: Diff status $c_row->{status}\n";
        }
    }
    $with_children++ if (@r3);
    print "\n";
    $total++;
}

print "Total: $total\n";
print "Total OK: $total_ok\n";
print "Total ??: $total_bad\n";
print "With no parents: $no_parents\n";
print "With no genus level parent: $no_genus_parent\n";
print "With children $with_children\n";



sub createAuthority {
    my $row = $_[0];
    my $ref = $_[1];
    my $taxon = $_[2];
    my $rank = $_[3];
    # author information comes from the original combination,
    # I'm doing this the "old" way instead of using some
    #  ridiculously complicated Poling-style objects
    $row->{'pages'} ||= '';
    $row->{'figures'} ||= '';
    $row->{'comments'} ||= '';
    $row->{'extant'} ||= '';
    
    my $pages = $dbh->quote($row->{'pages'});
    my $figures = $dbh->quote($row->{'figures'});
    my $comments = $dbh->quote($row->{'comments'});
    my $extant= $dbh->quote($row->{'extant'});

    my ($auth1init,$auth1last,$auth2init,$auth2last,$otherauthors,$pubyr);

    if ( $row->{'ref_is_authority'} !~ /yes/i)   {
        $row->{'author1init'} ||= '';
        $row->{'author1last'} ||= '';
        $row->{'author2init'} ||= '';
        $row->{'author2last'} ||= '';
        $row->{'otherauthors'} ||= '';
        $row->{'pubyr'} ||= '';
        $auth1init = $dbh->quote($row->{author1init}); 
        $auth1last = $dbh->quote($row->{author1last}); 
        $auth2init = $dbh->quote($row->{author2init}); 
        $auth2last = $dbh->quote($row->{author2last}); 
        $otherauthors = $dbh->quote($row->{otherauthors}); 
        $pubyr = $dbh->quote($row->{pubyr});
    } else {
        my $rsql = "SELECT * FROM refs WHERE reference_no=" . $row->{'reference_no'};
        my $rref = ${$dbt->getData($rsql)}[0];
        $rref->{'author1init'} ||= '';
        $rref->{'author1last'} ||= '';
        $rref->{'author2init'} ||= '';
        $rref->{'author2last'} ||= '';
        $rref->{'otherauthors'} ||= '';
        $rref->{'pubyr'} ||= '';
        $auth1init = $dbh->quote($rref->{author1init}); 
        $auth1last = $dbh->quote($rref->{author1last}); 
        $auth2init = $dbh->quote($rref->{author2init}); 
        $auth2last = $dbh->quote($rref->{author2last}); 
        $otherauthors = $dbh->quote($rref->{otherauthors}); 
        $pubyr = $dbh->quote($rref->{pubyr});
    }
         
    $sql = "INSERT INTO authorities (authorizer_no,enterer_no,reference_no,taxon_rank,taxon_name,ref_is_authority,author1init,author1last,author2init,author2last,otherauthors,pubyr,pages,figures,comments,created,modified,extant) VALUES ($row->{authorizer_no},$row->{enterer_no},$ref,'$rank','$taxon','',$auth1init,$auth1last,$auth2init,$auth2last,$otherauthors,$pubyr,$pages,$figures,'',NOW(),NOW(),$extant)";
    print "INSERT NEW GENUS: $sql\n";
    my $new_taxon_no;
    if ($doUpdates) {
        $dbh->do($sql);
        $new_taxon_no = $dbh->{'mysql_insertid'};
    } else {
        $new_taxon_no = $fake_taxon_no;
        $fake_taxon_no--;
    }
    return $new_taxon_no;
}



sub getTaxon {
    my $dbt = shift;
    my $no = shift;
    return ${$dbt->getData("SELECT * FROM authorities WHERE taxon_no=$no")}[0];
}
