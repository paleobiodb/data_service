#!/usr/bin/perl

#
# A script that does an exclusive-or with the taxa_tree_cache,taxa_list_cache,and 
# Classification::get_classification_hash for EVERY taxon, displays the results, meant for 
# debugging purposes. Does it for each taxa (takes a while to run, like 30+ 
# minutes). Redirect the output to a file, cat the file, and grep -v "Match"
# to see all the problem cases.
# All the problem cases should be weird exceptions due to corrupted opinion 
# records (generally where there are multiple original spellings for a single
# spelling)
#


use lib "../cgi-bin";
use DBConnection;
use DBTransactionManager;
use TaxaCache;
use Data::Dumper;
use TaxonInfo;
use Classification;

my $dbh = DBConnection::connect();
my $dbt = new DBTransactionManager($dbh);




$sql = "SELECT taxon_no FROM authorities";
@taxon_nos = map {$_->{'taxon_no'}} @{$dbt->getData($sql)};

print "1=tree, 2=list 3=old\n\n";
foreach my $taxon_no (@taxon_nos) {
   my $sql = "SELECT tc2.taxon_no FROM taxa_tree_cache tc1, taxa_tree_cache tc2 WHERE tc1.taxon_no=$taxon_no AND tc2.lft < tc1.lft and tc2.rgt > tc1.rgt AND tc2.synonym_no=tc2.taxon_no AND tc1.synonym_no != tc2.synonym_no ORDER BY tc2.lft DESC";
    @a1 = map {$_->{'taxon_no'}} @{$dbt->getData($sql)};
    $h2 = TaxaCache::getParents($dbt,[$taxon_no],'array');
    @a2 = @{$h2->{$taxon_no}};
    $h3 = Classification::get_classification_hash($dbt,'all',[$taxon_no],'array');
    @a3 = map{$_->{'taxon_spelling_no'}} @{$h3->{$taxon_no}};
    doMiter($taxon_no,\@a1,\@a2,\@a3);
}

print "\nDONE\n";

sub doMiter {
    my ($taxon_no,$a1,$a2,$a3) = @_;
    my @a1 = @$a1;
    my @a2 = @$a2;
    my @a3 = @$a3;
        
    my %r1;
    my %r2;
    my %r3;
    @r1{@a1} = ();
    @r2{@a2} = ();
    @r3{@a3} = ();

    my @a1_only = ();
    my @a2_only = ();
    my @a3_only = ();
    my @not_a1 = ();
    my @not_a2 = ();
    my @not_a3 = ();


    foreach my $t (@a1) {
        if (! exists $r2{$t} && !exists $r3{$t}) {
            push @a1_only,$t;
        }
    }

    foreach my $t (@a2) {
        if (! exists $r1{$t} && !exists $r3{$t}) {
            push @a2_only,$t;
        }
    }

    foreach my $t (@a3) {
        if (! exists $r1{$t} && !exists $r2{$t}) {
            push @a3_only,$t;
        }
    }

    foreach my $t (@a3) {
        if (exists $r2{$t} && !exists $r1{$t}) {
            push @not_a1,$t;
        }
    }

    foreach my $t (@a3) {
        if (exists $r1{$t} && !exists $r2{$t}) {
            push @not_a2,$t;
        }
    }

    foreach my $t (@a1) {
        if (exists $r2{$t} && !exists $r3{$t}) {
            push @not_a3,$t;
        }
    }

    if (@not_a1 || @not_a2 || @not_a3 || @a1_only || @a2_only || @a3_only) {
        print "Discrepancy for $taxon_no:\n";
        print "not_a1:".join(", ",@not_a1)."\n" if (@not_a1);
        print "not_a2:".join(", ",@not_a2)."\n" if (@not_a2);
        print "not_a3:".join(", ",@not_a3)."\n" if (@not_a3);
        print "a1_only:".join(", ",@a1_only)."\n" if (@a1_only);
        print "a2_only:".join(", ",@a2_only)."\n" if (@a2_only);
        print "a3_only:".join(", ",@a3_only)."\n" if (@a3_only);
    } else {
        print "Match for $taxon_no\n";
    }
}
