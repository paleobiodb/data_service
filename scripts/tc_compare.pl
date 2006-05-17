#!/usr/bin/perl

#
# Compares child lists of a taxon. Pass in the taxon_name on the command line
# Does an exclusive-or among three lists: 
#   1: taxa_tree_cache
#   2: taxa_list_cache
#   3: Normal (original) recursive methods (taxonomic_search)
#

use lib "../cgi-bin";
use DBConnection;
use DBTransactionManager;
use TaxaCache;
use Data::Dumper;
use TaxonInfo;

my $dbh = DBConnection::connect();
my $dbt = new DBTransactionManager($dbh);



$taxon = $ARGV[0];
@t = TaxonInfo::getTaxon($dbt,{'taxon_name'=>$taxon});
$taxon_no = $t[0]->{'taxon_no'};  

print "Comparing $taxon ($taxon_no)\n";

#$taxon_no = 36651; #Mammalia
#$taxon_no = 36652; #Cetacea
#$taxon_no = 8304; #Gastropoda
#$taxon_no = 38505 ; #Saurischia
#$taxon_no = 4 ; #Radiolaria
#$taxon_no = 755 ; #Rhizopodea
#$taxon_no = 54885; #Tracheophyta
#$taxon_no = 68233; #Hatwo test case
#$taxon_no = 36322; #Reptilia
my $sql1 = "SELECT tc2.taxon_no FROM taxa_tree_cache tc1, taxa_tree_cache tc2 WHERE tc1.taxon_no=$taxon_no AND tc2.lft >= tc1.lft and tc2.rgt <= tc1.rgt";
#my $sql1 = "SELECT tc2.taxon_no FROM taxa_tree_cache tc1, taxa_tree_cache tc2 WHERE tc1.taxon_no=$taxon_no AND tc2.lft >= tc1.lft and tc2.rgt <= tc1.rgt";
my $sql2 = "SELECT l.child_no FROM taxa_list_cache l WHERE l.parent_no=$taxon_no";  
my $sql2b = "SELECT tc2.taxon_no FROM taxa_tree_cache tc1, taxa_tree_cache tc2 WHERE tc1.taxon_no=$taxon_no AND tc2.lft = tc1.lft and tc2.rgt = tc1.rgt";

my @results3 = PBDBUtil::taxonomic_search($dbt,$taxon_no);


@results1 = map {$_->{'taxon_no'}} @{$dbt->getData($sql1)};
@results2 = map {$_->{'child_no'}} @{$dbt->getData($sql2)};
@results2b = map {$_->{'taxon_no'}} @{$dbt->getData($sql2b)};
push @results2,@results2b;

print "1=tree, 2=list 3=old\n\n";
doMiter(\@results1,\@results2,\@results3);

sub doMiter {
    my ($a1,$a2,$a3) = @_;
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

    print "not_a1:".join(", ",@not_a1)."\n\n";
    print "not_a2:".join(", ",@not_a2)."\n\n";
    print "not_a3:".join(", ",@not_a3)."\n\n";
    print "a1_only:".join(", ",@a1_only)."\n\n";
    print "a2_only:".join(", ",@a2_only)."\n\n";
    print "a3_only:".join(", ",@a3_only)."\n\n";
}
