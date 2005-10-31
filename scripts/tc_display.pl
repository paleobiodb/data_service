#!/usr/bin/perl

use lib "../cgi-bin";
use DBConnection;
use DBTransactionManager;
use TaxaCache;
use Data::Dumper;
use TaxonInfo;

my $dbh = DBConnection::connect();
my $dbt = new DBTransactionManager($dbh);

my ($compare,$list,$taxon);
foreach (@ARGV) {
    if ($_ =~ /-l/) { # --list
        $list = 1;
    } elsif ($_ =~ /-d(epth)*([0-9]+)/) { # --depth
        $depth = $2;
    } elsif ($_ =~ /-c/) { # --compare
        $compare = 1;
    } else {
        push @REST,$_;
    }
}


$taxon = $REST[0];
@t = TaxonInfo::getTaxon($dbt,'taxon_name'=>$taxon);
$taxon_no = $t[0]->{'taxon_no'};

print ($compare ? "COMPARING" : "SHOWING");
print " ";
print ($list ? "LIST" : "TREE");
print ((!$list && $depth) ? "-DEPTH $depth-.":"");
print " of $taxon ($taxon_no)\n";

if ($taxon_no) {
    if ($list) {
        @children = TaxaCache::getChildren($dbt,$taxon_no);
        print join(", ",sort {$a<=>$b} @children)."\n";
    } else {
        my $tree = TaxaCache::getChildren($dbt,$taxon_no,'tree',$depth);
        $tree->{'depth'} = 0;
        @nodes_to_print = ($tree);
        while (@nodes_to_print) {
            my $node = shift @nodes_to_print;
            print "  " for (0..$node->{'depth'});
            print $node->{'taxon_name'}."   ";
            foreach my $row (@{$node->{'spellings'}}) {
               print " sp. $row->{taxon_name}"; 
            }
            foreach my $row (@{$node->{'synonyms'}}) {
                print " syn. $row->{taxon_name}"; 
            }
            print "\n";
            my @children = ();
            push @children, @{$node->{'children'}};
            foreach (@children) {
                $_->{'depth'} = $node->{'depth'} + 1;
            }
            unshift @nodes_to_print,@children;
        }
    }

    if ($compare) {
        print "vs.\n";
        if ($list) {
            @c = PBDBUtil::taxonomic_search($dbt,$taxon_no);
            print join(", ",sort {$a<=>$b} @c)."\n";
        } else {
            PBDBUtil::getChildren($dbt,$taxon_no);
        }
    }
}
