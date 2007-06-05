#!/usr/bin/perl

use lib "../cgi-bin";
use DBConnection;
use DBTransactionManager;
use TaxaCache;
use Data::Dumper;
use Classification;
use TaxonInfo;

my $dbh = DBConnection::connect();
my $dbt = new DBTransactionManager($dbh);

my ($compare,$list,$taxon,@exclude);
my @taxa = ();
my $taxon = "";
foreach (@ARGV) {
    if ($_ =~ /-e(xclude)*([A-Za-z]+)/) { # --list
        push @exclude,$2,
    } elsif ($_ =~ /-l/) { # --list
        $list = 1;
    } elsif ($_ =~ /-d(epth)*([0-9]+)/) { # --depth
        $depth = $2;
    } elsif ($_ =~ /-c/) { # --compare
        $compare = 1;
    } else {
        push @taxa, $_;
    }
}

foreach my $e (@exclude) {
    my @x = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$e});
    $e = $x[0]->{'taxon_no'};
}

foreach my $t (@taxa) {
    my @x = TaxonInfo::getTaxa($dbt,{'taxon_name'=>$t});
    $t = $x[0]->{'taxon_no'};
}

print ($compare ? "COMPARING" : "SHOWING");
print " ";
print ($list ? "LIST" : "TREE");
print ((!$list && $depth) ? "-DEPTH $depth-.":"");
print " excluding ".join(", ",@exclude) if (@exclude);
print " of ".join(", ",@taxa)."\n";

foreach $taxon_no (@taxa) {
    if ($list) {
        @children = TaxaCache::getChildren($dbt,$taxon_no,'','',\@exclude);
        print join(", ",sort {$a<=>$b} @children)."\n";
    } else {
        my $tree = TaxaCache::getChildren($dbt,$taxon_no,'tree','',\@exclude);
        printTree($tree);
    }

    if ($compare) {
        print "vs.\n";
        if ($list) {
            @c = Classification::taxonomic_search($dbt,$taxon_no);
            print join(", ",sort {$a<=>$b} @c)."\n";
        } else {
            my $tree = Classification::getChildren($dbt,$taxon_no,'tree');
            printTree($tree);
        }
    }
}

sub printTree {
    my $tree = shift;
    $tree->{'depth'} = 0;
    @nodes_to_print = ($tree);
    while (@nodes_to_print) {
        my $node = shift @nodes_to_print;
        print "  " for (0..$node->{'depth'});
        if ($node->{synonym}) {
            print " syn. ";
        }
        print $node->{'taxon_name'}."   ";
        foreach my $row (@{$node->{'spellings'}}) {
           print " sp. $row->{taxon_name}"; 
        }
        #foreach my $row (@{$node->{'synonyms'}}) {
        #    print " syn. $row->{taxon_name}"; 
        #}
        print "\n";
        my @children = ();
        push @children, @{$node->{'children'}};
        foreach (@children) {
            $_->{'depth'} = $node->{'depth'} + 1;
        }
        unshift @nodes_to_print,@children;
        my @children = ();
        push @children, @{$node->{'synonyms'}};
        foreach (@children) {
            $_->{'depth'} = $node->{'depth'};
            $_->{'synonym'} = 1;
        }
        unshift @nodes_to_print,@children;
    }
}
