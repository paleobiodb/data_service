# 
# PBDB LIB Taxonomy.pm
# --------------------
# 
# Test the module Taxonomy.pm.  This file tests the ability to resolve names
# into taxon numbers.


use lib 'lib';
use lib 't';

use Test::More tests => 4;
use Test::Deep;

use CoreFunction qw(connectDB configData);
use Taxonomy;

use Data::Dumper;


# We turn off warnings for uninitialized variables and non-numeric
# comparisons, because we don't want to clutter up our testing code with
# "defined $t1 && $t1->{foo} eq ...

no warnings 'uninitialized';
no warnings 'numeric';



my ($dbh, $taxonomy);

eval {
    $dbh = connectDB("config.yml");
    $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
};

unless ( ok(defined $dbh && defined $taxonomy, "dbh acquired and taxonomy instantiated") )
{
    diag("message was: $@");
    BAIL_OUT;
}


# Define some test data to compare against the database results

my $TREE_TABLE = $taxonomy->{TREE_TABLE};
my $ATTRS_TABLE = $taxonomy->{ATTRS_TABLE};
my $COUNTS_TABLE = $taxonomy->{COUNTS_TABLE};

my $NAME1 = 'Dascillidae';
my $ID1 = 69296;

my $sql1 = "SELECT lft, rgt, n_occs, taxon_size as size, extant_size
	    FROM $TREE_TABLE join $ATTRS_TABLE using (orig_no) join $COUNTS_TABLE using (orig_no)
	    WHERE orig_no = $ID1";

my $TAXON1_VAR = $dbh->selectrow_hashref($sql1);

my $TAXON1_SEARCH = {
  orig_no => 69296,
  taxon_name => 'Dascillidae',
  taxon_rank => 9,
  senpar_no => 69295,
  lft => $TAXON1_VAR->{lft},
  rgt => $TAXON1_VAR->{rgt},
};

my $EX1 = '^Dascillinae';
my $EXID1 = '70061';
my $EX2 = '^Ectopria';
my $EXID2 = '283335';
my $BAD1 = '^BadName';
my $BAD2 = 'BadName';

my $MISSING1 = 'Dascillus';
my $MISSING2 = 'Ectopria laticollis';


my $NAME2 = 'Felidae';
my $ID2 = '41045';

my $NAME3 = 'Megatherium';
my $ID3 = '43616';

my $NAME4a = 'Cardium (Acanthocardia) exochum';
my $ID4a = '100054';

my $NAME4b = 'Afrocardium exochum';
my $ID4b = '100055';

my $NAME5a = 'Dino:T. rex';
my $NAME5b = 'Tyrannosaurus rex';
my $NAME5c = 'Telmatornis rex';
my $NAME5d = 'Dino:T.rex';

my $NAME6a = 'Ficus';
my $NAME6b = 'Mollusc:Ficus';
my $NAME6c = 'Gastro:Ficus';
my $NAME6d = 'Mollusc:Gastro:Ficus';
my $NAME6e = 'Plant:Ficus';
my $NAME6f = 'Morac:Ficus';

my $NAME6g = 'Gastro^Ficus';
my $NAME6h = 'Mollusc: Gastro^Ficus ^Maturifusidae';

my $DEC_RE = qr{ ^ \d+ \. \d+ $ }xs;


subtest 'resolve_names' => sub {
    
    my ($t1, $t2, $t3, @t, @w1, @w2, @w3);
    
    eval {
	$DB::single = 1;
	($t2) = $taxonomy->resolve_names($BAD1);
	@w2 = $taxonomy->list_warnings;
	($t3) = $taxonomy->resolve_names($BAD2);
	@w3 = $taxonomy->list_warnings;
	($t1) = $taxonomy->resolve_names($NAME1);
	@w1 = $taxonomy->list_warnings;
    };
    
    ok( !$@, 'resolve_names basic' ) or diag("message was: $@");
    
    cmp_deeply $t1, superhashof($TAXON1_SEARCH), "resolve_names '$NAME1'";
    is( scalar(@w1), 0, 'no warnings for good name' );
    ok( !defined $t2, "resolve_names '$BAD1'" );
    is( scalar(@w2), 1, 'warning for bad name' );
    ok( !defined $t3, "resolve_names '$BAD2'" );
    is( scalar(@w3), 1, 'warning for bad name 2' );
    
    my ($t4a, $t4b);
    
    $DB::single = 1;
    
    eval {
	($t4a) = $taxonomy->resolve_names($NAME4a);
	($t4b) = $taxonomy->resolve_names($NAME4b);
    };
    
    is( $t4a->{taxon_name}, $NAME4a, 'orig name' );
    is( $t4b->{taxon_name}, $NAME4b, 'current name' );
    
    eval {
	@t = $taxonomy->resolve_names("$NAME1, $NAME2, $NAME3");
    };
    
    ok( !$@, 'resolve_names multiple' ) or diag("message was: $@");
    
    my %found;
    
    foreach my $t ( @t )
    {
	$found{$t->{taxon_name}} = 1;
    }
    
    ok( $found{$NAME1}, "found $NAME1" );
    ok( $found{$NAME2}, "found $NAME2" );
    ok( $found{$NAME3}, "found $NAME3" );
    
    eval {
	@t = $taxonomy->resolve_names($NAME1, $NAME2);
    };
    
    ok( $@, 'error with bad second argument' );
    
    eval {
	@t = $taxonomy->resolve_names($NAME1, { status => 'invalid', bad_option => 1 });
    };
    
    ok( $@, 'error with bad option name' );
};


subtest 'prefix' => sub {
    
    my ($t6a, $t6b, $t6c, $t6d, $t6e, $t6f);
    
    eval {
	($t6a) = $taxonomy->resolve_names($NAME6a);
	($t6b) = $taxonomy->resolve_names($NAME6b);
	($t6c) = $taxonomy->resolve_names($NAME6c);
	($t6d) = $taxonomy->resolve_names($NAME6d);
	($t6e) = $taxonomy->resolve_names($NAME6e);
	($t6f) = $taxonomy->resolve_names($NAME6f);
    };

    ok( !$@, 'resolve_names with prefixes' ) or diag("message was: $@");
    
    ok( $t6a->{orig_no} > 0, "found $NAME6a" );
    ok( $t6b->{orig_no} > 0, "found $NAME6b" );
    ok( $t6c->{orig_no} > 0, "found $NAME6c" );
    ok( $t6d->{orig_no} > 0, "found $NAME6d" );
    ok( $t6e->{orig_no} > 0, "found $NAME6e" );
    ok( $t6f->{orig_no} > 0, "found $NAME6f" );
    
    is( $t6b->{orig_no}, $t6c->{orig_no}, 'animal prefixes' );
    is( $t6c->{orig_no}, $t6d->{orig_no}, 'double prefix' );
    is( $t6e->{orig_no}, $t6f->{orig_no}, 'plant prefixes');
    isnt($t6b->{orig_no}, $t6e->{orig_no}, 'plant vs. animal');
    ok( $t6a->{orig_no} eq $t6b->{orig_no} || $t6a->{orig_no} eq $t6e->{orig_no}, 'plant or animal');
    
    my (@t1, @t2, @t3);
    
    eval {
	@t1 = $taxonomy->resolve_names($NAME5a);
	@t2 = $taxonomy->resolve_names($NAME5a, { all_names => 1 });
	@t3 = $taxonomy->resolve_names($NAME5d, { all_names => 1 });
    };
    
    ok( !$@, 'resolve_names with all_names' ) or diag("message was: $@");
    
    my (%found1, %found2);
    
    foreach my $t ( @t1 )
    {
	$found1{$t->{taxon_name}} = 1;
    }
    
    foreach my $t ( @t2 )
    {
	$found2{$t->{taxon_name}} = 1;
    }
    
    is( scalar(@t1), 1, 'without all_names' );
    cmp_ok( scalar(@t2), '>', 1, 'with all_names' );
    
    ok( $found1{$NAME5b}, "found $NAME5b without all_names" );
    ok( $found2{$NAME5b}, "found $NAME5b with all_names" );
    ok( $found2{$NAME5c}, "found $NAME5c with all_names" );
    is( scalar(@t2), scalar(@t3), "equivalence of '$NAME5a' and '$NAME5b'" );
};


subtest 'exclusions' => sub {

    my (@t1, @t2, @t3, @t4);
    my (@w1, @w2, @w3, @w4);
    
    eval {
	@t1 = $taxonomy->resolve_names("$NAME1");
	@w1 = $taxonomy->list_warnings;
	@t2 = $taxonomy->resolve_names("$NAME1$EX1");
	@w2 = $taxonomy->list_warnings;
	@t3 = $taxonomy->resolve_names("$NAME1$EX1$EX2");
	@w3 = $taxonomy->list_warnings;
	@t4 = $taxonomy->resolve_names("$NAME1$BAD1");
	@w4 = $taxonomy->list_warnings;
    };

    ok( !$@, 'resolve_names with exclusions' ) or diag("message was: $@");
    
    is( scalar(@t1), 1, 'resolve_names plain' );
    is( scalar(@w1), 0, 'no warnings' );
    is( scalar(@t2), 2, 'resolve_names with one exclusion' );
    is( scalar(@w2), 0, 'no warnings with one exclusion' );
    is( scalar(@t3), 3, 'resolve_names with two exclusions' );
    is( scalar(@w3), 0, 'no warnings with two exclusions' );
    is( scalar(@t4), 1, 'resolve_names with bad exclusion' );
    is( scalar(@w4), 1, 'one warning with bad exclusion' );
    
    is( $t1[0]{orig_no}, $ID1, "resolve_names found '$NAME1'" );
    is( $t2[1]{orig_no}, $EXID1, "resolve_names found '$EX1'" );
    is( $t3[2]{orig_no}, $EXID2, "resolve_names found '$EX2'" );
};
