# 
# PBDB LIB Taxonomy.pm
# --------------------
# 
# Test the module Taxonomy.pm.  This file tests the ability to get simple
# lists of Taxon objects.


use lib 'lib';
use lib 't';

use Test::More tests => 6;
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

my $NAME1 = 'Dascillidae';
my $ID1 = 69296;

my $TAXON1 = { 
  'taxon_no' => '69296',
  'orig_no' => '69296',
  'taxon_name' => 'Dascillidae',
  'taxon_rank' => '9',
  'status' => 'belongs to',
  'base_no' => '69296',
  'parent_no' => '69295',
  'senpar_no' => '69295' };

bless $TAXON1, 'Taxon';

my $TREE_TABLE = $taxonomy->{TREE_TABLE};
my $ATTRS_TABLE = $taxonomy->{ATTRS_TABLE};
my $COUNTS_TABLE = $taxonomy->{COUNTS_TABLE};

my $sql1 = "SELECT lft, rgt, n_occs, taxon_size as size, extant_size, phylum_count,
		class_count, order_count, family_count, genus_count, species_count
	    FROM $TREE_TABLE join $ATTRS_TABLE using (orig_no) join $COUNTS_TABLE using (orig_no)
	    WHERE orig_no = $ID1";

my $TAXON1_VAR = $dbh->selectrow_hashref($sql1);

bless $TAXON1_VAR, 'Taxon';

my $TAXON1_SEARCH = {
  orig_no => 69296,
  taxon_name => 'Dascillidae',
  taxon_rank => 9,
  senpar_no => 69295,
  lft => $TAXON1_VAR->{lft},
  rgt => $TAXON1_VAR->{rgt},
};

bless $TAXON1_SEARCH, 'Taxon';

my $TAXON1_LINK = { 
  'accepted_no' => 69296,
  'parent_no' => 69295,
  'senpar_no' => 69295,
  'synonym_no' => 69296,
};

my $TAXON1_ATTR = {
  'attribution' => 'Guerin-Meneville 1843',
  'pubyr' => 1843,
};

my $TAXON1_COUNTS = {
  phylum_count => $TAXON1_VAR->{phylum_count},
  class_count => $TAXON1_VAR->{class_count},
  order_count => $TAXON1_VAR->{order_count},
  family_count => $TAXON1_VAR->{family_count},
  genus_count => $TAXON1_VAR->{genus_count},
  species_count => $TAXON1_VAR->{species_count},
};

my $TAXON1_PARENT = {
  parent_name => 'Dascilloidea',
  parent_rank => 10,
};

my $TAXON1_PHYLO = {
   class => 'Insecta',
   class_no => 56637,
   family => 'Dascillidae',
   family_no => 69296,
   kingdom => 'Metazoa',
   kingdom_no => 2,
   order => 'Coleoptera',
   order_no => 69148,
   phylum => 'Arthropoda',
   phylum_no => 18891,
};

my $TAXON1_DATA = {
  taxon_no => 69296,
  orig_no => 69296,
  taxon_name => 'Dascillidae',
  common_name => 'soft bodied plant beetle',
  taxon_rank => 9,
  status => 'belongs to',
  base_no => 69296,
  accepted_no => 69296,
  is_extant => 1,
  parent_no => 69295,
  reference_no => 5056,
  senpar_no => 69295,
  lft => $TAXON1_VAR->{lft},
  n_occs => $TAXON1_VAR->{n_occs},
};

bless $TAXON1_DATA, 'Taxon';		   

my $NAME2 = 'Felidae';
my $ID2 = 41045;

my $NAME3 = 'Megatherium';
my $ID3 = '43616';

my $NAME4a = 'Cardium (Acanthocardia) exochum';
my $ID4a = '100054';	# orig

my $NAME4b = 'Afrocardium exochum';
my $ID4b = '100055';	# current

my $DEC_RE = qr{ ^ \d+ \. \d+ $ }xs;


subtest 'list_taxa_simple: basic calls' => sub {
    
    my ($t1, $t2);
    
    ($t1) = $taxonomy->list_taxa_simple($ID1);
    
    cmp_deeply $t1, superhashof($TAXON1), "list_taxa_simple '$ID1'";
    
    my ($t1, $t2) = $taxonomy->list_taxa_simple([$ID1, $ID2]);
    
    ok( ref $t1 && ref $t2 && 
	($t1->{orig_no} eq $ID1 || $t1->{orig_no} eq $ID2) &&
	($t2->{orig_no} eq $ID1 || $t2->{orig_no} eq $ID2),
	"list_taxa_simple [$ID1, $ID2]");
    
    ($t1, $t2) = $taxonomy->list_taxa_simple("$ID1 ,$ID2");
    
    ok( ref $t1 && ref $t2 && 
	($t1->{orig_no} eq $ID1 || $t1->{orig_no} eq $ID2) &&
	($t2->{orig_no} eq $ID1 || $t2->{orig_no} eq $ID2),
	"list_taxa_simple \"$ID1 ,$ID2\"");
    
    my $tt = bless { taxon_no => $ID1 }, 'Taxon';
    
    eval {
	($t1) = $taxonomy->list_taxa_simple($tt);
    };
    
    ok ( !$@, 'list_taxa_simple with object' ) or diag( "message was: $@" );
    
    ok( ref $t1 && $t1->{orig_no} eq $ID1, 'list_taxa_simple with object' );
    
    my $tts = bless { $ID1 => 1, $ID2 => 1 }, 'TaxonSet';
    
    eval {
	($t1) = $taxonomy->list_taxa_simple($tts);
    };
    
    ok( !$@, 'list_taxa_simple with hash' ) or diag( "message was: $@" );
    
    ok( ref $t1 && ( $t1->{orig_no} eq $ID1 || $t1->{orig_no} eq $ID2 ) );
    
    eval {
	($t1) = $taxonomy->list_taxa_simple("abc");
	($t2) = $taxonomy->list_taxa_simple("99999999");
    };
    
    ok( !$@, 'list_taxa_simple with bad scalar' );
    
    ok( !defined $t1, 'list_taxa_simple with bad scalar' );
    ok( !defined $t2, 'list_taxa_simple with nonexistent taxon id');
    
    eval {
	($t1) = $taxonomy->list_taxa_simple("abc", "def");
    };
    
    ok( $@ =~ /second argument/, 'list_taxa_simple with bad options' );
    
    eval {
	($t1) = $taxonomy->list_taxa_simple([$ID1], [$ID2]);
    };
    
    ok( $@ =~ /second argument/, 'list_taxa_simple with two listrefs' );
    
    eval {
	($t1) = $taxonomy->list_taxa_simple($ID1, { bad_arg => 1, extant => 1 });
    };
    
    ok( $@ =~ /invalid option/, 'list_taxa_simple with invalid option' );
};


subtest 'list_taxa_simple: rank, status, and extant filters' => sub {
    
    my ($t1, $t2);
    
    ($t1) = $taxonomy->list_taxa_simple($ID1, { min_rank => 2, max_rank => 20 });
    
    ok( ref $t1 && $t1->{orig_no} eq $ID1, "min and max that encompass actual rank" );
    
    ($t1) = $taxonomy->list_taxa_simple($ID1, { min_rank => 20 });
    
    ok( !defined $t1, "high min" );
    
    ($t1) = $taxonomy->list_taxa_simple($ID1, { max_rank => 4 });
    
    ok( !defined $t1, "low max" );
    
    ($t1) = $taxonomy->list_taxa_simple($ID1, { status => 'valid' });
    
    ok( ref $t1 && $t1->{orig_no} eq $ID1, "status valid" );
    
    ($t1) = $taxonomy->list_taxa_simple($ID1, { status => 'invalid' });
    
    ok( !defined $t1, "status invalid" );
    
    ($t1) = $taxonomy->list_taxa_simple($ID2, { extant => 1 });
    
    ok( ref $t1 && $t1->{orig_no} eq $ID2, "extant true" );
    
    ($t1) = $taxonomy->list_taxa_simple($ID2, { extant => 0 });
    
    ok( !defined $t1, "extant false" );
    
    ($t1) = $taxonomy->list_taxa_simple($ID3, { extant => 1 });
    
    ok( !defined $t1, "extinct true" );
    
    ($t1) = $taxonomy->list_taxa_simple($ID3, { extant => 0 });
    
    ok( ref $t1 && $t1->{orig_no} eq $ID3, "extinct false" );
};


subtest 'list_taxa_simple: exact filter' => sub {
    
    my ($t4a, $t4b, $e4a_e, $e4b);
    
    eval {
	($t4a) = $taxonomy->list_taxa_simple($ID4a);
	($t4b) = $taxonomy->list_taxa_simple($ID4b);
	($e4a) = $taxonomy->list_taxa_simple($ID4a, { exact => 1 });
	($e4b) = $taxonomy->list_taxa_simple($ID4b, { exact => 1 });
    };
    
    ok( !$@, "list_taxa_simple with 'exact' option" ) or diag("message was: $@");
    return if $@;
    
    # We must delete 'base_no' fields, so we can compare that the records
    # are identical except for these fields.
    
    delete $t4a->{base_no};
    delete $t4b->{base_no};
    delete $e4a->{base_no};
    delete $e4b->{base_no};
    
    ok( $t4a->{orig_no} eq $ID4a && $t4a->{taxon_no} eq $ID4b, "not exact" );
    ok( $e4a->{orig_no} eq $ID4a && $e4a->{taxon_no} eq $ID4a, "exact" );
    
    cmp_deeply( $t4a, $t4b, 'not exact: no difference between variants' );
    cmp_deeply( $t4b, $e4b, 'exact/inexact: no difference with current variant');
    
    ok( !eq_deeply( $t4a, $e4a ), 'exact/inexact: yes difference between orig and current' );
};


subtest 'list_taxa_simple: fields' => sub {

    my ($t1, $t2, $t3, $t4, $t5);
    
    eval {
	($t1) = $taxonomy->list_taxa_simple($ID1, { fields => 'DATA' });
	($t2) = $taxonomy->list_taxa_simple($ID1, { fields => 'SEARCH' });
	($t3) = $taxonomy->list_taxa_simple($ID1, { fields => 'LINK, ATTR ,, APP,SIZE' });
	($t4) = $taxonomy->list_taxa_simple($ID1, { fields => ['PHYLO','PARENT', 'COUNTS'] });
	($t5) = $taxonomy->list_taxa_simple($ID1, { fields => 'SIMPLE, family_no, image_no' });
     };
    
    ok( !$@, "eval OK" ) or diag( "message was: $@" );
    
    cmp_deeply $t1, superhashof($TAXON1_DATA), "fields 'DATA'";

    ok( $t1->{lft} > 0 && $t1->{n_occs} > 0, "fields 'DATA' variable" );
    
    cmp_deeply $t2, noclass(superhashof($TAXON1_SEARCH)), "fields 'SEARCH'";
    
    ok( $t2->{lft} > 0 && $t2->{rgt} > 0, "fields 'SEARCH' variable");
    
    cmp_deeply $t3, noclass(superhashof($TAXON1_LINK)), "fields 'LINK'";
    
    cmp_deeply $t3, noclass(superhashof($TAXON1_ATTR)), "fields 'ATTR'";
    
    ok( defined $t3->{firstapp_ea} && $t3->{firstapp_ea} =~ $DEC_RE &&
	defined $t3->{firstapp_la} && $t3->{firstapp_la} =~ $DEC_RE &&
	defined $t3->{lastapp_ea} && $t3->{lastapp_ea} =~ $DEC_RE &&
	defined $t3->{lastapp_la} && $t3->{lastapp_la} =~ $DEC_RE,
	"fields 'APP'" );
    
    ok( $t3->{taxon_size} > 0 && $t3->{extant_size} > 0 && $t3->{n_occs} > 0,
	"fields 'SIZE'" );
    
    cmp_deeply $t4, noclass(superhashof($TAXON1_COUNTS)), "fields 'COUNTS'";

    cmp_deeply $t4, noclass(superhashof($TAXON1_PHYLO)), "fields 'PHYLO'";
    
    cmp_deeply $t4, noclass(superhashof($TAXON1_PARENT)), "fields 'PARENT'";
    
    is( $t5->{family_no}, '69296', "fields 'family_no'" );
    
    ok( $t5->{image_no} > 0, "fields 'image_no'" );
};


subtest 'list_subtree' => sub {
    
    my (@r1, $r1, @r2, @r3);
    
    eval {
	@r1 = $taxonomy->list_subtree($ID1);
	$r1 = $taxonomy->list_subtree($ID1, { return => 'listref' });
    };
    
    ok( !$@, 'list_subtree basic' ) or diag("message was: $@");
    ok( ref $r1 eq 'ARRAY' && scalar(@$r1) == scalar(@r1), "list_subtree return 'listref'" );
    
    my $r1min = 99; my $r1max = 0;
    
    foreach $t ( @r1 )
    {
	my $rank = $t->{taxon_rank};
	$r1min = $rank if $rank < $r1min;
	$r1max = $rank if $rank > $r1max;
    }
    
    is( $r1min, 3, 'list_subtree lower bound rank' );
    is( $r1max, 9, 'list_subtree upper bound rank' );
    
    eval {
	@r2 = $taxonomy->list_subtree($ID1, { min_rank => 5, max_rank => 5 });
    };    
    
    ok( !$@, 'list_subtree with rank filters' ) or diag("message was: $@");
    
    my $r2min = 99; my $r2max = 0;
        
    foreach $t ( @r2 )
    {
	my $rank = $t->{taxon_rank};
	$r2min = $rank if $rank < $r2min;
	$r2max = $rank if $rank > $r2max;
    }
    
    is( $r2min, 5, 'list_subtree with rank filter lower bound' );
    is( $r2min, 5, 'list_subtree with rank filter upper bound' );
    
    ok( scalar(@r2) < scalar(@r1), 'list_subtree with rank filter smaller result' );
    
    eval {
	@r3 = $taxonomy->list_subtree("$ID1, $ID2");
    };
    
    ok( !$@, 'list_subtree multiple base_nos' ) or diag("message was: $@");
    
    eval {
	@r3 = $taxonomy->list_subtree([$ID1, $ID2]);
    };
    
    ok( !$@, 'list_subtree multiple base_nos 2') or diag("message was: $@");
    
    eval {
	@r3 = $taxonomy->list_subtree($ID1, { fields => 'APP' });
    };
    
    ok( !$@, "list_subtree with option 'fields'" ) or diag("message was: $@");
    
    ok( $r3[5]{firstapp_ea} > 0, 'list_subtree provides proper fields' );
};


#print STDERR Dumper($t1);
