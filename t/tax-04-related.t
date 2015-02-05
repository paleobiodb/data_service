# -*- mode: CPerl; -*-
# 
# PBDB LIB Taxonomy.pm
# --------------------
# 
# Test the module Taxonomy.pm.  This file tests the method 'list_taxa'.

use strict;

use lib 'lib';
use lib 't';

use Test::More tests => 16;
use Test::Deep;
use Carp qw(carp croak);

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


my $NAME1 = 'Stegosauria';
my $ID1 = '38798';
my $TX1 = '147919';

my $NAME2 = 'Stegosauridae';
my $ID2 = '38803';

my $TREE_TABLE = $taxonomy->{TREE_TABLE};
my $AUTH_TABLE = $taxonomy->{AUTH_TABLE};
my $ATTRS_TABLE = $taxonomy->{ATTRS_TABLE};
my $INTS_TABLE = $taxonomy->{INTS_TABLE};
my $COUNTS_TABLE = $taxonomy->{COUNTS_TABLE};
my $REFS_TABLE = $taxonomy->{REFS_TABLE};

my (%SIMPLE_T, %SIMPLE_A, %DATA_T, %DATA_A);
my (%LINK, %APP, %ATTR, %PARENT, %SIZE, %PHYLO, %COUNTS, %CRMOD);
my (%image_no);

eval {
    
    my ($sql, $result);
    
    $sql = "SELECT a.taxon_no, a.orig_no, a.taxon_name, (a.taxon_rank + 0) as taxon_rank,
		t.status, t.parent_no, t.senpar_no
	    FROM $TREE_TABLE as t join $AUTH_TABLE as a using (orig_no)
	    WHERE a.taxon_no in ($ID1, $TX1, $ID2)";
    
    %SIMPLE_A = hash_by_taxon($dbh, $sql);
    
    $sql = "SELECT t.orig_no, t.spelling_no as taxon_no, t.name as taxon_name,
		t.rank as taxon_rank, t.status, t.parent_no, t.senpar_no
	    FROM $TREE_TABLE as t
	    WHERE t.orig_no in ($ID1, $ID2)";
    
    %SIMPLE_T = hash_by_taxon($dbh, $sql);
    
    $sql = "SELECT a.taxon_no, a.orig_no, a.taxon_name, (a.taxon_rank + 0) as taxon_rank,
		t.lft, t.status, t.accepted_no, t.parent_no, t.senpar_no,
		a.common_name, a.reference_no, v.n_occs, v.is_extant
	    FROM $TREE_TABLE as t join $ATTRS_TABLE as v using (orig_no)
		join $AUTH_TABLE as a on a.taxon_no = t.spelling_no
	    WHERE t.orig_no in ($ID1, $ID2)";
    
    %DATA_A = hash_by_taxon($dbh, $sql);
    
    $sql = "SELECT t.orig_no, t.lft, t.name as taxon_name, t.rank as taxon_rank,
		t.spelling_no as taxon_no, t.accepted_no, t.parent_no, t.senpar_no,
	        t.status, a.reference_no,
		v.n_occs, v.image_no, v.is_extant, a.common_name
	    FROM $TREE_TABLE as t join $ATTRS_TABLE as v using (orig_no)
		join $AUTH_TABLE as a on a.taxon_no = t.spelling_no
	    WHERE t.orig_no in ($ID1, $ID2)";
    
    %DATA_T = hash_by_taxon($dbh, $sql);
    
    $sql = "SELECT t.orig_no, t.synonym_no, t.accepted_no, t.parent_no, t.senpar_no
	    FROM $TREE_TABLE as t
	    WHERE orig_no in ($ID1, $ID2)";
    
    %LINK = hash_by_taxon($dbh, $sql);
    
    $sql = "SELECT v.orig_no,
		   v.first_early_age as firstapp_ea,
		   v.first_late_age as firstapp_la,
		   v.last_early_age as lastapp_ea,
		   v.last_late_age as lastapp_la
	    FROM $ATTRS_TABLE as v
	    WHERE orig_no in ($ID1, $ID2)";
    
    %APP = hash_by_taxon($dbh, $sql);
    
    $sql = "SELECT v.orig_no, v.pubyr, v.attribution
	    FROM $ATTRS_TABLE as v
	    WHERE orig_no in ($ID1, $ID2)";
    
    %ATTR = hash_by_taxon($dbh, $sql);
    
    $sql = "SELECT t.orig_no, pt.name as parent_name, pt.rank as parent_rank
	    FROM $TREE_TABLE as t join $TREE_TABLE as pt on pt.orig_no = t.senpar_no
	    WHERE t.orig_no in ($ID1, $ID2)";
    
    %PARENT = hash_by_taxon($dbh, $sql);
    
    $sql = "SELECT v.orig_no, v.taxon_size, v.extant_size, v.n_occs
	    FROM $ATTRS_TABLE as v
	    WHERE v.orig_no in ($ID1, $ID2)";
    
    %SIZE = hash_by_taxon($dbh, $sql);
    
    $sql = "SELECT ph.ints_no as orig_no, ph.kingdom_no, ph.kingdom, ph.phylum_no, ph.phylum,
		ph.class_no, ph.class, ph.order_no, ph.order, ph.family_no, ph.family
	    FROM $INTS_TABLE as ph
	    WHERE ph.ints_no in ($ID1, $ID2)";
    
    %PHYLO = hash_by_taxon($dbh, $sql);
    
    $sql = "SELECT pc.orig_no, pc.phylum_count, pc.class_count, pc.order_count,
		pc.family_count, pc.genus_count, pc.species_count
	    FROM $COUNTS_TABLE as pc
	    WHERE pc.orig_no in ($ID1, $ID2)";
    
    %COUNTS = hash_by_taxon($dbh, $sql);
    
    $sql = "SELECT a.taxon_no, a.created, a.modified FROM $AUTH_TABLE as a
	    JOIN $TREE_TABLE as t on a.taxon_no = t.spelling_no
	    WHERE t.orig_no in ($ID1, $ID2)";
    
    %CRMOD = hash_by_taxon($dbh, $sql);
    
    $sql = "SELECT v.orig_no, v.image_no
	    FROM $ATTRS_TABLE as v WHERE v.orig_no in ($ID1, $ID2)";
    
    %image_no = hash_field_by_taxon($dbh, 'image_no', $sql);

};

ok( !$@, 'got test data' ) or diag("message was: $@");

my $NAME3a = 'Loricatosaurus priscus';
my $ID3a = '142527';
my $NAME3b = 'Stegosaurus priscus';
my $ID3b = '56502';

my $NAME4a = 'Dacentrurinae';
my $ID4a = '142521';
my $NAME4b = 'Dacentruridae';
my $ID4b = '81342';

my $NAME5a = 'Lexovisaurus';
my $ID5a = '38812';
my $ACC5a = 'Stegosauridae';
my $ACCID5a = '38803';
my $NAME5b = 'Priodontosaurus';
my $ID5b = '105576';
my $ACC5b = $NAME5b;
my $ACCID5b = $ID5b;
my $NAME5c = 'Kentrurosaurus';
my $ID5c = '56503';
my $ACC5c = 'Kentrosaurus';
my $ACCID5c = '38811';

my $NAME6a = 'Hesperosaurus mjosi';
my $ID6a = '68159';
my $ID6b = '68158';
my $ID6c = '38814';
my $NAME6d = 'Hesperosaurus';
my $ID6d = '68158';
my $NAME6e = 'Stegopodus';
my $EX6 = 'Stegosauridae';

my $NAME7a = 'Ornithischia';

my $NAME8a = 'Allosaurus';
my $NAME8b = 'Stegosaurus';
my $NAME8c = 'Dinosauria';

my $NAME9a = 'Stegosaurus';
my $ID9a = '38814';
my $NAME9b = 'Felinae';
my $ID9b = '57432';

subtest 'list_taxa basic calls' => sub {
    
    my ($t1, @t1, $sth1, $t2, @t2, $count);
    
    eval {
	@t1 = $taxonomy->list_taxa('all_children', $ID1);
	$t1 = $taxonomy->list_taxa('all_children', $ID1, { return => 'listref' });
	$sth1 = $taxonomy->list_taxa('all_children', $ID1, { return => 'stmt', count => 1 });
	
	$count = $taxonomy->get_count;
	
	while ( $t2 = $sth1->fetchrow_hashref() )
	{
	    push @t2, $t2;
	}
    };
    
    ok( !$@, 'list_taxa basic' ) or diag("message was: $@");
    
    cmp_ok( scalar(@t1), '>', 60, "found children of '$NAME1'" );
    is( scalar(@$t1), scalar(@t1), "return => 'listref' gives same result set" );
    is( scalar(@t2), scalar(@t1), "return => 'stmt' gives same result set" );
    is( $count, scalar(@t1), "count => 1 returns proper rowcount" );
    
    my (%name1, %name2);
    
    foreach my $t (@t1)
    {
	$name1{$t->{taxon_name}} = 1;
    }
    
    foreach my $t (@t2)
    {
	$name2{$t->{taxon_name}} = 1;
    }
    
    ok( $name1{$NAME2}, "found '$NAME2' in default result list" );
    ok( $name2{$NAME2}, "found '$NAME2' in 'stmt' result list" );
    
    eval {
	@t1 = $taxonomy->list_taxa('bad_value', $ID1);
    };
    
    ok( $@, 'error with bad relationship' );
    
    eval {
	@t1 = $taxonomy->list_taxa('current', $ID1, $ID2);
    };
    
    ok( $@, 'error with bad third argument' );
    
    eval {
	@t1 = $taxonomy->list_taxa('current', $ID1, { status => 'valid', bad_option => 1 });
    };
    
    ok( $@, 'error with bad option name' );
};


subtest 'limit, offset, count' => sub {
    
    my (@t1, @t2, @t3, $sth1, $count, @w1, @w2, @w3);
    
    eval {
	@t1 = $taxonomy->list_taxa('all_children', $ID1, { limit => 10 });
	@w1 = $taxonomy->list_warnings;
	@t2 = $taxonomy->list_taxa('all_children', $ID1, { limit => 20, offset => 5 });
	@w2 = $taxonomy->list_warnings;
	$sth1 = $taxonomy->list_taxa('all_children', $ID1, { return => 'stmt', limit => 20, offset => 5 });
	@w3 = $taxonomy->list_warnings;
	
	$count = $taxonomy->get_count;
	
	while ( my $t = $sth1->fetchrow_hashref() )
	{
	    push @t3, $t;
	}
    };
    
    ok( !$@, "limit, offset" ) or diag("message was: $@");
    
    is( scalar(@t1), 10, "simple limit" );
    is( scalar(@t2), 20, "limit with offset" );
    cmp_ok( $t1[8]{orig_no}, '>', 0, "found taxon with limit" );
    is( $t1[8]{orig_no}, $t2[3]{orig_no}, "offset matches up" );
    is( $t1[8]{orig_no}, $t3[3]{orig_no}, "offset matches with return => 'stmt'" );
    is( scalar(@t3), scalar(@t2), "count matches with return => 'stmt'" );
    
    is( scalar(@w1), 0, "no warnings for correct call with 'limit'" );
    is( scalar(@w2), 0, "no warnings for correct call with 'offset'" );
    is( scalar(@w3), 0, "no warnings for correct call with 'stmt', 'limit', 'offset'" );
    
    eval {
	@t1 = $taxonomy->list_taxa('all_children', $ID1);
	@w1 = $taxonomy->list_warnings;
	@t2 = $taxonomy->list_taxa('all_children', $ID1, { limit => 'foo' });
	@w2 = $taxonomy->list_warnings;
	@t3 = $taxonomy->list_taxa('all_children', $ID1, { limit => 'all' });
	@w3 = $taxonomy->list_warnings;
    };
    
    ok( !$@, "non-numeric limits" ) or diag("message was: $@");
    
    is( scalar(@w1), 0, "no warnings for correct call with no options" );
    is( scalar(@t2), 0, "no taxa with bad limit" );
    is( scalar(@w2), 1, "one warning for bad limit" );
    is( scalar(@t3), scalar(@t1), "proper result set with limit 'all'" );
    is( scalar(@w3), 0, "no warnings for limit 'all'" );
};


subtest 'rel: exact and current' => sub {
    
    my (@t1, @t2, %name1, %name2);
    
    eval {
	@t1 = $taxonomy->list_taxa('exact', [$ID3a, $ID3b, $ID4a, $ID4b]);
	@t2 = $taxonomy->list_taxa('current', [$ID3a, $ID3b, $ID4a, $ID4b]);
    };
    
    ok( !$@, "list_taxa 'exact' and 'current'" ) or diag("message was: $@");
    
    foreach my $t (@t1)
    {
	$name1{$t->{taxon_name}} = 1;
    }
    
    foreach my $t (@t2)
    {
	$name2{$t->{taxon_name}} = 1;
    }
    
    ok( $name1{$NAME3a} && $name1{$NAME3b} && $name1{$NAME4a} && $name1{$NAME4b},
	"found exact names with 'exact'" );
    ok( $name2{$NAME3a} && $name2{$NAME4a},
	"found current names with 'current'" );
    ok( !$name2{$NAME3b} && !$name2{$NAME4b},
	"skipped non-current names with 'current'" );

};


subtest 'rel: variants' => sub {

    my (@t1, @t2, @t3, %orig_no, %taxon_no, %rank);
    
    eval {
	@t1 = $taxonomy->list_taxa('variants', $ID1);
	@t2 = $taxonomy->list_taxa('exact', $ID1, { all_variants => 1 });
	@t3 = $taxonomy->list_taxa('current', $ID1, { all_variants => 1 });
    };
    
    ok( !$@, "list_taxa: variants" ) or diag("message was: $@");
    
    my $current_count = 0;
    
    foreach my $t (@t1)
    {
	$orig_no{$t->{orig_no}} = 1;
	$taxon_no{$t->{taxon_no}} = 1;
	$rank{$t->{taxon_rank}} = 1;
	$current_count++ if $t->{is_current};
    }
    
    is( scalar(keys %orig_no), 1, 'one orig_no value' );
    cmp_ok( scalar(keys %taxon_no), '>', 3, 'more than three taxon_no values' );
    cmp_ok( scalar(keys %rank), '>', 3, 'more than three taxon_rank values' );
    
    is( $t1[0]{is_current}, 1, 'first variant is current' );
    is( $current_count, 1, 'only one variant is current' );
    
    is( scalar(@t2), scalar(@t1), "exact with all_variants" );
    is( scalar(@t3), scalar(@t1), "current with all_variants" );
};


subtest 'rel: accepted, senior, parent, senpar' => sub {
    
    my (@t1, @t2, @t3, %name1, %name2, %name3);
    
    eval {
	@t1 = $taxonomy->list_taxa('accepted', [$ID5a, $ID5b, $ID5c]);
	@t2 = $taxonomy->list_taxa('senior', [$ID5a, $ID5b, $ID5c]);
	@t3 = $taxonomy->list_taxa('parent', [$ID5a, $ID5b, $ID5c]);
    };
    
    ok( !$@, "list_taxa: 'accepted', 'senior', 'parent'" ) or diag("message was: $@");
    
    foreach my $t (@t1)
    {
	$name1{$t->{taxon_name}} = 1;
    }
    
    foreach my $t (@t2)
    {
	$name2{$t->{taxon_name}} = 1;
    }

    foreach my $t (@t3)
    {
	$name3{$t->{taxon_name}} = 1;
    }

    ok( $name1{$ACC5a} && $name1{$ACC5b} && $name1{$ACC5c}, "found accepted taxa" );
    is( scalar(keys %name1), 3, "accepted taxa count" );
    
    ok( $name2{$NAME5a} && $name2{$ACC5b} && $name2{$ACC5c}, "found senior taxa" );
    is( scalar(keys %name2), 3, "senior taxa count" );
    
    ok( $name3{$ACC5a}, "found parent taxa" );
    is( scalar(keys %name3), 1, "parent taxa count" );
    
    my ($t1, $t2, $t3);
    
    eval {
	($t1) = $taxonomy->list_taxa('senpar', $ID6a);
	($t2) = $taxonomy->list_taxa('parent', $ID6a);
	($t3) = $taxonomy->list_taxa('accepted', $t2);
    };
    
    ok( !$@, "list_taxa: 'accepted', 'parent', 'senpar'" ) or diag("message was: $@");
    
    is( $t1->{orig_no}, $ID6c, "senpar orig_no" );
    is( $t2->{orig_no}, $ID6b, "parent orig_no" );
    cmp_ok( $t1->{orig_no}, 'ne', $t2->{orig_no}, "senpar <> parent" );
    is( $t3->{orig_no}, $t1->{orig_no}, "senpar = parent + accepted" );
};


subtest 'rel: children, synonyms' => sub {
    
    my (@t1, @t2, @t3, %synonym, %parent_all, %parent_imm);
    
    eval {
	@t1 = $taxonomy->list_taxa('synonyms', $ID6d);
	@t2 = $taxonomy->list_taxa('children', $ID6d);
	@t3 = $taxonomy->list_taxa('children', $ID6d, { immediate => 1 });
    };
    
    ok( !$@, "list_taxa: 'synonyms', 'children'" ) or diag("message was: $@");
    
    my $senior_count = 0;
    
    foreach my $t (@t1)
    {
	$synonym{$t->{orig_no}} = 1;
	$senior_count++ if $t->{is_senior};
    }
    
    foreach my $t (@t2)
    {
	$parent_all{$t->{parent_no}} = 1;
    }
    
    foreach my $t (@t3)
    {
	$parent_imm{$t->{parent_no}} = 1;
    }
    
    my ($parent_check) = keys %parent_imm;
    
    is( $t1[0]{is_senior}, 1, 'first synonym is senior' );
    is( $senior_count, 1, 'only one synonym is senior' );
    cmp_ok( scalar(@t1), '>', 1, 'found multiple synonyms' );
    cmp_ok( scalar(@t2), '>', 1, 'found multiple children' );
    cmp_ok( scalar(@t3), '<', scalar(@t2), 'fewer immediate children' );
    
    is( $parent_check, $ID6d, 'immediate children have proper parent' );
    is( scalar(keys %parent_imm), 1, 'only one parent for immediate children' );
    
    my $bad_parent;
    
    foreach my $p (keys %parent_all)
    {
	$bad_parent = 1 unless $synonym{$p};
    }
    
    ok( !$bad_parent, 'no bad parents among all children' );
};


subtest 'rel: all_children' => sub {
    
    my (@t1, @t2, @t3, @t4, $st, %name1);
    
    eval {
	@t1 = $taxonomy->list_taxa('all_children', $ID1);
	@t2 = $taxonomy->list_taxa('all_children', $ID6d, { fields => 'RANGE' });
	@t3 = $taxonomy->list_taxa('all_children', $ID6d, { immediate => 1, fields => 'RANGE' });
	($st) = $taxonomy->list_taxa('senior', $ID6d);
	@t4 = $taxonomy->list_taxa('all_children', $ID1, { depth => 2 });
    };
    
    ok( !$@, "list_taxa: 'all_children'" ) or diag("message was: $@");
    
    foreach my $t (@t1)
    {
	$name1{$t->{taxon_name}} = 1;
    }
    
    ok( $name1{$NAME6a}, "found one of the children" );
    
    my $lft = $t2[0]{lft};
    my $rgt = $t2[0]{rgt};
    my $range2 = 0;
    
    foreach my $t (@t2)
    {
	$range2++ if $t->{lft} < $lft || $t->{lft} > $rgt;
    }
    
    ok( $lft, "found 'lft'" );
    ok( $rgt, "found 'rgt'" );
    ok( !$range2, "no non-immediate children outside of range" );
    ok( $t2[0]{orig_no}, "found all children base" );
    is( $t2[0]{orig_no}, $st->{orig_no}, "all children base = senior synonym" );
    
    $lft = $t3[0]{lft};
    $rgt = $t3[0]{rgt};
    my $range3 = 0;
    
    foreach my $t (@t3)
    {
	$range3++ if $t->{lft} < $lft || $t->{lft} > $rgt;
    }
    
    ok( $lft, "found 'lft' immediate" );
    ok( $rgt, "found 'rgt' immediate" );
    ok( !$range3, "no immediate children outside of range" );
    ok( $t3[0]{orig_no}, "found immediate children base" );
    is( $t3[0]{orig_no}, $ID6d, "immediate children base = orig" );
    
    cmp_ok( scalar(@t4), '>', 0, "found results with depth => 2" );
    cmp_ok( scalar(@t4), '<', scalar(@t1), "smaller result set with depth => 2" );
    
    # Now test exclusions
    
    my (@t4, @t5, @w4, %name2);
    
    eval {
	@t4 = $taxonomy->resolve_names("$NAME1^$EX6");
	@w4 = $taxonomy->list_warnings;
	@t5 = $taxonomy->list_taxa('all_children', \@t4);
    };
    
    ok( !$@, "list_taxa: 'all_children' with exclusions" ) 
	or diag("message was: $@");
    
    is( scalar(@w4), 0, "no warnings with exclusion" );
    
    foreach my $t (@t5)
    {
	$name2{$t->{taxon_name}} = 1;
    }
    
    ok( $name2{$NAME6e}, "found '$NAME6e'" );
    ok( !$name2{$NAME6a}, "did not find '$NAME6a'" );
    cmp_ok( scalar(@t5), '<', scalar(@t1), "found fewer children with exclusion" );
};


subtest 'rel: all_parents' => sub {
    
    my (@t1, @t2);
    
    eval {
	@t1 = $taxonomy->list_taxa('all_parents', $ID1);
	@t2 = $taxonomy->list_taxa('all_parents', $ID1, { min_rank => 13, max_rank => 21 });
    };
    
    ok( !$@, "list_taxa: 'all_parents'" ) or diag("message was: $@");
    
    is( $t1[0]{taxon_name}, 'Life', "found base 'Life'" );
    is( $t2[0]{taxon_name}, 'Chordata', "found restricted base 'Chordata'" );
    is( $t1[-1]{taxon_name}, $NAME1, "found root taxon" );
    is( $t2[-1]{taxon_name}, $NAME7a, "found restricted root taxon" );
};


subtest 'rel: common' => sub {
    
    my (@t1, @t2, @w1, @w2);
    
    eval {
	@t1 = $taxonomy->resolve_names([$NAME8a, $NAME8b]);
	@w1 = $taxonomy->list_warnings;
	@t2 = $taxonomy->list_taxa('common', \@t1);
	@w2 = $taxonomy->list_warnings;
    };
    
    ok( !$@, "list_taxa: 'common'" ) or diag("message was: $@");
    
    is( scalar(@w1), 0, "no warnings from resolve_names" );
    is( scalar(@w2), 0, "no warnings from list_taxa" );
    is( scalar(@t2), 1, "exactly one common taxon" );
    is( $t2[0]{taxon_name}, $NAME8c, "found common ancestor '$NAME8c'" );
};


subtest 'rel: all_taxa' => sub {

    my ($sth, $taxon_count, @t1, @t2);
    my $LIMIT = 50;
    
    eval {
	$sth = $taxonomy->list_taxa('all_taxa', undef, { return => 'stmt', count => 1 });
	$taxon_count = $taxonomy->get_count;
	
	foreach (1..10)
	{
	    push @t1, $sth->fetchrow_hashref();
	}
	
	@t2 = $taxonomy->list_taxa('all_taxa', undef, { limit => $LIMIT });
    };
    
    ok( !$@, "list_taxa: 'all_taxa'" ) or diag("message was: $@");
    is( $t1[0]{taxon_name}, 'Life', "first taxon is 'Life'" );
    cmp_ok( $taxon_count, '>', '200000', "found all taxa" );
    is( scalar(@t2), $LIMIT, "found $LIMIT taxa using parameter 'limit'" );
};


subtest 'fields' => sub {
    
    my (@t1t, @t1a, @t2t, @t2a);
    
    eval {
	@t1t = $taxonomy->list_taxa('current', $ID1, { fields => ['SIMPLE', 'ATTR'] });
	@t1a = $taxonomy->list_taxa('variants', $ID1, { fields => ['SIMPLE', 'ATTR'] });
	@t2t = $taxonomy->list_taxa('all_children', $ID1, { fields => 'DATA, image_no' });
	@t2a = $taxonomy->list_taxa('all_children', $ID1, { fields => 'DATA, image_no',
								    all_variants => 1 });
    };
    
    ok( !$@, "list_taxa: option 'fields' result OK" ) or diag("message was: $@");
    
    my ($test1t, $test1a, $test1b, $test2t, $test2a);
    
    foreach my $t (@t1t)
    {
	my $tn = $t->{taxon_no};
	cmp_deeply( $t, superhashof($SIMPLE_T{$tn}), "current SIMPLE $tn" );
    }
    
    foreach my $t (@t1a)
    {
	my $tn = $t->{taxon_no};
	cmp_deeply( $t, superhashof($SIMPLE_A{$tn}), "variants SIMPLE $tn" );
    }
    
    my ($d_count, $a_count);
    
    foreach my $t (@t2t)
    {
	my $tn = $t->{taxon_no};
	cmp_deeply( $t, superhashof($DATA_T{$tn}), "t DATA $tn" )
	    if $DATA_T{$tn};
	$d_count++ if $DATA_T{$tn};
    }
    
    cmp_ok( $d_count || 0, '>', 0, "found t DATA" );
    
    foreach my $t (@t2a)
    {
	my $tn = $t->{taxon_no};
	cmp_deeply( $t, superhashof($DATA_A{$tn}), "a DATA $tn" )
	    if $DATA_A{$tn};
	$a_count++ if $DATA_A{$tn};
    }
    
    cmp_ok( $a_count || 0, '>', 0, "found a DATA" );
    
    my ($img_count);
    
    foreach my $t (@t2a)
    {
	my $tn = $t->{taxon_no};
	is( $t->{image_no}, $image_no{$tn}, "t image_no $tn" )
	    if defined $image_no{$tn};
	$img_count++ if defined $image_no{$tn};
    }
    
    cmp_ok( $img_count || 0, '>', 0, "found t image_no" );
    
    my (@t, $taxon_count);
    
    eval {
	@t = $taxonomy->list_taxa('synonyms', $ID1, { fields => 'SIMPLE,, LINK,,APP,,ATTR,, PARENT,,SIZE ,,PHYLO,,COUNTS,,CRMOD, ,,,' });
	$taxon_count = 0;
    };
    ok( !$@, "list_taxa: more fields result OK" ) or diag("message was: $@");
    
    foreach my $t (@t)
    {
	my $tn = $t->{orig_no};
	
	if ( $ATTR{$tn} )
	{
	    cmp_deeply( $t, superhashof($LINK{$tn}), "fields LINK $tn" );
	    cmp_deeply( $t, superhashof($APP{$tn}), "fields APP $tn" );
	    cmp_deeply( $t, superhashof($ATTR{$tn}), "fields ATTR $tn" );
	    cmp_deeply( $t, superhashof($PARENT{$tn}), "fields PARENT $tn" );
	    cmp_deeply( $t, superhashof($SIZE{$tn}), "fields SIZE $tn" );
	    cmp_deeply( $t, superhashof($PHYLO{$tn}), "fields PHYLO $tn" );
	    cmp_deeply( $t, superhashof($COUNTS{$tn}), "fields COUNT $tn" );
	    cmp_deeply( $t, superhashof($CRMOD{$tn}), "fields CRMOD $tn" );
	    $taxon_count++;
	}
    }
    
    cmp_ok( $taxon_count, '>', 0, "found taxa with other fields" );
    
    my (@bad);
    
    eval {
	@bad = $taxonomy->list_taxa('variants', $ID1, { fields => ['BAD'] });
    };
    
    ok( $@, 'error with bad field specifier' );
    
    eval {
	@bad = $taxonomy->list_related_tax('variants', $ID1, { fields => '' });
    };
    
    ok( $@, 'error with empty field specifier' );
};


subtest 'crmod' => sub {

    my (@t1, @t2, @t3, @t4, @t5);
    
    eval {
	@t1 = $taxonomy->list_taxa('all_children', $ID1, { all_variants => 1, fields => 'SIMPLE,CRMOD' });
	@t2 = $taxonomy->list_taxa('all_children', $ID1, { all_variants => 1, min_created => '2010-06-18' });
	@t3 = $taxonomy->list_taxa('all_children', $ID1, { all_variants => 1, max_created => '2010-06-18' });
    };
    
    unless ( ok( !$@, 'eval OK' ) )
    {
	diag("message was: $@");
	return;
    }
    
    cmp_ok( scalar(@t2), '>', 0, "found results with min_created" );
    cmp_ok( scalar(@t3), '>', 0, "found results with max_created" );
    cmp_ok( scalar(@t2) + scalar(@t3), '==', scalar(@t1), "min_created and max_created are complements" );
    
    # select the modification date that comes soonest after 2010-01-01
    
    my $test_date;
    
    foreach my $t (@t1)
    {
	next unless $t->{modified} ge '2014';
	$test_date = $t->{modified} if !defined $test_date || $t->{modified} lt $test_date;
    }
    
    ok( $test_date =~ qr{\d\d\d\d-\d\d-\d\d}, "found an appropriate modification date" ) || return;
    
    # make sure that min_op_modified and max_op_modified properly partition
    # the result set
    
    eval {
	@t4 = $taxonomy->list_taxa('all_children', $ID1, { all_variants => 1, min_modified => $test_date });
	@t5 = $taxonomy->list_taxa('all_children', $ID1, { all_variants => 1, max_modified => $test_date });
    };

    unless ( ok( !$@, 'eval 2 OK' ) )
    {
	diag("message was: $@");
	return;
    }
    
    cmp_ok( scalar(@t4), '>', 0, "found results with min_modified" );
    cmp_ok( scalar(@t5), '>', 0, "found results with max_modified" );
    cmp_ok( scalar(@t4) + scalar(@t5), '==', scalar(@t1), "min_modified and max_modified are complements" );
};


subtest 'rank, extant, status' => sub {
    
    my (@t0, @t1, @t2, @t3, @t3a, @t4, @t5, @t6, @t7);
    
    eval {
	@t0 = $taxonomy->list_taxa('all_children', $ID1);
	@t1 = $taxonomy->list_taxa('all_children', $ID1, { min_rank => 5, max_rank => 8 });
	@t2 = $taxonomy->list_taxa('all_children', $ID1, { status => 'invalid' });
	@t3 = $taxonomy->list_taxa('all_children', $ID1, { min_rank => 5, status => 'invalid' });
	@t3a = $taxonomy->list_taxa('all_children', $ID1, { max_rank => 4, status => 'invalid' });
	@t4 = $taxonomy->list_taxa('all_children', $ID1, { status => 'valid' });
	@t5 = $taxonomy->list_taxa('all_children', $ID1, { status => 'junior' });
	@t6 = $taxonomy->list_taxa('all_children', $ID1, { status => 'senior' });
	@t7 = $taxonomy->list_taxa('all_children', $ID1, { status => 'all' });
    };
    
    ok( !$@, "list_taxa: rank, status" ) or diag("message was: $@");
    
    ok( scalar(@t0), "not empty with no filters" );
    ok( scalar(@t1), "not empty with rank filter" );
    ok( scalar(@t2), "not empty with status invalid" );
    ok( scalar(@t3), "not empty with rank and status filters" );
    ok( scalar(@t3a), "not empty with rank and status filters 2" );
    ok( scalar(@t4), "not empty with status valid" );
    ok( scalar(@t5), "not empty with status junior" );
    ok( scalar(@t6), "not empty with status senior" );
    is( scalar(@t7), scalar(@t0), "status all gives same result as no status filter" );
    
    cmp_ok( scalar(@t1), '<', scalar(@t0), "fewer taxa with rank filter" );
    cmp_ok( scalar(@t2), '<', scalar(@t0), "fewer taxa with status filter" );
    is( scalar(@t3) + scalar(@t3a), scalar(@t2), "disjoint sets with rank filter" );
    is( scalar(@t2) + scalar(@t4), scalar(@t0), "disjoint sets with status invalid/valid" );
    is( scalar(@t5) + scalar(@t6), scalar(@t4), "disjoint sets with status junior/senior" );
    
    my ($min1, $max1, %status2, %status4);
    
    my $NOM = 'nomen dubium';
    my $BEL = 'belongs to';
    my $SUB = 'subjective synonym of';
    
    foreach my $t (@t1)
    {
	$max1 = $t->{taxon_rank} if !defined $max1 || $t->{taxon_rank} > $max1;
	$min1 = $t->{taxon_rank} if !defined $min1 || $t->{taxon_rank} < $min1;
    }
    
    is( $max1, 8, "max rank with restriction" );
    is( $min1, 5, "min rank with restriction" );
    
    foreach my $t (@t2)
    {
	$status2{$t->{status}} = 1;
    }
    
    ok( $status2{$NOM}, "found status '$NOM' with 'invalid'" );
    ok( !$status2{$BEL}, "no status '$BEL' with 'invalid'" );
    ok( !$status2{$SUB}, "no status '$SUB' with 'invalid'" );
    
    foreach my $t (@t4)
    {
	$status4{$t->{status}} = 1;
    }
    
    ok( $status4{$BEL}, "found status '$BEL' with 'valid'" );
    ok( $status4{$SUB}, "found status '$SUB' with 'valid'" );
    ok( !$status4{$NOM}, "no status '$NOM' with 'valid'" );
};


subtest 'order' => sub {
    
    my (@t1a, @t1b, @t2a, @t2b, @t2c);
    my (@t3a, @t3b, @t4a, @t4b, @t5a, @t5b, @t6a, @t6b);
    
    eval {
	@t1a = $taxonomy->list_taxa('all_children', $ID1, { order => 'hierarchy', fields => 'RANGE' });
	@t1b = $taxonomy->list_taxa('all_children', $ID1, { order => 'hierarchy.desc', fields => 'RANGE' });

	@t2a = $taxonomy->list_taxa('synonyms', $ID9a, { order => 'name' });
	@t2b = $taxonomy->list_taxa('synonyms', $ID9a, { order => 'name.asc' });
	@t2c = $taxonomy->list_taxa('synonyms', $ID9a, { order => 'name.desc' });

	@t3a = $taxonomy->list_taxa('all_children', $ID1, { order => 'n_occs', fields => 'DATA' });
	@t3b = $taxonomy->list_taxa('all_children', $ID1, { order => 'n_occs.asc', fields => 'DATA' });
	
	@t4a = $taxonomy->list_taxa('all_children', $ID1, { order => 'firstapp', fields => 'SIMPLE,APP' });
	@t4b = $taxonomy->list_taxa('all_children', $ID1, { order => 'firstapp.asc', fields => 'SIMPLE,APP' });
	
	@t5a = $taxonomy->list_taxa('all_children', $ID1, { order => 'lastapp', fields => 'SIMPLE,APP' });
	@t5b = $taxonomy->list_taxa('all_children', $ID1, { order => 'lastapp.asc', fields => 'APP' });
	
	@t6a = $taxonomy->list_taxa('all_children', $ID1, { order => 'agespan', fields => 'SIMPLE,APP' });
	@t6b = $taxonomy->list_taxa('all_children', $ID1, { order => 'agespan.desc', fields => 'APP' });
    };
    
    ok( !$@, "list_taxa: order exec OK" ) or diag("message was: $@");
    
    check_order($taxonomy, \@t1a, 'lft', 'num', 'asc') or fail('order hierarchy default');
    check_order($taxonomy, \@t1b, 'lft', 'num', 'desc') or fail('order hierarchy desc');

    check_order($taxonomy, \@t2a, 'taxon_name', 'str', 'asc') or fail('order name default');
    check_order($taxonomy, \@t2b, 'taxon_name', 'str', 'asc') or fail('order name asc');
    check_order($taxonomy, \@t2c, 'taxon_name', 'str', 'desc') or fail('order name desc');
    
    check_order($taxonomy, \@t3a, 'n_occs', 'num', 'desc') or fail('order n_occs default');
    check_order($taxonomy, \@t3b, 'n_occs', 'num', 'asc') or fail('order n_occs asc');
    
    check_order($taxonomy, \@t4a, 'firstapp_ea', 'num', 'desc') or fail('order firstapp default');
    check_order($taxonomy, \@t4b, 'firstapp_ea', 'num', 'asc') or fail('order firstapp asc');
    
    check_order($taxonomy, \@t5a, 'lastapp_la', 'num', 'desc') or fail('order lastapp default');
    check_order($taxonomy, \@t5b, 'lastapp_la', 'num', 'asc') or fail('order lastapp asc');
    
    foreach my $t (@t6a, @t6b)
    {
	$t->{agespan} = int($t->{firstapp_ea} - $t->{lastapp_la} + 0.1)
	    if defined $t->{firstapp_ea};
    }
    
    check_order($taxonomy, \@t6a, 'agespan', 'num', 'asc') or fail('order agespan default');
    check_order($taxonomy, \@t6b, 'agespan', 'num', 'desc') or fail('order agespan desc');
    
    my (@t7a, @t7b, @t7c);
    my (@t8a, @t8b);
    my (@t9a, @t9b, @t10a, @t10b);
    
    eval {
	@t7a = $taxonomy->list_taxa('synonyms', $ID9a, { order => 'pubyr', fields => 'SIMPLE,ATTR' });
	@t7b = $taxonomy->list_taxa('synonyms', $ID9a, { order => 'pubyr.asc', fields => 'SIMPLE, ATTR'});
	@t7c = $taxonomy->list_taxa('synonyms', $ID9a, { order => 'pubyr.desc', fields => 'SIMPLE ,ATTR'});
	
	@t8a = $taxonomy->list_taxa('synonyms', $ID9a, { order => 'author', fields => 'SIMPLE,ATTR' });
	@t8b = $taxonomy->list_taxa('synonyms', $ID9a, { order => 'author.desc', fields => 'SIMPLE, ATTR'});
	
	@t9a = $taxonomy->list_taxa('synonyms', $ID9a, { order => 'created', fields => 'SIMPLE,CRMOD' });
	@t9b = $taxonomy->list_taxa('synonyms', $ID9a, { order => 'created.asc', fields => 'SIMPLE,CRMOD'});
	
	@t10a = $taxonomy->list_taxa('synonyms', $ID9a, { order => 'modified', fields => 'SIMPLE,CRMOD' });
	@t10b = $taxonomy->list_taxa('synonyms', $ID9a, { order => 'modified.asc', fields => 'SIMPLE,CRMOD'});
    };
    
    ok( !$@, "list_taxa: order 2 exec OK" ) or diag("message was: $@");
    
    check_order($taxonomy, \@t7a, 'pubyr', 'num', 'asc') or fail('order pubyr default');
    check_order($taxonomy, \@t7b, 'pubyr', 'num', 'asc') or fail('order pubyr asc');
    check_order($taxonomy, \@t7c, 'pubyr', 'num', 'desc') or fail('order pubyr desc');
    
    foreach my $t (@t8a, @t8b)
    {
	if ( defined $t->{attribution} )
	{
	    my $attr = $t->{attribution};
	    $attr =~ s/[()]//g;
	    $attr =~ s/\s+\d+$//;
	    $t->{author} = $attr;
	}
    }
    
    check_order($taxonomy, \@t8a, 'author', 'str', 'asc') or fail('order author default');
    check_order($taxonomy, \@t8b, 'author', 'str', 'desc') or fail('order author desc');
    
    check_order($taxonomy, \@t9a, 'created', 'str', 'desc') or fail('order created default');
    check_order($taxonomy, \@t9b, 'created', 'str', 'asc') or fail('order created asc');
    
    check_order($taxonomy, \@t10a, 'modified', 'str', 'desc') or fail('order modified default');
    check_order($taxonomy, \@t10b, 'modified', 'str', 'asc') or fail('order modified desc');
    
    my (@t11a, @t11b, @t12a, @t12b, @t13a, @t13b);
    
    eval {
	@t11a = $taxonomy->list_taxa('synonyms', $ID9b, { order => 'size', fields => 'SIMPLE,SIZE' });
	@t11b = $taxonomy->list_taxa('synonyms', $ID9b, { order => 'size.asc', fields => 'SIMPLE,SIZE'});
	
	@t12a = $taxonomy->list_taxa('synonyms', $ID9b, { order => 'extsize', fields => 'SIMPLE,SIZE' });
	@t12b = $taxonomy->list_taxa('synonyms', $ID9b, { order => 'extsize.asc', fields => 'SIMPLE,SIZE'});
	
	@t13a = $taxonomy->list_taxa('synonyms', $ID9b, { order => 'extant', fields => 'DATA' });
	@t13b = $taxonomy->list_taxa('synonyms', $ID9b, { order => 'extant.asc', fields => 'DATA'});
    };
    
    ok( !$@, "list_taxa: order 3 exec OK" ) or diag("message was: $@");
    
    check_order($taxonomy, \@t11a, 'taxon_size', 'num', 'desc') or fail('order size default');
    check_order($taxonomy, \@t11b, 'taxon_size', 'num', 'asc') or fail('order size desc');

    check_order($taxonomy, \@t12a, 'extant_size', 'num', 'desc') or fail('order extsize default');
    check_order($taxonomy, \@t12b, 'extant_size', 'num', 'asc') or fail('order extsize desc');
    
    check_order($taxonomy, \@t13a, 'is_extant', 'num', 'desc') or fail('order extant default');
    check_order($taxonomy, \@t13b, 'is_extant', 'num', 'asc') or fail('order extant desc');
};


sub check_order {
    
    my ($taxonomy, $result, $field, $type, $dir) = @_;
    
    my $last;
    my $index = -1;
    
    foreach my $t ( @$result )
    {
	$index++;
	
	# Skip nulls
	
	next unless defined $t->{$field};
	
	# If this is the first value, save it for later comparison
	
	unless ( $last )
	{
	    $last = $t->{$field};
	    next;
	}
	
	# Otherwise, compare with the previous value.
	
	elsif ( $type eq 'num' )
	{
	    if ( $dir eq 'asc' )
	    {
		unless ( $t->{$field} >= $last )
		{
		    print STDERR "check_order: violated at ($index) orig_no = $t->{orig_no}";
		    return 0;
		}
	    }
	    
	    elsif ( $dir eq 'desc' )
	    {
		unless ( $t->{$field} <= $last )
		{
		    print STDERR "check_order: violated at ($index) orig_no = $t->{orig_no}";
		    return 0;
		}
	    }
	    
	    else
	    {
		croak "invalid dir '$dir'";
	    }
	    
	    $last = $t->{$field};
	}
	
	elsif ( $type eq 'str' )
	{
	    if ( $dir eq 'asc' )
	    {
		unless ( $t->{$field} ge $last )
		{
		    print STDERR "check_order: violated at ($index) orig_no = $t->{orig_no}";
		    return 0;
		}
	    }
	    
	    elsif ( $dir eq 'desc' )
	    {
		unless ( $t->{$field} le $last )
		{
		    print STDERR "check_order: violated at ($index) orig_no = $t->{orig_no}";
		    return 0;
		}
	    }
	    
	    else
	    {
		croak "invalid dir '$dir'";
	    }
	    
	    $last = $t->{$field};
	}
	
	else
	{
	    croak "invalid type '$type'";
	}
    }
    
    if ( defined $last )
    {
	return 1;
    }
    
    else
    {
	print STDERR "check_order: no results found";
	return 0;
    }
};


sub hash_by_taxon {
    
    my ($dbh, $sql) = @_;
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    my %hash;
    
    foreach my $t ( @$result )
    {
	if ( $t->{taxon_no} )
	{
	    $hash{$t->{taxon_no}} ||= $t;
	}
	
	elsif ( $t->{orig_no} )
	{
	    $hash{$t->{orig_no}} ||= $t;
	}
    }
    
    return %hash;
};


sub hash_field_by_taxon {

    my ($dbh, $field, $sql) = @_;
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    my %hash;
    
    foreach my $t ( @$result )
    {
	if ( $t->{taxon_no} )
	{
	    $hash{$t->{taxon_no}} ||= $t->{$field};
	}
	
	elsif ( $t->{orig_no} )
	{
	    $hash{$t->{orig_no}} ||= $t->{$field};
	}
    }
    
    return %hash;
};
