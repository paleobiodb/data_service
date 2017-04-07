# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test the /data1.2/taxa/refs and /data1.2/taxa/byrefs operations,
# including all of the numerous parameters.
# 

use strict;

use feature 'unicode_strings';
use feature 'fc';

use Test::Most tests => 11;

use lib 't';
use Tester;
use Test::Conditions;

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });



# First test the fields of a single reference.

subtest 'single taxon refs' => sub {
    
    my $NAME_1 = 'Felis';
    
    my ($r1j, $x) = $T->fetch_records("/taxa/refs.json?name=$NAME_1&ref_type=auth",
				      "single authority ref json");
    
    unless ( $r1j )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my ($t1j) = { 'oid' => '!extid(ref)',
		  'rtp' => 'A',
		  'ai1' => qr{^\w[.]$},
		  'al1' => qr{\w\w\w\w\w},
		  'pby' => qr{^\d\d\d\d$},
		  'pbt' => qr{\w\w\w\w\w},
		  'pgf' => '!pos_int',
		  'pgl' => '!pos_int',
		};
    
    $T->check_fields( $r1j, $t1j, "single authority ref json" );
    ok( !$x, "single authority ref found only one record" );
    
    my ($r1t) = $T->fetch_records("/taxa/refs.csv?name=$NAME_1&ref_type=auth",
				  "single authority ref txt");
    
    my ($t1t) = { 'reference_no' => '!pos_int',
		  'record_type' => 'ref',
		  'ref_type' => 'auth',
		  'author1init' => $r1j->{ai1},
		  'author1last' => $r1j->{al1},
		  'pubyr' => $r1j->{pby},
		  'pubtitle' => $r1j->{pbt},
		  'firstpage' => $r1j->{pgf},
		  'lastpage' => $r1j->{pgl},
		};
    
    $T->check_fields( $r1t, $t1t, "single authority ref txt" );
    
    my ($r1r) = $T->fetch_records("/taxa/refs.ris?name=$NAME_1&ref_type=auth",
				  "single authority ref ris");
    
    unless ( $r1r )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my ($t1r) = { 'ID' => '!extid(ref)',
		  'KW' => qr{auth = \d},
		  'AU' => qr"^$r1j->{al1},\s*$r1j->{ai1}$",
		  'PY' => qr{^\d\d\d\d///$},
		  'T2' => $r1j->{pbt},
		  'SP' => $r1j->{pgf} . '-' . $r1j->{pgl},
		};
    
    $T->check_fields( $r1r, $t1r, "single authority ref ris" );

    my ($m1) = $T->fetch_url("/taxa/refs.json?name=abc*", "invalid character",
			     { no_diag => 1 } );
    
    $T->ok_warning_like(qr/invalid.*character/, "invalid character gets proper warning");
    $T->ok_response_code("200", "invalid character gets 200 response");
    
    my ($m2) = $T->fetch_url("/taxa/refs.json?name=abc*&strict", "invalid character strict",
			     { no_diag => 1, no_check => 1 } );

    $T->ok_error_like(qr/invalid.*character/, "invalid character strict gets proper error");
    $T->ok_response_code("400", "invalid character strict gets 400 response" );    
};


# Then fetch references using match_name.  Check to see that this matches the
# list of valid taxa.

subtest 'match_name basic' => sub {
    
    my $NAME_1 = 'CANIS %';
    
    # fetch all of the references for matching taxa, and the taxa for each
    # reference, and also fetch the list of taxa themselves.
    
    my (@r1) = $T->fetch_records("/taxa/refs.json?match_name=$NAME_1&ref_type=auth", 
				 "match_name refs");
    
    my (@b1) = $T->fetch_records("/taxa/byref.json?match_name=$NAME_1&ref_type=auth",
				 "match_name byref");
    
    my (@t1) = $T->fetch_records("/taxa/list.json?match_name=$NAME_1", "match_name taxa");
    
    # Test that the set of taxa associated with the references matches the taxa list.
    
    my %ref_oid = $T->extract_values( \@r1, 'oid' );
    my %taxa_rid = $T->extract_values( \@t1, 'rid' );
    my %taxa_oid = $T->extract_values( \@t1, 'oid' );
    my %reftaxa_tid = $T->extract_values( \@b1, 'tid' );
    my %reftaxa_rid = $T->extract_values( \@b1, 'rid' );
    
    $T->cmp_sets_ok( \%ref_oid, '==', \%taxa_rid, "ref oids match taxa rids");
    $T->cmp_sets_ok( \%ref_oid, '==', \%reftaxa_rid, "ref oids match reftaxa rids" );
    $T->cmp_sets_ok( \%taxa_oid, '==', \%reftaxa_tid, "taxa oids match reftaxa oids" );

    my ($m1) = $T->fetch_url("/taxa/refs.json?match_name=abc*", "invalid character",
			     { no_diag => 1 } );
    
    $T->ok_warning_like(qr/invalid.*character/, "invalid character gets proper warning");
    $T->ok_response_code("200", "invalid character gets 200 response");
    
    my ($m2) = $T->fetch_url("/taxa/refs.json?match_name=abc*&strict", "invalid character strict",
			     { no_diag => 1, no_check => 1 } );

    $T->ok_error_like(qr/invalid.*character/, "invalid character strict gets proper error");
    $T->ok_response_code("400", "invalid character strict gets 400 response" );
};


# Then test references for a taxonomic subtree.  Make sure that all of the results have basic
# fields. 

subtest 'bad params name + id' => sub {

    my $NAME_1 = 'Felis';
    
    my ($m1) = $T->fetch_url("/taxa/refs.json?base_name=abc*", "invalid character",
			     { no_diag => 1 } );
    
    $T->ok_warning_like(qr/invalid.*character/, "invalid character gets proper warning");
    $T->ok_response_code("200", "invalid character gets 200 response");
    
    my ($m2) = $T->fetch_url("/taxa/refs.json?base_name=abc*&strict", "invalid character strict",
			     { no_diag => 1, no_check => 1 } );

    $T->ok_error_like(qr/invalid.*character/, "invalid character strict gets proper error");
    $T->ok_response_code("400", "invalid character strict gets 400 response" );
    
    my (@r3) = $T->fetch_records("/taxa/refs.json?id=txn:69296,foo&ref_type=auth", "invalid id + valid id",
				 { no_diag => 1 });
    
    $T->ok_response_code("200", "invalid id + valid id gets 200 response" );
    $T->ok_warning_like(qr/'foo'/, "invalid id + valid id gets proper warning" );
    
    cmp_ok( @r3, '==', 1, "invalid id + valid id found one ref" );
};


subtest 'subtree basic with output' => sub {
    
    my $NAME_1 = 'Canidae';
    
    my (@r1j) = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&ref_type=all&show=counts,both,comments",
				 "subtree refs json with counts,both,comments");
    
    my (@r1t) = $T->fetch_records("/taxa/refs.csv?base_name=$NAME_1&ref_type=all&show=counts,both,comments",
				  "subtree refs csv with counts,both,comments");
    
    my (@r1r) = $T->fetch_records("/taxa/refs.ris?base_name=$NAME_1&ref_type=all&show=counts,both,comments",
				  "subtree refs ris with counts,both,comments");
    
    my $tc = Test::Conditions->new;
    $tc->set_limit( 'ref' => 10, 'AU' => 10, 'formatted' => 10 );
    
    foreach my $i ( 0..$#r1j )
    {
	my $rj = $r1j[$i];
	my $rt = $r1t[$i];
	my $rr = $r1r[$i];
	
	$tc->flag('missing txt') unless $rt;
	$tc->flag('missing ris') unless $rr;
	next unless $rt && $rr;
	
	$tc->flag('oid', $rj->{oid}) unless $rj->{oid} =~ /^ref:\d+$/;
	$tc->flag('reference_no', $rt->{reference_no}) unless $rt->{reference_no} =~ /^\d+$/ &&
	    $rj->{oid} eq 'ref:' . $rt->{reference_no};
	$tc->flag('ID', $rr->{ID}) unless $rr->{ID} eq $rj->{oid};
	
	my $al1 = $rj->{al1};
	my $ai1 = $rj->{ai1}; my $ai1_1 = substr($ai1,0,1);
	my $tit = $rj->{tit} || $rj->{pbt};
	
	$tc->flag('al1', $rj->{oid}) unless $al1;
	$tc->flag('author1last', $rt->{reference_no}) unless $rt->{author1last} eq $al1;
	$tc->flag('AU', $rr->{ID}) unless index($rr->{AU}, "$al1,$ai1_1") != -1
	    || $al1 =~ /jr|sr|ii|iv/i;
	
	$tc->flag('ref', $rj->{oid}) unless $rj->{ref} =~ qr{$ai1_1[^,]*$al1};
	$tc->flag('ref_title', $rt->{reference_no}) if $rt->{ref_title} && $rj->{tit} &&
	    $rt->{ref_title} ne $rj->{tit};
	$tc->flag('pub_title', $rt->{reference_no}) if $rt->{pub_title} && $rj->{pbt} &&
	    $rt->{pub_title} ne $rj->{pbt};
	
	$tc->flag('formatted', $rt->{reference_no}) if $rt->{formatted} && $rj->{ref} &&
	    $rt->{formatted} ne $rj->{ref};
	
	my $pby = $rj->{pby};
	
	$tc->flag('pby', $rj->{oid}) unless $pby;
	$tc->flag('pubyr', $rt->{reference_no}) unless $rt->{pubyr} eq $pby;
	$tc->flag('PY', $rr->{ID}) unless substr($rr->{PY},0,4) eq $pby;

	$tc->flag('ntx', $rj->{oid}) unless defined $rj->{ntx};
	$tc->flag('n_taxa', $rt->{reference_no}) unless defined $rt->{n_taxa};
	$tc->flag('KW', $rr->{ID}) unless defined $rr->{KW};
	
	$tc->flag('ntx/n_taxa', $rj->{oid}) unless $rj->{ntx} eq $rt->{n_taxa};
	if ( $rr->{KW} && $rr->{KW} =~ /taxa = (\d+)/ )
	{
	    $tc->flag('ntx/taxa', $rj->{oid}) unless $1 eq $rj->{ntx};
	}
	else
	{
	    $tc->flag('ntx/taxa', $rj->{oid}) if defined $rj->{ntx} && $rj->{ntx} != 0;
	}
	
	$tc->flag('ncl', $rj->{oid}) unless defined $rj->{ncl};
	$tc->flag('n_class', $rt->{reference_no}) unless defined $rt->{n_class};
	
	$tc->flag('ncl/n_class', $rj->{oid}) unless $rj->{ncl} eq $rt->{n_class};
	if ( $rr->{KW} && $rr->{KW} =~ /(?<!un)class = (\d+)/ )
	{
	    $tc->flag('ncl/class', $rj->{oid}) unless $1 eq $rj->{ncl};
	}
	else
	{
	    $tc->flag('ncl/class', $rj->{oid}) if defined $rj->{ncl} && $rj->{ncl} != 0;
	}
    }
    
    $tc->ok_all("all records have proper output field values");
    
    my %ntx = $T->extract_values( \@r1j, 'ntx' );
    my %nau = $T->extract_values( \@r1j, 'nau' );
    my %nva = $T->extract_values( \@r1j, 'nva' );
    my %ncl = $T->extract_values( \@r1j, 'ncl' );
    my %nuc = $T->extract_values( \@r1j, 'nuc' );
    my %noc = $T->extract_values( \@r1j, 'noc' );
    my %nsp = $T->extract_values( \@r1j, 'nsp' );
    my %nco = $T->extract_values( \@r1j, 'nco' );
    
    cmp_ok( keys %ntx, '>', 5, "found at least five values for 'ntx'");
    cmp_ok( keys %nau, '>', 5, "found at least five values for 'nau'");
    cmp_ok( keys %nva, '>', 5, "found at least five values for 'nva'");
    cmp_ok( keys %ncl, '>', 5, "found at least five values for 'ncl'");
    cmp_ok( keys %nuc, '>', 5, "found at least five values for 'nuc'");
    cmp_ok( keys %noc, '>', 5, "found at least five values for 'noc'");
    cmp_ok( keys %nsp, '>', 5, "found at least five values for 'nsp'");
    cmp_ok( keys %nco, '>', 5, "found at least five values for 'nco'");
};


# Then test references for a taxonomic subtree.  We match up the lists of taxonomic names,
# occurrences, etc. and make sure that the various sets of reference IDs match.

subtest 'subtree comprehensive' => sub {

    my $NAME_1 = 'Canidae';
    
    # First, we fetch all of the differenct kinds of references individually, plus all together.
    # We extract the 'oid' values from each list, to get a set of reference identifiers.
    
    my (@r1auth) = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&ref_type=auth",
				     "subtree refs auth");
    
    my %rid_r1auth = $T->extract_values( \@r1auth, 'oid' );
    
    my (@r1var) = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&ref_type=var",
				    "subtree refs var");
    
    my %rid_r1var = $T->extract_values( \@r1var, 'oid' );
    
    my (@r1class) = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&ref_type=class",
				      "subtree refs class");
    
    my %rid_r1class = $T->extract_values( \@r1class, 'oid' );
    
    my (@r1taxonomy) = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&ref_type=taxonomy",
					 "subtree refs taxonomy");
    
    my %rid_r1taxonomy = $T->extract_values( \@r1taxonomy, 'oid' );
    
    my (@r1ops) = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&ref_type=ops",
				    "subtree refs ops");
    
    my %rid_r1ops = $T->extract_values( \@r1ops, 'oid' );
    
    my (@r1occs) = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&ref_type=occs",
				     "subtree refs occs");
    
    my %rid_r1occs = $T->extract_values( \@r1occs, 'oid' );
    
    my (@r1specs) = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&ref_type=specs",
				      "subtree refs specs");
    
    my %rid_r1specs = $T->extract_values( \@r1specs, 'oid' );
    
    my (@r1colls) = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&ref_type=colls",
				      "subtree refs colls");
    
    my %rid_r1colls = $T->extract_values( \@r1colls, 'oid' );
    
    my (@r1all) = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&ref_type=all",
				    "subtree refs all");
    
    my %rid_r1all = $T->extract_values( \@r1all, 'oid' );
    
    my %rid_r1combo = $T->fetch_record_values("/taxa/refs.json?base_name=$NAME_1&ref_type=auth,ops,specs",
					      'oid', "subtree combo");
    
    unless ( @r1auth && @r1class && @r1all )
    {
	diag("skipping rest of subtest");
	return;
    }
    
    # Then we fetch the corresponding lists of taxa, opinions, occurrences, specimens, and
    # collections.  We extract the 'rid' values from each list, so we can compare these sets with
    # the sets of 'oid' values generated above.
    
    my (@r2taxa) = $T->fetch_records("/taxa/list.json?base_name=$NAME_1");
    
    my %rid_r2taxa = $T->extract_values( \@r2taxa, 'rid' );
    
    my (@r2var) = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&variant=all");
    
    my %rid_r2var = $T->extract_values( \@r2var, 'rid' );
    
    my (@r2ops) = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&op_type=all");
    
    my %rid_r2ops = $T->extract_values( \@r2ops, 'rid' );
    
    my (@r2class) = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&op_type=class");
    
    my %rid_r2class = $T->extract_values( \@r2class, 'rid' );
    
    my (@r2occs) = $T->fetch_records("/occs/list.json?base_name=$NAME_1");
    
    my %rid_r2occs = $T->extract_values( \@r2occs, 'rid' );
    
    my (@r2specs) = $T->fetch_records("/specs/list.json?base_name=$NAME_1");
    
    my %rid_r2specs = $T->extract_values( \@r2specs, 'rid' );
    
    my (@r2colls) = $T->fetch_records("/colls/list.json?base_name=$NAME_1");
    
    my %rid_r2colls = $T->extract_values( \@r2colls, 'rid' );
    
    # Now compare the matching sets to see if they are equal.
    
    $T->cmp_sets_ok( \%rid_r1auth, '==', \%rid_r2taxa, "auth ref rids match taxa current variant rids" );
    $T->cmp_sets_ok( \%rid_r1var, '==', \%rid_r2var, "var ref rids match taxa all variant rids" );
    $T->cmp_sets_ok( \%rid_r1class, '==', \%rid_r2class, "class ref rids match class ops rids" );
    $T->cmp_sets_ok( \%rid_r1ops, '==', \%rid_r2ops, "ops ref rids matches all ops rids" );
    $T->cmp_sets_ok( \%rid_r1occs, '==', \%rid_r2occs, "occs ref rids matches occs rids" );
    $T->cmp_sets_ok( \%rid_r1specs, '==', \%rid_r2specs, "specs ref rids matches specs rids" );
    $T->cmp_sets_ok( \%rid_r1colls, '==', \%rid_r2colls, "colls ref rids matches colls rids" );
    
    # And check some combined sets.
    
    my (%rid_r2taxonomy) = (%rid_r2taxa, %rid_r2class);
    my (%rid_r2combo) = (%rid_r2taxa, %rid_r2ops, %rid_r2specs);
    
    $T->cmp_sets_ok( \%rid_r1taxonomy, '==', \%rid_r2taxonomy, "taxonomy ref rids match taxa + class rids" );
    $T->cmp_sets_ok( \%rid_r1combo, '==', \%rid_r2combo, "combo ref rids match combined rid sets" );

    # Check for a bad ref_type parameter value.

    my $NAME_2 = 'dascillidae';
    
    my ($m1) = $T->fetch_nocheck("/taxa/refs.json?base_name=$NAME_2&ref_type=class,foo",
				 "ref_type valid + invalid");

    $T->ok_response_code( "200", "ref_type valid + invalid gets 200 response code" );
    $T->ok_warning_like( qr/bad value 'foo'/, "ref_type valid + invalid gets proper warning message" );

    my ($m2) = $T->fetch_nocheck("/taxa/refs.csv?base_name=$NAME_2&ref_type=foo",
				 "ref_type invalid");

    $T->ok_response_code( "400", "ref_type invalid gets 400 response code" );
    $T->ok_error_like( qr/no valid reference type/i, "ref_type invalid gets proper error message" );
};


subtest 'taxa relationships' => sub {
    
    my $NAME_1 = 'Stegosaurus';

    my (@r1) = $T->fetch_records("/taxa/refs.json?name=$NAME_1&rel=synonyms&ref_type=class");
    my (%rid_r1) = $T->extract_values( \@r1, 'oid' );
    
    my (@o1) = $T->fetch_records("/taxa/opinions.json?name=$NAME_1&rel=synonyms&op_type=class");
    my (%rid_o1) = $T->extract_values( \@o1, 'rid' );

    $T->cmp_sets_ok( \%rid_o1, '==', \%rid_r1, "opinion rids matches ref oids for synonym list" );
    cmp_ok( @r1, '>=', 2, "found at least three refs for synonyms" );
    cmp_ok( @o1, '>=', 3, "found at least three opinions for synonyms" );
    
    my $NAME_2 = 'Ranella bufo';

    my (@r2) = $T->fetch_records("/taxa/refs.json?name=$NAME_2&rel=variants&ref_type=auth");
    my (%rid_r2) = $T->extract_values( \@r2, 'oid' );

    my (@t2) = $T->fetch_records("/taxa/list.json?name=$NAME_2&rel=variants");
    my (%rid_t2) = $T->extract_values( \@t2, 'rid' );

    $T->cmp_sets_ok( \%rid_t2, '==', \%rid_r2, "taxon rids matches ref oids for variants list" );
    cmp_ok( @t2, '>=', 6, "found at least six taxa for variants" );

    # Now check bad parameter values

    my ($m1) = $T->fetch_nocheck("/taxa/refs.json?base_name=$NAME_1&rel=foo",
				 "rel invalid");

    $T->ok_response_code( "400", "rel invalid gets 400 response code" );
    $T->ok_error_like( qr/bad value 'foo'/i, "rel invalid gets proper error message" );
};


subtest 'ref parameters' => sub {
    
    my $NAME_1 = 'Canidae';
    my $NAME_2 = 'Brachiopoda';
    my $TITLEWORD_1 = 'Pliocene';
    
    my @r1 = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1", "taxonomy refs from '$NAME_1'");
    
    unless ( @r1 )
    {
        diag("skipping remainder of subtest");
        return;
    }
    
    my $rid_list = join(',', map { $_->{oid} } @r1[0..2]);
    
    my @r2a = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&ref_id=$rid_list", "selected refs from '$NAME_1'");
    my @r2b = $T->fetch_records("/taxa/refs.json?base_name=$NAME_2&ref_id=$rid_list", "selected refs from '$NAME_2'",
				{ no_records_ok => 1 });
    
    cmp_ok( @r2a, '==', 3, "selected refs from '$NAME_1' found 3 refs" );
    cmp_ok( @r2b, '==', 0, "selected refs from '$NAME_2' found no refs" );
    
    my $mm = $T->fetch_nocheck("/taxa/refs.csv?base_name=$NAME_2&ref_id=foo", "bad ref_id");

    $T->ok_warning_like(qr/'ref_id'/, "bad ref_id got proper warning");
    $T->ok_no_records("bad ref_id found no refs");
    
    my %r1_auth1 = $T->count_values( \@r1, 'al1' );
    my $auth1_max = $T->find_max( \%r1_auth1 );
    my $auth1_re = qr{$auth1_max};
    
    my %r1_auth2 = $T->count_values( \@r1, 'al2' );
    my $auth2_max = $T->find_max( \%r1_auth2 );
    my $auth2_re = qr{$auth2_max};
    
    ok( $auth1_max && $auth2_max, "found a first author and a second author" );
    
    my ($auth3a, $auth3b);
    
    foreach my $r ( @r1 )
    {
	if ( $r->{al1} && $r->{al2} )
	{
	    $auth3a = $r->{al1};
	    $auth3b = $r->{al2};
	    last;
	}
    }
    
    ok( $auth3a && $auth3b, "found a ref with two authors" );
    
    my $tc = Test::Conditions->new;
    
    my @r3a = $T->fetch_records("/taxa/refs.csv?base_name=$NAME_1&ref_author=$auth2_max", 
				"ref author '$auth2_max'");
    my @r3b = $T->fetch_records("/taxa/refs.csv?base_name=$NAME_1&ref_primary=$auth2_max", 
				"ref primary '$auth2_max'", { no_records_ok => 1 });
    my @r3c = $T->fetch_records("/taxa/refs.csv?base_name=$NAME_1&ref_primary=$auth1_max", 
				"ref primary '$auth1_max'");
    my @r3d = $T->fetch_records("/taxa/refs.csv?base_name=$NAME_1&ref_author=$auth1_max,$auth2_max", 
				"ref author '$auth1_max,$auth2_max'");
    my @r3e = $T->fetch_records("/taxa/refs.csv?base_name=$NAME_1&ref_author=$auth1_max and $auth2_max",
				"ref author '$auth1_max and $auth2_max'", { no_records_ok => 1 });
    my @r3f = $T->fetch_records("/taxa/refs.csv?base_name=$NAME_1&ref_author=$auth3a and $auth3b",
				"ref author '$auth3a and $auth3b");
    
    cmp_ok( @r3b, '<', @r3a, "ref primary finds fewer records than ref author" );
    cmp_ok( @r3c, '>', @r3b, "ref primary auth1_max finds more records than ref primary auth2_max" );
    cmp_ok( @r3e, '<', @r3d, "ref author with 'and' finds fewer records than ref author with ','" );
    cmp_ok( @r3f, '<', @r1, "ref author with 'and' finds fewer records than all" );
    
    foreach my $r ( @r3a )
    {
	$tc->flag('ref author', $r->{reference_no}) unless $r->{author1last} && $r->{author1last} =~ $auth2_re || 
	    $r->{author2last} && $r->{author2last} =~ $auth2_re ||
		$r->{otherauthors} && $r->{otherauthors} =~ $auth2_re;
    }
    
    foreach my $r ( @r3c )
    {
	$tc->flag('ref primary', $r->{reference_no}) unless $r->{author1last} && $r->{author1last} =~ $auth1_re;
    }
    
    $tc->ok_all("ref author checks");
    
    my @r4a = $T->fetch_records("/taxa/refs.ris?base_name=$NAME_1&ref_pubyr=2000-", "ref pubyr '2000-'");
    my @r4b = $T->fetch_records("/taxa/refs.ris?base_name=$NAME_1&ref_pubyr=-1999", "ref pubyr '-1999'");
    
    cmp_ok( @r4a + @r4b, '==', @r1, "ref pubyr filters add up to total refs" );
    
    my %r1_pubtitle = $T->count_values( \@r1, 'pbt' );
    my $pubtitle_max = $T->find_max( \%r1_pubtitle );
    
    my %r1_pubtype = $T->count_values( \@r1, 'pty' );
    my ($pubtype1, $pubtype2) = grep { $_ } keys %r1_pubtype;
    
    ok( $pubtype1 && $pubtype2, "found two publication types" );
    
    my @r5a = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&pub_title=$pubtitle_max", "pub title");
    my @r5b = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&ref_title=%$TITLEWORD_1%", "ref title");
    my @r5c = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&pub_type=$pubtype1, $pubtype2", "pub type");
    
    foreach my $r ( @r5a )
    {
	$tc->flag('pub title', $r->{oid}) unless fc $r->{pbt} eq fc $pubtitle_max;
    }
    
    foreach my $r ( @r5b )
    {
	$tc->flag('ref title', $r->{oid}) unless $r->{tit} =~ /$TITLEWORD_1/;
    }
    
    foreach my $r ( @r5c )
    {
	$tc->flag('pub type', $r->{oid}) unless $r->{pty} =~ /$pubtype1|$pubtype2/;
    }
    
    $tc->ok_all("title and type checks");
    
    my %r1_doi = $T->count_values( \@r1, 'doi' );
    my ($doi1, $doi2) = grep { $_ } keys %r1_doi;
    
    ok( $doi1 && $doi2, "found two dois");
    
    my @r6a = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&ref_doi=%", "ref has any doi");
    my @r6b = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&ref_doi=$doi1,$doi2", "ref selected doi");
    
    foreach my $r ( @r6a )
    {
	$tc->flag('any doi', $r->{oid}) unless $r->{doi} && $r->{doi} ne '';
    }
    
    foreach my $r ( @r6b )
    {
	$tc->flag('selected doi', $r->{oid}) unless $r->{doi} &&
	    ( $r->{doi} eq $doi1 || $r->{doi} eq $doi2);
    }
    
    $tc->ok_all("doi checks")
};


subtest 'taxon parameters' => sub {

    my $NAME_1 = 'Dinosauria^Aves';
    my $NAME_1b = 'Dinosauria';
    my $NAME_1e = 'Aves';
    my $INT_1 = 'Paleogene';
    
    my @r1 = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&interval=$INT_1&ref_type=auth", "refs with exclusion and interval");
    my @t1 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&interval=$INT_1", "taxa with exclusion and interval");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my %r1_rid = $T->extract_values( \@r1, 'oid' );
    my %t1_rid = $T->extract_values( \@t1, 'rid' );
    
    $T->cmp_sets_ok( \%r1_rid, '==', \%t1_rid, "ref oids equal taxa rids with exclusion and interval" );
    
    my ($r1e) = $T->fetch_records("/taxa/single.json?name=$NAME_1e", "taxon exclusion single");
    
    my $EXCLUDE_1 = $r1e->{oid};
    
    my @r1a = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1b&exclude_id=$EXCLUDE_1&interval=$INT_1&ref_type=auth",
				"refs with exclude_id and interval");
    
    cmp_ok( @r1a, '==', @r1, "refs with exclude_id finds same as refs with exclude" );
    
    my $NAME_2 = 'Hypsirophus';
    
    my @r2a = $T->fetch_records("/taxa/refs.json?base_name=$NAME_2", "refs without immediate");
    my @r2b = $T->fetch_records("/taxa/refs.json?base_name=$NAME_2&immediate", "refs with immediate");
    
    cmp_ok( @r2b, '<', @r2a, "refs with immediate finds fewer refs" );
    
    my $NAME_3a = 'Cetacea';
    my $NAME_3b = 'whale';
    my $LANG_3b = 'EN';
    
    my %r3a = $T->fetch_record_values("/taxa/refs.json?base_name=$NAME_3a", 'oid', "refs '$NAME_3a'");
    my %r3b = $T->fetch_record_values("/taxa/refs.json?base_name=$NAME_3b&common=$LANG_3b", 'oid', "refs common '$NAME_3b'");
    
    $T->cmp_sets_ok( \%r3a, '==', \%r3b, "common name finds same refs as scientific name" );
    
    # test rank, extant

    my $NAME_4 = 'Felidae';

    my @r4 = $T->fetch_records("/taxa/byref.json?base_name=$NAME_4",
			       "byref base");
    
    my @r4a = $T->fetch_records("/taxa/byref.json?base_name=$NAME_4&rank=genus",
				"byref genera");
    
    my @r4b = $T->fetch_records("/taxa/byref.json?base_name=$NAME_4&rank=below_genus,above_genus",
				"byref not genera");

    cmp_ok( @r4a + @r4b, '==', @r4, "byref genera + byref not genera = byref base" );

    my @r4c = $T->fetch_records("/taxa/byref.json?base_name=$NAME_4&rank=genus&extant=no",
				"byref extinct genera");

    my @r4d = grep { not(defined $_->{ext} && $_->{ext} == 0) } @r4a;
    
    cmp_ok( @r4c + @r4d, '==', @r4a, "byref extinct genera + byref not extinct genera = byref genera" );
    
    # Make sure we actually get the correct refs
    
    my %rid_4c = $T->extract_values( \@r4c, 'rid' );
    my %rid_4x = $T->fetch_record_values("/taxa/refs.json?base_name=$NAME_4&rank=genus&extant=no",
					 'oid', "ref records from extinct genera");
    
    $T->cmp_sets_ok( \%rid_4c, '==', \%rid_4x, "byref extinct genera matches refs from extinct genera" );
    
    # test taxon status

    my @r5a = $T->fetch_records("/taxa/byref.json?base_name=$NAME_4&taxon_status=junior",
				"byref junior taxa");

    my @r5b = $T->fetch_records("/taxa/byref.json?base_name=$NAME_4&taxon_status=senior",
				"byref senior taxa");

    cmp_ok( @r5a + @r5b, '==', @r4, "byref junior taxa + byref senior taxa = byref base");

    # Now try some bad taxon parameter values

    my $NAME_5 = 'DASCILLIDAE';
    
    my $m1 = $T->fetch_nocheck("/taxa/refs.ris?base_name=$NAME_5&interval=foo", "bad interval");

    $T->ok_response_code( "400", "bad interval gets 400 response" );
    $T->ok_error_like( qr/unknown interval|no valid interval/i, "bad interval gets proper error message" );

    my $m2 = $T->fetch_nocheck("/taxa/refs.json?base_name=$NAME_5&exclude_id=foo", "bad exclude_id");

    $T->ok_response_code( "200", "bad exclude_id gets 200 response" );
    $T->ok_warning_like( qr/'exclude_id'/, "bad exclude_id gets proper warning message" );
    
    my $m3 = $T->fetch_nocheck("/taxa/refs.csv?base_name=$NAME_5&rank=foo", "bad rank");
    
    $T->ok_response_code( "400", "bad exclude_id gets 400 response" );
    $T->ok_error_like( qr/invalid.*rank/, "bad rank gets proper error message" );

    my $m4 = $T->fetch_nocheck("/taxa/refs.json?base_name=$NAME_5&common=foo", "bad common");

    $T->ok_response_code( "200", "bad common gets 200 response" );
    $T->ok_warning_like( qr/language code/, "bad common gets proper warning message" );
    $T->ok_no_records;
};


subtest 'bad param values' => sub {
    
    my ($m1) = $T->fetch_nocheck("/taxa/refs.json?ref_author=smith", "missing main parameter");
    
    $T->ok_response_code("400", "missing main parameter gets 400 response code");
    $T->ok_error_like(qr/all_records/, "missing main parameter gets proper error message");
};


subtest 'date filters' => sub {
    
    my $NAME_1 = 'Canidae';
    my $DATE_1 = '2010';

    my @r1 = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1", "date filter base");

    my @r2a = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&refs_created_before=$DATE_1&show=crmod",
				"created before");

    my @r2b = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&refs_created_after=$DATE_1&show=crmod",
				"created after");
    
    cmp_ok( @r2a + @r2b, '==', @r1, "created before + created after = base" );
    
    my $tc = Test::Conditions->new;

    foreach my $r ( @r2a )
    {
	$tc->flag('created_before', $r->{oid}) unless $r->{dcr} && $r->{dcr} lt $DATE_1;
    }

    foreach my $r ( @r2b )
    {
	$tc->flag('created_after', $r->{oid}) unless $r->{dcr} && $r->{dcr} ge $DATE_1;
    }

    $tc->ok_all("date filtered records have proper values for 'dcr'");

    my @r3a = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&refs_modified_before=$DATE_1&show=crmod",
				"created before");

    my @r3b = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&refs_modified_after=$DATE_1&show=crmod",
				"created after");
    
    cmp_ok( @r3a + @r3b, '==', @r1, "created before + created after = base" );

    foreach my $r ( @r3a )
    {
	$tc->flag('modified_before', $r->{oid}) unless $r->{dmd} && $r->{dmd} lt $DATE_1;
    }

    foreach my $r ( @r3b )
    {
	$tc->flag('modifed_after', $r->{oid}) unless $r->{dmd} && $r->{dmd} ge $DATE_1;
    }

    $tc->ok_all("ref date filtered records have proper values for 'dmd'");
    
    # Same check for taxa_created_before...
    
    my @r4a = $T->fetch_records("/taxa/byref.json?base_name=$NAME_1&taxa_created_before=$DATE_1&show=crmod",
				"byref taxa created before");
    my @r4b = $T->fetch_records("/taxa/byref.json?base_name=$NAME_1&taxa_created_after=$DATE_1&show=crmod",
				"byref taxa created after");
    
    my @r4c = $T->fetch_records("/taxa/byref.json?base_name=$NAME_1", "byref date filter base");
    
    cmp_ok( @r4a + @r4b, '==', @r4c, "byref taxa created before + created after = date filter base");
    
    foreach my $r ( @r4a )
    {
	$tc->flag('created_before', $r->{oid}) unless $r->{dcr} && $r->{dcr} lt $DATE_1;
    }

    foreach my $r ( @r4b )
    {
	$tc->flag('created_after', $r->{oid}) unless $r->{dcr} && $r->{dcr} ge $DATE_1;
    }

    $tc->ok_all("taxa date filtered records have proper values for 'dcr'");
};


subtest 'authent filters' => sub {

    my $NAME_1 = 'Felidae';
    
    my @t1 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&show=ent,entname");
    
    my %ati = $T->count_values( \@t1, 'ati' );
    my ($ati_max) = $T->find_max( \%ati );
    my ($ath_max);
    
    foreach my $t ( @t1 )
    {
	if ( $t->{ati} eq $ati_max )
	{
	    $ath_max = $t->{ath};
	    last;
	}
    }
    
    ok( $ath_max, "found ath_max" );
    
    diag( "   ati: $ati_max, ath: $ath_max");
    
    my @r2a = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&taxa_authorized_by=$ati_max",
				"taxa authorized by ati_max");
    
    my @r2b = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1&taxa_authorized_by=$ath_max",
				"taxa authorized by ath_max");
    
    my @r2c = $T->fetch_records("/taxa/refs.json?base_name=$NAME_1", "base refs");
    
    cmp_ok( @r2a, '==', @r2b, "authorized by: ati_max = ath_max" );
    cmp_ok( @r2a, '<', @r2c, "authorized by ati_max < base refs" );
    
    my @r3a = $T->fetch_records("/taxa/byref.json?base_name=$NAME_1&taxa_authorized_by=$ati_max",
				"byref taxa authorized by ati_max");
    
    my @r3b = $T->fetch_records("/taxa/byref.json?base_name=$NAME_1&taxa_authorized_by=!$ati_max",
				"byref taxa authorized by !ati_max");
    
    my @r3c = $T->fetch_records("/taxa/byref.json?base_name=$NAME_1", "base byref");
    
    cmp_ok( @r3a + @r3b, '==', @r3c, "byref authorized by + not authorized by = base" );
    
    my %rid_2c = $T->extract_values( \@r2c, 'oid' );
    my %rid_3c = $T->extract_values( \@r3c, 'rid' );
    
    $T->cmp_sets_ok( \%rid_2c, '==', \%rid_3c, "rids match between refs and byref" );
};
