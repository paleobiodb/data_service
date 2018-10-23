# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test the /data1.2/taxa/opinions operation,
# including all of the numerous parameters.
# 

use strict;
use feature 'unicode_strings';
use feature 'fc';

use Test::Most tests => 12;

use lib 't';

use Tester;
use Test::Conditions;

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });


# AAA

# First test opinions about a single taxon.

subtest 'single taxon classification' => sub {
    
    my $NAME_1 = 'Felis';
    
    # First test fetching the classification opinion for this taxon, both as json and as txt.  We
    # actually fetch all opinions for this taxon, but the classification opinion should be first.
    
    my ($t1j) = $T->fetch_records("/taxa/single.json?name=$NAME_1&show=parent",
				  "taxon by name for checking json");
    
    unless ( $t1j )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my $TID_1 = $t1j->{oid};
    
    my $c1j = { 'oid' => '!extid(opn)',
		'otp' => 'C',
		'rnk' => 5,
		'nam' => $t1j->{nam},
		'tid' => $t1j->{oid},
		'sta' => 'belongs to',
		'prl' => $t1j->{prl},
		'par' => $t1j->{par},
		'spl' => 'original spelling',
		'oat' => qr{\w\w\w},
		'opy' => qr{^\d\d\d\d$},
		'rid' => '!extid(ref)',
	      };
    
    my ($r1j, $uc1) = $T->fetch_records("/taxa/opinions.json?name=$NAME_1",
					"opinions by name single taxon json");
				  
    $T->check_fields($r1j, $c1j, "single classification json");
    ok( $uc1, "opinions by name json also found at least one non-classification opinion" );
    
    my $rid1;
    
    if ( $r1j->{rid} )
    {
	$r1j->{rid} =~ /(\d\d\d\d+)/;
	$rid1 = $1;
    }
    
    my ($t1t) = $T->fetch_records("/taxa/single.txt?name=$NAME_1&show=parent",
				  "taxon by by name for checking txt");
    
    my $c1t = { 'opinion_no' => '!pos_int',
		'record_type' => 'opn',
		'opinion_type' => 'class',
		'taxon_rank' => 'genus',
		'taxon_name' => $t1t->{taxon_name},
		'orig_no' => $t1t->{orig_no},
		'child_spelling_no' => $t1t->{orig_no},
		'status' => $r1j->{sta},
		'parent_name' => $t1t->{parent_name},
		'parent_no' => $t1t->{parent_no},
		'spelling_reason' => $r1j->{spl},
		'author' => $r1j->{oat},
		'pubyr' => $r1j->{opy},
		'reference_no' => $rid1,
	     };
    
    my ($r1t) = $T->fetch_records("/taxa/opinions.txt?name=$NAME_1",
				  "opinions by name txt");
    
    $T->check_fields($r1t, $c1t, "single classification txt");
    
    my ($r2j, $uc2) = $T->fetch_records("/taxa/opinions.json?id=$TID_1",
					"opinions by id single taxon json");
    
    is_deeply( $r1j, $r2j, "opinion by name and opinion by id match" );
    ok( $uc2, "opinions by id json also found at least one non-classification opinion" );
    
    # Now test fetching just the classification opinion about this taxon.
    
    my (@r3) = $T->fetch_records("/taxa/opinions.json?id=$TID_1&op_type=class", "single taxon classification by id");
    
    cmp_ok( @r3, '==', 1, "single taxon classification by id got one opinion" );
    is_deeply( $r3[0], $r1j, "classification matches previously fetched classification opinion" );
};


# Then fetch opinions using match_name.  Check to see that this matches the
# list of valid taxa.

subtest 'match_name basic' => sub {
    
    my $NAME_1 = 'CANIS %';
    
    # fetch all of the opinions for matching taxa, and also fetch the taxa
    # themselves. 
    
    my (@r1) = $T->fetch_records("/taxa/opinions.json?match_name=$NAME_1&op_type=class", 
				 "match_name opinions");
    
    my (@t1) = $T->fetch_records("/taxa/list.json?match_name=$NAME_1&variant=all", "match_name taxa");
    
    # cmp_ok( @r1, '==', @t1, "match_name opinion count = match_name taxon
    # count" );
    
    my %taxa_names = $T->extract_values( \@t1, 'nam' );
    my %acc_names = $T->extract_values( \@t1, 'acn' );
    
    # Test that the set of names for the opinions matches the taxa list.
    
    my %op_names = $T->extract_values( \@r1, 'nam' );
    
    my $tc = Test::Conditions->new;
    
    foreach my $k ( keys %op_names )
    {
	$tc->flag('not in list', $k) unless exists $taxa_names{$k} || exists $acc_names{$k};
    }
    
    $tc->ok_all("opinions taxa is subset of list of taxa");
};


# Then test opinions about a taxonomic subtree.  First, we just check the
# basic fields.

subtest 'subtree basic' => sub {

    my $NAME_1 = 'Canidae';
    
    # Fetch all of the opinions for this subtree, and also fetch all of the taxa.
    
    my (@r1) = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&op_type=all",
				 "subtree opinions basic");
    
    unless ( @r1 )
    {
	diag("skipping rest of subtest");
	return;
    }
    
    my %taxa_names = $T->fetch_record_values("/taxa/list.json?base_name=$NAME_1", 'nam',
					     "subtree taxa names");
    
    # Test that the set of names for the opinions matches the taxa list.
    
    my %op_names = $T->extract_values( \@r1, 'nam' );
    
    $T->cmp_sets_ok( \%op_names, '==', \%taxa_names, "opinions taxa matches list of taxa" );
    
    # Check that we got more than just the classification opinions.
    
    cmp_ok( @r1, '>', keys %taxa_names, "got more than just classification opinions" );
    
    my %op_types = $T->extract_values( \@r1, 'otp' );
    
    is_deeply( \%op_types, { C => 1, U => 1 }, "found proper opinion types" );
    
    my %op_status = $T->extract_values( \@r1, 'sta' );
    
    foreach my $t ( 'belongs to', 'subjective synonym of', 'objective synonym of',
		    'replaced by', 'invalid subgroup of', 'nomen dubium', 'nomen vanum' )
    {
	ok( $op_status{$t}, "found at least one opinion with status '$t'" );
    }
    
    my %op_spelling = $T->extract_values( \@r1, 'spl' );
    
    foreach my $s ( 'misspelling', 'original spelling' )
    {
	ok( $op_spelling{$s}, "found at least one opinion with spelling reason '$s'" );
    }
    
    my %first_op_type;
    my $tc = Test::Conditions->new;  
    
    foreach my $r ( @r1 )
    {
	$tc->flag('oid', $r->{nam}) unless $r->{oid} && $r->{oid} =~ /^opn:\d+$/;
	$tc->flag('otp', $r->{oid}) unless $r->{otp} && $r->{otp} =~ /^[CU]$/;
	$first_op_type{$r->{nam}} ||= $r->{otp};
	$tc->flag('c_first', $r->{oid}) unless $r->{nam} && $first_op_type{$r->{nam}} eq 'C';
	$tc->flag('rnk', $r->{oid}) unless $r->{rnk} && $r->{rnk} > 0;
	$tc->flag('tid', $r->{oid}) unless $r->{tid} && $r->{tid} =~ /^txn:\d+$/;
	$tc->flag('sta', $r->{oid}) unless $r->{sta};
	$tc->flag('spl', $r->{oid}) unless $r->{spl};
	$tc->flag('oat', $r->{oid}) unless $r->{oat};
	$tc->flag('opy', $r->{oid}) unless $r->{opy} && $r->{opy} > 0;
	$tc->flag('rid', $r->{oid}) unless $r->{rid} && $r->{rid} =~ /^ref:\d+$/;
    }
    
    $tc->ok_all("subtree opinions basic");
};


subtest 'subtree other output blocks' => sub {

    my $NAME_1 = 'Canidae';
    
    my @r1 = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&op_type=all&" .
			       "show=attr,refattr,ref,basis,ent,entname,crmod", "subtree opinions show");
    
    my $tc = Test::Conditions->new;
    $tc->limit_max(atp => 50);
    
    my $aut_diff;
    
    foreach my $r ( @r1 )
    {
	$tc->flag('att', $r->{oid}) unless $r->{att} && $r->{att} =~ /\w\w.*\d\d\d\d/;
	$tc->flag('bas', $r->{oid}) unless $r->{bas} && $r->{bas} =~ /stated|implied|second/;
	$tc->flag('atp', $r->{oid}) unless $r->{atp} && $r->{atp} =~ /\w\w.*\d\d\d\d/;
	$tc->flag('ref', $r->{oid}) unless $r->{ref};
	
	$aut_diff++ if $r->{oat} ne $r->{aut} || $r->{opy} ne $r->{pby};
	
	my $aut_check = $r->{aut}; $aut_check =~ s/\s+et al.//; $aut_check =~ s/ and /.*/;
	$aut_check .= ".*$r->{pby}";
	
	$tc->flag('aut_ref', $r->{oid}) unless $r->{ref} =~ /$aut_check/;
	
	$tc->flag('ati', $r->{oid}) unless $r->{ati} =~ /^prs:\d+$/;
	$tc->flag('eni', $r->{oid}) unless $r->{eni} =~ /^prs:\d+$/;
	$tc->flag('mdi', $r->{oid}) unless ! defined $r->{mdi} || $r->{mdi} =~ /^prs:\d+$/;
	
	$tc->flag('ath', $r->{oid}) unless $r->{ath} =~ /\w\w/ && $r->{ath} !~ /\d/;
	$tc->flag('ent', $r->{oid}) unless $r->{ent} =~ /\w\w/ && $r->{ent} !~ /\d/;
	$tc->flag('mdf', $r->{oid}) unless ! defined $r->{mdf} || 
	    $r->{mdf} =~ /\w\w/ && $r->{mdf} !~ /\d/;
	
	$tc->flag('dcr', $r->{oid}) unless $r->{dcr} && $r->{dcr} =~ /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/;
	$tc->flag('dmd', $r->{oid}) unless $r->{dmd} && $r->{dmd} =~ /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/;
    }
    
    $tc->ok_all("subtree opinions show");
};


# Now test listing opinions using 'all_records'.  We use the limit and offset
# parameters as well, so as not to fetch the entire 150+ MB result.

subtest 'all_records and limit' => sub {
    
    my $OFFSET_1 = 10000;
    my $DIFF_1 = 80;
    my $OFFSET_2 = $OFFSET_1 + $DIFF_1;
    my $LIMIT_1 = 100;
    
    # First compare two overlapping chunks of the full result set of
    # classification opinions, and make sure they overlap properly.
    
    my @r1 = $T->fetch_records("/taxa/opinions.json?all_records&offset=$OFFSET_1&limit=$LIMIT_1",
    			       "all_records offset 10000, limit 100");
    
    cmp_ok( @r1, '==', 100, "all_records offset 10000, limit 100 returns 100 records" );
    
    my @r2 = $T->fetch_records("/taxa/opinions.json?all_records&offset=$OFFSET_2&limit=$LIMIT_1",
    			       "all_records offset 10080, limit 100");
    
    foreach my $i ( 0..($LIMIT_1-$DIFF_1-1) )
    {
    	unless ( $r2[$i]{oid} eq $r1[$i+$DIFF_1]{oid} )
    	{
    	    fail("all_records with differing offsets return overlapping result sets");
    	    last;
    	}
    }
    
    # Then do the same for the full set of all opinions.
    
    my @r3 = $T->fetch_records("/taxa/opinions.json?all_records&offset=$OFFSET_1&limit=$LIMIT_1",
    			       "all_records offset 10000, limit 100");
    
    cmp_ok( @r3, '==', 100, "all_records offset 10000, limit 100 returns 100 records" );
    
    my @r4 = $T->fetch_records("/taxa/opinions.json?all_records&offset=$OFFSET_2&limit=$LIMIT_1",
    			       "all_records offset 10080, limit 100");
    
    foreach my $i ( 0..($LIMIT_1-$DIFF_1-1) )
    {
    	unless ( $r4[$i]{oid} eq $r3[$i+$DIFF_1]{oid} )
    	{
    	    fail("all_records with differing offsets return overlapping result sets");
    	    last;
    	}
    }
    
};


# Then test some other taxon relationships.

subtest 'other relationships' => sub {
    
    my $NAME_1 = 'Stegosaurus';
    my $NAME_2 = 'Hypsirophus';
    
    my @r1 = $T->fetch_records("/taxa/opinions.json?name=$NAME_1&rel=synonyms&op_type=class",
			       "synonym classification");
    
    my %status1 = $T->count_values( \@r1, 'sta' );
    
    cmp_ok( $status1{'belongs to'}, '==', 1, "synonym classification found one 'belongs to' opinion" );
    
    my $tc = Test::Conditions->new;
    
    foreach my $k ( keys %status1 )
    {
	$tc->set("found status '$k'") unless $k eq 'belongs to' || $k eq 'subjective synonym of' ||
	    $k eq 'objective synonym of' || $k eq 'replaced by';
    }
    
    $tc->ok_all("synonym classification found proper status codes");
    
    my @r2a = $T->fetch_records("/taxa/opinions.json?name=$NAME_1&rel=all_parents&op_type=class",
			       "all_parents classification");
    
    ok( $r2a[0]{nam} eq 'Eukaryota' || $r2a[0]{nam} eq 'Life', "all_parents classification starts at the top" );
    ok( $r2a[-1]{nam} eq $NAME_1 || $r2a[-1]{prl} eq $NAME_1, "all_parents classification ends at the base" );
    
    my %status2a = $T->count_values( \@r2a, 'sta' );
    
    foreach my $k ( keys %status2a )
    {
	$tc->set("found status '$k'" ) unless $k eq 'belongs to' || $k eq $r2a[-1]{sta};
    }
    
    $tc->ok_all("all_parents classification found proper status codes");
    
    my @r2b = $T->fetch_records("/taxa/opinions.json?name=$NAME_2&rel=all_parents&op_type=class",
				"all_parents classification junior");
    
    ok( $r2b[0]{nam} eq 'Eukaryota' || $r2b[0]{nam} eq 'Life', "all_parents classification junior starts at the top" );
    ok( $r2b[-1]{nam} eq $NAME_1 || $r2b[-1]{prl} eq $NAME_1, "all_parents classification junior ends at the base" );
    
    my %status2b = $T->count_values( \@r2b, 'sta' );
    
    foreach my $k ( keys %status2a )
    {
	$tc->set("found status '$k'" ) unless $k eq 'belongs to' || $k eq $r2a[-1]{sta};
    }
    
    $tc->ok_all("all_parents classification junior found proper status codes");
};


# Now we test the interaction between the 'op_type' parameter, which selects
# opinions according to what they say about taxa, and the 'status' parameter
# which selects taxa according to what their classification opinion says about
# them.

subtest 'list by op_type and status' => sub {

    my $BASE_1 = 'Mammalia';
    my $BASE_2 = 'Canidae';
    
    my (@r1) = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=invalid&taxon_status=valid",
				 "invalid opinions about valid taxa");
    
    my $tc = Test::Conditions->new;
    
    foreach my $r ( @r1 )
    {
	$tc->flag('otp', $r->{oid}) unless $r->{otp} && ($r->{otp} eq 'U' || $r->{otp} eq 'X');
	$tc->flag('sta', $r->{oid}) unless $r->{sta} && $r->{sta} =~ /nomen|invalid|misspelling/;
    }
    
    $tc->ok_all("invalid opinions about valid taxa");
    
    my (@r2) = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=valid&taxon_status=invalid",
				 "valid opinions about invalid taxa");
    
    foreach my $r ( @r2 )
    {
	# $tc->flag('otp', $r->{oid}) unless $r->{otp} && $r->{otp} eq 'U';
	$tc->flag('sta', $r->{oid}) unless $r->{sta} && $r->{sta} =~ /belongs|synonym|replaced/;
    }
    
    $tc->ok_all("valid opinions about invalid taxa");
};


# Now test that taxon filters are applied properly when fetching opinions.

subtest 'taxon filters' => sub {

    my $BASE_1 = 'Canidae';
    my $BASE_2 = 'Mammalia';
    
    my (@r1) = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=class&rank=genus-below_family",
				 "opinions with rank filter");
    
    my $tc = Test::Conditions->new();
    
    foreach my $r ( @r1 )
    {
	$tc->flag('rnk', $r->{oid}) unless $r->{rnk} && $r->{rnk} =~ /^(?:5|6|7|8|25)$/;
    }
    
    $tc->ok_all("opinions with rank filter");
    
    my (@r2) = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_2&pres=ichno",
				 "opinions with pres filter");
    
    my %names2 = $T->fetch_record_values("/taxa/list.json?base_name=$BASE_2&pres=ichno", 'nam',
					  "taxa with pres filter");
    
    foreach my $r ( @r2 )
    {
	$tc->flag('nam', $r->{nam}) unless $names2{$r->{nam}};
    }
    
    $tc->ok_all("opinions with pres filter");
    
    my (@r3) = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&extant=yes",
				 "opinions with extant filter");
    
    
    my %names3 = $T->fetch_record_values("/taxa/list.json?base_name=$BASE_1&extant=yes", 'nam',
					  "taxa with extant filter");
    
    foreach my $r ( @r3 )
    {
	$tc->flag('nam', $r->{nam}) unless $names3{$r->{nam}};
    }
    
    $tc->ok_all("opinions with extant filter");
    
    my (@r4) = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&interval=oligocene",
				 "opinions with interval filter");
    
    my (@t4) = $T->fetch_records("/taxa/list.json?base_name=$BASE_1&interval=oligocene",
				 "taxa with interval filter");
    
    my %op_names4 = $T->extract_values( \@r4, 'nam' );
    my %taxa_names4 = $T->extract_values( \@t4, 'nam' );
    
    $T->cmp_sets_ok( \%op_names4, '==', \%taxa_names4, "opinions and taxa with interval filter match" );
};


subtest 'list by ref info' => sub {
    
    my $BASE_1 = 'Canidae';
    
    my (@o1) = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&show=refattr,ref",
				 "base opinion list");
    
    my %ref_ids = $T->count_values( \@o1, 'rid' );
    my %ref_authors = $T->count_values( \@o1, 'aut' );
    my %ref_pubyrs = $T->count_values( \@o1, 'pby' );
    my %op_authors = $T->count_values( \@o1, 'oat' );
    
    # Pick the most common values of 'rid', 'aut' and 'pby' so that we test finding records using
    # them.
    
    my ($pick_rid, $rid2) = $T->find_prevalent( \%ref_ids );
    my ($d2, $pick_aut, @auts) = $T->find_prevalent( \%ref_authors );
    my ($d3, $d4, $pick_pby) = $T->find_prevalent( \%ref_pubyrs );
    my ($oat1, @oats) = $T->find_prevalent( \%op_authors );
    
    # Remove " and ..." or "et al." to get a single author name.
    
    $pick_aut =~ s/\s+and .*|\s+et al[.]$//;
    
    # Pick one value of 'oat' that differs from the value of 'aut' in the same record, and one
    # value of 'opy' that differs from the value of 'pby' in the same record.  In other words,
    # find one record where the opinion author is different from the reference author, and one
    # record in which the opinion publication year is different from the reference publication
    # year (doesn't have to be the same one).  This allows us to check that we can find opinions
    # by any of these attributes independently of the others.

    my ($pick_oat, $other_oat, $pick_opy);
    
    foreach my $o ( @o1 )
    {
	$pick_oat ||= $o->{oat} if $o->{oat} && $o->{aut} && $o->{oat} ne $o->{aut};
	$pick_opy ||= $o->{opy} if $o->{opy} && $o->{pby} && $o->{opy} ne $o->{pby};
    }
    
    ok( $pick_oat, "found a value for 'oat' different from 'aut'" );
    ok( $pick_opy, "found a value for 'opy' different from 'pby'" );
    
    my (@o2a) = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=all&ref_id=$pick_rid",
				  "ref_id filter");
    my (@o2b) = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=all&ref_author=$pick_aut&show=ref",
				  "ref_author filter");
    my (@o2c) = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=all&ref_pubyr=$pick_pby&show=refattr",
				  "ref_pubyr filter");
    my (@o2d) = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=all&op_author=$pick_oat",
				  "op_author filter");
    my (@o2e) = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=all&op_pubyr=$pick_opy",
				  "op_pubyr filter");
    
    # Test that these opinion filters retrieve only records with the proper values.
    
    my $tc = Test::Conditions->new;
    
    foreach my $r ( @o2a )
    {
	$tc->flag('bad rid with ref_id', $r->{oid}) unless $r->{rid} eq $pick_rid;
    }
    
    foreach my $r ( @o2b )
    {
	$tc->flag('bad aut with ref_author', $r->{oid}) unless $r->{ref} =~ /$pick_aut/o;
    }
    
    foreach my $r ( @o2c )
    {
	$tc->flag('bad pby with ref_pubyr', $r->{oid}) unless $r->{pby} eq $pick_pby;
    }
    
    foreach my $r ( @o2d )
    {
	$tc->flag('bad oat with op_author', $r->{oid}) unless $r->{oat} =~ /$pick_oat/o;
    }
    
    foreach my $r ( @o2e )
    {
	$tc->flag('bad opy with op_pubyr', $r->{oid}) unless $r->{opy} eq $pick_opy;
    }
    
    $tc->ok_all("opinion filters found proper records");
    
    # Then check over the base set of opinions to make sure that they don't miss any with the
    # proper values.
    
    cmp_ok( @o2a, '==', $ref_ids{$pick_rid}, "ref_id filter found complete set" );
    cmp_ok( @o2c, '==', $ref_pubyrs{$pick_pby}, "ref_pubyr filter found complete set" );
    
    my %o2a = $T->extract_values( \@o2a, 'oid' );
    my %o2b = $T->extract_values( \@o2b, 'oid' );
    my %o2c = $T->extract_values( \@o2c, 'oid' );
    my %o2d = $T->extract_values( \@o2d, 'oid' );
    my %o2e = $T->extract_values( \@o2e, 'oid' );
    
    foreach my $r ( @o1 )
    {
	$tc->flag('missing rid with ref_id filter', $r->{oid})
	    if $r->{rid} eq $pick_rid && ! $o2a{$r->{oid}};
	
	$tc->flag('missing aut with ref_author filter', $r->{oid})
	    if $r->{ref} =~ /$pick_aut/o && ! $o2b{$r->{oid}};
	
	$tc->flag('missing pby with ref_pubyr filter', $r->{oid})
	    if $r->{pby} eq $pick_pby && ! $o2c{$r->{oid}};
	
	$tc->flag('missing oat with op_author filter', $r->{oid})
	    if $r->{oat} =~ /$pick_oat/o && ! $o2d{$r->{oid}};
	
	$tc->flag('missing opy with op_pubyr filter', $r->{opy})
	    if $r->{opy} eq $pick_opy && ! $o2e{$r->{oid}};
    }
    
    $tc->limit_max( 'missing aut with ref_author filter' => 20 );
    
    $tc->ok_all("opinion filters don't miss any records");
    
    # Now make sure that we can specify a range of publication years with both 'op_pubyr' and
    # 'ref_pubyr'.
    
    my @OPYF = ( 1950, 1970 );
    my @PBYF = ( 1990, 2010 );
    
    my @o3a = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=all&op_pubyr=$OPYF[0]-$OPYF[1]",
				"op_pubyr range");
    
    foreach my $r ( @o3a )
    {
	unless ( $r->{opy} >= $OPYF[0] && $r->{opy} <= $OPYF[1] )
	{
	    $tc->flag("bad value for 'opy'", $r->{oid});
	}
    }
    
    my @o3b = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=all&show=refattr&ref_pubyr=$PBYF[0]-$PBYF[1]",
				"ref_pubyr range");
    
    foreach my $r ( @o3b )
    {
	unless ( $r->{pby} >= $PBYF[0] && $r->{pby} <= $PBYF[1] )
	{
	    $tc->flag("bad value for 'pby'", $r->{oid});
	}
    }
    
    $tc->ok_all("year range filters");
    
    # Now test that we can specify multiple values for ref_id, ref_author, op_author
    
    my @o4a = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=all&ref_id=$pick_rid , $rid2",
				"two ref_id values");
    
    my @o4b = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=all&ref_id=$pick_rid",
				"first ref_id value");
    
    my @o4c = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=all&ref_id=$rid2",
				"second ref_id value");
    
    cmp_ok( @o4a, '==', @o4b + @o4c, "two ref_id values found proper number of records" );
    
    # Make sure we are testing two different author names
    
    my $a1 = $pick_aut; $a1 =~ s/ and .*| et .*//;
    my $a2;
    
    while ( @auts )
    {
	$a2 = shift @auts;
	$a2 =~ s/ and .*| et .*//;
	last if $a2 ne $a1;
    }
    
    my @o4d = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=all&show=ref&ref_author=$a1,$a2",
				"two ref_author values");
    
    my %rid_4d = $T->fetch_record_values("/taxa/refs.json?base_name=$BASE_1&ref_type=ops&ref_author=$a1,$a2", 'oid',
					 "refs from two ref_author values");
    
    my $rid_list = join(',', keys %rid_4d);
    
    my @o4e = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=all&ref_id=$rid_list",
				"opinions using rid_list");
    
    cmp_ok( @o4e, '==', @o4d, "two ref_author values found proper number of records" );
    
    # $tc->set('did not find first ref_author');
    # $tc->set('did not find second ref_author');
    
    # diag("ref_author=$a1,$a2");
    
    # foreach my $r ( @o4d )
    # {
    # 	if ( $r->{ref} =~ /$a1/o )
    # 	{
    # 	   $tc->clear('did not find first ref_author');
    # 	}
	
    # 	elsif ( $r->{ref} =~ /$a2/o )
    # 	{
    # 	   $tc->clear('did not find second ref_author');
    # 	}
	
    # 	else
    # 	{
    # 	    $tc->flag('found record without either author', $r->{oid});
    # 	}
    # }
    
    # my %oid4b = $T->extract_values( \@o4d, 'oid' );
    
    # foreach my $r ( @o1 )
    # {
    # 	$tc->flag('missed record with one of the authors', $r->{oid})
    # 	    if $r->{ref} =~ /$a1|$a2/o && ! $oid4b{$r->{oid}};
    # }
    
    # $tc->limit_max('missed record with one of the authors' => 10);
    
    # $tc->ok_all("two ref_author values");
    
    # Make sure we are testing two different op_author names.  This is a bit trickier, we need to
    # go through every record and make sure we're not missing any.
    
    my $oa1 = $pick_oat; $oa1 =~ s/ and .*| et .*//;
    my $oa2;
    
    while ( @oats )
    {
	$oa2 = shift @oats;
	$oa2 =~ s/ and .*| et .*//;
	last if $oa2 ne $oa1;
    }
    
    diag("op_author=$oa1,$oa2");
    
    my @o4f = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&op_type=all&op_author=$oa1,$oa2",
				"two op_author values");
    
    $tc->set('did not find first op_author');
    $tc->set('did not find second op_author');
    
    foreach my $r ( @o4f )
    {
	if ( $r->{oat} =~ /$oa1/o )
	{
	   $tc->clear('did not find first op_author');
	}
	
	elsif ( $r->{oat} =~ /$oa2/o )
	{
	   $tc->clear('did not find second op_author');
	}
	
	else
	{
	    $tc->flag('found record without either op_author', $r->{oid});
	}
    }
    
    my %oid4c = $T->extract_values( \@o4f, 'oid' );
    
    foreach my $r ( @o1 )
    {
	$tc->flag('missed record with one of the op_authors', $r->{oid})
	    if $r->{oat} =~ /$oa1|$oa2/ && ! $oid4c{$r->{oid}};
    }
    
    $tc->ok_all("two op_author values");
     
    # Now test ref_title and ref_pubtitle
    
    my ($refdata) = $T->fetch_records("/refs/single.json?id=$pick_rid");
    
    my $pick_pubtitle = $refdata->{pbt};
    my $pick_reftitle = $refdata->{tit};
    
    # diag("pub_title=$pick_pubtitle");
    # diag("ref_title=$pick_reftitle");
    
    my (@o5a) = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&pub_title=$pick_pubtitle&show=ref",
				  "pub_title filter");
    
    my (@o5b) = $T->fetch_records("/taxa/opinions.json?base_name=$BASE_1&ref_title=$pick_reftitle&show=ref",
    				  "ref_title filter");
    
    foreach my $r ( @o5a )
    {
	$tc->flag("found record with bad pub_title", $r->{oid})
	    unless $r->{ref} =~ $pick_pubtitle;
    }
    
    my $check_reftitle = $pick_reftitle;
    $check_reftitle =~ s/\(/\\(/g; $check_reftitle =~ s/\)/\\)/g;
    $check_reftitle =~ s/\?/\\?/g;
    
    foreach my $r ( @o5b )
    {
	$tc->flag("found record with bad ref_title", $r->{oid})
	    unless $r->{ref} =~ /$check_reftitle/o;
    }
    
    $tc->ok_all("pub_title and ref_title filters");
    
    # Now try pub_title and ref_title with wildcards.  We do this by comparing the list of 'rid'
    # values to the list of 'oid' values from /taxa/refs.json.
    
    my $PUBT_1 = 'Bulletin %';
    my $REFT_1 = 'Phylogenetic %';
    
    my ( %rid_6a ) = $T->fetch_record_values("/taxa/opinions.json?base_name=$BASE_1&op_type=all&pub_title=$PUBT_1", 'rid',
					     "pub_title filter with wildcard");
    
    my ( %rid_6b ) = $T->fetch_record_values("/taxa/opinions.json?base_name=$BASE_1&op_type=all&ref_title=$REFT_1", 'rid',
					     "ref_title filter with wildcard");
    
    my ( %oid_6a ) = $T->fetch_record_values("/taxa/refs.json?base_name=$BASE_1&ref_type=ops&pub_title=$PUBT_1", 'oid',
					     "refs from pub_title filter with wildcard");
    
    my ( %oid_6b ) = $T->fetch_record_values("/taxa/refs.json?base_name=$BASE_1&ref_type=ops&ref_title=$REFT_1", 'oid',
					     "refs from ref_title filter with wildcard");
    
    $T->cmp_sets_ok( \%rid_6a, '==', \%oid_6a, "pub_title filter finds opinions matching up with refs" );
    $T->cmp_sets_ok( \%rid_6b, '==', \%oid_6b, "ref_title filter finds opinions matching up with refs" );
};


subtest 'list by crmod and all_records' => sub {
    
    my $NAME_1 = 'Mammalia';
    my $COUNT_1 = '50';
    my $COUNT_2 = '100';
    
    my @r1a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&order=created&show=crmod&limit=$COUNT_1",
				"latest $COUNT_1 opinions from '$NAME_1'");
    
    unless ( @r1a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    cmp_ok( @r1a, '==', $COUNT_1, "latest $COUNT_1 opinions from '$NAME_1' found $COUNT_1 records" );
    $T->check_order( \@r1a, 'dcr', 'ge', 'oid', "latest $COUNT_1 opinions from '$NAME_1'" );
    
    my @r2a = $T->fetch_records("/taxa/opinions.json?all_records&order=modified&show=crmod&limit=$COUNT_2",
				"latest $COUNT_2 opinions");
    
    cmp_ok( @r2a, '==', $COUNT_2, "latest $COUNT_2 opinions found $COUNT_2 records" );
    $T->check_order( \@r2a, 'dmd', 'ge', 'oid', "latest $COUNT_2 opinions" );
    
    my @r2b = $T->fetch_records("/taxa/opinions.json?all_records&order=name&ops_created_before=2003&show=crmod",
				"opinions created before 2003");
    
    cmp_ok( @r2b, '>', 500, "opinions created before 2003 finds at least 500 records" );
    $T->check_order( \@r2b, 'nam', 'le', 'oid', "created before 2003 finds proper record order" );
    
    my $tc = Test::Conditions->new;
    
    foreach my $r ( @r2b )
    {
	$tc->flag('dcr', $r->{oid}) unless $r->{dcr} && $r->{dcr} lt "2003" && 
	    $r->{dcr} =~ /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/;
    }
    
    $tc->ok_all("opinions created before 2003 finds records with proper attributes");
    
    my @r2c = $T->fetch_records("/taxa/opinions.json?all_records&ops_modified_after=2014&show=crmod&limit=100",
				"modified after 2014");
    
    cmp_ok( @r2c, '==', 100, "modified after 2014 finds proper number of records" );
        
    foreach my $r ( @r2c )
    {
	$tc->flag('dmd', $r->{oid}) unless $r->{dmd} && $r->{dmd} ge "2014" && 
	    $r->{dmd} =~ /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/;
    }
    
    $tc->ok_all("opinions created after 2014 finds records with proper attributes" );
    
    # Now try 'taxa_created_before' and check that some of the opinion created
    # dates are later.
    
    my @r3 = $T->fetch_records("/taxa/opinions.json?all_records&taxa_created_before=2003&show=crmod",
			       "opinions on taxa created before 2003");
    
    $tc->set('all dcr before 2003');
    
    foreach my $r ( @r3 )
    {
	$tc->clear('all dcr before 2003') if $r->{dcr} gt '2003';
    }
    
    $tc->ok_all("opinions on taxa created before 2003 finds at least one record with greater dcr");
};


subtest 'opinion order' => sub {

    my $NAME_1 = 'Dascillidae';
    my $NAME_2 = 'Canis';
    
    # Start with the default, which should be 'hierarchy';
    
    my @r1a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&show=seq", "order default");
    
    unless ( @r1a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my @r1b = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&order=hierarchy&show=seq", 
				"order hierarchy");
    my @r1c = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&order=hierarchy.desc&show=seq",
				"order hierarchy.desc");
    
    check_order_hierarchy( \@r1b, 'oid', "order hierarchy" );
    
    is_deeply( \@r1b, \@r1a, "default order is 'hierarchy'");
    
    my @r1d = reverse @r1c;
    
    is_deeply( \@r1d, \@r1a, "order hierarchy.desc" );

    # Then check 'name'
    
    my @r2a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_2&order=name", 
				"order name");
    my @r2b = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_2&order=name.desc", 
				"order name.desc");
    
    $T->check_order( \@r2a, 'nam', 'le', 'oid', "order name" );
    $T->check_order( \@r2b, 'nam', 'ge', 'oid', "order name.desc" );
    
    # Then check 'childname'
    
    my @r2c = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_2&order=childname",
				"order childname");
    my @r2d = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_2&order=childname.desc",
				"order childname.desc");
    
    foreach my $r ( @r2c, @r2d )
    {
	$r->{cnm} ||= $r->{nam};
    }
    
    $T->check_order( \@r2c, 'cnm', 'le', 'oid', "order name" );
    $T->check_order( \@r2d, 'cnm', 'ge', 'oid', "order name.desc" );
    
    # Then check 'ref'
    
    my @r3a = $T->fetch_records("/taxa/opinions.csv?base_name=$NAME_1&order=ref", 
				"order ref");
    my @r3b = $T->fetch_records("/taxa/opinions.csv?base_name=$NAME_1&order=ref.desc", 
				"order ref.desc");
    
    $T->check_order( \@r3a, 'reference_no', '<=', 'opinion_no', "order ref" );
    $T->check_order( \@r3b, 'reference_no', '>=', 'opinion_no', "order ref.desc" );
    
    # Then check 'optype' and 'pubyr'.  We try them in both orders, and we
    # translate 'C' to '0' and 'U' to '1' in order to facilitate easy comparison.
    
    my @r4a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_2&order=optype,pubyr",
				"order optype, pubyr");
    
    my @r4b = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_2&order=pubyr,optype",
				"order pubyr.asc, optype.desc");
    
    foreach my $r ( @r4a, @r4b )
    {
	$r->{otp} = $r->{otp} eq 'C' ? 0 : 1;
    }
    
    $T->check_order( \@r4a, [ 'otp', 'opy' ], [ '<=', '>=' ], 'oid', "order optype, pubyr" );
    $T->check_order( \@r4b, [ 'opy', 'otp' ], [ '>=', '<=' ], 'oid', "order pubyr, optype" );
    
    # Check 'author'
    
    my @r5a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_2&order=author",
				"order author");
    my @r5b = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_2&order=author.desc",
				"order author.desc");
    
    $T->check_order( \@r5a, 'oat', 'le', 'oid', "order author" );
    $T->check_order( \@r5b, 'oat', 'ge', 'oid', "order author.desc" );
    
    # Check 'basis'
    
    my @r6a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_2&order=basis&show=basis",
				"order basis");
    my @r6b = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_2&order=basis.asc&show=basis",
				"order basis.asc");
    
    foreach my $r ( @r6a, @r6b )
    {
	$r->{bas} = $r->{bas} eq 'stated with evidence'    ? 3
	          : $r->{bas} eq 'stated without evidence' ? 2
							   : 1;		  
    }
    
    $T->check_order( \@r6a, 'bas', '>=', 'oid', "order basis" );
    $T->check_order( \@r6b, 'bas', '<=', 'oid', "order basis.asc" );
    
    # Check 'created', 'modified'
    
    my @r10a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&show=crmod&order=created",
				 "order created");
    my @r10b = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&show=crmod&order=created.asc",
				 "order created.asc");
    my @r10c = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&show=crmod&order=modified",
				 "order modified");
    my @r10d = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&show=crmod&order=modified.asc",
				 "order modified.asc");
    
    $T->check_order( \@r10a, 'dcr', 'ge', 'nam', "order created" );
    $T->check_order( \@r10b, 'dcr', 'le', 'nam', "order created.asc" );
    $T->check_order( \@r10c, 'dmd', 'ge', 'nam', "order modified" );
    $T->check_order( \@r10d, 'dmd', 'le', 'nam', "order modified.asc" );
    
    # Now check order with multiple parameters. We can't possibly check all combinations, so we'll
    # try just a few.
    
    my @r20a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_2&order=author,name",
				 "order author, name");
    
    $T->check_order( \@r20a, ['oat', 'nam'], ['le', 'le'], 'oid', "order author, name");
    
    my @r20b = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_2&order=author.desc,name.desc",
				 "order author, name reversed");
    
    $T->check_order( \@r20b, ['oat', 'nam'], ['ge', 'ge'], 'oid', "order author, name reversed" );
    
    # Then, we will need to try an order parameter with at least a few of the
    # other methods of selecting base taxa.
    
    my $NAME_21 = "Canis %";
    
    my @r21 = $T->fetch_records("/taxa/opinions.json?match_name=$NAME_21&order=pubyr.desc",
				 "match order pubyr.desc");
    
    $T->check_order( \@r21, 'opy', '>=', 'oid', "match order pubyr.desc" );
    
    my $NAME_22 = "Canis,Felis,Conus,Ursus";
    
    my @r22 = $T->fetch_records("/taxa/opinions.txt?taxon_name=$NAME_22&order=author",
				 "taxon_name order author");
    
    $T->check_order( \@r22, 'author', 'le', 'opinion_no', "taxon_name order author" );
    
    my %names22 = $T->extract_values( \@r22, 'taxon_name' );
    my %test22 = ( Canis => 1, Felis => 1, Conus => 1, Ursus => 1 );
    
    $T->cmp_sets_ok( \%names22, '==', \%test22, "taxon_name order author found proper taxon names" );
    
    my $NAME_23 = "Stegosaurus";
    
    my @r23 = $T->fetch_records("/taxa/opinions.json?name=$NAME_23&rel=synonyms&order=pubyr.asc",
				"synonyms order pubyr.asc");
    
    $T->check_order( \@r23, 'opy', '<=', 'oid', "synonyms order pubyr.asc" );
};


sub check_order_hierarchy {
    
    my ($records_ref, $idfield, $message) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;

    my $last;
    my $tc = Test::Conditions->new;

    foreach my $r ( @$records_ref )
    {
	unless ( $last )
	{
	    $last = $r;
	    next;
	}
	
	if ( $r->{lsq} < $last->{lsq} )
	{
	    $tc->flag('lft', $r->{$idfield});
	}

	elsif ( $r->{lsq} == $last->{lsq} )
	{
	    if ( $r->{otp} eq 'C' )
	    {
		$tc->flag('otp', $r->{$idfield});
	    }

	    elsif ( $last->{otp} ne 'C' && $r->{opy} > $last->{opy} )
	    {
		$tc->flag('opy', $r->{$idfield});
	    }
	}
    }
    
    $tc->ok_all($message);
}

# AAA

subtest 'list by authent' => sub {

    my $NAME_1 = 'Felidae';
    
    # First fetch a set of base records regardless of authorizer, enterer or modifier.
    
    my @r1 = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&show=ent,entname", "base records");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Make sure that the name and id fields all have proper values.
    
    my $tc = Test::Conditions->new;
    
    foreach my $r ( @r1 )
    {
	foreach my $k ( 'ati', 'eni', 'mdi' )
	{
	    next if $k eq 'mdi' && ! defined $r->{mdi};
	    
	    $tc->flag($k, $r->{oid}) unless defined $r->{$k} && $r->{$k} =~ /^prs:\d+$/;
	}
	
	foreach my $k ( 'ath', 'ent', 'mdf' )
	{
	    next if $k eq 'mdf' && ! defined $r->{mdf};
	    
	    $tc->flag($k, $r->{oid}) unless defined $r->{$k} && $r->{$k} ne '' && $r->{$k} !~ /\d/;
	}
    }
    
    $tc->ok_all( "base records all have proper authent names and ids" );
    
    # Find the person who has authorized the most records, then the non-authorizer who has entered
    # the most records, then the non-enterer who has modified the most records.  Do this for both
    # names and ids.
    
    my %ati = $T->count_values( \@r1, 'ati' );
    my %eni = $T->count_values( \@r1, 'eni' );
    my %mdi = $T->count_values( \@r1, 'mdi' );
    my %ath = $T->count_values( \@r1, 'ath' );
    my %ent = $T->count_values( \@r1, 'ent' );
    my %mdf = $T->count_values( \@r1, 'mdf' );
    
    my ($ath_max, $ath_count) = $T->find_max( \%ath ); delete $ent{$ath_max}; delete $mdf{$ath_max};
    my ($ent_max, $ent_count) = $T->find_max( \%ent ); delete $mdf{$ent_max};
    my ($mdf_max, $mdf_count) = $T->find_max( \%mdf );
    
    diag("   ath: $ath_max  ent: $ent_max  mdf: $mdf_max");
    
    my ($ati_max, $ati_count) = $T->find_max( \%ati ); delete $eni{$ati_max}; delete $mdi{$ati_max};
    my ($eni_max, $eni_count) = $T->find_max( \%eni ); delete $mdi{$eni_max};
    my ($mdi_max, $mdi_count) = $T->find_max( \%mdi );
    
    # Select all the records authorized and not authorized by that person, and make sure the
    # totals add up.
    
    my @r2a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_authorized_by=$ath_max&show=ent,entname",
				"authorized by max name");
    my @r2b = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_authorized_by=!$ath_max&show=ent,entname",
				"not authorized by max name");
    
    cmp_ok( @r2a + @r2b, '==', @r1, "authorized by + not authorized by = all" );
    
    # Same with external identifiers.
    
    my @r2c = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_authorized_by=$ati_max&show=ent,entname",
				"authorized by max id");
    my @r2d = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_authorized_by=!$ati_max&show=ent,entname",
				"not authorized by max id");
    
    cmp_ok( @r2c + @r2d, '==', @r1, "authorized by + not authorized by = all" );
    
    cmp_ok( @r2c, '==', @r2a, "authorized_by ati max = authorized_by ath max" );
    cmp_ok( @r2d, '==', @r2b, "not authorized_by ati max = not authorized_by ath max" );
    
    # Same with numeric identifiers.
    
    $ati_max =~ /(\d+)/; my $ati_num = $1;
    
    my @r2e = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_authorized_by=$ati_num&show=ent,entname",
				"authorized by max num");
    my @r2f = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_authorized_by=!$ati_num&show=ent,entname",
				"not authorized by max num");
    
    cmp_ok( @r2e + @r2f, '==', @r1, "authorized by + not authorized by = all" );
    
    cmp_ok( @r2c, '==', @r2e, "authorized_by ati max = authorized_by ati num max" );
    cmp_ok( @r2d, '==', @r2f, "not authorized_by ati max = not authorized_by ati num max" );
    
    # Make sure that each of the records has the proper identifier and name.
    
    foreach my $r ( @r2a, @r2c, @r2e )
    {
	$tc->flag('proper ati + ath', $r->{oid}) 
	    unless $r->{ati} eq $ati_max && $r->{ath} eq $ath_max;
    }
    
    $tc->ok_all( "authorized by max finds records with proper name and id" );
    
    foreach my $r ( @r2b, @r2d, @r2f )
    {
	$tc->flag('proper ati + ath', $r->{oid}) 
	    unless $r->{ati} ne $ati_max && $r->{ath} ne $ath_max;
    }
    
    $tc->ok_all( "not authorized by max finds records with proper name and id" );
    
    # Now check enterers in the same way.
    
    my @r3a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_entered_by=$ent_max&show=ent,entname",
				"entered by max name");
    my @r3b = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_entered_by=!$ent_max&show=ent,entname",
				"not entered by max name");
    
    cmp_ok( @r3a + @r3b, '==', @r1, "entered by + not entered by = all (ent_max)" );
    
    # Same with external identifiers.
    
    my @r3c = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_entered_by=$eni_max&show=ent,entname",
				"entered by max id");
    my @r3d = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_entered_by=!$eni_max&show=ent,entname",
				"not entered by max id");
    
    cmp_ok( @r3c + @r3d, '==', @r1, "entered by + not entered by = all (eni_max)" );
    
    # Same with numeric identifiers.
    
    $eni_max =~ /(\d+)/; my $eni_num = $1;
    
    my @r3e = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_entered_by=$eni_num&show=ent,entname",
				"entered by max num");
    my @r3f = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_entered_by=!$eni_num&show=ent,entname",
				"not entered by max num");
    
    cmp_ok( @r3e + @r3f, '==', @r1, "entered by + not entered by = all (eni_num)" );
    
    # Again make sure that each of the records has the proper identifier and name.
    
    foreach my $r ( @r3a, @r3c, @r3e )
    {
	$tc->flag("proper eni + ent", $r->{oid}) 
	    unless $r->{eni} eq $eni_max && $r->{ent} eq $ent_max;
    }
    
    $tc->ok_all( "entered by max finds records with proper name and id" );
    
    foreach my $r ( @r3b, @r3d, @r3f )
    {
	$tc->flag("proper eni + ent", $r->{oid})
	    unless $r->{eni} ne $eni_max && $r->{ent} ne $ent_max;
    }
    
    $tc->ok_all( "not entered by max finds records with proper name and id" );
    
    # Now same for modifiers.  For this, we have to take into account that not every record may
    # have a modifier.
    
    my @r4any = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_modified_by=%&show=ent,entname",
				   "modified by any");
    
    my @r4a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_modified_by=$mdf_max&show=ent,entname",
				"modified by max name");
    my @r4b = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_modified_by=!$mdf_max&show=ent,entname",
				"not modified by max name");
    
    cmp_ok( @r4a + @r4b, '==', @r1, "modified by + not modified by = any (mdf max)" );
    
    # Same with external identifiers.
    
    my @r4c = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_modified_by=$mdi_max&show=ent,entname",
				"modified by max id");
    my @r4d = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_modified_by=!$mdi_max&show=ent,entname",
				"not modified by max id");
    
    cmp_ok( @r4c + @r4d, '==', @r1, "modified by + not modified by = all (mdi max)" );
    
    # Same with numeric identifiers.
    
    $mdi_max =~ /(\d+)/; my $mdi_num = $1;
    
    my @r4e = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_modified_by=$mdi_num&show=ent,entname",
				"modified by max num");
    my @r4f = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_modified_by=!$mdi_num&show=ent,entname",
				"not modified by max num");
    
    cmp_ok( @r4e + @r4f, '==', @r1, "modified by + not modified by = all (mdi num)" );
    
    # Again make sure that each of the records has the proper identifier and name.
    
    foreach my $r ( @r4a, @r4c, @r4e )
    {
	$tc->flag("proper mdi + mdf", $r->{oid})
	    unless $r->{mdi} eq $mdi_max && $r->{mdf} eq $mdf_max;
    }
    
    $tc->ok_all( "modified by max finds records with proper name and id" );
    
    foreach my $r ( @r4b, @r4d, @r4f )
    {
	$tc->flag("proper mdi", $r->{oid})
	    unless ! defined $r->{mdi} || $r->{mdi} ne $mdi_max;
	$tc->flag("proper mdf", $r->{oid})
	    unless ! defined $r->{mdf} || $r->{mdf} ne $mdf_max;
    }
    
    $tc->ok_all( "not modified by max finds records with proper name and id" );
    
    # Then check '%!'. This should provide the inverse of @r4a in @r4any.
    
    my @r4g = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_modified_by=%!$mdi_max&show=ent,entname",
				"modified by other than max id");
    
    cmp_ok( @r4c + @r4g, '==', @r4any, "modified by + modified by other than = modified by any" );
    
    # Now we need to try the value '!'. This should return no records for 'authorized_by' and
    # 'entered_by', and only taxa that have not been modified for 'modified_by'.
    
    my @r5a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_authorized_by=!&show=ent,entname",
				"authorized by '!'", { no_records_ok => 1 });
    
    cmp_ok( @r5a, '==', 0, "authorized by '!' found no records" );
    
    my @r5b = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_entered_by=!&show=ent,entname",
				"entered by '!'", { no_records_ok => 1 });
    
    cmp_ok( @r5b, '==', 0, "entered by '!' found no records" );
    
    my @r5c = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_modified_by=!&show=ent,entname",
				"modified by '!'");
    
    foreach my $r ( @r5c )
    {
	$tc->flag("bad mdf or mdi", $r->{oid}) if $r->{mdf} || $r->{mdi};
    }
    
    $tc->ok_all( "modified by '!' finds records not modified at all" );
    
    # Now try the values '@' and '!@'.
    
    my @r5d = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_authorized_by=\@&show=ent,entname",
				"authorized by '\@'");
    
    my @r5e = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_authorized_by=!\@&show=ent,entname",
				"authorized by '!\@'");
    
    cmp_ok( @r5d + @r5e, '==', @r1, "authorized by '\@' + authorized by '!\@' = all" );
    
    foreach my $r ( @r5d )
    {
	$tc->flag('ati = eni', $r->{oid}) if $r->{ati} eq $r->{eni};
    }
    
    $tc->ok_all("authorized by '\@' finds proper records");
    
    foreach my $r ( @r5e )
    {
	$tc->flag('ati != eni', $r->{oid}) if $r->{ati} ne $r->{eni};
    }
    
    $tc->ok_all("authorized by '\@!' finds proper records");
    
    my @r5f = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_modified_by=\@&show=ent,entname",
				"modified by '\@'");
    
    my @r5g = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_modified_by=!\@&show=ent,entname",
				"modified by '!\@'");
    
    cmp_ok( @r5f + @r5g, '==', @r1, "modified by '\@' + modified by '!\@' = all" );
    
    # Check 'authent_by' using the person who authorized the most records, and the person who
    # entered the most.
    
    my @r6a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_authent_by=$ati_max&show=ent,entname",
				"authent_by ati_max");
    my @r6b = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_authent_by=!$ati_max&show=ent,entname",
				"authent_by !ati_max");
    
    cmp_ok( @r6a + @r6b, '==', @r1, "authent_by + not authent_by = all (ati max)" );
    
    foreach my $r ( @r6a )
    {
	$tc->flag('bad ati or eni', $r->{oid}) unless $r->{ati} eq $ati_max || $r->{eni} eq $ati_max;
    }
    
    $tc->ok_all( "authent_by finds records with proper auth/ent identifier" );
    
    foreach my $r ( @r6b )
    {
	$tc->flag('bad ati or eni', $r->{oid}) if $r->{ati} eq $ati_max || $r->{eni} eq $ati_max;
    }
    
    $tc->ok_all( "authent_by ! finds records with improper auth/ent identifier" );
    
    # Then do the same check using the person who entered the most records (but did not authorize
    # the most). 
    
    my @r6c = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_authent_by=$eni_max&show=ent,entname",
				"authent_by eni max");
    my @r6d = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_authent_by=!$eni_max&show=ent,entname",
				"not authent_by eni max");
    
    cmp_ok( @r6c + @r6d, '==', @r1, "authent_by + not authent_by = all (eni max)" );
    
    # Now do the same for touched_by, but a smaller number of tests.
    
    my @r7a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_touched_by=$ati_max&show=ent,entname",
				"touched_by ati max");
    my @r7b = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_touched_by=!$ati_max&show=ent,entname",
				"not touched_by ati max");
    
    cmp_ok( @r7a + @r7b, '==', @r1, "touched_by + not touched+by = all (ati max)" );
    
    my @r7c = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_touched_by=$mdi_max&show=ent,entname",
				"touched_by mdi max");
    my @r7d = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_touched_by=!$mdi_max&show=ent,entname",
				"not touched_by mdi max");
    
    cmp_ok( @r7c + @r7d, '==', @r1, "touched_by + not touched+by = all (mdi max)" );    
    
    # Then we check two different parameters together.  We can't possibly test all combinations,
    # but we can at least check one.
    
    my @r10a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_authorized_by=$ati_max&" .
				 "ops_modified_by=!$mdi_max&show=ent,entname", "auth_by and not mod_by");
    
    my %combo_oid = $T->extract_values( \@r10a, 'oid' );
    my %check_oid = $T->extract_values( \@r2a, 'oid' );
    my %mod_by_oid = $T->extract_values( \@r4c, 'oid' );
    
    # subtract %mod_by_oid from %check_oid, then test.
    
    delete $check_oid{$_} foreach keys %mod_by_oid;
    
    $T->cmp_sets_ok( \%combo_oid, '==', \%check_oid, "auth_by and not mod_by returns proper records" );
    
    # Then we try some parameters with multiple values.
    
    my @r11a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_entered_by=$ati_max,$eni_max&" .
				 "show=ent,entname", "entered_by multiple");
    
    my %eni_count = $T->count_values( \@r11a, 'eni' );
    
    cmp_ok( $eni_count{$ati_max} + $eni_count{$eni_max}, '==', @r11a, 
	    "entered_by multiple gets records with proper eni" );
    cmp_ok( $eni_count{$ati_max}, '>', 0, "entered_by multiple gets at least one entered by ati_max" );
    cmp_ok( $eni_count{$eni_max}, '>', 0, "entered_by multiple gets at least one entered by eni_max" );
    
    my @r11b = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_touched_by=$ati_max,$eni_max&" .
				 "show=ent,entname", "touched_by multiple");
    
    my @r11c = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&ops_touched_by=!$ati_max,$eni_max&" .
				 "show=ent,entname", "touched_by !multiple");
    
    cmp_ok( @r11b + @r11c, '==', @r1, "touched_by multiple + touched_by !multiple = all" );
    
    # Then we test that fetching opinions according to who entered the taxa works okay.  We check
    # this by matching up with the equivalent query to /taxa/list.
    
    my @r12a = $T->fetch_records("/taxa/opinions.json?base_name=$NAME_1&taxa_entered_by=$ati_max",
				 "opinions on taxa entered_by");
    my @t12a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_entered_by=$ati_max",
				 "taxa entered_by");
    
    my %ops_names = $T->extract_values( \@r12a, 'nam' );
    my %taxa_names = $T->extract_values( \@t12a, 'nam' );
    
    $T->cmp_sets_ok( \%ops_names, '==', \%taxa_names, "opinions on taxa entered_by matches taxa entered_by" );
};
