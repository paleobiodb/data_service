# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test all of the /data1.2/occs operations.
# 

use strict;

use feature 'unicode_strings';
use feature 'fc';

use Test::Most tests => 15;

# use CoreFunction qw(connectDB configData);
# use Taxonomy;

use lib 'lib', '../lib';

use TaxonDefs;

use lib 't';
use Tester;
use Test::Conditions;
use Test::Selection;


choose_subtests(@ARGV);

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });

my %INTERVAL_NAME;
my ($SAVE_OCCS_1, $SAVE_OCCS_2, $SAVE_NAME_1, $SAVE_BYOCC_1);


# Now try listing occurrences from a couple of taxa.  Check for basic
# consistency, and make sure that the fields of the basic block look right.
# We also test the 'bin' output block.

subtest 'subtree basic' => sub {
    
    select_subtest || return;
    
    # First fetch a reasonably large subtree.  We add 'show=bin' so we can
    # reuse this list in the next subtest.
    
    my $NAME_1 = 'CANIS';
    
    my @o1 = $T->fetch_records("/occs/list.json?base_name=$NAME_1&show=bin", "occs list json '$NAME_1'");
    
    unless ( @o1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Save a pointer to this list of occurrences, so we can use it in a
    # subsequent subtest without having to re-fetch it.
    
    $SAVE_OCCS_1 = \@o1;
    $SAVE_NAME_1 = $NAME_1;
    
    # Now perform some basic checks on the fetched subtree.
    
    my %tid1 = $T->fetch_record_values("/taxa/list.json?base_name=$NAME_1", 'oid', "taxa oids '$NAME_1'");
    
    my %o_tid1 = $T->extract_values( \@o1, 'tid' );
    
    $T->cmp_sets_ok( \%o_tid1, '<=', \%tid1, "occ tids match taxa list" );
    
    my %o_tdf1 = $T->extract_values( \@o1, 'tdf' );
    
    my %test1 = ( 'species not entered' => 1, 'subjective synonym of' => 1,
		  'obsolete variant of' => 1 );
    
    $T->cmp_sets_ok( \%o_tdf1, '>=', \%test1, "found sample 'tdf' values" );
    
    my %tna1 = $T->fetch_record_values("/taxa/list.json?base_name=$NAME_1", 'nam', "taxa names '$NAME_1'");
    
    my %o_tna1 = $T->extract_values( \@o1, 'tna' );
    
    $T->cmp_sets_ok( \%o_tna1, '<=', \%tna1, "occ taxa names match taxa list" );
    
    %INTERVAL_NAME = $T->fetch_record_values("/intervals/list.json?all_records", 'nam', "interval names");
    
    my $tc = Test::Conditions->new;
    
    foreach my $o ( @o1 )
    {
	my $oid = $o->{oid};
	
	$tc->flag('oid', 'MISSING VALUE') unless $oid && $oid =~ /^occ:\d+$/;
	# $tc->flag('typ', $oid) unless $o->{typ} && $o->{typ} eq 'occ';
	$tc->flag('cid', $oid) unless $o->{cid} && $o->{cid} =~ /^col:\d+$/;
	$tc->flag('lv1', $oid) unless $o->{lv1} && $o->{lv1} =~ /^clu:\d+$/;
	$tc->flag('lv2', $oid) unless $o->{lv2} && $o->{lv2} =~ /^clu:\d+$/;
	$tc->flag('lv3', $oid) unless $o->{lv3} && $o->{lv3} =~ /^clu:\d+$/;	
	$tc->flag('eid', $oid) if $o->{eid} && $o->{eid} !~ /^rei:\d+$/;
	$tc->flag('tna', $oid) unless $o->{tna} =~ /\w/;
	$tc->flag('rnk', $oid) unless $o->{rnk} && $o->{rnk} =~ /^\d+$/;
	$tc->flag('tna/rnk', $oid) if $o->{rnk} && $o->{rnk} < 5 && $o->{tna} !~ /\w [\w(]/;
	$tc->flag('oei', $oid) unless $o->{oei} && $INTERVAL_NAME{$o->{oei}};
	$tc->flag('eag', $oid) unless defined $o->{eag} && $o->{eag} =~ /^\d+$|^\d+[.]\d+$/;
	$tc->flag('lag', $oid) unless defined $o->{lag} && $o->{lag} =~ /^\d+$|^\d+[.]\d+$/;
	$tc->flag('rid', $oid) unless $o->{rid} && $o->{rid} =~ /^ref:\d+$/;
	$tc->flag('iid', $oid) if $o->{iid} && $o->{iid} !~ /^(var|txn):\d+$/;
	$tc->flag('iid/idn', $oid) if $o->{iid} && ! ( $o->{idn} && $o->{idn} =~ /\w/ );
	
	$tc->flag('nsp', $oid) if $o->{idn} && $o->{idn} =~ /n\. sp\./;
	$tc->flag('ngen', $oid) if $o->{idn} && $o->{idn} =~ /n\. gen\./;
	$tc->flag('cf', $oid) if $o->{idn} && $o->{idn} =~ /cf\./;
    }
    
    $tc->expect('nsp', 'ngen', 'cf');
    
    $tc->ok_all("json records have proper values");
    
    my @o2 = $T->fetch_records("/occs/list.txt?base_name=$NAME_1", "occs list txt '$NAME_1'");
    
    $SAVE_OCCS_2 = \@o2;
    
    if ( cmp_ok( @o2, '==', @o1, "occs list txt fetches same number of records" ) )
    {
	foreach my $i ( 0..$#o2 )
	{
	    my $o1 = $o1[$i];
	    my $o2 = $o2[$i];
	    my $oid = $o2->{occurrence_no};
	    
	    $tc->flag('occurrence_no', $oid) unless $o2->{occurrence_no} &&
		"occ:" . $o2->{occurrence_no} eq $o1->{oid};
	    $tc->flag('collection_no', $oid) unless $o2->{collection_no} &&
		"col:" . $o2->{collection_no} eq $o1->{cid};
	    $tc->flag('reference_no', $oid) unless $o2->{reference_no} &&
		"ref:" . $o2->{reference_no} eq $o1->{rid};
	    $tc->flag('record_type', $oid) unless $o2->{record_type} && $o2->{record_type} eq 'occ';
	    
	    $tc->flag('reid_no/eid', $oid) if $o2->{reid_no} xor $o1->{eid};
	    
	    $tc->flag('reid_no', $oid) if $o2->{reid_no} && $o1->{eid} &&
		"rei:" . $o2->{reid_no} ne $o1->{eid};
	    $tc->flag('accepted_no', $oid) unless $o2->{accepted_no} &&
		"txn:" . $o2->{accepted_no} eq $o1->{tid};
	    $tc->flag('identified_no', $oid) if $o2->{identified_no} && $o1->{iid} &&
		not("txn:" . $o2->{identified_no} eq $o1->{iid} || "var:" . $o2->{identified_no} eq $o1->{iid});
	    
	    $tc->flag('difference', $oid) if $o2->{difference} ne ( $o1->{tdf} || '' );
	    
	    $tc->flag('accepted_name', $oid) if $o2->{accepted_name} ne $o1->{tna};
	    $tc->flag('identified_name/idn', $oid) if $o2->{identified_name} ne ( $o1->{idn} || $o1->{tna} );
	    
	    $tc->flag('nsp', $oid) if $o2->{identified_name} && $o2->{identified_name} =~ /n\. sp\./;
	    $tc->flag('ngen', $oid) if $o2->{identified_name} && $o2->{identified_name} =~ /n\. gen\./;
	    $tc->flag('cf', $oid) if $o2->{identified_name} && $o2->{identified_name} =~ /cf\./;
	    
	    $tc->flag('accepted_rank', $oid) if $o2->{accepted_rank} ne $TaxonDefs::RANK_STRING{$o1->{rnk}};
	    $tc->flag('identified_rank', $oid) if $o2->{identified_rank} ne $o2->{accepted_rank} &&
		$o2->{identified_rank} ne ($TaxonDefs::RANK_STRING{$o1->{idr}} || '');
	    
	    $tc->flag('early_interval', $oid) if $o2->{early_interval} ne $o1->{oei};
	    $tc->flag('late_interval', $oid) if $o2->{late_interval} ne $o2->{early_interval} && 
		$o2->{late_interval} ne ( $o1->{oli} || '' );
	    $tc->flag('max_ma', $oid) if $o2->{max_ma} ne $o1->{eag};
	    $tc->flag('min_ma', $oid) if $o2->{min_ma} ne $o1->{lag};
	}
    }
    
    $tc->ok_all("txt records have proper values");
};


# Make sure that we can retrieve single records and multiple records by
# occurrence identifiers, and that the occs/single and occs/list operations
# return identical records.

subtest 'single and list by occ_id' => sub {

    select_subtest || return;
    
    my $NAME_1 = $SAVE_NAME_1;
    
    unless ( $SAVE_OCCS_1 && @$SAVE_OCCS_1 )
    {
	fail("no records from previous subtest");
	diag("skipping remainder of subtest");
	return;
    }
    
    # Now go through the list of occurrences and extract keys 'oid', 'cid' and
    # 'lv3'.  For 'oid', save a reference to each record under its 'oid' key.
    # Note that we have already checked that these keys are actually defined
    # for each record in the subtest 'subtree basic'.  Also extract the first
    # three keys from each field that end in '4', for use in tests below.
    
    my (%o1_oid, @sample_oid);
    
    foreach my $r ( @$SAVE_OCCS_1 )
    {
	$o1_oid{$r->{oid}} = $r;
	push @sample_oid, $r->{oid} if @sample_oid < 3 && $r->{oid} =~ /4$/;
    }
    
    $SAVE_BYOCC_1 = \%o1_oid;
    
    ok( @sample_oid == 3, "occs list json found at least 3 oids ending in '4'" );
    
    my $OID_LIST = join(',', @sample_oid);
    my %OID_HASH = map { $_ => 1 } @sample_oid;
    
    my @o2a = $T->fetch_records("/occs/list.json?occ_id=$OID_LIST&show=bin", "list occs by 'occ_id'");
    
    cmp_ok( @o2a, '==', 3, "found 3 identifiers with 'occ_id'" );
    
    my %oid_2a = $T->extract_values( \@o2a, 'oid' );
    
    my $tc = Test::Conditions->new;
    
    foreach my $r ( @o2a )
    {
	$oid_2a{$r->{oid}} = 1;
	$tc->flag('record did not match', $r->{oid}) unless defined $o1_oid{$r->{oid}} &&
	    eq_deeply($r, $o1_oid{$r->{oid}});
    }
    
    $tc->ok_all("list occs by 'occ_id' found matching records");
    $T->cmp_sets_ok( \%oid_2a, '==', \%OID_HASH, "list occs by 'occ_id' found proper records" );
    
    # Do the same thing again, but with the parameter 'id' instead of
    # 'occ_id'. 
    
    my %oid_2b = $T->fetch_record_values("/occs/list.json?id=$OID_LIST&show=bin", 'oid', "list occs by 'id'");
    
    $T->cmp_sets_ok( \%oid_2b, '==', \%OID_HASH, "list occs by 'id' found proper records" );
    
    # Now try again but with numeric ids instead of extended ones.

    my $NUM_LIST = $OID_LIST;
    $NUM_LIST =~ s/occ://g;

    my %oid_2c = $T->fetch_record_values("/occs/list.json?id=$NUM_LIST&show=bin", 'oid', "list occs by numeric id");
    
    $T->cmp_sets_ok( \%oid_2c, '==', \%OID_HASH, "list occs by 'id' found proper records" );

    # Now fetch each record individually using 'occs/single', using both
    # extended and numeric id and both parameter names.
    
    my (%oid_3a, %oid_3b, %oid_3c);
    
    foreach my $id ( @sample_oid )
    {
	my $num = $id; $num =~ s/occ://;
	
	my ($ra) = $T->fetch_records("/occs/single.json?id=$id&show=bin", "single occ by 'id=$id'");
	my ($rb) = $T->fetch_records("/occs/single.json?occ_id=$id&show=bin", "single occ by 'occ_id=$id'");
	my ($rc) = $T->fetch_records("/occs/single.json?occ_id=$num&show=bin", "single occ by 'occ_id=$num'");
	
	$oid_3a{$ra->{oid}} = 1;
	$oid_3b{$rb->{oid}} = 1;
	$oid_3c{$rc->{oid}} = 1;
    }

    $T->cmp_sets_ok( \%oid_3a, '==', \%OID_HASH, "single occs by 'id' found proper records" );
    $T->cmp_sets_ok( \%oid_3b, '==', \%OID_HASH, "single occs by 'occ_id' found proper records" );
    $T->cmp_sets_ok( \%oid_3c, '==', \%OID_HASH, "single occs by numeric id found proper records" );

    # Finally, try the same thing with a text response.

    my @o4a = $T->fetch_records("/occs/list.txt?id=$NUM_LIST", "list occs txt by id");

    foreach my $r ( @o4a )
    {
	$tc->flag('no matching oid found', $r->{occurrence_no})
	    unless $OID_HASH{'occ:' . $r->{occurrence_no}};
    }

    $tc->ok_all("list occs txt by id found proper records");
};


# Make sure that we can list by collection and cluster id.

subtest 'list by coll_id and clust_id' => sub {

    select_subtest || return;
    
    my $NAME_1 = $SAVE_NAME_1;
    my $NAME_2 = 'Canis latrans';
    
    unless ( $SAVE_OCCS_1 && @$SAVE_OCCS_1 )
    {
	fail("no records from previous subtest");
	diag("skipping remainder of subtest");
	return;
    }

    # Go through the record set from subtest 'subtree basic' and look for
    # collections that contain an occurrence of $NAME_2 and at least one other
    # taxon.
    
    my %coll_taxa;
    my %coll_occs;
    my %clust_occs;
    my %multi_taxa;
    
    foreach my $r ( @$SAVE_OCCS_1 )
    {
	next unless $r->{cid} && $r->{tna};
	$coll_taxa{$r->{cid}}{$r->{tna}} = 1;
	$coll_occs{$r->{cid}}{$r->{oid}} = 1;
	
	$multi_taxa{$r->{cid}} = 1 if keys %{$coll_taxa{$r->{cid}}} > 1;

	$clust_occs{$r->{lv1}}{$r->{oid}} = 1 if $r->{lv1};
	$clust_occs{$r->{lv2}}{$r->{oid}} = 1 if $r->{lv2};
	$clust_occs{$r->{lv3}}{$r->{oid}} = 1 if $r->{lv3};
    }
    
    my @test_colls = keys %multi_taxa;
    
    # The list @test_colls now lists collections that have more than one
    # occurrence from Canis.  We can fetch by collection id and check to make
    # sure that we get the same set of occurrence numbers.  We do this by
    # setting %test_oids to the union of the oid sets for the first three
    # collections in our list.
    
    my $COLL_LIST = join(',', @test_colls[0..2]);
    my %test_oids;

    foreach my $cid ( @test_colls[0..2] )
    {
	foreach my $oid ( keys %{$coll_occs{$cid}} )
	{
	    $test_oids{$oid} = 1;
	}
    }
    
    my %o1a = $T->fetch_record_values("/occs/list.json?base_name=$NAME_1&coll_id=$COLL_LIST", 'oid',
				      "list occs by coll_id");

    $T->cmp_sets_ok( \%o1a, '==', \%test_oids, "list occs by coll_id finds proper occs" );

    # Now try again with numeric ids

    my $NUM_LIST = $COLL_LIST; $NUM_LIST =~ s/col://g;

    my %o1b = $T->fetch_record_values("/occs/list.json?base_name=$NAME_1&coll_id=$NUM_LIST", 'oid',
				      "list occs by coll_id numeric");

    $T->cmp_sets_ok( \%o1b, '==', \%test_oids, "list occs by coll_id numeric finds proper occs" );

    # Then check that coll_id + taxon_name produces exactly one result.
    
    my @sample_colls = grep { $coll_taxa{$_}{$NAME_2} } @test_colls;
    my $SAMPLE_COLL = $sample_colls[0];
    
    my @o1c = $T->fetch_records("/occs/list.json?taxon_name=$NAME_2&coll_id=$SAMPLE_COLL",
				"list occs by taxon_name and coll_id");

    cmp_ok( @o1c, '==', 1, "list occs by taxon_name and coll_id found one occ" );
    is( $o1c[0]{tna}, $NAME_2, "occ had proper taxon name" );
    is( $o1c[0]{cid}, $SAMPLE_COLL, "occ had proper collection id" );
    
    # Now pick a sample cluster id at each of the three levels, and check that
    # we find all of the proper occurrences.
    
    my ($CLUST1) = grep { /^clu:1/ } keys %clust_occs;
    my ($CLUST2) = grep { /^clu:2/ } keys %clust_occs;
    my ($CLUST3a, $CLUST3b) = grep { /^clu:3/ } keys %clust_occs;
    
    my %o2a = $T->fetch_record_values("/occs/list.json?base_name=$NAME_1&clust_id=$CLUST1",
				      'oid', "list occs by clust_id lvl 1");
    
    my %o2b = $T->fetch_record_values("/occs/list.json?base_name=$NAME_1&clust_id=$CLUST2",
				      'oid', "list occs by clust_id lvl 2");
    
    my %o2c = $T->fetch_record_values("/occs/list.json?base_name=$NAME_1&clust_id=$CLUST3a ,$CLUST3b",
				      'oid', "list occs by clust_id lvl 3");
    
    $T->cmp_sets_ok( \%o2a, '==', $clust_occs{$CLUST1}, "list occs by clust_id lvl 1 found proper occs" );
    $T->cmp_sets_ok( \%o2b, '==', $clust_occs{$CLUST2}, "list occs by clust_id lvl 2 found proper occs" );
    
    my %clust3 = ( %{$clust_occs{$CLUST3a}}, %{$clust_occs{$CLUST3b}} );
    
    $T->cmp_sets_ok( \%o2c, '==', \%clust3, "list occs by clust_id lvl 3 found proper occs" );

    my $CLUST_N1 = $CLUST1; $CLUST_N1 =~ s/clu://;
    
    my %o3a = $T->fetch_record_values("/occs/list.json?base_name=$NAME_1&clust_id=$CLUST_N1",
				      'oid', "list occs by clust_id numeric");

    $T->cmp_sets_ok( \%o3a, '==', \%o2a, "list occs by clust_id numeric found proper occs" );
};


# Check for proper responses to bad arguments

subtest 'single and list with bad arguments' => sub {

    select_subtest || return;
    
    my $m1a = $T->fetch_nocheck("/occs/list.json?limit=10", "list no arguments except limit");
    my $m1b = $T->fetch_nocheck("/occs/list.json?all_records&limit=10", "list all_records and limit");
    
    unless ( $m1a && $T->get_response_code($m1a) ne '500' )
    {
	fail("bad response from server");
	diag("skipping remainder of subtest");
	return;
    }
    
    $T->ok_response_code($m1a, '400', "list no arguments except limit got 400 response");
    $T->ok_response_code($m1b, '200', "list all_records & limit got 200 response");
    
    my $m2a = $T->fetch_nocheck("/occs/list.json?id=abc,def", "list bad ids");
    my $m2b = $T->fetch_nocheck("/occs/list.json?id=6", "list not found");
    my $m2c = $T->fetch_nocheck("/occs/list.json?id=col:1003", "list wrong id type");
    
    $T->ok_response_code($m2a, '200', "list bad ids got 200 response");
    $T->ok_warning_like($m2a, qr{each value of 'id'}i, "list bad ids got proper warning");
    $T->ok_warning_like($m2a, qr{no valid .*identifier}i, "list bad ids got second warning");
    $T->ok_response_code($m2b, '200', "list not found got 200 response");
    $T->ok_warning_like($m2b, qr{unknown occurrence}i, "list not found got proper warning");
    $T->ok_warning_like($m2b, qr{no valid .*identifier}i, "list not found got second warning");
    $T->ok_response_code($m2c, '200', "list wrong id type got 200 response");
    $T->ok_warning_like($m2c, qr{identifier of type occ}i, "list wrong id type got proper warning");
    $T->ok_warning_like($m2c, qr{no valid .*identifier}i, "list wrong id type got second warning");
    
    my $m3a = $T->fetch_nocheck("/occs/list.json?coll_id=abc", "list bad coll ids");
    my $m3b = $T->fetch_nocheck("/occs/list.json?clust_id=abc", "list bad cluster ids");
    
    $T->ok_response_code($m3a, '200', "list bad coll ids got 200 response");
    $T->ok_warning_like($m3a, qr{no valid .*identifier}i, "list bad coll ids got proper warning");
    $T->ok_response_code($m3b, '200', "list bad cluster ids got 200 response");
    $T->ok_warning_like($m3b, qr{no valid .*identifier}i, "list bad cluster ids got proper warning");
    
    my $m4a = $T->fetch_nocheck("/occs/single.json", "single no arguments");
    my $m4b = $T->fetch_nocheck("/occs/single.json?id=abc", "single bad id");
    my $m4c = $T->fetch_nocheck("/occs/single.json?id=6", "single not found");

    $T->ok_response_code($m4a, '400', "single no arguments got 400 response");
    $T->ok_error_like($m4a, qr{you must specify .*occurrence identifier}i,
		      "single no arguments got proper error");
    $T->ok_response_code($m4b, '400', "single bad id got 400 response");
    $T->ok_error_like($m4b, qr{each value of 'id'}i, "single bad id got proper error");
    $T->ok_response_code($m4c, '404', "single not found got 404 response");
    $T->ok_error_like($m4c, qr{not found}i, "single not found got proper error");
};


# Check the match_name parameter

subtest 'match_name' => sub {

    select_subtest || return;
    
    my $NAME_1 = 'Canis %';
    
    # We first fetch all matching occurrences, and count how many occurrences
    # correspond to each distinct taxon name.
    
    my @o1a = $T->fetch_records("/occs/list.txt?match_name=$NAME_1", "list occs match_name");
    
    unless ( @o1a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my %occs_by_ident;
    my %occs_by_acc;
    
    foreach my $r ( @o1a )
    {
	$occs_by_ident{$r->{identified_no}}++;
	$occs_by_acc{$r->{accepted_no}}++;
    }
    
    # We then fetch each matching taxon, and note the occurrence counts.

    my @t1a = $T->fetch_records("/taxa/list.txt?match_name=$NAME_1",
				"list taxa match_name");

    my %taxon_occ_count;
    my %taxon_diff;
    
    foreach my $r ( @t1a )
    {
	$taxon_occ_count{$r->{taxon_no}} = $r->{n_occs};
	$taxon_diff{$r->{taxon_no}} = $r->{difference};
    }
    
    # Now we compare the two sets of counts.
    
    my $tc = Test::Conditions->new;
    $tc->limit_max('missing occurrence', 5);
    
    foreach my $n ( keys %taxon_occ_count )
    {
	next if $taxon_diff{$n};
	next unless $taxon_occ_count{$n};
	
	$tc->flag('missing occurrence', $n) unless
	    ( $occs_by_ident{$n} && $occs_by_ident{$n} == $taxon_occ_count{$n} ||
	      $occs_by_acc{$n} && $occs_by_acc{$n} == $taxon_occ_count{$n} );
    }
    
    foreach my $n ( keys %occs_by_acc )
    {
    	$tc->flag('missing taxon record', $n) unless 
    	    $taxon_occ_count{$n} && $taxon_occ_count{$n} == $occs_by_acc{$n};
    }
    
    $tc->ok_all("occ counts match up");
};


# Run some checks on 'taxon_name' as well.  Make sure that all uppercase and
# all lowercase work properly too, and check that synonyms fetch the same list.

subtest 'taxon_name' => sub {
    
    select_subtest || return;
    
    my $NAME_1 = 'canis latrans';
    
    # First get all of the synonyms, and figure out which one is senior.
    # Lowercase it to make sure that this doesn't make a difference.
    
    my $senior;
    my @synonyms;
    
    my @t1 = $T->fetch_records("/taxa/list.json?name=$NAME_1&rel=synonyms",
			       "list synonyms");
    
    unless ( @t1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    foreach my $t ( @t1 )
    {
	if ( $t->{tdf} && $t->{tdf} =~ /synonym/ )
	{
	    push @synonyms, $t->{nam};
	}
	
	else
	{
	    $senior = $t->{nam};
	}
    }
    
    unless ( $senior )
    {
	fail("could not find senior synonym");
    }
    
    unless ( @synonyms )
    {
	fail("could not find junior synonyms");
    }
    
    my $lowercase = lc $senior;
    
    my @o1 = $T->fetch_records("/occs/list.json?taxon_name=$lowercase", "list senior");
    
    # Make sure all of the records actually list that taxon name.
    
    my $tc = Test::Conditions->new;
    
    foreach my $r ( @o1 )
    {
	$tc->flag('bad name', $r->{oid}) unless $r->{tna} && $r->{tna} eq $senior;
    }
    
    $tc->ok_all("list senior check names");
    
    my %o2 = $T->fetch_record_values("/occs/list.json?taxon_name=$synonyms[0]", 'oid',
				     "list one synonym");
    
    my %o1 = $T->extract_values( \@o1, 'oid' );
    
    $T->cmp_sets_ok( \%o2, '==', \%o1, "junior fetches same list as senior" );
};


# Check that we can list by base and exclusion both with names and ids, and
# that the results are identical.

subtest 'base and exclude' => sub {
    
    select_subtest || return;
    
    my $NAME_1 = 'Dascillidae';
    my $NAME_2 = 'Lyprodascillus';
    
    my ($t1) = $T->fetch_records("/taxa/single.json?name=$NAME_1", "fetch taxon '$NAME_1'");
    my ($t2) = $T->fetch_records("/taxa/single.json?name=$NAME_2", "fetch taxon '$NAME_2'");
    
    unless ( $t1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my $ID_1 = $t1->{oid};
    my $ID_2 = $t2->{oid};
    
    my (%o1a) = $T->fetch_record_values("/occs/list.json?base_name=$NAME_1^$NAME_2", 'oid',
					"list by base_name with ^");
    my (%o1b) = $T->fetch_record_values("/occs/list.json?base_name=$NAME_1&exclude_id=$ID_2", 'oid',
					"list by base_name with exclude_id");
    my (%o1c) = $T->fetch_record_values("/occs/list.json?base_id=$ID_1&exclude_id=$ID_2", 'oid',
					"list by base_id with exclude_id");
    
    my (%o2a) = $T->fetch_record_values("/occs/list.json?base_name=$NAME_1", 'oid',
					"list by base_name '$NAME_1'");
    my (%o2b) = $T->fetch_record_values("/occs/list.json?base_name=$NAME_2", 'oid',
					"list by base_name '$NAME_2'");

    $T->cmp_sets_ok( \%o1a, '==', \%o1b, "exclusion with ^ matches with exclude_id" );
    $T->cmp_sets_ok( \%o1a, '==', \%o1c, "exclusion with names matches exclusion with ids" );

    my %o2c = %o2a;
    delete $o2c{$_} foreach keys %o2b;

    $T->cmp_sets_ok( \%o1a, '==', \%o2c, "subtraction of excluded subtree matches direct exclusion" );
};


# Now test the parameter 'immediate'.  This should only find occurrences
# identified as taxa that are immediately contained in the specified base
# name, rather than all taxa that are contained in the senior synonym of the
# specified name.

subtest 'immediate' => sub {
    
    select_subtest || return;
    
    my $NAME_1 = 'Mastodonsauridae';
    
    # First, find all synonyms of this name.  Look for the senior.
    
    my @syn = $T->fetch_records("/taxa/list.json?name=$NAME_1&rel=synonyms", "list synonyms");
    
    unless ( @syn )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my ($senior, @juniors);
    
    foreach my $t ( @syn )
    {
	if ( $t->{tdf} && $t->{tdf} =~ /synonym/ )
	{
	    push @juniors, $t->{nam};
	}
	
	elsif ( $t->{tdf} )
	{
	    diag("WARNING: name '$t->{nam}' had a tdf of '$t->{tdf}'");
	}
	
	else
	{
	    $senior = $t->{nam};
	}
    }
    
    # Then fetch the set of occurrence ids by the senior name and for one of the
    # juniors.  These should be equal.
    
    my %oid_senior = $T->fetch_record_values("/occs/list.json?base_name=$senior", 'oid', "full list");
    my %oid_junior = $T->fetch_record_values("/occs/list.json?base_name=$juniors[0]", 'oid', "full list b");
    
    $T->cmp_sets_ok( \%oid_senior, '==', \%oid_junior, "junior and senior fetch same occs" );
    
    # Now compute the union of the occurrence ids of each synonym with
    # 'immediate'.  This should be equal to either of the above sets.
    
    my %oid_immediate;
    
    foreach my $n ( $senior, @juniors )
    {
	my %oid = $T->fetch_record_values("/occs/list.json?base_name=$n&immediate", 'oid',
					  "list immediate '$n'");
	$oid_immediate{$_} = 1 foreach keys %oid;
    }
    
    $T->cmp_sets_ok( \%oid_immediate, '==', \%oid_senior, "senior and immediate union fetch same set" );
};


# Test the 'idtype' parameter.  This specifies how reidentified occurrences should be treated.

subtest 'idtype' => sub {
    
    select_subtest || return;
    
    my $NAME_1 = 'Disciniidae';
    
    my @occs_base = $T->fetch_records("/occs/list.json?base_name=$NAME_1", "list default");
    my %oid_base = $T->extract_values( \@occs_base, 'oid' );
    
    unless ( @occs_base )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my @occs_all = $T->fetch_records("/occs/list.json?base_name=$NAME_1&idtype=all", "list all");
    my %oid_all = $T->extract_values( \@occs_all, 'oid' );
    my %reid_all = $T->extract_values( \@occs_all, 'eid' );
    
    my @occs_orig = $T->fetch_records("/occs/list.json?base_name=$NAME_1&idtype=orig", "list orig");
    my %oid_orig = $T->extract_values( \@occs_orig, 'oid' );
    
    my @occs_reid = $T->fetch_records("/occs/list.json?base_name=$NAME_1&idtype=reid", "list reid");
    my %oid_reid = $T->extract_values( \@occs_reid, 'oid' );
    my %reid_reid = $T->extract_values( \@occs_reid, 'eid' );
    
    my @occs_latest = $T->fetch_records("/occs/list.json?base_name=$NAME_1&idtype=latest", "list latest");
    my %oid_latest = $T->extract_values( \@occs_latest, 'oid' );
    
    $T->cmp_sets_ok( \%oid_base, '==', \%oid_latest, "default = latest" );
    
    my %oid_union = ( %oid_orig, %oid_reid );
    
    $T->cmp_sets_ok( \%oid_union, '==', \%oid_all, "orig + reid_all = all" );
    
    $T->cmp_sets_ok( \%reid_all, '==', \%reid_reid, "reids: all = reid_all" );
    
    my $tc = Test::Conditions->new;
    
    foreach my $r ( @occs_base )
    {
	$tc->flag('found flag', $r->{oid}) if $r->{flg} && $r->{flg} =~ /R/;
    }
    
    $tc->ok_all("list default flags");
    
    $tc->expect('expect flag');
    
    foreach my $r ( @occs_all )
    {
	$tc->flag('expect flag', $r->{oid}) if $r->{flg} && $r->{flg} =~ /R/;
    }
    
    $tc->ok_all("list all flags");
    
    foreach my $r ( @occs_orig )
    {
	$tc->flag('expect flag', $r->{oid}) if $r->{flg} && $r->{flg} =~ /R/;
    }
    
    $tc->ok_all("list orig flags");
    
    foreach my $r ( @occs_reid )
    {
	$tc->flag('expect flag', $r->{oid}) if $r->{flg} && $r->{flg} =~ /R/;
    }
    
    $tc->ok_all("list reid flags");
    
    # Now test with 'match_name'.
    
    my $NAME_2 = 'vulpes vafer';
    
    my %o2_idn = $T->fetch_record_values("/occs/list.json?match_name=$NAME_2&idtype=all", 'idn',
					 "match_name with idtype=all");
    
    ok( $o2_idn{"Vulpes vafer"}, "match_name with idtype=all found old identification" );
    
};


# Test the parameters 'taxon_reso' and 'taxon_status'.

subtest 'taxon_reso and taxon_status' => sub {
    
    select_subtest || return;
    
    my $NAME_1 = 'Stegosauridae';
    
    # First, taxon_status.
    
    my @occs_base = $T->fetch_records("/occs/list.json?base_name=$NAME_1", "list base");
    
    unless ( @occs_base )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my @occs_invalid = $T->fetch_records("/occs/list.json?base_name=$NAME_1&taxon_status=invalid", 
					 "list invalid");
    
    my @occs_valid = $T->fetch_records("/occs/list.json?base_name=$NAME_1&taxon_status=valid",
					"list valid");
    
    my @occs_junior = $T->fetch_records("/occs/list.json?base_name=$NAME_1&taxon_status=junior",
					"list junior");
    
    my @occs_senior = $T->fetch_records("/occs/list.json?base_name=$NAME_1&taxon_status=senior",
					"list senior");
    
    cmp_ok( @occs_invalid + @occs_valid, '==', @occs_base, "valid + invalid = base" );
    cmp_ok( @occs_junior + @occs_senior, '==', @occs_valid, "junior + senior = valid" );
    
    my $tc = Test::Conditions->new;
    
    foreach my $r ( @occs_invalid )
    {
	$tc->flag('tdf', $r->{oid}) unless $r->{tdf} && $r->{tdf} =~ /nomen|invalid/i;
    }
    
    $tc->ok_all("taxon_status invalid");
    
    foreach my $r ( @occs_junior )
    {
	$tc->flag('tdf', $r->{oid}) unless $r->{tdf} && $r->{tdf} =~ /synonym|replaced/i;
    }
    
    $tc->ok_all("taxon_status junior");
    
    foreach my $r ( @occs_senior )
    {
	$tc->flag('tdf', $r->{oid}) if $r->{tdf} && $r->{tdf} =~ /nomen|invalid|synonym|replaced/i;
    }
    
    $tc->ok_all("taxon_status senior");
    
    # Then taxon_reso.
    
    my @occs_family = $T->fetch_records("/occs/list.json?base_name=$NAME_1&taxon_reso=family",
					"list family");
    
    my @occs_genus = $T->fetch_records("/occs/list.json?base_name=$NAME_1&taxon_reso=genus",
				       "list genus");
    
    my @occs_lumpgenus = $T->fetch_records("/occs/list.json?base_name=$NAME_1&taxon_reso=lump_genus",
					 "list lump_genus");
    
    my @occs_species = $T->fetch_records("/occs/list.json?base_name=$NAME_1&taxon_reso=species",
					 "list species");
    
    cmp_ok( @occs_family, '==', @occs_base, "family = base" );
    cmp_ok( @occs_genus, '<', @occs_base, "genus < base" );
    cmp_ok( @occs_lumpgenus, '<', @occs_genus, "lumpgenus < genus" );
    cmp_ok( @occs_species, '<', @occs_genus, "species < genus" );
    
    my (%lump_1a, %lump_1b);
    
    foreach my $r ( @occs_family )
    {
	$tc->flag('rnk_family', $r->{oid}) unless $r->{rnk} && $r->{rnk} <= 9;
    }
    
    foreach my $r ( @occs_genus )
    {
	$tc->flag('rnk_genus', $r->{oid}) unless $r->{rnk} && $r->{rnk} <= 5;
	
	my $genus = $r->{tna};
	my $lumpstr = "$r->{cid}/$genus";
	
	$tc->flag('no lumping', $r->{oid}) if $lump_1a{$lumpstr};
	$lump_1a{$lumpstr} = 1;
    }
    
    $tc->expect('no lumping');
    
    foreach my $r ( @occs_lumpgenus )
    {
	$tc->flag('rnk_lumpgenus', $r->{oid}) unless $r->{rnk} && $r->{rnk} == 5;
	$tc->flag('tna_lumpgenus', $r->{oid}) if $r->{tna} && $r->{tna} =~ /\w+ \w+/;
	
	my $lumpstr = "$r->{cid}/$r->{tna}";
	
	$tc->flag('lump duplicate', $r->{oid}) if $lump_1b{$lumpstr};
	$lump_1b{$lumpstr} = 1;
    }
    
    foreach my $r ( @occs_species )
    {
	$tc->flag('rnk_species', $r->{oid}) unless $r->{rnk} && $r->{rnk} <= 3;
	$tc->flag('tna_species', $r->{oid}) unless $r->{tna} && $r->{tna} =~ /\w+ \w+/;
    }
    
    $tc->ok_all("occs with taxon_reso");
    
    # We need to query a different range of taxa to check lumping by subgenus.
    
    my $NAME_2 = 'Terebratulidae';
    
    my @o2_base = $T->fetch_records("/occs/list.json?base_name=$NAME_2", "list base 2");
    
    my @o2_genus = $T->fetch_records("/occs/list.json?base_name=$NAME_2&taxon_reso=genus", 
				     "list genus 2");
    
    my @o2_lumpgenus = $T->fetch_records("/occs/list.json?base_name=$NAME_2&taxon_reso=lump_genus", 
					 "list lump_genus 2");
    
    my @o2_lumpgensub = $T->fetch_records("/occs/list.json?base_name=$NAME_2&taxon_reso=lump_gensub",
					  "list lump_gensub 2");
    
    cmp_ok( @o2_lumpgenus, '<', @o2_genus, "lump_genus < genus" );
    cmp_ok( @o2_lumpgensub, '<', @o2_genus, "lump_gensub < genus" );
    cmp_ok( @o2_lumpgensub, '>', @o2_lumpgenus, "lump_gensub > lump_genus" );
    
    my (%lump_2a, %lump_2b, %lump_2c);
    
    foreach my $r ( @o2_genus )
    {
	$tc->flag('rnk_genus', $r->{oid}) unless $r->{rnk} && $r->{rnk} <= 5;
	
	my $genus = $r->{tna}; $genus =~ s/ .*//;
	my $lumpstr = "$r->{cid}/$genus";
	
	$tc->flag('no lumping', $r->{oid}) if $lump_2a{$lumpstr};
	$lump_2a{$lumpstr} = 1;
    }
    
    foreach my $r ( @o2_lumpgenus )
    {
	$tc->flag('rnk_lumpgenus', $r->{oid}) unless $r->{rnk} && $r->{rnk} == 5;

	my $lumpstr = "$r->{cid}/$r->{tna}";
	
	$tc->flag('lump duplicate genus', $r->{oid}) if $lump_2b{$lumpstr};
	$lump_2b{$lumpstr} = 1;
    }
    
    foreach my $r ( @o2_lumpgensub )
    {
	$tc->flag('rnk_gensub', $r->{oid}) unless $r->{rnk} && $r->{rnk} <= 5 && $r->{rnk} >= 4;
	$tc->flag('rnk_subgenus', $r->{oid}) if $r->{rnk} == 4;
	$tc->flag('tna_subgenus', $r->{oid}) if $r->{tna} && $r->{tna} =~ qr{\(\w+\)};
	
	my $lumpstr = "$r->{cid}/$r->{tna}";
	
	$tc->flag('lump duplicate gensub', $r->{oid}) if $lump_2c{$lumpstr};
	$lump_2c{$lumpstr} = 1;
    }
    
    $tc->expect('rnk_subgenus', 'tna_subgenus');
    
    $tc->ok_all("occs with lumping by subgenus");    
};


# Next, check the parameters 'pres' and 'extant'.

subtest 'pres' => sub {
    
    select_subtest || return;
    
    # We need to check different groups of organisms, first check one that has
    # some form taxa.
    
    my $NAME_1 = 'Myrmecophagidae';
    
    my @occs_default = $T->fetch_records("/occs/list.json?base_name=$NAME_1&show=pres", 
					 "list default");
    
    my %oid_default = $T->extract_values( \@occs_default, 'oid' );
    
    unless ( @occs_default )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my @occs_all = $T->fetch_records("/occs/list.json?base_name=$NAME_1&pres=all&show=pres",
				     "list pres all");
    
    my %oid_all = $T->extract_values( \@occs_all, 'oid' );
    
    my @occs_form = $T->fetch_records("/occs/list.json?base_name=$NAME_1&pres=form&show=pres",
				      "list pres form");
    
    my %oid_form = $T->extract_values( \@occs_form, 'oid' );
    
    my @occs_regular = $T->fetch_records("/occs/list.json?base_name=$NAME_1&pres=regular&show=pres",
					 "list pres regular");
    
    my %oid_regular = $T->extract_values( \@occs_regular, 'oid' );
    
    $T->cmp_sets_ok( \%oid_default, '==', \%oid_all, "default = all" );
    cmp_ok( @occs_form + @occs_regular, '==', @occs_all, "form + regular = all" );
    
    my $tc = Test::Conditions->new;
    my %oid_check;
    
    foreach my $r ( @occs_all )
    {
	if ( $r->{flg} && $r->{flg} =~ /F/ )
	{
	    $tc->flag('expect_form', $r->{oid});
	    $oid_check{$r->{oid}} = 1;
	} 
	
	else
	{
	    $tc->flag('expect_none', $r->{oid});
	}
    }
    
    foreach my $r ( @occs_form )
    {
	$tc->flag('form_flg', $r->{oid}) unless $r->{flg} && $r->{flg} =~ /F/;
    }
    
    foreach my $r ( @occs_regular )
    {
	$tc->flag('regular_flg', $r->{oid}) if $r->{flg} && $r->{flg} =~ /[FI]/;
    }
    
    $tc->expect('expect_form', 'expect_none');
    
    $tc->ok_all("proper flags 1");
    
    $T->cmp_sets_ok( \%oid_check, '==', \%oid_form, "form occs match up" );
    
    # Now check a second group that has ichnotaxa as well.
    
    my $NAME_2 = 'Batrachopodidae';
    
    my (%o2_check_form, %o2_check_ichno, %o2_check_regular);
    
    my @o2_all = $T->fetch_records("/occs/list.json?base_name=$NAME_2&pres=all&show=pres",
				   "list pres all 2");
    
    my @o2_form = $T->fetch_records("/occs/list.json?base_name=$NAME_2&pres=form&show=pres",
				    "list pres form 2");
    
    my @o2_ichno = $T->fetch_records("/occs/list.json?base_name=$NAME_2&pres=ichno&show=pres",
				     "list pres ichno 2");
    
    my @o2_regular = $T->fetch_records("/occs/list.json?base_name=$NAME_2&pres=regular&show=pres",
				       "list pres regular 2");
    
    foreach my $r ( @o2_all )
    {
	if ( $r->{flg} && $r->{flg} =~ /I/ )
	{
	    $tc->flag('expect_ichno', $r->{oid});
	    $o2_check_ichno{$r->{oid}} = 1;
	}
	
	if ( $r->{flg} && $r->{flg} =~ /F/ )
	{
	    $tc->flag('expect_form', $r->{oid});
	    $o2_check_form{$r->{oid}} = 1;
	}
	
	if ( ! $r->{flg} || $r->{flg} !~ /[FI]/ )
	{
	    $tc->flag('expect_none', $r->{oid});
	    $o2_check_regular{$r->{oid}} = 1;
	}
    }
    
    $tc->expect('expect_ichno');
    
    my %o2_form = $T->extract_values( \@o2_form, 'oid' );
    my %o2_ichno = $T->extract_values( \@o2_ichno, 'oid' );
    my %o2_regular = $T->extract_values( \@o2_regular, 'oid' );
    
    foreach my $r ( @o2_form )
    {
	$tc->flag('flg_form', $r->{oid}) unless $r->{flg} && $r->{flg} =~ /F/;
    }
    
    foreach my $r ( @o2_ichno )
    {
	$tc->flag('flg_ichno', $r->{oid}) unless $r->{flg} && $r->{flg} =~ /I/;
    }
    
    foreach my $r ( @o2_regular )
    {
	$tc->flag('flg_regular', $r->{oid}) if $r->{flg} && $r->{flg} =~ /[IF]/;
    }
    
    $tc->ok_all("proper flags 2");
    
    $T->cmp_sets_ok( \%o2_form, '==', \%o2_check_form, "form occs match up" );
    $T->cmp_sets_ok( \%o2_ichno, '==', \%o2_check_ichno, "ichno occs match up" );
    $T->cmp_sets_ok( \%o2_regular, '==', \%o2_check_regular, "regular occs match up" );
};


subtest 'extant' => sub {

    select_subtest || return;
    
    my $NAME_1 = 'Dascillidae';
    
    my @o1a = $T->fetch_records("/occs/list.json?base_name=$NAME_1", "list base");
    
    unless ( @o1a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my @o1b = $T->fetch_records("/occs/list.json?base_name=$NAME_1&extant=yes",
				"list extant=yes");
    
    my @o1c = $T->fetch_records("/occs/list.json?base_name=$NAME_1&extant=no",
				"list extant=no");
    
    cmp_ok( @o1b, '<', @o1a, "extant yes < all" );
    cmp_ok( @o1c, '<', @o1a, "extant no < all" );
    
    # my $tc = Test::Conditions->new;
    
    # foreach my $r ( @o1a )
    # {
    # 	$tc->flag('expect_yes', $r->{oid}) if defined $r->{ext} && $r->{ext} eq '1';
    # 	$tc->flag('expect_no', $r->{oid}) if defined $r->{ext} && $r->{ext} eq '0';
    # }
    
    # foreach my $r ( @o1b )
    # {
    # 	$tc->flag('ext_yes', $r->{oid}) unless defined $r->{ext} && $r->{ext} eq '1';
    # }
    
    # foreach my $r ( @o1c )
    # {
    # 	$tc->flag('ext_no', $r->{oid}) unless defined $r->{ext} && $r->{ext} eq '0';
    # }
    
    # $tc->expect('expect_yes', 'expect_no');
    
    # $tc->ok_all("ext values");
};


subtest 'interval' => sub {
    
    select_subtest || return;
    
    my $NAME_1 = 'Canis';
    my $INT_1 = 'CALABRIAN';
    
    my @o1_contain = $T->fetch_records("/occs/list.json?base_name=$NAME_1&interval=$INT_1&timerule=contain",
				       "list interval contain");
    
    check_result( @o1_contain ) || return;
    
    my @o1_major = $T->fetch_records("/occs/list.json?base_name=$NAME_1&interval=$INT_1&timerule=major",
				     "list interval major");
    
    my @o1_buffer = $T->fetch_records("/occs/list.json?base_name=$NAME_1&interval=$INT_1&timerule=buffer&timebuffer=1",
				      "list interval buffer");
    
    my @o1_overlap = $T->fetch_records("/occs/list.json?base_name=$NAME_1&interval=$INT_1&timerule=overlap",
				       "list interval overlap");
    
    cmp_ok( @o1_major, '>', @o1_contain, "major > contain" );
    cmp_ok( @o1_buffer, '>', @o1_major, "buffer > major" );
    cmp_ok( @o1_overlap, '>', @o1_buffer, "major < overlap" );
    
    # cmp_ok( @o1_default, '==', @o1_major, "default = major" );
    
    my @o2_buffer = $T->fetch_records("/occs/list.json?base_name=$NAME_1&timerule=buffer&earlybuffer=5&latebuffer=1",
				      "list interval earlybuffer latebuffer");
    
    cmp_ok( @o2_buffer, '>', @o1_buffer, "expanded buffer increases result size");
    
    my @o3_buffer = $T->fetch_records("/occs/list.json?base_name=$NAME_1&timerule=buffer&timebuffer=5&latebuffer=1",
				      "list interval timebuffer latebuffer");
    
    cmp_ok( @o3_buffer, '==', @o2_buffer, "timebuffer same as earlybuffer" );
    
    my @o4_buffer = $T->fetch_records("/occs/list.json?base_name=$NAME_1&time_rule=buffer&time_buffer=5&late_buffer=1",
				      "list interval with underscores");
    
    my ($i1) = $T->fetch_records("/intervals/single.json?name=$INT_1", "single interval");
    
    return unless $i1;
    
    my $ID_1 = $i1->{oid};
    
    my @o5 = $T->fetch_records("/occs/list.json?base_name=$NAME_1&interval_id=$ID_1",
			       "list interval id default timerule");
    
    cmp_ok( @o5, '==', @o1_major, "list by interval id = interval by name default timerule" );
    
    my $MAX_1 = $i1->{eag};
    my $MIN_1 = $i1->{lag};
    
    my @o6 = $T->fetch_records("/occs/list.json?base_name=$NAME_1&max_ma=$MAX_1&min_ma=$MIN_1",
			       "list interval by max and min ma");
    
    cmp_ok( @o6, '==', @o1_major, "list by max and min ma = list by interval default timerule" );
    
    my @o7 = $T->fetch_records("/occs/list.json?base_name=$NAME_1&max_ma=$MAX_1",
			       "list interval by max only");
    
    cmp_ok( @o7, '>', @o1_major, "list by max only finds more records");
    
    # Check to make sure that 'interval' and 'interval_id' are okay as only
    # parameters
    
    my @o8a = $T->fetch_records("/occs/list.json?interval=$INT_1&limit=10", "list '$INT_1' limit 10");
    
    cmp_ok( @o8a, '==', 10, "found 10 records by interval name" );
    
    my @o8b = $T->fetch_records("/occs/list.json?interval_id=$ID_1&limit=10", "list '$ID_1' limit 10");
    
    cmp_ok( @o8b, '==', 10, "found 10 records by interval id" );
    
    # Check for bad parameters
    
    my @o9a = $T->fetch_nocheck("/occs/list.json?interval=bad&limit=10", "list bad interval name");
    
    $T->ok_response_code("400", "list bad interval got 400 response");
    $T->ok_error_like(qr{unknown interval}i, "list bad interval name got proper error");
    
    my @o9b = $T->fetch_nocheck("/occs/list.json?interval_id=bad&limit=10", "list bad interval id");
    
    $T->ok_response_code("400", "list bad interval id got 400 response");
    $T->ok_error_like(qr{no valid interval}i, "list bad interval id got proper error");
    
    my @o9c = $T->fetch_nocheck("/occs/list.json?max_ma=foo&limit=10", "list bad max_ma");
    
    $T->ok_response_code("400", "list bad max_ma got 400 response");
    $T->ok_error_like(qr{bad value}i, "list bad max_ma got proper error");
    
    my @o9d = $T->fetch_nocheck("/occs/list.json?max_ma=0&limit=10", "list max_ma = 0");
    
    $T->ok_response_code("400", "list max_ma = 0 got 400 response");
    $T->ok_error_like(qr{must be greater}i, "list max_ma = 0 got proper error");
    
    my @o9e = $T->fetch_nocheck("/occs/list.json?min_ma=foo&limit=10", "list bad min_ma");

    $T->ok_response_code("400", "list bad min_ma got 400 response");
    $T->ok_error_like(qr{bad value}i, "list bad min_ma got proper error");
    
    my @o9f = $T->fetch_records("/occs/list.json?min_ma=0&limit=10", "list min_ma = 0",
			    { no_records_ok => 1 });
    
    $T->ok_response_code("200", "list min_ma = 0 got 200 response" );
    cmp_ok( @o9f, '==', 10, "list min_ma = 0 with limit 10 got 10 records" );
};


# Test the parameters for listing occurrences by created/modified dates.  This will generally be
# used to show the most recently entered taxa.

subtest 'crmod and all_records' => sub {
    
    select_subtest || return;
    
    my $NAME_1 = 'Lingulata';
    my $COUNT_1 = '50';
    my $COUNT_2 = '100';
    
    my @o1a = $T->fetch_records("/occs/list.json?base_name=$NAME_1&order=created&show=crmod&limit=$COUNT_1",
				"latest $COUNT_1 taxa from '$NAME_1'");
    
    check_result( @o1a ) || return;
    
    cmp_ok( @o1a, '==', $COUNT_1, "latest $COUNT_1 taxa from '$NAME_1' found $COUNT_1 records" );
    $T->check_order( \@o1a, 'dcr', 'ge', 'oid', "latest $COUNT_1 taxa from '$NAME_1'" );
    
    my @o2a = $T->fetch_records("/occs/list.json?all_records&order=modified&show=crmod&limit=$COUNT_2",
				"latest $COUNT_2 taxa");
    
    cmp_ok( @o2a, '==', $COUNT_2, "latest $COUNT_2 taxa found $COUNT_2 records" );
    $T->check_order( \@o2a, 'dmd', 'ge', 'oid', "latest $COUNT_2 taxa" );
    
    my @o2b = $T->fetch_records("/occs/list.json?all_records&order=identification&occs_created_after=2003" .
				"&occs_created_before=2003-01-07&show=crmod",
				"created before 2003");
    
    # Correct for the case where 'tna' is not returned since it is identical
    # to 'idn'.
    
    foreach my $r ( @o2b )
    {
	$r->{tna} ||= $r->{idn};
    }
    
    $T->check_order( \@o2b, 'tna', 'le', 'oid', "created before 2003 finds proper record order" );
    
    my $tc = Test::Conditions->new;
    
    foreach my $r ( @o2b )
    {
	$tc->flag('dcr', $r->{oid}) unless $r->{dcr} && $r->{dcr} ge '2003-01-01' &&
	    $r->{dcr} lt '2003-01-07';
    }
    
    $tc->ok_all("created dates are in proper range");
    
    my @o2c = $T->fetch_records("/occs/list.json?all_records&occs_modified_after=2014&show=crmod&limit=100",
				"modified after 2014");
    
    cmp_ok( @o2c, '==', 100, "modified after 2014 finds proper number of records" );
    
    foreach my $r ( @o2c )
    {
	$tc->flag('dmd', $r->{oid}) unless $r->{dmd} && $r->{dmd} ge "2014" && 
	    $r->{dmd} =~ /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/;
    }
    
    $tc->ok_all("modified dates are in proper range");
};


# Check that authorizers, enterers and modifiers are reported properly, and that we can select taxa using
# names and identifiers.

subtest 'authent' => sub {
    
    select_subtest || return;
    
    my $NAME_1 = 'Felidae';
    
    # First fetch a set of base records regardless of authorizer, enterer or modifier.
    
    my @o1 = $T->fetch_records("/occs/list.json?base_name=$NAME_1&show=ent,entname", "base records");
    
    check_result( @o1 ) || return;
    
    # Make sure that the name and id fields all have proper values.
    
    my $tc = Test::Conditions->new;
    
 RECORD:
    foreach my $r ( @o1 )
    {
	foreach my $k ( 'ati', 'eni', 'mdi' )
	{
	    next if $k eq 'mdi' && ! defined $r->{mdi};
	    
	    $tc->flag($k, $r->{nam}) unless defined $r->{$k} && $r->{$k} =~ /^prs:\d+$/;
	}
	
	foreach my $k ( 'ath', 'ent', 'mdf' )
	{
	    next if $k eq 'mdf' && ! defined $r->{mdf};
	    
	    $tc->flag($k, $r->{nam}) unless defined $r->{$k} && $r->{$k} ne '' && $r->{$k} !~ /\d/;
	}
    }
    
    $tc->ok_all("base records all have proper authent names and ids");
    
    # Find the person who has authorized the most records, then the non-authorizer who has entered
    # the most records, then the non-enterer who has modified the most records.  Do this for both
    # names and ids.
    
    my %ati = $T->count_values( \@o1, 'ati' );
    my %eni = $T->count_values( \@o1, 'eni' );
    my %mdi = $T->count_values( \@o1, 'mdi' );
    my %ath = $T->count_values( \@o1, 'ath' );
    my %ent = $T->count_values( \@o1, 'ent' );
    my %mdf = $T->count_values( \@o1, 'mdf' );
    
    my ($ath_max, $ath_count) = $T->find_max( \%ath ); delete $ent{$ath_max}; delete $mdf{$ath_max};
    my ($ent_max, $ent_count) = $T->find_max( \%ent ); delete $mdf{$ent_max};
    my ($mdf_max, $mdf_count) = $T->find_max( \%mdf );
    
    diag("   ath: $ath_max  ent: $ent_max  mdf: $mdf_max");
    
    my ($ati_max, $ati_count) = $T->find_max( \%ati ); delete $eni{$ati_max}; delete $mdi{$ati_max};
    my ($eni_max, $eni_count) = $T->find_max( \%eni ); delete $mdi{$eni_max};
    my ($mdi_max, $mdi_count) = $T->find_max( \%mdi );
    
    # Select all the records authorized and not authorized by that person, and make sure the
    # totals add up.
    
    my @o2a = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_authorized_by=$ath_max&show=ent,entname",
				"authorized by max name");
    my @o2b = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_authorized_by=!$ath_max&show=ent,entname",
				"not authorized by max name");
    
    cmp_ok( @o2a + @o2b, '==', @o1, "authorized by + not authorized by = all" );
    
    # Same with external identifiers.
    
    my @o2c = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_authorized_by=$ati_max&show=ent,entname",
				"authorized by max id");
    my @o2d = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_authorized_by=!$ati_max&show=ent,entname",
				"not authorized by max id");
    
    cmp_ok( @o2c + @o2d, '==', @o1, "authorized by + not authorized by = all" );
    
    cmp_ok( @o2c, '==', @o2a, "authorized_by ati max = authorized_by ath max" );
    cmp_ok( @o2d, '==', @o2b, "not authorized_by ati max = not authorized_by ath max" );
    
    # Same with numeric identifiers.
    
    $ati_max =~ /(\d+)/; my $ati_num = $1;
    
    my @o2e = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_authorized_by=$ati_num&show=ent,entname",
				"authorized by max num");
    my @o2f = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_authorized_by=!$ati_num&show=ent,entname",
				"not authorized by max num");
    
    cmp_ok( @o2e + @o2f, '==', @o1, "authorized by + not authorized by = all" );
    
    cmp_ok( @o2c, '==', @o2e, "authorized_by ati max = authorized_by ati num max" );
    cmp_ok( @o2d, '==', @o2f, "not authorized_by ati max = not authorized_by ati num max" );
    
    # Make sure that each of the records has the proper identifier and name.
    
    foreach my $r ( @o2a, @o2c, @o2e )
    {
	$tc->flag('ati', $r->{nam}) unless $r->{ati} eq $ati_max;
	$tc->flag('ath', $r->{nam}) unless $r->{ath} eq $ath_max;
    }
    
    $tc->ok_all("authorized by max finds records with proper name and id");
    
    foreach my $r ( @o2b, @o2d, @o2f )
    {
	$tc->flag('ati', $r->{nam}) unless $r->{ati} ne $ati_max;
	$tc->flag('ath', $r->{nam}) unless $r->{ath} ne $ath_max;
    }
    
    $tc->ok_all("not authorized by max finds records with proper name and id");
    
    # Now check enterers in the same way.
    
    my @o3a = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_entered_by=$ent_max&show=ent,entname",
				"entered by max name");
    my @o3b = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_entered_by=!$ent_max&show=ent,entname",
				"not entered by max name");
    
    cmp_ok( @o3a + @o3b, '==', @o1, "entered by + not entered by = all" );
    
    # Same with external identifiers.
    
    my @o3c = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_entered_by=$eni_max&show=ent,entname",
				"entered by max id");
    my @o3d = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_entered_by=!$eni_max&show=ent,entname",
				"not entered by max id");
    
    cmp_ok( @o3c + @o3d, '==', @o1, "entered by + not entered by = all" );
    
    # Same with numeric identifiers.
    
    $eni_max =~ /(\d+)/; my $eni_num = $1;
    
    my @o3e = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_entered_by=$eni_num&show=ent,entname",
				"entered by max num");
    my @o3f = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_entered_by=!$eni_num&show=ent,entname",
				"not entered by max num");
    
    cmp_ok( @o3e + @o3f, '==', @o1, "entered by + not entered by = all" );
    
    # Again make sure that each of the records has the proper identifier and name.
    
    foreach my $r ( @o3a, @o3c, @o3e )
    {
	$tc->flag('eni', $r->{nam}) unless $r->{eni} eq $eni_max;
	$tc->flag('ent', $r->{nam}) unless $r->{ent} eq $ent_max;
    }
    
    $tc->ok_all("entered by max finds records with proper name and id");
    
    foreach my $r ( @o3b, @o3d, @o3f )
    {
	$tc->flag('eni', $r->{nam}) unless $r->{eni} ne $eni_max;
	$tc->flag('ent', $r->{nam}) unless $r->{ent} ne $ent_max;
    }
    
    $tc->ok_all("not entered by max finds records with proper name and id");
    
    # Now same for modifiers.  For this, we have to take into account that not every record may
    # have a modifier.
    
    my @o4any = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_modified_by=%&show=ent,entname",
				   "modified by any");
    
    my @o4a = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_modified_by=$mdf_max&show=ent,entname",
				"modified by max name");
    my @o4b = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_modified_by=!$mdf_max&show=ent,entname",
				"not modified by max name");
    
    cmp_ok( @o4a + @o4b, '==', @o1, "modified by + not modified by = modified by any (mdf max)" );
    
    # Same with external identifiers.
    
    my @o4c = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_modified_by=$mdi_max&show=ent,entname",
				"modified by max id");
    my @o4d = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_modified_by=!$mdi_max&show=ent,entname",
				"not modified by max id");
    
    cmp_ok( @o4c + @o4d, '==', @o1, "modified by + not modified by = modified by any (mdi max)" );
    
    # Same with numeric identifiers.
    
    $mdi_max =~ /(\d+)/; my $mdi_num = $1;
    
    my @o4e = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_modified_by=$mdi_num&show=ent,entname",
				"modified by max num");
    my @o4f = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_modified_by=!$mdi_num&show=ent,entname",
				"not modified by max num");
    
    cmp_ok( @o4e + @o4f, '==', @o1, "modified by + not modified by = modified by any (mdi num)" );
    
    # Again make sure that each of the records has the proper identifier and name.
    
    foreach my $r ( @o4a, @o4c, @o4e )
    {
	$tc->flag('invalid', $r->{nam}) unless ! $r->{mdi} || $r->{mdi} eq $mdi_max && $r->{mdf} eq $mdf_max;
    }
    
    $tc->ok_all("modified by max finds records with proper name and id");
    
    foreach my $r ( @o4b, @o4d, @o4f )
    {
	$tc->flag('invalid', $r->{nam}) unless ! $r->{mdi} || $r->{mdi} ne $mdi_max && $r->{mdf} ne $mdf_max;
    }
    
    $tc->ok_all("not modified by max finds records with proper name and id");
    
    # Now we need to try the value '!'. This should return no records for 'authorized_by' and
    # 'entered_by', and only taxa that have not been modified for 'modified_by'.
    
    my @o5a = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_authorized_by=!&show=ent,entname",
				"authorized by '!'", { no_records_ok => 1 });
    
    cmp_ok( @o5a, '==', 0, "authorized by '!' found no records" );
    
    my @o5b = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_entered_by=!&show=ent,entname",
				"entered by '!'", { no_records_ok => 1 });
    
    cmp_ok( @o5b, '==', 0, "entered by '!' found no records" );
    
    my @o5c = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_modified_by=!&show=ent,entname",
				"modified by '!'");
    
    foreach my $r ( @o5c )
    {
	$tc->flag('invalid', $r->{nam}) if $r->{mdf} || $r->{mdi};
    }
    
    $tc->ok_all("modified by '!' finds records with no modifier");
    
    # Check 'authent_by' using the person who authorized the most records.
    
    my @o6a = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_authent_by=$ati_max&show=ent,entname",
				"authent by max");
    my @o6b = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_authent_by=!$ati_max&show=ent,entname",
				"not authent by max");
    
    cmp_ok( @o6a + @o6b, '==', @o1, "authent_by + not authent_by = all (ati max)" );
    
    foreach my $r ( @o6a )
    {
	$tc->flag('invalid', $r->{nam}) unless $r->{ati} eq $ati_max || $r->{eni} eq $ati_max;
    }
    
    $tc->ok_all("authent by max finds records with proper auth/ent identifier");
    
    foreach my $r ( @o6b )
    {
	$tc->flag('invalid', $r->{nam}) if $r->{ati} eq $ati_max || $r->{eni} eq $ati_max;
    }
    
    $tc->ok_all("not authent by max finds records with improper auth/ent identifier");
    
    # Then check to make sure that the two sets of record oids match up.
    
    my @o3x = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_entered_by=$ati_max&show=ent,entname",
				"entered by ati max");
    
    my %authent_oid = $T->extract_values( \@o6a, 'oid' );
    my %auth_by_oid = $T->extract_values( \@o2a, 'oid' );
    my %ent_by_oid = $T->extract_values( \@o3x, 'oid' );
    
    my %check_oid = ( %auth_by_oid, %ent_by_oid );
    
    is_deeply( \%authent_oid, \%check_oid, "authent_by matches authorized_by U entered_by (ati max)" );
    
    # Then do the same check using the person who entered the most records (but did not authorize
    # the most). 
    
    # my @o6c = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_authent_by=$eni_max&show=ent,entname",
    # 				"authent_by eni max");
    # my @o6d = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_authent_by=!$eni_max&show=ent,entname",
    # 				"not authent_by eni max");
    
    # my @o2x = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_authorized_by=$eni_max&show=ent,entname",
    # 				"authorized_by eni max");
    
    # cmp_ok( @o6c + @o6d, '==', @o1, "authent_by + not authent_by = all (eni max)" );
    
    # %authent_oid = $T->extract_values( \@o6c, 'oid' );
    # %auth_by_oid = $T->extract_values( \@o3a, 'oid' );
    # %ent_by_oid = $T->extract_values( \@o2x, 'oid' );
    
    # %check_oid = ( %auth_by_oid, %ent_by_oid );
    
    # is_deeply( \%authent_oid, \%check_oid, "authent_by matches authorized_by U entered_by (eni max)" );
    
    # Now do the same for touched_by, but a smaller number of tests.
    
    my @o7a = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_touched_by=$ati_max&show=ent,entname",
				"touched_by ati max");
    my @o7b = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_touched_by=!$ati_max&show=ent,entname",
				"not touched_by ati max");
    
    cmp_ok( @o7a + @o7b, '==', @o1, "touched_by + not touched+by = all (ati max)" );
    
    my @o4x = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_modified_by=$ati_max&show=ent,entname",
				"modified_by ati max");
    
    my %touched_oid = $T->extract_values( \@o7a, 'oid' );
    %auth_by_oid = $T->extract_values( \@o2a, 'oid' );
    %ent_by_oid = $T->extract_values( \@o3x, 'oid' );
    my %mod_by_oid = $T->extract_values( \@o4x, 'oid' );
    
    %check_oid = ( %auth_by_oid, %ent_by_oid, %mod_by_oid );
    
    is_deeply( \%touched_oid, \%check_oid, "touched_by matches auth U entered U mod (ati max)" );
    
    my @o7c = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_touched_by=$mdi_max&show=ent,entname",
				"touched_by mdi max");
    my @o7d = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_touched_by=!$mdi_max&show=ent,entname",
				"not touched_by mdi max");
    
    cmp_ok( @o7c + @o7d, '==', @o1, "touched_by + not touched+by = all (mdi max)" );    
    
    # Then we check two different parameters together.  We can't possibly test all combinations,
    # but we can at least check one.
    
    my @o10a = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_authorized_by=$ati_max&" .
				 "occs_modified_by=!$mdi_max&show=ent,entname", "auth_by and not mod_by");
    
    my %combo_oid = $T->extract_values( \@o10a, 'oid' );
    %check_oid = $T->extract_values( \@o2a, 'oid' );
    %mod_by_oid = $T->extract_values( \@o4c, 'oid' );
    # my %no_mod_oid = $T->extract_values( \@o5c, 'oid' );
    
    # subtract %mod_by_oid from %check_oid, then test.
    
    delete $check_oid{$_} foreach keys %mod_by_oid; # , keys %no_mod_oid;
    
    $T->cmp_sets_ok( \%combo_oid, '==', \%check_oid, "auth_by and not mod_by returns proper records" );
    
    # Then we try a parameter with multiple values.
    
    my @o11a = $T->fetch_records("/occs/list.json?base_name=$NAME_1&occs_entered_by=$ati_max,$eni_max&" .
				 "show=ent,entname", "entered_by multiple");
    
    my %eni_count = $T->count_values( \@o11a, 'eni' );
    
    cmp_ok( $eni_count{$ati_max} + $eni_count{$eni_max}, '==', @o11a, 
	    "entered_by multiple gets records with proper eni" );
    cmp_ok( $eni_count{$ati_max}, '>', 0, "entered_by multiple gets at least one entered by ati_max" );
    cmp_ok( $eni_count{$eni_max}, '>', 0, "entered_by multiple gets at least one entered by eni_max" );
};


select_final;
