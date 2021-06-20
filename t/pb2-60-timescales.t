# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test querying for timescales and intervals using the new system.
# 

use strict;

use Test::More tests => 14;
use List::Util qw(max);

use lib 'lib', '../lib';

use ExternalIdent qw(%IDRE);

use lib 't';

use Tester;
use Test::Conditions;
use Test::Selection;


my $VALID_AGE = qr{ ^ \d+ $ | ^ \d+ [.] \d+ $ | ^ [.] \d+ $ }xs;
my %VALID_BTP = (absolute => 1, spike => 1, same => 1, fraction => 1);

# If we provided any command-line arguments, run only subtests whose names match.

choose_subtests(@ARGV);

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });

my @ALL_TIMESCALES;
my ($TIMESCALE_TAXON, $AUTH_ID, $AUTH_NAME);


# Test that the basic query operations all work. Save the list of timescales for use
# below. This subtest is always carried out even if subtest selection is being done.

subtest 'basic' => sub {

    # Fetch the full list of timescales.
    
    @ALL_TIMESCALES = $T->fetch_records("/timescales/list.json?all_records&show=desc,ent,entname,crmod", "fetch all timescales json");
    
    unless ( @ALL_TIMESCALES )
    {
	diag "No timescales were fetched.";
	exit;
    }
    
    ok($ALL_TIMESCALES[0] && $ALL_TIMESCALES[0]{oid}, "found oid for first timescale") || exit;
    
    my $ts1 = $ALL_TIMESCALES[0]{oid};
    
    my @r1 = $T->fetch_records("/timescales/intervals.json?timescale_id=$ts1", "fetch intervals");
    my @r2 = $T->fetch_records("/timescales/bounds.json?timescale_id=$ts1", "fetch bounds");
    
    # If at least one of the timescales has an associated taxon, save the first one we find. Same
    # for authorizer id and name. We just need one.

    foreach my $r ( @ALL_TIMESCALES )
    {
	$TIMESCALE_TAXON ||= $r->{txn} if $r->{txn};
	$AUTH_ID ||= $r->{ati} if $r->{ati};
	$AUTH_NAME ||= $r->{ath} if $r->{ath};
    }
};


# Test that the timescale list (both json and txt) returns the proper attributes.

subtest 'timescales' => sub {
    
    select_subtest || return;

    # First check that the basic list of timescales returns records with the proper attributes.
    
    my $tc = Test::Conditions->new;
    
    $tc->expect('has_ext', 'has_typ', 'has_txn', 'has_lck', 'has_vis', 'has_enc',
		'has_pri', 'has_rid', 'has_stages', 'has_periods');
    
    foreach my $r ( @ALL_TIMESCALES )
    {
	my $label = $r->{oid} || 'none';
	$tc->flag('oid', $label) unless $r->{oid} && $r->{oid} =~ $IDRE{TSC};
	$tc->flag('nam', $label) unless $r->{nam};
	$tc->flag('lag', $label) unless defined $r->{lag} && $r->{lag} =~ $VALID_AGE;
	$tc->flag('eag', $label) unless defined $r->{eag} && $r->{eag} =~ $VALID_AGE;
	$tc->flag('lck') if defined $r->{lck} && $r->{lck} !~ /^[01]$/;
	$tc->flag('vis') if defined $r->{vis} && $r->{vis} !~ /^[01]$/;
	$tc->flag('enc') if defined $r->{enc} && $r->{enc} !~ /^[01]$/;
	$tc->flag('pri') if defined $r->{pri} && $r->{pri} !~ /^\d+$/;
	$tc->flag('rid') if defined $r->{rid} && $r->{rid} !~ $IDRE{REF};
	
	$tc->flag('has_ext') if $r->{ext};
	$tc->flag('has_typ') if $r->{typ};
	$tc->flag('has_txn') if $r->{txn};
	$tc->flag('has_lck') if $r->{lck};
	$tc->flag('has_vis') if $r->{vis};
	$tc->flag('has_enc') if $r->{enc};
	$tc->flag('has_pri') if $r->{pri};
	$tc->flag('has_rid') if $r->{rid};
	$tc->flag('has_stages') if $r->{typ} && $r->{typ} eq 'stage';
	$tc->flag('has_periods') if $r->{typ} && $r->{typ} eq 'period';
    }
    
    $tc->ok_all("timescale attributes json");

    # Now try to fetch the first record, using 'single.json' with parameter 'id' and again with 'timescale_id'.
    
    my $ts1 = $ALL_TIMESCALES[0]{oid};
    
    my ($r1s) = $T->fetch_records("/timescales/single.json?id=$ts1&show=crmod,ent,entname,desc",
				  "fetch single timescale json");

    is_deeply($r1s, $ALL_TIMESCALES[0], "single timescale matches equivalent list record");
    
    my ($r1b) = $T->fetch_records("/timescales/single.json?timescale_id=$ts1&show=desc",
				  "fetch single timescale alt parameter");
    
    is($r1b->{oid}, $ts1, "got proper record id");
    
    # Now try to fetch the first record using 'list.json' with paramter 'timescale_id'.

    my ($r1c) = $T->fetch_records("/timescales/list.json?timescale_id=$ts1&show=desc",
				  "list one timescale by id");
    
    is($r1c->{oid}, $ts1, "got proper record id");
    
    # Now fetch the full list of timescales in txt format.
    
    my @r2 = $T->fetch_records("/timescales/list.txt?all_records&show=desc",
			       "fetch all timescales txt");
    
    return unless @r2;
    
    foreach my $r ( @r2 )
    {
	my $label = $r->{timescale_no} || 'none';
	$tc->flag('timescale_no', $label) unless $r->{timescale_no} && $r->{timescale_no} =~ /^\d+$/;
	$tc->flag('timescale_name', $label) unless $r->{timescale_name};
	$tc->flag('record_type', $label) unless defined $r->{record_type} && $r->{record_type} eq 'tsc';
	$tc->flag('min_age', $label) unless defined $r->{min_age} && $r->{min_age} =~ $VALID_AGE;
	$tc->flag('max_age', $label) unless defined $r->{max_age} && $r->{max_age} =~ $VALID_AGE;
	$tc->flag('admin_lock') if defined $r->{admin_lock} && $r->{admin_lock} !~ /^[01]?$/;
	$tc->flag('is_visible') if defined $r->{is_visible} && $r->{is_visible} !~ /^[01]?$/;
	$tc->flag('is_enterable') if defined $r->{is_enterable} && $r->{is_enterable} !~ /^[01]?$/;
	$tc->flag('priority') if defined $r->{priority} && $r->{priority} !~ /^\d+$/;
	$tc->flag('reference_no') if defined $r->{reference_no} && $r->{reference_no} !~ /^\d+$/;
	
	$tc->flag('has_ext') if $r->{timescale_extent};
	$tc->flag('has_typ') if $r->{timescale_type};
	$tc->flag('has_txn') if $r->{timescale_taxon};
	$tc->flag('has_lck') if $r->{admin_lock};
	$tc->flag('has_vis') if $r->{is_visible};
	$tc->flag('has_enc') if $r->{is_enterable};
	$tc->flag('has_pri') if $r->{priority};
	$tc->flag('has_rid') if $r->{reference_no};
	$tc->flag('has_stages') if $r->{timescale_type} && $r->{timescale_type} eq 'stage';
	$tc->flag('has_periods') if $r->{timescale_type} && $r->{timescale_type} eq 'period';
    }

    $tc->ok_all("timescale attributes txt");
};


# Then test that lists of bounds return the proper attributes.

subtest 'bounds' => sub {
    
    select_subtest || return;

    my $ts1 = $ALL_TIMESCALES[0]{oid};

    my $tc = Test::Conditions->new;

    $tc->expect('has_inm', 'has_spk', 'has_col', 'has_uid', 'has_ger');
    
    my @r1 = $T->fetch_records("/timescales/bounds.json?timescale_id=$ts1", "fetch bounds json");
    
    return unless @r1;
    
    foreach my $r ( @r1 )
    {
	my $label = $r->{oid} || 'none';

	$tc->flag('oid', $label) unless $r->{oid} && $r->{oid} =~ $IDRE{BND};
	$tc->flag('sid', $label) unless $r->{sid} && $r->{sid} =~ $IDRE{TSC};
	$tc->flag('age', $label) unless defined $r->{age} && $r->{age} =~ $VALID_AGE;
	$tc->flag('btp', $label) unless defined $r->{btp} && $VALID_BTP{$r->{btp}};
	
	$tc->flag('spk', $label) if defined $r->{spk} && $r->{spk} !~ /^[01]?$/;
	$tc->flag('uid', $label) if defined $r->{uid} && $r->{uid} !~ $IDRE{BND};
	$tc->flag('col', $label) if defined $r->{col} && $r->{col} !~ /^[#][A-Z\d]+$/;
	$tc->flag('ger', $label) if defined $r->{ger} && $r->{ger} !~ $VALID_AGE;

	$tc->flag('empty_interval', $label) if $r->{uid} && ! $r->{inm};
	$tc->flag('link', $label) if $r->{inm} && ! $r->{uid};

	$tc->flag('has_inm') if $r->{inm};
	$tc->flag('has_uid') if $r->{uid};
	$tc->flag('has_spk') if $r->{spk};
	$tc->flag('has_col') if $r->{col};
	$tc->flag('has_ger') if $r->{ger};
    }
    
    $tc->ok_all("bound attributes json");

    my $bnd1 = $r1[0]{oid};
    
    my ($r1s) = $T->fetch_records("/timescales/bounds.json?id=$bnd1", "fetch single bound json");

    is_deeply($r1s, $r1[0], "single bound matched list bound record");
    
    my @r2 = $T->fetch_records("/timescales/bounds.txt?timescale_id=$ts1", "fetch bounds txt");

    return unless @r2;
    
    foreach my $r ( @r2 )
    {
	my $label = $r->{bound_no} || 'none';
	
	$tc->flag('bound_no', $label) unless $r->{bound_no} && $r->{bound_no} =~ /^\d+$/;
	$tc->flag('timescale_no', $label) unless $r->{timescale_no} && $r->{timescale_no} =~ /^\d+$/;
	$tc->flag('record_type', $label) unless defined $r->{record_type} && $r->{record_type} eq 'bnd';
	$tc->flag('age', $label) unless defined $r->{age} && $r->{age} =~ $VALID_AGE;
	$tc->flag('bound_type', $label) unless defined $r->{bound_type} && $VALID_BTP{$r->{bound_type}};
	
	$tc->flag('age_error', $label) if $r->{age_error} && $r->{age_error} !~ $VALID_AGE;
	$tc->flag('is_spike', $label) if defined $r->{is_spike} && $r->{is_spike} !~ /^[01]?$/;
	$tc->flag('top_no', $label) if $r->{top_no} && $r->{top_no} !~ $IDRE{BND};
	$tc->flag('color', $label) if $r->{color} && $r->{color} !~ /^[#][A-Z\d]+$/;

	$tc->flag('empty_interval', $label) if $r->{top_no} && ! $r->{interval_name};
	$tc->flag('link', $label) if $r->{interval_name} && ! $r->{top_no};
	
	$tc->flag('has_inm') if $r->{interval_name};
	$tc->flag('has_uid') if $r->{top_no};
	$tc->flag('has_spk') if $r->{is_spike};
	$tc->flag('has_col') if $r->{color};
	$tc->flag('has_ger') if $r->{age_error};
    }
    
    $tc->ok_all("bound attributes txt");
};


# Then test that lists of intervals return the proper attributes.

subtest 'intervals' => sub {
    
    select_subtest || return;
    
    my $ts1 = $ALL_TIMESCALES[0]{oid};

    my $tc = Test::Conditions->new;
    
    my @r1 = $T->fetch_records("/timescales/intervals.json?timescale_id=$ts1", "fetch bounds json");
    
    return unless @r1;
    
    foreach my $r ( @r1 )
    {
	my $label = $r->{oid} || 'none';
	
	$tc->flag('oid', $label) unless $r->{oid} && $r->{oid} =~ $IDRE{INT};
	$tc->flag('sid', $label) unless $r->{sid} && $r->{sid} =~ $IDRE{TSC};
	$tc->flag('eag', $label) unless defined $r->{eag} && $r->{eag} =~ $VALID_AGE;
	$tc->flag('lag', $label) unless defined $r->{lag} && $r->{lag} =~ $VALID_AGE;
	$tc->flag('nam', $label) unless $r->{nam};
	$tc->flag('col', $label) unless $r->{col} && $r->{col} =~ /^[#][A-Z\d]+$/;
    }
    
    $tc->ok_all("interval attributes json");
    
    my @r2 = $T->fetch_records("/timescales/intervals.txt?timescale_id=$ts1", "fetch records txt");
    
    return unless @r2;

    foreach my $r ( @r2 )
    {
	my $label = $r->{interval_no} || 'none';
	
	$tc->flag('interval_no', $label) unless $r->{interval_no} && $r->{interval_no} =~ /^\d+$/;
	$tc->flag('timescale_no', $label) unless $r->{timescale_no} && $r->{timescale_no} =~ /^\d+$/;
	$tc->flag('record_type', $label) unless defined $r->{record_type} && $r->{record_type} eq 'int';
	$tc->flag('late_age', $label) unless defined $r->{late_age} && $r->{late_age} =~ $VALID_AGE;
	$tc->flag('early_age', $label) unless defined $r->{early_age} && $r->{early_age} =~ $VALID_AGE;
	$tc->flag('color', $label) unless defined $r->{color} && $r->{color} =~ /^[#][A-Z\d]+$/;
    }
    
    $tc->ok_all("interval attributes txt");
};


# Now check that the list timescales operation accepts all of the documented parameters, and that
# the results are as expected.

subtest 'timescale params' => sub {
    
    select_subtest || return;
    
    # First check that we can list multiple timescales using the 'id' parameter.
    
    my @tsid = map { $_->{oid} } @ALL_TIMESCALES[0,1,2];
    
    my $tsid_list = join(',', @tsid);

    my @t1 = $T->fetch_records("/timescales/list.json?id=$tsid_list", "fetch timescales by id");

    cmp_ok(@t1, '==', 3, "got three timescale records");

    # Then check that we can list the timescale that contains a specified bound. We fetch bounds
    # from the first two timescales, and check that we get two timescale records.

    my @b1 = $T->fetch_records("/timescales/bounds.json?timescale_id=$tsid[0]",
			       "fetch bounds from first timescale");
    my @b2 = $T->fetch_records("/timescales/bounds.json?timescale_id=$tsid[1]",
			       "fetch bounds from second timescale");

    my $bndid_list = $b1[2]{oid} . ',' . $b2[4]{oid};
    
    my @t2 = $T->fetch_records("/timescales/list.json?bound_id=$bndid_list",
			       "fetch timescales containing specified bounds");

    cmp_ok(@t2, '==', 2, "got two timescale reords");

    # Then do the same for intervals. We fetch intervals from the first two timescales, and check
    # that we get at least two timescale records. There may be more, because a given interval can
    # appear in more than one timescale.

    my @i1 = $T->fetch_records("/timescales/intervals.json?timescale_id=$tsid[0]",
			       "fetch intervals from first timescale");
    my @i2 = $T->fetch_records("/timescales/intervals.json?timescale_id=$tsid[1]",
			       "fetch intervals from second timescale");
    
    my $intid_list = $i1[3]{oid} . ',' . $i2[7]{oid};
    
    my @t3 = $T->fetch_records("/timescales/list.json?interval_id=$intid_list",
			       "fetch timescales containing specified intervals");
    
    cmp_ok(@t3, '>=', 2, "got at least two timescale reords");
    
    # Now make sure we can fetch timescales by type and extent.
    
    my $tc = Test::Conditions->new;
    
    my @t4 = $T->fetch_records("/timescales/list.json?type=period&show=desc", "fetch timescales of type 'period'");
    
    if ( @t4 )
    {
	like($t4[0]{nam}, qr{ICS Periods}, "first returned timescale was ICS periods");
    }
    
    foreach my $r ( @t4 )
    {
	$tc->flag('typ', $r->{oid}) unless $r->{typ} && $r->{typ} eq 'period';
    }
    
    my @t5 = $T->fetch_records("/timescales/list.json?extent=global&show=desc",
			       "fetch timescales with extent 'global'");
    
    if ( @t5 )
    {
	like($t4[0]{nam}, qr{ICS}, "first returned timescale was ICS");
    }

    foreach my $r ( @t5 )
    {
	$tc->flag('ext', $r->{oid}) unless $r->{ext} && $r->{ext} =~ /global|international/i;
    }
    
    # If at least one of the timescales has a taxon, check that we can retrieve by that
    # value.
    
    if ( $TIMESCALE_TAXON )
    {
	diag("found timescale taxon '$TIMESCALE_TAXON'");
	my @t6 = $T->fetch_records("/timescales/list.json?taxon=$TIMESCALE_TAXON&show=desc",
				   "fetch timescales with taxon '$TIMESCALE_TAXON'");

	my $test_re = qr{$TIMESCALE_TAXON}i;

	foreach my $r ( @t6 )
	{
	    $tc->flag('txn', $r->{oid}) unless $r->{txn} && $r->{txn} =~ $test_re;
	}
    }

    $tc->ok_all("filtering by type, extent, and taxon works properly");
    
    # Now check that we can retrieve by timescale name matching.

    my @t7 = $T->fetch_records("/timescales/list.json?timescale_match=permian",
			       "fetch timescales whose name matches 'permian'");

    my $tc = Test::Conditions->new;

    foreach my $r ( @t7 )
    {
	$tc->flag('name', $r->{oid}) unless $r->{nam} && $r->{nam} =~ /permian/i;
    }

    $tc->ok_all("all records had names matching 'permian'");
    
    # Now check that we can retrieve all timescales that include a given interval name.

    my @t8 = $T->fetch_records("/timescales/list.json?interval_name=permian",
			       "fetch timescales containing interval 'permian'");
    
    my @t9 = $T->fetch_records("/timescales/list.json?interval_match=perm",
			       "fetch timescales containing interval matching 'perm'");

    cmp_ok( @t9, '>=', @t8, "second query fetches at least as many records as first query" );
};


# Check additional timescale parameters.

subtest 'timescale params 2' => sub {
    
    select_subtest || return;

    my $tc = Test::Conditions->new;
    
    # Check min_ma and max_ma parameters.
    
    my @t1 = $T->fetch_records("/timescales/list.json?max_ma=600", "fetch timescales after 600 Ma");
    
    cmp_ok( @t1, '<', @ALL_TIMESCALES, "query with max_ma fetched fewer timescales" );
    
    foreach my $r ( @t1 )
    {
	$tc->flag('eag', $r->{oid}) unless defined $r->{eag} && $r->{eag} <= 600.0;
    }
    
    my @t2 = $T->fetch_records("/timescales/list.json?min_ma=100", "fetch timescales before 100 Ma");

    cmp_ok( @t2, '<', @ALL_TIMESCALES, "query with min_ma fetched fewer timescales" );

    foreach my $r ( @t2 )
    {
	$tc->flag('lag', $r->{oid}) unless defined $r->{lag} && $r->{lag} >= 100.0;
    }
    
    $tc->ok_all("filtering for min and max ages works properly");
    
    # Check created_before, created_after, modified_before, modified_after. This requires
    # computing the max 'dcr' and 'dmd' values.

    my $max_dcr = max map { $_->{dcr} } @ALL_TIMESCALES;
    my $max_dmd = max map { $_->{dmd} } @ALL_TIMESCALES;

    my @t3 = $T->fetch_records("/timescales/list.json?all_records&show=crmod&created_before=$max_dcr",
			       "fetch timescales using 'created_before'");
    
    cmp_ok( @t3, '<', @ALL_TIMESCALES, "query with created_before fetched fewer timescales");
    
    foreach my $r ( @t3 )
    {
	$tc->flag('created before', $r->{oid}) unless $r->{dcr} && $r->{dcr} lt $max_dcr;
    }
    
    my @t4 = $T->fetch_records("/timescales/list.json?all_records&show=crmod&created_after=$max_dcr",
			       "fetch timescales using 'created_before'");
    
    foreach my $r ( @t4 )
    {
	$tc->flag('created after', $r->{oid}) unless $r->{dcr} && $r->{dcr} ge $max_dcr;
    }
    
    cmp_ok( @t4, '<', @ALL_TIMESCALES, "query with created_before fetched fewer timescales");

    cmp_ok( scalar(@t3) + scalar(@t4), '==', @ALL_TIMESCALES, "both together fetched full list");
    
    my @t5 = $T->fetch_records("/timescales/list.json?all_records&show=crmod&modified_before=$max_dmd",
			       "fetch timescales using 'modified_before'");

    cmp_ok( @t5, '<', @ALL_TIMESCALES, "query with modified_before fetched fewer timescales");
    
    foreach my $r ( @t5 )
    {
	$tc->flag('modified before', $r->{oid}) unless $r->{dmd} && $r->{dmd} lt $max_dmd;
    }
    
    my @t6 = $T->fetch_records("/timescales/list.json?all_records&show=crmod&modified_after=$max_dmd",
			       "fetch timescales using 'modified_before'");
    
    cmp_ok( @t6, '<', @ALL_TIMESCALES, "query with modified_before fetched fewer timescales");
    
    foreach my $r ( @t6 )
    {
	$tc->flag('modified after', $r->{oid}) unless $r->{dmd} && $r->{dmd} ge $max_dmd;
    }
    
    cmp_ok( scalar(@t5) + scalar(@t6), '==', @ALL_TIMESCALES, "both together fetched full list");

    $tc->ok_all("filtering by date created, date modified works properly");
};


# Now check authorized_by, etc.

subtest 'timescale params 3' => sub {

    select_subtest || return;

    my $tc = Test::Conditions->new;
    
    if ( $AUTH_ID )
    {
	my @t1 = $T->fetch_records("/timescales/list.json?all_records&show=ent&authorized_by=$AUTH_ID",
				   "fetch authorized by '$AUTH_ID'");

	foreach my $r ( @t1 )
	{
	    $tc->flag('ati', $r->{oid}) unless $r->{ati} && $r->{ati} eq $AUTH_ID;
	}

	my @t2 = $T->fetch_records("/timescales/list.json?all_records&show=ent&entered_by=$AUTH_ID",
				   "fetch entered by '$AUTH_ID'");

	foreach my $r ( @t2 )
	{
	    $tc->flag('eni', $r->{oid}) unless $r->{eni} && $r->{eni} eq $AUTH_ID;
	}

	my @t3 = $T->fetch_records("/timescales/list.json?all_records&show=ent&modified_by=$AUTH_ID",
				   "fetch modified by '$AUTH_ID'", { no_records_ok => 1 });
	
	my @t4 = $T->fetch_records("/timescales/list.json?all_records&show=ent&authent_by=$AUTH_ID",
				   "fetch modified by '$AUTH_ID'", { no_records_ok => 1 });
	
	my @t5 = $T->fetch_records("/timescales/list.json?all_records&show=ent&touched_by=$AUTH_ID",
				   "fetch modified by '$AUTH_ID'", { no_records_ok => 1 });
    }
    
    if ( $AUTH_NAME )
    {
	my @t11 = $T->fetch_records("/timescales/list.json?all_records&show=entname&authorized_by=$AUTH_NAME",
				   "fetch authorized by '$AUTH_NAME'");

	foreach my $r ( @t11 )
	{
	    $tc->flag('ath', $r->{oid}) unless $r->{ath} && $r->{ath} eq $AUTH_NAME;
	}
	
	my @t12 = $T->fetch_records("/timescales/list.json?all_records&show=entname&entered_by=$AUTH_NAME",
				   "fetch entered by '$AUTH_NAME'");
	
	foreach my $r ( @t12 )
	{
	    $tc->flag('ent', $r->{oid}) unless $r->{ent} && $r->{ent} eq $AUTH_NAME;
	}

	my @t13 = $T->fetch_records("/timescales/list.json?all_records&show=entname&modified_by=$AUTH_NAME",
				   "fetch modified by '$AUTH_NAME'", { no_records_ok => 1 });
	
	my @t14 = $T->fetch_records("/timescales/list.json?all_records&show=entname&authent_by=$AUTH_NAME",
				   "fetch modified by '$AUTH_NAME'", { no_records_ok => 1 });
	
	my @t15 = $T->fetch_records("/timescales/list.json?all_records&show=entname&touched_by=$AUTH_NAME",
				   "fetch modified by '$AUTH_NAME'", { no_records_ok => 1 });
    }

    $tc->ok_all("filtering by authorizer and enterer worked properly");
};


# Check that bad parameter values generate errors.

subtest 'timescale params bad' => sub {
    
    select_subtest || return;
    
    # First check the 'id' parameters.
    
    $T->fetch_nocheck("/timescales/list.json?id=bnd:23", "bad id type");
    $T->ok_no_records("no records from bad id type");
    
    $T->fetch_nocheck("/timescales/list.json?timescale_id=int:23", "bad id type 2");
    $T->ok_no_records("no records from bad id type 2");
    
    $T->fetch_nocheck("/timescales/list.json?bound_id=tsc:1", "bad id type 3");
    $T->ok_no_records("no records from bad id type 3");
    
    $T->fetch_nocheck("/timescales/list.json?interval_id=tsc:1", "bad id type 4");
    $T->ok_no_records("no records from bad id type 4");

    # Then type, taxon, extent.
    
    $T->fetch_nocheck("/timescales/list.json?type=abcd", "bad timescale type");
    $T->ok_no_records("no records from bad timescale type");

    $T->fetch_nocheck("/timescales/list.json?taxon=abcd", "bad timescale taxon");
    $T->ok_no_records("no records from bad timescale taxon");
    
    $T->fetch_nocheck("/timescales/list.json?extent=abcd", "bad timescale extent");
    $T->ok_no_records("no records from bad timescale extent");
    
    # Then min_ma, max_ma, created_before, created_after

    $T->fetch_nocheck("/timescales/list.json?min_ma=abcd", "bad min_ma value");
    $T->ok_response_code("400", "got 400 response from bad min_ma value");
    
    $T->fetch_nocheck("/timescales/list.json?max_ma=abcd", "bad max_ma value");
    $T->ok_response_code("400", "got 400 response from bad max_ma value");
    
    $T->fetch_nocheck("/timescales/list.json?all_records&created_before=abcd", "bad created_before value");
    $T->ok_response_code("400", "got 400 response from bad created_before value");
    
    $T->fetch_nocheck("/timescales/list.json?all_records&created_after=abcd", "bad created_after value");
    $T->ok_response_code("400", "got 400 response from bad created_after value");
    
    $T->fetch_nocheck("/timescales/list.json?all_records&modified_before=abcd", "bad modified_before value");
    $T->ok_response_code("400", "got 400 response from bad modified_before value");
    
    $T->fetch_nocheck("/timescales/list.json?all_records&modified_after=abcd", "bad modified_after value");
    $T->ok_response_code("400", "got 400 response from bad modified_after value");
    
    # Now check that no parameters at all returns a 400 response.

    $T->fetch_nocheck("/timescales/list.json", "no parameters at all");
    $T->ok_response_code("400", "got 400 response from no parameters at all");
};


# Now check that the list bounds operation accepts all of the documented parameters, and that
# the results are as expected.

subtest 'bound params' => sub {
    
    select_subtest || return;
    
    my $INAMES = 'cretaceous,jurassic';
    
    # Check that we can query bounds from more than one timescale at a time.

    my $tsid_string = $ALL_TIMESCALES[0]{oid} . "," . $ALL_TIMESCALES[1]{oid};

    my @b1 = $T->fetch_records("/timescales/bounds.json?timescale_id=$tsid_string", "fetch bounds from two timescales");

    my %tsid = $T->count_values(\@b1, 'sid');

    cmp_ok(keys %tsid, '==', 2, "got bounds from two timescales");

    # Check that we can query more than one bound by id.

    my $bndid_string = $b1[0]{oid} . "," . $b1[1]{oid};

    my @b2 = $T->fetch_records("/timescales/bounds.json?bound_id=$bndid_string", "fetch two bounds");

    cmp_ok(@b2, '==', 2, "got two bounds from two bound ids");
    
    # Check that we can query by interval name and id.
    
    my @b3 = $T->fetch_records("/timescales/bounds.json?interval_name=$INAMES", "fetch bounds from '$INAMES'");
    
    my (@i1) = $T->fetch_records("/timescales/intervals.json?interval_name=$INAMES", "fetch interval '$INAMES'");

    if ( cmp_ok( @i1, '==', 2, "got two interval records" ) )
    {
	my $idlist = $i1[0]{oid} . ',' . $i1[1]{oid};
	
	my @b4 = $T->fetch_records("/timescales/bounds.json?interval_id=$idlist", "fetch bounds from '$idlist'");
	
	cmp_ok(@b3, '==', @b4, "got same number of records by id as by name");
    }
    
    # Check that we can query by taxon, extent, and timescale name.

    my @b4 = $T->fetch_records("/timescales/bounds.json?extent=global", "fetch bounds with extent 'global'");

    my @b5 = $T->fetch_records("/timescales/bounds.json?timescale_match=ics",
			       "fetch bounds from timescales matching 'ics'");
    
    if ( $TIMESCALE_TAXON )
    {
	my @b6 = $T->fetch_records("/timescales/bounds.json?taxon=$TIMESCALE_TAXON&show=desc",
				   "fetch timescales with taxon '$TIMESCALE_TAXON'");
	
	my $test_re = qr{$TIMESCALE_TAXON}i;

	my $tc = Test::Conditions->new;
	
	foreach my $r ( @b6 )
	{
	    $tc->flag('txn', $r->{oid}) unless $r->{txn} && $r->{txn} =~ $test_re;
	}

	$tc->ok_all("returned records have proper taxon");
    }
};


subtest 'bound params 2' => sub {
    
    select_subtest || return;

    my $tc = Test::Conditions->new;
    
    # Check that we can query by min and max age.

    my @b1 = $T->fetch_records("/timescales/bounds.json?min_ma=100&max_ma=300", "fetch bounds using age range");
    
    my %tsid = $T->count_values(\@b1, 'sid');
    
    cmp_ok(keys %tsid, '>', 1, "got bounds from more than one timescale");

    foreach my $r ( @b1 )
    {
	$tc->flag('age', $r->{oid}) unless defined $r->{age} && $r->{age} >= 100 && $r->{age} <= 300;
    }

    $tc->ok_all("filtering by min and max age works properly");
};


# Check that bad parameter values generate errors.

subtest 'bound params bad' => sub {
    
    select_subtest || return;
    
    # First check the 'id' parameters.
    
    $T->fetch_nocheck("/timescales/bounds.json?id=tsc:1", "bad id type");
    $T->ok_no_records("no records from bad id type");
    
    $T->fetch_nocheck("/timescales/bounds.json?bound_id=int:23", "bad id type 2");
    $T->ok_no_records("no records from bad id type 2");
    
    $T->fetch_nocheck("/timescales/bounds.json?timescale_id=bnd:23", "bad id type 3");
    $T->ok_no_records("no records from bad id type 3");
    
    $T->fetch_nocheck("/timescales/bounds.json?interval_id=tsc:1", "bad id type 4");
    $T->ok_no_records("no records from bad id type 4");
    
    # Then type, taxon, extent.
    
    $T->fetch_nocheck("/timescales/bounds.json?type=abcd", "bad bound type");
    $T->ok_no_records("no records from bad bound type");
    
    $T->fetch_nocheck("/timescales/bounds.json?taxon=abcd", "bad timescale taxon");
    $T->ok_no_records("no records from bad timescale taxon");
    
    $T->fetch_nocheck("/timescales/bounds.json?extent=abcd", "bad timescale extent");
    $T->ok_no_records("no records from bad timescale extent");
    
    # Then min_ma, max_ma, created_before, created_after

    $T->fetch_nocheck("/timescales/bounds.json?min_ma=abcd", "bad min_ma value");
    $T->ok_response_code("400", "got 400 response from bad min_ma value");
    
    $T->fetch_nocheck("/timescales/bounds.json?max_ma=abcd", "bad max_ma value");
    $T->ok_response_code("400", "got 400 response from bad max_ma value");
    
    $T->fetch_nocheck("/timescales/bounds.json?all_records&created_before=abcd", "bad created_before value");
    $T->ok_response_code("400", "got 400 response from bad created_before value");
    
    $T->fetch_nocheck("/timescales/bounds.json?all_records&created_after=abcd", "bad created_after value");
    $T->ok_response_code("400", "got 400 response from bad created_after value");
    
    $T->fetch_nocheck("/timescales/bounds.json?all_records&modified_before=abcd", "bad modified_before value");
    $T->ok_response_code("400", "got 400 response from bad modified_before value");
    
    $T->fetch_nocheck("/timescales/bounds.json?all_records&modified_after=abcd", "bad modified_after value");
    $T->ok_response_code("400", "got 400 response from bad modified_after value");
    
    # Now check that no parameters at all returns a 400 response.

    $T->fetch_nocheck("/timescales/bounds.json", "no parameters at all");
    $T->ok_response_code("400", "got 400 response from no parameters at all");
};


# Now check that the list interals operation accepts all of the documented parameters, and that
# the results are as expected.

subtest 'interval params' => sub {
    
    select_subtest || return;
    
    my $INAMES = 'cretaceous,jurassic';
    
    # Check that we can query intervals from more than one timescale at a time.
    
    my $tsid_string = $ALL_TIMESCALES[0]{oid} . "," . $ALL_TIMESCALES[1]{oid};

    my @i1 = $T->fetch_records("/timescales/intervals.json?timescale_id=$tsid_string",
			       "fetch intervals from two timescales");
    
    my %tsid = $T->count_values(\@i1, 'sid');
    
    cmp_ok(keys %tsid, '==', 2, "got bounds from two timescales");
    
    # Check that we can query more than one interval by id.
    
    my $intid_string = $i1[0]{oid} . "," . $i1[1]{oid};
    
    my @i2 = $T->fetch_records("/timescales/intervals.json?interval_id=$intid_string", "fetch two intervals");
    
    cmp_ok(@i2, '==', 2, "got two intervals from two interval ids");
    
    # Check that we can query by interval name.
    
    my @i3 = $T->fetch_records("/timescales/intervals.json?interval_name=$INAMES", "fetch intervals '$INAMES'");
    
    my @b1 = $T->fetch_records("/timescales/bounds.json?interval_name=$INAMES", "fetch bounds '$INAMES'");

    if ( cmp_ok( @b1, '>=', 2, "got at least two bounds records" ) )
    {
	my $idlist = $b1[0]{oid} . ',' . $b1[1]{oid};
	
	my @i3a = $T->fetch_records("/timescales/intervals.json?bound_id=$idlist", "fetch intervals for '$idlist'");
	
	cmp_ok(@i3, '==', @i3a, "got same number of records by id as by name");
    }

    # Check for the proper function of the 'all_timescales' parameter.

    my @i3a = $T->fetch_records("/timescales/intervals.json?interval_name=$INAMES&all_timescales",
				"fetch intervals '$INAMES' from all timescales");

    cmp_ok( @i3a, '>', @i3, "all timescales fetched more records" );
    
    # Check that we can query by taxon, extent, type, and timescale name.
    
    my $tc = Test::Conditions->new;
    
    my @i4 = $T->fetch_records("/timescales/intervals.json?extent=global&show=desc",
			       "fetch intervals with extent 'global'");

    foreach my $r ( @i4 )
    {
	$tc->flag('ext', $r->{oid}) unless $r->{ext} && $r->{ext} =~ /global|international/i;
    }
    
    my @i5 = $T->fetch_records("/timescales/intervals.json?timescale_match=ics&show=desc",
			       "fetch intervals from timescales matching 'ics'");

    foreach my $r ( @i5 )
    {
	$tc->flag('tsn', $r->{oid}) unless $r->{tsn} && $r->{tsn} =~ /ics/i;
    }

    my @i5a = $T->fetch_records("/timescales/intervals.json?type=period&show=desc",
				"fetch intervals of type 'period'");

    foreach my $r ( @i5a )
    {
	$tc->flag('itp', $r->{oid}) unless $r->{itp} && $r->{itp} eq 'period';
    }
    
    $tc->ok_all("filtering by extent, type, and timescale naem works properly"); 
    
    if ( $TIMESCALE_TAXON )
    {
	my @i6 = $T->fetch_records("/timescales/intervals.json?taxon=$TIMESCALE_TAXON&show=desc",
				   "fetch timescales with taxon '$TIMESCALE_TAXON'");
	
	my $test_re = qr{$TIMESCALE_TAXON}i;

	foreach my $r ( @i6 )
	{
	    $tc->flag('txn', $r->{oid}) unless $r->{txn} && $r->{txn} =~ $test_re;
	}

	$tc->ok_all("returned records have proper taxon");
    }
};


subtest 'interval params 2' => sub {
    
    select_subtest || return;

    my $tc = Test::Conditions->new;
    
    # Check that we can query by min and max age.

    my @i1 = $T->fetch_records("/timescales/intervals.json?min_ma=100&max_ma=300", "fetch intervals using age range");
    
    my %tsid = $T->count_values(\@i1, 'sid');
    
    cmp_ok(keys %tsid, '>', 1, "got bounds from more than one timescale");

    foreach my $r ( @i1 )
    {
	$tc->flag('lag', $r->{oid}) unless defined $r->{lag} && $r->{lag} >= 100;
	$tc->flag('eag', $r->{oid}) unless defined $r->{eag} && $r->{eag} <= 300;
    }

    $tc->ok_all("filtering by min and max age works properly");
};


# Check that bad parameter values generate errors.

subtest 'interval params bad' => sub {
    
    select_subtest || return;
    
    # First check the 'id' parameters.
    
    $T->fetch_nocheck("/timescales/intervals.json?id=tsc:1", "bad id type");
    $T->ok_no_records("no records from bad id type");
    
    $T->fetch_nocheck("/timescales/intervals.json?bound_id=int:23", "bad id type 2");
    $T->ok_no_records("no records from bad id type 2");
    
    $T->fetch_nocheck("/timescales/intervals.json?timescale_id=bnd:23", "bad id type 3");
    $T->ok_no_records("no records from bad id type 3");
    
    $T->fetch_nocheck("/timescales/intervals.json?interval_id=tsc:1", "bad id type 4");
    $T->ok_no_records("no records from bad id type 4");
    
    # Then type, taxon, extent.
    
    $T->fetch_nocheck("/timescales/intervals.json?type=abcd", "bad bound type");
    $T->ok_no_records("no records from bad bound type");
    
    $T->fetch_nocheck("/timescales/intervals.json?taxon=abcd", "bad timescale taxon");
    $T->ok_no_records("no records from bad timescale taxon");
    
    $T->fetch_nocheck("/timescales/intervals.json?extent=abcd", "bad timescale extent");
    $T->ok_no_records("no records from bad timescale extent");
    
    # Then min_ma, max_ma, created_before, created_after

    $T->fetch_nocheck("/timescales/intervals.json?min_ma=abcd", "bad min_ma value");
    $T->ok_response_code("400", "got 400 response from bad min_ma value");
    
    $T->fetch_nocheck("/timescales/intervals.json?max_ma=abcd", "bad max_ma value");
    $T->ok_response_code("400", "got 400 response from bad max_ma value");
    
    $T->fetch_nocheck("/timescales/intervals.json?all_records&created_before=abcd", "bad created_before value");
    $T->ok_response_code("400", "got 400 response from bad created_before value");
    
    $T->fetch_nocheck("/timescales/intervals.json?all_records&created_after=abcd", "bad created_after value");
    $T->ok_response_code("400", "got 400 response from bad created_after value");
    
    $T->fetch_nocheck("/timescales/intervals.json?all_records&modified_before=abcd", "bad modified_before value");
    $T->ok_response_code("400", "got 400 response from bad modified_before value");
    
    $T->fetch_nocheck("/timescales/intervals.json?all_records&modified_after=abcd", "bad modified_after value");
    $T->ok_response_code("400", "got 400 response from bad modified_after value");
    
    # Now check that no parameters at all returns a 400 response.

    $T->fetch_nocheck("/timescales/intervals.json", "no parameters at all");
    $T->ok_response_code("400", "got 400 response from no parameters at all");
};




