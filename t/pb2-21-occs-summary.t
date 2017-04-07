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

use Test::Most tests => 7;

# use CoreFunction qw(connectDB configData);
# use Taxonomy;

use lib 'lib', '../lib';

use ExternalIdent qw(%IDRE);
use TaxonDefs;

use lib 't';
use Tester;
use Test::Conditions;
use Test::Selection;


# If we provided any command-line arguments, run only subtests whose names match.

choose_subtests(@ARGV);

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });

# The following variables allow values fetched by one subtest to be used by subsequent tests.

my (%LEVEL_1_BIN_ID, %LEVEL_2_BIN_ID, $BIN_1_EMPTY, $BIN_2_EMPTY);
my $NAME_1 = 'Reptilia';
my $GENUS_2 = 'Cyclophthalmus';
my $NAME_3 = 'Scorpiones';
my $TAXON_ID_3 = 243100;

my $unsigned_re = qr{ ^ [1-9] [0-9]* $ }xs;
my $nonneg_re = qr{ ^ [0-9] [0-9]* $ }xs;
my $latlng_re = qr{ ^ -? [0-9]+ (?: [.] [0-9]+ (?: [eE] -? [0-9]+ )? )? $ }xs;


# First try a basic level 1 summary list, and check for proper field values in both json and text
# responses.  Save the level 1 bin ids to check below.

subtest 'summary basic' => sub {
    
    select_subtest || return;
    
    # First fetch the summary for large subtree.  We add 'show=time,ext' to test
    # all of the blocks.
    
    my @s1 = $T->fetch_records("/colls/summary.json?base_name=$NAME_1&level=1&show=ext,time,bin",
			       "colls summary 1 json '$NAME_1'");
    
    unless ( @s1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Then test each of the records.

    my $tc = Test::Conditions->new;
    
    test_summary_json($tc, \@s1, 1);
    
    # foreach my $r (@s1)
    # {
    # 	$tc->flag('bad oid', $r->{oid}) unless $r->{oid} && $r->{oid} =~ $IDRE{CLU};
    # 	$tc->flag('bad nco', $r->{oid}) unless $r->{nco} && $r->{nco} =~ $unsigned_re;
    # 	$tc->flag('bad noc', $r->{oid}) unless $r->{noc} && $r->{noc} =~ $unsigned_re;
    # 	$tc->flag('bad lng', $r->{oid}) unless defined $r->{lng} && $r->{lng} =~ $latlng_re;
    # 	$tc->flag('bad lat', $r->{oid}) unless defined $r->{lat} && $r->{lat} =~ $latlng_re;
    # 	$tc->flag('bad lx1', $r->{oid}) unless defined $r->{lx1} && $r->{lx1} =~ $latlng_re;
    # 	$tc->flag('bad lx2', $r->{oid}) unless defined $r->{lx2} && $r->{lx2} =~ $latlng_re;
    # 	$tc->flag('bad ly1', $r->{oid}) unless defined $r->{ly1} && $r->{ly1} =~ $latlng_re;
    # 	$tc->flag('bad ly2', $r->{oid}) unless defined $r->{ly2} && $r->{ly2} =~ $latlng_re;
    # 	$tc->flag('bad std', $r->{oid}) unless defined $r->{std} && $r->{std} =~ $latlng_re;
    # 	$tc->flag('bad cxi', $r->{oid}) unless defined $r->{cxi} && $r->{cxi} =~ $unsigned_re;
    # 	$tc->flag('unnecessary bin', $r->{oid}) if $r->{lv1};
	
    # 	$LEVEL_1_BIN_ID{$r->{oid}} = 'A' if $r->{oid};
    # }

    $tc->ok_all('summary basic 1 json');
    
    # Then again with text format.
    
    my @t1 = $T->fetch_records("/colls/summary.txt?base_name=$NAME_1&level=1&show=ext,time",
			       "colls summary 1 txt '$NAME_1'");
    
    foreach my $r (@t1)
    {
	unless ( $r->{lat} && $r->{lat} > 90 )
	{
	    $tc->flag('bad bin_id', $r->{oid}) unless $r->{bin_id} && $r->{bin_id} =~ $unsigned_re;
	}
	
	$tc->flag('bin_id does not match', $r->{bin_id}) if $r->{bin_id} && ! $LEVEL_1_BIN_ID{"clu:$r->{bin_id}"};
	$tc->flag('bad n_colls', $r->{oid}) unless $r->{n_colls} && $r->{n_colls} =~ $unsigned_re;
	$tc->flag('bad n_occs', $r->{oid}) unless $r->{n_occs} && $r->{n_occs} =~ $unsigned_re;
	$tc->flag('bad lng', $r->{oid}) unless defined $r->{lng} && $r->{lng} =~ $latlng_re;
	$tc->flag('bad lat', $r->{oid}) unless defined $r->{lat} && $r->{lat} =~ $latlng_re;
	$tc->flag('bad min_lng', $r->{oid}) unless defined $r->{min_lng} && $r->{min_lng} =~ $latlng_re;
	$tc->flag('bad max_lng', $r->{oid}) unless defined $r->{max_lng} && $r->{max_lng} =~ $latlng_re;
	$tc->flag('bad min_lat', $r->{oid}) unless defined $r->{min_lat} && $r->{min_lat} =~ $latlng_re;
	$tc->flag('bad max_lat', $r->{oid}) unless defined $r->{max_lat} && $r->{max_lat} =~ $latlng_re;
	$tc->flag('bad std_dev', $r->{oid}) unless defined $r->{std_dev} && $r->{std_dev} =~ $latlng_re;
	$tc->flag('bad cx_int_no', $r->{oid}) unless defined $r->{cx_int_no} && $r->{cx_int_no} =~ $unsigned_re;	
    }
    
    $tc->ok_all('summary basic 1 txt');
    
    my @c1 = $T->fetch_records("/occs/geosum.json?base_name=$NAME_1&level=1&show=ext", "occs geosum 1 json");
    
    ok( @c1 == @s1, "occs geosum fetches same number of elements" );
};


# Then check levels 2 and 3.

subtest 'summary levels' => sub {
    
    select_subtest || return;
    
    my @s2 = $T->fetch_records("/colls/summary.json?base_name=$NAME_1&level=2&show=ext,time,bin",
			       "colls summary 2 json '$NAME_1'");
    
    unless ( @s2 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    $BIN_1_EMPTY = 1 unless $s2[0]{lv1};
    
    # Then test each of the records.
    
    my $tc = Test::Conditions->new;
    
    test_summary_json($tc, \@s2, 2);
    
    $tc->ok_all('summary basic 2 json');
    
    my @s3 = $T->fetch_records("/colls/summary.json?base_name=$NAME_1&level=3&show=ext,time,bin",
			       "colls summary 2 json '$NAME_1'");
    
    $BIN_2_EMPTY = 1 unless $s3[0]{lv2};
    
    test_summary_json($tc, \@s3, 3);
    
    $tc->ok_all('summary basic 3 json');
    
    if ( $BIN_1_EMPTY || $BIN_2_EMPTY )
    {
	diag("NOTE: field 'bin_id_1' is empty.") if $BIN_1_EMPTY;
	diag("NOTE: field 'bin_id_2' is empty.") if $BIN_2_EMPTY;
    }
    
    else
    {
	foreach my $k ( keys %LEVEL_1_BIN_ID )
	{
	    $tc->flag('level 1 bin not found at level 2', $k) if $LEVEL_1_BIN_ID{$k} && $LEVEL_1_BIN_ID{$k} eq 'A';
	    $tc->flag('level 1 bin not found at level 3', $k) if $LEVEL_1_BIN_ID{$k} && $LEVEL_1_BIN_ID{$k} eq 'B';
	}
	
	foreach my $k ( keys %LEVEL_2_BIN_ID )
	{
	    $tc->flag('level 2 bin not found at level 3', $k) if $LEVEL_2_BIN_ID{$k} && $LEVEL_2_BIN_ID{$k} eq 'A';
	}
    }

    $tc->ok_all('higher bin levels match up');
};


sub test_summary_json {
    
    my ($tc, $records_ref, $level) = @_;
    
    foreach my $r ( @$records_ref )
    {
	unless ( $r->{lat} && $r->{lat} > 90 )
	{
	    $tc->flag('bad oid', $r->{oid}) unless $r->{oid} && $r->{oid} =~ $IDRE{CLU};
	}
	
	if ( $level == 1 )
	{
	    $tc->flag('unnecessary lv1', $r->{oid}) if $r->{lv1};
	    $tc->flag('unnecessary lv2', $r->{oid}) if $r->{lv2};
	    $tc->flag('unnecessary lv3', $r->{oid}) if $r->{lv3};
	    $LEVEL_1_BIN_ID{$r->{oid}} = 'A' if $r->{oid};
	}
	
	elsif ( $level == 2 )
	{
	    $tc->flag('missing lv1', $r->{oid}) unless $r->{lv1} || $BIN_1_EMPTY;
	    $tc->flag('lv1 does not match', $r->{oid}) if $r->{lv1} && ! $LEVEL_1_BIN_ID{$r->{lv1}};
	    $tc->flag('unnecessary lv2', $r->{oid}) if $r->{lv2};
	    $tc->flag('unnecessary lv3', $r->{oid}) if $r->{lv3};
	    $LEVEL_2_BIN_ID{$r->{oid}} = 'A' if $r->{oid};    
	    $LEVEL_1_BIN_ID{$r->{lv1}} = 'B' if $r->{lv1} && $LEVEL_1_BIN_ID{$r->{lv1}};
	}
	
	elsif ( $level == 3 )
	{
	    $tc->flag('missing lv1', $r->{oid}) unless $r->{lv1} || $BIN_1_EMPTY;
	    $tc->flag('lv1 does not match', $r->{oid}) if $r->{lv1} && ! $LEVEL_1_BIN_ID{$r->{lv1}};
	    $tc->flag('missing lv2', $r->{oid}) unless $r->{lv2} || $BIN_2_EMPTY;
	    $tc->flag('lv2 does not match', $r->{oid}) if $r->{lv2} && ! $LEVEL_2_BIN_ID{$r->{lv2}};
	    $tc->flag('unnecessary lv3', $r->{oid}) if $r->{lv3};
	    $LEVEL_2_BIN_ID{$r->{lv2}} = 'B' if $r->{lv2} && $LEVEL_2_BIN_ID{$r->{lv2}};
	    $LEVEL_1_BIN_ID{$r->{lv1}} = 'C' if $r->{lv1} && $LEVEL_1_BIN_ID{$r->{lv1}} && $LEVEL_1_BIN_ID{$r->{lv1}} eq 'B';
	}
	
	$tc->flag('lv1 does not match', $r->{oid}) if $r->{lv1} && ! $LEVEL_1_BIN_ID{$r->{lv1}};
	$tc->flag('bad nco', $r->{oid}) unless $r->{nco} && $r->{nco} =~ $unsigned_re;
	$tc->flag('bad noc', $r->{oid}) unless $r->{noc} && $r->{noc} =~ $unsigned_re;
	$tc->flag('bad lng', $r->{oid}) unless defined $r->{lng} && $r->{lng} =~ $latlng_re;
	$tc->flag('bad lat', $r->{oid}) unless defined $r->{lat} && $r->{lat} =~ $latlng_re;
	$tc->flag('bad lx1', $r->{oid}) unless defined $r->{lx1} && $r->{lx1} =~ $latlng_re;
	$tc->flag('bad lx2', $r->{oid}) unless defined $r->{lx2} && $r->{lx2} =~ $latlng_re;
	$tc->flag('bad ly1', $r->{oid}) unless defined $r->{ly1} && $r->{ly1} =~ $latlng_re;
	$tc->flag('bad ly2', $r->{oid}) unless defined $r->{ly2} && $r->{ly2} =~ $latlng_re;
	$tc->flag('bad std', $r->{oid}) unless defined $r->{std} && $r->{std} =~ $latlng_re;
	$tc->flag('bad cxi', $r->{oid}) unless defined $r->{cxi} && $r->{cxi} =~ $unsigned_re;
    }
}


# Now check all_records, with rowcount to make sure that those parameters are properly accepted.
# Also check to make sure that we have a proper cxi value for each top-level bin.

subtest 'summary all_records' => sub {
    
    select_subtest || return;
    
    my (@s1) = $T->fetch_records("/colls/summary.json?all_records&rowcount&level=1&show=time",
				 "colls summary 1 all_records");
    
    unless ( @s1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my $tc = Test::Conditions->new;
    my (%cxi_no);
    
    foreach my $r (@s1)
    {
	$tc->flag('bad cxi', $r->{oid}) unless defined $r->{cxi} && $r->{cxi} =~ $nonneg_re;
	$cxi_no{$r->{cxi}} = 1 if $r->{cxi};
    }
    
    $tc->ok_all('cxi okay');
    cmp_ok( keys %cxi_no, '>', 5, "found at least 5 distinct cxi values" );
};


# Now check geographic bounds.

subtest 'geographic bounds' => sub {
    
    select_subtest || return;
    
    my $XMIN = -80;
    my $XMAX = -70;
    my $YMIN = 40;
    my $YMAX = 50;

    my $CC3 = "SOA";

    my $XMINCC3 = -100;
    my $XMAXCC3 = -30;
    my $YMINCC3 = -60;
    my $YMAXCC3 = 25;
    
    my (@s2) = $T->fetch_records("/occs/geosum.json?level=1&lngmin=$XMIN&lngmax=$XMAX&latmin=$YMIN&latmax=$YMAX",
				 "occs geosum 1 geographic bounds");
    
    my $tc = Test::Conditions->new;
    
    foreach my $r (@s2)
    {
	$tc->flag('lng out of bounds', $r->{oid}) unless $r->{lng} && $r->{lng} >= $XMIN && $r->{lng} <= $XMAX;
	$tc->flag('lat out of bounds', $r->{oid}) unless $r->{lat} && $r->{lat} >= $YMIN && $r->{lat} <= $YMAX;
    }
    
    $tc->ok_all('geographic bounds match query');

    my (@c2) = $T->fetch_records("/occs/geosum.json?level=1&cc=$CC3", "occs geosum cc $CC3");

    my ($x1, $x2, $y1, $y2);

    foreach my $r (@c2)
    {
	$tc->flag('lng out of bounds cc3', $r->{oid}) unless $r->{lng} &&
	    $r->{lng} >= $XMINCC3 && $r->{lng} <= $XMAXCC3;
	$tc->flag('lat out of bounds cc3', $r->{oid}) unless $r->{lat} &&
	    $r->{lat} >= $YMINCC3 && $r->{lat} <= $YMAXCC3;
    }
    
    # diag("XMIN: $x1");
    # diag("XMAX: $x2");
    # diag("YMIN: $y1");
    # diag("YMAX: $y2");
};


# Then check various parameters for specifying time bounds.

subtest 'time bound parameters' => sub {

    select_subtest || return;
    
    my (@s3a) = $T->fetch_records("/occs/geosum.json?base_name=$NAME_1&level=3&interval=cretaceous",
				 "occs geosum 3 interval 'cretaceous'");
    
    unless ( @s3a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my (@s3b) = $T->fetch_records("/occs/geosum.json?base_name=$NAME_1&level=3&interval_id=14",
				  "occs geosum 3 interval_no 14");
    
    cmp_ok( @s3b, '==', @s3a, "interval_no and interval found same list");
    
    my (@s3c) = $T->fetch_records("/occs/geosum.json?base_name=$NAME_1&level=3&max_ma=145&min_ma=66",
				 "occs geosum 3 ma bounds");
    
    cmp_ok( @s3c, '==', @s3a, "ma bounds and interval found same list");
};


# Check various parameters for specifying taxonomic names.

subtest 'taxonomic name parameters' => sub {
    
    select_subtest || return;
    
    my (@s1a) = $T->fetch_records("/colls/summary.json?base_name=$NAME_3&level=1",
				  "colls summary 1 json '$NAME_1'");

    unless ( @s1a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my (@s1b) = $T->fetch_records("/colls/summary.json?base_id=$TAXON_ID_3&level=1",
				  "colls summary 1 json id $TAXON_ID_3");

    cmp_ok( @s1b, '==', @s1a, "taxon name and taxon id found same list" );
    
    my (@s1c) = $T->fetch_records("/colls/summary.json?match_name=$GENUS_2%20%&level=1",
				  "colls summary 1 json match '$GENUS_2 %'");
    
};

# Then check other occurrence filters.

subtest 'summary occ filters' => sub {

    select_subtest || return;
    
    my (@s3a) = $T->fetch_records("/occs/geosum.json?base_name=$NAME_1&level=3",
				  "occs geosum 3 base_name '$NAME_1'");

    unless ( @s3a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my (@s3b) = $T->fetch_records("/occs/geosum.json?base_name=$NAME_1&level=3&idqual=new",
				  "occs geosum 3 base_name '$NAME_1' idqual=new");

    cmp_ok( @s3b, '<', @s3a, "found fewer records with idqual=new" );
};


select_final;
