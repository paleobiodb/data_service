# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test all of the /data1.2/ API operations used
# by Navigator.
# 

use strict;

use feature 'unicode_strings';
use feature 'fc';

use Test::Most tests => 8;

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

my ($FULL_COLLS, $FULL_OCCS, %FULL_CXI, $LEVEL_1_BINS);

my $unsigned_re = qr{ ^ [1-9] [0-9]* $ }xs;
my $nonneg_re = qr{ ^ [0-9] [0-9]* $ }xs;
my $latlng_re = qr{ ^ -? [0-9]+ (?: [.] [0-9]+ (?: [eE] -? [0-9]+ )? )? $ }xs;

# First test the calls necessary to initialize the Navigator application.

subtest 'initialization' => sub {
    
    select_subtest || return;
    
    # First test the 'intervals/list' operation. If it fails, there is no point
    # in continuing this test because the API is either not active or is broken.
    
    my @intervals = $T->fetch_records("/intervals/list.json?scale=1&order=age.desc&max_ma=4100");
    
    unless ( @intervals )
    {
	diag("skipping remainder of test");
	BAIL_OUT;
    }
    
    # Check all the fields of the first record that are used by Navigator.
    
    ok($intervals[0]{lag}, "first record includes nonzero late age") &&
	like($intervals[0]{lag}, qr/^\d+$/, "late age is a nonnegative integer");
    
    ok(defined $intervals[0]{eag}, "first record includes early age") &&
	like($intervals[0]{eag}, qr/^\d+$/, "early age is a nonnegative integer");
    
    like($intervals[0]{oid}, qr/^int:\d+$/, "first record oid");
    
    # The following line tests intervals[1] instead of intervals[0] because the
    # first interval returned should be Archean, which doesn't have a parent.
    
    like($intervals[1]{pid}, qr/^int:\d+$/, "second record pid");
    
    like($intervals[0]{nam}, qr/^\w+$/, "first record name");
    
    like($intervals[0]{itp}, qr/^\w+$/, "first record interval type");
    
    # Check that the response has approximately the right number of records.
    
    cmp_ok(@intervals, '>', 150, "intervals returns more than 150 records");
    
    # Next check the 'config' operation.
    
    my @config = $T->fetch_records("config.json?show=countries");
    
    # Check all the fields of the first record that are used by Navigator.
    
    is($config[0]{cfg}, "cou", "first record cfg");
    
    like($config[0]{nam}, qr/^[\w ]+$/, "first record name");
    
    like($config[0]{cod}, qr/^[A-Z][A-Z]$/, "first record country code");
    
    like($config[0]{con}, qr/^[A-Z][A-Z][A-Z]$/, "first record continent code");
    
    # Check that the response has approximately the right number of records.
    
    cmp_ok(@config, '>', 200, "config returns more than 200 records");
    
    # Now test the 'colls/summary' operation.
    
    my @summary = $T->fetch_records("/colls/summary.json?lngmin=-180&" .
				    "lngmax=180&latmin=-90&latmax=90&show=time&level=1");
    
    # Check all the fields of the first record that are used by Navigator.
    
    like($summary[0]{oid}, qr/^clu:\d+$/, "first record oid");
    
    like($summary[0]{lat}, qr/^-?\d+[.]\d+$/, "first record lat");
    
    like($summary[0]{lng}, qr/^-?\d+[.]\d+$/, "first record lng");
    
    like($summary[0]{nco}, qr/^\d+$/, "first record nco");
    
    like($summary[0]{noc}, qr/^\d+$/, "first record noc");
    
    like($summary[0]{cxi}, qr/^\d+$/, "first record cxi");
    
    # Check that the response has approximately the right number of records.
    
    cmp_ok(@summary, '>', 500, "summary returns more than 500 records");
    
    # Count the total number of collections and occurrences for comparison in subsequent
    # subtests.

    foreach my $record (@summary)
    {
	$FULL_COLLS += $record->{nco};
	$FULL_OCCS += $record->{noc};
	$FULL_CXI{$record->{cxi}} = 1 if defined $record->{cxi};
    }
    
    $LEVEL_1_BINS = scalar(@summary);
    
    # Now test the 'occs/prevalence' operation.
    
    my @prevalence = $T->fetch_records("/occs/prevalence.json?limit=10&lngmin=-180.0&" .
				       "lngmax=180.0&latmin=-90.0&latmax=90.0");
    
    # Check that the first record includes all of the fields used by Navigator.
    
    like($prevalence[0]{oid}, qr/^txn:\d+$/, "first record oid");
    
    like($prevalence[0]{nam}, qr/\w+/, "first record nam");
    
    like($prevalence[0]{rnk}, qr/^\d+$/, "first record rnk");
    
    like($prevalence[0]{img}, qr/^\d+$/, "first record img");
    
    like($prevalence[0]{noc}, qr/^\d+$/, "first record noc");
    
    # Check that the response contains approximately the right number of records.
    
    cmp_ok(@prevalence, '>=', 8, "prevalence returns at least 8 records");
    
    # Now check the 'taxa/thumb' operation.
    
    my @thumb = $T->fetch_records("/taxa/thumb.png?id=935");
    
    cmp_ok(@thumb, '==', 1, "thumb returns 1 record");
};


# Next, test the calls that are made after the user clicks on a specific interval.

subtest 'interval' => sub {

    select_subtest || return;
    
    my @prevalence = $T->fetch_records("/occs/prevalence.json?limit=10&lngmin=-180.0&lngmax=180.0&" .
				       "latmin=-90.0&latmax=90.0&interval_id=39");
    
    my @summary = $T->fetch_records("/colls/summary.json?lngmin=-180&lngmax=180&latmin=-90&latmax=90&" .
				    "show=time&level=1&interval_id=39");
    
    # Count up the number of collections and occurrences in the specific interval
    # summary, and check that they are substantially less than the count for the full
    # summary.
    
    my ($interval_colls, $interval_occs, %interval_cxi);
    
    foreach my $record ( @summary )
    {
	$interval_colls += $record->{nco};
	$interval_occs += $record->{noc};
	$interval_cxi{$record->{cxi}} = 1 if defined $record->{cxi};
    }
    
    # diag "Full cxi = " . scalar(%FULL_CXI);
    # diag "Interval cxi = " . scalar(%interval_cxi);
    
    cmp_ok($interval_colls, '<', $FULL_COLLS / 8, "interval summary reduced collections");
    cmp_ok($interval_occs, '<', $FULL_OCCS / 8, "interval summary reduced occurrences");
    cmp_ok(scalar(%interval_cxi), '<', scalar(%FULL_CXI) / 2, "interval summary reduced cxi");
};


# Test the calls made after the user zooms in.

subtest 'zoom' => sub {

    select_subtest || return;
    
    # Zoom in once.
    
    my @summary1 = $T->fetch_records("/colls/summary.json?lngmin=-100.8984&lngmax=110.5664&"
				     . "latmin=-61.1008&latmax=18.9790&level=2&show=time");
    
    my @prevalence = $T->fetch_records("/occs/prevalence.json?limit=10&lngmin=-100.9&" .
					"lngmax=110.6&latmin=-61.1&latmax=19.0");

    # Fetch the number of level-3 bins, and check that it is larger than the number of
    # level-1 bins.
    
    my @summary2 = $T->fetch_records("/colls/summary.json?lngmin=-180&lngmax=180&latmin=-90&" .
				     "latmax=90&show=time&level=3");

    # diag "level 1 bins = $LEVEL_1_BINS";
    # diag "level 3 bins = " . scalar(@summary2);
    
    cmp_ok($LEVEL_1_BINS, '<', scalar(@summary2) / 8, "more level 3 bins than level 1 bins");
};


# Test the calls made after the user clicks a specific interval while zoomed in.

subtest 'zoom-interval' => sub {

    select_subtest || return;
    
    # Select a specific interval
    
    my @prevalence = $T->fetch_records("/occs/prevalence.json?limit=10&lngmin=-52.4&lngmax=159.1&" .
				       "latmin=-33.9&latmax=51.9&interval_id=39");
    
    my @summary = $T->fetch_records("/colls/summary.json?lngmin=-180&lngmax=180&latmin=-90&" .
				    "latmax=90&show=time&level=3&interval_id=39");
};


# Test the calls made after the user clicks on one of the prevalence icons.

subtest 'prevalent-taxon' => sub {

    select_subtest || return;

    # Click on the prevalence icon for "Reptilia".
    
    my @taxa1 = $T->fetch_records("/taxa/list.json?name=Reptilia&show=seq");
    
    my @taxa2 = $T->fetch_records("/taxa/list.json?status=all&name=Reptilia");
    
    my @prevalence = $T->fetch_records("/occs/prevalence.json?limit=10&lngmin=-52.4&lngmax=159.1&" .
				       "latmin=-33.9&latmax=51.9&interval_id=39&base_id=36322");
    
    my @summary = $T->fetch_records("/colls/summary.json?lngmin=-52.3828&lngmax=159.0820&latmin=-33.8704&" .
				    "latmax=51.9443&level=3&show=time&interval_id=39&base_id=36322");
    
    my @taxa3 = $T->fetch_records("/taxa/single.json?id=txn:36322&show=attr,nav,size");
};


# Test the calls made after the user opens the diversity panel.

subtest 'diversity' => sub {
    
    select_subtest || return;
    
    # Click on the Diversity icon.

    my @quickdiv = $T->fetch_records("/occs/quickdiv.json?lngmin=-52.4&lngmax=159.1&latmin=-33.9&" .
				     "latmax=51.9&count=genera&reso=stage&interval_id=39&base_id=36322");
    
    my @intervals = $T->fetch_records("/intervals/list.json?scale=1&order=age.desc&max_ma=251.902&min_ma=0");
    
    # Click on the Advanced Diversity button.
    
    my @diversity = $T->fetch_records("/occs/diversity.json?lngmin=-52.4&lngmax=159.1&latmin=-33.9&" .
				      "latmax=51.9&count=genera&reso=stage&recent=false&interval_id=39&base_id=36322");
};


subtest 'more-diversity' => sub {
    
    my @diversity = $T->fetch_records("/occs/diversity.json?base_name=gastropoda&count=genera");
};


select_final;
