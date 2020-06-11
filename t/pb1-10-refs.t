# 
# PBDB 1.1
# --------
# 
# Test the following operations:
# 
# /data1.1/refs/single.json
# /data1.1/refs/single.txt
# /data1.1/refs/single.ris
# /data1.1/refs/list.txt

use Test::Most tests => 5;

use JSON;
use Text::CSV_XS;

use lib 't';
use Tester;


my $T = Tester->new({ prefix => 'data1.1' });


# SEE ALSO: pb1-07-occs.t - tests txt and ris formats

# First define the values we will be using to check the collection operations.
# These are representative taxa and collections from the database.

my $REF_ID_1 = '38149';
my $REF_ID_2 = '877';

my $AUTH_NAME_1 = 'Stevens';
my $YEAR_1 = '1834';

my $r1j = { oid => $REF_ID_1,
	    typ => "ref",
	    rtp => "ref",
	    ai1 => "A. L.",
	    al1 => "Adams",
	    pby => '1879',
	    tit => "On remains of Mastodon and other Vertebrata of the Miocene beds of the Maltese islands",
	    pbt => "Quarterly Journal of the Geological Society, London",
	    vol => '35',
	    pgf => '517',
	    pgl => '531',
	    pty => "journal article",
	    lng => "English",
	  };

my $r1f = { oid => $REF_ID_1,
	    typ => "ref",
	    rtp => "ref",
	    ref => qr{ Adams .* Mastodon }xs,
	  };

my $rx1 = { ati => '!numeric',
	    eni => '!numeric',
	    dcr => qr{^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$},
	    dmd => qr{^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$},
	  };

my $r1t = { reference_no => $REF_ID_1,
	    record_type => "reference",
	    ref_type => "ref",
	    author1init => "A. L.",
	    author1last => "Adams",
	    pubyr => '1879',
	    reftitle => "On remains of Mastodon and other Vertebrata of the Miocene beds of the Maltese islands",
	    pubtitle => "Quarterly Journal of the Geological Society, London",
	    pubvol => '35',
	    firstpage => '517',
	    lastpage => '531',
	    publication_type => "journal article",
	    language => "English",
	  };

my $r1r = "TY  - JOUR
ID  - paleobiodb:ref:38149
AU  - Adams,A.L.
PY  - 1879///
TI  - On remains of Mastodon and other Vertebrata of the Miocene beds of the Maltese islands
T2  - Quarterly Journal of the Geological Society, London
VL  - 35
SP  - 517
EP  - 531
LA  - English
ER  - ";

my $r2t = { reference_no => $REF_ID_2,
	    comments => qr{ Small \s+ Shelly \s+ Fossil }xsi,
	  };

# First test refs/single.json.  Beween this and the next test
# (refs/single.txt) we must also test all of the optional output blocks.


subtest 'single json' => sub {
    
    my $single_json = $T->fetch_url("refs/single.json?id=$REF_ID_1&show=ent,crmod",
				    "single json request OK");
    my $single_form = $T->fetch_url("refs/single.json?id=$REF_ID_1&show=formatted",
				    "single formatted request OK");
    
    unless ( $single_json && $single_form )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Then check the json response in detail
    
    my ($r1) = $T->extract_records($single_json, 'single json');
    
    ok( $r1, "single json found record" ) || return;
    
    $T->check_fields($r1, $r1j, 'single json');
    $T->check_fields($r1, $rx1, 'ent and crmod');
    
    ($r1) = $T->extract_records($single_form, 'single formatted');
    
    ok( $r1, "single formatted found record" ) || return;
    
    $T->check_fields($r1, $r1f, 'single formatted');
};


subtest 'comments' => sub {

    my $single_txt = $T->fetch_url("refs/single.txt?id=$REF_ID_2&show=comments",
				    "comments request OK");
    
    unless ( $single_txt )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my ($r) = $T->extract_records($single_txt, 'comments');
    
    $T->check_fields($r, $r2t, 'comments');
};


subtest 'single txt' => sub {

    my $single_txt = $T->fetch_url("refs/single.txt?id=$REF_ID_1",
				    "single txt request OK");
    
    unless ( $single_txt )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Then check the text response in detail
    
    my ($r1) = $T->extract_records($single_txt, 'single txt');
    
    ok( $r1, "single txt found record" );
    
    $T->check_fields($r1, $r1t, 'single txt');
};


subtest 'author' => sub {
    
    my $list_auth = $T->fetch_url("refs/list.json?author=$AUTH_NAME_1&limit=500",
				  "list author request OK");
    my $list_prim = $T->fetch_url("refs/list.json?primary=$AUTH_NAME_1&limit=500",
				  "list primary request OK");
    
    unless ( $list_auth && $list_prim )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my @auth = $T->extract_records($list_auth, 'list author extract');
    my @prim = $T->extract_records($list_prim, 'list primary extract');
    
    cmp_ok( scalar(@auth), '>', scalar(@prim), 'primary finds fewer records than author' );
    cmp_ok( scalar(@auth), '<', 500, 'not all records found' );
    
    my ($prim_count, $auth_count, $not_first_count);
    
    foreach my $r (@prim)
    {
	$prim_count++ if defined $r->{al1} && $r->{al1} eq $AUTH_NAME_1;
    }
    
    foreach my $r (@auth)
    {
	$not_first_count++ if defined $r->{al1} && $r->{al1} ne $AUTH_NAME_1;
	$auth_count++ if defined $r->{al1} && $r->{al1} eq $AUTH_NAME_1 ||
	    defined $r->{al2} && $r->{al2} eq $AUTH_NAME_1 ||
		defined $r->{oau} && $r->{oau} =~ qr{$AUTH_NAME_1};
    }
    
    cmp_ok( $prim_count, '==', scalar(@prim), 'primary author count' );
    cmp_ok( $auth_count, '==', scalar(@auth), 'general author count' );
    cmp_ok( $not_first_count, '>', 0, 'not first author count' );
};


subtest 'year' => sub {

    my $list_txt = $T->fetch_url("refs/list.json?year=$YEAR_1&limit=500",
				  "list year request OK");
    
    unless ( $list_txt )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my @r = $T->extract_records($list_txt, 'list year');
    
    my ($year_count);
    
    foreach my $r (@r)
    {
	$year_count++ if defined $r->{pby} && $r->{pby} eq $YEAR_1;
    }
    
    cmp_ok( $year_count, '==', scalar(@r), 'year count' );
};


# subtest 'refs json' => sub {
    
#     my $refs_json = $T->fetch_url("colls/refs.json?base_name=$BASE_NAME_1&interval=$INTERVAL_1a&year=1879",
# 				  "refs json request OK");
    
#     unless ( $refs_json )
#     {
# 	diag("skipping remainder of subtest");
# 	return;
#     }
    
#     my (@r) = $T->extract_records($refs_json, 'refs json', { no_records_ok => 1 } );
    
#     cmp_ok ( scalar(@r), '>', 0, 'refs json returned at least 1 entry' );
    
#     my $check;
    
#     foreach my $r (@r)
#     {
# 	if ( $r->{oid} eq $REF_ID_1 )
# 	{
# 	    $T->check_fields($r, $r1j, "ref $REF_ID_1");
# 	    $check = 1;
# 	    last;
# 	}
#     }
    
#     ok( $check, "found record $REF_ID_1" );
# };


# subtest 'summary bins' => sub {

#     my $summary_json = $T->fetch_url("colls/summary.json?base_name=$BASE_NAME_1&interval=$INTERVAL_1a&level=1&limit=5",
# 				     "summary request OK");
    
#     unless ( $summary_json )
#     {
# 	diag("skipping remainder of subtest");
# 	return;
#     }
    
#     my @r = $T->extract_records($summary_json, 'summary bins');
    
    
#     cmp_ok( scalar(@r), '==', 5, 'found 5 records');
# };
