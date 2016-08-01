# 
# PBDB 1.1
# --------
# 
# Test the following operations:
# 
# /data1.1/colls/single.json
# /data1.1/colls/single.txt
# /data1.1/colls/list.txt
# /data1.1/colls/summary.json
# /data1.1/colls/refs.txt

use Test::Most tests => 5;

use JSON;
use Text::CSV_XS;

use lib 't';
use Tester;


my $T = Tester->new();


# First define the values we will be using to check the collection operations.
# These are representative taxa and collections from the database.

my $COLL_ID_1 = '50068';
my $COLL_FM_1 = 'San Mateo';
my $COLL_NAME_1 = "Lawrence Canyon 3";
my $COLL_ATTR_1 = 'Barnes and Howard 1981';
my $INTERVAL_1 = 'Zanclean';
my $LOC_CC_1 = 'US';
my $LOC_STA_1 = 'California';
my $LOC_CNY_1 = 'San Diego';
my $LOC_GSC_1 = 'outcrop';

my $COLL_ID_2 = '50069';
my $COLL_ID_3 = '50070';

my $c1j = { oid => $COLL_ID_1,
	    typ => 'col',
	    sfm => $COLL_FM_1,
	    nam => $COLL_NAME_1,
	    att => $COLL_ATTR_1,
	    oei => $INTERVAL_1,
	    eag => '!pos_num',
	    lag => '!pos_num',
	    rid => [ '!pos_num' ],
	    cc2 => $LOC_CC_1,
	    sta => $LOC_STA_1,
	    cny => $LOC_CNY_1,
	    gsc => $LOC_GSC_1,
	    lng => '!numeric',
	    lat => '!numeric',
	    pln => '!numeric',
	    pla => '!numeric',
	    gpl => '!numeric',
	    cxi => '!numeric',
	    ein => '!numeric',
	    lin => '!numeric',
	  };

my $c1t = { collection_no => $COLL_ID_1,
	    record_type => 'collection',
	    formation => $COLL_FM_1,
	    collection_name => $COLL_NAME_1,
	    llp => 'UM',
	    early_interval => $INTERVAL_1,
	    late_interval => '',
	    early_age => '!empty',
	    late_age => '!empty',
	    reference_no => qr{ ^ [0-9,\s]+ $ }xs,
	    lng => '!numeric',
	    lat => '!numeric',
	    primary_reference => qr{Barnes},
	    authorizer => '!nonempty',
	    enterer => '!nonempty',
	    created => qr{^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$},
	    modified => qr{^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$},
	  };

my $BASE_NAME_1 = 'Cetacea';
my $INTERVAL_1a = 'Miocene';

my $REF_ID_1 = '38149';

my $r1j = { oid => $REF_ID_1,
	    typ => "ref",
	    rtp => "coll",
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


# First test colls/single.json.  Beween this and the next test
# (occs/single.txt) we must also test all of the optional output blocks.


subtest 'single json' => sub {
    
    my $single_json = $T->fetch_url("/data1.1/colls/single.json?id=$COLL_ID_1&show=loc,paleoloc,time,attr",
				    "single json request OK");
    
    unless ( $single_json )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Then check the json response in detail
    
    my ($r1) = $T->extract_records($single_json, 'single json');
    
    ok( $r1, "single json found record '$COLL_ID_1'" );
    
    $T->check_fields($r1, $c1j, 'single json');
};


subtest 'single txt' => sub {

    my $single_txt = $T->fetch_url("/data1.1/colls/single.txt?id=$COLL_ID_1&show=ref,entname,crmod",
				    "single txt request OK");
    
    unless ( $single_txt )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Then check the text response in detail
    
    my ($r1) = $T->extract_records($single_txt, 'single txt');
    
    ok( $r1, "single txt found record '$COLL_ID_1'" );
    
    $T->check_fields($r1, $c1t, 'single txt');
};


subtest 'list json' => sub {
    
    my $list_coll = $T->fetch_url("/data1.1/colls/list.json?coll_id=$COLL_ID_1,$COLL_ID_2,$COLL_ID_3&show=loc,paleoloc,time,attr",
				  "list coll request OK");
    
    unless ( $list_coll )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my @r = $T->extract_records($list_coll, 'list json');
    
    my ($matched);
    
    foreach my $r (@r)
    {
	if ( $r->{oid} eq $COLL_ID_1 )
	{
	    $T->check_fields($r, $c1j, "coll $COLL_ID_1");
	    $matched = 1;
	}
	
	ok( $r->{oid} eq $COLL_ID_1 || $r->{oid} eq $COLL_ID_2 || $r->{oid} eq $COLL_ID_3,
	    "id match $r->{oid}" );
    }
    
    ok( $matched, "found record $COLL_ID_1" );
    cmp_ok( scalar(@r), '==', 3, 'found 3 records' );
};


subtest 'refs json' => sub {
    
    my $refs_json = $T->fetch_url("/data1.1/colls/refs.json?base_name=$BASE_NAME_1&interval=$INTERVAL_1a&year=1879",
				  "refs json request OK");
    
    unless ( $refs_json )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my (@r) = $T->extract_records($refs_json, 'refs json', { no_records_ok => 1 } );
    
    cmp_ok ( scalar(@r), '>', 0, 'refs json returned at least 1 entry' );
    
    my $check;
    
    foreach my $r (@r)
    {
	if ( $r->{oid} eq $REF_ID_1 )
	{
	    $T->check_fields($r, $r1j, "ref $REF_ID_1");
	    $check = 1;
	    last;
	}
    }
    
    ok( $check, "found record $REF_ID_1" );
};


subtest 'summary bins' => sub {

    my $summary_json = $T->fetch_url("/data1.1/colls/summary.json?base_name=$BASE_NAME_1&interval=$INTERVAL_1a&level=1&limit=5",
				     "summary request OK");
    
    unless ( $summary_json )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my @r = $T->extract_records($summary_json, 'summary bins');
    
    
    cmp_ok( scalar(@r), '==', 5, 'found 5 records');
};
