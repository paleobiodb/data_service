# 
# PBDB 1.1
# --------
# 
# Test the following operations:
# 
# /data1.1/occs/single.json
# /data1.1/occs/single.txt
# /data1.1/occs/list.txt
# /data1.1/occs/refs.ris
# /data1.1/occs/refs.txt
# /data1.1/occs/refs.json
# /data1.1/occs/taxa.txt

use Test::Most tests => 8;

use JSON;
use Text::CSV_XS;

use lib 't';
use Tester;


my $T = Tester->new();


# First define the values we will be using to check the occurrences operations.
# These are representative taxa and occurrences from the database.

my $OCC_ID_1 = '1001';
my $OCC_NAME_1 = 'Wellerella sp.';
my $OCC_MATCH_1 = 'Wellerella';
my $COLL_ID_1 = '160';
my $TAXON_ID_1 = '29018';
my $INTERVAL_1 = 'Missourian';
my $LOC_CC_1 = 'US';
my $LOC_STA_1 = 'Missouri';
my $LOC_CNY_1 = 'Clinton';

my $o1j = { oid => $OCC_ID_1,
	    typ => 'occ',
	    cid => $COLL_ID_1,
	    tna => $OCC_NAME_1,
	    rnk => 5,
	    tid => $TAXON_ID_1,
	    mna => $OCC_MATCH_1,
	    mra => 5,
	    mid => $TAXON_ID_1,
	    oei => $INTERVAL_1,
	    eag => '!pos_num',
	    lag => '!pos_num',
	    rid => [ '!pos_num' ],
	    cc2 => $LOC_CC_1,
	    sta => $LOC_STA_1,
	    cny => $LOC_CNY_1,
	    lng => '!numeric',
	    lat => '!numeric',
	    pln => '!numeric',
	    pla => '!numeric',
	    gpl => '!numeric',
	    cxi => '!numeric',
	    ein => '!numeric',
	    lin => '!numeric',
	  };

my $o1c = { gnl => 'Wellerella', gnn => '!pos_num',
	    fml => 'Wellerellidae', fmn => '!pos_num',
	    odl => 'Rhynchonellida', odn => '!pos_num',
	    cll => 'Rhynchonellata', cln => '!pos_num',
	    phl => 'Brachiopoda', phn => '!pos_num',
	  };

my $o1t = { occurrence_no => $OCC_ID_1,
	    record_type => 'occurrence',
	    collection_no => $COLL_ID_1,
	    taxon_name => $OCC_NAME_1,
	    taxon_rank => 'genus',
	    taxon_no => $TAXON_ID_1,
	    matched_name => $OCC_MATCH_1,
	    matched_rank => 'genus',
	    matched_no => $TAXON_ID_1,
	    early_interval => $INTERVAL_1,
	    late_interval => '',
	    early_age => '!pos_num',
	    late_age => '!pos_num',
	    reference_no => qr{^[\d\s,]+$},
	    genus => 'Wellerella',
	    genus_no => '!pos_num',
	    formation => 'Plattsburg',
	    member => 'Hickory Creek Shale',
	    lithology1 => '"shale"',
	    environment => 'marine indet.',
	    primary_reference => qr{Malinky.*Paleoecology and taphonomy},
	    authorizer => '!nonempty',
	    enterer => '!nonempty',
	    created => qr{^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$},
	    modified => qr{^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$},
	  };

my $OCC_ID_2 = '1002';
my $OCC_ID_3 = '1003';
my $COLL_OCC_COUNT = 5;

my $BASE_NAME_1 = 'Cetacea';
my $INTERVAL_1a = 'Miocene';

my $REF_ID_1 = '38149';

my $r1j = { oid => $REF_ID_1,
	    typ => "ref",
	    rtp => "occ",
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

my $r1t = { reference_no => $REF_ID_1,
	    record_type => "reference",
	    ref_type => "occ",
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

my $TAXON_NAME_1 = 'Balaenoptera rostrata';


# First test occs/single.json.  Beween this and the next test
# (occs/single.txt) we must also test all of the optional output blocks.


subtest 'single json' => sub {
    
    my $single_json = $T->fetch_url("/data1.1/occs/single.json?id=$OCC_ID_1&show=loc,coords,phylo,paleoloc,time",
				    "single json request OK");
    
    unless ( $single_json )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Then check the json response in detail
    
    my ($r1);
    
    eval {
	($r1) = $T->extract_records($single_json, 'single json');
    };
    
    ok( $r1, "single json found record '$OCC_ID_1'" );
    
    $T->check_fields($r1, $o1j, 'single json');
    $T->check_fields($r1, $o1c, 'single json');
};


subtest 'single txt' => sub {

    my $single_txt = $T->fetch_url("/data1.1/occs/single.txt?id=$OCC_ID_1&show=genus,stratext,lithext,geo,ref,entname,crmod",
				    "single txt request OK");
    
    unless ( $single_txt )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Then check the text response in detail
    
    my ($r1);
    
    eval {
	($r1) = $T->extract_records($single_txt, 'single txt');
    };
    
    ok( $r1, "single txt found record '$OCC_ID_1'" );
    
    $T->check_fields($r1, $o1t, 'single txt');

};


subtest 'list json' => sub {
    
    my $list_coll = $T->fetch_url("/data1.1/occs/list.json?coll_id=$COLL_ID_1",
				  "list coll request OK");
    
    unless ( $list_coll )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my (@r) = $T->extract_records($list_coll, 'list coll');
    
    cmp_ok( scalar(@r), '>', $COLL_OCC_COUNT, "list coll returned at least $COLL_OCC_COUNT results" );
    
    my $list_occs = $T->fetch_url("/data1.1/occs/list.json?id=$OCC_ID_1,$OCC_ID_2,$OCC_ID_3",
				  "list occs request OK");
    
    my (%found) = $T->scan_records($list_occs, 'oid', 'list occs');
    
    cmp_ok( scalar(keys %found), '==', 3, 'list occs retrieved 3 records' );
    ok( $found{$OCC_ID_1} && $found{$OCC_ID_2} && $found{$OCC_ID_3}, 'list occs retrieved correct records' );
};


subtest 'list json 2' => sub {
    
    my $list_json = $T->fetch_url("/data1.1/occs/list.json?base_name=$BASE_NAME_1&interval=$INTERVAL_1a&limit=100",
				  "list json request OK");
    
    unless ( $list_json )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my (@r) = $T->extract_records($list_json, 'list json', { no_records_ok => 1 } );
    
    cmp_ok( scalar(@r), '==', 100, 'list json returned correct number of results' );
};


subtest 'refs json' => sub {
    
    my $refs_json = $T->fetch_url("/data1.1/occs/refs.json?base_name=$BASE_NAME_1&interval=$INTERVAL_1a&year=1879",
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


subtest 'refs txt' => sub {
    
    my $refs_txt = $T->fetch_url("/data1.1/occs/refs.txt?base_name=$BASE_NAME_1&interval=$INTERVAL_1&year=1879",
				  "refs txt request OK");
    
    unless ( $refs_txt )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my (@r) = $T->extract_records($refs_txt, 'refs txt', { no_records_ok => 1 } );
    
    my $check;
    
    foreach my $r (@r)
    {
	if ( $r->{reference_no} eq $REF_ID_1 )
	{
	    $T->check_fields($r, $r1t, "ref $REF_ID_1");
	    $check = 1;
	    last;
	}
    }
    
    ok( $check, "found record $REF_ID_1" );
};


subtest 'refs ris' => sub {

    my $refs_ris = $T->fetch_url("/data1.1/occs/refs.ris?base_name=$BASE_NAME_1&interval=$INTERVAL_1a&year=1879",
				  "refs ris request OK");
    
    unless ( $refs_ris )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my $content = $refs_ris->content;
    my $r;
    
    if ( $content =~ qr{ ( ^TY\s\s-\s .*? 38149 .*? ^ER\s\s-\s ) }xms )
    {
	$r = $1;
	$r =~ s/\r\n/\n/g;
	pass("found record $REF_ID_1" );
	cmp_ok( $r, 'eq', $r1r, 'content matched template' );
    }
    
    else
    {
	fail("found record $REF_ID_1");
    }
};


subtest 'occs taxa' => sub {

    my $occs_taxa = $T->fetch_url("/data1.1/occs/taxa.txt?base_name=$BASE_NAME_1&interval=$INTERVAL_1a&limit=1000",
				  "list json request OK");

    unless ( $occs_taxa )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my (%found) = $T->scan_records($occs_taxa, 'taxon_name', 'list json' );
    
    ok( $found{$TAXON_NAME_1}, "found taxon '$TAXON_NAME_1'" );
};
