# 
# PBDB 1.1
# --------
# 
# Test the following operations:
# 
# /data1.1/strata/list.json
# /data1.1/strata/auto.json

use Test::Most tests => 4;

use JSON;
use Text::CSV_XS;

use lib 't';
use Tester;


my $T = Tester->new();


# First define the values we will be using to check the strata operations.
# These are representative strata from the database.

my $TEST_NAME_1 = 'Green River';
my $TEST_RANK_1 = 'formation';
my $TEST_CHECK_1 = 'Green River';
my $TEST_BADRANK_1 = 'group';

my $t1 = { typ => 'str',
	    nam => $TEST_CHECK_1,
	    rnk => $TEST_RANK_1,
	    nco => "!pos_num",
	    noc => "!pos_num",
	  };

my $TEST_NAME_2 = 'Yoho%';
my $TEST_CHECK_2 = 'Yoho Shale';
my $TEST_RANK_2 = 'member';

my $t2 = { record_type => 'stratum',
	   name => $TEST_CHECK_2,
	   rank => $TEST_RANK_2,
	   n_colls => "!pos_num",
	   n_occs => "!pos_num",
	 };

my $COORDS = "lngmin=0&lngmax=15&latmin=0&latmax=15";
my $COORDS_RANK = 'group';
my $COORDS_COUNT = 5;

my $tc = { typ => 'str',
	   nam => '!nonempty',
	   rnk => $COORDS_RANK,
	   nco => '!numeric',
	   noc => '!numeric',
	 };

my $AUTO_NAME = 'aba';
my $AUTO_LIMIT = 10;
my $AUTO_COUNT = 5;

my $ta = { typ => 'str',
	   nam => '!nonempty',
	   rnk => '!nonempty',
	   nco => '!numeric',
	   noc => '!numeric',
	 };


# First test strata/list.  We check both .json and .txt responses.

subtest 'list json' => sub {
    
    my $list_json = $T->fetch_url("/data1.1/strata/list.json?name=$TEST_NAME_1&rank=$TEST_RANK_1",
				  "list json request OK");
    
    unless ( $list_json )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Then check the json response in detail
    
    my (@r, %found);
    
    eval {
	@r = $T->extract_records($list_json, 'list json');
    };
    
    foreach my $r (@r)
    {
	$found{$r->{nam}} = $r;
    }
    
    my $r1 = $found{$TEST_CHECK_1};
    
    ok( $r1, "list json found record '$TEST_NAME_1'" );
    
    $T->check_fields($r1, $t1, 'list json');
    
    # Now check that we get no results with a bad rank.
    
    my $list_bad = $T->fetch_url("/data1.1/strata/list.json?name=$TEST_NAME_1&rank=$TEST_BADRANK_1",
				  "list bad request OK");
    
    eval {
	@r = $T->extract_records($list_bad, 'list bad', { no_records_ok => 1 } );
    };
    
    cmp_ok( scalar(@r), '==', 0, 'bad rank returns no records' );
};


subtest 'list txt' => sub {
    
    my $list_txt = $T->fetch_url("/data1.1/strata/list.txt?name=$TEST_NAME_2&rank=$TEST_RANK_2",
				  "list txt request OK");
    
    unless ( $list_txt )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Then check the json response in detail
    
    my (@r, %found);
    
    eval {
	@r = $T->extract_records($list_txt, 'list txt');
    };
    
    foreach my $r (@r)
    {
	$found{$r->{name}} = $r;
    }
    
    my $r2 = $found{$TEST_CHECK_2};
    
    ok( $r2, "list txt found record '$TEST_NAME_2'" );
    
    $T->check_fields($r2, $t2, 'list txt');
};


subtest 'list coords' => sub {

    my $list_coords = $T->fetch_url("/data1.1/strata/list.json?$COORDS&rank=$COORDS_RANK",
				  "list coords request OK");
    
    unless ( $list_coords )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Then check the json response in detail
    
    my (@r, %found);
    
    eval {
	@r = $T->extract_records($list_coords, 'list coords');
    };
    
    cmp_ok( scalar(@r), '>=', $COORDS_COUNT, "list coords found at least $COORDS_COUNT records");
    
    foreach my $r (@r)
    {
	$T->check_fields($r, $tc, "coords result '$r->{nam}'") || last;
    }
};


subtest 'auto' => sub {

    my $auto_json = $T->fetch_url("/data1.1/strata/auto.json?name=$AUTO_NAME&limit=$AUTO_LIMIT");

    unless ( $auto_json )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Then check the json response in detail
    
    my (@r, %found);
    
    eval {
	@r = $T->extract_records($auto_json, 'strata auto');
    };
    
    cmp_ok( scalar(@r), '>=', $AUTO_COUNT, "auto found at least $AUTO_COUNT records");

    foreach my $r (@r)
    {
	$T->check_fields($r, $ta, "auto result '$r->{nam}'") || last;
    }
};
