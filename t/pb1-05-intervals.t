# 
# PBDB 1.1
# --------
# 
# Test the following operations:
# 
# /data1.1/intervals/single.json
# /data1.1/intervals/single.txt
# /data1.1/intervals/list.json
# /data1.1/intervals/list.txt
# /data1.1/scales/single.json
# /data1.1/scales/list.json
# 

use Test::Most tests => 5;

use JSON;
use Text::CSV_XS;

use lib 't';
use Tester;


my $T = Tester->new({ prefix => 'data1.1' });


# First define the values we will be using to check the interval operations.
# These are representative intervals from the database.

my $TEST_ID_1 = '14';
my $TEST_NAME_1 = 'Cretaceous';
my $TEST_ABR_1 = 'K';

my $TEST_ID_2 = '801';
my $TEST_NAME_2 = 'Payntonian';


my $t1 = { oid => $TEST_ID_1,
	   typ => "int",
	   nam => $TEST_NAME_1,
	   sca => "1",
	   lvl => "3",
	   abr => $TEST_ABR_1,
	   pid => "2",
	   eag => "!pos_num",
	   lag => "!pos_num",
	   col => qr{^#[0-9A-F]+$},
	   rid => [ "!pos_num" ],
	 };

my $t1t = { interval_no => $TEST_ID_1,
	    record_type => "interval",
	    scale_no => 1,
	    level => 3,
	    interval_name => $TEST_NAME_1,
	    abbrev => $TEST_ABR_1,
	    parent_no => 2,
	    early_age => "!pos_num",
	    late_age => "!pos_num",
	    color => qr{^#[0-9A-F]+$},
	    reference_no => "!pos_num",
	  };

my $EARLY_BOUND = 100;
my $LATE_BOUND = 50;


# First test intervals/single.  We check both .json and .txt responses.

my ($interval_id, $parent_id);

subtest 'single json' => sub {
    
    my $single_json = $T->fetch_url("intervals/single.json?id=$TEST_ID_1",
				    "single json request OK");
    
    unless ( $single_json )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # First check the we have the proper headers
    
    is( $single_json->header('Content-Type'), 'application/json; charset=utf-8', 'single json content-type' );
    is( $single_json->header('Access-Control-Allow-Origin'), '*', 'single json access-control-allow-origin' );
    
    # Then check the json response in detail
    
    my ($response, $r);
    
    eval {
	$response = decode_json( $single_json->content );
	$r = $response->{records}[0];
	$interval_id = $r->{oid};
	$parent_id = $r->{par};
    };
    
    ok( ref $r eq 'HASH' && keys %$r, 'single json content decoded') or return;
    
    # Then check the field values
    
    $T->check_fields($r, $t1, 'single json');
};


subtest 'single txt' => sub {

    my $single_txt = $T->fetch_url("intervals/single.txt?id=$TEST_ID_1",
				    "single txt request OK");
    
    unless ( $single_txt )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # First check the we have the proper headers
    
    is( $single_txt->header('Content-Type'), 'text/plain; charset=utf-8', 'single txt content-type' );
    is( $single_txt->header('Access-Control-Allow-Origin'), '*', 'single txt access-control-allow-origin' );
    
    # Then check the txt response in detail
    
    my ($response, $r);
    
    eval {
	($r) = $T->extract_records( $single_txt, 'single txt' );
    };
    
    # ok( ref $r eq 'HASH' && keys %$r, 'single txt content decoded') or
    # return;
    
    # Then check the field values
    
    $T->check_fields($r, $t1t, 'single txt');
};


# Then check intervals/list.  We check all of the parameters, including 'order'.

subtest 'list intervals' => sub {

    my $list_json = $T->fetch_url("intervals/list.json?vocab=pbdb&scale=1&max_ma=$EARLY_BOUND&min_ma=$LATE_BOUND",
				  "list json request OK");
    
    unless ( $list_json )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my (@r1, @r2, %found, $last_age);
    
    eval {
	@r1 = $T->extract_records( $list_json, 'list json' );
    };
    
    foreach my $r (@r1)
    {
	my $id = $r->{interval_no};
	$found{$id} = 1;
	
	ok( $r->{early_age} <= $EARLY_BOUND && $r->{late_age} >= $LATE_BOUND,
	    "list json interval $id bad age bounds" );
	
	ok( !defined $last_age || $r->{late_age} >= $last_age,
	    "list json interval $id bad order younger" );
	
	$last_age = $r->{late_age};
    }
    
    # Now fetch the intervals in reverse order and check to make sure the
    # interval numbers match.
    
    my $list_txt = $T->fetch_url("intervals/list.txt?scale=1&max_ma=$EARLY_BOUND&min_ma=$LATE_BOUND&order=older");
    
    unless ( $list_txt )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    eval {
	@r2 = $T->extract_records( $list_txt, 'list txt' );
    };
    
    $last_age = undef;
    
    foreach my $r (@r2)
    {
	my $id = $r->{interval_no};
	
	ok( $found{$id}, 'list txt interval $id not found' );
	delete $found{$id};
	
	ok( $r->{early_age} <= $EARLY_BOUND && $r->{late_age} >= $LATE_BOUND,
	    "list json interval $id bad age bounds" );
	
	ok( !defined $last_age || $r->{early_age} <= $last_age,
	    "list json interval $id bad order older" );
	
	$last_age = $r->{early_age};
    }
    
    ok( scalar(keys %found) == 0, 'ids do not match between older and younger' );
};


subtest 'list specific' => sub {

    my $list_json = $T->fetch_url("intervals/list.json?id=2,3,4",
				  "list json request OK");
    
    unless ( $list_json )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my (@r1, @r2, %found);
    
    eval {
	@r1 = $T->extract_records( $list_json, 'list json' );
    };
    
    foreach my $r (@r1)
    {
	$found{$r->{oid}} = 1;
    }
    
    cmp_ok( scalar(@r1), '==', 3, 'found 3 records' );
    
    ok( $found{2} && $found{3} && $found{4}, 'found requested records' );
};


subtest 'time scales' => sub {
    
    my $single_resp = $T->fetch_url("scales/single.json?id=1",
				  "single scale request OK");
    
    my $list_resp = $T->fetch_url("scales/list.json?id=1",
				  "scale list request OK");
    
    my $bad_resp = $T->fetch_url("scales/list.json?id=998,999",
				 "bad scales request OK");
    
    unless ( $single_resp && $list_resp && $bad_resp )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my (@r1, @r2, @r3);
    
    eval {
	@r1 = $T->extract_records( $single_resp, 'single scale' );
	@r2 = $T->extract_records( $list_resp, 'scale list' );
    };
    
    cmp_ok( scalar(@r1), '==', 1, 'single scale found 1 record' );
    cmp_ok( scalar(@r2), '==', 1, 'scale list found 1 record' );
    
    cmp_ok( $r1[0]{oid}, 'eq', '1', 'single scale found correct record' );
    cmp_ok( $r2[0]{oid}, 'eq', '1', 'scale list found correct record' );
    
    eval {
	@r3 = $T->extract_records( $bad_resp, 'bad scales', { no_records_ok => 1 } );
    };
    
    cmp_ok( scalar(@r3), '==', 0, 'no results from bad scales' );
};
