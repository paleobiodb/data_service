# 
# PBDB 1.2
# --------
# 
# Test the following operations:
# 
# /data1.2/config.json ? show=all & count
# /data1.2/config.txt ? show=all
# /data1.2/config.csv ? show=all
# /data1.2/config.tsv ? show=all
#
# Test for errors:
# 
# /data1.2/config.foo
# /data1.2/config.json ? show=foo
# 
# Given the way in which Web::DataService works (separation of output
# serialization from output data generation) we will assume that if the data
# is properly serialized into each of the above formats for the 'config'
# operation then those serializations will also work properly for other
# operations.  If this later turns out not to be the case, we can add more
# tests.
# 

use strict;

use open ':std', ':encoding(utf8)';
use Test::Most tests => 8;

use lib 't';
use Tester;


# We start by creating a Tester instance that we will use for the subsequent tests:

my $T = Tester->new({ prefix => 'data1.2' });


# We first test the 'json' format.  We include in this test:
# 
# - content type header
# - access-control-allow-origin header
# - found attribute
# - returned attribute
# - elapsed attribute
# 
# If these are all proper for this request, we assume they will be proper for
# other requests in json format.  If that later turns out not to be the case, we can
# add more tests.
# 
# If we cannot process this basic request, there is no point in continuing
# with the other tests because the service is likely to be totally
# non-functional.  So in that case we bail out.

subtest 'config.json' => sub {

    bail_on_fail;
    
    my $config_json = $T->fetch_url("/config.json?show=all&rowcount", "config.json fetch");
    
    restore_fail;
    
    unless ( $config_json )
    {
	diag("skipping remainder of this subtest");
	return;
    }
        
    is( $config_json->header('Content-Type'), 'application/json; charset=utf-8', 'config.json content-type' );
    is( $config_json->header('Access-Control-Allow-Origin'), '*', 'config.json access-control-allow-origin' );
    
    my ($raw_data, $data, $found, $returned, $elapsed, $records);
    
    eval {
	$data = decode_json($config_json->content);
	$found = $data->{records_found};
	$returned = $data->{records_returned};
	$elapsed = $data->{elapsed_time};
	$records = $data->{records};
    };
    
    cmp_ok( $found, '>', 10, 'config.json found some records' );
    cmp_ok( $returned, '>', 10, 'config.json returned some records' );
    cmp_ok( $elapsed, '>', 0, 'config.json elapsed time reported' );
    
    cmp_ok( $returned, '==', scalar(@$records), 'returned count consistent' );
    
    my %section = $T->scan_records($config_json, 'cfg', 'config.json scan records');
    my %rank = $T->scan_records($config_json, 'rnk', 'config.json scan records');
    
    ok( $section{clu}, 'found at least one cluster' );
    ok( $rank{genus}, 'found rank \'genus\'' );
    ok( $section{con}, 'found at least one continent' );
    ok( $section{cou}, 'found at least one country' );
};


# Then we test the 'txt' format.  This also includes checking the following:
# 
# - content type header
# - content disposition header
# - access-control-allow-origin header
# - found attribute
# - returned attribute
# - elapsed attribute
# 
# If these are all proper for this request, we assume they will be proper for
# other requests in any of the text formats.  If that later turns out not to be the
# case, we can add more tests.

subtest 'config.txt' => sub {

    my $config_txt = $T->fetch_url("/config.txt?show=all&rowcount", "config.txt fetch");
    
    unless ( $config_txt )
    {
	diag("skipping remainder of this subtest");
	return;
    }
    
    is( $config_txt->header('Content-Type'), 'text/plain; charset=utf-8', 'config.txt content-type' );
    ok( ! $config_txt->header('Content-Disposition'), 'config.txt disposition');
    
    my $info = $T->extract_info($config_txt, "config.txt extract info");
    my @records = $T->extract_records($config_txt, "config.txt extract records", { type => 'rowcount' });
    
    my $found = $info->{"Records Found"} || 0;
    my $returned = $info->{"Records Returned"} || 0;
    my $elapsed = $info->{"Elapsed Time"} || 0;
    
    my(%section, %rank);
    
    foreach my $r ( @records )
    {
	$section{$r->{config_section}} = 1 if $r->{config_section};
	$rank{$r->{taxonomic_rank}} = $r->{rank_code} if $r->{taxonomic_rank};
    }
    
    cmp_ok( $found, '>', 10, 'config.txt found some records' );
    cmp_ok( $returned, '>', 10, 'config.txt returned some records' );
    cmp_ok( $elapsed, '>', 0, 'config.txt elapsed time reported' );
    
    ok( $section{'clu'}, 'config.txt found at least one cluster' );
    ok( $rank{'genus'}, 'config.txt found gank \'genus\'' );
    ok( $rank{'genus'} eq '5', 'config.txt found proper code for rank \'genus\'' );
    ok( $section{'con'}, 'config.txt found at least one continent' );
    
    cmp_ok( @records, '==', $returned, 'config.txt returned count consistent' );
};


subtest 'config.csv' => sub {
    
    my $config_csv = $T->fetch_url("/config.csv?show=all&rowcount", "config.csv fetch");
    
    unless ( $config_csv )
    {
	diag("skipping remainder of this subtest");
	return;
    }
    
    is( $config_csv->header('Content-Type'), 'text/csv; charset=utf-8', 'config.csv content-type' );
    is( $config_csv->header('Content-Disposition'), 'attachment; filename="pbdb_data.csv"', 'config.csv disposition');
    
    my $info = $T->extract_info($config_csv, "config.csv extract info");
    my @records = $T->extract_records($config_csv, "config.csv extract records", { type => 'rowcount' });
    
    my $found = $info->{"Records Found"} || 0;
    my $returned = $info->{"Records Returned"} || 0;
    my $elapsed = $info->{"Elapsed Time"} || 0;
    
    my(%section, %rank);
    
    foreach my $r ( @records )
    {
	$section{$r->{config_section}} = 1 if $r->{config_section};
	$rank{$r->{taxonomic_rank}} = $r->{rank_code} if $r->{taxonomic_rank};
    }
    
    cmp_ok( $found, '>', 10, 'config.csv found some records' );
    cmp_ok( $returned, '>', 10, 'config.csv returned some records' );
    cmp_ok( $elapsed, '>', 0, 'config.csv elapsed time reported' );
    
    ok( $section{'clu'}, 'config.csv found at least one cluster' );
    ok( $rank{'genus'}, 'config.csv found gank \'genus\'' );
    ok( $rank{'genus'} eq '5', 'config.csv found proper code for rank \'genus\'' );
    ok( $section{'con'}, 'config.csv found at least one continent' );
    
    cmp_ok( @records, '==', $returned, 'config.csv returned count consistent' );
};


subtest 'config.tsv' => sub {
    
    my $config_tsv = $T->fetch_url("/config.tsv?show=all&rowcount", "config.tsv fetch");
    
    unless ( $config_tsv )
    {
	diag("skipping remainder of this subtest");
	return;
    }
    
    is( $config_tsv->header('Content-Type'), 'text/tab-separated-values; charset=utf-8', 'config.tsv content-type' );
    is( $config_tsv->header('Content-Disposition'), 'attachment; filename="pbdb_data.tsv"', 'config.tsv disposition');
    
    my $info = $T->extract_info($config_tsv, "config.tsv extract info");
    my @records = $T->extract_records($config_tsv, "config.tsv extract records", { type => 'rowcount' });
    
    my $found = $info->{"Records Found"} || 0;
    my $returned = $info->{"Records Returned"} || 0;
    my $elapsed = $info->{"Elapsed Time"} || 0;
    
    my(%section, %rank);
    
    foreach my $r ( @records )
    {
	$section{$r->{config_section}} = 1 if $r->{config_section};
	$rank{$r->{taxonomic_rank}} = $r->{rank_code} if $r->{taxonomic_rank};
    }
    
    cmp_ok( $found, '>', 10, 'config.tsv found some records' );
    cmp_ok( $returned, '>', 10, 'config.tsv returned some records' );
    cmp_ok( $elapsed, '>', 0, 'config.tsv elapsed time reported' );
    
    ok( $section{'clu'}, 'config.tsv found at least one cluster' );
    ok( $rank{'genus'}, 'config.tsv found gank \'genus\'' );
    ok( $rank{'genus'} eq '5', 'config.tsv found proper code for rank \'genus\'' );
    ok( $section{'con'}, 'config.tsv found at least one continent' );
    
    cmp_ok( @records, '==', $returned, 'config.tsv returned count consistent' );
};


# Now test the bad media type response.

subtest 'config.foo' => sub {
    
    my $config_bad = $T->fetch_nocheck("/config.foo?show=all&rowcount", "config.foo fetch");
    
    unless ( $config_bad )
    {
	diag("skipping remainder of this subtest");
	return;
    }
    
    cmp_ok( $config_bad->code, 'eq', '415', 'config.foo returns 415' );
};


# And also a bad 'show' parameter

subtest 'config.json bad show' => sub {

    my $config_json = $T->fetch_nocheck("/config.json?show=foo", "config.json bad show");
    
    unless ( $config_json )
    {
	diag("skipping remainder of this subtest");
	return;
    }
    
    my ($raw_data, $data, @warnings);
    
    eval {
	$data = decode_json($config_json->content);
	@warnings = @{$data->{warnings}};
    };

    unless ( ok( !$@, 'config.json bad show unpack' ) )
    {
	diag( "    message was: $@" );
	return;
    }
    
    unless ( scalar(@warnings) == 2 )
    {
	fail( 'config.json bad show has 2 warnings' );
	return;
    }
    
    ok( $T->check_messages( \@warnings, qr{bad value 'foo'}i ), 'found bad value warning');
    ok( $T->check_messages( \@warnings, qr{output blocks.*specified}i ), 'found no output blocks warning');
};


# Now give some deliberately bad queries, to check that the error responses
# match what they are supposed to.

subtest 'error response 404' => sub {

    my $m404 = $T->fetch_nocheck("/taxa/single.json?name=not_a_taxon_name", 'not found json');
    
    unless ( $m404 )
    {
	diag("skipping remainder of this subtest");
	return;	
    }
    
    is( $m404->code, '404', 'not found json has code 404' );
    is( $m404->header("Content-Type"), 'application/json; charset=utf-8', 
	'not found json has proper content type' );
    
    my $json_404 = $T->decode_json_response($m404, 'not found json');
    my $message;
    
    is( $json_404->{status_code}, '404', "not found json has 'status code' of 404" );
    
    if( ref $json_404->{errors} eq 'ARRAY' && @{$json_404->{errors}} == 1 )
    {
	$message = $json_404->{errors}[0];
	like( $message, qr{not found}, "not found json error contains 'not found'" );
	like( $message, qr{not_a_taxon_name}, "not found json error contains argument" );
    }
    
    else
    {
	fail('not found json has one error');
    }
    
    my $m404a = $T->fetch_nocheck("/taxa/single.txt?name=not_a_taxon_name", 'not found txt');
    
    is( $m404a->code, '404', 'not found txt has code 404' );
    is( $m404a->header("Content-Type"), 'text/html; charset=utf-8', 
	'not found txt has proper content type' );
    
    my $body = $m404a->content;
    
    like( $body, qr{404}i, "not found txt respose contains '404'" );
    like( $body, qr{not found}, "not found txt response contains 'not found'" );
    like( $body, qr{not_a_taxon_name}, "not found txt response contains argument" );
    like( $body, qr{$message}, "not found txt response contains same message as json" ) if $message;
};


subtest 'error response 400' => sub {

    my $m400 = $T->fetch_nocheck("/taxa/single.json?foo=bar", 'bad param json');
    
    unless ( $m400 )
    {
	diag("skipping remainder of this subtest");
	return;	
    }
    
    is( $m400->code, '400', 'bad param json has code 400' );
    is( $m400->header("Content-Type"), 'application/json; charset=utf-8', 
	'bad param json has proper content type' );
    
    my $json_400 = $T->decode_json_response($m400, 'bad param json');
    my @messages;
    
    is( $json_400->{status_code}, '400', "bad param json has 'status code' of 400" );
    
    if( ref $json_400->{errors} eq 'ARRAY' && @{$json_400->{errors}} == 2 )
    {
	@messages = @{$json_400->{errors}};
	like( $messages[0], qr{parameter 'foo'}, "bad param json has bad parameter error" );
	like( $messages[1], qr{'name'.*'id'|'id'.*'name'}, "bad param json says proper params" );
    }
    
    else
    {
	fail('bad param json has two errors');
    }
    
    my $m400a = $T->fetch_nocheck("/taxa/single.txt?foo=bar", 'bad param txt');
    
    is( $m400a->code, '400', 'bad param txt has code 400' );
    is( $m400a->header("Content-Type"), 'text/html; charset=utf-8', 
	'bad param txt has proper content type' );
    
    my $body = $m400a->content;
    
    like( $body, qr{400}i, "bad param txt respose contains '400'" );
    like( $body, qr{parameter 'foo'}i, "bad param txt response contains 'bad param'" );
    like( $body, qr{'name'.*'id'|'id'.*'name'}, "bad param txt response says proper params" );
    
    foreach my $m (@messages)
    {
	ok( $body =~ qr{$m}, "bad param txt response contains message '$m'" );
    }
};
