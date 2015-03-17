# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# This file tests the 'datainfo' and 'rowcount' responses, together with
# 'count' and 'limit' parameters and the headers 'Content-type' and
# 'Access-control-allow-origin'.  The operations tested are:
# 
# /taxa/single
# /taxa/list
# /taxa/refs
# 
# The output formats tested are:
# 
#   json
#   txt
#   ris
# 
# We are making the assumption that if datainfo and rowcount work for these
# operations then they will work for all, since they are handled by
# Web::DataService and not by the code itself.  The limit and count parameters
# should probably be tested individually for each operation, since they depend
# upon proper coding of the operation methods.
# 

use lib '../lib';

use Test::Most tests => 5;

use JSON;
use Text::CSV_XS;

use lib 't';
use Tester;


my $T = Tester->new();


# First define the values we will be using to check the taxonomy operations.
# These are representative taxa from the database.

my $SERVICE_TITLE = "The Paleobiology Database";

my $TEST_NAME_1 = 'Dascillidae';

my @TEST_AUTHOR_7a = ('Crowson');
my $TEST_TITLE_7a = 'The Biology of the Coleoptera';

my $t1 = { 'nam' => $TEST_NAME_1,
	   'typ' => "txn",
	 };
	   
my $t1_num = { 'oid' => 1 };

my $t1t = { 'taxon_name' => $TEST_NAME_1,
	    'record_type' => "taxon",
	  };

my $t1t_num = { 'taxon_no' => 1, 'orig_no' => 1 };

# Then the fields and values to expect as a result of the 'datainfo' parameter.

my $OP1 = "/data1.2/taxa/single";
my $OP2 = "/data1.2/taxa/list";
my $OP3 = "/data1.2/taxa/refs";

my $LIMIT_1 = "5";

my $ARG1 = "name=$TEST_NAME_1&show=attr,app,size,phylo&rowcount&datainfo";
my $ARG2 = "base_name=$TEST_NAME_1&show=attr,app,size,phylo&rowcount&datainfo&limit=$LIMIT_1";
my $ARG3 = "base_name=$TEST_NAME_1&rowcount&datainfo&limit=$LIMIT_1";

my $info = { "data_provider" => $SERVICE_TITLE,
	     "data_source" => $SERVICE_TITLE,
	     "data_license" => "Creative Commons CC-BY",
	     "license_url" => qr{ ^ http://creativecommons[.]org/\w+ }xs,
	     "access_time" => qr{ \d\d\d\d-\d\d-\d\d .* GMT $ }xs,
	     "title" => "PBDB Data Service" };

my $infos = { "documentation_url" => $T->make_url("${OP1}_doc.html"),
	      "data_url" => $T->make_url("${OP1}.json?$ARG1") };

my $infol = { "documentation_url" => $T->make_url("${OP2}_doc.html"),
	      "data_url" => $T->make_url("${OP2}.json?$ARG2") };

my $infot = { "Data Provider" => $SERVICE_TITLE,
	      "Data Source" => $SERVICE_TITLE,
	      "Data License" => "Creative Commons CC-BY",
	      "License URL" => qr{ ^ http://creativecommons[.]org/\w+ }xs,
	      "Access Time" => qr{ \d\d\d\d-\d\d-\d\d .* GMT $ }xs,
	      "Title" => "PBDB Data Service" };

my $infost = { "Documentation URL" => $T->make_url("${OP1}_doc.html"),
	       "Data URL" => $T->make_url("${OP1}.txt?$ARG1") };

my $infolt = { "Documentation URL" => $T->make_url("${OP2}_doc.html"),
	       "Data URL" => $T->make_url("${OP2}.txt?$ARG2") };

my $params = { "name" => $TEST_NAME_1,
	       "show" => "attr,app,size,phylo" };

my $paramsl = { "base_name" => $TEST_NAME_1,
		"show" => "attr,app,size,phylo",
		"status" => "all",
		"limit" => $LIMIT_1 };

my $rcs = { "elapsed_time" => qr{\d[.]\d}xs,
	    "records_found" => "1",
	    "records_returned" => "1" };

my $rcst = { "Elapsed Time" => qr{\d[.]\d}xs,
	     "Records Found" => "1",
	     "Records Returned" => "1" };

my $rcl = { "elapsed_time" => qr{\d[.]\d}xs,
	    "records_returned" => $LIMIT_1 };

my $rclt = { "Elapsed Time" => qr{\d[.]\d}xs,
	     "Records Returned" => $LIMIT_1 };

# Then do some initial fetches using the taxon name.  Once we get a taxon
# identifier back, we will use that to test fetching by identifier.
# 
# We check both .json and .txt responses.

subtest 'single json info' => sub {
    
    my $single_json = $T->fetch_url("${OP1}.json?$ARG1", "single json request OK");
    
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
    };
    
    ok( ref $r eq 'HASH' && keys %$r, 'single json content decoded') or return;
    
    # Check the 'datainfo' fields
    
    $T->check_fields($response, $info, 'single json datainfo');
    $T->check_fields($response, $infos, 'single json datainfo');
    
    # Check the 'datainfo' parameter list
    
    $T->check_fields($response->{parameters}, $params, 'single json parameter');
    
    # Check the rowcount fields
    
    $T->check_fields($response, $rcs, 'single json');
    
    # Check a few basic data fields, just to make sure that we are getting
    # some data back.  The /taxa/single response will be checked more fully in
    # pb2-04-taxa.t.
    
    $T->check_fields($r, $t1, 'single json field');
};


subtest 'single txt info' => sub {
    
    my $single_txt = $T->fetch_url("${OP1}.txt?$ARG1", "single txt request OK");
    
    unless ( $single_txt )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Now check the txt response in detail 
    
    is( $single_txt->header('Content-Type'), 'text/plain; charset=utf-8', 'single txt content-type' );
    
    my ($info) = $T->extract_info($single_txt, 'single txt extract info');
    
    my ($r) = $T->extract_records($single_txt, 'single txt extract records', { type => 'info' } );
    
    # Check the 'datainfo' fields
    
    $T->check_fields($info, $infot, 'single txt datainfo');
    $T->check_fields($info, $infost, 'single txt datainfo');
    
    # Check the 'datainfo' parameter list
    
    $T->check_fields($info->{parameters}, $params, 'single txt parameter');
    
    # Check the rowcount fields
    
    $T->check_fields($info, $rcst, 'single txt');
    
    # Check a few basic data fields, just to make sure that we are getting
    # some data back.  The /taxa/single response will be checked more fully in
    # pb2-04-taxa.t.
    
    $T->check_fields($r, $t1t, 'single txt field');
};


# Next we test the various 'list' operations:

subtest 'list json info' => sub {
    
    my $list_json = $T->fetch_url("${OP2}.json?$ARG2", "list json request OK");
    
    unless ( $list_json )
    {
	diag("skipping remainder of subtest");
	return;
    }

    # First check that we have the proper headers
    
    is( $list_json->header('Content-Type'), 'application/json; charset=utf-8', 'list json content-type' );
    is( $list_json->header('Access-Control-Allow-Origin'), '*', 'list json access-control-allow-origin' );
    
    # Then check the json response in detail
    
    my ($response, $r);
    
    eval {
	$response = decode_json( $list_json->content );
	$r = $response->{records}[0];
    };
    
    ok( ref $r eq 'HASH' && keys %$r, 'list json content decoded') or return;
    
    # Check the 'datainfo' fields
    
    $T->check_fields($response, $info, 'list json datainfo');
    $T->check_fields($response, $infol, 'list json datainfo');
    
    # Check the 'datainfo' parameter list
    
    $T->check_fields($response->{parameters}, $paramsl, 'list json parameter');
    
    # Check the rowcount fields
    
    $T->check_fields($response, $rcl, 'list json');
    cmp_ok( $response->{records_found}, '>', $response->{records_returned}, 'list json records found is greater' );
    
    # Check a few basic data fields, just to make sure that we are getting
    # some data back.  The /taxa/single response will be checked more fully in
    # pb2-04-taxa.t.
    
    $T->check_fields($r, $t1, 'single json field');
};


subtest 'list txt info' => sub {
    
    my $list_txt = $T->fetch_url("${OP2}.txt?$ARG2", "list txt request OK");
    
    unless ( $list_txt )
    {
	diag("skipping remainder of subtest");
	return;
    }

    # Now check the txt response in detail 
    
    is( $list_txt->header('Content-Type'), 'text/plain; charset=utf-8', 'list txt content-type' );
    
    my ($info) = $T->extract_info($list_txt, 'list txt extract info');
    
    my ($r) = $T->extract_records($list_txt, 'list txt extract records', { type => 'info' } );
    
    # Check the 'datainfo' fields
    
    $T->check_fields($info, $infot, 'list txt datainfo');
    $T->check_fields($info, $infolt, 'list txt datainfo');
    
    # Check the 'datainfo' parameter list
    
    $T->check_fields($info->{parameters}, $paramsl, 'list txt parameter');
    
    # Check the rowcount fields
    
    $T->check_fields($info, $rclt, 'list txt');
    cmp_ok( $info->{"Records Found"}, '>', $info->{"Records Returned"}, 'list txt records found is greater' );
    
    # Check a few basic data fields, just to make sure that we are getting
    # some data back.  The /taxa/single response will be checked more fully in
    # pb2-04-taxa.t.
    
    $T->check_fields($r, $t1t, 'list txt field');
};


subtest 'list ris info' => sub {

    my $response = $T->fetch_url("${OP3}.ris?$ARG3", "list refs ris request OK") || return;
    
    my $url = $T->make_url("${OP3}.ris?$ARG3");
    
    my $body = $response->content;
    
    ok($body =~ qr{^Provider: $SERVICE_TITLE}m, "list refs ris 'provider'");
    ok($body =~ qr{^Database: $SERVICE_TITLE}m, "list refs ris 'database'");
    ok($body =~ qr{^Content: text/plain; charset="utf-8"}m, "list refs ris has proper content type");
    
    ok($body =~ qr{^TI  - Data Source}m, "list refs ris datainfo 'TI'");
    ok($body =~ qr{^DP  - $SERVICE_TITLE}m, "list refs ris datainfo 'DP'");
    ok($body =~ qr{^UR  - http://}m, "list refs ris datainfo 'UR'");
    ok($body =~ qr{^Y2  - .*\d\d\d\d-\d\d-\d\d.*GMT}m, "list refs ris datainfo 'Y2'");
    
    ok($body =~ qr{^KW  - base_name = $TEST_NAME_1}m, "list refs ris has datasource KW line");
    ok($body =~ qr{^TI  - $TEST_TITLE_7a}m, "list refs ris found at least one of the proper records");
};


