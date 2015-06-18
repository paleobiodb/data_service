# 
# PBDB 1.1
# --------
# 
# Test the following operations:
# 
# /data1.1/taxa/single.json ? name=Dascilidae & showsource
# /data1.1/taxa/single.json ? id=<id>
# /data1.1/taxa/list.json ? base_name=Dascillidae & count
# /data1.1/taxa/list.json ? base_id=<id>
# /data1.1/taxa/single.json ? id=<parent of Dascillidae>
# 

use Test::Most tests => 19;

use JSON;
use Text::CSV_XS;

use lib 't';
use Tester;


my $T = Tester->new();


# First define the values we will be using to check the taxonomy operations.
# These are representative taxa from the database.

my $TEST_NAME_1 = 'Dascillidae';
my $TEST_NAME_2 = 'Dascilloidea';

my $TEST_NAME_3 = 'Felidae';
my $TEST_NAME_3a = 'Felinae';
my $TEST_NAME_3b = 'Machairodontinae';
my $TEST_NAME_3c = 'Felis catus';
my $TEST_NAME_3P = 'Aeluroidea';

my $TEST_NAME_4 = 'Canidae';
my $TEST_NAME_4P = 'Canoidea';

my $TEST_NAME_COMMON = 'Carnivora';

my $TEST_NAME_5 = 'Caviidae';

my $TEST_NAME_6 = 'Tyrannosauridae';

my $TEST_NAME_7 = 'Dascillidae';
my @TEST_AUTHOR_7a = ('Crowson', 'Zhang');
my $TEST_TITLE_7a = 'Miocene insects and spiders from Shanwang, Shandong';

my $TEST_NAME_8 = 'Metazoa';

my $TEST_AUTO_1 = 'cani';
my @TEST_AUTO_1a = ("Caniformia", "canine");
my $TEST_AUTO_2 = 't.rex';
my @TEST_AUTO_2a = ("Tyrannosaurus rex", "Telmatornis rex");

my $TEST_IMAGE_1 = 910;
my $TEST_IMAGE_SIZE_1a = 2047;
my $TEST_IMAGE_SIZE_1b = 1302;

my $t1 = { 'nam' => $TEST_NAME_1,
	   'typ' => "txn",
	   'rnk' => 9,
	   'nm2' => "soft bodied plant beetle",
	   'att' => "Guerin-Meneville 1843",
	   'sta' => "belongs to",
	   "kgl" => "Metazoa",
	   "phl" => "Arthropoda",
	   "cll" => "Insecta",
	   "odl" => "Coleoptera",
	   "fml" => "Dascillidae" };
	   
my $t1_num = { 'oid' => 1, 'gid' => 1, 'par' => 1, 'ext' => 1,
	       'fea' => 1, 'fla' => 1, 'lea' => 1, 'lla' => 1,
	       'siz' => 1, 'exs' => 1 };

my $t1t = { 'taxon_name' => $TEST_NAME_1,
	    'record_type' => "taxon",
	    'rank' => 'family',
	    'common_name' => "soft bodied plant beetle",
	    'attribution' => "Guerin-Meneville 1843",
	    'status' => "belongs to",
	    "kingdom" => "Metazoa",
	    "phylum" => "Arthropoda",
	    "class" => "Insecta",
	    "order" => "Coleoptera",
	    "family" => "Dascillidae" };

my $t1t_num = { 'taxon_no' => 1, 'orig_no' => 1, 'parent_no' => 1, 'reference_no' => 1, 'is_extant' => 1,
		"firstapp_ea" => 1, "firstapp_la" => 1, "lastapp_ea" => 1,
		"lastapp_la" => 1, "size" =>1, "extant_size" => 1 };

my $t1n = { 'prl' => 'Dascilloidea',
	    'prr' => 'superfamily',
	    'kgn' => '2',
	    'kgl' => 'Metazoa',
	    'phn' => '18891',
	    'phl' => 'Arthropoda',
	    'phc' => '0',
	    'cln' => '56637',
	    'cll' => 'Insecta',
	    'clc' => '0',
	    'odn' => '69148',
	    'odl' => 'Coleoptera',
	    'odc' => '0',
	    'fmn' => '69296',
	    'fml' => 'Dascillidae',
	    'fmc' => 0 };

my $t1n_num = { 'gnc' => 1 };

my $t1n_list = { 'chl' => 1, 'gns' => 1 };

my $t8n = { 'prl' => 'Opisthokonta',
	    'prr' => 'unranked clade',
	    'kgn' => '2',
	    'kgl' => 'Metazoa' };

my $t8n_num = { 'phc' => 1, 'clc' => 1, 'odc' => 1, 'fmc' => 1, 'gnc' => 1 };

my $t8n_list = { 'chl' => 1, 'phs' => 1, 'cls' => 1, 'ods' => 1, 'fms' => 1, 'gns' => 1 };

# Then the fields and values to expect as a result of the 'showsource' parameter.

my $ss = { "data_provider" => 1,
	   "data_source" => 1,
	   "data_license" => 1,
	   "license_url" => 1,
	   "documentation_url" => $T->make_url("/data1.1/taxa/single_doc.html"),
	   "data_url" => $T->make_url("/data1.1/taxa/single.json?name=$t1->{nam}&show=attr,app,size,phylo&showsource"),
	   "access_time" => 1,
	   "title" => 1 };

my $sst = { "Data Provider" => 1,
	    "Data Source" => 1,
	    "Data License" => 1,
	    "License URL" => 1,
	    "Documentation URL" => $T->make_url("/data1.1/taxa/single_doc.html"),
	    "Data URL" => $T->make_url("/data1.1/taxa/single.txt?name=$t1->{nam}&show=attr,app,size,phylo&showsource"),
	    "Access Time" => 1,
	    "Title" => 1 };

my $ssp = { "name" => $TEST_NAME_1,
	    "show" => "attr,app,size,phylo" };


# Then do some initial fetches using the taxon name.  Once we get a taxon
# identifier back, we will use that to test fetching by identifier.
# 
# We check both .json and .txt responses.

my ($taxon_id, $parent_id);

subtest 'single json by name' => sub {
    
    my $single_json = $T->fetch_url("/data1.1/taxa/single.json?name=$TEST_NAME_1&show=attr,app,size,phylo&showsource",
				    "single json request OK");
    
    unless ( $single_json )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # First check the we have the proper headers
    
    is( $single_json->header('Content-Type'), 'application/json; charset=utf-8', 'single json content-type' );
    is( $single_json->header('Access-Control-Allow-Origin'), '*', 'operation access-control-allow-origin' );
    
    # Then check the json response in detail
    
    my ($response, $r);
    
    eval {
	$response = decode_json( $single_json->content );
	$r = $response->{records}[0];
	$taxon_id = $r->{oid};
	$parent_id = $r->{par};
    };
    
    ok( ref $r eq 'HASH' && keys %$r, 'single json content decoded') or return;
    
    # Check the 'showsource' fields
    
    foreach my $key ( keys %$ss )
    {
	next unless ok( defined $response->{$key} && $response->{$key} ne '', 
			"single json showsource '$key'" );
	
	unless ( $ss->{$key} eq '1' )
	{
	    is( $response->{$key}, $ss->{$key}, "single json showsource value '$key'" );
	}
    }
    
    foreach my $key ( keys %$ssp )
    {
	next unless ok( defined $response->{parameters}{$key},
			"single json showsource parameter '$key'" );
	is( $response->{parameters}{$key}, $ssp->{$key},
	    "single json showsource parameter value '$key'" );
    }
    
    # Check the data fields
    
    foreach my $key ( keys %$t1 )
    {
	next unless ok( defined $r->{$key}, "single json has field '$key'" );
	is( $r->{$key}, $t1->{$key}, "single json field value '$key'" );
    }
    
    foreach my $key ( keys %$t1_num )
    {
	ok( defined $r->{$key} && $r->{$key} > 0, "single json has numeric value for '$key'" );
    }
};


subtest 'single json nav' => sub {
    
    diag("checking taxon '$TEST_NAME_1'");
    
    my $single_json = $T->fetch_url("/data1.1/taxa/single.json?name=$TEST_NAME_1&show=nav",
				    "single json nav request OK");
    
    unless ( $single_json )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my ($r) = $T->extract_records($single_json, "single json nav extract records");
    
    foreach my $key ( keys %$t1n )
    {
	next unless ok( defined $r->{$key}, "single json nav has field '$key'" );
	is( $r->{$key}, $t1n->{$key}, "single json field value '$key'" );
    }
    
    foreach my $key ( keys %$t1n_num )
    {
	ok( defined $r->{$key} && $r->{$key} > 0, "single json has numeric value for '$key'" );
    }
    
    foreach my $key ( keys %$t1n_list )
    {
	ok( ref $r->{$key} eq 'ARRAY' && $r->{$key}[0]{oid}, "single json nav has sublist '$key'");
    }
    
    diag("checking taxon '$TEST_NAME_8'");
    
    my $higher_json = $T->fetch_url("/data1.1/taxa/single.json?name=$TEST_NAME_8&show=nav",
				    "single json nav request OK") || return;
    
    ($r) = $T->extract_records($higher_json, "single json nav extract records");
    
    foreach my $key ( keys %$t8n )
    {
	next unless ok( defined $r->{$key}, "single json nav has field '$key'" );
	is( $r->{$key}, $t8n->{$key}, "single json field value '$key'" );
    }
    
    foreach my $key ( keys %$t8n_list )
    {
	ok( ref $r->{$key} eq 'ARRAY' && $r->{$key}[0]{oid}, "single json nav has sublist '$key'");
    }
    
    foreach my $key ( keys %$t8n_num )
    {
	ok( defined $r->{$key} && $r->{$key} > 0, "single json has numeric value for '$key'" );
    }
};


subtest 'single txt by name' => sub {
    
    my $single_txt = $T->fetch_url("/data1.1/taxa/single.txt?name=$TEST_NAME_1&show=attr,app,size,phylo&showsource",
				   "single txt request OK");
    
    unless ( $single_txt )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Now check the txt response in detail 
    
    is( $single_txt->header('Content-Type'), 'text/plain; charset=utf-8', 'single txt content-type' );
    
    my ($info) = $T->extract_info($single_txt, 'single txt extract info');
    
    my ($r) = $T->extract_records($single_txt, 'single txt extract records', { type => 'info' } );
    
    # Check the info fields
    
    foreach my $key ( keys %$sst )
    {
	next unless ok( defined $info->{$key} && $info->{$key} ne '', 
			"single txt showsource '$key'" );
	
	unless ( $sst->{$key} eq '1' )
	{
	    is( $info->{$key}, $sst->{$key}, "single txt showsource value '$key'" );
	}
    }
    
    foreach my $key ( keys %$ssp )
    {
	next unless ok( defined $info->{parameters}{$key},
			"single json showsource parameter '$key'" );
	is( $info->{parameters}{$key}, $ssp->{$key},
	    "single json showsource parameter value '$key'" );
    }
    
    # Check the data fields
    
    foreach my $key ( keys %$t1t )
    {
	ok( defined $r->{$key}, "single txt has field '$key'" ) &&
	    is( $r->{$key}, $t1t->{$key}, "single txt field value '$key'" );
    }
    
    foreach my $key ( keys %$t1t_num )
    {
	ok( defined $r->{$key} && $r->{$key} > 0, "single txt has numeric value for '$key'" );
    }
};


# Now we check for a request using the 'id' parameter, with the value
# retrieved from the first request.  We also fetch the parent of our test
# taxon, and make sure that it gets retrieved correctly as well.

subtest 'single json by id' => sub {
    
    my $response = $T->fetch_url("/data1.1/taxa/single.json?id=$taxon_id",
				 "single json by id request OK");
    
    unless ( $response )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my ($r) = $T->extract_records($response, "single json by id extract records");
    
    return unless $r;
    
    my $taxon_name = $r->{nam};
    
    ok( defined $taxon_name, 'single json by id taxon name' ) or return;
    is( $taxon_name, $TEST_NAME_1, "single json by id retrieves proper record" );
};


subtest 'parent json' => sub {
    
    my $response = $T->fetch_url("/data1.1/taxa/single.json?id=$parent_id",
			    "parent json request OK");
    
    my ($r) = $T->extract_records($response, "parent json extract records");
    
    return unless $r;
    
    my $taxon_name = $r->{nam};
    ok( defined $taxon_name, 'parent json taxon name' ) or return;
    is( $taxon_name, $TEST_NAME_2, "parent json retrieves proper record" );
};


# Next we test the various 'list' operations:

subtest 'list self' => sub {
    
    my $response = $T->fetch_url("/data1.1/taxa/list.json?name=$TEST_NAME_3,$TEST_NAME_4",
			    "list self request OK");
    
    my %found = $T->scan_records($response, 'nam', "list self extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{$TEST_NAME_3} && $found{$TEST_NAME_4}, "list self found both records");
};


subtest 'list synonyms' => sub {

    my $response = $T->fetch_url("/data1.1/taxa/list.json?name=Sirenia&rel=synonyms",
				 "list synonyms request OK");
    
    my %found = $T->scan_records($response, 'nam', "list synonyms extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{Manatina} && $found{Manatides} && $found{Sirenia}, "list synonyms found a sample of records");
};


my ($num_children, $num_all_children);

subtest 'list children' => sub {

    my $response = $T->fetch_url("/data1.1/taxa/list.json?name=$TEST_NAME_3&rel=children",
			    "list children request OK");
    
    my %found = $T->scan_records($response, 'nam', "list children extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{$TEST_NAME_3a}, "list children found '$TEST_NAME_3a'");
    ok($found{$TEST_NAME_3b}, "list children found '$TEST_NAME_3b'");
    $num_children = scalar(keys %found);
};


subtest 'list all children' => sub {

    my $response = $T->fetch_url("/data1.1/taxa/list.json?name=$TEST_NAME_3&rel=all_children",
			    "list all children request OK");
    
    my %found = $T->scan_records($response, 'nam', "list all children extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{$TEST_NAME_3c}, "list all children found '$TEST_NAME_3c'");
    ok($found{$TEST_NAME_3b}, "list all children found '$TEST_NAME_3b'");
    $num_all_children = scalar(keys %found);
};


cmp_ok($num_children, '<', $num_all_children, "all children count greater than immediate children count");


subtest 'list parents' => sub {

    my $response = $T->fetch_url("/data1.1/taxa/list.json?name=$TEST_NAME_3,$TEST_NAME_4&rel=parents",
			    "list parents request OK");
    
    my %found = $T->scan_records($response, 'nam', "list parents extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{$TEST_NAME_3P} && $found{$TEST_NAME_4P}, "list parents returned the proper records");
};


subtest 'list all parents' => sub {

    my $response = $T->fetch_url("/data1.1/taxa/list.json?name=$TEST_NAME_3,$TEST_NAME_4&rel=all_parents",
			    "list all parents request OK");
    
    my %found = $T->scan_records($response, 'nam', "list all parents extract records");
    
    return if $found{NO_RECORDS};
    
    foreach my $name ('Eucarya', 'Metazoa', 'Vertebrata', 'Therapsida',
		      $TEST_NAME_3P, $TEST_NAME_4P, $TEST_NAME_3, $TEST_NAME_4)
    {
	ok($found{$name}, "list parents found '$name'");
    }
};


subtest 'list common ancestor' => sub {

    my $response = $T->fetch_url("/data1.1/taxa/list.json?name=$TEST_NAME_3,$TEST_NAME_4&rel=common_ancestor",
				 "list all parents request OK");
    
    my %found = $T->scan_records($response, 'nam', "list all parents extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{$TEST_NAME_COMMON}, "list common ancestor found the correct record");
};


# Now we test the 'status' parameter.

subtest 'list status' => sub {

    my $all_resp = $T->fetch_url("/data1.1/taxa/list.json?base_name=$TEST_NAME_5&status=all&limit=100000",
				 "list status all request OK") || return;
    
    my $all_count = $T->extract_records($all_resp, "list status all extract records") || return;
    
    my $valid_resp = $T->fetch_url("data1.1/taxa/list.json?base_name=$TEST_NAME_5&status=valid&limit=100000",
				   "list status valid request OK") || return;
    
    my $valid_count = $T->extract_records($valid_resp, "list status valid extract records") || return;
    
    my $invalid_resp = $T->fetch_url("/data1.1/taxa/list.json?base_name=$TEST_NAME_5&status=invalid&limit=100000",
				     "list status invalid request OK") || return;
    
    my $invalid_count = $T->extract_records($invalid_resp, "list status invalid extract records") || return;
    
    my $senior_resp = $T->fetch_url("/data1.1/taxa/list.json?base_name=$TEST_NAME_5&status=senior&limit=100000",
				    "list status senior request OK") || return;
    
    my $senior_count = $T->extract_records($senior_resp, "list status senior extract records") || return;
    
    cmp_ok($all_count, '>', $senior_count, "senior count is less than total count");
    cmp_ok($all_count, '==', $valid_count + $invalid_count, "valid + invalid = all");
    
    my %status = $T->scan_records($all_resp, 'sta', 'list status all scan status codes');
    
    ok($status{'belongs to'} && $status{'subjective synonym of'} &&
       $status{'replaced by'}, 'list status all returns a selection of status codes');
};


subtest 'list status 2' => sub {
    
    my $invalid_resp = $T->fetch_url("/data1.1/taxa/list.json?base_name=$TEST_NAME_6&status=invalid",
				     "list status 2 invalid request OK");
    
    my %status = $T->scan_records($invalid_resp, 'sta', 'list status 2 invalid scan status codes');
    
    ok($status{'nomen dubium'} && $status{'nomen nudum'}, 
       'list status 2 returns invalid status codes');
    ok(!$status{'belongs to'} && !$status{'subjective synonym of'} && !$status{'objective synonym of'},
       'list status 2 does not return valid status codes');
};


# Then test taxon references, both json and ris formats.

subtest 'list refs json' => sub {

    my $response = $T->fetch_url("/data1.1/taxa/refs.json?base_name=$TEST_NAME_7",
				 "list refs json request OK") || return;
    
    my %found = $T->scan_records($response, 'al1', "list refs json extract records");
    
    return if $found{NO_RECORDS};
    
    ok($T->found_all(\%found, @TEST_AUTHOR_7a), "list refs json found a sample of records");
};


subtest 'list refs ris' => sub {

    my $response = $T->fetch_url("/data1.1/taxa/refs.ris?base_name=$TEST_NAME_7&showsource",
				 "list refs ris request OK") || return;
    
    my $body = $response->content;
    
    ok($body =~ qr{^Provider:\s+\w}m, "list refs ris has 'provider:'");
    ok($body =~ qr{^Content: text/plain; charset="utf-8"}m, "list refs ris has proper content type");
    ok($body =~ qr{^UR  - http://.+/data1.1/taxa/refs.ris\?base_name=$TEST_NAME_7&showsource}m,
       "list refs ris has showsource UR line");
    ok($body =~ qr{^KW  - base_name = $TEST_NAME_7}m, "list refs ris has datasource KW line");
    ok($body =~ qr{^T2  - $TEST_TITLE_7a}m, "list refs ris found at least one of the proper records");
};


subtest 'auto json' => sub {

    my $cani = $T->fetch_url("/data1.1/taxa/auto.json?name=$TEST_AUTO_1&limit=10",
			     "auto json '$TEST_AUTO_1' request OK") || return;
    
    my %found = $T->scan_records($cani, 'nam', "auto json extract records");
    
    return if $found{NO_RECORDS};
    
    ok($T->found_all(\%found, @TEST_AUTO_1a), "auto json found a sample of records");
    
    my $trex = $T->fetch_url("/data1.1/taxa/auto.json?name=$TEST_AUTO_2&limit=10",
			     "auto json $TEST_AUTO_2' request OK") || return;
    
    %found = $T->scan_records($trex, 'nam', "auto json extract records");
    
    return if $found{NO_RECORDS};
    
    ok($T->found_all(\%found, @TEST_AUTO_2a), "auto json found a sample of records");
};


subtest 'images' => sub {
    
    my $thumb = $T->fetch_url("/data1.1/taxa/thumb.png?id=$TEST_IMAGE_1",
			      "image thumb request OK") || return;
    
    my $thumb_length = length($thumb->content) || 0;
    
    cmp_ok($thumb_length, '==', $TEST_IMAGE_SIZE_1a, 'image thumb size');
    
    my $icon = $T->fetch_url("/data1.1/taxa/icon.png?id=910",
			     "image icon request OK") || return;
    
    my $icon_length = length($icon->content) || 0;
    
    cmp_ok($icon_length, '==', $TEST_IMAGE_SIZE_1b, 'image icon size');
};
