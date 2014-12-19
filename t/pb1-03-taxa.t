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

use Test::Most tests => 17;

use LWP::UserAgent;
use JSON;
use Text::CSV_XS;

use lib 't';
use MyTest;

my $ua = LWP::UserAgent->new(agent => "PBDB Tester/0.1");
my $csv = Text::CSV_XS->new();

my $SERVER = $ENV{PBDB_TEST_SERVER} || '127.0.0.1:3000';

diag("TESTING SERVER: $SERVER");

# First define the values we will be using to check the taxonomy operations.
# These are representative taxa from the database.

my $TEST_NAME_1 = 'Dascillidae';
my $TEST_NAME_2 = 'Dascilloidea';

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

my $t1t_num = { 'taxon_no' => 1, 'orig_no' => 1, 'parent_no' => 1, 'senior_no' => 1,
		'reference_no' => 1, 'is_extant' => 1,
		"firstapp_ea" => 1, "firstapp_la" => 1, "lastapp_ea" => 1,
		"lastapp_la" => 1, "size" =>1, "extant_size" => 1 };

# Then the fields and values to expect as a result of the 'showsource' parameter.

my $ss = { "data_provider" => 1,
	   "data_source" => 1,
	   "data_license" => 1,
	   "license_url" => 1,
	   "documentation_url" => "http://$SERVER/data1.1/taxa/single_doc.html",
	   "data_url" => "http://$SERVER/data1.1/taxa/single.json?name=$t1->{nam}&show=attr,app,size,phylo&showsource",
	   "access_time" => 1,
	   "title" => 1 };

my $sst = { "Data Provider" => 1,
	    "Data Source" => 1,
	    "Data License" => 1,
	    "License URL" => 1,
	    "Documentation URL" => "http://$SERVER/data1.1/taxa/single_doc.html",
	    "Data URL" => "http://$SERVER/data1.1/taxa/single.txt?name=$t1->{nam}&show=attr,app,size,phylo&showsource",
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
    
    my $single_json = test_url($ua,
			       "http://$SERVER/data1.1/taxa/single.json?name=$TEST_NAME_1&show=attr,app,size,phylo&showsource",
			       "single json request OK") || return;
    
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


subtest 'single txt by name' => sub {
    
    my $single_txt = test_url($ua,
			      "http://$SERVER/data1.1/taxa/single.txt?name=$TEST_NAME_1&show=attr,app,size,phylo&showsource",
			      "single txt request OK") || return;
    
    # Now check the txt response in detail 
    
    is( $single_txt->header('Content-Type'), 'text/plain; charset=utf-8', 'single txt content-type' );
    
    my ($response, $parameters, $r);
    
    eval {
	
	my $section = 'top';
	my (@fields, @values);
	
	foreach my $line ( split( qr{[\n\r]+}, $single_txt->content ) )
	{
	    if ( $line =~ qr{^"Parameters:"} )
	    {
		$section = 'parameters';
	    }
	    
	    elsif ( $line =~ qr{^"Records:"} )
	    {
		$section = 'fields';
	    }
	    
	    elsif ( $section eq 'parameters' )
	    {
		$csv->parse($line);
		my ($dummy, $param, $value) = $csv->fields;
		$parameters->{$param} = $value;
	    }
	    
	    elsif ( $section eq 'fields' )
	    {
		$csv->parse($line);
		@fields = $csv->fields;
		$section = 'record';
	    }
	    
	    elsif ( $section eq 'record' )
	    {
		$csv->parse($line);
		@values = $csv->fields;
		last;
	    }
	    
	    elsif ( $section eq 'top' )
	    {
		$csv->parse($line);
		my ($field, $value) = $csv->fields;
		$response->{$field} = $value;
	    }
	}
	
	foreach my $i ( 0..$#fields )
	{
	    $r->{$fields[$i]} = $values[$i];
	}
    };
    
    ok( ref $r eq 'HASH' && keys(%$r) > 1, 'single txt content decoded') or return;
    
    # Check the 'showsource' fields
    
    foreach my $key ( keys %$sst )
    {
	next unless ok( defined $response->{$key} && $response->{$key} ne '', 
			"single txt showsource '$key'" );
	
	unless ( $sst->{$key} eq '1' )
	{
	    is( $response->{$key}, $sst->{$key}, "single txt showsource value '$key'" );
	}
    }
    
    foreach my $key ( keys %$ssp )
    {
	next unless ok( defined $parameters->{$key},
			"single json showsource parameter '$key'" );
	is( $parameters->{$key}, $ssp->{$key},
	    "single json showsource parameter value '$key'" );
    }
    
    # CHeck the data fields
    
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
    
    my $response = test_url($ua, "http://$SERVER/data1.1/taxa/single.json?id=$taxon_id",
			    "single json by id request OK");
    
    my ($r) = extract_records_json($response, "single json by id extract records");
    
    return unless $r;
    
    my $taxon_name = $r->{nam};
    
    ok( defined $taxon_name, 'single json by id taxon name' ) or return;
    is( $taxon_name, $TEST_NAME_1, "single json by id retrieves proper record" );
};


subtest 'parent json' => sub {
    
    my $response = test_url($ua, "http://$SERVER/data1.1/taxa/single.json?id=$parent_id",
			    "parent json request OK") || return;
    
    my ($r) = extract_records_json($response, "parent json extract records");
    
    return unless $r;
    
    my $taxon_name = $r->{nam};
    ok( defined $taxon_name, 'parent json taxon name' ) or return;
    is( $taxon_name, $TEST_NAME_2, "parent json retrieves proper record" );
};


# Next we test the various 'list' operations:

subtest 'list self' => sub {
    
    my $response = test_url($ua, "http://$SERVER/data1.1/taxa/list.json?name=Felidae,Canidae",
			    "list self request OK") || return;
    
    my %found = scan_records_json($response, 'nam', "list self extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{Felidae} && $found{Canidae}, "list self found both records");
};


subtest 'list synonyms' => sub {

    my $response = test_url($ua, "http://$SERVER/data1.1/taxa/list.json?name=Sirenia&rel=synonyms",
			    "list synonyms request OK") || return;
    
    my %found = scan_records_json($response, 'nam', "list synonyms extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{Manatina} && $found{Manatides} && $found{Sirenia}, "list synonyms found a sample of records");
};


my ($num_children, $num_all_children);

subtest 'list children' => sub {

    my $response = test_url($ua, "http://$SERVER/data1.1/taxa/list.json?name=Felidae&rel=children",
			    "list children request OK") || return;
    
    my %found = scan_records_json($response, 'nam', "list children extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{Felinae} && $found{Pantherinae}, "list children found a sample of records");
    $num_children = scalar(keys %found);
};


subtest 'list all children' => sub {

    my $response = test_url($ua, "http://$SERVER/data1.1/taxa/list.json?name=Felidae&rel=all_children",
			    "list all children request OK") || return;
    
    my %found = scan_records_json($response, 'nam', "list all children extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{'Felis catus'} && $found{Pantherinae}, "list all children found a sample of records");
    $num_all_children = scalar(keys %found);
};


cmp_ok($num_children, '<', $num_all_children, "all children count greater than immediate children count");


subtest 'list parents' => sub {

    my $response = test_url($ua, "http://$SERVER/data1.1/taxa/list.json?name=Felidae,Canidae&rel=parents",
			    "list parents request OK") || return;
    
    my %found = scan_records_json($response, 'nam', "list parents extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{'Aeluroidea'} && $found{Canoidea}, "list parents returned the proper records");
};


subtest 'list all parents' => sub {

    my $response = test_url($ua, "http://$SERVER/data1.1/taxa/list.json?name=Felidae,Canidae&rel=all_parents",
			    "list all parents request OK") || return;
    
    my %found = scan_records_json($response, 'nam', "list all parents extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{Eukaryota} && $found{Metazoa} && $found{Vertebrata} &&
       $found{Therapsida} && $found{Canoidea} && $found{Aeluroidea} &&
       $found{Canidae} && $found{Felidae},
       "list parents found a sample of records");
};


subtest 'list common ancestor' => sub {

    my $response = test_url($ua, "http://$SERVER/data1.1/taxa/list.json?name=Felidae,Canidae&rel=common_ancestor",
			    "list all parents request OK") || return;
    
    my %found = scan_records_json($response, 'nam', "list all parents extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{Carnivora}, "list common ancestor found the correct record");
};


# Now we test the 'status' parameter.

subtest 'list status' => sub {

    my $all_resp = test_url($ua, "http://$SERVER/data1.1/taxa/list.json?base_name=Caviidae&status=all",
			    "list status all request OK") || return;
    
    my $all_count = extract_records_json($all_resp, "list status all extract records") || return;
    
    my $valid_resp = test_url($ua, "http://$SERVER/data1.1/taxa/list.json?base_name=Caviidae&status=valid",
			      "list status valid request OK") || return;
    
    my $valid_count = extract_records_json($valid_resp, "list status valid extract records") || return;
    
    my $invalid_resp = test_url($ua, "http://$SERVER/data1.1/taxa/list.json?base_name=Caviidae&status=invalid",
				"list status invalid request OK") || return;
    
    my $invalid_count = extract_records_json($invalid_resp, "list status invalid extract records") || return;
    
    my $senior_resp = test_url($ua, "http://$SERVER/data1.1/taxa/list.json?base_name=Caviidae&status=senior",
			       "list status senior request OK") || return;
    
    my $senior_count = extract_records_json($senior_resp, "list status senior extract records") || return;
    
    cmp_ok($all_count, '>', $valid_count, "valid count is less than all count");
    cmp_ok($all_count, '>', $senior_count, "senior count is less than all count");
    cmp_ok($all_count, '>', $invalid_count, "invalid count is less than all count");
    
    my %status = scan_records_json($all_resp, 'sta', 'list status all scan status codes');
    
    ok($status{'belongs to'} && $status{'subjective synonym of'} &&
       $status{'replaced by'}, 'list status all returns a selection of status codes');
};


# Then test taxon references, both json and ris formats.

subtest 'list refs json' => sub {

    my $response = test_url($ua, "http://$SERVER/data1.1/taxa/refs.json?base_name=Dascillidae",
			    "list refs json request OK") || return;
    
    my %found = scan_records_json($response, 'al1', "list refs json extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{"Crowson"} && $found{"Zhang"}, "list refs json found a sample of records");
};


subtest 'list refs ris' => sub {

    my $response = test_url($ua, "http://$SERVER/data1.1/taxa/refs.ris?base_name=Dascillidae&showsource",
			    "list refs ris request OK") || return;
    
    my $body = $response->content;
    
    ok($body =~ qr{^Provider:\s+\w}m, "list refs ris has 'provider:'");
    ok($body =~ qr{^Content: text/plain; charset="utf-8"}m, "list refs ris has proper content type");
    ok($body =~ qr{^UR  - http://.+/data1.1/taxa/refs.ris\?base_name=Dascillidae&showsource}m,
       "list refs ris has showsource UR line");
    ok($body =~ qr{^KW  - base_name = Dascillidae}m, "list refs ris has datasource KW line");
    ok($body =~ qr{^T2  - Miocene insects and spiders from Shanwang, Shandong}m,
       "list refs ris found at least one of the proper records");
};


subtest 'auto json' => sub {

    my $cani = test_url($ua, "http://$SERVER/data1.1/taxa/auto.json?name=cani&limit=10",
			"auto json 'cani' request OK") || return;
    
    my %found = scan_records_json($cani, 'nam', "auto json extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{"Caniformia"} && $found{"canine"}, "auto json found a sample of records");

    my $trex = test_url($ua, "http://$SERVER/data1.1/taxa/auto.json?name=t.rex&limit=10",
			"auto json 't.rex' request OK") || return;

    %found = scan_records_json($trex, 'nam', "auto json extract records");
    
    return if $found{NO_RECORDS};
    
    ok($found{"Tyrannosaurus rex"} && $found{"Telmatornis rex"}, "auto json found a sample of records");
};


subtest 'images' => sub {
    
    my $thumb = test_url($ua, "http://$SERVER/data1.1/taxa/thumb.png?id=910",
			"image thumb request OK") || return;
    
    my $thumb_length = length($thumb->content) || 0;
    
    cmp_ok($thumb_length, '==', 2047, 'image thumb size');
    
    my $icon = test_url($ua, "http://$SERVER/data1.1/taxa/icon.png?id=910",
			"image icon request OK") || return;
    
    my $icon_length = length($icon->content) || 0;
    
    cmp_ok($icon_length, '==', 1302, 'image icon size');
};
