# 
# PBDB 1.1
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

use open ':std', ':encoding(utf8)';
use Test::Most tests => 6;

use LWP::UserAgent;
use JSON;

use lib 't';
use Tester;

no warnings 'uninitialized';

# We start by creating a Tester instance that we will use for the subsequent tests:

my $T = Tester->new();


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
    
    my $config_json = $T->fetch_url("/data1.2/config.json?show=all&count", "config.json fetch");
    
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

    my $config_txt = $T->fetch_url("/data1.2/config.txt?show=all&count", "config.txt fetch");
    
    unless ( $config_txt )
    {
	diag("skipping remainder of this subtest");
	return;
    }
    
    is( $config_txt->header('Content-Type'), 'text/plain; charset=utf-8', 'config.txt content-type' );
    ok( ! $config_txt->header('Content-Disposition'), 'config.txt disposition');
    
    my($raw_data, $found, $returned, $elapsed, $body, $header_count, $record_count);
    my($found_clu, $found_trn, $found_con);
    
    eval {
	$raw_data = $config_txt->content;
    };
    
    unless ( ok( !$@, 'config.txt unpack' ) )
    {
	diag( "    message was: $@" );
	return;
    }
    
    $raw_data ||= '';
    
    foreach my $line ( split qr{[\r\n]+}, $raw_data )
    {
	if ( $body )
	{
	    if ( $line =~ qr{ ^ "config_section" }xs )
	    {
		$header_count++;
	    }
	    
	    else
	    {
		$record_count++;
		$found_clu = 1 if $line =~ qr{ ^ (?:"",)* "clu" }xs;
		$found_trn = 1 if $line =~ qr{ ^ (?:"",)* "trn" }xs;
		$found_con = 1 if $line =~ qr{ ^ (?:"",)* "con" }xs;
	    }
	}
	
	elsif ( $line =~ qr{ ^ "Elapsed\sTime:","(.*)"}xs )
	{
	    $elapsed = $1;
	}
	
	elsif ( $line =~ qr{ ^ "Records\sFound:","(.*)"}xs )
	{
	    $found = $1;
	}
	
	elsif ( $line =~ qr{ ^ "Records\sReturned:","(.*)"}xs )
	{
	    $returned = $1;
	}
	
	elsif ( $line =~ qr{ ^ "Records:"}xs )
	{
	    $body = 1;
	}
    }
    
    cmp_ok( $found, '>', 10, 'config.txt found some records' );
    cmp_ok( $returned, '>', 10, 'config.txt returned some records' );
    cmp_ok( $elapsed, '>', 0, 'config.txt elapsed time reported' );
    
    ok( $found_clu, 'config.txt found at least one cluster' );
    ok( $found_trn, 'config.txt found gank \'genus\'' );
    ok( $found_con, 'config.txt found at least one continent' );
    
    cmp_ok( $header_count, '==', 1, 'config.txt found one header line' );
    cmp_ok( $record_count, '==', $returned, 'config.txt returned count consistent' );
};


subtest 'config.csv' => sub {
    
    my $config_csv = $T->fetch_url("/data1.2/config.csv?show=all&count", "config.csv fetch");
    
    unless ( $config_csv )
    {
	diag("skipping remainder of this subtest");
	return;
    }
    
    is( $config_csv->header('Content-Type'), 'text/csv; charset=utf-8', 'config.csv content-type' );
    is( $config_csv->header('Content-Disposition'), 'attachment; filename="pbdb_data.csv"', 'config.csv disposition');
    
    my($raw_data, $found, $returned, $elapsed, $body, $header_count, $record_count);
    my($found_clu, $found_trn, $found_con);
    
    eval {
	$raw_data = $config_csv->content;
    };
    
    unless ( ok( !$@, 'config.csv unpack' ) )
    {
	diag( "    message was: $@" );
	return;
    }
    
    $raw_data ||= '';
    
    foreach my $line ( split qr{[\r\n]+}, $raw_data )
    {
	if ( $body )
	{
	    if ( $line =~ qr{ ^ "config_section" }xs )
	    {
		$header_count++;
	    }
	    
	    else
	    {
		$record_count++;
		$found_clu = 1 if $line =~ qr{ ^ (?:"",)* "clu" }xs;
		$found_trn = 1 if $line =~ qr{ ^ (?:"",)* "trn" }xs;
		$found_con = 1 if $line =~ qr{ ^ (?:"",)* "con" }xs;
	    }
	}
	
	elsif ( $line =~ qr{ ^ "Elapsed\sTime:","(.*)"}xs )
	{
	    $elapsed = $1;
	}
	
	elsif ( $line =~ qr{ ^ "Records\sFound:","(.*)"}xs )
	{
	    $found = $1;
	}
	
	elsif ( $line =~ qr{ ^ "Records\sReturned:","(.*)"}xs )
	{
	    $returned = $1;
	}
	
	elsif ( $line =~ qr{ ^ "Records:"}xs )
	{
	    $body = 1;
	}
    }
    
    cmp_ok( $found, '>', 10, 'config.json found some records' );
    cmp_ok( $returned, '>', 10, 'config.json returned some records' );
    cmp_ok( $elapsed, '>', 0, 'config.json elapsed time reported' );
    
    ok( $found_clu, 'found at least one cluster' );
    ok( $found_trn, 'found gank \'genus\'' );
    ok( $found_con, 'found at least one continent' );
    
    cmp_ok( $header_count, '==', 1, 'config.csv found one header line' );
    cmp_ok( $record_count, '==', $returned, 'config.csv returned count consistent' );
};


subtest 'config.tsv' => sub {
    
    my $config_tsv = $T->fetch_url("/data1.2/config.tsv?show=all&count", "config.tsv fetch");
    
    unless ( $config_tsv )
    {
	diag("skipping remainder of this subtest");
	return;
    }
    
    is( $config_tsv->header('Content-Type'), 'text/tab-separated-values; charset=utf-8', 'config.tsv content-type' );
    is( $config_tsv->header('Content-Disposition'), 'attachment; filename="pbdb_data.tsv"', 'config.tsv disposition');
    
    my($raw_data, $found, $returned, $elapsed, $body, $header_count, $record_count);
    my($found_clu, $found_trn, $found_con);
    
    eval {
	$raw_data = $config_tsv->content;
    };
    
    unless ( ok( !$@, 'config.tsv unpack' ) )
    {
	diag( "    message was: $@" );
	return;
    }
    
    foreach my $line ( split qr{[\r\n]+}, $raw_data )
    {
	if ( $body )
	{
	    if ( $line =~ qr{ ^ config_section \t }xs )
	    {
		$header_count++;
	    }
	    
	    else
	    {
		$record_count++;
		$found_clu = 1 if $line =~ qr{ ^ \t* clu \t }xs;
		$found_trn = 1 if $line =~ qr{ ^ \t* trn \t }xs;
		$found_con = 1 if $line =~ qr{ ^ \t* con \t }xs;
	    }
	}
	
	elsif ( $line =~ qr{ ^ Elapsed\sTime: \t ([^\t]+) }xs )
	{
	    $elapsed = $1;
	}
	
	elsif ( $line =~ qr{ ^ Records\sFound: \t ([^\t]+) }xs )
	{
	    $found = $1;
	}
	
	elsif ( $line =~ qr{ ^ Records\sReturned: \t ([^\t]+)}xs )
	{
	    $returned = $1;
	}
	
	elsif ( $line =~ qr{ ^ Records: }xs )
	{
	    $body = 1;
	}
    }
    
    cmp_ok( $found, '>', 10, 'config.tsv found some records' );
    cmp_ok( $returned, '>', 10, 'config.tsv returned some records' );
    cmp_ok( $elapsed, '>', 0, 'config.tsv elapsed time reported' );
    
    ok( $found_clu, 'config.tsv found at least one cluster' );
    ok( $found_trn, 'config.tsv found gank \'genus\'' );
    ok( $found_con, 'config.tsv found at least one continent' );
    
    cmp_ok( $header_count, '==', 1, 'config.tsv found one header line' );
    cmp_ok( $record_count, '==', $returned, 'config.tsv returned count consistent' );
};


# Now test the bad media type response.

subtest 'config.foo' => sub {
    
    my $config_bad = $T->fetch_nocheck("/data1.2/config.foo?show=all&count", "config.foo fetch");
    
    unless ( $config_bad )
    {
	diag("skipping remainder of this subtest");
	return;
    }
    
    cmp_ok( $config_bad->code, 'eq', '415', 'config.foo returns 415' );
};


# And also a bad 'show' parameter

subtest 'config.json bad show' => sub {

    my $config_json = $T->fetch_url("/data1.2/config.json?show=foo", "config.json bad show");
    
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
    
    ok( $warnings[0] =~ qr{bad value 'foo'}, 'config.json missing show bad value' );
};
