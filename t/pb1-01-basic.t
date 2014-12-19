# 
# PBDB 1.1
# --------
# 
# Test the following operations:
# 
# /data1.1/config.json ? show=all & count
# /data1.1/config.txt ? show=all
# /data1.1/config.csv ? show=all
# /data1.1/config.tsv ? show=all
#
# Test for errors:
# 
# /data1.1/config.foo
# /data1.1/config.json ? show=foo
# 
# Given the way in which Web::DataService works (separation of output
# serialization from output data generation) we will assume that if the data
# is properly serialized into each of the above formats for the 'config'
# operation then those serializations will also work properly for other
# operations.  If this later turns out not to be the case, we can add more
# tests.
# 

use open ':std', ':encoding(utf8)';
use Test::Most tests => 19;

use LWP::UserAgent;
use JSON;

use lib 't';
use Tester;


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
    
    my $config_json = $T->fetch_url("/data1.1/config.json?show=all&count", "config.json");
    
    restore_fail;
    
    return unless $config_json;
    
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
    
    my ($found_clu, $found_rank, $found_continent);
    
    foreach my $record ( @$records )
    {
	$found_clu = $record if $record->{cfg} eq 'clu' && $record->{lvl} > 0;
	$found_rank = $record if $record->{cfg} eq 'trn' && $record->{rnk} eq 'genus';
	$found_continent = $record if $record->{cfg} eq 'con' && $record->{nam} ne '';
    }
    
    ok( $found_clu, 'found at least one cluster' );
    ok( $found_rank, 'found gank \'genus\'' );
    ok( $found_continent, 'found at least one continent' );
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

    my $config_txt = $T->fetch_url("/data1.1/config.txt?show=all&count", "config.txt");
    
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

    $config_csv = $T->fetch_url("/data1.1/config.csv?show=all&count", "format csv");
    $config_tsv = $T->fetch_url("/data1.1/config.tsv?show=all&count", "format tsv");

ok( $config_csv->is_success, 'config.csv success' );
is( $config_csv->header('Content-Type'), 'text/csv; charset=utf-8', 'config.csv content-type' );
is( $config_tsv->header('Content-Type'), 'text/tab-separated-values; charset=utf-8', 'config.tsv content-type' );
is( $config_csv->header('Content-Disposition'), 'attachment; filename="pbdb_data.csv"', 'config.csv disposition');
is( $config_tsv->header('Content-Disposition'), 'attachment; filename="pbdb_data.tsv"', 'config.tsv disposition');

subtest 'config.csv contents' => sub {
    
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


ok( $config_tsv->is_success, 'config.tsv success' );

subtest 'config.tsv contents' => sub {
    
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


# Now test the bad media type

cmp_ok( $config_bad->code, 'eq', '415', 'config.foo returns 415' );

# And also the missing 'show' parameter

subtest 'config.json missing "show"' => sub {

    ok( $config_json->is_success, 'config.json missing "show" success' );
    
    my ($raw_data, $data, @warnings);
    
    eval {
	$raw_data = $config_foo->content;
	$data = decode_json($raw_data);
	@warnings = @{$data->{warnings}};
    };

    unless ( ok( !$@, 'config.json unpack' ) )
    {
	diag( "    message was: $@" );
	return;
    }
    
    unless ( scalar(@warnings) == 2 )
    {
	fail( 'config.json missing "show" has 2 warnings' );
	return;
    }
    
    ok( $warnings[0] =~ qr{bad value 'foo'}, 'config.json missing "show" bad value' );
};
