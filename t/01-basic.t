use Test::Most tests => 12;

use LWP::UserAgent;
use JSON;

my $ua = LWP::UserAgent->new(agent => "PBDB Tester/0.1");

my ($config1, $resp2, $resp3);

eval {
    $config1 = $ua->get('http://localhost:3000/data1.1/config.json?show=all&count');
};

ok( !$@, 'config request' ) or diag( "    message was: $@" );

ok( $config1->is_success, 'config ok' );
is( $config1->header('Content-Type'), 'application/json; charset=utf-8', 'config content-type' );
is( $config1->header('Access-Control-Allow-Origin'), '*', 'access-control-allow-origin' );

my ($raw_data, $data, $found, $returned, $elapsed, $records);

eval {
    $raw_data = $config1->content;
    $data = decode_json($raw_data);
    $found = $data->{records_found};
    $returned = $data->{records_returned};
    $elapsed = $data->{elapsed_time};
    $records = $data->{records};
};

ok( !$@, 'config json' ) or diag( "    message was: $@" );

cmp_ok( $found, '>', 10, 'found some records' );
cmp_ok( $returned, '>', 10, 'returned some records' );
cmp_ok( $elapsed, '>', 0, 'elapsed time reported' );

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
