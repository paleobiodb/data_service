# 
# PBDB 1.1
# --------
# 
# Test the following operations:
# 
# /data1.1/taxa/
# /data1.1/taxa/index.html
# /data1.1/taxa/index.pod
# /data1.1/taxa_doc.html
# /data1.1/taxa_doc.pod
# /data1.1/taxa/single/
# /data1.1/taxa/single/index.html
# /data1.1/taxa/single/index.pod
# /data1.1/taxa/single_doc.html
# /data1.1/taxa/single_doc.pod
# 

use Test::Most tests => 7;

use LWP::UserAgent;
use JSON;

my $ua = LWP::UserAgent->new(agent => "PBDB Tester/0.1");

my $SERVER = $ENV{PBDB_TEST_SERVER} || '127.0.0.1:3000';

diag("TESTING SERVER: $SERVER");

my ($taxa_doc, $taxa_html, $taxa_pod, $taxa_html2, $taxa_pod2);
my ($single_doc, $single_html, $single_pod, $single_html2, $single_pod2);

eval {
    $taxa_doc = $ua->get("http://$SERVER/data1.1/taxa/");
    $taxa_html = $ua->get("http://$SERVER/data1.1/taxa/index.html");
    $taxa_pod = $ua->get("http://$SERVER/data1.1/taxa/index.pod");
    $taxa_html2 = $ua->get("http://$SERVER/data1.1/taxa_doc.html");
    $taxa_pod2 = $ua->get("http://$SERVER/data1.1/taxa_doc.pod");
    
    $single_doc = $ua->get("http://$SERVER/data1.1/taxa/single/");
    $single_html = $ua->get("http://$SERVER/data1.1/taxa/single/index.html");
    $single_pod = $ua->get("http://$SERVER/data1.1/taxa/single/index.pod");
    $single_html2 = $ua->get("http://$SERVER/data1.1/taxa/single_doc.html");
    $single_pod2 = $ua->get("http://$SERVER/data1.1/taxa/single_doc.pod");
};

# First make sure that the requests execute okay, and that the proper headers
# come back.

ok( !$@, 'initial requests' ) or diag( "    message was: $@" );
ok( defined $taxa_doc && $taxa_doc->is_success, '/taxa/ documentation request ok' );

subtest 'documentation headers' => sub {

    unless ( defined $taxa_doc && $taxa_doc->is_success )
    {
	fail('doc request success');
	diag('skipping remainder of tests');
	return;
    }
    
    is( $taxa_doc->header('Content-Type'), 'text/html; charset=utf-8', '/taxa/ content-type' );
    is( $taxa_doc->header('Access-Control-Allow-Origin'), '*', '/taxa/ access-control-allow-origin' );
    is( $taxa_pod->header('Content-Type'), 'text/plain; charset=utf-8', '/taxa/index.pod content-type' );
    is( $taxa_pod->header('Access-Control-Allow-Origin'), '*', '/taxa/index.pod access-control-allow-origin' );
    
    ok( $taxa_html->is_success, '/taxa/index.html request ok');
    ok( $taxa_pod->is_success, '/taxa/index.pod request ok');
    ok( $taxa_html2->is_success, '/taxa_doc.html request ok');
    ok( $taxa_pod2->is_success, '/taxa_doc.pod request ok');

    ok( $single_doc->is_success, '/taxa/single/ request ok');
    ok( $single_html->is_success, '/taxa/single/index.html request ok');
    ok( $single_pod->is_success, '/taxa/single/index.pod request ok');
    ok( $single_html2->is_success, '/taxa/single_doc.html request ok');
    ok( $single_pod2->is_success, '/taxa/single_doc.pod request ok');
};

# Then make sure that each of these requests produces at least some content.
# We will test one html and one pod request, and assume that if the variant
# requests produce at least some body content then they produce the proper
# content (because Web::DataService handles a documentation request the same
# way once it determines what the node path and output format are).

my ($content_doc, $content_html, $content_pod, $content_html2, $content_pod2);
my ($cs_doc, $cs_html, $cs_pod, $cs_html2, $cs_pod2);

subtest 'documentation unpack' => sub {

    unless ( defined $taxa_doc && $taxa_doc->is_success )
    {
	fail('doc request success');
	diag('skipping remainder of tests');
	return;
    }
    
    eval { 
	$content_doc = $taxa_doc->content;
	$content_html = $taxa_html->content;
	$content_pod = $taxa_pod->content;
	$content_html2 = $taxa_html2->content;
	$content_pod2 = $taxa_pod2->content;
	$cs_doc = $single_doc->content;
	$cs_html = $single_html->content;
	$cs_pod = $single_pod->content;
	$cs_html2 = $single_html2->content;
	$cs_pod2 = $single_pod2->content;
    };
    
    ok( !$@, 'documentation unpack' ) or diag( "    message was: $@" );
    
    ok( defined $content_doc && $content_doc =~ qr{href="/data1.1/taxa/single.json}m,
	'/taxa/ documentation basic check' );
    ok( defined $content_html && $content_html =~ qr{href="/data1.1/taxa/single.json}m,
	'/taxa/index.html documentation basic check' );
    ok( defined $content_html2 && $content_html2 =~ qr{href="/data1.1/taxa/single.json}m,
	'/taxa_doc.html documentation basic check' );
    ok( defined $content_pod && $content_pod =~ qr{L</data1.1/taxa/single.json}m,
	'/taxa/index.pod documentation basic check' );
    ok( defined $content_pod2 && $content_pod2 =~ qr{L</data1.1/taxa/single.json}m,
	'/taxa_doc.pod documentation basic check' );
    
    ok( defined $cs_doc && $cs_doc =~ qr{<td class="pod_term">show}m,
	'/taxa/single/ documentation basic check' );
    ok( defined $cs_html && $cs_html =~ qr{<td class="pod_term">show}m,
	'/taxa/single/index.html documentation basic check' );
    ok( defined $cs_html2 && $cs_html2 =~ qr{<td class="pod_term">show}m,
	'/taxa/single_doc.html documentation basic check' );
    ok( defined $cs_pod && $cs_pod =~ qr{^=item show}m,
	'/taxa/single/index.pod documentation basic check' );
    ok( defined $cs_pod2 && $cs_pod2 =~ qr{^=item show}m,
	'/taxa/single_doc.pod documentation basic check' );

};

# Now do some more detailed checking into the HTML and POD documentation, to
# make sure that all of the relevant sections are there.

subtest '/taxa/ html documentation checks' => sub {
    
    unless ( defined $content_html && $content_html ne '' )
    {
	fail('html content empty');
	diag('skipping remainder of tests');
	return;
    }
    
    ok( $content_html =~ qr{<title>.*PBDB[^<]*</title>}m, 'html title' );
    ok( $content_html =~ qr{<h1 id="title">.*PBDB[^<]*</h1>}m, 'html header' );
    ok( $content_html =~ qr{<h2 class="pod_heading" id="DESCRIPTION">DESCRIPTION</h2>}m, 'html description section' );
    ok( $content_html =~ qr{<h2 class="pod_heading" id="SYNOPSIS">SYNOPSIS</h2>}m, 'html synopsis section' );
    ok( $content_html =~ qr{CONTACT</h2>}m, 'html footer' );
};


subtest '/taxa/ pod documentation checks' => sub {

    unless ( defined $content_pod && $content_pod ne '' )
    {
	fail('pod content empty');
	diag('skipping remainder of tests');
	return;
    }
    
    ok( $content_pod =~ qr{^=for wds_title .*PBDB}m, 'pod title' );
    ok( $content_pod =~ qr{^=head3 L<PBDB}m, 'pod nav' );
    ok( $content_pod =~ qr{^=head2 DESCRIPTION}m, 'pod description section' );
    ok( $content_pod =~ qr{^=head2 SYNOPSIS}m, 'pod synopsis section' );
    ok( $content_pod =~ qr{^=head2 CONTACT}m, 'pod footer' );
};


subtest '/taxa/single html documentation checks' => sub {
    
    unless ( defined $cs_html && $cs_html ne '' )
    {
	fail('html content empty');
	diag('skipping remainder of tests');
	return;
    }
    
    
    my ($section, $title_ok, $header_ok, $nav_ok);
    my ($description_ok);
    my ($usage_ok);
    my ($parameters_ok);
    my ($methods_ok);
    my ($response_a, $response_b);
    my ($formats_ok);
    my ($vocabularies_ok);
    
    foreach my $line ( split( qr{[\n\r]+}, $cs_html ) )
    {
	if ( $line =~ qr{^<h2 \s+ class="pod_heading" \s+ id="([^"]+)">}x )
	{
	    $section = $1;
	}
	
	elsif ( ! $section )
	{
	    $title_ok = 1 if $line =~ qr{<title>.*PBDB[^<]*</title>};
	    $header_ok = 1 if $line =~ qr{<h1 id="title">.*PBDB[^<]*</h1>};
	    $nav_ok = 1 if $line =~ qr{href="/data1.1/">[^<]*PBDB};
	}
	
	elsif ( $section eq 'DESCRIPTION' )
	{
	    $description_ok = 1 if $line =~ qr{<p class="pod_para">\w};
	}
	
	elsif ( $section eq 'USAGE' )
	{
	    $usage_ok = 1 if $line =~ qr{href="/data1.1/taxa/single.json};
	}
	
	elsif ( $section eq 'PARAMETERS' )
	{
	    $parameters_ok = 1 if $line =~ qr{<td class="pod_term">show</td>};
	}
	
	elsif ( $section eq 'METHODS' )
	{
	    $methods_ok = 1 if $line =~ qr{GET};
	}
	
	elsif ( $section eq 'RESPONSE' )
	{
	    $response_a = 1 if $line =~ qr{<td class="pod_def">basic</td>};
	    $response_b = 1 if $line =~ qr{<td class="pod_def">crmod</td>};
	}
	
	elsif ( $section eq 'FORMATS' )
	{
	    $formats_ok = 1 if $line =~ qr{>JSON format<};
	}
	
	elsif ( $section eq 'VOCABULARIES' )
	{
	    $vocabularies_ok = 1 if $line =~ qr{>Compact field names<};
	}
    }
    
    ok( $title_ok, 'html title' );
    ok( $header_ok, 'html header' );
    ok( $nav_ok, 'html nav' );
    ok( $description_ok, 'html DESCRIPTION' );
    ok( $usage_ok, 'html USAGE' );
    ok( $parameters_ok, 'html PARAMETERS' );
    ok( $methods_ok, 'html METHODS' );
    ok( $response_a && $response_b, 'html RESPONSE' );
    ok( $formats_ok, 'html FORMATS' );
    ok( $vocabularies_ok, 'html VOCABULARIES' );
};

