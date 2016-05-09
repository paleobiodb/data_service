# 
# PBDB 1.2
# --------
# 
# Test the documentation page output of the data service.
# 
# The purpose of this file is to test that documentation pages produced by the
# data service have the expected structure and contents.  Other files will
# test the production of documentation pages for the individual operations.
# 

use strict;

use Test::Most tests => 6;

use lib 't';
use Tester;


# We start by creating a Tester instance that we will use for the subsequent tests:

my $T = Tester->new({ prefix => 'data1.2' });

my $PREFIX = 'data1.2';

# Declare variables to hold the results fetched during these tests.

my ($taxa_doc, $taxa_html, $taxa_pod, $taxa_html2, $taxa_pod2);
my ($single_doc, $single_html, $single_pod, $single_html2, $single_pod2);


# Now fetch documentation using a variety of URLs and make sure that the
# content-type and access-control-allow-origin headers come back properly.

subtest 'fetch documentation' => sub {

    # Fetch documentation using a variety of URLs, to make sure that all of
    # the different ways of specifying a documentation page work correctly.
    
    eval {
	$taxa_doc = $T->fetch_url("/taxa/");
	$taxa_html = $T->fetch_url("/taxa/index.html");
	$taxa_pod = $T->fetch_url("/taxa/index.pod");
	$taxa_html2 = $T->fetch_url("/taxa_doc.html");
	$taxa_pod2 = $T->fetch_url("/taxa_doc.pod");
	
	$single_doc = $T->fetch_url("/taxa/single/");
	$single_html = $T->fetch_url("/taxa/single/index.html");
	$single_pod = $T->fetch_url("/taxa/single/index.pod");
	$single_html2 = $T->fetch_url("/taxa/single_doc.html");
	$single_pod2 = $T->fetch_url("/taxa/single_doc.pod");
    };
    
    ok( !$@, 'fetch documentation' ) or diag( "    message was: $@" );
    
    unless ( $taxa_doc && $single_pod2 )
    {
	diag("skipping remainder of this test");
	done_testing();
	exit;
    }
    
    $T->ok_content_type($taxa_doc, 'text/html', 'utf-?8', '/taxa/ content type' );
    $T->ok_content_type($taxa_html, 'text/html', 'utf-?8', '/taxa/index.html content type' );
    $T->ok_content_type($taxa_pod, 'text/plain', 'utf-?8', '/taxa/index.pod content type' );
    $T->ok_content_type($taxa_html2, 'text/html', 'utf-?8', '/taxa_doc.html content type' );
    $T->ok_content_type($taxa_pod2, 'text/plain', 'utf-?8', '/taxa_doc.pod content type' );
    
    is( $taxa_doc->header('Access-Control-Allow-Origin'), '*', '/taxa/ access-control-allow-origin' );
    is( $taxa_html->header('Access-Control-Allow-Origin'), '*', '/taxa/index.html access-control-allow-origin' );
    is( $taxa_pod->header('Access-Control-Allow-Origin'), '*', '/taxa/index.pod access-control-allow-origin' );
    is( $taxa_html2->header('Access-Control-Allow-Origin'), '*', '/taxa_doc.html access-control-allow-origin' );
    is( $taxa_pod2->header('Access-Control-Allow-Origin'), '*', '/taxa_doc.pod access-control-allow-origin' );
};


# Then make sure that each of these requests produces at least some content.
# We will test one html and one pod request, and assume that if the variant
# requests produce at least some body content then they produce the proper
# content (because Web::DataService handles a documentation request the same
# way once it determines what the node path and output format are).

my ($content_doc, $content_html, $content_pod, $content_html2, $content_pod2);
my ($cs_doc, $cs_html, $cs_pod, $cs_html2, $cs_pod2);

subtest 'documentation unpack' => sub {

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
    
    unless ( ok( !$@, 'documentation unpack' ) )
    {
	diag( "    message was: $@" );
	diag( "    skipping remainder of subtest" );
	return;
    }
    
    ok( defined $content_doc && $content_doc =~ qr{href="/$PREFIX/taxa/single.json}m,
	'/taxa/ documentation basic check' );
    ok( defined $content_html && $content_html =~ qr{href="/$PREFIX/taxa/single.json}m,
	'/taxa/index.html documentation basic check' );
    ok( defined $content_html2 && $content_html2 =~ qr{href="/$PREFIX/taxa/single.json}m,
	'/taxa_doc.html documentation basic check' );
    ok( defined $content_pod && $content_pod =~ qr{L</$PREFIX/taxa/single.json}m,
	'/taxa/index.pod documentation basic check' );
    ok( defined $content_pod2 && $content_pod2 =~ qr{L</$PREFIX/taxa/single.json}m,
	'/taxa_doc.pod documentation basic check' );
    
    ok( defined $cs_doc && $cs_doc =~ qr{<td class="pod_term"[^>]*>show}m,
	'/taxa/single/ documentation basic check' );
    ok( defined $cs_html && $cs_html =~ qr{<td class="pod_term"[^>]*>show}m,
	'/taxa/single/index.html documentation basic check' );
    ok( defined $cs_html2 && $cs_html2 =~ qr{<td class="pod_term"[^>]*>show}m,
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
    ok( $content_html =~ qr{<h2 class="pod_heading"[^>]*>DESCRIPTION</h2>}m, 'html description section' );
    ok( $content_html =~ qr{<h2 class="pod_heading"[^>]*>SYNOPSIS</h2>}m, 'html synopsis section' );
    ok( $content_html =~ qr{CONTACT</h2>}m, 'html footer' );
    
    ok( $content_html =~ qr{href="/data1[.]2/taxa/opinions_doc[.]html"}m, 'link to subsection' );
    
    ok( $content_html !~ qr{node:taxa/|op:taxa/}m, 'link translation' );
    
    # $$$ add more checks
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
    
    ok( $content_pod =~ qr{^=item L<Opinions about taxa|/data1.2/taxa/opinions_doc.html>}mi,
	'link to subsection' );
    
    ok( $content_pod !~ qr{node:taxa/|op:taxa/}m, 'link translation' );
    
    # $$$$ add checks for links etc.
};


subtest '/taxa/single html documentation checks' => sub {
    
    unless ( defined $cs_html && $cs_html ne '' )
    {
	fail('html content empty');
	diag('skipping remainder of tests');
	return;
    }
    
    
    my ($section, %ok);
    
    foreach my $line ( split( qr{[\n\r]+}, $cs_html ) )
    {
	if ( $line =~ qr{^<h2 \s+ class="pod_heading" \s+ id="([^"]+)">}xs )
	{
	    $section = $1;
	}
	
	elsif ( $line =~ qr{<h2 \s+ id="contact"}xs )
	{
	    $section = 'CONTACT';
	}
	
	elsif ( ! $section )
	{
	    $ok{title} = 1 if $line =~ qr{<title>.*PBDB[^<]*</title>};
	    $ok{header} = 1 if $line =~ qr{<h1 id="title">.*PBDB[^<]*</h1>};
	    $ok{nav} = 1 if $line =~ qr{<a class="pod_link" href="/$PREFIX/">[^<]*PBDB};
	    $ok{img} = 1 if $line =~ qr{<img [^<]*src="/$PREFIX/};
	}
	
	elsif ( $section eq 'DESCRIPTION' )
	{
	    $ok{description} = 1 if $line =~ qr{<p class="pod_para">\w};
	}
	
	elsif ( $section eq 'USAGE' )
	{
	    $ok{usage} = 1 if $line =~ qr{href="/$PREFIX/taxa/single.json};
	}
	
	elsif ( $section eq 'PARAMETERS' )
	{
	    $ok{parameters} = 1 if $line =~ qr{<td class="pod_term">show</td>};
	}
	
	elsif ( $section eq 'METHODS' )
	{
	    $ok{methods} = 1 if $line =~ qr{GET};
	}
	
	elsif ( $section eq 'RESPONSE' )
	{
	    $ok{response_a} = 1 if $line =~ qr{<td class="pod_def">basic</td>};
	    $ok{response_b} = 1 if $line =~ qr{<td class="pod_def">crmod</td>};
	}
	
	elsif ( $section eq 'FORMATS' )
	{
	    $ok{formats} = 1 if $line =~ qr{>JSON format<};
	}
	
	elsif ( $section eq 'VOCABULARIES' )
	{
	    $ok{vocabularies} = 1 if $line =~ qr{>Compact field names<};
	}
	
	elsif ( $section eq 'CONTACT' )
	{
	    $ok{contact} = 1 if $line =~ qr{href="mailto:([^"]+)">\1};
	}
    }
    
    foreach my $check ( qw(title header nav img
			   description usage parameters methods
			   response_a response_b formats vocabularies contact) )
    {
	ok( $ok{$check}, "html $check" );
    }
};


subtest '/taxa/single pod documentation checks' => sub {
    
    unless ( defined $cs_pod && $cs_pod ne '' )
    {
	fail('pod content empty');
	diag('skipping remainder of tests');
	return;
    }
    
    my ($section, %ok);
    
    foreach my $line ( split( qr{[\n\r]+}, $cs_pod ) )
    {
	if ( $line =~ qr{^=head2 (.*)} )
	{
	    $section = $1;
	}
	
	elsif ( $line =~ qr{^=begin (.*)} )
	{
	    $section = $1;
	}
	
	elsif ( ! $section )
	{
	    $ok{encoding} = 1 if $line =~ qr{^=encoding utf8$};
	}
	
	elsif ( $section eq 'wds_nav' )
	{
	    $ok{nav} = 1 if $line =~ qr{^=head3 L<[^|]+[|]/$PREFIX/> E<gt>.*single taxon}i;
	}
	
	elsif ( $section eq 'DESCRIPTION' )
	{
	    $ok{description} = 1 if $line =~ qr{single};
	}
	
	elsif ( $section eq 'USAGE' )
	{
	    $ok{usage} = 1 if $line =~ qr{^L</$PREFIX/taxa/single[.](json|txt)[?]\w+=};
	}
	
	elsif ( $section eq 'PARAMETERS' )
	{
	    $ok{parameters} = 1 if $line =~ qr{^=item show$};
	}
	
	elsif ( $section eq 'METHODS' )
	{
	    $ok{methods} = 1 if $line =~ qr{C<GET>};
	}
	
	elsif ( $section eq 'RESPONSE' )
	{
	    $ok{response_header} = 1 if $line =~ 
		qr{ ^ =for \s+ wds_table_header \s+ Field \s+ name[*]/[2-9] 
		      \s+ [|] \s+ Block \s+ [|] \s+ Description $ }xsi;
	    $ok{response_item} = 1 if $line =~ 
		qr{ ^ =item \s+ \w+ \s+ (?:/ \s+ \w+)+ \s+ [(] \s+ basic \s+ [)] $ }xs;
	    $ok{response_body} = 1 if $line =~
		qr{ taxonomic \s+ name }xsi;
	}
	
	elsif ( $section eq 'FORMATS' )
	{
	    $ok{format_json} = 1 if $line =~ qr{L<JSON format[|]/$PREFIX/formats/json_doc.html};
	    $ok{format_txt} = 1 if $line =~ qr{L<Text formats[|]/$PREFIX/formats/text_doc.html};
	}
	
	elsif ( $section eq 'VOCABULARIES' )
	{
	    $ok{vocabulary_json} = 1 if $line =~ 
		qr{^=item Compact field names [|] C<com> [|] C<json>$};
	    $ok{vocabulary_com} = 1 if $line =~ 
		qr{^=item PaleobioDB field names [|] C<pbdb> [|] C<txt>, C<csv>, C<tsv>$};
	}
	
	elsif ( $section eq 'CONTACT' )
	{
	    $ok{contact} = 1 if $line =~ qr{L<([^|]+)[|]mailto:\1>};
	}
    }
    
    foreach my $check ( qw(encoding nav description usage parameters methods contact
			   response_header response_item response_body
			   format_json format_txt vocabulary_json vocabulary_com) )
    {
	ok( $ok{$check}, "pod $check" );
    }
};

