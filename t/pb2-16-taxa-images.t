# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test the /data1.2/taxa/refs and /data1.2/taxa/byrefs operations,
# including all of the numerous parameters.
# 

use strict;

use feature 'unicode_strings';
use feature 'fc';

use Test::More tests => 4;

use lib 't';
use Tester;
# use Test::Conditions;

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });


# First test selection of images by id.  Check basic attributes of the response.

subtest 'image basic' => sub {

    my $ID1 = "1000";
    my $ID2 = "php:1000";
    
    my ($m1) = $T->fetch_url("/taxa/thumb.png?id=$ID1");
    
    unless ( $m1 && $m1->code eq '200' )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    $T->ok_content_type("image/png", undef, "thumb has proper content type");
    
    my $img1 = $m1->content;
    
    my (@r1) = $T->fetch_records("/taxa/thumb.json?id=$ID1");
    
    my ($m2) = $T->fetch_url("/taxa/icon.png?id=$ID1");
    
    $T->ok_content_type("image/png", undef, "icon has proper content type");
    
    my $img2 = $m2->content;
    
    cmp_ok( length($img1), '!=', length($img2), "thumb has different size from icon" );
    
    my (@r2) = $T->fetch_records("taxa/icon.json?id=$ID1");
    
    cmp_ok( @r1, '==', 1, "taxa/thumb.json found one record" );
    cmp_ok( @r2, '==', 1, "taxa/icon.json found one record" );
    
    is_deeply( $r1[0], $r2[0], "two attribute records match" );
    
    cmp_ok( $r1[0]{oid}, 'eq', $ID2, "attribute record includes proper oid" );

    my ($r2b) = $T->fetch_records("/taxa/icon.json?id=$ID1&extids=no");

    cmp_ok( $r2b->{oid}, 'eq', $ID1, "attribute record with extids=no includes proper oid" );
    
    my ($m3) = $T->fetch_url("/taxa/thumb.png?id=$ID2");
    
    my $img3 = $m3->content;

    cmp_ok( $img1, 'ne', '', "image content not empty" );
    cmp_ok( $img1, 'eq', $img3, "image fetched with extended id gets same content" );
};


# Now check for a nonexistant image

subtest 'image bad id' => sub {

    my $BAD1 = '99999999';
    my $BAD2 = '99abc';
    my $BAD3 = 'txn:1000';
    
    my ($m1) = $T->fetch_nocheck("/taxa/thumb.png?id=$BAD1");
    
    $T->ok_response_code("404", "nonexistent image id gets 404 response");
    
    my ($m2) = $T->fetch_nocheck("/taxa/icon.png?id=$BAD2");
    
    $T->ok_response_code("400", "bad image id gets 400 response");

    my ($m3) = $T->fetch_nocheck("/taxa/thumb.png?id=$BAD3");

    $T->ok_response_code("400", "wrong type of extended id gets 400 response");
};


# Now check fetching images by taxon id

subtest 'image by taxon id' => sub {
    
    my $TAXON1 = '41198';
    my $TAXON1t = 'txn:41198';
    my $TAXON1v = 'var:41198';

    my ($m1) = $T->fetch_url("/taxa/thumb.png?taxon_id=$TAXON1");
    
    unless ( $m1 && $m1->code eq '200' )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my $img1 = $m1->content;

    my ($m2) = $T->fetch_url("/taxa/thumb.png?taxon_id=$TAXON1t");

    my $img2 = $m2->content;

    my ($m3) = $T->fetch_url("/taxa/thumb.png?taxon_id=$TAXON1v");

    my $img3 = $m3->content;

    cmp_ok( $img1, 'ne', '', "image content not empty" );
    cmp_ok( $img1, 'eq', $img2, "image content matches (a)" );
    cmp_ok( $img1, 'eq', $img3, "image content matches (b)" );
};


# Now check fetching images by taxon name, including synonyms

subtest 'image by taxon name' => sub {
    
    my $NAME1 = "canis";
    my $NAME2 = "canis,felis";
    
    my $NAME3a = "dinosauria^aves";
    my $NAME3b = "dinosauria";
    
    my $NAME4a = "gastro:ficus";
    my $NAME4b = "plant:ficus";
    
    my $NAME5a = 'stegosaurus';
    my $NAME5b = 'hypsirophus';
    
    my $NAME6a = "not found";
    my $NAME6b = "not*a*taxon";
    
    my ($m1) = $T->fetch_url("/taxa/thumb.png?name=$NAME1");
    
    unless ( $m1 && $m1->code eq '200' )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my $img1 = $m1->content;
    
    cmp_ok( $img1, 'ne', '', "image content not empty" );
    
    my ($m2) = $T->fetch_nocheck("/taxa/thumb.png?name=$NAME2");

    $T->ok_response_code("400", "two names gets 400 response");

    my ($m3a) = $T->fetch_url("/taxa/thumb.png?name=$NAME3a");

    my $img3a = $m3a->content;

    my ($m3b) = $T->fetch_url("/taxa/thumb.png?name=$NAME3b");

    my $img3b = $m3b->content;
    
    cmp_ok( $img3a, 'ne', '', "image 3 not empty" );
    cmp_ok( $img3a, 'eq', $img3b, "exclusion gets same image as base taxon" );

    my ($m4a) = $T->fetch_url("/taxa/thumb.png?name=$NAME4a");

    my $img4a = $m4a->content;
    
    my ($m4b) = $T->fetch_url("/taxa/thumb.png?name=$NAME4b");

    my $img4b = $m4b->content;
    
    cmp_ok( $img4a, 'ne', '', "image 4a not empty" );
    cmp_ok( $img4b, 'ne', '', "image 4b not empty" );
    cmp_ok( $img4a, 'ne', $img4b, "$NAME4a gets different image than $NAME4b" );
    
    my ($r5a) = $T->fetch_records("/taxa/thumb.json?name=$NAME5a");

    my ($r5b) = $T->fetch_records("/taxa/thumb.json?name=$NAME5b");

    cmp_ok( $r5a->{oid}, 'eq', $r5b->{oid}, "synonyms get same image" );
    cmp_ok( $r5a->{oid}, 'ne', '', "image id not empty" );

    my ($m6a) = $T->fetch_nocheck("/taxa/thumb.png?name=$NAME6a");

    $T->ok_response_code( "404", "nonexistent name gets 404 response" );

    my ($m6b) = $T->fetch_nocheck("/taxa/thumb.png?name=$NAME6b");

    $T->ok_response_code( "400", "name with bad character gets 400 response" );
};
