# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test both querying and data entry for educational resources.
# 

use strict;

use feature 'unicode_strings';
use feature 'fc';

use Test::More tests => 11;

# use CoreFunction qw(connectDB configData);
# use Taxonomy;

use lib 'lib', '../lib';

use ExternalIdent qw(%IDRE);
use TaxonDefs;

use lib 't';
use Tester;
use Test::Conditions;
use Test::Selection;


my (%FIELD_MAP) = ( record_label => 'rlb',
		    eduresource_no => 'oid',
		    eduresource_id => 'oid',
		    status => 'sta' );

my ($TEST_TITLE_1) = "Test Record 2b";
my ($LIST_TITLE_1) = "Test%20Record%202b";

my ($TEST_TITLE_2) = "Test Record Updated A";
my ($LIST_TITLE_2) = "Test%20Record%20Updated%20A";

my ($NO_CLEANUP);

# Check for options

if ( $ARGV[0] eq '--no-cleanup' )
{
    shift @ARGV;
    $NO_CLEANUP = 1;
}

# If we provided any command-line arguments, run only subtests whose names match.

choose_subtests(@ARGV);

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });

# Create a second instance of Tester for making requests on the main web
# server. 

my $TT = Tester->new({ server => '127.0.0.1:80' });

# Then check to MAKE SURE that the server is in test mode and the test eduresources tables are
# enabled. This is very important, because we DO NOT WANT to change the data in the main
# tables. If we don't get the proper response back, we need to bail out. These count as the first
# two tests i this file.

$T->test_mode('session_data', 'enable') || BAIL_OUT("could not select test session data");
$T->test_mode('eduresources', 'enable') || BAIL_OUT("could not select test eduresource tables");


# Test adding a new resource record. We start by testing under a userid with the superuser
# privilege. In fact, we test that a user with this privilege can add an active record as opposed
# to a pending one. We also test that all of the fields for this record type are properly stored
# and reported back.

subtest 'add simple' => sub {
    
    select_subtest || return;
    
    # First, see if a user with the superuser privileges can add. If this fails, there is no
    # reason to go any further.
    
    $T->set_cookie("session_id", "SESSION-SUPERUSER");
    
    my $record1 = { record_label => 'a1',
		    title => 'Test Record 1',
		    tags => 'api,tutorial',
		    description => 'Test record with \'é\'.',
		    url => 'http://paleobiodb.org/',
		    is_video => 0,
		    author => 'Somebody',
		    audience => 'Test',
		    email => 'Email test',
		    affil => 'Affil test',
		    orcid => '1234-5678-1234-5678',
		    taxa => 'Aves',
		    timespan => 'Cretaceous',
		    topics => 'Some topic, some other topic',
		    status => 'active' };
    
    my (@r1) = $T->send_records("/eduresources/addupdate.json", "superuser add", json => $record1);
    
    unless ( @r1 )
    {
	BAIL_OUT("adding a new record failed");
    }
    
    foreach my $k ( keys %$record1 )
    {
	if ( $k eq 'tags' )
	{
	    like($r1[0]{tags}, qr{API.*tutorial|tutorial.*API}, "added record has tags 'API' and 'tutorial'");
	}
	
	else
	{
	    my $f = $FIELD_MAP{$k} || $k;
	    cmp_ok($r1[0]{$f}, 'eq', $record1->{$k}, "added record has correct value for '$k'");
	}
    }
    
    like($r1[0]{oid}, qr{^edr:\d+$}, "added record has proper oid");
    
    # Now make sure that the new record is actually active.
    
    my $new_id = $r1[0]{oid};
    
    if ( $new_id )
    {
	my (@a1) = $T->fetch_records("/eduresources/active.json?id=$new_id", "fetch new record active version",
				     { no_records_ok => 1 });
	
	unless ( @a1 )
	{
	    fail("new record was activated");
	}
    }
    
    # Check that a record added by the superuser without an explicit status gets the status of
    # 'pending'. 
    
    my $record2 = { title => 'Test pending' };
    
    my (@r2) = $T->send_records("/eduresources/addupdate.json", "superuser add 2", json => $record2);
    
    $new_id = $r2[0]{oid};
    
    ok($new_id, "superuser add 2 gives proper oid");
    
    if ( $new_id )
    {
	my ($m2) = $T->fetch_url("/eduresources/active.json?id=$new_id", "fetch new record active 2",
				 { no_records_ok => 1 } );
	
	$T->ok_no_records("new record was not active");
	cmp_ok($r2[0]{sta}, 'eq', 'pending', "new record had status 'pending'");
    }
};


# Now test adding by non-superusers. We need to make sure that only people with the proper
# permission can add to the table.

subtest 'add by role' => sub {

    select_subtest || return;
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    my $record2a = { record_label => '2a',
		    title => 'Test Record 2a' };
    
    my (@r2a) = $T->send_records("/eduresources/addupdate.json", "enterer add", json => $record2a);
    
    ok($r2a[0]{sta} eq 'pending', "added record with status 'pending'");
    
    $T->set_cookie("session_id", "SESSION-STUDENT");
    
    my $record2b = { record_label => '2b',
		    title => $TEST_TITLE_1 };
    
    my (@r2b) = $T->send_records("/eduresources/addupdate.json", "student add", json => $record2b);
    
    ok($r2b[0]{sta} eq 'pending', "added record with status 'pending'");
    
    $T->set_cookie("session_id", "SESSION-WITH-ADMIN");
    
    my $record2c = { record_label => '2c',
		    title => 'Test Record 2c' };
    
    my (@r2c) = $T->send_records("/eduresources/addupdate.json", "table permission add", json => $record2c);
    
    ok($r2c[0]{sta} eq 'pending', "added record with status 'pending'");
    
    $T->set_cookie("session_id", "SESSION-GUEST");
    
    my $record2d = { record_label => '2c',
		    title => 'Test Record 2d' };
    
    my (@r2d) = $T->send_records("/eduresources/addupdate.json", "guest add", json => $record2d);
    
    $T->set_cookie("session_id", "");
    
    my $record2e = { record_label => '2e',
		    title => 'Test Record 2e' };
    
    my ($m) = $T->send_data_nocheck("/eduresources/addupdate.json", "no login add", json => $record2e);
    
    $T->ok_response_code("401", "no login add gets 401 response");
};


# Now test the users with superuser or administrative privilege can update any resource and can
# activate them and deactivate them as well.

subtest 'update admin' => sub {
    
    select_subtest || return;
    
    $T->set_cookie("session_id", "SESSION-SUPERUSER");
    
    # Check to see if we can fetch the test record. If not, then we cannot complete this subtest.
    
    my (@r1) = $T->fetch_records("/eduresources/list.json?title=$LIST_TITLE_1", "fetch test record");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Now try to update this record as the superuser. Make sure we can change all of the fields
    # including the status.
    
    my $test_record_oid = $r1[0]{oid};
    
    my $update1 = { eduresource_id => $test_record_oid,
		    title => $TEST_TITLE_2,
		    tags => 'web',
		    description => 'Updated description',
		    url => 'http://updated.org/',
		    is_video => 1,
		    author => 'Somebody else',
		    audience => 'Test 2',
		    email => 'Email test 2',
		    affil => 'Affil test 2',
		    orcid => '1234-5678-1234-5679',
		    taxa => 'Protoaves',
		    timespan => 'Cretaceous',
		    topics => 'Some new topic',
		    status => 'inactive' };
    
    my (@r2) = $T->send_records("/eduresources/update.json", "superuser update 1", json => $update1);
    
    unless ( @r2 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    foreach my $k ( keys %$update1 )
    {
	my $f = $FIELD_MAP{$k} || $k;
	cmp_ok($r2[0]{$f}, 'eq', $update1->{$k}, "added record has correct value for '$k'");
    }
    
    # Double-check by fetching the record separately from both the master table and the active
    # table.

    my (@r2a) = $T->fetch_records("/eduresources/list.json?id=$test_record_oid", "check update 1");
    
    cmp_ok($r2a[0]{title}, 'eq', $r2[0]{title}, "separate fetch returns same title");
    
    my ($m2c) = $T->fetch_nocheck("/eduresources/active.json?id=$test_record_oid", "check active 1");
    
    $T->ok_no_records("inactivated record not found in active list");
    
    # Then try to update this record as a user with administrative permission. We only need to
    # test a single field change, plus status change.

    $T->set_cookie("session_id", "SESSION-WITH-ADMIN");
    
    my $update2 = { eduresource_id => $test_record_oid,
		    description => 'Updated 2',
		    status => 'pending' };
    
    my (@r3) = $T->send_records("/eduresources/update.json", "admin update 1", json => $update2);
    
    unless ( @r3 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    cmp_ok($r3[0]{description}, 'eq', $update2->{description}, "admin update 1 was carried out");
    cmp_ok($r3[0]{sta}, 'eq', $update2->{status}, "admin update 1 changed status");

    # Then try to activate the resource.

    my (@r3a) = $T->fetch_records("/eduresources/update.json?id=$test_record_oid&status=active",
				  "admin activate");

    cmp_ok($r3a[0]{sta}, 'eq', 'active', "admin activate succeeded");

    my (@r3b) = $T->fetch_records("/eduresources/active.json?id=$test_record_oid", "check active 3");
};


# Now test that a non-admin user can edit records they created and only those, and cannot change
# record status.

subtest 'update nonadmin' => sub {
    
    select_subtest || return;
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    # Fetch information about the user corresponding to this session id.
    
    my (@p1) = $T->fetch_records("/people/me.json", "fetch logged-in user");
    
    # Fetch the list of records, and choose one that was entered by this person and another that
    # was not. If there aren't any, skip those tests.
    
    my (@r1) = $T->fetch_records("/eduresources/list.json?all_records&show=ent", "list all");
    
    my ($owned_oid, $not_owned_oid);
    
    foreach my $r (@r1)
    {
	$owned_oid = $r->{oid} if $r->{eni} && $r->{eni} eq $p1[0]{oid};
	$not_owned_oid = $r->{oid} if $r->{eni} && $r->{eni} ne $p1[0]{oid};
    }
    
    ok($owned_oid, "found an owned record");
    ok($not_owned_oid, "found a non-owned record");
    
    # Try to change the owned record.
    
    my $update1 = { description => "changed owned" };
    my $update2 = { description => "changed non-owned" };
    
    if ( $owned_oid )
    {
	my (@r2a) = $T->send_records("/eduresources/update.json?id=$owned_oid", "update owned",
				     json => $update1);
	
	if ( @r2a )
	{
	    cmp_ok($r2a[0]{description}, 'eq', $update1->{description}, "updated record correctly");
	}
	$DB::single = 1;
	my $m2b = $T->fetch_nocheck("/eduresources/update.json?id=$owned_oid&status=active", 
					"update owned status");
	
	$T->ok_response_code("400", "update owned status returned proper error code");
	$T->ok_error_like(qr{E_PERM.*status}i, "update owned status returned proper warning");
    }
    
    # Then try to change the non-owned record.
    
    if ( $not_owned_oid )
    {
	my $m3a = $T->send_data_nocheck("/eduresources/update.json?id=$not_owned_oid",
				       "update non-owned", json => $update2);
	
	$T->ok_response_code("400", "update non-owned returned proper response code");
	$T->ok_error_like(qr{E_PERM.*update}i, "update non-owned returned proper error message");
	
	my $m3b = $T->fetch_nocheck("/eduresources/update.json?id=$not_owned_oid&status=active",
				    "update non-owned status");
	
	$T->ok_response_code("400", "update non-owned status returned proper response code");
	$T->ok_error_like(qr{E_PERM.*status}i, "update non-owned status returned proper error message");
    }
    
    # Now we switch to the admin user and activate the owned record, and then switch back and
    # update that record. Check that the new status is 'changes'.
    
    $T->set_cookie("session_id", "SESSION-WITH-ADMIN");
    
    my (@r4a) = $T->send_records("/eduresources/update.json", "activate owned",
				json => { eduresource_id => $owned_oid, status => 'active' });
    
    if ( @r4a )
    {
	cmp_ok($r4a[0]{sta}, 'eq', 'active', "activate owned set proper status");
    }
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");    
    
    my (@r4b) = $T->send_records("/eduresources/addupdate.json", "update for changes",
				 json => { eduresource_id => $owned_oid, 
					   description => "update once again" });
    
    if ( @r4b )
    {
	cmp_ok($r4b[0]{sta}, 'eq', 'changes', "update for change set proper status");
    }
};


# Test image data.

subtest 'image data' => sub {
    
    select_subtest || return;
    
    # 1. create a new record with image data

    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    my $image1a = { record_label => 'a1',
		   title => 'Test With Image 1',
		   image_data => &image1 };
    
    my (@r1) = $T->send_records("/eduresources/addupdate.json", "insert with image 1",
				json => $image1a);
    my $oid1 = $r1[0]{oid};
    cmp_ok( $r1[0]{rlb}, 'eq', 'a1', "insert with image 1 got proper label" ) if @r1;
    
    # 2. create a new record without image data
    
    my $image1b = { record_label => 'a2',
		   title => 'Test With Image 2' };

    my (@r2) = $T->send_records("/eduresources/addupdate.json", "insert for update test",
				json => $image1b);
    
    my $oid2 = $r2[0]{oid};
    cmp_ok( $r2[0]{rlb}, 'eq', 'a2', "insert for update test got proper label" ) if @r2;
    
    # 3. add image data to that record

    my $update2 = { eduresource_id => $oid2,
		    image_data => &image1 };
    
    my (@r2b) = $T->send_records("/eduresources/addupdate.json", "update with image 1",
				 json => $update2);
    
    # 4. try an image with a different file type, that is too big and requires resizing.
    
    my $image2 = { record_label => 'a2',
		   title => 'Test With Image 2',
		   image_data => &image2 };
    
    my (@r2c) = $T->send_records("/eduresources/addupdate.json", "insert with image 2",
				json => $image2);
    my $oid2c = $r2c[0]{oid};
    cmp_ok( $r2c[0]{rlb}, 'eq', 'a2', "insert with image 2 got proper label" ) if @r2c;
    
    # 5. fetch the image contents and make sure they match.
    
    my (@r3) = $T->fetch_records("/eduresources/list.json?id=$oid1,$oid2,$oid2c&show=image",
				 "check images");
    
    cmp_ok( @r3, '==', 3, "check images fetched three records" );
    
    ok( $r3[0]{image_data} eq &image1, "image data 1 (insert) matches source" );
    ok( $r3[1]{image_data} eq &image1, "image data 1 (update) matches source" );
    ok( $r3[2]{image_data} ne &image2, "image data 2 does not match source" );
    
    my $converted_length = length($r3[2]{image_data});
    cmp_ok( $converted_length, '<', 8000, "image 2 was resized smaller" );
    
    # 6. activate all three records
    
    $T->set_cookie("session_id", "SESSION-WITH-ADMIN");
    
    my ($r4a) = $T->fetch_records("/eduresources/update.json?id=$oid1&status=active");
    my ($r4b) = $T->fetch_records("/eduresources/update.json?id=$oid2&status=active");
    my ($r4c) = $T->fetch_records("/eduresources/update.json?id=$oid2c&status=active");
    
    my $oid4a = $r4a->{oid};
    my $oid4b = $r4b->{oid};
    my $oid4c = $r4c->{oid};
    
    my $img1a = $r4a->{image};
    my $img1b = $r4b->{image};
    my $img2 = $r4c->{image};
    
    ok( $oid4a, "updated record 1a" ) &&
	cmp_ok( $r4a->{sta}, 'eq', 'active', "activated record 1 (insert)" );
    
    ok( $oid4b, "updated record 1b" ) &&
	cmp_ok( $r4b->{sta}, 'eq', 'active', "activated record 1 (update)" );
    
    ok( $oid4c, "updated record 2" ) &&
	cmp_ok( $r4b->{sta}, 'eq', 'active', "activated record 2" );
    
    diag( "file 1a: " . $img1a );
    diag( "file 1b: " . $img1b );
    diag( "file 2: " . $img2 );
    
    # 7. check that the image data files appear. Try to fetch them from the
    # main web server, but if we can't do that then try to read them from the
    # directory on disk. Obviously, the latter will only work if we are
    # running this test on the local machine.
    
    # my $image_path = `grep eduresources_img_path config.yml`;    
    # chomp $image_path;
    # $image_path =~ s/^.*:\s+//;
    
    my $image_dir = `grep eduresources_img_dir config.yml`;
    chomp $image_dir;
    $image_dir =~ s/^.*:\s+//;
    
    my ($m4a) = $TT->fetch_url($img1a, "fetch image 1a", { no_check => 1 });
    my ($m4b) = $TT->fetch_url($img1b, "fetch image 1b", { no_check => 1 });
    my ($m4c) = $TT->fetch_url($img2, "fetch image 2", { no_check => 1 });
    
    if ( $m4a && $TT->get_response_code($m4a) ne '' )
    {
	diag("fetching images from WEB SERVER");
	
	if ( ok( $TT->get_response_code($m4a) eq '200', "fetch image 1a returned 200" ) )
	{
	    like( $m4a->content_type, qr{image/jpe?g}, "fetched image 1a has proper content type" );
	    cmp_ok( length($m4a->content), '>', 0, "fetched image 1a has non-empty content" );
	}
	
	if ( ok( $m4b && $TT->get_response_code($m4b) eq '200', "fetch image 1b returned 200" ) )
	{
	    like( $m4b->content_type, qr{image/jpe?g}, "Fetched image 1b has proper content type" );
	    cmp_ok( length($m4b->content), '>', 0, "fetched image 1b has non-empty content" );
	}
	
	if ( ok( $m4c && $TT->get_response_code($m4c) eq '200', "fetch image 2 returned 200" ) )
	{
	    like( $m4c->content_type, qr{image/gif}, "Fetched image 2 has proper content type" );
	    cmp_ok( length($m4c->content), '>', 0, "fetched image 2 has non-empty content" );
	}
    }
    
    elsif ( $image_dir )
    {
	diag("fetching images from DIRECTORY");
	
	my $image_file1 = "$image_dir$img1a";
	my $image_file2 = "$image_dir$img1b";
	my $image_file3 = "$image_dir$img2";
	
	ok( -e $image_file1, "found image file 1" );
	ok( -e $image_file2, "found image file 2" );
	ok( ! -e $image_file3, "did not find image file 3" );
    }
    
    else
    {
	fail("check for images");
    }
    
    # 6. deactivate one record and delete the other
    
    my ($r5a) = $T->fetch_records("/eduresources/update.json?id=$oid1&status=pending");
    
    cmp_ok( $r5a->{sta}, 'eq', 'pending', "status of record $oid1 returned 'pending'" );
    
    my ($r5b) = $T->fetch_records("/eduresources/delete.json?id=$oid2");
    
    cmp_ok( $r5b->{sta}, 'eq', 'deleted', "status of record $oid2 returned 'deleted'" );
    
    # 7. check that the image data file vanishes
    
    my ($m5a) = $TT->fetch_url($img1a, "fetch image 1a", { no_check => 1 });
    my ($m5b) = $TT->fetch_url($img1b, "fetch image 1b", { no_check => 1 });
    
    if ( $m5a && $TT->get_response_code($m5a) ne '' )
    {
	diag("checking deleted images using WEB SERVER");
	
	ok( $TT->get_response_code($m5a) eq '404', "image 1a has been deleted" );
	ok( $TT->get_response_code($m5b) eq '404', "image 1b has been deleted" );
    }
    
    elsif ( $image_dir )
    {
	diag("checking deleted images using DIRECTORY");
	
	my $image_file1 = "$image_dir$img1a";
	my $image_file2 = "$image_dir$img1b";
	
	ok( ! -e $image_file1, "image file 1 has been deleted" );
	ok( ! -e $image_file2, "image file 2 has been deleted" );
    }
    
    else
    {
	fail("check for image deletion");
    }
};


# Now test administrators can delete records, and that enterers without
# superuser or administrative privilege can only delete their own records.

subtest 'delete permissions' => sub {
    
    select_subtest || return;
    
    # First insert a record under the admin userid
    
    $T->set_cookie("session_id", "SESSION-WITH-ADMIN");
    
    my $test1 = { record_label => '1a',
		  title => 'Delete Test 1' };
    
    my (@r1) = $T->send_records("/eduresources/addupdate.json", "insert for delete test 1",
				json => $test1);
    
    ok( $r1[0]{oid}, 'inserted record 1' );
    
    my $oid1 = $r1[0]{oid};
    
    # Then insert two records under a non-admin userid
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    my $test2 = { records => [ { record_label => '2a',
				 title => 'Delete Test 2' },
			       { record_label => '3a',
				 title =>'Delete Test 3' } ] };
    
    my (@r2) = $T->send_records("/eduresources/addupdate.json", "insert for delete test 2,3",
				json => $test2);
    
    ok( $r2[0]{oid}, 'inserted record 2' );
    ok( $r2[1]{oid}, 'inserted record 3' );
    
    my $oid2 = $r2[0]{oid};
    my $oid3 = $r2[1]{oid};
    
    # Now try to delete the record we don't own.
    
    my ($m1) = $T->fetch_nocheck("/eduresources/delete.json?id=$oid1", "delete non-owned record 1");
    
    $T->ok_response_code($m1, '400', "delete non-owned record 1 got 400 response");
    $T->ok_error_like($m1, qr{E_PERM.*delete}, "delete non-owned record 1 got appropriate error message");
    
    # Then try to delete a record we do own.
    
    my ($m2) = $T->fetch_url("/eduresources/delete.json?id=$oid3", "delete non-owned record 3");
    
    # Now switch back to the admin userid and try to delete both the record they own and the one
    # they don't.
    
    $T->set_cookie("session_id", "SESSION-WITH-ADMIN");
    
    my ($m3) = $T->fetch_url("/eduresources/delete.json?id=$oid1", "delete owned record 1");
    my ($m4) = $T->fetch_url("/eduresources/delete.json?id=$oid2", "delete admin record 2");
};


# Attempt to insert and update records with invalid fields, and check that the
# proper errors are returned.

subtest 'invalid' => sub {

    select_subtest || return;
    
    $T->set_cookie("session_id", "SESSION-WITH-ADMIN");
    
    # Try to insert several records with errors, and check that we get back the right messages.
    
    my $errors = [ { record_label => 'e1', author => 'somebody' },	# 'title' not specified
		   { record_label => 'e2', title => 'Error 2',		# 'author' too long
		     author => 'abcdefg' x 100 },
		   { record_label => 'e3', title => 'Error 3',		# bad value for 'orcid'
		     orcid => '1234-5678-9012-345',			# and for 'is_video'
		     is_video => 'foo' },
		   { record_label => 'e4', title => 'Error 4',		# bad tags
		     tags => 'foo, bar, api' },
		   { record_label => 'e5', title => 'Error 5',		# bad image data
		     image_data => 'foobar' },
		   { record_label => 'e6', title => 'Error 6',		# this one is good
		     author => 'abcd' } ];
    
    my ($m1) = $T->send_data_nocheck("/eduresources/addupdate.json", "insert with errors",
				     json => $errors);
    
    $T->ok_response_code($m1, '400', "insert with errors got 400 response");
    $T->ok_error_like($m1, qr{E_REQUIRED \(e1\):.*title}i, "error for missing title");
    $T->ok_error_like($m1, qr{E_PARAM \(e2\):.*characters}i, "error for too long author");
    $T->ok_error_like($m1, qr{E_PARAM \(e3\):.*orcid.*valid}i, "error for bad orcid");
    $T->ok_error_like($m1, qr{E_PARAM \(e3\):.*is_video.*1}i, "error for bad is_video");
    $T->ok_error_like($m1, qr{E_PARAM \(e5\):.*format}i, "error for bad image data");
    $T->ok_warning_like($m1, qr{W_TAG_NOT_FOUND \(e4\):.*foo}i, "warning for bad tag 'foo'");
    $T->ok_warning_like($m1, qr{W_TAG_NOT_FOUND \(e4\):.*bar}i, "warning for bad tag 'bar'");
    
    # Then try to insert the same records with the allowance 'PROCEED'. Records 4 and 6 should be
    # inserted.
    
    my ($m2) = $T->send_data("/eduresources/addupdate.json?allow=PROCEED", 
			     "insert with errors and proceed", json => $errors,
			     { no_diag => 1 });
    
    my (%label) = $T->scan_records($m2, 'rlb', "scan records");
    
    ok( $label{'e4'}, "inserted record e4" );
    ok( $label{'e6'}, "inserted record e6" );
    
    $T->ok_warning_like($m2, qr{F_REQUIRED}, "warning for missing title");
    $T->ok_warning_like($m2, qr{F_PARAM.*'author'}, "warning for too long author");
    $T->ok_warning_like($m2, qr{F_PARAM.*'orcid'}, "warning for bad orcid");
    $T->ok_warning_like($m2, qr{F_PARAM.*'is_video'}, "warning for bad is_video");
    $T->ok_warning_like($m2, qr{F_PARAM.*image_data}, "warning for bad image data");
};


# Finish by cleaning up all records in the test eduresources table that were entered by the test
# users. This is a final safeguard just in case the real table is being used by mistake.

subtest 'cleanup' => sub {
    
    select_subtest || return;

    if ( $NO_CLEANUP )
    {
	ok("skipping cleanup");
	return;
    }
    
    $T->set_cookie("session_id", "SESSION-SUPERUSER");
    
    my (@r1) = $T->fetch_records("/eduresources/list.json?all_records&show=ent", "list all");
    
    my @id_list;
    
    foreach my $r ( @r1 )
    {
	if ( $r->{eni} && ( $r->{eni} =~ /prs:3\d\d\d/ || $r->{eni} eq 'USERID-GUEST' ) )
	{
	    push @id_list, $r->{oid};
	}
    }
    
    unless ( @id_list )
    {
	fail("There should be records that need to be cleaned up, but none were found.");
	return;
    }

    my $id_string = join(',', @id_list);

    my (@r2) = $T->fetch_records("/eduresources/delete.json?id=$id_string", "delete records to clean up");
    
    my $delete_count = scalar(@r2);

    diag("Cleaned up $delete_count records.");
    
    cmp_ok($delete_count, '==', @id_list);
};


select_final;


sub image1 {

    return "data:image/png;base64,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
MPjj+d/n8fldK6Ur/9k=";
}

sub image2 {
    return "R0lGODlhHATbA8QQAAEBARERESEhITExMUFBQVFRUWFhYXFxcYCAgI+Pj5+fn6+vr7+/v8/Pz9/f
3+/v7////wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACH/C1hN
UCBEYXRhWE1QPD94cGFja2V0IGJlZ2luPSLvu78iIGlkPSJXNU0wTXBDZWhpSHpyZVN6TlRjemtj
OWQiPz4gPHg6eG1wbWV0YSB4bWxuczp4PSJhZG9iZTpuczptZXRhLyIgeDp4bXB0az0iQWRvYmUg
WE1QIENvcmUgNS4wLWMwNjEgNjQuMTQwOTQ5LCAyMDEwLzEyLzA3LTEwOjU3OjAxICAgICAgICAi
PiA8cmRmOlJERiB4bWxuczpyZGY9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkvMDIvMjItcmRmLXN5
bnRheC1ucyMiPiA8cmRmOkRlc2NyaXB0aW9uIHJkZjphYm91dD0iIiB4bWxuczp4bXBNTT0iaHR0
cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RSZWY9Imh0dHA6Ly9ucy5hZG9i
ZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZVJlZiMiIHhtbG5zOnhtcD0iaHR0cDovL25zLmFk
b2JlLmNvbS94YXAvMS4wLyIgeG1sbnM6ZGM9Imh0dHA6Ly9wdXJsLm9yZy9kYy9lbGVtZW50cy8x
LjEvIiB4bXBNTTpPcmlnaW5hbERvY3VtZW50SUQ9InV1aWQ6NUQyMDg5MjQ5M0JGREIxMTkxNEE4
NTkwRDMxNTA4QzgiIHhtcE1NOkRvY3VtZW50SUQ9InhtcC5kaWQ6NEYyQ0RENkEyNkM1MTFFM0JD
RkFFOEQ1RUU3RUQ3N0YiIHhtcE1NOkluc3RhbmNlSUQ9InhtcC5paWQ6NEYyQ0RENjkyNkM1MTFF
M0JDRkFFOEQ1RUU3RUQ3N0YiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgSWxsdXN0cmF0b3IgQ1M1
LjEiPiA8eG1wTU06RGVyaXZlZEZyb20gc3RSZWY6aW5zdGFuY2VJRD0idXVpZDphZTk5MmZmNC05
ZDkwLTY3NGQtYmJmZS1jMTg4YTQ4NTc1MzIiIHN0UmVmOmRvY3VtZW50SUQ9InhtcC5kaWQ6Rjc3
RjExNzQwNzIwNjgxMTkyQjBFQ0Q1QjkwNjc1Q0IiLz4gPGRjOnRpdGxlPiA8cmRmOkFsdD4gPHJk
ZjpsaSB4bWw6bGFuZz0ieC1kZWZhdWx0Ij5QcmludDwvcmRmOmxpPiA8L3JkZjpBbHQ+IDwvZGM6
dGl0bGU+IDwvcmRmOkRlc2NyaXB0aW9uPiA8L3JkZjpSREY+IDwveDp4bXBtZXRhPiA8P3hwYWNr
ZXQgZW5kPSJyIj8+Af/+/fz7+vn49/b19PPy8fDv7u3s6+rp6Ofm5eTj4uHg397d3Nva2djX1tXU
09LR0M/OzczLysnIx8bFxMPCwcC/vr28u7q5uLe2tbSzsrGwr66trKuqqainpqWko6KhoJ+enZyb
mpmYl5aVlJOSkZCPjo2Mi4qJiIeGhYSDgoGAf359fHt6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNi
YWBfXl1cW1pZWFdWVVRTUlFQT05NTEtKSUhHRkVEQ0JBQD8+PTw7Ojk4NzY1NDMyMTAvLi0sKyop
KCcmJSQjIiEgHx4dHBsaGRgXFhUUExIREA8ODQwLCgkIBwYFBAMCAQAAIfkEAQAAEAAsAAAAABwE
2wMABf8gJI5kaZ5oqq5s675wLM90bd94ru987//AoHBILBqPyKRyyWw6n9CodEqtWq/YrHbL7Xq/
4LB4TC6bz+i0es1uu9/wuHxOr9vv+Lx+z+/7/4BrDQgIDA8vDwyBi4yNjo+QkZKTKw8LBgIAmpoC
BYUqDJkFlKSlpqeoqaqrNw0JBJuxspoDBggEuLmxCqy9vr/AwcLDXwuZs8jJyg3Ezc7P0NHS0A/H
ytfYAofT3N3e3+DhbQjY5dij4unq6+zt7jsPAebzyQvv9/j5+vvQDgX0AGVp40ewoMGDCPM4MBCw
YawDCSNKnEix4pQH5Bxq1JTAosePIEOKZIFR3saTAhL/OBjJsqXLl+9KnpwZi8A2mDhz6tzJChbN
n7Ru8hxKtKhRPAyAKgUggNnRp1CjSgXjcylQBCQNTd3KtavXG0mtLiWgQCgEBwwYMOSk6Kvbt3C9
DhBL1xOhf8kI2EJgL67fv4BbKqBLuOHKwIgTKy7owFrhx9gCOF1MubJlbwxMQt58LkHby6BDi16V
kbNpbAZGq17NutGC07CvGTDburbt220cxN6NbABt3MCDC+eimbdx38OTK18OJazx5wAGHGZOvbp1
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";
}
