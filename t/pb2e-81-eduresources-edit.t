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

use Test::More tests => 8;

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
	$T->ok_error_like(qr{E_PERM.*status}i, "update non-owned returned proper error message");
	
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
    
    # 1. create a new record with image data
    
    # 2. create a new record without image data
    
    # 3. add image data to that record
    
    # 4. activate both records
    
    # 5. check that the image data files appear
    
    # 5a. fetch the contents and make sure they match
    
    # 6. deactivate one record and delete the other
    
    # 7. check that the image data file vanishes
    
    # 8. fetch the image data for the deactivated record and check that it matches.
    
    
};


# Now test administrators can delete records, and that enterers without
# superuser or administrative privilege can only delete their own records.


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
