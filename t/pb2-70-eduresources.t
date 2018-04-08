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

use Test::More tests => 6;

# use CoreFunction qw(connectDB configData);
# use Taxonomy;

use lib 'lib', '../lib';

use ExternalIdent qw(%IDRE);
use TaxonDefs;

use lib 't';
use Tester;
use Test::Conditions;
use Test::Selection;


# If we provided any command-line arguments, run only subtests whose names match.

choose_subtests(@ARGV);

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });


# Then try to enable the test session data table, but disable the test eduresources table. Since
# this test only queries public data, we don't have to bail out if either operation fails. This
# counts as the first two tests in this file.

$T->test_mode_nocheck('session_data', 'enable');
$T->test_mode_nocheck('eduresources', 'disable');


# Test querying for educational resource records under various conditions.

subtest 'by role' => sub {

    select_subtest || return;
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    my (@r1) = $T->fetch_records("/eduresources/list.json?all_records", "enterer all records");
    my (@r2) = $T->fetch_records("/eduresources/active.json", "enterer active records");
    
    $T->set_cookie("session_id", "SESSION-GUEST");
    
    my (@r3) = $T->fetch_records("/eduresources/list.json?all_records", "guest all records");
    my (@r4) = $T->fetch_records("/eduresources/active.json", "guest active records");

    $T->set_cookie("session_id", "");
    
    my ($m1) = $T->fetch_nocheck("/eduresources/list.json?all_records", "no login all records");
    my (@r5) = $T->fetch_records("/eduresources/active.json", "no login active records");
    
    $T->ok_response_code($m1, '401', "all records no login");
};


# Test the parameters for selecting various kinds of records, at least as much as we can. The
# eduresources-edit test file will be able to test this more thoroughly, by adding specific
# records and then querying for them.

subtest 'parameters' => sub {

    select_subtest || return;

    $T->set_cookie("session_id", "");

    # Check queries with no parameters.
    
    my (@r1) = $T->fetch_records("/eduresources/active.json", "active with no parameters");
    my ($m2) = $T->fetch_nocheck("/eduresources/list.json", "list with no parameters");
    
    $T->ok_response_code($m2, '400', "list with no parameters");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Check 'status'
    
    my (@r2) = $T->fetch_records("/eduresources/active.json?status=active", "active with status=active");
    
    cmp_ok(@r2, '<=', @r1, "active with status=active fetches no more records");
    
    my (@r2a) = $T->fetch_records("/eduresources/list.json?all_records&active", "list all active");
    
    cmp_ok(@r2a, '==', @r1, "list with ?active gets same number of records as active");
    
    $T->set_cookie("session_id", "SESSION-GUEST");
    
    # Check 'status' again with a login that can list non-active records.

    my (@r3) = $T->fetch_records("/eduresources/list.json?status=pending", "status=pending",
				 { no_records_ok =>1 });
    
    # Check 'all_records', and then check listing individual records with both 'list' and 'single'.
    
    my (@r4) = $T->fetch_records("/eduresources/list.json?all_records", "guest all records");
    
    unless ( @r4 )
    {
	diag "SKIPPING remainder of subtest";
	return;
    }
    
    my ($id0) = $r4[0]{oid};
    my ($id1) = $r4[1]{oid};
    
    $id1 ||= $id0;	# If we only have one record, list it twice to make sure the 'id'
                        # parameter can take multiple arguments.
    
    my $count = $id1 eq $id0 ? 1 : 2;
    
    my (@r5) = $T->fetch_records("/eduresources/list.json?id=$id0,$id1", "list specific records");
    
    cmp_ok(@r5, '==', $count, "found $count records");
    
    my (%list_ids) = $T->extract_values(\@r5, 'oid', "extract oids from list");
    
    ok($list_ids{$id0}, "found first oid");
    ok($list_ids{$id1}, "found second oid");
    
    my $i = $r5[0]{oid} eq $id0 ? 0 : 1;
    
    cmp_ok($r5[0]{title}, 'eq', $r4[$i]{title}, "titles match with list");

    my (@r6) = $T->fetch_records("/eduresources/single.json?id=$id0", "single record");

    cmp_ok(@r6, '<', 2, "found at most one record");
    
    cmp_ok($r6[0]{oid}, 'eq', $id0, "found correct record");
    
    cmp_ok($r6[0]{title}, 'eq', $r4[0]{title}, "titles match with single");
    
    # Check 'title' and 'keyword' parameters. Grab a word of at least five characters from title
    # and description, and then query for them.
    
    my ($title_word) = $r4[0]{title} =~ qr{ (\b\w\w\w\w\w+\b) }xs;
    my ($desc_word) = $r4[0]{description} =~ qr{ (\b\w\w\w\w\w+\b) }xs;
    
    diag("title word: '$title_word'");
    diag("desc word: '$desc_word'");
    
    my (@r7a) = $T->fetch_records("/eduresources/list.json?title=%$title_word%", "list title='\%$title_word\%'");
    my (@r7b) = $T->fetch_records("/eduresources/list.json?keyword=$desc_word", "list keyword='$desc_word'");
    
    # Check 'tag' parameter. Grab the first tag from the list, and then query for it.
    
    my ($tag_word) = $r4[0]{tags} =~ qr{ ^ ([^, ]+) }xs;
    
    diag("tag word: '$tag_word'");
    
    my (@r7c) = $T->fetch_records("/eduresources/list.json?tag=$tag_word", "list tag='$tag_word'");
    
    # Check 'enterer' parameter. The result will almost certainly be an empty list, but we want to
    # test that the two specified parameter values are both accepted. This will be checked more
    # thorougly by the eduresources-edit test file.
    
    my (@r8a) = $T->fetch_records("/eduresources/list.json?enterer=me", "list enterer=me",
			      { no_records_ok => 1 });
    my (@r8b) = $T->fetch_records("/eduresources/list.json?enterer=auth", "list enterer=auth",
			      { no_records_ok => 1 });
  
};


# Test the basic data fields, as much as we can. The eduresources-edit test file will be able to
# do this more thoroughly, by adding specific records and then querying for them.

subtest 'fields' => sub {

    select_subtest || return;
    
    $T->set_cookie("session_id", "");
    
    my (@r1) = $T->fetch_records("/eduresources/active.json", "list active");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my $tc = Test::Conditions->new;
    $tc->expect('description', 'url', 'is_video', 'author', 'image', 'tags');
    
    foreach my $r ( @r1 )
    {
	my $oid = $r->{oid} || '';
	
	$tc->flag('bad oid', $oid) unless $oid && $oid =~ $IDRE{EDR};
	$tc->flag('bad title', $oid) unless $r->{title};
	
	$tc->flag('description', $oid) if $r->{description};
	$tc->flag('url', $oid) if $r->{url};
	$tc->flag('is_video', $oid) if defined $r->{is_video} && $r->{is_video} ne '';
	$tc->flag('author', $oid) if $r->{author};
	$tc->flag('image', $oid) if $r->{image};
	$tc->flag('tags', $oid) if $r->{tags};
    }
    
    $tc->ok_all("active records");
};


select_final;
