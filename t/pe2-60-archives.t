# -*- mode: cperl -*-
#
# PBDB 1.2
# --------
#
# The purpose of this file is to test data entry and editing for data archives: creating an
# archive from a data request with 'archive_title', retrieving it, and adding, updating, and
# deleting archive records through archives/addupdate and archives/delete. An archive with a
# DOI may not be changed, except for its description, or deleted, except by an administrator
# with the allowance ARCHIVE_HAS_DOI.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use feature 'unicode_strings';

use Test::More tests => 10;

use lib qw(lib ../lib t);

use TableDefs qw(%TABLE);
use CoreTableDefs;

use Tester;
use PBDBTester;


# Create an instance of the Tester class, with which to make requests to the server.

my $T = Tester->new({ prefix => 'data1.2' });

# Then check to MAKE SURE that the server is in test mode and the test archive table is
# enabled. This is very important, because we DO NOT WANT to change the data in the main
# tables, or to overwrite the files of real archives. If we don't get the proper response
# back, we need to bail out. These count as the first two tests in this file.

$T->test_mode('session_data', 'enable') || BAIL_OUT("could not select test session data");

# Create an instance of PBDBTester, with which to check the contents of the test table.

my $ET = PBDBTester->new('ArchiveEdit');

# [Changed by Claude under the guidance of Michael McClennen: every table group that
# ArchiveEdit writes to is switched to the test database by select_module_tables, which
# bails out if any of its tables cannot be switched.]

$ET->select_module_tables($T, 'ArchiveEdit');

# The directory in which archive files are stored. In test mode, their names start with 'T'.

my $ARCHIVE_DIR = '/var/paleomacro/archives';


# Return the number from an identifier such as 'dar:12'.

sub id_no {
    
    my ($oid) = @_;
    
    return $oid && $oid =~ /(\d+)$/ ? $1 : 0;
}


# Establish an empty archive table in the test database, and select it locally.

subtest 'establish tables' => sub {
    
    $ET->establish_test_tables('archive_data', 'test') ||
	BAIL_OUT("could not establish test tables for 'archive_data'");
    
    $ET->start_test_mode('archive_data') ||
	BAIL_OUT("could not select test archive table locally");
    
    like( $TABLE{ARCHIVE_DATA}, qr{^pbdbTEST[.]}, "the archive table is in the test database" ) ||
	BAIL_OUT("the archive table is not in the test database");
    
    $ET->ok_record_count(0, 'ARCHIVE_DATA', '*', "the test archive table is empty");
};


# Any database member may add an archive record. The response gives the new record with its
# identifier and label.

subtest 'add' => sub {
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    my $record = { _label => 'a1', title => 'Test Archive 1', authors => 'Smith, J.',
		   description => 'First test archive' };
    
    my ($r1) = $T->send_records("/archives/addupdate.json", "member add", json => [ $record ]);
    
    ok( $r1, "an authorizer added an archive" ) || return;
    
    like( $r1->{oid}, qr{^dar:\d+$}, "it has a proper identifier" );
    is( $r1->{rlb}, 'a1', "it has its label" );
    
    foreach my $k ( qw(title authors description) )
    {
	is( $r1->{$k}, $record->{$k}, "it has the right value for '$k'" );
    }
    
    $T->set_cookie("session_id", "");
    
    my ($m2) = $T->send_data_nocheck("/archives/addupdate.json", "add without logging in",
				     json => [ { title => 'Not Allowed' } ]);
    
    $T->ok_response_code($m2, '401', "adding without logging in gets 401");
    
    $ET->ok_record_count(0, 'ARCHIVE_DATA', "title = 'Not Allowed'", "nothing was stored");
};


# The owner of an archive may update it, and somebody else may not.

subtest 'update' => sub {
    
    my ($no) = $ET->dbh->selectrow_array("
		SELECT archive_no FROM $TABLE{ARCHIVE_DATA} WHERE title = 'Test Archive 1'");
    
    ok( $no, "found the archive to update" ) || return;
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    my ($r1) = $T->send_records("/archives/addupdate.json", "update by the owner",
				json => [ { archive_id => "dar:$no", title => 'Test Archive 1a' } ]);
    
    is( $r1 && $r1->{title}, 'Test Archive 1a', "the owner changed the title" );
    is( $r1 && $r1->{authors}, 'Smith, J.', "and the authors were kept" );
    
    $T->set_cookie("session_id", "SESSION-OTHER");
    
    my ($m2) = $T->send_data_nocheck("/archives/addupdate.json", "update by somebody else",
				     json => [ { archive_id => "dar:$no", title => 'Not Mine' } ]);
    
    $T->ok_response_code($m2, '400', "an update by somebody else gets 400");
    $T->ok_error_like($m2, qr{E_PERM}, "and E_PERMISSION");
    
    $ET->ok_record_count(0, 'ARCHIVE_DATA', "title = 'Not Mine'", "the title was not changed");
};


# Only an administrator may give an archive a DOI, and only with the allowance
# ARCHIVE_HAS_DOI. Once it has one, its owner may change only its description, and may not
# delete it.

subtest 'doi' => sub {
    
    my ($no) = $ET->dbh->selectrow_array("
		SELECT archive_no FROM $TABLE{ARCHIVE_DATA} WHERE title = 'Test Archive 1a'");
    
    ok( $no, "found the archive" ) || return;
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    my ($m1) = $T->send_data_nocheck("/archives/addupdate.json", "owner sets the doi",
				     json => [ { archive_id => "dar:$no", doi => '10.1234/test.a' } ]);
    
    $T->ok_response_code($m1, '400', "the owner may not set the doi");
    $T->ok_error_like($m1, qr{E_PERMISSION_COLUMN.*doi}, "and gets E_PERMISSION_COLUMN");
    
    $T->set_cookie("session_id", "SESSION-SUPERUSER");
    
    my ($m2) = $T->send_data_nocheck("/archives/addupdate.json", "admin sets the doi",
				     json => [ { archive_id => "dar:$no", doi => '10.1234/test.a' } ]);
    
    $T->ok_response_code($m2, '400', "an administrator without the allowance gets 400");
    $T->ok_error_like($m2, qr{C_ARCHIVE_HAS_DOI}, "and the caution C_ARCHIVE_HAS_DOI");
    
    my ($r3) = $T->send_records("/archives/addupdate.json?allow=ARCHIVE_HAS_DOI",
				"admin sets the doi with the allowance",
				json => [ { archive_id => "dar:$no", doi => '10.1234/test.a' } ]);
    
    is( $r3 && $r3->{doi}, '10.1234/test.a', "the administrator set the doi" );
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    my ($r4) = $T->send_records("/archives/addupdate.json", "owner changes the description",
				json => [ { archive_id => "dar:$no", description => 'Changed' } ]);
    
    is( $r4 && $r4->{description}, 'Changed', "the owner may still change the description" );
    
    my ($m5) = $T->send_data_nocheck("/archives/addupdate.json", "owner changes the title",
				     json => [ { archive_id => "dar:$no", title => 'Changed' } ]);
    
    $T->ok_response_code($m5, '400', "the owner may not change the title");
    $T->ok_error_like($m5, qr{E_IMMUTABLE}, "and gets E_IMMUTABLE");
    
    my ($m6) = $T->fetch_nocheck("/archives/delete.json?archive_id=dar:$no", "owner deletes");
    
    $T->ok_response_code($m6, '400', "the owner may not delete it");
    $T->ok_error_like($m6, qr{E_IMMUTABLE}, "and gets E_IMMUTABLE");
    
    $T->set_cookie("session_id", "SESSION-SUPERUSER");
    
    my ($m7) = $T->fetch_nocheck("/archives/delete.json?archive_id=dar:$no",
				 "admin deletes without the allowance");
    
    $T->ok_response_code($m7, '400', "an administrator needs the allowance to delete it");
    $T->ok_error_like($m7, qr{C_ARCHIVE_HAS_DOI}, "and gets C_ARCHIVE_HAS_DOI");
    
    $ET->ok_record_count(1, 'ARCHIVE_DATA', "archive_no = $no", "the archive is still there");
    
    $T->fetch_url("/archives/delete.json?archive_id=dar:$no&allow=ARCHIVE_HAS_DOI",
		  "admin deletes with the allowance");
    
    $ET->ok_record_count(0, 'ARCHIVE_DATA', "archive_no = $no", "the archive was deleted");
};


# Archive records can be deleted several at once in the URL, in a request body, and with the
# DELETE method. Deleting one that does not exist is an error, and only the owner may delete
# an archive.

subtest 'delete' => sub {
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    my (@r) = $T->send_records("/archives/addupdate.json", "add four archives",
			       json => [ map { { _label => "d$_", title => "Delete Test $_" } } 1..4 ]);
    
    cmp_ok( scalar(@r), '==', 4, "four archives were added" ) || return;
    
    my @no = map { id_no($_->{oid}) } @r;
    
    $T->set_cookie("session_id", "SESSION-OTHER");
    
    my ($m0) = $T->fetch_nocheck("/archives/delete.json?archive_id=dar:$no[0]",
				 "delete by somebody else");
    
    $T->ok_response_code($m0, '400', "a delete by somebody else gets 400");
    $T->ok_error_like($m0, qr{E_PERM}, "and E_PERMISSION");
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    my (@d1) = $T->fetch_records("/archives/delete.json?archive_id=dar:$no[0],dar:$no[1]",
				 "delete two in the URL");
    
    cmp_ok( scalar(@d1), '==', 2, "both deletions were reported" );
    ok( ! $T->get_warnings, "with no warnings" );
    is( $d1[0] && $d1[0]{sta}, 'deleted', "with status 'deleted'" );
    
    $ET->ok_record_count(0, 'ARCHIVE_DATA', "archive_no in ($no[0],$no[1])", "both were deleted");
    
    my (@d2) = $T->send_records("/archives/delete.json", "delete with a request body",
				json => [ { _label => 'x3', archive_id => "dar:$no[2]" } ]);
    
    $ET->ok_record_count(0, 'ARCHIVE_DATA', "archive_no = $no[2]",
			 "the archive in the body was deleted");
    
    is( $d2[0] && $d2[0]{rlb}, 'x3', "it is reported with its label" );
    
    $T->server_request('delete', "/archives/delete.json?archive_id=dar:$no[3]",
		       "delete with method DELETE");
    
    $ET->ok_record_count(0, 'ARCHIVE_DATA', "archive_no = $no[3]",
			 "the archive was deleted with method DELETE");
    
    my ($m1) = $T->fetch_nocheck("/archives/delete.json?archive_id=dar:99999999",
				 "delete an archive that does not exist");
    
    $T->ok_response_code($m1, '400', "deleting an archive that does not exist gets 400");
    $T->ok_error_like($m1, qr{E_NOT_FOUND}, "and E_NOT_FOUND");
};


# A data request with 'archive_title' stores its result as an archive. In test mode, the
# archive file name starts with 'T'. The archive can then be retrieved, and its contents
# replaced only with 'archive_replace'.

my (@ARCHIVE_FILES);

subtest 'create and retrieve' => sub {
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    my $data_url = "/taxa/list.json?name=Dascillidae";
    
    my ($m1) = $T->fetch_url("$data_url&archive_title=Test%20Created%20Archive",
			     "create an archive from a data request");
    
    my $response = $m1 ? $m1->decoded_content : '';
    
    like( $response, qr{Created archive dar:\d+}, "the response names the new archive" );
    
    my ($rec) = $ET->fetch_records('ARCHIVE_DATA', "title = 'Test Created Archive'");
    
    ok( $rec, "the archive record was stored" ) || return;
    
    my $no = $rec->{archive_no};
    
    like( $response, qr{dar:$no\b}, "with its number" );
    like( $rec->{uri_path}, qr{taxa/list}, "it has the path of the request" );
    
    ($rec) = $ET->fetch_records('ARCHIVE_DATA', "archive_no = $no");
    
    is( $rec && $rec->{status}, 'complete', "its status is 'complete'" );
    unlike( $rec->{uri_args}, qr{archive_title}, "its arguments leave out 'archive_title'" );
    
    my $file = "$ARCHIVE_DIR/T$no.gz";
    
    push @ARCHIVE_FILES, $file;
    
    ok( -e $file, "the archive file was written with a name starting with 'T'" );
    ok( ! -e "$ARCHIVE_DIR/$no.gz" || (stat "$ARCHIVE_DIR/$no.gz")[9] < time - 60,
	"no file was written under the real archive name" );
    
    my ($m2) = $T->fetch_url("/archives/retrieve.json?id=dar:$no", "retrieve the archive",
			     { no_check => 1 });
    
    $T->ok_response_code($m2, '200', "the archive was retrieved");
    
    my $content = $m2 ? $m2->decoded_content : '';
    
    like( $content, qr{"records"}, "the archive contains the records" );
    
    # The same title again is refused unless 'archive_replace' is given.
    
    my ($m3) = $T->fetch_nocheck("$data_url&archive_title=Test%20Created%20Archive",
				 "create an archive with the same title");
    
    $T->ok_response_code($m3, '400', "the same title again gets 400");
    
    $T->fetch_url("$data_url&archive_title=Test%20Created%20Archive&archive_replace=yes",
		  "replace the archive");
    
    $ET->ok_record_count(1, 'ARCHIVE_DATA', "title = 'Test Created Archive'",
			 "there is still one archive with that title");
};


# The records entered here can be retrieved by the data service.

subtest 'retrieval' => sub {
    
    my ($no) = $ET->dbh->selectrow_array("
		SELECT archive_no FROM $TABLE{ARCHIVE_DATA} WHERE title = 'Test Created Archive'");
    
    ok( $no, "found a test archive" ) || return;
    
    my ($r) = $T->fetch_records("/archives/single.json?id=dar:$no", "fetch the archive record");
    
    is( $r && $r->{title}, 'Test Created Archive', "it has the right title" );
};


# Remove the test archives and their files.

subtest 'cleanup' => sub {
    
    my $files = $ET->dbh->selectcol_arrayref("SELECT archive_no FROM $TABLE{ARCHIVE_DATA}");
    
    push @ARCHIVE_FILES, map { ("$ARCHIVE_DIR/T$_.gz", "$ARCHIVE_DIR/T$_.header") } @$files;
    
    foreach my $file ( @ARCHIVE_FILES )
    {
	unlink $file if $file =~ m{/T\d+[.]\w+$} && -e $file;
    }
    
    ok( ! (grep { -e $_ } @ARCHIVE_FILES), "the test archive files were removed" );
    
    $ET->clear_table('ARCHIVE_DATA');
    
    $ET->ok_record_count(0, 'ARCHIVE_DATA', '*', "the test archive table is empty");
};


# Switch the test tables back to the real ones when this file is done, so that later
# test files are not affected.

END {
    $ET->release_module_tables($T) if $ET && $T;
}
