# -*- mode: cperl -*-
#
# PBDB 1.2
# --------
#
# The purpose of this file is to test data entry and editing for bibliographic references,
# through the operations refs/addupdate and refs/delete. Possible duplicates are looked for
# among the real references, which are read but not changed.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use feature 'unicode_strings';

use Test::More tests => 11;

use lib qw(lib ../lib t);

use TableDefs qw(%TABLE);
use CoreTableDefs;

use Tester;
use PBDBTester;


# Create an instance of the Tester class, with which to make requests to the server.

my $T = Tester->new({ prefix => 'data1.2' });

# Then check to MAKE SURE that the server is in test mode and the test reference tables are
# enabled. This is very important, because we DO NOT WANT to change the data in the main
# tables. If we don't get the proper response back, we need to bail out. These count as the
# first two tests in this file.

$T->test_mode('session_data', 'enable') || BAIL_OUT("could not select test session data");

# Create an instance of PBDBTester, with which to check the contents of the test tables.

my $ET = PBDBTester->new('ReferenceEdit');

# [Changed by Claude under the guidance of Michael McClennen: every table group that
# ReferenceEdit writes to is switched to the test database by select_module_tables, which
# bails out if any of its tables cannot be switched.]

$ET->select_module_tables($T, 'ReferenceEdit');


# The fields of a valid journal article, apart from the authors. The title is one that no
# real reference has, so that it is not taken for a duplicate.

my %ARTICLE = ( publication_type => 'journal article', pubyr => 2001,
		pubtitle => 'Journal of Testing', reftitle => 'A test study of fossil clams' );


# Return the number from an identifier such as 'ref:12'.

sub id_no {
    
    my ($oid) = @_;
    
    return $oid && $oid =~ /(\d+)$/ ? $1 : 0;
}


# Return the authors or editors of the specified reference, as a string.

sub names {
    
    my ($table, $reference_no) = @_;
    
    my $rows = $ET->dbh->selectall_arrayref("SELECT firstname, lastname FROM $TABLE{$table}
		WHERE reference_no = $reference_no ORDER BY place");
    
    return join('; ', map { "$_->[1], $_->[0]" } @$rows);
}


# Establish empty reference tables in the test database, and select them locally.

subtest 'establish tables' => sub {
    
    $ET->establish_test_tables('reference_data', 'test') ||
	BAIL_OUT("could not establish test tables for 'reference_data'");
    
    $ET->start_test_mode('reference_data') ||
	BAIL_OUT("could not select test reference tables locally");
    
    like( $TABLE{REFERENCE_DATA}, qr{^pbdbTEST[.]}, "the reference table is in the test database" ) ||
	BAIL_OUT("the reference table is not in the test database");
    
    $ET->ok_record_count(0, 'REFERENCE_DATA', '*', "the test reference table is empty");
    
    # A reference may not be deleted if other records use it, and those records are in
    # tables that this file does not switch to the test database. So the test references
    # are numbered above every real one, which no real record can use.
    
    my ($max) = $ET->dbh->selectrow_array("
		SELECT max(reference_no) FROM $TABLE{REFERENCE_DUPLICATE_SEARCH}");
    
    $ET->dbh->do("ALTER TABLE $TABLE{REFERENCE_DATA} AUTO_INCREMENT = " . ($max + 1000));
    
    pass("test references are numbered above " . ($max + 1000));
};


# A database member may add a reference. The authors and editors are stored in their own
# tables, and the response gives the new record with its identifier and label.

subtest 'add' => sub {
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    my ($r1) = $T->send_records("/refs/addupdate.json", "enterer add",
				json => [ { _label => 'r1', %ARTICLE, pages => '12-15',
					    authors => 'Smith, John A.; Jones, B.',
					    editors => 'Doe, Jane' } ]);
    
    ok( $r1, "an enterer added a reference" ) || return;
    
    like( $r1->{oid}, qr{^ref:\d+$}, "it has a proper identifier" );
    is( $r1->{rlb}, 'r1', "it has its label" );
    is( $r1->{tit}, $ARTICLE{reftitle}, "it has the right title" );
    is( $r1->{pby}, $ARTICLE{pubyr}, "and the right year" );
    
    my $no = id_no($r1->{oid});
    
    is( names('REFERENCE_AUTHORS', $no), 'Smith, John A.; Jones, B.', "the authors were stored" );
    is( names('REFERENCE_EDITORS', $no), 'Doe, Jane', "and the editors" );
    
    $T->set_cookie("session_id", "SESSION-GUEST");
    
    my ($m2) = $T->send_data_nocheck("/refs/addupdate.json", "guest add",
				     json => [ { %ARTICLE, reftitle => 'Reference by a guest',
						 authors => 'Smith, J.' } ]);
    
    isnt( $T->get_response_code($m2), '200', "a guest may not add a reference" );
    
    $T->set_cookie("session_id", "");
    
    my ($m3) = $T->send_data_nocheck("/refs/addupdate.json", "add without logging in",
				     json => [ { %ARTICLE, reftitle => 'Reference by nobody',
						 authors => 'Smith, J.' } ]);
    
    $T->ok_response_code($m3, '401', "adding without logging in gets 401");
    
    $ET->ok_record_count(0, 'REFERENCE_DATA', "reftitle like 'Reference by %'",
			 "neither was stored");
};


# A reference must have a title, a publication type, a year, and authors.

subtest 'required fields' => sub {
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    my ($m1) = $T->send_data_nocheck("/refs/addupdate.json", "add without authors",
				     json => [ { %ARTICLE, reftitle => 'No authors here' } ]);
    
    $T->ok_response_code($m1, '400', "a reference without authors gets 400");
    $T->ok_error_like($m1, qr{E_REQUIRED}, "and E_REQUIRED");
    
    my %no_title = %ARTICLE;
    delete $no_title{reftitle};
    
    my ($m2) = $T->send_data_nocheck("/refs/addupdate.json", "add without a title",
				     json => [ { %no_title, authors => 'Smith, J.' } ]);
    
    $T->ok_response_code($m2, '400', "a reference without a title gets 400");
    $T->ok_error_like($m2, qr{E_REQUIRED.*reftitle}, "and E_REQUIRED for 'reftitle'");
    
    $ET->ok_record_count(0, 'REFERENCE_DATA', "reftitle = 'No authors here'", "nothing was stored");
};


# A record with an identifier updates that reference. The identifier may also be given in
# the URL. Updating the authors replaces them, and updating other fields leaves them alone.

subtest 'update' => sub {
    
    my ($no) = $ET->dbh->selectrow_array("
		SELECT reference_no FROM $TABLE{REFERENCE_DATA} WHERE reftitle = '$ARTICLE{reftitle}'");
    
    ok( $no, "found the reference to update" ) || return;
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    my ($r1) = $T->send_records("/refs/addupdate.json", "update the pages",
				json => [ { reference_id => "ref:$no", pages => '12-16' } ]);
    
    is( $r1 && $r1->{oid}, "ref:$no", "the same reference was returned" );
    is( names('REFERENCE_AUTHORS', $no), 'Smith, John A.; Jones, B.', "the authors were kept" );
    
    my ($r2) = $T->send_records("/refs/addupdate.json?ref_id=ref:$no", "update with the id in the URL",
				json => [ { authors => 'Taylor, P.' } ]);
    
    is( $r2 && $r2->{oid}, "ref:$no", "the reference named in the URL was updated" );
    is( names('REFERENCE_AUTHORS', $no), 'Taylor, P.', "and its authors were replaced" );
    
    $ET->ok_record_count(1, 'REFERENCE_DATA', '*', "no new reference was added");
    
    my ($m3) = $T->send_data_nocheck("/refs/addupdate.json", "update a nonexistent reference",
				     json => [ { reference_id => 'ref:99999999', pages => '1' } ]);
    
    $T->ok_response_code($m3, '400', "updating a reference that does not exist gets 400");
};


# A title or author name with bad capitalization gets the caution C_CAPITAL, which the
# allowance CAPITAL lets through.

subtest 'capitalization' => sub {
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    my $record = { %ARTICLE, reftitle => 'a lowercase test title', authors => 'smith, j.' };
    
    my ($m1) = $T->send_data_nocheck("/refs/addupdate.json", "add with bad capitalization",
				     json => [ $record ]);
    
    $T->ok_response_code($m1, '400', "bad capitalization gets 400");
    $T->ok_error_like($m1, qr{C_CAPITAL}, "and C_CAPITAL");
    
    $T->send_records("/refs/addupdate.json?allow=CAPITAL", "add with CAPITAL", json => [ $record ]);
    
    $ET->ok_record_count(1, 'REFERENCE_DATA', "reftitle = 'a lowercase test title'",
			 "the reference was stored with the allowance");
};


# A reference that matches a real one gets the caution C_DUPLICATE, which the allowance
# DUPLICATE lets through.

subtest 'duplicates' => sub {
    
    my ($real) = $ET->dbh->selectrow_hashref("
		SELECT reference_no, reftitle, pubyr, pubtitle, publication_type, pubvol, firstpage,
		       lastpage, author1last, author1init
		FROM $TABLE{REFERENCE_DUPLICATE_SEARCH}
		WHERE publication_type = 'journal article' and reftitle <> '' and pubyr > 0
		  and author1last <> '' and author1init <> '' and otherauthors = ''
		  and author2last = '' and pubtitle <> ''
		ORDER BY reference_no LIMIT 1");
    
    ok( $real, "found a real reference to copy" ) || return;
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    my $copy = { publication_type => $real->{publication_type}, pubyr => $real->{pubyr},
		 reftitle => $real->{reftitle}, pubtitle => $real->{pubtitle},
		 pubvol => $real->{pubvol}, pages => "$real->{firstpage}-$real->{lastpage}",
		 authors => "$real->{author1last}, $real->{author1init}" };
    
    my ($m1) = $T->send_data_nocheck("/refs/addupdate.json?allow=CAPITAL", "add a duplicate",
				     json => [ $copy ]);
    
    $T->ok_response_code($m1, '400', "a duplicate of a real reference gets 400");
    $T->ok_error_like($m1, qr{C_DUPLICATE}, "and C_DUPLICATE");
    
    my ($r2) = $T->send_records("/refs/addupdate.json?allow=CAPITAL,DUPLICATE",
				"add a duplicate with DUPLICATE", json => [ $copy ]);
    
    ok( $r2 && $r2->{oid}, "the duplicate was stored with the allowance" );
};


# References can be deleted several at once in the URL, in a request body, and with the
# DELETE method. Only the owner may delete a reference, and deleting one that does not exist
# is an error.

subtest 'delete' => sub {
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    my (@r) = $T->send_records("/refs/addupdate.json", "add four references",
			       json => [ map { { _label => "d$_", %ARTICLE,
						 reftitle => "Delete test number $_",
						 authors => 'Smith, J.' } } 1..4 ]);
    
    cmp_ok( scalar(@r), '==', 4, "four references were added" ) || return;
    
    my @no = map { id_no($_->{oid}) } @r;
    
    $T->set_cookie("session_id", "SESSION-OTHER");
    
    my ($m0) = $T->fetch_nocheck("/refs/delete.json?ref_id=ref:$no[0]", "delete by somebody else");
    
    $T->ok_response_code($m0, '400', "a delete by somebody else gets 400");
    $T->ok_error_like($m0, qr{E_PERM}, "and E_PERMISSION");
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    my (@d1) = $T->fetch_records("/refs/delete.json?ref_id=ref:$no[0],ref:$no[1]",
				 "delete two in the URL");
    
    cmp_ok( scalar(@d1), '==', 2, "both deletions were reported" );
    
    $ET->ok_record_count(0, 'REFERENCE_DATA', "reference_no in ($no[0],$no[1])", "both were deleted");
    $ET->ok_record_count(0, 'REFERENCE_AUTHORS', "reference_no in ($no[0],$no[1])",
			 "and their authors");
    
    my (@d2) = $T->send_records("/refs/delete.json", "delete with a request body",
				json => [ { _label => 'x3', ref_id => "ref:$no[2]" } ]);
    
    $ET->ok_record_count(0, 'REFERENCE_DATA', "reference_no = $no[2]",
			 "the reference in the body was deleted");
    
    is( $d2[0] && $d2[0]{rlb}, 'x3', "it is reported with its label" );
    
    $T->server_request('delete', "/refs/delete.json?ref_id=ref:$no[3]", "delete with method DELETE");
    
    $ET->ok_record_count(0, 'REFERENCE_DATA', "reference_no = $no[3]",
			 "the reference was deleted with method DELETE");
    
    my ($m1) = $T->fetch_nocheck("/refs/delete.json?ref_id=ref:99999999",
				 "delete a reference that does not exist");
    
    $T->ok_response_code($m1, '400', "deleting a reference that does not exist gets 400");
    $T->ok_error_like($m1, qr{E_NOT_FOUND}, "and E_NOT_FOUND");
};


# The records entered here can be retrieved by the data service.

subtest 'retrieval' => sub {
    
    my ($no) = $ET->dbh->selectrow_array("
		SELECT reference_no FROM $TABLE{REFERENCE_DATA} WHERE reftitle = '$ARTICLE{reftitle}'");
    
    ok( $no, "found a test reference" ) || return;
    
    my ($r) = $T->fetch_records("/refs/single.json?id=ref:$no", "fetch the reference");
    
    is( $r && $r->{tit}, $ARTICLE{reftitle}, "it has the right title" );
    is( $r && $r->{al1}, 'Taylor', "and the updated first author" );
};


# Remove the test references.

subtest 'cleanup' => sub {
    
    $ET->clear_table($_) foreach qw(REFERENCE_DATA REFERENCE_AUTHORS REFERENCE_EDITORS);
    
    $ET->ok_record_count(0, 'REFERENCE_DATA', '*', "the test reference table is empty");
};


# Switch the test tables back to the real ones when this file is done, so that later
# test files are not affected.

END {
    $ET->release_module_tables($T) if $ET && $T;
}
