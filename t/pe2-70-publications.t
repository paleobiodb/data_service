# -*- mode: cperl -*-
#
# PBDB 1.2
# --------
#
# The purpose of this file is to test data entry and editing for publications, through the
# operations pubs/addupdate and pubs/delete. Only a superuser, or a user with administrative
# permission on the table, may add, modify, or delete publication records.
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

# Then check to MAKE SURE that the server is in test mode and the test publication table is
# enabled. This is very important, because we DO NOT WANT to change the data in the main
# tables. If we don't get the proper response back, we need to bail out. These count as the
# first two tests in this file.

$T->test_mode('session_data', 'enable') || BAIL_OUT("could not select test session data");

# Create an instance of PBDBTester, with which to check the contents of the test table.

my $ET = PBDBTester->new('PBDBEdit');

# [Changed by Claude under the guidance of Michael McClennen: every table group that
# PBDBEdit writes to is switched to the test database by select_module_tables, which
# bails out if any of its tables cannot be switched.]

$ET->select_module_tables($T, 'PBDBEdit');


# Return the number from an identifier such as 'pub:12'.

sub id_no {
    
    my ($oid) = @_;
    
    return $oid && $oid =~ /(\d+)$/ ? $1 : 0;
}


# Establish an empty publication table in the test database, and select it locally.

subtest 'establish tables' => sub {
    
    $ET->establish_test_tables('publication_data', 'test') ||
	BAIL_OUT("could not establish test tables for 'publication_data'");
    
    $ET->start_test_mode('publication_data') ||
	BAIL_OUT("could not select test publication table locally");
    
    like( $TABLE{PUBLICATION_DATA}, qr{^pbdbTEST[.]}, "the publication table is in the test database" ) ||
	BAIL_OUT("the publication table is not in the test database");
    
    $ET->ok_record_count(0, 'PUBLICATION_DATA', '*', "the test publication table is empty");
};


# A superuser, and a user with administrative permission on the table, may add publications.
# The response gives each new record with its identifier and the values that were stored.

subtest 'add by admin' => sub {
    
    $T->set_cookie("session_id", "SESSION-SUPERUSER");
    
    my $record = { _label => 'p1', title => 'Test Publication 1', authors => 'Smith, J.',
		   year => 2020, journal => 'Journal of Tests', volume => '12',
		   first_page => '100', last_page => '110', doi => '10.1234/test.1' };
    
    my ($r1) = $T->send_records("/pubs/addupdate.json", "superuser add", json => [ $record ]);
    
    ok( $r1, "the superuser added a publication" ) || return;
    
    like( $r1->{oid}, qr{^pub:\d+$}, "it has a proper identifier" );
    
    foreach my $k ( qw(title authors year journal volume first_page last_page doi) )
    {
	is( $r1->{$k}, $record->{$k}, "it has the right value for '$k'" );
    }
    
    is( $r1->{rlb}, 'p1', "it has its label" );
    
    $ET->ok_record_count(1, 'PUBLICATION_DATA', "pub_no = " . id_no($r1->{oid}),
			 "it is in the table");
    
    $T->set_cookie("session_id", "SESSION-WITH-ADMIN");
    
    my ($r2) = $T->send_records("/pubs/addupdate.json", "table admin add",
				json => [ { title => 'Test Publication 2', year => 2021 } ]);
    
    ok( $r2 && $r2->{oid}, "a user with administrative permission added a publication" );
};


# Nobody else may add or modify publications, and nothing may be done without logging in.

subtest 'add by others' => sub {
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    my ($m1) = $T->send_data_nocheck("/pubs/addupdate.json", "authorizer add",
				     json => [ { title => 'Not Allowed 1' } ]);
    
    $T->ok_response_code($m1, '400', "an authorizer without permission gets 400");
    $T->ok_error_like($m1, qr{E_PERM}, "and E_PERMISSION");
    
    my ($pub_no) = $ET->dbh->selectrow_array("SELECT min(pub_no) FROM $TABLE{PUBLICATION_DATA}");
    
    my ($m2) = $T->send_data_nocheck("/pubs/addupdate.json", "authorizer update",
				     json => [ { pub_id => "pub:$pub_no", title => 'Not Allowed 2' } ]);
    
    $T->ok_response_code($m2, '400', "an update by an authorizer gets 400");
    $T->ok_error_like($m2, qr{E_PERM}, "and E_PERMISSION");
    
    $T->set_cookie("session_id", "");
    
    my ($m3) = $T->send_data_nocheck("/pubs/addupdate.json", "add without logging in",
				     json => [ { title => 'Not Allowed 3' } ]);
    
    $T->ok_response_code($m3, '401', "adding without logging in gets 401");
    
    $ET->ok_record_count(0, 'PUBLICATION_DATA', "title like 'Not Allowed%'", "nothing was stored");
};


# A record with an identifier updates that publication, leaving its other fields alone. An
# identifier that does not exist is an error.

subtest 'update' => sub {
    
    $T->set_cookie("session_id", "SESSION-SUPERUSER");
    
    my ($pub_no) = $ET->dbh->selectrow_array("
		SELECT pub_no FROM $TABLE{PUBLICATION_DATA} WHERE title = 'Test Publication 1'");
    
    ok( $pub_no, "found the publication to update" ) || return;
    
    my ($r1) = $T->send_records("/pubs/addupdate.json", "update a publication",
				json => [ { pub_id => "pub:$pub_no", title => 'Test Publication 1a' } ]);
    
    is( $r1 && $r1->{oid}, "pub:$pub_no", "the same publication was returned" );
    is( $r1 && $r1->{title}, 'Test Publication 1a', "its title was changed" );
    is( $r1 && $r1->{journal}, 'Journal of Tests', "and its journal was kept" );
    
    my ($m2) = $T->send_data_nocheck("/pubs/addupdate.json", "update a nonexistent publication",
				     json => [ { pub_id => 'pub:99999999', title => 'Nonexistent' } ]);
    
    $T->ok_response_code($m2, '400', "updating a publication that does not exist gets 400");
    
    $ET->ok_record_count(0, 'PUBLICATION_DATA', "title = 'Nonexistent'", "nothing was stored");
};


# Bad values are reported, and nothing is stored. With PROCEED, the good records are stored
# and the errors become warnings.

subtest 'errors' => sub {
    
    $T->set_cookie("session_id", "SESSION-SUPERUSER");
    
    my $records = [ { _label => 'e1', title => 'Error 1', year => 'abcd' },
		    { _label => 'e2', title => 'Error 2', volume => 'x' x 20 },
		    { _label => 'e3', title => 'Error 3', nosuchfield => 'x' },
		    { _label => 'e4', title => 'Error 4 (good)' } ];
    
    my ($m1) = $T->send_data_nocheck("/pubs/addupdate.json", "add with errors", json => $records);
    
    $T->ok_response_code($m1, '400', "a request with bad values gets 400");
    $T->ok_error_like($m1, qr{\(e1\).*year}, "the bad year is reported");
    $T->ok_error_like($m1, qr{E_WIDTH \(e2\).*volume}, "the long volume is reported");
    $T->ok_error_like($m1, qr{\(e3\).*nosuchfield}, "the unknown field is reported");
    
    $ET->ok_record_count(0, 'PUBLICATION_DATA', "title like 'Error%'", "nothing was stored");
    
    my ($m2) = $T->send_data("/pubs/addupdate.json?allow=PROCEED", "add with errors and PROCEED",
			     json => $records);
    
    ok( ! $T->get_errors($m2), "with PROCEED there are no errors" );
    $T->ok_warning_like($m2, qr{\(e1\).*year}, "the bad year is a warning");
    
    $ET->ok_record_count(1, 'PUBLICATION_DATA', "title like 'Error%'", "only the good record was stored");
};


# Publications can be deleted several at once in the URL, in a request body, and with the
# DELETE method. Deleting one that does not exist is an error, and only an administrator
# may delete.

subtest 'delete' => sub {
    
    $T->set_cookie("session_id", "SESSION-SUPERUSER");
    
    my (@r) = $T->send_records("/pubs/addupdate.json", "add four publications",
			       json => [ map { { _label => "d$_", title => "Delete Test $_" } } 1..4 ]);
    
    cmp_ok( scalar(@r), '==', 4, "four publications were added" ) || return;
    
    my @no = map { id_no($_->{oid}) } @r;
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    my ($m0) = $T->fetch_nocheck("/pubs/delete.json?pub_id=pub:$no[0]", "delete by an authorizer");
    
    $T->ok_response_code($m0, '400', "a delete by an authorizer gets 400");
    
    $ET->ok_record_count(1, 'PUBLICATION_DATA', "pub_no = $no[0]", "the publication was not deleted");
    
    $T->set_cookie("session_id", "SESSION-SUPERUSER");
    
    my (@d1) = $T->fetch_records("/pubs/delete.json?pub_id=pub:$no[0],pub:$no[1]",
				 "delete two in the URL");
    
    cmp_ok( scalar(@d1), '==', 2, "both deletions were reported" );
    is( $d1[0] && $d1[0]{sta}, 'deleted', "with status 'deleted'" );
    
    $ET->ok_record_count(0, 'PUBLICATION_DATA', "pub_no in ($no[0],$no[1])", "both were deleted");
    
    my (@d2) = $T->send_records("/pubs/delete.json", "delete with a request body",
				json => [ { _label => 'x3', pub_id => "pub:$no[2]" } ]);
    
    $ET->ok_record_count(0, 'PUBLICATION_DATA', "pub_no = $no[2]",
			 "the publication in the body was deleted");
    
    is( $d2[0] && $d2[0]{rlb}, 'x3', "it is reported with its label" );
    
    $T->server_request('delete', "/pubs/delete.json?pub_id=pub:$no[3]", "delete with method DELETE");
    
    $ET->ok_record_count(0, 'PUBLICATION_DATA', "pub_no = $no[3]",
			 "the publication was deleted with method DELETE");
    
    # After a delete, the next publication gets the number after the highest remaining one.
    
    my ($max) = $ET->dbh->selectrow_array("SELECT max(pub_no) FROM $TABLE{PUBLICATION_DATA}");
    
    my ($n) = $T->send_records("/pubs/addupdate.json", "add after deleting",
			       json => [ { title => 'After Delete' } ]);
    
    is( id_no($n && $n->{oid}), $max + 1, "the numbers stay consecutive" );
    
    my ($m1) = $T->fetch_nocheck("/pubs/delete.json?pub_id=pub:99999999",
				 "delete a publication that does not exist");
    
    $T->ok_response_code($m1, '400', "deleting a publication that does not exist gets 400");
    $T->ok_error_like($m1, qr{E_NOT_FOUND}, "and E_NOT_FOUND");
};


# The records entered here can be retrieved by the data service.

subtest 'retrieval' => sub {
    
    my ($pub_no) = $ET->dbh->selectrow_array("
		SELECT pub_no FROM $TABLE{PUBLICATION_DATA} WHERE title = 'Test Publication 1a'");
    
    ok( $pub_no, "found a test publication" ) || return;
    
    my ($r) = $T->fetch_records("/pubs/single.json?id=pub:$pub_no", "fetch the publication");
    
    is( $r && $r->{title}, 'Test Publication 1a', "it has the right title" );
    
    my (@l) = $T->fetch_records("/pubs/list.json?all_records", "list all publications");
    
    my $count = $ET->count_records('PUBLICATION_DATA');
    
    cmp_ok( scalar(@l), '==', $count, "the list has every test publication" );
};


# Remove the test publications.

subtest 'cleanup' => sub {
    
    $ET->clear_table('PUBLICATION_DATA');
    
    $ET->ok_record_count(0, 'PUBLICATION_DATA', '*', "the test publication table is empty");
};


# Switch the test tables back to the real ones when this file is done, so that later
# test files are not affected.

END {
    $ET->release_module_tables($T) if $ET && $T;
}
