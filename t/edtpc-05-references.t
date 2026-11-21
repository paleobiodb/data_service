#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the EditTransaction client modules, the subclasses of
# EditTransaction that handle edits to particular kinds of Paleobiology Database records.
#
# edtpc-05-references.t : Test ReferenceEdit, which handles bibliographic references:
#                         author and editor names, page numbers, capitalization checks,
#                         required fields, duplicate detection, deletion of references that
#                         are in use, permissions, and logging of the author and editor tables.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 10;

use File::Temp qw(tempdir);

use TableDefs qw(%TABLE original_table_name);

use EDTPCTest;
use PBDBTester;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_has_one_condition ok_has_condition
		  clear_table sql_command sql_fetchrow ok_found_record ok_no_record);


# Establish the tester and select the test tables.

my $T = PBDBTester->new('ReferenceEdit');

EDTPCTest->select_test_tables('ReferenceEdit');

sub perm {
    
    my ($name) = @_;
    
    return $T->new_perm("SESSION-$name", 'REFERENCE_DATA');
}


# The fields of a valid journal article, apart from the authors.

my %ARTICLE = ( publication_type => 'journal article', pubyr => 2001,
		reftitle => 'A study of fossil clams', pubtitle => 'Journal of Testing' );


# Return the authors or editors of the specified reference, as a string.

sub names {
    
    my ($table, $reference_no) = @_;
    
    my $rows = $T->dbh->selectall_arrayref("SELECT firstname, lastname FROM $TABLE{$table}
		WHERE reference_no = $reference_no ORDER BY place");
    
    return join('; ', map { "$_->[1], $_->[0]" } @$rows);
}


# Start with empty tables.

subtest 'setup' => sub {
    
    clear_table($_) foreach EDTPCTest->module_tables('ReferenceEdit');
    
    ok_no_record('REFERENCE_DATA');
};


# Insert a reference with several authors. The first two go into separate columns, with
# initials generated from the first names. The rest go into 'otherauthors'. A name with an
# asterisk is a last name only. Every author goes into REFERENCE_AUTHORS.

subtest 'insert with authors' => sub {
    
    my $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('REFERENCE_DATA', { %ARTICLE, pages => '12-15',
	authors => 'Smith, John A.; Jones, B.; Brown, C. D.; Black*' });
    
    ok_commit("the reference was inserted");
    
    my ($key) = $edt->inserted_keys;
    
    ok( $key, "the new key was returned" ) || return;
    
    my ($a1init, $a1last, $a2init, $a2last, $other, $first, $last, $enterer) =
	sql_fetchrow("SELECT author1init, author1last, author2init, author2last, otherauthors,
			firstpage, lastpage, enterer_no
		FROM <<REFERENCE_DATA>> WHERE reference_no = $key");
    
    is( "$a1init $a1last | $a2init $a2last", "J. A. Smith | B. Jones",
	"the first two authors were stored in their own columns" );
    
    is( $other, "C. D. Brown, Black", "the other authors were stored in 'otherauthors'" );
    
    is( "$first | $last", "12 | 15", "the page numbers were split" );
    
    is( $enterer, 39997, "the enterer was filled in" );
    
    is( names('REFERENCE_AUTHORS', $key), "Smith, John A.; Jones, B.; Brown, C. D.; Black, ",
	"every author was stored in REFERENCE_AUTHORS" );
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('REFERENCE_DATA', { %ARTICLE, reftitle => 'Another paper on clams',
	pages => '27', authors => [ 'Smith, J.' ], editors => 'Doe, Jane; Roe, R.' });
    
    ok_commit("a reference with an author list and editors was inserted");
    
    ($key) = $edt->inserted_keys;
    
    my ($editors, $firstpage, $lastpage) = sql_fetchrow("SELECT editors, firstpage, lastpage
		FROM <<REFERENCE_DATA>> WHERE reference_no = $key");
    
    is( $editors, "J. Doe, R. Roe", "the editors were stored with their initials" );
    
    is( names('REFERENCE_EDITORS', $key), "Doe, Jane; Roe, R.",
	"the editors were stored in REFERENCE_EDITORS" );
    
    is( names('REFERENCE_AUTHORS', $key), "Smith, J.", "an author list was accepted" );
    
    ok( $firstpage eq '27' && ! defined $lastpage, "a single page was stored as the first page" );
    
    # Page ranges written with spaces around the dash should be split the same way.
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('REFERENCE_DATA', { %ARTICLE, reftitle => 'Trilobites of the Ordovician',
	pages => '12 - 15', authors => 'Taylor, P.' });
    
    ok_commit("a reference with spaces in the page range was inserted");
    
    ($key) = $edt->inserted_keys;
    
    ($firstpage, $lastpage) = sql_fetchrow("SELECT firstpage, lastpage
		FROM <<REFERENCE_DATA>> WHERE reference_no = $key");
    
    is( "$firstpage|$lastpage", "12|15", "the page range was split without spaces" );
};


# Check the required fields and the formats of the author and editor names.

subtest 'required fields and names' => sub {
    
    my $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('REFERENCE_DATA', { %ARTICLE, reftitle => 'No authors here' });
    
    ok_failed_commit("a reference without authors was rejected");
    
    ok_has_one_condition('E_REQUIRED', "the missing authors were reported");
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('REFERENCE_DATA', { %ARTICLE, reftitle => 'No type',
					    publication_type => '', authors => 'Smith, J.' });
    
    ok_failed_commit("a reference without a publication type was rejected");
    
    ok_has_one_condition('E_REQUIRED', "the missing publication type was reported");
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('REFERENCE_DATA', { %ARTICLE, reftitle => 'Bad authors',
					    authors => { last => 'Smith' } });
    
    ok_failed_commit("an author list that is a hash was rejected");
    
    ok_has_one_condition('E_FORMAT', "the bad author list was reported");
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('REFERENCE_DATA', { %ARTICLE, reftitle => 'Bad editors',
					    authors => 'Smith, J.', editors => { last => 'Doe' } });
    
    ok_failed_commit("an editor list that is a hash was rejected");
    
    ok_has_one_condition('E_FORMAT', "the bad editor list was reported");
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('REFERENCE_DATA', { %ARTICLE, reftitle => 'Empty name',
					    authors => 'Smith, J.; X' });
    
    ok_failed_commit("an author whose last name has only one letter was rejected");
    
    ok_has_one_condition('E_NAME_EMPTY', "the empty name was reported");
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('REFERENCE_DATA', { %ARTICLE, reftitle => 'Long name',
					    authors => 'Smith, J.; ' . ('Z' x 100) });
    
    ok_failed_commit("an author name that is too long was rejected");
    
    ok_has_one_condition('E_NAME_WIDTH', "the long name was reported");
    
    ok_no_record('REFERENCE_DATA', "reftitle in ('No authors here', 'No type', 'Bad authors',
					'Bad editors', 'Empty name', 'Long name')");
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('REFERENCE_DATA', { publication_type => 'book', pubyr => 2001,
					    authors => 'Smith, J.' });
    
    ok_failed_commit("a reference without a title was rejected");
    
    ok_has_one_condition('E_REQUIRED', "the missing title was reported");
};


# Titles and names must be capitalized, unless the CAPITAL allowance is given.

subtest 'capitalization' => sub {
    
    my $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('REFERENCE_DATA', { %ARTICLE, reftitle => 'a lowercase title',
					    authors => 'smith, j.' });
    
    ok_failed_commit("a reference with bad capitalization was rejected");
    
    ok_has_condition('C_CAPITAL', "bad capitalization was reported" );
    
    is( scalar($edt->conditions), 2, "both the title and the author name were reported" );
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'), 'CAPITAL');
    
    $edt->insert_record('REFERENCE_DATA', { %ARTICLE, reftitle => 'a lowercase title',
					    authors => 'smith, j.' });
    
    ok_commit("the reference was accepted with the CAPITAL allowance");
    
    ok_found_record('REFERENCE_DATA', "reftitle = 'a lowercase title'");
};


# Updating the authors replaces the rows in REFERENCE_AUTHORS. Updating other fields leaves
# them alone. Any authorized database member may update a reference.

subtest 'update' => sub {
    
    my ($key) = sql_fetchrow("SELECT reference_no FROM <<REFERENCE_DATA>>
		WHERE reftitle = 'A study of fossil clams'");
    
    my $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->update_record('REFERENCE_DATA', { reference_id => "ref:$key",
					    authors => 'Green, G.; White, W.' });
    
    ok_commit("the authors were updated");
    
    is( names('REFERENCE_AUTHORS', $key), "Green, G.; White, W.",
	"the author rows were replaced" );
    
    my ($a1last, $a2last, $other) = sql_fetchrow("SELECT author1last, author2last, otherauthors
		FROM <<REFERENCE_DATA>> WHERE reference_no = $key");
    
    is( "$a1last | $a2last | $other", "Green | White | ", "the author columns were updated" );
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('OTHER'));
    
    $edt->update_record('REFERENCE_DATA', { reference_no => $key, comments => 'by other' });
    
    ok_commit("another authorized member updated the reference");
    
    is( names('REFERENCE_AUTHORS', $key), "Green, G.; White, W.",
	"the author rows were not changed" );
    
    ok_found_record('REFERENCE_DATA', "reference_no = $key and comments = 'by other'");
};


# A replace stores the new record and the new authors.

subtest 'replace' => sub {
    
    my ($key) = sql_fetchrow("SELECT reference_no FROM <<REFERENCE_DATA>>
		WHERE reftitle = 'A study of fossil clams'");
    
    my $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->replace_record('REFERENCE_DATA', { %ARTICLE, reference_no => $key,
					     reftitle => 'Replaced title', authors => 'Gray, H.' });
    
    ok_commit("the reference was replaced");
    
    is( names('REFERENCE_AUTHORS', $key), "Gray, H.", "the author rows were replaced" );
    
    ok_found_record('REFERENCE_DATA', "reference_no = $key and reftitle = 'Replaced title'
					and comments is null");
};


# A reference cannot be deleted while other records refer to it. Only its owner (or an
# administrator) may delete it.

subtest 'delete' => sub {
    
    my $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('REFERENCE_DATA', { %ARTICLE, reftitle => 'To be deleted',
	authors => 'Smith, J.', editors => 'Doe, J.' });
    
    ok_commit("a reference was inserted") || return;
    
    my ($key) = $edt->inserted_keys;
    
    foreach my $table ( 'COLLECTION_DATA', 'AUTHORITY_DATA' )
    {
	sql_command("INSERT INTO <<$table>> (reference_no) VALUES ($key)");
	
	$edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
	
	$edt->delete_record('REFERENCE_DATA', $key);
	
	ok_failed_commit("a reference used by $table cannot be deleted");
	
	ok_has_one_condition('E_CANNOT_DELETE', "the refusal was reported");
	
	clear_table($table);
    }
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('OTHER'));
    
    $edt->delete_record('REFERENCE_DATA', $key);
    
    ok_failed_commit("another member cannot delete the reference");
    
    ok_has_one_condition('E_PERMISSION', "the refusal was reported");
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->delete_record('REFERENCE_DATA', "ref:$key");
    
    ok_commit("the owner deleted the reference");
    
    ok_no_record('REFERENCE_DATA', "reference_no = $key");
    ok_no_record('REFERENCE_AUTHORS', "reference_no = $key");
    ok_no_record('REFERENCE_EDITORS', "reference_no = $key");
};


# Only authorized database members may insert references.

subtest 'permissions' => sub {
    
    foreach my $name ( 'GUEST', 'UNAUTH' )
    {
	my $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm($name));
	
	$edt->insert_record('REFERENCE_DATA', { %ARTICLE, reftitle => "Reference by $name",
						authors => 'Smith, J.' });
	
	ok_failed_commit("$name cannot insert a reference");
	
	ok_has_condition('E_PERMISSION', "the refusal was reported");
    }
    
    ok_no_record('REFERENCE_DATA', "reftitle like 'Reference by %'");
};


# Changes to REFERENCE_AUTHORS and REFERENCE_EDITORS are logged. A deletion is logged with a
# statement that would put the deleted rows back.

subtest 'logging' => sub {
    
    my $logdir = tempdir(CLEANUP => 1);
    my $logfile = "$logdir/edtpc-05.log";
    
    my $save_filename = ReferenceEdit->log_filename;
    
    ReferenceEdit->log_filename($logfile);
    
    my $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('REFERENCE_DATA', { %ARTICLE, reftitle => 'Logged reference',
	authors => 'Smith, J.; Jones, B.', editors => 'Doe, J.' });
    
    ok_commit("a reference was inserted");
    
    my ($key) = $edt->inserted_keys;
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->delete_record('REFERENCE_DATA', $key);
    
    ok_commit("the reference was deleted");
    
    ReferenceEdit->log_filename($save_filename);
    
    my $log = '';
    
    if ( open(my $fh, '<', $logfile) )
    {
	local $/;
	$log = <$fh>;
    }
    
    like( $log, qr/^# .* [|] INSERT [|] \S*ref_authors [|] reference_no = '$key'/m,
	  "the insertion of the authors was logged" );
    
    like( $log, qr/^# .* [|] DELETE [|] \S*ref_authors [|] reference_no = '$key'.*\n
		   ^\# \s REPLACE \s INTO \s \S*ref_authors .* 'Smith' .* 'Jones'/mx,
	  "the deletion of the authors was logged with the deleted rows" );
    
    like( $log, qr/^# .* [|] DELETE [|] \S*ref_editors [|] reference_no = '$key'.*\n
		   ^\# \s REPLACE \s INTO \s \S*ref_editors .* 'Doe'/mx,
	  "the deletion of the editors was logged with the deleted rows" );
};


# A new reference that matches an existing one is reported as a possible duplicate, unless
# the DUPLICATE allowance is given. With IGNORE-DUPLICATE, the action is skipped and its
# result is the list of matching references. Duplicate detection uses fulltext relevance
# scores, which are only meaningful with a realistic number of records. So ReferenceEdit
# searches the table REFERENCE_DUPLICATE_SEARCH, which is the real 'refs' table even when
# REFERENCE_DATA has been switched to the test database. The new references in this test
# duplicate real ones.

subtest 'duplicates' => sub {
    
    my $search_name = $TABLE{REFERENCE_DUPLICATE_SEARCH};
    
    is( $search_name, original_table_name($TABLE{REFERENCE_DATA}),
	"duplicates are searched for in the real table" );
    
    my $r = $T->dbh->selectrow_hashref("SELECT * FROM $search_name WHERE reference_no = 5000");
    
    unless ( $r && $r->{reftitle} && $r->{pubtitle} && $r->{author1last} )
    {
	plan skip_all => "reference 5000 is not suitable for this test";
	return;
    }
    
    my $authors = "$r->{author1last}, $r->{author1init}";
    $authors .= "; $r->{author2last}, $r->{author2init}" if $r->{author2last};
    
    my $dup = { publication_type => $r->{publication_type} || 'journal article',
		pubyr => $r->{pubyr}, reftitle => $r->{reftitle}, pubtitle => $r->{pubtitle},
		pubvol => $r->{pubvol}, pages => "$r->{firstpage}-$r->{lastpage}",
		authors => $authors };
    
    my $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'), 'CAPITAL');
    
    $edt->insert_record('REFERENCE_DATA', { %$dup });
    
    ok_failed_commit("a duplicate reference was rejected");
    
    ok_has_one_condition('C_DUPLICATE', "the duplicate was reported");
    
    like( ($edt->conditions)[0], qr/\bref:5000\b/, "the matching reference was identified" );
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'),
		      'CAPITAL', 'IGNORE-DUPLICATE');
    
    $edt->insert_record('REFERENCE_DATA', { %$dup });
    
    ok_commit("the transaction succeeded with IGNORE-DUPLICATE");
    
    is( $edt->action_status, 'skipped', "the action was skipped" );
    
    ok( ref $edt->action_result eq 'ARRAY' && grep( { $_ == 5000 } $edt->action_result->@* ),
	"the action result lists the matching reference" );
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'),
		      'CAPITAL', 'DUPLICATE');
    
    $edt->insert_record('REFERENCE_DATA', { %$dup });
    
    ok_commit("the duplicate was inserted with the DUPLICATE allowance");
    
    ok_found_record('REFERENCE_DATA', "reftitle = " . $T->dbh->quote($r->{reftitle}));
    
    # A reference whose DOI matches an existing reference should be reported, even if
    # nothing else matches.
    
    my ($doi_no, $doi) = $T->dbh->selectrow_array("SELECT reference_no, doi FROM $search_name
		WHERE doi like '10.%' ORDER BY reference_no LIMIT 1");
    
    ok( $doi_no, "found a real reference with a DOI" ) || return;
    
    $edt = ok_new_edt(class => 'ReferenceEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('REFERENCE_DATA', { %ARTICLE, pubyr => 1888,
	reftitle => 'Completely different words', authors => 'Zzyzx, Q.', doi => $doi });
    
    ok_failed_commit("a reference with the DOI of an existing one was rejected");
    
    ok_has_one_condition('C_DUPLICATE', "the duplicate was reported");
    
    like( ($edt->conditions)[0], qr/\bref:$doi_no\b/, "the matching reference was identified" );
};
