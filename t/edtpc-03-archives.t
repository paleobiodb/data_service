#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the EditTransaction client modules, the subclasses of
# EditTransaction that handle edits to particular kinds of Paleobiology Database records.
#
# edtpc-03-archives.t : Test ArchiveEdit, which handles data archive records. An archive
#                       that has been assigned a DOI may not be altered or deleted except
#                       by an administrator using the allowance ARCHIVE_HAS_DOI, apart
#                       from its description, and only an administrator may set a DOI.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 8;

use TableDefs qw(%TABLE);

use EDTPCTest;
use PBDBTester;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_has_one_condition ok_has_condition
		  clear_table sql_command sql_fetchrow ok_found_record ok_no_record);


# Establish the tester and select the test tables.

my $T = PBDBTester->new('ArchiveEdit');

EDTPCTest->select_test_tables('ArchiveEdit');

sub perm {
    
    my ($name) = @_;
    
    return $T->new_perm("SESSION-$name", 'ARCHIVES');
}


# Reset the test records. Archive 1 has a DOI and archive 2 does not. Both are owned by
# ENTERER.

sub reset_archives {
    
    clear_table('ARCHIVES');
    
    sql_command("INSERT INTO <<ARCHIVES>> (archive_no, authorizer_no, enterer_no, title,
		 description, doi) VALUES (1, 39998, 39997, 'with doi', 'desc 1', '10.9999/one'),
		 (2, 39998, 39997, 'without doi', 'desc 2', '')");
}

subtest 'setup' => sub {
    
    reset_archives();
    
    ok_found_record('ARCHIVES', "archive_no = 1 and doi <> ''");
    ok_found_record('ARCHIVES', "archive_no = 2 and doi = ''");
};


# Only an administrator can create an archive with a DOI, and then only with the allowance
# ARCHIVE_HAS_DOI.

subtest 'insert with doi' => sub {
    
    my $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('ARCHIVES', { title => 'member doi', doi => '10.9999/member' });
    
    ok_failed_commit("a member cannot create an archive with a doi");
    
    ok_has_one_condition('E_PERMISSION_COLUMN', qr/'doi'/, "the doi column was reported");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('SUPERUSER'));
    
    $edt->insert_record('ARCHIVES', { title => 'admin doi', doi => '10.9999/admin' });
    
    ok_failed_commit("an administrator needs an allowance to create an archive with a doi");
    
    ok_has_one_condition('C_ARCHIVE_HAS_DOI', "the caution was reported");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('SUPERUSER'), 'ARCHIVE_HAS_DOI');
    
    $edt->insert_record('ARCHIVES', { title => 'admin doi', doi => '10.9999/admin' });
    
    ok_commit("an administrator with the allowance can create an archive with a doi");
    
    ok_found_record('ARCHIVES', "title = 'admin doi' and doi = '10.9999/admin'");
    ok_no_record('ARCHIVES', "title = 'member doi'");
};


# Only an administrator can set the DOI of an existing archive.

subtest 'set doi' => sub {
    
    reset_archives();
    
    my $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('ENTERER'));
    
    $edt->update_record('ARCHIVES', { archive_no => 2, doi => '10.9999/two' });
    
    ok_failed_commit("the owner cannot set the doi");
    
    ok_has_one_condition('E_PERMISSION_COLUMN', qr/'doi'/, "the doi column was reported");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('ENTERER'));
    
    $edt->update_record('ARCHIVES', { archive_no => 2, title => 'empty doi', doi => '' });
    
    ok_commit("the owner can submit an empty doi for an archive that has none");
    
    ok_found_record('ARCHIVES', "archive_no = 2 and title = 'empty doi' and doi = ''");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('ENTERER'));
    
    $edt->update_record('ARCHIVES', { archive_no => 1, doi => '' });
    
    ok_failed_commit("the owner cannot remove the doi");
    
    ok_has_condition('E_PERMISSION_COLUMN', qr/'doi'/, "the doi column was reported");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('SUPERUSER'));
    
    $edt->update_record('ARCHIVES', { archive_no => 2, doi => '10.9999/two' });
    
    ok_failed_commit("an administrator needs an allowance to set the doi");
    
    ok_has_one_condition('C_ARCHIVE_HAS_DOI', "the caution was reported");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('SUPERUSER'), 'ARCHIVE_HAS_DOI');
    
    $edt->update_record('ARCHIVES', { archive_no => 2, doi => '10.9999/two' });
    
    ok_commit("an administrator with the allowance can set the doi");
    
    ok_found_record('ARCHIVES', "archive_no = 2 and doi = '10.9999/two'");
};


# An archive with a DOI is immutable, except for its description. Archives without a DOI
# can be changed by their owners as usual.

subtest 'update with doi' => sub {
    
    reset_archives();
    
    my $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('ENTERER'));
    
    $edt->update_record('ARCHIVES', { archive_no => 2, title => 'changed title' });
    
    ok_commit("the owner can change an archive without a doi");
    
    ok_found_record('ARCHIVES', "archive_no = 2 and title = 'changed title'");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('ENTERER'));
    
    $edt->update_record('ARCHIVES', { archive_no => 1, title => 'changed title' });
    
    ok_failed_commit("the owner cannot change the title of an archive with a doi");
    
    ok_has_one_condition('E_IMMUTABLE', "the archive was reported as immutable");
    
    ok_found_record('ARCHIVES', "archive_no = 1 and title = 'with doi'");
  
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('ENTERER'));
    
    $edt->update_record('ARCHIVES', { archive_no => 1, description => 'changed desc' });
    
    ok_commit("the owner can change the description of an archive with a doi");
    
    ok_found_record('ARCHIVES', "archive_no = 1 and description = 'changed desc'");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('ENTERER'));
    
    $edt->update_record('ARCHIVES', { archive_id => 'dar:1', description => 'second desc' });
    
    ok_commit("the description can be changed when the key is given as archive_id");
    
    ok_found_record('ARCHIVES', "archive_no = 1 and description = 'second desc'");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('SUPERUSER'));
    
    $edt->update_record('ARCHIVES', { archive_no => 1, title => 'admin title' });
    
    ok_failed_commit("an administrator needs an allowance to change an archive with a doi");
    
    ok_has_one_condition('C_ARCHIVE_HAS_DOI', "the caution was reported");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('SUPERUSER'), 'ARCHIVE_HAS_DOI');
    
    $edt->update_record('ARCHIVES', { archive_no => 1, title => 'admin title' });
    
    ok_commit("an administrator with the allowance can change an archive with a doi");
    
    ok_found_record('ARCHIVES', "archive_no = 1 and title = 'admin title'");
};


# Replacing an archive with a DOI requires administrative permission and the allowance,
# even if the DOI is unchanged.

subtest 'replace with doi' => sub {
    
    reset_archives();
    
    my $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('SUPERUSER'));
    
    $edt->replace_record('ARCHIVES', { archive_no => 1, title => 'replaced',
				       doi => '10.9999/one' });
    
    ok_failed_commit("an administrator needs an allowance to replace an archive with a doi");
    
    ok_has_one_condition('C_ARCHIVE_HAS_DOI', "the caution was reported");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('SUPERUSER'), 'ARCHIVE_HAS_DOI');
    
    $edt->replace_record('ARCHIVES', { archive_no => 1, title => 'replaced',
				       doi => '10.9999/one' });
    
    ok_commit("an administrator with the allowance can replace an archive with a doi");
    
    ok_found_record('ARCHIVES', "archive_no = 1 and title = 'replaced' and doi = '10.9999/one'");
    
    ok_found_record('ARCHIVES', "archive_no = 1 and enterer_no = 39997 and modifier_no = 39999",
		    "the owner was kept and the administrator was recorded as the modifier");
};


# An archive with a DOI can only be deleted by an administrator with the allowance.

subtest 'delete with doi' => sub {
    
    reset_archives();
    
    my $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('ENTERER'));
    
    $edt->delete_record('ARCHIVES', 2);
    
    ok_commit("the owner can delete an archive without a doi");
    
    ok_no_record('ARCHIVES', "archive_no = 2");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('ENTERER'));
    
    $edt->delete_record('ARCHIVES', 1);
    
    ok_failed_commit("the owner cannot delete an archive with a doi");
    
    ok_has_one_condition('E_IMMUTABLE', "the archive was reported as immutable");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('SUPERUSER'));
    
    $edt->delete_record('ARCHIVES', 1);
    
    ok_failed_commit("an administrator needs an allowance to delete an archive with a doi");
    
    ok_has_one_condition('C_ARCHIVE_HAS_DOI', "the caution was reported");
    
    ok_found_record('ARCHIVES', "archive_no = 1");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('SUPERUSER'), 'ARCHIVE_HAS_DOI');
    
    $edt->delete_record('ARCHIVES', 1);
    
    ok_commit("an administrator with the allowance can delete an archive with a doi");
    
    ok_no_record('ARCHIVES', "archive_no = 1");
};


# Another member cannot change or delete an archive, and the refusal is reported as a
# permission error.

subtest 'other members' => sub {
    
    reset_archives();
    
    my $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('OTHER'));
    
    $edt->update_record('ARCHIVES', { archive_no => 2, title => 'not mine' });
    
    ok_failed_commit("another member cannot change an archive");
    
    ok_has_one_condition('E_PERMISSION', "the refusal was reported as a permission error");
    
    $edt = ok_new_edt(class => 'ArchiveEdit', permission => perm('OTHER'));
    
    $edt->delete_record('ARCHIVES', 'dar:2');
    
    ok_failed_commit("another member cannot delete an archive");
    
    ok_has_one_condition('E_PERMISSION', "the refusal was reported as a permission error");
    
    ok_found_record('ARCHIVES', "archive_no = 2 and title = 'without doi'");
};


# ArchiveEntry.pm creates its transactions with positional arguments: a permission object,
# a table, and an allowance hash. Check that this form works, including the allowance.

subtest 'positional arguments' => sub {
    
    reset_archives();
    
    my $edt = ArchiveEdit->new($T->dbh, perm('SUPERUSER'), 'ARCHIVES', { ARCHIVE_HAS_DOI => 1 });
    
    isa_ok( $edt, 'ArchiveEdit', "the transaction" );
    
    ok( $edt->allows('ARCHIVE_HAS_DOI'), "the allowance was accepted" );
    
    $edt->update_record({ archive_no => 1, title => 'positional' });
    
    ok_commit($edt, "the record was updated");
    
    ok_found_record('ARCHIVES', "archive_no = 1 and title = 'positional'");
};
