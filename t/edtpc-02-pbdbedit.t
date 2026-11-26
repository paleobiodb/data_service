#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the EditTransaction client modules, the subclasses of
# EditTransaction that handle edits to particular kinds of Paleobiology Database records.
#
# edtpc-02-pbdbedit.t : Test PBDBEdit, the general-purpose client module, using the
#                       PUBLICATION_DATA table: inserting, updating, and deleting records,
#                       external identifiers for the primary key, and permissions. Only
#                       administrators may edit this table, which has no ownership columns.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

# [Changed by Claude under the guidance of Michael McClennen: the table specifier
# PUBLICATIONS is now PUBLICATION_DATA.]

use lib 't', '../lib', 'lib';
use Test::More tests => 6;

use TableDefs qw(%TABLE);

use EDTPCTest;
use PBDBTester;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_has_one_condition ok_no_conditions
		  clear_table sql_command sql_fetchrow ok_found_record ok_no_record);


# Establish the tester and select the test tables.

my $T = PBDBTester->new('PBDBEdit');

EDTPCTest->select_test_tables('PBDBEdit');

sub perm {
    
    my ($name) = @_;
    
    return $T->new_perm("SESSION-$name", 'PUBLICATION_DATA');
}


# Start with a single publication record.

subtest 'setup' => sub {
    
    clear_table('PUBLICATION_DATA');
    
    sql_command("INSERT INTO <<PUBLICATION_DATA>> (pub_no, title, year) VALUES (1, 'first pub', 2001)");
    
    ok_found_record('PUBLICATION_DATA', "pub_no = 1");
};


# Insert a record, and check that the new key is returned and the record is stored.

subtest 'insert' => sub {
    
    my $edt = ok_new_edt(class => 'PBDBEdit', permission => perm('SUPERUSER'));
    
    $edt->insert_record('PUBLICATION_DATA', { title => 'second pub', year => 2002,
					  journal => 'Test Journal' });
    
    ok_commit("the record was inserted");
    
    my ($key) = $edt->inserted_keys;
    
    ok( $key && $key > 1, "the new key was returned" );
    
    my ($title, $journal) = sql_fetchrow("SELECT title, journal FROM <<PUBLICATION_DATA>>
					  WHERE pub_no = $key");
    
    is( "$title | $journal", "second pub | Test Journal", "the record was stored" );
};


# The primary key of PUBLICATION_DATA has the external identifier type 'PUB', so a key may be
# given either as a number or as an external identifier of that type.

subtest 'external identifiers' => sub {
    
    my $edt = ok_new_edt(class => 'PBDBEdit', permission => perm('SUPERUSER'));
    
    $edt->update_record('PUBLICATION_DATA', { pub_no => 'pub:1', title => 'first pub changed' });
    
    ok_commit("a record was updated using an external identifier");
    
    is( join(',', $edt->updated_keys), '1', "the key was unpacked" );
    
    ok_found_record('PUBLICATION_DATA', "pub_no = 1 and title = 'first pub changed'");
    
    $edt = ok_new_edt(class => 'PBDBEdit', permission => perm('SUPERUSER'));
    
    $edt->update_record('PUBLICATION_DATA', { pub_no => 'ref:1', title => 'wrong type' });
    
    ok_failed_commit("an identifier of the wrong type was rejected");
    
    ok_has_one_condition('E_EXTID', "the wrong identifier type was reported");
    
    ok_no_record('PUBLICATION_DATA', "title = 'wrong type'");
};


# Delete a record, using an external identifier.

subtest 'delete' => sub {
    
    sql_command("INSERT INTO <<PUBLICATION_DATA>> (pub_no, title) VALUES (10, 'to delete')");
    
    my $edt = ok_new_edt(class => 'PBDBEdit', permission => perm('SUPERUSER'));
    
    $edt->delete_record('PUBLICATION_DATA', 'pub:10');
    
    ok_commit("the record was deleted");
    
    is( join(',', $edt->deleted_keys), '10', "the deleted key was returned" );
    
    ok_no_record('PUBLICATION_DATA', "pub_no = 10");
};


# Only administrators may insert, update, or delete publications. A user with the 'admin'
# permission on the table counts as an administrator. A user with the 'post' permission on
# the table may insert publications, and one with the 'modify' permission may also update
# them. Since the table has no ownership columns, a user with only 'post' cannot change
# even the publications they entered, and only an administrator can delete one.

subtest 'permissions' => sub {
    
    foreach my $name ( 'ENTERER', 'GUEST' )
    {
	my $edt = ok_new_edt(class => 'PBDBEdit', permission => perm($name));
	
	$edt->insert_record('PUBLICATION_DATA', { title => "pub by $name" });
	
	ok_failed_commit("$name cannot insert a publication");
	
	ok_has_one_condition('E_PERMISSION', "the refusal was reported");
    }
    
    my $edt = ok_new_edt(class => 'PBDBEdit', permission => perm('ENTERER'));
    
    $edt->update_record('PUBLICATION_DATA', { pub_no => 1, title => 'not allowed' });
    
    ok_failed_commit("a non-administrator cannot update a publication");
    
    ok_has_one_condition('E_PERMISSION', "the refusal was reported as a permission error");
    
    $edt = ok_new_edt(class => 'PBDBEdit', permission => perm('ENTERER'));
    
    $edt->update_record('PUBLICATION_DATA', { pub_no => 9999, title => 'not allowed' });
    
    ok_failed_commit("a non-administrator cannot update a missing publication");
    
    ok_has_one_condition('E_NOT_FOUND', "the missing record was reported");
    
    $edt = ok_new_edt(class => 'PBDBEdit', permission => perm('ENTERER'));
    
    $edt->delete_record('PUBLICATION_DATA', 1);
    
    ok_failed_commit("a non-administrator cannot delete a publication");
    
    ok_has_one_condition('E_PERMISSION', "the refusal was reported");
    
    ok_no_record('PUBLICATION_DATA', "title like 'pub by %' or title = 'not allowed'");
    ok_found_record('PUBLICATION_DATA', "pub_no = 1");
    
    $T->set_specific_permission('PUBLICATION_DATA', perm('WITH-ADMIN'), 'admin');
    
    $edt = ok_new_edt(class => 'PBDBEdit', permission => perm('WITH-ADMIN'));
    
    $edt->insert_record('PUBLICATION_DATA', { title => 'pub by table admin' });
    $edt->update_record('PUBLICATION_DATA', { pub_no => 1, title => 'changed by table admin' });
    
    ok_commit("a user with the 'admin' permission on the table can edit publications");
    
    ok_found_record('PUBLICATION_DATA', "title = 'pub by table admin'");
    ok_found_record('PUBLICATION_DATA', "pub_no = 1 and title = 'changed by table admin'");
    
    $T->clear_specific_permissions('PUBLICATION_DATA');
    
    $T->set_specific_permission('PUBLICATION_DATA', perm('ENTERER'), 'post');
    
    $edt = ok_new_edt(class => 'PBDBEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('PUBLICATION_DATA', { title => 'pub by poster' });
    
    ok_commit("a user with the 'post' permission can insert a publication");
    
    my ($posted) = $edt->inserted_keys;
    
    ok_found_record('PUBLICATION_DATA', "title = 'pub by poster'");
    
    $edt = ok_new_edt(class => 'PBDBEdit', permission => perm('ENTERER'));
    
    $edt->update_record('PUBLICATION_DATA', { pub_no => $posted, title => 'changed by poster' });
    
    ok_failed_commit("a user with the 'post' permission cannot update a publication");
    
    ok_has_one_condition('E_PERMISSION', "the refusal was reported");
    
    $edt = ok_new_edt(class => 'PBDBEdit', permission => perm('ENTERER'));
    
    $edt->delete_record('PUBLICATION_DATA', $posted);
    
    ok_failed_commit("a user with the 'post' permission cannot delete a publication");
    
    ok_has_one_condition('E_PERMISSION', "the refusal was reported");
    
    $T->set_specific_permission('PUBLICATION_DATA', perm('ENTERER'), 'modify');
    
    $edt = ok_new_edt(class => 'PBDBEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('PUBLICATION_DATA', { title => 'pub by modifier' });
    $edt->update_record('PUBLICATION_DATA', { pub_no => 1, title => 'changed by modifier' });
    
    ok_commit("a user with the 'modify' permission can insert and update publications");
    
    ok_found_record('PUBLICATION_DATA', "title = 'pub by modifier'");
    ok_found_record('PUBLICATION_DATA', "pub_no = 1 and title = 'changed by modifier'");
    
    $edt = ok_new_edt(class => 'PBDBEdit', permission => perm('ENTERER'));
    
    $edt->delete_record('PUBLICATION_DATA', $posted);
    
    ok_failed_commit("a user with the 'modify' permission cannot delete a publication");
    
    ok_has_one_condition('E_PERMISSION', "the refusal was reported");
    
    ok_found_record('PUBLICATION_DATA', "pub_no = $posted");
    
    $T->clear_specific_permissions('PUBLICATION_DATA');
};


# PublicationEntry.pm creates its transactions with positional arguments: a permission
# object, a table, and an allowance hash. Check that this form works.

subtest 'positional arguments' => sub {
    
    my $edt = PBDBEdit->new($T->dbh, perm('SUPERUSER'), 'PUBLICATION_DATA', { });
    
    isa_ok( $edt, 'PBDBEdit', "the transaction" );
    
    $edt->insert_record({ title => 'default table pub' });
    
    ok_commit($edt, "a record was inserted into the default table");
    
    ok_found_record('PUBLICATION_DATA', "title = 'default table pub'");
};
