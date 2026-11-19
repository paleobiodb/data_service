#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for EditTransaction::Mod::PaleoBioDB, the module that adapts
# EditTransaction to the Paleobiology Database.
#
# edtpb-04-permissions.t : Test the permission methods of this module: table permissions,
#                          row permissions according to ownership, locks, BY_AUTHORIZER,
#                          CAN_DELETE, and CAN_MOVE, administrative permission, and access
#                          to collections.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 15;

use TableDefs qw(%TABLE set_table_property);

use EDTPBTest;
use PBDBTester;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_has_condition ok_has_one_condition
		  capture_mode captured_output clear_captured_output
		  clear_table sql_command ok_found_record ok_no_record);


# Establish the tester and select the test tables.

my $T = PBDBTester->new('EDTPBTest');

EDTPBTest->select_test_tables;

my @TABLES = qw(EDTPB_MAIN EDTPB_SUB EDTPB_BYAUTH EDTPB_UID EDTPB_KEEP EDTPB_OPEN COLLECTION_DATA);


# The test users are identified by the names of their sessions, without the prefix
# 'SESSION-'. ENTERER is enterer 39997 with authorizer 39998 (AUTHORIZER). OTHER is enterer
# 39995 with authorizer 39999 (SUPERUSER). STUDENT is enterer 39996, also with authorizer
# 39999. UNAUTH is enterer 39994 with no authorizer. WITH-ADMIN is enterer 39991.

sub perm {
    
    my ($name, $table) = @_;
    
    return $T->new_perm("SESSION-$name", $table || 'EDTPB_MAIN');
}


# Return the result of check_row_permission or check_table_permission for the specified
# user, as a string.

sub row_perm {
    
    my ($name, $table, $requested, $where) = @_;
    
    my $edt = EDTPBTest->new($T, { permission => perm($name, $table) });
    
    return join(',', $edt->check_row_permission($table, $requested, $where));
}

sub table_perm {
    
    my ($name, $table, $requested) = @_;
    
    my $edt = EDTPBTest->new($T, { permission => perm($name, $table) });
    
    return $edt->check_table_permission($table, $requested);
}


# Set up the test records and the specific table permissions. OTHER has 'modify' permission
# on EDTPB_MAIN, STUDENT has 'none', UNAUTH has 'view', and WITH-ADMIN has 'admin'.

subtest 'setup' => sub {
    
    foreach my $t ( @TABLES )
    {
	clear_table($t);
	$T->clear_specific_permissions($t);
    }
    
    sql_command("INSERT INTO <<EDTPB_MAIN>> (main_no, authorizer_no, enterer_no, string_req,
		 admin_lock, owner_lock) VALUES (1, 39998, 39997, 'own', 0, 0),
		 (2, 39999, 39995, 'other', 0, 0), (3, 39998, 39997, 'admin locked', 1, 0),
		 (4, 39998, 39997, 'owner locked', 0, 1), (5, 39999, 39995, 'other owner locked', 0, 1),
		 (6, 39998, 39997, 'second', 0, 0)");
    
    sql_command("INSERT INTO <<EDTPB_BYAUTH>> (byauth_no, authorizer_no, enterer_no)
		 VALUES (1, 39999, 39995), (2, 39998, 39998)");
    
    sql_command("INSERT INTO <<EDTPB_UID>> (uid_no, enterer_id) VALUES (1, 'USERID-ENTERER'),
		 (2, 'USERID-OTHER')");
    
    sql_command("INSERT INTO <<EDTPB_KEEP>> (keep_no, authorizer_no, enterer_no) VALUES (1, 39998, 39997)");
    
    sql_command("INSERT INTO <<EDTPB_OPEN>> (open_no) VALUES (1)");
    
    $T->set_specific_permission('EDTPB_MAIN', perm('OTHER'), 'modify');
    $T->set_specific_permission('EDTPB_MAIN', perm('STUDENT'), 'none');
    $T->set_specific_permission('EDTPB_MAIN', perm('UNAUTH'), 'view');
    $T->set_specific_permission('EDTPB_MAIN', perm('WITH-ADMIN'), 'admin');
};


# Check permissions on tables as a whole.

subtest 'table permissions' => sub {
    
    is( table_perm('ENTERER', 'EDTPB_MAIN', 'post'), 'granted', "a user with an authorizer may post" );
    is( table_perm('ENTERER', 'EDTPB_MAIN', 'view'), 'own', "a user who may post may view their own records" );
    is( table_perm('ENTERER', 'EDTPB_MAIN', 'modify'), 'none', "a user who may post may not modify others' records" );
    is( table_perm('UNAUTH', 'EDTPB_MAIN', 'post'), 'none', "a user without an authorizer may not post" );
    is( table_perm('UNAUTH', 'EDTPB_MAIN', 'view'), 'granted', "a specific 'view' permission is granted" );
    is( table_perm('OTHER', 'EDTPB_MAIN', 'modify'), 'granted', "a specific 'modify' permission is granted" );
    is( table_perm('OTHER', 'EDTPB_MAIN', 'post'), 'granted',
	"a specific 'modify' permission implies 'post'" );  # [Added by Claude under the guidance of Michael McClennen]
    is( table_perm('OTHER', 'EDTPB_MAIN', 'view'), 'granted',
	"a specific 'modify' permission implies 'view'" );  # [Added by Claude under the guidance of Michael McClennen]
    is( table_perm('STUDENT', 'EDTPB_MAIN', 'post'), 'none', "a specific permission of 'none' denies everything" );
    is( table_perm('WITH-ADMIN', 'EDTPB_MAIN', 'modify'), 'admin', "a specific 'admin' permission gives 'admin'" );
    is( table_perm('SUPERUSER', 'EDTPB_MAIN', 'modify'), 'admin', "the superuser has 'admin'" );
    is( table_perm('GUEST', 'EDTPB_MAIN', 'post'), 'none', "a guest may not post" );
    is( table_perm('STUDENT', 'EDTPB_UID', 'post'), 'granted',
	"a table with CAN_POST 'LOGGED_IN' allows any logged-in user to post" );
    is( table_perm('GUEST', 'EDTPB_OPEN', 'modify'), 'unrestricted', "a table with no properties is unrestricted" );
};


# Check row permissions according to who created or authorized each record.

subtest 'ownership' => sub {
    
    my $both = 'main_no in (1,2)';
    
    is( row_perm('ENTERER', 'EDTPB_MAIN', 'modify', $both), 'none,1,owned,1',
	"the enterer owns their own record and not the other one" );
    is( row_perm('AUTHORIZER', 'EDTPB_MAIN', 'modify', $both), 'none,1,owned,1',
	"the authorizer of a record owns it" );
    is( row_perm('OTHER', 'EDTPB_MAIN', 'modify', $both), 'granted,1,owned,1',
	"a user with 'modify' permission may modify records owned by others" );
    is( row_perm('STUDENT', 'EDTPB_MAIN', 'modify', $both), 'none',
	"a user with the permission 'none' may modify no records" );
    is( row_perm('GUEST', 'EDTPB_MAIN', 'modify', $both), 'none,2', "a guest may modify no records" );
    is( row_perm('ENTERER', 'EDTPB_MAIN', 'view', $both), 'none,1,owned,1',
	"a user who may only post may view their own records" );
    is( row_perm('UNAUTH', 'EDTPB_MAIN', 'view', $both), 'granted,2',
	"a user with 'view' permission may view any record" );
    is( row_perm('ENTERER', 'EDTPB_MAIN', 'admin', $both), 'none', "a user without admin permission gets 'none' for 'admin'" );
    is( row_perm('ENTERER', 'EDTPB_MAIN', 'modify', 'main_no = 99'), 'notfound', "a record that does not exist is reported" );
    is( row_perm('ENTERER', 'EDTPB_MAIN', 'replace', 'main_no = 99'), 'granted,1',
	"a user who may post may replace a record that does not exist" );
    is( row_perm('ENTERER', 'EDTPB_UID', 'modify', 'uid_no in (1,2)'), 'none,1,owned,1',
	"in a table that uses user ids, the user owns the records with their user id" );
};


# Check row permissions for locked records.

subtest 'locks' => sub {
    
    my $locked = 'main_no in (3,4,5)';
    
    is( row_perm('ENTERER', 'EDTPB_MAIN', 'modify', $locked), 'none,1,locked,1,owned_unlock,1',
	"the owner cannot modify a record with an administrative lock, but can unlock an owner lock" );
    is( row_perm('OTHER', 'EDTPB_MAIN', 'modify', $locked), 'locked,2,owned_unlock,1',
	"a user with 'modify' permission cannot modify records locked by others" );
    is( row_perm('SUPERUSER', 'EDTPB_MAIN', 'modify', $locked), 'admin_unlock,3',
	"an administrator can unlock any record" );
    is( row_perm('ENTERER', 'EDTPB_MAIN', 'view', 'main_no in (3,4)'), 'owned,2',
	"locks do not prevent viewing" );
};


# Check BY_AUTHORIZER, under which users who have the same authorizer as the creator of a
# record also own it.

subtest 'BY_AUTHORIZER' => sub {
    
    is( row_perm('STUDENT', 'EDTPB_BYAUTH', 'modify', 'byauth_no = 1'), 'owned,1',
	"a user with the same authorizer as the creator owns the record" );
    is( row_perm('ENTERER', 'EDTPB_BYAUTH', 'modify', 'byauth_no = 1'), 'none,1',
	"a user with a different authorizer does not" );
};


# Check the CAN_DELETE property.

subtest 'deletion' => sub {
    
    is( row_perm('ENTERER', 'EDTPB_MAIN', 'delete', 'main_no = 1'), 'none',
	"without CAN_DELETE, the owner may not delete a record" );
    is( row_perm('WITH-ADMIN', 'EDTPB_MAIN', 'delete', 'main_no = 1'), 'admin,1',
	"without CAN_DELETE, an administrator may delete a record" );
    is( row_perm('ENTERER', 'EDTPB_BYAUTH', 'delete', 'byauth_no in (1,2)'), 'none,1,owned,1',
	"with CAN_DELETE 'OWNER', only the owner may delete a record" );
    is( row_perm('ENTERER', 'EDTPB_UID', 'delete', 'uid_no in (1,2)'), 'granted,1,owned,1',
	"with CAN_DELETE 'AUTHORIZED', any user may delete a record" );
    is( row_perm('SUPERUSER', 'EDTPB_KEEP', 'delete', 'keep_no = 1'), 'none',
	"with CAN_DELETE 'NONE', even an administrator may not delete a record" );
    
    my $edt = ok_new_edt(permission => perm('ENTERER'));
    
    $edt->delete_record('EDTPB_MAIN', 1);
    
    ok_failed_commit("the owner cannot delete a record from EDTPB_MAIN");
    
    ok_has_one_condition('all', 'E_PERMISSION', qr/delete/, "the refusal is reported");
};


# Check administrative permission.

subtest 'administrators' => sub {
    
    is( row_perm('WITH-ADMIN', 'EDTPB_MAIN', 'modify', 'main_no in (1,2)'), 'admin,2',
	"a user with 'admin' permission may modify any record" );
    is( row_perm('SUPERUSER', 'EDTPB_MAIN', 'admin', 'main_no = 1'), 'admin,1',
	"the superuser has 'admin' permission on any record" );
    is( row_perm('SUPERUSER', 'EDTPB_MAIN', 'modify', 'main_no = 99'), 'notfound',
	"a record that does not exist is reported" );
    is( row_perm('SUPERUSER', 'EDTPB_MAIN', 'replace', 'main_no = 99'), 'admin,1',
	"an administrator may replace a record that does not exist" );
    is( row_perm('SUPERUSER', 'EDTPB_MAIN', 'view', 'main_no in (1,3)'), 'admin,2',
	"an administrator may view any record" );
    is( row_perm('SUPERUSER', 'EDTPB_UID', 'modify', 'uid_no = 1'), 'admin,1',
	"in a table without lock columns, an administrator may modify any record" );
    is( row_perm('SUPERUSER', 'EDTPB_UID', 'replace', 'uid_no = 99'), 'admin,1',
	"in a table without lock columns, an administrator may replace a record that does not exist" );
    is( row_perm('SUPERUSER', 'EDTPB_UID', 'modify', 'uid_no = 99'), 'notfound',
	"in a table without lock columns, a record that does not exist is reported" );
};


# Check tables without authorization properties.

sub set_open_delete {
    
    set_table_property(EDTPB_OPEN => CAN_DELETE => $_[0]);
    EDTPBTest->clear_table_cache('EDTPB_OPEN');
}

subtest 'unrestricted tables' => sub {
    
    is( row_perm('GUEST', 'EDTPB_OPEN', 'modify', 'open_no = 1'), 'unrestricted,1', "anybody may modify a record" );
    is( row_perm('GUEST', 'EDTPB_OPEN', 'replace', 'open_no = 99'), 'unrestricted,1', "anybody may replace a record" );
    is( row_perm('GUEST', 'EDTPB_OPEN', 'modify', 'open_no = 99'), 'notfound', "a record that does not exist is reported" );
    
    # A 'delete' request must satisfy the CAN_DELETE property, if it is set. [Added by
    # Claude under the guidance of Michael McClennen]
    
    is( row_perm('GUEST', 'EDTPB_OPEN', 'delete', 'open_no = 1'), 'unrestricted,1',
	"with CAN_DELETE unset, anybody may delete a record" );
    
    foreach my $value ( 'AUTHORIZED', 'OWNER' )
    {
	set_open_delete($value);
	is( row_perm('GUEST', 'EDTPB_OPEN', 'delete', 'open_no = 1'), 'unrestricted,1',
	    "with CAN_DELETE '$value', anybody may delete a record" );
    }
    
    set_open_delete('ADMIN');
    is( row_perm('ENTERER', 'EDTPB_OPEN', 'delete', 'open_no = 1'), 'none',
	"with CAN_DELETE 'ADMIN', an ordinary user may not delete a record" );
    is( row_perm('SUPERUSER', 'EDTPB_OPEN', 'delete', 'open_no = 1'), 'unrestricted,1',
	"with CAN_DELETE 'ADMIN', a superuser may delete a record" );
    is( row_perm('ENTERER', 'EDTPB_OPEN', 'modify', 'open_no = 1'), 'unrestricted,1',
	"with CAN_DELETE 'ADMIN', an ordinary user may still modify a record" );
    
    set_open_delete('NONE');
    is( row_perm('SUPERUSER', 'EDTPB_OPEN', 'delete', 'open_no = 1'), 'none',
	"with CAN_DELETE 'NONE', not even a superuser may delete a record" );
    
    set_open_delete(undef);
};


# Check access to collections, which depends on their access level and release date.

subtest 'collections' => sub {
    
    sql_command("INSERT INTO <<COLLECTION_DATA>> (collection_no, authorizer_no, enterer_no, access_level,
		 research_group, release_date) VALUES
		 (1, 39998, 39997, 'authorizer only', '', '2099-01-01'),
		 (2, 39999, 39995, 'the public', '', '2099-01-01'),
		 (3, 39999, 39995, 'database members', '', '2099-01-01'),
		 (4, 39999, 39995, 'group members', 'paleobotany,taphonomy', '2099-01-01'),
		 (5, 39999, 39995, 'authorizer only', '', '2099-01-01'),
		 (6, 39999, 39995, 'authorizer only', '', '2000-01-01')");
    
    # ENTERER belongs to the research group 'paleobotany', and OTHER, who entered the
    # collections, to 'vertebrate'.
    
    $T->dbh->do("UPDATE $TABLE{PERSON_DATA} SET research_group = 'paleobotany' WHERE person_no = 39997");
    $T->dbh->do("UPDATE $TABLE{PERSON_DATA} SET research_group = 'vertebrate' WHERE person_no = 39995");
    
    my %expected = ( 1 => 'owned,1', 2 => 'granted,1', 3 => 'granted,1', 4 => 'granted,1',
		     5 => 'none,1', 6 => 'granted,1' );
    
    my %label = ( 1 => "a collection the user entered", 2 => "a public collection",
		  3 => "a collection available to database members",
		  4 => "a collection available to a research group the user belongs to",
		  5 => "a collection available only to its authorizer",
		  6 => "a collection whose release date has passed" );
    
    foreach my $c ( 1..6 )
    {
	is( row_perm('ENTERER', 'COLLECTION_DATA', 'view', "collection_no = $c"), $expected{$c},
	    "a member can view $label{$c}" );
    }
    
    is( row_perm('STUDENT', 'COLLECTION_DATA', 'view', 'collection_no = 4'), 'none,1',
	"a member who does not belong to the research group cannot view the collection" );
    
    is( row_perm('GUEST', 'COLLECTION_DATA', 'view', 'collection_no = 3'), 'none,1',
	"a guest cannot view a collection available to database members" );
    
    
    # Anybody may view a public or released collection, but only a logged-in member with
    # an authorizer may modify one. [Added by Claude under the guidance of Michael
    # McClennen]
    
    foreach my $c ( 2, 6 )
    {
	is( row_perm('GUEST', 'COLLECTION_DATA', 'view', "collection_no = $c"), 'granted,1',
	    "a guest can view $label{$c}" );
	is( row_perm('GUEST', 'COLLECTION_DATA', 'modify', "collection_no = $c"), 'none,1',
	    "a guest cannot modify $label{$c}" );
	is( row_perm('UNAUTH', 'COLLECTION_DATA', 'view', "collection_no = $c"), 'granted,1',
	    "a member without an authorizer can view $label{$c}" );
	is( row_perm('UNAUTH', 'COLLECTION_DATA', 'modify', "collection_no = $c"), 'none,1',
	    "a member without an authorizer cannot modify $label{$c}" );
    }
    
    is( row_perm('UNAUTH', 'COLLECTION_DATA', 'modify', 'collection_no = 3'), 'none,1',
	"a member without an authorizer cannot modify a collection available to database members" );
    
    foreach my $c ( 2, 3, 4, 6 )
    {
	is( row_perm('ENTERER', 'COLLECTION_DATA', 'modify', "collection_no = $c"), 'granted,1',
	    "a member with an authorizer can modify $label{$c}" );
    }
    
    $T->dbh->do("UPDATE $TABLE{PERSON_DATA} SET research_group = NULL WHERE person_no in (39995, 39997)");
};


# Check the CAN_MOVE property, which determines who may move a record in a subordinate table
# to a different superior record.

sub set_can_move {
    
    set_table_property(EDTPB_SUB => CAN_MOVE => $_[0]);
    EDTPBTest->clear_table_cache('EDTPB_SUB');
}

subtest 'CAN_MOVE' => sub {
    
    set_can_move('AUTHORIZED');
    
    is( table_perm('ENTERER', 'EDTPB_SUB', 'move'), 'granted', "'AUTHORIZED' allows a user with an authorizer" );
    is( table_perm('UNAUTH', 'EDTPB_SUB', 'move'), 'none', "'AUTHORIZED' refuses a user without one" );
    
    set_can_move('MEMBERS');
    
    is( table_perm('UNAUTH', 'EDTPB_SUB', 'move'), 'granted', "'MEMBERS' allows any database member" );
    is( table_perm('GUEST', 'EDTPB_SUB', 'move'), 'none', "'MEMBERS' refuses a guest" );
    
    set_can_move('OWNER');
    
    is( table_perm('ENTERER', 'EDTPB_SUB', 'move'), 'granted', "'OWNER' leaves the decision to the record permission" );
    
    set_can_move('NONE');
    
    is( table_perm('ENTERER', 'EDTPB_SUB', 'move'), 'none', "'NONE' refuses everybody" );
    is( table_perm('SUPERUSER', 'EDTPB_SUB', 'move'), 'admin', "an administrator is not refused by check_table_permission" );
    
    # Move a record through a transaction.
    
    set_can_move('AUTHORIZED');
    
    clear_table('EDTPB_SUB');
    
    sql_command("INSERT INTO <<EDTPB_SUB>> (sub_no, main_no, authorizer_no, enterer_no, name)
		 VALUES (1, 1, 39998, 39997, 'moved')");
    
    my $edt = ok_new_edt(permission => perm('ENTERER', 'EDTPB_SUB'));
    
    $edt->update_record('EDTPB_SUB', { sub_no => 1, main_no => 6 });
    
    ok_commit("the owner of both records can move a record when CAN_MOVE allows it");
    
    ok_found_record('EDTPB_SUB', "sub_no = 1 and main_no = 6", "the record was moved");
    
    set_can_move('NONE');
    
    $edt = ok_new_edt(permission => perm('ENTERER', 'EDTPB_SUB'));
    
    $edt->update_record('EDTPB_SUB', { sub_no => 1, main_no => 1 });
    
    ok_failed_commit("the record cannot be moved when CAN_MOVE is 'NONE'");
    
    ok_has_one_condition('all', 'E_PERMISSION', qr/move/, "the refusal is reported");
    
    set_can_move(undef);
};


# Check that deleting a record from a subordinate table requires 'delete' permission on
# that record as well as 'modify' permission on the superior record. [Added by Claude
# under the guidance of Michael McClennen]

sub set_sub_delete {
    
    set_table_property(EDTPB_SUB => CAN_DELETE => $_[0]);
    EDTPBTest->clear_table_cache('EDTPB_SUB');
}

sub reset_sub_record {
    
    clear_table('EDTPB_SUB');
    
    sql_command("INSERT INTO <<EDTPB_SUB>> (sub_no, main_no, authorizer_no, enterer_no, name)
		 VALUES (1, 1, 39998, 39997, 'deleted')");
}

subtest 'subordinate deletion' => sub {
    
    # With CAN_DELETE unset, only an administrator may delete a subordinate record. The
    # record may still be updated.
    
    set_sub_delete(undef);
    reset_sub_record();
    
    my $edt = ok_new_edt(permission => perm('ENTERER', 'EDTPB_SUB'));
    
    $edt->delete_record('EDTPB_SUB', 1);
    
    ok_failed_commit("the owner cannot delete a subordinate record when CAN_DELETE is unset");
    
    ok_has_one_condition('all', 'E_PERMISSION', "the refusal is reported");
    
    $edt = ok_new_edt(permission => perm('ENTERER', 'EDTPB_SUB'));
    
    $edt->update_record('EDTPB_SUB', { sub_no => 1, name => 'updated' });
    
    ok_commit("the owner can still update the record");
    
    $edt = ok_new_edt(permission => perm('SUPERUSER', 'EDTPB_SUB'));
    
    $edt->delete_record('EDTPB_SUB', 1);
    
    ok_commit("a superuser can delete the record");
    
    ok_no_record('EDTPB_SUB', "sub_no = 1", "the record was deleted");
    
    # With 'OWNER', the owner may delete the record but a user who may only modify the
    # superior record may not.
    
    set_sub_delete('OWNER');
    reset_sub_record();
    
    $edt = ok_new_edt(permission => perm('OTHER', 'EDTPB_SUB'));
    
    $edt->delete_record('EDTPB_SUB', 1);
    
    ok_failed_commit("'modify' permission on the superior record is not enough under 'OWNER'");
    
    ok_has_one_condition('all', 'E_PERMISSION', "the refusal is reported");
    
    $edt = ok_new_edt(permission => perm('ENTERER', 'EDTPB_SUB'));
    
    $edt->delete_record('EDTPB_SUB', 1);
    
    ok_commit("the owner can delete the record under 'OWNER'");
    
    ok_no_record('EDTPB_SUB', "sub_no = 1", "the record was deleted");
    
    # With 'AUTHORIZED', any user who may modify the superior record may delete it.
    
    set_sub_delete('AUTHORIZED');
    reset_sub_record();
    
    $edt = ok_new_edt(permission => perm('OTHER', 'EDTPB_SUB'));
    
    $edt->delete_record('EDTPB_SUB', 1);
    
    ok_commit("a user who may modify the superior record can delete it under 'AUTHORIZED'");
    
    ok_no_record('EDTPB_SUB', "sub_no = 1", "the record was deleted");
    
    # With 'NONE', not even a superuser may delete it.
    
    set_sub_delete('NONE');
    reset_sub_record();
    
    $edt = ok_new_edt(permission => perm('SUPERUSER', 'EDTPB_SUB'));
    
    $edt->delete_record('EDTPB_SUB', 1);
    
    ok_failed_commit("nobody can delete a subordinate record when CAN_DELETE is 'NONE'");
    
    ok_found_record('EDTPB_SUB', "sub_no = 1", "the record was not deleted");
    
    set_sub_delete(undef);
};


# Check authorize_insert_key, which decides whether a record may be inserted with a key
# value chosen by the user.

subtest 'authorize_insert_key' => sub {
    
    my $edt = ok_new_edt(permission => perm('SUPERUSER'));
    
    is( $edt->authorize_insert_key('insert', 'EDTPB_MAIN'), 'none',
	"without the INSERT_KEY allowance, a key may not be chosen" );
    
    $edt->insert_record('EDTPB_MAIN', { main_no => 50, string_req => 'chosen key' });
    
    ok_has_one_condition('&_', 'E_PERMISSION', qr/key '50'/, "an insert with a chosen key is refused");
    
    # Without INSERT_KEY, replacing a record that does not exist is refused too, even for
    # a superuser. With it, an administrator may do either. [Added by Claude under the
    # guidance of Michael McClennen]
    
    $edt = ok_new_edt(permission => perm('SUPERUSER'));
    
    $edt->replace_record('EDTPB_MAIN', { main_no => 51, string_req => 'chosen key' });
    
    ok_failed_commit("replacing a missing record without INSERT_KEY fails");
    
    ok_has_one_condition('all', 'E_PERMISSION', qr/key '51'/, "the refusal is reported");
    
    $edt = ok_new_edt(permission => perm('SUPERUSER'), 'INSERT_KEY');
    
    ok( ! $edt->has_condition('all', 'W_BAD_ALLOWANCE'), "INSERT_KEY is a known allowance" );
    
    is( $edt->authorize_insert_key('insert', 'EDTPB_MAIN'), 'granted',
	"with the INSERT_KEY allowance, a key may be chosen" );
    
    $edt->insert_record('EDTPB_MAIN', { main_no => 50, string_req => 'chosen key' });
    $edt->replace_record('EDTPB_MAIN', { main_no => 51, string_req => 'chosen key' });
    
    ok_commit("with INSERT_KEY, an administrator may insert and replace with chosen keys");
    
    ok_found_record('EDTPB_MAIN', "main_no in (50, 51)", "the records were inserted");
    
    # A user who may only post is refused even with INSERT_KEY, unless they have been
    # granted 'insert_key' on the table.
    
    $edt = ok_new_edt(permission => perm('ENTERER'), 'INSERT_KEY');
    
    $edt->insert_record('EDTPB_MAIN', { main_no => 52, string_req => 'chosen key' });
    $edt->replace_record('EDTPB_MAIN', { main_no => 53, string_req => 'chosen key' });
    
    ok_failed_commit("with INSERT_KEY, a user who may only post cannot choose keys");
    
    ok_has_condition('all', 'E_PERMISSION', qr/key '52'|insert into this table/, "the insert is refused");
    
    $T->set_specific_permission('EDTPB_MAIN', perm('ENTERER'), 'post,insert_key');
    
    is( table_perm('ENTERER', 'EDTPB_MAIN', 'insert'), 'granted',
	"a granted 'insert_key' permission gives 'insert'" );
    
    $edt = ok_new_edt(permission => perm('ENTERER'), 'INSERT_KEY');
    
    $edt->insert_record('EDTPB_MAIN', { main_no => 52, string_req => 'chosen key' });
    $edt->replace_record('EDTPB_MAIN', { main_no => 53, string_req => 'chosen key' });
    
    ok_commit("with INSERT_KEY and a granted 'insert_key', a user may choose keys");
    
    ok_found_record('EDTPB_MAIN', "main_no in (52, 53)", "the records were inserted");
    
    $edt = ok_new_edt(permission => perm('ENTERER'));
    
    $edt->insert_record('EDTPB_MAIN', { main_no => 54, string_req => 'chosen key' });
    
    ok_failed_commit("a granted 'insert_key' does not work without the INSERT_KEY allowance");
    
    $T->dbh->do("DELETE FROM $TABLE{TABLE_PERMS} WHERE person_no = 39997 AND table_name = 'EDTPB_MAIN'");
    $T->dbh->do("DELETE FROM $TABLE{EDTPB_MAIN} WHERE main_no in (50, 51, 52, 53)");
};


# Check some of these permissions through transactions.

subtest 'transactions' => sub {
    
    my $edt = ok_new_edt(permission => perm('OTHER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, string_val => 'by other' });
    
    ok_commit("a user with 'modify' permission can update a record owned by somebody else");
    
    $edt = ok_new_edt(permission => perm('STUDENT'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 2, string_val => 'by student' });
    
    ok_failed_commit("a user with the permission 'none' cannot update a record");
    
    $edt = ok_new_edt(permission => perm('ENTERER', 'EDTPB_BYAUTH'));
    
    $edt->delete_record('EDTPB_BYAUTH', 2);
    
    ok_commit("a user can delete their own record when CAN_DELETE is 'OWNER'");
    
    ok_no_record('EDTPB_BYAUTH', "byauth_no = 2", "the record was deleted");
    
    $edt = ok_new_edt(permission => perm('SUPERUSER', 'EDTPB_KEEP'));
    
    $edt->delete_record('EDTPB_KEEP', 1);
    
    ok_failed_commit("nobody can delete a record when CAN_DELETE is 'NONE'");
};


# Check the debugging output of the permission methods, and a few remaining cases.

subtest 'debugging output' => sub {
    
    clear_captured_output();
    capture_mode(1);
    
    foreach my $name ( 'ENTERER', 'OTHER', 'UNAUTH', 'GUEST', 'SUPERUSER' )
    {
	my $edt = EDTPBTest->new($T, { permission => perm($name), allows => 'DEBUG_MODE' });
	
	$edt->check_table_permission('EDTPB_MAIN', $_) foreach qw(view post modify admin);
	$edt->check_row_permission('EDTPB_MAIN', $_, 'main_no in (1,2,3,4,5)') foreach qw(view modify delete);
	$edt->check_row_permission('EDTPB_MAIN', 'view', 'main_no = 99');
	$edt->check_row_permission('EDTPB_MAIN', 'replace', 'main_no = 99');
	$edt->check_row_permission('EDTPB_UID', 'delete', 'uid_no in (1,2)');
	$edt->check_row_permission('EDTPB_KEEP', 'delete', 'keep_no = 1');
	$edt->check_row_permission('EDTPB_OPEN', 'modify', 'open_no = 1');
    }
    
    capture_mode(0);
    
    like( captured_output(), qr/Permission for EDTPB_MAIN/, "the permission methods write debugging output" );
    
    clear_captured_output();
    
    is( row_perm('GUEST', 'EDTPB_MAIN', 'view', 'main_no = 1'), 'none',
	"a guest may not view records in a table that requires an authorizer" );
    
    is( row_perm('UNAUTH', 'EDTPB_MAIN', 'view', 'main_no = 99'), 'notfound',
	"a record that does not exist is reported for a user with 'view' permission" );
    
    is( row_perm('ENTERER', 'EDTPB_MAIN', 'modify', 'main_no in (4,6)'), 'owned_unlock,1,owned,1',
	"owned records with and without an owner lock are counted separately" );
    
    is( row_perm('OTHER', 'EDTPB_MAIN', 'modify', 'main_no in (5,6)'), 'granted_unlock,1,granted,1',
	"an owned and locked record and an unowned record give 'granted_unlock'" );
};

# Remove the specific table permissions.

subtest 'cleanup' => sub {
    
    $T->clear_specific_permissions($_) foreach @TABLES;
    
    ok( 1, "specific table permissions removed" );
};
