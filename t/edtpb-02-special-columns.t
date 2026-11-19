#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for EditTransaction::Mod::PaleoBioDB, the module that adapts
# EditTransaction to the Paleobiology Database.
#
# edtpb-02-special-columns.t : Test the handling of the special columns that record who
#                              created, authorized, and modified a record, and the lock
#                              columns.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 9;

use TableDefs qw(%TABLE);

use EDTPBTest;
use PBDBTester;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_has_condition ok_has_one_condition
		  ok_no_conditions clear_table sql_command);


# Establish the tester and select the test tables. The first group of tests below calls
# 'validate_special_column' directly, on actions on the table EDTPB_MAIN. The rest carry
# out transactions using the test users.

my $T = PBDBTester->new('EDTPBTest');

EDTPBTest->select_test_tables;


# Create a permission object for a user with the specified authorizer and enterer numbers.
# This avoids looking up a login session in the database.

sub test_permission {
    
    my ($authorizer_no, $enterer_no) = @_;
    
    return bless { authorizer_no => $authorizer_no, enterer_no => $enterer_no,
		   user_id => "U$enterer_no", role => 'enterer' }, 'Permissions';
}


# Create a new transaction of class EDTPBTest with the specified allowances, and an action
# on EDTPB_MAIN with the specified permission and operation. Return both.

sub new_action {
    
    my ($permission, $operation, @allowances) = @_;
    
    my $edt = EDTPBTest->new($T->dbh, { permission => test_permission(11, 22),
					 allows => [ 'SILENT_MODE', @allowances ] });
    
    my $record = $operation eq 'insert' ? { string_req => 'special' }
	: { main_no => 5, string_req => 'special' };
    
    my $action = $edt->_new_action($operation, 'EDTPB_MAIN', $record);
    
    $action->set_permission($permission);
    
    return ($edt, $action);
}


# The column record for a column that records an authorizer, and holds keys of EDTPB_MAIN.

my $AUTH_CR = { Field => 'main_no', FOREIGN_KEY => 'EDTPB_MAIN', FOREIGN_COL => 'main_no' };


# Check that the condition E_KEY_NOT_FOUND, which this module uses, has a template.

subtest 'E_KEY_NOT_FOUND' => sub {
    
    my ($edt) = new_action('admin', 'insert');
    
    ok( eval { $edt->add_condition('main', 'E_KEY_NOT_FOUND', 'authorizer_no', 99); 1 },
	"E_KEY_NOT_FOUND can be added" );
    
    is( ($edt->conditions)[0], "E_KEY_NOT_FOUND: Field 'authorizer_no': no record was found with key '99'",
	"E_KEY_NOT_FOUND has a message" );
};


# Check that a value given for an authorizer column is checked, including a value given as
# an external identifier.

subtest 'authorizer values' => sub {
    
    clear_table('EDTPB_MAIN');
    
    sql_command("INSERT INTO <<EDTPB_MAIN>> (main_no, string_req) VALUES (5, 'five')");
    
    my ($edt, $action) = new_action('admin', 'insert', 'ALTER_TRAIL');
    
    is_deeply( [ $edt->validate_special_column($action, 'main_no', $AUTH_CR, 'auth_authorizer',
						 undef, 'authorizer_no') ],
	       [ 'clean', 11 ], "with no value, the authorizer is filled in from the permission" );
    
    is_deeply( [ $edt->validate_special_column($action, 'main_no', $AUTH_CR, 'auth_authorizer',
						 '5', 'authorizer_no') ],
	       [ 'clean', 5 ], "an administrator with ALTER_TRAIL can set the authorizer" );
    
    is_deeply( [ $edt->validate_special_column($action, 'main_no', $AUTH_CR, 'auth_authorizer',
						 'prs:5', 'authorizer_no') ],
	       [ 'clean', 5 ], "the authorizer can be given as an external identifier" );
    
    $edt->validate_special_column($action, 'main_no', $AUTH_CR, 'auth_authorizer', '99',
				  'authorizer_no');
    
    ok( (grep { /E_KEY_NOT_FOUND.*99/ } $edt->conditions), "a key that does not exist is reported" );
    
    is( $edt->validate_special_column($action, 'main_no', $AUTH_CR, 'auth_authorizer', 'ref:5',
				      'authorizer_no'), 'ignore',
	"an external identifier of the wrong type is ignored" );
    
    ok( (grep { /E_EXTID.*type/ } $edt->conditions('&_')), "the wrong type is reported" );
    
    is_deeply( [ $edt->validate_special_column($action, 'main_no', $AUTH_CR, 'auth_authorizer',
						 '', 'authorizer_no') ],
	       [ 'clean', 11 ], "an empty value is treated like no value" );
    
    # Without ALTER_TRAIL, or without administrative permission, the value cannot be set.
    
    ($edt, $action) = new_action('admin', 'insert');
    
    is( $edt->validate_special_column($action, 'main_no', $AUTH_CR, 'auth_authorizer', '5',
				      'authorizer_no'), 'ignore',
	"an administrator cannot set the authorizer without ALTER_TRAIL" );
    
    ok( (grep { /C_ALTER_TRAIL/ } $edt->conditions), "the missing allowance is reported" );
    
    ($edt, $action) = new_action('granted', 'insert', 'ALTER_TRAIL');
    
    is( $edt->validate_special_column($action, 'main_no', $AUTH_CR, 'auth_authorizer', '5',
				      'authorizer_no'), 'ignore',
	"a user without administrative permission cannot set the authorizer" );
    
    ok( (grep { /E_PERMISSION_COLUMN.*authorizer_no/ } $edt->conditions), "the refusal is reported" );
};


# Check that the lock columns can only be set with the proper permission.

subtest 'lock values' => sub {
    
    my ($edt, $action) = new_action('granted', 'insert');
    
    my $lock_cr = { Field => 'admin_lock' };
    
    is( $edt->validate_special_column($action, 'admin_lock', $lock_cr, 'adm_lock', '1', 'admin_lock'),
	'ignore', "a user without administrative permission cannot set the administrative lock" );
    
    ok( (grep { /E_PERMISSION_COLUMN/ } $edt->conditions), "the refusal is reported" );
    
    ($edt, $action) = new_action('admin', 'insert');
    
    is_deeply( [ $edt->validate_special_column($action, 'admin_lock', $lock_cr, 'adm_lock', '1',
						 'admin_lock') ],
	       [ 'clean', 1 ], "an administrator can set the administrative lock" );
    
    # The owner lock can be set by the owner of a record, or on insert.
    
    my $own_cr = { Field => 'owner_lock' };
    
    ($edt, $action) = new_action('owned', 'update');
    
    is_deeply( [ $edt->validate_special_column($action, 'owner_lock', $own_cr, 'own_lock', 'yes',
						 'owner_lock') ],
	       [ 'clean', 1 ], "the owner of a record can set the owner lock" );
    
    ($edt, $action) = new_action('granted', 'update');
    
    is( $edt->validate_special_column($action, 'owner_lock', $own_cr, 'own_lock', '0', 'owner_lock'),
	'ignore', "a user who does not own the record cannot clear the owner lock" );
    
    ok( (grep { /E_PERMISSION.*unlock/ } $edt->conditions), "the refusal is reported" );
    
    ($edt, $action) = new_action('granted', 'insert');
    
    is_deeply( [ $edt->validate_special_column($action, 'owner_lock', $own_cr, 'own_lock', '1',
						 'owner_lock') ],
	       [ 'clean', 1 ], "any user can set the owner lock on a new record" );
    
    ($edt, $action) = new_action('admin', 'update');
    
    is( $edt->validate_special_column($action, 'admin_lock', $lock_cr, 'adm_lock', 'maybe', 'admin_lock'),
	'ignore', "a value that is not a boolean is ignored" );
    
    ok( (grep { /E_FORMAT/ } $edt->conditions('&_')), "the bad value is reported" );
};


# The remaining tests carry out transactions using the test users. Each starts from a record
# in EDTPB_MAIN with known values, entered by 'SESSION-ENTERER' (enterer 39997, authorizer
# 39998) with old timestamps, so that changes to the timestamps can be detected.

sub perm {
    
    my ($name) = @_;
    
    return $T->new_perm("SESSION-$name", 'EDTPB_MAIN');
}

sub reset_record {
    
    clear_table('EDTPB_MAIN');
    
    sql_command("INSERT INTO <<EDTPB_MAIN>> (main_no, authorizer_no, enterer_no, string_req,
		 created, modified) VALUES (1, 39998, 39997, 'original', '2020-01-01 00:00:00',
		 '2020-02-02 00:00:00')");
}

sub record {
    
    return $T->dbh->selectrow_hashref("SELECT * FROM $TABLE{EDTPB_MAIN} WHERE main_no = 1");
}


# Check the values filled in on insert.

subtest 'insert' => sub {
    
    clear_table('EDTPB_MAIN');
    clear_table('EDTPB_UID');
    
    my $edt = ok_new_edt(permission => perm('ENTERER'));
    
    $edt->insert_record('EDTPB_MAIN', { string_req => 'inserted' });
    $edt->insert_record('EDTPB_UID', { string_val => 'inserted' });
    
    ok_commit;
    
    my $r = $T->dbh->selectrow_hashref("SELECT * FROM $TABLE{EDTPB_MAIN}");
    
    is_deeply( [ @$r{qw(authorizer_no enterer_no modifier_no updater_no)} ], [ 39998, 39997, 0, 0 ],
	       "the authorizer and enterer are filled in, and the modifier and updater are not" );
    
    ok( $r->{created} && $r->{modified}, "the creation and modification times are set" );
    
    ok( ! defined $r->{updated}, "the update time is not set" );
    
    my $u = $T->dbh->selectrow_hashref("SELECT * FROM $TABLE{EDTPB_UID}");
    
    is_deeply( [ @$u{qw(enterer_id modifier_id)} ], [ 'USERID-ENTERER', '' ],
	       "in a table that uses user ids, the enterer is filled in as a user id" );
};


# Check the values filled in on update.

subtest 'update' => sub {
    
    reset_record();
    
    my $edt = ok_new_edt(permission => perm('ENTERER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, string_val => 'updated' });
    
    ok_commit;
    
    my $r = record();
    
    is_deeply( [ @$r{qw(authorizer_no enterer_no modifier_no)} ], [ 39998, 39997, 39997 ],
	       "the modifier is filled in, and the authorizer and enterer are unchanged" );
    
    isnt( $r->{modified}, '2020-02-02 00:00:00', "the modification time is changed" );
    is( $r->{created}, '2020-01-01 00:00:00', "the creation time is unchanged" );
    
    $edt = ok_new_edt(permission => perm('AUTHORIZER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, string_val => 'by authorizer' });
    
    ok_commit("the authorizer of a record can update it");
    
    is( record()->{modifier_no}, 39998, "the modifier is the authorizer" );
    
    $edt = ok_new_edt(permission => perm('OTHER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, string_val => 'by other' });
    
    ok_failed_commit("a user who does not own the record cannot update it");
    
    ok_has_condition('all', 'E_PERMISSION', "the refusal is reported");
};


# Check that a replace keeps the values that record who created and authorized the record.

subtest 'replace' => sub {
    
    reset_record();
    
    sql_command("UPDATE <<EDTPB_MAIN>> SET updater_no = 39999, modified = '2020-02-02 00:00:00'
		 WHERE main_no = 1");
    
    my $edt = ok_new_edt(permission => perm('ENTERER'));
    
    $edt->replace_record('EDTPB_MAIN', { main_no => 1, string_req => 'replaced' });
    
    ok_commit;
    
    my $r = record();
    
    is( $r->{string_req}, 'replaced', "the record was replaced" );
    
    is_deeply( [ @$r{qw(authorizer_no enterer_no updater_no)} ], [ 39998, 39997, 39999 ],
	       "the authorizer, enterer, and updater are copied from the old record" );
    
    is( $r->{modifier_no}, 39997, "the modifier is filled in" );
    
    is( $r->{created}, '2020-01-01 00:00:00', "the creation time is copied from the old record" );
};


# Check FIXUP_MODE, in which an administrator can change a record without changing its
# modifier and modification time.

subtest 'FIXUP_MODE' => sub {
    
    reset_record();
    
    my $edt = ok_new_edt('FIXUP_MODE', permission => perm('SUPERUSER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, string_val => 'fixed' });
    
    ok_commit;
    
    my $r = record();
    
    is( $r->{string_val}, 'fixed', "the record was updated" );
    
    is_deeply( [ $r->{modifier_no}, $r->{modified} ], [ 0, '2020-02-02 00:00:00' ],
	       "the modifier and modification time are unchanged" );
    
    is( $r->{updater_no}, 39999, "the updater is filled in" );
    
    ok( $r->{updated}, "the update time is set" );
    
    $edt = ok_new_edt('FIXUP_MODE', permission => perm('ENTERER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, string_val => 'not fixed' });
    
    ok_failed_commit("FIXUP_MODE requires administrative permission");
    
    ok_has_one_condition('all', 'E_FIXUP_MODE', "the refusal is reported");
};


# Check setting the special columns explicitly, which requires administrative permission
# and the ALTER_TRAIL allowance.

subtest 'setting special columns' => sub {
    
    reset_record();
    
    my $edt = ok_new_edt('ALTER_TRAIL', permission => perm('SUPERUSER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, authorizer_no => 'prs:39999', enterer_id => 39998,
					created => '2021-03-04 05:06:07' });
    
    ok_commit;
    
    my $r = record();
    
    is_deeply( [ @$r{qw(authorizer_no enterer_no created)} ], [ 39999, 39998, '2021-03-04 05:06:07' ],
	       "the authorizer, enterer, and creation time can be set" );
    
    $edt = ok_new_edt('ALTER_TRAIL', permission => perm('SUPERUSER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, authorizer_no => 12345 });
    
    ok_failed_commit("an authorizer that does not exist is rejected");
    
    ok_has_one_condition('all', 'E_KEY_NOT_FOUND', qr/12345/, "the bad key is reported");
    
    $edt = ok_new_edt('ALTER_TRAIL', permission => perm('SUPERUSER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, created => 'not a date' });
    
    ok_failed_commit("a bad creation time is rejected");
    
    ok_has_one_condition('all', 'E_FORMAT', qr/created/, "the bad value is reported");
    
    $edt = ok_new_edt(permission => perm('SUPERUSER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, authorizer_no => 39999 });
    
    ok_failed_commit("without ALTER_TRAIL, the authorizer cannot be set");
    
    ok_has_one_condition('all', 'C_ALTER_TRAIL', "the missing allowance is reported");
    
    reset_record();
    
    $edt = ok_new_edt('ALTER_TRAIL', permission => perm('ENTERER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, authorizer_no => 39999 });
    
    ok_failed_commit("a user without administrative permission cannot set the authorizer");
    
    ok_has_one_condition('all', 'E_PERMISSION_COLUMN', qr/authorizer_no/, "the refusal is reported");
};


# Check the lock columns. Changing a locked record requires the LOCKED allowance, unless the
# change clears every lock that is set on the record.

subtest 'locks' => sub {
    
    reset_record();
    
    my $edt = ok_new_edt(permission => perm('ENTERER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, admin_lock => 1 });
    
    ok_failed_commit("the owner of a record cannot set the administrative lock");
    
    ok_has_one_condition('all', 'E_PERMISSION_COLUMN', qr/admin_lock/, "the refusal is reported");
    
    $edt = ok_new_edt(permission => perm('SUPERUSER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, admin_lock => 1 });
    
    ok_commit("an administrator can set the administrative lock");
    
    is( record()->{admin_lock}, 1, "the record is locked" );
    
    $edt = ok_new_edt(permission => perm('ENTERER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, string_val => 'locked' });
    
    ok_failed_commit("the owner cannot update a record with an administrative lock");
    
    ok_has_one_condition('all', 'E_LOCKED', "the lock is reported");
    
    $edt = ok_new_edt(permission => perm('SUPERUSER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, string_val => 'admin update' });
    
    ok_failed_commit("an administrator cannot otherwise change a locked record without LOCKED");
    
    ok_has_condition('all', 'C_LOCKED', "the missing allowance is reported");
    
    ok_has_condition('all', 'W_LOCKED', "the action has a warning that the record is locked" );  # [Added by Claude under the guidance of Michael McClennen]
    
    $edt = ok_new_edt('LOCKED', permission => perm('SUPERUSER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, string_val => 'admin update' });
    
    ok_commit("an administrator can change a locked record with LOCKED");
    
    is_deeply( [ @{record()}{qw(string_val admin_lock)} ], [ 'admin update', 1 ],
	       "the record was changed and is still locked" );
    
    $edt = ok_new_edt(permission => perm('SUPERUSER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, admin_lock => 0, string_val => 'unlocked' });
    
    ok_commit("an administrator can unlock a record without LOCKED");
    
    ok( ! $edt->has_condition('all', 'W_LOCKED'), "an action that unlocks the record has no lock warning" );  # [Added by Claude under the guidance of Michael McClennen]
    
    is_deeply( [ @{record()}{qw(string_val admin_lock)} ], [ 'unlocked', 0 ],
	       "the record was changed and unlocked" );
    
    # The owner lock.
    
    $edt = ok_new_edt(permission => perm('ENTERER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, owner_lock => 1 });
    
    ok_commit("the owner can set the owner lock");
    
    is( record()->{owner_lock}, 1, "the record is locked" );
    
    $edt = ok_new_edt(permission => perm('ENTERER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, string_val => 'owner locked' });
    
    ok_failed_commit("the owner cannot change a record with an owner lock without LOCKED");
    
    ok_has_condition('all', 'C_LOCKED', "the missing allowance is reported");
    
    $edt = ok_new_edt(permission => perm('SUPERUSER'));
    
    $edt->delete_record('EDTPB_MAIN', 1);
    
    ok_failed_commit("an administrator cannot delete a locked record without LOCKED");
    
    ok_has_condition('all', 'C_LOCKED', "the missing allowance is reported");
    
    # If both locks are set, clearing only one of them does not unlock the record.
    
    sql_command("UPDATE <<EDTPB_MAIN>> SET admin_lock = 1 WHERE main_no = 1");
    
    $edt = ok_new_edt(permission => perm('SUPERUSER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, admin_lock => 0 });
    
    ok_failed_commit("clearing one of two locks requires LOCKED");
    
    ok_has_condition('all', 'C_LOCKED', "the missing allowance is reported");
    
    ok_has_condition('all', 'W_LOCKED', "an action that leaves a lock set has a lock warning" );  # [Added by Claude under the guidance of Michael McClennen]
    
    $edt = ok_new_edt(permission => perm('SUPERUSER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, admin_lock => 0, owner_lock => 0 });
    
    ok_commit("clearing both locks does not require LOCKED");
    
    ok( ! $edt->has_condition('all', 'W_LOCKED'), "an action that clears both locks has no lock warning" );  # [Added by Claude under the guidance of Michael McClennen]
    
    is_deeply( [ @{record()}{qw(admin_lock owner_lock)} ], [ 0, 0 ], "the record is unlocked" );
    
    # The owner can clear the owner lock.
    
    sql_command("UPDATE <<EDTPB_MAIN>> SET owner_lock = 1 WHERE main_no = 1");
    
    $edt = ok_new_edt(permission => perm('ENTERER'));
    
    $edt->update_record('EDTPB_MAIN', { main_no => 1, owner_lock => 0, string_val => 'owner unlocked' });
    
    ok_commit("the owner can unlock a record without LOCKED");
    
    ok( ! $edt->has_condition('all', 'W_LOCKED'), "the owner's unlocking action has no lock warning" );  # [Added by Claude under the guidance of Michael McClennen]
    
    is_deeply( [ @{record()}{qw(string_val owner_lock)} ], [ 'owner unlocked', 0 ],
	       "the record was changed and unlocked" );
};
