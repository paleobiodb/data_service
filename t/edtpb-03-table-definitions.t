#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for EditTransaction::Mod::PaleoBioDB, the module that adapts
# EditTransaction to the Paleobiology Database.
#
# edtpb-03-table-definitions.t : Test the properties that this module adds to table and
#                                column definitions, and the handling of key values and
#                                data values given as external identifiers.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 7;

use TableDefs qw(%TABLE);
use ExternalIdent;

use EDTPBTest;
use PBDBTester;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_has_condition ok_has_one_condition
		  ok_no_conditions clear_table sql_command ok_found_record);


# Establish the tester and select the test tables. These tests are carried out by the
# superuser, so that permissions do not affect them.

my $T = PBDBTester->new('EDTPBTest');

EDTPBTest->select_test_tables;

my $PERM = $T->new_perm('SESSION-SUPERUSER', 'EDTPB_MAIN');


# Clear the test tables and add records with known keys.

sub reset_tables {
    
    clear_table('EDTPB_MAIN');
    clear_table('EDTPB_SUB');
    
    sql_command("INSERT INTO <<EDTPB_MAIN>> (main_no, string_req) VALUES (5, 'five'), (6, 'six')");
}


# Check the properties added by finish_table_definition.

subtest 'table definitions' => sub {
    
    my $edt = ok_new_edt(permission => $PERM);
    
    my $main = $edt->table_info_ref('EDTPB_MAIN');
    
    is( $main->{PRIMARY_FIELD}, 'main_id', "the primary key gets an alternate field name ending in _id" );
    
    is( $main->{AUTH_COLUMN_STRING}, 'authorizer_no,enterer_no,admin_lock,owner_lock',
	"the authorization columns are listed" );
    
    ok( ! $main->{UNRESTRICTED}, "a table with a CAN_POST property is not unrestricted" );
    
    my %expected = ( authorizer_no => [ 'auth_authorizer', 'PRS', 'authorizer_id' ],
		     enterer_no => [ 'auth_creator', 'PRS', 'enterer_id' ],
		     modifier_no => [ 'auth_modifier', 'PRS', 'modifier_id' ],
		     created => [ 'ts_created', undef, undef ],
		     modified => [ 'ts_modified', undef, undef ],
		     admin_lock => [ 'adm_lock', undef, undef ],
		     owner_lock => [ 'own_lock', undef, undef ],
		     reference_no => [ undef, 'REF', 'reference_id' ],
		     main_no => [ undef, 'COL', undef ] );
    
    foreach my $colname ( sort keys %expected )
    {
	my $cr = $edt->table_column_ref('EDTPB_MAIN', $colname);
	
	is_deeply( [ $cr->{DIRECTIVE}, $cr->{EXTID_TYPE}, $cr->{ALTERNATE_NAME} ], $expected{$colname},
		   "column '$colname' has the expected directive, external identifier type, and alternate name" );
    }
    
    my $sub = $edt->table_info_ref('EDTPB_SUB');
    
    is( $sub->{AUTH_FIELD}, 'main_id', "a subordinate table gets an alternate name for its link column" );
    
    my $uid = $edt->table_info_ref('EDTPB_UID');
    
    is( $uid->{AUTH_COLUMN_STRING}, 'approver_id as authorizer_id,enterer_id',
	"a column with a nonstandard name is listed under the standard one" );
    
    is( $edt->table_column_ref('EDTPB_UID', 'enterer_id')->{DIRECTIVE}, 'auth_creator',
	"a user id column gets its directive" );
    
    ok( $edt->table_info_ref('EDTPB_OPEN')->{UNRESTRICTED}, "a table with no CAN_ properties is unrestricted" );
};


# Check key values given as external identifiers, which are unpacked by before_key_column.

subtest 'key values' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt(permission => $PERM);
    
    $edt->update_record('EDTPB_MAIN', { main_no => 'col:5', string_val => 'by identifier' });
    $edt->update_record('EDTPB_MAIN', { main_id => 'col:6', string_val => 'by alternate name' });
    
    ok_no_conditions('all', "keys given as external identifiers of the proper type are accepted");
    
    ok_commit;
    
    ok_found_record('EDTPB_MAIN', "main_no = 5 and string_val = 'by identifier'", "the first record was updated");
    ok_found_record('EDTPB_MAIN', "main_no = 6 and string_val = 'by alternate name'", "the second record was updated");
    
    $edt = ok_new_edt(permission => $PERM);
    
    $edt->update_record('EDTPB_MAIN', { main_no => PBDB::ExtIdent->new('col', 5), string_val => 'by object' });
    
    ok_no_conditions('&_', "a key given as an external identifier object is accepted");
    
    is( $edt->action_keyval('&_'), 5, "the key value is unpacked from the object" );
    
    $edt->update_record('EDTPB_MAIN', { main_no => 'ref:5', string_val => 'wrong type' });
    
    ok_has_condition('&_', 'E_EXTID', qr/type/, "an external identifier of the wrong type is rejected");
    
    $edt->update_record('EDTPB_MAIN', { main_no => PBDB::ExtIdent->new('ref', 5), string_val => 'wrong type' });
    
    ok_has_condition('&_', 'E_EXTID', qr/type/, "an external identifier object of the wrong type is rejected");
    
    $edt->update_record('EDTPB_OPEN', { open_no => 'col:1', string_val => 'not accepted' });
    
    ok_has_condition('&_', 'E_EXTID', qr/does not accept/,
		     "an external identifier is rejected for a key column with no identifier type");
};


# Check data values given as external identifiers, which are unpacked by check_data_column.

subtest 'data values' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt(permission => $PERM);
    
    $edt->insert_record('EDTPB_MAIN', { string_req => 'ref seven', reference_no => 'ref:7' });
    $edt->insert_record('EDTPB_MAIN', { string_req => 'ref eight', reference_id => 'ref:8' });
    $edt->insert_record('EDTPB_MAIN', { string_req => 'ref nine', reference_no => PBDB::ExtIdent->new('ref', 9) });
    $edt->insert_record('EDTPB_MAIN', { string_req => 'text', string_val => 'page:12' });
    
    ok_no_conditions('all', "data values given as external identifiers are accepted");
    
    ok_commit;
    
    ok_found_record('EDTPB_MAIN', "string_req = 'ref seven' and reference_no = 7", "the value was unpacked");
    ok_found_record('EDTPB_MAIN', "string_req = 'ref eight' and reference_no = 8",
		    "the value was unpacked when given under the alternate name");
    ok_found_record('EDTPB_MAIN', "string_req = 'ref nine' and reference_no = 9",
		    "the value was unpacked from an object");
    ok_found_record('EDTPB_MAIN', "string_req = 'text' and string_val = 'page:12'",
		    "a text value that looks like an external identifier is stored as it is");
    
    $edt = ok_new_edt(permission => $PERM);
    
    $edt->insert_record('EDTPB_MAIN', { string_req => 'wrong type', reference_no => 'col:7' });
    
    ok_has_one_condition('&_', 'E_EXTID', qr/type/, "an external identifier of the wrong type is rejected");
    
    $edt->insert_record('EDTPB_MAIN', { string_req => 'bad number', reference_no => 'ref:ERROR' });
    
    ok_has_condition('&_', 'E_FORMAT', "an external identifier without a valid number is rejected");
    
    $edt->insert_record('EDTPB_MAIN', { string_req => 'not accepted', int_val => 'col:1' });
    
    ok_has_condition('&_', 'E_EXTID', qr/does not accept/,
		     "an external identifier is rejected for an integer column with no identifier type");
};


# Check validate_extid_value directly.

subtest 'validate_extid_value' => sub {
    
    my $edt = ok_new_edt(permission => $PERM);
    
    is_deeply( [ $edt->validate_extid_value('COL', 'col:12', 'f') ], [ 1, 12 ],
	       "a string of the proper type is unpacked" );
    
    is_deeply( [ $edt->validate_extid_value('COL', 'paleobiodb.org:col:12', 'f') ], [ 1, 12 ],
	       "a string with a URN prefix is unpacked" );
    
    is_deeply( [ $edt->validate_extid_value('COL', '12', 'f') ], [ 1, 12 ],
	       "a plain number is accepted" );
    
    my $object = PBDB::ExtIdent->new('col', 12);
    
    my ($result, $value) = $edt->validate_extid_value('COL', $object, 'f');
    
    ok( $result && "$value" eq '12', "an object of the proper type is accepted" );
    
    like( $edt->validate_extid_value('COL', 'occ:12', 'f')->[2], qr/must be of type 'col'.*occ/,
	  "a string of the wrong type is rejected" );
    
    like( $edt->validate_extid_value('COL', PBDB::ExtIdent->new('occ', 12), 'f')->[2],
	  qr/must be of type 'col'.*occ/, "an object of the wrong type is rejected" );
    
    like( $edt->validate_extid_value('COL', 'abc', 'f')->[2], qr/not a valid external identifier/,
	  "a value that is not an external identifier is rejected" );
};


# Check that a superuser can insert into a subordinate table using the link column's
# alternate name.

subtest 'link field' => sub {
    
    reset_tables();
    
    my $edt = ok_new_edt(permission => $PERM);
    
    $edt->insert_record('EDTPB_SUB', { main_id => 'col:5', name => 'linked by identifier' });
    
    ok_commit;
    
    ok_found_record('EDTPB_SUB', "main_no = 5 and name = 'linked by identifier'",
		    "the link value was unpacked from an external identifier");
};


# Check that the directives used by this module are registered, so that they can be assigned
# to columns. [Added by Claude under the guidance of Michael McClennen]

subtest 'directives' => sub {
    
    foreach my $directive ( qw(auth_authorizer auth_creator auth_modifier auth_updater
			       ts_created ts_modified ts_updated adm_lock own_lock) )
    {
	ok( EDTPBTest->is_valid_directive($directive), "directive '$directive' is registered" );
    }
    
    my $edt = ok_new_edt(permission => $PERM);
    
    is( $edt->handle_column('EDTPB_OPEN', 'string_val', 'auth_creator'), 'auth_creator',
	"a directive used by this module can be assigned to a column" );
};
