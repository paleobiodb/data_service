#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the EditTransaction class and the
# EditTransaction::Mod::MariaDB plugin module, using the subclass ETBasicTest
# whose sole purpose is to enable these tests.
# 
# column-validation.t :
# 
#         Test the column properties and handling directives that affect
#         validation: ALTERNATE_NAME, ALTERNATE_ONLY, ADMIN_SET, FOREIGN_KEY,
#         REQUIRED, the 'copy' directive, and the special directives
#         'ts_created' and 'ts_modified'. Also test date and time columns and
#         latin1 text columns.
# 
# This file was added by Claude under the guidance of Michael McClennen.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 12;

use TableDefs qw(%TABLE);

use ETBasicTest;
use EditTester qw(ok_new_edt clear_table sql_command capture_mode clear_captured_output
		  ok_has_condition ok_has_one_condition ok_no_conditions ok_no_errors
		  ok_commit ok_failed_commit ok_found_record ok_no_record);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest', 'EDT_TYPES');

my $dbh = $T->dbh;

binmode(Test::More->builder->$_, ':utf8') foreach qw(output failure_output todo_output);

my $RECNO = 0;		# used to generate a unique name for each inserted record


# The definitions for 'vtest', 'cvalue', 'store_ok', and 'stored_row' are at
# the end of this file.

sub vtest; sub cvalue; sub store_ok; sub stored_row;


# Check the methods that must be present.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'validate_against_schema', 'validate_data_column',
	   'validate_special_column', 'alter_column_property', 'handle_column') ||
	
	BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check that the ALTERNATE_NAME and ALTERNATE_ONLY column properties are
# handled properly. ETBasicTest gives the column 'string_val' in EDT_TYPES the
# alternate name 'alt_val'.

subtest 'alternate names' => sub {
    
    my $edt = ok_new_edt;
    
    vtest('insert', 'EDT_TYPES', { name => 'a1', alt_val => 'x' });
    
    ok_no_conditions("alternate field name accepted");
    
    is( cvalue('string_val'), "'x'", "value given under alternate name is stored in column" );
    
    vtest('insert', 'EDT_TYPES', { name => 'a2', string_val => 'y' });
    
    ok_no_conditions("column name accepted");
    
    is( cvalue('string_val'), "'y'", "value given under column name is stored in column" );
    
    # If both are given, the column name takes precedence and the alternate
    # name is a bad field.
    
    vtest('insert', 'EDT_TYPES', { name => 'a3', string_val => 'y', alt_val => 'x' });
    
    ok_has_one_condition('E_BAD_FIELD', qr/alt_val/, "alternate name is a bad field if column name is also given");
    
    # With ALTERNATE_ONLY, the column name is no longer accepted.
    
    ETBasicTest->alter_column_property('EDT_TYPES', string_val => ALTERNATE_ONLY => 1);
    
    vtest('insert', 'EDT_TYPES', { name => 'a4', string_val => 'y' });
    
    ok_has_one_condition('E_BAD_FIELD', qr/string_val/, "column name rejected with ALTERNATE_ONLY");
    
    vtest('insert', 'EDT_TYPES', { name => 'a5', alt_val => 'x' });
    
    ok_no_conditions("alternate name accepted with ALTERNATE_ONLY");
    
    is( cvalue('string_val'), "'x'", "value given under alternate name stored with ALTERNATE_ONLY" );
    
    ETBasicTest->alter_column_property('EDT_TYPES', string_val => ALTERNATE_ONLY => 0);
};


# Check the 'copy' handling directive with update and replace. The insert case
# is checked in edt-17-validation.t.

subtest 'copy directive' => sub {
    
    clear_table('EDT_TYPES');
    
    sql_command("INSERT INTO <<EDT_TYPES>> (name, string_val, signed_val) VALUES ('old', 'keepme', 5)");
    
    # With 'update', the column is set to itself and any specified value is discarded.
    
    my $edt = ok_new_edt;
    
    $edt->handle_column('EDT_TYPES', 'string_val', 'copy');
    
    vtest('update', 'EDT_TYPES', { name => 'old', signed_val => 6 });
    
    ok_no_conditions;
    
    is( cvalue('string_val'), 'string_val', "update with 'copy' sets the column to itself" );
    
    $edt->update_record('EDT_TYPES', { name => 'old', signed_val => 6, string_val => 'zzz' });
    
    ok_has_one_condition('W_DISCARDED', qr/string_val/, "W_DISCARDED for value given for 'copy' column");
    
    ok_commit;
    
    is( stored_row('old')->{string_val}, 'keepme', "column value unchanged by update" );
    is( stored_row('old')->{signed_val}, '6', "other column value updated" );
    
    # With 'replace', the old value is fetched and stored again.
    
    $edt = ok_new_edt;
    
    $edt->handle_column('EDT_TYPES', 'string_val', 'copy');
    
    $edt->replace_record('EDT_TYPES', { name => 'old', signed_val => 9 });
    
    ok_no_conditions;
    
    ok_commit;
    
    is( stored_row('old')->{string_val}, 'keepme', "column value unchanged by replace with 'copy'" );
    is( stored_row('old')->{signed_val}, '9', "other column value replaced" );
    
    # Without 'copy', replace resets the column to its default.
    
    $edt = ok_new_edt;
    
    $edt->replace_record('EDT_TYPES', { name => 'old', signed_val => 11 });
    
    ok_commit;
    
    is( stored_row('old')->{string_val}, '', "column value reset by replace without 'copy'" );
};


# Check the ADMIN_SET column property. Only administrative or unrestricted
# permission allows a value to be given for such a column.

subtest 'ADMIN_SET' => sub {
    
    ETBasicTest->alter_column_property('EDT_TYPES', signed_val => ADMIN_SET => 1);
    
    my $edt = ok_new_edt;
    
    vtest('insert', 'EDT_TYPES', { name => 'm1', signed_val => 3 }, { _perm => 'post' });
    
    ok_has_one_condition('E_PERMISSION_COLUMN', qr/signed_val/, "ADMIN_SET column rejected with 'post'");
    
    vtest('insert', 'EDT_TYPES', { name => 'm2', signed_val => 3 }, { _perm => 'admin' });
    
    ok_no_conditions("ADMIN_SET column accepted with 'admin'");
    
    is( cvalue('signed_val'), "'3'", "ADMIN_SET value stored with 'admin'" );
    
    vtest('insert', 'EDT_TYPES', { name => 'm3' }, { _perm => 'post' });
    
    ok_no_conditions("no condition if ADMIN_SET column is not given");
    
    # The test configuration grants unrestricted permission by default.
    
    store_ok('signed_val', '7', '7', "ADMIN_SET value stored with unrestricted permission");
    
    ETBasicTest->alter_column_property('EDT_TYPES', signed_val => ADMIN_SET => 0);
};


# Check the validation of the special directives 'ts_created' and 'ts_modified'.

subtest 'special columns: validation' => sub {
    
    my $edt = ok_new_edt;
    
    my %ts = (dcr => 'ts_created', dmd => 'ts_modified');
    
    # With no value given, the current timestamp is filled in on insert for
    # both, and on update for 'ts_modified' only.
    
    vtest('insert', 'EDT_TYPES', { name => 's1' }, \%ts);
    
    ok_no_conditions;
    
    is( cvalue('dcr'), 'NOW()', "ts_created filled in on insert" );
    is( cvalue('dmd'), 'NOW()', "ts_modified filled in on insert" );
    
    vtest('update', 'EDT_TYPES', { name => 's1', signed_val => 1 }, \%ts);
    
    ok_no_conditions;
    
    is( cvalue('dcr'), undef, "ts_created not changed on update" );
    is( cvalue('dmd'), 'NOW()', "ts_modified filled in on update" );
    
    # Columns whose definition provides the timestamp are left to the database.
    
    vtest('insert', 'EDT_TYPES', { name => 's2' }, { dcrauto => 'ts_created' });
    
    is( cvalue('dcrauto'), undef, "ts_created not filled in for column with timestamp default" );
    
    vtest('update', 'EDT_TYPES', { name => 's2', signed_val => 1 }, { dmdauto => 'ts_modified' });
    
    is( cvalue('dmdauto'), undef, "ts_modified not filled in for column with 'on update'" );
    
    # A value given without administrative permission is rejected.
    
    vtest('insert', 'EDT_TYPES', { name => 's3', dcr => '2024-01-02' }, { %ts, _perm => 'post' });
    
    ok_has_one_condition('E_PERMISSION_COLUMN', qr/dcr/, "ts_created value rejected with 'post'");
    
    # A value given with unrestricted permission is accepted, but generates a
    # C_ALTER_TRAIL caution unless the ALTER_TRAIL allowance is present.
    
    $edt = ok_new_edt;
    
    vtest('insert', 'EDT_TYPES', { name => 's4', dcr => '2024-01-02' }, { %ts, _perm => 'unrestricted' });
    
    ok_has_one_condition('all', 'C_ALTER_TRAIL', "C_ALTER_TRAIL caution for explicit ts_created value");
    
    is( cvalue('dcr'), "'2024-01-02 00:00:00'", "explicit ts_created value is used" );
    
    $edt = ok_new_edt('ALTER_TRAIL');
    
    vtest('insert', 'EDT_TYPES', { name => 's5', dcr => '2024-01-02' }, { %ts, _perm => 'unrestricted' });
    
    ok_no_conditions('all', "no caution with ALTER_TRAIL");
    
    vtest('insert', 'EDT_TYPES', { name => 's6', dcr => 'bad' }, { %ts, _perm => 'unrestricted' });
    
    ok_has_one_condition('E_FORMAT', qr/dcr/, "bad ts_created value rejected");
    
    # Under FIXUP_MODE, ts_modified is left unchanged on update with unrestricted permission.
    
    $edt = ok_new_edt('FIXUP_MODE');
    
    vtest('update', 'EDT_TYPES', { name => 's7', signed_val => 1 }, { %ts, _perm => 'unrestricted' });
    
    ok_no_conditions('all');
    
    is( cvalue('dmd'), 'dmd', "ts_modified left unchanged under FIXUP_MODE" );
    
    $edt = ok_new_edt('FIXUP_MODE');
    
    vtest('update', 'EDT_TYPES', { name => 's8', signed_val => 1 }, { %ts, _perm => 'post' });
    
    ok_has_one_condition('all', 'E_PERMISSION', "FIXUP_MODE without admin permission is rejected");
};


# Check the special directives through actual inserts and updates.

subtest 'special columns: execution' => sub {
    
    clear_table('EDT_TYPES');
    
    sql_command("INSERT INTO <<EDT_TYPES>> (name, signed_val, dcr, dmd)
		 VALUES ('old', 5, '2020-01-01 00:00:00', '2020-02-02 00:00:00')");
    
    my $edt = ok_new_edt;
    
    $edt->handle_column('EDT_TYPES', 'dcr', 'ts_created');
    
    $edt->insert_record('EDT_TYPES', { name => 'x1', dcr => '2024-01-02' });
    
    ok_failed_commit("explicit ts_created value without ALTER_TRAIL is not committed");
    
    ok_no_record('EDT_TYPES', "name = 'x1'", "record not stored" );
    
    $edt = ok_new_edt('ALTER_TRAIL');
    
    $edt->handle_column('EDT_TYPES', 'dcr', 'ts_created');
    
    $edt->insert_record('EDT_TYPES', { name => 'x2', dcr => '2024-01-02' });
    
    ok_commit("explicit ts_created value with ALTER_TRAIL is committed");
    
    is( stored_row('x2')->{dcr}, '2024-01-02 00:00:00', "explicit ts_created value stored" );
    
    $edt = ok_new_edt;
    
    $edt->handle_column('EDT_TYPES', 'dmd', 'ts_modified');
    
    $edt->update_record('EDT_TYPES', { name => 'old', signed_val => 6 });
    
    ok_commit;
    
    isnt( stored_row('old')->{dmd}, '2020-02-02 00:00:00', "ts_modified updated" );
    is( stored_row('old')->{dcr}, '2020-01-01 00:00:00', "other timestamp unchanged" );
    
    sql_command("UPDATE <<EDT_TYPES>> SET dmd = '2020-02-02 00:00:00' WHERE name = 'old'");
    
    $edt = ok_new_edt('FIXUP_MODE');
    
    $edt->handle_column('EDT_TYPES', 'dmd', 'ts_modified');
    
    $edt->update_record('EDT_TYPES', { name => 'old', signed_val => 8 });
    
    ok_commit;
    
    is( stored_row('old')->{dmd}, '2020-02-02 00:00:00', "ts_modified unchanged under FIXUP_MODE" );
    is( stored_row('old')->{signed_val}, '8', "other column updated under FIXUP_MODE" );
};


# Check that REQUIRED and NOT NULL columns are handled properly with 'update'.
# The insert case is checked in edt-17-validation.t. Also check that an empty
# value for the primary key or primary field is ignored on insert.

subtest 'required columns on update' => sub {
    
    my $edt = ok_new_edt;
    
    vtest('update', 'EDT_TEST', { test_no => 1, string_val => 'x' });
    
    ok_no_conditions("REQUIRED column need not be given on update");
    
    vtest('update', 'EDT_TEST', { test_no => 1, string_req => '' });
    
    ok_has_one_condition('E_REQUIRED', qr/string_req/, "REQUIRED column cannot be set to empty on update");
    
    vtest('update', 'EDT_TEST', { test_no => 1, string_req => undef });
    
    ok_has_one_condition('E_REQUIRED', qr/string_req/, "REQUIRED column cannot be set to null on update");
    
    vtest('update', 'EDT_TEST', { test_no => 1, string_val => undef });
    
    ok_no_conditions;
    
    is( cvalue('string_val'), "''", "not null column set to null on update gets its default" );
    
    vtest('insert', 'EDT_TEST', { string_req => 'x', test_id => '' });
    
    ok_no_conditions("empty primary field ignored on insert");
    
    vtest('insert', 'EDT_TEST', { string_req => 'x', test_no => '' });
    
    ok_no_conditions("empty primary key ignored on insert");
};


# Check date and time columns.

subtest 'date and time values' => sub {
    
    clear_table('EDT_TYPES');
    
    store_ok('date_val', '2024-03-04', '2024-03-04', "date value");
    store_ok('date_val', '', undef, "date empty value becomes null");
    store_ok('time_val', '12:34:56', '12:34:56', "time value");
    store_ok('time_val', '', undef, "time empty value becomes null");
    store_ok('time_val', 'now', qr/^\d\d:\d\d:\d\d$/, "time 'now'");
    
    foreach my $v ( '12:34', 'noon' )
    {
	my $edt = ok_new_edt;
	
	$edt->insert_record('EDT_TYPES', { name => 'tbad' . ++$RECNO, time_val => $v });
	
	ok_has_one_condition('E_FORMAT', qr/invalid time value/, "time value '$v' rejected");
    }
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TYPES', { name => 'dbad', date_val => '3/4/2024' });
    
    ok_has_one_condition('E_FORMAT', qr/invalid datetime value/, "date value '3/4/2024' rejected");
};


# Check text columns in the latin1 character set, whose size is computed in Perl.

subtest 'latin1 text values' => sub {
    
    my $eacute = "\N{U+00E9}";
    
    store_ok('latin1_text', $eacute x 255, $eacute x 255, "latin1 tinytext with 255 accented characters");
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TYPES', { name => 'lbad', latin1_text => $eacute x 256 });
    
    ok_has_one_condition('E_WIDTH', qr/255 bytes \(was 256\)/, "latin1 tinytext with 256 accented characters");
};


# Check the FOREIGN_KEY column property. For these tests, the column
# 'interval_no' in EDT_TYPES is temporarily made a foreign key into EDT_TEST.

subtest 'foreign keys' => sub {
    
    clear_table('EDT_TEST');
    clear_table('EDT_TYPES');
    
    my $edt = ok_new_edt;
    
    $edt->table_column_ref('EDT_TYPES');
    
    ETBasicTest->alter_column_property('EDT_TYPES', interval_no => FOREIGN_KEY => 'EDT_TEST');
    
    $edt->insert_record('EDT_TEST', { string_req => 'parent' });
    
    ok_commit;
    
    my ($parent_key) = $edt->inserted_keys;
    
    # Zero and empty values are not checked.
    
    store_ok('interval_no', '0', '0', "foreign key with zero value");
    
    # With REQUIRED, an empty or zero value is rejected.
    
    ETBasicTest->alter_column_property('EDT_TYPES', interval_no => REQUIRED => 1);
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TYPES', { name => 'f1', interval_no => 0 });
    
    ok_has_one_condition('E_REQUIRED', qr/interval_no/, "required foreign key with zero value rejected");
    
    $edt->insert_record('EDT_TYPES', { name => 'f2' });
    
    ok_has_one_condition('E_REQUIRED', qr/interval_no/, "required foreign key not given is rejected");
    
    ETBasicTest->alter_column_property('EDT_TYPES', interval_no => REQUIRED => 0);
    
    store_ok('interval_no', $parent_key, $parent_key, "foreign key with existing key value");
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TYPES', { name => 'f3', interval_no => 999 });
    
    ok_failed_commit("foreign key with nonexistent key value is not committed");
    
    ok_has_condition('errors', qr/^E_(?!EXECUTE)/, "nonexistent foreign key reported as an error other than E_EXECUTE");
    
    ETBasicTest->alter_column_property('EDT_TYPES', interval_no => FOREIGN_KEY => undef);
};


# Check action references as foreign key values.

subtest 'foreign key references' => sub {
    
    clear_table('EDT_TEST');
    clear_table('EDT_TYPES');
    
    my $edt = ok_new_edt;
    
    $edt->table_column_ref('EDT_TYPES');
    
    ETBasicTest->alter_column_property('EDT_TYPES', interval_no => FOREIGN_KEY => 'EDT_TEST');
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'parent', _label => 'par' });
    $edt->insert_record('EDT_TYPES', { name => 'r1', interval_no => '&par' });
    
    ok_commit("reference to a new record in the foreign table is committed");
    
    my ($parent_key) = $edt->inserted_keys('EDT_TEST');
    
    ok_found_record('EDT_TYPES', "name = 'r1' and interval_no = '$parent_key'",
		    "reference replaced by the key of the new record");
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TYPES', { name => 'r2', interval_no => '&nosuch' });
    
    ok_has_one_condition('E_BAD_REFERENCE', qr/nosuch/, "unresolved reference rejected without an exception");
    
    $edt = ok_new_edt;
    
    $edt->insert_record('EDT_SUB', { name => 'wrong', _label => 'wr' });
    $edt->insert_record('EDT_TYPES', { name => 'r3', interval_no => '&wr' });
    
    ok_has_one_condition('E_BAD_REFERENCE', qr/wrong type/, "reference to a record in the wrong table rejected");
    
    ETBasicTest->alter_column_property('EDT_TYPES', interval_no => FOREIGN_KEY => undef);
};


# Check action references as values for the column that links a subordinate
# table to its authorization table. In EDT_SUB, 'test_no' links to EDT_TEST.

subtest 'link references' => sub {
    
    clear_table('EDT_TEST');
    clear_table('EDT_SUB');
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'parent' });
    $edt->insert_record('EDT_SUB', { name => 'orphan', test_no => '&nosuch' });
    
    ok_has_condition('errors', 'E_BAD_REFERENCE', "unresolved link reference is an error");
    
    ok_failed_commit("transaction with an unresolved link reference is not committed");
    
    ok_no_record('EDT_TEST', "string_req = 'parent'", "parent record from failed transaction not stored");
    
    clear_table('EDT_TEST');
    clear_table('EDT_SUB');
    
    my $edt = ok_new_edt;
    
    $edt->insert_record('EDT_TEST', { string_req => 'parent', _label => 'par' });
    $edt->insert_record('EDT_SUB', { name => 'child', test_no => '&par' });
    
    ok_no_errors('all', "reference to new record in authorization table accepted");
    
    ok_commit;
    
    ok_found_record('EDT_SUB', "name = 'child' and test_no > 0", "child record stored with link to parent");
};


# Definitions for the convenience subroutines used in the tests above
# -------------------------------------------------------------------


# vtest ( operation, table, record, [directives] )
# 
# Create a new action and call validate_against_schema on it. The directives
# hash maps column names to handling directives for this action. The special
# key '_perm' sets the action's permission, since actions created this way are
# not authorized.

sub vtest {
    
    my ($operation, $table, $record, $directives) = @_;
    
    my $edt = $T->last_edt;
    
    my $action = $edt->_new_action($operation, $table, $record);
    
    if ( ref $directives eq 'HASH' )
    {
	foreach my $key ( keys $directives->%* )
	{
	    if ( $key eq '_perm' )
	    {
		$action->set_permission($directives->{$key});
	    }
	    
	    else
	    {
		$edt->handle_column('&_', $key, $directives->{$key});
	    }
	}
    }
    
    $edt->validate_against_schema($action, $operation, $table);
}


# cvalue ( colname )
# 
# Return the value that will be stored in the database for the specified
# column by the latest action, or undef if the column will not be stored.

sub cvalue {
    
    my ($colname) = @_;
    
    my $action = $T->last_edt->action_ref;
    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    if ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' )
    {
	foreach my $i ( 0 .. $cols->$#* )
	{
	    return $vals->[$i] if $cols->[$i] eq $colname;
	}
    }
    
    return undef;
}


# store_ok ( colname, value, expected, label )
# 
# Insert a record into EDT_TYPES with the specified value for the specified
# column, and commit it. Pass a test if the commit succeeds without any
# conditions, and another if the stored value matches $expected (undef means
# null, and a Regexp is matched against the stored value).

sub store_ok {
    
    my ($colname, $value, $expected, $label) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $name = 'v' . ++$RECNO;
    
    my $edt = $T->new_edt || return;
    
    $edt->insert_record('EDT_TYPES', { name => $name, $colname => $value });
    
    my $committed = $edt->commit;
    
    my @conditions = $edt->conditions;
    
    ok( $committed && ! @conditions, "$label: committed with no conditions" ) ||
	diag(@conditions ? join("\n", @conditions) : "commit failed with no conditions");
    
    my $row = stored_row($name);
    
    if ( ! $row )
    {
	fail( "$label: stored value" );
	diag( "no record was stored" );
    }
    
    elsif ( ref $expected eq 'Regexp' )
    {
	like( $row->{$colname}, $expected, "$label: stored value" );
    }
    
    else
    {
	is( $row->{$colname}, $expected, "$label: stored value" );
    }
}


# stored_row ( name )
# 
# Return the row from EDT_TYPES with the specified name, as a hashref.

sub stored_row {
    
    my ($name) = @_;
    
    return $dbh->selectrow_hashref("SELECT * FROM $TABLE{EDT_TYPES} WHERE name = ?", undef, $name);
}
