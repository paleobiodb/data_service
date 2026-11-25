#
# Paleobiology Database library modules
# -------------------------------------
#
# This file contains unit tests for library modules that are used by the EditTransaction
# client modules and by the data service.
#
# lib-06-permissions.t : Test Permissions.pm, which looks up a user's login session and
#                        decides what they may do with each table and record. The
#                        EditTransaction module PaleoBioDB and the data service use it.
#
# The sessions are the standard test sessions in the test session_data table. The table
# permissions are written to the test table_perms table, under table names used only by
# these tests. The record tests use the table EDTPB_MAIN in the test database.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 11;

use TableDefs qw(%TABLE set_table_name set_table_property);

use EDTPBTest;
use PBDBTester;
use EditTester qw(clear_table sql_command sql_fetchrow);

use Permissions;


# Establish the tester and select the test tables.

my $T = PBDBTester->new('EDTPBTest');

EDTPBTest->select_test_tables;

my $dbh = $T->dbh;


# Table names used only by these tests, each with one permission property. No database
# tables are needed for them, because Permissions only reads their properties.

my %PROP = ( LIB06_POST_LOGGED_IN => [ CAN_POST => 'LOGGED_IN' ],
	     LIB06_POST_MEMBERS => [ CAN_POST => 'MEMBERS' ],
	     LIB06_POST_AUTHORIZED => [ CAN_POST => 'AUTHORIZED' ],
	     LIB06_POST_ALL => [ CAN_POST => 'ALL' ],
	     LIB06_VIEW_MEMBERS => [ CAN_VIEW => 'MEMBERS' ],
	     LIB06_VIEW_AUTHORIZED => [ CAN_VIEW => 'AUTHORIZED' ],
	     LIB06_VIEW_ANY => [ CAN_VIEW => 'ANY' ],
	     LIB06_MODIFY_LOGGED_IN => [ CAN_MODIFY => 'LOGGED_IN' ],
	     LIB06_MODIFY_AUTHORIZED => [ CAN_MODIFY => 'AUTHORIZED' ],
	     LIB06_BYAUTH => [ CAN_POST => 'AUTHORIZED', BY_AUTHORIZER => 1 ],
	     LIB06_OPEN => [ ] );

foreach my $t ( sort keys %PROP )
{
    set_table_name($t => lc $t);
    my @props = @{$PROP{$t}};
    set_table_property($t, splice(@props, 0, 2)) while @props;
}


# Return a new Permissions object for one of the standard test sessions, or for no session
# if the name is empty.

sub perm {
    
    my ($name, $table) = @_;
    
    return Permissions->new($dbh, $name ? "SESSION-$name" : undef, $table);
}


# Return the permissions that a session has on a table, as a sorted string.

sub tp {
    
    my ($name, $table) = @_;
    
    my $tp = perm($name)->get_table_permissions($table);
    
    return join(',', sort grep { $tp->{$_} } keys %$tp);
}


# The standard test sessions, and the EDTPB_MAIN records: record 1 was entered by ENTERER
# (39997) under AUTHORIZER (39998), and record 2 by OTHER (39995) under 39999.

subtest 'setup' => sub {
    
    $T->establish_session_data;
    
    EDTPBTest->establish_test_tables($dbh);
    
    clear_table('EDTPB_MAIN');
    
    sql_command("INSERT INTO <<EDTPB_MAIN>> (main_no, authorizer_no, enterer_no, string_req)
		VALUES (1, 39998, 39997, 'lib06 one'), (2, 39999, 39995, 'lib06 two')");
    
    $T->clear_specific_permissions($_) foreach grep { /^LIB06/ } keys %PROP;
    
    my ($count) = sql_fetchrow("SELECT count(*) FROM <<SESSION_DATA>> WHERE session_id like 'SESSION-%'");
    
    is( $count, 8, "the standard sessions are in place" );
};


# A Permissions object holds the user's identity from the session record.

subtest 'sessions' => sub {
    
    my %expected = ( AUTHORIZER => 'authorizer 39998 39998 USERID-AUTHORIZER',
		     ENTERER => 'enterer 39998 39997 USERID-ENTERER',
		     STUDENT => 'student 39999 39996 USERID-STUDENT',
		     GUEST => 'guest 0 0 USERID-GUEST',
		     UNAUTH => 'guest 0 39994 USERID-UNAUTH',
		     SUPERUSER => 'authorizer 39999 39999 USERID-SUPERUSER' );
    
    foreach my $name ( sort keys %expected )
    {
	my $p = perm($name);
	
	isa_ok( $p, 'Permissions', "SESSION-$name" );
	is( join(' ', $p->role, $p->authorizer_no, $p->enterer_no, $p->user_id), $expected{$name},
	    "the role and identity of SESSION-$name" );
    }
    
    ok( perm('SUPERUSER')->is_superuser, "SESSION-SUPERUSER is a superuser" );
    ok( ! perm('AUTHORIZER')->is_superuser, "SESSION-AUTHORIZER is not" );
    
    # A user without an authorizer is a guest, and cannot be a superuser.
    
    sql_command("INSERT INTO <<SESSION_DATA>> (session_id, user_id, authorizer_no, enterer_no, role,
		expire_days, superuser) VALUES ('LIB06-SUPERGUEST', 'USERID-LIB06', 0, 39994,
		'authorizer', 30000, 1)");
    
    my $p = Permissions->new($dbh, 'LIB06-SUPERGUEST');
    
    is( $p->role, 'guest', "a session without an authorizer has the role 'guest'" );
    ok( ! $p->is_superuser, "and is not a superuser" );
    
    # Without a session, or with one that does not exist, there are no permissions.
    
    foreach my $id ( undef, 'NO-SUCH-SESSION' )
    {
	my $p = Permissions->new($dbh, $id);
	
	is( join(' ', $p->role, $p->authorizer_no, $p->enterer_no, "'" . $p->user_id . "'"), "none 0 0 ''",
	    "no login for " . ($id // 'no session') );
    }
    
    is( Permissions->no_login($dbh, 'EDTPB_MAIN')->{table_permission}{EDTPB_MAIN} && 1, 1,
	"no_login with a table has an empty permission hash for it" );
    
    eval { Permissions->new(undef, 'SESSION-ENTERER') };
    
    like( $@, qr{request or dbh is required}, "a database handle is required" );
    
    eval { Permissions->new($dbh, 'SESSION-ENTERER', 'NO_SUCH_TABLE') };
    
    like( $@, qr{unknown table 'NO_SUCH_TABLE'}, "an unknown table is rejected" );
    
    # Each use of a session updates its record date.
    
    sql_command("UPDATE <<SESSION_DATA>> SET record_date = now() - interval 1 day
		WHERE session_id = 'SESSION-ENTERER'");
    
    perm('ENTERER');
    
    my ($recent) = sql_fetchrow("SELECT record_date > now() - interval 1 minute FROM <<SESSION_DATA>>
		WHERE session_id = 'SESSION-ENTERER'");
    
    ok( $recent, "the session's record date was updated" );
};


# A session that has not been used for more than expire_days days has expired. It gives no
# permissions, and stays expired.

subtest 'expired sessions' => sub {
    
    sql_command("INSERT INTO <<SESSION_DATA>> (session_id, user_id, authorizer_no, enterer_no, role,
		expire_days, record_date) VALUES ('LIB06-EXPIRED', 'USERID-LIB06', 39998, 39997,
		'enterer', 1, now() - interval 5 day)");
    
    my $p = Permissions->new($dbh, 'LIB06-EXPIRED');
    
    ok( $p->{expired}, "the session has expired" );
    is( "$p->{role} $p->{enterer_no}", 'none 0', "and gives no permissions" );
    
    $p = Permissions->new($dbh, 'LIB06-EXPIRED');
    
    ok( $p->{expired}, "the session is still expired the next time" );
    is( $p->{role}, 'none', "and still gives no permissions" );
};


# Without specific table permissions, the permissions come from the table's CAN_POST,
# CAN_VIEW, and CAN_MODIFY properties: LOGGED_IN means anyone with a login, MEMBERS means
# a database member (with an enterer_no), AUTHORIZED means a member with an authorizer,
# and ALL or ANY means everyone. A table with none of these properties is unrestricted.

subtest 'default permissions' => sub {
    
    my %expected = (
	LIB06_POST_LOGGED_IN => { '' => '', GUEST => 'post', UNAUTH => 'post', ENTERER => 'post' },
	LIB06_POST_MEMBERS => { '' => '', GUEST => '', UNAUTH => 'post', ENTERER => 'post' },
	LIB06_POST_AUTHORIZED => { '' => '', GUEST => '', UNAUTH => '', ENTERER => 'post' },
	LIB06_POST_ALL => { '' => 'post', GUEST => 'post', UNAUTH => 'post', ENTERER => 'post' },
	LIB06_VIEW_MEMBERS => { '' => '', GUEST => '', UNAUTH => 'view', ENTERER => 'view' },
	LIB06_VIEW_AUTHORIZED => { '' => '', GUEST => '', UNAUTH => '', ENTERER => 'view' },
	LIB06_VIEW_ANY => { '' => 'view', GUEST => 'view', UNAUTH => 'view', ENTERER => 'view' },
	LIB06_MODIFY_LOGGED_IN => { '' => '', GUEST => 'modify', UNAUTH => 'modify,post,view',
				    ENTERER => 'modify,post,view' },
	LIB06_MODIFY_AUTHORIZED => { '' => '', GUEST => '', UNAUTH => '', ENTERER => 'modify,post,view' },
	LIB06_OPEN => { '' => 'unrestricted', GUEST => 'unrestricted', ENTERER => 'unrestricted' } );
    
    foreach my $table ( sort keys %expected )
    {
	foreach my $name ( sort keys $expected{$table}->%* )
	{
	    is( tp($name, $table), $expected{$table}{$name},
		"$table for " . ($name || 'no login') );
	}
    }
    
    is( tp('ENTERER', 'LIB06_BYAUTH'), 'by_authorizer,post', "BY_AUTHORIZER adds 'by_authorizer'" );
    
    # AUTHORIZED requires both an authorizer_no and an enterer_no. No test session has an
    # authorizer without an enterer, so this object is made directly.
    
    my $no_enterer = bless { authorizer_no => 39998, enterer_no => 0, user_id => 'USERID-X',
			     role => 'guest', dbh => $dbh }, 'Permissions';
    
    ok( ! $no_enterer->default_table_permissions('LIB06_POST_AUTHORIZED')->{post},
	"CAN_POST => 'AUTHORIZED' requires an enterer_no" );
    
    my $p = perm('ENTERER');
    
    $p->get_table_permissions('LIB06_POST_MEMBERS');
    
    is( $p->{auth_diag}{LIB06_POST_MEMBERS}, 'MEMBERS', "the source of the permission is recorded" );
};


# Specific permissions in the table_perms table replace the defaults. 'modify' also gives
# 'post' and 'view'.

subtest 'specific permissions' => sub {
    
    $T->set_specific_permission('LIB06_POST_AUTHORIZED', perm('UNAUTH'), 'view');
    $T->set_specific_permission('LIB06_POST_AUTHORIZED', perm('STUDENT'), 'modify');
    $T->set_specific_permission('LIB06_POST_AUTHORIZED', perm('OTHER'), 'none');
    $T->set_specific_permission('LIB06_POST_AUTHORIZED', perm('WITH-ADMIN'), 'admin');
    
    is( tp('UNAUTH', 'LIB06_POST_AUTHORIZED'), 'view', "a specific 'view'" );
    is( tp('STUDENT', 'LIB06_POST_AUTHORIZED'), 'modify,post,view', "a specific 'modify'" );
    is( tp('OTHER', 'LIB06_POST_AUTHORIZED'), 'none', "a specific 'none'" );
    is( tp('WITH-ADMIN', 'LIB06_POST_AUTHORIZED'), 'admin', "a specific 'admin'" );
    is( tp('ENTERER', 'LIB06_POST_AUTHORIZED'), 'post', "others have the default" );
    
    # A table given to 'new' has its permissions looked up at once.
    
    my $p = perm('STUDENT', 'LIB06_POST_AUTHORIZED');
    
    is( join(',', sort keys $p->{table_permission}{LIB06_POST_AUTHORIZED}->%*), 'modify,post,view',
	"the permissions of a table given to 'new'" );
    is( $p->{auth_diag}{LIB06_POST_AUTHORIZED}, 'TABLE_PERMS', "come from table_perms" );
    
    # The permissions are cached, until they are cleared.
    
    $T->set_specific_permission('LIB06_POST_AUTHORIZED', perm('STUDENT'), 'view');
    
    is( join(',', sort keys $p->get_table_permissions('LIB06_POST_AUTHORIZED')->%*), 'modify,post,view',
	"the cached permissions are used" );
    
    $p->clear_cached_permissions('LIB06_POST_AUTHORIZED');
    
    is( join(',', sort keys $p->get_table_permissions('LIB06_POST_AUTHORIZED')->%*), 'view',
	"after clearing one table, they are looked up again" );
    
    $p->get_table_permissions('LIB06_OPEN');
    $p->clear_cached_permissions;
    
    is( scalar(keys $p->{table_permission}->%*), 0, "clearing all of them" );
};


# check_table_permission returns the permission if the user has it, 'admin' for an
# administrator or superuser, 'own' for 'view' to a user who may only post, and 'none'
# otherwise.

subtest 'check_table_permission' => sub {
    
    # STUDENT has 'modify' on LIB06_POST_AUTHORIZED.
    
    $T->set_specific_permission('LIB06_POST_AUTHORIZED', perm('STUDENT'), 'modify');
    
    my %cases = (
	"SUPERUSER LIB06_POST_AUTHORIZED post" => 'admin',
	"SUPERUSER LIB06_POST_AUTHORIZED view" => 'view',
	"WITH-ADMIN LIB06_POST_AUTHORIZED post" => 'admin',
	"OTHER LIB06_POST_AUTHORIZED post" => 'none',
	"ENTERER LIB06_POST_AUTHORIZED post" => 'post',
	"ENTERER LIB06_POST_AUTHORIZED view" => 'own',
	"ENTERER LIB06_VIEW_ANY view" => 'view',
	"ENTERER LIB06_VIEW_ANY post" => 'none',	"GUEST LIB06_POST_AUTHORIZED post" => 'none',
	"STUDENT LIB06_POST_AUTHORIZED modify" => 'modify',
	"ENTERER LIB06_POST_AUTHORIZED modify" => 'none',
	" LIB06_POST_ALL post" => 'post' );
    
    foreach my $case ( sort keys %cases )
    {
	my ($name, $table, $requested) = split / /, $case;
	
	is( perm($name)->check_table_permission($table, $requested), $cases{$case},
	    "'$requested' on $table for " . ($name || 'no login') );
    }
    
    my $p = perm('ENTERER');
    
    eval { $p->check_table_permission('LIB06_OPEN') };
    like( $@, qr{no permission specified}, "a permission is required" );
    
    eval { $p->check_table_permission(undef, 'view') };
    like( $@, qr{no table name specified}, "a table is required" );    eval { $p->check_table_permission('LIB06_OPEN', 'fly') };
    like( $@, qr{bad permission 'fly'}, "an unknown permission is rejected" );
    
    eval { $p->check_table_permission('LIB06_OPEN', 'edit') };
    like( $@, qr{bad permission 'edit'}, "'edit' is no longer a permission; 'modify' replaces it" );
};


# check_record_permission decides what a user may do with one record, from the table
# permissions and the record's authorizer, enterer, and locks.

subtest 'check_record_permission' => sub {
    
    # STUDENT has 'modify' on LIB06_POST_AUTHORIZED, and WITH-ADMIN has 'admin'.
    
    $T->set_specific_permission('LIB06_POST_AUTHORIZED', perm('STUDENT'), 'modify');
    
    my $mine = { authorizer_no => 39998, enterer_no => 39997 };
    my $theirs = { authorizer_no => 39999, enterer_no => 39995 };
    
    my @cases = (
	[ 'ENTERER', 'LIB06_POST_AUTHORIZED', 'modify', $mine, 'modify', "the enterer may edit" ],
	[ 'ENTERER', 'LIB06_POST_AUTHORIZED', 'delete', $mine, 'delete', "and delete" ],
	[ 'ENTERER', 'LIB06_POST_AUTHORIZED', 'view', $mine, 'view', "and view" ],
	[ 'AUTHORIZER', 'LIB06_POST_AUTHORIZED', 'modify', $mine, 'modify', "the authorizer may edit" ],
	[ 'ENTERER', 'LIB06_POST_AUTHORIZED', 'modify', $theirs, 'none', "but not someone else's record" ],
	[ 'ENTERER', 'LIB06_POST_AUTHORIZED', 'modify', { %$mine, owner_lock => 1 }, 'modify,unlock',
	  "a locked record must be unlocked by its owner" ],
	[ 'ENTERER', 'LIB06_POST_AUTHORIZED', 'modify', { %$mine, admin_lock => 1 }, 'locked',
	  "a record with an administrative lock" ],
	[ 'STUDENT', 'LIB06_POST_AUTHORIZED', 'modify', $theirs, 'modify', "'modify' allows editing others' records" ],
	[ 'STUDENT', 'LIB06_POST_AUTHORIZED', 'modify', { %$theirs, owner_lock => 1 }, 'locked',
	  "but not if the owner has locked it" ],
	[ 'WITH-ADMIN', 'LIB06_POST_AUTHORIZED', 'modify', $theirs, 'admin', "an administrator" ],
	[ 'WITH-ADMIN', 'LIB06_POST_AUTHORIZED', 'delete', { %$theirs, admin_lock => 1 }, 'admin,unlock',
	  "an administrator may unlock" ],
	[ 'SUPERUSER', 'LIB06_POST_AUTHORIZED', 'view', $theirs, 'view', "a superuser viewing" ],
	[ 'ENTERER', 'LIB06_VIEW_ANY', 'view', $theirs, 'view', "a table that anyone may view" ],
	[ '', 'LIB06_POST_AUTHORIZED', 'modify', $mine, 'none', "no login" ],
	[ 'ENTERER', 'LIB06_POST_AUTHORIZED', 'insert_key', $mine, 'none', "'insert_key' without permission" ],
	[ 'ENTERER', 'LIB06_POST_AUTHORIZED', 'modify', { enterer_id => 'USERID-ENTERER' }, 'modify',
	  "a record with the user's id" ],
	[ 'STUDENT', 'LIB06_BYAUTH', 'modify', { authorizer_no => 39999, enterer_no => 39995 }, 'modify',
	  "BY_AUTHORIZER: the same authorizer" ],
	[ 'ENTERER', 'LIB06_BYAUTH', 'modify', { authorizer_no => 39999, enterer_no => 39995 }, 'none',
	  "BY_AUTHORIZER: a different one" ] );
    
    foreach my $c ( @cases )
    {
	my ($name, $table, $requested, $record, $expected, $label) = @$c;
	
	is( perm($name)->check_record_permission($table, $requested, 'x = 1', $record), $expected, $label );
    }
    
    # Without the record, its authorization fields are read from the table.
    
    set_table_property('EDTPB_MAIN', AUTH_FIELDS => 'authorizer_no, enterer_no');
    
    is( perm('ENTERER')->check_record_permission('EDTPB_MAIN', 'modify', 'main_no = 1'), 'modify',
	"a record read from the table" );
    is( perm('ENTERER')->check_record_permission('EDTPB_MAIN', 'modify', 'main_no = 2'), 'none',
	"another one" );
    is( perm('ENTERER')->check_record_permission('EDTPB_MAIN', 'modify', 'main_no = 99'), 'notfound',
	"a record that does not exist" );
    
    eval { perm('ENTERER')->check_record_permission('EDTPB_MAIN', 'modify') };
    like( $@, qr{no key expr specified}, "a key expression is required" );
    
    eval { perm('ENTERER')->check_record_permission('EDTPB_MAIN', 'fly', 'main_no = 1') };
    like( $@, qr{bad permission 'fly'}, "an unknown permission is rejected" );
};


# get_record_authinfo reads the AUTH_FIELDS of a record, or else just its primary key.

subtest 'get_record_authinfo' => sub {
    
    my $p = perm('ENTERER');
    
    is_deeply( $p->get_record_authinfo('EDTPB_MAIN', 'main_no = 1'),
	       { authorizer_no => 39998, enterer_no => 39997 }, "the authorization fields" );
    
    set_table_property('EDTPB_MAIN', AUTH_FIELDS => undef);
    
    is_deeply( $p->get_record_authinfo('EDTPB_MAIN', 'main_no = 1'), { main_no => 1 },
	       "only the primary key if there are no authorization fields" );
    is( $p->get_record_authinfo('EDTPB_MAIN', 'main_no = 99'), undef, "a record that does not exist" );
    is_deeply( $p->get_record_authinfo('LIB06_OPEN', 'x = 1'), { }, "a table without a primary key" );
    
    # A record in such a table is not reported as not found. The decision comes from the
    # table permissions.
    
    is( perm('ENTERER')->check_record_permission('LIB06_MODIFY_LOGGED_IN', 'modify', 'x = 1'), 'modify',
	"a record in a table without a primary key, for a user with 'modify'" );
    is( perm('ENTERER')->check_record_permission('LIB06_POST_AUTHORIZED', 'modify', 'x = 1'), 'none',
	"and for a user without it" );
};


# The accessors for the table permissions.

subtest 'table permission accessors' => sub {
    
    is( perm('SUPERUSER')->table_permission('LIB06_POST_AUTHORIZED', 'post'), 1,
	"a superuser has every permission" );
    is( perm('ENTERER')->table_permission('LIB06_POST_AUTHORIZED'), '', "no permission was asked for" );
    
    my $p = perm('WITH-ADMIN', 'LIB06_POST_AUTHORIZED');
    
    is( $p->table_permission('LIB06_POST_AUTHORIZED', 'admin'), 1, "table_permission" );
    is( join(',', sort $p->table_permissions('LIB06_POST_AUTHORIZED')), 'admin', "table_permissions" );
};


# With the debug option, the SQL statements are printed to STDERR.

subtest 'debugging output' => sub {
    
    my $stderr = '';
    
    {
	local *STDERR;
	open STDERR, '>', \$stderr;
	
	Permissions->new($dbh, 'SESSION-ENTERER', undef, { debug => 1 });
    }
    
    like( $stderr, qr{SELECT authorizer_no, enterer_no, user_id}, "the session lookup was printed" );
};


# Remove the sessions and permissions added by these tests.

subtest 'cleanup' => sub {
    
    sql_command("DELETE FROM <<SESSION_DATA>> WHERE session_id like 'LIB06-%'");
    
    $T->clear_specific_permissions($_) foreach grep { /^LIB06/ } keys %PROP;
    
    my ($count) = sql_fetchrow("SELECT count(*) FROM <<TABLE_PERMS>> WHERE table_name like 'LIB06%'");
    
    is( $count, 0, "the test permissions were removed" );
};
