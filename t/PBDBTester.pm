#
# EditTester.pm: a class for running tests on EditTransaction.pm and its subclasses.
# 


use strict;

package PBDBTester;

use parent 'EditTester';

use TableDefs qw(%TABLE enable_test_mode);
use CoreFunction qw(configData);
use Test::More;
use Carp qw(croak);

use namespace::clean;



# new ( ... )
# 
# Call the constructor method of the parent class, and then do some additional processing
# necessary for handling session data.

sub new {
    
    my ($class, @args) = @_;
    
    my $instance = EditTester->new(@args);
    
    # Double check to make sure that the session_data table has actually been switched over to the
    # test database.
    
    enable_test_mode('session_data');
    
    my $test_db = configData->{test_db};
    
    unless ( $test_db && $TABLE{SESSION_DATA} =~ /$test_db/ )
    {
	diag("Could not enable test mode for 'SESSION_DATA'.");
	BAIL_OUT;
    }
    
    my $id_bound;
    
    my $dbh = $instance->dbh;
    
    eval {
	($id_bound) = $dbh->selectrow_array("
		SELECT min(enterer_no) FROM $TABLE{SESSION_DATA} WHERE enterer_no > 0");
    };
    
    my $id_bound;
    
    eval {
	($id_bound) = $dbh->selectrow_array("
		SELECT min(enterer_no) FROM $TABLE{SESSION_DATA} WHERE enterer_no > 0");
    };
    
    $instance->{id_bound} = $id_bound;
    
    return bless $instance;
}


# establish_session_data ( )
#
# Create or re-create the session_data, person_data, and table_perms tables. If they exist and are
# not empty, clear the contents and establish known records for testing purposes. This method
# should only be called after calling "select_test_tables('session_data')".

sub establish_session_data {
    
    my ($T) = @_;

    my $dbh = $T->dbh;
    
    BAIL_OUT "The table name $TABLE{SESSION_DATA} is not correct. You must establish a test database."
	unless $TABLE{SESSION_DATA} =~ /test[.]/i;
    
    BAIL_OUT "The table name $TABLE{PERSON_DATA} is not correct. You must establish a test database."
	unless $TABLE{PERSON_DATA} =~ /test[.]/i;
    
    BAIL_OUT "The table name $TABLE{WING_USERS} is not correct. You must establish a test database."
	unless $TABLE{WING_USERS} =~ /test[.]/i;
    
    BAIL_OUT "The table name $TABLE{TABLE_PERMS} is not correct. You must establish a test database."
	unless $TABLE{TABLE_PERMS} =~ /test[.]/i;

    my ($person_count, $user_count);
    
    eval {
	($person_count) = $dbh->selectrow_array("SELECT count(*) FROM $TABLE{PERSON_DATA}");
	($user_count) = $dbh->selectrow_array("SELECT count(*) FROM $TABLE{WING_USERS}");
    };
    
    if ( ($person_count && $person_count > 100) || ($user_count && $user_count > 100) )
    {
	BAIL_OUT "There are more than 100 rows in the table '$TABLE{PERSON_DATA}'. " .
	    "You must establish a test database.";
    }
    
    eval {
	
	$dbh->do("CREATE TABLE IF NOT EXISTS $TABLE{SESSION_DATA} (
	  `session_id` varchar(80) NOT NULL,
	  `user_id` char(36) NOT NULL,
	  `authorizer` varchar(64) NOT NULL DEFAULT '',
	  `enterer` varchar(64) NOT NULL DEFAULT '',
	  `role` varchar(20) DEFAULT NULL,
	  `reference_no` int(11) DEFAULT NULL,
	  `queue` varchar(255) DEFAULT NULL,
	  `record_date` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
	  `created_date` timestamp NOT NULL DEFAULT current_timestamp(),
	  `expire_days` int NOT NULL default 1,
	  `superuser` tinyint(1) DEFAULT 0,
	  `authorizer_no` int(10) NOT NULL DEFAULT 0,
	  `enterer_no` int(10) NOT NULL DEFAULT 0,
	  PRIMARY KEY (`session_id`))");
	
	$dbh->do("DELETE FROM $TABLE{SESSION_DATA}");

	$dbh->do("INSERT INTO $TABLE{SESSION_DATA} (session_id, user_id, authorizer_no, 
						enterer_no, role, expire_days, superuser)
	VALUES  ('SESSION-AUTHORIZER','USERID-AUTHORIZER','39998','39998','authorizer',30000,0),
		('SESSION-ENTERER','USERID-ENTERER','39998','39997','enterer',30000,0),
		('SESSION-GUEST','USERID-GUEST','0','0','guest',30000,0),
		('SESSION-STUDENT','USERID-STUDENT','39999','39996','student',30000,0),
		('SESSION-OTHER','USERID-OTHER','39999','39995','enterer',30000,0),
		('SESSION-UNAUTH','USERID-UNAUTH','0','39994','enterer',30000,0),
		('SESSION-SUPERUSER','USERID-SUPERUSER','39999','39999','authorizer',30000,1),
		('SESSION-WITH-ADMIN','USERID-WITH-ADMIN','39999','39991','enterer',30000,0)");
	
	$dbh->do("CREATE TABLE IF NOT EXISTS $TABLE{PERSON_DATA} (
	  `person_no` int(10) unsigned NOT NULL AUTO_INCREMENT,
	  `name` varchar(64) NOT NULL DEFAULT '',
	  `reversed_name` varchar(64) NOT NULL DEFAULT '',
	  `first_name` varchar(30) NOT NULL DEFAULT '',
	  `middle` varchar(10) DEFAULT NULL,
	  `last_name` varchar(30) NOT NULL DEFAULT '',
	  `country` varchar(80) DEFAULT NULL,
	  `institution` varchar(80) DEFAULT NULL,
	  `email` varchar(255) DEFAULT NULL,
	  `homepage` varchar(255) DEFAULT NULL,
	  `photo` varchar(255) DEFAULT NULL,
	  `is_authorizer` tinyint(1) NOT NULL DEFAULT 0,
	  `role` set('authorizer','enterer','officer','student','technician') DEFAULT NULL,
	  `active` tinyint(1) NOT NULL DEFAULT 1,
	  `heir_no` int(10) unsigned NOT NULL DEFAULT 0,
	  `research_group` varchar(64) DEFAULT NULL,
	  `preferences` varchar(64) DEFAULT NULL,
	  `last_download` varchar(64) DEFAULT NULL,
	  `created` datetime DEFAULT NULL,
	  `modified` datetime DEFAULT NULL,
	  `last_action` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
	  `last_entry` datetime DEFAULT NULL,
	  `hours` int(4) unsigned DEFAULT NULL,
	  `hours_ever` int(6) unsigned DEFAULT NULL,
	  `hours_authorized` int(6) unsigned DEFAULT NULL,
	  `superuser` tinyint(1) DEFAULT 0,
	  PRIMARY KEY (`person_no`))");

	$dbh->do("DELETE FROM $TABLE{PERSON_DATA} WHERE institution = 'Test'");

	$dbh->do("INSERT INTO $TABLE{PERSON_DATA} (person_no, name, reversed_name, institution)
		VALUES	(39999,'A. Superuser','Superuser, A.','Test'),
			(39998,'A. Authorizer','Authorizer, A.','Test'),
			(39997,'A. Enterer','Enterer, A.','Test'),
			(39996,'A. Student','Student, A.','Test'),
			(39995,'B. Enterer','Enterer, B.','Test'),
			(39994,'C. Enterer','Enterer, C.','Test'),
			(39991,'A. Admin','Admin, A.','Test')");

	$dbh->do("CREATE TABLE IF NOT EXISTS $TABLE{WING_USERS} (
	  `id` char(36) COLLATE utf8_unicode_ci NOT NULL,
	  `date_created` datetime NOT NULL DEFAULT current_timestamp,
	  `date_updated` datetime NOT NULL DEFAULT current_timestamp,
	  `admin` tinyint(4) NOT NULL DEFAULT 0,
	  `real_name` varchar(255) COLLATE utf8_unicode_ci DEFAULT '',
	  `password_type` varchar(10) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'bcrypt',
	  `password_salt` char(16) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'abcdefghijklmnop',
	  `username` varchar(30) COLLATE utf8_unicode_ci NOT NULL,
	  `email` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
	  `password` char(50) COLLATE utf8_unicode_ci DEFAULT NULL,
	  `use_as_display_name` varchar(10) COLLATE utf8_unicode_ci DEFAULT 'username',
	  `developer` tinyint(4) NOT NULL DEFAULT 0,
	  `last_login` datetime DEFAULT NULL,
	  `country` char(2) COLLATE utf8_unicode_ci NOT NULL default '',
	  `person_no` int(11) DEFAULT NULL,
	  `middle_name` varchar(80) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
	  `role` enum('guest','authorizer','enterer','student') COLLATE utf8_unicode_ci NOT NULL,
	  `institution` varchar(80) COLLATE utf8_unicode_ci NOT NULL,
	  `last_name` varchar(80) COLLATE utf8_unicode_ci NOT NULL,
	  `first_name` varchar(80) COLLATE utf8_unicode_ci NOT NULL,
	  `authorizer_no` int(11) DEFAULT NULL,
	  `orcid` varchar(19) COLLATE utf8_unicode_ci NOT NULL default '',
	  `contributor_status` enum('active','disabled','deceased') 
		COLLATE utf8_unicode_ci NOT NULL default 'active',
	  `last_pwchange` datetime DEFAULT NULL,
	  PRIMARY KEY (`id`),
	  UNIQUE KEY `users_username` (`username`),
	  KEY `idx_search` (`real_name`,`username`,`email`),
	  KEY `idx_country` (`country`),
	  KEY `idx_last_name` (`last_name`),
	  KEY `users_email` (`email`),
	  KEY `idx_person_no` (`person_no`))");
	
	$dbh->do("DELETE FROM $TABLE{WING_USERS}");
	
	$dbh->do("INSERT INTO $TABLE{WING_USERS} (id, person_no, username, role,
						  real_name, first_name, last_name, institution)
    VALUES  ('USERID-SUPERUSER','39999','sua','authorizer','A. Superuser','A.','Superuser','Test'),
	    ('USERID-AUTHORIZER','39998','aua','authorizer','A. Authorizer','A.','Authorizer','Test'),
	    ('USERID-ENTERER','39997','ena','enterer','A. Enterer','A.','Enterer','Test'),
	    ('USERID-STUDENT','39996','sta','student','A. Student','A.','Student','Test'),
	    ('USERID-OTHER','39995','enb','enterer','B. Enterer','B.','Enterer','Test'),
	    ('USERID-UNAUTH','39994','enc','enterer','C. Enterer','C.','Enterer','Test'),
	    ('USERID-WITH-ADMIN','39991','adm','enterer','A. Admin','A.','Admin','Test'),
	    ('USERID-GUEST','0','gua','guest','A. Guest','A.','Guest','Test')");
	
	$dbh->do("CREATE TABLE IF NOT EXISTS $TABLE{TABLE_PERMS} (
	  `person_no` int(10) unsigned NOT NULL,
	  `table_name` varchar(80) NOT NULL,
	  `permission` set('none','view','post','modify','delete','insert_key','admin') NOT NULL,
	  UNIQUE KEY `person_no` (`person_no`,`table_name`))");
	
	# $dbh->do("INSERT INTO $TABLE{TABLE_PERMS} (person_no, table_name, permission)
	# 	VALUES ('39991', 'RESOURCE_QUEUE', 'admin')");
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("Could not establish session data. Message was: $msg");
	BAIL_OUT;
    }

    my ($id_bound) = $dbh->selectrow_array("SELECT min(enterer_no) FROM $TABLE{SESSION_DATA} WHERE enterer_no > 0");

    unless ( $id_bound > 3000 )
    {
	BAIL_OUT "Could not establish a proper boundary between test person_no values and real ones.";
    }
}


sub set_specific_permission {

    my ($T, $table_name, $perm, $value) = @_;
    
    $T->clear_edt;
    
    $T->dbh->do("ROLLBACK");
    
    my ($person_no);
    
    croak "The second argument must be a permission object" unless ref $perm eq 'Permissions';
    # croak "Bad permission '$value'" unless $value &&
    # 	($value eq 'admin' || $value eq 'post' || $value eq 'modify' || $value eq 'none');
    
    $table_name =~ s/^\w+[.]//;
    $person_no = $perm->{enterer_no};
    
    my $sql = "REPLACE INTO $TABLE{TABLE_PERMS} (person_no, table_name, permission)
		 VALUES ('$person_no', '$table_name', '$value')";
    
    $T->debug_line($sql);
    $T->debug_skip;
    
    my $result = $T->dbh->do($sql);

    unless ( $result )
    {
	croak "permission was not set";
    }
}


sub clear_specific_permissions {
    
    my ($T, $arg) = @_;

    $T->clear_edt;
    
    my $sql;
    
    $T->dbh->do("ROLLBACK");
    
    if ( ref $arg eq 'Permissions' )
    {
	my $person_no = $arg->{enterer_no};
	$sql = "DELETE FROM $TABLE{TABLE_PERMS} WHERE person_no = '$person_no'";
    }
    
    elsif ( $arg )
    {
	$sql = "DELETE FROM $TABLE{TABLE_PERMS} WHERE table_name = '$arg'";
    }
    
    else
    {
	$sql = "DELETE FROM $TABLE{TABLE_PERMS}";
    }

    $T->debug_line($sql);
    $T->debug_skip;

    $T->dbh->do($sql);
}


sub get_session_authinfo {
    
    my ($T, $session_id) = @_;

    croak "you must specify a session id" unless $session_id;
    
    return $T->dbh->selectrow_array("SELECT authorizer_no, enterer_no, user_id
		FROM $TABLE{SESSION_DATA}
		WHERE session_id = " . $T->dbh->quote($session_id));
}


# new_perm ( session_id, table_name )
# 
# Create a new Permissions object for the given session id. The session record must be in the
# session_data table in the selected database. Consequently, the test database should be selected
# before running this.
#
# If the optional table name is given, it will be used instead of the default table in fetching
# table permissions.

sub new_perm {
    
    my ($T, $session_id, $table_name) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    croak "you must specify a session id string" unless $session_id && ! ref $session_id;
    
    $table_name //= $T->{table};
    
    $T->{last_exception} = undef;
    
    my ($perm, $options);

    $options = { debug => 1 } if $T->debug_mode;
    
    eval {
	$perm = Permissions->new($T->dbh, $session_id, $table_name, $options);
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("EXCEPTION: $msg");
	$T->{last_exception} = $msg;
    }
    
    if ( $perm && ($perm->role ne 'none' || $session_id eq 'NO_LOGIN' ) )
    {
	pass("created permission object for '$session_id'");
	return $perm;
    }
    
    else
    {
	fail("created permission object for '$session_id'");
	return;
    }
}


# test_permissions ( table, insert_perm, [check_perm,] test, result, label )
# 
# We now define a subroutine which will check the ability to do all four operations on a
# particular table, or some subset, according to a particular set of permissions. This subroutine
# will pass or fail a test depending on the result of these operations.

sub test_permissions {
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    # First check the arguments.

    my $T = shift;
    my $table = shift;
    my $insert_perm = shift;
    my $edit_perm = $insert_perm;
    
    croak "bad insert permission '" . ref $insert_perm . "'"
	unless ref $insert_perm eq 'Permissions';
    
    if ( ref $_[0] eq 'Permissions' )
    {
	$edit_perm = shift;
    }
    
    elsif ( ref $_[0] )
    {
	croak "bad check permission '" . ref $_[0] . "'";
    }
    
    my ($test, $result, $label) = @_;
    
    croak "bad test '$test'" unless $test &&
	($test eq 'basic' || $test =~ qr{ ^ [IDRUK]+ $ }xs );
    
    $test = 'IDRU' if $test eq 'basic';
    
    if ( $result && $result eq 'succeeds' )
    {
	$label ||= 'transaction succeeded';
    }
    
    elsif ( $result && $result eq 'fails' )
    {
	$label ||= 'transaction failed with E_PERM';
    }
    
    else
    {
	croak "bad check '$result', must be one of 'succeeds', 'fails'";
    }
    
    my $primary = get_table_property($table, 'PRIMARY_KEY') or
	croak "cannot fetch primary key for table '$table'";
    
    my $string = $table =~ /EDT_AUX/ ? 'name' : 'string_req';
    
    # First test insertion. We use the first specified permission to do this. We must insert no
    # matter which tests are being done, so that we have records to replace, update, and/or
    # delete.
    
    my @insert_errors;
    my $perm_count = 0;
    
    my ($key1, $key2, $key3);
    
    my $edt = $T->new_edt($insert_perm, { IMMEDIATE_MODE => 1 });
    
    if ( $test =~ /[IR]/ )
    {
	$key1 = $edt->insert_record($table, { $string => 'insert permission' }, 'keyval')
	    && $edt->get_keyval;
	
	$perm_count++ if $test =~ /I/;
    }
    
    if ( $test =~ /U/ )
    {
	$key2 = $edt->insert_record($table, { $string => 'insert permission 2' }, 'keyval') &&
	    $edt->get_keyval;
    }
    
    if ( $test =~ /D/ )
    {
	$key3 = $edt->insert_record($table, { $string => 'insert permission 3' }, 'keyval')
	    && $edt->get_keyval;
    }
    
    # If we are using a different permission for the rest of the tests, then commit the first
    # transaction and start a second one. If any errors have occurred during the insert phase, the
    # test fails.
    
    if ( $edit_perm != $insert_perm )
    {
	$edt->commit;
	
	if ( $edt->errors )
	{
	    foreach my $e ( $edt->errors )
	    {
		diag($e->code . ': ' . $edt->generate_msg($e));
	    }
	    
	    fail($label);
	}
	
	$edt = $T->new_edt($edit_perm, { IMMEDIATE_MODE => 1 });
	$perm_count = 0;
    }
    
    elsif ( $result eq 'fails' && $edt->errors )
    {
	$edt->rollback;
	return pass( $label );
    }
    
    if ( $test =~ /R/ && $key1 )
    {
	$edt->replace_record($table, { _primary => $key1, $string => 'replace permission' });
	$perm_count++;
    }
    
    if ( $test =~ /U/ && $key2 )
    {
	$edt->update_record($table, { _primary => $key2, $string => 'update permission' });
	$perm_count++;
    }
    
    if ( $test =~ /D/ && $key3 )
    {
	my ($delete_check) = $T->fetch_records_by_key($table, $key3);
	
	unless ( $delete_check && $delete_check->{$string} eq 'insert permission 3' )
	{
	    diag("Expected record was not found for deletion");
	    fail($label);
	    $edt->rollback;
	    return;
	}
	
	$edt->delete_record($table, $key3);
	$perm_count++;
    }

    if ( $test =~ /K/ )
    {
	my ($max) = $T->dbh->selectrow_array("SELECT max($primary) FROM $TABLE{$table}");
	
	$edt->replace_record($table, { $primary => $max + 1, $string => 'specific key permission' });
	$perm_count++;
    }
    
    # Now commit the transaction (possibly the second one). If it succeeds, then return the proper
    # result.
    
    if ( $edt->commit )
    {
	# If the transaction was supposed to succeed, then the test passes. Otherwise, it fails.
	
	return ok( $result eq 'succeeds', $label );
    }
    
    # $T->diag_errors($edt);
    
    # If the transaction fails, we have to look at the reasons behind the failure.
    
    my (%error_count, $bad_code);
    
    foreach my $e ( @insert_errors, $edt->errors )
    {
	if ( $e->code eq 'E_PERM' )
	{
	    $error_count{E_PERM}++;
	    diag($e->code . ': ' . $edt->generate_msg($e)) if $result eq 'succeeds';
	}
	
	else
	{
	    $bad_code = 1;
	    $error_count{$e->code}++;
	    diag($e->code . ': ' . $edt->generate_msg($e));
	}
    }
    
    # If we have any errors other than E_PERM, then the test fails regardless of what the fourth
    # argument was.
    
    if ( $bad_code )
    {
	return fail( $label );
    }
    
    # If the test was supposed to succeed, we fail if we have even one E_PERM.
    
    elsif ( $result eq 'succeeds' )
    {
	return fail( $label );
    }
    
    # Otherwise, if we have at one E_PERM for each operation we counted, then the test succeeds if the
    # transaction was supposed to fail.
    
    elsif ( $error_count{E_PERM} && $perm_count && $error_count{E_PERM} == $perm_count )
    {
	return pass( $label );
    }
    
    # If we don't get at least one E_PERM or other error, something has gone very wrong.
    
    else
    {
	diag( "The transaction failed, but the wrong number of E_PERM errors was found." );
	return fail( $label );
    }
}


our ($UNIQ_B) = 0;

# test_subordinate_permissions ( table, insert_perm, [check_perm,] test, result, label )
# 
# This subroutine does the same thing as 'test_permissions' above, but on subordinate tables.
# This subroutine will pass or fail a test depending on the result of the selected operations.

sub test_subordinate_permissions {
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    # First check the arguments.

    my $T = shift;
    my $table = shift;
    my $insert_perm = shift;
    my $edit_perm = $insert_perm;
    
    croak "bad insert permission '" . ref $insert_perm . "'"
	unless ref $insert_perm eq 'Permissions';
    
    if ( ref $_[0] eq 'Permissions' )
    {
	$edit_perm = shift;
    }
    
    elsif ( ref $_[0] )
    {
	croak "bad check permission '" . ref $_[0] . "'";
    }
    
    my ($test, $result, $label) = @_;
    
    croak "bad test '$test'" unless $test &&
	($test eq 'basic' || $test =~ qr{ ^ [IDRUK]+ $ }xs );
    
    $test = 'IDRU' if $test eq 'basic';
    
    if ( $result && $result eq 'succeeds' )
    {
	$label ||= 'transaction succeeded';
    }
    
    elsif ( $result && $result eq 'fails' )
    {
	$label ||= 'transaction failed with E_PERM';
    }
    
    else
    {
	croak "bad check '$result', must be one of 'succeeds', 'fails'";
    }
    
    my $sup_table = get_table_property($table, 'SUPERIOR_TABLE') or
	croak "cannot fetch superior table for '$table'";
    
    my $primary = get_table_property($table, 'PRIMARY_KEY') or
	croak "cannot fetch primary key for table '$table'";
    
    my $linkcol = get_table_property($sup_table, 'PRIMARY_KEY') or
	croak "cannot fetch primary key for table '$sup_table'";
    
    my $string = $table =~ /EDT_AUX/ ? 'name' : 'string_req';
    my $sup_string = 'string_req'; # there is currently only one superior table in use: EDT_TEST
    
    $UNIQ_B++;
    
    # First insert a record into both the superior and subordinate table. We use the first specified
    # permission to do this. We must insert no matter which tests are being done, so that we have
    # a record with which to associate the subordinate records we are inserting, updating,
    # replacing, and deleting, and a subordinate record to test.
    
    my ($keyA, $keyB1, $keyB2, $keyB3, $keyC1, $keyC2, $keyC3);
    
    my $edt = $T->new_edt($insert_perm, { IMMEDIATE_MODE => 1 });
    
    $keyA = $edt->insert_record($sup_table, { $sup_string => 'permission test record' })
	&& $edt->get_keyval;

    if ( $test =~ /[IR]/ )
    {
	$keyB1 = $edt->insert_record($table, { $linkcol => $keyA, $string => "subordinate test 1.$UNIQ_B" })
	    && $edt->get_keyval;
    }

    if ( $test =~ /U/ )
    {
	$keyB2 = $edt->insert_record($table, { $linkcol => $keyA, $string => "subordinate test 2.$UNIQ_B" })
	    && $edt->get_keyval;
    }

    if ( $test =~ /D/ )
    {
	$keyB3 = $edt->insert_record($table, { $linkcol => $keyA, $string => "subordinate test 3.$UNIQ_B" })
	    && $edt->get_keyval;
    }
    
    unless ( $edt->commit )
    {
	foreach my $e ( $edt->errors )
	{
	    diag($e->code . ': ' . $edt->generate_msg($e));
	}
	
	return fail($label);
    }
    
    # Now create a second edt with the second permission, with which to test the specified
    # operations on the subordinate table.

    my $operation_count = 0;
    
    $edt = $T->new_edt($edit_perm, { IMMEDIATE_MODE => 1 });
    
    # First test insertion of subordinate records.
    
    if ( $test =~ /[IR]/ )
    {
	$keyC1 = $edt->insert_record($table, { $linkcol => $keyA, $string => "subordinate check 1.$UNIQ_B" })
	    && $edt->get_keyval;
	$operation_count++;
    }
    
    if ( $test =~ /U/ )
    {
	$keyC2 = $edt->insert_record($table, { $linkcol => $keyA, $string => "subordinate check 2.$UNIQ_B" })
	    && $edt->get_keyval;
	$operation_count++;
    }
    
    if ( $test =~ /D/ )
    {
	$keyC3 = $edt->insert_record($table, { $linkcol => $keyA, $string => "subordinate check 3.$UNIQ_B" })
	    && $edt->get_keyval;
	$operation_count++;
    }
        
    if ( $test =~ /R/ )
    {
	if ( $keyB1 )
	{
	    $edt->replace_record($table, { _primary => $keyB1, $string => "replace B1.$UNIQ_B" });
	    $operation_count++;
	}
	
	if ( $keyC1 )
	{
	    $edt->replace_record($table, { $primary => $keyC1, $string => "replace C1.$UNIQ_B" }) if $keyC1;
	    $operation_count++;
	}

	ok( $keyB1 || $keyC1, "got at least one key to test for replace" );
    }
    
    if ( $test =~ /U/ )
    {
	if ( $keyB2 )
	{
	    $edt->update_record($table, { $primary => $keyB2, $string => "update B2.$UNIQ_B" });
	    $operation_count++;
	}

	if ( $keyC2 )
	{
	    $edt->update_record($table, { $primary => $keyC2, $string => "update C2.$UNIQ_B" });
	    $operation_count++;
	}

	ok( $keyB2 || $keyC2, "got at least one key to test for update" );
    }
    
    if ( $test =~ /D/ )
    {
	if ( $keyB3 )
	{
	    my ($delete_check) = $T->fetch_records_by_key($table, $keyB3);
	    
	    unless ( $delete_check && $delete_check->{$string} eq "subordinate test 3.$UNIQ_B" )
	    {
		diag("Expected record was not found for deletion");
		fail($label);
		$edt->rollback;
		return;
	    }
	    
	    $edt->delete_record($table, $keyB3);
	    $operation_count++;
	}

	if ( $keyC3 )
	{
	    my ($delete_check) = $T->fetch_records_by_key($table, $keyC3);
	    
	    unless ( $delete_check && $delete_check->{$string} eq "subordinate check 3.$UNIQ_B" )
	    {
		diag("Expected record was not found for deletion");
		fail($label);
		$edt->rollback;
		return;
	    }
	    
	    $edt->delete_record($table, $keyC3);
	    $operation_count++;
	}

	ok( $keyB3 || $keyC3, "got at least one key to test for delete" );
    }

    if ( $test =~ /K/ )
    {
	my ($max) = $T->dbh->selectrow_array("SELECT max($primary) FROM $TABLE{$table}");
	
	$edt->replace_record($table, { $primary => $max + 1, $string => 'specific key permission' });
	$operation_count++;
    }
    
    # Now commit the second transaction. If it succeeds, then return the proper
    # result.
    
    if ( $edt->commit )
    {
	# If the transaction was supposed to succeed, then the test passes. Otherwise, it fails.
	
	return ok( $result eq 'succeeds', $label );
    }
    
    # If the transaction fails, we have to look at the reasons behind the failure.
    
    my ($e_perm_count, $bad_code);
    
    foreach my $e ( $edt->errors )
    {
	if ( $e->code eq 'E_PERM' )
	{
	    $e_perm_count++;
	    diag($e->code . ': ' . $edt->generate_msg($e)) if $result eq 'succeeds';
	}
	
	else
	{
	    $bad_code = 1;
	    diag($e->code . ': ' . $edt->generate_msg($e));
	}
    }
    
    # If we have any errors other than E_PERM, then the test fails regardless of what the fourth
    # argument was.
    
    if ( $bad_code )
    {
	return fail( $label );
    }
    
    # If the test was supposed to succeed, we fail if we have even one E_PERM.
    
    elsif ( $result eq 'succeeds' )
    {
	return fail( $label );
    }
    
    # Otherwise, if we have exactly one E_PERM for each operation we counted, then the test succeeds if the
    # transaction was supposed to fail.
    
    elsif ( $e_perm_count && $operation_count && $e_perm_count == $operation_count )
    {
	return pass( $label );
    }
    
    # If we don't get at least one E_PERM or other error, something has gone very wrong.
    
    else
    {
	diag( "The transaction failed, but the wrong number of E_PERM errors was found." );
	return fail( $label );
    }
}


sub safe_clear_table {
    
    my ($T, $table, $field, $base_table, $link_field) = @_;
    
    $T->clear_edt;
    
    my $dbh = $T->dbh;
    
    my ($sql, $result);
    
    croak "you must specify a table" unless $table;

    if ( defined $base_table )
    {
	croak "you must specify a base table" unless $base_table;
	croak "you must specify a link field" unless $link_field;
	
	$sql = "DELETE $TABLE{$table} FROM $TABLE{$table} join $TABLE{$base_table} USING ($link_field) WHERE " .
	    $T->test_entry_filter($field, $base_table);
    }
    
    else
    {
	$sql = "DELETE FROM $TABLE{$table} WHERE " . $T->test_entry_filter($field);
    }
    
    $T->debug_line($sql);
    
    $result = $dbh->do($sql);
    
    if ( $result )
    {
	$T->debug_line("Deleted $result rows");
    }
    
    my ($remaining) = $dbh->selectrow_array("SELECT count(*) FROM $TABLE{$table}");

    # If the table is now empty, reset its auto_increment count to 1.
    
    if ( defined $remaining && $remaining == 0 )
    {
	$sql = "ALTER TABLE $TABLE{$table} AUTO_INCREMENT = 1";
	
	$T->debug_line($sql);
	
	eval {
	    $dbh->do($sql);
	};
    }
}


sub test_entry_filter {
    
    my ($T, $field, $base_table) = @_;
    
    croak "you must specify an identifier field, must be either authorizer_no, enterer_no, user_id" unless $field;
    
    # First make sure we have a dividing line between real and test person_no values.
    
    unless ( $T->{id_bound} && $T->{id_bound} > 3000 )
    {
	BAIL_OUT "Could not establish the boundary between real person_no values and test values";
    }
    
    # Then clear all test entries from the specified table.

    my $expr = $base_table ? "$TABLE{$base_table}.$field" : $field;
    
    if ( $field eq 'user_id' )
    {
	return "$expr like 'USERID-%'";
    }
    
    elsif ( $field eq 'authorizer_no' || $field eq 'enterer_no' )
    {
	return "$expr >= $T->{id_bound}";
    }
    
    else
    {
	croak "unrecognized identifier field '$field'";
    }
}


1;

