#
# Tester.pm: a class for running tests on EditTransaction.pm and its subclasses.
# 



use strict;
use feature 'unicode_strings';
use feature 'fc';

package EditTester;

use Scalar::Util qw(looks_like_number reftype weaken);
use Carp qw(croak);
use Test::More;
use base 'Exporter';

use CoreFunction qw(connectDB configData);
use TableDefs qw(init_table_names select_test_tables get_table_property get_column_properties
		 $EDT_TEST $EDT_AUX
		 $SESSION_DATA $PERSON_DATA $TABLE_PERMS);

use EditTest;

use namespace::clean;

our $LAST_BANNER = '';

our (@EXPORT_OK) = qw();



# new ( options )
# 
# Create a new EditTester instance.

sub new {
    
    my ($class, $options) = @_;
    
    $options ||= { };
    
    $options->{debug} = 1 if @ARGV && $ARGV[0] eq 'debug';
    $options->{notsilent} = 1 if @ARGV && $ARGV[0] eq 'notsilent';
    
    my $dbh;
    
    eval {
	$dbh = connectDB("config.yml");
	$dbh->{mysql_enable_utf8} = 1;
	$dbh->do('SET @@SQL_MODE = CONCAT(@@SQL_MODE, ",STRICT_TRANS_TABLES")');
	init_table_names(configData, 1);
	select_test_tables('edt_test', 1);
	select_test_tables('session_data', 1);
    };
    
    unless ( defined $dbh )
    {
	my $msg = trim_exception($@);

	if ( $msg )
	{
	    diag("Could not connect to database. Message was: $msg");
	}

	else
	{
	    diag("Could not connect to database. No error message.");
	}

	BAIL_OUT;
    }
    
    my $instance = { dbh => $dbh,
		     debug => $options->{debug},
		     notsilent => $options->{notsilent} };
    
    bless $instance, $class;
    
    return $instance;
}


# dbh ( )
#
# Return the database handle for the current tester.

sub dbh {
    
    return $_[0]->{dbh};
}


# debug ( )
#
# Return the status of the debug flag on this object.

sub debug {

    return $_[0]->{debug};
}


# create_tables ( )
# 
# Create or re-create the tables necessary for testing.

sub create_tables {

    my ($T) = @_;
    
    eval {
	establish_session_data($T->dbh);
	EditTest->establish_tables($T->dbh);
    };

    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("Could not establish tables. Message was: $msg");
	BAIL_OUT;
    }
}


sub establish_session_data {
    
    my ($dbh) = @_;

    croak "The table name $SESSION_DATA is not correct. You must establish a test database."
	unless $SESSION_DATA =~ /test/i;
    
    eval {
	
	$dbh->do("CREATE TABLE IF NOT EXISTS $SESSION_DATA (
  `session_id` varchar(80) NOT NULL,
  `user_id` char(36) NOT NULL,
  `authorizer` varchar(64) NOT NULL DEFAULT '',
  `enterer` varchar(64) NOT NULL DEFAULT '',
  `role` varchar(20) DEFAULT NULL,
  `roles` varchar(128) DEFAULT NULL,
  `reference_no` int(11) DEFAULT NULL,
  `queue` varchar(255) DEFAULT NULL,
  `record_date` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `marine_invertebrate` tinyint(1) DEFAULT 0,
  `micropaleontology` tinyint(1) DEFAULT 0,
  `paleobotany` tinyint(1) DEFAULT 0,
  `taphonomy` tinyint(1) DEFAULT 0,
  `vertebrate` tinyint(1) DEFAULT 0,
  `superuser` tinyint(1) DEFAULT 0,
  `authorizer_no` int(10) NOT NULL DEFAULT 0,
  `enterer_no` int(10) NOT NULL DEFAULT 0,
  PRIMARY KEY (`session_id`))");
	
	$dbh->do("DELETE FROM $SESSION_DATA");

	$dbh->do("INSERT INTO $SESSION_DATA (session_id, user_id, authorizer_no, enterer_no, role, superuser)
		VALUES  ('SESSION-AUTHORIZER','USERID-AUTHORIZER','3998','3998','authorizer',0),
			('SESSION-ENTERER','USERID-ENTERER','3998','3997','enterer',0),
			('SESSION-GUEST','USERID-GUEST','0','0','guest',0),
			('SESSION-STUDENT','USERID-STUDENT','3999','3996','student',0),
			('SESSION-OTHER', 'USERID-OTHER', '3999', '3995', 'enterer', 0),
			('SESSION-UNAUTH', 'USERID-UNAUTH', '0', '3994', 'enterer', 0),
			('SESSION-SUPERUSER','USERID-SUPERUSER','3999','3999','authorizer', 1),
			('SESSION-WITH-ADMIN','USERID-WITH-ADMIN','3999','3991','enterer', 0)");

	$dbh->do("CREATE TABLE IF NOT EXISTS $PERSON_DATA (
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

	$dbh->do("DELETE FROM $PERSON_DATA WHERE institution = 'Test'");

	$dbh->do("INSERT INTO $PERSON_DATA (person_no, name, reversed_name, institution)
		VALUES	(3999,'A. Superuser','Superuser, A.','Test'),
			(3998,'A. Authorizer','Authorizer, A.','Test'),
			(3997,'A. Enterer','Enterer, A.','Test'),
			(3996,'A. Student','Student, A.','Test'),
			(3995,'B. Enterer','Enterer, B.','Test'),
			(3994,'C. Enterer','Enterer, C.','Test'),
			(3991,'A. Admin','Admin, A.','Test')");

	$dbh->do("CREATE TABLE IF NOT EXISTS $TABLE_PERMS (
  `person_no` int(10) unsigned NOT NULL,
  `table_name` varchar(80) NOT NULL,
  `permission` set('none','view','post','modify','delete','insert_key','admin') NOT NULL,
  UNIQUE KEY `person_no` (`person_no`,`table_name`))");
	
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("Could not establish session data. Message was: $msg");
	BAIL_OUT;
    }
}


sub set_specific_permission {

    my ($T, $table_name, $perm, $value) = @_;

    $T->clear_edt;
    
    my ($person_no);
    
    croak "The second argument must be a permission object" unless ref $perm eq 'Permissions';
    # croak "Bad permission '$value'" unless $value &&
    # 	($value eq 'admin' || $value eq 'post' || $value eq 'modify' || $value eq 'none');
    
    $table_name =~ s/^\w+[.]//;
    $person_no = $perm->{enterer_no};
    
    my $sql = "REPLACE INTO $TABLE_PERMS (person_no, table_name, permission)
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

    if ( ref $arg eq 'Permissions' )
    {
	my $person_no = $arg->{enterer_no};
	$sql = "DELETE FROM $TABLE_PERMS WHERE person_no = '$person_no'";
    }
    
    elsif ( $arg )
    {
	$sql = "DELETE FROM $TABLE_PERMS WHERE table_name = '$arg'";
    }
    
    else
    {
	$sql = "DELETE FROM $TABLE_PERMS";
    }

    $T->debug_line($sql);
    $T->debug_skip;

    $T->dbh->do($sql);
}


sub trim_exception {

    my ($msg) = @_;

    return $msg;
}


sub debug_line {
    
    my ($T, $line) = @_;

    print STDERR " ### $line\n" if $T->{debug};
}


sub debug_skip {

    my ($T) = @_;
    
    print STDERR "\n" if $T->{debug};
}


# new_edt ( perms, options )
#
# Create a new EditTest object. The CREATE allowance is specified by default.

sub new_edt {
    
    my ($T, $perm, $options) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $options ||= { };
    $options->{CREATE} = 1 unless exists $options->{CREATE};
    
    if ( my $edt = $T->get_new_edt($perm, $options) )
    {
	pass("created edt");
	return $edt;
    }

    else
    {
	fail("created edt");
	return;
    }
}


# _new_edt ( perms, options )
#
# Do the work of creating a new EditTest object.

sub get_new_edt {
    
    my ($T, $perm, $options) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $T->{last_edt} = undef;
    $T->{last_exception} = undef;
    
    # Turn on debug mode if 'debug' was given as an argument to the entire test, and turn off
    # silent mode if 'notsilent' was given. Otherwise, silent mode is on by default.
    
    $options->{DEBUG_MODE} = 1 if $T->{debug} && ! exists $options->{DEBUG_MODE};
    # $options->{SILENT_MODE} = 1 unless $T->{debug} || $T->{notsilent} || exists $options->{SILENT_MODE};
    
    # Now process all of the specified options, and apply all those which are in upper case as
    # allowances.
    
    my $allow = { };
    
    if ( ref $options eq 'HASH' )
    {
	foreach my $k ( keys %$options )
	{
	    if ( $k =~ /^[A-Z_]+$/ )
	    {
		$allow->{$k} = $options->{$k} ? 1 : 0;
	    }
	}
    }
    
    my $edt = EditTest->new($T->dbh, $perm, $EDT_TEST, $allow);
    
    if ( $edt )
    {
	# pass("created edt");
	$T->{last_edt} = $edt;
	return $edt;
    }
    
    else
    {
	# fail("created edt");
	$T->{last_edt} = undef;
	return;
    }
}


# new_perm ( session_id, table_name )
#
# Create a new Permissions object for the given session id. The session record must be in the
# session_data table in the selected database. Consequently, the test database should be selected
# before running this.
#
# If the optional table name is given, it will be used instead of $EDT_TEST in fetching table
# permissions.

sub new_perm {
    
    my ($T, $session_id, $table_name) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    croak "you must specify a session id string" unless $session_id && ! ref $session_id;
    
    $table_name ||= $EDT_TEST;

    $T->{last_exception} = undef;

    my $perm;

    eval {
	$perm = Permissions->new($T->dbh, $session_id, $table_name);
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


# The following routines wrap the basic editing calls
# ---------------------------------------------------

sub do_insert_records {
    
    my ($T, $perm, $options, $label, $table, @records) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    croak "you must specify at least one record" unless @records && ref $records[0] eq 'HASH';
    
    $options ||= { };
    $options->{CREATE} = 1;
    
    my $edt = $T->get_new_edt($perm, $options);
    
    return $T->do_one_operation($edt, $options, 'insert_record', $label, $table, @records);
}


sub do_update_records {

    my ($T, $perm, $options, $label, $table, @records) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    croak "you must specify at least one record" unless @records && ref $records[0] eq 'HASH';
    
    my $edt = $T->get_new_edt($perm, $options);
    
    return $T->do_one_operation($edt, $options, 'update_record', $label, $table, @records);
}


sub do_replace_records {

    my ($T, $perm, $options, $label, $table, @records) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    croak "you must specify at least one record" unless @records && ref $records[0] eq 'HASH';
    
    my $edt = $T->get_new_edt($perm, $options);
    
    return $T->do_one_operation($edt, $options, 'replace_record', $label, $table, @records);
}


sub do_delete_records {

    my ($T, $perm, $options, $label, $table, @records) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    croak "you must specify at least one record" unless @records && $records[0];
    
    my $edt = $T->get_new_edt($perm, $options);
    
    return $T->do_one_operation($edt, $options, 'delete_record', $label, $table, @records);
}


sub do_one_operation {

    my ($T, $edt, $options, $operation, $label, $table, @records) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    unless ( $edt )
    {
	if ( $options && $options->{nocheck} )
	{
	    pass($label);
	}

	else
	{
	    fail($label);
	}

	return;
    }
    
    $T->{last_exception} = undef;
    
    my $result;
    
    eval {
	foreach my $r ( @records )
	{
	    $edt->$operation($table, $r);
	}
	
	$result = $edt->execute;
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("EXCEPTION: $msg");
	
	if ( $options && $options->{nocheck} )
	{
	    pass($label);
	}

	else
	{
	    fail($label);
	}
	
	$T->{last_exception} = $msg;
	return;
    }
    
    if ( $options && $options->{nocheck} )
    {
	pass($label);
	return 1;
    }
    
    elsif ( $result )
    {
	pass($label);
	return 1;
    }

    else
    {
	$T->diag_errors($edt);
	fail($label);
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
	die "cannot fetch primary key for table '$table'";
    
    my $string = $table =~ /edt_aux/ ? 'name' : 'string_req';
    
    # First test insertion. We use the first specified permission to do this. We must insert no
    # matter which tests are being done, so that we have records to replace, update, and/or
    # delete.
    
    my ($insert_result, @insert_errors, $perm_count);
    my ($key1, $key2, $key3);
    
    my $edt = $T->new_edt($insert_perm, { IMMEDIATE_MODE => 1 });
    
    if ( $test =~ /[IR]/ )
    {
	$key1 = $edt->insert_record($table, { $string => 'insert permission' });
	$perm_count++ if $test =~ /I/;
    }
    
    if ( $test =~ /U/ )
    {
	$key2 = $edt->insert_record($table, { $string => 'insert permission 2' });
    }
    
    if ( $test =~ /D/ )
    {
	$key3 = $edt->insert_record($table, { $string => 'insert permission 3' });
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
	$edt->replace_record($table, { $primary => $key1, $string => 'replace permission' });
	$perm_count++;
    }
    
    if ( $test =~ /U/ && $key2 )
    {
	$edt->update_record($table, { $primary => $key2, $string => 'update permission' });
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
	my ($max) = $T->dbh->selectrow_array("SELECT max($primary) FROM $table");
	
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


sub last_edt {

    my ($T) = @_;
    
    return ref $T->{last_edt} && $T->{last_edt}->isa('EditTransaction') ? $T->{last_edt} : undef;
}


sub clear_edt {

    my ($T) = @_;

    $T->{last_edt} = undef;
}


sub ok_result {
    
    my $T = shift;
    my $result = shift;
    my $label = shift || "operation succeeded";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $edt;
    
    if ( ref $_[0] && $_[0]->isa('EditTransaction') )
    {
	$edt = shift;
    }
    
    else
    {
	$edt = $T->{last_edt};
    }
    
    if ( $result )
    {
	pass($label);
    }
    
    else
    {
	$T->diag_errors('current');
	fail($label);
    }
}


sub ok_no_errors {

    my $T = shift;
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift : $T->{last_edt};
    
    croak "no EditTransaction found" unless $edt;
    
    my $selector = 'current';

    if ( $_[0] && ($_[0] eq 'current' || $_[0] eq 'any') )
    {
	$selector = shift;
    }

    my $label = shift;
    $label ||= "no errors found";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( $selector eq 'any' && $edt->errors )
    {
	$T->diag_errors($edt, 'any');
	fail($label);
	return;
    }

    elsif ( $selector eq 'current' && $edt->specific_errors )
    {
	$T->diag_errors($edt, 'current');
	fail($label);
	return;
    }
    
    else
    {
	pass($label);
	return 1;
    }
}


sub ok_has_error {
    
    my $T = shift;
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift : $T->{last_edt};
    
    croak "no EditTransaction found" unless $edt;
    
    my $selector = 'current';

    if ( $_[0] && ($_[0] eq 'current' || $_[0] eq 'any') )
    {
	$selector = shift;
    }
    
    my $check = shift;
    
    unless ( $check && ( ref $check eq 'Regexp' || $check =~ /^[A-Z0-9_]+$/ ) )
    {
	croak "you must specify either a condition code or regexp";
    }
    
    my $label = shift;
    $label ||= "found matching error";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my @errors = ($selector eq 'current' ? $edt->specific_errors : $edt->errors);
    
    foreach my $e ( @errors )
    {
	if ( ref $check eq 'Regexp' )
	{
	    my $msg = $e->code;
	    $msg .= ' (' . $e->label . ')' if $e->label;
	    $msg .= ': ' . $edt->generate_msg($e);
	    
	    if ( $msg =~ $check )
	    {
		pass($label);
		return 1;
	    }
	}
	
	elsif ( $check eq $e->code )
	{
	    pass($label);
	    return 1;
	}
    }
    
    $T->diag_errors($edt, $selector);
    fail($label);
    return;
}


sub diag_errors {

    my $T = shift;

    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift : $T->{last_edt};
    
    my $selector = shift || 'current';
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    croak "no EditTransaction found" unless ref $edt;
    
    my @errors = ($selector eq 'current' ? $edt->specific_errors : $edt->errors);
    
    foreach my $e ( @errors )
    {
	my $msg = $e->code;
	$msg .= ' (' . $e->label . ')' if $e->label;
	$msg .= ': ' . $edt->generate_msg($e);
	
        diag($msg);
    }
}


# sub ok_current_error {
    
#     my $T = shift;
    
#     my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift : $T->{last_edt};
    
#     my $regexp = shift;
#     croak "you must specify an error code or a regexp" unless $regexp && ref $regexp eq 'Regexp';
    
#     my $label = shift;
#     croak "you must specify a label" unless $label && ! ref $label;
    
#     local $Test::Builder::Level = $Test::Builder::Level + 1;
    
#     my @current = $edt->specific_errors;
    
#     foreach my $e ( @current )
#     {
# 	my $msg = $e->code . ': ' . $edt->generate_msg($e);
	
# 	if ( $msg =~ $regexp )
# 	{
# 	    pass($label);
# 	    return 1;
# 	}
#     }
    
#     fail($label);
#     return;
# }


sub ok_no_warnings {

    my $T = shift;
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift : $T->{last_edt};
    
    croak "no EditTransaction found" unless $edt;
    
    my $selector = 'current';

    if ( $_[0] && ($_[0] eq 'current' || $_[0] eq 'any') )
    {
	$selector = shift;
    }

    my $label = shift;
    $label ||= "no warnings found";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( $selector eq 'any' && $edt->warnings )
    {
	$T->diag_warnings($edt, 'any');
	fail($label);
	return;
    }

    elsif ( $selector eq 'current' && $edt->specific_warnings )
    {
	$T->diag_warnings($edt, 'current');
	fail($label);
	return;
    }
    
    else
    {
	pass($label);
	return 1;
    }
}


sub ok_has_warning {
    
    my $T = shift;
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift : $T->{last_edt};
    
    croak "no EditTransaction found" unless $edt;
    
    my $selector = 'current';

    if ( $_[0] && ($_[0] eq 'current' || $_[0] eq 'any') )
    {
	$selector = shift;
    }
    
    my $check = shift;
    
    unless ( $check && ( ref $check eq 'Regexp' || $check =~ /^[A-Z0-9_]+$/ ) )
    {
	croak "you must specify either a condition code or regexp";
    }
    
    my $label = shift;
    
    $label ||= "found matching warning";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my @warnings = ($selector eq 'current' ? $edt->specific_warnings : $edt->warnings);
    
    foreach my $w ( @warnings )
    {
	if ( ref $check eq 'Regexp' )
	{
	    my $msg = $w->code;
	    $msg .= ' (' . $w->label . ')' if $w->label;
	    $msg .= ': ' . $edt->generate_msg($w);
	    
	    if ( $msg =~ $check )
	    {
		pass($label);
		return 1;
	    }
	}

	elsif ( $check eq $w->code )
	{
	    pass($label);
	    return 1;
	}
    }

    $T->diag_warnings($edt, $selector);
    fail($label);
    return;
}


sub ok_no_conditions {
    
    my $T = shift;
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift : $T->{last_edt};
    
    croak "no EditTransaction found" unless $edt;
    
    my $selector = 'current';

    if ( $_[0] && ($_[0] eq 'current' || $_[0] eq 'any') )
    {
	$selector = shift;
    }

    my $label = shift;
    $label ||= "no error or warning conditions found";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( $selector eq 'any' && ($edt->errors || $edt->warnings ) )
    {
	$T->diag_errors($edt, 'any') if $edt->errors;
	$T->diag_warnings($edt, 'any') if $edt->warnings;
	fail($label);
	return;
    }
    
    elsif ( $selector eq 'current' && ($edt->specific_errors || $edt->specific_warnings) )
    {
	$T->diag_errors($edt, 'current') if $edt->specific_errors;
	$T->diag_warnings($edt, 'current') if $edt->specific_warnings;
	fail($label);
	return;
    }
    
    else
    {
	pass($label);
	return 1;
    }
}


sub diag_warnings {

    my ($T, $edt, $selector) = @_;

    $selector ||= 'current';
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $edt //= $T->{last_edt};

    croak "no EditTransaction found" unless ref $edt;
    
    my @warnings = ($selector eq 'current' ? $edt->specific_warnings : $edt->warnings);
    
    foreach my $w ( @warnings )
    {
	my $msg = $w->code;
	$msg .= ' (' . $w->label . ')' if $w->label;
	$msg .= ': ' . $edt->generate_msg($w);
	
        diag($msg);
    }
}


# sub ok_current_warning {
    
#     my $T = shift;
    
#     my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift : $T->{last_edt};
    
#     my $regexp = shift;
#     croak "you must specify a regexp" unless $regexp && ref $regexp eq 'Regexp';
    
#     my $label = shift;
#     croak "you must specify a label" unless $label && ! ref $label;
    
#     local $Test::Builder::Level = $Test::Builder::Level + 1;
    
#     foreach my $w ( $edt->specific_warnings )
#     {
# 	my $msg =~ $w->code . ': ' . $edt->generate_msg($w);
	
# 	if ( $msg =~ $regexp )
# 	{
# 	    pass($label);
# 	    return 1;
# 	}
#     }

#     fail($label);
#     return;
# }

	
# sub ok_last_exception {

#     my ($T, $regex, $label) = @_;
    
#     croak "you must specify a regular expression" unless $regex && ref $regex eq 'Regexp';
#     croak "you must specify a label" unless $label;
    
#     local $Test::Builder::Level = $Test::Builder::Level + 1;
    
#     my $msg = $T->{last_exception};
    
#     ok( $msg && $msg =~ $regex, $label);
# }


sub ok_found_record {
    
    my ($T, $table, $expr, $label) = @_;
    
    my $dbh = $T->dbh;
    
    # Check arguments
    
    croak "you must specify an expression" unless defined $expr && ! ref $expr && $expr ne '';
    $label ||= 'found at least one record';

    # If the given expression is a single decimal number, assume it is a key.
    
    if ( $expr =~ /^\d+$/ )
    {
	my $key_name = get_table_property($table, 'PRIMARY_KEY') or
	    croak "could not determine primary key for table '$table'";
	$expr = "$key_name = $expr";
    }
    
    # Execute the SQL expression and test the result.
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $sql = "SELECT COUNT(*) FROM $table WHERE $expr";
    
    $T->debug_line($sql);
    
    my ($count) = $dbh->selectrow_array($sql);
    
    $T->debug_line("Returned $count rows");
    $T->debug_skip;

    ok( $count, $label );

    return $count;
}


sub ok_no_record {
    
    my ($T, $table, $expr, $label) = @_;
    
    my $dbh = $T->dbh;
    
    # Check arguments
    
    croak "you must specify an expression" unless defined $expr && ! ref $expr && $expr ne '';
    $label ||= 'record was absent';

    # If the given expression is a single decimal number, assume it is a key.
    
    if ( $expr =~ /^\d+$/ )
    {
	my $key_name = get_table_property($table, 'PRIMARY_KEY') or
	    croak "could not determine primary key for table '$table'";
	$expr = "$key_name = $expr";
    }
    
    # Execute the SQL expression and test the result.
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $sql = "SELECT COUNT(*) FROM $table WHERE $expr";
    
    $T->debug_line($sql);
    
    my ($count) = $dbh->selectrow_array($sql);
    
    $T->debug_line("Returned $count rows");
    $T->debug_skip;

    ok( $count == 0, $label );
}


sub ok_count_records {
    
    my ($T, $count, $table, $expr, $label) = @_;
    
    my $dbh = $T->dbh;

    croak "invalid count '$count'" unless defined $count && $count =~ /^\d+$/;
    croak "you must specify a table" unless $table;
    croak "you must specify a valid SQL expression" unless $expr;

    $label ||= "found proper number of records";
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $sql = "SELECT count(*) FROM $table WHERE $expr";
    
    $T->debug_line($sql);
    
    my ($result) = $dbh->selectrow_array($sql);
    
    if ( defined $result && $result == $count )
    {
	pass($label);
    }

    else
    {
	$result //= "undefined";
	
	fail("$label");
	diag("     got: $result");
	diag("expected: $count"); 
    }
}


sub inserted_keys {
    
    my ($T, $edt) = @_;

    $edt //= $T->{last_edt};
    
    return unless $edt;
    return $edt->inserted_keys;
}


sub updated_keys {
    
    my ($T, $edt) = @_;

    $edt //= $T->{last_edt};
    
    return unless $edt;
    return $edt->updated_keys;
}


sub replaced_keys {
    
    my ($T, $edt) = @_;

    $edt //= $T->{last_edt};
    
    return unless $edt;
    return $edt->replaced_keys;
}


sub deleted_keys {
    
    my ($T, $edt) = @_;

    $edt //= $T->{last_edt};
    
    return unless $edt;
    return $edt->deleted_keys;
}


sub clear_table {
    
    my ($T, $table) = @_;

    $T->clear_edt;
    
    my $dbh = $T->dbh;
    
    croak "you must specify a table" unless $table;
    
    my $sql = "DELETE FROM $table";
    
    $T->debug_line($sql);
    
    my $results = $dbh->do($sql);
    
    $sql = "ALTER TABLE $table AUTO_INCREMENT = 1";

    $T->debug_line($sql);
    
    eval {
	$dbh->do($sql);
    };
    
    if ( $results )
    {
	$T->debug_line("Deleted $results rows");
    }
    
    $T->debug_skip;
    
    
    
    return;
}


sub add_test_records {
    
    my ($T, $table) = @_;

    # $$$
}


sub fetch_records_by_key {
    
    my ($T, $table, @keys) = @_;
    
    my $dbh = $T->dbh;

    croak "you must specify a table" unless $table;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;

    unless ( @keys && $keys[0] )
    {
	fail("no keys were defined");
	return;
    }
    
    my @key_list;

    foreach my $k ( @keys )
    {
	next unless defined $k;
	croak "keys cannot be refs" if ref $k;
	push @key_list, $dbh->quote($k);
    }
    
    return unless @key_list;
    
    my $key_string = join(',', @key_list);
    my $key_name = get_table_property($table, 'PRIMARY_KEY');
    
    croak "could not determine primary key for table '$table'" unless $key_name;
    
    my $sql = "SELECT * FROM $table WHERE $key_name in ($key_string)";

    $T->debug_line($sql);
    
    my $results = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    if ( ref $results eq 'ARRAY' )
    {
	$T->debug_line("Returned " . scalar(@$results) . " rows");
	$T->debug_skip;
	
	if ( @$results )
	{
	    pass("found records");
	}

	else
	{
	    fail("found records");
	}
	
	return @$results;
    }

    else
    {
	$T->debug_line("Returned no results");
	$T->debug_skip;
	fail("found records");
	return;
    }
}


sub fetch_records_by_expr {

    my ($T, $table, $expr) = @_;
    
    my $dbh = $T->dbh;
    
    croak "you must specify a table" unless $table;
    croak "you must specify a valid SQL expression" unless $expr;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $sql = "SELECT * FROM $table WHERE $expr";
    
    $T->debug_line($sql);
    
    my $results = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    if ( ref $results eq 'ARRAY' )
    {
	$T->debug_line("Returned " . scalar(@$results) . " rows");
	$T->debug_skip;
	
	if ( @$results )
	{
	    pass("found records");
	}

	else
	{
	    fail("found records");
	}

	return @$results;
    }
    
    else
    {
	$T->debug_line("Returned no results");
	$T->debug_skip;
	fail("found records");
	return;
    }
}


sub fetch_keys_by_expr {

    my ($T, $table, $expr) = @_;
    
    my $dbh = $T->dbh;
    
    croak "you must specify a table" unless $table;
    croak "you must specify a valid SQL expression" unless $expr;
    
    my $key_name = get_table_property($table, 'PRIMARY_KEY');
    
    croak "could not determine primary key for table '$table'" unless $key_name;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $sql = "SELECT $key_name FROM $table WHERE $expr";
    
    $T->debug_line($sql);
    
    my $results = $dbh->selectcol_arrayref($sql);
    
    if ( ref $results eq 'ARRAY' )
    {
	$T->debug_line("Returned " . scalar(@$results) . " rows");
	$T->debug_skip;
	
	if ( @$results )
	{
	    pass("found keys");
	}

	else
	{
	    fail("found keys");
	}

	return @$results;
    }
    
    else
    {
	$T->debug_line("Returned no results");
	$T->debug_skip;
	fail("found keys");
	return;
    }
}



1;

