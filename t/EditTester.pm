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
use TableDefs qw(init_table_names select_test_tables get_table_property get_column_properties $EDT_TEST
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
    
    my $dbh;
    
    eval {
	$dbh = connectDB("config.yml");
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
		     debug => $options->{debug} };
    
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
	# select_test_tables('edt_test', 1);
	# select_test_tables('session_data', 1);
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
			('SESSION-STUDENT','USERID-STUDENT','3998','3996','student',0),
			('SESSION-SUPERUSER','USERID-SUPERUSER','3999','3999','authorizer',1),
			('SESSION-WITH-ADMIN','USERID-WITH-ADMIN','3999','3991','enterer',0)");

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
			(3991,'A. Admin','Admin, A.','Test')");

	$dbh->do("CREATE TABLE IF NOT EXISTS $TABLE_PERMS (
  `person_no` int(10) unsigned NOT NULL,
  `table_name` varchar(80) NOT NULL,
  `permission` enum('none','view','post','view/post','admin') NOT NULL,
  UNIQUE KEY `person_no` (`person_no`,`table_name`))");
	
	my $table_name = $EDT_TEST; $table_name =~ s/^\w+[.]//;
	
	$dbh->do("REPLACE INTO $TABLE_PERMS (person_no, table_name, permission)
			VALUES (3991, '$table_name', 'admin')");
	
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("Could not establish session data. Message was: $msg");
	BAIL_OUT;
    }
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
    
    my $allow = { };
    
    if ( ref $options eq 'HASH' )
    {
	foreach my $k ( keys %$options )
	{
	    if ( $k =~ /^[A-Z_]+$/ && $options->{$k} )
	    {
		$allow->{$k} = 1;
	    }
	}
    }
    
    $allow->{DEBUG_MODE} = 1 if $T->{debug};
    
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

    my $label = shift;

    croak "you must specify a label" unless $label;
    
    unless ($result )
    {
	$T->diag_errors;
	fail($label);
    }
}


sub ok_no_errors {

    my $T = shift;
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift : $T->{last_edt};
    
    my $label = shift;
    croak "you must specify a label" unless $label && ! ref $label;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( $edt->errors )
    {
	$T->diag_errors($edt);
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
    
    my $regexp = shift;
    croak "you must specify a regexp" unless $regexp && ref $regexp eq 'Regexp';
    
    my $label = shift;
    croak "you must specify a label" unless $label && ! ref $label;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    foreach my $e ( $edt->error_strings )
    {
	if ( $e =~ $regexp )
	{
	    pass($label);
	    return 1;
	}
    }

    fail($label);
    return;
}

	
sub ok_no_warnings {

    my $T = shift;
    
    my $edt = ref $_[0] && $_[0]->isa('EditTransaction') ? shift : $T->{last_edt};
    
    my $label = shift;
    croak "you must specify a label" unless $label && ! ref $label;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    if ( $edt->warnings )
    {
	$T->diag_warnings($edt);
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
    
    my $regexp = shift;
    croak "you must specify a regexp" unless $regexp && ref $regexp eq 'Regexp';
    
    my $label = shift;
    croak "you must specify a label" unless $label && ! ref $label;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    foreach my $e ( $edt->warning_strings )
    {
	if ( $e =~ $regexp )
	{
	    pass($label);
	    return 1;
	}
    }

    fail($label);
    return;
}

	
sub ok_last_exception {

    my ($T, $regex, $label) = @_;
    
    croak "you must specify a regular expression" unless $regex && ref $regex eq 'Regexp';
    croak "you must specify a label" unless $label;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $msg = $T->{last_exception};
    
    ok( $msg && $msg =~ $regex, $label);
}


sub ok_found_record {
    
    my ($T, $table, $expr, $label) = @_;
    
    my $dbh = $T->dbh;
    
    # Check arguments
    
    croak "you must specify an expression" unless defined $expr && ! ref $expr && $expr ne '';
    $label ||= 'record was found';

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


sub diag_errors {

    my ($T, $edt) = @_;

    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $edt //= $T->{last_edt};
    return unless ref $edt;
    
    foreach my $e ( $edt->error_strings )
    {
        diag($e);
    }
}


sub diag_warnings {

    my ($T, $edt) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    $edt //= $T->{last_edt};
    return unless ref $edt;
    
    foreach my $w ( $edt->warning_strings )
    {
        diag($w);
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

    my $dbh = $T->dbh;

    croak "you must specify a table" unless $table;

    my $sql = "DELETE FROM $table";
    
    $T->debug_line($sql);
    
    my $results = $dbh->do($sql);

    if ( $results )
    {
	$T->debug_line("Deleted $results rows");
    }

    $T->debug_skip;

    return;
}


sub fetch_records_by_key {
    
    my ($T, $table, @keys) = @_;
    
    my $dbh = $T->dbh;

    croak "you must specify a table" unless $table;
    croak "you must specify at least one key" unless @keys && $keys[0];
    
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
	return @$results;
    }

    else
    {
	$T->debug_line("Returned no results");
	$T->debug_skip;
	return;
    }
}


sub fetch_records_by_expr {

    my ($T, $table, $expr) = @_;

    my $dbh = $T->dbh;

    croak "you must specify a table" unless $table;
    croak "you must specify a valid SQL expression" unless $expr;
    
    my $sql = "SELECT * FROM $table WHERE $expr";

    $T->debug_line($sql);

    my $results = $dbh->selectall_arrayref($sql, { Slice => { } });

    if ( ref $results eq 'ARRAY' )
    {
	$T->debug_line("Returned " . scalar(@$results) . " rows");
	$T->debug_skip;
	return @$results;
    }

    else
    {
	$T->debug_line("Returned no results");
	$T->debug_skip;
	return;
    }
}


1;

