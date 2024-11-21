# 
# EditTransaction project
# 
#   ETBasicTest.pm - a class for use in testing EditTransaction.pm
#   
#   This class is a subclass of EditTransaction, and is used by the unit tests
#   for EditTransaction and its related classes.
#   
#   Each instance of this class is responsible for initiating a database transaction, checking a
#   set of records for insertion, update, or deletion, and either committing or rolling back the
#   transaction depending on the results of the checks. If the object is destroyed, the transaction will
#   be rolled back.

use strict;

package ETBasicTest;

use EditTransaction;

use parent 'EditTransaction';

use TableDefs qw(%TABLE set_table_name set_table_group set_table_property set_column_property);

use Carp qw(carp croak);

use Role::Tiny::With;

use Class::Method::Modifiers qw(before around);

with 'EditTransaction::Mod::MariaDB';

use namespace::clean;


# At runtime, set column properties for our test table
# ----------------------------------------------------

{
    set_table_name(EDT_TEST => 'edt_test');
    set_table_name(EDT_TYPES => 'edt_extended');
    set_table_name(EDT_SUB => 'edt_sub');
    set_table_name(EDT_NOPRIM => 'edt_noprimary');
    set_table_name(EDT_AUTH => 'edt_auth');
    set_table_name(EDT_ANY => 'edt_any');
    
    set_table_group('edt_test' => 'EDT_TEST', 'EDT_TYPES', 'EDT_SUB', 'EDT_NOPRIM');
    
    # Set properties for EDT_TEST
    
    set_table_property(EDT_TEST => PRIMARY_KEY => 'test_no');
    set_table_property(EDT_TEST => PRIMARY_FIELD => 'test_id');
    set_table_property(EDT_TEST => TABLE_COMMENT => 'Test comment');
    
    set_column_property(EDT_TEST => string_req => REQUIRED => 1);
    set_column_property(EDT_TEST => string_val => ALTERNATE_NAME => 'alt_val');
    
    # Set properties for EDT_SUB
    
    set_table_property(EDT_SUB => SUPERIOR_TABLE => 'EDT_TEST');
    set_table_property(EDT_SUB => CAN_POST => 'AUTHORIZED');
    set_table_property(EDT_SUB => CAN_MODIFY => 'AUTHORIZED');
    set_table_property(EDT_SUB => PRIMARY_KEY => 'aux_no');
    set_table_property(EDT_SUB => PRIMARY_FIELD => 'aux_id');
    
    set_column_property(EDT_SUB => test_no => FOREIGN_KEY => 'EDT_TEST');
    set_column_property(EDT_SUB => name => REQUIRED => 1);
    
    # Set properties for EDT_TYPES
    
    set_table_property(EDT_TYPES => CAN_MODIFY => 'ALL');
    set_column_property(EDT_TYPES => string_req => COLUMN_COMMENT => 'This is a test comment.');
    set_column_property(EDT_TYPES => string_val => ALTERNATE_NAME => 'alt_val');
    
    # Set properties for EDT_AUTH
    
    set_table_property(EDT_AUTH => CAN_POST => 'AUTHORIZED');
    set_table_property(EDT_AUTH => PRIMARY_KEY => 'test_no');
    set_table_property(EDT_AUTH => PRIMARY_FIELD => 'test_id');
    
    set_column_property(EDT_AUTH => string_req => REQUIRED => 1);
    set_column_property(EDT_AUTH => string_req => COLUMN_COMMENT => 'This is a test comment.');
    set_column_property(EDT_AUTH => string_val => ALTERNATE_NAME => 'alt_val');
    
    set_table_property(EDT_ANY => CAN_POST => 'LOGGED_IN');
    set_table_property(EDT_ANY => PRIMARY_KEY => 'any_no');
    
    set_column_property(EDT_ANY => string_req => REQUIRED => 1);
    
    __PACKAGE__->register_allowances('TEST_DEBUG');
    __PACKAGE__->register_conditions(E_TEST => ["TEST ERROR '&1'", "TEST ERROR"],
				     W_TEST => ["TEST WARNING '&1'", "TEST WARNING"]);
    
    # __PACKAGE__->register_database_module();
}


# The following methods allow the use of the test database instead of the main one.
# ---------------------------------------------------------------------------------

# enable_test_mode ( class, table, ds )
# 
# Change the global variables that hold the names of the eduresource tables over to the test
# database. If $ds is either 1 or a reference to a Web::DataService object with the debug flag
# set, then print out a debugging message.

# sub enable_test_mode {
    
#     my ($class, $table, $ds) = @_;
    
#     croak "You must define 'test_db' in the configuration file" unless $TEST_DB;
    
#     $EDT_TEST = alternate_table($TEST_DB, $EDT_TEST);
#     $EDT_SUB = alternate_table($TEST_DB, $EDT_SUB);
#     $EDT_ANY = alternate_table($TEST_DB $EDT_ANY);
    
#     if ( $ds && $ds == 1 || ref $ds && $ds->debug )
#     {
# 	$ds->debug_line("TEST MODE: enable 'edt_test'\n");
#     }
    
#     return 1;
# }


# sub disable_test_mode {

#     my ($class, $table, $ds) = @_;
    
#     $EDT_TEST = original_table($EDT_TEST);
#     $EDT_SUB = original_table($EDT_SUB);
#     $EDT_ANY = original_table($EDT_ANY);
    
#     if ( $ds && $ds == 1 || ref $ds && $ds->debug )
#     {
# 	$ds->debug_line("TEST MODE: disable 'edt_test'\n");
#     }
    
#     return 2;
# }


# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

before 'authorize_action' => sub {
    
    my ($edt, $action, $operation, $table, $keyexpr) = @_;
    
    my $record = $action->record;

    if ( $record && $record->{string_req} )
    {
	if ( $record->{string_req} eq 'authorize exception' )
	{
	    die "exception during authorization";
	}
	
	# elsif ( $record->{string_req} eq 'authorize save' )
	# {
	#     $edt->{save_authorize_action} = $action;
	#     $edt->{save_authorize_operation} = $operation;
	#     $edt->{save_authorize_table} = $table;
	#     $edt->{save_authorize_keyexpr} = $keyexpr;
	# }

	# elsif ( $record->{string_req} eq 'authorize methods' )
	# {
	#     my $keyexpr = $action->keyexpr;
	#     my @keylist = $action->keylist;
	#     my @values = $edt->test_old_values($action, $table);
	    
	#     $edt->{save_method_keyexpr} = $keyexpr;
	#     $edt->{save_method_keylist} = \@keylist;
	#     $edt->{save_method_values} = \@values;
	# }
    }
};


before 'validate_action' => sub {

    my ($edt, $action, $operation, $table, $keyexpr) = @_;
    
    my $record = $action->record;
    
    if ( $record && $record->{string_req} )
    {
	if ( $record->{string_req} eq 'validate exception' )
	{
	    die "exception during validation";
	}
    
# 	elsif ( $record->{string_req} eq 'validate save' )
# 	{
# 	    $edt->{save_validate_action} = $action;
# 	    $edt->{save_validate_operation} = $operation;
# 	    $edt->{save_validate_table} = $table;
# 	    $edt->{save_validate_keyexpr} = $keyexpr;
# 	    $edt->{save_validate_errors} = $action->has_errors;
# 	}
	
# 	elsif ( $record->{string_req} eq 'validate methods' )
# 	{
# 	    my $keyexpr = $action->keyexpr;
# 	    my @keylist = $action->keylist;
# 	    my @values = $edt->test_old_values($action, $table);
	    
# 	    $edt->{save_method_keyexpr} = $keyexpr;
# 	    $edt->{save_method_keylist} = \@keylist;
# 	    $edt->{save_method_values} = \@values;
# 	}
    }
};


# around 'check_table_permission' => sub {
    
#     my ($orig, $edt, $table, $requested) = @_;
    
#     if ( $edt->{testing}{suppress_insert_key} && $requested eq 'insert' )
#     {
# 	return 'none';
#     }
    
#     else
#     {
# 	return $orig->(@_);
#     }
# };


sub test_old_values {

    my ($edt, $action, $table) = @_;

    my @values;
    
    if ( $table eq 'EDT_TEST' )
    {
	@values = $edt->get_old_values($table, $action->keyexpr, 'string_req, string_val');
    }
    
    elsif ( $table eq 'EDT_SUB' )
    {
	@values = $edt->get_old_values($table, $action->keyexpr, 'name');
    }
    
    return @values;
}


sub initialize_instance {
    
    my ($edt) = @_;
    
    $edt->{save_init_count} = 0;
    $edt->{save_final_count} = 0;
    $edt->{save_cleanup_count} = 0;
    
    $edt->{save_before_count} = 0;
    $edt->{save_after_count} = 0;
    $edt->{save_cleanup_action_count} = 0;    
}


sub initialize_transaction {

    my ($edt, $table) = @_;

    if ( $edt->get_attr('initialize exception') )
    {
	die "initialize exception";
    }

    elsif ( $edt->get_attr('initialize error') )
    {
	$edt->add_condition('E_TEST', 'initialize');
    }
    
    elsif ( $edt->get_attr('initialize warning') )
    {
	$edt->add_condition('W_TEST', 'initialize');
    }
    
    elsif ( my $value = $edt->get_attr('initialize add') )
    {
	my $quoted = $edt->dbh->quote($value);
	$edt->dbh->do("INSERT INTO $TABLE{EDT_TEST} (string_req) values ($quoted)");
    }
    
    $edt->{save_init_count}++;
    $edt->{save_init_table} = $table;
    $edt->{save_init_status} = $edt->status;
    $edt->{save_init_action} = $edt->current_action;
}


sub finalize_transaction {

    my ($edt, $table) = @_;
    
    if ( $edt->get_attr('finalize exception') )
    {
	die "finalize exception";
    }
    
    elsif ( $edt->get_attr('finalize error' ) )
    {
	$edt->add_condition('E_TEST', 'finalize');
    }
    
    elsif ( $edt->get_attr('finalize warning') )
    {
	$edt->add_condition('W_TEST', 'finalize');
    }
    
    elsif ( my $value = $edt->get_attr('finalize add') )
    {
	my $quoted = $edt->dbh->quote($value);
	$edt->dbh->do("INSERT INTO $TABLE{EDT_TEST} (string_req) values ($quoted)");
    }
    
    elsif ( my $value = $edt->get_attr('finalize remove') )
    {
	my $quoted = $edt->dbh->quote($value);
	$edt->dbh->do("DELETE FROM $TABLE{EDT_TEST} WHERE string_req=$quoted");	
    }
    
    $edt->{save_final_count}++;
    $edt->{save_final_table} = $table;
    $edt->{save_final_status} = $edt->status;
    $edt->{save_final_action} = $edt->current_action;
}


sub cleanup_transaction {
    
    my ($edt, $table) = @_;
    
    if ( $edt->get_attr('cleanup exception') )
    {
	die "generated exception";
    }
    
    $edt->{save_cleanup_count}++;
    $edt->{save_cleanup_table} = $table;
    $edt->{save_cleanup_status} = $edt->status;
}


sub before_action {
    
    my ($edt, $action, $operation, $table) = @_;
    
    my $record = $action->record;
    
    if ( $record && $record->{string_req} )
    {
	if ( $record->{string_req} eq 'before exception' )
	{
	    die "generated exception";
	}

	elsif ( $record->{string_req} eq 'before error' )
	{
	    $edt->add_condition('E_TEST', 'before');
	}
	
	elsif ( $record->{string_req} eq 'before warning' )
	{
	    $edt->add_condition('W_TEST', 'before');
	}

	elsif ( $record->{string_req} eq 'before set_attr' )
	{
	    $action->set_attr(test => 'abc');
	}

	elsif ( $record->{string_req} eq 'before abandon' )
	{
	    $edt->abort_action;
	}

	elsif ( $record->{string_req} eq 'before methods' )
	{
	    my $keyexpr = $action->keyexpr;
	    my @keylist = $action->keylist;
	    my @values = $edt->test_old_values($action, $table);
	    
	    $edt->{save_method_keyexpr} = $keyexpr;
	    $edt->{save_method_keylist} = \@keylist;
	    $edt->{save_method_values} = \@values;
	}
    }
    
    elsif ( $operation eq 'delete_cleanup' )
    {
	my $keyexpr = $action->keyexpr;
	
	$edt->{save_before_keyexpr} = $keyexpr;
    }
    
    if ( my $value = $edt->get_attr('before add') )
    {
	my $quoted = $edt->dbh->quote($value);
	$edt->dbh->do("INSERT INTO $TABLE{EDT_TEST} (string_req) values ($quoted)");
    }
    
    $edt->{save_before_count}++;
    $edt->{save_before_action} = $action;
    $edt->{save_before_operation} = $operation;
    $edt->{save_before_table} = $table;
    $edt->{save_before_status} = $edt->transaction;
}


sub after_action {

    my ($edt, $action, $operation, $table, $keyval) = @_;
    
    my $record = $action->record;
    
    if ( $record && $record->{string_req} )
    {
	if ( $record->{string_req} eq 'after exception' )
	{
	    die "generated exception";
	}
	
	elsif ( $record->{string_req} eq 'after error' )
	{
	    $edt->add_condition('E_TEST', 'after');
	}
	
	elsif ( $record->{string_req} eq 'after warning' )
	{
	    $edt->add_condition('W_TEST', 'after');
	}

	elsif ( $record->{string_req} eq 'before set_attr' )
	{
	    $edt->{save_after_attr} = $action->get_attr('test');
	}
	
	elsif ( $record->{string_req} eq 'after methods' )
	{
	    my $keyexpr = $edt->get_keyexpr($action);
	    my @keylist = $action->keylist;
	    my @values = $edt->test_old_values($action, $table);
	    
	    $edt->{save_method_keyval} = $keyval;
	    $edt->{save_method_keyexpr} = $keyexpr;
	    $edt->{save_method_keylist} = \@keylist;
	    $edt->{save_method_values} = \@values;
	}
    }
    
    elsif ( $operation eq 'delete_cleanup' )
    {
	my $keyexpr = $action->keyexpr;
	
	$edt->{save_after_keyexpr} = $keyexpr;
    }
    
    if ( $edt->get_attr('after delete') )
    {
	my $value = $edt->get_attr('before add');
	my $quoted = $edt->dbh->quote($value);
	$edt->dbh->do("DELETE FROM $table WHERE string_req=$quoted");
    }

    # if ( $operation eq 'delete' && $table eq 'EDT_TEST' )
    # {
    # 	my $keyexpr = $action->keyexpr;
	
    # 	if ( $keyexpr )
    # 	{
    # 	    $edt->dbh->do("DELETE FROM $TABLE{EDT_SUB} WHERE $keyexpr");
    # 	    $edt->{save_delete_aux} = $keyexpr;
    # 	}
    # }
    
    $edt->{save_after_count}++;
    $edt->{save_after_action} = $action;
    $edt->{save_after_operation} = $operation;
    $edt->{save_after_table} = $table;
    $edt->{save_after_result} = $keyval;
    $edt->{save_after_status} = $edt->transaction;
}


sub cleanup_action {
    
    my ($edt, $action, $operation, $table) = @_;
    
    if ( $edt->get_attr('cleanup action exception') )
    {
	$edt->add_condition('W_TEST', 'cleanup exception');
	die "generated exception";
    }
    
    $edt->{save_cleanup_action_count}++;
    $edt->{save_cleanup_action_action} = $action;
    $edt->{save_cleanup_action_operation} = $operation;
    $edt->{save_cleanup_action_table} = $table;
    $edt->{save_cleanup_action_status} = $edt->transaction;
}


# debug_line ( text )
#
# Capture debug output for testing.

# sub debug_line {
    
#     return unless ref $_[0] && defined $_[1];
#     return $_[0]->SUPER::debug_line($_[1]) unless $_[0]->{allows}{TEST_DEBUG};
    
#     push @{$_[0]->{debug_output}}, $_[1] if $_[0]->{debug};
# }


# sub error_line {

#     return unless ref $_[0] && defined $_[1];
#     return $_[0]->SUPER::error_line($_[1]) unless $_[0]->{allows}{TEST_DEBUG};

#     push @{$_[0]->{debug_output}}, $_[1] unless $_[0]->{silent};
# }


sub write_debug_output {

    return $_[0]->SUPER::write_debug_output($_[1]) unless $_[0]->{allows}{TEST_DEBUG};
    
    push @{$_[0]->{debug_output}}, $_[1];
}


sub clear_debug_output {

    my ($edt) = @_;

    $edt->{debug_output} = [ ];
}


sub debug_output {

    my ($edt) = @_;

    if ( ref $edt->{debug_output} eq 'ARRAY' )
    {
	return join "\n", $edt->{debug_output}->@*;
    }

    else
    {
	return '';
    }
}


sub has_debug_output {

    my ($edt, $regex) = @_;
    
    croak "you must specify a regular expression" unless $regex && ref $regex eq 'Regexp';
    
    return unless $edt->{debug_output};
    
    foreach my $line ( @{$edt->{debug_output}} )
    {
	return 1 if $line =~ $regex;
    }

    return;
}


# establish_test_tables ( class, dbh, options )
# 
# This class method creates database tables necessary to use this class for testing purposes, or
# replaces the existing ones.

sub establish_test_tables {
    
    my ($class, $dbh, $options) = @_;
    
    $options ||= { };
    
    # Create, or re-create, the table 'edt_test'.
    
    $dbh->do("SET SESSION foreign_key_checks = 0");
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDT_TEST}");
    
    $dbh->do("CREATE TABLE $TABLE{EDT_TEST} (
		test_no int unsigned primary key auto_increment,
		string_val varchar(40) not null default '',
		string_req varchar(40) not null,
		signed_val mediumint)
		
		default charset utf8");
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDT_SUB}");
    
    $dbh->do("CREATE TABLE $TABLE{EDT_SUB} (
		aux_no int unsigned primary key auto_increment,
		name varchar(255) not null default '',
		test_no int unsigned not null default 0,
		unique key (name)) default charset utf8");

    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDT_TYPES}");
    
    $dbh->do("CREATE TABLE $TABLE{EDT_TYPES} (
		name varchar(40) not null primary key,
		interval_no int unsigned not null default 0,
		string_val varchar(40) not null default '',
		latin1_val varchar(40) charset latin1 not null default '',
		greek_val varchar(40) charset greek not null default '',
		binary_val varbinary(40),
		text_val text,
		blob_val blob,
		signed_val mediumint,
		unsigned_val mediumint unsigned not null default 0,
		tiny_val tinyint unsigned,
		decimal_val decimal(5,2),
		unsdecimal_val decimal(5,2) unsigned not null default 0,
		double_val double,
		unsfloat_val float unsigned not null default 0,
		boolean_val boolean,
		enum_val enum('abc', 'd\N{U+1F10}f', 'ghi', '''jkl'''),
		set_val set('abc', 'd\N{U+1F10}f', 'ghi', '''jkl'''),
		dcr timestamp,
		dmd timestamp,
		dcrauto timestamp default current_timestamp,
		dmdauto timestamp default current_timestamp on update current_timestamp)
		
		default charset utf8");
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDT_NOPRIM}");
    
    $dbh->do("CREATE TABLE $TABLE{EDT_NOPRIM} (
		name varchar(40) not null,
		value varchar(40) not null,
		unique key (name, value)) default charset utf8");
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDT_ANY}");
    
    $dbh->do("CREATE TABLE $TABLE{EDT_ANY} (
		any_no int unsigned primary key auto_increment,
		authorizer_no int unsigned not null,
		enterer_no int unsigned not null,
		enterer_id varchar(36) not null,
		string_req varchar(255) not null default '') default charset utf8");
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDT_AUTH}");
    
    $dbh->do("CREATE TABLE $TABLE{EDT_AUTH} (
		test_no int unsigned primary key auto_increment,
		authorizer_no int unsigned not null default 0,
		enterer_no int unsigned not null default 0,
		modifier_no int unsigned not null default 0,
		interval_no int unsigned not null default 0,
		string_val varchar(40) not null default '',
		string_req varchar(40) not null default '',
		latin1_val varchar(40) charset latin1 not null default '',
		greek_val varchar(40) charset greek not null default '',
		binary_val varbinary(40),
		text_val text,
		blob_val blob,
		signed_val mediumint,
		unsigned_val mediumint unsigned not null default 0,
		tiny_val tinyint unsigned,
		decimal_val decimal(5,2),
		unsdecimal_val decimal(5,2) unsigned not null default 0,
		double_val double,
		unsfloat_val float unsigned not null default 0,
		boolean_val boolean,
		enum_val enum('abc', 'd\N{U+1F10}f', 'ghi', '''jkl'''),
		set_val set('abc', 'd\N{U+1F10}f', 'ghi', '''jkl'''),
		admin_str varchar(40) not null default '',
		admin_lock boolean not null default 0,
		owner_lock boolean not null default 0,
		created timestamp default current_timestamp,
		modified timestamp default current_timestamp) default charset utf8");
    
    $dbh->do("SET SESSION foreign_key_checks = 1");
    
    return 1;
}


1;


