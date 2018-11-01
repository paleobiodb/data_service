# 
# The Paleobiology Database
# 
#   TestEdit.pm - a class for use in testing EditTransaction.pm
#   
#   This class is a subclass of EditTransaction, and is used by the unit tests
#   for EditTransaction and its related classes.
#   
#   Each instance of this class is responsible for initiating a database transaction, checking a
#   set of records for insertion, update, or deletion, and either committing or rolling back the
#   transaction depending on the results of the checks. If the object is destroyed, the transaction will
#   be rolled back.


package EditTest;

use strict;

use Carp qw(carp croak);
use Try::Tiny;

use TableDefs qw(%TABLE set_table_name set_table_group set_table_property set_column_property);

use base 'EditTransaction';

use namespace::clean;

# Table names

# our $EDT_TEST = 'edt_test';
# our $EDT_AUX = 'edt_aux';
# our $EDT_ANY = 'edt_any';


# At runtime, set column properties for our test table
# ----------------------------------------------------

{
    set_table_name(EDT_TEST => 'edt_test');
    set_table_name(EDT_AUX => 'edt_aux');
    set_table_name(EDT_ANY => 'edt_any');
    
    set_table_group('edt_test' => 'EDT_TEST', 'EDT_AUX', 'EDT_ANY');
    
    set_table_property(EDT_TEST => CAN_POST => 'AUTHORIZED');
    set_table_property(EDT_TEST => ALLOW_DELETE => 1);
    set_table_property(EDT_TEST => PRIMARY_KEY => 'test_no');
    set_table_property(EDT_TEST => TABLE_COMMENT => 'This table is used for testing EditTransaction.pm and its subclass EditTest.pm');
    
    set_column_property(EDT_TEST => string_req => REQUIRED => 1);
    set_column_property(EDT_TEST => string_req => COLUMN_COMMENT => 'This is a test comment.');
    set_column_property(EDT_TEST => string_val => ALTERNATE_NAME => 'alt_val');
    set_column_property(EDT_TEST => string_val => VALIDATOR => 'test_validator');
    set_column_property(EDT_TEST => admin_str => ADMIN_SET => 1);
    
    set_table_property(EDT_AUX => PERMISSION_TABLE => 'EDT_TEST');
    set_table_property(EDT_AUX => CAN_POST => 'AUTHORIZED');
    set_table_property(EDT_AUX => CAN_MODIFY => 'AUTHORIZED');
    set_table_property(EDT_AUX => ALLOW_DELETE => 1);
    set_table_property(EDT_AUX => PRIMARY_KEY => 'aux_no');
    
    set_column_property(EDT_AUX => test_no => FOREIGN_TABLE => 'EDT_TEST');
    set_column_property(EDT_AUX => test_no => ALTERNATE_NAME => 'test_id');
    set_column_property(EDT_AUX => name => REQUIRED => 1);
    
    set_table_property(EDT_ANY => CAN_POST => 'LOGGED_IN');
    set_table_property(EDT_ANY => ALLOW_DELETE => 1);
    set_table_property(EDT_ANY => PRIMARY_KEY => 'any_no');
    
    set_column_property(EDT_ANY => string_req => REQUIRED => 1);
    
    EditTest->register_allowances('TEST_DEBUG');
    EditTest->register_conditions(E_TEST => "TEST ERROR '%1'",
				  W_TEST => "TEST WARNING '%1'");
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
#     $EDT_AUX = alternate_table($TEST_DB, $EDT_AUX);
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
#     $EDT_AUX = original_table($EDT_AUX);
#     $EDT_ANY = original_table($EDT_ANY);
    
#     if ( $ds && $ds == 1 || ref $ds && $ds->debug )
#     {
# 	$ds->debug_line("TEST MODE: disable 'edt_test'\n");
#     }
    
#     return 2;
# }


# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

sub authorize_action {
    
    my ($edt, $action, $operation, $table, $keyexpr) = @_;
    
    my $record = $action->record;

    if ( $record && $record->{string_req} )
    {
	if ( $record->{string_req} eq 'authorize exception' )
	{
	    die "generated exception";
	}
	
	elsif ( $record->{string_req} eq 'authorize error' )
	{
	    $edt->add_condition('E_TEST', 'xyzzy');
	}

	elsif ( $record->{string_req} eq 'authorize warning' )
	{
	    $edt->add_condition('W_TEST', 'xyzzy');
	}

	elsif ( $record->{string_req} eq 'authorize save' )
	{
	    $edt->{save_authorize_action} = $action;
	    $edt->{save_authorize_operation} = $operation;
	    $edt->{save_authorize_table} = $table;
	    $edt->{save_authorize_keyexpr} = $keyexpr;
	}

	elsif ( $record->{string_req} eq 'authorize methods' )
	{
	    my $keyexpr = $action->keyexpr;
	    my @keylist = $action->keylist;
	    my @values = $edt->test_old_values($action, $table);
	    
	    $edt->{save_method_keyexpr} = $keyexpr;
	    $edt->{save_method_keylist} = \@keylist;
	    $edt->{save_method_values} = \@values;
	}
    }
    
    return EditTransaction::authorize_action(@_);
}


sub validate_action {

    my ($edt, $action, $operation, $table, $keyexpr) = @_;

    my $record = $action->record;

    if ( $record && $record->{string_req} )
    {
	if ( $record->{string_req} eq 'validate exception' )
	{
	    die "generated exception";
	}
	
	elsif ( $record->{string_req} eq 'validate error' )
	{
	    $edt->add_condition('E_TEST', 'xyzzy');
	}

	elsif ( $record->{string_req} eq 'validate warning' )
	{
	    $edt->add_condition('W_TEST', 'xyzzy');
	}

	elsif ( $record->{string_req} eq 'validate save' )
	{
	    $edt->{save_validate_action} = $action;
	    $edt->{save_validate_operation} = $operation;
	    $edt->{save_validate_table} = $table;
	    $edt->{save_validate_keyexpr} = $keyexpr;
	    $edt->{save_validate_errors} = $action->has_errors;
	}
	
	elsif ( $record->{string_req} eq 'validate methods' )
	{
	    my $keyexpr = $action->keyexpr;
	    my @keylist = $action->keylist;
	    my @values = $edt->test_old_values($action, $table);
	    
	    $edt->{save_method_keyexpr} = $keyexpr;
	    $edt->{save_method_keylist} = \@keylist;
	    $edt->{save_method_values} = \@values;
	}
    }

    if ( $record && $record->{name} )
    {
	if ( $record->{name} =~ /validate label (.*)/i )
	{
	    $edt->{save_validate_label} = $edt->label_table($1);
	}
    }
    
    return EditTransaction::validate_action(@_);
}


sub test_old_values {

    my ($edt, $action, $table) = @_;

    my @values;
    
    if ( $table eq 'EDT_TEST' )
    {
	@values = $edt->get_old_values($table, $action->keyexpr, 'string_req, string_val');
    }
    
    elsif ( $table eq 'EDT_AUX' )
    {
	@values = $edt->get_old_values($table, $action->keyexpr, 'name');
    }
    
    return @values;
}


sub test_validator {
    
    my ($edt, $value, $field, $action) = @_;
    
    if ( defined $value && length($value) == 10 )
    {
	return ('E_FORMAT', "test validator args: $field $action");
    }
    
    else
    {
	return;
    }	
}


sub initialize_transaction {

    my ($edt, $table) = @_;

    if ( $edt->get_attr('initialize exception') )
    {
	die "generated exception";
    }

    elsif ( $edt->get_attr('initialize error') )
    {
	$edt->add_condition('E_TEST', 'initialize');
    }
    
    elsif ( my $value = $edt->get_attr('initialize add') )
    {
	my $quoted = $edt->dbh->quote($value);
	$edt->dbh->do("INSERT INTO $TABLE{EDT_AUX} (name) values ($quoted)");
    }
    
    $edt->{save_init_count}++;
    $edt->{save_init_table} = $table;
    $edt->{save_init_status} = $edt->transaction;
}


sub finalize_transaction {

    my ($edt, $table) = @_;
    
    if ( $edt->get_attr('finalize exception') )
    {
	die "generated exception";
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
	$edt->dbh->do("INSERT INTO $TABLE{EDT_AUX} (name) values ($quoted)");
    }
    
    $edt->{save_final_count}++;
    $edt->{save_final_table} = $table;
    $edt->{save_final_status} = $edt->transaction;
}


sub cleanup_transaction {
    
    my ($edt, $table) = @_;
    
    if ( $edt->get_attr('cleanup exception') )
    {
	die "generated exception";
    }
    
    $edt->{save_cleanup_count}++;
    $edt->{save_cleanup_table} = $table;
    $edt->{save_cleanup_status} = $edt->transaction;
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
    
    if ( my $value = $edt->get_attr('before add') )
    {
	my $quoted = $edt->dbh->quote($value);
	$edt->dbh->do("INSERT INTO $TABLE{EDT_AUX} (name) values ($quoted)");
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
    
    if ( $edt->get_attr('after delete') )
    {
	my $value = $edt->get_attr('before add');
	my $quoted = $edt->dbh->quote($value);
	$edt->dbh->do("DELETE FROM $table WHERE string_req=$quoted");
    }

    if ( $operation eq 'delete' && $table eq 'EDT_TEST' )
    {
	my $keyexpr = $action->keyexpr;
	
	if ( $keyexpr )
	{
	    $edt->dbh->do("DELETE FROM $TABLE{EDT_AUX} WHERE $keyexpr");
	    $edt->{save_delete_aux} = $keyexpr;
	}
    }
    
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


# establish_tables ( class, dbh, options )
# 
# This class method creates database tables necessary to use this class for testing purposes, or
# replaces the existing ones.

sub establish_tables {
    
    my ($class, $dbh, $options) = @_;
    
    $options ||= { };
    
    # Create, or re-create, the table 'edt_test'.
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDT_TEST}");
    
    $dbh->do("CREATE TABLE $TABLE{EDT_TEST} (
		test_no int unsigned primary key auto_increment,
		authorizer_no int unsigned not null,
		enterer_no int unsigned not null,
		modifier_no int unsigned not null default 0,
		interval_no int unsigned not null default 0,
		string_val varchar(40) not null default '',
		string_req varchar(40) not null default '',
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
		created timestamp default current_timestamp,
		modified timestamp default current_timestamp)");
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDT_AUX}");
    
    $dbh->do("CREATE TABLE $TABLE{EDT_AUX} (
		aux_no int unsigned primary key auto_increment,
		name varchar(255) not null default '',
		test_no int unsigned not null default 0,
		unique key (name))");

    $dbh->do("DROP TABLE IF EXISTS $TABLE{EDT_ANY}");
    
    $dbh->do("CREATE TABLE $TABLE{EDT_ANY} (
		any_no int unsigned primary key auto_increment,
		authorizer_no int unsigned not null,
		enterer_no int unsigned not null,
		enterer_id varchar(36) not null,
		string_req varchar(255) not null default '')");
		
}


1;


