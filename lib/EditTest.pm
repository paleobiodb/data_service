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

use TableDefs qw(set_table_property set_column_property $EDT_TEST);
use ResourceTables;

use base 'EditTransaction';

use namespace::clean;

our (%CONDITION_TEMPLATE) = (E_TEST => "TEST ERROR '%1'",
			     W_TEST => "TEST WARNING '%1'");


# At runtime, set column properties for our test table
# ----------------------------------------------------

{
    set_table_property($EDT_TEST, ALLOW_POST => 'AUTHORIZED');
    set_table_property($EDT_TEST, ALLOW_DELETE => 1);
    set_table_property($EDT_TEST, PRIMARY_KEY => 'test_no');
    set_column_property($EDT_TEST, 'string_req', REQUIRED => 1);
    set_column_property($EDT_TEST, 'signed_req', REQUIRED => 1);

    EditTest->register_allowances('TEST_DEBUG');
}


# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# get_condition_template ( code, table, selector )
#
# Return the proper template for error and warning conditions defined by this subclass. If no
# matching template can be found, we call the parent method. Other subclasses of EditTransaction
# should do something similar.

sub get_condition_template {
    
    my ($edt, $code, $table, $selector) = @_;
    
    if ( $CONDITION_TEMPLATE{$code} )
    {
	return $CONDITION_TEMPLATE{$code};
    }

    else
    {
	return $edt->SUPER::get_condition_template($code, $table, $selector);
    }
}


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
	    $edt->add_condition($action, 'E_TEST', 'xyzzy');
	}

	elsif ( $record->{string_req} eq 'authorize warning' )
	{
	    $edt->add_condition($action, 'W_TEST', 'xyzzy');
	}

	elsif ( $record->{string_req} eq 'authorize save' )
	{
	    $edt->{save_authorize_action} = $action;
	    $edt->{save_authorize_operation} = $operation;
	    $edt->{save_authorize_table} = $table;
	    $edt->{save_authorize_keyexpr} = $keyexpr;
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
	    $edt->add_condition($action, 'E_TEST', 'xyzzy');
	}

	elsif ( $record->{string_req} eq 'validate warning' )
	{
	    $edt->add_condition($action, 'W_TEST', 'xyzzy');
	}

	elsif ( $record->{string_req} eq 'validate save' )
	{
	    $edt->{save_validate_action} = $action;
	    $edt->{save_validate_operation} = $operation;
	    $edt->{save_validate_table} = $table;
	    $edt->{save_validate_keyexpr} = $keyexpr;
	    $edt->{save_validate_errors} = $action->has_errors;
	}
    }
    
    return EditTransaction::validate_action(@_);
}


sub initialize_transaction {

    my ($edt, $table) = @_;

    if ( $edt->get_attr('initialize exception') )
    {
	die "generated exception";
    }

    elsif ( $edt->get_attr('initialize error') )
    {
	$edt->add_condition(undef, 'E_TEST', 'initialize');
    }
    
    elsif ( my $value = $edt->get_attr('initialize add') )
    {
	my $quoted = $edt->dbh->quote($value);
	$edt->dbh->do("INSERT INTO $table (string_req) values ($quoted)");
    }

    $edt->{save_init_table} = $table;
    $edt->{save_init_count}++;
}


sub finalize_transaction {

    my ($edt, $table) = @_;
    
    if ( $edt->get_attr('finalize exception') )
    {
	die "generated exception";
    }
    
    elsif ( $edt->get_attr('finalize error' ) )
    {
	$edt->add_condition(undef, 'E_TEST', 'finalize');
    }
    
    elsif ( $edt->get_attr('finalize warning') )
    {
	$edt->add_condition(undef, 'W_TEST', 'finalize');
    }
    
    elsif ( my $value = $edt->get_attr('finalize add') )
    {
	my $quoted = $edt->dbh->quote($value);
	$edt->dbh->do("INSERT INTO $table (string_req) values ($quoted)");
    }
    
    $edt->{save_final_count}++;
    $edt->{save_final_table} = $table;
}


sub cleanup_transaction {
    
    my ($edt, $table) = @_;
    
    if ( $edt->get_attr('cleanup exception') )
    {
	die "generated exception";
    }
    
    $edt->{save_cleanup_count}++;
    $edt->{save_cleanup_table} = $table;
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
	    $edt->add_condition($action, 'E_TEST', 'before');
	}
	
	elsif ( $record->{string_req} eq 'before warning' )
	{
	    $edt->add_condition($action, 'W_TEST', 'before');
	}
    }
    
    if ( my $value = $edt->get_attr('before add') )
    {
	my $quoted = $edt->dbh->quote($value);
	$edt->dbh->do("INSERT INTO $table (string_req) values ($quoted)");
    }
    
    $edt->{save_before_count}++;
    $edt->{save_before_action} = $action;
    $edt->{save_before_operation} = $operation;
    $edt->{save_before_table} = $table;
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
	    $edt->add_condition($action, 'E_TEST', 'after');
	}

	elsif ( $record->{string_req} eq 'after warning' )
	{
	    $edt->add_condition($action, 'W_TEST', 'after');
	}
    }
    
    if ( $edt->get_attr('after delete') )
    {
	my $value = $edt->get_attr('before add');
	my $quoted = $edt->dbh->quote($value);
	$edt->dbh->do("DELETE FROM $table WHERE string_req=$quoted");
    }
    
    $edt->{save_after_count}++;
    $edt->{save_after_action} = $action;
    $edt->{save_after_operation} = $operation;
    $edt->{save_after_table} = $table;
    $edt->{save_after_result} = $keyval;
}


sub cleanup_action {
    
    my ($edt, $action, $operation, $table) = @_;
    
    if ( $edt->get_attr('cleanup action exception') )
    {
	$edt->add_condition(undef, 'W_TEST', 'cleanup exception');
	die "generated exception";
    }
    
    $edt->{save_cleanup_action_count}++;
    $edt->{save_cleanup_action_action} = $action;
    $edt->{save_cleanup_action_operation} = $operation;
    $edt->{save_cleanup_action_table} = $table;
}


# debug_line ( text )
#
# Capture debug output for testing.

sub debug_line {
    
    return unless ref $_[0] && defined $_[1] && $_[0]->{debug};
    return $_[0]->SUPER::debug_line($_[1]) unless $_[0]->{allows}{TEST_DEBUG};
    
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
    
    $dbh->do("DROP TABLE IF EXISTS $EDT_TEST");
    
    $dbh->do("CREATE TABLE $EDT_TEST (
		test_no int unsigned primary key auto_increment,
		authorizer_no int unsigned not null,
		enterer_no int unsigned not null,
		modifier_no int unsigned not null,
		interval_no int unsigned not null,
		string_val varchar(40) not null,
		string_req varchar(40) not null,
		signed_val mediumint,
		unsigned_val mediumint unsigned,
		decimal_val decimal(5,2),
		float_val double,
		boolean_val boolean,
		created timestamp default current_timestamp,
		modified timestamp default current_timestamp)");

}


1;


