#
# Tester.pm: a class for running tests on EditTransaction.pm and its subclasses.
# 



use strict;
use feature 'unicode_strings';
use feature 'fc';

package EditTester;

use Scalar::Util qw(looks_like_number reftype);
use Carp qw(croak);
use Test::More;
use base 'Exporter';

use CoreFunction qw(connectDB configData);
use TableDefs qw(init_table_names select_test_tables $EDT_TEST);

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

    my $dbh;
    
    eval {
	$dbh = connectDB("config.yml");
	init_table_names(configData, 1);
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


# create_tables ( )
# 
# Create or re-create the tables necessary for testing.

sub create_tables {

    my ($T) = @_;
    
    eval {
	select_test_tables('edt_test', 1);
	EditTest->establish_tables($T->dbh);
    };

    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("Could not establish tables. Message was: $msg");
	BAIL_OUT;
    }
}


sub trim_exception {

    my ($msg) = @_;

    return $msg;
}


# new_edt ( perms, options )
#
# Create a new EditTest object. The CREATE allowance is specified by default.

sub new_edt {
    
    my ($T, $perm, $options) = @_;
    
    $options ||= { };
    $options->{CREATE} = 1;

    return $T->get_new_edt($perm, $options);
}


# new_edt_nocreate ( perms, options )
#
# Create a new EditTest object without the CREATE allowance.

sub new_edt_nocreate {
    
    my ($T, $perm, $options) = @_;
    
    return $T->get_new_edt($perm, $options);
}


# _new_edt ( perms, options )
#
# Do the work of creating a new EditTest object.

sub get_new_edt {
    
    my ($T, $perm, $options) = @_;
    
    croak "you must specify a permission" unless ref $perm && $perm->isa('Permissions');
    
    my $allow = { };

    if ( ref $options eq 'HASH' )
    {
	foreach my $k ( keys %$options )
	{
	    if ( $k =~ /^[A-Z_]+$/ )
	    {
		$allow->{$k} = 1;
	    }
	}
    }
    
    $allow->{DEBUG_MODE} if $T->{debug};

    my $edt;

    eval {
	$edt = EditTest->new($T->dbh, $perm, $EDT_TEST, $allow);
    };

    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("EXCEPTION: $msg");
    }

    elsif ( !$edt )
    {
	diag("ERROR: no object was created");
    }
    
    return $edt;
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
    
    croak "you must specify a session id string" unless $session_id && ! ref $session_id;
    
    $table_name ||= $EDT_TEST;

    my $perm;

    eval {
	$perm = Permissions->new($T->dbh, $session_id, $table_name);
    };

    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("EXCEPTION: $msg");
    }

    return $perm;
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
    my $result;

    if ( $edt )
    {
	eval {
	    foreach my $r ( @records )
	    {
		$edt->insert_record($table, $r);
	    }
	    
	    $result = $edt->execute;
	};
	
	if ( $@ )
	{
	    my $msg = trim_exception($@);
	    diag("EXCEPTION: $msg");
	}
    }
    
    $T->{last_edt} = $edt;
    
    ok( $result, $label );
    
    return $result;
}


sub do_update_records {

    my ($T, $perm, $options, $label, $table, @records) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    croak "you must specify at least one record" unless @records && ref $records[0] eq 'HASH';
    
    my $edt = $T->get_new_edt($perm, $options);
    
    $T->{last_edt} = $edt;
    return $T->do_one_operation($edt, $options, 'update_record', $label, $table, @records);
}


sub do_replace_records {

    my ($T, $perm, $options, $label, $table, @records) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    croak "you must specify at least one record" unless @records && ref $records[0] eq 'HASH';
    
    my $edt = $T->get_new_edt($perm, $options);
    my $result;

    if ( $edt )
    {
	eval {
	    foreach my $r ( @records )
	    {
		$edt->replace_record($table, $r);
	    }
	    
	    $result = $edt->execute;
	};
	
	if ( $@ )
	{
	    my $msg = trim_exception($@);
	    diag("EXCEPTION: $msg");
	}
    }
    
    $T->{last_edt} = $edt;
    
    ok( $result, $label );
    
    return $result;
}


sub do_delete_records {

    my ($T, $perm, $options, $label, $table, @records) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    croak "you must specify at least one record" unless @records && ref $records[0] eq 'HASH';
    
    my $edt = $T->get_new_edt($perm, $options);
    my $result;

    if ( $edt )
    {
	eval {
	    foreach my $r ( @records )
	    {
		$edt->delete_record($table, $r);
	    }
	    
	    $result = $edt->execute;
	};
	
	if ( $@ )
	{
	    my $msg = trim_exception($@);
	    diag("EXCEPTION: $msg");
	}
    }
    
    $T->{last_edt} = $edt;
    
    ok( $result, $label );
    
    return $result;
}


sub do_one_operation {

    my ($T, $edt, $options, $operation, $label, $table, @records) = @_;
    
    unless ( $edt )
    {
	fail($label);
	return;
    }
    
    my $result;
    
    eval {
	foreach my $r ( @records )
	{
	    $edt->update_record($table, $r);
	}
	
	$result = $edt->execute;
    };
    
    if ( $@ )
    {
	my $msg = trim_exception($@);
	diag("EXCEPTION: $msg");
	fail($label);
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


sub diag_errors {

    my ($T, $edt) = @_;

    $edt //= $T->{last_edt};
    return unless ref $edt;
    
    foreach my $e ( $edt->errors )
    {
        my $msg = $edt->generate_msg($e);
        diag("ERROR: $msg");
    }
}


sub diag_warnings {

    my ($T, $edt) = @_;
    
    $edt //= $T->{last_edt};
    return unless ref $edt;
    
    foreach my $e ( $edt->warnings )
    {
        my $msg = $edt->generate_msg($e);
        diag("WARNING: $msg");
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




1;

