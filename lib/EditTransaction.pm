# 
# The Paleobiology Database
# 
#   EditOperation.pm - base class for database editing
# 

package EditTransaction;

use strict;

use TableDefs qw(get_table_property);

use Carp qw(carp croak);
use Try::Tiny;


# This class is intended to encapsulate the lowest-level machinery necessary for updating records
# in the database. It handles transaction initiation, commitment, and rollback, and also error and
# warning conditions. It does not contain any routines for adding or updating records.
#
# This class can be subclassed (see TimescaleEdit.pm) in order to provide additional logic for
# checking values and adding and updating records.


# new ( dbh, options )
#
# Create a new EditTransaction object. 

sub new {
    
    my ($class, $dbh, $options) = @_;
    
    my $edt = { dbh => $dbh,
		condition => { },
		error => { },
		warning => { },
		result => '',
		state => '' };
    
    bless $edt, $class;
    
    if ( $options && ref $options eq 'HASH' )
    {
	$edt->{session_id} = $options->{session_id} if $options->{session_id};
	$edt->{auth_info} = $options->{auth_info} if $options->{auth_info};
	$edt->{debug} = 1 if $options->{debug};
	$edt->{condition} = $options->{conditions};
    }
    
    my ($authorizer_no, $enterer_no, $guest_no, $is_super, $is_fixup);
    
    if ( $edt->{auth_info} )
    {
	if ( $edt->{auth_info}{guest_no} && $edt->{auth_info}{role} eq 'guest' )
	{
	    $guest_no = $edt->{auth_info}{guest_no};
	}
	
	else
	{
	    $authorizer_no = $edt->{auth_info}{authorizer_no};
	    $enterer_no = $edt->{auth_info}{enterer_no};
	    $is_fixup = $edt->{auth_info}{fixup};
	}
    }
    
    elsif ( $edt->{session_id} )
    {
	my $quoted_id = $dbh->quote($edt->{session_id});
	
	my $sql = "
		SELECT authorizer_no, enterer_no, superuser FROM session_data
		WHERE session_id = $quoted_id";
	
	print STDERR "$sql\n\n" if $edt->debug;
	
	($authorizer_no, $enterer_no, $is_super) = $dbh->selectrow_array($sql);
    }
    
    if ( $authorizer_no && $enterer_no )
    {
	$edt->{authorizer_no} = $dbh->quote($authorizer_no);
	$edt->{enterer_no} = $dbh->quote($enterer_no);
	$edt->{is_super} = 1 if $is_super;
	
	print STDERR " >>> START TRANSACTION\n\n" if $options->{debug};
	
	$dbh->do("START TRANSACTION");
	
	$edt->{state} = 'active';
    }

    elsif ( $guest_no )
    {
	$edt->{guest_no} = $dbh->quote($guest_no);

	print STDERR " >>> START TRANSACTION (guest)\n\n" if $options->{debug};
	
	$dbh->do("START TRANSACTION");
	
	$edt->{state} = 'active';
    }

    elsif ( $is_fixup )
    {
	$edt->{is_super} = 1;
	$edt->{is_fixup} = 1;
	
	print STDERR " >>> START TRANSACTION (fixup)\n\n" if $options->{debug};
	
	$dbh->do("START TRANSACTION");
	
	$edt->{state} = 'active';
    }
    
    else
    {
	$edt->add_condition("E_SESSION", undef, "bad session identifier");
	$edt->{state} = 'blocked';
    }
    
    return $edt;
}


sub DESTROY {
    
    my ($edt) = @_;
    
    return if $edt->{state} eq 'committed' || $edt->{state} eq 'aborted';
    
    my $dbh = $edt->{dbh};
    
    print STDERR " <<< ROLLBACK TRANSACTION\n\n" if $edt->debug;
	
    $dbh->do("ROLLBACK");
}


sub commit {

    my ($edt) = @_;
    
    my $dbh = $edt->{dbh};
    
    print STDERR " <<< COMMIT TRANSACTION\n\n" if $edt->debug;
	
    $dbh->do("COMMIT");
    $edt->{state} = 'committed';
    
    return $edt;
}


sub rollback {

    my ($edt) = @_;
    
    my $dbh = $edt->{dbh};
    
    print STDERR " <<< ROLLBACK TRANSACTION\n\n" if $edt->debug;
	
    $dbh->do("ROLLBACK");
    $dbh->{state} = 'aborted';
    
    return $edt;
}


sub new_record {
    
    my ($edt) = @_;
    
    delete $edt->{this_record_errors};
}


sub add_condition {
    
    my ($edt, $code, $record_label, $data) = @_;
    
    # if ( $code eq 'E_NOT_FOUND' && $edt->{condition}{PROCEED} )
    # {
    # 	$edt->{warning}{W_NOT_FOUND} ||= [ ];
    # 	push @{$edt->{warning}{W_NOT_FOUND}}, [$record_label, $data];
    # }
    
    # $$$ where do we put the logic to convert errors into warnings with 'PROCEED'?
    
    if ( $code =~ qr{ ^ [EC] _ }xsi )
    {
        $edt->{error}{$code} ||= [ ];
	push @{$edt->{error}{$code}}, [$record_label, $data];
	
	$edt->{this_record_errors}++;
	$edt->{state} = 'error' if $edt->{state} eq 'active';
    }
    
    else
    {
	$edt->{warning}{$code} ||= [ ];
	push @{$edt->{warning}{$code}}, [$record_label, $data];
    }
    
    return $edt;
}


sub errors {

    my ($edt) = @_;
    
    return %{$edt->{error}};
}


sub warnings {
    
    my ($edt) = @_;
    
    return %{$edt->{warning}};
}


sub can_check {

    my ($edt) = @_;
    
    if ( $edt->{state} )
    {
	return 1 if $edt->{state} eq 'active' || $edt->{state} eq 'error';
    }
    
    return 0;
}


sub can_edit {
    
    my ($edt) = @_;
    
    if ( $edt->{state} )
    {
	return 1 if $edt->{state} eq 'active';
	return 1 if $edt->{state} eq 'error' && $edt->{condition}{PROCEED} && ! $edt->{this_record_errors};
    }
    
    return 0;
}


sub check_only {

    my ($edt) = @_;
    
    $edt->{state} = 'error' if $edt->{state} eq 'active';
    delete $edt->{condition}{PROCEED};
}


sub dbh {
    
    my ($edt) = @_;
    
    return $edt->{dbh};
}


sub debug {

    my ($edt) = @_;
    
    return $edt->{debug};
}


sub generate_set_list {
    
    my ($edt, $fields, $values) = @_;
    
    return unless ref $fields eq 'ARRAY' && @$fields;
    
    my @set_list;
    
    foreach my $i ( 0..$#$fields )
    {
	my $value = defined $values->[$i] && $values->[$i] ne '' ? $values->[$i] : 'NULL';
	push @set_list, "$fields->[$i]=$values->[$i]";
    }
    
    return join(', ', @set_list);
}




1;
