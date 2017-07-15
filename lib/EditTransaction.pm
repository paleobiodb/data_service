# 
# The Paleobiology Database
# 
#   EditOperation.pm - base class for database editing
# 

package EditTransaction;

use strict;

use Carp qw(carp croak);
use Try::Tiny;

use ConsoleLog qw(logMessage);



our ($UPDATE_MATCHED);


sub new {
    
    my ($class, $dbh, $options) = @_;
    
    my $edt = { dbh => $dbh,
		condition => { },
		error => { },
		warning => { },
		result => '',
		state => '' };
    
    if ( $options && ref $options eq 'HASH' )
    {
	$edt->{session_id} = $options->{session_id} if $options->{session_id};
	$edt->{auth_info} = $options->{auth_info} if $options->{auth_info};
	$edt->{debug} = 1 if $options->{debug};
	$edt->{condition} = $options->{conditions};
    }
    
    my ($authorizer_no, $enterer_no, $is_super);
    
    if ( $edt->{auth_info} )
    {
	$authorizer_no = $edt->{auth_info}{authorizer_no};
	$enterer_no = $edt->{auth_info}{enterer_no};
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
    
    else
    {
	$edt->add_condition("E_SESSION: bad session identifier");
	$edt->{state} = 'blocked';
    }
    
    return bless $edt, $class;
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
	$edt->{state} = 'error';
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


sub allow_proceed {

    my ($edt) = @_;
    
    $edt->{allow_proceed} = 1;
}


# sub add_condition {

#     my ($edt, $code, $record_label, $data) = @_;
    
#     $edt->{condition}{$code} ||= [ ];
    
#     push @{$edt->{condition}{"C_$code"}}, [$record_label, $data];
    
#     $edt->{errors_occurred} = 1;
#     $edt->{state} = 'error';
    
#     return $edt;
# }


# sub conditions {

#     my ($edt) = @_;
    
#     return %{$edt->{condition}};
# }


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
    
    $edt->{state} = 'error';
    delete $edt->{condition}{PROCEED};
}

# sub status {
    
#     my ($edt) = shift;
    
#     if ( @_ )
#     {
# 	$edt->{status} = (shift // '');
#     }
    
#     return $edt->{status};
# }


sub dbh {
    
    my ($edt) = @_;
    
    return $edt->{dbh};
}


sub authorizer_no {
    
    my ($edt) = @_;
    
    return $edt->{authorizer_no};
}


sub enterer_no {
    
    my ($edt) = @_;
    
    return $edt->{enterer_no};
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
