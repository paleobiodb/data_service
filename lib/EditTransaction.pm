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
		conditions => [ ],
		result => '',
		state => '' };
    
    if ( $options && ref $options eq 'HASH' )
    {
	$edt->{session_id} = $options->{session_id} if $options->{session_id};
	$edt->{auth_info} = $options->{auth_info} if $options->{auth_info};
	$edt->{debug} = 1 if $options->{debug};
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
	
	($authorizer_no, $enterer_no, $is_super) = $dbh->selectrow_array($sql);
    }
    
    if ( $authorizer_no && $enterer_no )
    {
	$edt->{authorizer_no} = $authorizer_no;
	$edt->{enterer_no} = $enterer_no;
	$edt->{is_super} = 1 if $is_super;
	
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
    
    $dbh->do("COMMIT");
    $edt->{state} = 'committed';
    
    return $edt;
}


sub rollback {

    my ($edt) = @_;
    
    my $dbh = $edt->{dbh};
    
    $dbh->do("ROLLBACK");
    $dbh->{state} = 'aborted';
    
    return $edt;
}


sub add_condition {

    my ($edt, $message) = @_;
    
    push @{$edt->{conditions}}, $message;
    
    if ( $message =~ /^[EC]_/ )
    {
	$edt->{errors_occurred} = 1;
	$edt->{state} = 'error';
    }
    
    return $edt;
}


sub conditions {

    my ($edt) = @_;
    
    return @{$edt->{conditions}};
}


sub clear_conditions {

    my ($edt) = @_;
    
    @{$edt->{conditions}} = ();
    return $edt;
}


sub can_proceed {

    my ($edt) = @_;
    
    return $edt->{state} && ($edt->{state} eq 'active' || $edt->{state} eq 'error') ? 1 : 0;
}


sub can_edit {
    
    my ($edt) = @_;
    
    return $edt->{state} && $edt->{state} eq 'active' ? 1 : 0;
}


sub check_only {

    my ($edt) = @_;
    
    $edt->{state} = 'error';
}

# sub status {
    
#     my ($edt) = shift;
    
#     if ( @_ )
#     {
# 	$edt->{status} = (shift // '');
#     }
    
#     return $edt->{status};
# }


sub errors_occurred {

    my ($edt) = @_;
    
    return $edt->{errors_occurred};
}


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


1;
