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


# sub initialize {
    
#     my ($class, $ds) = @_;
    
#     return if defined $UPDATE_MATCHED;
    
#     my $dbh = $ds->get_connection;
    
#     $dbh->do("CREATE TEMPORARY TABLE a (b int unsigned not null)");
#     $dbh->do("INSERT INTO a (b) values (1)");
    
#     my $result = $dbh->do("UPDATE a SET b = 1 WHERE b = 1");
    
#     if ( $result )
#     {
# 	print STDERR "UPDATE statement returns number of rows MATCHED.\n" if $ds->debug;
#     }
    
#     else
#     {
# 	print STDERR "UPDATE statement returns number of rows CHANGED.\n" if $ds->debug; 
#     }
    
#     $UPDATE_MATCHED = $result || 0;
# }


sub new {
    
    my ($class, $dbh, $options) = @_;
    
    my $edt = { dbh => $dbh, 
		conditions => [ ],
		result => '',
		state => 'active' };
    
    $edt->{debug} = 1 if $options && $options->{debug};
    
    $dbh->do("START TRANSACTION");
    
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

sub record_keys {
    
    my ($edt, @keys) = @_;
    
    if ( @keys )
    {
	$edt->{keys} = \@keys;
    }
    
    return ref $edt->{keys} eq 'ARRAY' ? @{$edt->{keys}} : ();
}


sub dbh {
    
    my ($edt) = @_;
    
    return $edt->{dbh};
}


sub debug {

    my ($edt) = @_;
    
    return $edt->{debug};
}


1;
