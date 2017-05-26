# 
# The Paleobiology Database
# 
#   CommonEdit.pm
# 

package CommonEdit;

use strict;

use Carp qw(carp croak);
use Try::Tiny;

use ConsoleLog qw(logMessage);

use base 'Exporter';

our(@EXPORT_OK) = qw(start_transaction commit_transaction rollback_transaction);




sub start_transaction {

    my ($dbh) = @_;
    
    $dbh->do("START TRANSACTION");
}


sub commit_transaction {

    my ($dbh) = @_;
    
    $dbh->do("COMMIT");
}


sub rollback_transaction {

    my ($dbh) = @_;
    
    $dbh->do("ROLLBACK");
}



package EditResult;

sub new {

    return bless { result => '' };
}


sub add_condition {

    my ($result, $message) = @_;
    
    push @{$result->{conditions}}, $message;
    
    if ( $message =~ /^[EC]_/ )
    {
	$result->{status} = 'ERROR';
    }
    
    return $result;
}


sub conditions {

    my ($result) = @_;
    
    return @{$result->{conditions}} if ref $result->{conditions} eq 'ARRAY';
    return;
}


sub status {
    
    my ($result) = shift;
    
    if ( @_ )
    {
	$result->{status} = (shift // '');
    }
    
    return $result->{status};
}


sub record_keys {
    
    my ($result, @keys) = @_;
    
    if ( @keys )
    {
	$result->{keys} = \@keys;
    }
    
    return @{$result->{keys}};
}


1;
