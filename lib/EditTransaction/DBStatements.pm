# 
# EditTransaction::Execution
# 
# This role provides methods for executing common DBI methods directly, while emitting
# debugging messages if debug mode is on.
# 


package EditTransaction::DBStatements;

use strict;

use Carp qw(croak);

use Role::Tiny;

use feature 'try';


sub selectrow_array {

    my ($edt, $sql, $attrs, @bind_values) = @_;

    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    if ( @bind_values && $edt->debug_mode )
    {
	my $line = join("', '", @bind_values);
	$edt->debug_line("  ['$line']\n\n");
    }
    
    shift @_;
    
    try {
	$edt->dbh->selectrow_array(@_);
    }

    catch ($e) {
	croak($e);
    }
}


sub do_stmt {

    my ($edt, $sql, $attrs, @bind_values) = @_;
    
    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    if ( @bind_values && $edt->debug_mode )
    {
	my $line = join("', '", @bind_values);
	$edt->debug_line("  ['$line']\n\n");
    }
    
    shift @_;
    
    try {
	$edt->dbh->do(@_);
    }
    
    catch ($e) {
	croak($e);
    }
}

1;


