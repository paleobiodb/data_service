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

no warnings 'experimental';


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
	my $result = $edt->dbh->do(@_);
	
	$edt->debug_line("This statement affected $result rows\n\n")
	    if $edt->debug_mode && defined $result && $result ne '';

	return $result;
    }
    
    catch ($e) {
	croak($e);
    }
}


sub do_logged_stmt {

    my ($edt, $op, $table_specifier, $sql, $keyexpr, $rev) = @_;

    $edt->debug_line("$sql\n") if $edt->debug_mode;
    
    try {
	my $result = $edt->dbh->do($sql);
	
	$edt->debug_line("This statement affected $result rows\n\n")
	    if $edt->debug_mode && defined $result && $result ne '';
	
	$edt->log_aux_event($op, $table_specifier, $sql, $keyexpr, $rev);
	
	return $result;
    }
    
    catch ($e) {
	croak($e);
    }
}

1;


