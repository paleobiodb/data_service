# 
# Datalog.pm
# 
# This module provides a logging facility for the EditTransaction class. Every action in a
# committed transaction is logged to a file on disk. For every action, several lines are written
# to the log file indicating the date and time at which the action took place, what type of action
# it is, who performed it, the SQL necessary to repeat it, and the SQL necessary to undo it.
# 
# This class is meant to be used internally by EditTransaction and its subclasses.
# 


package EditTransaction::Datalog;

use strict;

use TableDefs qw(get_table_property);

use EditTransaction;
use Carp qw(carp croak);

use namespace::clean;








package EditTransaction::LogEntry;

use strict;

use namespace::clean;


sub new {
    
    my ($class, $keyval, $action) = @_;
    
    my $entry = { keyval => $keyval,
		  action => $action };

    bless $entry, $class;
    return $entry;
}


1;
