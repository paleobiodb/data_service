# 
# The Paleobiology Database
# 
#   ReferenceEdit.pm - a class for handling updates to bibliographic reference records
#   
#   This class is a subclass of EditTransaction, and encapsulates the logic for adding, updating,
#   and deleting educational resource records.
#   
#   Each instance of this class is responsible for initiating a database transaction, checking a
#   set of records for insertion, update, or deletion, and either committing or rolling back the
#   transaction depending on the results of the checks. If the object is destroyed, the transaction will
#   be rolled back. This is all handled by code in EditTransaction.pm.
#   
#   To use it, first call $edt = ReferenceEdit->new() with appropriate arguments (see
#   EditTransaction.pm). Then you can call $edt->, etc.


package ReferenceEdit;

use base 'EditTransaction';

use strict;

use Carp qw(carp croak);
use Try::Tiny;

use TableDefs qw(%TABLE);
use CoreTableDefs;

use namespace::clean;

our (@CARP_NOT) = qw(EditTransaction);


{
    ArchiveEdit->register_allowances('SKIP_MATCHES');
}

# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# validate_action ( action, operation, table )
# 
# For insertions, check to see if the record to be inserted matches any existing record. If so,
# add a caution to this record.

sub validate_action {
    
    my ($edt, $action, $operation, $table) = @_;
    
    if ( $action eq 'insert' )
    {
	my $record = $action->record;
	
	if ( my $match_id = match_refs($record) )
	{
	    $edt->add_condition('E_MATCH');
	}
    }
}


sub match_refs {




}

