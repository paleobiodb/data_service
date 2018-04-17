# 
# The Paleobiology Database
# 
#   SpecimenEdit.pm - a class for handling updates to fossil specimen records
#   
#   This class is a subclass of EditTransaction, and encapsulates the logic for adding, updating,
#   and deleting fossil specimen records.
#   
#   Each instance of this class is responsible for initiating a database transaction, checking a
#   set of records for insertion, update, or deletion, and either committing or rolling back the
#   transaction depending on the results of the checks. If the object is destroyed, the transaction will
#   be rolled back. This is all handled by code in EditTransaction.pm.
#   
#   To use it, first call $edt = SpecimenEdit->new() with appropriate arguments (see
#   EditTransaction.pm). Then you can call $edt->insert_record(), etc.


package SpecimenEdit;

use strict;

use Carp qw(carp croak);

use TableDefs qw(%TABLE is_test_mode);
use SpecimenDefs;

use base 'EditTransaction';

use namespace::clean;



# {
    # SpecimenEdit->register_conditions(
    # 	E_PERM => { status => "You do not have permission to change the status of this record" },
    # 	W_TAG_NOT_FOUND => "Unrecognized resource tag '%1'",
    # 	W_PERM => { status => "The status of this record has been set to 'pending'" });
# }


# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# validate_action ( table, operation, action )
# 
# This method is called from EditTransaction.pm to validate each insert and update action.
# We override it to do additional checks before calling validate_against_schema.

# sub validate_action {
    
#     my ($edt, $action, $operation, $table, $keyexpr) = @_;
    
#     my $record = $action->record;
#     my $permission = $action->permission;
    
#     $edt->validate_against_schema($action, $operation, $table);
# }


# after_action ( action, operation, table, result )
#
# This method is called from EditTransaction.pm after each action, whether or not it is
# successful.  We override it to handle adding and removing records from the active resource table
# as appropriate.

# sub after_action {
    
#     my ($edt, $action, $operation, $table, $result) = @_;
    
# }



1;
