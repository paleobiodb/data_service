# 
# The Paleobiology Database
# 
#   PbdbEdit.pm - a class for handling updates to database records that don't
#   require a more specific subclass.
#   
#   This class is a subclass of EditTransaction, and encapsulates the logic for adding, updating,
#   and deleting educational resource records.
#   
#   Each instance of this class is responsible for initiating a database
#   transaction, checking a set of records for insertion, update, or deletion,
#   and either committing or rolling back the transaction depending on the
#   results of the checks. If the object is destroyed, the transaction will be
#   rolled back. This is all handled by code in EditTransaction.pm.
#   
#   To use it, first call $edt = PbdbEdit->new() with appropriate arguments (see
#   EditTransaction.pm). Then you can call PbdbEdit->process_record, etc. 


package PbdbEdit;

use strict;

use parent 'EditTransaction';

use Role::Tiny::With;

with 'EditTransaction::Mod::MariaDB';
with 'EditTransaction::Mod::PaleoBioDB';

use namespace::clean;

1;
