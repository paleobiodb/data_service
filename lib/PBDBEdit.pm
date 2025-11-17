# 
# The Paleobiology Database
# 
#   PBDBEdit.pm - a class for handling updates to arbitrary Paleobiology Database records.
#   
#   This class is a subclass of EditTransaction, and includes the necessary
#   modules for editing PaleoBioDB tables.
#   
#   To use it, first call $edt = PBDBEdit->new with appropriate arguments (see
#   EditTransaction.pm). Then you can call $edt->insert_record, etc.


use strict;

package PBDBEdit;

use parent 'EditTransaction';

use Role::Tiny::With;

with 'EditTransaction::Mod::MariaDB';
with 'EditTransaction::Mod::PaleoBioDB';

use namespace::clean;

