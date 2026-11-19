# 
# The Paleobiology Database
# 
#   PBDBEdit.pm - a class for handling updates to arbitrary Paleobiology Database records,
#   including records that don't require a more specific subclass.
#   
#   This class is a subclass of EditTransaction, and includes the necessary
#   modules for editing PaleoBioDB tables.
#   
#   To use it, first call $edt = PBDBEdit->new with appropriate arguments (see
#   EditTransaction.pm). Then you can call $edt->insert_record, etc.
#   
#   [Changed by Claude under the guidance of Michael McClennen: the identical class
#   PbdbEdit was merged into this one, and the module now returns a true value]


use strict;

package PBDBEdit;

use parent 'EditTransaction';

use Role::Tiny::With;

with 'EditTransaction::Mod::MariaDB';
with 'EditTransaction::Mod::PaleoBioDB';

use namespace::clean;

1;
