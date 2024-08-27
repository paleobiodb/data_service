# 
# EditTransaction project
# 
#   ETTrivialClass.pm - a class for use in testing EditTransaction.pm
#   
#   This class is a subclass of EditTransaction, and is used by the unit tests
#   for EditTransaction and its related classes.


package ETTrivialClass;

use parent 'EditTransaction';

use Role::Tiny::With;

with 'EditTransaction::Mod::MariaDB';

1;
