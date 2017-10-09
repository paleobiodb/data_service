# 
# EditAction.pm
# 
# This class encapsulates a single action to be executed on the database, generally a single SQL
# statement or a set of related SQL statements executed sequentially. Under most circumstances,
# each record submitted to a data service operation will generate a single action. Sometimes,
# auxiliary actions will be included to update linking tables and such.
# 
# This class is meant to be used internally by EditTransaction and its subclasses.
# 


package EditAction;

use strict;

