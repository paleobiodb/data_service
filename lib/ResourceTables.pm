# 
# The Paleobiology Database
# 
#   ResourceTables.pm
# 
# Routines for manipulating the educational resource tables, plus table property definitions.
# 

package ResourceTables;

use strict;

use TableDefs qw($RESOURCE_ACTIVE $RESOURCE_QUEUE $RESOURCE_IMAGES $RESOURCE_TAGS
		 set_table_property set_column_property);


# At runtime, set table properties for our main table. All of the other tables used by the resource modules
# are subordinate, and do not have their own properties.

{
    set_table_property($RESOURCE_QUEUE, ALLOW_POST => 'LOGGED_IN');
    set_table_property($RESOURCE_QUEUE, ALLOW_VIEW => 'LOGGED_IN');
    set_table_property($RESOURCE_QUEUE, ALLOW_DELETE => 1);
    set_table_property($RESOURCE_QUEUE, AUTH_FIELDS => "authorizer_no, enterer_no, enterer_id");
    set_table_property($RESOURCE_QUEUE, PRIMARY_KEY => "eduresource_no");
    set_table_property($RESOURCE_QUEUE, PRIMARY_ATTR => "eduresource_id");
    
    set_column_property($RESOURCE_QUEUE, 'eduresource_no', ID_TYPE => 'EDR');
    set_column_property($RESOURCE_QUEUE, 'title', REQUIRED => 1);
    set_column_property($RESOURCE_QUEUE, 'status', ADMIN_SET => 1);
    
    set_table_property($RESOURCE_ACTIVE, PRIMARY_KEY => 'id');
}


