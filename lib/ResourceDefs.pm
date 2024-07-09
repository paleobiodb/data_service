# 
# The Paleobiology Database
# 
#   ResourceDefs.pm
# 
# Definitions and properties necessary for working with the Educational Resource tables.
# 

package ResourceDefs;

use strict;

use Carp qw(croak);

use TableDefs qw(set_table_name set_table_group set_table_property set_column_property);

# our (@TABLES) = qw($RESOURCE_QUEUE $RESOURCE_IMAGES
# 		   $RESOURCE_TAG_NAMES $RESOURCE_TAGS $RESOURCE_ACTIVE);

# our (@EXPORT_OK) = qw($RESOURCE_QUEUE $RESOURCE_IMAGES
# 		   $RESOURCE_TAG_NAMES $RESOURCE_TAGS $RESOURCE_ACTIVE);


# use base 'Exporter';


# The following variables hold the name of each of the tables that are used to handle Educational
# Resources. Their values may change, if we switch over to the test database.

# our $RESOURCE_QUEUE = 'eduresource_queue';
# our $RESOURCE_IMAGES = 'eduresource_images';
# our $RESOURCE_ACTIVE = 'eduresources';
# our $RESOURCE_TAG_NAMES = 'edutags';
# our $RESOURCE_TAGS = 'eduresource_tags';


# At runtime, set table and column properties.

{
    set_table_name(RESOURCE_QUEUE => 'eduresource_queue');
    set_table_name(RESOURCE_IMAGES => 'eduresource_images');
    set_table_name(RESOURCE_ACTIVE => 'eduresource_active' );
    set_table_name(RESOURCE_IMAGES_ACTIVE => 'eduresource_images_active');
    set_table_name(RESOURCE_TAG_NAMES => 'edutags' );
    set_table_name(RESOURCE_TAGS => 'eduresource_tags' );
    
    set_table_group('eduresources' => 'RESOURCE_QUEUE', 'RESOURCE_IMAGES', 'RESOURCE_ACTIVE',
				      'RESOURCE_IMAGES_ACTIVE', 'RESOURCE_TAG_NAMES', 'RESOURCE_TAGS');
    
    set_table_property(RESOURCE_QUEUE => CAN_POST => 'LOGGED_IN');
    # set_table_property(RESOURCE_QUEUE => CAN_VIEW => 'LOGGED_IN');
    set_table_property(RESOURCE_QUEUE => PRIMARY_KEY => "eduresource_no");
    set_table_property(RESOURCE_QUEUE => PRIMARY_FIELD => "eduresource_id");
    set_table_property(RESOURCE_QUEUE => AUTH_FIELDS => "authorizer_no, enterer_no");
    
    set_column_property(RESOURCE_QUEUE => 'eduresource_no', EXTID_TYPE => 'EDR');
    set_column_property(RESOURCE_QUEUE => 'title', REQUIRED => 1);
    # set_column_property(RESOURCE_QUEUE => 'status', ADMIN_SET => 1);
    
    set_table_property(RESOURCE_ACTIVE => PRIMARY_KEY => 'eduresource_no');
    set_table_property(RESOURCE_ACTIVE => PRIMARY_FIELD => 'eduresource_id');
}


# enable_test_mode ( class, table, ds )
# 
# Change the global variables that hold the names of the eduresource tables over to the test
# database. If $ds is either 1 or a reference to a Web::DataService object with the debug flag
# set, then print out a debugging message.

# sub enable_test_mode {
    
#     my ($class, $table, $ds) = @_;
    
#     croak "You must define 'test_db' in the configuration file" unless $TEST_DB;
    
#     change_table_db('RESOURCE_QUEUE', $TEST_DB);
#     change_table_db('RESOURCE_IMAGES', $TEST_DB);
#     change_table_db('RESOURCE_TAG_NAMES', $TEST_DB);
#     change_table_db('RESOURCE_TAGS', $TEST_DB);
#     change_table_db('RESOURCE_ACTIVE', $TEST_DB);
    
#     # $RESOURCE_QUEUE = substitute_table("$TEST_DB.eduresource_queue", "eduresource_queue");
#     # $RESOURCE_IMAGES = substitute_table("$TEST_DB.eduresource_images", "eduresource_images");
#     # $RESOURCE_TAG_NAMES = substitute_table("$TEST_DB.edutags", "edutags");
#     # $RESOURCE_TAGS = substitute_table("$TEST_DB.eduresource_tags", 'eduresource_tags');
#     # $RESOURCE_ACTIVE = substitute_table("$TEST_DB.eduresources", 'eduresources');

#     if ( $ds && $ds == 1 || ref $ds && $ds->debug )
#     {
# 	$ds->debug_line("TEST MODE: enable 'eduresources'\n");
#     }
    
#     return 1;
# }


# sub disable_test_mode {

#     my ($class, $table, $ds) = @_;
    
#     restore_table_name('RESOURCE_QUEUE');
#     restore_table_name('RESOURCE_IMAGES');
#     restore_table_name('RESOURCE_TAG_NAMES');
#     restore_table_name('RESOURCE_TAGS');
#     restore_table_name('RESOURCE_ACTIVE');
    
#     # $RESOURCE_QUEUE = original_table($RESOURCE_QUEUE);
#     # $RESOURCE_IMAGES = original_table($RESOURCE_IMAGES);
#     # $RESOURCE_TAG_NAMES = original_table($RESOURCE_TAG_NAMES);
#     # $RESOURCE_TAGS = original_table($RESOURCE_TAGS);
#     # $RESOURCE_ACTIVE = original_table($RESOURCE_ACTIVE);
    
#     if ( $ds && $ds == 1 || ref $ds && $ds->debug )
#     {
# 	$ds->debug_line("TEST MODE: disable 'eduresources'\n");
#     }
    
#     return 2;
# }


1;
