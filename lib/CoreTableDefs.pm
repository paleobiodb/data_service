# The Paleobiology Database
# 
#   CoreTableDefs.pm
# 
# Definitions and properties necessary for working with the core Paleobiology Database tables.
# 

package CoreTables;

use strict;

use Carp qw(croak);

use TableDefs qw(set_table_name set_table_group set_table_property set_column_property);



# At runtime, set table and column properties.

{
    set_table_name(AUTHORITY_DATA => 'authorities');
    set_table_name(OPINION_DATA => 'opinions');
    set_table_name(OCCURRENCE_DATA => 'occurrences');
    set_table_name(COLLECTION_DATA => 'collections');
    set_table_name(REFERENCE_DATA => 'refs');
    
    set_table_name(SPECIMEN_DATA => 'specimens');
    set_table_name(SPECIMEN_MATRIX => 'spec_matrix');
    set_table_name(SPECELT_DATA => 'specelt_data');
    set_table_name(SPECELT_MAP => 'specelt_map');
    
    set_table_group('specimen_data' => 'SPECIMENTS', 'SPEC_MATRIX');
    set_table_group('specimen_elements' => 'SPECELT_DATA', 'SPECELT_MAP');
    
    set_table_property('SPECIMEN_DATA', CAN_POST => 'AUTHORIZED');
    set_table_property('SPECIMEN_DATA', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('SPECIMEN_DATA', PRIMARY_KEY => "specimen_no");
    
    set_column_property('SPECIMEN_DATA', 'specimen_no', EXTID_TYPE => 'SPM');
    set_column_property('SPECIMEN_DATA', 'taxon_no', EXTID_TYPE => 'TID');
    set_column_property('SPECIMEN_DATA', 'specimen_id', REQUIRED => 1);
    set_column_property('SPECIMEN_DATA', 'specimen_id', ALTERNATE_NAME => 'specimen_code');
    set_column_property('SPECIMEN_DATA', 'specimen_id', ALTERNATE_ONLY => 1);
    set_column_property('SPECIMEN_DATA', 'reference_no', REQUIRED => 1);
    set_column_property('SPECIMEN_DATA', 'reference_no', EXTID_TYPE => 'REF');
    
    set_table_property(SPEC_MATRIX => TABLE_COMMENT =>
		       "This table should not be modified directly, but only through automatic processes");
}



1;
