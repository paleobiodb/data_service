# The Paleobiology Database
# 
#   CoreTableDefs.pm
# 
# Definitions and properties necessary for working with the core Paleobiology Database tables.
# 

package CoreTableDefs;

use strict;

use Carp qw(croak);

use TableDefs qw(set_table_name set_table_group set_table_property set_column_property);



# At runtime, set table and column properties.

{
    set_table_name(PERSON => 'person');
    
    set_table_name(TAXON_TREES => 'taxon_trees');
    
    set_table_name(AUTHORITY_DATA => 'authorities');
    set_table_property('AUTHORITY_DATA', CAN_POST => 'NOT_STUDENT');
    set_table_property('AUTHORITY_DATA', CAN_MODIFY => 'NOT_STUDENT');
    set_table_property('AUTHORITY_DATA', CAN_DELETE => 'admin');
    
    set_table_name(OPINION_DATA => 'opinions');
    set_table_property('OPINION_DATA', CAN_POST => 'NOT_STUDENT');
    set_table_property('OPINION_DATA', CAN_MODIFY => 'NOT_STUDENT');
    set_table_property('OPINION_DATA', CAN_DELETE => 'admin');
    
    set_table_name(OCCURRENCE_DATA => 'occurrences');
    set_table_property('OCCURRENCE_DATA', CAN_DELETE => 'admin');
    
    set_table_name(OCCURRENCE_MATRIX => 'occ_matrix');
    
    set_table_name(OCC_TAXON_SUMMARY => 'occ_taxon');
    set_table_name(OCC_REF_SUMMARY => 'ref_summary');
    set_table_name(OCC_INT_SUMMARY => 'int_summary');
    set_table_name(OCC_TS_SUMMARY => 'ts_summary');
    
    set_table_group('occurrence_data' => 'OCCURRENCE_DATA', 'OCCURRENCE_MATRIX',
		    'OCC_TAXON_SUMMARY', 'OCC_REF_SUMMARY', 
		    'OCC_INT_SUMMARY', 'OCC_TS_SUMMARY');
    
    set_table_name(REID_DATA => 'reidentifications');
    set_table_property('REID_DATA', CAN_DELETE => 'admin');
    
    set_table_name(COLLECTION_DATA => 'collections');
    set_table_name(COLLECTION_MATRIX => 'coll_matrix');
    
    set_table_property('COLLECTION_DATA', CAN_DELETE => 'admin');
    
    set_table_name(REFERENCE_DATA => 'refs');
    set_table_name(REFERENCE_AUTHORS => 'ref_authors');
    set_table_name(REFERENCE_EDITORS => 'ref_editors');
    set_table_name(REFERENCE_SOURCES => 'ref_sources');
    set_table_name(REFERENCE_SCORES => 'ref_scores');
    set_table_name(REFERENCE_SEARCH => 'ref_search');
    
    set_table_property('REFERENCE_DATA', PRIMARY_KEY => 'reference_no');
    set_table_property('REFERENCE_DATA', PRIMARY_FIELD => 'reference_id');
    set_table_property('REFERENCE_DATA', CAN_POST => 'AUTHORIZED');
    set_table_property('REFERENCE_DATA', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('REFERENCE_DATA', CAN_DELETE => 'admin');
    set_table_property('REFERENCE_DATA', REQUIRED_COLS => 'reftitle');
    
    set_table_name(INTERVAL_DATA => 'interval_data');
    set_table_name(CLASSIC_INTERVALS => 'intervals');
    set_table_name(CLASSIC_INTERVAL_LOOKUP => 'interval_lookup');
    set_table_name(SCALE_DATA => 'scale_data');
    set_table_name(SCALE_MAP => 'scale_map');
    
    set_table_name(SPECIMEN_DATA => 'specimens');
    set_table_name(SPECIMEN_MATRIX => 'spec_matrix');
    set_table_name(MEASUREMENT_DATA => 'measurements');
    set_table_name(SPECELT_DATA => 'specelt_data');
    set_table_name(SPECELT_MAP => 'specelt_map');
    
    set_table_group('specimen_data' => 'SPECIMEN_DATA', 'SPECIMEN_MATRIX', 'MEASUREMENT_DATA');
    set_table_group('specimen_elements' => 'SPECELT_DATA', 'SPECELT_MAP');
    
    set_table_property('SPECIMEN_DATA', CAN_POST => 'AUTHORIZED');
    set_table_property('SPECIMEN_DATA', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('SPECIMEN_DATA', PRIMARY_KEY => "specimen_no");
    set_table_property('SPECIMEN_DATA', PRIMARY_FIELD => "specimen_id");
    set_table_property('SPECIMEN_DATA', CASCADE_DELETE => [ 'MEASUREMENT_DATA' ]);
    
    set_column_property('SPECIMEN_DATA', 'specimen_id', REQUIRED => 1);
    set_column_property('SPECIMEN_DATA', 'specimen_id', ALTERNATE_NAME => 'specimen_code');
    set_column_property('SPECIMEN_DATA', 'specimen_id', ALTERNATE_ONLY => 1);
    set_column_property('SPECIMEN_DATA', 'reference_no', REQUIRED => 1);
    set_column_property('SPECIMEN_DATA', 'reference_no', ALTERNATE_NAME => 'reference_id');
    set_column_property('SPECIMEN_DATA', 'instcoll_no', IGNORE => 1);
    set_column_property('SPECIMEN_DATA', 'inst_code', IGNORE => 1);
    set_column_property('SPECIMEN_DATA', 'coll_code', IGNORE => 1);
    
    set_table_property('MEASUREMENT_DATA', CAN_POST => 'AUTHORIZED');
    set_table_property('MEASUREMENT_DATA', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('MEASUREMENT_DATA', PRIMARY_KEY => 'measurement_no');
    set_table_property('MEASUREMENT_DATA', PRIMARY_FIELD => 'measurement_id');
    
    set_column_property('MEASUREMENT_DATA', 'specimen_no', REQUIRED => 1);
    set_column_property('MEASUREMENT_DATA', 'specimen_no', ALTERNATE_NAME => 'specimen_id');
    set_column_property('MEASUREMENT_DATA', 'measurement_type', REQUIRED => 1);
    # set_column_property('MEASUREMENT_DATA', 'real_average', IGNORE => 1);
    # set_column_property('MEASUREMENT_DATA', 'real_median', IGNORE => 1);
    # set_column_property('MEASUREMENT_DATA', 'real_min', IGNORE => 1);
    # set_column_property('MEASUREMENT_DATA', 'real_max', IGNORE => 1);
    # set_column_property('MEASUREMENT_DATA', 'real_error', IGNORE => 1);
    
    set_table_property('SPECIMEN_MATRIX', PRIMARY_KEY => "specimen_no");
    set_table_property(SPEC_MATRIX => TABLE_COMMENT =>
		       "This table should not be modified directly, but only through automatic processes");
    
    set_table_name(PUBLICATIONS => 'pubs');
    set_table_property('PUBLICATIONS', PRIMARY_KEY => 'pub_no');
    set_table_property('PUBLICATIONS', PRIMARY_FIELD => 'pub_no');
    set_column_property('PUBLICATIONS', 'pub_no', EXTID_TYPE => 'PUB');
    
    set_table_name(ARCHIVES => 'data_archives');
    set_table_property('ARCHIVES', PRIMARY_KEY => 'archive_no');
    set_table_property('ARCHIVES', PRIMARY_FIELD => 'archive_id');
    # set_table_property('ARCHIVES', BY_AUTHORIZER => 1);
    
    set_table_name(APP_STATE => 'navigator_states');
    set_table_property('APP_STATE', PRIMARY_KEY => 'id');
    
    set_table_name(SESSION_DATA => 'session_data');
}



1;
