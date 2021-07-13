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
    set_table_name(TAXON_TREES => 'taxon_trees');
    set_table_name(AUTHORITY_DATA => 'authorities');
    set_table_name(OPINION_DATA => 'opinions');
    
    set_table_name(COLLECTION_DATA => 'collections');
    set_table_name(COLLECTION_MATRIX => 'coll_matrix');
    set_table_name(COLLECTION_STRATA => 'coll_strata');
    set_table_name(COLLECTION_LITHOLOGIES => 'coll_lith');
    set_table_name(COLLECTION_LOCATION => 'coll_loc');
    set_table_name(COLLECTION_BIN_DATA => 'coll_bins');
    set_table_name(COLLECTION_BIN_LOCATION => 'bin_loc');
    set_table_name(COUNTRY_MAP => 'country_map');
    set_table_name(ENVIRONMENT_MAP => 'environment_map');
    set_table_name(MACROSTRAT_LITHS => 'macrostrat.liths');
    set_table_name(PALEOCOORD_DATA => 'paleocoords');
    set_table_name(STRATUM_DATA => 'strata_names');
    set_table_name(CONTINENT_DATA => 'continent_data');
    
    set_table_name(OCCURRENCE_DATA => 'occurrences');
    set_table_name(OCCURRENCE_MATRIX => 'occ_matrix');
    set_table_name(OCCURRENCE_TAXON_SUMMARY => 'occ_taxon');
    set_table_name(OCCURRENCE_REF_SUMMARY => 'ref_summary');

    set_table_name(TAXON_COLLECTION_COUNTS => 'taxon_colls');
    
    set_table_name(INTERVAL_DATA => 'interval_data');
    set_table_name(SCALE_MAP => 'scale_map');
    
    set_table_group('occurrence_data' => 'OCCURRENCE_DATA', 'OCCURRENCE_MATRIX');
    
    set_table_name(REFERENCE_DATA => 'refs');
    
    set_table_property('REFERENCE_DATA', CAN_POST => 'AUTHORIZED');
    set_table_property('REFERENCE_DATA', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('REFERENCE_DATA', PRIMARY_KEY => 'reference_no');
    set_table_property('REFERENCE_DATA', PRIMARY_FIELD => 'reference_id');
    
    set_column_property('REFERENCE_DATA', 'reftitle', REQUIRED => 1);
    set_column_property('REFERENCE_DATA', 'publication_type', REQUIRED => 1);
    
    set_table_name(REFERENCE_SOURCES => 'ref_sources');
    
    set_table_property('REFERENCE_SOURCES', PRIMARY_KEY => 'refsource_no');
    
    set_table_name ('REFERENCE_SCORES' => 'ref_scores');
    
    set_table_name(INTERVAL_DATA => 'interval_data');
    
    set_table_name(SPECIMEN_DATA => 'specimens');
    set_table_name(SPECIMEN_MATRIX => 'spec_matrix');
    set_table_name(MEASUREMENT_DATA => 'measurements');
    set_table_name(SPECELT_DATA => 'specelt_data');
    set_table_name(SPECELT_MAP => 'specelt_map');
    
    set_table_group('specimen_data' => 'SPECIMEN_DATA', 'SPECIMEN_MATRIX', 'MEASUREMENT_DATA');
    set_table_group('specimen_elements' => 'SPECELT_DATA', 'SPECELT_MAP');
    
    set_table_property('SPECIMEN_DATA', CAN_POST => 'AUTHORIZED');
    set_table_property('SPECIMEN_DATA', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('SPECIMEN_DATA', ALLOW_DELETE => 1);
    set_table_property('SPECIMEN_DATA', PRIMARY_KEY => "specimen_no");
    set_table_property('SPECIMEN_DATA', PRIMARY_FIELD => "specimen_id");
    
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
    set_table_property('MEASUREMENT_DATA', ALLOW_DELETE => 1);
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
    set_table_property('PUBLICATIONS', PRIMARY_FIELD => 'pub_id');
    
    set_table_name(ARCHIVES => 'data_archives');
    set_table_property('ARCHIVES', PRIMARY_KEY => 'archive_no');
    set_table_property('ARCHIVES', PRIMARY_FIELD => 'archive_id');
    set_table_property('ARCHIVES', BY_AUTHORIZER => 1);
    
    set_table_name(APP_STATE => 'navigator_states');
    set_table_property('APP_STATE', PRIMARY_KEY => 'id');
}



1;
