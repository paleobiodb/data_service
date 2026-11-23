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

# [Added by Claude under the guidance of Michael McClennen: the definitions for the Educational
# Resource tables are kept in their own module, but are loaded along with these so that every
# table is defined no matter which other modules are loaded.]

use ResourceDefs;



# At runtime, set table and column properties.

{
    set_table_name(PERSON_DATA => 'person');
    set_table_name(SESSION_DATA => 'session_data');
    
    set_table_name(TAXON_TREES => 'taxon_trees');
    set_table_name(TAXON_INTS => 'taxon_ints');
    set_table_name(TAXON_LOWER => 'taxon_lower');
    set_table_name(TAXON_ATTRS => 'taxon_attrs');
    set_table_name(TAXON_NAMES => 'taxon_names');
    set_table_name(TAXON_PICS => 'taxon_pics');
    
    set_table_name(AUTHORITY_DATA => 'authorities');
    set_table_property('AUTHORITY_DATA', CAN_POST => 'NOT_STUDENT');
    set_table_property('AUTHORITY_DATA', CAN_MODIFY => 'NOT_STUDENT');
    set_table_property('AUTHORITY_DATA', CAN_DELETE => 'OWNER');
    set_table_property('AUTHORITY_DATA', LOG_CHANGES => 1);
    
    set_table_name(AUTHORITY_AUTHORS => 'authority_authors');
    set_table_property('AUTHORITY_AUTHORS', LOG_CHANGES => 1);
    
    set_table_group('authority_data' => 'AUTHORITY_DATA', 'AUTHORITY_AUTHORS');
    
    set_table_name(OPINION_DATA => 'opinions');
    set_table_property('OPINION_DATA', CAN_POST => 'NOT_STUDENT');
    set_table_property('OPINION_DATA', CAN_MODIFY => 'NOT_STUDENT');
    set_table_property('OPINION_DATA', CAN_DELETE => 'OWNER');
    set_table_property('OPINION_DATA', LOG_CHANGES => 1);
    
    set_table_name(PHYLOPIC_DATA => 'phylopics');
    set_table_name(PHYLOPIC_NAMES => 'phylopic_names');
    set_table_name(PHYLOPIC_CHOICE => 'phylopic_choice');
    set_table_name(PHYLOPIC_SEEN => 'phylopic_seen');
    
    # [Moved here by Claude under the guidance of Michael McClennen from PhylopicEdit.pm.]
    set_table_property('PHYLOPIC_CHOICE', CAN_POST => 'ADMIN');
    set_table_property('PHYLOPIC_CHOICE', CAN_MODIFY => 'ADMIN');
    set_table_property('PHYLOPIC_CHOICE', CAN_DELETE => 'ADMIN');
    set_table_property('PHYLOPIC_CHOICE', REQUIRED_COLS => ['image_no']);
    set_column_property('PHYLOPIC_CHOICE', 'image_no', ALTERNATE_NAME => 'selected');
    set_column_property('PHYLOPIC_CHOICE', 'image_no', EXTID_TYPE => 'PHP');
    
    set_table_name(OCCURRENCE_DATA => 'occurrences');
    set_table_property('OCCURRENCE_DATA', AUTH_TABLE => 'COLLECTION_DATA');
    set_table_property('OCCURRENCE_DATA', CAN_DELETE => 'AUTHORIZED');
    set_table_property('OCCURRENCE_DATA', CAN_MOVE => 'NONE');
    set_table_property('OCCURRENCE_DATA', LOG_CHANGES => 1);
    
    # [Moved here by Claude under the guidance of Michael McClennen from CollectionEdit.pm.]
    set_table_property('OCCURRENCE_DATA', REQUIRED_COLS => ['reference_no']);
    set_column_property('OCCURRENCE_DATA', 'occurrence_no', EXTID_TYPE => 'OCC');
    set_column_property('OCCURRENCE_DATA', 'collection_no', EXTID_TYPE => 'COL');
    set_column_property('OCCURRENCE_DATA', 'reference_no', EXTID_TYPE => 'REF');
    set_column_property('OCCURRENCE_DATA', 'taxon_no', EXTID_TYPE => 'TID');
    
    set_table_name(OCCURRENCE_MATRIX => 'occ_matrix');
    
    set_table_name(OCC_TAXON_SUMMARY => 'occ_taxon');
    set_table_name(OCC_REF_SUMMARY => 'ref_summary');
    set_table_name(OCC_INT_SUMMARY => 'int_summary');
    set_table_name(OCC_TS_SUMMARY => 'ts_summary');
    
    # [Changed by Claude under the guidance of Michael McClennen: added REID_DATA.]
    set_table_group('occurrence_data' => 'OCCURRENCE_DATA', 'OCCURRENCE_MATRIX', 'REID_DATA',
		    'OCC_TAXON_SUMMARY', 'OCC_REF_SUMMARY', 
		    'OCC_INT_SUMMARY', 'OCC_TS_SUMMARY');
    
    set_table_name(REID_DATA => 'reidentifications');
    set_table_property('REID_DATA', AUTH_TABLE => 'COLLECTION_DATA');
    set_table_property('REID_DATA', CAN_DELETE => 'AUTHORIZED');
    set_table_property('REID_DATA', CAN_MOVE => 'NONE');
    set_table_property('REID_DATA', LOG_CHANGES => 1);
    
    # [Moved here by Claude under the guidance of Michael McClennen from CollectionEdit.pm.]
    set_table_property('REID_DATA', REQUIRED_COLS => ['reference_no']);
    set_column_property('REID_DATA', 'reid_no', EXTID_TYPE => 'REI');
    set_column_property('REID_DATA', 'occurrence_no', EXTID_TYPE => 'OCC');
    set_column_property('REID_DATA', 'collection_no', EXTID_TYPE => 'COL');
    set_column_property('REID_DATA', 'reference_no', EXTID_TYPE => 'REF');
    set_column_property('REID_DATA', 'taxon_no', EXTID_TYPE => 'TID');
    
    set_table_name(COLLECTION_DATA => 'collections');
    set_table_property('COLLECTION_DATA', CAN_POST => 'AUTHORIZED');
    set_table_property('COLLECTION_DATA', CAN_MODIFY => 'AUTHORIZED');	# [Moved here by Claude under the guidance of Michael McClennen from CollectionEdit.pm]
    set_table_property('COLLECTION_DATA', CAN_DELETE => 'OWNER');
    set_table_property('COLLECTION_DATA', LOG_CHANGES => 1);
    # [Moved here by Claude under the guidance of Michael McClennen from CollectionEdit.pm.]
    set_table_property('COLLECTION_DATA', REQUIRED_COLS =>
		       ['access_level', 'release_date', 'collection_name', 'collection_type',
			'reference_no', 'country', 'latdeg', 'lngdeg', 'latlng_basis',
			'lithology1', 'environment', 'assembl_comps']);
    set_column_property('COLLECTION_DATA', 'collection_no', EXTID_TYPE => 'COL');
    set_column_property('COLLECTION_DATA', 'reference_no', EXTID_TYPE => 'REF');
    
    set_table_name(COLLECTION_MATRIX => 'coll_matrix');
    
    set_table_name(COLLECTION_REFS => 'secondary_refs');
    set_table_property('COLLECTION_REFS', LOG_CHANGES => 1);
    
    set_table_name(COLLECTION_LOC => 'coll_loc');
    set_table_name(COLLECTION_INTS => 'coll_ints');
    set_table_name(COLLECTION_LITHS => 'coll_lith');
    set_table_name(COLLECTION_STRATA => 'coll_strata');
    set_table_name(COLLECTION_STRAT_NAMES => 'collection_strat_names');
    set_table_name(STRATA_NAMES => 'strat_raw');
    set_table_name(STRAT_RAW => 'strat_raw');
    set_table_name(STRAT_NAMES => 'strat_names');
    set_table_name(STRAT_NATTRS => 'strat_name_attrs');
    set_table_name(STRAT_NREFS => 'strat_name_refs');
    set_table_name(STRAT_CONCEPTS => 'strat_concepts');
    set_table_name(STRAT_CONCEPT_CCS => 'strat_concept_ccs');
    set_table_name(STRAT_OPINIONS => 'strat_opinions');
    set_table_name(STRAT_OREFS => 'strat_opinion_refs');
    set_table_name(STRAT_MS_NAMES => 'strat_ms_names');
    set_table_name(STRAT_MS_MATCHES => 'strat_ms_matches');
    set_table_name(STRAT_C_MATCHES => 'strat_candidate_matches');
    
    set_table_name(COLLECTION_UNITS => 'coll_units');
    set_table_property('COLLECTION_UNITS', LOG_CHANGES => 1);
    
    set_table_name(COLLECTION_UNITS_STATIC => 'coll_units_static');
    
    set_table_group('collection_data' => 'COLLECTION_DATA', 'COLLECTION_MATRIX',
		    'COLLECTION_LOC', 'COLLECTION_INTS', 'COLLECTION_STRATA',
		    'STRATA_NAMES', 'STRAT_RAW', 'STRAT_NAMES', 'STRAT_CONCEPTS',
		    'COLLECTION_UNITS');
    
    # [Removed by Claude under the guidance of Michael McClennen: the table names PALEOCOORDS,
    # PALEOSTATIC, PALEOCOORDS_BINS, PALEOSTATIC_BINS, and PALEOMODELS, and the table group
    # 'paleocoordinates'. The same tables are defined below as PCOORD_DATA, PCOORD_STATIC,
    # PCOORD_BINS_DATA, PCOORD_BINS_STATIC, and PCOORD_MODELS, in the group
    # 'paleocoord_data'. Nothing used the old names or the old group.]

    set_table_name(MACROSTRAT_LITHS => 'macrostrat.liths');
    
    set_table_name(COUNTRY_MAP => 'country_map');
    set_table_name(CONTINENT_DATA => 'continent_data');
    
    set_table_group('country_data' => 'COUNTRY_MAP', 'CONTINENT_DATA');
    
    set_table_name(SUMMARY_BINS => 'coll_bins');
    set_table_name(SUMMARY_LOC => 'bin_loc');
    
    set_table_group('collection_summary' => 'SUMMARY_BINS', 'SUMMARY_LOC');
    
    set_table_name(DIVERSITY_GLOBAL => 'div_global');
    set_table_name(DIVERSITY_MATRIX => 'div_matrix');
    
    # [Changed by Claude under the guidance of Michael McClennen: corrected the table names.]
    set_table_group(diversity_data => 'DIVERSITY_GLOBAL', 'DIVERSITY_MATRIX');
    
    set_table_name(PREVALENCE_GLOBAL => 'pvl_global');
    set_table_name(PREVALENCE_MATRIX => 'pvl_matrix');
    set_table_name(PREVALENCE_COLLS => 'pvl_collections');
    
    # [Changed by Claude under the guidance of Michael McClennen: corrected the table names.]
    set_table_group(prevalence_data => 'PREVALENCE_GLOBAL', 'PREVALENCE_MATRIX',
		    'PREVALENCE_COLLS');
    
    set_table_name(REFERENCE_DATA => 'refs');
    set_table_name(REFERENCE_AUTHORS => 'ref_authors');
    set_table_name(REFERENCE_EDITORS => 'ref_editors');
    set_table_name(REFERENCE_SOURCES => 'ref_sources');
    set_table_name(REFERENCE_SCORES => 'ref_scores');
    
    # [Added by Claude under the guidance of Michael McClennen: the table searched by
    # ReferenceEdit.pm for possible duplicates of a new reference. It is the same table as
    # REFERENCE_DATA, but it is deliberately left out of the 'reference_data' group so that
    # it is not switched over to the test database along with REFERENCE_DATA. Tests can then
    # check for duplicates of real references.]
    set_table_name(REFERENCE_DUPLICATE_SEARCH => 'refs');
    
    set_table_property('REFERENCE_DATA', PRIMARY_KEY => 'reference_no');
    set_table_property('REFERENCE_DATA', PRIMARY_FIELD => 'reference_id');
    set_table_property('REFERENCE_DATA', CAN_POST => 'AUTHORIZED');
    set_table_property('REFERENCE_DATA', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('REFERENCE_DATA', CAN_DELETE => 'OWNER');
    # [Changed by Claude under the guidance of Michael McClennen: the required columns are
    # those from ReferenceEdit.pm.]
    set_table_property('REFERENCE_DATA', REQUIRED_COLS => 'reftitle, publication_type, pubyr');
    set_table_property('REFERENCE_DATA', LOG_CHANGES => 1);
    
    # [Moved here by Claude under the guidance of Michael McClennen from ReferenceEdit.pm.]
    set_table_property('REFERENCE_DATA', SPECIAL_COLS => 
		       'ts_created, ts_modified, authorizer_no, enterer_no, modifier_no');
    set_column_property('REFERENCE_DATA', 'reference_no', EXTID_TYPE => 'REF');
    
    set_table_property('REFERENCE_AUTHORS', LOG_CHANGES => 1);
    set_table_property('REFERENCE_EDITORS', LOG_CHANGES => 1);
    
    set_table_group('reference_data' => 'REFERENCE_DATA', 'REFERENCE_AUTHORS',
		    'REFERENCE_EDITORS');
    
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
    set_table_property('SPECIMEN_DATA', LOG_CHANGES => 1);
    
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
    set_table_property('MEASUREMENT_DATA', LOG_CHANGES => 1);
    
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
    
    # [Removed by Claude under the guidance of Michael McClennen: a second block of interval
    # table definitions, which set INTERVAL_DATA again and defined INTERVAL_LOOKUP,
    # TIMESCALE_DATA, and TIMESCALE_MAP as other names for the tables defined above as
    # CLASSIC_INTERVAL_LOOKUP, SCALE_DATA, and SCALE_MAP, along with the table group
    # 'intervals'. Nothing used those names or the group except PB2/TimescaleData.pm,
    # whose methods were not reached by any route, and which has also been deleted.]
    
    set_table_name(INTERVAL_MAJOR_MAP => 'int_major_map');
    
    # Paleocoordinate tables
    
    set_table_name(PCOORD_DATA => 'paleocoords');
    set_table_name(PCOORD_STATIC => 'paleostatic');
    set_table_name(PCOORD_MODELS => 'paleomodels');
    set_table_name(PCOORD_PLATES => 'geoplates');
    set_table_name(PCOORD_BINS_DATA => 'paleocoords_bins');
    set_table_name(PCOORD_BINS_STATIC => 'paleostatic_bins');
    
    set_table_group(paleocoord_data => 'PCOORD_DATA', 'PCOORD_STATIC',
		    'PCOORD_MODELS', 'PCOORD_PLATES',
		    'PCOORD_BINS_DATA', 'PCOORD_BINS_STATIC');
    
    # Publications and archives
    
    set_table_name(PUBLICATIONS => 'pubs');
    set_table_property('PUBLICATIONS', PRIMARY_KEY => 'pub_no');
    set_table_property('PUBLICATIONS', PRIMARY_FIELD => 'pub_no');
    set_table_property('PUBLICATIONS', LOG_CHANGES => 1);
    set_table_property('PUBLICATIONS', CAN_POST => 'ADMIN');	# [Added by Claude under the guidance of Michael McClennen]
    set_table_property('PUBLICATIONS', CAN_MODIFY => 'ADMIN');	# [Added by Claude under the guidance of Michael McClennen]
    set_column_property('PUBLICATIONS', 'pub_no', EXTID_TYPE => 'PUB');
    
    set_table_name(ARCHIVES => 'data_archives');
    set_table_property('ARCHIVES', PRIMARY_KEY => 'archive_no');
    set_table_property('ARCHIVES', PRIMARY_FIELD => 'archive_id');
    set_table_property('ARCHIVES', LOG_CHANGES => 1);
    set_column_property('ARCHIVES', 'archive_no', EXTID_TYPE => 'DAR');	# [Added by Claude under the guidance of Michael McClennen]
    
    # [Added by Claude under the guidance of Michael McClennen: without these, edits to archives
    # were not permission-checked. Any database member may create an archive, and only its
    # owner or an administrator may modify or delete it.]
    
    set_table_property('ARCHIVES', CAN_POST => 'MEMBERS');
    set_table_property('ARCHIVES', CAN_DELETE => 'OWNER');
    # set_table_property('ARCHIVES', BY_AUTHORIZER => 1);
    
    set_table_name(APP_STATE => 'navigator_states');
    set_table_property('APP_STATE', PRIMARY_KEY => 'id');
    
    set_table_name(SESSION_DATA => 'session_data');
}



1;
