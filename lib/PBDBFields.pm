#
# PBDBFields.pm
#
# This module lists the special column names used by the Paleobiology Database and the special
# proeprties they are given.



package PBDBFields;

use base 'Exporter';

our (@EXPORT_OK) = qw(%COMMON_FIELD_SPECIAL %COMMON_FIELD_IDTYPE %FOREIGN_KEY_TABLE %FOREIGN_KEY_COL);



our %COMMON_FIELD_SPECIAL = ( authorizer_no => 'auth_authorizer',
			      authorizer_id => 'auth_authorizer',
			      enterer_no => 'auth_creator',
			      enterer_id => 'auth_creator',
			      modifier_no => 'auth_modifier',
			      modifier_id => 'auth_modifier',
			      updater_no => 'auth_updater',
			      created => 'ts_created',
			      modified => 'ts_modified',
			      updated => 'ts_updated',
			      admin_lock => 'adm_lock',
			      owner_lock => 'own_lock' );

our %FOREIGN_KEY_TABLE = ( taxon_no => 'AUTHORITY_DATA',
			   resource_no => 'REFERENCE_DATA',
			   collection_no => 'COLLECTION_DATA',
			   occurrence_no => 'OCCURRENCE_DATA',
			   specimen_no => 'SPECIMEN_DATA',
			   measurement_no => 'MEASUREMENT_DATA',
			   specelt_no => 'SPECELT_DATA',
			   reid_no => 'REID_DATA',
			   opinion_no => 'OPINION_DATA',
			   interval_no => 'INTERVAL_DATA',
			   timescale_no => 'TIMESCALE_DATA',
			   bound_no => 'TIMESCALE_BOUNDS',
			   eduresource_no => 'RESOURCE_QUEUE',
			   person_no => 'WING_USERS',
			   authorizer_no => 'WING_USERS',
			   enterer_no => 'WING_USERS',
			   modifier_no => 'WING_USERS');

our %FOREIGN_KEY_COL = ( authorizer_no => 'person_no',
			 enterer_no => 'person_no',
			 modifier_no => 'person_no' );

our %COMMON_FIELD_IDTYPE = ( taxon_no => 'TID',
			     orig_no => 'TXN',
			     reference_no => 'REF',
			     collection_no => 'COL',
			     occurrence_no => 'OCC',
			     specimen_no => 'SPM',
			     measurement_no => 'MEA',
			     specelt_no => 'ELS',
			     reid_no => 'REI',
			     opinion_no => 'OPN',
			     interval_no => 'INT',
			     timescale_no => 'TSC',
			     bound_no => 'BND',
			     eduresource_no => 'EDR',
			     person_no => 'PRS',
			     authorizer_no => 'PRS',
			     enterer_no => 'PRS',
			     modifier_no => 'PRS' );


1;
