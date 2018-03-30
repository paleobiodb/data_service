# 
# The Paleobiology Database
# 
#   TableDefs.pm
#
# This file specifies the database tables to be used by the data service code.
#
# This allows for tables to be later renamed without having to search laboriously through the code
# for each statement referring to them. It also provides for operating the data service in "test
# mode", which can select alternate tables for use in running the unit tests.

package TableDefs;

use strict;

use Carp qw(croak);
use Hash::Util qw(lock_hash unlock_hash);

use base 'Exporter';

our (@EXPORT_OK) = qw($COLLECTIONS $AUTHORITIES $OPINIONS $REFERENCES $OCCURRENCES $REIDS $SPECIMENS
		      $PERSON_DATA $SESSION_DATA $TABLE_PERMS $WING_USERS
		      $COLL_MATRIX $COLL_BINS $COLL_STRATA $COUNTRY_MAP $CONTINENT_DATA
		      $COLL_LITH $COLL_ENV $STRATA_NAMES
		      $BIN_KEY $BIN_LOC $BIN_CONTAINER
		      $PALEOCOORDS $GEOPLATES $COLL_LOC $COLL_INTS
		      $DIV_MATRIX $DIV_GLOBAL $PVL_MATRIX $PVL_GLOBAL
		      $OCC_MATRIX $OCC_TAXON $REF_SUMMARY $SPEC_MATRIX
		      $SPEC_ELEMENTS $SPEC_ELT_MAP $LOCALITIES $WOF_PLACES $COLL_EVENTS
		      $OCC_BUFFER_MAP $OCC_MAJOR_MAP $OCC_CONTAINED_MAP $OCC_OVERLAP_MAP
		      $INTERVAL_DATA $INTERVAL_MAP $INTERVAL_BRACKET $INTERVAL_BUFFER
		      $SCALE_DATA $SCALE_LEVEL_DATA $SCALE_MAP
		      $PHYLOPICS $PHYLOPIC_NAMES $PHYLOPIC_CHOICE $TAXON_PICS
		      $IDIGBIO
		      $MACROSTRAT_LITHS
		      $MACROSTRAT_INTERVALS $MACROSTRAT_SCALES $MACROSTRAT_SCALES_INTS
		      $TIMESCALE_DATA $TIMESCALE_ARCHIVE
		      $TIMESCALE_REFS $TIMESCALE_INTS $TIMESCALE_BOUNDS $TIMESCALE_PERMS
		      $RESOURCE_QUEUE $RESOURCE_IMAGES $RESOURCE_TAG_NAMES $RESOURCE_TAGS $RESOURCE_ACTIVE
		      $EDT_TEST $EDT_AUX $EDT_ANY
		      %COMMON_FIELD_SPECIAL %COMMON_FIELD_IDTYPE %FOREIGN_KEY_TABLE %FOREIGN_KEY_COL 
		      init_table_names select_test_tables is_test_mode
		      set_table_property get_table_property original_table
		      set_column_property get_column_properties);


# Define the properties that are allowed to be specified for tables and table columns.

our (%TABLE_PROP_NAME) = ( CAN_POST => 1,
			   CAN_VIEW => 1,
			   CAN_MODIFY => 1,
			   ALLOW_DELETE => 1,
			   ALLOW_INSERT_KEY => 1,
			   BY_AUTHORIZER => 1,
			   PRIMARY_KEY => 1,
			   PRIMARY_ATTR => 1,
			   NO_LOG => 1,
			   TABLE_COMMENT => 1 );

our (%COLUMN_PROP_NAME) = ( ALTERNATE_NAME => 1,
			    FOREIGN_KEY => 1,
			    FOREIGN_TABLE => 1,
			    EXTID_TYPE => 1,
			    ALLOW_TRUNCATE => 1,
			    VALUE_SEPARATOR => 1,
			    VALIDATOR => 1,
			    REQUIRED => 1,
			    ADMIN_SET => 1,
			    COLUMN_COMMENT => 1 );


# Define the properties of certain fields that are common to many tables in the PBDB.

our (%FOREIGN_KEY_TABLE) = ( taxon_no => 'AUTHORITIES',
			     resource_no => 'REFERENCES',
			     collection_no => 'COLLECTIONS',
			     occurrence_no => 'OCCURRENCES',
			     specimen_no => 'SPECIMENS',
			     measurement_no => 'MEASUREMENTS',
			     spec_elt_no => 'SPEC_ELEMENTS',
			     reid_no => 'REIDS',
			     opinion_no => 'OPINIONS',
			     interval_no => 'INTERVAL_DATA',
			     timescale_no => 'TIMESCALE_DATA',
			     bound_no => 'TIMESCALE_BOUNDS',
			     eduresource_no => 'RESOURCE_QUEUE',
			     person_no => 'WING_USERS',
			     authorizer_no => 'WING_USERS',
			     enterer_no => 'WING_USERS',
			     modifier_no => 'WING_USERS');

our (%FOREIGN_KEY_COL) = ( authorizer_no => 'person_no',
			   enterer_no => 'person_no',
			   modifier_no => 'person_no' );

our (%COMMON_FIELD_IDTYPE) = ( taxon_no => 'TXN',
			       resource_no => 'RES',
			       collection_no => 'COL',
			       occurrence_no => 'OCC',
			       specimen_no => 'SPM',
			       measurement_no => 'MEA',
			       spec_elt_no => 'ELS',
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

our (%COMMON_FIELD_SPECIAL) = ( authorizer_no => 'authent',
				enterer_no => 'authent',
				modifier_no => 'authent',
				enterer_id => 'authent',
				created => 'crmod',
				modified => 'crmod',
				admin_locked => 'admin' );


# Define the mechanism for substituting test tables instead of real ones.

our ($TEST_MODE, $TEST_DB);


# init_table_names ( config, test_mode )
# 
# If this subroutine is run with $test_mode true, then it enables the flag that allows switching
# over to the test tables using 'select_test_tables'.

sub init_table_names
{
    my ($config, $test_mode) = @_;
    
    if ( $test_mode )
    {
	$TEST_MODE = $test_mode;
	$TEST_DB = $config->{test_db};
    }
}


sub is_test_mode
{
    return $TEST_MODE;
}


# select_test_tables ( tablename, enable, ds )
# 
# If $enable is true, then set the table name(s) associated with $tablename to their test values,
# as opposed to their regular ones. If $enable is false, switch them back. The argument $ds should
# be a data service object if this is run from a data service process. If run from a command-line
# script, then the argument should be '1' to enable debugging output.

sub select_test_tables
{
    my ($tablename, $enable, $ds) = @_;
    
    my $debug = defined $ds ? (ref $ds && $ds->debug || $ds eq '1') : 0;
    
    if ( $tablename eq 'session_data' )
    {
	return test_session_data($enable, $ds, $debug);
    }
    
    elsif ( $tablename eq 'eduresources' )
    {
	return test_eduresources($enable, $ds, $debug);
    }

    elsif ( $tablename eq 'edt_test' )
    {
	return test_edt($enable, $ds, $debug);
    }
    
    else
    {
	die "500 unknown tablename '$tablename'"
    }
}


# classic tables

our $COLLECTIONS = "collections";
our $AUTHORITIES = "authorities";
our $OPINIONS = "opinions";
our $REFERENCES = "refs";
our $OCCURRENCES = "occurrences";
our $REIDS = "reidentifications";
our $SPECIMENS = "specimens";
our $MEASUREMENTS = "measurements";

set_table_property($COLLECTIONS, PRIMARY_KEY => 'collection_no');
set_table_property($AUTHORITIES, PRIMARY_KEY => 'taxon_no');
set_table_property($OPINIONS, PRIMARY_KEY => 'opinion_no');
set_table_property($REFERENCES, PRIMARY_KEY => 'reference_no');
set_table_property($OCCURRENCES, PRIMARY_KEY => 'occurrence_no');
set_table_property($REIDS, PRIMARY_KEY => 'reid_no');
set_table_property($SPECIMENS, PRIMARY_KEY => 'specimen_no');


# Authentication and permission tables

our $PERSON_DATA = "person";
our $TABLE_PERMS = "table_permissions";
our $SESSION_DATA = "session_data";
our $WING_USERS = "pbdb_wing.users";

set_table_property($PERSON_DATA, PRIMARY_KEY => 'person_no');
set_table_property($SESSION_DATA, PRIMARY_KEY => 'session_id');
set_table_property($WING_USERS, PRIMARY_KEY => 'id');

# If we are being run in test mode, substitute table names as indicated by the configuration file.

sub test_session_data {
    
    my ($enable, $ds, $debug) = @_;
    
    if ( $enable )
    {
	die "You must define 'test_db' in the configuration file and call 'init_table_names'" unless $TEST_DB;
	
	$SESSION_DATA = substitute_table("$TEST_DB.session_data", "session_data");
	$TABLE_PERMS = substitute_table("$TEST_DB.table_permissions", "table_permissions");
	$PERSON_DATA = substitute_table("$TEST_DB.person", "person");
	
	eval {
	    PB2::CommonData->update_person_name_cache($ds) if ref $ds;
	};
	
	print STDERR "TEST MODE: enable 'session_data'\n\n" if $debug;
	
	return 1;
    }
    
    else
    {
	$SESSION_DATA = "session_data";
	$TABLE_PERMS = "table_permissions";
	$PERSON_DATA = "person";
	
	print STDERR "TEST MODE: disable 'session_data'\n\n" if $debug;
	
	return 2;
    }
}


# new collection tables

our $COLL_MATRIX = "coll_matrix";
our $COLL_BINS = "coll_bins";
our $COLL_INTS = "coll_ints";
our $COLL_STRATA = "coll_strata";
our $STRATA_NAMES = "strata_names";
our $COLL_LOC = "coll_loc";
our $COUNTRY_MAP = "country_map";
our $CONTINENT_DATA = "continent_data";
our $BIN_LOC = "bin_loc";
our $BIN_CONTAINER = "bin_container";
our $PALEOCOORDS = 'paleocoords';
our $GEOPLATES = 'geoplates';

our $COLL_LITH = 'coll_lith';
our $COLL_ENV = 'coll_env';

our $BIN_KEY = "999999";

# new occurrence tables

our $OCC_MATRIX = "occ_matrix";
our $OCC_EXTRA = "occ_extra";
our $OCC_TAXON = "occ_taxon";
our $REF_SUMMARY = "ref_summary";

our $OCC_BUFFER_MAP = 'occ_buffer_map';
our $OCC_MAJOR_MAP = 'occ_major_map';
our $OCC_CONTAINED_MAP = 'occ_contained_map';
our $OCC_OVERLAP_MAP = 'occ_overlap_map';

# new specimen tables

our $SPEC_MATRIX = "spec_matrix";
our $SPEC_ELEMENTS = "spec_elements";
our $SPEC_ELT_MAP = "spec_elt_map";

our $LOCALITIES = "localities";
our $WOF_PLACES = "wof_places";
our $COLL_EVENTS = "coll_events";

# new interval tables

our $INTERVAL_DATA = "interval_data";
our $SCALE_DATA = "scale_data";
our $SCALE_LEVEL_DATA = "scale_level_data";
our $SCALE_MAP = "scale_map";
our $INTERVAL_BRACKET = "interval_bracket";
our $INTERVAL_MAP = "interval_map";
our $INTERVAL_BUFFER = "interval_buffer";

# taxon pic tables

our $PHYLOPICS = 'phylopics';
our $PHYLOPIC_NAMES = 'phylopic_names';
our $PHYLOPIC_CHOICE = 'phylopic_choice';
our $TAXON_PICS = 'taxon_pics';

# taxon diversity and prevalence tables

our $DIV_MATRIX = 'div_matrix';
our $DIV_GLOBAL = 'div_global';
our $PVL_MATRIX = 'pvl_matrix';
our $PVL_GLOBAL = 'pvl_global';

# iDigBio external info table

our $IDIGBIO = 'idigbio';

# Macrostrat tables that we use

our $MACROSTRAT_LITHS = 'macrostrat.liths';
our $MACROSTRAT_INTERVALS = 'macrostrat.intervals';
our $MACROSTRAT_SCALES = 'macrostrat.timescales';
our $MACROSTRAT_SCALES_INTS = 'macrostrat.timescales_intervals';

# New timescale system

our $TIMESCALE_DATA = 'timescales';
our $TIMESCALE_REFS = 'timescale_refs';
our $TIMESCALE_INTS = 'timescale_ints';
our $TIMESCALE_BOUNDS = 'timescale_bounds';
our $TIMESCALE_QUEUE = 'timescale_queue';
our $TIMESCALE_PERMS = 'timescale_perms';

# Educational resources

our $RESOURCE_QUEUE = 'eduresource_queue';
our $RESOURCE_IMAGES = 'eduresource_images';
our $RESOURCE_TAG_NAMES = 'edutags';
our $RESOURCE_TAGS = 'eduresource_tags',
our $RESOURCE_ACTIVE = 'eduresources';

sub test_eduresources {
	
    my ($enable, $ds, $debug) = @_;
    
    if ( $enable )
    {
	die "You must define 'test_db' in the configuration file" unless $TEST_DB;
	
	$RESOURCE_QUEUE = substitute_table("$TEST_DB.eduresource_queue", "eduresource_queue");
	$RESOURCE_IMAGES = substitute_table("$TEST_DB.eduresource_images", "eduresource_images");
	$RESOURCE_TAG_NAMES = substitute_table("$TEST_DB.edutags", "edutags");
	$RESOURCE_TAGS = substitute_table("$TEST_DB.eduresource_tags", 'eduresource_tags');
	$RESOURCE_ACTIVE = substitute_table("$TEST_DB.eduresources", 'eduresources');
	
	print STDERR "TEST MODE: enable 'eduresources'\n\n" if $debug;
	
	return 1;
    }
    
    else
    {
	$RESOURCE_QUEUE = 'eduresource_queue';
	$RESOURCE_IMAGES = 'eduresource_images';
	$RESOURCE_TAG_NAMES = 'edutags';
	$RESOURCE_TAGS = 'eduresource_tags';
	$RESOURCE_ACTIVE = 'eduresources';
	
	print STDERR "TEST MODE: disable 'eduresources'\n\n" if $debug;
	
	return 2;
    }
}


# Test class for EditTransaction.

our $EDT_TEST = 'edt_test';
our $EDT_AUX = 'edt_aux';
our $EDT_ANY = 'edt_any';

sub test_edt {
    
    my ($enable, $ds, $debug) = @_;

    if ( $enable )
    {
	die "You must define 'test_db' in the configuration file" unless $TEST_DB;
	
	$EDT_TEST = substitute_table("$TEST_DB.edt_test", "edt_test");
	$EDT_AUX = substitute_table("$TEST_DB.edt_aux", "edt_aux");
	$EDT_ANY = substitute_table("$TEST_DB.edt_any", "edt_any");
	
	print STDERR "TEST MODE: enable 'edt_test'\n\n" if $debug;
	
	return 1;
    }

    else
    {
	$EDT_TEST = 'edt_test';
	$EDT_AUX = 'edt_aux';
	$EDT_ANY = 'edt_any';
	
	print STDERR "TEST MODE: disable 'edt_test'\n\n" if $debug;

	return 2;
    }
}
	
	
# Define global hash variables to hold table properties and column properties, in a way that can
# be accessed by other modules. Routines for getting and setting these appear below.

our (%TABLE_PROPERTIES, %COLUMN_PROPERTIES);


# Now define routines for getting and setting table properties. We ignore any database prefix on
# the table name, because we want the properties to be the same regardless of whether they are in
# the main database, the test database, or some other database we have subsequently defined. We
# are operating under the assumption that two tables with the same name in different databases are
# meant to be alternatives to each other, i.e. a main table and a test table.


our (%TABLE_NAME_MAP);


sub set_table_property {
    
    my ($table_name, $property, $value) = @_;
    
    croak "Invalid table property '$property'" unless $TABLE_PROP_NAME{$property};
    
    my $base_name = $TABLE_NAME_MAP{$table_name} || $table_name;
    $TABLE_PROPERTIES{$base_name}{$property} = $value;
}


sub get_table_property {
    
    my ($table_name, $property) = @_;
    
    croak "Invalid table property '$property'" unless $TABLE_PROP_NAME{$property};
    
    if ( $TABLE_PROPERTIES{$TABLE_NAME_MAP{$table_name}} )
    {
	return $TABLE_PROPERTIES{$TABLE_NAME_MAP{$table_name}}{$property};
    }
    
    elsif ( $TABLE_PROPERTIES{$table_name} )
    {
	$TABLE_NAME_MAP{$table_name} = $table_name;
	return $TABLE_PROPERTIES{$TABLE_NAME_MAP{$table_name}}{$property};
    }
    
    else
    {
	croak "No properties set for table '$table_name'";
    }
}


sub set_column_property {
    
    my ($table_name, $column_name, $property, $value) = @_;
    
    croak "Invalid column property '$property'" unless $COLUMN_PROP_NAME{$property};
    
    my $base_name = $TABLE_NAME_MAP{$table_name} || $table_name;
    
    $COLUMN_PROPERTIES{$base_name}{$column_name}{$property} = $value;

    if ( ref $value )
    {
	if ( $property eq 'VALIDATOR' )
	{	
	    croak "value must be either a code ref or a string" unless ref $value eq 'CODE';
	}

	elsif ( $property eq 'VALUE_SEPARATOR' )
	{
	    croak "value must be a regexp" unless ref $value eq 'Regexp';
	}
    }
}


sub get_column_property {
    
    my ($table_name, $column_name, $property) = @_;
    
    return $COLUMN_PROPERTIES{$TABLE_NAME_MAP{$table_name} || $table_name}{$column_name}{$property};
}


sub get_column_properties {

    my ($table_name, $column_name) = @_;
    
    my $base_name = ($TABLE_NAME_MAP{$table_name} ||= $table_name);
    
    if ( $column_name )
    {
	return %{$COLUMN_PROPERTIES{$base_name}{$column_name}};
    }
    
    elsif ( $COLUMN_PROPERTIES{$base_name} )
    {
	return map { $_ => 1 } keys %{$COLUMN_PROPERTIES{$base_name}};
    }
        
    elsif ( $TABLE_PROPERTIES{$base_name} )
    {
	return;
    }
    
    else
    {
	croak "No properties set for table '$table_name'";	
    }
}


sub substitute_table {

    my ($new_name, $old_name) = @_;
    
    $TABLE_NAME_MAP{$new_name} = $old_name;
    return $new_name;
}


sub original_table {
    
    return $TABLE_NAME_MAP{$_[0]};
}


1;

