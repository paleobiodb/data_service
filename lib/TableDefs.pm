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
		      $IDIGBIO %IDP VALID_IDENTIFIER
		      $MACROSTRAT_LITHS
		      $MACROSTRAT_INTERVALS $MACROSTRAT_SCALES $MACROSTRAT_SCALES_INTS
		      $TIMESCALE_DATA $TIMESCALE_ARCHIVE
		      $TIMESCALE_REFS $TIMESCALE_INTS $TIMESCALE_BOUNDS $TIMESCALE_PERMS
		      $RESOURCE_QUEUE $RESOURCE_IMAGES $RESOURCE_TAG_NAMES $RESOURCE_TAGS $RESOURCE_ACTIVE
		      %TABLE_PROPERTIES %TEST_SELECT
		      %COMMON_FIELD_IDTYPE %COMMON_FIELD_OTHER %FOREIGN_KEY_TABLE);


# If the name of a test database was specified in the configuration file, remember it.

our ($TEST_DB);

{
    $TEST_DB = Dancer::config->{test_db};
}


# classic tables

our $COLLECTIONS = "collections";
our $AUTHORITIES = "authorities";
our $OPINIONS = "opinions";
our $REFERENCES = "refs";
our $OCCURRENCES = "occurrences";
our $REIDS = "reidentifications";
our $SPECIMENS = "specimens";

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

if ( $PBData::TEST_MODE )
{
    $TEST_SELECT{session_data} = sub {
	
	my ($ds, $enable) = @_;
	
	if ( $enable )
	{
	    die "You must define 'test_db' in the configuration file" unless $TEST_DB;
	    
	    $SESSION_DATA = substitute_table("$TEST_DB.session_data", "session_data");
	    $TABLE_PERMS = substitute_table("$TEST_DB.table_permissions", "table_permissions");
	    $PERSON_DATA = substitute_table("$TEST_DB.person", "person");

	    eval {
		PB2::CommonData->update_person_name_cache($ds);
	    };

	    print STDERR "TEST MODE: enable 'session_data'\n\n" if $ds->debug;
	    
	    return 1;
	}
	
	else
	{
	    $SESSION_DATA = "session_data";
	    $TABLE_PERMS = "table_permissions";
	    $PERSON_DATA = "person";
	    
	    print STDERR "TEST MODE: disable 'session_data'\n\n" if $ds->debug;
	    
	    return 2;
	}
    };
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
my $resource_tags_main = Dancer::config->{eduresources_tags} || 'eduresource_tags';
our $RESOURCE_TAGS = $resource_tags_main;
my $resource_active_main = Dancer::config->{eduresources_active} || 'eduresources';
our $RESOURCE_ACTIVE = $resource_active_main;

if ( $PBData::TEST_MODE )
{
    $TEST_SELECT{eduresources} = sub {
	
	my ($ds, $enable) = @_;
	
	if ( $enable )
	{
	    die "You must define 'test_db' in the configuration file" unless $TEST_DB;
	    
	    $RESOURCE_QUEUE = substitute_table("$TEST_DB.eduresource_queue", "eduresource_queue");
	    $RESOURCE_IMAGES = substitute_table("$TEST_DB.eduresource_images", "eduresource_images");
	    $RESOURCE_TAG_NAMES = substitute_table("$TEST_DB.edutags", "edutags");
	    $RESOURCE_TAGS = substitute_table("$TEST_DB.$resource_tags_main", $resource_tags_main);
	    $RESOURCE_ACTIVE = substitute_table("$TEST_DB.$resource_active_main", $resource_active_main);
	    
	    print STDERR "TEST MODE: enable 'eduresources'\n\n" if $ds->debug;
	    
	    return 1;
	}
	
	else
	{
	    $RESOURCE_QUEUE = 'eduresource_queue';
	    $RESOURCE_IMAGES = 'eduresource_images';
	    $RESOURCE_TAG_NAMES = 'edutags';
	    $RESOURCE_TAGS = $resource_tags_main;
	    $RESOURCE_ACTIVE = $resource_active_main;
	    
	    print STDERR "TEST MODE: disable 'eduresources'\n\n" if $ds->debug;
	    
	    return 2;
	}
    };
    
    # $USING_TEST_TABLES{eduresources} = $TEST_SELECT{eduresources};
}


# Define the properties of certain fields that are common to many tables in the PBDB.

our (%FOREIGN_KEY_TABLE) = ( taxon_no => 'AUTHORITIES',
			     resource_no => 'REFERENCES',
			     collection_no => 'COLLECTIONS',
			     interval_no => 'INTERVAL_DATA' );

our (%COMMON_FIELD_IDTYPE) = ( taxon_no => 'TXN',
			       resource_no => 'RES',
			       collection_no => 'COL',
			       interval_no => 'INT',
			       authorizer_no => 'PRS',
			       enterer_no => 'PRS',
			       modifier_no => 'PRS',
			     );

our (%COMMON_FIELD_OTHER) = ( authorizer_no => 'authent',
			      enterer_no => 'authent',
			      modifier_no => 'authent',
			      enterer_id => 'authent',
			      created => 'crmod',
			      modified => 'crmod',
			      admin_locked => 'admin',
			    );


# Define global hash variables to hold table properties and column properties, in a way that can
# be accessed by other modules. Routines for getting and setting these appear below.

our (%TABLE_PROPERTIES, %COLUMN_PROPERTIES);


# Now define routines for getting and setting table properties. We ignore any database prefix on
# the table name, because we want the properties to be the same regardless of whether they are in
# the main database, the test database, or some other database we have subsequently defined. We
# are operating under the assumption that two tables with the same name in different databases are
# meant to be alternatives to each other, i.e. a main table and a test table.

our (%TABLE_PROP_NAME) = ( ALLOW_POST => 1,
			   ALLOW_VIEW => 1,
			   ALLOW_DELETE => 1,
			   ALLOW_KEY_INSERT => 1,
			   BY_AUTHORIZER => 1,
			   AUTH_FIELDS => 1,
			   PRIMARY_KEY => 1,
			   PRIMARY_ATTR => 1 );

our (%COLUMN_PROP_NAME) = ( REQUIRED => 1,
			    ADMIN_SET => 1 );

our (%TABLE_NAME_MAP);


sub set_table_property {
    
    my ($table_name, $property, $value) = @_;
    
    croak "Invalid table property '$property'" unless $TABLE_PROP_NAME{$property};
    
    my $base_name = $TABLE_NAME_MAP{$table_name} || $table_name;
    $TABLE_PROPERTIES{$base_name}{$property} = $value;
    
    # if ( $table_name && $table_name =~ qr{ ( [^.]+ $ ) }xs )
    # {
    # 	croak "Invalid table property '$property'" unless $TABLE_PROP_NAME{$property};
	
    # 	$TABLE_PROPERTIES{$1}{$property} = $value;
    # 	$TABLE_NAME_MAP{$table_name} = $1;
    # }
    
    # else
    # {
    # 	$table_name ||= '';
    # 	croak "Invalid table name '$table_name'";
    # }
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
    
    # elsif ( $table_name && $table_name =~ qr{ ( [^.]+ $ ) }xs )
    # {
    # 	croak "Invalid table property '$property'" unless $TABLE_PROP_NAME{$property};
    # 	croak "No properties set for '$table_name'" unless $TABLE_PROPERTIES{$1};
	
    # 	$TABLE_NAME_MAP{$table_name} = $1;
    # 	$TABLE_PROPERTIES{$1}{$property} = undef;
    # 	return $TABLE_PROPERTIES{$1}{$property};
    # }
    
    # else
    # {
    # 	$table_name ||= '';
    # 	croak "Invalid table name '$table_name'";
    # }
}


sub set_column_property {
    
    my ($table_name, $column_name, $property, $value) = @_;
    
    croak "Invalid column property '$property'" unless $COLUMN_PROP_NAME{$property};
    
    my $base_name = $TABLE_NAME_MAP{$table_name} || $table_name;
    $TABLE_PROPERTIES{$base_name}{$column_name}{$property} = $value;
}


sub get_column_properties {

    my ($table_name) = @_;
    
    # croak "Invalid column property '$property'" unless $COLUMN_PROP_NAME{$property};
    
    if ( $COLUMN_PROPERTIES{$TABLE_NAME_MAP{$table_name}} )
    {
	return $COLUMN_PROPERTIES{$TABLE_NAME_MAP{$table_name}};
    }
    
    elsif ( $COLUMN_PROPERTIES{$table_name} )
    {
	$TABLE_NAME_MAP{$table_name} = $table_name;
	return $COLUMN_PROPERTIES{$TABLE_NAME_MAP{$table_name}};
    }
    
    elsif ( $TABLE_PROPERTIES{$table_name} )
    {
	return { };
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

1;
