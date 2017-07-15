# 
# The Paleobiology Database
# 
#   CollectionDefs.pm
# 

package TableDefs;

use strict;

use Carp qw(croak);

use base 'Exporter';

our (@EXPORT_OK) = qw($COLLECTIONS $AUTHORITIES $OPINIONS $REFERENCES $OCCURRENCES $REIDS $SPECIMENS
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
		      $RESOURCE_DATA $RESOURCE_QUEUE $RESOURCE_IMAGES 
		      %TABLE_PROPERTIES);


# classic tables

our $COLLECTIONS = "collections";
our $AUTHORITIES = "authorities";
our $OPINIONS = "opinions";
our $REFERENCES = "refs";
our $OCCURRENCES = "occurrences";
our $REIDS = "reidentifications";
our $SPECIMENS = "specimens";

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

our $RESOURCE_DATA = 'eduresources';
our $RESOURCE_QUEUE = 'eduresource_queue';
our $RESOURCE_IMAGES = 'eduresource_images';

# Table properties, especially regarding permissions. If the table has the
# property 'BY_ENTERER' then records may only be entered by their enterers,
# rather than by anybody in their authorizer group. If the table has the
# property 'ALLOW_POST', with the value 'MEMBERS', then all logged-in database
# members can post to it.

our (%TABLE_PROPERTIES) = ( $RESOURCE_QUEUE => { ALLOW_POST => 'MEMBERS' } );

1;
