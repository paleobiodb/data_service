# 
# The Paleobiology Database
# 
#   CollectionDefs.pm
# 

package TableDefs;

use strict;

use base 'Exporter';

our (@EXPORT_OK) = qw($COLLECTIONS $AUTHORITIES $OPINIONS $REFERENCES $OCCURRENCES $REIDS
		      $COLL_MATRIX $COLL_BINS $COLL_STRATA $COUNTRY_MAP $CONTINENT_DATA
		      $PALEOCOORDS $GEOPLATES $COLL_LOC
		      $OCC_MATRIX $OCC_EXTRA $OCC_TAXON $OCC_REF $DIV_SAMPLE
		      $INTERVAL_DATA $INTERVAL_MAP $INTERVAL_BRACKET $INTERVAL_BUFFER
		      $SCALE_DATA $SCALE_LEVEL_DATA $SCALE_MAP
		      $PHYLOPICS $PHYLOPIC_NAMES $PHYLOPIC_CHOICE $TAXON_PICS);

# classic tables

our $COLLECTIONS = "collections";
our $AUTHORITIES = "authorities";
our $OPINIONS = "opinions";
our $REFERENCES = "refs";
our $OCCURRENCES = "occurrences";
our $REIDS = "reidentifications";

# new collection tables

our $COLL_MATRIX = "coll_matrix";
our $COLL_BINS = "coll_bins";
our $COLL_STRATA = "coll_strata";
our $COLL_LOC = "coll_loc";
our $COUNTRY_MAP = "country_map";
our $CONTINENT_DATA = "continent_data";
our $PALEOCOORDS = 'paleocoords';
our $GEOPLATES = 'geoplates';

# new occurrence tables

our $OCC_MATRIX = "occ_matrix";
our $OCC_EXTRA = "occ_extra";
our $OCC_TAXON = "occ_taxon";
our $OCC_REF = "occ_ref";

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


1;
