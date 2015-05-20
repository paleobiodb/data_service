# 
# The Paleobiology Database
# 
#   CollectionDefs.pm
# 

package TableDefs;

use strict;

use Carp qw(croak);

use base 'Exporter';

our (@EXPORT_OK) = qw($COLLECTIONS $AUTHORITIES $OPINIONS $REFERENCES $OCCURRENCES $REIDS
		      $COLL_MATRIX $COLL_BINS $COLL_STRATA $COUNTRY_MAP $CONTINENT_DATA
		      $BIN_KEY $BIN_LOC $BIN_CONTAINER
		      $PALEOCOORDS $GEOPLATES $COLL_LOC $COLL_INTS
		      $DIV_MATRIX $DIV_GLOBAL $PVL_MATRIX $PVL_GLOBAL
		      $OCC_MATRIX $OCC_EXTRA $OCC_TAXON $REF_SUMMARY
		      $OCC_BUFFER_MAP $OCC_MAJOR_MAP $OCC_CONTAINED_MAP $OCC_OVERLAP_MAP
		      $INTERVAL_DATA $INTERVAL_MAP $INTERVAL_BRACKET $INTERVAL_BUFFER
		      $SCALE_DATA $SCALE_LEVEL_DATA $SCALE_MAP
		      $PHYLOPICS $PHYLOPIC_NAMES $PHYLOPIC_CHOICE $TAXON_PICS
		      $IDIGBIO %IDP VALID_IDENTIFIER);

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
our $COLL_INTS = "coll_ints";
our $COLL_STRATA = "coll_strata";
our $COLL_LOC = "coll_loc";
our $COUNTRY_MAP = "country_map";
our $CONTINENT_DATA = "continent_data";
our $BIN_LOC = "bin_loc";
our $BIN_CONTAINER = "bin_container";
our $PALEOCOORDS = 'paleocoords';
our $GEOPLATES = 'geoplates';

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

# List the identifier prefixes:

our %IDP = ( URN => 'urn:paleobiodb.org:',
	     TID => 'txn|var',
	     TXN => 'txn',
	     VAR => 'var',
	     OPN => 'opn',
	     REF => 'ref',
	     OCC => 'occ',
	     COL => 'col',
	     INT => 'int',
	     SCL => 'scl',
	     CLU => 'clu' );

# VALID_IDENT ( type )
# 
# Return a closure which will validate identifiers of the given type.
# Acceptable values are any positive integer <n>, optionally preceded by the
# type, optionally preceded by the common URN prefix.  So, for example, each
# of the following are equivalent for specifying taxon number 23021:
# 
# 23021, txn23021, urn:paleobiodb.org:txn23021

sub VALID_IDENTIFIER {
    
    my ($type) = @_;
    
    croak "VALID_IDENTIFIER requires a parameter indicating the identifier type" unless $type;
    
    return sub { return valid_identifier(shift, shift, $type) };
}


# Construct the regular expressions to validate the various identifier types.

our %IDRE;

my $key_expr = '';

foreach my $key ( keys %IDP )
{
    next if $key eq 'URN';
    $IDRE{$key} = qr{ ^ (?: (?: $IDP{URN} )? (?: $IDP{$key} ) )? ( [0]+ | [1-9][0-9]* ) $ }xsi;
    $key_expr .= '|' if $key_expr;
    $key_expr .= $IDP{$key};
}

$IDRE{ANY} = qr{ ^ (?: (?: $IDP{URN} )? (?: $key_expr ) )? ( [0]+ | [1-9][0-9]* ) $ }xsi;


# valid_ident ( value, context, type )
# 
# Validator subroutine for paleobiodb.org identifiers.

sub valid_identifier {

    my ($value, $context, $type) = @_;
    
    if ( $value =~ $IDRE{$type} )
    {
	return { value => $1 };
    }
    
    if ( $value =~ qr{ ^ urn: }xsi )
    {
	if ( $value =~ qr{ ^ $IDP{URN} (.*) }xsi )
	{
	    return { error => "the value of {param} must be '$IDP{URN}$IDP{$type}' " .
		     "followed by a nonnegative integer (was '$value')" };
	}
	
	else
	{
	    return { error => "the value of {param} must be a valid local identifier " .
		     "or must start with the prefix '$IDP{URN}' (was {value})" };
	}
    }
    
    if ( $type eq 'ANY' )
    {
	return { error => "the value of {param} must be a nonnegative integer, optionally prefixed with an identifier type" };
    }
    
    else
    {
	return { error => "the value of {param} must be a nonnegative integer, optionally prefixed with '$IDP{$type}'" };
    }
}



1;
