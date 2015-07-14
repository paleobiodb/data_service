#
# ConfigData
# 
# A classs that returns information from the PaleoDB database about the
# values necessary to properly handle the data returned by other queries.
# 
# Author: Michael McClennen

package PB1::ConfigData;

use strict;

use TableDefs qw($CONTINENT_DATA $COLL_BINS $COUNTRY_MAP);
use TaxonDefs qw(%TAXON_RANK %RANK_STRING);

use Carp qw(carp croak);

our (@REQUIRES_ROLE) = qw(PB1::CommonData);

use Moo::Role;


# Variables to store the configuration information.

our ($BINS, $RANKS, $CONTINENTS, $COUNTRIES);


# Initialization
# --------------

# initialize ( )
# 
# This routine is called once by the Web::DataService module, to initialize this
# output class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # We start by defining an output map that lists the output blocks to be
    # used in generating responses for the operation defined by this class.
    # Each block is assigned a short key.
    
    $ds->define_set('1.1:config:get_map' =>
	{ value => 'clusters', maps_to => '1.1:config:geosum' },
	    "Return information about the levels of geographic clustering defined in this database.",
	{ value => 'ranks', maps_to => '1.1:config:ranks' },
	    "Return information about the taxonomic ranks defined in this database.",
	{ value => 'continents', maps_to => '1.1:config:continents' },
	    "Return information about the continents known to this database.",
	{ value => 'all', maps_to => '1.1:config:all' },
	    "Return all of the above blocks of information.",
	    "This is generally useful only with C<json> format.");
    
    # Next, define these output blocks.
    
    $ds->define_block('1.1:config:geosum' =>
	{ output => 'config_section', com_name => 'cfg', value => 'clu', if_field => 'cluster_level' },
	    "The configuration section: 'clu' for clusters",
	{ output => 'cluster_level', com_name => 'lvl' },
	    "Cluster level, starting at 1",
	{ output => 'degrees', com_name => 'deg' },
	    "The width and height of the area represented by each cluster, in degrees.  Each level of clustering is aligned so that",
	    "0 lat and 0 lng fall on cluster boundaries, and the cluster width/height must evenly divide 90.",
	{ output => 'count', com_name => 'cnt' },
	    "The approximate number of summary clusters at this level.",
	{ output => 'max_colls', com_name => 'mco' },
	    "The maximum nmber of collections in any cluster at this level (can be used for scaling cluster indicators)",
	{ output => 'max_occs', com_name => 'moc' },
	    "The maximum number of occurrences in any cluster at this level (can be used for scaling cluster indicators)");
    
    $ds->define_block('1.1:config:ranks' =>
	{ output => 'config_section', com_name => 'cfg', value => 'trn', if_field => 'taxonomic_rank' },
	    "The configuration section: 'trn' for taxonomic ranks",
	{ output => 'taxonomic_rank', com_name => 'rnk' },
	    "Taxonomic rank",
	{ output => 'rank_code', com_name => 'cod' },
	    "Numeric code representing this rank in responses using the 'com' vocabulary,",
	    "which is the default for C<json> format");
        
    $ds->define_block('1.1:config:continents' =>
	{ output => 'config_section', com_name => 'cfg', value => 'con', if_field => 'continent_name' },
	    "The configuration section: 'con' for continents",
	{ output => 'continent_name', com_name => 'nam' },
	    "Continent name",
	{ output => 'continent_code', com_name => 'cod' },
	    "The code used to indicate this continent when selecting fossil occurrences by continent");
    
    $ds->define_block('1.1:config:all',
	{ include => 'clusters' },
	{ include => 'ranks' },
	{ include => 'continents' });
    
    # Then define a ruleset to interpret the parmeters accepted by operations
    # from this class.
    
    $ds->define_ruleset('1.1:config' =>
	"The following URL parameters are accepted for this path:",
	{ param => 'show', valid => $ds->valid_set('1.1:config:get_map'),
	  list => q{,} },
	    "The value of this parameter selects which information to return:",
	{ allow => '1.1:special_params' },
	"^You can also use any of the L<special parameters|/data1.1/special_doc.html> with this request.");
    
    # Now gather the information that will be reported by this module.  It
    # won't change, so for the sake of efficiency we get it once at startup.
    
    # Get the list of geographical cluster data from the $COLL_BINS table.
    
    my $dbh = $ds->get_connection;
    
    my $sql = "
	SELECT b.bin_level as cluster_level, count(*) as count, max(n_colls) as max_colls, max(n_occs) as max_occs, 
		(SELECT 360.0/n_colls FROM $COLL_BINS as x
		 WHERE bin_level = b.bin_level and interval_no = 999999) as degrees
	FROM $COLL_BINS as b where interval_no = 0 GROUP BY bin_level";
    
    $BINS = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    # Get the list of taxonomic ranks from the module TaxonDefs.pm.
    
    $RANKS = [];
    
    foreach my $r ($TAXON_RANK{min}..$TAXON_RANK{max})
    {
	next unless exists $RANK_STRING{$r};
	push @$RANKS, { rank_code => $r, taxonomic_rank => $RANK_STRING{$r} };
    }
    
    # Get the list of continents from the database.
    
    $CONTINENTS = $dbh->selectall_arrayref("
	SELECT continent as continent_code, name as continent_name FROM $CONTINENT_DATA", { Slice => {} });
    
    # Get the list of countries from the database.
    
    $COUNTRIES = $dbh->selectall_arrayref("
	SELECT cc, continent, name FROM $COUNTRY_MAP", { Slice => {} });
}


# Data service operations
# -----------------------

# get ( )
# 
# Return configuration information.

sub get {

    my ($request) = @_;
    
    my $show_all; $show_all = 1 if $request->has_block('all');
    my @result;
    
    push @result, @$BINS if $request->has_block('clusters') or $show_all;
    push @result, @$RANKS if $request->has_block('ranks') or $show_all;
    push @result, @$CONTINENTS if $request->has_block('continents') or $show_all;
    
    if ( my $offset = $request->result_offset(1) )
    {
    	splice(@result, 0, $offset);
    }
    
    $request->list_result(@result);
}


1;
