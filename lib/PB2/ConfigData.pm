#
# ConfigData
# 
# A classs that returns information from the PaleoDB database about the
# values necessary to properly handle the data returned by other queries.
# 
# Author: Michael McClennen

package PB2::ConfigData;

use strict;

use TableDefs qw($CONTINENT_DATA $COLL_BINS $COLL_LITH $COLLECTIONS $COUNTRY_MAP %TABLE);
use TaxonDefs qw(%TAXON_RANK %RANK_STRING);

use Carp qw(carp croak);

our (@REQUIRES_ROLE) = qw(PB2::CommonData);

use Moo::Role;


# Variables to store the configuration information.

our ($BINS, $RANKS, $CONTINENTS, $COUNTRIES, $LITHOLOGIES, $PCOORD_MODELS);


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
    
    $ds->define_set('1.2:config:get_map' =>
	{ value => 'clusters', maps_to => '1.2:config:geosum' },
	    "Return information about the levels of geographic clustering defined in this database.",
	{ value => 'ranks', maps_to => '1.2:config:ranks' },
	    "Return information about the taxonomic ranks defined in this database.",
	{ value => 'continents', maps_to => '1.2:config:continents' },
	    "Return continent names and their corresponding codes.",
	{ value => 'countries', maps_to => '1.2:config:countries' },
	    "Return country names and the corresponding ISO-3166-1 country codes.",
	{ value => 'lithologies', maps_to => '1.2:config:lithologies' },
	    "Return lithologies and lithology types.",
	{ value => 'pgmodels', maps_to => '1.2:config:pgmodels' },
	    "Return available paleogeography models.",
	{ value => 'all', maps_to => '1.2:config:all' },
	    "Return all of the above blocks of information.");
    
    # Next, define these output blocks.
    
    $ds->define_block('1.2:config:geosum' =>
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
    
    $ds->define_block('1.2:config:ranks' =>
	{ output => 'config_section', com_name => 'cfg', value => 'trn', if_field => 'taxonomic_rank' },
	    "The configuration section: 'trn' for taxonomic ranks",
	{ output => 'taxonomic_rank', com_name => 'rnk' },
	    "Taxonomic rank",
	{ output => 'rank_code', com_name => 'cod', data_type => 'pos' },
	    "Numeric code representing this rank in responses using the 'com' vocabulary,",
	    "which is the default for C<json> format");
    
    $ds->define_block('1.2:config:continents' =>
	{ output => 'config_section', com_name => 'cfg', value => 'con', if_field => 'continent_name' },
	    "The configuration section: 'con' for continents",
	{ output => 'continent_name', com_name => 'nam' },
	    "Continent name",
	{ output => 'continent_code', com_name => 'cod' },
	    "The code used to indicate this continent when selecting fossil occurrences by continent");
    
    $ds->define_block('1.2:config:countries' =>
	{ output => 'config_section', com_name => 'cfg', value => 'cou', if_field => 'name' },
	    "The configuration section: 'cnt' for countries",
	{ output => 'name', pbdb_name => 'country_name', com_name => 'nam' },
	    "Country name",
	{ output => 'cc', pbdb_name => 'country_code', com_name => 'cod' },
	    "The code used to indicate this continent when selecting fossil occurrences by country.",
	    "These are the standard ISO-3166-1 country codes.",
	{ output => 'continent', com_name => 'con' },
	    "The code for the continent on which this country is located");
    
    $ds->define_block('1.2:config:lithologies' => 
	{ output => 'config_section', com_name => 'cfg', value => 'lth', if_field => 'lithology' },
	{ output => 'lithology', com_name => 'lth' },
	{ output => 'lith_type', com_name => 'ltp' });
    
    $ds->define_block('1.2:config:pgmodels' =>
	{ set => '*', code => \&process_description },
	{ output => 'config_section', com_name => 'cfg', value => 'pgm', if_field => 'code' },
	{ output => 'code', com_name => 'cod' },
	{ output => 'label', com_name => 'lbl' },
	{ output => 'description', com_name => 'dsc'});
    
    $ds->define_block('1.2:config:all' =>
	{ include => '1.2:config:geosum' },
	{ include => '1.2:config:ranks' },
	{ include => '1.2:config:continents' },
	{ include => '1.2:config:countries' },
	{ include => '1.2:config:lithologies' },
	{ include => '1.2:config:pgmodels' });
    
    # Then define a ruleset to interpret the parmeters accepted by operations
    # from this class.
    
    $ds->define_ruleset('1.2:config' =>
	"The following URL parameters are accepted for this path:",
	{ param => 'show', valid => '1.2:config:get_map', list => ',' },
	    "The value of this parameter selects which information to return:",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|/data1.2/special_doc.html> with this request.");
    
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
    
    # Get the list of lithologies from the database.
    
    $LITHOLOGIES = $dbh->selectall_arrayref("
	SELECT distinct lithology, lith_type FROM $COLL_LITH", { Slice => {} });
    
    # Get the list of paleocoordinate models from the database.
    
    $PCOORD_MODELS = $dbh->selectall_arrayref("
	SELECT name, description FROM $TABLE{PCOORD_MODELS} WHERE is_active
	ORDER BY is_default desc, name asc", { Slice => { } });
    
    if ( ref $PCOORD_MODELS eq 'ARRAY' )
    {
	foreach my $entry ( @$PCOORD_MODELS )
	{
	    if ( $entry->{name} =~ /Wright/ )
	    {
		$entry->{code} = 'gplates';
		$entry->{label} = 'GPlates';
	    }
	    
	    elsif ( $entry->{name} =~ /([^\d]+)/ )
	    {
		$entry->{code} = lc $1;
		$entry->{label} = $1;
	    }
	    
	    else
	    {
		$entry->{code} = lc $entry->{label};
	    }
	}
    }
}


# Transform POD L<> tags into HTML anchor tags.

sub process_description {
    
    my ($request, $record) = @_;
    
    no warnings 'uninitialized';
    
    if ( $record->{description} =~ qr{ ^ (.*?) L< (.*?) [|] (.*?) > (.*) }xs )
    {
	$record->{description} = "$1<a href=\"$3\" target=\"_blank\">$2</a>$4";
    }
    
    elsif ( $record->{description} =~ qr{ ^ (.*) L< (.*?) > (.*) }xs )
    {
	$record->{description} = "$1$2$3";
    }
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
    push @result, @$COUNTRIES if $request->has_block('countries') or $show_all;
    push @result, @$LITHOLOGIES if $request->has_block('lithologies') or $show_all;
    push @result, @$PCOORD_MODELS if $request->has_block('pgmodels') or $show_all;
    
    if ( my $offset = $request->result_offset(1) )
    {
    	splice(@result, 0, $offset);
    }
    
    print STDERR "CONFIG REQUEST" . "\n\n" if $request->debug;
    
    $request->list_result(@result);
}


1;
