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

our ($BINS, $RANKS, $CONTINENTS, $COUNTRIES, 
     $LITHOLOGIES, $MINOR_LITHS, $LITHIFICATION, $LITH_ADJECTIVES,
     $ENVIRONMENTS, $TEC_SETTINGS, $COLL_METHODS, $DATE_METHODS,
     $PRES_MODES, $PCOORD_MODELS, $RES_GROUPS);


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
	{ value => 'collblock', maps_to => '1.2:config:collblock' },
	    "Return all of the information necessary for editing collections:",
	    "lithologies, lithifications, minor lithologies, lithology adjectives,",
	    "environments, tectonic settings, collection methods, dating methods,",
	    "collection/preservation modes, and research groups",
	{ value => 'lithblock', maps_to => '1.2:config:lithblock' },
	    "Return lithologies, lithology types, minor lithologies, and lithology adjectives.",
	{ value => 'lithologies', maps_to => '1.2:config:lithologies' },
	    "Return major lithologies.",
	{ value => 'minorliths', maps_to => '1.2:config:minorliths' },
	    "Return minor lithologies.",
	{ value => 'lithification', maps_to => '1.2:config:lithification' },
	    "Return lithification values.",
	{ value => 'lithadj', maps_to => '1.2:config:lithadjs' },
	    "Return lithology adjectives.",
	{ value => 'envs', maps_to => '1.2:config:environments' },
	    "Return environments.",
	{ value => 'tecs', maps_to => '1.2:config:tecsettings' },
	    "Return tectonic settings.",
	{ value => 'collmet', maps_to => '1.2:config:collmet' },
	    "Return collection methods.",
	{ value => 'datemet', maps_to => '1.2:config:datemet' },
	    "Return dating methods.",
	{ value => 'presmodes', maps_to => '1.2:config:pres_modes' },
	    "Return preservation modes.",
	{ value => 'pgmodels', maps_to => '1.2:config:pgmodels' },
	    "Return available paleogeography models.",
	{ value => 'resgroups', maps_to => '1.2:config:resgroups' },
	    "Return research group names.",
	{ value => 'all', maps_to => '1.2:config:all' },
	    "Return all of the above blocks of information.");
    
    # Next, define these output blocks.
    
    $ds->define_block('1.2:config:geosum' =>
	{ output => 'config_section', com_name => 'cfg', value => 'clu', if_field => 'cluster_level' },
	    "Value 'clu' for clusters",
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
	    "Value 'trn' for taxonomic ranks",
	{ output => 'taxonomic_rank', com_name => 'rnk' },
	    "Taxonomic rank",
	{ output => 'rank_code', com_name => 'cod', data_type => 'pos' },
	    "Numeric code representing this rank in responses using the 'com' vocabulary,",
	    "which is the default for C<json> format");
    
    $ds->define_block('1.2:config:continents' =>
	{ output => 'config_section', com_name => 'cfg', value => 'con', if_field => 'continent_name' },
	    "Value 'con' for continents",
	{ output => 'continent_name', com_name => 'nam' },
	    "Continent name",
	{ output => 'continent_code', com_name => 'cod' },
	    "The code used to indicate this continent when selecting fossil occurrences by continent");
    
    $ds->define_block('1.2:config:countries' =>
	{ output => 'config_section', com_name => 'cfg', value => 'cou', if_field => 'name' },
	    "Value 'cou' for countries",
	{ output => 'name', pbdb_name => 'country_name', com_name => 'nam' },
	    "Country name",
	{ output => 'cc', pbdb_name => 'country_code', com_name => 'cod' },
	    "The code used to indicate this continent when selecting fossil occurrences by country.",
	    "These are the standard ISO-3166-1 country codes.",
	{ output => 'continent', com_name => 'con' },
	    "The code for the continent on which this country is located");
    
    $ds->define_block('1.2:config:lithologies' => 
	{ output => 'config_section', com_name => 'cfg', value => 'lth', if_field => 'lithology' },
	    "Value 'lth' for lithologies",
	{ output => 'lithology', com_name => 'nam' },
	    "Lithology name",
	{ output => 'lith_type', com_name => 'ltp' },
	    "Lithology type");
    
    $ds->define_block('1.2:config:minorliths' => 
	{ output => 'config_section', com_name => 'cfg', value => 'mlt', if_field => 'minorlith' },
	    "Value 'mlt' for minor lithologies",
	{ output => 'minorlith', com_name => 'nam' },
	    "Minor lithology value");
    
    $ds->define_block('1.2:config:lithification' =>
	{ output => 'config_section', com_name => 'cfg', value => 'ltf', if_field => 'lithification' },
	    "Value 'ltf' for lithification descriptions",
	{ output => 'lithification', com_name => 'nam' },
	    "Lithification description");
    
    $ds->define_block('1.2:config:lithadjs' =>
	{ output => 'config_section', com_name => 'cfg', value => 'lta', if_field => 'lithadj' },
	    "Value 'lta' for litholgy adjectives",
	{ output => 'lithadj', com_name => 'nam' },
	    "Lithology adjective");
    
    $ds->define_block('1.2:config:lithblock' =>
	{ include => '1.2:config:lithologies' },
	{ include => '1.2:config:minorliths' },
	{ include => '1.2:config:lithification' },
	{ include => '1.2:config:lithadjs' });
    
    $ds->define_block('1.2:config:environments' => 
	{ output => 'config_section', com_name => 'cfg', value => 'env', if_field => 'environment' },
	    "Value 'env' for environments",
	{ output => 'environment', com_name => 'nam' },
	    "Environment name");
    
    $ds->define_block('1.2:config:tecsettings' =>
	{ output => 'config_section', com_name => 'cfg', value => 'tec', if_field => 'tec_setting' },
	    "Value 'tec' for tectonic settings",
	{ output => 'tec_setting', com_name => 'nam' },
	    "Tectonic setting name");
    
    $ds->define_block('1.2:config:collmets' =>
	{ output => 'config_section', com_name => 'cfg', value => 'cmt', if_field => 'coll_method' },
	    "Value 'cmt' for collection/preparation methods",
	{ output => 'coll_method', com_name => 'nam' },
	    "Collection/preparation method name");
    
    $ds->define_block('1.2:config:datemets' =>
	{ output => 'config_section', com_name => 'cfg', value => 'dmt', if_field => 'date_method' },
	   "Value 'dmt' for dating methods",
	{ output => 'date_method', com_name => 'nam' },
	    "Dating methods");
    
    $ds->define_block('1.2:config:pres_modes' =>
	{ output => 'config_section', com_name => 'cfg', value => 'prm', if_field => 'pres_mode' },
	    "The configuration section: 'prm' for preservation modes",
	{ output => 'pres_mode', com_name => 'nam' },
	    "Preservation mode");
    
    $ds->define_block('1.2:config:resgroups' =>
	{ output => 'config_section', com_name => 'cfg', value => 'rsg', if_field => 'group_name' },
	    "Value 'rsg' for research groups",
	{ output => 'group_name', com_name => 'nam' },
	    "Research group name");
    
    $ds->define_block('1.2:config:collblock' =>
	{ include => '1.2:config:lithologies' },
	{ include => '1.2:config:minorliths' },
	{ include => '1.2:config:lithification' },
	{ include => '1.2:config:lithadjs' },
	{ include => '1.2:config:environments' },
	{ include => '1.2:config:collmets' },
	{ include => '1.2:config:datemets' },
	{ include => '1.2:config:tecsettings' },
	{ include => '1.2:config:pres_modes' },
	{ include => '1.2:config:resgroups' });
    
    $ds->define_block('1.2:config:pgmodels' =>
	{ set => '*', code => \&process_description },
	{ output => 'config_section', com_name => 'cfg', value => 'pgm', if_field => 'code' },
	    "The configuration section: 'pgm' for paleogeographic models",
	{ output => 'code', com_name => 'cod' },
	    "Use this string to select this model.",
	{ output => 'label', com_name => 'lbl' },
	    "The name of the model.",
	{ output => 'description', com_name => 'dsc'},
	    "Description of the model, including the bibliographic reference for the source.");
    
    $ds->define_block('1.2:config:all' =>
	{ include => '1.2:config:geosum' },
	{ include => '1.2:config:ranks' },
	{ include => '1.2:config:continents' },
	{ include => '1.2:config:countries' },
	{ include => '1.2:config:lithologies' },
	{ include => '1.2:config:minorliths' },
	{ include => '1.2:config:lithification' },
	{ include => '1.2:config:lithadjs' },
	{ include => '1.2:config:environments' },
	{ include => '1.2:config:tecsettings' },
	{ include => '1.2:config:collmets' },
	{ include => '1.2:config:datemets' },
	{ include => '1.2:config:pres_modes' },	
	{ include => '1.2:config:pgmodels' },
	{ include => '1.2:config:resgroups' });
    
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
    
    my ($field, $field_type) = $dbh->selectrow_array("
	SHOW COLUMNS FROM $TABLE{COLLECTION_DATA} like 'lithology2'");
    
    my @lithology_list = $field_type =~ /'(.*?)'/g;
    
    my $lith_types = $dbh->selectall_arrayref("
	SELECT distinct lithology, lith_type FROM $COLL_LITH", { Slice => { } });
    
    my %lith_type;
    
    foreach my $row ( @$lith_types )
    {
	$lith_type{$row->{lithology}} = $row->{lith_type};
    }
    
    add_divisions(\@lithology_list, '"siliciclastic"', '"mixed carbonate-siliciclastic"',
		  'lime mudstone', 'calcareous ooze', 'amber', 'evaporite', 'phyllite',
		  '"volcaniclastic"');
    
    $LITHOLOGIES = [ ];
    
    push @$LITHOLOGIES, { lithology => $_, lith_type => $lith_type{$_} || 'mixed' } 
	foreach @lithology_list;
    
    # Get the list of minor lithologies from the database.
    
    ($field, $field_type) = $dbh->selectrow_array("
	SHOW COLUMNS FROM $TABLE{COLLECTION_DATA} like 'minor_lithology'");
    
    my @minor_lith_list = $field_type =~ /'(.*?)'/g;
    
    $MINOR_LITHS = [ ];
    
    push @$MINOR_LITHS, { minorlith => $_ } foreach @minor_lith_list;
    
    # Get the list of lithification descriptions from the database.
    
    ($field, $field_type) = $dbh->selectrow_array("
	SHOW COLUMNS FROM $TABLE{COLLECTION_DATA} like 'lithification'");
    
    my @lithification_list = $field_type =~ /'(.*?)'/g;
    
    $LITHIFICATION = [ ];
    
    push @$LITHIFICATION, { lithification => $_ } foreach @lithification_list;
    
    # Get the list of lithology adjectives from the database.
    
    ($field, $field_type) = $dbh->selectrow_array("
	SHOW COLUMNS FROM $COLLECTIONS like 'lithadj'");
    
    my @lithadj_list = $field_type =~ /'(.*?)'/g;
    
    add_divisions(\@lithadj_list, 'condensed', 'very fine', 'bentonitic', 'flat-pebble', 'black');
    
    $LITH_ADJECTIVES = [ ];
    
    push @$LITH_ADJECTIVES, { lithadj => $_ } foreach @lithadj_list;
    
    # Get the list of environments from the database.
    
    ($field, $field_type) = $dbh->selectrow_array("
	SHOW COLUMNS FROM $TABLE{COLLECTION_DATA} like 'environment'");
    
    my @env_list = $field_type =~ /'(.*?)'/g;
    
    unshift @env_list, '-- General --';
    
    add_divisions(\@env_list, ['carbonate indet.', '-- Carbonate marine --'],
		  'peritidal', 'reef, buildup or bioherm', 'deep subtidal ramp',
		  'slope', ['marginal marine indet', '-- Siliciclastic marine --'],
		  'estuary/bay', 'delta plain', 'foreshore', 'submarine fan',
		  ['fluvial indet.', '-- Terrestrial --'],
		  'lacustrine - large', 'dune', 'cave', 'tar');
    
    $ENVIRONMENTS = [ ];
    
    push @$ENVIRONMENTS, { environment => $_ } foreach @env_list;
    
    # Get the list of tectonic settings from the database.
    
    ($field, $field_type) = $dbh->selectrow_array("
	SHOW COLUMNS FROM $TABLE{COLLECTION_DATA} like 'tectonic_setting'");
    
    my @tec_list = $field_type =~ /'(.*?)'/g;
    
    $TEC_SETTINGS = [ ];
    
    push @$TEC_SETTINGS, { tec_setting => $_ } foreach @tec_list;
    
    # Get the list of collection/preservation methods from the database.
    
    ($field, $field_type) = $dbh->selectrow_array("
	SHOW COLUMNS FROM $TABLE{COLLECTION_DATA} like 'coll_meth'");
    
    my @collmet_list = $field_type =~ /'(.*?)'/g;
    
    add_divisions(\@collmet_list, ['bulk', '-- Collection methods --'],
		  ['chemical', '-- Preservation methods --'],
		  ['field collection', '-- Fossil source --'],
		  ['repository not specified', '-- Other --']);
    
    $COLL_METHODS = [ ];
    
    push @$COLL_METHODS, { coll_method => $_ } foreach @collmet_list;
    
    # Get the list of dating methods from the database.
    
    ($field, $field_type) = $dbh->selectrow_array("
	SHOW COLUMNS FROM $TABLE{COLLECTION_DATA} like 'direct_ma_method'");
    
    my @datemet_list = $field_type =~ /'(.*?)'/g;
    
    $DATE_METHODS = [ ];
    
    push @$DATE_METHODS, { date_method => $_ } foreach @datemet_list;
    
    # Get the list of preservation modes from the database.
    
    ($field, $field_type) = $dbh->selectrow_array("
	SHOW COLUMNS FROM $COLLECTIONS like 'pres_mode'");
    
    my @presmode_list = $field_type =~ /'(.*?)'/g;
    
    $PRES_MODES = [ ];
    
    push @$PRES_MODES, { pres_mode => $_ } foreach @presmode_list;
    
    # Get the list of research groups from the database.
    
    ($field, $field_type) = $dbh->selectrow_array("
	SHOW COLUMNS FROM $COLLECTIONS like 'research_group'");
    
    my @resgroup_list = $field_type =~ /'(.*?)'/g;
    
    $RES_GROUPS = [ ];
    
    push @$RES_GROUPS, { group_name => $_ } foreach @resgroup_list;
    
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


# Add divisions to lists that will be used for menus or other user interface
# selection elements.

sub add_divisions {
    
    my ($list, @divisions) = @_;
    
    my $index = 0;
    
    foreach my $item ( @divisions )
    {
	my ($search, $add);
	
	if ( ref $item )
	{
	    ($search, $add) = @$item;
	}
	
	else
	{
	    $search = $item;
	    $add = '--';
	}
	
	# Start the search where the previous one left off, for efficiency.
	
	my $found = undef;
	my $bound = $index;
	
	for ( ; $index <= $#$list; $index++ )
	{
	    # If we found what we were looking for, splice in either the
	    # indicated label element or else '--' which represents a section
	    # divider. 
	    
	    if ( defined $list->[$index] && $list->[$index] eq $search )
	    {
		splice @$list, $index, 0, $add;
		$index++;
		$found = 1;
		last;
	    }
	}
	
	# If we didn't find the item, try the search once more from the beginning.
	
	unless ( $found )
	{
	    for ( $index = 0; $index < $bound; $index++ )
	    {
		if ( $list->[$index] eq $search )
		{
		    splice @$list, $index, 0, $add;
		    last;
		}
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
    my $show_lith; $show_lith = 1 if $request->has_block('lithblock');
    my $show_coll; $show_coll = 1 if $request->has_block('collblock');
    $show_lith = 1 if $show_coll;
    my @result;
    
    push @result, @$BINS if $request->has_block('clusters') or $show_all;
    push @result, @$RANKS if $request->has_block('ranks') or $show_all;
    push @result, @$CONTINENTS if $request->has_block('continents') or $show_all;
    push @result, @$COUNTRIES if $request->has_block('countries') or $show_all;
    push @result, @$LITHOLOGIES if $request->has_block('lithologies') or $show_all or $show_lith;
    push @result, @$MINOR_LITHS if $request->has_block('minorliths') or $show_all or $show_lith;
    push @result, @$LITHIFICATION if $request->has_block('lithification') or $show_all or $show_lith;
    push @result, @$LITH_ADJECTIVES if $request->has_block('lithadj') or $show_all or $show_lith;
    push @result, @$ENVIRONMENTS if $request->has_block('envs') or $show_all or $show_coll;
    push @result, @$TEC_SETTINGS if $request->has_block('tecs') or $show_all or $show_coll;
    push @result, @$COLL_METHODS if $request->has_block('collmet') or $show_all or $show_coll;
    push @result, @$DATE_METHODS if $request->has_block('datemet') or $show_all or $show_coll;
    push @result, @$PRES_MODES if $request->has_block('presmodes') or $show_all or $show_coll;
    push @result, @$RES_GROUPS if $request->has_block('resgroups') or $show_all or $show_coll;
    push @result, @$PCOORD_MODELS if $request->has_block('pgmodels') or $show_all;
    
    if ( my $offset = $request->result_offset(1) )
    {
    	splice(@result, 0, $offset);
    }
    
    print STDERR "CONFIG REQUEST" . "\n\n" if $request->debug;
    
    $request->list_result(@result);
}


1;
