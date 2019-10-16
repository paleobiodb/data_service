# 
# The Paleobiology Database
# 
#   TaxonDefs.pm
# 

package TaxonDefs;

use strict;

use base 'Exporter';

our (@EXPORT_OK) = qw(@TREE_TABLE_LIST %TAXON_TABLE %TAXON_RANK %RANK_STRING
		      %UNS_NAME %UNS_RANK
		      %AUTH_TABLE %OPINION_TABLE %OPINION_CACHE %REFS_TABLE
		      %ATTRS_TABLE %INTS_TABLE %SEARCH_TABLE
		      %TAXONOMIC_STATUS %NOMENCLATURAL_STATUS
		      $ALL_STATUS $VALID_STATUS $INVALID_STATUS $JUNIOR_STATUS $SENIOR_STATUS
		      $VARIANT_STATUS
		      $CLASSIC_TREE_CACHE $CLASSIC_LIST_CACHE
		      @ECOTAPH_FIELD_DEFS $RANK_MAP);

our (@TREE_TABLE_LIST) = ('taxon_trees');

our (%TAXON_TABLE);


# If new sets of taxonomy tables are added, they will need to be defined here.  Each new tree
# table will need its own 'names' table, 'attrs' table, etc.  The new tree table(s) may or may not
# be associated with separate 'authorities', 'opinions', etc.

$TAXON_TABLE{taxon_trees} = {
    names => 'taxon_names',
    attrs => 'taxon_attrs',
    ages => 'taxon_ages',
    search => 'taxon_search',
    ints => 'taxon_ints',
    lower => 'taxon_lower',
    counts => 'taxon_counts',
    ecotaph => 'taxon_ecotaph',
    etbasis => 'taxon_etbasis',
    images => 'taxon_pics',
    
    authorities => 'authorities',
    opinions => 'opinions',
    opcache => 'order_opinions',
    et_base => 'ecotaph',
    refs => 'refs'
 };

# We need to kill $CLASSIC_LIST_CACHE as soon as possible.

our $CLASSIC_TREE_CACHE = "taxa_tree_cache";
our $CLASSIC_LIST_CACHE = "taxa_list_cache";
our $RANK_MAP = "rank_map";

# This rank hierarchy has not changed from paleobiodb classic, and probably will not change in the future.

our (%TAXON_RANK) = ( 'max' => 26, 26 => 26, 'informal' => 26, 'unranked_clade' => 25, 'unranked' => 25, 25 => 25,
		      'kingdom' => 23, 23 => 23, 'subkingdom' => 22, 22 => 22,
		      'superphylum' => 21, 21 => 21, 'phylum' => 20, 20 => 20, 'subphylum' => 19, 19 => 19,
		      'superclass' => 18, 18 => 18, 'class' => 17, 17 => 17, 'subclass' => 16, 16 => 16,
		      'infraclass' => 15, 15 => 15, 'superorder' => 14, 14 => 14, 'order' => 13, 13 => 13,
		      'suborder' => 12, 12 => 12, 'infraorder' => 11, 11 => 11, 'superfamily' => 10, 10 => 10,
		      'family' => 9, 9 => 9, 'subfamily' => 8, 8 => 8, 'tribe' => 7, 7 => 7, 'subtribe' => 6, 6 => 6,
		      'genus' => 5, 5 => 5, 'subgenus' => 4, 4 => 4, 'species' => 3, 3 => 3, 'subspecies' => 2, 2 => 2, 'min' => 2 );

# This is a reverse of the above map.

our (%RANK_STRING) = ( 26 => 'informal', 25 => 'unranked clade', 23 => 'kingdom',
		       22 => 'subkingdom', 21 => 'superphylum', 20 => 'phylum', 19 => 'subphylum',
		       18 => 'superclass', 17 => 'class', 16 => 'subclass', 15 => 'infraclass',
		       14 => 'superorder', 13 => 'order', 12 => 'suborder', 11 => 'infraorder',
		       10 => 'superfamily', 9 => 'family', 8 => 'subfamily', 7 => 'tribe', 
		       6 => 'subtribe', 5 => 'genus', 4 => 'subgenus', 3 => 'species', 2 => 'subspecies');

# Unspecified taxonomic classifications have their own codes.

our (%UNS_NAME) = ( 'NS' => 'NO_SPECIES_SPECIFIED',
		    'NG' => 'NO_GENUS_SPECIFIED',
		    'NF' => 'NO_FAMILY_SPECIFIED',
		    'NO' => 'NO_ORDER_SPECIFIED',
		    'NC' => 'NO_CLASS_SPECIFIED',
		    'NP' => 'NO_PHYLUM_SPECIFIED',
		    'NK' => 'NO_KINGDOM_SPECIFIED',
		    'NT' => 'NO_TAXON_SPECIFIED' );

our (%UNS_RANK) = ( 'NF' => 3, 'NG' => 5, 'NF' => 9, 'NO' => 13, 'NC' => 17, 'NP' => 20,
		    'NK' => 23, 'NT' => 0 );

# The status codes and various subsets

our ($ALL_STATUS) = "'belongs to','subjective synonym of','objective synonym of','invalid subgroup of','misspelling of','replaced by','nomen dubium','nomen nudum','nomen oblitum','nomen vanum','root'";
our ($VARIANT_STATUS) = "'misspelling of'";
our ($INVALID_STATUS) = "'nomen dubium','nomen nudum','nomen oblitum','nomen vanum','invalid subgroup of'";
our ($JUNIOR_STATUS) = "'subjective synonym of','objective synonym of','replaced by'";
our ($SENIOR_STATUS) = "'belongs to', $INVALID_STATUS";
our ($VALID_STATUS) = "'belongs to','subjective synonym of','objective synonym of','replaced by','root'";


# These maps translate the paleobiodb status codes into the attributes needed by Darwin Core.

our (%TAXONOMIC_STATUS) = (
	'belongs to' => 'valid',
	'subjective synonym of' => 'heterotypic synonym',
	'objective synonym of' => 'homotypic synonym',
	'invalid subgroup of' => 'invalid',
	'misspelling of' => 'invalid',
	'replaced by' => 'invalid',
	'nomen dubium' => 'invalid',
	'nomen nudum' => 'invalid',
	'nomen oblitum' => 'invalid',
	'nomen vanum' => 'invalid',
);


our (%NOMENCLATURAL_STATUS) = (
	'invalid subgroup of' => 'invalid subgroup',
	'misspelling of' => 'misspelling',
	'replaced by' => 'replaced by',
	'nomen dubium' => 'nomen dubium',
	'nomen nudum' => 'nomen nudum',
	'nomen oblitum' => 'nomen oblitum',
	'nomen vanum' => 'nomen vanum',
);


our (@ECOTAPH_FIELD_DEFS) = (
	# taph
	{ basis => 'taphonomy_basis_no', 
	  fields => ['composition', 'thickness', 'architecture', 'skeletal_reinforcement'] },
	# ecospace
	{ basis => 'environment_basis_no', 
	  fields => ['taxon_environment'] },
	{ basis => 'motility_basis_no', 
	  fields => ['motility'] },
	{ basis => 'vision_basis_no',
	  fields => ['vision'] },
	{ basis => 'life_habit_basis_no', 
	  fields => ['life_habit'] },
	{ basis => 'diet_basis_no', 
	  fields => ['diet'] },
	{ basis => 'reproduction_basis_no',
	  fields => ['reproduction'] },
	{ basis => 'ontogeny_basis_no',
	  fields => ['ontogeny'] },
);

# 	{ output => 'composition', field => 'composition1', field2 => 'composition2',
# 	  com_name => 'jsk', doc => "Skeletal composition of fossils from this taxon" },
# 	{ output => 'life_environment', field => 'taxon_environment',
# 	  com_name => 'jen', name => 'life_environment',
# 	  doc => "General environment in which this taxon lives or lived" },
# 	{ output => 'feeding_mode', field => 'diet1', field2 => 'diet2',
# 	  com_name => 'jfm', doc => "Diet or feeding mode of this taxon" },
# );
