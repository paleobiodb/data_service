# 
# The Paleobiology Database
# 
#   TaxonDefs.pm
# 

package TaxonDefs;

use strict;

use base 'Exporter';

our (@EXPORT_OK) = qw(@TREE_TABLE_LIST %TAXON_TABLE %TAXON_RANK %RANK_STRING
		      %AUTH_TABLE %OPINION_TABLE %OPINION_CACHE %REFS_TABLE
		      %ATTRS_TABLE %INTS_TABLE %SEARCH_TABLE
		      %TAXONOMIC_STATUS %NOMENCLATURAL_STATUS
		      $CLASSIC_TREE_CACHE $CLASSIC_LIST_CACHE);

our (@TREE_TABLE_LIST) = ('taxon_trees');

our (%TAXON_TABLE);

$TAXON_TABLE{taxon_trees} = {
    names => 'taxon_names',
    attrs => 'taxon_attrs',
    search => 'taxon_search',
    ints => 'taxon_ints',
    lower => 'taxon_lower',
    counts => 'taxon_counts',
    images => 'taxon_pics',
    
    authorities => 'authorities',
    opinions => 'opinions',
    opcache => 'order_opinions',
    refs => 'refs'
 };

our $CLASSIC_TREE_CACHE = "taxa_tree_cache";
our $CLASSIC_LIST_CACHE = "taxa_list_cache";


our (%TAXON_RANK) = ( 'max' => 26, 26 => 26, 'informal' => 26, 'unranked_clade' => 25, 'unranked' => 25, 25 => 25,
		      'kingdom' => 23, 23 => 23, 'subkingdom' => 22, 22 => 22,
		      'superphylum' => 21, 21 => 21, 'phylum' => 20, 20 => 20, 'subphylum' => 19, 19 => 19,
		      'superclass' => 18, 18 => 18, 'class' => 17, 17 => 17, 'subclass' => 16, 16 => 16,
		      'infraclass' => 15, 15 => 15, 'superorder' => 14, 14 => 14, 'order' => 13, 13 => 13,
		      'suborder' => 12, 12 => 12, 'infraorder' => 11, 11 => 11, 'superfamily' => 10, 10 => 10,
		      'family' => 9, 9 => 9, 'subfamily' => 8, 8 => 8, 'tribe' => 7, 7 => 7, 'subtribe' => 6, 6 => 6,
		      'genus' => 5, 5 => 5, 'subgenus' => 4, 4 => 4, 'species' => 3, 3 => 3, 'subspecies' => 2, 2 => 2, 'min' => 2 );

our (%RANK_STRING) = ( 26 => 'informal', 25 => 'unranked clade', 23 => 'kingdom',
		       22 => 'subkingdom', 21 => 'superphylum', 20 => 'phylum', 19 => 'subphylum',
		       18 => 'superclass', 17 => 'class', 16 => 'subclass', 15 => 'infraclass',
		       14 => 'superorder', 13 => 'order', 12 => 'suborder', 11 => 'infraorder',
		       10 => 'superfamily', 9 => 'family', 8 => 'subfamily', 7 => 'tribe', 
		       6 => 'subtribe', 5 => 'genus', 4 => 'subgenus', 3 => 'species', 2 => 'subspecies');

our (%NAME_TABLE) = ("taxon_trees" => "taxon_names");
our (%ATTRS_TABLE) = ("taxon_trees" => "taxon_attrs");
our (%SEARCH_TABLE) = ("taxon_trees" => "taxon_search");
our (%INTS_TABLE) = ("taxon_trees" => "taxon_ints");

our (%AUTH_TABLE) = ("taxon_trees" => "authorities");
our (%OPINION_TABLE) = ("taxon_trees" => "opinions");
our (%OPINION_CACHE) = ("taxon_trees" => "order_opinions");
our (%REFS_TABLE) = ("taxon_trees" => "refs");


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


