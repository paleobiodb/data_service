# 
# The Paleobiology Database
# 
#   TaxonDefs.pm
# 

package TaxonDefs;

use strict;

use base 'Exporter';

our (@EXPORT_OK) = qw(@TREE_TABLE_LIST %TAXON_TABLE %TAXON_RANK $CLASSIC_TREE_CACHE $CLASSIC_LIST_CACHE);

our (@TREE_TABLE_LIST) = ('taxon_trees');

our (%TAXON_TABLE);

$TAXON_TABLE{taxon_trees} = {
    names => 'taxon_names',
    attrs => 'taxon_attrs',
    search => 'taxon_search',
    ints => 'taxon_ints',
    counts => 'taxon_counts',
    
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
