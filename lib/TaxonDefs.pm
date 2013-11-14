# 
# The Paleobiology Database
# 
#   TaxonDefs.pm
# 

package TaxonDefs;

use strict;

use base 'Exporter';

our (@EXPORT_OK) = qw(@TREE_TABLE_LIST %TAXON_TABLE $CLASSIC_TREE_CACHE $CLASSIC_LIST_CACHE);

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

