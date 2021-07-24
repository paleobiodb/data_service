# 
# The Paleobiology Database
# 
#   DiversityDefs.pm
# 
# Definitions and properties necessary for working with the taxonomic diversity tables.
# 

package DiversityTables;

use strict;

use TableDefs qw(set_table_name set_table_group set_table_property set_column_property);

# At runtime, set table and column properties.

{
    set_table_name(DIVERSITY_MATRIX => 'div_matrix');
    set_table_name(DIVERSITY_GLOBAL_MATRIX => 'div_global');
    
    set_table_name(PREVALENCE_MATRIX => 'pvl_matrix');
    set_table_name(PREVALENCE_GLOBAL_MATRIX => 'pvl_global');
    
    set_table_name(DIVERSITY_STATS => 'div_total');
    
    set_table_group('diversity_tables' => 'DIVERSITY_MATRIX', 'DIVERSITY_GLOBAL_MATRIX',
		    'PREVALENCE_MATRIX', 'PREVALENCE_GLOBAL_MATRIX', 'DIVERSITY_STATS');
    
    set_table_name(OCCURRENCE_MAJOR_MAP => 'occ_major_map');
    set_table_name(COLLECTION_INTERVALS => 'coll_ints');
}


