# 
# The Paleobiology Database
# 
#   TimescaleDefs.pm
# 
# Definitions and properties necessary for working with the Timescale tables.
# 

package TimescaleDefs;

use strict;

use Carp qw(croak);

use TableDefs qw(%TABLE $TEST_DB %FOREIGN_KEY_COL set_table_group set_table_property set_column_property
		 set_table_name change_table_db restore_table_name);


# At runtime, register table names and table properties.

{
    set_table_name(TIMESCALE_DATA => 'timescales');
    set_table_name(TIMESCALE_BOUNDS => 'timescale_bounds');
    set_table_name(TIMESCALE_INTS => 'timescale_ints');

    set_table_group(timescale_data => 'TIMESCALE_DATA', 'TIMESCALE_BOUNDS', 'TIMESCALE_INTS');

    set_table_name(MACROSTRAT_LITHS => 'macrostrat.liths');
    set_table_name(MACROSTRAT_INTERVALS => 'macrostrat.intervals');
    set_table_name(MACROSTRAT_SCALES => 'macrostrat.timescales');
    set_table_name(MACROSTRAT_SCALES_INTS => 'macrostrat.timescales_intervals');

    set_table_group(macrostrat => 'MACROSTRAT_LITHS', 'MACROSTRAT_INTERVALS',
		    'MACROSTRAT_SCALES', 'MACROSTRAT_SCALES_INTS');
        
    set_table_property('TIMESCALE_DATA', CAN_POST => 'AUTHORIZED');
    set_table_property('TIMESCALE_DATA', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('TIMESCALE_DATA', PRIMARY_KEY => "timescale_no");
    set_table_property('TIMESCALE_DATA', CASCADE_DELETE => ['TIMESCALE_BOUNDS']);
    
    set_column_property('TIMESCALE_DATA', 'timescale_name', REQUIRED => 1);
    # set_column_property('TIMESCALE_DATA', 'priority', ADMIN_SET => 1);
    set_column_property('TIMESCALE_DATA', 'has_error', ADMIN_SET => 1);
    set_column_property('TIMESCALE_DATA', 'min_age', IGNORE => 1);
    set_column_property('TIMESCALE_DATA', 'max_age', IGNORE => 1);
    
    set_table_property('TIMESCALE_BOUNDS', CAN_POST => 'AUTHORIZED');
    set_table_property('TIMESCALE_BOUNDS', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('TIMESCALE_BOUNDS', PRIMARY_KEY => "bound_no");
    set_table_property('TIMESCALE_BOUNDS', SUPERIOR_TABLE => 'TIMESCALE_DATA');
    
    set_column_property('TIMESCALE_BOUNDS', 'base_no', EXTID_TYPE => 'BND');
    set_column_property('TIMESCALE_BOUNDS', 'base_no', FOREIGN_KEY => 'TIMESCALE_BOUNDS');
    set_column_property('TIMESCALE_BOUNDS', 'range_no', EXTID_TYPE => 'BND');
    set_column_property('TIMESCALE_BOUNDS', 'range_no', FOREIGN_KEY => 'TIMESCALE_BOUNDS');
    set_column_property('TIMESCALE_BOUNDS', 'top_no', EXTID_TYPE => 'BND');
    set_column_property('TIMESCALE_BOUNDS', 'top_no', FOREIGN_KEY => 'TIMESCALE_BOUNDS');
    set_column_property('TIMESCALE_BOUNDS', 'color_no', EXTID_TYPE => 'BND');
    set_column_property('TIMESCALE_BOUNDS', 'color_no', FOREIGN_KEY => 'TIMESCALE_BOUNDS');
    
    set_table_property('TIMESCALE_INTS', CAN_MODIFY => 'ADMIN');
    set_table_property('TIMESCALE_INTS', PRIMARY_KEY => "interval_no");
    set_table_property('TIMESCALE_INTS', SUPERIOR_TABLE => 'TIMESCALE_DATA');
}




1;
