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

use TableDefs qw(%TABLE $TEST_DB set_table_group set_table_property set_column_property
		 set_table_name change_table_db restore_table_name);


my (%TIMESCALE_TABLES) = ( TIMESCALE_DATA => 'timescales',
			   TIMESCALE_REFS => 'timescale_refs',
			   TIMESCALE_INTS => 'timescale_ints',
			   TIMESCALE_BOUNDS => 'timescale_bounds',
			   TIMESCALE_QUEUE => 'timescale_queue',
			   TIMESCALE_PERMS => 'timescale_perms' );

my (%MACROSTRAT_TABLES) = ( MACROSTRAT_LITHS => 'macrostrat.liths',
			    MACROSTRAT_INTERVALS => 'macrostrat.intervals',
			    MACROSTRAT_SCALES => 'macrostrat.timescales',
			    MACROSTRAT_SCALES_INTS => 'macrostrat.timescales_intervals' );


# At runtime, register table names and table properties.

{
    foreach my $t ( keys %TIMESCALE_TABLES )
    {
	set_table_name( $t => $TIMESCALE_TABLES{$t} );
    }

    foreach my $t ( keys %MACROSTRAT_TABLES )
    {
	set_table_name( $t => $MACROSTRAT_TABLES{$t} );
    }

    set_table_group(timescales => 'TIMESCALE_DATA', 'TIMESCALE_BOUNDS', 'TIMESCALE_REFS', 'TIMESCALE_INTS');
    
    set_table_property('TIMESCALE_DATA', CAN_POST => 'AUTHORIZED');
    set_table_property('TIMESCALE_DATA', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('TIMESCALE_DATA', ALLOW_DELETE => 1);
    set_table_property('TIMESCALE_DATA', PRIMARY_KEY => "timescale_no");
    set_table_property('TIMESCALE_DATA', CASCADE_DELETE => ['TIMESCALE_BOUNDS',
							    'TIMESCALE_INTS',
							    'TIMESCALE_REFS']);
    
    set_column_property('TIMESCALE_DATA', 'is_active', ADMIN_SET => 1);
    set_column_property('TIMESCALE_DATA', 'has_error', ADMIN_SET => 1);
    set_column_property('TIMESCALE_DATA', 'authority', ADMIN_SET => 1);
    
    set_table_property('TIMESCALE_BOUNDS', CAN_POST => 'AUTHORIZED');
    set_table_property('TIMESCALE_BOUNDS', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('TIMESCALE_BOUNDS', ALLOW_DELETE => 1);
    set_table_property('TIMESCALE_BOUNDS', PRIMARY_KEY => "bound_no");
    set_table_property('TIMESCALE_BOUNDS', SUPERIOR_TABLE => 'TIMESCALE_DATA');
    
    set_table_property('TIMESCALE_INTS', CAN_POST => 'AUTHORIZED');
    set_table_property('TIMESCALE_INTS', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('TIMESCALE_INTS', PRIMARY_KEY => "interval_no");
    set_table_property('TIMESCALE_INTS', SUPERIOR_TABLE => 'TIMESCALE_DATA');
    
    set_table_property('TIMESCALE_REFS', CAN_POST => 'AUTHORIZED');
    set_table_property('TIMESCALE_REFS', CAN_MODIFY => 'AUTHORIZED');
    set_table_property('TIMESCALE_REFS', ALLOW_DELETE => 1);
    set_table_property('TIMESCALE_REFS', SUPERIOR_TABLE => 'TIMESCALE_DATA');
    
    set_column_property('TIMESCALE_REFS', 'timescale_no', REQUIRED => 1);
    set_column_property('TIMESCALE_REFS', 'reference_no', REQUIRED => 1);
}


# enable_test_mode ( class, table, ds )
# 
# Change the global variables that hold the names of the eduresource tables over to the test
# database. If $ds is either 1 or a reference to a Web::DataService object with the debug flag
# set, then print out a debugging message.

sub enable_test_mode {
    
    my ($class, $table, $ds) = @_;
    
    croak "You must define 'test_db' in the configuration file" unless $TEST_DB;
    
    foreach my $t ( keys %TIMESCALE_TABLES )
    {
	change_table_db($t, $TEST_DB);
    }
    
    if ( $ds && $ds == 1 || ref $ds && $ds->debug )
    {
	$ds->debug_line("TEST MODE: enable 'eduresources'\n");
    }
    
    return 1;
}


sub disable_test_mode {

    my ($class, $table, $ds) = @_;
    
    foreach my $t ( keys %TIMESCALE_TABLES )
    {
	restore_table_name($t);
    }
    
    if ( $ds && $ds == 1 || ref $ds && $ds->debug )
    {
	$ds->debug_line("TEST MODE: disable 'eduresources'\n");
    }
    
    return 2;
}


1;
