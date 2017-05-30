# 
# Paleobiology Data Service version 1.2 - data entry
# 
# This file defines the data entry operations for version 1.2 of the Paleobiology Data Service.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>

use strict;
use feature 'unicode_strings';

package PBEntry;

use PB2::TimescaleEntry;


sub initialize {

    my ($ds2) = @_;
    
    $ds2->define_node({ path => 'entry',
			title => 'Data Entry' });
    
    $ds2->define_node({ path => 'entry/timescales',
			title => 'Timescales and interval boundaries' });
    
    $ds2->define_node({ path => 'bounds/update',
			place => 0,
			allow_method => 'GET,PUT',
			title => 'Update or entry of timescale bounds',
			role => 'PB2::TimescaleEntry',
			method => 'update_bounds',
			output => '1.2:timescales:bound',
			optional_output => '1.2:timescales:optional_bound' },
	"This operation allows you to update existing timescale bounds or enter",
	"new ones. If you wish to enter new records, you must include a special",
	"parameter to indicate this. By default, this operation returns the complete",
	"list of bounds for the updated timescale(s) after the operation is complete.",
	"This allows an editing application to keep its display in synchrony with",
	"the state of each timescale in the database.");
    
    $ds2->list_node({ path => 'bounds/update',
		      list => 'entry/timescales',
		      place => 1 });
    
    $ds2->define_node({ path => 'bounds/add',
		        place => 0,
			allow_method => 'GET,PUT',
		        title => 'Entry or update of timescale bounds',
		        role => 'PB2::TimescaleEntry',
			method => 'update_bounds',
		        arg => 'add',
		        output => '1.2:timescales:bound',
		        optional_output => '1.2:timescales:optional_bound' },
	"This operation allows you to enter new timescale bounds or update",
	"existing ones. By default, it returns the complete list of bounds for",
	"the updated timescale(s) after the operation is complete. This",
	"allows an editing application to keep its display in synchrony with",
	"the state of each timescale in the database.");
    
    $ds2->list_node({ path => 'bounds/add',
		      list => 'entry/timescales',
		      place => 1 });
    
    $ds2->list_node({ path => 'entry/timescales',
		      list => 'entry',
		      place => 1 },
	"Entry operations for timescales and timescale bounds.");
    
}


1;
