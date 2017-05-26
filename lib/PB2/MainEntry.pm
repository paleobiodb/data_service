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
    
    $ds2->define_node({ path => 'bounds/update',
			place => 0,
			allow_method => 'GET,PUT',
			title => 'Entry or update of timescale bounds',
			role => 'PB2::TimescaleEntry',
			method => 'update_bounds',
			output => '1.2:timescales:bound',
			optional_output => '1.2:timescales:optional_bound' },
	"This operation allows you to enter new timescale bounds or update",
	"existing ones. By default, it returns the complete list of bounds for",
	"the specified timescale(s), including all additions and updates. This",
	"allows an editing application to update its display in synchrony with",
	"the status of this timescale in the database.");
    
    $ds2->list_node({ path => 'bounds/update',
		      list => 'timescale_entry',
		      place => 1 });
    
    
    
}


1;
