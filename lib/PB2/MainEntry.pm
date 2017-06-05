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
    
    $ds2->define_node({ path => 'timescales/addupdate',
			title => 'Add timescales or update existing timescales',
			place => 0,
			allow_method => 'GET,PUT',
			role => 'PB2::TimescaleEntry',
			method => 'update_timescales',
			arg => 'add',
			output => '1.2:timescales:basic',
			optional_output => '1.2:timescales:optional_basic' },
	"This operation allows you to add new timescales to the database and/or",
	"update the attributes of existing timescales.");
    
    $ds2->list_node({ path => 'timescales/addupdate',
		      list => 'entry/timescales',
		      place => 1 });
    
    $ds2->extended_doc({ path => 'timescales/addupdate' },
	"You may provide the necessary parameters in the URL (with method C<B<GET>>)",
	"or in the request body in JSON format (with method C<B<PUT>>). With the latter,",
	"you may specify multiple records. Any records which specify a timescale identifier",
	"will update the attributes of that timescale if you have permission to do so.",
	"Otherwise, a new timescale will be created, owned by you.",
	">By default, this operation returns the new or updated timescale record(s).");
    
    $ds2->define_node({ path => 'timescales/update',
			title => 'Update existing timescales',
			place => 0,
			allow_method => 'GET,PUT',
			role => 'PB2::TimescaleEntry',
			method => 'update_timescales',
			arg => 'update',
			output => '1.2:timescales:basic',
			optional_output => '1.2:timescales:optional_basic' },
	"This operation allows you to update the attributes of existing timescales.");
    
    $ds2->list_node({ path => 'timescales/update',
		      list => 'entry/timescales',
		      place => 1 });
    
    $ds2->extended_doc({ path => 'timescales/update' },
	"You may provide the necessary parameters in the URL (with method C<B<GET>>)",
	"or in the request body in JSON format (with method C<B<PUT>>). With the latter,",
	"you may specify multiple records. All records must specify a timescale identifier,",
	"and will update the attributes of the timescale if you have permission to do so.",
	">By default, this operation returns the updated timescale record(s).");
    
    $ds2->define_node({ path => 'timescales/delete',
			title => 'Delete timescales',
			place => 0,
			allow_method => 'GET,PUT,DELETE',
			role => 'PB2::TimescaleEntry',
			method => 'delete_timescales' },
	"This operation allows you to delete one or more existing timescales.");
    
    $ds2->list_node({ path => 'timescales/delete',
		      list => 'entry/timescales',
		      place => 1 });
    
    $ds2->extended_doc({ path => 'timescales/delete' },
	"You may provide the necessary parameters in the URL (with method C<B<GET>> or C<B<DELETE>>)",
	"or in the request body in JSON format (with method C<B<PUT>>). With the latter,",
	"you may specify multiple records. All records must specify a timescale identifier,",
	"and will delete the specified timescale if you have permission to do so.",
	">If there are interval boundaries in other timescales that depend on the",
	"timescale(s) to be deleted, then the operation will be blocked unless the",
	"parameter C<B<allow=BREAK_DEPENDENCIES>> is included.",
	">Nothing will be returned except a result code indicating success or failure,",
	"plus any errors or warnings that were generated.");
    
    $ds2->define_node({ path => 'bounds/addupdate',
		        title => 'Add interval boundaries or update existing boundaries',
		        place => 0,
			allow_method => 'GET,PUT',
		        role => 'PB2::TimescaleEntry',
			method => 'update_bounds',
		        arg => 'add',
		        output => '1.2:timescales:bound',
		        optional_output => '1.2:timescales:optional_bound' },
	"This operation allows you to add new interval boundaries to the database",
	"and/or update the attributes of existing interval boundaries.");

    $ds2->list_node({ path => 'bounds/addupdate',
		      list => 'entry/timescales',
		      place => 2 });
    
    $ds2->extended_doc({ path => 'bounds/addupdate' },
	"You may provide the necessary parameters in the URL (with method C<B<GET>>)",
	"or in the request body in JSON format (with method C<B<PUT>>). With the latter,",
	"you may specify multiple records. Any records which specify a boundary identifier",
	"will update the attributes of that boundary. Otherwise, a new boundary will be",
	"created. You must have permission to edit the specified timescale.",
	">By default, this operation returns the complete list",
	"of boundaries for the updated timescale(s) after the operation is complete. This",
	"allows an editing application to keep its display in synchrony with",
	"the state of each timescale in the database.");
    
    $ds2->define_node({ path => 'bounds/update',
			title => 'Update existing interval boundaries',
			place => 0,
			allow_method => 'GET,PUT',
			role => 'PB2::TimescaleEntry',
			method => 'update_bounds',
			output => '1.2:timescales:bound',
			optional_output => '1.2:timescales:optional_bound' },
	"This operation allows you to update the attributes of existing interval boundaries.");
    
    $ds2->list_node({ path => 'bounds/update',
		      list => 'entry/timescales',
		      place => 2 });
    
    $ds2->extended_doc({ path => 'bounds/update' },
	"You may provide the necessary parameters in the URL (with method C<B<GET>>)",
	"or in the request body in JSON format (with method C<B<PUT>>). With the latter,",
	"you may specify multiple records. All records must specify a boundary identifier,",
	"and will update the attributes of the specified boundary. You must have permission",
	"to edit the timescale.",
	">By default, this operation returns the complete",
	"list of boundaries for the updated timescale(s) after the operation is complete.",
	"This allows an editing application to keep its display in synchrony with",
	"the state of each timescale in the database.");
        
    $ds2->define_node({ path => 'bounds/delete',
			title => 'Delete interval bounds',
			place => 0,
			allow_method => 'GET,PUT,DELETE',
			role => 'PB2::TimescaleEntry',
			method => 'delete_bounds' },
	"This operation allows you to delete one or more existing interval bounds.");
    
    $ds2->list_node({ path => 'bounds/delete',
		      list => 'entry/timescales',
		      place => 2 });
    
    $ds2->extended_doc({ path => 'bounds/delete' },
	"You may provide the necessary parameters in the URL (with method C<B<GET>> or C<B<DELETE>>)",
	"or in the request body in JSON format (with method C<B<PUT>>). With the latter,",
	"you may specify multiple records. All records must specify a boundary identifier,",
	"and will delete the specified boundary. You must have permission to edit the timescale.",
	">If there are other interval boundaries that depend on the",
	"boundarie(s) to be deleted, then the operation will be blocked unless the",
	"parameter C<B<allow=BREAK_DEPENDENCIES>> is included.",
	">Nothing will be returned except a result code indicating success or failure,",
	"plus any errors or warnings that were generated.");
    
    $ds2->list_node({ path => 'entry/timescales',
		      list => 'entry',
		      place => 1 },
	"Data entry operations for timescales and timescale bounds.");
    
}


1;
