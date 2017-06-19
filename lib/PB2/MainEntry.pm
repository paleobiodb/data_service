# 
# Paleobiology Data Service version 1.2 - data entry
# 
# This file defines the data entry operations for version 1.2 of the Paleobiology Data Service.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>

use strict;
use feature 'unicode_strings';

package PBEntry;

use PB2::CommonEntry;
use PB2::ResourceEntry;


sub initialize {

    my ($ds2) = @_;
    
    $ds2->define_node({ path => 'entry',
			title => 'Data Entry' });
    
    $ds2->define_node({ path => 'entry/eduresources',
			title => 'Educational Resources' });
    
    $ds2->define_node({ path => 'eduresources/addupdate',
			title => 'Add educational resources or update existing records',
			place => 0,
			allow_method => 'GET,PUT',
			role => 'PB2::ResourceEntry',
			method => 'update_resources',
			arg => 'add' },
	"This operation allows you to add new educational resource records to the database and/or",
	"update the attributes of existing records.");
    
    $ds2->list_node({ path => 'eduresources/addupdate',
		      list => 'entry/eduresources',
		      place => 1 });
    
    $ds2->extended_doc({ path => 'eduresources/addupdate' },
	"You may provide the necessary parameters in the URL (with method C<B<GET>>)",
	"or in the request body in JSON format (with method C<B<PUT>>). With the latter,",
	"you may specify multiple records. Any records which specify an eduresource identifier",
	"will update the attributes of that record if you have permission to do so.",
	"Otherwise, a new record will be created, owned by you.",
	">By default, this operation returns the new or updated record(s).");
    
    $ds2->define_node({ path => 'eduresources/update',
			title => 'Update existing educational resource records',
			place => 0,
			allow_method => 'GET,PUT',
			role => 'PB2::ResourceEntry',
			method => 'update_resources',
			arg => 'update' },
	"This operation allows you to update the attributes of existing educational resource records.");
    
    $ds2->list_node({ path => 'eduresources/update',
		      list => 'entry/eduresources',
		      place => 1 });
    
    $ds2->extended_doc({ path => 'eduresources/update' },
	"You may provide the necessary parameters in the URL (with method C<B<GET>>)",
	"or in the request body in JSON format (with method C<B<PUT>>). With the latter,",
	"you may specify multiple records. All records must specify an eduresource identifier,",
	"and will update the attributes of the record if you have permission to do so.",
	">By default, this operation returns the updated record(s).");
    
    $ds2->define_node({ path => 'eduresources/delete',
			title => 'Delete educational resources',
			place => 0,
			allow_method => 'GET,PUT,DELETE',
			role => 'PB2::ResourceEntry',
			method => 'delete_resources' },
	"This operation allows you to delete one or more existing educational resources.");
    
    $ds2->list_node({ path => 'eduresources/delete',
		      list => 'entry/eduresources',
		      place => 1 });
    
    $ds2->extended_doc({ path => 'eduresources/delete' },
	"You may provide the necessary parameters in the URL (with method C<B<GET>> or C<B<DELETE>>)",
	"or in the request body in JSON format (with method C<B<PUT>>). With the latter,",
	"you may specify multiple records. All records must specify an eduresource identifier,",
	"and will delete the specified record if you have permission to do so.",
	">Nothing will be returned except a result code indicating success or failure,",
	"plus any errors or warnings that were generated.");
    
}


1;
