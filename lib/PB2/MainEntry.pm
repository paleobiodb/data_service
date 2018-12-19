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
use PB2::SpecimenEntry;
use PB2::TimescaleEntry;
use PB2::ResourceEntry;


sub initialize {

    my ($ds2) = @_;
    
    $ds2->define_node({ path => 'entry',
			title => 'Data Entry' });

    # Timescales and Interval bounds
    
    $ds2->define_node({ path => 'entry/timescales',
    			title => 'Timescales and interval boundaries' });
    
    $ds2->list_node({ path => 'entry/timescales',
    		      list => 'entry',
    		      place => 1 },
    	"Data entry operations for timescales and timescale bounds.");
    
    $ds2->define_node({ path => 'timescales/addupdate',
    			title => 'Add timescales or update existing timescales',
    			place => 0,
    			allow_method => 'PUT,POST',
    			role => 'PB2::TimescaleEntry',
    			method => 'update_records',
    			output => '1.2:timescales:basic',
    			optional_output => '1.2:timescales:optional_basic' },
    	"This operation allows you to add new timescales to the database and/or",
    	"update the attributes of existing timescales.");
    
    $ds2->list_node({ path => 'timescales/addupdate',
    		      list => 'entry/timescales',
    		      place => 1 });
    
    $ds2->extended_doc({ path => 'timescales/addupdate' },
    	"This operation can be used to add new timescale or bound records to the database,",
	"or to delete or update the attributes of existing records. The new or updated values",
	"must be supplied in the",
	"request body in JSON format, using the HTTP C<B<PUT>> method. Any supplied record which",
	"contains the field 'bound_id' will be interpreted to refer to an existing bound record.",
	"Any record that contains 'bound_type' but not 'bound_id' will be interpreted as a new",
	"boundary record. Any record that contains neither of these but does contain 'timescale_id'",
	"will be interpreted to refer to an existing timescale record. Any record that contains none",
	"of these but does contain 'timescale_name' will be interpreted as a new timescale record.",
	"All other records will generate an error.",
	">To delete an existing table row, include a record that specifies a timescale or bound",
	"identifier and also the field C<B<_operation>> with the value C<B<delete>>.",
     	"If there are bounds in other timescales that depend on the",
    	"timescales(s) or bound(s) to be deleted, then the operation will be blocked unless you also",
	"include the parameter C<B<allow=BREAK_DEPENDENCIES>>.",
   	">By default, this operation returns a list of new or updated record(s).");
    
    $ds2->define_node({ path => 'timescales/delete',
    			title => 'Delete timescales',
    			place => 0,
    			allow_method => 'GET,PUT,DELETE',
    			role => 'PB2::TimescaleEntry',
    			method => 'delete_records',
			arg => 'timescales',
    		        output => '1.2:timescales:basic' },
    	"This operation allows you to delete one or more existing timescales.");
    
    $ds2->list_node({ path => 'timescales/delete',
    		      list => 'entry/timescales',
    		      place => 1 });
    
    $ds2->extended_doc({ path => 'timescales/delete' },
    	"You may specify the timescale(s) to be deleted either using the request parameter",
	"B<C<timescale_id>> or else by including a request body in JSON format. The HTTP",
	"methods should be C<B<GET>> or C<B<DELETE>> in the former case, C<B<PUT>> in the latter.",
	"The request body should either be a list of timescale identifiers separated by commas",
	"or else a list of records that each specifies a timescale identifier.",
    	">If there are interval boundaries in other timescales that depend on the",
    	"timescale(s) to be deleted, then the operation will be blocked unless you also",
	"include the parameter C<B<allow=BREAK_DEPENDENCIES>>.",
    	">Nothing will be returned except a result code indicating success or failure,",
    	"plus any errors or warnings that were generated.");
    
    # $ds2->define_node({ path => 'bounds/addupdate',
    # 		        title => 'Add interval boundaries or update existing boundaries',
    # 		        place => 0,
    # 			allow_method => 'GET,PUT,POST',
    # 		        role => 'PB2::TimescaleEntry',
    # 			method => 'update_bounds',
    # 		        arg => 'add',
    # 		        output => '1.2:timescales:bound',
    # 		        optional_output => '1.2:timescales:optional_bound' },
    # 	"This operation allows you to add new interval boundaries to the database",
    # 	"and/or update the attributes of existing interval boundaries.");

    # $ds2->list_node({ path => 'bounds/addupdate',
    # 		      list => 'entry/timescales',
    # 		      place => 2 });
    
    # $ds2->extended_doc({ path => 'bounds/addupdate' },
    # 	"This operation can be used to add new interval bounds to the database or to",
    # 	"delete or update the attributes of existing bounds. The new or updated values",
    # 	"must be supplied in the request body in JSON format, using the HTTP C<B<PUT>> method.",
    # 	"You can add, update, or delete bounds in any timescale that you have permission to modify.",
    # 	"Any record that specifies a bound identifier will update the corresponding table row,",
    # 	"provided it exists. Any record which",
    # 	"does not will cause a new row to be added to the interval bounds table. Such",
    # 	"records must specify an existing timescale into which the new bound will be added,",
    # 	"or else an error will be returned.",
    # 	">To delete an existing table row, you can include a record that specifies a bound",
    # 	"identifier and also the field C<B<_operation>> with the value C<B<delete>>.",
    # 	"You may also include the parameter C<B<replace>>, which specifies that the interval",
    # 	"bounds in the timescale(s) whose identifiers are found in the request body will be",
    # 	"completely replaced by the bounds listed in the request body. Any existing bound that",
    # 	"does not appear as an update record will be deleted.",
    # 	"If there are interval boundaries in other timescales that depend on the",
    # 	"bound(s) to be deleted, then the operation will be blocked unless you also",
    # 	"include the parameter C<B<allow=BREAK_DEPENDENCIES>>.",
    # 	">By default, this operation returns the full list of bounds for every updated timescale",
    # 	"after the operation is carried out.");
    
    $ds2->define_node({ path => 'bounds/delete',
    			title => 'Delete interval bounds',
    			place => 0,
    			allow_method => 'GET,PUT,DELETE',
    			role => 'PB2::TimescaleEntry',
    			method => 'delete_records',
			arg => 'bounds' },
    	"This operation allows you to delete one or more existing interval bounds.");
    
    $ds2->list_node({ path => 'bounds/delete',
    		      list => 'entry/timescales',
    		      place => 2 });
    
    $ds2->extended_doc({ path => 'bounds/delete' },
    	"You may specify the bounds(s) to be deleted either using the request parameter",
	"B<C<bound_id>> or else by including a request body in JSON format. The HTTP",
	"methods should be C<B<GET>> in the former case, C<B<PUT>> in the latter. The",
	"request body should either be a list of bound identifiers separated by commas",
	"or else a list of records that each specifies a bound identifier.",
    	">If there are interval boundaries in the same or other timescales that depend on the",
    	"bounds(s) to be deleted, then the operation will be blocked unless you also",
	"include the parameter C<B<allow=BREAK_DEPENDENCIES>>.",
    	">Nothing will be returned except a result code indicating success or failure,",
    	"plus any errors or warnings that were generated.");

    # $ds2->define_node({ path => 'tsi/delete',
    # 			title => 'Delete intervals',
    # 			place => 0,
    # 			allow_method => 'GET,PUT,DELETE',
    # 			role => 'PB2::TimescaleEntry',
    # 			method => 'delete_records',
    # 			arg => 'intervals' },
    # 	"This operation allows you to delete one or more existing intervals.");
    
    # $ds2->extended_doc({ path => 'tsi/delete' },
    # 	"You may specify the intervals(s) to be deleted either using the request parameter",
    # 	"B<C<bound_id>> or else by including a request body in JSON format. The HTTP",
    # 	"methods should be C<B<GET>> in the former case, C<B<PUT>> in the latter. The",
    # 	"request body should either be a list of interval identifiers separated by commas",
    # 	"or else a list of records that each specifies a bound identifier.",
    # 	">If there are interval boundaries in the same or other timescales that depend on the",
    # 	"bounds(s) to be deleted, then the operation will be blocked unless you also",
    # 	"include the parameter C<B<allow=BREAK_DEPENDENCIES>>.",
    # 	">Nothing will be returned except a result code indicating success or failure,",
    # 	"plus any errors or warnings that were generated.");
    
    # Educational Resources
    
    $ds2->define_node({ path => 'entry/eduresources',
			title => 'Educational Resources' });
    
    $ds2->list_node({ path => 'entry/eduresources',
		      list => 'entry',
		      place => 2 },
	"Data entry operations for educational resource records.");
    
    $ds2->define_node({ path => 'eduresources/addupdate',
			title => 'Add educational resources or update existing records',
			place => 0,
			allow_method => 'GET,PUT,POST',
			doc_template => 'entry_operation.tt',
			role => 'PB2::ResourceEntry',
			method => 'update_resources',
			arg => 'add',
			output => '1.2:eduresources:basic',
			optional_output => '1.2:eduresources:optional_output' },
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
			allow_method => 'GET,PUT,POST',
			doc_template => 'entry_operation.tt',
			role => 'PB2::ResourceEntry',
			method => 'update_resources',
			arg => 'update',
			output => '1.2:eduresources:basic',
			optional_output => '1.2:eduresources:optional_output' },
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
			allow_method => 'GET,PUT,POST,DELETE',
			role => 'PB2::ResourceEntry',
			method => 'delete_resources',
			output => '1.2:eduresources:basic'},
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
    
    $ds2->define_node({ path => 'entry/specs',
			title => 'Specimens and Measurements' });
    
    $ds2->list_node({ path => 'entry/specs',
		      list => 'entry',
		      place => 2 },
	"Data entry operations for specimen and measurement records.");
    
    $ds2->define_node({ path => 'specs/addupdate',
			title => 'Add specimen records or update existing records',
			place => 1,
			allow_method => 'GET,PUT,POST',
			doc_template => 'entry_operation.tt',
			role => 'PB2::SpecimenEntry',
			method => 'update_specimens',
			arg => 'add',
			# before_record_hook => 'my_select_output_block',
			output => '1.2:specs:basic',
			optional_output => '1.2:specs:basic_map' },
	"This operation allows you to add new specimen and measurement records",
	"to the database and/or update the attributes of existing records.");
    
    $ds2->list_node({ path => 'specs/addupdate',
		      list => 'entry/specs',
		      place => 1 });
    
    $ds2->extended_doc({ path => 'specs/addupdate' },
	"This operation allows you to add new specimen and measurement records",
	"to the database and/or update the attributes of existing records.",
	"The records to be added and/or updated should appear in the request body,",
	"in JSON format, as a list of objects.",
	">Any record which includes either C<specimen_id> (and not C<measurement_type>) or",
	"C<measurement_id> will update the specified attributes of the specified",
	"record, provided it exists and you have permission to do so. Any attributes",
	"not included in the record will be left unchanged. Any record",
	"that contains C<measurement_type> but not C<measurement_id> will be added",
	"to the database as a new row in the B<measurements> table. Any record that",
	"contains C<specimen_code> but not C<specimen_id> will be added to the",
	"database as a new row in the B<specimens> table. Any other record will",
	"generate an error. The new records will be owned by you.",
        ">By default, this operation returns the new or updated record(s).");
    
    $ds2->define_node({ path => 'specs/update',
			title => 'Update existing specimen records',
			place => 1,
			allow_method => 'GET,PUT,POST',
			doc_template => 'entry_operation.tt',
			role => 'PB2::SpecimenEntry',
			method => 'update_specimens',
			# before_record_hook => 'my_select_output_block',
			output => '1.2:specs:basic',
			optional_output => '1.2:specs:basic_map' },
	"This operation allows you to update existing specimen and measurement records");
    
    $ds2->list_node({ path => 'specs/update',
		      list => 'entry/specs',
		      place => 1 });
    
    $ds2->extended_doc({ path => 'specs/update' },
	"This operation allows you to update specimen and measurement records",
	"in the database. The records to be updated should appear in the request body,",
	"in JSON format, as a list of objects.",
	">Any record which includes C<measurement_id> will update the specified attributes",
	"of the specified measurement record, provided it exists and you have permission to do so.",
	"Any record which includes C<specimen_id> but not C<measurement_id> will",
	"update the specified attributes of the specified specimen record, provided it",
	"exists and you have permission to do so. Any attributes not included in the",
	"record will be left unchanged.",
        ">By default, this operation returns the updated record(s).");
    
    $ds2->define_node({ path => 'specs/delete',
			title => 'Delete specimen records',
			place => 0,
			allow_method => 'GET,PUT,POST',
			doc_template => 'entry_operation.tt',
			role => 'PB2::SpecimenEntry',
			method => 'delete_specimens',
			output => '1.2:specs:deleted' },
	"This operation allows you to delete specimen records from the database."); 
    
    $ds2->list_node({ path =>'specs/delete',
		      list => 'entry/specs',
		      place => 2 });
    
    $ds2->define_node({ path => 'specs/addupdate_measurements',
			title => 'Add measurement records or update existing records',
			place => 2,
			allow_method => 'GET,PUT,POST',
			doc_template => 'entry_operation.tt',
			role => 'PB2::SpecimenEntry',
			method => 'update_measurements',
			arg => 'add',
			output => '1.2:measure:basic' },
	"This operation allows you to add new specimen measurement records to the database and/or",
	"update the attributes of existing records.");
    
    $ds2->list_node({ path => 'specs/addupdate_measurements',
		      list => 'entry/specs',
		      place => 2 });
    
    $ds2->extended_doc({ path => 'specs/addupdate_measurements' },
	"You may provide the necessary parameters in the URL (with method C<B<GET>>)",
	"or in the request body in JSON format (with method C<B<PUT>>). With the latter,",
	"you may specify multiple records. Any records which specify an eduresource identifier",
	"will update the attributes of that record if you have permission to do so.",
	"Otherwise, a new record will be created, owned by you.",
	">By default, this operation returns the new or updated record(s).");
}


1;
