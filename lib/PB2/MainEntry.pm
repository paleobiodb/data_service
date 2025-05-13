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
use PB2::ReferenceEntry;
use PB2::ReferenceAux;
use PB2::CollectionEntry;
use PB2::SpecimenEntry;
use PB2::ResourceEntry;
use PB2::PublicationEntry;
use PB2::ArchiveEntry;

sub initialize {

    my ($ds2) = @_;
    
    $ds2->define_node({ path => 'entry',
			title => 'Data Entry' });

    # Bibliographic references
    
    $ds2->define_node({ path => 'entry/refs',
			title => 'Bibliographic references' });
    
    $ds2->list_node({ path => 'entry/refs',
		      list => 'entry',
		      place => 1 },
		    "Data entry operations for bibliographic references");
    
    $ds2->define_node({ path => 'refs/matchlocal',
			title => 'Check for a matching bibliographic reference locally',
		        place => 2,
			allow_method => 'GET',
		        role => 'PB2::ReferenceAux',
			method => 'match_local' },
	"This operation takes a partial set of bibliographic reference attributes",
	"and returns any matching entries from the local database. A DOI alone is",
	"often enough, but good results can be gotten from one or two authors, a",
	"publication year, and some words from the title.");
    
    $ds2->define_node({ path => 'refs/matchext',
			title => 'Fetch reference data from known external sources',
			place => 2,
			allow_method => 'GET',
			role => 'PB2::ReferenceAux',
			method => 'match_external',
			optional_output => '1.2:refs:matchext_map' },
	"This operation takes a partial set of bibliographic reference attributes",
	"and returns matching entries from external sources such as Crossref and XDD.",
	"A DOI alone is often enough, but good results can be gotten from one or two",
	"authors, a publication year, and some words from the title.");
    
    $ds2->define_node({ path => 'refs/add',
			title => 'Add new bibliographic references',
			place => 10,
			allow_method => 'PUT,POST',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:refs:addupdate_body',
			role => 'PB2::ReferenceEntry',
			method => 'addupdate_refs',
			arg => 'insert',
			output => '1.2:refs:basic',
			optional_output => '1.2:refs:output_map' },
	"This operation allows you to add new bibliographic references to",
	"the database.");
    
    $ds2->define_node({ path => 'refs/add_sandbox',
			title => 'Sandbox for the refs/add operation',
			place => 10,
			allow_format => 'html',
			allow_method => 'GET',
			doc_template => 'sandbox_operation.tt',
			role => 'PB2::ReferenceEntry',
			method => 'addupdate_sandbox',
			arg => 'insert' },
	"This operation displays an HTML form which you can use to generate",
	"calls to the refs/add operation.");
    
    $ds2->define_node({ path => 'refs/update',
			title => 'Update existing bibliographic references',
			place => 10,
			allow_method => 'PUT,POST',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:refs:addupdate_body',
			role => 'PB2::ReferenceEntry',
			method => 'addupdate_refs',
			arg => 'update',
			output => '1.2:refs:basic',
			optional_output => '1.2:refs:output_map' },
	"This operation allows you to update existing bibliographic references in",
	"the database.");
    
    $ds2->define_node({ path => 'refs/update_sandbox',
			title => 'Sandbox for the refs/update operation',
			place => 10,
			allow_format => 'html',
			allow_method => 'GET',
			doc_template => 'sandbox_operation.tt',
			role => 'PB2::ReferenceEntry',
			method => 'addupdate_sandbox',
			arg => 'update' },
	"This operation displays an HTML form which you can use to generate",
	"calls to the refs/update operation.");
    
    $ds2->define_node({ path => 'refs/addupdate',
			title => 'Add bibliographic references or update existing references',
			place => 10,
			allow_method => 'PUT,POST',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:refs:addupdate_body',
			role => 'PB2::ReferenceEntry',
			method => 'addupdate_refs',
			arg => 'addupdate',
			output => '1.2:refs:basic',
			optional_output => '1.2:refs:output_map' },
	"This operation allows you to add new bibliographic references to the database",
	"and/or update existing references.");
    
    # $ds2->define_node({ path => 'refs/replace',
    # 			title => 'Replace existing bibliographic references',
    # 			place => 10,
    # 			allow_method => 'PUT,POST',
    # 			doc_template => 'entry_operation.tt',
    # 			ruleset => '1.2:refs:addupdate',
    # 			body_ruleset => '1.2:refs:addupdate_body',
    # 			role => 'PB2::ReferenceEntry',
    # 			method => 'addupdate_refs',
    # 			arg => 'replace',
    # 			output => '1.2:refs:basic',
    # 			optional_output => '1.2:refs:output_map' },
    # 	"This operation allows you to replace existing bibliographic reference records",
    # 	"in the database.");
    
    $ds2->define_node({ path => 'refs/delete',
			title => 'Delete existing bibliographic references',
			place => 10,
			allow_method => 'GET,DELETE',
			doc_template => 'entry_operation.tt',
			role => 'PB2::ReferenceEntry',
			method => 'delete_refs',
			output => '1.2:refs:basic' },
	"This operation allows you to delete existing bibliographic reference records,",
	"provided they are not referenced by any other database records. It works only",
	"on records created by yourself, unless you have administrative privilege.");
    
    $ds2->define_node({ path => 'refs/selected',
			title => 'Fetch the bibliographic reference selected for data entry',
			place => 11,
			allow_method => 'GET',
			doc_template => 'operation.tt',
			role => 'PB2::ReferenceAux',
			method => 'selected',
			output => '1.2:refs:basic',
			optional_output => '1.2:refs:output_map' },
	"This operation returns the bibliographic reference (if any) that is currently",
	"selected for your current session in the Classic environment. If no reference",
	"is selected, the result will contain no records. If you are not logged in,",
	"a 401 error will be returned.");
    
    $ds2->define_node({ path => 'refs/classic_select',
			title => 'Select a bibliographic reference for data entry',
			place => 11,
			allow_method => 'GET',
			doc_template => 'entry_operation.tt',
			role => 'PB2::ReferenceAux',
			method => 'classic_select',
			output => '1.2:refs:basic',
			optional_output => '1.2:refs:output_map' },
	"This operation allows you to select the specified reference for your current",
	"session in the Classic environment. After it is executed, the next time the",
	"current user visits the Classic environment, this reference will be the selected one.");
    
    # Fossil Collections

    $ds2->define_node({ path => 'colls/add',
			title => 'Add new collections',
			place => 10,
			allow_method => 'PUT,POST',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:colls:addupdate_body',
			role => 'PB2::CollectionEntry',
			method => 'addupdate_colls',
			arg => 'insert',
			output => '1.2:colls:basic',
			optional_output => '1.2:colls:basic_map' },
	"This operation allows you to add new fossil collections to the database.");
    
    $ds2->define_node({ path => 'colls/add_sandbox',
			title => 'Sandbox for the colls/add operation',
			place => 10,
			allow_format => 'html',
			allow_method => 'GET',
			doc_template => 'sandbox_operation.tt',
			role => 'PB2::CollectionEntry',
			method => 'addupdate_sandbox',
			arg => 'insert',
			body_ruleset => '1.2:colls:addupdate_body' },
	"This operation displays an HTML form which you can use to generate",
	"calls to the colls/add operation.");
    
    $ds2->define_node({ path => 'colls/update',
			title => 'Update existing collections',
			place => 10,
			allow_method => 'PUT,POST',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:colls:addupdate_body',
			role => 'PB2::CollectionEntry',
			method => 'addupdate_colls',
			arg => 'update',
			output => '1.2:colls:basic',
			optional_output => '1.2:colls:basic_map' },
	"This operation allows you to update fossil collections in the database.");
        
    $ds2->define_node({ path => 'colls/update_sandbox',
			title => 'Sandbox for the colls/update operation',
			place => 10,
			allow_format => 'html',
			allow_method => 'GET',
			doc_template => 'sandbox_operation.tt',
			role => 'PB2::CollectionEntry',
			method => 'addupdate_sandbox',
			arg => 'update',
			body_ruleset => '1.2:colls:addupdate_body' },
	"This operation displays an HTML form which you can use to generate",
	"calls to the colls/update operation.");
    
    # $ds2->define_node({ path => 'colls/update_occs',
    # 			title => 'Sandbox for the colls/update_occs operation.',
    # 			place => 10,
    # 			allow_method => 'PUT,POST',
    # 			doc_template => 'entry_operation.tt',
    # 			body_ruleset => '1.2:occs:addupdate_body',
    # 			method => 'update_occs',
    # 			output => '1.2:occs:edit',
    # 			optional_output => '1.2:occs:edit_map' },
    # 	"This operation allows you to add, update, and/or delete occurrences and",
    # 	"reidentifications associated with a specified collection.");
    
    $ds2->define_node({ path => 'colls/update_occs_sandbox',
			title => 'Sandbox for the colls/update_occs operation',
			place => 10,
			allow_format => 'html',
			allow_method => 'GET',
			doc_template => 'sandbox_operation.tt',
			role => 'PB2::CollectionEntry',
			method => 'update_occs_sandbox',
			body_ruleset => '1.2:occs:addupdate_body' },
	"This operation displays an HTML form which you can use to generate",
	"calls to the colls/update_occs operation.");
    
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
			usage => [ "eduresources/addupdate.json" ],
			allow_method => 'GET,PUT,POST',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:eduresources:addupdate_body',
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
			usage => [ "eduresources/update.json" ],
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
			usage => [ "eduresources/delete.json?id=edr:2" ],
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
    
    # Entry operations for publications.
    
    $ds2->define_node({ path => 'entry/pubs',
			title => 'Official publications' });
    
    $ds2->list_node({ path => 'entry/pubs',
		      list => 'entry',
		      place => 3 },
	"Data entry operations for official publication records.");
    
    $ds2->define_node({ path => 'pubs/addupdate',
			title => 'Add official publications or update existing records',
			place => 0,
			allow_method => 'PUT,POST',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:pubs:addupdate_body',
			role => 'PB2::PublicationEntry',
			allow_format => '+larkin',
			method => 'update_publications',
			output => '1.2:pubs:basic',
			optional_output => '1.2:pubs:optional_output' },
	"This operation allows you to add new official publication records to the database and/or",
	"update the attributes of existing records.");
    
    $ds2->list_node({ path => 'pubs/addupdate',
		      list => 'entry/pubs',
		      place => 1 });
    
    $ds2->extended_doc({ path => 'pubs/addupdate' },
	"You may provide the necessary parameters in the URL (with method C<B<GET>>)",
	"or in the request body in JSON format (with method C<B<PUT>>). With the latter,",
	"you may specify multiple records. Any records which specify a publication identifier",
	"will update the attributes of that record if you have permission to do so.",
	"Otherwise, a new record will be created, owned by you.",
	">By default, this operation returns the new or updated record(s).");
    
    $ds2->define_node({ path => 'pubs/delete',
			title => 'Delete official publications',
			place => 0,
			allow_method => 'GET,PUT,POST,DELETE',
			role => 'PB2::PublicationEntry',
			method => 'delete_publications',
			output => '1.2:pubs:basic'},
	"This operation allows you to delete one or more existing official publications.");
    
    $ds2->list_node({ path => 'pubs/delete',
		      list => 'entry/pubs',
		      place => 1 });
    
    $ds2->extended_doc({ path => 'pubs/delete' },
	"You may provide the necessary parameters in the URL (with method C<B<GET>> or C<B<DELETE>>)",
	"or in the request body in JSON format (with method C<B<PUT>> or C<B<POST>>). With the latter,",
	"you may specify multiple records. All records must specify a publication identifier,",
	"and will delete the specified record if you have permission to do so.",
	">Nothing will be returned except a result code indicating success or failure,",
	"plus any errors or warnings that were generated.");
    
    # Entry operations for data archives.
    
    $ds2->define_node({ path => 'entry/archives',
			title => 'Data archives' });
    
    $ds2->list_node({ path => 'entry/archives',
		      list => 'entry',
		      place => 3 },
	"Data entry operations for data archive records.");
    
    $ds2->define_node({ path => 'archives/addupdate',
			title => 'Add data archive records or update existing records',
			place => 0,
			allow_method => 'PUT,POST',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:archives:addupdate_body',
			role => 'PB2::ArchiveEntry',
			allow_format => '+larkin',
			method => 'update_archives',
			output => '1.2:archives:basic',
			optional_output => '1.2:archives:optional_output' },
	"This operation allows you to add new data archive records to the database and/or",
	"update the attributes of existing records.");
    
    $ds2->list_node({ path => 'archives/addupdate',
		      list => 'entry/archives',
		      place => 1 });
    
    $ds2->extended_doc({ path => 'archives/addupdate' },
	"You may provide the necessary parameters in the URL (with method C<B<GET>>)",
	"or in the request body in JSON format (with method C<B<PUT>>). With the latter,",
	"you may specify multiple records. Any records which specify a data archive record",
	"will update the attributes of that record if you have permission to do so.",
	"Otherwise, a new record will be created, owned by you.",
	">By default, this operation returns the new or updated record(s).");
    
    $ds2->define_node({ path => 'archives/delete',
			title => 'Delete data archives',
			place => 0,
			allow_method => 'GET,PUT,POST,DELETE',
			role => 'PB2::ArchiveEntry',
			method => 'delete_archives',
			output => '1.2:archives:basic'},
	"This operation allows you to delete one or more existing data archives.");
    
    $ds2->list_node({ path => 'archives/delete',
		      list => 'entry/archives',
		      place => 1 });
    
    $ds2->extended_doc({ path => 'archives/delete' },
	"You may provide the necessary parameters in the URL (with method C<B<GET>> or C<B<DELETE>>)",
	"or in the request body in JSON format (with method C<B<PUT>> or C<B<POST>>). With the latter,",
	"you may specify multiple records. All records must specify a data archive identifier,",
	"and will delete the specified record if you have permission to do so.",
	">Nothing will be returned except a result code indicating success or failure,",
	"plus any errors or warnings that were generated.");
    
    # Entry operations for specimens and measurements.
    
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
			body_ruleset => '1.2:specs:basic_entry,1.2:specs:measurement_entry',
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
