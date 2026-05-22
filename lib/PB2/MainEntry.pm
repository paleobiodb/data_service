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
use PB2::TaxonEntry;
use PB2::ResourceEntry;
use PB2::PublicationEntry;
use PB2::ArchiveEntry;
use PB2::PreferencesEntry;

sub initialize {

    my ($ds2) = @_;
    
    $ds2->define_node({ path => 'entry',
			title => 'Data Entry' },
	"The categories listed below include all of the data entry operations.",
	"Operations that alter the database require you to be logged in with a",
	"role that allows you to enter data.");
    
    # Fossil Occurrences
    
    $ds2->define_node({ path => 'entry/occs',
			title => 'Fossil Occurrences',
			doc_template => 'category.tt' },
	"The operations in this category provide data entry for Fossil Occurrences.",
	"Operations that alter the database require you to be logged in with",
	"a role that allows you to enter data.");
    
    $ds2->list_node({ path => 'entry/occs',
		      list => 'entry',
		      place => 1 },
	"Data entry operations for Fossil Occurrences");
    
    $ds2->define_node({ path => 'occs/update',
			title => 'Update occurrences',
			place => 20,
			usage => ["/occs/update.json?show=all&vocab=pbdb"],
			allow_method => 'PUT,POST',
			tag => 'entry',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:occs:addupdate_body',
			role => 'PB2::CollectionEntry',
			method => 'update_occs',
			output => '1.2:occs:display',
			optional_output => '1.2:occs:display_map' },
	"The operation B<C<occs/update>> allows you to add, update, and delete",
	"Fossil Occurrences in the database. It requires that you be logged in",
	"to the database, and that you have permission to edit the collection(s) with",
	"which the occurrence(s) will be associated.");
    
    $ds2->list_node({ path => 'occs/update',
		      list => 'entry/occs',
		      place => 1 });
    
    $ds2->define_node({ path => 'occs/update_sandbox',
			title => 'Sandbox for the occs/update operation',
			place => 20,
			usage => ["/occs/update_sandbox.html"],
			allow_format => 'html',
			allow_method => 'GET',
			doc_template => 'sandbox_operation.tt',
			role => 'PB2::CollectionEntry',
			method => 'update_occs_sandbox',
			body_ruleset => '1.2:occs:addupdate_body' },
	"The operation B<C<occs/update_sandbox>> displays an HTML form which",
	"you can use to generate calls to the B<C<occs/update operation>>.");
    
    $ds2->list_node({ path => 'occs/update_sandbox',
		      list => 'entry/occs',
		      place => 1 });
    
    $ds2->define_node({ path => 'occs/checknames',
			title => 'Check occurrence names before submission',
			place => 21,
			usage => ["/occs/checknames.json?name=Dascillus%20shandongianus"],
			allow_method => 'GET,POST',
			doc_template => 'entry_operation.tt',
			role => 'PB2::CollectionEntry',
			method => 'check_taxonomic_names',
			ruleset => '1.2:occs:checknames',
			body_ruleset => '1.2:occs:checknames_body',
			output => '1.2:occs:checknames' },
	"The operation B<C<occs/checknames>> checks one or more taxonomic names.",
	"It is recommended to call this operation before submitting occurrences",
	"for insertion or update.");
    
    $ds2->list_node({ path => 'occs/checknames',
		      list => 'entry/occs',
		      place => 2 });
    
    $ds2->define_node({ path => 'occs/checknames_sandbox',
			title => 'Sandbox for the occs/checknames operation',
			place => 21,
			usage => ["/occs/checknames_sandbox.html"],
			allow_format => 'html',
			allow_method => 'GET',
			doc_template => 'sandbox_operation.tt',
			role => 'PB2::CollectionEntry',
			method => 'check_names_sandbox',
			body_ruleset => '1.2:occs:checknames_body' },
	"The operation B<C<occs/checknames_sandbox>> displays an HTML form which",
	"you can use to generate calls to the B<C<occs/checknames>> operation.");
    
    $ds2->list_node({ path => 'occs/checknames_sandbox',
		      list => 'entry/occs',
		      place => 2 });
    
    # Fossil Collections
    
    $ds2->define_node({ path => 'entry/colls',
			title => 'Fossil Collections',
			doc_template => 'category.tt' },
	"The operations in this category provide data entry for Fossil Collections.",
	"Operations that alter the database require you to be logged in with",
	"a role that allows you to enter such data.");

    $ds2->list_node({ path => 'entry/colls',
		      list => 'entry',
		      place => 2 },
	"Data entry operations for Fossil Collections");
    
    $ds2->define_node({ path => 'colls/add',
			title => 'Add new collections',
			place => 10,
			usage => ["/colls/add.json?show=edit&vocab=pbdb"],
			allow_method => 'PUT,POST',
			tag => 'entry',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:colls:addupdate_body',
			role => 'PB2::CollectionEntry',
			method => 'addupdate_colls',
			arg => 'insert',
			output => '1.2:colls:basic',
			optional_output => '1.2:colls:basic_map' },
	"The operation B<C<colls/add>> allows you to add new Fossil Collections",
	"to the database. This operation requires you to be logged in",
	"with a database contributor role.");
    
    $ds2->list_node({ path => 'colls/add',
		      list => 'entry/colls',
		      place => 1 });
    
    $ds2->define_node({ path => 'colls/add_sandbox',
			title => 'Sandbox for the colls/add operation',
			place => 20,
			usage => ["/colls/add_sandbox.html"],
			allow_format => 'html',
			allow_method => 'GET',
			doc_template => 'sandbox_operation.tt',
			role => 'PB2::CollectionEntry',
			method => 'addupdate_sandbox',
			arg => 'insert',
			body_ruleset => '1.2:colls:addupdate_body' },
	"The operation B<C<colls/add_sandbox>> displays an HTML form which you can use",
	"to generate calls to the B<C<colls/add>> operation.");
    
    $ds2->list_node({ path => 'colls/add_sandbox',
		      list => 'entry/colls',
		      place => 1 });
    
    $ds2->define_node({ path => 'colls/update',
			title => 'Update existing collections',
			place => 20,
			usage => ["/colls/update.json?show=edit&vocab=pbdb"],
			allow_method => 'PUT,POST',
			tag => 'entry',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:colls:addupdate_body',
			role => 'PB2::CollectionEntry',
			method => 'addupdate_colls',
			arg => 'update',
			output => '1.2:colls:basic',
			optional_output => '1.2:colls:basic_map' },
	"The operation B<C<colls/update>> allows you to update existing",
	"Fossil Collections in the database. This operation requires you to be logged",
	"in to the database and to have permission to edit the specified collections.");
    
    $ds2->list_node({ path => 'colls/update',
		      list => 'entry/colls',
		      place => 2 });
    
    $ds2->define_node({ path => 'colls/update_sandbox',
			title => 'Sandbox for the colls/update operation',
			place => 20,
			usage => ["/colls/update_sandbox.html"],
			allow_format => 'html',
			allow_method => 'GET',
			doc_template => 'sandbox_operation.tt',
			role => 'PB2::CollectionEntry',
			method => 'addupdate_sandbox',
			arg => 'update',
			body_ruleset => '1.2:colls:addupdate_body' },
	"The operation B<C<colls/update_sandbox>> displays an HTML form which",
	"you can use to generate calls to the B<C<colls/update>> operation.");

    $ds2->list_node({ path => 'colls/update_sandbox',
		      list => 'entry/colls',
		      place => 2 });
    
    $ds2->define_node({ path => 'colls/delete',
			title => 'Delete existing collections',
			place => 20,
			usage => ["/colls/delete.json?id=1234"],
			allow_method => 'GET,POST',
			tag => 'entry',
			doc_template => 'operation.tt',
			role => 'PB2::CollectionEntry',
			method => 'addupdate_colls',
			arg => 'delete',
			output => '1.2:colls:basic' },
	"The operation B<C<colls/delete>> allows you to delete Fossil Collections",
	"which were entered",
	"or authorized by you, or for which you have administrative privilege. This",
	"operation requires you to be logged in to the database with a database",
	"contributor role.");
    
    $ds2->list_node({ path => 'colls/delete',
		      list => 'entry/colls',
		      place => 3 });
    
    # Taxon images
    
    $ds2->define_node({ path => 'taxa/image_choices',
			title => 'List taxon image choices',
			place => 20,
			allow_format => 'json',
			role => 'PB2::TaxonEntry',
			method => 'list_image_choices',
			output => '1.2:taxa:image_choices' },
	"The operation B<C<taxa/image_choices>> lists the available image choices",
	"for the specified range of taxa. It is only available to logged-in database",
	"contributors");
    
    $ds2->define_node({ path => 'taxa/update_image_choices',
		       place => 21,
		       allow_format => 'json',
		       allow_method => 'PUT,POST',
		       tag => 'entry',
		       doc_template => 'entry_operation.tt',
		       body_ruleset => '1.2:taxa:image_choices_body',
		       role => 'PB2::TaxonEntry',
		       method => 'update_image_choices',
		       output => '1.2:taxa:image_choices' },
	"The operation B<C<taxa/update_image_choices>> allows you to choose among the",
	"available image choices for specific taxa. The chosen images will then be",
	"displayed in Navigator for all users. This operation is only available to",
	"logged-in database contributors who have administrative permission on the",
	"PHYLOPIC_CHOICE table.");
    
    $ds2->define_node({ path => 'taxa/update_image_choices_sandbox',
			title => 'Sandbox for the taxa/update_image_choices operation',
			place => 21,
			usage => ["/taxa/update_image_choices_sandbox.html"],
			allow_format => 'html',
			tag => 'entry',
			doc_template => 'sandbox_operation.tt',
			role => 'PB2::TaxonEntry',
			method => 'update_image_choices_sandbox',
			body_ruleset => '1.2:taxa:image_choices_body',
			output => '1.2:taxa:image_choices' },
	"The operation B<C<taxa/update_image_choices_sandbox>> displays an HTML form",
	"which you can use to generate calls to the B<C<taxa/update_image_choices>>",
	"operation.");
    
    # Educational Resources
    
    $ds2->define_node({ path => 'entry/eduresources',
			doc_template => 'category.tt',
			title => 'Educational Resources' },
	"The operations listed in this section provide data entry for Educational",
	"Resources. These operations require that you be logged in to the database,",
	"as either a database contributor or a guest.");
    
    $ds2->list_node({ path => 'entry/eduresources',
		      list => 'entry',
		      place => 10 },
	"Data entry operations for Educational Resources");
    
    $ds2->define_node({ path => 'eduresources/addupdate',
			title => 'Add educational resources or update existing records',
			place => 0,
			usage => [ "eduresources/addupdate.json" ],
			allow_method => 'GET,PUT,POST',
			tag => 'entry',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:eduresources:addupdate_body',
			role => 'PB2::ResourceEntry',
			method => 'update_resources',
			arg => 'add',
			output => '1.2:eduresources:basic',
			optional_output => '1.2:eduresources:optional_output' },
	"The operation B<C<eduresources/addupdate>> allows you to add new Educational",
	"Resource records to the database and/or",
	"update the attributes of existing records. This operation requires you to",
	"be logged in to the database as either a database contributor or a guest.",
	"Unless you have the B<Educational Resources> special permission, you can",
	"only modify records that you originally submitted.");
    
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
			tag => 'entry',
			doc_template => 'entry_operation.tt',
			role => 'PB2::ResourceEntry',
			method => 'update_resources',
			arg => 'update',
			output => '1.2:eduresources:basic',
			optional_output => '1.2:eduresources:optional_output' },
	"The operation B<C<eduresources/update>> allows you to update the attributes of",
	"existing educational resource records. This operation requires you to be logged",
	"in to the database as either a database contributor or a guest.",
	"Unless you have the B<Educational Resources> special permission, you can",
	"only modify records that you originally submitted.");
    
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
			tag => 'entry',
			role => 'PB2::ResourceEntry',
			method => 'delete_resources',
			output => '1.2:eduresources:basic'},
	"The operation B<C<eduresources/delete>> allows you to delete existing",
	"Educational Resource records. This operation requires you to be logged in to",
	"the database as either a database contributor or a guest. Unless you have",
	"the B<Educational Resources> special permission, you can only delete",
	"records that you originally submitted.");
    
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
			doc_template => 'category.tt',
			title => 'Research Publications' },
	"The operations in this section provide data entry for the Official Publications",
	"list. These operations require you to be logged in to the database and to have",
	"the B<Official Publications> special permission.");
    
    $ds2->list_node({ path => 'entry/pubs',
		      list => 'entry',
		      place => 9 },
	"Data entry operations for Official Publication records");
    
    $ds2->define_node({ path => 'pubs/addupdate',
			title => 'Add official publications or update existing records',
			place => 0,
			allow_method => 'PUT,POST',
			tag => 'entry',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:pubs:addupdate_body',
			role => 'PB2::PublicationEntry',
			allow_format => '+larkin',
			method => 'update_publications',
			output => '1.2:pubs:basic',
			optional_output => '1.2:pubs:optional_output' },
	"The B<C<pubs/addupdate>> operation allows you to add new Official Publication",
	"records to the database and/or update the attributes of existing records.",
	"It requires you to be logged in to the database and to have the",
	"B<Official Publications> special permission.");
    
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
			tag => 'entry',
			role => 'PB2::PublicationEntry',
			method => 'delete_publications',
			output => '1.2:pubs:basic'},
	"The B<C<pubs/delete>> operation allows you to delete existing",
	"Official Publication records. It requires you to be logged in to the",
	"database and to have the B<Official Publications> special permission.",
	"You can only delete records that you created, unless you have the",
	"B<admin> version of the special permission.");
    
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
		      place => 8 },
	"Data entry operations for data archive records.");
    
    $ds2->define_node({ path => 'archives/addupdate',
			title => 'Add data archive records or update existing records',
			place => 0,
			allow_method => 'PUT,POST',
			tag => 'entry',
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
			tag => 'entry',
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
			doc_template => 'category.tt',
			title => 'Specimens and Measurements' },
	"The operations in this category provide data entry for Fossil Specimens.",
	"Operations that alter the database require you to be logged in with",
	"a role that allows you to enter data.");
    
    $ds2->list_node({ path => 'entry/specs',
		      list => 'entry',
		      place => 3 },
	"Data entry operations for Fossil Specimen and Measurement records");
    
    $ds2->define_node({ path => 'specs/addupdate',
			title => 'Add specimen records or update existing records',
			place => 1,
			allow_method => 'GET,PUT,POST',
			tag => 'entry',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:specs:basic_entry,1.2:specs:measurement_entry',
			role => 'PB2::SpecimenEntry',
			method => 'update_specimens',
			arg => 'add',
			# before_record_hook => 'my_select_output_block',
			output => '1.2:specs:basic',
			optional_output => '1.2:specs:basic_map' },
	"The operation B<C<specs/addupdate>> allows you to add new specimen and",
	"measurement records to the database and/or update the attributes of existing",
	"records. This operation requires you to be logged in to the database and",
	"to have edit permission on any collections that the specimens will be",
	"associated with.");
    
    $ds2->list_node({ path => 'specs/addupdate',
		      list => 'entry/specs',
		      place => 1 });
    
    $ds2->extended_doc({ path => 'specs/addupdate' },
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
			tag => 'entry',
			doc_template => 'entry_operation.tt',
			role => 'PB2::SpecimenEntry',
			method => 'update_specimens',
			# before_record_hook => 'my_select_output_block',
			output => '1.2:specs:basic',
			optional_output => '1.2:specs:basic_map' },
	"The operation B<C<specs/update>> allows you to update existing specimen",
	"and measurement records. This operation requires you to be logged in to",
	"the database and to have edit permission on the collection(s) that the",
	"specimens are associated with.");
    
    $ds2->list_node({ path => 'specs/update',
		      list => 'entry/specs',
		      place => 1 });
    
    $ds2->extended_doc({ path => 'specs/update' },
	"The records to be updated should appear in the request body,",
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
			tag => 'entry',
			doc_template => 'entry_operation.tt',
			role => 'PB2::SpecimenEntry',
			method => 'delete_specimens',
			output => '1.2:specs:deleted' },
	"The operation B<C<specs/delete>> allows you to delete specimen records",
	"from the database. This operation requires you to be logged in and to have",
	"edit permission on the collection(s) with which the specimens are associated."); 
    
    $ds2->list_node({ path =>'specs/delete',
		      list => 'entry/specs',
		      place => 2 });
    
    $ds2->define_node({ path => 'specs/addupdate_measurements',
			title => 'Add measurement records or update existing records',
			place => 2,
			allow_method => 'GET,PUT,POST',
			tag => 'entry',
			doc_template => 'entry_operation.tt',
			role => 'PB2::SpecimenEntry',
			method => 'update_measurements',
			arg => 'add',
			output => '1.2:measure:basic' },
	"The operation B<C<specs/addupdate_measurements>> allows you to add new",
	"specimen measurement records to the database and/or",
	"update the attributes of existing records. It requires you to be logged in",
	"to the database and to have edit permission on the collection(s) that the",
	"specimens are associated with.");
    
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
    
    # Bibliographic References
    
    $ds2->define_node({ path => 'entry/refs',
			title => 'Bibliographic References',
			doc_template => 'category.tt' },
	"The operations in this category provide data entry for",
	"Bibliographic References. Operations that alter the database require you",
	"to be logged in with a role that allows you to enter data.");
    
    $ds2->list_node({ path => 'entry/refs',
		      list => 'entry',
		      place => 4 },
	"Data entry operations for Bibliographic References");
    
    $ds2->define_node({ path => 'refs/matchlocal',
			title => 'Check for a matching bibliographic reference locally',
		        place => 10,
			usage => ["/refs/matchlocal.json?ref_author=Sepkoski&ref_title=Compendium"],
			allow_method => 'GET',
		        role => 'PB2::ReferenceAux',
			method => 'match_local' },
	"The operation B<C<refs/matchlocal>> takes a partial set of Bibliographic",
	"Reference attributes",
	"and returns any matching entries from the local database. A DOI alone is",
	"often enough, but good results can be gotten from one or two authors, a",
		      "publication year, and some words from the title.");
    
    $ds2->list_node({ path => 'refs/matchlocal',
		      list => 'entry/refs',
		      place => 1 });
    
    $ds2->define_node({ path => 'refs/matchext',
			title => 'Fetch reference data from known external sources',
			place => 10,
			usage => ["/refs/matchext.json?ref_author=Sepkoski&ref_title=" .
				  "Fossil%20Record&ref_pubyr=2002&show=source,authorlist"],
			allow_method => 'GET',
			tag => 'entry',
			role => 'PB2::ReferenceAux',
			method => 'match_external',
			optional_output => '1.2:refs:matchext_map' },
	"The operation B<C<refs/matchext>> takes a partial set of bibliographic",
	"reference attributes",
	"and returns matching entries from external sources such as Crossref and XDD.",
	"A DOI alone is often enough, but good results can be gotten from one or two",
	"authors, a publication year, and some words from the title.");
    
    $ds2->list_node({ path => 'refs/matchext',
		      list => 'entry/refs',
		      place => 1 });
    
    $ds2->define_node({ path => 'refs/addupdate',
			title => 'Add bibliographic references or update existing references',
			place => 11,
			usage => ["/refs/addupdate.json?show=formatted"],
			allow_method => 'PUT,POST',
			tag => 'entry',
			doc_template => 'entry_operation.tt',
			body_ruleset => '1.2:refs:addupdate_body',
			role => 'PB2::ReferenceEntry',
			method => 'addupdate_refs',
			arg => 'addupdate',
			output => '1.2:refs:basic',
			optional_output => '1.2:refs:output_map' },
	"The operation B<C<refs/addupdate>> allows you to add new Bibliographic References",
	"to the database and/or update existing references.");
    
    $ds2->list_node({ path => 'refs/addupdate',
		      list => 'entry/refs',
		      place => 1 });
    
    $ds2->define_node({ path => 'refs/addupdate_sandbox',
			title => 'Sandbox for the refs/addupdate operation',
			place => 11,
			usage => ["/refs/addupdate_sandbox.html"],
			allow_format => 'html',
			allow_method => 'GET',
			doc_template => 'sandbox_operation.tt',
			role => 'PB2::ReferenceEntry',
			method => 'addupdate_sandbox',
			arg => 'update' },
	"The operation B<C<refs/addupdate_sandbox>> displays an HTML form which",
	"you can use to generate calls to the B<C<refs/addupdate>> operation.");
    
    $ds2->define_node({ path => 'refs/delete',
			title => 'Delete existing bibliographic references',
			place => 12,
			usage => ["/refs/delete.json?id=1234"],
			allow_method => 'GET,DELETE',
			tag => 'entry',
			doc_template => 'entry_operation.tt',
			role => 'PB2::ReferenceEntry',
			method => 'delete_refs',
			output => '1.2:refs:basic' },
	"The operation B<C<refs/delete>> allows you to delete existing Bibliographic",
	"reference records,",
	"provided they are not referenced by any other database records. It works only",
	"on records created by yourself, unless you have administrative privilege.");
    
    $ds2->list_node({ path => 'refs/delete',
		      list => 'entry/refs',
		      place => 1 });
    
    $ds2->define_node({ path => 'refs/selected',
			title => 'Fetch the bibliographic reference selected for data entry',
			place => 13,
			usage => ["/refs/selected.json"],
			allow_method => 'GET',
			doc_template => 'operation.tt',
			role => 'PB2::ReferenceAux',
			method => 'selected',
			output => '1.2:refs:basic',
			optional_output => '1.2:refs:output_map' },
	"The operation B<C<refs/selected>> returns the Bibliographic Reference (if any)",
	"that is currently",
	"selected for your current session in the Classic environment. If no reference",
	"is selected, the result will contain no records. If you are not logged in,",
	"a 401 error will be returned.");
    
    $ds2->list_node({ path => 'refs/selected',
		      list => 'entry/refs',
		      place => 1 });
    
    $ds2->define_node({ path => 'refs/classic_select',
			title => 'Select a bibliographic reference for data entry',
			place => 14,
			usage => ["/refs/classic_select.json?id=6930"],
			allow_method => 'GET',
			doc_template => 'entry_operation.tt',
			role => 'PB2::ReferenceAux',
			method => 'classic_select',
			output => '1.2:refs:basic',
			optional_output => '1.2:refs:output_map' },
	"The operation B<C<refs/classic_select>> selects the specified",
	"reference for your current",
	"session in the Classic environment. After it is executed, the next time the",
	"current user visits the Classic environment, this reference will be the selected one.",
	"if you are not currently logged in, a 401 error will be returned.");
    
    # Preferences
    
    $ds2->define_node({ path => 'prefs',
			title => 'Preferences' });
    
    $ds2->define_node({ path => 'prefs/set',
			title => 'Set preferences',
			allow_method => 'GET,PUT,POST',
			role => 'PB2::PreferencesEntry',
			method => 'set_preference',
			output => '1.2:prefs:basic',
			optional_output => '1.2:prefs:map' },
	"This operation sets one or more preferences or default values for fields.");
    
    $ds2->define_node({ path => 'prefs/set_sandbox',
		       title => 'Sandbox for the prefs/set operation',
		       place => 2,
		       allow_format => 'html',
		       allow_method => 'GET',
		       role => 'PB2::PreferencesEntry',
		       method => 'set_sandbox' },
	"This operation displays a form with which you can generate calls to the",
	"prefs/set operation.");
}


1;
