#  
# ReferenceEntry
# 
# This role provides operations for entry and editing of bibliographic references.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::ReferenceEntry;

use HTTP::Validate qw(:validators);

use TableDefs qw(%TABLE);

use CoreTableDefs;
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);
use TableData qw(complete_ruleset);

use ReferenceEdit;

use Carp qw(carp croak);

use Moo::Role;

our (@REQUIRES_ROLE) = qw(PB2::Authentication PB2::CommonData PB2::CommonEntry PB2::ReferenceData);


# initialize ( )
# 
# This routine is called by the Web::DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # Value sets for specifying data entry options
    
    $ds->define_set('1.2:refs:allowances' =>
	{ insert => '1.2:common:std_allowances' },
	{ value => 'DUPLICATE_REF' },
	    "This allowance disables the check for duplicate bibliographic references.",
	    "Without it, any record that appears to have a high likelihood of",
	    "duplicating a reference that is already in the database will generate a caution.");
    
    $ds->define_ruleset('1.2:refs:addupdate' =>
	{ allow => '1.2:refs:specifier' },
	{ optional => 'SPECIAL(show)', valid => '1.2:refs:output_map' },
	{ optional => 'allow', valid => '1.2:refs:allowances', list => ',' },
	    "Allows the operation to proceed with certain conditions or properties:",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:refs:addupdate_body' =>
	">>The body of this request must be either a single JSON object, or an array of",
	"JSON objects, or else a single record in C<application/x-www-form-urlencoded> format.",
	"The following fields are allowed in each record. If no specific documentation is given",
	"the value must match the corresponding column from the C<B<$TABLE{REFERENCES}>> table",
	"in the database. Any columns that do not accept a null value must be included in every new",
	"record.",
	{ optional => 'reference_id', valid => VALID_IDENTIFIER('REF'),
	  alias => ['reference_no', 'id', 'oid'] },
	    "If this field is empty, this record will be inserted into the database",
	    "and a new identifier will be returned. If it is non-empty, it must match",
	    "the identifier of an existing record."
	{ allow => '1.2:common:entry_fields' },
	{ optional => '_allow_duplicate', valid => BOOLEAN_VALUE },
	    "If this field has a true value, the check for duplicate references will be",
	    "skipped for this record only. The DUPLICATE_REF allowance can be used to",
	    "skip the check for this entire request.",
	{ allow => '1.2:archives:entry' });
    
    $ds->define_ruleset('1.2:refs:delete' =>
	{ param => 'reference_id', valid => VALID_IDENTIFIER('REF'), list => ',',
	  alias => ['reference_no', 'id', 'oid'] },
	    "The identifier(s) of the record(s) to be deleted. You may specify",
	    "multiple identifiers as a comma-separated list.",
	{ optional => 'allow', valid => '1.2:refs:allowances', list => ',' },
	    "Allow the operation to proceed with certain conditions or properties.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    my $dbh = $ds->get_connection;
    
    complete_ruleset($ds, $dbh, '1.2:refs:addupate_body', 'REFERENCES');
}


# addupdate_refs ( request, arg )
#
# Execute one or more database operations involving bibliographic references. The request body is
# decoded into a list of one or more records, each of which specifies an insert, update, replace,
# delete, or auxiliary operation. These are all processed together as part of a single transaction.

my $addupdate_config = { class => 'ReferenceEdit',
			 main_table => 'REFERENCE_DATA',
			 url_ruleset => '1.2:refs:specifier',
			 body_ruleset => '1.2:refs:addupdate_body',
			 primary_identifier => 'reference_id',
			 record_selector => [ { ruleset => '1.2:abc:def', table => 'AUXILIARY_DATA',
						keys => 'abc, def, ghi' } ]
		       };

sub addupdate_refs {
    
    my ($request, $arg) = @_;
    
    # $$$
    
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single record only.
    
    my $allowances = { };
    
    my $main_params = $request->get_main_params($allowances, '1.2:refs:specifier');
    my $perms = $request->require_authentication('REFERENCES');
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->unpack_input_records($main_params, '1.2:refs:addupdate_body');
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad data");
    }
    
    # If we get here without any errors being detected so far, create a new EditTransaction object to
    # handle this operation.
    
    my $edt = EditTransaction->new($request, $perms, 'REFERENCES', $allowances);
    
    # Now go through the records and handle each one in turn. This will check every record and
    # queue them up for insertion and/or updating.
    
    foreach my $r (@records)
    {
	$edt->process_record('REFERENCES', $r);
    }
    
    # If no errors have been detected so far, execute the queued actions inside a database
    # transaction. If any errors occur during that process, the transaction will be automatically
    # rolled back unless the PROCEED allowance was given. Otherwise, it will be automatically
    # committed.
    
    $edt->commit;
    
    # Now handle any errors or warnings that may have been generated.
    
    $request->collect_edt_warnings($edt);
    
    if ( $edt->errors )
    {
    	$request->collect_edt_errors($edt);
    	die $request->exception(400, "Bad request");
    }
    
    # Return all new, updated, or deleted records.
    
    my @deleted_keys = $edt->deleted_keys;
    
    $request->list_deleted_refs(\@deleted_keys, $edt->key_labels) if @deleted_keys;
    
    my @existing_keys = ($edt->inserted_keys, $edt->updated_keys, $edt->replaced_keys);
    
    $request->list_updated_refs($dbh, \@existing_keys, $edt->key_labels) if @existing_keys;
}
    
}


