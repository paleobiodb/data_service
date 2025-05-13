#  
# ReferenceEntry
# 
# This role provides operations for entry and editing of bibliographic references.
# 
# Author: Michael McClennen

use strict;

package PB2::ReferenceEntry;

use HTTP::Validate qw(:validators);

use TableDefs qw(%TABLE);

use CoreTableDefs;
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);
use TableData qw(complete_ruleset complete_valueset);

use ReferenceEdit;
use ReferenceManagement;

use Carp qw(carp croak);
use JSON qw(to_json);
use Storable qw(freeze);
use LWP::UserAgent;

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
	{ value => 'DUPLICATE' },
	    "Allow allow this operation even if it may lead to a duplicate record in the database.",
	{ value => 'CAPITAL' },
	    "Allow bad capitalization of names and titles.");
    
    $ds->define_set('1.2:refs:publication_type');
    
    $ds->define_ruleset('1.2:refs:add' =>
	{ optional => 'SPECIAL(show)', valid => '1.2:refs:output_map' },
	{ optional => 'allow', valid => '1.2:refs:allowances', list => ',' },
	    "Allows the operation to proceed with certain conditions or properties:",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:refs:update' =>
	{ optional => 'ref_id', valid => VALID_IDENTIFIER('REF'), alias => 'id' },
	    "The identifier of a reference to update. If this parameter is specified,",
	    "then the body record should not contain a collection identifier.",
	{ optional => 'SPECIAL(show)', valid => '1.2:refs:output_map' },
	{ optional => 'allow', valid => '1.2:refs:allowances', list => ',' },
	    "Allows the operation to proceed with certain conditions or properties:",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");

    $ds->define_ruleset('1.2:refs:addupdate' =>
	{ optional => 'ref_id', valid => VALID_IDENTIFIER('REF'), alias => 'id' },
	    "The identifier of a reference to update. If this parameter is specified,",
	    "then the body record should not contain a collection identifier.",
	    "For an add operation, do not specify this parameter.",
	{ optional => 'SPECIAL(show)', valid => '1.2:refs:output_map' },
	{ optional => 'allow', valid => '1.2:refs:allowances', list => ',' },
	    "Allows the operation to proceed with certain conditions or properties:",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");

    # $ds->define_ruleset('1.2:refs:author_entry');
    
    $ds->define_ruleset('1.2:refs:addupdate_body' =>
	">>The body of this request must be either a single JSON object, or an array of",
	"JSON objects, or else a single record in C<application/x-www-form-urlencoded> format.",
	"The following fields are allowed in each record. If no specific documentation is given",
	"the value must match the corresponding column from the C<B<$TABLE{REFERENCE_DATA}>> table",
	"in the database. Any columns that do not accept a null value must be included in every new",
	"record.",
	{ optional => 'reference_id', valid => VALID_IDENTIFIER('REF'),
	  alias => ['reference_no', 'id', 'ref_id', 'oid'] },
	    "If this field is empty, a record will be inserted into the database",
	    "and a new identifier will be returned. If it is non-empty, it must match",
	    "the identifier of an existing record. That record will be updated.",
	{ allow => '1.2:common:entry_fields' },
	{ optional => 'publication_type', alias => ['ref_type'] },
	    "Type of reference to be added: journal article, book chapter, thesis, etc.",
	{ optional => 'pubyr' },
	    "Year of publication",
	{ optional => 'authors', multiple => 1, note => 'textarea,json' },
	    "The author(s) of the work, in the proper order, separated by semicolons.",
	    "Each author name can be specified as 'first last' or as 'last, first'.",
	    "If the body of the request is in JSON format, the authors can be provided",
	    "as a list of strings or a list of objects with fields 'firstname' and 'lastname'.",
	{ optional => 'reftitle', alias => 'ref_title', note => 'textarea' },
	    "Title of the work",
	{ optional => 'pubvol', alias => 'pub_vol' },
	    "Volume in which the work appears",
	{ optional => 'pubno', alias => 'pub_no', },
	    "Issue in which the work appears",
	{ optional => 'pages' },
	    "Page range for the work",
	{ optional => 'language' },
	    "The language of the work",
	{ optional => 'doi' },
	    "One or more DOIs associated with the work, separated by whitespace.",
	{ optional => 'pubtitle', alias => 'pub_title', note => 'textarea' },
	    "Title of the publication in which the work appears (journal, book, series, etc.)",
	{ optional => 'publisher' },
	    "The publisher of the work or of the publication in which it appears",
	{ optional => 'pubcity', alias => 'pub_city' },
	    "The city where published",
	{ optional => 'editors', multiple => 1, note => 'textarea,json' },
	    "The editor(s) of the book, compendium, or other publication. These must",
	    "be given in proper order, separated by semicolons. Each editor name can be",
	    "specified as 'first last' or as 'last, first'.",
	    "If the body of the request is in JSON format, the editors can be provided",
	    "as a list of strings or a list of objects with fields 'firstname' and 'lastname'.",
	{ optional => 'isbn' },
	    "One or more ISBNs associated with the work, separated by whitespace.",
	{ optional => 'project_name' },
	    "The name of the project for which this reference was entered",
	{ optional => 'comments', note => 'textarea' },
	    "Commands and/or remarks about this bibliographic reference");
    
    $ds->define_ruleset('1.2:refs:delete' =>
	{ param => 'ref_id', valid => VALID_IDENTIFIER('REF'), list => ',',
	  alias => ['reference_no', 'id', 'reference_id', 'oid'] },
	    "The identifier(s) of the record(s) to be deleted. You may specify",
	    "multiple identifiers as a comma-separated list.",
	{ optional => 'allow', valid => '1.2:refs:allowances', list => ',' },
	    "Allow the operation to proceed with certain conditions or properties.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    # my $dbh = $ds->get_connection;
    
    # complete_ruleset($ds, $dbh, '1.2:refs:addupate_body', 'REFERENCES');
    
    # complete_valueset($ds, $dbh, '1.2:refs:publication_type', 'REF_TYPES');
    
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
    
    my ($request, $operation) = @_;
    
    $operation ||= '';
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single record only.
    
    my $perms = $request->require_authentication('REFERENCE_DATA');
    
    my ($allowances, $main_params) = $request->parse_main_params('1.2:refs:specifier');
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->parse_body_records($main_params, '1.2:refs:addupdate_body');
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad data");
    }
    
    # If we get here without any errors being detected so far, create a new EditTransaction object to
    # handle this operation.
    
    my $edt = ReferenceEdit->new($request, { permission => $perms, 
					     table => 'REFERENCE_DATA', 
					     allows => $allowances } );
    
    # Now go through the records and handle each one in turn. This will check every record and
    # queue them up for the specified operation.
    
    foreach my $r (@records)
    {
	if ( $r->{_operation} )
	{
	    $edt->process_record('REFERENCE_DATA', $r);
	}
	
	elsif ( $operation eq 'insert' )
	{
	    $edt->insert_record('REFERENCE_DATA', $r);
	}
	
	elsif ( $operation eq 'update' )
	{
	    $edt->update_record('REFERENCE_DATA', $r);
	}
	
	elsif ( $operation eq 'replace' )
	{
	    $edt->replace_record('REFERENCE_DATA', $r);
	}
	
	elsif ( $operation eq 'delete' )
	{
	    $edt->delete_record('REFERENCE_DATA', $r);
	}
	
	else
	{
	    $edt->insert_update_record('REFERENCE_DATA', $r);
	}
    }
    
    # If no errors have been detected so far, execute the queued actions inside
    # a database transaction. If any errors occur during that process, the
    # transaction will be automatically rolled back unless the NOT_FOUND or
    # PROCEED allowance was given. Otherwise, it will be automatically
    # committed.
    
    $edt->commit;
    
    # Now handle any errors or warnings that may have been generated.
    
    $request->collect_edt_warnings($edt);
    $request->collect_edt_errors($edt);
    
    if ( $edt->fatals )
    {
    	die $request->exception(400, "Bad request");
    }
    
    # Return all new, updated, or deleted records.
    
    my @deleted_keys = $edt->deleted_keys;
    
    $request->list_deleted_refs(\@deleted_keys, $edt->key_labels) if @deleted_keys;
    
    my @existing_keys = ($edt->inserted_keys, $edt->updated_keys, $edt->replaced_keys);
    
    unless ( $request->has_block('none') )
    {
	$request->list_updated_refs($dbh, \@existing_keys, $edt->key_labels) if @existing_keys;
    }
}


sub list_updated_refs {
    
    my ($request, $dbh, $ref_ids, $ref_labels) = @_;
    
    # Get a list of the reference_no values to return.
    
    my @ids = grep { $_ > 0 } $ref_ids->@*;
    
    return unless @ids;
    
    my $id_list = join(',', @ids);
    
    my $filter_string = "reference_no in ($id_list)";
    
    # Fetch the main reference records.
    
    $request->strict_check;
    $request->extid_check;
    
    $request->substitute_select( cd => 'r' );
    my $fields = $request->select_string;
    my $tables = $request->tables_hash;
    
    my $join_list = $request->generate_join_list($tables);
    
    my $calc = $request->sql_count_clause;
    
    my $sql = "SELECT $calc $fields
		FROM refs as r
		    $join_list
		WHERE $filter_string
		ORDER BY reference_no";
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    # Fetch the author/editor names
    
    $sql = "SELECT * FROM ref_authors
	       WHERE $filter_string
	       ORDER BY reference_no, place";
    
    my $attrib = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    my (%authors, %editors) = @_;
    
    foreach my $a ( $attrib->@* )
    {
	my $refno = $a->{reference_no};
	
	$authors{$refno} ||= [ ];
	push $authors{$refno}->@*, $a;
    }
    
    # Link them up
    
    foreach my $r ( $result->@* )
    {
	my $refno = $r->{reference_no};
	
	if ( $authors{$refno} )
	{
	    $r->{authors} = $authors{$refno};
	}
    }
    
    # Return the result list
    
    $request->list_result($result);
}


sub delete_refs {
    
    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL.
    
    my (@id_list) = $request->clean_param_list('ref_id');
    
    # Check for any allowances.
    
    my $allowances = { };
    
    if ( my @allowance = $request->clean_param_list('allow') )
    {
	$allowances->{$_} = 1 foreach @allowance;
    }
    
    # Authenticate to the database
    
    my $perms = $request->require_authentication('REFERENCE_DATA');
    
    # Create a transaction object for this operation.
    
    my $edt = ReferenceEdit->new($request, $perms, 'REFERENCE_DATA', $allowances);
    
    # Then go through the records and handle each one in turn.
    
    foreach my $id (@id_list)
    {
	$edt->delete_record('REFERENCE_DATA', $id);
    }
    
    $edt->commit;
    
    # Now handle any errors of warnings that may have been generated.
    
    $request->collect_edt_warnings($edt);
    $request->collect_edt_errors($edt);
    
    if ( $edt->fatals )
    {
	die $request->exception(400, "Bad request");
    }
    
    # Then return one result record for each deleted database record.
    
    my @results;
    
    foreach my $record_id ( $edt->deleted_keys )
    {
	push @results, { reference_no => generate_identifier('REF', $record_id),
			 _status => 'deleted' };
    }
    
    $request->{main_result} = \@results;
    $request->{result_count} = scalar(@results);
}


sub addupdate_sandbox {
    
    my ($request, $operation) = @_;

    if ( $operation eq 'insert' )
    {
	$request->generate_sandbox({ operation => 'refs/add',
				     ruleset => '1.2:refs:addupdate_body',
				     allowances => '1.2:refs:allowances',
				     extra_params => 'vocab=pbdb&show=both,edit' });
    }
    
    elsif ( $operation eq 'update' )
    {
	$request->generate_sandbox({ operation => 'refs/update',
				     ruleset => '1.2:refs:addupdate_body',
				     allowances => '1.2:refs:allowances',
				     extra_params => 'vocab=pbdb&show=both,edit' });
    }
    
    else
    {
	$request->generate_sandbox('unknown');
    }
}

1;

