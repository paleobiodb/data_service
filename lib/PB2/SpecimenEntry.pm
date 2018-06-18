#  
# SpecimenEntry
# 
# A role that provides for data entry and editing for specimens.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::SpecimenEntry;

use HTTP::Validate qw(:validators);

use TableDefs qw(%TABLE);

use CoreTableDefs;
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);
use TableData qw(complete_ruleset);

use SpecimenEdit;

# use Taxonomy;
# use TaxonDefs qw(%RANK_STRING);

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;

our (@REQUIRES_ROLE) = qw(PB2::Authentication PB2::CommonData PB2::CommonEntry PB2::OccurrenceData PB2::SpecimenData);


# initialize ( )
# 
# This routine is called by the Web::DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # Start with value sets for specifying data entry options
    
    $ds->define_set('1.2:specs:conditions' =>
	{ value => 'UNKNOWN_TAXON' },
	    "Proceed with adding the specimen even if the taxon name is not known",
	    "to the database.");
    
    $ds->define_set('1.2:specs:entry_return' =>
	{ value => 'updated' },
	    "Return the new or updated specimen records. This is the default",
	{ value => 'none' },
	    "Return nothing except the status code and any warnings",
	    "or cautions that were generated.");
    
    $ds->define_set('1.2:specs:measurement_return' =>
	{ value => 'updated' },
	    "Return just the new or updated measurement records. This is the default.",
	{ value => 'specimen' },
	    "Return the full list of measurement records for every specimen that",
	    "has at least one updated record.",
	{ value => 'none' },
	    "Return nothing except the status code and any warnings or",
	    "cautions that were generated.");
    
    $ds->define_set('1.2:specs:types' =>
	{ value => 'holo' },
	    "The specimen is a holotype.",
	{ value => 'para' },
	    "The specimen is paratype.",
	{ value => 'mult' },
	    "The specimen consists of more than one paratype.");
    
    # Rulesets for entering and updating data.
    
    $ds->define_ruleset('1.2:specs:basic_entry' =>
	{ optional => 'record_label', valid => ANY_VALUE },
	    "You may provide a value for this attribute in any record",
	    "submitted for entry or update. This allows the data service",
	    "to accurately indicate which records generated errors or warnings.",
	    "You may specify any string, but if you submit multiple records in",
	    "one call each record should have a unique value.",
	{ optional => 'specimen_id', valid => VALID_IDENTIFIER('SPM') },
	    "The identifier of the specimen to be updated. If empty,",
	    "a new specimen record will be created.",
	{ optional => 'collection_id', valid => VALID_IDENTIFIER('COL') },
	    "The identifier of a collection record representing the site from",
	    "which the specimen was collected.",
	# { optional => 'inst_code', value => ANY_VALUE },
	#     "The acronym or abbreviation of the institution holding the specimen.",
	# { optional => 'instcoll_code', value => ANY_VALUE },
	#     "The acronym or abbreviation for the institutional collection of which",
	#     "the specimen is a part.",
	{ optional => 'specimen_code', valid => ANY_VALUE },
	    "The specimen code or identifier as assigned by its holding institution.",
	{ optional => 'taxon_name', valid => ANY_VALUE },
	    "The name of the taxon to which this specimen is identified.",
	    "You must either specify this OR B<C<taxon_id>>.",
	{ optional => 'taxon_id', valid => VALID_IDENTIFIER('TXN') },
	    "The identifier of the taxon to which this specimen is identified.",
	    "You must either specify this OR B<C<taxon_name>>.",
	{ at_most_one => [ 'taxon_name', 'taxon_id' ] },
	{ optional => 'reference_id', valid => VALID_IDENTIFIER('REF') },
	    "The identifier of the reference with which this specimen is identified.");
    
    $ds->define_ruleset('1.2:specs:measurement_entry' =>
	{ optional => 'record_label', valid => ANY_VALUE },
	    "You may provide a value for this attribute in any record",
	    "submitted for entry or update. This allows the data service",
	    "to accurately indicate which records generated errors or warnings.",
	    "You may specify any string, but if you submit multiple records in",
	    "one call each record should have a unique value.",
	{ optional => 'measurement_id', valid => VALID_IDENTIFIER('SPM') },
	    "The identifier of the measurement to be updated. If empty,",
	    "a new measurement record will be created.",
	{ optional => 'specimen_id', valid => VALID_IDENTIFIER('SPM') },
	    "The identifier of the specimen with which this measurement",
	    "is to be associated. If empty, it will be associated with the",
	    "most recent specimen record processed during this operation.");
    
    $ds->define_ruleset('1.2:specs:op_mod' =>
	">>The following parameters affect how this operation is carried out:",
	{ optional => 'allow', valid => '1.2:specs:conditions' },
	    "This parameter specifies a list of actions that will",
	    "be allowed to occur during processing of this request, and",
	    "not block it from completing. B<Important:> for many applications,",
	    "it is best to allow the request to block, get confirmation from",
	    "the user for each flagged condition, and if confirmed then repeat the request",
	    "with these specific actions allowed using this parameter. Accepted",
	    "values include:");
    
    $ds->define_ruleset('1.2:specs:ret_mod' =>
	">>The following parameters specify what should be returned from this",
	"operation:",
    	{ optional => 'SPECIAL(show)', valid => '1.2:specs:basic_map' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:specs:meas_mod' =>
	">>The following parameters specify what should be returned from this",
	"operation:",
	{ optional => 'return', valid => '1.2:specs:measurement_return' },
	    "This parameter specifies what records should be returned from the",
	    "update operation. You can choose to return just the updated records,",
	    "all measurement records associated with any specimen with at least one",
	    "updated record, or nothing at all.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:specs:addupdate' =>
	{ allow => '1.2:specs:op_mod' },
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every specimen specified in the body.",
	{ allow => '1.2:specs:basic_entry' },
	{ allow => '1.2:specs:ret_mod' });
    
    $ds->define_ruleset('1.2:specs:addupdate_body' =>
	{ allow => '1.2:specs:basic_entry' });
    
    $ds->define_ruleset('1.2:specs:update' =>
	{ allow => '1.2:specs:op_mod' },
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every specimen specified in the body.",
	{ allow => '1.2:specs:basic_entry' },
	{ allow => '1.2:specs:ret_mod' });
    
    $ds->define_ruleset('1.2:specs:update_body' =>
	{ allow => '1.2:specs:basic_entry' });
    
    $ds->define_ruleset('1.2:specs:delete' =>
	{ allow => '1.2:specs:op_mod' }, 
	">>The following parameter may be given either in the URL or in",
	"the request body. Either way, you may specify more than one value,",
	"as a comma-separated list.",
	{ param => 'spec_id', valid => VALID_IDENTIFIER('SPM'), list => ',' },
	    "The identifier(s) of the specimen(s) to delete.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:specs:addupdate_measurements' =>
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every measurement specified in the body.",
	{ allow => '1.2:specs:measurement_entry' },
	{ allow => '1.2:specs:meas_mod' });
    
    $ds->define_ruleset('1.2:specs:addupdate_measurements_body' =>
	{ allow => '1.2:specs:measurement_entry' });
    
    $ds->define_ruleset('1.2:specs:update_measurements' =>
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every measurement specified in the body.",
	{ allow => '1.2:specs:measurement_entry' },
	{ allow => '1.2:specs:meas_mod' });
    
    $ds->define_ruleset('1.2:specs:update_measurements_body' =>
	{ allow => '1.2:specs:measurement_entry' });
    
    my $dbh = $ds->get_connection;
    
    complete_ruleset($ds, $dbh, '1.2:specs:addupdate_body', 'SPECIMEN_DATA');
    complete_ruleset($ds, $dbh, '1.2:specs:update_body', 'SPECIMEN_DATA');
    complete_ruleset($ds, $dbh, '1.2:specs:addupdate_measurements_body', 'MEASUREMENT_DATA');
    complete_ruleset($ds, $dbh, '1.2:specs:update_measurements_body', 'MEASUREMENT_DATA');
}


sub update_specimens {
    
    my ($request, $arg) = @_;
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single timescale only.
    
    my %allowances = ( IMMEDIATE_MODE => 1 );
    
    $allowances{CREATE} = 1 if $arg && $arg eq 'add';
    
    my $main_params = $request->get_main_params(\%allowances, '1.2:specs:basic_entry');
    my $perms = $request->require_authentication('SPECIMEN_DATA');
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->unpack_input_records($main_params,
						   ['1.2:specs:addupdate_measurements_body', 'measurement_id', 'measurement_type'],
						   ['1.2:specs:addupdate_body', 'specimen_id', 'DEFAULT']);
    
    # If any errors were found in the parameters, stop now and return an HTTP 400 response.
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad request");
    }
    
    # Otherwise, start a new transaction.
    
    my $edt = SpecimenEdit->new($request, $perms, 'SPECIMEN_DATA', \%allowances);
    
    # Now go through the records and handle each one in turn. This will check every record and
    # queue them up for insertion and/or updating.

    foreach my $r (@records)
    {
	if ( exists $r->{measurement_type} )
	{
	    $edt->insert_update_record('MEASUREMENT_DATA', $r);
	}

	else
	{
	    $edt->insert_update_record('SPECIMEN_DATA', $r);
	}
    }
    
    # If no errors have been detected so far, execute the queued actions inside a database
    # transaction. If any errors occur during that process, the transaction will be automatically
    # rolled back. Otherwise, it will be automatically committed.
    
    $edt->commit;
    
    # If any warnings (non-fatal conditions) were detected, add them to the
    # request record so they will be communicated back to the user.
    
    $request->collect_edt_warnings($edt);
    
    # If we completed the procedure without any exceptions, but error conditions were detected
    # nonetheless, these should be reported. In this case, the transaction will have automatically
    # been rolled back.
    
    if ( $edt->errors )
    {
    	$request->collect_edt_errors($edt);
	
	if ( $edt->has_condition_code('E_EXECUTE') )
	{
	    die $request->exception(500, "Internal error");
	}
	
	else
	{
	    die $request->exception(400, "Bad request");
	}
    }
    
    # Return all inserted or updated records.
    
    my ($id_string) = join(',', $edt->inserted_keys, $edt->updated_keys);
    
    $request->list_updated_specimens($dbh, $id_string, $edt->key_labels) if $id_string;
}


sub list_updated_specimens {
    
    my ($request, $dbh, $id_list, $label_ref) = @_;
    
    $request->substitute_select( mt => 'ss', cd => 'ss' );
    
    my $tables = $request->tables_hash;
    
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;

    # Determine the fields to be selected.

    my $fields = $request->select_string;
    
    # Determine the necessary joins.
    
    # my ($join_list) = $request->generate_join_list('tsb', $tables);
    
    # Determine which extra tables, if any, must be joined to the query.  Then
    # construct the query.
    
    my $join_list = $request->PB2::SpecimenData::generateJoinList('c', $tables);
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM $TABLE{SPECIMEN_MATRIX} as ss JOIN $TABLE{SPECIMEN_DATA} as sp using (specimen_no)
		LEFT JOIN $TABLE{OCCURRENCE_MATRIX} as o on o.occurrence_no = ss.occurrence_no and o.reid_no = ss.reid_no
		LEFT JOIN $TABLE{COLLECTION_MATRIX} as c on o.collection_no = c.collection_no
		LEFT JOIN $TABLE{AUTHORITY_DATA} as a on a.taxon_no = ss.taxon_no
		$join_list
        WHERE ss.specimen_no in ($id_list)
	GROUP BY ss.specimen_no
	ORDER BY ss.specimen_no
	$limit";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    my $results = $dbh->selectall_arrayref($request->{main_sql}, { Slice => { } });
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
    
    # If we got some results, go through them and substitute in the record labels.
    
    if ( ref $results eq 'ARRAY' && @$results )
    {
	foreach my $r ( @$results )
	{
	    my $keyval = $r->{specimen_no};
	    
	    if ( ref $label_ref eq 'HASH' && $label_ref->{$keyval} )
	    {
		$r->{record_label} = $label_ref->{$keyval};
	    }
	}
	
	$request->list_result($results);
    }
}


sub delete_specimens {
    
    my ($request) = @_;
    
}


sub update_measurements {
    
    my ($request, $arg) = @_;
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single timescale only.
    
    my %allowances = ( IMMEDIATE_MODE => 1 );
    
    $allowances{CREATE} = 1 if $arg && $arg eq 'add';
    
    my $main_params = $request->get_main_params(\%allowances, '1.2:specs:measurement_entry');
    my $perms = $request->require_authentication('MEASUREMENT_DATA');
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->unpack_input_records($main_params, '1.2:specs:addupdate_measurements_body', 'measurement_id');
    
    # If any errors were found in the parameters, stop now and return an HTTP 400 response.
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad request");
    }
    
    # Otherwise, start a new transaction.
    
    my $edt = SpecimenEdit->new($request, $perms, 'MEASUREMENT_DATA', \%allowances);
    
    # Now go through the records and handle each one in turn. This will check every record and
    # queue them up for insertion and/or updating.
    
    foreach my $r (@records)
    {
	$edt->insert_update_record('MEASUREMENT_DATA', $r);
    }
    
    # If no errors have been detected so far, execute the queued actions inside a database
    # transaction. If any errors occur during that process, the transaction will be automatically
    # rolled back. Otherwise, it will be automatically committed.
    
    $edt->commit;
    
    # If any warnings (non-fatal conditions) were detected, add them to the
    # request record so they will be communicated back to the user.
    
    $request->collect_edt_warnings($edt);
    
    # If we completed the procedure without any exceptions, but error conditions were detected
    # nonetheless, these should be reported. In this case, the transaction will have automatically
    # been rolled back.
    
    if ( $edt->errors )
    {
    	$request->collect_edt_errors($edt);
	
	if ( $edt->has_condition_code('E_EXECUTE') )
	{
	    die $request->exception(500, "Internal error");
	}
	
	else
	{
	    die $request->exception(400, "Bad request");
	}
    }

    # Otherwise, we're good to go.

    else
    {
	my $return_what = $request->clean_param('return') || 'specimen';
	my $id_string = '';

	if ( $return_what eq 'updated' )
	{
	    $id_string = join(',', $edt->inserted_keys, $edt->updated_keys);
	}

	elsif ( $return_what eq 'specimen' )
	{
	    $id_string = join(',', $edt->get_attr_keys('updated_specimen'));
	}
	
	$request->list_updated_measurements($dbh, $return_what, $id_string, $edt->key_labels) if $id_string;
    }
}


sub list_updated_measurements {
    
    my ($request, $dbh, $return_type, $id_list, $label_ref) = @_;
    
    $request->substitute_select( mt => 'ss', cd => 'ss' );
    
    my @filters = $return_type eq 'specimen' ? "ms.specimen_no in ($id_list)" : "ms.measurement_no in ($id_list)";
    my $filter_string = join(' and ', @filters);
    
    my $tables = $request->tables_hash;
    
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;

    # Determine the fields to be selected.

    my $fields = $request->select_string;
    
    # Determine the necessary joins.
    
    # my ($join_list) = $request->generate_join_list('tsb', $tables);
    
    # Determine which extra tables, if any, must be joined to the query.  Then
    # construct the query.
    
    my $join_list = $request->PB2::SpecimenData::generateJoinList('c', $tables);
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM $TABLE{MEASUREMENT_DATA} as ms join $TABLE{SPECIMEN_DATA} as sp using (specimen_no)
        WHERE $filter_string
	GROUP BY ms.measurement_no
	ORDER BY ms.specimen_no, ms.measurement_no
	$limit";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    my $results = $dbh->selectall_arrayref($request->{main_sql}, { Slice => { } });
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
    
    # If we got some results, go through them and substitute in the record labels.
    
    if ( ref $results eq 'ARRAY' && @$results )
    {
	foreach my $r ( @$results )
	{
	    my $keyval = $r->{measurement_no};
	    
	    if ( ref $label_ref eq 'HASH' && $label_ref->{$keyval} )
	    {
		$r->{record_label} = $label_ref->{$keyval};
	    }
	}
	
	$request->list_result($results);
    }
}




# sub update_collevents {


# }


# sub delete_collevents {


# }


1;
