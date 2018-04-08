#  
# ResourceEntry
# 
# A role that allows entry and manipulation of records representing educational resources. This is
# a template for general record editing, in situations where the records in a table are
# independent of the rest of the database.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::ResourceEntry;

use ResourceDefs qw($RESOURCE_ACTIVE $RESOURCE_QUEUE $RESOURCE_IMAGES $RESOURCE_TAGS);
use ResourceEdit;

use ExternalIdent qw(generate_identifier VALID_IDENTIFIER);
use TableData qw(complete_ruleset);

use HTTP::Validate qw(:validators);
use File::Temp qw(tempfile);
use Carp qw(carp croak);
use Try::Tiny;
use MIME::Base64;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::Authentication PB2::CommonData PB2::CommonEntry PB2::ResourceData);

our (%TAG_VALUES);


# initialize ( )
# 
# This routine is called by the Web::DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # Value sets for specifying data entry options.
    
    $ds->define_set('1.2:eduresources:allowances' =>
	{ value => 'PROCEED' },
	    "If some but not all of the submitted records contain errors",
	    "then allowing C<B<PROCEED>> will cause any records that can",
	    "be added or updated to proceed. Otherwise, the entire operation",
	    "will be aborted if any errors occur.");
    
    # Rulesets for entering and updating data.
    
    $ds->define_ruleset('1.2:eduresources:entry' =>
	{ param => 'record_label', valid => ANY_VALUE },
	    "This parameter is only necessary in body records, and then only if",
	    "more than one record is included in a given request. This allows",
	    "you to associate any returned error messages with the records that",
	    "generated them. You may provide any non-empty value.",
	{ param => 'eduresource_id', valid => VALID_IDENTIFIER('EDR'), alias => 'id' },
	    "The identifier of the educational resource record to be updated. If it is",
	    "empty, a new record will be created. You can also use the alias B<C<id>>.",
	{ optional => 'status', valid => '1.2:eduresources:status' },
	    "This parameter should only be given if the logged-in user has administrator",
	    "privileges on the educational resources table. It allows the resource to be",
	    "activated or inactivated, controlling whether or not it appears on the",
	    "Resources page of the website. Newly added resources are given the status",
	    "C<B<pending>> by default. If an active resource is later updated, its",
	    "status is automatically changed to C<B<changes>>. If the record's status",
	    "is later set to C<B<active>> once again, the new values will be copied",
	    "over to the table that drives the Resources page. Accepted values for",
	    "this parameter are:",
	{ optional => 'tags', valid => ANY_VALUE },
	    "The value of this parameter should be a list of tag names, identifying",
	    "the tags/headings with which this resource should be associated. You",
	    "can specify this as either a comma-separated list in a string, or as a",
	    "JSON list of strings. Alternatively, you can use the integer identifiers",
	    "corresponding to the tags.");
    
    $ds->define_ruleset('1.2:eduresources:allow' =>
	{ optional => 'allow', valid => '1.2:eduresources:allowances', list => ',' },
	    "Allow the operation to proceed with certain conditions or properties.");
    
    $ds->define_ruleset('1.2:eduresources:addupdate' =>
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every resource specified in the body.",
	{ allow => '1.2:eduresources:entry' },
	{ optional => 'allow', valid => '1.2:eduresources:allowances', list => ',' },
	    "Allow the operation to proceed with certain conditions or properties.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:eduresources:addupdate_body' =>
	">>You may include one or more records in the body of the request, in JSON form.",
	"The body must be either a single JSON object, or an array of objects. The fields",
	"in each object must be as specified below. If no specific documentation is given",
	"the value must match the corresponding column in the C<B<$RESOURCE_QUEUE>> table",
	"in the database.",
	{ allow => '1.2:eduresources:entry' },
	{ optional => 'image_data', valid => ANY_VALUE },
	    "An image to be associated with this record, encoded into base64. The",
	    "data may begin with the HTML prefix C<data:image/[type]; base64,>.");
    
    $ds->define_ruleset('1.2:eduresources:update' =>
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every resource specified in the body.",
	{ allow => '1.2:eduresources:entry' },
	{ optional => 'allow', valid => '1.2:eduresources:allowances', list => ',' },
	    "Allow the operation to proceed with certain conditions or properties.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:eduresources:update_body' =>
	">>You may include one or more records in the body of the request, in JSON form.",
	"The body must be either a single JSON object, or an array of objects. The fields",
	"in each object must be as specified below. If no specific documentation is given",
	"the value must match the corresponding column in the C<B<eduresources>> table",
	"in the database. For this operation, every record must include a value for",
	"B<C<eduresource_id>>.",
	{ allow => '1.2:eduresources:entry' },
	{ optional => 'image_data', valid => ANY_VALUE },
	    "An image to be associated with this record, encoded into base64. The",
	    "data may begin with the HTML prefix C<data:image/[type]; base64,>.");
    
    $ds->define_ruleset('1.2:eduresources:delete' =>
	">>The following parameter may be given either in the URL or in",
	"the request body. Either way, you may specify more than one value,",
	"as a comma-separated list.",
	{ param => 'eduresource_id', valid => VALID_IDENTIFIER('EDR'), list => ',', alias => 'id' },
	    "The identifier(s) of the resource record(s) to delete.",
	{ optional => 'allow', valid => '1.2:eduresources:allowances', list => ',' },
	    "Allow the operation to proceed with certain conditions or properties.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    my $dbh = $ds->get_connection;
    
    ResourceEdit->configure($dbh, Dancer::config);
    
    complete_ruleset($ds, $dbh, '1.2:eduresources:addupdate_body', $RESOURCE_QUEUE);
    complete_ruleset($ds, $dbh, '1.2:eduresources:update_body', $RESOURCE_QUEUE);
}


our (%IGNORE_PARAM) = ( 'allow' => 1, 'return' => 1, 'record_label' => 1 );


sub update_resources {
    
    my ($request, $arg) = @_;
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single record only.
    
    my $allowances = { ALTER_TRAIL => 1 };
    
    $allowances->{CREATE} = 1 if $arg && $arg eq 'add';
    
    my $main_params = $request->get_main_params($allowances);
    my $perms = $request->require_authentication($RESOURCE_QUEUE);
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->unpack_input_records($main_params, '1.2:eduresources:addupdate_body');
    
    if ( $request->errors )
    {
	die $request->exception(400);
    }
    
    # If we get here without any errors being detected so far, create a new ResourceEdit object to
    # handle this operation. ResourceEdit is a subclass of EditTransaction, specialized for this
    # table.
    
    my $edt = ResourceEdit->new($request, $perms, $RESOURCE_QUEUE, $allowances);
    
    # Now go through the records and handle each one in turn. This will check every record and
    # queue them up for insertion and/or updating.
    
    foreach my $r (@records)
    {
	$edt->insert_update_record($RESOURCE_QUEUE, $r);
    }
    
    # If no errors have been detected so far, execute the queued actions inside a database
    # transaction. If any errors occur during that process, the transaction will be automatically
    # rolled back. Otherwise, it will be automatically committed.
    
    $edt->commit;
    
    # Now handle any errors or warnings that may have been generated.
    
    $request->collect_edt_warnings($edt);
    
    if ( $edt->errors )
    {
    	$request->collect_edt_errors($edt);
    	die $request->exception(400, "Bad request");
    }
    
    # Return all inserted or updated records.
    
    my ($id_string) = join(',', $edt->inserted_keys, $edt->updated_keys);
	
    $request->list_updated_resources($dbh, $id_string, $edt->key_labels) if $id_string;
}


sub list_updated_resources {
    
    my ($request, $dbh, $id_list, $label_ref) = @_;
    
    $request->substitute_select( mt => 'edr', cd => 'edr' );
    
    my $tables = $request->tables_hash;
    
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine the necessary joins.
    
    # my ($join_list) = $request->generate_join_list('tsb', $tables);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $calc edr.*, if(edr.status = 'active', act.image, null) as active_image FROM $RESOURCE_QUEUE as edr
		left join $RESOURCE_ACTIVE as act on edr.eduresource_no = act.$ResourceEdit::RESOURCE_IDFIELD
	WHERE edr.eduresource_no in ($id_list)
	GROUP BY edr.eduresource_no";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    my $results = $dbh->selectall_arrayref($request->{main_sql}, { Slice => { } });
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
    
    # If we got some results, go through them and substitute in the record labels.
    
    if ( ref $results eq 'ARRAY' && @$results )
    {
	foreach my $r ( @$results )
	{
	    my $keyval = $r->{eduresource_no};
	    
	    if ( $label_ref && $label_ref->{$keyval} )
	    {
		$r->{record_label} = $label_ref->{$keyval};
	    }
	}
	
	$request->list_result($results);
    }
}



sub delete_resources {

    my ($request) = @_;

    my $dbh = $request->get_connection;

    # Get the resources to delete from the URL paramters. This operation takes no body.

    my (@id_list) = $request->clean_param_list('eduresource_id');
    
    # Check for any allowances.
    
    my $allowances = { MULTI_DELETE => 1 };
    
    if ( my @allowance = $request->clean_param_list('allow') )
    {
	$allowances->{$_} = 1 foreach @allowance;
    }
    
    # Determine our authentication info, and then create an EditTransaction object.
    
    my $perms = $request->require_authentication($RESOURCE_QUEUE);
    
    my $edt = ResourceEdit->new($request, $perms, $RESOURCE_QUEUE, $allowances);

    # Then go through the records and handle each one in turn.
    
    foreach my $id (@id_list)
    {
	$edt->delete_record($RESOURCE_QUEUE, $id);
    }
    
    # If no errors have been detected so far, execute the queued actions inside a database
    # transaction. If any errors occur during that process, the transaction will be automatically
    # rolled back. Otherwise, it will be automatically committed.
    
    $edt->execute;
    
    # Now handle any errors or warnings that may have been generated.
    
    $request->collect_edt_warnings($edt);
    
    if ( $edt->errors )
    {
    	$request->collect_edt_errors($edt);
    	die $request->exception(400, "Bad request");
    }
    
    # Then return one result record for each deleted database record.
    
    my @results;
    
    foreach my $record_id ( $edt->deleted_keys )
    {
	push @results, { eduresource_no => generate_identifier('EDR', $record_id),
			 status => 'deleted' };
    }
    
    $request->{main_result} = \@results;
    $request->{result_count} = scalar(@results);
}

1;
