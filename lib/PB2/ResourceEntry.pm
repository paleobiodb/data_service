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

use HTTP::Validate qw(:validators);

use TableDefs qw($RESOURCE_ACTIVE $RESOURCE_QUEUE $RESOURCE_IMAGES $RESOURCE_TAGS);
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);
use TableData qw(complete_ruleset);
use File::Temp qw(tempfile);

use ResourceEdit;

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
    
    $ds->define_ruleset('1.2:eduresources:addupdate' =>
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every resource specified in the body.",
	{ allow => '1.2:eduresources:entry' },
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
	">>You may include one or more records in the body, in JSON form. The fields",
	"given in the body must match the C<B<eduresources>> table definition in the database.",
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
    
    my $allows = { };
    
    $allows->{CREATE} = 1 if $arg && $arg eq 'add';
    
    my $main_params = $request->get_main_params($allows);
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
    
    my $edt = ResourceEdit->new($request, $perms, $RESOURCE_QUEUE, $allows);
    
    # Now go through the records and handle each one in turn. This will check every record and
    # queue them up for insertion and/or updating.
    
    foreach my $r (@records)
    {
	$edt->insert_update_record($RESOURCE_QUEUE, $r);
    }
    
    # If no errors have been detected so far, execute the queued actions inside a database
    # transaction. If any errors occur during that process, the transaction will be automatically
    # rolled back. Otherwise, it will be automatically committed.
    
    $edt->execute;
    
    # Now handle any errors or warnings that may have been generated.
    
    $request->add_edt_warnings($edt);
    
    if ( $edt->errors )
    {
    	$request->add_edt_errors($edt);
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
	SELECT $calc edr.* FROM $RESOURCE_QUEUE as edr
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


    # my %record_activation;
    # my @good_records;
    
    # foreach my $r ( @records )
    # {
    # 	my $record_label = $r->{record_label} || $r->{eduresource_id} || '';
    # 	my $op = $r->{eduresource_id} ? 'update' : 'add';
	
    # 	# If we are updating an existing record, we need to validate its fields.
	
    # 	if ( my $record_id = $r->{eduresource_id} || $r->{eduresource_no} )
    # 	{
    # 	    $r->{eduresource_no} = $record_id = $request->validate_extident('EDR', $record_id, 'eduresource_id');
    # 	    delete $r->{eduresource_id};
	    
    # 	    # Fetch the current authorization and status information.
	    
    # 	    # my ($current_status, $record_authno, $record_entno, $record_entid) = 
    # 	    # 	$request->fetch_record_values($dbh, $RESOURCE_QUEUE, "eduresource_no = $record_id", 
    # 	    # 				      'status, authorizer_no, enterer_no, enterer_id');
	    
    # 	    # Make sure that we have authorization to modify this record, and that it actually exists.
	    
    # 	    my ($current) = $request->fetch_record($dbh, $RESOURCE_QUEUE, "eduresource_no=$record_id",
    # 						   'eduresource_no, status, authorizer_no, enterer_no, enterer_id');
	    
    # 	    # If we cannot find the record, then add an error to the request and continue on to
    # 	    # the next.
	    
    # 	    unless ( $current )
    # 	    {
    # 		$request->add_record_error('E_NOT_FOUND', $record_label, "record not found");
    # 		next;
    # 	    }

    # 	    # Otherwise, check the permission that the current user has on this record. If they
    # 	    # have the 'admin' permission, they can modify the record and also adjust its status.

    # 	    my $permission = $request->check_record_permission($current, $RESOURCE_QUEUE, 'edit', 'eduresource_no');
	    
    # 	    if ( $permission eq 'admin' )
    # 	    {
    # 		# If the status is being explicitly set to 'active', then the new record
    # 		# values should be copied into the active resources table. This is a valid
    # 		# operation even if a previous version of the record is already in that table.
		
    # 		if ( $r->{status} && $r->{status} eq 'active' )
    # 		{
    # 		    $record_activation{$record_id} = 'copy';
    # 		}
		
    # 		# Otherwise, if this record has a status that marks it as already having been
    # 		# copied to the active table, then we need to check and see if it is being set
    # 		# to some inactive status. If so, it will need to be removed from the active
    # 		# table.
		
    # 		elsif ( $r->{status} && $r->{status} ne 'changes' )
    # 		{
    # 		    $record_activation{$record_id} = 'delete';
    # 		}
		
    # 		else
    # 		{
    # 		    $r->{status} = 'changes';
    # 		}
		
    # 		# Otherwise, the status can be set or left unchanged according to the value of
    # 		# $r->{status}.
    # 	    }
	    
    # 	    # If we have the 'edit' role on this record, then we can update its values but not its
    # 	    # status. If the current status is 'active', then it will be automatically changed to
    # 	    # 'changes'.
	    
    # 	    elsif ( $permission eq 'edit' )
    # 	    {
    # 		if ( $r->{status} )
    # 		{
    # 		    $request->add_record_warning('W_PERM', $record_label, 
    # 				"you do not have permission to change the status of this record");
    # 		    next;
    # 		}
		
    # 		if ( $current->{status} eq 'active' )
    # 		{
    # 		    $r->{status} = 'changes';
    # 		}
    # 	    }
	    
    # 	    # Otherwise, we do not have permission to edit this record.
	    
    # 	    else
    # 	    {
    # 		$request->add_record_error('E_PERM', $record_label, "you do not have permission to edit this record");
    # 		next;
    # 	    }
    # 	}
	
    # 	# If we do not have a record identifier, then we are adding a new record. This requires
    # 	# different validation checks.
	
    # 	else
    # 	{
    # 	    # Make sure that this operation allows us to create records in the first place.
	    
    # 	    unless ( $conditions{CREATE_RECORDS} )
    # 	    {
    # 		$request->add_record_error('C_CREATE', $record_label, "missing record identifier; this operation cannot create new records");
    # 		next;
    # 	    }
	    
    # 	    # Make sure that we have authorization to add records to this table.
	    
    # 	    my $permission = $request->check_table_permission($dbh, $RESOURCE_QUEUE, 'post');
	    
    # 	    # If we have 'admin' privileges on the resource queue table, then we can add a new
    # 	    # record with any status we choose. The status will default to 'pending' if not
    # 	    # explicitly set.
	    
    # 	    if ( $permission eq 'admin' )
    # 	    {
    # 		$r->{status} ||= 'pending';
    # 	    }
	    
    # 	    # If we have 'post' privileges, we can create a new record. The status will
    # 	    # automatically be set to 'pending', regardless of what is specified in the record.
	    
    # 	    elsif ( $permission eq 'post' )
    # 	    {
    # 		$r->{status} = 'pending';
    # 	    }
	    
    # 	    # Otherwise, we have no ability to do anything at all.
	    
    # 	    else
    # 	    {
    # 		$request->add_record_error('E_PERM', undef, 
    # 				    "you do not have permission to add records");
    # 		next;
    # 	    }
    # 	}
	
    # 	# If $r has a 'tags' field, then look up the tag definitions if necessary and translate
    # 	# the value into a list of integers.
	
    # 	if ( my $tag_list = $r->{tags} )
    # 	{
    # 	    $request->cache_tag_values();
	    
    # 	    my @tags = ref $tag_list eq 'ARRAY' ? @$tag_list : split (/\s*,\s*/, $tag_list);
    # 	    my @tag_ids;
	    
    # 	    foreach my $t ( @tags )
    # 	    {
    # 		if ( $t =~ /^\d+$/ )
    # 		{
    # 		    push @tag_ids, $t;
    # 		}
		
    # 		elsif ( $PB2::ResourceData::TAG_VALUE{lc $t} )
    # 		{
    # 		    push @tag_ids, $PB2::ResourceData::TAG_VALUE{lc $t};
    # 		}
		
    # 		else
    # 		{
    # 		    $request->add_record_warning('E_TAG', $record_label, "unknown resource tag '$t'");
    # 		}
    # 	    }
	    
    # 	    $r->{tags} = join(',', @tag_ids);
    # 	}
	
    # 	# Now validate the fields and construct the lists that will be used to generate an SQL
    # 	# statement to add or update the record.
	
    # 	$request->validate_against_table($dbh, $RESOURCE_QUEUE, $op, $r, 'eduresource_no', \%RESOURCE_IGNORE);
	
    # 	push @good_records, $r;
    # }
    
    # # If any errors were found in the parameters, stop now and return an HTTP 400 response.
    
    # if ( $request->errors )
    # {
    # 	die $request->exception(400, "Bad request");
    # }
    
    # # If no good records were found, stop now and return an HTTP 400 response.
    
    # unless ( @good_records )
    # {
    # 	$request->add_error("E_NO_RECORDS: no valid records for add or update");
    # 	die $request->exception(400, "Bad request");
    # }
    
    # # Now go through and try to actually add or update these records.
    
    # my $edt = EditTransaction->new($dbh, { conditions => \%conditions,
    # 					   debug => $request->debug,
    # 					   auth_info => $auth_info });
    
    # # $request->check_edt($edt);
    
    # my (%updated_records);
    
    # try {

    # 	foreach my $r ( @records )
    # 	{
    # 	    my $record_label = $r->{record_label} || $r->{eduresource_no} || '';
	    
    # 	    # If we have a value for eduresource_no, update the corresponding record.
	    
    # 	    if ( my $record_id = $r->{eduresource_no} )
    # 	    {
    # 		$request->do_update($dbh, $RESOURCE_QUEUE, "eduresource_no = $record_id", $r, \%conditions);
		
    # 		$request->store_image($dbh, $record_id, $r) if $r->{image_data};
		
    # 		$updated_records{$record_id} = 1;
		
    # 		# If this record should be added to or removed from the active table, do so now.
		
    # 		if ( $record_activation{$record_id} )
    # 		{
    # 		    $request->activate_resource($dbh, $r->{eduresource_no}, $record_activation{$record_id});
    # 		}		
    # 	    }
	    
    # 	    else
    # 	    {
    # 		my $new_id = $request->do_add($dbh, $RESOURCE_QUEUE, $r, \%conditions);
		
    # 		$request->store_image($dbh, $new_id, $r) if $new_id && $r->{image_data};
		
    # 		$updated_records{$new_id} = 1 if $new_id;
		
    # 		$request->{my_record_label}{$new_id} = $r->{record_label} if $r->{record_label};
		
    # 		# If this record should be added to the active table, do so now.
		
    # 		if ( $r->{status} eq 'active' )
    # 		{
    # 		    $request->activate_resource($dbh, $new_id, 'copy');
    # 		}
    # 	    }
    # 	}
    # }
    
    # # If an exception is caught, we roll back the transaction before re-throwing it as an internal
    # # error. This will generate an HTTP 500 response.
    
    # catch {

    # 	$edt->rollback;
    # 	die $_;
    # };
    
    # # If any warnings (non-fatal conditions) were detected, add them to the
    # # request record so they will be communicated back to the user.
    
    # $request->add_edt_warnings($edt);
    
    # # If we completed the procedure without any exceptions, but error conditions were detected
    # # nonetheless, we also roll back the transaction.
    
    # if ( $edt->errors )
    # {
    # 	$edt->rollback;
    # 	$request->add_edt_errors($edt);
    # 	die $request->exception(400, "Bad request");
    # }
    
    # # If the parameter 'strict' was given and warnings were generated, also roll back the
    # # transaction.
    
    # elsif ( $request->clean_param('strict') && $request->warnings )
    # {
    # 	$edt->rollback;
    # 	die $request->exceptions(400, "E_STRICT: warnings were generated");
    # }
    
    # else
    # {
    # 	# If we get here, we're good to go! Yay!!!
	
    # 	$edt->commit;
	
    # 	# Return the indicated information. This will generally be the updated record.
	
    # 	my ($id_string) = join(',', keys %updated_records);
	
    # 	$request->list_updated_resources($dbh, $id_string) if $id_string;
    # }
# }


sub delete_resources {

    my ($request) = @_;

    my $dbh = $request->get_connection;

    # Get the resources to delete from the URL paramters. This operation takes no body.

    my (@id_list) = $request->clean_param_list('eduresource_id');

    # Determine our authentication info, and then create an EditTransaction object.
    
    my $perms = $request->require_authentication($RESOURCE_QUEUE);
    
    my $edt = ResourceEdit->new($request, $perms, $RESOURCE_QUEUE, { MULTI_DELETE => 1 });

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
    
    $request->add_edt_warnings($edt);
    
    if ( $edt->errors )
    {
    	$request->add_edt_errors($edt);
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
    

#     my %delete_ids;
    
#     foreach my $record_id ( @id_list )
#     {
# 	next unless $record_id =~ /^\d+$/;
	
# 	my ($current) = $request->fetch_record($dbh, $RESOURCE_QUEUE, "eduresource_no=$record_id",
# 					       'eduresource_no, status, authorizer_no, enterer_no, enterer_id');
	
# 	# If we cannot find the record, then add an error to the request and continue on to
# 	# the next.
	
# 	unless ( $current )
# 	{
# 	    $request->add_record_warning('W_NOT_FOUND', $record_id, "record not found");
# 	    next;
# 	}
	
# 	my ($permission) = $request->check_record_permission($current, $RESOURCE_QUEUE, 'edit', 'eduresource_no');
	
# 	# If we have either the 'admin' or 'edit' role on this record, we can delete it.
	
# 	if ( $permission eq 'admin' || $permission eq 'edit' )
# 	{
# 	    $delete_ids{$record_id} = 1;
# 	}
	
# 	# Otherwise, we do not have permission to delete this record.
	
# 	else
# 	{
# 	    $request->add_record_warning('W_PERM', $record_id, "you do not have permission to delete this record");
# 	}
#     }

#     # Unless we have records that we can delete, return immediately.

#     unless ( %delete_ids )
#     {
# 	$request->add_warning('W_NOTHING: nothing to delete');
# 	return;
#     }

#     # Otherwise, we create a new EditTransaction and then try to delete the records.
    
#     my %conditions;
    
#     my $edt = EditTransaction->new($dbh, { conditions => \%conditions,
# 					   debug => $request->debug,
# 					   auth_info => $auth_info });

#     try {

# 	my $id_list = join(',', keys %delete_ids);

# 	my $sql = "
# 		DELETE FROM $RESOURCE_QUEUE
# 		WHERE eduresource_no in ($id_list)";

# 	print STDERR "$sql\n\n" if $request->debug;

# 	my $result = $dbh->do($sql);
	
# 	$sql = "
# 		DELETE FROM $RESOURCE_ACTIVE
# 		WHERE $RESOURCE_IDFIELD in ($id_list)";
	
# 	print STDERR "$sql\n\n" if $request->debug;
	
# 	$result = $dbh->do($sql);
	
# 	$sql = "
# 		DELETE FROM $RESOURCE_TAGS
# 		WHERE resource_id in ($id_list)";
	
# 	print STDERR "$sql\n\n" if $request->debug;
	
# 	$result = $dbh->do($sql);
	
# 	$sql = "
# 		DELETE FROM $RESOURCE_IMAGES
# 		WHERE eduresource_no in ($id_list)";
	
# 	print STDERR "$sql\n\n" if $request->debug;
	
# 	$result = $dbh->do($sql);
#     }
    
#     # If an exception is caught, roll back the transaction before re-throwing it as an internal
#     # error. This will generate an HTTP 500 response.
	
#     catch {

# 	$edt->rollback;
# 	die $_;
#     };

#     # If any warnings (non-fatal conditions) were detected, add them to the
#     # request record so they will be communicated back to the user.
    
#     $request->add_edt_warnings($edt);
    
#     # If we completed the procedure without any exceptions, but error conditions were detected
#     # nonetheless, we also roll back the transaction.
    
#     if ( $edt->errors )
#     {
# 	$edt->rollback;
# 	$request->add_edt_errors($edt);
# 	die $request->exception(400, "Bad request");
#     }
    
#     # If the parameter 'strict' was given and warnings were generated, also roll back the
#     # transaction.
    
#     elsif ( $request->clean_param('strict') && $request->warnings )
#     {
# 	$edt->rollback;
# 	die $request->exceptions(400, "E_STRICT: warnings were generated");
#     }
    
#     else
#     {
# 	# If we get here, we're good to go! Yay!!!
	
# 	$edt->commit;
	
# 	# Return a list of records that were deleted.

# 	my @results;
	




1;
