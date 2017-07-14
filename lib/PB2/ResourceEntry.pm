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

use TableDefs qw($RESOURCE_DATA $RESOURCE_QUEUE);
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);
use PB2::TableData qw(complete_ruleset);

use EditTransaction;

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::CommonEntry);

our ($RESOURCE_ACTIVE, $RESOURCE_TAGS, $RESOURCE_IDFIELD);

# initialize ( )
# 
# This routine is called by the Web::DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # Value sets for specifying data entry options.
    
    $ds->define_set('1.2:eduresources:status' =>
	{ value => 'active' },
	    "An active resource is one that is visible on the Resources page",
	    "of this website.",
	{ value => 'pending' },
	    "A pending resource is one that is not currently active on the Resources page,",
	    "and has not yet been reviewed for possible activation.",
	{ value => 'inactive' },
	    "An inactive resource is one that has been reviewed, and was not",
	    "chosen for activation.");
    
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
	    "The value of this parameter should be a list of integers, identifying",
	    "the tags/headings with which this resource should be associated. You",
	    "can specify this as either a comma-separated list in a string, or as a",
	    "JSON list of integers.");
    
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
	"the value must match the corresponding column in the C<B<eduresources>> table",
	"in the database.",
	{ allow => '1.2:eduresources:entry' });
    
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
	{ allow => '1.2:eduresources:entry' });
    
    $ds->define_ruleset('1.2:eduresources:delete' =>
	">>The following parameter may be given either in the URL or in",
	"the request body. Either way, you may specify more than one value,",
	"as a comma-separated list.",
	{ param => 'eduresource_id', valid => VALID_IDENTIFIER('EDR'), list => ',' },
	    "The identifier(s) of the resource record(s) to delete.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $RESOURCE_ACTIVE = $ds->config_value('eduresources_active');
    $RESOURCE_TAGS = $ds->config_value('eduresources_tags');
    $RESOURCE_IDFIELD = $ds->config_value('eduresources_idfield') || 'id';
    
    die "You must provide a configuration value for 'eduresources_active' and 'eduresources_tags'"
	unless $RESOURCE_ACTIVE && $RESOURCE_TAGS;
    
    my $dbh = $ds->get_connection;
    
    complete_ruleset($ds, $dbh, '1.2:eduresources:addupdate_body', $RESOURCE_QUEUE);
    complete_ruleset($ds, $dbh, '1.2:eduresources:update_body', $RESOURCE_QUEUE);
}



our (%IGNORE_PARAM) = ( 'allow' => 1, 'return' => 1, 'record_label' => 1 );


sub update_resources {
    
    my ($request, $arg) = @_;
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single record only.
    
    my %conditions;
    
    $conditions{CREATE_RECORDS} = 1 if $arg && $arg eq 'add';
    
    my $main_params = $request->get_main_params(\%conditions);
    my $auth_info = $request->get_auth_info($dbh, $RESOURCE_QUEUE);
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->unpack_input_records($main_params);
    
    # Now go through the records and validate each one in turn.
    
    my %record_activation;
    
    foreach my $r ( @records )
    {
	my $record_label = $r->{record_label} || $r->{eduresource_id} || '';
	my $op = $r->{eduresource_id} ? 'update' : 'add';
	
	# If we are updating an existing record, we need to validate its fields.
	
	if ( my $record_id = $r->{eduresource_id} || $r->{eduresource_no} )
	{
	    $r->{eduresource_no} = $record_id = $request->validate_extident('EDR', $record_id, 'eduresource_id');
	    delete $r->{eduresource_id};
	    
	    # Fetch the current authorization and status information.
	    
	    my ($current_status, $record_authno, $record_entno) = 
		$request->fetch_record_values($dbh, $RESOURCE_QUEUE, "eduresource_no = $record_id", 
					      'status, authorizer_no, enterer_no');
	    
	    # Make sure the record actually exists.
	    
	    unless ( defined $current_status )
	    {
		$request->add_record_error('E_NOT_FOUND', $record_label, "record '$record_id' not found");
		next;
	    }
	    
	    # Then make sure that we have authorization to modify this record.
	    
	    my $role = $request->check_record_auth($dbh, $RESOURCE_QUEUE, $record_id, $record_authno, $record_entno);
	    
	    # If we have the 'admin' role on this record, we can modify the record and we can also
	    # adjust its status.
	    
	    if ( $role eq 'admin' )
	    {
		# If the status is being explicitly set to 'active', then the new record
		# values should be copied into the active resources table. This is a valid
		# operation even if a previous version of the record is already in that table.
		
		if ( $r->{status} && $r->{status} eq 'active' )
		{
		    $record_activation{$record_id} = 'copy';
		}
		
		# Otherwise, if this record has a status that marks it as already having been
		# copied to the active table, then we need to check and see if it is being set
		# to some inactive status. If so, it will need to be removed from the active
		# table.
		
		elsif ( $r->{status} && $r->{status} ne 'changes' )
		{
		    $record_activation{$record_id} = 'delete';
		}
		
		else
		{
		    $r->{status} = 'changes';
		}
		
		# Otherwise, the status can be set or left unchanged according to the value of
		# $r->{status}.
	    }
	    
	    # If we have the 'edit' role on this record, then we can update its values but not its
	    # status. If the current status is 'active', then it will be automatically changed to
	    # 'changes'.
	    
	    elsif ( $role eq 'edit' )
	    {
		if ( $r->{status} )
		{
		    $request->add_record_warning('W_PERM', $record_label, 
				"you do not have permission to change the status of this record");
		    next;
		}
		
		if ( $current_status eq 'active' )
		{
		    $r->{status} = 'changes';
		}
	    }
	    
	    # Otherwise, we do not have permission to edit this record.
	    
	    else
	    {
		$request->add_record_error('E_PERM', $record_label, "you do not have permission to edit this record");
		next;
	    }
	}
	
	# If we do not have a record identifier, then we are adding a new record. This requires
	# different validation checks.
	
	else
	{
	    # Make sure that this operation allows us to create records in the first place.
	    
	    unless ( $conditions{CREATE_RECORDS} )
	    {
		$request->add_record_error('C_CREATE', $record_label, "missing record identifier; this operation cannot create new records");
		next;
	    }
	    
	    # Make sure that we have authorization to add records to this table.
	    
	    my $role = $request->check_record_auth($dbh, $RESOURCE_QUEUE);
	    
	    # If we have 'admin' privileges on the resource queue table, then we can add a new
	    # record with any status we choose. The status will default to 'pending' if not
	    # explicitly set.
	    
	    if ( $role eq 'admin' )
	    {
		$r->{status} ||= 'pending';
	    }
	    
	    # If we have 'post' privileges, we can create a new record. The status will
	    # automatically be set to 'pending', regardless of what is specified in the record.
	    
	    elsif ( $role eq 'post' )
	    {
		$r->{status} = 'pending';
	    }
	    
	    # Otherwise, we have no ability to do anything at all.
	    
	    else
	    {
		$request->add_record_error('E_PERM', undef, 
				    "you do not have permission to add records");
		next;
	    }
	}
	
	# Now validate the fields and construct the lists that will be used to generate an SQL
	# statement to add or update the record.
	
	$request->validate_against_table($dbh, $RESOURCE_QUEUE, $op, $r, $record_label);
    }
    
    # If any errors were found in the parameters, stop now and return an HTTP 400 response.
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad request");
    }
    
    # Now go through and try to actually add or update these records.
    
    my $edt = EditTransaction->new($dbh, { conditions => \%conditions,
					   debug => $request->debug,
					   auth_info => $auth_info });
    
    # $request->check_edt($edt);
    
    my (%updated_records);
    
    try {

	foreach my $r ( @records )
	{
	    my $record_label = $r->{record_label} || $r->{eduresource_no} || '';
	    
	    # If we have a value for eduresource_no, update the corresponding record.
	    
	    if ( my $record_id = $r->{eduresource_no} )
	    {
		$request->do_update($dbh, $RESOURCE_QUEUE, "eduresource_no = $record_id", $r, \%conditions);
		
		$updated_records{$record_id} = 1;
		
		# If this record should be added to or removed from the active table, do so now.
		
		if ( $record_activation{$record_id} )
		{
		    $request->activate_resource($dbh, $r->{eduresource_no}, $record_activation{$record_id});
		}		
	    }
	    
	    else
	    {
		my $new_id = $request->do_add($dbh, $RESOURCE_QUEUE, $r, \%conditions);
		
		$updated_records{$new_id} = 1 if $new_id;
		
		# If this record should be added to the active table, do so now.
		
		if ( $r->{status} eq 'active' )
		{
		    $request->activate_resource($dbh, $new_id, 'copy');
		}
	    }
	}
    }
    
    # If an exception is caught, we roll back the transaction before re-throwing it as an internal
    # error. This will generate an HTTP 500 response.
    
    catch {

	$edt->rollback;
	die $_;
    };
    
    # If any warnings (non-fatal conditions) were detected, add them to the
    # request record so they will be communicated back to the user.
    
    $request->add_edt_warnings($edt);
    
    # If we completed the procedure without any exceptions, but error conditions were detected
    # nonetheless, we also roll back the transaction.
    
    if ( $edt->errors )
    {
	$edt->rollback;
	$request->add_edt_errors($edt);
	die $request->exception(400, "Bad request");
    }
    
    # If the parameter 'strict' was given and warnings were generated, also roll back the
    # transaction.
    
    elsif ( $request->clean_param('strict') && $request->warnings )
    {
	$edt->rollback;
	die $request->exceptions(400, "E_STRICT: warnings were generated");
    }
    
    else
    {
	# If we get here, we're good to go! Yay!!!
	
	$edt->commit;
	
	# Return the indicated information. This will generally be the updated record.
	
	my ($id_string) = join(',', keys %updated_records);
	
	$request->list_updated_resources($dbh, $id_string) if $id_string;
    }
}


sub activate_resource {
    
    my ($request, $dbh, $eduresource_no, $action) = @_;
    
    $dbh ||= $request->get_connection;
    
    my $quoted_id = $dbh->quote($eduresource_no);
    
    unless ( $eduresource_no && $eduresource_no =~ /^\d+$/ )
    {
	die "error activating or inactivating resource: bad resource id";
    }
    
    if ( $action eq 'delete' )
    {
	my $sql = "
		DELETE FROM $RESOURCE_ACTIVE
		WHERE $RESOURCE_IDFIELD = $quoted_id";
	
	print STDERR "$sql\n\n" if $request->debug;
	
	my $result = $dbh->do($sql);
	
	$sql = "	DELETE FROM $RESOURCE_TAGS
		WHERE resource_id = $quoted_id";
	
	print STDERR "$sql\n\n" if $request->debug;
	
	$result = $dbh->do($sql);
		
	my $a = 1;	# we can stop here when debugging
    }
    
    elsif ( $action eq 'copy' )
    {
	my $sql = "
		SELECT e.eduresource_no as id, e.* FROM $RESOURCE_QUEUE as e
		WHERE eduresource_no = $quoted_id";
	
	print STDERR "$sql\n\n" if $request->debug;
	
	my ($r) = $dbh->selectrow_hashref($sql);
	
	$request->validate_limited($dbh, $RESOURCE_ACTIVE, 'replace', $r, $eduresource_no);
	
	$request->do_replace($dbh, $RESOURCE_ACTIVE, $r, { });
	
	$sql =  "	DELETE FROM $RESOURCE_TAGS
		WHERE resource_id = $quoted_id";
	
	print STDERR "$sql\n\n" if $request->debug;
	
	my $result = $dbh->do($sql);
	
	# Then add tags, if any
	
	if ( $r->{tags} )
	{
	    my @tags = split /,\s*/, $r->{tags};
	    my @insert;
	    
	    foreach my $t ( @tags )
	    {
		next unless $t =~ /^\d+$/ && $t;
		
		push @insert, "($eduresource_no, $t)";
	    }
	    
	    if ( @insert )
	    {
		my $insert_str = join(', ', @insert);
		
		$sql = "	INSERT INTO $RESOURCE_TAGS (resource_id, tag_id) VALUES $insert_str";
		
		print STDERR "$sql\n\n" if $request->debug;
		
		my $result = $dbh->do($sql);
		
		my $a = 1;	# we can stop here when debugging
	    }
	}
    }
    
    else
    {
	die "invalid activation action '$action'";
    }
}


sub delete_resources {


}


sub list_updated_resources {
    
    my ($request, $dbh, $list) = @_;
    
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
	SELECT edr.* FROM $RESOURCE_QUEUE as edr
	WHERE edr.eduresource_no in ($list)
	GROUP BY edr.eduresource_no";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}

1;
