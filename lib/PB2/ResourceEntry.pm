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

use EditTransaction;

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::CommonEntry);

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
	{ optional => 'is_active', valid => BOOLEAN_VALUE },
	    "If this parameter is given with a true value, then the resource record",
	    "will be moved to the active list. If given with a false value, then",
	    "the record will be moved to the inactive list. If not specified,",
	    "the record will not be moved.");
    
    $ds->define_ruleset('1.2:eduresources:addupdate' =>
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every resource specified in the body.",
	{ allow => '1.2:eduresources:entry' },
	">>You may include one or more records in the body, in JSON form. The fields",
	"given in the body must match the C<B<eduresources>> table definition in the database.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:eduresources:update' =>
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every resource specified in the body.",
	{ allow => '1.2:eduresources:entry' },
	">>You may include one or more records in the body, in JSON form. The fields",
	"given in the body must match the C<B<eduresources>> table definition in the database.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:eduresources:delete' =>
	">>The following parameter may be given either in the URL or in",
	"the request body. Either way, you may specify more than one value,",
	"as a comma-separated list.",
	{ param => 'eduresource_id', valid => VALID_IDENTIFIER('EDR'), list => ',' },
	    "The identifier(s) of the resource record(s) to delete.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    
}



our (%IGNORE_PARAM) = ( 'allow' => 1, 'return' => 1, 'record_id' => 1 );


sub update_resources {
    
    my ($request, $arg) = @_;
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single record only.
    
    my %conditions;
    
    $conditions{CREATE_RECORDS} = 1 if $arg && $arg eq 'add';
    
    my $main_params = $request->get_main_params(\%conditions);
    my $auth_info = $request->get_auth_info($dbh);
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->unpack_input_records($main_params);
    
    # Now go through the records and validate each one in turn.
    
    foreach my $r ( @records )
    {
	my $record_label = $r->{record_label} || $r->{eduresource_id} || '';
	my $op = $r->{eduresource_id} ? 'update' : 'add';
	
	if ( my $id = $r->{eduresource_id} )
	{
	    $r->{eduresource_no} = $request->validate_extident('EDR', $id, 'eduresource_id');
	    delete $r->{eduresource_id};
	}
	
	elsif ( ! $conditions{CREATE_RECORDS} )
	{
	    my $lstr = $record_label ? " ($record_label)" : "";
	    $request->add_error("C_CREATE$lstr: missing record identifier; this operation cannot create new records");
	    next;
	}
	
	$request->validate_against_table($dbh, $RESOURCE_QUEUE, $op, $r, $record_label);
    }
    
    # If any errors were found in the parameters, stop now and return an HTTP 400 response.
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad request");
    }
    
    # Now go through and try to actually add or update these records.
    
    my $edt = EditTransaction->new($dbh, { debug => $request->debug,
					   auth_info => $auth_info });
    
    # $request->check_edt($edt);
    
    my (%updated_records);
    
    try {

	foreach my $r ( @records )
	{
	    my $record_id = $r->{record_id} || $r->{eduresource_no} || '';
	    
	    if ( $r->{eduresource_no} && $r->{eduresource_no} > 0 )
	    {
		$request->do_update($dbh, $RESOURCE_QUEUE, "eduresource_no = $r->{eduresource_no}", $r, \%conditions);
		
		$updated_records{$r->{eduresource_no}} = 1;
	    }
	    
	    else
	    {
		my $new_id = $request->do_add($dbh, $RESOURCE_QUEUE, $r, \%conditions);
		
		$updated_records{$new_id} = 1 if $new_id;
	    }
	    
	    # Then process all conditions (errors, cautions, warnings) generated by this
	    # operation.
	    
	    foreach my $e ( $edt->conditions )
	    {
		if ( $record_id ne '' )
		{
		    $e =~ s/^(\w+)[:]\s*/$1 ($record_id): /;
		}
		
		if ( $e =~ /^[EC]/ )
		{
		    $request->add_error($e);
		}
		
		else
		{
		    $request->add_warning($e);
		}
	    }
	    
	    $edt->clear_conditions;
	}
    }
    
    # If an exception is caught, we roll back the transaction before re-throwing it as an internal
    # error. This will generate an HTTP 500 response.
    
    catch {

	$edt->rollback;
	die $_;
    };
    
    # If we completed the procedure without any exceptions, but error conditions were detected
    # nonetheless, we also roll back the transaction.
    
    if ( $edt->errors_occurred )
    {
	$edt->rollback;
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
	(SELECT edr.*, 'active' as status FROM $RESOURCE_DATA as edr
        WHERE edr.eduresource_no in ($list)
	GROUP BY edr.eduresource_no)
	UNION
	(SELECT edr.*, 'pending' as status FROM $RESOURCE_QUEUE as edr
	WHERE edr.eduresource_no in ($list)
	GROUP BY edr.eduresource_no)";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}

1;
