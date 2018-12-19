#  
# TimescaleData
# 
# A role that returns information from the PaleoDB database about geological timescales
# and time intervals.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::TimescaleEntry;

use HTTP::Validate qw(:validators);

use TableDefs qw(%TABLE);
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);

use TimescaleEdit;

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::Authentication PB2::CommonData PB2::CommonEntry PB2::TimescaleData PB2::ReferenceData);

# initialize ( )
# 
# This routine is called by the Web::DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # Value sets for specifying data entry options.
    
    $ds->define_set('1.2:timescales:conditions' =>
	{ value => 'CREATE_INTERVALS' },
	    "If any interval names are encountered during this operation which",
	    "do not correspond to interval records already in the database, then",
	    "create new interval records rather than blocking the request",
	{ value => 'BREAK_DEPENDENCIES' },
	    "When deleting interval bonundaries, if other boundaries depend on",
	    "the boundaries to be deleted then the dependency is broken.",
	    "If it involved the boundary age, then the dependent",
	    "boundaries are converted to type C<B<absolute>>. If it involved",
	    "color or bibliographic referene, these values become local properties",
	    "of the formerly dependent boundaries. Without this condition, any",
	    "attempt to delete boundaries which have dependents will be blocked.");
    
    $ds->define_set('1.2:timescales:bounds_return' =>
	{ value => 'updated' },
	    "Return just the new or updated records. This is the default when no bounds are specified.",
	{ value => 'full' },
	    "Return the full list of interval boundaries for every updated timescale.",
	    "This is the default when bounds are specified.",
	{ value => 'none' },
	    "Return nothing except the status code and any warnings or",
	    "cautions that were generated.");
        
    # Rulesets for entering and updating data.
    
    $ds->define_ruleset('1.2:timescales:entry' =>
	{ optional => '_label', valid => ANY_VALUE },
	    "You may provide a value for this attribute in any record",
	    "submitted for entry or update. This allows the data service",
	    "to accurately indicate which records generated errors or warnings.",
	    "You may specify any string, but if you submit multiple records in",
	    "one call each record should have a unique value.",
	{ optional => '_operation', valid => ANY_VALUE },
	    "You may provide a value for this attribute in any record",
	    "submitted to this data service. This specifies the operation.",
	    "to be performed using this record, overriding the automatic",
	    "determination. The main use of this field is with the value 'delete',",
	    "which causes this record to be deleted from the database.",
	{ param => 'timescale_id', valid => VALID_IDENTIFIER('TSC'), alias => 'oid' },
	    "The identifier of the timescale to be updated. If it is",
	    "empty, a new timescale will be created.",
	{ param => 'timescale_name', valid => ANY_VALUE, alias => 'nam' },
	    "The name of the timescale.",
	{ optional => 'timescale_type', valid => '1.2:timescales:interval_types', alias => 'typ' },
	    "The type of interval this timescale contains. The value must be one of:",
	{ optional => 'timescale_extent', valid => ANY_VALUE, alias => 'ext' },
	    "The geographic extent over which the timescale is valid, which",
	    "can be any string but should be expressed as an adjective. For",
	    "example, C<North American>. If the interval is part of the",
	    "international chronostratographic system, the value should be",
	    "C<international>. Otherwise, if the interval is valid globally",
	    "then the value should be C<global>.",
	{ optional => 'timescale_taxon', valid => ANY_VALUE, alias => 'txn' },
	    "The taxonomic group with respect to which the timescale is",
	    "defined, if any. This should be expressed as a common name",
	    "rather than a scientific one. Examples: C<conodont>, C<mammal>.",
	{ optional => 'timescale_comments', valid => ANY_VALUE, alias => 'tsc' },
	    "This field can be used to store an arbitrary string of text",
	    "associated with each timescale.",
	{ optional => 'is_visible', valid => BOOLEAN_VALUE, alias => 'vis' },
	    "If set to true, then this timescale will be visible to all database users.",
	{ optional => 'is_enterable', valid => BOOLEAN_VALUE, alias => 'enc' },
	    "If set to true, then this timescale can be used when entering collections.",
	# { optional => 'is_private', valid => BOOLEAN_VALUE },
	#     "If set to true, then this timescale can be modified only by its owner or",
	#     "by someone with administrative privileges.",
	{ optional => 'admin_lock', valid => BOOLEAN_VALUE, alias => 'lck' },
	    "When set to true, then the attributes and boundaries of this timescale",
	    "are locked and cannot be modified until this value is set to false.",
	{ optional => 'priority', valid => INT_VALUE, alias => 'pri' },
	    "This value is used whenever more than one timescale mentions",
	    "a particular interval. The one with the higher value for this",
	    "attribute will be taken to specify the boundaries of the interval.",
	    "The value of this attribute must be an integer from 0-255.",
	    "This value can only be set by a user with administrative privilege on",
	    "the timescale tables.",
	{ optional => 'source_timescale_id', valid => VALID_IDENTIFIER('TSC') },
	    "If a value is specified for this attribute, then any interval boundaries that",
	    "are created in the current timescale and that correspond to boundaries",
	    "in the specified source timescale will be created as dependent boundaries",
	    "that refer to the boundaries in the source timescale.",
	{ optional => 'reference_id', valid => VALID_IDENTIFIER('REF') },
	    "If this parameter is specified, it gives the identifier of a",
	    "bibliographic reference for this timescale.");
    
    $ds->define_ruleset('1.2:bounds:entry' =>
	{ optional => '_label', valid => ANY_VALUE },
	    "You may provide a value for this attribute in any record",
	    "submitted to this data service. This allows the data service",
	    "to accurately indicate which records generated errors or warnings.",
	    "You may specify any string, as long as it is non-empty and unique",
	    "among all of the records in this request.",
	{ optional => '_operation', valid => ANY_VALUE },
	    "You may provide a value for this attribute in any record",
	    "submitted to this data service. This specifies the operation.",
	    "to be performed using this record, overriding the automatic",
	    "determination.",
	{ optional => 'bound_id', valid => VALID_IDENTIFIER('BND'), alias => 'oid' },
	    "The identifier of the boundary to be updated. If empty,",
	    "a new boundary will be created.",
	{ optional => 'timescale_id', valid => VALID_IDENTIFIER('TSC'), alias => 'sid' },
	    "The identifier of the timescale in which bound(s)",
	    "are located. This is optional, but can be used to make",
	    "sure that the proper bounds are being updated.",
	{ optional => 'bound_type', valid => '1.2:timescales:bound_types', alias => 'btp' },
	    "The bound type, which must be one of the following:",
	{ optional => 'interval_type', valid => '1.2:timescales:interval_types', alias => 'typ' },
	    "The interval type. If not given, it defaults to the type specified",
	    "in the timescale. The value muse be one of:",
	{ optional => 'age', valid => \&valid_age },
	    "The age of this boundary, in Ma",
	{ optional => 'age_error', valid => \&valid_age, alias => 'ger' },
	    "The uncertainty in the age, in Ma",
	{ optional => 'interval_name', valid => ANY_VALUE, alias => 'inm' },
	    "The name of the interval lying B<above> this boundary. If this",
	    "field is blank, then the boundary is either a top boundary with",
	    "nothing above it or else the interval above it represents a hole in",
	    "this timescale.",
	{ optional => 'top_id', valid => VALID_IDENTIFIER('BND'), alias => 'uid' },
	    "If this value is specified, then the interval of which this bound is",
	    "the bottom end will be taken to end at the specified bound. Otherwise,",
	    "it will be taken to end at the next bound up in order by age.",
	{ optional => 'base_id', valid => VALID_IDENTIFIER('BND'), alias => 'bid' },
	    "If the B<C<bound_type>> is either C<B<same>> or C<B<fraction>>,",
	    "then you must also specify a reference bound using this parameter.",
	{ optional => 'color_id', valid => VALID_IDENTIFIER('BND'), alias => 'cid' },
	    "If this value is non-empty, then the color for the current boundary",
	    "will be taken from the boundary identified by this value.",
	{ optional => 'range_id', valid => VALID_IDENTIFIER('BND'), alias => 'tid' },
	    "If the B<C<bound_type>> is C<B<fraction>>, then you must specify a",
	    "second reference bound using this parameter. The value of B<C<offset>>",
	    "is then taken to indicate a fraction of the difference between the",
	    "ages of the two reference bounds.",
	{ optional => 'fraction', valid => \&valid_age, alias => 'frc' },
	    "If the boundary type is C<B<fraction>>, then the age of this boundary",
	    "is derived as the specified fraction of the difference between",
	    "the base and range boundaries.",
	{ optional => 'color', valid => ANY_VALUE, alias => 'col' },
	    "If this parameter is specified, then it gives a color in which",
	    "to display the upper interval.",
	{ optional => 'reference_id', valid => VALID_IDENTIFIER('REF') },
	    "If this parameter is specified, it gives the identifier of a",
	    "bibliographic reference for this boundary. If not specified,",
	    "the reference is taken from the boundary specified by B<C<refsource_id>>,",
	    "or if that is empty, from the reference for this timescale.");
    
    $ds->define_ruleset('1.2:timescales:op_mod' =>
	">>The following parameters affect how this operation is carried out:",
	{ optional => 'allow', valid => '1.2:timescales:conditions' },
	    "This parameter specifies a list of actions that will",
	    "be allowed to occur during processing of this request, and",
	    "not block it from completing. B<Important:> for interactive applications,",
	    "it is best to allow the request to block, get confirmation from",
	    "the user for each flagged condition, and if confirmed then repeat the request",
	    "with these specific actions allowed using this parameter. Accepted",
	    "values include:",
	{ optional => 'cleanup', valid => 'FLAG_VALUE' },
	    "If this parameter is given along with at least one bound",
	    "record in the request body, then for each timescale for which",
	    "one or more bound addition or updates was specified, all bound",
	    "records not added or updated during this operation will be",
	    "deleted.");
    
    # $ds->define_ruleset('1.2:timescales:ret_mod' =>
    # 	">>The following parameters specify what should be returned from this",
    # 	"operation:",
    # 	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_basic' },
    # 	{ allow => '1.2:special_params' },
    # 	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:timescales:ret_mod' =>
	">>The following parameters specify what should be returned from this",
	"operation:",
	{ optional => 'return', valid => '1.2:timescales:bounds_return' },
	    "This parameter specifies what records should be returned from the",
	    "update operation. You can choose to return just the updated records",
	    "all records associated with any updated timescales, or nothing at all.",
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_bound' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:timescales:addupdate' =>
	{ allow => '1.2:timescales:op_mod' }, 
	{ allow => '1.2:timescales:ret_mod' },
       	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:timescales:delete' =>
	{ allow => '1.2:timescales:op_mod' }, 
	">>The following parameter may be given either in the URL or in",
	"the request body. Either way, you may specify more than one value,",
	"as a comma-separated list.",
	{ param => 'timescale_id', valid => VALID_IDENTIFIER('TSC'), list => ',', alias => 'id' },
	    "The identifier(s) of the timescale(s) to delete.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    # $ds->define_ruleset('1.2:bounds:addupdate' =>
    # 	{ allow => '1.2:timescales:op_mod' }, 
    # 	">>The following parameters may be given either in the URL or in",
    # 	"the request body, or some in either place. If they are given",
    # 	"in the URL, they apply to every boundary specified in the body.",
    # 	{ allow => '1.2:bounds:entry' },
    # 	{ allow => '1.2:bounds:ret_mod' });
    
    # $ds->define_ruleset('1.2:bounds:update' => 
    # 	">>The following parameters may be given either in the URL or in",
    # 	"the request body, or some in either place. If they are given",
    # 	"in the URL, they apply to every bound specified in the body.",
    # 	{ allow => '1.2:bounds:entry' },
    # 	{ allow => '1.2:bounds:ret_mod' });

    $ds->define_ruleset('1.2:bounds:delete' =>
	{ allow => '1.2:timescales:op_mod' }, 
	">>The following parameter may be given either in the URL or in",
	"the request body. Either way, you may specify more than one value,",
	"as a comma-separated list.",
	{ param => 'timescale_id', valid => VALID_IDENTIFIER('TSC') },
	    "The identifier of a timescale. If specified, all bounds",
	    "from this timescale will be deleted.",
	{ param => 'bound_id', valid => VALID_IDENTIFIER('BND'), list => ',', alias => 'id' },
	    "The identifier(s) of the interval boundary or boundaries to delete.",
	{ at_most_one => ['timescale_id', 'bound_id'] },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");

    $ds->define_ruleset('1.2:intervals:delete' =>
	">>The following parameter may be given either in the URL or in",
	"the request body. Either way, you may specify more than one value,",
	"as a comma-separated list.",
	{ param => 'interval_id', valid => VALID_IDENTIFIER('INT'), list => ',', alias => 'id' },
	    "The identifier(s) of the interval(s) to delete.",
	{ param => 'interval_name', valid => ANY_VALUE, list => ',' },
	    "The names(s) of the interval(s) delete.",
	{ at_most_one => ['interval_id', 'interval_name'] },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
	  
}



our (%IGNORE_PARAM) = ( 'allow' => 1, 'return' => 1, 'record_label' => 1 );


sub update_records {

    my ($request, $arg) = @_;
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single timescale only.
    
    my %allowances = ( CREATE => 1, IMMEDIATE_MODE => 1 );
    
    my $main_params = $request->get_main_params(\%allowances);
    my $perms = $request->require_authentication('TIMESCALE_DATA');
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->unpack_input_records({ },
						   ['1.2:bounds:entry', 'bound_id', 'bound_type'],
						   ['1.2:bounds:entry', 'bound_id', 'btp'],
						   ['1.2:timescales:entry', 'timescale_id', 'DEFAULT']);
    
    # If any errors were found in the parameters, stop now and return an HTTP 400 response.
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad request");
    }
    
    # Now go through and try to actually execute the operation specified by each input record. This
    # needs to be inside a transaction, so that if any errors are generated the entire update can
    # be aborted. We start by creating a new TimescaleEdit object, which automatically starts a
    # transaction. This object will also keep track of any errors or other conditions that are
    # generated by any of the update operations.
    
    my $edt = TimescaleEdit->new($request, $perms, 'TIMESCALE_DATA', \%allowances);
    
    # Now go through the records and handle each one in turn. This will check every record and
    # queue them up for insertion and/or updating.
    
    my ($timescales_deleted, $bounds_deleted);
    
    foreach my $r (@records)
    {
	if ( exists $r->{bound_type} || exists $r->{bound_no} || exists $r->{bound_id} )
	{
	    $edt->process_record('TIMESCALE_BOUNDS', $r);
	    $bounds_deleted = 1 if $r->{_operation} && $r->{_operation} eq 'delete';
	}
	
	elsif ( exists $r->{timescale_name} || exists $r->{timescale_no} || exists $r->{timescale_id} )
	{
	    $edt->process_record('TIMESCALE_DATA', $r);
	    $timescales_deleted = 1 if $r->{_operation} && $r->{_operation} eq 'delete';
	}
	
	else
	{
	    $edt->bad_record('TIMESCALE_DATA', $r);
	}
    }

    # Figure out the list of timescales that were referred to in any of the bound records.
    
    my $bound_timescale_list = join(',', $edt->superior_keys('TIMESCALE_BOUNDS'));
    
    if ( $bound_timescale_list && $request->clean_param('cleanup') )
    {
	$edt->delete_cleanup('TIMESCALE_BOUNDS', "timescale_no in ($bound_timescale_list)");
    }
    
    # If no errors have been detected so far, execute the queued actions inside a database
    # transaction. If any errors occur during that process, the transaction will be automatically
    # rolled back. Otherwise, it will be automatically committed.
    
    $edt->commit;
    
    # If any warnings (non-fatal conditions) were detected, add them to the
    # request record so they will be communicated back to the user.
    
    $request->collect_edt_warnings($edt);
    
    # If we completed the procedure without any exceptions, but error conditions were detected
    # nonetheless, we also roll back the transaction.
    
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
    
    $request->extid_check;
    
    # my ($id_string) = join(',', $edt->inserted_keys, $edt->updated_keys);
    
    # $request->list_timescales_after_update($dbh, $edt->key_labels, $id_string) if $id_string;
    
    my (@results);
    
    my $return = $request->clean_param('return') || '';
    
    if ( my $key_list = join(',', $edt->inserted_keys('TIMESCALE_DATA'), $edt->updated_keys('TIMESCALE_DATA'),
			    $edt->replaced_keys('TIMESCALE_DATA')) )
    {
	push @results, $request->list_timescales_after_update($dbh, $key_list, $edt->key_labels('TIMESCALE_DATA'))
	    unless $return eq 'none';
    }

    if ( $timescales_deleted )
    {
	my @keys = $edt->deleted_keys('TIMESCALE_DATA');
	push @results, $request->list_deleted_records('timescales', \@keys);
    }
    
    # if ( $id_string = join(',', $edt->inserted_keys('TIMESCALE_BOUNDS'), $edt->updated_keys('TIMESCALE_BOUNDS')) )

    if ( $bound_timescale_list && $return ne 'none' )
    {
	if ( $return eq 'updated' )
	{
	    my $key_list = join(',', $edt->inserted_keys('TIMESCALE_BOUNDS'), $edt->updated_keys('TIMESCALE_BOUNDS'));
	    
	    push @results, $request->list_bounds_after_update($dbh, 'updated', $key_list,
							      $edt->key_labels('TIMESCALE_BOUNDS'));
	}
	
	else
	{
	    push @results, $request->list_bounds_after_update($dbh, 'full', $bound_timescale_list,
							      $edt->key_labels('TIMESCALE_BOUNDS'));
	}
    }
    
    if ( $bounds_deleted )
    {
	my @keys = $edt->deleted_keys('TIMESCALE_BOUNDS');
	push @results, $request->list_deleted_records('bounds', \@keys);
    }
    
    $request->list_result(\@results);
}


sub delete_records {
    
    my ($request, $arg) = @_;
    
    my $dbh = $request->get_connection;
    
    # Get the resources to delete from the URL paramters. This operation takes no body.

    my (@id_list) = $request->clean_param_list('timescale_id');
    
    # First get the parameters from the URL, the permissions, and create a transaction object.
    
    my %allowances;
    
    my $main_params = $request->get_main_params(\%allowances);
    my $perms = $request->require_authentication('TIMESCALE_DATA');
    
    my $edt = TimescaleEdit->new($request, $perms, 'TIMESCALE_DATA', \%allowances);

    my $table;
    
    if ( $arg eq 'timescales' )
    {
	$table = 'TIMESCALE_DATA';
    }

    elsif ( $arg eq 'bounds' )
    {
	$table = 'TIMESCALE_BOUNDS';
    }

    else
    {
	print STDERR "ERROR: invalid argument to 'delete_records'\n";
	die $request->exception(500, "Internal error");
    }
    
    # Then go through the records and handle each one in turn.
    
    foreach my $id (@id_list)
    {
	$edt->delete_record($table, $id);
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

	if ( $edt->has_condition_code('E_EXECUTE') )
	{
	    die $request->exception(500, "Internal error");
	}
	
	else
	{
	    die $request->exception(400, "Bad request");
	}
    }
    
    # Then return one result record for each deleted database record.

    my @keys = $edt->deleted_keys($table);
    my @results = $request->list_deleted_records($arg, \@keys);

    $request->list_result(\@results);
}


# sub update_bounds {
    
#     my ($request, $arg) = @_;
    
#     my $dbh = $request->get_connection;
    
#     # First get the parameters from the URL, and/or from the body if it is from a web form. In the
#     # latter case, it will necessarily specify a single boundary only.
    
#     my %conditions;
    
#     $conditions{CREATE_RECORDS} = 1 if $arg && ($arg eq 'add' or $arg eq 'replace');
    
#     my $main_params = $request->get_main_params(\%conditions, '1.2:timescales:entry');
#     my $auth_info = $request->get_auth_info($dbh);

#     # foreach my $k ( @request_keys )
#     # {
#     # 	my @list = $request->clean_param_list($k);
	
#     # 	if ( $k eq 'show' || $k eq 'return' )
#     # 	{
#     # 	    next;
#     # 	}
	
#     # 	elsif ( $k eq 'allow' )
#     # 	{
#     # 	    $conditions{$_} = 1 foreach @list;
#     # 	}
	
#     # 	elsif ( @list == 1 )
#     # 	{
#     # 	    $main_params{$k} = $list[0];
#     # 	}
	
#     # 	else {
#     # 	    $main_params{$k} = \@list;
#     # 	}
#     # }
    
#     # Then decode the body, and extract parameters from it. If an error occured, return an
#     # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
#     my (@records) = $request->unpack_input_records($main_params, '1.2:bounds:entry', 'bound_id');
    
#     # my ($body, $error) = $request->decode_body;
    
#     # if ( $error )
#     # {
#     # 	die $request->exception(400, "E_REQUEST_BODY: Badly formatted request body: $error");
#     # }
    
#     # if ( ref $body eq 'HASH' && ref $body->{all} eq 'HASH' )
#     # {
#     # 	foreach my $k ( keys %{$body->{all}} )
#     # 	{
#     # 	    $main_params{$k} = $body->{all}{$k};
#     # 	}
	
#     # 	my $result = $request->validate_params('1.2:bounds:entry', \%main_params);
	
#     # 	if ( $result->errors )
#     # 	{
#     # 	    foreach my $e ( $result->errors )
#     # 	    {
#     # 		$request->add_error("E_PARAM: $e");
#     # 	    }
	    
#     # 	    foreach my $w ( $result->warnings )
#     # 	    {
#     # 		$request->add_warning("W_PARAM: $w");
#     # 	    }
	    
#     # 	    die $request->exception(400, "Invalid request");
#     # 	}
#     # }
    
#     # # Then look for a list of records under the key 'records'. Or if the body decodes to a
#     # # top-level array then assume each entry in the array is a record.
    
#     # my $record_list;
    
#     # if ( ref $body eq 'ARRAY' )
#     # {
#     # 	$record_list = $body;
#     # }
    
#     # elsif ( ref $body eq 'HASH' && ref $body->{records} eq 'ARRAY' )
#     # {
#     # 	$record_list = $body->{records};
#     # }
    
#     # elsif ( defined $body && $body ne '' && ref $body ne 'HASH' )
#     # {
#     # 	$request->add_error("E_BODY: Badly formatted request body: must be a hash or an array");
#     # 	die $request->exception(400, "Invalid request");
#     # }
    
#     # elsif ( $main_params{bound_id} && $main_params{bound_id} > 0 )
#     # {
#     # 	$record_list = [ { bound_id => $main_params{bound_id}, record_id => $main_params{record_id} } ];
#     # }
    
#     # else
#     # {
#     # 	die $request->exception(400, "E_NO_UPDATE: no record to update");
#     # }
    
#     # Now go through the records and validate each one in turn.
    
#     # my %record_id;
    
#     # foreach my $r ( @$record_list )
#     # {
#     # 	my $record_id = $r->{record_label} || $r->{bound_id} || '';
	
#     # 	foreach my $k ( keys %main_params )
#     # 	{
#     # 	    $r->{$k} = $main_params{$k} unless defined $r->{$k} || $IGNORE_PARAM{$k};
#     # 	}
	
#     # 	my $result = $request->validate_params('1.2:bounds:entry', $r);
	
#     # 	foreach my $e ( $request->errors )
#     # 	{
#     # 	    my $msg = $record_id ? "E_PARAM ($record_id): $e" : "E_PARAM: $e";
#     # 	    $request->add_error($msg);
#     # 	}
#     # }
    
#     # If any errors were found in the parameters, stop now and return an HTTP 400 response.
    
#     if ( $request->errors )
#     {
# 	die $request->exception(400, "Bad request");
#     }
    
#     # If no input records were found, return a warning.
    
#     elsif ( @records == 0 )
#     {
# 	$request->add_warning("W_EMPTY: no input records were found");
# 	return;
#     }
    
#     # Now go through and try to actually update each of the bounds. This needs to be inside a
#     # transaction, so that if any errors are generated the entire update can be aborted. We start
#     # by creating a new TimescaleEdit object, which automatically starts a transaction. This
#     # object will also keep track of any errors or other conditions that are generated by any of
#     # the update operations.
    
#     my $edt = TimescaleEdit->new($dbh, { conditions => \%conditions,
# 					 debug => $request->debug,
# 					 auth_info => $auth_info });
    
#     try {

# 	foreach my $r ( @records )
# 	{
# 	    my $record_id = $r->{record_id} || $r->{bound_id} || '';
	    
# 	    if ( $r->{bound_id} )
# 	    {
# 		print STDERR "UPDATING record '$record_id'\n" if $request->debug;
		
# 		$edt->update_boundary($r);
# 	    }
	    
# 	    else
# 	    {
# 		print STDERR "ADDING record '$record_id'\n" if $request->debug;
		
# 		$edt->add_boundary($r);
# 	    }
	    
# 	    # # Then process all conditions (errors, cautions, warnings) generated by this
# 	    # # operation.
	    
# 	    # foreach my $e ( $edt->conditions )
# 	    # {
# 	    # 	if ( $record_id ne '' )
# 	    # 	{
# 	    # 	    $e =~ s/^(\w+)[:]\s*/$1 ($record_id): /;
# 	    # 	}
		
# 	    # 	if ( $e =~ /^[EC]/ )
# 	    # 	{
# 	    # 	    $request->add_error($e);
# 	    # 	}
		
# 	    # 	else
# 	    # 	{
# 	    # 	    $request->add_warning($e);
# 	    # 	}
# 	    # }
	    
# 	    # $edt->clear_conditions;
# 	}
	
# 	# If this routine was called as 'bounds/replace', then we need to delete all bounds from
# 	# updated timescales that were not explicitly added or updated by this operation.
	
# 	if ( $arg && $arg eq 'replace' )
# 	{
# 	    foreach my $timescale_id ( $edt->timescales_updated )
# 	    {
# 		$edt->delete_boundary('unupdated', $timescale_id);
		
# 		# foreach my $e ( $edt->conditions )
# 		# {
# 		#     $e =~ s/^(\w+)[:]\s*/$1 ($timescale_id): /;
		    
# 		#     if ( $e =~ /^[EC]/ )
# 		#     {
# 		# 	$request->add_error($e);
# 		#     }
		    
# 		#     else
# 		#     {
# 		# 	$request->add_warning($e);
# 		#     }
# 		# }
# 	    }
	    
# 	    # foreach my $t ( @timescale_list )
# 	    # {
# 	    # 	$edt->delete_boundary( { timescale_id => $t, un_updated => 1 } ) if $t;
# 	    # }
# 	}
	
# 	# Now we need to recompute the attributes of any updated boundaries plus any boundaries
# 	# that depend on them, update the min and max ages of the associated timescales, and then
# 	# clear all is_updated flags.
	
# 	$edt->complete_bound_updates;
#     }
	
#     # If an exception is caught, we roll back the transaction before re-throwing it as an internal
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
    
#     # If we get here, we're good to go! Yay!!!
    
#     else
#     {
# 	$edt->commit;
	
# 	# Return the indicated information. This will generally be one or more boundary records.
	
# 	my $return_what = $request->clean_param('return') || 'timescale';
# 	my $list = '';
	
# 	if ( $return_what eq 'timescale' )
# 	{
# 	    $list = join(',', $edt->timescales_updated);
# 	}
	
# 	elsif ( $return_what eq 'updated' )
# 	{
# 	    $list = join(',', $edt->bounds_updated);
# 	}
	
# 	$request->list_bounds_for_update($dbh, $return_what, $list) if $list;
#     }
# }


# sub delete_bounds {


# }


# list_timescales_after_update ( dbh, key_list, label_ref )
#
# Return a list of all timescale records that were added or updated by the current operation. The
# $key_list argument must contain a list of all inserted, deleted, and replaced keys. If
# $label_ref is specified, it must be a hash mapping keys to record labels.

sub list_timescales_after_update {
    
    my ($request, $dbh, $key_list, $label_ref) = @_;
    
    my $tables = $request->tables_hash;
    
    $request->substitute_select( cd => 'ts' );
    
    # If a query limit has been specified, return if the
    # limit is 0. That would be another way of specifying a return type of 'none'.
    
    my $limit = $request->sql_limit_clause(1);
    
    return if defined $limit && $limit eq '0';
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $request->select_string;
    
    # Determine the order in which the results should be returned.
    
    my $order_expr = 'ORDER BY ts.timescale_no';
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list('ts', $tables);
    
    # Then query for timescales.
    
    $request->{main_sql} = "
	SELECT $fields
	FROM $TABLE{TIMESCALE_DATA} as ts $join_list
        WHERE timescale_no in ($key_list)
	GROUP BY ts.timescale_no $order_expr";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    my $results = $dbh->selectall_arrayref($request->{main_sql}, { Slice => { } });
    
    # Now add the labels to the return records.
    
    return () unless ref $results eq 'ARRAY' && @$results;
    
    if ( ref $label_ref eq 'HASH' )
    {
	foreach my $r ( @$results )
	{
	    if ( my $keyval = $r->{timescale_no} )
	    {
		$r->{_label} = $label_ref->{$keyval} if $label_ref->{$keyval};
	    }
	}
    }

    return @$results;
}


# list_bounds_after_update ( dbh, return_type, key_list, label_ref )
#
# Return a list of bound records to be returned by an addupdate operation. The $key_list argument
# must contain a list of all inserted, deleted, and replaced keys. The $return_type argument must
# be either 'full' or 'updated'. In the former case, $key_list must be a list of keys indicating
# the timescales that were touched by this operation. All current bounds from these timescales
# will be returned. In the latter case, $key_list must be a list of all inserted, updated, and
# replaced bound keys. If $label_ref is specified, it must be a hash mapping keys to record
# labels.

sub list_bounds_after_update {
    
    my ($request, $dbh, $return_type, $key_list, $label_ref) = @_;
    
    my $tables = $request->tables_hash;
    
    $request->substitute_select( cd => 'ts' );
    
    my $filter_string = $return_type eq 'full' ? "tsb.timescale_no in ($key_list)"
					       : "tsb.bound_no in ($key_list)";
    
    # If a query limit has been specified, return if the
    # limit is 0. That would be another way of specifying a return type of 'none'.
    
    my $limit = $request->sql_limit_clause(1);
    
    return if defined $limit && $limit eq '0';
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = join(',', @{$request->ds->{block}{'1.2:timescales:bound'}{output_list}[0]{select}});

    $tables->{tsi} = 1;
    $tables->{tsb} = 1;
    $tables->{ts} = 1;
    
    # Determine the order in which the results should be returned.
    
    my $order_expr = 'ORDER BY tsb.timescale_no, tsb.age';
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list('tsb', $tables);
    
    # Then query for bounds.
    
    $request->{main_sql} = "
	SELECT $fields
	FROM $TABLE{TIMESCALE_BOUNDS} as tsb $join_list
        WHERE $filter_string
	GROUP BY tsb.bound_no $order_expr";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    
    my $results = $dbh->selectall_arrayref($request->{main_sql}, { Slice => { } });
    
    # Now add the labels to the return records, and select the proper output block.
    
    return () unless ref $results eq 'ARRAY' && @$results;
    
    if ( ref $label_ref eq 'HASH' )
    {
	foreach my $r ( @$results )
	{
	    if ( my $keyval = $r->{bound_no} )
	    {
		$r->{_label} = $label_ref->{$keyval} if $label_ref->{$keyval};
	    }
	    
	    $request->select_output_block($r, '1.2:timescales:bound');
	}
    }
    
    return @$results;
}


# list_deleted_records ( record_type, key_list )
# 
# Return a list of hashes representing deleted records. The $key_list argument must be a list of
# deleted keys of the appropriate record type. If $label_ref is specified, it must be a hash mapping keys to record
# labels.

sub list_deleted_records {
    
    my ($request, $record_type, $key_list, $label_ref) = @_;

    my @result;
    
    foreach my $keyval ( @$key_list )
    {
	my $record;
	
	if ( $record_type eq 'bounds' )
	{
	    $record = { bound_no => $keyval, status => 'deleted' };
	}
	
	else
	{
	    $record = { timescale_no => $keyval, status => 'deleted' };
	}
	
	$record->{_label} = $label_ref->{$keyval} if ref $label_ref eq 'HASH' && $label_ref->{$keyval};
	push @result, $record;
    }

    return @result;
}


# valid_age ( value )
# 
# This routine acts as a validator for age values. We cannot use the ordinary DECI_VALUE
# validator, because it chops off trailing zeros.

sub valid_age {
    
    my ($value) = @_;
    
    unless ( $value =~ qr{ ^ (?: \d+ | \d+ [.] \d* | [.] \d+ ) $ }xs )
    {
	return { error => "bad value '$value' for {param}: must be a decimal number" };
    }
    
    return { value => $value };
}


1;
