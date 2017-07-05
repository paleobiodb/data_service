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

use TableDefs qw($TIMESCALE_DATA $TIMESCALE_INTS $TIMESCALE_BOUNDS);
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);

use TimescaleEdit;

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::CommonEntry PB2::TimescaleData PB2::ReferenceData);

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
	    "Return just the new or updated interval boundary records.",
	{ value => 'timescale' },
	    "Return the full list of interval boundaries for every updated timescale.",
	    "This is the default.",
	{ value => 'none' },
	    "Return nothing except the status code and any warnings or",
	    "cautions that were generated.");
    
    # Rulesets for entering and updating data.
    
    $ds->define_ruleset('1.2:timescales:entry' =>
	{ optional => 'record_label', valid => ANY_VALUE },
	    "You may provide a value for this attribute in any record",
	    "submitted for entry or update. This allows the data service",
	    "to accurately indicate which records generated errors or warnings.",
	    "You may specify any string, but if you submit multiple records in",
	    "one call each record should have a unique value.",
	{ param => 'timescale_id', valid => VALID_IDENTIFIER('TSC') },
	    "The identifier of the timescale to be updated. If it is",
	    "empty, a new timescale will be created.",
	{ param => 'timescale_name', valid => ANY_VALUE },
	    "The name of the timescale.",
	{ optional => 'timescale_type', valid => '1.2:timescales:types' },
	    "The type of intervals this timescale contains. The value muse be one of:",
	{ optional => 'timescale_extent', valid => ANY_VALUE },
	    "The geographic extent over which the timescale is valid, which",
	    "can be any string but should be expressed as an adjective. For",
	    "example, C<North American>. If the interval is part of the",
	    "international chronostratographic system, the value should be",
	    "C<international>. Otherwise, if the interval is valid globally",
	    "then the value should be C<global>.",
	{ optional => 'timescale_taxon', valid => ANY_VALUE },
	    "The taxonomic group with respect to which the timescale is",
	    "defined, if any. This should be expressed as a common name",
	    "rather than a scientific one. Examples: C<conodont>, C<mammal>.",
	{ optional => 'is_active', valid => BOOLEAN_VALUE },
	    "If set to true, then this timescale will be visible to all database",
	    "users and will be available for use in entering and downloading data.",
	{ optional => 'authority_level', valid => POS_VALUE },
	    "This value is used whenever more than one timescale mentions",
	    "a particular interval. The one with the higher value for this",
	    "attribute will be taken to specify the boundaries of the interval.",
	    "The value of this attribute must be an integer from 0-255.",
	{ optional => 'source_timescale_id', valid => VALID_IDENTIFIER('TSC') },
	    "If a value is specified for this attribute, then any interval boundaries that",
	    "are created in the current timescale and that correspond to boundaries",
	    "in the specified source timescale will be created as dependent boundaries",
	    "that refer to the boundaries in the source timescale.",
	{ optional => 'reference_id', valid => VALID_IDENTIFIER('REF') },
	    "If this parameter is specified, it gives the identifier of a",
	    "bibliographic reference for this timescale.");
    
    $ds->define_ruleset('1.2:bounds:entry' =>
	{ optional => 'record_label', valid => ANY_VALUE },
	    "You may provide a value for this attribute in any record",
	    "submitted to this data service. This allows the data service",
	    "to accurately indicate which records generated errors or warnings.",
	    "You may specify any string, as long as it is non-empty and unique",
	    "among all of the records in this request.",
	{ optional => 'bound_id', valid => VALID_IDENTIFIER('BND') },
	    "The identifier of the boundary to be updated. If empty,",
	    "a new boundary will be created.",
	{ optional => 'timescale_id', valid => VALID_IDENTIFIER('TSC') },
	    "The identifier of the timescale in which bound(s)",
	    "are located. This is optional, but can be used to make",
	    "sure that the proper bounds are being updated.",
	{ optional => 'bound_type', valid => '1.2:timescales:bound_types' },
	    "The bound type, which must be one of the following:",
	{ optional => 'interval_type', valid => '1.2:timescales:types' },
	    "The interval type. If not given, it defaults to the type specified",
	    "in the timescale. The value muse be one of:",
	{ optional => 'interval_extent', valid => ANY_VALUE },
	    "The geographic extent over which the interval is valid, which",
	    "can be any string but should be expressed as an adjective. For",
	    "example, C<North American>. If the interval is part of the",
	    "international chronostratographic system, the value should be",
	    "C<international>. Otherwise, if the interval is valid globally",
	    "then the value should be C<global>.",
	{ optional => 'interval_taxon', valid => ANY_VALUE },
	    "The taxonomic group with respect to which the interval is",
	    "defined, if any. This should be expressed as a common name",
	    "rather than a scientific one. Examples: C<conodont>, C<mammal>.",
	{ optional => 'is_locked', valid => BOOLEAN_VALUE },
	    "This attribute is relevant if the boundary is based on another",
	    "reference boundary or boundaries. If set to true, then this boundary",
	    "will not change even if the reference boundary does. If set to false, then the",
	    "boundary will be updated to reflect the reference boundary.",
	{ optional => 'age', valid => DECI_VALUE },
	    "The age of this boundary, in Ma",
	{ optional => 'age_error', valid => DECI_VALUE },
	    "The uncertainty in the age, in Ma",
	{ optional => 'offset', valid => DECI_VALUE },
	    "If the boundary type is C<B<offset>>, then the age of this boundary",
	    "is derived by adding the value of this attribute (in Ma) to the age of",
	    "the reference boundary. If the boundary type is C<B<percent>>,",
	    "then the age is derived as the specified percentage of the difference between",
	    "the two reference boundaries.",
	{ optional => 'offset_error', valid => DECI_VALUE },
	    "The value of this attribute gives the uncertainty in the offset/percentage.",
	{ optional => 'interval_id', valid => VALID_IDENTIFIER('INT') },
	    "The identifier of the interval (if any) for which this is the lower",
	    "boundary. Boundary attributes such as C<B<color>>, C<B<reference_no>>,",
	    "C<B<interval_extent>>, and C<B<interval_taxon>> are taken to apply",
	    "to the upper interval.",
	{ optional => 'interval_name', valid => ANY_VALUE },
	    "If you do not specify C<B<interval_id>>, you may instead specify",
	    "this attribute. If the named interval does not exist in the database",
	    "a record will be created for it provided that B<C<create_intervals>> is",
	    "also specified with this request.",
	{ at_most_one => [ 'interval_id', 'interval_name' ] },
	{ optional => 'lower_id', valid => VALID_IDENTIFIER('INT') },
	    "The identifier of the interval (if any) for which this is the upper",
	    "boundary.",
	{ optional => 'lower_name', valid => ANY_VALUE },
	    "If you do not specify C<B<lower_id>>, you may instead specify",
	    "this attribute. If the named interval does not exist in the database",
	    "a record will be created for it provided that B<C<create_intervals>> is",
	    "also specified with this request.",
	{ at_most_one => [ 'lower_id', 'lower_name' ] },
	{ optional => 'base_id', valid => VALID_IDENTIFIER('BND') },
	    "If the B<C<bound_type>> is C<B<same>>, C<B<offset>>, or C<B<percent>>,",
	    "then you must also specify a reference bound using this parameter.",
	{ optional => 'range_id', valid => VALID_IDENTIFIER('BND') },
	    "If the B<C<bound_type>> is C<B<percent>>, then you must specify a",
	    "second reference bound using this parameter. The value of B<C<offset>>",
	    "is then taken to indicate a percentage of the difference between the",
	    "ages of the two reference bounds.",
	{ optional => 'color_id', valid => VALID_IDENTIFIER('BND') },
	    "If this parameter is specified, then the color of the upper",
	    "interval will be taken from the specified bound.",
	{ optional => 'refsource_id', valid => VALID_IDENTIFIER('BND') },
	    "If this parameter is specified, then the bibliographic reference",
	    "for this bound will be taken from the specified bound.",
	{ optional => 'color', valid => ANY_VALUE },
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
	    "not block it from completing. B<Important:> for many applications,",
	    "it is best to allow the request to block, get confirmation from",
	    "the user for each flagged condition, and if confirmed then repeat the request",
	    "with these specific actions allowed using this parameter. Accepted",
	    "values include:");
    
    $ds->define_ruleset('1.2:timescales:ret_mod' =>
	">>The following parameters specify what should be returned from this",
	"operation:",
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_basic' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:bounds:ret_mod' =>
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
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every boundary specified in the body.",
	{ allow => '1.2:timescales:entry' },
	{ allow => '1.2:timescales:ret_mod' });
    
    $ds->define_ruleset('1.2:timescales:update' =>
	{ allow => '1.2:timescales:op_mod' }, 
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every boundary specified in the body.",
	{ allow => '1.2:timescales:entry' },
	{ allow => '1.2:timescales:ret_mod' });
    
    $ds->define_ruleset('1.2:timescales:delete' =>
	{ allow => '1.2:timescales:op_mod' }, 
	">>The following parameter may be given either in the URL or in",
	"the request body. Either way, you may specify more than one value,",
	"as a comma-separated list.",
	{ param => 'timescale_id', valid => VALID_IDENTIFIER('TSC'), list => ',' },
	    "The identifier(s) of the timescale(s) to delete.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:bounds:addupdate' =>
	{ allow => '1.2:timescales:op_mod' }, 
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every boundary specified in the body.",
	{ allow => '1.2:bounds:entry' },
	{ allow => '1.2:bounds:ret_mod' });
    
    $ds->define_ruleset('1.2:bounds:update' => 
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every bound specified in the body.",
	{ allow => '1.2:bounds:entry' },
	{ allow => '1.2:bounds:ret_mod' });

    $ds->define_ruleset('1.2:bounds:delete' =>
	{ allow => '1.2:timescales:op_mod' }, 
	">>The following parameter may be given either in the URL or in",
	"the request body. Either way, you may specify more than one value,",
	"as a comma-separated list.",
	{ param => 'bound_id', valid => VALID_IDENTIFIER('BND'), list => ',' },
	    "The identifier(s) of the interval boundarie(s) to delete.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
}



our (%IGNORE_PARAM) = ( 'allow' => 1, 'return' => 1, 'record_label' => 1 );


sub update_timescales {

    my ($request, $arg) = @_;
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single timescale only.
    
    my %conditions;
    
    $conditions{CREATE_RECORDS} = 1 if $arg && $arg eq 'add';
    
    my $main_params = $request->get_main_params(\%conditions, '1.2:timescales:entry');
    my $auth_info = $request->get_auth_info($dbh);
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->unpack_input_records($main_params, '1.2:timescales:entry', 'timescale_id');
    
    # If any errors were found in the parameters, stop now and return an HTTP 400 response.
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad request");
    }
    
    # If no input records were found, return a warning.
    
    elsif ( @records == 0 )
    {
	$request->add_warning("W_EMPTY: no input records were found");
	return;
    }
    
    # Now go through and try to actually update each of the timescales. This needs to be inside a
    # transaction, so that if any errors are generated the entire update can be aborted. We start
    # by creating a new TimescaleEdit object, which automatically starts a transaction. This
    # object will also keep track of any errors or other conditions that are generated by any of
    # the update operations.
    
    my $edt = TimescaleEdit->new($dbh, { conditions => \%conditions,
					 debug => $request->debug,
					 auth_info => $auth_info });
    
    try {

	foreach my $r ( @records )
	{
	    my $record_id = $r->{record_label} || $r->{timescale_id} || '';
	    
	    if ( $r->{timescale_id} )
	    {
		print STDERR "UPDATING record '$record_id'\n" if $request->debug;
		
		$edt->update_timescale($r);
	    }
	    
	    else
	    {
		print STDERR "ADDING record '$record_id'\n" if $request->debug;
		
		$edt->add_timescale($r);
	    }
	}
	
	# Now we need to recompute the attributes of any updated boundaries plus any boundaries
	# that depend on them, update the min and max ages of the associated timescales, and then
	# clear all is_updated flags.
	
	$edt->complete_bound_updates;
    }
    
    # If an exception is caught, we roll back the transaction before re-throwing it as an internal
    # error. This will generate an HTTP 500 response.
    
    catch {

	$edt->rollback;
	die $_;
    };
    
    # If we completed the procedure without any exceptions, but error conditions were detected
    # nonetheless, we also roll back the transaction.
    
    $request->add_edt_warnings($edt);
    
    if ( $edt->errors )
    {
	$edt->rollback;
	$request->add_edt_errors($edt);
	die $request->exception(400, "Bad request");
    }
    
    elsif ( $request->clean_param('strict') && $request->warnings )
    {
	$edt->rollback;
	die $request->exceptions(400, "E_STRICT: warnings were generated");
    }
    
    else
    {
	# If we get here, we're good to go! Yay!!!
	
	$edt->commit;
	
	# Return the indicated information. This will generally be one or more timescale records.
	
	my $list = join(',', $edt->timescales_updated);
	
	$request->list_timescales_for_update($dbh, undef, $list) if $list;
    }
}


sub delete_timescales {
    
    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single boundary only.
    
    my %conditions;
    
    my $main_params = $request->get_main_params(\%conditions);
    my $auth_info = $request->get_auth_info($dbh);
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->unpack_input_records($main_params);
    
    my %delete_records;
    
    # Now go through the records and validate each one in turn.
    
    foreach my $r ( @records )
    {
	my @ids = ref $r->{timescale_id} eq 'ARRAY' ? @{$r->{timescale_id}} : $r->{timescale_id};
	
	foreach my $id ( @ids )
	{
	    my $validated = $request->validate_extident('TSC', $id, 'eduresource_id');
	    $delete_records{$validated} = 1 if $validated;
	}
    }
    
    # Now go through and try to actually delete each of the timescales. This needs to be inside a
    # transaction, so that if any errors are generated the entire update can be aborted. We start
    # by creating a new TimescaleEdit object, which automatically starts a transaction. This
    # object will also keep track of any errors or other conditions that are generated by any of
    # the update operations.
    
    my $edt = TimescaleEdit->new($dbh, { debug => $request->debug,
					 auth_info => $auth_info });
    
    # $request->check_edt($edt);
    
    try {

	my $list = join(',', keys %delete_records);
	
	if ( $list )
	{
	    $edt->delete_timescale($list, \%conditions);
	}

	foreach my $e ( $edt->conditions )
	{
	    if ( $e =~ /^[EC]/ )
	    {
		$request->add_error($e);
	    }
	    
	    else
	    {
		$request->add_warning($e);
	    }
	}
	
	# Now we need to recompute the attributes of any updated boundaries plus any boundaries
	# that depend on them, update the min and max ages of the associated timescales, and then
	# clear all is_updated flags.
	
	$edt->complete_bound_updates;
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
	
	# Perhaps we should return something, but for now we don't.
    }
}


sub update_bounds {
    
    my ($request, $arg) = @_;
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single boundary only.
    
    my %conditions;
    
    $conditions{CREATE_RECORDS} = 1 if $arg && ($arg eq 'add' or $arg eq 'replace');
    
    my $main_params = $request->get_main_params(\%conditions, '1.2:timescales:entry');
    my $auth_info = $request->get_auth_info($dbh);

    # foreach my $k ( @request_keys )
    # {
    # 	my @list = $request->clean_param_list($k);
	
    # 	if ( $k eq 'show' || $k eq 'return' )
    # 	{
    # 	    next;
    # 	}
	
    # 	elsif ( $k eq 'allow' )
    # 	{
    # 	    $conditions{$_} = 1 foreach @list;
    # 	}
	
    # 	elsif ( @list == 1 )
    # 	{
    # 	    $main_params{$k} = $list[0];
    # 	}
	
    # 	else {
    # 	    $main_params{$k} = \@list;
    # 	}
    # }
    
    # Then decode the body, and extract parameters from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->unpack_input_records($main_params, '1.2:biybds:entry', 'bound_id');
    
    # my ($body, $error) = $request->decode_body;
    
    # if ( $error )
    # {
    # 	die $request->exception(400, "E_REQUEST_BODY: Badly formatted request body: $error");
    # }
    
    # if ( ref $body eq 'HASH' && ref $body->{all} eq 'HASH' )
    # {
    # 	foreach my $k ( keys %{$body->{all}} )
    # 	{
    # 	    $main_params{$k} = $body->{all}{$k};
    # 	}
	
    # 	my $result = $request->validate_params('1.2:bounds:entry', \%main_params);
	
    # 	if ( $result->errors )
    # 	{
    # 	    foreach my $e ( $result->errors )
    # 	    {
    # 		$request->add_error("E_PARAM: $e");
    # 	    }
	    
    # 	    foreach my $w ( $result->warnings )
    # 	    {
    # 		$request->add_warning("W_PARAM: $w");
    # 	    }
	    
    # 	    die $request->exception(400, "Invalid request");
    # 	}
    # }
    
    # # Then look for a list of records under the key 'records'. Or if the body decodes to a
    # # top-level array then assume each entry in the array is a record.
    
    # my $record_list;
    
    # if ( ref $body eq 'ARRAY' )
    # {
    # 	$record_list = $body;
    # }
    
    # elsif ( ref $body eq 'HASH' && ref $body->{records} eq 'ARRAY' )
    # {
    # 	$record_list = $body->{records};
    # }
    
    # elsif ( defined $body && $body ne '' && ref $body ne 'HASH' )
    # {
    # 	$request->add_error("E_BODY: Badly formatted request body: must be a hash or an array");
    # 	die $request->exception(400, "Invalid request");
    # }
    
    # elsif ( $main_params{bound_id} && $main_params{bound_id} > 0 )
    # {
    # 	$record_list = [ { bound_id => $main_params{bound_id}, record_id => $main_params{record_id} } ];
    # }
    
    # else
    # {
    # 	die $request->exception(400, "E_NO_UPDATE: no record to update");
    # }
    
    # Now go through the records and validate each one in turn.
    
    # my %record_id;
    
    # foreach my $r ( @$record_list )
    # {
    # 	my $record_id = $r->{record_label} || $r->{bound_id} || '';
	
    # 	foreach my $k ( keys %main_params )
    # 	{
    # 	    $r->{$k} = $main_params{$k} unless defined $r->{$k} || $IGNORE_PARAM{$k};
    # 	}
	
    # 	my $result = $request->validate_params('1.2:bounds:entry', $r);
	
    # 	foreach my $e ( $request->errors )
    # 	{
    # 	    my $msg = $record_id ? "E_PARAM ($record_id): $e" : "E_PARAM: $e";
    # 	    $request->add_error($msg);
    # 	}
    # }
    
    # If any errors were found in the parameters, stop now and return an HTTP 400 response.
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad request");
    }
    
    # If no input records were found, return a warning.
    
    elsif ( @records == 0 )
    {
	$request->add_warning("W_EMPTY: no input records were found");
	return;
    }
    
    # Now go through and try to actually update each of the bounds. This needs to be inside a
    # transaction, so that if any errors are generated the entire update can be aborted. We start
    # by creating a new TimescaleEdit object, which automatically starts a transaction. This
    # object will also keep track of any errors or other conditions that are generated by any of
    # the update operations.
    
    my $edt = TimescaleEdit->new($dbh, { debug => $request->debug,
					 auth_info => $auth_info });
    
    # $request->check_edt($edt);
    
    try {

	foreach my $r ( @records )
	{
	    my $record_id = $r->{record_id} || $r->{bound_id} || '';
	    
	    if ( $r->{bound_id} )
	    {
		print STDERR "UPDATING record '$record_id'\n" if $request->debug;
		
		$edt->update_boundary($r, \%conditions);
	    }
	    
	    else
	    {
		print STDERR "ADDING record '$record_id'\n" if $request->debug;
		
		$edt->add_boundary($r, \%conditions);
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
	
	# If this routine was called as 'bounds/replace', then we need to delete all bounds from
	# updated timescales that were not explicitly added or updated by this operation.
	
	if ( $arg && $arg eq 'replace' )
	{
	    foreach my $timescale_id ( $edt->timescales_updated )
	    {
		$edt->delete_boundary('unupdated', $timescale_id);
		
		foreach my $e ( $edt->conditions )
		{
		    $e =~ s/^(\w+)[:]\s*/$1 ($timescale_id): /;
		    
		    if ( $e =~ /^[EC]/ )
		    {
			$request->add_error($e);
		    }
		    
		    else
		    {
			$request->add_warning($e);
		    }
		}
	    }
	    
	    # foreach my $t ( @timescale_list )
	    # {
	    # 	$edt->delete_boundary( { timescale_id => $t, un_updated => 1 } ) if $t;
	    # }
	}
	
	# Now we need to recompute the attributes of any updated boundaries plus any boundaries
	# that depend on them, update the min and max ages of the associated timescales, and then
	# clear all is_updated flags.
	
	$edt->complete_bound_updates;
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
	
	# Return the indicated information. This will generally be one or more boundary records.
	
	my $return_what = $request->clean_param('return') || 'timescale';
	my $list = '';
	
	if ( $return_what eq 'timescale' )
	{
	    $list = join(',', $edt->timescales_updated);
	}
	
	elsif ( $return_what eq 'updated' )
	{
	    $list = join(',', $edt->bounds_updated);
	}
	
	$request->list_bounds_for_update($dbh, $return_what, $list) if $list;
    }
}


sub delete_bounds {


}


# sub check_record {

#     my ($request, $record) = @_;
    
#     # If any records are missing a value for 'bound_id' (meaning that new
#     # bounds hould be created) then throw an error unless 'create_records' was
#     # specified. 
    
#     unless ( ($record->{bound_id} && $record->{bound_id} > 0) || $record->{create_records} )
#     {
# 	$request->add_error("E_NO_CREATE: you must specify 'create_records' if you include records with no identifier");
#     }
# }


sub list_bounds_for_update {
    
    my ($request, $dbh, $return_type, $list) = @_;
    
    my $tables = $request->tables_hash;
    
    $request->substitute_select( mt => 'tsb', cd => 'tsb' );
    
    my @filters = $return_type eq 'timescale' ? "timescale_no in ($list)" : "bound_no in ($list)";
    my $filter_string = join(' and ', @filters);
    
    $request->extid_check;

    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $request->select_string;
    
    # Determine the order in which the results should be returned.
    
    my $order_expr = 'ORDER BY tsb.age';
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list('tsb', $tables);
    
    # Then query for bounds.
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM $TIMESCALE_BOUNDS as tsb $join_list
        WHERE $filter_string
	GROUP BY tsb.bound_no $order_expr";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


sub list_timescales_for_update {
    
    my ($request, $dbh, $return_type, $list) = @_;
    
    my $tables = $request->tables_hash;
    
    $request->substitute_select( mt => 'ts', cd => 'ts' );
    
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $request->select_string;
    
    # Determine the order in which the results should be returned.
    
    my $order_expr = 'ORDER BY ts.timescale_no';
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list('ts', $tables);
    
    # Then query for bounds.
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM $TIMESCALE_DATA as ts $join_list
        WHERE timescale_no in ($list)
	GROUP BY ts.timescale_no $order_expr";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}

1;
