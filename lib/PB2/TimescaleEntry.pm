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
use TimescaleEdit qw(add_boundary update_boundary delete_boundary);
use CommonEdit qw(start_transaction commit_transaction rollback_transaction);

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::TimescaleData PB2::ReferenceData);

# initialize ( )
# 
# This routine is called by the DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # Value sets for specifying data entry options.
    
    $ds->define_set('1.2:timescales:cautions' =>
	{ value => 'CREATE_INTERVALS' },
	    "If any interval names are encountered during this operation which",
	    "do not correspond to interval records already in the database, then",
	    "create new interval records rather than blocking the request");
    
    $ds->define_set('1.2:timescales:bounds_return' =>
	{ value => 'updated' },
	    "Return just the updated bounds.",
	{ value => 'timescale' },
	    "Return the full list of bounds for every updated timescale.",
	    "This is the default.",
	{ value => 'none' },
	    "Return nothing except the status code and any warnings or",
	    "cautions that were generated.");
    
    # Rulesets for entering and updating data.
    
    $ds->define_ruleset('1.2:common:update_flags' =>
	{ optional => 'create_records', valid => FLAG_VALUE },
	    "If this flag is given, then any records which lack",
	    "a value for the primary identifier will be created and",
	    "a new identifier assigned to them. Otherwise, the request",
	    "will be rejected if any records lack a primary identifier.");
    
    $ds->define_ruleset('1.2:bounds:entry' =>
	{ optional => 'record_id', valid => ANY_VALUE },
	    "A uniqe value for this attribute must be provided for every",
	    "record submitted to this data service. This allows the data service",
	    "to accurately indicate which records generated errors or warnings.",
	    "You may specify any string, as long as it is non-empty and unique",
	    "among all of the records in this request.",
	{ optional => 'timescale_id', valid => VALID_IDENTIFIER('TSC') },
	    "The identifier of the timescale in which bound(s)",
	    "are located. This is optional, but can be used to make",
	    "sure that the proper bounds are being updated.",
	{ optional => 'bound_id', valid => VALID_IDENTIFIER('BND') },
	    "The identifier of the bound to be updated. This is required.",
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
	{ optional => 'bound_id', valid => VALID_IDENTIFIER('BND') },
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
    
    $ds->define_ruleset('1.2:bounds:update' => 
	">>The following parameters affect how this operation is carried out:",
	{ allow => '1.2:common:update_flags' },
	# { optional => 'create_intervals', valid => FLAG_VALUE },
	#     "If this parameter is given, and if any value of C<B<interval_name>>",
	#     "or C<B<lower_name>> does not correspond to an interval record",
	#     "in the database, then one will be created.",
	{ optional => 'allow', valid => '1.2:timescales:cautions' },
	    "This parameter specifies a list of conditions that will",
	    "be allowed to occur during processing of this request, and",
	    "not block it from completing. B<Important:> for most applications,",
	    "it is best to allow the request to block, get confirmation from",
	    "the user for each separate condition, and if confirmed then repeat the request",
	    "with these specific conditions allowed using this parameter. Accepted",
	    "values include:",
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every boundary specified in the body.",
	{ allow => '1.2:bounds:entry' },
	">>The following parameters specify what should be returned from this",
	"operation:",
	{ optional => 'return', valid => '1.2:timescales:bounds_return' },
	    "This parameter specifies what records should be returned from the",
	    "update operation. You can choose to return just the updated records",
	    "all records associated with any updated timescales, or nothing at all.",
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_bound' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    
}


sub update_bounds {
    
    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single boundary only.
    
    my @request_keys = $request->param_keys;
    my %main_params;
    
    foreach my $k ( @request_keys )
    {
	my @list = $request->clean_param_list($k);
	
	if ( @list == 1 ) { $main_params{$k} = $list[0]; }
	else { $main_params{$k} = \@list; }
    }
    
    # Then decode the body, and extract parameters from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my ($body, $error) = $request->decode_body;
    
    if ( $error )
    {
	die $request->exception(400, "E_REQUEST_BODY: Badly formatted request body: $error");
    }
    
    if ( ref $body eq 'HASH' && ref $body->{all} eq 'HASH' )
    {
	foreach my $k ( keys %{$body->{all}} )
	{
	    $main_params{$k} = $body->{all}{$k};
	}
	
	my $result = $request->validate_params('1.2:bounds:entry', \%main_params);
	
	if ( $result->errors )
	{
	    foreach my $e ( $result->errors )
	    {
		$request->add_error("E_PARAM: $e");
	    }
	    
	    foreach my $w ( $result->warnings )
	    {
		$request->add_warning("W_PARAM: $w");
	    }
	    
	    die $request->exception(400, "Invalid request");
	}
    }
    
    # Figure out what conditions will be allowed durint this operation.
    
    my $conditions = $request->clean_param_hash('allow');
    
    # Then look for a list of records under the key 'records'. Or if the body decodes to a
    # top-level array then assume each entry in the array is a record.
    
    my $record_list;
    
    if ( ref $body eq 'ARRAY' )
    {
	$record_list = $body;
    }
    
    elsif ( ref $body eq 'HASH' && ref $body->{records} eq 'ARRAY' )
    {
	$record_list = $body->{records};
    }
    
    elsif ( defined $body && $body ne '' && ref $body ne 'HASH' )
    {
	$request->add_error("E_REQUEST_BODY: Badly formatted request body: must be a hash or an array");
	die $request->exception(400, "Invalid request");
    }
    
    elsif ( $main_params{bound_id} && $main_params{bound_id} > 0 )
    {
	$main_params{record_id} ||= 'main';
	$record_list = [ \%main_params ];
    }
    
    else
    {
	die $request->exception(400, "E_NO_UPDATE: no record to update");
    }
    
    # Now go through the records and validate each one in turn.
    
    my %record_id;
    my %timescale_id;
    
    foreach my $r ( @$record_list )
    {
	my $record_id = $r->{record_id};
	
	if ( ! defined $record_id || $record_id eq '' )
	{
	    $request->add_error("record unknown: E_RECORD_ID: no identifier specified");
	    next;
	}
	
	elsif ( $record_id{$record_id} )
	{
	    $request->add_error("record $record_id: E_RECORD_ID: record identifier is not unique");
	    next;
	}
	
	$record_id{$record_id} = 1;
	$timescale_id{$r->{timescale_id} + 0} = 1 if $r->{timescale_id};
	
	foreach my $k ( keys %main_params )
	{
	    $r->{$k} = $main_params{$k} unless defined $r->{$k};
	}
	
	my $result = $request->validate_params('1.2:bounds:entry', $r);
	
	foreach my $e ( $request->errors )
	{
	    $request->add_error("record $record_id: E_PARAM: $e");
	}
	
	$request->check_record($r);
    }
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad request");
    }
    
    if ( $request->cautions && ! $main_params{force} )
    {
	die $request->exception(422, "Cannot process");
    }
    
    # Now go through and try to actually update each of the bounds. This needs
    # to be inside a transaction, so that if any errors are generated the
    # entire update can be aborted.
    
    my (%bound_updated, %timescale_updated);
    
    try {

	start_transaction($dbh);
	
	my $options = { debug => $request->debug };
	
	foreach my $r ( @$record_list )
	{
	    my $bound_no = $main_params{bound_id} + 0;
	    my $record_id = $r->{record_id};
	    my $result;
	    
	    if ( $bound_no )
	    {
		print STDERR "UPDATING record '$record_id' ($bound_no)\n";
		
		$result = update_boundary($dbh, $r, $conditions, $options);
	    }
	    
	    else
	    {
		print STDERR "ADDING record '$record_id'\n";
		
		$result = add_boundary($dbh, $r, $conditions, $options);
	    }
	    
	    foreach my $c ( $result->conditions )
	    {
		if ( $c =~ /^E/ )
		{
		    $request->add_error("record $record_id: $c");
		    $options->{check_only} = 1;
		}
		
		elsif ( $c =~ /^C/ )
		{
		    $request->add_caution("record $record_id: $c");
		    $options->{check_only} = 1;
		}
		
		else
		{
		    $request->add_warning("record $record_id: $c");
		}
	    }
	    
	    # Keep track of bound_id and timescale_id values, for use in computing the return
	    # value of this operation.
	    
	    my ($bound_updated, $timescale_updated) = $result->record_keys;
	    
	    $bound_updated{$bound_updated} = 1;
	    $timescale_updated{$timescale_updated} = 1;
	}
	
	# check_boundaries($dbh, $timescale_id);
    }
    
    catch {

	rollback_transaction($dbh);
	die $_;
    };
    
    if ( $request->errors )
    {
	rollback_transaction($dbh);
	die $request->exception(400, "Bad request");
    }
    
    elsif ( $request->cautions && ! $main_params{force} )
    {
	rollback_transaction($dbh);
	die $request->exception(400, "Bad request");
    }
    
    elsif ( $request->clean_param('strict') && $request->warnings )
    {
	rollback_transaction($dbh);
	die $request->exceptions(400, "E_STRICT: warnings were generated");
    }
    
    else
    {
	# If we get here, we're good to go! Yay!!!
	
	commit_transaction($dbh);
	
	# Return the indicated information.
	
	my $bounds_return = $request->clean_param('bounds_return') || 'timescale';
	my $list = '';
	
	if ( $bounds_return eq 'timescale' )
	{
	    $list = join(',', keys %timescale_updated);
	}
	
	elsif ( $bounds_return eq 'updated' )
	{
	    $list = join(',', keys %bound_updated);
	}
	
	$request->list_bounds_for_update($dbh, $bounds_return, $list) if $list;
    }
}


sub check_record {

    my ($request, $record) = @_;
    
    # If any records are missing a value for 'bound_id' (meaning that new
    # bounds hould be created) then throw an error unless 'create_records' was
    # specified. 
    
    unless ( ($record->{bound_id} && $record->{bound_id} > 0) || $record->{create_records} )
    {
	$request->add_error("E_NO_CREATE: you must specify 'create_records' if you include records with no identifier");
    }
}


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

1;
