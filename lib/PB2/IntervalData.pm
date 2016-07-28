#
# IntervalQuery
# 
# A class that returns information from the PaleoDB database about time intervals.
# This is a subclass of PBDataQuery.
# 
# Author: Michael McClennen

use strict;

package PB2::IntervalData;

use HTTP::Validate qw(:validators);
use Carp qw(carp croak);
use TableDefs qw($INTERVAL_DATA $INTERVAL_MAP $SCALE_DATA $SCALE_MAP $SCALE_LEVEL_DATA);
use ExternalIdent qw(VALID_IDENTIFIER generate_identifier %IDP);


our (@REQUIRES_ROLE) = qw(PB2::CommonData);

use Moo::Role;

no warnings 'numeric';


# Store the basic data about each interval and scale.

our (%IDATA, %INAME, %SDATA, %SLDATA, %SMDATA);
our (%BOUNDARY_LIST, %BOUNDARY_MAP);


# initialize ( )
# 
# This routine is called automatically by the data service to initialize this
# class.

sub initialize {

    my ($class, $ds) = @_;
    
    # Define output blocks for displaying time interval information
    
    $ds->define_block('1.2:intervals:basic' =>
	{ select => [ qw(i.interval_no i.interval_name i.abbrev sm.scale_no sm.scale_level
			 sm.parent_no sm.color i.early_age i.late_age i.reference_no) ] },
	{ output => 'interval_no', com_name => 'oid' },
	    "A positive integer that uniquely identifies this interval",
	{ output => 'record_type', com_name => 'typ', value => $IDP{INT} },
	    "The type of this object: C<$IDP{INT}> for an interval",
	{ output => 'scale_no', com_name => 'tsc' },
	    "The time scale in which this interval lies.  An interval may be reported more than",
	    "once, as a member of different time scales",
	{ output => 'scale_level', com_name => 'lvl' },
	    "The level within the time scale to which this interval belongs.  For example,",
	    "the default time scale is organized into the following levels:",
	    "=over", "=item Level 1", "Eons",
		       "=item Level 2", "Eras",
		       "=item Level 3", "Periods",
		       "=item Level 4", "Epochs",
		       "=item Level 5", "Stages",
	    "=back",
	{ output => 'interval_name', com_name => 'nam' },
	    "The name of this interval",
	{ output => 'abbrev', com_name => 'abr' },
	    "The standard abbreviation for the interval name, if any",
	{ output => 'parent_no', com_name => 'pid' },
	    "The identifier of the parent interval",
	{ output => 'color', com_name => 'col' },
	    "The standard color for displaying this interval",
	{ output => 'early_age', pbdb_name => 'max_ma', com_name => 'eag' },
	    "The early age boundary of this interval (in Ma)",
	{ output => 'late_age', pbdb_name => 'min_ma', com_name => 'lag' },
	    "The late age boundary of this interval (in Ma)",
	# { set => 'reference_no', append => 1 },
	{ output => 'reference_no', com_name => 'rid', text_join => ', ', show_as_list => 1 },
	    "The identifier(s) of the references from which this data was entered",
	{ set => '*', code => \&process_int_ids });
    
    $ds->define_block('1.2:scales:basic' =>
	{ select => [ 'sc.scale_no', 'sc.scale_name', 'sc.levels as num_levels',
		      'sc.early_age', 'sc.late_age', 'sc.reference_no',
		      'sl.scale_level', 'sl.level_name' ] },
	{ output => 'scale_no', com_name => 'oid' },
	    "A positive integer that uniquely identifies this time scale",
	{ output => 'record_type', com_name => 'typ', com_value => 'tsc', value => 'timescale' },
	    "The type of this object: 'tsc' for a time scale",
	{ output => 'scale_name', com_name => 'nam' },
	    "The name of this time scale",
	{ output => 'num_levels', com_name => 'nlv' },
	    "The number of levels into which this time scale is organized",
	{ output => 'level_list', com_name => 'lvs', sub_record => '1.2:scales:level',
	  if_format => 'json' },
	    "A list of levels associated with this time scale, if more than one.",
	    "This field will only be present in C<json> responses.",
	{ output => 'scale_level', com_name => 'lvl', not_format => 'json' },
	    "Level number.",
	{ output => 'level_name', com_name => 'nam', not_format => 'json' },
	    "Level name",
	{ output => 'early_age', pbdb_name => 'max_ma', com_name => 'eag' },
	    "The early bound of this time scale, in Ma",
	{ output => 'late_age', pbdb_name => 'min_ma', com_name => 'lag' },
	    "The late bound of this time scale, in Ma",
	# { set => 'reference_no', append => 1 },
	{ output => 'reference_no', com_name => 'rid', text_join => ', ', show_as_list => 1 },
	    "The identifier(s) of the references from which this data was entered",
	{ set => '*', code => \&process_int_ids });
    
    $ds->define_block('1.2:scales:level' =>
	{ output => 'scale_level', com_name => 'lvl' },
	    "Level number",
	{ output => 'level_name', com_name => 'nam' },
	    "Level name");
    
    # Define the set of time resolution rules that we implement.
    
    $ds->define_set('1.2:timerules' =>
	{ value => 'contain' },
	    "Select only records whose temporal locality is strictly contained in the specified time range.",
	    "This is the most restrictive rule.  For diversity output, this rule guarantees that each occurrence",
	    "will fall into at most one temporal bin, but many occurrences will be ignored because their temporal",
	    "locality is too wide to fall into any of the bins.",
	{ value => 'major' },
	    "Select only records for which at least 50% of the temporal locality range falls within the specified",
	    "time range.",
	    "For diversity output, this rule also guarantees that each occurrence will fall into at most one",
	    "temporal bin.  Many occurrences will be ignored because their temporal locality is more than twice",
	    "as wide as any of the overlapping bins, but fewer will be ignored than with the C<contain> rule.",
	    "This is the B<default> timerule unless you specifically select one.",
	{ value => 'buffer' },
	    "Select only records whose temporal locality overlaps the specified time range and also falls",
	    "completely within a 'buffer zone' around this range.  This buffer defaults",
	    "to 12 million years for the Paleozoic and Mesozoic and 5 million years for the Cenozoic.",
	    "You can override the buffer width using the parameters B<C<timebuffer>> and",
	    "B<C<late_buffer>>.  For diversity output, some occurrences will be counted as falling into more",
	    "than one bin.  Some occurrences will still be ignored, but fewer than with the above rules.",
	{ value => 'overlap' },
	    "Select only records whose temporal locality overlaps the specified time range by any amount.",
	    "This is the most permissive rule.  For diversity output, every occurrence will be counted.",
	    "Many will be counted as falling into more than one bin.");

    # Then define some rulesets to describe the parameters accepted by the
    # operations defined here.
    
    $ds->define_ruleset('1.2:intervals:specifier' =>
	"You must specify one of the following parameters:",
	{ param => 'id', valid => VALID_IDENTIFIER('INT'), alias => 'interval_id' },
	    "Return the interval corresponding to the specified identifier.",
	{ param => 'name' },
	    "Return the interval with the specified name.",
	{ at_most_one => ['id', 'name'],
	  errmsg => "You may not specify both 'name' and 'id' in the same query." });
    
    $ds->define_ruleset('1.2:intervals:selector' => 
	{ param => 'all_records', valid => FLAG_VALUE },
	    "List all intervals known to the database.",
	{ param => 'scale_id', valid => [VALID_IDENTIFIER('TSC'), ENUM_VALUE('all')], 
	  list => ',', alias => 'scale',
	  errmsg => "the value of {param} should be a list of scale identifiers or 'all'" },
	    "Return intervals from the specified time scale(s).",
	    "The value of this parameter should be a list of scale identifiers separated",
	    "by commas, or 'all'",
	{ param => 'scale_level', valid => POS_VALUE, list => ',', alias => 'level' },
	    "Return intervals from the specified scale level(s).  The value of this",
	    "parameter can be one or more level numbers separated by commas.",
	{ param => 'id', valid => VALID_IDENTIFIER('INT'), list => ',', alias => 'interval_id',
	  bad_value => '_' },
	    "Return intervals that have the specified identifiers",
	{ param => 'name', list => ',' },
	    "Return intervals that have the specified names",
	{ at_most_one => ['id', 'name'],
	  errmsg => "You may not specify both 'name' and 'id' in the same query." },
	{ param => 'min_ma', valid => DECI_VALUE(0) },
	    "Return only intervals that are at least this old",
	{ param => 'max_ma', valid => DECI_VALUE(0) },
	    "Return only intervals that are at most this old",
	{ optional => 'order', valid => ENUM_VALUE('age', 'age.asc', 'age.desc', 'name', 'name.asc', 'name.desc'), default => 'age' },
	    "Return the intervals in order starting as specified.  Possible values include ",
	    "C<age>, C<name>.  Defaults to C<age>.");
    
    $ds->define_ruleset('1.2:intervals:list' => 
	{ require => '1.2:intervals:selector' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");

    $ds->define_ruleset('1.2:intervals:single' => 
	{ allow => '1.2:intervals:specifier' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:scales:specifier' =>
	{ param => 'id', valid => VALID_IDENTIFIER('TSC') },
	    "Return the time scale corresponding to the specified identifier. (REQUIRED)");
    
    $ds->define_ruleset('1.2:scales:selector' =>
	"To return all time scales, use this URL path with no parameters.",
	{ param => 'id', valid => VALID_IDENTIFIER('TSC'), list => ',' },
	    "Return intervals that have the specified identifier(s).",
	    "You may specify more than one, as a comma-separated list.");
    
    $ds->define_ruleset('1.2:scales:single' =>
	{ require => '1.2:scales:specifier' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:scales:list' =>
	{ allow => '1.2:scales:selector' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    # Now some rulesets that can be used by other parts of the application.
    
    $ds->define_ruleset('1.2:ma_selector' =>
	{ param => 'min_ma', valid => DECI_VALUE(0) },
	    "Return only records whose temporal locality is at least this old, specified in Ma.",
	{ param => 'max_ma', valid => DECI_VALUE(0) },
	    "Return only records whose temporal locality is at most this old, specified in Ma.");
    
    $ds->define_ruleset('1.2:interval_selector' =>
	{ param => 'interval_id', valid => VALID_IDENTIFIER('INT'), list => ',' },
	    "Return only records whose temporal locality falls within the given geologic time",
	    "interval or intervals, specified by numeric identifier.  B<If you specify more",
	    "than one interval, the time range used will be the contiguous period from the",
	    "beginning of the earliest to the end of the latest specified interval.>",
	{ param => 'interval', valid => ANY_VALUE },
	    "Return only records whose temporal locality falls within the named geologic time",
	    "interval or intervals, specified by name.  You may specify more than one interval,",
	    "separated by either commas or a dash.  B<If you specify more than one interval,",
	    "the time range used will be the contiguous period from the beginning of the",
	    "earliest to the end of the latest specified interval.>",
	{ at_most_one => ['interval_id', 'interval', 'min_ma'] },
	{ at_most_one => ['interval_id', 'interval', 'max_ma'] });
    
    $ds->define_ruleset('1.2:timerule_selector' =>
	{ optional => 'timerule', valid => '1.2:timerules', alias => 'time_rule', default => 'major' },
	    "Resolve temporal locality according to the specified rule, as listed below.  This",
	    "rule is applied to determine which occurrences, collections, and/or taxa will be selected if",
	    "you also specify an age range using any of the parameters listed immediately above.",
	    "For diversity output, this rule is applied to",
	    "place each occurrence into one or more temporal bins, or to ignore the occcurrence if it",
	    "does not match any of the bins.  The available rules are:",
	{ optional => 'timebuffer', alias => ['time_buffer', 'earlybuffer', 'early_buffer'], valid => DECI_VALUE(0,20) },
	    "Override the default buffer period when resolving",
	    "temporal locality.  The value must be given in millions of years.  This parameter",
	    "is only relevant if B<C<timerule>> is set to C<B<buffer>>.",
	{ optional => 'latebuffer', alias => 'late_buffer', valid => DECI_VALUE(0,20) },
	    "Override the default buffer period for the end of the time range when resolving temporal",
	    "locality.  This allows the buffer to be different on the late end of the interval than",
	    "on the early end.  The value must be given in millions of years.  This parameter is only relevant",
	    "if B<C<timerule>> is set to C<B<buffer>>.");
    
    # Read in all of the interval data, so we don't have to make lots of
    # queries for it later.
    
    my $dbh = $ds->get_connection;
    
    $class->read_interval_data($dbh);
}


# get ( )
# 
# Query for all relevant information about the interval specified by the
# 'id' parameter.  Returns true if the query succeeded, false otherwise.

sub get {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number or a name.
    
    my $id = $request->clean_param('id');
    my $name = $request->clean_param('name');
    my $filter;
    
    die "Bad identifier '$id'" unless (defined $id and $id =~ /^\d+$/ || defined $name && $name ne '');
    
    if ( $name )
    {
	my $quoted = $dbh->quote($name);
	$filter = "interval_name = $quoted";
    }
    
    else
    {
	$filter = "interval_no = $id";
    }
    
    $request->strict_check;
    $request->extid_check;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'i' );
    
    my $fields = $request->select_string;
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generateJoinList('i', $request->{select_tables});
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $fields
	FROM $INTERVAL_DATA as i LEFT JOIN $SCALE_MAP as sm using (interval_no)
		$join_list
        WHERE $filter
	GROUP BY i.interval_no";
    
    print STDERR $request->{main_sql} . "\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    # Unless we found a record, return 404.
    
    die "404 No such interval was found\n" unless ref $request->{main_record};
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
    
    return 1;
}


# list ( )
# 
# Query the database for basic info about all specified intervals.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub list {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # If we were asked for a hierarchy, indicate that we will need to process
    # the result set before sending it.
    
    $request->{process_resultset} = \&generateHierarchy if 
	defined $request->{op} && $request->{op} eq 'hierarchy';
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters;
    
    my $scale = $request->clean_param('scale');
    my @scale_ids = $request->clean_param_list('scale_id');
    my @scale_levels = $request->clean_param_list('scale_level');
    my @ids = $request->clean_param_list('id');
    
    my $level_string = join(',', @scale_levels);
    
    if ( defined $scale && $scale eq 'all' )
    {
	unless ( $level_string )
	{
	    push @filters, "sm.scale_level is not null";
	}
    }
    
    elsif ( $request->param_given('scale_id') || $request->param_given('scale') )
    {
	push @scale_ids, 0 unless @scale_ids;
	my $filter_string = join(',', @scale_ids);
	
	if ( $filter_string !~ /all/ )
	{
	    push @filters, "sm.scale_no in ($filter_string)";
	}
    }
    
    if ( $request->param_given('id') )
    {
	push @ids, -1 unless @ids;
	my $id_string = join(',', @ids);
	push @filters, "i.interval_no in ($id_string)";
    }
    
    elsif ( my @names = $request->clean_param_list('name') )
    {
	my @ids;
	
	foreach my $name (@names)
	{
	    if ( $INAME{lc $name} )
	    {
		push @ids, $INAME{lc $name}{interval_no};
	    }
	    
	    else
	    {
		$request->add_warning("The interval '$name' is not known to the database");
	    }
	}
	
	push @ids, -1 unless @ids;
	my $id_string = join(',', @ids);
	push @filters, "i.interval_no in ($id_string)";
    }
    
    if ( my $min_ma = $request->clean_param('min_ma') )
    {
	$min_ma += 0;
	push @filters, "i.late_age >= $min_ma" if $min_ma > 0;
    }
    
    if ( my $max_ma = $request->clean_param('max_ma') )
    {
	$max_ma += 0;
	push @filters, "i.early_age <= $max_ma" if $max_ma > 0;
    }
    
    if ( $level_string )
    {
	push @filters, "sm.scale_level in ($level_string)";
    }
    
    push @filters, "1=1" unless @filters;
    
    # Get the results in the specified order
    
    my $order = $request->clean_param('order');
    
    my $order_expr = '';
    
    if ( defined $order )
    {
	if ( $order eq 'age' || $order eq 'age.asc' )
	{
	    $order_expr = "ORDER BY i.early_age";
	}
	
	elsif ( $order eq 'age.desc' )
	{
	    $order_expr = "ORDER BY i.early_age desc";
	}
	
	elsif ( $order eq 'name' || $order eq 'name.asc' )
	{
	    $order_expr = "ORDER BY i.interval_name";
	}
	
	elsif ( $order eq 'name.desc' )
	{
	    $order_expr = "ORDER BY i.interval_name desc";
	}
	
	else
	{
	    die $request->exception(400, "unknown value '$order' for parameter 'order'");
	}
    }
    
    $request->strict_check;
    $request->extid_check;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'i' );
    
    my $fields = $request->select_string;
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generateJoinList('i', $request->{select_tables});
    
    # If a query limit has been specified, modify the query accordingly.
    # If we were asked to count rows, modify the query accordingly
    
    my $limit = $request->sql_limit_clause(1);    
    my $calc = $request->sql_count_clause;
    
    # Generate the main query.
    
    my $filter_string = join(' and ', @filters);
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM $INTERVAL_DATA as i LEFT JOIN $SCALE_MAP as sm using (interval_no)
		$join_list
	WHERE $filter_string
	$order_expr
	$limit";
    
    print STDERR $request->{main_sql} . "\n\n" if $request->debug;
    
    # Then prepare and execute the main query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
        
    return 1;
}


# list_scales ( )
# 
# Query the database for time scales.

sub list_scales {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my @filters;
    
    # Get the list of database fields.
    
    $request->substitute_select( mt => 'sc' );
    
    my $fields = $request->select_string;
    
    # Construct the list of filter expressions.
    
    if ( my @id_list = $request->clean_param_list('id') )
    {
	my $id_list = join(',', @id_list);
	push @filters, "scale_no in ($id_list)";
    }
    
    push @filters, "1=1" unless @filters;
    
    $request->strict_check;
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    # If we were asked to count rows, modify the query accordingly
    
    my $limit = $request->sql_limit_clause(1);    
    my $calc = $request->sql_count_clause;
    my $order_expr = '';
    
    # Generate the main query.
    
    my $filter_string = join(' and ', @filters);
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM $SCALE_DATA as sc LEFT JOIN $SCALE_LEVEL_DATA as sl using (scale_no)
	WHERE $filter_string ORDER BY scale_no, scale_level
	$limit";
    
    print STDERR $request->{main_sql} . "\n\n" if $request->debug;
    
    # Then prepare and execute the main query.
    
    my $sth = $dbh->prepare($request->{main_sql});
    $sth->execute();
    
    # If the result is to be returned in JSON format, we need to reorganize it:
    
    my (@scales, %scale);
    
    if ( $request->output_format eq 'json' )
    {
	while ( my $row = $sth->fetchrow_hashref() )
	{
	    my $scale_no = $row->{scale_no};
	    
	    unless ( $scale{$scale_no} )
	    {
		$row->{level_list} = [];
		$scale{$scale_no} = $row;
		push @scales, $row;
	    }
	    
	    push @{$scale{$scale_no}{level_list}}, { scale_level => $row->{scale_level},
						     level_name => $row->{level_name} };
	}
	
	$request->{main_result} = \@scales;
    }
    
    # Otherwise we can just return the statement handle and let the data
    # service code do the rest.
    
    else
    {
	$request->{main_sth} = $sth;
    }
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


# generateJoinList ( tables )
# 
# Generate the actual join string indicated by the table hash.

sub generateJoinList {

    my ($request, $mt, $tables) = @_;
    
    my $join_list = '';
    
    # Return an empty string unless we actually have some joins to make
    
    return $join_list unless ref $tables eq 'HASH' and %$tables;
    
    # Create the necessary join expressions.
    
    $join_list .= "LEFT JOIN refs as r on r.reference_no = $mt.reference_no\n" 
	if $tables->{r};
    
    return $join_list;
}


# process_interval_params ( )
# 
# If the current request includes any of the following parameters:
# 
#    interval
#    interval_id
#    max_ma
#    min_ma
# 
# return information sufficient to generate appropriate SQL filter
# expressions.  This includes the max and min Ma values, plus the interval
# identifiers for the earliest and latest intervals.  The Ma range returned
# spans all of the given intervals, no matter how many or in which order they
# were specified.  Any number of intervals can be specified, separated by
# either dashes or commas.
# 
# Returns: $max_ma, $min_ma, $early_interval_no, $late_interval_no

sub process_interval_params {
    
    my ($request) = @_;
    
    my (@ids, @errors);
    my ($max_ma, $min_ma, $early_interval_no, $late_interval_no, $early_duration, $late_duration);
    
    # First check for each of the relevant parameters.
    
    if ( $request->param_given('interval_id') )
    {
	foreach my $id ( $request->clean_param_list('interval_id') )
	{
	    if ( defined $id && $IDATA{$id} )
	    {
		push @ids, $id;
	    }
	    
	    else
	    {
		die $request->exception(400, "The interval identifier '$id' was not found in the database");
	    }
	}
	
	unless ( @ids )
	{
	    die $request->exception(400, "No valid interval identifiers were given");
	}
    }
    
    elsif ( my $interval_name_value = $request->clean_param('interval') )
    {
	foreach my $name ( split qr{\s*[,-]+\s*}, $interval_name_value )
	{
	    next unless defined $name && $name =~ qr{\S};
	    
	    if ( my $i = $INAME{lc $name} )
	    {
		push @ids, $i->{interval_no};
	    }
	    
	    else
	    {
		push @errors, "Unknown interval '$name'";
	    }
	}
    }
    
    else
    {
	my $value = $request->clean_param('max_ma');
	
	if ( defined $value && $value ne '' )
	{
	    $max_ma = $value;
	    
	    die $request->exception("400", "The value of 'max_ma' must be greater than 0")
		unless $max_ma;
	}
	
	if ( my $value = $request->clean_param('min_ma') )
	{
	    $min_ma = $value;
	}
    }
    
    # If we have found any errors, report them and abort the request.
    
    if ( @errors )
    {
	my $errstring = join('; ', @errors);
	
	die "400 $errstring\n";
    }
    
    # If we have one or more interval ids, scan through to find the earliest
    # and latest.
    
    foreach my $interval_no ( @ids )
    {
	my $i = $IDATA{$interval_no};
	
	my $new_max = $i->{early_age};
	my $new_min = $i->{late_age};
	
	if ( !defined $max_ma || $new_max >= $max_ma )
	{
	    $max_ma = $new_max;
	    
	    if ( !$early_duration || ($i->{early_age} - $i->{late_age}) < $early_duration )
	    {
		$early_interval_no = $interval_no;
		$early_duration = $i->{early_age} - $i->{late_age};
	    }
	}
	
	if ( !defined $min_ma || $new_min <= $min_ma )
	{
	    $min_ma = $new_min;
	    
	    if ( !$late_duration || ($i->{early_age} - $i->{late_age}) < $late_duration )
	    {
		$late_interval_no = $interval_no;
		$late_duration = $i->{early_age} - $i->{late_age};
	    }
	}
    }
    
    # Now return the results.
    
    return ($max_ma, $min_ma, $early_interval_no, $late_interval_no);
    
    # 	    my $qname = $dbh->quote($name);
    # 	    my ($id) = $dbh->selectrow_array("
    # 		SELECT interval_no FROM $INTERVAL_DATA
    # 		WHERE interval_name like $qname");
	    
    # 	    if ( $id )
    # 	    {
    # 		push @ids, $id;
    # 	    }
	    
    # 	    else
    # 	    {
    # 		push @errors, $name;
    # 	    }
    # 	}
    # }
	
	
	# if ( @errors )
	# {
	#     my $error_string = join("', '", @errors);
	    
	#     die "400 bad value '$error_string' for parameter 'interval_id': must be of the form 'I<n>' where <n> is a positive integer\n";
	# }
    # }
    
    # elsif ( $interval_name_value )
    # {
    # 	foreach my $name ( split qr{[\s,-]+}, $interval_name_value )
    # 	{
    # 	    my $qname = $dbh->quote($name);
    # 	    my ($id) = $dbh->selectrow_array("
    # 		SELECT interval_no FROM $INTERVAL_DATA
    # 		WHERE interval_name like $qname");
	    
    # 	    if ( $id )
    # 	    {
    # 		push @ids, $id;
    # 	    }
	    
    # 	    else
    # 	    {
    # 		push @errors, $name;
    # 	    }
    # 	}
	
    # 	if ( @errors )
    # 	{
    # 	    my $error_string = join("', '", @errors);
	    
    # 	    die "400 could not find any intervals matching '$error_string'\n";
    # 	}
    # }
    
    # return unless @ids;
    
    # my $id_list = join(',', @ids);
    
    # my $result = $dbh->selectall_arrayref("
    # 		SELECT early_age, late_age, interval_no
    # 		FROM $INTERVAL_DATA WHERE interval_no in ($id_list)");
    
    # my ($max_ma, $min_ma, $early_interval_no, $late_interval_no);
    
    # foreach my $r ( @$result )
    # {
    # 	if ( !defined $max_ma || $r->[0] > $max_ma )
    # 	{
    # 	    $max_ma = $r->[0];
    # 	    $early_interval_no = $r->[2];
    # 	}
	
    # 	if ( !defined $min_ma || $r->[1] < $min_ma )
    # 	{
    # 	    $min_ma = $r->[1];
    # 	    $late_interval_no = $r->[2];
    # 	}
    # }
    
    # return ($max_ma, $min_ma, $early_interval_no, $late_interval_no);
}


# generateHierarchy ( rows )
# 
# Arrange the rows into a hierarchy.  This is only called on requests
# which use the 'hierarchy' route.

sub generateHierarchy {

    my ($request, $rowref) = @_;
    
    return $rowref unless $request->output_format eq 'json';
    
    my @toplevel = ();
    my %row = ();
    
    foreach my $r ( @$rowref )
    {
	$r->{hier_child} ||= [];
	$row{$r->{interval_no}} = $r;
	
	if ( $r->{scale_level} == 1 )
	{
	    push @toplevel, $r;
	}
	
	else
	{
	    my $parent_no = $r->{parent_no};
	    $row{$parent_no}{hier_child} ||= [];
	    push @{$row{$parent_no}{hier_child}}, $r;
	}
    }
    
    return \@toplevel;
}


# read_interval_data ( dbh )
# 
# Read the basic interval and scale data from the relevant data tables and
# install them into package-local variables.

sub read_interval_data {
    
    my ($class, $dbh) = @_;
    
    # Abort if we have already done this task.
    
    return if %IDATA;
    
    # First read in a list of all the intervals and put them in the IDATA
    # hash, indexed by interval_no.
    
    my $sql = "SELECT * FROM $INTERVAL_DATA";
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    if ( ref $result eq 'ARRAY' )
    {
	my $interval_no;
	
	foreach my $i ( @$result )
	{
	    next unless $interval_no = $i->{interval_no};
	    $IDATA{$interval_no} = $i;
	    $INAME{lc $i->{interval_name}} = $i;
	}
    }
    
    # Then read in a list of all the scales and put them in the SDATA hash,
    # indexed by scale_no.
    
    $sql = "SELECT * FROM $SCALE_DATA";
    
    $result = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    if ( ref $result eq 'ARRAY' )
    {
	my $scale_no;
	
	foreach my $s ( @$result )
	{
	    next unless $scale_no = $s->{scale_no};
	    $SDATA{$scale_no} = $s;
	}
    }
    
    # Then read in a list of the scale levels and fill them in to the
    # %SLDATA hash.
    
    $sql = "SELECT * FROM $SCALE_LEVEL_DATA";
    
    $result = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    if ( ref $result eq 'ARRAY' )
    {
	my ($scale_no, $scale_level, %sample_level);
	
	foreach my $s ( @$result )
	{
	    next unless $scale_no = $s->{scale_no};
	    next unless $scale_level = ($s->{scale_level} // $s->{level});
	    $SLDATA{$scale_no}{$scale_level} = $s->{level_name};
	    $sample_level{$scale_no}{$scale_level} = 1 if $s->{sample};
	}
	
	# The 'sample_list' field will be a list of the levels at which
	# diversity statistics should be counted.  So, for example, for the
	# standard timescale (scale_no = 1), the "Eon" and "Era" levels are
	# just too coarse for diversity computations to make any sense.  It
	# only makes sense to do them at "Period" and below.  We sort the list
	# from largest to smallest, which means from finest-resolution to coarsest.
	
	foreach $scale_no ( keys %SLDATA )
	{
	    $SLDATA{$scale_no}{sample} = $sample_level{$scale_no};
	}
    }
    
    # Now read in the mapping from interval numbers to scale levels and parent
    # intervals. 
    
    $sql = "SELECT * FROM $SCALE_MAP";
    
    $result = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    if ( ref $result eq 'ARRAY' )
    {
	my ($interval_no, $scale_no, $scale_level, $parent_no, $color);
	
	foreach my $m ( @$result )
	{
	    next unless $scale_no = $m->{scale_no};
	    next unless $scale_level = ($m->{scale_level} // $m->{level});
	    next unless $interval_no = $m->{interval_no};
	    next unless $parent_no = $m->{parent_no};
	    $color = $m->{color};
	    
	    $SMDATA{$scale_no}{$interval_no} = { %{$IDATA{$interval_no}}, 
						 parent_no => $parent_no,
						 color => $color,
						 scale_level => $scale_level + 0,
						 "L$scale_level" => $interval_no };
	}
	
	# Now compute boundary lists and parent level mappings. $$$
	
	foreach $scale_no ( keys %SMDATA )
	{
	    my (%boundary_map);
	    
	    foreach $interval_no ( keys %{$SMDATA{$scale_no}} )
	    {
		my $i = $SMDATA{$scale_no}{$interval_no};
		my $parent_no = $i->{parent_no};
		my $scale_level = $i->{scale_level};
		my $boundary_age = $i->{early_age};
		
		# Note the early age for this interval as one of the
		# boundaries for this scale level.
		
		$boundary_map{$scale_level}{$boundary_age} = $i;
		
		# Iteratively compute the level mapping for this interval and
		# all its parents.  So, for example, if we know that interval
		# 50 is at level 5 and its parent is 27 which is at level 4,
		# then the "L5" value for interval 50 is 50 and the "L4" value
		# is 27.  If the parent of 27 is 18, then the "L3" value for
		# 50 is 18.  And so on.
		
		while ( my $p = $SMDATA{$scale_no}{$parent_no} )
		{
		    $i->{"L$p->{scale_level}"} = $parent_no;
		    $parent_no = $p->{parent_no};
		}
	    }
	    
	    # Now sort each of the boundary lists (oldest to youngest) and
	    # store them in the appropriate package variable.
	    
	    foreach my $scale_level ( keys %boundary_map )
	    {
		$BOUNDARY_LIST{$scale_no}{$scale_level} = [ sort { $b <=> $a } keys %{$boundary_map{$scale_level}} ];
		$BOUNDARY_MAP{$scale_no}{$scale_level} = $boundary_map{$scale_level};
	    }
	}
    }
    
    my $a = 1;	# we can stop here when debugging.
}


# initOutput ( )
# 
# This routine is used in case we are generating hierarchical output, setting
# up the necessary data structures to do so.  If we are just generating a
# list, this is irrelevant but harmless.

sub initOutput {

    my ($self) = @_;
    
    $self->{tree_stack} = [];
    $self->{comma_stack} = [];
    
    return;
}


sub process_int_ids {
    
    my ($request, $record) = @_;
    
    return unless $request->{block_hash}{extids};

    # my $make_ids = $request->clean_param('extids');
    # $make_ids = 1 if ! $request->param_given('extids') && $request->output_vocab eq 'com';
    
    # return unless $make_ids;
    
    # $request->delete_output_field('record_type');
    
    foreach my $f ( qw(interval_no parent_no) )
    {
	$record->{$f} = generate_identifier('INT', $record->{$f}) if defined $record->{$f};
	# $record->{$f} = "$IDP{INT}:$record->{$f}" if defined $record->{$f};
    }
    
    foreach my $f ( qw(scale_no) )
    {
	$record->{$f} = generate_identifier('TSC', $record->{$f}) if defined $record->{$f};
	# $record->{$f} = "scl$record->{$f}" if defined $record->{$f};
    }
    
    $record->{reference_no} = generate_identifier('REF', $record->{reference_no}) 
	if defined $record->{reference_no};
}

1;
