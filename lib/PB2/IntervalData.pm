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

our (@REQUIRES_ROLE) = qw(PB2::CommonData);

use Moo::Role;

no warnings 'numeric';


# Store the basic data about each interval and scale.

our (%INTERVAL_DATA, %SCALE_DATA, %SCALE_LEVEL_DATA);
our (%BOUNDARY_LIST, %BOUNDARY_MAP);

# initialize ( )
# 
# This routine is called automatically by the data service to initialize this
# class.

sub initialize {

    my ($class, $ds) = @_;
    
    # Define output blocks for displaying time interval information
    
    $ds->define_block('1.2:intervals:basic' =>
	{ select => [ qw(i.interval_no i.interval_name i.abbrev sm.scale_no sm.level
			 sm.parent_no sm.color i.early_age i.late_age i.reference_no) ] },
	{ output => 'interval_no', com_name => 'oid' },
	    "A positive integer that uniquely identifies this interval",
	{ output => 'record_type', com_name => 'typ', com_value => 'int', value => 'interval' },
	    "The type of this object: 'int' for an interval",
	{ output => 'scale_no', com_name => 'sca' },
	    "The time scale in which this interval lies.  An interval may be reported more than",
	    "once, as a member of different time scales",
	{ output => 'level', com_name => 'lvl' },
	    "The level within the time scale to which this interval belongs",
	{ output => 'interval_name', com_name => 'nam' },
	    "The name of this interval",
	{ output => 'abbrev', com_name => 'abr' },
	    "The standard abbreviation for the interval name, if any",
	{ output => 'parent_no', com_name => 'pid' },
	    "The identifier of the parent interval",
	{ output => 'color', com_name => 'col' },
	    "The standard color for displaying this interval",
	{ output => 'late_age', com_name => 'lag' },
	    "The late age boundary of this interval (in Ma)",
	{ output => 'early_age', com_name => 'eag' },
	    "The early age boundary of this interval (in Ma)",
	{ set => 'reference_no', append => 1 },
	{ output => 'reference_no', com_name => 'rid', text_join => ', ' },
	    "The identifier(s) of the references from which this data was entered");
    
    $ds->define_block('1.2:scales:basic' =>
	{ select => [ 'sc.scale_no', 'sc.scale_name', 'sc.levels as num_levels',
		      'sc.early_age', 'sc.late_age', 'sc.reference_no',
		      'sl.level', 'sl.level_name' ] },
	{ output => 'scale_no', com_name => 'oid' },
	    "A positive integer that uniquely identifies this time scale",
	{ output => 'record_type', com_name => 'typ', com_value => 'scl', value => 'timescale' },
	    "The type of this object: 'scl' for a time scale",
	{ output => 'scale_name', com_name => 'nam' },
	    "The name of this time scale",
	{ output => 'num_levels', com_name => 'nlv' },
	    "The number of levels into which this time scale is organized",
	{ output => 'level_list', com_name => 'lvs', sub_record => '1.2:scales:level',
	  if_format => 'json' },
	    "A list of levels associated with this time scale, if more than one.",
	    "This field will only be present in C<json> responses.",
	{ output => 'level', com_name => 'lvl', not_format => 'json' },
	    "Level number.",
	{ output => 'level_name', com_name => 'nam', not_format => 'json' },
	    "Level name",
	{ output => 'early_age', com_name => 'eag' },
	    "The early bound of this time scale, in Ma",
	{ output => 'late_age', com_name => 'lag' },
	    "The late bound of this time scale, in Ma",
	{ set => 'reference_no', append => 1 },
	{ output => 'reference_no', com_name => 'rid' },
	    "The identifier(s) of the references from which this data was entered");
    
    $ds->define_block('1.2:scales:level' =>
	{ output => 'level', com_name => 'lvl' },
	    "Level number",
	{ output => 'level_name', com_name => 'nam' },
	    "Level name");
    
    # Then define some rulesets to describe the parameters accepted by the
    # operations defined here.
    
    $ds->define_ruleset('1.2:intervals:specifier' =>
	{ param => 'id', valid => POS_VALUE },
	    "Return the interval corresponding to the specified identifier. (REQUIRED)");
    
    $ds->define_ruleset('1.2:intervals:selector' => 
	{ param => 'scale_id', valid => [POS_VALUE, ENUM_VALUE('all')], 
	  list => ',', alias => 'scale',
	  error => "the value of {param} should be a list of positive integers or 'all'" },
	    "Return intervals from the specified time scale(s).",
	    "The value of this parameter should be a list of positive integers or 'all'",
	{ param => 'id', valid => POS_VALUE, list => ',' },
	    "Return intervals that have the specified identifiers",
	{ param => 'min_ma', valid => DECI_VALUE(0) },
	    "Return only intervals that are at least this old",
	{ param => 'max_ma', valid => DECI_VALUE(0) },
	    "Return only intervals that are at most this old",
	{ optional => 'order', valid => ENUM_VALUE('older', 'younger'), default => 'younger' },
	    "Return the intervals in order starting as specified.  Possible values include ",
	    "C<older>, C<younger>.  Defaults to C<younger>.");
    
    $ds->define_ruleset('1.2:intervals:list' => 
	{ allow => '1.2:intervals:selector' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");

    $ds->define_ruleset('1.2:intervals:single' => 
	{ allow => '1.2:intervals:specifier' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:scales:specifier' =>
	{ param => 'id', valid => POS_VALUE },
	    "Return the time scale corresponding to the specified identifier. (REQUIRED)");
    
    $ds->define_ruleset('1.2:scales:selector' =>
	"To return all time scales, use this URL path with no parameters.",
	{ param => 'id', valid => POS_VALUE, list => ',' },
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
    
    # Make sure we have a valid id number.
    
    my $id = $request->clean_param('id');
    
    die "Bad identifier '$id'" unless defined $id and $id =~ /^\d+$/;
    
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
        WHERE i.interval_no = $id
	GROUP BY i.interval_no";
    
    print STDERR $request->{main_sql} . "\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
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
    
    if ( defined $scale && $scale eq 'all' )
    {
	push @filters, "sm.level is not null";
    }
    
    elsif ( @scale_ids )
    {
	my $filter_string = join(',', @scale_ids);
	
	if ( $filter_string !~ /all/ )
	{
	    push @filters, "sm.scale_no in ($filter_string)";
	}
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
    
    push @filters, "1=1" unless @filters;
    
    # Get the results in the specified order
    
    my $order = $request->clean_param('order');
    
    my $order_expr = defined $order && $order eq 'younger' ?
	"ORDER BY sm.scale_no, sm.level, i.late_age" :
	    "ORDER BY sm.scale_no, sm.level, i.early_age desc";
    
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
	WHERE $filter_string ORDER BY scale_no, level
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
	    
	    push @{$scale{$scale_no}{level_list}}, { level => $row->{level},
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
	
	if ( $r->{level} == 1 )
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
    
    return if %INTERVAL_DATA;
    
    my (%interval_data);
    
    # First read in a list of all the intervals and put them in a hash indexed
    # by interval_no.
    
    my $sql = "SELECT * FROM $INTERVAL_DATA";
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    if ( ref $result eq 'ARRAY' )
    {
	my $interval_no;
	
	foreach my $i ( @$result )
	{
	    next unless $interval_no = $i->{interval_no};
	    $interval_data{$interval_no} = $i;
	}
    }
    
    # Then read in a list of all the scales and put them in a hash indexed by
    # scale_no.
    
    $sql = "SELECT * FROM $SCALE_DATA";
    
    $result = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    if ( ref $result eq 'ARRAY' )
    {
	my $scale_no;
	
	foreach my $s ( @$result )
	{
	    next unless $scale_no = $s->{scale_no};
	    $SCALE_DATA{$scale_no} = $s;
	}
    }
    
    # Then read in a list of the scale levels and fill them in to the
    # %SCALE_DATA hash.
    
    $sql = "SELECT * FROM $SCALE_LEVEL_DATA";
    
    $result = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    if ( ref $result eq 'ARRAY' )
    {
	my ($scale_no, $level, %sample_list);
	
	foreach my $s ( @$result )
	{
	    next unless $scale_no = $s->{scale_no};
	    next unless $level = $s->{level};
	    $SCALE_LEVEL_DATA{$scale_no}{$level} = $s->{level_name};
	    push @{$sample_list{$scale_no}}, $level if $s->{sample};
	}
	
	# The 'sample_list' field will be a list of the levels at which
	# diversity statistics should be counted.  So, for example, for the
	# standard timescale (scale_no = 1), the "Eon" and "Era" levels are
	# just too coarse for diversity computations to make any sense.  It
	# only makes sense to do them at "Period" and below.  We sort the list
	# from largest to smallest, which means from finest-resolution to coarsest.
	
	foreach $scale_no ( keys %SCALE_DATA )
	{
	    $SCALE_DATA{$scale_no}{sample_list} = [ map { "L$_" } sort { $b <=> $a } @{$sample_list{$scale_no}} ];
	}
    }
    
    # Now read in the mapping from interval numbers to scale levels and parent
    # intervals. 
    
    $sql = "SELECT scale_no, level, interval_no, parent_no FROM $SCALE_MAP";
    
    $result = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    if ( ref $result eq 'ARRAY' )
    {
	my ($interval_no, $scale_no, $level, $parent_no);
	
	foreach my $m ( @$result )
	{
	    next unless $scale_no = $m->{scale_no};
	    next unless $level = $m->{level};
	    next unless $interval_no = $m->{interval_no};
	    next unless $parent_no = $m->{parent_no};
	    
	    $INTERVAL_DATA{$scale_no}{$interval_no} = { %{$interval_data{$interval_no}}, 
							parent_no => $parent_no,
							level => $level + 0,
						        "L$level" => $interval_no };
	}
	
	# Now compute boundary lists and parent level mappings.
	
	foreach $scale_no ( keys %SCALE_DATA )
	{
	    my (%boundary_list, %boundary_map);
	    
	    foreach $interval_no ( keys %{$INTERVAL_DATA{$scale_no}} )
	    {
		my $i = $INTERVAL_DATA{$scale_no}{$interval_no};
		my $parent_no = $i->{parent_no};
		my $level = $i->{level};
		my $boundary_age = $i->{early_age};
		
		# Add this interval's boundary to the boundary list and
		# boundary map for its level.
		
		push @{$boundary_list{$level}}, $boundary_age;
		$boundary_map{$level}{$boundary_age} = $i;
		
		# Iteratively compute the level mapping for this interval and
		# all its parents.  So, for example, if we know that interval
		# 50 is at level 5 and its parent is 27 which is at level 4,
		# then the "L5" value for interval 50 is 50 and the "L4" value
		# is 27.  If the parent of 27 is 18, then the "L3" value for
		# 50 is 18.  And so on.
		
		while ( my $p = $INTERVAL_DATA{$scale_no}{$parent_no} )
		{
		    $i->{"L$p->{level}"} = $parent_no;
		    $parent_no = $p->{parent_no};
		}
	    }
	    
	    # Now sort each of the boundary lists (oldest to youngest) and
	    # store them in the appropriate package variable.
	    
	    foreach my $level ( keys %boundary_list )
	    {
		$BOUNDARY_LIST{$scale_no}{$level} = [ sort { $b <=> $a } @{$boundary_list{$level}} ];
		$BOUNDARY_MAP{$scale_no}{$level} = $boundary_map{$level};
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

1;
