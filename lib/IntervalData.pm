#
# IntervalQuery
# 
# A class that returns information from the PaleoDB database about time intervals.
# This is a subclass of PBDataQuery.
# 
# Author: Michael McClennen

package IntervalData;

use strict;
use base 'Web::DataService::Request';

use Web::DataService qw( :validators );

use Carp qw(carp croak);

use CommonData qw(generateReference);
use IntervalTables qw($INTERVAL_DATA $INTERVAL_MAP $SCALE_DATA $SCALE_MAP $SCALE_LEVEL_DATA);

our (@REQUIRES_CLASS) = qw(CommonData);


# initialize ( )
# 
# This routine is called automatically by the data service to initialize this
# class.

sub initialize {

    my ($class, $ds) = @_;
    
    # Define output blocks for displaying time interval information
    
    $ds->define_block('1.1:intervals:basic' =>
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
    
    $ds->define_block('1.1:scales:basic' =>
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
	{ output => 'level_list', com_name => 'lvs', rule => '1.1:scales:level',
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
    
    $ds->define_block('1.1:scales:level' =>
	{ output => 'level', com_name => 'lvl' },
	    "Level number",
	{ output => 'level_name', com_name => 'nam' },
	    "Level name");
    
    # Then define some rulesets to describe the parameters accepted by the
    # operations defined here.
    
    $ds->define_ruleset('1.1:intervals:specifier' =>
	{ param => 'id', valid => POS_VALUE },
	    "Return the interval corresponding to the specified identifier. (REQUIRED)");
    
    $ds->define_ruleset('1.1:intervals:selector' => 
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
    
    $ds->define_ruleset('1.1:intervals:list' => 
	{ allow => '1.1:intervals:selector' },
	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common> with this request");

    $ds->define_ruleset('1.1:intervals:single' => 
	{ allow => '1.1:intervals:specifier' },
	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common> with this request");
    
    $ds->define_ruleset('1.1:scales:specifier' =>
	{ param => 'id', valid => POS_VALUE },
	    "Return the time scale corresponding to the specified identifier. (REQUIRED)");
    
    $ds->define_ruleset('1.1:scales:selector' =>
	"To return all time scales, use this URL path with no parameters.",
	{ param => 'id', valid => POS_VALUE, list => ',' },
	    "Return intervals that have the specified identifier(s).",
	    "You may specify more than one, as a comma-separated list.");
    
    $ds->define_ruleset('1.1:scales:single' =>
	{ require => '1.1:scales:specifier' },
	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common> with this request");
    
    $ds->define_ruleset('1.1:scales:list' =>
	{ allow => '1.1:scales:selector' },
	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common> with this request");
    
}


# get ( )
# 
# Query for all relevant information about the interval specified by the
# 'id' parameter.  Returns true if the query succeeded, false otherwise.

sub get {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_dbh;
    
    # Make sure we have a valid id number.
    
    my $id = $self->{params}{id};
    
    die "Bad identifier '$id'" unless defined $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->select_string({ mt => 'i' });
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('i', $self->{select_tables});
    
    # Generate the main query.
    
    $self->{main_sql} = "
	SELECT $fields
	FROM $INTERVAL_DATA as i LEFT JOIN $SCALE_MAP as sm using (interval_no)
		$join_list
        WHERE i.interval_no = $id
	GROUP BY i.interval_no";
    
    print STDERR $self->{main_sql} . "\n\n" if $self->debug;
    
    $self->{main_record} = $dbh->selectrow_hashref($self->{main_sql});
    
    # If we were asked to get the count, then do so
    
    $self->sql_count_rows;
    
    return 1;
}


# list ( )
# 
# Query the database for basic info about all specified intervals.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub list {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_dbh;
    
    # If we were asked for a hierarchy, indicate that we will need to process
    # the result set before sending it.
    
    $self->{process_resultset} = \&generateHierarchy if 
	defined $self->{op} && $self->{op} eq 'hierarchy';
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters;
    
    if ( defined $self->{params}{scale} and $self->{params}{scale} eq 'all' )
    {
	push @filters, "sm.level is not null";
    }
    
    elsif ( ref $self->{params}{scale_id} eq 'ARRAY' )
    {
	my $filter_string = join(',', @{$self->{params}{scale_id}});
	
	if ( $filter_string !~ /all/ )
	{
	    push @filters, "sm.scale_no in ($filter_string)";
	}
    }
    
    if ( exists $self->{params}{min_ma} )
    {
	my $min = $self->{params}{min_ma};
	push @filters, "i.late_age >= $min";
    }
    
    if ( exists $self->{params}{max_ma} )
    {
	my $max = $self->{params}{max_ma};
	push @filters, "i.early_age <= $max";
    }
    
    push @filters, "1=1" unless @filters;
    
    # Get the results in the specified order
    
    my $order_expr = defined $self->{params}{order} && $self->{params}{order} eq 'younger' ?
	"ORDER BY sm.scale_no, sm.level, i.late_age" :
	    "ORDER BY sm.scale_no, sm.level, i.early_age desc";
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->select_string({ mt => 'i' });
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('i', $self->{select_tables});
    
    # If a query limit has been specified, modify the query accordingly.
    # If we were asked to count rows, modify the query accordingly
    
    my $limit = $self->sql_limit_clause(1);    
    my $calc = $self->sql_count_clause;
    
    # Generate the main query.
    
    my $filter_string = join(' and ', @filters);
    
    $self->{main_sql} = "
	SELECT $calc $fields
	FROM $INTERVAL_DATA as i LEFT JOIN $SCALE_MAP as sm using (interval_no)
		$join_list
	WHERE $filter_string
	$order_expr
	$limit";
    
    print STDERR $self->{main_sql} . "\n\n" if $self->debug;
    
    # Then prepare and execute the main query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $self->sql_count_rows;
        
    return 1;
}


# list_scales ( )
# 
# Query the database for time scales.

sub list_scales {
    
    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_dbh;
    my @filters;
    
    # Get the list of database fields.
    
    my $fields = $self->select_string({ mt => 'sc' });
    
    # Construct the list of filter expressions.
    
    if ( my $id = $self->clean_param('id') )
    {
	if ( ref $id eq 'ARRAY' )
	{
	    my $id_list = join(',', @$id);
	    push @filters, "scale_no in ($id_list)";
	}
	
	else
	{
	    push @filters, "scale_no = $id";
	}
    }
    
    push @filters, "1=1" unless @filters;
    
    # If a query limit has been specified, modify the query accordingly.
    # If we were asked to count rows, modify the query accordingly
    
    my $limit = $self->sql_limit_clause(1);    
    my $calc = $self->sql_count_clause;
    my $order_expr = '';
    
    # Generate the main query.
    
    my $filter_string = join(' and ', @filters);
    
    $self->{main_sql} = "
	SELECT $calc $fields
	FROM $SCALE_DATA as sc LEFT JOIN $SCALE_LEVEL_DATA as sl using (scale_no)
	WHERE $filter_string ORDER BY scale_no, level
	$limit";
    
    print STDERR $self->{main_sql} . "\n\n" if $self->debug;
    
    # Then prepare and execute the main query.
    
    my $sth = $dbh->prepare($self->{main_sql});
    $sth->execute();
    
    # If the result is to be returned in JSON format, we need to reorganize it:
    
    my (@scales, %scale);
    
    if ( $self->{format} eq 'json' )
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
	
	$self->{main_result} = \@scales;
    }
    
    # Otherwise we can just return the statement handle and let the data
    # service code do the rest.
    
    else
    {
	$self->{main_sth} = $sth;
    }
    
    # If we were asked to get the count, then do so
    
    $self->sql_count_rows;
}


# generateJoinList ( tables )
# 
# Generate the actual join string indicated by the table hash.

sub generateJoinList {

    my ($self, $mt, $tables) = @_;
    
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

    my ($self, $rowref) = @_;
    
    return $rowref unless $self->{output_format} eq 'json';
    
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
