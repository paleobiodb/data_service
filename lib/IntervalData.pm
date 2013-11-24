#
# IntervalQuery
# 
# A class that returns information from the PaleoDB database about time intervals.
# This is a subclass of PBDataQuery.
# 
# Author: Michael McClennen

package IntervalData;

use strict;
use base 'DataService::Base';

use Carp qw(carp croak);

use PBDBData qw(generateReference);
use IntervalTables qw($INTERVAL_DATA $INTERVAL_MAP $SCALE_DATA $SCALE_MAP $SCALE_LEVEL_DATA);


our (%SELECT, %OUTPUT, %PROC, %TABLES);

$SELECT{basic} =
    "i.interval_no, i.interval_name, i.abbrev, sm.scale_no, sm.level, sm.parent_no, sm.color, i.base_age, i.top_age, i.reference_no";

$OUTPUT{basic} =
   [
    { rec => 'interval_no', com => 'oid',
	doc => "A positive integer that uniquely identifies this interval"},
    { rec => 'record_type', com => 'typ', com_value => 'int', value => 'interval',
        doc => "The type of this object: 'int' for an interval" },
    { rec => 'scale_no', com => 'sid',
	doc => "The time scale in which this interval lies.  An interval may be reported more than once, as a member of different time scales" },
    { rec => 'level', com => 'lvl',
        doc => "The level within the time scale to which this interval belongs" },
    { rec => 'scales', com => 'sca',
        doc => "The time scale(s) and level(s) with which this interval is associated" },
    { rec => 'interval_name', com => 'nam',
	doc => "The name of this interval" },
    { rec => 'abbrev', com => 'abr',
        doc => "The standard abbreviation for the interval name, if any" },
    { rec => 'parent_no', com => 'pid',
        doc => "The identifier of the parent interval" },
    { rec => 'color', com => 'col',
        doc => "The standard color for displaying this interval" },
    { rec => 'top_age', com => 'lag',
        doc => "The late age boundary of this interval (in Ma)" },
    { rec => 'base_age', com => 'eag',
        doc => "The early age boundary of this interval (in Ma)" },
    { rec => 'reference_no', com => 'rid', json_list => 1,
        doc => "The identifier(s) of the references from which this data was entered" },
   ];

$PROC{basic} = 
   [
    { rec => 'scale_no', add => 'scales', use_main => 1, code => \&generateScaleEntry },
   ];

$SELECT{ref} = "r.author1init as r_ai1, r.author1last as r_al1, r.author2init as r_ai2, r.author2last as r_al2, r.otherauthors as r_oa, r.pubyr as r_pubyr, r.reftitle as r_reftitle, r.pubtitle as r_pubtitle, r.editors as r_editors, r.pubvol as r_pubvol, r.pubno as r_pubno, r.firstpage as r_fp, r.lastpage as r_lp";

$TABLES{ref} = 'r';

$PROC{ref} = 
   [
    { rec => 'r_al1', add => 'ref_list', use_main => 1, code => \&DataService::Base::generateReference },
   ];

$OUTPUT{ref} =
   [
    { rec => 'ref_list', pbdb => 'references', com => 'ref',
	doc => "The reference(s) associated with this collection (as formatted text)" },
   ];



# get ( )
# 
# Query for all relevant information about the interval specified by the
# 'id' parameter.  Returns true if the query succeeded, false otherwise.

sub get {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # Make sure we have a valid id number.
    
    my $id = $self->{params}{id};
    
    die "Bad identifier '$id'" unless defined $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->generate_query_fields('i');
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('i', $self->{select_tables});
    
    # Generate the main query.
    
    $self->{main_sql} = "
	SELECT $fields
	FROM $INTERVAL_DATA as i LEFT JOIN $SCALE_MAP as sm using (interval_no)
		$join_list
        WHERE i.interval_no = $id
	GROUP BY i.interval_no";
    
    print $self->{main_sql} . "\n\n" if $PBDB_Data::DEBUG;
    
    $self->{main_record} = $dbh->selectrow_hashref($self->{main_sql});
    
    # Abort if we couldn't retrieve the record.
    
    return unless $self->{main_record};
    
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
    
    my $dbh = $self->{dbh};
    
    my $tables = {};
    my $calc = '';
    
    # If we were asked for a hierarchy, indicate that we will need to process
    # the result set before sending it.
    
    $self->{process_resultset} = \&generateHierarchy if $self->{op} eq 'hierarchy';
    
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
	
	if ( $filter_string =~ /all/ )
	{
	    push @filters, "sm.scale_no is not null";
	}
	else
	{
	    push @filters, "sm.scale_no in ($filter_string)";
	}
    }
    
    if ( exists $self->{params}{min_ma} )
    {
	my $min = $self->{params}{min_ma};
	push @filters, "i.top_age >= $min";
    }
    
    if ( exists $self->{params}{max_ma} )
    {
	my $max = $self->{params}{max_ma};
	push @filters, "i.base_age <= $max";
    }
    
    push @filters, "1=1" unless @filters;
    
    # Get the results in the specified order
    
    my $order_expr = $self->{params}{order} eq 'younger' ?
	"ORDER BY sm.scale_no, sm.level, i.top_age" :
	    "ORDER BY sm.scale_no, sm.level, i.base_age desc";
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->generate_query_fields('i');
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('i', $self->{select_tables});
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->generateLimitClause();
    
    # If we were asked to count rows, modify the query accordingly
    
    if ( $self->{params}{count} )
    {
	$calc = 'SQL_CALC_FOUND_ROWS';
    }
    
    # Generate the main query.
    
    my $filter_list = join(' and ', @filters);
    
    $self->{main_sql} = "
	SELECT $calc $fields
	FROM $INTERVAL_DATA as i LEFT JOIN $SCALE_MAP as sm using (interval_no)
		$join_list
	WHERE $filter_list
	$order_expr
	$limit";
    
    print $self->{main_sql} . "\n\n" if $PBDB_Data::DEBUG;
    
    # Then prepare and execute the main query and the secondary query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    if ( $calc )
    {
	($self->{result_count}) = $dbh->selectrow_array("SELECT FOUND_ROWS()");
    }
    
    return 1;
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


# generateScaleEntry ( )
# 
# Return a list of (scale_no, level).

sub generateScaleEntry {
    
    my ($self, $rowref) = @_;
    
    return [$rowref->{scale_no}, $rowref->{level}];
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
