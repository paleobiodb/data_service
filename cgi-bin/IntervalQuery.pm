#
# IntervalQuery
# 
# A class that returns information from the PaleoDB database about time intervals.
# This is a subclass of PBDataQuery.
# 
# Author: Michael McClennen

package IntervalQuery;

use strict;
use base 'DataQuery';

use Carp qw(carp croak);


our (%SELECT, %OUTPUT, %PROC, %TABLES);

$SELECT{single} = $SELECT{list} = "i.interval_no, i.interval_name, i.abbrev, i.level, i.parent_no, i.color, i.base_age, i.top_age, i.reference_no";

$OUTPUT{single} = $OUTPUT{list} = 
   [
    { rec => 'interval_no', com => 'oid',
	doc => "A positive integer that uniquely identifies this interval"},
    { rec => 'record_type', com => 'typ', com_value => 'int', value => 'interval',
        doc => "The type of this object: 'int' for an interval" },
    { rec => 'interval_name', com => 'nam',
	doc => "The name of this interval" },
    { rec => 'abbrev', com => 'abr',
        doc => "The standard abbreviation for the interval name, if any" },
    { rec => 'level', com => 'lvl',
        doc => "The level of this interval: eon=1, era=2, period=3, epoch=4, age=5" },
    { rec => 'parent_no', com => 'par',
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

$SELECT{ref} = "r.author1init as r_ai1, r.author1last as r_al1, r.author2init as r_ai2, r.author2last as r_al2, r.otherauthors as r_oa, r.pubyr as r_pubyr, r.reftitle as r_reftitle, r.pubtitle as r_pubtitle, r.editors as r_editors, r.pubvol as r_pubvol, r.pubno as r_pubno, r.firstpage as r_fp, r.lastpage as r_lp";

$TABLES{ref} = 'r';

$PROC{ref} = 
   [
    { rec => 'r_al1', add => 'ref_list', use_main => 1, code => \&DataQuery::generateReference },
   ];

$OUTPUT{ref} =
   [
    { rec => 'ref_list', pbdb => 'references', com => 'ref',
	doc => "The reference(s) associated with this collection (as formatted text)" },
   ];

our (%DOC_ORDER);

$DOC_ORDER{'single'} = ['single', 'ref'];
$DOC_ORDER{'list'} = ['single', 'ref'];


# fetchSingle ( )
# 
# Query for all relevant information about the collection specified by the
# 'id' parameter.  Returns true if the query succeeded, false otherwise.

sub fetchSingle {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # Make sure we have a valid id number.
    
    my $id = $self->{params}{id};
    
    die "Bad identifier '$id'" unless defined $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = join(', ', @{$self->{select_list}});
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('i', $self->{select_tables});
    
    # Generate the main query.
    
    $self->{main_sql} = "
	SELECT $fields
	FROM interval_map as i $join_list
        WHERE i.interval_no = $id
	GROUP BY i.interval_no";
    
    $self->{main_record} = $dbh->selectrow_hashref($self->{main_sql});
    
    # Abort if we couldn't retrieve the record.
    
    return unless $self->{main_record};
    
    return 1;
}


# fetchMultiple ( )
# 
# Query the database for basic info about all intervals.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub fetchMultiple {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    my $tables = {};
    my $calc = '';
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = "i.level is not null";
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = join(', ', @{$self->{select_list}});
    
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
	FROM interval_map as i $join_list
	WHERE $filter_list
	ORDER BY i.level, i.top_age
	$limit";
    
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


1;
