#
# PersonData
# 
# A class that returns information from the PaleoDB database about a single
# registered database user or a category of users.  This is a subclass of
# DataQuery.
# 
# Author: Michael McClennen

package PersonData;

use strict;
use base 'DataService::Base';

use Carp qw(carp croak);


our (%OUTPUT, %SELECT, %TABLES);

$SELECT{basic} = $SELECT{basic} = "p.person_no, p.name, p.country, p.institution, p.email, p.is_authorizer";

$OUTPUT{basic} = $OUTPUT{basic} = 
   [
    { rec => 'person_no', com => 'oid',
      doc => "A positive integer that uniquely identifies this database contributor" },
    { rec => 'record_type', com => 'typ', com_value => 'prs', value => 'person',
      doc => "The type of this object: {value} for a database contributor" },
    { rec => 'name', com => 'nam',
      doc => "The person's name" },
    { rec => 'institution', com => 'ist',
      doc => "The person's institution" },
    { rec => 'country', com => 'ctr',
      doc => "The database contributor's country" },
    { rec => 'email', com => 'eml',
      doc => "The person's e-mail address" },
   ];

$SELECT{toprank} = "p.person_no, p.name, p.country, p.institution";

$TABLES{toprank} = ['c'];

$OUTPUT{toprank} =
   [
    { rec => 'person_no', com => 'oid',
      doc => "A positive integer that uniquely identifies this database contributor" },
    { rec => 'record_type', com => 'typ', com_value => 'prs', value => 'person',
      doc => "The type of this object: {value} for a database contributor" },
    { rec => 'name', com => 'nam',
      doc => "The person's name" },
    { rec => 'institution', com => 'ist',
      doc => "The person's institution" },
    { rec => 'country', com => 'ctr',
      doc => "The database contributor's country" },
   ];

# get ( )
# 
# Query for all relevant information about the requested taxon.
# 
# Options may have been set previously by methods of this class or of the
# parent class DataQuery.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub get {

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
    
    my ($join_list) = $self->generateJoinList('c', $self->{select_tables});
    
    # Generate the main query.
    
    $self->{main_sql} = "
	SELECT $fields
	FROM person as p
		$join_list
        WHERE p.person_no = $id";
    
    print $self->{main_sql} . "\n\n" if $PBDB_Data::DEBUG;
    
    $self->{main_record} = $dbh->selectrow_hashref($self->{main_sql});
    
    # Abort if we couldn't retrieve the record.
    
    return unless $self->{main_record};
    
    # Return true otherwise.

    return 1;
}


# list ( )
# 
# Query the database for basic info about all taxa satisfying the conditions
# previously specified by a call to setParameters.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub list {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    my $op = $self->{op};
    
    my $calc = '';
    my $limit = '';
    my $order = '';
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = join(', ', @{$self->{select_list}});
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my (@filters, $tables);
    
    if ( $op eq 'toprank' )
    {
	@filters = CollectionQuery::generateQueryFilters($self, 'c', $self->{select_tables});
	
	if ( defined $self->{params}{subject} && $self->{params}{subject} eq 'occs' )
	{
	    $fields .= ", count(o.occurrence_no) as instance_count";
	    $self->{select_tables}{o} = 1;
	}
	else
	{
	    $fields .= ", count(c.collection_no) as instance_count";
	}
	
	$order = 'GROUP BY p.person_no ORDER BY instance_count DESC';
	$limit = 'LIMIT 10';
    }
    
    else
    {
	@filters = $self->generateQueryFilters('p', $self->{select_tables});
	
	# If a query limit has been specified, modify the query accordingly.
	
	$limit = $self->generateLimitClause();
    }
    
    push @filters, "1=1" unless @filters;
    
    my $filter_string = join(q{ and }, @filters);
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('p', $self->{select_tables});
    
    # If we were asked to count rows, modify the query accordingly
    
    if ( $self->{params}{count} )
    {
	$calc = 'SQL_CALC_FOUND_ROWS';
    }
    
    # Generate the main query
    
    $self->{main_sql} = "
	SELECT $calc $fields
	FROM person as p
		$join_list
        WHERE $filter_string
	$order $limit";
    
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


sub generateQueryFilters {

    my ($self, $mt, $tables_ref) = @_;
    
    my $dbh = $self->{dbh};
    my @filters;
    
    # Check for parameter 'name'
    
    if ( exists $self->{params}{name} )
    {
	my $name = $dbh->quote($self->{params}{name});
	push @filters, "name like $name or reversed_name like $name";
    }
    
    return @filters;
}

sub generateJoinList {
    
    my ($self, $mt, $tables) = @_;
    
    my $join_list = '';
    
    # Return an empty string unless we actually have some joins to make
    
    return $join_list unless ref $tables eq 'HASH' and %$tables;
    
    # Some tables imply others.
    
    $tables->{o} = 1 if $tables->{t};
    $tables->{c} = 1 if $tables->{o};
    
    # Create the necessary join expressions.
    
    $join_list .= "JOIN coll_matrix as c on p.person_no = c.authorizer_no\n"
	if $tables->{c};
    $join_list .= "JOIN occ_matrix as o using (collection_no)\n"
	if $tables->{o};
    $join_list .= "JOIN taxon_trees as t using (orig_no)\n"
	if $tables->{t};
    
    return $join_list;
}


1;
