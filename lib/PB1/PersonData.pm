#
# PersonData
# 
# A class that returns information from the PaleoDB database about a single
# registered database user or a category of users.  This is a subclass of
# DataQuery.
# 
# Author: Michael McClennen

use strict;

package PB1::PersonData;

use HTTP::Validate qw(:validators);

use Carp qw(carp croak);

our (@REQUIRES_ROLE) = qw(PB1::CommonData);

use Moo::Role;

# initialize ( )
# 
# This routine is called by the data service to initialize this class.

sub initialize {

    my ($class, $ds) = @_;
    
   # Define the basic output block for person data.
    
    $ds->define_block('1.1:people:basic' =>
	{ select => [ qw(p.person_no p.name p.country p.institution
			 p.email p.is_authorizer) ] },
	{ output => 'person_no', com_name => 'oid' },
	    "A positive integer that uniquely identifies this database contributor",
	{ output => 'record_type', com_name => 'typ', com_value => 'prs', value => 'person' },
	    "The type of this object: {value} for a database contributor",
	{ output => 'name', com_name => 'nam' },
	    "The person's name",
	{ output => 'institution', com_name => 'ist' },
	    "The person's institution",
	{ output => 'country', com_name => 'ctr' },
	    "The database contributor's country");
    
    # Then some rulesets.
    
    $ds->define_ruleset('1.1:people:specifier' => 
	{ param => 'id', valid => POS_VALUE, alias => 'person_id' },
	    "The numeric identifier of the person to select");

    $ds->define_ruleset('1.1:people:selector' => 
	{ param => 'name', valid => ANY_VALUE },
	    "A name, in either order: 'J. Smith' or 'Smith, J.' with C<%> as a wildcard");

    $ds->define_ruleset('1.1:people:single' => 
	{ allow => '1.1:people:specifier' },
	{ allow => '1.1:special_params' });
    
    $ds->define_ruleset('1.1:people:list' => 
	{ require => '1.1:people:selector' },
	{ allow => '1.1:special_params' });
}
    

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
    
    my $dbh = $self->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $self->clean_param('id');
    
    die "Bad identifier '$id'" unless defined $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->generate_query_fields('p');
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('c', $self->{select_tables});
    
    # Generate the main query.
    
    $self->{main_sql} = "
	SELECT $fields
	FROM person as p
		$join_list
        WHERE p.person_no = $id";
    
    print $self->{main_sql} . "\n\n" if $self->debug;
    
    $self->{main_record} = $dbh->selectrow_hashref($self->{main_sql});
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
    
    my $dbh = $self->get_connection;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->select_string('p');
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my (@filters, $tables);
    
    @filters = $self->generateQueryFilters('p', $self->{select_tables});
    push @filters, "1=1" unless @filters;
    
    my $filter_string = join(q{ and }, @filters);
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->sql_limit_clause(1);
    my $count = $self->sql_count_clause;
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('p', $self->{select_tables});
    
    # Generate the main query
    
    $self->{main_sql} = "
	SELECT $count $fields
	FROM person as p
		$join_list
        WHERE $filter_string
	ORDER BY p.reversed_name $limit";
    
    print STDERR $self->{main_sql} . "\n\n" if $self->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
}


sub generateQueryFilters {

    my ($self, $mt, $tables_ref) = @_;
    
    my $dbh = $self->{dbh};
    my @filters;
    
    # Check for parameter 'name'
    
    if ( exists $self->{clean_params}{name} )
    {
	my $name = $dbh->quote($self->{clean_params}{name});
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
