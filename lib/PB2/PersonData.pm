#
# PersonData
# 
# A class that returns information from the PaleoDB database about a single
# registered database user or a category of users.  This is a subclass of
# DataQuery.
# 
# Author: Michael McClennen

use strict;

package PB2::PersonData;

use TableDefs qw(%TABLE);
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);

use HTTP::Validate qw(:validators);

use Carp qw(carp croak);

our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::Authentication);

use Moo::Role;


# initialize ( )
# 
# This routine is called by the data service to initialize this class.

sub initialize {

    my ($class, $ds) = @_;
    
    # Define the basic output block for person data.
    
    $ds->define_set('1.2:people:contributor_status' =>
	{ value => 'active' },
	    "The person is an active database contributor.",
	{ value => 'disabled' },
	    "The person's account has been disabled, either temporarily or permanently.",
	{ value => 'deceased' },
	    "The person is deceased, and so their contributed records have been reassigned.");
    
    $ds->define_set('1.2:people:contributor_role' =>
	{ value =>'authorizer' },
	    "The person is a database authorizer.",
	{ value => 'enterer' },
	    "The person is a database enterer, working under the supervision of an authorizer.",
	{ value => 'student' },
	    "The person is a student, with fewer privileges than an enterer, also working",
	    "under the supervision of an authorizer.",
	{ value => 'guest' },
	    "The person has created an account, but has not yet been accepted as a database",
	    "contributor.");
    
    $ds->define_block('1.2:people:basic' =>
	{ select => [ 'wu.id as person_id', 'wu.real_name', 'wu.country',
		      'wu.institution', 'wu.role', 'wu.orcid', 
		      'wu.contributor_status as status' ] },
	{ set => '*', code => \&process_record },
	{ output => 'person_no', com_name => 'oid' },
	    "A positive integer that uniquely identifies this database contributor,",
	    "or else a unique record identifier for guest accounts.",
	{ output => 'record_type', com_name => 'typ', value => $IDP{PRS} },
	    "The type of this object: C<B<$IDP{PRS}>> for a database contributor",
	{ output => 'real_name', com_name => 'nam' },
	    "The person's name",
	{ output => 'institution', com_name => 'ist' },
	    "The person's institution, if known to the database.",
	{ output => 'country', com_name => 'ctr' },
	    "The person's country, if known to the database.",
	{ output => 'orcid', com_name => 'orc' },
	    "The person's ORCID, if known to the database.",
	{ output => 'role', com_name => 'rol' },
	    "The person's database role, if they are a database contributor. The",
	    "value of this field will be one of the following:",
	    $ds->document_set('1.2:people:contributor_role'),
	{ output => 'status', com_name => 'sta' },
	    "The person's status, if they are a database contributor. The value of",
	    "this field will be one of the following:",
	    $ds->document_set('1.2:people:contributor_status'));
    
    $ds->define_output_map('1.2:people:basic_map' =>
	{ value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for each record");
    
    # Then some rulesets.
    
    $ds->define_ruleset('1.2:people:specifier' => 
	{ param => 'id', valid => ANY_VALUE, alias => 'person_id' },
	    "The identifier of a database contributor, either as an external identifier or",
	    "an integer, or the user identifier string.",
	{ param => 'loggedin', valid => FLAG_VALUE },
	    "This parameter is only valid if given in a request accompanied by a cookie indicating",
	    "a still-valid login session. The information about the currently logged-in user is",
	    "returned.",
	{ at_most_one => ['id', 'loggedin'] });
    
    $ds->define_ruleset('1.2:people:selector' =>
	"One of the following parameters must be specified, to indicate which records to return:",
	{ param => 'all_records', valid => FLAG_VALUE },
	    "Return all database contributor records. This parameter does not require a value.",
	{ param => 'id', valid => ANY_VALUE, list => ',', alias => 'person_id' },
	    "Return only the records corresponding to the specified database contributor identifiers.",
	    "More than one can be given, as a comma-separated list.",
	    "These can be specified as integer or external identifiers, or as user identifier strings.",
	{ param => 'name', valid => ANY_VALUE },
	    "A string to be matched against person names.");
    
    $ds->define_ruleset('1.2:people:single' => 
	{ require => '1.2:people:specifier' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:people:basic_map' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.2:people:loggedin' =>
	{ allow => '1.2:special_params' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:people:basic_map' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.2:people:list' => 
	{ require => '1.2:people:selector' },
	{ allow => '1.2:common:select_crmod' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:people:basic_map' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    # $ds->define_ruleset('1.2:people:auto' =>
    # 	{ param => 'name', valid => ANY_VALUE },
    # 	    "A string of at least 3 characters, which will be matched against the beginning",
    # 	    "of each last name.",
    # 	"^You can also use any of the L<special parameters|node:special> with this request.");
}


# get ( )
# 
# Query for all relevant information about the requested taxon.
# 
# Options may have been set previously by methods of this class or of the
# parent class DataQuery.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub get_person {

    my ($request, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number, or else the 'loggedin' parameter.
    
    my ($person_no, $wing_id);
    
    if ( $arg && $arg eq 'loggedin' || $request->clean_param('loggedin') )
    {
	my $auth_info = $request->require_authentication();
	
	if ( $auth_info->{enterer_no} )
	{
	    $person_no = $dbh->quote($auth_info->{enterer_no});
	}
	
	elsif ( $auth_info->{user_id} )
	{
	    $wing_id = $dbh->quote($auth_info->{user_id});
	}
	
	else
	{
	    die $request->exception(401, "You must be logged in to execute this operation.");
	}
    }
    
    elsif ( defined ( my $id = $request->clean_param('id') ) )
    {
	if ( $id =~ /^[A-Z0-9-]{10,80}$/ )
	{
	    my $auth_info = $request->require_authentication();
	    $wing_id = $dbh->quote($id);
	}
	
	elsif ( $id && $id =~ /^\d+$/ )
	{
	    $person_no = $dbh->quote($id);
	}
	
	else
	{
	    die $request->exception(400, "E_BAD_PARAM: Bad value '$id' for 'id'");
	}
    }
    
    else
    {
	die $request->exception(400, "E_PARAM_ERROR: at least one parameter is required for this operation");
    }
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    # $request->substitute_select( cd => 'p' );
    
    my $fields = $request->select_string();
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether we should generate external identifiers.
    
    $request->strict_check;
    $request->extid_check;
    
    # Determine the necessary joins.
    
    # my ($join_list) = $request->generateJoinList('p', $request->tables_hash);
    
    # Generate the main query.
    
    if ( $wing_id )
    {
	if ( $request->has_block('1.2:common:crmod') )
	{
	    $fields =~ s/\$cd.created/wu.date_created/;
	    $fields =~ s/\$cd.modiied/wu.date_modified/;
	}
	
	$request->{main_sql} = "
	SELECT $fields, wu.person_no
	FROM $TABLE{WING_USERS} as wu
	WHERE id = $wing_id";
    }
    
    else
    {
	if ( $request->has_block('1.2:common:crmod') )
	{
	    $fields =~ s/\$cd.created/coalesce(wu.date_created, p.created) as created/;
	    $fields =~ s/\$cd.modified/coalesce(wu.date_modified, p.modified) as modified/;
	}
	
	$request->{main_sql} = "
	SELECT p.name as p_name, p.role as p_role, p.institution as p_institution,
		p.country as p_country, p.active as p_active, p.person_no, $fields 
	FROM $TABLE{PERSON_DATA} as p left join $TABLE{WING_USERS} as wu using (person_no)
	WHERE person_no = $person_no";
    }
    
    print STDERR $request->{main_sql} . "\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});

    # Return an error response if we couldn't retrieve the record.
    
    die "404 Not found\n" unless $request->{main_record};
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
    
    # my ($join_list) = $self->generateJoinList('p', $self->{select_tables});
    
    # Generate the main query
    
    $self->{main_sql} = "
	SELECT $count $fields
	FROM person as p
        WHERE $filter_string
	ORDER BY p.reversed_name $limit";
    
    print STDERR $self->{main_sql} . "\n\n" if $self->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
}


sub people_auto {
    
    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    
    # If the 'strict' parameter was given, make sure we haven't generated any warnings. If the
    # 'extid' parameter was given, make sure that all identifiers are properly formatted.
    
    $request->strict_check;
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    my $count = $request->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $request->select_string('p');
    
    # Generate the necessary filter.
    
    my $name = $request->clean_param('name');
    my @filters;
    
    # If we have a name of at least three characters, return some results. Otherwise, use a filter
    # expression that will return nothing.
    
    if ( $name && length($name) >= 3 )
    {
	my $quoted = $dbh->quote("${name}%");
	push @filters, "reversed_name like $quoted";
    }
    
    else
    {
	push @filters, "reversed_name like 'qqq'";
    }
    
    my $filter_string = join(' and ', @filters);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $count $fields
	FROM person as p
	WHERE $filter_string
	ORDER BY p.reversed_name $limit";
    
    print STDERR $request->{main_sql} . "\n\n" if $request->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
}


# auto_complete_prs ( name, limit )
# 
# This operation provides an alternate query functionality, designed to be called from the
# combined auto-complete operation.

sub auto_complete_prs {
    
    my ($request, $name, $limit) = @_;
    
    my $dbh = $request->get_connection();
    
    my $quoted_name = $dbh->quote("${name}%");
    
    my $use_extids = $request->has_block('extids');
    
    $limit ||= 10;
    my @filters;
    
    if ( $name =~ qr{ ^ ( \w ) [.]? \s+ ( [a-zA-Z_%] .* ) }xs )
    {
	push @filters, "p.name like '$1% $2'";
    }
    
    else
    {
	push @filters, "p.reversed_name like $quoted_name";
    }
    
    my $filter_string = join(' and ', @filters);
    
    my $sql = "
	SELECT person_no, name, country, institution, 'prs' as record_type
	FROM person as p
	WHERE $filter_string
	ORDER BY reversed_name asc LIMIT $limit";
    
    print STDERR "$sql\n\n" if $request->debug;
    
    my $result_list = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    if ( ref $result_list eq 'ARRAY' )
    {
	foreach my $r ( @$result_list )
	{
	    $r->{record_id} = $use_extids ? generate_identifier('PRS', $r->{person_no}) :
		$r->{person_no};
	}
	
	return @$result_list;
    }
    
    return;
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
        
    return $join_list;
}


# process_record ( )
# 
# This routine is called automatically to process each record before it is output.

sub process_record {

    my ($request, $record) = @_;
    
    if ( $record->{person_id} && ! $record->{person_no} )
    {
	$record->{person_no} = $record->{person_id};
    }
    
    elsif ( $record->{person_no} )
    {
	$record->{person_no} = generate_identifier('PRS', $record->{person_no});
    }
    
    $record->{real_name} ||= $record->{p_name};
    $record->{institution} ||= $record->{p_institution};
    $record->{country} ||= $record->{p_country};
    $record->{role} ||= $record->{p_role};
    $record->{status} = 'inactive' if defined $record->{p_active} && $record->{p_active} eq '0';
    $record->{status} ||= 'active' if $record->{p_active};
}

1;
