# 
# Statistics.pm
# 
# A class that contains routines for generating statistics about the database. I have also stuck
# the application state store/fetch routine in here, because it is also used by the frontend.
# 
# Author: Michael McClennen

package PB2::Statistics;

use strict;

use HTTP::Validate qw(:validators);
use Carp qw(croak);
use TableDefs qw(%TABLE);
use ExternalIdent qw(extract_identifier generate_identifier);

use Moo::Role;


# initialize ( )
# 
# This routine is called by the DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;

    $ds->define_block('1.2:larkin:summary' =>
	{ output => 'references', com_name => 'references', data_type => 'pos' },
	    "The number of references represented in the database",
	{ output => 'taxa', com_name => 'taxa', data_type => 'pos' },
	    "The number of taxa represented in the database",
	{ output => 'opinions', com_name => 'opinions', data_type => 'pos' },
	    "The number of taxonomic opinions represented in the database",
	{ output => 'collections', com_name => 'collections', data_type => 'pos' },
	    "The number of fossil collections represented in the database",
	{ output => 'occurrences', com_name => 'occurrences', data_type => 'pos' },
	    "The number of fossil occurrences represented in the database",
	{ output => 'scientists', com_name => 'scientists', data_type => 'pos' },
	    "The number of database users who have entered records in the database");

    $ds->define_block('1.2:larkin:single' =>
	{ output => 'c', com_name => 'c', data_type => 'int' },
	    "The count for the variable specified in this request for a particular month or year.",
	{ output => 'date', com_name => 'date' },
	    "A year or a month, specified as yyyy or yyyy-mm.");
    
    $ds->define_set('1.2:larkin:variables' =>
	{ value => 'occurrences' },
	{ value => 'collections' },
	{ value => 'references' },
	{ value => 'taxa' },
	{ value => 'opinions' },
	{ value => 'members' },
	{ value => 'scientists' },
	{ value => 'publications' });
    
    $ds->define_set('1.2:larkin:group_by' =>
	{ value => 'month' },
	{ value => 'year' });

    $ds->define_block('1.2:app-state:store' =>
	{ output => 'id', data_type => 'str' },
	    "This field returns the identifier under which the state has been saved.");
    
    $ds->define_block('1.2:app-state:fetch' => 
	{ output => 'data', data_type => 'json', bad_value => '{ }' },
	    "This field returns the saved application state, in JSON format.");
    
    $ds->define_ruleset('1.2:larkin_stats' => 
	{ param => 'variable', valid => '1.2:larkin:variables' },
	    "Selects the variable to be returned by this request.",
	    "Accepted values are:",
	{ param => 'summary', valid => FLAG_VALUE },
	    "Reports the values of all variables at the present time.",
	    "This parameter does not take a value. If specified, you may",
	    "not specify the parameter B<variable>.",
	{ at_most_one => ['variable', 'summary'] },
	{ param => 'groupBy', valid => '1.2:larkin:group_by' },
	    "Specifies the summation period. The default is 'year'.",
	    "Accepted values are:");
    
    $ds->define_ruleset('1.2:frontend:app-state' =>
	{ optional => 'id', valid => ANY_VALUE },
	    "This parameter is only valid with a GET request. It specifies",
	    "which application state record to return.");
   
}


# larkin_stats ( )
#
# Return database statistics in a legacy format used by the frontend website code.

sub larkin_stats {
    
    my ($request) = @_;
    
    # If the 'variable' parameter is given, we generate a response for the indicated variable.
    
    if ( my $variable = $request->clean_param('variable') )
    {
	my $sum_over = $request->clean_param('groupBy') || 'year';
	
	return larkin_variable($request, $variable, $sum_over);
    }

    # Otherwise, if the 'summary' parameter is given, we generate a summary.
    
    elsif ( $request->clean_param('summary') )
    {
	return larkin_summary($request);
    }
    
    # Otherwise, we throw a 400 error.
    
    else
    {
	die "400 bad parameters";
    }
}


sub larkin_summary {
    
    my ($request) = @_;

    # Set the proper output configuration.

    $request->add_output_blocks('main', '1.2:larkin:summary');
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make the request.

    my $sql = "SELECT
	(SELECT COUNT(*) FROM refs) AS 'references',
	(SELECT COUNT(*) FROM authorities) AS 'taxa',
	(SELECT COUNT(*) FROM opinions) AS 'opinions',
	(SELECT COUNT(*) FROM collections) AS 'collections',
	(SELECT COUNT(*) FROM occurrences) AS 'occurrences',
	(SELECT COUNT(*) FROM pbdb_wing.users WHERE role = 'authorizer' or role = 'enterer') AS 'scientists'";
    
    $request->{ds}->debug_line("$sql\n") if $request->debug;
    
    my ($result) = $dbh->selectall_arrayref($sql, { Slice => { } });

    return $request->list_result($result);
}


sub larkin_variable {

    my ($request, $variable, $sum_over) = @_;
    
    # Set the proper output configuration.

    $request->add_output_blocks('main', '1.2:larkin:single');
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Construct and execute the request.

    my ($datefield, $dateformat, $group_expr);
    
    if ( $variable eq 'members' || $variable eq 'scientists' )
    {
	$datefield = 'coalesce(p.created, u.date_created)';
    }

    else
    {
	$datefield = 'created';
    }
    
    if ( $sum_over eq 'year' )
    {
	$dateformat =  "DATE_FORMAT($datefield, '%Y')";
	$group_expr = "year($datefield)";
    }

    else
    {
	$dateformat = "DATE_FORMAT($datefield, '%Y-%m')";
	$group_expr = "year($datefield), month($datefield)";
    }
    
    my $sql;

    my %table_map = ( taxa => 'authorities',
		      publications => 'pubs',
		      references => 'refs' );
    
    my $table = $table_map{$variable} || $variable;
    
    if ( $variable eq 'occurrences' || $variable eq 'taxa' || $variable eq 'references' )
    {
	$sql = "SELECT COUNT(*) c, $dateformat date
		FROM $table
		WHERE created IS NOT NULL AND year(created) > 1997
		GROUP BY $group_expr";
    }
    
    elsif ( $variable eq 'members' || $variable eq 'scientists' )
    {
	my $filter = "u.role <> 'guest' AND u.date_created IS NOT NULL";
	$filter .= ' AND p.hours_ever > 0' if $variable eq 'scientists';
	
	$sql = "SELECT count(*) as c, $dateformat date 
		FROM person as p JOIN pbdb_wing.users as u using (person_no)
		WHERE $filter
		GROUP BY $group_expr";
    }
    
    elsif ( $variable eq 'publications' || $variable eq 'opinions' )
    {
	$sql = "SELECT count(*) c, $dateformat date
		FROM $table
		WHERE created is not null
		GROUP BY $group_expr";
    }
    
    else
    {
	die "400 Unknown variable '$variable'";
    }
    
    $request->{ds}->debug_line("$sql\n") if $request->debug;
    
    my ($result) = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    $request->{main_sql} = $sql;
    
    return $request->list_result($result);
}


# This method is used for both storing and fetching application state, depending on the HTTP
# method used.

sub app_state {
    
    my ($request) = @_;

    my $dbh = $request->get_connection();
    
    if ( $request->http_method eq 'POST' )
    {
	my ($body, $error) = $request->decode_body;

	if ( $error )
	{
	    die $request->exception(400, "Badly formatted request body: $error");
	}
	
	my $quoted_data;
	
	if ( $body->{state} )
	{
	    my $state = JSON::encode_json($body->{state});
	    $quoted_data = $dbh->quote($state);
	}
	
	elsif ( $body )
	{
	    my $state = JSON::encode_json($body);
	    $quoted_data = $dbh->quote($state);
	}
	
	else
	{
	    die $request->exception(400, "Empty request body");
	}
	
	my $bytes = substr(sprintf("%08x", int(rand(1000000000000))),0,8);
	substr($bytes,0,1) = 1 if $bytes =~ /^0/;
	
	my $sql = "INSERT INTO $TABLE{APP_STATE} (id, app, data)
		VALUES ('$bytes', 'navigator', $quoted_data)";
	
	$request->debug_line("$sql\n") if $request->debug;
	
	my $result = $dbh->do($sql);
	
	if ( $result )
	{
	    $request->add_output_blocks('main', '1.2:app-state:store');
	    return $request->single_result({ id => $bytes });
	}

	else
	{
	    return $request->exception('400', 'Please try again');
	}
    }
    
    else
    {
	my $id = $request->clean_param('id');
	die $request->exception('400', "You must specify a value for 'id'") unless $id;
	
	my $quoted = $dbh->quote($id);
	
	my $sql = "SELECT data FROM $TABLE{APP_STATE} WHERE id = $quoted";
	
	$request->debug_line("\n$sql\n");
	
	my ($data) = $dbh->selectrow_array($sql);

	if ( $data )
	{
	    $request->add_output_blocks('main', '1.2:app-state:fetch');
	    return $request->list_result({ data => $data });
	}
	
	else
	{
	    die $request->exception('404');
	}
    }
}


1;
