# 
# The Paleobiology Database
# 
#   Authentication.pm - authenticate requests that require access to non-public data
# 


package PB2::Authentication;

use strict;

use Carp qw(carp croak);

use Moo::Role;



sub require_auth {
    
    my ($request, $dbh, $table_name) = @_;
    
    return $request->get_auth_info($dbh, $table_name, { required => 1 });
}


sub get_auth_info {
    
    my ($request, $dbh, $table_name, $options) = @_;
    
    $options ||= { };
    
    # If we already have authorization info cached for this request, just
    # return it. But if a table name is given, and the requestor's role for
    # that table is not known, look it up.
    
    if ( $request->{my_auth_info} )
    {
	if ( $table_name && ! $request->{my_auth_info}{table_role}{$table_name} )
	{
	    $request->get_table_role($dbh, $table_name);
	}
	
	return $request->{my_auth_info};
    }
    
    # Otherwise, if we have a session cookie, then look up the authorization
    # info from the session_data table. If we are given a table name, then
    # look up the requestor's role for that table as well.
    
    $dbh ||= $request->get_connection;
    
    if ( my $cookie_id = Dancer::cookie('session_id') )
    {
	my $session_id = $dbh->quote($cookie_id);
	
	my $auth_info;
	
	if ( $table_name )
	{
	    my $quoted_table = $dbh->quote($table_name);
	    
	    my $sql = "
		SELECT authorizer_no, enterer_no, superuser, s.role, p.role as table_role
		FROM session_data as s left join table_permissions as p
			on p.person_no = s.enterer_no and p.table_name = $quoted_table
		WHERE session_id = $session_id";
	    
	    print STDERR "$sql\n\n" if $request->debug;
	    
	    $auth_info = $dbh->selectrow_hashref($sql);
	    
	    my $table_role = $auth_info->{table_role} || 'none';
	    delete $auth_info->{table_role};
	    
	    $auth_info->{table_role}{$table_name} = $table_role;
	}
	
	else
	{
	    my $sql = "
		SELECT authorizer_no, enterer_no, superuser, role FROM session_data as s
		WHERE session_id = $session_id";
	    
	    print STDERR "$sql\n\n" if $request->debug;
	    
	    $auth_info = $dbh->selectrow_hashref($sql);
	}
	
	# If we have retrieved the proper information, cache it and return it.
	
	if ( ref $auth_info eq 'HASH' && $auth_info->{authorizer_no} && $auth_info->{enterer_no} )
	{
	    $request->{my_auth_info} = $auth_info;
	    return $auth_info;
	}
    }
    
    if ( $options->{required} )
    {
	die $request->exception(401, $options->{errmsg} || "You must be logged in to perform this operation");
    }
    
    else
    {
	return { authorizer_no => 0, enterer_no => 0 };
    }
}


sub get_table_role {
    
    my ($request, $dbh, $table_name) = @_;
    
    unless ( $request->{my_auth_info}{table_role}{$table_name} )
    {
	$dbh ||= $request->get_connection;
	
	my $quoted_person = $dbh->quote($request->{my_auth_info}{enterer_no});
	my $quoted_table = $dbh->quote($table_name);
	
	my $sql = "
		SELECT role FROM table_permissions
		WHERE person_no = $quoted_person and table_name = $quoted_table";
	
	my $role;
	
	eval {
	    ($role) = $dbh->selectrow_array($sql);
	};
	
	$request->{my_auth_info}{table_role}{$table_name} = $role || 'none';
    }
    
    return $request->{my_auth_info}{table_role}{$table_name};
}


1;
