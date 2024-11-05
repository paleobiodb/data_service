# 
# The Paleobiology Database
# 
#   Authentication.pm - authenticate requests that require access to non-public data
# 


package PB2::Authentication;

use strict;

use Carp qw(carp croak);

use Permissions;

use Moo::Role;


# authenticate ( table_name )
# 
# Retrieve the session_id cookie, if there is one, and create a Permissions object through which
# table and record access permissions can be verified. If there is already a cached Permissions
# object for this request, just add the new table permissions to it.

sub authenticate {
    
    my ($request, $table_name) = @_;
    
    if ( $request->{my_perms} && $request->{my_perms}->isa('Permissions') )
    {
	$request->{my_perms}->get_table_permissions($table_name) if $table_name;
    }
    
    elsif ( my $session_id = Dancer::cookie('session_id') // Dancer::request->env->{session_id} )
    {
	my $dbh = $request->get_connection;
	my $options = { debug => $request->debug };
	
	$request->{my_perms} = Permissions->new($dbh, $session_id, $table_name, $options);
    }
    
    else
    {
	my $options = { debug => $request->debug };
	
	$request->{my_perms} = Permissions->no_login($request->get_connection, $table_name, $options);
    }

    return $request->{my_perms};
}


# require_authentication ( table_name, errmsg )
# 
# This routine does the same as 'authentication', except that it returns a 401 error if the user
# is not logged in. If an error message is given, it is used in the response. Otherwise, a default
# response will be sent.

sub require_authentication {
    
    my ($request, $table_name, $errmsg) = @_;
    
    if ( $request->{my_perms} && $request->{my_perms}->isa('Permissions') )
    {
	$request->{my_perms}->get_table_permissions($table_name) if $table_name;
    }
    
    elsif ( my $session_id = Dancer::cookie('session_id') // Dancer::request->env->{session_id} )
    {
	my $dbh = $request->get_connection;
	my $options = { debug => $request->debug };
	
	$request->{my_perms} = Permissions->new($dbh, $session_id, $table_name, $options);
    }

    unless ( $request->{my_perms} && $request->{my_perms}{role} && 
	     $request->{my_perms}{role} ne 'none' )
    {
	if ( $request->{my_perms}{expired} )
	{
	    $errmsg = "Your login session has expired. Please log in again.";
	}
	
	die $request->exception(401, $errmsg || "You must be logged in to perform this operation");
    }
    
    return $request->{my_perms};
}



# sub get_auth_info {
    
#     my ($request, $dbh, $table_name, $options) = @_;
    
#     $options ||= { };
    
#     # If we already have authorization info cached for this request, just
#     # return it. But if a table name is given, and the requestor's role for
#     # that table is not known, look it up.
    
#     if ( $request->{my_auth_info} )
#     {
# 	if ( $table_name && ! $request->{my_auth_info}{table_permission}{$table_name} )
# 	{
# 	    $request->get_table_permission($dbh, $table_name);
# 	}
	
# 	return $request->{my_auth_info};
#     }
    
#     # Otherwise, if we have a session cookie, then look up the authorization
#     # info from the session_data table. If we are given a table name, then
#     # look up the requestor's role for that table as well.
    
#     $dbh ||= $request->get_connection;
    
#     if ( my $cookie_id = Dancer::cookie('session_id') )
#     {
# 	# my $perms = Permissions->new($request, $dbh, $cookie_id, $table_name);
    
# 	my $session_id = $dbh->quote($cookie_id);
	
# 	my $auth_info;
	
# 	if ( $table_name )
# 	{
# 	    my $lookup_name = $table_name;
# 	    $lookup_name =~ s/^\w+[.]//;
	    
# 	    my $quoted_table = $dbh->quote($lookup_name);
	    
# 	    my $sql = "
# 		SELECT authorizer_no, enterer_no, user_id, superuser, s.role, p.permission
# 		FROM $SESSION_DATA as s left join $TABLE_PERMS as p
# 			on p.person_no = s.enterer_no and p.table_name = $quoted_table
# 		WHERE session_id = $session_id";
	    
# 	    print STDERR "$sql\n\n" if $request->debug;
	    
# 	    $auth_info = $dbh->selectrow_hashref($sql);
	    
# 	    if ( $auth_info->{permission} )
# 	    {
# 		my @list = grep $_, (split qr{/}, $auth_info->{permission});
		
# 		$auth_info->{table_permission}{$table_name}{$_} = 1 foreach @list;
# 		$auth_info->{auth_diag}{$table_name} = 'PERMISSIONS';
		
# 		delete $auth_info->{permission};
# 	    }
	    
# 	    else
# 	    {
# 		$request->default_table_permission($table_name, $auth_info);
# 	    }
# 	}
	
# 	else
# 	{
# 	    my $sql = "
# 		SELECT authorizer_no, enterer_no, user_id, superuser, role FROM $SESSION_DATA as s
# 		WHERE session_id = $session_id";
	    
# 	    print STDERR "$sql\n\n" if $request->debug;
	    
# 	    $auth_info = $dbh->selectrow_hashref($sql);
# 	}
	
# 	# If this request comes from a database contributor, cache this info and return it.
	
# 	if ( ref $auth_info eq 'HASH' && $auth_info->{authorizer_no} && $auth_info->{enterer_no} )
# 	{
# 	    $request->{my_auth_info} = $auth_info;
# 	    bless $auth_info, 'AuthInfo';
# 	    return $auth_info;
# 	}
	
# 	# If this request comes from a guest user, cache this info and return it. But make
# 	# absolutely sure that the role is 'guest' and the superuser bit is turned off. If our
# 	# configuration file has a 'generic_guest_no' value, then put that into the
# 	# guest_no field. Otherwise, it will be left as 0.
	
# 	elsif ( ref $auth_info eq 'HASH' && $auth_info->{user_id} )
# 	{
# 	    $auth_info->{role} = 'guest';
# 	    $auth_info->{superuser} = 0;
	    
# 	    $request->default_table_permission($table_name, $auth_info)	if $table_name;
	    
# 	    if ( my $guest_no = $request->ds->config_value('generic_guest_no') )
# 	    {
# 		$auth_info->{guest_no} = $guest_no;
# 	    }
	    
# 	    $request->{my_auth_info} = $auth_info;
# 	    bless $auth_info, 'AuthInfo';
# 	    return $auth_info;
# 	}
	
# 	# If we get here, then the requestor isn't even logged in. So fall through to the check below.
#     }
    
#     if ( $options->{required} )
#     {
# 	die $request->exception(401, $options->{errmsg} || "You must be logged in to perform this operation");
#     }
    
#     else
#     {
# 	my $default = { authorizer_no => 0, enterer_no => 0, user_id => '', role => 'none' };
# 	$default->{table_permission}{$table_name} = { } if $table_name;
	
# 	$request->{my_auth_info} = $default;
# 	bless $default, 'AuthInfo';
# 	return $default;
#     }
# }

1;
