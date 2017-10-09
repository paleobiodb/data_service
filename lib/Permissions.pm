# 
# Permissions.pm
# 
# This module handles table and record permissions. Each request that deals with access to
# non-public data or with modification of data should create a Permissions object through which it
# can verify the appropriate table and record permissions before proceeding.
# 
# If the request comes from a logged-in user, the session_id must be passed to the
# constructor. This enables the session information to be retrieved, and the appropriate
# permissions for that user to be granted.
# 


package Permissions;

use strict;

use TableDefs qw($SESSION_DATA $TABLE_PERMS get_table_property);

use Carp qw(carp croak);
use Scalar::Util qw(weaken);


our (%PERMISSION_NAME) = ( view => 1, post => 1, edit => 1, delete => 1, admin => 1 );


# new ( request, session_id, table_name )
# 
# Given a session_id string, look up the user_id, authorizer_no, and enterer_no values from the
# session table and create a new Permissions object. If a table name is given, fill in the
# permissions (if any) that the user has for that table.
# 
# Each Permissions object is tied to a particular request, but we weaken the reference so that it
# will not prevent the request object from being destroyed when it goes out of scope. In general,
# the Permissions object should only be stored in dynamic variables or as a subordinate object to
# a request.

sub new {
    
    my ($class, $request, $session_id, $table_name) = @_;
    
    my $perms;
    
    # If we have a session cookie, then look up the authorization info from the session_data
    # table. If we are given a table name, then look up the requestor's role for that table as
    # well.
    
    if ( my $session_id )
    {
	my $dbh = $request->get_connection;
	my $quoted_id = $dbh->quote($session_id);
	
	if ( $table_name )
	{
	    my $lookup_name = $table_name;
	    $lookup_name =~ s/^\w+[.]//;
	    
	    my $quoted_table = $dbh->quote($lookup_name);
	    
	    my $sql = "
		SELECT authorizer_no, enterer_no, user_id, superuser as is_superuser, 
		       s.role, p.permission
		FROM $SESSION_DATA as s left join $TABLE_PERMS as p
			on p.person_no = s.enterer_no and p.table_name = $quoted_table
		WHERE session_id = $quoted_id";
	    
	    print STDERR "$sql\n\n" if $request->debug;
	    
	    $perms = $dbh->selectrow_hashref($sql);
	    
	    # If we get nothing, then the login session is no longer valid.
	    
	    unless ( $perms && $perms->{user_id} )
	    {
		return Permissions->no_login($request, $table_name);
	    }
	    
	    # If we retrieved a specific table permission, add it into the new object. Otherwise, add
	    # the default permission for this table.
	    
	    if ( $perms->{permission} )
	    {
		my @list = grep $_, (split qr{/}, $perms->{permission});
		
		$perms->{table_permission}{$table_name}{$_} = 1 foreach @list;
		$perms->{auth_diag}{$table_name} = 'PERMISSIONS';
		
		delete $perms->{permission};
	    }
	    
	    else
	    {
		# $perms->{table_permission}{$table_name} = $perms->default_table_permission($table_name);
		$perms->default_table_permissions($table_name);
	    }
	}
	
	# If we weren't given a table name, just query for the session data.
	
	else
	{
	    my $sql = "
		SELECT authorizer_no, enterer_no, user_id, superuser as is_superuser, role
		FROM $SESSION_DATA WHERE session_id = $quoted_id";
	    
	    print STDERR "$sql\n\n" if $request->debug;
	    
	    $perms = $dbh->selectrow_hashref($sql);
	    
	    # If we get nothing, then the login session is no longer valid. Otherwise, bless the
	    # newly acquired info into the proper class.
	    
	    unless ( $perms && $perms->{user_id} )
	    {
		return Permissions->no_login($request, $table_name);
	    }
	}
	
	# If this request comes from a user who is not a full database member with an authorizer,
	# make absolutely sure that the role is 'guest' and the superuser bit is turned off.
	
	unless ( $perms->{authorizer_no} && $perms->{authorizer_no} > 0 &&
		 $perms->{enterer_no} && $perms->{enterer_no} > 0 )
	{
	    $auth_info->{role} = 'guest';
	    $auth_info->{superuser} = 0;
	}
	
	# Cache the request and dbh values for later use, plus the debug flag.
	
	bless $perms, $class;
	
	$perms->{dbh} = $dbh;
	weaken $perms->{dbh};
	
	$perms->{request} = $request;
	weaken $perms->{request};
	
	$perms->{debug} = $request->debug;
	
	$perms->{role} ||= 'guest';
	
	return $perms;
    }
    
    # If we don't have a session_id, then the user has no permissions at all.
    
    else
    {
	return Permissions->no_login($request, $table_name);
    }
}


# no_login ( request, table_name )
# 
# Return a Permissions object that gives no permissions at all. If a table name was given, add a
# placeholder permissions hash that does not give any permissions.

sub no_login {
    
    my ($class, $request, $table_name) = @_;
    
    my $no_perms = { authorizer_no => 0, enterer_no => 0, user_id => '', role => 'none' };
    
    $no_perms->{table_permission}{$table_name} = { } if $table_name;
    
    $no_perms->{dbh} = $request->get_connection;
    weaken $no_perms->{dbh};
    
    $no_perms->{request} = $request;
    weaken $no_perms->{request};
    
    $no_perms->{debug} = $request->debug;
    
    bless $no_perms, $class;
    return $no_perms;
}


# basic accessor methods

sub role {

    return $_[0]->{role};
}


sub authorizer_no {
    
    return $_[0]->{authorizer_no};
}


sub enterer_no {
    
    return $_[0]->{enterer_no};
}


sub user_id {
    
    return $_[0]->{user_id};
}


sub superuser {
    
    return $_[0]->{is_superuser};
}


# get_table_permissions ( table_name )
# 
# Return the table permission hash for the specified table. If necessary, look them up in the
# database or compute them from the authorization info and table permissions.

sub get_table_permissions {
    
    my ($perms, $table_name) = @_;
    
    # If we don't already have the permissions for this table cached, retrieve them.
    
    unless ( ref $perms->{table_permission}{$table_name} eq 'HASH' )
    {
	# If the current user is a database member, check for explicitly granted permissions. If
	# the table name has a database prefix, strip it off (see 'new' above).
	
	if ( $perms->{enterer_no} )
	{
	    my $lookup_name = $table_name;
	    $lookup_name =~ s/^\w+[.]//;
	    
	    my $quoted_person = $dbh->quote($perms->{enterer_no});
	    my $quoted_table = $dbh->quote($lookup_name);
	    
	    my $dbh = $perms->{dbh};
	    
	    my $sql = "
		SELECT permission FROM $TABLE_PERMS
		WHERE person_no = $quoted_person and table_name = $quoted_table";
	    
	    my $permission;
	    
	    eval {
		($permission) = $dbh->selectrow_array($sql);
	    };
	    
	    # If we found a permission string, unpack it and set each permission.
	    
	    if ( $permission )
	    {
		my @list = split qr{/}, $permissions;
		$perms->{table_permission}{$table_name} = { map { $_ => 1 } @list };
	    }
	    
	    # Otherwise, compute the permissions from the authorization info and table properties.
	    
	    else
	    {
		$request->default_table_permission($table_name);
	    }
	}
	
	# If the current user is not a database member, likewise go with the authorization info
	# and table properties.
	
	else
	{
	    $request->default_table_permission($table_name);
	}
    }
    
    return $request->{my_auth_info}{table_permission}{$table_name};
}


# default_table_permissions ( table_name )
# 
# If we have do not already know the table permissions for the specified table, compute them
# from the authorization info and table properties.

sub default_table_permissions {
    
    my ($perms, $table_name) = @_;
    
    # Return if we have already determined the permissions for the specified table.
    
    return if ref $perms->{table_permission}{$table_name} eq 'HASH';
    
    # Otherwise, initialize the permission hash for this table.
    
    $perms->{table_permission}{$table_name} = { };
    
    # If this table allows posting for certain classes of people, check to see
    # if the current user falls into one of them.
    
    if ( my $allow_post = get_table_property($table_name, 'ALLOW_POST') )
    {
	if ( $allow_post eq 'LOGGED_IN' && $perms->{user_id} )
	{
	    $perms->{auth_diag}{$table_name} = 'LOGGED_IN';
	    $perms->{table_permission}{$table_name}{post} = 1;
	}
	
	elsif ( $allow_post eq 'MEMBERS' && $perms->{enterer_no} )
	{
	    $perms->{auth_diag}{$table_name} = 'MEMBERS';
	    $perms->{table_permission}{$table_name}{post} = 1;
	}
	
	elsif ( $allow_post eq 'AUTHORIZED' && $perms->{authorizer_no} )
	{
	    $perms->{auth_diag}{$table_name} = 'AUTHORIZED';
	    $perms->{table_permission}{$table_name}{post} = 1;
	}
    }

    # If this table allows viewing for certain classes of people, check to see if the current user
    # falls into one of them.
    
    if ( my $allow_view = get_table_property($table_name, 'ALLOW_VIEW') )
    {
	if ( $allow_view eq 'LOGGED_IN' && $perms->{user_id} )
	{
	    $perms->{auth_diag}{$table_name} = 'LOGGED_IN';
	    $perms->{table_permission}{$table_name}{view} = 1;
	}
	
	elsif ( $allow_view eq 'MEMBERS' && $perms->{enterer_no} )
	{
	    $perms->{auth_diag}{$table_name} = 'MEMBERS';
	    $perms->{table_permission}{$table_name}{view} = 1;
	}
	
	elsif ( $allow_view eq 'AUTHORIZED' && $perms->{enterer_no} && $perms->{authorizer_no} )
	{
	    $perms->{auth_diag}{$table_name} = 'AUTHORIZED';
	    $perms->{table_permission}{$table_name}{view} = 1;
	}
    }
    
    # If this table allows editing of records for certain classes of people, check to see if the
    # current user falls into one of them.
    
    if ( my $allow_edit = get_table_property($table_name, 'ALLOW_EDIT') )
    {
	if ( $allow_edit eq 'LOGGED_IN' && $perms->{user_id} )
	{
	    $perms->{auth_diag}{$table_name} = 'LOGGED_IN';
	    $perms->{table_permission}{$table_name}{edit} = 1;
	}
	
	elsif ( $allow_edit eq 'MEMBERS' && $perms->{enterer_no} )
	{
	    $perms->{auth_diag}{$table_name} = 'MEMBERS';
	    $perms->{table_permission}{$table_name}{edit} = 1;
	}
	
	elsif ( $allow_edit eq 'AUTHORIZED' && $perms->{enterer_no} && $perms->{authorizer_no} )
	{
	    $perms->{auth_diag}{$table_name} = 'AUTHORIZED';
	    $perms->{table_permission}{$table_name}{edit} = 1;
	}
    }
    
    return $perms->{table_permission}{$table_name};
}


# check_table_permission ( table_name, permission )
# 
# Check whether the current user has the specified permission on the specified table name. If the
# user has the requested permission, that same value will be returned. If the user has
# administrative permission on the table, the value 'admin' will be returned instead. If the user
# does not have the requested permission, the empty string will be returned.
# 
# This routine is intended to be called directly from operation methods.

sub check_table_permission {
    
    my ($perms, $table_name, $permission) = @_;
    
    croak "bad call to 'check_table_permission': no permission specified" unless $permission;
    croak "bad call to 'check_table_permission': no table name specified" unless $table_name;
    croak "bad call to 'check_table_permission': bad permission '$permission'"
	unless $PERMISSION_NAME{$permission};
    
    my $p_hash = $perms->get_table_permissions($table_name);
    
    # If the user has the superuser privilege, or the 'admin' permission on this table, then they
    # have any requested permission.
    
    if ( $perms->{superuser} || $p_hash->{admin} )
    {
	print STDERR "    Permission for $table_name : '$permission' from " . 
	    ($perms->{superuser} ? 'SUPERUSER' : 'PERMISSIONS') . "\n\n"
	    if $perms->{debug};
	
	return 'admin';
    }
    
    # If the user does not have 'admin' permission, then the 'delete' permission is only allowed
    # if the table has the ALLOW_DELETE property.
    
    if ( $permission eq 'delete' )
    {
	unless ( $perms->{can_delete}{$table_name} //= get_table_property($table_name, 'ALLOW_DELETE') )
	{
	    print STDERR "    Permission for $table_name ($key_expr) : '$permission' DENIED by TABLE PROPERTY\n\n"
		if $perms->{debug};
	    
	    return '';
	}
    }
    
    # Otherwise, if we know they have the requested permission, return it.
    
    elsif ( $p_hash->{$permission} )
    {
	my $diag = $perms->{auth_diag}{$table_name} || 'DEFAULT';
	
	print STDERR "    Permission for $table_name : '$permission' from $diag\n\n"
	    if $perms->{debug};
	
	return $permission;
    }
    
    # If the requested permission is 'view' but the user has only 'post' permission, then return
    # 'own' to indicate that they may view their own records only.
    
    elsif ( $permission eq 'view' && $p_hash->{post} )
    {
	my $diag = $perms->{auth_diag}{$table_name} || 'DEFAULT';
	
	print STDERR "    Permission for $table_name : '$permission' from $diag\n\n"
	    if $perms->{debug};
	
	return 'own';
    }
    
    # Otherwise, they have no privileges whatsoever to this table.
    
    print STDERR "   Permission for $table_name : '$permission' DENIED : NO PERMISSION\n\n"
	if $perms->{debug};
    
    return '';
}


# check_record_permission ( table_name, permission, key_expr, record )
# 
# Check whether the current user has the specified permission on the specified table name, for the
# record indicated by $record. This last parameter should either be a record hash containing the
# appropriate authorization information or a string in the form 'key_field=record_id'. If the user
# has the requested permission, that same value will be returned. If the user has administrative
# permission on the table, the value 'admin' will be returned instead. If the user does not have
# the requested permission, the empty string will be returned.
# 
# This routine is intended to be called directly from operation methods.

sub check_record_permission {
    
    my ($perms, $table_name, $permission, $key_expr, $record) = @_;
    
    croak "check_record_permission: no permission specified" unless $permission;
    croak "check_record_permission: no table name specified" unless $table_name;
    croak "check_record_permission: no key expr specified" unless $key_expr;
    croak "check_record_permission: bad permission '$permission'"
	unless $PERMISSION_NAME{$permission};
    
    # Start by fetching the user's permissions for the table as a whole.
    
    my $p_hash = $perms->get_table_permissions($table_name);
    
    # If the requested permission is 'view' and the table permissions allow this, then we are done.
    
    if ( $permission eq 'view' && $p_hash->{$permission} )
    {
	print STDERR "    Permission for $table_name ($key_expr) : '$permission' from PERMISSIONS\n\n"
	    if $perms->{debug};
	
	return $permission;
    }
    
    # Unless we were given the current contents of the record, fetch the info necessary to
    # determine permissions.  If the record was not found, then return
    # 'notfound' to indicate that the record was not found.
    
    unless ( ref $record eq 'HASH' )
    {
	$record = $perms->get_record_authinfo($table_name, $key_expr);
	
	unless ( ref $record )
	{
	    print STDERR "    Permission for $table_name ($key_expr) : '$permission' DENIED : $record\n\n"
		if $perms->{debug};
	    
	    return $record;
	}
    }
    
    # If the table permission is 'admin' or if the user has superuser privileges, then they have
    # all privileges on this record including 'delete'. Return 'admin' to indicate that they have
    # the requested permission and also administrative permission. But if the record is locked,
    # then return 'locked' instead. The operation method should, in this case, allow the user to
    # unlock the record but not to modify it or delete it.
    
    if ( $perms->{superuser} || $p_hash->{admin} )
    {
	print STDERR "    Permission for $table_name ($key_expr) : 'admin' from " . 
	    ($perms->{superuser} ? 'SUPERUSER' : 'PERMISSIONS') . "\n\n"
	    if $perms->{debug};
	
	return $record->{admin_locked} ? 'locked' : 'admin';
    }
    
    # If the user does not have 'admin' permission, then the 'delete' permission is only allowed
    # if the table has the ALLOW_DELETE property.
    
    if ( $permission eq 'delete' )
    {
	unless ( $perms->{can_delete}{$table_name} //= get_table_property($table_name, 'ALLOW_DELETE') )
	{
	    print STDERR "    Permission for $table_name ($key_expr) : '$permission' DENIED by TABLE PROPERTY\n\n"
		if $perms->{debug};
	    
	    return '';
	}
    }
    
    # If the record has an adminsitrative lock, then the user does not have any permissions to it
    # unless they have administrative privileges.
    
    if ( $record->{admin_locked} )
    {
	print STDERR "    Permission for $table_name ($key_expr) : '$permission' DENIED : LOCKED\n\n"
	    if $perms->{debug};
	
	return '';
    }
    
    # If the user is the person who originally created or authorized the record, then they have
    # 'view', 'edit', and 'delete' permissions (the latter if allowed for this table).
    
    if ( $record->{enterer_no} && $perms->{enterer_no} &&
	 $record->{enterer_no} eq $perms->{enterer_no} )
    {
	print STDERR "    Permission for $table_name ($key_expr) : '$permission' from enterer_no\n\n"
	    if $perms->{debug};
	
	return $permission;
    }
    
    if ( $record->{authorizer_no} && $perms->{enterer_no} &&
	 $record->{authorizer_no} eq $perms->{enterer_no} )
    {
	print STDERR "    Permission for $table_name ($key_expr) : '$permission' from authorizer_no\n\n"
	    if $perms->{debug};
	
	return $permission;
    }
    
    if ( $record->{enterer_id} && $perms->{user_id} &&
	 $record->{enterer_id} eq $perms->{user_id} )
    {
	print STDERR "    Permission for $table_name ($key_expr) : '$permission' from enterer_id\n\n"
	    if $perms->{debug};
	
	return $permission;
    }
    
    # If the user has the same authorizer as the person who originally created the record, then
    # they have 'view', 'edit' and 'delete' permissions if the table has the 'BY_AUTHORIZER'
    # property.
    
    if ( $record->{authorizer_no} && $perms->{authorizer_no} &&
	 $record->{authorizer_no} eq $perms->{authorizer_no} )
    {
	if ( $perms->{table_by_authorizer}{$table_name} //= get_table_property($table_name, 'BY_AUTHORIZER') )
	{
	    print STDERR "    Permission for $table_name ($key_expr) : '$permission' from BY_AUTHORIZER\n\n"
		if $perms->{debug};
	    
	    return $permission;
	}
    }
    
    # Otherwise, the requestor has no permission on this record.
    
    print STDERR "    Permission for $table_name ($key_expr) : '$permission' DENIED : NO PERMISSION\n\n"
	if $perms->{debug};
    
    return '';
}


# record_filter ( table_name )
# 
# Return a filter expression that should be included in an SQL statement to select only records
# viewable by this user.

sub record_filter {
    
    my ($perms, $table_name) = @_;
    
    # This still needs to be implemented... $$$
}


1;
