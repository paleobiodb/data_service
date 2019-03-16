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

use TableDefs qw(%TABLE get_table_property original_table);
use TableData qw(get_authinfo_fields);

use Carp qw(carp croak);
use Scalar::Util qw(weaken blessed);


our (%PERMISSION_NAME) = ( view => 1, post => 1, edit => 1, delete => 1, insert_key => 1 );


# new ( dbh, session_id, table_name, options )
# 
# Given a session_id string, look up the user_id, authorizer_no, and enterer_no values from the
# session table and create a new Permissions object. If a table name is given, fill in the
# permissions (if any) that the user has for that table.
# 
# Each Permissions object is generated for a data service request or the execution of a
# command-line script.

sub new {
    
    my ($class, $request_or_dbh, $session_id, $table_specifier, $options) = @_;
    
    $options ||= { };
    
    my $perms;
    
    croak "new Permissions: request or dbh is required"
	unless $request_or_dbh && blessed($request_or_dbh);
    
    # Make sure we have a database handle.
    
    my $dbh = $request_or_dbh;
    
    if ( $request_or_dbh->can('get_connection') )
    {
	$dbh = $request_or_dbh->get_connection;
    }
    
    # If we were given a login session id, then look up the authorization info from the
    # session_data table.
    
    if ( $session_id )
    {
	my $quoted_id = $dbh->quote($session_id);
	
	my $perms;
	
	# If we were also given a table name, then check to see if this user has any special
	# permissions on that table. Otherwise, just get the login information.
	
	if ( $table_specifier )
	{
	    croak "unknown table '$table_specifier'" unless exists $TABLE{$table_specifier};
	    
	    # my $lookup_name = original_table($TABLE{$table_specifier});
	    # $lookup_name =~ s/^\w+[.]//;
	    
	    my $quoted_table = $dbh->quote($table_specifier);
	    
	    my $sql = "
		SELECT authorizer_no, enterer_no, user_id, superuser as is_superuser, 
		       s.role, p.permission
		FROM $TABLE{SESSION_DATA} as s left join $TABLE{TABLE_PERMS} as p
			on p.person_no = s.enterer_no and p.table_name = $quoted_table
		WHERE session_id = $quoted_id";
	    
	    print STDERR "$sql\n\n" if $options->{debug};
	    
	    $perms = $dbh->selectrow_hashref($sql);
	}
	
	else
	{
	    my $sql = "
		SELECT authorizer_no, enterer_no, user_id, superuser as is_superuser, role
		FROM $TABLE{SESSION_DATA} WHERE session_id = $quoted_id";
	    
	    print STDERR "$sql\n\n" if $options->{debug};
	    
	    $perms = $dbh->selectrow_hashref($sql);
	}
	
	# If we retrieved a record that includes a value for 'user_id', then we have the basis for
	# a valid Permissions object. Otherwise, return a dummy Permissions object that has no
	# permissions other than those available to everybody.
	
	if ( $perms && $perms->{user_id} )
	{
	    bless $perms, $class;
	}
	
	else
	{
	    return Permissions->no_login($dbh, $table_specifier, $options);
	}
	
	# If a table name was specified and we retrieved a specific table permission, add it into
	# the new object. Otherwise, add the default permission for the specified table.
	
	if ( $table_specifier )
	{
	    if ( $perms->{permission} )
	    {
		my @list = grep $_, (split qr{,}, $perms->{permission});
		
		$perms->{table_permission}{$table_specifier}{$_} = 1 foreach @list;
		$perms->{auth_diag}{$table_specifier} = 'TABLE_PERMS';
		
		delete $perms->{permission};
	    }
	    
	    else
	    {
		$perms->default_table_permissions($table_specifier);
	    }
	}
		
	# If this request comes from a user who is not a full database member with an authorizer,
	# make absolutely sure that the role is 'guest' and the superuser bit is turned off.
	
	unless ( $perms->{authorizer_no} && $perms->{authorizer_no} > 0 &&
		 $perms->{enterer_no} && $perms->{enterer_no} > 0 )
	{
	    $perms->{role} = 'guest';
	    delete $perms->{is_superuser};
	}
	
	# Cache the dbh in case we need it later, plus the debug flag. If 'role' is not set for some
	# reason, default it to 'guest'.
	
	$perms->{dbh} = $dbh;
	weaken $perms->{dbh};
	
	$perms->{debug} = 1 if $options->{debug};
	
	$perms->{role} ||= 'guest';
	
	return $perms;
    }
    
    # If we don't have a session_id, then the user has no permissions at all.
    
    else
    {
	return Permissions->no_login($dbh, $table_specifier, $options);
    }
}


# no_login ( dbh, table_name )
# 
# Return a Permissions object that gives no permissions at all. If a table name was given, add a
# placeholder permissions hash that does not give any permissions.

sub no_login {
    
    my ($class, $dbh, $table_specifier, $options) = @_;
    
    my $no_perms = { authorizer_no => 0, enterer_no => 0, user_id => '', role => 'none' };
    
    $no_perms->{table_permission}{$table_specifier} = { } if $table_specifier;
    
    $no_perms->{dbh} = $dbh;
    weaken $no_perms->{dbh};
    
    $no_perms->{debug} = $options->{debug};
    
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


sub is_superuser {
    
    return $_[0]->{enterer_no} && $_[0]->{is_superuser};
}


sub debug_line {
    
    return unless ref $_[0] && $_[0]->{debug};
    
    my ($perms, $line) = @_;
    
    print STDERR "$line\n" if $perms->{debug};
}


# get_table_permissions ( table_name )
# 
# Return the table permission hash for the specified table. If necessary, look them up in the
# database or compute them from the authorization info and table permissions.

sub get_table_permissions {
    
    my ($perms, $table_specifier) = @_;
    
    # If we don't already have the permissions for this table cached, retrieve them.
    
    unless ( ref $perms->{table_permission}{$table_specifier} eq 'HASH' )
    {
	# If the current user is a database member, check for explicitly granted permissions. If
	# the table name has a database prefix, strip it off (see 'new' above).
	
	if ( $perms->{enterer_no} )
	{
	    my $lookup_name = $table_specifier;
	    $lookup_name =~ s/^\w+[.]//;
	    
	    my $dbh = $perms->{dbh};
	    
	    my $quoted_person = $dbh->quote($perms->{enterer_no});
	    my $quoted_table = $dbh->quote($lookup_name);
	    
	    my $sql = "
		SELECT permission FROM $TABLE{TABLE_PERMS}
		WHERE person_no = $quoted_person and table_name = $quoted_table";
	    
	    my $permission;
	    
	    eval {
		($permission) = $dbh->selectrow_array($sql);
	    };
	    
	    # If we found a permission string, unpack it and set each permission.
	    
	    if ( $permission )
	    {
		my @list = split qr{,}, $permission;
		$perms->{table_permission}{$table_specifier} = { map { $_ => 1 } @list };

		# The permission 'modify' also implies 'post'.
		$perms->{table_permission}{$table_specifier}{post} = 1 if
		    $perms->{table_permission}{$table_specifier}{modify};
	    }
	    
	    # Otherwise, compute the permissions from the authorization info and table properties.
	    
	    else
	    {
		$perms->default_table_permissions($table_specifier);
	    }
	}
	
	# If the current user is not a database member, likewise go with the authorization info
	# and table properties.
	
	else
	{
	    $perms->default_table_permissions($table_specifier);
	}
    }
    
    return $perms->{table_permission}{$table_specifier};
}


# clear_cached_permissions ( [table_name] )
# 
# Clear all cached table permissions. This method is provided for use in testing this and related
# modules. It is safe for general use, because any subsequent attempt to retrieve table
# permissions will cause them to be reloaded.

sub clear_cached_permissions {
    
    my ($perms, $table_specifier) = @_;
    
    # If a table name was given, then clear its permissions. Otherwise, clear all of them.

    if ( $table_specifier )
    {
	delete $perms->{table_permission}{$table_specifier};
    }
    
    else
    {
	$perms->{table_permission} = { };
    }
}


# default_table_permissions ( table_name )
# 
# If we have do not already have the user's table permissions for the specified table, set a
# default from the authorization info and table properties.

sub default_table_permissions {
    
    my ($perms, $table_specifier) = @_;
    
    # Return if we have already determined the permissions for the specified table.
    
    return $perms->{table_permission}{$table_specifier}
	if ref $perms->{table_permission}{$table_specifier} eq 'HASH';
    
    # Otherwise, initialize the permission hash for this table.
    
    my $tp = $perms->{table_permission}{$table_specifier} = { };
    
    # Do we need to add 'CAN_ADMIN' ???
    
    # If this table allows posting for certain classes of people, check to see
    # if the current user falls into one of them.
    
    if ( my $allow_post = get_table_property($table_specifier, 'CAN_POST') )
    {
	if ( $allow_post eq 'LOGGED_IN' && $perms->{user_id} )
	{
	    $perms->{auth_diag}{$table_specifier} = 'LOGGED_IN';
	    $tp->{post} = 1;
	}
	
	elsif ( $allow_post eq 'MEMBERS' && $perms->{enterer_no} )
	{
	    $perms->{auth_diag}{$table_specifier} = 'MEMBERS';
	    $tp->{post} = 1;
	}
	
	elsif ( $allow_post eq 'AUTHORIZED' && $perms->{authorizer_no} )
	{
	    $perms->{auth_diag}{$table_specifier} = 'AUTHORIZED';
	    $tp->{post} = 1;
	}
    }
    
    # If this table allows viewing for certain classes of people, check to see if the current user
    # falls into one of them.
    
    if ( my $allow_view = get_table_property($table_specifier, 'CAN_VIEW') )
    {
	if ( $allow_view eq 'LOGGED_IN' && $perms->{user_id} )
	{
	    $perms->{auth_diag}{$table_specifier} = 'LOGGED_IN';
	    $tp->{view} = 1;
	}
	
	elsif ( $allow_view eq 'MEMBERS' && $perms->{enterer_no} )
	{
	    $perms->{auth_diag}{$table_specifier} = 'MEMBERS';
	    $tp->{view} = 1;
	}
	
	elsif ( $allow_view eq 'AUTHORIZED' && $perms->{enterer_no} && $perms->{authorizer_no} )
	{
	    $perms->{auth_diag}{$table_specifier} = 'AUTHORIZED';
	    $tp->{view} = 1;
	}

	elsif ( $allow_view eq 'ANY' )
	{
	    $perms->{auth_aig}{$table_specifier} = 'ANY';
	    $tp->{view} = 1;
	}
    }
    
    # If this table allows modification of records owned by others for certain classes of people, check
    # to see if the current user falls into one of them.
    
    if ( my $allow_modify = get_table_property($table_specifier, 'CAN_MODIFY') )
    {
	if ( $allow_modify eq 'LOGGED_IN' && $perms->{user_id} )
	{
	    $perms->{auth_diag}{$table_specifier} = 'LOGGED_IN';
	    $tp->{modify} = 1;
	}
	
	elsif ( $allow_modify eq 'MEMBERS' && $perms->{enterer_no} )
	{
	    $perms->{auth_diag}{$table_specifier} = 'MEMBERS';
	    $tp->{modify} = 1;
	}
	
	elsif ( $allow_modify eq 'AUTHORIZED' && $perms->{enterer_no} && $perms->{authorizer_no} )
	{
	    $perms->{auth_diag}{$table_specifier} = 'AUTHORIZED';
	    $tp->{modify} = 1;
	}
    }
    
    return $tp;
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
    
    my ($perms, $table_specifier, $permission) = @_;
    
    croak "bad call to 'check_table_permission': no permission specified" unless $permission;
    croak "bad call to 'check_table_permission': no table name specified" unless $table_specifier;
    croak "bad call to 'check_table_permission': bad permission '$permission'"
	unless $PERMISSION_NAME{$permission};
    
    my $tp = $perms->get_table_permissions($table_specifier);
    
    # If the user has the superuser privilege, or the 'admin' permission on this table, then they
    # have any requested permission.
    
    if ( $perms->is_superuser || $tp->{admin} )
    {
	$perms->debug_line( "    Permission for $table_specifier : '$permission' from " . 
			    ($perms->is_superuser ? 'SUPERUSER' : 'ADMIN') . "\n" );
	
	return 'admin';
    }
    
    # If the user has the permission 'none', then they do not have any permission on this
    # table. This overrides any other attribute except 'admin' or superuser.
    
    elsif ( $tp->{none} )
    {
	$perms->debug_line( "    Permission for $table_specifier : '$permission' DENIED by TABLE_PERMS\n");
	
	return 'none';
    }
    
    # If the user does not have 'admin' permission, then the 'delete' permission is only allowed
    # if the table has the ALLOW_DELETE property.
    
    if ( $permission eq 'delete' )
    {
	unless ( defined $tp->{delete} )
	{
	    $tp->{delete} = get_table_property($table_specifier, 'ALLOW_DELETE') ? 1 : 0;
	}
	
	unless ( $tp->{delete} )
    	{
    	    $perms->debug_line( "    Permission for $table_specifier : '$permission' DENIED by TABLE PROPERTY\n" );
	    
    	    return 'none';
    	}
    }
    
    # If the user does not have 'admin' permission, then the 'insert_key' permission is only allowed
    # if they also have 'post' and if table has the ALLOW_INSERT_KEY property.
    
    elsif ( $permission eq 'insert_key' )
    {
	unless ( $tp->{post} )
	{
	    $perms->debug_line( "   Permission for $table_specifier : '$permission' DENIED : NO PERMISSION\n" );
	    
	    return 'none';
	}
	
	unless ( defined $tp->{insert_key} )
	{
	    $tp->{insert_key} = get_table_property($table_specifier, 'ALLOW_INSERT_KEY') ? 1 : 0;
	}
	
	unless ( $tp->{insert_key} )
    	{
    	    $perms->debug_line( "    Permission for $table_specifier : '$permission' DENIED by TABLE PROPERTY\n" );
	    
    	    return 'none';
    	}
    }
    
    # Now, if we know they have the requested permission, return it.
    
    if ( $tp->{$permission} )
    {
	my $diag = $perms->{auth_diag}{$table_specifier} || 'DEFAULT';
	
	$perms->debug_line( "    Permission for $table_specifier : '$permission' from $diag\n" );
	
	return $permission;
    }
    
    # If the requested permission is 'view' but the user has only 'post' permission, then return
    # 'own' to indicate that they may view their own records only.
    
    elsif ( $permission eq 'view' && $tp->{post} )
    {
	my $diag = $perms->{auth_diag}{$table_specifier} || 'DEFAULT';
	
	$perms->debug_line( "    Permission for $table_specifier : '$permission' from $diag\n" );
	
	return 'own';
    }
    
    # Otherwise, they have no privileges whatsoever to this table.
    
    $perms->debug_line( "   Permission for $table_specifier : '$permission' DENIED : NO PERMISSION\n" );
    
    return 'none';
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
    
    my ($perms, $table_specifier, $permission, $key_expr, $record) = @_;
    
    croak "check_record_permission: no permission specified" unless $permission;
    croak "check_record_permission: no table name specified" unless $table_specifier;
    croak "check_record_permission: no key expr specified" unless $key_expr;
    croak "check_record_permission: bad permission '$permission'"
	unless $PERMISSION_NAME{$permission};
    
    # Start by fetching the user's permissions for the table as a whole.
    
    my $tp = $perms->get_table_permissions($table_specifier);
    
    # If the requested permission is 'view' and the table permissions allow this, then we are done.
    
    if ( $permission eq 'view' && $tp->{view} )
    {
	$perms->debug_line( "    Permission for $table_specifier ($key_expr) : '$permission' from TABLE_PERMS\n" );
	
	return $permission;
    }
    
    # Otherwise, if the person is not logged in then they have no permission to do anything.
    
    if ( $perms->{role} eq 'none' )
    {
	$perms->debug_line( "    Permission for $table_specifier ($key_expr) : '$permission' DENIED : NOT LOGGED IN\n" );
	
	return 'none';
    }
    
    # Otherwise, we need to check the record itself to see if it exists. If so, we also fetch the
    # information necessary to tell whether the user is the person who created or authorized
    # it. Unless we were given the current contents of the record, fetch the info necessary to
    # determine permissions.  If the record was not found, then return 'notfound' to indicate that
    # the record was not found.
    
    unless ( ref $record eq 'HASH' )
    {
	$record = $perms->get_record_authinfo($table_specifier, $key_expr);
	
	unless ( ref $record eq 'HASH' && %$record )
	{
	    $perms->debug_line( "    Permission for $table_specifier ($key_expr) : '$permission' : NOT FOUND\n" );
	    
	    return 'notfound';
	}
    }
    
    # If the table permission is 'admin' or if the user has superuser privileges, then they have
    # all privileges on this record including 'delete'. Return 'admin' to indicate that they have
    # the requested permission and also administrative permission. But if the record is locked,
    # then return 'unlock' instead. The operation method should, in this case, allow the user to
    # modify or delete the record only if they are also unlocking it.
    
    if ( $perms->is_superuser || $tp->{admin} )
    {
	my $p = ($record->{admin_lock} || $record->{owner_lock}) ? 'admin,unlock' : 'admin';
	
	$perms->debug_line( "    Permission for $table_specifier ($key_expr) : '$p' from " . 
			    ($perms->is_superuser ? 'SUPERUSER' : 'ADMIN') . "\n" );
	
	return $p;
    }
    
    # If the user does not have 'admin' permission, then the 'delete' permission is only allowed
    # if the table has the ALLOW_DELETE property.
    
    if ( $permission eq 'delete' )
    {
    	unless ( defined $tp->{delete} )
	{
	    $tp->{delete} = get_table_property($table_specifier, 'ALLOW_DELETE') ? 1 : 0;
	}
	
	unless ( $tp->{delete} )
    	{
    	    $perms->debug_line( "    Permission for $table_specifier ($key_expr) : '$permission' DENIED : TABLE PROPERTY\n" );
	    
    	    return 'none';
    	}
    }
    
    # If the record has an administrative lock, then the user does not have any permissions to it
    # unless they have administrative privileges, in which case the operation would have been
    # approved above. We return 'locked' instead of the requested permission, if they would
    # otherwise have that permission.
    
    if ( $record->{admin_lock} )
    {
	$perms->debug_line( "    Permission for $table_specifier ($key_expr) : '$permission' DENIED : LOCKED\n" );
	
	return 'locked';
    }
    
    # If the requested permission is 'edit' or 'delete' and the user has 'modify' permission on
    # the table as a whole, then they can edit or delete this particular record regardless of who
    # owns it. Otherwise, they only have permission to edit or delete records that they entered or
    # authorized. But a locked record can only be unlocked by the owner or by an administrator.
    
    if ( $tp->{modify} && ( $permission eq 'edit' || $permission eq 'delete' ) && ! $record->{owner_lock} )
    {
	my $diag = $perms->{auth_diag}{$table_specifier} || 'DEFAULT';
	
	$perms->debug_line( "    Permission for $table_specifier ($key_expr) : '$permission' from $diag\n" );
	
	return $permission;
    }
    
    # If the user is the person who originally created or authorized the record, then they have
    # 'view', 'edit', and 'delete' permissions (the latter if allowed for this table).
    
    if ( $record->{enterer_no} && $perms->{enterer_no} &&
	 $record->{enterer_no} eq $perms->{enterer_no} )
    {
	$perms->debug_line( "    Permission for $table_specifier ($key_expr) : '$permission' from enterer_no\n" );
	
	return $record->{owner_lock} ? "$permission,unlock" : $permission;
    }
    
    if ( $record->{authorizer_no} && $perms->{enterer_no} &&
	 $record->{authorizer_no} eq $perms->{enterer_no} )
    {
	$perms->debug_line( "    Permission for $table_specifier ($key_expr) : '$permission' from authorizer_no\n" );
	
	return $record->{owner_lock} ? "$permission,unlock" : $permission;
    }
    
    if ( $record->{enterer_id} && $perms->{user_id} &&
	 $record->{enterer_id} eq $perms->{user_id} )
    {
	$perms->debug_line( "    Permission for $table_specifier ($key_expr) : '$permission' from enterer_id\n" );
	
	return $record->{owner_lock} ? "$permission,unlock" : $permission;
    }
    
    # If the user has the same authorizer as the person who originally created the record, then
    # they have 'view', 'edit' and 'delete' permissions if the table has the 'BY_AUTHORIZER'
    # property.
    
    if ( $record->{authorizer_no} && $perms->{authorizer_no} &&
	 $record->{authorizer_no} eq $perms->{authorizer_no} )
    {
	if ( $tp->{by_authorizer} //= get_table_property($table_specifier, 'BY_AUTHORIZER') )
	{
	    $perms->debug_line( "    Permission for $table_specifier ($key_expr) : '$permission' from BY_AUTHORIZER\n" );
	    
	    return $record->{owner_lock} ? "$permission,unlock" : $permission;
	}
    }
    
    # Otherwise, the requestor has no permission on this record. If they would have been able to
    # modify it except that the record was locked, return 'locked'. Otherwise, return 'none'.
    
    $perms->debug_line( "    Permission for $table_specifier ($key_expr) : '$permission' DENIED : NO PERMISSION\n" );
    
    if ( $tp->{modify} && ( $permission eq 'edit' || $permission eq 'delete' ) )
    {
	return 'locked';
    }
    
    else
    {
	return 'none';
    }
}


# check_if_owner ( table_specifier, permission, key_expr )
#
# Return 1 if the current user has owner rights to the record, 0 otherwise. Superusers and table
# administrators count as owners.

sub check_if_owner {
    
    my ($perms, $table_specifier, $key_expr, $record) = @_;
    
    croak "check_record_permission: no table name specified" unless $table_specifier;
    croak "check_record_permission: no key expr specified" unless $key_expr;
    
    # Start by fetching the user's permissions for the table as a whole.
    
    my $tp = $perms->get_table_permissions($table_specifier);
    
    # If the table permission is 'admin' or if the user has superuser privileges, then return
    # true.
    
    if ( $perms->is_superuser || $tp->{admin} )
    {
	$perms->debug_line( "    Owner of $table_specifier ($key_expr) : true from " . 
			    ($perms->is_superuser ? 'SUPERUSER' : 'ADMIN') . "\n" );
	
	return 1;
    }

    # Otherwise, we need to fetch the information necessary to tell whether the user is the person
    # who created or authorized it. Unless we were given the current contents of the record, fetch
    # the info necessary to determine permissions.  If the record was not found, then return
    # 0.
    
    unless ( ref $record eq 'HASH' )
    {
	$record = $perms->get_record_authinfo($table_specifier, $key_expr);
	
	unless ( ref $record eq 'HASH' && %$record )
	{
	    $perms->debug_line( "    Owner of $table_specifier ($key_expr) : NOT FOUND\n" );
	    
	    return 0;
	}
    }
    
    # If the user is the person who originally created or authorized the record, then they are the owner.
    
    if ( $record->{enterer_no} && $perms->{enterer_no} &&
	 $record->{enterer_no} eq $perms->{enterer_no} )
    {
	$perms->debug_line( "    Owner of $table_specifier ($key_expr) : true from enterer_no\n" );
	
	return 1;
    }
    
    if ( $record->{authorizer_no} && $perms->{enterer_no} &&
	 $record->{authorizer_no} eq $perms->{enterer_no} )
    {
	$perms->debug_line( "    Owner of $table_specifier ($key_expr) : true from authorizer_no\n" );
	
	return 1;
    }
    
    if ( $record->{enterer_id} && $perms->{user_id} &&
	 $record->{enterer_id} eq $perms->{user_id} )
    {
	$perms->debug_line( "    Owner of $table_specifier ($key_expr) : true from enterer_id\n" );
	
	return 1;
    }
    
    # If the user has the same authorizer as the person who originally created the record, then
    # that counts too if the table has the 'BY_AUTHORIZER' property.
    
    if ( $record->{authorizer_no} && $perms->{authorizer_no} &&
	 $record->{authorizer_no} eq $perms->{authorizer_no} )
    {
	if ( $tp->{by_authorizer} //= get_table_property($table_specifier, 'BY_AUTHORIZER') )
	{
	    $perms->debug_line( "    Owner of $table_specifier ($key_expr) : true from BY_AUTHORIZER\n" );
	    
	    return 1;
	}
    }
    
    # Otherwise, the current user is not the owner of the record.

    return 0;
}


# get_record_authinfo ( table_name, key_expr )
# 
# Fetch the authorization info for this record, in order to determine if the current user has
# permission to carry out some operation on it.

sub get_record_authinfo {
    
    my ($perms, $table_specifier, $key_expr) = @_;
    
    # First get a list of the authorization fields for this table.
    
    my $auth_fields = get_authinfo_fields($perms->{dbh}, $table_specifier, $perms->{debug});
    
    # If it is empty, then just fetch the key value. This will allow us to check that the record
    # actually exists. If the table has no primary key, then just return an empty record.
    
    unless ( $auth_fields )
    {
	$auth_fields = get_table_property($table_specifier, 'PRIMARY_KEY');
	
	return { } unless $auth_fields;
    }
    
    # Otherwise, construct an SQL statement to get the values of these fields.
    
    my $sql = "
	SELECT $auth_fields FROM $TABLE{$table_specifier}
	WHERE $key_expr LIMIT 1";
    
    $perms->debug_line( "$sql\n" );
    
    my $record = $perms->{dbh}->selectrow_hashref($sql);
    
    return $record;
}


# record_filter ( table_name )
# 
# Return a filter expression that should be included in an SQL statement to select only records
# viewable by this user.

sub record_filter {
    
    my ($perms, $table_specifier) = @_;
    
    # This still needs to be implemented... $$$
}


1;
