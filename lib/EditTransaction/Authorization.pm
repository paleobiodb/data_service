# 
# EditTransaction::Authorization
# 
# This role provides methods for authorizing the individual actions that perform
# the work of each transaction.
# 


package EditTransaction::Authorization;

use strict;

use Switch::Plain;
use Scalar::Util qw(reftype);
use Carp qw(carp croak);
use TableDefs qw(%TABLE);

use Role::Tiny;

no warnings 'uninitialized';


# Default authorization methods
# -----------------------------

# The default behavior for EditTransaction is to allow every action. If you wish to restrict
# actions to authorized users, you must implement a subclass that overrides the following
# methods. A common way to do this is to include an application role that overrides these methods
# to implement some set of authorization rules, along with a set of special columns that indicate
# row ownership.
# 
# For classes that provide authorization checking, a permission object must be supplied for each
# new transaction instance. The supplied permission object must encapsulate the user's identity
# and permissions. Authentication of the user and generation of the proper permission object are
# outside the scope of the EditTransaction system.


# check_instance_permission ( permission )
#
# This routine is called once as a class method for every blessed argument to the class
# constructor. It should return true if the argument represents a valid permission object, false
# otherwise. This default method always returns false, because the default configuration for
# EditTransaction does not accept a permission object.

sub check_instance_permission {

    return '';
}


# validate_instance_permission ( permission )
# 
# This routine is called as a class method for each new EditTransaction instance, with its
# argument being the permission object (if any) supplied for this instance. It is expected to
# either throw an exception using 'croak' or else return something to be stored in the
# 'permission' attribute of the new instance. Typically, the permission object will be checked
# and, if valid, will be returned.
# 
# This default method returns 'unrestricted', which allows all actions.

sub validate_instance_permission {

    return 'unrestricted';
}


# check_table_permission ( table, requested )
# 
# This routine is called as an instance method to authorize certain kinds of actions. It is passed
# a table specifier and a requested permission from the following list:
# 
# view           view rows in this table
# 
# post           add rows to this table with auto-insert primary key values; this implies
#                authorization to modify rows you own
# 
# insert         add rows to this table with specified primary key values; this also
#                implies authorization to modify rows you own
# 
# modify         modify rows in this table
# 
# admin          execute restricted actions on this table
# 
# The return value should be either a listref representing an error condition, or a string value
# from the following list. The result should be the first value that applies to the specified
# database table according to the permission object.
# 
# unrestricted   this class does not do authorization checks, or this particular table
#                does not have any authorization information, or the permission object
#                includes the 'superuser' attribute.
# 
# admin          the permission object allows the requested authorization and includes
#                administrative permission on this table
# 
# own            the permission object allows the requested authorization only for objects
#                owned by the user identified by the permission object.
# 
# granted        if the permission object allows the requested authorization
# 
# none           if the permission object does not allow the requested authorization
# 
# In no case should an empty result ever be returned. This default method returns 'unrestricted',
# which allows all actions.

sub check_table_permission {
    
    my ($edt, $table_specifier, $requested) = @_;
    
    my $tableinfo = $edt->table_info_ref($table_specifier) || return 'none';
    
    if ( $requested eq 'view' )
    {
	return 'none' if $tableinfo->{CAN_VIEW} eq 'none';
    }
    
    elsif ( $requested eq 'post' )
    {
	return 'none' if $tableinfo->{CAN_POST} eq 'none';
    }
    
    elsif ( $requested eq 'insert' )
    {
	return 'none' if $tableinfo->{CAN_INSERT} eq 'none';
    }
    
    elsif ( $requested eq 'modify' )
    {
	return 'none' if $tableinfo->{CAN_MODIFY} eq 'none';
    }
    
    return 'unrestricted';
}


# check_row_permission ( table, requested, where )
# 
# This routine is called as an instance method to authorize certain kinds of actions. It
# is passed a table specifier, a requested authorization from the following list, and an
# SQL expression that will be used to select rows from the specified table.
# 
# view           view the rows selected from this table by the specified expression
# 
# modify         modify the rows selected from this table by the specified expression
# 
# delete         delete the rows selected from this table by the specified expression
# 
# If an error occurs during this routine, the return value should be a listref
# representing the error condition. If the where expression does not match any rows, the
# return value should be either:
# 
# notfound       no row matches the specified expression
# 
# If the where expression matches one or more rows, each of the following permissions that
# applies to at least one row in the set should be returned followed by a count of
# matching rows. When multiple values are returned, they should occur in the order listed
# below. Each row should be counted according to the highest of the permissions that apply
# to it.
# 
# unrestricted   this class does not do authorization checks, or this specific table does
#                not contain authorization information, or the permission object includes
#                the 'superuser' attribute.
# 
# none           the requested permission is denied for some other reason than a row lock
# 
# locked         the requested permission would be granted except that the row(s) has an
#                administrative lock or else has an owner lock and is owned by somebody else
# 
# owned          the selected row is owned by the user identified by the permission object
# owned_unlock   the suffix is added if an owner lock is set for the row(s)
# 
# admin          the permission object allows the requested authorization for the selected
# admin_unlock   row and also indicates administrative permission on this table; the suffix
#                is added if any lock is set for this row
# 
# granted        the requested authorization is allowed by the permission object but
#                the user does not own the selected row
# 
# For a single matching row, the permission may optionally be followed by '1'. In no case
# should the return value be empty.
# 
# This default method returns 'notfound' if no rows match the where expression, and 'unrestricted'
# followed by the row count otherwise.

sub check_row_permission {
    
    my ($edt, $table_specifier, $requested, $key_expr) = @_;
    
    my $tableinfo = $edt->table_info_ref($table_specifier) || return 'none';
    
    if ( $requested eq 'view' )
    {
	return 'none' if $tableinfo->{CAN_VIEW} eq 'none';
    }
    
    elsif ( $requested eq 'modify' )
    {
	return 'none' if $tableinfo->{CAN_MODIFY} eq 'none';
    }
    
    if ( $requested eq 'delete' )
    {
	return 'none' if $tableinfo->{CAN_DELETE} eq 'none';
    }
    
    my ($count) = $edt->db_count_matching_rows($table_specifier, $key_expr);
    
    if ( ref $count eq 'ARRAY' )
    {
	return $count;
    }
    
    elsif ( $count )
    {
	return ('unrestricted', $count);
    }
    
    else
    {
	return 'notfound';
    }
}


sub check_superior_permission {
    
    my ($edt, $superior_table, $requested, $subordinate_table, $key_expr);
    
    my $count = $edt->db_count_matching_rows($subordinate_table, $key_expr);
    
    if ( ref $count eq 'ARRAY' )
    {
	return $count;
    }
    
    elsif ( $count )
    {
	return ('unrestricted', $count);
    }
    
    else
    {
	return 'notfound';
    }
}


sub authorize_insert_key {
    
    return 'unrestricted';
}


# Authorization of actions
# ------------------------

# authorize_against_table ( action, operation, table_specifier )
# 
# Determine whether the permission object associated with this transaction is authorized
# to perform the specified action.  If so, store the indicated permission in the action
# record and return it.

sub authorize_against_table {
    
    my ($edt, $action, $operation, $table_specifier) = @_;
    
    # If the action permission is already set, return it.
    
    if ( $action->permission && $action->permission ne 'PENDING' )
    {
	return $action->permission;
    }
    
    # If $operation or $table_specifier aren't passed as arguments, fill them in.
    
    $operation ||= $action->operation;
    $table_specifier ||= $action->table;
    
    # Get a reference to the cached information about the table on which the action will
    # be operating. If none is available, set the permission to 'none' and return. The
    # action should already have an 'E_BAD_TABLE' condition if this is the case.
    
    my $tableinfo = $edt->table_info_ref($table_specifier) || 
	return $action->permission('none');
    
    my $auth_table = $tableinfo->{AUTH_TABLE};
    my $auth_tableinfo;
    
    # Step 1: determine permission
    # ----------------------------
    
    my $key_expr = $action->keyexpr;
    
    my ($primary, $count, @rest);
    
    my ($subordinate_count, $link_expr, $link_permission, $insert_key);
    
    # If the table corresponding to $table_specifier is subordinate to another table,
    # any action requires 'modify' permission on the linked records in that table.
    
    if ( $auth_table )
    {
	my $auth_tableinfo = $edt->table_info_ref($auth_table);
	
	unless ( $auth_tableinfo )
	{
	    $edt->add_condition('E_BAD_TABLE', $auth_table);
	    return;
	}
	
	my $link_col = $tableinfo->{AUTH_KEY};
	my $auth_col = $auth_tableinfo->{PRIMARY_KEY} || $link_col;
	
	unless ( $link_col )
	{
	    $edt->add_condition('E_EXECUTE', 'no linking column was found');
	}
	
	# If no linking keys were found, attempt to fetch a linking key from the
	# existing database record.
	
	if ( $key_expr && ! $action->linkvalues )
	{
	    my $dbh = $edt->dbh;
	    
	    my $sql = "SELECT distinct $link_col FROM $TABLE{$table_specifier} WHERE $key_expr";
	    
	    $edt->debug_line("$sql\n") if $edt->debug_mode;
	    
	    my $result = $dbh->selectcol_arrayref($sql);
	    
	    my $link_list = join(',', map { $dbh->quote($_) } @$result);
	    
	    if ( $link_list )
	    {
		$action->set_linkinfo($auth_col, $link_col, $link_col, $result, $link_list);
	    }
	}
	
	# If a link expression is present, check for 'modify' permission on the specified
	# superior record.
	
	if ( $link_expr = $action->linkexpr )
	{
	    ($link_permission) = $edt->check_row_permission($auth_table, 'modify', 
							    $link_expr);
	    
	    $link_permission ||= 'none';
	    
	    # If an error occurred, add it to the action and return.
	    
	    if ( ref $link_permission eq 'ARRAY' )
	    {
		my $msg = join(': ', $link_permission->@*);
		$edt->error_line($msg);
		$edt->add_condition($action, 'E_BAD_LINK', $action->linkval);
		return $action->set_permission('none');
	    }
	    
	    # If the linking key was not found, add E_NOT_FOUND.
	    
	    elsif ( $link_permission eq 'notfound' )
	    {
		$edt->add_condition($action, 'E_NOT_FOUND', 'link');
		return $action->set_permission('notfound');
	    }
	    
	    # If permission was denied, add E_PERMISSION.
	    
	    elsif ( $link_permission eq 'none' )
	    {
		$edt->add_condition($action, 'E_PERMISSION', 'link');
		return $action->set_permission('none');
	    }
	    
	    # If the linked record is locked, drop down to step 2.
	    
	    elsif ( $link_permission =~ /lock/ )
	    {
		$primary = $link_permission;
		$count = 1;
	    }
	    
	    # If permission is granted for the linked record and a key expression is
	    # present, get the permission for the records selected by that expression.
	    
	    elsif ( $key_expr )
	    {
		($primary, $count) = $edt->check_row_permission($table_specifier, 'modify', 
								$key_expr);
		
		# If an error was returned, add it and return 'none'.
		
		if ( ref $primary eq 'ARRAY' )
		{
		    $edt->add_condition($action, $primary->@*);
		    return $action->set_permission('none');
		}
		
		# If the operation is an insert or replace and the key expression does not
		# match any row, check to see if we have permission to insert records with
		# arbitrary key values into the table.
		
		elsif ( $primary eq 'notfound' && $operation =~ /^ins|^rep/ )
		{
		    my $insert_perm = $edt->authorize_insert_key($operation, 
								 $table_specifier) || 'none';
		    
		    if ( $insert_perm eq 'none' )
		    {
			$edt->add_condition($action, 'E_NOT_FOUND');
			return $action->set_permission('notfound');
		    }
		    
		    else
		    {
			$primary = $insert_perm;
			$insert_key = 1;
		    }
		}
		
		elsif ( $operation eq 'insert' )
		{
		    $edt->add_condition($action, 'E_DUPLICATE');
		    return $action->set_permission('none');
		}
	    }
	    
	    elsif ( $operation =~ /^ins/ )
	    {
		$primary = $link_permission;
		$operation = $action->set_operation('insert');
		$count = 1;
	    }
	}
	
	else
	{
	    $edt->add_condition($action, 'E_NO_KEY', 'link');
	    return $action->set_permission('none');
	}
    }
    
    # For a regular table, if the action parameters include a key expression then check
    # whether we have permission to operate on the selected records.  The requested
    # permission is 'modify', unless the operation is delete in which case we request
    # 'delete' permission. The reason for this is that some tables may require
    # administrative permission to delete records, or may allow deletion only by record
    # owners whereas any authorized user may modify.
    
    elsif ( $key_expr )
    {
	my $requested = $operation eq 'delete' ? 'delete' : 'modify';
	
	($primary, $count, @rest) = 
	    $edt->check_row_permission($table_specifier, $requested, $key_expr);
	
	$primary ||= 'none';
	
	if ( ref $primary eq 'ARRAY' )
	{
	    $edt->add_condition($action, $primary->@*);
	    return $action->set_permission('none');
	}
	
	elsif ( $primary eq 'notfound' && $operation =~ /^ins|^rep/ )
	{
	    my $insert_perm = $edt->authorize_insert_key($operation, 
							 $table_specifier) || 'none';
	    
	    if ( $insert_perm eq 'none' )
	    {
		$edt->add_condition($action, 'E_NOT_FOUND');
		return $action->set_permission('notfound');
	    }
	    
	    else
	    {
		$primary = $insert_perm;
		$insert_key = 1;
		$count = 1;
	    }
	}
	
	elsif ( $operation eq 'insert' )
	{
	    $edt->add_condition($action, 'E_DUPLICATE');
	    return $action->set_permission('none');
	}
    }
    
    # If no key expression is present and the operation is one of the 'insert' variants,
    # then check for 'post' permission on the table.
    
    elsif ( $operation =~ /^ins/ )
    {
	$primary = $edt->check_table_permission($table_specifier, 'post') || 'none';
	$operation = $action->set_operation('insert');
	$count = 1;
    }
    
    # If the action parameters do not include a key expression and the operation is
    # 'other', check for 'admin' permission on the table.
    
    if ( $operation eq 'other' && ! $key_expr )
    {
	$primary = $edt->check_table_permission($table_specifier, 'admin') || 'none';
	$count = 1;
    }
    
    # At this point, if we do not have a primary permission, that means no key expression
    # is present and the operation requires one. Add E_NO_KEY and return.
    
    unless ( $primary )
    {
	$edt->add_condition($action, 'E_NO_KEY', 'operation', $operation);
	return $action->set_permission('none');
    }
    
    # Step 2: resolve 'insert' and 'replace' operations
    # -------------------------------------------------
    
    # The insert and replace operations may require extra steps to resolve.
    
    if ( $operation =~ /^ins|^rep/ )
    {
	# If $insert_key was set, the operation is set to 'insert'. We must check whether
	# permission is granted to insert records with specified keys into the table. If
	# not, an E_PERMISSION condition will be added below.
	
	if ( $insert_key )
	{
	    $primary = $edt->check_table_permission($table_specifier, 'insert') || 'none';
	    $operation = $action->set_operation('insert');
	}
	
	# Otherwise, if the operation is 'insert' but no key expression was specified, and a
	# primary key value is required (i.e. because the primary key is not auto_insert),
	# add E_REQUIRED.  This is needed because the validation subroutine does not check
	# the primary key column(s). 
	
	elsif ( $operation eq 'insert' )
	{
	    if ( $tableinfo->{PRIMARY_REQUIRED} )
	    {
		$edt->add_condition($action, 'E_REQUIRED', $tableinfo->{PRIMARY_KEY});	
	    }
	}
	
	# Otherwise, no insertion is taking place. Change 'insreplace' and 'insupdate' to
	# 'replace' and 'update' respectively.
	
	elsif ( $operation eq 'insreplace' )
	{
	    $operation = $action->set_operation('replace');
	}
	
	elsif ( $operation eq 'insupdate' )
	{
	    $operation = $action->set_operation('update');
	}
    }
    
    # # If we are creating a row and the 'CREATE' allowance is not present, add C_CREATE.
    
    # if ( $create_row )
    # {
    # 	$edt->add_condition('main', 'C_CREATE') unless $edt->allows('CREATE');
    # }
    
    # Step 3: set action permission
    # -----------------------------
    
    # If the primary permission is a listref, that means an error occured while checking
    # the permission. Add it to the action and return.
    
    if ( ref $primary eq 'ARRAY' )
    {
	$edt->add_condition($action, $primary->@*);
	return $action->set_permission('none');
    }
    
    # If the primary permission is 'notfound', that means no records at all matched the
    # specified key expression for this action.
    
    if ( $primary eq 'notfound' )
    {
	if ( $link_permission )
	{
	    $edt->add_condition($action, 'E_NOT_FOUND', 'link');
	}
	
	else
	{
	    $edt->add_condition($action, 'E_NOT_FOUND');
	}
	
	return $action->set_permission('notfound');
    }
    
    # If the primary permission is 'none', that means there are one or more records the
    # user has no authorization to operate on, or else the user lacks the necessary
    # permission on the table itself.
    
    elsif ( $primary eq 'none' )
    {
	$edt->add_condition($action, 'E_PERMISSION', $operation, $count);
	$action->set_permission('none');
	
	return ($primary, $count, @rest);
    }
    
    # If the primary permission is 'locked', that means the user is authorized to operate on all
    # the records but at least one is either admin_locked or is owner_locked by somebody
    # else.
    
    elsif ( $primary eq 'locked' )
    {
	if ( $action->keymult )
	{
	    $edt->add_condition($action, 'E_LOCKED', 'multiple', $count);
	}
	
	else
	{
	    $edt->add_condition($action, 'E_LOCKED');
	}
	
	$action->set_permission('none');
	
	return ($primary, $count, @rest);
    }
    
    # If the primary permission includes '_unlock', that means some of the records were locked by
    # the user themselves, or else the user has administrative privilege and can unlock
    # anything. If the transaction allows 'LOCKED', we can proceed. Otherwise, add an unlock
    # requirement to this action.
    
    # $$$ the return from get_row_permissions should state which field is locked, so that
    # we can check if it is being unlocked by the current action.
    
    elsif ( $primary =~ /_unlock/ )
    {
	if ( $edt->allows('LOCKED') )
	{
	    $primary =~ s/_unlock//;
	}
	
	else
	{
	    $edt->add_condition('main', 'C_LOCKED');
	    $edt->add_condition($action, 'W_LOCKED');
	    # $action->requires_unlock(1);
	}
    }
    
    # Store the permission with the action and return.
    
    $action->set_permission($primary);
    
    return ($primary, $count);
}


# Key values
# ----------
#
# Most actions are associated with one or more unique key values which specify
# the database record(s) being operated on. The following methods are involved in
# handling them.

# unpack_key_values ( action, table, operation, params )
# 
# Examine $params, and construct a canonical list of key values. The key value
# parameter can be either a listref or a scalar. If a scalar, it is assumed to
# contain either a single key value or a comma-separated list of key values. In
# the latter case, all commas, spaces and quotation marks are discarded and a
# list is returned of the remaining values.
# 
# If at least one valid key value is found, and none of them are invalid, generate an SQL
# expression using $column_name that will select the corresponding records. This method
# does not check if those key values actually exist in the database. Under most
# circumstances, $column_name will be the primary key column for the table to which the
# key values will apply.
# 
# Store the key value(s) and SQL expression in the action object. If individual key values
# are found to be invalid (not empty or 0, which are ignored), add error condition(s) to
# the action.

sub unpack_key_values {
    
    my ($edt, $action, $table_specifier, $operation, $params) = @_;
    
    # Get a full description of this table from the database, if we don't already have it.
    # If we can't get one, return without doing anything. This should never happen,
    # because _new_action checks the table information before calling this subroutine.
    
    my $tableinfo = $edt->table_info_ref($table_specifier) || return;
    
    # If the parameters include a non-empty value for _where and no primary key
    # information, set the key expression to the value of _where. But if both 'where' and
    # a primary key value are given, add an error condition.
    
    if ( $params->{_where} )
    {
	my $primary_key = $tableinfo->{PRIMARY_KEY};
	my $primary_field = $tableinfo->{PRIMARY_FIELD};
	
	if ( $params->{_primary} ||
	     ($primary_key && $params->{$primary_key}) ||
	     ($primary_field && $params->{$primary_field}) )
	{
	    $edt->add_condition('E_BAD_KEY', '_where');
	    return $action->set_permission('none');
	}
	
	else
	{
	    $action->set_keyinfo('', '', undef, $params->{_where});
	    return;
	}
	
	# Also add an error condition if this is an 'insert' or 'replace'
	# operation, as neither of those can take a 'where' clause.
	
	if ( $operation =~ /^ins|^rep/ )
	{
	    $edt->add_condition('E_HAS_WHERE', $operation);
	}
    }
    
    # Otherwise, look for a primary key. If none is defined for this table, return without
    # doing anything.
    
    my ($key_column, $link_column, $abort);
    
    unless ( $key_column = $tableinfo->{PRIMARY_KEY} )
    {
	# If the action parameters include _primary, add an error condition.
	
	$edt->add_condition('E_BAD_KEY', "table '$table_specifier' has no primary key")
	    if $params->{_primary};
	
	# Return without setting any key values.
	
	return;
    }
    
    my $columninfo = $edt->table_column_ref($table_specifier, $key_column);
    
    # Check if this table is marked as being subordinate to another table. If so, make
    # sure that table and the necessary attributes exist.
    
    my $auth_table = $tableinfo->{AUTH_TABLE};
    my ($auth_tableinfo, $link_columninfo);
    
    if ( $auth_table )
    {
	unless ( $auth_tableinfo = $edt->table_info_ref($auth_table) )
	{
	    $edt->add_condition('E_BAD_TABLE', 'superior', $auth_table);
	    $abort = 1;
	}
	
	unless ( ( $link_column = $tableinfo->{AUTH_KEY} ) &&
		 ( $link_columninfo = $edt->table_column_ref($table_specifier, $link_column) ) )
	{
	    $edt->add_condition('E_BAD_TABLE', $table_specifier);
	    $abort = 1;
	}
    }
    
    # If the operation is 'delete_cleanup', key values refer to the superior table rather
    # than the one we are operating on.
    
    if ( $operation eq 'delete_cleanup' )
    {
	unless ( $auth_table )
	{
	    $edt->add_condition('E_BAD_TABLE', 'no superior', $table_specifier);
	    $abort = 1;
	}
	
	$key_column = $tableinfo->{AUTH_KEY};
	$table_specifier = $auth_table;
	$tableinfo = $auth_tableinfo;
    }
    
    if ( $abort )
    {
	return $action->set_permission('none');
    }
    
    # Now look for key values. Unless we have a hashref of action parameters, return
    # immediately.
    
    return unless ref $params && reftype $params eq 'HASH';
    
    # Check for key values under any of the following parameters: _primary, the primary
    # key, and the field named by PRIMARY_FIELD if any. Add an error condition if more
    # than one of these has a value.
    
    my ($key_field, $raw_values, $ref_type);
    
    foreach my $p ( $key_column, '_primary', $tableinfo->{PRIMARY_FIELD} )
    {
	if ( $p && $params->{$p} )
	{
	    $raw_values = $params->{$p};
	    $ref_type = ref $raw_values;
	    
	    next unless ! $ref_type || $ref_type =~ /::/ ||
		$ref_type eq 'ARRAY' && $raw_values->@*;
	    
	    if ( $key_field && $key_field ne $p )
	    {
		$edt->add_condition('E_BAD_KEY_FIELD', $key_field, $p);
		$raw_values = undef;
	    }
	    
	    else
	    {
		$key_field = $p;
	    }
	}
    }
    
    # If we found any raw values, filter them to separate acceptable from unacceptable
    # ones. Add error conditions to the action as appropriate.
    
    my $app_call = $edt->can('before_key_column');
    my $dbh = $edt->dbh;
    
    if ( $key_field && $raw_values )
    {
	if ( my @key_values = $edt->check_key_values($action, $columninfo,
						     $key_field, $raw_values, $app_call) )
	{
	    my $quoted = join ',', map { $dbh->quote($_) } @key_values;
	    
	    if ( @key_values == 1 )
	    {
		$action->set_keyinfo($key_column, $key_field, $key_values[0], $quoted); 
	    }
	    
	    elsif ( @key_values > 1 )
	    {
		# The 'insert' and 'replace' operations require a single key per
		# record.
		
		if ( $operation =~ /^ins|^rep/ )
		{
		    $edt->add_condition('E_MULTI_KEY', $operation);
		}
		
		$action->set_keyinfo($key_column, $key_field, \@key_values, $quoted);
	    }
	}
    }
    
    else
    {
	$action->set_keyinfo($key_column, $key_field, undef, "0");
    }
    
    # If this is a subordinate table, check and store any link values that were provided.
    
    if ( $auth_table )
    {
	my $auth_column = $auth_tableinfo->{PRIMARY_KEY} || $link_column;
	
	my ($link_field, $link_value);
	
	foreach my $p ( $link_column, $tableinfo->{AUTH_FIELD} )
	{
	    if ( $p && $params->{$p} )
	    {
		$link_value = $params->{$p};
		$ref_type = ref $link_value;
		
		next unless ! $ref_type || $ref_type =~ /::/ ||
		    $ref_type eq 'ARRAY' && $raw_values->@*;
		
		if ( $link_field && $link_field ne $p )
		{
		    $edt->add_condition($action, 'E_BAD_KEY_FIELD', $link_field, $p);
		    $link_value = undef;
		}
		
		else
		{
		    $link_field = $p;
		}
	    }
	}
	
	if ( $link_field && $link_value )
	{
	    my @link_values = $edt->check_key_values($action, $link_columninfo,
						     $link_field, $link_value, $app_call);
	    
	    if ( @link_values > 1 )
	    {
		$edt->add_condition($action, 'E_BAD_FIELD', 'link', $tableinfo->{AUTH_KEY});
	    }
	    
	    else
	    {
		my $quoted = $dbh->quote($link_value);
		$action->set_linkinfo($auth_column, $link_column, $link_field, $link_value, $quoted);
	    }
	}
	
	else
	{
	    $action->set_linkinfo($auth_column, $link_column, $link_field);
	}
    }	
}


    # # Otherwise, look for particular patterns in $raw_values. If if it matches one of
    # # the expressions "<name> = <value>" or "<name> in (<values...>)", extract the value
    # # string and make sure that the column matches $key_column.
    
    # if ( ! ref $raw_values && $raw_values =~
    # 	 qr{ ^ \s* (\w+) (?: \s* = \s* | \s+ in \s* [(] ) ( [^)]* ) [)]? \s* $ }xsi )
    # {
    # 	my $check_column = $1;
    # 	$raw_values = $2;

    # 	if ( $check_column ne $key_column )
    # 	{
    # 	    $action->add_condition('E_BAD_SELECTOR', $field || 'unknown',
    # 				   "invalid key column '$check_column'");
    # 	}
    # }		

sub check_key_values {
    
    my ($edt, $action, $columninfo, $field, $value, $app_call) = @_;
    
    my (@key_values, @bad_values);
    
    my $action_table = $action->table;
        
  VALUE:
    foreach my $v ( ref $value eq 'ARRAY' ? $value->@* : 
		    $value =~ /,/ ? split(/\s*,\s*/, $value) : $value )
    {
	# Skip values that are empty or zero.
	
	next VALUE unless $v;
	
	# If the value is quoted, remove the quotes and skip if the remainder is empty or zero.
	
	if ( $v =~ qr{ ^ (['"]) (.*) \1 $ }xs )
	{
	    $v = $2;
	    next VALUE unless $v;
	}
	
	# A value that is an action label must be looked up. If the action is found but has no
	# key value, authentication will have to be delayed until execution time.
	
	elsif ( $v =~ /^&/ )
	{
	    my $ref_action = $edt->{action_ref}{$v};
		
	    if ( $ref_action && $ref_action->table eq $action_table )
	    {
		if ( $ref_action->keyvalues )
		{
		    push @key_values, $ref_action->keyvalues;
		}
		    
		else
		{
		    push @key_values, $v;
		    $action->permission('PENDING');
		}
	    }
		
	    else
	    {
		$action->add_condition('E_BAD_REFERENCE', $field, $v);
	    }
		
	    next VALUE;
	}
	    
	# Otherwise, the key value must be checked. If this EditTransaction subclass
	# includes an application role that implements 'before_key_column', call it now.
	# If it returns an error condition, add that condition and go on to the next
	# value.
	
	if ( $app_call )
	{
	    my ($result, $clean_value, $additional) =
		$edt->before_key_column($columninfo, $action->operation, $v, $field);
	    
	    if ( ref $result eq 'ARRAY' || ref $additional eq 'ARRAY' )
	    {
		$edt->add_condition($action, $result->@*, $additional);
		next VALUE if ref $result;
	    }
	    
	    elsif ( $result )
	    {
		$v = $clean_value;
	    }
	}
	
	# If the key column type is integer, reject key values that are not
	# integers.
	
	if ( $columninfo->{TypeMain} eq 'unsigned' && $v !~ /^\s*\d+\s*$/ )
	{
	    push @bad_values, $v;
	}
	
	elsif ( $columninfo->{TypeMain} eq 'integer' && $v !~ /^\s*-?\d+\s*$/ )
	{
	    push @bad_values, $v;
	}
	
	else
	{
	    push @key_values, $v;
	}
    }
    
    # If any bad values were found, add an error condition for all of them.
    
    if ( @bad_values )
    {
	my $key_string = join ',', @bad_values;
	$edt->add_condition($action, 'E_BAD_KEY', $field, $key_string);
    }
    
    # Return the good values, if any.
    
    return @key_values;
}


1;

# # authorize_superior ( action, operation, table, suptable, keyexpr )
# # 
# # Carry out the authorization operation where the table to be authorized against ($suptable) is
# # different from the one on which the action is being executed ($table_specifier). In this situation, the
# # "subordinate table" is $table_specifier while the "superior table" is $suptable. The former is subordinate
# # because authorization for actions taken on it is referred to the superior table.

# sub authorize_superior {

#     my ($edt, $action, $operation, $table_specifier, $suptable, $keyexpr) = @_;
    
#     my ($linkcol, $supcol, $altfield, @linkval, $update_linkval, %PERM, $perm);
    
#     local ($_);
    
#     # Start by fetching information about the link between the subordinate table and the superior table.
    
#     ($linkcol, $supcol, $altfield) = $edt->get_linkinfo($table_specifier, $suptable);
    
#     # For an insert operation, the link value is given in the record to be inserted. If it is not
#     # found, the action cannot be authorized.
    
#     if ( $operation eq 'insert' )
#     {
# 	@linkval = $edt->input_record_value($action, $linkcol, $altfield);
	
# 	unless ( @linkval )
# 	{
# 	    $edt->add_condition($action, 'E_REQUIRED', $linkcol);
# 	    $PERM{error} = 1;
# 	}
#     }
    
#     # The operations 'update_many' and 'delete_many' require administrative permission for the
#     # table on which they are being authorized. If we have this permission, the operation is
#     # authorized. If not, it isn't.
    
#     if ( $operation eq 'update_many' || $operation eq 'delete_many' )
#     {
# 	return $edt->check_table_permission($suptable, 'admin');
#     }
    
#     # For a 'delete_cleanup' operation, authorization is based on the specified key expression.
    
#     elsif ( $operation eq 'delete_cleanup' )
#     {
# 	croak "bad key expression" unless ref $keyexpr eq 'ARRAY' && $keyexpr->@*;
# 	@linkval = $keyexpr->@*;
#     }
    
#     # For update, replace, delete, and other operations, the value of this column in the existing
#     # database record(s) must be retrieved.
    
#     else
#     {
# 	eval
# 	{
# 	    @linkval = $edt->db_column_values($table_specifier, $keyexpr, $linkcol, { distinct => 1 });
# 	};
	
# 	return 'none' unless @linkval;
	
# 	# If a different value is given in the record, we must check this value too. The link
# 	# value can only be updated if authorization succeeds for both of them, and if the
# 	# transaction allows MOVE_SUBORDINATES.
	
# 	if ( $operation eq 'update' || $operation eq 'replace' )
# 	{
# 	    $update_linkval = $action->record_value_alt($linkcol, $altfield);
	    
# 	    if ( $update_linkval && any { $_ ne $update_linkval } @linkval )
# 	    {
# 		push @linkval, $update_linkval;
# 		$PERM{c_move} = 1 unless $edt->{allows}{MOVE_SUBORDINATES};
# 	    }
# 	}
#     }
    
#     # $$$ split this off into an 'authorize action' method. 
    
#     # Determine the aggregate permission. If any of the link values cannot yet be evaluated, mark
#     # this action as needing authorization at execution time. If any of them have a permission of
#     # 'none' or 'locked', that will be the aggregate permission. Otherwise, if any of them have a
#     # permission of 'edit', that will be the aggregate permission. If all have 'admin', then that
#     # will be the aggregate. If we cannot find any permissions at all, the aggregate will be
#     # empty.
    
#     foreach my $lv ( @linkval )
#     {
# 	# If any of the link values is an action reference, look it up. If no matching action can
# 	# be found, add an error condition. Otherwise, if the action has a key value then use
# 	# it. If not, this action will have to be authorized at execution time.
	
# 	if ( $lv =~ /^&./ )
# 	{
# 	    my $ref_action = $edt->{action_ref}{$lv};
	    
# 	    if ( $ref_action && $ref_action->table eq $suptable )
# 	    {
# 		my @refkeys = $ref_action->keyvalues;
		
# 		if ( @refkeys == 1 )
# 		{
# 		    $lv = $refkeys[0];
# 		}

# 		# If there is more than one key value corresponding to this reference, it
# 		# cannot be used to authorize a subordinate action.

# 		elsif ( @refkeys > 1 )
# 		{
# 		    $edt->add_condition($action, 'E_BAD_REFERENCE', '_multiple_', $linkcol, $lv);
# 		    $PERM{error} = 1;
# 		    next;
# 		}
		
# 		# If the reference is good but the reference has no key value, it is almost
# 		# certainly a reference to a new record insertion that hasn't been executed
# 		# yet. This means we will have to put off the authorization until execution time.
		
# 		else
# 		{
# 		    $PERM{later} = 1;
# 		    next;
# 		}
# 	    }
	    
# 	    # If we cannot find the reference at all, add an error condition. This is NOT a
# 	    # permission error, and cannot be demoted.
	    
# 	    else
# 	    {
# 		$edt->add_condition($action, 'E_BAD_REFERENCE', $linkcol, $lv);
# 		$PERM{error} = 1;
# 		next;
# 	    }
# 	}
	
# 	# If we get here, then we have an actual key value. If we have a cached permission value,
# 	# use it. Otherwise, generate one. If the keyexpr is just a key value, we need to turn it
# 	# into an expression.
	
# 	unless ( $perm = $edt->{permission_record_edit_cache}{$suptable}{$lv} )
# 	{
# 	    my $keyexpr = "$supcol=$lv";
# 	    $perm = $edt->check_row_permission($suptable, 'edit', $keyexpr);
# 	    $edt->{permission_record_edit_cache}{$suptable}{$lv} = $perm;
# 	}
	
# 	# Keep track of which permissions we have found. If the permission has the unlock
# 	# attribute, count that as well.
	
# 	if ( $perm =~ /^(.*?),(.*)/ )
# 	{
# 	    my $main = $1;
# 	    my $attrs = $2;
	    
# 	    $PERM{$main} = 1;
# 	    $PERM{unlock} = 1 if $attrs =~ /unlock/;
# 	}
	
# 	else
# 	{
# 	    $PERM{$perm} = 1;
# 	}
	
# 	$PERM{bad} = 1 if $perm !~ /^none|^locked|^edit|^admin/;
	
# 	# Finally, keep track of the superior keys we have authorized against.
	
# 	$edt->{superior_auth_keys}{$lv} = 1;
#     }
    
#     # If we need to add a C_MOVE_SUBORDINATES condition, do so now.
    
#     $edt->add_condition($action, 'C_MOVE_SUBORDINATES') if $PERM{c_move};
    
#     # Now use the %PERM hash to generate an aggregate permission for this action. If any of the
#     # linked records had 'none' or 'locked', the action cannot be executed regardless of the other
#     # permissions. If any of the linked records have an unrecognized position, the action cannot
#     # be executed either.
    
#     if ( $PERM{error} )
#     {
# 	return 'error';
#     }
    
#     elsif ( $PERM{none} || $PERM{bad} )
#     {
# 	return 'none';
#     }
    
#     elsif ( $PERM{locked} )
#     {
# 	return 'locked';
#     }
    
#     # Otherwise, if any of the linked records had 'later' then we must put off the authorization
#     # until execution time. Save all of the necessary information with the action.
    
#     elsif ( $PERM{later} )
#     {
# 	$action->set_linkinfo($linkcol, \@linkval);
# 	$action->_authorize_later;
# 	return 'PENDING';
#     }

#     # Otherwise, the action can proceed. If all of the linked records had 'admin' permission, that
#     # is the aggregate permission. Otherwise, it is 'edit'. If any of the linked records had the
#     # 'unlock' attribute, add that to the aggregate permission. Now is the time to add
#     # C_MOVE_SUBORDINATES if that is indicated.
    
#     elsif ( $PERM{edit} || $PERM{admin} )
#     {
# 	my $aggregate = $PERM{edit} ? 'edit' : 'admin';
# 	$aggregate .= ',unlock' if $PERM{unlock};
	
	
# 	return $aggregate;
#     }
    
#     # As a fallback, just return 'none'.
    
#     else
#     {
# 	return 'none';
#     }
# }


# # get_linkinfo ( table, suptable )
# #
# # Return information about the link between $table_specifier and $suptable.

# sub get_linkinfo {

#     my ($edt, $table_specifier, $suptable) = @_;
    
#     # If we have this information already cached, return it now.
    
#     if ( $edt->{permission_link_cache}{$table_specifier} && ref $edt->{permission_link_cache} eq 'ARRAY' &&
# 	 $edt->{permission_link_cache}{$table_specifier}[0] )
#     {
# 	return $edt->{permission_link_cache}{$table_specifier}->@*;
#     }
    
#     # Otherwise, the subordinate table must contain a column that links records in this table to
#     # records in the superior table. If no linking column is specified, assume it is the same as
#     # the primary key of the superior table.
    
#     my $linkcol = get_table_property($table_specifier, 'AUTH_KEY');
#     my $supcol = get_table_property($suptable, 'PRIMARY_KEY');
    
#     $linkcol ||= $supcol;
    
#     my $altfield = get_table_property($table_specifier, $linkcol, 'ALTERNATE_NAME') ||
# 	($linkcol =~ /(.*)_no$/ && "${1}_id");
    
#     croak "AUTH_TABLE was given as '$suptable' but no key column was found"
# 	unless $linkcol;
    
#     $edt->{permission_link_cache}{$table_specifier} = [$linkcol, $supcol, $altfield];
    
#     return ($linkcol, $supcol, $altfield);
# }


#     # If we were given a key expression for this record, fetch the current value for the
#     # linking column from that row.
    
#     my ($keyval, $linkval, $new_linkval, $record_col);
    
#     if ( $keyexpr )
#     {
# 	# $$$ This needs to be updated to allow for multiple $linkval keys!!!
	
# 	unless ( $linkval )
# 	{
# 	    $edt->add_condition($action, 'E_NOT_FOUND', $action->keyval);
# 	    return 'none';
# 	}
#     }
    
#     # Then fetch the new value, if any, from the action record. But not for a 'delete_cleanup'
#     # operation, for which that is not applicable.
    
#     if ( $operation eq 'insert' || $operation eq 'update' || $operation eq 'replace' || $operation eq 'other' )
#     {
# 	($new_linkval, $record_col) = $edt->record_value($action, $table_specifier, $linkcol);
#     }
    
#     # If we don't have one or the other value, that is an error.
    
#     unless ( $linkval || $new_linkval )
#     {
# 	$edt->add_condition($action, 'E_REQUIRED', $linkcol);
# 	return 'none';
#     }
    
#     # If we have both and they differ, that is also an error. It is disallowed to use an 'update'
#     # operation to switch the association of a subordinate record to a different superior record.
    
#     if ( $linkval && $new_linkval && $linkval ne $new_linkval )
#     {
# 	$edt->add_condition($action, 'E_BAD_UPDATE', $record_col);
# 	return 'none';
#     }
    
#     # Now that these two conditions have been checked, we make sure that $linkval has the proper
#     # value in it regardless of whether it is new (i.e. for an 'insert') or old (for other operations).
    
#     $linkval ||= $new_linkval;
    
#     # Now store this value in the action, for later record-keeping.
    
#     $action->set_linkval($linkval);
    
#     # If we have a cached permission result for this linkval, then just return that. There is no
#     # reason to look up the same superior record multiple times in the course of a single
#     # transaction.
    
#     my $alt_permission;
    
#     if ( $edt->{linkval_cache}{$linkval} )
#     {
# 	$alt_permission = $edt->{linkval_cache}{$linkval};
#     }
    
#     # If the link value is a label, then it must represent an action that either updated
#     # or inserted a record into the proper table earlier in the transaction. If this
#     # operation succeeded, it must have been properly authorized. So all we need to do is
#     # check that this action is associated with the correct table, and if so we return the
#     # indicated permission.

#     # $$$ This is not enough, because of PROCEED. The previous action may have failed but the
#     # transaction may still go through. This will need to be checked during execution.
    
#     elsif ( $linkval =~ /^@/ )
#     {
# 	my $action = $edt->{action_ref}{$linkval};
	
# 	if ( $action && $action->table eq $suptable )
# 	{
# 	    $alt_permission = $edt->check_table_permission($suptable, 'edit');
# 	    $edt->{linkval_cache}{$linkval} = $alt_permission;
	    
# 	    # # If we have 'admin' permission on the superior table, then we return 'admin'.
	    
# 	    # if (  eq 'admin' )
# 	    # {
# 	    # 	$permission = 'admin';
# 	    # 	$edt->{linkval_cache}{$linkval} = $permission;
# 	    # }
	    
# 	    # # Otherwise, we return the proper permission for the operation we are doing.
	    
# 	    # elsif ( $operation eq 'insert' )
# 	    # {
# 	    # 	$permission = 'post';
# 	    # 	$edt->{linkval_cache}{$linkval} = 'edit';
# 	    # }
	    
# 	    # elsif ( $operation eq 'delete' )
# 	    # {
# 	    # 	$permission = 'delete';
# 	    # 	$edt->{linkval_cache}{$linkval} = 'edit';
# 	    # }
	    
# 	    # else
# 	    # {
# 	    # 	$permission = 'edit';
# 	    # 	$edt->{linkval_cache}{$linkval} = 'edit';
# 	    # }
	    
# 	    # return $permission;
# 	}
	
# 	else
# 	{
# 	    $edt->add_condition($action, 'E_BAD_REFERENCE', $record_col, $label);
# 	    return 'error';
# 	}
#     }
    
#     # Otherwise, generate a key expression for the superior record so that we can check
#     # permissions on that. If we cannot generate one, then we return with an error. The
#     # 'aux_keyexpr' routine will already have added one or more error conditions in this
#     # case.
    
#     else
#     {
# 	my $alt_keyexpr = $edt->aux_keyexpr($action, $suptable, $sup_keycol, $linkval, $record_col);
	
# 	return 'error' unless $alt_keyexpr;
	
# 	# Now we carry out the permission check on the permission table. The permission check is for
# 	# modifying the superior record, since that is essentially what we are doing. Whether we are
# 	# inserting, updating, or deleting subordinate records, that essentially counts as modifying
# 	# the superior record.
	
# 	$alt_permission = $edt->check_row_permission($suptable, 'edit', $alt_keyexpr);
# 	$edt->{linkval_cache}{$linkval} = $alt_permission;
#     }
    
#     # Now, if the alt permission is 'admin', then the subordinate permission must be as well.
    
#     if ( $alt_permission =~ /admin/ )
#     {
# 	return $alt_permission;
#     }
    
#     # If the alt permission is 'edit', then we need to figure out what subordinate permission we
#     # are being asked for and return that.
    
#     elsif ( $alt_permission =~ /edit|post/ )
#     {
# 	my $unlock = $alt_permission =~ /unlock/ ? ',unlock' : '';
	
# 	if ( $operation eq 'insert' )
# 	{
# 	    return 'post';
# 	}
	
# 	elsif ( $operation eq 'delete' || $operation eq 'delete_many' || $operation eq 'delete_cleanup' )
# 	{
# 	    return "delete$unlock";
# 	}
	
# 	elsif ( $operation eq 'update' || $operation eq 'update_many' || $operation eq 'replace' || $operation eq 'other' )
# 	{
# 	    return "edit$unlock";
# 	}
	
# 	else
# 	{
# 	    croak "bad subordinate operation '$operation'";
# 	}
#     }
    
#     # If the returned permission is 'notfound', then the record that is supposed to be linked to
#     # does not exist. This should generate an E_KEY_NOT_FOUND.
    
#     elsif ( $alt_permission eq 'notfound' )
#     {
# 	$record_col ||= '';
# 	$edt->add_condition($action, 'E_KEY_NOT_FOUND', $record_col, $linkval);
# 	return 'error';
#     }
    
#     # Otherwise, the permission returned should be 'none'. So return that.
    
#     else
#     {
# 	return 'none';
#     }
# }


    # # The 'insupdate' operation requires that the table have a primary key. At most one
    # # key value can be specified. If the permission is 'PENDING' that means a key
    # # reference was specified instead of a key value, which is not allowed for
    # # 'insupdate'. If no key value was specified, change the operation to 'insert'.
    
    # if ( $operation eq 'insupdate' )
    # {
    # 	if ( ! $action->keycol )
    # 	{
    # 	    $edt->add_condition($action, 'E_NO_KEY');
    # 	    $abort = 1;
    # 	}
	
    # 	elsif ( $action->keymult )
    # 	{
    # 	    $edt->add_condition($action, 'E_MULTI_KEY');
    # 	    $abort = 1;
    # 	}
	
    # 	elsif ( $action->permission eq 'PENDING' )
    # 	{
    # 	    $edt->add_condition($action, 'E_BAD_KEY', $action->keyval);
    # 	    $abort = 1;
    # 	}
	
    # 	elsif ( ! $action->keyval )
    # 	{
    # 	    $operation = $action->set_operation('insert');
    # 	    $edt->add_condition($action, 'C_CREATE') unless $edt->allows('CREATE');
    # 	}
    # }
    
    # # The 'replace' operation requires a single primary key value if the table has a
    # # primary key. Key references are not allowed for 'replace' either (see insupdate).
    
    # elsif ( $operation eq 'replace' )
    # {
    # 	if ( $action->keymult )
    # 	{
    # 	    $edt->add_condition($action, 'E_MULTIPLE_KEYS');
    # 	    $abort = 1;
    # 	}
	
    # 	elsif ( $action->keycol && ! $action->keyval )
    # 	{
    # 	    $edt->add_condition($action, 'E_NO_KEY');
    # 	    $abort = 1;
    # 	}
	
    # 	elsif ( $action->permission eq 'PENDING' )
    # 	{
    # 	    $edt->add_condition('E_BAD_KEY', $action->keyval);
    # 	    $abort = 1;
    # 	}
    # }
    
    # # The 'update' and 'delete' operations require a valid key expression, which
    # # may or may not select specific primary key values.
    
    # elsif ( $operation =~ /^update|^delete/ )
    # {
    # 	$edt->add_conditions($action, 'E_NO_KEY') unless $keyexpr;
    # 	$abort = 1;
    # }
    
    # # If the operation is 'insert', add E_HAS_KEY if the table has a primary key and a key
    # # value was specified. Otherwise, add C_CREATE unless the CREATE allowance is present.
    
    # elsif ( $operation eq 'insert' )
    # {
    # 	if ( $action->keycol && $action->keyval )
    # 	{
    # 	    $edt->add_condition('E_HAS_KEY', 'insert');
    # 	    $abort = 1;
    # 	}
	
    # 	elsif ( ! $edt->allows('CREATE') )
    # 	{
    # 	    $edt->add_condition($action, 'C_CREATE');
    # 	}
    # }
    
    # # If a condition has been generated that completes the authorization, return now.
    
    # if ( $abort )
    # {
    # 	$action->set_permission('none');
    # 	return;
    # }
    
