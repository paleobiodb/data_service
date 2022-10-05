# 
# EditTransaction::Authorization
# 
# This role provides methods for authorizing the individual actions that perform
# the work of each transaction.
# 


package EditTransaction::Authorization;

use strict;

use Switch::Plain;
use Carp qw(carp croak);

use Moo::Role;

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
# a table specifier and a requested authorization from the following list:
# 
# view           view rows in this table
# 
# post           add rows to this table; this implies authorization to modify rows you own
# 
# modify         modify rows in this table other than ones you own
# 
# admin          execute restricted actions on this table
# 
# insert_key     add rows to this table with specified primary key values
# 
# The return value should be either a listref representing an error condition, or a string value
# from the following list. The result should be the first value that applies to the specified
# database table according to the permission object.
# 
# unrestricted   if this class does not do authorization checks, or if this particular table
#                does not have any authorization information
# 
# admin          if the permission object allows the requested authorization and also indicates
#                administrative permission on this table or superuser privilege
# 
# own            if the permission object allows the requested authorization only for objects
#                owned by the user identified by the permission object.
# 
# granted        if the permission object allows the requested authorization
# 
# none           if the permission object does not allow the requested authorization
# 
# In no case should an empty result ever be returned. This default method returns 'unrestricted',
# which allows all actions.

sub check_table_permission {

    return 'unrestricted';
}


# check_record_permission ( table, requested, where )
# 
# This routine is called as an instance method to authorize certain kinds of actions. It is passed
# a table specifier, a requested authorization from the following list, and an SQL expression that
# will be used to select rows from the specified table.
# 
# view           view the rows selected from this table by the specified expression
# 
# edit           modify the rows selected from this table by the specified expression
# 
# delete         delete the rows selected from this table by the specified expression
# 
# If an error occurs during this routine, the return value should be a listref representing the
# error condition. If the where expression does not match any rows, the return value should be:
# 
# notfound       if no row matches the specified expression
# 
# If the where expression matches one row, the return value should be the first of the following
# strings that applies to the matching row:
# 
# unrestricted   if this class does not do authorization checks, or if this specific table
#                does not contain authorization information
# 
# none           if the requested authorization is denied for some other reason than a row lock
# 
# locked         if the requested authorization would be granted except that the row has an
#                administrative lock or else has an owner lock and is owned by somebody else
# 
# owned          if the selected row is owned by the user identified by the permission object;
# owned_unlock   the suffix is added if an owner lock is set for this row
# 
# admin          if the permission object allows the requested authorization for the selected
# admin_unlock   row and also indicates administrative permission on this table; the suffix
#                is added if any lock is set for this row
# 
# granted        if the requested authorization is allowed by the permission object but
#                the user does not own the selected row
# 
# If the SQL expression matches two or more rows, the return value should be a list consisting of
# strings from the above table alternating with integer values. Each string value should be
# followed by the number of rows to which it applies. Each row should be counted with the first
# string value that applies to it. For a single matching row, the string value may optionally be
# followed by '1'. In no case should the return value be empty.
# 
# This default method returns 'notfound' if no rows match the where expression, and 'unrestricted'
# followed by the row count otherwise.

sub check_record_permission {
    
    my ($edt, $table_specifier, $requested, $selector) = @_;
    
    my $count;
    
    eval {
        $count = $edt->count_matching_rows($table_specifier, $selector);
    };
    
    if ( $@ )
    {
	$edt->error_line($@) unless $edt->silent_mode;
	return ['E_EXECUTE', "an error occurred while counting the selected record(s)"];
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


# Authorization of actions
# ------------------------

# authorize_action ( action, table, operation, flag )
# 
# Determine whether the current user is authorized to perform the specified action. If so, store
# the indicated permission in the action record. For any operation but 'insert' a key expression
# must be provided.
# 
# This method may be overridden by subclasses, though that is an iffy thing to do because it will
# circumvent all of the permission checks implemented here. Under most circumstances, an override
# method should make additional checks and then call this one. Override methods should indicate
# error and warning conditions by calling the method 'add_condition'.
# 
# If the flag 'FINAL' is given and the authorization has been marked as pending, complete it
# now.

sub authorize_action {
    
    my ($edt, $action, $operation, $table_specifier, $flag) = @_;
    
    # If the authorization cannot be resolved yet, return now. In this case, authorization
    # will be completed just before the action is executed. This typically happens when an
    # action reference is provided as a key value.
    
    if ( $action->permission eq 'PENDING' )
    {
	return unless $flag eq 'FINAL';
    }
    
    # If the action has a key value, check the permission.
    
    # Each operation has different requirements for key values. Check these now.
    
    my $keyexpr = $action->keyexpr;
    my $abort;
    
    # The 'insupdate' operation requires that the table have a primary key. At most one
    # key value can be specified. If the permission is 'PENDING' that means a key
    # reference was specified instead of a key value, which is not allowed for
    # 'insupdate'. If no key value was specified, change the operation to 'insert'.
    
    if ( $operation eq 'insupdate' )
    {
	if ( ! $action->keycol )
	{
	    $edt->add_condition($action, 'E_NO_KEY');
	    $abort = 1;
	}
	
	elsif ( $action->keymult )
	{
	    $edt->add_condition($action, 'E_MULTI_KEY');
	    $abort = 1;
	}
	
	elsif ( $action->permission eq 'PENDING' )
	{
	    $edt->add_condition($action, 'E_BAD_KEY', $action->keyval);
	    $abort = 1;
	}
	
	elsif ( ! $action->keyval )
	{
	    $operation = $action->set_operation('insert');
	    $edt->add_condition($action, 'C_CREATE') unless $edt->allows('CREATE');
	}
    }
    
    # The 'replace' operation requires a single primary key value if the table has a
    # primary key. Key references are not allowed for 'replace' either (see insupdate).
    
    elsif ( $operation eq 'replace' )
    {
	if ( $action->keymult )
	{
	    $edt->add_condition($action, 'E_MULTIPLE_KEYS');
	    $abort = 1;
	}
	
	elsif ( $action->keycol && ! $action->keyval )
	{
	    $edt->add_condition($action, 'E_NO_KEY');
	    $abort = 1;
	}
	
	elsif ( $action->permission eq 'PENDING' )
	{
	    $edt->add_condition('E_BAD_KEY', $action->keyval);
	    $abort = 1;
	}
    }
    
    # The 'update' and 'delete' operations require a valid key expression, which
    # may or may not select specific primary key values.
    
    elsif ( $operation =~ /^update|^delete/ )
    {
	$edt->add_conditions($action, 'E_NO_KEY') unless $keyexpr;
	$abort = 1;
    }
    
    # If the operation is 'insert', add E_HAS_KEY if the table has a primary key and a key
    # value was specified. Otherwise, add C_CREATE unless the CREATE allowance is present.
    
    elsif ( $operation eq 'insert' )
    {
	if ( $action->keycol && $action->keyval )
	{
	    $edt->add_condition('E_HAS_KEY', 'insert');
	    $abort = 1;
	}
	
	elsif ( ! $edt->allows('CREATE') )
	{
	    $edt->add_condition($action, 'C_CREATE');
	}
    }
    
    # If a condition has been generated that completes the authorization, return now.
    
    if ( $abort )
    {
	$action->set_permission('none');
	return;
    }
    
    # Otherwise, get a reference to the information record for this table.
    
    my $tableinfo = $edt->table_info_ref($table_specifier);
    
    unless ( $tableinfo )
    {
	$edt->add_condition($action, 'E_BAD_TABLE', $table_specifier);
	return $action->set_permission('none');
    }
    
    # Check whether the SUPERIOR_TABLE property is set for the specified table. If so, then the
    # authorization check needs to be done on this other table instead of the specified one.
    
    my @permcounts;
    
    if ( my $superior = $tableinfo->{SUPERIOR_TABLE} )
    {
	@permcounts = $edt->authorize_subordinate($action, $operation, $table_specifier, $superior);
    }
    
    # Otherwise, use the standard authorization for each operation. Some operations are authorized
    # against the table permissions, others against individual record permissions.
    
    else
    {
	sswitch ( $operation )
	{
	    case 'insert': {
		
		@permcounts = $edt->check_table_permission($table_specifier, 'post');
	    }
	    
	    case 'update':
	    case 'insupdate':
	    case 'replace': {
		
	        @permcounts = $edt->check_record_permission($table_specifier, 'modify', $keyexpr);
	    }

	    case 'delete': {
		
		@permcounts = $edt->check_record_permission($table_specifier, 'delete', $keyexpr);
	    }
	    
	    case 'delete_cleanup': {
		
		croak "the operation '$operation' can only be done on a subordinate table";
	    }
	    
	    case 'other': {
		
		# $$$ TO DO: need to add a mechanism to specify which permission
		# (i.e. 'modify', 'view', etc. a specific 'other' action requires)
		
		if ( $keyexpr )
		{
		    @permcounts = $edt->check_record_permission($table_specifier, 'modify', $keyexpr);
		}
		
		else
		{
		    @permcounts = $edt->check_table_permission($table_specifier, 'admin');
		}
	    }
	    
	  default: {
		die "bad operation '$operation' in 'authorize_action'";
	    }
	};
    }
    
    # In either case, we will get one or more results from the call. If there are more than
    # one, each result except possibly the last will be followed by a count.
    
    my $result = shift @permcounts;
    my $count = shift @permcounts;
    
    # If the 'notfound' result is first, that means no records at all were found
    # for the specified key expression. This blocks the action unless the operation is
    # 'replace' or 'insupdate', which can possibly proceed.
    
    if ( $result eq 'notfound' )
    {
	# An 'insupdate' operation can turn into an insert if the table
	# permissions include 'insert_key' and this transaction has the CREATE
	# allowance. A 'replace' operation is allowed to proceed under the same
	# circumstances. 
	
	if ( $operation eq 'replace' || $operation eq 'insupdate')
	{
	    my $can_insert = $edt->check_table_permission($table_specifier, 'insert_key');
	    
	    if ( $can_insert && $can_insert ne 'none' )
	    {
		unless ( $edt->{allows}{CREATE} )
		{
		    $edt->add_condition($action, 'C_CREATE');
		}
		
		$action->set_permission($can_insert);
	    }
	    
	    else
	    {
		$edt->add_condition($action, 'E_NOT_FOUND');
		$action->set_permission('notfound');
	    }
	    
	    # If the operation is 'insupdate', change it to 'insert'.
	    
	    if ( $operation eq 'insupdate' )
	    {
		$action->set_operation('insert');
	    }
	}
	
	# In all other situations, add an E_NOT_FOUND condition.
	
	else
	{
	    $edt->add_condition($action, 'E_NOT_FOUND');
	    $action->set_permission('notfound');
	}
	
	return ('notfound');
    }
    
    # Otherwise, if the operation is 'insupdate' then change it to 'update'.
    
    elsif ( $operation eq 'insupdate' )
    {
	$action->set_operation('update');
    }
    
    # Now handle the other cases. If the primary permission is 'none', that
    # means there are at least some records the user has no authorization to
    # operate on, or else the user lacks the necessary permission on the table
    # itself. This is reported as E_PERM.
    
    elsif ( $result eq 'none' )
    {
	$edt->add_condition($action, 'E_PERM', $operation, $count);
	$action->set_permission('none');
	
	return ('none', $count, @permcounts);
    }
    
    # If the primary permission is 'locked', that means the user is authorized to operate on all
    # the records but at least one is either admin_locked or is owner_locked by somebody
    # else. This is reported as E_LOCKED.
    
    elsif ( $result eq 'locked' )
    {
	if ( $action->keymult )
	{
	    $edt->add_condition($action, 'E_LOCKED', 'multiple', $count);
	}
	
	else
	{
	    $edt->add_condition($action, 'E_LOCKED');
	}
	
	$action->set_permission('locked');
	
	return ('locked', $count, @permcounts);
    }
    
    # If the primary permission includes '_unlock', that means some of the records were locked by
    # the user themselves, or else the user has adminitrative privilege and can unlock
    # anything. If the transaction allows 'LOCKED', we can proceed. Otherwise, add an unlock
    # requirement to this action.
    
    elsif ( $result =~ /_unlock/ )
    {
	if ( $edt->allows('LOCKED') )
	{
	    $result =~ s/_unlock//;
	}
	
	else
	{
	    $edt->add_condition($action, 'C_LOCKED');
	    # $action->requires_unlock(1);
	}
	
	return ($result, $count, @permcounts);
    }
    
    # Otherwise, store the permission with the action and return it along with
    # the counts.
    
    else
    {
	$action->set_permission($result);
	
	return ($result, $count, @permcounts);
    }
}


# authorize_subordinate ( action, operation, table, suptable, keyexpr )
# 
# Carry out the authorization operation where the table to be authorized against ($suptable) is
# different from the one on which the action is being executed ($table_specifier). In this situation, the
# "subordinate table" is $table_specifier while the "superior table" is $suptable. The former is subordinate
# because authorization for actions taken on it is referred to the superior table.

sub authorize_subordinate {

    my ($edt, $action, $operation, $table_specifier, $suptable, $keyexpr) = @_;
    
    my ($linkcol, $supcol, $altfield, @linkval, $update_linkval, %PERM, $perm);
    
    local ($_);
    
    # Start by fetching information about the link between the subordinate table and the superior table.
    
    ($linkcol, $supcol, $altfield) = $edt->get_linkinfo($table_specifier, $suptable);
    
    # For an insert operation, the link value is given in the record to be inserted. If it is not
    # found, the action cannot be authorized.
    
    if ( $operation eq 'insert' )
    {
	@linkval = $edt->input_record_value($action, $linkcol, $altfield);
	
	unless ( @linkval )
	{
	    $edt->add_condition($action, 'E_REQUIRED', $linkcol);
	    $PERM{error} = 1;
	}
    }
    
    # The operations 'update_many' and 'delete_many' require administrative permission for the
    # table on which they are being authorized. If we have this permission, the operation is
    # authorized. If not, it isn't.
    
    if ( $operation eq 'update_many' || $operation eq 'delete_many' )
    {
	return $edt->check_table_permission($suptable, 'admin');
    }
    
    # For a 'delete_cleanup' operation, authorization is based on the specified key expression.
    
    elsif ( $operation eq 'delete_cleanup' )
    {
	croak "bad key expression" unless ref $keyexpr eq 'ARRAY' && $keyexpr->@*;
	@linkval = $keyexpr->@*;
    }
    
    # For update, replace, delete, and other operations, the value of this column in the existing
    # database record(s) must be retrieved.
    
    else
    {
	eval
	{
	    @linkval = $edt->db_column_values($table_specifier, $keyexpr, $linkcol, { distinct => 1 });
	};
	
	return 'none' unless @linkval;
	
	# If a different value is given in the record, we must check this value too. The link
	# value can only be updated if authorization succeeds for both of them, and if the
	# transaction allows MOVE_SUBORDINATES.
	
	if ( $operation eq 'update' || $operation eq 'replace' )
	{
	    $update_linkval = $action->record_value_alt($linkcol, $altfield);
	    
	    if ( $update_linkval && any { $_ ne $update_linkval } @linkval )
	    {
		push @linkval, $update_linkval;
		$PERM{c_move} = 1 unless $edt->{allows}{MOVE_SUBORDINATES};
	    }
	}
    }
    
    # $$$ split this off into an 'authorize action' method. 
    
    # Determine the aggregate permission. If any of the link values cannot yet be evaluated, mark
    # this action as needing authorization at execution time. If any of them have a permission of
    # 'none' or 'locked', that will be the aggregate permission. Otherwise, if any of them have a
    # permission of 'edit', that will be the aggregate permission. If all have 'admin', then that
    # will be the aggregate. If we cannot find any permissions at all, the aggregate will be
    # empty.
    
    foreach my $lv ( @linkval )
    {
	# If any of the link values is an action reference, look it up. If no matching action can
	# be found, add an error condition. Otherwise, if the action has a key value then use
	# it. If not, this action will have to be authorized at execution time.
	
	if ( $lv =~ /^&./ )
	{
	    my $ref_action = $edt->{action_ref}{$lv};
	    
	    if ( $ref_action && $ref_action->table eq $suptable )
	    {
		my @refkeys = $ref_action->keyvalues;
		
		if ( @refkeys == 1 )
		{
		    $lv = $refkeys[0];
		}

		# If there is more than one key value corresponding to this reference, it
		# cannot be used to authorize a subordinate action.

		elsif ( @refkeys > 1 )
		{
		    $edt->add_condition($action, 'E_BAD_REFERENCE', '_multiple_', $linkcol, $lv);
		    $PERM{error} = 1;
		    next;
		}
		
		# If the reference is good but the reference has no key value, it is almost
		# certainly a reference to a new record insertion that hasn't been executed
		# yet. This means we will have to put off the authorization until execution time.
		
		else
		{
		    $PERM{later} = 1;
		    next;
		}
	    }
	    
	    # If we cannot find the reference at all, add an error condition. This is NOT a
	    # permission error, and cannot be demoted.
	    
	    else
	    {
		$edt->add_condition($action, 'E_BAD_REFERENCE', $linkcol, $lv);
		$PERM{error} = 1;
		next;
	    }
	}
	
	# If we get here, then we have an actual key value. If we have a cached permission value,
	# use it. Otherwise, generate one. If the keyexpr is just a key value, we need to turn it
	# into an expression.
	
	unless ( $perm = $edt->{permission_record_edit_cache}{$suptable}{$lv} )
	{
	    my $keyexpr = "$supcol=$lv";
	    $perm = $edt->check_record_permission($suptable, 'edit', $keyexpr);
	    $edt->{permission_record_edit_cache}{$suptable}{$lv} = $perm;
	}
	
	# Keep track of which permissions we have found. If the permission has the unlock
	# attribute, count that as well.
	
	if ( $perm =~ /^(.*?),(.*)/ )
	{
	    my $main = $1;
	    my $attrs = $2;
	    
	    $PERM{$main} = 1;
	    $PERM{unlock} = 1 if $attrs =~ /unlock/;
	}
	
	else
	{
	    $PERM{$perm} = 1;
	}
	
	$PERM{bad} = 1 if $perm !~ /^none|^locked|^edit|^admin/;
	
	# Finally, keep track of the superior keys we have authorized against.
	
	$edt->{superior_auth_keys}{$lv} = 1;
    }
    
    # If we need to add a C_MOVE_SUBORDINATES condition, do so now.
    
    $edt->add_condition($action, 'C_MOVE_SUBORDINATES') if $PERM{c_move};
    
    # Now use the %PERM hash to generate an aggregate permission for this action. If any of the
    # linked records had 'none' or 'locked', the action cannot be executed regardless of the other
    # permissions. If any of the linked records have an unrecognized position, the action cannot
    # be executed either.
    
    if ( $PERM{error} )
    {
	return 'error';
    }
    
    elsif ( $PERM{none} || $PERM{bad} )
    {
	return 'none';
    }
    
    elsif ( $PERM{locked} )
    {
	return 'locked';
    }
    
    # Otherwise, if any of the linked records had 'later' then we must put off the authorization
    # until execution time. Save all of the necessary information with the action.
    
    elsif ( $PERM{later} )
    {
	$action->set_linkinfo($linkcol, \@linkval);
	$action->_authorize_later;
	return 'PENDING';
    }

    # Otherwise, the action can proceed. If all of the linked records had 'admin' permission, that
    # is the aggregate permission. Otherwise, it is 'edit'. If any of the linked records had the
    # 'unlock' attribute, add that to the aggregate permission. Now is the time to add
    # C_MOVE_SUBORDINATES if that is indicated.
    
    elsif ( $PERM{edit} || $PERM{admin} )
    {
	my $aggregate = $PERM{edit} ? 'edit' : 'admin';
	$aggregate .= ',unlock' if $PERM{unlock};
	
	
	return $aggregate;
    }
    
    # As a fallback, just return 'none'.
    
    else
    {
	return 'none';
    }
}


# get_linkinfo ( table, suptable )
#
# Return information about the link between $table_specifier and $suptable.

sub get_linkinfo {

    my ($edt, $table_specifier, $suptable) = @_;
    
    # If we have this information already cached, return it now.
    
    if ( $edt->{permission_link_cache}{$table_specifier} && ref $edt->{permission_link_cache} eq 'ARRAY' &&
	 $edt->{permission_link_cache}{$table_specifier}[0] )
    {
	return $edt->{permission_link_cache}{$table_specifier}->@*;
    }
    
    # Otherwise, the subordinate table must contain a column that links records in this table to
    # records in the superior table. If no linking column is specified, assume it is the same as
    # the primary key of the superior table.
    
    my $linkcol = get_table_property($table_specifier, 'SUPERIOR_KEY');
    my $supcol = get_table_property($suptable, 'PRIMARY_KEY');
    
    $linkcol ||= $supcol;
    
    my $altfield = get_table_property($table_specifier, $linkcol, 'ALTERNATE_NAME') ||
	($linkcol =~ /(.*)_no$/ && "${1}_id");
    
    croak "SUPERIOR_TABLE was given as '$suptable' but no key column was found"
	unless $linkcol;
    
    $edt->{permission_link_cache}{$table_specifier} = [$linkcol, $supcol, $altfield];
    
    return ($linkcol, $supcol, $altfield);
}


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
	
# 	$alt_permission = $edt->check_record_permission($suptable, 'edit', $alt_keyexpr);
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


1;
