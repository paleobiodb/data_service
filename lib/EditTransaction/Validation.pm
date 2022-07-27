# 
# The Paleobiology Database
# 
#   EditTransaction::Validation - role for checking permissions and validating actions.
# 


package EditTransaction::Validation;

use strict;

use Carp qw(carp croak);
use ExternalIdent qw(%IDP %IDRE);
use TableDefs qw(get_table_property %TABLE
		 %COMMON_FIELD_SPECIAL %COMMON_FIELD_IDTYPE %FOREIGN_KEY_TABLE %FOREIGN_KEY_COL);
use TableData qw(get_table_schema);
use Permissions;

use Moo::Role;

no warnings 'uninitialized';


UNITCHECK {
    EditTransaction->register_hook_value('validate_special_column', ['ts_created', 'ts_modified'],
					 \&validate_special_column, 'DEFAULT');
};


# Permission checking
# -------------------

# The methods listed below call the equivalent methods of the Permissions object that
# was used to initialize this EditTransaction.

sub check_table_permission {

    my ($edt, $table, $permission) = @_;
    
    unless ( $edt->{permission_table_cache}{$table}{$permission} )
    {
	$edt->{permission_table_cache}{$table}{$permission} = 
	    $edt->{perms}->check_table_permission($table, $permission);
    }
}

sub check_record_permission {
    
    my ($edt, $table, $requested, $key_expr) = @_;
    
    return $edt->{perms}->check_multiple_permission($table, $requested, $key_expr);
    
    # unless ( $edt->{permission_record_cache}{$table}{$key_expr}{$requested} )
    # {
    # 	$edt->{permission_record_cache}{$table}{$key_expr}{$requested} = 
    # 	    [ $edt->{perms}->check_multiple_permission($table, $requested, $key_expr, $record) ];
    # }
    
    # return $edt->{permission_record_cache}{$table}{$key_expr}{$requested}->@*;
}

sub check_many_permission {

    my ($edt, $table, $permission, $key_expr, $record) = @_;
    
    return $edt->{perms}->check_many_permission($table, $permission, $key_expr, $record);
}

# Action validation
# -----------------

# The methods in this section provide default validation for records to be inserted and
# updated. This is done by comparing the field values to the types of the corresponding columns
# from the database schema for the table, plus any attributes specifically specified for the
# column using 'set_column_property' such as 'REQUIRED' and 'ADMIN_SET'.
# 
# Subclasses may override this method, to add additional checks. It is recommended that they call
# this method as a SUPER, because it comprehensively checks every field value against the
# corresponding table definition.


# validate_against_schema ( action, operation, table, flag )
# 
# Check the field values to be stored in the database against the corresponding table definition,
# and call 'add_condition' to record any error or warning conditions that are detected. The
# column names and corresponding values to be stored are added to the action record using
# 'set_column_values', for later use by the action execution methods.
# 
# If the $flag argument is present, it should have the value 'FINAL' to indicate that we should
# now complete a pending validation.

our %EXTID_CHECK;

sub validate_against_schema {

    my ($edt, $action, $operation, $table, $flag) = @_;
    
    no warnings 'uninitialized';
    
    # If the validation status is 'COMPLETE', then return immediately without doing anything.
    
    my $vstatus = $action->validation_status;
    
    return if $vstatus eq 'COMPLETE';
    
    # If the status is 'PENDING', then return unless this method was called with the 'FINAL'
    # flag. In this case, complete the validation process and set the status to reflect it.
    
    unless ( $flag && $flag eq 'FINAL' )
    {
	return if $vstatus eq 'PENDING';
    }
    
    # Grab some extra attributes to be used in the validation process.
    
    my $record = $action->record;
    my $permission = $action->permission;
    my $keycol = $action->keycol;
    
    # Grab the table schema, or throw an exception if it is not available. This information is cached, so
    # the database will only need to be asked for this information once per process per table.
    
    my $dbh = $edt->dbh;
    my ($table_desc, $column_desc) = $edt->call_hook('get_table_description', $table);
    
    # Get all column directives for this action.
    
    my %directives = $action->all_directives;
    
    # Iterate through the list of table columns and construct a list of column names and values
    # for insertion, update, or replacement.
    
    my (@column_names, @column_values, %used, @unchanged);
    
  COLUMN:
    foreach my $colname ( $table_desc->{COLUMN_LIST}->@* )
    {
	# Get the description record for this column.
	
	my $cr = $schema->{$colname};
	
	# The following variables keep track of the value found in the record and the hash key
	# under which it was found, plus some other attributes.
	
	my ($value, $fieldname, $special, $additional, $no_quote, $is_default);
	
	# Check to see if this column has a directive. If none was explicitly assigned, certain
	# specially named columns default to specific handling directives.
	
	my $directive = $directives{$colname};
	
	# If we are directed to ignore this column, skip it unless it is the primary key.
	
	if ( $directive eq 'ignore' )
	{
	    next COLUMN unless $colname eq $keycol;
	}
	
	# The primary key column is handled differently from all the others. Its fieldname and
	# value were previously determined, and its value has either already been checked or will
	# be when authentication completes.
	
	if ( $colname eq $keycol )
	{
	    # Count the field name as having been used in this record. It might not be the same as
	    # the column name.
	    
	    if ( $fieldname = $action->keyfield )
	    {
		$used{$fieldname} = 1;
	    }
	    
	    # If the operation is 'replace', use the already determined key value and set the
	    # directive to 'pass' to disable any further checking.
	    
	    if ( $operation eq 'replace' )
	    {
		$value = $action->keyval;
		$directive = 'pass';
	    }
	    
	    # For all other operations, the primary key should not appear in the column/value
	    # lists.
	    
	    else
	    {
		next COLUMN;
	    }
	}
	
	# For all other columns, if the column name appears in the record then use the
	# corresponding value unless the ALTERNATE_ONLY property is set for this column.
	
	elsif ( exists $record->{$colname} && not $cr->{ALTERNATE_ONLY} )
	{
	    $fieldname = $colname;
	    $value = $record->{$colname};
	    $used{$fieldname} = 1;
	}
	
	# Otherwise, if an alternate field name appears in the record, use the corresponding
	# value.
	
	elsif ( $cr->{ALTERNATE_NAME} && exists $record->{$cr->{ALTERNATE_NAME}} )
	{
	    $fieldname = $cr->{ALTERNATE_NAME};
	    $value = $record->{$fieldname};
	    $used{$fieldname} = 1;
	}
	
	# Otherwise, the column has no assigned value.
	
	# Determine how to handle this column
	# -----------------------------------
	
	# If the directive is either 'pass' or 'unquoted', skip all value checking. Ignore the
	# column unless it has an assigned value.
	
	if ( $directive eq 'pass' || $directive eq 'unquoted' )
	{
	    $no_quote = 1 if $directive eq 'unquoted';
	    next COLUMN unless $fieldname;
	}
	
	# If the directive is 'copy', the response depends on the operation.
	
	elsif ( $directive eq 'copy' )
	{
	    # For 'update', set the column value to itself.

	    if ( $operation eq 'update' )
	    {
		$value = $colname;
		$no_quote = 1;
	    }
	    
	    # For 'replace', add this column to the @unchanged list. For 'insert', skip the column
	    # entirely.
	    
	    else
	    {
		push @unchanged, $colname if $operation eq 'replace';
		next COLUMN;
	    }
	}
	
	# If the column has one of the special handling directives, then check the value according
	# that directive. This is done even if there is no assigned value, because some column values
	# are assigned by the special handler.
	
	elsif ( $directive && $directive ne 'validate' )
	{
	    # If the current EditTransaction uses an application module that implements an
	    # 'app_validate_special_column' hook, call that hook. This module provides a default
	    # that handles only the 'ts_created' and 'ts_modified' directives. Everything else
	    # must be handled by the application module.
	    
	    if ( my $hook = $edt->has_hook('validate_special_column', $directive) )
	    {
		($value, $additional, $no_quote) =
		    &$hook($edt, $directive, $cr, $operation, $permission, $value, $fieldname);
	    }
	    
	    # If no hook was found for this directive, add an error.
	    
	    else
	    {
		my $message = $fieldname ? "Field '$fieldname': no validation hook for '$directive'" :
		    "Column '$colname': no validation hook for '$directive'";
		
		$edt->add_condition($action, 'E_EXECUTE', $message);
		next COLUMN;
	    }
	    
	    # If an 'E_PERM_LOCK' was returned, fill in the message.
	    
	    if ( ref $value eq 'ARRAY' && $value->[0] eq 'E_PERM_LOCK' )
	    {
		if ( $action->keymult )
		{
		    push @$value, '_multiple_';
		}
	    }
	    
	    # If the value is 'UNCHANGED', handle it according to the operation. For 'update', set
	    # the value to the column name and continue. For 'replace', put the column on the
	    # @unchanged list. For 'insert', ignore this column.
	    
	    if ( $value eq 'UNCHANGED' )
	    {
		if ( $operation eq 'update' )
		{
		    $value = $colname;
		}

		else
		{
		    push @unchanged, $colname if $operation eq 'replace';
		    next COLUMN;
		}
	    }
	}
	
	# For a column that has no special handling directive and a non-empty value, validate the
	# value according to the column attributes.
	
	elsif ( defined $value && $value ne '' )
	{
	    # If the current EditTransaction uses an application module that implements the
	    # 'check_data_column' hook, call that hook. This allows the application module to
	    # check and possibly modify the assigned value before it is validated against the
	    # database schema. If the $no_quote return parameter is true, the returned value is
	    # passed to the database with no further validation except for the foreign key
	    # check. For example, this feature can be used to substitute an SQL expression for the
	    # assigned value.
	    
	    if ( my $hook = $edt->has_hook('check_data_column') )
	    {
		($value, $additional, $no_quote) =
		    &$hook($edt, $cr, $operation, $permission, $value, $fieldname);
	    }
	    
	    # If this column is a foreign key, and the value is neither empty or 0 nor an error
	    # condition, mark the value to be checked against the specified table at execution
	    # time. If the value is an action reference, first check that the reference is valid.
	    
	    if ( my $foreign_table = $cr->{FOREIGN_KEY} )
	    {
		if ( $value && ! ref $value eq 'ARRAY' )
		{
		    if ( $value =~ /^&/ )
		    {
			$no_quote = 1;
			
			my $ref_action = $edt->{action_ref}{$value};
			
			unless ( $ref_action )
			{
			    $value = [ 'E_BAD_REFERENCE', '_unresolved_', $fieldname, $value ];
			}
			
			unless ( $ref_action->table eq $foreign_table )
			{
			    $value = [ 'E_BAD_REFERENCE', '_mismatch_', $fieldname, $value ];
			}
		    }
		    
		    $action->add_precheck('foreign_key', $colname, $fieldname,
					  $foreign_table, $cr->{FOREIGN_COL});
		}
	    }
	    
	    # Otherwise, check the value according to the column type. Validation of column values
	    # is performed by the database interface module through the 'validate_data_column' hook.
	    
	    elsif ( ref $cr->{TypeParams} eq 'ARRAY' and not $no_quote )
	    {
		my $datatype = $cr->{TypeParams}[0];
		
		if ( my $hook = $edt->has_hook('validate_data_column', $datatype) )
		{
		    ($value, $additional, $no_quote) =
			&$hook($edt, $cr, $value, $fieldname);
		}
		
		# If the data type of this column is not recognized by the database module,
		# stringify the value and continue. This might cause problems.
		
		elsif ( defined $value )
		{
		    $value = '' . $value;
		}
	    }
	}
	
	# If the operation is 'update' and there is no assigned value, skip this column entirely.
	
	elsif ( $operation eq 'update' && not $fieldname )
	{
	    next COLUMN;
	}
	
	# Otherwise, a column that is NOT_NULL without a default value must be given a defined value.
	
	elsif ( $cr->{NOT_NULL} && ! defined $value && ! defined $cr->{Default} )
	{
	    # For a special column, this will only happen if a bug occurs in the
	    # 'app_validate_special_column' routine.
	    
	    if ( $directive && $directive ne 'validate' )
	    {
		$edt->add_condition($action, 'E_EXECUTE', "Column '$colname': value cannot be null");
	    }
	    
	    # For a data column add an error condition only if this column does not also have the
	    # REQUIRED property. In that case, an error will be added below.
	    
	    elsif ( ! $cr->{REQUIRED} )
	    {
		$edt->add_condition($action, 'E_REQUIRED', $fieldname, "value cannot be null");
	    }
	}
	
	# Additional error checking
	# -------------------------
	
	# At this point, the column value has been generated. If the value has been changed to an
	# error condition, add that condition and go on to the next column. If $additional is a
	# condition, add it too. This one might be a warning instead of an error.
	
	if ( ref $additional eq 'ARRAY' )
	{
	    unshift @$additional, $action unless $additional->[0] eq 'main';
	    $edt->add_condition(@$additional);
	}
	
	if ( ref $value eq 'ARRAY' )
	{
	    unshift @$value, $action unless $value->[0] eq 'main';
	    $edt->add_condition(@$value);
	    next COLUMN;
	}
	
	# If there was no special handling directive, do some additional checks.
	
	unless ( $directive && $directive ne 'validate' )
	{
	    # If we have an assigned value and this column has the ADMIN_SET property, add an
	    # error condition unless we have administrative privilege or universal privilege.
	    
	    if ( $fieldname && $cr->{ADMIN_SET} && $permission !~ /admin|univ/ )
	    {
		$edt->add_condition($action, 'E_PERM_COL', $fieldname);
	    }
	    
	    # A column that has the REQUIRED property must have a non-empty value.
	    
	    if ( $cr->{REQUIRED} )
	    {
		my $value_ok;
		
		# If the column is also a FOREIGN_KEY, the value must be non-zero. An
		# action reference counts as non-zero, because it will be required to
		# have a non-zero value later when it is resolved.
		
		if ( $cr->{FOREIGN_KEY} )
		{
		    $value_ok = 1 if $value;
		}
		
		# Otherwise, any non-empty value is okay.
		
		elsif ( defined $value && $value ne '' )
		{
		    $value_ok = 1;
		}
		
		# Add an error condition for values that are not okay.
		
		unless ( $value_ok )
		{
		    $edt->add_condition($action, 'E_REQUIRED', $fieldname, "must have a non-empty value")
		}
	    }
	}
	
	# Go on to the next column if any error conditions have been added.
	
	next COLUMN if $action->has_errors;
	
	# If the value is still undefined, skip this column unless it was an assigned value.
	
	next COLUMN unless defined $value || $fieldname;
	
	# Use the value
	# -------------
	
	# If we get here, then we are going to use this value in executing the operation.  All
	# defined values are quoted, unless the $no_quote flag is set. The only exception is a
	# value of 'NULL' for one of the special columns.
	
	if ( defined $value && not ($directive && $value eq 'NULL') )
	{
	    $value = $dbh->quote($value) unless $no_quote;
	}
	
	else
	{
	    $value = 'NULL';
	}
	
	push @column_names, $colname;
	push @column_values, $value;
    }
    
    # Complete the validation
    # -----------------------
    
    # If this is a primary action (not auxiliary) and there are any unrecognized keys in this
    # record, add an error or a warning depending on whether BAD_FIELDS is allowed for this
    # transaction.
    
    if ( ! $action->parent )
    {
	foreach my $key ( keys %$record )
	{
	    unless ( $used{$key} ||
		     $key =~ /^_/ ||
		     %directives && $directives{_FIELD_}{$key} eq 'ignore' )
	    {
		if ( $edt->allows('BAD_FIELDS') )
		{
		    $edt->add_condition($action, 'W_BAD_FIELD', $key, $table);
		}
		
		else
		{
		    $edt->add_condition($action, 'E_BAD_FIELD', $key, $table);
		}
	    }
	}
    }
    
    # If the action has no errors, finish processing the column names and values.
    
    if ( not $action->has_errors )
    {
	# If some columns are to remain unchanged in a 'replace' operation, download their values
	# from the old record. Add the columns and values to the end of the respective lists.
 	
	if ( @unchanged )
	{
	    my $column_string = join ',', @unchanged;
	    
	    my (@old_values) = $edt->get_old_values($table, $action->keyexpr, $column_string);
	
	    foreach my $i ( 0..$#unchanged )
	    {
		my $quoted = defined $old_values[$i] ? $dbh->quote($old_values[$i]) : 'NULL';
		
		push @column_names, $unchanged[$i];
		push @column_values, $quoted;
	    }
	}
	
	# Now store the column list and value hash for subsequent use in constructing SQL statements.
	
	$action->set_column_values(\@column_names, \@column_values);
    }
    
    # Mark the validation as complete.
    
    $action->validation_complete;
}


# validate_special_column ( directive, cr, operation, permission, fieldname, value )
# 
# This method is called once for each of the following column types that occurs in the table
# currently being operated on. The column names will almost certainly be different.
# 
# The parameter $directive must be one of the following:
# 
# ts_created      Records the date and time at which this record was created.
# ts_modified     Records the date and time at which this record was last modified.
# 
# Values for these columns cannot be specified explicitly except by a user with administrative
# permission, and then only if this EditTransaction allows the condition 'ALTER_TRAIL'.
# 
# If this transaction is in FIXUP_MODE, both field values will be left unchanged if the user has
# administrative privilege. Otherwise, a permission error will be returned.
#
# The parameter $cr must contain the column description record.

my %CACHE_MODIFIED_EXPR;

sub validate_special_column {

    my ($edt, $directive, $cr, $operation, $permission, $fieldname, $value) = @_;
    
    # If the value is non-empty, check that it matches the required format.
    
    if ( defined $value && $value ne '' )
    {
	my ($additional, $no_quote);
	
	# The ts fields take datetime values, which are straightforward to validate.
	
	if ( my $hook = $edt->has_dbf_hook('validate_value') )
	{
	    ($value, $additional, $no_quote) =
		$edt->call_hook('validate_value', $cr, $fieldname, $value);
	}
	
	else
	{
	    ($value, $additional, $no_quote) =
		$edt->validate_datetime_value($cr->{Type}, $fieldname, $value);
	    
	    # $edt->add_condition('W_EXECUTE', "no 'validate_value' hook was found");
	}
	
	# If we have admin permission or general permission, add an ALTER_TRAIL caution unless the
	# ALTER_TRAIL allowance is present.
	
	if ( $permission =~ /univ|admin/ )
	{
	    unless ( $edt->{allows}{ALTER_TRAIL} )
	    {
		$additional = $value if ref $value eq 'ARRAY';
		$value = [ 'C_ALTER_TRAIL', $fieldname ];
	    }
	}
	
	# Otherwise, add a permission error. If we already have an error condition related to the
	# value format, bump it into second place.
	
	else
	{
	    $additional = $value if ref $value eq 'ARRAY';
	    $value = [ 'E_PERM_COL', $fieldname ];
	}
	
	return ($value, $additional, $no_quote);
    }
    
    # For ts_modified, an empty value is replaced by the current timestamp unless the transaction
    # is in FIXUP_MODE.
    
    elsif ( $directive eq 'ts_modified' && $edt->{allows}{FIXUP_MODE} )
    {
	if ( $permission =~ /[*]|admin/ )
	{
	    return 'UNCHANGED';
	}
	
	else
	{
	    return [ 'main', 'E_PERM', 'fixup_mode' ];
	}
    }
    
    # Otherwise, fill the necessary value if the column doesn't have have a default and/or update clause.
    
    elsif ( $operation eq 'insert' && $cr->{INSERT_FILL} )
    {
	return ($cr->{INSERT_FILL}, undef, 1);
    }

    elsif ( $operation =~ /^update|^replace/ && $cr->{UPDATE_FILL} )
    {
	return ($cr->{UPDATE_FILL}, undef, 1);
    }
    
    # If no value is necessary, return undefined for everything.
    
    else
    {
	return ();
    }
}


1;

