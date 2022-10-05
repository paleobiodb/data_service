# 
# EditTransaction::Validation - role for validating actions.
# 


package EditTransaction::Validation;

use strict;

use Carp qw(carp croak);
use Permissions;

use Moo::Role;

no warnings 'uninitialized';


# Action validation
# -----------------

# The methods in this section provide default validation for records to be inserted and
# updated. This is done by comparing the field values to the types of the corresponding columns
# from the database schema for the table, plus any attributes specifically specified for the
# column using 'set_column_property' such as 'REQUIRED' and 'ADMIN_SET'.


# validate_action ( action, operation, table, flag )
# 
# Check the field values to be stored in the database against the corresponding table definition,
# and call 'add_condition' to record any error or warning conditions that are detected. The
# column names and corresponding values to be stored are added to the action record using
# 'set_column_values', for later use by the action execution methods.
# 
# If the $flag argument is present, it should have the value 'FINAL' to indicate that we should
# now complete a pending validation.

sub validate_action {

    my ($edt, $action, $operation, $table_specifier, $flag) = @_;
    
    $DB::single = 1 if $edt->{breakpoint}{validation};
    
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
    
    # Grab the table schema, or add an error condition if it is not available. This information is
    # cached, so the database will only need to be asked for this information once per process per
    # table.
    
    my $dbh = $edt->dbh;
    my $tableinfo = $edt->table_info_ref($table_specifier);
    my $columninfo = $edt->table_column_ref($table_specifier);
    
    unless ( ref $tableinfo eq 'HASH' && $tableinfo->%* &&
	     ref $columninfo eq 'HASH' && $columninfo->%* )
    {
	$edt->add_condition($action, 'E_EXECUTE', "an error occurred while fetching the table schema");
	return;
    }
    
    # Figure out which extra methods to call.
    
    my $app_call = $edt->can('check_data_column');
    
    # Iterate through the columns in this table
    # -----------------------------------------
    
    # Iterate through the list of table columns and construct a list of column names and values
    # for insertion, update, or replacement.
    
    my (@column_names, @column_values, %used, @unchanged);
    
  COLUMN:
    foreach my $colname ( $tableinfo->{COLUMN_LIST}->@* )
    {
	# Get the description record for this column.
	
	my $cr = $columninfo->{$colname};
	
	$DB::single = 1 if $edt->{breakpoint}{colname}{$colname};
	
	# The following variables keep track of the value found in the record and the hash key
	# under which it was found, plus some other attributes.
	
	my ($value, $fieldname, $special, $result, $clean_value, $additional, $no_quote);
	
	# Check to see if this column has a handling directive. The default is 'validate'.
	
	my $directive = $action->{directives}{$colname} || 
			$edt->{directives}{$table_specifier}{$colname} || 'validate';
	
	# Determine the assigned value for this column, if any
	# ----------------------------------------------------
	
	# The key column is handled differently from all the others. Its fieldname and value were
	# previously determined, and its value has either already been checked or will be when
	# authentication completes.
	
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
	
	# Determine the stored value for this column
	# ------------------------------------------
	
	# Case 0: If the directive is 'ignore', skip this column entirely. Add a warning
	# if a value was specified for the column.
	
	if ( $directive eq 'ignore' )
	{
	    $edt->add_condition('W_DISCARDED', $fieldname) if defined $value;
	    next COLUMN;
	}
	
	# Case 1: If the directive is either 'pass' or 'unquoted', skip all value checking. Ignore
	# the column if there is no assigned value. If the directive is 'unquoted', set $no_quote.
	
	if ( $directive eq 'pass' || $directive eq 'unquoted' )
	{
	    next COLUMN unless $fieldname;
	    $no_quote = 1 if $directive eq 'unquoted';
	}
	
	# Case 2: If the directive is 'copy', the column value should be unchanged after this
	# operation is complete.
	
	elsif ( $directive eq 'copy' )
	{
	    # Add a warning if the parameters include a value for this column.
	    
	    $edt->add_condition('W_DISCARDED', $fieldname) if defined $value;
	    	    
	    # For 'update', set the column value to itself. This will override any 'on update'
	    # clause in the column definition.
	    
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
	
	# Case 3: If the column has one of the special handling directives, check the value
	# according that directive. This is done even if there is no assigned value, because some
	# special column values (i.e. modification time) are assigned by the special handler.
	
	elsif ( $directive ne 'validate' )
	{
	    $special = $directive;
	    
	    # Validate this column, even if there is no assigned value.
	    
	    my ($result, $clean_value, $additional, $clean_no_quote) =
		$edt->validate_special_column($directive, $cr, $action, $value, $fieldname);
	    
	    # If any conditions were generated, add them to the action. If $result is a condition
	    # then it will be an error, so there is no point in checking this column further.
	    
	    if ( ref $result eq 'ARRAY' || ref $additional eq 'ARRAY' )
	    {
		$DB::single = 1 if $edt->{breakpoint}{valerr};
		$edt->add_validation_condition($action, $result, $additional);
		next COLUMN if ref $result;
	    }
	    
	    # If the result is 'UNCHANGED', handle it the same way as 'copy'.
	    
	    if ( $result eq 'UNCHANGED' )
	    {
		if ( $operation eq 'update' )
		{
		    $value = $colname;
		    $no_quote = 1;
		}
		
		else
		{
		    push @unchanged, $colname if $operation eq 'replace';
		    next COLUMN;
		}
	    }
	    
	    # If a new value is returned, substitute it for the original one.
	    
	    elsif ( $result )
	    {
		$value = $clean_value;
		$no_quote = $clean_no_quote;
	    }
	}
	
	# Case 4: The default directive is 'validate'. For any column that has a
	# non-null assigned value, check that value against the column properties.
	
	elsif ( defined $value )
	{
	    # If this column has the ADMIN_SET property, add an error condition unless we have
	    # administrative privilege or universal privilege.
	    
	    if ( $fieldname && $cr->{ADMIN_SET} && $permission !~ /admin|univ/ )
	    {
		$edt->add_condition($action, 'E_PERM_COL', $fieldname);
	    }
	    
	    # If the current EditTransaction includes an application role that implements the
	    # 'check_data_column' method, call that method. This allows the role to check
	    # and possibly modify the assigned value before it is validated against the database
	    # schema. If the $no_quote return parameter is true, the returned value is passed to
	    # the database with no further validation except for the foreign key check. For
	    # example, this feature can be used to substitute an SQL expression for the assigned
	    # value.
	    
	    if ( $app_call )
	    {
		my ($result, $clean_value, $additional, $clean_no_quote) =
		    $edt->check_data_column($cr, $action, $value, $fieldname);
		
		# If any conditions were generated, add them to the action. If $result is a
		# condition then it will be an error, so there is no point in checking this column
		# further.
		
		if ( ref $result eq 'ARRAY' || ref $additional eq 'ARRAY' )
		{
		    $edt->add_validation_condition($action, $result, $additional);
		    next COLUMN if ref $result;
		}
		
		# If a new value was returned, substitute it for the original.
		
		if ( $result )
		{
		    $value = $clean_value;
		    $no_quote = $clean_no_quote;
		}
	    }
	    
	    # If this column is a foreign key, validation cannot be completed until execution time.
	    
	    if ( my $foreign_table = $cr->{FOREIGN_KEY} )
	    {
		# An assigned value that is not empty or zero will be checked against the
		# specified table at execution time. If at that time it does not correspond to any
		# in the foreign table, an error condition will be added.
		
		if ( $value )
		{
		    # If the value is an action reference, check to make sure that the reference
		    # is resolvable and that the referenced action uses the proper table.
		    
		    if ( $value =~ /^&/ )
		    {
			$no_quote = 1;
			
			my $ref_action = $edt->{action_ref}{$value};
			
			unless ( $ref_action )
			{
			    $edt->add_condition($action, 'E_BAD_REFERENCE', '_unresolved_', $fieldname, $value);
			}
			
			unless ( $ref_action->table eq $foreign_table )
			{
			    $edt->add_condition($action, 'E_BAD_REFERENCE', '_mismatch_', $fieldname, $value);
			}
		    }
		    
		    # Add a pre-execution check for this column, and go on to the next.
		    
		    $action->add_precheck($colname, $fieldname, 'foreign_key',
					  $foreign_table, $cr->{FOREIGN_COL});
		    next COLUMN;
		}
		
		# If the assigned value is empty or zero and the column has the REQUIRED
		# attribute, add an error condition.
		
		elsif ( $cr->{REQUIRED} )
		{
		    $edt->add_condition($action, 'E_REQUIRED', "this column requires a non-empty value");
		    next COLUMN;
		}
	    }
	    
	    # For all columns other than foreign keys, if $no_quote has not been set then check the
	    # value according to the column type. A true value for $no_quote means that the value
	    # was already changed to something other than a literal.
	    
	    elsif ( defined $value && ! $no_quote )
	    {
		my ($result, $clean_value, $additional, $clean_no_quote) =
		    $edt->validate_data_column($cr, $value, $fieldname);
		
		# If any conditions were generated, add them to the action. If $result is a
		# condition then it will be an error, so there is no point in checking this column
		# further.
		
		if ( ref $result eq 'ARRAY' || ref $additional eq 'ARRAY' )
		{
		    $edt->add_validation_condition($action, $result, $additional);
		    next COLUMN if ref $result;
		}
		
		# If a cleaned value was returned, substitute it for the value that we had so far.
		
		if ( $result )
		{
		    $value = $clean_value;
		    $no_quote = $clean_no_quote;
		}
	    }
	    
	    # If the column has the REQUIRED property and the value is empty, add an error
	    # condition. A zero value is okay here, whereas it is not for a foreign key.
	    
	    if ( $cr->{REQUIRED} && defined $value && $value eq '' )
	    {
		$edt->add_condition($action, 'E_REQUIRED', "this column requires a non-empty value");
		next COLUMN;
	    }
	}
	
	# Case 5: If there is no assigned value and the operation is 'update', skip this column
	# entirely.
	
	elsif ( $operation eq 'update' && not $fieldname )
	{
	    next COLUMN;
	}
	
	# Handle null values
	# ------------------
	
	unless ( defined $value )
	{
	    # If the column is NOT_NULL or REQUIRED, add an error condition.
	    
	    if ( $cr->{NOT_NULL} || $cr->{REQUIRED} )
	    {
		# For a special column, this will only happen if a bug occurs in the
		# 'validate_special_column' routine. So add an E_EXECUTE in this case.
		
		if ( $special )
		{
		    $edt->add_condition($action, 'E_EXECUTE', "Column '$colname': value cannot be null");
		}
		
		# Otherwise, add an E_REQUIRED. If the column also has the REQUIRED property, tell the
		# client that the value must be non-empty instead of just non-null. If this column
		# doesn't have an assigned value, use the column name because we don't have a field name.
		
		else
		{
		    my $message = $cr->{REQUIRED} ? "this column requires a non-empty value"
			: "this column requires a non-null value";
		    
		    my $field = $fieldname || $colname;
		    
		    $edt->add_condition($action, 'E_REQUIRED', $field, $message);
		}
	    }
	    
	    # Otherwise, skip the column entirely unless the null value is specifically assigned.
	    
	    elsif ( ! $fieldname )
	    {
		next COLUMN;
	    }
	}
	
	# Handle error conditions
	# -----------------------
	
	# Go on to the next column if any error conditions have been added, either to this
	# column or any previous ones. The action will never be executed, so there is no
	# point in continuing further. We still want to keep processing the rest of the
	# columns, so the client will get an accurate report of any other validation
	# errors that may occur. The 'PROCEED' flag is for testing purposes only.
	
	next COLUMN if $action->has_errors && $vstatus ne 'PROCEED';
	
	# Use the value
	# -------------
	
	# If we get here, then we are going to use this value in executing the operation.  All
	# defined values are quoted, unless the $no_quote flag is set.
	
	if ( defined $value )
	{
	    $value = $dbh->quote($value) unless $no_quote;
	}
	
	# Undefined values are translated into NULL.
	
	else
	{
	    $value = 'NULL';
	}
	
	# Column names are quoted unless they consist entirely of word characters.
	
	$colname = $dbh->quote_identifier($colname) if $colname =~ /[^\w]/;
	
	# Store the column names and values in parallel lists.
	
	push @column_names, $colname;
	push @column_values, $value;
    }
    
    # Complete the validation
    # -----------------------
    
    # If there are any unrecognized keys in this record, add an error or a warning depending on
    # whether BAD_FIELDS is allowed for this transaction.
    
    foreach my $key ( keys %$record )
    {
	# Ignore any key whose name starts with an underscore, and also those
	# which have been marked using handle_column.
	
	unless ( $used{$key} || $key =~ /^_/ )
	{
	    # If we have been told to ignore this field, skip it. The //
	    # operator is used because a 'USE_FIELD' on the action should
	    # override an 'IGNORE_FIELD' on the transaction.
	    
	    if ( $action->{directives}{"FIELD:$key"} eq 'ignore' || 
		 $edt->{directives}{"FIELD:$key"} eq 'ignore' )
	    {
		next;
	    }
	    
	    # Otherwise, add an error or warning condition according to whether
	    # or not the 'BAD_FIELDS' allowance was specified.
	    
	    elsif ( $edt->allows('BAD_FIELDS') )
	    {
		$edt->add_condition($action, 'W_BAD_FIELD', $key, $table_specifier);
	    }
	    
	    else
	    {
		$edt->add_condition($action, 'E_BAD_FIELD', $key, $table_specifier);
	    }
	}
    }
    
    # If the action has no errors, finish processing the column names and values. The 
    # second clause is there for testing purposes.
    
    if ( ! $action->has_errors || $vstatus eq 'PROCEED' )
    {
	# If some columns are to remain unchanged in a 'replace' operation, download their values
	# from the old record. Add the columns and values to the end of the respective lists.
 	
	if ( @unchanged )
	{
	    my $column_string = join ',', @unchanged;
	    
	    my (@old_values) = $edt->get_old_values($table_specifier, $action->keyexpr, $column_string);
	    
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


# validate_special_column ( directive, cr, action, value, fieldname )
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

my $SQL_CURRENT_TIMESTAMP;

sub validate_special_column {

    my ($edt, $directive, $cr, $action, $value, $fieldname) = @_;
    
    my $reportname = $fieldname || $cr->{Field};
    
    $SQL_CURRENT_TIMESTAMP ||= $edt->sql_current_timestamp;
    
    # If the directive is one we know about, handle it.
    
    if ( $directive eq 'ts_created' || $directive eq 'ts_modified' )
    {
	# If the column type is not 'datetime', return an error condition.
	
	unless ( $cr->{TypeMain} eq 'datetime' )
	{
	    $edt->error_line("Error: table '$action->table', column '$reportname':");
	    $edt->error_line("    directive '$directive' requires a column type of " .
			     "date, time, datetime, or timestamp");
	    
	    return ['E_BAD_DIRECTIVE', $reportname, "handling directive does not match column type"];
	}
	
	# If the value is non-empty, check that it matches the required format.
	
	my $operation = $action->operation;
	my $permission = $action->permission;
	
	if ( defined $value && $value ne '' )
	{
	    # The ts fields take datetime values, which are straightforward to validate.
	    
	    my ($result, $clean_value, $additional, $no_quote) =
		$edt->validate_datetime_value($cr->{TypeParams}, $value, $fieldname);
	    
	    # If we have admin permission or general permission, add an
	    # ALTER_TRAIL caution unless the ALTER_TRAIL allowance is present.
	    # If we already have an error condition related to the value format,
	    # bump it into second place.
	    
	    if ( $permission =~ /^admin|^unrestricted/ )
	    {
		unless ( $edt->{allows}{ALTER_TRAIL} )
		{
		    $additional = $result if ref $result eq 'ARRAY';
		    $result = [ 'C_ALTER_TRAIL', $fieldname ];
		}
	    }
	    
	    # Otherwise, add a permission error. 
	    
	    else
	    {
		$additional = $result if ref $result eq 'ARRAY';
		$result = [ 'E_PERM_COL', $fieldname ];
	    }
	    
	    # If we have something to return, then return it now.
	    
	    if ( $result )
	    {
		return ($result, $clean_value, $additional, $no_quote);
	    }
	}
	
	# For ts_modified, the column value will remain unchanged if the
	# transaction is executing in FIXUP_MODE, provided we have the necessary
	# permission.
    
	elsif ( $directive eq 'ts_modified' && $edt->{allows}{FIXUP_MODE} )
	{
	    if ( $permission =~ /^admin|^unrestricted/ )
	    {
		return 'UNCHANGED';
	    }
	    
	    else
	    {
		return [ 'main', 'E_PERM', 'fixup_mode' ];
	    }
	}
	
	# Otherwise, fill the necessary value if the column doesn't have have a
	# default and/or update clause.
	
	elsif ( $operation eq 'insert' && $cr->{Default} !~ /current_timestamp/i )
	{
	    return (1, 'NOW()', undef, 1);
	}
	
	elsif ( $directive eq 'ts_modified' && $operation =~ /^update|^replace/
		&& $cr->{Extra} !~ /on update/i )
	{
	    return (1, 'NOW()', undef, 1);
	}
	
	return;
	
	# If the value is valid as-is, return the empty list.
    }
    
    # If the directive is not one we know about, return an error condition.
    
    else
    {
	return ['E_BAD_DIRECTIVE', $reportname, "no handler for directive '$directive'"];
    }
}


# add_validation_condition ( action, error, additional )
#
# If $error is a listref, add it to the action as an error condition. If $additional is a listref, add
# it too. The latter might be either a second error or a warning.

sub add_validation_condition {

    my ($edt, $action, $error, $additional) = @_;
    
    if ( ref $error eq 'ARRAY' )
    {
	unshift @$error, $action unless $error->[0] eq 'main';
	$edt->add_condition(@$error);
    }
    
    if ( ref $additional eq 'ARRAY' )
    {
	unshift @$additional, $action unless $additional->[0] eq 'main';
	$edt->add_condition(@$additional);
    }
}


1;
