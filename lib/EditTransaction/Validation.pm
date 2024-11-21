# 
# EditTransaction::Validation - role for validating actions.
# 


package EditTransaction::Validation;

use strict;

use Encode qw(encode);
use Carp qw(carp croak);
use Permissions;

use Role::Tiny;

no warnings 'uninitialized';


# Action validation
# -----------------

# The methods in this section provide default validation for records to be inserted and
# updated. This is done by comparing the field values to the types of the corresponding columns
# from the database schema for the table, plus any attributes specifically specified for the
# column using 'set_column_property' such as 'REQUIRED' and 'ADMIN_SET'.


# validate_action ( action, operation, table )
# 
# This method is designed to be overridden by subclasses. By default, it does
# nothing. 

sub validate_action {
    
}


# validate_against_schema ( action )
# 
# Check the field values to be stored in the database against the corresponding table definition,
# and call 'add_condition' to record any error or warning conditions that are detected. The
# column names and corresponding values to be stored are added to the action record using
# 'set_column_values', for later use by the action execution methods.
# 
# If the $flag argument is present, it should have the value 'FINAL' to indicate that we should
# now complete a pending validation.

sub validate_against_schema {

    my ($edt, $action, $operation, $table_specifier) = @_;
    
    $operation ||= $action->operation;
    $table_specifier ||= $action->table;
    
    $DB::single = 1 if $edt->{breakpoint}{validation};
    
    # If the validation status is 'COMPLETE', then return immediately without doing anything.
    
    my $vstatus = $action->validation_status;
    
    return if $vstatus eq 'COMPLETE';
    
    # # If the status is 'PENDING', then return unless this method was called with the 'FINAL'
    # # flag. In this case, complete the validation process and set the status to reflect it.
    
    # unless ( $flag && $flag eq 'FINAL' )
    # {
    # 	return if $vstatus eq 'PENDING';
    # }
    
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
	unless ( $action->{error_count} )
	{
	    $edt->add_condition($action, 'E_EXECUTE', 
				"an error occurred while fetching the table schema");
	}
	
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
	    
	    # If the operation is 'insert' or 'replace', use the already determined key
	    # value and set the directive to 'pass' to disable any further checking.
	    
	    if ( $operation eq 'insert' || $operation eq 'replace' )
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
	
	# Case 3: If the assigned value is not null, or if the column has a special
	# handling directive, call the appropriate routine to validate it. Columns with
	# special handling directives are called even with null values because in some
	# cases they must provide a value.
	
	elsif ( defined $value || $directive ne 'validate' )
	{
	    # The column validation routines should return one of the following
	    # results:
	    # 
	    # clean             Substitute the new value for the assigned one
	    # 
	    # unquoted          Substitute the new value and set the $no_quote flag
	    # 
	    # unchanged         The new value will be whatever the row currently has
	    # 
	    # ignore            Skip this column entirely (typically when an error
	    #                   condition was generated)
	    # 
	    # pass              Use the assigned value as is.	    
	    
	    my ($result, $new_value);
	    
	    if ( $directive eq 'validate' )
	    {
		($result, $new_value) = $edt->validate_data_column($action, $colname, $cr, 
								   'validate', $value, $fieldname);
	    }
	    
	    else
	    {
		$special = $directive;
	    
		($result, $new_value) = $edt->validate_special_column($action, $colname, $cr, 
								      $directive, $value, $fieldname);
	    }
	    
	    # If the resulting directive is either 'clean' or 'unquoted', the new value is
	    # substituted for the old one. Otherwise, the existing value is left
	    # unchanged.
	    
	    if ( $result eq 'clean' )
	    {
		$value = $new_value;
	    }
	    
	    elsif ( $result eq 'unquoted' )
	    {
		$value = $new_value;
		$no_quote = 1;
	    }
	    
	    # If the result is 'unchanged', handle it the same way as 'copy'.
	    
	    elsif ( $result eq 'unchanged' )
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
	    
	    # If the result is 'ignore', then immediately go on to the next column. This
	    # will typically be returned when an error condition has been added.
	    
	    elsif ( $result eq 'ignore' )
	    {
		next COLUMN;
	    }
	    
	    # Otherwise, the result should be 'pass'. Keep the assigned value.
	}
	
	# Case 4: If there is no assigned value and the operation is 'update', skip this column
	# entirely.
	
	elsif ( $operation eq 'update' && not $fieldname )
	{
	    next COLUMN;
	}
	
	# Check for required values
	# -------------------------
	
	# For an 'update' operation, the requirement is only checked if the
	# column is given a new value.
	
	if ( $cr->{REQUIRED} && ($operation !~ /^update/ || $fieldname) )
	{
	    if ( ! defined $value || $value eq '' )
	    {
		$edt->add_condition('E_REQUIRED', $fieldname || $colname);
	    }
	}
	
	elsif ( $cr->{NOT_NULL} && ! defined $value )
	{
	    if ( $operation !~ /^update/ || $fieldname )
	    {
		# For a special column, this will only happen if a bug occurs in the
		# 'validate_special_column' routine. So add an E_EXECUTE in this case.
		
		if ( $special )
		{
		    $edt->add_condition($action, 'E_EXECUTE', $fieldname || $colname,
					"special value cannot be null");
		}
		
		else
		{
		    $edt->add_condition($action, 'E_REQUIRED', $fieldname || $colname);
		}
	    }
	}
	
	# Any column that was not mentioned in the operation record and does not
	# have a value and is not marked as required or not null is skipped.
	
	elsif ( ! $fieldname && ! defined $value )
	{
	    next COLUMN;
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
    
    unless ( $action->{ignore_bad_fields} )
    {
	foreach my $key ( keys %$record )
	{
	    # Ignore any key which was already validated (%used), any key whose
	    # name starts with an underscore, and also ignore the primary key
	    # for this table if it has an empty value.  The latter may occur
	    # with an insert operation.
	    
	    unless ( $used{$key} || $key =~ /^_/ )
	    {
		if ( (!defined $record->{$key} || $record->{$key} eq '') &&
		     ($key eq $tableinfo->{PRIMARY_KEY} || 
		      $key eq $tableinfo->{PRIMARY_FIELD}) )
		{
		    next;
		}
		
		# If we have been directed to ignore this field with
		# 'ignore_field', skip it.
		
		elsif ( $action->{directives}{"FIELD:$key"} eq 'ignore' || 
			$edt->{directives}{$table_specifier}{"FIELD:$key"} eq 'ignore' )
		{
		    next;
		}
		
		# Otherwise, add an error or warning condition according to whether
		# or not the 'BAD_FIELDS' allowance was specified.
		
		if ( $edt->allows('BAD_FIELDS') )
		{
		    $edt->add_condition($action, 'W_BAD_FIELD', $key, $table_specifier);
		}
		
		else
		{
		    $edt->add_condition($action, 'E_BAD_FIELD', $key, $table_specifier);
		}
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


# validate_data_column ( action, colname, cr, directive, value, fieldname )
# 
# Validate the specified value according to the directive and the column properties given
# in the column info record ($cr). Subclasses and application modules may add 'before'
# subroutines that can modify the value and/or directive and can add error conditions.

sub validate_data_column {
    
    my ($edt, $action, $colname, $cr, $directive, $value, $fieldname) = @_;
    
    # If the directive is anything other than 'validate', return immediately. This means
    # the disposition has already been determined by a 'before' subroutine.
    
    if ( $directive ne 'validate' )
    {
	return ($directive, $value);
    }
    
    # If this column has the ADMIN_SET property, add an error condition unless we have
    # administrative privilege or unrestricted privilege.
    
    if ( $fieldname && $cr->{ADMIN_SET} )
    {
	if ( $action->permission !~ /^admin|^unrestricted/ )
	{
	    $edt->add_condition($action, 'E_PERMISSION_COLUMN', $fieldname);
	    return 'ignore';
	}
    }
    
    # If this column is a foreign key, validation cannot be completed until execution time.
    
    if ( my $foreign_table = $cr->{FOREIGN_KEY} )
    {
	# An assigned value that is not empty or zero will be checked against the
	# specified table at execution time. If at that time it does not correspond to any
	# in the foreign table, an error condition will be added. If the value is an
	# action reference, check to make sure that the reference is resolvable and that
	# the referenced action uses the proper table.
	
	if ( $value =~ /^&/ )
	{
	    my $ref_action = $edt->{action_ref}{$value};
	    
	    unless ( $ref_action )
	    {
		$edt->add_condition($action, 'E_BAD_REFERENCE', '_unresolved_', $fieldname, $value);
	    }
	    
	    unless ( $ref_action->table eq $foreign_table )
	    {
		$edt->add_condition($action, 'E_BAD_REFERENCE', '_mismatch_', $fieldname, $value);
	    }
	    
	    $action->add_precheck($colname, $fieldname, 'foreign_key',
				  $foreign_table, $cr->{FOREIGN_COL});
	    
	    return 'ignore';
	}
	
	# If the value is not an action reference and is not empty, add a pre-check.
	# Otherwise, add an error condition if the column has the REQUIRED attribute.
	
	if ( $value )
	{
	    $action->add_precheck($colname, $fieldname, 'foreign_key',
				  $foreign_table, $cr->{FOREIGN_COL});
	}
	
	else
	{
	    $edt->add_condition($action, 'E_REQUIRED', $fieldname) if $cr->{REQUIRED};
	    return 'ignore';
	}
    }
    
    # Validate the value we were given against the column attributes.
    
    my ($result, $new_value, $additional) = 
	$edt->validate_data_value($cr, $value, $fieldname);
    
    # If any conditions were generated, add them to the action. If $result is a
    # condition then it will be an error, so there is no point in checking this column
    # further.
    
    if ( ref $additional eq 'ARRAY' )
    {
	# unshift @$additional, $action unless $additional->[0] eq 'main';
	$edt->add_condition(@$additional);
    }
    
    if ( ref $result eq 'ARRAY' )
    {
	# unshift @$result, $action unless $result->[0] eq 'main';
	$edt->add_condition(@$result);
	return 'ignore';
    }
    
    # If the column has the REQUIRED property and the value is empty, add an error
    # condition. A zero value is okay here, whereas it is not for a foreign key.
	    
    if ( $cr->{REQUIRED} && defined $value && $value eq '' )
    {
	$edt->add_condition($action, 'E_REQUIRED', $fieldname);
	return 'ignore';
    }
    
    # Otherwise, return the result and the cleaned value if one was returned.
    
    return ($result, $new_value);
}


# validate_special_column ( action, colname, cr, directive, value, fieldname )
# 
# This method is called once for each of the following column types that occurs in the table
# currently being operated on. The column names will almost certainly be different.
# 
# The parameter $directive must be one of the following:
# 
# ts_created      This column stores the date and time at which the row was created.
# ts_modified     This column stores the date and time at which the row was last modified.
# row_lock        If set, the row is locked and cannot be modified without the 'LOCKED' allowance.
# 
# Values for these columns cannot be specified explicitly except by a user with administrative
# permission, and then only if this EditTransaction allows the condition 'ALTER_TRAIL'.
# 
# If this transaction is in FIXUP_MODE, both field values will be left unchanged if the user has
# administrative privilege. Otherwise, a permission error will be returned.
#
# The parameter $cr must contain the column description record.

our (%SQL_CURRENT_TIMESTAMP);

sub validate_special_column {

    my ($edt, $action, $colname, $cr, $directive, $value, $fieldname) = @_;
    
    # If it is one of the timestamp special handling directives, handle it now.
    
    if ( $directive eq 'ts_created' || $directive eq 'ts_modified' )
    {
	# If the column type is not 'datetime', return an error condition.
	
	unless ( $cr->{TypeMain} eq 'datetime' )
	{
	    my $reportname = $fieldname || $colname;
	    
	    $edt->error_line("Error: table '$action->table', column '$reportname':");
	    $edt->error_line("    directive '$directive' requires a column type of " .
			     "date, time, datetime, or timestamp");
	    
	    $edt->add_condition('main', 'E_BAD_DIRECTIVE', $reportname,
				"handling directive does not match column type");
	    return 'ignore';
	}
	
	# If the value is non-empty, check that it matches the required format.
	
	#my $operation = $action->operation;
	my $permission = $action->permission;
	
	if ( defined $value && $value ne '' )
	{
	    # The ts fields take datetime values, which are straightforward to validate.
	    
	    my ($result, $new_value) =
		$edt->validate_datetime_value($cr->{TypeParams}, $value, $fieldname);
	    
	    if ( ref $result eq 'ARRAY' )
	    {
		$edt->add_condition($action, $result->@*);
	    }
	    
	    # If we have admin permission or general permission, return the result from
	    # 'validate_datetime_value'. But add an ALTER_TRAIL caution unless the
	    # ALTER_TRAIL allowance is present.
	    
	    if ( $permission =~ /^admin|^unrestricted/ )
	    {
		unless ( $edt->{allows}{ALTER_TRAIL} )
		{
		    $edt->add_condition('main', 'C_ALTER_TRAIL', $fieldname);
		}
		
		return ($result, $new_value);
	    }
	    
	    # Otherwise, add a permission error. 
	    
	    else
	    {
		$edt->add_condition($action, 'E_PERMISSION_COLUMN', $fieldname);
		return 'ignore';
	    }
	}
	
	# For ts_modified, the column value will remain unchanged if the
	# transaction is executing in FIXUP_MODE, provided we have the necessary
	# permission.
	
	elsif ( $directive eq 'ts_modified' && $edt->{allows}{FIXUP_MODE} )
	{
	    if ( $permission =~ /^admin|^unrestricted/ )
	    {
		return 'unchanged';
	    }
	    
	    else
	    {
		$edt->add_condition('main', 'E_PERM', 'fixup_mode', $action->table);
		return 'ignore';
	    }
	}
	
	# Otherwise, we may need to fill in some values. Compute the TS_FILL_INIT and
	# TS_FILL_MOD  attributes unless they already exist.
	
	if ( ! exists $cr->{TS_FILL_INIT} )
	{
	    $cr->{TS_FILL_INIT} = $cr->{Default} !~ /current_timestamp/i;
	    $cr->{TS_FILL_MOD} = $cr->{Extra} !~ /on update/i;
	}
	
	if ( $cr->{TS_FILL_INIT} && $action->operation eq 'insert' )
	{
	    my $sql = $SQL_CURRENT_TIMESTAMP{ref $edt} ||= $edt->sql_current_timestamp;
	    return ('unquoted', $sql);
	}
	
	elsif ( $cr->{TS_FILL_MOD} && $directive eq 'ts_modified' &&
		$action->operation =~ /^update|^replace/ )
	{
	    my $sql = $SQL_CURRENT_TIMESTAMP{ref $edt} ||= $edt->sql_current_timestamp;
	    return ('unquoted', $sql);
	}
	
	# Otherwise, return nothing.
	
	else
	{
	    return;
	}
    }
    
    elsif ( $directive eq 'row_lock' )
    {
	# Return immediately unless the value is non-empty.
	
	return unless defined $value && $value ne '';
	
	# If the column type is not 'boolean' or 'integer', return an error condition.
	
	unless ( $cr->{TypeMain} eq 'boolean' || $cr->{TypeMain} eq 'integer' )
	{
	    my $reportname = $fieldname || $colname;
	    
	    $edt->error_line("Error: table '$action->table', column '$reportname':");
	    $edt->error_line("    directive '$directive' requires a column type of " .
			     "boolean or integer");
	    
	    $edt->add_condition($action, 'E_BAD_DIRECTIVE', $reportname,
				"handling directive does not match column type");
	    return 'ignore';
	}
	
	# If the value is non-empty, check that it matches the required format.
	
	my ($result, $new_value) = 
	    $edt->validate_boolean_value($cr->{TypeParams}, $value, $fieldname);
	
	# Unless we have either administrative or unrestricted permission, add a
	# permission error.
	
	if ( $action->permission =~ /^admin|^unrestricted/ )
	{
	    return ($result, $new_value);
	}
	
	elsif ( $value )
	{
	    $edt->add_condition($action, 'E_PERMISSION', 'lock');
	    return 'ignore';
	}
	
	else
	{
	    $edt->add_condition($action, 'E_PERMISSION', 'unlock') unless
		$action->operation eq 'insert';
	    return 'ignore';
	}
    }
    
    # If the disposition of the value has already been decided by a 'before' subroutine,
    # return it now.
    
    elsif ( $directive =~ /^pass|^ignore|^clean|^noquote|^unchanged/ )
    {
	return ($directive, $value);
    }
    
    # If the directive is not one we know about, return an error condition.
    
    else
    {
	$edt->add_condition('main', 'E_BAD_DIRECTIVE', $fieldname|| $colname,
			    "no handler for directive '$directive'");
    }
}


# Methods for validating column values
# ------------------------------------

our $DECIMAL_NUMBER_RE = qr{ ^ \s* ( [+-]? ) \s* (?: ( \d+ ) (?: [.] ( \d* ) )? | [.] ( \d+ ) ) \s*
			     (?: [Ee] \s* ( [+-]? ) \s* ( \d+ ) )? \s* $ }xs;


# The routines in this section all use the same return convention. The result will either be the
# empty list, or it will be some or all of the following:
# 
#     (result, clean_value, additional, clean_no_quote)
# 
# A. If the specified value is valid and no warnings were generated, the empty list is returned.
# 
# B. If specified value is invalid, the first return value will be a listref containing an
#    error condition code and parameters. If any additional error or warning was
#    generated, it will appear as the third return value. The second value will be
#    undefined and should be ignored.
# 
# C. If a replacement value is generated (i.e. a truncated character string), a 2-4
#    element list will be returned. The first element will be '1', and the second element
#    will be the clean (replacement) value. The third element, if present, will be a
#    warning condition. The fourth element, if present, indicates that the returned value
#    is not an SQL literal and should not be quoted.
# 
# D. If the column value should remain unchanged despite any 'on update' clause, the
#    single value 'unchanged' is returned.


# validate_column ( cr, value, fieldname )
# 
# Check the specified value to make sure it matches the column properties given by $cr. If it does
# not, return an error condition.

sub validate_data_value {

    my ($edt, $cr, $value, $fieldname) = @_;
    
    my ($maintype) = $cr->{TypeMain};
    
    if ( $maintype eq 'char' )
    {
	return $edt->validate_char_value($cr->{TypeParams}, $value, $fieldname, $cr->{ALLOW_TRUNCATE});
    }
    
    elsif ( $maintype eq 'boolean' )
    {
	return $edt->validate_boolean_value($cr->{TypeParams}, $value, $fieldname);
    }
    
    elsif ( $maintype eq 'integer' || $maintype eq 'unsigned' )
    {
	return $edt->validate_integer_value($cr->{TypeParams}, $value, $fieldname);
    }
    
    elsif ( $maintype eq 'fixed' )
    {
	return $edt->validate_fixed_value($cr->{TypeParams}, $value, $fieldname, $cr->{ALLOW_TRUNCATE});
    }
    
    elsif ( $maintype eq 'floating' )
    {
	return $edt->validate_float_value($cr->{TypeParams}, $value, $fieldname);
    }
    
    elsif ( $maintype eq 'enum' )
    {
	return $edt->validate_enum_value($cr->{TypeParams}, $value, $fieldname);
    }
    
    elsif ( $maintype eq 'datetime' )
    {
	return $edt->validate_datetime_value($cr->{TypeParams}, $value, $fieldname);
    }
    
    elsif ( $maintype eq 'geometry' )
    {
	return $edt->validate_geometry_value($cr->{TypeParams}, $value, $fieldname);
    }
    
    # If the data type is anything else, stringify the value and go with it. This
    # might cause problems in occasional situations.
    
    else
    {
	return ('clean', "$value");
    }
}


# validate_char_value ( type, value, fieldname, can_truncate )
# 
# Check that the specified value is suitable for storing into a boolean column in the
# database. If it is not, return an error condition as a listref.
# 
# If the value is good, return a canonical version suitable for storing into the column. An
# undefined return value will indicate a null. The second return value, if present, will be a
# warning condition as a listref. The third return value, if present, will indicate that the
# returned value has already been quoted.

sub validate_char_value {
    
    my ($edt, $type, $value, $fieldname, $can_truncate) = @_;
    
    my ($subtype, $size, $var, $charset) = ref $type eq 'ARRAY' ? $type->@* : $type || '';
    
    my ($value_size, $truncated, $dbh, $quoted);
    
    # If the character set of a text/char column is not utf8, then encode the value into the
    # proper character set before checking the length.
    
    if ( $subtype eq 'text' && $charset && $charset ne 'utf8' )
    {
	# If the column is latin1, we can do the conversion in Perl.
	
	if ( $charset eq 'latin1' )
	{
	    $value = encode('cp1252', $value);
	    $value_size = length($value);
	}
	
	# Otherwise, we must let the database do the conversion.
	
	else
	{
	    $dbh = $edt->dbh;
	    $quoted = $dbh->quote($value);
	    ($value_size) = $dbh->selectrow_array("SELECT length(convert($quoted using $charset))");
	}
    }
    
    else
    {
	$value_size = length(encode('UTF-8', $value));
    }
    
    # If the size of the value exceeds the size of the column, then we either truncate the data if
    # allowed or else reject the value.
    
    if ( $size && $value_size > $size )
    {
	my $word = $subtype eq 'text' ? 'characters' : 'bytes';
	
	# If we can truncate the value, then do so. If the character set is neither utf8 nor
	# latin1, have the database do it.
	
	if ( $can_truncate )
	{
	    if ( $quoted )
	    {
		($truncated) = $dbh->selectrow_array("SELECT left(convert($quoted using $charset)), $size)");
	    }
	    
	    else
	    {
		$truncated = substr($value, 0, $size);
	    }
	    
	    return ('clean', $truncated, [ 'W_TRUNC', $fieldname, "value was truncated to $size $word" ]);
	}
	
	else
	{
	    return [ 'E_WIDTH', $fieldname, "value exceeds column size of $size $word (was $value_size)" ];
	}
    }
    
    # If the value is valid as-is, return the empty list.
    
    return;
}


# validate_boolean_value ( type, value, fieldname )
# 
# Check that the specified value is suitable for storing into a boolean column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_boolean_value {
    
    my ($edt, $type, $value, $fieldname) = @_;
    
    # For a boolean column, the value must be either 1 or 0. But we allow 'yes', 'no', 'true',
    # 'false', 'on', 'off' as case-insensitive synonyms. A string that is empty or has only
    # whitespace is turned into a null.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return (1, undef);
    }
    
    elsif ( $value =~ qr{ ^ \s* (?: ( 1 | true | yes | on ) | 
				    ( 0 | false | no | off ) ) \s* $ }xsi )
    {
	my $clean_value = $1 ? '1' : '0';
	return ('clean', $clean_value);
    }
    
    else
    {
	return [ 'E_FORMAT', $fieldname, "value must be one of: 1, 0, true, false, yes, no, on, off" ];
    }
}


# validate_integer_value ( type, value, fieldname )
# 
# Check that the specified value is suitable for storing into an integer column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

our (%SIGNED_BOUND) = ( tiny => 127,
			small => 32767,
			medium => 8388607,
			regular => 2147483647,
			big => 9223372036854775807 );

our (%UNSIGNED_BOUND) = ( tiny => 255,
			  small => 65535,
			  medium => 16777215,
			  regular => 4294967295,
			  big => 18446744073709551615 );

sub validate_integer_value {
    
    my ($edt, $type, $value, $fieldname) = @_;
    
    my ($unsigned, $size) = ref $type eq 'ARRAY' ? $type->@* : $type;
    
    my $max = $type eq 'unsigned' ? $UNSIGNED_BOUND{$size} : $SIGNED_BOUND{$size};
    
    # First make sure that the value is either empty or matches the proper format. A value which
    # is empty or contains only whitespace will be treated as a NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return ('clean', undef);
    }
    
    elsif ( $value !~ qr{ ^ \s* ( [-+]? ) \s* ( \d+ ) \s* $ }xs )
    {
	my $phrase = $type eq 'unsigned' ? 'an unsigned' : 'an';
	
	return [ 'E_FORMAT', $fieldname, "value must be $phrase integer" ];
    }
    
    elsif ( $type eq 'unsigned' )
    {
	$value = $2;
	
	if ( $1 && $1 eq '-' )
	{
	    return [ 'E_RANGE', $fieldname, "value must an unsigned integer" ];
	}
	
	elsif ( defined $max && $value > $max )
	{
	    return [ 'E_RANGE', $fieldname, "value must be less than or equal to $max" ];
	}
    }
    
    else
    {
	$value = ($1 && $1 eq '-') ? "-$2" : $2;
	
	if ( defined $max )
	{
	    my $lower = $max + 1;
	    
	    if ( $value > $max || (-1 * $value) > $lower )
	    {
		return [ 'E_RANGE', $fieldname, "value must lie between -$lower and $max" ];
	    }
	}
    }
    
    # If the value is valid as-is, return the empty list.

    return;
}


# validate_fixed_value ( type, value, fieldname )
# 
# Check that the specified value is suitable for storing into a fixed-point decimal column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_fixed_value {

    my ($edt, $type, $value, $fieldname, $can_truncate) = @_;
    
    my ($unsigned, $whole, $precision) = ref $type eq 'ARRAY' ? $type->@* : $type;
    
    # First make sure that the value is either empty or matches the proper format.  A value which
    # is empty or contains only whitespace is turned into NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return ('clean', undef);
    }
    
    elsif ( $value !~ $DECIMAL_NUMBER_RE )
    {
	my $phrase = $unsigned ? 'an unsigned' : 'a';
	
	return ['E_FORMAT', $fieldname, "value must be $phrase decimal number" ];
    }
    
    else
    {
	# If the column is unsigned, make sure there is no minus sign.
	
	if ( $unsigned && defined $1 && $1 eq '-' )
	{
	    return [ 'E_RANGE', $fieldname, "value must be an unsigned decimal number" ];
	}
	
	# Now put the number back together from the regex captures. If there is an
	# exponent, reformat it as a fixed point.
	
	my $sign = $1 && $1 eq '-' ? '-' : '';
	my $intpart = $2 // '';
	my $fracpart = $3 // $4 // '';
	
	if ( $6 )
	{
	    my $exponent = ($5 && $5 eq '-' ? "-$6" : $6);
	    my $formatted = sprintf("%.10f", "${intpart}.${fracpart}E${exponent}");
	    
	    ($intpart, $fracpart) = split(/[.]/, $formatted);
	}
	
	# Check that the number of digits is not exceeded, either before or after the decimal. In
	# the latter case, we add an error unless the column property ALLOW_TRUNCATE is set in
	# which case we add a warning.
	
	$intpart =~ s/^0+//;
	$fracpart =~ s/0+$//;
	
	if ( $intpart && length($intpart) > $whole )
	{
	    my $total = $whole + $precision;
	    
	    return [ 'E_RANGE', $fieldname, 
		     "value is too large for decimal($total,$precision)" ];
	}
	
	# Rebuild the value, with the fracional part trimmed.
	
	my $clean_value = $sign;
	$clean_value .= $intpart || '0';
	$clean_value .= '.' . substr($fracpart, 0, $precision);
	
	# If the value is too wide, return either an error condition or the truncated value and a warning.
	
	if ( $fracpart && length($fracpart) > $precision )
	{
	    my $total = $whole + $precision;
	    
	    if ( $can_truncate )
	    {
		return ('clean', $clean_value, 
			['W_TRUNC', $fieldname, 
			 "value has been truncated to decimal($total,$precision)"]);
	    }
	    
	    else
	    {
		return [ 'E_WIDTH', $fieldname,
			 "too many decimal digits for decimal($total,$precision)" ];
	    }
	}
	
	# If the clean value is different from the original but is equivalent, return it.
	
	elsif ( $clean_value ne $value )
	{
	    return ('clean', $clean_value);
	}
    }
    
    # If the value is valid as-is, return the empty list.
    
    return;
}


# validate_float_value ( type, value, fieldname )
# 
# Check that the specified value is suitable for storing into a floating-point decimal column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_float_value {

    my ($edt, $type, $value, $fieldname) = @_;
    
    my ($unsigned, $precision) = ref $type eq 'ARRAY' ? $type->@* : $type;
    
    # First make sure that the value is either empty or matches the proper format. A value which
    # is empty or contains only whitespace will be treated as a NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return ('clean', undef);
    }
    
    elsif ( $value !~ $DECIMAL_NUMBER_RE )
    {
	my $phrase = $unsigned ? 'an unsigned' : 'a';
	
	return [ 'E_FORMAT', $fieldname, "value must be $phrase floating point number" ];
    }
    
    else
    {
	my $sign = (defined $1 && $1 eq '-') ? '-' : '';
	
	# If the column is unsigned, make sure there is no minus sign.
	
	if ( $unsigned && $sign eq '-' )
	{
	    return [ 'E_RANGE', $fieldname, "value must be an unsigned floating point number" ];
	}
	
	# Put the pieces of the value back together.
	
	my $clean_value = $sign . ( $2 // '' ) . '.';
	$clean_value .= ( $3 // $4 // '' );
	
	if ( $6 )
	{
	    my $exp_sign = $5 eq '-' ? '-' : '';
	    $clean_value .= 'E' . $exp_sign . $6;
	}
	
	# Then check that the number is not too large to be represented, given the size of the
	# field. We are conservative in the bounds we check. We do not check for the number of
	# decimal places being exceeded, because floating point is naturally inexact. Also, if
	# maximum digits were specified we ignore these.
			    
	my $bound = $precision eq 'double' ? 1E308 : 1E38;
	my $word = $precision eq 'float' ? 'single' : 'double';
	
	if ( $value > $bound || ( $value < 0 && -$value > $bound ) )
	{
	    return [ 'E_RANGE', $fieldname, "magnitude is too large for $word-precision floating point" ];
	}

	# If the clean value is different from the original, return that.

	elsif ( $clean_value ne $value )
	{
	    return ('clean', $clean_value);
	}
    }

    # If the value is valid as-is, return the empty list.

    return;
}


# validate_enum_value ( type, value, fieldname )
# 
# Check that the specified value is suitable for storing into an enumerated or set valued column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_enum_value {

    my ($edt, $type, $value, $fieldname) = @_;
    
    my ($subtype, $good_values) = ref $type eq 'ARRAY' ? $type->@* : $type;
    
    # If the data type is either 'set' or 'enum', then we check to make sure that the value is one
    # of the allowable ones. We always match without regard to case, using the Unicode 'fold case'
    # function (fc).
    
    use feature 'fc';
    
    $value =~ s/^\s+//;
    $value =~ s/\s+$//;
    
    my @raw = $value;
    
    # if ( $type eq 'set' )
    # {
    # 	my $sep = $column_defn->{VALUE_SEPARATOR} || qr{ \s* , \s* }xs;
    # 	@raw = split $sep, $value;
    # }
    
    my (@good, @bad);
    
    foreach my $v ( @raw )
    {
	next unless defined $v && $v ne '';

	if ( ! $good_values )
	{
	    push @good, $v;
	}
	
	elsif ( ref $good_values && $good_values->{fc $v} )
	{
	    push @good, $v;
	}
	
	else
	{
	    push @bad, $v;
	}
    }
    
    if ( @bad )
    {
	my $value_string = join(', ', @bad);
	my $word = @bad > 1 ? 'values' : 'value';
	my $word2 = @bad > 1 ? 'are' : 'is';
	
	return [ 'E_RANGE', $fieldname, "$word '$value_string' $word2 not allowed for this table column" ];
    }
    
    elsif ( @good )
    {
	return ('clean', join(',', @good));
    }
    
    else
    {
	return ('clean', undef);
    }
}


# validate_datetime_value ( type, value, fieldname )
# 
# Check that the specified value is suitable for storing into a time or date or datetime valued
# column in the database. If it is not, add an error condition and return a non-scalar value as a
# flag to indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_datetime_value {
    
    my ($edt, $type, $value, $fieldname) = @_;
    
    my ($specific) = ref $type eq 'ARRAY' ? $type->@* : $type;
    
    if ( $value =~ qr{ ^ now (?: [(] [)] ) ? $ }xsi )
    {
	return ('unquoted', "NOW()", undef, 1);
    }
    
    elsif ( $value =~ qr{ ^ \d\d\d\d\d\d\d\d\d\d+ $ }xs )
    {
	return ('unquoted', "FROM_UNIXTIME($value)", undef, 1);
    }
    
    elsif ( $specific eq 'time' )
    {
	if ( $value =~ qr{ ^ \d\d : \d\d : \d\d $ }xs )
	{
	    return;
	}

	else
	{
	    return [ 'E_FORMAT', $fieldname, "invalid time value '$value'" ];
	}
    }
    
    elsif ( $value =~ qr{ ^ ( \d\d\d\d - \d\d - \d\d ) ( \s+ \d\d : \d\d : \d\d ) ? $ }xs )
    {
	if ( $2 || $specific eq 'date' )
	{
	    return;
	}
	
	else
	{
	    return ('clean', "$value 00:00:00");
	}
    }
    
    else
    {
	return [ 'E_FORMAT', $fieldname, "invalid datetime value '$value'" ];
    }
}


# validate_geometry_value ( type, value, fieldname )
# 
# Check that the specified value is suitable for storing into a geometry valued column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_geometry_value {
    
    my ($edt, $type, $value, $fieldname) = @_;
    
    my ($specific) = ref $type eq 'ARRAY' ? $type->@* : $type || 'unknown';
    
    # $$$ we still need to write some code to validate these.
    
    return;
}


# # add_validation_condition ( action, error, additional )
# #
# # If $error is a listref, add it to the action as an error condition. If $additional is a
# # listref, add it too. The latter might be either a second error or a warning.

# sub add_validation_condition {

#     my ($edt, $action, $error, $additional) = @_;
    
#     if ( ref $error eq 'ARRAY' )
#     {
# 	unshift @$error, $action unless $error->[0] eq 'main';
# 	$edt->add_condition(@$error);
#     }
    
#     if ( ref $additional eq 'ARRAY' )
#     {
# 	unshift @$additional, $action unless $additional->[0] eq 'main';
# 	$edt->add_condition(@$additional);
#     }
# }


1;
