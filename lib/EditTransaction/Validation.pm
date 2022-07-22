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
    EditTransaction->register_directive('ts_created', \&validate_special_column, 'DEFAULT');
    EditTransaction->register_directive('ts_modified', \&validate_special_column, 'DEFAULT');
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


# validate_action ( action, operation, table, flag )
#
# Call the validate_record method to check that the new field values to be added or updated in the
# database are consistent with the corresponding table definition. The $keyexpr parameter is
# not used by this code, since it does not check the current values in the database. However, this
# parameter is provided for every operation except 'insert' in case an override method wants to
# use it.
#
# This method may be overridden by subclasses, in order to provide different checks or additional
# checks. Such methods should indicate error and warning conditions using the method
# 'add_condition'. Override methods will probably want to call SUPER::validate_action as
# well, because it provides comprehensive checks to make sure that all record values can be
# properly stored in the database. Specific columns can be exempted from validation checks by
# calling 'column_skip_validate' on the action object and providing one or more column names.
#
# When this routine is called the first time, it may put off completion by calling the action
# method 'validate_later' and then returning. In this case, it will be called a second time with
# the flag 'COMPLETE'. The routine should then complete whatever work it needs to do.

sub validate_action {
    
    my ($edt, $action, $operation, $table, $flag) = @_;
    
    # This is used for testing purposes.
    
    if ( $EditTransaction::TEST_PROBLEM{validate} )
    {
	die "TEST VALIDATE";
    }
}


# column_special ( special, column ... )
#
# This is intended to be called as either a class method or an instance. It specifies special
# treatment for certain columns given by name.

# sub column_special {
    
#     my ($edt, $table_specifier, $special, @columns) = @_;
    
#     my $hash;
    
#     # If this was called as an instance method, attach the special column information to this
#     # instance, stored under the table name as a hash key.
    
#     if ( ref $edt )
#     {
#     	$hash = $edt->{column_special}{$table_specifier} ||= { };
#     }
    
#     # Otherwise, store it in a global variable using the name of the class and table name as a
#     # hash key.
    
#     else
#     {
#     	$hash = $SPECIAL_BY_CLASS{$edt}{$table_specifier} ||= { };
#     }
    
#     # Now set the specific attribute for non-empty column name.
    
#     foreach my $col ( @columns )
#     {
# 	$hash->{$col} = $special if $col;
#     }
# }


# validate_against_schema ( action, operation, table, flag )
# 
# Check the field values to be stored in the database against the corresponding table definition,
# and call 'add_condition' to record any error or warning conditions that are detected. The
# column names and corresponding values to be stored are added to the action record using
# 'set_column_values', for later use by the action execution methods.
# 
# If the $flag argument is present, it should have the value 'FINAL' to indicate that we should
# now complete a pending validation.

our $DECIMAL_NUMBER_RE = qr{ ^ \s* ( [+-]? ) \s* (?: ( \d+ ) (?: [.] ( \d* ) )? | [.] ( \d+ ) ) \s*
			     (?: [Ee] \s* ( [+-]? ) \s* ( \d+ ) )? \s* $ }xs;

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
    
    # Otherwise, complete the validation unless the status is 'COMPLETE' or 'PENDING'. These are
    # the only two status codes, so return if 'validation' returns a non-empty value. This
    # typically happens when a 'validate_action' override method calls 'validate_later'. The
    # result is to postpone validation until just before execution, when this method will be
    # called with the 'COMPLETE' flag.
    
    else
    {
	return if $action->validation_status;
    }
    
    # Grab some extra attributes to be used in the validation process.
    
    my $record = $action->record;
    my $permission = $action->permission;
    my $keycol = $action->keycol;
    
    # Grab the table schema, or throw an exception if it is not available. This information is cached, so
    # the database will only need to be asked for this information once per process per table.
    
    my $dbh = $edt->dbh;
    my $schema = get_table_schema($dbh, $table, $edt->{debug_mode});
    
    # Get all column directives for this action.
    
    my %directives = $action->all_directives;
    
    # Iterate through the list of table columns and construct a list of column names and values
    # for insertion, update, or replacement.
    
    my (@column_names, @column_values, %used, @unchanged);
    
  COLUMN:
    foreach my $colname ( @{$schema->{_column_list}} )
    {
	# Get the description record for this column.
	
	my $cr = $schema->{$colname};
	
	# The following variables keep track of the value found in the record and the hash key
	# under which it was found, plus some other attributes.
	
	my ($value, $fieldname, $special, $additional, $no_quote, $is_default);
	
	# Check to see if this column has a directive. If none was explicitly assigned, certain
	# specially named columns default to specific handling directives.
	
	my $directive = $directives{$colname} || $COMMON_FIELD_SPECIAL{$colname} || '';
	
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

	    if ( $operation eq 'updated' )
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
	    my $type = $cr->{TypeParams};
	    
	    if ( defined $value && $value ne '' || $edt->{allows}{FIXUP_MODE} )
	    {
		($value, $additional, $no_quote) =
		    $edt->call_app_hook('validate_special_column', $directive, $type, $permission,
					$fieldname, $value);
	    }
	    
	    elsif ( $directive =~ /^ad/ )
	    {
		if ( defined $value && $value ne '' )
		{
		    ($value, $additional, $no_quote) =
			$edt->validate_special_admin($directive, $type, $permission, $fieldname, $value);
		}
	    }
	    
	    elsif ( $directive =~ /^ow/ )
	    {
		if ( defined $value && $value ne '' )
		{
		    ($value, $additional, $no_quote) =
			$edt->validate_special_owner($directive, $type, $permission, $fieldname, $value);
		}
	    }
	    
	    # If an 'E_PERM_LOCK' was returned, fill in the message.
	    
	    if ( ref $value eq 'ARRAY' && $value->[0] eq 'E_PERM_LOCK' )
	    {
		if ( $action->keymult )
		{
		    push @$value, '_multiple_';
		}
	    }
	    
	    # If the value is undefined, fill in the proper default value if any.
	    
	    unless ( $value )
	    {
		($value, $additional, $no_quote) = $edt->special_default_value($directive, $cr, $operation);
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
	    # If the column allows external identifiers, check to see if the value is one. If so,
	    # the raw value will be unpacked and the clean value substituted.
	    
	    if ( my $expected = $cr->{EXTID_TYPE} || $COMMON_FIELD_IDTYPE{$colname} )
	    {
		if ( looks_like_extid($value) )
		{
		    ($value, $additional, $no_quote) = 
			$edt->validate_extid_value($expected, $fieldname, $value);
		}
	    }
	    
	    # Add an error or warning condition if we are given an external identifier for a
	    # column that doesn't accept them.
	    
	    elsif ( ref $value eq 'PBDB::ExtIdent' )
	    {
		$value = [ 'E_EXTID', $fieldname,
			   "this field does not accept external identifiers" ];
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
	    
	    # Otherwise, check the value according to the column type.
	    
	    elsif ( ref $cr->{TypeParams} eq 'ARRAY' )
	    {
		my $type = $cr->{TypeParams};
		my $maintype = $type->[0] || 'unknown';
		
		if ( $maintype eq 'text' || $maintype eq 'data' )
		{
		    ($value, $additional, $no_quote) =
			$edt->validate_char_value($type, $fieldname, $value, $cr->{ALLOW_TRUNCATE});
		}
		
		elsif ( $maintype eq 'boolean' )
		{
		    ($value, $additional) = $edt->validate_boolean_value($type, $fieldname, $value);
		}
		
		elsif ( $maintype eq 'integer' )
		{
		    ($value, $additional) = $edt->validate_integer_value($type, $fieldname, $value);
		}
		
		elsif ( $maintype eq 'fixed' )
		{
		    ($value, $additional) 
			= $edt->validate_fixed_value($type, $fieldname, $value, $cr->{ALLOW_TRUNCATE});
		}
		
		elsif ( $maintype eq 'floating' )
		{
		    ($value, $additional) = $edt->validate_float_value($type, $fieldname, $value);
		}
		
		elsif ( $maintype eq 'enum' || $maintype eq 'set' )
		{
		    ($value, $additional) = $edt->validate_enum_value($type, $fieldname, $value);
		}
		
		elsif ( $maintype eq 'date' )
		{
		    ($value, $additional) = $edt->validate_datetime_value($type, $fieldname, $value);
		}
		
		elsif ( $maintype eq 'geometry' )
		{
		    ($value, $additional) = $edt->validate_geometry_value($type, $fieldname, $value);
		}
		
		# If the data type is anything else, stringify the value and go with it. This
		# might cause problems in occasional situations.
		
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
	    if ( $directive )
	    {
		$edt->add_condition($action, 'E_EXECUTE', "null value for directive '$directive'");
	    }

	    else
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
	    # error condition unless we have administrative privilege.
	    
	    if ( $fieldname && $cr->{ADMIN_SET} && $permission !~ /admin/ )
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
		
		# If the value is not defined but the column has a non-empty default, it is okay.
		
		elsif ( ! defined $value && defined $cr->{Default} && $cr->{Default} ne '' )
		{
		    $value_ok = 1;
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


# validate_char_value ( type, fieldname, value )
# 
# Check that the specified value is suitable for storing into a boolean column in the
# database. If it is not, return an error condition as a listref.
# 
# If the value is good, return a canonical version suitable for storing into the column. An
# undefined return value will indicate a null. The second return value, if present, will be a
# warning condition as a listref. The third return value, if present, will indicate that the
# returned value has already been quoted.

sub validate_char_value {
    
    my ($edt, $arg, $fieldname, $value, $can_truncate) = @_;
    
    my ($type, $size, $variable, $charset) = ref $arg eq 'ARRAY' ? $arg->@* : $arg;
    
    my $value_size = length($value);
    my $is_quoted;
    my $additional;
    
    # If the character set of a text/char column is not utf8, then encode it into the proper
    # character set before checking the length.
    
    if ( $type eq 'text' && $charset && $charset ne 'utf8' )
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
	    my $dbh = $edt->dbh;
	    my $quoted = $dbh->quote($value);
	    $value = "convert($quoted using $charset)";
	    ($value_size) = $dbh->selectrow_array("SELECT length($value)");
	    $is_quoted = 1;
	}
    }
    
    # If the size of the value exceeds the size of the column, then we either truncate the data if
    # the column has the ALLOW_TRUNCATE attribute or else reject the value.
    
    if ( defined $size && $value_size > $size )
    {
	my $word = $type eq 'text' ? 'characters' : 'bytes';
	
	if ( $can_truncate )
	{
	    $value = substr($value, 0, $size);
	    $additional = [ 'W_TRUNC', $fieldname,
			    "value was truncated to a length of $size $word" ];
	}
	
	else
	{
	    return [ 'E_WIDTH', $fieldname,
		     "value must be no more than $size $word in length, was $value_size" ];
	}
    }
    
    return ($value, $additional, $is_quoted);
}


# validate_boolean_value ( type, fieldname, value )
# 
# Check that the specified value is suitable for storing into a boolean column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_boolean_value {
    
    my ($edt, $arg, $fieldname, $value) = @_;
    
    # For a boolean column, the value must be either 1 or 0. But we allow 'yes', 'no', 'true', and
    # 'false' as synonyms. A string that is empty or has only whitespace is turned into a null.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
    }
    
    elsif ( $value =~ qr{ ^ \s* (?: ( 1 | true | yes | on ) | 
				    ( 0 | false | no | off ) ) \s* $ }xsi )
    {
	return $1 ? '1' : '0';
    }
    
    else
    {
	return [ 'E_FORMAT', $fieldname, "value must be one of: 1, 0, true, false, yes, no, on, off" ];
    }
}


# validate_integer_value ( type, fieldnme, value )
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
    
    my ($edt, $arg, $fieldname, $value) = @_;
    
    my ($type, $unsigned, $size) = ref $arg eq 'ARRAY' ? $arg->@* : $arg;
    
    my $max = $unsigned ? $UNSIGNED_BOUND{$size} : $SIGNED_BOUND{$size};
    
    # First make sure that the value is either empty or matches the proper format. A value which
    # is empty or contains only whitespace will be treated as a NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
    }
    
    elsif ( $value !~ qr{ ^ \s* ( [-+]? ) \s* ( \d+ ) \s* $ }xs )
    {
	if ( $value =~ $IDRE{LOOSE} )
	{
	    return [ 'E_EXTID', $fieldname, "external identifiers are not accepted for this field" ];
	}

	else
	{
	    my $phrase = $unsigned ? 'an unsigned' : 'an';
	    
	    return [ 'E_FORMAT', $fieldname, "value must be $phrase integer" ];
	}
    }
    
    elsif ( $unsigned )
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
	
	else
	{
	    return $value;
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
	
	return $value; # otherwise
    }
}


# validate_fixed_value ( type, fieldname, value )
# 
# Check that the specified value is suitable for storing into a fixed-point decimal column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_fixed_value {

    my ($edt, $arg, $fieldname, $value, $can_truncate) = @_;
    
    my ($type, $unsigned, $whole, $precision) = ref $arg eq 'ARRAY' ? $arg->@* : $arg;
    
    my $additional;
    
    # First make sure that the value is either empty or matches the proper format.  A value which
    # is empty or contains only whitespace is turned into NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
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
	    
	    return [ 'E_RANGE', $fieldname, "value is too large for decimal($total,$precision)" ];
	}
	
	if ( $fracpart && length($fracpart) > $precision )
	{
	    my $total = $whole + $precision;
	    
	    if ( $can_truncate )
	    {
		$additional = [ 'W_TRUNC', $fieldname,
				"value has been truncated to decimal($total,$precision)" ];
	    }
	    
	    else
	    {
		return [ 'E_WIDTH', $fieldname,
			 "too many decimal digits for decimal($total,$precision)" ];
	    }
	}
	
	# Rebuild the value, with the fracional part trimmed.
	
	$value = $sign;
	$value .= $intpart || '0';
	$value .= '.' . substr($fracpart, 0, $precision);
	
	return ($value, $additional);
    }
}


# validate_float_value ( type, fieldname, value )
# 
# Check that the specified value is suitable for storing into a floating-point decimal column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_float_value {

    my ($edt, $arg, $fieldname, $value) = @_;
    
    my ($type, $unsigned, $precision) = ref $arg eq 'ARRAY' ? $arg->@* : $arg;
    
    # First make sure that the value is either empty or matches the proper format. A value which
    # is empty or contains only whitespace will be treated as a NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
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
	
	$value = $sign . ( $2 // '' ) . '.';
	$value .= ( $3 // $4 // '' );
	
	if ( $6 )
	{
	    my $esign = $5 eq '-' ? '-' : '';
	    $value .= 'E' . $esign . $6;
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

	return $value;
    }
}


# validate_enum_value ( type, fieldname, value )
# 
# Check that the specified value is suitable for storing into an enumerated or set valued column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_enum_value {

    my ($edt, $arg, $fieldname, $value) = @_;
    
    my ($type, $good_values) = ref $arg eq 'ARRAY' ? $arg->@* : $arg;
    
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
	
	elsif ( $good_values->{fc $v} )
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
	return join(',', @good);
    }
    
    else
    {
	return undef;
    }
}


# validate_datetime_value ( type, fieldname, value )
# 
# Check that the specified value is suitable for storing into a time or date or datetime valued
# column in the database. If it is not, add an error condition and return a non-scalar value as a
# flag to indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_datetime_value {
    
    my ($edt, $arg, $fieldname, $value) = @_;
    
    my ($type, $specific) = ref $arg eq 'ARRAY' ? $arg->@* : $arg;
    
    if ( $value =~ qr{ ^ now (?: [(] [)] ) ? $ }xsi )
    {
	return ('NOW()', undef, 1);
    }
    
    elsif ( $value =~ qr{ ^ \d\d\d\d\d\d\d\d\d\d+ $ }xs )
    {
	return ("FROM_UNIXTIME($value)", undef, 1);
    }
    
    elsif ( $specific eq 'time' )
    {
	if ( $value !~ qr{ ^ \d\d : \d\d : \d\d $ }xs )
	{
	    return [ 'E_FORMAT', $fieldname, "invalid time '$value'" ];
	}

	else
	{
	    return $value;
	}
    }
    
    else
    {
	if ( $value !~ qr{ ^ ( \d\d\d\d - \d\d - \d\d ) ( \s+ \d\d : \d\d : \d\d ) ? $ }xs )
	{
	    return [ 'E_FORMAT', $fieldname, "invalid datetime '$value'" ];
	}
	
	unless ( defined $2 && $2 ne '' )
	{
	    $value .= ' 00:00:00';
	}
	
	return $value;
    }
}


# validate_geometry_value ( type, fieldname, value )
# 
# Check that the specified value is suitable for storing into a geometry valued column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_geometry_value {
    
    my ($edt, $arg, $fieldname, $value) = @_;
    
    my ($type, $specific) = ref $arg eq 'ARRAY' ? $arg->@* : $arg;
    
    # $$$ we still need to write some code to validate these.
    
    return $value;
}


# looks_like_extid ( value )
#

sub looks_like_extid {

    my ($value) = @_;

    return ref $value eq 'PBDB::ExtIdent' || $value =~ $IDRE{LOOSE};
}


# validate_extid_value ( type, fieldname, value )
#
# 

sub validate_extid_value {

    my ($edt, $type, $fieldname, $value) = @_;
    
    # If the external identifier has already been parsed and turned into an object, make sure it
    # has the proper type and return the stringified value.
    
    if ( ref $value eq 'PBDB::ExtIdent' )
    {
	my $value_type = $value->type;
	
	# If the value matches the proper type, stringify it and return it.
	
	$EXTID_CHECK{$type} ||= qr/$IDP{$type}/;
	
	if ( $value_type =~ $EXTID_CHECK{$type} )
	{
	    return '' . $value;
	}
	
	# Otherwise, return an error condition.
	
	else
	{
	    return [ 'E_EXTID', $fieldname,
		     "external identifier must be of type '$IDP{$type}', was '$value_type'" ];
	}
    }
    
    # If the value is a string that looks like an unparsed external identifier of the proper type,
    # unpack it and return the extracted value.
    
    elsif ( $value =~ $IDRE{$type} )
    {
	return $2;
    }
    
    # If it looks like an external identifier but is not of the right type, return an error
    # condition.
    
    elsif ( $value =~ $IDRE{LOOSE} )
    {
	$value = [ 'E_EXTID', $fieldname,
		   "external identifier must be of type '$IDP{$type}', was '$1'" ];
    }
    
    # Otherise, return undef to indicate that the value isn't an external identifier.
    
    else
    {
	return undef;
    }
}


sub check_key {
    
    my ($edt, $check_table, $check_col, $value) = @_;
    
    return unless $check_table && $check_col && $value;

    my $quoted = $edt->dbh->quote($value);
    
    my $sql = "SELECT $check_col FROM $TABLE{$check_table} WHERE $check_col=$quoted LIMIT 1";
    
    $edt->debug_line( "$sql\n" );
    
    my ($found) = $edt->dbh->selectrow_array($sql);
    
    return $found;    
}


# validate_special_column ( directive, cr, permission, fieldname, value )
# 
# This method is called once for each of the following column types that occurs in the table
# currently being operated on. The column names will almost certainly be different.
# 
# The parameter $directive must be one of the following:
# 
# ts_created      Records the date and time at which this record was created.
# ts_modified     Records the date and time at which this record was last modified.
# au_creater      Records the person_no or user_id of the person who created this record.
# au_authorizer   Records the person_no or user_id of the person who authorized its creation.
# au_modifier     Records the person_no or user_id of the person who last modified this record.
# 
# Values for these columns cannot be specified explicitly except by a user with administrative
# permission, and then only if this EditTransaction allows the condition 'ALTER_TRAIL'.
# 
# If this transaction is in FIXUP_MODE, both field values will be left unchanged if the user has
# administrative privilege. Otherwise, a permission error will be returned.
#
# The parameter $cr must contain the column description record.

sub validate_special_column {

    my ($edt, $directive, $cr, $permission, $fieldname, $value) = @_;
    
    # If the value is empty, return undef. The only exception is for the modifier/modified fields
    # if the transaction is in FIXUP_MODE.
    
    unless ( defined $value && $value eq '' )
    {
	if ( $edt->{allows}{FIXUP_MODE} && $directive eq 'ts_modified' )
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
	
	else
	{
	    return undef;
	}
    }
    
    # Otherwise, a non-empty value has been specified for this field. Check that the value matches
    # the required format.
    
    my ($additional, $no_quote);
    
    # The ts fields take datetime values, which are straightforward to validate.
    
    if ( $directive =~ /^ts/ )
    {
	($value, $additional, $no_quote) = $edt->validate_datetime_value('datetime', $fieldname, $value);
    }
    
    # The au fields take key values as specified in the column description. The ones that have
    # integer values accept external identifiers of type PRS.
    
    else
    {
	# If we don't have any type parameters for some reason, default to integer.
	
	my $type = ref $cr->{TypeParams} eq 'ARRAY' ? $cr->{TypeParams} : 'integer';
	my $maintype = ref $type eq 'ARRAY' ? $type->[0] : $type;
	
	# If the column type is 'integer', check to see if the value is an external identifier of
	# the specified type (defaulting to 'PRS').
	
	if ( $maintype eq 'integer' && looks_like_extid($value) )
	{
	    ($value) = $edt->validate_extid_value($cr->{EXTID_TYPE} || 'PRS', $fieldname, $value);
	}
	
	# If we don't already have an error condition, check if the key value is present in the
	# proper table. If not, set an error condition.
	
	unless ( ref $value eq 'ARRAY' || $edt->check_foreign_key($cr, $value) )
	{
	    $value = [ 'E_KEY_NOT_FOUND', $fieldname, $value ];
	}
    }
    
    # If the user has administrative permission on this table, check to see if the ALTER_TRAIL
    # allowance is present and add a caution if it is not. If we already have an error condition
    # related to the value, bump it into second place.
    
    # $$$ check for ENABLE_ALTER_TRAIL or require superuser.

    if ( $permission =~ /admin/ )
    {
	unless ( $edt->{allows}{ALTER_TRAIL} )
	{
	    $additional = $value if ref $value eq 'ARRAY';
	    $value = [ 'C_ALTER_TRAIL', $fieldname ];
	}
    }
    
    # Otherwise, add a permission error. If we already have an error condition related to the
    # value, bump it into second place.
    
    else
    {
	$additional = $value if ref $value eq 'ARRAY';
	$value = [ 'E_PERM_COL', $fieldname ];
    }
    
    return ($value, $additional, $no_quote);
}


1;

