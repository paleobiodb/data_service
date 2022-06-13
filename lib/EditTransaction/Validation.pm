# 
# The Paleobiology Database
# 
#   EditTransaction::Validation - role for checking permissions and validating actions.
# 


package EditTransaction::Validation;

use Moo::Role;

use strict;
no warnings 'uninitialized';

use Carp qw(carp croak);
use ExternalIdent qw(%IDP %IDRE);
use TableDefs qw(get_table_property %TABLE
		 %COMMON_FIELD_SPECIAL %COMMON_FIELD_IDTYPE %FOREIGN_KEY_TABLE %FOREIGN_KEY_COL);
use TableData qw(get_table_schema);
use Permissions;


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
    
    my ($edt, $table, $permission, $key_expr, $record) = @_;
    
    unless ( $edt->{permission_record_cache}{$table}{$key_expr}{$permission} )
    {
	$edt->{permission_record_cache}{$table}{$key_expr}{$permission} = 
	    $edt->{perms}->check_record_permission($table, $permission, $key_expr, $record);
    }
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


# validate_action ( action, operation, table, keyexpr )
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

sub validate_action {
    
    my ($edt, $action, $operation, $table, $keyexpr) = @_;
    
    if ( $EditTransaction::TEST_PROBLEM{validate} )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'TEST VALIDATE');
	return;
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


# validate_against_schema ( action, operation, table, special )
# 
# Check the field values to be stored in the database against the corresponding table definition,
# and call 'add_condition' to record any error or warning conditions that are detected. The
# column names and corresponding values to be stored are added to the action record using
# 'set_column_values', for later use by the action execution methods.
# 
# If the argument $special is given, it must be a hash ref whose keys are column names. Currently
# the only accepted value is 'skip', indicating that this field should be skipped. This is
# available for use when this method is called from within a subclass method that overrides
# 'validate_action'.

our $DECIMAL_NUMBER_RE = qr{ ^ \s* ( [+-]? ) \s* (?: ( \d+ ) (?: [.] ( \d* ) )? | [.] ( \d+ ) ) \s*
			     (?: [Ee] \s* ( [+-]? ) \s* ( \d+ ) )? \s* $ }xs;

our %EXTID_CHECK;

sub validate_against_schema {

    my ($edt, $action, $operation, $table) = @_;
    
    $operation ||= $action->operation;
    $table ||= $action->table;
    
    my $record = $action->record;
    my $permission = $action->permission;
    my $keycol = $action->keycol;
    
    my $is_owner;
    
    # Grab the table schema, or throw an exception if it is not available. This information is cached, so
    # the database will only need to be asked for this information once per process per table.
    
    my $dbh = $edt->dbh;
    my $schema = get_table_schema($dbh, $table, $edt->{debug_mode});
    
    # If we haven't yet copied over the 
    
    # If the operation is 'replace', then get the current created/modified timestamps from the old
    # record.
    
    my (@copy_columns);
    
    # if ( $operation eq 'replace' )
    # {
    # 	($old_values{created}, $old_values{modified},
    # 	 $old_values{authorizer_no}, $old_values{enterer_no}, $old_values{modifier_no}) =
    # 	    $edt->get_old_values($action, $table, 'created, modified, authorizer_no, enterer_no, modifier_no');
    # }
    
    # Start by going through the list of field names, and constructing a list of values to be
    # inserted.
    
    my (@columns, @values, %used);

  COLUMN:
    foreach my $col ( @{$schema->{_column_list}} )
    {
	my $cr = $schema->{$col};
	
	# Start by checking to see if there are special handling instructions for this column.
	
	my $special = $action->column_special($col);
	
	# If we are supposed to ignore this column, then do so.
	
	next COLUMN if $special eq 'ignore';

	# If a value for this column is found in the record, then use that.
	
	my $value = $record->{$col};
	my $record_col = $col;
	my $quote_this_value;
	my $is_default;
	
	# Skip the primary key for any operation except 'replace'. For 'replace' operations, we
	# use the cleaned key value without checking it. In all cases, the primary key value has
	# already been validated by the routine that called this one. The primary key may be
	# referred to in the action record under its alternate name, as specified by the
	# PRIMARY_FIELD property of the table. The ALTERNATE_NAME property should not be used for
	# primary keys.
	
	if ( $col eq $keycol )
	{
	    if ( exists $record->{$col} )
	    {
		$used{$col} = 1;
	    }
	    
	    elsif ( my $alt = get_table_property($table, 'PRIMARY_FIELD') )
	    {
		$used{$alt} = 1;
		$record_col = $alt;
	    }
	    
	    next COLUMN unless $operation eq 'replace';
	    
	    $value = $action->keyval;
	    $special = 'pass';
	}
	
	# Otherwise, if the column name is not mentioned in the record but an alternate name is
	# defined, then check that.
	
	elsif ( $cr->{ALTERNATE_ONLY} || ! exists $record->{$col} )
	{
	    my $alt = $cr->{ALTERNATE_NAME};
	    
	    # If an alternate name for this column is known, and this name appears as a key in the
	    # record, use that name and value. But not if we have been told to ignore it.
	    
	    if ( $alt && exists $record->{$alt} && $action->get_special($alt) ne 'ignore' )
	    {
		$record_col = $alt;
		$value = $record->{$alt};
	    }
	    
	    else
	    {
		$record_col = $alt if $cr->{ALTERNATE_ONLY};
		$value = undef;
	    }
	}
	
	# Record the keys that correspond to values from the record. We will use this info later
	# to throw error or warning conditions for any record keys that we do not recognize.
	
	$used{$record_col} = 1 if exists $record->{$record_col};
	
	# Don't check any columns we are directed to ignore. These were presumably checked by code
	# from a subclass that has called this method. Columns that have a type assigned by
	# %COMMON_FIELD_SPECIAL cannot be passed.
	
	my $type = $COMMON_FIELD_SPECIAL{$col};
	
	if ( $type || $special ne 'pass' )
	{
	    # Handle special columns in the appropriate ways.
	    
	    if ( $type )
	    {
		# The 'crmod' fields store the record creation and modification dates. These cannot be
		# specified explicitly except by a user with administrative permission, and then only
		# if this EditTransaction allows the condition 'ALTER_TRAIL'. In that case, check to
		# make sure that they have the proper format. But always ignore empty values.
		
		if ( $type eq 'crmod' )
		{
		    # If the value of 'modified' is 'NORMAL', that overrides everything else. Just
		    # treat all of the 'crmod' and 'authent' fields normally, which means "as if a
		    # null value was given". It is also okay to specify 'UNCHANGED' for 'created'
		    # since this is the normal behavior.
		    
		    if ( $record->{modified} && $record->{modified} eq 'NORMAL' )
		    {
			$value = undef;
		    }
		    
		    elsif ( $col eq 'created' && defined $value && $value eq 'UNCHANGED' )
		    {
			$value = undef;
		    }
		    
		    # Now, if a value is specified for any of the crmod fields, then add an error
		    # condition unless the user has permission to explicitly set these fields.
		    
		    if ( defined $value && $value ne '' )
		    {
			my $error;
			
			unless ( $permission =~ /admin/ )
			{
			    $edt->add_condition($action, 'E_PERM_COL', $record_col);
			    $error = 1;
			}
			
			unless ( $edt->{fixup_mode} || $edt->allows('ALTER_TRAIL') )
			{
			    $edt->add_condition($action, 'C_ALTER_TRAIL');
			    $error = 1;
			}
			
			# If the value is explicitly 'UNCHANGED', then leave it unchanged. This
			# requires copying the old value if the operation is 'replace'.
			
			if ( $value eq 'UNCHANGED' )
			{
			    next COLUMN unless $operation eq 'replace';
			    
			    push @copy_columns, $col;
			    $value = undef;
			}
			
			# Otherwise, check that the value matches the required format.
			
			else
			{
			    ($value, $quote_this_value) =
				$edt->validate_datetime_value($action, $schema->{$col}, $record_col, $value);
			    
			    next if $error || ref $value;
			}
		    }
		    
		    # Otherwise, if we are working under FIXUP_MODE, then leave no record of the
		    # modification. This is only allowed with 'admin' privilege on the table in
		    # question. If 'modified' is specifically a key in the action, with an undefined
		    # value, then skip this section because the user wants it treated normally.
		    
		    elsif ( $operation ne 'insert' && $col eq 'modified' && $edt->{fixup_mode} &&
			    ! exists $record->{$col} )
		    {
			if ( $permission !~ /admin/ )
			{
			    $edt->add_condition($action, 'E_PERM_COL', $col);
			    next;
			}
			
			elsif ( $operation eq 'replace' )
			{
			    push @copy_columns, 'modified';
			    $value = undef;
			}
			
			else
			{
			    next;
			}
		    }
		    
		    # Otherwise, if the operation is 'update' then set the modification time to
		    # the present. This is handled by specifying an explicit null value. The creation
		    # time will be unchanged, unless explicitly specified above.
		    
		    elsif ( $operation eq 'update' && $col eq 'modified' )
		    {
			$value = undef;
		    }
		    
		    # If the operation is 'replace', then copy the creation time from the old
		    # record. The modification time will be null unless specifically specified
		    # above, which will cause it to be set to the current time.
		    
		    elsif ( $operation eq 'replace' && $col eq 'created' )
		    {
			push @copy_columns, $col;
			$value = undef;
		    }
		    
		    # Otherwise, we skip the column. For a newly inserted record, this will cause
		    # the 'created' and 'modified' times to be set to the current timestamp.
		    
		    else
		    {
			next;
		    }
		}
		
		# The 'authent' fields store the identifiers of the record authorizer, enterer, and
		# modifier. These are subject to the same conditions as the 'crmod' fields if
		# specified explicitly. But empty values get filled in according to the values for the
		# current user.
		
		elsif ( $type eq 'authent' )
		{
		    # If the value of 'modified' is 'NORMAL', that overrides everything else. Just
		    # treat all of the 'crmod' and 'authent' fields normally, which means "as if a
		    # null value was given". It is also okay to specify 'UNCHANGED' for
		    # 'authorizer_no' and 'enterer_no' since this is the normal behavior.
		    
		    if ( $record->{modified} && $record->{modified} eq 'NORMAL' )
		    {
			$value = undef;
		    }
		    
		    elsif ( $col =~ /^auth|^ent/ && defined $value && $value eq 'UNCHANGED' )
		    {
			$value = undef;
		    }
		    
		    # Now, If the value is not empty, check to make sure the user has permission
		    # to set a specific value.
		    
		    if ( defined $value && $value ne '' )
		    {
			my $error;
			
			unless ( $permission =~ /admin/ )
			{
			    $edt->add_condition($action, 'E_PERM_COL', $record_col);
			    $error = 1;
			}
			
			unless ( $edt->{allows}{FIXUP_MODE} || $edt->{allows}{ALTER_TRAIL} )
			{
			    $edt->add_condition($action, 'C_ALTER_TRAIL');
			    $error = 1;
			}
			
			# If the value is explicitly 'UNCHANGED', then leave it unchanged. This
			# requires copying the old value if the operation is 'replace'.
			
			if ( $value eq 'UNCHANGED' )
			{
			    next COLUMN unless $operation eq 'replace';
			    
			    push @copy_columns, $col;
			    $value = undef;
			}
						
			# Now check to make sure the value is properly formatted.
			
			if ( ref $value eq 'PBDB::ExtIdent' )
			{
			    unless ( $value->{type} eq $IDP{PRS} )
			    {
				$edt->add_condition($action, 'E_EXTTYPE', $record_col, $value,
						    "must be an external identifier of type '$IDP{PRS}'");
				next;
			    }
			    
			    $value = $value->stringify;
			}
			
			elsif ( $value =~ $IDRE{PRS} )
			{
			    $value = $2;
			    
			    # If the value is 0, or ERROR, or something else not valid, add an error
			    # condition.
			    
			    unless ( $value > 0 )
			    {
				$edt->add_condition($action, 'E_RANGE', $record_col,
						    "value does not specify a valid record");
				next;
			    }
			}
			
			elsif ( $value =~ $IDRE{LOOSE} )
			{
			    $edt->add_condition($action, 'E_EXTTYPE', $record_col,
						"external id type '$1' is not valid for this field");
			    next;
			}
			
			# Otherwise, if it looks like an external identifier but is not of the right
			# type, then add an error condition.
			
			elsif ( ref $value || $value !~ qr{ ^ \d+ $ }xs )
			{
			    $edt->add_condition($action, 'E_FORMAT', $record_col, 
						'must be an external identifier or an unsigned integer');
			    next;
			}
			
			# Now make sure that the specific person actually exists.
			
			unless ( $edt->check_key('PERSON_DATA', $col, $value) )
			{
			    $edt->add_condition($action, 'E_KEY_NOT_FOUND', $record_col, $value);
			    next;
			}
			
			next if $error;
		    }
		    
		    # Otherwise, if we are working under FIXUP_MODE or this action was
		    # specifically directed to leave no record of the modification, then do
		    # that. But this is only allowed with 'admin' privilege on the table in
		    # question. If 'modifier_no' is specifically a key in the action, with an undefined
		    # value, then skip this section because the user wants it treated normally.
		    
		    elsif ( $operation ne 'insert' && $col eq 'modifier_no' && $edt->{allows}{FIXUP_MODE} &&
			    ! exists $record->{$col} )
		    {
			if ( $permission !~ /admin/ )
			{
			    $edt->add_condition($action, 'E_PERM_COL', $record_col);
			    next;
			}
			
			elsif ( $operation eq 'replace' )
			{
			    push @copy_columns, $col;
			    $value = undef;
			}
			
			else
			{
			    next;
			}
		    }
		    
		    # If (as is generally supposed to happen) no value is specified for this
		    # column, then fill it in from the known information. The 'authorizer_no',
		    # 'enterer_no', and 'enterer_id' fields are filled in on record insertion, and
		    # 'modifier_no' on record update. If this is a 'replace' operation, then
		    # specify that this value should be replaced by the one in the old record.
		    
		    elsif ( $col eq 'authorizer_no' && $operation ne 'update' )
		    {
			$value = $edt->{perms}->authorizer_no;
			
			push @copy_columns, $col if $operation eq 'replace';
		    }
		    
		    elsif ( $col eq 'enterer_no' && $operation ne 'update' )
		    {
			$value = $edt->{perms}->enterer_no;
			
			push @copy_columns, $col if $operation eq 'replace';
		    }
		    
		    elsif ( $col eq 'enterer_id' && $operation ne 'update' )
		    {
			$value = $edt->{perms}->user_id;
			$quote_this_value = 1;
			
			push @copy_columns, $col if $operation eq 'replace';
			
		    }
		    
		    elsif ( $col eq 'modifier_no' && $operation ne 'insert' )
		    {
			if ( $action->{_no_modifier} )
			{
			    $value = 0;
			}

			else
			{
			    $value = $edt->{perms}->enterer_no;
			}
		    }
		    
		    elsif ( $col eq 'modifier_id' && $operation ne 'insert' )
		    {
			$value = $edt->{perms}->user_id;
		    }
		    
		    # Otherwise, we skip this column.
		    
		    else
		    {
			next;
		    }
		}
		
		# The 'admin' fields specify attributes that can only be controlled by users with
		# administrative privilege. For now, this includes only 'admin_lock'. 
		
		elsif ( $type eq 'admin' )
		{
		    # If the value is empty, skip it and let it be filled in by the database engine.
		    
		    next unless defined $value && $value ne '';
		    
		    # Otherwise, check to make sure the user has permission to set a specific value.

		    unless ( $permission =~ /admin/ )
		    {
			$edt->add_condition($action, 'E_PERM_COL', $col);
		    }
		    
		    # If so, make sure the value is correct.
		    
		    if ( $col eq 'admin_lock' && not ( $value eq '1' || $value eq '0' ) )
		    {
			$edt->add_condition($action, 'E_FORMAT', $col, 'value must be 1 or 0');
		    }
		}

		# The 'owner' fields specify attributes that can only be controlled by owners.
		# For now, this includes only 'owner_lock'.
		
		elsif ( $type eq 'owner' )
		{
		    # If the value is empty, skip it and let it be filled in by the database engine.
		    
		    next unless defined $value && $value ne '';
		    
		    # Otherwise, check to make sure the current user is the owner or administrator.
		    
		    unless ( defined $is_owner )
		    {
			$is_owner = $edt->{perms}->check_if_owner($table, $action->keyexpr);
		    }
		    
		    unless ( $is_owner)
		    {
			$edt->add_condition($action, 'E_PERM_COL', $col);
		    }
		    
		    # If so, make sure the value is correct.
		    
		    if ( $col eq 'owner_lock' && not ( $value eq '1' || $value eq '0' ) )
		    {
			$edt->add_condition($action, 'E_FORMAT', $col, 'value must be 1 or 0');
		    }
		}
		
		else
		{
		    croak "bad internal field type";
		}
	    }
	    
	    # Otherwise, if the value is defined then validate against the column definition.
	    
	    elsif ( defined $value )
	    {
		# If the column allows external identifiers, and if the value is one, then unpack
		# it. If the value is already a PBDB::ExtIdent object, we assume that type checking has
		# already been done.
		
		if ( my $extid_type = $cr->{EXTID_TYPE} || $COMMON_FIELD_IDTYPE{$col} )
		{
		    # If the external identifier has already been parsed, make sure it has the
		    # proper type.
		    
		    if ( ref $value eq 'PBDB::ExtIdent' )
		    {
			$EXTID_CHECK{$extid_type} ||= qr{$IDP{$extid_type}};
			my $type = $value->type;
			
			unless ( $type eq 'unk' || $type =~ $EXTID_CHECK{$extid_type} )
			{
			    $edt->add_condition($action, 'E_EXTTYPE', $record_col,
						"wrong type for external identifier: must be '$IDP{$extid_type}'");
			    next;
			}
			
			$value = $value->stringify;
			$record->{$record_col} = $value;
		    }
		    
		    # If it is a number or a label reference, then leave it alone. We'll have to change
		    # this check if we ever add non-integer keys.
		    
		    elsif ( $value =~ /^\d+$|^@/ )
		    {
			# do nothing
		    }

		    # If it is the empty string, set it to zero.
		    
		    elsif ( $value eq '' )
		    {
			$value = 0;
		    }
		    
		    # If it looks like an external identifier of the proper type, unpack it.
		    
		    elsif ( $value =~ $IDRE{$extid_type} )
		    {
			$value = $2;

			# If the value is a positive integer, do nothing
			
			if ( $value =~ /^\d+$/ )
			{
			    # do nothing
			}
			
			# If the value is ERROR, or something else not valid, add an error
			# condition.
			
			else
			{
			    $edt->add_condition($action, 'E_RANGE', $record_col,
						"value does not specify a valid record");
			    next;
			}
		    }
		    
		    # Otherwise, if it looks like an external identifier but is not of the right
		    # type, then add an error condition.
		    
		    elsif ( $value =~ $IDRE{LOOSE} )
		    {
			$edt->add_condition($action, 'E_EXTTYPE', $record_col,
					    "external id type '$1' is not valid for this field");
			next;
		    }
		    
		    # Otherwise, add an error condition if we are expecting an integer. If we ever
		    # add non-integer keys, we'll have to come up with some other check.
		    
		    elsif ( $cr->{TypeParams}[0] && $cr->{TypeParams}[0] eq 'integer' )
		    {
			$edt->add_condition($action, 'E_FORMAT', $record_col,
					    "value must be an unsigned integer or an external " .
					    "identifier of type '$IDP{$extid_type}'");
			next;
		    }
		}
		
		# At this point, throw an exception (a real one) if we are handed a value which is
		# an anonymous hash or array ref. In fact, the only reference type we accept is a
		# PBDB external identifier.
		
		if ( ref $value && reftype $value ne 'SCALAR' )
		{
		    my $type = ref $value;

		    if ( $type eq 'PBDB::ExtIdent' )
		    {
			$edt->add_condition($action, 'E_EXTTYPE', $record_col,
					    "no external identifier type was defined for this field");
		    }

		    else
		    {
			croak "invalid value type '$type' for col '$col'";
		    }
		}
		
		# Handle references to keys from other PBDB tables by checking them
		# against the specified table.
		
		if ( my $foreign_table = $cr->{FOREIGN_TABLE} || $FOREIGN_KEY_TABLE{$col} )
		{
		    if ( $value =~ /^@(.*)/ )
		    {
			my $check_table = $edt->{label_found}{$1};

			unless ( $check_table && $check_table eq $foreign_table )
			{
			    $edt->add_condition($action, 'E_LABEL_NOT_FOUND', $record_col, $value);
			    next;
			}
			
			$quote_this_value = 1;
			$action->substitute_label($col);
		    }
		    
		    elsif ( $value )
		    {
			no strict 'refs';
			
			# my $f_table = ${$foreign_table};
			my $foreign_col = $cr->{FOREIGN_KEY} || $FOREIGN_KEY_COL{$col} || $col;
			
			unless ( $edt->check_key($foreign_table, $foreign_col, $value) )
			{
			    $edt->add_condition($action, 'E_KEY_NOT_FOUND', $record_col, $value);
			    next;
			}
		    }
		    
		    else
		    {
			$value = undef;
		    }
		}
		
		# Otherwise, check the value according to the column type.
		
		elsif ( ref $cr->{TypeParams} )
		{
		    my ($type, @param) = @{$schema->{$col}{TypeParams}};
		    
		    if ( $type eq 'text' || $type eq 'data' )
		    {
			($value, $quote_this_value) = $edt->validate_character_value($action, $schema->{$col}, $record_col, $value);
			next if ref $value;
		    }
		    		    
		    elsif ( $type eq 'boolean' )
		    {
			$value = $edt->validate_boolean_value($action, $schema->{$col}, $record_col, $value);
			
			next if ref $value;
		    }
		    
		    elsif ( $type eq 'integer' )
		    {
			$value = $edt->validate_integer_value($action, $schema->{$col}, $record_col, $value);

			next if ref $value;
		    }
		    
		    elsif ( $type eq 'fixed' )
		    {
			$value = $edt->validate_fixed_value($action, $schema->{$col}, $record_col, $value);
			
			next if ref $value;
		    }
		    
		    elsif ( $type eq 'floating' )
		    {
			$value = $edt->validate_float_value($action, $schema->{$col}, $record_col, $value);
			
			next if ref $value;
		    }
		    		    
		    elsif ( $type eq 'enum' || $type eq 'set' )
		    {
			$value = $edt->validate_enum_value($action, $schema->{$col}, $record_col, $value);
			$quote_this_value = 1;
			next if ref $value;
		    }

		    elsif ( $type eq 'date' )
		    {
			($value, $quote_this_value) =
			    $edt->validate_datetime_value($action, $schema->{$col}, $record_col, $value);
			
			next if ref $value;
		    }

		    elsif ( $type eq 'geometry' )
		    {
			$value = $edt->validate_geometry_value($action, $schema->{$col}, $record_col, $value);
			next if ref $value;
		    }
		    
		    # If the data type is anything else, we just throw up our hands and accept
		    # whatever they give us. This might not be wise.

		    # Now store the cleaned value back into the record, so that before_action and
		    # after_action routines will have access to it.
		    
		    $record->{$record_col} = $value;
		}
	    }
	    
	    # Now we have to re-check whether we have a defined value or not. Some of the data
	    # types checked above turn whitespace into null, for example. If we have a value, then
	    # if a validator function has been defined for this column, call it. If the function
	    # returns a condition code, then add the specified error or warning condition.
	    
	    if ( defined $value )
	    {
		if ( $cr->{VALIDATOR} )
		{
		    my $v = $cr->{VALIDATOR};
		    
		    my ($code, @error_params) = ref $v eq 'CODE' ?
			&$v($edt, $value, $record_col, $action) :
			$edt->$v($value, $record_col, $action);
		    
		    if ( $code )
		    {
			$error_params[0] ||= 'value is not valid for this field';
			$edt->add_condition($action, $code, $record_col, @error_params);
			next;
		    }
		}
	    }
	    
	    # Otherwise, we don't have a defined value for this column. If the column name is
	    # 'modified' and this is an 'update' or 'replace' operation, or 'created' on a
	    # 'replace' operation, then let it go through as a null. This will cause the current
	    # timestamp to be stored.
	    
	    elsif ( $col eq 'modified' && $operation ne 'insert' )
	    {
		# let this column go through with a value of NULL
	    }
	    
	    elsif ( $col eq 'created' && $operation eq 'replace' )
	    {
		# let this column go through with a value of NULL
	    }
	    
	    # Otherwise, if this column is required to have a value, then throw an exception
	    # unless this is an update operation and the column does not appear in the action
	    # record. Any columns not explicitly given a value in an update operation are left
	    # with whatever value was previously stored in the table.
	    
	    elsif ( ($cr->{REQUIRED} || $cr->{NOT_NULL} ) &&
		    ( $operation ne 'update' || exists $record->{$record_col} ) )
	    {
		my $col_name;
		
		if ( $record_col ne $col ) { $col_name = $record_col; }
		else { $col_name = $cr->{ALTERNATE_NAME} || $record_col; }
		
		$edt->add_condition($action, 'E_REQUIRED', $col_name);
		next;
	    }
	    
	    # If this column does appear in the action record, then it should be explicitly
	    # included in the SQL statement. If it has a default value, we substitute
	    # that. Otherwise, we will let its value be NULL.
	    
	    elsif ( exists $record->{$record_col} )
	    {
		if ( defined $cr->{Default} )
		{
		    $value = $cr->{Default};
		    $quote_this_value = 1;
		    $is_default = 1;
		}
	    }
	    
	    # If we get here, then the column does not appear in the action record, is not
	    # explicitly required, and is not implicitly required for this operation
	    # (i.e. 'modified' with an 'update' or 'replace' operation). So we skip it.
	    
	    else
	    {
		next;
	    }
	}
	
	# If we were directed not to validate this column, we still need to check whether it is
	# mentioned in the record. If not, we skip it.
	
	elsif ( ! exists $record->{$record_col} )
	{
	    next;
	}
	
	# If this column has the ADMIN_SET property, then throw an exception unless
	# the user has 'admin' privilege, or unless the value being set is the default.
	
	if ( $cr->{ADMIN_SET} && ! $is_default && $action->permission ne 'admin' )
	{
	    $edt->add_condition($action, 'E_PERM_COL', $record_col);
	}
	
	# If we get here, then we have a good value! Push the column and value on the respective
	# lists. An undefined value is pushed as NULL, otherwise the value is quoted. The default
	# behavior for mariadb when given the empty string as a value for a numeric column is to
	# store zero. So we'll go with that.
	
	push @columns, $col;
	
	if ( defined $value )
	{
	    $value = $dbh->quote($value) if $quote_this_value;
	    push @values, $value;
	}
	
	else
	{
	    push @values, 'NULL';
	}
    }
    
    # If this is a primary action (not auxiliary) and there are any unrecognized keys in this
    # record, add an error or a warning depending on whether BAD_FIELDS is allowed for this
    # transaction.
    
    unless ( $action->is_child )
    {
	foreach my $key ( keys %$record )
	{
	    next if $used{$key};
	    next if $key =~ /^_/;
	    next if $action->{ignore_field}{$key};
	    
	    if ( $edt->allows('BAD_FIELDS') )
	    {
		$edt->add_condition($action, 'W_BAD_FIELD', $key);
	    }
	    
	    else
	    {
		$edt->add_condition($action, 'E_BAD_FIELD', $key);
	    }
	}
    }
    
    # If the action has no errors, then we save the column values to it.
    
    unless ( $action->errors )
    {    
	# If we were directed to copy any old column values, do this first.
	
	if ( @copy_columns )
	{
	    my (@copy_values) = $edt->get_old_values($table, $action->keyexpr, join(',', @copy_columns));
	    
	    my (%copy_values, $substitution_count);
	    
	    foreach my $i ( 0..$#copy_columns )
	    {
		$copy_values{$copy_columns[$i]} = $dbh->quote($copy_values[$i]) if defined $copy_values[$i];
	    }
	    
	    foreach my $i ( 0..$#columns )
	    {
		if ( defined $copy_values{$columns[$i]} )
		{
		    $values[$i] = $copy_values{$columns[$i]};
		    $substitution_count++;
		}
		
		last if $substitution_count == scalar(keys %copy_values);
	    }
	}
	
	# Now store our column and value lists for subsequent use in constructing SQL statements.
	
	$action->set_column_values(\@columns, \@values);
    }
    
    return;
}


# validate_character_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into a boolean column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_character_value {
    
    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    my ($type, $size, $variable, $charset) = @{$column_defn->{TypeParams}};
    
    my $value_size = length($value);
    my $quote_this_value = 1;
    
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
	    $quote_this_value = 0;
	}
    }
    
    # If the size of the value exceeds the size of the column, then we either truncate the data if
    # the column has the ALLOW_TRUNCATE attribute or else reject the value.
    
    if ( defined $size && $value_size > $size )
    {
	my $word = $type eq 'text' ? 'characters' : 'bytes';
	
	if ( $column_defn->{ALLOW_TRUNCATE} )
	{
	    $value = substr($value, 0, $size);
	    $edt->add_condition($action, 'W_TRUNC', $record_col,
				"value was truncated to a length of $size $word");
	}
	
	else
	{
	    $edt->add_condition($action, 'E_WIDTH', $record_col,
				"value must be no more than $size $word in length, was $value_size");
	    return { };
	}
    }
    
    # If this column is required and the value is empty, add an error condition.
    
    if ( $value eq '' && $column_defn->{REQUIRED} )
    {
	$edt->add_condition($action, 'E_REQUIRED', $record_col);
	return { };
    }
    
    return ($value, $quote_this_value);
}


# validate_boolean_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into a boolean column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_boolean_value {
    
    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    # If the type is boolean, the value must be either 1 or 0. But we allow 'yes', 'no', 'true',
    # and 'false' as synonyms. A string that is empty or has only whitespace is turned into a
    # null.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
    }
    
    else
    {
	unless ( $value =~ qr{ ^ \s* (?: ( 1 | true | yes ) | ( 0 | false | no ) ) \s* $ }xsi )
	{
	    $edt->add_condition($action, 'E_FORMAT', $record_col,
				"value must be one of: 1, 0, true, false, yes, no");
	    return { };
	}
	
	return $1 ? 1 : 0;
    }
}


# validate_integer_value ( action, column_defn, record_col, value )
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
    
    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    my ($type, $unsigned, $size) = @{$column_defn->{TypeParams}};

    my $max = $unsigned ? $UNSIGNED_BOUND{$size} : $SIGNED_BOUND{$size};
    
    # First make sure that the value is either empty or matches the proper format. A value which
    # is empty or contains only whitespace will be treated as a NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
    }
    
    elsif ( $value !~ qr{ ^ \s* ( [-+]? ) \s* ( \d+ ) \s* $ }xs )
    {
	my $phrase = $unsigned ? 'an unsigned' : 'an';
	
	$edt->add_condition($action, 'E_FORMAT', $record_col,
			    "value must be $phrase integer");
	return { };
    }
    
    elsif ( $unsigned )
    {
	$value = $2;
	
	if ( $1 && $1 eq '-' )
	{
	    $edt->add_condition($action, 'E_RANGE', $record_col, 
				"value must an unsigned decimal number");
	    return { };
	}
	
	elsif ( $value > $max )
	{
	    $edt->add_condition($action, 'E_RANGE', $record_col,
				"value must be less than or equal to $max");
	    return { };
	}
	
	else
	{
	    return $value;
	}
    }
    
    else
    {
	$value = ($1 && $1 eq '-') ? "-$2" : $2;
	
	my $lower = $max + 1;
	
	if ( $value > $max || (-1 * $value) > $lower )
	{
	    $edt->add_condition($action, 'E_RANGE', $record_col, 
				"value must lie between -$lower and $max");
	    return { };
	}
	
	else
	{
	    return $value;
	}
    }
}


# validate_fixed_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into a fixed-point decimal column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_fixed_value {

    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    my ($type, $unsigned, $whole, $precision) = @{$column_defn->{TypeParams}};
    
    # First make sure that the value is either empty or matches the proper format.  A value which
    # is empty or contains only whitespace is turned into NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
    }
    
    elsif ( $value !~ $DECIMAL_NUMBER_RE )
    {
	my $phrase = $unsigned ? 'an unsigned' : 'a';
	
	$edt->add_condition($action, 'E_FORMAT', $record_col,
			    "value must be $phrase decimal number");
	return { };
    }
    
    else
    {
	# If the column is unsigned, make sure there is no minus sign.
	
	if ( $unsigned && defined $1 && $1 eq '-' )
	{
	    $edt->add_condition($action, 'E_RANGE', $record_col,
				"value must be an unsigned decimal number");
	    return { };
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
	    
	    $edt->add_condition($action, 'E_RANGE', $record_col,
				"value is too large for decimal($total,$precision)");
	    return { };
	}
	
	if ( $fracpart && length($fracpart) > $precision )
	{
	    my $total = $whole + $precision;
	    
	    if ( $column_defn->{ALLOW_TRUNCATE} )
	    {
		$edt->add_condition($action, 'W_TRUNC', $record_col,
				    "value has been truncated to decimal($total,$precision)");
	    }
	    
	    else
	    {
		$edt->add_condition($action, 'E_WIDTH', $record_col,
				    "too many decimal digits for decimal($total,$precision)");
		return { };
	    }
	}
	
	# Rebuild the value, with the fracional part trimmed.
	
	$value = $sign;
	$value .= $intpart || '0';
	$value .= '.' . substr($fracpart, 0, $precision);
	
	return $value;
    }
}


# validate_float_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into a floating-point decimal column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_float_value {

    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    my ($type, $unsigned, $precision) = @{$column_defn->{TypeParams}};
    
    # First make sure that the value is either empty or matches the proper format. A value which
    # is empty or contains only whitespace will be treated as a NULL.
    
    if ( $value =~ qr{ ^ \s* $ }xs )
    {
	return undef;
    }
    
    elsif ( $value !~ $DECIMAL_NUMBER_RE )
    {
	my $phrase = $unsigned ? 'an unsigned' : 'a';
	
	$edt->add_condition($action, 'E_FORMAT', $record_col,
			    "value must be $phrase floating point number");
	return { };
    }
    
    else
    {
	my $sign = (defined $1 && $1 eq '-') ? '-' : '';
	
	# If the column is unsigned, make sure there is no minus sign.
	
	if ( $unsigned && $sign eq '-' )
	{
	    $edt->add_condition($action, 'E_RANGE', $record_col,
				"value must be an unsigned floating point number");
	    return { };
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
	    $edt->add_condition($action, 'E_RANGE', $record_col,
				"magnitude is too large for $word-precision floating point");
	    return { };
	}

	return $value;
    }
}


# validate_enum_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into an enumerated or set valued column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_enum_value {

    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    my ($type, $good_values) = @{$column_defn->{TypeParams}};
    
    # If the data type is either 'set' or 'enum', then we check to make sure that the value is one
    # of the allowable ones. We always match without regard to case, using the Unicode 'fold case'
    # function (fc).
    
    use feature 'fc';
    
    $value =~ s/^\s+//;
    $value =~ s/\s+$//;
    
    my @raw = $value;
    
    if ( $type eq 'set' )
    {
	my $sep = $column_defn->{VALUE_SEPARATOR} || qr{ \s* , \s* }xs;
	@raw = split $sep, $value;
    }
    
    my (@good, @bad);
    
    foreach my $v ( @raw )
    {
	next unless defined $v && $v ne '';
	
	if ( $good_values->{fc $v} )
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
	
	$edt->add_condition($action, 'E_RANGE', $record_col,
			    "$word '$value_string' $word2 not allowed for this table column");
	return { };
    }
    
    if ( @good )
    {
	return join(',', @good);
    }
    
    else
    {
	return undef;
    }
}


# validate_datetime_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into a time or date or datetime valued
# column in the database. If it is not, add an error condition and return a non-scalar value as a
# flag to indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_datetime_value {
    
    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    my ($type, $specific) = @{$column_defn->{TypeParams}};
    
    if ( $value =~ qr{ ^ now (?: [(] [)] ) ? $ }xsi )
    {
	return 'NOW()';
    }

    elsif ( $value =~ qr{ ^ \d\d\d\d\d\d\d\d\d\d+ $ }xs )
    {
	return "FROM_UNIXTIME($value)";
    }
    
    elsif ( $specific eq 'time' )
    {
	if ( $value !~ qr{ ^ \d\d : \d\d : \d\d $ }xs )
	{
	    $edt->add_condition($action, 'E_FORMAT', $record_col, "invalid time format '$value'");
	    return { };
	}
	
	return ($value, 1);
    }
    
    else
    {
	if ( $value !~ qr{ ^ ( \d\d\d\d - \d\d - \d\d ) ( \s+ \d\d : \d\d : \d\d ) ? $ }xs )
	{
	    $edt->add_condition($action, 'E_FORMAT', $record_col, "invalid datetime format '$value'");
	    return { };
	}
	
	unless ( defined $2 && $2 ne '' )
	{
	    $value .= ' 00:00:00';
	}
	
	return ($value, 1);
    }
}


# validate_geometry_value ( action, column_defn, record_col, value )
# 
# Check that the specified value is suitable for storing into a geometry valued column in the
# database. If it is not, add an error condition and return a non-scalar value as a flag to
# indicate that no further processing should be done on it.
# 
# If the value is good, this routine will return a canonical version suitable for storing into the
# column. An undefined return value will indicate a null.

sub validate_geometry_value {
    
    my ($edt, $action, $column_defn, $record_col, $value) = @_;
    
    my ($type, $specific) = @{$column_defn->{TypeParams}};
    
    # $$$ we still need to write some code to validate these.
    
    return $value;
}


# check_key ( table, value )
#
# Make sure that the specified key exists in the specified table.

sub check_key {
    
    my ($edt, $table_specifier, $col, $value) = @_;
    
    if ( $FOREIGN_KEY_COL{$col} )
    {
	$col = $FOREIGN_KEY_COL{$col};
    }
    
    my $quoted = $edt->dbh->quote($value);
    
    my $sql = "SELECT $col FROM $TABLE{$table_specifier} WHERE $col=$quoted";

    $edt->debug_line( "$sql\n" );
    
    my ($found) = $edt->dbh->selectrow_array($sql);

    return $found;
}

1;

