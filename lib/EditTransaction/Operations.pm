# 
# EditTransaction::Operations
# 
# This role provides methods for initiating individual database operations such
# as insertions, deletions, updates, replacements, and others. Each operation
# generates a new action, which is added to the action queue of the transaction
# on which the method is called.
# 


package EditTransaction::Operations;

use strict;

use Switch::Plain;
use Carp qw(carp croak);
use Scalar::Util qw(weaken blessed);

use parent 'Exporter';

use Role::Tiny;

no warnings 'uninitialized';


our (@EXPORT_OK) = qw(%VALID_OPERATION);

our (%VALID_OPERATION) = ( insert => 1, update => 1, insupdate => 1, 
			   delete => 1, replace => 1, delete_cleanup => 1, 
			   other => 1, sql => 1 );


# Record operations
# -----------------

# The methods in this section are called by interface methods to carry out actions such as
# inserting, updating, and deleting records in the database. Each one returns true if the
# operation succeeds or is validated and queued for later execution, false otherwise.


# record_operation ( operation, table_specifier, parameters )
# 
# Execute the specified database operation using the specified table and
# parameters.

sub record_operation {
    
    my ($edt, $operation, @rest) = @_;
    
    # Create a new object to represent this action.
    
    my ($table_specifier, $parameters) = $edt->_action_args($operation, @rest);
    
    my $action = $edt->_new_action($operation, $table_specifier, $parameters);
    
    $edt->{record_count}++;
    
    # If the record includes any errors or warnings, import them as conditions.
    
    if ( my $ew = $action->record_value('_errwarn') )
    {
	$edt->import_conditions($action, $ew);
    }
    
    # Check for an inappropriate _where clause.
    
    if ( $parameters->{_where} && $operation =~ /^ins|^rep/ )
    {
	$edt->add_condition($action, 'E_HAS_WHERE');
    }
    
    # Execute the following code inside an eval block. If an exception is thrown
    # during authorization or validation, either from Authorization.pm or
    # Validation.pm or from a subclass, capture the error and add an E_EXECUTE
    # condition. This will enable the transaction to keep going if the PROCEED
    # allowance is present, and will enable client interface code to handle it
    # as a failed transaction rather than a thrown exception.
    
    eval {
	
	# If the action has a key value, and if the action parameters contain
	# the field '_label', store the key/label association. Otherwise, if the
	# action has a scalar key value, use that.
	
	if ( my $keyval = $action->keyval )
	{
	    if ( my $label = $action->record_value('_label') )
	    {
		$edt->store_label($table_specifier, $keyval, $label);
	    }
	    
	    elsif ( ref $keyval ne 'ARRAY' )
	    {
		my $ref;
		
		if ( blessed $keyval && $keyval->can('regenerate') )
		{
		    $ref = $keyval->regenerate;
		}
		
		else
		{
		    $ref = "$keyval";
		}
		
		if ( $edt->{action_ref}{"&$ref"} )
		{
		    my $index = 2;
		    $index++ while $edt->{action_ref}{"&$ref-$index"};
		    $ref = "$ref-$index";
		}
		
		$edt->{action_ref}{"&$ref"} = $action;
		weaken $edt->{action_ref}{"&$ref"};
		$action->set_label($ref);
	    }
	}
	
	# Check to make sure we have permission to carry out this operation.  An
	# error or caution will be added if the necessary permission cannot be
	# established.
	
	my $result = $edt->authorize_against_table($action, $operation, $table_specifier, 
						   $parameters);
	
	# If the action can proceed (in other words, if authorization does not
	# add any errors or cautions) then it must be validated.
	
	if ( $action->can_proceed )
	{
	    # If the operation is 'insupdate', change it to either 'insert' or
	    # 'update' depending on whether a key value is present.
	    
	    if ( $operation eq 'insupdate' )
	    {
		$operation = defined $action->keyval ? 'update' : 'insert';
		$action->operation($operation);
	    }
	    
	    # Call validate_action. This method is designed to be overridden by
	    # subclasses, and has an opportunity to carry out additional
	    # database queries, add conditions, etc.
	    
	    $edt->validate_action($action, $operation, $table_specifier);
	    
	    # If the operation is not a deletion, check the parameters against
	    # the table schema.  Make sure that the column values meet all of
	    # the criteria for this table.  Any discrepancies will cause error
	    # and/or warning conditions to be added.
	    
	    if ( $operation !~ /^delete/ )
	    {
		$edt->validate_against_schema($action, $operation, $table_specifier);
	    }
	}
    };
    
    # If a exception occurred, write the exception to the error stream and add an error
    # condition to this action.
    
    if ( $@ )
    {
	$edt->error_line($@);
	$edt->add_condition($action, 'E_EXECUTE', 
			    'an exception occurred during authorization or validation');
    }
    
    # Regardless of any added conditions, handle the action and return the action
    # reference.
    
    $edt->_handle_action($action);
}


# store_label ( label, keyval )
# 
# Store the association between the specified label and the specified key value(s).

sub store_label {
    
    my ($edt, $table_specifier, $keyval, $label) = @_;
    
    if ( ref $keyval eq 'ARRAY' )
    {
	foreach my $k ( @$keyval )
	{
	    $edt->{key_labels}{$table_specifier}{$k} = $label;
	}
    }
    
    elsif ( defined $keyval )
    {
	$edt->{key_labels}{$table_specifier}{$keyval} = $label;
    }
}


# insert_record ( [table_specifier], parameters )
# 
# Insert a record defined by the parameters into the specified database table.
# If no table is given, use the default table for this transaction or throw an
# exception. If the allowance 'CREATE' is not present, a 'C_CREATE' caution will
# be generated.

sub insert_record {

    my ($edt, @rest) = @_;
    
    # my ($table_specifier, $parameters) = $edt->_action_args('insert', @rest);
    
    $edt->record_operation('insert', @rest);
}


# update_record ( [table_specifier], parameters )
# 
# Update the specified database table, changing the record(s) specified by the
# parameters. If no table is given, use the default table for this transaction
# or throw an exception.

sub update_record {
    
    my ($edt, @rest) = @_;
    
    # my ($table_specifier, $parameters) = $edt->_action_args('update', @rest);
    
    $edt->record_operation('update', @rest);
}


# replace_record ( [table_specifier], parameters )
# 
# Replace the record specified by the parameters in the specified database table
# with a new record defined by the parameters. Typically, the parameters will
# include a primary key value which selects the record to be replaced. If no
# record corresponding to the primary key value exists, the record will be
# inserted if permissions allow and if the allowance 'CREATE' is present. If no
# table is given, use the default table for this transaction or throw an
# exception.

sub replace_record {

    my ($edt, @rest) = @_;
    
    my ($table_specifier, $parameters) = $edt->_action_args('replace', @rest);
    
    $edt->record_operation('replace', @rest);
}


# delete_record ( [table_specifier], parameters )
# 
# Delete the record(s) defined by the parameters from the specified database
# table.  If no table is given, use the default table for this transaction or
# throw an exception. The parameters can be provided as a single primary key
# value or a list (either listref or comma-separated) of primary key values.

sub delete_record {

    my ($edt, @rest) = @_;
    
    # my ($table_specifier, $parameters) = $edt->_action_args('delete', @rest);
    
    $edt->record_operation('delete', @rest);
}


# insert_update_record ( [table_specifier], parameters )
# 
# Insert a record into the specified database table or update an existing
# record, depending on the parameters.  is already in the table, that record
# will be updated. If not, the record will be inserted.

sub insert_update_record {
    
    my ($edt, @rest) = @_;
    
    # my ($table_specifier, $parameters) = $edt->_action_args('insupdate', @rest);
    
    $edt->record_operation('insupdate', @rest);
}


# delete_cleanup ( table, selector )
# 
# Create an action that will cause all records from the specified table that
# match the specified selector to be deleted UNLESS they have been inserted,
# updated, or replaced during the current transaction. This action is designed
# to be used by a transaction that wishes to completely replace the set of
# records in a subsidiary table that are tied to one or more records in a
# superior table. In most circumstances, it should be called as the last action
# in the transaction.
# 
# This action is only valid if the specified table is a subordinate table.

sub delete_cleanup {
    
    my ($edt, @rest) = @_;
    
    # my ($table_specifier, $parameters) = $edt->_action_args('delete_cleanup', @rest);
    
    $edt->record_operation('delete_cleanup', @rest);
}


# other_action ( method, [table_specifier], parameters )
# 
# An action not defined by this module is to be carried out. The argument $method must be a method
# name defined in a subclass of this module, which will be called to carry out the action. The
# method name will be passed as the operation when calling subclass methods for authoriation and validation.

sub other_action {
    
    my ($edt, $method, @rest) = @_;
    
    # Create a new object to represent this action.
    
    my ($table_specifier, $parameters) = $edt->_action_args('other', @rest);
    
    my $action = $edt->_new_action('other', $table_specifier, $parameters);
    
    $edt->{record_count}++;
    
    # Set the method according to the first argument.
    
    $action->set_method($method);
    
    # If the record includes any errors or warnings, import them as conditions.
    
    if ( ref $parameters eq 'HASH' && (my $ew = $action->record_value('_errwarn')) )
    {
	$edt->import_conditions($action, $ew);
    }
    
    # If we have a primary key value, we can authorize against that record. This may be a
    # limitation in future, but for now the action is authorized if they have 'edit' permission
    # on that record.
    
    if ( $edt->keyval )
    {
	eval {
	    # First check to make sure we have permission to update this record. An error
	    # condition will be added if the proper permission cannot be established.
	    
	    $edt->authorize_against_table($action, 'other', $table_specifier);
	    
	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
	    # class-specific checks and substitutions.
	    
	    $edt->validate_action($action, 'other', $table_specifier);
	};
	
	if ( $@ )
	{
	    $edt->error_line($@);
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	};
    }
    
    # If no primary key value was specified for this record, add an error condition.
    
    # $$$ At some point in the future, we may add the ability to carry out operations using table
    # rather than record authentication.
    
    else
    {
	$edt->add_condition($action, 'E_NO_KEY', 'operation');
    }
    
    # Handle the action and return the action reference.
    
    return $edt->_handle_action($action, 'other');
}


# skip_record ( )
# 
# Create a placeholder action that will never be executed. This should be called for input records
# that have been determined to be invalid before they are passed to this module.

sub skip_record {

    my ($edt, @rest) = @_;
    
    my $operation = $VALID_OPERATION{$rest[0]} ? shift @rest : 'skip';
    
    # Create a new object to represent this action.
    
    my ($table_specifier, $parameters) = $edt->_action_args('skip', @rest);
    
    my $action = $edt->_new_action($operation, $table_specifier, $parameters);
    
    $edt->{record_count}++;
    
    # If the record includes any errors or warnings, import them as conditions.
    
    if ( my $ew = $action->record_value('_errwarn') )
    {
	$edt->import_conditions($action, $ew);
    }
    
    # If an operation other than 'skip' was specified, modify the action record.
    
    elsif ( my $op = $action->record_value('_operation') )
    {
	if ( $VALID_OPERATION{$op} )
	{
	    $action->{operation} = $op;
	}
	
	else
	{
	    $edt->add_condition($action, 'E_BAD_OPERATION', $op);
	}
    }
    
    # Update skip_count and the action status.
    
    $edt->{skip_count}++;
    $action->set_status('skipped');
    
    # Handle this action, which will be a no-op.
    
    return $edt->_handle_action($action, 'skip');
}


# process_record ( table, record )
# 
# If the record contains the key '_operation', then call the method indicated by the key
# value. Otherwise, call either 'update_record' or 'insert_record' depending on whether or not the
# record contains a value for the table's primary key. This is a convenient shortcut for use by
# interface code.

sub process_record {
    
    my ($edt, $table_specifier, $parameters) = @_;
    
    # If the table specifier is a hashref, assume that the table specifier was omitted.
    
    if ( ref $table_specifier eq 'HASH' )
    {
	$parameters = $table_specifier;
	$table_specifier = $edt->{default_table};
    }
    
    # If the record contains the key _skip with a true value, add a placeholder action and do not
    # process this record.
    
    if ( $parameters->{_skip} )
    {
	return $edt->skip_record($table_specifier, $parameters);
    }
    
    # If the record contains the key _operation with a nonempty value, call the corresponding
    # method.
    
    elsif ( $parameters->{_operation} )
    {
	if ( $parameters->{_operation} eq 'delete' )
	{
	    return $edt->delete_record($table_specifier, $parameters);
	}
	
	elsif ( $parameters->{_operation} eq 'replace' )
	{
	    return $edt->replace_record($table_specifier, $parameters);
	}

	elsif ( $parameters->{_operation} eq 'insert' )
	{
	    return $edt->replace_record($table_specifier, $parameters);
	}

	elsif ( $parameters->{_operation} eq 'update' )
	{
	    return $edt->update_record($table_specifier, $parameters);
	}
	
	else
	{
	    return $edt->skip_record($table_specifier, $parameters);
	}
    }
    
    # If the record contains the key _action with a nonempty value, create a special action to
    # carry out the specified operation. This will typically be a method defined in a subclass of
    # EditTransaction.
    
    elsif ( $parameters->{_action} )
    {
	return $edt->other_action($table_specifier, $parameters->{_action}, $parameters);
    }
    
    # Otherwise, fall back to insert_update_record. If the record contains a
    # value for the table's primary key, and this value exists, the parameter
    # values will be applied as an update. Otherwise, a new record will be
    # inserted if the table permissions allow that and the CREATE allowance was
    # provided to this transaction.
    
    else
    {
	return $edt->insert_update_record($table_specifier, $parameters);
    }
}


# do_sql ( stmt, options )
# 
# Create an action that will execute the specified SQL statement, and do nothing else. The
# execution is protected by a try block, and an E_EXECUTE condition will be added if it fails. The
# appropriate cleanup methods will be called in this case. If an options hash is provided with the
# key 'result' and a scalar reference, the result code returned by the statement execution will be
# written to that reference. If the value is a code reference, it will be called with the result
# as an argument.

sub do_sql {

    my ($edt, $sql, $options) = @_;
    
    # Substitute any table specifiers in the SQL statement for the actual table names.
    
    # $sql =~ s{ << (\w+) >> }{ $TABLE{$1} || "_INVALID_<<$1>>" }xseg;
    
    # If any invalid tables were found, throw an exception.

    if ( $sql =~ qr{ _INVALID_<< (\w+) >> | <<>> }xs )
    {
	my $bad_table = $1 || '';
	croak "E_BAD_TABLE: invalid table name '$bad_table'";
    }
    
    # Otherwise, create a record containing this statement and then create a new action for it.
    
    my $parameters = { sql => $sql };

    if ( ref $options eq 'HASH' && $options->{store_result} )
    {
	croak "result value must be a scalar or code ref" unless
	    ref $options->{store_result} =~ /^SCALAR|^CODE/;
	
	$parameters->{store_result} = $options->{store_result};
	weaken $options->{store_result};
    }

    # $$$ need sql statement labels. In fact, we need an SQL statement type.
    
    # my $action = EditTransaction::Action->new($edt, '<SQL>', 'other', ':#s1', $parameters);
    
    # $action->set_method('_execute_sql_action');
    
    # $edt->_handle_action($action, 'other');

    # return $action->label;
}


# _handle_action ( action )
# 
# Handle the specified action record. If errors were generated for this record, put it on the 'bad
# record' list. Otherwise, either execute it immediately or put it on the action list to be
# executed later. This method returns true if the action is successfully executed or is queued for
# later execution, false otherwise.

sub _handle_action {
    
    my ($edt, $action, $operation) = @_;
    
    # If the action has accumulated any errors, update fail_count and the action status.
    
    if ( $action->has_errors )
    {
	$edt->{fail_count}++;
	$action->set_status('failed');
    }
    
    # Otherwise, the action is able to be executed. If execution is active,
    # execute it now. This will also cause any other pending actions to execute.
    
    elsif ( $edt->{execution} )
    {
	$edt->execute_action_list;
    }
    
    # Then return the action reference. This can be used to track the status of the
    # action before and after it is executed.
    
    return $action->refstring;
}


1;


# update_record ( table, record )
# 
# The specified record is to be updated in the specified table. Depending on the settings of this
# particular EditTransaction, this action may happen immediately or may be executed later. The
# record in question MUST include a primary key value, indicating which record to update.

# sub update_record {
    
#     my ($edt, @rest) = @_;
    
#     # Create a new object to represent this action.
    
#     my ($table_specifier, $parameters) = $edt->_action_args('update', @rest);
    
#     my $action = $edt->_new_action('update', $table_specifier, $parameters);
    
#     $edt->{record_count}++;
    
#     # If the record includes any errors or warnings, import them as conditions.
    
#     if ( my $ew = $action->record_value('_errwarn') )
#     {
# 	$edt->import_conditions($action, $ew);
#     }
    
#     # As with 'insert_record', execute the following code in an eval block.
    
#     eval {
	
# 	# Check to make sure we have permission to insert a record into this table.
# 	# An error or caution will be added if the necessary permission cannot be
# 	# established.
	
# 	$edt->authorize_against_table($action, 'update', $table_specifier);
	
# 	# Then check the record to be inserted, making sure that the column values meet all of
# 	# the criteria for this table. Any discrepancies will cause error and/or warning
# 	# conditions to be added.
	
# 	$edt->validate_action($action, 'update', $table_specifier);
#     };
    
#     # If a exception occurred, write the exception to the error stream and add an error
#     # condition to this action.
    
#     if ( $@ )
#     {
# 	$edt->error_line($@);
# 	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
#     }
    
#     # Regardless of any added conditions, handle the action and return the action
#     # reference.
    
#     return $edt->_handle_action($action, 'insert');
# }


# replace_record ( table, record )
# 
# The specified record is to be inserted into the specified table, replacing any record that may
# exist with the same primary key value. Depending on the settings of this particular EditTransaction,
# this action may happen immediately or may be executed later. The record in question MUST include
# a primary key value.

# sub replace_record {
    
#     my ($edt, @rest) = @_;
    
#     # Create a new object to represent this action.
    
#     my ($table_specifier, $parameters) = $edt->_action_args('replace', @rest);
    
#     my $action = $edt->_new_action('replace', $table_specifier, $parameters);
    
#     $edt->{record_count}++;
    
#     # If the record includes any errors or warnings, import them as conditions.
    
#     if ( my $ew = $action->record_value('_errwarn') )
#     {
# 	$edt->import_conditions($action, $ew);
#     }
    
#     # We can only replace a record if a single key value is specified.
    
#     if ( my $keyval = $action->keyval )
#     {
# 	eval {
# 	    # First check to make sure we have permission to replace this record. An
# 	    # error condition will be added if the proper permission cannot be established.
	    
# 	    $edt->authorize_against_table($action, 'replace', $table_specifier);
	    
# 	    # If more than one key value was specified, add an error condition.

# 	    if ( ref $keyval eq 'ARRAY' )
# 	    {
# 		$edt->add_condition($action, 'E_MULTI_KEY', 'replace');
# 	    }
	    
# 	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
# 	    # class-specific checks and substitutions.
	    
# 	    $edt->validate_action($action, 'replace', $table_specifier);
	    
# 	    # Finally, check the record to be inserted, making sure that the column values meet all of
# 	    # the criteria for this table. Any discrepancies will cause error and/or warning
# 	    # conditions to be added.
	    
# 	    $edt->validate_against_schema($action, 'replace', $table_specifier);
# 	};
	
# 	# If a exception occurred, write the exception to the error stream and add an error
# 	# condition to this action.
	
# 	if ( $@ )
#         {
# 	    $edt->error_line($@);
# 	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
# 	}
#     }
    
#     # If no primary key value was specified for this record, add an error condition. This will, of
#     # course, have to be reported under the record label that was passed in as part of the record
#     # (if one was in fact given) or else the index of the record in the input set.
    
#     else
#     {
# 	$edt->add_condition($action, 'E_NO_KEY', 'replace');
#     }
    
#     # Handle the action and return the action reference.
    
#     return $edt->_handle_action($action, 'replace');
# }


# delete_record ( table, record )
# 
# The specified record is to be deleted from the specified table. Depending on the settings of
# this particular EditTransaction, this action may happen immediately or may be executed
# later. The record in question must include a primary key value, indicating which record to
# delete. In fact, for this operation only, the $record argument may be a key value rather than a
# hash ref.

# sub delete_record {

#     my ($edt, @rest) = @_;
    
#     # Create a new object to represent this action.
    
#     my ($table_specifier, $parameters) = $edt->_action_args('delete', @rest);
    
#     my $action = $edt->_new_action('delete', $table_specifier, $parameters);
    
#     $edt->{record_count}++;
    
#     # If the record includes any errors or warnings, import them as conditions.
    
#     if ( ref $parameters eq 'HASH' && (my $ew = $action->record_value('_errwarn')) )
#     {
# 	$edt->import_conditions($action, $ew);
#     }
    
#     # A record can only be deleted if a primary key value is specified.
    
#     if ( my $keyval = $action->keyval )
#     {
# 	eval {
# 	    # First check to make sure we have permission to delete this record. An
# 	    # error condition will be added if the proper permission cannot be established.
	    
# 	    $edt->authorize_against_table($action, 'delete', $table_specifier);
	    
# 	    # Then call the 'validate_action' method, which can be overriden by subclasses to do
# 	    # class-specific checks and substitutions.
	    
# 	    $edt->validate_action($action, 'delete', $table_specifier);
# 	};
	
# 	if ( $@ )
# 	{
# 	    $edt->error_line($@);
# 	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
# 	}
#     }
    
#     # If no primary key was specified, add an error condition.
    
#     else
#     {
# 	$edt->add_condition($action, 'E_NO_KEY', 'delete');
#     }
    
#     # Handle the action and return the action reference.
    
#     return $edt->_handle_action($action, 'delete');
# }


# sub insert_update_record {
    
#     # Create a new object to represent this action. We need to specify an
#     # operation, so it will be 'update' unless corrected below.
    
#     my $operation = 'update';
    
#     my ($table_specifier, $parameters) = $edt->_action_args('update', @rest);
    
#     my $action = $edt->_new_action('update', $table_specifier, $parameters);
    
#     $edt->{record_count}++;
    
#     # If the record includes any errors or warnings, import them as conditions.
    
#     if ( my $ew = $action->record_value('_errwarn') )
#     {
# 	$edt->import_conditions($action, $ew);
#     }
    
#     # Execute the following code inside an eval block. If an exception is thrown
#     # during authorization or validation, either from Authorization.pm or
#     # Validation.pm or from a subclass, capture the error and add an E_EXECUTE
#     # condition. This will enable the transaction to keep going if the PROCEED
#     # allowance is present, and will enable client interface code to handle it
#     # as a failed transaction rather than a thrown exception.
    
#     eval {
	
# 	# Check to make sure we have permission to carry out this operation An
# 	# error or caution will be added if the necessary permission cannot be
# 	# established.
	
# 	my $result = $edt->authorize_against_table($action, 'insupdate', $table_specifier);
	
# 	if ( $result eq 'insert' )
# 	{
# 	    $operation = 'insert';
# 	    $action->set_operation('insert');
# 	}
	
# 	# Then check the record to be inserted, making sure that the column values meet all of
# 	# the criteria for this table. Any discrepancies will cause error and/or warning
# 	# conditions to be added.
	
# 	$edt->validate_action($action, $operation, $table_specifier);	
#     };
    
#     # If a exception occurred, write the exception to the error stream and add an error
#     # condition to this action.
    
#     if ( $@ )
#     {
# 	$edt->error_line($@);
# 	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
#     }
    
#     # Regardless of any added conditions, handle the action and return the action
#     # reference.
    
#     $edt->_handle_action($action, $operation);
# }

# sub delete_cleanup {
    
#     my ($edt, @rest) = @_;
    
#     # Create a new object to represent this action.
    
#     my ($table_specifier, $parameters) = $edt->_action_args('delete_cleanup', @rest);
    
#     my $action = $edt->_new_action('delete_cleanup', $table_specifier, $parameters);
    
#     $edt->{record_count}++;
    
#     # Make sure we have a non-empty selector, although we will need to do more checks on it later.
    
#     if ( $action->keyval )
#     {
# 	eval {
# 	    # First check to make sure we have permission to delete records in this table. An
# 	    # error condition will be added if the proper permission cannot be established.
	    
# 	    $edt->authorize_against_table($action, 'delete_cleanup', $table_specifier);
# 	};
	
# 	# If a exception occurred, write the exception to the error stream and add an error
# 	# condition to this action.
	
# 	if ( $@ )
# 	{
# 	    $edt->error_line($@);
# 	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
# 	}
	
# 	# If the selector does not mention the linking column, throw an exception rather than adding
# 	# an error condition. These are static errors that should not be occurring because of bad
# 	# end-user input.
	
# 	if ( my $linkcol = $action->linkcol )
# 	{
# 	    my $keyexpr = $action->keyexpr;
	    
# 	    unless ( $keyexpr =~ /\b$linkcol\b/ )
# 	    {
# 		croak "'$keyexpr' is not a valid selector, must mention $linkcol";
# 	    }
# 	}
	
# 	else
# 	{
# 	    croak "could not find linking column for table $table_specifier";
# 	}
#     }

#     else
#     {
# 	$edt->add_condition($action, 'E_NO_KEY', 'delete_cleanup');
#     }
    
#     # Handle the action and return the action reference.
    
#     return $edt->_handle_action($action, 'delete_cleanup');
# }



# sub delete_cleanup {
    
#     my ($edt, @rest) = @_;
    
#     # Create a new object to represent this action.
    
#     my ($table_specifier, $parameters) = $edt->_action_args('delete_cleanup', @rest);
    
#     my $action = $edt->_new_action('delete_cleanup', $table_specifier, $parameters);
    
#     # If the parameters include any errors or warnings, import them as conditions.
    
#     if ( my $ew = $action->record_value('_errwarn') )
#     {
# 	$edt->import_conditions($action, $ew);
#     }
    
#     # Execute the following code inside an eval block. If an exception is thrown
#     # during authorization or validation, either from Authorization.pm or
#     # Validation.pm or from a subclass, capture the error and add an E_EXECUTE
#     # condition. This will enable the transaction to keep going if the PROCEED
#     # allowance is present, and will enable client interface code to handle it
#     # as a failed transaction rather than a thrown exception.
    
#     eval {
	
# 	# Check to make sure we have permission to carry out this operation.  An
# 	# error or caution will be added if the necessary permission cannot be
# 	# established.
	
# 	$edt->authorize_against_table($action, 'delete_cleanup', $table_specifier);
	
# 	# If authorize_against_table returned 'insert', change the operation.
	
# 	if ( $result eq 'insert' )
# 	{
# 	    $operation = 'insert';
# 	    $action->set_operation('insert');
# 	}
	
# 	# If the operation is not a deletion, check the parameters. Make sure
# 	# that the column values meet all of the criteria for this table.  Any
# 	# discrepancies will cause error and/or warning conditions to be added.
	
# 	if ( $operation !~ /^delete/ )
# 	{
# 	    $edt->validate_action($action, $operation, $table_specifier);
# 	}
#     };
    
#     # If a exception occurred, write the exception to the error stream and add an error
#     # condition to this action.
    
#     if ( $@ )
#     {
# 	$edt->error_line($@);
# 	$edt->add_condition($action, 'E_EXECUTE', 
# 			    'an exception occurred during authorization or validation');
#     }
    
#     # Regardless of any added conditions, handle the action and return the action
#     # reference.
    
#     $edt->_handle_action($action, $operation);
    
#     # Create a new object to represent this action.
    
#     my ($table_specifier, $parameters) = $edt->_action_args('delete_cleanup', @rest);
    
#     my $action = $edt->_new_action('delete_cleanup', $table_specifier, $parameters);
    
#     $edt->{record_count}++;
    
#     # Make sure we have a non-empty selector, although we will need to do more checks on it later.
    
#     if ( $action->keyval )
#     {
# 	eval {
# 	    # First check to make sure we have permission to delete records in this table. An
# 	    # error condition will be added if the proper permission cannot be established.
	    
# 	    $edt->authorize_against_table($action, 'delete_cleanup', $table_specifier);
# 	};
	
# 	# If a exception occurred, write the exception to the error stream and add an error
# 	# condition to this action.
	
# 	if ( $@ )
# 	{
# 	    $edt->error_line($@);
# 	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
# 	}
	
# 	# If the selector does not mention the linking column, throw an exception rather than adding
# 	# an error condition. These are static errors that should not be occurring because of bad
# 	# end-user input.
	
# 	if ( my $linkcol = $action->linkcol )
# 	{
# 	    my $keyexpr = $action->keyexpr;
	    
# 	    unless ( $keyexpr =~ /\b$linkcol\b/ )
# 	    {
# 		croak "'$keyexpr' is not a valid selector, must mention $linkcol";
# 	    }
# 	}
	
# 	else
# 	{
# 	    croak "could not find linking column for table $table_specifier";
# 	}
#     }

#     else
#     {
# 	$edt->add_condition($action, 'E_NO_KEY', 'delete_cleanup');
#     }
    
#     # Handle the action and return the action reference.
    
#     return $edt->_handle_action($action, 'delete_cleanup');
# }


