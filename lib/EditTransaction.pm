# 
# The Paleobiology Database
# 
#   EditTransaction.pm - base class for data acquisition and modification
# 

package EditTransaction;

use strict;

use ExternalIdent qw(%IDP);
use TableDefs qw(get_table_property get_column_properties original_table $PERSON_DATA
		 %COMMON_FIELD_IDTYPE %COMMON_FIELD_OTHER %FOREIGN_KEY_TABLE);
use TableData qw(get_table_schema);
use EditAction;
use Permissions;

use Carp qw(carp croak);
use Try::Tiny;
use Scalar::Util qw(weaken blessed);


# This class is intended to encapsulate the mid-level code necessary for updating records in the
# database in the context of a data service operation or a command-line utility. It handles
# transaction initiation, commitment, and rollback, permission checking, and also error and
# warning conditions.
# 
# This class can be subclassed (see ResourceEdit.pm, TimescaleEdit.pm) in order to provide
# additional logic for checking values and performing auxiliary operations in conjunction with
# database inserts, updates, and deletes.

our ($MULTI_DELETE_LIMIT) = 100;

our (%ALLOW_BY_CLASS) = ( EditTransaction => { CREATE => 1,
					       PROCEED => 1, 
					       KEY_INSERT => 1,
					       MULTI_INSERT => 1,
					       MULTI_DELETE => 1,
					       NO_RECORDS => 1,
					       ALTER_TRAIL => 1 } );

our (%CONDITION_TEMPLATE) = (
		C_CREATE => "Allow 'CREATE' to create records",
		C_NO_RECORDS => "Allow 'NO_RECORDS' to allow transactions with no records",
		E_EXECUTE => "An error occurred while executing '%1'",
		E_NO_KEY => "The %1 operation requires a primary key value",
		E_HAS_KEY => "You may not specify a primary key value for the %1 operation",
		E_KEY_NOT_FOUND => "Field '%1': no %3 record was found with key '%2'",
		E_NOT_FOUND => "No record was found with key '%1'",
		E_PERM => { insert => "You do not have permission to insert a record into this table",
			    update => "You do not have permission to update this record",
			    replace_new => "This record does not exist, ".
				"and you do not have permission to insert it",
			    replace_old => "You do not have permission to replace this record",
			    delete => "You do not have permission to delete this record",
			    default => "You do not have permission for this operation" },
		E_PERM_COL => "You do not have permission to set the value of the field '%1'",
		E_REQUIRED => "Field '%1' must have a nonempty value",
		E_PARAM => "Field '%1': %2",
		W_ALLOW => "Unknown allowance '%1'",
		W_EXECUTE => "An error occurred while executing '%1'",
		UNKNOWN => "MISSING ERROR MESSAGE");


# CONSTRUCTOR and destructor
# --------------------------

# new ( request_or_dbh, perms, table, allows )
# 
# Create a new EditTransaction object, for use in association with the specified request. It is
# also possible to specify a DBI database connection handle, as would typically be done by a
# command-line utility. The second argument should be a Permissions object which has already been
# created, the third a table name, and the fourth a hash of allowed cautions.

sub new {
    
    my ($class, $request_or_dbh, $perms, $table, $allows) = @_;
    
    # Check the arguments.
    
    croak "new EditTransaction: request or dbh is required"
	unless $request_or_dbh && blessed($request_or_dbh);
    
    croak "new EditTransaction: perms is required"
	unless blessed $perms && $perms->isa('Permissions');
    
    # Create a new EditTransaction object, and bless it into the proper class.
    
    my $edt = { perms => $perms,
		main_table => $table || '', 
		allows => { },
		action_list => [ ],
		bad_list => [ ],
		errors => [ ],
		warnings => [ ],
		condition => { },
		current_errors => [ ],
		current_warnings => [ ],
		current_condition => { },
		current_label => undef,
		inserted_keys => { },
		updated_keys => { },
		deleted_keys => { },
		key_labels => { },
		record_count => 0,
		action_count => 0,
		commit_count => 0,
		rollback_count => 0,
		transaction => '',
		state => 'ok' };
    
    bless $edt, $class;
    
    # Store the request, dbh, and debug flag as local fields. Weaken all references, because
    # otherwise those objects might be prevented from being destroyed when they go out of
    # scope. In particular, the $reference object might subsequently be updated to contain a
    # reference to this EditTransaction object and as we all know, circular references can prevent
    # garbage collection unless one of them is weakened.
    
    if ( $request_or_dbh->can('get_connection') )
    {
	$edt->{request} = $request_or_dbh;
	weaken $edt->{request};
	
	$edt->{dbh} = $request_or_dbh->get_connection;
	weaken $edt->{dbh};
	
	$edt->{debug} = $request_or_dbh->debug if $request_or_dbh->can('debug');
    }
    
    else
    {
	$edt->{dbh} = $request_or_dbh;
	weaken $edt->{dbh};
	
	$edt->{debug} = ref $allows eq 'HASH' && $allows->{DEBUG_MODE} ? 1 : 0;
    }
    
    # If we are given either a hash or an array of conditions that should be allowed, store them
    # in the object.
    
    my @allows;
    
    if ( ref $allows eq 'HASH' )
    {
	@allows = grep { $allows->{$_} } keys %$allows;
    }
    
    elsif ( ref $allows eq 'ARRAY' )
    {
	@allows = @$allows;
    }

    elsif ( defined $allows )
    {
	@allows = grep { $_ } split(/\s*,\s*/, $allows);
    }
    
    foreach my $k ( @allows )
    {
	if ( $ALLOW_BY_CLASS{$class}{$k} || $ALLOW_BY_CLASS{EditTransaction}{$k} )
	{
	    $edt->{allows}{$k} = 1;
	}
	
	else
	{
	    $edt->add_condition('W_ALLOW', $k);
	}
    }
    
    return $edt;
}


# If this object is destroyed while a transaction is in progress, roll it back.

sub DESTROY {
    
    my ($edt) = @_;
    
    if ( $edt->{transaction} && $edt->{transaction} eq 'active' )
    {    
	$edt->rollback;
    }
}


# Basic accessor methods
# ----------------------

# These are all read-only.

sub dbh {
    
    return $_[0]->{dbh};
}


sub request {
    
    return $_[0]->{request};
}


sub perms {
    
    return $_[0]->{perms};
}


sub debug {
    
    return $_[0]->{debug};
}


sub role {
    
    return $_[0]->{perms}->role;
}


# Error, caution, and warning conditions
# --------------------------------------

# Error and warning conditions are indicated by codes, all in upper case word symbols. Those that
# start with 'E_' represent errors, those that start with 'C_' represent cautions, and those that
# start with 'W_' represent warnings. In general, errors cause the operation to be aborted while
# warnings do not. Cautions cause the operation to be aborted unless specifically allowed.
# 
# Codes that start with 'C_' indicate cautions that may be allowed, so that the operation proceeds
# despite them. A canonical example is 'C_CREATE', which is returned if records are to be
# created. If the data service operation method knows that records are to be created, it can
# explicitly allow 'CREATE', which will allow the records to be created. Alternatively, it can
# return 'C_CREATE' as an error code to the client-side application, which can ask the user if
# they really want to create new records. If they answer affirmatively, the operation can be
# re-tried with 'CREATE' specifically allowed. The same can be done with other cautions.
# 
# Codes that start with 'E_' indicate conditions that prevent the operation from proceeding. For
# example, 'E_PERM' indicates that the user does not have permission to operate on the specified
# record or table. 'E_NOT_FOUND' indicates that a record to be updated is not in the
# database. Unlike cautions, these conditions cannot be specifically allowed. However, the special
# allowance 'PROCEED' specifies that whatever parts of the operation are able to succeed should be
# carried out, even if some record operations fail.
# 
# Codes that start with 'W_' indicate warnings that should be passed back to the client but do not
# prevent the operation from proceeding.
# 
# Codes that start with 'D_' and 'F_' indicate conditions that would otherwise have been cautions
# or errors, under the 'PROCEED' allowance. These are treated as warnings.
# 
# Allowed conditions must be specified for each EditTransaction object when it is created.


# register_allows ( condition... )
# 
# Register the names of extra conditions that can be allowed for transactions in a particular
# subclass. This class method is designed to be called at startup from a module that subclasses
# this one.

sub register_allows {
    
    my ($class, @names) = @_;
    
    foreach my $n ( @names )
    {
	$ALLOW_BY_CLASS{$class}{$n} = 1;
    }
}


# allows ( condition )
# 
# Returns true if the specified condition is allowed for this EditTransaction, false
# otherwise. The set of allowed conditions was specified when this object was originally created.

sub allows {
    
    return $_[0]->{allows}{$_[1]};
}


# add_condition ( condition, data... )
# 
# Add a condition (error or warning) that pertains to the entire transaction rather than a single
# record. One or more pieces of data will generally also be passed in, which can be used later by
# code in the data service operation module to generate an error or warning message to return to
# the user. Since these conditions apply to the transaction as a whole, they cannot be ignored
# with 'PROCEED'.

sub add_condition { 
    
    my ($edt, $code, @data) = @_;
    
    my $condition = EditCondition->new($code, undef, undef, @data);
    
    if ( $code =~ qr{ ^ [EC] _ }xs )
    {
	push @{$edt->{errors}}, $condition;
    }
    
    elsif ( $code =~ qr{ ^ W_ }xs )
    {
	push @{$edt->{warnings}}, $condition;
    }
    
    else
    {
	croak "bad condition '$code'";
    }
    
    $edt->{condition}{$code} = 1;
    
    return 1;
}


# errors ( )
# 
# Return the list of errors for the current EditTransaction. In numeric context, Perl will simply
# evaluate this as a number. In boolean context, this will be evaluated as true if there are any and
# false if not. This is one of my favorite features of Perl.

sub errors {

    return @{$_[0]->{errors}};
}


# warnings ( )
# 
# Return the list of warnings for the current EditTransaction.

sub warnings {
    
    return @{$_[0]->{warnings}};
}


# add_record_condition ( condition, data... )
# 
# Add a condition (error or warning) that pertains to the current record.

sub add_record_condition {
    
    my ($edt, $code, $table, @data) = @_;
    
    croak "this call requires that a record operation be initiated first" unless defined $edt->{current_label};
    
    my $condition = EditCondition->new($code, $edt->{current_label}, $table, @data);
    
    if ( $code =~ qr{ ^ [EC] _ }xs )
    {
	push @{$edt->{current_errors}}, $condition;
    }
    
    elsif ( $code =~ qr{ ^ W_ }xs )
    {
	push @{$edt->{current_warnings}}, $condition;
    }
    
    else
    {
	croak "bad condition '$code'";
    }
    
    return $edt;
}


# record_errors ( )
# 
# Return the list of errors (not warnings) for the current record. This is used below to test
# whether or not we can proceed with the current record.

sub record_errors {
    
    return @{$_[0]->{current_errors}};
}


# record_warnings ( )
# 
# Return the list of warnings for the current record. This is only here in case it is needed by a
# subroutine defined by some subclass.

sub record_warnings {

    return @{$_[0]->{current_warnings}};
}


# generate_msg ( condition )
#
# This routine generates an error message from a condition record.

sub generate_msg {
    
    my ($edt, $condition) = @_;
    
    # Extract the necessary information from the condition record.
    
    my $code = $condition->code;
    my $table = $condition->table;
    my @params = $condition->data;
    
    # If the code was altered because of the PROCEED allowance, change it back
    # so we can look up the proper template.
    
    my $lookup = $code;
    substr($lookup,0,1) =~ tr/DF/CE/;
    
    # Look up the template according to the specified, code, table, and first
    # parameter. The method called may be overridden by a subclass, in order
    # to handle codes that we do not know about.
    
    my $template = $edt->get_condition_template($lookup, $table, $params[0]);
    
    # Then generate the message.
    
    return $edt->substitute_msg($code, $table, $template, @params);
}


# get_condition_template ( code, table, selector )
#
# Given a code, a table, and an optional selector string, return a message template.  This method
# is designed to be overridden by subclasses, but the override methods must call
# SUPER::get_condition_template if they cannot find a template for their particular class that
# corresponds to the information they are given.

sub get_condition_template {

    my ($edt, $code, $table, $selector) = @_;
    
    if ( ref $CONDITION_TEMPLATE{$code} eq 'HASH' )
    {
	if ( $selector && $CONDITION_TEMPLATE{$code}{$selector} )
	{
	    return $CONDITION_TEMPLATE{$code}{$selector};
	}
	
	elsif ( $CONDITION_TEMPLATE{$code}{default} )
	{
	    return $CONDITION_TEMPLATE{$code}{default};
	}
	
	else
	{
	    return $CONDITION_TEMPLATE{'UNKNOWN'};
	}
    }
    
    elsif ( $CONDITION_TEMPLATE{$code} )
    {
	return $CONDITION_TEMPLATE{$code};
    }
    
    else
    {
	return $CONDITION_TEMPLATE{'UNKNOWN'};
    }
}    


# substitute_msg ( code, table, template, params... )
#
# Generate a message string using the specified elements. The message template may include any of
# the following symbols:
# 
# %t		substitute the name of the database table that was being operated on
# %1..%9	substitute one of the parameters associated with the error

sub substitute_msg {
    
    my ($edt, $code, $table, $template, @params) = @_;
    
    # If we have a non-empty template, then substitute all of the symbols that appear in it.
    
    if ( defined $template && $template ne '' )
    {
	
	my $message = $template;
	
	foreach my $t ( @params )
	{
	    if ( length($t) > 80 )
	    {
		$t = substr($t,0,80) . '...';
	    }
	}
	
	$message =~ s{ [%]t }{ $table }xs;
	$message =~ s{ [%](\d) }{ $params[$1-1] }xseg;
	
	if ( $code eq 'E_PARAM' && defined $params[2] && $params[2] ne '' )
	{
	    $message .= ", was '$params[2]'";
	}
	
	return $message;
    }

    # Otherwise, return the empty string.
    
    else
    {
	return '';
    }
}


# Record sequencing
# -----------------

# _new_record {
# 
# Prepare for a new record operation. This includes moving any record errors and warnings to the
# main error and warning lists. It also determines the label to be used in reporting any
# conditions about this record. This is intended to be a private method, called only from within
# this class.

sub _new_record {
    
    my ($edt, $table, $operation, $record) = @_;
    
    croak "no record specified" unless ref $record eq 'HASH' ||
	$operation eq 'delete' && defined $record && $record ne '';
    
    # If there are any errors and warnings pending from the previous record, move them to the main
    # lists.
    
    $edt->_finish_record;
    
    # Then determine a label for this record. If one is specified, use that. Otherwise, keep count
    # of how many records we have seen so far and use that prepended by '#'.
    
    $edt->{record_count}++;
    
    if ( defined $record->{record_label} && $record->{record_label} ne '' )
    {
	$edt->{current_label} = $record->{record_label};
    }
    
    else
    {
	$edt->{current_label} = '#' . $edt->{record_count};
    }
    
    # Then create a new action record and return it.
    
    return EditAction->new($table, $operation, $record, $edt->{current_label});
}


# _finish_record ( )
# 
# Finish processing the current record. All record conditions are moved over to the main lists,
# and the 'current_label' is set to undefined.

sub _finish_record {
    
    my ($edt) = @_;
    
    if ( @{$edt->{current_errors}} )
    {
	if ( $edt->allows('PROCEED') )
	{
	    while ( my $e = shift @{$edt->{current_errors}} )
	    {
		substr($e->[0],0,1) =~ tr/CE/DF/;
		push @{$edt->{warnings}}, $e;
		$edt->{condition}{$e->[0]} = 1;
	    }
	}
	
	elsif ( $edt->allows('NOT_FOUND') )
	{
	    while ( my $e = shift @{$edt->{current_errors}} )
	    {
		if ( $e->[0] eq 'E_NOT_FOUND' )
		{
		    $e->[0] = 'F_NOT_FOUND';
		    push @{$edt->{warnings}}, $e;
		}
		
		else
		{
		    push @{$edt->{errprs}}, $e;
		}
		
		$edt->{condition}{$e->[0]} = 1;
	    }
	}
	
	else
	{
	    while ( my $e = shift @{$edt->{current_errors}} )
	    {
		push @{$edt->{errors}}, $e;
		$edt->{condition}{$e->[0]} = 1;
	    }
	}
    }
    
    while ( my $w = shift @{$edt->{current_warnings}} )
    {
	push @{$edt->{warnings}}, $w;
    }
    
    $edt->{current_label} = undef;
}


# _clear_record ( )
# 
# Clear any error and warning messages generated by the current record, and also the record
# label. This method is called when processing of a record is to be abandoned.

sub _clear_record {
    
    my ($edt) = @_;
    
    @{$edt->{curremt_errors}} = ();
    @{$edt->{current_warnings}} = ();
    $edt->{current_label} =  undef;
}


# record_label ( )
# 
# Return the record label for the record currently being processed. This is valid both during
# checking and execution.

sub record_label {

    return $_[0]->{current_label};
}

# Transaction control
# -------------------

# start_transaction ( )
# 
# Start the database transaction. This is done automatically when 'execute' is called, but can
# be done explicitly at an earlier stage if the checking of record values needs to be done
# inside a transaction.

sub start_transaction {
    
    my ($edt) = @_;
    
    if ( $edt->{transaction} eq 'active' )
    {
	print STDERR " WARNING: transaction already active\n\n" if $edt->debug;
	return;
    }
    
    my $label = $edt->role eq 'guest' ? '(guest) ' : '';
    
    print STDERR " >>> START TRANSACTION $label\n\n" if $edt->debug;
    
    $edt->dbh->do("START TRANSACTION");
    $edt->{transaction} = 'active';
    
    # Call the 'begin_transaction' method, which is designed to be overridden by subclasses. The
    # default method does nothing.
    
    $edt->begin_transaction($edt->{main_table});
    
    return $edt;
}


# commit ( )
# 
# Commit the database transaction. After this is done, this EditTransaction cannot be used for any
# more actions. If the operation method needs to make more changes to the database, a new
# EditTransaction must be created.
# 
# $$$ Perhaps I will later modify this class so that it can be used for multiple transactions in turn.

sub commit {
    
    my ($edt) = @_;
    
    print STDERR " <<< COMMIT TRANSACTION\n\n" if $edt->debug;
	
    $edt->dbh->do("COMMIT");
    $edt->{transaction} = 'committed';
    $edt->{commit_count}++;
    
    return $edt;
}


sub rollback {

    my ($edt) = @_;
    
    print STDERR " <<< ROLLBACK TRANSACTION\n\n" if $edt->debug;
	
    $edt->dbh->do("ROLLBACK");
    $edt->{transaction} = 'aborted';
    $edt->{rollback_count}++;
    
    return $edt;
}


# Record operations
# -----------------

# The operations in this section are called by data service operation methods to insert, update,
# and delete records in the database.


# start_execution
# 
# Call 'start_transaction' and also set the 'execute_immediately' flag. This means that subsequent
# actions will be carried out immediately on the database rather than waiting for a call to
# 'execute'.

sub start_execution {
    
    my ($edt) = @_;
    
    $edt->start_transaction;
    $edt->{execute_immediately} = 1;
}


# insert_record ( table, record )
# 
# The specified record is to be inserted into the specified table. Depending on the settings of
# this particular EditTransaction, this action may happen immediately or may be executed
# later. The record in question must NOT include a primary key value.

sub insert_record {
    
    my ($edt, $table, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and initialize
    # the action object for the record being inserted.
    
    my $action = $edt->_new_record($table, 'insert', $record);
    
    # We can only create records if specifically allowed. This may be specified by the user as a
    # parameter to the operation being executed, or it may be set by the operation method itself
    # if the operation is specifically designed to create records.
    
    if ( $edt->allows('CREATE') )
    {
	# First check to make sure we have permission to insert a record into this table. A
	# subclass may override this method, if it needs to make different checks than the default
	# ones.
	
	my $permission = $edt->authorize_action($action, 'insert', $table);
	
	# If the user does not have permission to add a record, add an error condition.
	
	if ( $permission ne 'post' && $permission ne 'admin' )
	{
	    $edt->add_record_condition('E_PERM', $table, 'insert');
	}
	
	# A record to be inserted must not have a primary key value specified for it. Records with
	# primary key values can only be passed to 'update_record' or 'replace_record'.
	
	if ( $action->keyval )
	{
	    $edt->add_record_condition('E_HAS_KEY', $table, 'insert');
	}
	
	# Then check the actual record to be inserted, to make sure that the column values meet
	# all of the criteria for this table. If any error or warning conditions are detected,
	# they are added to the current transaction. A subclass may override this method, if it
	# needs to make additional checks or perform additional work.
	
	$edt->validate_action($action, 'insert', $table);
    }
    
    # If an attempt is made to add a record without the 'CREATE' allowance, add the appropriate
    # error condition.
    
    else
    {
	$edt->add_record_condition('C_CREATE', $table);
    }
    
    # Either execute the action immediately or add it to the appropriate list depending on whether
    # or not any error conditions are found.
    
    return $edt->_handle_action($action);
}


# update_record ( table, record )
# 
# The specified record is to be updated in the specified table. Depending on the settings of this
# particular EditTransaction, this action may happen immediately or may be executed later. The
# record in question must include a primary key value, indicating which record to update.

sub update_record {
    
    my ($edt, $table, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being updated.
    
    my $action = $edt->_new_record($table, 'update', $record);
    
    # We can only update a record if a primary key value is specified.
    
    if ( my $keyexpr = $edt->get_keyexpr($action) )
    {
	# First check to make sure we have permission to update this record. A subclass may
	# override this method, if it needs to make different checks than the default ones.
	
	my $permission = $edt->authorize_action($action, 'update', $table, $keyexpr);
	
	# If no such record is found in the database, add an error condition. If this
	# EditTransaction has been created with the 'PROCEED' or 'NOT_FOUND' allowance, it
	# will automatically be turned into a warning and will not cause the transaction to be
	# aborted.
	
	if ( $permission eq 'notfound' )
	{
	    $edt->add_record_condition('E_NOT_FOUND', $table, $action->keyval);
	}
	
	# If the user does not have permission to edit the record, add an error condition. 
	
	elsif ( $permission ne 'edit' && $permission ne 'admin' )
	{
	    $edt->add_record_condition('E_PERM', $table, 'update');
	}
	
	# Then check the new record values, to make sure that the column values meet all of the
	# criteria for this table. If any error or warning conditions are detected, they are added
	# to the current transaction. A subclass may override this method, if it needs to make
	# additional checks or perform additional work.
	
	$edt->validate_action($action, 'update', $table, $keyexpr);
    }
    
    # If no primary key value was specified for this record, add an error condition. This will, of
    # course, have to be reported under the record label that was passed in as part of the record
    # (if one was in fact given) or else the index of the record in the input set.
    
    else
    {
	$edt->add_record_condition('E_NO_KEY', $table, 'update');
    }
    
    # Either execute the action immediately or add it to the appropriate list depending on whether
    # or not any error conditions are found.
    
    return $edt->_handle_action($action);
}


# replace_record ( table, record )
# 
# The specified record is to be inserted into the specified table, replacing any record that may
# exist with the same primary key value. Depending on the settings of this particular EditTransaction,
# this action may happen immediately or may be executed later. The record in question must include
# a primary key value.

sub replace_record {
    
    my ($edt, $table, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being replaced.
    
    my $action = $edt->_new_record($table, 'replace', $record);
    
    # We can only replace a record if a primary key value is specified.
    
    if ( my $keyexpr = $edt->get_keyexpr($action) )
    {
	# First check to make sure we have permission to replace this record. A subclass may
	# override this method, if it needs to make different checks than the default ones.
	
	my $permission = $edt->authorize_action($action, 'replace', $table, $keyexpr);
	
	# If no such record is found in the database, check to see if this EditTransaction allows
	# CREATE. If this is the case, and if the user also has 'admin' permission on this table,
	# then a new record will be created with the specified primary key value. Otherwise, an
	# appropriate error condition will be added.
	
	if ( $permission eq 'notfound' )
	{
	    if ( $edt->allows('CREATE') )
	    {
		if ( $edt->check_table_permission($table, 'admin') eq 'admin' )
		{
		    $permission = 'admin';
		    $action->set_permission($permission);
		}
		
		else
		{
		    $edt->add_record_condition('E_PERM', $table, 'replace_new');
		}
	    }
	    
	    # If we are not allowed to create new records, add an error condition. If this
	    # EditTransaction has been created with the PROCEED or NOT_FOUND allowance, it
	    # will automatically be turned into a warning and will not cause the transaction to be
	    # aborted.
	    
	    else
	    {
		$edt->add_record_condition('E_NOT_FOUND', $table, $action->keyval);
	    }
	}
	
	# If the user does not have permission to edit the record, add an error condition. 
	
	elsif ( $permission ne 'edit' && $permission ne 'admin' )
	{
	    $edt->add_record_condition('E_PERM', $table, 'replace_old');
	}
	
	# Then check the new record values, to make sure that the replacement record meets all of
	# the criteria for this table. If any error or warning conditions are detected, they are
	# added to the current transaction. A subclass may override this method, if it needs to
	# make additional checks or perform additional work.
	
	$edt->validate_action($action, 'replace', $table, $keyexpr);
    }
    
    # If no primary key value was specified for this record, add an error condition. This will, of
    # course, have to be reported under the record label that was passed in as part of the record
    # (if one was in fact given) or else the index of the record in the input set.
    
    else
    {
	$edt->add_record_condition('E_NO_KEY', $table, 'replace');
    }
    
    # Create an action record, and either execute it immediately or add it to the appropriate list
    # depending on whether or not any error conditions are found.
    
    return $edt->_handle_action($action);
}


# delete_record ( table, record )
# 
# The specified record is to be deleted from the specified table. Depending on the settings of
# this particular EditTransaction, this action may happen immediately or may be executed
# later. The record in question must include a primary key value, indicating which record to
# delete. In fact, for this operation only, the $record argument may be a key value rather than a
# hash ref.

sub delete_record {

    my ($edt, $table, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being deleted.
    
    my $action = $edt->_new_record($table, 'delete', $record);
    
    # A record can only be deleted if a primary key value is specified.
    
    if ( my $keyexpr = $edt->get_keyexpr($action) )
    {
	# First check to make sure we have permission to delete this record. A subclass may
	# override this method, if it needs to make different checks than the default ones.
	
	my $permission = $edt->authorize_action($action, 'delete', $table, $keyexpr);
	
	# If no such record is found in the database, add an error condition. If this
	# EditTransaction has been created with the 'PROCEED' or 'NOT_FOUND' allowance, it
	# will automatically be turned into a warning and will not cause the transaction to be
	# aborted.
	
	if ( $permission eq 'notfound' )
	{
	    $edt->add_record_condition('E_NOT_FOUND', $table, $action->keyval);
	}
	
	# If we do not have permission to delete the record, add an error condition.
	
	elsif ( $permission ne 'delete' && $permission ne 'admin' )
	{
	    $edt->add_record_condition('E_PERM', $table, 'delete');
	}
	
	# If a 'validate_delete' method was specified, then call it. This method may abort the
	# deletion by adding an error condition. Otherwise, we assume that the permission check we
	# have already done is all that is necessary.
	
	$edt->validate_action($action, 'delete', $table, $keyexpr);
    }
    
    # If no primary key was specified, add an error condition.
    
    else
    {
	$edt->add_record_condition('E_NO_KEY', $table, 'delete');
    }
    
    # Create an action record, and then take the appropriate action.
    
    return $edt->_handle_action($action);
}


# insert_update_record ( table, record )
# 
# Call either 'insert_record' or 'update_record', depending on whether the record has a value for
# the primary key attribute. This is a convenient shortcut for use by operation methods.

sub insert_update_record {
    
    my ($edt, $table, $record) = @_;
    
    if ( EditAction->get_record_key($table, $record) )
    {
	return $edt->update_record($table, $record);
    }
    
    else
    {
	return $edt->insert_record($table, $record);
    }
}


# ignore_record ( )
# 
# Indicates that a particular record that was sent by the user should be ignored. This will keep
# the record count up-to-date for generating record labels with which to tag subsequent error and
# warning messages.

sub ignore_record {

    my ($edt, $table, $record) = @_;
    
    $edt->{record_count}++;
}


# abandon_record ( )
# 
# This method may be called from record validation routines defined in subclasses of
# EditTransaction, if it is determined that a particular record action should be skipped but the
# rest of the transaction should proceed.

sub abandon_record {
    
    my ($edt) = @_;
    
    $edt->_clear_record;
}


# authorize_action ( action, table, operation, keyexpr )
# 
# Determine whether the current user is authorized to perform the specified action. If so, store
# the indicated permission in the action record and also return it. For any operation but 'insert'
# a key expression is provided. The return value should be one of the following:
# 
# admin		the user has administrative privilege on the table, so the action is authorized
# post		the user is authorized to add new records to the table
# edit		the user is authorized to update or replace the specified record
# delete	the user is authorized to delete the specified record
# none		the user is not authorized to perform this action
# notfound	no record was found corresponding to the specified key expression
# 
# This method may be overridden by subclasses of this class. Override methods should indicate
# error and warning conditions by calling the method 'add_record_condition'.

sub authorize_action {
    
    my ($edt, $action, $operation, $table, $keyexpr) = @_;
    
    if ( $operation eq 'insert' )
    {
	return $action->set_permission($edt->check_table_permission($table, 'post'))
    }
    
    elsif ( $operation eq 'update' || $operation eq 'replace' )
    {
	return $action->set_permission($edt->check_record_permission($table, 'edit', $keyexpr));
    }
    
    elsif ( $operation eq 'delete' )
    {
	return $action->set_permission($edt->check_record_permission($table, 'delete', $keyexpr));
    }
    
    else
    {
	croak "bad operation '$operation'";
    }
}


# _handle_action ( action )
# 
# Handle the specified action record. If errors were generated for this record, put it on the 'bad
# record' list. Otherwise, either execute it immediately or put it on the action list to be
# executed later.

sub _handle_action {

    my ($edt, $action) = @_;
    
    # If any errors were already generated for the record currently being processed, put this
    # action on the 'bad action' list and otherwise do nothing.
    
    if ( $edt->record_errors )
    {
	push @{$edt->{bad_list}}, $action;
	$edt->{fail_count}++;
	
	my $keyval = $action->keyval;
	$edt->{failed_keys}{$keyval} = 1 if defined $keyval && $keyval ne '';
	
	return;
    }
    
    # If errors were generated for previous records, then there is no point in proceeding with
    # this action since the edit transaction will either never be started or will be subsequently
    # rolled back. Since we already know that no errors were generated for this particular record,
    # there is nothing more that needs to be done.
    
    elsif ( $edt->errors )
    {
	return;
    }
    
    # If the 'execute immediately' flag has been turned on, then execute this action now.
    
    elsif ( $edt->{execute_immediately} )
    {
	$edt->execute_action($action);
	return;
    }

    # Otherwise, we push it on the action list for later execution.
    
    else
    {
	push @{$edt->{action_list}}, $action;
	return;
    }
}


sub execute_action {
    
    my ($edt, $action) = @_;
    
    my $operation = $action->operation;
    
    if ( $operation eq 'insert' )
    {
	return $edt->_execute_insert($action);
    }
    
    elsif ( $operation eq 'update' )
    {
	return $edt->_execute_update($action);
    }
    
    elsif ( $operation eq 'replace')
    {
	return $edt->_execute_replace($action);
    }
    
    elsif ( $operation eq 'delete' )
    {
	return $edt->_execute_delete($action);
    }
    
    else
    {
	croak "bad operation '$operation'";
    }
    
    return;
}


# execute ( )
# 
# Start a database transaction, if one has not already been started. Then execute all of the
# pending insert/update/delete operations, and then either commit or rollback as
# appropriate. Returns true on success, false otherwise.

sub execute {
    
    my ($edt) = @_;
    
    # Finish processing of the final record that was added to the action list, if any.
    
    $edt->_finish_record;
    
    # If errors have already occurred (i.e. when records were checked for insertion or updating),
    # then return without doing anything. If a transaction is already active, then roll it back.
    
    if ( my $error_count = scalar($edt->errors) )
    {
	$edt->end_transaction($edt->{main_table}, $error_count);
	$edt->rollback if $edt->{transaction} eq 'active';
	return;
    }
    
    # If there are no actions to do, and none have been done so far, then rollback any transaction
    # and return unless the NO_RECORDS condition is allowed.
    
    unless ( @{$edt->{action_list}} || $edt->{action_count} )
    {
	unless ( $edt->allows('NO_RECORDS' ) )
	{
	    $edt->end_transaction($edt->{main_table}, 0);
	    $edt->rollback if $edt->{transaction} eq 'active';
	    $edt->add_condition('C_NO_RECORDS');
	    return;
	}
    }
    
    # The main part of this routine is executed inside a try block, so that we can roll back the
    # transaction if any errors occur.
    
    my $result;
    my $end_transaction_called;
    
    try {
	
	# If we haven't already executed 'start_transaction' on the database, do so now.
	
	$edt->start_transaction unless $edt->{transaction} eq 'active';
	
	# Then go through the action list and execute each action in turn. If there are multiple
	# inserts or deletes in a row on the same table, handle them with a single call for
	# efficiency.
	
	while ( my $action = shift @{$edt->{action_list}} )
	{
	    last if $edt->errors;
	    
	    my $operation = $action->operation;
	    
	    $edt->{current_label} = $action->label;
	    
	    if ( $operation eq 'insert' )
	    {
		# push @records, $record;
		
		# while ( $edt->{action_list}[0] && $edt->{action_list}[0][0] eq 'insert' &&
		# 	$edt->{action_list}[0][1] eq $table &&
		#         ref $edt->{action_list}[0][2] eq 'HASH' )
		# {
		#     my $next_action = shift @{$edt->{action_list}};
		#     push @records, $next_action->[2];
		# }
		
		$edt->_execute_insert($action);
	    }
	    
	    elsif ( $operation eq 'update' )
	    {
		$edt->_execute_update($action);
	    }
	    
	    elsif ( $operation eq 'replace' )
	    {
		$edt->_execute_replace($action);
	    }
	    
	    elsif ( $operation eq 'delete' )
	    {
		# If we are allowing multiple deletion and there are more actions remaining, check
		# to see if any of them are also deletes on the same table and with the same
		# permission. If so, coalesce them all into one action.
		
		if ( $edt->allows('MULTI_DELETE') && @{$edt->{action_list}} )
		{
		    my @additional;
		    my $table = $action->table;
		    my $permission = $action->permission;
		    
		    while ( my $next = $edt->{action_list}[0] )
		    {
			if ( $next->operation eq 'delete' && 
			     $next->table eq $table &&
			     $next->permission eq $permission )
			{
			    push @additional, shift(@{$edt->{action_list}});
			    last unless @additional < $MULTI_DELETE_LIMIT;
			}
			
			else
			{
			    last;
			}
		    }
		    
		    if ( @additional )
		    {
			$action->_coalesce(@additional);
		    }
		}
		
		# Now execute the action.
		
		$edt->_execute_delete($action);
	    }
	}
	
	# Call the 'end_transaction' method, which is designed to be overridden by subclasses. The
	# default does nothing. We have to remember that we have called this method, so it doesn't
	# get called again if an exception is thrown.
	
	$end_transaction_called = 1;
	
	$edt->end_transaction($edt->{main_table}, scalar($edt->errors));
	
	# If any errors have occurred, we roll back the transaction. We have to call $edt->errors
	# again, because the end_transaction method might have added an error condition.
	
	if ( $edt->errors )
	{
	    $edt->rollback;
	}
	
	# Otherwise, we're good to go! Yay!
	
	else
	{
	    $edt->commit;
	    $result = 1;
	}
    }
    
    # If an exception is caught, we roll back the transaction and add an error condition.
    
    catch {

	$edt->end_transaction($edt->{main_table}, 1) unless $end_transaction_called;
	$edt->rollback;
	print STDERR "$_\n\n";
	$edt->add_condition('E_EXECUTE', 'execute');
    };
    
    return $result;
}


# get_keyexpr ( action )
# 
# Generate a key expression for the specified action, that will select the particular record being
# acted on. If the action has no key value (i.e. is an 'insert' operation) then return the
# undefined value.

sub get_keyexpr {
    
    my ($edt, $action) = @_;
    
    my $keycol = $action->keycol;
    my $keyval = $action->keyval;

    return '0' unless $keycol;
    
    if ( $action->is_multiple )
    {
	my $dbh = $edt->dbh;
	my @keys = map { $dbh->quote($_) } $action->all_keys;
	
	return "$keycol in (" . join(',', @keys) . ")";
    }
    
    elsif ( defined $keyval && $keyval ne '' )
    {
	return "$keycol=" . $edt->dbh->quote($keyval);
    }
    
    else
    {
	return '0';
    }
}


sub get_keylist {

    my ($edt, $action) = @_;
    
    my $keycol = $action->keycol;
    my $keyval = $action->keyval;
    
    return unless $keycol;
    
    if ( $action->is_multiple )
    {
	my $dbh = $edt->dbh;
	my @keys = map { $dbh->quote($_) } $action->all_keys;
	
	return join(',', @keys);
    }
    
    elsif ( defined $keyval && $keyval ne '' )
    {
	return $edt->dbh->quote($keyval);
    }
    
    else
    {
	return '';
    }
}


# set_permission ( action, key_expr )
# 
# Determine the current user's permission to do the specified action.

sub set_permission {
    
    my ($edt, $action, $keyexpr) = @_;
    
    my $operation = $action->operation;
    my $table = $action->table;
    
    if ( $operation eq 'insert' )
    {
	return $action->set_permission($edt->check_table_permission($table, 'post'))
    }
    
    elsif ( $operation eq 'update' || $operation eq 'replace' )
    {
	$keyexpr ||= $edt->get_keyexpr($action);
	return $action->set_permission($edt->check_record_permission($table, 'edit', $keyexpr));
    }
    
    elsif ( $operation eq 'delete' )
    {
	$keyexpr ||= $edt->get_keyexpr($action);
	return $action->set_permission($edt->check_record_permission($table, 'delete', $keyexpr));
    }
    
    else
    {
	croak "bad operation '$operation'";
    }
}


# _execute_insert ( action )
# 
# Actually perform an insert operation on the database. The record keys and values have been
# checked by 'validate_record' or some other code, and lists of columns and values generated.

sub _execute_insert {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    # Start by calling the 'before_action' method. This is designed to be overridden by
    # subclasses, and can be used to do any necessary auxiliary actions to the database. The
    # default method does nothing.
    
    $edt->before_action($action, 'insert', $table);
    
    # Check to make sure that we actually have column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition('E_EXECUTE', 'internal error: column/value error');
	return;
    }
    
    # Construct the INSERT statement.
    
    my $dbh = $edt->dbh;
    
    my $column_list = join(',', @$cols);
    my $value_list = join(',', @$vals);
    
    my $sql = "	INSERT INTO $table ($column_list)
		VALUES ($value_list)";
    
    print STDERR "$sql\n\n" if $edt->debug;
    
    my ($result, $new_keyval);
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    try {
	
	$result = $dbh->do($sql);
	
	if ( $result )
	{
	    $new_keyval = $dbh->last_insert_id(undef, undef, undef, undef);
	}
	
	if ( $new_keyval )
	{
	    $action->set_keyval($new_keyval);
	}
	
	else
	{
	    $edt->add_record_condition('E_EXECUTE', $table, 'insert'); # $$$
	    $result = undef;
	}
    }
    
    catch {
	
	$edt->add_record_condition('E_EXECUTE', $table, 'insert');
    };
    
    # Now call the 'after_action' method. This is designed to be overridden by subclasses, and can
    # be used to do any necessary auxiliary actions to the database. If the insert succeeded, the
    # $result parameter will contain the new primary key value. Otherwise, it will be
    # undefined. The default method does nothing.
    
    $edt->after_action($action, 'insert', $table, $result ? $new_keyval : undef);
    
    # If the insert succeeded, return the new primary key value. Also record this value so that it
    # can be queried for later. Otherwise, return undefined.
    
    if ( $new_keyval )
    {
	$edt->{action_count}++;
	$edt->{inserted_keys}{$new_keyval} = 1;
	$edt->{key_labels}{$new_keyval} = $action->label;
	return $new_keyval;
    }
    
    else
    {
	$edt->{fail_count}++;
	return undef;
    }
}


# _execute_replace ( table, record )
# 
# Actually perform an replace operation on the database. The record keys and values have been
# checked by 'validate_record' or some other code, and lists of columns and values generated.

sub _execute_replace {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    # Start by calling the 'before_action' method. This is designed to be overridden by
    # subclasses, and can be used to do any necessary auxiliary actions to the database. The
    # default method does nothing.
    
    $edt->before_action($action, 'replace', $table);
    
    # Check to make sure that we actually have column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition('E_EXECUTE', 'internal error: column/value error');
	return;
    }
    
    # Construct the REPLACE statement.
    
    my $dbh = $edt->dbh;
    
    my $column_list = join(',', @$cols);
    my $value_list = join(',', @$vals);
    
    my $sql = "	REPLACE INTO $table ($column_list)
		VALUES ($value_list)";
    
    print STDERR "$sql\n\n" if $edt->debug;
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    my $result;
    
    try {
	
	$result = $dbh->do($sql);
	
	unless ( $result )
	{
	    $edt->add_record_condition('E_EXECUTE', $table, 'replace');
	}
    }
    
    catch {
	
	$edt->add_record_condition('E_EXECUTE', $table, 'replace');
    };
    
    # Now call the 'after_action' method. This is designed to be overridden by subclasses, and can
    # be used to do any necessary auxiliary actions to the database. The default method does nothing.
    
    my $keyval = $action->keyval;
    
    $edt->after_action($action, 'replace', $table, $result);
    
    # If the replace succeeded, return true. Otherwise, return false. In either case, record the
    # mapping between key value and record label.
    
    $edt->{key_labels}{$keyval} = $action->label;
    
    if ( $result )
    {
	$edt->{action_count}++;
	$edt->{replaced_keys}{$keyval} = 1;
	return $result;
    }
    
    else
    {
	$edt->{fail_count}++;
	$edt->{failed_keys}{$keyval} = 1;
	return undef;
    }
}


# _execute_update ( table, record )
# 
# Actually perform an update operation on the database. The keys and values have been checked
# previously.

sub _execute_update {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    # Start by calling the 'before_action' method. This is designed to be overridden by
    # subclasses, and can be used to do any necessary auxiliary actions to the database. The
    # default method does nothing.
    
    $edt->before_action($action, 'update', $table);
    
    # Check to make sure that we actually have column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition('E_EXECUTE', 'internal error: column/value error');
	return;
    }
    
    # Construct the UPDATE statement.
    
    my $dbh = $edt->dbh;
    my $set_list = '';
    
    foreach my $i ( 0..$#$cols )
    {
	$set_list .= ', ' if $set_list;
	$set_list .= "$cols->[$i]=$vals->[$i]";
    }
    
    my $key_expr = $edt->get_keyexpr($action);
    
    my $sql = "	UPDATE $table SET $set_list
		WHERE $key_expr";
    
    print STDERR "$sql\n\n" if $edt->debug;
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    my ($result);
    
    try {
	
	$result = $dbh->do($sql);
	
	unless ( $result )
	{
	    $edt->add_record_condition('E_EXECUTE', $table, 'update');
	}
	
	# $$$ we maybe should set RaiseError instead?
    }
    
    catch {
	
	$edt->add_record_condition('E_EXECUTE', $table, 'update');
    };
    
    # Now call the 'after_action' method. This is designed to be overridden by subclasses, and can
    # be used to do any necessary auxiliary actions to the database. The default method does nothing.
    
    $edt->after_action($action, 'update', $table, $result);
    
    # If the update succeeded, return true. Otherwise, return false. In either case, record the
    # mapping between key value and record label.
    
    my $keyval = $action->keyval;
    
    $edt->{key_labels}{$keyval} = $action->label;
    
    if ( $result )
    {
	$edt->{action_count}++;
	$edt->{updated_keys}{$keyval} = 1;
	return $result;
    }
    
    else
    {
	$edt->{fail_count}++;
	$edt->{failed_keys}{$keyval} = 1;
	return undef;
    }
}


# _execute_delete ( table, record )
# 
# Actually perform a delete operation on the database. The only field that makes any difference
# here is the primary key.

sub _execute_delete {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    # Start by calling the 'before_action' method. This is designed to be overridden by
    # subclasses, and can be used to do any necessary auxiliary actions to the database. The
    # default method does nothing.    
    
    $edt->before_action($action, 'delete', $table);
    
    # Construct the DELETE statement.
    
    my $dbh = $edt->dbh;
    
    my $key_expr = $edt->get_keyexpr($action);
    
    my $sql = "	DELETE FROM $table WHERE $key_expr";
    
    print STDERR "$sql\n\n" if $edt->debug;
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED.
    
    my ($result);
    
    try {
	
	$result = $dbh->do($sql);
	
	unless ( $result )
	{
	    $edt->add_record_condition('E_EXECUTE', $table, 'delete');
	}
	
	# $$$ we maybe should set RaiseError instead?
    }
    
    catch {
	
	$edt->add_record_condition('E_EXECUTE', $table, 'delete');
    };
    
    # Now call the 'after_action' method. This is designed to be overridden by subclasses, and can
    # be used to do any necessary auxiliary actions to the database. The default method does nothing.
    
    $edt->after_action($action, 'delete', $table, $result);
    
    # If the delete succeeded, return true. Otherwise, return false. In either case, record the
    # mapping between key value and record label.
    
    my ($count, @keys, @labels, $has_labels);
    
    if ( $action->is_multiple )
    {
	$count = $action->count;
	@keys = $action->all_keys;
	@labels = $action->all_labels;
	$has_labels = $action->has_labels;
    }
    
    else
    {
	$count = 1;
	push @keys, $action->keyval;
	push @labels, $action->label;
	$has_labels = 1 if defined $labels[0] && $labels[0] ne '';
    }
    
    if ( $has_labels )
    {
	foreach my $i ( 0..$#keys )
	{
	    $edt->{key_labels}{$keys[$i]} = $labels[$i] if defined $labels[$i] && $labels[$i] ne '';
	}
    }
    
    if ( $result )
    {
	$edt->{action_count} += $count;
	$edt->{deleted_keys}{$_} = 1 foreach @keys;
	return $result;
    }
    
    else
    {
	$edt->{fail_count} += $count;
	$edt->{failed_keys}{$_} = 1 foreach @keys;
	return undef;
    }

    # else
    # {
    # 	my $keyval = $action->keyval;
    # 	my $label = $action->label;
	
    # 	$edt->{key_labels}{$keyval} = $label if $label;
	
    # 	if ( $result )
    # 	{
    # 	    $edt->{action_count}++;
    # 	    $edt->{deleted_keys}{$keyval} = 1;
    # 	    return $result;
    # 	}
	
    # 	else
    # 	{
    # 	    $edt->{fail_count}++;
    # 	    $edt->{failed_keys}{$keyval} = 1;
    # 	    return undef;
    # 	}
    # }
}


# Methods to be overridden
# ------------------------

# The following methods do nothing, and exist solely to be overridden by subclasses. This enables
# subclasses to execute auxiliary database operations before and/or after actions and
# transactions.

# begin_transaction ( table )
#
# This method is passed whatever is designated as the "main table" for this transaction. It is
# designed to be overridden by subclasses, so that any necessary work can be carried out at the
# beginning of the transaction. The default method defined here does nothing.

sub begin_transaction {
    
    my ($edt, $table) = @_;
    
    my $a = 1;	# We can stop here when debugging.
}


# end_transaction ( table, rollback )
#
# This method is passed whatever is designated as the "main table" for this transaction. It is
# designed to be overridden by subclasses, so that any necessary work can be carried out at the
# end of the transaction.
# 
# The argument $rollback will be true if the transaction will be rolled back after the method
# returns, false if it will be committed. Of course, if the method itself calls 'add_condition' or
# else throws an exception, the transaction will be rolled back anyway.

sub end_transaction {

    my ($edt, $table, $rollback) = @_;

    my $a = 1;	# We can stop here when debugging.
}


# before_action ( action, operation, table )
#
# This method is called before each action. It is designed to be overridden by subclasses, so that
# any necessary auxiliary work can be carried out. The default method defined here does nothing.

sub before_action {

    my ($edt, $action, $operation, $table) = @_;

    my $a = 1;	# We can stop here when debugging.
}


# after_action ( action, operation, table, result )
#
# This method is called after each action. It is designed to be overridden by subclasses, so that
# any necessary auxiliary work can be carried out. The parameter $result will get the result of
# the database operation, except for an 'insert' operation where it holds the primary key value of
# the newly inserted record on success, and undefined on failure.

sub after_action {

    my ($edt, $action, $operation, $table, $result) = @_;
    
    my $a = 1;	# We can stop here when debugging.
}


# Progress and results of actions
# -------------------------------

# The methods in this section can be called from code in subclasses to determine the progress of
# the EditTransaction and carry out auxiliary actions such as inserts to or deletes from other
# tables that are tied to the main one by foreign keys.

sub inserted_keys {

    return keys %{$_[0]->{inserted_keys}};
}


sub updated_keys {

    return keys %{$_[0]->{updated_keys}};
}


sub replaced_keys {

    return keys %{$_[0]->{replaced_keys}};
}


sub deleted_keys {

    return keys %{$_[0]->{deleted_keys}};
}


sub failed_keys {

    return keys %{$_[0]->{failed_keys}};
}


sub key_labels {

    return $_[0]{key_labels};
}


sub action_count {
    
    return $_[0]->{action_count};
}


sub fail_count {
    
    return $_[0]->{fail_count};
}


# Permission checking
# -------------------

# The methods in this section simply call the equivalent methods of the Permissions object that
# was used to initialize this EditTransaction.

sub check_table_permission {

    my ($edt, $table, $permission) = @_;
    
    return $edt->{perms}->check_table_permission($table, $permission);
}

sub check_record_permission {
    
    my ($edt, $table, $permission, $key_expr, $record) = @_;
    
    return $edt->{perms}->check_record_permission($table, $permission, $key_expr, $record);
}


# Action validation
# -----------------

# The methods in this section provide default validation for records to be inserted and
# updated. This is done by comparing the field values to the types of the corresponding columns
# from the database schema for the table, plus any attributes specifically specified for the
# column using 'set_column_property' such as 'REQUIRED' and 'ADMIN_SET'.
# 
# Subclasses of EditTransaction may override this method, to add additional checks. It is
# recommended that they call this method as a SUPER, because it comprehensively checks every
# field value against the corresponding table definition.

our (%SIGNED_BOUND) = ( tiny => 127,
			small => 32767,
			medium => 8388607,
			regular => 2147483647 );

our (%UNSIGNED_BOUND) = ( tiny => 255,
			  small => 65535,
			  medium => 16777215,
			  regular => 4294967295 );
			 


# validate_action ( action, operation, table, keyexpr )
#
# Call the validate_record method to check that the new field values to be added or updated in the
# database are correct according to the corresponding table definition. The $keyexpr parameter is
# not used by this code, since it does not check the current values in the database. However, this
# parameter is provided for every operation except 'insert' in case an override method needs to
# use it.
#
# This method may be overridden by subclasses of this class, in order to provide different checks
# or additional checks. Such methods should indicate error and warning conditions using the method
# 'add_record_condition'. Override methods will probably want to call 'validate_record' as well,
# because it provides comprehensive checks to make sure that all record values can be properly
# stored in the database.

sub validate_action {
    
    my ($edt, $action, $operation, $table, $keyexpr) = @_;
    
    return $edt->validate_against_schema($action, $operation, $table);
}


# validate_against_schema ( action, operation, table, special )
# 
# Check the field values to be stored in the database against the corresponding table definition,
# and call 'add_record_condition' to record any error or warning conditions that are detected. The
# column names and corresponding values to be stored are added to the action record using
# 'set_column_values', for later use by the action execution methods.
# 
# If the argument $special is given, it must be a hash ref whose keys are column names. Currently
# the only accepted value is 'skip', indicating that this field should be skipped. This is
# available for use when this method is called from within a subclass method that overrides
# 'validate_action'.

sub validate_against_schema {

    my ($edt, $action, $operation, $table, $special) = @_;

    $special ||= { };
    $operation ||= $action->operation;
    $table ||= $action->table;
    
    my $record = $action->record;
    my $permission = $action->permission;
    
    # Grab the table schema, or throw an exception if it is not available. This information is cached, so
    # the database will only need to be asked for this information once per process per table.
    
    my $dbh = $edt->dbh;
    my $schema = get_table_schema($dbh, $table, $edt->debug);
    my $property = get_column_properties($table);
    
    # Start by going through the list of field names, and constructing a list of values to be
    # inserted.
    
    my (@columns, @values);
    
    foreach my $col ( @{$schema->{_column_list}} )
    {
	# Skip any columns we are directed to ignore. These will presumably handled by the code
	# from a subclass that has called this method.
	
	if ( $special->{$col} && $special->{$col} eq 'skip' )
	{
	    next;
	}
	
	# The name under which the value is stored in the record provided us may not be exactly
	# the same as the database column name. Start with the assumption that it is, but if
	# the column ends in '_no' then also check for a corresponding column ending in '_id'.
	
	my $record_col = $col;
	
	unless ( exists $record->{$record_col} )
	{
	    if ( $col =~ qr{ ^ (.*) _no $ }xs )
	    {
		$record_col = $1 . '_id';
	    }
	}
	
	# Grab whatever value has been specified for this column.
	
	my $value = $record->{$record_col};
	
	# Handle special columns in the appropriate ways.
	
	if ( my $type = $COMMON_FIELD_OTHER{$col} )
	{
	    # The 'crmod' fields store the record creation and modification dates. These cannot be
	    # specified explicitly except by a user with administrative permission, and then only
	    # if this EditTransaction allows the condition 'ALTER_TRAIL'. In that case, check to
	    # make sure that they have the proper format. But always ignore empty values.
	    
	    if ( $type eq 'crmod' )
	    {
		# If the value is empty, skip it and let it be filled in by the database engine.
		
		next unless defined $value && $value ne '';
		
		# Otherwise, check to make sure the user has permission to set a specific value.
		
		unless ( $permission eq 'admin' && $edt->allows('ALTER_TRAIL') )
		{
		    $edt->add_record_condition('E_PERM_COL', $table, $record_col);
		    next;
		}
		
		# If so, check that the value matches the required format.
		
		unless ( $value =~ qr{ ^ \d\d\d\d - \d\d - \d\d (?: \s+ \d\d : \d\d : \d\d ) $ }xs )
		{
		    $edt->add_record_condition('E_PARAM', $table, $record_col, $value, 'invalid format');
		    next;
		}
	    }
	    
	    # The 'authent' fields store the identifiers of the record authorizer, enterer, and
	    # modifier. These are subject to the same conditions as the 'crmod' fields if
	    # specified explicitly. But empty values get filled in according to the values for the
	    # current user.
	    
	    elsif ( $type eq 'authent' )
	    {
		# If the value is not empty, check to make sure the user has permission to set a
		# specific value.
		
		if ( defined $value && $value ne '' )
		{
		    unless ( $permission eq 'admin' && $edt->allows('ALTER_TRAIL') )
		    {
			$edt->add_record_condition('E_PERM_COL', $table, $record_col);
			next;
		    }
		    
		    if ( ref $value eq 'PBDB::ExtIdent' )
		    {
			unless ( $value->{type} eq 'PRS' )
			{
			    $edt->add_record_condition('E_PARAM', $table, $record_col, $value,
						       "must be an external identifier of type '$IDP{PRS}'");
			}
			
			$value = $value->stringify;
		    }
		    
		    elsif ( ref $value || $value !~ qr{ ^ \d+ $ }xs )
		    {
			$edt->add_record_condition('E_PARAM', $table, $record_col, $value,
						   'must be an external identifier or an unsigned integer');
			next;
		    }
		    
		    unless ( $edt->check_key($PERSON_DATA, $value) )
		    {
			$edt->add_record_condition('E_KEY_NOT_FOUND', $table, $record_col, $value, 'person');
		    }
		}
		
		# If (as is generally supposed to happen) no value is specified for this column,
		# then fill it in from the known information. The 'authorizer_no', 'enterer_no',
		# and 'enterer_id' fields are filled in on record insertion, and 'modifier_no' on
		# record update.
		
		elsif ( $col eq 'authorizer_no' && $operation ne 'update' )
		{
		    $value = $edt->{perms}->authorizer_no;
		}
		
		elsif ( $col eq 'enterer_no' && $operation ne 'update' )
		{
		    $value = $edt->{perms}->enterer_no;
		}
		
		elsif ( $col eq 'enterer_id' && $operation ne 'update' )
		{
		    $value = $edt->{perms}->user_id;
		}
		
		elsif ( $col eq 'modifier_no' && $operation eq 'update' )
		{
		    $value = $edt->{perms}->enterer_no;
		}
		
		elsif ( $col eq 'modifier_id' && $operation eq 'update' )
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
		
		unless ( $permission eq 'admin' && $edt->allows('ALTER_TRAIL') )
		{
		    $edt->add_record_condition('E_PERM_COL', $table, $col);
		    next;
		}
		
		# If so, make sure the value is correct.
		
		if ( $col eq 'admin_lock' && not ( $value eq '1' || $value eq '0' ) )
		{
		    $edt->add_record_condition('E_PARAM', $table, $col, $value, 'value must be 1 or 0');
		    next;
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
	    # If the value is empty but a value is required for this column, throw an error.
	    
	    if ( $value eq '' && $property->{$col}{REQUIRED} )
	    {
		$edt->add_record_condition('E_REQUIRED', $table, $record_col);
		next;
	    }
	    
	    # Handle references to keys from other PBDB tables by checking them against the
	    # specified table. We use a symbolic reference because the system of table names is based
	    # on global variables, whose values might change. Yes, I know this is not the best way
	    # to do it.
	    
	    if ( my $foreign_table = $FOREIGN_KEY_TABLE{$col} )
	    {
		if ( $value )
		{
		    no strict 'refs';
		    
		    my $foreign_table_name = ${$foreign_table};
		    
		    unless ( $edt->check_key($foreign_table_name, $value) )
		    {
			$edt->add_record_condition('E_KEY_NOT_FOUND', $table, $record_col, $value);
			next;
		    }
		}
		
		else
		{
		    $value = '0';
		}
	    }
	    
	    # Otherwise, check the column type.
	    
	    elsif ( my $type = $schema->{$col}{Type} )
	    {
		# If the type is char or varchar, we only need to check the maximum length.
		
		if ( $type =~ qr{ ^ (?: var )? char \( ( \d+ ) }xs )
		{
		    if ( length($value) > $1 )
		    {
			$edt->add_record_condition('E_PARAM', $table, $record_col, $value, "must be no more than $1 characters");
			next;
		    }
		}
		
		# If the type is text or tinytext, similarly.
		
		elsif ( $type =~ qr{ ^ (tiny)? text }xs )
		{
		    my $max_length = $1 ? 255 : 65535;
		    
		    if ( length($value) > $max_length )
		    {
			$edt->add_record_condition('E_PARAM', $table, $record_col, $value, "must be no more than $1 characters");
			next;
		    }
		}
		
		# If the type is integer, do format and bound checking. Special case booleans,
		# which are represented as tinyint(1).
		
		elsif ( $type =~ qr{ ^ (tiny|small|medium|big)? int \( (\d+) \) \s* (unsigned)? }xs )
		{
		    my $size = $1 || 'regular';
		    my $bits = $2;
		    my $unsigned = $3;
		    
		    if ( $bits eq '1' )
		    {
			if ( $value !~ qr{ ^ [01] $ }xs )
			{
			    $edt->add_record_condition('E_PARAM', $table, $record_col, $value, "value must be 0 or 1");
			    next;
			}
		    }
		    
		    elsif ( $unsigned )
		    {
			if ( $value !~ qr{ ^ \d+ $ }xs )
			{
			    $edt->add_record_condition('E_PARAM', $table, $record_col, $value, 
						       "value must be an unsigned integer");
			    next;
			}
			
			elsif ( $value > $UNSIGNED_BOUND{$size} )
			{
			    $edt->add_record_condition('E_PARAM', $table, $record_col, $value,
						       "value must be no greater than $UNSIGNED_BOUND{$size}");
			}
		    }
		    
		    else
		    {
			if ( $value !~ qr{ ^ -? \s* \d+ $ }xs )
			{
			    $edt->add_record_condition('E_PARAM', $table, $record_col, $value, "value must be an integer");
			    next;
			}
			
			elsif ( $value > $SIGNED_BOUND{$size} || -1 * $value > $SIGNED_BOUND{$size} + 1 )
			{
			    my $lower = $SIGNED_BOUND{$size} + 1;
			    $edt->add_record_condition('E_PARAM', $table, $record_col, $value, 
						       "value must lie between -$lower and $SIGNED_BOUND{$size}");
			    next;
			}
		    }
		}
		
		# If the type is decimal, do format and bound checking. 
		
		elsif ( $type =~ qr{ ^ decimal \( (\d+) , (\d+) \) \s* (unsigned)? }xs )
		{
		    my $unsigned = $3;
		    
		    # $$$ we need to add width checking
		    
		    if ( $unsigned )
		    {
			if ( $value !~ qr{ ^ (?: \d+ [.] \d* | \d* [.] \d+ ) }xs )
			{
			    $edt->add_record_condition('E_PARAM', $table, $record_col, $value, "must be an unsigned decimal number");
			    next;
			}
		    }
		    
		    else
		    {
			if ( $value !~ qr{ ^ -? (?: \d+ [.] \d* | \d* [.] \d+ ) }xs )
			{
			    $edt->add_record_condition('E_PARAM', $table, $record_col, $value, "must be a decimal number");
			    next;
			}
		    }
		}
		
		# $$$ should add float later
		
		# Otherwise, we just throw up our hands and accept whatever they give us. This
		# might not be wise.
	    }
	}
	
	# If a value is required for this column and none was given, then we need to check whether
	# this is an update of an existing record. If it is, and if this column was not mentioned
	# in the record at all, then we just skip it. Otherwise, we signal an error.
	
	elsif ( $property->{$col}{REQUIRED} )
	{
	    if ( $operation eq 'update' && ! exists $record->{$record_col} )
	    {
		next;
	    }
	    
	    else
	    {
		$edt->add_record_condition('E_REQUIRED', $table, $record_col);
		next;
	    }
	}
	
	# Otherwise, if this column is not mentioned in the record at all, just skip it. If the
	# column exists in the record with an undefined value, the code below will substitute a
	# value of NULL.
	
	elsif ( ! exists $record->{$record_col} )
	{
	    next;
	}
	
	# If we get here, then we have a good value! Push the column and value on the respective
	# lists. An undefined value is pushed as NULL, otherwise the value is quoted. The default
	# behavior for mariadb when given the empty string as a value for a numeric column is to
	# store zero. So we'll go with that.
	
	push @columns, $col;
	
	if ( defined $value )
	{
	    push @values, $dbh->quote($value);
	}
	
	else
	{
	    push @values, 'NULL';
	}
    }
    
    # Now store our column and value lists for subsequent use in constructing SQL statements.
    
    $action->set_column_values(\@columns, \@values);
    
    return;
}


# Error and warning conditions
# ----------------------------
# 
# We define a separate package for error and warning conditions.

package EditCondition;

sub new {
    
    my ($class, $code, $label, $table, @data) = @_;
    
    my $new = [ $code, $label, $table, @data ];
    
    return bless $new, $class;
}


sub code {
    
    my ($condition) = @_;
    
    return $condition->[0];
}


sub label {
    
    my ($condition) = @_;
    
    return $condition->[1];
}


sub table {

    my ($condition) = @_;

    return $condition->[2];
}


sub data {
    
    my ($condition) = @_;
    
    return @$condition[3..$#$condition];
}

1;
