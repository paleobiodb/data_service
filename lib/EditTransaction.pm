# 
# The Paleobiology Database
# 
#   EditTransaction.pm - base class for data acquisition and modification
# 

package EditTransaction;

use strict;

use ExternalIdent qw(%IDP);
use TableDefs qw(get_table_property get_column_properties $PERSON_DATA
		 %COMMON_FIELD_IDTYPE %COMMON_FIELD_OTHER %FOREIGN_KEY_TABLE);
use TableData qw(get_table_schema);
use EditTransaction::Action;
use Permissions;

use Carp qw(carp croak);
use Try::Tiny;
use Scalar::Util qw(weaken blessed);

use Switch::Plain;

# This class is intended to encapsulate the mid-level code necessary for updating records in the
# database in the context of a data service operation or a command-line utility. It handles
# transaction initiation, commitment, and rollback, permission checking, and also error and
# warning conditions.
# 
# This class can be subclassed (see ResourceEdit.pm, TimescaleEdit.pm) in order to provide
# additional logic for checking values and performing auxiliary operations in conjunction with
# database inserts, updates, and deletes.

our ($MULTI_DELETE_LIMIT) = 100;
our ($MULTI_INSERT_LIMIT) = 100;

our (%ALLOW_BY_CLASS) = ( EditTransaction => { 
		CREATE => 1,
		MULTI_INSERT => 1,
		MULTI_DELETE => 1,
		ALTER_TRAIL => 1,
		NOT_FOUND => 1,
		NO_RECORDS => 1,
		DEBUG_MODE => 1,
		SILENT_MODE => 1,
		PROCEED_MODE => 1, 
		IMMEDIATE_MODE => 1 } );

our (%CONDITION_BY_CLASS) = ( EditTransaction => {		     
		C_CREATE => "Allow 'CREATE' to create records",
		C_NO_RECORDS => "Allow 'NO_RECORDS' to allow transactions with no records",
		E_EXECUTE => "%1",
		E_NO_KEY => "The %1 operation requires a primary key value",
		E_HAS_KEY => "You may not specify a primary key value for the %1 operation",
		E_KEY_NOT_FOUND => "Field '%1': no %3 record was found with key '%2'",
		E_NOT_FOUND => "No record was found with key '%1'",
		E_PERM => { insert => "You do not have permission to insert a record into this table",
			    update => "You do not have permission to update this record",
			    replace_new => "No record was found with key '%2', ".
				"and you do not have permission to insert one",
			    replace_old => "You do not have permission to replace this record",
			    delete => "You do not have permission to delete this record",
			    default => "You do not have permission for this operation" },
		E_PERM_COL => "You do not have permission to set the value of the field '%1'",
		E_REQUIRED => "Field '%1' must have a nonempty value",
		E_PARAM => "Field '%1': %2",
		W_ALLOW => "Unknown allowance '%1'",
		W_EXECUTE => "%1",
		UNKNOWN => "MISSING ERROR MESSAGE" });

our (%TEST_PROBLEM);	# This variable can be set in order to trigger specific errors, in order
                        # to test the error-response mechanisms.

our (%OPERATION_TYPE) = ( insert => 'record', replace => 'record', update => 'record', delete => 'single' );

# The following hash is used to make sure that if one transaction interrupts the other, we will
# know about it.

# $$$

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
		current_action => undef,
		proceed => undef,
		record_count => 0,
		action_count => 0,
		fail_count => 0,
		skip_count => 0,
		commit_count => 0,
		rollback_count => 0,
		transaction => '',
		state => 'ok' };
    
    bless $edt, $class;
    
    # Store the request, dbh, and debug flag as local fields. If we are storing a reference to a
    # request, we must weaken it to ensure that this object from being destroyed when it goes out
    # of scope. Circular references might otherwise prevent this.
    
    if ( $request_or_dbh->can('get_connection') )
    {
	$edt->{request} = $request_or_dbh;
	weaken $edt->{request};
	
	$edt->{dbh} = $request_or_dbh->get_connection;
	$edt->{debug} = $request_or_dbh->debug if $request_or_dbh->can('debug') &&
	    not( ref $allows eq 'HASH' && defined $allows->{DEBUG_MODE} && $allows->{DEBUG_MODE} eq '0' );
	
	die "TEST NO CONNECT" if $TEST_PROBLEM{no_connect};
    }
    
    elsif ( ref($request_or_dbh) =~ /DBI::db$/ )
    {
	$edt->{dbh} = $request_or_dbh;
	
	die "TEST NO CONNECT" if $TEST_PROBLEM{no_connect};
    }

    else
    {
	croak "no database handle was provided";
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

	    if ( $k eq 'PROCEED_MODE' ) { $edt->{proceed} = 1 }
	    elsif ( $k eq 'NOT_FOUND' ) { $edt->{proceed} ||= 2 }
	    elsif ( $k eq 'DEBUG_MODE' ) { $edt->{debug} = 1; $edt->{silent} = undef }
	    elsif ( $k eq 'SILENT_MODE' ) { $edt->{silent} = 1 unless $edt->{debug} }
	    elsif ( $k eq 'IMMEDIATE_MODE' ) { $edt->{execute_immediately} = 1 }
	}
	
	else
	{
	    $edt->add_condition('W_ALLOW', $k);
	}
    }
    
    # Throw an exception if we don't have a valid database handle. Otherwise, rollback any
    # uncommitted work, since if there is previous work on THIS DATABASE CONNECTION that qwasn't
    # explicitly committed before creating this new transaction then something is wrong.
    
    croak "missing dbh" unless ref $edt->{dbh};
    
    # Now set the database handle attributes properly.
    
    $edt->{dbh}->{RaiseError} = 1;
    $edt->{dbh}->{PrintError} = 0; # $edt->{debug};
    
    # If IMMEDIATE_MODE was specified, then immediately start a new transaction. The same effect
    # can be provided by calling the method 'start_execution' on this new object.
    
    if ( $edt->{execute_immediately} )
    {
	$edt->_start_transaction;
    }
    
    return $edt;
}


# If this object is destroyed while a transaction is in progress, roll it back.

sub DESTROY {
    
    my ($edt) = @_;
    
    if ( $edt->is_active )
    {
	$edt->_rollback_transaction(1);
    }
}


# Basic accessor methods
# ----------------------

# These are all read-only.

sub dbh {
    
    return $_[0]{dbh};
}


sub request {
    
    return $_[0]{request};
}


sub transaction {

    return $_[0]{transaction} || '';
}


sub has_started {

    return $_[0]{transaction} ? $_[0]{transaction} : '';
}


sub is_active {

    return $_[0]{transaction} && $_[0]{transaction} eq 'active' ? 'active' : '';
}


sub has_finished {

    return $_[0]{transaction} && $_[0]{transaction} ne 'active' ? $_[0]{transaction} : '';
}


sub has_committed {

    return $_[0]{transaction} && $_[0]{transaction} eq 'committed' ? 'committed' : '';
}


sub can_proceed {

    return defined $_[0]{transaction} && ($_[0]{transaction} eq '' || $_[0]{transaction} eq 'active') &&
	! ( $_[0]{errors} && @{$_[0]{errors}} )
}


sub perms {
    
    return $_[0]->{perms};
}


sub debug {
    
    return $_[0]->{debug};
}


sub role {
    
    return $_[0]->{perms} ? $_[0]->{perms}->role : '';
}


# Additional attributes
# ---------------------

# The following routines allow getting and setting of arbitrary attributes, which can be used for
# communication between subclass methods and interface code.

sub set_attr {

    croak "you must specify an attribute name" unless $_[1];
    $_[0]->{attrs}{$_[1]} = $_[2];
}


sub get_attr {

    croak "you must specify an attribute name" unless $_[1];
    return $_[0]->{attrs} ? $_[0]->{attrs}{$_[1]} : undef;
}



# Debugging and error message display
# -----------------------------------

# error_line ( text )
# 
# This method is called internally to display error messages, including
# exceptions that were generated during the execution of code in this
# module. These messages are suppressed if SILENT_MODE is true. Note that
# setting DEUBG_MODE to true. will turn off SILENT_MODE.

sub error_line {

    return if ref $_[0] && $_[0]->{silent};

    my ($edt, $line) = @_;

    if ( $edt->{request} )
    {
	$edt->{request}->debug_line($line);
    }

    else
    {
	print STDERR "$line\n";
    }
}


# debug_line ( text )
#
# This method is called internally to display extra output for
# debugging. These messages are only shown if DEBUG_MODE is true.
    
sub debug_line {
    
    return unless ref $_[0] && $_[0]->{debug};
    
    my ($edt, $line) = @_;
    
    if ( $edt->{request} )
    {
	$edt->{request}->debug_line($line);
    }

    else
    {
	print STDERR "$line\n";
    }
}


# Error, caution, and warning conditions
# --------------------------------------

# Error and warning conditions are indicated by codes, all composed of upper case word symbols. Those
# that start with 'E_' represent errors, those that start with 'C_' represent cautions, and those
# that start with 'W_' represent warnings. In general, errors cause the operation to be aborted
# while warnings do not. Cautions cause the operation to be aborted unless specifically allowed.
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
# allowance 'PROCEED_MODE' specifies that whatever parts of the operation are able to succeed
# should be carried out, even if some record operations fail. The special allowance 'NOT_FOUND'
# indicates that E_NOT_FOUND should be demoted to a warning, and that particular record skipped,
# but other errors will still block the operation from proceeding.
# 
# Codes that start with 'W_' indicate warnings that should be passed back to the client but do not
# prevent the operation from proceeding.
# 
# Codes that start with 'D_' and 'F_' indicate conditions that would otherwise have been cautions
# or errors, under the 'PROCEED_MODE' or 'NOT_FOUND' allowance. These are treated as warnings.
# 
# Allowed conditions must be specified for each EditTransaction object when it is created.


# register_allows ( condition... )
# 
# Register the names of extra allowances for transactions in a particular subclass. This class
# method is designed to be called at startup from modules that subclasses this one.

sub register_allowances {
    
    my ($class, @names) = @_;
    
    foreach my $n ( @names )
    {
	$ALLOW_BY_CLASS{$class}{$n} = 1;
    }
}


# inherit_from ( from_class )
# 
# Copy all of the allowances and conditions from the specified class to this one.

sub inherit_from {
    
    my ($class, $from_class) = @_;
    
    if ( ref $ALLOW_BY_CLASS{$from_class} eq 'HASH' )
    {
	foreach my $n ( keys %{$ALLOW_BY_CLASS{$from_class}} )
	{
	    $ALLOW_BY_CLASS{$class}{$n} = $ALLOW_BY_CLASS{$from_class}{$n};
	}
    }

    if ( ref $CONDITION_BY_CLASS{$from_class} eq 'HASH' )
    {
	foreach my $n ( keys %{$CONDITION_BY_CLASS{$from_class}} )
	{
	    $CONDITION_BY_CLASS{$class}{$n} = $CONDITION_BY_CLASS{$from_class}{$n};
	}
    }
}


# allows ( condition )
# 
# Returns true if the specified condition is allowed for this EditTransaction, false
# otherwise. The set of allowed conditions was specified when this object was originally created.

sub allows {
    
    return ref $_[0]->{allows} && defined $_[1] && $_[0]->{allows}{$_[1]};
}


# register_conditions ( condition ... )
#
# Register the names and templates of conditions which may be generated by transactions in a
# particular subclass. This is designed to be called at startup from modules which subclass this
# one.

sub register_conditions {

    my $class = shift;
    
    # Process the arguments in pairs.
    
    while ( @_ )
    {
	my $code = shift;
	
	croak "you must specify an even number of arguments" unless @_;
	
	my $template = shift;

	# Make sure the code follows the proper pattern, and the template is not empty.
	
	croak "bad condition code '$code'" unless $code =~ qr{ ^ [ECW] _ [A-Z0-9_]+ $ }xs;
	croak "bad condition template '$template'" unless defined $template && $template ne '';
	
	$CONDITION_BY_CLASS{$class}{$code} = $template;
    }
};


# add_condition ( [action], condition, data... )
# 
# Register a condition (error, caution, or warning) that pertains to the either the entire transaction
# or to a single action. One or more pieces of data will generally also be passed in, which can be
# used later by code in the data service operation module to generate an error or warning message
# to return to the user. Conditions that pertain to an action may be translated to warnings if
# either the PROCEED_MODE or the NOT_FOUND allowance was specified.
# 
# If the first parameter is a reference to an action, then the condition will be attached to that
# action. If it is the undefined value or the string 'main', then the condition will apply to the
# transaction as a whole. Otherwise, the condition will be attached to the current action if there
# is one or the transaction as a whole otherwise.

sub add_condition {
    
    my $edt = shift;
    
    my $action;
    
    # If the first parameter is a reference, it must be a reference to an action. 
    
    if ( ref $_[0] )
    {
	$action = shift;
	
	croak "first parameter must be either a code, or an action, or 'main'"
	    unless $action->isa('EditTransaction::Action');
    }

    # If it is 'main' or the undefined value, then the action will be empty.
    
    elsif ( ! defined $_[0] || $_[0] eq 'main' )
    {
	shift;	# in this case, $action will be empty
    }

    # Otherwise, we use the "current action", which may be empty if this method is called before
    # the first action is handled or after the last one is done.
    
    else
    {
	$action = $edt->{current_action};
    }
    
    # The next parameter must be the condition code. Any subsequent parameters are data elements
    # that will be used as parameters for generating the error message.
    
    my $code = shift || '';

    # Make sure this is a code that we recognize.

    unless ( $CONDITION_BY_CLASS{ref $edt}{$code} || $CONDITION_BY_CLASS{EditTransaction}{$code} )
    {
	croak "unknown condition code '$code'";
    }
    
    # If the code starts with E_ or C_ then it represents an error.
    
    if ( $code =~ qr{ ^ [EC] }xs )
    {
	# If it is attached to an action, then mark that action as having at least one error, and
	# check to see if it needs to be demoted.
	
	if ( $action )
	{
	    $action->add_error;
	    
	    # If this transaction allows either PROCEED_MODE or NOT_FOUND, then we demote this
	    # error to a warning. But in the latter case, only if it is E_NOT_FOUND.
	    
	    if ( $edt->{proceed} && ( $edt->{proceed} == 1 || $code eq 'E_NOT_FOUND' ) )
	    {
		substr($code,0,1) =~ tr/CE/DF/;
		
		$edt->{condition}{$code}++;
		
		my $condition = EditTransaction::Condition->new($action, $code, @_);
		push @{$edt->{warnings}}, $condition;
		
		return $condition;
	    }
	}
	
	# If we get here, then the condition will be saved as an error.
	
	$edt->{condition}{$code}++;
	
	my $condition = EditTransaction::Condition->new($action, $code, @_);
	push @{$edt->{errors}}, $condition;

	return $condition;
    }
    
    # if the code starts with W_ then it represents a warning.
    
    elsif ( $code =~ qr{ ^ [W] }xs )
    {
	# If it is attached to an action, mark that action as having at least one warning.
	
	if ( $action )
	{
	    $action->add_warning;
	}

	# This condition will be saved on the warning list.
	
	$edt->{condition}{$code}++;
	
	my $condition = EditTransaction::Condition->new($action, $code, @_);
	push @{$edt->{warnings}}, $condition;

	return $condition;
    }

    # If it doesn't match either pattern, throw an exception.
    
    else
    {
	croak "bad condition code '$code'";
    }
}


# demote_condition ( action )
#
# 


# errors ( )
# 
# Return the list of error and caution condition records for the current EditTransaction. In
# numeric context, Perl will simply evaluate this as a number. In boolean context, this will be
# evaluated as true if there are any and false if not. This is one of my favorite features of
# Perl.

sub errors {

    return @{$_[0]->{errors}};
}


# warnings ( )
# 
# Return the list of warning condition records for the current EditTransaction. See &errors above.

sub warnings {
    
    return @{$_[0]->{warnings}};
}


# specific_errors ( [action] )
# 
# Return the list of error and caution condition records for the current action, or for a
# specified action, or those not associated with any action. For the latter case, the argument
# 'main' should be provided.

sub specific_errors {
    
    my ($edt, $search) = @_;
    
    # If a specific action was given, look for that one unless the argument 'main' was given, in
    # which case we look for no action at all. If no action was given, we just use the current
    # action, which may be empty.
    
    if ( $search eq 'main' )
    {
	$search = undef;
    }
    
    else
    {
	$search ||= $edt->{current_action};
    }
    
    # If we are looking for a specific action, then we can return immediately under either of the
    # following conditions: if this routine was called in scalar context, or if this action was
    # not associated with any errors. In either case, we need only check the action's error count.
    
    my @specific_list;
    
    if ( $search )
    {
	return $search->{error_count} // 0 unless wantarray && $search->{error_count};

	# If PROCEED_MODE or NOT_FOUND is allowed, then we have to check the warning list first,
	# because errors for this action may have been demoted to warnings.

	if ( $edt->{proceed} && $search->{error_count} )
	{
	    foreach my $i ( 1..@{$edt->{warnings}} )
	    {
		last if $search->{error_count} == @specific_list;
		
		if ( $edt->{warnings}[-$i][0] == $search &&
		     $edt->{warnings}[-$i][1] && $edt->{warnings}[-$i][1] =~ /^[DF]/ )
		{
		    unshift @specific_list, $edt->{warnings}[-$i];
		}
	    }
	}
    }
    
    # Otherwise, go through the error list backwards looking for errors that match the specified
    # action. Note the negation operator on the array index. The reason we search backwards is that we
    # are most likely to be looking for errors associated with the most recent action.
    
    foreach my $i ( 1..@{$edt->{errors}} )
    {
	# If we are looking for a specific action, then stop when we have found as many errors as are
	# recorded for this action.
	
	last if $search && $search->{error_count} == @specific_list;
	
	# The first field of each condition record indicates the action (if any) that it was
	# attached to.
	
	if ( $edt->{errors}[-$i][0] == $search )
	{
	    unshift @specific_list, $edt->{errors}[-$i];
	}
    }
    
    return @specific_list;
}


# specific_warnings ( [action] )
#
# Do the same for warnings.

sub specific_warnings {
    
    my ($edt, $search) = @_;
    
    # If a specific action was given, look for that one unless the argument 'main' was given, in
    # which case we look for no action at all. If no action was given, we just use the current
    # action, which may be empty.
    
    if ( $search eq 'main' )
    {
	$search = undef;
    }
    
    else
    {
	$search ||= $edt->{current_action};
    }

    # If we are looking for a specific action, then we can return immediately under either of the
    # following conditions: if this routine was called in scalar context, or if this action was
    # not associated with any warnings. In either case, we need only check the action's warning count.
    
    if ( $search )
    {
	return $search->{warning_count} // 0 unless wantarray && $search->{warning_count};
    }
    
    # Otherwise, go through the warning list backwards looking for warnings that match the specified
    # action. Note the negation operator on the array index. The reason we search backwards is that we
    # are most likely to be looking for warnings associated with the most recent action.
    
    my @specific_list;
    
    foreach my $i ( 1..@{$edt->{warnings}} )
    {
	last if $search && $search->{warning_count} == @specific_list;
	
	# If we are looking for a specific action, as opposed to no action, then stop when we
	# reach the warning count recorded for that action.
	
	if ( $edt->{warnings}[-$i][0] == $search &&
	     $edt->{warnings}[-$i][1] && $edt->{warnings}[-$i][1] =~ /^W/ )
	{
	    unshift @specific_list, $edt->{warnings}[-$i];
	}
    }

    return @specific_list;
}


# error_strings ( )
#
# Return a list of error strings that can be printed out, returned in a JSON data structure, or
# otherwise displayed to the end user.

sub error_strings {
    
    my ($edt) = @_;
    
    return $edt->_generate_strings('errors');
}


sub warning_strings {

    my ($edt) = @_;

    return $edt->_generate_strings('warnings');
}


sub _generate_strings {

    my ($edt, $field) = @_;
    
    my %message;
    my @messages;
    
    foreach my $e ( @{$edt->{$field}} )
    {
	my $str = $e->code . ': ' . $edt->generate_msg($e);
	
	push @messages, $str unless $message{$str};
	push @{$message{$str}}, $e->label;
    }

    my @strings;
    
    foreach my $m ( @messages )
    {
	my $nolabel;
	my %label;
	my @labels;
	
	foreach my $l ( @{$message{$m}} )
	{
	    if ( defined $l && $l ne '' ) { push @labels, $l unless $label{$l}; $label{$l} = 1; }
	    else { $nolabel = 1; }
	}
	
	if ( $nolabel )
	{
	    push @strings, $m;
	}
	
	if ( @labels > 3 )
	{
	    my $count = scalar(@labels) - 2;
	    my $list = " ($labels[0], $labels[1], and $count more):";
	    push @strings, $m =~ s/:/$list/r;
	}

	elsif ( @labels )
	{
	    my $list = ' (' . join(', ', @labels) . '):';
	    push @strings, $m =~ s/:/$list/r;
	}
    }

    return @strings;
}


# record_errors ( )
# 
# Return the list of errors (not warnings) for the current record. This is used below to test
# whether or not we can proceed with the current record.

# sub record_errors {
    
#     return @{$_[0]->{current_errors}};
# }


# record_warnings ( )
# 
# Return the list of warnings for the current record. This is only here in case it is needed by a
# subroutine defined by some subclass.

# sub record_warnings {

#     return @{$_[0]->{current_warnings}};
# }


# generate_msg ( condition )
#
# This routine generates an error message from a condition record.

sub generate_msg {
    
    my ($edt, $condition) = @_;
    
    # Extract the necessary information from the condition record.
    
    my $code = $condition->code;
    my $table = $condition->table;
    my @params = $condition->data;
    
    # If the code was altered because of the PROCEED_MODE allowance, change it back
    # so we can look up the proper template.
    
    my $lookup = $code;
    substr($lookup,0,1) =~ tr/DF/CE/;
    
    # Look up the template according to the specified, code, table, and first
    # parameter. The method called may be overridden by a subclass, in order
    # to handle codes that we do not know about.
    
    my $template = $edt->get_condition_template($lookup, $params[0]);
    
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

    my ($edt, $code, $selector) = @_;
    
    my $template = $CONDITION_BY_CLASS{ref $edt}{$code} ||
	$CONDITION_BY_CLASS{EditTransaction}{$code};
    
    if ( ref $template eq 'HASH' )
    {
	if ( $selector && $template->{$selector} )
	{
	    return $template->{$selector};
	}
	
	elsif ( $template->{default} )
	{
	    return $template->{default};
	}
	
	else
	{
	    return $CONDITION_BY_CLASS{EditTransaction}{'UNKNOWN'};
	}
    }
    
    else
    {
	return $template || $CONDITION_BY_CLASS{EditTransaction}{'UNKNOWN'};
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
	$template =~ s{ [%]t }{ $table }xs;
	$template =~ s{ [%](\d) }{ &squash_param($params[$1-1]) }xseg;
	
	if ( $code eq 'E_PARAM' && defined $params[2] && $params[2] ne '' )
	{
	    $template .= ", was '$params[2]'";
	}
	
	return $template;
    }

    # Otherwise, return the empty string.
    
    else
    {
	return '';
    }
}


# squash_param ( param )
#
# Return a value suitable for inclusion into a message template. If the parameter value is longer
# than 40 characters, it is truncated and ellipses are appended. If the value is not defined, then
# 'UNKNOWN PARAMETER' is returned.

sub squash_param {

    if ( defined $_[0] )
    {
	if ( length($_[0]) > 40 )
	{
	    return substr($_[0],0,40) . '...';
	}

	else
	{
	    return $_[0];
	}
    }

    else
    {
	return 'UNKNOWN';
    }
}


# Record sequencing
# -----------------

# _new_record {
# 
# Prepare for a new record operation. This includes moving any errors, cautions, and warnings
# generated for the previous record to the main error and warning lists. It also determines the
# label to be used in reporting any conditions about the new record. This is intended to be a private
# method, called only from within this class.

sub _new_record {
    
    my ($edt, $table, $operation, $record) = @_;
    
    croak "no record specified" unless ref $record eq 'HASH' ||
	$operation eq 'delete' && defined $record && $record ne '';
    
    # # If there are any errors and warnings pending from the previous record, move them to the main
    # # lists.
    
    # $edt->_finish_record;
    
    # Then determine a label for this record. If one is specified, use that. Otherwise, keep count
    # of how many records we have seen so far and use that prepended by '#'.
    
    my $label;
    
    $edt->{record_count}++;
    
    if ( ref $record && defined $record->{record_label} && $record->{record_label} ne '' )
    {
	$label = $record->{record_label};
    }
    
    else
    {
	$label = '#' . $edt->{record_count};
    }
    
    # Then create a new EditTransaction::Action object, save it, and return it.

    $edt->{current_action} = EditTransaction::Action->new($table, $operation, $record, $label);
}


# _finish_record ( )
# 
# Finish processing the current record. All record conditions are moved over to the main lists,
# and the 'current_label' is set to undefined.

# sub _finish_record {
    
#     my ($edt) = @_;

#     # Clear the "current record label" to indicate that we are done processing the most recent
#     # record.

#     $edt->{current_label} = undef;
    
#     # # If any errors have been generated for the current record, move them.
    
#     # if ( $edt->{current_errors} && @{$edt->{current_errors}} )
#     # {
#     # 	# If the allowance 'PROCEED_MODE' is in effect, then all errors and cautions are converted into
#     # 	# warnings and the initial letter of each code changed from E -> F and C -> D.
	
#     # 	if ( $edt->allows('PROCEED_MODE') )
#     # 	{
#     # 	    while ( my $e = shift @{$edt->{current_errors}} )
#     # 	    {
#     # 		substr($e->[0],0,1) =~ tr/CE/DF/;
#     # 		push @{$edt->{warnings}}, $e;
#     # 		$edt->{condition}{$e->[0]} = 1;
#     # 	    }
#     # 	}

#     # 	# Otherwise, if the allowance 'NOT_FOUND' is in effect, then 'E_NOT_FOUND' errors are
#     # 	# treated as above and all other errors and cautions are moved unchanged.
	
#     # 	elsif ( $edt->allows('NOT_FOUND') )
#     # 	{
#     # 	    while ( my $e = shift @{$edt->{current_errors}} )
#     # 	    {
#     # 		if ( $e->[0] eq 'E_NOT_FOUND' )
#     # 		{
#     # 		    $e->[0] = 'F_NOT_FOUND';
#     # 		    push @{$edt->{warnings}}, $e;
#     # 		}
		
#     # 		else
#     # 		{
#     # 		    push @{$edt->{errors}}, $e;
#     # 		}
		
#     # 		$edt->{condition}{$e->[0]} = 1;
#     # 	    }
#     # 	}

#     # 	# Otherwise, just move all errors and cautions over to the main list.
	
#     # 	else
#     # 	{
#     # 	    while ( my $e = shift @{$edt->{current_errors}} )
#     # 	    {
#     # 		push @{$edt->{errors}}, $e;
#     # 		$edt->{condition}{$e->[0]} = 1;
#     # 	    }
#     # 	}
#     # }

#     # # If there are any warnings, just move them over unchanged.

#     # if ( $edt->{current_warnings} && @{$edt->{current_warnings}} )
#     # {
#     # 	while ( my $w = shift @{$edt->{current_warnings}} )
#     # 	{
#     # 	    push @{$edt->{warnings}}, $w;
#     # 	}
#     # }
    
#     # # Clear the 'current record label'.
    
#     # $edt->{current_label} = undef;
# }


# _clear_record ( )
# 
# Clear any error and warning messages generated by the current record, and also the record
# label. This method is called when processing of a record is to be abandoned.

# sub _clear_record {
    
#     my ($edt) = @_;
    
#     @{$edt->{curremt_errors}} = ();
#     @{$edt->{current_warnings}} = ();
#     $edt->{current_label} =  undef;
# }


# record_label ( )
# 
# Return the record label for the record currently being processed. This is valid both during
# checking and execution.

# sub record_label {

#     return $_[0]->{current_label};
# }


# Transaction control
# -------------------

# start_transaction ( )
# 
# Start the database transaction associated with this EditTransaction. This is done automatically
# when 'execute' is called, but can be done explicitly at an earlier stage if the checking of
# record values needs to be done inside a transaction. Returns true if the transaction is can proceed,
# false otherwise.

sub start_transaction {
    
    my ($edt) = @_;

    # If this transaction has already committed, throw an exception.

    if ( $edt->has_committed )
    {
	croak "this transaction has already committed";
    }
    
    # If we have not started the transaction yet, and there are no errors, then start it now.
    
    elsif ( ! $edt->has_started && ! $edt->errors )
    {
	$edt->_start_transaction;
    }
    
    return $edt->can_proceed;
}


sub _start_transaction {

    my ($edt) = @_;
    
    my $label = $edt->role eq 'guest' ? '(guest) ' : '';
    my ($result, $save_action);
    
    $edt->debug_line( " >>> START TRANSACTION $label\n" );
    
    try {

	# Start the transaction. This will implicitly commit any uncommitted work.
	
	$edt->dbh->do("START TRANSACTION");
	$edt->{transaction} = 'active';
	
	# Save and clear the 'current action', if any, before calling
	# 'initialize_transaction'. This way, error conditions generated by that method will not
	# be attached to any specific transaction.
	
	$save_action = $edt->{current_action};
	$edt->{current_action} = undef;
	
	# Call the 'initialize_transaction' method, which is designed to be overridden by subclasses. The
	# default method does nothing.
	
	$edt->initialize_transaction($edt->{main_table});
    }
    
    catch {

	$edt->{transaction} = 'aborted';
	
	my $msg = $edt->{transaction} eq 'active' ? 'an exception occurred on initialization' :
	    'an exception occurred while starting the transaction';
	
	$edt->add_condition(undef, 'E_EXECUTE', $msg);
	$edt->error_line($_);
    };

    # Restore the current action, if any.
    
    $edt->{current_action} = $save_action;
}


# commit ( )
# 
# If this EditTransaction has not yet been completed, do so. After this is done, this
# EditTransaction cannot be used for any more actions. If the operation method needs to make more
# changes to the database, a new EditTransaction must be created. This operation returns a true
# value if the transaction succeeded, false otherwise.
# 
# $$$ Perhaps I will later modify this class so that it can be used for multiple transactions in turn.

sub commit {
    
    my ($edt) = @_;

    return $edt->execute;
}


sub _commit_transaction {
    
    my ($edt) = @_;
    
    $edt->{current_action} = undef;    
    
    $edt->debug_line( " <<< COMMIT TRANSACTION\n" );
    
    try {
	
	$edt->dbh->do("COMMIT");
	$edt->{transaction} = 'committed';
	$edt->{commit_count}++;
    }
	
    catch {
	$edt->add_condition(undef, 'E_EXECUTE', 'an exception occurred on transaction commit');
	$edt->error_line($_);
	$edt->{transaction} = 'aborted';
	$edt->{rollback_count}++;
    };
}


# rollback ( )
#
# If this EditTransaction has not yet been completed, roll back whatever work has been done. If
# the database transaction has not yet been started, just mark the transaction 'finished' and
# return. This operation returns a true value if an active transaction was rolled back, and false
# otherwise.

sub rollback {

    my ($edt) = @_;
    
    if ( $edt->is_active )
    {
	$edt->_rollback_transaction;
    }
    
    else
    {
	$edt->{transaction} ||= 'finished';
    }
    
    return $edt->{transaction} eq 'aborted' ? 1 : undef;
}


sub _rollback_transaction {
    
    my ($edt, $from_destroy) = @_;
    
    $edt->{current_action} = undef;    
    
    if ( $from_destroy )
    {
	$edt->debug_line( " <<< ROLLBACK TRANSACTION FROM DESTROY\n" );
    }
    
    else
    {
	$edt->debug_line( " <<< ROLLBACK TRANSACTION\n" );
    }
    
    try {
	
	$edt->dbh->do("ROLLBACK");
    }
    
    catch {
	$edt->add_condition(undef, 'E_EXECUTE', 'an exception occurred on transaction rollback');
	$edt->error_line($_);
    };
    
    $edt->{rollback_count}++;
    $edt->{transaction} = 'aborted';
}


# Record operations
# -----------------

# The operations in this section are called by data service operation methods to insert, update,
# and delete records in the database.


# start_execution
# 
# Call 'start_transaction' and also set the 'execute_immediately' flag. This means that subsequent
# actions will be carried out immediately on the database rather than waiting for a call to
# 'execute'. Returns true if the transaction can proceed, false otherwise.

sub start_execution {
    
    my ($edt) = @_;
    
    $edt->start_transaction;
    $edt->{execute_immediately} = 1;

    # If the transaction is now active, then execute all pending actions before we do anything
    # else.
    
    if ( $edt->is_active )
    {
	$edt->_execute_action_list;
    }
    
    return $edt->can_proceed;
}


# insert_record ( table, record )
# 
# The specified record is to be inserted into the specified table. Depending on the settings of
# this particular EditTransaction, this action may happen immediately or may be executed
# later. The record in question MUST NOT include a primary key value.

sub insert_record {
    
    my ($edt, $table, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main
    # lists, and create a new EditTransaction::Action to represent this insertion.
    
    my $action = $edt->_new_record($table, 'insert', $record);
    
    # We can only create records if specifically allowed. This may be specified by the user as a
    # parameter to the operation being executed, or it may be set by the operation method itself
    # if the operation is specifically designed to create records.
    
    if ( $edt->allows('CREATE') )
    {
	try {

	    # First check to make sure we have permission to insert a record into this table. A
	    # subclass may override this method, if it needs to make different checks than the default
	    # ones.
	    
	    my $permission = $edt->authorize_action($action, 'insert', $table);
	    
	    # If the user does not have permission to add a record, add an error condition.
	    
	    if ( $permission ne 'post' && $permission ne 'admin' )
	    {
		$edt->add_condition($action, 'E_PERM', 'insert');
	    }
	    
	    # A record to be inserted must not have a primary key value specified for it. Records with
	    # primary key values can only be passed to 'update_record' or 'replace_record'.
	    
	    if ( $action->keyval )
	    {
		$edt->add_condition($action, 'E_HAS_KEY', 'insert');
	    }
	    
	    # Then check the actual record to be inserted, to make sure that the column values meet
	    # all of the criteria for this table. If any error or warning conditions are detected,
	    # they are added to the current transaction. A subclass may override this method, if it
	    # needs to make additional checks or perform additional work.
	    
	    $edt->validate_action($action, 'insert', $table);
	}

	catch {
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	    $edt->error_line($_);
	};
    }
    
    # If an attempt is made to add a record without the 'CREATE' allowance, add the appropriate
    # error condition.
    
    else
    {
	$edt->add_condition($action, 'C_CREATE');
    }
    
    # Either execute the action immediately or add it to the appropriate list depending on whether
    # or not any error conditions are found.
    
    return $edt->_handle_action($action);
}


# update_record ( table, record )
# 
# The specified record is to be updated in the specified table. Depending on the settings of this
# particular EditTransaction, this action may happen immediately or may be executed later. The
# record in question MUST include a primary key value, indicating which record to update.

sub update_record {
    
    my ($edt, $table, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being updated.
    
    my $action = $edt->_new_record($table, 'update', $record);
    
    # We can only update a record if a primary key value is specified.
    
    if ( my $keyexpr = $edt->get_keyexpr($action) )
    {
	try {

	    # First check to make sure we have permission to update this record. A subclass may
	    # override this method, if it needs to make different checks than the default ones.
	    
	    my $permission = $edt->authorize_action($action, 'update', $table, $keyexpr);
	    
	    # If no such record is found in the database, add an error condition. If this
	    # EditTransaction has been created with the 'PROCEED_MODE' or 'NOT_FOUND' allowance, it
	    # will automatically be turned into a warning and will not cause the transaction to be
	    # aborted.
	    
	    if ( $permission eq 'notfound' )
	    {
		$edt->add_condition($action, 'E_NOT_FOUND', $action->keyval);
	    }
	    
	    # If the user does not have permission to edit the record, add an error condition. 
	    
	    elsif ( $permission ne 'edit' && $permission ne 'admin' )
	    {
		$edt->add_condition($action, 'E_PERM');
	    }
	    
	    # Then check the new record values, to make sure that the column values meet all of the
	    # criteria for this table. If any error or warning conditions are detected, they are added
	    # to the current transaction. A subclass may override this method, if it needs to make
	    # additional checks or perform additional work.
	    
	    $edt->validate_action($action, 'update', $table, $keyexpr);
	}

	catch {
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	    $edt->error_line($_);
	};
    }
    
    # If no primary key value was specified for this record, add an error condition. This will, of
    # course, have to be reported under the record label that was passed in as part of the record
    # (if one was in fact given) or else the index of the record in the input set.
    
    else
    {
	$edt->add_condition($action, 'E_NO_KEY', 'update');
    }
    
    # Either execute the action immediately or add it to the appropriate list depending on whether
    # or not any error conditions are found.
    
    return $edt->_handle_action($action);
}


# replace_record ( table, record )
# 
# The specified record is to be inserted into the specified table, replacing any record that may
# exist with the same primary key value. Depending on the settings of this particular EditTransaction,
# this action may happen immediately or may be executed later. The record in question MUST include
# a primary key value.

sub replace_record {
    
    my ($edt, $table, $record) = @_;
    
    # Move any accumulated record error or warning conditions to the main lists, and determine the
    # key expression and label for the record being replaced.
    
    my $action = $edt->_new_record($table, 'replace', $record);
    
    # We can only replace a record if a primary key value is specified.
    
    if ( my $keyexpr = $edt->get_keyexpr($action) )
    {
	try {
	    
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
		    $permission = $edt->check_table_permission($table, 'admin');
		    
		    if ( $permission eq 'admin' )
		    {
			$action->set_permission($permission);
		    }
		    
		    elsif ( get_table_property($table, 'ALLOW_KEY_INSERT') && $permission eq 'post' )
		    {
			$action->set_permission($permission);
		    }
		    
		    elsif ( $edt->allows('NOT_FOUND') )
		    {
			$edt->add_condition($action, 'E_NOT_FOUND', $action->keyval);
		    }
		    
		    else
		    {
			$edt->add_condition($action, 'E_PERM', 'replace_new', $action->keyval);
		    }
		}
		
		# If we are not allowed to create new records, add an error condition. If this
		# EditTransaction has been created with the PROCEED_MODE or NOT_FOUND allowance, it
		# will automatically be turned into a warning and will not cause the transaction to be
		# aborted.
		
		else
		{
		    $edt->add_condition($action, 'E_NOT_FOUND', $action->keyval);
		}
	    }
	    
	    # If the user does not have permission to edit the record, add an error condition. 
	    
	    elsif ( $permission ne 'edit' && $permission ne 'admin' )
	    {
		$edt->add_condition($action, 'E_PERM', 'replace_old');
	    }
	    
	    # Then check the new record values, to make sure that the replacement record meets all of
	    # the criteria for this table. If any error or warning conditions are detected, they are
	    # added to the current transaction. A subclass may override this method, if it needs to
	    # make additional checks or perform additional work.
	    
	    $edt->validate_action($action, 'replace', $table, $keyexpr);
	}

	catch {
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	    $edt->error_line($_);
	};
    }
    
    # If no primary key value was specified for this record, add an error condition. This will, of
    # course, have to be reported under the record label that was passed in as part of the record
    # (if one was in fact given) or else the index of the record in the input set.
    
    else
    {
	$edt->add_condition($action, 'E_NO_KEY', 'replace');
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
	try {
	    
	    # First check to make sure we have permission to delete this record. A subclass may
	    # override this method, if it needs to make different checks than the default ones.
	    
	    my $permission = $edt->authorize_action($action, 'delete', $table, $keyexpr);
	    
	    # If no such record is found in the database, add an error condition. If this
	    # EditTransaction has been created with the 'PROCEED_MODE' or 'NOT_FOUND' allowance, it
	    # will automatically be turned into a warning and will not cause the transaction to be
	    # aborted.
	    
	    if ( $permission eq 'notfound' )
	    {
		$edt->add_condition($action, 'E_NOT_FOUND', $action->keyval);
	    }
	    
	    # If we do not have permission to delete the record, add an error condition.
	    
	    elsif ( $permission ne 'delete' && $permission ne 'admin' )
	    {
		$edt->add_condition($action, 'E_PERM');
	    }
	    
	    # If a 'validate_delete' method was specified, then call it. This method may abort the
	    # deletion by adding an error condition. Otherwise, we assume that the permission check we
	    # have already done is all that is necessary.
	    
	    $edt->validate_action($action, 'delete', $table, $keyexpr);
	}

	catch {
	    $edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during validation');
	    $edt->error_line($_);
	};
    }
    
    # If no primary key was specified, add an error condition.
    
    else
    {
	$edt->add_condition($action, 'E_NO_KEY', 'delete');
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
    
    if ( EditTransaction::Action->get_record_key($table, $record) )
    {
	return $edt->update_record($table, $record);
    }
    
    else
    {
	return $edt->insert_record($table, $record);
    }
}


# get_record_key ( table, record )
# 
# Return the key value (if any) specified in this record. Look first to see if the table has a
# 'PRIMARY_ATTR' property. If so, check to see if we have a value for the named
# attribute. Otherwise, check to see if the table has a 'PRIMARY_KEY' property and check under
# that name as well. If no non-empty value is found, return undefined.

sub get_record_key {

    my ($edt, $table, $record) = @_;
    
    if ( my $key_attr = get_table_property($table, 'PRIMARY_ATTR') )
    {
	if ( ref $record eq 'HASH' && defined $record->{$key_attr} && $record->{$key_attr} ne '' )
	{
	    return $record->{$key_attr};
	}
	
	else
	{
	    return;
	}
    }
    
    elsif ( my $key_column = get_table_property($table, 'PRIMARY_KEY') )
    {
	if ( ref $record eq 'HASH' && defined $record->{$key_column} && $record->{$key_column} ne '' )
	{
	    $record->{$key_column};
	}
    }
    
    return;
}


# ignore_record ( )
# 
# Indicates that a particular record should be ignored. This will keep the
# record count up-to-date for generating record labels with which to tag
# subsequent error and warning messages.

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
    
    # $$$ need to remove any error messages from this record.
}


# aux_action ( table, operation, record )
# 
# This method is called from client code or subclass methods that wish to create auxiliary actions
# to supplement the current one. For example, adding a record to one table may involve also adding
# another record to a different table.

sub aux_action {
    
    my ($edt, $table, $operation, $record) = @_;
    
    my $action = EditTransaction::Action->new($table, $operation, $record);
    
    $action->set_auxiliary($edt->{current_action});
    
    return $action;
}


sub current_action {

    my ($edt) = @_;

    return $edt->{current_action};
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
# This method may be overridden by subclasses. Override methods should indicate error and warning
# conditions by calling the method 'add_condition'.

sub authorize_action {
    
    my ($edt, $action, $operation, $table, $keyexpr) = @_;
    
    die "TEST AUTHORIZE" if $TEST_PROBLEM{authorize};
    
    sswitch ( $operation )
    {
	case 'insert': {
	    return $action->set_permission($edt->check_table_permission($table, 'post'))
	}
	
	case 'update':
	case 'replace': {
	    return $action->set_permission($edt->check_record_permission($table, 'edit', $keyexpr));
	}
	
	case 'delete': {
	    return $action->set_permission($edt->check_record_permission($table, 'delete', $keyexpr));
	}

        default: {
	    croak "bad operation '$operation'";
	}
    }
}


# _handle_action ( action )
# 
# Handle the specified action record. If errors were generated for this record, put it on the 'bad
# record' list. Otherwise, either execute it immediately or put it on the action list to be
# executed later.

sub _handle_action {
    
    my ($edt, $action) = @_;
    
    # If this transaction has already committed, throw a real exception. Client code should never
    # try to execute operations on a transaction that has already committed. (One that has been
    # rolled back is different. The status of such a transaction will clearly indicate that the
    # operations will not actually be carried out.)

    if ( $edt->{transaction} && $edt->{transaction} eq 'committed' )
    {
	croak "This transaction has already been committed";
    }
    
    # If any errors were already generated for the record currently being processed, put this
    # action on the 'bad action' list and otherwise do nothing.
    
    if ( $action->has_errors )
    {
	push @{$edt->{bad_list}}, $action;
	$edt->{fail_count}++;
	
	my $keyval = $action->keyval;
	push @{$edt->{failed_keys}}, $keyval if defined $keyval && $keyval ne '';
	
	return;
    }
    
    # If errors were generated for previous records, then there is no point in proceeding with
    # this action since the edit transaction will either never be started or will be subsequently
    # rolled back. Since we already know that no errors were generated for this particular record,
    # there is nothing more that needs to be done.
    
    elsif ( $edt->errors )
    {
	$edt->{skip_count}++;
	return;
    }
    
    # If we get here, then there is nothing to prevent the action from being executed. If the
    # 'execute immediately' flag has been turned on, then execute this action now.
    
    elsif ( $edt->{execute_immediately} )
    {
	return $edt->_execute_action($action);
    }
    
    # Otherwise, we push it on the action list for later execution. We return 1 in this case, to
    # indicate that the operation has succeeded up to this point.
    
    else
    {
	push @{$edt->{action_list}}, $action;
	return 1;
    }
}


# execute_action ( action )
#
# This method is designed to be called either internally by this class or explicitly by code from
# other classes. It executes a single action, and returns the result.

sub execute_action {
    
    my ($edt, $action) = @_;
    
    # If errors have already occurred, then do nothing.
    
    if ( $edt->errors )
    {
	$edt->{skip_count}++;
	return;
    }
    
    # If we haven't already started the transaction in the database, do so now.
    
    elsif ( ! $edt->has_started )
    {
	$edt->_start_transaction;
    }

    # Now execute the action.

    return $edt->_execute_action($action);
}


sub _execute_action {

    my ($edt, $action) = @_;

    my $result;
    
    # Call the appropriate routine to execute this operation.
    
    sswitch ( $action->operation )
    {
	case 'insert': {
	    $result = $edt->_execute_insert($action);
	}
	
	case 'update': {
	    $result = $edt->_execute_update($action);
	}
	
	case 'replace': {
	    $result = $edt->_execute_replace($action);
	}
	
	case 'delete': {
	    $result = $edt->_execute_delete($action);
	}
	
        default: {
	    croak "bad operation '$_'";
	}
    }
    
    # If errors have occurred, then we need to roll back the transaction.
    
    if ( $edt->errors )
    {
	# try {
	#     $edt->cleanup_transaction($edt->{main_table});
	# }
	    
	# catch {
	#     $edt->add_condition(undef, 'E_EXECUTE', 'an exception was thrown during cleanup');
	#     $edt->error_line($_);
	# };
	
	# $edt->_rollback_transaction;
	return undef;
    }
    
    else
    {
	return $result;
    }
}


# execute ( )
# 
# Start a database transaction, if one has not already been started. Then execute all of the
# pending insert/update/delete operations, and then either commit or rollback as
# appropriate. Returns a true value if the transaction succeeded, and false otherwise.

sub execute {
    
    my ($edt) = @_;

    $edt->{current_action} = undef;
    
    # Return immediately if the transaction has already finished.
    
    if ( $edt->has_finished )
    {
	return $edt->{transaction} eq 'committed' ? 1 : undef;
    }
    
    # If there are no actions to do, and none have been done so far, and no errors have already
    # occurred, then add C_NO_RECORDS unless the NO_RECORDS condition is allowed. This will cause
    # the transaction to be immediately aborted.
    
    unless ( @{$edt->{action_list}} || @{$edt->{errors}} || $edt->{action_count} || $edt->allows('NO_RECORDS') )
    {
	$edt->add_condition(undef, 'C_NO_RECORDS');
    }
    
    # If errors have already occurred (i.e. when records were checked for insertion or updating),
    # then return without doing anything. If a transaction is already active, then roll it
    # back. If the transaction has already been initialized and a cleanup_transaction method is
    # defined for this class, call it.
    
    if ( $edt->errors )
    {
	if ( $edt->is_active )
	{
	    try {
		
		$edt->cleanup_transaction($edt->{main_table});
	    }
	    
	    catch {
		$edt->add_condition(undef, 'E_EXECUTE', 'an exception occurred during cleanup');
		$edt->error_line($_);
	    };
	    
	    $edt->_rollback_transaction;
	}

	if ( $edt->{action_list} )
	{
	    $edt->{skip_count} += scalar(@{$edt->{action_list}});
	    $edt->{action_list} = [ ];
	}
	
	$edt->{transaction} ||= 'finished';
	return undef;
    }
    
    # Now execute any pending actions. Unless immediate mode has been turned on, these will
    # include all actions that have been done on this transaction.
    
    $edt->_execute_action_list;
    
    # Now we need to finish the transaction. If errors have occurred, then call the
    # 'cleanup_transaction' method. Otherwise, call 'finalize_transaction'.
    
    my ($result, $culprit);
    
    try {
	
	$edt->{current_action} = undef;
	
	# If errors have occurred, then call the 'cleanup_transaction' method, which is designed to
	# be overridden by subclasses. The default does nothing.
	
	if ( $edt->errors )
	{
	    $culprit = 'cleanup';
	    $edt->cleanup_transaction($edt->{main_table});
	}
	
	# Otherwise, we call 'finalize_transaction'. This too is designed to be overridden by
	# subclasses, and the default does nothing.
	
	else
	{
	    $culprit = 'execution';
	    $edt->finalize_transaction($edt->{main_table});
	}
    }
    
    # If an error occurs, we add an error condition, which will cause the transaction to be
    # aborted below.
    
    catch {
	
	$edt->add_condition(undef, 'E_EXECUTE', "an exception was thrown during $culprit");
	$edt->error_line($_);
    };
    
    # At this point, we need to do a final check to see if there are any errors accumulated for
    # this transaction. An error condition could have been added explicitly by the
    # finalize or cleanup method, or could have been generated if that method died.
    
    try {
	
	# If there are any errors, then roll back the transaction.
	
	if ( $edt->errors )
	{
	    $culprit = 'rollback';
	    $edt->_rollback_transaction;
	}
	
	# Otherwise, we're good to go! Yay!
	
	else
	{
	    $culprit = 'commit';
	    $edt->_commit_transaction;
	    $result = 1;
	}
    }

    catch {

	$edt->add_condition(undef, 'E_EXECUTE', "an exception was thrown during $culprit");
	$edt->error_line($_);
    };
    
    return $result;
}


# _execute_action_list ( )
#
# Execute any pending actions. This is called one of two ways: either by 'execute' or by 'start_execution'.

sub _execute_action_list {

    my ($edt) = @_;

    # The main part of this routine is executed inside a try block, so that we can roll back the
    # transaction if any errors occur.
    
    my $result;
    my $cleanup_called;
    
    try {
	
	# If we haven't already executed 'start_transaction' on the database, do so now.
	
	$edt->_start_transaction unless $edt->has_started;
	
	# Then go through the action list and execute each action in turn. If there are multiple
	# deletes in a row on the same table, these can be handled with a single call for
	# efficiency.
	
	while ( my $action = shift @{$edt->{action_list}} )
	{
	    $edt->{current_action} = $action;
	    
	    # If any errors have accumulated on this transaction, skip all remaining actions. This
	    # includes any errors that may have been generated by the previous action.
	    
	    if ( $edt->errors )
	    {
		$edt->{skip_count}++;				# the one we just shifted
		$edt->{skip_count} += @{$edt->{action_list}};	# any remaining
		$edt->{action_list} = [ ];			# clear the action list
		last;
	    }
	    
	    # If this particular action has any errors, then skip it. We need to do this check
	    # separately, because if PROCEED_MODE has been set for this transaction then any
	    # errors that were generated during validation of this action will have been converted
	    # to warnings. But in that case, $action->has_errors will still return true, and this
	    # action should not be executed.
	    
	    elsif ( $action->has_errors )
	    {
		$edt->{fail_count}++;
		next;
	    }
	    
	    # Now execute the appropriate handler for this action's operation.
	    
	    sswitch ( $action->operation )
	    {
		case 'insert': {
		    $edt->_execute_insert($action);
		}
		
		case 'update': {
		    $edt->_execute_update($action);
		}

		case 'replace': {
		    $edt->_execute_replace($action);
		}

		case 'delete': {
		    
		    # If we are allowing multiple deletion and there are more actions remaining, check
		    # to see if the immediately subsequent ones are also deletes on the same table and
		    # with the same permission. If so, coalesce them all into one action.
		    
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
				next if $action->has_errors;
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
		
	        default: {
		    croak "bad operation '$_'";
		}
	    }
	}
    }
    
    # If an exception is caught, we add an error condition. This will stop any further execution.
    
    catch {
	
	$edt->add_condition('E_EXECUTE', 'an exception was thrown during execution');

	# Add to the skip count all remaining actions.
	
	$edt->{skip_count} += scalar(@{$edt->{action_list}});
    };
}


# get_keyexpr ( action )
# 
# Generate a key expression for the specified action, that will select the particular record being
# acted on. If the action has no key value (i.e. is an 'insert' operation) or if no key column is
# known for this table then return '0'. The reason for returning this value is that it can be
# substituted into an SQL 'WHERE' clause and will be syntactically correct but always false.

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


# check_permission ( action, key_expr )
# 
# Determine the current user's permission to do the specified action.

sub check_permission {
    
    my ($edt, $action, $keyexpr) = @_;
    
    my $table = $action->table;

    sswitch ( $action->operation )
    {
	case 'insert': {
	    return $action->set_permission($edt->check_table_permission($table, 'post'))
	}
	
	case 'update':
	case 'replace': {
	    $keyexpr ||= $edt->get_keyexpr($action);
	    return $action->set_permission($edt->check_record_permission($table, 'edit', $keyexpr));
	}
    
	case 'delete': {
	    $keyexpr ||= $edt->get_keyexpr($action);
	    return $action->set_permission($edt->check_record_permission($table, 'delete', $keyexpr));
	}
	
       default: {
	   croak "bad operation '$_'";
       }
    }
}


# _execute_insert ( action )
# 
# Actually perform an insert operation on the database. The record keys and values should already
# have been checked by 'validate_record' or some other code, and lists of columns and values
# generated.

sub _execute_insert {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    # Set this as the current action.

    $edt->{current_action} = $action;
    
    # Check to make sure that we actually have column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'column/value mismatch on insert');
	return;
    }

    # If the following flag is set, deliberately generate an SQL error for
    # testing purposes.
    
    if ( $TEST_PROBLEM{insert_sql} )
    {
	push @$cols, 'XXXX';
    }
    
    # Construct the INSERT statement.
    
    my $dbh = $edt->dbh;
    
    my $column_list = join(',', @$cols);
    my $value_list = join(',', @$vals);
        
    my $sql = "	INSERT INTO $table ($column_list)
		VALUES ($value_list)";
    
    $edt->debug_line( "$sql\n" );
    
    my ($result, $cleanup_called, $new_keyval);
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED_MODE.
    
    try {
	
	# Start by calling the 'before_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing.
	
	$edt->before_action($action, 'insert', $table);

	# Then execute the insert statement itself.
	
	$result = $dbh->do($sql);
	
	if ( $result )
	{
	    $new_keyval = $dbh->last_insert_id(undef, undef, undef, undef);
	}
	
	# Finaly, call the 'after_action' method. This is designed to be overridden by subclasses,
	# and can be used to do any necessary auxiliary actions to the database. If the insert
	# failed, then 'cleanup_action' is called instead.
	
	if ( $new_keyval )
	{
	    $action->set_keyval($new_keyval);
	    $edt->after_action($action, 'insert', $table, $new_keyval);
	}
	
	else
	{
	    $cleanup_called = 1;
	    $edt->add_condition($action, 'E_EXECUTE', 'insert statement failed');
	    $edt->cleanup_action($action, 'insert', $table);
	    $result = undef;
	}
    }
    
    catch {
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	$edt->error_line($_);

	unless ( $cleanup_called )
	{
	    try {
		$edt->cleanup_action($action, 'insert', $table);
	    }
	    
	    catch {
		$edt->add_condition($action, 'E_EXECUTE', 'an exception was thrown during cleanup');
		$edt->error_line($_);
	    };
	}
    };
    
    # If the insert succeeded, return the new primary key value. Also record this value so that it
    # can be queried for later. Otherwise, return undefined.
    
    if ( $new_keyval && ! $action->has_errors )
    {
	$edt->{action_count}++;
	push @{$edt->{inserted_keys}}, $new_keyval;
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
# Actually perform an replace operation on the database. The record keys and values should already
# have been checked by 'validate_record' or some other code, and lists of columns and values
# generated.

sub _execute_replace {

    my ($edt, $action) = @_;
    
    my $table = $action->table;
    
    # Set this as the current action.

    $edt->{current_action} = $action;
    
    # Check to make sure that we actually have column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'column/value mismatch on replace');
	return;
    }
    
    # If the following flag is set, deliberately generate an SQL error for
    # testing purposes.
    
    if ( $TEST_PROBLEM{replace_sql} )
    {
	push @$cols, 'XXXX';
    }
    
    # Construct the REPLACE statement.
    
    my $dbh = $edt->dbh;
    
    my $column_list = join(',', @$cols);
    my $value_list = join(',', @$vals);
    
    my $sql = "	REPLACE INTO $table ($column_list)
		VALUES ($value_list)";
    
    $edt->debug_line( "$sql\n" );
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED_MODE.
    
    my ($result, $cleanup_called);
    
    try {
	
	# Start by calling the 'before_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing.
	
	$edt->before_action($action, 'replace', $table);

	# Then execute the replace statement itself.
	
	$result = $dbh->do($sql);
	
        # Finally, call the 'after_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing. If the replace failed, then 'cleanup_action' is called
	# instead.
	
	if ( $result )
	{
	    $edt->after_action($action, 'replace', $table);
	}
	
	else
	{
	    $cleanup_called = 1;
	    $edt->add_condition($action, 'E_EXECUTE', 'replace statement failed');
	    $edt->cleanup_action($action, 'replace', $table);
	}
    }
    
    catch {	
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	$edt->error_line($_);
	
	unless ( $cleanup_called )
	{
	    try {
		$edt->cleanup_action($action, 'replace', $table);
	    }
	    
	    catch {
		$edt->add_condition($action, 'E_EXECUTE', 'an exception was thrown during cleanup');
		$edt->error_line($_);
	    };
	}
    };
    
    my $keyval = $action->keyval;
    
    # If the replace succeeded, return true. Otherwise, return false. In either case, record the
    # mapping between key value and record label.
    
    $edt->{key_labels}{$keyval} = $action->label;
    
    if ( $result && ! $action->has_errors )
    {
	$edt->{action_count}++;
	push @{$edt->{replaced_keys}}, $keyval;
	return $result;
    }
    
    else
    {
	$edt->{fail_count}++;
	push @{$edt->{failed_keys}}, $keyval;
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
    
    # Set this as the current action.

    $edt->{current_action} = $action;
    
    # Check to make sure that we actually have column/value lists, and that the number of columns
    # and values is equal and non-zero.
    
    my $cols = $action->column_list;
    my $vals = $action->value_list;
    
    unless ( ref $cols eq 'ARRAY' && ref $vals eq 'ARRAY' && @$cols && @$cols == @$vals )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'column/value mismatch on update');
	return;
    }
    
    # If the following flag is set, deliberately generate an SQL error for
    # testing purposes.
    
    if ( $TEST_PROBLEM{update_sql} )
    {
	push @$cols, 'XXXX';
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
    
    $edt->debug_line( "$sql\n" );
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED_MODE.
    
    my ($result, $cleanup_called);
    
    try {
	
	# Start by calling the 'before_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing.
	
	$edt->before_action($action, 'update', $table);

	# Then execute the update statement itself.
	
	$result = $dbh->do($sql);
	
	# Finally, call the 'after_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing. If the update failed, then 'cleanup_action' is called
	# instead.
	
	if ( $result )
	{
	    $edt->after_action($action, 'update', $table);
	}
	
	else
	{
	    $cleanup_called = 1;
	    $edt->add_condition($action, 'E_EXECUTE', 'update failed');
	    $edt->cleanup_action($action, 'update', $table);
	}
    }
    
    catch {
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	$edt->debug_line($_);
	
	unless ( $cleanup_called )
	{
	    try {
		$edt->cleanup_action($action, 'update', $table);
	    }
	    
	    catch {
		$edt->add_condition($action, 'E_EXECUTE', 'an exception was thrown during cleanup');
		$edt->debug_line($_);
	    };
	}
    };
    
    # If the update succeeded, return true. Otherwise, return false. In either case, record the
    # mapping between key value and record label.
    
    my $keyval = $action->keyval;
    
    $edt->{key_labels}{$keyval} = $action->label;
    
    if ( $result && ! $action->has_errors )
    {
	$edt->{action_count}++;
	push @{$edt->{updated_keys}}, $keyval;
	return $result;
    }
    
    else
    {
	$edt->{fail_count}++;
	push @{$edt->{failed_keys}}, $keyval;
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
    
    my $dbh = $edt->dbh;
    
    my $key_expr = $edt->get_keyexpr($action);
    
    # Set this as the current action.

    $edt->{current_action} = $action;
    
    # If the following flag is set, deliberately generate an SQL error for
    # testing purposes.
    
    if ( $TEST_PROBLEM{delete_sql} )
    {
	$key_expr .= 'XXXX';
    }
    
    # Construct the DELETE statement.
    
    my $sql = "	DELETE FROM $table WHERE $key_expr";
    
    $edt->debug_line( "$sql\n" );
    
    # Execute the statement inside a try block. If it fails, add either an error or a warning
    # depending on whether this EditTransaction allows PROCEED_MODE.
    
    my ($result, $cleanup_called);
    
    try {
	
	# Start by calling the 'before_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing.    
	
	$edt->before_action($action, 'delete', $table);

	# Then execute the delete statement itself.
	
	$result = $dbh->do($sql);
	
	# Finally, call the 'after_action' method. This is designed to be overridden by
	# subclasses, and can be used to do any necessary auxiliary actions to the database. The
	# default method does nothing. If the delete failed, then 'cleanup_action' is called
	# instead.
	
	if ( $result )
	{
	    $edt->after_action($action, 'delete', $table);
	}
	
	else
	{
	    $cleanup_called = 1;
	    $edt->add_condition($action, 'E_EXECUTE', 'delete failed');
	    $edt->cleanup_action($action, 'delete', $table);
	}
    }
    
    catch {
	$edt->add_condition($action, 'E_EXECUTE', 'an exception occurred during execution');
	$edt->error_line($_);
	
	unless ( $cleanup_called )
	{
	    try {
		$edt->cleanup_action($action, 'delete', $table);
	    }
	    
	    catch {
		$edt->add_condition($action, 'E_EXECUTE', 'an exception was thrown during cleanup');
		$edt->error_line($_);
	    };
	}
    };
    
    # Record the number of records deleted, along with the mapping between key values and record labels.
    
    my ($count, @keys, @labels);
    
    if ( $action->is_multiple )
    {
	$count = $action->count;
	
	@keys = $action->all_keys;
	@labels = $action->all_labels;
	
	foreach my $i ( 0..$#keys )
	{
	    $edt->{key_labels}{$keys[$i]} = $labels[$i] if defined $labels[$i] && $labels[$i] ne '';
	}
    }
    
    else
    {
	$count = 1;
	
	@keys = $action->keyval;
	$edt->{key_labels}{$keys[0]} = $action->label;
    }
    
    # If the delete succeeded, return true. Otherwise, return false.
    
    if ( $result && ! $action->has_errors )
    {
	$edt->{action_count} += $count;
	push @{$edt->{deleted_keys}}, @keys;
	return $result;
    }
    
    else
    {
	$edt->{fail_count} += $count;
	push @{$edt->{failed_keys}}, @keys;
	return undef;
    }
}


# Methods to be overridden
# ------------------------

# The following methods do nothing, and exist solely to be overridden by subclasses. This enables
# subclasses to execute auxiliary database operations before and/or after actions and
# transactions.

# initialize_transaction ( table )
#
# This method is passed the name that was designated as the "main table" for this transaction. The
# method is designed to be overridden by subclasses, so that any necessary work can be carried out
# at the beginning of the transaction. The default method defined here does nothing.

sub initialize_transaction {
    
    my ($edt, $table) = @_;
    
    my $a = 1;	# We can stop here when debugging.
}


# finalize_transaction ( table, rollback )
#
# This method is passed the name that was designated as the "main table" for this transaction. The
# method is designed to be overridden by subclasses, so that any necessary work can be carried out
# at the end of the transaction.
# 
# The argument $rollback will be true if the transaction will be rolled back after the method
# returns, false if it will be committed. Of course, if the method itself calls 'add_condition' or
# else throws an exception, the transaction will be rolled back anyway.

sub finalize_transaction {

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


# after_action ( action, operation, table, key )
#
# This method is called after each successfully completed action. It is designed to be overridden
# by subclasses, so that any necessary auxiliary work can be carried out. For insert operations,
# the parameter $key will get the primary key value of the newly inserted record. Otherwise, this
# parameter will be undefined.

sub after_action {

    my ($edt, $action, $operation, $table, $result) = @_;
    
    my $a = 1;	# We can stop here when debugging.
}


# cleanup_action ( action, operation, table )
#
# This method is called after each failed action. It is designed to be overridden by subclasses,
# so that any necessary auxiliary work can be carried out.

sub cleanup_action {

    my ($edt, $action, $operation, $table) = @_;
    
    my $a = 1;  # We can stop here when debugging.
}


# Progress and results of actions
# -------------------------------

# The methods in this section can be called from code in subclasses to determine the progress of
# the EditTransaction and carry out auxiliary actions such as inserts to or deletes from other
# tables that are tied to the main one by foreign keys.

sub inserted_keys {

    return @{$_[0]->{inserted_keys}} if $_[0]->{inserted_keys};
}


sub updated_keys {

    return @{$_[0]->{updated_keys}} if $_[0]->{updated_keys};
}


sub replaced_keys {

    return @{$_[0]->{replaced_keys}} if $_[0]->{replaced_keys};
}


sub deleted_keys {

    return @{$_[0]->{deleted_keys}} if $_[0]->{deleted_keys};
}


sub failed_keys {

    return keys @{$_[0]->{failed_keys}} if $_[0]->{failed_keys};
}


sub key_labels {

    return $_[0]{key_labels} if $_[0]->{key_labels};
}


sub record_count {

    return $_[0]->{record_count} || 0;
}


sub action_count {
    
    return $_[0]->{action_count} || 0;
}


sub fail_count {
    
    return $_[0]->{fail_count} || 0;
}


sub skip_count {

    return $_[0]->{skip_count} || 0;
}


# Permission checking
# -------------------

# The methods listed below call the equivalent methods of the Permissions object that
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
# Subclasses may override this method, to add additional checks. It is recommended that they call
# this method as a SUPER, because it comprehensively checks every field value against the
# corresponding table definition.

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
    
    if ( $TEST_PROBLEM{validate} )
    {
	$edt->add_condition($action, 'E_EXECUTE', 'TEST VALIDATE');
	return;
    }
    
    # For all operations except deletions, validate the column values specified for this action
    # against the table schema.
    
    if ( $operation ne 'delete' )
    {
	return $edt->validate_against_schema($action, $operation, $table);
    }
}


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

sub validate_against_schema {

    my ($edt, $action, $operation, $table) = @_;

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
	
	# Don't check any columns we are directed to ignore. These were presumably checked by code
	# from a subclass that has called this method.
	
	unless ( $action->{skip_validate} && $action->{skip_validate}{$col} )
	{
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
			$edt->add_condition($action, 'E_PERM_COL', $record_col);
			next;
		    }
		    
		    # If so, check that the value matches the required format.
		    
		    unless ( $value =~ qr{ ^ \d\d\d\d - \d\d - \d\d (?: \s+ \d\d : \d\d : \d\d ) $ }xs )
		    {
			$edt->add_condition($action, 'E_PARAM', $record_col, $value, 'invalid format');
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
			    $edt->add_condition($action, 'E_PERM_COL', $record_col);
			    next;
			}
			
			if ( ref $value eq 'PBDB::ExtIdent' )
			{
			    unless ( $value->{type} eq 'PRS' )
			    {
				$edt->add_condition($action, 'E_PARAM', $record_col, $value,
						    "must be an external identifier of type '$IDP{PRS}'");
			    }
			    
			    $value = $value->stringify;
			}
			
			elsif ( ref $value || $value !~ qr{ ^ \d+ $ }xs )
			{
			    $edt->add_condition($action, 'E_PARAM', $record_col, $value,
						'must be an external identifier or an unsigned integer');
			    next;
			}
			
			unless ( $edt->check_key($PERSON_DATA, $value) )
			{
			    $edt->add_condition($action, 'E_KEY_NOT_FOUND', $record_col, $value, 'person');
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
		    
		    unless ( $permission eq 'admin' )
		    {
			$edt->add_condition($action, 'E_PERM_COL', $col);
			next;
		    }
		    
		    # If so, make sure the value is correct.
		    
		    if ( $col eq 'admin_lock' && not ( $value eq '1' || $value eq '0' ) )
		    {
			$edt->add_condition($action, 'E_PARAM', $col, $value, 'value must be 1 or 0');
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
		    $edt->add_condition($action, 'E_REQUIRED', $record_col);
		    next;
		}
		
		# Handle references to keys from other PBDB tables by checking them against the
		# specified table. We use a symbolic reference because the system of table names is based
		# on global variables, whose values might change. Yes, I know this is not the cleanest way
		# to do it.
		
		if ( my $foreign_table = $FOREIGN_KEY_TABLE{$col} )
		{
		    if ( $value )
		    {
			no strict 'refs';
			
			my $foreign_table_name = ${$foreign_table};
			
			unless ( $edt->check_key($foreign_table_name, $value) )
			{
			    $edt->add_condition($action, 'E_KEY_NOT_FOUND', $record_col, $value);
			    next;
			}
		    }
		    
		    else
		    {
			$value = '0';
		    }
		}
		
		# Otherwise, check the value according to the column type.
		
		elsif ( my $type = $schema->{$col}{Type} )
		{
		    # If the type is char or varchar, we only need to check the maximum length.
		    
		    if ( $type =~ qr{ ^ (?: var )? char \( ( \d+ ) }xs )
		    {
			if ( length($value) > $1 )
			{
			    $edt->add_condition($action, 'E_PARAM', $record_col,
						"must be no more than $1 characters, $value");
			    next;
			}
		    }
		    
		    # If the type is text or tinytext, similarly.
		    
		    elsif ( $type =~ qr{ ^ (tiny)? text }xs )
		    {
			my $max_length = $1 ? 255 : 65535;
			
			if ( length($value) > $max_length )
			{
			    $edt->add_condition($action, 'E_PARAM', $record_col,
						"must be no more than $1 characters", $value);
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
				$edt->add_condition($action, 'E_PARAM', $record_col,
						    "value must be 0 or 1", $value);
				next;
			    }
			}
			
			elsif ( $unsigned )
			{
			    if ( $value !~ qr{ ^ \d+ $ }xs )
			    {
				$edt->add_condition($action, 'E_PARAM', $record_col, 
						    "value must be an unsigned integer", $value);
				next;
			    }
			    
			    elsif ( $value > $UNSIGNED_BOUND{$size} )
			    {
				$edt->add_condition($action, 'E_PARAM', $record_col,
						    "value must be no greater than $UNSIGNED_BOUND{$size}", $value);
			    }
			}
			
			else
			{
			    if ( $value !~ qr{ ^ -? \s* \d+ $ }xs )
			    {
				$edt->add_condition($action, 'E_PARAM', $record_col,
						    "value must be an integer", $value);
				next;
			    }
			    
			    elsif ( $value > $SIGNED_BOUND{$size} || -1 * $value > $SIGNED_BOUND{$size} + 1 )
			    {
				my $lower = $SIGNED_BOUND{$size} + 1;
				$edt->add_condition($action, 'E_PARAM', $record_col, 
						    "value must lie between -$lower and $SIGNED_BOUND{$size}", $value);
				next;
			    }
			}
		    }
		    
		    # If the type is decimal, do format and bound checking. 
		    
		    elsif ( $type =~ qr{ ^ decimal \( (\d+) , (\d+) \) \s* (unsigned)? }xs )
		    {
			my $width = $1;
			my $prec = $2;
			my $unsigned = $3;
			
			if ( $unsigned )
			{
			    if ( $value !~ qr{ ^ (?: \d+ [.] \d* | \d* [.] \d+ ) }xs )
			    {
				$edt->add_condition($action, 'E_PARAM', $record_col,
						    "must be an unsigned decimal number", $value);
				next;
			    }
			}
			
			else
			{
			    if ( $value !~ qr{ ^ -? (?: \d+ [.] \d* | \d* [.] \d+ ) }xs )
			    {
				$edt->add_condition($action, 'E_PARAM', $record_col,
						    "must be a decimal number", $value);
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
		    $edt->add_condition($action, 'E_REQUIRED', $record_col);
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
	}

	# If we were directed not to validate this column, we still need to check whether it is
	# mentioned in the record. If not, we skip it.

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

package EditTransaction::Condition;


# new ( action, code, data... )
#
# Create a new EditTransaction::Condition for the specified action, which may be undef. The second
# argument must be a condition code, i.e. 'E_PERM' or 'W_NOT_FOUND'. The remaining arguments, if
# any, indicate the particulars of the condition and are used in generating a string value from
# the condition record.

sub new {
    
    my $class = shift;
    
    return bless [ @_ ], $class;
}


# code ( )
#
# Return the code associated with this error condition.

sub code {
    
    my ($condition) = @_;
    
    return $condition->[1];
}


# label ( )
#
# Return the label associated with this error condition. If no action was specified, the empty
# string is returned.

sub label {
    
    my ($condition) = @_;
    
    return $condition->[0] && $condition->[0]->isa('EditTransaction::Action') ?
	$condition->[0]->label : '';
}


# table ( )
#
# Return the table associated with this error condition. If no action was specified, the empty
# string is returned.

sub table {

    my ($condition) = @_;

    return $condition->[0] && $condition->[0]->isa('EditTransaction::Action') ?
	$condition->[0]->table : '';
}


# data ( )
#
# Return the data elements, if any, associated with this error condition.

sub data {
    
    my ($condition) = @_;
    
    return @$condition[2..$#$condition];
}

1;
