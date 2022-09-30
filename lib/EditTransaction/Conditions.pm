#
# EditTransaction::Conditions - a role that provides routines for handling error, caution, and
# warning conditions.
#


package EditTransaction::Conditions;

use strict;

use EditTransaction;
use Carp qw(croak);
use Scalar::Util qw(reftype blessed);

use Moo::Role;

no warnings 'uninitialized';

use parent qw(Exporter);


# Default condition codes and templates
# -------------------------------------

# Condition codes are classified based on their first letter:
# 
#  C      Codes beginning with C_ represent cautions. Cautions are blocking
#         conditions, preventing a transaction from completing. They provide
#         feedback to the client code that is intended to be passed on to
#         the end user. For example, C_LOCKED indicates that one or more of the
#         records to be updated are locked. The client interface may choose to
#         respond by asking the user something like "Some of these records are
#         locked.  Update them anyway?" If the user responds in the affirmative,
#         the response can be repeated verbatim with the additional option
#         "allow=LOCKED". Each caution has a corresponding allowance that can be
#         used to bypass it.
# 
#  E      Codes beginning with E_ represent errors. Errors are also blocking
#         conditions. An error condition represents a problem that prevents the
#         transaction from completing, such as a column value that is too large
#         for the column, a table specifier representing a nonexistent table,
#         etc.
# 
#  F      Codes beginning with F_ represent errors that have been "demoted" to
#         be non-blocking. The allowances 'PROCEED', 'NOT_FOUND', and
#         'NOT_PERMITTED' allow a transaction to continue if certain error
#         conditions are generated. The particular action that generated the
#         condition will not be performed, but the rest of the transaction will
#         complete if no other errors occur. For example, a transaction with
#         "allow=NOT_FOUND" will not perform updates or deletes on records that
#         are not in the table, but other actions in the same transaction will
#         continue to be executed.  The transaction will be committed unless
#         other errors not covered by the allowance are generated.  Errors that
#         are covered by an allowance will be demoted to nonfatal conditions.
#         For example: E_NOT_FOUND => F_NOT_FOUND.
# 
#  W      Codes beginning with W_ represent warnings. They are non-blocking.
#         They serve to inform the client that the result of the transaction may
#         not be as desired. For example, W_TRUNC is generated when an oversized
#         value is assigned to a database column that allows truncation. This
#         condition code  indicates that the assigned value is not what was
#         actually stored into the database.

our (@EXPORT_OK) = qw(%CONDITION_BY_CLASS);

our (%CONDITION_BY_CLASS) = ( EditTransaction => {		     
		C_CREATE => "Allow 'CREATE' to create records",
		C_LOCKED => "Allow 'LOCKED' to update locked records",
		C_ALTER_TRAIL => "Allow 'ALTER_TRAIL' to explicitly set crmod and authent fields",
		C_CHANGE_PARENT => "Allow 'CHANGE_PARENT' to allow subordinate link values to be changed",
		E_BAD_CONNECTION => ["&1", "Database connection failed"],
		E_BAD_TABLE => "'&1' does not correspond to any known database table",
		E_NO_KEY => "The &1 operation requires a primary key value",
		E_HAS_KEY => "You may not specify a primary key value for the &1 operation",
		E_MULTI_KEY => "You may only specify a single primary key value for the &1 operation",
		E_BAD_KEY => ["Field '&1': Invalid key value(s): &2",
			      "Invalid key value(s): &2",
			      "Invalid key value(s): &1"],
		E_BAD_SELECTOR => ["Field '&1': &2", "&2"],
		E_BAD_REFERENCE => { _multiple_ => ["Field '&2': found multiple keys for '&3'",
						    "Found multiple keys for '&3'",
						    "Found multiple keys for '&2'"],
				     _unresolved_ => ["Field '&2': no key value found for '&3'",
						      "No key value found for '&3'",
						      "No key value found for '&2'"],
				     _mismatch_ => ["Field '&2': the reference '&3' has the wrong type",
						    "Reference '&3' has the wrong type",
						    "Reference '&2' has the wrong type"],
				     default => ["Field '&1': no record with the proper type matches '&2'",
						 "No record with the proper type matches '&2'",
						 "No record with the proper type matches '&1'"] },
		E_NOT_FOUND => ["No record was found with key '&1'", 
				"No record was found with this key"],
		E_LOCKED => { multiple => ["Found &2 locked record(s)",
					   "One or more of these records is locked"],
			      default => "This record is locked" },
		E_PERM_LOCK => { _multiple_ => 
				["You do not have permission to lock/unlock &2 of these records",
				 "You do not have permission to lock/unlock one or more of these records"],
				 default => "You do not have permission to lock/unlock this record" },
		E_PERM => { insert => "You do not have permission to insert a record into this table",
			    update => "You do not have permission to update this record",
			    update_many => "You do not have permission to update records in this table",
			    replace_new => "No record was found with key '&2', ".
				"and you do not have permission to insert one",
			    replace_existing => "You do not have permission to replace this record",
			    delete => "You do not have permission to delete this record",
			    delete_many => "You do not have permission to delete records from this table",
			    delete_cleanup => "You do not have permission to delete these records",
			    fixup_mode => "You do not have permission for fixup mode on this table",
			    default => "You do not have permission for this operation" },
		E_BAD_OPERATION => ["Invalid operation '&1'", "Invalid operation"],
		E_BAD_RECORD => "",
		E_BAD_CONDITION => "&1 '&2'",
		E_PERM_COL => "You do not have permission to set the value of '&1'",
		E_REQUIRED => "Field '&1': must have a nonempty value",
		E_RANGE => ["Field '&1': &2", "&2", "Field '&1'"],
		E_WIDTH => ["Field '&1': &2", "&2", "Field '&1'"],
		E_FORMAT => ["Field '&1': &2", "&2", "Field '&1'"],
		E_EXTID => ["Field '&1': &2", "Field '&1': bad external identifier",
			    "Bad external identifier"],
			    # "Field '&1': external identifier must be of type '&2'",
			    # "External identifier must be of type '&2', was '&3'",
			    # "External identifier must be of type '&2'",
			    # "No external identifier type is defined for field '&1'"],
		E_PARAM => "",
  		E_EXECUTE => ["&1", "Unknown"],
		E_DUPLICATE => "Duplicate entry '&1' for key '&2'",
		E_BAD_FIELD => "Field '&1' does not correspond to any column in '&2'",
		E_UNRECOGNIZED => "This record not match any record type accepted by this operation",
		E_IMPORTED => "",
		W_BAD_ALLOWANCE => "Unknown allowance '&1'",
		W_EXECUTE => ["&1", "Unknown"],
		W_UNCHANGED => "",
		W_NOT_FOUND => "",
		W_PARAM => "",
		W_TRUNC => ["Field '&1': &2", "Field '&1'"],
		W_EXTID => ["Field '&1' : &2", 
			    "Field '&1': column does not accept external identifiers, value looks like one"],
		W_BAD_FIELD => "Field '&1' does not correspond to any column in '&2'",
		W_EMPTY_RECORD => "Item is empty",
		W_IMPORTED => "",
		UNKNOWN => "Unknown condition code" });


our ($CONDITION_CODE_STRICT) = qr{ ^ [CEW]_ [A-Z0-9_-]+ $ }x;
our ($CONDITION_CODE_LOOSE) =  qr{ ^ [CEFW]_ [A-Z0-9_-]+ $ }x;
our ($CONDITION_CODE_START) =  qr{ ^ [CEFW]_ }x;
our ($CONDITION_LINE_IMPORT) = qr{ ^ ([CEFW]_[A-Z0-9_-]+) (?: \s* [(] .*? [)] )? (?: \s* : \s* )? (.*) }x;

# register_conditions ( condition ... )
#
# Register the names and templates of conditions which may be generated by transactions in a
# particular subclass. This is designed to be called at startup from modules which subclass this
# one.

sub register_conditions {

    my $class = shift;
    
    croak "you must call this as a class method" unless $class->isa('EditTransaction') && ! ref $class;
    
    # Process the arguments in pairs.
    
    my $count;
    
    while ( @_ )
    {
	my $code = shift;
	
	croak "you must specify an even number of arguments" unless @_;
	
	my $template = shift;
	
	# Make sure the code follows the proper pattern and the template is defined. It may be
	# the empty string, but it must be given.
	
	croak "bad condition code '$code'" unless $code =~ $CONDITION_CODE_STRICT;
	croak "condition template cannot be undefined" unless defined $template;
	
	$CONDITION_BY_CLASS{$class}{$code} = $template;
	
	$count++;
    }
    
    return $count;
};


sub is_valid_condition {
    
    my ($class, $name) = @_;
    
    if ( ref $class )
    {
	return $CONDITION_BY_CLASS{ref $class}{$name} || $CONDITION_BY_CLASS{EditTransaction}{$name}
	    ? 1 : '';
    }
    
    else
    {
	return $CONDITION_BY_CLASS{$class}{$name} || $CONDITION_BY_CLASS{EditTransaction}{$name}
	    ? 1 : '';
    }
}


# add_condition ( [action], code, param... )
# 
# Add a condition (error, caution, or warning) that pertains to the either the entire
# transaction or to a single action. The condition is specified by a code, optionally
# followed by one or more parameters which will be used later to generate an error or
# warning message. Conditions that pertain to an action may be demoted to warnings if
# any of the allowances PROCEED, NOT_FOUND, or NOT_PERMITTED was specified.
# 
# If the first parameter is a reference to an action, then the condition will be attached
# to that action. If it is the undefined value or the string 'main', then the condition
# will apply to the transaction as a whole. Otherwise, the condition will be attached to
# the current action if there is one or the transaction as a whole otherwise.
#
# This method either adds the condition, or else throws an exception.

sub add_condition {
    
    my ($edt, @params) = @_;
    
    # Start by determining the action (if any) to which this condition should be attached.
    
    my ($action, $code);

    # If the first parameter is a Perl reference, it must be a reference to an
    # action. This method should only be called in this way from within this class and its
    # subclasses. Outside code should always refer to actions using string references.
    
    if ( ref $params[0] )
    {
	$action = shift @params;
	
	unless ( blessed $action && $action->isa('EditTransaction::Action') )
	{
	    croak "'$action' is not an action reference";
	}
    }
    
    # If the first parameter is either 'main' or the undefined value, then the condition
    # will be attached to the transaction as a whole.
    
    elsif ( ! defined $params[0] || $params[0] eq 'main' )
    {
	shift @params;
    }
    
    # If the first parameter starts with '&', look it up as an action reference. Calls of
    # this kind will always come from client code.
    
    elsif ( $params[0] =~ /^&./ )
    {
	unless ( $action = $edt->{action_ref}{$params[0]} )
	{
	    croak "no matching action found for '$params[0]'";
	}
	
	shift @params;
    }
    
    # Otherwise, default to the current action. Depending on when this method is called,
    # it may be empty, in which case the condition will be attached to the transaction as
    # a whole. If the first parameter is '_', remove it.
    
    else
    {
	$action = $edt->{current_action};
	
	shift @params if $params[0] eq '_';
    }
    
    # There must be at least one remaining parameter, and it must match the syntax of a
    # condition code. If it starts with F, change it back to E. If it does not have the
    # form of a condition code, throw an exception. Any subsequent parameters will be kept
    # and used to generate the condition message.
    
    if ( $params[0] && $params[0] =~ $CONDITION_CODE_LOOSE )
    {
	$code = shift @params;

	if ( $code =~ /^F/ )
	{
	    substr($code, 0, 1) = 'E';
	}
    }
    
    elsif ( $params[0] )
    {
	croak "'$params[0]' is not a valid selector or condition code";
    }
    
    else
    {
	croak "you must specify a condition code";
    }
    
    # If this condition belongs to an action, add it to that action. Adjust the condition counts
    # for the transaction, but only if the action is not marked as skipped. If the action already
    # has this exact condition, return without doing anything.
    
    if ( $action )
    {
	# When an error condition is attached to an action and this transaction allows
	# PROCEED, the error is demoted to a warning. If this transaction allows NOT_FOUND,
	# then an E_NOT_FOUND error is demoted to a warning but others are not.
	
	if ( $action && $code =~ /^E/ && ref $edt->{allows} eq 'HASH' )
	{
	    if ( $edt->{allows}{PROCEED} ||
		 $edt->{allows}{NOT_FOUND} && $code eq 'E_NOT_FOUND' ||
		 $edt->{allows}{NOT_PERMITTED} && $code eq 'E_PERM' )
	    {
		substr($code, 0, 1) = 'F';
	    }
	}

	# Try to add this condition to the action. If add_condition fails, that means the
	# condition duplicates one that is already attached to the action. If it succeeds,
	# then update the transaction condition counts unless this is a skipped action.
	
	if ( $action->add_condition($code, @params) && $action->status ne 'skipped' )
	{
	    $edt->{condition_code}{$code}++;
	    
	    # If the code starts with E or C then it represents an error or caution.
	    
	    if ( $code =~ /^[EC]/ )
	    {
		$edt->{error_count}++;
	    }
	    
	    # If the code starts with F, then it represents a demoted error. It counts as a
	    # warning for the transaction as a whole, but as an error for the action.
	    
	    elsif ( $code =~ /^F/ )
	    {
		$edt->{demoted_count}++;
	    }
	    
	    # Otherwise, it represents a warning.
	    
	    else
	    {
		$edt->{warning_count}++;
	    }
	    
	    # Return true to indicate that the condition was attached.
	    
	    return 1;
	}
    }
    
    # Otherwise, the condition is to be attached to the transaction as a whole unless it
    # duplicates one that is already there. If the transaction already has this exact
    # condition, return without doing anything. Use the same kind of guard statements as
    # above, and also on the condition list.
    
    elsif ( ! $edt->_has_main_condition($code, @params) )
    {
	$edt->{conditions} = [ ] unless ref $edt->{conditions} eq 'ARRAY';
	push $edt->{conditions}->@*, [undef, $code, @params];
	
	$edt->{condition_code}{$code}++;
	
	# If the code starts with [EC], it represents an error or caution.
	
	if ( $code =~ /^[EC]/ )
	{
	    $edt->{error_count}++;
	}
	
	# Otherwise, it represents a warning.
	
	else
	{
	    $edt->{warning_count}++;
	}
	
	# Return 1 to indicate that the condition was attached.
	
	return 1;
    }
    
    # If we get here, return false. Either the condition was a duplicate, or the action
    # was skipped or aborted and thus is not counted.
    
    return;
}


# has_condition ( [selector], code, [arg1, arg2, arg3] )
# 
# Return true if this transaction contains a condition with the specified code. If 1-3 extra
# arguments are also given, return true only if each argument value matches the
# corresponding condition parameter. The code may be specified either as a string or a regex.
# 
# If the first argument is 'main', the condition lists for the transaction as a whole are
# searched. If it is an action reference, the condition lists for that action are searched. If it
# is 'all', then all condition lists are searched. This is the default.

sub has_condition {
    
    my ($edt, $code, @v) = @_;
    
    my $selector = 'all';
    
    # If the first argument is a valid selector, remap the arguments.
    
    if ( $code =~ /^main$|^all$|^_$|^&./ )
    {
	($edt, $selector, $code, @v) = @_;
    }
    
    # Make sure that we were given either a regex or a string starting with [CDEFW] as the
    # code to look for.
    
    unless ( $code && (ref $code && reftype $code eq 'REGEXP' || $code =~ $CONDITION_CODE_LOOSE ) )
    {
	croak $code ? "'$code' is not a valid selector or condition code" :
	    "you must specify a condition code";
    }
    
    # If the selector is either 'main' or 'all', check the main condition list.  Return true if we
    # find an entry that has the proper code and also matches any extra values that were given.
    
    if ( $selector eq 'main' || $selector eq 'all' )
    {
	return 1 if $edt->_has_main_condition($code, @v);
	
	# If the selector is 'all', return true if any of the actions has a matching
	# condition. Return false otherwise.
	
	if ( $selector eq 'all' && ref $edt->{action_list} eq 'ARRAY' )
	{
	    foreach my $action ( $edt->{action_list}->@* )
	    {
		return 1 if $action->has_condition($code, @v);
	    }
	}
	
	return '';
    }
    
    # If the selector is '_', check the current action.
    
    elsif ( $selector eq '_' )
    {
	if ( $edt->{current_action} )
	{
	    return $edt->{current_action}->has_condition($code, @v);
	}
    }
    
    # If the selector is an action reference, check that action.
    
    elsif ( $selector =~ /^&./ )
    {
	if ( my $action = $edt->{action_ref}{$selector} )
	{
	    return $action->has_condition($code, @v);
	}
	
	# else
	# {
	#     croak "no matching action found for '$selector'";
	# }
    }
    
    else
    {
	croak "'$selector' is not a valid selector";
    }
    
    # If we get here, we were either given a refstring that does not match any
    # action or else '_' with no current action. In either case, return undef.
    
    return undef;
}


sub _has_main_condition {

    my ($edt, $code, @v) = @_;
    
    my $is_regexp = ref $code && reftype $code eq 'REGEXP';
    
    if ( ref $edt->{conditions} eq 'ARRAY' )
    {
	foreach my $i ( 0 .. $edt->{conditions}->$#* )
	{
	    my $c = $edt->{conditions}[$i];
	    
	    if ( ref $c eq 'ARRAY' &&
		( $code eq $c->[1] || $is_regexp && $c->[1] =~ $code) &&
		( @v == 0 || ( ! defined $v[0] || $v[0] eq $c->[2] ) &&
			     ( ! defined $v[1] || $v[1] eq $c->[3] ) &&
			     ( ! defined $v[2] || $v[2] eq $c->[4] ) ) )
	    {
		return $i + 1;
	    }
	}
    }
}


# conditions ( [selector], [type] )
# 
# In list context, return a list of stringified error and/or warning conditions recorded
# for this transaction. In scalar context, return how many there are. The selector and
# type can be given in either order. The selector can be any of the following, defaulting
# to 'all':
# 
#     main		Return conditions that are attached to the transaction as a whole.
#     _ 		Return conditions that are attached to the latest action.
#     &...              Return conditions that are attached to the referenced action.
#     all		Return all conditions.
# 
# The type can be any of the following, also defaulting to 'all':
# 
#     errors		Return only error conditions.
#     fatal		With selector 'all', return error conditions that were not demoted to warnings.
#     nonfatal		With selector 'all', return warning conditions and demoted errors.
#     warnings		Return only warning conditions.
#     all		Return all conditions.
#
# The types 'fatal' and 'nonfatal' are the same as 'errors' and 'warnings' respectively when used
# with 'main'.

my %TYPE_RE = ( errors => qr/^[EFC]/,
		fatal => qr/^[EC]/,
		nonfatal => qr/^[FW]/,
		warnings => qr/^W/,
		all => qr/^[EFCW]/ );

my $csel_pattern = qr{ ^ (?: main$|_$|all$|& ) }xs;
my $ctyp_pattern = qr{ ^ (?: errors|fatal|nonfatal|warnings ) $ }xs;

sub conditions {
    
    my ($edt, @params) = @_;
    
    local ($_);
    
    # First extract the selector and type from the parameters. They can occur in either
    # order. Both are optional, defaulting to 'all'.
    
    my $selector = 'all';
    my $type = 'all';
    
    if ( $params[0] )
    {
	if ( $params[0] =~ $csel_pattern )
	{
	    $selector = $params[0];
	}
	
	elsif ( $params[0] =~ $ctyp_pattern )
	{
	    $type = $params[0];
	}
	
	elsif ( $params[0] ne 'all' )
	{
	    croak "'$params[0]' is not a valid selector or condition type";
	}
    }
    
    if ( $params[1] )
    {
	if ( $params[1] =~ $csel_pattern && $selector eq 'all' )
	{
	    $selector = $params[1];
	}

	elsif ( $params[1] =~ $ctyp_pattern && $type eq 'all' )
	{
	    $type = $params[1];
	}

	elsif ( $params[1] ne 'all' )
	{
	    croak "'$params[1]' is not a valid selector or condition type";
	}
    }
    
    # Get the proper regexp to pull out the desired conditions.
    
    my $filter = $TYPE_RE{$type} || $TYPE_RE{all};
    
    # If the selector is 'main', grep through the main conditions list. If the
    # selector is '_' and there are no actions yet, return the main
    # conditions because those are the latest ones to be added.
    
    if ( $selector eq 'main' || $selector eq '_' && $edt->_action_list == 0 )
    {
    	if ( wantarray )
    	{
    	    return map { $edt->condition_string($_->@*) }
		grep { ref $_ eq 'ARRAY' && $_->[1] =~ $filter }
		$edt->_main_conditions;
    	}
	
    	else
    	{
    	    return grep { ref $_ eq 'ARRAY' && $_->[1] =~ $filter } $edt->_main_conditions;
    	}
    }
    
    # For '_', we return either or both of the 'errors' and 'warning' lists from
    # the current action. For an action reference, we use the corresponding action.
    
    elsif ( $selector =~ /^_$|^&/ )
    {
	my $action;
	
	if ( $selector eq '_' )
	{
	    $action = $edt->{current_action} || return;
	}
	
	else
	{
	    $action = $edt->{action_ref}{$selector} || return;
	}
	
	if ( wantarray )
	{
	    return map { $edt->condition_string($_->@*) }
		grep { ref $_ eq 'ARRAY' && $_->[1] =~ $filter }
		$action->conditions;
	}
	
	else
	{
	    return grep { ref $_ eq 'ARRAY' && $_->[1] =~ $filter } $action->conditions;
	}
    }
    
    # For 'all' in list context, we grep both the main conditions list and the one for
    # every action.
    
    elsif ( wantarray )
    {
	return map { $edt->condition_string($_->@*) }
	    grep { ref $_ eq 'ARRAY' && $_->[1] =~ $filter }
	    $edt->_main_conditions,
	    map { $_->status ne 'skipped' ? $_->conditions : () } $edt->_action_list;
    }
    
    # For 'all' in scalar context, return the count(s) that correspond to $type.
    
    elsif ( $type eq 'errors' )
    {
	return $edt->{error_count} + $edt->{demoted_count};
    }
    
    elsif ( $type eq 'warnings' )
    {
	return $edt->{warning_count};
    }
    
    elsif ( $type eq 'fatal' )
    {
	return $edt->{error_count};
    }
    
    elsif ( $type eq 'nonfatal' )
    {
	return $edt->{warning_count} + $edt->{demoted_count};
    }
    
    else
    {
	return $edt->{error_count} + $edt->{demoted_count} + $edt->{warning_count};
    }
}


sub _main_conditions {

    return ref $_[0]{conditions} eq 'ARRAY' ? $_[0]{conditions}->@* : ();
}


# condition_string ( condition )
#
# Return a stringified version of the specified condition tuple (action, code, parameters...).

sub condition_string {

    my ($edt, $label, $code, @params) = @_;
    
    # If no code was given, return undefined.
    
    return unless $code;
    
    # If this condition is associated with an action, include the action's label in
    # parentheses.
    
    my $labelstr = defined $label && $label ne '' ? " ($label)" : "";
    
    if ( my $msg = $edt->condition_message($code, @params) )
    {
	return "${code}${labelstr}: ${msg}";
    }
    
    else
    {
	return "${code}${labelstr}";
    }
}


sub condition_nolabel {

    my ($edt, $label, $code, @params) = @_;
    
    return unless $code;
    
    if ( my $msg = $edt->condition_message($code, @params) )
    {
	return "${code}: ${msg}";
    }
    
    else
    {
	return $code;
    }
}


# has_errors ( )
#
# If this EditTransaction has accumulated any fatal errors, return the count. Otherwise, return
# false.

sub has_errors {

    return $_[0]{error_count} || 0;
}


# errors ( [selector] )
# 
# This is provided for backward compatibility. When called with no arguments in scalar
# context, it efficiently returns the error condition count. Otherwise, the argument is
# passed to the 'conditions' method.

sub errors {
    
    my ($edt, $selector) = @_;

    if ( wantarray || $selector )
    {
	return $edt->conditions($selector || 'all', 'errors');
    }

    else
    {
	return $edt->{error_count} + $edt->{demoted_count};
    }
}


# warnings ( [selector] )
# 
# Like 'errors', this method is provided for backward compatibility. When called with no
# arguments in scalar context, it efficiently returns the warning condition
# count. Otherwise, the argument is passed to the 'conditions' method. 

sub warnings {

    my ($edt, $selector) = @_;
    
    if ( wantarray || $selector )
    {
	return $edt->conditions($selector || 'all', 'warnings');
    }
    
    else
    {
	return $edt->{warning_count};
    }
}


# error_strings ( ) and warning_strings ( )
#
# These are deprecated aliases for 'errors' and 'warnings'.

sub error_strings {

    goto &errors;
}


sub warning_strings {

    goto &warnings;
}


# fatals ( [selector] )
#
# When called with no arguments in scalar context, this method efficiently returns the fatal
# condition count. Otherwise, the argument is passed to the 'conditions' method.

sub fatals {

    my ($edt, $selector) = @_;

    if ( wantarray || $selector )
    {
	return $edt->conditions($selector || 'all', 'fatal');
    }

    else
    {
	return $edt->{error_count};
    }
}


# nonfatals ( [selector] )
#
# When called with no arguments in scalar context, this method efficiently returns the nonfatal
# condition count. Otherwise, the argument is passed to the 'conditions' method.

sub nonfatals {

    my ($edt, $selector) = @_;

    if ( wantarray || $selector )
    {
	return $edt->conditions($selector || 'all', 'nonfatal');
    }

    else
    {
	return $edt->{warning_count} + $edt->{demoted_count};
    }
}


# has_condition_code ( code... )
#
# Return true if any of the specified codes have been attached to the current transaction.

sub has_condition_code {
    
    my $edt = shift;
    
    local ($_);
    
    # Return true if any of the following codes are found.
    
    return any { 1; $edt->{condition_code}{$_}; } @_;
}


# condition_message ( code, [parameters...] )
# 
# This routine generates an error message from a condition code and optinal associated
# parameters.

sub condition_message {
    
    my ($edt, $code, @params) = @_;
    
    # If the code was altered because of the PROCEED allowance, change it back
    # so we can look up the proper template.
    
    my $lookup = $code;
    substr($lookup,0,1) =~ tr/F/E/;
    
    # Look up the template according to the specified code and first parameter.  This may
    # return one or more templates.
    
    my @templates = $edt->get_condition_template($lookup, $params[0]);
    
    # Remove any undefined values from the end of the parameter list, so that the proper template
    # will be selected for the parameters given.
    
    pop @params while @params > 0 && ! defined $params[-1];
    
    # Run down the list until we find a template for which all of the required parameters have values.

  TEMPLATE:
    foreach my $tpl ( @templates )
    {
	if ( defined $tpl && $tpl ne '' )
	{
	    my @required = $tpl =~ /[&](\d)/g;
	    
	    foreach my $n ( @required )
	    {
		next TEMPLATE unless defined $params[$n-1] && $params[$n-1] ne '';
	    }
	    
	    $tpl =~ s/ [&](\d) / &_squash_param($params[$1-1]) /xseg;
	    return $tpl;
	}
    }
    
    # If none of the templates are fulfilled, concatenate the parameters with a space
    # between each one.
    
    return join(' ', @params);
}


# _squash_param ( param )
#
# Return a value suitable for inclusion into a message template. If the parameter value is longer
# than 40 characters, it is truncated and ellipses are appended. If the value is not defined, then
# 'UNKNOWN' is returned.

sub _squash_param {

    if ( defined $_[0] && length($_[0]) > 80 )
    {
	return substr($_[0],0,80) . '...';
    }
    
    else
    {
	return $_[0] // 'UNKNOWN';
    }
}


# get_condition_template ( code, selector, param_count )
#
# Given a code, a table, and an optional selector string, return a message template.  This method
# is designed to be overridden by subclasses, but the override methods must call
# SUPER::get_condition_template if they cannot find a template for their particular class that
# corresponds to the information they are given.

sub get_condition_template {

    my ($edt, $code, $selector) = @_;
    
    my $template = $CONDITION_BY_CLASS{ref $edt}{$code} //
	           $CONDITION_BY_CLASS{EditTransaction}{$code};
    
    if ( ref $template eq 'HASH' && $template->{$selector} )
    {
	$template = $template->{$selector};
    }
    
    elsif ( ref $template eq 'HASH' && $template->{default} )
    {
	$template = $template->{default};
    }
    
    # If we have reached a string value, return it. If it is a non-empty list, return
    # the list contents.
    
    if ( $template && ref $template eq 'ARRAY' && $template->@* )
    {
	return $template->@*;
    }
    
    elsif ( defined $template && ! ref $template )
    {
	return $template;
    }
    
    # Otherwise, return the UNKNOWN template.
    
    else
    {
	return $selector ? $CONDITION_BY_CLASS{EditTransaction}{'UNKNOWN'} . " for '$selector'"
	    : $CONDITION_BY_CLASS{EditTransaction}{'UNKNOWN'} . " for 'code'";
    }
}


# import_conditions ( action, external_condition... )
# 
# Import information generated elsewhere that represents error or warning conditions, and add one
# or more condition records to the specified action.

sub import_conditions {

    my ($edt, $action, @arguments) = @_;
    
    # Add every condition specified in the argument list to the current transaction and the
    # current action. Each element might be a list of lists, or a single list, or a list of strings,
    # or a single string.
    
    my @conditions;
    
    foreach my $arg ( @arguments )
    {
	if ( ref $arg eq 'ARRAY' && $arg->@* )
	{
	    if ( $arg->[0] && $arg->[0] =~ $CONDITION_CODE_START )
	    {
		if ( $arg->[1] && $arg->[1] =~ $CONDITION_CODE_START )
		{
		    push @conditions, $arg->@*;
		}
		
		else
		{
		    push @conditions, $arg;
		}
	    }
	    
	    elsif ( ref $arg->[0] eq 'ARRAY' )
	    {
		push @conditions, $arg->@*;
	    }
	    
	    else
	    {
		push @conditions, ['E_BAD_CONDITION', "Unrecognized data format for import", ref $arg];
		last;
	    }
	}
	
	elsif ( ref $arg )
	{
	    push @conditions, ['E_BAD_CONDITION', "Unrecognized data format for import", ref $arg];
	    last;
	}
	
	elsif ( $arg && $arg =~ $CONDITION_CODE_START )
	{
	    push @conditions, $arg;
	}
	
	else
	{
	    push @conditions, ['E_BAD_CONDITION', "Unrecognized data format for import", $arg];
	    last;
	}
    }
    
    foreach my $c ( @conditions )
    {
	if ( ref $c eq 'ARRAY' )
	{
	    my $code = shift $c->@*;
	    
	    if ( $code && $code =~ $CONDITION_CODE_LOOSE )
	    {
		$edt->_import_condition($action, $code, $c->@*);
	    }
	    
	    else
	    {
		$edt->add_condition($action, 'E_BAD_CONDITION', "Invalid condition code", $code);
	    }
	}
	
	elsif ( $c =~ $CONDITION_LINE_IMPORT )
	{
	    $edt->_import_condition($action, $1, $2);
	}
	
	elsif ( $c )
	{
	    $c =~ qr{ ^ (\w+) }xs;
	    $edt->add_condition($action, 'E_BAD_CONDITION', "Invalid condition code", $1);
	}
    }
}


sub _import_condition {

    my ($edt, $action, $code, $message) = @_;
    
    # Return the code to its canonical version.
    
    substr($code, 0, 1) =~ tr/DF/CE/;
    
    # If we have a template corresponding to that code, add the condition as is.
    
    if ( $edt->get_condition_template($code) )
    {
	$edt->add_condition($action, $code, $message);
    }
    
    # Otherwise, change any warning condition to 'W_IMPORTED' and all other conditions to
    # 'E_IMPORTED'. A condition that does not have either an 'E' or a 'W' prefix might be
    # an error, so we assume that it is.
    
    else
    {
	my $newcode = $code =~ /^W/ ? 'W_IMPORTED' : 'E_IMPORTED';
	$edt->add_condition($action, $newcode, "$code: $message");
    }
}


1;
