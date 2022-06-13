# 
# The Paleobiology Database
# 
#   EditTransaction::Condition - class for error and warning conditions.
# 


package EditTransaction::Condition;

use strict;

use Scalar::Util 'weaken';

use namespace::clean;


# new ( action, code, data... )
#
# Create a new EditTransaction::Condition for the specified action, which may be undef. The second
# argument must be a condition code, i.e. 'E_PERM' or 'W_NOT_FOUND'. The remaining arguments, if
# any, indicate the particulars of the condition and are used in generating a string value from
# the condition record.

sub new {
    
    my $class = shift;
    
    my $new = bless [ @_ ], $class;
    weaken $new->[0] if ref $new->[0];
    
    return $new;
}


# action ( )
#
# Return the action associated with this condition object.

sub action {

    return $_[0][0];
}


# code ( )
#
# Return the code associated with this error condition.

sub code {

    return $_[0][1];
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

