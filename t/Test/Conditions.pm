# 
# Test::Conditions.pm
# 
# This module allows you to set and clear and arbitrary set of conditions.  Its purpose is to
# facilitate testing large data structures, for example trees and lists, without generating
# enormous numbers of individual tests.  Instead, you can create a Test::Conditions object, and
# then run through the various nodes in the data structure running a series of checks on each
# node.


package Test::Conditions;

use Carp qw(croak);
use Test::More;

use namespace::clean;


# new ( args... )
# 
# Create a new Test::Conditions object, with the specified arguments.
# Currently, the only argument accepted is 'limit'.

sub new {
    
    my ( $class, %args ) = @_;
    
    my $new = { limit => 0,
		label => { },
		count => { },
		tested => { },
	      };
    
    bless $new, $class;
    
    foreach my $k ( keys %args )
    {
	if ( $k eq 'limit' )
	{
	    $new->{limit} = $args{limit};
	}
	
	else
	{
	    croak "unknown argument '$k'";
	}
    }
    
    return $new;
}


sub limit_max {
    
    my ($tc, %limits) = @_;
    
    foreach my $k ( keys %limits )
    {
	croak "bad key '$k'" unless defined $k && $k ne '';
	croak "odd number of arguments or undefined argument" unless defined $limits{$k};
	croak "limit values must be nonnegative integers" unless $limits{$k} =~ /^\d+$/;
    }
    
    foreach my $k ( keys %limits )
    {
	if ( $k eq 'DEFAULT' )
	{
	    $tc->{limit} = $limits{$k};
	}
	
	else
	{
	    $tc->{exlimit}{$k} = $limits{$k};
	}
    }
}


sub set_limit {

    goto &limit_max;
}


sub get_limit {
    
    my ($tc, $key) = @_;
    
    return $tc->{exlimit}{$key} if defined $key && $key ne '';
    return $tc->{limit};
}


sub expect_min {

    my ($tc, %expect) = @_;
    
    foreach my $k ( keys %expect )
    {
	croak "bad key '$k'" unless defined $k && $k ne '';
	croak "odd number of arguments or undefined argument" unless defined $expect{$k};
	croak "expect values must be nonnegative integers" unless $expect{$k} =~ /^\d+$/;
    }
    
    foreach my $k ( keys %expect )
    {
	$tc->{expect}{$k} = $expect{$k};
    }
}


sub expect {
    
    my ($tc, @expect) = @_;
    
    my %e = map { $_ => 1 } @expect;
    
    $tc->expect_min(%e);
}


sub get_expect {
    
    my ($tc, $key) = @_;
    
    return $tc->{expect}{$key} if defined $key && $key ne '';
    return;
}


sub set {
    
    my ($tc, $key) = @_;
    
    croak "you must specify a non-empty key" unless defined $key && $key ne '';
    
    if ( $tc->{tested}{$key} )
    {
	delete $tc->{label}{$key};
	delete $tc->{count}{$key};
	delete $tc->{tested}{$key};
    }
    
    $tc->{set}{$key} = 1;
}


sub flag {
    
    my ($tc, $key, $label) = @_;
    
    # croak "you must specify a label" unless defined $label;
    croak "you must specify a non-empty key" unless defined $key && $key ne '';
    
    $tc->set($key);
    
    $tc->{count}{$key}++;
    $tc->{label}{$key} ||= $label if defined $label;
}


sub decrement {
    
    my ($tc, $key) = @_;
    
    croak "you must specify a non-empty key" unless defined $key && $key ne '';
    croak "key '$key' is not set" unless $tc->{count}{$key};
    
    $tc->{count}{$key}--;
}


sub clear {
    
    my ($tc, $key) = @_;
    
    croak "you must specify a non-empty key" unless defined $key && $key ne '';
    
    if ( $tc->{tested}{$key} )
    {
	delete $tc->{tested}{$key};
	$tc->{set}{$key} == -1;
    }
    
    elsif ( defined $tc->{set}{$key} && $tc->{set}{$key} >= 0 )
    {
	$tc->{set}{$key} = 0;
    }
    
    else
    {
	$tc->{set}{$key} = -1;
    }
    
    delete $tc->{count}{$key};
    delete $tc->{label}{$key};
}


sub list_untested_keys {

    my ($tc) = @_;
    
    return unless ref $tc->{set} eq 'HASH';
    return grep { ! $tc->{tested}{$_} && $tc->{set}{$_} } keys %{$tc->{set}};
}


sub list_expected_keys {
    
    my ($tc) = @_;
    
    return unless ref $tc->{expect} eq 'HASH';
    return keys %{$tc->{expect}};
}


sub list_all_keys {
    
    my ($tc) = @_;
    
    return unless ref $tc->{set} eq 'HASH';
    return grep { $tc->{set}{$_} } keys %{$tc->{set}};
}


sub is_set {
    
    my ($tc, $key) = @_;
    
    return $tc->{set}{$key};
}


sub is_tested {
    
    my ($tc, $key) = @_;
    
    return $tc->{tested}{$key};
}


sub get_count {

    my ($tc, $key) = @_;
    
    return $tc->{count}{$key};
}


sub get_label {

    my ($tc, $key) = @_;
    
    return unless exists $tc->{label}{$key};
    return defined $tc->{label}{$key} ? $tc->{label}{$key} : 'unknown';
}


sub get_limit {
    
    my ($tc, $key) = @_;
    
    return $tc->{exlimit}{$key} if defined $tc->{exlimit}{$key};
    return $tc->{limit} if defined $tc->{limit};
    return 0;
}


sub ok_all {

    my ($tc, $message) = @_;
    
    croak "you must specify a message" unless $message;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my (@fail, @ok, %found);
    
 KEY:
    foreach my $k ( $tc->list_untested_keys )
    {
	my $count = $tc->get_count($k);
	my $limit = $tc->get_limit($k);
	my $expect = $tc->get_expect($k);
	my $label = $tc->get_label($k);
	
	$tc->{tested}{$k} = 1;
	$found{$k} = 1;
	
	if ( $expect )
	{
	    next KEY if $expect == 1;
	    next KEY if defined $count && $count >= $expect;
	    
	    my $m = "    Condition '$k': found $count instance";
	    $m .= "s" if $count > 1;
	    $m .= ", expected at least $expect";
	    
	    push @fail, $m;
	}
	
	elsif ( defined $count && $count <= $limit )
	{
	    my $m = "    Condition '$k': flagged $count instance";
	    $m .= "s" if $count > 1;
	    $m .= " ('$label')" if defined $label & $label ne '';
	    $m .= " (limit $limit)" if $limit;
	    
	    push @ok, $m;
	}
	
	elsif ( defined $count )
	{
	    my $m = "    Condition '$k': flagged $count instance";
	    $m .= "s" if $count > 1;
	    $m .= " ('$label')" if defined $label & $label ne '';
	    
	    push @fail, $m;
	}
	
	elsif ( $tc->{set}{$k} && $tc->{set}{$k} == -1 )
	{
	    push @fail, "    Condition '$k': cleared without being set";
	}
	
	else
	{
	    push @fail, "    Condition '$k'";
	}
    }
    
    # Now deal with the conditions we found
    
    foreach my $k ( $tc->list_expected_keys )
    {
	unless ( $found{$k} )
	{
	    my $e = $tc->get_expect($k);
	    my $m = "    Condition '$k': found no instances, expected at least $e";
	    push @fail, $m;
	}	
    }
    
    if ( @fail )
    {
	fail($message);
	diag($_) foreach @fail;
	
	if ( @ok )
	{
	    diag("This test also generated the following warnings:");
	    diag($_) foreach @ok;
	}
    }
    
    elsif ( @ok )
    {
	pass($message);
	diag("Passed test '$message' with warnings:");
	diag($_) foreach @ok;
    }
    
    else
    {
	pass($message);
    }
}


sub ok_key {

    my ($tc, $key, $message) = @_;

    croak "you must specify a message" unless $message;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    my $set = $tc->is_set($key);
    my $count = $tc->get_count($key);
    my $limit = $tc->get_limit($key);
    
    if ( ! $set )
    {
	pass($message);
    }
    
    elsif ( defined $count && $count <= $limit )
    {
	pass($message);
	diag("Passed test '$message' with warning:");
	
	my $m = "    Condition '$key': flagged $count instance";
	$m .= "s" if $count > 1;
	$m .= " ('$label')" if defined $label & $label ne '';
	
	diag($m);
    }
    
    elsif ( defined $count )
    {
	fail($message);
	
	my $m = "    Condition '$key': flagged $count instance";
	$m .= "s" if $count > 1;
	$m .= " ('$label')" if defined $label & $label ne '';
	
	diag($m);
    }
    
    elsif ( $tc->{set}{$k} && $tc->{set}{$k} == -1 )
    {
	fail($message);
	diag("    Condition '$k': cleared without being set");
    }
    
    else
    {
	fail($message);
	diag("    Condition '$k'");
    }

    $tc->{tested}{$key} = 1;
}


sub reset_keys {
    
    my ($tc) = @_;
    
    $tc->{set} = { };
    $tc->{label} = { };
    $tc->{count} = { };
    $tc->{tested} = { };
}


sub reset_key {
    
    my ($tc, $key) = @_;
    
    croak "you must specify a non-empty key" unless defined $key && $key ne '';
    
    delete $tc->{set}{$key};
    delete $tc->{label}{$key};
    delete $tc->{count}{$key};
    delete $tc->{tested}{$key};
}


sub reset_limits {
    
    my ($tc) = @_;
    
    # Remove any limits and expects that were set for this instance.
    
    $tc->{exlimit} = { };
}


sub reset_expects {
    
    my ($tc) = @_;
    
    $tc->{expect} = { };
}


=head1 NAME

Test::Conditions - test for multiple conditions in a simple and compact way

=head1 SYNOPSIS

    $tc = Test::Conditions->new;
    
    foreach my $node ( @list )
    {
        $tc->flag('foo', $node->{name})
            unless defined $node->{foo};
        $tc->flag('bar', $node->{name})
            unless defined $node->{bar} && $node->{bar} > 0;
    }
    
    $tc->ok_all("all nodes have proper attributes");

=head1 DESCRIPTION

The purpose of this module is to facilitate testing complex data structures
such as trees or lists of hashes.  You may want to run certain tests on each
node of the structure, and report the results in a compact way.  You might,
for example, wish to test a list or other structure with 1,000 nodes and
report the result as a single event rather than some multiple of 1,000 event.
If so, this module can do that.

An object of class Test::Conditions can keep track of any number of
conditions, and reports a single event when its 'ok_all' method is called:
FAIL if one or more conditions are set, and OK if none are.  Each condition
which is set is reported as a separate diagnostic message.  Futhermore, if the
nodes or other pieces of the data structure have unique identifiers, you can
easily arrange for Test::Conditions to report the identifier of one of the
failing nodes to help you in diagnosing the problem.

=head1 METHODS

=head2 Class methods

=head3 new ( parameters... )

Examples:

    $tc = Test::Conditions->new;

    $tc2 = Test::Conditions->new( limit => 50 );

Creates a new Test::Conditions object with the specified parameters.  The
parameters currently accepted are:

=head4 limit

Sets the default limit for all conditions under this object to the specified
value.

=head2 Instance methods

=head3 set ( condition )

Sets the specified condition.  The single argument must be a scalar whose
value is the name of the condition to be set.

=head3 clear ( condition )

Clears the specified condition.  The single argument must be a scalar whose
value is the anme of the condition to be cleared.

=head3 flag ( condition, label )

Sets the specified condition, and records $$$

=cut

1;
