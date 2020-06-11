# 
# Test::Selection.pm
# 
# This module allows you to select one or more subtests out of a large file by
# passing a command-line argument.  This is intended primarily as a
# development aid for use in debugging subtests one at a time while ignoring
# those that are known to work properly.


package Test::Selection;

use Carp qw(croak);
use Test::More;
use Exporter 'import';


our (@EXPORT) = qw(choose_subtests select_subtest select_final check_result);

our (%SELECTED_TEST, %PERFORMED_TEST, $SKIPPED_TESTS);


sub choose_subtests {
    
    my (@testnames) = @_;
    
    %SELECTED_TEST = ();
    %PERFORMECE_TEST = ();
    $SKIPPED_TESTS = 0;
    
    foreach my $name ( @testnames )
    {
	next if $name eq 'debug';
	$SELECTED_TEST{$name} = 1;
    }
}


# select_subtest

sub select_subtest {
    
    # If no argument is given, then check to see if this was called from
    # within a subtest.  If so, use the subtest name.  Otherwise throw an
    # exception. 
    
    my $name = shift;
    
    my $tb = Test::Builder->new;
    
    $name ||= $tb->name || $tb->{Stack}[0]{_meta}{Test::Builder}{child};
    
    # If we can't figure out the name of the test, throw an exception.
    
    unless ( defined $name )
    {
	croak "Could not determine name of subtest";
    }
    
    # If no subtests are selected, just return true.
    
    return 1 unless %SELECTED_TEST;
    
    # If this subtest is selected then output a special diag line and return true.
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;

    if ( $SELECTED_TEST{$name} )
    {
	$PERFORMED_TEST{$name} = 1;
	Test::More::diag("*************** $name ***************");
	return 1;
    }
    
    # Otherwise, this subtest is not selected.  So pass a placeholder test and
    # return false.  The placeholder test prevents the subtest from which this
    # subroutine was called from being flagged with "No tests run!"
    
    Test::More::pass('test not selected');
    $SKIPPED_TESTS++;
    
    return;
}


# select_final ( )
# 
# This subroutine should be called as the very last statement of the test
# file.  If there are selected tests which have not been performed, raise a
# warning!!! 

sub select_final {
    
    my @not_found;
    
    # If no subtests are selected, just pass.
    
    unless ( %SELECTED_TEST )
    {
	ok("Test::Select: No tests were selected, so all were run");
	return;
    }
    
    # Go through all the list of selected tests and see if we actually did
    # them.
    
    foreach my $t ( keys %SELECTED_TEST )
    {
	push @not_found, $t unless $PERFORMED_TEST{$t};
    }
    
    if ( @not_found )
    {
	local $Test::Builder::Level = $Test::Builder::Level + 1;
	
	diag("!!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!!");
	
	foreach my $t ( @not_found )
	{
	    diag("**** $t: not found ****");
	}
	
	fail("Test::Select: Not all of the specified tests were run");
    }
    
    else
    {
	my $selected_tests = keys %SELECTED_TEST;
	my $plural = keys %SELECTED_TEST > 1 ? 's' : '';
	
	diag("**** Ran $selected_tests test${plural}, skipped $SKIPPED_TESTS ****");
	ok("Test::Select: Ran all of the specified tests");
    }
}


# check_result ( arg )
#
# If the specified arg is empty, emit a diagnostic stating "skipping remainder
# of subtest" and return false.  Otherwise, return true.  This can be called
# from any subtest right after the first result is fetched, so that the rest
# of the test can be skipped if that result is empty.  This is designed to be
# called as follows:
# 
#     check_result(@result) || return;
# 
# In order to prevent a "no tests were run" warning, you must generate at
# least one event before calling this subroutine.  Calling fetch_url,
# fetch_records, etc. will do this.

sub check_result {
    goto &_check_result;
}

sub _check_result ($) {
    
    my ($result) = @_;
    
    # If the result is not empty, return true.
    
    if ( $result )
    {
	return 1;
    }
    
    # Otherwise, emit a diagnostic message and return false.  We increment
    # $Test::Builder::Level so the message will be reported at the proper line
    # of the caller.
    
    else
    {
	local $Test::Builder::Level = $Test::Builder::Level + 1;
	diag("skipping remainder of subtest");
	return;
    }
}

1;
