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

our (%SELECTED_TEST, %PERFORMED_TEST);


# sub import {

#     my ($pkg, $selector) = @_;
    
#     my $callpkg = caller(0);
    
#     *{"$callpkg\::choose_subtests"} = \&choose_subtests;
#     *{"$callpkg\::select_subtest"} = \&select_subtest;
#     *{"$callpkg\::select_final"} = \&select_final;
# }


sub choose_subtests {
    
    my (@testnames) = @_;
    
    foreach my $name ( @testnames )
    {
	$SELECTED_TEST{$name} = 1;
    }
    
    # if ( $selector eq ':args' )
    # {
    # 	foreach my $arg ( @ARGV )
    # 	{
    # 	    $SELECTED_TEST{$arg} = 1;
    # 	}
    # }
    
    # If we were called with an argument starting with '--', i.e. '--select',
    # then look for arguments of the form "--select=testname".  If no
    # arguments of this form are found, then all subtests will be selected implicitly.
    
    # elsif ( $selector && $selector =~ qr{ ^ -- (\w+) }xs )
    # {
    # 	my $argname = $1;
	
    # 	foreach my $arg ( @ARGV )
    # 	{
    # 	    if ( $arg =~ qr{ ^ $argname = (.*) }xso )
    # 	    {
    # 		$SELECTED_TEST{$1};
    # 	    }
    # 	}
    # }
    
    # else
    # {
    # 	foreach my $name ( $selector, @testnames )
    # 	{
    # 	    $SELECTED_TEST{$name} = 1;
    # 	}
    # }
}


# select_subtest

sub select_subtest {
    
    # If no argument is given, then check to see if this was called from
    # within a subtest.  If so, use the subtest name.  Otherwise throw an
    # exception. 
    
    my $name = shift;
    
    my $tb = Test::Builder->new;
    my $in_subtest;
    
    # if ( $tb->{Parent} )
    # {
	$name ||= $tb->name;
    # 	$in_subtest = 1;
    # }
    
    # elsif ( ! $name )
    # {
    # 	croak "select_subtest must have an argument if called outside of a subtest\n";
    # }
    
    # If no subtests are selected, just return true.
    
    if ( ! keys %SELECTED_TEST )
    {
	return 1;
    }
    
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
    
    Test::More::pass('test not selected') if $in_subtest;
    return;
}


# select_final ( )
# 
# This subroutine should be called as the very last statement of the test
# file.  If there are selected tests which have not been performed, raise a
# warning!!! 

sub select_final {
    
    my @not_found;
    
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
	    fail("$t: not found");
	}
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
