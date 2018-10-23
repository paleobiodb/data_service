# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test data entry and editing for timescales.
# 

use strict;

use feature 'unicode_strings';
use feature 'fc';

use Data::Dump qw(pp);

use Test::More tests => 2;

use lib qw(lib ../lib t);

use Tester;

use Test::Selection;


# If we provided any command-line arguments, run only subtests whose names match.

choose_subtests(@ARGV);

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });

my ($perm_a, $perm_e);

# Then check to MAKE SURE that the server is in test mode and the test timescale tables are
# enabled. This is very important, because we DO NOT WANT to change the data in the main
# tables. If we don't get the proper response back, we need to bail out. These count as the first
# two tests in this file.

$T->test_mode('session_data', 'enable') || BAIL_OUT("could not select test session data");
# $T->test_mode('timescales', 'enable') || BAIL_OUT("could not select test timescale tables");


subtest 'delete' => sub {
    
    select_subtest || return;
    
    # First, see if a user with the superuser privileges can add. If this fails, there is no
    # reason to go any further.
    
    $T->set_cookie("session_id", "SESSION-WITH-ADMIN");
    
    # my ($m1) = $T->fetch_url("/bounds/replace.json?timescale_id=tsc:141");
    
    # my $bounds = [ { age => '0.00' },
    # 		   { age => '100.', top_id => '@#1', interval_name => 'Cenozoic' },
    # 		   { age => '200', top_id => '@#2', interval_name => 'Cretaceous' } ];
    
    my $bounds = [ { bound_id => 'bnd:3439', age => '0.00' },
    		   { bound_id => 'bnd:3440', age => '100.3', interval_name => 'Cenozoic' },
    		   { bound_id => 'bnd:3441', age => '200', top_id => 'bnd:99999' } ];
    
    my (@r1) = $T->send_records("/bounds/replace.json?timescale_id=tsc:8", "bounds replace", json => $bounds);
    
    print STDERR $T->{last_response}->content;
    
    # # foreach my $r (@r1)
    # # {
    # # 	diag(pp($r));
    # # }
}
