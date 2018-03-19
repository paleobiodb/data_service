#
# PBDB Data Service
# -----------------
# 
# This file contains unit tests for the EditTransaction class.
# 
# edt-03-tester.t : Test the class EditTester, which is used in all of the other tests. We need to
# make sure that it is actually running the tests properly and not silently flubbing any of htem.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 1;

use TableDefs qw($EDT_TEST $EDT_AUX $EDT_ANY
		 get_table_property set_table_property set_column_property);

use TableData qw(reset_cached_column_properties);

use EditTest;
use EditTester;

use Carp qw(croak);


# The following call establishes a connection to the database, using EditTester.pm.

my $T;


# Test that we can create EditTester objects, and check the basic accessor methods of the class.

subtest 'basic' => sub {
    
    
    pass('placeholder');
}
