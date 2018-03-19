#
# PBDB Data Service
# -----------------
# 
# This file contains unit tests for the EditTransaction class.
# 
# edt-15-interlock.t : Test that starting an EditTransaction while another one is already active
# will abort the first one.
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

my $T = EditTester->new;





# check that starting a new EditTransaction while a previous one is active will abort it.


pass('placeholder');
