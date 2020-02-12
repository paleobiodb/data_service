# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test the module Tester.pm and make sure that it actually conducts
# tests properly.
# 

use strict;
use feature 'unicode_strings';

use Test::Most tests => 1;

use lib 't';

use Tester;
use Test::Conditions;
use Test::Selection;


choose_subtests(@ARGV);

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });


# $$$ we need to fill in tests

pass("placeholder");
