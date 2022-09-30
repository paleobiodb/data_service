#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the EditTransaction class and the
# EditTransaction::Mod::MariaDB plugin module, using the subclass ETBasicTest
# whose sole purpose is to enable these tests.
# 
# validation.t :
# 
#         Test the methods used for validating records. All action parameters
#         are validated against the corresponding database columns. If the
#         parameter value cannot be stored in the database column, an error or
#         warning condition will be generated.
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 1;

use ETBasicTest;
use ETTrivialClass;
use EditTester qw(ok_eval ok_exception ok_new_edt clear_table sql_command
		  last_result last_result_list ok_has_condition ok_has_error
		  ok_found_record ok_no_record ok_count_records fetch_records);


# Establish an EditTester instance.

$DB::single = 1;

my $T = EditTester->new('ETBasicTest');


# Define a convenience subroutine for creating and validating an action.
