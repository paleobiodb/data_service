#
# PBDB Data Service
# -----------------
#
# This file should be run ON THE DATA SERVICE HOST before running any of the pb2e-*.t files. It
# will ensure proper table permissions for the test users.
# 
# pb2e-10-session-data.t : Set up test user permissions for use by other test files.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 1;

use TableDefs;
use ResourceDefs; # qw($RESOURCE_QUEUE);

use PBDBTester;


# The following calls establish a connection to the database, then create or re-create the
# necessary tables.

my $T = PBDBTester->new;

my $perm_a = $T->new_perm('SESSION-WITH-ADMIN');

$T->establish_session_data;
$T->set_specific_permission('RESOURCE_QUEUE', $perm_a, 'admin');
$T->set_specific_permission('PUBLICATIONS', $perm_a, 'admin');
$T->set_specific_permission('ARCHIVES', $perm_a, 'admin');
# $T->set_specific_permission('TIMESCALE_DATA', $perm_a, 'admin');

