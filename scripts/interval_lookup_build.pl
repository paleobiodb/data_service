#!/usr/local/bin/perl

use FindBin qw($Bin);
use lib "$Bin/../cgi-bin";
use DBTransactionManager;
use TimeLookup;

my $dbt = new DBTransactionManager();
my $t = new TimeLookup($dbt);
$t->generateLookupTable($dbt,1);
