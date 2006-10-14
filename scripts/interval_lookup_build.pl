#!/usr/bin/perl

use lib "../cgi-bin";
use DBConnection;
use DBTransactionManager;
use TimeLookup;

my $dbh = DBConnection::connect();
my $dbt = new DBTransactionManager($dbh);

my $t = new TimeLookup($dbt);
$t->generateLookupTable($dbt,1);
