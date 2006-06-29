#!/usr/bin/perl

use lib "/Volumes/pbdb_RAID/httpdocs/cgi-bin";
use DBConnection;
use DBTransactionManager;
use TimeLookup;

my $dbh = DBConnection::connect();
my $dbt = new DBTransactionManager($dbh);

TimeLookup::generateLookupTable($dbt,1);
