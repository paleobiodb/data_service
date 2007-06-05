#!/usr/local/bin/perl

use lib "../cgi-bin";
use DBConnection;
use DBTransactionManager;
use TaxaCache;

my $dbh = DBConnection::connect();
my $dbt = new DBTransactionManager($dbh);

TaxaCache::rebuildCache($dbt);

print "Done";
