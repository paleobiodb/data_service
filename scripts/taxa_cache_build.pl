#!/usr/bin/perl

use "../cgi-bin";
use DBConnection;
use DBTransactionManager;
use TaxaCache;

my $dbh = DBConnection::connect();
my $dbt = new DBTransactionManager($dbh);

TaxaCache::rebuildCache($dbt);

print "Done";
