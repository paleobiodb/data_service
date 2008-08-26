#!/usr/local/bin/perl

use lib "../cgi-bin";
use DBConnection;
use DBTransactionManager;
use TaxaCache;

my $dbh = DBConnection::connect();
my $dbt = new DBTransactionManager($dbh);

TaxaCache::rebuildCache($dbt);
TaxaCache::cleanListCache($dbt);
TaxaCache::rebuildListCache($dbt);

print "done rebuilding caches\n";

