#!/usr/local/bin/perl

use lib "../cgi-bin";
use DBConnection;
use DBTransactionManager;
use TaxonTrees;
use Getopt::Std;

my $dbh = DBConnection::connect();

my %options;

getopts('abcdefgh', \%options);

TaxonTrees::rebuild($dbh, \%options);

print "done rebuilding caches\n";

