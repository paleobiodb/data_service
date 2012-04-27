#!/usr/local/bin/perl

use lib "../cgi-bin";
use DBConnection;
use DBTransactionManager;
use TaxaTree;
use Getopt::Std;

my $dbh = DBConnection::connect();

my %options;

getopts('abcdef', \%options);

TaxaTree::rebuild($dbh, \%options);

print "done rebuilding caches\n";

