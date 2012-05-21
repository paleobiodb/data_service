#!/usr/local/bin/perl

use lib "../cgi-bin";
use DBConnection;
use TaxonTrees;
use Getopt::Std;

my $dbh = DBConnection::connect();

my %options;

getopts('abcdefgkx', \%options);

TaxonTrees::build($dbh, 2, \%options);

print "done rebuilding caches\n";

