#!/usr/local/bin/perl

use lib "../cgi-bin";
use DBConnection;
use TaxonTrees;
use Taxonomy;
use Getopt::Std;

my $dbh = DBConnection::connect();

my $t = Taxonomy->new($dbh, 'taxon_trees');

my %options;

getopts('abcdefgkx', \%options);

TaxonTrees::buildTables($dbh, 'taxon_trees', { msg_level => 2 }, \%options);

print "done rebuilding caches\n";

