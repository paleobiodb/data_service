#!/usr/local/bin/perl

use lib "../cgi-bin";
use DBConnection;
use TaxonTrees;
use Getopt::Std;

my $dbh = DBConnection::connect();

#my %options;

#getopts('abcdefgkx', \%options);

my $table_name = shift @ARGV;

$TaxonTrees::TREE_TABLE = $table_name;

$dbh->do("DELETE FROM $TaxonTrees::TREE_TABLE");
$dbh->do("INSERT INTO $TaxonTrees::TREE_TABLE SELECT * FROM ${TaxonTrees::TREE_TABLE}bak");

TaxonTrees::adjustTreeSequence($dbh, @ARGV);

#print "done rebuilding caches\n";

