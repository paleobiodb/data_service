#!/usr/local/bin/perl

use lib "../cgi-bin";
use DBConnection;
use TaxonTrees;
use Getopt::Std;
use Data::Dumper;

my $dbh = DBConnection::connect();

my ($method, $taxon_no) = @ARGV;

my (@result) = eval "TaxonTrees::$method(\$dbh, \$taxon_no)";

if ( $@ )
{
    print STDERR "$@";
}
else
{
    print Dumper(@result);
}


