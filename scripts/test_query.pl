#!/usr/local/bin/perl

use lib "../cgi-bin";
use DBConnection;
use Taxonomy;
use Getopt::Std;
use Data::Dumper;

my $dbh = DBConnection::connect();

my $t = Taxonomy->new($dbh, 'taxon_trees');

my $a = 1;

if ( $@ )
{
    print STDERR "$@";
}
else
{
    print Dumper(@result);
}


