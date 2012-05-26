#!/usr/local/bin/perl

use lib "../cgi-bin";
use DBConnection;
use TaxonTrees;
use Getopt::Std;

my $dbh = DBConnection::connect();

my %options;
my @values;

getopts('oct', \%options);

push @values, @ARGV;

my $string = join(', ', @values);

if ( $options{'o'} )
{
    TaxonTrees::update($dbh, undef, \@values, 2, 1);
}

elsif ( $options{'c'} )
{
    TaxonTrees::update($dbh, \@values, undef, 2, 1);
}

elsif ( $options{'t'} )
{
    TaxonTrees::check($dbh, 2);
}

