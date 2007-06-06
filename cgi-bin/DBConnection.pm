#!/usr/bin/perl

# written 1/2004 by rjp
# provides a global way to connect to the database.
# replaces older connection.pl file which made use of global variables including the password.

package DBConnection;
use strict;
use DBI;
# return a handle to the database (often called $dbh)

sub connect {
    my $driver =   "mysql";
    my $hostName = "localhost";
    my $userName = "pbdbuser";
    my $dbName =   "pbdb";

    # Make sure a symbolic link to this file always exists;
    my $password = `cat /home/paleodbpasswd/passwd`;
    chomp($password);  #remove the newline!  Very important!
    my $dsn = "DBI:$driver:database=$dbName;host=$hostName";

    my $connection = DBI->connect($dsn, $userName, $password, {RaiseError=>1});
    if (!$connection) {
        die("Could not connect to database");
    } else {
        return $connection;
    }
}


1;

