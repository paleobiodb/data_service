#!/usr/bin/perl

# written 1/2004 by rjp
# provides a global way to connect to the database.
# replaces older connection.pl file which made use of global variables including the password.

package DBConnection;
use strict;
use DBI;
use Constants qw($DB_SOCKET $DB_PASSWD);
# return a handle to the database (often called $dbh)

sub connect {
    my $driver =   "mysql";
    my $hostName = "localhost";
    my $userName = "pbdbuser";
    my $dbName =   "pbdb";

    my $dsn;
    if ( $DB_SOCKET )	{
        $dsn = "DBI:$driver:database=$dbName;host=$hostName;mysql_socket=$DB_SOCKET";
    } else	{
        $dsn = "DBI:$driver:database=$dbName;host=$hostName";
    }

    my $connection;
    if ( $DB_PASSWD )	{
        $connection = DBI->connect($dsn, $userName, $DB_PASSWD, {RaiseError=>1});
    } else	{
        my $password = `cat /home/paleodbpasswd/passwd`;
        chomp($password);  #remove the newline!  Very important!
        $connection = DBI->connect($dsn, $userName, $password, {RaiseError=>1});
    }
    if (!$connection) {
        die("Could not connect to database");
    } else {
        return $connection;
    }
}


1;

