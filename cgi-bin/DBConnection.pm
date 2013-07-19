#!/usr/local/bin/perl

# written 1/2004 by rjp
# provides a global way to connect to the database.
# replaces older connection.pl file which made use of global variables including the password.

package DBConnection;
use strict;
use DBI;
use Constants qw($SQL_DB $DB_USER $DB_SOCKET $DB_PASSWD);
# return a handle to the database (often called $dbh)

sub connect {
    my $driver =   "mysql";
    my $hostName = "localhost";

    my $dsn;
    if ( $DB_SOCKET )	{
        $dsn = "DBI:$driver:database=$SQL_DB;host=$hostName;mysql_socket=$DB_SOCKET;mysql_client_found_rows=0";
    } else	{
        $dsn = "DBI:$driver:database=$SQL_DB;host=$hostName;mysql_client_found_rows=0";
    }

    my $connection;
    if ( $DB_PASSWD )	{
        $connection = DBI->connect($dsn, $DB_USER, $DB_PASSWD, {RaiseError=>1});
    } else	{
        my $password = `cat /home/paleodbpasswd/passwd`;
        chomp($password);  #remove the newline!  Very important!
        $connection = DBI->connect($dsn, $DB_USER, $password, {RaiseError=>1});
    }
    if (!$connection) {
        die("Could not connect to database");
    } else {
        return $connection;
    }
}


1;

