#!/usr/bin/perl

# written 1/2004 by rjp
# provides a global way to connect to the database.
# replaces older connection.pl file which made use of global variables including the password.

# **************
# Note, VERY IMPORTANT
# This file is *DIFFERENT* on the backup server and the server.
# Make sure to *NOT* copy this back and forth because the wrong database name might be used.
# **************

package DBConnection;

use strict;
use DBI;

# these three lines are the same on both machines.
my $driver =		"mysql";
my $hostName =		"localhost";
my $userName =		"pbdbuser";

# alter this line to the correct database name depending on whether we're running
# on the backup machine or the real server.
# backup = "pbdb_paul"
# server = "pbdb"
my $dbName =		"pbdb_paul";

# the password is stored in a file.  This path will work on both the linux box
# and the XServe since we have a symbolic link set up.
my $passwd = `cat /home/paleodbpasswd/passwd`;
chomp($passwd);  #remove the newline!  Very important!

my $dsn = "DBI:$driver:database=$dbName;host=$hostName";

# return a handle to the database (often called $dbh)
sub connect {
	return (DBI->connect($dsn, $userName, $passwd, {RaiseError=>1}));	
}
 

1;

