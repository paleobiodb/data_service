#!/usr/bin/perl

# written 1/2004 by rjp
# provides a global way to connect to the database.
# replaces older connection.pl file which made use of global variables including the password.

# Note: now does a hostname lookup to figure out which machine it's on.. So it's 
# now safe (as of 3/11/2004) to copy this file to both servers.

package DBConnection;

use strict;
use DBI;

# these three lines are the same on both machines.
my $driver =		"mysql";
my $hostName =		"localhost";
my $userName =		"pbdbuser";


# figure out if we're on the backup server or the real server..

# backup = "pbdb_paul"
# server = "pbdb"
my $dbName = "pbdb";

# something like flatpebble.nceas.ucsb.edu, or paleobackup.nceas.ucsb.edu..
#my $hostname = `hostname`;  

# if ($hostname =~ m/paleobackup/) {
#	$dbName = "pbdb_paul";	
#} else {
#	$dbName = "pbdb";	# the live server.	
#}


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

