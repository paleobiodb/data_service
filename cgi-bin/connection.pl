# This is the master connection file for use in bridge.pl
# (and any other script that needs to connect to paleodb
# database ).

$driver =	"mysql";
$host =		"localhost";
$user =		"pbdbuser";
$db =		"pbdb_paul";
#$db =		"pbdb_new";

open PASSWD,"</home/paleodbpasswd/passwd";
$password = <PASSWD>;
$password =~ s/\n//;
close PASSWD;

