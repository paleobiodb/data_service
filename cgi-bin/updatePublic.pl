#!/usr/bin/perl
# original version by JA 17.3.02
# completely rewritten by Garot June 2002
# updates list and occurrence counts on public main page
# suggested by T. Olszewski
# also prints count of different researchers JA 9.5.02
# prints same data to paleodb/index page JA 10.5.02
# image randomization routine modified by rjp, 12/03.

# NOTE: assumes /etc/cron.hourly will call the script

use DBI;
use DBConnection;

$IMGDIR="/pbdb/html/public/images";

my $DEBUG = 0;
my $sql;
my $sth;
my @stats;

my $dbh = DBConnection::connect();

$sql = "SELECT count(*) FROM refs";
$sth = $dbh->prepare( $sql ) || die ( "$sql\n$!" );
$sth->execute();
@stats = $sth->fetchrow_array();
my $reference_total = $stats[0];
$sth->finish();

$sql = "SELECT count(*) FROM collections";
$sth = $dbh->prepare( $sql ) || die ( "$sql\n$!" );
$sth->execute();
@stats = $sth->fetchrow_array();
my $collection_total = $stats[0];
$sth->finish();

$sql = "SELECT count(*) FROM occurrences";
$sth = $dbh->prepare( $sql ) || die ( "$sql\n$!" );
$sth->execute();
@stats = $sth->fetchrow_array();
my $occurrence_total = $stats[0];
$sth->finish();

$sql = "SELECT count(distinct enterer) FROM refs";
$sth = $dbh->prepare( $sql ) || die ( "$sql\n$!" );
$sth->execute();
@stats = $sth->fetchrow_array();
my $enterer_total = $stats[0];
$sth->finish();

# Now put into our holding tank
$sql =	"UPDATE statistics SET ".
		"		reference_total = $reference_total, ".
		"		collection_total = $collection_total, ".
		"		occurrence_total = $occurrence_total, ".
		"		enterer_total = $enterer_total ";
$dbh->do ( $sql );
if ( $DEBUG ) { print "$sql\n"; }

$dbh->disconnect();


#added by rjp on 12/9/2003

# get a list of all the files in this directory
@images = `ls $IMGDIR/*.jpg`;

# only do the jpegs for now, because who knows what will happen
# if you change the file extension to .gif on a jpeg.

$filename = $images[int(rand $#images)];
chomp($filename);

`cp $filename $IMGDIR/fossil.jpg`;



