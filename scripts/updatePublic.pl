#!/usr/bin/perl
# original version by JA 17.3.02
# completely rewritten by Garot June 2002
# updates list and occurrence counts on public main page
# suggested by T. Olszewski
# also prints count of different researchers JA 9.5.02
# prints same data to paleodb/index page JA 10.5.02
# image randomization routine modified by rjp, 12/03.
# added institution and country JA 15.4.04

# NOTE: assumes /etc/cron.hourly will call the script


use constant BASE => '/Volumes/pbdb_RAID/httpdocs';
use constant CGI_DIR => '/Volumes/pbdb_RAID/httpdocs/cgi-bin';
$IMGDIR = BASE.'/html/public/images';
$BANNERIMGDIR = BASE.'/html/public/bannerimages';

#if (scalar(@ARGV)) {
#    $BASE = $ARGV[0];
#}

use lib CGI_DIR;

use DBI;
use DBConnection;

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

$sql = "SELECT count(*) FROM authorities";
$sth = $dbh->prepare( $sql ) || die ( "$sql\n$!" );
$sth->execute();
@stats = $sth->fetchrow_array();
my $taxon_total = $stats[0];
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

$sql = "SELECT count(distinct institution) FROM person WHERE institution IS NOT NULL";
$sth = $dbh->prepare( $sql ) || die ( "$sql\n$!" );
$sth->execute();
@stats = $sth->fetchrow_array();
my $institution_total = $stats[0];
$sth->finish();

$sql = "SELECT count(distinct country) FROM person WHERE country IS NOT NULL";
$sth = $dbh->prepare( $sql ) || die ( "$sql\n$!" );
$sth->execute();
@stats = $sth->fetchrow_array();
my $country_total = $stats[0];
$sth->finish();

# Now put into our holding tank
$sql =	"UPDATE statistics SET ".
		"		reference_total = $reference_total, ".
		"		taxon_total = $taxon_total, ".
		"		collection_total = $collection_total, ".
		"		occurrence_total = $occurrence_total, ".
		"		enterer_total = $enterer_total, ".
		"		institution_total = $institution_total, ".
		"		country_total = $country_total ";
$dbh->do ( $sql );
if ( $DEBUG ) { print "$sql\n"; }

$dbh->disconnect();


#added by rjp on 12/9/2003

# get a list of all the files in this directory
@images = `ls $IMGDIR/*.jpg`;

if ($DEBUG) { print "images: @images \n"; }

# only do the jpegs for now, because who knows what will happen
# if you change the file extension to .gif on a jpeg.

$img_idx = int(rand($#images + 1));
if ($DEBUG) { print "index: $img_idx \n"; }

$filename = $images[$img_idx];
chomp($filename);

if ($DEBUG) { print "filename: $filename \n"; }

`cp -f $filename $IMGDIR/fossil.jpg`;

# rotate the banner image JA 5.11.05

@images = `ls $BANNERIMGDIR/*.jpg`;
$img_idx = int(rand($#images + 1));
$filename = $images[$img_idx];
chomp($filename);
`cp -f $filename $BANNERIMGDIR/fossil.jpg`;



