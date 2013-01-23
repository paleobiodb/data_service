#!/opt/local/bin/perl
# original version by JA 17.3.02
# completely rewritten by Garot June 2002
# updates list and occurrence counts on public main page
# suggested by T. Olszewski
# also prints count of different researchers JA 9.5.02
# prints same data to paleodb/index page JA 10.5.02
# image randomization routine modified by rjp, 12/03.
# added institution and country JA 15.4.04

# NOTE: assumes /etc/cron.hourly will call the script
# now called by /Library/LaunchDaemons/com.kosnik.updatePublic.plist JA 23.5.09

use constant BASE => '/Library/WebServer/paleodb';
use constant CGI_DIR => '/Library/WebServer/paleodb/cgi-bin';
$IMGDIR = BASE.'/Library/WebServer/paleodb/html/public/images';
$BANNERIMGDIR = BASE.'/Library/WebServer/paleodb/html/public/bannerimages';

#if (scalar(@ARGV)) {
#    $BASE = $ARGV[0];
#}

use lib CGI_DIR;

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

$sql = "SELECT count(distinct institution) FROM person,refs WHERE institution IS NOT NULL AND person_no=enterer_no";
$sth = $dbh->prepare( $sql ) || die ( "$sql\n$!" );
$sth->execute();
@stats = $sth->fetchrow_array();
my $institution_total = $stats[0];
$sth->finish();

$sql = "SELECT count(distinct country) FROM person,refs WHERE country IS NOT NULL AND person_no=enterer_no";
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


# count the number of hours spent entering data over the last month JA 1.1.10
# won't work on MySQL 4 because it doesn't support subselects

# have to clear the values first
$sql = "UPDATE person SET hours=NULL,last_action=last_action WHERE hours IS NOT NULL";
$dbh->do ( $sql );

$sql = "SELECT enterer_no,count(distinct(concat(to_days(created),hour(created)))) c FROM ((SELECT enterer_no,created FROM refs WHERE created>now()-interval 1 month) UNION (SELECT enterer_no,created FROM collections WHERE created>now()-interval 1 month) UNION (SELECT enterer_no,created FROM opinions WHERE created>now()-interval 1 month) UNION (SELECT enterer_no,created FROM specimens WHERE created>now()-interval 1 month)) AS t GROUP BY enterer_no";
$sth = $dbh->prepare( $sql ) || die ( "$sql\n$!" );
$sth->execute();
while ( $s = $sth->fetchrow_hashref() )	{
	$sql = "UPDATE person SET hours=".$s->{'c'}.",last_action=last_action WHERE person_no=".$s->{'enterer_no'};
	$dbh->do ( $sql );
}
$sth->finish();

# hey, why not compute hours ever worked JA 17.3.11

$sql = "SELECT enterer_no,count(distinct(concat(to_days(created),hour(created)))) c FROM ((SELECT enterer_no,created FROM refs) UNION (SELECT enterer_no,created FROM collections) UNION (SELECT enterer_no,created FROM opinions) UNION (SELECT enterer_no,created FROM specimens)) AS t GROUP BY enterer_no";
$sth = $dbh->prepare( $sql ) || die ( "$sql\n$!" );
$sth->execute();
while ( $s = $sth->fetchrow_hashref() )	{
	$sql = "UPDATE person SET hours_ever=".$s->{'c'}.",last_action=last_action WHERE person_no=".$s->{'enterer_no'};
	$dbh->do ( $sql );
}
$sth->finish();

# we need to know hours authorized to identify major contributors

# the total needs to be cleared to make sure upload-only authorizers aren't
#  given compounded credit
$sql = "UPDATE person SET hours_authorized=NULL,last_action=last_action WHERE hours_authorized IS NOT NULL";
$dbh->do ( $sql );

$sql = "SELECT authorizer_no,count(distinct(concat(to_days(created),hour(created)))) c FROM ((SELECT authorizer_no,created FROM refs) UNION (SELECT authorizer_no,created FROM collections WHERE upload='') UNION (SELECT authorizer_no,created FROM opinions) UNION (SELECT authorizer_no,created FROM specimens)) AS t GROUP BY authorizer_no";
$sth = $dbh->prepare( $sql ) || die ( "$sql\n$!" );
$sth->execute();
while ( $s = $sth->fetchrow_hashref() )	{
	$sql = "UPDATE person SET hours_authorized=".$s->{'c'}.",last_action=last_action WHERE person_no=".$s->{'authorizer_no'};
	$dbh->do ( $sql );
}

# credit for uploaded collections (uploaded taxonomy is a minor issue)
# the ratio of 3.4 comes from here:
# select count(*)/count(distinct(concat(authorizer_no,' ',to_days(created),hour(created)))) from collections where upload='';
# the effect on the ranking turns out to be minimal

$sql = "SELECT authorizer_no,count(*)/3.4 c FROM collections WHERE upload='YES' GROUP BY authorizer_no";
$sth = $dbh->prepare( $sql ) || die ( "$sql\n$!" );
$sth->execute();
while ( $s = $sth->fetchrow_hashref() )	{
	$sql = "UPDATE person SET hours_authorized=0,last_action=last_action WHERE hours_authorized IS NULL AND person_no=".$s->{'authorizer_no'};
	$dbh->do ( $sql );
	$sql = "UPDATE person SET hours_authorized=hours_authorized+".$s->{'c'}.",last_action=last_action WHERE person_no=".$s->{'authorizer_no'};
	$dbh->do ( $sql );
}
$sth->finish();


$dbh->disconnect();


# JA: following is legacy, might be useful as a guide if we ever want to
#  do something like this again

#added by rjp on 12/9/2003

# get a list of all the files in this directory
#@images = `ls $IMGDIR/*.jpg | grep -v fossil.jpg`;

#if ($DEBUG) { print "images: @images \n"; }

# only do the jpegs for now, because who knows what will happen
# if you change the file extension to .gif on a jpeg.

#$img_idx = int(rand($#images + 1));
#if ($DEBUG) { print "index: $img_idx \n"; }

#$filename = $images[$img_idx];
#chomp($filename);

#if ($DEBUG) { print "filename: $filename \n"; }

#`cp -f $filename $IMGDIR/fossil.jpg`;

# rotate the banner image JA 5.11.05
#@images = `ls $BANNERIMGDIR/*.jpg | grep -v fossil.jpg`;
#$img_idx = int(rand($#images + 1));
#$filename = $images[$img_idx];
#chomp($filename);
#`cp -f $filename $BANNERIMGDIR/fossil.jpg`;



