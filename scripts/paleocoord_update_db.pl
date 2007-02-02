#!/usr/bin/perl 

BEGIN {
$ENV{MAP_COAST_DIR} = "/home/peters/apache/cgi-bin/data";
$ENV{DOWNLOAD_OUTFILE_DIR} = "/Volumes/pbdb_RAID/httpdocs/html/paleodb/data";
$ENV{DOWNLOAD_DATAFILE_DIR} = "/Volumes/pbdb_RAID/httpdocs/cgi-bin/data";
$ENV{MAP_COAST_DIR} = "/Volumes/pbdb_RAID/httpdocs/cgi-bin/data";
$ENV{MAP_GIF_DIR} = "/Volumes/pbdb_RAID/httpdocs/html/public/maps";
$ENV{REPORT_DDIR} = "/Volumes/pbdb_RAID/httpdocs/cgi-bin/data";
$ENV{REPORT_DDIR2} = "/Volumes/pbdb_RAID/httpdocs/html/public/data";
}

use lib '../cgi-bin';
use TimeLookup;
use Map;
use DBConnection;
use DBTransactionManager;

# Flags and constants
my $DEBUG = 0;			# The debug level of the calling program
my $COAST_DIR = "../cgi-bin/data";
my $dbh = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh);

my $doUpdate = 0;
if ($ARGV[0] eq '--do_sql') {
    $doUpdate = 1;
    print "RUNNING SQL\n";
} else {
    print "DRY RUN\n";
}     

$sql = "SELECT latdeg,latdir,latmin,latsec,latdec,lngdeg,lngdir,lngmin,lngsec,lngdec,collection_no,max_interval_no,min_interval_no,paleolat,paleolng FROM collections";
my $sth = $dbh->prepare($sql);
$sth->execute();

my @results = @{$dbt->getData($sql)};

my $t = new TimeLookup($dbt);
@_ = $t->getBoundaries();
%upperbound = %{$_[0]};
%lowerbound = %{$_[1]};

my $map_o = new Map;
$map_o->readPlateIDs();
my %seen_age = ();

while ($row = $sth->fetchrow_hashref()) {
    my ($lng,$lat) = getDec($row);

    my $lb =  $lowerbound{$row->{'max_interval_no'}};
    my $ub =  $upperbound{$row->{'min_interval_no'}};
    if ( !$row->{'min_interval_no'} )    {
        $ub = $upperbound{$row->{'max_interval_no'}};
    }

    my $collage = int(($lb+$ub)/2 + .5);

    print "#$row->{collection_no} LAT:$lat LNG:$lng AGE:$collage\n";
    printf ("%-20s%-20s\n","OLD PLAT:$row->{paleolat}","OLD PLNG:$row->{paleolng}");
    if ($lat !~ /\d/|| $lng !~ /\d/) {
        print "ERROR: No coord\n"; 
        next;
    }

    if ($collage <= 600 && $collage >= 0) {
        $map_o->{maptime} = $collage;
        if (!$seen_age{$collage}) {
            $map_o->mapGetRotations();
            $seen_age{$collage} = 1;
        }
        # Get Map rotation information - needs maptime to be set (to collage)
        # rotx, roty, rotdeg get set by the function, needed by projectPoints below

        ($a,$b,$plng,$plat,$pid) = $map_o->projectPoints($lng,$lat,'',1);
        if ( $lngdeg !~ /NaN/ && $latdeg !~ /NaN/ )       {
            $plat = sprintf("%.2f",$plat);
            $plng = sprintf("%.2f",$plng);
            printf ("%-20s%-20s\n","NEW PLAT:$plat","NEW PLNG:$plng");

            $sql = "UPDATE collections SET paleolng=$plng, paleolat=$plat,modified=modified WHERE collection_no=$row->{collection_no}";
            print "$sql\n";
            print "\n";
            if ($doUpdate) {
                $dbh->do($sql);
            }
        } else {
            print "NO paleocoord for COL $row->{collection_no} LAT $lat LNG $lng\n";
        }
    } else {
        print "COLLAGE not valid for COL $row->{collection_no}: $collage\n";
    }
}

sub getDec {
    my $row = shift;
    my $y = $row->{'latdeg'};
    if ($row->{'latmin'} ne '') {
        $y = $row->{'latdeg'} + ($row->{'latmin'}/60) + ($row->{'latsec'}/3600);
    } else {
        $y = $row->{'latdeg'} .".".int($row->{'latdec'});
    }                                                                

    if ($row->{'latdir'} =~ /^S/) {
        $y *= -1;
    }

    my $x = $row->{'lngdeg'};
    if ($row->{'lngmin'} ne '') {
        $x = $row->{'lngdeg'} + ($row->{'lngmin'}/60) + ($row->{'lngsec'}/3600);
    } else {
        $x = $row->{'lngdeg'} .".". int($row->{'lngdec'});
    }
    if ($row->{'lngdir'} =~ /^W/) {
        $x *= -1;
    }
    return ($x,$y);
}

sub dbg {
	my $self = shift;
	my $message = shift;

	if ( $DEBUG && $message ) { print "<font color='green'>$message</font><BR>\n"; }

	return $DEBUG;					# Either way, return the current DEBUG value
}

