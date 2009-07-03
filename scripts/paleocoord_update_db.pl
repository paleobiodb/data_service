#!/usr/local/bin/perl 

BEGIN {
$ENV{DOWNLOAD_OUTFILE_DIR} = "/Users/alroy/pb/html/paleodb/data";
$ENV{DOWNLOAD_DATAFILE_DIR} = "/Users/alroy/pb/cgi-bin/data";
$ENV{MAP_COAST_DIR} = "/Users/alroy/pb/cgi-bin/data";
$ENV{MAP_GIF_DIR} = "/Users/alroy/pb/html/public/maps";
$ENV{REPORT_DDIR} = "/Users/alroy/pb/cgi-bin/data";
$ENV{REPORT_DDIR2} = "/Users/alroy/pb/html/public/data";
}

use lib "/Users/alroy/pb/cgi-bin";
use lib '/opt/local/lib/perl5/site_perl/5.8.9/darwin-2level';
use lib '/opt/local/lib/perl5/site_perl/5.8.9/darwin-2level/auto';
use lib '/opt/local/lib/perl5/vendor_perl/5.8.9';
use lib '/opt/local/lib/perl5/vendor_perl/5.8.9/darwin-2level';
use TimeLookup;
use Map;
use DBConnection;
use DBTransactionManager;

# Flags and constants
my $DEBUG = 0;			# The debug level of the calling program
my $COAST_DIR = "/Users/alroy/pb/cgi-bin/data";
my $dbt = DBTransactionManager->new();
my $dbh = $dbt->dbh;

my $doUpdate = 0;
foreach my $arg (@ARGV) {
    if ($arg eq '--do_sql') {
        $doUpdate = 1;
    } elsif ($arg =~ /--debug=(\d)/) {
        $DEBUG = $1;
    } elsif ($arg =~ /--debug/) {
        $DEBUG = 1;
    }
}

if ($doUpdate) {
    print "RUNNING SQL\n" if ($DEBUG);
} else {
    print "DRY RUN\n" if ($DEBUG);
}     

$sql = "SELECT latdeg,latdir,latmin,latsec,latdec,lngdeg,lngdir,lngmin,lngsec,lngdec,collection_no,max_interval_no,min_interval_no,paleolat,paleolng,plate FROM collections";
#my $sth = $dbh->prepare($sql);
#$sth->execute();

my @results = @{$dbt->getData($sql)};

my $t = new TimeLookup($dbt);
@_ = $t->getBoundaries();
%upperbound = %{$_[0]};
%lowerbound = %{$_[1]};

my $map_o = new Map;
$map_o->readPlateIDs();
my %seen_age = ();

foreach my $row (@results) {
    my ($lng,$lat) = getDec($row);

    my $lb =  $lowerbound{$row->{'max_interval_no'}};
    my $ub =  $upperbound{$row->{'min_interval_no'}};
    if ( !$row->{'min_interval_no'} )    {
        $ub = $upperbound{$row->{'max_interval_no'}};
    }

    my $collage = int(($lb+$ub)/2 + .5);

    print "#$row->{collection_no} LAT:$lat LNG:$lng AGE:$collage\n" if ($DEBUG > 1);
    my $old_plat = sprintf("%.2f",$row->{'paleolat'});
    my $old_plng = sprintf("%.2f",$row->{'paleolng'});
    my $old_plate = $row->{'plate'};
    printf ("%-20s%-20s\n","OLD PLAT:$row->{paleolat}","OLD PLNG:$row->{paleolng}") if ($DEBUG > 1);
    if ($lat !~ /\d/|| $lng !~ /\d/) {
        print "ERROR: No coord\n" if ($DEBUG > 1); 
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
            printf ("%-20s%-20s\n","NEW PLAT:$plat","NEW PLNG:$plng") if ($DEBUG > 1);
            
            if ($old_plng ne $plng || $old_plat ne $plat || $old_plate ne $pid) {
                $sql = "UPDATE collections SET paleolng=$plng, paleolat=$plat, plate=$pid, modified=modified WHERE collection_no=$row->{collection_no}";
                print "$sql\n" if ($DEBUG);
                print "\n" if ($DEBUG > 1);
                if ($doUpdate) {
                    $dbh->do($sql);
                }
            }
        } else {
            print "NO paleocoord for COL $row->{collection_no} LAT $lat LNG $lng\n" if ($DEBUG > 1);
        }
    } else {
        print "COLLAGE not valid for COL $row->{collection_no}: $collage\n" if ($DEBUG > 1);
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

