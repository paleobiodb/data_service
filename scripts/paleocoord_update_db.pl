#!/usr/bin/perl 

use lib '../cgi-bin';
use TimeLookup;
use DBConnection;
use DBTransactionManager;

# Flags and constants
my $DEBUG = 0;			# The debug level of the calling program
my $COAST_DIR = "../cgi-bin/data";
my $dbh = DBConnection::connect();
my $dbt = DBTransactionManager->new($dbh);

my $PI = 3.14159265;
my $C72 = cos(72 * $PI / 180);


my $doUpdate = 0;
if ($ARGV[0] eq '--do_sql') {
    $doUpdate = 1;
    print "RUNNING SQL\n";
} else {
    print "DRY RUN\n";
}     

my %rotdeg;
my %rotx;
my %roty;
my %lastrotx;
my %lastroty;

getPlates();
getAllRotations();

$sql = "SELECT latdeg,latdir,latmin,latsec,latdec,lngdeg,lngdir,lngmin,lngsec,lngdec,collection_no,max_interval_no,min_interval_no,paleolat,paleolng FROM collections";
my $sth = $dbh->prepare($sql);
$sth->execute();

my @results = @{$dbt->getData($sql)};

@_ = TimeLookup::findBoundaries($dbh,$dbt);
%upperbound = %{$_[0]};
%lowerbound = %{$_[1]};


while ($row = $sth->fetchrow_hashref()) {
    my ($x,$y) = getDec($row);


    my $lb =  $lowerbound{$row->{'max_interval_no'}};
    my $ub =  $upperbound{$row->{'min_interval_no'}};

    if ( !$row->{'min_interval_no'} )    {
        $ub = $upperbound{$row->{'max_interval_no'}};
    }

    my $collage = int(($lb+$ub)/2 + .5);

    print "#$row->{collection_no} LAT:$y LNG:$x AGE:$collage\n";
    printf ("%-20s%-20s\n","OLD PLAT:$row->{paleolat}","OLD PLNG:$row->{paleolng}");
    if (!$x || !$y) {
        print "ERROR: No coord\n"; 
        next;
    }
    
    ($px,$py) = projectPoints($x,$y,$collage);
    if ($px =~ /nan/i) {
        print "ERROR: No paleocoord\n";
        next;
    }
    $pxt = sprintf("%.2f",$px);
    $pyt = sprintf("%.2f",$py);
    printf ("%-20s%-20s\n","NEW PLAT:$pyt","NEW PLNG:$pxt");
    $sql = "UPDATE collections SET paleolng=$pxt, paleolat=$pyt,modified=modified WHERE collection_no=$row->{collection_no}";
    print "$sql\n";
    print "\n";
    if ($doUpdate) {
        $dbh->do($sql);
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

# read Scotese's plate ID and rotation data files
sub getPlates {
    if ( ! open IDS,"<$COAST_DIR/plateidsv2.lst" ) {
        die("Couldn't open [$COAST_DIR/plateidsv2.lst]: $!");
    }

    # skip the first line
    <IDS>;

    # read the plate IDs: numbers are longitude, latitude, and ID number
    while (<IDS>)	{
        s/\n//;
        my ($x,$y,$z) = split /,/,$_;
        $plate{$x}{$y} = $z;
    }
    close IDS;
}

sub getAllRotations {
	if ( ! open ROT,"<$COAST_DIR/master01c.rot" ) {
		die("Couldn't open [$COAST_DIR/master01c.rot]: $!");
	}

	# read the rotations
	# numbers are millions of years ago; plate ID; latitude and longitude
	#  of pole of rotation; and degrees rotated
	while (<ROT>)	{
		s/\n//;
		my @temp = split /,/,$_;
	# Philippines test: pole of rotation doesn't change, so actually
	#  the plate comes into existence after the Paleozoic
		if ( $lastrotx{$temp[1]} != $temp[3] || $lastroty{$temp[1]} != $temp[2] || $lastrotdeg{$temp[1]} != $temp[4] || $temp[1] == 1 )	{
			$rotx{$temp[0]}{$temp[1]} = $temp[3];
			$roty{$temp[0]}{$temp[1]} = $temp[2];
			$rotdeg{$temp[0]}{$temp[1]} = $temp[4];
		}
		if ( $temp[3] =~ /[0-9]/ )	{
			$lastrotx{$temp[1]} = $temp[3];
			$lastroty{$temp[1]} = $temp[2];
			$lastrotdeg{$temp[1]} = $temp[4];
		}
	}
	close ROT;
	# rotations for the Recent are all zero; poles are same as 10 Ma
	my @pids = sort { $a <=> $b } keys %{$rotx{'10'}};
    for $p ( @pids )	{
        $rotx{0}{$p} = $rotx{10}{$p};
        $roty{0}{$p} = $roty{10}{$p};
        $rotdeg{0}{$p} = 0;
    }

    # use world's dumbest linear interpolation to estimate pole of rotation and
    #  angle of rotation values if this time interval is non-standard
    foreach $ma (1 .. 599) {
        if ( ! $roty{$ma}{'1'} )	{

            my $basema = $ma;
            while ( ! $roty{$basema}{'1'} && $basema >= 0 )	{
                $basema--;
            }
            my $topma = $ma;
            while ( ! $roty{$topma}{'1'} && $topma < 1000 )	{
                $topma++;
            }

            if ( $topma < 1000 )	{
                $basewgt = ( $topma - $ma ) / ( $topma - $basema );
                $topwgt = ( $ma - $basema ) / ( $topma - $basema );
                my @pids = sort { $a <=> $b } keys %{$rotx{$topma}};
                for $pid ( @pids )	{
                    my $x1 = $rotx{$basema}{$pid};
                    my $x2 = $rotx{$topma}{$pid};
                    my $y1 = $roty{$basema}{$pid};
                    my $y2 = $roty{$topma}{$pid};
                    my $z1 = $rotdeg{$basema}{$pid};
                    my $z2 = $rotdeg{$topma}{$pid};

                    # Africa/plate 701 150 Ma bug: suddenly the pole of
                    #  rotation is projected to the opposite side of the
                    #  planet, so the degrees of rotation have a flipped
                    #  sign
                    # sometimes the lat/long signs flip but the degrees
                    #  of rotation don't (e.g., plate 619 410 Ma case),
                    #  and therefore nothing should be done to the latter;
                    #  test is whether the degrees have opposite signs
                    # sometimes the pole just goes around the left or right
                    #  edge of the map (e.g., Madagascar/plate 702 230 Ma),
                    #  so nothing should be done; the longitudes will be
                    #  off by > 270 degrees in that case

                    if ( abs($x1 - $x2) > 90 && abs($x1 - $x2) < 270 && ( ( $x1 > 0 && $x2 < 0 ) || ( $x1 < 0 && $x2 > 0 ) ) ) 	{
                        if ( ( $y1 > 0 && $y2 < 0 ) || ( $y1 < 0 && $y2 > 0 ) )	{
                            if ( $x2 > 0 )	{
                                $x2 = $x2 - 180;
                            } else	{
                                $x2 = $x2 + 180;
                            }
                            $y2 = -1 * $y2;
                            if ( ( $z1 > 0 && $z2 < 0 ) || ( $z1 < 0 && $z2 > 0 ) )	{
                                $z2 = -1 * $z2;
                            }
                        }
                    }

                    # sometimes the degrees of rotation suddenly flip
                    #  even though the pole doesn't  (e.g., plate 616
                    #  410 Ma case)
                    if ( abs($z1 - $z2) > 90 && ( $z1 > 0 && $z2 < 0 || $z1 < 0 && $z2 > 0 ) )	{
                        if ( abs($z1 - $z2) < 270 )	{
                            $z2 = -1 * $z2;
                        }
                        # sometimes the degrees have just gone over 180 or
                        #  under -180 (e.g., plate 611 375 Ma case)
                        else	{
                            if ( $z1 > 0 )	{
                                $z1 = $z1 - 360;
                            } else	{
                                $z1 = $z1 + 360;
                            }
                        }
                    }

                
                    # averaging works better and better as you get close
                    #  to the origin, and works horribly near the edges of
                    #  the map, so treat the first pole as the origin,
                    #  rotate the second accordingly, interpolate, and
                    #  unrotate the interpolated pole
                    # key test cases involve Antarctica (45, 85, 290 Ma)

                    ($x2,$y2) = rotatePoint($x2,$y2,$x1,$y1);
                    my $interpolatedx = $topwgt * $x2;
                    my $interpolatedy = $topwgt * $y2;

                    ($rotx{$ma}{$pid},$roty{$ma}{$pid})  = rotatePoint($interpolatedx,$interpolatedy,$x1,$y1,"reversed");

                    $rotdeg{$ma}{$pid} = ( $basewgt * $z1 ) + ( $topwgt * $z2 );

                    # it's mathematically possible that the degrees have
                    #  averaged out to over 180 or under -180 in something
                    #  like the plate 611 375 Ma case
                    if ( $rotdeg{$ma}{$pid} > 180 )	{
                        $rotdeg{$ma}{$pid} = $rotdeg{$ma}{$pid} - 360;
                    } elsif ( $rotdeg{$ma}{$pid} < - 180 )	{
                        $rotdeg{$ma}{$pid} = $rotdeg{$ma}{$pid} + 360;
                    }
                }
            }
        }
    }
}


sub projectPoints	{
	my ($x,$y,$ma) = @_;
    
	my $pid;
	my $rotation;
	my $oldx;
	my $oldy;

	# rotate point if a paleogeographic map is being made
	# strategy: rotate point such that the pole of rotation is the
	#  north pole; use the GCD between them to get the latitude;
	#  add the degree offset to its longitude to get the new value;
	#  re-rotate point back into the original coordinate system
	if ($ma > 0) {
		$oldx = $x;
		$oldy = $y;

        # integer coordinates are needed to determine the plate ID
        # IMPORTANT: Scotese's plate ID data are weird in that the coordinates
        #   refer to the lower left (southwest) corner of each grid cell, so,
        #  say, cell -10 / 10 is from -10 to -9 long and 10 to 11 lat
		my $q; 
		my $r;
		if ( $x >= 0 )	{
			$q = int($x);
		} else	{
			$q = int($x-1);
		}
		if ( $y >= 0 )	{
			$r = int($y);
		} else	{
			$r = int($y-1);
		}

        $pid = $plate{$q}{$r};
            
    	# if there are no data, just bomb out
		if ( $pid eq "" || $rotx{$ma}{$pid} eq "" || $roty{$ma}{$pid} eq "" )	{
			return('NaN1','NaN1');
		}

    	# how far are we going?
		$rotation = $rotdeg{$ma}{$pid};

    	# if the pole of rotation is in the southern hemisphere,
    	#  rotate negatively (clockwise) - WARNING: I have no idea why
    	#  this works, but it does
		if ( $roty{$ma}{$pid} <= 0 )	{
			$rotation = -1 * $rotation;
		}

    	# locate the old origin in the "new" system defined by the POR
    	# the POR is the north pole, so the origin is 90 deg south of it
    	# for a southern hemisphere POR, flip the longitude
		my $neworigx;
		my $neworigy;
		if ( $roty{$ma}{$pid} > 0 )	{
			$neworigx = $rotx{$ma}{$pid};
		} elsif ( $rotx{$ma}{$pid} > 0 )	{
			$neworigx = $rotx{$ma}{$pid} - 180;
		} else	{
			$neworigx = $rotx{$ma}{$pid} + 180;
		}
		$neworigy = abs($roty{$ma}{$pid}) - 90;

    	# rotate the point into the new coordinate system
		($x,$y) = rotatePoint($x,$y,$neworigx,$neworigy);
		if ( $x =~ /NaN/ || $y =~ /NaN/ )	{
			return('NaN2','NaN2');
		}

    	# adjust the longitude
		$x = $x + $rotation;

		if ( $x <= -180 )	{
			$x = $x + 360;
		} elsif ( $x >= 180 )	{
			$x = $x - 360;
		}

    	# put the point back in the old projection
		($x,$y) = rotatePoint($x,$y,$neworigx,$neworigy,"reversed");
		if ( $x =~ /NaN/ || $y =~ /NaN/ )	{
			return('NaN3','NaN3');
		}
	}

	$rawx = $x;
	$rawy = $y;

	#if ( $projection eq "equirectangular" && $x ne "" )	
	#if ( $x ne "" )	{
#		$x = $x * sin( 60 * $PI / 180);
#		$y = $y * sin( 60 * $PI / 180);
#	} 

	return($x,$y,$rawx,$rawy,$pid);
}

sub rotatePoint	{

	my ($x,$y,$origx,$origy,$direction) = @_;

	# flip the pole of rotation if you're going backwards
	if ( $direction eq "reversed" )	{
		$origx = -1 * $origx;
		$origy = -1 * $origy;
	}
	# recenter the longitude on the new origin
	else	{
		$x = $x - $origx;
		if ( $x <= -180 )	{
			$x = $x + 360;
		} elsif ( $x >= 180 )	{
			$x = $x - 360;
		}
	}

	# find the great circle distance to the new origin
	my $gcd = GCD($y,$origy,$x);

	# find the great circle distance to the point opposite the new origin
	my $oppgcd;
	if ( $x > 0 )	{
		$oppgcd = GCD($y,-1*$origy,180-$x);
	} else	{
		$oppgcd = GCD($y,-1*$origy,180+$x);
	}

	# find the great circle distance to the POR (the new north pole)
	my $porgcd;
	if ( $origy <= 0 )	{ # pole is at same longitude as origin
		$porgcd = GCD($y,90+$origy,$x);
	} elsif ( $x > 0 )	{ # pole is at 180 deg, point is east
		$porgcd = GCD($y,90-$origy,180-$x);
	} else	{ # pole is at 180 deg, point is west
		$porgcd = GCD($y,90-$origy,180+$x);
	}

	# now finally shift the point's coordinate relative to the new origin

	# find new latitude exploiting fact that great circle distance from
	#  point to the new north pole must be 90 - latitude

	$y = 90 - $porgcd;
	if ( $y > 89.9 )	{
		$y = 89.9;
	}
	if ( $x >= 179.999 )	{
		$x = 179.9;
	} elsif ( $x <= -179.999 )	{
		$x = -179.9;
	} elsif ( abs($x) < 0.005 )	{
		$x = 0.1;
	} 

	# find new longitude exploiting fact that distance from point to
	#  origin G scales to latitude Y and longitude X, so X = acos(cosGcosY)
	if ( abs($x) > 0.005 && abs($x) < 179.999 && abs($y) < 90 )	{
		if ( $gcd > 90 )	{
			if ( abs( abs($y) - abs($oppgcd) ) < 0.001 )	{
				$oppgcd = $oppgcd + 0.001;
			}
			if ( $x > 0 )	{
				$x = 180 - ( 180 / $PI * acos( cos($oppgcd * $PI / 180) / cos($y * $PI / 180) ) );
			} else	{
				$x = -180 + ( 180 / $PI * acos( cos($oppgcd * $PI / 180) / cos($y * $PI / 180) ) );
			}
		} else	{
			if ( abs( abs($y) - abs($gcd) ) < 0.001 )	{
				$gcd = $gcd + 0.001;
			}
			if ( $x > 0 )	{
				$x = 180 / $PI * acos( cos($gcd * $PI / 180) / cos($y * $PI / 180) );
			} else	{
				$x = -1 * 180 / $PI * acos( cos($gcd * $PI / 180) / cos($y * $PI / 180) );
			}

		}
	} else	{
	# toss out points with extreme values that blow up the arcos
	#  function due to rounding error (should never happen given
	#  corrections made right before calculation above)
		return('NaN0','NaN0');
	}

	# recenter the longitude on the old origin at the end
	#   of a paleolat calculation
	if ( $direction eq "reversed" )	{
		$x = $x - $origx;
		if ( $x <= -180 )	{
			$x = $x + 360;
		} elsif ( $x >= 180 )	{
			$x = $x - 360;
		}
	}
	return ($x,$y);

}

sub dbg {
	my $self = shift;
	my $message = shift;

	if ( $DEBUG && $message ) { print "<font color='green'>$message</font><BR>\n"; }

	return $DEBUG;					# Either way, return the current DEBUG value
}

# mapsearchfieldsX value => query parameter mapping.  really should have value
sub acos {
    my $a;
    if ($_[0] > 1 || $_[0] < -1) {
        $a = 1;
#        carp "Map.pm warning, bad args passed to acos: $_[0] x $x y $y";
    } else {
        $a = $_[0];
    }
    atan2( sqrt(1 - $a * $a), $a )
}
sub asin { atan2($_[0], sqrt(1 - $_[0] * $_[0])) }
sub tan { sin($_[0]) / cos($_[0]) }
sub GCD { ( 180 / $PI ) * acos( ( sin($_[0]*$PI/180) * sin($_[1]*$PI/180) ) + ( cos($_[0]*$PI/180) * cos($_[1]*$PI/180) * cos($_[2]*$PI/180) ) ) }

