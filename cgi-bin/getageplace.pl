# computes upper and lower age estimate and paleocoordinates for every
#  collection in the database
# written 5-6.3.04; based on a copy of Map.pm
# also prints formation name and original coordinates 12.3.04
# allows choice of research group 19.3.04
# fixed a bug in which west and south original longs and lats weren't
#  printed with negative signs; fixed a bug (GROUP couldn't be empty) 30.6.04

use Class::Date qw(date localdate gmdate now);
use TimeLookup;
use Globals;
use DBI;
use DBTransactionManager;
use Session;

# Flags and constants
my $DEBUG = 0;			# The debug level of the calling program
my $sql;				# Any SQL string

my $COAST_DIR = "data";        # For maps
my $PI = 3.14159265;
my $C72 = cos(72 * $PI / 180);

my $GROUP = ""; # marine, vertebrate, paleobotany, micropaleo, fivepct

my $driver = "mysql";
my $db = "pbdb_paul";
my $host = "localhost";
my $user = 'pbdbuser';

open PASSWD,"</Users/paleodbpasswd/passwd";
$password = <PASSWD>;
$password =~ s/\n//;
close PASSWD;

my $dbh = DBI->connect("DBI:$driver:database=$db;host=$host",
                      $user, $password, {RaiseError => 1});

my $s = Session->new();
my $dbt = DBTransactionManager->new($dbh, $s);

sub acos { atan2( sqrt(1 - $_[0] * $_[0]), $_[0] ) }
sub tan { sin($_[0]) / cos($_[0]) }

# returns great circle distance given two latitudes and a longitudinal offset
sub GCD { ( 180 / $PI ) * acos( ( sin($_[0]*$PI/180) * sin($_[1]*$PI/180) ) + ( cos($_[0]*$PI/180) * cos($_[1]*$PI/180) * cos($_[2]*$PI/180) ) ) }


$|=1;					# Freeflowing data

&queryDb();
&calibrateTimeScale();
&getRotations();
&getAllCoords();


sub queryDb	{

	# Start the SQL
	if ( $GROUP =~ /marine/i )	{
		$sql = qq|SELECT *, DATE_FORMAT(release_date, '%Y%m%d') rd_short FROM collections WHERE research_group IN ('marine invertebrate')|;
	} elsif ( $GROUP =~ /vertebrate/i )	{
		$sql = qq|SELECT *, DATE_FORMAT(release_date, '%Y%m%d') rd_short FROM collections WHERE research_group IN ('vertebrate')|;
	} elsif ( $GROUP =~ /paleobotany/i )	{
		$sql = qq|SELECT *, DATE_FORMAT(release_date, '%Y%m%d') rd_short FROM collections WHERE research_group IN ('paleobotany')|;
	} elsif ( $GROUP =~ /micropaleontology/i )	{
		$sql = qq|SELECT *, DATE_FORMAT(release_date, '%Y%m%d') rd_short FROM collections WHERE research_group IN ('micropaleontology')|;
	} elsif ( $GROUP =~ /fivepct/i )	{
		$sql = qq|SELECT *, DATE_FORMAT(release_date, '%Y%m%d') rd_short FROM collections,refs WHERE research_group IN ('vertebrate') AND project_name IN ('5%') AND collections.reference_no=refs.reference_no|;
	} else	{
		$sql = qq|SELECT *, DATE_FORMAT(release_date, '%Y%m%d') rd_short FROM collections|;
	}

	$sql =~ s/\s+/ /gs;
	$sth = $dbh->prepare($sql);
	$sth->execute();

}

sub calibrateTimeScale	{

	@_ = TimeLookup::findBoundaries($dbh,$dbt);
	%upperbound = %{$_[0]};
	%lowerbound = %{$_[1]};

}


# read Scotese's plate ID and rotation data files
sub getRotations	{

	open IDS,"<$COAST_DIR/plateidsv2.lst";

	# skip the first line
	<IDS>;

	# read the plate IDs: numbers are longitude, latitude, and ID number
	while (<IDS>)	{
		s/\n//;
		my ($x,$y,$z) = split /,/,$_;
		$plate{$x}{$y} = $z;
	}

	open ROT, "<$COAST_DIR/master01c.rot";

	# read the rotations
	# numbers are millions of years ago; plate ID; latitude and longitude
	#  of pole of rotation; and degrees rotated
	while (<ROT>)	{
		s/\n//;
		my @temp = split /,/,$_;
		$rotx{$temp[0]}{$temp[1]} = $temp[3];
		$roty{$temp[0]}{$temp[1]} = $temp[2];
		$rotdeg{$temp[0]}{$temp[1]} = $temp[4];
	}
	close ROT;
	# rotations for the Recent are all zero; poles are same as 10 Ma
	for $p (1..999)	{
		$rotx{0}{$p} = $rotx{10}{$p};
		$roty{0}{$p} = $roty{10}{$p};
		$rotdeg{0}{$p} = 0;
	}

# use world's dumbest linear interpolation to estimate pole of rotation and
#  angle of rotation values if this time interval is non-standard
	for my $collage (1..599)	{
	if ( ! $roty{$collage}{'1'} )	{

		my $basema = $collage;
		while ( ! $roty{$basema}{'1'} && $basema >= 0 )	{
			$basema--;
		}
		my $topma = $collage;
		while ( ! $roty{$topma}{'1'} && $topma < 1000 )	{
			$topma++;
		}

		if ( $topma < 1000 )	{
			$basewgt = ( $topma - $collage ) / ( $topma - $basema );
			$topwgt = ( $collage - $basema ) / ( $topma - $basema );
			for $pid (1..1000)	{
				my $x1 = $rotx{$basema}{$pid};
				my $x2 = $rotx{$topma}{$pid};
				my $y1 = $roty{$basema}{$pid};
				my $y2 = $roty{$topma}{$pid};
				my $z1 = $rotdeg{$basema}{$pid};
				my $z2 = $rotdeg{$topma}{$pid};

				$rotx{$collage}{$pid} = ( $basewgt * $x1 ) + ( $topwgt * $x2 ) ;
				# the amazing "Madagascar 230 Ma" correction
				if ( ( $x1 > 0 && $x2 < 0 ) || ( $x1 < 0 && $x2 > 0 ) )	{
					if ( abs($x1 - $x2) > 180 )	{ # Madagascar case
						$rotx{$collage}{$pid} = ( ( 180 - $x1 ) + ( 180 - $x2 ) ) / 2;
					} elsif ( abs($x1 - $x2) > 90 )	{ # Africa plate 701/150 Ma case
						$y2 = -1 * $y2;
						$z2 = -1 * $z2;
						if ( abs($x1) > abs($x2) )	{
							$rotx{$collage}{$pid} = ( 180 + $x1 + $x2 ) / 2;
						} else	{
							$rotx{$collage}{$pid} = ( 180 - $x1 - $x2 ) / 2;
						}
					}
				}

				$roty{$collage}{$pid} = ( $basewgt * $y1 ) + ( $topwgt * $y2 );
				$rotdeg{$collage}{$pid} = ( $basewgt * $z1 ) + ( $topwgt * $z2 );
			}
		}
	}
	}

}

# compute the paleocoordinates
sub getAllCoords	{

  # draw collection data points
	open OUT,">data/collection.ageplace";
	while ( my $collRef = $sth->fetchrow_hashref() )	{
 		%coll = %{$collRef};
 		if ( ( $coll{'latdeg'} > 0 || $coll{'latmin'} > 0 || $coll{'latdec'} > 0 ) && ( $coll{'lngdeg'} > 0 || $coll{'lngmin'} > 0 || $coll{'lngdec'} > 0 ) )	{

			my $colllowerbound =  $lowerbound{$coll{'max_interval_no'}};
			my $collupperbound =  $upperbound{$coll{'min_interval_no'}};
			if ( $min_interval_no eq "" )	{
				$collupperbound = $upperbound{$coll{'max_interval_no'}};
			}
			my $collage = ( $colllowerbound + $collupperbound ) / 2;

			$lngoff = $coll{'lngdeg'};
    			$latoff = $coll{'latdeg'};
			$flngoff = $coll{'lngdeg'} + ($coll{'lngmin'}/60) + ($coll{'lngsec'}/3600) + $coll{'lngdec'};
			$flatoff = $coll{'latdeg'} + ($coll{'latmin'}/60) + ($coll{'latsec'}/3600) + $coll{'latdec'};
     			($x1,$y1) = &getCoords($lngoff,$latoff,$flngoff,$flatoff,int($collage + 0.5));
			if ($coll{'lngdir'} =~ /West/)	{
				$flngoff = $flngoff * -1;
			}
			if ($coll{'latdir'} =~ /West/)	{
				$flatoff = $flatoff * -1;
			}

			print OUT "$coll{'collection_no'}\t";
			print OUT "$coll{'max_interval_no'}\t$coll{'min_interval_no'}\t";
			printf OUT "%.1f\t",$colllowerbound;
			printf OUT "%.1f\t",$collupperbound;
			printf OUT "%.1f\t%.1f\t",$flngoff,$flatoff;
			printf OUT "%.1f\t%.1f\t",$x1,$y1;
			print OUT "$coll{'formation'}\n";

		}
	}


}

sub getCoords	{

	my ($x,$y,$fx,$fy,$collage) = @_;
	if ($coll{'lngdir'} =~ /West/)	{
		$x = $x * -1;
		$fx = $fx * -1;
	}
	if ($coll{'latdir'} =~ /South/)	{
		$y = $y * -1;
		$fy = $fy * -1;
	}
	($x,$y) = &projectPoints($x,$y,$fx,$fy,$collage);
	if ( $x ne "NaN" && $y ne "NaN" )	{
		return($x,$y);
	} else	{
		return;
	}
}

sub projectPoints	{

	my ($x,$y,$fx,$fy,$collage) = @_;
	my $pid;
	my $rotation;
	my $oldx;
	my $oldy;

	# rotate point if a paleogeographic map is being made
	# strategy: rotate point such that the pole of rotation is the
	#  north pole; use the GCD between them to get the latitude;
	#  add the degree offset to its longitude to get the new value;
	#  re-rotate point back into the original coordinate system
	if ( $collage > 0 && $projected{$fx}{$fy} eq "" )	{

		my $ma = $collage;
		$oldx = $fx;
		$oldy = $fy;

	# integer coordinates are needed to determine the plate ID
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

	# what plate is this point on?
		$pid = $plate{$q}{$r};

	# if there are no data, just bomb out
		if ( $pid eq "" || $rotx{$ma}{$pid} eq "" || $roty{$ma}{$pid} eq "" )	{
			return('NaN','NaN');
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
	# NOTE: the floating point (exact) coordinates are used instead of the
	#  cell-midpoint coordinates ($x and $y)
		($x,$y) = rotatePoint($fx,$fy,$neworigx,$neworigy);
		if ( $x eq "NaN" || $y eq "NaN" )	{
			return('NaN','NaN');
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
		$projected{$oldx}{$oldy} = $x . ":" . $y . ":" . $pid;
		if ( $x eq "NaN" || $y eq "NaN" )	{
			return('NaN','NaN');
		}

	}
	if ( $oldx eq "" && $oldy eq "" && $projected{$fx}{$fy} ne "" )	{
		($x,$y,$pid) = split /:/,$projected{$fx}{$fy};
	}

	return($x,$y);
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
	if ( $y == 90 )	{
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
		return('NaN','NaN');
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


1;
