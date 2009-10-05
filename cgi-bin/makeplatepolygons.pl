# made 30.4.06 from a copy of tiltmask.pl
# fixed Andes and Philippines bugs 5.5.06
# fixed Antarctica bug 9.5.06
# added smoothing of internal corners to match smoothing of external corners;
#  bug fix in setting of plate ID (zeroed out for oceanic crust) 6.10.06

package Map;
use GD;
use Class::Date qw(date localdate gmdate now);
use Image::Magick;

# Flags and constants
my $DEBUG=0;			# The debug level of the calling program
my $rs;					# Generic recordset

my $BRIDGE_HOME = "/cgi-bin/bridge.pl";
my $GIF_HTTP_ADDR="/public/maps";               # For maps
my $PI = 3.14159265;
my $C72 = cos(72 * $PI / 180);
my $AILEFT = 100;
my $AITOP = 500;

sub acos {
    my $a;
    if ($_[0] > 1 || $_[0] < -1) {
        $a = 1;
    } else {
        $a = $_[0];
    }
    atan2( sqrt(1 - $a * $a), $a )
}
sub tan { sin($_[0]) / cos($_[0]) }
# returns great circle distance given two latitudes and a longitudinal offset
sub GCD { ( 180 / $PI ) * acos( ( sin($_[0]*$PI/180) * sin($_[1]*$PI/180) ) + ( cos($_[0]*$PI/180) * cos($_[1]*$PI/180) * cos($_[2]*$PI/180) ) ) }


$|=1;					# Freeflowing data

for $maptime (0)	{
#for $maptime (0..600)	{
	print "\r$maptime";
	&mapGetRotations();
	&makeMask();
}


# read Scotese's plate ID and rotation data files
sub mapGetRotations	{

	%plate = ();
	%rotx = ();
	%roty = ();
	%rotdeg = ();

	open IDS,"<./data/plateidsv2.lst";

	# skip the first line
	<IDS>;

	# read the plate IDs: numbers are longitude, latitude, and ID number
	while (<IDS>)	{
		s/\n//;
		my ($x,$y,$z) = split /,/,$_;
		# Andes correction: Scotese sometimes assigned 254 Ma ages to
		#  oceanic crust cells that are much younger, so those need to
		#  be eliminated JA 4.5.06
		if ( $z >= 900)	{
			$cellage{$x}{$y} = -1;
		} else	{
			$plate{$x}{$y} = $z;
			$plate{$x-1.01}{$y-1.01} = $z;
			$plate{$x-1.01}{$y-0.51} = $z;
			$plate{$x-0.51}{$y-1.01} = $z;
			$plate{$x-1.52}{$y-1.01} = $z;
			$plate{$x-1.01}{$y-1.52} = $z;

			$plate{$x-1.01}{$y+1.01} = $z;
			$plate{$x-1.01}{$y+0.51} = $z;
			$plate{$x-0.51}{$y+1.01} = $z;
			$plate{$x-1.52}{$y+1.01} = $z;
			$plate{$x-1.01}{$y+1.52} = $z;

			$plate{$x+1.01}{$y+1.01} = $z;
			$plate{$x+1.01}{$y+0.51} = $z;
			$plate{$x+0.51}{$y+1.01} = $z;
			$plate{$x+1.52}{$y+1.01} = $z;
			$plate{$x+1.01}{$y+1.52} = $z;

			$plate{$x+1.01}{$y-1.01} = $z;
			$plate{$x+1.01}{$y-0.51} = $z;
			$plate{$x+0.51}{$y-1.01} = $z;
			$plate{$x+1.52}{$y-1.01} = $z;
			$plate{$x+1.01}{$y-1.52} = $z;
		}
	}

	open ROT,"<./data/master01c.rot";

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
	if ( $maptime > 0 && $maptime < 10 )	{
		for my $p ( @pids )	{
			$rotx{0}{$p} = $rotx{10}{$p};
			$roty{0}{$p} = $roty{10}{$p};
			$rotdeg{0}{$p} = 0;
		}
	}

# use world's dumbest linear interpolation to estimate pole of rotation and
#  angle of rotation values if this time interval is non-standard
	if ( ! $roty{$maptime}{'1'} )	{

		my $basema = $maptime;
		while ( ! $roty{$basema}{'1'} && $basema >= 0 )	{
			$basema--;
		}
		my $topma = $maptime;
		while ( ! $roty{$topma}{'1'} && $topma < 1000 )	{
			$topma++;
		}

		if ( $topma < 1000 )	{
			$basewgt = ( $topma - $maptime ) / ( $topma - $basema );
			$topwgt = ( $maptime - $basema ) / ( $topma - $basema );
			@pids = sort { $a <=> $b } keys %{$rotx{$topma}};
			for my $pid ( @pids )	{
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

				($rotx{$maptime}{$pid},$roty{$maptime}{$pid})  = rotatePoint($interpolatedx,$interpolatedy,$x1,$y1,"reversed");

				$rotdeg{$maptime}{$pid} = ( $basewgt * $z1 ) + ( $topwgt * $z2 );

			# it's mathematically possible that the degrees have
			#  averaged out to over 180 or under -180 in something
			#  like the plate 611 375 Ma case
			if ( $rotdeg{$self->{maptime}}{$pid} > 180 )	{
				$rotdeg{$self->{maptime}}{$pid} = $rotdeg{$self->{maptime}}{$pid} - 360;
			} elsif ( $rotdeg{$self->{maptime}}{$pid} < - 180 )	{
				$rotdeg{$self->{maptime}}{$pid} = $rotdeg{$self->{maptime}}{$pid} + 360;
			}

			}
		}
	}

}

sub makeMask	{

	my @maskcoords = ();
	%projected = ();

	open MASK,"<./data/agev7.txt";
	my $lat = 90;
	while (<MASK>)	{
		s/\n//;
		@crustages = split /\t/,$_;
		$lng = -180;
		for $crustage (@crustages)	{
		# oceanic crust test: ages assigned to -1 if plate IDs
		#  are >= 900
			if ( $cellage{$lng}{$lat} != -1 && $crustage == 254 )	{
				push @maskcoords,$lng . ":" . $lat;
			} else	{
				$plate{$lng}{$lat} = "";
			}
			$lng++;
		}
		$lat--;
	}
	close MASK;

	open OUT,">./data/platepolygons/polygons.$maptime";
	open OUT2,">./data/platepolygons/edges.$maptime";
	for $mc (@maskcoords)	{
		my ($lng,$lat) = split /:/,$mc;
		$outside = "";
		if ( $plate{$lng}{$lat} != $plate{$lng-1}{$lat} ||
			$plate{$lng}{$lat} != $plate{$lng+1}{$lat} ||
			$plate{$lng}{$lat} != $plate{$lng}{$lat-1} ||
			$plate{$lng}{$lat} != $plate{$lng}{$lat+1} ||
			$plate{$lng}{$lat} != $plate{$lng-1}{$lat-1} ||
			$plate{$lng}{$lat} != $plate{$lng-1}{$lat+1} ||
			$plate{$lng}{$lat} != $plate{$lng+1}{$lat-1} ||
			$plate{$lng}{$lat} != $plate{$lng+1}{$lat+1} )	{
			$outside = "YES";
		}

		print OUT "# -b\n";
		if ( $outside eq "YES" )	{
			print OUT2 "# -b\n";
		}
		$lng1 = $lng - 1.01;
		$lng2 = $lng + 1.01;
		$lat1 = $lat - 1.01;
		$lat2 = $lat + 1.01;

		# lower left
		if ( $plate{$lng-1}{$lat} != $plate{$lng}{$lat} &&
			$plate{$lng}{$lat-1} != $plate{$lng}{$lat} )	{
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng - 0.51,$lat - 1.01,"crust");
			printPoint();
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng - 1.01,$lat - 0.51,"crust");
			printPoint();
		} elsif ( $plate{$lng-1}{$lat} == $plate{$lng}{$lat} &&
			$plate{$lng}{$lat-1} == $plate{$lng}{$lat} &&
			$plate{$lng-1}{$lat-1} != $plate{$lng}{$lat} )	{
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng - 1.52,$lat - 1.01,"crust");
			printPoint();
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng - 1.01,$lat - 1.52,"crust");
			printPoint();
		} else	{
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng1,$lat1,"crust");
			printPoint();
		}

		# upper left
		if ( $plate{$lng-1}{$lat} != $plate{$lng}{$lat} &&
			$plate{$lng}{$lat+1} != $plate{$lng}{$lat} )	{
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng - 1.01,$lat + 0.51,"crust");
			printPoint();
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng - 0.51,$lat + 1.01,"crust");
			printPoint();
		} elsif ( $plate{$lng-1}{$lat} == $plate{$lng}{$lat} &&
			$plate{$lng}{$lat+1} == $plate{$lng}{$lat} &&
			$plate{$lng-1}{$lat+1} != $plate{$lng}{$lat} )	{
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng - 1.52,$lat + 1.01,"crust");
			printPoint();
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng - 1.01,$lat + 1.52,"crust");
			printPoint();
		} else	{
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng1,$lat2,"crust");
			printPoint();
		}

		# upper right
		if ( $plate{$lng+1}{$lat} != $plate{$lng}{$lat} &&
			$plate{$lng}{$lat+1} != $plate{$lng}{$lat} )	{
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng + 0.51,$lat + 1.01,"crust");
			printPoint();
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng + 1.01,$lat + 0.51,"crust");
			printPoint();
		} elsif ( $plate{$lng+1}{$lat} == $plate{$lng}{$lat} &&
			$plate{$lng}{$lat+1} == $plate{$lng}{$lat} &&
			$plate{$lng+1}{$lat+1} != $plate{$lng}{$lat} )	{
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng + 1.52,$lat + 1.01,"crust");
			printPoint();
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng + 1.01,$lat + 1.52,"crust");
			printPoint();
		} else	{
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng2,$lat2,"crust");
			printPoint();
		}

		# lower right
		if ( $plate{$lng+1}{$lat} != $plate{$lng}{$lat} &&
			$plate{$lng}{$lat-1} != $plate{$lng}{$lat} )	{
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng + 1.01,$lat - 0.51,"crust");
			printPoint();
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng + 0.51,$lat - 1.01,"crust");
			printPoint();
		} elsif ( $plate{$lng+1}{$lat} == $plate{$lng}{$lat} &&
			$plate{$lng}{$lat-1} == $plate{$lng}{$lat} &&
			$plate{$lng+1}{$lat-1} != $plate{$lng}{$lat} )	{
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng + 1.52,$lat - 1.01,"crust");
			printPoint();
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng + 1.01,$lat - 1.52,"crust");
			printPoint();
		} else	{
			($newlng,$newlat,$rawlng,$rawlat,$pid) = &projectPoints($lng2,$lat1,"crust");
			printPoint();
		}
	}
	close OUT;
	close OUT2;

}

sub printPoint	{

	print OUT "$newlng\t$newlat\t$pid\n";
	if ( $outside eq "YES" )	{
		print OUT2 "$newlng\t$newlat\t$pid\n";
	}

}

sub projectPoints	{

	my ($x,$y,$pointclass) = @_;

	my $pid;
	my $rotation;
	my $oldx;
	my $oldy;

	# rotate point if a paleogeographic map is being made
	# strategy: rotate point such that the pole of rotation is the
	#  north pole; use the GCD between them to get the latitude;
	#  add the degree offset to its longitude to get the new value;
	#  re-rotate point back into the original coordinate system
	if ( $maptime > 0 && ( $midlng != $x || $midlat != $y ) && $pointclass ne "grid" && $projected{$x}{$y} eq "" )	{

		my $ma = $maptime;
		$oldx = $x;
		$oldy = $y;

	# integer coordinates are needed to determine the plate ID
	# IMPORTANT: this section, crucial in Map.pm, is blocked out so
	#  the four corners of each grid cell can be rotated
		my $q; 
		my $r;
		if ( $x >= 0 )	{
		#	$q = int($x+0.5);
		} else	{
		#	$q = int($x-0.5);
		}
		if ( $y >= 0 )	{
		#	$r = int($y+0.5);
		} else	{
		#	$r = int($y-0.5);
		}

	# what plate is this point on?
		$pid = $plate{$x}{$y};
	#	$pid = $plate{$q}{$r};

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
		($x,$y) = rotatePoint($x,$y,$neworigx,$neworigy);
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
		$projected{$oldx}{$oldy} = $x . ":" . $y;
		if ( $x eq "NaN" || $y eq "NaN" )	{
			return('NaN','NaN');
		}

	}
	if ( $oldx ne "" && $oldy ne "" )	{
		($x,$y) = split /:/,$projected{$oldx}{$oldy};
	}

	# rotate point if origin is not at 0/0
	if ( $pointclass ne "crust" && ( $midlat != 0 || $midlng != 0 ) )	{
		($x,$y) = rotatePoint($x,$y,$midlng,$midlat);
		if ( $x eq "NaN" || $y eq "NaN" )	{
			return('NaN','NaN');
		}
	}

	$rawx = $x;
	$rawy = $y;

	if ( $pointclass eq "crust" )	{
		if ( ! $pid )	{
			$pid = $plate{$x}{$y};
		}
		return($x,$y,$rawx,$rawy,$pid);
	}

	if ( $projection eq "orthographic" && $x ne "" )	{

		# how far is this point from the origin?
		my $dist = ($x**2 + $y**2)**0.5;
		# dark side of the Earth is invisible!
		if ( $dist > 90 )	{
			return('NaN','NaN');
		}
		# transform to radians
		$dist = $PI * $dist / 360;
		$x = $x * cos($dist) * ( 1 / cos( $PI / 4 ) ) ;
		$y = $y * cos($dist) * ( 1 / cos( $PI / 4 ) ) ;
		# fool tests for returned null data elsewhere in the script
		if ( $x == 0 )	{
			$x = 0.001;
		}
		if ( $y == 0 )	{
			$y = 0.001;
		}
	} elsif ( $projection eq "Mollweide" && $x ne "")	{
	# WARNING: this is an approximation of the Mollweide projection;
	#  a factor of 180 deg for the longitude would seem intuitive,
	#  but 190 gives a better visual match to assorted Google images
		$x = $x * cos($y * $PI / 190);
		$y = $y * cos($y * $PI / 360);
	} elsif ( $projection eq "Eckert" && $x ne "")	{
	# WARNING: this is an approximation of the Eckert IV projection
	#  and is not nearly as complicated
		$x = $x * cos($y * $PI / 300);
		$y = $y * cos($y * $PI / 360);
	}
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
