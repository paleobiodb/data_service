package Map;
use GD;
use Class::Date qw(date localdate gmdate now);

# Flags and constants
my $DEBUG=0;			# The debug level of the calling program
my $dbh;				# The database handle
my $q;					# Reference to the parameters
my $s;					# Reference to the session
my $sql;				# Any SQL string
my $rs;					# Generic recordset

my $BRIDGE_HOME = "/cgi-bin/bridge.pl";
my $GIF_HTTP_ADDR="/public/maps";               # For maps
my $COAST_DIR = $ENV{MAP_COAST_DIR};        # For maps
my $GIF_DIR = $ENV{MAP_GIF_DIR};      # For maps
my $PI = 3.14159265;
my $C72 = cos(72 * $PI / 180);
sub acos { atan2( sqrt(1 - $_[0] * $_[0]), $_[0] ) }
sub tan { sin($_[0]) / cos($_[0]) }

sub new {
	my $class = shift;
	$dbh = shift;
	$q = shift;
	$s = shift;
	my $self = {};

	bless $self, $class;
	return $self;
}

sub buildMap {
	my $self = shift;

	$|=1;					# Freeflowing data

	$self->mapGetScale();
	$self->mapDefineOutlines();
	$self->mapQueryDb();
	$self->mapDrawMap();

}

sub mapQueryDb	{
	my $self = shift;

	# if a research project (not a group) is requested, get a list of
	#  references included in that project JA 3.10.02
	my $reflist;
	if ( $q->param('research_group') =~ /(^ETE$)|(^5%$)|(^PGAP$)/ )	{
		$sql = "SELECT reference_no FROM refs WHERE project_name LIKE '%";
		$sql .= $q->param('research_group') . "%'";

		$sth = $dbh->prepare($sql);
		$sth->execute();
		@refrefs = @{$sth->fetchall_arrayref()};
		$sth->finish();

		for $refref (@refrefs)  {
			$reflist .= "," . ${$refref}[0];
		}
		$reflist =~ s/^,//;
	}


	# if a genus name is requested, query the occurrences table to get
	#  a list of useable collections
	# Also handles species name searches JA 19-20.8.02
	my $genus;
	my $species;
	if ( $q->param('genus_name') ) {
		# PM 09/13/02 added the '\s+' pattern for space matching.
		if ( $q->param('genus_name') =~ /\s+/ )	{
			($genus,$species) = split /\s+/,$q->param('genus_name');
		} elsif ( $q->param('taxon_rank') eq "species" )	{
			$species = $q->param('genus_name');
		} else	{
			$genus = $q->param('genus_name');
		}
		$sql = qq|SELECT collection_no FROM occurrences WHERE |;
		if ( $genus )	{
			$sql .= "genus_name='" . $genus;
			if ( $species )	{
				$sql .= "' AND ";
			}
		}
		if ( $species )	{
			$sql .= "species_name='" . $species;
		}
		$sql .= "'";
		$sth2 = $dbh->prepare($sql);
		# DEBUG: PM 09/13/02
		$self->dbg("mapQueryDb sql: $sql<br>");
		$sth2->execute();
		# DEBUG: PM 09/10/02
		$self->dbg("results from collection_no search in db: <br>");
		while (my $occRef = $sth2->fetchrow_hashref())	{
			my %occ = %{$occRef};
			if ($occ{'collection_no'} ne ""){
			  $self->dbg($occ{'collection_no'});
			  $collok{$occ{'collection_no'}} = "Y";
			}
		}
	}

	# figure out what collection table values are being matched 
	# in what fields
	@allfields = (	'research_group',
			'authorizer',
			'country',
			'state',
			'period',
			'epoch',
			'stage', 
			'formation', 
			'lithology1', 
			'environment',
			'modified_since');

	# Handle days/weeks/months ago requests JA 25.6.02
	if ($q->param('modified_since'))	{
		#local $Class::Date::DATE_FORMAT="%Y%m%d%H%M%S";
		$nowDate = now();
		if ( "yesterday" eq $q->param('modified_since') )	{
			$nowDate = $nowDate-'1D';
		}
		elsif ( "two days ago" eq $q->param('modified_since') )	{
			$nowDate = $nowDate-'2D';
		}
		elsif ( "three days ago" eq $q->param('modified_since') )	{
			$nowDate = $nowDate-'3D';
		}
		elsif ( "last week" eq $q->param('modified_since') )	{
			$nowDate = $nowDate-'7D';
		}
		elsif ( "two weeks ago" eq $q->param('modified_since') )	{
			$nowDate = $nowDate-'14D';
		}
		elsif ( "three weeks ago" eq $q->param('modified_since') )	{
			$nowDate = $nowDate-'21D';
		}
		elsif ( "last month" eq $q->param('modified_since') )	{
			$nowDate = $nowDate-'1M';
		}
		my ($a,$b) = split / /,$nowDate;
		my ($a,$b,$c) = split /-/,$a,3;
		$q->param('year' => $a);
		$q->param('month' => $b);
		$q->param('date' => $c);
	}
	if ($q->param('year'))	{
		$q->param('modified_since' => $q->param('year')." ".$q->param('month')." ".$q->param('date'));
	}

	for my $field ( @allfields ) {
		if ( $q->param($field) && $q->param( $field ) ne "all")	{
			$filledfields{$field} = $q->param ( $field );
		}
	}

	# keep track of the search string so it can be printed later
	if ( $q->param('genus_name') )	{
		if ( $species )	{
			$searchstring = "species = ";
			if ( $genus )	{
				$searchstring .= $genus." ".$species;
			} else	{
				$searchstring .= $species;
			}
		} elsif ( $q->param('taxon_rank') eq "Higher-taxon" )	{
			$searchstring = "taxon = ";
			$searchstring .= $q->param('genus_name');
		} else	{
			$searchstring = "genus = ";
			$searchstring .= $q->param('genus_name');
		}
		$searchstring .= ", ";
	}
	if ($cont ne "")	{
		$searchstring .= "continent = $cont, ";
	}
	for $curField (keys %filledfields)	{
		if ($filledfields{$curField} ne "")	{
			$searchstring .= "$curField = $filledfields{$curField}, ";
		}
	}
	$searchstring =~ s/, $//;
	# clean "1" that appears at end of "lithology1"
	$searchstring =~ s/1//;

	# Start the SQL 
	$sql = qq|SELECT *, DATE_FORMAT(release_date, '%Y%m%d') rd_short  FROM collections |;

	# PM 09/10/02. Specify a default WHERE clause of the collection
	# numbers returned above. This drastically reduces database load for
	# the map drawing section of the taxon information script.
	my $collection_no_list = join(",",keys(%collok));
	my $where = "";
	# %collok is only populated if $q->param("genus_name") was provided
	if($collection_no_list ne ""){
		$where = "WHERE collection_no IN($collection_no_list) ";
	}

	for $t (keys %filledfields)	{
		if ( $filledfields{$t} )	{
			# handle period
			if ($t eq "period")	{
				$where = &::buildWhere ( $where, qq| (period_max='$filledfields{$t}' OR period_min='$filledfields{$t}')| );
			}
			# handle epoch 
			elsif ($t eq "epoch")	{
				$where = &::buildWhere ( $where, qq| (epoch_max='$filledfields{$t}' OR epoch_min='$filledfields{$t}')| );
			}
			# handle stage
			elsif ($t eq "stage")	{
				$where = &::buildWhere ( $where, qq| (intage_max LIKE "$filledfields{$t}%" OR intage_min LIKE "$filledfields{$t}%" OR locage_max LIKE "$filledfields{$t}%" OR locage_min LIKE "$filledfields{$t}%")| );
			}
			# handle lithology
			elsif ($t eq "lithology1")	{
				$where = &::buildWhere ( $where, qq| (lithology1='$filledfields{$t}' OR lithology2='$filledfields{$t}')| );
			}
			# handle modified date
			elsif ($t eq "modified_since")	{
				%month2num = (	"January" => "01", "February" => "02", "March" => "03",
								"April" => "04", "May" => "05", "June" => "06",
								"July" => "07", "August" => "08", "September" => "09",
								"October" => "10", "November" => "11",
								"December" => "12");
				my ($yy,$mm,$dd) = split / /,$filledfields{$t},3;
				if (length $mm == 1)	{
					$dd = "0".$mm;
				}
				if (length $dd == 1)	{
					$dd = "0".$dd;
				}
				if ($mm =~ /[a-z]/)	{
					$filledfields{$t} = $yy.$month2num{$mm}.$dd."000000";
				}
				else	{
					$filledfields{$t} = $yy.$mm.$dd."000000";
				}
				$where = &::buildWhere ( $where, qq| modified>$filledfields{$t}| );
			# following written by JA 3.10.02
			} elsif ( $t eq "research_group" && $q->param('research_group') =~ /(^ETE$)|(^5%$)|(^PGAP$)/ )	{
				$where .= " AND reference_no IN (" . $reflist . ")";
			} elsif ( $t eq "research_group" ) {
				# research_group is now a set -- tone 7 jun 2002
#				$where .= " AND FIND_IN_SET(research_group, '".$q->param("research_group")."')";
				$where .= " AND FIND_IN_SET('".$q->param("research_group")."', research_group)>0";
			} else {
				$where = &::buildWhere ( $where, qq| $t='$filledfields{$t}'| );
			}
		}
	}

 	if ( $where ) { $sql .= "$where "; }
	$sql =~ s/FROM collections  AND /FROM collections WHERE /;
	$sql =~ s/\s+/ /gms;
	$self->dbg ( "Final sql: $sql<br>" );
	# NOTE: Results attached to this statement handle are used in mapDrawMap
	$sth = $dbh->prepare($sql);
	$sth->execute();

}

sub mapGetScale	{
	my $self = shift;

  $scale = 1;
  $cont = $q->param('mapcontinent');
  $scale = $q->param('mapscale');
  $scale =~ s/x //i;

  if ($cont ne "standard")	{
    if ($cont =~ /Africa/)	{
      $midlng = 35;
      $midlat = 10;
    } elsif ($cont =~ /Antarctica/)	{
      $midlng = 0;
      $midlat = -89;
    } elsif ($cont =~ /Arctic/)	{
      $midlng = 0;
      $midlat = 89;
    } elsif ($cont =~ /Asia \(north\)/)	{
      $midlng = 100;
      $midlat = 50;
    } elsif ($cont =~ /Asia \(south\)/)	{
      $midlng = 100;
      $midlat = 20;
    } elsif ($cont =~ /Australia/)	{
      $midlng = 135;
      $midlat = -28;
    } elsif ($cont =~ /Europe/)	{
      $midlng = 10;
      $midlat = 50;
    } elsif ($cont =~ /North America/)	{
      $midlng = -100;
      $midlat = 35;
    } elsif ($cont =~ /South America/)	{
      $midlng = -50;
      $midlat = -10;
    }
  }
  # NOTE: shouldn't these be module globals??
  $offlng = 180 * ( $scale - 1 ) / $scale;
  $offlat = 90 * ( $scale - 1 ) / $scale;
}

# extract outlines taken from NOAA's NGDC Coastline Extractor
sub mapDefineOutlines	{
	my $self = shift;

	if ( $q->param('mapresolution') eq "coarse" )	{
		$resostem = 50;
	} elsif ( $q->param('mapresolution') eq "medium" )	{
		$resostem = 25;
	} elsif ( $q->param('mapresolution') eq "fine" )	{
		$resostem = 10;
	}

	if ( ! open COAST,"<$COAST_DIR/noaa.coastlines.$resostem" ) {
		$self->htmlError ( "Couldn't open [$COAST_DIR/noaa.coastlines.$resostem]: $!" );
	}
	while (<COAST>)	{
		($a,$b) = split /\t/,$_;
		if ($a ne "")	{
			push @worldlng,$a;
			push @worldlat,$b;
		}
	}

	if ( $q->param('borderlinecolor') ne "none" )	{
		if ( ! open BORDER,"<$COAST_DIR/noaa.borders.$resostem" ) {
			$self->htmlError ( "Couldn't open [$COAST_DIR/noaa.borders.$resostem]: $!" );
		}
		while (<BORDER>)	{
			($a,$b) = split /\t/,$_;
			if ($a ne "")	{
				push @borderlng,$a;
				push @borderlat,$b;
			}
		}
		close BORDER;
	}
	if ( $q->param('usalinecolor') ne "none" )	{
		if ( ! open USA,"<$COAST_DIR/noaa.usa.$resostem" ) {
			$self->htmlError ( "Couldn't open [$COAST_DIR/noaa.usa.$resostem]: $!" );
		}
		while (<USA>)	{
			($a,$b) = split /\t/,$_;
			if ($a ne "")	{
				push @usalng,$a;
				push @usalat,$b;
			}
		}
		close USA;
	}
}

sub mapDrawMap	{
  my $self = shift;

  # erase the last map that was drawn
  if ( ! open GIFCOUNT,"<$GIF_DIR/gifcount" ) {
		$self->htmlError ( "Couldn't open [$GIF_DIR/gifcount]: $!" );
  }
  $gifcount = <GIFCOUNT>;
  s/\n//;
  $gifname = "pbdbmap" . $gifcount . ".gif";
  unlink "$GIF_DIR/$gifname";
  close GIFCOUNT;
  $gifcount++;
  if ( ! open GIFCOUNT,">$GIF_DIR/gifcount" ) {
		$self->htmlError ( "Couldn't open [$GIF_DIR/gifcount]: $!" );
  }
  print GIFCOUNT "$gifcount";
  close GIFCOUNT;

  $gifcount++;
  $gifname = "pbdbmap" . $gifcount . ".gif";
  if ( ! open MAPGIF,">$GIF_DIR/$gifname" ) {
		$self->htmlError ( "Couldn't open [$GIF_DIR/$gifname]: $!" );
  }

  # figure out the desired size of the rectangle surrounding the map
  $maxLat = $q->param('maxlat');
  $minLat = $q->param('minlat');
  $maxLng = $q->param('maxlng');
  $minLng = $q->param('minlng');

  $hmult = 1.6;
  $vmult = 2;
  $hpix = 360;
  $vpix = 180;
  if ( $q->param('projection') eq "orthographic" )	{
    $hpix = 280;
    $vpix = 280;
  } elsif ( ( $cont =~ /Africa/ || $cont =~ /South America/ ) &&
            $scale > 1.5 )	{
    $hpix = 280;
    $vpix = 240;
  }
  # PM 09/10/02 - Draw a half-sized map for the taxon information script.
  if($q->param("taxon_info_script") eq "yes")	{
	$hmult = 0.8;
	$vmult = 1.0;
  }
  if ( $q->param("projection") eq "orthographic")	{
    $hmult = $hmult * 1.25;
  }
  $height = $vmult * $vpix;
  $width = $hmult * $hpix;
  # recenter the image if the GIF size is non-standard
  $gifoffhor = ( 360 - $hpix ) / ( $scale * 2 );
  $gifoffver = ( 180 - $vpix ) / ( $scale * 2 );

  $im = new GD::Image($width,$height);

  $col{'white'} = $im->colorAllocate(255,255,255);
  $col{'borderblack'} = $im->colorAllocate(1,1,1);
  $col{'black'} = $im->colorAllocate(0,0,0);
  $col{'gray'} = $im->colorAllocate(127,127,127);
  $col{'lightgray'} = $im->colorAllocate(191,191,191);
  $col{'offwhite'} = $im->colorAllocate(254,254,254);
  $col{'pink'} = $im->colorAllocate(255,191,191);
  $col{'red'} = $im->colorAllocate(255,0,0);
  $col{'darkred'} = $im->colorAllocate(127,0,0);
  $col{'brown'} = $im->colorAllocate(127,63,0);
  $col{'ochre'} = $im->colorAllocate(191,127,0);
  $col{'orange'} = $im->colorAllocate(255,127,0);
  $col{'yellow'} = $im->colorAllocate(255,255,0);
  $col{'green'} = $im->colorAllocate(0,255,0);
  $col{'emerald'} = $im->colorAllocate(0,159,0);
  $col{'teal'} = $im->colorAllocate(0,255,255);
  $col{'blue'} = $im->colorAllocate(63,63,255);
  $col{'darkblue'} = $im->colorAllocate(0,0,255);
  $col{'violet'} = $im->colorAllocate(191,0,255);
  $col{'darkviolet'} = $im->colorAllocate(127,0,127);
  $col{'purple'} = $im->colorAllocate(223,0,255);

  $dotcolor = $q->param('dotcolor');
  $bordercolor = $dotcolor;
  if ($q->param('dotborder') eq "with")	{
	if($q->param('mapbgcolor') eq "black")	{
		$bordercolor = "white";
	}
	else	{
		$bordercolor = "borderblack";
	}
  }
  $dotsizeterm = $q->param('pointsize');
  $dotshape = $q->param('pointshape');
  if ($dotsizeterm eq "tiny")	{
    $dotsize = 1.5;
  } elsif ($dotsizeterm eq "small")	{
    $dotsize = 2;
  } elsif ($dotsizeterm eq "medium")	{
    $dotsize = 3;
  } elsif ($dotsizeterm eq "large")	{
    $dotsize = 4;
  }
  $maxdotsize = $dotsize;
  if ($dotsizeterm eq "proportional")	{
    $maxdotsize = 7;
  }


	# create an interlaced GIF with a white background
	$im->interlaced('true');
	if ( $q->param('mapbgcolor') ne "transparent" )	{
		if ( $q->param("projection") ne "orthographic" )	{
			$im->fill(100,100,$col{$q->param('mapbgcolor')});
		} else	{
		# for a orthographic projection, draw a circle and fill it
			$im->transparent('');
			my ($origx,$origy) = $self->projectPoints($midlng,$midlat);
			$origx = $self->getLng($origx);
			$origy = $self->getLat($origy);
			my $edgecolor = $col{$q->param('coastlinecolor')};
			if ( $q->param('coastlinecolor') eq "white" )	{
				$edgecolor = $col{'offwhite'};
			}
          		$im->arc($origx,$origy,180*$hmult*$scale,180*$hmult*$scale,0,360,$edgecolor);
			$im->fillToBorder($origx,$origy,$edgecolor,$col{$q->param('mapbgcolor')});
		}
	} else	{
		$im->transparent('');
	}

	print "<table>\n<tr>\n<td>\n<map name=\"PBDBmap\">\n";

	# Get rows okayed by permissions module
	my $p = Permissions->new ( $s );
	my @dataRows = ( );
	my $limit = 1000000;
	my $ofRows = 0;
	# NOTE: this statement handle is apparently picked up from the
	# mapQueryDb method, though it's not 'self' or passed in...
	# (It stays in scope only because it was initialized through $dbh
	# which is global)
	$p->getReadRows ( $sth, \@dataRows, $limit, \$ofRows );

	$self->dbg ( "Returned $ofRows rows okayed by permissions module" );

 # draw coastlines
 # first rescale the coordinates depending on the rotation
  if ( $q->param('mapcontinent') ne "standard" || $q->param('projection') ne "rectilinear" )	{
    for $c (0..$#worldlat)	{
      if ( $worldlat[$c] ne "" )	{
        ($worldlng[$c],$worldlat[$c]) = $self->projectPoints($worldlng[$c],$worldlat[$c]);
      }
    }
    if ( $q->param('borderlinecolor') ne "none" )	{
      for $c (0..$#borderlat)	{
        if ( $borderlat[$c] ne "" )	{
          ($borderlng[$c],$borderlat[$c]) = $self->projectPoints($borderlng[$c],$borderlat[$c]);
        }
      }
    }
    if ( $q->param('usalinecolor') ne "none" )	{
      for $c (0..$#usalat)	{
        if ( $usalat[$c] ne "" )	{
          ($usalng[$c],$usalat[$c]) = $self->projectPoints($usalng[$c],$usalat[$c]);
        }
      }
    }
  }
  if ( $q->param('linethickness') eq "thick" )	{
    $thickness = 0.5;
  } elsif ( $q->param('linethickness') eq "medium" )	{
    $thickness = 0.25;
  } else	{
    $thickness = 0;
  }
  for $c (0..$#worldlat-1)	{
    if ( $worldlat[$c] ne "" && $worldlat[$c+1] ne "" &&
         ( abs ( $worldlng[$c] - $worldlng[$c+1] ) < 345 ) )	{
      $im->line( $self->getLng($worldlng[$c]),$self->getLat($worldlat[$c]),$self->getLng($worldlng[$c+1]),$self->getLat($worldlat[$c+1]),$col{$q->param('coastlinecolor')});
     # extra lines offset horizontally
      if ( $thickness > 0 )	{
        $im->line( $self->getLng($worldlng[$c])-$thickness,$self->getLat($worldlat[$c]),$self->getLng($worldlng[$c+1])-$thickness,$self->getLat($worldlat[$c+1]),$col{$q->param('coastlinecolor')});
        $im->line( $self->getLng($worldlng[$c])+$thickness,$self->getLat($worldlat[$c]),$self->getLng($worldlng[$c+1])+$thickness,$self->getLat($worldlat[$c+1]),$col{$q->param('coastlinecolor')});
     # extra lines offset vertically
        $im->line( $self->getLng($worldlng[$c]),$self->getLat($worldlat[$c])-$thickness,$self->getLng($worldlng[$c+1]),$self->getLat($worldlat[$c+1])-$thickness,$col{$q->param('coastlinecolor')});
        $im->line( $self->getLng($worldlng[$c]),$self->getLat($worldlat[$c])+$thickness,$self->getLng($worldlng[$c+1]),$self->getLat($worldlat[$c+1])+$thickness,$col{$q->param('coastlinecolor')});
      }
    }
  }
  if ( $q->param('borderlinecolor') ne "none" )	{
    for $c (0..$#borderlat-1)	{
      if ( $borderlat[$c] ne "" && $borderlat[$c+1] ne "" &&
           ( abs ( $borderlng[$c] - $borderlng[$c+1] ) < 345 ) )	{
        $im->line( $self->getLng($borderlng[$c]),$self->getLat($borderlat[$c]),$self->getLng($borderlng[$c+1]),$self->getLat($borderlat[$c+1]),$col{$q->param('borderlinecolor')});
      }
    }
  }
  if ( $q->param('usalinecolor') ne "none" )	{
    for $c (0..$#usalat-1)	{
      if ( $usalat[$c] ne "" && $usalat[$c+1] ne "" &&
           ( abs ( $usalng[$c] - $usalng[$c+1] ) < 345 ) )	{
        $im->line( $self->getLng($usalng[$c]),$self->getLat($usalat[$c]),$self->getLng($usalng[$c+1]),$self->getLat($usalat[$c+1]),$col{$q->param('usalinecolor')});
      }
    }
  }

 # draw grids
  $grids = $q->param('gridsize');
  if ($grids > 0)	{
    if ( $q->param('projection') eq "rectilinear" )	{
      for my $lat (1..int($vpix/$grids)-1)	{
        $color = $col{$q->param('gridcolor')};
        if ($lat * $grids == 90)	{
          $color = $col{'gray'};
        }
        $im->line(0,$scale*$vmult*$lat*$grids,$scale*$hmult*$hpix,$scale*$vmult*$lat*$grids,$color);
      }
      for my $lng (k..int($hpix/$grids)-1)	{
        $color = $col{$q->param('gridcolor')};
        if ($lng * $grids == 180)	{
          $color = $col{'gray'};
        }
        $im->line($scale*$hmult*$lng*$grids,0,$scale*$hmult*$lng*$grids,$scale*$vmult*$vpix,$color);
      }
    }
  # draw grids given a orthographic projection
    else	{
      for my $lat ( int(-90/$grids)..int(90/$grids) )	{
        for my $deg (-180..179)	{
          my ($lng1,$lat1) = $self->projectPoints($deg , $lat * $grids);
          my ($lng2,$lat2) = $self->projectPoints($deg + 1 , $lat * $grids);
          if ( $lng1 && $lng2 )	{
            $im->line($self->getLng($lng1),$self->getLat($lat1),$self->getLng($lng2),$self->getLat($lat2),$col{$q->param('gridcolor')});
          }
        }
      }
      for my $lng ( int(-180/$grids)..int(180/$grids) )	{
        for my $deg (-90..89)	{
          my ($lng1,$lat1) = $self->projectPoints($lng * $grids, $deg);
          my ($lng2,$lat2) = $self->projectPoints($lng * $grids, $deg + 1);
          if ( $lat1 && $lat2 )	{
            $im->line($self->getLng($lng1),$self->getLat($lat1),$self->getLng($lng2),$self->getLat($lat2),$col{$q->param('gridcolor')});
          }
        }
      }
    }
  }

  # draw collection data points
  foreach $collRef ( @dataRows ) {
    %coll = %{$collRef};
    if ( ( $coll{'latdeg'} > 0 || $coll{'latmin'} > 0 || $coll{'latdec'} > 0 ) &&
         ( $coll{'lngdeg'} > 0 || $coll{'lngmin'} > 0 || $coll{'lngdec'} > 0 ) && 
			( $collok{$coll{'collection_no'}} eq "Y" || 
			! $q->param('genus_name') ) 
		) {

      ($x1,$y1) = $self->getCoords($coll{'lngdeg'},$coll{'latdeg'});

      if ( $x1 > 0 && $y1 > 0 && $x1-$maxdotsize > 0 && 
			$x1+$maxdotsize < $width &&
			$y1-$maxdotsize > 0 && 
			$y1+$maxdotsize < $height )	{
        $atCoord{$x1}{$y1}++;
        $longVal{$x1} = $coll{'lngdeg'} . " " . $coll{'lngdir'};
        $latVal{$y1} = $coll{'latdeg'} . " " . $coll{'latdir'};
        $matches++;
      }
    }
  }

  for $x1 (keys %longVal){
	for $y1 (keys %latVal){
		if ($atCoord{$x1}{$y1} > 0){
			if ($dotsizeterm eq "proportional"){
				$dotsize = int($atCoord{$x1}{$y1}**0.5) + 1;
			}
		# There is no way for a public user to use this at the moment.
        #if( $q->param('user') =~ /paleodb/ && $q->param('user') !~ /public/ ){
			print "<area shape=\"rect\" coords=\"";
			printf "%d,%d,%d,%d", int($x1-(1.5*$dotsize)), int($y1-(1.5*$dotsize)), int($x1+(1.5*$dotsize)), int($y1+(1.5*$dotsize));
			print "\" href=\"$BRIDGE_HOME?action=displayCollResults";
			for $t (keys %filledfields)	{
				if ($filledfields{$t} ne "")	{
					my $temp = $filledfields{$t};
					$temp =~ s/"//g;
					$temp =~ s/ /\+/g;
					print "&$t=$temp";

					# HACK: force search to use wildcards if lithology or formation is searched, in order
					#  to avoid problems with values that include double quotes
					if ($t =~ /(formation)|(lithology1)/)	{
						print "&wild=Y";
					}
				}
			}
			if ( $q->param('genus_name') )	{
				print "&genus_name=" . $q->param('genus_name');
				print "&taxon_rank=" . $q->param('taxon_rank');
			}
			($lngdeg,$lngdir) = split / /,$longVal{$x1};
			($latdeg,$latdir) = split / /,$latVal{$y1};
			print "&latdeg=$latdeg&latdir=$latdir&lngdeg=$lngdeg&lngdir=$lngdir\">\n";
		#}

    # draw a circle and fill it
        if ($dotshape =~ /^circles$/)	{
          $im->arc($x1,$y1,($dotsize*3)+2,($dotsize*3)+2,0,360,$col{$bordercolor});
          if ( $x1+($dotsize*3)+2 < $width && $x1-($dotsize*3)-2 > 0 &&
               $y1+($dotsize*3)+2 < $height && $y1-($dotsize*3)-2 > 0 )	{
            $im->fillToBorder($x1,$y1,$col{$bordercolor},$col{$dotcolor});
            $im->fillToBorder($x1+$dotsize,$y1,$col{$bordercolor},$col{$dotcolor});
            $im->fillToBorder($x1-$dotsize,$y1,$col{$bordercolor},$col{$dotcolor});
            $im->fillToBorder($x1,$y1+$dotsize,$col{$bordercolor},$col{$dotcolor});
            $im->fillToBorder($x1,$y1-$dotsize,$col{$bordercolor},$col{$dotcolor});
          }
        }
        elsif ($dotshape =~ /^crosses$/)	{
          $im->line($x1-$dotsize,$y1-$dotsize,$x1+$dotsize,$y1+$dotsize,$col{$dotcolor});
          $im->line($x1-$dotsize+0.50,$y1-$dotsize+0.50,$x1+$dotsize+0.50,$y1+$dotsize+0.50,$col{$dotcolor});
          $im->line($x1-$dotsize+0.50,$y1-$dotsize-0.50,$x1+$dotsize+0.50,$y1+$dotsize-0.50,$col{$dotcolor});
          $im->line($x1-$dotsize-0.50,$y1-$dotsize+0.50,$x1+$dotsize-0.50,$y1+$dotsize+0.50,$col{$dotcolor});
          $im->line($x1-$dotsize-0.50,$y1-$dotsize-0.50,$x1+$dotsize-0.50,$y1+$dotsize-0.50,$col{$dotcolor});

          $im->line($x1+$dotsize,$y1-$dotsize,$x1-$dotsize,$y1+$dotsize,$col{$dotcolor});
          $im->line($x1+$dotsize+0.50,$y1-$dotsize+0.50,$x1-$dotsize+0.50,$y1+$dotsize+0.50,$col{$dotcolor});
          $im->line($x1+$dotsize+0.50,$y1-$dotsize-0.50,$x1-$dotsize+0.50,$y1+$dotsize-0.50,$col{$dotcolor});
          $im->line($x1+$dotsize-0.50,$y1-$dotsize+0.50,$x1-$dotsize-0.50,$y1+$dotsize+0.50,$col{$dotcolor});
          $im->line($x1+$dotsize-0.50,$y1-$dotsize-0.50,$x1-$dotsize-0.50,$y1+$dotsize-0.50,$col{$dotcolor});
        }
        elsif ($dotshape =~ /^diamonds$/)	{
          my $poly = new GD::Polygon;
          $poly->addPt($x1,$y1+($dotsize*2));
          $poly->addPt($x1+($dotsize*2),$y1);
          $poly->addPt($x1,$y1-($dotsize*2));
          $poly->addPt($x1-($dotsize*2),$y1);
          $im->filledPolygon($poly,$col{$dotcolor});
        }
        elsif ($dotshape =~ /^stars$/)	{
          my $poly = new GD::Polygon;
          $poly->addPt($x1,$y1-($dotsize/$C72));
          $poly->addPt($x1+($dotsize*sin(36*$PI/180)),$y1-($dotsize*cos(36*$PI/180)));
          $poly->addPt($x1+($dotsize/$C72*sin(72*$PI/180)),$y1-($dotsize/$C72*cos(72*$PI/180)));
          $poly->addPt($x1+($dotsize*sin(108*$PI/180)),$y1-($dotsize*cos(108*$PI/180)));
          $poly->addPt($x1+($dotsize/$C72*sin(144*$PI/180)),$y1-($dotsize/$C72*cos(144*$PI/180)));
          $poly->addPt($x1+($dotsize*sin(180*$PI/180)),$y1-($dotsize*cos(180*$PI/180)));
          $poly->addPt($x1+($dotsize/$C72*sin(216*$PI/180)),$y1-($dotsize/$C72*cos(216*$PI/180)));
          $poly->addPt($x1+($dotsize*sin(252*$PI/180)),$y1-($dotsize*cos(252*$PI/180)));
          $poly->addPt($x1+($dotsize/$C72*sin(288*$PI/180)),$y1-($dotsize/$C72*cos(288*$PI/180)));
          $poly->addPt($x1+($dotsize*sin(324*$PI/180)),$y1-($dotsize*cos(324*$PI/180)));
          $im->filledPolygon($poly,$col{$dotcolor});
        }
    # or draw a triangle
        elsif ($dotshape =~ /^triangles$/)	{
          my $poly = new GD::Polygon;
       # lower left vertex
          $poly->addPt($x1+($dotsize*2),$y1+($dotsize*2*sin(60*$PI/180)));
       # top middle vertex
          $poly->addPt($x1,$y1-($dotsize*2*sin(60*$PI/180)));
       # lower right vertex
          $poly->addPt($x1-($dotsize*2),$y1+($dotsize*2*sin(60*$PI/180)));
          $im->filledPolygon($poly,$col{$dotcolor});
        }
    # or draw a square
        else	{
          $im->filledRectangle($x1-($dotsize*1.5),$y1-($dotsize*1.5),$x1+($dotsize*1.5),$y1+($dotsize*1.5),$col{$dotcolor});
        }
      }
    }
  }

 # redraw the borders if they are not the same color as the points
  if ($dotcolor ne $bordercolor)	{
    for $x1 (keys %longVal)	{
      for $y1 (keys %latVal)	{
        if ($atCoord{$x1}{$y1} > 0)	{
          if ($dotsizeterm eq "proportional")	{
            $dotsize = int($atCoord{$x1}{$y1}**0.5) + 1;
          }
          if ($dotshape =~ /^circles$/)	{
            $im->arc($x1,$y1,($dotsize*3)+2,($dotsize*3)+2,0,$hpix,$col{$bordercolor});
          } elsif ($dotshape =~ /^crosses$/)	{
          } elsif ($dotshape =~ /^diamonds$/)	{
            my $poly = new GD::Polygon;
            $poly->addPt($x1,$y1+($dotsize*2));
            $poly->addPt($x1+($dotsize*2),$y1);
            $poly->addPt($x1,$y1-($dotsize*2));
            $poly->addPt($x1-($dotsize*2),$y1);
            $im->polygon($poly,$col{$bordercolor});
          } elsif ($dotshape =~ /^stars$/)	{
            my $poly = new GD::Polygon;
            $poly->addPt($x1,$y1-($dotsize/$C72));
            $poly->addPt($x1+($dotsize*sin(36*$PI/180)),$y1-($dotsize*cos(36*$PI/180)));
            $poly->addPt($x1+($dotsize/$C72*sin(72*$PI/180)),$y1-($dotsize/$C72*cos(72*$PI/180)));
            $poly->addPt($x1+($dotsize*sin(108*$PI/180)),$y1-($dotsize*cos(108*$PI/180)));
            $poly->addPt($x1+($dotsize/$C72*sin(144*$PI/180)),$y1-($dotsize/$C72*cos(144*$PI/180)));
            $poly->addPt($x1+($dotsize*sin(180*$PI/180)),$y1-($dotsize*cos(180*$PI/180)));
            $poly->addPt($x1+($dotsize/$C72*sin(216*$PI/180)),$y1-($dotsize/$C72*cos(216*$PI/180)));
            $poly->addPt($x1+($dotsize*sin(252*$PI/180)),$y1-($dotsize*cos(252*$PI/180)));
            $poly->addPt($x1+($dotsize/$C72*sin(288*$PI/180)),$y1-($dotsize/$C72*cos(288*$PI/180)));
            $poly->addPt($x1+($dotsize*sin(324*$PI/180)),$y1-($dotsize*cos(324*$PI/180)));
         #   $im->polygon($poly,$col{$bordercolor});
            $im->polygon($poly,$col{'borderblack'});
          } elsif ($dotshape =~ /^triangles$/)	{
            my $poly = new GD::Polygon;
            $poly->addPt($x1+($dotsize*2),$y1+($dotsize*2*sin(60*$PI/180)));
            $poly->addPt($x1,$y1-($dotsize*2*sin(60*$PI/180)));
            $poly->addPt($x1-($dotsize*2),$y1+($dotsize*2*sin(60*$PI/180)));
            $im->polygon($poly,$col{$bordercolor});
          } else	{
            $im->rectangle($x1-($dotsize*1.5),$y1-($dotsize*1.5),$x1+($dotsize*1.5),$y1+($dotsize*1.5),$col{$bordercolor});
          }
        }
      }
    }
  }

  binmode STDOUT;
  print MAPGIF $im->gif;
  close MAPGIF;
  chmod 0664, "$GIF_DIR/$gifname";

  print "</map>\n";
  print "</table>\n";
  print "<table cellpadding=10 width=100%>\n";
  print "<tr><td align=center><img border=\"0\" alt=\"PBDB map\" height=\"$height\" width=\"$width\" src=\"$GIF_HTTP_ADDR/$gifname\" usemap=\"#PBDBmap\" ismap>\n\n";
  print "</table>\n";

  print "<center>\n<table><tr>\n";
  if ($matches > 1)	{
    print "<td class=\"large\"><b>$matches collections ";
  }
  elsif ($matches == 1)	{
    print "<td class=\"large\"><b>Exactly one collection ";
  }
  else	{
    # PM 09/13/02 Added bit about missing lat/long data to message
    print "<td class=\"large\"><b>Sorry! Either the collections were missing lat/long data, or no collections ";
  }
  print "matched your query";
  if ($searchstring ne "")	{
    $searchstring =~ s/_/ /g;
    print " \"<i>$searchstring</i>\"";
  }
  print "</b></td></tr></table>\n";
  if ($dotsizeterm eq "proportional")	{
    print "<br>Sizes of $dotshape are proportional to counts of collections at each point.\n"
  }

  print "</font><p>\n<b><a href='?action=displayMapForm'>Search&nbsp;again</a></b>\n<p>\n";

  print "</center></body>\n</html>\n";

}

sub getCoords	{
	my $self = shift;

	my ($x,$y) = @_;
	if ($coll{'lngdir'} =~ /W/)	{
		$x = $x * -1;
	}
	if ($coll{'latdir'} =~ /S/)	{
		$y = $y * -1;
	}
	($x,$y) = $self->projectPoints($x,$y);
	# Get pixel values, but shift everything a half degree so dots
	#  are at midpoints of 1 by 1 deg rectangles
	if ( $x && $y )	{
		return($self->getLng($x - 0.5),$self->getLat($y + 0.5));
	} else	{
		return;
	}
}

sub projectPoints	{
	my $self = shift;

	my ($x,$y) = @_;

	# rotate point if origin is not at 0/0
	if ( $midlat != 0 || $midlng != 0 )	{

	# recenter the longitude on the new origin
		$x = $x - $midlng;
		if ( $x <= -180 )	{
			$x = $x + 360;
		} elsif ( $x >= 180 )	{
			$x = $x - 360;
		}

	# find the great circle distance to the new origin
		my $gcd = ( 180 / $PI ) * acos( ( sin($y*$PI/180) * sin($midlat*$PI/180) ) + ( cos($y*$PI/180) * cos($midlat*$PI/180) * cos($x*$PI/180) ) );

	# find the great circle distance to the point opposite the new pole
		my $oppgcd;
		if ( $x > 0 )	{
			$oppgcd = ( 180 / $PI ) * acos( ( sin($y*$PI/180) * sin($midlat*$PI/-180) ) + ( cos($y*$PI/180) * cos($midlat*$PI/-180) * cos((180-$x)*$PI/180) ) );
		} else	{
			$oppgcd = ( 180 / $PI ) * acos( ( sin($y*$PI/180) * sin($midlat*$PI/-180) ) + ( cos($y*$PI/180) * cos($midlat*$PI/-180) * cos((180+$x)*$PI/180) ) );
		}

	# find the great circle distance to the new north pole
		my $npgcd;
		if ( $midlat <= 0 )	{ # pole is at same longitude as origin
			$npgcd = ( 180 / $PI ) * acos( ( sin($y*$PI/180) * sin(($midlat+90)*$PI/180) ) + ( cos($y*$PI/180) * cos(($midlat+90)*$PI/180) * cos($x*$PI/180) ) );
		} else	{ # pole is at opposite longitude
			$npgcd = ( 180 / $PI ) * acos( ( sin($y*$PI/180) * sin((90-$midlat)*$PI/180) ) + ( cos($y*$PI/180) * cos((90-$midlat)*$PI/180) * cos((180-$x)*$PI/180) ) );
		}

	# now finally shift the point's coordinate relative to the new origin

	# find new latitude exploiting fact that great circle distance from
	#  point to the new north pole must be 90 - latitude

		$y = 90 - $npgcd;

	# find new longitude

		if ( abs($x) > 0.005 && abs($x) < 179.999 )	{
			if ( $gcd > 90 )	{
				if ( abs($y) +0.001 >= $oppgcd && abs($y) - 0.001 <= $oppgcd )	{
					$oppgcd = $oppgcd + 0.001;
				}
				if ( $x > 0 )	{
					$x = 180 - ( 180 / $PI * acos( cos($oppgcd * $PI / 180) / cos($y * $PI / 180) ) );
				} else	{
					$x = -180 + ( 180 / $PI * acos( cos($oppgcd * $PI / 180) / cos($y * $PI / 180) ) );
				}
			} else	{
				if ( abs($y) +0.001 >= $gcd && abs($y) - 0.001 <= $gcd )	{
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
		#  function due to rounding error
			$x = "";
			$y = "";
		}
	} # end of rotation algorithm

	if ( $q->param('projection') eq "orthographic" && $x ne "" )	{

		# how far is this point from the origin?
		my $dist = ($x**2 + $y**2)**0.5;
		# dark side of the Earth is invisible!
		if ( $dist > 90 )	{
			return;
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
	} elsif ( $q->param('projection') eq "Mollweide" && $x ne "")	{
	# WARNING: this is an approximation of the Mollweide projection;
	#  a factor of 180 deg for the longitude would seem intuitive,
	#  but 190 gives a better visual match to assorted Google images
		$x = $x * cos($y * $PI / 190);
		$y = $y * cos($y * $PI / 360);
	} elsif ( $q->param('projection') eq "Eckert" && $x ne "")	{
	# WARNING: this is an approximation of the Eckert IV projection
	#  and is not nearly as complicated
		$x = $x * cos($y * $PI / 300);
		$y = $y * cos($y * $PI / 360);
	}
	return($x,$y);
}

sub getLng	{
	my $self = shift;

	my $l = $_[0];
	$l = (180 + $l - $offlng - $gifoffhor) * $hmult * $scale;
	return $l;
}

sub getLat	{
	my $self = shift;

	my $l = $_[0];
	$l = (90 - $l - $offlat - $gifoffver) * $vmult * $scale;
	return $l;
}

# This is only shown for internal errors
sub htmlError {
    my $self = shift;
    my $message = shift;

    print $message;
    exit 1;
}

sub dbg {
	my $self = shift;
	my $message = shift;

	if ( $DEBUG && $message ) { print "<font color='green'>$message</font><BR>\n"; }

	return $DEBUG;					# Either way, return the current DEBUG value
}


1;
