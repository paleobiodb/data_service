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
			'period',
			'epoch',
			'stage', 
			'formation', 
			'lithology', 
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
		if ($filledfields{$s} ne "")	{
			$searchstring .= "$curField = $filledfields{$s}, ";
		}
	}
	$searchstring =~ s/, $//;

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
			elsif ($t eq "lithology")	{
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
  if ($cont ne "global")	{
    $scale = $q->param('mapscale');
    $scale =~ s/x //i;
    if ($cont =~ /Africa/)	{
      $midlng = 35;
      $midlat = -10;
    }
    elsif ($cont =~ /Asia \(north\)/)	{
      $midlng = 100;
      $midlat = -50;
    }
    elsif ($cont =~ /Asia \(south\)/)	{
      $midlng = 100;
      $midlat = -20;
    }
    elsif ($cont =~ /Australia/)	{
      $midlng = 135;
      $midlat = 28;
    }
    elsif ($cont =~ /Europe/)	{
      $midlng = 10;
      $midlat = -50;
    }
    if ($cont =~ /North America/)	{
      $midlng = -100;
      $midlat = -35;
    }
    elsif ($cont =~ /South America/)	{
      $midlng = -50;
      $midlat = 10;
    }
    $midlng = $midlng + 180;
    $midlat = $midlat + 90;
    # NOTE: shouldn't these be module globals??
    $offlng = $midlng - (180 / $scale);
    $offlat = $midlat - (90 / $scale);
  }
}

sub mapDefineOutlines	{
	my $self = shift;

	if ( ! open COAST,"<$COAST_DIR/noaa.coastlines" ) {
		$self->htmlError ( "Couldn't open [$COAST_DIR/noaa.coastlines]: $!" );
	}
	while (<COAST>)	{
		($a,$b) = split /\t/,$_;
		if ($a ne "")	{
			push @worldlng,$a;
			push @worldlat,$b;
		}
	}
	close COAST;
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
  if ($cont =~ /Africa/ || $cont =~ /South America/)	{
    $hpix = 280;
    $vpix = 240;
  }
  # PM 09/10/02 - Draw a half-sized map for the taxon information script.
  if($q->param("taxon_info_script") eq "yes"){
	$hmult = 0.8;
	$vmult = 1.0;
  }
  $height = $vmult * $vpix;
  $width = $hmult * $hpix;
  $im = new GD::Image($width,$height);

  $col{'white'} = $im->colorAllocate(255,255,255);
  $col{'black'} = $im->colorAllocate(0,0,0);
  $col{'gray'} = $im->colorAllocate(127,127,127);
  $col{'lightgray'} = $im->colorAllocate(191,191,191);
  $col{'red'} = $im->colorAllocate(255,0,0);
  $col{'darkred'} = $im->colorAllocate(127,0,0);
  $col{'yellow'} = $im->colorAllocate(255,255,0);
  $col{'green'} = $im->colorAllocate(0,255,0);
  $col{'orange'} = $im->colorAllocate(255,127,0);
  $col{'violet'} = $im->colorAllocate(191,0,255);
  $col{'darkviolet'} = $im->colorAllocate(127,0,127);
  $col{'darkblue'} = $im->colorAllocate(0,0,255);
  $col{'blue'} = $im->colorAllocate(63,63,255);
  $col{'teal'} = $im->colorAllocate(0,255,255);

  $dotcolor = $q->param('dotcolor');
  $bordercolor = $dotcolor;
  if ($q->param('dotborder') eq "with")	{
	if($q->param('mapbgcolor') eq "black"){
		$bordercolor='white';
	}
	else{
		$bordercolor = 'black';
	}
  }
  ($dotsizeterm,$dotshape) = split / /,$q->param('pointshape');
  if ($dotsizeterm eq "small")	{
    $dotsize = 2;
  }
  elsif ($dotsizeterm eq "medium")	{
    $dotsize = 3;
  }
  elsif ($dotsizeterm eq "large")	{
    $dotsize = 4;
  }
  $maxdotsize = $dotsize;
  if ($dotsizeterm eq "proportional")	{
    $maxdotsize = 7;
  }


	# create an interlaced GIF with a white background
	$im->interlaced('true');
	if ( $q->param('mapbgcolor') ne "transparent" )	{
		$im->fill(100,100,$col{$q->param('mapbgcolor')});
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

 # draw grids
  $grids = $q->param('gridsize');
  if ($grids > 0)	{
    for $lat (1..int($vpix/$grids)-1)	{
      $color = $col{$q->param('gridcolor')};
      if ($lat * $grids == 90)	{
        $color = $col{'gray'};
      }
      $im->line(0,$scale*$vmult*$lat*$grids,$scale*$hmult*$hpix,$scale*$vmult*$lat*$grids,$color);
    }
    for $lng (1..int($hpix/$grids)-1)	{
      $color = $col{$q->param('gridcolor')};
      if ($lng * $grids == 180)	{
        $color = $col{'gray'};
      }
      $im->line($scale*$hmult*$lng*$grids,0,$scale*$hmult*$lng*$grids,$scale*$vmult*$vpix,$color);
    }
  }

 # draw coastlines
  for $c (0..$#worldlat-1)	{
    if ($worldlat[$c] ne "" && $worldlat[$c+1] ne "")	{
      $im->line( $self->getLng($worldlng[$c]),$self->getLat($worldlat[$c]),$self->getLng($worldlng[$c+1]),$self->getLat($worldlat[$c+1]),$col{$q->param('coastlinecolor')});
    }
  }

  # draw collection data points
  foreach $collRef ( @dataRows ) {
    %coll = %{$collRef};
    if ( ( $coll{'latdeg'} || $coll{'latmin'} || $coll{'latdec'} ) &&
         ( $coll{'lngdeg'} || $coll{'lngmin'} || $coll{'lngdec'} ) && 
			( $collok{$coll{'collection_no'}} eq "Y" || 
			! $q->param('genus_name') ) 
		) {

      ($x1,$y1) = $self->getCoords($coll{'lngdeg'},$coll{'latdeg'});
      if ( $x1-$maxdotsize > 0 && 
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
			printf "%d,%d,%d,%d", int($x1-$dotsize), int($y1-$dotsize), int($x1+$dotsize), int($y1+$dotsize);
			print "\" href=\"$BRIDGE_HOME?action=displayCollResults";
			for $t (keys %filledfields)	{
				if ($filledfields{$t} ne "")	{
					my $temp = $filledfields{$t};
					$temp =~ s/"//g;
					$temp =~ s/ /\+/g;
					print "&$t=$temp";

					# HACK: force search to use wildcards if lithology or formation is searched, in order
					#  to avoid problems with values that include double quotes
					if ($t =~ /(formation)|(lithology)/)	{
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

    # draw a circle
        if ($dotshape =~ /circles/)	{
          $im->arc($x1,$y1,($dotsize*2)+2,($dotsize*2)+2,0,360,$col{$bordercolor});
        }
    # or draw a square
        else	{
          $im->rectangle($x1-$dotsize,$y1-$dotsize,$x1+$dotsize,$y1+$dotsize,$col{$bordercolor});
        }
        $im->fill($x1+1,$y1+1,$col{$dotcolor});
        $im->fill($x1+1,$y1-1,$col{$dotcolor});
        $im->fill($x1-1,$y1+1,$col{$dotcolor});
        $im->fill($x1-1,$y1-1,$col{$dotcolor});
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
          if ($dotshape =~ /circles/)	{
            $im->arc($x1,$y1,($dotsize*2)+2,($dotsize*2)+2,0,$hpix,$col{$bordercolor});
          }
          else	{
            $im->rectangle($x1-$dotsize,$y1-$dotsize,$x1+$dotsize,$y1+$dotsize,$col{$bordercolor});
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
	# Get pixel values, but shift everything a half degree so dots
	#  are at midpoints of 1 by 1 deg rectangles
	return($self->getLng($x - 0.5),$self->getLat($y + 0.5));
}

sub getLng	{
	my $self = shift;

	my $l = $_[0];
	$l = (180 + $l - $offlng) * $hmult * $scale;
	return $l;
}

sub getLat	{
	my $self = shift;

	my $l = $_[0];
	$l = (90 - $l - $offlat) * $vmult * $scale;
	return $l;
}

# This only shown for internal errors
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
