package Map;

use Debug;
use GD;
use Class::Date qw(date localdate gmdate now);
use Image::Magick;
use TimeLookup;
use Globals;

# Flags and constants
my $DEBUG = 1;			# The debug level of the calling program
my $dbh;				# The database handle
my $dbt;				# The DBTransactionManager object
my $q;					# Reference to the parameters
my $s;					# Reference to the session
my $sql;				# Any SQL string
my $rs;					# Generic recordset

my $BRIDGE_HOME = "/cgi-bin/bridge.pl";
my $GIF_HTTP_ADDR = "/public/maps";               # For maps
my $COAST_DIR = $ENV{MAP_COAST_DIR};        # For maps
my $GIF_DIR = $ENV{MAP_GIF_DIR};      # For maps
my $PI = 3.14159265;
my $C72 = cos(72 * $PI / 180);
my $AILEFT = 100;
my $AITOP = 580;
my @coll_counts = ();

sub acos { atan2( sqrt(1 - $_[0] * $_[0]), $_[0] ) }
sub tan { sin($_[0]) / cos($_[0]) }

# returns great circle distance given two latitudes and a longitudinal offset
sub GCD { ( 180 / $PI ) * acos( ( sin($_[0]*$PI/180) * sin($_[1]*$PI/180) ) + ( cos($_[0]*$PI/180) * cos($_[1]*$PI/180) * cos($_[2]*$PI/180) ) ) }

sub new {
	my $class = shift;
	$dbh = shift;
	$q = shift;
	$s = shift;
	$dbt = shift;
	my $self = {maptime=>0,plate=>()};

	bless $self, $class;

	return $self;
}



# added 12/11/2003 to allow Taxoninfo to set the center coordinates for the map
# *after* the map object has been created.
sub setQAndUpdateScale {
	my $self = shift;
	$q = shift;
	
	$self->mapGetScale;  # to update this value..  This is a bad way to do it,
	# so come up with a better way later..
}

sub buildMap {
	Debug::dbPrint("Made it to buildMap");
	
	my $self = shift;

	$|=1;					# Freeflowing data

	$self->{'maptime'} = $q->param('maptime');
	if ( $self->{'maptime'} eq "" )	{
		$self->{'maptime'} = 0;
	}

	$self->mapGetScale();
	$self->mapDefineOutlines();
	if ( $self->{'maptime'} > 0 )	{
		$self->mapGetRotations();
	}


    %fieldnames = ( "research group" => "research_group",
		            "state/province" => "state",
		            "time interval" => "interval_name",
		            "lithology" => "lithology1",
		            "paleoenvironment" => "environment",
		            "taxon" => "genus_name" );

    $img_link = $self->mapSetupImage();
    for my $ptset (1..4) {
        $dotsizeterm = $q->param("pointsize$ptset");
        $dotshape = $q->param("pointshape$ptset");
        $dotcolor = $q->param("dotcolor$ptset");
        $bordercolor = $dotcolor;

        if ($q->param("dotborder$ptset") ne "no" )	{
            if($q->param('mapbgcolor') eq "black" || $q->param("dotborder$ptset") eq "white" )	{
                $bordercolor = "white";
            } else {
                $bordercolor = "borderblack";
            }
        }

        if ($dotsizeterm eq "tiny")	{
            $dotsize = 1;
        } elsif ($dotsizeterm eq "small")	{
            $dotsize = 1.5;
        } elsif ($dotsizeterm eq "medium")	{
            $dotsize = 2;
        } elsif ($dotsizeterm eq "large")	{
            $dotsize = 3;
        } elsif ($dotsizeterm eq "huge")	{
            $dotsize = 4;
        }
        $maxdotsize = $dotsize;
        if ($dotsizeterm eq "proportional")	{
          $maxdotsize = 7;
        }

        if ($ptset > 1) {
            $extraField = $fieldnames{$q->param('mapsearchfields'.$ptset)} 
                                        || 
                          $q->param('mapsearchfields'.$ptset);
            $extraFieldValue = $q->param('mapsearchterm'.$ptset);

            if ($extraField && $extraFieldValue) {
                $q->param($extraField => $extraFieldValue);
                my $dataRowsRef = $self->mapQueryDb();
                $self->mapDrawPoints($dataRowsRef);
            }
        } elsif ($ptset == 1) {
            my $dataRowsRef = $self->mapQueryDb();
            $self->mapDrawPoints($dataRowsRef);
        }
           
    }
    $self->mapFinishImage();

    return $img_link;
}

#this is the complement to buildMapOnly, used in TaxonInfo
sub drawMapOnly {
    my $self = shift; 
    my $dataRowsRef = shift;

    $img_link = $self->mapSetupImage();

    $dotsizeterm = $q->param("pointsize1") || "tiny";
    $dotshape = $q->param("pointshape1") || "circles";
    $dotcolor = $q->param("dotcolor1") || "black";
    $bordercolor = $dotcolor;

    if ($q->param("dotborder1") ne "no")	{
        if($q->param('mapbgcolor') eq "black" || $q->param("dotborder1") eq "white" )	{
            $bordercolor = "white";
        } else {
            $bordercolor = "borderblack";
        }
    }

    if ($dotsizeterm eq "tiny")	{ $dotsize = 1; } 
    elsif ($dotsizeterm eq "small")	{ $dotsize = 1.5; }
    elsif ($dotsizeterm eq "medium") { $dotsize = 2;} 
    elsif ($dotsizeterm eq "large") {$dotsize = 3;} 
    elsif ($dotsizeterm eq "huge")	{$dotsize = 4;}
    $maxdotsize = $dotsize;
    if ($dotsizeterm eq "proportional")	{ $maxdotsize = 7; }

    $self->mapDrawPoints($dataRowsRef);
    $self->mapFinishImage();
    return $img_link;
}    

# same as buildMap, but doesn't call mapDrawPoints/mapSetupImage/mapFinishImage, and returns dataRows.
sub buildMapOnly {
	my $self = shift;
	my $in_list = (shift or "");

	$|=1;					# Freeflowing data

	$self->{maptime} = $q->param('maptime');
	if ( $self->{maptime} eq "" )	{
		$self->{maptime} = 0;
	}

	$self->mapGetScale();
	$self->mapDefineOutlines();
	if ( $self->{maptime} > 0 )	{
		$self->mapGetRotations();
	}

    # Returns a reference to array returned from permissions
	return $self->mapQueryDb($in_list);
}

# This function prints footer for the image, makes clickable background tiles,
# converts and outputs the image to different formats, and closes up everything
sub mapFinishImage {
    my $self = shift;

    $im->arc(97,$height+6,10,10,0,360,$col{'black'});
    $im->string(gdTinyFont,5,$height+1,"plotting software c 2002 J. Alroy",$col{'black'});
    print AI "0 To\n";
    printf AI "1 0 0 1 %.1f %.1f 0 Tp\nTP\n",$AILEFT+5,$AITOP-$height-8;
    my $mycolor = $aicol{'black'};
    $mycolor =~ s/ K/ k/;
    printf AI "0 Tr\n0 O\n%s\n",$mycolor;
    print AI "/_Courier 10 Tf\n";
    printf AI "0 Tw\n";
    print AI "(plotting software c 2002 J. Alroy) Tx 1 0 Tk\nTO\n";
    print AI "0 To\n";
    printf AI "1 0 0 1 %.1f %.1f 0 Tp\nTP\n",$AILEFT+111,$AITOP-$height-10;
    print AI "/_Courier 18 Tf\n";
    print AI "(o) Tx 1 0 Tk\nTO\n";
    if ( $self->{maptime} > 0 )	{
        if ( $width > 300 )	{
            $im->arc($width-103,$height+6,10,10,0,360,$col{'black'});
            $im->string(gdTinyFont,$width-225,$height+1,"tectonic reconstruction c 2002 C. R. Scotese",$col{'black'});
        } else	{
            $im->arc($width-103,$height+18,10,10,0,360,$col{'black'});
            $im->string(gdTinyFont,$width-225,$height+13,"tectonic reconstruction c 2002 C. R. Scotese",$col{'black'});
            $scoteseoffset = 12;
        }
        print AI "0 To\n";
        printf AI "1 0 0 1 %.1f %.1f 0 Tp\nTP\n",$AILEFT+$width-270,$AITOP-$height-8-$scoteseoffset;
        print AI "/_Courier 10 Tf\n";
        print AI "(tectonic reconstruction c 2002 C. R. Scotese) Tx 1 0 Tk\nTO\n";
        print AI "0 To\n";
        printf AI "1 0 0 1 %.1f %.1f 0 Tp\nTP\n",$AILEFT+$width-128,$AITOP-$height-10-$scoteseoffset;
        print AI "/_Courier 18 Tf\n";
        print AI "(o) Tx 1 0 Tk\nTO\n";
    }

    # cap the byline with a horizontal line
    $im->line(0,$height,$width,$height,$col{'black'});
    print AI "u\n";  # start the group
    print AI "$mycolor\n";
    printf AI "0.5 w\n";
    printf AI "%.1f %.1f m\n",$AILEFT,$AITOP-$height;
    printf AI "%.1f %.1f l\n",$AILEFT+$width,$AITOP-$height;
    print AI "S\n";
    print AI "U\n";  # terminate the group

    binmode STDOUT;
    print MAPGIF $im->png;
    close MAPGIF;
    chmod 0664, "$GIF_DIR/$gifname";

    my $image = Image::Magick->new;

    open GIF,"<$GIF_DIR/$gifname";
    $image->Read(file=>\*GIF);

    open AIFOOT,"<./data/AI.footer";
    while (<AIFOOT>){
        print AI $_;
    }
    close AIFOOT;

    open JPG,">$GIF_DIR/$jpgname";
    $image->Write(file=>JPG);
    chmod 0664, "$GIF_DIR/$jpgname";

    open PICT,">$GIF_DIR/$pictname";
    $image->Write(file=>PICT);
    chmod 0664, "$GIF_DIR/$pictname";

    close GIF;

    # make clickable background rectangles for repositioning the map

    # need a list of possible parameters
    my @params = ('research_group', 'authorizer', 'enterer', 'modified_since', 'date', 'month', 'year', 'country', 'state', 'interval_name', 'formation', 'lithology1', 'environment', 'taxon_rank', 'genus_name', 'pointsize1', 'dotcolor1', 'pointshape1', 'dotborder1', 'mapsearchfields2', 'mapsearchterm2', 'pointsize2', 'dotcolor2', 'pointshape2', 'dotborder2', 'mapsearchfields3', 'mapsearchterm3', 'pointsize3', 'dotcolor3', 'pointshape3', 'dotborder3', 'mapsearchfields4', 'mapsearchterm4', 'pointsize4', 'dotcolor4', 'pointshape4', 'dotborder4', 'mapsize', 'projection', 'maptime', 'mapfocus', 'mapresolution', 'mapbgcolor', 'crustcolor', 'gridsize', 'gridcolor', 'gridposition', 'linethickness', 'latlngnocolor', 'coastlinecolor', 'borderlinecolor', 'usalinecolor');

    my $clickstring = "$BRIDGE_HOME?action=displayMapResults";
    for $p ( @params )	{
        if ( $q->param($p) )	{
            $clickstring .= "&" . $p . "=" . $q->param($p);
        }
    }
    for my $i ( 1..10 )	{
        for my $j ( 1..10 )	{
            my $xbot = int(( $i - 1 ) / 10 * $width);
            my $xtop = int($i / 10 * $width);
            my $ybot = int(( $j - 1 ) / 10 * $height);
            my $ytop = int($j / 10 * $height);
            $newlng = int($midlng + ( ( 360 / $scale ) * ( $i - 5 ) / 10 ));
            $newlat = int($midlat - ( ( 180 / $scale ) * ( $j - 5 ) / 10 ));
            $latlngstring = "&maplng=" . $newlng;
            $latlngstring .= "&maplat=" . $newlat;
            # need this because mapscale is varied for the "Zoom"
            #  buttons below
            $latlngstring .= "&mapscale=" . $scale;
            print MAPOUT "<area shape=\"rect\" coords=\"" . $xbot . "," . $ybot . "," . $xtop . "," . $ytop . "\" href=\"" , $clickstring , $latlngstring , "\">\n";
        }
    }

    print MAPOUT "</map>\n";
    print MAPOUT "</table>\n";

    print MAPOUT "<table cellpadding=0 width=100%>\n<tr>\n";
    print MAPOUT "<td valign=\"middle\">\n";
    print MAPOUT "<table cellpadding=0 cellspacing=1><tr>\n<td align=\"right\" valign=\"top\" bgcolor=\"black\">\n";
    print MAPOUT "<table cellpadding=5 cellspacing=1>\n";
    if(!$q->param("taxon_info_script")){
        print MAPOUT "<tr><td width=110 valign=\"top\" bgcolor=\"white\" class=\"small\">";
        if ($coll_counts[0] > 1)	{
            print MAPOUT "<b>$coll_counts[0]&nbsp;collections</b> fall ";
        } elsif ($coll_counts[0] == 1)	{
            print MAPOUT "<b>Exactly&nbsp;one collection</b> falls ";
        }  else	{
            # PM 09/13/02 Added bit about missing lat/long data to message
            print MAPOUT "<b>Sorry!</b> Either the collections were missing lat/long data, or no collections fall ";
        }
        print MAPOUT "within the mapped area, have lat/long data, and matched your query";
        print MAPOUT "</td>\n";
    }

    if ($dotsizeterm eq "proportional")	{
        print MAPOUT "<tr><td width=100 valign=\"top\" bgcolor=\"white\" class=\"small\">";
        print MAPOUT "<br>Sizes of $dotshape are proportional to counts of collections at each point.\n"
    }

    print MAPOUT "<tr><td width=100 valign=\"top\" bgcolor=\"white\" class=\"small\">";
    print MAPOUT "You may download the image in ";
    print MAPOUT "<b><a href=\"$GIF_HTTP_ADDR/$ainame\">Adobe Illustrator</a></b>, ";
    print MAPOUT "<b><a href=\"$GIF_HTTP_ADDR/$gifname\">PNG</a></b>, ";
    print MAPOUT "<b><a href=\"$GIF_HTTP_ADDR/$jpgname\">JPEG</a></b>, ";
    print MAPOUT "or <b><a href=\"$GIF_HTTP_ADDR/$pictname\">PICT</a></b> format\n";
    print MAPOUT "</td></tr>\n";

    print MAPOUT "<tr><td width=100 valign=\"top\" bgcolor=\"white\" class=\"small\">";
    print MAPOUT "Click on a point to recenter the map\n";
    print MAPOUT "</td></tr>\n";

    unless($q->param("taxon_info_script") eq "yes"){
        $clickstring .= "&maplng=" . $midlng;
        $clickstring .= "&maplat=" . $midlat;

        $zoom1 = 2;
        while ( $scale + $zoom1 > 12 )	{
            $zoom1--;
        }
        $zoom2 = 2;
        while ( $scale - $zoom2 < 1 )	{
            $zoom2--;
        }

        print MAPOUT "<tr><td width=100 align=\"center\" valign=\"top\" bgcolor=\"white\" class=\"large\">";
        $temp = $clickstring . "&mapscale=" . ( $scale + $zoom1 );
        print MAPOUT "<p class=\"large\"><b><a href=\"$temp\">Zoom&nbsp;in</a></b></p>\n";
        print MAPOUT "</td></tr>\n";

        print MAPOUT "<tr><td width=100 align=\"center\" valign=\"top\" bgcolor=\"white\" class=\"large\">";
        $temp = $clickstring . "&mapscale=" . ( $scale - $zoom2 );
        print MAPOUT "<p class=\"large\"><b><a href=\"$temp\">Zoom&nbsp;out</a></b></p>\n";
        print MAPOUT "</td></tr>\n";

        print MAPOUT "<tr><td width=100 align=\"center\" valign=\"top\" bgcolor=\"white\" class=\"large\">";
        print MAPOUT "<p class=\"large\"><b><a href='?action=displayMapForm'>Search&nbsp;again</a></b></p>\n";
        print MAPOUT "</td></tr>\n";
    }
    print MAPOUT "</tr></table>\n";
    print MAPOUT "</td></tr></table>\n";
    print MAPOUT "</td>\n";

    print MAPOUT "<td align=center><img border=\"0\" alt=\"PBDB map\" height=\"$totalheight\" width=\"$width\" src=\"$GIF_HTTP_ADDR/$gifname\" usemap=\"#PBDBmap\" ismap>\n\n";
    print MAPOUT "</table>\n";

    print MAPOUT "</center>";
    close MAPOUT;
}




sub mapQueryDb	{
	my $self = shift;
	my $in_list = (shift or "");

	# if a research project (not a group) is requested, get a list of
    #  references included in that project JA 3.10.02
    my $reflist;
    if ( $q->param('research_group') =~ /(^decapod$)|(^ETE$)|(^5%$)|(^1%$)|(^PACED$)|(^PGAP$)/ )	{
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

    # make a list of collections falling in a time interval
    # JA 20.7.03
    my $intlist;
    if ( $q->param('interval_name') =~ /[A-Za-z]/ )	{
        my ($collref,$bestbothscale) = TimeLookup::processLookup($dbh, $dbt, '', $q->param('interval_name'), '', '');
        my @colls = @{$collref};
        $intlist = join ',', @colls;
    }

    # query the occurrences table to get a list of useable collections
    # Also handles species name searches JA 19-20.8.02
    my $genus;
    my $species;
    if($q->param('genus_name')){
        # PM 09/13/02 added the '\s+' pattern for space matching.
        if($q->param('genus_name') =~ /\w+\s+\w+/){
            ($genus,$species) = split /\s+/,$q->param('genus_name');
        }
        elsif($q->param('taxon_rank') eq "species"){
            $species = $q->param('genus_name');
        }
        else{
            $genus = $q->param('genus_name');
        }
        $sql = qq|SELECT collection_no FROM occurrences WHERE |;
        if($q->param('taxon_rank') eq "Higher taxon" ||
                $q->param('taxon_rank') eq "Higher-taxon"){
            $self->dbg("genus_name q param:".$q->param('genus_name')."<br>");
            $sql .= "genus_name IN (";
            if($in_list eq ""){
                $self->dbg("RE-RUNNING TAXONOMIC SEARCH in Map.pm<br>");
                # JA: replaced recurse call with taxonomic_search call
                #  7.5.04 because I am not maintaining recurse
                $in_list=PBDBUtil::taxonomic_search($q->param('genus_name'),$dbt);
            }
            $sql .= $in_list . ")";
        }
        else{
            if($genus){
                $sql .= "genus_name='" . $genus;
                if($species){
                    $sql .= "' AND ";
                }
            }
            if($species){
            $sql .= "species_name='" . $species;
            }
            $sql .= "'";
        }
        $sth2 = $dbh->prepare($sql);
        # DEBUG: PM 09/13/02
        $self->dbg("mapQueryDb sql: $sql<br>");
        $sth2->execute();
        # DEBUG: PM 09/10/02
        #$self->dbg("results from collection_no search in db: <br>");
        %collok = ();
        while (my $occRef = $sth2->fetchrow_hashref())	{
            my %occ = %{$occRef};
            if ($occ{'collection_no'} ne ""){
                #$self->dbg($occ{'collection_no'});
                $collok{$occ{'collection_no'}} = "Y";
                #print "<!-- $occ{'collection_no'} -->\n";
            }
        }
        $sth2->finish();
    }

    # figure out what collection table values are being matched 
    # in what fields
    @allfields = (	'research_group',
            'enterer',
            'authorizer',
            'country',
            'state',
            'time interval',
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
        my ($a,$b,$c);
        ($a,$b) = split / /,$nowDate;
        ($a,$b,$c) = split /-/,$a,3;
        $q->param('year' => $a);
        $q->param('month' => $b);
        $q->param('date' => $c);
    }
    if ($q->param('year'))	{
        $q->param('modified_since' => $q->param('year')." ".$q->param('month')." ".$q->param('date'));
    }

    # Clean up the environment field
    my $environment = $q->param('environment');
    if ( $environment =~ /General/ )	{
        $q->param(environment => '');
    } elsif ( $environment =~ /Carbonate/ )	{
        $q->param(environment => 'carbonate indet.');
    } elsif ( $environment =~ /Siliciclastic/ )	{
        $q->param(environment => 'marginal marine indet.');
    } elsif ( $environment =~ /Terrestrial/ )	{
        $q->param(environment => 'fluvial-lacustrine indet.');
    }

	for my $field ( @allfields ) {
		if ( $q->param($field) && $q->param( $field ) ne "all")	{
			$filledfields{$field} = $q->param ( $field );
		}
	}

	# Start the SQL 
	$sql = qq|SELECT *, DATE_FORMAT(release_date, '%Y%m%d') rd_short  FROM collections |;

	# PM 09/10/02. Specify a default WHERE clause of the collection
	# numbers returned above. This drastically reduces database load for
	# the map drawing section of the taxon information script.
	my $collection_no_list = join(", ",keys(%collok));
	my $where = "";
	# %collok is only populated if $q->param("genus_name") was provided
	if($collection_no_list ne ""){
		$where = "WHERE collection_no IN($collection_no_list) ";
	}

	# handle time interval JA 20.7.03
	if ( $q->param('interval_name') =~ /[A-Za-z]/ )	{
		$where = &::buildWhere ( $where, " ( collection_no IN (" . $intlist . ") )" );
	}

	for $t (keys %filledfields)	{
		if ( $filledfields{$t} )	{
			# handle stage
			if ($t eq "interval_name")	{
                my ($inlistref,$bestbothscale) = TimeLookup::processLookup($dbh,$dbt,'',$filledfields{$t},'','','intervals');
                @intervals = @{$inlistref};
                if (@intervals) {
				    $where = &::buildWhere ( $where, qq| max_interval_no IN (|.join(",",@intervals).qq|)| );
                }
			}
			# handle lithology
			elsif ($t eq "lithology1")	{
				$where = &::buildWhere ( $where, qq| (lithology1='$filledfields{$t}' OR lithology2='$filledfields{$t}')| );
			}
			# handle modified date
			elsif ($t eq "modified_since")	{
				my ($yy,$mm,$dd) = split / /,$filledfields{$t},3;
                $filledfields{$t} = $dbh->quote(sprintf("%d-%02d-%02d 00:00:00",$yy,$mm,$dd));
				if ( $q->param("beforeafter") eq "created after" )	{
					$where = &::buildWhere ( $where, qq| created>$filledfields{$t}| );
				} elsif ( $q->param("beforeafter") eq "created before" )	{
					$where = &::buildWhere ( $where, qq| created<$filledfields{$t}| );
				} elsif ( $q->param("beforeafter") eq "modified after" )	{
					$where = &::buildWhere ( $where, qq| modified>$filledfields{$t}| );
				} elsif ( $q->param("beforeafter") eq "modified before" )	{
					$where = &::buildWhere ( $where, qq| modified<$filledfields{$t}| );
				}
			# following written by JA 3.10.02
			} elsif ( $t eq "research_group" && $q->param('research_group') =~ /(^decapod$)|(^ETE$)|(^5%$)|(^1%$)|(^PACED$)|(^PGAP$)/ )	{
				$where .= " AND reference_no IN (" . $reflist . ")";
			} elsif ( $t eq "research_group" ) {
				# research_group is now a set -- tone 7 jun 2002
				$where .= " AND FIND_IN_SET(".$dbh->quote($q->param("research_group")).", research_group)";
			} else {
				$where = &::buildWhere ( $where, qq| $t='$filledfields{$t}'| );
			}
		}
	}

 	if ( $where ) { $sql .= "$where "; }
	$sql =~ s/FROM collections  AND /FROM collections WHERE /;
	$sql =~ s/\s+/ /gs;
	$self->dbg ( "Final sql: $sql<br>" );
	# NOTE: Results attached to this statement handle are used in mapDrawPoints
	$sth = $dbh->prepare($sql);
	$sth->execute();

    # Pass collections thru permissions
    my $p = Permissions->new($s);
	my $limit = 1000000;
	my $ofRows = 0;
    my @dataRows = ();

    $p->getReadRows ( $sth, \@dataRows, $limit, \$ofRows );
    $self->dbg ( "Returned $ofRows rows okayed by permissions module" );

    return \@dataRows;	
}


sub mapGetScale	{
	my $self = shift;

	$projection = $q->param('projection');

    $scale = 1;
    $scale = $q->param('mapscale');
    $scale =~ s/x //i;

    ($cont,$coords) = split / \(/,$q->param('mapfocus');
    $coords =~ s/\)//;  # cut off the right parenthesis.
    ($midlat,$midlng) = split /,/,$coords;
    # the user might enter a zero for one value or the other, so just one
    #  non-zero value is needed
    if ( $q->param('maplat') || $q->param('maplng') )	{
        $midlat = $q->param('maplat');
        $midlng = $q->param('maplng');
    }

    # NOTE: shouldn't these be module globals??
    $offlng = 180 * ( $scale - 1 ) / $scale;
    $offlat = 90 * ( $scale - 1 ) / $scale;
}


# extract outlines taken from NOAA's NGDC Coastline Extractor
sub mapDefineOutlines	{
	my $self = shift;

	if ( $q->param('mapresolution') eq "coarse" )	{
		$resostem = "075";
	} elsif ( $q->param('mapresolution') eq "medium" )	{
		$resostem = "050";
	} elsif ( $q->param('mapresolution') eq "fine" )	{
		$resostem = "025";
	} elsif ( $q->param('mapresolution') eq "very fine" )	{
		$resostem = "010";
	}

	# read grid cell ages
	open MASK,"<$COAST_DIR/agev7.txt";
	my $lat = 90;
	while (<MASK>)	{
		s/\n//;
		my @crustages = split /\t/,$_;
		my $lng = -180;
		for $crustage (@crustages)	{
			$cellage{$lng}{$lat} = $crustage;
			if ( $cellage{$lng}{$lat} == 254 )	{
				$cellage{$lng}{$lat} = 999;
			}
			$lng++;
		}
		$lat--;
	}

	if ( ! open COAST,"<$COAST_DIR/noaa.coastlines.$resostem" ) {
		$self->htmlError ( "Couldn't open [$COAST_DIR/noaa.coastlines.$resostem]: $!" );
	}
	while (<COAST>)	{
		s/\n//;
		($a,$b) = split /\t/,$_;
		if ( $a > 0 )	{
			$ia = int($a + 0.5);
		} else	{
			$ia = int($a - 0.5);
		}
		if ( $b > 0 )	{
			$ib = int($b + 0.5);
		} else	{
			$ib = int($b - 0.5);
		}
		# save data
		# NOTE: separators are saved intentionally so they
		#  can be used for that purpose later on
		if ( $a =~ /#/ || ( $a =~ /[0-9]/ && $cellage{$ia}{$ib} >= $self->{maptime} ) )	{
			push @worldlng,$a;
			push @worldlat,$b;
		}
	}

	if ( $q->param('borderlinecolor') ne "none" )	{
		if ( ! open BORDER,"<$COAST_DIR/noaa.borders.$resostem" ) {
			$self->htmlError ( "Couldn't open [$COAST_DIR/noaa.borders.$resostem]: $!" );
		}
		while (<BORDER>)	{
			s/\n//;
			($a,$b) = split /\t/,$_;
			if ( $a > 0 )	{
				$ia = int($a + 0.5);
			} else	{
				$ia = int($a - 0.5);
			}
			if ( $b > 0 )	{
				$ib = int($b + 0.5);
			} else	{
				$ib = int($b - 0.5);
			}
			if ( $a =~ /#/ || ( $a =~ /[0-9]/ && $cellage{$ia}{$ib} >= $self->{maptime} ) )	{
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
			s/\n//;
			($a,$b) = split /\t/,$_;
			if ( $a > 0 )	{
				$ia = int($a + 0.5);
			} else	{
				$ia = int($a - 0.5);
			}
			if ( $b > 0 )	{
				$ib = int($b + 0.5);
			} else	{
				$ib = int($b - 0.5);
			}
			if ( $a =~ /#/ || ( $a =~ /[0-9]/ && $cellage{$ia}{$ib} >= $self->{maptime} ) )	{
				push @usalng,$a;
				push @usalat,$b;
			}
		}
		close USA;
	}
}

# read Scotese's plate ID and rotation data files
sub mapGetRotations	{
	my $self = shift;

	if ( ! open IDS,"<$COAST_DIR/plateidsv2.lst" ) {
		$self->htmlError ( "Couldn't open [$COAST_DIR/plateidsv2.lst]: $!" );
	}

	# skip the first line
	<IDS>;

	# read the plate IDs: numbers are longitude, latitude, and ID number
	while (<IDS>)	{
		s/\n//;
		my ($x,$y,$z) = split /,/,$_;
		$plate{$x}{$y} = $z;
	}
    $self->{plate} = \%plate;

	if ( ! open ROT,"<$COAST_DIR/master01c.rot" ) {
		$self->htmlError ( "Couldn't open [$COAST_DIR/master01c.rot]: $!" );
	}

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
	if ( $self->{maptime} > 0 && $self->{maptime} < 10 )	{
		for $p (1..999)	{
			$rotx{0}{$p} = $rotx{10}{$p};
			$roty{0}{$p} = $roty{10}{$p};
			$rotdeg{0}{$p} = 0;
		}
	}

    # use world's dumbest linear interpolation to estimate pole of rotation and
    #  angle of rotation values if this time interval is non-standard
	if ( ! $roty{$self->{maptime}}{'1'} )	{

		my $basema = $self->{maptime};
		while ( ! $roty{$basema}{'1'} && $basema >= 0 )	{
			$basema--;
		}
		my $topma = $self->{maptime};
		while ( ! $roty{$topma}{'1'} && $topma < 1000 )	{
			$topma++;
		}

		if ( $topma < 1000 )	{
			$basewgt = ( $topma - $self->{maptime} ) / ( $topma - $basema );
			$topwgt = ( $self->{maptime} - $basema ) / ( $topma - $basema );
			for $pid (1..1000)	{
				my $x1 = $rotx{$basema}{$pid};
				my $x2 = $rotx{$topma}{$pid};
				my $y1 = $roty{$basema}{$pid};
				my $y2 = $roty{$topma}{$pid};
				my $z1 = $rotdeg{$basema}{$pid};
				my $z2 = $rotdeg{$topma}{$pid};

				$rotx{$self->{maptime}}{$pid} = ( $basewgt * $x1 ) + ( $topwgt * $x2 ) ;
				# the amazing "Madagascar 230 Ma" correction
				if ( ( $x1 > 0 && $x2 < 0 ) || ( $x1 < 0 && $x2 > 0 ) )	{
					if ( abs($x1 - $x2) > 180 )	{ # Madagascar case
						$rotx{$self->{maptime}}{$pid} = ( ( 180 - $x1 ) + ( 180 - $x2 ) ) / 2;
					} elsif ( abs($x1 - $x2) > 90 )	{ # Africa plate 701/150 Ma case
						$y2 = -1 * $y2;
						$z2 = -1 * $z2;
						if ( abs($x1) > abs($x2) )	{
							$rotx{$self->{maptime}}{$pid} = ( 180 + $x1 + $x2 ) / 2;
						} else	{
							$rotx{$self->{maptime}}{$pid} = ( 180 - $x1 - $x2 ) / 2;
						}
					}
				}

				$roty{$self->{maptime}}{$pid} = ( $basewgt * $y1 ) + ( $topwgt * $y2 );
				$rotdeg{$self->{maptime}}{$pid} = ( $basewgt * $z1 ) + ( $topwgt * $z2 );
			}
		}
	}

}


sub mapSetupImage {
    my $self = shift;

    # erase all files that haven't been accessed in more than a day
	opendir(DIR,"$GIF_DIR") or die "couldn't open $GIF_DIR ($!)";
	# grab only files with extensions;  not subdirs or . or ..
	my @filenames = grep { /.*?\.(\w+)/ } readdir(DIR);
	closedir(DIR);

	foreach my $file (@filenames){
		if((-A "$GIF_DIR/$file") > 1){
			unlink "$GIF_DIR/$file";
		}
	}


    # get the next number for file creation.
    if ( ! open GIFCOUNT,"<$GIF_DIR/gifcount" ) {
		$self->htmlError ( "Couldn't open [$GIF_DIR/gifcount]: $!" );
    }
    $gifcount = <GIFCOUNT>;
    chomp($gifcount);
    close GIFCOUNT;

    $gifcount++;
    if ( ! open GIFCOUNT,">$GIF_DIR/gifcount" ) {
          $self->htmlError ( "Couldn't open [$GIF_DIR/gifcount]: $!" );
    }
    print GIFCOUNT "$gifcount";
    close GIFCOUNT;

    $gifcount++;

    # set up the filenames
    $gifname = "pbdbmap" . $gifcount . ".png";
    $htmlname = "pbdbmap".$gifcount.".html";
    $ainame = "pbdbmap" . $gifcount . ".ai";
    $jpgname = "pbdbmap" . $gifcount . ".jpg";
    $pictname = "pbdbmap" . $gifcount . ".pict";
    if ( ! open MAPGIF,">$GIF_DIR/$gifname" ) {
          $self->htmlError ( "Couldn't open [$GIF_DIR/$gifname]: $!" );
    }

	# Write this to a file, not stdout
	open(MAPOUT,">$GIF_DIR/$htmlname") or die "couldn't open $GIF_DIR/$htmlname ($!)";

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
          $q->param('mapsize' => '75%');
    }
    $x = $q->param('mapsize');
    $x =~ s/%//;
    $hmult = $hmult * $x / 100;
    $vmult = $vmult * $x / 100;
    if ( $q->param("projection") eq "orthographic")	{
        $hmult = $hmult * 1.25;
    }
    $height = $vmult * $vpix;
    $width = $hmult * $hpix;

    # recenter the image if the GIF size is non-standard
    $gifoffhor = ( 360 - $hpix ) / ( $scale * 2 );
    $gifoffver = ( 180 - $vpix ) / ( $scale * 2 );

    if ( $width > 300 )	{
        if (!$im)  {
            $im = new GD::Image($width,$height+12);
        }
        $totalheight = $height + 12;
    } else	{
        if (!$im)  {
            $im = new GD::Image($width,$height+24);
        }
        $totalheight = $height + 24;
    }

    open AI,">$GIF_DIR/$ainame";
    open AIHEAD,"<./data/AI.header";
    while (<AIHEAD>)	{
        print AI $_;
    }
    close AIHEAD;

    $sizestring = $width . "x";
    $sizestring .= $height + 12;

    $col{'white'} = $im->colorAllocate(255,255,255);
    $aicol{'white'} = "0.00 0.00 0.00 0.00 K";
    $col{'borderblack'} = $im->colorAllocate(1,1,1);
    $aicol{'borderblack'} = "0.02 0.02 0.02 0.99 K";
    $col{'black'} = $im->colorAllocate(0,0,0);
    $aicol{'black'} = "0.00 0.00 0.00 1.00 K";
    $col{'gray'} = $im->colorAllocate(127,127,127);
    $aicol{'gray'} = "0.43 0.31 0.29 0.13 K";
    $col{'light gray'} = $im->colorAllocate(191,191,191);
    $aicol{'light gray'} = "0.23 0.16 0.13 0.02 K";
    $col{'offwhite'} = $im->colorAllocate(254,254,254);
    $aicol{'offwhite'} = "0.0039 0.0039 0.00 0.00 K";
    $col{'dark red'} = $im->colorAllocate(127,0,0);
    $aicol{'dark red'} = "0.33 0.94 0.95 0.25 K";
    $col{'red'} = $im->colorAllocate(255,0,0);
    $aicol{'red'} = "0.01 0.96 0.91 0.0 K";
    $col{'pink'} = $im->colorAllocate(255,159,255);
    $aicol{'pink'} = "0.03 0.38 0.00 0.00 K";
    $col{'brown'} = $im->colorAllocate(127,63,0);
    $aicol{'brown'} = "0.33 0.61 0.98 0.25 K";
    $col{'light brown'} = $im->colorAllocate(223,191,159);
    $aicol{'light brown'} = "0.11 0.20 0.28 0.02 K";
    $col{'ochre'} = $im->colorAllocate(191,127,0);
    $aicol{'ochre'} = "0.20 0.41 0.97 0.07 K";
    $col{'orange'} = $im->colorAllocate(255,127,0);
    $aicol{'orange'} = "0.02 0.50 0.93 0.00 K";
    $col{'light orange'} = $im->colorAllocate(255,159,63);
    $aicol{'light orange'} = "0.02 0.24 0.58 0.00 K";
    $col{'yellow'} = $im->colorAllocate(255,255,0);
    $aicol{'yellow'} = "0.03 0.02 0.91 0.00 K";
    $col{'light yellow'} = $im->colorAllocate(255,255,127);
    $aicol{'light yellow'} = "0.02 0.01 0.50 0.00 K";
    $col{'green'} = $im->colorAllocate(0,255,0);
    $aicol{'green'} = "0.93 0.00 1.00 0.00 K";
    $col{'light green'} = $im->colorAllocate(127,255,127);
    $aicol{'light green'} = "0.51 0.00 0.58 0.00 K";
    $col{'turquoise'} = $im->colorAllocate(95,191,159);
    $aicol{'turquoise'} = "0.64 0.02 0.33 0.00 K";
    $col{'jade'} = $im->colorAllocate(0,143,63);
    $aicol{'jade'} = "0.93 0.05 0.91 0.01 K";
    $col{'teal'} = $im->colorAllocate(0,127,127);
    $aicol{'teal'} = "0.95 0.17 0.40 0.04 K";
    $col{'dark blue'} = $im->colorAllocate(0,0,127);
    $aicol{'dark blue'} = "0.98 0.98 0.02 0.00 K";
    $col{'blue'} = $im->colorAllocate(63,63,255);
    $aicol{'blue'} = "0.80 0.68 0.00 0.00 K";
    $col{'light blue'} = $im->colorAllocate(63,159,255);
    $aicol{'light blue'} = "0.79 0.15 0.00 0.00 K";
    $col{'sky blue'} = $im->colorAllocate(0,255,255);
    $aicol{'sky blue'} = "0.84 0.00 0.00 0.00 K";
    $col{'lavender'} = $im->colorAllocate(127,127,255);
    $aicol{'lavender'} = "0.53 0.42 0.00 0.00 K";
    $col{'violet'} = $im->colorAllocate(191,0,255);
    $aicol{'violet'} = "0.18 0.94 0.00 0.00 K";
    $col{'light violet'} = $im->colorAllocate(191,159,255);
    $aicol{'light violet'} = "0.24 0.33 0.00 0.00 K";
    $col{'purple'} = $im->colorAllocate(223,0,255);
    $aicol{'purple'} = "0.06 0.93 0.00 0.00 K";

	# create an interlaced GIF with a white background
	$im->interlaced('true');
	$im->transparent('');
	if ( $q->param('mapbgcolor') ne "transparent" )	{
		($x,$y) = $self->drawBackground();
	}

	if ( $q->param('gridposition') eq "in back" )	{
		$self->drawGrids();
	}

	# color in the continents based on Scotese's age data
	if ( $q->param('crustcolor') ne "none" && $q->param('crustcolor') )	{

		open MASK,"<$COAST_DIR/masks2/tiltmask.".$self->{maptime};
		while (<MASK>)	{
			s/\n//;
			my ($lat,$lng) = split /\t/,$_;
			$touched{$lng}{$lat} = 1;
		}
		close MASK;

		my $crustcolor = $q->param('crustcolor');
		my $mycolor = $aicol{$crustcolor};
		$mycolor =~ s/ K/ k/;
		print AI "u\n";  # start the group

		# draw a rectangle for each touched cell
		for $lat (-90..90)	{
			for $lng (-180..180)	{
				if ( $touched{$lng}{$lat} ne "" )	{
					my @xs = ();
					my @ys = ();
					push @xs, $lng+1;
					push @xs, $lng+1;
					push @xs, $lng-1;
					push @xs, $lng-1;
					push @ys, $lat+1;
					push @ys, $lat-1;
					push @ys, $lat-1;
					push @ys, $lat+1;
					my $npts = 3;
					my @newxs = ();
					my @newys = ();
					# smooth left half of cell to the north
					if ( $touched{$lng-2}{$lat+2} ne "" &&
					     $touched{$lng}{$lat+2} eq "" )	{
						push @newxs, $xs[3] - ($xs[3] - $xs[2]) / 2;
						push @newys, $ys[3] + ($ys[3] - $ys[2]) / 2;
						$npts++;
					}
					# smooth right half of cell to the north
					if ( $touched{$lng+2}{$lat+2} ne "" &&
					     $touched{$lng}{$lat+2} eq "" )	{
						push @newxs, $xs[0] + ($xs[0] - $xs[1]) / 2;
						push @newys, $ys[0] + ($ys[0] - $ys[1]) / 2;
						$npts++;
					}
					# flatten upper right corner by
					#  adding a point
					if ( $touched{$lng}{$lat+2} eq "" &&
					     $touched{$lng+2}{$lat} eq "" )	{
						push @newxs, $xs[0] - ($xs[0] - $xs[3]) / 2;
						push @newys, $ys[0];
						push @newxs, $xs[0];
						push @newys, $ys[0] - ($ys[0] - $ys[1]) / 2;
						$npts++;
					} else	{
						push @newxs, $xs[0];
						push @newys, $ys[0];
					}
					# smooth upper half of cell to the east
					if ( $touched{$lng+2}{$lat+2} ne "" &&
					     $touched{$lng+2}{$lat} eq "" )	{
						push @newxs, $xs[0] + ($xs[0] - $xs[3]) / 2;
						push @newys, $ys[0] + ($ys[0] - $ys[3]) / 2;
						$npts++;
					}
					# smooth lower half of cell to the east
					if ( $touched{$lng+2}{$lat-2} ne "" &&
					     $touched{$lng+2}{$lat} eq "" )	{
						push @newxs, $xs[1] + ($xs[1] - $xs[2]) / 2;
						push @newys, $ys[1] - ($ys[1] - $ys[2]) / 2;
						$npts++;
					}
					# lower right corner
					if ( $touched{$lng+2}{$lat} eq "" &&
					     $touched{$lng}{$lat-2} eq "" )	{
						push @newxs, $xs[1];
						push @newys, $ys[1] + ($ys[0] - $ys[1]) / 2;
						push @newxs, $xs[1] - ($xs[1] - $xs[2]) / 2;
						push @newys, $ys[1];
						$npts++;
					} else	{
						push @newxs, $xs[1];
						push @newys, $ys[1];
					}
					# lower left corner
					if ( $touched{$lng}{$lat-2} eq "" &&
					     $touched{$lng-2}{$lat} eq "" )	{
						push @newxs, $xs[2] + ($xs[1] - $xs[2]) / 2;
						push @newys, $ys[2];
						push @newxs, $xs[2];
						push @newys, $ys[2] + ($ys[3] - $ys[2]) / 2;
						$npts++;
					} else	{
						push @newxs, $xs[2];
						push @newys, $ys[2];
					}
					# upper left corner
					if ( $touched{$lng-2}{$lat} eq "" &&
					     $touched{$lng}{$lat+2} eq "" )	{
						push @newxs, $xs[3];
						push @newys, $ys[3] - ($ys[3] - $ys[2]) / 2;
						push @newxs, $xs[3] + ($xs[0] - $xs[3]) / 2;
						push @newys, $ys[3];
						$npts++;
					} else	{
						push @newxs, $xs[3];
						push @newys, $ys[3];
					}
					@xs = @newxs;
					@ys = @newys;
					my $nan = "";
					for $p (0..$npts)	{
						($xs[$p],$ys[$p],$rawxs[$p],$rawys[$p]) = $self->projectPoints($xs[$p],$ys[$p],"grid");
						if ( $p == 0 )	{
							$firstx = $rawxs[0];
							$firsty = $rawys[0];
						}
						if ( abs($rawxs[$p] - $firstx) > 180 )	{
							$xs[$p] = -1 * $xs[$p];
						}
						if ( abs($rawys[$p] - $firsty) > 90 )	{
							$ys[$p] = -1 * $ys[$p];
						}
						$xs[$p] = $self->getLngTrunc($xs[$p]);
						$ys[$p] = $self->getLatTrunc($ys[$p]);
						if ( $xs[$p] eq "NaN" || $ys[$p] eq "NaN" )	{
							$nan = "Y";
						}
					}

					if ( $nan eq "" )	{
       						my $poly = new GD::Polygon;
						for $p (0..$npts)	{
							$poly->addPt($xs[$p],$ys[$p]);
						}
      		 				$im->filledPolygon($poly,$col{$crustcolor});
						print AI "0 O\n";
						print AI "$mycolor\n";
						print AI "4 M\n";
						printf AI "%.1f %.1f m\n",$AILEFT+$xs[0],$AITOP-$ys[0];
						for $p (1..$npts)	{
							printf AI "%.1f %.1f L\n",$AILEFT+$xs[$p],$AITOP-$ys[$p];
						}
						printf AI "%.1f %.1f L\n",$AILEFT+$xs[0],$AITOP-$ys[0];
						print AI "f\n";
					}
				}
			}
		}

		print AI "U\n";  # terminate the group
	}

    # draw coastlines
    # first rescale the coordinates depending on the rotation
    if ( $q->param('mapcontinent') ne "standard" || $q->param('projection') ne "rectilinear" )	{
        for $c (0..$#worldlat)	{
            if ( $worldlat[$c] =~ /[0-9]/ )	{
                ($worldlng[$c],$worldlat[$c],$worldlngraw[$c],$worldlatraw[$c],$worldplate[$c]) = $self->projectPoints($worldlng[$c],$worldlat[$c]);
            }
        }
        if ( $q->param('borderlinecolor') ne "none" )	{
            for $c (0..$#borderlat)	{
                if ( $borderlat[$c] =~ /[0-9]/ )	{
                    ($borderlng[$c],$borderlat[$c],$borderlngraw[$c],$borderlatraw[$c],$borderplate[$c]) = $self->projectPoints($borderlng[$c],$borderlat[$c]);
                }
            }
        }
        if ( $q->param('usalinecolor') ne "none" )	{
            for $c (0..$#usalat)	{
                if ( $usalat[$c] =~ /[0-9]/ )	{
                    ($usalng[$c],$usalat[$c],$usalngraw[$c],$usalatraw[$c],$usaplate[$c]) = $self->projectPoints($usalng[$c],$usalat[$c]);
                }
            }
        }
    }
    if ( $q->param('linethickness') eq "thick" )	{
        $thickness = 0.5;
        $aithickness = 1.5;
    } elsif ( $q->param('linethickness') eq "medium" )	{
        $thickness = 0.25;
        $aithickness = 1;
    } else	{
        $thickness = 0;
        $aithickness = 0.5;
    }

    # draw coastlines
    # do NOT connect neighboring points that (1) are on different tectonic plates,
    #  or (2) now are widely separated because one point has rotated onto the
    #  other edge of the map
    $coastlinecolor  = $q->param('coastlinecolor');
    print AI "u\n";  # start the group
    for $c (0..$#worldlat-1)	{
        if ( $worldlat[$c] !~ /NaN/ && $worldlat[$c+1] !~ /NaN/ &&
             $worldlat[$c] =~ /[0-9]/ && $worldlat[$c+1] =~ /[0-9]/ &&
             $worldplate[$c] == $worldplate[$c+1] &&
             abs ( $worldlatraw[$c] - $worldlatraw[$c+1] ) < 5 &&
             abs ( $worldlngraw[$c] - $worldlngraw[$c+1] ) < 5 )	{
            my $x1 = $self->getLng($worldlng[$c]);
            my $y1 = $self->getLat($worldlat[$c]);
            my $x2 = $self->getLng($worldlng[$c+1]);
            my $y2 = $self->getLat($worldlat[$c+1]);
            if ( $x1 !~ /NaN/ && $y1 !~ /NaN/ && $x2 !~ /NaN/ && $y2 !~ /NaN/ )	{
                $im->line( $x1, $y1, $x2, $y2, $col{$coastlinecolor} );
                print AI "$aicol{$coastlinecolor}\n";
                printf AI "%.1f w\n",$aithickness;
                printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
                printf AI "%.1f %.1f l\n",$AILEFT+$x2,$AITOP-$y2;
                print AI "S\n";
                # extra lines offset horizontally
                if ( $thickness > 0 )	{
                    $im->line( $x1-$thickness,$y1,$x2-$thickness,$y2,$col{$coastlinecolor});
                    $im->line( $x1+$thickness,$y1,$x2+$thickness,$y2,$col{$coastlinecolor});
                    # extra lines offset vertically
                    $im->line( $x1,$y1-$thickness,$x2,$y2-$thickness,$col{$coastlinecolor});
                    $im->line( $x1,$y1+$thickness,$x2,$y2+$thickness,$col{$coastlinecolor});
                }
            }
        }
    }
    print AI "U\n";  # terminate the group

    # draw the international borders
    if ( $q->param('borderlinecolor') ne "none" )	{
        $borderlinecolor = $q->param('borderlinecolor');
        print AI "u\n";  # start the group
        for $c (0..$#borderlat-1)	{
            if ( $borderlat[$c] !~ /NaN/ && $borderlat[$c+1] !~ /NaN/ &&
                 $borderlat[$c] =~ /[0-9]/ && $borderlat[$c+1] =~ /[0-9]/ &&
                 $borderplate[$c] == $borderplate[$c+1] &&
                 abs ( $borderlatraw[$c] - $borderlatraw[$c+1] ) < 5 &&
                 abs ( $borderlngraw[$c] - $borderlngraw[$c+1] ) < 5 )	{
                my $x1 = $self->getLng($borderlng[$c]);
                my $y1 = $self->getLat($borderlat[$c]);
                my $x2 = $self->getLng($borderlng[$c+1]);
                my $y2 = $self->getLat($borderlat[$c+1]);
                if ( $x1 !~ /NaN/ && $y1 !~ /NaN/ && $x2 !~ /NaN/ && $y2 !~ /NaN/ )	{
                  $im->line( $x1, $y1, $x2, $y2, $col{$borderlinecolor} );
                  print AI "$aicol{$borderlinecolor}\n";
                  print AI "0.5 w\n";
                  printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
                  printf AI "%.1f %.1f l\n",$AILEFT+$x2,$AITOP-$y2;
                  print AI "S\n";
                }
            }
        }
        print AI "U\n";  # terminate the group
    }

    # draw USA state borders
    if ( $q->param('usalinecolor') ne "none" )	{
        $usalinecolor = $q->param('usalinecolor');
        print AI "u\n";  # start the group
        for $c (0..$#usalat-1)	{
            if ( $usalat[$c] !~ /NaN/ && $usalat[$c+1] !~ /NaN/ &&
                 $usalat[$c] =~ /[0-9]/ && $usalat[$c+1] =~ /[0-9]/ &&
                 $usaplate[$c] == $usaplate[$c+1] &&
                 abs ( $usalatraw[$c] - $usalatraw[$c+1] ) < 5 &&
                 abs ( $usalngraw[$c] - $usalngraw[$c+1] ) < 5 )	{
                my $x1 = $self->getLng($usalng[$c]);
                my $y1 = $self->getLat($usalat[$c]);
                my $x2 = $self->getLng($usalng[$c+1]);
                my $y2 = $self->getLat($usalat[$c+1]);
                if ( $x1 !~ /NaN/ && $y1 !~ /NaN/ && $x2 !~ /NaN/ && $y2 !~ /NaN/ )	{
                    $im->line( $x1, $y1, $x2, $y2, $col{$usalinecolor} );
                    print AI "$aicol{$usalinecolor}\n";
                    print AI "0.5 w\n";
                    printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
                    printf AI "%.1f %.1f l\n",$AILEFT+$x2,$AITOP-$y2;
                    print AI "S\n";
                }
            }
        }
        print AI "U\n";  # terminate the group
    }

    if ( $q->param('gridposition') ne "in back" )	{
        $self->drawGrids();
    }

    return "$GIF_DIR/$htmlname";
}

# Draw the points for collections on the map
sub mapDrawPoints{
    my $self = shift;
    my $dataRowsRef = shift;

    # draw collection data points
    %atCoord = ();
    %longVal = ();
    %latVal = ();
    my $matches = 0;
	foreach $collRef ( @{$dataRowsRef} ) {
 		%coll = %{$collRef};
 		if ( ( $coll{'latdeg'} > 0 || $coll{'latmin'} > 0 || $coll{'latdec'} > 0 ) &&
         ( $coll{'lngdeg'} > 0 || $coll{'lngmin'} > 0 || $coll{'lngdec'} > 0 ) &&
			( $collok{$coll{'collection_no'}} eq "Y" ||
			! $q->param('genus_name') )) {
            # When magnification is high, want to use minutes 
            # in addition to degrees, so the resolution is a bit higher
            if ($scale > 6)  {
                $lngoff = $coll{'lngdeg'};
                $lnghalf = ".00";
                # doubles the number of points longitudinally
                if ( $coll{'lngmin'} >= 30 || ($coll{'lngdec'} =~ /^\d/ && $coll{'lngdec'} >= .50) )	{
                  $lngoff = $lngoff + 0.5;
                  $lnghalf = ".50";
                }

                # E/W modification appears unnecessary, but code is here just in case
                if ( $coll{'lngdir'} eq "East" )	{
                  $lngoff = $lngoff + 0.0;
                } elsif ( $coll{'lngdir'} eq "West" )	{
                  $lngoff = $lngoff - 0.0;
                }
                
                $latoff = $coll{'latdeg'};
                $lathalf = ".00";
                # quadruples the number of point rows latitudinally
                if ( $coll{'latmin'} >= 45 || ($coll{'latdec'} =~ /^\d/ && $coll{'latdec'} >= .75) )	{
                  $latoff = $latoff + 0.75;
                  $lathalf = ".75";
                } elsif ( $coll{'latmin'} >= 30 || ($coll{'latdec'} =~ /^\d/ && $coll{'latdec'} >= .50) )	{
                  $latoff = $latoff + 0.5;
                  $lathalf = ".50";
                } elsif ( $coll{'latmin'} >= 15 || ($coll{'latdec'} =~ /^\d/ && $coll{'latdec'} >= .25) )	{
                  $latoff = $latoff + 0.25;
                  $lathalf = ".25";
                }
                
                if ( $coll{'latdir'} eq "North" )	{
                  $latoff = $latoff + 0.25;
                } elsif ( $coll{'latdir'} eq "South" )	{
                  $latoff = $latoff - 0.25;
                }
                $coordres = 'half';
            } else {
                $lngoff = $coll{'lngdeg'};
                # E/W modification appears unnecessary, but code is here just in case
                if ( $coll{'lngdir'} eq "East" )	{
                  $lngoff = $lngoff + 0.0;
                } elsif ( $coll{'lngdir'} eq "West" )	{
                  $lngoff = $lngoff - 0.0;
                }
                $lngres = 'full';
                $latoff = $coll{'latdeg'};
                $lathalf = ".00";
                $lnghalf = ".00";
                # doubles the number of point rows latitudinally
                if ( $coll{'latmin'} >= 30 || $coll{'latdec'} =~ /^[5-9]/ )	{
                  $latoff = $latoff + 0.5;
                  $lathalf = ".50";
                }
                if ( $coll{'latdir'} eq "North" )	{
                  $latoff = $latoff + 0.5;
                } elsif ( $coll{'latdir'} eq "South" )	{
                  $latoff = $latoff - 0.5;
                }
                $coordres = 'full';
            }
          
            ($x1,$y1,$hemi) = $self->getCoords($lngoff,$latoff);

            if ( $x1 > 0 && $y1 > 0 && $x1-$maxdotsize > 0 &&
                $x1+$maxdotsize < $width &&
                $y1-$maxdotsize > 0 &&
                $y1+$maxdotsize < $height )	{
                    $atCoord{$x1}{$y1}++;
                    $longVal{$x1} = $coll{'lngdeg'} . $lnghalf . " " . $coll{'lngdir'};
                    $latVal{$y1} = $coll{'latdeg'} . $lathalf . " " . $coll{'latdir'};

                    #$self->dbg("Collection ".$coll{'collection_no'}." pixels($x1,$y1) " 
                    #         . "with degrees(".$coll{'lngdeg'}." ".$coll{'lngmin'}."/".$coll{'lngdec'}.",".$coll{'latdeg'}." ".$coll{'latmin'}."/".$coll{'latdec'}.")"
                    #         . "binned to degrees(".$longVal{$x1}.",".$latVal{$y1}.")");

                    $hemiVal{$x1}{$y1} = $hemi;
                    $matches++;
            }
        }
    }
    
    push @coll_counts, $matches;
	$self->dbg("matches: $matches<br>");
	# Bail if we don't have anything to draw.
	if($matches < 1 && $q->param('taxon_info_script') eq "yes"){
		print "NO MATCHING COLLECTION DATA AVAILABLE<br>";
		return;
	}

	print MAPOUT "<table><tr><td>\n<map name=\"PBDBmap\">\n";


    print AI "u\n";  # start the group
    for $x1 (keys %longVal)	{
	    for $y1 (keys %latVal)	{
		    if ($atCoord{$x1}{$y1} > 0)	{
			    if ($dotsizeterm eq "proportional")	{
				    $dotsize = int($atCoord{$x1}{$y1}**0.5) + 1;
			    }
			    print MAPOUT "<area shape=\"rect\" coords=\"";
			    if ( $hemiVal{$x1}{$y1} eq "N" )	{
				    printf MAPOUT "%d,%d,%d,%d", int($x1-(1.5*$dotsize)), int($y1+0.5-(1.5*$dotsize)), int($x1+(1.5*$dotsize)), int($y1+0.5+(1.5*$dotsize));
			    } else	{
				    printf MAPOUT "%d,%d,%d,%d", int($x1-(1.5*$dotsize)), int($y1-0.5-(1.5*$dotsize)), int($x1+(1.5*$dotsize)), int($y1-0.5+(1.5*$dotsize));
			    }
			    print MAPOUT "\" href=\"$BRIDGE_HOME?action=displayCollResults";
			    for $t (keys %filledfields)	{
				    if ($filledfields{$t} ne "")	{
					    my $temp = $filledfields{$t};
					    $temp =~ s/"//g;
					    $temp =~ s/ /\+/g;
					    print MAPOUT "&$t=$temp";
				    }
			    }
			    if ( $q->param('interval_name') )	{
				    print MAPOUT "&max_interval=" . $q->param('interval_name');
			    }
			    if ( $q->param('genus_name') )	{
				    # get rid of spaces in a genus-species name
				    my $clean_name = $q->param('genus_name');
				    $clean_name =~ s/ /\+/g;
				    print MAPOUT "&genus_name=" . $clean_name;
				    print MAPOUT "&taxon_rank=" . $q->param('taxon_rank');
			    }
			    ($latdeg,$latdir) = split / /,$latVal{$y1};
			    ($lngdeg,$lngdir) = split / /,$longVal{$x1};
                ($latdeg, $latdec) = split /\./,$latdeg;
                ($lngdeg, $lngdec) = split /\./,$lngdeg;
                #resolution = full or half degree
                print MAPOUT "&coordres=$coordres"; 
                print MAPOUT "&latdeg=$latdeg&latdec_range=$latdec&latdir=$latdir&lngdeg=$lngdeg&lngdec_range=$lngdec&lngdir=$lngdir\">\n";

                my $mycolor = $aicol{$dotcolor};
                $mycolor =~ s/ K/ k/;
                if ( $dotshape !~ /circles/ && $dotshape !~ /crosses/ )	{
                    print AI "0 O\n";
                    print AI "$mycolor\n";
                    print AI "0 G\n";
                    print AI "4 M\n";
                } elsif ( $dotshape !~ /circles/ )	{
                    print AI "$mycolor\n";
                    print AI "0 G\n";
                }
                # draw a circle and fill it

                if ($dotshape =~ /^circles$/)	{
                  if ( $x1+($dotsize*1.5)+1 < $width && $x1-($dotsize*1.5)-1 > 0 &&
                       $y1+($dotsize*1.5)+1 < $height && $y1-($dotsize*1.5)-1 > 0 )	{
                    $im->arc($x1,$y1,($dotsize*3)+2,($dotsize*3)+2,0,360,$col{$bordercolor});
                    $im->fillToBorder($x1,$y1,$col{$bordercolor},$col{$dotcolor});
                    $im->fillToBorder($x1+$dotsize,$y1,$col{$bordercolor},$col{$dotcolor});
                    $im->fillToBorder($x1-$dotsize,$y1,$col{$bordercolor},$col{$dotcolor});
                    $im->fillToBorder($x1,$y1+$dotsize,$col{$bordercolor},$col{$dotcolor});
                    $im->fillToBorder($x1,$y1-$dotsize,$col{$bordercolor},$col{$dotcolor});
                my $diam = $dotsize * 3;
                my $rad = $diam / 2;
                my $aix = $AILEFT+$x1+$rad;
                my $aiy = $AITOP-$y1;
                my $obl = $diam * 0.27612;
                print AI "$mycolor\n";
                print AI "0 G\n";
                printf AI "%.1f %.1f m\n",$aix,$aiy;
                printf AI "%.1f %.1f %.1f %.1f %.1f %.1f c\n",$aix,$aiy-$obl,$aix-$rad+$obl,$aiy-$rad,$aix-$rad,$aiy-$rad;
                printf AI "%.1f %.1f %.1f %.1f %.1f %.1f c\n",$aix-$rad-$obl,$aiy-$rad,$aix-$diam,$aiy-$obl,$aix-$diam,$aiy;
                printf AI "%.1f %.1f %.1f %.1f %.1f %.1f c\n",$aix-$diam,$aiy+$obl,$aix-$rad-$obl,$aiy+$rad,$aix-$rad,$aiy+$rad;
                printf AI "%.1f %.1f %.1f %.1f %.1f %.1f c\n",$aix-$rad+$obl,$aiy+$rad,$aix,$aiy+$obl,$aix,$aiy;
                if ( $bordercolor !~ "borderblack" )	{
                    print AI "f\n";
                } else	{
                    print AI "b\n";
                    }
                  }
                } elsif ($dotshape =~ /^crosses$/)	{
                  $im->line($x1-$dotsize,$y1-$dotsize,$x1+$dotsize,$y1+$dotsize,$col{$dotcolor});
                  $im->line($x1-$dotsize+0.50,$y1-$dotsize+0.50,$x1+$dotsize+0.50,$y1+$dotsize+0.50,$col{$dotcolor});
                  $im->line($x1-$dotsize+0.50,$y1-$dotsize-0.50,$x1+$dotsize+0.50,$y1+$dotsize-0.50,$col{$dotcolor});
                  $im->line($x1-$dotsize-0.50,$y1-$dotsize+0.50,$x1+$dotsize-0.50,$y1+$dotsize+0.50,$col{$dotcolor});
                  $im->line($x1-$dotsize-0.50,$y1-$dotsize-0.50,$x1+$dotsize-0.50,$y1+$dotsize-0.50,$col{$dotcolor});
                  printf AI "2 w\n";
                  printf AI "%.1f %.1f m\n",$AILEFT+$x1-$dotsize,$AITOP-$y1+$dotsize;
                  printf AI "%.1f %.1f l\n",$AILEFT+$x1+$dotsize,$AITOP-$y1-$dotsize;
                  print AI "S\n";

                  $im->line($x1+$dotsize,$y1-$dotsize,$x1-$dotsize,$y1+$dotsize,$col{$dotcolor});
                  $im->line($x1+$dotsize+0.50,$y1-$dotsize+0.50,$x1-$dotsize+0.50,$y1+$dotsize+0.50,$col{$dotcolor});
                  $im->line($x1+$dotsize+0.50,$y1-$dotsize-0.50,$x1-$dotsize+0.50,$y1+$dotsize-0.50,$col{$dotcolor});
                  $im->line($x1+$dotsize-0.50,$y1-$dotsize+0.50,$x1-$dotsize-0.50,$y1+$dotsize+0.50,$col{$dotcolor});
                  $im->line($x1+$dotsize-0.50,$y1-$dotsize-0.50,$x1-$dotsize-0.50,$y1+$dotsize-0.50,$col{$dotcolor});
                  print AI "$aicol{$dotcolor}\n";
                  printf AI "2 w\n";
                  printf AI "%.1f %.1f m\n",$AILEFT+$x1+$dotsize,$AITOP-$y1+$dotsize;
                  printf AI "%.1f %.1f l\n",$AILEFT+$x1-$dotsize,$AITOP-$y1-$dotsize;
                  print AI "S\n";
                } elsif ($dotshape =~ /^diamonds$/)	{
                  my $poly = new GD::Polygon;
                  $poly->addPt($x1,$y1+($dotsize*2));
                  $poly->addPt($x1+($dotsize*2),$y1);
                  $poly->addPt($x1,$y1-($dotsize*2));
                  $poly->addPt($x1-($dotsize*2),$y1);
                  $im->filledPolygon($poly,$col{$dotcolor});
                  printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1-($dotsize*2);
                  printf AI "%.1f %.1f L\n",$AILEFT+$x1+($dotsize*2),$AITOP-$y1;
                  printf AI "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y1+($dotsize*2);
                  printf AI "%.1f %.1f L\n",$AILEFT+$x1-($dotsize*2),$AITOP-$y1;
                  printf AI "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y1-($dotsize*2);
                }
                elsif ($dotshape =~ /^stars$/)	{
                  my $poly = new GD::Polygon;
                  printf AI "%.1f %.1f m\n",$AILEFT+$x1+($dotsize*sin(9*36*$PI/180)),$AITOP-$y1+($dotsize*cos(9*36*$PI/180));
                  for $p (0..9)	{
                    if ( $p % 2 == 1 )	{
                      $poly->addPt($x1+($dotsize*sin($p*36*$PI/180)),$y1-($dotsize*cos($p*36*$PI/180)));
                      printf AI "%.1f %.1f L\n",$AILEFT+$x1+($dotsize*sin($p*36*$PI/180)),$AITOP-$y1+($dotsize*cos($p*36*$PI/180));
                    } else	{
                      $poly->addPt($x1+($dotsize/$C72*sin($p*36*$PI/180)),$y1-($dotsize/$C72*cos($p*36*$PI/180)));
                      printf AI "%.1f %.1f L\n",$AILEFT+$x1+($dotsize/$C72*sin($p*36*$PI/180)),$AITOP-$y1+($dotsize/$C72*cos($p*36*$PI/180));
                    }
                  }
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
                  printf AI "%.1f %.1f m\n",$AILEFT+$x1+($dotsize*2),$AITOP-$y1-($dotsize*2*sin(60*$PI/180));
                  printf AI "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y1+($dotsize*2*sin(60*$PI/180));
                  printf AI "%.1f %.1f L\n",$AILEFT+$x1-($dotsize*2),$AITOP-$y1-($dotsize*2*sin(60*$PI/180));
                  printf AI "%.1f %.1f L\n",$AILEFT+$x1+($dotsize*2),$AITOP-$y1-($dotsize*2*sin(60*$PI/180));
                }
            # or draw a square
                else	{
                  $im->filledRectangle($x1-($dotsize*1.5),$y1-($dotsize*1.5),$x1+($dotsize*1.5),$y1+($dotsize*1.5),$col{$dotcolor});
                  printf AI "%.1f %.1f m\n",$AILEFT+$x1-($dotsize*1.5),$AITOP-$y1-($dotsize*1.5);
                  printf AI "%.1f %.1f L\n",$AILEFT+$x1-($dotsize*1.5),$AITOP-$y1+($dotsize*1.5);
                  printf AI "%.1f %.1f L\n",$AILEFT+$x1+($dotsize*1.5),$AITOP-$y1+($dotsize*1.5);
                  printf AI "%.1f %.1f L\n",$AILEFT+$x1+($dotsize*1.5),$AITOP-$y1-($dotsize*1.5);
                  printf AI "%.1f %.1f L\n",$AILEFT+$x1-($dotsize*1.5),$AITOP-$y1-($dotsize*1.5);
                }
                if ( $dotshape !~ /circles/ && $dotshape !~ /crosses/ )	{
                  if ( $bordercolor !~ "borderblack" )	{
                    print AI "f\n";
                  } else	{
                    print AI "b\n";
                  }
                }
            }
        }
    }
    print AI "U\n";  # terminate the group

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
            } elsif ($dotshape =~ /^crosses$/)	{ # don't do anything
            } elsif ($dotshape =~ /^diamonds$/)	{
              my $poly = new GD::Polygon;
              $poly->addPt($x1,$y1+($dotsize*2));
              $poly->addPt($x1+($dotsize*2),$y1);
              $poly->addPt($x1,$y1-($dotsize*2));
              $poly->addPt($x1-($dotsize*2),$y1);
              $im->polygon($poly,$col{$bordercolor});
            } elsif ($dotshape =~ /^stars$/)	{
              my $poly = new GD::Polygon;
              for $p (0..9)	{
                if ( $p % 2 == 1 )	{
                  $poly->addPt($x1+($dotsize*sin($p*36*$PI/180)),$y1-($dotsize*cos($p*36*$PI/180)));
              } else	{
                  $poly->addPt($x1+($dotsize/$C72*sin($p*36*$PI/180)),$y1-($dotsize/$C72*cos($p*36*$PI/180)));
                }
              }
              $im->polygon($poly,$col{$bordercolor});
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
}

sub getCoords	{
	my $self = shift;

	my ($x,$y) = @_;
	if ($coll{'lngdir'} =~ /West/)	{
		$x = $x * -1;
	}
	if ($coll{'latdir'} =~ /South/)	{
		$y = $y * -1;
	}
	($x,$y) = $self->projectPoints($x,$y);
	# Get pixel values
	$x = $self->getLng($x);
	$y = $self->getLat($y);
	if ( $x ne "NaN" && $y ne "NaN" )	{
		if ( $y > 0 )	{
			return($x,$y,"North");
		} else	{
			return($x,$y,"South");
		}
	} else	{
		return;
	}
}

sub projectPoints	{
	my $self = shift;

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
	if ( $self->{maptime} > 0 && ( $midlng != $x || $midlat != $y ) && $pointclass ne "grid" && $projected{$x}{$y} eq "" )	{

		my $ma = $self->{maptime};
		$oldx = $x;
		$oldy = $y;


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
    my %plate = %{$self->{plate}};
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
		$projected{$oldx}{$oldy} = $x . ":" . $y . ":" . $pid;
		if ( $x eq "NaN" || $y eq "NaN" )	{
			return('NaN','NaN');
		}

	}
	if ( $oldx eq "" && $oldy eq "" && $projected{$x}{$y} ne "" && $pointclass ne "grid" )	{
		($x,$y,$pid) = split /:/,$projected{$x}{$y};
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

sub getLng	{
	my $self = shift;

	my $l = $_[0];
	if ( $l eq "NaN" )	{
		return('NaN');
	}
	$l = (180 + $l - $offlng - $gifoffhor) * $hmult * $scale;
	if ( $l < 0 || $l > $width )	{
		return('NaN');
	}
	if ( $l == 0 )	{
		$l = 0.0001;
	}
	return $l;
}

sub getLngTrunc	{
	my $self = shift;

	my $l = $_[0];
	if ( $l eq "NaN" )	{
		return('NaN');
	}
	$l = (180 + $l - $offlng - $gifoffhor) * $hmult * $scale;
	if ( $l <= 0 )	{
		return(0.0001);
	} elsif ( $l > $width )	{
		return($width);
	}
	return $l;
}

sub getLat	{
	my $self = shift;

	my $l = $_[0];
	if ( $l eq "NaN" )	{
		return('NaN');
	}
	$l = (90 - $l - $offlat - $gifoffver) * $vmult * $scale;
	if ( $l < 0 || $l > $height )	{
		return('NaN');
	}
	if ( $l == 0 )	{
		$l = 0.0001;
	}
	return $l;
}

sub getLatTrunc	{
	my $self = shift;

	my $l = $_[0];
	if ( $l eq "NaN" )	{
		return('NaN');
	}
	$l = (90 - $l - $offlat - $gifoffver) * $vmult * $scale;
	if ( $l <= 0 )	{
		return(0.0001);
	} elsif ( $l > $height )	{
		return($height);
	}
	return $l;
}

sub drawBackground	{
	my $self = shift;
	my $stage = shift;

	my ($origx,$origy) = $self->projectPoints($midlng,$midlat);
	$origx = $self->getLng($origx);
	$origy = $self->getLat($origy);
	$edgecolor = $col{$q->param('coastlinecolor')};
	$aiedgecolor = $aicol{$q->param('coastlinecolor')};
	my $mycolor = $aicol{$q->param('mapbgcolor')};
	$mycolor =~ s/ K/ k/;
	print AI "u\n";  # start the group
	if ( $edgecolor eq "white" )	{
		$edgecolor = $col{'offwhite'};
	}
	if ( $q->param('projection') eq "rectilinear" )	{
          	$im->filledRectangle(0,0,$width,$height,$col{$q->param('mapbgcolor')});
  		print AI "0 O\n";
            	printf AI "%s\n",$mycolor;
		printf AI "%.1f %.1f m\n",$AILEFT,$AITOP;
		printf AI "%.1f %.1f L\n",$AILEFT+$width,$AITOP;
		printf AI "%.1f %.1f L\n",$AILEFT+$width,$AITOP-$height;
		printf AI "%.1f %.1f L\n",$AILEFT,$AITOP-$height;
		printf AI "%.1f %.1f L\n",$AILEFT,$AITOP;
	} else	{
         	my $poly = new GD::Polygon;
  		print AI "0 O\n";
           	printf AI "%s\n",$mycolor;
		my $x1;
		my $y1;
		for my $hemi (0..1)	{
			for my $lat (-90..89)	{
				my $ll = $lat;
				if ( $hemi == 1 )	{
					$ll = -1 * $ll;
				}
				if ( $q->param('projection') eq "orthographic" )	{
					$x1 = 90 * cos($ll * $PI / 180);
					$y1 = 90 * sin($ll * $PI / 180);
				} elsif ( $q->param('projection') eq "Eckert" )	{
					$x1 = 180 * cos($ll * $PI / 300);
					$y1 = $ll * cos($ll * $PI / 360);
				} elsif ( $q->param('projection') eq "Mollweide" )	{
					$x1 = 180 * cos($ll * $PI / 190);
					$y1 = $ll * cos($ll * $PI / 360);
				}
				if ( $hemi == 1 )	{
					$x1 = -1* $x1;
				}
				if ( $q->param('projection') ne "orthographic" )	{
				}
				$x1 = $self->getLngTrunc($x1);
				$y1 = $self->getLatTrunc($y1);
				$poly->addPt($x1,$y1);
				if ( $lat == -90 && $hemi == 0 )	{
					printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
				} else	{
					printf AI "%.1f %.1f L\n",$AILEFT+$x1,$AITOP-$y1;
				}
			}
		}
		$im->filledPolygon($poly,$col{$q->param('mapbgcolor')});
	}
	print AI "f\n";
  	print AI "U\n";  # terminate the group

	return($origx,$origy);
}

sub drawGrids	{
    my $self = shift;

  $grids = $q->param('gridsize');
  $gridcolor = $q->param('gridcolor');
  print AI "u\n";  # start the group
  if ($grids > 0)	{
    $latlngnocolor = $q->param('latlngnocolor');
    for my $lat ( int(-90/$grids)..int(90/$grids) )	{
      @edgexs = ();
      @edgeys = ();
      for my $deg (-180..179)	{
        my ($lng1,$lat1) = $self->projectPoints($deg , $lat * $grids, "grid");
        my ($lng2,$lat2) = $self->projectPoints($deg + 1 , $lat * $grids, "grid");
        if ( $lng1 ne "NaN" && $lat1 ne "NaN" && $lng2 ne "NaN" && $lat2 ne "NaN" && abs($lng1-$lng2) < 90 )	{
          my $x1 = $self->getLng($lng1);
          my $y1 = $self->getLat($lat1);
          my $x2 = $self->getLng($lng2);
          my $y2 = $self->getLat($lat2);
          if ( $x1 > 0 && $y1 > 0 && $x1 ne "NaN" && $y1 ne "NaN" && ( $y2 eq "NaN" || $x2 eq "NaN" ) )	{
            push @edgexs , $x1;
            push @edgeys , $y1;
          } elsif ( ( $x1 eq "NaN" || $y1 eq "NaN" ) && $x2 > 0 && $y2 > 0 && $x2 ne "NaN" && $y2 ne "NaN" )	{
            push @edgexs , $x2;
            push @edgeys , $y2;
          }
          if ( $x1 ne "NaN" && $y1 ne "NaN" && $x2 ne "NaN" && $y2 ne "NaN" )	{
            $im->line( $x1, $y1, $x2, $y2, $col{$gridcolor} );
            print AI "$aicol{$gridcolor}\n";
            printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
            printf AI "%.1f %.1f l\n",$AILEFT+$x2,$AITOP-$y2;
            print AI "S\n";
          }
        }
      }
      for my $i ( 0..$#edgexs )	{
         my $xfudge = -4;
         if ( $edgexs[$i] < 20 )	{
           $xfudge = 4;
         } elsif ( $edgexs[$i] > $width - 20 )	{
           $xfudge = -12;
         }
         my $yfudge = -5;
         if ( $edgeys[$i] < 20 )	{
           $yfudge = -2;
         } elsif ( $edgeys[$i] > $height - 20 )	{
           $yfudge = -12;
         }
         $im->string(gdSmallFont,$edgexs[$i] + $xfudge,$edgeys[$i] + $yfudge,$lat * $grids,$col{$latlngnocolor});
      }
    }

    for my $lng ( int(-180/$grids)..int(180/$grids) )	{
      @edgexs = ();
      @edgeys = ();
      for my $doubledeg (-180..178)	{
	my $deg = $doubledeg / 2;
        my ($lng1,$lat1) = $self->projectPoints($lng * $grids, $deg, "grid");
        my ($lng2,$lat2) = $self->projectPoints($lng * $grids, $deg + 0.5, "grid");
	if ( $lng1 == 180 )	{
		$lng1 = 179.5;
	}
	if ( $lng2 == 180 )	{
		$lng2 = 179.5;
	}
        if ( $lng1 ne "NaN" && $lat1 ne "NaN" && $lng2 ne "NaN" && $lat2 ne "NaN" && abs($lat1-$lat2) < 45 )	{
          my $x1 = $self->getLng($lng1);
          my $y1 = $self->getLat($lat1);
          my $x2 = $self->getLng($lng2);
          my $y2 = $self->getLat($lat2);
          if ( $x1 > 0 && $y1 > 0 && $x1 ne "NaN" && $y1 ne "NaN" && ( $y2 eq "NaN" || $x2 eq "NaN" ) )	{
            push @edgexs , $x1;
            push @edgeys , $y1;
          } elsif ( ( $x1 eq "NaN" || $y1 eq "NaN" ) && $x2 > 0 && $y2 > 0 && $x2 ne "NaN" && $y2 ne "NaN" )	{
            push @edgexs , $x2;
            push @edgeys , $y2;
          }
          if ( $x1 ne "NaN" && $y1 ne "NaN" && $x2 ne "NaN" && $y2 ne "NaN" )	{
            $im->line( $x1, $y1, $x2, $y2, $col{$gridcolor} );
            print AI "$aicol{$gridcolor}\n";
            printf AI "%.1f %.1f m\n",$AILEFT+$x1,$AITOP-$y1;
            printf AI "%.1f %.1f l\n",$AILEFT+$x2,$AITOP-$y2;
            print AI "S\n";
          }
        }
      }
      for my $i ( 0..$#edgexs )	{
         my $xfudge = -4;
         if ( $lng * $grids > 99 )	{
           $xfudge = $xfudge - 4;
         }
         if ( $edgexs[$i] < 20 )	{
           $xfudge = 4;
         } elsif ( $edgexs[$i] > $width - 20 )	{
           $xfudge = -12;
           if ( $lng * $grids > 99 )	{
             $xfudge = $xfudge - 4;
           }
         }
         my $yfudge = -5;
         if ( $edgeys[$i] < 20 )	{
           $yfudge = -2;
         } elsif ( $edgeys[$i] > $height - 20 )	{
           $yfudge = -12;
         }
         $im->string(gdSmallFont,$edgexs[$i] + $xfudge,$edgeys[$i] + $yfudge,$lng * $grids,$col{$latlngnocolor});
      }
    }
  }
  print AI "U\n";  # terminate the group
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
