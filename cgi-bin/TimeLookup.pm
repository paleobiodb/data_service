package TimeLookup;

$DEBUG = 0;

# written by JA 12.7.03
# WARNING: the logic assumes you want all intervals falling ENTIRELY WITHIN
#  the time interval you have queried

my $dbh;
my $dbt;

sub cleanArrays	{

	$eml_max_interval = "";
	$max_interval_name = "";
	$eml_min_interval = "";
	$min_interval_name = "";

# this is the master list of interval numbers to be used
	@intervals = ();
# without making this local, repeated calls to TimeLookup will accrete
#  more and more "good" intervals
	@tempintervals = ();

	%intervalInScale = ();
	%pubyr = ();
	%bestscale = ();
	%bestscaleyr = ();
	%bestboundary = ();
	%bestboundyr = ();
	%yesints = ();
	%immediatemax = ();
	%immediatemin = ();

}

sub processLookup	{

	$dbh = shift;
	$dbt = shift;

	&cleanArrays();

	$eml_max_interval = shift;
	$max_interval_name = shift;
	$eml_min_interval = shift;
	$min_interval_name = shift;

	if ( ! $min_interval_name )	{
		$eml_min_interval = $eml_max_interval;
		$min_interval_name = $max_interval_name;
	} elsif ( ! $max_interval_name )	{
		$eml_max_interval = $eml_min_interval;
		$max_interval_name = $min_interval_name;
	}

	&findBestScales();
	&getIntervalRange();
	&findImmediateCorrelates();
	&mapIntervals();
	&returnCollectionList();

}

sub processScaleLookup	{

	$dbh = shift;
	$dbt = shift;
	$focal_scale = shift;

	&cleanArrays();

# get an array of the interval numbers falling in the requested scale
	$sql = "SELECT interval_no FROM correlations WHERE scale_no=" . $focal_scale;
	my @intrefs = @{$dbt->getData($sql)};

	&findBestScales();
	&findImmediateCorrelates();

# for each interval in the scale, find all other intervals mapping into it
	for my $intref ( @intrefs )	{

		@intervals = ();
		@tempintervals = ();
		push @intervals , $intref->{interval_no};
		%yesints = ();
		$yesints{$intref->{interval_no}} = "Y";
		&mapIntervals();

	# get the name of the interval
		$sql = "SELECT eml_interval,interval_name FROM intervals WHERE interval_no=" . $intref->{interval_no};
		my $interval_name = @{$dbt->getData($sql)}[0]->{interval_name};
		my $eml_interval = @{$dbt->getData($sql)}[0]->{eml_interval};
		if ( $eml_interval ne "" )	{
			$interval_name = $eml_interval . " " . $interval_name;
		}

	# get a list of collections in this interval
		$sql = "SELECT collection_no FROM collections WHERE ";
		$sql .= "max_interval_no IN ( " . join(',',@intervals) . " ) ";
		$sql .= "AND ( min_interval_no IN ( " . join(',',@intervals) . " ) ";
		$sql .= " OR min_interval_no < 1 )";
		my @collrefs = @{$dbt->getData($sql)};

	# make a hash array in which keys are collection numbers and
	#   values are the name of this interval in the focal scale
		for my $collref ( @collrefs )   {
			$intervalInScale{$collref->{collection_no}} = $interval_name;
		}

	}
	return \%intervalInScale;
}

# JA 2-3.3.04
sub processBinLookup	{

	$dbh = shift;
	$dbt = shift;

	&cleanArrays();

	# get a lookup of the boundary ages for all intervals
	@_ = &findBoundaries($dbh,$dbt);
	my %upperbound = %{$_[0]};
	my %lowerbound = %{$_[1]};

	# this hash array defines the binning
	%binning = ("33" => "Cenozoic 6", # Pleistocene
		"34" => "Cenozoic 6", # Pliocene
		"83" => "Cenozoic 6", # Late Miocene
		"84" => "Cenozoic 5", # Middle Miocene
		"85" => "Cenozoic 5", # Early Miocene
		"36" => "Cenozoic 4", # Oligocene
		"88" => "Cenozoic 3", # Late Eocene
		"107" => "Cenozoic 3", # Bartonian
		"108" => "Cenozoic 2", # Lutetian
		"90" => "Cenozoic 2", # Early Eocene
		"38" => "Cenozoic 1", # Paleocene
		"112" => "Cretaceous 8", # Maastrichtian
		"113" => "Cretaceous 7", # Campanian
		"114" => "Cretaceous 6", # Santonian
		"115" => "Cretaceous 6", # Coniacian
		"116" => "Cretaceous 6", # Turonian
		"117" => "Cretaceous 5", # Cenomanian
		"118" => "Cretaceous 4", # Albian
		"119" => "Cretaceous 3", # Aptian
		"120" => "Cretaceous 2", # Barremian
		"121" => "Cretaceous 2", # Hauterivian
		"122" => "Cretaceous 1", # Valanginian
		"123" => "Cretaceous 1", # Berriasian
		"124" => "Jurassic 6", # Tithonian
		"125" => "Jurassic 5", # Kimmeridgian
		"126" => "Jurassic 5", # Oxfordian
		"127" => "Jurassic 5", # Callovian
		"128" => "Jurassic 4", # Bathonian
		"129" => "Jurassic 4", # Bajocian
		"130" => "Jurassic 3", # Aalenian
		"131" => "Jurassic 3", # Toarcian
		"132" => "Jurassic 2", # Pliensbachian
		"133" => "Jurassic 1", # Sinemurian
		"134" => "Jurassic 1", # Hettangian
		"135" => "Triassic 5", # Rhaetian
		"136" => "Triassic 4", # Norian
		"137" => "Triassic 3", # Carnian
		"138" => "Triassic 2", # Ladinian
		"139" => "Triassic 1", # Anisian
		"46" => "Triassic 1", # Early Triassic
		"715" => "Permian 5", # Changhsingian
		"716" => "Permian 4", # Wuchiapingian
		"145" => "Permian 4", # Capitanian
		"146" => "Permian 3", # Wordian
		"717" => "Permian 3", # Roadian
		"148" => "Permian 2", # Kungurian
		"149" => "Permian 2", # Artinskian
		"150" => "Permian 1", # Sakmarian
		"151" => "Permian 1", # Asselian
		"49" => "Carboniferous 5", # Gzelian
		"50" => "Carboniferous 5", # Kasimovian
		"51" => "Carboniferous 5", # Moscovian
		"52" => "Carboniferous 4", # Bashkirian
		"166" => "Carboniferous 4", # Alportian
		"167" => "Carboniferous 4", # Chokierian
		"168" => "Carboniferous 3", # Arnsbergian
		"169" => "Carboniferous 3", # Pendleian
		"170" => "Carboniferous 3", # Brigantian
		"171" => "Carboniferous 2", # Asbian
		"172" => "Carboniferous 2", # Holkerian
		"173" => "Carboniferous 2", # Arundian
		"174" => "Carboniferous 2", # Chadian
		"55" => "Carboniferous 1", # Tournaisian
		"177" => "Devonian 5", # Famennian
		"178" => "Devonian 4", # Frasnian
		"57" => "Devonian 3", # Middle Devonian
		"181" => "Devonian 2", # Emsian
		"182" => "Devonian 1", # Pragian
		"183" => "Devonian 1", # Lochkovian
		"59" => "Silurian 2", # Pridoli
		"60" => "Silurian 2", # Ludlow
		"61" => "Silurian 2", # Wenlock
		"62" => "Silurian 1", # Llandovery
		"638" => "Ordovician 5", # Ashgillian
		"639" => "Ordovician 4", # Caradocian
		"30" => "Ordovician 3", # Middle Ordovician
		"641" => "Ordovician 2", # Latorpian
		"559" => "Ordovician 1", # Tremadocian
		"69" => "Cambrian 4", # Merioneth
		"70" => "Cambrian 3", # St David's
		"71" => "Cambrian 2", # Caerfai
		"748" => "Cambrian 1"); # Manykaian

	my @binnames = values %binning;

	&findBestScales();
	&findImmediateCorrelates();

	# find the list of collections belonging to each bin
	for my $binname ( @binnames )	{
		@intervals = ();
		@tempintervals = ();
		%yesints = ();

	# find the highest-level intervals falling in the bin
		my @stagenos = keys %binning;
		for my $sn ( @stagenos )	{
			if ( $binning{$sn} eq $binname )	{
				push @intervals, $sn;
				$yesints{$sn} = "Y";
			}
		}

	# now look up the subtended intervals falling in the bin
		&mapIntervals();

	# the boundary estimates for included intervals might contradict
	#  direct estimates for larger intervals; if so, alter the
	#  offending estimates
		for my $i ( @intervals )	{
			my $max = $immediatemax{$i};
			while ( $max > 0 )	{
				if ( $lowerbound{$i} > $lowerbound{$max} )	{
					$lowerbound{$i} = $lowerbound{$max};
				}
				$max = $immediatemax{$max};
			}
			my $min = $immediatemin{$i};
			if ( $min == 0 )	{
				$min = $immediatemax{$i};
			}
			while ( $min > 0 )	{
				if ( $upperbound{$i} < $upperbound{$min} )	{
					$upperbound{$i} = $upperbound{$min};				}
				my $lastmin = $min;
				$min = $immediatemin{$min};
				if ( $min == 0 )	{
					$min = $immediatemax{$lastmin};
				}
			}
		}

	# find the boundary ages for the bin by checking the boundaries of
	#  all intervals falling within it
		for my $i ( @intervals )	{
			if ( $upperbound{$i} < $upperbinbound{$binname} || $upperbinbound{$binname} eq "" )	{
				$upperbinbound{$binname} = $upperbound{$i};
			}
			if ( $lowerbound{$i} > $lowerbinbound{$binname} )	{
				$lowerbinbound{$binname} = $lowerbound{$i};
			}
		}

	# get a list of collections in this bin
		$sql = "SELECT collection_no FROM collections WHERE ";
		$sql .= "max_interval_no IN ( " . join(',',@intervals) . " ) ";
		$sql .= "AND ( min_interval_no IN ( " . join(',',@intervals) . " ) ";
		$sql .= " OR min_interval_no < 1 )";
		my @collrefs = @{$dbt->getData($sql)};

	# make a hash array in which keys are collection numbers and
	#   values are the name of this bin
		for my $collref ( @collrefs )   {
			$intervalInScale{$collref->{collection_no}} = $binname;
		}

	}

	return (\%intervalInScale,\%upperbinbound,\%lowerbinbound);

}

# find the numerical upper and lower bound for each and every interval
# JA 5.3.04
sub findBoundaries	{

	$dbh = shift;
	$dbt = shift;

	&findBestScales();
	&findImmediateCorrelates();

	# set lower boundaries for intervals having direct estimates
	for my $i ( keys %bestscale )	{
		$lowerbound{$i} = $bestboundary{$i};
	}

	# percolate upwards the boundary estimates
	# first the lower boundaries (high numbers)
	for my $i ( keys %bestscale )	{
		$j = $immediatemax{$i};
		while ( $j > 0 && $lowerbound{$i} > 0 )	{
			if ( $lowerbound{$i} > $lowerbound{$j} && $bestboundyr{$i} > $bestboundyr{$j} )	{
				$lowerbound{$j} = $lowerbound{$i};
	# stop if the next, more broad interval already has an older estimate
			} elsif ( $lowerbound{$i} < $lowerbound{$j} )	{
				last;
			}
			$j = $immediatemax{$j};
		}
	}

	# Gallic case: no direct estimate and percolation didn't work because
	#  interval isn't the immediatemax of anything in the most recent time
	#  scales, so try grabbing the lowerbound of the immediately included
	#  interval in the last scale to use the outmoded term
	# WARNING: this won't work if not just the interval but its
	#  immediately included interval are outmoded
	for my $i ( keys %bestscale )	{
		if ( $lowerbound{$i} eq "" )	{
			$lowerbound{$i} = $lowerbound{$bestincludedmax{$i}};
		}
	}

	# set upper boundaries for intervals having direct estimates
	# NOTE: now we're using the percolated lower boundaries instead of
	#   the original estimates
	for my $i ( keys %bestscale )	{
		$upperbound{$i} = $lowerbound{$bestnext{$i}};
	}

	# percolate upwards the upper boundaries (low numbers)
	# NOTE: only do this if the upper boundaries aren't set at all and
	#  the included interval is the youngest in its scale to map into
	#  the including interval
	for my $i ( keys %bestscale )	{
		my $j = $immediatemin{$i};
		if ( $j == 0 )	{
			$j = $immediatemax{$i};
		}
		my $nextj = $immediatemin{$bestnext{$i}};
		if ( $nextj == 0 )	{
			$nextj = $immediatemax{$bestnext{$i}};
		}
		while ( $j > 0  && $upperbound{$i} > 0 )	{
	# here's that tricky conditional
			if ( $upperbound{$j} == 0 && $j != $nextj )	{
				$upperbound{$j} = $upperbound{$i};
	# stop if the next, more broad interval already has a younger estimate
			} elsif ( $upperbound{$i} > $upperbound{$j} && $upperbound{$j} > 0 )	{
				last;
			}
			$lastj = $j;
			$j = $immediatemin{$j};
			if ( $j == 0 )	{
				$j = $immediatemax{$j};
			}
			$nextj = $immediatemin{$bestnext{$lastj}};
			if ( $nextj == 0 )	{
				$nextj = $immediatemax{$bestnext{$lastj}};
			}
		}
	}

	# Pridoli case: upper bound was undefined originally but now exists,
	#  so set it
	for my $i ( keys %bestscale )	{
		if ( $upperbound{$i} eq "" && $lowerbound{$bestnext{$i}} > 0 )	{
			$upperbound{$i} = $lowerbound{$bestnext{$i}};
		}
	}

	# for the intervals that neither (1) had directly defined boundaries,
	#  nor (2) had included intervals with useable boundaries, try the
	#  boundaries for the next higher-ranked interval that has estimates
	for my $i ( keys %bestscale )	{
	# if the interval already has a defined lower boundary,
	#   nothing will happen
		my $j = $i;
		while ( $lowerbound{$i} eq "" && $j > 0 )	{
			$j = $immediatemax{$j};
			$lowerbound{$i} = $lowerbound{$j};
		}
	# now find the upper boundary, again doing nothing if there already
	#  is a defined value
		my $j = $i;
		while ( $upperbound{$i} eq "" && $j > 0 )	{
			my $lastj = $j;
			$j = $immediatemin{$j};
			if ( $j == 0 )	{
				$j = $immediatemax{$lastj};
			}
			$upperbound{$i} = $upperbound{$j};
		}
	}

	# make a hash table where keys are interval names
	# needed by Download.pm
	$sql = "SELECT interval_no,eml_interval,interval_name FROM intervals";
	my @intrefs = @{$dbt->getData($sql)};

	for my $ir ( @intrefs )	{
		my $in = $ir->{interval_name};
		if ( $ir->{eml_interval} )	{
			$in = $ir->{eml_interval} . " " . $in;
		}
		$upperboundbyname{$ir->{interval_name}} = $upperbound{$ir->{interval_no}};
		$lowerboundbyname{$ir->{interval_name}} = $lowerbound{$ir->{interval_no}};
	}

	return (\%upperbound,\%lowerbound,\%upperboundbyname,\%lowerboundbyname);

}

sub findBestScales	{

# find the pubyr of each time scale's ref
	$sql = "SELECT scale_no,reference_no FROM scales";
	my @refnos = @{$dbt->getData($sql)};
	for my $r ( 0..$#refnos )	{
		$sql = "SELECT pubyr FROM refs WHERE reference_no=" . $refnos[$r]->{reference_no};
		$pubyr{$refnos[$r]->{scale_no}} = @{$dbt->getData($sql)}[0]->{pubyr};
	}

# figure out which scale to trust for each interval
	$sql = "SELECT interval_no,scale_no,next_interval_no,max_interval_no,lower_boundary FROM correlations";
	my @corrs = @{$dbt->getData($sql)};
	for my $corr ( @corrs )	{
		if ( $pubyr{$corr->{scale_no}} > $bestscaleyr{$corr->{interval_no}} )	{
			$bestscaleyr{$corr->{interval_no}} = $pubyr{$corr->{scale_no}};
			$bestscale{$corr->{interval_no}} = $corr->{scale_no};
			if ( $corr->{next_interval_no} > 0 )	{
				$bestnext{$corr->{interval_no}} = $corr->{next_interval_no};
			}
			# need this for orphan intervals like the Gallic that
			#  aren't the immediatemax of anything in the most
			#  recent time scales
			$bestincludedmax{$corr->{max_interval_no}} = $corr->{interval_no};
		}
		if ( $pubyr{$corr->{scale_no}} > $bestboundyr{$corr->{interval_no}} && $corr->{lower_boundary} > 0 )	{
			$bestboundyr{$corr->{interval_no}} = $pubyr{$corr->{scale_no}};
			$bestboundary{$corr->{interval_no}} = $corr->{lower_boundary};
		}
	}

}

sub getIntervalRange	{

# BEGIN if input was numeric values
	if ( $max_interval_name > 0 && $min_interval_name > 0 )	{

# rename the "names" as boundaries, just for fun
		$max_boundary = $max_interval_name;
		$min_boundary = $min_interval_name;

# figure out the number of intervals
		$sql = "SELECT interval_no FROM intervals";
		my @results = @{$dbt->getData($sql)};

# for each interval, get the next interval and lower boundary from
#   the best scale
		for $r ( 0..$#results )	{
			$i = $r + 1;
			if ( $bestscale{$i} > 0 )	{

				$sql = "SELECT next_interval_no,lower_boundary FROM correlations WHERE interval_no=" . $i . " AND scale_no=" . $bestscale{$i};
				my @intdata = @{$dbt->getData($sql)};

# now get the lower boundary of the next interval
				$sql = "SELECT lower_boundary FROM correlations WHERE interval_no=" . $intdata[0]->{next_interval_no} . " AND scale_no=" . $bestscale{$i};
				my @nextlb = @{$dbt->getData($sql)};

# if both boundaries are defined, compare them to the max and min boundaries
				if ( $intdata[0]->{lower_boundary} > 0 && $nextlb[0]->{lower_boundary} > 0 )	{
	# if the boundaries are within the max and min, beatify the interval
					if ( $intdata[0]->{lower_boundary} <= $max_boundary && $nextlb[0]->{lower_boundary} >= $min_boundary )	{
						push @intervals, $i;
					}
				}
			}

		}

	} # END processing if input was numeric values

# BEGIN if input was interval names and not numbers
	if ( $max_interval_name =~ /[A-Za-z]/ )	{

# find the scale no for the max interval
	my $sql = "SELECT interval_no FROM intervals WHERE eml_interval='";
	$sql .= $eml_max_interval . "' AND interval_name='";
	$sql .= $max_interval_name . "'";
	my @results = @{$dbt->getData($sql)}[0];
	my $max_interval_no;
	if ( @results )	{
		$max_interval_no = $results[0]->{interval_no};
	}

# find the scale no for the min interval
	my $sql = "SELECT interval_no FROM intervals WHERE eml_interval='";
	$sql .= $eml_min_interval . "' AND interval_name='";
	$sql .= $min_interval_name . "'";
	@results = @{$dbt->getData($sql)}[0];
	my $min_interval_no;
	if ( @results )	{
		$min_interval_no = $results[0]->{interval_no};
	}

# if numbers weren't found for either interval, bomb out!
	if ( ! $max_interval_no && ! $min_interval_no )	{
		return;
	}

# push the numbers onto the master list
	push @intervals, $max_interval_no;
	if ( $max_interval_no != $min_interval_no && ( $min_interval_no > 0 ) )	{
		push @intervals, $min_interval_no;
	}

# find all scales including both numbers or (if there is one) the number
	$sql = "SELECT scale_no FROM correlations WHERE interval_no=";
	$sql .= $max_interval_no;
	my @scales = @{$dbt->getData($sql)};

	$sql = "SELECT scale_no FROM correlations WHERE interval_no=";
	$sql .= $min_interval_no;
	push @scales, @{$dbt->getData($sql)};

# if the scale's pubyr is most recent, record the scale number
	my %seen = ();
	my $maxyr;
	my $bestbothscale;
	for my $scale ( @scales )	{
		$seen{$scale->{scale_no}}++;
		if ( $pubyr{$scale->{scale_no}} > $maxyr && $seen{$scale->{scale_no}} == 2 )	{
			$bestbothscale = $scale->{scale_no};
			$maxyr = $pubyr{$scale->{scale_no}};
		}
	}

# get the "next" (youngest) interval nos for each interval in the scale
	if ( $bestbothscale )	{
		$sql = "SELECT interval_no,next_interval_no FROM correlations WHERE ";
		$sql .= "scale_no=" . $bestbothscale;
		@results = @{$dbt->getData($sql)};
		for my $r ( @results )	{
			$next{$r->{interval_no}} = $r->{next_interval_no};
		}

# using the best scale, run up from the max to the min and add all the intervals
		my $nowat = $max_interval_no;
		while ( $nowat != $min_interval_no && $nowat > 0 )	{
			push @tempintervals, $nowat;
			$nowat = $next{$nowat};
		}

# if the min wasn't run across, maybe they're reversed, so go the other way
		if ( $nowat < 1 )	{
			my $nowat = $min_interval_no;
			@tempintervals = ();
			while ( $nowat != $max_interval_no && $nowat > 0 )	{
				push @tempintervals, $nowat;
				$nowat = $next{$nowat};
			}
		}

# if the max wasn't run across, something is seriously wrong, so throw out
#   the temporary list of intervals and go on
		if ( $nowat < 1 )	{
			@tempintervals = ();
		} elsif ( @tempintervals )	{
# only now add the temp list to the master list of intervals in the range
			push @intervals, @tempintervals;
		}
	}

	} # END of processing if input was interval names

# if the search didn't match anything, bomb out
	if ( ! @intervals )	{
		return;
	}

# for convenience, make a hash array where the keys are the intervals
	for my $i ( @intervals )	{
		$yesints{$i} = "Y";
	}

}

# find the immediate max and min correlates of each interval for use in
#   mapIntervals
sub findImmediateCorrelates	{

	$sql = "SELECT interval_no FROM intervals";
# since the keys are primary the highest number for an interval is just
#  the table size plus 1
	@results = @{$dbt->getData($sql)};
	$ninterval = $#results + 1;

# get lookup hashes of the max and min interval nos for each interval no
	for my $i ( 1..$ninterval )	{
		if ( $bestscale{$i} > 0 )	{
			$sql = "SELECT max_interval_no,min_interval_no FROM correlations WHERE interval_no=" . $i . " AND scale_no=" . $bestscale{$i};
			my $maxmin = @{$dbt->getData($sql)}[0];
			$immediatemax{$i} = $maxmin->{max_interval_no};
			$immediatemin{$i} = $maxmin->{min_interval_no};
		}
	}

}

# check every known interval to see if it maps to the master list
# logic: if the interval's max and min correlates both map to the list,
#  the interval is entirely within the queried interval
sub mapIntervals	{

	my $max;
	my $min;

	for my $i ( 1..$ninterval )	{
		if ( $bestscale{$i} > 0 )	{
		$max = $immediatemax{$i};
		$min = $immediatemin{$i};

	# if both min and max in the "official" list, you're golden
		if ( $yesints{$max} && ( $yesints{$min} || $min == 0 ) )	{
			push @tempintervals , $i;
			$max = 0;
			$min = 0;
		}
	# if not, look at the max/min intervals of the max and min
		while ( $max > 0 )	{
		# this could happen if a correlate isn't actually in a scale
			if ( ! $bestscale{$max} || ( ! $bestscale{$min} && $min > 0 ) )	{
				last;
			}


	# find the min correlate
			my $lastmin = $min;
			if ( $min > 0 )	{
				$min = $immediatemin{$min};
	# the min interval might have only had a max; if so check that too
				if ( $min == 0 )	{
					$min = $immediatemax{$lastmin};
				}
			} else	{
	# even if the interval had no min, its parent might have
				$min = $immediatemin{$max};
			}

	# now find the max correlate
			$max = $immediatemax{$max};

	# if the "grandparents" are within the list, add the interval
	#    we started with and bomb out
			if ( $yesints{$max} && ( $yesints{$min} || $min == 0 ) )	{
				push @tempintervals , $i;
				$max = 0;
				$min = 0;
			}
		}
	}

	}


# add the list of correlated intervals to the list of submitted intervals
	if ( @tempintervals )	{
		push @intervals, @tempintervals;
	}

}

# query the collections table for collections where the max is in the list
#   and so is the min
sub returnCollectionList	{

	$sql = "SELECT collection_no FROM collections WHERE ";
	$sql .= "max_interval_no IN ( " . join(',',@intervals) . " ) ";
	$sql .= "AND ( min_interval_no IN ( " . join(',',@intervals) . " ) ";
	$sql .= " OR min_interval_no < 1 )";
	my @collrefs = @{$dbt->getData($sql)};

	my @collections = ();
	for my $collref ( @collrefs )	{
		push @collections, $collref->{collection_no};
	}

# return the matching collections
	return \@collections;
}


1;
