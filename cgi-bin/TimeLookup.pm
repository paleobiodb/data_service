package TimeLookup;

$DEBUG = 0;

# written by JA 12.7.03
# WARNING: the logic assumes you want all intervals falling ENTIRELY WITHIN
#  the time interval you have queried

sub processLookup	{

	my $dbh = shift;
	my $dbt = shift;

	my $eml_max_interval = shift;
	my $max_interval_name = shift;
	my $eml_min_interval = shift;
	my $min_interval_name = shift;

	if ( ! $min_interval_name )	{
		$eml_min_interval = $eml_max_interval;
		$min_interval_name = $max_interval_name;
	} elsif ( ! $max_interval_name )	{
		$eml_max_interval = $eml_min_interval;
		$max_interval_name = $min_interval_name;
	}

# this is the master list of interval numbers to be used
	my @intervals;

# find the pubyr of each time scale's ref
	$sql = "SELECT scale_no,reference_no FROM scales";
	my @refnos = @{$dbt->getData($sql)};
	my %pubyr = ();
	for my $r ( 0..$#refnos )	{
		$sql = "SELECT pubyr FROM refs WHERE reference_no=" . $refnos[$r]->{reference_no};
		$pubyr{$refnos[$r]->{scale_no}} = @{$dbt->getData($sql)}[0]->{pubyr};
	}

# figure out which scale to trust for each interval
	$sql = "SELECT interval_no,scale_no FROM correlations";
	my @corrs = @{$dbt->getData($sql)};
	for my $corr ( @corrs )	{
		if ( $pubyr{$corr->{scale_no}} > $bestscaleyr{$corr->{interval_no}} )	{
			$bestscaleyr{$corr->{interval_no}} = $pubyr{$corr->{scale_no}};
			$bestscale{$corr->{interval_no}} = $corr->{scale_no};
		}
	}

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
		my @tempintervals = ();
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

# without making this local, repeated calls to TimeLookup will accrete
#  more and more "good" intervals
	my @tempintervals;

# for convenience, make a hash array where the keys are the intervals
	my %yesints = ();
	for my $i ( @intervals )	{
		$yesints{$i} = "Y";
	}

# check every known interval to see if it maps to the master list
# logic: if the interval's max and min correlates both map to the list,
#  the interval is entirely within the queried interval

# now go through the intervals
# need a list of them first
	$sql = "SELECT interval_no FROM intervals";
# since the keys are primary the highest number for an interval is just
#  the table size plus 1
	@results = @{$dbt->getData($sql)};
	$ninterval = $#results + 1;

	my $max;
	my $min;

	for my $i ( 1..$ninterval )	{
		if ( $bestscale{$i} > 0 )	{
		$sql = "SELECT max_interval_no,min_interval_no FROM correlations WHERE interval_no=" . $i . " AND scale_no=" . $bestscale{$i};
		my $maxmin = @{$dbt->getData($sql)}[0];
		$max = $maxmin->{max_interval_no};
		$min = $maxmin->{min_interval_no};

	# if both min and max in the "official" list, you're golden
		if ( $yesints{$max} && ( $yesints{$min} || $min == 0 ) )	{
			push @tempintervals , $i;
			$max = 0;
			$min = 0;
		}
	# if not, look at the max/min intervals of the max and min
		while ( $max > 0 )	{
		# this could happen if a correlate isn't actually in a scale
			if ( ! $bestscale{$max} || ! $bestscale{$min} )	{
				last;
			}

			my $lastmin = $min;

	# first check the max correlate
			$sql = "SELECT max_interval_no FROM correlations WHERE interval_no=" . $max . " AND scale_no=" . $bestscale{$max};
			$maxmin = @{$dbt->getData($sql)}[0];
			$max = $maxmin->{max_interval_no};

	# ... and then the min correlate
			if ( $min > 0 )	{
				$sql = "SELECT min_interval_no FROM correlations WHERE interval_no=" . $min . " AND scale_no=" . $bestscale{$min};
				$maxmin = @{$dbt->getData($sql)}[0];
				$min = $maxmin->{min_interval_no};
			}

	# the min interval might have only had a max; if so check that too
			if ( $min == 0 && $lastmin > 0 )	{
				$sql = "SELECT max_interval_no FROM correlations WHERE interval_no=" . $lastmin . " AND scale_no=" . $bestscale{$lastmin};
				$maxmin = @{$dbt->getData($sql)}[0];
				$min = $maxmin->{max_interval_no};
			}
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

# query the collections table for collections where the max is in the list
#   and so is the min
	$sql = "SELECT collection_no FROM collections WHERE ";
	$sql .= "max_interval_no IN ( " . join(',',@intervals) . " ) ";
	$sql .= "AND ( min_interval_no IN ( " . join(',',@intervals) . " ) ";
	$sql .= " OR min_interval_no < 1 )";
	my @collrefs = @{$dbt->getData($sql)};

	my @collections;
	for my $collref ( @collrefs )	{
		push @collections, $collref->{collection_no};
	}

# return the matching collections
	return \@collections;
}


1;
