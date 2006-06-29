package TimeLookup;

use Data::Dumper;
use CGI::Carp;

#$DEBUG = 0;

# written by JA 12.7.03
# WARNING: the logic assumes you want all intervals falling ENTIRELY WITHIN
#  the time interval you have queried

my $dbh;
my $dbt;

sub cleanArrays	{
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

	my $eml_max_interval = shift;
	my $max_interval_name = shift;
	my $eml_min_interval = shift;
	my $min_interval_name = shift;

	my $return_type = shift;
    #Where numeric (Ma) values are used, use the midpoint instead of max and min bounds
    # when throwing out intervals. Used by Neptune script only right now
    my $use_mid = shift; 
    my $bestbothscale;
    my @errors = ();
    my @warnings = ();



    # 10 M.Y. binning - i.e. Triassic 2
    if ($max_interval_name =~ /^(?:\w+ \d)$/ || $min_interval_name =~ /^(?:\w+ \d)$/) {
        if (!$min_interval_name) {
            $min_interval_name=$max_interval_name;
        } elsif (!$max_interval_name) {
            $max_interval_name=$min_interval_name;
        }
        my @binnames = getTenMYBins(); 
        my ($index1,$index2) = (-1,-1);
        for($i=0;$i<scalar(@binnames);$i++) {
            if ($max_interval_name eq $binnames[$i]) {
                $index1 = $i;
            }
            if ($min_interval_name eq $binnames[$i]) {
                $index2 = $i;
            }
        }

        if ($index1 < 0) {
            push @errors, "Term $max_interval_name not valid or not in the database";
            return ([],@errors);
        } elsif ($index2 < 0) {
            push @errors, "Term $min_interval_name not valid or not in the database.";
            return ([],@errors);
        } else {
            if ($index1 > $index2) {
                ($index1,$index2) = ($index2,$index1);
            }
            #print "INDEX 1 is $index1 INDEX 2 is $index2\n";
            my $binning = processBinLookup($dbh,$dbt,'binning');
            #print "ALL intervals" . join(", ",@intervals)."<BR>";
            my %binmap;
            while (my ($interval_no,$binname) = each %$binning) {
                push @{$binmap{$binname}},$interval_no;
            }
            
            for ($index1 .. $index2) {
                $binname = $binnames[$_];
                #print "ADDING BIN NAME $binname with intervals ".scalar(@{$binmap{$binname}})."<BR>";
                push @intervals, @{$binmap{$binname}};
            }
            #print "ALL intervals" . join(", ",@intervals);
        }
    } elsif ($max_interval_name =~ /^[0-9]+$/ || $min_interval_name =~ /^[0-9]+$/) {
        if ($max_interval_name !~ /^[0-9]+$/) {
            $max_interval_name = 9999;
        }
        if ($min_interval_name !~ /^[0-9]+$/) {
            $min_interval_name = 0;
        }
        #($ub,$lb) = findBoundaries($dbh,$dbt);
        #my ($max_boundary,$min_boundary);

        #$max_boundary = $lb->{$max_interval_no};
        #$min_boundary = $ub->{$min_interval_no};
        #if (!$max_boundary || !$min_boundary) {
        #    main::dbg("Could not find boundaries $max_boundary, $min_boundary\n");
        #    return;    
        #} 
        main::dbg("Lookup type 1 with boundaries $max_interval_name, $min_interval_name\n");
	    &findBestScales();
	    &getIntervalRangeByBoundary($max_interval_name,$min_interval_name,$use_mid);
    } else {
        if ( $min_interval_name eq '')	{
            $eml_min_interval = $eml_max_interval;
            $min_interval_name = $max_interval_name;
        }
        if ( $max_interval_name eq '') 	{
            $eml_max_interval = $eml_min_interval;
            $max_interval_name = $min_interval_name;
        }
        $max_interval_no = getIntervalNo($dbt,$eml_max_interval,$max_interval_name);
        $min_interval_no = getIntervalNo($dbt,$eml_min_interval,$min_interval_name);
   
        # if numbers weren't found for either interval, bomb out!
        if (!$max_interval_no) {
            push @errors, "The term \"$max_interval_name\" not valid or not in the database";
        }
        if (!$min_interval_no) {
            push @errors, "The term \"$min_interval_name\" not valid or not in the database";
        }
        if (@errors) {
            return ([],@errors);
        }
        
    	&findBestScales();
	    &findImmediateCorrelates();

        # Make sure these are called before yesints modified, as they clear it out 
        if (checkIntervalIsObsolete($min_interval_no)) {
            push @warnings, "The term \"$min_interval_name\" may no longer be valid; please use a newer, equivalent term";
        }
        if ($min_interval_no != $max_interval_no) {
            if (checkIntervalIsObsolete($max_interval_no)) {
                push @warnings, "The term \"$max_interval_name\" may no longer be valid; please use a newer, equivalent term";
            }
        }

        # push the numbers onto the master list
        push @intervals, $max_interval_no;
        $yesints{$max_interval_no} = 'Y';
    	push @intervals, $min_interval_no if ($max_interval_no != $min_interval_no);
        $yesints{$min_interval_no} = 'Y';

	    &getIntervalRangeByNo($max_interval_no,$min_interval_no);
        &mapIntervalsUpward();
	    &mapIntervals();
        $bestbothscale = findBestBothScale($dbt,$max_interval_no,$min_interval_no);
        if (!$bestbothscale) {
            push @warnings, "The terms \"$max_interval_name\" and \"$min_interval_name\" are not in the same time scale, so intervals in between them could not be determined; please use terms in the same scale";
        }
#        print Dumper(@intervals);
    }
    return (\@intervals,\@errors,\@warnings);
}

sub checkIntervalIsObsolete {
    my $interval_no = shift;

    my $sql = "SELECT s.scale_rank FROM correlations c, scales s, refs r WHERE c.scale_no=s.scale_no AND s.reference_no=r.reference_no AND c.interval_no=$interval_no ORDER by r.pubyr DESC LIMIT 1";
    my $rank = ${$dbt->getData($sql)}[0]->{'scale_rank'};
    # Skip these, weird cases we can't handle yet
    if ($rank =~ /stage|chron/) {
        return 0;
    }

    @intervals = ();
    @tempintervals = ();
    $yesints{$interval_no} = 'Y';

    &mapIntervals();
    $sql = "SELECT DISTINCT interval_no FROM correlations WHERE max_interval_no=$interval_no OR min_interval_no=$interval_no";
    #print $sql;
    @results = @{$dbt->getData($sql)};
    my $moved_count = 0;
    foreach my $row (@results) {
        if (!$yesints{$row->{'interval_no'}} && $bestscaleyr{$row->{'interval_no'}} > $bestscaleyr{$interval_no}) {
                #print "Could not find $row->{interval_no}<BR>";
                $moved_count++;
        }
    }

    @intervals = ();
    @tempintervals = ();
    %yesints = ();
    if (scalar(@results) && $moved_count == scalar(@results)) {
        return 1;
    } else {
        return 0;
    }
}

sub processScaleLookup	{

	$dbh = shift;
	$dbt = shift;
	$focal_scale = shift;
    $return_type = shift;

	&cleanArrays();

# get an array of the interval numbers falling in the requested scale
	$sql = "SELECT c.interval_no,i.eml_interval,i.interval_name FROM correlations c LEFT JOIN intervals i ON c.interval_no = i.interval_no WHERE c.scale_no=" . $focal_scale;
	my @intrefs = @{$dbt->getData($sql)};

	&findBestScales();
	&findImmediateCorrelates();

    my %intervalToScale = ();
    my %intervalToInterval = ();
# for each interval in the scale, find all other intervals mapping into it
	for my $intref ( @intrefs )	{

		@intervals = ();
		@tempintervals = ();
		push @intervals , $intref->{interval_no};
		%yesints = ();
		$yesints{$intref->{interval_no}} = "Y";
		&mapIntervals();

        # The 'intervalToScale' return type returns a mapping of interval_nos to the interval name of the
        # scale we're processing.  Used in Report.pm PS 12/27/2004
        if ($return_type eq 'intervalToInterval') {
            foreach $interval_in_scale (@intervals) {
                $intervalToInterval{$interval_in_scale} = $intref->{'interval_no'};
            }
        } elsif ($return_type eq 'intervalToScale') {
            foreach $interval_in_scale (@intervals) {
                if ($intref->{'eml_interval'}) {
                    $intervalToScale{$interval_in_scale} = $intref->{'eml_interval'} . ' ' . $intref->{'interval_name'}; 
                } else {
                    $intervalToScale{$interval_in_scale} = $intref->{'interval_name'} ;
                }
            }
        # Else we map collection_nos to interval name of the scale we're processing, and return a hash of that.
        } else {
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
	}

    if ($return_type eq 'intervalToInterval') {
        return \%intervalToInterval;
    } if ($return_type eq 'intervalToScale') {
        return \%intervalToScale;
    } else {
    	return \%intervalInScale;
    }
}

# JA 2-3.3.04
sub processBinLookup	{

	$dbh = shift;
	$dbt = shift;
	my $returndata = shift;

	&cleanArrays();

    my (%upperbound,%lowerbound);
	# get a lookup of the boundary ages for all intervals
    if ($returndata ne 'binning') {
        @_ = &findBoundaries($dbh,$dbt);
        %upperbound = %{$_[0]};
        %lowerbound = %{$_[1]};
    }
    

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
	# used from 19.3.05
		"135" => "Triassic 4", # Rhaetian
		"136" => "Triassic 4", # Norian
		"137" => "Triassic 3", # Carnian
		"45" => "Triassic 2", # Middle Triassic
	# used up to 19.3.05
	#	"135" => "Triassic 5", # Rhaetian
	#	"136" => "Triassic 5", # Norian
	#	"137" => "Triassic 4", # Carnian
	#	"138" => "Triassic 3", # Ladinian
	#	"139" => "Triassic 2", # Anisian
	# used up to 17.8.04
	#	"136" => "Triassic 4", # Norian
	#	"137" => "Triassic 3", # Carnian
	#	"138" => "Triassic 2", # Ladinian
	#	"139" => "Triassic 1", # Anisian
		"46" => "Triassic 1", # Early Triassic
		"143" => "Permian 4", # Changxingian
		"715" => "Permian 4", # Changhsingian
	# used up to 16.8.04
	#	"715" => "Permian 5", # Changhsingian
		"716" => "Permian 4", # Wuchiapingian
		"145" => "Permian 3", # Capitanian
	# used up to 16.8.04
	#	"145" => "Permian 4", # Capitanian
		"146" => "Permian 3", # Wordian
		"717" => "Permian 3", # Roadian
		"148" => "Permian 2", # Kungurian
		"149" => "Permian 2", # Artinskian
		"150" => "Permian 1", # Sakmarian
		"151" => "Permian 1", # Asselian
	# used up to 9.8.04, reverted back to 17.8.04
		"49" => "Carboniferous 5", # Gzelian
		"50" => "Carboniferous 5", # Kasimovian
		"51" => "Carboniferous 4", # Moscovian
	# used up to 17.8.04
	#	"51" => "Carboniferous 5", # Moscovian
		"52" => "Carboniferous 4", # Bashkirian
		"166" => "Carboniferous 3", # Alportian
		"167" => "Carboniferous 3", # Chokierian
	# used up to 9.8.04
	#	"166" => "Carboniferous 4", # Alportian
	#	"167" => "Carboniferous 4", # Chokierian
		"168" => "Carboniferous 3", # Arnsbergian
		"169" => "Carboniferous 3", # Pendleian
		"170" => "Carboniferous 3", # Brigantian
	# added 29.6.06
		"53" => "Carboniferous 3", # Serpukhovian
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
	# added 8.6.06
		"63" => "Ordovician 5", # Ashgill
	# added 29.6.06
		"192" => "Ordovician 5", # Hirnantian
		"639" => "Ordovician 4", # Caradocian
	# added 8.6.06
		"64" => "Ordovician 4", # Caradoc
	# added 29.6.06
		"787" => "Ordovician 4", # early Late Ordovician
		"65" => "Ordovician 3", # Llandeilo
		"66" => "Ordovician 3", # Llanvirn
	# used up to 15.8.04
	#	"30" => "Ordovician 3", # Middle Ordovician
		"596" => "Ordovician 2", # Arenigian
	# added 8.6.06
		"67" => "Ordovician 2", # Arenig
	# added 29.6.06
		"789" => "Ordovician 2", # late Early Ordovician
	# used up to 15.8.04
	#	"641" => "Ordovician 2", # Latorpian
		"559" => "Ordovician 1", # Tremadocian
	# added 8.6.06
		"68" => "Ordovician 1", # Tremadoc
		"69" => "Cambrian 4", # Merioneth
	# added 29.6.06
		"780" => "Cambrian 4", #  Furongian
		"70" => "Cambrian 3", # St David's
	# added 29.6.06
		"781" => "Cambrian 3", # Middle Cambrian
		"71" => "Cambrian 2", # Caerfai
	# next four added 29.6.06
		"749" => "Cambrian 2", # Toyonian
		"750" => "Cambrian 2", # Botomian
		"213" => "Cambrian 2", # Atdabanian
		"214" => "Cambrian 2", # Tommotian
		"748" => "Cambrian 1", # Manykaian
	# added 29.6.06
		"799" => "Cambrian 1"); # Nemakit-Daldynian

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
	# these get tacked on to @intervals, which also includes the
	#  immediately subtended intervals of this bin
		&mapIntervals();

	# we also want to know the bin assignments of the not-immediately
	#  subtended intervals, say, for some function to print a list of all
	#  subtended intervals
	# this is only going to be returned if the request was for boundaries
	#  and not for assignments of collections
		for my $i ( @intervals )	{
			$binning{$i} = $binname;
		}

    # we don't care about boundary estimates if we just want a interval-->bin 
    # mapping, so skip that part
        next if ($returndata eq 'binning');

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
	# don't do this if we only need boundary estimates
		if ( $returndata ne "boundaries" )	{
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

	}
	@tempintervals = ();
	%yesints = ();
    @intervals = (); # Clear it out, don't want to have leftovers screwing stuff up later

	if ( $returndata eq "binning" )	{
        return \%binning;
	} elsif ( $returndata eq "boundaries" )	{
		return (\%upperbinbound,\%lowerbinbound,\%binning);
	} else	{
		return (\%intervalInScale,\%upperbinbound,\%lowerbinbound);
	}

}

# find the numerical upper and lower bound for each and every interval
# JA 5.3.04
sub findBoundaries	{

	$dbh = shift;
	$dbt = shift;
    my $scale_no = shift;
    $skip_orphaned_intervals = shift;

	&findBestScales($scale_no);
	&findImmediateCorrelates();


#    print "<br><br>bestincludedmax".Dumper(\%bestincludedmax); 
#    print "<br><br>bestscale".Dumper(\%bestscale); 
#    print "<br><br>bestboundary".Dumper(\%bestboundary); 
#    print "<br><br>immediatemax".Dumper(\%immediatemax); 
#    print "<br><br>immediatemin".Dumper(\%immediatemin); 
#    print "<br><br>bestnext".Dumper(\%bestnext); 

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

	# percolate downwards the lower boundaries (early Early Hemphillian
	#  case)  26.1.05 JA
	# need to do this when you have (say) zones with no age estimates,
	#  but they fall into (say) age/stages with estimates
	for my $i ( keys %bestscale )	{
		if ( $lowerbound{$i} eq "" )	{
			$j = $immediatemax{$i};
			while ( $j > 0 && $lowerbound{$i} == "" )	{
				$lowerbound{$i} = $lowerbound{$j};
				$j = $immediatemax{$j};
			}
		}
	}

	# set upper boundaries for intervals having direct estimates
	# NOTE: now we're using the percolated lower boundaries instead of
	#   the original estimates
	for my $i ( keys %bestscale )	{
		if ( $lowerbound{$i} != $lowerbound{$bestnext{$i}} )	{
			$upperbound{$i} = $lowerbound{$bestnext{$i}};
		}
		# if the next interval has an identical lower bound,
		#  keep going up trying to find one with a different
		#  lower bound JA 26.1.05
		else	{
			$tempbestnext = $bestnext{$i};
			while ( $lowerbound{$i} == $lowerbound{$tempbestnext} && $tempbestnext > 0 )	{
				$tempbestnext = $bestnext{$tempbestnext};
				if ( $lowerbound{$i} != $lowerbound{$tempbestnext} )	{
					$upperbound{$i} = $lowerbound{$tempbestnext};
				}
			}
		}
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
		$j = $i;
		while ( $upperbound{$i} eq "" && $j > 0 )	{
			my $lastj = $j;
			$j = $immediatemin{$j};
			if ( $j == 0 )	{
				$j = $immediatemax{$lastj};
			}
			$upperbound{$i} = $upperbound{$j};
		}
	}

    # Ufimian case. Optionally remove intervals which no longer map to any location
    # in the composite scale by deleting intervals that don't have any interval pointing
    # to them.  
    if ($skip_orphaned_intervals) {
        #while(($k,$v)=each %bestnext) {
        foreach $v (values %bestnext) {
            $refbestnext{$v}=1;
        }
        for my $int_no (keys %lowerbound) {
            if (!$refbestnext{$int_no}) {
                # We have to make sure that this interval is not an interval that is starting a scale! 
                my $sql = "SELECT * FROM correlations WHERE next_interval_no=$int_no";
                my @result = @{$dbt->getData($sql)};
                if (@result) {
                    #print "no ref for $int_no"; 
                    delete $lowerbound{$int_no};
                    delete $upperbound{$int_no};
                }
            } 
        }
    }

    # Holocene hack PS 9/14/2005
    $upperbound{32} = 0;

	# make a hash table where keys are interval names
	# needed by Download.pm
	$sql = "SELECT interval_no,eml_interval,interval_name FROM intervals";
	my @intrefs = @{$dbt->getData($sql)};

	for my $ir ( @intrefs )	{
		my $in = $ir->{interval_name};
		if ( $ir->{eml_interval} )	{
			$in = $ir->{eml_interval} . " " . $in;
		}
		$upperboundbyname{$in} = $upperbound{$ir->{interval_no}} if exists $upperbound{$ir->{interval_no}};
		$lowerboundbyname{$in} = $lowerbound{$ir->{interval_no}} if exists $lowerbound{$ir->{interval_no}};
	}

	return (\%upperbound,\%lowerbound,\%upperboundbyname,\%lowerboundbyname);

}

sub findBestScales	{
    my $scale_no = shift;

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
        if ($pubyr{$corr->{scale_no}} > $bestscaleyr{$corr->{interval_no}}) {
        #if ((!$scale_no && ($pubyr{$corr->{scale_no}} > $bestscaleyr{$corr->{interval_no}})) ||
        #    ( $scale_no && ($corr->{scale_no}==$scale_no)))	
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
    #; Hack for Confidence.pm.  If we specifiy a scale no, don't use the composite interval estimates
    if ($scale_no) {
        for my $corr ( @corrs )	{
            if ($corr->{'scale_no'} == $scale_no) {
                $bestscale{$corr->{interval_no}} = $corr->{scale_no};
                if ( $corr->{next_interval_no} > 0 )	{
                    $bestnext{$corr->{interval_no}} = $corr->{next_interval_no};
                } 
                if ($corr->{lower_boundary} > 0){
                    $bestboundary{$corr->{interval_no}} = $corr->{lower_boundary};
                }
            }
        }
    }
}

# Boundaries are given in millions of years
# This will find all intervals between the two boundaries specified
# and place those intervals into the globabl intervals hash
sub getIntervalRangeByBoundary {
    my ($max_boundary,$min_boundary);
    if ($_[0] > $_[1]) {
        $max_boundary = $_[0];
        $min_boundary = $_[1];
    } else {
        $max_boundary = $_[1];
        $min_boundary = $_[0];
    }
    my $use_mid = $_[2];
    main::dbg("Using numbers - max is $max_boundary and min is $min_boundary");

    # Changed to use the nicer findBoundaries function, 
    # instead of trying to figure out boundaries on its own, PS 04/08/2005
    ($upperbound,$lowerbound) = findBoundaries($dbh,$dbt);
    while (my ($i,$lbound) = each %$lowerbound) {
        my $ubound = $upperbound->{$i};
        if ($lbound ne '' && $ubound ne '') {
            if ($use_mid) {
                my $mid = ($ubound+$lbound)/2;
                if ($mid <= $max_boundary && $mid >= $min_boundary) {
                    push @intervals,$i;
                }
            } else {
                if ($lbound <= $max_boundary && $ubound >= $min_boundary) {
                    push @intervals,$i;
                }
            }
        }
    }
}


# This function will find all intervals that an interval maps into
sub getMaxIntervals {
    $dbt = shift;
    $dbh = $dbt->dbh;
    my $interval_no = shift;
    if (!%immediatemax) {
        &findImmediateCorrelates();
    }
    my @intervals = ();
    my @queue = ();
    push @queue, $interval_no;
    while (@queue) {
        my $i = pop(@queue);
        if (my $j = $immediatemax{$i}) {
            push @intervals,$j;
            push @queue,$j;
        }    
        if (my $j = $immediatemin{$i}) {
            push @intervals,$j;
            push @queue,$j;
        }    
    }
    return @intervals;
}


# Separated out of the old getIntervalRange, which did multiple separate things before PS 04/08/2005
# This will get all the intervals that lie between two intervals on the same scale
# and add those intervals to the global $yesints hash and @intervals hash
sub getIntervalRangeByNo {
    my ($orig_max_interval_no, $orig_min_interval_no) = @_;

    # Search for scales the intervals both belong to, and use those if they exist
    my $bestbothscale = findBestBothScale($dbt,$orig_max_interval_no,$orig_min_interval_no);
    my ($commonscale, $straggler_no, $max_interval_no, $min_interval_no);
    # If they don't belong to a common scale, then maybe one of the intervals belongs to
    # a scale thats common with the other interval
    if (!$bestbothscale) {
        @max_parents = getMaxIntervals($dbt,$orig_max_interval_no);
        @min_parents = getMaxIntervals($dbt,$orig_min_interval_no);
        foreach $max_parent (@max_parents) {
            last if ($commonscale); 
            $commonscale = findBestBothScale($dbt,$max_parent,$orig_min_interval_no);
            if ($commonscale) {
                main::dbg("Found a common scale $commonscale between $orig_min_interval_no and parent $max_parent of $orig_max_interval_no");
                $max_interval_no=$max_parent;
                $min_interval_no=$orig_min_interval_no;
                $bestbothscale = $commonscale;
                $straggler_no = $orig_max_interval_no;
            }
        }
        foreach $min_parent (@min_parents) {
            last if ($commonscale); 
            $commonscale = findBestBothScale($dbt,$orig_max_interval_no,$min_parent);
            if ($commonscale) {
                main::dbg("Found a common scale $commonscale between $orig_max_interval_no and parent $min_parent of $orig_min_interval_no");
                $max_interval_no=$orig_max_interval_no;
                $min_interval_no=$min_parent;
                $bestbothscale = $commonscale;
                $straggler_no = $orig_min_interval_no;
            }
        }
    } else {
        $max_interval_no =  $orig_max_interval_no;
        $min_interval_no =  $orig_min_interval_no;
    }

    if ($bestbothscale)	{
        if ($max_interval_no == $min_interval_no) {
            return;
        }
        $sql = "SELECT interval_no,next_interval_no FROM correlations WHERE ";
        $sql .= "scale_no=" . $bestbothscale;
        @results = @{$dbt->getData($sql)};
        for my $r ( @results )	{
            $next{$r->{interval_no}} = $r->{next_interval_no};
        }
        my $found_range = 0;
        my $direction = 0;
        # using the best scale, run up from the max to the min and add all the intervals
        my $nowat = $max_interval_no;
        while ($nowat = $next{$nowat}) {
            if ($nowat == $min_interval_no)  {
                $found_range = 1;
                $direction = 1;
                last;
            }
            push @tempintervals, $nowat;
        }

        # if the min wasn't run across, maybe they're reversed, so go the other way
        if ( !$found_range) {
            $nowat = $min_interval_no;
            @tempintervals = ();
            while ( $nowat = $next{$nowat})	{
                if ($nowat == $max_interval_no) {
                    $found_range = 1;
                    $direction = 0;
                    last;
                }
                push @tempintervals, $nowat;
            }
        }

# if the max wasn't run across, something is seriously wrong, so throw out
#   the temporary list of intervals and go on
        if (!$found_range) {
            @tempintervals = ();
            carp("Something wrong in getIntervalRangeByNo: Could not find range of interval values
for scale $bestbothscale and intervals $max_interval_no $min_interval_no"); 
        } elsif ( @tempintervals )	{
            if ($straggler_no && $bestscale{$straggler_no}) {
                $sql = "(SELECT c2.interval_no, c2.next_interval_no, 2.max_interval_no, c2.min_interval_no FROM correlations c1, correlations c2 WHERE c1.interval_no=$straggler_no AND c1.max_interval_no=c2.max_interval_no AND c2.scale_no=$bestscale{$straggler_no})". 
                       " union ".
                my $sql = "SELECT interval_no, next_interval_no, max_interval_no, min_interval_no ". 
                           " FROM correlations c1, c2 WHERE c1. c1.scale_no=$bestscale{$straggler_no}";
                my @results = @{$dbt->getData($sql)};
                main::dbg("Looking for stragglers in scale $bestscale{$straggler_no} for straggler $straggler_no and direction $direction");
                my %next;
                for my $r ( @results )	{
                    if ($direction == 0) {
                        $next{$r->{interval_no}} = $r->{next_interval_no};
                        main::dbg("$r->{interval_no} --> $r->{next_interval_no}, ");
                    } else {
                        $next{$r->{next_interval_no}} = $r->{interval_no};
                        main::dbg("$r->{next_interval_no} --> $r->{interval_no}, ");
                    }
                }
                $nowat = $straggler_no;
                while (($nowat = $next{$nowat}) && 
                       (($immediatemax{$nowat} && $immediatemax{$nowat} == $immediatemax{$straggler_no}) || 
                        ($immediatemin{$nowat} && $immediatemin{$nowat} == $immediatemin{$straggler_no}))) {
                    push @tempintervals, $nowat;
                    main::dbg("Found straggler $nowat");
                }
            } 
        
# only now add the temp list to the master list of intervals in the range
            push @intervals, @tempintervals;
        }

# note that we're not doing a proper return of $yesints and @intervals
# because we're just setting module globals (call me lazy)
# for convenience, make a hash array where the keys are the intervals
        for my $i ( @intervals )	{
            $yesints{$i} = "Y";
        }
    } else {
        main::dbg("Could not find bestbothscale for $max_interval_no, $min_interval_no in function getIntervalRangeByNo");
    }
}

# A trivial function PS 04/08/2005
sub getIntervalNo {
    my $dbt = shift;
    my $eml = shift;
    my $name = shift;
    my $dbh = $dbt->dbh;

    my $sql = "SELECT interval_no FROM intervals ".
              " WHERE interval_name=".$dbh->quote($name);
    if ($eml) {
        $sql .= " AND eml_interval=".$dbh->quote($eml);
    }
              
    my $row = ${$dbt->getData($sql)}[0];
    if ($row) {
        return $row->{'interval_no'};
    } else {
        return undef;
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

    if (!%bestscale) {
        findBestScales();
    }

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

sub findBestBothScale{
    my $dbt = shift;
    my $max_interval_no = shift;
    my $min_interval_no = shift;
    my $bestbothscale;
    my $max_pubyr = -1;

    return ($bestbothscale) if ($max_interval_no !~ /^\d+$/ || $min_interval_no !~ /^\d+$/);
    
    # find the scale no for the max interval
    my $sql = "SELECT scale_no FROM correlations WHERE interval_no=$max_interval_no";
    my @max_scales = map {$_->{'scale_no'}} @{$dbt->getData($sql)};

    # find the scale no for the min interval
    $sql = "SELECT scale_no FROM correlations WHERE interval_no=$min_interval_no";
    my @min_scales = map {$_->{'scale_no'}} @{$dbt->getData($sql)};

    # if the scale's pubyr is most recent, record the scale number
    for my $max_scale (@max_scales) {
        for my $min_scale (@min_scales) {
            if ($min_scale == $max_scale && $pubyr{$max_scale} > $max_pubyr) {
                $bestbothscale = $max_scale;
                $max_pubyr = $pubyr{$max_scale};
            }
        }
    }    
    return $bestbothscale;
}

# This finds upward mappings i.e. epochs implied by stages.  The algorithm is such:
# If all the intervals in a particular scale in a particular broader interval (epoch/etc) are
# present, then include that broader interval as well.  Do this repeatedly until we can't
# find any more intervals to include
# A problem might occur if we have a really tiny scale that doesn't cover all of
# a broader interval. No way to reliable test for this, may have to find these
# and exclude them manually - PS 04/13/2005
sub mapIntervalsUpward {
    my @new_intervals; 
    my $first_time = 1;
    while (@new_intervals || $first_time) {
        my $intervals_str;
        if ($first_time) {
            $intervals_str = join(",",@intervals); 
        } else {
            $intervals_str = join(",",@new_intervals);
        }
        $first_time = 0;
        $sql = "(select c2.scale_no,c2.interval_no, c2.max_interval_no, c2.min_interval_no from correlations c1, correlations c2 where c1.interval_no in ($intervals_str) and c1.max_interval_no=c2.max_interval_no)". 
               " union ".
               "(select c2.scale_no,c2.interval_no, c2.max_interval_no, c2.min_interval_no from correlations c1, correlations c2 where c1.interval_no in ($intervals_str) and c1.max_interval_no=c2.max_interval_no)"; 
       
#            print "classify upwards $sql";
        my @results = @{$dbt->getData($sql)};
        my %interval_set = ();
        foreach my $row (@results) {
            $interval_set{$row->{'max_interval_no'}}{$row->{'scale_no'}}{$row->{'interval_no'}} = 1 if ($row->{'max_interval_no'});
            $interval_set{$row->{'min_interval_no'}}{$row->{'scale_no'}}{$row->{'interval_no'}} = 1 if ($row->{'min_interval_no'});
        }
#            print "YesInts".Dumper(\%yesints);
        # Reset new_intervals.  The behavior is:
        #  if we find new upward intervals, we want to continue looking upwards, as there might
        #  be other intervals that we have yet to map. if we don't find any new upward intervals,
        #  then we should stop looking, we're done
        @new_intervals = ();
        while(my ($higher_int,$scales)=each %interval_set) {
            while (my ($scale_no,$children_ints) = each %$scales) {
                # We add an interval if all intervals that map into that interval (in the same scale) are 
                # already included, thus implying that this interval should be included as well
                my $found = scalar(keys(%$children_ints));
#                print "FOUND higher_int $higher_int with found_cnt = $found\n";
                foreach $child_int (keys(%$children_ints)) {
#                    print "Found child $child_int for higher int $higher_int\n";
                    if ($yesints{$child_int}) {
                        $found--;
                    }
                }
                #print "Found leftofter $found\n";
                if ($found == 0) {
                    main::dbg("Adding interval $higher_int because all intervals in scale $scale_no that map into it are included");
                    push @new_intervals, $higher_int;
                    push @intervals, $higher_int;
                    $yesints{$higher_int}='Y';
                }
            }
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
        foreach my $ti (@tempintervals) {
            push @intervals, $ti;
            $yesints{$ti} = 1;
        }
    }
}

# query the collections table for collections where the max is in the list
#   and so is the min - DEPRECATED
sub getCollectionList	{
    my @collections = ();

    if (@intervals) {
        $sql = "SELECT collection_no FROM collections WHERE ";
        $sql .= "max_interval_no IN ( " . join(',',@intervals) . " ) ";
        $sql .= "AND ( min_interval_no IN ( " . join(',',@intervals) . " ) ";
        $sql .= " OR min_interval_no < 1 )";
        my @collrefs = @{$dbt->getData($sql)};

        for my $collref ( @collrefs )	{
            push @collections, $collref->{collection_no};
        }
    }
# return the matching collections plus a value indicating whether the
#  intervals are in the same scale (computed by getIntervalRange)
    return \@collections;
}

# Utility function, parse input from form into valid eml+interval name pair, if possible
sub splitInterval {
    my $dbt = shift || return ('','');
    my $interval_name = shift;

    my @terms = split(/ /,$interval_name);
    my @eml_terms;
    my @interval_terms;
    foreach $term (@terms) {
        if ($term =~ /early|lower|middle|late|upper/i) {
            push @eml_terms, $term;
        } else {
            push @interval_terms, $term;
        }
    }
    my $interval = join(" ",@interval_terms);

    my $eml;
    if (scalar(@eml_terms) == 1) {
        $eml = 'Early/Lower' if ($eml_terms[0] =~ /lower|early/i);
        $eml = 'Late/Upper' if ($eml_terms[0] =~ /late|upper/i);
        $eml = 'Middle' if ($eml_terms[0] =~ /middle/i);
    } elsif(scalar(@eml_terms) > 1) {
        my ($eml0, $eml1);
        $eml0 = 'early'  if ($eml_terms[0] =~ /early|lower/i);
        $eml0 = 'middle' if ($eml_terms[0] =~ /middle/i);
        $eml0 = 'late'   if ($eml_terms[0] =~ /late|upper/i);
        $eml1 = 'Early'  if ($eml_terms[1] =~ /early|lower/i);
        $eml1 = 'Middle' if ($eml_terms[1] =~ /middle/i);
        $eml1 = 'Late'   if ($eml_terms[1] =~ /late|upper/i);
        if ($eml0 && $eml1) {
            $eml = $eml0.' '.$eml1;
        }
    }
                                                                                                                                                             
    return ($eml,$interval);
}

sub getTenMYBins() {
    return ("Cenozoic 6", "Cenozoic 5", "Cenozoic 4", "Cenozoic 3", "Cenozoic 2", "Cenozoic 1", "Cretaceous 8", "Cretaceous 7", "Cretaceous 6", "Cretaceous 5", "Cretaceous 4", "Cretaceous 3", "Cretaceous 2", "Cretaceous 1", "Jurassic 6", "Jurassic 5", "Jurassic 4", "Jurassic 3", "Jurassic 2", "Jurassic 1", "Triassic 4", "Triassic 3", "Triassic 2", "Triassic 1", "Permian 4", "Permian 3", "Permian 2", "Permian 1", "Carboniferous 5", "Carboniferous 4", "Carboniferous 3", "Carboniferous 2", "Carboniferous 1", "Devonian 5", "Devonian 4", "Devonian 3", "Devonian 2", "Devonian 1", "Silurian 2", "Silurian 1", "Ordovician 5", "Ordovician 4", "Ordovician 3", "Ordovician 2", "Ordovician 1", "Cambrian 4", "Cambrian 3", "Cambrian 2", "Cambrian 1");
}

# Returns an array of interval names in the correct order for a given scale
# With the newest interval first -- not finished yet, don't use
# PS 02/28/3004
sub getScaleOrder {
    my $dbt = shift;
    my $scale_no = shift;
    my $return_type = shift || "name"; #name or number
    my $is_composite = shift;

    my @scale_list = ();

    if ($is_composite) {
        if (!%bestnext) {
            findBestScales();
        }    
        # find first guy in scale - sort of a hack, just use max interval no for now
#        my $sql = "SELECT max(lower_boundary) as maxb FROM correlations WHERE scale_no=".$dbt->dbh->quote($scale_no);
#        my @results = @{$dbt->getData($sql)};
        #$sql = "SELECT interval_no FROM correlations WHERE scale_no=".$dbt->dbh->quote($scale_no)." AND lower_boundary=".$results[0]->{'maxb'};
        my $sql = "SELECT MAX(interval_no) as interval_no FROM correlations WHERE scale_no=".$dbt->dbh->quote($scale_no);
        my @results = @{$dbt->getData($sql)};
        my $interval_no = $results[0]->{'interval_no'};
        my $count = 0;
        while (1) {
            if ($count++ > 200) {die "loop";}
            unshift @scale_list, $interval_no;
            if (!exists $bestnext{$interval_no}) {
                if ($bestscale{$interval_no} != $scale_no) {
                    #Backtrack till we can rejoin teh scale
                    foreach $orig_interval_no (@scale_list) {
                        my $sql = "SELECT next_interval_no FROM correlations WHERE interval_no=$orig_interval_no AND scale_no=".$dbt->dbh->quote($scale_no);
                        my @results = @{$dbt->getData($sql)};
                        if (scalar(@results)) {
                            $interval_no=$results[0]->{'next_interval_no'};
                            last;
                        }
                    }
                    if (!$interval_no) {
                        last;
                    }
                } else {
                    last;
                }
            } else {
                $interval_no=$bestnext{$interval_no};
            }
        }
    } else {
        my $count;
        my $sql = "SELECT correlations.interval_no, next_interval_no, interval_name FROM correlations, intervals".
                  " WHERE correlations.interval_no=intervals.interval_no".
                  " AND scale_no=".$dbt->dbh->quote($scale_no). 
                  " AND next_interval_no=0";
        my @results = @{$dbt->getData($sql)};
        while (scalar(@results)) {
            if ($count++ > 200) { die "infinite loop in getScaleOrder"; }
            my $row = $results[0];
            if ($return_type eq 'number') {
                push @scale_list, $row->{'interval_no'};
            } else {
                if ($row->{'eml_interval'}) {
                    push @scale_list, $row->{'eml_interval'} . ' ' .$row->{'interval_name'};
                } else {
                    push @scale_list, $row->{'interval_name'};
                }
            }
            $sql = "SELECT eml_interval, interval_name, correlations.interval_no FROM correlations, intervals".
                   " WHERE correlations.interval_no=intervals.interval_no".
                   " AND scale_no=".$dbt->dbh->quote($scale_no). 
                   " AND next_interval_no=$row->{interval_no}";
            @results = @{$dbt->getData($sql)};
        }
    }
        
    return @scale_list;
}

sub lookupIntervals {
    my ($dbt,$intervals,$fields) = @_;
    
    my @fields = ('interval_name','period_name','epoch_name','stage_name','ten_my_bin','lower_boundary','upper_boundary');
    if ($fields) {
        @fields = @$fields;
    } 
    my @intervals = @$intervals;

    my @sql_fields;
    my @left_joins;
    foreach my $f (@fields) {
        if ($f eq 'interval_name') {
            push @sql_fields, "TRIM(CONCAT(i1.eml_interval,' ',i1.interval_name)) AS interval_name";
            push @left_joins, "LEFT JOIN intervals i1 ON il.interval_no=i1.interval_no";
        } elsif ($f eq 'period_name') {
            push @sql_fields, "TRIM(CONCAT(i2.eml_interval,' ',i2.interval_name)) AS period_name";
            push @left_joins, "LEFT JOIN intervals i2 ON il.period_no=i2.interval_no";
        } elsif ($f eq 'epoch_name') {
            push @sql_fields, "TRIM(CONCAT(i3.eml_interval,' ',i3.interval_name)) AS epoch_name";
            push @left_joins, "LEFT JOIN intervals i3 ON il.epoch_no=i3.interval_no";
        } elsif ($f eq 'subepoch_name') {
            push @sql_fields, "TRIM(CONCAT(i4.eml_interval,' ',i4.interval_name)) AS subepoch_name";
            push @left_joins, "LEFT JOIN intervals i4 ON il.subepoch_no=i4.interval_no";
        } elsif ($f eq 'stage_name') {
            push @sql_fields, "TRIM(CONCAT(i5.eml_interval,' ',i5.interval_name)) AS stage_name";
            push @left_joins, "LEFT JOIN intervals i5 ON il.stage_no=i5.interval_no";
        } else {
            push @sql_fields, 'il.'.$f;
        }
    }
   
    my $sql = "SELECT il.interval_no,".join(",",@sql_fields)." FROM interval_lookup il ".join(" ",@left_joins);
    if (@intervals) {
        $sql .= " WHERE il.interval_no IN (".join(", ",@intervals).")";
    }
    my @results = @{$dbt->getData($sql)};
    my %interval_table = ();
    foreach my $row (@results) {
        $interval_table{$row->{'interval_no'}} = $row;
    }

    return \%interval_table;
    
}

sub generateLookupTable {
    my $dbt = shift;
    my $dbh = $dbt->dbh;

    my $period_lookup = TimeLookup::processScaleLookup($dbh,$dbt,'69','intervalToInterval');
    my $epoch_lookup  = TimeLookup::processScaleLookup($dbh,$dbt,'71','intervalToInterval');
    my $subepoch_lookup  = TimeLookup::processScaleLookup($dbh,$dbt,'72','intervalToInterval');
    my $stage_lookup  = TimeLookup::processScaleLookup($dbh,$dbt,'73','intervalToInterval');
    my $bin_lookup = processBinLookup($dbh,$dbt,"binning");
    my ($ub_lookup,$lb_lookup) =  TimeLookup::findBoundaries($dbh,$dbt);
    my $sql = "SELECT interval_no FROM intervals";
    my @results = @{$dbt->getData($sql)};
    foreach my $row (@results) {
        my $period_no = $dbh->quote($period_lookup->{$row->{'interval_no'}});
        my $subepoch_no = $dbh->quote($subepoch_lookup->{$row->{'interval_no'}});
        my $epoch_no = $dbh->quote($epoch_lookup->{$row->{'interval_no'}});
        my $stage_no = $dbh->quote($stage_lookup->{$row->{'interval_no'}});
        my $ten_my_bin = $dbh->quote($bin_lookup->{$row->{'interval_no'}});
        my $ub = $dbh->quote($ub_lookup->{$row->{'interval_no'}});
        my $lb = $dbh->quote($lb_lookup->{$row->{'interval_no'}});
        my $sql = "REPLACE INTO interval_lookup(interval_no,ten_my_bin,stage_no,subepoch_no,epoch_no,period_no,lower_boundary,upper_boundary) VALUES ($row->{interval_no},$ten_my_bin,$stage_no,$subepoch_no,$epoch_no,$period_no,$lb,$ub)";
        $dbh->do($sql);
    }
}

sub getMaxMinArrays {
    $dbh = shift;
    $dbt = shift;
    if (!%immediatemax) {
        &findImmediateCorrelates();
    }

    return (\%immediatemax,\%immediatemin);
}

1;
