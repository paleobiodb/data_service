# 
# DiversityData
# 
# A role that returns information from the PaleoDB database about a the
# taxonomic diversity of occurrences.  This is a subordinate role to
# OccurrenceData.pm.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::DiversityData;

use TaxonDefs qw(@TREE_TABLE_LIST %TAXON_TABLE %TAXON_RANK %RANK_STRING);
use Taxonomy;

use Moo::Role;


sub generate_diversity_table {

    my ($self, $sth, $options) = @_;
    
    my $ds = $self->ds;
    my $dbh = $self->get_connection;
    
    # First figure out which timescale (and thus which list of intervals) we
    # will be using in order to bin the occurrences.  We will eventually add
    # other scale options than 1.  If no resolution is specified, use the
    # maximum resolution of the selected scale.
    
    my $scale_no = $options->{scale_no} || 1;
    my $scale_level = $options->{timereso} || $PB2::IntervalData::SCALE_DATA{$scale_no}{levels};
    
    my $debug_mode = $self->debug;
    
    # Figure out the parameters to use in the binning process.
    
    my $timerule = $options->{timerule};
    my $timebuffer = $options->{timebuffer};
    
    # Declare variables to be used in this process.
    
    my $intervals = $PB2::IntervalData::INTERVAL_DATA{$scale_no};
    my $boundary_list = $PB2::IntervalData::BOUNDARY_LIST{$scale_no}{$scale_level};
    my $boundary_map = $PB2::IntervalData::BOUNDARY_MAP{$scale_no}{$scale_level};
    
    my ($starting_age, $ending_age, %taxon_first, %taxon_last, %occurrences, %unique_in_bin);
    my ($total_count, $imprecise_time_count, $imprecise_taxon_count, $missing_taxon_count, $bin_count);
    my (%imprecise_interval, %imprecise_taxon);
    my (%interval_report, %taxon_report);
    
    # Get the age bounds (if any) that were specified for this process.
    
    my $interval_no = $self->clean_param('interval_id');
    my $interval_name = $self->clean_param('interval');
    my $min_ma = $self->clean_param('min_ma');
    my $max_ma = $self->clean_param('max_ma');
    
    my ($early_limit, $late_limit);
    
    if ( $interval_no )
    {
	my $no = $interval_no + 0;
	my $sql = "SELECT early_age, late_age FROM interval_data WHERE interval_no = $no";
	
	my ($max_ma, $min_ma) = $dbh->selectrow_array($sql);
	
	unless ( $max_ma )
	{
	    $early_limit = 0;
	    $late_limit = 0;
	    $self->add_warning("unknown interval id '$interval_no'");
	}
	
	else
	{
	    $early_limit = $max_ma;
	    $late_limit = $min_ma;
	}
    }
    
    elsif ( $interval_name )
    {
	my $name = $dbh->quote($interval_name);
	my $sql = "SELECT early_age, late_age FROM interval_data WHERE interval_name like $name";
	
	my ($max_ma, $min_ma) = $dbh->selectrow_array($sql);
	
	unless ( $max_ma )
	{
	    $early_limit = 0;
	    $late_limit = 0;
	    $self->add_warning("unknown interval '$interval_name'");
	}
	
	else
	{
	    $early_limit = $max_ma;
	    $late_limit = $min_ma;
	}
    }
    
    elsif ( $min_ma || $max_ma )
    {
	$early_limit = $max_ma + 0 if $max_ma;
	$late_limit = $min_ma + 0;
    }
    
    # Now scan through the occurrences.  We cache the lists of matching
    # intervals from the selected scale, under the name of the interval(s)
    # recorded for the occurrence (which may or may not be in the standard
    # timescale).
    
    my (%interval_cache);
    
 OCCURRENCE:
    while ( my $r = $sth->fetchrow_hashref )
    {
	$total_count++;
	
	# Start by figuring out the interval(s) in which to bin this
	# occurrence.  Depending upon the value of $timerule, there may be
	# more than one.
	
	# The first step is to compute the key under which to cache lists of
	# matching intervals.
	
	my $interval_key = $r->{early_name} || 'UNKNOWN';
	$interval_key .= '-' . $r->{late_name}
	    if defined $r->{late_name} && defined $r->{early_name} && $r->{late_name} ne $r->{early_name};
	
	# If we have already figured out which intervals match this, we're
	# done.  Otherwise, we must do this computation.
	
	my $bins = $interval_cache{$interval_key};
	
	my $occ_early = $r->{early_age} + 0;
	my $occ_late = $r->{late_age} + 0;
	
	unless ( $bins )
	{
	    $bins = $interval_cache{$interval_key} = [];
	    
	    # Scan the entire list of intervals for the selected timescale,
	    # looking for those that match according to the value of
	    # $timerule.
	    
	INTERVAL:
	    foreach my $early_bound ( @$boundary_list )
	    {
		# Skip all intervals that fall below the lower limit specified
		# by the request parameters.
		
		next INTERVAL if defined $early_limit && $early_bound > $early_limit;
		
		# Skip all intervals that do not overlap with the occurrence
		# range, and stop the scan when we have passed that range.
		
		last INTERVAL if $early_bound <= $occ_late;
		
		my $int = $boundary_map->{$early_bound};
		my $late_bound = $int->{late_age};
		
		next INTERVAL if $late_bound >= $occ_early;
		
		next INTERVAL if defined $late_limit && $late_bound < $late_limit;
		
		# Skip any interval that is not selected by the specified
		# timerule.  Note that the 'overlap' timerule includes
		# everything that overlaps.
		
		if ( $timerule eq 'contain' )
		{
		    last INTERVAL if $occ_early > $early_bound || $occ_late < $late_bound;
		}
		
		elsif ( $timerule eq 'major' )
		{
		    my $overlap;
		    
		    if ( $occ_late >= $late_bound )
		    {
			if ( $occ_early <= $early_bound )
			{
			    $overlap = $occ_early - $occ_late;
			}
			
			else
			{
			    $overlap = $early_bound - $occ_late;
			}
		    }
		    
		    elsif ( $occ_early > $early_bound )
		    {
			$overlap = $early_bound - $late_bound;
		    }
		    
		    else
		    {
			$overlap = $occ_early - $late_bound;
		    }
		    
		    next INTERVAL if $occ_early != $occ_late && $overlap / ($occ_early - $occ_late) < 0.5;
		}
		
		elsif ( $timerule eq 'buffer' )
		{
		    my $buffer = $timebuffer || ($early_bound > 66 ? 12 : 5);
		    
		    next INTERVAL if $occ_early > $early_bound + $buffer || 
			$occ_late < $late_bound - $buffer;
		}
		
		# If we are not skipping this interval, add it to the list.
		
		push @$bins, $early_bound;
		
		# If we are using timerule 'major' or 'contains', then stop
		# the scan because each occurrence gets assigned to only one
		# bin. 
		
		last INTERVAL if $timerule eq 'contains' || $timerule eq 'major';
	    }
	}
	
	# If we did not find at least one bin to assign this occurrence to,
	# report that fact and go on to the next occurrence.
	
	unless ( @$bins )
	{
	    $imprecise_time_count++;
	    $imprecise_interval{$interval_key}++;
	    if ( $debug_mode )
	    {
		$interval_key .= " [$occ_early - $occ_late]";
		$interval_report{'0 IMPRECISE <= ' . $interval_key}++;
	    }
	    next OCCURRENCE;
	}

	# Otherwise, count this occurrence in each selected bin.  Then adjust
	# the range of bins that we are reporting to reflect this occurrence.
	
	foreach my $b ( @$bins )
	{
	    $occurrences{$b}++;
	    $bin_count++;
	}
	
	$starting_age = $bins->[0] unless defined $starting_age && $starting_age >= $bins->[0];
	$ending_age = $bins->[-1] unless defined $ending_age && $ending_age <= $bins->[-1];
	
	# If we are in debug mode, also count it in the %interval_report hash.
	
	if ( $debug_mode )
	{
	    my $report_key = join(',', @$bins) . ' <= ' . $interval_key . " [$occ_early - $occ_late]";
	    $interval_report{$report_key}++;
	}
	
	# Now check to see if the occurrence is taxonomically identified
	# precisely enough to count further.
	
	my $taxon_no = $r->{taxon1};
	
	unless ( $taxon_no )
	{
	    $taxon_report{$r->{genus_name}}++;
	    
	    if ( !defined $r->{rank} || $r->{rank} > $options->{count_rank} )
	    {
		$imprecise_taxon_count++;
	    }
	    else
	    {
		$missing_taxon_count++;
	    }
	    
	    next;
	}
	
	# If this is the oldest occurrence of the taxon that we have found so
	# far, mark it as originating in the first (oldest) matching bin.
	
	unless ( defined $taxon_first{$taxon_no} && $taxon_first{$taxon_no} >= $bins->[0] )
	{
	    $taxon_first{$taxon_no} = $bins->[0];
	}
	
	# If this is the youngest occurrence of the taxon that we have found
	# so far, mark it as ending in the last (youngest) matching bin.
	
	unless ( defined $taxon_last{$taxon_no} && $taxon_last{$taxon_no} <= $bins->[-1] )
	{
	    $taxon_last{$taxon_no} = $bins->[-1];
	}
	
	# If the 'use_recent' option was given, and the taxon is known to be
	# extant, then mark it as ending at the present (0 Ma).
	
	if ( $options->{use_recent} && $r->{is_extant} )
	{
	    $taxon_last{$taxon_no} = 0;
	}
	
	# Now count the taxon in each selected bin.
	
	foreach my $b ( @$bins )
	{
	    $unique_in_bin{$b}{$taxon_no} ||= 1;
	}
    }
    
    # At this point we are done scanning the occurrence list.  Unless
    # $starting_age has a value, we don't have any results.
    
    unless ( $starting_age )
    {
	return;
    }
    
    # Now we need to compute the four diversity statistics defined by Foote:
    # XFt, XFL, XbL, Xbt.  So we start by running through the bins and
    # initializing the counts.  We also keep track of all the bins between
    # $starting_age and $ending_age.
    
    my (%X_Ft, %X_FL, %X_bL, %X_bt);
    my (@bins, $is_last);
    
    foreach my $age ( @$boundary_list )
    {
	next if $age > $starting_age;
	last if $age < $ending_age;
	
	push @bins, $age;
	
	$X_Ft{$age} = 0;
	$X_FL{$age} = 0;
	$X_bL{$age} = 0;
	$X_bt{$age} = 0;
    }
    
    # Then we scan through the taxa.  For each one, we scan through the bins
    # from the taxon's origination to its ending and mark the appropriate
    # counts.  This step takes time o(MN) where M is the number of taxa and N
    # the number of intervals.
    
    foreach my $taxon_no ( keys %taxon_first )
    {
	my $first_bin = $taxon_first{$taxon_no};
	my $last_bin = $taxon_last{$taxon_no};
	
	# If the interval of first appearance is the same as the interval of
	# last appearance, then this is a singleton.
	
	if ( $first_bin == $last_bin )
	{
	    $X_FL{$first_bin}++;
	    next;
	}
	
	# Otherwise, we mark the bin where the taxon starts and the bin where
	# it ends, and then scan through the bins between to mark
	# rangethroughs.
	
	$X_Ft{$first_bin}++;
	$X_bL{$last_bin}++;
	
	foreach my $bin (@bins)
	{
	    last if $bin <= $last_bin;
	    $X_bt{$bin}++ if $bin < $first_bin;
	}
    }
    
    # If we are in debug mode, report the interval assignments.
    
    # if ( $self->debug ) 
    # {
    # 	# $self->add_warning("Skipped $imprecise_time_count occurrences because of imprecise temporal locality:")
    # 	#     if $imprecise_time_count;
	
    # 	# foreach my $key ( sort { $b cmp $a } keys %interval_report )
    # 	# {
    # 	#     $self->add_warning("    $key ($interval_report{$key})");
    # 	# }
	
    # 	foreach my $key ( sort { $a cmp $b } keys %taxon_report )
    # 	{
    # 	    $self->add_warning("    $key ($taxon_report{$key})");
    # 	}
    # }
    
    # Add a summary record with counts.
    
    $self->summary_data({ total_count => $total_count,
			  bin_count => $bin_count,
			  imprecise_time => $imprecise_time_count,
			  imprecise_taxon => $imprecise_taxon_count,
			  missing_taxon => $missing_taxon_count });
    
    # Now we scan through the bins again and prepare the data records.
    
    my @result;
    
    foreach my $age (@bins)
    {
	my $r = { interval_no => $boundary_map->{$age}{interval_no},
		  interval_name => $boundary_map->{$age}{interval_name},
		  early_age => $age,
		  late_age => $boundary_map->{$age}{late_age},
		  originations => $X_Ft{$age},
		  extinctions => $X_bL{$age},
		  singletons => $X_FL{$age},
		  range_throughs => $X_bt{$age},
		  sampled_in_bin => scalar(keys %{$unique_in_bin{$age}}) || 0,
		  n_occs => $occurrences{$age} || 0 };
	
	push @result, $r;
    }
    
    $self->list_result(reverse @result);
}


# The following variables are visible to all of the subroutines in the
# remainder of this file.  This is done to reduce the number of parameters
# that must be passed to &count_subtaxa and &add_result_record.

my (%occ_node, %taxon_node, %uns_counter, %lower_taxon_nos);

my %uns_name = ( 3 => 'NO_SPECIES_SPECIFIED', 5 => 'NO_GENUS_SPECIFIED',
		 9 => 'NO_FAMILY_SPECIFIED', 13 => 'NO_ORDER_SPECIFIED',
		 0 => 'NO_TAXON_SPECIFIED' );

my %uns_prefix = ( 3 => 'UF', 5 => 'UG', 9 => 'UF', 13 => 'UO', 0 => 'UU' );

sub generate_phylogeny_ints {

    my ($request, $sth) = @_;
    
    my $dbh = $request->get_connection;
    
    # First figure out the level to which we will be resolving the phylogeny.
    
    my $reso_rank = $request->{my_reso_rank} || 9;
    my $count_rank = $request->{my_count_rank} || 5;
    my $promote = $request->{my_promote};
    
    # Initialize the variables necessary for enumerating the phylogeny.
    
    my (%base_taxa, $total_count);
    
    %taxon_node = ();				# visible to called subroutines
    %uns_counter = ();				# visible to called subroutines
    
    # Then go through the occurrences one by one, putting together a tree
    # and counting at the specified taxonomic levels.
    
 OCCURRENCE:
    while ( my $r = $sth->fetchrow_hashref )
    {
	$total_count++;
	
	# First pin down the various tree levels.
	
	my $rank = $r->{rank};
	
	# Then create any taxon nodes that don't already exist, and increment
	# the occurrence counts at all levels.
	
	my $class_no = $r->{class_no} || uns_identifier(17);
	$base_taxa{$class_no} = 1;
	
	my $class_node = $taxon_node{$class_no} //= { taxon_rank => 17, occs => 0, 
						      taxon_name => $r->{class} };
	
	no warnings 'numeric';
	
	if ( $rank <= 13 )
	{
	    my $order_no = $r->{order_no} || $class_node->{uns} || ($class_node->{uns} = uns_identifier(13));
	    my $order_node = $taxon_node{$order_no} //= { taxon_rank => 13, occs => 0, 
							  taxon_name => $r->{order} };
	    
	    $order_node->{is_uns} = 1 unless $order_no > 0;
	    $class_node->{chld}{$order_no} = 1;
	    
	    if ( $count_rank <= 9 && $rank <= 9 )
	    {
		my $family_no = $r->{family_no} || $order_node->{uns} || ($order_node->{uns} = uns_identifier(9));
		my $family_node = $taxon_node{$family_no} //= { taxon_rank => 9, occs => 0, 
								taxon_name => $r->{family} };
		
		$family_node->{is_uns} = 1 unless $family_no > 0;
		$order_node->{chld}{$family_no} = 1;
		
		if ( $count_rank <= 5 && $rank <= 5 )
		{
		    my ($genus_no, $genus_name);
		    if ( $promote && $r->{subgenus_no} )
		    {
			$genus_no = $r->{subgenus_no} || $family_node->{uns} || ($family_node->{uns} = uns_identifier(5));
			$genus_name = $r->{subgenus};
		    }
		    else
		    {
			$genus_no = $r->{genus_no} || $family_node->{uns} || ($family_node->{uns} = uns_identifier(5));
			$genus_name = $r->{genus};
		    }
		    my $genus_node = $taxon_node{$genus_no} //= { taxon_rank => 5, occs => 0, 
								  taxon_name => $genus_name };
		    
		    $genus_node->{is_uns} = 1 unless $genus_no > 0;
		    $family_node->{chld}{$genus_no} = 1;
		    
		    if ( $count_rank <= 3 && $rank <= 3 )
		    {
			my $species_no = $r->{species_no} || $genus_node->{uns} || ($genus_node->{uns} = uns_identifier(5));
			my $species_node = $taxon_node{$species_no} //= { taxon_rank => 3, occs => 0, 
									  taxon_name => $r->{species} };
			
			$species_node->{is_uns} = 1 unless $species_no > 0;
			$genus_node->{chld}{$species_no} = 1;
			$species_node->{occs}++;
		    }
		    
		    else
		    {
			$genus_node->{occs}++;
		    }
		}
		
		else
		{
		    $family_node->{occs}++;
		}
	    }
	    
	    else
	    {
		$order_node->{occs}++;
	    }
	}
	
	else
	{
	    $class_node->{occs}++;
	}
    }
    
    # If we were asked for additional taxonomic information, fill that in.
    
    if ( $request->{my_attr} )
    {
	$request->{my_taxonomy} ||= Taxonomy->new($dbh, 'taxon_trees');
	$request->get_taxon_info(\%taxon_node);
    }
    
    # Now that we have the occurrence counts and any additional taxonomic
    # information that was requested, recursively traverse the tree and fill
    # in the counts (number of occurrences, species, genera, etc.) for each
    # node.
    
    my $check_count = 0;
    
    foreach my $class_no ( keys %base_taxa )
    {
	$request->count_tree($class_no);
	$check_count += $taxon_node{$class_no}{occs};
    }
    
    unless ( $total_count == $check_count )
    {
	my $deficit = $total_count - $check_count;
	$request->add_warning("Something went wrong.  $deficit occurrences were missed.");
    }
    
    # Now traverse the tree again and produce the appropriate output.  If
    # there is more than one base taxon (class), then output them in sorted
    # order as separate trees.
    
    my (@sorted_classes) = sort { ($taxon_node{$a}{taxon_name} || '~') cmp 
				  ($taxon_node{$b}{taxon_name} || '~') } keys %base_taxa;
    
    foreach my $base_no ( @sorted_classes )
    {
	# If the root of the tree has only one child, skip down until we get
	# to a node with more than one child.
	
	while ( keys %{$taxon_node{$base_no}{chld}} == 1 )
	{
	    ($base_no) = keys %{$taxon_node{$base_no}{chld}};
	}
	
	# Then output the result records for this base taxon and all of its
	# subtaxa.
	
	$request->add_result_records($base_no, 0);
    }
}


sub generate_phylogeny_full {

    my ($request, $sth, $base_taxa) = @_;
    
    my $dbh = $request->get_connection;
    
    $request->{my_taxonomy} ||= Taxonomy->new($dbh, 'taxon_trees');
    
    # First determine the root of the subtree we are analyzing, or throw an
    # exception. 
    
    my (%base_taxa, %added_taxa);
    
    foreach my $t ( @$base_taxa )
    {
	$base_taxa{$t->{orig_no}} = 1;
    }
    
    die "400 You must specify one or more base taxa using the parameter 'base_name' or 'base_id'\n"
	unless keys %base_taxa;
    
    my $uns_no = uns_identifier(0);
    
    # Then figure out the level to which we will be resolving the phylogeny.
    
    my $reso_rank = $request->{my_reso_rank} || 9;
    my $count_rank = $request->{my_count_rank} || 5;
    my $promote = $request->{my_promote};
    
    # Initialize the variables necessary for enumerating the phylogeny.
    
    my ($total_count);
    
    %taxon_node = ();			# visible to called subroutines
    %uns_counter = ();			# visible to called subroutines
    
    # Get the taxonomic hierarchy for the specified base taxa.  We will be
    # filling in child and occurrence information below.
    
    foreach my $base_no ( keys %base_taxa )
    {
	$request->get_upper_taxa($base_no);
    }
    
    # Then go through the occurrences one by one, putting together a tree
    # and counting at the specified taxonomic levels.
    
 OCCURRENCE:
    while ( my $r = $sth->fetchrow_hashref )
    {
	$total_count++;
	
	# Start with the taxonomic rank of this occurrence.
	
	my $rank = $r->{rank};
	
	# Then create any taxon nodes that don't already exist, and increment
	# the occurrence counts at all levels.
	
	my $higher_no = $r->{ints_no} || $uns_no;
	my $higher_node = $taxon_node{$higher_no};
	
	unless ( $higher_node )
	{
	    $higher_node = $taxon_node{$higher_no} = { taxon_rank => 0, occs => 0 };
	    $added_taxa{$higher_no} = 1;
	}
	
	if ( $rank <= 5 && $count_rank <= 5 )
	{
	    my ($genus_no, $genus_name);
	    if ( $promote && $r->{subgenus_no} )
	    {
		$genus_no = $r->{subgenus_no};
		$genus_name = $r->{subgenus} || '~';
	    }
	    else
	    {
		$genus_no = $r->{genus_no} || uns_identifier(5);
		$genus_name = $r->{genus} || '~';
	    }
	    my $genus_node = $taxon_node{$genus_no};
	    
	    unless ( $genus_node )
	    {
		$genus_node = $taxon_node{$genus_no} = { taxon_rank => 5, occs => 0,
							 taxon_name => $genus_name };
		$added_taxa{$genus_no} = 1 if $reso_rank <= 5;
	    }
	    
	    $higher_node->{chld}{$genus_no} = 1;
	    
	    if ( $count_rank <= 3 && $rank <= 3 )
	    {
		my $species_no = $r->{species_no} || uns_identifier(3);
		my $species_node = $occ_node{$species_no};
		
		unless ( $species_node )
		{
		    $species_node = $taxon_node{$species_no} = { taxon_rank => 3, occs => 0,
								 taxon_name => $r->{species} || '~' };
		    $added_taxa{$species_no} = 1 if $reso_rank <= 3;
		}
		
		$species_node->{occs}++;
		$genus_node->{chld}{$species_no} = 1;
	    }
	    
	    else
	    {
		$genus_node->{occs}++;
	    }
	}
	
	else
	{
	    $higher_node->{occs}++;
	}
    }
    
    # Fetch the full taxonomic information for all of the taxa added above.
    
    if ( keys %added_taxa )
    {
	$request->get_taxon_info(\%added_taxa);
    }
    
    # Now go through our base taxa recursively again and count subtaxa.
    
    foreach my $base_no ( keys %base_taxa )
    {
	$request->count_taxa($base_no);
    }
    
    # Now traverse the tree again and produce the appropriate output.
    
    my (@sorted_classes) = sort { ($taxon_node{$a}{taxon_name} // '~') cmp ($taxon_node{$b}{taxon_name} // '~') } keys %base_taxa;
    
    foreach my $class_no ( @sorted_classes )
    {
	$request->add_result_records($class_no, 0);
    }
    
    # Now add a summary record.
    
    $request->summary_data({ total_count => $total_count });
}


# uns_identifier ( rank )
# 
# Return an 'unspecified taxon' identifier for the given rank.  These are used
# as placeholders to reprsent taxa which are missing from our hierarchy.

sub uns_identifier {

    my ($rank) = @_;
    
    $uns_counter{$rank}++;
    return $uns_prefix{$rank} . $uns_counter{$rank};
}


# get_upper_taxa ( base_no, options )
# 
# Fetch the taxonomic hierarchy rooted at the given taxon number, but only
# taxa above the genus level.  The retrieved records are stored in
# %taxon_node, with orig_no as key.

sub get_upper_taxa {
    
    my ($request, $base_no, $options) = @_;
    
    return unless $base_no > 0;
    
    # Get a list of all the taxa in the specified subtree, above the rank of
    # genus.  If the option 'app' was specified, include the first-and-last
    # appearance info.
    
    my @fields = ('SIMPLE', 'family_no');
    push @fields, 'ATTR' if $request->{my_attr};
    
    my $taxa_list;
    
    eval {
	$taxa_list = $request->{my_taxonomy}->list_subtree($base_no, { min_rank => 6, 
								       fields => \@fields, 
								       return => 'listref' });
    };
    
    #print STDERR "$Taxonomy::SQL_STRING\n\n" if $request->debug;
    
    # If no taxa were returned, then the base taxon is probably a genus or
    # below.  So just fetch that single record, so that we at least have
    # something. 
    
    unless ( @$taxa_list )
    {
	$taxa_list = $request->{my_taxonomy}->list_taxa($base_no, { fields => \@fields, 
								    return => 'listref' });
    }
    
    # Now go through the list.  When we find a taxon whose info we have not
    # yet gotten, just put the record into the $taxon_node hash.  Otherwise,
    # copy the relevant info.
    
    foreach my $r ( @$taxa_list )
    {
	my $taxon_no = $r->{orig_no};
	my $parent_no = $r->{parsen_no};
	
	$taxon_node{$taxon_no} = $r;
	
	# Hook this node up to its parent, which will already be in the
	# %taxon_node hash because &list_subtree retrieves nodes in
	# tree-sequence order.
	
	$taxon_node{$parent_no}{chld}{$taxon_no} = 1 if $taxon_node{$parent_no};
	
	# If we were asked for attribution information, compute that now.
	
	$r->{attribution} = $request->generateAttribution($r) if $request->{my_attr};
    }
}


# get_taxon_info ( taxon_nos, options )
# 
# Fetch records representing the taxa identified by the specified set of
# taxon_no values.  Fill in the requested information into the corresponding
# records in %taxon_node.

sub get_taxon_info {
    
    my ($request, $taxon_nos) = @_;
    
    # Get a list of the specified taxa.  If the option 'app' was specified,
    # include the first-and-last appearance info.
    
    my @fields = 'SIMPLE';
    push @fields, 'ATTR' if $request->{my_attr};
    
    my $taxa_list = [];
    
    eval {
	$taxa_list = $request->{my_taxonomy}->list_taxa($taxon_nos, { fields => \@fields,
								      return => 'listref' });
    };
    
    print STDERR "$Taxonomy::SQL_STRING\n\n" if $request->debug;
    
    # Now go through the list, and copy the relevant info.
    
    foreach my $r ( @$taxa_list )
    {
	my $taxon_no = $r->{orig_no};
	my $taxon_node = $taxon_node{$taxon_no} // {};
	
	# If the node doesn't give a taxonomic rank, copy over the name and rank.
	
	unless ( $taxon_node->{taxon_rank} )
	{
	    $taxon_node->{taxon_name} = $r->{taxon_name};
	    $taxon_node->{taxon_rank} = $r->{taxon_rank};
	}
	
	# If we were asked for attribution information, compute that now.
	
	if ( $request->{my_attr} )
	{
	    $taxon_node->{attribution} = $request->generateAttribution($r);
	}
    }
}


# count_taxa ( node )
# 
# This function recursively counts occurrences and taxa in all of the subnodes
# of the given node, and adds up the totals.  It assumes that all of the
# relevant information is in %taxon_node, which is declared above.

sub count_taxa {
    
    my ($request, $node_no) = @_;
    
    my $node = $taxon_node{$node_no};
    
    $node->{touched} = 1;
    $node->{tree_occs} = $node->{occs};
    $node->{occs} ||= 0;
    $node->{orders} = 0;
    $node->{families} = 0 if $request->{my_count_rank} <= 9;
    $node->{genera} = 0 if $request->{my_count_rank} <= 5;
    $node->{species} = 0 if $request->{my_count_rank} <= 3;
    
    # Recurse through the taxonomic hierarchy represented by the 'chld'
    # field.
    
    if ( ref $node->{chld} )
    {
	foreach my $child_no ( keys %{$node->{chld}} )
	{
	    my $child_node = $taxon_node{$child_no};
	    next unless $child_node->{taxon_rank};
	    next if $child_node->{touched};
	    
	    # Count up each child node in turn.  Skip those which have no
	    # occurrences.
	    
	    $request->count_taxa($child_no);
	    
	    next unless $child_node->{tree_occs};
	    
	    # For those which do, add the occurrence and subtaxon counts to
	    # the corresponding counts in the current node.
	    
	    $request->sum_subtaxa($node, $child_node);
	}
    }
    
    my $a = 1;	# we can stop here when debugging
}


# sub count_subtaxa_full {
    
#     my ($request, $node_no) = @_;
    
#     no warnings 'uninitialized';
#     no warnings 'numeric';
    
#     # Initialize this taxon node for counting.
    
#     my $node = $taxon_node{$node_no};
    
#     $node->{touched} = 1;
#     $node->{tree_occs} = $node->{occs};
#     $node->{orders} = 0;
#     $node->{families} = 0 if $request->{my_count_rank} <= 9;
#     $node->{genera} = 0 if $request->{my_count_rank} <= 5;
#     $node->{species} = 0 if $request->{my_count_rank} <= 3;
    
#     $node->{family_ok} = 1 if $node->{family_no} || $node->{taxon_rank} == 9;
    
#     my @uns_children;
    
#     # Recurse through the taxonomic hierarchy represented by the 'chld'
#     # field.
    
#     if ( ref $node->{chld} )
#     {
#     CHILD:
# 	foreach my $child_no ( keys %{$node->{chld}} )
# 	{
# 	    # Count each child node recursively.  A child node has the
# 	    # 'family_ok' flag set if its parent does, because this means that
# 	    # we have encountered a family node on our path down the tree so
# 	    # far. 
	    
# 	    my $child_node = $taxon_node{$child_no};
# 	    next if $child_node->{touched};
	    
# 	    $child_node->{family_ok} = 1 if $node->{family_ok};
	    
# 	    $request->count_subtaxa_full($child_no);
	    
# 	    # Skip those that do not have any occurrences.
	    
# 	    next CHILD unless $child_node->{occs};
	    
# 	    # For those children that do have occurrences, add their subtaxon
# 	    # counts to those of the current node.
	    
# 	    $request->sum_subtaxa($node, $child_node);
	    
# 	    # If we are using the partial taxonomy, then we are done.
	    
# 	    # next CHILD unless $full_taxonomy;
	    
# 	    # Add the child's occurrence counts to the parent.
	    
# 	}
#     }
    
#     my $a = 1;	# we can stop here when debugging
# }


sub sum_subtaxa {
    
    my ($request, $node, $child) = @_;
    
    no warnings 'uninitialized';
    
    # Now add the number of species, genera, etc. counted for the child to the
    # parent node.
    
    my $child_orders = $child->{taxon_rank} == 13 && ! $child->{is_uns}	? 1
		     : $child->{orders}					? $child->{orders}
									: 0;
    
    $node->{orders} += $child_orders if $node->{taxon_rank} > 13;

    if ( $request->{my_count_rank} <= 9 )
    {
	my $child_families = $child->{taxon_rank} == 9 && ! $child->{is_uns} ? 1
			   : $child->{families}				     ? $child->{families}
									     : 0;

	$node->{families} += $child_families if $node->{taxon_rank} > 9;
    }

    if ( $request->{my_count_rank} <= 5 )
    {
	my $child_genera = $child->{taxon_rank} == 5 && ! $child->{is_uns} ? 1
			 : $child->{genera}				   ? $child->{genera}
									   : 0;

	$node->{genera} += $child_genera if $node->{taxon_rank} > 5;
    }

    if ( $request->{my_count_rank} <= 3 )
    {
	my $child_species = $child->{taxon_rank} == 3 && ! $child->{is_uns} ? 1
			  : $child->{species}				    ? $child->{species}
									    : 0;
	
	$node->{species} += $child_species if $node->{taxon_rank} > 3;
    }

    $node->{tree_occs} += $child->{tree_occs} if $child->{tree_occs};
}


sub add_result_records {
    
    my ($request, $taxon_no, $parent_no, $family_ok) = @_;
    
    no warnings 'uninitialized';
    
    my $node = $taxon_node{$taxon_no};
    
    # Skip any taxon for which no occurrences have been recorded.  This will
    # only happen with the full hierarchy.
    
    return unless $node->{tree_occs};
    
    # Skip any taxon that ranks below the specified resolution level.
    
    my $rank = $node->{taxon_rank};
    return if $rank < $request->{my_reso_rank};
    
    # Determine the taxon name, or generate a "NONE_SPECIFIED" name
    # appropriate to the taxon rank.
    
    my $name = $node->{taxon_name};
    $name = $uns_name{$rank || 0} unless $name && $name ne '~';
    
    # If the 'family_no' field is set, or if the rank is 9, then we have found
    # a family.
    
    $family_ok = 1 if $node->{family_no} || $node->{taxon_rank} == 9;
    
    # Create an output record.
    
    my $taxon_record = { taxon_no => $taxon_no,
			 parent_no => $parent_no,
			 taxon_name => $name,
			 taxon_rank => $rank,
			 spec_occs => $node->{occs},
		         n_occs => $node->{tree_occs} };
    
    # Add the appropriate subtaxon counts.
    
    if ( $rank > 13 && $request->{my_count_rank} <= 13 )
    {
	$taxon_record->{n_orders} = $node->{orders};
    }
    
    if ( $rank > 9 && $request->{my_count_rank} <= 9 )
    {
	$taxon_record->{n_families} = $node->{families};
    }
    
    if ( $rank > 5 && $request->{my_count_rank} <= 5 )
    {
	$taxon_record->{n_genera} = $node->{genera};
    }
    
    if ( $rank > 3 && $request->{my_count_rank} <= 3 )
    {
	$taxon_record->{n_species} = $node->{species};
    }
    
    # If we were asked for additional info, add that now.
    
    $taxon_record->{attribution} = $taxon_node{$taxon_no}{attribution}
	if $taxon_node{$taxon_no}{attribution};
    
    # Add this taxon to the result list.
    
    $request->add_result($taxon_record);
    
    # If this is a "leaf taxon" according to the specified resolution level,
    # then stop here.  Otherwise, recurse to the children of this taxon.
    
    return if $rank == $request->{my_reso_rank};
    
    my @children = keys %{$node->{chld}};
    my @deferred;
    
 CHILD:
    foreach my $child_no ( sort { ($taxon_node{$b}{taxon_rank} || 99) <=>
				  ($taxon_node{$a}{taxon_rank} || 99) or
			          ($taxon_node{$a}{taxon_name} || '~') cmp 
				  ($taxon_node{$b}{taxon_name} || '~') } @children )
    {
	my $child_node = $taxon_node{$child_no};
	next unless $child_node->{taxon_rank};
	
	# Now this part is a bit tricky.  If the child node represents a taxon
	# of less than family level, and we haven't encountered a family on
	# our path down the tree so far, then defer processing this child
	# until we have done all the ok ones.
	
	if ( $node->{taxon_rank} > 9 && $child_node->{taxon_rank} > 0 && 
	     $child_node->{taxon_rank} < 9 && ! $node->{family_ok} )
	{
	    push @deferred, $child_no;
	    next CHILD;
	}
	
	# Recursively generate the child records.
	
	$request->add_result_records($child_no, $taxon_no, $family_ok);
    }
    
    # If we have any deferred nodes, then create a new "unspecified family"
    # node and paste it into the tree.  All of the deferred children will be
    # children of this node.
    
    if ( @deferred )
    {
	my $uns_no = uns_identifier(9);
	my $uns_node = $taxon_node{$uns_no} = { taxon_rank => 9, occs => 0, 
						taxon_name => '~' };
	
	$node->{chld}{$uns_no} = 1;
	
	# Now move each of the deferred children under this node, and add up
	# all of the subtaxa and occurrences.
	
	foreach my $child_no ( @deferred )
	{
	    $uns_node->{chld}{$child_no} = 1;
	    delete $node->{chld}{$child_no};
	    
	    $request->sum_subtaxa($uns_node, $taxon_node{$child_no});
	}
	
	# Then recursively generate the output records for this new node.  We
	# set the 'family_ok' flag so that we won't get another "unspecified
	# family" node under this one.
	
	$request->add_result_records($uns_no, $taxon_no, 1);

	my $a = 1;	# we can stop here when debugging
    }
}


# sub generate_prevalence {
    
#     my ($request, $result, $limit, $detail) = @_;
    
#     no warnings 'uninitialized';
    
#     my (@processed, %exclude);
    
#     if ( ref $request->{my_base_taxa} eq 'ARRAY' )
#     {
#     A:
# 	while ( @processed )
# 	{
# 	    foreach my $t (@{$request->{my_base_taxa}})
# 	    {
# 		if ( $processed[0]{lft} <= $t->{lft} && $processed[0]{rgt} >= $t->{rgt} )
# 		{
# 		    shift @processed;
# 		    next A;
# 		}
# 	    }
	    
# 	    last A;
# 	}
#     }
    
#     # if ( $detail == 2 )
#     # {
#     # 	shift @$result while $result->[0]{rank} > 17;
#     # }
    
#     # elsif ( $detail == 3 )
#     # {
#     # 	shift @$result while $result->[0]{rank} > 13;
#     # }
    
#  RECORD:
#     foreach my $r (@$result)
#     {
# 	next RECORD if $exclude{$r->{orig_no}};
# 	next RECORD if $detail == 2 && $r->{rank} > 17;
# 	next RECORD if $detail == 3 && $r->{rank} > 13;
	
# 	foreach my $i (@processed)
# 	{
# 	    next RECORD if $r->{lft} >= $i->{lft} && $r->{lft} <= $i->{rgt};
# 	}
	
# 	push @processed, $r;
# 	last if @processed == $limit;
#     }
    
#     $request->list_result(\@processed);
# }


# $$$$ start here !!!

sub generate_prevalence_alt {

    my ($request, $result, $limit, $detail) = @_;
    
    no warnings 'uninitialized';
    
    my $taxonomy = $request->{my_taxonomy};
    
    my (%phylum, %class, %order);
    
    foreach my $r (@$result)
    {
	my $phylum_no = $r->{phylum_no} || 0;
	my $class_no = $r->{class_no} || 0;
	my $order_no = $r->{order_no} || 0;
	
	$phylum{$phylum_no} ||= { orig_no => $phylum_no, rank => 20 };
	$phylum{$phylum_no}{n_occs} += $r->{n_occs};
	
	if ( $class_no > 0 )
	{
	    $class{$class_no} ||= { orig_no => $class_no, rank => 17 };
	    $class{$class_no}{n_occs} += $r->{n_occs};
	    $phylum{$phylum_no}{class}{$class_no} = $class{$class_no};
	    
	    if ( $order_no > 0 )
	    {
		$order{$order_no} ||= { orig_no => $order_no, rank => 13 };
		$order{$order_no}{n_occs} += $r->{n_occs};
		$class{$class_no}{order}{$order_no} = $order{$order_no};
	    }
	    
	    else
	    {
		$class{$class_no}{order}{0} ||= { orig_no => 0, rank => 13 };
		$class{$class_no}{order}{0}{n_occs} += $r->{n_occs};
	    }
	}
	
	else
	{
	    $phylum{$phylum_no}{class}{0} ||= { orig_no => 0, rank => 17 };
	    $phylum{$phylum_no}{class}{0}{n_occs} += $r->{n_occs};
	    
	    if ( $order_no > 0 )
	    {
		$order{$order_no} ||= { orig_no => $order_no, rank => 13 };
		$order{$order_no}{n_occs} += $r->{n_occs};
		$phylum{$phylum_no}{class}{0}{order}{$order_no} = $order{$order_no};
	    }
	    
	    else
	    {
		$phylum{$phylum_no}{class}{0}{order}{0} ||= { orig_no => 0, rank => 13 };
		$phylum{$phylum_no}{class}{0}{order}{0}{n_occs} += $r->{n_occs};
	    }
	}
    }
    
    my $dbh = $request->{dbh};
    
    my $orig_string = join(',', grep { $_ > 0 } (keys %phylum, keys %class, keys %order));
    
    $orig_string ||= '0';
    
    my $sql = "
	SELECT orig_no, name, lft, rgt, image_no FROM $taxonomy->{TREE_TABLE}
		JOIN $taxonomy->{ATTRS_TABLE} using (orig_no)
	WHERE orig_no in ($orig_string)";
    
    my $data = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    foreach my $d (@$data)
    {
	my $record = $phylum{$d->{orig_no}} || $class{$d->{orig_no}} || $order{$d->{orig_no}};
	
	$record->{name} = $d->{name};
	$record->{lft} = $d->{lft};
	$record->{rgt} = $d->{rgt};
	$record->{image_no} = $d->{image_no};
    }
    
    # Now we start with a first cut at the result list by taking the phyla in
    # descending order of prevalence (n_occs - number of occurrences).  If
    # there is a phylum "0", then we use the classes it contains instead.
    
    my @list = grep { $_->{orig_no} } values %phylum;
    
    if ( exists $phylum{0} )
    {
	push @list, grep { $_->{orig_no} } values $phylum{0}{class};
    }
    
    @list = sort { $b->{n_occs} <=> $a->{n_occs} } @list;
    
    # Now that the basic list has been sorted we determine the threshold of
    # occurrences above which an entry remains on the list, according to the
    # requested number of results.
    
    my $threshold = $list[$limit-1]{n_occs};
    my $length = scalar(@list);
    my $deficit = $limit > $length ? $limit - $length : 0;
    
    # We then go through the list and alter some of the entries.  Any entry
    # with exactly one 'class' sub-entry gets replaced by its sub-entry
    # (assuming its class number is not zero).  If 'Chordata' appears as an
    # entry, it is split if at least one of its children would remain in the
    # list.
    
    my @subelements;
    
    for (my $i = 0; $i < @list; $i++)
    {
	my @subs;
	my $count = 0;
	
	if ( $list[$i]{name} eq 'Chordata' && ref $list[$i]{class} eq 'HASH' )
	{
	    foreach my $n ( keys %{$list[$i]{class}} )
	    {
		unless ( $n )
		{
		    $list[$i]{class}{$n}{name} = 'Chordata (other)';
		    $list[$i]{class}{$n}{image_no} = $list[$i]{image_no};
		}
		push @subs, $list[$i]{class}{$n};
		$count++ if $list[$i]{class}{$n}{n_occs} > $threshold;
	    }
	    
	    if ( $count > 1 )
	    {
		push @subelements, @subs;
		splice(@list, $i, 1);
		$i--;
	    }
	}
    }
    
    @list = sort { $b->{n_occs} <=> $a->{n_occs} } @list, @subelements;
    @subelements = ();
	
    # Now, if we go through the list one more time.  If any element is such
    # that splitting it would not cause any elements to move past the given
    # limit on the result list, then split it.
    
 ELEMENT:
    for (my $i = 0; $i < $limit && $i < @list; $i++)
    {
	my $name = $list[$i]{name};
	my $rank = $list[$i]{rank};
	my $subkey = $rank eq '17' ? 'order' : 'class';
	
	next unless ref $list[$i]{$subkey} eq 'HASH';
	next unless $rank eq '20';
	
	my @subs = keys %{$list[$i]{$subkey}};
	my $count = 0;
	
	if ( @subs == 1 )
	{
	    if ( $list[$i]{$subkey}{$subs[0]}{name} )
	    {
		$list[$i] = $list[$i]{$subkey}{$subs[0]};
	    }
	    next ELEMENT;
	}
	
	foreach my $n (@subs)
	{
	    if ( $list[$i]{$subkey}{$n}{n_occs} > $threshold )
	    {
		$count++;
	    }
	    
	    unless ( $list[$i]{$subkey}{$n}{name} )
	    {
		$list[$i]{$subkey}{$n}{name} = "$name (other)";
		$list[$i]{$subkey}{$n}{image_no} = $list[$i]{image_no};
		$list[$i]{$subkey}{$n}{orig_no} = $list[$i]{orig_no};
	    }
	}
	
	next ELEMENT unless $count > 0;
	next ELEMENT if $count > $deficit + 1;
	
	push @subelements, $list[$i]{$subkey}{$_} foreach @subs;
	splice(@list, $i, 1);
	$i--;
    }
    
    @list = sort { $b->{n_occs} <=> $a->{n_occs} } @list, @subelements;
    @subelements = ();    
    
    my @result;
    
    foreach my $r (@list)
    {
	next unless $r->{orig_no} && $r->{image_no};
	push @result, $r;
	last if @result >= $limit;
    }
    
    return $request->list_result(\@result);
    
    #my @keys = sort { $record{$b}{n_occs} <=> $record{$a}{n_occs} } keys %record;
    #my @records = map { $record{$_} } @keys;
    
    #return $request->generate_prevalence(\@records, $limit, $detail);
};


# sub generate_prevalence_old {
    
#     my ($request, $sth, $tree_table) = @_;
    
#     no warnings 'uninitialized';
    
#     my (%n_occs, %rank);
    
#     # First count all of the phyla, classes and orders into which the
#     # occurrences fall.
    
#     while ( my $r = $sth->fetchrow_hashref )
#     {
# 	if ( $r->{phylum_no} && $r->{phylum_no} > 0 )
# 	{
# 	    $n_occs{$r->{phylum_no}} += $r->{n_occs};
# 	    $rank{$r->{phylum_no}} = 20;
# 	}
	
# 	if ( $r->{class_no} && $r->{class_no} > 0 )
# 	{
# 	    $n_occs{$r->{class_no}} += $r->{n_occs};
# 	    $rank{$r->{class_no}} = 17;
# 	}
	
# 	if ( $r->{order_no} && $r->{order_no} > 0 )
# 	{
# 	    $n_occs{$r->{order_no}} += $r->{n_occs};
# 	    $rank{$r->{order_no}} = 13;
# 	}
#     }
    
#     # Then determine the taxa with the highest counts from this list.  If a
#     # query limit has been specified, only return that many items.
    
#     my @taxa = sort { $n_occs{$b} <=> $n_occs{$a} || $rank{$b} <=> $rank{a} } keys %n_occs;
    
#     my $limit;
    
#     if ( $limit = $request->result_limit )
#     {
# 	$limit += $request->result_offset;
# 	splice(@taxa, $limit, 0) if $limit < @taxa;
#     }
    
#     # Construct a query that will retrieve the other necessary information
#     # about these taxa.
    
#     my $taxon_string = join q{,}, sort { $a <=> $b } @taxa;
    
#     my $dbh = $request->get_connection;
#     my $attrs_table = $TAXON_TABLE{$tree_table}{attrs};
#     my $ints_table = $TAXON_TABLE{$tree_table}{ints};
    
#     my $sql = "
# 	SELECT t.orig_no, t.name, t.rank, ph.class_no, ph.phylum_no, v.image_no
# 	FROM $tree_table as t JOIN $ints_table as ph using (ints_no)
# 		LEFT JOIN $attrs_table as v using (orig_no)
# 	WHERE t.orig_no in ($taxon_string)";
    
#     my $result = $dbh->selectall_arrayref($sql, { Slice => {} });
    
#     # Add the occurrence counts.
    
#     if ( ref $result eq 'ARRAY' )
#     {
# 	foreach my $r ( @$result )
# 	{
# 	    $r->{n_occs} = $n_occs{$r->{orig_no}};
# 	    $n_occs{$r->{orig_no}} = $r;
# 	}
#     }
    
#     # Return the result.
    
#     $request->list_result(map { $n_occs{$_} } @taxa);
# }


1;


