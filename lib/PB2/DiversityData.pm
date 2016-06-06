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
use TableDefs qw($INTERVAL_MAP);
use Taxonomy;
use Try::Tiny;
use Carp qw(croak confess);

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
# that must be passed to &count_subtaxa and &add_result_records.

my (%taxon_node, %uns_counter, %lower_taxon_nos);

my %uns_name = ( 3 => 'NO_SPECIES_SPECIFIED', 5 => 'NO_GENUS_SPECIFIED',
		 9 => 'NO_FAMILY_SPECIFIED', 13 => 'NO_ORDER_SPECIFIED',
		 17 => 'NO_CLASS_SPECIFIED', 20 => 'NO_PHYLUM_SPECIFIED',
		 23 => 'NO_KINGDOM_SPECIFIED', 0 => 'NO_TAXON_SPECIFIED' );

my %uns_prefix = ( 3 => 'US', 5 => 'UG', 9 => 'UF', 13 => 'UO', 17 => 'UC',
		   20 => 'UP', 23 => 'NK', 0 => 'UU' );

sub generate_taxon_table_ints {

    my ($request, $sth, $taxon_status) = @_;
    
    my $dbh = $request->get_connection;
    
    $request->{my_taxonomy} ||= Taxonomy->new($dbh, 'taxon_trees');
    
    # First figure out the level to which we will be resolving the phylogeny
    
    my $reso_rank = $request->{my_reso_rank} || 9;
    my $count_rank = $request->{my_count_rank} || 5;
    my $promote = $request->{my_promote};
    
    # Initialize the variables necessary for enumerating the phylogeny.
    
    my (%base_taxa);
    my $total_count = 0;
    my $missing_count = 0;
    
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
	
	# Occurrences with ints_no = 0 cannot be properly displayed, since we
	# have no idea where in the taxonomic hierarchy they are supposed to
	# be.  So count them and otherwise ignore them.
	
	unless ( $r->{ints_no} )
	{
	    $missing_count++;
	    next OCCURRENCE;
	}
	
	# Then create any taxon nodes that don't already exist, and increment
	# the occurrence counts at all levels.
	
	my $base_no = $r->{class_no} || $r->{phylum_no} || uns_identifier(17);
	$base_taxa{$base_no} = 1;
	
	unless ( $taxon_node{$base_no} )
	{
	    if ( $r->{class_no} )
	    {
		$taxon_node{$base_no} = { taxon_rank => 17, n_occs => 0, taxon_name => $r->{class}, is_base => 1 };
	    }
	    
	    else
	    {
		$taxon_node{$base_no} = { taxon_rank => 20, n_occs => 0, taxon_name => $r->{phylum}, is_base => 1 };
	    }
	}
	
	my $base_node = $taxon_node{$base_no};
	
	no warnings 'numeric';
	
	if ( $rank <= 13 )
	{
	    my $order_no = $r->{order_no} || $base_node->{uns} || ($base_node->{uns} = uns_identifier(13));
	    my $order_node = $taxon_node{$order_no} //= { taxon_rank => 13, n_occs => 0, 
							  taxon_name => $r->{order} };
	    
	    $order_node->{is_uns} = 1 unless $order_no > 0;
	    $base_node->{chld}{$order_no} = 1;
	    
	    if ( $count_rank <= 9 && $rank <= 9 )
	    {
		my $family_no = $r->{family_no} || $order_node->{uns} || ($order_node->{uns} = uns_identifier(9));
		my $family_node = $taxon_node{$family_no} //= { taxon_rank => 9, n_occs => 0, 
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
		    my $genus_node = $taxon_node{$genus_no} //= { taxon_rank => 5, n_occs => 0, 
								  taxon_name => $genus_name };
		    
		    $genus_node->{is_uns} = 1 unless $genus_no > 0;
		    $family_node->{chld}{$genus_no} = 1;
		    
		    if ( $count_rank <= 3 && $rank <= 3 )
		    {
			my $species_no = $r->{species_no} || $genus_node->{uns} || ($genus_node->{uns} = uns_identifier(5));
			my $species_node = $taxon_node{$species_no} //= { taxon_rank => 3, n_occs => 0, 
									  taxon_name => $r->{species} };
			
			$species_node->{is_uns} = 1 unless $species_no > 0;
			$genus_node->{chld}{$species_no} = 1;
			$species_node->{n_occs}++;
		    }
		    
		    else
		    {
			$genus_node->{n_occs}++;
		    }
		}
		
		else
		{
		    $family_node->{n_occs}++;
		}
	    }
	    
	    else
	    {
		$order_node->{n_occs}++;
	    }
	}
	
	else
	{
	    $base_node->{n_occs}++;
	}
    }
    
    # If we were asked for additional taxonomic information, fill that in.
    
    $request->get_taxon_info(\%taxon_node);
    
    # Now that we have the occurrence counts and any additional taxonomic
    # information that was requested, recursively traverse the tree and fill
    # in the counts (number of occurrences, species, genera, etc.) for each
    # node.
    
    my $check_count = 0;
    
    foreach my $base_no ( keys %base_taxa )
    {
	$request->count_taxa($base_no);
	$check_count += $taxon_node{$base_no}{n_occs};
    }
    
    unless ( $total_count == $check_count )
    {
	my $deficit = $total_count - $check_count - $missing_count;
	$request->add_warning("Something went wrong.  $deficit occurrences were missed.")
	    if $deficit;
    }
    
    # If filtering options were specified, deal with them now.
    
    my $options = $request->generate_occs_taxa_options;
    
    # Now traverse the tree again and produce the appropriate output.  If
    # there is more than one base taxon (class), then output them in sorted
    # order as separate trees.
    
    my (@sorted_classes) = sort { ($taxon_node{$a}{taxon_name} || '~') cmp 
				  ($taxon_node{$b}{taxon_name} || '~') } keys %base_taxa;
    
    foreach my $base_no ( @sorted_classes )
    {
	# If the root of the tree has only one child, skip down until we get
	# to a node with more than one child.
	
	# while ( keys %{$taxon_node{$base_no}{chld}} == 1 )
	# {
	#     ($base_no) = keys %{$taxon_node{$base_no}{chld}};
	# }
	
	# Then output the result records for this base taxon and all of its
	# subtaxa.
	
	$request->add_result_records($options, $base_no, 0);
    }
    
    # Now add a summary record.
    
    $request->summary_data({ total_count => $total_count,
			     missing_taxon => $missing_count });
}


sub generate_taxon_table_full {

    my ($request, $sth, $taxon_status, $base_taxa) = @_;
    
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
    
    my $track_time = $request->{my_track_time};
    
    # Initialize the variables necessary for enumerating the phylogeny.
    
    my ($total_count);
    
    %taxon_node = ();			# visible to called subroutines
    %uns_counter = ();			# visible to called subroutines
    
    # Get the taxonomic hierarchy for the specified base taxa.  We will be
    # filling in child and occurrence information below.
    
    foreach my $base_no ( keys %base_taxa )
    {
	$request->get_upper_taxa($base_no);
	$taxon_node{$base_no}{is_base} = 1;
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
	    $higher_node = $taxon_node{$higher_no} = { taxon_rank => 0, n_occs => 0 };
	    $added_taxa{$higher_no} = 1;
	}
	
	$request->track_time($higher_node, $r) if $track_time;
	
	if ( $rank <= 5 && $count_rank <= 5 )
	{
	    my ($genus_no, $genus_name, $genus_node, $subgenus_no, $subgenus_name, $subgenus_node);
	    
	    if ( $r->{genus_no} )
	    {
		$genus_no = $r->{genus_no};
		$genus_name = $r->{genus} || '~';
	    }
	    
	    elsif ( $rank == 5 )
	    {
		$genus_no = $r->{orig_no};
		$genus_name = $r->{ident_name} || '~';
	    }
	    
	    if ( $genus_no )
	    {
		$genus_node = $taxon_node{$genus_no};
		
		unless ( $genus_node )
		{
		    $genus_node = $taxon_node{$genus_no} = { taxon_rank => 5, n_occs => 0,
							     taxon_name => $genus_name };
		    $added_taxa{$genus_no} = 1 if $reso_rank <= 5;
		}
		
		$higher_node->{chld}{$genus_no} = 1;
		$request->track_time($genus_node, $r) if $track_time;
	    }
	    
	    if ( $rank <= 4 && $count_rank <= 4 && $genus_no )
	    {
		if ( $r->{subgenus_no} )
		{
		    $subgenus_no = $r->{subgenus_no};
		    $subgenus_name = $r->{subgenus} || '~';
		}
		
		elsif ( $rank == 4 )
		{
		    $subgenus_no = $r->{orig_no};
		    $subgenus_name = $r->{ident_name} || '~';
		}
		
		if ( $subgenus_no )
		{
		    $subgenus_node = $taxon_node{$subgenus_no};
		    
		    unless ( $subgenus_node )
		    {
			$subgenus_node = $taxon_node{$subgenus_no} = { taxon_rank => 4, n_occs => 0,
								       taxon_name => $subgenus_name };
			$added_taxa{$genus_no} = 1 if $reso_rank <= 4;
		    }
		    
		    $genus_node->{chld}{$subgenus_no} = 1;
		    $request->track_time($subgenus_node, $r) if $track_time;
		}
	    }
	    
	    if ( $count_rank <= 3 && $rank <= 3 )
	    {
		my ($species_no, $species_name);
		
		if ( $r->{species_no} )
		{
		    $species_no = $r->{species_no};
		    $species_name = $r->{species} || '~';
		}
		
		else
		{
		    $species_no = $r->{orig_no};
		    $species_name = $r->{ident_name} || '~';
		}
		
		if ( $species_no )
		{
		    my $species_node = $taxon_node{$species_no};
		    
		    unless ( $species_node )
		    {
			$species_node = $taxon_node{$species_no} = { taxon_rank => 3, n_occs => 0,
								     taxon_name => $species_name };
			$added_taxa{$species_no} = 1 if $reso_rank <= 3;
		    }
		    
		    $species_node->{n_occs}++;
		    
		    if ( $subgenus_node )
		    {
			$subgenus_node->{chld}{$species_no} = 1;
		    }
		    
		    elsif ( $genus_node )
		    {
			$genus_node->{chld}{$species_no} = 1;
		    }
		    
		    $request->track_time($species_node, $r) if $track_time;
		}
		
		$subgenus_node->{n_occs}++ if $subgenus_node;
		$genus_node->{n_occs}++ if $genus_node;
	    }
	    
	    else
	    {
		$genus_node->{n_occs}++ if $genus_node;
	    }
	}
	
	else
	{
	    $higher_node->{n_occs}++;
	}
	
	my $a = 1;	# we can stop here when debugging
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
    
    # If we were asked to track time ranges of taxa, then load the
    # corresponding interval names.
    
    # $request->get_interval_info(\%taxon_node);
    
    # If filtering options were specified, deal with them now.
    
    my $options = $request->generate_occs_taxa_options;
    
    # Now traverse the tree again and produce the appropriate output.
    
    my (@sorted_classes) = sort { ($taxon_node{$a}{taxon_name} // '~') cmp ($taxon_node{$b}{taxon_name} // '~') } keys %base_taxa;
    
    foreach my $base_no ( @sorted_classes )
    {
	$request->add_result_records($options, $base_no, 0);
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


# track_time ( node, occ )
# 
# Update the time range for the specified node according to the early_age and
# late_age values in the occurrence record.

sub track_time {
    
    my ($request, $node, $occ) = @_;
    
    if ( defined $occ->{early_age} && defined $occ->{late_age} )
    {
	if ( !defined $node->{firstocc_ea} || $occ->{early_age} > $node->{firstocc_ea} )
	{
	    $node->{firstocc_ea} = $occ->{early_age};
	}
	
	if ( !defined $node->{firstocc_la} || $occ->{late_age} > $node->{firstocc_la} )
	{
	    $node->{firstocc_la} = $occ->{late_age};
	}
	
	if ( !defined $node->{lastocc_ea} || $occ->{early_age} < $node->{lastocc_ea} )
	{
	    $node->{lastocc_ea} = $occ->{early_age};
	}
	
	if ( !defined $node->{lastocc_la} || $occ->{late_age} < $node->{lastocc_la} )
	{
	    $node->{lastocc_la} = $occ->{late_age};
	}
    }
    
    elsif ( defined $occ->{firstocc_ea} )
    {
	if ( !defined $node->{firstocc_ea} || $occ->{firstocc_ea} > $node->{firstocc_ea} )
	{
	    $node->{firstocc_ea} = $occ->{firstocc_ea};
	}
	
	if ( !defined $node->{firstocc_la} || $occ->{firstocc_la} > $node->{firstocc_la} )
	{
	    $node->{firstocc_la} = $occ->{firstocc_la};
	}
	
	if ( !defined $node->{lastocc_ea} || $occ->{lastocc_ea} < $node->{lastocc_ea} )
	{
	    $node->{lastocc_ea} = $occ->{lastocc_ea};
	}
	
	if ( !defined $node->{lastocc_la} || $occ->{lastocc_la} < $node->{lastocc_la} )
	{
	    $node->{lastocc_la} = $occ->{lastocc_la};
	}
    }
}


# get_upper_taxa ( base_no, options )
# 
# Fetch the taxonomic hierarchy rooted at the given taxon number, but only
# taxa above the genus level.  The retrieved records are stored in
# %taxon_node, with orig_no as key.

sub get_upper_taxa {
    
    my ($request, $base_no, $options) = @_;
    
    return unless $base_no > 0;
    
    my $taxonomy = $request->{my_taxonomy};
    
    # Get a list of all the taxa in the specified subtree, above the rank of
    # genus.  If the option 'app' was specified, include the first-and-last
    # appearance info.
    
    my @fields = ('DATA', 'RANK', 'family_no', $request->select_list_for_taxonomy);
    
    my $taxa_list;
    
    try {
	$taxa_list = $taxonomy->list_subtree($base_no, { min_rank => 6, 
							 fields => \@fields, 
							 return => 'listref' });
    }
    
    catch {
	die $_ if $_;
    };
    
    print STDERR $taxonomy->last_sql . "\n\n" if $request->debug;
    
    # If no taxa were returned, then the base taxon is probably a genus or
    # below.  So just fetch that single record, so that we at least have
    # something. 
    
    unless ( @$taxa_list )
    {
	$taxa_list = $taxonomy->list_taxa('current', $base_no, { fields => \@fields, 
								 return => 'listref' });
    }
    
    # Now go through the list.  When we find a taxon whose info we have not
    # yet gotten, just put the record into the $taxon_node hash.  Otherwise,
    # copy the relevant info.
    
    foreach my $r ( @$taxa_list )
    {
	my $taxon_no = $r->{orig_no};
	my $parent_no = $r->{senpar_no};
	
	$taxon_node{$taxon_no} = $r;
	
	# Hook this node up to its parent, which will already be in the
	# %taxon_node hash because &list_subtree retrieves nodes in
	# tree-sequence order.
	
	$taxon_node{$parent_no}{chld}{$taxon_no} = 1 if $taxon_node{$parent_no};
	
	# Clear the occurrence count, since we will be counting up occurrences
	# from the currently selected set rather than using the global count.
	
	$taxon_node{$taxon_no}{n_occs} = 0;
    }
}


# get_taxon_info ( taxon_nos, options )
# 
# Fetch records representing the taxa identified by the specified set of
# taxon_no values.  Fill in the requested information into the corresponding
# records in %taxon_node.

my $REQUEST_LIMIT = 500;

sub get_taxon_info {
    
    my ($request, $taxon_nos) = @_;
    
    # Get a list of the specified taxa.  If the option 'app' was specified,
    # include the first-and-last appearance info.
    
    my @fields = ('DATA', 'family_no');
    
    push @fields, $request->select_list_for_taxonomy('taxa');
    
    # foreach my $f ( $request->select_list )
    # {
    # 	next if $f =~ qr{\.modified};
    # 	$f = 'CRMOD' if $f =~ qr{\.created$};
    # 	push @fields, $f;
    # }
    
    my @ranks = $request->clean_param_list('rank');
    
    push @fields, 'RANK' if @ranks;
    
    my @ids;
    
    foreach my $id ( keys %$taxon_nos )
    {
	push @ids, $id;
	next unless @ids >= $REQUEST_LIMIT;
	
	$request->make_taxon_request(\@ids, \@fields);
	@ids = ();
    }
    
    $request->make_taxon_request(\@ids, \@fields) if @ids;
}


sub make_taxon_request {
    
    my ($request, $taxon_list, $field_list) = @_;
    
    my $taxonomy = $request->{my_taxonomy};
    
    my $taxa_list = [];
    
    try {
	$taxa_list = $taxonomy->list_taxa('current', $taxon_list, { fields => $field_list,
								    return => 'listref' });
    }
    
    catch {
	die $_ if $_;
    };
    
    print STDERR $taxonomy->last_sql . "\n\n" if $request->debug;
    
    # Now go through the list, and copy the relevant info.
    
    foreach my $r ( @$taxa_list )
    {
	my $taxon_no = $r->{orig_no};
	my $taxon_node = $taxon_node{$taxon_no} // {};
	
	# Copy over all of the attributes that aren't already set.  Skip the
	# attribute n_occs, because we are counting up occurrences from the
	# currently selected set rather than using the global occurrence counts.
	
	foreach my $f ( keys %$r )
	{
	    next if $f eq 'n_occs';
	    $taxon_node->{$f} = $r->{$f} unless defined $taxon_node->{$f};
	}
	
	# # If the node doesn't give a taxonomic rank, copy over the name, rank
	# # and tree sequence.
	
	# unless ( $taxon_node->{taxon_rank} )
	# {
	#     $taxon_node->{taxon_name} = $r->{taxon_name};
	#     $taxon_node->{taxon_rank} = $r->{taxon_rank};
	#     $taxon_node->{lft} = $r->{lft};
	# }
	
	# # If we were asked for attribution information, compute that now.
	
	# if ( $request->{my_attr} )
	# {
	#     $taxon_node->{attribution} = $r->{attribution};
	# }
    }
}


# get_interval_info ( nodes )
# 
# Fetch interval names from the interval_map table to go with the occurrence
# time ranges.

sub get_interval_info {
    
    my ($request, $nodes) = @_;
    
    my $dbh = $request->get_connection;
    
    my %range_keys;
    
    foreach my $node ( values %$nodes )
    {
	next unless defined $node->{firstocc_ea} && defined $node->{lastocc_la};
	my $first = $node->{firstocc_ea} + 0;
	my $last = $node->{lastocc_la} + 0;
	$range_keys{"'$first-$last'"} = 1;
	$node->{range_key} = "$first-$last";
    }
    
    my $key_list = join(',', keys %range_keys);
    
    return unless $key_list;
    
    my $sql = "SELECT range_key, early_interval, late_interval
	       FROM $INTERVAL_MAP WHERE range_key in ($key_list)";
    
    my $result = $dbh->selectall_arrayref($sql);
    
    my (%early, %late);
    
    foreach my $r ( @$result )
    {
	my ($range_key, $early_interval, $late_interval) = @$r;
	
	$early{$range_key} = $early_interval;
	$late{$range_key} = $late_interval;
    }
    
    foreach my $node ( values %$nodes )
    {
	next unless defined $node->{range_key};
	$node->{occ_early_interval} = $early{$node->{range_key}};
	$node->{occ_late_interval} = $late{$node->{range_key}};
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
    $node->{n_occs} ||= 0;
    $node->{specific_occs} = $node->{n_occs};
    $node->{n_orders} = 0;
    $node->{n_families} = 0 if $request->{my_count_rank} <= 9;
    $node->{n_genera} = 0 if $request->{my_count_rank} <= 5;
    $node->{n_species} = 0 if $request->{my_count_rank} <= 3;
    delete $node->{base_no};
    
    # Mark nodes whose parent doesn't appear in the result set.
    
    $node->{is_root} = 1 if defined $node->{senpar_no} && ! exists $taxon_node{$node->{senpar_no}};
    
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
	    
	    next unless $child_node->{n_occs};
	    
	    # For those which do, add the occurrence and subtaxon counts to
	    # the corresponding counts in the current node.
	    
	    $request->sum_subtaxa($node, $child_node);
	}
    }
    
    my $a = 1;	# we can stop here when debugging
}


sub sum_subtaxa {
    
    my ($request, $node, $child) = @_;
    
    no warnings 'uninitialized';
    
    # Now add the number of species, genera, etc. counted for the child to the
    # parent node.
    
    my $child_orders = $child->{taxon_rank} == 13 && ! $child->{is_uns}	? 1
		     : $child->{n_orders}				? $child->{n_orders}
									: 0;
    
    $node->{n_orders} += $child_orders if $node->{taxon_rank} > 13;

    if ( $request->{my_count_rank} <= 9 )
    {
	my $child_families = $child->{taxon_rank} == 9 && ! $child->{is_uns} ? 1
			   : $child->{n_families}			     ? $child->{n_families}
									     : 0;

	$node->{n_families} += $child_families if $node->{taxon_rank} > 9;
    }

    if ( $request->{my_count_rank} <= 5 )
    {
	my $child_genera = $child->{taxon_rank} == 5 && ! $child->{is_uns} ? 1
			 : $child->{n_genera}				   ? $child->{n_genera}
									   : 0;

	$node->{n_genera} += $child_genera if $node->{taxon_rank} > 5;
    }

    if ( $request->{my_count_rank} <= 3 )
    {
	my $child_species = $child->{taxon_rank} == 3 && ! $child->{is_uns} ? 1
			  : $child->{n_species}				    ? $child->{n_species}
									    : 0;
	
	$node->{n_species} += $child_species if $node->{taxon_rank} > 3;
    }

    $node->{n_occs} += $child->{n_occs} if $child->{n_occs};
    
    $request->track_time($node, $child) if $request->{my_track_time};
}


sub generate_occs_taxa_options {
    
    my ($request) = @_;
    
    my $options = { };
    
    if ( my $extant = $request->clean_param('extant') )
    {
	$options->{extant} = $extant;
    }
    
    my @ranks = $request->clean_param_list('rank');
    
    if ( @ranks )
    {
	my (@min_rank, @max_rank, %sel_rank);
	
        foreach my $rank ( @ranks )
	{
	    if ( $rank =~ qr{ ^ (min_|above_) ([^-]+) - (max_|below_) (.*) $ }xs )
	    {
		my $min = $1 eq 'min_' ? $2 : $2 + 0.1;
		my $max = $3 eq 'max_' ? $4 : $4 - 0.1;
		
		push @min_rank, $min;
		push @max_rank, $max;
	    }
	    
	    elsif ( $rank =~ qr{ ^ (max_|below_) (.*) }xs )
	    {
		my $max = $1 eq 'max_' ? $2 : $2 - 0.1;
		push @max_rank, $max;
	    }
	    
	    elsif ( $rank =~ qr{ ^ (min_|above_) (.*) }xs )
	    {
		my $min = $1 eq 'min_' ? $2 : $2 + 0.1;
		push @min_rank, $min;
	    }
	    
	    else
	    {
		$sel_rank{$rank} = 1;
		push @min_rank, $rank;
	    }
	}
	
	$options->{sel_rank} = \%sel_rank;
	
	foreach my $min (@min_rank)
	{
	    if ( !defined $options->{min_rank} || $min < $options->{min_rank} )
	    {
		$options->{min_rank} = $min;
	    }
	}
	
	foreach my $max (@max_rank)
	{
	    if ( !defined $options->{max_rank} || $max > $options->{max_rank} )
	    {
		$options->{max_rank} = $max;
	    }
	}
    }
    
    if ( my $status = $request->clean_param('taxon_status') )
    {
	$options->{status} = $status;
    }
    
    return $options;
}


sub add_result_records {
    
    my ($request, $options, $taxon_no, $parent_no, $family_ok, $recursion_count) = @_;
    
    $options ||= { };
    
    no warnings 'uninitialized';
    
    my $node = $taxon_node{$taxon_no};
    
    # If we have seen this node already, ignore it.  This is necessary because
    # we would otherwise get runaway recursion.
    
    return if $taxon_node{$taxon_no}{seen};
    
    $taxon_node{$taxon_no}{seen} = 1;
    
    # Do an additional check for runaway recursion.
    
    if ( defined $recursion_count && $recursion_count >= 200 )
    {
	die "Runaway recursion!\n";
    }
    
    $recursion_count++;
    
    # Skip any taxon for which no occurrences have been recorded.  This will
    # only happen with the full hierarchy.
    
    return unless $node->{n_occs};
    
    # Skip any taxon that ranks below the specified resolution level.
    
    my $rank = $node->{taxon_rank};
    return if $rank < $request->{my_reso_rank};
    
    # Skip any taxon that falls below the minimum specified rank.
    
    if ( $options->{min_rank} )
    {
	return if $node->{max_rank} < $options->{min_rank};
    }
    
    # If this taxon node does not have a name, then it represents a missing
    # part of the taxonomic hierarchy.  So generate a "NONE_SPECIFIED" name
    # appropriate to the taxon rank.
    
    unless ( $node->{orig_no} )
    {
	$node->{taxon_name} = $uns_name{$rank || 0};
	#unless $node->{taxon_name} && $node->{taxon_name} ne '~';
	$node->{orig_no} = $taxon_no;
    }
    
    # Make this node conform to the hierarchy we are generating, by overriding
    # the parent_no value if it exists.  We want to make sure that people can
    # link up the generated records to each other.
    
    $node->{senpar_no} = $parent_no if $parent_no;
    
    # If the 'family_no' field is set, or if the rank is 9, then we have found
    # a family.
    
    $family_ok = 1 if $node->{family_no} || $node->{taxon_rank} == 9;
    
    # Add the appropriate subtaxon counts.
    
    if ( $rank > 13 && $request->{my_count_rank} <= 13 )
    {
	$node->{n_orders} = $node->{n_orders};
    }
    
    if ( $rank > 9 && $request->{my_count_rank} <= 9 )
    {
	$node->{n_families} = $node->{n_families};
    }
    
    if ( $rank > 5 && $request->{my_count_rank} <= 5 )
    {
	$node->{n_genera} = $node->{n_genera};
    }
    
    if ( $rank > 3 && $request->{my_count_rank} <= 3 )
    {
	$node->{n_species} = $node->{n_species};
    }
    
    # Add this taxon to the result list, but only if it meets all of the criteria.
    
    if ( $request->check_record($node, $options) )
    {
	$request->add_result($node);
    }
    
    # If this is a "leaf taxon" according to the specified resolution level,
    # then stop here.  Otherwise, recurse to the children of this taxon.
    
    return if $rank == $request->{my_reso_rank};
    
    if ( $options->{min_rank} )
    {
	return if $node->{max_rank} == $options->{min_rank};
    }
   
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
	
	$request->add_result_records($options, $child_no, $taxon_no, $family_ok, $recursion_count);
    }
    
    # If we have any deferred nodes, then create a new "unspecified family"
    # node and paste it into the tree.  All of the deferred children will be
    # children of this node.
    
    if ( @deferred )
    {
	my $uns_no = uns_identifier(9);
	my $uns_node = $taxon_node{$uns_no} = { taxon_rank => 9, n_occs => 0, 
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
	
	$request->add_result_records($options, $uns_no, $taxon_no, 1, $recursion_count);
	
	my $a = 1;	# we can stop here when debugging
    }
}


sub check_record {
    
    my ($request, $node, $options) = @_;
    
    if ( my $status = $options->{status} )
    {
	if ( $status eq 'accepted' || $status eq 'senior' )
	{
	    return unless !defined $node->{status} || $node->{status} eq 'belongs to';
	}
	
	elsif ( $status eq 'valid' )
	{
	    return unless !defined $node->{status} || 
		$node->{status} eq 'belongs to' ||
		$node->{status} eq 'subjective synonym of' ||
		    $node->{status} eq 'objective synonym of' ||
			$node->{status} eq 'replaced by';
	}
	
	elsif ( $status eq 'junior' )
	{
	    return unless defined $node->{status};
	    return unless $node->{status} eq 'subjective synonym of' ||
		$node->{status} eq 'objective synonym of' ||
		    $node->{status} eq 'replaced by';
	}
	
	elsif ( $status eq 'invalid' )
	{
	    return unless defined $node->{status};
	    return if $node->{status} eq 'belongs to' ||
		$node->{status} eq 'subjective synonym of' ||
		    $node->{status} eq 'objective synonym of' ||
			$node->{status} eq 'replaced by';
	}
    }
    
    unless ( ref $options eq 'HASH' && $options->{sel_rank} && $options->{sel_rank}{$node->{taxon_rank}} )
    {
	if ( $options->{max_rank} && defined $node->{min_rank} )
	{
	    return unless $node->{min_rank} <= $options->{max_rank};
	}
	
	elsif ( ref $options->{sel_rank} eq 'HASH' )
	{
	    return;
	}
    }
    
    if ( defined $options->{extant} )
    {
	if ( $options->{extant} )
	{
	    return unless $node->{is_extant};
	}
	
	else
	{
	    return if $node->{is_extant};
	}
    }
    
    return 1;
}


# generate_prevalence ( data, limit )
# 
# Generate a list of the most prevalent taxa.  The parameter $data must be an
# arrayref that conveys the result of a database query on either the
# 'prv_matrix' or 'prv_global' table.  The parameter $limit should be the
# desired number of entries to return.
# 
# Unlike most of the data service operations, the result of this operation
# will depend upon the value of $limit.  The more entries are requested, the
# more precise we can be.  The algorithm is as follows: start by listing all
# of the phyla; then if the number of these is smaller than the requested
# number of entries, split the biggest phyla into classes.  Repeat until
# either all of the classes have been split or until the list fills up.  If
# there is still room in the list, start splitting the classes into orders.

sub generate_prevalence {

    my ($request, $data, $limit) = @_;
    
    no warnings 'uninitialized';
    
    my $taxonomy = $request->{my_taxonomy};
    
    my (%phylum, %class, %order);
    
    # We start by tallying all of the entries in the data by phylum, class
    # and order.
    
    foreach my $r (@$data)
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
    
    # Get name, image_no, and tree sequence for each of these taxa.
    
    my $dbh = $request->{dbh};
    
    my $orig_string = join(',', grep { $_ > 0 } (keys %phylum, keys %class, keys %order));
    
    $orig_string ||= '0';
    
    my $sql = "
	SELECT orig_no, name, lft, rgt, image_no FROM $taxonomy->{TREE_TABLE}
		JOIN $taxonomy->{ATTRS_TABLE} using (orig_no)
	WHERE orig_no in ($orig_string)";
    
    my $taxa = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    foreach my $d (@$taxa)
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
    
    if ( ref $phylum{0}{class} eq 'HASH' )
    {
	push @list, grep { $_->{orig_no} } values %{$phylum{0}{class}};
    }
    
    @list = sort { $b->{n_occs} <=> $a->{n_occs} } @list;
    
    # Now that the basic list has been sorted we determine the threshold of
    # occurrences above which an entry remains on the list, according to the
    # requested number of results.
    
    my $length = scalar(@list);
    my $threshold = $limit <= $length ? $list[$limit-1]{n_occs} : 0;
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
		    $list[$i]{class}{$n}{name} = 'Chordata (unclassified)';
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
    
    # Now recompute the length, threshold, and deficit.

    $length = scalar(@list);
    $threshold = $limit <= $length ? $list[$limit-1]{n_occs} : 0;
    $deficit = $limit > $length ? $limit - $length : 0;
    
    # Now, we go through the list a second time.  If any element is such that
    # splitting it would not cause any existing elements to move past the
    # given limit on the result list, then split it.
    
 ELEMENT:
    for (my $i = 0; $i < $limit && $i < @list; $i++)
    {
	my $name = $list[$i]{name};
	my $rank = $list[$i]{rank};
	my $subkey = $rank eq '17' ? 'order' : 'class';
	
	next unless ref $list[$i]{$subkey} eq 'HASH';
	next unless $rank eq '20' || $length <= 3;
	
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
		$list[$i]{$subkey}{$n}{name} = "$name (unclassified)";
		$list[$i]{$subkey}{$n}{image_no} = $list[$i]{image_no};
		$list[$i]{$subkey}{$n}{orig_no} = $list[$i]{orig_no};
	    }
	}
	
	next ELEMENT unless $count > 0;
	next ELEMENT if $count > $deficit + 1 && $deficit < 5;
	
	push @subelements, $list[$i]{$subkey}{$_} foreach @subs;
	splice(@list, $i, 1);
	$i--;
    }
    
    @list = sort { $b->{n_occs} <=> $a->{n_occs} } @list, @subelements;
    @subelements = ();    
    
    # Again, recalculate the length, threshold, and deficit.
    
    $length = scalar(@list);
    $threshold = $limit <= $length ? $list[$limit-1]{n_occs} : 0;
    $deficit = $limit > $length ? $limit - $length : 0;
    
    # If the list is not full, then go through it a third time and see if we
    # can split classes into orders.
    
    if ( $length < $limit )
    {
    ELEMENT:
	for (my $i = 0; $i < $limit && $i < @list; $i++)
	{
	    my $name = $list[$i]{name};
	    my $rank = $list[$i]{rank};
	    next unless $rank eq '17';
	    
	    my $subkey = $rank eq '17' ? 'order' : 'class';
	    
	    next unless ref $list[$i]{$subkey} eq 'HASH';
	    
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
		    $list[$i]{$subkey}{$n}{name} = "$name (unclassified)";
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
    }
    
    @list = sort { $b->{n_occs} <=> $a->{n_occs} } @list, @subelements;
    @subelements = ();
    
    # Now we go through one more time and mark every entry that contains
    # another entry with (other).
    
    foreach my $i ( 0..$limit-1 )
    {
	foreach my $j ( 0..$limit-1 )
	{
	    if ( $i != $j && $list[$i]{lft} <= $list[$j]{lft} && $list[$i]{rgt} >= $list[$j]{rgt} )
	    {
		$list[$i]{name} .= " (other)" unless $list[$i]{name} =~ qr{\)$}x;
	    }
	}
    }
    
    # Now use this list to generate the result.
    
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


