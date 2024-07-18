#
# IntervalBase
# 
# This module caches information from the PBDB interval and timescale tables,
# and provides basic functionality regarding interval data.
# 
# Author: Michael McClennen

use strict;

package IntervalBase;

use TableDefs qw(%TABLE);
use CoreTableDefs;
use Carp qw(croak);

use base 'Exporter';

our (@EXPORT_OK) = qw(ts_defined ts_name ts_bounds ts_record ts_intervals ts_by_age ts_list
		      ts_boundary_list ts_boundary_no ts_boundary_name ts_boundary_next
		      int_defined int_bounds int_scale int_name int_type int_correlation
		      int_container int_record ints_by_age ts_intervals_by_age
		      interval_nos_by_age ints_by_prefix
		      INTL_SCALE BIN_SCALE AGE_RE);

our (%IDATA, %INAME, %IPREFIX, %ICORR, %SDATA, %SSEQUENCE, @SCALE_NUMS);
our (%BOUNDARY_LIST, %BOUNDARY_MAP);

use constant INTL_SCALE => 1;	# The identifier of the ICS international time scale.
use constant BIN_SCALE => 10;	# The identifier of the 10-million-year bin scale.

use constant AGE_RE => qr{^\d[.\d]*$};

our (@CENO_COLOR) = ('#FB8069', '#FB8D76', '#FB9A85', '#FCB4A2', '#FCC0B2',
		      '#FCC0B2', '#FCC0B2', '#FCC0B2', '#FCC0B2', '#FCC0B2');

our (@PALEO_COLOR) = ('#FFFF33', '#FFFF4D', '#FFFF59', '#FFFF66', '#FFFF73',
		       '#FFFF73', '#FFFF73', '#FFFF73', '#FFFF73', '#FFFF73');

our (%IS_DEFAULT_TYPE) = (age => 1, bin => 1, zone => 1, chron => 1);


# cache_interval_data ( dbh )
# 
# Read the basic interval and scale data from the relevant data tables and
# install them into exportable variables.

sub cache_interval_data {
    
    my ($class, $dbh) = @_;
    
    # If we have already done this, then there is nothing else to do.
    
    return if %IDATA;
    
    my ($sql, $result);
    
    my %ref_uniq;
    
    # First, read in a list of all the scales and put them in the SDATA hash,
    # indexed by scale_no.
    
    $sql = "SELECT scale_no, scale_name, early_age, late_age, locality, reference_no
	    FROM $TABLE{SCALE_DATA} ORDER BY scale_no";
    
    foreach my $s ( $dbh->selectall_array($sql, { Slice => {} }) )
    {
	my $scale_no = $s->{scale_no};
	my $reference_no = $s->{reference_no};
	
	$s->{early_age} =~ s/[.]?0+$// if defined $s->{early_age};
	$s->{late_age} =~ s/[.]?0+$// if defined $s->{late_age};
	
	$s->{b_age} = $s->{early_age};
	$s->{t_age} = $s->{late_age};
	
	$SDATA{$scale_no} = $s;
	push @SCALE_NUMS, $scale_no;
	
	if ( $reference_no )
	{
	    push $SDATA{$scale_no}{reflist}->@*, $reference_no;
	    $ref_uniq{$scale_no}{$reference_no} = 1;
	}
    }
    
    # Then read in the interval definitions. This requires both the
    # interval_data table and the scale_map table.
    
    $sql = "SELECT i.interval_no, i.interval_name, i.abbrev, i.scale_no as main_scale_no,
		   i.b_type, i.b_ref, i.t_type, i.t_ref,
		   i.early_age as b_age, i.late_age as t_age, 
		   sm.scale_no, sm.color, sm.type, sm.obsolete, sm.parent_no, sm.reference_no
		FROM $TABLE{SCALE_MAP} as sm join $TABLE{INTERVAL_DATA} as i using (interval_no)
		ORDER BY sm.scale_no, sm.sequence";
    
    foreach my $sm ( $dbh->selectall_array($sql, { Slice => {} }) )
    {
	my $interval_name = $sm->{interval_name};
	my $interval_no = $sm->{interval_no};
	my $scale_no = $sm->{scale_no};
	my $reference_no = $sm->{reference_no};
	
	$sm->{t_age} =~ s/[.]?0+$//;
	$sm->{b_age} =~ s/[.]?0+$//;
	$sm->{t_ref} =~ s/[.]?0+$//;
	$sm->{b_ref} =~ s/[.]?0+$//;
	$sm->{early_age} = $sm->{b_age};
	$sm->{late_age} = $sm->{t_age};
	
	push $SSEQUENCE{$scale_no}->@*, $sm;
	
	# A record where scale_no = main_scale_no is the main definition for
	# that particular interval. Where the interval has been incorporated
	# into other scales, main_scale_no <> scale_no.
	
	if ( $scale_no eq $sm->{main_scale_no} )
	{
	    delete $sm->{main_scale_no};
	    
	    $IDATA{$interval_no} = $sm;
	    $INAME{lc $interval_name} = $sm;
	    
	    my $interval_prefix = lc $interval_name;
	    $interval_prefix =~ s/ ^early\s | ^middle\s | ^late\s //xs;
	    $interval_prefix = substr($interval_prefix, 0, 3);
	    
	    push @{$IPREFIX{$interval_prefix}}, $sm if $scale_no eq INTL_SCALE;
	}
	
	# Fields with false or empty values can be eliminated to save space.
	
	delete $sm->{abbrev} unless $sm->{abbrev};
	delete $sm->{obsolete} unless $sm->{obsolete};
	delete $sm->{parent_no} unless $sm->{parent_no};
	
	# Keep a list of reference_no values associated with each scale
	
	unless ( $ref_uniq{$scale_no}{$reference_no} )
	{
	    push $SDATA{$scale_no}{reflist}->@*, $reference_no;
	    $ref_uniq{$scale_no}{$reference_no} = 1;
	}
	
	# Set the default type for this scale, if it has not already been set.
	
	unless ( $SDATA{$scale_no}{default_type} )
	{
	    $SDATA{$scale_no}{default_type} = $sm->{type} if $IS_DEFAULT_TYPE{$sm->{type}};
	}
    }
    
    # Then cache information about interval correlations.
    
    $sql = "SELECT interval_no, stage_no, subepoch_no, epoch_no, period_no, ten_my_bin
	    FROM interval_lookup";
    
    foreach my $r ( $dbh->selectall_array($sql, { Slice => {} }) )
    {
	my $interval_no = $r->{interval_no};
	$ICORR{$interval_no} = $r;
    }
    
    # Add eon correlations to the precambrian.
    
    my @precambrian;
    my $INTL_SCALE = INTL_SCALE;
    
    foreach my $int ( $SSEQUENCE{$INTL_SCALE}->@* )
    {
	push @precambrian, $int if $int->{type} eq 'eon' && $int->{b_age} > 550;
    }
    
    foreach my $int ( values %IDATA )
    {
	if ( $int->{b_age} > 550 )
	{
	    my $interval_no = $int->{interval_no};
	    
	    foreach my $eon ( @precambrian )
	    {
		if ( $eon->{t_age} <= $int->{t_age} &&
		     $eon->{b_age} >= $int->{b_age} )
		{
		    $ICORR{$interval_no}{eon_no} = $eon->{interval_no};
		}
	    }
	}
    }
	
    # Now compute a boundary list and boundary map for each interval type in the
    # international scale and the ten million year bin scale.
    
    foreach my $scale_no ( INTL_SCALE, BIN_SCALE )
    {
	my %boundary_map;
	
	foreach my $i ( $SSEQUENCE{$scale_no}->@* )
	{
	    my $b_age = $i->{early_age};
	    my $type = $i->{type};
	    
	    if ( $type ne 'era' && $type ne 'eon' &&
	         $b_age > 0.01 )
	    {
		$boundary_map{$type}{$b_age} = $i;
	    }
	}
	
	# Add Holocene to the 'age' map for the international scale. Otherwise,
	# it will end with the Late Pleistocene.
	
	if ( $scale_no eq INTL_SCALE )
	{
	    my $hbound = $INAME{holocene}{early_age};
	    $boundary_map{age}{$hbound} = $INAME{holocene};
	}
	
	# Now sort each of the boundary lists (oldest to youngest) and
	# store them in the appropriate package variable. Add the end age of the scale.
	
	foreach my $type ( keys %boundary_map )
	{
	    $BOUNDARY_LIST{$scale_no}{$type} = 
		[ sort { $b <=> $a } keys %{$boundary_map{$type}} ];
	    
	    push @{$BOUNDARY_LIST{$scale_no}{$type}}, $SDATA{$scale_no}{late_age};
	    $BOUNDARY_MAP{$scale_no}{$type} = $boundary_map{$type};
	}
	
	my $default_type = $SDATA{$scale_no}{default_type};
	$BOUNDARY_LIST{$scale_no}{default} = $BOUNDARY_LIST{$scale_no}{$default_type};
    }
    
    my $a = 1;	# we can stop here when debugging.
}


sub cache_filled {
    
    return scalar(%IDATA);
}


sub ts_defined {
    
    my ($scale_no) = @_;
    
    if ( $scale_no && $SDATA{$scale_no} && $SDATA{$scale_no}{scale_no} )
    {
	return $SDATA{$scale_no}{scale_no};
    }
    
    else
    {
	return;
    }
}


sub ts_name {
    
    my ($scale_no) = @_;
    
    if ( $scale_no && $SDATA{$scale_no} && $SDATA{$scale_no}{scale_no} )
    {
	return $SDATA{$scale_no}{scale_name};
    }
    
    else
    {
	return;
    }
}


sub ts_record {
    
    my ($scale_no) = @_;
    
    if ( $scale_no && $SDATA{$scale_no} && $SDATA{$scale_no}{scale_no} )
    {
	return { $SDATA{$scale_no}->%* };
    }
    
    else
    {
	return;
    }
}


sub ts_bounds {
    
    my ($scale_no) = @_;
    
    if ( $scale_no && $SDATA{$scale_no} && $SDATA{$scale_no}{scale_no} )
    {
	return ($SDATA{$scale_no}{early_age}, $SDATA{$scale_no}{late_age});
    }
    
    else
    {
	return;
    }
}


sub ts_list {
    
    return @SCALE_NUMS;
}


sub ts_by_age {
    
    my ($b_age, $t_age, $selector) = @_;
    
    my @result;
    
    if ( $selector eq 'overlap' )
    {
	foreach my $ts ( values %SDATA )
	{
	    if ( $ts->{early_age} > $t_age && $ts->{late_age} < $b_age )
	    {
		push @result, { $ts->%* } unless $ts->{scale_no} eq INTL_SCALE;
	    }
	}
    }
    
    elsif ( $selector eq 'contained' )
    {
	foreach my $ts ( values %SDATA )
	{
	    if ( $ts->{early_age} <= $b_age && $ts->{late_age} >= $t_age )
	    {
		push @result, { $ts->%* } unless $ts->{scale_no} eq INTL_SCALE;
	    }
	}
    }
    
    else
    {
	croak "invalid selector '$selector'";
    }
    
    return @result;
}


sub ts_intervals {
    
    my ($scale_no) = @_;
    
    my @result;
    
    if ( $scale_no && $SSEQUENCE{$scale_no} )
    {
	foreach my $int ( $SSEQUENCE{$scale_no}->@* )
	{
	    push @result, { $int->%* };
	}
    }
    
    return @result;
}


sub ts_boundary_list {
    
    my ($scale_no, $type) = @_;
    
    return $BOUNDARY_LIST{$scale_no}{$type} ? $BOUNDARY_LIST{$scale_no}{$type}->@* : ();
}


sub ts_boundary_no {
    
    my ($scale_no, $type, $bound) = @_;
    
    return $BOUNDARY_MAP{$scale_no}{$type}{$bound}{interval_no};
}


sub ts_boundary_name {
    
    my ($scale_no, $type, $bound) = @_;
    
    return $BOUNDARY_MAP{$scale_no}{$type}{$bound}{interval_name};
}


sub ts_boundary_next {
    
    my ($scale_no, $type, $bound) = @_;
    
    return $BOUNDARY_MAP{$scale_no}{$type}{$bound}{t_age};
}


sub int_defined {
    
    my ($name_or_num) = @_;
    
    if ( $name_or_num && $IDATA{$name_or_num} && $IDATA{$name_or_num}{interval_no} )
    {
	return $IDATA{$name_or_num}{interval_no};
    }
    
    elsif ( $name_or_num && $INAME{lc $name_or_num} && $INAME{lc $name_or_num}{interval_no} )
    {
	return $INAME{lc $name_or_num}{interval_no};
    }
    
    else
    {
	return;
    }
}


sub int_name {
    
    my ($name_or_num) = @_;
    
    if ( $name_or_num && $IDATA{$name_or_num} && $IDATA{$name_or_num}{interval_no} )
    {
	return $IDATA{$name_or_num}{interval_name};
    }
    
    elsif ( $name_or_num && $INAME{lc $name_or_num} && $INAME{lc $name_or_num}{interval_no} )
    {
	return $IDATA{lc $name_or_num}{interval_name};
    }
    
    else
    {
	return;
    }
}


sub int_type {
    
    my ($name_or_num) = @_;
    
    if ( $name_or_num && $IDATA{$name_or_num} && $IDATA{$name_or_num}{interval_no} )
    {
	return $IDATA{$name_or_num}{type};
    }
    
    elsif ( $name_or_num && $INAME{lc $name_or_num} && $INAME{lc $name_or_num}{interval_no} )
    {
	return $IDATA{lc $name_or_num}{type};
    }
    
    else
    {
	return;
    }
}


sub int_scale {
    
    my ($name_or_num) = @_;
    
    if ( $name_or_num && $IDATA{$name_or_num} && $IDATA{$name_or_num}{interval_no} )
    {
	return $IDATA{$name_or_num}{scale_no};
    }
    
    elsif ( $name_or_num && $INAME{lc $name_or_num} && $INAME{lc $name_or_num}{interval_no} )
    {
	return $INAME{lc $name_or_num}{scale_no};
    }
    
    else
    {
	return;
    }
}


sub int_bounds {
    
    my ($name_or_num) = @_;
    
    if ( $name_or_num && $IDATA{$name_or_num} && $IDATA{$name_or_num}{interval_no} )
    {
	return ($IDATA{$name_or_num}{b_age}, $IDATA{$name_or_num}{t_age});
    }
    
    elsif ( $name_or_num && $INAME{lc $name_or_num} && $INAME{lc $name_or_num}{interval_no} )
    {
	return ($INAME{lc $name_or_num}{b_age}, $INAME{lc $name_or_num}{t_age});
    }
    
    else
    {
	return;
    }
}


sub int_container {
    
    my ($name_or_num) = @_;
    
    if ( $name_or_num && $IDATA{$name_or_num} && $IDATA{$name_or_num}{interval_no} )
    {
	return $ICORR{$name_or_num}{eon_no} // $ICORR{$name_or_num}{period_no};
    }
    
    elsif ( my $interval_no = $name_or_num && $INAME{lc $name_or_num} && 
	    $INAME{lc $name_or_num}{interval_no} )
    {
	return $ICORR{$interval_no}{eon_no} // $ICORR{$name_or_num}{period_no};
    }
    
    else
    {
	return;
    }
}


sub int_record {
    
    my ($name_or_num) = @_;
    
    if ( $IDATA{$name_or_num} && $IDATA{$name_or_num}{interval_no} )
    {
	return { $IDATA{$name_or_num}->%* };
    }
    
    elsif ( $INAME{lc $name_or_num} && $INAME{lc $name_or_num}{interval_no} )
    {
	return { $INAME{lc $name_or_num}->%* };
    }
    
    else
    {
	return;
    }
}


sub int_correlation {
    
    my ($interval_no, $attr) = @_;
    
    return $ICORR{$interval_no} && $ICORR{$interval_no}{$attr};
}


sub ints_by_age {
    
    my ($b_age, $t_age, $selector) = @_;
    
    my @result;
    
    if ( $b_age < $t_age )
    {
	my $temp = $b_age;
	$b_age = $t_age;
	$t_age = $temp;
    }
    
    if ( !defined $selector || $selector eq 'contained' )
    {
	foreach my $int ( values %IDATA )
	{
	    if ( $int->{b_age} <= $b_age && $int->{t_age} >= $t_age )
	    {
		push @result, { $int->%* };
	    }
	}
    }
    
    elsif ( $selector eq 'overlap' )
    {
	foreach my $int ( values %IDATA )
	{
	    if ( $int->{b_age} > $t_age && $int->{t_age} < $b_age )
	    {
		push @result, { $int->%* };
	    }
	}
    }
    
    elsif ( $selector eq 'major' )
    {
	foreach my $int ( values %IDATA )
	{
	    if ( $int->{b_age} > $t_age && $int->{t_age} < $b_age )
	    {
		my $overlap;
		
		if ( $int->{b_age} >= $b_age )
		{
		    if ( $int->{t_age} > $t_age )
		    {
			$overlap = $b_age - $int->{t_age};
		    }
		    
		    else
		    {
			$overlap = $b_age - $t_age;
		    }
		}
		
		else
		{
		    $overlap = $int->{b_age} - $t_age;
		}
		
		if ( $overlap / ($int->{b_age} - $int->{t_age}) >= 0.5 )
		{
		    push @result, { $int->%* };
		}
	    }
	}
    }
    
    elsif ( $selector eq 'contains' )
    {
	foreach my $int ( values %IDATA )
	{
	    if ( $int->{b_age} > $b_age && $int->{t_age} <= $t_age ||
		 $int->{b_age} >= $b_age && $int->{t_age} < $t_age )
	    {
		push @result, { $int->%* };
	    }
	}
    }
    
    else
    {
	croak "invalid selector '$selector'";
    }
    
    return @result;
}


sub ts_intervals_by_age {
    
    my ($scale_no, $b_age, $t_age, $selector) = @_;
    
    return unless $scale_no && $SSEQUENCE{$scale_no};
    
    my @result;
    
    if ( $b_age < $t_age )
    {
	my $temp = $b_age;
	$b_age = $t_age;
	$t_age = $temp;
    }
    
    if ( !defined $selector || $selector eq 'contained' )
    {
	foreach my $int ( $SSEQUENCE{$scale_no}->@* )
	{
	    if ( $int->{b_age} <= $b_age && $int->{t_age} >= $t_age )
	    {
		push @result, { $int->%* };
	    }
	}
    }
    
    elsif ( $selector eq 'overlap' )
    {
	foreach my $int ( $SSEQUENCE{$scale_no}->@* )
	{
	    if ( $int->{b_age} > $t_age && $int->{t_age} < $b_age )
	    {
		push @result, { $int->%* };
	    }
	}
    }
    
    elsif ( $selector eq 'major' )
    {
	foreach my $int ( $SSEQUENCE{$scale_no}->@* )
	{
	    if ( $int->{b_age} > $t_age && $int->{t_age} < $b_age )
	    {
		my $overlap;
		
		if ( $int->{b_age} >= $b_age )
		{
		    if ( $int->{t_age} > $t_age )
		    {
			$overlap = $b_age - $int->{t_age};
		    }
		    
		    else
		    {
			$overlap = $b_age - $t_age;
		    }
		}
		
		else
		{
		    $overlap = $int->{b_age} - $t_age;
		}
		
		if ( $overlap / ($int->{b_age} - $int->{t_age}) >= 0.5 )
		{
		    push @result, { $int->%* };
		}
	    }
	}
    }
    
    elsif ( $selector eq 'contains' )
    {
	foreach my $int ( $SSEQUENCE{$scale_no}->@* )
	{
	    if ( $int->{b_age} > $b_age && $int->{t_age} <= $t_age ||
		 $int->{b_age} >= $b_age && $int->{t_age} < $t_age )
	    {
		push @result, { $int->%* };
	    }
	}
    }
    
    else
    {
	croak "invalid selector '$selector'";
    }
    
    return @result;
}


sub interval_nos_by_age {
    
    my ($b_age, $t_age, $selector) = @_;
    
    my @result;
    
    if ( $b_age < $t_age )
    {
	my $temp = $b_age;
	$b_age = $t_age;
	$t_age = $temp;
    }
    
    if ( !defined $selector || $selector eq 'contained' )
    {
	foreach my $int ( values %IDATA )
	{
	    if ( $int->{b_age} <= $b_age && $int->{t_age} >= $t_age )
	    {
		push @result, $int->{interval_no};
	    }
	}
    }
    
    elsif ( $selector eq 'overlap' )
    {
	foreach my $int ( values %IDATA )
	{
	    if ( $int->{b_age} > $t_age && $int->{t_age} < $b_age )
	    {
		push @result, $int->{interval_no};
	    }
	}
    }
    
    elsif ( $selector eq 'major' )
    {
	foreach my $int ( values %IDATA )
	{
	    if ( $int->{b_age} > $t_age && $int->{t_age} < $b_age )
	    {
		my $overlap;
		
		if ( $int->{b_age} >= $b_age )
		{
		    if ( $int->{t_age} > $t_age )
		    {
			$overlap = $b_age - $int->{t_age};
		    }
		    
		    else
		    {
			$overlap = $b_age - $t_age;
		    }
		}
		
		else
		{
		    $overlap = $int->{b_age} - $t_age;
		}
		
		if ( $overlap / ($int->{b_age} - $int->{t_age}) >= 0.5 )
		{
		    push @result, $int->{interval_no};
		}
	    }
	}
    }
    
    elsif ( $selector eq 'contains' )
    {
	foreach my $int ( values %IDATA )
	{
	    if ( $int->{b_age} > $b_age && $int->{t_age} <= $t_age ||
		 $int->{b_age} >= $b_age && $int->{t_age} < $t_age )
	    {
		push @result, $int->{interval_no};
	    }
	}
    }
    
    else
    {
	croak "invalid selector '$selector'";
    }
    
    return @result;
}


sub ints_by_prefix {
    
    my ($prefix) = @_;
    
    if ( $IPREFIX{$prefix} )
    {
	return $IPREFIX{$prefix}->@*;
    }
    
    else
    {
	return;
    }
}


# generate_ts_diagram ( options, scale_hash, int_hash, scale_no... )
# 
# Generate an HTML table expression that displays the specified timescales as a
# diagram of boxes. The arguments $scales and $intervals must be listrefs.

sub generate_ts_diagram {
    
    my ($class, $options, $scale_hash, $ints_hash, @scale_list) = @_;
    
    # If no age limits are given, use 0 and 5000.
    
    my $t_limit = $options->{t_limit} || 0;
    my $b_limit = $options->{b_limit} || 5000;
    
    # Unless we are displaying the Precambrian, there is no point in showing eons and
    # eras.
    
    my $remove_eras = $b_limit < 550;
    
    # Phase I: collect interval boundaries
    
    # Start by computing the age of each boundary in the scale, and the minimum and
    # maximum. If top and base limits were given, restrict the set of age boundaries to
    # that range.
    
    my (%ints_list);	# List of intervals to be displayed for each timescale
    
    my (%ibound);	# Boundary ages from the international timescale
    
    my ($t_intl, $b_intl);	# The range of ages from the international timescale
    
    my (%bound);	# Boundary ages from scales other than the international timescale
    
    my ($t_range, $b_range);	# The range of ages from those scales
    
    my @errors;		# Any error messages that are generated are appended to
                        # this list
    
    # For each displayed scale in turn, run through its list of intervals.
    
    foreach my $snum ( @scale_list )
    {
	foreach my $int ( $ints_hash->{$snum}->@* )
	{
	    # Ignore eras and eons if $remove_eras is true.
	    
	    next if $remove_eras && $int->{type} =~ /^era|^eon/;
	    
	    # We cannot display any interval that doesn't have a good top and
	    # bottom age. This shouldn't happen, unless some other part of the
	    # interval system has gone seriously wrong.
	    
	    my $top = $int->{t_age};
	    my $base = $int->{b_age};
	    my $name = $int->{interval_name};
	    
	    unless ( defined $top && $top =~ AGE_RE )
	    {
		push @errors, "could not display '$name': bad top age '$top'";
		next;
	    }
	    
	    unless ( defined $base && $base =~ AGE_RE )
	    {
		push @errors, "could not display '$name': bad base age '$top'";
		next;
	    }
	    
	    # Skip this interval if it falls outside of the age limits.
	    
	    next if $base <= $t_limit;
	    next if $top >= $b_limit;
	    
	    # Add this interval to the list for display
	    
	    push $ints_list{$snum}->@*, $int;
	    
	    # Keep track of the age boundaries separately for the international scale and
	    # all other scales. Keep track of the minimum and maximum boundary ages
	    # separately as well.
	    
	    if ( $snum eq INTL_SCALE )
	    {
		$ibound{$top} = 1;
		
		if ( !defined $t_intl || $top < $t_intl )
		{
		    $t_intl = $top;
		}
	    }
	    
	    else
	    {
		$bound{$top} = 1;
		
		if ( !defined $t_range || $top < $t_range )
		{
		    $t_range = $top;
		}
	    }
		
	    if ( $snum eq INTL_SCALE )
	    {
		$ibound{$base} = 1;
		
		if ( !defined $b_intl || $base > $b_intl )
		{
		    $b_intl = $base;
		}
	    }
	    
	    else
	    {
		$bound{$base} = 1;
		
		if ( !defined $b_range || $base > $b_range )
		{
		    $b_range = $base;
		}
	    }
	}
    }
    
    # If we are displaying one or more scales other than the international one, use only
    # the international boundaries which lie in their range. Do not display the whole
    # international scale unless it is the only one being shown.
    
    if ( defined $b_range && ! defined $options->{b_limit} )
    {
	foreach my $b ( keys %ibound )
	{
	    $bound{$b} = 1 if $b >= $t_range && $b <= $b_range;
	}
    }
    
    # If we are displaying only the international scale, use all of its
    # boundaries between $t_limit and $b_limit.
    
    else
    {
	foreach my $b ( keys %ibound )
	{
	    $bound{$b} = 1;
	}
	
	$t_range = $t_intl;
	$b_range = $b_intl;
    }
    
    # Don't show eras and eons unless the bottom of the displayed range reaches
    # into the Precambrian.
    
    $remove_eras = 1 if $b_range < 550;
    
    # Phase II: Generate the 2-d array
    
    # The following arrays and hashes store rest of the information necessary to draw
    # the diagram.
    
    my @bound2d;	# Each element (cell) represents one interval boundary plus the
			# content below it. The first column holds the age.
    
    my @col_scale;	# Stores the scale number corresponding to each column
    
    my @col_type;	# Stores which interval type belongs in which column
    
    my @col_color;	# Stores the default color for each column
    
    my %cell_height;	# Stores the height of each cell
    
    my %cell_type;	# Stores the interval type for each cell
    
    my %cell_label;	# Stores the label for each cell.
    
    my %cell_combined;	# Stores the keys of additional intervals that were
                        # combined with this one.
    
    my %cell_color;	# Stores the color (if any) for each cell.
    
    my %cell_top;	# The top bound value for each cell
    
    my %cell_base;	# The base bound value for each cell
    
    my %cell_topen;	# True for each cell with an open top
    
    my %cell_bopen;	# True for each cell with an open bottom
    
    my %cell_hi;	# True for each cell to be highlighted
    
    my %bound_intp;	# True for each age value that was interpolated
    
    my $cindex = 0;	# Column color index
    
    # Store age boundaries in the first column of the bounds2d array, in order from newest
    # to oldest.
    
    my @bound_list = sort { $a <=> $b } keys %bound;
    
    foreach my $b ( @bound_list )
    {
	push @bound2d, [ $b ];
    }
    
    my $max_row = $#bound_list;
    
    # Compute the smallest difference between two bounds, and from that the
    # scale factor for linear time.
    
    # my ($min_diff);
    
    # foreach my $i ( 0..$max_row-1 )
    # {
    # 	my $diff = $bound_list[$i+1] - $bound_list[$i];
    # 	$min_diff = $diff if ! $min_diff || $diff < $min_diff;
    # }
    
    # Now go through the scales and their intervals one by one. Place each interval in
    # turn in the 2-d array in such a way that it does not overlap any of the intervals
    # already there.
    
    # The following two variables bracket the columns that correspond to a given scale.
    # Because they are initialized to zero, the first scale starts with both of them given
    # the value of 1. Each new scale starts with the next empty column.
    
    my $min_col = 0;
    my $max_col = 0;
    
    foreach my $snum ( @scale_list )
    {
	$min_col = $max_col + 1;
	$max_col = $min_col;
	
	# Run through the intervals a second time in the order they appear in the
	# timescale.  This order can affect how they are displayed if some of them
	# overlap others.
	
      INTERVAL:
	foreach my $int ( $ints_list{$snum}->@* )
	{
	    # Ignore eras and eons if $remove_eras is true.
	    
	    next if $remove_eras && $int->{type} =~ /^era|^eon/;
	    
	    # Each interval is stored under a key generated form the scale
	    # number and interval n umber.
	    
	    my $inum = $int->{interval_no};
	    my $iname = $int->{interval_name};
	    my $itype = $int->{type};
	    my $ikey = "$snum-$inum";
	    
	    my $itop = $int->{t_age};
	    my $ibase = $int->{b_age};
	    
	    # Ignore any interval that falls outside of the age range to be displayed.
	    
	    next if $ibase <= $t_range;
	    next if $itop >= $b_range;
	    
	    # If this interval overlaps the top or bottom age boundary, display only the
	    # part that falls within these boundaries. The horizontal boundary line will
	    # be suppressed in these cases (see below) so that the overlap is clear.
	    
	    if ( $itop < $t_range )
	    {
		$itop = $t_range;
		$cell_topen{$ikey} = 1;
	    }
	    
	    if ( $ibase > $b_range )
	    {
		$ibase = $b_range;
		$cell_bopen{$ikey} = 1;
	    }
	    
	    # If either bound has been evaluated (interpolated) then mark it as such.
	    
	    $bound_intp{$itop} = 1 if $int->{t_type} eq 'interpolated';
	    $bound_intp{$ibase} = 1 if $int->{b_type} eq 'interpolated'; 
	    
	    # Store the label, color, and highlight for this cell.
	    
	    $cell_label{$ikey} = $iname;
	    $cell_label{$ikey} .= ' &dagger;' if $int->{obsolete} && $options->{mark_obsolete};
	    $cell_color{$ikey} = $int->{color} if $int->{color};
	    $cell_hi{$ikey} = 1 if $inum && $options->{highlight}{$inum};
	    
	    # Determine which column this interval should be placed into. The value of $c
	    # will be that column number.
	    
	    my $c = $min_col;
	    
	    # Find the top and bottom rows corresponding to the top and bottom
	    # boundaries of this interval.
	    
	    my ($rtop, $rbase);
	    
	    for my $r ( 0..$max_row )
	    {
		$rtop = $r if $bound2d[$r][0] eq $itop;
		$rbase = $r, last if $bound2d[$r][0] eq $ibase;
	    }
	    
	    # If either the top or the bottom age cannot be matched to a row, this
	    # interval cannot be placed.
	    
	    unless ( defined $rtop && defined $rbase )
	    {
		push @errors, "Could not place '$iname': bad bounds";
		next;
	    }
	    
	    # Place this interval either in the minimum column for this scale, or up to 10
	    # columns further to the right if necessary to avoid overlapping any interval
	    # that has already been placed.
	    
	  COLUMN:
	    while ( $c < $min_col+5 )
	    {
		# If this interval has a type that is different from the interval type for
		# the current column, move one column to the right and try again.
		
		if ( $col_type[$c] && $itype )
		{
		    $c++, next COLUMN unless $col_type[$c] eq $itype;
		}
		
		# Otherwise, set the interval type for this column to the type for this
		# interval if it is not already set.
		
		elsif ( $itype )
		{
		    $col_type[$c] ||= $int->{type};
		}
		
		# If there is already an interval at $rtop, and that interval
		# has the same top, bottom, and type as the current one, add the
		# current label to that cell and move on to the next interval.
		
		if ( my $pkey = $bound2d[$rtop][$c] )
		{
		    if ( $cell_top{$pkey} && $itop eq $cell_top{$pkey} &&
			 $cell_base{$pkey} && $ibase eq $cell_base{$pkey} &&
			 $cell_type{$pkey} && $itype eq $cell_type{$pkey} )
		    {
			delete $cell_label{$ikey};
			$cell_label{$pkey} .= '/' . $iname;
			$cell_combined{$pkey} //= '';
			$cell_combined{$pkey} .= '+s' . $ikey;
			$cell_hi{$pkey} = 1 if $cell_hi{$ikey};
			next INTERVAL;
		    }
		}
		
		# Otherwise, if any of the cells where this interval would be
		# placed are already occupied,  move one column to the right and
		# try again.
		
		for my $r ( $rtop..$rbase-1 )
		{
		    $c++, next COLUMN if $bound2d[$r][$c] && $bound2d[$r][$c];
		}
		
		# Set the column color if it isn't yet set, and then set the
		# cell color if it isn't yet set.
				
		if ( $snum ne INTL_SCALE )
		{
		    unless ( $col_color[$c] )
		    {
			$col_color[$c] = $itop > 65 ? $PALEO_COLOR[$cindex] : $CENO_COLOR[$cindex];
			$cindex++;
		    }
		    
		    $cell_color{$ikey} //= $col_color[$c];
		}
		
		# If this column doesn't yet have a scale number, assign it now.
		
		$col_scale[$c] ||= $snum;
		
		# If we get to this point, there is nothing to prevent us placing the
		# interval in the current column. So stop here.
		
		last COLUMN;
	    }
	    
	    # Keep track of the maximum column number used by this timescale.
	    
	    $max_col = $c if $c > $max_col;
	    
	    # If we ran past 5 columns, that means we ran out of columns to place
	    # this interval in.
	    
	    if ( $c - $min_col >= 5 )
	    {
		push @errors, "Could not place '$iname': not enough columns";
		next INTERVAL;
	    }
	    
	    # Otherwise, place the interval by storing its interval number in all of
	    # the cells from the top boundary row to just above the bottom
	    # boundary row.
	    
	    for my $r ( $rtop..$rbase-1 )
	    {
		$bound2d[$r][$c] = $ikey;
	    }
	    
	    # Store the height, boundary ages, and type for this cell.
	    
	    $cell_height{$ikey} = $rbase - $rtop;
	    $cell_top{$ikey} = $itop;
	    $cell_base{$ikey} = $ibase;
	    $cell_type{$ikey} = $itype;
	}
    }
    
    return { bound2d => \@bound2d,
	     col_scale => \@col_scale,
	     cell_height => \%cell_height,
	     cell_top => \%cell_top,
	     cell_base => \%cell_base,
	     cell_type => \%cell_type,
	     cell_label => \%cell_label,
	     cell_color => \%cell_color,
	     cell_combined => \%cell_combined,
	     cell_hi => \%cell_hi,
	     bound_intp => \%bound_intp,
	     max_row => $max_row,
	     max_col => $max_col,
	     errors => \@errors };
}


sub generate_ts_html {
    
    my ($class, $d, $scale_hash) = @_;
    
    # Phase III: generate HTML
    
    my $html_output = "<table class=\"ts_display\" id=\"ts_diagram\" onclick=\"selectInterval(event)\">\n";
    my @col_lastkey;
    
    foreach my $r ( 0 .. 0 )
    {
	$html_output .= "<tr>";
	
	my $last_scale = $d->{col_scale}[1];
	my $col_count = 0;
	
	foreach my $c ( 1 .. $d->{max_col} )
	{
	    if ( $d->{col_scale}[$c] ne $last_scale )
	    {
		my $label = $scale_hash->{$last_scale}{scale_name};
		$html_output .= generate_diagram_header($label, $col_count);
		$last_scale = $d->{col_scale}[$c];
		$col_count = 1;
	    }
	    
	    else
	    {
		$col_count++;
	    }
	    
	    $col_lastkey[$c] = 'init';
	}
	
	my $label = $scale_hash->{$last_scale}{scale_name};
	$html_output .= generate_diagram_header($label, $col_count);
	$html_output .= generate_diagram_corner();
	$html_output .= "</tr>\n";
    }
    
    foreach my $r ( 0 .. $d->{max_row} - 1 )
    {
	$html_output .= generate_diagram_row();
	
	foreach my $c ( 1 .. $d->{max_col} )
	{
	    my $ikey = $d->{bound2d}[$r][$c];
	    
	    no warnings 'uninitialized';
	    
	    if ( $ikey ne $col_lastkey[$c] )
	    {
		if ( $ikey )
		{
		    my $id = 's' . $ikey;
		    $id .= $d->{cell_combined}{$ikey} if $d->{cell_combined}{$ikey};
		    
		    my $range = $d->{cell_base}{$ikey} - $d->{cell_top}{$ikey};
		    
		    $html_output .= 
			generate_diagram_cell($id, $d->{cell_label}{$ikey}, $d->{cell_height}{$ikey},
					      $range, $d->{cell_color}{$ikey}, $d->{cell_hi}{$ikey});
		}
		
		else
		{
		    my $height = 0;
		    
		    foreach my $n ( $r .. $d->{max_row} - 1 )
		    {
			last if $d->{bound2d}[$n][$c];
			$height++;
		    }
		    
		    $html_output .= generate_diagram_empty_cell($height);
		}
		
		$col_lastkey[$c] = $ikey;
	    }
	}
	
	my $age = $d->{bound2d}[$r][0];
	
	$html_output .= generate_diagram_bound($r, $age, $d->{bound_intp}{$age});
	$html_output .= "</tr>\n";
    }
    
    foreach my $r ( $d->{max_row} .. $d->{max_row} )
    {
	last unless $r >= 0;
	
	my $age = $d->{bound2d}[$r][0];
	
	$html_output .= "<tr>";
	$html_output .= generate_diagram_bottom($d->{max_col});
	$html_output .= generate_diagram_bound($r, $age, $d->{bound_intp}{$age}, 1);
	$html_output .= "</tr>\n";
    }
    
    $html_output .= "</table>\n";
    
    return $html_output;
}


sub generate_diagram_header {
    
    my ($label, $width) = @_;
    
    if ( $width > 1 )
    {
	return "<th class=\"ts_header\" colspan=\"$width\">$label</th>";
    }
    
    else
    {
	return "<th class=\"ts_header\">$label</th>";
    }
}


sub generate_diagram_corner {
    
    return "<th class=\"ts_corner\"></th>";
}


sub generate_diagram_row {
    
    my ($height) = @_;
    
    if ( $height )
    {
	return "<tr style=\"height: ${height}px; max-height: ${height}px\">";
    }
    
    else
    {
	return "<tr>";
    }
}


sub generate_diagram_cell {
    
    my ($key, $label, $height, $range, $color, $highlight) = @_;
    
    my $id = '';
    $id = " id=\"$key\"" if $key;
    my $class = 'ts_interval';
    
    $class .= ' ts_highlight' if $highlight;
    
    my $style = '';
    $style = " style=\"background-color: $color\"" if $color;
    
    my $rowspan = '';
    $rowspan = " rowspan=\"$height\"" if $height > 1;
    
    return "<td class=\"$class\"$style$rowspan$id>$label</td>";
}


sub generate_diagram_empty_cell {
    
    my ($height) = @_;
    
    my $rowspan = '';
    $rowspan = " rowspan=\"$height\"" if $height > 1;
    
    return "<td class=\"ts_empty\"$rowspan></td>";
}


sub generate_diagram_bound {
    
    my ($row, $label, $interp, $is_last) = @_;
    
    my $id = "b$row";
    
    $label .= '&nbsp;*' if $interp;
    
    my $last = $is_last ? 'last' : 'bound';
    
    return "<td class=\"ts_bound\" id=\"$id\"><span class=\"ts_${last}label\">$label</span></td>";
}


sub generate_diagram_bottom {
    
    my ($width) = @_;
    
    return "<td class=\"ts_bottom\" colspan=\"$width\"></td>";
}

		

