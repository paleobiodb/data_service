#
# IntervalQuery
# 
# A class that returns information from the PaleoDB database about time intervals.
# This is a subclass of PBDataQuery.
# 
# Author: Michael McClennen

use strict;

package PB2::IntervalData;

use HTTP::Validate qw(:validators);
use Carp qw(carp croak);
use TableDefs qw(%TABLE);
use CoreTableDefs;

use IntervalBase qw(ts_defined ts_record ts_intervals ts_bounds ts_has_type ts_list
		    int_defined int_record int_bounds ints_by_prefix ts_list);
use ExternalIdent qw(VALID_IDENTIFIER generate_identifier %IDP %IDRE);

use List::Util qw(min max);

our (@REQUIRES_ROLE) = qw(PB2::CommonData);

use Moo::Role;

no warnings 'numeric';


# initialize ( )
# 
# This routine is called automatically by the data service to initialize this
# class.

sub initialize {

    my ($class, $ds) = @_;
    
    $ds->define_set('1.2:intervals:types' => 
	{ value => 'eon' },
	{ value => 'era' },
	{ value => 'period' },
	{ value => 'epoch' },
	{ value => 'subepoch' },
	{ value => 'age' },
	{ value => 'subage' },
	{ value => 'zone' },
	{ value => 'chron' },
        { value => 'bin' });
    
    # Define output blocks for displaying time interval information
    
    $ds->define_block('1.2:intervals:basic' =>
	{ select => [ qw(i.interval_no i.interval_name i.abbrev sm.scale_no 
			 sm.parent_no sm.color i.early_age i.late_age i.reference_no) ] },
	# { select => [ qw(i.interval_no i.interval_name i.abbrev sm.scale_no sm.scale_level
	# 		 sm.parent_no sm.color i.early_age i.late_age i.reference_no) ] },
	{ output => 'interval_no', com_name => 'oid' },
	    "The unique identifier of this interval",
	{ output => 'record_type', com_name => 'typ', value => $IDP{INT} },
	    "The type of this object: C<$IDP{INT}> for an interval",
	{ output => 'scale_no', com_name => 'tsc' },
	    "The time scale in which this interval is defined.  An interval may be reported more than",
	    "once, as a member of different time scales",
	{ output => 'interval_name', com_name => 'nam' },
	    "The name of this interval",
	{ output => 'abbrev', com_name => 'abr' },
	    "The standard abbreviation for the interval name, if any",
	{ output => 'type', com_name => 'itp' },
	    "The interval type: eon, era, period, epoch, subepoch, age, subage, zone",
	{ output => 'parent_no', com_name => 'pid' },
	    "The identifier of the parent interval, if any",
	{ output => 'color', com_name => 'col' },
	    "The standard color for displaying this interval",
	{ output => 't_age', pbdb_name => 't_age', com_name => 'lag', data_type => 'dec' },
	    "The top age of this interval, in Ma",
	{ output => 'b_age', pbdb_name => 'b_age', com_name => 'eag', data_type => 'dec' },
	    "The base age of this interval, in Ma",
	{ output => 'reference_no', com_name => 'rid' },
	    "The identifier of the bibliographic reference from which this data was entered",
	{ set => '*', code => \&process_int_ids });
    
    $ds->define_block('1.2:timescales:basic' =>
	{ output => 'scale_no', com_name => 'oid' },
	    "The unique identifier of this time scale",
	{ output => 'record_type', com_name => 'typ', value => $IDP{TSC} },
	    "The type of this object: C<$IDP{TSC}> for a time scale",
	{ output => 'scale_name', com_name => 'nam' },
	    "The name of this time scale",
	{ output => 't_age', pbdb_name => 't_age', com_name => 'lag', data_type => 'dec' },
	    "The top age of this time scale, in Ma",
	{ output => 'b_age', pbdb_name => 'b_age', com_name => 'eag', data_type => 'dec' },
	    "The base age of this time scale, in Ma",
	{ output => 'locality', com_name => 'loc' },
	    "The region of the world where this timescale is valid",
	{ output => 'reference_no', com_name => 'rid' },
	    "The identifier(s) of the references from which this data was entered",
	{ set => '*', code => \&process_int_ids });
    
    $ds->define_block('1.2:intervals:colls' => 
	{ output => 'colls_defined', com_name => 'nco' },
	    "The number of collections whose definition refers to this interval",
	{ output => 'colls_major', com_name => 'nmc' },
	    "The total number of collections within this interval's time range according",
	    "to the 'major' timerule",
	{ output => 'occs_major', com_name => 'nmo' },
	    "The total number of occurrences within this interval's time range according",
	    "to the 'major' timerule");
    
    $ds->define_output_map('1.2:intervals:output_map' =>
	{ value => 'colls', maps_to => '1.2:intervals:colls' },
	    "Show the number of collections whose definition refers to each",
	    "returned interval or timescale. This can be an expensive operation",
	    "if the number of intervals is large.");
    
    $ds->define_ruleset('1.2:intervals:show' =>
	{ optional => 'show', list => q{,},
	  valid => '1.2:intervals:output_map' },
	    "This parameter is used to select additional blocks of information to be returned",
	    "along with the basic record for each collection.  Its value should be",
	    "one or more of the following, separated by commas:");
    
    $ds->define_block('1.2:timescales:diagram' =>
	{ output => 'diagram', com_name => 'dgr' },
	    "An HTML table expression showing the intervals from the specified",
	    "scale(s) with correlated boundaries.",
	{ output => 'bounds', com_name => 'bnd' },
	    "A list of the ages of interval bounds displayed in the diagram");
    
    # Define the set of time resolution rules that we implement.
    
    $ds->define_set('1.2:timerules' =>
	{ value => 'contain' },
	    "Select only records whose temporal locality is strictly contained in the",
	    "specified time range. This is the most restrictive rule.  For diversity",
	    "output, this rule guarantees that each occurrence will fall into at most",
	    "one temporal bin, but many occurrences will be ignored because their temporal",
	    "locality is too wide to fall into any of the bins.",
	{ value => 'major' },
	    "Select only records for which more than 50% of the temporal locality range",
	    "falls within the specified time range. For diversity output, this rule also",
	    "guarantees that each occurrence will fall into at most one temporal bin.",
	    "Many occurrences will be ignored because their temporal locality is more",
	    "than twice as wide as any of the overlapping bins, but fewer will be ignored",
            "than with the C<contain> rule. This is the B<default> timerule unless you",
	    "specifically select one.",
	{ value => 'buffer', undocumented => 1 },
	    # "Select only records whose temporal locality overlaps the specified time range",
	    # "and also falls completely within a 'buffer zone' around this range.  This",
	    # "buffer defaults to 12 million years for the Paleozoic and Mesozoic and 5",
	    # "million years for the Cenozoic. You can override the buffer width using the",
	    # "parameters B<C<timebuffer>> and B<C<late_buffer>>.  For diversity output,",
	    # "some occurrences will be counted as falling into more than one bin.  Some",
	    # "occurrences will still be ignored, but fewer than with the above rules.",
	{ value => 'overlap' },
	    "Select only records whose temporal locality overlaps the specified time",
	    "range by any amount. This is the most permissive rule.  For diversity output,",
	    "every occurrence will be counted. Many will be counted as falling into more",
	    "than one bin.");

    # Then define some rulesets to describe the parameters accepted by the
    # operations defined here.
    
    $ds->define_ruleset('1.2:intervals:specifier' =>
	"You must specify one of the following parameters:",
	{ param => 'id', valid => VALID_IDENTIFIER('INT'), alias => 'interval_id' },
	    "Return the interval corresponding to the specified identifier.",
	{ param => 'name' },
	    "Return the interval with the specified name.",
	{ at_most_one => ['id', 'name'],
	  errmsg => "You may not specify both 'name' and 'id' in the same query." });
    
    $ds->define_ruleset('1.2:intervals:selector' => 
	{ param => 'all_records', valid => FLAG_VALUE },
	    "List all intervals known to the database.",
	{ param => 'scale_id', valid => VALID_IDENTIFIER('TSC'), list => ',' },
	    "Return intervals from the specified time scale(s).",
	    "The value of this parameter should be one or more scale identifiers separated",
	    "by commas.",
	{ param => 'scale', valid => ANY_VALUE, list => ',' },
	    "Return intervals from the specified time scale(s). The value",
	    "of this parameter should be one or more scale names or numbers, separated by",
	    "commas.",
	{ param => 'id', valid => VALID_IDENTIFIER('INT'), list => ',', alias => 'interval_id',
	  bad_value => '_' },
	    "Return intervals that have the specified identifier(s). You may enter one or more,",
	    "separated by commas.",
	{ param => 'name', list => ',' },
	    "Return intervals that have the specified name(s).",
	{ param => 'type', valid => '1.2:intervals:types', list => ',' },
	    "Return only intervals of the specified type(s):",
	{ param => 'min_ma', valid => DECI_VALUE(0) },
	    "Return only intervals that are at least this old",
	{ param => 'max_ma', valid => DECI_VALUE(0) },
	    "Return only intervals that are at most this old",
	{ optional => 'timerule', valid => '1.2:timerules' },
	    "You can use this parameter with C<max_ma> and C<min_ma> to determine which",
	    "intervals fall into the specified time range under the specified rule.",
	    "Accepted values are: ", "=over", 
	    "=item contain", "Report intervals that are contained in the specified range",
	    "=item major", "Report intervals for which more than 50% of their range overlaps",
	    "the specified range",
	    "=item overlap", "Report intervals whose range overlaps the specified range",
	    "=back",
	{ optional => 'order', valid => ENUM_VALUE('age', 'age.asc', 'age.desc', 
						   'name', 'name.asc', 'name.desc') },
	    "Return the intervals in the specified order.  Accepted values are ",
	    "C<age>, C<name>. You may append C<.asc> or C<.desc> to either.");
    
    $ds->define_ruleset('1.2:intervals:list' => 
	{ require => '1.2:intervals:selector' },
	{ allow => '1.2:intervals:show' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");

    $ds->define_ruleset('1.2:intervals:single' => 
	{ require => '1.2:intervals:specifier' },
	{ allow => '1.2:intervals:show' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:timescales:specifier' =>
	{ param => 'id', alias => ['scale_id'], valid => VALID_IDENTIFIER('TSC') },
	    "Return the time scale corresponding to the specified identifier.",
	{ param => 'name', alias => ['scale_name', 'scale'], valid => ANY_VALUE },
	    "Return the time scale corresponding to the specified name.",
	{ at_most_one => ['id', 'name'] });
    
    $ds->define_ruleset('1.2:timescales:selector' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "List all time scales known to the database.",
	{ param => 'id', alias => ['scale_id'], valid => VALID_IDENTIFIER('TSC'), list => ',' },
	    "Return time scales that have the specified identifier(s).",
	    "You may specify more than one, as a comma-separated list.",
	{ param => 'name', alias => ['scale_name', 'scale'], valid => ANY_VALUE, list => ',' },
	    "Return time scales that have the specified name(s). You may specify",
	    "more than one, as a comma-separated list.",
	{ param => 'type', valid => '1.2:intervals:types', list => ',' },
	    "Return only time scales that contain the specified interval type(s).",
	    "You may specify more than one, as a comma-separated list:",
	{ param => 'min_ma', valid => DECI_VALUE(0) },
	    "Return only time scales whose late boundary is at least this old.",
	{ param => 'max_ma', valid => DECI_VALUE(0) },
	    "Return only time scales whose early boundary is at most this old.");
    
    $ds->define_ruleset('1.2:timescales:single' =>
	{ require => '1.2:timescales:specifier' },
	{ allow => '1.2:intervals:show' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:timescales:list' =>
	{ require => '1.2:timescales:selector' },
	{ allow => '1.2:intervals:show' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:timescales:diagram' =>
	{ require => '1.2:timescales:selector' },
	{ optional => 'display', valid => ANY_VALUE },
	    "Display only the portion of the timescale corresponding to the",
	    "named interval or interval range. To specify a range, separate",
	    "two interval names with a dash.",
	{ optional => 'interval', valid => ANY_VALUE, list => ',' },
	    "Highlight the named interval(s) in the diagram. You can specify",
	    "more than one as a comma-separated list, and you can use identifiers",
	    "as well as names.",
	{ optional => 'range', valid => ANY_VALUE },
	    "Highlight all intervals in the specified range. Specify two",
	    "interval names or identifiers separated by a dash.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    # Now some rulesets that can be used by other parts of the application.
    
    $ds->define_ruleset('1.2:ma_selector' =>
	{ param => 'min_ma', valid => DECI_VALUE(0) },
	    "Return only records whose temporal locality is at least this old, specified in Ma.",
	{ param => 'max_ma', valid => DECI_VALUE(0) },
	    "Return only records whose temporal locality is at most this old, specified in Ma.");
    
    $ds->define_ruleset('1.2:interval_selector' =>
	{ param => 'interval_id', valid => VALID_IDENTIFIER('INT'), list => ',' },
	    "Return only records whose temporal locality falls within the given geologic time",
	    "interval or intervals, specified by numeric identifier.  B<If you specify more",
	    "than one interval, the time range used will be the contiguous period from the",
	    "beginning of the earliest to the end of the latest specified interval.>",
	{ param => 'interval', valid => ANY_VALUE },
	    "Return only records whose temporal locality falls within the named geologic time",
	    "interval or intervals, specified by name.  You may specify more than one interval,",
	    "separated by either commas or a dash.  B<If you specify more than one interval,",
	    "the time range used will be the contiguous period from the beginning of the",
	    "earliest to the end of the latest specified interval.>",
	{ at_most_one => ['interval_id', 'interval', 'min_ma'] },
	{ at_most_one => ['interval_id', 'interval', 'max_ma'] });
    
    $ds->define_ruleset('1.2:timerule_selector' =>
	{ optional => 'scale_id', valid => VALID_IDENTIFIER('TSC') },
	    "The identifier of the timescale to be used when binning occurrences. Use",
	    "1 for the International Chronostratigraphic Timescale, and 10 for the PBDB",
	    "Ten Million Year bins. If not specified, the default is 1.",
	{ optional => 'scale' },
	    "The name of the timescale to be used when binning occurrences. The default",
	    "is 'International Chronostratigraphic Timescale'.",
	{ at_most_one => ['scale_id', 'scale'] },
	{ optional => 'interval_type', valid => '1.2:intervals:types' },
	    "The interval type to be used when binning occurrences. If not specified,",
	    "the default interval type for the selected timescale is used.",
	{ optional => 'timerule', valid => '1.2:timerules', alias => 'time_rule', default => 'major' },
	    "Resolve temporal locality according to the specified rule, as listed below.  This",
	    "rule is applied to determine which occurrences, collections, and/or taxa will be selected if",
	    "you also specify an age range using any of the parameters listed immediately above.",
	    "For diversity output, this rule is applied to",
	    "place each occurrence into one or more temporal bins, or to ignore the occcurrence if it",
	    "does not match any of the bins.  The available rules are:",
	{ optional => 'timebuffer', alias => ['time_buffer', 'earlybuffer', 'early_buffer'], valid => DECI_VALUE(0,20) },
	    "Override the default buffer period when resolving",
	    "temporal locality.  The value must be given in millions of years.  This parameter",
	    "is only relevant if B<C<timerule>> is set to C<B<buffer>>.",
	{ optional => 'latebuffer', alias => 'late_buffer', valid => DECI_VALUE(0,20) },
	    "Override the default buffer period for the end of the time range when resolving temporal",
	    "locality.  This allows the buffer to be different on the late end of the interval than",
	    "on the early end.  The value must be given in millions of years.  This parameter is only relevant",
	    "if B<C<timerule>> is set to C<B<buffer>>.");
    
    # Read in all of the interval data, so we don't have to make lots of
    # queries for it later.
    
    my $dbh = $ds->get_connection;
    
    IntervalBase->cache_interval_data($dbh);
}


# get ( )
# 
# Query for all relevant information about the interval specified by the
# 'id' parameter.  Returns true if the query succeeded, false otherwise.

sub get {

    my ($request) = @_;
    
    $request->strict_check;
    $request->extid_check;
    
    my $int;
    
    if ( my $id = $request->clean_param('id') )
    {
	$int = int_record($id);
    }
    
    elsif ( my $name = $request->clean_param('name') )
    {
	$int = int_record($name);
    }
    
    if ( $int )
    {
	if ( $request->has_block('colls') )
	{
	    my $interval_no = $int->{interval_no};
	    my $coll_hash = $request->query_colls('int', $interval_no);
	    $int->{colls_defined} = $coll_hash->{$interval_no}{colls_defined} // '0';
	    $int->{colls_contained} = $coll_hash->{$interval_no}{colls_contained} // '0';
	    $int->{occs_contained} = $coll_hash->{$interval_no}{occs_contained} // '0';
	    $int->{colls_major} = $coll_hash->{$interval_no}{colls_major} // '0';
	    $int->{occs_major} = $coll_hash->{$interval_no}{occs_major} // '0';
	}
	
	$request->single_result($int);
    }
    
    else
    {
	die $request->exception(404, "Not found");
    }
}


# list ( )
# 
# Return a list of interval records matching the specified parameters.

sub list {
    
    my ($request) = @_;
    
    $request->strict_check;
    $request->extid_check;
    
    my (@result, %int_uniq, $use_list, @scale_list, %scale_uniq);
    
    my $max_ma = $request->clean_param('max_ma');
    my $min_ma = $request->clean_param('min_ma');
    
    my $has_max = defined $max_ma && $max_ma ne '';
    my $has_min = defined $min_ma && $min_ma ne '';
    
    my $timerule = $request->clean_param('timerule') || 'contain';
    
    my %has_type = map { $_ => 1 } $request->clean_param_list('type');
    
    my @int_select = $request->clean_param_list('id');
    push @int_select, $request->clean_param_list('name');
    
    my @scale_select = $request->clean_param_list('scale_id');
    push @scale_select, $request->clean_param_list('scale');
    
    if ( @int_select )
    {
	my %scale_selected;
	
	foreach my $s ( @scale_select )
	{
	    if ( my $scale_no = ts_defined($s) )
	    {
		$scale_selected{$scale_no} = 1;
	    }
	    
	    else
	    {
		$request->add_warning("timescale '$s' was not found");
	    }
	}	
	
	foreach my $i ( @int_select )
	{
	    my $interval_no = int_defined($i);
	    
	    if ( $interval_no )
	    {
		unless ( $int_uniq{$i} )
		{
		    $int_uniq{$i} = 1;
		    
		    my $int = int_record($i);
		    
		    if ( $has_max || $has_min )
		    {
			next if exclude_by_time($int, $timerule, $max_ma, $min_ma);
		    }
		    
		    next if %has_type && ! $has_type{$int->{type}};
		    next if @scale_select && ! $scale_selected{$int->{scale_no}};
		    
		    push @result, int_record($i);
		}
	    }
	    
	    else
	    {
		$request->add_warning("interval '$i' was not found");
	    }
	}
    }
    
    else
    {
	if ( @scale_select )
	{
	    foreach my $s ( @scale_select )
	    {
		if ( my $scale_no = ts_defined($s) )
		{
		    unless ( $scale_uniq{$scale_no} )
		    {
			$scale_uniq{$scale_no} = 1;
			push @scale_list, $scale_no;
		    }
		}
		
		else
		{
		    $request->add_warning("timescale '$s' was not found");
		}
	    }
	}
	
	else
	{
	    @scale_list = ts_list();
	}
	
	foreach my $s ( @scale_list )
	{
	    foreach my $int ( ts_intervals($s) )
	    {
		if ( $has_max || $has_min )
		{
		    next if exclude_by_time($int, $timerule, $max_ma, $min_ma);
		}
		
		# next if $has_max && $int->{b_age} > $max_ma;
		# next if $has_min && $int->{t_age} < $min_ma;
		next if %has_type && ! $has_type{$int->{type}};
		
		push @result, $int;
	    }
	}
    }
    
    if ( $request->has_block('colls') )
    {
	my $coll_hash;
	
	if ( @result )
	{
	    my @int_nos = map { $_->{interval_no} } @result;
	    $coll_hash = $request->query_colls('int', @int_nos);
	}
	
	if ( $coll_hash )
	{
	    foreach my $int ( @result )
	    {
		my $interval_no = $int->{interval_no};
		$int->{colls_defined} = $coll_hash->{$interval_no}{colls_defined} // '0';
		$int->{colls_contained} = $coll_hash->{$interval_no}{colls_contained} // '0';
		$int->{occs_contained} = $coll_hash->{$interval_no}{occs_contained} // '0';
		$int->{colls_major} = $coll_hash->{$interval_no}{colls_major} // '0';
		$int->{occs_major} = $coll_hash->{$interval_no}{occs_major} // '0';
	    }
	}
    }
    
    my $order = $request->clean_param('order');
    
    if ( $order eq 'age' || $order eq 'age.asc' )
    {
	@result = sort { $a->{t_age} <=> $b->{t_age} ||
			     $b->{b_age} <=> $a->{b_age} ||
			     $a->{scale_no} <=> $b->{scale_no} } @result;
    }
    
    elsif ( $order eq 'age.desc' )
    {
	@result = sort { $b->{b_age} <=> $a->{b_age} ||
			     $a->{t_age} <=> $b->{t_age} ||
			     $a->{scale_no} <=> $b->{scale_no} } @result;
    }
    
    elsif ( $order eq 'name' || $order eq 'name.asc' )
    {
	@result = sort { $a->{interval_name} cmp $b->{interval_name} ||
			     $a->{scale_no} <=> $b->{scale_no} } @result;
    }
    
    elsif ( $order eq 'name.desc' )
    {
	@result = sort { $b->{interval_name} cmp $a->{interval_name} ||
			     $a->{scale_no} <=> $b->{scale_no} } @result;
    }
    
    $request->list_result(@result);
    $request->set_result_count(scalar(@result));
}


# exclude_by_time ( interval, timerule, max_ma, min_ma )
# 
# Return true if the specified interval falls outside of the specified range
# according to the specified timerule. Return false otherwise.

sub exclude_by_time {
    
    my ($int, $timerule, $max_ma, $min_ma) = @_;
    
    if ( $timerule eq 'contain' )
    {
	return 1 if defined $max_ma && $max_ma ne '' && $int->{b_age} > $max_ma;
	return 1 if defined $min_ma && $min_ma ne '' && $int->{t_age} < $min_ma;
    }
    
    elsif ( $timerule eq 'overlap' )
    {
	return 1 if defined $max_ma && $max_ma ne '' && $int->{t_age} >= $max_ma;
	return 1 if defined $min_ma && $min_ma ne '' && $int->{b_age} <= $min_ma;
    }
    
    else # timerule = 'major'
    {
	if ( defined $max_ma && $max_ma ne '' )
	{
	    return 1 if $int->{t_age} >= $max_ma;
	    
	    if ( defined $min_ma && $min_ma ne '' )
	    {
		return 1 if $int->{b_age} <= $min_ma;
		
		if ( $int->{b_age} >= $max_ma )
		{
		    if ( $int->{t_age} >= $min_ma )
		    {
			return 1 if ($max_ma - $int->{t_age}) / ($int->{b_age} - $int->{t_age}) < 0.5;
		    }
		    
		    else
		    {
			return 1 if ($max_ma - $min_ma) / ($int->{b_age} - $int->{t_age}) < 0.5;
		    }
		}
		
		else
		{
		    return 1 if ($int->{b_age} - $min_ma) / ($int->{b_age} - $int->{t_age}) < 0.5;
		}
	    }
	    
	    elsif ( $int->{b_age} > $max_ma )
	    {
		return 1 if ($max_ma - $int->{t_age}) / ($int->{b_age} - $int->{t_age}) < 0.5;
	    }
	}
	
	elsif ( defined $min_ma && $min_ma ne '' )
	{
	    return 1 if $int->{b_age} <= $min_ma;
	    
	    if ( $int->{t_age} < $min_ma )
	    {
		return 1 if ($int->{b_age} - $min_ma) / ($int->{b_age} - $int->{t_age}) < 0.5;
	    }
	}
    }
    
    # If this interval is not excluded, return false.
    
    return '';
}


# list_timescales ( )
# 
# Query the database for time scales.

sub list_timescales {
    
    my ($request, $arg) = @_;
    
    $request->strict_check;
    $request->extid_check;
    
    my (@scale_list, @to_filter, @result);
    
    @scale_list = $request->clean_param_list('id');
    push @scale_list, $request->clean_param_list('name');
    
    if ( @scale_list )
    {
	foreach my $s ( @scale_list )
	{
	    push @to_filter, ts_record($s);
	}
    }
    
    else
    {
	@to_filter = map { ts_record($_) } ts_list();
    }
    
    my $max_ma = $request->clean_param('max_ma');
    my $min_ma = $request->clean_param('min_ma');
    
    my $has_max = defined $max_ma && $max_ma ne '';
    my $has_min = defined $min_ma && $min_ma ne '';
    
    my @types = $request->clean_param_list('type');
    
    foreach my $scale ( @to_filter )
    {
	next if $has_max && $scale->{late_age} >= $max_ma;
	next if $has_min && $scale->{early_age} <= $min_ma;
	next if @types && ! ts_has_type($scale->{scale_no}, @types);
	
	push @result, $scale;
    }
    
    if ( $request->has_block('colls') )
    {
	my @scale_nos = map { $_->{scale_no} } @result;
	
	my $coll_hash = $request->query_colls('ts', @scale_nos);
	
	foreach my $scale ( @result )
	{
	    my $scale_no = $scale->{scale_no};
	    $scale->{colls_defined} = $coll_hash->{$scale_no}{colls_defined} // '0';
	}
    }
    
    $request->list_result(@result);
    $request->set_result_count(scalar(@result));
    
    if ( $arg && $arg eq 'single' && ! @result )
    {
	die $request->exception(404, "Not found");
    }    
}


sub query_colls {
    
    my ($request, $type, @args) = @_;
    
    my $dbh = $request->get_connection();
    
    my $id_string = join(',', @args);
    my $sql;
    
    if ( $type eq 'int' )
    {
	$sql = "SELECT * FROM $TABLE{OCC_INT_SUMMARY}
		WHERE interval_no in ($id_string)";
	
	# $sql = "SELECT i.interval_no, count(*) as n_colls
	# 	FROM $TABLE{INTERVAL_DATA} as i 
	# 	    join $TABLE{COLLECTION_DATA} as c on
	# 		i.interval_no = c.max_interval_no or
	# 		i.interval_no = c.min_interval_no or
	# 		i.interval_no = c.ma_interval_no
	# 	WHERE i.interval_no in ($id_string)
	# 	GROUP BY i.interval_no";
    }
    
    elsif ( $type eq 'int_by_ts' )
    {
	$sql = "SELECT * FROM $TABLE{OCC_INT_SUMMARY} as s
		WHERE scale_no in ($id_string)";
	
	# $sql = "SELECT i.interval_no, count(*) as n_colls
	# 	FROM $TABLE{INTERVAL_DATA} as i
	# 	    join $TABLE{SCALE_MAP} as sm using (interval_no)
	# 	    join $TABLE{COLLECTION_DATA} as c on
	# 		i.interval_no = c.max_interval_no or
	# 		i.interval_no = c.min_interval_no or
	# 		i.interval_no = c.ma_interval_no
	# 	WHERE sm.scale_no in ($id_string)
	# 	GROUP BY i.interval_no";
    }
    
    elsif ( $type eq 'ts' )
    {
	$sql = "SELECT * FROM $TABLE{OCC_TS_SUMMARY} as s
		WHERE scale_no in ($id_string)";
	
	# $sql = "SELECT sm.scale_no, count(*) as n_colls
	# 	FROM $TABLE{COLLECTION_DATA} as c join $TABLE{SCALE_MAP} as sm
	# 		on sm.interval_no = c.max_interval_no or
	# 		   sm.interval_no = c.min_interval_no or
	# 		   sm.interval_no = c.ma_interval_no
	# 	     join $TABLE{INTERVAL_DATA} as i using (interval_no)
	# 	WHERE i.scale_no = sm.scale_no and sm.scale_no in ($id_string)
	# 	GROUP BY sm.scale_no";
    }
    
    my $colls_hash = { };
    
    $request->debug_line("$sql\n") if $request->debug;
    
    foreach my $r ( $dbh->selectall_array($sql, { Slice => { } }) )
    {
	my $id = $r->{interval_no} || $r->{scale_no};
	$colls_hash->{$id} = $r;
    }
    
    return $colls_hash;
}


# diagram_timescales ( )
# 
# Generate a diagram of the specified timescale(s).

sub diagram_timescales {
    
    my ($request) = @_;
    
    # Get a list of the scales to display.
    
    my @scale_list = $request->clean_param_list('id');
    push @scale_list, $request->clean_param_list('name');
    
    $request->strict_check;
    
    my (%sdata, %ssequence);
    
    # Check to make sure that all of the specified scales are defined.
    
    foreach my $s ( @scale_list )
    {
	unless ( ts_defined($s) )
	{
	    die $request->exception('400', "Scale $s is not defined");
	}
	
	$sdata{$s} = ts_record($s);
	$ssequence{$s} = [ ts_intervals($s) ];
    }
    
    # Check for other parameters.
    
    my $options = { };
    
    if ( my $display = $request->clean_param('display') )
    {
	$display = lc $display;
	
	if ( $display =~ /(.*?)-(.*)/ )
	{
	    die $request->exception(400, "unknown interval '$1'")
		unless int_defined($1);
	    
	    die $request->exception(400, "unknown interval '$2'")
		unless int_defined($2);
	    
	    my ($b1, $t1) = int_bounds($1);
	    my ($b2, $t2) = int_bounds($2);
	    
	    $options->{t_limit} = min($t1, $t2);
	    $options->{b_limit} = max($b1, $b2);
	}
	
	else
	{
	    die $request->exception(400, "unknown interval '$display'")
		unless int_defined($display);
	    
	    ($options->{b_limit}, $options->{t_limit}) = int_bounds($display);
	}
    }
    
    if ( my @intervals = $request->clean_param_list('interval') )
    {
	foreach ( @intervals )
	{
	    if ( my $interval_no = int_defined($_) )
	    {
		$options->{highlight}{$interval_no} = 1;
	    }
	}
    }
    
    if ( my @ids = $request->clean_param_list('interval_id') )
    {
	foreach ( @ids )
	{
	    if ( my $interval_no = int_defined($_) )
	    {
		$options->{highlight}{$interval_no} = 1;
	    }
	}
    }
    
    # Generate the diagram.
    
    my $d = IntervalBase->generate_ts_diagram($options, \%sdata, \%ssequence, @scale_list);
    
    # Generate the HTML expression and list of bounds.
    
    my $html_output = IntervalBase->generate_ts_html($d, \%sdata);
    
    my @bounds_list = map { $_->[0] } $d->{bound2d}->@*;
    
    if ( ref $d->{errors} eq 'ARRAY' && $d->{errors}->@* )
    {
	foreach my $m ( $d->{errors}->@* )
	{
	    $request->add_warning($m);
	}
    }
    
    # Return this information to the client process.
    
    $request->single_result({ diagram => $html_output,
			      bounds => \@bounds_list });
}


# process_interval_params ( )
# 
# If the current request includes any of the following parameters:
# 
#    interval
#    interval_id
#    max_ma
#    min_ma
# 
# return information sufficient to generate appropriate SQL filter
# expressions.  This includes the max and min Ma values, plus the interval
# identifiers for the earliest and latest intervals.  The Ma range returned
# spans all of the given intervals, no matter how many or in which order they
# were specified.  Any number of intervals can be specified, separated by
# either dashes or commas.
#
# If the parameter $not_strict is true, then don't throw an exception for warnings.
# 
# Returns: $max_ma, $min_ma, $early_interval_no, $late_interval_no

sub process_interval_params {
    
    my ($request, $no_warnings) = @_;
    
    my (@ids, @warnings, @errors);
    my ($max_ma, $min_ma, $early_interval_no, $late_interval_no, $early_duration, $late_duration);
    
    # If this routine has already been called for this request, just return the result.

    if ( ref $request->{my_interval_bounds}  eq 'ARRAY' )
    {
	return @{$request->{my_interval_bounds}};
    }
    
    # First check for each of the relevant parameters.
    
    if ( $request->param_given('interval_id') )
    {
	foreach my $id ( $request->clean_param_list('interval_id') )
	{
	    if ( int_defined($id) )
	    {
		push @ids, int_defined($id);
	    }
	    
	    elsif ( $id )
	    {
		push @warnings, "The interval identifier '$id' was not found in the database.";
	    }
	}
	
	unless ( @ids )
	{
	    push @warnings, "No valid interval identifiers were given.";
	}
    }
    
    elsif ( my $interval_name_value = $request->clean_param('interval') )
    {
	foreach my $name ( split qr{\s*[,-]+\s*}, $interval_name_value )
	{
	    next unless defined $name && $name =~ qr{\S};
	    
	    if ( $name =~ $IDRE{INT} )
	    {
		$name = $1;
	    }
	    
	    if ( int_defined($name) )
	    {
		push @ids, int_defined($name);
	    }
	    
	    else
	    {
		push @warnings, "Unknown interval '$name.'";
	    }
	}
	
	unless ( @ids )
	{
	    push @errors, "No valid interval names were given.";
	}
    }
    
    else
    {
	my $value = $request->clean_param('max_ma');
	
	if ( defined $value && $value ne '' )
	{
	    $max_ma = $value;
	}
	
	if ( my $value = $request->clean_param('min_ma') )
	{
	    $min_ma = $value;
	}

	if ( defined $min_ma && defined $max_ma && $min_ma >= $max_ma )
	{
	    push @warnings, "The value of 'min_ma' is greater than or equal to the value of 'max_ma'.";
	}

	elsif ( defined $max_ma && $max_ma == 0 )
	{
	    push @warnings, "The value of 'max_ma' must be greater than zero.";
	}
    }
    
    # If we have found any errors, report them and abort the request.
    
    if ( @errors || @warnings )
    {
	# if ( $not_strict && ! @errors )
	# {
	#     $request->add_warning(@warnings);
	# }
	
	if ( @errors )
	{
	    $request->add_warning(@warnings) if @warnings;
	    
	    my $errstring = join(' ', @errors);
	    die "400 $errstring\n";
	}
	
	else
	{
	    my $errstring = join(' ', @warnings);
	    die "400 $errstring\n";
	}
    }
    
    # If we have one or more interval ids, scan through to find the earliest
    # and latest.
    
    foreach my $interval_no ( @ids )
    {
	my ($new_max, $new_min) = int_bounds($interval_no);
	
	if ( !defined $max_ma || $new_max >= $max_ma )
	{
	    $max_ma = $new_max;
	    $early_interval_no = $interval_no + 0;
	    
	    # if ( !$early_duration || ($i->{early_age} - $i->{late_age}) < $early_duration )
	    # {
	    # 	$early_interval_no = $interval_no + 0;
	    # 	$early_duration = $i->{early_age} - $i->{late_age};
	    # }
	}
	
	if ( !defined $min_ma || $new_min <= $min_ma )
	{
	    $min_ma = $new_min;
	    $late_interval_no = $interval_no + 0;
	    
	    # if ( !$late_duration || ($i->{early_age} - $i->{late_age}) < $late_duration )
	    # {
	    # 	$late_interval_no = $interval_no + 0;
	    # 	$late_duration = $i->{early_age} - $i->{late_age};
	    # }
	}
    }
    
    # Now return the results.

    $request->{my_interval_bounds} = [ $max_ma, $min_ma, $early_interval_no, $late_interval_no ];
    
    return ($max_ma, $min_ma, $early_interval_no, $late_interval_no);
}


# interval_age_range ( interval_names )
# 
# Given an interval name or range, return the first and last ages.

sub interval_age_range {

    my ($request, $name) = @_;
    
    if ( int_defined($name) )
    {
	return int_bounds($name);
    }
    
    elsif ( $name =~ qr{ ^ (\w+) \s* - \s* (\w+) $ }xs )
    {
	if ( int_defined($1) && int_defined($2) )
	{
	    my ($b1, $t1) = int_bounds($1);
	    my ($b2, $t2) = int_bounds($2);
	    
	    my $b = max($b1, $b2);
	    my $t = min($t1, $t2);
	    
	    return ($b, $t);
	}
    }
    
    return;
}


# bin_by_interval ( record, bounds_list, timerule, timebuffer, latebuffer )
# 
# Given a record representing an occurrence or collection and a list of interval bounds
# representing time bins, return a list of the bins into which the record falls according
# according to the specified time rule. If the timerule is 'buffer', then a timebuffer and
# optionally a latebuffer value can also be provided.
# 
# Any record can be passed to this method, as long as it has the fields 'early_age' and
# 'late_age'.

sub bin_by_interval {
    
    my ($request, $record, $bounds_list, $timerule, $timebuffer, $latebuffer) = @_;
    
    return unless defined $record->{early_age} && defined $record->{late_age};
    
    my $occ_early = $record->{early_age} + 0;
    my $occ_late = $record->{late_age} + 0;
    
    my $interval_key = "$occ_early-$occ_late";
    
    # If we have already computed the list of bins for the specified age range, then we don't need
    # to do that again.
    
    if ( my $bin_list = $request->{"my_intervals_$timerule"}{$interval_key} )
    {
	return @$bin_list;
    }
    
    # Otherwise, we need to go through every interval in the timescale selected for this request
    # and pick the ones into which this record should be binned.
    
    my @occ_bins;
    my $last = scalar(@$bounds_list) - 2;
    
 INTERVAL:
    foreach my $i ( 0 .. $last )
    {
	# Skip all intervals that do not overlap with the occurrence range, and stop the scan when
	# we have passed that range.
	
	my $late_bound = $bounds_list->[$i+1];
	
	next INTERVAL if $late_bound >= $occ_early;
	
	my $early_bound = $bounds_list->[$i];
	
	last INTERVAL if $early_bound <= $occ_late;
	
	# Skip any interval that is not selected by the specified timerule.  Note that the
	# 'overlap' timerule includes everything that overlaps.
	
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
	    my $early_buffer = $timebuffer || ($early_bound > 66 ? 12 : 5);
	    
	    next INTERVAL if $occ_early > $early_bound + $early_buffer;
	    
	    my $late_buffer = $latebuffer || $early_buffer;
	    
	    next INTERVAL if $occ_late < $late_bound - $late_buffer;
	}
	
	# If we are not skipping this interval, add it to the list.
	
	push @occ_bins, $early_bound;
	
	# If we are using timerule 'major' or 'contains', then stop
	# the scan because each occurrence gets assigned to only one
	# bin.
	
	last INTERVAL if $timerule eq 'contains' || $timerule eq 'major';
    }
    
    # Return the list of matching bins, but also remember it in case other occurrences are found
    # with the same age range.

    $request->{"my_intervals_$timerule"}{$interval_key} = \@occ_bins;
    
    return @occ_bins;
}


# auto_complete_int ( name, limit )
# 
# This routine returns interval records matching the specified name, and is intended for use with
# auto-completion in client applications. The parameter $name must contain at least three letters,
# or else an empty result is returned. The parameter $limit specifies the maximum number of
# matches to be returned.

sub auto_complete_int {
    
    my ($request, $name, $limit) = @_;
    
    # Return an empty result if the argument starts with 'early', 'middle', or 'late' or some
    # prefix thereof, and does not specify any other letters. There is no point in matching until
    # we have at least a few letters of the base interval name.
    
    return if $name =~ qr{ ^ (?: e(a(r(ly?)?)?)? \s* | m(i(d(d(le?)?)?)?)? \s* | l(a(te?)?)? \s* ) $ }xsi;
    
    # Take out 'early', 'middle' or 'late' if they occur at the beginning of the name, and
    # lowercase it. (Fold case isn't needed because interval names are all in the unaccented roman
    # alphabet). Return an empty result unless we have at least three characters, and extract the
    # first three to look up using %IPREFIX.
    
    my $search_name = lc $name;
    
    $search_name =~ s/ ^early\s* | ^middle\s* | ^late\s* //xs;
    
    my $prefix = substr($search_name, 0, 3);
    my $name_len = length($name);
    
    return unless length($prefix) == 3;
    
    # For each interval with the specified prefix, check whether it matches the full name
    # given. If so, add this to the results. But stop once we have reached the specified limit.
    
    my @results;
    my $count;
    my $use_extids = $request->has_block('extids');
    
    foreach my $i ( ints_by_prefix($prefix) )
    {
	if ( lc substr($i->{interval_name}, 0, $name_len) eq lc $name )
	{
	    my $record_id = $use_extids ? generate_identifier('INT', $i->{interval_no}) : $i->{interval_no};
	    
	    push @results, { name => $i->{interval_name}, record_type => 'int', record_id => $record_id,
			     early_age => $i->{early_age}, late_age => $i->{late_age} };
	    
	    last if ++$count >= $limit;
	}
    }
    
    return @results;
}


# initOutput ( )
# 
# This routine is used in case we are generating hierarchical output, setting
# up the necessary data structures to do so.  If we are just generating a
# list, this is irrelevant but harmless.

sub initOutput {

    my ($self) = @_;
    
    $self->{tree_stack} = [];
    $self->{comma_stack} = [];
    
    return;
}


sub process_int_ids {
    
    my ($request, $record) = @_;
    
    return unless $request->{block_hash}{extids};

    # my $make_ids = $request->clean_param('extids');
    # $make_ids = 1 if ! $request->param_given('extids') && $request->output_vocab eq 'com';
    
    # return unless $make_ids;
    
    # $request->delete_output_field('record_type');
    
    foreach my $f ( qw(interval_no parent_no) )
    {
	$record->{$f} = generate_identifier('INT', $record->{$f}) if defined $record->{$f};
	# $record->{$f} = "$IDP{INT}:$record->{$f}" if defined $record->{$f};
    }
    
    foreach my $f ( qw(scale_no) )
    {
	$record->{$f} = generate_identifier('TSC', $record->{$f}) if defined $record->{$f};
	# $record->{$f} = "scl$record->{$f}" if defined $record->{$f};
    }
    
    if ( ref $record->{reference_no} eq 'ARRAY' )
    {
	foreach my $r ( $record->{reference_no}->@* )
	{
	    $r = generate_identifier('REF', $r);
	}
    }
    
    elsif ( $record->{reference_no} )
    {
	$record->{reference_no} = generate_identifier('REF', $record->{reference_no});
    }
}

1;
