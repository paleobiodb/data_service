#
# Paleobiology Database library modules
# -------------------------------------
#
# This file contains unit tests for library modules that are used by the EditTransaction
# client modules and by the data service.
#
# lib-03-intervalbase.t : Test IntervalBase.pm, which caches the timescales and intervals
#                         and answers questions about them: names, bounds, correlations,
#                         searches by age, interval boundaries, and the timescale diagrams
#                         and their HTML.
#
# IntervalBase reads the real interval and timescale tables, which these tests do not
# write to. The expected values are read from those tables with SQL, independently of the
# module. The diagram tests use made-up intervals, so that their layout is known exactly,
# and one check against the real international timescale.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 10;

use TableDefs qw(%TABLE);

use PBDBTester;

use IntervalBase qw(ts_defined ts_name ts_bounds ts_record ts_intervals ts_has_type ts_by_age
		    ts_list ts_boundary_list ts_boundary_no ts_boundary_name ts_boundary_next
		    int_defined int_bounds int_scale int_name int_type int_correlation
		    int_container int_record ints_by_age ts_intervals_by_age
		    interval_nos_by_age ints_by_prefix classic_scale_order classic_scale_map
		    INTL_SCALE BIN_SCALE AGE_RE);


# Establish the tester. No tables are switched to the test database, because IntervalBase
# only reads the interval tables.

my $T = PBDBTester->new('CollectionEdit');

my $dbh = $T->dbh;


# The main definition of each interval, read from the real tables: the interval_data
# record and the scale_map record in the interval's own scale.

my (%INT, %BYNAME);


# Return a list of numbers as a sorted string, for comparing sets of intervals.

sub nos { return join(',', sort { $a <=> $b } @_) }


# Return the interval numbers of a list of interval records, as a sorted string.

sub rec_nos { return nos(map { $_->{interval_no} } @_) }


# Return the number of values returned by a function. Some of the functions return an
# empty list with a bare 'return', which gives undef in scalar context.

sub count { my @list = @_; return scalar(@list) }


# Return the interval numbers of the main definitions that satisfy the condition, as a
# sorted string.

sub expected { my ($cond) = @_; return nos(map { $_->{interval_no} } grep { $cond->($_) } values %INT) }


# Load the interval cache, and read the main definitions for comparison.

subtest 'cache' => sub {
    
    ok( ! IntervalBase::cache_filled(), "the cache starts out empty" );
    
    IntervalBase->cache_interval_data($dbh);
    
    ok( IntervalBase::cache_filled(), "the cache was filled" ) || BAIL_OUT("no interval data");
    
    my $rows = $dbh->selectall_arrayref("
		SELECT i.interval_no, i.interval_name, i.early_age + 0 as b_age, i.late_age + 0 as t_age,
		       sm.scale_no, sm.type
		FROM $TABLE{SCALE_MAP} as sm join $TABLE{INTERVAL_DATA} as i using (interval_no)
		WHERE sm.scale_no = i.scale_no", { Slice => {} });
    
    # The ages are numbers, so that they are written the way the module writes them,
    # without trailing zeros.
    
    foreach my $r ( @$rows )
    {
	$r->{b_age} += 0;
	$r->{t_age} += 0;
    }
    
    %INT = map { $_->{interval_no} => $_ } @$rows;
    %BYNAME = map { lc $_->{interval_name} => $_ } @$rows;
    
    is( scalar(keys %IntervalBase::IDATA), scalar(keys %INT), "every interval was cached" );
    
    my $scales = $dbh->selectcol_arrayref("SELECT scale_no FROM $TABLE{SCALE_DATA} ORDER BY scale_no");
    
    is( join(',', ts_list()), join(',', @$scales), "ts_list returns every scale in order" );
    
    # A second call does not read the tables again.
    
    my $before = \%IntervalBase::IDATA;
    my $first = $IntervalBase::IDATA{$rows->[0]{interval_no}};
    
    IntervalBase->cache_interval_data($dbh);
    
    ok( $IntervalBase::IDATA{$rows->[0]{interval_no}} == $first, "a second call leaves the cache alone" );
    
    is( INTL_SCALE, 1, "INTL_SCALE" );
    is( BIN_SCALE, 10, "BIN_SCALE" );
    like( '72.2', AGE_RE, "AGE_RE matches an age" );
    unlike( 'abc', AGE_RE, "AGE_RE does not match a word" );
};


# Each timescale can be looked up by number or by name, in any case.

subtest 'timescales' => sub {
    
    my $s = $dbh->selectrow_hashref("SELECT scale_no, scale_name, early_age + 0 as b, late_age + 0 as t
		FROM $TABLE{SCALE_DATA} WHERE scale_no = 1");
    
    is( ts_defined(1), 1, "ts_defined by number" );
    is( ts_defined(uc $s->{scale_name}), 1, "ts_defined by name, in any case" );
    is( ts_defined('No such timescale'), undef, "an unknown name is not defined" );
    is( ts_defined(99999), undef, "an unknown number is not defined" );
    is( ts_name(1), $s->{scale_name}, "ts_name by number" );
    is( ts_name(lc $s->{scale_name}), $s->{scale_name}, "ts_name by name" );
    
    my ($b, $t) = ts_bounds(1);
    
    ok( $b == $s->{b} && $t == $s->{t}, "ts_bounds by number" );
    is( join(',', ts_bounds($s->{scale_name})), "$b,$t", "ts_bounds by name" );
    unlike( "$b,$t", qr{[.]0|[.]$}, "trailing zeros are removed from the ages" );
    is( count(ts_bounds('No such timescale')), 0, "an unknown timescale has no bounds" );
    
    my $r = ts_record(1);
    
    is( $r->{scale_name}, $s->{scale_name}, "ts_record" );
    
    $r->{scale_name} = 'changed';
    
    is( ts_name(1), $s->{scale_name}, "ts_record returns a copy" );
    is( ts_record('No such timescale'), undef, "an unknown timescale has no record" );
    is( ts_record(1)->{default_type}, 'age', "the default type of the international scale is 'age'" );
    is( ts_record(BIN_SCALE)->{default_type}, 'bin', "the default type of the bin scale is 'bin'" );
    
    # The intervals of a timescale are in sequence order.
    
    my $seq = $dbh->selectcol_arrayref("SELECT interval_no FROM $TABLE{SCALE_MAP}
		WHERE scale_no = 20 ORDER BY sequence");
    
    is( join(',', map { $_->{interval_no} } ts_intervals(20)), join(',', @$seq),
	"ts_intervals returns the intervals of a scale in sequence" );
    is( join(',', map { $_->{interval_no} } ts_intervals(ts_name(20))), join(',', @$seq),
	"ts_intervals by name" );
    is( count(ts_intervals('No such timescale')), 0, "an unknown timescale has no intervals" );
    
    my $types = $dbh->selectcol_arrayref("SELECT distinct type FROM $TABLE{SCALE_MAP} WHERE scale_no = 20");
    
    ok( ts_has_type(20, $types->[0]), "ts_has_type: scale 20 has '$types->[0]'" );
    ok( ts_has_type(ts_name(20), 'nosuchtype', $types->[0]), "ts_has_type by name, with several types" );
    is( ts_has_type(20, 'nosuchtype'), '', "ts_has_type: a type the scale does not have" );
    
    # ts_by_age finds the timescales other than the international one that overlap or are
    # contained in a range of ages.
    
    my $overlap = $dbh->selectcol_arrayref("SELECT scale_no FROM $TABLE{SCALE_DATA}
		WHERE early_age > 66 and late_age < 72.2 and scale_no <> 1");
    my $contained = $dbh->selectcol_arrayref("SELECT scale_no FROM $TABLE{SCALE_DATA}
		WHERE early_age <= 143.1 and late_age >= 66 and scale_no <> 1");
    
    is( nos(map { $_->{scale_no} } ts_by_age(72.2, 66, 'overlap')), nos(@$overlap),
	"ts_by_age 'overlap'" );
    is( nos(map { $_->{scale_no} } ts_by_age(143.1, 66, 'contained')), nos(@$contained),
	"ts_by_age 'contained'" );
    
    eval { ts_by_age(72.2, 66, 'bogus') };
    
    like( $@, qr{invalid selector 'bogus'}, "ts_by_age rejects an unknown selector" );
};


# Each interval can be looked up by number or by name.

subtest 'intervals' => sub {
    
    my $m = $BYNAME{maastrichtian};
    
    is( int_defined('Maastrichtian'), $m->{interval_no}, "int_defined by name" );
    is( int_defined('MAASTRICHTIAN'), $m->{interval_no}, "int_defined in any case" );
    is( int_defined($m->{interval_no}), $m->{interval_no}, "int_defined by number" );
    is( int_defined('Nosuchian'), undef, "an unknown interval is not defined" );
    is( int_defined(''), undef, "an empty name is not defined" );
    is( int_name($m->{interval_no}), 'Maastrichtian', "int_name by number" );
    is( int_name('maastrichtian'), 'Maastrichtian', "int_name by name" );
    is( int_scale('Maastrichtian'), $m->{scale_no}, "int_scale by name" );
    is( int_scale($m->{interval_no}), $m->{scale_no}, "int_scale by number" );
    is( int_type($m->{interval_no}), $m->{type}, "int_type by number" );
    
    my ($b, $t) = int_bounds('Maastrichtian');
    
    ok( $b == $m->{b_age} && $t == $m->{t_age}, "int_bounds by name" );
    is( join(',', int_bounds($m->{interval_no})), "$b,$t", "int_bounds by number" );
    is( count(int_bounds('Nosuchian')), 0, "an unknown interval has no bounds" );
    
    my $h = $BYNAME{holocene};
    
    is( (int_bounds('Holocene'))[1], '0', "an age of zero is '0'" );
    
    my $r = int_record('Maastrichtian');
    
    is( $r->{interval_name}, 'Maastrichtian', "int_record" );
    
    $r->{interval_name} = 'changed';
    
    is( int_name('Maastrichtian'), 'Maastrichtian', "int_record returns a copy" );
    is( int_record('Nosuchian'), undef, "an unknown interval has no record" );
    
    # The correlations are read from the classic interval lookup table.
    
    my $c = $dbh->selectrow_hashref("SELECT * FROM $TABLE{CLASSIC_INTERVAL_LOOKUP}
		WHERE interval_no = $m->{interval_no}");
    
    is( int_correlation($m->{interval_no}, 'period_no'), $c->{period_no}, "int_correlation 'period_no'" );
    is( int_correlation($m->{interval_no}, 'ten_my_bin'), $c->{ten_my_bin}, "int_correlation 'ten_my_bin'" );
    ok( ! int_correlation(999999, 'period_no'), "an unknown interval has no correlation" );
    
    # The container of an interval is its period, or for the Precambrian its eon.
    
    is( int_container($m->{interval_no}), $c->{period_no}, "the container of an interval is its period" );
    
    my @eons = grep { $_->{scale_no} == 1 && $_->{type} eq 'eon' && $_->{b_age} > 550 } values %INT;
    my ($old) = grep { $_->{b_age} > 550 && $_->{scale_no} == 1 && $_->{type} ne 'eon' } values %INT;
    my ($eon) = grep { $_->{b_age} >= $old->{b_age} && $_->{t_age} <= $old->{t_age} } @eons;
    
    is( int_container($old->{interval_no}), $eon->{interval_no},
	"the container of a Precambrian interval, $old->{interval_name}, is its eon" );
    is( int_container(999999), undef, "an unknown interval has no container" );
    
    # ints_by_prefix finds the international intervals by the first three letters of their
    # names, without 'Early', 'Middle', or 'Late'.
    
    my $cre = expected(sub { $_[0]{scale_no} == 1 &&
				 lc($_[0]{interval_name}) =~ /^(?:early |middle |late )?cre/ });
    
    is( rec_nos(ints_by_prefix('cre')), $cre, "ints_by_prefix 'cre'" );
    is( count(ints_by_prefix('zzz')), 0, "an unknown prefix" );
};


# ints_by_age, interval_nos_by_age, and ts_intervals_by_age find the intervals that are
# contained in a range of ages (the default), overlap it, overlap it mostly ('major'), or
# contain it. The order of the two ages does not matter.

subtest 'searches by age' => sub {
    
    foreach my $range ( [72.2, 66], [100, 70], [70, 100] )
    {
	my ($b, $t) = $range->[0] > $range->[1] ? @$range : reverse @$range;
	my $label = "$range->[0]-$range->[1]";
	
	my %cond = (
	    contained => sub { $_[0]{b_age} <= $b && $_[0]{t_age} >= $t },
	    overlap => sub { $_[0]{b_age} > $t && $_[0]{t_age} < $b },
	    major => sub { my ($i) = @_; $i->{b_age} > $t && $i->{t_age} < $b &&
			       (($i->{b_age} < $b ? $i->{b_age} : $b) - ($i->{t_age} > $t ? $i->{t_age} : $t))
			       / ($i->{b_age} - $i->{t_age}) >= 0.5 },
	    contains => sub { $_[0]{b_age} > $b && $_[0]{t_age} <= $t ||
				  $_[0]{b_age} >= $b && $_[0]{t_age} < $t } );
	
	foreach my $sel ( qw(contained overlap major contains) )
	{
	    my $expected = expected($cond{$sel});
	    my $intl = expected(sub { $_[0]{scale_no} == 1 && $cond{$sel}->(@_) });
	    
	    is( rec_nos(ints_by_age(@$range, $sel)), $expected, "ints_by_age $label '$sel'" );
	    is( nos(interval_nos_by_age(@$range, $sel)), $expected, "interval_nos_by_age $label '$sel'" );
	    
	    # ts_intervals_by_age searches the intervals of one scale, which are all main
	    # definitions in the international scale.
	    
	    is( rec_nos(ts_intervals_by_age(1, @$range, $sel)), $intl, "ts_intervals_by_age $label '$sel'" );
	}
    }
    
    is( rec_nos(ints_by_age(72.2, 66)), rec_nos(ints_by_age(72.2, 66, 'contained')),
	"the default selector is 'contained'" );
    is( count(ts_intervals_by_age('No such timescale', 72.2, 66)), 0, "an unknown timescale" );
    
    foreach my $f ( [ 'ints_by_age', \&ints_by_age ], [ 'interval_nos_by_age', \&interval_nos_by_age ] )
    {
	eval { $f->[1]->(72.2, 66, 'bogus') };
	
	like( $@, qr{invalid selector 'bogus'}, "$f->[0] rejects an unknown selector" );
    }
    
    # An interval whose top and base are the same age lies entirely within any range that
    # overlaps it, so 'major' includes it.
    
    my ($zero) = grep { $_->{b_age} == $_->{t_age} } values %INT;
  
  SKIP: {
	skip "no interval of zero length", 3 unless $zero;
    
	my ($b, $t) = ($zero->{b_age} + 5, $zero->{t_age} - 5);
	
	my $no = $zero->{interval_no};
	
	ok( (grep { $_->{interval_no} == $no } ints_by_age($b, $t, 'major')),
	    "ints_by_age 'major' includes $zero->{interval_name}, which has zero length" );
	ok( (grep { $_ == $no } interval_nos_by_age($b, $t, 'major')), "interval_nos_by_age 'major'" );
	ok( (grep { $_->{interval_no} == $no } ts_intervals_by_age($zero->{scale_no}, $b, $t, 'major')),
	    "ts_intervals_by_age 'major'" );
    }
};


# Each timescale has a list of boundaries for each interval type, oldest first and ending
# with the top of the scale, and maps from each boundary to the interval that starts
# there. The default list of the international scale has the ages, except the Holocene
# ones, then the Holocene, and the Precambrian periods.

subtest 'boundaries' => sub {
    
    my @periods = sort { $b->{b_age} <=> $a->{b_age} }
	grep { $_->{scale_no} == 1 && $_->{type} eq 'period' } values %INT;
    my $top = (ts_bounds(1))[1];
    
    is( join(' ', map { 0 + $_ } ts_boundary_list(1, 'period')),
	join(' ', (map { 0 + $_->{b_age} } @periods), $top), "the period boundaries of the international scale" );
    
    my $m = $BYNAME{maastrichtian};
    
    is( ts_boundary_no(1, 'age', $m->{b_age}), $m->{interval_no}, "ts_boundary_no" );
    is( ts_boundary_name(1, 'age', $m->{b_age}), 'Maastrichtian', "ts_boundary_name" );
    is( ts_boundary_next(1, 'age', $m->{b_age}), $m->{t_age}, "ts_boundary_next is the top of the interval" );
    is( ts_boundary_no(ts_name(1), 'age', $m->{b_age}), $m->{interval_no}, "ts_boundary_no by scale name" );
    is( ts_boundary_no(1, 'age', 12345), undef, "an age that is not a boundary" );
    
    my @default = ts_boundary_list(1, 'default');
    my $h = $BYNAME{holocene};
    
    ok( (grep { $_ == $h->{b_age} } @default), "the default list has the base of the Holocene" );
    ok( ! (grep { $_ == $BYNAME{meghalayan}{b_age} } @default), "but not the base of the Meghalayan" );
    ok( (grep { $_ == $periods[0]{b_age} } @default), "and has the oldest period" );
    is( ts_boundary_name(1, 'default', $h->{b_age}), 'Holocene', "the boundary name of the Holocene" );
    
    is( ts_boundary_no(1, 'default', $h->{b_age}), $h->{interval_no}, "the default map has the Holocene" );
    is( ts_boundary_next(1, 'default', $h->{b_age}), $h->{t_age}, "and its top" );
    
    # Boundaries of other scales are computed when first needed. A scale without a default
    # list uses the list of its default type.
    
    my @sub = sort { $b->{b_age} <=> $a->{b_age} } grep { $_->{scale_no} == 20 } values %INT;
    my $type = ts_record(20)->{default_type};
    
    ok( ! $IntervalBase::BOUNDARY_LIST{20}, "the boundaries of scale 20 were not computed yet" );
    is( join(' ', map { 0 + $_ } ts_boundary_list(20, $type)),
	join(' ', (map { 0 + $_->{b_age} } grep { $_->{type} eq $type } @sub), (ts_bounds(20))[1]),
	"the '$type' boundaries of scale 20" );
    is( join(' ', ts_boundary_list(20, 'default')), join(' ', ts_boundary_list(20, $type)),
	"its default list is the list of its default type" );
    is( count(ts_boundary_list(20, 'nosuchtype')), 0, "a type the scale does not have" );
};


# classic_scale_map and classic_scale_order give the period or ten-million-year bin of
# each interval, and the order of the periods or bins. Classic uses them.

subtest 'classic scales' => sub {
    
    my $lookup = $dbh->selectall_hashref("SELECT * FROM $TABLE{CLASSIC_INTERVAL_LOOKUP}", 'interval_no');
    
    my $m = $BYNAME{maastrichtian}{interval_no};
    
    my $periods = classic_scale_map('periods', 'numbers');
    
    is( scalar(keys %$periods), scalar(grep { $_->{period_no} } values %$lookup),
	"each interval with a period is in the period map" );
    is( $periods->{$m}, $lookup->{$m}{period_no}, "the period of the Maastrichtian" );
    is( classic_scale_map('periods', 'names')->{$m}, int_name($lookup->{$m}{period_no}),
	"the period name of the Maastrichtian" );
    is( classic_scale_map('69')->{$m}, $lookup->{$m}{period_no}, "scale '69' is the same as 'periods'" );
    is( classic_scale_map('bins')->{$m}, $lookup->{$m}{ten_my_bin}, "the bin of the Maastrichtian" );
    is( classic_scale_map('ten_my_bins')->{$m}, $lookup->{$m}{ten_my_bin}, "'ten_my_bins'" );
    is( scalar(keys classic_scale_map('nosuchscale')->%*), 0, "an unknown scale" );
    
    my $period_seq = $dbh->selectall_arrayref("SELECT sm.interval_no, i.interval_name
		FROM $TABLE{SCALE_MAP} as sm join $TABLE{INTERVAL_DATA} as i using (interval_no)
		WHERE sm.scale_no = 1 and sm.type = 'period' ORDER BY sm.sequence");
    my $bin_seq = $dbh->selectcol_arrayref("SELECT i.interval_name
		FROM $TABLE{SCALE_MAP} as sm join $TABLE{INTERVAL_DATA} as i using (interval_no)
		WHERE sm.scale_no = 10 ORDER BY sm.sequence");
    
    is( join(',', classic_scale_order('periods')), join(',', map { $_->[0] } @$period_seq),
	"the order of the periods" );
    is( join(',', classic_scale_order('periods', 'names')), join(',', map { $_->[1] } @$period_seq),
	"the order of the period names" );
    is( join(',', classic_scale_order('bins')), join(',', @$bin_seq), "the order of the bins" );
    
    is( count(classic_scale_order('nosuchscale')), 0, "an unknown scale has no order" );
};


# The functions that generate the pieces of HTML for a diagram.

subtest 'diagram pieces' => sub {
    
    is( IntervalBase::generate_diagram_header('Scale', 1), '<th class="ts_header">Scale</th>',
	"a header one column wide" );
    is( IntervalBase::generate_diagram_header('Scale', 3), '<th class="ts_header" colspan="3">Scale</th>',
	"a header three columns wide" );
    is( IntervalBase::generate_diagram_corner(), '<th class="ts_corner"></th>', "the corner" );
    is( IntervalBase::generate_diagram_row(), '<tr>', "a row" );
    is( IntervalBase::generate_diagram_row(20), '<tr style="height: 20px; max-height: 20px">',
	"a row with a height" );
    is( IntervalBase::generate_diagram_cell('s1-2', 'Name', 1, 5, '', ''),
	'<td class="ts_interval" id="s1-2">Name</td>', "a cell" );
    is( IntervalBase::generate_diagram_cell('s1-2', 'Name', 3, 5, '#FFFFFF', 1),
	'<td class="ts_interval ts_highlight" style="background-color: #FFFFFF" rowspan="3" id="s1-2">Name</td>',
	"a highlighted cell with a color, three rows high" );
    is( IntervalBase::generate_diagram_empty_cell(1), '<td class="ts_empty"></td>', "an empty cell" );
    is( IntervalBase::generate_diagram_empty_cell(2), '<td class="ts_empty" rowspan="2"></td>',
	"an empty cell two rows high" );
    is( IntervalBase::generate_diagram_bound(4, '66'),
	'<td class="ts_bound" id="b4"><span class="ts_boundlabel">66</span></td>', "a boundary" );
    is( IntervalBase::generate_diagram_bound(4, '66', 1, 1),
	'<td class="ts_bound" id="b4"><span class="ts_lastlabel">66&nbsp;*</span></td>',
	"the last boundary, interpolated" );
    is( IntervalBase::generate_diagram_bottom(3), '<td class="ts_bottom" colspan="3"></td>', "the bottom" );
};


# A diagram of made-up intervals. Scale 1, the international scale, has a period Alpha
# (30-0 Ma) and two epochs, Beta (10-0) and Gamma (30-10). Scale 5 has the ages Delta and
# Epsilon (both 20-5), which share a cell, and Zeta (25-15), which overlaps them and so
# goes in a second column. When another scale is shown, only the international boundaries
# in its range are used, and the international intervals are cut off at that range.

my %SD = ( 1 => { scale_no => 1, scale_name => 'Test International' },
	   5 => { scale_no => 5, scale_name => 'Test Regional' } );

sub int_rec {
    my ($no, $name, $type, $t, $b, %attr) = @_;
    return { interval_no => $no, interval_name => $name, type => $type, t_age => $t, b_age => $b,
	     t_type => 'defined', b_type => 'defined', %attr };
}

my %SEQ = ( 1 => [ int_rec(101, 'Alpha', 'period', 0, 30, color => '#AAAAAA'),
		   int_rec(102, 'Beta', 'epoch', 0, 10),
		   int_rec(103, 'Gamma', 'epoch', 10, 30) ],
	    5 => [ int_rec(201, 'Delta', 'age', 5, 20, t_type => 'interpolated'),
		   int_rec(202, 'Epsilon', 'age', 5, 20),
		   int_rec(203, 'Zeta', 'age', 15, 25, obsolete => 1) ] );

my $D;

subtest 'diagram' => sub {
    
    $D = IntervalBase->generate_ts_diagram({ highlight => { 202 => 1 }, mark_obsolete => 1 },
					   \%SD, \%SEQ, 1, 5);
    
    is( join(' ', map { $_->[0] } $D->{bound2d}->@*), '5 10 15 20 25',
	"the boundaries are those of scale 5, and the international one in its range" );
    is( $D->{max_row}, 4, "max_row" );
    is( $D->{max_col}, 4, "four columns" );
    is( join(' ', map { $_ // '-' } $D->{col_scale}->@[1..4]), '1 1 5 5', "the scale of each column" );
    is_deeply( $D->{errors}, [], "no errors" );
    
    # The column of each interval.
    
    my %col;
    
    foreach my $r ( 0 .. $D->{max_row} - 1 )
    {
	foreach my $c ( 1 .. $D->{max_col} )
	{
	    $col{$D->{bound2d}[$r][$c]} = $c if $D->{bound2d}[$r][$c];
	}
    }
    
    is( join(' ', map { "$_:$col{$_}" } sort keys %col), '1-101:1 1-102:2 1-103:2 5-201:3 5-203:4',
	"each interval is in its column, and Epsilon shares Delta's cell" );
    
    is( join(' ', map { "$_:$D->{cell_height}{$_}" } sort keys $D->{cell_height}->%*),
	'1-101:4 1-102:1 1-103:3 5-201:3 5-203:2', "the height of each cell, in rows" );
    is( "$D->{cell_top}{'1-101'}-$D->{cell_base}{'1-101'}", '5-25', "Alpha is cut off at 5 and 25" );
    is( $D->{cell_label}{'5-201'}, 'Delta/Epsilon', "Delta and Epsilon share a label" );
    is( $D->{cell_combined}{'5-201'}, '+s5-202', "and a cell" );
    ok( ! exists $D->{cell_label}{'5-202'}, "Epsilon has no label of its own" );
    is( $D->{cell_label}{'5-203'}, 'Zeta &dagger;', "an obsolete interval is marked" );
    ok( $D->{cell_hi}{'5-201'}, "highlighting Epsilon highlights the cell it shares" );
    is( $D->{cell_color}{'1-101'}, '#AAAAAA', "an international interval has its own color" );
    ok( ! $D->{cell_color}{'1-102'}, "or none" );
    is( "$D->{cell_color}{'5-201'} $D->{cell_color}{'5-203'}",
	"$IntervalBase::CENO_COLOR[0] $IntervalBase::CENO_COLOR[1]",
	"the columns of another scale get colors in turn" );
    is( join(' ', sort keys $D->{bound_intp}->%*), '5', "the interpolated boundary" );
    
    # Without another scale, every international boundary is used.
    
    my $d = IntervalBase->generate_ts_diagram({ }, \%SD, \%SEQ, 1);
    
    is( join(' ', map { $_->[0] } $d->{bound2d}->@*), '0 10 30', "the international scale by itself" );
    
    # The age limits leave out the intervals outside them.
    
    $d = IntervalBase->generate_ts_diagram({ t_limit => 10, b_limit => 30 }, \%SD, \%SEQ, 1);
    
    is( join(' ', sort keys $d->{cell_height}->%*), '1-101 1-103', "Beta is outside the limits" );
    
    $d = IntervalBase->generate_ts_diagram({ t_limit => 0, b_limit => 30 }, \%SD, \%SEQ, 5);
    
    is( join(' ', sort keys $d->{cell_height}->%*), '5-201 5-203', "another scale by itself, with limits" );
    
    # Intervals that cannot be displayed or placed are reported.
    
    my %bad = ( 5 => [ int_rec(301, 'Badtop', 'age', 'x', 20), int_rec(302, 'Badbase', 'age', 5, 'y') ] );
    
    $d = IntervalBase->generate_ts_diagram({ }, \%SD, \%bad, 5);
    
    is( $d->{errors}[0], "could not display 'Badtop': bad top age 'x'", "a bad top age" );
    
    is( $d->{errors}[1], "could not display 'Badbase': bad base age 'y'", "a bad base age" );
    
    my %crowded = ( 5 => [ map { int_rec(400 + $_, "Crowd$_", 'age', 0, 10 + $_) } 1 .. 6 ] );
    
    $d = IntervalBase->generate_ts_diagram({ }, \%SD, \%crowded, 5);
    
    is( join('; ', $d->{errors}->@*), "Could not place 'Crowd6': not enough columns",
	"an interval that needs a sixth column cannot be placed" );
};


# generate_ts_html turns the diagram into an HTML table: a header row with the name of
# each scale over its columns, a row for each boundary with the cells that start there,
# and a last row with the oldest boundary.

subtest 'diagram HTML' => sub {
    
    my $html = IntervalBase->generate_ts_html($D, \%SD);
    my @rows = split /\n/, $html;
    
    is( $rows[0], '<table class="ts_display" id="ts_diagram" onclick="selectInterval(event)">', "the table" );
    is( $rows[1], '<tr><th class="ts_header" colspan="2">Test International</th>' .
	'<th class="ts_header" colspan="2">Test Regional</th><th class="ts_corner"></th></tr>',
	"the header row" );
    is( $rows[2], '<tr><td class="ts_interval" style="background-color: #AAAAAA" rowspan="4" id="s1-101">Alpha</td>' .
	'<td class="ts_interval" id="s1-102">Beta</td>' .
	qq{<td class="ts_interval ts_highlight" style="background-color: $IntervalBase::CENO_COLOR[0]" rowspan="3" id="s5-201+s5-202">Delta/Epsilon</td>} .
	'<td class="ts_empty" rowspan="2"></td>' .
	'<td class="ts_bound" id="b0"><span class="ts_boundlabel">5&nbsp;*</span></td></tr>',
	"the first row" );
    is( $rows[3], '<tr><td class="ts_interval" rowspan="3" id="s1-103">Gamma</td>' .
	'<td class="ts_bound" id="b1"><span class="ts_boundlabel">10</span></td></tr>', "the second row" );
    like( $rows[4], qr{id="s5-203">Zeta &dagger;</td>}, "the third row starts Zeta" );
    is( $rows[-2], '<tr><td class="ts_bottom" colspan="4"></td>' .
	'<td class="ts_bound" id="b4"><span class="ts_lastlabel">25</span></td></tr>', "the last row" );
    is( $rows[-1], '</table>', "the end of the table" );
    is( scalar(@rows), 8, "a header row, four rows for the boundaries, and the last row" );
    
    # The real international scale, limited to the Cretaceous.
    
    my ($base, $top) = int_bounds('Cretaceous');
    my %sd = ( 1 => ts_record(1) );
    my %seq = ( 1 => [ ts_intervals(1) ] );
    
    my $d = IntervalBase->generate_ts_diagram({ b_limit => $base, t_limit => $top }, \%sd, \%seq, 1);
    
    my @shown = grep { $_->{scale_no} == 1 && $_->{type} !~ /^era|^eon/ &&
			   $_->{b_age} > $top && $_->{t_age} < $base } values %INT;
    my %bounds = map { (0 + $_->{b_age} => 1, 0 + $_->{t_age} => 1) } @shown;
    
    is( join(' ', map { 0 + $_->[0] } $d->{bound2d}->@*), join(' ', sort { $a <=> $b } keys %bounds),
	"the Cretaceous diagram has the boundaries of the Cretaceous intervals" );
    is_deeply( $d->{errors}, [], "with no errors" );
    ok( (grep { $_ eq 'Maastrichtian' } values $d->{cell_label}->%*), "it shows the Maastrichtian" );
    like( IntervalBase->generate_ts_html($d, \%sd), qr{>Maastrichtian</td>}, "and so does its HTML" );
};


# Looking up the type or container of an interval by name gives the same answer as by
# number, and adds nothing to the cache.

subtest 'lookups by name' => sub {
    
    my $m = $BYNAME{maastrichtian};
    
    my $c = $dbh->selectrow_hashref("SELECT * FROM $TABLE{CLASSIC_INTERVAL_LOOKUP}
		WHERE interval_no = $m->{interval_no}");
    
    my $count = scalar(keys %IntervalBase::IDATA);
    
    is( int_type('Maastrichtian'), $m->{type}, "int_type by name" );
    is( scalar(keys %IntervalBase::IDATA), $count, "the lookup does not add a record to the cache" );
    is( int_container('Maastrichtian'), $c->{period_no}, "int_container by name" );
    ok( ! exists $IntervalBase::ICORR{Maastrichtian}, "the lookup does not add a correlation" );
    
    # A null reference_no in scale_map does not add an undefined entry to the reference
    # list of the scale.
    
    my $undef_refs = grep { grep { ! defined } ($_->{reflist} // [])->@* } values %IntervalBase::SDATA;
    
    is( $undef_refs, 0, "no scale has an undefined reference" );
};
