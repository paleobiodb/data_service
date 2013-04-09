package TimeLookup;

use Data::Dumper;
use CGI::Carp;
use strict;

# Ten million year bins, in order from oldest to youngest
@TimeLookup::bins = ("Cenozoic 6", "Cenozoic 5", "Cenozoic 4", "Cenozoic 3", "Cenozoic 2", "Cenozoic 1", "Cretaceous 8", "Cretaceous 7", "Cretaceous 6", "Cretaceous 5", "Cretaceous 4", "Cretaceous 3", "Cretaceous 2", "Cretaceous 1", "Jurassic 6", "Jurassic 5", "Jurassic 4", "Jurassic 3", "Jurassic 2", "Jurassic 1", "Triassic 4", "Triassic 3", "Triassic 2", "Triassic 1", "Permian 4", "Permian 3", "Permian 2", "Permian 1", "Carboniferous 5", "Carboniferous 4", "Carboniferous 3", "Carboniferous 2", "Carboniferous 1", "Devonian 5", "Devonian 4", "Devonian 3", "Devonian 2", "Devonian 1", "Silurian 2", "Silurian 1", "Ordovician 5", "Ordovician 4", "Ordovician 3", "Ordovician 2", "Ordovician 1", "Cambrian 4", "Cambrian 3", "Cambrian 2", "Cambrian 1");

my %isBin;
$isBin{$_}++ foreach @TimeLookup::bins;

%TimeLookup::binning = (
    "33" => "Cenozoic 6", # Pleistocene
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
# used up to 6.11.06
#    "166" => "Carboniferous 3", # Alportian
#    "167" => "Carboniferous 3", # Chokierian
# used up to 9.8.04
#	"166" => "Carboniferous 4", # Alportian
#	"167" => "Carboniferous 4", # Chokierian
# Serpukhovian added 29.6.06
    "53" => "Carboniferous 3", # Serpukhovian
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
# added 8.6.06
    "63" => "Ordovician 5", # Ashgill
# added 29.6.06
    "192" => "Ordovician 5", # Hirnantian
    "639" => "Ordovician 4", # Caradocian
# added 8.6.06
    "64" => "Ordovician 4", # Caradoc
# added 29.6.06
    "787" => "Ordovician 4", # early Late Ordovician
# now spans bins 3 and 4
#    "65" => "Ordovician 3", # Llandeilo
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
    "799" => "Cambrian 1" # Nemakit-Daldynian
);

@TimeLookup::FR2_bins = ("Pleistocene","Pliocene","Upper Miocene","Middle Miocene","Lower Miocene","Chattian","Rupelian","Priabonian","Bartonian","Lutetian","Ypresian","Thanetian","Danian","Maastrichtian","Campanian","Santonian","Coniacian","Turonian","Cenomanian","Albian","Aptian","Barremian","Hauterivian","Valanginian","Berriasian","Portlandian","Kimmeridgian","Oxfordian","Callovian","Bathonian","Bajocian","Aalenian","Toarcian","Pliensbachian","Sinemurian","Hettangian","Rhaetian","Norian","Carnian","Ladinian","Anisian","Scythian","Tatarian","Kazanian","Kungurian","Artinskian","Sakmarian","Asselian","Gzelian","Kasimovian","Moscovian","Bashkirian","Serpukhovian","Visean","Tournaisian","Famennian","Frasnian","Givetian","Eifelian","Emsian","Pragian","Lochkovian","Pridoli","Ludlow","Wenlock","Llandovery","Ashgill","Caradoc","Llanvirn","Arenig","Tremadoc","Merioneth","St Davids","Caerfai","Vendian");

my %isFR2Bin;
$isFR2Bin{$_}++ foreach @TimeLookup::FR2_bins;

%TimeLookup::FR2_binning = (
	"23" => "Vendian",
	"782" => "Caerfai", # equated with the entire Early Cambrian
	"70" => "St Davids",
	"69" => "Merioneth",
	"68" => "Tremadoc",
	"67" => "Arenig",
	"66" => "Llanvirn",
	"65" => "Llanvirn", # former Llandeilo, no longer valid
	"64" => "Caradoc",
	"63" => "Ashgill",
	"62" => "Llandovery",
	"61" => "Wenlock",
	"60" => "Ludlow",
	"59" => "Pridoli",
	"183" => "Lochkovian",
	"182" => "Pragian",
	"181" => "Emsian",
	"180" => "Eifelian",
	"179" => "Givetian",
	"178" => "Frasnian",
	"177" => "Famennian",
	"55" => "Tournaisian",
	"54" => "Visean",
	"53" => "Serpukhovian",
	"52" => "Bashkirian",
	"51" => "Moscovian",
	"50" => "Kasimovian",
	"49" => "Gzelian",
	"151" => "Asselian",
	"150" => "Sakmarian",
	"149" => "Artinskian",

# in the Permian, the FR2 time scale uses Russian time terms with very complex
#  relationships to the standard global time scale that are inferred from the
#  following:

# Sennikov and Golubev 2006:
# Ufimian (post-Kungurian) = latest Cisuralian (LM)
# Kazanian = Biarmian
# Urzhumian = Biarmian
# Severodvinian = Tatarian
# Vjatkian = Tatarian

# Leonova 2007:
# Kungurian = pre-Roadian = latest LM
# Ufimian = synonym or part of Kungurian (or possibly straddles boundary)
# Kazanian = Roadian
# Tatarian = Wordian and remaining Permian

# Taylor et al. 2009:
# Urzhumian = Tatarian = mid-Capitanian (and earlier?)
# Severodovinian = Tatarian = late Capitanian + most of the Wuchiapingian
# Vyatkian = Tatarian = latest Wuchiapingian + Changhsingian

# composite:
# Kungurian (includes Ufimian)
# Kazanian = Roadian
# Urzhumian = Tatarian = Wordian + early Capitanian [inferred]
# Severodovinian = Tatarian = late Capitanian + most of the Wuchiapingian
# Vjatkian = Tatarian = latest Wuchiapingian + Changhsingian

# Fossil Record 2:
# Kungurian
# Ufimian (invalid)
# Kazanian
# (Urzhumian omitted)
# Tatarian

	"148" => "Kungurian", # Kungurian proper
	"147" => "Kungurian", # Ufimian
	"905" => "Kazanian", # Kazanian proper
	"717" => "Kazanian", # Roadian
	"904" => "Tatarian", # Tatarian proper
	"146" => "Tatarian", # Wordian, assuming Urzhumian falls in FR2's "Tatarian"
	"145" => "Tatarian", # Capitanian, with same assumption
	"771" => "Tatarian", # Lopingian

	# Scythian equals Early Triassic
	"46" => "Scythian",
	"139" => "Anisian",
	"138" => "Ladinian",
	"137" => "Carnian",
	"136" => "Norian",
	"135" => "Rhaetian",
	"134" => "Hettangian",
	"133" => "Sinemurian",
	"132" => "Pliensbachian",
	"131" => "Toarcian",
	"130" => "Aalenian",
	"129" => "Bajocian",
	"128" => "Bathonian",
	"127" => "Callovian",
	"126" => "Oxfordian",
	"125" => "Kimmeridgian",
	# equals Tithonian
	"124" => "Portlandian",
	"123" => "Berriasian",
	"122" => "Valanginian",
	"121" => "Hauterivian",
	"120" => "Barremian",
	"119" => "Aptian",
	"118" => "Albian",
	"117" => "Cenomanian",
	"116" => "Turonian",
	"115" => "Coniacian",
	"114" => "Santonian",
	"113" => "Campanian",
	"112" => "Maastrichtian",
	"111" => "Danian",
	# Selandian included in Benton's Thanetian based on Harland et al. 1989
	"743" => "Thanetian",
	"110" => "Thanetian",
	"109" => "Ypresian",
	"108" => "Lutetian",
	"107" => "Bartonian",
	"106" => "Priabonian",
	"105" => "Rupelian",
	"104" => "Chattian",
	"85" => "Lower Miocene",
	"84" => "Middle Miocene",
	"83" => "Upper Miocene",
	"34" => "Pliocene",
	"33" => "Pleistocene"
);

%TimeLookup::rank_order = (
    'eon/eonothem' => 1,
    'era/erathem' => 2,
    'period/system' => 3,
    'subperiod/system' =>4,
    'epoch/series' =>5,
    'subepoch/series' =>6,
    'age/stage' =>7,
    'subage/stage' =>8,
    'chron/zone' =>9
);

sub getBins {
    return @TimeLookup::bins;
}

sub getFR2Bins {
    return @TimeLookup::FR2_bins;
}

sub getBinning {
    return \%TimeLookup::binning;
}

sub new {
    my $c = shift;
    my $dbt = shift;

    my $self  = {'ig'=>undef,'dbt'=>$dbt,'set_boundaries'=>0, 'sl'=>{},'il'=>{}};
    bless $self,$c;
}

# first draft started 5.4.11
# new version mostly written 3-12.8.11
# cut out getBoundariesReal, getFromChildren 13.12.11 and a ton of other junk
#  9.3.12
# intended to replace the major functions getIntervalGraph, generateLookupTable, _initInterval, and
#  related functions called by getIntervalGraph:
# _removeInvalidCorrelations
# _findSharedBoundaries
# _findEquivalentTerms
# _combineBoundaries
# and the extremely complicated getBoundariesReal
# also allowed removing isObsolete, findPath, makePrecedesHash, markPrecedesLB, markFollowsUB, isCovered, best_by_continent, matchAny,
#  serializeItv, deserializeItv, PriorityQueue functions new, insert, pop, and remove, etc., etc.
# getParentIntervals, getPrecedingIntervals, and getFollowingIntervals were
#  apparently already obsolete, so I threw them out
# _dumpGraph, _dumpInterval, and _printConstraint were PS test functions, ditto
# and maybe more...
# each interval is assigned the following properties formerly generated by
#  initInterval:
#  interval_no, base_age, top_age
# values computed by initInterval, only used here and in Scales, and
#  hopefully obsolete:
#  next, prev, all_scales, boundary_scale, all_next, all_prev
# initInterval also computed the following, which are not useful:
#  visited?, children?, defunct?, next_scale
# defunct is used in isObsolete
# next_scale was only used in printTree

sub buildLookupTable	{

	my $dbt = shift;
	my $dbh = $dbt->dbh;

	# plane attributes
	my ($planes,@intervalsAbove,@intervalsBelow);
	# interval attributes
	my (%name,@interval_nos);
	my (%bestPrev,%bestNext,%bestMax,%bestMin,%notScaleTop,%notScaleBottom);
	my (%isAbovePlane,%isBelowPlane,%topPlane,%basePlane,%baseAge,%topAge,%baseSource,%topSource,%baseAgeRelation,%topAgeRelation);

	# get upper plane info for all intervals ending in the Recent
	# dropped Neogene from this list 31.7.12
	$planes = 1;
	for my $no ( 32,12,1,751,925,943 )	{
		if ( ! $topPlane{$no} )	{
			$topPlane{$no} = 1;
			$isBelowPlane{1}{$no}++;
			push @{$intervalsBelow[1]} , $no;
		}
	}

	# get interval names and initialize plane info
	my $sql = "SELECT interval_no,eml_interval,interval_name FROM intervals";
	my @intervals = @{$dbt->getData($sql)};
	$name{$_->{'interval_no'}} = $_->{'interval_name'} foreach @intervals;
	push @interval_nos , $_->{'interval_no'} foreach @intervals;
	for my $i ( @intervals )	{
		my $no = $i->{'interval_no'};
		if ( $i->{'eml_interval'} )	{
			$name{$no} = $i->{'eml_interval'}." ".$name{$i->{'interval_no'}};
		}
		$planes++;
		$basePlane{$no} = $planes;
		($baseAge{$no},$topAge{$no},$baseSource{$no},$topSource{$no},$baseAgeRelation{$no},$topAgeRelation{$no}) = ("NULL","NULL","NULL","NULL","NULL","NULL");
		$isAbovePlane{$planes}{$no}++;
		push @{$intervalsAbove[$planes]} , $no;
	}

	# special handling for intervals at the dawn of time
	merge($basePlane{11},$basePlane{$_}) foreach ( 80,753,760 );

	# get the entire correlations table in order of reliability
	# order of preference is recently published scales, high-level scales,
	#  global scales, most recently entered scales
	my $sql = "SELECT r.reference_no,r.pubyr,s.scale_no,s.continent,i.eml_interval,i.interval_name,c.correlation_no,c.interval_no,c.next_interval_no,c.max_interval_no,c.min_interval_no,c.base_age,if(s.continent='global',1,0) isglobal,CASE scale_rank WHEN 'eon/eonothem' THEN 1 WHEN 'era/erathem' THEN 2 WHEN 'period/system' THEN 3 WHEN 'subperiod/system' THEN 4 WHEN 'epoch/series' THEN 5 WHEN 'subepoch/series' THEN 6 WHEN 'age/stage' THEN 7 WHEN 'subage/stage' THEN 8 WHEN 'chron/zone' THEN 9 END AS rank FROM refs r,scales s,correlations c,intervals i WHERE r.reference_no=s.reference_no AND s.scale_no=c.scale_no AND i.interval_no=c.interval_no ORDER BY r.pubyr DESC,rank ASC,isglobal DESC,s.scale_no DESC,c.correlation_no ASC";
	my @correlations = @{$dbt->getData($sql)};

	# the correlations are still slightly out of order because some
	#  intervals appears both as regional and global terms
	my $sql = "SELECT s.continent,c.interval_no FROM refs r,scales s,correlations c WHERE r.reference_no=s.reference_no AND s.scale_no=c.scale_no ORDER BY r.pubyr DESC,s.scale_no DESC,c.correlation_no ASC";
	my @bydate = @{$dbt->getData($sql)};
	my %continent;
	for my $i ( 0..$#bydate)	{
		my $no = $bydate[$i]->{'interval_no'};
		$continent{$no} = ( $continent{$no} eq "" ) ? $bydate[$i]->{'continent'} : $continent{$no};
	}

	# get basic data that are not dependent on relationships between
	#  children and/or parents
	for my $i ( 1..$#correlations )	{
		my ($c0,$c1,$c2) = ($correlations[$i-1],$correlations[$i],$correlations[$i+1]);
		my ($no1,$no2) = ($c1->{'interval_no'},$c2->{'interval_no'});
		( ! $bestMax{$no1} ) ? $bestMax{$no1} = $c1->{'max_interval_no'} : $bestMax{$no1};
		( ! $bestMin{$no1} && $c1->{'min_interval_no'} > 0 ) ? $bestMin{$no1} = $c1->{'min_interval_no'} : $bestMin{$no1};
		( $bestMin{$no1} == 0 ) ? $bestMin{$no1} = $c1->{'max_interval_no'} : $bestMin{$no1};
		( $c0->{'scale_no'} == $c1->{'scale_no'} ) ? $notScaleTop{$no1}++ : "";
		( $c1->{'scale_no'} == $c2->{'scale_no'} ) ? $notScaleBottom{$no1}++ : "";
	}


	# BINDING

	for my $i ( 1..$#correlations )	{

		my ($c0,$c1,$c2) = ($correlations[$i-1],$correlations[$i],$correlations[$i+1]);
		my ($no0,$no1,$no2) = ($c0->{'interval_no'},$c1->{'interval_no'},$c2->{'interval_no'});

		# merge intervals that only ever appear at the base of a scale
		#  with their max parents
		if ( $notScaleBottom{$no1} == 0 && $bestMax{$no1} == $c1->{'max_interval_no'} && $basePlane{$bestMax{$no1}} > 0 )	{
			# prevents some Llandeilo problems
			if ( $basePlane{$no0} != $basePlane{$c1->{'max_interval_no'}} )	{
				merge($basePlane{$bestMax{$no1}},$basePlane{$no1});
			}
		}

		if ( $c0->{'scale_no'} != $c1->{'scale_no'} )	{
			next;
		}
		my $max0 = $c0->{'max_interval_no'};
		my $min1;
		$min1 = ( $c1->{'min_interval_no'} == 0) ? $c1->{'max_interval_no'} : $c1->{'min_interval_no'};

	# CHILD TO CHILD BINDING

	# skip if there is a conflict involving the children
	# case 1: children share top planes (e.g., Ufimian)
	# inheriting the previous interval from no1 is better than nothing
		if ( $topPlane{$no0} == $topPlane{$no1} && $topPlane{$no0} > 0 )	{
			( ! $bestPrev{$no0} ) ? $bestPrev{$no0} = $bestPrev{$no1} : "";
			next;
		}
	# case 2: children share base planes
		if ( $basePlane{$no0} == $basePlane{$no1} && $basePlane{$no0} > 0 )	{
			next;
		}
	# case 3: children are already separated by an interval
		if ( $bestPrev{$no0} == $bestNext{$no1} && $bestPrev{$no0} )	{
			next;
		}
	# more complicated tests for the same thing
		if ( ( $basePlane{$no0} == $topPlane{$bestNext{$no1}} || $basePlane{$bestPrev{$no0}} == $topPlane{$no1} ) && $bestPrev{$no0} && $bestNext{$no1} )	{
			next;
		}
	# case 4: children are separated by two intervals
		if ( $basePlane{$bestPrev{$no0}} == $topPlane{$bestNext{$no1}} && $bestPrev{$no0} && $bestNext{$no1} )	{
			next;
		}
	# case 5: parents are already separated by an interval
		if ( $bestPrev{$max0} == $bestNext{$min1} && $bestPrev{$max0} )	{
			next;
		}
	# case 6: n1 is an old dropped interval (e.g., Dragonian, Llandeilo)
		if ( $basePlane{$no0} == $topPlane{$no2} )	{
			$bestNext{$no1} = $bestNext{$no0};
			$bestPrev{$no1} = $bestPrev{$no2};
			$isBelowPlane{$topPlane{$no0}}{$no1}++;
			$topPlane{$no1} = $topPlane{$no0};
			merge($basePlane{$no2},$basePlane{$no1});
			next;
		}
	# case 7: younger child's base is above parent plane based on better
	#  correlations (e.g., base Caerfai vs. Cambrian-Vendian)
	# younger child base and parent plane are okay, so bind the older child
	#  top to the parent plane if the children aren't already bound
	# need an exception for late Llandeilo and late Llanvirn
		if ( $isAbovePlane{$basePlane{$bestPrev{$no0}}}{$max0} && $max0 != $min1 && ( $name{$no1} !~ /^(late|early)/i || $name{$no1} !~ /$name{$min1}/ ) )	{
			if ( $basePlane{$no0} != $topPlane{$no1} )	{
				$topPlane{$no1} = $basePlane{$max0};
				$isBelowPlane{$topPlane{$no1}}{$no1}++;
			} else	{
			}
			next;
		}

	# special handling for early and late subdivisions
	# basically, binding to the parent takes precedence over everything
	# don't worry about the child plane because there may be conflicts
	#  such as "early Llandeilo" over late Llanvirn
		my $earlyLate;
		if ( $name{$no0} =~ /^early/i && $name{$no0} =~ /$name{$max0}$/i && $name{$no0} !~ / .* /i )	{
			$bestPrev{$no0} = $bestPrev{$max0};
			merge($basePlane{$max0},$basePlane{$no0});
			$earlyLate++;
		}
		if ( $name{$no1} =~ /^late/i && $name{$no1} =~ /$name{$min1}$/i && $name{$no1} !~ / .* /i )	{
			$bestNext{$no1} = $bestNext{$min1};
			if ( $topPlane{$no1} > 0 )	{
				merge($topPlane{$min1},$topPlane{$no1});
			} else	{
				$isBelowPlane{$topPlane{$min1}}{$no1}++;
				$topPlane{$no1} = $topPlane{$min1};
			}
			$earlyLate++;
		}
	# skip if there is a parent-parent conflict (Llandeilo case)
		if ( $earlyLate && $max0 != $min1 && ( ( $topPlane{$max0} == $topPlane{$min1} && $topPlane{$max0} ) || ( $basePlane{$max0} == $basePlane{$min1} && $basePlane{$max0} ) ) )	{
			next;
		}

	# bind the children if they aren't already bound
	# this involves deleting the top plane of interval n1
	# make sure to save key info based on this correlation
		if ( $basePlane{$no0} != $topPlane{$no1} )	{
			( ! $bestPrev{$no0} ) ? $bestPrev{$no0} = $no1 : "";
			( ! $bestNext{$no1} ) ? $bestNext{$no1} = $no0 : "";
			if ( $topPlane{$no1} > 0 )	{
				merge($basePlane{$no0},$topPlane{$no1});
			} else	{
				$isBelowPlane{$basePlane{$no0}}{$no1}++;
				$topPlane{$no1} = $basePlane{$no0};
			}
		}

	# PARENT TO PARENT BINDING

	# skip if the parents are the same
		if ( $max0 == $min1 )	{
			next;
		}
		my $eml;
	# skip early/middle/late parents because these relations could be bogus
	#  and EML intervals should be dealt with using child-to-child relations
	# child-parent binding could still be valid, so only skip binding
	#  section below 31.7.12
		if ( ( $name{$max0} =~ /^(early|middle|late)/i || $name{$min1} =~ /^(early|middle|late)/i ) && $basePlane{$max0} != $topPlane{$min1} )	{
			$eml++;
		}
	# skip if there is a simple conflict involving the parents
	# assume child-younger parent binding is okay because the problem is
	#  with a previously encountered redefinition of the older parent
	#  (which made it younger)
		if ( $topPlane{$max0} == $topPlane{$min1} && $topPlane{$max0} > 0 )	{
			merge($basePlane{$no0},$basePlane{$max0});
			next;
		}
		if ( $basePlane{$max0} == $basePlane{$min1} && $basePlane{$max0} > 0 )	{
			next;
		}
	# skip if the parents are already separated by something
		if ( $bestPrev{$max0} == $bestNext{$min1} && $bestPrev{$max0} )	{
			next;
		}
	# more complicated tests for the same thing
		if ( $basePlane{$max0} == $topPlane{$bestNext{$min1}} || ( $basePlane{$bestPrev{$max0}} == $topPlane{$min1} && $topPlane{$min1} > 0 ) )	{
			next;
		}
	# skip if the children's plane is already bound to the top of the
	#  younger parent or to the base of the older parent
		if ( $basePlane{$no0} == $topPlane{$max0} || $basePlane{$no0} == $basePlane{$min1} )	{
			next;
		}
		if ( $topPlane{$no0} == $basePlane{$max0} )	{
			next;
		}

	# bind the parents to each other if this hasn't happened already
		if ( $basePlane{$max0} != $topPlane{$min1} && ! $eml )	{
			( ! $bestPrev{$max0} ) ? $bestPrev{$max0} = $min1 : "";
			( ! $bestNext{$min1} ) ? $bestNext{$min1} = $max0 : "";
			if ( $topPlane{$min1} > 0 )	{
				merge($basePlane{$max0},$topPlane{$min1});
			} else	{
				$isBelowPlane{$basePlane{$max0}}{$min1}++;
				$topPlane{$min1} = $basePlane{$max0};
			}
		}

	# CHILD TO PARENT BINDING

	# rock-bottom basic checks (one case of the first, none of the second)
		if ( $topPlane{$no0} == $basePlane{$min1} )	{
			next;
		}
		if ( $basePlane{$no1} == $topPlane{$max0} )	{
			next;
		}
	# skip if there is a simple conflict between the current parent's planes
	#   and planes involving the best parents
		if ( $basePlane{$max0} == $topPlane{$bestMax{$no0}} )	{
			next;
		}
		if ( $topPlane{$min1} == $basePlane{$bestMin{$no1}} )	{
			next;
		}
	# this never happens (and shouldn't)
		if ( $topPlane{$bestMin{$no0}} == $basePlane{$max0} )	{
			next;
		}
	# Clarendonian case (attempted bind to Mio-Pliocene plane)
		if ( $topPlane{$bestMin{$bestMin{$no0}}} == $basePlane{$max0} )	{
			next;
		}
	# more paranoia checks
		if ( $basePlane{$bestMax{$no1}} == $topPlane{$min1} )	{
			next;
		}
		if ( $basePlane{$bestMax{$bestMax{$no1}}} == $topPlane{$min1} )	{
			next;
		}
	# convoluted Atokan/Morrown case: child plane is already bound with
	#  base of interval below older parent
		if ( $basePlane{$no0} == $basePlane{$bestPrev{$bestMin{$min1}}} )	{
			next;
		}
	# another rare problem seen in the Carboniferous: no1 is already
	#  bound to an interval whose max is no1's min
		if ( $min1 == $bestMax{$bestNext{$no1}} )	{
			next;
		}
	# the Carboniferous again... the plane between no0 and no1 already
	#  falls somewhere within one of these parents
		if ( $bestMax{$no0} == $bestMin{$no1} && ( $bestMax{$bestMax{$no0}} == $min1 || $bestMax{$bestMax{$no0}} == $max0 ) )	{
			next;
		}

	# check bases of all intervals below the children's plane and skip if
	#  any of them share a base with the parent's plane
	# Melekesian/Vereian - Bashkirian/Moscovian - Westphalian B/C case:
	#  the two parents (B/M) are the max and min parents of another interval
	#  (Westphalian B) already bound below the child plane
		my $bad;
		for my $i ( keys %{$isBelowPlane{$basePlane{$no0}}} )	{
			if ( $bestMax{$i} == $min1 && $bestMin{$i} == $max0 )	{
				$bad++;
			}
		}
	# Roadian-Wordian vs. Zechstein-Rotliegendes case: something is bound
	#  below to the child plane and above to the younger max interval's
	#  max interval
	#   max is younger than younger child's min
		for my $i ( keys %{$isAbovePlane{$basePlane{$no0}}} )	{
			for my $j ( keys %{$isBelowPlane{$basePlane{$bestMax{$max0}}}} )	{
				if ( $i == $j )	{
					$bad++;
				}
			}
		}
		if ( $bad > 0 )	{
			next;
		}

	# finally, bind the children to the parent's plane
		if ( $basePlane{$no0} != $basePlane{$max0} )	{
			merge($basePlane{$max0},$basePlane{$no0});
		}
	}

	# BINDING CLEANUPS

	# bind tops of intervals that only ever are at the top of a scale
	for my $no ( @interval_nos )	{
		if ( $notScaleTop{$no} == 0 && $topPlane{$no} != 1 )	{
			if ( $topPlane{$no} > 0 && $topPlane{$bestMin{$no}} > 0  )	{
				merge($topPlane{$bestMin{$no}},$topPlane{$no});
			} elsif ( $topPlane{$bestMin{$no}} > 0 )	{
				$bestNext{$no} = $bestNext{$bestMin{$no}};
				$isBelowPlane{$topPlane{$bestMin{$no}}}{$no}++;
				$topPlane{$no} = $topPlane{$bestMin{$no}};
			}
		# fallback if tops of child and parent are both unbound
		#  at this point
		# neither one will have a bestNext value
			else	{
				$planes++;
				$isBelowPlane{$planes}{$no}++;
				$isBelowPlane{$planes}{$bestMin{$no}}++;
				$topPlane{$no} = $planes;
				$topPlane{$bestMin{$no}} = $planes;
			}
		}
	}

	# bind bases of intervals that are completely unbound even though
	#  bestPrev values are available (e.g., Ufimian)
	for my $no ( @interval_nos )	{
		my @belows = keys %{$isBelowPlane{$basePlane{$no}}};
		if ( ! @belows && $bestPrev{$no} > 0 )	{
			merge($topPlane{$bestPrev{$no}},$basePlane{$no});
		}
	}

	sub merge	{
		my ($x,$y) = @_;
		if ( $x == $y || $x == 0 )	{
			return;
		}
		$isBelowPlane{$x}{$_}++ foreach keys %{$isBelowPlane{$y}};
		$topPlane{$_} = $x foreach keys %{$isBelowPlane{$y}};
		delete $isBelowPlane{$y};
		$isAbovePlane{$x}{$_}++ foreach keys %{$isAbovePlane{$y}};
		$basePlane{$_} = $x foreach keys %{$isAbovePlane{$y}};
		delete $isAbovePlane{$y};
	}

	# FIND AGE ESTIMATES FOR PLANES

	# note: the most reliable estimates are usually most recently published
	my (%bestDate,%bestDateRank,%bestSource);
	for my $i ( 0..$#correlations )	{
		my $c = $correlations[$i];
		if ( ! $bestDate{$basePlane{$c->{'interval_no'}}} && $c->{'base_age'} > 0 )	{
			$bestDate{$basePlane{$c->{'interval_no'}}} = $c->{'base_age'};
			$bestSource{$basePlane{$c->{'interval_no'}}} = $c->{'correlation_no'};
			$bestDateRank{$basePlane{$c->{'interval_no'}}} = $i;
		}
	}


	# REMOVE CONFLICTING DATES

	# takes advantage of the fact that the correlations within any scale
	#  are always in age order (youngest to oldest)
	# WARNING: assumes that there won't be a contradictory cascade of
	#  date knockouts, e.g., B knocks out C and A knocks out B but A and C
	#  are actually consistent
	# WARNING: this isn't 100% guaranteed to catch all conflicts but
	#  certainly gets almost all of them
	my ($lastScale,$lastAge,$lastBasePlane,$lastTopPlane) = (0,0,'','');
	for my $i ( 0..$#correlations )	{
		my $c = $correlations[$i];
		if ( $c->{'scale_no'} ne $lastScale )	{
			$lastAge = 0;
			$lastBasePlane = "";
			$lastTopPlane = "";
			$lastScale = $c->{'scale_no'};
		}
		my $no = $c->{'interval_no'};
		if ( $bestDate{$basePlane{$no}} eq "" )	{
			next;
		}

		# simple base-top check
		if ( $bestDate{$basePlane{$no}} < $bestDate{$topPlane{$no}} )	{
			if ($bestDateRank{$basePlane{$no}} > $bestDateRank{$topPlane{$no}} )	{
				delete $bestDate{$basePlane{$no}};
				next;
			} elsif ($bestDateRank{$basePlane{$no}} < $bestDateRank{$topPlane{$no}} )	{
				delete $bestDate{$topPlane{$no}};
			}
		}

		# child-child checks
		if ( $bestDate{$basePlane{$no}} <= $lastAge && $basePlane{$no} != $lastBasePlane && $topPlane{$no} == $lastBasePlane )	{
			if ($bestDateRank{$basePlane{$no}} > $bestDateRank{$lastBasePlane} )	{
				delete $bestDate{$basePlane{$no}};
				next;
			} elsif ($bestDateRank{$basePlane{$no}} < $bestDateRank{$lastBasePlane} )	{
				delete $bestDate{$lastBasePlane};
			}
		}

		# child-parent checks
		# base of child's parent is younger than the child's base
		if ( $bestDate{$basePlane{$bestMax{$no}}} > 0 && $bestDate{$basePlane{$no}} > 0 && $bestDate{$basePlane{$bestMax{$no}}} < $bestDate{$basePlane{$no}} )	{
			if ( $bestDateRank{$basePlane{$bestMax{$no}}} < $bestDateRank{$basePlane{$no}} )	{
				delete $bestDate{$basePlane{$no}};
				next;
			} elsif ( $bestDateRank{$basePlane{$bestMax{$no}}} > $bestDateRank{$basePlane{$no}} )	{
				delete $bestDate{$basePlane{$bestMax{$no}}};
			}
		}
		# top of child's parent is older than the child's base 
		if ( $bestDate{$topPlane{$bestMax{$no}}} > 0 && $bestDate{$basePlane{$no}} > 0 && $bestDate{$topPlane{$bestMax{$no}}} > $bestDate{$basePlane{$no}} )	{
			if ( $bestDateRank{$topPlane{$bestMax{$no}}} < $bestDateRank{$basePlane{$no}} )	{
				delete $bestDate{$basePlane{$no}};
				next;
			} elsif ( $bestDateRank{$topPlane{$bestMax{$no}}} > $bestDateRank{$basePlane{$no}} )	{
				delete $bestDate{$topPlane{$bestMax{$no}}};
			}
		}
		if ( $bestDate{$basePlane{$no}} )	{
			$lastAge = $bestDate{$basePlane{$no}};
			$lastBasePlane = $basePlane{$no};
			$lastTopPlane = $topPlane{$no};
		}
	}

	# compute bounds on age values (before or after) for planes that
	#  haven't been dated directly
	my (%maxBaseDate,%minBaseDate,%maxSource,%minSource);
	for my $p ( keys %isBelowPlane )	{
		($maxBaseDate{$p},$minBaseDate{$p}) = ("NULL","NULL");
		#($bestSource{$p},$maxBaseDate{$p},$minBaseDate{$p}) = ("NULL","NULL","NULL");
		if ( $bestDate{$p} == 0 )	{
		# first try to extract a bound based on the youngest base age
		#  of any max parent of any interval just above the plane
			for my $i ( keys %{$isAbovePlane{$p}} )	{
				if ( ( $maxBaseDate{$p} eq "NULL" || $bestDate{$basePlane{$bestMax{$i}}} < $maxBaseDate{$p} ) && $bestDate{$basePlane{$bestMax{$i}}} > 0 )	{
					$maxBaseDate{$p} = $bestDate{$basePlane{$bestMax{$i}}};
					$maxSource{$p} = $bestSource{$basePlane{$bestMax{$i}}};
				}
			}
		# next try to extract a bound based on the youngest base age
		#  of any interval directly below this one's base
			for my $i ( keys %{$isBelowPlane{$p}} )	{
				if ( ( $maxBaseDate{$p} eq "NULL" || $bestDate{$basePlane{$i}} < $maxBaseDate{$p} ) && $bestDate{$basePlane{$i}} > 0 )	{
					$maxBaseDate{$p} = $bestDate{$basePlane{$i}};
					$maxSource{$p} = $bestSource{$basePlane{$i}};
				}
			}
		# next try climbing down through previous intervals
		# try both the intervals directly preceding the plane's
		#  just-below intervals and those preceding their parents
		# not 100% guaranteed to get the youngest bound, but should
		#  work almost all the time
			if ( $maxBaseDate{$p} eq "NULL" )	{
				for my $i ( keys %{$isBelowPlane{$p}} )	{
					my $prev = $bestPrev{$i};
					while ( $prev > 0 )	{
						if ( ( $maxBaseDate{$p} eq "NULL" || $bestDate{$basePlane{$prev}} < $maxBaseDate{$p} ) && $bestDate{$basePlane{$prev}} > 0 )	{
							$maxBaseDate{$p} = $bestDate{$basePlane{$prev}};
							$maxSource{$p} = $bestSource{$basePlane{$prev}};
						}
						$prev = $bestPrev{$prev};
					}
					$prev = $bestMax{$i};
					while ( $prev > 0 )	{
						if ( ( $maxBaseDate{$p} eq "NULL" || $bestDate{$basePlane{$prev}} < $maxBaseDate{$p} ) && $bestDate{$basePlane{$prev}} > 0 )	{
							$maxBaseDate{$p} = $bestDate{$basePlane{$prev}};
							$maxSource{$p} = $bestSource{$basePlane{$prev}};
						}
						$prev = $bestPrev{$prev};
					}
				}
			}
		# likewise the oldest base age of intervals above
			for my $i ( keys %{$isBelowPlane{$p}} )	{
				if ( ( $minBaseDate{$p} eq "NULL" || $bestDate{$topPlane{$bestMin{$i}}} < $minBaseDate{$p} ) && $bestDate{$topPlane{$bestMin{$i}}} > 0 )	{
					$minBaseDate{$p} = $bestDate{$topPlane{$bestMin{$i}}};
					$minSource{$p} = $bestSource{$topPlane{$bestMin{$i}}};
				}
			}
			for my $i ( keys %{$isAbovePlane{$p}} )	{
				if ( ( $minBaseDate{$p} eq "NULL" || $bestDate{$topPlane{$i}} > $minBaseDate{$p} ) && $bestDate{$topPlane{$i}} > 0 )	{
					$minBaseDate{$p} = $bestDate{$topPlane{$i}};
					$minSource{$p} = $bestSource{$topPlane{$i}};
				}
			}
			if ( $minBaseDate{$p} eq "NULL" )	{
				for my $i ( keys %{$isAbovePlane{$p}} )	{
					my $next = $bestNext{$i};
					while ( $next > 0 )	{
						if ( ( $minBaseDate{$p} eq "NULL" || $bestDate{$topPlane{$next}} > $minBaseDate{$p} ) && $bestDate{$topPlane{$next}} > 0 )	{
							$minBaseDate{$p} = $bestDate{$topPlane{$next}};
							$minSource{$p} = $bestSource{$topPlane{$next}};
						}
						$next = $bestNext{$next};
					}
					$next = $bestMin{$i};
					while ( $next > 0 )	{
						if ( ( $minBaseDate{$p} eq "NULL" || $bestDate{$topPlane{$next}} > $minBaseDate{$p} ) && $bestDate{$topPlane{$next}} > 0 )	{
							$minBaseDate{$p} = $bestDate{$topPlane{$next}};
							$minSource{$p} = $bestSource{$topPlane{$next}};
						}
						$next = $bestNext{$next};
					}
				}
			}
		}
	}

	($bestDate{'0'},$bestDate{'NULL'}) = (0,"NULL");
	($bestSource{'0'},$bestSource{'NULL'}) = ("NULL","NULL");

	# compute age, source, and relation values for intervals
	for my $no ( @interval_nos )	{
		if ( $bestDate{$basePlane{$no}} > 0 )	{
			$baseAge{$no} = $bestDate{$basePlane{$no}};
			$baseSource{$no} = $bestSource{$basePlane{$no}};
			$baseAgeRelation{$no} = "'equal to'";
		}
		if ( $topPlane{$no} == 1 )	{
			$topAge{$no} = 0;
			$topAgeRelation{$no} = "'equal to'";
		} elsif ( $bestDate{$topPlane{$no}} > 0 )	{
			$topAge{$no} = $bestDate{$topPlane{$no}};
			$topSource{$no} = $bestSource{$topPlane{$no}};
			$topAgeRelation{$no} = "'equal to'";
		}
		if ( $baseAge{$no} eq "NULL" && $maxBaseDate{$basePlane{$no}} > 0 )	{
			$baseAge{$no} = $maxBaseDate{$basePlane{$no}};
			$baseSource{$no} = $maxSource{$basePlane{$no}};
			$baseAgeRelation{$no} = "'after'";
		}
		if ( $topAge{$no} eq "NULL" && $minBaseDate{$topPlane{$no}} > 0 )	{
			$topAge{$no} = $minBaseDate{$topPlane{$no}};
			$topSource{$no} = $minSource{$topPlane{$no}};
			$topAgeRelation{$no} = "'before'";
		}
	}

	# BUILD LOOKUP TABLE

	# insert boundaries first because they will be needed later to map
	#   intervals into scales
	# adapted from a small part of the old generateLookupTable function
	for my $no ( @interval_nos )	{
		$basePlane{$no} = ( ! $basePlane{$no} ) ? 0 : $basePlane{$no};
		$topPlane{$no} = ( ! $topPlane{$no} ) ? 0 : $topPlane{$no};
		my $sql = "SELECT interval_no FROM interval_lookup WHERE interval_no=$no";
		if ( ${$dbt->getData($sql)}[0]->{'interval_no'} )	{
			my $sql = "UPDATE interval_lookup SET base_plane=$basePlane{$no},top_plane=$topPlane{$no},base_age_relation=$baseAgeRelation{$no},base_age=$baseAge{$no},base_age_source=$baseSource{$no},top_age_relation=$topAgeRelation{$no},top_age=$topAge{$no},top_age_source=$topSource{$no} WHERE interval_no=$no";
			$dbh->do($sql);
		} else	{
			my $sql = "INSERT INTO interval_lookup(interval_no,base_plane,top_plane,base_age_relation,base_age,base_age_source,top_age_relation,top_age,top_age_source) VALUES ($no,$basePlane{$no},$topPlane{$no},$baseAgeRelation{$no},$baseAge{$no},$baseSource{$no},$topAgeRelation{$no},$topAge{$no},$topSource{$no})";
			$dbh->do($sql);
		}
	}

	# map intervals into scales
	# slow but simple and reliable method replacing interval-in-scale
	#  assignment routine of generateLookupTable
	my %lookup;
	my @with_dates;
	for my $no ( @interval_nos )	{
		if ( $baseAge{$no} ne "NULL" && $topAge{$no} ne "NULL" )	{
			push @with_dates , $no;
		}
		$lookup{$_}{$no} = "NULL" foreach ( 69,71,72,73,'bins','fossil record 2' );
	}
	for my $scale ( 69,71,72,73,'bins','fossil record 2' )	{
		my (@scale_intervals,%binning);
		if ($scale =~ /bin/ && $scale !~ /fossil/i)	{
			%binning = %TimeLookup::binning;
			@scale_intervals = @TimeLookup::bins;
		} elsif ($scale =~ /fossil/i)	{
			%binning = %TimeLookup::FR2_binning;
			@scale_intervals = @TimeLookup::FR2_bins;
		} else	{
			my $sql = "SELECT interval_no FROM correlations WHERE scale_no=$scale";
			@scale_intervals = map {$_->{'interval_no'}} @{$dbt->getData($sql)};
		}
		# hack needed because Gradstein subsumes the Quaternary
		#  within the Neogene JA 21.8.12
		if ( $scale == 69 )	{
			unshift @scale_intervals , 12;
		}
		if ( %binning )	{
			for my $interval ( keys %binning )	{
				if ( $baseAge{$interval} > $baseAge{$binning{$interval}} )	{
					$baseAge{$binning{$interval}} = $baseAge{$interval};
				}
				if ( $topAge{$interval} < $topAge{$binning{$interval}} || ! $topAge{$binning{$interval}} )	{
					$topAge{$binning{$interval}} = $topAge{$interval};
				}
			}
		}
		for my $no ( @with_dates )	{
			for my $scale_interval ( @scale_intervals )	{
				if ( $baseAge{$scale_interval} eq "NULL" || $topAge{$scale_interval} eq "NULL" )	{
					next;
				}
				if ( $baseAge{$scale_interval} >= $baseAge{$no} && $topAge{$scale_interval} <= $topAge{$no} )	{
					$lookup{$scale}{$no} = $scale_interval;
					$lookup{$scale}{$no} = ( $scale_interval =~ /[A-Za-z ]/ ) ? "'".$lookup{$scale}{$no}."'" : $lookup{$scale}{$no};
					last;
				}
			}
		}
	}
	for my $no ( @interval_nos )	{
		my $sql = "UPDATE interval_lookup SET ten_my_bin=$lookup{'bins'}{$no},fr2_bin=$lookup{'fossil record 2'}{$no},stage_no=$lookup{'73'}{$no},subepoch_no=$lookup{'72'}{$no},epoch_no=$lookup{'71'}{$no},period_no=$lookup{'69'}{$no} WHERE interval_no=$no";
		$dbh->do($sql);
	}

}

# JA 9.3.12
# super-simple function that replaces Schroeter's epic getIntervalGraph
sub allIntervals	{
	my $dbt = shift;
	my %intervals;
	my $sql = "SELECT IF((eml_interval IS NOT NULL AND eml_interval!=''),CONCAT(i.eml_interval,' ',i.interval_name),i.interval_name) AS name,il.* FROM intervals i,interval_lookup il WHERE i.interval_no=il.interval_no";
	$intervals{$_->{'interval_no'}} = $_ foreach @{$dbt->getData($sql)};
	return %intervals;
}

# Convenience
# JA: this one and the subtended functions getRangeByBoundary and
#  getRangeByInterval are actually pretty important
sub getRange {
    my $self = shift;
    my ($eml_max,$max,$eml_min,$min,%options) = @_;
    if ($max =~ /^[0-9.]+$/ || $min =~ /^[0-9.]+$/) {
        return $self->getRangeByBoundary($max,$min,%options),[],[];
    } else {
        return $self->getRangeByInterval(@_);
        
    }
}


# JA: this one is only ever used by FossilRecord::submitSearchTaxaForm,
#  so it's more or less obsolete and certainly way too complicated because
#  the hashes should be computable straight off of interval_lookup

# Pass in a range of intervals and this populates and passes back four hashes
# %pre hash has intervals that come before the range (including overlapping with part of the range)
# %post has intervals that come after the range (including overlapping with part of the range)
# %range has intervals in the range passed in
# %unknown has intervals  we dont' know what to do with, which are mostly larger intervals
#   that span both before and after the range of intervals
sub getCompleteRange {
    my ($self,$intervals) = @_;
    my (%pre,%range,%post,%unknown);

# disabled this for now pending rewrite of this function (if ever needed)
# JA 9.3.12
    my $ig; # = $self->getIntervalGraph();
    foreach my $i (@$intervals) {
        $range{$i} = $ig->{$i};
    }

    my $i = $intervals->[0];
    if ($i) {
        my $first_itv = $ig->{$i};
        while (my ($i,$itv) = each %$ig) {
            $itv->{'visited'} = 0;
        }
        $self->{precedes_lb} = {}; 
        #$self->markPrecedesLB($ig,$first_itv,0);
        
        while (my ($i,$itv) = each %$ig) {
            $itv->{'visited'} = 0;
        }
        $self->{follows_ub} = {};
        #$self->markFollowsUB($ig,$first_itv,0);

        foreach my $post_no (keys %{$self->{precedes_lb}{$i}}) {
            if (!$range{$post_no}) {
                $post{$post_no} = $ig->{$post_no};
            }
        }
        foreach my $pre_no (keys %{$self->{follows_ub}{$i}}) {
            if (!$range{$pre_no} && !$post{$pre_no}) {
                $pre{$pre_no} = $ig->{$pre_no};
            }
        }
    }
    while (my ($i,$itv) = each %$ig) {
        if (!$range{$i} && !$pre{$i} && !$post{$i}) {
            $unknown{$i} = $itv;
        }
    }
    return (\%pre,\%range,\%post,\%unknown);
}

# old Schroeter function that finds all intervals falling within a range
#  of Ma values
# heavily rewritten to take advantage of allIntervals by JA 9.3.12
sub getRangeByBoundary {
    my $self = shift;
    my ($max,$min,%options) = @_;
    my %intervals = allIntervals($self->{dbt});

    if ($max !~ /^[0-9]*\.?[0-9]+$/) {
        $max = 9999;
    }
    if ($min !~ /^[0-9]*\.?[0-9]+$/) {
        $min = 0;
    }
    if ($min > $max) {
        ($max,$min) = ($min,$max);
    }

    my @interval_nos;
    for my $no ( keys %intervals )	{
        if ( $options{'use_mid'} )	{
            my $mid = ( $intervals{$no}->{'base_age'} + $intervals{$no}->{'top_age'} ) / 2;
            if ($min <= $mid && $mid <= $max)	{
                push @interval_nos , $no;
            }
        } elsif ( $min <= $intervals{$no}->{'top_age'} && $max >= $intervals{$no}->{'base_age'} )	{
            push @interval_nos , $no;
        }
    }

    return \@interval_nos;
}

# You can pass in a 10 million year bin or an eml/interval pair
sub getRangeByInterval {
    my $self = shift;
    my $dbt = $self->{'dbt'};

    my ($eml_max,$max,$eml_min,$min,%options) = @_;

    my @errors = ();
    my @warnings = ();

    if (! $min) {
        $eml_min = $eml_max;
        $min = $max;
    }
    if (! $max) {
        $eml_max = $eml_min;
        $max = $min;
    }
    my @intervals;
    if ($max =~ /^[A-Z][a-z]+ \d$/ || $min =~ /^[A-Z][a-z]+ \d$/)	{
        # 10 M.Y. binning - i.e. Triassic 2
        my ($index1,$index2) = (-1,-1);
        for(my $i=0;$i<scalar(@TimeLookup::bins);$i++) {
            if ($max eq $TimeLookup::bins[$i]) {
                $index1 = $i;
            }
            if ($min eq $TimeLookup::bins[$i]) {
                $index2 = $i;
            }
        }

        if ($index1 < 0) {
            return ([],["Term $max not valid or not in the database"]);
        } elsif ($index2 < 0) {
            return ([],["Term $min not valid or not in the database"]);
        } else {
            if ($index1 > $index2) {
                ($index1,$index2) = ($index2,$index1);
            }
            @intervals = $self->mapIntervals(@TimeLookup::bins[$index1 .. $index2]);
        }
    } else {
        my ($max_interval_no,$min_interval_no);
        if ($max =~ /^\d+$/) {
            $max_interval_no = $max;
        } else {
            $max_interval_no = $self->getIntervalNo($eml_max,$max);
            my $max_name = $eml_max ? "$eml_max $max" : $max;
            if (!$max_interval_no) {
                push @errors, qq/The term "$max_name" not valid or not in the database/;
            }
        }
        if ($min =~ /^\d+$/) {
            $min_interval_no = $min;
        } else {
            $min_interval_no = $self->getIntervalNo($eml_min,$min);
            my $min_name = $eml_min ? "$eml_min $min" : $min;
            if (!$min_interval_no) {
                push @errors, qq/The term "$min_name" not valid or not in the database/;
            }
        }
   
        # if numbers weren't found for either interval, bomb out!
        if (@errors) {
            return ([],\@errors,\@warnings);
        }
       
        @intervals = $self->mapIntervals($max_interval_no,$min_interval_no);

    }
    return (\@intervals,\@errors,\@warnings);
}


# JA: only ever used by generateLookupTable in this module but used
#  repeatedly elsewhere, so it really needs to be looked over

# You can pass in both an integer corresponding to the scale_no of the scale or
# the keyword "bins" correspdoning to 10 my bins. Passes back a hashref where
# the key => value pair is the mapping. If $return_type is "name" the "value"
# will be the interval name, else it will be the interval_no.  For bins, it'll be the bin name always
# I.E.:  $hashref = $t->getScaleMapping('bins'), $hashref = $t->getScaleMapping(69,'name');
sub getScaleMapping {
    my $self = shift;
    my $dbt = $self->{'dbt'};

    my $scale = shift;
    my $return_type = shift || "number";

    # first retrieve a list of (parent) interval_nos included in the scale

    # This bins thing is slightly tricky - if the keyword "bins" is passed
    # in, then map to bins
    my @intervals;
    if ($scale =~ /bin/ && $scale !~ /fossil/i) {
        @intervals = @TimeLookup::bins;
    } elsif ($scale =~ /fossil/i) {
        @intervals = @TimeLookup::FR2_bins;
    } else {
        my $scale = int($scale);
        return unless $scale;
        my $sql = "SELECT interval_no FROM correlations WHERE scale_no=$scale";
        @intervals = map {$_->{'interval_no'}} @{$dbt->getData($sql)};
    }

    my %mapping = ();

    foreach my $i (@intervals) {
        # Map intervals accepts both 10 my bins and integers
        my @mapped = $self->mapIntervals($i);
        foreach my $j (@mapped) {
            $mapping{$j} = $i;
        }
    } 
   
    # If $scale is "bins" then the return type is always going
    # to be the name of the bin, so don't change anything
    if ($return_type =~ /name/ && $scale !~ /bin/) {
        # Return interval_no => interval_name mapping
        my %intervals = allIntervals($dbt);
        $mapping{$_} = $intervals{$mapping{$_}}->{'name'} foreach keys %intervals;
    } # Else default is to return interval_no => interval_no
    return \%mapping;
}


# old Schroeter function dramatically simplified by switching it to use
#  base_age and top_age values JA 18.10.11
# previously allowed interval objects to be passed in and out; now only
#  allows interval_nos and bin names to be passed in and always passes out
#  interval_nos
sub mapIntervals {
    my $self = shift;

    my @intervals = @_;
    return unless (@intervals);

    if ($isBin{$intervals[0]} || $isFR2Bin{$intervals[0]}) {
        # We gotta convert the bins into an array of regular intervals
        my %binmap;
        if ($isBin{$intervals[0]})	{
            while (my ($interval_no,$binname) = each %TimeLookup::binning) {
                push @{$binmap{$binname}},$interval_no;
            }
        } else	{
            while (my ($interval_no,$binname) = each %TimeLookup::FR2_binning) {
                push @{$binmap{$binname}},$interval_no;
            }
        }
        my @bins = @intervals;
        @intervals = ();
        foreach my $bin (@bins) {
            push @intervals, @{$binmap{$bin}};
        }
    # matches simple interval_no
    } elsif ($intervals[0] !~ /^\d+$/) {
        die("mapIntervals called with unknown input: ".join(",",@intervals));
    }

    # new code starts here
    # first find the age range of the submitted interval_nos
    my $sql = "SELECT max(base_age) base,min(top_age) top FROM interval_lookup WHERE interval_no IN(".join(',',@intervals).")";
    my $range = ${$self->{dbt}->getData($sql)}[0];

    # now this is trivial
    $sql = "SELECT interval_no,base_age,top_age FROM interval_lookup WHERE base_age<=".$range->{'base'}." AND top_age>=".$range->{'top'};
    my @no_refs = @{$self->{dbt}->getData($sql)};

    return map { $_->{'interval_no'} } @no_refs;
}


sub getBoundaries {
    my $self = shift;
    my $dbt = $self->{dbt};

    my %ub = ();
    my %lb = ();

    my $sql = "SELECT interval_no,top_age,base_age FROM interval_lookup";
    my @results = @{$dbt->getData($sql)};
    foreach my $row (@results) {
        $ub{$row->{interval_no}} = $row->{top_age};
        $lb{$row->{interval_no}} = $row->{base_age};
    }

    return (\%ub,\%lb);
}


# radically simplified to take advantage of direct hits on interval_lookup
#  JA 6.4.11
sub computeBinBounds {
    my $self = shift;
    my $binScheme = shift;
    my $dbt = $self->{'dbt'};

    my $upperbinbound = {};
    my $lowerbinbound = {};
    my @binCount = @TimeLookup::bins;
    if ( $binScheme =~ /fr/i )	{
        @binCount = @TimeLookup::FR2_bins;
    }
    for(my $i=0;$i < @binCount; $i++) {
        my $bin = $TimeLookup::bins[$i];
        my %binning = %TimeLookup::binning;
        if ( $binScheme =~ /fr/i )	{
            $bin = $TimeLookup::FR2_bins[$i];
            %binning = %TimeLookup::FR2_binning;
         }
        my @interval_nos;
        while (my ($itv_no,$in_bin) = each %binning) {
            if ($in_bin eq $bin) {
                push @interval_nos , $itv_no;
            }
        }

        my $sql = "SELECT MAX(base_age) lb,MIN(top_age) ub FROM interval_lookup WHERE interval_no IN (".join(',',@interval_nos).")";
        my $bounds = ${$dbt->getData($sql)}[0];
        $lowerbinbound->{$bin} = $bounds->{'lb'};
        $upperbinbound->{$bin} = $bounds->{'ub'};

        if ($i == 0) {
            $upperbinbound->{$bin} = 0;
        }
    }
    return ($upperbinbound,$lowerbinbound);
}


sub getIntervalNo {
    my $self = shift;
    my $dbt;
    if ($self->isa('DBTransactionManager')) {
        $dbt = $self;
    } else {
        $dbt = $self->{'dbt'};
    }
    my $dbh = $dbt->dbh;

    my $eml = shift;
    my $name = shift;

    my $sql = "SELECT interval_no FROM intervals ".
              " WHERE interval_name=".$dbh->quote($name);
    if ($eml) {
        $sql .= " AND eml_interval=".$dbh->quote($eml);
    } else {
        $sql .= " AND (eml_interval IS NULL or eml_interval='')";
    }
              
    my $row = ${$dbt->getData($sql)}[0];
    if ($row) {
        return $row->{'interval_no'};
    } else {
        return undef;
    }
}


# Utility function, parse input from form into valid eml+interval name pair, if possible
# Can be called directly or in obj oriented fashion, which is what the shift is for
sub splitInterval {
    shift if ref $_[0];
    my $interval_name = shift;

    my @terms = split(/ /,$interval_name);
    my @eml_terms;
    my @interval_terms;
    foreach my $term (@terms) {
        if ($term =~ /e\.|l\.|m\.|early|lower|middle|late|upper/i) {
            push @eml_terms, $term;
        } else {
            push @interval_terms, $term;
        }
    }
    my $interval = join(" ",@interval_terms);
    $interval =~ s/^\s*//;
    $interval =~ s/\s*$//;

    my $eml;
    if (scalar(@eml_terms) == 1) {
        $eml = 'Early/Lower' if ($eml_terms[0] =~ /e\.|lower|early/i);
        $eml = 'Late/Upper' if ($eml_terms[0] =~ /l\.|late|upper/i);
        $eml = 'Middle' if ($eml_terms[0] =~ /m\.|middle/i);
    } elsif(scalar(@eml_terms) > 1) {
        my ($eml0, $eml1);
        $eml0 = 'early'  if ($eml_terms[0] =~ /e\.|early|lower/i);
        $eml0 = 'middle' if ($eml_terms[0] =~ /m\.|middle/i);
        $eml0 = 'late'   if ($eml_terms[0] =~ /l\.|late|upper/i);
        $eml1 = 'Early'  if ($eml_terms[1] =~ /e\.|early|lower/i);
        $eml1 = 'Middle' if ($eml_terms[1] =~ /m\.|middle/i);
        $eml1 = 'Late'   if ($eml_terms[1] =~ /l\.|late|upper/i);
        if ($eml0 && $eml1) {
            $eml = $eml0.' '.$eml1;
        }
    }

    return ($eml,$interval);
}

# Returns an array of interval names in the correct order for a given scale
# With the newest interval first -- not finished yet, don't use
# PS 02/28/3004
# JA: actually, this function seems to work OK and is used heavily elsewhere
sub getScaleOrder {
    my $self = shift;
    my $dbt = $self->{'dbt'};
    my $dbh = $dbt->dbh;
    
    my $scale_no = shift;
    my $return_type = shift || "name"; #name or number

    my @scale_list = ();

    my $count;
    my @results;
    my %next_i;
    if ($return_type  =~ /number/) {
        my $sql = "SELECT c.correlation_no, c.base_age, c.interval_no, c.next_interval_no FROM correlations c".
                  " WHERE c.scale_no=".$dbt->dbh->quote($scale_no);
        @results = @{$dbt->getData($sql)};
    } else {
        my $sql = "SELECT c.correlation_no, c.base_age, c.interval_no, c.next_interval_no, i.eml_interval, i.interval_name FROM correlations c, intervals i".
                  " WHERE c.interval_no=i.interval_no".
                  " AND c.scale_no=".$dbt->dbh->quote($scale_no);
        @results = @{$dbt->getData($sql)};
    }
    my %ints;
    my %nexts;
    foreach my $row (@results) {
        $ints{$row->{'interval_no'}} = $row;
        $nexts{$row->{'next_interval_no'}} = 1;
    }
    my @base_intervals;
    foreach my $row (@results) {
        if (!$nexts{$row->{'interval_no'}}) {
            push @base_intervals,$row->{'interval_no'};
        }
    }
    @base_intervals = sort {
        $ints{$b}->{'base_age'} <=> $ints{$a}->{'base_age'} ||
        $ints{$b}->{'correlation_no'} <=> $ints{$a}->{'correlation_no'}
    } @base_intervals;
    my @intervals;
    foreach my $base (@base_intervals) {
        my $i = $base;
        while (my $interval = $ints{$i}) {
            push @intervals, $interval;
            $i = $interval->{'next_interval_no'};
        }
    }

    foreach my $row (reverse @intervals) {
        if ($return_type =~ /number/) {
            push @scale_list, $row->{'interval_no'};
        } else {
            if ($row->{'eml_interval'}) {
                push @scale_list, $row->{'eml_interval'} . ' ' .$row->{'interval_name'};
            } else {
                push @scale_list, $row->{'interval_name'};
            }
        }
    }
        
    return @scale_list;
}

# JA: this is an old Schroeter function that is similar to allIntervals but
#  sorts out period, epoch, etc. interval names and is used by Collection
#  and Download, so it shouldn't be deprecated
sub lookupIntervals {
    my ($self,$intervals,$fields) = @_;
    my $dbt = $self->{'dbt'};
    
    my @fields = ('interval_name','period_name','epoch_name','stage_name','ten_my_bin','FR2_bin','base_age','top_age','interpolated_base','interpolated_top');
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


# JA: this function now has really big problems because it depends on
#  the deprecated interval_hash values max_no, min_no, prev_no, and
#  lower_estimate_type; I think it's fixable by computing prev_no off of
#  an interval_lookup hit and replacing max_no/min_no logic with comparisons
#  based on actual base_age and top_age values

# JA 16-18.7.10
# computes and stores guesstimated ages for boundaries that are not directly
#  dated but do fall between boundaries with hard dates
# works with one scale segment at a time
# a segment must consist of several intervals that each correlate at least
#  partially with a single parent
sub interpolateBoundaries	{
	my $self = shift;
	my $dbt = $self->{'dbt'};
	my $dbh = $dbt->dbh;

	my $sql = "SELECT interval_no,interval_hash,ten_my_bin FROM interval_lookup ORDER BY interval_no";
	my @refs = @{$dbt->getData($sql)};
	my %itvs;
	foreach my $r ( @refs ) {
		my $VAR1;
		$itvs{$r->{interval_no}} = eval $r->{interval_hash};
		$itvs{$r->{interval_no}}->{bin} = $r->{ten_my_bin};
	}

	my %est_base;
	my %est_top;

	for my $i ( keys %itvs )	{
		if ( $itvs{$i}->{max_no} != $itvs{$i}->{min_no} || $itvs{$i}->{max_no} != $itvs{$itvs{$i}->{prev_no}}->{min_no} )	{
			my $last = $i;
			my $next = $itvs{$i}->{next_no};
			my @segment = ($i);
			while ( ( $itvs{$next}->{max_no} == $itvs{$i}->{min_no} || $itvs{$next}->{max_no} == $itvs{$i}->{max_no} ) && $itvs{$next}->{lower_estimate_type} eq "correlated" )	{
				$last = $next;
				push @segment , $last;
				$next = $itvs{$next}->{next_no};
			}
			if ( $i == $last )	{
				next;
			}
			# at this point last either has a firmly dated top or
			#  is at a scale top
			my $inseg = $#segment + 1;
			if ( $itvs{$i}->{max_no} != $itvs{$i}->{min_no} )	{
				$inseg -= 0.5;
			}
			if ( $itvs{$last}->{max_no} != $itvs{$last}->{min_no} )	{
				$inseg -= 0.5;
			}
			my $base = $itvs{$itvs{$i}->{min_no}}->{base_age};
			# the base might have been set in a previous round of
			# interpolation
			if ( $est_base{$i} > 0 )	{
				$base = $est_base{$i};
			}
			my $top = $itvs{$itvs{$last}->{max_no}}->{top_age};
			my $sum = 0;
			my $span = ( $base - $top ) / $inseg;
			if ( ! $est_base{$i} )	{
				$est_base{$i} = $itvs{$i}->{base_age};
			}
			if ( $itvs{$i}->{max_no} != $itvs{$i}->{min_no} )	{
				$sum = 0.5;
			} else	{
				$sum = 1;
			}
			$est_top{$i} = $base - $sum * $span;
			if ( $itvs{$last}->{max_no} != $itvs{$last}->{min_no} )	{
				$est_base{$last} = $top + $span / 2;
			} else	{
				$est_base{$last} = $top + $span;
			}
			$est_top{$last} = $itvs{$last}->{top_age};
			for my $s ( 1..$#segment-1 )	{
				$est_base{$segment[$s]} = $base - $span * $sum;
				$sum++;
				$est_top{$segment[$s]} = $base - $span * $sum;
			}
		}
	}
	foreach my $r ( @refs ) {
		my $i = $r->{interval_no};
		if ( ! $est_base{$i} )	{
			$est_base{$i} = "NULL";
		}
		if ( ! $est_top{$i} )	{
			$est_top{$i} = "NULL";
		}
		my $sql = "UPDATE interval_lookup SET interpolated_base=".$est_base{$i}.",interpolated_top=".$est_top{$i}." WHERE interval_no=".$i;
		$dbh->do($sql);
	}
}

# JA: only used in Scales::displayInterval
sub printBoundary {
    shift if ref ($_[0]);
    my $bound = shift;
    return $bound if ($bound == 0);
    $bound =~ s/(0)+$//;
    $bound =~ s/\.$//;
    return $bound;
}

return 1;

