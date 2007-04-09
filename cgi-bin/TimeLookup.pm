package TimeLookup;

use Data::Dumper;
use CGI::Carp;
#use Memoize;
use strict;
#memoize('findFollowing');

# Ten million year bins, in order from oldest to youngest
@TimeLookup::bins = ("Cenozoic 6", "Cenozoic 5", "Cenozoic 4", "Cenozoic 3", "Cenozoic 2", "Cenozoic 1", "Cretaceous 8", "Cretaceous 7", "Cretaceous 6", "Cretaceous 5", "Cretaceous 4", "Cretaceous 3", "Cretaceous 2", "Cretaceous 1", "Jurassic 6", "Jurassic 5", "Jurassic 4", "Jurassic 3", "Jurassic 2", "Jurassic 1", "Triassic 4", "Triassic 3", "Triassic 2", "Triassic 1", "Permian 4", "Permian 3", "Permian 2", "Permian 1", "Carboniferous 5", "Carboniferous 4", "Carboniferous 3", "Carboniferous 2", "Carboniferous 1", "Devonian 5", "Devonian 4", "Devonian 3", "Devonian 2", "Devonian 1", "Silurian 2", "Silurian 1", "Ordovician 5", "Ordovician 4", "Ordovician 3", "Ordovician 2", "Ordovician 1", "Cambrian 4", "Cambrian 3", "Cambrian 2", "Cambrian 1");

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
    "799" => "Cambrian 1" # Nemakit-Daldynian
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

sub new {
    my $c = shift;
    my $dbt = shift;

    my $self  = {'ig'=>undef,'dbt'=>$dbt,'set_boundaries'=>0, 'sl'=>{},'il'=>{}};
    bless $self,$c;
}

# Convenience
sub getRange {
    my $self = shift;
    my ($eml_max,$max,$eml_min,$min,%options) = @_;
    if ($max =~ /^[0-9.]+$/ || $min =~ /^[0-9.]+$/) {
        return $self->getRangeByBoundary($max,$min,%options),[],[];
    } else {
        return $self->getRangeByInterval(@_);
        
    }
}

# Boundaries are given in millions of years
# Intervals must fall completely within the bounds, unless use_mid is passed, in which case
# intervals midpoints must fall completley within the bounds
sub getRangeByBoundary {
    my $self = shift;
    my $ig = $self->getIntervalGraph;

    my ($max,$min,%options) = @_;

    if ($max !~ /^[0-9]*\.?[0-9]+$/) {
        $max = 9999;
    }
    if ($min !~ /^[0-9]*\.?[0-9]+$/) {
        $min = 0;
    }
    if ($min > $max) {
        ($max,$min) = ($min,$max);
    }

    my @intervals;
    my ($ub,$lb) = $self->getBoundaries;
    foreach my $i (keys %$ig) {
        if ($ub->{$i} ne "" && $lb->{$i} ne "") {
            if ($options{'use_mid'}) {
                my $mid = ($ub->{$i} + $lb->{$i})/2;
                if ($min <= $mid && $mid <= $max) {
                    push @intervals,$i;
                }
            } else {
                if ($min <= $ub->{$i} && $lb->{$i} <= $max) {
                    push @intervals,$i;
                }
            }
        }
    }

    return \@intervals;
}

# You can pass in a 10 million year bin or an eml/interval pair
sub getRangeByInterval {
    my $self = shift;
    my $ig = $self->getIntervalGraph;
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
            } else {
                if ($min_interval_no != $max_interval_no &&
                    $self->isObsolete($max_interval_no)) {
                    push @warnings, qq/The term "$max_name" may no longer be valid; please use a newer, equivalent term/;
                }
            }
        }
        if ($min =~ /^\d+$/) {
            $min_interval_no = $min;
        } else {
            $min_interval_no = $self->getIntervalNo($eml_min,$min);
            my $min_name = $eml_min ? "$eml_min $min" : $min;
            if (!$min_interval_no) {
                push @errors, qq/The term "$min_name" not valid or not in the database/;
            } else {
                if ($self->isObsolete($min_interval_no)) {
                    push @warnings, qq/The term "$min_name" may no longer be valid; please use a newer, equivalent term/;
                }
            }
        }
   
        # if numbers weren't found for either interval, bomb out!
        if (@errors) {
            return ([],\@errors,\@warnings);
        }
       

        my @range = $self->findPath($max_interval_no,$min_interval_no);
        @intervals = $self->mapIntervals(@range);

    }
    return (\@intervals,\@errors,\@warnings);
}

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

  
    # This bins thing is slightly tricky - if the keyword "bins" is passed
    # in, then map to bins
    my @intervals;
    if ($scale =~ /bin/) {
        @intervals = @TimeLookup::bins;
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
        my $ig = $self->getIntervalGraph;
        while (my ($k,$v) = each %mapping) {
            $mapping{$k} = $ig->{$v}->{'name'};
        }
    } # Else default is to return interval_no => interval_no
    return \%mapping;
}

sub getBins {
    return @TimeLookup::bins;
}

sub getBinning {
    return \%TimeLookup::binning;
}

# Tells whether an interval is obsolete, i.e. Gallic, Tertiary, Quaternary
# You can pass it an interval_no or the interval object directly
# Algotihm is slightly tricky.  First filter by intervals
# that have no intervals assigned into them.  (children is empty, "defunct" children is not)
# Then make sure at least one of those children fits this criteria:
#   is from a lower scale rank (at time of assignment, not currently)
#   is from the same continent
#   was never a parent to the current interval
# These criteria are to distinguish between inter-scale correlations and true child-parent style subdivisions
# TBD? second type of obsolete - intervals without a place in the composite i.e. (Ufimian)
# TBD: alternate criteria - child is not boundary crosser? boundary crossers tend to be correlations, not sub divisions
sub isObsolete {
    my $self = shift;
    my $dbt = $self->{'dbt'};
    my $ig = $self->getIntervalGraph;

    my $itv = shift;
    $itv = ref $itv ? $itv : $ig->{$itv};
  
    if (@{$itv->{'defunct'}} && !@{$itv->{'children'}}) {
        my $itv_rank = $TimeLookup::rank_order{$itv->{'best_scale'}->{'scale_rank'}};
        foreach my $c (@{$itv->{'defunct'}}) {
            if ($c == $itv->{'max'} || $c == $itv->{'min'}) {
                next;
            } 
#            if ($c->{'best_scale'}->{'continent'} ne $itv->{'best_scale'}->{'continent'}) {
#                next;
#            }
            my $valid_child = 0;
            for (my $i = 0; $i < @{$c->{'all_max'}};$i++) {
                my $max = $c->{'all_max'}->[$i];
                my $min = $c->{'all_min'}->[$i];
                my $scale = $c->{'all_max_scales'}->[$i];
                if ($scale->{'continent'} eq $itv->{'best_scale'}->{'continent'} &&
                    $TimeLookup::rank_order{$scale->{'scale_rank'}} > $itv_rank) {
#                    print "FOUND defunct child $c->{name}:$scale->{scale_rank}:$scale->{abbrev} for $itv->{name}:$itv->{best_scale}->{scale_rank}\n";
                    return 1;
                }
            }
        }
    }
    if ((!$itv->{'prev'} || ($itv->{'prev'} && $itv->{'prev'}->{'next'} != $itv)) && 
        (!$itv->{'next'} || ($itv->{'next'} && $itv->{'next'}->{'prev'} != $itv))) {
#        print "$itv->{name} is obsolete because it has no prev or next\n";
        return 2;
    }
    return 0;
}


# Given a set of starting intervals (or bins), finds intervals that are implied
# by those intervals, including all child intervals and parent intervals implied
# by the children.  Note that saying something like (mapIntervals("Bin 1"),mapIntervals("Bin 2")) 
# may not be the same as map(Intervals("Bin 1","Bin 2")) since the second call will get
# additional children and parent intervals that map like between the bins while the first won't 
# Can pass in interval objects, intervals, or ten my bins (text) and will return appropriate output
# I.E.:
# @intervals = $t->mapIntervals(40,41,42,43,44)
# @interval_objects = $t->mapIntervals($itv1,$itv2,$itv3)
# @intervals = $t->mapIntervals("Cretaceous 1","Cretaceous 2","Cretaceous 3");
sub mapIntervals {
    my $self = shift;
    my $ig = $self->getIntervalGraph;

    my @intervals = @_;
    return unless (@intervals);

    # Reset the visited values - each call is indepedent of others
    foreach my $itv (values %$ig) {
        $itv->{'visited'} = 0;
    }

    my $input_type = '';
    if (ref($intervals[0])) {
        $input_type = 'objects';
    } elsif ($intervals[0] =~ /^\w+ \d$/) {
        # We gotta convert the bins into an array of regular intervals
        $input_type = 'bins';
        my %binmap;
        while (my ($interval_no,$binname) = each %TimeLookup::binning) {
            push @{$binmap{$binname}},$interval_no;
        }
        my @bins = @intervals;
        @intervals = ();
        foreach my $bin (@bins) {
            push @intervals, @{$binmap{$bin}};
        }
        @intervals = map {$ig->{$_}} @intervals; 
    } elsif ($intervals[0] =~ /^\d+$/) {
        # Code expects intervals objects below - convert back afterwards
        $input_type = 'integers';
        @intervals = map {$ig->{$_}} @intervals; 
    } else {
        die("mapIntervals called with unknown input: ".join(",",@intervals));
    }
    
    my @q = @intervals;
    
    my @mapped = ();
    my %parents;
    while (@q) {
        while (my $itv = shift @q) {
            unless ($itv->{'visited'}) {
                push @mapped,$itv;
                $itv->{'visited'} = 1;    
                $parents{$itv->{'max'}} = $itv->{'max'} if ($itv->{'max'});
                $parents{$itv->{'min'}} = $itv->{'min'} if ($itv->{'min'});
                
                foreach my $c (@{$itv->{'children'}}) {
                    if ($c->{'max'}->{'visited'} && 
                        $c->{'min'}->{'visited'} && 
                        ! $c->{'visited'}) {
                        push @q,$c;
                    }
                }
            }
        }

        # Put in terms equivalent to a term a or sequence already mapped
        foreach my $itv (values %$ig) {
            if (!$itv->{'visited'} && $itv->{'shared_lower'} && $itv->{'shared_upper'}) {
                foreach my $sl (@{$itv->{'shared_lower'}}) {
                    foreach my $ul (@{$itv->{'shared_upper'}}) {
                        if ($sl == $ul && $sl != $itv && $sl->{'visited'} && $ul->{'visited'}) {
                            push @q, $itv;
                        }
                    } 
                }

            }
        }
        # Put in terms equivalent to a term a or sequence already mapped
        foreach my $itv (values %$ig) {
            if (!$itv->{'visited'} && $self->{'equiv'}->{$itv}) {
                my $set = $self->{'equiv'}->{$itv};
                my $all_visited = 1;
                foreach my $itv (@$set) {
                    if (!$itv->{'visited'}) {
                        $all_visited = 0;
                        last;
                    }
                }
                if ($all_visited) {
#                    print "All members equiv to $itv->{name} [".join(",",map{$_->{'name'}} @$set)."] were found\n";
                    push @q, $itv;
                }
            }
        }
        # Not totally sure quite why this works right now
        # TBD: Range across scales
        # TBD: equiv local/globals
        foreach my $itv (values %$ig) {
            if (!$itv->{'visited'} && 
                $itv->{'prev'} && $itv->{'prev'}->{'visited'} && 
                $itv->{'next'} && $itv->{'next'}->{'visited'}) {
                push @q, $itv;
            }
        }
        # Is covered means it has children that span the 
        # entire length of the parent, so the parent
        # should be included
        foreach my $p (values %parents) {
            if (!$p->{'visited'} && $self->isCovered($p)) {
                push @q,$p;
            }
        }
    }
    if ($input_type eq 'integers' || $input_type eq 'bins') {
        return map {$_->{'interval_no'}} @mapped;
    } else {
        return @mapped;
    }
}


# This basically implements Dijkstra's shortest path algorithm, but with some
# tweaks:  
#   * After we starting the path in one direction (searching for nodes
#   going forward (earlier in time) or backward (older in time) we want to keep
#   going in that same direction and not backtrack - implement this by having
#   the distance be positive (forward) or backward.  
#   * We don't just search for the best scale, we search for any scale to satisfy 
#   the condition, but weight against older scales (search all_prev, all_next, not just prev,next)
#   * We can "levels" (i.e. following an edge up to a parent or down to a child)
#   but changing levels is weighted against so we won't do it unless we have to
#   The level change only happens on "shared" boundaries, or boundary crossers so
#   we can guarantee a relationship like "A is always younger than B" or vice versa 
#   when trying to find a valid path
sub findPath {
    my $self = shift;
    my ($from,$to) = @_; 
    if ($from == $to) {
        return ($from);
    } 
    my $ig = $self->getIntervalGraph;
    $from = ref $from ? $from : $ig->{$from};
    $to = ref $to ? $to : $ig->{$to};

    # Reset graph
    my $infinity = 9999999;
    foreach my $itv (values %$ig) {
        $itv->{'visited'} = $infinity;
    }
    $from->{'visited'} = 0;

    my %previous;
    my $pq = PriorityQueue->new();
    $pq->insert($from,0);
    while (my $v = $pq->pop()) {

        if ($v == $to) {
            last;
        }
        my @edges = ();

        if ($v->{'visited'} >= 0) {
            if ($v->{'all_next'}) {
                foreach my $next (@{$v->{'all_next'}}) {
                    # The BEST next gets a lower weight
                    if ($v->{'next'} == $next) {
                        push @edges, [$next,1];
                    } else {
                        push @edges, [$next,5];
                    }
                }
            }
            if ($v->{'min'} && $v->{'max'} != $v->{'min'}) {
                # Discourage changing "levels" 
                push @edges, [$v->{'min'},71];
            }
            if ($v->{'max'} && $v->{'shared_lower'} && $v->{'shared_lower'} == $v->{'max'}->{'shared_lower'}) {
                push @edges, [$v->{'max'},71];
            }
            foreach my $c (@{$v->{'children'}}) {
                # Discourage changing "levels" downward even mor
                if ($v->{'shared_lower'} && $v->{'shared_lower'} == $c->{'shared_lower'}) {
                    push @edges,[$c,103];
                }
            }
        }
        if ($v->{'visited'} <= 0) {
            if ($v->{'all_prev'}) {
                foreach my $prev (@{$v->{'all_prev'}}) {
                    if ($prev->{'next'} == $v) {
                        push @edges, [$prev,-1];
                    } else {
                        push @edges, [$prev,-5];
                    }
                }
            }
            if ($v->{'max'} && $v->{'max'} != $v->{'min'}) {
                push @edges, [$v->{'max'},-71];
            }
            if ($v->{'min'} && $v->{'shared_upper'} && $v->{'shared_upper'} == $v->{'min'}->{'shared_upper'}) {
                push @edges, [$v->{'min'},-71];
            }
            foreach my $c (@{$v->{'children'}}) {
                if ($v->{'shared_upper'} && $v->{'shared_upper'} == $c->{'shared_upper'}) {
                    push @edges,[$c,-103];
                }
            }
        }

        foreach my $e (@edges) {
            my ($u,$weight) = @$e;
            if (abs($u->{'visited'}) > abs($v->{'visited'} + $weight)) {
                $u->{'visited'} = $v->{'visited'} + $weight;
                $pq->insert($u,abs($u->{'visited'}));
                $previous{$u->{'interval_no'}} = $v->{'interval_no'};
            }
        }
    }
    my @path = ();
    if ($to->{'visited'} != $infinity) {
        my $next = $to->{'interval_no'};
        while ($previous{$next}) {
            push @path, $next;
            $next = $previous{$next};
        }
        push @path, $from->{'interval_no'};
    }

    return @path;
}

sub makePrecedesHash {
    my $self = shift;
    my $ig = $self->getIntervalGraph;
    while (my ($i,$itv) = each %$ig) {
        $itv->{'visited'} = 0;
    }
    while (my ($i,$itv) = each %$ig) {
        $self->markFollowing($ig,$itv,0);
        #foreach ($self->findFollowing($ig,$itv)) {
        #    $self->{'precedes'}{$i}{$_} = 1;
        #}
    }
}

sub markFollowing {
    my ($self,$ig,$itv,$depth) = @_;
    return if ($itv->{'visited'});
    $itv->{'visited'} = 1;
    if ($itv->{'next'}) {
        my @all_next = ($itv->{'next'});
        my @q = ();
        foreach my $c (@{$itv->{'next'}{'children'}}) {
            #if ($c->{'max'} == $itv->{'next'}) {
            push @q, [$c,$itv->{'next'}];
            #}
        }
        my %seen;
        while (my $pair = shift @q) {
            my ($c,$p) = @$pair;
            next if ($seen{$c->{'interval_no'}}); 
            $seen{$c->{'interval_no'}} = 1;
            if ($c->{'max'} == $p) {
                push @all_next, $c;
                foreach my $gc (@{$c->{'children'}}) {
                    push @q, [$gc,$c];
                }
            }
        }
#        foreach my $c (@{$next->{'children'}}) {
#            if ($c->{'max'} == $next) {
#                push @all_next, $c;
#            }
#        }
        my $next = $itv->{'next'};
        my $max = $next->{'max'};
        my $min = $next->{'min'};
        %seen = ();
        while ($max) {
            next if ($seen{$max->{'interval_no'}}); $seen{$max->{'interval_no'}} = 1;
            if ($next->{'shared_lower'} && $next->{'shared_lower'} == $max->{'shared_lower'}) {
                push @all_next, $max;
                push @all_next, $min if ($max != $min);
                $next = $max;
                $max = $next->{'max'};
                $min = $next->{'min'};
            } elsif ($next->{'max'} && $next->{'max'} != $next->{'min'}) {
                push @all_next, $next->{'min'};
                $next = $min;
                $max = $next->{'max'};
                $min = $next->{'min'};
            } else {
                $max = undef;
            }
        }
        foreach my $next_itv (@all_next) {
            $self->{'precedes'}{$itv->{'interval_no'}}{$next_itv->{'interval_no'}} = 1;
#            $self->markFollowing($ig,$next_itv,$depth+1);
#            foreach my $next_no (keys %{$self->{'precedes'}{$next_itv->{'interval_no'}}}) {
#                $self->{'precedes'}{$itv->{'interval_no'}}{$next_no} = 1;
#            }
        }
    }
    if ($itv->{'min'}) {
        $self->markFollowing($ig,$itv->{'min'});
        foreach my $next_no (keys %{$self->{'precedes'}{$itv->{'min'}{'interval_no'}}}) {
            $self->{'precedes'}{$itv->{'interval_no'}}{$next_no} = 1;
        }
    }
    foreach my $c (@{$itv->{'children'}}) {
        if (($c->{'shared_upper'} && $c->{'shared_upper'} == $itv->{'shared_upper'}) || ($c->{'max'} == $itv && $c->{'min'} != $itv)) {
            $self->markFollowing($ig,$c);
            foreach my $next_no (keys %{$self->{'precedes'}{$c->{'interval_no'}}}) {
                $self->{'precedes'}{$itv->{'interval_no'}}{$next_no} = 1;
            }
        }
    }
}


sub makePrecedesHashX {
    my $self = shift;
    my $ig = $self->getIntervalGraph;
    while (my ($i,$itv) = each %$ig) {
        my $next = $itv->{'next'};
        my $j = 0;
        my %seen = ();
        while ($next) {
            last if ($seen{$next->{'interval_no'}}); $seen{$next->{'interval_no'}} = 1;
            $self->{precedes}{$i}{$next->{'interval_no'}} = 1;
            $next = $next->{'next'};
        }
    }
    while (my ($i,$itv) = each %$ig) {
        my $min = $itv->{'min'};
        my $j = 0;
        my %seen;
        while ($min) {
            last if ($seen{$min->{'interval_no'}}); $seen{$min->{'interval_no'}} = 1;
            foreach (keys %{$self->{precedes}{$min->{'interval_no'}}}) {
#                last if ($self->{precedes}{$i}{$_});
                $self->{precedes}{$i}{$_} = 1; 
            }
            $min = $min->{'min'};
        }
    }
    while (my ($i,$itv) = each %$ig) {
        my @q = ();
        foreach my $c (@{$itv->{'children'}}) {
            push @q, [$c,$itv];
        }
        my %seen;
        while (my $pair = shift @q) {
            my ($c,$p) = @$pair;
            if ($c->{'shared_upper'} == $p->{'shared_upper'} || ($c->{'max'} == $p && $c->{'min'} != $p)) {
                next if ($seen{$c->{'interval_no'}}); $seen{$c->{'interval_no'}} = 1;
                foreach (keys %{$self->{precedes}{$c->{'interval_no'}}}) {
#                    last if ($self->{precedes}{$i}{$_});
                    $self->{precedes}{$i}{$_} = 1; 
                }
                foreach my $gc (@{$c->{'children'}}) {
                    push @q, [$gc,$c];
                }
            }
        }
    }
}


sub getBoundaries {
    my $self = shift;
    my $return_type = shift;
    my $ig = $self->getIntervalGraph;

    $self->makePrecedesHash();

    unless ($self->{'set_boundaries'}) {
        $self->{'set_boundaries'} = 1;
        my ($UPPER_MAX,$UPPER_EQ,$UPPER_MIN,$LOWER_MAX,$LOWER_EQ,$LOWER_MIN) = (1,2,3,4,5,6);

        my @q = ();
        foreach my $itv (values %$ig) {
            if ($itv->{'boundary'} > 0) {
                push @q,[$itv,$LOWER_EQ,$itv,0,$itv]; 
            }
        }
        @q = sort {
            $a->[0]->{'boundary_scale'}->{'pubyr'} <=> $b->[0]->{'boundary_scale'}->{'pubyr'} ||
            $a->[0]->{'boundary_scale'}->{'scale_no'} <=> $b->[0]->{'boundary_scale'}->{'scale_no'} ||
            $a->[0]->{'interval_no'} <=> $b->[0]->{'interval_no'}
        } @q;
        # Holocene/Quaternary/Neogene/Cenozoic/Phanerozoic upper bounds need to be put in manually (0 ma AKA NOW) PS 9/14/2005
        foreach (32,25,12,1,751) {
            my $itv = $ig->{$_};
            push @q,[$itv,$UPPER_EQ,{'boundary'=>0,'boundary_scale'=>{'abbrev'=>'gl'}},0,$itv];
        }

        while(my $constraint = pop @q) {
            my ($itv,$action,$src,$depth,$from) = @$constraint;
            next if ($depth > 2);
            my $abbrev = $src->{'boundary_scale'}->{'abbrev'};
            
            my $changed = 0;
            my $conflict = 0;
            my @is_younger_than = ();
            my @is_older_than = ();
            if ($action == $UPPER_MAX) {
                @is_older_than = ('upper_boundary','upper_min');
                @is_younger_than = ('lower_max','lower_boundary');
            } elsif ($action == $UPPER_EQ) {
                @is_older_than = ('upper_min');
                @is_younger_than = ('upper_max','lower_boundary','lower_max');
            } elsif ($action == $UPPER_MIN) {
                @is_older_than = ();
                @is_younger_than = ('lower_max','lower_boundary','upper_max','upper_boundary');
            } elsif ($action == $LOWER_MAX) {
                @is_older_than = ('lower_boundary','lower_min','upper_boundary','upper_min');
                @is_younger_than = ();
            } elsif ($action == $LOWER_EQ) {
                @is_older_than = ('upper_min','upper_boundary','lower_min');
                @is_younger_than = ('lower_max');
            } elsif ($action == $LOWER_MIN) {
                @is_older_than = ('upper_boundary','upper_min');
                @is_younger_than = ('lower_max','lower_boundary');
            }
                
            foreach (@is_younger_than) {
                my $key = $_.$abbrev;
                if (exists $itv->{$key}) {
                    unless ($src->{'boundary'} <= $itv->{$key}) {
                        $conflict = " $src->{'boundary'} is not younger than $key $itv->{$key}";
                        last;
                    }
                }
            }
            foreach (@is_older_than) {
                my $key = $_.$abbrev;
                if (exists $itv->{$key}) {
                    unless ($src->{'boundary'} >= $itv->{$key}) {
                        $conflict = " $src->{'boundary'} is not older than $key $itv->{$key}";
                        last;
                    }
                }
            }
            
            # Deals with Rhaetian case.  Gradstein 2004 says [Norian --> Rhaetian --> Hettangian]
            # Only gradstein says [Sevatian maps into Norian, next interval is Lower Hettangian], so basically
            # Rhaetian is cut out of the picture entirely and assumed to be part of Norian.  In This case
            # don't let the information propagated from Hettangian screw up Rhaetians boundary, but
            # let the Sevation boundary continue to have the old info. 
            if ($self->{precedes}{$itv->{interval_no}}{$src->{interval_no}} && ($action == $LOWER_EQ || $action == $LOWER_MAX)) {
                $conflict = " following interval $src->{name} can't set lower, lower max";
            }
            if ($self->{precedes}{$src->{interval_no}}{$itv->{interval_no}} && ($action == $UPPER_EQ || $action == $LOWER_EQ || $action == $UPPER_MIN || $action == $LOWER_MIN)) {
                $conflict = " preceding interval $src->{name} can't set lower, upper, lower min, or upper min";
            }

            if (!$conflict) {
                if (1 || $src == $from || 
                    $abbrev eq $itv->{'best_scale'}->{'abbrev'} ||
                    $abbrev eq $itv->{'best_boundary'}->{'abbrev'} ||
                    $abbrev eq 'gl') {
                    if ($action == $UPPER_MAX) {
                        if (!exists $itv->{'upper_max'.$abbrev} || $src->{'boundary'} < $itv->{'upper_max'.$abbrev}) {
                            $itv->{'upper_max'.$abbrev} = $src->{'boundary'};
                            $itv->{'upper_max'.$abbrev.'src'} = $src;
                            $changed++;
                        }
                    } elsif ($action == $UPPER_EQ) {
                        if (!exists $itv->{'upper_boundary'.$abbrev}) {
                            $itv->{'upper_boundary'.$abbrev} = $src->{'boundary'};
                            $itv->{'upper_boundary'.$abbrev.'src'} = $src;
                            $changed++;
                        }
                    } elsif ($action == $UPPER_MIN) {
                        if (!exists $itv->{'upper_min'.$abbrev} || $src->{'boundary'} > $itv->{'upper_min'.$abbrev}) {
                            $itv->{'upper_min'.$abbrev} = $src->{'boundary'};
                            $itv->{'upper_min'.$abbrev.'src'} = $src;
                            $changed++;
                        }
                    } elsif ($action == $LOWER_MAX) {
                        if (!exists $itv->{'lower_max'.$abbrev} || $src->{'boundary'} < $itv->{'lower_max'.$abbrev}) {
                            $itv->{'lower_max'.$abbrev} = $src->{'boundary'};
                            $itv->{'lower_max'.$abbrev.'src'} = $src;
                            $changed++;
                        }
                    } elsif ($action == $LOWER_EQ) {
                        if (!exists ($itv->{'lower_boundary'.$abbrev})) {
                            $itv->{'lower_boundary'.$abbrev} = $src->{'boundary'};
                            $itv->{'lower_boundary'.$abbrev.'src'} = $src;
                            $changed++;
                        }
                    } elsif ($action == $LOWER_MIN) {
                        if (!exists $itv->{'lower_min'.$abbrev} || $src->{'boundary'} > $itv->{'lower_min'.$abbrev}) {
                            $itv->{'lower_min'.$abbrev} = $src->{'boundary'};
                            $itv->{'lower_min'.$abbrev.'src'} = $src;
                            $changed++;
                        }
                    }
                }
            }
            if ($changed) {
#                print _printConstraint($constraint);
                push @{$itv->{'constraints'}},$constraint;
            } elsif ($conflict) {
                push @{$itv->{'conflicts'}},[@{$constraint},$conflict];
                next;
            } else {
                # No new data was provided, stop here
                next;
            } 

            if ($itv->{'lower_max'.$abbrev} && 
                $itv->{'lower_max'.$abbrev} == $itv->{'lower_min'.$abbrev} &&
                ! $itv->{'lower_boundary'.$abbrev}) {
                $itv->{'lower_boundary'.$abbrev} = $itv->{'lower_max'.$abbrev};
                $itv->{'lower_boundary'.$abbrev.'src'} = $itv->{'lower_max'.$abbrev.'src'};
                $action = $LOWER_EQ;
            }
            if ($itv->{'upper_max'.$abbrev} && 
                $itv->{'upper_max'.$abbrev} == $itv->{'upper_min'.$abbrev} &&
                ! $itv->{'upper_boundary'.$abbrev}) {
                $itv->{'upper_boundary'.$abbrev} = $itv->{'upper_max'.$abbrev};
                $itv->{'upper_boundary'.$abbrev.'src'} = $itv->{'upper_max'.$abbrev.'src'};
                $action = $UPPER_EQ;
            }

            my @next_q;
            # Handle propagation to previous intervals
#            foreach my $prev (@{$itv->{'all_prev'}}) 
#                if ($prev->{'next'} == $itv) 
            if ($itv->{'prev'} && $itv->{'prev'}->{'next'} == $itv) {
                if ($action == $LOWER_EQ) {
                    push @next_q, [$itv->{prev},$UPPER_EQ,$src,$depth+1,$itv];
                } elsif ($action == $LOWER_MIN) {
                    push @next_q, [$itv->{prev},$UPPER_MIN,$src,$depth+1,$itv];
                } elsif ($action == $LOWER_MAX) {
                    push @next_q, [$itv->{prev},$UPPER_MAX,$src,$depth+1,$itv];
                } 
            }

            # Handle propagation to next interval
            if ($itv->{'next'} && $itv->{'next'}->{'prev'} == $itv) {
                if ($action == $UPPER_EQ) {
                    push @next_q, [$itv->{'next'},$LOWER_EQ,$src,$depth+1,$itv];
                } elsif ($action == $UPPER_MIN) {
                    push @next_q, [$itv->{'next'},$LOWER_MIN,$src,$depth+1,$itv];
                } elsif ($action == $UPPER_MAX) {
                    push @next_q, [$itv->{'next'},$LOWER_MAX,$src,$depth+1,$itv];
                } 
            }

            # Handle propagation to other equivalent intervals 
            if ($action == $UPPER_EQ ||
                $action == $UPPER_MAX ||
                $action == $UPPER_MIN) {
                if ($itv->{'shared_upper'}) {
                    foreach my $shared (@{$itv->{'shared_upper'}}) {
                        push @next_q,[$shared,$action,$src,$depth,$itv];
                    }
                }
            }
            if ($action == $LOWER_EQ ||
                $action == $LOWER_MAX ||
                $action == $LOWER_MIN) {
                if ($itv->{'shared_lower'}) {
                    foreach my $shared (@{$itv->{'shared_lower'}}) {
                        push @next_q,[$shared,$action,$src,$depth,$itv];
                    }
                }
            }

            # Handle propagation to max and min interval
            if ($itv->{'max'}) {
                if ($action == $LOWER_EQ || $action == $LOWER_MAX || $action == $LOWER_MIN) {
                    if ($itv->{'shared_lower'} && $itv->{'max'}->{'shared_lower'} == $itv->{'shared_lower'}) {
                        push @next_q, [$itv->{'max'},$action,$src,$depth,$itv];
                    } else {
                        if ($action == $LOWER_EQ || $action == $LOWER_MIN) {
                            push @next_q, [$itv->{'max'},$LOWER_MIN,$src,$depth,$itv];
                        }
                    }
                    if ($itv->{'max'} != $itv->{'min'}) {
                        if ($action == $LOWER_EQ || $action == $LOWER_MAX) {
                            push @next_q, [$itv->{'min'},$LOWER_MAX,$src,$depth,$itv];
                        }
                    }
                } elsif ($action == $UPPER_EQ || $action == $UPPER_MIN || $action == $UPPER_MAX) {
                    if ($itv->{'shared_upper'} && $itv->{'min'}->{'shared_upper'} == $itv->{'shared_upper'}) {
                        push @next_q, [$itv->{'min'},$action,$src,$depth,$itv];
                    } else {
                        if ($action == $UPPER_EQ || $action == $UPPER_MAX) {
                            push @next_q, [$itv->{'min'},$UPPER_MAX,$src,$depth,$itv];
                        }
                    }
                    if ($itv->{'max'} != $itv->{'min'}) {
                        if ($action == $UPPER_EQ || $action == $UPPER_MIN) {
                            push @next_q, [$itv->{'max'},$UPPER_MIN,$src,$depth,$itv];
                        }
                    }
                }
            }

            # Handle propagation to child intervals
            foreach my $c (@{$itv->{'children'}}) {
                if ($action == $LOWER_EQ || $action == $LOWER_MAX || $action == $LOWER_MIN) {
                    if ($c->{'max'} == $c->{'min'}) {
                        # Completely contained child
                        if ($itv->{'shared_lower'} && $c->{'shared_lower'} == $itv->{'shared_lower'}) {
                            push @next_q, [$c,$action,$src,$depth,$itv];
                        } else {
                            if ($action == $LOWER_EQ || $action == $LOWER_MAX) {
                                push @next_q, [$c,$LOWER_MAX,$src,$depth,$itv];
                            }
                        }
                    } elsif ($c->{'max'} != $itv) {
                        # Boundary crosser at max side
                        if ($action == $LOWER_EQ) {
                            push @next_q, [$c,$LOWER_MIN,$src,$depth,$itv];
                            push @next_q, [$c,$UPPER_MAX,$src,$depth,$itv];
                        } elsif ($action == $LOWER_MAX) {
                            push @next_q, [$c,$UPPER_MAX,$src,$depth,$itv];
                        } elsif ($action == $LOWER_MIN) {
                            push @next_q, [$c,$LOWER_MIN,$src,$depth,$itv];
                        }
                    } elsif ($c->{'min'} != $itv) {
                        # Boundary crosser at min side
                        if ($action == $LOWER_EQ || $action == $LOWER_MAX) {
                            push @next_q, [$c,$LOWER_MAX,$src,$depth,$itv];
                        } 
                    }
                } elsif ($action == $UPPER_EQ || $action == $UPPER_MAX || $action == $UPPER_MIN) {
                    if ($c->{'max'} == $c->{'min'}) {
                        # Completely contained child
                        if ($itv->{'shared_upper'} && $c->{'shared_upper'} == $itv->{'shared_upper'}) {
                            push @next_q, [$c,$action,$src,$depth,$itv];
                        } else {
                            if ($action == $UPPER_EQ || $action == $UPPER_MIN) {
                                push @next_q, [$c,$UPPER_MIN,$src,$depth,$itv];
                            }
                        }
                    } elsif ($c->{'min'} != $itv) {
                        # Boundary crosser at min side
                        if ($action == $UPPER_EQ) {
                            push @next_q, [$c,$LOWER_MIN,$src,$depth,$itv];
                            push @next_q, [$c,$UPPER_MAX,$src,$depth,$itv];
                        } elsif ($action == $UPPER_MIN) {
                            push @next_q, [$c,$LOWER_MIN,$src,$depth,$itv];
                        } elsif ($action == $UPPER_MAX) {
                            push @next_q, [$c,$UPPER_MAX,$src,$depth,$itv];
                        }
                    } elsif ($c->{'max'} != $itv) {
                        # Boundary crosser at max  side
                        if ($action == $UPPER_EQ || $action == $UPPER_MIN) {
                            push @next_q, [$c,$UPPER_MIN,$src,$depth,$itv];
                        }
                    }

                }
            }
#            print $next_q[0][2]->{'boundary_scale'}->{'pubyr'}." vs. ".$next_q[0]->[4]->{'best_scale'}->{'pubyr'}."\n";
#            @next_q = grep {$_->[2]->{'boundary_scale'}->{'pubyr'} <= $_->[4]->{'best_scale'}->{'pubyr'}} @next_q;
            push @q, @next_q;
        }

        # Makes guess at the remaining boundaries. First we try to get a continent specific boundary.
        # If one doesn't exist then we fall back to a global estimate. Lastly try other continents
        foreach my $itv (values %$ig) {
            my %abbrs = ();
            my $abbrev = $itv->{'best_scale'}->{'abbrev'};
            $abbrs{$abbrev} = 1;
            if ($itv->{'max'}) {
                $abbrs{$itv->{'max'}->{'best_scale'}->{'abbrev'}} = 2;
            }
            if ($itv->{'min'}) {
                $abbrs{$itv->{'max'}->{'best_scale'}->{'abbrev'}} = 2;
            }
            $abbrs{'gl'} = 3 unless $abbrs{'gl'};

#            my @abbrevs = ('As','Au','Eu','NZ','NA','SA');
            my @abbrevs = keys %abbrs;
            @abbrevs = sort {$abbrs{$a} <=> $abbrs{$b}} @abbrevs;
#            for (my $i = 0;$i < @abbrevs; $i++) {
#                if ($abbrevs[$i] eq $abbrev) {
#                    splice(@abbrevs,$i,1);
#                }
#            }
#            unshift @abbrevs, 'gl';
#            unshift @abbrevs, $abbrev;

            foreach my $abbrev (@abbrevs) {
                foreach my $bound ('upper_max','lower_max') {
                    if ($itv->{$bound.$abbrev} =~ /\d/) {
                        if ($itv->{$bound} !~ /\d/ || $itv->{$bound} > $itv->{$bound.$abbrev}) {
                            $itv->{$bound} = $itv->{$bound.$abbrev};
                            $itv->{$bound.'src'} = $itv->{$bound.$abbrev.'src'};
                        }
                    }
                }
                foreach my $bound ('upper_min','lower_min') {
                    if ($itv->{$bound.$abbrev} =~ /\d/) {
                        if ($itv->{$bound} !~ /\d/ || $itv->{$bound} < $itv->{$bound.$abbrev}) {
                            $itv->{$bound} = $itv->{$bound.$abbrev};
                            $itv->{$bound.'src'} = $itv->{$bound.$abbrev.'src'};
                        }
                    }
                }
                foreach my $bound ('upper_boundary','lower_boundary') {
                    if ($itv->{$bound.$abbrev} =~ /\d/) {
                        if ($itv->{$bound} !~ /\d/) {
                            $itv->{$bound} = $itv->{$bound.$abbrev};
                            $itv->{$bound.'src'} = $itv->{$bound.$abbrev.'src'};
                        }
                    }
                }
                #if ($itv->{$_.$abbrev} =~ /\d/) {
                #    $itv->{$_} = $itv->{$_.$abbrev};
                #    $itv->{$_.'src'} = $itv->{$_.$abbrev.'src'};
                #} elsif ($itv->{$_.'gl'} =~ /\d/) {
                #    $itv->{$_} = $itv->{$_.'gl'};
                #    $itv->{$_.'src'} = $itv->{$_.'gl'.'src'};
                #}
            }
            if ($itv->{'lower_boundary'} =~ /\d/) {
                $itv->{'lower_estimate_type'} = 'direct';
            }
            if ($itv->{'upper_boundary'} =~ /\d/) {
                $itv->{'upper_estimate_type'} = 'direct';
            }
        }

        my %set = (); my %unset = ();
        foreach my $itv (values %$ig) {
#            if ($self->isObsolete($itv)) {
#            }
            if ($itv->{'lower_boundary'} =~ /\d/ && $itv->{'upper_boundary'} =~ /\d/) {
                $set{$itv} = $itv;
            } else {
                $unset{$itv} = $itv;
            }
        }

        my @leaves = ();
        foreach my $itv (values %unset) {
            my $has_child = 0;
            foreach my $c (@{$itv->{'children'}}) {
                if ($c->{'max'} == $itv && $c->{'min'} == $itv) {
                    $has_child = 1;
                }
            }
            if (!$has_child) {
                push @leaves, $itv;
            }
        }
#        print "1SET: ".scalar(values(%set))."\n";
#        print "1UNSET: ".scalar(values(%unset))."\n";

        my @obsolete;
        foreach my $itv (@leaves) {
            if ($self->isObsolete($itv) == 1) {
                push @obsolete, $itv;
                next;
            }
            if ($itv->{'upper_boundary'} !~ /\d/ && $itv->{'upper_min'} =~ /\d/) {
                $itv->{'upper_boundary'} = $itv->{'upper_min'};
                $itv->{'upper_boundarysrc'} = $itv->{'upper_minsrc'};
                $itv->{'upper_estimate_type'} = 'correlated';
            }
            if ($itv->{'upper_boundary'} !~ /\d/ && $itv->{'upper_max'} =~ /\d/) {
                $itv->{'upper_boundary'} = $itv->{'upper_max'};
                $itv->{'upper_boundarysrc'} = $itv->{'upper_maxsrc'};
                $itv->{'upper_estimate_type'} = 'correlated';
            }
            if ($itv->{'lower_boundary'} !~ /\d/ && $itv->{'lower_max'} =~ /\d/) {
                $itv->{'lower_boundary'} = $itv->{'lower_max'};
                $itv->{'lower_boundarysrc'} = $itv->{'lower_maxsrc'};
                $itv->{'lower_estimate_type'} = 'correlated';
            }
            if ($itv->{'lower_boundary'} !~ /\d/ && $itv->{'lower_mn'} =~ /\d/) {
                $itv->{'lower_boundary'} = $itv->{'lower_min'};
                $itv->{'lower_boundarysrc'} = $itv->{'lower_minsrc'};
                $itv->{'lower_estimate_type'} = 'correlated';
            }
            if ($itv->{'lower_boundary'} =~ /\d/ && $itv->{'upper_boundary'} =~ /\d/) {
                delete $unset{$itv};
                $set{$itv} = $itv;
            }
        }
#        print "1bSET: ".scalar(values(%set))."\n";
#        print "1bUNSET: ".scalar(values(%unset))."\n";

        foreach (values %$ig) {
            $_->{'visited'} = 0;
            $_->{'depth'} = 0;
        }
#        foreach (@leaves) {
#            $_->{'depth'} = 0;
#        }
        @q = @leaves;
        while (my $itv = shift @q) {
            next if $itv->{'visited'};
            $itv->{'visited'} = 1;
            if ($itv->{'max'}) {
                push @q, $itv->{'max'}; 
                $itv->{'max'}->{'depth'} = ($itv->{'max'}->{'depth'} > $itv->{'depth'}) ? $itv->{'max'}->{'depth'} : $itv->{'depth'} + 1;
            }
            if ($itv->{'min'} && $itv->{'max'} != $itv->{'min'}) {
                push @q, $itv->{'min'}; 
                $itv->{'min'}->{'depth'} = ($itv->{'min'}->{'depth'} > $itv->{'depth'}) ? $itv->{'min'}->{'depth'} : $itv->{'depth'} + 1;
            }
        }
        
        foreach my $itv (sort {$a->{'depth'} <=> $b->{'depth'}} values %unset) {
            if (!$itv->{'depth'} && !$set{$itv}) {
#                print "NO DEPTH FOR $itv->{interval_no} $itv->{name} ?\n";
            }
            my ($min,$max) = $self->getFromChildren($itv);
            if ($max =~ /\d/) {
                $itv->{'lower_boundary'} = $max;
                $itv->{'lower_estimate_type'} = 'children';
            }
            if ($min =~ /\d/) {
                $itv->{'upper_boundary'} = $min;
                $itv->{'upper_estimate_type'} = 'children';
            }
            if ($itv->{'lower_boundary'} =~ /\d/ && $itv->{'upper_boundary'} =~ /\d/) {
                delete $unset{$itv};
                $set{$itv} = $itv;
            }
        }

#        print "2SET: ".scalar(values(%set))."\n";
#        print "2UNSET: ".scalar(values(%unset))."\n";

        # Fix for gallic, quaternary, tertiary, etc
        foreach my $itv (sort {$a->{'interval_no'} <=> $b->{'interval_no'}} @obsolete) {
#            print "$itv->{name} IS OBSOLETE\n";
            my ($min,$max) = $self->getFromChildren($itv,'defunct');
            if ($itv->{'lower_boundary'} !~ /\d/) {
                if ($itv->{'prev'} && $itv->{'prev'}->{'upper_boundary'} =~ /\d/ && !$self->isObsolete($itv->{'prev'})) {
                    $itv->{'lower_boundary'} = $itv->{'prev'}->{'upper_boundary'};
                    $itv->{'lower_boundarysrc'} = $itv->{'prev'};
                    $itv->{'lower_estimate_type'} = 'previous';
                } else {
                    $itv->{'lower_boundary'} = $max;
                    $itv->{'lower_estimate_type'} = 'children';
                }
#                if ($itv->{'prev'} && $itv->{'prev'}->{'upper_boundary'} eq '') {
#                    $itv->{'prev'}->{'upper_boundary'} = $max;
#                }
            }
            if ($itv->{'upper_boundary'} !~ /\d/) {
                if ($itv->{'next'} && $itv->{'next'}->{'lower_boundary'} =~ /\d/ && !$self->isObsolete($itv->{'next'})) {
                    $itv->{'upper_boundary'} = $itv->{'next'}->{'lower_boundary'};
                    $itv->{'upper_boundarysrc'} = $itv->{'next'};
                    $itv->{'upper_estimate_type'} = 'next';
                } else {
                    $itv->{'upper_boundary'} = $min;
                    $itv->{'upper_estimate_type'} = 'children';
                }
#                if ($itv->{'next'} && $itv->{'next'}->{'lower_boundary'} eq '') {
#                    $itv->{'next'}->{'lower_boundary'} = $min;
#                }
            }
            if ($itv->{'lower_boundary'} =~ /\d/&& $itv->{'upper_boundary'} =~ /\d/) {
                delete $unset{$itv};
                $set{$itv} = $itv;
            }
        }
#        print "3SET: ".scalar(values(%set))."\n";
#        print "3UNSET: ".scalar(values(%unset))."\n";
        # Fix for cases like the edicaran/sinian/poundian
        foreach my $itv (values %unset) {
            if ($itv->{'next'} && $itv->{'next'}->{'lower_boundary'} =~ /\d/) {
                if ($itv->{'upper_boundary'} !~ /\d/) {
                    $itv->{'upper_boundary'} = $itv->{'next'}->{'lower_boundary'};
                    $itv->{'upper_boundarysrc'} = $itv->{'next'};
                    $itv->{'upper_estimate_type'} = 'next';
                }
            } elsif (!$itv->{'next'} && $itv->{'all_next'} && 
                $itv->{'all_next'}->[0]->{'lower_boundary'} =~ /\d/) {
                if ($itv->{'upper_boundary'} !~ /\d/) {
                    $itv->{'upper_boundary'} = $itv->{'all_next'}->[0]->{'lower_boundary'};
                    $itv->{'upper_boundarysrc'} = $itv->{'all_next'}->[0];
                    $itv->{'upper_estimate_type'} = 'next';
                }
            }
            if ($itv->{'prev'} && $itv->{'prev'}->{'upper_boundary'} =~ /\d/) {
                if ($itv->{'lower_boundary'} !~ /\d/) {
                    $itv->{'lower_boundary'} = $itv->{'prev'}->{'upper_boundary'};
                    $itv->{'lower_boundarysrc'} = $itv->{'prev'};
                    $itv->{'lower_estimate_type'} = 'previous';
                }
            } elsif (!$itv->{'prev'} && $itv->{'all_prev'} && 
                $itv->{'all_prev'}->[0]->{'upper_boundary'} =~ /\d/) {
                if ($itv->{'lower_boundary'} !~ /\d/) {
                    $itv->{'lower_boundary'} = $itv->{'all_prev'}->[0]->{'upper_boundary'};
                    $itv->{'lower_boundarysrc'} = $itv->{'all_prev'}->[0];
                    $itv->{'lower_estimate_type'} = 'previous';
                }
            }
            if ($itv->{'lower_boundary'} =~ /\d/ && $itv->{'upper_boundary'} =~ /\d/) {
                $set{$itv} = $itv;
                delete $unset{$itv};
            }
        }
#        print "4SET: ".scalar(values(%set))."\n";
#        print "4UNSET: ".scalar(values(%unset))."\n";
    }
    if ($return_type ne 'bins') {
        # Finally finished, return
        my %upper = ();
        my %lower = ();
        foreach my $itv (values %$ig) {
            $lower{$itv->{'interval_no'}} = $itv->{'lower_boundary'}; 
            $upper{$itv->{'interval_no'}} = $itv->{'upper_boundary'}; 
        }
        return (\%upper,\%lower);
    } else {
        return $self->_computeBinBounds();
    }
}

sub getFromChildren {
    my ($self,$itv,$defunct) = @_;
    my $max = -1;
    my $min = 999999;
    my @children;
    if ($defunct) {
        @children = @{$itv->{'defunct'}};
    } else {
        @children = @{$itv->{'children'}};
    }
#    print "FROM CHILDREN: $itv->{name}\n";
    foreach my $c (@children) {
#        if ($itv->{'interval_no'} == 3) { print "CHILD $c->{name} $c->{lower_boundary} $c->{upper_boundary}\n"; }
        if ($defunct || $c->{'max'} == $itv && $c->{'min'} == $itv) {
            if ($c->{'lower_boundary'} > $max) {
                $max = $c->{'lower_boundary'};
#                print " MAX $max - from $c->{interval_no}\n"
            } 
            if ($c->{'upper_boundary'} < $min) {
                $min = $c->{'upper_boundary'};
#                print " MIN $min - from $c->{interval_no}\n"
            }
        }
    }
    
    $max = ($max == -1) ? "" : $max;
    $min = ($min == 999999) ? "" : $min;
    return ($min,$max);
}

sub _computeBinBounds {
    my $self = shift;
    my $ig = $self->getIntervalGraph;

    my $upperbinbound = {};
    my $lowerbinbound = {};
    for(my $i=0;$i < @TimeLookup::bins; $i++) {
        my $bin = $TimeLookup::bins[$i];
#        my @intervals = map{$ig->{$_}} $self->mapIntervals($bin);
        my @intervals;
        while (my ($itv_no,$in_bin) = each %TimeLookup::binning) {
            if ($in_bin eq $bin) {
                push @intervals, $ig->{$itv_no};
            }
        }

        # find the boundary ages for the bin by checking the boundaries of
        #  all intervals falling within it
        foreach my $itv ( @intervals )    {
#            if ($itv->{'lower_boundarysrc'}->{'boundary_scale'}->{'abbrev'} eq 'gl') {
                my $lower_bound = $itv->{'lower_boundarygl'};
                if ($lower_bound !~ /\d/) {
                    foreach my $abbrev ('As','Au','Eu','NZ','NA','SA') {
                        if ($itv->{'lower_boundary'.$abbrev} =~ /\d/) {
                            $lower_bound = $itv->{'lower_boundary'.$abbrev};
#                            print "NO LOWER BOUND, TRY LOCAL: $lower_bound\n";
                        }
                    }
                }
                if ( $lower_bound =~ /\d/ && $lower_bound > $lowerbinbound->{$bin} )   {
                    $lowerbinbound->{$bin} = $lower_bound;
                }
#            }

#            if ($itv->{'upper_boundarysrc'}->{'boundary_scale'}->{'abbrev'} eq 'gl') {
#                my $upper_bound = $itv->{'upper_boundarygl'};
#                if ($upper_bound !~ /\d/) {
#                    foreach my $abbrev ('As','Au','Eu','NZ','NA','SA') {
#                        if ($itv->{'upper_boundary'.$abbrev} =~ /\d/) {
#                            $upper_bound = $itv->{'upper_boundary'.$abbrev};
#                            print "NO UPPER BOUND, TRY LOCAL: $upper_bound\n";
#                        }
#                    }
#                }
#                if ( $upper_bound =~ /\d/ && $upper_bound < $upperbinbound->{$bin} || $upperbinbound->{$bin} eq "" ) {
#                        $upperbinbound->{$bin} = $upper_bound;
#                }
#            }
#            print "LB $lower_bound UB $upper_bound ITV $itv->{interval_no}<BR>";
        }
        if ($i == 0) {
            $upperbinbound->{$bin} = 0;
        } else {
            my $next_bin = $TimeLookup::bins[$i-1];
            $upperbinbound->{$bin} = $lowerbinbound->{$next_bin};
        }
    }
    return ($upperbinbound,$lowerbinbound);
}

sub getIntervalGraph {
    my $self = shift;

    # If its already been created, return it
    return $self->{'ig'} if $self->{'ig'};
    
    my $dbt = $self->{'dbt'};
    my $dbh = $dbt->dbh;

    # Else create a new one
    my $ig = {};
    $self->{'ig'} = $ig;

    my %scales;
    my $sql = "SELECT s.created,s.scale_no,s.scale_name,s.continent,s.scale_rank,s.reference_no,r.pubyr FROM scales s, refs r WHERE s.reference_no=r.reference_no";
    foreach (@{$dbt->getData($sql)}) {
        my $abbrev = $_->{'continent'};
        if ($abbrev =~ /^(\w)(\w)\w+(?: (\w))?/) {
            if ($3) {
                $abbrev = $1.$3;
            } else {
                $abbrev= $1.$2;
            }
        }
        $_->{'abbrev'} = $abbrev;
        $scales{$_->{'scale_no'}} = $_;
    }

    $sql = "SELECT interval_no,eml_interval,interval_name FROM intervals";
    my @intervals = @{$dbt->getData($sql)};

    $sql = "SELECT scale_no, interval_no, next_interval_no, max_interval_no, min_interval_no, lower_boundary FROM correlations c";
    my @results = @{$dbt->getData($sql)};

    my %correlations = ();
    foreach (@results) {
        push @{$correlations{$_->{'interval_no'}}},$_;
    }
    # We first initialize with empty hashes.  Do this so we can directly reference
    # the interval objects even if they haven't been fully initialized yet
    foreach (@intervals) {
        my $interval_no = $_->{'interval_no'};
        my $name = $_->{'interval_name'};
        $name = "$_->{eml_interval} $name" if ($_->{eml_interval});
        $ig->{$interval_no} = {"interval_no"=>$interval_no,"name"=>$name,"distance"=>0,"children"=>[],"defunct"=>[]};
    }
    #  Now do the major gruntwork of initalizing the interval object
    foreach (@intervals) {
        my $interval_no = $_->{'interval_no'};
        $self->_initInterval($ig,$ig->{$interval_no},$correlations{$interval_no},\%scales);
    }

    #  We have to do this after everything above has been set in place
    foreach (@intervals) {
        my $interval_no = $_->{'interval_no'};
        $self->_findSharedBoundaries($ig->{$interval_no});
        $self->_findEquivalentTerms($ig->{$interval_no});
    }
    
    # manual carboniferous fix
    $self->_combineBoundaries('shared_upper',$ig->{18},$ig->{27});
    $self->_combineBoundaries('shared_lower',$ig->{18},$ig->{28});
    return $ig;
}

# This function determines whether a given interval has a set of children which full spans
# The enter length of the interval.  I.E.  {Oligocene,Eocene,Paleocene} covers Paleogene so implies it
# Do this by looking for a boundary crosser at each end
sub isCovered {
    my $self = shift;
    my $itv = shift;

    my $cross_lb = 0;
    my $cross_ub = 0;
    foreach my $c (@{$itv->{'children'}}) {
        if ($c->{'visited'}) {
            if ($c->{'max'} != $itv || ($itv->{'shared_lower'} && $c->{'shared_lower'} == $itv->{'shared_lower'})) {
                $cross_lb = 1;
            }
            if ($c->{'min'} != $itv || ($itv->{'shared_upper'} && $c->{'shared_upper'} == $itv->{'shared_upper'})) {
                $cross_ub = 1;
            }
        }
    }

    if ($cross_lb && $cross_ub) {
        return 1;
    } else {
        return 0;
    }
}

# Ask John:
# that bookend lower boundary - can be detected?


sub best_by_continent {
    my ($itv,$type,$continent) = @_;
    return unless $itv;
    return unless $itv->{'all_'.$type.'_scales'};

    my $idx = -1;
    my @scales = @{$itv->{'all_'.$type.'_scales'}};
    for($idx = 0; $idx < @scales; $idx++) { 
        if ($scales[$idx]->{'continent'} eq $continent) {
            last;
        }
    }
    if ($idx >=  @scales) {
        return;
    } else {
        return $itv->{'all_'.$type}->[$idx];
    }
}

# If anything is array 1 matches anything in array 2, return true
sub matchAny {
    my @A1 = @{$_[0]};
    my @A2 = @{$_[1]};

    my $matched = 0;
    foreach my $j (@A1) {
        foreach my $k (@A2) {
            if ($j == $k) {
                return 1;
            }
        }
    }
    return 0;
}

# This algorithm sets variable in the interval object if the minimum correlate
# shares the same boundary as the interval itself called "shared_upper".  "shared_upper" is
# an array of references to all intervals that share that boundary.  
# Detection is easy enough.  Assuming you have the following structure:
# Where horizontal arrays denote "next" intervals, vertical lines denotes a boundary, The C -> A
# denotes the C's min correlate, the D -> B denotes B's max correlate
#  A -|-> B   then the shared_upper set is {A,C} and the shared_lower set is {B,D}
#  ^  |   ^   So just look for structures like this.
#  C -|-> D
sub _findSharedBoundaries {
    my $self = shift;
   
    my ($A,$B,$C,$D,$B2);
    $C = shift; 

    return unless $C && $C->{'min'}; 
    my $max_name = quotemeta $C->{'max'}->{'name'};
    if ($C->{'name'} =~ /Early\/Lower $max_name$/ || 
        $C->{'name'} =~ /early Early $max_name$/ ||
        $C->{'name'} =~ /early $max_name$/) {
#        print "COMBINING LOWER $C->{'max'}->{'name'} && $C->{name}\n";
        $self->_combineBoundaries('shared_lower',$C,$C->{'max'});
    }
    my $min_name = quotemeta $C->{'min'}->{'name'};
    if ($C->{'name'} =~ /Late\/Upper $min_name$/ ||
        $C->{'name'} =~ /late Late $min_name$/ ||
        $C->{'name'} =~ /late $min_name$/) {
#        print "COMBINING UPPER $C->{'min'}->{'name'} && $C->{name}\n";
        $self->_combineBoundaries('shared_upper',$C,$C->{'min'});
    }

    return unless $C && $C->{'next'} && $C->{'min'};
    my %continents;
    $continents{$_} = 1 for map {$_->{'continent'}} @{$C->{'all_next_scales'}};
    foreach my $continent (keys %continents) {
        # These correspond to the diagam above
        if (scalar(keys(%continents)) > 1) {
            $A = best_by_continent($C,'min',$continent);
            $D = best_by_continent($C,'next',$continent);
            $B = best_by_continent($D,'max',$continent);
            $B2 = best_by_continent($A,'next',$continent);
        } else {
            $A = $C->{'min'};
            $D = $C->{'next'};
            $B = $D->{'max'};
            $B2 = $A->{'next'};
        }

        next unless ($A && $B && $B2 && $C && $D);
       
        # The "matchAny" comes up with Calabrian - Calabrians max is Pleistocene, which shared with E. Pleistocene
        # The previous intervals min is L. Pliocene, which has a next of E. Pleistocene, which doesn't match
        # Pleistocene directly but matches E. Pleistocene in its shared_lower
        if ($B == $B2 || 
            ($B->{'shared_lower'} && matchAny($B->{'shared_lower'},[$B2])) ||
            ($B2->{'shared_lower'} && matchAny($B2->{'shared_lower'},[$B]))
            ) {

            $self->_combineBoundaries('shared_upper',$A,$C);
            $self->_combineBoundaries('shared_lower',$B,$D);
        }
    }
}


# See cases like Griesbachian -> Olenekian -> Smithian -> Spathian & Griesbacian -> Nammalian -> Spathian
# Or Kungurian -> Ufimian -> Wordian. L
# Look for pattern A -> B -> C matches A -> D -> C OR A -> B -> C matches A -> [D -> E] -> C
sub _findEquivalentTerms {
    my $self = shift;
    my $itv = shift;

    my %all_next;
    foreach my $next (@{$itv->{'all_next'}}) {
        $all_next{$next}=$next;
    }
    my @paths = ();
    foreach my $prev (@{$itv->{'all_prev'}}) {
        foreach my $scale_no (keys %{$prev->{'by_scale'}}) {
            my $found_equiv = 0;
            my @next_by_scale = ();
            my $next = $prev;
            foreach (my $i = 0;$i < 4;$i++) {
                $next = $next->{'by_scale'}{$scale_no}{'next'};
                last unless $next;
                last if ($next == $itv);
                if ($all_next{$next}) {
                    $found_equiv = 1;
                    last;
                }
                push @next_by_scale, $next;
                
            }
            if ($found_equiv && @next_by_scale) {
                push @paths, \@next_by_scale;
            }
        }
    }

    foreach my $path (@paths) {
        my @p = @$path;
#        print "Found path for $itv->{name} [".join(",",map{$_->{'name'}} @p)."]\n";
        $self->{'equiv'}->{$itv} = $path;
        # Useful for the interval_hash field in interval_lookup
        $itv->{'equiv'} = $path;
        foreach my $other_itv (@p) {
            $self->{'equiv'}->{$other_itv} = [$itv];
        }
        $self->_combineBoundaries('shared_upper',$itv,$p[-1]);
        $self->_combineBoundaries('shared_lower',$itv,$p[0]);
    }
#    if ($self->isObsolete($itv) == 2) {
    if (0) {
        foreach my $prev (@{$itv->{'all_prev'}}) {
            # 1 level
            if ($prev->{'next'} != $itv && 
                $prev->{'next'} && $prev->{'next'}->{'next'} &&
                matchAny([$prev->{'next'}->{'next'}],$itv->{'all_next'})) {
#                print "Found match: $itv->{name} vs. $prev->{next}->{name}\n"; 
                $self->_combineBoundaries('shared_upper',$itv,$prev->{'next'});
                $self->_combineBoundaries('shared_lower',$itv,$prev->{'next'});
#                $itv->{'equiv'} = [$prev->{'next'}];
#                $prev->{'next'}->{'equiv'} = [$itv];
            }
            # 2 level
            if ($prev->{'next'} != $itv && 
                $prev->{'next'} && $prev->{'next'}->{'next'} && $prev->{'next'}->{'next'}->{'next'} &&
                matchAny([$prev->{'next'}->{'next'}->{'next'}],$itv->{'all_next'})) {
#                print "Found match: $itv->{name} vs. $prev->{next}->{name} & $prev->{next}->{next}->{name}\n";
                $self->_combineBoundaries('shared_upper',$itv,$prev->{'next'}->{'next'});
                $self->_combineBoundaries('shared_lower',$itv,$prev->{'next'});
#                $itv->{'equiv'} = [$prev->{'next'},$prev->{'next'}->{'next'}];
            }
        }
    }
#    }
}

# Type should be either "shared_upper" or "shared_lower" for shared upper (younger) and lower (older_ boundaries respectively
# This makres a special key ($type) to point to a shared array in memory. The array is conceptually like a set.
# For example, Late Ordovician and Ordovican share an upper boundary (they are interval objects);
#   $ordovician->{shared_upper}        --> [$ordovician,$late_ordovician]
#   $late_ordovician->{shared_upper}   -----^ 
sub _combineBoundaries {
    my $self = shift;
    my ($type,$itv1,$itv2) = @_;
    if ($itv1->{$type} && $itv2->{$type}) {
        if ($itv1->{$type} != $itv2->{$type}) {
            #We're mergine two separate "islands"
            foreach my $itv (@{$itv2->{$type}}) {
                push @{$itv1->{$type}}, $itv;
                $itv->{$type} = $itv1->{$type};
            }
        } else {
            # We've already done this
#                print "WARNING: $itv1->{interval_no} and $itv2->{interval_no} have already been marked as being part of the same set!\n";
        }
    } elsif ($itv1->{$type}) {
        # Interval is already part of a larger set, add the minimum correlate to the larger set
        push @{$itv1->{$type}},$itv2;
        $itv2->{$type} = $itv1->{$type};
    } elsif ($itv2->{$type}) {
        # Minimum correlate is already part of a larger set, add interval
        # to the larger set
        push @{$itv2->{$type}},$itv1;
        $itv1->{$type} = $itv2->{$type};
    } else {
        #  This shared boundary isn't part of any larger set of shared boundaries,
        #  so create a new "shared boundary" object
        my $shared_set = [$itv1,$itv2];
        $itv1->{$type} = $shared_set;
        $itv2->{$type} = $shared_set;
    }
}

# Initializes an interval pseudo-object.  The object has the following fields:
#   interval_no
#   visited: internal bookkeeping for when we map intervals. Think of visited in the sense of graph nodes
#   children: all intervals that map into this interval, not ordered though
#   defunct: all intervals that mapped into this interval at one point in time but no longer do
#   max: pointer to maximum correlate
#   min: pointer to minimum correlate
#   next: pointer to next (newer) interval
#   all_next: array of pointers to all next intervals, sorted by pubyr desc (all_next[0] is newest)
#   prev: pointer to prevous (older) interval
#   all_prev: array of pointers to all prev intervals, sorted by pubyr desc (all_prev[0] is newest)
#   next_scale: pointer to best scale that gives a next interval
#   all_scales: pointer to all scale objects which use this interval
#   boundary_scale: pointer to best scale that gives a boundary
#   lower_boundary: floating point lower_boundary value
#   upper_boundary: flatoing point upper_boundary value

sub _initInterval {
    my $self = shift;

    my ($ig,$itv,$correlations,$scales) = @_;

    my %seen_parents = ();

    my %all_by_scale = ();
    my %all_with_next = ();
    my %all_with_boundary = ();
    my %all_with_max = ();
    my $best_max = {};

    # First intialize all the boundaries and pointers
    foreach my $row (@$correlations) {
        my $scale = $scales->{$row->{'scale_no'}};
       
        $all_by_scale{$row->{'scale_no'}} = $row;
        if ($row->{'max_interval_no'} > 0 && $row->{'max_interval_no'} != $row->{'interval_no'}) {
            $all_with_max{$row->{'max_interval_no'}} = [$row,$scale];
        }
        if ($row->{'next_interval_no'} > 0) {
            unless ($row->{'interval_no'} == 77 && $row->{'next_interval_no'} == 76) {
                $all_with_next{$row->{'next_interval_no'}} = [$row,$scale];
                $itv->{'by_scale'}{$row->{'scale_no'}}{'next'} = $ig->{$row->{'next_interval_no'}};
            }
        }
        if ($row->{'lower_boundary'} > 0) {
            $all_with_boundary{$row->{'next_interval_no'}} = [$row,$scale];
        }
        $seen_parents{$row->{'max_interval_no'}}++ if ($row->{'max_interval_no'});
        $seen_parents{$row->{'min_interval_no'}}++ if ($row->{'min_interval_no'});
    }


    # Catalog and store all scales - first turn the scale_no into
    # scale objects with the map, then just directly sort those
    # on their hash keys - We then store an array of scale objects for later use
    my @all_scales = sort {
        $b->{'pubyr'} <=> $a->{'pubyr'} ||
        $b->{'scale_no'} <=> $a->{'scale_no'}
    } map {$scales->{$_}} keys %all_by_scale;
    $itv->{'all_scales'} = \@all_scales;
    $itv->{'best_scale'} = $all_scales[0];

    sub scale_sort {
        $b->[1]->{'pubyr'} <=> $a->[1]->{'pubyr'} ||
        $b->[1]->{'scale_no'} <=> $a->[1]->{'scale_no'}
    };

    my $best_continent = $all_scales[0]->{'continent'};
   
    # Mark the best boundary
    # The grep makes sure the boundary you use is consistent with whether
    # we're using the interval as a local or global interval now, mixing and matching is bad
    my @all_with_boundary = sort scale_sort values %all_with_boundary;
    my @all_with_boundary_continent = grep {$_->[1]->{'continent'} eq $best_continent} @all_with_boundary;
    if (@all_with_boundary) {
        $itv->{'boundary'} = $all_with_boundary[0][0]->{'lower_boundary'};
        $itv->{'boundary_scale'} = $all_with_boundary[0][1];
    }

    # Mark the best next interval and what not
    # The grep makes shure the boundary you use is consistent with whether
    # we're using the interval as a local or global interval now, mixing and matching is bad
    # See Zechstein - latest "next" is from a global scale but should be thrown out because
    # its now in a local Europe scale which maps it very differently
    my @all_with_next = sort scale_sort values %all_with_next;
    my @all_with_next_continent = grep {$_->[1]->{'continent'} eq $best_continent} @all_with_next;
    if (@all_with_next_continent) {
        $itv->{'next'} = $ig->{$all_with_next_continent[0][0]->{'next_interval_no'}};
        $itv->{'next_scale'} = $all_with_next_continent[0][1];
        # Set the best prev.  Multiple intervals may have the same next_interval_no
        # so set the prev to the "best" prev (the one with the newest correlation)
        if ($itv->{'next'} && $itv->{'next'}->{'prev'}) {
            # The next interval already has a 'prev' - have to compare scale pubyrs
            # to determine whether to overwrite
            my $other_itv = $itv->{'next'}->{'prev'};
            if ($itv->{'next_scale'}->{'pubyr'} > $other_itv->{'next_scale'}->{'pubyr'} ||
                 ($itv->{'next_scale'}->{'pubyr'} == $other_itv->{'next_scale'}->{'pubyr'} && 
                  $itv->{'next_scale'}->{'scale_no'} > $other_itv->{'next_scale'}->{'scale_no'})) {
#                print "WARNING: $itv->{next}->{interval_no} already has a prev $itv->{next}->{prev}->{interval_no}, overwriting with $itv->{interval_no}\n";
                $itv->{'next'}->{'prev'} = $itv;
            } else {
#                print "WARNING: $itv->{next}->{interval_no} already has a prev $itv->{next}->{prev}->{interval_no}, NOT overwriting with $itv->{interval_no}\n";
            }
        } else {
            $itv->{'next'}->{'prev'} = $itv;
        }
    }

    # We have to keep track of all next intervals - Store the interval
    # objects directly as an array 
    my @all_next = map {$ig->{$_->[0]->{'next_interval_no'}}} @all_with_next;
    my @all_next_scales = map {$_->[1]} @all_with_next;
    $itv->{'all_next'} = \@all_next;
    $itv->{'all_next_scales'} = \@all_next_scales;
    foreach my $next_itv (@all_next) {
        push @{$next_itv->{'all_prev'}},$itv;
    }

    # Mark the max and min - Note we want to use the best max from the same continent as
    # the best scale the interval is in, not mix and match.  We want to store every
    # continent the interval has been mapped in though, so the findSharedBoundaries routine
    # can work
    my @all_with_max = sort scale_sort values %all_with_max;
    my @all_with_max_continent = grep {$_->[1]->{'continent'} eq $best_continent} @all_with_max;
    if (@all_with_max_continent) {
        $itv->{'max'} = $ig->{$all_with_max_continent[0][0]->{'max_interval_no'}};
        $itv->{'max_scale'} = $all_with_max_continent[0][1];
        push @{$itv->{'max'}->{'children'}},$itv;
        delete $seen_parents{$itv->{'max'}->{'interval_no'}};

        my $min_no = $all_with_max_continent[0][0]->{'min_interval_no'};
        if ($min_no) {
            $itv->{'min'} = $ig->{$min_no};
            push @{$itv->{'min'}->{'children'}},$itv;
            delete $seen_parents{$itv->{'min'}->{'interval_no'}};
        } else {
            $itv->{'min'} = $itv->{'max'};
        }
    }
    my @all_max = map {$ig->{$_->[0]->{'max_interval_no'}}} @all_with_max;
    my @all_min = map {
        my $min_no = $_->[0]->{'min_interval_no'};
        my $max_no = $_->[0]->{'max_interval_no'};
        if ($min_no) {
            $ig->{$min_no}
        } else {
            $ig->{$max_no}
        }
    } @all_with_max;
    my @all_max_scales = map {$_->[1]} @all_with_max;
    $itv->{'all_max'} = \@all_max;
    $itv->{'all_min'} = \@all_min;
    $itv->{'all_max_scales'} = \@all_max_scales;
    $itv->{'all_min_scales'} = \@all_max_scales;

    # defunct are children no longer assinged into that interval
    foreach my $parent_no (keys %seen_parents) {
        my $parent = $ig->{$parent_no};
        push @{$parent->{'defunct'}},$itv;
    }
}

sub _dumpGraph {
    my $self = shift;
    my $ig = $self->getIntervalGraph;
    my $txt;
    foreach my $k (sort {$a<=>$b} keys %{$ig}) {
        $txt .= $self->_dumpInterval($ig->{$k})."\n";
    }
    return $txt;
}

sub _printConstraint {
    my $c = shift;
    my ($target,$action,$src,$depth,$last_from,$conflict) = @$c;
    my ($UPPER_MAX,$UPPER_EQ,$UPPER_MIN,$LOWER_MAX,$LOWER_EQ,$LOWER_MIN) = (1,2,3,4,5,6);

    my $action_txt = ($action == $UPPER_MAX) ? "upper max is" 
                   : ($action == $UPPER_EQ)  ? "upper bound is"
                   : ($action == $UPPER_MIN) ? "upper min is"
                   : ($action == $LOWER_MAX) ? "lower max is"
                   : ($action == $LOWER_EQ)  ? "lower bound is"
                   : ($action == $LOWER_MIN) ? "lower min is"
                   : " ?? unknown ??"; 
   
    my $constraint = ($conflict) ? "conflict: $conflict" : "constraint";
    return "  $constraint: $target->{interval_no}:$target->{name} $action_txt $src->{boundary} $src->{boundary_scale}->{pubyr}:$src->{boundary_scale}->{abbrev}:$src->{boundary_scale}->{scale_no} from $src->{interval_no}:$src->{name} percolated from $last_from->{interval_no}:$last_from->{name}\n";
}

sub _dumpInterval {
    my $self = shift;
    my $itv = shift;
    my $txt = "Interval $itv->{interval_no}:$itv->{name}\n";
    foreach ('prev','next','max','min') {
        if ($itv->{$_}) {
            $txt .= "  $_: $itv->{$_}->{interval_no}:$itv->{$_}->{name}\n";
        }
    }
    $txt .= "  range: $itv->{lower_boundary} - $itv->{upper_boundary}\n";
    foreach my $abbrev ('gl','As','Au','Eu','NZ','NA','SA') {
        my $lower_max = 'lower_max'.$abbrev;
        my $lower_boundary = 'lower_boundary'.$abbrev;
        my $lower_min = 'lower_min'.$abbrev;
        my $upper_max = 'upper_max'.$abbrev;
        my $upper_boundary = 'upper_boundary'.$abbrev;
        my $upper_min = 'upper_min'.$abbrev;
        if ($itv->{$lower_max} || $itv->{$lower_boundary} || $itv->{$lower_min} ||
            $itv->{$upper_max} || $itv->{$upper_boundary} || $itv->{$upper_min}) { 
            $txt .= "  $abbrev:lower:[$itv->{$lower_max}/$itv->{$lower_boundary}/$itv->{$lower_min}] - $abbrev:upper:[$itv->{$upper_max}/$itv->{$upper_boundary}/$itv->{$upper_min}]\n";
        }
    }
    foreach ('max_scale','best_scale','next_scale','boundary_scale') {
        if ($itv->{$_}) {
            $txt .= "  $_: $itv->{$_}->{scale_no},$itv->{$_}->{pubyr} $itv->{$_}->{continent} $itv->{$_}->{scale_rank}\n";
        }
    }
    foreach ('constraints','conflicts') {
        if ($itv->{$_}) {
            foreach my $c (@{$itv->{$_}}) {
                $txt .= _printConstraint($c);
            }
        }
    }
    foreach ('all_max_scales','all_next_scales') {
        if ($itv->{$_}) {
            $txt .= "  $_: ".join(", ",map {$_->{'scale_no'}.":".$_->{'continent'}.":".$_->{'pubyr'}} @{$itv->{$_}})."\n";
        }
    }
    foreach ('equiv','children','defunct','all_prev','all_next','all_max','all_min') {
        if ($itv->{$_}) {
            $txt .= "  $_: ".join(", ",map {$_->{'interval_no'}.":".$_->{'name'}} @{$itv->{$_}})."\n";
        }
    }
    foreach ('shared_lower','shared_upper') {
        if ($itv->{$_}) {
            $txt .= "  $_: ".join(", ",map {$_->{'interval_no'}.":".$_->{'name'}} @{$itv->{$_}})."\n";
        }
    }
    return $txt;
}


# This function will find all intervals that an interval maps into
sub getParentIntervals {
    my $self = shift;
    my $ig = $self->getIntervalGraph;

    my $itv = shift;
    my $input_type = '';
    if (ref $itv) {
        $input_type = 'objects';
    } else {
        $input_type = 'integers';
        $itv = $ig->{$itv};
    }

    my @intervals = ();
    my @q = ($itv);
    my %seen = ();
    while (my $itv = pop @q) {
        if ($itv->{'max'} && !$seen{$itv->{'max'}}) {
            $seen{$itv->{'max'}} = 1;
            push @q, $itv->{'max'};
            push @intervals, $itv->{'max'};
        }
        if ($itv->{'min'} && $itv->{'min'} != $itv->{'max'} && !$seen{$itv->{'min'}}) {
            $seen{$itv->{'min'}} = 1;
            push @q, $itv->{'min'};
            push @intervals, $itv->{'min'};
        }
    }
    if ($input_type eq 'integers') {
        return map {$_->{'interval_no'}} @intervals;
    } else {
        return @intervals;
    }
} 

# A trivial function PS 04/08/2005
sub getIntervalNo {
    my $self = shift;
    my $dbt = $self->{'dbt'};
    my $dbh = $dbt->dbh;

    my $eml = shift;
    my $name = shift;

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

# Utility function, parse input from form into valid eml+interval name pair, if possible
# Can be called directly or in obj oriented fashion, which is what the shift is for
sub splitInterval {
    shift if ref $_[0];
    my $interval_name = shift;

    my @terms = split(/ /,$interval_name);
    my @eml_terms;
    my @interval_terms;
    foreach my $term (@terms) {
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

# Returns an array of interval names in the correct order for a given scale
# With the newest interval first -- not finished yet, don't use
# PS 02/28/3004
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
        my $sql = "SELECT c.correlation_no, c.lower_boundary, c.interval_no, c.next_interval_no FROM correlations c".
                  " WHERE c.scale_no=".$dbt->dbh->quote($scale_no);
        @results = @{$dbt->getData($sql)};
    } else {
        my $sql = "SELECT c.correlation_no, c.lower_boundary, c.interval_no, c.next_interval_no, i.eml_interval, i.interval_name FROM correlations c, intervals i".
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
        $ints{$b}->{'lower_boundary'} <=> $ints{$a}->{'lower_boundary'} ||
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

sub lookupIntervals {
    my ($self,$intervals,$fields) = @_;
    my $dbt = $self->{'dbt'};
    
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
    my $self = shift;
    my $dbt = $self->{'dbt'};
    my $dbh = $dbt->dbh;

    my $period_lookup   = $self->getScaleMapping('69');
    my $epoch_lookup    = $self->getScaleMapping('71');
    my $subepoch_lookup = $self->getScaleMapping('72');
    my $stage_lookup    = $self->getScaleMapping('73');
    my $bin_lookup      = $self->getScaleMapping('bins');
    my ($ub_lookup,$lb_lookup) = $self->getBoundaries;
    my $ig = $self->getIntervalGraph;

    my $sql = "SELECT interval_no FROM intervals";
    my @intervals = map {$_->{'interval_no'}} @{$dbt->getData($sql)};
    foreach my $interval_no (@intervals) {
        my $period_no = $dbh->quote($period_lookup->{$interval_no});
        my $subepoch_no = $dbh->quote($subepoch_lookup->{$interval_no});
        my $epoch_no = $dbh->quote($epoch_lookup->{$interval_no});
        my $stage_no = $dbh->quote($stage_lookup->{$interval_no});
        my $ten_my_bin = $dbh->quote($bin_lookup->{$interval_no});
        my $ub = $dbh->quote($ub_lookup->{$interval_no});
        my $lb = $dbh->quote($lb_lookup->{$interval_no});
        my $itv = $ig->{$interval_no};
        my $interval_hash = $dbh->quote($self->serializeItv($itv));
        my $sql = "SELECT interval_no FROM interval_lookup WHERE interval_no=$interval_no";
        my @r = @{$dbt->getData($sql)};
        if ($r[0]) {
            my $sql = "UPDATE interval_lookup SET ten_my_bin=$ten_my_bin,stage_no=$stage_no,subepoch_no=$subepoch_no,epoch_no=$epoch_no,period_no=$period_no,lower_boundary=$lb,upper_boundary=$ub,interval_hash=$interval_hash WHERE interval_no=$interval_no";
#            print $sql,"\n";
            $dbh->do($sql);
        } else {
            my $sql = "INSERT INTO interval_lookup(interval_no,ten_my_bin,stage_no,subepoch_no,epoch_no,period_no,lower_boundary,upper_boundary,interval_hash) VALUES ($interval_no,$ten_my_bin,$stage_no,$subepoch_no,$epoch_no,$period_no,$lb,$ub,$interval_hash)";
#            print $sql,"\n";
            $dbh->do($sql);
        }
    }
}

# Serializes an itv object by turning the links into numbers (primary keys such
# as interval_no and scale_no).  To unserialize, just eval the text.
sub serializeItv {
    my ($self,$itv) = @_;
    my %new_hash = (
        'lower_boundary'=>$itv->{'lower_boundary'},
        'upper_boundary'=>$itv->{'upper_boundary'},
        'lower_estimate_type'=>$itv->{'lower_estimate_type'},
        'upper_estimate_type'=>$itv->{'upper_estimate_type'},
        'interval_no'=>$itv->{'interval_no'},
        'interval_name'=>$itv->{'name'}
    );

    foreach ('prev','next','max','min','lower_boundarysrc','upper_boundarysrc') {
        if ($itv->{$_}) {
            $new_hash{$_."_no"} = $itv->{$_}->{interval_no};
        }
    }
    foreach my $abbrev ('gl','As','Au','Eu','NZ','NA','SA') {
        my $lower_max = 'lower_max'.$abbrev;
        my $lower_boundary = 'lower_boundary'.$abbrev;
        my $lower_min = 'lower_min'.$abbrev;
        my $upper_max = 'upper_max'.$abbrev;
        my $upper_boundary = 'upper_boundary'.$abbrev;
        my $upper_min = 'upper_min'.$abbrev;
        if ($itv->{$lower_max} || $itv->{$lower_boundary} || $itv->{$lower_min} ||
            $itv->{$upper_max} || $itv->{$upper_boundary} || $itv->{$upper_min}) { 
#            $txt .= "  $abbrev:lower:[$itv->{$lower_max}/$itv->{$lower_boundary}/$itv->{$lower_min}] - $abbrev:upper:[$itv->{$upper_max}/$itv->{$upper_boundary}/$itv->{$upper_min}]\n";
        }
    }
    foreach ('max_scale','best_scale','next_scale','boundary_scale') {
        if ($itv->{$_}) {
            $new_hash{$_."_no"} = $itv->{$_}->{scale_no};
        }
    }
    foreach ('constraints','conflicts') {
        if ($itv->{$_}) {
            foreach my $c (@{$itv->{$_}}) {
#                $txt .= _printConstraint($c);
            }
        }
    }
    foreach ('all_max_scales','all_next_scales') {
        if ($itv->{$_}) {
            my @scale_nos = map {$_->{'scale_no'}} @{$itv->{$_}};
            $new_hash{$_."_nos"} = \@scale_nos;
        }
    }
    foreach ('equiv','children','defunct','all_prev','all_next','all_max','all_min','shared_lower','shared_upper') {
        if ($itv->{$_}) {
            my @interval_nos = map {$_->{'interval_no'}} @{$itv->{$_}};
            $new_hash{$_."_nos"} = \@interval_nos;
        }
    }
    local $Data::Dumper::Indent;
    local $Data::Dumper::Sortkeys;
    $Data::Dumper::Indent = 0;
    $Data::Dumper::Sortkeys = 1;
    return Dumper(\%new_hash);
}

sub deserializeItv {
	my $self = shift;
    my $itv_hash = shift;

	my $VAR1; # Prevent run time strict violation
	my $itv = eval $itv_hash;

    foreach ('max_scale','best_scale','next_scale','boundary_scale') {
		my $scale_no = $itv->{$_."_no"};
		if ($scale_no) {
        	$itv->{$_} = $self->getScale($scale_no);
		}
    }
    foreach ('all_max_scales','all_next_scales') {
        if ($itv->{$_."_nos"}) {
            for(my $i=0;$i<@{$itv->{$_."_nos"}};$i++) {
				my $scale_no = $itv->{$_."_nos"}->[$i];
        		$itv->{$_}->[$i] = $self->getScale($scale_no);
            }
        }
    }
    foreach ('prev','next','max','min','lower_boundarysrc','upper_boundarysrc') {
		my $interval_no = $itv->{$_."_no"};
        if ($interval_no) {
        	$itv->{$_} = $self->getInterval($interval_no);
        }
    }
    foreach ('equiv','children','defunct','all_prev','all_next','all_max','all_min','shared_lower','shared_upper') {
        if ($itv->{$_."_nos"}) {
            for(my $i=0;$i<@{$itv->{$_."_nos"}};$i++) {
				my $interval_no = $itv->{$_."_nos"}->[$i];
				$itv->{$_}->[$i] = $self->getInterval($interval_no);
			}
        }
    }
	return $itv;
}

sub getScale {
	my ($self,$s) = @_;
	my $dbt = $self->{'dbt'};
    my $sql = "SELECT s.created,s.scale_no,s.scale_name,s.continent,s.scale_rank,s.reference_no,r.pubyr,s.basis FROM scales s, refs r WHERE s.reference_no=r.reference_no AND s.scale_no=".int($s);
	return ${$dbt->getData($sql)}[0];
}

sub getInterval {
	my ($self,$i) = @_;
	my $dbt = $self->{'dbt'};
	my $sql = "SELECT interval_hash FROM interval_lookup WHERE interval_no=".int($i);
	my $hash = ${$dbt->getData($sql)}[0]->{interval_hash};
    my $VAR1;
    my $itv = eval $hash;
    return $itv;
}

sub printBoundary {
    shift if ref ($_[0]);
    my $bound = shift;
    return $bound if ($bound == 0);
    $bound =~ s/(0)+$//;
    $bound =~ s/\.$//;
    return $bound;
}

# Priority queue AKA binary heap - pop removes the element with the 
# smallest priority. Heap is implemented as a sorted array
package PriorityQueue;

sub new {
    my $c = shift;
    my $self = {'seen'=>{},'heap'=>[]};
    bless $self,$c;
}

# Called like: $queue->insert($hashref,10);
# If inserting an element that already exists the old one will
# first be removed to guarantee elements only appear once
sub insert {
    my ($self,$el,$priority) = @_;
    my $heap = $self->{'heap'};

    # Element already in the heap? remove it first
    if ($self->{'seen'}->{$el->{'interval_no'}}) {
        $self->remove($el);
    }

    my $min_idx = 0;
    my $max_idx = scalar(@$heap);

    while ($min_idx != $max_idx) {
        if ($max_idx - $min_idx == 1) {
            if ($heap->[$min_idx]->[0] <= $priority) {
                $min_idx = $max_idx;
            }
            last;
        }
        my $target = int(($max_idx + $min_idx)/2);
        if ($heap->[$target]->[0] <= $priority) {
            $min_idx = $target;
        } else {
            $max_idx = $target;
        }
    }
    $self->{'seen'}->{$el->{'interval_no'}} = 1;
    splice(@$heap,$min_idx,0,[$priority,$el]);
}

sub pop {
    my $self = shift;

    my $ref = shift @{$self->{'heap'}};
    if ($ref) {
        my $el = $ref->[1];
        delete $self->{'seen'}->{$el->{'interval_no'}};
        return $el;
    } else {
        return undef;
    }
}

# Called like: $queue->remove($hashref);
# Only used internally
sub remove {
    my ($self,$el) = @_;
    my $idx = 0; 
    while ($self->{'heap'}->[$idx]->[1]->{'interval_no'} != $el->{'interval_no'}) {
        $idx++;
        last if ($idx > @{$self->{'heap'}});
    }
    if ($idx < @{$self->{'heap'}}) {
        my $ref = splice(@{$self->{'heap'}},$idx,1);
        delete $self->{'seen'}->{$el->{'interval_no'}};
        return $ref->[1];
    }
}

return 1;

