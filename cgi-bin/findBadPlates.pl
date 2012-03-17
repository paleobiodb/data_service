# 16.3.12

open IN,"<./data/master01c.rot";
while (<IN>)	{
	@d = split /,/,$_;
	$exists{$d[1]}++;
}
close IN;

open IN,"<./data/plateidsv2.lst";
$_ = <IN>;
while (<IN>)	{
	s/\n//;
	($x,$y,$p) = split /,/,$_;
	if ( ! $exists{$p} )	{
		$unrotated{$p}++;
	}
	$at{$x}{$y} = $p;
}
close IN;

for $x ( -180..180 )	{
	for $y ( -90..90 )	{
		my $p = $at{$x}{$y};
		$cont = int( $p / 100 );
		if ( $unrotated{$p} )	{
			if ( $at{$x-1}{$y} != $p && int( $at{$x-1}{$y} / 100 ) == $cont && ! $unrotated{$at{$x-1}{$y}} )	{
				$next{$p}{$at{$x-1}{$y}}++;
			}
			if ( $at{$x}{$y-1} != $p && int( $at{$x}{$y-1} / 100 ) == $cont && ! $unrotated{$at{$x}{$y-1}} )	{
				$next{$p}{$at{$x}{$y-1}}++;
			}
			if ( $at{$x+1}{$y} != $p && int( $at{$x+1}{$y} / 100 ) == $cont && ! $unrotated{$at{$x+1}{$y}} )	{
				$next{$p}{$at{$x+1}{$y}}++;
			}
			if ( $at{$x}{$y+1} != $p && int( $at{$x}{$y+1} / 100 ) == $cont && ! $unrotated{$at{$x}{$y+1}} )	{
				$next{$p}{$at{$x}{$y+1}}++;
			}

			if ( $at{$x-1}{$y} != $p && int( $at{$x-1}{$y} / 100 ) == $cont && $unrotated{$at{$x-1}{$y}} )	{
				$badNext{$p}{$at{$x-1}{$y}}++;
			}
			if ( $at{$x}{$y-1} != $p && int( $at{$x}{$y-1} / 100 ) == $cont && $unrotated{$at{$x}{$y-1}} )	{
				$badNext{$p}{$at{$x}{$y-1}}++;
			}
			if ( $at{$x+1}{$y} != $p && int( $at{$x+1}{$y} / 100 ) == $cont && $unrotated{$at{$x+1}{$y}} )	{
				$badNext{$p}{$at{$x+1}{$y}}++;
			}
			if ( $at{$x}{$y+1} != $p && int( $at{$x}{$y+1} / 100 ) == $cont && $unrotated{$at{$x}{$y+1}} )	{
				$badNext{$p}{$at{$x}{$y+1}}++;
			}
		}
	}
}

@uns = keys %unrotated;
@uns = sort { $a <=> $b } @uns;

for my $p ( @uns )	{
	if ( $p == 0 )	{
		next;
	}
	@nexts = keys %{$next{$p}};
	@nexts = sort { $next{$p}{$b} <=> $next{$p}{$a} } @nexts;
	$nextTo{$p} = $nexts[0];
	$timesNextTo{$p} = $next{$p}{$nexts[0]};
}

for my $p ( @uns )	{
	if ( $p == 0 )	{
		next;
	}
	if ( ! $nextTo{$p} ) 	{
		print "BAD $p\n";
		@nexts = keys %{$badNext{$p}};
		@nexts = sort { $badNext{$p}{$b} <=> $badNext{$p}{$a} } @nexts;
		for $b ( @nexts )	{
			if ( $nextTo{$b} > 0 )	{
				$nextTo{$p} = $nextTo{$b};
				last;
			}
		}
	}
}

open OUT,">./data/bad_plate_neighbors";
print OUT "plate\tneighbor\ttimes adjacent\n";
for my $p ( @uns )	{
	if ( $p == 0 )	{
		next;
	}
	print OUT "$p\t$nextTo{$p}\t$timesNextTo{$p}\n";
}
close OUT;




