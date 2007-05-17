package SanityCheck;

use strict;

# 15.5.07 JA

sub processSanityCheck	{
	my ($q,$dbt,$hbo,$s) = @_;

	my $sql;
	my %plural = ('order' => 'orders', 'family' => 'families', 'genus' => 'genera');
	my @ranks = ('order', 'family', 'genus');
	my @grade = ('F','F','F','D','D','C','C','B','B','A','A');

	$sql = "SELECT lft,rgt,taxon_rank rank FROM authorities a,taxa_tree_cache t WHERE a.taxon_no=t.taxon_no AND taxon_name='" . $q->param('taxon_name') ."'";
	my $row = @{$dbt->getData($sql)}[0];
	my $lft = $row->{lft};
	my $rgt = $row->{rgt};
	my $error_message;
	if ( ! $lft )	{
		$error_message = "<p><i>\"" . $q->param('taxon_name') . "\" is not in the Database. Please try again.</i></p>";
	} elsif ( $lft + 1 == $rgt )	{
		$error_message = "<p><i>" . $q->param('taxon_name') . " does not include any subtaxa. Please try again.</i></p>";
	} elsif ( $row->{rank} =~ /genus|species/ )	{
		$error_message = "<p><i>" . $q->param('taxon_name') . " is not a higher taxon. Please try again.</i></p>";
	}
	if ( $error_message )	{
		my $vars = {'error_message' => $error_message};
		main::displaySanityForm($vars);
		return;
	}

	printf "<center><h3 style=\"margin-bottom: 2em;\">Progress report: %s</h3></center>\n\n",$q->param('taxon_name');

	# author and year known
	$sql = "SELECT taxon_rank rank,count(*) c FROM authorities a,taxa_tree_cache t WHERE a.taxon_no=t.taxon_no AND lft>$lft AND rgt<$rgt AND t.taxon_no=spelling_no AND t.taxon_no=synonym_no AND taxon_rank IN ('genus','family','order') AND (ref_is_authority='YES' OR (author1last IS NOT NULL AND author1last!='')) GROUP BY taxon_rank";
	my @rows = @{$dbt->getData($sql)};
	my %authorknown;
	for my $r ( @rows )	{
		$authorknown{$r->{rank}} = $r->{c};
	}

	# author and year not known
	$sql = "SELECT taxon_rank rank,count(*) c FROM authorities a,taxa_tree_cache t WHERE a.taxon_no=t.taxon_no AND lft>$lft AND rgt<$rgt AND t.taxon_no=spelling_no AND t.taxon_no=synonym_no AND taxon_rank IN ('genus','family','order') AND (ref_is_authority!='YES' AND (author1last IS NULL OR author1last='')) GROUP BY taxon_rank";
	my @rows2 = @{$dbt->getData($sql)};
	my %authorunknown;
	for my $r ( @rows2 )	{
		$authorunknown{$r->{rank}} = $r->{c};
	}
	my %total;
	my $authortext;
	for my $rank ( @ranks )	{
		if ( $authorknown{$rank} + $authorunknown{$rank} > 1 )	{
			$total{$rank} = $authorknown{$rank} + $authorunknown{$rank};
			$authortext .= sprintf "%d of %d ",$authorknown{$rank}, $authorknown{$rank} + $authorunknown{$rank};
			$authortext .= sprintf " (%.1f%%)<br>\n",100 * $authorknown{$rank} / $total{$rank};
		}
	}


	# joining taxa_list_cache and occurrences is incredibly slow, so
	#  here's a two-step workaround
	# the key tool is a matrix where the row index is the tree cache
	#   primary key, and 0 and 1 columns are the left and right of the
	#   genus spanning this position, if there is one
	# this will fail if for some reason valid genera overlap
	$sql = "SELECT lft,rgt,taxon_rank rank,extant FROM authorities a,taxa_tree_cache t WHERE a.taxon_no=t.taxon_no and lft>$lft AND rgt<$rgt AND t.taxon_no=synonym_no AND taxon_rank IN ('genus','family','order') GROUP BY t.taxon_no";
	my @rows = @{$dbt->getData($sql)};
	my %LR;
	my %extant;
	for my $row ( @rows )	{
		for my $i ( $row->{lft}..$row->{rgt} )	{
			$LR{$row->{rank}}[$i][0] = $row->{lft};
			$LR{$row->{rank}}[$i][1] = $row->{rgt};
		}
		$extant{$row->{rank}}{$row->{extant}}++;
	}
	$sql = "SELECT lft,rgt FROM occurrences o,taxa_tree_cache t WHERE o.taxon_no=t.taxon_no AND lft>$lft AND rgt<$rgt GROUP BY t.taxon_no";
	my @rows = @{$dbt->getData($sql)};
	my %sampled;
	for my $row ( @rows )	{
		for my $rank ( @ranks )	{
			if ( $LR{$rank}[$row->{lft}][0] > 0 && $LR{$rank}[$row->{lft}][1] > 0 )	{
				if ( $LR{$rank}[$row->{lft}][0] == $LR{$rank}[$row->{rgt}][0] && $LR{$rank}[$row->{lft}][1] == $LR{$rank}[$row->{rgt}][1] )	{
					$sampled{$rank}{$LR{$rank}[$row->{lft}][0]." ".$LR{$rank}[$row->{lft}][1]}++;
				}
			}
		}
	}
	my %withoccs;
	for my $rank ( @ranks )	{
		my @temp = keys %{$sampled{$rank}};
		$withoccs{$rank} = $#temp + 1;
	}

	printBoxTop("Valid subtaxa with occurrences");
	for my $rank ( @ranks ) 	{
		if ( $total{$rank} > 1 )	{
			printf "%d of %d $plural{$rank} (%.1f%%)<br>\n",$withoccs{$rank}, $total{$rank}, 100 * $withoccs{$rank} / $total{$rank};
		}
	}
	print "<p>\n";
	printf "<b>Our grade: %s</b><br>",$grade[int(10 * $withoccs{'genus'} / $total{'genus'})];
	printBoxBottom();

	printBoxTop("Valid subtaxa marked as extant or extinct");
	my %unknownExtant;
	for my $rank ( @ranks ) 	{
		if ( $total{$rank} > 1 )	{
			$unknownExtant{$rank} = ( $total{$rank} - $extant{$rank}{'YES'} - $extant{$rank}{'NO'} );
			printf "$plural{$rank}: %d extant, %d extinct, %d unknown (%.1f/%.1f/%.1f%%)<br>\n",$extant{$rank}{'YES'}, $extant{$rank}{'NO'}, $unknownExtant{$rank}, 100 * $extant{$rank}{'YES'} / $total{$rank}, 100 * $extant{$rank}{'NO'} / $total{$rank}, 100 * $unknownExtant{$rank} / $total{$rank};
		}
	}
	print "<p>\n";
	printf "<b>Our grade: %s</b><br>",$grade[int(10 * ( $total{'genus'} - $unknownExtant{'genus'} ) / $total{'genus'})];
	printBoxBottom();

	printBoxTop("Valid subtaxa with author and year data");
	print "$authortext<p>\n";
	printf "<b>Our grade: %s</b><br>",$grade[int(10 * $authorknown{'genus'} / $total{'genus'})];
	printBoxBottom();

	# the only opinion comes from the Compendium, Carroll, or McKenna/Bell
	# this is tricky because the NAFMSD data uploaded on 23.1.02 overlapped
	#  with Carroll, so we have to assume that names published before 1988
	#  actually are in Carroll
	$sql = "SELECT taxon_rank rank,child_no no FROM refs r,opinions o,authorities a,taxa_tree_cache t WHERE r.reference_no=a.reference_no AND child_no=a.taxon_no AND a.taxon_no=t.taxon_no AND lft>$lft AND rgt<$rgt AND t.taxon_no=synonym_no AND taxon_rank IN ('genus','family','order') AND (o.reference_no IN (6930,4783,7584) OR (a.created<20030124000000 AND ((a.pubyr<1988 AND a.pubyr>1700) OR (r.pubyr<1988 AND r.pubyr>1700)))) GROUP BY child_no";
	@rows = @{$dbt->getData($sql)};
	my %compendium;
	my %uncompendium;
	if ( $#rows > -1 )	{
		my $in_list = $rows[0]->{no};
		$compendium{$rows[0]->{rank}}++;
		for my $i ( 1..$#rows )	{
			$in_list .= "," . $rows[$i]->{no};
			$compendium{$rows[$i]->{rank}}++;
		}
		$sql = "SELECT taxon_rank rank,child_no no FROM opinions o,authorities a WHERE child_no=a.taxon_no AND child_no IN ($in_list) AND o.reference_no NOT IN (6930,4783,7584) GROUP BY child_no";
		@rows = @{$dbt->getData($sql)};
		for my $i ( 0..$#rows )	{
			$uncompendium{$rows[$i]->{rank}}++;
		}
	}

	printBoxTop("Subtaxa with opinions from both a compilation and a primary source");
	my $incompilation = 0;
	for my $rank ( @ranks ) 	{
		if ( $compendium{$rank} > 1 )	{
			printf "%d of %d $plural{$rank} (%.1f%%)<br>\n",$uncompendium{$rank}, $compendium{$rank}, 100 * $uncompendium{$rank} / $compendium{$rank};
			$incompilation++;
		}
	}
	if ( $incompilation == 0 )	{
		print "We have no opinions about this group at all from compilations<br>\n";
	}
	print "<p>\n";
	if ( $incompilation == 0 )	{
		printf "<b>Our grade: A</b><br>\n";
	} else	{
		printf "<b>Our grade: %s</b><br>\n",$grade[int(10 * $uncompendium{'genus'} / $compendium{'genus'})];
	}
	printBoxBottom();

	printBoxTop("Subtaxa not even recorded in a compilation");
	for my $rank ( @ranks ) 	{
		if ( $compendium{$rank} > 1 )	{
			printf "%d of %d $plural{$rank} (%.1f%%)<br>\n",$total{$rank} - $compendium{$rank}, $total{$rank}, 100 * ( $total{$rank} - $compendium{$rank} ) / $total{$rank};
		}
	}
	if ( $incompilation == 0 )	{
		print "The compilations don't record this group at all<br>\n";
	}
	print "<p>\n";
	printf "<b>Their grade: %s</b><br>",$grade[int(10 * $compendium{'genus'} / $total{'genus'})];
	printBoxBottom();

	print qq|<form method="POST" action="bridge.pl">
<input type="hidden" name="action" value="displaySanityForm">
<center><p>Next taxon to check: <input type="text" name="taxon_name" value=""  size="35"></p></center>
</form>
|;

	return;
}

sub printBoxTop	{
	my $headline = shift;

	print qq|<div class="displayPanel" align=left>
  <span class="displayPanelHeader"><b>$headline</b></span>
  <div class="displayPanelContent small">
  <p>
|;

	return;
}

sub printBoxBottom	{

	print qq|  </p>
  </div>
</div>
|;

	return;
}

1;
