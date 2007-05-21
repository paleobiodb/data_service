package SanityCheck;

use strict;

# 15.5.07 JA

sub processSanityCheck	{
	my ($q,$dbt,$hbo,$s) = @_;

	my $sql;
	my %plural = ('order' => 'orders', 'family' => 'families', 'genus' => 'genera');
	my @ranks = ('order', 'family', 'genus');
	my @grades = ('F','F','F','D','D','C','C','B','B','A','A');
	my $grade;
	my %dataneeded;
	my %dataneeded2;

	my $lft;
	my $rgt;
	my $lftrgt2 = "AND (lft+1<rgt OR taxon_rank='genus')";
	my $error_message;
	if ( ! $q->param('taxon_name') )	{
		$error_message = "You must enter a taxon name.";
	} else	{
		$sql = "SELECT lft,rgt,taxon_rank rank FROM authorities a,taxa_tree_cache t WHERE a.taxon_no=t.taxon_no AND taxon_name='" . $q->param('taxon_name') ."'";
		my $row = @{$dbt->getData($sql)}[0];
		$lft = $row->{lft};
		$rgt = $row->{rgt};
		if ( ! $lft )	{
			$error_message = "\"" . $q->param('taxon_name') . "\" is not in the Database.";
		} elsif ( $lft + 1 == $rgt )	{
			$error_message = $q->param('taxon_name') . " does not include any subtaxa.";
		} elsif ( $row->{rank} =~ /genus|species/ )	{
			$error_message = $q->param('taxon_name') . " is not a higher taxon.";
		}
		if ( $q->param('excluded_taxon') )	{
			$sql = "SELECT lft,rgt,taxon_rank rank FROM authorities a,taxa_tree_cache t WHERE a.taxon_no=t.taxon_no AND taxon_name='" . $q->param('excluded_taxon') ."'";
			$row = @{$dbt->getData($sql)}[0];
			$lftrgt2 .= " AND (lft<" . $row->{lft} . " OR rgt>" . $row->{rgt} . ")";
			if ( ! $row->{lft} )	{
				$error_message .= " \"" . $q->param('excluded_taxon') . "\" is not in the Database.";
			} elsif ( $row->{lft} + 1 == $row->{rgt} )	{
				$error_message .= " " . $q->param('excluded_taxon') . " does not include any subtaxa.";
			} elsif ( $row->{rank} =~ /genus|species/ )	{
				$error_message .= " " . $q->param('excluded_taxon') . " is not a higher taxon.";
			}
		}
	}
	if ( $error_message )	{
		$error_message = "<p><i>" . $error_message . " Please try again.</i></p>\n";
		my $vars = {'error_message' => $error_message};
		main::displaySanityForm($vars);
		return;
	}

	printf "<center><h3 style=\"margin-bottom: 2em;\">Progress report: %s",$q->param('taxon_name');
	if ( $q->param('excluded_taxon') )	{
		printf " minus %s",$q->param('excluded_taxon');
	}
	printf "</h3></center>\n\n",$q->param('taxon_name');

	# author and year known
	$sql = "SELECT taxon_rank rank,count(*) c FROM authorities a,taxa_tree_cache t WHERE a.taxon_no=t.taxon_no AND lft>$lft AND rgt<$rgt $lftrgt2 AND t.taxon_no=spelling_no AND t.taxon_no=synonym_no AND taxon_rank IN ('genus','family','order') AND (ref_is_authority='YES' OR (author1last IS NOT NULL AND author1last!='')) GROUP BY taxon_rank";
	my @rows = @{$dbt->getData($sql)};
	my %authorknown;
	for my $r ( @rows )	{
		$authorknown{$r->{rank}} = $r->{c};
	}

	# author and year not known - don't group, we need the names
	$sql = "SELECT taxon_name name,taxon_rank rank,lft,rgt FROM authorities a,taxa_tree_cache t WHERE a.taxon_no=t.taxon_no AND lft>$lft AND rgt<$rgt $lftrgt2 AND t.taxon_no=spelling_no AND t.taxon_no=synonym_no AND taxon_rank IN ('genus','family','order') AND (ref_is_authority!='YES' AND (author1last IS NULL OR author1last=''))";
	my @rows2 = @{$dbt->getData($sql)};
	my %authorunknown;
	for my $r ( @rows2 )	{
		$authorunknown{$r->{rank}}++;
		$dataneeded{$r->{rank}}{$r->{name}}++;
	}
	my %total;
	my $authortext;
	for my $rank ( @ranks )	{
		if ( $authorknown{$rank} + $authorunknown{$rank} > 0 )	{
			$total{$rank} = $authorknown{$rank} + $authorunknown{$rank};
			$authortext .= sprintf "%d of %d $plural{$rank} ",$authorknown{$rank}, $authorknown{$rank} + $authorunknown{$rank};
			$authortext .= sprintf " (%.1f%%)<br>\n",100 * $authorknown{$rank} / $total{$rank};
			if ( $authorunknown{$rank} > 0 && $authorunknown{$rank} <= 200 )	{
				$authortext .= "<span class=\"small\">[missing data: ";
				my @temp = keys %{$dataneeded{$rank}};
				@temp = sort @temp;
				$authortext .= $temp[0];
				for my $i ( 1..$#temp )	{
					$authortext .= ", " . $temp[$i];
				}
				$authortext .= "]</span><br>\n";
			}
			if ( $rank ne "genus" )	{
				$authortext .= "<br>\n";
			}
		}
	}


	# joining taxa_list_cache and occurrences is incredibly slow, so
	#  here's a two-step workaround
	# the key tool is a matrix where the row index is the tree cache
	#   primary key, and 0 and 1 columns are the left and right of the
	#   genus spanning this position, if there is one
	# this will fail if for some reason valid genera overlap
	$sql = "SELECT lft,rgt,taxon_rank rank,taxon_name name,extant FROM authorities a,taxa_tree_cache t WHERE a.taxon_no=t.taxon_no and lft>$lft AND rgt<$rgt $lftrgt2 AND t.taxon_no=synonym_no AND taxon_rank IN ('genus','family','order') GROUP BY t.taxon_no";
	my @rows = @{$dbt->getData($sql)};
	my %LR;
	my %extant;
	%dataneeded = ();
	for my $row ( @rows )	{
		for my $i ( $row->{lft}..$row->{rgt} )	{
			$LR{$row->{rank}}[$i][0] = $row->{lft};
			$LR{$row->{rank}}[$i][1] = $row->{rgt};
			$LR{$row->{rank}}[$i][2] = $row->{name};
			$dataneeded{$row->{rank}}{$row->{name}}++;
		}
		$extant{$row->{rank}}{$row->{extant}}++;
		if ( $row->{extant} !~ /yes|no/i )	{
			$dataneeded2{$row->{rank}}{$row->{name}}++;
		}
	}
	$sql = "SELECT lft,rgt FROM occurrences o,taxa_tree_cache t WHERE o.taxon_no=t.taxon_no AND lft>$lft AND rgt<$rgt GROUP BY t.taxon_no";
	my @rows = @{$dbt->getData($sql)};
	$sql = "SELECT lft,rgt FROM reidentifications r,taxa_tree_cache t WHERE r.taxon_no=t.taxon_no AND lft>$lft AND rgt<$rgt GROUP BY t.taxon_no";
	push @rows , @{$dbt->getData($sql)};
	my %sampled;
	for my $row ( @rows )	{
		for my $rank ( @ranks )	{
			if ( $LR{$rank}[$row->{lft}][0] > 0 && $LR{$rank}[$row->{lft}][1] > 0 )	{
				if ( $LR{$rank}[$row->{lft}][0] == $LR{$rank}[$row->{rgt}][0] && $LR{$rank}[$row->{lft}][1] == $LR{$rank}[$row->{rgt}][1] )	{
					$sampled{$rank}{$LR{$rank}[$row->{lft}][0]." ".$LR{$rank}[$row->{lft}][1]}++;
					delete $dataneeded{$rank}{$LR{$rank}[$row->{lft}][2]};
				}
			}
		}
	}
	my %withoccs;
	for my $rank ( @ranks )	{
		my @temp = keys %{$sampled{$rank}};
		$withoccs{$rank} = $#temp + 1;
	}

	print "<p class=\"small\" style=\"margin-left: 2em; margin-right: 2em;\">Minimum percentages needed to earn grades: D > 30, C > 50, B > 70, A > 90. Percentages are based on data for genera.</p><br>\n\n";

	printBoxTop("Valid subtaxa with occurrences");
	for my $rank ( @ranks ) 	{
		if ( $total{$rank} > 0 )	{
			printf "%d of %d $plural{$rank} (%.1f%%)<br>\n",$withoccs{$rank}, $total{$rank}, 100 * $withoccs{$rank} / $total{$rank};
			if ( $total{$rank} - $withoccs{$rank} > 0 && $total{$rank} - $withoccs{$rank} <= 200 )	{
				my @temp = keys %{$dataneeded{$rank}};
				printMissing($rank,\@temp);
			}
			if ( $rank ne "genus" )	{
				print "<br>\n";
			}
		}
	}
	$grade = $grades[int(10 * $withoccs{'genus'} / $total{'genus'})];
	printBoxBottom("Our",$grade);

	printBoxTop("Valid subtaxa marked as extant or extinct");
	my %unknownExtant;
	for my $rank ( @ranks ) 	{
		if ( $total{$rank} > 0 )	{
			$unknownExtant{$rank} = ( $total{$rank} - $extant{$rank}{'YES'} - $extant{$rank}{'NO'} );
			printf "$plural{$rank}: %d extant, %d extinct, %d unknown (%.1f/%.1f/%.1f%%)<br>\n",$extant{$rank}{'YES'}, $extant{$rank}{'NO'}, $unknownExtant{$rank}, 100 * $extant{$rank}{'YES'} / $total{$rank}, 100 * $extant{$rank}{'NO'} / $total{$rank}, 100 * $unknownExtant{$rank} / $total{$rank};
			if ( $unknownExtant{$rank} > 0 && $unknownExtant{$rank} <= 200 )	{
				my @temp = keys %{$dataneeded2{$rank}};
				printMissing($rank,\@temp);
			}
			if ( $rank ne "genus" )	{
				print "<br>\n";
			}
		}
	}
	$grade = $grades[int(10 * ( $total{'genus'} - $unknownExtant{'genus'} ) / $total{'genus'})];
	printBoxBottom("Our",$grade);

	printBoxTop("Valid subtaxa with author and year data");
	print "$authortext";
	$grade = $grades[int(10 * $authorknown{'genus'} / $total{'genus'})];
	printBoxBottom("Our",$grade);

	# the only opinion comes from the Compendium, Carroll, or McKenna/Bell
	# this is tricky because the NAFMSD data uploaded on 23.1.02 overlapped
	#  with Carroll, so we have to assume that names published before 1988
	#  actually are in Carroll
	$sql = "SELECT taxon_rank rank,taxon_name name,child_no no FROM refs r,opinions o,authorities a,taxa_tree_cache t WHERE r.reference_no=a.reference_no AND child_no=a.taxon_no AND a.taxon_no=t.taxon_no AND lft>$lft AND rgt<$rgt $lftrgt2 AND t.taxon_no=synonym_no AND taxon_rank IN ('genus','family','order') AND (o.reference_no IN (6930,4783,7584) OR (a.created<20030124000000 AND ((a.pubyr<1988 AND a.pubyr>1700) OR (r.pubyr<1988 AND r.pubyr>1700)))) GROUP BY child_no";
	@rows = @{$dbt->getData($sql)};
	my %compendium;
	my %uncompendium;
	%dataneeded = ();
	if ( $#rows > -1 )	{
		my $in_list = $rows[0]->{no};
		$compendium{$rows[0]->{rank}}++;
		$dataneeded{$rows[0]->{rank}}{$rows[0]->{name}}++;
		for my $i ( 1..$#rows )	{
			$in_list .= "," . $rows[$i]->{no};
			$compendium{$rows[$i]->{rank}}++;
			$dataneeded{$rows[$i]->{rank}}{$rows[$i]->{name}}++;
		}
		$sql = "SELECT taxon_rank rank,taxon_name name,child_no no FROM opinions o,authorities a WHERE child_no=a.taxon_no AND child_no IN ($in_list) AND o.reference_no NOT IN (6930,4783,7584) GROUP BY child_no";
		@rows = @{$dbt->getData($sql)};
		for my $i ( 0..$#rows )	{
			$uncompendium{$rows[$i]->{rank}}++;
			delete $dataneeded{$rows[$i]->{rank}}{$rows[$i]->{name}};
		}
	}

	printBoxTop("Subtaxa in compilations also having opinions from a primary source");
	my $incompilation = 0;
	for my $rank ( @ranks ) 	{
		if ( $compendium{$rank} > 0 )	{
			printf "%d of %d $plural{$rank} (%.1f%%)<br>\n",$uncompendium{$rank}, $compendium{$rank}, 100 * $uncompendium{$rank} / $compendium{$rank};
			$incompilation++;
			if ( $compendium{$rank} - $uncompendium{$rank} > 0 && $compendium{$rank} - $uncompendium{$rank} <= 200 )	{
				my @temp = keys %{$dataneeded{$rank}};
				printMissing($rank,\@temp);
			}
			if ( $rank ne "genus" )	{
				print "<br>\n";
			}
		}
	}
	if ( $incompilation == 0 )	{
		print "We have no opinions about this group at all from compilations<br>\n";
	}
	if ( $incompilation == 0 )	{
		$grade = "A";
	} else	{
		$grade = $grades[int(10 * $uncompendium{'genus'} / $compendium{'genus'})];
	}
	printBoxBottom("Our",$grade);

	printBoxTop("Subtaxa in the Database not recorded in a compilation");
	print "<i>Our system includes tetrapod names from Carroll (1988) and McKenna and Bell (1997), and marine animal names from Sepkoski (2002).</i></p>\n\n";
	print "<p style=\"padding-left: 1em;\">\n";
	for my $rank ( @ranks ) 	{
		if ( $total{$rank} > 0 )	{
			printf "%d of %d $plural{$rank} (%.1f%%)<br>\n",$total{$rank} - $compendium{$rank}, $total{$rank}, 100 * ( $total{$rank} - $compendium{$rank} ) / $total{$rank};
			if ( $rank ne "genus" )	{
				print "<br>\n";
			}
		}
	}
	if ( $incompilation == 0 )	{
		print "The compilations don't record this group at all<br>\n";
	}
	my $grade = $grades[int(10 * $compendium{'genus'} / $total{'genus'})];
	printBoxBottom("Their",$grade);

	print qq|<form method="POST" action="bridge.pl">
<input type="hidden" name="action" value="startProcessSanityCheck">
<center><p>Next taxon to check: <input type="text" name="taxon_name" value="" size="16"> excluding <input type="text" name="excluded_taxon" value="" size="16"> <input type="submit" value="check"></p></center>
</form>
|;

	return;
}

sub printBoxTop	{
	my $headline = shift;

	print qq|<div class="displayPanel" align="left" style="margin-left: 1em; margin-right: 1em;">
  <span class="displayPanelHeader"><b>$headline</b></span>
  <div class="displayPanelContent small">
  <p style="padding-left: 1em;">
|;

	return;
}

sub printBoxBottom	{
	my $usthem = shift;
	my $grade = shift;

	print "  <p style=\"padding-left: 1em;\">\n";
	printf "<b>$usthem grade: $grade</b><br>";

	print qq|  </p>
  </div>
</div>
|;

	return;
}

sub printMissing	{
	my $rank = shift;
	my $tempref = shift;
	my @temp = @{$tempref};

	print "<span class=\"small\">[missing data: ";
	@temp = sort @temp;
	print $temp[0];
	for my $i ( 1..$#temp )	{
		print ", " . $temp[$i];
	}
	print "]</span><br>\n";
}

1;