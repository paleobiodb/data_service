package PrintHierarchy;

use TaxonInfo;
use Constants qw($READ_URL $WRITE_URL $HTML_DIR $PAGE_TOP $PAGE_BOTTOM $TAXA_TREE_CACHE);
use strict;

sub classificationForm	{
	my $hbo = shift;
	my $s = shift;
	my $error = shift;
	my $ref_list = shift;
	my %refno;
	$refno{'current_ref'} = $s->get('reference_no');
	if ( $s->get('enterer_no') > 0 )	{
		$refno{'not_guest'} = 1;
	}
	if ( $error )	{
		$refno{'error_message'} = "<p style=\"margin-top: -0.2em; margin-bottom: 1em;\"><i>\n".$error;
		$refno{'error_message'} .= ( ! $ref_list ) ? "<br>Please search again</i></p>" : "</i></p>\n";
		$refno{'ref_list'} = $ref_list;
	}
	print $hbo->populateHTML('classify_form',\%refno);
}

# JA 27.2.12
# complete rewrite of most of this module
sub classify	{
	my ($dbt,$hbo,$s,$q) = @_;

	my %shortranks = ( "subspecies" => "","species" => "",
	 "subgenus" => "Subg.", "genus" => "G.", "subtribe" => "Subtr.",
	 "tribe" => "Tr.", "subfamily" => "Subfm.", "family" => "Fm.",
	 "superfamily" => "Superfm.", "infraorder" => "Infraor.",
	 "suborder" => "Subor.", "order" => "Or.", "superorder" => "Superor.",
	 "infraclass" => "Infracl.", "subclass" => "Subcl.", "class" => "Cl.",
	 "superclass" => "Supercl.", "subphylum" => "Subph.",
	 "phylum" => "Ph.");

	my $taxon_no = $q->param('taxon_no');
	my $reference_no = $q->param('reference_no');
	if ( $q->param('parent_no') )	{
		$taxon_no = $q->param('parent_no');
	}
	# if something like "Jones 1984" was submitted, find the matching
	#  reference with the most opinions
	# assume they are not looking for junior authors
	if ( $q->param('citation') )	{
		my ($auth,$year) = split / /,$q->param('citation');
		if ( $year < 1700 || $year > 2100 )	{
			print $hbo->stdIncludes($PAGE_TOP);
			classificationForm($hbo, $s, 'The publication year is misformatted');
			print $hbo->stdIncludes($PAGE_BOTTOM);
			exit;
		}
		my $sql = "SELECT reference_no,author1last,author2last,otherauthors,pubyr,reftitle,pubtitle,pubvol,firstpage,lastpage FROM refs WHERE author1last='$auth' AND pubyr=$year ORDER BY author1last DESC,author2last DESC,pubyr DESC";
		my @refs = @{$dbt->getData($sql)};
		if ( $#refs == -1 )	{
		} elsif ( $#refs == 0 )	{
			$reference_no = $refs[0]->{'reference_no'};
		} else	{
			print $hbo->stdIncludes($PAGE_TOP);
			my @ref_list;
			push @ref_list , "<p class=\"verysmall\" style=\"margin-left: 2em; margin-right: 0.5em; text-indent: -1em; text-align: left; margin-bottom: -0.8em;\">".Reference::formatLongRef($_)." (ref ".$_->{'reference_no'}.")</p>\n" foreach @refs;
			classificationForm($hbo, $s, 'The following matches were found',join('',@ref_list)."<div style=\"height: 1em;\"></div>");
			print $hbo->stdIncludes($PAGE_BOTTOM);
			exit;
		}
	}
	my $fields = "t.taxon_no,taxon_name,taxon_rank,common_name,extant,status,IF (ref_is_authority='YES',r.author1last,a.author1last) author1last,IF (ref_is_authority='YES',r.author2last,a.author2last) author2last,IF (ref_is_authority='YES',r.otherauthors,a.otherauthors) otherauthors,IF (ref_is_authority='YES',r.pubyr,a.pubyr) pubyr,lft,rgt";
	my (@taxa,@parents,%children,$title);

	# references require special handling because they may classify
	#  multiple taxa and because parent-child relations are drawn directly
	#  from opinions instead of taxa_tree_cache
	if ( ! $taxon_no && $reference_no )	{
		my $sql = "SELECT child_spelling_no,parent_spelling_no FROM opinions WHERE reference_no=".$reference_no." AND ref_has_opinion='YES'";
		my @opinions = @{$dbt->getData($sql)};
		if ( ! @opinions )	{
			print $hbo->stdIncludes($PAGE_TOP);
			classificationForm($hbo, $s, 'No taxonomic opinions are tied to this reference');
			print $hbo->stdIncludes($PAGE_BOTTOM);
			exit;
		}
		my %in;
		$in{$_->{'child_spelling_no'}}++ foreach @opinions;
		$in{$_->{'parent_spelling_no'}}++ foreach @opinions;
		$sql = "SELECT $fields FROM authorities a,$TAXA_TREE_CACHE t,opinions o,refs r WHERE a.taxon_no=t.taxon_no AND t.opinion_no=o.opinion_no AND a.reference_no=r.reference_no AND t.taxon_no IN (".join(',',keys %in).")";
		@taxa = @{$dbt->getData($sql)};
		# some parents may be completely unclassified
		my $non_opinion_fields = $fields;
		$non_opinion_fields =~ s/,status//;
		$sql = "SELECT $non_opinion_fields FROM authorities a,$TAXA_TREE_CACHE t,refs r WHERE a.taxon_no=t.taxon_no AND t.opinion_no=0 AND a.reference_no=r.reference_no AND t.taxon_no IN (".join(',',keys %in).")";
		push @taxa , @{$dbt->getData($sql)};
		my %parent;
		$parent{$_->{'child_spelling_no'}} = $_->{'parent_spelling_no'} foreach @opinions;
		for my $i ( 0..$#taxa )	{
			push @{$children{$parent{$taxa[$i]->{'taxon_no'}}}} , $taxa[$i];
			if ( ! $parent{$taxa[$i]->{'taxon_no'}} )	{
				push @parents , $taxa[$i];
			}
		}
		$sql = "SELECT * FROM refs WHERE reference_no=".$reference_no;
		$title = TaxonInfo::formatShortAuthor( ${$dbt->getData($sql)}[0] );
	# try to get a taxon_no for a common name
	} elsif ( ! $taxon_no && $q->param('common_name') )	{
		my $common = $q->param('common_name');
		my $sql = "SELECT a.taxon_no FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND common_name='".$common."' ORDER BY rgt-lft DESC LIMIT 1";
		$taxon_no = ${$dbt->getData($sql)}[0]->{'taxon_no'};
		if ( ! $taxon_no && $common =~ /s$/ )	{
			$common =~ s/s$//;
			$sql = "SELECT a.taxon_no FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND common_name='".$common."' ORDER BY rgt-lft DESC LIMIT 1";
			$taxon_no = ${$dbt->getData($sql)}[0]->{'taxon_no'};
		}
	# ditto for a standard taxonomic name
	} elsif ( ! $taxon_no && $q->param('taxon_name') )	{
		my $sql = "SELECT a.taxon_no FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND taxon_name='".$q->param('taxon_name')."' ORDER BY rgt-lft DESC LIMIT 1";
		$taxon_no = ${$dbt->getData($sql)}[0]->{'taxon_no'};
	}

	if ( ! $taxon_no && ! @taxa )	{
		if ( ! $q->param('boxes_only') )	{
			print $hbo->stdIncludes($PAGE_TOP);
			classificationForm($hbo, $s, 'Nothing matched the search term');
			print $hbo->stdIncludes($PAGE_BOTTOM);
		}
		exit;
	}

	# grab all children of the parent taxon
	if ( $taxon_no )	{
		my $sql = "SELECT lft,rgt FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND a.taxon_no=".$taxon_no;
		my $range = ${$dbt->getData($sql)}[0];
		if ( $range->{'lft'} + 1 == $range->{'rgt'} )	{
			if ( ! $q->param('boxes_only') )	{
				print $hbo->stdIncludes($PAGE_TOP);
				classificationForm($hbo, $s, 'Nothing is classified within this taxon');
				print $hbo->stdIncludes($PAGE_BOTTOM);
			}
			exit;
		}

		$sql = "SELECT $fields FROM authorities a,$TAXA_TREE_CACHE t,opinions o,refs r WHERE a.taxon_no=t.taxon_no AND t.opinion_no=o.opinion_no AND a.reference_no=r.reference_no AND t.taxon_no=t.spelling_no AND lft>=".$range->{'lft'}." AND rgt<=".$range->{'rgt'}." ORDER BY lft";
		@taxa = @{$dbt->getData($sql)};
		$title = "the ".$taxa[0]->{'taxon_rank'}." ".TaxonInfo::italicize( $taxa[0] );
		$title =~ s/unranked //;

		push @parents , $taxa[0];
		for my $i ( 1..$#taxa )	{
			my $isChild = 0;
			for my $pre ( reverse 0..$i-1 )	{
				if ( $taxa[$pre]->{'rgt'} > $taxa[$i]->{'rgt'} )	{
					push @{$children{$taxa[$pre]->{'taxon_no'}}} , $taxa[$i];
					$isChild++;
					last;
				}
			}
			if ( $isChild == 0 )	{
				push @parents , $taxa[$i];
			}
		}
	}

	# put valid and invalid children in separate arrays
	my (%valids,%invalids);
	for my $t ( @taxa )	{
		for my $c ( @{$children{$t->{'taxon_no'}}} )	{
			if ( $c->{'status'} =~ /belongs/ && ( $c->{'lft'} + 1 < $c->{'rgt'} || $c->{'taxon_rank'} =~ /species|genus/ ) )	{
				$c->{'status'} =~ s/belongs to/valid/;
				push @{$valids{$t->{'taxon_no'}}} , $c;
			} else	{
				$c->{'status'} =~ s/ (of|by)//;
				$c->{'status'} =~ s/subjective //;
				$c->{'status'} =~ s/belongs to/empty/;
				push @{$invalids{$t->{'taxon_no'}}} , $c;
			}
		}
		if ( $valids{$t->{'taxon_no'}} )	{
			@{$valids{$t->{'taxon_no'}}} = sort { $a->{'taxon_name'} cmp $b->{'taxon_name'} } @{$valids{$t->{'taxon_no'}}};
		}
	}

	if ( ! $q->param('boxes_only') )	{
		print $hbo->stdIncludes($PAGE_TOP);
		chmod 0664, "$HTML_DIR/public/classification/classification.csv";
		open OUT, ">$HTML_DIR/public/classification/classification.csv";
		print OUT "taxon_rank,taxon_name,author,common_name,status,extant\n";
	}
	print $hbo->populateHTML('js_classification');
	if ( ! $q->param('boxes_only') )	{
		print "<center><p class=\"pageTitle\">Classification of $title</p></center>\n\n";
	}
	print "<div class=\"verysmall\" style=\"width: 50em; margin-left: auto; margin-right: auto;\">\n\n";

	# don't display every name, only the top-level ones
	my $shownDepth = 2;
	if ( $#taxa < 10 )	{
		$shownDepth = 999999;
	} elsif ( $#taxa < 30 )	{
		$shownDepth = 3;
	}
	for my $p ( @parents )	{
		printBox( $p , '' , 1 );
	}

	print "\n</div>\n\n";

	# recursively print boxes including taxon names and subtaxa
	sub printBox	{
		my $t = shift;
		my $parent_no = shift;
		my $depth = shift;
		my @nos;
		push @nos , $_->{'taxon_no'} foreach @{$valids{$t->{'taxon_no'}}};
		my $list = join(',',@nos);
		if ( $invalids{$t->{'taxon_no'}} )	{
			$list .= ",".$t->{'taxon_no'}."bad";
		}
		$list =~ s/^,//;
		my $extant = ( $t->{'extant'} !~ /y/i ) ? "no" : "yes";
		print OUT "$t->{'taxon_rank'},\"$t->{'taxon_name'}\",\"".TaxonInfo::formatShortAuthor($t)."\",\"$t->{'common_name'}\",\"$t->{'status'}\",$extant\n";
		$extant = ( $t->{'extant'} !~ /y/i ) ? "&dagger;" : "";
		my $name = $shortranks{$t->{'taxon_rank'}}." "."$extant<a href=\"$READ_URL?action=basicTaxonInfo&amp;taxon_no=$t->{taxon_no}\">".TaxonInfo::italicize($t)."</a>";
		if ( $t->{'author1last'} )	{
			$name .= " ".TaxonInfo::formatShortAuthor($t);
		}
		if ( $t->{'common_name'} )	{
			$name .= " [".$t->{'common_name'}."]";
		}
		my $class = ( $depth <= $shownDepth ) ? 'classBox' : 'hiddenClassBox';
		my $style = ( $depth == 1 ) ? ' style="border-left: 0px; margin-bottom: 0.8em; "' : '';
		print qq|  <div id="t$t->{taxon_no}" class="$class"$style>
|;
		my $firstMargin = ( $depth <= $shownDepth ) ? "0em" : "0em";
		if ( $list )	{
			print qq|    <div id="n$t->{taxon_no}" class="classTaxon" style="margin-bottom: $firstMargin;" onMouseOver="showChildren('$t->{taxon_no}','$list');">|;
		} else	{
			print qq|    <div id="n$t->{taxon_no}" class="classTaxon">|;
		}
		print "$name</div>\n";
		if ( $depth < $shownDepth && $list )	{
			print qq|    <div id="hot$t->{taxon_no}" class="classHotCorner" style="font-size: 0.7em;"><span onMouseOver="hideChildren('$t->{taxon_no}','$list');">hide</span></div>
|;
		} elsif ( $list )	{
			print qq|    <div id="hot$t->{taxon_no}" class="classHotCorner"><span onMouseOver="showChildren('$t->{taxon_no}','$list');">+</span></div>
|;
		}
		printBox( $_ , $t->{'taxon_no'} , $depth + 1 ) foreach @{$valids{$t->{'taxon_no'}}};
		if ( $invalids{$t->{'taxon_no'}} )	{
			my $class = ( $depth + 1 <= $shownDepth ) ? 'classBox' : 'hiddenClassBox';
			print qq|  <div id="t$t->{'taxon_no'}bad" class="$class">
|;
			@{$invalids{$t->{'taxon_no'}}} = sort { $a->{'taxon_name'} cmp $b->{'taxon_name'} } @{$invalids{$t->{'taxon_no'}}};
			for my $t ( @{$invalids{$t->{'taxon_no'}}} )	{
				my $extant = ( $t->{'extant'} !~ /y/i ) ? "no" : "yes";
				print OUT "$t->{'taxon_rank'},\"$t->{'taxon_name'}\",\"".TaxonInfo::formatShortAuthor($t)."\",\"$t->{'common_name'}\",\"$t->{'status'}\",$extant\n";
			}
			my @badList;
			push @badList , TaxonInfo::italicize($_)." ".TaxonInfo::formatShortAuthor($_)." [".$_->{'status'}."]" foreach @{$invalids{$t->{'taxon_no'}}};
			my $marginTop = ( $list ) ? "0.5em" : "0.5em;";
			print "Invalid names: ".join(', ',@badList)."</div>\n";
		}
		print "  </div>\n";
		return;
	}

	if ( ! $q->param('boxes_only') )	{
		print qq|<form method="POST" action="$READ_URL" name="doDownloadTaxonomy">
<input type="hidden" name="action" value="displayDownloadTaxonomyResults">
|;
 		if ( $taxon_no ) {
			print qq|<input type="hidden" name="taxon_no" value="$taxon_no">|;
		}
		if ( $reference_no ) {
			print qq|<input type="hidden" name="reference_no" value="$reference_no">|;
		}
		print "</form>\n"; 
		print '<center><p class="tiny">';
		print '<a href="/public/classification/classification.csv">Download</a></b> this list of taxonomic names';
		print ' - <a href=# onClick="javascript: document.doDownloadTaxonomy.submit()">Download</a> authority and opinion data for these taxa';
		print " - <a href=\"$READ_URL?action=classify\">See another classification</a></p></center>";
		print $hbo->stdIncludes($PAGE_BOTTOM);
		close OUT;
	}
	return $#taxa + 1;
}


1;
