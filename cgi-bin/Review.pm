package Review;

use Constants qw($READ_URL $HTML_DIR $TAXA_TREE_CACHE);

use Debug;
use Reference;

use strict;

# 17-18.1.10 JA

sub displayReviewForm	{
	my ($dbt,$q,$s,$hbo) = @_;

	# enterers may not create pages (sorry)
	if ( $s->get('enterer') ne $s->get('authorizer') )	{
		print "<div align=\"center\">".Debug::printWarnings("Only authorizers can edit review pages. Many apologies.")."</div>";
		return;
	}

	my %vars;
	my $publish_link = qq|Publish page by making it viewable on the public web site? <select id="release" name="release"><option>no</option><option>yes</option></select>|;

	# these fields should exist only on the way back from a preview
	if ( $q->param('title') =~ /[A-Za-z]/ || $q->param('text') =~ /[A-Za-z]/ )	{
		%vars = $q->Vars();
		$vars{'add_edit'} = "Editing";
		if ( $q->param('release') =~ /y|n/i || $q->param('review_no') == 0 )	{
			$vars{'released'} = $publish_link;
		} else	{
			my $sql = "SELECT released FROM reviews WHERE review_no=".$q->param('review_no');
			my $review = ${$dbt->getData($sql)}[0];
			$vars{'released'} = "<i>This page was published on ".$review->{'released'}."</i>";
		}
		print $hbo->populateHTML("review_form", \%vars);
		return;
	}

	# if this is a second trip to the function, display the review
	if ( $q->param('review_no') > 0 )	{
		my $sql = "SELECT * FROM reviews r,versions v WHERE r.review_no=v.review_no AND latest='Y' AND author_no=".$s->get('enterer_no')." AND r.review_no=".$q->param('review_no');
		my $review = ${$dbt->getData($sql)}[0];

		# paranoia check (authors shouldn't be able to select a review
		#  unless they own it)
		if ( ! $review )	{
			print "<div align=\"center\">".Debug::printWarnings("You don't own this review page, so you can't edit it. Nothing personal.")."</div>";
			return;
		}

		$vars{$_} = $review->{$_} foreach ('review_no','personage','interval_no','region','taxon_no','title','text');

		# grab taxon name
		if ( $vars{'taxon_no'} > 0 )	{
			$sql = "SELECT taxon_name FROM authorities WHERE taxon_no=".$vars{'taxon_no'};
			$vars{'taxon'} = ${$dbt->getData($sql)}[0]->{'taxon_name'};
		}

		# grab interval name
		if ( $vars{'interval_no'} > 0 )	{
			$sql = "SELECT interval_name FROM intervals WHERE interval_no=".$vars{'interval_no'};
			$vars{'interval'} = ${$dbt->getData($sql)}[0]->{'interval_name'};
		}

		$vars{'add_edit'} = "Editing";
		if ( $review->{'released'} )	{
			$vars{'released'} = "<i>This page was published on ".$review->{'released'}."</i>";
		} else	{
			$vars{'released'} = $publish_link;
		}
		print $hbo->populateHTML("review_form", \%vars);
		return;
	}
	# second trip, but the user wants to create a new page
	elsif ( $q->param('review_no') == - 1)	{
		$vars{'add_edit'} = "Entry";
		$vars{'released'} = $publish_link;
		print $hbo->populateHTML("review_form", \%vars);
		return;
	}

	my $sql = "SELECT r.review_no,official_no,title FROM reviews r,versions v WHERE r.review_no=v.review_no AND author_no=".$s->get('enterer_no')." AND latest='Y'";
	my @reviews = @{$dbt->getData($sql)};

	# if the author has no pages, display the form immediately
	if ( ! @reviews )	{
		$vars{'add_edit'} = "Entry";
		$vars{'released'} = $publish_link;
		print $hbo->populateHTML("review_form", \%vars);
		return;
	}

	# otherwise show choices
	else	{
	# this is incredibly lame, but inheritance of width=100% from div
	#  surrounding whole page forces explicit width specification
		my $max;
		for my $r ( @reviews )	{
			if ( length($r->{'title'}) > $max )	{
				$max = length($r->{'title'});
			}
		}
		my $width = sprintf("%.1fem",$max*0.55+10);
		print "<center><p class=\"pageTitle\">Please select a review page to edit</p></center>\n\n";
		print qq|
<form name="chooseReview" method=post action="bridge.pl">
<input type="hidden" name="a" value="displayReviewForm">
<input type="hidden" name="review_no" value="">
</form>

<div class="displayPanel" style="width: $width; margin-left: auto; margin-right: auto;">
<div class="displayPanelContent" style="margin-left: 1em;">
|;

		for my $r ( @reviews )	{
			print "<p style=\"text-indent: -2em; margin-left: 2em;\">&bull; <a href=# onClick=\"javascript: document.chooseReview.review_no.value='".$r->{'review_no'}."'; document.chooseReview.submit();\">$r->{'title'}</a>";
			if ( $r->{'official_no'} > 0 )	{
				print " (PaleoDB Review #$r->{'official_no'})";
			}
			print "</p>\n";
		}
		print "<p>&bull; <a href=# onClick=\"javascript: document.chooseReview.review_no.value='-1'; document.chooseReview.submit();\"><i>Create a new review page</i></a>";
		print "</p>\n";

		print "\n</div>\n</div>\n</div>\n\n";
	}

	return;

}


sub processReviewForm	{
	my ($dbt,$q,$s,$hbo) = @_;
	my $dbh = $dbt->dbh;

	my %vars = $q->Vars();

	my ($sql,@sets);

	my ($taxon_no,$error);
	if ( $vars{'taxon'} )	{
	# assumes you mean the biggest one if there are homonyms
		$sql = "SELECT a.taxon_no FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND taxon_name='".$vars{'taxon'}."' ORDER BY rgt-lft DESC LIMIT 1";
		$taxon_no = ${$dbt->getData($sql)}[0]->{'taxon_no'};
	# error if taxon not found
	 	if ( $taxon_no == 0 )	{
			$error = "WARNING: the taxon name '".$vars{'taxon'}."' doesn't exist in the database!";
		}
		push @sets , "taxon_no=$taxon_no";
	}

	if ( $q->param('preview') =~ /y/i )	{
		showReview($dbt,$q,$s,$hbo,$error);
		return;
	}

	# need formatting checks on title, personage, and taxon
	# also interval must not have spaces etc.

	$vars{'title'} =~ s/'/\\'/g;
	$vars{'personage'} =~ s/'/\\'/g;
	$vars{'text'} =~ s/'/\\'/g;

	my $interval_no;
	if ( $vars{'interval'} )	{
		$sql = "SELECT interval_no FROM intervals WHERE interval_name='".$vars{'interval'}."'";
		$interval_no = ${$dbt->getData($sql)}[0]->{'interval_no'};
		push @sets , "interval_no=$interval_no";
	}

	my $sql;
	if ( $vars{'review_no'} > 0 )	{
		if ( $vars{'personage'} )	{
			push @sets , "personage='".$vars{'personage'}."'";
		}
		if ( $vars{'region'} )	{
			push @sets , "region='".$vars{'region'}."'";
		}
		$sql = "UPDATE reviews SET ".join(',',@sets)." WHERE review_no=$vars{'review_no'}";
		$dbh->do($sql);
	} else	{
		if ( ! $taxon_no )	{
			$taxon_no = "NULL";
		}
		$sql = "INSERT INTO reviews (author_no,personage,interval_no,region,taxon_no) VALUES (".$s->get('enterer_no').",'".$vars{'personage'}."',$interval_no,'".$vars{'region'}."',$taxon_no)";
		$dbh->do($sql);
		$sql = "SELECT max(review_no) r FROM reviews WHERE author_no=".$s->get('enterer_no');
		$vars{'review_no'} = ${$dbt->getData($sql)}[0]->{'r'};
		$q->param('review_no' => $vars{'review_no'});
		$sql = "UPDATE reviews SET modified=modified,created=modified WHERE review_no=".$vars{'review_no'};
		$dbh->do($sql);
	}
	$sql = "INSERT INTO versions (review_no,title,text,latest) VALUES ($vars{'review_no'},'".$vars{'title'}."','".$vars{'text'}."','Y')";
	$dbh->do($sql);

	$sql = "SELECT max(version_no) v FROM versions WHERE review_no=".$vars{'review_no'};
	my $version = ${$dbt->getData($sql)}[0]->{'v'};
	$sql = "UPDATE versions SET modified=modified,created=modified WHERE version_no=".$version;
	$dbh->do($sql);
	$sql = "UPDATE versions SET modified=modified,latest=NULL WHERE version_no!=".$version." AND review_no=".$vars{'review_no'};
	$dbh->do($sql);

	# fix official PaleoDB Review number
	if ( $vars{'release'} =~ /yes/i )	{
		$sql = "SELECT official_no FROM reviews WHERE official_no IS NOT NULL AND review_no=".$vars{'review_no'};
		my @already = @{$dbt->getData($sql)};
	# sanity check, should never happen
		if ( @already )	{
			$error = "WARNING: this review page has been released already!";
		} else	{
			$sql = "SELECT max(official_no) AS no FROM reviews";
			my $no = ${$dbt->getData($sql)}[0]->{'no'};
			$no++;
			$sql = "UPDATE reviews SET modified=modified,released=modified,official_no=$no WHERE review_no=".$vars{'review_no'};
			$dbh->do($sql);
		}
	}

	showReview($dbt,$q,$s,$hbo,$error);

}


sub listReviews	{
	my ($dbt,$q,$s,$hbo) = @_;

	my $sql = "SELECT first_name,last_name,institution,r.review_no,official_no,title FROM person,reviews r,versions v WHERE person_no=author_no AND r.review_no=v.review_no AND latest='Y' AND (author_no=".$s->get('enterer_no')." OR (released IS NOT NULL AND released<now()))";
	if ( $s->get('enterer_no') == 0 )	{
		$sql .= " AND released>0";
	}
	$sql .= " ORDER BY official_no";
	my @reviews = @{$dbt->getData($sql)};

	print "<center><p class=\"pageTitle\" style=\"margin-top: 2em;\">Paleobiology Database Reviews</p></center>\n\n";

	print qq|
<form name="chooseReview" method=post action="bridge.pl">
<input type="hidden" name="a" value="showReview">
<input type="hidden" name="review_no" value="">
</form>
|;

	print "<div class=\"displayPanel\" style=\"width: 40em; margin-left: auto; margin-right: auto;\">\n\n";
	print "<div class=\"displayPanelContent\">\n";
	for my $r ( @reviews )	{
		print "<p style=\"text-indent: -2em; margin-left: 2em;\">&bull; $r->{'first_name'} $r->{'last_name'}, <a href=# onClick=\"javascript: document.chooseReview.review_no.value='".$r->{'review_no'}."'; document.chooseReview.submit();\"><i>$r->{'title'}</i></a>";
		if ( $r->{'official_no'} > 0 )	{
			print " (Paleobiology Database Review #".$r->{'official_no'}.")";
		}
		print "</p>\n";
	}
	print "\n</div>\n</div>\n</div>\n\n";

}


sub showReview	{
	my ($dbt,$q,$s,$hbo,$error) = @_;

	my %keywords;
	my @keyword_vars = ('personage','interval','region','taxon');

	# supply an edit button if the enterer is the author

	my ($goal,$sql,$author,$institution,$title,$text,$no);
	# title and text will be passed in if this is a preview or submission
	#  results page
	if ( $q->param('title') =~ /[A-Za-z]/ && $q->param('text') =~ /[A-Za-z]/ )	{
		$sql = "SELECT first_name,last_name,institution FROM person WHERE person_no=".$s->get('enterer_no');
		my $person = ${$dbt->getData($sql)}[0];
		$author = $person->{'first_name'}." ".$person->{'last_name'};
		$institution = $person->{'institution'};
		$title = $q->param('title');
		$text = $q->param('text');
		$no = $q->param('official_no');

		# anything that might have quotes needs to be escaped because
		#  everything goes in hidden inputs
		my $escaped = $title;
		$escaped =~ s/"/&quot;/g;
		$q->param('title' => $escaped);
		my $escaped = $text;
		$escaped =~ s/"/&quot;/g;
		$q->param('text' => $escaped);

		print "<form method=post name=redisplayForm>\n";
		$q->param('action' => 'displayReviewForm');
		my $was_preview;
		if ( $q->param('preview') =~ /y/i )	{
			$goal = "preview";
			$q->param('preview' => '');
			$was_preview++;
		} else	{
			$goal = "submit";
		}
		my @params = $q->param;
		for my $p ( @params )	{
			print "<input type=hidden name=$p value=\"".$q->param($p)."\">\n";
		}
		print "</form>\n\n";

		for my $k ( @keyword_vars )	{
			$keywords{$k} = $q->param($k);
		}

		if ( $was_preview > 0 )	{
			print "<center><p class=\"large\" style=\"margin-top: 2em; margin-bottom: -1em;\"><i>Here is your page preview. <a href=# onClick=\"javascript: document.redisplayForm.submit();\">Click here</a> to go back.</i></p></center>\n\n";
		}
		# should be a submission results page
		else	{
			print "<center><p class=\"large\" style=\"margin-top: 2em; margin-bottom: -1em;\"><i>Your page was saved and looks like this. <a href=# onClick=\"javascript: document.redisplayForm.submit();\">Click here</a> to go back.</i></p></center>\n\n";
		}
		if ( $error )	{
			print "<center><p class=\"large\" style=\"margin-top: 2em; margin-bottom: -1em;\"><i>$error</i></p></center>\n\n";
		}
	}
	# otherwise retrieve everything from the database
	else	{
		$goal = "view";
		$sql = "SELECT first_name,last_name,institution,r.*,v.* FROM person p,reviews r,versions v WHERE person_no=author_no AND r.review_no=v.review_no AND latest='Y' AND r.review_no=".$q->param('review_no');
		my $review = ${$dbt->getData($sql)}[0];
		$author = $review->{'first_name'}." ".$review->{'last_name'};
		$institution = $review->{'institution'};
		$title = $review->{'title'};
		$text = $review->{'text'};
		$no = $review->{'official_no'};
		if ( $review->{'interval_no'} > 0 )	{
			$sql = "SELECT interval_name FROM intervals WHERE interval_no=".$review->{'interval_no'};
			$review->{'interval'} = ${$dbt->getData($sql)}[0]->{'interval_name'};
		}
		if ( $review->{'taxon_no'} > 0 )	{
			$sql = "SELECT taxon_name FROM authorities WHERE taxon_no=".$review->{'taxon_no'};
			$review->{'taxon'} = ${$dbt->getData($sql)}[0]->{'taxon_name'};
		}
		for my $k ( @keyword_vars )	{
			$keywords{$k} = $review->{$k};
		}
	}

	print "<center><p class=\"pageTitle\" style=\"margin-top: 2em; margin-bottom: 0em;\">$title</p>\n\n";
	if ( $no )	{
		print "<p class=\"large\" style=\"margin-top: 0.5em; margin-bottom: 0em;\">Paleobiology Database Review #$no</p>\n";
	}
	print "<p>$author, $institution</p></center>\n\n<br>";

	my @words;
	for my $k ( @keyword_vars )	{
		if ( $keywords{$k} && $title !~ /$keywords{$k}/ )	{
			push @words , $keywords{$k};
		}
	}
	if ( $#words == 0 )	{
		print "<center><p class=\"small\" style=\"margin-top: -2em;\">Keyword: $words[0]</p></center>\n\n";
	} elsif ( $#words > 0 )	{
		@words = sort @words;
		print "<center><p class=\"small\" style=\"margin-top: -2em;\">Keywords: ".join(', ',@words)."</p></center>\n\n";
	}

	my $panelStart = qq|
<div class="displayPanel large" style="margin-right: 2em; padding-bottom: 1em;">
<span class="displayPanelHeader">
|;
	my $panelEnd = qq|
</span>
<div class="displayPanelContent small" style="padding-left: 2em;">
|;

	# clean up newlines and insert <p> tags
	# this weirdness is necessary because the database apparently
	#  sticks in some ugly whitespace after newlines
	$text =~ s/\n\s/\n/mg;
	$text =~ s/\s\n/\n/mg;
	$text =~ s/\n\n\n\n/\n\n\n/mg;
	$text =~ s/\n\n\n\n/\n\n\n/mg;
	# key <p> insert line
	$text =~ s/([A-Za-z0-9:\.\!>])(\n\n\n)([A-Za-z0-9<])/$1<\/p>\n\n<p>$3/mg;
	# fix for text before an isolated link
	# WARNING: we assume that normal paragraphs don't begin with links
	$text =~ s/([A-Za-z0-9:\.\!>])(\n{2,3})(\[\[)/$1<\/p>$2$3/mg;
	# fix for text after an isolated link
	$text =~ s/(\]\])(\n{2,3})([A-Za-z0-9<])/$1$2<p>$3/mg;
	# fix for section dividers
	$text =~ s/(=\n\n*)([A-Za-z0-9\.\!>])/$1<p>$2/gm;
	$text =~ s/([A-Za-z0-9\.\!>])(\n\n*=)/$1<\/p>$2/gm;

	# fix subsections first
	$text =~ s/\n(==)(.*)(==)/\n<center><p class="medium">$2<\/p><\/center>/g;
	# major sections are now good to go
	# different handling for first major section
	$text =~ s/^\n//g;
	$text =~ s/(^)(=)(.*)(=)/$panelStart$3$panelEnd/g;
	$text =~ s/(\n)(=)(.*)(=)/\n<\/div>\n<\/div>\n\n$panelStart$3$panelEnd/g;

	# mark up bold and ital assuming the nice user is not playing with
	#  our minds by putting close tags where they shouldn't go
	$text =~ s/(\'\'\')([ \.,:;])/<\/b>$2/g;
	$text =~ s/\'\'\'/<b>/g;
	$text =~ s/(\'\')([ \.,:;])/<\/i>$2/g;
	$text =~ s/\'\'/<i>/g;

	# map parsing
	my @mapsections = split /\[\[Map:/,$text;
	# first "map" is just text
	my $newtext = shift @mapsections;
	for my $i ( 0..$#mapsections )	{
		my ($map,$rest) = split/\]\]/,$mapsections[$i],2;

		my ($alt,$caption,$leftright,$width,$tagref) = parseTags($map);
		my @tags = @{$tagref};

		my ($maplink,$isthere);
		$maplink = "/public/reviews/".join('_',@tags).".png";
		$maplink =~ s/ /_/g;

		# try to find the image if it doesn't need to be finalized
		if ( $goal ne "submit" )	{
			$isthere = `ls $HTML_DIR$maplink`;
			if ( $isthere !~ /[A-Za-z]\.[a-z]/ )	{
				makeMap($dbt,$q,$s,$hbo,join('_',@tags));
				$isthere = `ls $HTML_DIR$maplink`;
			}
		} else	{
			makeMap($dbt,$q,$s,$hbo,join('_',@tags));
			$isthere = `ls $HTML_DIR$maplink`;
		}
		if ( $isthere =~ /[A-Za-z]\.[a-z]/ )	{
			($newtext,$rest) = insertImage([$newtext,$rest,$leftright,$width,$alt,$maplink,$caption]);
		}
		$newtext .= $rest;

	}
	$text = $newtext;

	# image parsing
	my @isections = split /\[\[Image:/,$text;
	my $newtext = shift @isections;
	for my $i ( 0..$#isections )	{
		my ($image,$rest) = split/\]\]/,$isections[$i],2;

		my ($alt,$caption,$leftright,$width,$tagref) = parseTags($image);
		my @tags = @{$tagref};

		my $link .= `ls $HTML_DIR/public/upload_images/*/$tags[0].*`;
		$link =~ s/$HTML_DIR//;
		($newtext,$rest) = insertImage([$newtext,$rest,$leftright,$width,$alt,$link,$caption]);
		$newtext .= $rest;
	}
	$text = $newtext;

	# mark up external links
	while ( $text =~ /(\[\[http:\/\/)([A-Za-z\.\/]*)( )([^\[]*)(\]\])/ )	{
		$text =~ s/(\[\[http:\/\/)([A-Za-z\.\/]*)( )([^\[]*)(\]\])/<a href="http:\/\/$2">$4<\/a>/;
	}

	# anchors not dealt with yet

	# special handling for properly formatted collection links
	$text =~ s/\[\[collection /<a href="$READ_URL\?a=basicCollectionSearch&amp;collection_no=/g;
	# special handling for properly formatted taxon links
	$text =~ s/\[\[taxon /<a href="$READ_URL\?a=basicTaxonInfo&amp;taxon_name=/g;
	# give up and try a basic search
	$text =~ s/\[\[/<a href="$READ_URL\?a=quickSearch&amp;quick_search=/g;

	$text =~ s/\]\]/<\/a>/g;
	$text =~ s/\|/">/g;

	# reference parsing
	my ($nref,@refs);
	my @bits = split /<ref>/,$text;
	my $newtext = shift @bits;
	for my $b ( @bits )	{
		my ($ref,$rest) = split /<\/ref>/,$b;
	# the user may have entered a PaleoDB ref number
		if ( $ref =~ /^[0-9]*$/ )	{
			$sql = "SELECT * FROM refs WHERE reference_no=$ref";
			my $refref = ${$dbt->getData($sql)}[0];
			$ref = Reference::formatLongRef($refref);
		}
		$nref++;
		push @refs , "<a name=\"ref$nref\"></a>".$nref.". ".$ref;
		$newtext .= "<a href=\"#ref$nref\"><sup>[".$nref."]</sup></a>".$rest;
	}
	$text = $newtext;

	my $span = '<span style="text-indent: 2em; margin-left: -2em;">';
	my $reflist = $span.join("</span><br>$span",@refs)."</span>";
	$reflist = '<div style="margin-left: 2em;">'.$reflist.'</div>';
	$text =~ s/\{\{reflist\}\}/$reflist/;

	print $text;

	# end the last major section
	print "\n</div>\</div>\n\n";

	print "<center><b><a href=\"$READ_URL?a=listReviews\">See more PaleoDB review pages</a></b></center>\n\n";

}


sub parseTags	{
	my $link = shift;
	my @tags = split /\|/,$link;

	for my $i ( 0..$#tags )	{
		$tags[$i] =~ s/^ //;
		$tags[$i] =~ s/ $//;
	}

	my ($alt,$caption);
	$caption = pop @tags;
	if ( $tags[$#tags] =~ /^alt=/ )	{
		$alt = pop @tags;
		$alt =~ s/^alt=//;
	}

	# left, right, and number-px are reserved but go anywhere
	my $leftright = "right";
	my $width = "300";
	my @cleantags;
	for my $t ( @tags )	{
		if ( $t eq "left" || $t eq "right" )	{
			$leftright = $t;
		} elsif ( $t =~ /^[0-9]*px$/ )	{
			$width = $t;
			$width =~ s/px$//;
		} else 	{
			push @cleantags , $t;
		}
	}
	@tags = @cleantags;

	return ($alt,$caption,$leftright,$width,\@tags);
}


sub insertImage	{
	my $ref = shift;
	my ($this,$next,$leftright,$width,$alt,$link,$caption) = @$ref;

	# if a paragraph starts immediately afterwards, put
	#  both things in a div and float the image
	# first put in the div for the paragraph
	if ( $next =~ /^\s*<p/ )	{
		$this .= "<div>\n";
		$next =~ s/(<\/p>)/$1<\/div>\n/;
	}
	$this .= "<div style=\"position: relative; float: $leftright; clear: $leftright; margin-left: 1em; margin-right: 1em; width: ".$width."px;\">\n<img";
	if ( $alt )	{
		$this .= " alt=\"$alt\"";
	}
	$this .= " src=\"$link\" width=$width style=\"margin-bottom: 0.5em;\">\n";
	if ( $caption )	{
		$this .= "<br><span class=\"verysmall\">$caption</span>\n";
	}
	$this .= "</div>\n";

	# make sure the image stays in the section
	if ( $next =~ /^\s*<p/ )	{
		$next =~ s/(<\/p><\/div>)/$1\n<div style="clear: both;"><\/div>/;
	} else	{
		$this .= "\n<div style=\"clear: both;\"></div>\n\n";
	}
	return ($this,$next);
}

sub makeMap	{
	my ($dbt,$q,$s,$hbo,$maplink) = @_;


	# everything left must be cleaned up
	my @tags = split /_/,$maplink;
	my $map = join('|',@tags);
	$map =~ s/\'/\\'/g;
	my @tags = split /\|/,$map;

	# everything left is assumed to be a map query param
	my (%options,@errors);
	$options{'permission_type'} = 'read';
	$options{'calling_script'} = 'Review';
	my %unmatched;
	$unmatched{$_}++ foreach @tags;

	# taxon names take precedence because they are common
	my $sql = "SELECT a.taxon_no,taxon_name,lft,rgt FROM authorities a,$TAXA_TREE_CACHE t WHERE a.taxon_no=t.taxon_no AND taxon_name IN ('". join("','",keys %unmatched) ."') ORDER BY rgt-lft DESC";
	my @matches = @{$dbt->getData($sql)};
	my %seen;
	if ( @matches )	{
		# need to remove duplicates by hand (oh well)
		my $taxon;
		for my $m ( @matches )	{
			if ( ! $seen{$m->{'taxon_name'}} )	{
				$seen{$m->{'taxon_name'}}++;
				if ( $taxon )	{
					push @errors , "you can't draw a map using multiple taxon names";
				}
				$taxon = $m;
				delete $unmatched{$m->{'taxon_name'}};
			}
		}
		my @in_list;
		$sql = "SELECT taxon_no FROM $TAXA_TREE_CACHE WHERE lft>=".$taxon->{'lft'}." AND rgt<=".$taxon->{'rgt'};
		push @in_list , $_->{'taxon_no'} foreach @{$dbt->getData($sql)};
		$options{'taxon_list'} = \@in_list;
	}

	# interval names are the next best guess (and fast to search)
	$sql = "SELECT interval_no,interval_name FROM intervals WHERE interval_name IN ('". join("','",keys %unmatched) ."')";
	@matches = @{$dbt->getData($sql)};
	%seen = ();
	for my $m ( @matches )	{
		if ( ! $seen{$m->{'interval_name'}} )	{
			$seen{$m->{'interval_name'}}++;
			if ( $options{'max_interval'} )	{
				push @errors , "you can't draw a map using multiple time interval names";
			}
			$options{'max_interval'} = $m->{'interval_name'};
			delete $unmatched{$m->{'interval_name'}};
		}
	}

	# finally continent or country
	# use a fixed continent list
	my @continents = ('Africa','Asia','Australia','Europe','North America','South America');
	for my $t ( keys %unmatched )	{
		for my $c ( @continents )	{
			if ( $t eq $c )	{
				if ( $options{'country'} )	{
					push @errors , "you can't draw a map using multiple place names";
				}
				$options{'country'} = $t;
				delete $unmatched{$t};
				last;
			}
		}
	}

	# if param is a (useful) country we must have data for it
	$sql = "SELECT country,count(*) c FROM collections WHERE country IN ('". join("','",keys %unmatched) ."') GROUP BY country";
	@matches = @{$dbt->getData($sql)};
	%seen = ();
	for my $m ( @matches )	{
		if ( ! $seen{$m->{'country'}} )	{
			$seen{$m->{'country'}}++;
			if ( $options{'country'} )	{
				push @errors , "you can't draw a map using multiple place names";
			}
			$options{'country'} = $m->{'country'};
			delete $unmatched{$m->{'country'}};
		}
	}

	my @bad = keys %unmatched;
	if ( $#bad == 0 )	{
		push @errors , "the map parameter \"".$bad[0]."\" doesn't match a taxon, time interval, or place";
	} elsif ( $#bad > 0 )	{
		push @errors , "the map parameters \"".join(", ",@bad)."\" don't match taxa, time intervals, and/or places";
	}

	my $fields = ['collection_name','country','state','max_interval_no','min_interval_no','latdeg','latdec','latmin','latsec','latdir','lngdeg','lngdec','lngmin','lngsec','lngdir','seq_strat'];
	my ($colls) = Collection::getCollections($dbt,$s,\%options,$fields);
	if ( ! $colls )	{
		push @errors , "no collections were found to map";
	}

	if ( @errors )	{
		print "<center><p class=\"small\" style=\"margin-bottom: 2em;\">WARNING: ".join("; ",@errors)."</p></center>\n\n";
	}
	require Map;
	my $m = Map->new($q,$dbt,$s);
	my ($map_html_path,$errors,$warnings) = $m->buildMap('dataSet'=>$colls);

	my $count = `cat $HTML_DIR/public/maps/gifcount`;
	$maplink =~ s/ /_/g;
	`cp -p $HTML_DIR/public/maps/pbdbmap$count.png $HTML_DIR/public/reviews/$maplink.png`;

	return;

}

1;
