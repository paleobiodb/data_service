# The Paleobiology Database
# 
#   RefCheck.pm
# 
# Subroutines for working with external sources of bibliographic reference data.
# 

package ReferenceMatch;

use strict;

use feature 'unicode_strings';
use feature 'fc';

use Unicode::Normalize;
use Unicode::Collate;
use Algorithm::Diff qw(sdiff);
use Text::Levenshtein::Damerau qw(edistance);
# use Text::Transliterator::Unaccent;
# use Encode;
# use Carp qw(croak);
# use Scalar::Util qw(reftype);

use Exporter 'import';

our (@EXPORT_OK) = qw(ref_similarity get_reftitle get_pubtitle get_publisher
		      get_authorname get_authorlist split_authorlist parse_authorname
		      get_pubyr get_doi title_words @SCORE_VARS);

our $IgnoreCaseAccents = Unicode::Collate->new(
     level         => 1,
     normalization => undef);

our (@SCORE_VARS) = qw(sum complete count title pub auth1 auth2 pubyr volume pages pblshr);

our (%DEBUG, %COUNT);


# Reference matching
# ------------------

# ref_similarity ( r, m, options )
# 
# Return a matrix of scores from 0 to 100 (stored as a hashref) that represents the degree of
# similarity and conflict between the bibliographic reference attributes specified in r and those
# specified in m. The second argument m should contain the data from a record in the
# REFERENCE_DATA (refs) table, while r should contain some set of query attributes. For example, r
# might be the attributes of a new reference to be added, or attributes fetched from an external
# source such as crossref. The goal of this function is to determine whether the already existing
# record m is a good match.
# 
# The arguments $r and $m should both be hashrefs. If a third argument is given, it should be a
# hashref of option values.

sub ref_similarity {
    
    my ($r, $m, $options) = @_;
    
    %DEBUG = ();
    
    $options ||= { };
    
    # The basic idea is to compute a similarity score and a conflict score for each of the most
    # important field values. Similarity scores are all normalized to the range 0-100, with 100
    # being an exact similarity and 0 being no significant similarity. Conflict scores are also
    # normalized to the same range, with 100 meaning that the two values most likely do not
    # represent the same reference and 0 meaning no conflict. These two scores are not always
    # inverses of one another; if a given attribute appears only in one record but not the other
    # then both scores will usually be 0.
    
    # First compute the similarity and conflict scores for the work titles.
    # ---------------------------------------------------------------------
    
    my ($title_a, @title_alt_a) = get_reftitle($r);
    my ($title_b, @title_alt_b) = get_reftitle($m);
    
    my ($subtitle_a, @subtitle_alt_a) = get_subtitle($r);
    my ($subtitle_b, @subtitle_alt_b) = get_subtitle($m);
    
    # If one of the titles contains a : and the other does not but has a subtitle, add the
    # subtitle. If neither record contains a : then ignore any subtitles and match the main
    # titles only.
    
    if ( $title_a =~ /:/ && $title_b !~ /:/ && $subtitle_b )
    {
	$title_b = "$title_b: $subtitle_b";
    }
    
    elsif ( $title_b =~ /:/ && $title_a !~ /:/ && $subtitle_a )
    {
	$title_a = "$title_a: $subtitle_a";
    }
    
    # Compute the title similarity score.
    
    my ($title_similar, $title_conflict) = title_similarity($title_a, $title_b, 'work');
    
    save_debug('ti', 't1');
    
    # If the title similarity score is less than 80 and we also have one or more alternate titles
    # or subtitles in either record, try matching all combinations and take the highest similarity
    # score we find.
    
    if ( $title_similar < 80 && (@title_alt_a || @title_alt_b || $subtitle_a || $subtitle_b) )
    {
	my ($multi_similar, $multi_conflict) =
	    title_multimatch( [$title_a, @title_alt_a], [$subtitle_a, @subtitle_alt_a],
			      [$title_b, @title_alt_b], [$subtitle_b, @subtitle_alt_b] );
	
	save_debug('ti', 'ta');
	
	if ( $multi_similar > $title_similar )
	{
	    $title_similar = $multi_similar;
	    $title_conflict = $multi_conflict;
	    $DEBUG{t1mms}++;
	}
	
	else
	{
	    $DEBUG{t1mmf}++;
	}
    }
    
    # Then compute the similarity and conflict scores for the first and second authors. For now,
    # we ignore the rest of the author list.
    # --------------------------------------
    
    my ($auth1last_a, $auth1first_a) = get_authorname($r, 1);
    my ($auth1last_b, $auth1first_b) = get_authorname($m, 1);
    
    my ($auth1_similar, $auth1_conflict) = author_similarity($auth1last_a, $auth1first_a,
							     $auth1last_b, $auth1first_b);
    
    save_debug('au', 'a1');
    
    my ($auth2last_a, $auth2first_a) = get_authorname($r, 2);
    my ($auth2last_b, $auth2first_b) = get_authorname($m, 2);
    
    my ($auth2_similar, $auth2_conflict) = author_similarity($auth2last_a, $auth2first_a,
							     $auth2last_b, $auth2first_b, 2);
    
    save_debug('au', 'a2');
    
    # If at least one second author is missing, we consider some special cases.
    
    if ( ! $auth2last_a || ! $auth2last_b )
    {
	# If the first authors are similar and both second authors are missing, the second author
	# similarity is (100, 0). This is presumably a work with only one author.
	
	if ( ! $auth2last_a && ! $auth2last_b )
	{
	    if ( $auth1_similar )
	    {
		$auth2_similar = 100;
		$auth2_conflict = 0;
	    }
	}
	
	# If one record has two authors and the other doesn't, the second author similarity is set
	# to (0, 50), though it may be overridden below.
	
	else
	{
	    $auth2_similar = 0;
	    $auth2_conflict = 50;
	}
    }
    
    # If a conflict threshold was specified and has been exceeded, abort.
    # -------------------------------------------------------------------
    
    my $conflict_check = $title_conflict + $auth1_conflict + $auth2_conflict;
    
    if ( $options->{max_c} && $conflict_check > $options->{max_c} )
    {
	return { abort => 1, sum_c => $conflict_check };
    }
    
    # Then compute the similarity and conflict scores for the publication years.
    # --------------------------------------------------------------------------
    
    my $pubyr_a = get_pubyr($r);
    my $pubyr_b = get_pubyr($m);
    
    my ($pubyr_similar, $pubyr_conflict) = pubyr_similarity($pubyr_a, $pubyr_b);
    
    # Then compute the similarity and conflict scores for the volume, issue, and pages.
    # ---------------------------------------------------------------------------------
    
    my ($vol_a, $issue_a, $fp_a, $lp_a) = get_volpages($r);
    my ($vol_b, $issue_b, $fp_b, $lp_b) = get_volpages($m);
    
    # If the volume and page numbers are equal and non-empty but one of the issue numbers is
    # missing, assume that it matches too.
     
    if ( $vol_a && $fp_a && $lp_a && $vol_a eq $vol_b && $fp_a eq $fp_b && $lp_a eq $lp_b )
    {
	if ( $issue_a && ! $issue_b )
	{
	    $DEBUG{miss} = 1;
	    $issue_a = $issue_b;
	}
	
	elsif ( $issue_b && ! $issue_a )
	{
	    $DEBUG{miss} = 1;
	    $issue_b = $issue_a;
	}
    }
    
    my ($vol_similar, $vol_conflict) = vol_similarity($vol_a, $issue_a, $vol_b, $issue_b);
    my ($pages_similar, $pages_conflict) = vol_similarity($fp_a, $lp_a, $fp_b, $lp_b);
    
    # If a conflict threshold was specified and has been exceeded, abort.
    # -------------------------------------------------------------------

    $conflict_check = $title_conflict + $auth1_conflict + $auth2_conflict +
	$pubyr_conflict + $vol_conflict;
    
    if ( $options->{max_c} && $conflict_check > $options->{max_c} )
    {
	return { abort => 1, sum_c => $conflict_check };
    }
    
    # Then compute the similarity and conflict scores for the publication titles.
    # ---------------------------------------------------------------------------
    
    my ($pub_a, @pub_alt_a) = get_pubtitle($r);
    my ($pub_b, @pub_alt_b) = get_pubtitle($m);
    
    my ($pub_similar, $pub_conflict) = short_similarity($pub_a, $pub_b);
    
    save_debug('sh', 'pt');
    
    # If the publication similarity score is less than 80 and we also have one or more alternate
    # publication titles in either record, try matching all combinations and take the highest
    # similarity score we find.
    
    if ( $pub_similar < 80 && (@pub_alt_a || @pub_alt_b) )
    {
	my ($multi_similar, $multi_conflict) =
	    title_multimatch( [$pub_a, @pub_alt_a], undef,
			      [$pub_b, @pub_alt_b], undef, 'pub' );
	
	save_debug('sh', 'pa');
	
	if ( $multi_similar > $pub_similar )
	{
	    $pub_similar = $multi_similar;
	    $pub_conflict = $multi_conflict;
	    $DEBUG{ptmms} = 1;
	}
	
	else
	{
	    $DEBUG{ptmmf} = 1;
	}
    }
    
    # Then compute similarity and conflict scores for the publisher.
    # --------------------------------------------------------------
    
    my $publisher_a = get_publisher($r);
    my $publisher_b = get_publisher($m);
    
    my ($publish_similar, $publish_conflict) = short_similarity($publisher_a, $publisher_b);
    
    save_debug('sh', 'pb');
    
    # Now we consider some special cases.
    # -----------------------------------
    
    # If the publication year is similar and at least one of title and publication is similar and
    # at least one of volume and pages is similar, but either the first or the second authors are
    # dissimilar, we may need to revise the author similarity scores. To be on the safe side, we
    # require that at least one of the other similarity scores be a perfect 100. That is a good
    # indication that the two works are actually the same.
    
    if ( $pubyr_similar && ( $title_similar >= 80 || $pub_similar >= 80 ) &&
	 ( $vol_similar == 100 || $pages_similar ) &&
         ( $pubyr_similar == 100 || $title_similar == 100 || $pages_similar == 100 ) )
    {
	$DEBUG{spec}++;
	
	# If there are at least two authors and the first authors are not at least 70% similar,
	# see if each first author matches some other author in the other list. If so, set the
	# first author similarity to the average of the two similarity scores, with a maximum of
	# 80 points because they are in the wrong place. Set the first author conflict to the
	# maximum of the two conflict scores.
	
	my (@authors_a, @authors_b);
	
	if ( $auth2last_a && $auth2last_b && $auth1_similar < 70 )
	{
	    @authors_a = get_authorlist($r);
	    @authors_b = get_authorlist($m);
	    
	    # There is no need to match against the first authors, because we already know they are
	    # dissimilar.
	    
	    my ($alt_similar_a, $alt_conflict_a) =
		match_names($auth1last_a, $auth1first_a, @authors_b[1..$#authors_b]);
	    
	    my ($alt_similar_b, $alt_conflict_b) =
		match_names($auth1last_b, $auth1first_b, @authors_a[1..$#authors_a]);
	    
	    my $avg_sim = int(($alt_similar_a + $alt_similar_b) / 2);
	    my $avg_con = int(($alt_conflict_a + $alt_conflict_b) / 2);
	    
	    if ( $avg_sim > $auth1_similar )
	    {
		$auth1_similar = min_score(80, $avg_sim);
		$auth1_conflict = min_score(100 - $auth1_similar, $avg_con);
		$DEBUG{a1mms} = 1;
	    }
	    
	    else
	    {
		$DEBUG{a1mms} = 1;
	    }
	}
	
	# If there are at least two authors and the second authors are not at least 70% similar,
	# do the same for them.
	
	if ( $auth2last_a && $auth2last_b && $auth2_similar < 70 )
	{
	    # If we have already computed the author lists, there is no need to do so again.
	    
	    @authors_a = get_authorlist($r) unless @authors_a;
	    @authors_b = get_authorlist($m) unless @authors_b;
	    
	    # There is no need to match against the second authors, because we already know they
	    # are dissimilar.
	    
	    my ($alt_similar_a, $alt_conflict_a) =
		match_names($auth2last_a, $auth2first_a, @authors_b[0], @authors_b[2..$#authors_b]);
	    
	    my ($alt_similar_b, $alt_conflict_b) =
		match_names($auth2last_b, $auth2first_b, @authors_a[0], @authors_a[2..$#authors_a]);
	    
	    my $avg_sim = int(($alt_similar_a + $alt_similar_b) / 2);
	    my $avg_con = int(($alt_conflict_a + $alt_conflict_b) / 2);
	    
	    if ( $avg_sim > $auth2_similar )
	    {
		$auth2_similar = min_score(80, $avg_sim);
		$auth2_conflict = min_score(100 - $auth1_similar, $avg_con);
		$DEBUG{a2mms} = 1;
	    }
	    
	    else
	    {
		$DEBUG{a2mmf} = 1;
	    }
	}
	
	# If the first authors are similar but one record has two or more authors while the other
	# lists only one author, it is quite possible that one of the two records was incorrectly
	# entered. Unfortunately, there are many records in both the Paleobiology Database and
	# Crossref in which only the first author is listed and subsequent authors are left
	# out. In this situation, given that the similarities checked above also hold, assume that
	# the second and subsequent authors were probably left out of the other record.  Give the
	# second authors a similarity of 70, to reflect that likelihood.
	
	if ( $auth1_similar && ( $auth2last_a && ! $auth2last_b ||
				 $auth2last_b && ! $auth2last_a ) )
	{
	    $auth2_similar = 70;
	    $auth2_conflict = 0;
	    $DEBUG{a2def}++;
	}
    }
    
    # Make one last conflict check.
    
    $conflict_check = $title_conflict + $auth1_conflict + $auth2_conflict +
	$pubyr_conflict + $vol_conflict + $pub_conflict + $publish_conflict;
    
    if ( $options->{max_c} && $conflict_check > $options->{max_c} )
    {
	return { abort => 1, sum_c => $conflict_check };
    }
    
    # Compute the debug string for this record, and increment the global counts.
    
    my @debuglist;
    
    foreach my $d ( sort keys %DEBUG )
    {
	if ( $DEBUG{$d} == 1 )
	{
	    push @debuglist, $d;
	    $COUNT{$d}++;
	}
	
	elsif ( $DEBUG{$d} )
	{
	    push @debuglist, "$d:$DEBUG{$d}";
	    $COUNT{$d} += $DEBUG{$d} if $DEBUG{$d} > 0;
	}
    }
    
    my $debugstr = join(' ', @debuglist);
    
    # Compute the second-level score variables 'complete', 'count', and 'sum'.
    # ------------------------------------------------------------------------

    my $complete_similar = 0;
    my $count_similar = 0;
    my $sum_similar = 0;
    
    foreach my $value ( $title_similar, $pub_similar, $auth1_similar, $auth2_similar,
		        $pubyr_similar, $vol_similar, $pages_similar, $publish_similar )
    {
	if ( $value )
	{
	    $complete_similar++ if $value == 100;
	    $count_similar++;
	    $sum_similar += $value;
	}
    }
    
    my $complete_conflict = 0;
    my $count_conflict = 0;
    my $sum_conflict = 0;
    
    foreach my $value ( $title_conflict, $pub_conflict, $auth1_conflict, $auth2_conflict,
		        $pubyr_conflict, $vol_conflict, $pages_conflict, $publish_conflict )
    {
	if ( $value )
	{
	    $complete_conflict++ if $value == 100;
	    $count_conflict++;
	    $sum_conflict += $value;
	}
    }
    
    # Return all of these values as a hashref.
    # ----------------------------------------
    
    my $values = { complete_s => $complete_similar, complete_c => $complete_conflict,
		   count_s => $count_similar, count_c => $count_conflict,
		   sum_s => $sum_similar, sum_c => $sum_conflict,
		   title_s => $title_similar, title_c => $title_conflict,
		   pub_s => $pub_similar, pub_c => $pub_conflict,
		   auth1_s => $auth1_similar, auth1_c => $auth1_conflict,
		   auth2_s => $auth2_similar, auth2_c => $auth2_conflict,
		   pubyr_s => $pubyr_similar, pubyr_c => $pubyr_conflict,
		   volume_s => $vol_similar, volume_c => $vol_conflict,
		   pages_s => $pages_similar, pages_c => $pages_conflict,
		   pblshr_s => $publish_similar, pblshr_c => $publish_conflict,
		   debugstr => $debugstr };
    
    return $values;
}


sub save_debug {
    
    my ($from, $to) = @_;
    
    my $fl = length($from);
    
    foreach my $k ( keys %DEBUG )
    {
	if ( substr($k, 0, $fl) eq $from )
	{
	    my $new = $to . substr($k, $fl);
	    $DEBUG{$new} = $DEBUG{$k};
	    delete $DEBUG{$k};
	}
    }
}


sub clear_counts {

    %COUNT = ();
}


# Attribute extraction
# --------------------

# The following functions extract attributes from reference data records, which must be
# hashrefs. In each case, we check for the fieldnames from paleobiodb, crossref, and bibjson. If
# no corresponding attribute value is found, return undefined.

# get_reftitle ( r, type )
# 
# Return the reference title from $r.

sub get_reftitle {
    
    my ($r) = @_;
    
    # If this is a paleobiodb record, return the 'reftitle' value if it is non-empty. If there is
    # a pubtitle but no reftitle, return that. Otherwise, return undefined.
    
    if ( $r->{reftitle} || $r->{pubtitle} || $r->{author1last} )
    {
	my $title = $r->{reftitle} || $r->{pubtitle};
	
	# If the value contains a [ character, it may represent two alternate titles (often one in
	# English and one in some other language).
	
	if ( $title && $title =~ qr{ \[ }xs )
	{
	    return clean_string(split_title($title));
	}

	else
	{
	    return clean_string($title);
	}
    }
    
    # Otherwise, the title will be stored under the 'title' attribute. In a crossref record this
    # will always be an array of strings, and it might be so in BibJSON as well. If there is more
    # than one value, return them all.
    
    elsif ( ref $r->{title} eq 'ARRAY' && $r->{title}->@* )
    {
	return clean_string($r->{title}->@*);
    }
    
    # If the title attribute is a simple string, return that. BibJSON records are most likely to
    # have a simple string value under 'title'. As above, if the value contains a [ character it
    # may represent two alternate titles.
    
    elsif ( $r->{title} && ! ref $r->{title} )
    {
	if ( $r->{title} =~ qr{ \[ }xs )
	{
	    return clean_string(split_title($r->{title}));
	}

	else
	{
	    return clean_string($r->{title});
	}
    }
    
    # If none of these are found, return undef.
    
    return undef;
}


# get_subtitle ( r )
#
# Return the subtitle if any from $r.

sub get_subtitle {
    
    my ($r) = @_;
    
    # If there is a 'subtitle' key that is an array of values, return them all.
    
    if ( ref $r->{subtitle} eq 'ARRAY' && $r->{subtitle}->@* )
    {
	return clean_string($r->{subtitle}->@*);
    }

    # If the value of 'subtitle' is a simple nonempty string, return that.
    
    elsif ( $r->{subtitle} && ! ref $r->{subtitle} )
    {
	return clean_string($r->{subtitle});
    }
}


# get_pubtitle ( r )
#
# Return the publication title from $r.

sub get_pubtitle {

    my ($r) = @_;
    
    # If this is a paleobiodb record, return the 'pubtitle' value if it is non-empty. But if there
    # is a pubtitle but no reftitle, return undefined because that means the record was entered
    # incorrectly and the pubtitle should actually be the reftitle.
    
    if ( $r->{reftitle} || $r->{pubtitle} || $r->{author1last} )
    {
	my $title = $r->{reftitle} ? $r->{pubtitle} : undef;

	if ( $title && $title =~ qr{ \[ }xs )
	{
	    return clean_string(split_title($title));
	}

	else
	{
	    return clean_string($title);
	}
    }
    
    # Crossref records store the publication under 'container-title'. If there is more than one,
    # return them all.
    
    elsif ( ref $r->{'container-title'} eq 'ARRAY' && $r->{'container-title'}->@* )
    {
	return clean_string($r->{'container-title'}->@*);
    }
    
    # A BibJSON record may have the publication title under any of the following: 'journal',
    # 'booktitle', 'series'.
    
    else
    {
	my $title = $r->{journal} || $r->{booktitle} || $r->{series};

	if ( $title && $title =~ qr{ \[ }xs )
	{
	    return clean_string(split_title($title));
	}

	else
	{
	    return clean_string($title);
	}
    }
}


# split_title ( title )
#
# If the given string matches a pattern like "... [...]" or "[...] [...]", then it may represent
# two alternate titles crammed into one field. Typically, one of these will be in English and one
# in some other language. Return them both.

sub split_title {

    my ($title) = @_;
    
    # Look for either of the patterns specified above. We use a branch reset expression (?|...) so
    # that $1 is always the first of the two titles no matter which branch of the pattern
    # matches. The terminating right bracket is occasionally missing for unknown reasons.
    
    if ( $title =~ qr{ ^ \s* (?| \[ (.*) \] | (.*) ) \s* \[ (.*) \]? $ }xs )
    {
	my $first = $1;
	my $second = $2;

	# We need to reject the case of a word or two in square brackets at the end of a long
	# title. That syntax is occasionally used, but means something entirely different. So if
	# first is more than twice as long as second, just return the original title as a whole.
	
	if ( $second && length($first) / length($second) > 2 )
	{
	    return $title;
	}
	
	# Otherwise, return both sections as individual titles.

	else
	{
	    return ($first, $second);
	}
    }
}


# get_authorname ( r, index )
# 
# Return the name of one of the authors from $r, as (last, first). The selected author is
# specified by $index, and defaults to 1 if not given. Two strings are always returned, but one or
# both may be empty. The first value (lastname) will always be non-empty if there is any name at
# all.

sub get_authorname {

    my ($r, $index) = @_;
    
    # If no index is specified, return the first author.
    
    $index ||= 1;
    
    # The following variable will hold the selected name, if any.
    
    my $selected;
    
    # If this is a paleobiodb record, return the first or second author name or else look through
    # otherauthors.
    
    if ( $r->{reftitle} || $r->{pubtitle} || $r->{author1last} )
    {
	if ( $index == 1 )
	{
	    return (clean_string($r->{author1last}), clean_string($r->{author1init}));
	}
	
	elsif ( $index == 2 )
	{
	    return (clean_string($r->{author2last}), clean_string($r->{author2init}));
	}
	
	elsif ( $r->{otherauthors} )
	{
	    my @names = split_authorlist(clean_string($r->{otherauthors}));
	    
	    if ( $index - 3 < @names )
	    {
		$selected = $names[$index - 3];

		# There are a few paleobiodb entries that mistakenly have / instead of . after the
		# initial. Fix them now.
		
		if ( $selected =~ qr{/} )
		{
		    $selected =~ s{ ( ^ \pL\pM* | \s \pL\pM* ) [/] }{\1.}gx;
		}
	    }
	}
    }
    
    # Otherwise, the authors will be listed under 'author'. Both BibJSON and Crossref use this
    # fieldname. If it is an array, select the specified element.
    
    elsif ( $r->{author} && ref $r->{author} eq 'ARRAY' )
    {
	# Some Crossref entries list author affiliations incorrectly by interleaving them with the
	# author entries. In order to check for this, if the entries are hashrefs we look for a
	# field in the first entry which represents a family/last name.
	
	my $family_field;
	
	if ( ref $r->{author}[0] eq 'HASH' )
	{
	    $family_field = $r->{author}[0]{family} ? 'family' :
		$r->{author}[0]{last} ? 'last' :
		$r->{author}[0]{lastname} ? 'lastname' : '';
	}
	
	# If we have a family/last name field, we can compensate for the interleaving problem by
	# skipping entries that do not have that field. We also skip entries up to the specified
	# $index value.
	
	my $count = 0;
	
	foreach my $entry ( $r->{author}->@* )
	{
	    next if $family_field && ! $entry->{$family_field};
	    next if ++$count < $index;

	    if ( ref $entry )
	    {
		$selected = $entry;
	    }

	    else
	    {
		$selected = clean_string($entry);
	    }
	    
	    last;
	}
    }
    
    # If the author list is a string, we split it into individual names and then select the
    # specified one.
    
    elsif ( $r->{author} && ! ref $r->{author} )
    {
	my @names = split_authorlist(clean_string($r->{author}));
	
	$selected = $names[$index];
    }

    # If we have selected a hashref, extract the first and last components and clean them.
    
    if ( ref $selected )
    {
	return clean_string(parse_authorname($selected));
    }

    # If we have selected a string, it has already been cleaned.

    elsif ( $selected )
    {
	return parse_authorname($selected);
    }
    
    # Otherwise, return empty strings.

    else
    {
	return ('', '');
    }
}


# get_authorlist ( r, index )
# 
# Return a list of the authors from $r. Each element of the list will be a listref in the form
# [last, first] or [last, first, affiliation]. The result starts at position $index in the author
# list, defaulting to 1. Each returned element will have a nonempty value for the last name.  If
# no authors are found, the result will be empty.

our($CONTAINS_INITIALS) = qr{ \b \pL\pM*[.] | ^ \pL\pM* \s | \s \pL\pM* $ }xs;

our($NAME_SUFFIX) = qr{ ^ (?:jr|ii|iii|iv|2nd|3rd|4th) [.] $ }xsi;

sub get_authorlist {
    
    my ($r, $index) = @_;
    
    $index ||= 1;
    
    # The following variable will collect the names.
    
    my @result;
    
    # If there is an 'author1last' field, this must be a record from the paleobiodb.
    
    if ( $r->{author1last} )
    {
	# If $index is 1, start with the author1 fields. We already know that author1last is not
	# empty, but author1init may be.
	
	if ( $index == 1 )
	{
	    push @result, [ clean_string($r->{author1last}),
			    clean_string($r->{author1init}) ];
	}
	
	# If $index is 1 or 2, look at the author2 fields. If author2last is empty,
	# skip it.
	
	if ( $index <= 2 && $r->{author2last} )
	{
	    push @result, [ clean_string($r->{author2last}),
			    clean_string($r->{author2init}) ];
	}

	elsif ( ! $r->{author2last} )
	{
	    $index--;
	}
	
	# Then split up otherauthors. If $index is greater than 3, skip some of the resulting
	# entries.

	if ( $r->{otherauthors} )
	{
	    my $otherlist = clean_string($r->{otherauthors});
	    
	    # There are a few paleobiodb entries that mistakenly have / instead of . after the
	    # initial. So substitute the latter for the former.
	    
	    if ( $otherlist =~ qr{/} )
	    {
		$otherlist =~ s{ ( ^ \pL\pM* | \s \pL\pM* ) [/] }{\1.}gx;
	    }
	    
	    my @othernames = split_authorlist($otherlist);
	    
	    if ( $index > 3 )
	    {
		splice(@othernames, 0, $index - 3);
	    }
	    
	    # Parse each entry and add a listref of name components to the result list.
	    
	    foreach my $name ( @othernames )
	    {
		my @components = parse_authorname($name);
		
		if ( $components[0] )
		{
		    push @result, \@components;
		}
	    }
	}
    }
    
    # Otherwise, the authors if any must be listed under 'author' since both Crossref and
    # BibJSON use that field name.
    
    elsif ( $r->{author} && ref $r->{author} eq 'ARRAY' )
    {
	# If 'author' is an array of hashes, we need to work around the interleaving bug discussed
	# in get_authorname above.
	
	my $family_field;

	if ( ref $r->{author}[0] eq 'HASH' )
	{
	    $family_field = $r->{author}[0]{family} ? 'family' :
		$r->{author}[0]{last} ? 'last' :
		$r->{author}[0]{lastname} ? 'lastname' : '';
	}
	
	# Go through the entries, skipping until we reach $index. If a family/last name field has
	# been identified, skip entries that do not have a value for that field. But if such
	# entries have a 'name' or 'affiliation' attribute, add that information to the previous name.
	
	my $count = 0;
	
	foreach my $entry ( $r->{author}->@* )
	{
	    # Skip any entry the lacks a family name field, but if we have already found one or
	    # more proper entries and this entry has a value for 'name', assume the value is
	    # intended to give the affiliation for the most recent name entry. Add the value as an
	    # additional component to that entry.
	    
	    if ( $family_field && ! $entry->{$family_field} )
	    {
		if ( @result && $entry->{name} )
		{
		    my $affiliation = $entry->{name};
		    
		    if ( ref $affiliation eq 'ARRAY' )
		    {
			$affiliation = $affiliation->[0];
		    }
		    
		    push $result[-1]->@*, $affiliation;
		}
	    }
	    
	    # Skip name entries up to the position indicated by $index, then parse each subsequent
	    # name and add a listref of the name components to the result list.
	    
	    elsif ( ++$count >= $index )
	    {
		# If the entry is a hashref or an array, parse it and then clean the individual
		# components. If we get a list with a non-empty first component (last name) then
		# add it to the results.
		
		if ( ref $entry )
		{
		    my @components = clean_string(parse_authorname($entry));
		    push @result, \@components if $components[0];
		}
		
		# If the entry is a non-empty string, clean it and then parse it.
		
		elsif ( $entry )
		{
		    my @components = parse_authorname(clean_string($entry));
		    push @result, \@components if $components[0];
		}
	    }
	}
    }
    
    # If the 'author' field is a hashref, assume it represents a single name and parse it.

    elsif ( ref $r->{author} eq 'HASH' )
    {
	my @components = clean_string(parse_authorname($r->{author}));
	push @result, \@components if $components[0];
    }
    
    # If the 'author' field is a simple string, we must split it into individual names.
    
    elsif ( $r->{author} && ! ref $r->{author} )
    {
	my @names = split_authorlist(clean_string($r->{author}));
	
	if ( $index > 1 )
	{
	    splice(@names, 0, $index - 1);
	}
	
	foreach my $name ( @names )
	{
	    my @components = parse_authorname($name);
	    push @result, \@components if $components[0];
	}
    }
    
    # Return the result if any.

    return @result;
}


# split_authorlist ( authorlist )
#
# Given a string composed of one or more author names, split it up into individual names as
# correctly as we can manage. The argument string must be normalized to Unicode NFD before it is
# passed in, which can be carried out by passing it through clean_string() first.

sub split_authorlist {

    my ($authorlist) = @_;
    
    # If the authorlist does not contain at least two alphabetic characters in a row, possibly
    # with marks in between, return the empty list.
    
    return () unless $authorlist =~ /\pL\pM*\pL/;
    
    # Start by removing initial and final whitespace, and then splitting the list on commas. The
    # word 'and' following a comma is included, but only if it is all lowercase or all
    # uppercase. The reason for this is that some of the data from Crossref is all in uppercase,
    # whereas the word 'and' in the paleobiodb records is always lowercase. It is possible, though
    # very unlikely, that somebody might have 'And' as one of the words in their name and we don't
    # want to mistakenly drop it.
    
    $authorlist =~ s/^\s+//;
    $authorlist =~ s/\s+$//;
    $authorlist =~ s/\s+/ /g;
    
    my @items = split qr{ \s*,\s* (?: and \s* | AND \s* )? }xs, $authorlist;
    
    # If the last item includes the word 'and' with at least two words before and two after it,
    # split on that word.
    
    if ( $items[-1] =~ qr{ ^ (.* \S \s+ \S+) \s+ (?: and | AND ) \s+ (\S+ \s+ \S .*) $ }xs )
    {
	$items[-1] = $1;
	push @items, $2;
    }
    
    # Then go through the list one by one and merge the items into names as necessary. Names that
    # look like "Smith, A. B." will appear as two items in this list, and will be merged into
    # one. Some entries in the paleobiodb have authorlists in this format.
    
    my @names;
    
    foreach my $item ( @items )
    {
	# If the item is 'jr.', 'ii', 'iii', etc. add it to the previous name. If there is no
	# previous name, discard it.
	
	if ( $item =~ $NAME_SUFFIX )
	{
	    if ( @names )
	    {
		$names[-1] .= ", $item";
	    }
	}
	
	# If the item contains at least three alphabetic characters in a row, it is almost certainly a
	# name. Add it to the name list.
	
	elsif ( $item =~ qr{ \pL\pM* \pL\pM* \pL }xs )
	{
	    push @names, $item;
	}
	
	# If the item doesn't have three alphabetic characters in a row and either contains a
	# letter followed by a period or is a single letter or has a single letter at the start or
	# end, it probably represents initials associated with the previous item. If the previous
	# item doesn't contain initials, add this item to it. Otherwise, discard this item.
	
	elsif ( $item =~ qr{ \b \pL\pM*[.] | ^ \pL\pM* \s | \s \pL\pM* $ | ^ \pL\pM* $ }xs )
	{
	    if ( @names && $names[-1] !~ $CONTAINS_INITIALS )
	    {
		$names[-1] .= ", $item";
	    }
	}
	
	# If the item has at least two alphabetic characters in a row, it is almost certainly a
	# name. Add it to the name list. We need to do this check here because there are names
	# such as 'Wu' and also initials such as 'A. de B.'. So we check for initials first and
	# then check for two-letter names.

	elsif ( $item =~ qr{ \pL\pM* \pL }xs )
	{
	    push @names, $item;
	}
	
	# If the item has at least one alphabetic character and the previous name has no initials,
	# add this item to it. Otherwise, discard this item.

	elsif ( $item =~ /\pL/ )
	{
	    if ( @names && $names[-1] !~ $CONTAINS_INITIALS )
	    {
		$names[-1] .= ", $item";
	    }
	}
    }
    
    return @names;
}


# parse_authorname ( entry )
#
# Given a single entry representing an author name, extract first and last names from it. The
# argument may either be a hashref, an arrayref, or a string. If it is a string, it should be
# processed by &clean_string before being passed in. This will normalize it to Unicode
# NFD. If it is a reference, the result should be processed by &clean_string.

sub parse_authorname {

    my ($entry) = @_;
    
    my ($last, $first, $affiliation);
    
    # If the selected entry is an arrayref, return the contents as-is. This is most likely to
    # happen if an arrayref of already parsed name components is passed to this routine again.
    
    if ( ref $entry eq 'ARRAY' )
    {
	return @$entry;
    }
    
    # If the selected entry is a hashref, look for the various fieldnames used for first and last
    # names. Only return the first name if we have found a non-empty last name. If don't find a
    # separate last name but we do find a 'name' field, fall through to the next section.
    
    elsif ( ref $entry eq 'HASH' )
    {
	$last = $entry->{last} || $entry->{lastname} || $entry->{family};
	$first = $entry->{first} || $entry->{firstname} || $entry->{given};
	$affiliation = $entry->{affiliation} || $entry->{institution};
	
	if ( ref $affiliation eq 'ARRAY' )
	{
	    $affiliation = $affiliation->[0];
	}
	
	elsif ( ref $affiliation eq 'HASH' )
	{
	    $affiliation = $affiliation->{name};
	}
	
	if ( $last )
	{
	    return ($last, $first, $affiliation);
	}
	
	elsif ( $entry->{name} )
	{
	    $entry = $entry->{name};
	}

	else
	{
	    return;
	}
    }
    
    # If the entry is anything other than a nonempty string, return the empty list.
    
    return unless $entry && ! ref $entry;
    
    # Otherwise, we parse the string into a first and last name. Note that $affiliation may have
    # been set above if the original entry was a hashref with both 'name' and 'affiliation'
    # as keys. Otherwise, it will be undefined.
    
    # If the full name contains a comma, split it on commas. The last name is always the first
    # component.
    
    if ( $entry =~ /,/ )
    {
	my @components = split /\s*,\s*/, $entry;
	
	$last = shift @components;
	
	# If the last name is followed by a component such as 'jr', 'ii', 'iii', etc. optionally
	# followed by a period, then add that to the last name.
	
	if ( @components && $components[0] =~ $NAME_SUFFIX )
	{
	    $last .= " " . shift @components;
	}

	# Otherwise, if we have at least two more components and the last component looks like that,
	# add it to the last name.

	elsif ( @components > 1 && $components[-1] =~ $NAME_SUFFIX )
	{
	    $last .= " " . pop @components;
	}

	# If we have one or more remaining components, take the first non-empty one as the first
	# name and return. If they are all empty, then the first name will be empty.
	
	if ( @components )
	{
	    shift @components while @components > 1 && $components[0] eq '';
	    return ($last, $components[0], $affiliation);
	}
	
	# Otherwise, take the last name we have extracted and fall through to the next step.
	
	else
	{
	    $entry = $last;
	}
    }
    
    # If we get here, we have a name that looks like either "B. Smith", "Smith B." or "Bob Smith".
    
    # First look for any of the following patterns: "B. Smith", "B Smith", "B-C Smith",
    # "B.-C. Smith", "B. C. D. Smith", "B C D Smith", etc. Note that .* is greedy by default. We
    # want to match everything up to the very last letter that is preceded by a non-letter and
    # followed by an optional period and then a space.
    
    if ( $entry =~ qr{ ^ ( \pL\pM* \b .* \b \pL\pM* [.]? | \pL\pM* [.]? ) \s+ (\S .*) }xs )
    {
	$first = $1;
	$last = $2;
	
	return ($last, $first, $affiliation);
    }

    # If the name contains any initial followed by a period and ends with at least two letters in
    # a row, break after the initial. This will properly parse names such as "Bob A. Smith".
    
    if ( $entry =~ qr{ ( .* \b \pL\pM* [.] ) \s* ( \S .* \pL\pM*\pL\pM* | \pL\pM*\pL\pM* ) $ }xs )
    {
	$first = $1;
	$last = $2;

	return ($last, $first, $affiliation);
    }
    
    # Otherwise, look for initials at the end. The first name starts with the first letter that is
    # preceded by a space and followed by a word boundary. In this case we use .*? for a
    # non-greedy match because we want to capture all of the initials rather than just the last
    # one.
    
    if ( $entry =~ qr{ (.*? \S) \s+ ( \pL\pM*[-. ] .* | \pL\pM* $ ) }xs )
    {
	$last = $1;
	$first = $2;

	return ($last, $first, $affiliation);
    }

    # If we don't find either of these patterns, split off the last word as the last name. But if
    # the second-to-last word is 'de' or 'van', then include it.
    
    if ( $entry =~ qr{ ^ (.*) \s+ ( (?: van \s+ | de \s+ )? \S* ) $ }xsi )
    {
	$first = $1;
	$last = $2;
	
	return ($last, $first, $affiliation);
    }
    
    # If the name doesn't have multiple words, return the whole thing as the last name.
    
    else
    {
	return ($entry, '', $affiliation);
    }
}


# get_volpages ( r )
# 
# Return the volume, number, first and last pages from $r.

sub get_volpages {
    
    my ($r) = @_;
    
    my ($volume, $issue, $first, $last);
    
    # If this is a paleobiodb record, return the info from the paleobiodb fields.
    
    if ( $r->{reftitle} || $r->{pubtitle} || $r->{author1last} )
    {
	# In some entries the firstpage and lastpage fields contain more than one number, and in
	# some entries one or the other is blank. If either one is blank, set it to the same value
	# as the other.
	
	my $firstpage = $r->{firstpage} || $r->{lastpage};
	my $lastpage = $r->{lastpage} || $r->{firstpage};

	# Then extract the first decimal number from firstpage and the last from lastpage.
	
	if ( $firstpage =~ qr{ ^ (\d+) }xs )
	{
	    $first = $1;
	}
	
	if ( $lastpage =~ qr{ ^ (\d+) $ }xs )
	{
	    $last = $1;
	}
	
	# The volume and issue numbers are simple fields.
	
	$volume = $r->{pubvol};
	$issue = $r->{pubno};
    }
    
    # Otherwise, look under either 'pages' or 'page' for the page numbers. This field will have
    # both first and last page numbers, and possibly others as well. As above, grab just the first
    # and last. The volume is always in 'volume', but the issue number may be in either 'number'
    # or 'issue'.
    
    else
    {
	$volume = $r->{volume};
	$issue = $r->{issue} || $r->{number};
	
	my $pages = $r->{pages} || $r->{page};
	
	if ( $pages =~ qr{ ^ (\d+) }xs )
	{
	    $first = $1;
	}

	if ( $pages =~ qr{ (\d+) $ }xs )
	{
	    $last = $1;
	}
    }
    
    return ($volume, $issue, $first, $last);
}


# get_pubyr ( r )
#
# Return the publication year from $r.

sub get_pubyr {
    
    my ($r) = @_;
    
    # If this is a paleobiodb record, return the 'pubyr' field or nothing.
    
    if ( $r->{reftitle} || $r->{pubtitle} || $r->{author1last} )
    {
	return $r->{pubyr};
    }

    # If this is a Crossref record, the publication year might be under 'published-print' or
    # 'published-online'. Or if it is a dissertation, it might be under 'issued' or 'approved'.

    # If none of those fields are found, try 'year' and 'pubyr'.

    else
    {
	return $r->{'published-print'} && extract_json_year($r->{'published-print'}) ||
	    $r->{'published-online'} && extract_json_year($r->{'published-online'}) ||
	    $r->{'issued'} && extract_json_year($r->{'issued'}) ||
	    $r->{'approved'} && extract_json_year($r->{'approved'}) ||
	    $r->{year} || $r->{pubyr};
    }
}


# get_publisher ( r )
#
# Return the publisher from $r.

sub get_publisher {
    
    my ($r) = @_;

    # This one is easy, because all of the standards use the field 'publisher' to hold this
    # information.

    return clean_string($r->{publisher});
}


# get_doi ( r )
# 
# Return the doi from $r.

sub get_doi {
    
    my ($r) = @_;
    
    my $doi = $r->{doi} || $r->{DOI};
    
    # If the doi is stored in URL form, strip off the protocol and hostname.
    
    if ( $doi && $doi =~ qr{ ^ \w+ [:] // [^/]+ / (.*) }xs )
    {
	$doi = $1;
    }
    
    # Make sure it contains at least one / between two other characters.
    
    if ( $doi && $doi =~ qr{ [^/] / [^/] }xs )
    {
	return $doi;
    }

    else
    {
	return '';
    }
}


# extract_json_year ( json_value )
#
# Extract a year from the JSON date values returned by crossref and possibly other sources. If the
# value is a nested array, return the first value of the first (generally only)
# subarray. Otherwise, extract the first four digit string found as the date.

sub extract_json_year {
    
    my ($value) = @_;
    
    if ( ref $value eq 'HASH' && $value->{'date-parts'} )
    {
	$value = $value->{'date-parts'};
    }
    
    if ( ref $value eq 'ARRAY' )
    {
	($value) = @$value;
    }
    
    if ( ref $value eq 'ARRAY' )
    {
	return $value->[0];
    }
    
    elsif ( ! ref $value && $value =~ /([12]\d\d\d)/ )
    {
	return $1;
    }
    
    else
    {
	return undef;
    }
}


# clean_string ( string... )
#
# For each nonempty argument, replace any HTML character entity sequences with the corresponding
# characters and then normalize to NFD. We choose the decomposed normalization so that we can easily
# compare letters without regard to accent marks.

sub clean_string {
    
    my (@values) = @_;
    
    foreach ( @values )
    {
	if ( defined $_ && $_ ne '' )
	{
	    $_ =~ s{ &\#(\d+); }{ chr($1) }xsge;
	    $_ = NFD($_);
	}
	
	$_ //= '';
    }
    
    return wantarray ? @values : $values[0];
}


# Similarity functions
# --------------------

# Each of these functions returns a list of two scores. The first is a similarity score from
# 0-100, representing roughly the percentage chance that the two values represent the same
# work. The second is a conflict score from 0-100, representing roughly the percentage chance that
# the two values represent different works.
#
# In order for any of the similarity functions to work correctly, the arguments must be normalized
# to Unicode NFD before being passed in. The clean_string() function will accomplish this.


# title_similarity ( title_a, title_b )
# 
# Compute a fuzzy match score between $title_a and $title_b, doing our best to allow for both
# mistyping and leaving out a word or two. This routine is used to compare work titles.

our %STOPWORD = ( '' => 1, a => 1, an => 1, and => 1, or => 1, in => 1,
		  the => 1, at => 1, for => 1, on => 1, of => 1 );

our $THREE_LETTERS = qr{ \pL\pM*\pL\pM*\pL }xs;

sub title_similarity {
    
    my ($title_a, $title_b) = @_;
    
    $COUNT{ti_}++;
    
    # If one or both arguments are empty, return (0, 0). In that case we do not have enough
    # information to tell us that the records are similar, nor that they are dissimilar. We
    # consider a title to be empty if it doesn't have at least 3 alphabetic characters in a
    # row. The letters may have marks in between.
    
    unless ( $title_a && $title_a =~ $THREE_LETTERS &&
	     $title_b && $title_b =~ $THREE_LETTERS )
    {
	$DEBUG{tiemp}++;
	return (0, 0);
    }
    
    # If the two titles are equal when case and accents are ignored, return (100, 0). We ignore
    # accent marks because these are often mistyped or omitted.
    
    if ( eq_insensitive($title_a, $title_b) )
    {
	$DEBUG{tieqs}++;
	return (100, 0);
    }
    
    # Otherwise, start by removing HTML tags, which occasionally appear in Crossref titles.
    
    $title_a =~ s{ &lt; .*? &gt; | < .*? > }{}xsg;
    $title_b =~ s{ &lt; .*? &gt; | < .*? > }{}xsg;

    # Continue by removing accent marks and then splitting each title into words. Any sequence of
    # letters and numbers counts as a word, and any sequences of other characters (punctuation and
    # spacing) counts as a word boundary. Stopwords are also removed, since they occasionally get
    # left out of a title by mistake.
    
    $title_a =~ s/\pM//g;
    $title_b =~ s/\pM//g;
    
    my @words_a = grep { ! $STOPWORD{$_} } map { fc } split /[^\pL\pN]+/, $title_a;
    my @words_b = grep { ! $STOPWORD{$_} } map { fc } split /[^\pL\pN]+/, $title_b;
    
    # If one title starts with "<section heading>, in ..." where the ... matches the other title,
    # that will still count as a match. In particular, some PBDB reference titles were entered in
    # that form. The variable name suffix _wc means "word count".
    
    my $initial_wca = check_for_section_prefix($title_a);
    my $initial_wcb = check_for_section_prefix($title_b);
    
    # Similarity check #1:
    #
    # If the two caseless and accentless word sequences are identical, return a similarity of
    # 100. If one word sequence is a prefix of the other, and the extra stuff at the end of the
    # other one is not too long, also return a match. Sometimes a subtitle is mistakenly left out,
    # and sometimes extra cruft is appended for various reasons. This is a very inexpensive check
    # to make.

    my ($longer_wc, $shorter_wc);
    
    # Try the full titles first.
    
    if ( $shorter_wc = prefix_match(\@words_a, \@words_b) )
    {
	$longer_wc = max_score(scalar(@words_a), scalar(@words_b));
	$DEBUG{tiprfs}++;
    }
    
    # If those don't match, and one or both have possible initial section headings, try again with
    # those skipped.
    
    elsif ( $initial_wca || $initial_wcb )
    {
	if ( $shorter_wc = prefix_match(\@words_a, \@words_b, $initial_wca, $initial_wcb) )
	{
	    $longer_wc = max_score(scalar(@words_a) - $initial_wca,
				   scalar(@words_b) - $initial_wcb);
	    $DEBUG{tiprfhs}++;
	}
    }
    
    # If we have a match either way, return it.
    
    if ( $shorter_wc )
    {
	if ( $longer_wc == $shorter_wc || $shorter_wc > 3 && $longer_wc < $shorter_wc * 2 )
	{
	    return (100, 0);
	}
	
	elsif ( $shorter_wc > 3 )
	{
	    return (70, 0);
	}
	
	else
	{
	    return (50, 0);
	}
    }
    
    # Similarity check #2
    # 
    # If the prefix match failed, check to see if two or more of the early words in each title
    # appear early in the other title. This is an inexpensive check which can reject non-matching
    # titles easily. If successful, fall through to the next check. Otherwise, return a conflict
    # of 100.

    my $initial_match;
    
    if ( initial_words_match(\@words_a, \@words_b, 5, 2) )
    {	
	$initial_match = 1;		# fall through and continue to the next check
	$DEBUG{tiwmc}++;
    }
    
    # As above, if there is an initial heading sequence on either or both titltes then repeat the
    # check with this sequence skipped. If this test succeeds, remove the initial sequence(s)
    # before falling through to the subsequent checks.
    
    elsif ( $initial_wca || $initial_wcb )
    {
	splice(@words_a, 0, $initial_wca);
	splice(@words_b, 0, $initial_wca);
	
	if ( initial_words_match(\@words_a, \@words_b, 5, 2) )
	{
	    $initial_match = 1;		# fall through and continue to the next check
	    $DEBUG{tiwmhc}++;
	}
	
	else
	{
	    $DEBUG{tiwmhf}++;
	    return (0, 100);
	}
    }
    
    else
    {
	$DEBUG{tiwmf}++;
	return (0, 100);
    }
    
    # Similarity check #3
    #
    # There is a chance that the two titles are similar, so the next step is to compare them using
    # a more comprehensive and more expensive procedure. We use Algorithm::Diff to compute the
    # difference between the two word lists, looking for the following special cases:
    # 
    # a) Differing words that have only a small difference, making it likely that one of
    # the two was mis-entered.
    # 
    # b) One title contains a word which is the same as several words from the other one
    # concatenated without any spaces, in the same place in the sequence. Unfortunately, we
    # sometimes find this error in crossref titles. Apparently, some of the spaces in certain
    # titles were at some point entered as "nonbreaking spaces" and a subsequent processing step
    # dropped them entirely.
    # 
    # c) Two titles that are similar except for an extra set of words on the end of one of them.
    # This probably means either one of them accidentally had its final part left off or else the
    # other one had some cruft added to the end of it. We still have to consider this case despite
    # the prefix check above, because that check will have failed if either (a) or (b) is true for
    # this set of titles.
    
    $DEBUG{ticmp} = 1;
    
    my @diff = sdiff(\@words_a, \@words_b);
    
    my (@extra_a, @extra_b, @suffix_a, @suffix_b, $misspell_count, $min_words);
    
    my $sim = 100;
    my $con = 0;
    
    # Go through the diff segments one by one. The value of $op generated by the sdiff subroutine
    # from Algorithm::Diff is as follows:
    # 
    #   'u' means that both lists have the same word at this position.
    #   'c' means that the lists differ at this position.
    #   '-' means that list a has an insertion (or list b has a deletion).
    #   '+' means that list b has an insertion (or list a has a deletion).
    
    while ( @diff )
    {
	my $segment = shift @diff;
	
	my ($op, $word_a, $word_b) = $segment->@*;
	
	# Any op other than a '+' means that any accumulated @suffix_b must be moved to @extra_b
	# because it is not actually a suffix. The greatest-common-subsequence algorithm used by
	# Algorithm::Diff ensures that.
	
	if ( @suffix_b && $op ne '+' )
	{
	    push @extra_b, @suffix_b;
	    @suffix_b = ();
	}
	
	# Likewise, any op other than a '-' means that any accumulated @suffix_a must be
	# moved to @extra_a because it is not actually a suffix.
	
	if ( @suffix_a && $op ne '-' )
	{
	    push @extra_a, @suffix_a;
	    @suffix_a = ();
	}
	
	# Words that are unchanged between the two lists are ignored.
	
	next if $op eq 'u';
	
	# Words that are changed but with a very short edit distance are considered to be possibly
	# identical. We keep track of how many there are. Words that are very different go on
	# the difference lists.
	
	if ( $op eq 'c' )
	{
	    my $len_a = length($word_a);
	    my $len_b = length($word_b);
	    
	    # If the two words are close in length, compute their edit distance. If the edit
	    # distance is small, increment the misspell count and go on to the next segment.
	    
	    if ( abs($len_a - $len_b) < 3 )
	    {
		my $edist = edistance($word_a, $word_b, 3);
		
		# The allowable edit distance varies depending on the length of the shorter word.
		
		my $len = min_score($len_a, $len_b);
		
		my $max_distance = $len > 9 ? 3
				 : $len > 5 ? 2
					    : 1;
		
		# If the edit distance is within the allowable range, increment the misspelling
		# count and go on to the next segment.
		
		if ( $edist >= 0 && $edist <= $max_distance )
		{
		    $misspell_count++;
		    next;
		}
	    }
	    
	    # Otherwise, check to see if the larger of the two words is similar to the smaller one
	    # concatenated with one or more subsequent words from its sequence.
	    
	    elsif ( $len_a > $len_b )
	    {
		# If the the next segment is a sequence b insertion and the sequence a word is
		# longer than the sequence b word plus the next word following, with a margin of 2
		# for possible entry mistakes, then do a more thorough test using the
		# match_squashed subroutine. If that test is positive, then skip to the next
		# segment. The subroutine removes the segments corresponding to the extra words in
		# sequence b, so we don't need to deal with them here.
		
		if ( $diff[0][0] eq '+' && $len_a > $len_b + length($diff[0][2]) - 2 )
		{
		    my $edist = match_squashed_word($word_a, $word_b, 'b', \@diff);
		    
		    if ( $edist >= 0 && $edist < 4 )
		    {
			$DEBUG{tisqwg}++;
			$misspell_count++ if $edist;
			next;
		    }
		    
		    else
		    {
			$DEBUG{tisqwb}++;
		    }
		}
	    }
	    
	    elsif ( $len_b > $len_a )
	    {
		# Similarly, if the the next segment is a sequence a insertion and the sequence b
		# word is longer than the sequence a word plus the next word following, with a
		# margin of 2 for possible entry mistakes, then do a more thorough test using the
		# match_squashed subroutine. If that test is positive, then skip to the next
		# segment. The subroutine removes the segments corresponding to the extra words in
		# sequence a, so we don't need to deal with them here.
		
		if ( $diff[0][0] eq '-' && $len_b > $len_a + length($diff[0][1]) - 2 )
		{
		    my $edist = match_squashed_word($word_b, $word_a, 'a', \@diff);

		    if ( $edist > 0 && $edist < 4 )
		    {
			$DEBUG{tisqwg}++;
			$misspell_count if $edist;
			next;
		    }
		    
		    else
		    {
			$DEBUG{tisqwb}++;
		    }
		}
	    }
	    
	    # Otherwise, both words go on the word difference lists.
	    
	    push @extra_a, $word_a;
	    push @extra_b, $word_b;
	}
	
	# Words that are insertions are also potential suffix words. If they aren't actually
	# suffixes they will be moved to the corresponding extra list on a subsequent iteration.
	
	elsif ( $op eq '-' )
	{
	    push @suffix_a, $word_a;
	}
	
	elsif ( $op eq '+' )
	{
	    push @suffix_b, $word_b;
	}
    }
    
    # Now that we have gone through the entire sequence, consider the suffixes if any. If one of
    # the titles has a suffix then drop it if the other title has at least 4 words and the suffix
    # length is less than half of the title length. As noted above, it is a common entry error for
    # the last part of a title to be left off, and also common for extra cruft to be added at the
    # end of a title. As the above code is written, it is impossible for both titles to have a suffix.
    
    if ( @suffix_a || @suffix_b )
    {
	$DEBUG{tisfx}++;
	
	# Compute the number of words in the shorter title, which will almost always be the one
	# that doesn't have a suffix.
	
	my $length_a = scalar(@words_a) - scalar(@suffix_a);
	my $length_b = scalar(@words_b) - scalar(@suffix_b);
	
	$min_words = min_score($length_a, $length_b);
	
	# Deduct similarity points if the shorter title has three or fewer words.
	
	if ( $min_words < 4 )
	{
	    $sim = $sim - 15;
	}
	
	# Deduct similarity points if the suffix is more than half the length of the remaining
	# title.
	
	if ( scalar(@suffix_a) * 2 >= $length_a ||
	     scalar(@suffix_b) * 2 >= $length_b )
	{
	    $sim = $sim - 15;
	}
    }

    else
    {
	$min_words = min_score(scalar(@words_a), scalar(@words_b));
    }
    
    # For a long title, if there are more than more than 4 words in one title but not the other
    # then return a conflict. For shorter titles, the threshold is smaller.
    
    my $threshold = $min_words > 10 ? 4
		  : $min_words > 6  ? 3
	          : $min_words > 4  ? 2
	          :                   1;
    
    if ( @extra_a > $threshold || @extra_b > $threshold )
    {
	$DEBUG{tiexwf}++;
	return (0, 100);
    }
    
    # Otherwise, deduct 10 points for each distinct word that doesn't appear in the other title
    # and 3 points for each misspelling. These points are also added to the conflict score, since
    # they provide evidence that the titles may not be the same.
    
    my $distinct_count = scalar(@extra_a) + scalar(@extra_b);
    
    my $sim = $sim - 10 * $distinct_count - 3 * $misspell_count;
    my $con = $con + 10 * $distinct_count + 3 * $misspell_count;
    
    if ( $sim <= 0 )
    {
	$DEBUG{ticmpf} = 1;
	return (0, 100);
    }
    
    else
    {
	$DEBUG{ticmps} = 1;
	return ($sim, $con);
    }
}


# title_words ( title )
#
# Perform the same normalization procedure as title_similarity above on the specified
# string. Return a list of words in foldcase without whitespace, punctuation, or accent
# marks.

sub title_words {

    my ($title, $keep_accents) = @_;
    
    # Start by removing any HTML tags that might be in the title.
    
    $title =~ s{ &lt; .*? &gt; | < .*? > }{}xsg;
    
    # Continue by removing accent marks and then splitting each title into words. Any
    # sequence of letters and numbers counts as a word, and any sequences of other
    # characters (punctuation and spacing) counts as a word boundary. Stopwords are also
    # removed, since they occasionally get left out of a title by mistake. All of the
    # words are put into foldcase.
    
    $title =~ s/\pM//g unless $keep_accents;
    
    return grep { ! $STOPWORD{$_} } map { fc } split /[^\pL\pN\pM]+/, $title;
}


# check_for_section_prefix ( title )
#
# If the argument contains the sequence ', in ' then it may start with a section heading
# prefix. Split everything up to the match into words in the same way that
# title_similarity() does, and return the number of words. A return value of 0 means that
# no such prefix was found.

sub check_for_section_prefix {
    
    # If we find a the sequence ", in " in the original title string, we have a possible
    # section heading prefix. Split everything before the match into words in the same way
    # as the title is split, and count them. We ignore the word 'in' because it is a
    # stopword.
    
    if ( $_[0] =~ qr{ ^ (.*) , \s in \s \S }xsi )
    {
	my $prefix = $1;
	my @prefix_words = grep { ! $STOPWORD{$_} } split /[^\pL\pN]+/, $prefix;
	
	return scalar(@prefix_words);
    }
    
    # Otherwise, return zero.
    
    return 0;
}


# prefix_match ( words_a, words_b, skip_a, skip_b )
#
# If the shorter of the two sequences is a prefix of the longer, then return the shorter
# length (which is the number of matching words). If either of the 'skip' parameters have
# a nonzero value, skip that number of words at the start of the corresponding sequence. A
# return value of 0 means that neither of the two sequences is a prefix of the other.

sub prefix_match {

    my ($words_ref_a, $words_ref_b, $skip_a, $skip_b) = @_;

    $skip_a ||= 0; $skip_b ||= 0;
    
    my $shorter_length = min_score(scalar(@$words_ref_a) - $skip_a,
				   scalar(@$words_ref_b) - $skip_b);
    
    my $i;
    
    for ($i=0; $i<$shorter_length; $i++ )
    {
	return 0 if $words_ref_a->[$i + $skip_a] ne $words_ref_b->[$i + $skip_b];
    }
    
    return $i;
}


# subset_match ( words_a, words_b )
#
# If one of the two sequences is a subset of the other, return the length of that sequence. In
# other words, the result is true if every word in one of the two sequences is also in the other.

sub subset_match {
    
    my ($words_ref_a, $words_ref_b) = @_;
    
    my ($matches_a, $matches_b);
    
  WORD_A:
    for (my $i=0; $i<scalar(@$words_ref_a); $i++)
    {
	for (my $j=0; $j<scalar(@$words_ref_b); $j++)
	{
	    if ( $words_ref_a->[$i] eq $words_ref_b->[$j] )
	    {
		$matches_a++;
		next WORD_A;
	    }
	}
    }
    
    if ( $matches_a == scalar(@$words_ref_a) )
    {
	return $matches_a;
    }
    
  WORD_B:
    for (my $i=0; $i<scalar(@$words_ref_b); $i++)
    {
	for (my $j=0; $j<scalar(@$words_ref_a); $j++)
	{
	    if ( $words_ref_b->[$i] eq $words_ref_a->[$j] )
	    {
		$matches_b++;
		next WORD_B;
	    }
	}
    }
    
    if ( $matches_b == scalar(@$words_ref_b) )
    {
	return $matches_b;
    }
    
    else
    {
	return 0;
    }
}


# initial_words_match ( words_a, words_b, span, min )
# 
# Count how many words in the initial sequences of words_a and words_b also appear in the other
# initial sequence. The initial sequence length is given by `span`. Return true if we find at
# least `min` matches.

sub initial_words_match {

    my ($words_ref_a, $words_ref_b, $span, $min) = @_;
    
    my ($match_count, $i, $j);
    
    for ($i=0; $i<$span; $i++)
    {
	for ($j=0; $j<$span; $j++)
	{
	    if ( $words_ref_a->[$i] eq $words_ref_b->[$j] )
	    {
		$match_count++;
		return 1 if $match_count >= $min;
	    }
	}
    }

    return 0;
}


# match_squashed ( longer, shorter, seq_select, segments_ref )
#
# If the longer word matches the shorter one concatenated with one or more of the following words
# from its sequence, then remove those words from the segment list (which is pointed to by
# $segments_ref) and return true. The $seq_select parameter will be 'b' if the shorter word was
# from sequence b, 'a' if the shorter word was from sequence a.
#
# Returns the edit distance between the two if it is small, 100 otherwise.

sub match_squashed_word {

    my ($longer, $shorter, $seq_select, $segments_ref) = @_;
    
    # First see if the shorter word is a prefix of the longer, or a very close match. Accept it if
    # the Levenshtein-Damerau edit distance is 2 or less. Otherwise, return a rejection.
    
    my $reduced = substr($longer, 0, length($shorter));
    
    my $edist = edistance($reduced, $shorter, 2);
    
    return 100 unless $edist >= 0 && $edist <= 2;
    
    # Now start with $shorter and add subsequent words until the length is close to or exceeds the
    # length of $longer. Put an overall limit on the number of additional words to catch runaway
    # loops.
    
    my $op_select = $seq_select eq 'a' ? '-' : '+';
    my $word_select = $seq_select eq 'a' ? 1 : 2;
    
    my $words_ahead = 0;
    my $concatenated = $shorter;
    
    while ( $words_ahead < 10 && $segments_ref->[$words_ahead] &&
	    $segments_ref->[$words_ahead][0] eq $op_select &&
	    length($concatenated) < length($longer) - 2 )
    {
	$concatenated .= $segments_ref->[$words_ahead][$word_select];
	$words_ahead++;
    }
    
    # If the length is comparable, check the edit distance.

    if ( abs( length($concatenated) - length($longer) ) < 3 )
    {
	$edist = edistance($concatenated, $longer, 3);
	
	# The allowable edit distance varies depending on the length of the shorter word.
	
	my $len = min_score($concatenated, $longer);
	
	my $max = $len > 9 ? 3
	    : $len > 5 ? 2
	    : 1;
	
	# If the edit distance is within the allowable range, return it and remove all the
	# segments corresponding to the concatenated words.
	
	if ( $edist >= 0 && $edist <= $max )
	{
	    splice(@$segments_ref, 0, $words_ahead);
	    return $edist;
	}
    }
    
    # Otherwise, return a rejection.

    return 100;
}


# short_similarity ( pubtitle_a, pubtitle_b )
# 
# Compute a fuzzy match score between $pubtitle_a and $pubtitle_b, doing our best to allow for
# both mistyping and leaving out a word or two. This is a much simpler function than
# title_similarity. It is used for both publication/container titles and publisher names.

sub short_similarity {

    my ($title_a, $title_b) = @_;
    
    $COUNT{sh_}++;
    
    # If one or both arguments are empty, return (0, 0). In that case we do not have enough
    # information to tell us that the records are similar, nor that they are dissimilar. We
    # consider a title to be empty if it doesn't have at least 3 alphabetic characters in a
    # row. The letters may have marks in between.
    
    unless ( $title_a && $title_a =~ $THREE_LETTERS &&
	     $title_b && $title_b =~ $THREE_LETTERS )
    {
	$DEBUG{shemp}++;
	return (0, 0);
    }
    
    # If the two titles are equal when case and accents are ignored, return (100, 0). We ignore
    # accent marks because these are often mistyped or omitted.
    
    if ( eq_insensitive($title_a, $title_b) )
    {
	$DEBUG{sheqs}++;
	return (100, 0);
    }
    
    # Otherwise, start by removing HTML tags, which occasionally appear in Crossref titles.
    
    $title_a =~ s{ &lt; .*? &gt; | < .*? > }{}xsg;
    $title_b =~ s{ &lt; .*? &gt; | < .*? > }{}xsg;

    # Continue by removing accent marks and then splitting each title into words. Any sequence of
    # letters and numbers counts as a word, and any sequences of other characters (punctuation and
    # spacing) counts as a word boundary. Stopwords are also removed, since they occasionally get
    # left out of a title by mistake.
    
    $title_a =~ s/\pM//g;
    $title_b =~ s/\pM//g;
    
    my @words_a = grep { ! $STOPWORD{$_} } map { fc } split /[^\pL\pN]+/, $title_a;
    my @words_b = grep { ! $STOPWORD{$_} } map { fc } split /[^\pL\pN]+/, $title_b;
    
    my $longer_wc = max_score(scalar(@words_a), scalar(@words_b));
    
    # If one of the titles is a prefix of the other, return a match.
    
    if ( my $shorter_wc = prefix_match(\@words_a, \@words_b) )
    {
	$DEBUG{shpres}++;
	
	if ( $longer_wc == $shorter_wc || $shorter_wc > 2 && $longer_wc < $shorter_wc * 2 )
	{
	    return (100, 0);
	}
	
	elsif ( $shorter_wc > 1 )
	{
	    return (70, 0);
	}
	
	else
	{
	    return (50, 0);
	}
    }
    
    # If every word in one title is also in the other title, return a weaker match.
    
    elsif ( my $shorter_wc = subset_match(\@words_a, \@words_b) )
    {
	$DEBUG{shsubs}++;
	
	if ( $longer_wc = $shorter_wc || $shorter_wc > 2 && $longer_wc < $shorter_wc * 2 )
	{
	    return (80, 0);
	}

	else
	{
	    return (50, 0);
	}
    }
    
    # Otherwise, return a conflict.
    
    else
    {
	$DEBUG{shcmpf}++;
	return (0, 100);
    }
}


# title_multimatch ( titles_a, subtitles_a, titles_b, subtitles_b )
# 
# This subroutine takes two lists of titles and two lists of subtitles, and tries all combinations
# of the titles using the subtitles as appropriate. The highest similarity score is returned.

sub title_multimatch {
    
    my ($titles_a, $subtitles_a, $titles_b, $subtitles_b, $type) = @_;
    
    my $max_similarity = undef;
    my $same_conflict = undef;
    
    # Iterate through the 'a' titles.
    
    foreach my $a ( 0..$titles_a->$#* )
    {
	# Iterate thorugh the 'b' titles.
	
	foreach my $b ( 0..$titles_b->$#* )
	{
	    my ($sim, $con);
	    
	    # Pull out the two titles.

	    my $ta = $titles_a->[$a];
	    my $tb = $titles_b->[$b];
	    
	    # If one of the titles contains a : and the other does not but has a subtitle, add the
	    # subtitle. If neither title contains a : then ignore any subtitles and match the main
	    # titles only.

	    if ( $subtitles_b && $ta =~ /:/ && $tb !~ /:/ && $subtitles_b->[$b] )
	    {
		$tb = "$tb: $subtitles_b->[$b]";
	    }

	    elsif ( $subtitles_a && $tb =~ /:/ && $tb !~ /:/ && $subtitles_a->[$a] )
	    {
		$ta = "$ta: $subtitles_a->[$a]";
	    }

	    # Compute the similarity and conflict scores.

	    if ( $type eq 'pub' )
	    {
		($sim, $con) = short_similarity($ta, $tb);
	    }
	    
	    else
	    {
		($sim, $con) = title_similarity($ta, $tb);
	    }
	    
	    # If the similarity score is higher than the current maximum, keep the new similarity
	    # score and the corresponding conflict score.

	    if ( ! defined $max_similarity || $sim > $max_similarity )
	    {
		$max_similarity = $sim;
		$same_conflict = $con;
	    }
	}
    }

    return ($max_similarity, $same_conflict);
}


# author_similarity ( last_a, first_a, last_b, first_b )
# 
# Compute a fuzzy match score between author names a and b, doing our best to allow for
# mistyping and missing accent marks. This routine returns a list of two numbers. The first
# is a similarity score, normalized to 0-100 with 100 representing equality (case and accent marks
# are ignored). The second is a conflict score, also normalized to 0-100 with 100 representing no
# possibility that these values were originally the same.
#
# IMPORTANT: In order for this function to return proper results, all arguments must be in Unicode
# Normalization Form D.


our $FIRST_WORD = qr{ ^ \PL* ( \pL\pM* [\pL\pN] [\pL\pN\pM]* ) }xs;

our $FIRST_INITIAL = qr{ (\pL) }xs;

our $SECOND_INITIAL = qr{ ^ \PL* \pL .*? [^\pL\pM\pN] .*? (\pL) }xs;

our $LAST_TWO_LETTERS = qr{ (\pL) \PL* (\pL) \PL* $ }xs;


sub author_similarity {
    
    my ($last_a, $first_a, $last_b, $first_b) = @_;
    
    my $sim = 0;
    my $con = 0;
    my $try_LD;

    $COUNT{au_}++;
    
    # If one or both last names are empty, return (0, 0). There is not enough information to
    # conclude that the names are similar, nor enough to conclude that they are dissimilar. A last
    # name is considered to be empty if it doesn't contain at least one letter.
    
    return (0, 0) unless $last_a && $last_a =~ /[\pL]/ &&
	$last_b && $last_b =~ /[\pL]/;
    
    # If either last name has no more than one letter in a row while the corresponding first name
    # has more than one, that means the two were accidentally entered into the wrong fields. So
    # swap them. When counting letters, we skip over any marks (but not punctuation) that may
    # occur between them.
    
    if ( $last_a !~ /\pL\pM*\pL/ && $first_a && $first_a =~ /\pL\pM*\pL/ )
    {
	my $temp = $last_a;
	$last_a = $first_a;
	$first_a = $temp;
	$DEBUG{auswap}++;
    }
    
    if ( $last_b !~ /\pL\pM*\pL/ && $first_b && $first_b =~ /\pL\pM*\pL/ )
    {
	my $temp = $last_b;
	$last_b = $first_b;
	$first_b = $temp;
	$DEBUG{auswap}++;
    }
    
    # Now extract the first letter of each first and last name and convert them into foldcase. We
    # will use these during the comparison algorithm below.
    
    my ($first_init_a) = map { fc($_) } $first_a =~ $FIRST_INITIAL;
    my ($first_init_b) = map { fc($_) } $first_b =~ $FIRST_INITIAL;

    my ($last_init_a) = map { fc($_) } $last_a =~ $FIRST_INITIAL;
    my ($last_init_b) = map { fc($_) } $last_b =~ $FIRST_INITIAL;

    # Similarity check #1:
    # 
    # If the first letters of $first_a and $last_a cross-match the first letters of $first_b and
    # $last_b, it be that one of the data enterers entered the first and last names in the wrong
    # order. But we can only make that determination if the two pairs of letters differ.
    
    if ( $first_init_a eq $last_init_b && $first_init_b eq $last_init_a &&
	 $first_init_a ne $last_init_a )
    {
	# If either first name is given in full (not as initials) and if its first full word
	# matches the first full word of the other last name, return a similarity of 100. We
	# might have a very occasional false positive, but most of these will be correct matches.

	my ($firstname_word_a) = $first_a =~ $FIRST_WORD;
	my ($lastname_word_a) = $last_a =~ $FIRST_WORD;
	my ($firstname_word_b) = $first_b =~ $FIRST_WORD;
	my ($lastname_word_b) = $last_b =~ $FIRST_WORD;
	
	if ( $firstname_word_a && $lastname_word_b &&
	     eq_insensitive($firstname_word_a, $lastname_word_b) )
	{
	    $DEBUG{aucrms}++;
	    return (100, 0);
	}
	
	if ( $firstname_word_b && $lastname_word_a &&
	     eq_insensitive($firstname_word_b, $lastname_word_a) )
	{
	    $DEBUG{aucrms}++;
	    return (100, 0);
	}
	
	# If both first names are given as initials, the match cannot be confirmed absolutely. But
	# it has some chance of being correct, so return similarity of 80 if there are two initials
	# and they both match up, or a similarity of 50 otherwise.
	
	if ( $first_a =~ qr{ ^ \pM*\pL\pM* \b }xs && $first_b =~ qr{ ^ \pM*\pL\pM* \b }xs )
	{
	    $DEBUG{aucrmp}++;
	    
	    my ($first2_a) = $first_a =~ $SECOND_INITIAL;
	    my ($last2_b) = $last_b =~ $SECOND_INITIAL;
	    
	    if ( $first2_a && fc($first2_a) eq fc($last2_b) )
	    {
		return (80, 0);
	    }
	    
	    my ($first2_b) = $first_b =~ $SECOND_INITIAL;
	    my ($last2_a) = $last_a =~ $SECOND_INITIAL;
	    
	    if ( $first2_b && fc($first2_b) eq fc($last2_a) )
	    {
		return (80, 0);
	    }
	    
	    return (50, 0);
	}
	
	# Otherwise, fall through and apply the normal matching rules.
	
	$DEBUG{aucrmf}++;
    }
    
    # Split the two last names up into words, by which we mean sequences of letters, numbers, and
    # marks. All whitespace and punctuation is ignored.
    
    my @words_a = $last_a =~ /([\pL\pN\pM]+)/g;
    my @words_b = $last_b =~ /([\pL\pN\pM]+)/g;
    
    # If either name has spurious intials at the end, discard them. A final initial is a letter
    # possibly followed by some marks.
    
    while ( $words_a[-1] =~ qr{ ^ \pL\pM* $ }xs )
    {
	pop @words_a;
    }
    
    while ( $words_b[-1] =~ qr{ ^ \pL\pM* $ }xs )
    {
	pop @words_b;
    }
    
    # If the last word of either name is 'jr', 'ii', etc. then discard it. These are sometimes
    # left off by data enterers, so it makes sense to just ignore them.
    
    pop @words_a if $words_a[-1] =~ qr{ ^ (?: jr|ii|iii|iv|2nd|3rd|4th ) $ }xs;
    pop @words_b if $words_b[-1] =~ qr{ ^ (?: jr|ii|iii|iv|2nd|3rd|4th ) $ }xs;
    
    # If the two last names have different numbers of words, we construct two different
    # sequences: one that drops words from the end of the longer one to even them out, and one
    # that drops words from the beginning of the longer one to even them out. We see both patterns
    # in mis-entered names.
    
    my ($last_initial_a, $last_initial_b,
	$last_final_a, $last_final_b,
	$lf_init_a, $lf_init_b,
	$different_lengths);
    
    if ( @words_a > @words_b )
    {
	$different_lengths = 1;
	my $count = @words_b;
	$last_initial_a = join(' ', @words_a[0..$count-1]);
	$last_final_a = join(' ', @words_a[-$count..-1]);
	$last_initial_b = $last_final_b = join(' ', @words_b);
    }

    elsif ( @words_b > @words_a )
    {
	$different_lengths = 1;
	my $count = @words_a;
	$last_initial_b = join(' ', @words_b[0..$count-1]);
	$last_final_b = join(' ', @words_b[-$count..-1]);
	$last_initial_a = $last_final_a = join(' ', @words_a);
    }
    
    else
    {
	$last_initial_a = join(' ', @words_a);
	$last_initial_b = join(' ', @words_b);
    }
    
    # Both sequences are then cleaned of all non-letter characters and converted into
    # foldcase. This allows them to be compared on a solely alphabetic basis.
    
    $last_initial_a = fc($last_initial_a =~ s/\PL//gr);
    $last_initial_b = fc($last_initial_b =~ s/\PL//gr);

    if ( $different_lengths )
    {
	$last_final_a = fc($last_final_a =~ s/\PL//gr);
	$last_final_b = fc($last_final_b =~ s/\PL//gr);
	$lf_init_a = substr($last_final_a, 0, 1);
	$lf_init_b = substr($last_final_b, 0, 1);
    }	
    
    # Similarity check #2:
    # 
    # If the last names are equal when case and accents are ignored, the base similarity is
    # 100. First compare the the initial sequences, which are the entire names in cases where the
    # number of words are the same. If the number of words is different, compare the final
    # sequences too. If either one matches, the names are similar.
    
    if ( $last_initial_a eq $last_initial_b ||
	 $different_lengths && $last_final_a eq $last_final_b )
    {
	$sim = 100;
	if ( $different_lengths ) { $DEBUG{ausubs}++ }
	else{ $DEBUG{aueqs}++ }
    }
    
    # Similarity check #3:
    #
    # If the last names do not match exactly, there is a chance that one or both have been entered
    # with misspellings. We can catch cases with just one or two misspellings by using the
    # Levenshtein-Damerau algorithm to compare the edit distance between the two names. This can
    # be somewhat slow, so we only do it if *either* the first letters of the two names match or
    # the last two letters of the two names match.
    
    elsif ( $last_init_a eq $last_init_b ||
	    substr($last_initial_a,-2,2) eq substr($last_initial_b,-2,2) ||
	    $different_lengths && $lf_init_a eq $lf_init_b ||
	    $different_lengths && substr($last_final_a,-2,2) eq substr($last_final_b,-2,2) )
    {
	if ( $different_lengths) { $DEBUG{aufmsub}++ }
	else { $DEBUG{aufmeq}++ }
	
	my $max = 2;
	my $lnthr = 8;
	my $long_names = length($last_initial_a) >= $lnthr && length($last_initial_b) >= $lnthr;
	my $distance = edistance( $last_initial_a, $last_initial_b, $max );
	
	# If the distance was too large and the two names are of different lengths, try the final
	# sequences too.
	
	if ( $different_lengths && ( $distance < 0 || $distance > $max ) )
	{
	    $long_names = length($last_final_a) >= $lnthr && length($last_final_b) >= $lnthr;
	    $distance = edistance( $last_final_a, $last_final_b, $max );
	}
	
	# If the edit distance between the names is 1, the base similarity is 85 with a conflict
	# of 15.
	
	if ( $distance == 1 )
	{
	    $DEBUG{audst1}++;
	    $sim = 85;
	    $con = 15;
	}
	
	# If the edit distance between the names is 2 and both names exceed the threshold length,
	# the base similarity is 70 with a conflict of 30.
	
	elsif ( $distance == 2 && $long_names )
	{
	    $DEBUG{audst2}++;
	    $sim = 70;
	    $con = 30;
	}
	
	# Otherwise, return a conflict of 100.
	
	else
	{
	    $DEBUG{audstf}++;
	    return (0, 100);
	}
    }
    
    # If the two names weren't equal and we didn't try the fuzzy match at all, return a conflict
    # of 100.
    
    else
    {
	$DEBUG{aufmf}++;
	return (0, 100);
    }
    
    # If the two last names have different numbers of words, deduct 20 points from the similarity.
    
    if ( $different_lengths )
    {
	$sim = $sim - 20;
    }

    # Add a 20 point conflict if the first initials are different.
    
    if ( $first_init_a && $first_init_b && $first_init_a ne $first_init_b )
    {
	$sim = $sim - 20;
	$con = $con + 20;
    }
    
    # If one first initial is empty and the other is not, deduct 10 points.
    
    elsif ( $first_init_a && ! $first_init_b || $first_init_b && ! $first_init_a )
    {
	$sim = $sim - 10;
    }

    # Return the similarity and conflict scores, making sure they are between 0 and 100.
    
    $sim = 0 if $sim < 0;
    $con = 100 if $con > 100;
    
    return (max_score($sim, 0), min_score($con, 100));
}


# match_names ( last, first, name... )
# 
# Return the best similarity score and associated conflict score for the specified last and first
# name when matched against the subsequent list of names. The third return value gives the
# position of the matching name in the list, starting with 1. If no name matches, the returned
# position is zero.
# 
# The third and subsequent arguments may be either an array of [last, first], or else the last and
# first names will be extracted by parse_authorname(). All of the argument values must be
# normalized to Unicode NFD before being passed in. Any results returned by get_authorname and
# get_authorlist are normalized, and can be passed as they are. Otherwise, clean_string() can be
# used.
#
# The author_similarity procedure is run only for pairs of names where the first initials are
# equal. This considerably speeds up the execution of this subroutine, but may cause a small
# number of false negatives.

sub match_names {
    
    my ($last, $first, @match_list) = @_;
    
    # Extract the initial letters from the first and last names. These will be used to speed up
    # the search.
    
    my ($last_init) = $last =~ $FIRST_INITIAL;
    my ($first_init) = $first =~ $FIRST_INITIAL;
    
    # Iterate through the entries to be matched against, looking for the best match. Start by
    # checking the initial letters, and only run the full similarity procedure if they match.
    
    my $selected_similarity = 0;
    my $selected_conflict = 0;
    my $selected_pos = 0;
    
    for ( my $i=0; $i<@match_list; $i++ )
    {
	my ($last_i, $first_i);
	
	# If the entry is an array, pull out the two name components. Otherwise, use
	# parse_authorname().
	
	if ( ref $match_list[$i] eq 'ARRAY' )
	{
	    ($last_i, $first_i) = $match_list[$i]->@[0..1];
	}

	else
	{
	    ($last_i, $first_i) = parse_authorname($match_list[$i]);
	}
	
	# Extract the initial letters, and compare them to the initial letters of the name we are
	# trying to match. The last initials must be the same and must not be empty. The first
	# initials must either be the same or both be empty.
	
	my ($last_init_i) = $last_i =~ $FIRST_INITIAL;
	my ($first_init_i) = $first_i =~ $FIRST_INITIAL;
	
	my $straight_match = $last_init && $last_init_i &&
			     $last_init eq $last_init_i &&
			     $first_init eq $first_init_i;
	
	# If the first and last initials are both present and are different, also check for a
	# cross-match between the two names. This is necessary because East Asian names are
	# sometimes incorrectly entered with the first and last switched. One entry might have the
	# two names entered correctly while the other has them switched. If the first and last
	# initials are the same we cannot do this, which will cause a small number of false
	# negatives.
	
	my $cross_match = $last_init && $last_init_i &&
			  $first_init && $first_init_i &&
			  $last_init ne $first_init &&
			  $last_init eq $first_init_i &&
			  $first_init eq $last_init_i;

	# If we get either kind of match, run the full similarity algorithm on this pair of
	# names. If the similarity is better than any we have found so far, select these numbers.
	
	if ( $straight_match || $cross_match )
	{
	    my ($similarity, $conflict) = author_similarity($last, $first, $last_i, $first_i);
	    
	    if ( $similarity > $selected_similarity )
	    {
		$selected_similarity = $similarity;
		$selected_conflict = $conflict;
		$selected_pos = $i + 1;
	    }
	}
    }
    
    return ($selected_similarity, $selected_conflict, $selected_pos);
}


# vol_similarity ( vol_a, issue_a, vol_b, issue_b )
#
# Compute a fuzzy match score between the volume and issue numbers of the two references. We also
# use this routine to compute a fuzzy match score between the first and last page numbers.

sub vol_similarity {

    my ($vol_a, $issue_a, $vol_b, $issue_b) = @_;

    $vol_a ||= '';
    $vol_b ||= '';
    $issue_a ||= '';
    $issue_b ||= '';
    
    # If either pair of attributes is empty, return (0, 0). We do not have enough information to
    # judge similarity or difference.
    
    if ( $vol_a eq '' && $issue_a eq '' || $vol_b eq '' && $issue_b eq '' )
    {
	return (0, 0);
    }

    # If the two pairs of attributes are equal and at least one is non-empty, return (100, 0).
    
    elsif ( $vol_a eq $vol_b && $issue_a eq $issue_b && ($vol_a || $issue_a) )
    {
	return (100, 0);
    }
    
    # If just one pair is equal and non-empty but the other differs, return (70, 30).
    
    elsif ( $vol_a eq $vol_b && $vol_a || $issue_a eq $issue_b && $issue_a )
    {
	return (70, 30);
    }
    
    # Otherwise, return (0, 100);

    else
    {
	return (0, 100);
    }
}


# pubyr_similarity ( pubyr_a, pubyr_b )
# 
# Compute a fuzzy match score between pubyr_a and pubyr_b. Publication years are rarely mistyped
# (at least this is my hypothesis) since they are so important. For now, we return either 100 or
# 0, nothing in between.

sub pubyr_similarity {

    my ($pubyr_a, $pubyr_b) = @_;
    
    # If one or both arguments is empty, we return (0, 0). In that case we do not have enough
    # information to tell us that the records are similar, nor that they are dissimilar.

    return (0, 0) unless $pubyr_a && $pubyr_b;

    # If both arguments are 4-digit years that differ by 1, return (70, 30). Apparently,
    # off-by-one errors are not uncommon. This may have to do with confusion between
    # pre-publication and actual publication dates, but I am not certain. In any case, it does
    # occur in a non-trivial number of cases that are otherwise obvious matches.
    
    if ( $pubyr_a =~ /^\d\d\d\d$/ && $pubyr_b =~ /^\d\d\d\d$/ )
    {
	return (70, 30) if $pubyr_a - $pubyr_b == 1 || $pubyr_b - $pubyr_a == 1;
    }
    
    # Otherwise, we return (100, 0) if the two values are equal, and (0, 100) if they are not.

    return $pubyr_a eq $pubyr_b ? (100, 0) : (0, 100);
}


sub eq_insensitive {

    return undef unless defined $_[0] && defined $_[1];
    return $IgnoreCaseAccents->eq($_[0], $_[1]);
}


sub max_score {

    my ($a, $b) = @_;

    return $a >= $b ? $a : $b;
}


sub min_score {

    my ($a, $b) = @_;

    return $a >= $b ? $b : $a;
}


1;

