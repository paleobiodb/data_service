# The Paleobiology Database
# 
#   RefCheck.pm
# 
# Subroutines for working with external sources of bibliographic reference data.
# 

package ReferenceMatch;

use strict;

use feature 'unicode_strings';

use Unicode::Collate;
use Algorithm::Diff qw(sdiff);
use Text::Levenshtein::Damerau qw(edistance);
use Text::Transliterator::Unaccent;
use Encode;
use Carp qw(croak);
use Scalar::Util qw(reftype);

use Exporter 'import';

our (@SCORE_VARS) = qw(complete count sum title pub auth1 auth2 pubyr volume pages pblshr);

our (@EXPORT_OK) = qw(ref_similarity get_reftitle get_pubtitle get_authorname get_authorlist
		      split_authorlist parse_authorname get_pubyr @SCORE_VARS);

our $IgnoreCaseAccents = Unicode::Collate->new(
     level         => 1,
     normalization => undef);

our $Unaccent;


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
    
    # If the title similarity score is less than 100 and $title_a matches the pattern "..., in
    # ...", where the second sequence is reasonably long and starts with a capital letter,
    # recompute the similarity with the first sequence removed. If the shortened version matches
    # $title_b better, use those scores instead. The reason for this rule is that some of the
    # paleobiodb references have been entered in the form "Section Heading, in Work Title", and
    # this rule compensates for that.
    
    if ( $title_similar < 100 && $title_a =~ qr{ , \s+ in \s+ ([[:upper:]].+) $ }xs )
    {
	my $reduced_a = $1;
	
	if ( (length($reduced_a) > 20 && length($reduced_a) > length($title_b) - 20) ||
	     (length($reduced_a) > 40) )
	{
	    my ($reduced_similar, $reduced_conflict) = title_similarity($reduced_a, $title_b, 'work');
	    
	    if ( $reduced_similar > $title_similar )
	    {
		$title_similar = $reduced_similar;
		$title_conflict = $reduced_conflict;
		$title_a = $reduced_a;
	    }
	}
    }
    
    # If the title similarity score is still less than 100 and we also have one or more alternate
    # titles in either record, try matching all combinations and take the highest similarity score
    # we find.
    
    if ( $title_similar < 100 && (@title_alt_a || @title_alt_b) )
    {
	my ($multi_similar, $multi_conflict) =
	    title_multimatch( [$title_a, @title_alt_a], [$subtitle_a, @subtitle_alt_a],
			      [$title_b, @title_alt_b], [$subtitle_b, @subtitle_alt_b] );
	
	if ( $multi_similar > $title_similar )
	{
	    $title_similar = $multi_similar;
	    $title_conflict = $multi_conflict;
	}
    }
    
    # If the title similarity score is still less than 100 and we also have a subtitle in either
    # record, try matching all combinations of titles and subtitles and take the highest
    # similarity score we find.
    
    if ( $title_similar < 100 && ($subtitle_a || $subtitle_b) )
    {
	my ($with_subtitle_similar, $with_subtitle_conflict) =
	    title_multimatch( [$title_a, @title_alt_a, $subtitle_a, @subtitle_alt_a], undef,
			      [$title_b, @title_alt_b, $subtitle_b, @subtitle_alt_b], undef );

	if ( $with_subtitle_similar > $title_similar )
	{
	    $title_similar = $with_subtitle_similar;
	    $title_conflict = $with_subtitle_conflict;
	}
    }
    
    # Then compute the similarity and conflict scores for the publication titles.
    # ---------------------------------------------------------------------------
    
    my ($pub_a, @pub_alt_a) = get_pubtitle($r);
    my ($pub_b, @pub_alt_b) = get_pubtitle($m);
    
    my ($pub_similar, $pub_conflict) = title_similarity($pub_a, $pub_b, 'pub');
    
    # If the publication similarity score is less than 100 and we also have one or more alternate
    # publication titles in either record, try matching all combinations and take the highest
    # similarity score we find.
    
    if ( $pub_similar < 100 && (@pub_alt_a || @pub_alt_b) )
    {
	($pub_similar, $pub_conflict) =
	    title_multimatch( [$pub_a, @pub_alt_a], undef,
			      [$pub_b, @pub_alt_b], undef );
    }
    
    # Then compute the similarity and conflict scores for the first and second authors. For now,
    # we ignore the rest of the author list.
    # --------------------------------------
    
    my ($auth1last_a, $auth1first_a) = get_authorname($r, 1);
    my ($auth1last_b, $auth1first_b) = get_authorname($m, 1);
    
    my ($auth1_similar, $auth1_conflict) = author_similarity($auth1last_a, $auth1first_a,
							     $auth1last_b, $auth1first_b);
    
    my ($auth2last_a, $auth2first_a) = get_authorname($r, 2);
    my ($auth2last_b, $auth2first_b) = get_authorname($m, 2);
    
    my ($auth2_similar, $auth2_conflict) = author_similarity($auth2last_a, $auth2first_a,
							     $auth2last_b, $auth2first_b, 2);
    
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
	# to (0, 100), though it may be overridden below.
	
	else
	{
	    $auth2_similar = 0;
	    $auth2_conflict = 100;
	}
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
	$issue_a ||= $issue_b;
	$issue_b ||= $issue_a;
    }
    
    my ($vol_similar, $vol_conflict) = vol_similarity($vol_a, $issue_a, $vol_b, $issue_b);
    my ($pages_similar, $pages_conflict) = vol_similarity($fp_a, $lp_a, $fp_b, $lp_b);
    
    # Then compute similarity and conflict scores for the publisher.
    # --------------------------------------------------------------
    
    my $publisher_a = get_publisher($r);
    my $publisher_b = get_publisher($m);
    
    my ($publish_similar, $publish_conflict) = title_similarity($publisher_a, $publisher_b, 'pblshr');
    
    # Now we consider some special cases.
    # -----------------------------------
    
    # If the publication year is similar and at least one of
    # title and publication is similar and at least one of volume and pages is similar, the author
    # similarity may need to be overridden. We also require that at least one of these scores be a
    # perfect 100.
    
    if ( $pubyr_similar && ( $title_similar || $pub_similar ) &&
	 ( $vol_similar == 100 || $pages_similar ) &&
         ( $pubyr_similar == 100 || $title_similar == 100 || $pages_similar == 100 ) )
    {
	# If there are at least two authors but the first and second authors don't match, we check
	# to see if the first and second authors match each other in the opposite order. If so, we
	# assume this was a data entry mistake and set the author similarity scores to reflect the
	# cross match. In almost all such cases, both records actually represent the same work. We
	# might occasionally get false positives if these two authors published more than one work
	# together, but hopefully the additional similarity requirements in the clause above will
	# filter those out.
	
	if ( $auth2last_a && $auth2last_b && ! $auth1_similar && ! $auth2_similar )
	{
	    my ($a1b2_similar, $a1b2_conflict) =
		author_similarity($auth1last_a, $auth1first_a, $auth2last_b, $auth2first_b);
	    
	    if ( $a1b2_similar )
	    {
		my ($a2b1_similar, $a2b1_conflict) =
		    author_similarity($auth2last_a, $auth2first_a, $auth1last_b, $auth1first_b);
		
		if ( $a2b1_similar )
		{
		    if ( $a1b2_similar >= $a2b1_similar )
		    {
			$auth1_similar = $a1b2_similar;
			$auth2_similar = $a2b1_similar;
		    }

		    else
		    {
			$auth1_similar = $a2b1_similar;
			$auth2_similar = $a1b2_similar;
		    }
		    
		    $auth1_conflict = 100 - $auth1_similar;
		    $auth2_conflict = 100 - $auth2_similar;
		}
	    }
	}
	
	# If the first authors are similar but the second are not, check to see if each of the
	# second authors appears elsewhere in the other record's author list. If this is true for
	# both second authors, and if both records have the same number of authors, and given that
	# the similarities checked above also hold, we assume that one of the author lists was
	# entered incorrectly and that the two records do in fact refer to the same work.

	elsif ( $auth2last_a && $auth2last_b && $auth1_similar && ! $auth2_similar )
	{
	    my @authorlist_a = get_authorlist($r, 3);
	    my @authorlist_b = get_authorlist($m, 3);

	    my $found_a = 0;
	    my $found_b = 0;

	    # Search for author2_a in the authorlist of record b, starting at place 3. Stop if we
	    # find a name with a similarity score above 90, and otherwise store the maximum
	    # similarity score we find.
	    
	    foreach my $entry ( @authorlist_b )
	    {
		if ( my ($similarity) = author_similarity($auth2last_a, $auth2first_a,
							$entry->[0], $entry->[1]) )
		{
		    $found_a = $similarity if $similarity > $found_a;
		    last if $found_a > 90;
		}
	    }
	    
	    # Search for author2_b in the authorlist of record a, starting at place 3. Stop if we
	    # find a name with a similarity score above 90, and otherwise store the maximum
	    # similarity score we find.
	    
	    foreach my $entry ( @authorlist_a )
	    {
		if ( my ($similarity) = author_similarity($auth2last_b, $auth2first_b,
							$entry->[0], $entry->[1]) )
		{
		    $found_b = $similarity if $similarity > $found_b;
		    last if $found_b > 90;
		}
	    }
	    
	    # Set the author2 similarity score to the average of the two search results, and the
	    # conflict to the complement of that.
	    
	    $auth2_similar = int(($found_a + $found_b)/2);
	    $auth2_conflict = 100 - $auth2_similar;
	}
	
	# If the first authors are similar but one record has two or more authors while the other
	# lists only one author, it is quite possible that one of the two records was incorrectly
	# entered. Unfortunately, there are many records in both the Paleobiology Database and
	# Crossref in which only the first author is listed and subsequent authors are left
	# out. In this situation, given that the similarities checked above also hold, we assume
	# that the second and subsequent authors were simply left out of the other record.
	
	elsif ( $auth1_similar && ( $auth2last_a && ! $auth2last_b ||
				    $auth2last_b && ! $auth2last_a ) )
	{
	    $auth2_similar = 100;
	    $auth2_conflict = 0;
	}
    }
    
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
		   pblshr_s => $publish_similar, pblshr_c => $publish_conflict, };
    
    return $values;
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
    
    elsif ( ref $r->{'container-title'} eq 'ARRAY' & $r->{'container-title'}->@* )
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
	
	if ( length($first) / length($second) > 2 )
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
	    return (clean_string($r->{author1last} || ''), clean_string($r->{author1init} || ''));
	}
	
	elsif ( $index == 2 )
	{
	    return (clean_string($r->{author2last} || ''), clean_string($r->{author2init} || ''));
	}
	
	elsif ( $r->{otherauthors} )
	{
	    my @names = clean_string(split_authorlist($r->{otherauthors}));

	    if ( $index - 3 < @names )
	    {
		$selected = $names[$index - 3];
	    }
	}
    }
    
    # Otherwise, the authors will be listed under 'author'. Both BibJSON and Crossref use this
    # fieldname. If it is an array, select the specified element.
    
    if ( $r->{author} && ref $r->{author} eq 'ARRAY' )
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
	    
	    $selected = $entry;
	    last;
	}
    }
    
    # If the author list is a string, we split it into individual names and then select the
    # specified one.
    
    elsif ( $r->{author} && ! ref $r->{author} )
    {
	my @names = split_authorlist($r->{author});
	
	$selected = $names[$index];
    }

    # If we have selected a name, return its last and first components.

    if ( $selected )
    {
	return clean_string(parse_authorname($selected));
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
	    push @result, [ $r->{author1last}, ($r->{author1init} || '') ];
	}
	
	# If $index is 1 or 2, look at the author2 fields. If author2last is empty,
	# assume there is only one author.
	
	if ( $index <= 2 )
	{
	    return @result unless $r->{author2last};
	    push @result, [ $r->{author2last}, ($r->{author2init} || '') ];
	}
	
	# Then split up otherauthors. If $index is greater than 3, skip some of the resulting
	# entries.
	
	my @othernames = split_authorlist($r->{otherauthors});
	
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
		my @components = parse_authorname($entry);
		
		if ( $components[0] )
		{
		    push @result, \@components;
		}
	    }
	}
    }
    
    # If the 'author' field is a hashref, assume it represents a single name and parse it.

    elsif ( ref $r->{author} eq 'HASH' )
    {
	my @components = parse_authorname($r->{author});

	if ( $components[0] )
	{
	    push @result, \@components;
	}
    }
    
    # If the 'author' field is a simple string, we must split it into individual names.
    
    elsif ( $r->{author} && ! ref $r->{author} )
    {
	my @names = split_authorlist($r->{author});
	
	if ( $index > 1 )
	{
	    splice(@names, 0, $index - 1);
	}

	foreach my $name ( @names )
	{
	    my @components = parse_authorname($name);

	    if ( $components[0] )
	    {
		push @result, \@components;
	    }
	}
    }
    
    # Return the result if any.

    return @result;
}


# split_authorlist ( authorlist )
#
# Given a string composed of one or more author names, split it up into individual names as
# correctly as we can manage.

sub split_authorlist {

    my ($authorlist) = @_;
    
    # The following declaration appears at the top of the file as well, but it is so important
    # that I am repeating it here in case some misguided person removes the one at the top.
    
    use feature 'unicode_strings';
    
    # If the authorlist does not contain at least two alphabetic characters in a row, return the
    # empty list.
    
    return () unless $authorlist =~ /[[:alpha:]][[:alpha:]]/;
    
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
    
    if ( $items[-1] =~ qr{ ^ (.* \S \s+ \S+) \s+ ( and | AND ) \s+ (\S+ \s+ \S .*) $ }xs )
    {
	$items[-1] = $1;
	push @items, $3;
    }
    
    # Then go through the list one by one and merge the items into names as necessary. Names that
    # look like "Smith, A. B." will appear as two items in this list, and will be merged into
    # one. Some entries in the paleobiodb have authorlists in this format.
    
    my @names;
    
    foreach my $item ( @items )
    {
	# If the item is 'jr.', 'ii', 'iii', or 'iv', add it to the previous name. If there is no
	# previous name, discard it.
	
	if ( $item =~ qr { ^ ( jr[.]? | iii?[.]? | iv[.]? ) $ }xsi )
	{
	    if ( @names )
	    {
		$names[-1] .= ", $item";
	    }
	}
	
	# If the item contains at least three alphabetic characters in a row, it is almost certainly a
	# name. Add it to the name list.
	
	elsif ( $item =~ /[[:alpha:]]{3}/ )
	{
	    push @names, $item;
	}
	
	# If the item doesn't have three alphabetic characters in a row and either contains a
	# letter followed by a period or is a single letter or has a single letter at the start or
	# end, it probably represents initials associated with the previous item. If the previous
	# item doesn't contain initials, add this item to it. Otherwise, discard this item.
	
	elsif ( $item =~ qr{ \b [[:alpha:]][.] | ^ [[:alpha:]] \s | \s [[:alpha:]] $ | ^ [[:alpha:]] $ }xs )
	{
	    if ( @names && $names[-1] !~ qr{ \b [[:alpha:]][.] | ^ [[:alpha:]] \s | \s [[:alpha:]] $ }xs )
	    {
		$names[-1] .= ", $item";
	    }
	}
	
	# If the item has at least two alphabetic characters in a row, it is almost certainly a
	# name. Add it to the name list. We need to do this check here because there are names
	# such as 'Wu' and also initials such as 'A. de B.'. So we check for initials first and
	# then check for two-letter names.

	elsif ( $item =~ /[[:alpha:]]{2}/ )
	{
	    push @names, $item;
	}

	# If the item has at least one alphabetic character and the previous name has no initials,
	# add this item to it. Otherwise, discard this item.

	elsif ( $item =~ /[[:alpha:]]/ )
	{
	    if ( @names && $names[-1] !~ qr{ \b [[:alpha:]][.] | ^ [[:alpha:]] \s | \s [[:alpha:]] $ }xs )
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
# entry may either be a hashref, an arrayref, or a string.

sub parse_authorname {

    my ($entry) = @_;
    
    # If the selected entry is an arrayref, return the contents as-is. This is most likely to
    # happen if an arrayref of already parsed name components is passed to this routine again.

    if ( ref $entry eq 'ARRAY' )
    {
	return @$entry;
    }
    
    # If the selected entry is a hashref, look for the various fieldnames used for first and last
    # names. Only return the first name if we have found a non-empty last name. If don't find a
    # separate last name but we do find a 'name' field, fall through to the next section.
    
    if ( ref $entry eq 'HASH' )
    {
	my $last = $entry->{last} || $entry->{lastname} || $entry->{family};
	my $first = $entry->{first} || $entry->{firstname} || $entry->{given};
	my $affiliation = $entry->{affiliation} || $entry->{institution};

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
    
    # If the entry is anything other than a nonempty string, return.
    
    return unless $entry || ! ref $entry;

    # If the full name contains a comma, split it on commas. The last name is always the first
    # component.
    
    if ( $entry =~ /,/ )
    {
	my @components = split /\s*,\s*/, $entry;
	
	my $last = shift @components;
	
	# If we have a 'jr' or 'iii' suffix (optionally followed by a period) then add that to the
	# last name.

	if ( $components[0] =~ qr{ ^ ( jr[.]? | iii?[.]? | iv[.]? ) .* $ }xsi )
	{
	    my $suffix = shift @components;
	    $last .= ", $suffix";
	}
	
	# If we have at least one more component that contains one or more alphabetic characters,
	# take the first of those as the first name and discard the rest.
	
	if ( @components && $components[0] =~ /[[:alpha:]]/ )
	{
	    return ($last, $components[0]);
	}
	
	# Otherwise, fall through to the next step.
	
	else
	{
	    $entry = $last;
	}
    }
    
    # If we get here, we have a name that looks like either "B. Smith" or "Smith B."; we start by
    # looking for initials at the front of the name. As a very first step, there are a few
    # paleobiodb entries that mistakenly have / instead of . after the initial. So substitute the
    # latter for the former.
    
    $entry =~ s{ ( ^ [[:alpha:]] | \s [[:alpha:]] ) / }{\1.}gx;
    
    # Patterns we are looking for include "B. Smith", 
    # "B Smith", "B-C Smith", "B.-C. Smith", "B. C. D. Smith", "B C D Smith", etc. Note that
    # .* is greedy by default. We want to match up to the very last letter that is preceded
    # by a non-letter and followed by an optional period and then a space.
    
    if ( $entry =~ qr{ ^ ( [[:alpha:]][.]? | [[:alpha:]][-. ][[:alpha:]][.]? |
			   [[:alpha:]][-. ].*[^[:alpha:]][[:alpha:]][.]? ) \s+ (\S .*) }xs )
    {
	my $first = $1;
	my $last = $2;
	
	return ($last, $first);
    }
    
    # Otherwise, look for initials at the end. The first name starts with the first letter that is
    # preceded by a space and followed by . - or space or the end of the string. Note that in this
    # case we use .*? for a non-greedy match because we want to capture all of the initials rather
    # than just the last one.
    
    if ( $entry =~ qr{ (.*? \S) \s+ ( [[:alpha:]][-. ] .* | [[:alpha:]] $ ) }xs )
    {
	my $last = $1;
	my $first = $2;

	return ($last, $first);
    }

    # If we don't find either of these patterns, split off everything up to the last word not
    # followed by a comma as the first name.
    
    if ( $entry =~ qr{ ^ (.+) (?<! ,) \s ( [[:alpha:]] .* ) }xs )
    {
	my $first = $1;
	my $last = $2;

	return ($last, $first);
    }
    
    # If the name doesn't have multiple words, return the whole thing as the last name.
    
    else
    {
	return ($entry, '');
    }
}


# find_authorname ( r, index, last, first )
# 
# Return a similarity score (no conflict score) if the specified name is found in the author list
# of $r. If $index is 1 (or not specified) then all authors are searched. If $index is 2, the
# second and subsequent authors are searched, etc.

sub find_authorname {
    
    my ($r, $last, $first, $index) = @_;

    # # If $index is not given, assume 1. Return 0 unless a last name is given.
    
    # $index ||= 1;
    
    # return 0 unless $last;
    
    # # If this is a paleobiodb record, we check the relevant fields directly.
    
    # if ( $r->{author1last} )
    # {
    # 	# If index is not greater than 1, check author1. If the score is greater than zero, return
    # 	# it. Same for author2 and index 2.
	
    # 	unless ( $index > 1 )
    # 	{
    # 	    my $score = check_authorname($r->{author1last}, $r->{author1init},
    # 					 $last, $first);
	    
    # 	    return $score if $score;
    # 	}
	
    # 	unless ( $index > 2 )
    # 	{
    # 	    my $score = check_authorname($r->{author2last}, $r->{author2init},
    # 					 $last, $first);
	    
    # 	    return $score if $score;
    # 	}
	
    # 	# If we get here, we need to check otherauthors. Grab the last sequence of 2 more more
    # 	# alphabetic characters from $last, and see if it occurs somewhere in the list.
	
    # 	return 0 unless $last =~ qr{ ([[:alpha:]][[:alpha:]]+) $ }xs;
	
    # 	my $lastword = $1;
	
    # 	return 0 unless $r->{otherauthors} =~ /$lastword/;
	
    # 	# First determine if the otherauthors value has initials first or not.
	
    # 	my $initfirst = $r->{otherauthors} =~ qr{ ^ [[:alpha:]] [-. ] }xs;
	
    # 	# If we have a first initial, try searching with it first.
	
    # 	if ( $first =~ qr{ ^ ([[:alpha:]]) }xs )
    # 	{
    # 	    my $init = $1;
	    
    # 	    # First look for the entire last name with first initial. If we find it, return a
    # 	    # 100% match.
	    
    # 	    if ( $initfirst && $r->{otherauthors} =~ qr{ (^|,\s*) $init \b [^,]+ \b $last (,|$) }xsi )
    # 	    {
    # 		return 100;
    # 	    }
	    
    # 	    elsif ( !$initfirst && $r->{otherauthors} =~ qr{ \b $last, \s* $init \b }xsi )
    # 	    {
    # 		return 100;
    # 	    }
	    
    # 	    # Then try just the last word with the first initial. If we find it, return a 90% match.
	    
    # 	    elsif ( $initfirst && $r->{otherauthors} =~ qr{ (^|,\s*) $init \b [^,]+ \b $lastword (,|$) }xsi )
    # 	    {
    # 		return 90;
    # 	    }
	    
    # 	    elsif ( !$initfirst && $r->{otherauthors} =~ qr{ \b $lastword, \s* $init \b }xsi )
    # 	    {
    # 		return 90;
    # 	    }
    # 	}
	
    # 	# Without an initial, we return at best an 80% match.
	
    # 	elsif ( $initfirst && $r->{otherauthors} =~ qr{ \b $last (,|$) }xsi )
    # 	{
    # 	    return 80;
    # 	}
	
    # 	elsif ( !$initfirst && $r->{otherauthors} =~ qr{ \b $last, }xsi )
    # 	{
    # 	    return 80;
    # 	}
	
    # 	# If we match just the last word, return a 50% match.
	
    # 	elsif ( $initfirst && $r->{otherauthors} =~ qr{ \b $lastword (,|$) }xsi )
    # 	{
    # 	    return 50;
    # 	}
	
    # 	elsif ( !$initfirst && $r->{otherauthors} =~ qr{ \b $lastword, }xsi )
    # 	{
    # 	    return 50;
    # 	}

    # 	# If nothing matches, return 0.
	
    # 	return 0;
    # }

    # Otherwise, if there is an 'authors' field, search through it one by one.
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

    return $doi;
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
# With each argument, replace HTML character entity sequences with the corresponding characters.

sub clean_string {
    
    foreach ( @_ )
    {
	if ( defined $_ && $_ =~ /[[:alpha:]]/ )
	{
	    $_ =~ s{ &\#(\d+); }{ chr($1) }xsge;
	}
    }

    return @_;
}


# Similarity functions
# --------------------

# title_similarity ( title_a, title_b, type )
# 
# Compute a fuzzy match score between $title_a and $title_b, doing our best to allow for both
# mistyping and leaving out a word or two. This routine returns a list of two numbers. The first
# is a similarity score, normalized to 0-100 with 100 representing equality (case and accent marks
# are ignored). The second is a conflict score, also normalized to 0-100 with 100 representing no
# possibility that these values were originally the same.

our %STOPWORD = ( a => 1, an => 1, and => 1, the => 1, at => 1, for => 1, on => 1, of => 1 );

sub title_similarity {
    
    my ($title_a, $title_b, $title_type) = @_;

    # If one or both arguments are empty, we return (0, 0). In that case we do not have enough
    # information to tell us that the records are similar, nor that they are dissimilar. We
    # consider a title to be empty if it doesn't have at least 3 alphabetic characters in a row.
    
    return (0, 0) unless $title_a && $title_a =~ /[[:alpha:]]{3}/ &&
	$title_b && $title_b =~ /[[:alpha:]]{3}/;

    # If the two titles are equal when case and accents are ignored, return (100, 0). We ignore
    # accent marks because these are often mistyped or omitted.

    if ( eq_insensitive($title_a, $title_b) )
    {
	return (100, 0);
    }
    
    # Otherwise, we start by removing HTML tags, which do occasionally appear in Crossref titles.
    
    $title_a =~ s{ &lt; .*? &gt; | < .*? > }{}xsg;
    $title_b =~ s{ &lt; .*? &gt; | < .*? > }{}xsg;
    
    # Continue by removing everything else except alphanumerics and spaces. We are primarily
    # interested in comparing the set of words in the two titles. All other characters or
    # sequences of such are converted into a single space to preserve word boundaries.
    
    $title_a =~ s/[^[:alnum:][:space:]]+/ /g;
    $title_b =~ s/[^[:alnum:][:space:]]+/ /g;
    
    # For work titles, if one of the two is a prefix of the other then return a match. It is
    # likely that extra cruft was accidentally appended to the other title in one of the entries,
    # or that ending words were accidentally left out. For efficiency, we only do this detailed check
    # if the two titles start with the five letters.
    
    if ( eq_insensitive(substr($title_a, 0, 5), substr($title_b, 0, 5)) &&
	 length($title_a) != length($title_b) )
    {
	my $greater = length($title_a) > length($title_b) ? $title_a : $title_b;
	my $lesser = length($title_a) > length($title_b) ? $title_b : $title_a;
	my $reduced = substr($greater, 0, length($lesser));
	my $len_greater = length($greater);
	my $len_lesser = length($lesser);
	my $diff = $len_greater - $len_lesser;
	
	# For publication titles, return a 50% match if the greater exceeds the lesser by less
	# than 20% and the lesser is at least 5 characters.
	
	if ( $title_type eq 'pub' )
	{
	    if ( $diff * 5 <= $len_lesser && $len_lesser >= 5 )
	    {
		return (50, 50);
	    }
	}

	# For work titles, return a 100% match if the lesser is at least 20 characters and the
	# greater is less than twice as long, an 80% match if it is greater than twice.

	elsif ( $title_type eq 'work' && $len_lesser >= 20 )
	{
	    if ( $len_greater / $len_lesser <= 2 )
	    {
		return (100, 0);
	    }

	    else
	    {
		return (80, 20);
	    }
	}
	
	# # If $title_a is longer, compare its initial sequence to $title_b. If the two are equal,
	# # return a match if the difference in length is not too great.
	
	# if ( $length_a > $length_b && $length_b >= 5 )
	# {
	#     my $reduced_a = substr($title_a, 0, $length_b);
	    
	#     if ( eq_insensitive($reduced_a, $title_b) )
	#     {
	# 	if ( $title_type eq 'work' && $length_b >= 20 
		
	# 	return ($sim, $con);
	#     }
	# }
	
	# # If $title_b is longer, compare its initial sequence to $title_a. Under ordinary
	# # circumstances we don't need to consider the case where they are equal, because then they
	# # would have already matched above. But just in case some code is later added that breaks
	# # this, we include the case where the two are equal in length here.
	
	# elsif ( $length_b >= $length_a && $length_a >= 20 )
	# {
	#     my $reduced_b = substr($title_b, 0, $length_a);
	    
	#     if ( eq_insensitive($reduced_b, $title_a) )
	#     {
	# 	return ($sim, $con);
	#     }
	# }
    }
    
    # Then we split each title into words and remove stopwords. The stopword list contains words
    # that are often left out of titles.
    
    my @title_a = grep { !$STOPWORD{lc $_} } split /[[:space:]]+/, $title_a;
    my @title_b = grep { !$STOPWORD{lc $_} } split /[[:space:]]+/, $title_b;
    
    # # Then we compare the two lists of words under case- and accent-insensitive comparison. We
    # # compute a partial prefix, and will then see how that compares to the two word lists.
    
    # my @common_prefix;
    
    # foreach my $i ( 0..$#title_a )
    # {
    # 	if ( $title_a[$i] eq $title_b[$i] || eq_insensitive($title_a[$i], $title_b[$i]) )
    # 	{
    # 	    push @common_prefix, $title_a[$i];
    # 	}
	
    # 	else
    # 	{
    # 	    last;
    # 	}
    # }
    
    # # If the two word lists have the same length as the common prefix, that means they are
    # # identical under the eq_insensitive comparison. In that case, the result is (100, 0).

    # if ( @common_prefix == @title_a && @common_prefix == @title_b )
    # {
    # 	return (100, 0);
    # }
    
    # # Otherwise, if the common prefix is the same length as one title or the other that means one
    # # title is a prefix of the other. If the missing sequence is small enough, then return a match.
    
    # if ( @common_prefix == @title_a || @common_prefix == @title_b )
    # {
    # 	my $longer = @title_a > @title_b ? scalar(@title_a) : scalar(@title_b);
    # 	my $shorter = scalar(@common_prefix);
    # 	my $difference = $longer - $shorter;
	
    # 	# If the shorter title is at least 6 words, we allow up to 4 missing words at the end with
    # 	# a 5 point deduction for each one.
	
    # 	if ( $shorter >= 6 && $difference <= 4 )
    # 	{
    # 	    my $deduction = $difference * 5;
    # 	    return (100 - $deduction, $deduction);
    # 	}

    # 	# If the shorter title is at least 4 words, we allow up to 3 missing words at the end with
    # 	# a 10 point deduction for each one.

    # 	if ( $shorter >= 4 && $difference <= 3 )
    # 	{
    # 	    my $deduction = $difference * 10;
    # 	    return (100 - $deduction, $deduction);
    # 	}
    # }
    
    # If neither sequence is a prefix of the other, the most efficient approach is to try to
    # eliminate obvious mismatches very quickly. If there is no common prefix, we look for a word
    # at least six characters long that is the same as the corresponding word in one of the first
    # five places, or a word of at least six characters in one of the first five places of either
    # title that appears in one of the first three places of the other one.
    
    my $found_word;
    
    foreach my $i ( 0..4 )
    {
	if ( length($title_a[$i]) >= 6 && eq_insensitive($title_a[$i], $title_b[$i]) )
	{
	    $found_word = 1;
	    last;
	}
    }    

    unless ( $found_word )
    {
      WORD:
	foreach my $i ( 0..4 )
	{
	    if ( length($title_a[$i]) >= 6 )
	    {
		foreach my $j ( 0..2 )
		{
		    if ( eq_insensitive($title_a[$i], $title_b[$j]) )
		    {
			$found_word = 1;
			last WORD;
		    }
		}
	    }
	    
	    if ( length($title_b[$i]) >= 6 )
	    {
		foreach my $j ( 0..2 )
		{
		    if ( eq_insensitive($title_b[$i], $title_a[$j]) )
		    {
			$found_word = 1;
			last WORD;
		    }
		}
	    }
	}
    }
    
    # Unless we find such a matching word, we declare a complete mismatch.
    
    unless ( $found_word )
    {
	return (0, 100);
    }
    
    # If a match has not been ruled out, compute the number of words in the shorter of the two
    # word lists.
    
    my $min_words = min_score(scalar(@title_a), scalar(@title_b));

    # Then run through both word lists and strip off any accents. Create a new Unaccent object if
    # we don't already have one. Run through them a second time and put them into fold case.
    
    $Unaccent ||= Text::Transliterator::Unaccent->new(script => 'Latin');
    
    $Unaccent->(@title_a);
    $Unaccent->(@title_b);
    
    foreach (@title_a, @title_b)
    {
	$_ = CORE::fc($_);
    }
    
    # Now use Algorithm::Diff to compute the difference between the two word lists, looking for
    # the following special cases:
    #
    # 1) Differing words that have very short editing differences, making it likely that one of
    # the two was mis-entered.
    # 
    # 2) Two titles that are similar if a sequence of words is removed from the end of one of
    # them. This probably means either one of them accidentally had its final part left off or
    # else the other one had some cruft added to the end of it.
    # 
    # 3) One title contains a word which is the same as several words from the other one
    # concatenated without any spaces, in the same place in the sequence. Unfortunately, we
    # sometimes find this error in crossref titles. Apparently, some of the spaces in certain
    # titles were at some point entered as "nonbreaking spaces" and a subsequent processing step
    # dropped them entirely.
    
    my @diff = sdiff(\@title_a, \@title_b);
    
    my (@extra_a, @extra_b, @suffix_a, @suffix_b, $misspell_count);

    my $deduction = 0;
    
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
		my $edist = edistance($word_a, $word_b, 3, \&eq_insensitive);
		
		# The allowable edit distance varies depending on the length of the shorter word.
		
		my $len = min_score($len_a, $len_b);
		
		my $max = $len > 6 ? 3
		    : $len > 4 ? 2
		    : 1;
		
		# If the edit distance is within the allowable range, increment the misspelling
		# count and go on to the next segment.
		
		if ( $edist >= 0 && $edist <= $max )
		{
		    $misspell_count++;
		    next;
		}
	    }
	    
	    # Otherwise, check to see if the larger of the two words is similar to the smaller
	    # concatenated with one or more subsequent words. This code is written to do some
	    # inexpensive checks first to rule out obvious mismatches before we do a more thorough
	    # and expensive check to confirm.
	    
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
		    my $edist = match_squashed($word_a, $word_b, 'b', \@diff);
		    
		    if ( $edist < 4 )
		    {
			$misspell_count++ if $edist;
			next;
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
		    my $edist = match_squashed($word_b, $word_a, 'a', \@diff);

		    if ( $edist < 4 )
		    {
			$misspell_count++ if $edist;
			next;
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
    
    # Now that we have gone through the entire sequence, if one or the other title has a suffix we
    # ignore it completely if the other title has at least 3 words. If the minimum word length is
    # smaller, we deduct similarity points.
    
    if ( @suffix_a || @suffix_b )
    {
	if ( $min_words < 3 )
	{
	    $deduction = 20;
	}
    }
    
    # For a long title, if either sequence has more than 4 words not in the other one, we declare the
    # two titles to be completely different. For shorter titles, the threshold is smaller.
    
    my $threshold = $min_words > 10 ? 4
		  : $min_words > 6  ? 3
	          : $min_words > 4  ? 2
	          :                   1;
    
    if ( @extra_a > $threshold || @extra_b > $threshold )
    {
	return (0, 100);
    }
    
    # # Otherwise, look for pairs of words with a short edit difference. If we find a pair, deduct
    # # them from the counts.
    
    # my %matched;
    
    # foreach my $w ( @words_a )
    # {
    # 	foreach my $v ( @words_b )
    # 	{
    # 	    if ( ! $matched{$v} )
    # 	    {
    # 		my $edist = edistance($w, $v, 3, \&eq_insensitive);
		
    # 		if ( $edist >= 0 && $edist <= 3 )
    # 		{
    # 		    $misspell_count++;
    # 		    $matched{$v};
    # 		}
    # 	    }
    # 	}
    # }
    
	# my $tld = Text::Levenshtein::Damerau->new($w, { max_distance => 3,
	# 						eq_function => \&eq_insensitive });
	
	# my $close = $tld->dld({ list => \@words_b });
	
	# if ( $close && %$close )
	# {
	#     foreach my $word ( keys %$close )
	#     {
	# 	if ( $close->{$word} >= 0 && $close->{$word} <= 3 )
	# 	{
	# 	    $count_a--;
	# 	    $count_b--;
	# 	    $misspell_count++;
	# 	    @words_b = grep { $_ ne $word } @words_b;
	# 	}
	#     }
	# }
    
    # Otherwise, we start with 100 and deduct 10 points for each distinct word that doesn't appear
    # in the other title and 3 points for each misspelling. Return that number as the similarity
    # score, and its difference from 100 as the conflict score.
    
    my $distinct_count = scalar(@extra_a) + scalar(@extra_b);
    
    my $score = 100 - 10 * $distinct_count - 3 * $misspell_count;
    
    return ($score, 100 - $score);
}


# match_squashed ( longer, shorter, seq_select, segments_ref )
#
# If the longer word matches the shorter one concatenated with one or more of the following words
# from its sequence, then remove those words from the segment list (which is pointed to by
# $segments_ref) and return true. The $seq_select parameter will be 'b' if the shorter word was
# from sequence b, 'a' if the shorter word was from sequence a.
#
# Returns the edit distance between the two if it is small, 100 otherwise.

sub match_squashed {

    my ($longer, $shorter, $seq_select, $segments_ref) = @_;
    
    # First see if the shorter word is a prefix of the longer. Accept it if the TLD
    # distance is 2 or less. Otherwise, return a rejection.
    
    my $reduced = substr($longer, 0, length($shorter));
    
    my $edist = edistance($reduced, $shorter, 2, \&eq_insensitive);
    
    return 100 unless $edist >= 0 && $edist <= 2;
    
    # Now start with $shorter and add subsequent words until the length is close to or exceeds the
    # length of $longer. Put an overall limit on the number of additional words to catch runaway
    # loops.
    
    my $op_select = $seq_select eq 'a' ? '-' : '+';
    my $word_select = $seq_select eq 'a' ? 1 : 2;
    
    my $words_ahead = 0;
    my $concatenated = $shorter;
    
    while ( $words_ahead < 100 && $segments_ref->[$words_ahead] &&
	    $segments_ref->[$words_ahead][0] eq $op_select &&
	    length($concatenated) < length($longer) - 2 )
    {
	$concatenated .= $segments_ref->[$words_ahead][$word_select];
	$words_ahead++;
    }
    
    # If the length is comparable, check the edit distance.

    if ( abs( length($concatenated) - length($longer) ) < 3 )
    {
	my $edist = edistance($concatenated, $longer, 3, \&eq_insensitive);
	
	# The allowable edit distance varies depending on the length of the shorter word.
	
	my $len = min_score($concatenated, $longer);
	
	my $max = $len > 6 ? 3
	    : $len > 4 ? 2
	    : 1;
	
	# If the TLD editing distance is within the allowable range, return it and remove all the
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


# title_multimatch ( titles_a, subtitles_a, titles_b, subtitles_b )
# 
# This subroutine takes two lists of titles and two lists of subtitles, and tries all combinations
# of the titles using the subtitles as appropriate. The highest similarity score is returned.

sub title_multimatch {
    
    my ($titles_a, $subtitles_a, $titles_b, $subtitles_b) = @_;
    
    my $max_similarity = undef;
    my $same_conflict = undef;
    
    # Iterate through the 'a' titles.
    
    foreach my $a ( 0..$titles_a->$#* )
    {
	# Iterate thorugh the 'b' titles.
	
	foreach my $b ( 0..$titles_b->$#* )
	{
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

	    my ($sim, $con) = title_similarity($ta, $tb);

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

sub author_similarity {
    
    my ($last_a, $first_a, $last_b, $first_b) = @_;
    
    # If one or both last names are empty, we return (0, 0). In that case we do not have enough
    # information to tell us that the records are similar, nor that they are dissimilar. We
    # consider a last name to be empty if it doesn't have at least one alphabetic character.
    
    return (0, 0) unless $last_a && $last_a =~ /[[:alpha:]]/ &&
	$last_b && $last_b =~ /[[:alpha:]]/;

    # If either last name has no more than one alphabetic character in a row while the
    # corresponding first name has more than one, that means the two were accidentally entered
    # into the wrong fields. So swap them.

    if ( $last_a !~ /[[:alpha:]]{2}/ && $first_a && $first_a =~ /[[:alpha:]]{2}/ )
    {
	my $temp = $last_a;
	$last_a = $first_a;
	$first_a = $temp;
    }
    
    if ( $last_b !~ /[[:alpha:]]{2}/ && $first_b && $first_b =~ /[[:alpha:]]{2}/ )
    {
	my $temp = $last_b;
	$last_b = $first_b;
	$first_b = $temp;
    }
    
    # Extract the initial letters from the first one or two words of each first name. Many of them
    # are stored as initials anyway, so we simplify by only considering the first letter. If there
    # are more than two initials, we ignore the rest.
    
    my ($init_a, $init2_a) =
	$first_a =~ qr{ ([[:alpha:]]) [[:alpha:]]* (?: [^[:alpha:]]+ ([[:alpha:]]))? }xs;
    
    my ($init_b, $init2_b) =
	$first_a =~ qr{ ([[:alpha:]]) [[:alpha:]]* (?: [^[:alpha:]]+ ([[:alpha:]]))? }xs;
    
    # If either last name has spurious initials at the end, remove them.
    
    $last_a =~ s{ ( \s [[:alpha:]][-.\s]* )+ $ }{}x;
    $last_b =~ s{ ( \s [[:alpha:]][-.\s]* )+ $ }{}x;
    
    # Remove suffixes such as jr. and iii from the ends of last names. If the names are otherwise
    # identical and one has such a suffix while the other doesn't, we assume it was left off from
    # the other entry by mistake.
    
    if ( $last_a =~ qr{ ^ (.*?) [\s,]+ (jr|ii|iii|iv|2nd|3rd|4th) [.]? $ }xsi )
    {
	$last_a = $1;
    }
    
    if ( $last_b =~ qr{ ^ (.*?) [\s,]+ (jr|ii|iii|iv|2nd|3rd|4th) [.]? $ }xsi )
    {
	$last_b = $1;
    }
    
    # If the last names are now equal when case and accents are ignored, then we consider the the
    # names to be a match.
    
    if ( eq_insensitive($last_a, $last_b) )
    {
	# If the first two initials are equal or both empty, return a similarity of 100. If the
	# first two initials match but the second two do not, return a similarity of 90.
	
	if ( $init_a eq $init_b || eq_insensitive($init_a, $init_b) )
	{
	    if ( $init2_a eq $init2_b || eq_insensitive($init2_a, $init2_b) )
	    {
		return (100, 0);
	    }
	    
	    else
	    {
		return (90, 0);
	    }
	}
	
	# If one of the first names is empty and the other is not, also return a similarity of 90.
	
	if ( ! $init_a || ! $init_b )
	{
	    return (90, 0);
	}
	
	# If the first initials differ, return a similarity of 80 with a difference of 20.
	
	else
	{
	    return (80, 20);
	}
    }
    
    # Otherwise, we split both names up into words.  If the two last names have different numbers
    # of words, check to see if one has one or more words missing at the end or if they fit the
    # pattern of 'a. b.' 'smith' vs. 'a.' 'bob smith'. Generate both an initial segment and a
    # final segment for both names that have the same number of 
    
    # $last_a =~ s/-/ /g;
    # $last_b =~ s/-/ /g;
    
    my @words_a = $last_a =~ /([[:alpha:]]+)/g;
    my @words_b = $last_b =~ /([[:alpha:]]+)/g;
    
    my ($last_final_a, $last_initial_a, $last_final_b, $last_initial_b);
    
    if ( @words_a > @words_b )
    {
	my $count = @words_b;
	$last_initial_a = join(' ', @words_a[0..$count-1]);
	$last_final_a = join(' ', @words_a[$count..-1]);
	$last_initial_b = $last_final_b = join(' ', @words_b);
	# splice(@words_a, 0, $difference);
	# $orig_a = $last_a;
	# $last_a = join(' ', @words_a);
    }

    elsif ( @words_b > @words_a )
    {
	my $count = @words_a;
	my $last_initial_b = join(' ', @words_b[0..$count-1]);
	my $last_final_b = join(' ', @words_b[$count..-1]);
	$last_initial_a = $last_final_a = join(' ', @words_a);
	# my $difference = @words_b - @words_a;
	# splice(@words_b, 0, $difference);
	# $orig_b = $last_b;
	# $last_b = join(' ', @words_b);
    }
    
    # If the first letters of $first_a and $last_a cross-match the first letters of $first_b and
    # $last_b, it may be an East Asian name where the data enterer was confused about first/last
    # order. But we can only make that determination if the two pairs of letters differ.
    
    if ( substr($last_a, 0, 1) eq $init_b && substr($last_b, 0, 1) eq $init_a &&
	 substr($last_a, 0, 1) ne $init_a )
    {
	# If either first name is given in full (not as initials) and if its first full word
	# matches the first full word of the other last name, return a similarity of 100. We
	# might have a very occasional false positive, but most of these will be correct matches.
	
	my ($firstname_word_a) = $first_a =~ qr{ ^ ([[:alpha:]][[:alpha:]]+) }xs;
	my ($firstname_word_b) = $first_b =~ qr{ ^ ([[:alpha:]][[:alpha:]]+) }xs;
	my ($lastname_word_a) = $last_a =~ qr{ ^ ([[:alpha:]][[:alpha:]]+) }xs;
	my ($lastname_word_b) = $last_b =~ qr{ ^ ([[:alpha:]][[:alpha:]]+) }xs;
	
	if ( $firstname_word_a && $lastname_word_b &&
	     eq_insensitive($firstname_word_a, $lastname_word_b) )
	{
	    return (100, 0);
	}
	
	if ( $firstname_word_b && $lastname_word_a &&
	     eq_insensitive($firstname_word_b, $lastname_word_a) )
	{
	    return (100, 0);
	}
	
	# If both first names are given as initials, we cannot confirm the match absolutely. But
	# it has some chance of being correct, so return an 80% match if there are two initials
	# and they both match up, or 50% match otherwise. $$$ check both initials if present
	
	if ( $first_a =~ qr{ ^ [[:alpha:]] \b }xs && $first_b =~ qr{ ^ [[:alpha:]] \b }xs )
	{
	    if ( $init2_a && @words_b > 1 )
	    {
		my $last2_b = substr($words_b[1], 0, 1);
		
		if ( eq_insensitive($init2_a, $last2_b) )
		{
		    return (80, 0);
		}
	    }

	    if ( $init2_b && @words_a > 1 )
	    {
		my $last2_a = substr($words_a[1], 0, 1);
		
		if ( eq_insensitive($init2_b, $last2_a) )
		{
		    return (80, 0);
		}
	    }
	    
	    return (50, 0);
	}

	# Otherwise, we fall through and apply the normal matching rules.
    }
    
    # If we get to here, compute the Levenshtein-Damerau editing difference between the two last
    # names. Deduct 15 points per difference, for up to 2 differences. If the names are very
    # short, allow fewer differences.
    
    my $min_length = min_score(length($last_a), length($last_b));
    
    my $limit = $min_length > 5 ? 2 : 1;
    
	      # : $min_length > 3 ? 2
	      # :                   1;
    
    my $tld = Text::Levenshtein::Damerau->new($last_a, { max_distance => 3,
							 eq_function => \&eq_insensitive });
    my $distance = $tld->dld($last_b);
    
    # If the distance exceeds our limit, return a similarity of 0.
    
    unless ( $distance >= 1 && $distance <= $limit )
    {
	return (0, 100);
    }
    
    # Otherwise, compute the deduction and compare the first names.
    
    my $deduction = $distance ? $distance * 15 : 0;
    
    # If both first initials are equal, return a similarity of 100 minus the last name
    # deduction. This includes both first names being empty.
    
    if ( $init_a eq $init_b || eq_insensitive($init_a, $init_b) )
    {
	return (100 - $deduction, $deduction);
    }
    
    # If one of the first names is empty and the other is not, deduct 10 more points.
    
    if ( ! $first_a || ! $first_b )
    {
	return (90 - $deduction, 10 + $deduction);
    }
    
    # If the first two initials differ, deduct 20 more points.
    
    else
    {
	return (80 - $deduction, 10 + $deduction);
    }
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

