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
use Carp qw(croak);
use Scalar::Util qw(reftype);

use Exporter 'import';

our (@SCORE_VARS) = qw(complete count sum title pub auth1 auth2 pubyr volume pages pblshr);

our (@EXPORT_OK) = qw(ref_similarity get_reftitle get_pubtitle get_authornames get_pubyr 
		      @SCORE_VARS);

our $IgnoreCaseAccents = Unicode::Collate->new(
     level         => 1,
     normalization => undef);


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
    
    # First compute the similarity and conflict scores for the work titles and the publication
    # titles.
    
    my $title_a = get_reftitle($r);
    my $title_b = get_reftitle($m);
    
    my $pub_a = get_pubtitle($r);
    my $pub_b = get_pubtitle($m);
    
    my ($title_similar, $title_conflict) = title_similarity($title_a, $title_b);
    my ($pub_similar, $pub_conflict) = title_similarity($pub_a, $pub_b);
    
    # Then compute the similarity and conflict scores for the first and second authors. For now,
    # we ignore the rest of the author list.
    
    my ($auth1last_a, $auth1first_a) = get_authornames($r, 1);
    my ($auth1last_b, $auth1first_b) = get_authornames($m, 1);
    
    my ($auth1_similar, $auth1_conflict) = author_similarity($auth1last_a, $auth1first_a,
							     $auth1last_b, $auth1first_b);
    
    my ($auth2last_a, $auth2first_a) = get_authornames($r, 2);
    my ($auth2last_b, $auth2first_b) = get_authornames($m, 2);
    
    my ($auth2_similar, $auth2_conflict) = author_similarity($auth2last_a, $auth2first_a,
							     $auth2last_b, $auth2first_b, 2);
    
    # If both records have a similar first author and at least one second author is missing, the
    # second author similarity score is special cased:
    
    if ( $auth1_similar && (! $auth2last_a || ! $auth2last_b) )
    {
	# If both second authors are missing, the similarity is (100, 0). This is presumably a
	# work with only one author. Otherwise, it must be the case that one work has at least two
	# authors and the other only has one. In this case, the similarity is (0, 100).
	
	if ( ! $auth2last_a && ! $auth2last_b )
	{
	    $auth2_similar = 100;
	    $auth2_conflict = 0;
	}
	
	else
	{
	    $auth2_similar = 0;
	    $auth2_conflict = 100;
	}
    }

    # If the first and second authors are in the wrong order but otherwise match each other, the
    # similarity scores are also special cased.

    elsif ( $auth1last_a && $auth1last_b && $auth2last_a && $auth2last_b )
    {
	my ($a1b2_similar, $a2b1_similar);
	
	my ($a1b2_similar) = author_similarity($auth1last_a, $auth1first_a,
					       $auth2last_b, $auth2first_b);
	
	if ( $a1b2_similar )
	{
	    ($a2b1_similar) = author_similarity($auth2last_a, $auth2first_a,
						$auth1last_b, $auth1first_b);
	}

	if ( $a1b2_similar >= 80 && $a2b1_similar >= 80 )
	{
	    $auth1_similar = 70;
	    $auth1_conflict = 0;
	    $auth2_similar = 70;
	    $auth2_conflict = 0;
	}
    }
    
    # Then compute the similarity and conflict scores for the publication years.
    
    my $pubyr_a = get_pubyr($r);
    my $pubyr_b = get_pubyr($m);
    
    my ($pubyr_similar, $pubyr_conflict) = pubyr_similarity($pubyr_a, $pubyr_b);
    
    # Then compute the similarity and conflict scores for the volume, issue, and pages. If the
    # volume and page numbers are equal and non-empty but one of the issue numbers is missing,
    # assume that it matches too.
    
    my ($vol_a, $issue_a, $fp_a, $lp_a) = get_volpages($r);
    my ($vol_b, $issue_b, $fp_b, $lp_b) = get_volpages($m);
    
    if ( $vol_a && $fp_a && $lp_a && $vol_a eq $vol_b && $fp_a eq $fp_b && $lp_a eq $lp_b )
    {
	$issue_a ||= $issue_b;
	$issue_b ||= $issue_a;
    }
    
    my ($vol_similar, $vol_conflict) = vol_similarity($vol_a, $issue_a, $vol_b, $issue_b);
    my ($pages_similar, $pages_conflict) = vol_similarity($fp_a, $lp_a, $fp_b, $lp_b);
    
    # Then compute similarity and conflict scores for the publisher.

    my $publisher_a = get_publisher($r);
    my $publisher_b = get_publisher($m);

    my ($publish_similar, $publish_conflict) = title_similarity($publisher_a, $publisher_b);
    
    # Then the doi.
    
    # my $doi_a = get_doi($r);
    # my $doi_b = get_doi($m);

    # Compute the second-level score variables 'complete', 'count', and 'sum'.

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
    
    # # Then compute a weighted score. For starters, we will try weighting all of them equally.
    
    # my $match_score =
    # 	$title_similar * 0.2 +
    # 	$pub_similar * 0.2 +
    # 	$auth1_similar * 0.2 +
    # 	$auth2_similar * 0.2 +
    # 	$pubyr_similar * 0.2;

    # return $match_score;


    
    # If $r and $m have matching DOIs, the match score is 90 + the number of important fields in m
    # that have non-empty values. This way, if more than one record in the database matches the
    # same DOI we return the one that has the most information filled in.
    
    # if ( $r->{doi} && $m->{doi} )
    # {
    # 	# If either doi is in the URL form, reduce it to the doi content.

    # 	my $doi1 = $r->{doi};
    # 	my $doi2 = $m->{doi};
	
    # 	$doi1 =~ s{ ^ \w+:// [^/]+ / }{}xs;
    # 	$doi2 =~ s{ ^ \w+:// [^/]+ / }{}xs;
	
    # 	if ( $doi1 eq $doi2 )
    # 	{
    # 	    my $count = 0;
    # 	    $count++ if $m->{reftitle};
    # 	    $count++ if $m->{pubtitle};
    # 	    $count++ if ($m->{author1last} || $m->{authors};
    # 	    $count++ if $m->{pubyr};
    # 	    $count++ if $m->{pubvol};
    # 	    $count++ if $m->{pubno};
    # 	    $count++ if ($m->{firstpage} || $m->{pages});
    # 	    $count++ if $m->{publisher};
    # 	    return 90 + $count;
    # 	}
    # }
	
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
	return $r->{reftitle} || $r->{pubtitle};
    }
    
    # Otherwise, the title will be stored under the 'title' attribute. In a crossref record this
    # will always be an array of strings, and it might be so in BibJSON as well. If there is more
    # than one value, we assume that the first one is the best one to use. If there is also a
    # 'subtitle' array, its first value is appended to the title value separated by a colon.
    
    elsif ( $r->{title} && ref $r->{title} eq 'ARRAY' && @{$r->{title}} )
    {
	my $reftitle = $r->{title}[0];
	
	if ( $reftitle && $r->{subtitle} && ref $r->{subtitle} eq 'ARRAY' && @{$r->{subtitle}} )
	{
	    $reftitle .= ": $r->{subtitle}[0]";
	}
	
	return $reftitle;
    }
    
    # If the title attribute is a simple string, return that. BibJSON records are most likely to
    # have a single string value under 'title'. If there is a 'subtitle' attribute that is also a
    # simple string, append that.
    
    elsif ( $r->{title} && ! ref $r->{title} )
    {
	my $reftitle = $r->{title};

	if ( $r->{subtitle} && ! ref $r->{subtitle} )
	{
	    $reftitle .= ": $r->{subtitle}";
	}

	return $reftitle;
    }
    
    return undef;
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
	return $r->{reftitle} ? $r->{pubtitle} : undef;
    }
    
    # Crossref records store the publication under 'container-title'. As with the title of the
    # work, if there is more than one we just return the first (and presumably most important) one.
    
    elsif ( $r->{'container-title'} && ref $r->{'container-title'} eq 'ARRAY' &&
	    @{$r->{'container-title'}} )
    {
	return $r->{'container-title'}[0];
    }
    
    # A BibJSON record may have the publication title under any of the following: 'journal',
    # 'booktitle', 'series'.
    
    else
    {
	return $r->{journal} || $r->{booktitle} || $r->{series};
    }
}


# get_authornames ( r, index )
# 
# Return the (last, first) name of the author specified by $index from $r. The value of $index
# should be either 1 for the first author or 2 for the second. This function will not return
# anything from a paleobiodb record for any number past 2.
#
# Returns a list of two values.

sub get_authornames {

    my ($r, $index) = @_;
    
    # If no index is specified, return the first author.
    
    $index ||= 1;
    
    # If this is a paleobiodb record, return the first or second author names, or an empty list.
    
    if ( $r->{reftitle} || $r->{pubtitle} || $r->{author1last} )
    {
	if ( $index == 1 )
	{
	    return ($r->{author1last} || '', $r->{author1init} || '');
	}
	
	elsif ( $index == 2 )
	{
	    return ($r->{author2last}, $r->{author2init});
	}

	else
	{
	    return ();
	}
    }
    
    # Otherwise, the authors will be listed under 'author'. Both BibJSON and Crossref use that
    # fieldname. If this is an array, return the values from the specified element. If the
    # specified name does not exist, return an empty list.

    my $a;
    
    if ( $r->{author} && ref $r->{author} eq 'ARRAY' && @{$r->{author}} )
    {
	$a = $r->{author}[$index-1];
    }
    
    # If the author list is a string, we must attempt to parse it into names. Punting on this
    # for now.
    
    elsif ( $r->{author} && ! ref $r->{author} )
    {
	return ();
    }
    
    # Return the empty list unless we have found a non-empty name.
    
    return () unless $a;
    
    # If the name entry is a hashref, look for the various fieldnames used for first and last
    # names. Only return the first name if we have found a non-empty last name. If don't find a
    # separate last name but we do find a 'name' field, fall through to the next section.
    
    my $fullname;
    
    if ( ref $a eq 'HASH' )
    {
	my $last = $a->{last} || $a->{lastname} || $a->{family};
	my $first = $a->{first} || $a->{firstname} || $a->{given};
	
	if ( $last )
	{
	    return ($last, $first);
	}
	
	elsif ( $a->{name} )
	{
	    $fullname = $a->{name};
	}
    }
    
    # If the author entry is a string, then process it as a full name.
    
    elsif ( ! ref $a )
    {
	$fullname = $a;
    }
    
    # If the full name contains a comma, split it into last, first.
    
    if ( $fullname && $fullname =~ qr{ (.+?) ,\s* (.*) }xs )
    {
	my $last = $1;
	my $first = $2;
	
	# Check for ", jr" or ", iii". If found, put it back on the end of the last name
	# where it belongs.
	
	if ( $first =~ qr{ ( jr[.]? | iii[.]? ) \s*,\s* (.*) }xsi )
	{
	    $last .= ", $1";
	    $first = $2;
	}
	
	# If we have a name, return it.
	
	if ( $last )
	{
	    return ($last, $first);
	}
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

    return $r->{publisher};
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


# Similarity functions
# --------------------

# title_similarity ( title_a, title_b )
# 
# Compute a fuzzy match score between $title_a and $title_b, doing our best to allow for both
# mistyping and leaving out a word or two. This routine returns a list of two numbers. The first
# is a similarity score, normalized to 0-100 with 100 representing equality (case and accent marks
# are ignored). The second is a conflict score, also normalized to 0-100 with 100 representing no
# possibility that these values were originally the same.

our %STOPWORD = ( a => 1, an => 1, and => 1, the => 1, at => 1, for => 1, on => 1, of => 1 );

sub title_similarity {
    
    my ($title_a, $title_b) = @_;

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
    
    # Then we split each title into words and remove stopwords. The stopword list contains words
    # that are often left out of titles.
    
    my @title_a = grep { !$STOPWORD{lc $_} } split /[[:space:]]+/, $title_a;
    my @title_b = grep { !$STOPWORD{lc $_} } split /[[:space:]]+/, $title_b;
    
    # Then we compare the two lists of words under case- and accent-insensitive comparison. We
    # compute a partial prefix, and will then see how that compares to the two word lists.
    
    my @common_prefix;
    
    foreach my $i ( 0..$#title_a )
    {
	if ( $title_a[$i] eq $title_b[$i] || eq_insensitive($title_a[$i], $title_b[$i]) )
	{
	    push @common_prefix, $title_a[$i];
	}
	
	else
	{
	    last;
	}
    }
    
    # If the two word lists have the same length as the common prefix, that means they are
    # identical under the eq_insensitive comparison. In that case, the result is (100, 0).

    if ( @common_prefix == @title_a && @common_prefix == @title_b )
    {
	return (100, 0);
    }
    
    # Otherwise, if the common prefix is the same length as one title or the other that means one
    # title is a prefix of the other. If the missing sequence is small enough, then return a match.
    
    if ( @common_prefix == @title_a || @common_prefix == @title_b )
    {
	my $longer = @title_a > @title_b ? scalar(@title_a) : scalar(@title_b);
	my $shorter = scalar(@common_prefix);
	my $difference = $longer - $shorter;
	
	# If the shorter title is at least 6 words, we allow up to 4 missing words at the end with
	# a 5 point deduction for each one.
	
	if ( $shorter >= 6 && $difference <= 4 )
	{
	    my $deduction = $difference * 5;
	    return (100 - $deduction, $deduction);
	}

	# If the shorter title is at least 4 words, we allow up to 3 missing words at the end with
	# a 10 point deduction for each one.

	if ( $shorter >= 4 && $difference <= 3 )
	{
	    my $deduction = $difference * 10;
	    return (100 - $deduction, $deduction);
	}
    }

    # If neither sequence is a prefix of the other, the most efficient approach is to try to
    # eliminate obvious mismatches very quickly. If there is no common prefix, we look for a word
    # at least six characters long in one of the first two places of either title that appears in
    # one of the first three places of the other one. Unless we find one, we declare a
    # mismatch. If the common prefix is one word, we start after that.
    
    # if ( @common_prefix < 2 )
    # {
    # 	my $start = scalar(@common_prefix);
    # 	my $found_match;
	
    #   WORD:
    # 	foreach my $i ( $start..$start+1 )
    # 	{
    # 	    if ( length($title_a[$i]) >= 6 )
    # 	    {
    # 		foreach my $j ( $start..$start+2 )
    # 		{
    # 		    if ( eq_insensitive($title_a[$i], $title_b[$j]) )
    # 		    {
    # 			$found_match = 1;
    # 			last WORD;
    # 		    }
    # 		}
    # 	    }

    # 	    if ( length($title_b[$i]) >= 6 )
    # 	    {
    # 		foreach my $j ( $start..$start+2 )
    # 		{
    # 		    if ( eq_insensitive($title_b[$i], $title_a[$j]) )
    # 		    {
    # 			$found_match = 1;
    # 			last WORD;
    # 		    }
    # 		}
    # 	    }
    # 	}

    # 	unless ( $found_match )
    # 	{
    # 	    return (0, 100);
    # 	}
    # }

    # If we get here, we haven't ruled out a match. Our next step is to use Algorithm::Diff to
    # compute the difference between the two lists, under case- and accent-insensitive comparison.
    
    my @diff = sdiff(\@title_a, \@title_b, undef, \&eq_insensitive);
    
    my (@words_a, @words_b, $misspell_count);
    
    foreach my $d ( @diff )
    {
	my ($op, $word1, $word2) = @$d;
	
	# Words that are unchanged between the two lists are ignored.

	next if $op eq 'u';
	
	# Words that are changed but with a very short edit difference are considered to be
	# identical. But we keep track of how many there are.
	
	if ( $op eq 'c' )
	{
	    my $edist = edistance($word1, $word2, 3, \&eq_insensitive);

	    if ( $edist > 0 && $edist <= 3 )
	    {
		$misspell_count++;
		next;
	    }
	}
	
	# Otherwise, we add the words to the word difference lists.
	
	if ( $op eq 'c' || $op eq '-' )
	{
	    push @words_a, $word1;
	}
	
	if ( $op eq 'c' || $op eq '+' )
	{
	    push @words_b, $word2;
	}
    }
    
    # For a long title, if either sequence has more than 3 words not in the other one, we declare the
    # two titles to be completely different. For shorter titles, the threshold is 2 or 1.
    
    my $threshold = @title_a > 6 ? 3
	          : @title_a > 4 ? 2
	          :               1;
    
    if ( @words_a > $threshold || @words_b > $threshold )
    {
	return 0;
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
    
    # We then start with 100 and deduct 10 points for each distinct word that doesn't appear in
    # the other title and 3 points for each misspelling. Return that number as the similarity score,
    # and its difference from 100 as the conflict score.

    my $distinct_count = scalar(@words_a) + scalar(@words_b);
    
    my $score = 100 - 10 * $distinct_count - 3 * $misspell_count;
    
    return ($score, 100 - $score);
}


    # return undef unless defined $title_a || defined $title_b;
    # return 0 unless defined $title_a && defined $title_b;
    
    # If either title_a or title_b is empty, they are considered to be completely different. We
    # consider a title empty if it doesn't have at least 3 alphanumeric characters in a row.
    
    # return 0 unless $title_a =~ /[[:alnum:]]/ && $title_b =~ /[[:alnum:]]/;


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
    # consider a last name to be empty if it doesn't have at least 2 alphabetic characters in a
    # row.
    
    return (0, 0) unless $last_a && $last_a =~ /[[:alpha:]]{2}/ &&
	$last_b && $last_b =~ /[[:alpha:]]{2}/;
    
    # Extract the initial letter from each first name. Many of them are stored as initials anyway,
    # so we simplify by only considering the first letter.
    
    my ($init_a) = $first_a =~ qr{ ([[:alpha:]]) }xs;
    my ($init_b) = $first_b =~ qr{ ([[:alpha:]]) }xs;
    
    # Remove suffixes such as jr. and iii from the ends of last names. If the names are otherwise
    # identical and one has such a suffix while the other doesn't, we assume it was left off from
    # the other entry by mistake.
    
    if ( $last_a =~ qr{ ^ (.*?) [\s,]+ (jr|iii) [.]? $ }xsi )
    {
	$last_a = $1;
    }
    
    if ( $last_b =~ qr{ ^ (.*?) [\s,]+ (jr|iii) [.]? $ }xsi )
    {
	$last_b = $1;
    }
    
    # If the two last names have different numbers of words, check to see if they fit the pattern
    # of 'a. b.' 'smith' vs. 'a.' 'bob smith'. If one has more words than the other, remove enough
    # from the beginning to even them out.
    
    my @words_a = $last_a =~ /(\S+)/g;
    my @words_b = $last_b =~ /(\S+)/g;
    
    if ( @words_a > @words_b )
    {
	my $difference = @words_a - @words_b;
	splice(@words_a, 0, $difference);
	$last_a = join(' ', @words_a);
    }

    elsif ( @words_b > @words_a )
    {
	my $difference = @words_b - @words_a;
	splice(@words_b, 0, $difference);
	$last_b = join(' ', @words_b);
    }
    
    # Then check if the last names are equal when case and accents are ignored.
    
    if ( eq_insensitive($last_a, $last_b) )
    {
	# if ( $first_a =~ qr{ ^ [[:alpha:]] \b }xs || $last_a =~ qr{ ^ [[:alpha:]] \b }xs )
	
	# If the initials are equal or both empty, return a similarity of 100.
	
	if ( $init_a eq $init_b || eq_insensitive($init_a, $init_b) )
	{
	    return (100, 0);
	}
	
	# If one of the first names is empty and the other is not, return a similarity of 90.
	
	if ( ! $init_a || ! $init_b )
	{
	    return (90, 0);
	}

	# If the first initials differ, return a similarity of 80.

	else
	{
	    return (80, 0);
	}
    }
    
    # Deal with a. b. smith vs. a. bob smith $$$
    
    # Otherwise, compute the Levenshtein-Damerau editing difference between the two last
    # names. Deduct 10 points per difference, for up to 3 differences.

    else
    {
	my $tld = Text::Levenshtein::Damerau->new($last_a, { max_distance => 3,
							     eq_function => \&eq_insensitive });
	my $distance = $tld->dld($last_b);

	# If the distance is more than 3, return a similarity of 0. Since we set a max_distance,
	# the result will be undefined in this case.
	
	unless ( $distance >= 1 && $distance <= 3 )
	{
	    return (0, 100);
	}
	
	# Otherwise, compute the deduction and compare the first names.
	
	my $deduction = $distance ? $distance * 10 : 0;
	
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
    
    # In all other cases, return a similarity of 0.

    return (0, 100);
}


	# # If one is an initial and it matches the other, return a similarity of 100.

	# if ( $first_a =~ qr{ ^ ([[:alpha:]]) \b }xs )
	# {
	#     my $init_a = $1;
	#     my $init_b = substr($first_b, 0, 1);
	    
	#     if ( eq_insensitive($init_a, $init_b) )
	#     {
	# 	return (100, 0);
	#     }
	# }

	# if ( $first_b =~ qr{ ^ ([[:alpha:]]) \b }xs )
	# {
	#     my $init_b = $1;
	#     my $init_a = substr($first_b, 0, 1);
	    
	#     if ( eq_insensitive($init_a, $init_b) )
	#     {
	# 	return (100, 0);
	#     }
	# }


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


1;

