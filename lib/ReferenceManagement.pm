# 
# The Paleobiology Database
# 
#   ReferenceManagement.pm
#   
#   This module contains the code for searching the local bibliographic references table and for
#   querying external sources of bibliographic reference information.
# 
# 

package ReferenceManagement;

use strict;

use feature 'unicode_strings';

use Carp qw(croak);
use LWP::UserAgent;
use URI::Escape;
use JSON;

use Scalar::Util qw(reftype blessed);

use TableDefs qw(%TABLE);
use CoreTableDefs;
use ReferenceMatch qw(get_reftitle get_subtitle get_pubtitle get_publisher
		      get_authorname get_authorlist get_pubyr get_doi get_volpages
		      title_words ref_similarity ref_match @SCORE_VARS);

our ($USER_AGENT);

our ($CROSSREF_BASE) = "https://api.crossref.org/works";
our ($XDD_BASE) = "https://xdd.wisc.edu/api/articles";

our ($JSON_ENCODER) = JSON->new->canonical;

our ($GOOD_MATCH_MIN) = 500;
our ($PARTIAL_MATCH_MIN) = 300;


{
    $USER_AGENT = LWP::UserAgent->new(timeout => 30,
		agent => 'Paleobiology Database/1.2 (mailto:mmcclenn@geology.wisc.edu); ');
}


# new ( dbh )
# 
# Create a new ReferenceManagement instance, which can be used to do queries on the table.

sub new {
    
    my ($class, $dbh) = @_;
    
    croak "you must specify a database handle" unless $dbh;
    
    croak "invalid database handle '$dbh'" unless blessed $dbh && 
	ref($dbh) =~ /\bDBI\b|\bDatabase::Core\b/;
    
    my $instance = { dbh => $dbh };
    
    $instance->{debug_mode} = 1 if $ENV{DEBUG};
    
    return bless $instance, $class;
}


# debug_mode ( value )
# 
# If an argument is given, set debug_mode. Return the value of debug_mode,
# whether or not an argument was provided.

sub debug_mode {
    
    my ($rm, $value) = @_;
    
    if ( @_ > 1 )
    {
	$rm->{debug_mode} = $value ? 1 : '';
    }
    
    return $rm->{debug_mode};
}


# local_query ( dbh, attrs )
# 
# Return a list of local bibliographic references that match the specified attributes, in
# decreasing order of similarity, date entered.

sub local_query {
    
    my ($rm, $attrs) = @_;
    
    croak "you must specify a hashref of reference attributes" unless
	ref $attrs eq 'HASH';
    
    my $dbh = $rm->{dbh};
    
    my ($selector, $fields, $options, @matches);
	
    # If a doi was given, find all references with that doi. Compare them all to the given
    # attributes; if no other attributes were given, each one gets a score of 90 plus the
    # number of important attributes with a non-empty value. The idea is that if there is
    # more than one we should select the matching reference record that has the greatest amount
    # of information filled in.
    
    if ( $attrs->{doi} )
    {
	my $quoted = $dbh->quote($attrs->{doi});
	my $filter = "doi=$quoted";
	
	my $sql = $rm->generate_ref_query($selector, $fields, $filter, $options);
	
	push @matches, $rm->select_records($sql, 'return', $options);
	
	# Assign match scores.
	
	foreach my $m ( @matches )
	{
	    my $score = 90;
	    $score++ if $m->{reftitle};
	    $score++ if $m->{pubtitle};
	    $score++ if $m->{author1last};
	    $score++ if $m->{author2last};
	    $score++ if $m->{pubvol};
	    $score++ if $m->{pubno};
	    $score++ if $m->{firstpage};
	    $score++ if $m->{lastpage};
	    
	    $m->{score} = $score;
	}
    }
    
    # If no doi was given or if no references with that doi were found, look for references that
    # match some combination of reftitle, pubtitle, pubyr, author1last, author2last.
    
    unless ( @matches )
    {
	my $having;

	# If we have a reftitle or a pubtitle, use the refsearch table for full-text matching.
	
	if ( $attrs->{reftitle} && $attrs->{pubtitle} )
	{
	    my $refquoted = $dbh->quote($attrs->{reftitle});
	    my $pubquoted = $dbh->quote($attrs->{pubtitle});

	    $fields = "r.*, match(refsearch.reftitle) against($refquoted) as score1,
		  match(refsearch.pubtitle) against ($pubquoted) as score2";
	    $having = "score1 > 5 and score2 > 5";
	}
	
	elsif ( $attrs->{reftitle} )
	{
	    my $quoted = $dbh->quote($attrs->{reftitle});

	    $fields = "r.*, match(refsearch.reftitle) against($quoted) as score";
	    $having = "score > 5";
	}
	
	elsif ( $attrs->{pubtitle} )
	{
	    my $quoted = $dbh->quote($attrs->{pubtitle});
	    
	    $fields = "r.*, match(refsearch.pubtitle) against($quoted) as score";
	    $having = "score > 0";
	}
	
	# Then add clauses to restrict the selection based on pubyr and author names.
	
	my @filters;
	
	if ( $attrs->{pubyr} )
	{
	    my $quoted = $dbh->quote($attrs->{pubyr});
	    push @filters, "refs.pubyr = $quoted";
	}
	
	# if ( $attrs->{author1last} && $attrs->{author2last} )
	# {
	#     my $quoted1 = $dbh->quote($attrs->{author1last});
	#     my $quoted2 = $dbh->quote($attrs->{author2last});
	    
	#     push @clauses, "(refs.author1last sounds like $quoted1 and " .
	# 	"refs.author2last sounds like $quoted2)";
	# }
	
	# elsif ( $attrs->{author1last} )
	# {
	#     my $quoted1 = $dbh->quote($attrs->{author1last});
	    
	#     push @clauses, "refs.author1last sounds like $quoted1";
	# }

	# if ( $attrs->{anyauthor} )
	# {
	#     my $quoted1 = $dbh->quote($attrs->{anyauthor});
	#     my $quoted2 = $dbh->quote('%' . $attrs->{anyauthor} . '%');
	    
	#     push @clauses, "(refs.author1last sounds like $quoted1 or " .
	# 	"refs.author2last sounds like $quoted1 or refs.otherauthors like $quoted2)";
	# }
	
	# Now put the pieces together into a single SQL statement and execute it.

	push @filters, "($attrs->{filter})" if $attrs->{filter};
	
	my $filter = join(' and ', @filters);
	
	my $sql = $rm->generate_ref_base($selector, $fields, $filter, $options);
	
	$sql .= "\n\tHAVING $having" if $having;
	
	my @other, $rm->select_records($sql, 'return', $options);
	
	$rm->{selection_sql} = $sql;
	
	# If we get results, look through them and keep any that have even a slight chance of
	# matching.
	
	foreach my $m ( @other )
	{
	    if ( $m->{score1} || $m->{score2} )
	    {
		$m->{score} = $m->{score1} + $m->{score2};
	    }
	    
	    push @matches, $m;
	}
    }
    
    # Now sort the matches in descending order by score.
    
    my @sorted = sort { $b->{score} <=> $a->{score} } @matches;

    return @sorted;    
}



# external_query ( attrs )
# 
# Perform a search on this data source using the specified set of reference attributes. If
# this search is intended to fetch data matching an existing bibliographic reference
# record in the Paleobiology Database, the attribute `reference_no` should be included.

sub external_query {
    
    my ($rm, $source, $attrs) = @_;
    
    # If the source isn't specified, use the one from $rm.
    
    $source ||= $rm->{source} || croak "you must specify a source to query";
    
    # If the attrs parameter is a reference, it must be a hashref.
    
    croak "second argument must be a hashref" unless ref($attrs) && reftype($attrs) eq 'HASH';
    
    # This operation will involve one or more requests on the datasource. We may need to
    # try several different requests before we are satisfied that no matching record can
    # be found in the external dataset. In order to coordinate this, we use a hashref that
    # can hold flags and variables.
    
    my $progress = { };
    my $request_count = 0;
    my $code_500_count = 0;
    my ($request_url, $response_code, $response_status, $match_item, $match_score, $abort);
    my @matches;
    
    # Loop until we find a sufficiently similar record, or until the generate_request_url
    # method stops returning new URLs, or until we exceed a set number of requests.
    
  REQUEST:
    while ( $request_url = $rm->generate_request_url($source, $progress, $attrs) )
    {
	# Abort if we exceed a set number of requests.
	
	last REQUEST if ++$request_count > 10;
	
	# If the last request took at least 5 seconds to complete, wait an additional
	# second. If it took at least 10 seconds to complete, wait an additional 5
	# seconds. This helps to keep the server from getting overloaded.
	
	if ( $progress->{last_latency} >= 5 )
	{
	    my $delay = $progress->{last_latency} >= 10 ? 5 : 1;
	    sleep($delay);
	}
	
	# Send the request and wait for the response. Record how long it takes.
	
	print STDERR "Request Text: $progress->{query_text}\n" if $rm->{debug_mode};
	print STDERR "Request URL: $request_url\n" if $rm->{debug_mode};
	
	my $inittime = time;
	
	my $response = $USER_AGENT->get($request_url);
	
	$response_code = $response->code;
	$response_status = $response->status_line;
	
	$progress->{last_url} = $request_url;
	$progress->{last_code} = $response_code;
	$progress->{last_latency} = time - $inittime;
	
	print STDERR "Response status: $response_status\n\n" if $rm->{debug_mode};
	
	# If the request is successful, decode the content and score all of the items it
	# contains.
	
	if ( $response_code =~ /^2../ )
	{
	    my $decoded_content = $rm->decode_response_json($response);
	    my @items = $rm->extract_source_records($source, $decoded_content, $attrs);
	    
	    # Loop through the items and score them one by one. Ignore any item with a
	    # conflict sum of 300 or more. If we find an item with a similarity sum of 500
	    # or better, choose it immediately. Otherwise, set aside any items that have a
	    # similarity sum of 300 or better.
	    
	    foreach my $item ( @items )
	    {
		my $fuzzy_match = ref_match($attrs, $item);
		
		my $r = { score => $fuzzy_match->{sum_s} - 
				   0.5 * $fuzzy_match->{sum_c} };
		
		$r->{r_reftitle} = get_reftitle($item);
		
		if ( my ($subtitle) = get_subtitle($item) )
		{
		    $r->{r_reftitle} .= ": $subtitle";
		}
		
		# Now process the author names. We must generate both the old
		# pbdb fields and also an author list in BibJSON format.
		
		my @authors = get_authorlist($item);
		my @bibj_authors;
		my $otherauthors = '';
		
		foreach my $i ( 0..$#authors )
		{
		    my ($last, $first, $affiliation, $orcid);
		    
		    if ( ref $authors[$i] eq 'ARRAY' )
		    {
			($last, $first, $affiliation, $orcid) = $authors[$i]->@*;
		    }
		    
		    else
		    {
			($last, $first, $affiliation, $orcid) = parse_authorname($authors[$i]);
		    }
		    
		    if ( $orcid && $orcid =~ qr{ orcid[.]org/(.*) }xs )
		    {
			$orcid = $1;
		    }
		    
		    # Fill in the old pbdb fields.
		    
		    if ( $i == 0 )
		    {
			$r->{r_al1} = $last // '';
			$r->{r_ai1} = $first // '';
		    }
		    
		    elsif ( $i == 1 )
		    {
			$r->{r_al2} = $last // '';
			$r->{r_ai2} = $first // '';
		    }
		    
		    elsif ( $i > 1 )
		    {
			$otherauthors .= ', ' if $otherauthors;
			
			if ( $first && $last )
			{
			    $otherauthors .= "$first $last";
			}
			
			else
			{
			    $otherauthors .= $last;
			}
		    }
		    
		    # Create an author record in BibJSON format.
		    
		    my $bibj = { };
		    
		    if ( $first && $last )
		    {
			$bibj->{firstname} = $first;
			$bibj->{lastname} = $last;
		    }
		    
		    else
		    {
			$bibj->{name} = $last || $first || '';
		    }
		    
		    $bibj->{affiliation} = $affiliation if $affiliation;
		    $bibj->{ORCID} = $orcid if $orcid;
		    
		    push @bibj_authors, $bibj;
		}
		
		$r->{r_author} = \@bibj_authors;
		$r->{r_oa} = $otherauthors if $otherauthors;
		
		if ( my ($pubyr) = get_pubyr($item) )
		{
		    $r->{r_pubyr} = $pubyr;
		}
		
		if ( my ($pubtitle) = get_pubtitle($item) )
		{
		    $r->{r_pubtitle} = $pubtitle;
		}
		
		if ( my ($publisher) = get_publisher($item) )
		{
		    $r->{r_publisher} = $publisher;
		}
		
		if ( my ($doi) = get_doi($item) )
		{
		    $r->{r_doi} = $doi;
		}
		
		my ($volume, $issue) = get_volpages($item);
		
		$r->{r_pubvol} = $volume if defined $volume && $volume ne '';
		$r->{r_pubno} = $issue if defined $issue && $issue ne '';
		
		my ($pages) = $item->{pages} || $item->{page};
		
		$r->{r_fp} = $pages if defined $pages && $pages ne '';
		
		$r->{source} = $source;
		$r->{source_url} = $request_url;
		$r->{source_data} = JSON->new->encode($item);
		
		push @matches, $r;
	    }
	}
	
	# If the response indicates a client error, abort the fetch.
	
	elsif ( $response_code =~ /^4../ )
	{
	    $abort = $response->status_line;
	    last REQUEST;
	}
	
	# If the response indicates a server error, abort if we have gotten more than a set
	# number of server errors. Otherwise, wait a few seconds and try again.
	
	elsif ( $response_code =~ /^5../ )
	{
	    if ( ++$code_500_count < 3 )
	    {
		sleep(2 + $code_500_count * 2);
		next REQUEST;
	    }
	    
	    else
	    {
		$abort = $response->status_line;
		last REQUEST;
	    }
	}
	
	# With any other code, abort.
	
	else
	{
	    $abort = $response->status_line;
	    last REQUEST;
	}
    }
    
    # If we aborted, return the error status.
    
    if ( $abort )
    {
	return { status => $abort, error => 1, source => $source, score => 0 }
    }
    
    # Otherwise, return the matches.
    
    if ( @matches )
    {
	return @matches;
    }
    
    # If we have a good match, return it.
    
    # if ( $match_item )
    # {
    # 	return { status => "200 Good Match",
    # 		 success => 1,
    # 		 source => $source,
    # 		 request_count => $request_count,
    # 		 item => $match_item,
    # 		 scores => $match_score,
    # 		 query_url => $request_url,
    # 		 query_text => $progress->{query_text} };
    # }
    
    # # If we have one or more possible matches, choose the one with the best similarity
    # # score.
    
    # if ( @possible_matches )
    # {
    # 	my $best_similarity = 0;
    # 	my $best_match;
	
    # 	foreach my $i ( 0..$#possible_matches )
    # 	{
    # 	    if ( $possible_matches[$i][1]{sum_s} > $best_similarity )
    # 	    {
    # 		$best_match = $possible_matches[$i];
    # 		$best_similarity = $possible_matches[$i][1]{sum_s};
    # 	    }
    # 	}
	
    # 	if ( $best_match )
    # 	{
    # 	    return { status => "280 Partial match",
    # 		     success => 1,
    # 		     source => $source,
    # 		     request_count => $request_count,
    # 		     item => $best_match->[0],
    # 		     scores => $best_match->[1],
    # 		     query_url => $best_match->[2],
    # 		     query_text => $best_match->[3] };
    # 	}
    # }
    
    # Otherwise, return either Not Found or No Valid Requests as appropriate.
    
    if ( $request_count )
    {
	return { status => "404 Not found",
		 notfound => 1,
		 source => $source,
		 score => 0,
		 request_count => $request_count };
    }
    
    else
    {
	return { status => "480 No valid requests",
		 notfound => 1,
		 source => $source,
		 score => 0,
		 request_count => 0 };
    }
}


# decode_response_json ( response )
# 
# This method decodes the response content from a data source request into a Perl data
# structure and returns it.

sub decode_response_json {
    
    my ($rm, $response) = @_;
    
    my $content = $response->decoded_content;
    
    return decode_json($content);
}


# extract_source_records ( source, response_data, attrs )
# 
# This method takes a Perl data structure decoded from the response to a data source
# request. It returns a list of items that may (or may not) be a match for the specified
# reference attributes. The source-specific methods may filter the list to exclude obvious
# mismatches, but are not required to do so.

sub extract_source_records {

    my ($rm, $source, $response_data, $attrs) = @_;
    
    if ( $source eq 'crossref' )
    {
	return $rm->extract_crossref_records($response_data, $attrs);
    }
    
    elsif ( $source eq 'xdd' )
    {
	return $rm->extract_xdd_records($response_data, $attrs);
    }
    
    else
    {
	croak "Unrecognized data source: '$source'";
    }
}


# generate_request_url ( source, progress, attrs )
# 
# Generate a request URL that will be used to query this datasource, trying to match the
# given set of reference attributes. The $progress hash is used to keep track of which
# requests have been tried so far.

sub generate_request_url {
    
    my ($rm, $source, $progress, $attrs) = @_;
    
    # If the previous request returned a server error, try the same request again. The
    # calling method is responsible for waiting an appropriate amount of time between
    # requests.
    
    if ( $progress->{last_code} =~ /^5../ )
    {
	return $progress->{last_url};
    }
    
    # Otherwise, call the method appropriate to this datasource.
    
    elsif ( $source eq 'crossref' )
    {
	return $rm->generate_crossref_request($progress, $attrs);
    }
    
    elsif ( $source eq 'xdd' )
    {
	return $rm->generate_xdd_request($progress, $attrs);
    }
    
    else
    {
	croak "Unrecognized data source: '$source'";
    }
}


# generate_crossref_request ( progress, attrs )
# 
# Generate a request URL that will be used to query the Crossref dataset, trying to match
# the given set of reference attributes. The $progress hash is used to keep track of which
# requests have been tried so far.

sub generate_crossref_request {
    
    my ($rm, $progress, $attrs) = @_;

    # If the attributes include a DOI, try that first.
    
    if ( ! $progress->{try_doi} )
    {
	$progress->{try_doi} = 1;
	
	if ( my $doi = get_doi($attrs) )
	{
	    $progress->{query_text} = "doi: $doi";
	    
	    my $encoded = uri_escape_utf8($doi);
	    return "$CROSSREF_BASE/$encoded";
	}

	print STDERR "Crossref: no doi\n" if $rm->{debug_mode};
    }
    
    # If the attributes do not include a DOI, or if we have already tried that, then try a
    # bibliographic query.
    
    if ( ! $progress->{try_biblio} )
    {
	$progress->{try_biblio} = 1;
	
	my @title_words;
	my @container_words;
	my @author_words;
	
	# If the attributes include a reference title containing at least 3 letters in a
	# row, chop it up into words. Ignore punctuation, whitespace, and stopwords, and
	# put each word into foldcase.
	
	my $reftitle = get_reftitle($attrs);
	
	if ( $reftitle =~ /\pL{3}/ )
	{
	    push @title_words, title_words($reftitle);
	}
	
	# If the attributes include a publication year, add that too.
	
	my $pubyr = get_pubyr($attrs);
	
	# push @query_words, $pubyr if $pubyr;
	
	# If the attributes include a publication title which is different from the reference
	# title, do the same thing.
	
	my $pubtitle = get_pubtitle($attrs);
	
	if ( $pubtitle && $pubtitle ne $reftitle )
	{
	    push @container_words, title_words($pubtitle);
	}
	
	# Add up to 3 author lastnames from the reference attributes.
	
	foreach my $i (1..3)
	{
	    my ($lastname, $firstname) = get_authorname($attrs, $i);
	    
	    if ( $lastname && $lastname =~ /\pL{2}/ )
	    {
		push @author_words, grep { /\pL{2}/ } title_words($lastname);
	    }
	    
	    if ( $firstname && $firstname =~ /\pL{2}/ )
	    {
		push @author_words, grep { /\pL{2}/ } title_words($firstname);
	    }
	}
	
	# If we have enough attributes for a reasonable request, assemble and return it.
	
	if ( @title_words > 1 || @title_words && $pubyr ||
	     @title_words && @author_words ||
	     @title_words && @container_words ||
	     @author_words && @container_words ||
	     @author_words && $pubyr )
	{
	    my (@url_params, @text_params);
	    
	    if ( @title_words )
	    {
		my $value = join ' ', @title_words, $pubyr;
		
		push @url_params, "query.title=" . uri_escape_utf8($value);
		push @text_params, "title: $value";
	    }
	    
	    if ( $pubyr )
	    {
		push @url_params, "query.bibliographic=" . uri_escape_utf8($pubyr);
		push @text_params, "pubyr: $pubyr";
	    }
	    
	    if ( @container_words )
	    {
		my $value = join ' ', @container_words;
		
		push @url_params, "query.container-title=" . uri_escape_utf8($value);
		push @text_params, "container: $value";
	    }
	    
	    if ( @author_words )
	    {
		my $value = join ' ', @author_words;
		
		push @url_params, "query.contributor=" . uri_escape_utf8($value);
		push @text_params, "author: $value";
	    }
	    
	    $progress->{query_text} = join '; ', @text_params;
	    
	    push @url_params, "rows=5";
	    
	    my $query_string = join '&', @url_params;
	    return "$CROSSREF_BASE?$query_string";
	}

	print STDERR "Crossref: not enough bibliographic information.\n"
	    if $rm->{debug_mode};
    }
    
    # If we have tried both of these, return nothing.
    
    return;
}


# extract_crossref_records ( response_data, attrs )
#
# Extract data items from a Crossref response. Remove unnecessary keys from each item,
# so that we won't store information that is unnecessary for our purpose.

sub extract_crossref_records {

    my ($rm, $data, $attrs) = @_;
    
    if ( ref $data eq 'HASH' && ref $data->{message}{items} eq 'ARRAY' )
    {
	return map { $rm->clean_crossref_item($_) } @{$data->{message}{items}};
    }
    
    elsif ( ref $data eq 'HASH' && $data->{message}{deposited} )
    {
	return $rm->clean_crossref_item($data->{message});
    }
    
    elsif ( ref $data eq 'ARRAY' && $data->[0]{deposited} || $data->[0]{indexed} )
    {
	return $rm->clean_crossref_item($data);
    }

    elsif ( $data->{deposited} || $data->{indexed} )
    {
	return $rm->clean_crossref_item($data);
    }

    return;
}


sub clean_crossref_item {
    
    my ($rm, $data) = @_;

    delete $data->{reference};
    delete $data->{license};
    delete $data->{'content-domain'};
    
    foreach my $key ( 'created', 'deposited', 'indexed', 'published' )
    {
	if ( $data->{$key}{'date-time'} )
	{
	    $data->{$key} = $data->{$key}{'date-time'};
	}

	elsif ( ref $data->{$key}{'date-parts'} eq 'ARRAY' &&
		ref $data->{$key}{'date-parts'}[0] eq 'ARRAY' &&
		$data->{$key}{'date-parts'}[0][0] =~ /^\d+$/ )
	{
	    $data->{$key} = join '-', $data->{$key}{'date-parts'}[0]->@*;
	}
    }

    return $data;
}


# generate_xdd_request ( progress, attrs )
# 
# Generate a request URL that will be used to query the XDD dataset, trying to match
# the given set of reference attributes. The $progress hash is used to keep track of which
# requests have been tried so far.

sub generate_xdd_request {
    
    my ($rm, $progress, $attrs) = @_;
    
    # If the attributes include a DOI, try that first.
    
    if ( ! $progress->{try_doi} )
    {
	$progress->{try_doi} = 1;
	
	if ( my $doi = get_doi($attrs) )
	{
	    $progress->{query_text} = "doi: $doi";
	    
	    my $encoded = uri_escape_utf8($doi);
	    return "$XDD_BASE?doi=$encoded";
	}
	
	print STDERR "XDD: no doi\n" if $rm->{debug_mode};
    }
    
    # If the attributes do not include a DOI, or if we have already tried that, then try a
    # bibliographic query. It may be necessary to try several queries, due to the
    # deficiencies in the XDD api. The first step is to process the attributes.
    
    unless ( defined $progress->{pubyr} )
    {
	$progress->{pubyr} = (get_pubyr($attrs) || '');
	
	my $pubyr = $progress->{pubyr};
	my $minyr = $progress->{pubyr} - 1;
	my $maxyr = $progress->{pubyr} + 1;

	$progress->{pubyr_clause} = $progress->{pubyr} ?
	    "min_published=$pubyr&max_published=$pubyr" : '';
	$progress->{pubyr_loose} = $progress->{pubyr} ?
	    "min_published=$minyr&max_published=$maxyr" : '';
	$progress->{pubyr_text} = $progress->{pubyr} ? "pubyr: $progress->{pubyr}" : '';
	
	$progress->{reftitle} = (get_reftitle($attrs) || '');
	$progress->{pubtitle} = (get_pubtitle($attrs) || '');
	
	$progress->{reftitle_has_marks} = 1 if $progress->{reftitle} =~ /\pM/;
	$progress->{pubtitle_has_marks} = 1 if $progress->{pubtitle} =~ /\pM/;
	
	$progress->{refwords} = [ title_words($progress->{reftitle}, 1) ];
	$progress->{pubwords} = [ title_words($progress->{pubtitle}, 1) ];
	
	$progress->{reftitle_okay} = $progress->{reftitle} &&
	    length($progress->{reftitle}) >= 15 &&
	    $progress->{refwords}->@* > 3;
	
	$progress->{pubtitle_okay} = $progress->{pubtitle} &&
	    $progress->{pubwords}->@*;
	
	my ($author1last) = get_authorname($attrs, 1);
	my ($author2last) = get_authorname($attrs, 2);
	
	$progress->{author1last} = ($author1last || '');
	$progress->{author2last} = ($author2last || '');
	
	$progress->{author1_has_marks} = 1 if $author1last =~ /\pM/;
	$progress->{author2_has_marks} = 1 if $author2last =~ /\pM/;
    }
    
    # If we have a reftitle of at least 15 characters and at least 4 words, try that first and
    # see if it's in the top 3 results.
    
    if ( ! $progress->{try_title_only} )
    {
	$progress->{try_title_only} = 1;
	
	if ( $progress->{reftitle_okay} )
	{
	    return $rm->xdd_simple_request($progress,
				  { title_words => $progress->{refwords},
				    pubyr => $progress->{pubyr},
				    limit => 5 });
	}
    }
    
    # If that doesn't work, try the reftitle with author1, then with author2.
    
    if ( ! $progress->{try_title_auth1} )
    {
	$progress->{try_title_auth1} = 1;

	if ( $progress->{reftitle_okay} && $progress->{author1last} )
	{
	    return $rm->xdd_simple_request($progress,
				  { title_words => $progress->{refwords},
				    pub_words => $progress->{pubwords},
				    pubyr => $progress->{pubyr},
				    lastname => $progress->{author1last},
				    limit => 5 });
	}
    }
    
    if ( ! $progress->{try_title_auth2} )
    {
	$progress->{try_title_auth2} = 1;

	if ( $progress->{reftitle_okay} && $progress->{author2last} )
	{
	    return $rm->xdd_simple_request($progress,
				  { title_words => $progress->{refwords},
				    pub_words => $progress->{pubwords},
				    pubyr => $progress->{pubyr},
				    lastname => $progress->{author2last},
				    limit => 5 });
	}
    }
    
    print STDERR "XDD: not enough bibliographic information\n" if $rm->{debug_mode};
    
    # If we have tried all of these, return nothing.
    
    return;
    
    # # Since the XDD api only allows querying for a single author, the first try is made
    # # using the first author. If that fails, a second try will be made using the second
    # # author. If one or the other is entered correctly, 
    
    # if ( ! $progress->{try_biblio} == 2 )
    # {
    # 	my $author_no;
	
    # 	if ( $progress->{try_biblio} == 1 )
    # 	{
    # 	    $author_no = 2;
    # 	    $progress->{try_biblio} = 2;
    # 	}

    # 	else
    # 	{
    # 	    $author_no = 1;
    # 	    $progress->{try_biblio} = 1;
    # 	}
	
    # 	my $reftitle = get_reftitle($attrs);

    # 	my @title_words = title_words($reftitle);
	
    # 	my $pubyr = get_pubyr($attrs);
	
    # 	my $minyr = $pubyr-1;
    # 	my $maxyr = $pubyr+1;
	
    # 	my ($author_last) = get_authorname($attrs, $author_no);
	
    # 	if ( (@title_words || $pubyr) && $author_last )
    # 	{
    # 	    my (@url_params, @text_params);
	    
    # 	    if ( @title_words )
    # 	    {
    # 		my $value = join ' ', @title_words;
    # 		my $modified = join ',', map "$_*" @title_words;
		
    # 		push @url_params, "term=$modified";
    # 		push @text_params, "title: $value";
    # 	    }
	    
	    
    # 	my $pubyr_select = "&min_published=$minyr&max_published=$maxyr";
	
    # 	$query_text = "title=$title$pubyr_select";
    # 	$query_url = "https://xdd.wisc.edu/api/articles?max=5&" . $query_text;
    # 	}
}


sub xdd_simple_request {

    my ($rm, $progress, $params) = @_;
    
    my (@url_params, @text_params);
    
    if ( $params->{title_words} && ref $params->{title_words} eq 'ARRAY' &&
	 $params->{title_words}->@* )
    {
	my $string = join ' ', $params->{title_words}->@*;
	
	push @url_params, "title_like=" . uri_escape_utf8($string);
	push @text_params, "title: $string";
    }
    
    elsif ( my $title = $params->{title} )
    {
	push @url_params, "title_like=" . uri_escape_utf8($title);
	push @text_params, "title: $title";
    }
    
    if ( my $pubyr = $params->{pubyr} )
    {
	push @url_params, $progress->{pubyr_clause};
	push @text_params, "pubyr: $pubyr";
    }

    if ( my $lastname = $params->{lastname} )
    {
	push @url_params, "lastname=" . uri_escape_utf8($lastname);
	push @text_params, "lastname: $lastname";
    }
    
    my $limit = $params->{limit} || 5;
    
    push @url_params, "max=$limit";
    
    $progress->{query_text} = join '; ', @text_params;
    
    my $query_args = join '&', @url_params;
    
    return "$XDD_BASE?$query_args";
}


# extract_xdd_records ( response_data, attrs )
#
# Extract data items from an XDD response. Remove unnecessary keys from each item,
# so that we won't store information that is unnecessary for our purpose.

sub extract_xdd_records {

    my ($rm, $data, $attrs) = @_;

    if ( ref $data->{success}{data} eq 'ARRAY' )
    {
	return map { $rm->clean_xdd_item($_) } $data->{success}{data}->@*;
    }

    elsif ( $data->{success}{title} || $data->{success}{author} )
    {
	return $rm->clean_xdd_item($data->{success});
    }
    
    elsif ( $data->{title} || $data->{author} )
    {
	return $rm->clean_xdd_item($data);
    }

    return;
}


sub clean_xdd_item {
    
    my ($rm, $data) = @_;
    
    delete $data->{citation_list};
    
    return $data;
}








