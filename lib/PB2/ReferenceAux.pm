#  
# ReferenceAux
# 
# This role provides operations for matching references based on sparse attributes
# 
# Author: Michael McClennen

use strict;

package PB2::ReferenceAux;

use HTTP::Validate qw(:validators);

use TableDefs qw(%TABLE);

use CoreTableDefs;
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);

use ReferenceManagement;
use ReferenceMatch qw(parse_authorname author_similarity);

use Carp qw(carp croak);
use JSON qw(to_json);

use Moo::Role;

our (@REQUIRES_ROLE) = qw(PB2::Authentication PB2::CommonData PB2::ReferenceData);

# initialize ( )
# 
# This routine is called by the Web::DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    my $no_auth = "invalid author name {value}, must contain at least two letters";
    my $no_letter = "the value of {param} must contain at least two letters (was {value})";
    
    $ds->define_ruleset('1.2:refs:match' =>
    	{ param => 'ref_author', valid => MATCH_VALUE('\p{L}\p{L}'),
	  bad_value => '_', errmsg => $no_auth },
    	    "Return references for which any of the authors matches the specified",
	    "name or names. You can use any of the following patterns:",
	    "=over","=item Smith","=item J. Smith", "=item Smith and Jones", "=back",
	    "The last form selects only references where both names are listed as authors,",
	    "in any order.",
	    "You can use C<%> and C<_> as wildcards, but each name must contain at least one letter.",
	    "You can include more than one name, separated by commas.",
    	{ param => 'ref_primary', valid => MATCH_VALUE('\p{L}\p{L}'),
	  bad_value => '_', errmsg => $no_auth },
    	    "Return references for which the primary author matches the specified name",
	    "or names (see C<ref_author> above).  If you give a name like 'Smith and Jones',",
	    "then references are returnd for which Smith is the primary author and Jones is",
	    "also an author.",
	{ at_most_one => ['ref_author', 'ref_primary'] },
	{ param => 'ref_pubyr', valid => \&PB2::ReferenceData::valid_pubyr, alias => 'pubyr' },
	    "Return references published during the indicated year or range of years.",
	    "The parameter value must match one of the following patterns:",
	    "=over","=item 2000","=item 1990-2000", "=item 1990-", "=item -2000", "=back",
	{ param => 'ref_title', valid => MATCH_VALUE('.*\p{L}\p{L}.*'), errmsg => $no_letter },
	    "Return references whose title matches the specified word or words.  You can",
	    "use C<%> and C<_> as wildcards, but the value must contain at least one letter.",
    	{ param => 'pub_title', valid => MATCH_VALUE('.*\p{L}\p{L}.*'), errmsg => $no_letter },
	    "Select only references from publications whose title matches the specified",
	    "word or words.  You can use C<%> and C<_> as wildcards, but the value must",
	    "contain at least one letter.",
	# { param => 'ref_abbr', valid => MATCH_VALUE('.*\p{L}\p{L}.*'), errmsg => $no_letter },
	#     "Select only references whose abbreviation matches the specified word or words.  You can",
	#     "use C<%> and C<_> as wildcards, but the value must contain at least one letter.",
	#     "This is especially useful when searching for museum collections by collection acronym.",
    	# { param => 'pub_abbr', valid => MATCH_VALUE('.*\p{L}\p{L}.*'), errmsg => $no_letter },
	#     "Select only references from publications whose abbreviation matches the specified",
	#     "word or words.  You can use C<%> and C<_> as wildcards, but the value",
	#     "must contain at least one letter.",
	#     "This is especially useful when searching for museum collections by institution acronym.",
	{ param => 'ref_doi', valid => ANY_VALUE },
	    "Select only records entered from references with any of the specified DOIs.",
	    "You may specify one or more, separated by commas.",
	{ optional => 'loose', valid => FLAG_VALUE },
	    "If specified, return all records regardless of the fuzzy match threshold.");
    
    $ds->define_output_map('1.2:refs:matchext_map' =>
	{ value => 'formatted' },
	    "If this option is specified, show the formatted reference instead of",
	    "the individual fields.",
	{ value => 'both' },
	    "If this option is specified, show both the formatted reference and",
	    "the individual fields",
	{ value => 'authorlist' },
	    "Show a single list of authors instead of separate fields",
	{ value => 'source', maps_to => '1.2:refs:extrequest' },
	   "Show information about the external source of each record");
    
    $ds->define_ruleset('1.2:refs:display_ext' =>
	{ optional => 'show', valid => '1.2:refs:matchext_map', list => ',' },
	    "Indicates additional information to be shown along",
	    "with the basic record.  The value should be a comma-separated list containing",
	    "one or more of the following values:");
    
    $ds->define_ruleset('1.2:refs:matchlocal' =>
	">>You must provide at least two of the following parameters:",
	{ allow => '1.2:refs:match' },
	">>You can also specify any of the following parameters:",
    	{ allow => '1.2:refs:display' },
    	{ allow => '1.2:special_params' },
    	"^You can also use any of the L<special parameters|node:special>");
    
    $ds->define_ruleset('1.2:refs:matchext' =>
	">>You must provide at least two of the following parameters:",
	{ allow => '1.2:refs:match' },
	">>You can also specify any of the following parameters:",
    	{ allow => '1.2:refs:display_ext' },
    	{ allow => '1.2:special_params' },
    	"^You can also use any of the L<special parameters|node:special>");
    
    $ds->define_block('1.2:refs:extrequest' =>
	{ output => 'source', com_name => 'src' },
	    "The external source from which this record was fetched",
	{ output => 'source_url', com_name => 'req' },
	    "The request URL that was used to fetch this record",
	{ output => 'source_data', com_name => 'exd' },
	    "The externally fetched data that is the source for this record");
    
    $ds->define_ruleset('1.2:refs:classic_select' =>
    	{ require => '1.2:refs:specifier' },
    	{ allow => '1.2:refs:display' },
    	{ allow => '1.2:special_params' },
    	"^You can also use any of the L<special parameters|node:special>");
        
}


# match_local ( )
# 
# Return a list of local bibliographic references that match the specified
# attributes, roughly in decreasing order of similarity, date entered. This is a
# "fuzzy match" with respect to author and publication year, unlike the refs/list
# operation.

sub match_local {
    
    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    
    $request->substitute_select( cd => 'r' );
    
    my $fields = $request->select_string;
    
    my ($sql, $fulltext, @matches);
    
    # If a doi was given, find all references with that doi. Compare them all to the given
    # attributes; if no other attributes were given, each one gets a score of 90 plus the
    # number of important attributes with a non-empty value. The idea is that if there is
    # more than one we should select the matching reference record that has the greatest amount
    # of information filled in.
    
    if ( my $ref_doi = $request->clean_param('ref_doi') )
    {
	my $quoted = $dbh->quote($ref_doi);
	
	$sql = "SELECT $fields
		FROM $TABLE{REFERENCE_DATA} as r
		WHERE doi=$quoted";
	
	$request->debug_line("$sql\n") if $request->debug;
	
	my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
	
	# Assign match scores and add to the match list.
	
	if ( ref $result eq 'ARRAY' && @$result )
	{
	    foreach my $m ( @$result )
	    {
		my $score = 90;
		$score++ if $m->{reftitle};
		$score++ if $m->{pubtitle};
		$score++ if $m->{author1last};
		$score++ if $m->{pubvol};
		$score++ if $m->{pubno};
		$score++ if $m->{publisher};
		$score++ if $m->{firstpage};
		
		$m->{r_relevance} = $score;
	    }
	    
	    push @matches, sort { $b->{r_relevance} <=> $a->{r_relevance} } @$result;
	}
    }
    
    # Then, look for references that match some combination of reftitle,
    # pubtitle, pubyr, authors.
    
    my (@filters, $having, $order);
    
    my $ref_title = $request->clean_param("ref_title");
    my $pub_title = $request->clean_param("pub_title");
    
    # If we have a reftitle or a pubtitle, use the refsearch table for full-text matching.
    
    if ( $ref_title && $pub_title )
    {
	my $refquoted = $dbh->quote($ref_title);
	my $pubquoted = $dbh->quote($pub_title);
	
	$fulltext = "match(r.reftitle) against($refquoted) as score1,
		  match(r.pubtitle) against ($pubquoted) as score2";
	$having = "score1 > 5";
	$order = "ORDER BY score1 + 0.5 * score2 desc";
    }
    
    elsif ( $ref_title )
    {
	my $quoted = $dbh->quote($ref_title);
	
	$fulltext = "match(r.reftitle) against($quoted) as score";
	$having = "score > 5";
	$order = "ORDER BY score desc";
    }
    
    else
    {
	$request->add_error("E_REQUIRED: you must provide a non-empty value for 'ref_title' with this operation");
	die $request->exception(400, "Bad request");
    }
    
    # If a publication year was specified, add a filter.
    
    my $exact_pubyr;
    
    if ( my $ref_pubyr = $request->clean_param("ref_pubyr") )
    {
	if ( $ref_pubyr =~ /^\d\d\d\d$/ )
	{
	    my $list = join("','", $ref_pubyr-1, $ref_pubyr, $ref_pubyr+1);
	    push @filters, "r.pubyr in ('$list')";
	    $exact_pubyr = $ref_pubyr;
	}
	
	elsif ( $ref_pubyr =~ /^(\d\d\d\d)\s*-\s*(\d\d\d\d)$/ )
	{
	    push @filters, "r.pubyr >= '$1'";
	    push @filters, "r.pubyr <= '$2'";
	}
	
	elsif ( $ref_pubyr =~ /^(\d\d\d\d)\s*-\s*$/ )
	{
	    push @filters, "r.pubyr >= '$1'";
	}
	
	elsif ( $ref_pubyr =~ /^\s*-\s*(\d\d\d\d)$/ )
	{
	    push @filters, "r.pubyr <= '$1'";
	}
	
	else
	{
	    my $quoted = $dbh->quote($ref_pubyr);
	    $request->add_error("E_FORMAT: ref_pubyr: bad value $quoted");
	    die $request->exception(400, "Bad request");
	}
    }
    
    my ($authorname, $author_is_primary, $a1last, $a1first, $a2last, $a2first);
    
    if ( $authorname = $request->clean_param('ref_primary') )
    {
	$author_is_primary = 1;
    }
    
    else
    {
	$authorname = $request->clean_param('ref_author');
    }
    
    if ( $authorname =~ /(.*?) and (.*)/ )
    {
	my $name1 = $1;
	my $name2 = $2;
	
	($a1last, $a1first) = parse_authorname($name1);
	($a2last, $a2first) = parse_authorname($name2);
    }
    
    else
    {
	($a1last, $a1first) = parse_authorname($authorname);
    }
    
    my $clause = join(' and ', @filters);
    
    $clause ||= '1';
    
    my $calc = $request->sql_count_clause;
    
    my $authorlist = "group_concat(concat_ws(';',lastname,firstname) separator '|') as authorlist";
    
    $sql = "SELECT $calc $fields, $authorlist, $fulltext
	    FROM $TABLE{REFERENCE_DATA} as r
		left join $TABLE{REFERENCE_AUTHORS} as ra using (reference_no)
	    WHERE $clause
	    GROUP BY reference_no
	    HAVING $having LIMIT 500";
    
    $request->debug_line("$sql\n") if $request->debug;
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    if ( ref $result eq 'ARRAY' && @$result )
    {
	my $total_sim;
	my $max_relevance = $result->[0]{score} || $result->[0]{score1} + 0.5 * $result->[0]{score2};
	
	foreach my $m ( @$result )
	{
	    # Title/publication similarity
	    
	    my $relevance = $m->{score} || $m->{score1} + 0.5 * $m->{score2};
	    
	    my $text_sim = int( 100 * $relevance / $max_relevance + 0.5 );
	    
	    # Author similarity
	    
	    my $author_sim;
	    
	    my @authors = $request->extract_authorlist($m);
	    
	    my $r1last = $authors[0]{lastname};
	    my $r1first = $authors[0]{firstname};
	    
	    my ($sim1, $con1) = author_similarity($a1last, $a1first, $r1last, $r1first);
	    
	    if ( $author_is_primary )
	    {
		if ( $sim1 >= 50 && $a2last )
		{
		    my $sim2 = 0;
		    
		    foreach my $i ( 1..$#authors )
		    {
			my ($sim, $con) = author_similarity($a2last, $a2first,
							    $authors[$i]{lastname},
							    $authors[$i]{firstname});
			
			$sim2 = $sim if $sim > $sim2;
		    }
		    
		    $author_sim = 0.5 * $sim1 + 0.5 * $sim2;
		}
		
		elsif ( $a2last )
		{
		    $author_sim = 0.5 * $sim1;
		}
		
		else
		{
		    $author_sim = $sim1;
		}
	    }
	    
	    else
	    {
		if ( $sim1 < 90 )
		{
		    foreach my $i ( 1..$#authors )
		    {
			my ($sim, $con) = author_similarity($a1last, $a1first,
							    $authors[$i]{lastname},
							    $authors[$i]{firstname});
			
			$sim1 = $sim if $sim > $sim1;
		    }
		}
		
		if ( $a2last )
		{
		    my $sim2 = 0;
		    
		    foreach my $i ( 0..$#authors )
		    {
			my ($sim, $con) = author_similarity($a2last, $a2first,
							    $authors[$i]{lastname},
							    $authors[$i]{firstname});
			
			$sim2 = $sim if $sim > $sim2;
		    }
		    
		    $author_sim = 0.5 * $sim1 + 0.5 * $sim2;
		}
		
		else
		{
		    $author_sim = $sim1;
		}
		
		$m->{author_sim} = $author_sim;
	    }
	    
	    # Pubyr similarity
	    
	    my $pubyr_sim;
	    
	    if ( $m->{r_pubyr} && $exact_pubyr )
	    {
		$pubyr_sim = $m->{r_pubyr} eq $exact_pubyr ? 100 : 50;
	    }
	    
	    else
	    {
		$pubyr_sim = 100;
	    }
	    
	    $m->{r_relevance} = $text_sim + $author_sim + $pubyr_sim;
	    $m->{r_relevance_a} = "T$text_sim A$author_sim P$pubyr_sim";
	}
	
	push @matches, sort { $b->{r_relevance} <=> $a->{r_relevance} } 
	    grep { $_->{r_relevance} >= 200 && $_->{author_sim} > 0 } @$result;
    }
    
    $request->list_result(@matches);
    
    $request->sql_count_rows;
    
    return 1;
}


sub extract_authorlist {
    
    my ($request, $record) = @_;
    
    my @authors;
    
    if ( $record->{authorlist} )
    {
	my @extracted = split /[|]/, $record->{authorlist};
	
	foreach my $i ( 0..$#extracted )
	{
	    my ($last, $first) = split /;/, $extracted[$i];
	    
	    push @authors, PB2::ReferenceData::bibjson_name_record($first, $last);
	}
    }
    
    else
    {
	push @authors, PB2::ReferenceData::bibjson_name_record($record->{r_ai1}, $record->{r_al1}) if $record->{r_al1};
	push @authors, PB2::ReferenceData::bibjson_name_record($record->{r_ai2}, $record->{r_al2}) if $record->{r_al2};
	push @authors, PB2::ReferenceData::bibjson_name_list($record->{r_oa}) if $record->{r_oa};
    }
    
    return @authors;
}


sub match_external {
    
    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    
    my $attrs = { };
    
    my $rm = ReferenceManagement->new($dbh);
    
    $rm->debug_mode(1) if $request->debug;
    
    if ( my $ref_doi = $request->clean_param("ref_doi") )
    {
	$attrs->{doi} = $ref_doi;
    }
    
    if ( my $ref_pubyr = $request->clean_param("ref_pubyr") )
    {
	$attrs->{pubyr} = $ref_pubyr;
    }
    
    my $authorname = '';
    
    if ( $authorname = $request->clean_param('ref_primary') )
    {
	$attrs->{author_is_primary} = 1;
    }
    
    else
    {
	$authorname = $request->clean_param('ref_author');
    }
    
    if ( $authorname =~ /(.+?) \s+ and \s+ (.*)/x )
    {
	my $auth1 = $1;
	my $auth2 = $2;
	
	$attrs->{author} = [ clean_author($auth1), clean_author($auth2) ];
    }
    
    else
    {
	$attrs->{author} = clean_author($authorname);
    }
    
    if ( my $reftitle = $request->clean_param('ref_title') )
    {
	$attrs->{reftitle} = $reftitle;
    }
    
    if ( my $pubtitle = $request->clean_param('pub_title') )
    {
	$attrs->{pubtitle} = $pubtitle;
    }
    
    die $request->exception(400, "You must specify at least two attributes to match")
	unless keys %$attrs > 1 || $attrs->{doi} || $attrs->{ref_title};
    
    if ( $request->clean_param('loose') )
    {
	$attrs->{loose_match} = 1;
    }
    
    my (@matches);
    
    push @matches, $rm->external_query('crossref', $attrs);
    # push @matches, $rm->external_query('xdd', $attrs);
    
    if ( my $abort = $matches[0]{status} )
    {
	if ( $abort =~ /^(4..)/ && $abort !~ /^404/ )
	{
	    die $request->exception(500, "Got $1 response from external resource");
	}
	
	elsif ( $abort =~ /^(5..)/ )
	{
	    die $request->exception(502, "Got $1 response from external resource");
	}
    }
    
    my @sorted = sort { $b->{r_relevance} <=> $a->{r_relevance} } grep { $_->{r_relevance} } @matches;
    
    $request->list_result(@sorted);
}


sub clean_author {
    
    my ($authorname) = @_;
    
    if ( $authorname =~ /[*]/ )
    {
	$authorname =~ s/\s*[*]\s*//g;
	return { name => $authorname };
    }
    
    else
    {
	$authorname =~ s/\s*$//;
	$authorname =~ s/^\s*//;
	
	return $authorname;
    }
}


# classic_select ( )
# 
# Select the specified reference for the current session in the Classic
# environment. 

sub classic_select {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $request->clean_param('ref_id');
    
    die "Bad identifier '$id'" unless defined $id and $id =~ /^\d+$/;
    
    $request->strict_check;
    $request->extid_check;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( cd => 'r' );
    
    my $fields = $request->select_string;
    
    my $tables = $request->tables_hash;
    my $join_list = $request->generate_join_list($tables);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $fields
	FROM $TABLE{REFERENCE_DATA} as r $join_list
        WHERE r.reference_no = $id
	GROUP BY r.reference_no";
    
    print STDERR $request->{main_sql} . "\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    return unless $request->{main_record};
    
    my $session_id = Dancer::cookie('session_id');
    
    my $quoted_id = $dbh->quote($session_id);
    
    my ($sql, $s);
    
    if ( $session_id )
    {
	$sql = "SELECT authorizer_no, enterer_no, user_id, superuser as is_superuser, role,
		       timestampdiff(day,record_date,now()) as days_old, expire_days
		FROM $TABLE{SESSION_DATA} WHERE session_id = $quoted_id";
	    
	print STDERR "$sql\n\n" if $request->{debug};
    
	$s = $dbh->selectrow_hashref($sql);
    }
    
    die $request->exception(401, "You must log in first") unless $s;
    
    die $request->exception(401, "Unauthorized") unless $s->{role} =~ /^auth|^ent|^stu/;
    
    my $reference_no = $request->{main_record}{reference_no};
    
    $sql = "UPDATE $TABLE{SESSION_DATA} SET reference_no = '$reference_no'
		WHERE session_id = $quoted_id";
    
    my $result = $dbh->do($sql);
    
    my $a = 1;	# we can stop here when debugging
}

1;
