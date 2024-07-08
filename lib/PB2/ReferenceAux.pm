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
    	{ param => 'ref_author', valid => MATCH_VALUE('.*\p{L}\p{L}.*'),
	  bad_value => '_', errmsg => $no_auth },
    	    "Return references for which any of the authors matches the specified",
	    "name or names. You can use any of the following patterns:",
	    "=over","=item Smith","=item J. Smith", "=item Smith and Jones", "=back",
	    "The last form selects only references where both names are listed as authors,",
	    "in any order.",
	    "You can use C<%> and C<_> as wildcards, but each name must contain at least one letter.",
	    "You can include more than one name, separated by commas.",
    	{ param => 'ref_primary', valid => MATCH_VALUE('.*\p{L}\p{L}.*'),
	  bad_value => '_', errmsg => $no_auth },
    	    "Return references for which the primary author matches the specified name",
	    "or names (see C<ref_author> above).  If you give a name like 'Smith and Jones',",
	    "then references are returnd for which Smith is the primary author and Jones is",
	    "also an author.",
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
	    "You may specify one or more, separated by commas.");
    
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
	{ output => 'source_url', com_name => 'url' },
	    "The request URL that was used to fetch this record",
	{ output => 'source_data', com_name => 'exd' },
	    "The externally fetched data that is the source for this record");

}


# match_local ( )
# 
# Return a list of local bibliographic references that match the specified
# attributes, roughly in decreasing order of similarity, date entered.

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
	    
	    $m->{score} = $score;
	    push @matches, $m;
	}
    }
    
    # If no doi was given or if no references with that doi were found, look for references that
    # match some combination of reftitle, pubtitle, pubyr, authors.
    
    unless ( @matches )
    {
	my (@filters, $having);
	
	my $ref_title = $request->clean_param("ref_title");
	my $pub_title = $request->clean_param("pub_title");
	
	# If we have a reftitle or a pubtitle, use the refsearch table for full-text matching.
	
	if ( $ref_title && $pub_title )
	{
	    my $refquoted = $dbh->quote($ref_title);
	    my $pubquoted = $dbh->quote($pub_title);

	    $fulltext = "match(rs.reftitle) against($refquoted) as score1,
		  match(rs.pubtitle) against ($pubquoted) as score2";
	    $having = "score1 > 5 and score2 > 5";
	}
	
	elsif ( $ref_title )
	{
	    my $quoted = $dbh->quote($ref_title);

	    $fulltext = "match(rs.reftitle) against($quoted) as score";
	    $having = "score > 5";
	}
	
	elsif ( $pub_title )
	{
	    my $quoted = $dbh->quote($pub_title);
	    
	    $fulltext = "match(rs.pubtitle) against($quoted) as score";
	    $having = "score > 0";
	}
	
	# Then add clauses to restrict the selection based on pubyr and author names.
	
	if ( my $ref_pubyr = $request->clean_param("ref_pubyr") )
	{
	    push @filters, $request->generate_pubyear_filter($ref_pubyr);
	}
	
	if ( my $authorname = $request->clean_param('ref_author') )
	{
	    push @filters, $request->generate_auth_filter($authorname, 'author');
	}
	
	if ( my $authorname = $request->clean_param('ref_primary') )
	{
	    push @filters, $request->generate_auth_filter($authorname, 'primary');
	}
	
	# Now put the pieces together into a single SQL statement and execute
	# it. But return an error if we don't have at least two different
	# attributes to match on.
	
	die $request->exception(400, "You must specify at one attribute to match")
	    unless @filters or $having;
	
	my $clause = join(' and ', @filters);
	
	$clause ||= '1';
	
	my $limit = $request->sql_limit_clause;
	
	my $calc = $request->sql_count_clause;
	
	if ( $fulltext )
	{
	    $sql = "SELECT $calc $fields, $fulltext
		FROM $TABLE{REFERENCE_DATA} as r
		     join $TABLE{REFERENCE_SEARCH} as rs using (reference_no)
		WHERE $clause
		HAVING $having $limit";
	}
	
	else
	{
	    $sql = "SELECT $calc $fields
		FROM $TABLE{REFERENCE_DATA} as r
		WHERE $clause $limit";
	}
	
	$request->debug_line("$sql\n") if $request->debug;
	
	my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
	
	foreach my $m ( @$result )
	{
	    if ( $m->{score1} || $m->{score2} )
	    {
		$m->{score} = $m->{score1} + $m->{score2};
	    }
	    
	    $m->{score} ||= 1;
	    
	    push @matches, $m;
	}
    }
    
    # Now sort the matches in descending order by score.
    
    my @sorted = sort { $b->{score} <=> $a->{score} } @matches;
    
    $request->list_result(@sorted);
    
    $request->sql_count_rows;
    
    return 1;
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
    
    if ( my $authorname = $request->clean_param('ref_author') )
    {
	$attrs->{author} = $authorname;
    }
    
    if ( my $authorname = $request->clean_param('ref_primary') )
    {
	$attrs->{author} = $authorname;
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
	unless keys %$attrs > 1 || $attrs->{doi};
    
    my (@matches);
    
    push @matches, $rm->external_query('crossref', $attrs);
    push @matches, $rm->external_query('xdd', $attrs);
    
    my @sorted = sort { $b->{score} <=> $a->{score} } grep { $_->{score} } @matches;
    
    # print STDERR "==========\n";
    # print STDERR to_json(\@matches) . "\n";
    # print STDERR "==========\n";
    
    $request->list_result(@sorted);
}


1;
