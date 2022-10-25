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
use ReferenceMatch qw(get_reftitle get_pubtitle get_publisher get_authorname
		      get_pubyr get_doi title_words ref_similarity @SCORE_VARS);




# new ( dbh )
# 
# Create a new ReferenceManagement instance, which can be used to do queries on the table.

sub new {
    
    my ($dbh) = @_;
    
    croak "you must specify a database handle" unless $dbh;
    
    croak "invalid database handle '$dbh'" unless blessed $dbh && 
	ref $dbh =~ /\bDBI\b/;
    
    my $instance = { dbh => $dbh };
    
    return bless $instance;
}


# query_local_refs ( dbh, attrs )
# 
# Return a list of local bibliographic references that match the specified attributes, in
# decreasing order of similarity, date entered.

sub query_local_refs {
    
    my ($rm, $attrs) = @_;
    
    croak "you must specify a hashref of reference attributes" unless
	ref $attrs eq 'HASH';
    
    # If a doi was given, find all references with that doi. Compare them all to the given
    # attributes; if no other attributes were given, each one gets a score of 90 plus the
    # number of important attributes with a non-empty value. The idea is that if there is
    # more than one we should select the matching reference record that has the greatest amount
    # of information filled in.
    
    if ( $attrs->{doi} )
    {
	my $quoted = $dbh->quote($attrs->{doi});
	my $filter = "doi=$quoted";
	
	my $sql = $ds->generate_ref_query($selector, $fields, $filter, $options);
	
	push @matches, $ds->select_records($sql, 'return', $options);
	
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
	
	my @clauses;
	
	if ( $attrs->{pubyr} )
	{
	    my $quoted = $dbh->quote($attrs->{pubyr});
	    push @clauses, "refs.pubyr = $quoted";
	}
	
	if ( $attrs->{author1last} && $attrs->{author2last} )
	{
	    my $quoted1 = $dbh->quote($attrs->{author1last});
	    my $quoted2 = $dbh->quote($attrs->{author2last});
	    
	    push @clauses, "(refs.author1last sounds like $quoted1 and " .
		"refs.author2last sounds like $quoted2)";
	}
	
	elsif ( $attrs->{author1last} )
	{
	    my $quoted1 = $dbh->quote($attrs->{author1last});
	    
	    push @clauses, "refs.author1last sounds like $quoted1";
	}

	if ( $attrs->{anyauthor} )
	{
	    my $quoted1 = $dbh->quote($attrs->{anyauthor});
	    my $quoted2 = $dbh->quote('%' . $attrs->{anyauthor} . '%');
	    
	    push @clauses, "(refs.author1last sounds like $quoted1 or " .
		"refs.author2last sounds like $quoted1 or refs.otherauthors like $quoted2)";
	}
	
	# Now put the pieces together into a single SQL statement and execute it.

	push @clauses, "($attrs->{filter})" if $attrs->{filter};

	my $filter = join(' and ', @clauses);

	my $sql = $ds->generate_ref_base($selector, $fields, $filter, $options);
	
	$sql .= "\n\tHAVING $having" if $having;
	
	my @other, $ds->select_records($sql, 'return', $options);
	
	$ds->{selection_sql} = $sql;
	
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

    
    
}









