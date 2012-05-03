# 
# The Paleobiology Database
# 
#   TaxaTree.pm
# 

=head1 General Description

This module builds and maintains a hierarchy of taxonomic names.  This
hierarchy is based on the data in the C<opinions> and C<authorities> tables,
and is stored in the tables C<taxon_trees> and C<taxa_parents>.  These tables
are also referred to extensively throughout the rest of the database code,
because the taxonomic hierarchy is central to the organization of the data in
the database.

=head2 Definitions

Each distinct taxonomic name/rank combination represented in the database has
a unique entry in the C<authorities> table, and a unique internal id number
(taxon_no) assigned to it in that table.  In the documentation for this
database, we use the term I<taxon> to represent the concept "distinct
taxonomic name/rank combination".  So, for example, "Rhizopodea" as a class
and "Rhizopodea" as a phylum are considered to be distinct I<taxa> in this
database.  This is necessary because the database stores a continuum of
historical data; when a taxon's rank or spelling has been changed at some
point in the past, we need to have a way of properly representing taxonomic
opinions from both before and after the change.  Thus we need to store both
combinations as separate entries in the database.  When talking about a taxon
in the sense of a grouping of organisms irrespective of spelling and rank
changes, we will use the term "taxonomic concept".

For each taxon in the database (in other words, for each distinct name/rank
combination) we algorithmically select a "best opinion" from the entries in
the C<opinions> table, representing the most recent and reliable taxonomic
opinion that specifies a relationship between this taxon and the rest of the
taxonomic hierarchy.  These "best opinions" are then used to arrange the taxa
into a collection of trees.  Note that the taxa do not form a single tree,
because there are a number of fossil taxa for which it is not known within
which higher taxa they fall.

=head2 Organization of taxa

The entries in C<taxon_trees> are in 1-1 correspondence with the entries in the
C<authorities> table, linked by the key field C<taxon_no>.  These taxa are further
organized according to four separate relations, each computed from data in the
C<opinions> table.  The names listed in parentheses are the fields in
C<taxon_trees> which record each relation:

=over 4

=item Taxaonomic concept group (orig_no)

This relation groups together all of the name/rank combinations that represent
the same taxonomic concept.  In other words, when a taxon's rank is changed,
or its spelling is changed, all of the resultant entries will have a different
C<taxon_no> but the same C<orig_no>.  The C<orig_no> for each entry is equal
to the C<taxon_no> of its original combination (so all original combinations
have C<taxon_no> = C<orig_no>).

Note that this relation can also be taken as an equivalence relation, whereas
two taxa have the same C<orig_no> if and only if they represent the same
taxonomic concept.

=item Concept group leader (spelling_no)

This relation selects from each taxonomic concept the currently accepted
name/rank combination.  The value of C<spelling_no> for any entry is the
C<taxon_no> of the currently accepted combination for its concept group.
Thus, all members of a given concept group have the same value of
C<spelling_no>.

We will refer to this taxon as the "group leader" in the remainder of this
documentation.  Note that a group leader always has C<taxon_no> =
C<spelling_no>.

=item Synonymy (synonym_no)

This relation groups together all of the taxonomic concepts which are
considered to be synonyms of each other.  Two taxa are considered to be
synonymous if one is a subjective or objective synonym of the other, or was
replaced by the other, or if one is an invalid subgroup or nomen dubium, nomen
vanum or nomen nudum inside the other.

The value of C<synonym_no> is the C<taxon_no> associated with the group leader
of the most senior synonym for the given entry.  Thus, the currently accepted
combination for the most senior synonym for any entry can always be quickly
and efficiently looked up.  A group leader which has no senior synonym always
has C<synonym_no> = C<spelling_no> = C<taxon_no>.

Note that this is a relation on concept groups, not on taxa; all of the
members of a concept group have the same C<synonym_no>.  All concept groups
which are synonyms of each other will have the same C<synonym_no>, but
different C<spelling_no>.  This relation, like the concept group relation
above, can also be taken as an equivalence relation, whereas two taxa have the
same C<synonym_no> if and only if they are synonyms of each other.

=item Hierarchy (parent_no)

This relation associates lower with higher taxa.  It forms a collection of
trees, because (as noted above) there are a number of fossil taxa for which it
is not known within which higher taxa they fall.

Any taxon which does not fall within another taxon in the database (either
because no such relationship is known or because it is a maximally general
taxon) will have C<parent_no> = 0.

All taxa which are synonyms of each other will have the same C<parent_no>
value, and the C<parent_no> (if not 0) will always refer to the parent of the
most senior synonym.  The value of C<parent_no> is always a taxon which is a
concept group leader and not a junior synonym.  Thus, a parent taxon will
always have C<synonym_no> = C<taxon_no> = C<spelling_no>.

This relation, like the previous ones, can be taken as an equivalence
relation, whereas two taxa have the same C<parent_no> if and only if they are
siblings of each other.

=back

=head2 Opinions

In addition to the fields listed above, each entry in C<taxon_trees> also has
an C<opinion_no> field.  This field points to the "best opinion" (most recent
and reliable) that has been algorithmically selected from the available
opinions for that taxon.

For a junior synonym, the value of opinion_no will be the opinion which
specifies its immediately senior synonym.  There may exist synonym chains in
the database, where A is a junior synonym of B which is a junior synonym of
C.  In any case, C<synonym_no> should always point to the most senior synonym
of each taxon.

For a senior synonym, or for any taxon which does not have synonyms, the value
of C<opinion_no> will be the opinion which specifies its immediately higher
taxon.  Note, however, that the opinion may specify a different spelling than
the value of parent_no, because parent_no always points to the concept group
leader no matter what the opinion says.

=head2 Tree structure

In order to facilitate logical operations on the taxa hierarchy, the entries
in C<taxon_trees> are marked via preorder tree traversal.  This is done with
fields C<lft> and C<rgt>.  The C<lft> field stores the traversal sequence, and
the C<rgt> field of a given entry stores the maximum C<lft> value of the entry
and all of its descendants.  An entry which has no descendants has C<lft> =
C<rgt>.  Using these fields, we can formulate simple and efficient SQL queries
to fetch all of the descendants of a given entry and other similar operations.
For more information, see L<http://en.wikipedia.org/wiki/Nested_set_model>.

All entries in the same concept group have the same C<lft> and C<rgt> values.

The one operation that is not easy to do using the preorder traversal sequence
is to compute the list of all parents of a given taxon.  Thus, we use a
separate table, C<taxon_parents> to store this information.  This table has
just two fields, C<parent_no> and C<child_no>, and stores the transitive
closure of the hierarchy relation.

=head2 Additional Tables

One auxiliary table is needed in order to properly compute the relations
described above.  This table, called C<suppress_opinions> is needed because
the synonymy and hierarchy relations must be structured as collections of
trees.  Unfortunately, the set of opinions stored in the database often
generates cycles in both of these relations.  For example, there will be cases
in which the best opinion on taxon A states that it is a subjective synonym of
B, while the best opinion on taxon B states that it is a subjective synonym of
A.  In order to resolve this, the algorithm that computes the synonymy and
hierarchy relations must break each cycle by choosing the best (most recent
and reliable) opinion from those that define the cycle and suppressing any
opinion that contradicts the chosen one.  The C<suppress_opinions> table
records which opinions are so suppressed.

=head1 Interface

The interface to this module is as follows (note: I<this is a draft
specification>).  In the following documentation, the parameter C<dbh> is
always a database handle.  Note that routines whose names begin with "get"
return objects, while those whose names begin with "list" return lists of
taxon identifiers.

=over 4

=item addTaxon ( dbh, taxon_no )

Add the given taxon to the tree.  This taxon must previously have been entered
into the C<authorities> table.  The taxon will be added without parents,
synonyms or spellings; therefore, it will be necessary to subsequently call
either C<rebuild()> or C<updateOpinion()> in order to link it to the rest of the
taxa tree.

=item updateOpinion ( dbh, opinion_no ... )

Adjust the taxa tree, taking into account any changes that have been made to
the given opinions.  Some or all of these opinions may have been newly created
or deleted.

=item getCurrentSpelling ( dbh, taxon_no )

Returns an object representing the concept group leader (current spelling) of
the given taxon.

=item listCurrentSpelling ( dbh, taxon_no )

Returns the taxon number of the concept group leader (current spelling) of the
given taxon, which may be the given taxon itself.

=item getAllSpellings ( dbh, taxon_no )

Returns a list of objects representing all taxa that fall into the same
concept group as the given taxon.  The first object in the list will be the
group leader.

=item listAllSpellings ( dbh, taxon_no )

Returns a list of taxon identifiers, listing all taxa that fall into the same
concept group as the given taxon.  The first item in the list will be the
group leader.

=item getOriginalCombination ( dbh, taxon_no )

Returns an object representing the original combination of the given taxon,
which may be the taxon itself.

=item listOriginalCombination ( dbh, taxon_no )

Returns the taxon identifier for the original combination of the given taxon,
which may be the taxon itself.

=item getMostSeniorSynonym ( dbh, taxon_no )

Returns an object representing the most senior synonym of the given taxon.  If
the taxon does not have a senior synonym, an object representing the taxon
itself is returned.

=item getAllSynonyms ( dbh, taxon_no )

Returns a list of objects representing all synonyms of the given taxon, in
order of seniority.  If the given taxon has no synonyms, a single object is
returned representing the taxon itself.

=item listAllSynonyms ( dbh, taxon_no )

Returns a list of taxon identifiers, listing all synonyms (junior and senior)
of the given taxon as well as the taxon itself.  These are ordered senior to junior.

=item getParent ( dbh, taxon_no )

Returns an object representing the parent of the given taxon.  If there is
none, then nothing is returned.

=item listParent ( dbh, taxon_no )

Returns the taxon identifier of the parent of the given taxon, or 0
if there is none.

=item getImmediateParent ( dbh, taxon_no )

Returns an object representing the immediate parent of the given taxon, which
may not be the same as the one referred to by C<parent_no>.  It may be a
junior synonym instead, if the relevant opinions so state.

=item getParents ( dbh, taxon_no )

Returns a list of objects representing all parents of the given taxon, in
order from highest to lowest.

=item listParents ( dbh, taxon_no )

Returns a list of taxon identifiers representing all parents of the given
taxon, in order from highest to lowest.

=item getChildren ( dbh, taxon_no, options )

Returns a list of objects representing the children of the given taxon.  If
the option "include_synonyms" is true, then synonyms of the children are
included as well.  If the option "all" is true, then all descendants are
included, not just immediate children.

=item listChildren ( dbh, taxon_no, options )

Returns a list of taxon identifiers for all children of the given taxon.  If
the option "include_synonyms" is true, then synonyms of the children are
included as well.  If the option "all" is true, then all descendants are
included, not just immediate children.

=item getHistory ( dbh, taxon_no )

Returns a list of objects representing all taxa which have had the same
conceptual meaning as the given taxon at some point in history.  These are
sorted by publication date, and include some but not all synonyms (i.e. not
invalid subgroups of the given taxon).

=back

=cut

package TaxaTree;

use Data::Dumper;
use CGI::Carp;
use TaxonInfo;
use Text::JaroWinkler qw( strcmp95 );
use Constants qw($TAXA_TREE_CACHE $TAXA_LIST_CACHE $IS_FOSSIL_RECORD);

use strict;

# Main table names

our $TREE_TABLE = "taxon_trees";
our $PARENT_TABLE = "taxon_parents";
our $SUPPRESS_TABLE = "suppress_opinions";

# Backup names for use with RENAME TABLES

our $TREE_BAK = "taxa_cache_bak";
our $PARENT_BAK = "taxa_parents_bak";
our $SUPPRESS_BAK = "suppress_bak";

# Working table names

our $TREE_TEMP = "tn";
our $PARENT_TEMP = "pn";
our $SUPPRESS_TEMP = "sn";

# Auxiliary table names

our $OPINION_TEMP = "order_opinions";
our $SPELLING_TEMP = "spelling_opinions";
our $BEST_TEMP = "best_opinions";

=item rebuild ( dbh, step )

Completely rebuild the taxa tree from the C<opinions> and C<authorities>
tables.  This is safe to run while the database is in active use.

=cut

# In order to be safe to run while the database is in use, we operate on
# temporary tables which are then atomically swapped for the actual ones via a
# single RENAME TABLE operation.  While this has been going on, authorities
# and opinions may have been added or modified.  Therefore, we must keep track
# of which ones those are, and at the very end of the procedure we must alter
# the taxa tables to reflect the changed opinions.

# For debugging purposes only, we accept a second parameter %step to indicate
# which of the steps should be carried out.

sub rebuild {
    
    my ($dbh, $step) = @_;
    
    # We start by saving the time at which this procedure starts.  Any
    # authorities or opinions added or updated after this time must be taken
    # into account at the very end.
    
    my ($start_time) = $dbh->selectrow_array("SELECT now()");
    
    unless ( defined $step and ref $step eq 'HASH' )
    {
	$step = { 'a' => 1, 'b' => 1, 'c' => 1, 'd' => 1, 'e' => 1,
		  'f' => 1, 'g' => 1 };
    }
    
    # The following steps are required in order to carry out a full rebuild of
    # the taxon tree tables:
    
    createNewTables($dbh) if $step->{a};
    groupByConcept($dbh) if $step->{b};
    computeSpelling($dbh) if $step->{c};
    computeSynonymy($dbh) if $step->{d};
    computeHierarchy($dbh) if $step->{e};
    computeTreeFields($dbh) if $step->{f};
    activateNewTables($dbh) if $step->{g};
}    

my $NEW_TABLE;		# For debugging purposes, this allows us to properly
                        # zero out the relevant columns if we are repeatedly
                        # re-running various steps in this process.

# createNewTables ( dbh )
# 
# Create the new tables needed for a taxon trees rebuild.  These consist of
# three new tables that will eventually become the taxon tree tables, plus two
# auxiliary tables that are used to select the best opinions for each taxon.
    
sub createNewTables {
  
    my ($dbh) = @_;
    
    my ($result);
    
    print STDERR "Rebuild: creating temporary tables (-a)\n";
    $DB::single = 1;
    
    # We start by re-creating all of the temporary tables, dropping the old ones
    # if they still exist.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $TREE_TEMP");
    $result = $dbh->do("CREATE TABLE $TREE_TEMP 
			       (taxon_no int unsigned not null,
				orig_no int unsigned not null,
				spelling_no int unsigned not null,
				synonym_no int unsigned not null,
				parent_no int unsigned not null,
				opinion_no int unsigned not null,
				lft int unsigned not null,
				rgt int unsigned not null,
				depth int unsigned not null,
				PRIMARY KEY (taxon_no),
				KEY (orig_no),
				KEY (spelling_no),
				KEY (synonym_no),
				KEY (parent_no),
				KEY (opinion_no),
				KEY (lft),
				KEY (rgt))");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $PARENT_TEMP");
    $result = $dbh->do("CREATE TABLE $PARENT_TEMP
			       (parent_no int unsigned,
				child_no int unsigned,
				KEY (parent_no),
				KEY (child_no))");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SUPPRESS_TEMP");
    $result = $dbh->do("CREATE TABLE $SUPPRESS_TEMP
			       (opinion_no int unsigned,
				suppress int unsigned,
				PRIMARY KEY (opinion_no))");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $OPINION_TEMP");
    $result = $dbh->do("CREATE TABLE $OPINION_TEMP
			  (orig_no int unsigned not null,
			   opinion_no int unsigned not null,
			   ri int not null,
			   pubyr varchar(4),
			   status enum('belongs to','subjective synonym of','objective synonym of','invalid subgroup of','misspelling of','replaced by','nomen dubium','nomen nudum','nomen oblitum','nomen vanum'),
			   spelling_reason enum('original spelling','recombination','reassignment','correction','rank change','misspelling'),
			   child_no int unsigned not null,
			   child_spelling_no int unsigned not null,
			   parent_no int unsigned not null,
			   parent_spelling_no int unsigned not null,
			   KEY (orig_no), KEY (opinion_no))");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SPELLING_TEMP");
    $result = $dbh->do("CREATE TABLE $SPELLING_TEMP
			    (orig_no int unsigned,
			     spelling_no int unsigned,
			     is_misspelling boolean,
			     PRIMARY KEY (orig_no))");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $BEST_TEMP");
    $result = $dbh->do("CREATE TABLE $BEST_TEMP
			    (orig_no int unsigned,
			     parent_no int unsigned,
			     opinion_no int unsigned,
			     ri int not null,
			     pubyr varchar(4),
			     status enum('belongs to','subjective synonym of','objective synonym of','invalid subgroup of','misspelling of','replaced by','nomen dubium','nomen nudum','nomen oblitum','nomen vanum'),
			     PRIMARY KEY (orig_no))");
    
    # Now we populate $OPINIONS_TEMP with opinion data in the proper order.
    # This is much more efficient than using a view (verified by
    # experimentation using MySQL 5.1).  This table will be used as an auxiliary
    # in computing the four taxon relations.
    
    print STDERR "Rebuild:     populating opinion table\n";
    
    # This query is adapated from the old getMostRecentClassification()
    # routine, from TaxonInfo.pm line 2003.
    
    $result = $dbh->do("LOCK TABLE opinions as o read, refs as r read, $OPINION_TEMP write");
    
    $result = $dbh->do("INSERT INTO $OPINION_TEMP
		SELECT o.child_no, o.opinion_no, 
			if(o.pubyr IS NOT NULL AND o.pubyr != '', o.pubyr, r.pubyr) as pubyr,
			IF ((o.basis != '' AND o.basis IS NOT NULL), CASE o.basis
			WHEN 'second hand' THEN 1
			WHEN 'stated without evidence' THEN 2
			WHEN 'implied' THEN 2
			WHEN 'stated with evidence' THEN 3 END,
			IF(r.reference_no = 6930, 0, CASE r.basis
				WHEN 'second hand' THEN 1
				WHEN 'stated without evidence' THEN 2
				WHEN 'stated with evidence' THEN 3
				ELSE 2 END)) AS ri,
			o.status, o.spelling_reason, o.child_no, o.child_spelling_no,
			o.parent_no, o.parent_spelling_no
		FROM opinions o LEFT JOIN refs r USING (reference_no)
		ORDER BY ri DESC, pubyr DESC, opinion_no DESC");
    
    $result = $dbh->do("UNLOCK TABLES");
    
    $NEW_TABLE = 1;		# we can stop here when debugging.
}


# groupByConcept ( dbh )
# 
# Fill in the taxon_no and orig_no fields of $TREE_TEMP.  This computes the
# "taxonomic concept group" relation.

sub groupByConcept {

    my ($dbh) =  @_;
    
    my ($result);
    
    print STDERR "Rebuild: computing taxonomic concept group relation (-b)\n";
    $DB::single = 1;
    
    # The following algorithm is adapted from the old getOriginalCombination()
    # routine from TaxonInfo.pm, line 2139.
    
    # We start by checking for taxa that are mentioned in some opinion as
    # "child_spelling_no" and set the "original combination" as the
    # "child_no" from that opinion.  If there is more than one such opinion,
    # we take the oldest first, and if there are still conflicts, the one
    # one with the lowest opinion number (the one entered first.)
    
    print STDERR "Rebuild:     setting orig_no, phase 1\n";
    
    $result = $dbh->do("INSERT IGNORE INTO $TREE_TEMP (taxon_no, orig_no)
			SELECT distinct child_spelling_no, child_no
			FROM $OPINION_TEMP
			WHERE child_no != 0 and child_spelling_no != 0
			ORDER BY pubyr ASC, opinion_no ASC");
    
    # Next, for we check for taxa that are mentioned in some opinion as
    # "child_no", but not in any opinion as "child_spelling_no".  These taxa
    # are their own "original combinations".  We filter out those which were
    # caught in the last statement by using INSERT IGNORE.  Since we're
    # looking for all taxa which meet this condition, we don't need to order
    # the result.
    
    print STDERR "Rebuild:     setting orig_no, phase 2\n";
    
    $result = $dbh->do("INSERT IGNORE INTO $TREE_TEMP (taxon_no, orig_no)
			SELECT child_no, child_no
			FROM $OPINION_TEMP WHERE child_no != 0");
    
    # There are still some taxa we have not gotten yet, so we next check for
    # taxa that are mentioned in some opinion as a "parent_spelling_no", and
    # set the original combination to "parent_no" from that opinion.  Again,
    # we need to order by publication year and opinion_no in case there are
    # conflicts.
    
    print STDERR "Rebuild:     setting orig_no, phase 3\n";
    
    $result = $dbh->do("INSERT IGNORE INTO $TREE_TEMP (taxon_no, orig_no)
			SELECT distinct parent_spelling_no, parent_no
			FROM $OPINION_TEMP
			WHERE parent_spelling_no != 0 and parent_no != 0
			ORDER BY pubyr ASC, opinion_no ASC");
    
    # Every taxon not caught so far is its own "original combination" by
    # default.  We want every taxon in authorities to have a corresponding
    # entry in taxon_trees, so we add everything that's not already there.
    
    print STDERR "Rebuild:     setting orig_no, phase 4\n";
    
    $result = $dbh->do("INSERT IGNORE INTO $TREE_TEMP (taxon_no, orig_no)
			SELECT taxon_no, taxon_no
			FROM authorities a");
    
    my $a = 1;		# we can stop here when debugging
};


# computeSpelling ( dbh )
# 
# Fill in the spelling_no field of $TREE_TEMP.  This computes the "concept
# group leader" relation.  We do this by selecting the best opinion (most
# recent and reliable) for each concept group.  Unless the best opinion is
# recorded as a misspelling, we take its spelling to be the accepted one.
# Otherwise, we look for the best spelling match among the available opinions
# using the Jaro-Winkler algorithm.

sub computeSpelling {

    my ($dbh) = @_;
    
    my ($result);
    
    print STDERR "Rebuild: computing concept group leader relation (-c)\n";
    $DB::single = 1;
    
    # At this point in the rebuilding process we have the taxa grouped by
    # concept, and the next step is to select the single best (most reliable
    # and recent) spelling opinion for each concept group.  Note that orig_no
    # is a unique key on $SPELLING_TEMP, so by using INSERT IGNORE on a
    # selection which is already in the proper order, we can easily pick out
    # the best opinion.
    
    $result = $dbh->do("
		INSERT IGNORE INTO $SPELLING_TEMP
		SELECT child_no, child_spelling_no,
		       if(spelling_reason = 'misspelling', true, false)
		FROM $OPINION_TEMP");
    
    # The problematic cases are the ones where the best opinion is marked as a
    # misspelling.  For each of these cases, we will have to grab all opinions
    # for the concept group and find the closest spelling match.
    
    # First fetch the orig_no of each concept group for which this is the
    # case, along with the actual misspelled name.
    
    print STDERR "Rebuild:     downloading spellings\n";
    
    my $misspellings = $dbh->prepare("
			SELECT s.orig_no, a.taxon_name
			FROM $SPELLING_TEMP s JOIN authorities a ON a.taxon_no = s.spelling_no
			WHERE s.is_misspelling");
    
    $misspellings->execute();
    
    my (%misspelling, %best_match, %best_coeff, %seen,
	$orig_no, $taxon_no, $taxon_name);
    
    while ( ($orig_no, $taxon_name) = $misspellings->fetchrow_array() )
    {
	$misspelling{$orig_no} = $taxon_name;
    }
    
    # Then, fetch all of the candidate spellings for each of these
    # misspellings.  We select them in descending order of publication year,
    # so that if equal-weight misspellings are found then we choose the most
    # recent one.
    # 
    # For each possible spelling, we compute the candidate's Jaro-Winkler
    # coefficient against the known misspelling.  If this is better than the
    # best match previously found, it becomes the preferred spelling for this
    # taxon.  Note that the Jaro-Winkler coefficient runs from 0 (no similarity)
    # to 1 (perfect similarity).
    
    print STDERR "Rebuild:     looking for good spellings\n";
    
    my $candidates = $dbh->prepare("
			SELECT DISTINCT o.orig_no, o.child_spelling_no, a.taxon_name
			FROM $OPINION_TEMP o JOIN $SPELLING_TEMP s USING (orig_no)
				JOIN authorities a ON a.taxon_no = o.child_spelling_no
			WHERE s.is_misspelling and o.spelling_reason != 'misspelling'
			ORDER BY o.pubyr DESC");
		       
    $candidates->execute();
    
    while ( ($orig_no, $taxon_no, $taxon_name) = $candidates->fetchrow_array() )
    {
	next if $taxon_name eq $misspelling{$orig_no};
	next if $seen{$orig_no . $taxon_name};
	
	my ($alen) = length($taxon_name);
	my ($blen) = length($misspelling{$orig_no});
	my ($coeff) = strcmp95( $taxon_name, $misspelling{$orig_no},
				   ($alen > $blen ? $alen : $blen) );
	
	if ( !defined $best_coeff{$orig_no} or $coeff > $best_coeff{$orig_no} )
	{
	    $best_coeff{$orig_no} = $coeff;
	    $best_match{$orig_no} = $taxon_no;
	}
	
	$seen{$orig_no . $taxon_name} = 1;
    }
    
    # Now we fix all of the misspellings that we identified above.
    
    my $fix_spelling = $dbh->prepare("UPDATE $SPELLING_TEMP SET spelling_no = ?
				      WHERE orig_no = ?");
    
    foreach $orig_no (keys %misspelling)
    {
	my $spelling_no = $best_match{$orig_no} || $orig_no;
	
	$fix_spelling->execute($spelling_no, $orig_no);
    }
    
    # Next, we copy all of the computed spelling_no values into $TREE_TEMP.
    
    print STDERR "Rebuild:     setting spelling_no\n";
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN $SPELLING_TEMP s USING (orig_no)
			SET t.spelling_no = s.spelling_no");
    
    # Finally, in every row of $TREE_TEMP that still has a spelling_no of 0,
    # we set spelling_no = taxon_no.  In other words, taxa which have no
    # associated opinions are automatically their own group leader.
    
    $result = $dbh->do("UPDATE $TREE_TEMP SET spelling_no = taxon_no WHERE spelling_no = 0");
    
    my $a = 1;		# we can stop on this line when debugging
}


# setSynonymy ( dbh )
# 
# Fill in the synonym_no field of $TREE_TEMP.  This computes the "synonymy"
# relation.  We also fill in the opinion_no field for each entry that
# represents a junior synonym.
# 
# Before setting the synonym numbers, we need to download the entire synonymy
# relation (i.e. the set of junior/senior synonym pairs) and look for cycles.
# Unfortunately, the database does contain some of these.  For example, in
# some opinion, A. might assert that "x is a junior synonym of y" while in
# another opinion B. asserts "y is a junior synonym of x".  Other cycles might
# involve 3 or more different taxa.  Whenever we find such a cycle, we must
# compare the relevant opinions to find the most recent and reliable one.
# Whichever taxon that opinion asserts to be the senior synonym will be taken
# to be so; the next opinion in the cycle, which asserts that this taxon is a
# junior synonym, will be suppressed.
# 
# The suppressed opinions are recorded in the table suppress_temp.  Once all
# cycles are identified and the appropriate records added to suppress_temp,
# a followup cycle check is performed.  This is necessary because whenever an
# opinion is suppressed, the next-best opinion might cause a new cycle.
# Consequently, we repeat this process until there are no cycles remaining.
# 
# At that point, we can fill in the synonym_no field in the taxa tree cache,
# and the opinion_no field for junior synonyms.  The opinion_no for senior
# synonyms and for taxa which are not synonyms will be filled in by
# computeHierarchy.

sub computeSynonymy {
    
    my ($dbh) = @_;
    
    my ($result, $filter, @check_taxa, %taxa_moved);
    
    print STDERR "Rebuild: computing synonymy relation (-d)\n";
    $DB::single = 1;
    
    $result = $dbh->do("UPDATE $TREE_TEMP SET synonym_no = 0, opinion_no = 0") unless $NEW_TABLE;
    
    # We start by computing the best opinion (most recent and reliable) for
    # each taxonomic concept group (identified by orig_no) so that we can then
    # fetch the subset of those opinions that indicate synonymy.  This is a
    # slightly different set of opinions than for computing spelling, so we
    # need a separate table for it.
    
    # We use the same mechanism to select the best opinion as we did
    # previously with the spelling opinions.  The table has orig_no as a
    # unique key, and we INSERT IGNORE with a selection that is already
    # properly ordered.
    
    # We ignore opinions that are 'misspelling of', because they should affect
    # spelling numbers only and not synonymy or hierarchy.  We also ignore any
    # opinion where child_no = parent_no, since we are concerned here with
    # relationships between different taxonomic concept groups.
    
    $result = $dbh->do("INSERT IGNORE INTO $BEST_TEMP
			SELECT orig_no, parent_no, opinion_no, ri, pubyr, status
			FROM order_opinions
			WHERE status != 'misspelling of' and child_no != parent_no
				and child_spelling_no != parent_no");
    
    # Now we select the subset of these "best opinions" which indicate synonymy.
    
    print STDERR "Rebuild:     downloading synonymy opinions\n";
    
    my $synonym_opinions = $dbh->prepare("
		SELECT orig_no, parent_no, opinion_no, ri, pubyr
		FROM $BEST_TEMP b
		WHERE status != 'belongs to' AND parent_no != 0");
    
    $synonym_opinions->execute();
    
    # The %juniors array lists all of the taxa that are asserted to be junior
    # synonyms of a given taxon.  The %opinions array is indexed by the junior
    # taxon number, and holds the information necessary to determine the most
    # recent and reliable opinion from any given set.
    
    my (%juniors, %opinions);
    
    while ( my ($junior, $senior, $op, $ri, $pub) =
			$synonym_opinions->fetchrow_array() )
    {
	$juniors{$senior} = [] unless exists $juniors{$senior};
	push @{$juniors{$senior}}, $junior;
	
	$opinions{$junior} = [$senior, $ri, $pub, $op];
    }
    
    # Next, we check for cycles.  The result of the following call is a list
    # of pairs; each pair gives a taxon which is asserted to be a junior
    # synonym by an opinion which needs to be suppressed, along with the
    # opinion_no of the opinion in question.
    
    print STDERR "Rebuild:     checking for cycles...\n";
    
    my @breaks = breakCycles($dbh, \%juniors, \%opinions);
    
    print STDERR "Rebuild:         found " . scalar(@breaks) . " cycles \n";
    
    # As long as there are cycles to be broken, we suppress the indicated
    # opinions and then re-do the check (just in case the next-best
    # opinions cause a new cycle).
    
    while ( @breaks )
    {
	# Go through the cycle-breaking list, and insert the indicated
	# opinions into the $SUPPRESS_TEMP table.  We also keep track of the
	# associated taxa in @check_taxa.  We will use that list to fetch the
	# changed opinions, and also we need only check these taxa in our
	# followup cycle check because any new cycle must involve one of them.
	
	@check_taxa = ();
	
	foreach my $pair (@breaks)
	{
	    my ($check_taxon, $suppress_opinion) = @$pair;
	    
	    $result = $dbh->do("INSERT IGNORE INTO $SUPPRESS_TEMP
				    VALUES ($suppress_opinion, 1)");
	    
	    push @check_taxa, $check_taxon;
	}
	
	# Next, we update the $BEST_TEMP table by deleting the suppressed
	# opinions, then replacing them with the next-best opinion.  We need
	# to delete first, because there may be no next-best opinion!  (Also
	# because that way we can use INSERT IGNORE to pick the single best
	# opinion for each orig_no as above).
	
	if ( @check_taxa )
	{
	    $filter = '(' . join(',', @check_taxa) . ')';
	    
	    $result = $dbh->do("DELETE FROM best_opinions WHERE orig_no in $filter");
	    
	    $result = $dbh->do("
		INSERT IGNORE INTO $BEST_TEMP
		SELECT orig_no, parent_no, opinion_no, ri, pubyr, status
		FROM order_opinions LEFT JOIN $SUPPRESS_TEMP USING (opinion_no)
		WHERE suppress is null and orig_no in $filter
			and status != 'misspelling of' AND child_no != parent_no
				AND child_spelling_no != parent_no
		ORDER BY ri DESC, pubyr DESC, opinion_no DESC");
	}
	
	# In order to repeat the cycle check, we need to grab these new
	# opinions.
	
	$synonym_opinions = $dbh->prepare("
		SELECT orig_no, parent_no, ri, pubyr, opinion_no
		FROM $BEST_TEMP b
		WHERE status != 'belongs to' and parent_no != 0
			and b.orig_no in $filter");
	
	$synonym_opinions->execute();
	
	while ( my ($junior, $senior, $ri, $pub, $op) =
			$synonym_opinions->fetchrow_array() )
	{
	    $juniors{$senior} = [] unless exists $juniors{$senior};
	    push @{$juniors{$senior}}, $junior;

	    $taxa_moved{$junior} = $senior;	# We need to record each
                                                # taxon's new senior, because
                                                # there's no easy way to
                                                # delete from %juniors.
	    
	    $opinions{$junior} = [$senior, $ri, $pub, $op];
	}
	
	# Now we can do our follow-up cycle check, and bounce back to the top
	# of the loop if there are still cycles.  The %taxa_moved hash serves
	# two purposes here.  First, the check is restricted to its keys
	# because any new cycle must involve one of them, and second, it records
	# when an entry in %juniors should be ignored (because a new opinion
	# has changed that taxon's senior synonym).
	
	@breaks = breakCycles($dbh, \%juniors, \%opinions, \%taxa_moved);
	
	print STDERR "Rebuild:         found " . scalar(@breaks) . " cycles \n";
    }
    
    # Now that we've broken all of the cycles in $BEST_TEMP, we can use it
    # to set the synonym numbers in $TREE_TEMP.  We have to join with a second
    # copy of $TREE_TEMP to look up the spelling_no for each senior synonym,
    # because the synonym_no should always point to a spelling group leader no
    # matter what spelling the relevant opinion uses.
    
    print STDERR "Rebuild:     setting synonym_no, opinion_no\n";
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN $BEST_TEMP b USING (orig_no)
				JOIN $TREE_TEMP t2 ON b.parent_no = t2.taxon_no
			SET t.synonym_no = t2.spelling_no, t.opinion_no = b.opinion_no
			WHERE status != 'belongs to'");
    
    # Taxa which are not junior synonyms of other taxa are their own best
    # synonym.  So, for each row in which synonym_no = 0, we set it to
    # spelling_no.
    
    $result = $dbh->do("UPDATE $TREE_TEMP SET synonym_no = spelling_no WHERE synonym_no = 0");
    
    # So far we have computed the immediate senior synonym for each taxon, but
    # what we want is the *most senior* synonym for each taxon.  Thus, we need
    # to look for instances where a -> b and b -> c, and change the relation
    # so that a -> c and b -> c.  Because the chains may be more than three
    # taxa long, we need to repeat the following process until no more rows
    # are affected, with a limit of 20 just in case our algorithm above was
    # faulty and some cycles have slipped through.
    
    print STDERR "Rebuild:     de-chaining synonym_no\n";
    
    my $count = 0;
    
    do
    {
	$result = $dbh->do("UPDATE $TREE_TEMP t JOIN $TREE_TEMP t2
				ON t.synonym_no = t2.taxon_no and t.synonym_no != t2.synonym_no
			    SET t.synonym_no = t2.synonym_no");
    }
	while $result > 0 && ++$count < 20;
    
    if ( $count >= 20 )
    {
	print STDERR "Rebuild:     ERROR - possible synonymy cycle detected during de-chaining";
    }
    
    my $a = 1;		# we can stop on this line when debugging
}


# computeHierarchy ( dbh )
# 
# Fill in the parent_no field of $TREE_TEMP.  The parent_no for each taxon
# which is not itself a junior synonym is computed by determining the best
# (most recent and reliable) opinion from among those for the taxon and all
# its junior synonyms.  The parent_no for a junior synonym will always be the
# same as for its senior synonym.
# 
# This routine also fills in the opinion_no field for all entries which are
# not junior synonyms (those entries were already given an opinion_no by the
# computeSynonymy routine).
# 
# Just as with computeSynonymy above, we must first perform a cycle check.  If
# we find any cycles, we break them by adding more records to the same
# $SUPPRESS_TEMP table used by computeSynonymy.  The check is then repeated in
# case new cycles have been created by the newly selected opinions.

sub computeHierarchy {
    
    my ($dbh) = @_;
    
    my ($result, $filter, @check_taxa, %taxa_moved);
    
    print STDERR "Rebuild: computing hierarchy relation (-e)\n";
    $DB::single = 1;
    
    $dbh->do("UPDATE $TREE_TEMP SET parent_no = 0") unless $NEW_TABLE;
    
    # We already have the $BEST_TEMP relation, but some of the values in it
    # may be wrong because the best opinion for a senior synonym might
    # actually be associated with one of the junior synonyms.  So we need to
    # update the relation to take this into account.
    
    # The first step is to compute an auxiliary relation that associates the
    # spelling_no of each senior synonym with the the spelling_no values of
    # itself and all junior synonyms.  We will use the 'is_senior' field to
    # make sure that an opinion on a senior synonym is considered before an
    # opinion on a junior one, in case of ties in reliability index and
    # publication year.
    
    $result = $dbh->do("DROP TABLE IF EXISTS junior_temp");
    $result = $dbh->do("CREATE TABLE junior_temp
				(junior_no int unsigned,
				 senior_no int unsigned,
				 is_senior int unsigned,
				 primary key (junior_no),
				 key (senior_no))");
    
    # We start by adding all senior synonyms into the table, with is_senior=1
    
    $result = $dbh->do("INSERT IGNORE INTO junior_temp
			SELECT orig_no, orig_no, 1
			FROM $TREE_TEMP WHERE spelling_no = synonym_no");
    
    # Then, we add all immediately junior synonyms, but only subjective and
    # objective synonyms and replaced taxa.  We leave out nomen dubium, nomen
    # vanum, nomen nudum, and invalid subgroup, because an opinion on any of
    # those shouldn't affect the senior taxon.  All these entries have
    # is_senior=0
    
    $result = $dbh->do("INSERT IGNORE INTO junior_temp
			SELECT t.orig_no, o.parent_no, 0
			FROM $TREE_TEMP t JOIN order_opinions o USING (opinion_no)
			WHERE status in ('subjective synonym of', 'objective synonym of',
						'replaced by')");
    
    # Now we can select the best 'belongs to' opinion for each senior synonym
    # from the opinions pertaining to it and its junior synonyms. We use a
    # temporary table, so that we can orig_no as a unique key and then use
    # INSERT IGNORE with an appropriately ordered set to select the best
    # opinion for each orig_no.  Once the best opinions are computed, we
    # replace the belongs-to entries from $BEST_TEMP (i.e. the entries
    # representing senior synonyms) with the newly computed ones while leaving
    # the rest of the entries (those that represent junior synonyms) alone.
    
    print STDERR "Rebuild:     selecting best opinions\n";
    
    $result = $dbh->do("DROP TABLE IF EXISTS belongs_temp");
    $result = $dbh->do("CREATE TABLE belongs_temp
				(orig_no int unsigned,
				 opinion_no int unsigned,
				 PRIMARY KEY (orig_no))");
    
    $result = $dbh->do("INSERT IGNORE INTO belongs_temp
			SELECT t.orig_no, o.opinion_no
			FROM $TREE_TEMP t JOIN junior_temp j ON t.orig_no = j.senior_no
				JOIN order_opinions o ON o.orig_no = j.junior_no
			WHERE o.status = 'belongs to' and t.orig_no != o.parent_no
				and o.child_no != o.parent_no and o.child_spelling_no != o.parent_no
			ORDER BY o.ri DESC, o.pubyr DESC, j.is_senior DESC, o.opinion_no DESC");
    
    $result = $dbh->do("UPDATE $BEST_TEMP b JOIN belongs_temp x using (orig_no)
				JOIN order_opinions o ON o.opinion_no = x.opinion_no
			SET b.opinion_no = o.opinion_no, b.ri = o.ri, b.pubyr = o.pubyr,
				b.parent_no = o.parent_no
			WHERE b.status = 'belongs to'");
    
    # Next, we download this entire set so that we can look for cycles.
    
    print STDERR "Rebuild:     downloading hierarchy opinions\n";
    
    my $belongs_opinions = $dbh->prepare("
		SELECT orig_no, parent_no, ri, pubyr, opinion_no
		FROM $BEST_TEMP where parent_no != 0");
    
    $belongs_opinions->execute();
    
    # The %children array lists all of the taxa that are asserted to belong to
    # a given taxon.  The %opinions array is indexed by the child taxon
    # number, and holds the information necessary to determine the most recent
    # and reliable opinion from any given set.
    
    my (%children, %opinions);
    
    while ( my ($child, $parent, $ri, $pub, $op) =
			$belongs_opinions->fetchrow_array() )
    {
	if ( $child == $parent )
	{
	    print STDERR "Rebuild     ERROR: parent = child = $child ($op)\n";
	}
	
	$children{$parent} = [] unless exists $children{$parent};
	push @{$children{$parent}}, $child;
	
	$opinions{$child} = [$parent, $ri, $pub, $op];
    }
    
    print STDERR "Rebuild:     checking for cycles...\n";
    
    my @breaks = breakCycles($dbh, \%children, \%opinions);
    
    print STDERR "Rebuild:         found " . scalar(@breaks) . " cycles \n";
    
    # As long as there are cycles to be broken, we suppress the indicated
    # opinions and then re-check for cycles (just in case the next-best
    # opinions cause a new cycle).
    
    while ( @breaks )
    {
	# First go through the cycle-breaks already computed, and insert the
	# indicated opinions into the $SUPPRESS_TEMP table.  We also keep
	# track of the associated taxa, since any new cycle must involve one
	# of them.
	
	@check_taxa = ();
	
	foreach my $pair (@breaks)
	{
	    my ($check_taxon, $suppress_opinion) = @$pair;
	    
	    $result = $dbh->do("INSERT IGNORE INTO $SUPPRESS_TEMP
				    VALUES ($suppress_opinion, 1)");
	    
	    push @check_taxa, $check_taxon;
	}
	
	# We also have to clean up the belongs_opinions table, so that we can
	# compute the next-best opinion for each suppressed one.  So we delete
	# the suppressed opinions, and replace them with the next-best
	# opinion.  We need to delete first, because there may be no next-best
	# opinion!
	
	if ( @check_taxa )
	{
	    $filter = '(' . join(',', @check_taxa) . ')';
	    
	    $result = $dbh->do("DELETE FROM belongs_opinions WHERE orig_no in $filter");
	    
	    $result = $dbh->do("
		INSERT IGNORE INTO belongs_opinions
		SELECT j.senior_no, o.opinion_no
		FROM order_opinions o JOIN junior_temp j ON o.orig_no = j.junior_no
			LEFT JOIN $SUPPRESS_TEMP USING (opinion_no)
		WHERE o.status = 'belongs to' and j.senior_no in $filter
		ORDER BY o.ri DESC, o.pubyr DESC, j.is_senior DESC, o.opinion_no DESC");
	}
	
	# In order to repeat the cycle check, we need to grab these new
	# opinions.
	
	$belongs_opinions = $dbh->prepare("
		SELECT b.orig_no, o.parent_no, o.ri, o.pubyr, opinion_no
		FROM belongs_opinions b JOIN order_opinions o USING (opinion_no)
		WHERE b.orig_no != o.parent_no and o.parent_no != 0 and b.orig_no in $filter");
	
	$belongs_opinions->execute();
	
	while ( my ($child, $parent, $ri, $pub, $op) =
			$belongs_opinions->fetchrow_array() )
	{
	    $children{$parent} = [] unless exists $children{$parent};
	    push @{$children{$parent}}, $child;

	    $taxa_moved{$child} = $parent;
	    
	    $opinions{$child} = [$parent, $ri, $pub, $op];
	}
	
	# Now we can do our follow-up cycle check, and bounce back to the top
	# of the loop if there are still cycles.
	
	@breaks = breakCycles($dbh, \%children, \%opinions, \%taxa_moved);
	
	print STDERR "Rebuild:     found " . scalar(@breaks) . "cycles \n";
    }
    
    # Now that we have eliminated all cycles, we can set the opinion_no field
    # for everything that hasn't been set already (i.e. all taxa that are not
    # junior synonyms).
    
    print STDERR "Rebuild:     setting opinion_no\n";
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN $BEST_TEMP b USING (orig_no)
			SET t.opinion_no = b.opinion_no
			WHERE t.opinion_no = 0");
    
    # Then, we set the parent_no for all taxa.  All taxa in a synonym group
    # will share the same parent, but we need the orig_no of the
    # synonym_no. So, we have to join on $TREE_TEMP a second time to look that
    # up. We also need to record the synonym_no of the indicated parent, so we
    # join on $TREE_TEMP a third time to look that one up!
    
    print STDERR "Rebuild:     setting parent_no\n";
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN $TREE_TEMP t2 ON t2.taxon_no = t.synonym_no
				JOIN order_opinions o ON o.opinion_no = t2.opinion_no
				JOIN $TREE_TEMP t3 ON t3.taxon_no = o.parent_no
			SET t.parent_no = t3.synonym_no");
    
    my $a = 1;
}


# breakCycles ( dbh, juniors, opinions, overrides )
# 
# Given a record of junior-senior taxon pairs keyed on the senior taxon
# (%$juniors) and a listing of opinion information keyed on the junior taxon
# (%$opinions), locate any cycles and return a list of (orig_no, opinion_no)
# pairs which, when suppressed, will eliminate those cycles.  This routine is
# called both by computeSynonymy and computeHierarchy.
# 
# Cycles are located by taking each key of %$juniors in turn and doing a
# breadth-first search.  We prune the search whenever we encounter a node
# we've seen already, and stop whenever we encounter a node that we've seen
# already on *this search*.  In that case, we have a cycle.
#
# If $overrides is specified, it must be a hash ref.  The keys specify the
# taxa to check, while the values indicate the current senior of each one.
# This is used on the second and subsequent calls, after the first set of
# cycles is broken and some of the links in %$juniors have been changed.

sub breakCycles {
  
    my ($dbh, $juniors, $opinions, $overrides) = @_;
    
    my (%seen, $t, $u, @result);
    
    # Go through all of the senior taxa in turn.  For each taxon, do a
    # breadth-first search through its junior taxa, looking for cycles.  If
    # $overrides is specified, just check its keys.  Otherwise, check the keys
    # of the %$juniors hash.
    
    my $base = $overrides || $juniors;
    
 keys:
    foreach $t (keys %$base)
    {
	next if $seen{$t};	# skip any key we've seen already, because
                                # we've already resolved it one way or the
                                # other.
	
	my @search = ($t);	# the breadth-first search queue
	my $cycle_found = 0;	# a flag to indicate that we found a cycle
	
	# Look for a cycle starting at $t.  Note that any given taxon can be
	# part of at most one cycle.
	
	while ( @search )
	{
	    $u = shift @search;
	    
	    # If we found a node that we've visited before on this
	    # iteration of the 'keys' loop, then we have identified a
	    # cycle.
	    
	    if ( $seen{$u} == $t )
	    {
		$cycle_found = 1;
		last;
	    }
	    
	    # Otherwise, if we found a node that we already visited on a
	    # different iteration, it can't possibly be part of the same
	    # cycle as $t.  So we can prune the search.
	    
	    elsif ( $seen{$u} )
	    {
		next;
	    }
	    
	    # Otherwise, we mark this node as visited.
	    
	    $seen{$u} = $t;
	    
	    # If this node has any children, add them to the search queue.
	    # But ignore any child whose senior has been changed to a
	    # different taxon (this is recorded in %$overrides).
	    
	    if ( exists $juniors->{$u} )
	    {
		if ( $overrides )
		{
		    foreach my $key ( @{$juniors->{$u}} )
		    {
			unless ( exists $overrides->{$key} &&
					$overrides->{$key} != $u )
			{
			    push @search, $key;
			}
		    }
		}
		
		else
		{
		    push @search, @{$juniors->{$u}};
		}
	    }
	}
	
	# If we have found a cycle, we then need to compare the set of
	# opinions that make it up, and determine which is the best (in other
	# words, the most recent and reliable.)  The *next* opinion in the
	# cycle will need to be suppressed, because it conflicts with the best
	# one.
	
	if ( $cycle_found )
	{
	    # anchor the comparison at an arbitrary point in the cycle,
	    # then move forward one link before starting to compare.
	    
	    my $best = my $start = $u;
	    $u = $opinions->{$u}[0];
	    
	    # keep following links until we get back to the start, looking
	    # for the opinion with (1) the best reliability index, (2) the
	    # most recent publication year, (3) the most recent opinion
	    # number to break ties.
	    
	    until ( $u == $start )
	    {
		if ( $opinions->{$u}[1] > $opinions->{$best}[1] ) {
		    $best = $u;
		}
		elsif ( $opinions->{$u}[1] == $opinions->{$best}[1] && 
			$opinions->{$u}[2] > $opinions->{$best}[2] ) {
		    $best = $u;
		}
		elsif ( $opinions->{$u}[1] == $opinions->{$best}[1] &&
			$opinions->{$u}[2] == $opinions->{$best}[2] &&
			$opinions->{$u}[3] > $opinions->{$best}[3] ) {
		    $best = $u;
		}
		
		$u = $opinions->{$u}[0];
	    }
	    
	    # Now that we've found the best opinion, we add the next opinion
	    # in the cycle to the result set.
	    
	    my $recompute_taxon = $opinions->{$best}[0];
	    my $suppress_opinion = $opinions->{$recompute_taxon}[3];
	    
	    push @result, [$recompute_taxon, $suppress_opinion];
	}
	
	# If we didn't find a cycle for this instance of $t, just go on to the
	# next one.
    }
    
    return @result;
}


our (%children, %parent, %tree);

# computeTreeFields ( dbh )
# 
# Arrange the rows of $TREE_TEMP into a Nested Set tree by filling in the lft,
# rgt and depth fields.  For more information, see:
# http://en.wikipedia.org/wiki/Nested_set_model.

sub computeTreeFields {
    
    my ($dbh) = @_;
    
    print STDERR "Rebuild: traversing and marking taxon trees (-f)\n";
    $DB::single = 1;
    
    $dbh->do("UPDATE $TREE_TEMP SET lft=0, rgt=0, depth=0") unless $NEW_TABLE;
    
    print STDERR "Rebuild:     downloading hierarchy relation\n";
    
    my $pc_pairs = $dbh->prepare("SELECT distinct spelling_no, parent_no
				  FROM $TREE_TEMP
				  WHERE parent_no > 0"); 
    
    $pc_pairs->execute();
    
    while ( my ($child_no, $parent_no) = $pc_pairs->fetchrow_array() )
    {
	$children{$parent_no} = [] unless defined $children{$parent_no};
	push @{$children{$parent_no}}, $child_no;
	print STDERR "ERROR: $child_no has multiple parents: $parent{$child_no}, $parent_no\n"
	    if exists $parent{$child_no};
	$parent{$child_no} = $parent_no;
    }
    
    # Now we build the tree, starting with taxon 1 'Eukaryota' at the top of
    # the tree with lft=1 and depth=1.
    
    print STDERR "Rebuild:     traversing tree rooted at 'Eurkaryota'\n";
    
    my $lft = computeTreeValues(1, 1, 1, 0);
    
    # Next, we go through all of the other taxa and insert the ones that
    # haven't yet been inserted.  This is necessary because many taxa have no
    # known parents.
    
    print STDERR "Rebuild:     traversing all other taxon trees\n";
    
 taxon:
    foreach my $taxon (keys %children)
    {
	next if exists $tree{$taxon};
	
	my %seen;
	my $t = $taxon;
	
	while ( $parent{$t} )
	{
	    $seen{$t} = 1;
	    if ( $seen{$parent{$t}} )
	    {
		print STDOUT "Tree violation: $t => $parent{$t}\n";
		next taxon;
	    }
	    $t = $parent{$t};
	}
	
	$lft = computeTreeValues($t, $lft+1, 1, 0);
    }
    
    # Now we need to actually insert all of those values back into the
    # database. 
    
    print STDERR "Rebuild:     setting lft, rgt, depth\n";
    
    my $sth = $dbh->prepare("UPDATE $TREE_TEMP SET lft=?, rgt=?, depth=? WHERE spelling_no=?");
    
    foreach my $taxon (keys %tree)
    {
	$sth->execute(@{$tree{$taxon}}, $taxon)
    }
    
    my $a = 1;
}


# computeTreeValues ( taxon, lft, depth )
# 
# This recursive procedure is given a taxon number plus the 'lft' and 'depth'
# values which that taxon should take in order to be placed properly in the
# tree.  It creates tree nodes for this taxon and all of its descendants, and
# fills in the proper 'lft', 'rgt' and 'depth' values for them all.  The
# procedure returns the maximum 'rgt' value from all of the children, which in
# turn is used to fill in the parent's 'rgt' value.

sub computeTreeValues {

    my ($taxon, $lft, $depth, $parent) = @_;
    
    # First check that we haven't seen this taxon yet.  That should always be
    # true, unless there are cycles in the parent-child relation (which would
    # be bad!)
    
    unless ( exists $tree{$taxon} )
    {
	# Create a new node to represent the taxon.
	
	$tree{$taxon} = [$lft, $lft, $depth];
	
	# If this taxon has children, we must then iterate through them and
	# insert each one (and all its descendants) into the tree.  When we
	# are finished, we fill in this node's 'rgt' field with the value
	# returned from the last child.
	
	if ( exists $children{$taxon} )
	{
	    foreach my $child ( @{$children{$taxon}} )
	    {
		$lft = computeTreeValues($child, $lft + 1, $depth + 1, $taxon);
	    }
	    
	    # When we are finished with all of the children, fill in the 'rgt'
	    # field with the value returned from the last child.
	    
	    $tree{$taxon}[1] = $lft;
	}
	
	# If there are no children, we just leave the node as it is.
	
	return $lft;
    }
    
    # If we have already seen this taxon, then we have a cycle!  Print a
    # warning, and otherwise ignore it.  This means that we have to return
    # $lft - 1, because we didn't actually insert any node.
    
    else
    {
	print STDERR "Warning: tree cycle for taxon $taxon (parent $parent)\n";
	return $lft - 1;
    }
}


sub activateNewTables {

    my ($dbh) = @_;
    
    
    
}


1;
