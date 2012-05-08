# 
# The Paleobiology Database
# 
#   TaxaTree.pm
# 

=head1 General Description

This module builds and maintains a hierarchy of taxonomic names.  This
hierarchy is based on the data in the C<opinions> and C<authorities> tables,
and is stored in the tables C<taxon_trees> and C<taxon_ancestors>.  These
tables are also referred to extensively throughout the rest of the database
code, because the taxonomic hierarchy is central to the organization of the
data in the database.

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
in C<taxon_trees> are sequenced via preorder tree traversal.  This is recorded
in the fields C<lft> and C<rgt>.  The C<lft> field stores the traversal
sequence, and the C<rgt> field of a given entry stores the maximum sequence
number of the entry and all of its descendants.  An entry which has no
descendants has C<lft> = C<rgt>.  The C<depth> field stores the distance of a
given entry from the root of its taxon tree, with top-level nodes having
C<depth> = 1.  Note that all entries in the same concept group have the same
C<lft>, C<rgt>, and C<depth> values.

Using these fields, we can formulate simple and efficient SQL queries to fetch
all of the descendants of a given entry and other similar operations.  For
more information, see L<http://en.wikipedia.org/wiki/Nested_set_model>.

The one necessary operation that is not easy to carry out using this method is
to compute the list of all ancestors of a given taxon.  To do this, we use a
separate table, C<taxon_ancestors>.  This table has three fields,
C<parent_no>, C<depth>, and C<child_no>, and stores the transitive closure of
the hierarchy relation.  The C<depth> field allows us to order the ancestors
properly from senior to junior.

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

=item updateOpinions ( dbh, opinion_no ... )

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

=item rebuild ( dbh, debug )

Completely rebuild the taxa tree from the C<opinions> and C<authorities>
tables.  This is safe to run while the database is in active use.  If 'debug'
is specified, it must be a hash reference indicating which of the steps in the
rebuilding process to carry out.

=back

=cut

package TaxonTrees;

# Table names

our $TREE_TABLE = "taxon_trees";
our $ANCESTOR_TABLE = "taxon_ancestors";
our $OPINION_TABLE = "order_opinions";
our $SUPPRESS_TABLE = "suppress_opinions";

our $OPINION_OLD = "old_opinions";

# Modules needed

use Text::JaroWinkler qw( strcmp95 );
use RebuildTaxonTrees;

use strict;


# addTaxon ( dbh, taxon_no )
# 
# This routine should be called when a new taxon is added to the database; it
# creates a new row in taxon_trees to match.
# 
# The new taxon will not have any relationship with the rest of the taxon
# trees; therefore, it will therefore be necessary to call updateOpinion() and
# provide one or more opinion_no values to indicate its relationship with
# other taxa.

sub addTaxon {

    my ($dbh, $taxon_no) = @_;
    
    # Create a new row in $TAXON_TABLE.  The fields 'parent_no', 'opinion_no',
    # 'lft', 'rgt', and 'depth' will default to 0.
    
    my $result = $dbh->do("INSERT INTO $TAXON_TABLE (taxon_no, orig_no, spelling_no, synonym_no)
			   VALUES ($taxon_no, $taxon_no, $taxon_no, $taxon_no)");
    
    my $a = 1;		# we can stop here when debugging
}


our (%new_opinion, %old_opinion, %new_orig, %delete_orig, %group_change);

# updateOpinions ( dbh, opinion_no ... )
# 
# This routine should be called whenever opinions are created or edited.  One
# or more opinion_no values can be provided.  The taxon tree tables will be
# updated to match the new opinions.

sub updateOpinions {

    my ($dbh, @opinions) = @_;
    
    # First, create the new tables that will be needed for the update process.
    
    createUpdateTables($dbh);
    
    # Next, determine any changes to concept group membership.
    
    computeGroupChanges($dbh, @opinions);
    
    # Next, determine any changes to group leadership.
    
    computeSpellingChanges($dbh, @opinions);
    
    # Next, determine any changes to synonymy.
    
    computeSynonymyChanges($dbh, @opinions);
    
    # Next, determine any changes to hierarchy.
    
    adjustHierarchy($dbh, @opinions);
    adjustTreeSequence($dbh, @opinions);
    adjustAncestry($dbh, @opinions);
}


sub createUpdateTables {

    my ($dbh) = @_;

    # Create a new table, and fill it with the old opinion data.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $OPINION_OLD");
    $result = $dbh->do("CREATE TABLE $OPINION_OLD LIKE $OPINION_TABLE");
    
    $result = $dbh->do("INSERT INTO $OPINION_OLD
			SELECT * FROM $OPINION_TABLE
			WHERE opinion_no in $opfilter");
    
    # Then delete the old opinion data from $OPINION_TABLE and insert the new.
    # We have to explicitly delete because an opinion might have been deleted,
    # which means there would be no new row for that opinion_no.
    
    $result = $dbh->do("DELETE FROM $OPINION_TABLE WHERE opinion_no in $opfilter");
    
    $result = $dbh->do("REPLACE INTO $OPINION_TABLE
		SELECT o.child_no, o.opinion_no, 
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
			if(o.pubyr IS NOT NULL AND o.pubyr != '', o.pubyr, r.pubyr) as pubyr,
			o.status, o.spelling_reason, o.child_no, o.child_spelling_no,
			o.parent_no, o.parent_spelling_no
		FROM opinions o LEFT JOIN refs r USING (reference_no)
		WHERE opinion_no in $opfilter
		ORDER BY ri DESC, pubyr DESC, opinion_no DESC");
    
    # Now create a temporary taxon_tree table in which to make the changes.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $TREE_TEMP");
    $result = $dbh->do("CREATE TABLE $TREE_TEMP LIKE $TREE_TABLE");
    
    # And a temporary table to record best opinions
    
    $result = $dbh->do("DROP TABLE IF EXISTS $BEST_TEMP");
    $result = $dbh->do("CREATE TABLE $BEST_TEMP
			    (orig_no int unsigned,
			     parent_no int unsigned,
			     opinion_no int unsigned,
			     ri int not null,
			     pubyr varchar(4),
			     status enum('belongs to','subjective synonym of','objective synonym of','invalid subgroup of','misspelling of','replaced by','nomen dubium','nomen nudum','nomen oblitum','nomen vanum'),
			     PRIMARY KEY (orig_no))");
    
    my $a = 1;		# we can stop here when debugging
}


# computeGroupChanges ( dbh, opinion_no ... )
# 
# Determine which taxa will be added to (%new_orig) or removed from
# (%delete_orig) taxonomic concept groups.  The keys of each hash specify the
# taxa in question (taxon_no), and the values specify the groups they are
# being added to or removed from (orig_no).

sub computeGroupChanges {

    my ($dbh, @opinions) = @_;
    
    # In order to figure out whether we have any changes to concept group
    # membership, we must download the new and old versions of the indicated
    # opinions.
    
    my ($op, $child_orig, $child_no, $parent_orig, $parent_no, $status, $spelling);
    my ($taxon_no, $orig_no);
    
    # First fetch the updated opinions, and figure out the "original combination"
    # assignments they make.  Record an error if these are inconsistent.
    
    my $opfilter = '(' . join(',', @opinions) . ')';
    
    my $new_op_data = $dbh->prepare("
		SELECT opinion_no, child_no, child_spelling_no,
		       parent_no, parent_spelling_no, status, spelling_reason
		FROM $OPINION_TABLE WHERE opinion_no in $opfilter");
    
    $new_op_data->execute();
    
    while ( ($op, $child_orig, $child_no, $parent_orig,
	     $parent_no, $status, $spelling) = $new_op_data->fetchrow_array() )
    {
	if ( exists $new_orig{$child_no} && $new_orig{$child_no} != $child_orig )
	{
	    print STDERR "Update: ERROR in opinion $opinon_no: original of $child_no inconsistent\n";
	}
	elsif ( $child_no > 0 )
	{
	    $new_orig{$child_no} = $child_orig;
	}
	
	if ( exists $new_orig{$parent_no} && $new_orig{$parent_no} != $parent_orig )
	{
	    print STDERR "Update: ERROR in opinion $opinion_no: original of $parent_no inconsistent\n";
	}
	elsif ( $parent_no > 0 )
	{
	    $new_orig{$parent_no} = $parent_orig;
	}
    }
    
    # Now do the same with the corresponding old opinion records.  The only
    # difference is that if we find the same "original combination" assignment
    # in the new and old opinions, we delete it from %new_orig and don't add
    # it to %old_orig.  Since it hasn't changed, we don't need to do anything
    # with it.
    
    my $old_op_data = $dbh->prepare("
		SELECT opinion_no, child_no, child_spelling_no,
		       parent_no, parent_spelling_no, status, spelling_reason
		FROM $OPINION_OLD");
    
    $old_op_data->execute();
    
    while ( ($op, $child_orig, $child_no, $parent_orig,
	     $parent_no, $status, $spelling) = $new_op_data->fetchrow_array() )
    {
	if ( $new_orig{$child_no} == $child_orig )
	{
	    delete $new_orig{$child_no};
	}
	elsif ( exists $new_orig{$child_no} && $new_orig{$child_no} != $child_orig )
	{
	    print STDERR "Update: ERROR in opinion $opinon_no: original of $child_no inconsistent\n";
	}
	elsif ( $child_no > 0 )
	{
	    $delete_orig{$child_no} = $child_orig;
	}
	
	if ( $new_orig{$parent_no} == $parent_orig )
	{
	    delete $new_orig{$parent_no};
	}
	elsif ( exists $new_orig{$parent_no} && $new_orig{$parent_no} != $parent_orig )
	{
	    print STDERR "Update: ERROR in opinion $opinion_no: original of $parent_no inconsistent\n";
	}
	elsif ( $parent_no > 0 )
	{
	    $delete_orig{$parent_no} = $parent_orig;
	}
    }
    
    # Now we need to figure out which concept groups (if any) will change
    # their membership.  We do this by checking %new_orig and %delete_orig
    # against the existing opinions.  If the changed opinions say the same
    # thing as other unchanged opinions, then concept group membership will
    # not change and so we can delete these changes from %new_orig and
    # %delete_orig.
    
    my $filter2 = join '(' . join(',', keys %new_orig, keys %delete_orig) . ')';
    
    my $other_opdata = $dbh->prepare("
		SELECT child_spelling_no, child_no FROM $OPINION_TABLE
		WHERE child_spelling_no in $filter2 and opinion_no not in $opfilter
		UNION
		SELECT parent_spelling_no, parent_no FROM $OPINION_TABLE
		WHERE parent_spelling_no in $filter2 and opinion_no not in $opfilter");
    
    $other_opdata->execute();
    
    while ( ($taxon_no, $orig_no) = $other_opdata->fetchrow_array() )
    {
	delete $new_orig{$taxon_no} if $new_orig{$taxon_no} = $orig_no;
	delete $delete_orig{$taxon_no} if $delete_orig{$taxon_no} = $orig_no;
    }
    
    # For each taxon that is a key in %delete_orig, it will either be given a
    # new value by an entry in %new_orig, or else its "original combination"
    # should be the taxon itself.
    
    foreach my $t (keys %delete_orig)
    {
	$new_orig{$t} = $t unless exists $new_orig{$t};
    }
    
    # We go through the remaining entries; any taxon which is a value in
    # either hash represents a concept group which is either gaining or losing
    # a member.  After this step, we no longer have any need for %delete_orig.
    
    foreach my $t (values %new_orig, values %delete_orig)
    {
	$group_change{$t} = 1;
    }
    
    # We then copy all rows representing taxa in any of these groups from
    # $TREE_TABLE to $TREE_TEMP so that we can work on them safely.
    
    my $orig_filter = '(' . join(',', keys %group_change) . ')';
    
    $result = $dbh->do("INSERT INTO $TREE_TEMP
			SELECT * FROM $TREE_TABLE
			WHERE orig_no in $orig_filter");
    
    my $a = 1;		# we can stop here when debugging
}


# computeSpellingChanges ( dbh, opinion_no ... )
# 
# Determine which taxonomic concept groups will get new leaders.  This affects
# the value of spelling_no.

sub computeSpellingChanges {

    my ($dbh) = @_;
    
    # For each of the concept groups whose membership is changing, we must
    # recompute the group leader.  This means selecting all relevant opinions
    # sorted by reliability index and publication year, and taking the best
    # one for each concept group.
    
    my $orig_filter = '(' . join(',', keys %group_change) . ')';
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SPELLING_TEMP");
    $result = $dbh->do("CREATE TABLE $SPELLING_TEMP
			    (orig_no int unsigned,
			     spelling_no int unsigned,
			     is_misspelling boolean,
			     PRIMARY KEY (orig_no))");
    
    $result = $dbh->do("
		INSERT IGNORE INTO $SPELLING_TEMP
		SELECT child_no, child_spelling_no,
		       if(spelling_reason = 'misspelling', true, false)
		FROM $OPINION_TABLE
		WHERE orig_no in $orig_filter");
    
    # We need to make sure that we have an entry in $SPELLING_TEMP for concept
    # group that is affected by the update, so for all orig_no values in
    # $TREE_TEMP that were missed in $SPELLING_TEMP we add entries stating
    # that spelling_no should be orig_no.  This is so that if the last opinion
    # regarding a taxon is deleted, its spelling_no reverts to orig_no (which
    # at that point will equal taxon_no).
    
    $result = $dbh->do("
		INSERT IGNORE INTO $SPELLING_TEMP
		SELECT orig_no, orig_no, false
		FROM $TREE_TEMP");
    
    # The problematic cases are the ones where the best opinion is marked as a
    # misspelling.  For each of these cases, we will have to grab all opinions
    # for the concept group and find the closest spelling match.
    
    # First fetch the orig_no of each concept group for which this is the
    # case, along with the actual misspelled name.
    
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

    # If any misspellings were found, then fetch all of the candidate
    # spellings for each of these misspellings.  We select them in descending
    # order of publication year, so that if equal-weight misspellings are
    # found then we choose the most recent one.
    # 
    # For each possible spelling, we compute the candidate's Jaro-Winkler
    # coefficient against the known misspelling.  If this is better than the
    # best match previously found, it becomes the preferred spelling for this
    # taxon.  Note that the Jaro-Winkler coefficient runs from 0 (no similarity)
    # to 1 (perfect similarity).
    
    if ( %misspelling )
    {
	my $candidates = $dbh->prepare("
			SELECT DISTINCT o.orig_no, o.child_spelling_no, a.taxon_name
			FROM $OPINION_TABLE o JOIN $SPELLING_TEMP s USING (orig_no)
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
    }
    
    # Now we copy all of the newly computed spellings into $TREE_TEMP.
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN $SPELLING_TEMP s USING (orig_no)
			SET t.spelling_no = s.spelling_no");
    
    my $a = 1;		# we can stop on this line when debugging
}


sub computeSynonymyChanges {
    
    my ($dbh) = @_;
    
    $result = $dbh->do("INSERT IGNORE INTO $BEST_TEMP
			SELECT orig_no, parent_no, opinion_no, ri, pubyr, status
			FROM $OPINION_TABLE
			WHERE status != 'misspelling of' and child_no != parent_no
				and child_spelling_no != parent_no");
    
    # Now we download the subset of these "best opinions" which indicate synonymy.
    
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
    

    
}

1;
