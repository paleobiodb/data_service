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
database, we use the term I<spelling> to represent the idea "distinct
taxonomic name/rank combination".  So, for example, "Rhizopodea" as a class
and "Rhizopodea" as a phylum are considered to be distinct I<spellings> of the
same I<taxonomic concept>.  In this case, the taxon's rank was changed at some
point in the past.  It is also the case that "Cyrnaonyx" and "Cyraonyx" are
distinct spellings of the same taxonomic concept, but in this case one was
used at some point as a misspelling of the other.  Each spelling is a member
of exactly one taxonomic concept.

For each taxonomic concept in the database, we algorithmically select a
"classification opinion" from the entries in the C<opinions> table,
representing the most recent and reliable taxonomic opinion that specifies a
relationship between this taxon and the rest of the taxonomic hierarchy.
These classification opinions are then used to arrange the taxa into a
collection of trees.  Note that the taxa do not form a single tree, because
there are a number of fossil taxa for which classification opinions have not
yet been entered into the database.

=head2 Organization of taxa

The C<authorities> table contains one row for each distinct spelling with
C<taxon_no> as primary key.  The C<taxon_trees> table contains one row for
each taxonomic concept, with C<orig_no> as primary key.  The C<orig_no> field
also exists as a foreign key in C<authorities>, tying the two tables together.

The taxonomic spellings and concepts are organized according to four separate
relations, based on the data in C<authorities> and C<opinions>.  These
relations are discussed below; the name listed in parentheses after each one
is the name of the field which records the relation.

=over 4

=item Taxonomic concept (orig_no)

This relation groups together all of the spellings (name/rank combinations)
that represent the same taxonomic concept.  In other words, when a taxon's
rank is changed, or its spelling is changed, all of the resultant entries in
C<authorities> will have a different C<taxon_no> but the same C<orig_no>.  The
C<orig_no> for each concept is the C<taxon_no> of its original spelling, so
all original spellings have C<taxon_no = orig_no>.  This is the only one
of the organizing relations that is stored in the C<authorities> table.

Note that this relation can be taken as an equivalence relation, whereas two
spellings have the same C<orig_no> if and only if they represent the same
taxonomic concept.

=item Accepted spelling (spelling_no)

This relation selects from each taxonomic concept the currently accepted
spelling (in other words, the currently accepted name/rank combination).  The
value of C<spelling_no> for any concept is the C<taxon_no> corresponding to
this spelling.

=item Synonymy (synonym_no)

This relation groups together all of the taxonomic concepts which are
considered to be synonyms of each other.  Two taxa are considered to be
synonymous if one is a subjective or objective synonym of the other, or was
replaced by the other, or if one is an invalid subgroup or nomen dubium, nomen
vanum or nomen nudum inside the other.

The value of C<synonym_no> is the C<orig_no> of the most senior synonym for
the given concept group.  This means that all concepts which are synonyms of
each other will have the same C<synonym_no>, but different C<orig_no>, and the
senior synonym will have C<synonym_no = orig_no>.  This relation can also
be taken as an equivalence relation, whereas two concept groups have the same
C<synonym_no> if and only if they are synonyms of each other.

=item Hierarchy (parent_no)

This relation associates lower with higher taxa.  It forms a collection of
trees, because (as noted above) there are a number of fossil taxa for which no
classifying opinion has yet been entered.  Any taxonomic concept for which no
opinion has been entered will have C<parent_no = 0>.

All taxa which are synonyms of each other will have the same C<parent_no>
value, and the C<parent_no> (if not 0) will be the one associated with the
classification opinion on the most senior synonym.  Thus, this is really a
relation on synonym groups.  In computing the hierarchy, we consider all
opinions on a synonym group together.

This relation, like the previous ones, can be taken as
an equivalence relation, whereas two taxonomic concepts have the same
C<parent_no> if and only if they are siblings of each other.

=back

=head2 Opinions

In addition to the fields listed above, each entry in C<taxon_trees> also has
an C<opinion_no> field.  This field points to the classification opinion that
has been algorithmically selected from the available opinions for that taxon.

For a junior synonym, the value of opinion_no will be the opinion which
specifies its immediately senior synonym.  There may exist synonym chains in
the database, where A is a junior synonym of B which is a junior synonym of C.
In any case, C<synonym_no> should always point to the most senior synonym.

For all taxonomic concepts which are not junior synonyms, the value of
C<opinion_no> will be the opinion which specifies its immediately higher
taxon.  Note that this opinion will also specify a particular spelling of the
higher taxon, which may not be the currently accepted one.  In any case,
C<parent_no> will always point to the original spelling of the parent taxon.

=head2 Tree structure

In order to facilitate tree printouts and logical operations on the taxa
hierarchy, the entries in C<taxon_trees> are sequenced via preorder tree
traversal.  This is recorded in the fields C<lft> and C<rgt>.  The C<lft>
field stores the traversal sequence, and the C<rgt> field of a given entry
stores the maximum sequence number of the entry and all of its descendants.
An entry which has no descendants has C<lft> = C<rgt>.  The C<depth> field
stores the distance of a given entry from the root of its taxon tree, with
top-level nodes having C<depth> = 1.  All entries which have no parents or
children will have null values in C<lft>, C<rgt> and C<depth>.

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
described above.  This table, called C<suppress_opinions>, is needed because
the synonymy and hierarchy relations must be structured as collections of
trees.  Unfortunately, the set of opinions stored in the database may generate
cycles in one or both of these relations.  For example, there will be cases in
which the best opinion on taxon A states that it is a subjective synonym of B,
while the best opinion on taxon B states that it is a subjective synonym of A.
In order to resolve this, the algorithm that computes the synonymy and
hierarchy relations breaks each cycle by choosing the best (most recent and
reliable) opinion from those that define the cycle and suppressing any opinion
that contradicts the chosen one.  The C<suppress_opinions> table records which
opinions are so suppressed.

=head1 Interface

The interface to this module is as follows (note: I<this is a draft
specification>).  In the following documentation, the parameter C<dbh> is
always a database handle.  Note that routines whose names begin with "get"
return objects, while those whose names begin with "list" return lists of
taxon identifiers.

=over 4

=item addConcept ( dbh, orig_no )

Add a new taxonomic concept to the taxon trees.  This should be called
whenever a new concept is entered by a user.

=item updateTaxonTrees ( dbh, concept_list, opinion_list )

Adjust the entries to take into account changes to the listed opinions and
also changes to the membership of the listed concepts.  One or both of these
lists may be empty.  Some of the listed concepts and/or opinions may
disappear, as may happen if an opinion is deleted or if two concept groups are
merged.  This routine should be called whenever C<orig_no> values in the
C<authorities> table are adjusted, and both the old and the new orig_no values
should be passed as part of the 'concept_list' argument.  It should also be
called whenever opinions are edited, added or deleted.

=item buildTaxonTrees ( dbh )

Build the taxon tree tables from scratch using the data in C<authorities> and
C<opinions>.  Although this is probably not absolutely necessary if
updateTaxonTrees() has been called consistently whenever the C<authorities>
and C<opinions> tables are changed, the author of this module suggests that it
be run on a daily basis at a convenient time when traffic on the database is
light.

=item getCurrentSpelling ( dbh, taxon_no )

Given the identifier of a spelling or of a taxonomic concept,
returns an object representing the currently accepted spelling.

=item listCurrentSpelling ( dbh, taxon_no )

Given the identifier of a spelling or of a taxonomic concept, returns the
spelling number (taxon_no) of the currently accepted spelling.

=item getAllSpellings ( dbh, taxon_no )

Given the identifier of a spelling or of a taxonomic concept, returns a list
of objects representing all spellings of the indicated concept.  The first
object in the list will be the currently accepted spelling.

=item listAllSpellings ( dbh, taxon_no )

Given the identifier of a spelling or of a taxonomic concept, returns a list
of spelling numbers (taxon_no) representing all spellings of the indicated
concept.  The first item in the list will be the currently accepted spelling.

=item getOriginalCombination ( dbh, taxon_no )

Returns an object representing the original spelling of the specified
taxonomic concept, which may be the specified taxon itself.

=item listOriginalCombination ( dbh, taxon_no )

Returns the taxon_no for the original spelling of the given taxonomic concept,
which may be the specified taxon itself.

=item getMostSeniorSynonym ( dbh, taxon_no )

Returns an object representing the most senior synonym of the specified
taxonomic concept.  If the concept does not have a senior synonym, an object
representing the concept itself is returned.

=item getAllSynonyms ( dbh, taxon_no )

Returns a list of objects representing all synonyms of the specified taxonomic
concept, in order of seniority.  If the specified concept has no synonyms, a single
object is returned representing the concept itself.

=item listAllSynonyms ( dbh, taxon_no )

Returns a list of taxon identifiers, listing all synonyms (junior and senior)
of the given taxon as well as the taxon itself.  These are ordered senior to
junior.  If the concept has no synonyms, its own identifier is returned.

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

package TaxonTrees;

# Controls for debug messages

our $MSG_TAG = 'unknown';
our $MSG_LEVEL = 0;

# Main table names

our $TREE_TABLE = "taxon_trees";
our $ANCESTOR_TABLE = "taxon_ancestors";
our $SUPPRESS_TABLE = "suppress_opinions";
our $OPINION_CACHE = "order_opinions";

our $AUTHORITIES_TABLE = "authorities";
our $OPINIONS_TABLE = "opinions";
our $REFS_TABLE = "refs";

# Backup names for use with RENAME TABLES

our $TREE_BAK = "taxon_trees_bak";
our $ANCESTOR_BAK = "taxon_ancestors_bak";
our $SUPPRESS_BAK = "suppress_opinions_bak";
our $OPINION_BAK = "order_opinions_bak";

# Working table names

our $TREE_TEMP = "tn";
our $ANCESTOR_TEMP = "pn";
our $SUPPRESS_TEMP = "sn";
our $OPINION_TEMP = "opn";

# Auxiliary table names

our $SPELLING_TEMP = "spelling_aux";
our $SYNONYM_TEMP = "synonym_aux";
our $CLASS_TEMP = "best_opinions";
our $PARENT_TEMP = "parent_aux";


# Modules needed

use Text::JaroWinkler qw( strcmp95 );

use strict;


# addConcept ( dbh, orig_no )
# 
# Add a new concept to the database.  This should be called whenever a new
# concept is added to the authorities table.  The new concept will have by
# default no relationship to any other concepts.  Therefore, once opinions
# have been entered pertaining to it, updateOpinions() or updateConcepts()
# will have to be called in order to link it to the rest of the taxon trees.

sub addConcept {

    my ($dbh, $orig_no) = @_;
    
    # Add a new row to $TREE_TABLE appropriate for a taxon with no
    # relationship to any other taxa.
    
    my ($new_seq) = $dbh->selectrow_array("SELECT max(lft)+1 FROM $TREE_TABLE");
    
    my $result = $dbh->do("INSERT INTO $TREE_TABLE 
			   VALUES ($orig_no, $orig_no, $orig_no, $orig_no, 0, 
				   $new_seq, $new_seq, 1)");

    my $a = 1;		# we can stop here when debugging
}


# update ( dbh, concept_list, opinion_list, msg_level )
# 
# This routine should be called whenever opinions are created, edited, or
# deleted, or when concept membership in the authorities table is changed.
# The argument 'concept_list' should be a list of concept identifiers that
# have changed (either gaining or losing members) and the 'opinion_list'
# argument should be a list of opinions that have been added, edited or
# deleted.  Either or both may be empty or undefined.
# 
# The taxon tree tables will be updated to match the new state of authorities
# and opinions.  This involves first computing the list of taxonomic concepts
# that could possibly be affected by the change, and then recomputing the
# three organizing relations of "group leader", "synonymy" and "hierarchy" for
# those concepts.  Finally, the taxonomic tree is renumbered and the ancestor
# relationship adjusted to match.
# 
# The argument 'msg_level', if given, specifies how verbose this routine will
# be in writing messages to the log.  It defaults to 1, which means a minimal
# message set.

sub update {

    my ($dbh, $concept_list, $opinion_list, $msg_level) = @_;
    
    my %update_concepts;	# list of concepts to be updated
    
    # First, set the variables that control log output.
    
    $MSG_TAG = 'Update'; $MSG_LEVEL = $msg_level || 1;
    
    # If we have been notified that concept membership has changed, then all
    # of these changed concepts must be updated in the taxon tree tables.
    # Before we proceed, we must update the opinions and opinion cache tables
    # to reflect the new concept membership.
    
    if ( ref $concept_list eq 'ARRAY' and @$concept_list > 0 )
    {
	# First clean the list to make sure every entry is unique.
	
	foreach my $t (@$concept_list)
	{
	    $update_concepts{$t} = 1;
	}
	
	my @concept_list = sort { $a <=> $b } keys %update_concepts;
	
	logMessage(1, "notified concepts: " .
	    join(', ', @concept_list) . "\n");
	
	# Update the opinions and opinion cache tables to reflect the new
	# concept membership.
	
	updateOpinionConcepts($dbh, \@concept_list);
    }
    
    # If we have been notified that opinions have changed, then add to the
    # update list all concepts that are referenced by either the previous or
    # the current version of each opinion.  Then update the opinion cache with
    # the new data.
    
    if ( ref $opinion_list eq 'ARRAY' and @$opinion_list > 0 )
    {
	# Add to the list all concepts referenced by either the old or the new
	# version of the changed opinions.  It doesn't matter in which order
	# we call getOpinionConcepts and updateOpinionConcepts because all of
	# the original concept values changed by the latter will already be
	# listed in %update_concepts.
	
	foreach my $t ( getOpinionConcepts($dbh, $opinion_list) )
	{
	    $update_concepts{$t} = 1;
	}
	
	logMessage(1, "notified opinions: " . 
	    join(', ', @$opinion_list) . "\n");
	
	updateOpinionCache($dbh, $opinion_list);
    }
    
    # Proceed only if we have one or more concepts to update.
    
    unless ( %update_concepts )
    {
	return;
    }
    
    else
    {
	logMessage(1, "updating the following concepts: " .
	    join(', ', keys %update_concepts) . "\n");
    }
    
    # The rest of this routine updates the subset of $TREE_TABLE and
    # $ANCESTOR_TABLE that comprises the concept groups listed in
    # %update_concepts along with their junior synonyms and children.
    
    # First create a temporary table to hold the new rows that will eventually
    # go into $TREE_TABLE.  To start with, we will need one row for each
    # concept in %update_concepts.  Create some auxiliary tables as well.
    
    createTempTables($dbh, \%update_concepts);
    
    # Next, compute group leadership for every concept in $TREE_TEMP.
    
    computeSpelling($dbh);
    
    # Next, expand $TREE_TEMP to include junior synonyms of the concepts
    # represented in it.  We need to do this before we update the synonymy
    # relation, because a change in a concept's synonymy will not only change
    # its own synonym_no but that of its junior synonyms as well.
    
    expandToJuniors($dbh);
    
    # Then compute the synonymy relation for every concept in $TREE_TEMP.
    
    computeSynonymy($dbh);
    
    # Next, expand $TREE_TEMP to include senior synonyms of the concepts
    # represented in it.  We need to do this before we update the hierarchy
    # relation, because the hierarchy relation is computed on synonym groups.
    
    expandToSeniors($dbh);
    
    # At this point, we need to de-chain the synonyms, so that synonym_no
    # points to the most senior synonym for each concept.  This is necessary
    # so that computeHierarchy can properly gather the synonym groups.
    
    deChainSynonyms($dbh);
    
    # Next, expand $TREE_TEMP to include immediate children of the concepts
    # represented in it (including junior synonyms).  We need to do this
    # before we update the hierarchy relation, so that we can detect any
    # cycles that might occur.  This is unlikely, but it's best to check.
    
    # expandToChildren($dbh);
    
    # Then compute the hierarchy relation for every concept in $TREE_TEMP.
    
    computeHierarchy($dbh);
    
    # At this point, we need to de-chain all synonym_no and parent_no values
    # that point out of $TREE_TEMP into $TREE_TABLE.  They might point to
    # junior synonyms in $TREE_TABLE, and if so they must be updated to point
    # to the proper senior synonyms as indicated by the corresponding
    # synonym_no values in $TREE_TABLE.
    
    deChainReferences($dbh);
    
    # If the adjustments to the hierarchy are small, then we can now update
    # $TREE_TABLE using the rows from $TREE_TEMP and then adjust the tree
    # sequence in-place.  This will finish the procedure.
    
    if ( treePerturbation($dbh) < 1 )
    {
	updateTreeTables($dbh);
    }
    
    # Otherwise, we need to completely rebuild the tree sequence and then
    # activate the temporary tables using atomic rename.
    
    else
    {
	# Copy all rows from $TREE_TABLE into $TREE_TEMP that aren't already
	# represented there.
	
	my $result = $dbh->do("
		INSERT IGNORE INTO $TREE_TEMP
		SELECT * FROM $TREE_TABLE");
	
	# Do the same thing for $SUPPRESS_TEMP, but ignore anything that is a
	# classification opinion in $TREE_TEMP (such an opinion was previously
	# suppressed, but now should not be).
	
	my $result = $dbh->do("
		INSERT INTO $SUPPRESS_TEMP
		SELECT * FROM $SUPPRESS_TABLE s
			LEFT JOIN $TREE_TEMP t ON s.opinion_no = t.opinion_no
		WHERE orig_no is null");
	
	# Now that $TREE_TEMP has the entire tree, resequence it.
	
	computeTreeSequence($dbh);
	
	# recompute the ancestry relation
	
	#computeAncestry($dbh);
	
	# Then activate the temporary tables by renaming them over the previous
	# ones. 
	
	activateNewTables($dbh);
    }
    
    my $a = 1;		# we can stop here when debugging
}


# build ( dbh, msg_level )
# 
# Build the taxon tree tables from scratch, using only the information in
# $AUTHORITIES_TABLE and $OPINIONS_TABLE.  If the 'msg_level' parameter is
# given, it controls the verbosity of log messages produced.
# 
# The $step_control parameter is for debugging only.  If specified it must be
# a hash reference that will control which steps are taken.

sub build {

    my ($dbh, $msg_level, $step_control) = @_;
    
    # First, set the variables that control log output.
    
    $MSG_TAG = 'Rebuild'; $MSG_LEVEL = $msg_level || 1;
    
    unless ( ref $step_control eq 'HASH' and %$step_control ) {
	$step_control = { 'a' => 1, 'b' => 1, 'c' => 1,
			  'd' => 1, 'e' => 1, 'f' => 1 };
    };
    
    # Then create the necessary tables.
    
    rebuildOpinionCache($dbh) if $step_control->{a};
    createTempTables($dbh) if $step_control->{a};
    
    # Next, determine the currently accepted spelling for each concept from
    # the data in $OPINION_CACHE.
    
	clearSpelling($dbh) if $step_control->{x} and $step_control->{b};
    
    computeSpelling($dbh) if $step_control->{b};
    
    # Next, compute the synonymy relation from the data in $OPINION_CACHE.
    
	clearSynonymy($dbh) if $step_control->{x} and $step_control->{c};
    
    computeSynonymy($dbh) if $step_control->{c};
    
    # De-chain the synonymy relation.
    
    deChainSynonyms($dbh) if $step_control->{c};
    
    # Next, compute the hierarchy relation from the data in $OPINION_CACHE.
    
	clearHierarchy($dbh) if $step_control->{x} and $step_control->{d};
    
    computeHierarchy($dbh) if $step_control->{d};
    
    # Next, sequence the taxon trees using the hierarchy relation.
    
	clearTreeSequence($dbh) if $step_control->{x} and $step_control->{e};
    
    computeTreeSequence($dbh) if $step_control->{e};
    
    # Next, compute the ancestry relation using the sequenced trees.
    
    # computeAncestry($dbh) if $step_control->{f};
    
    # Finally, activate the new tables we have just computed by renaming them
    # over the previous ones.
    
    activateNewTables($dbh, $step_control->{k}) if $step_control->{g};
    
    my $a = 1;		# we can stop here when debugging
}


# updateOpinionCache ( dbh, opinion_list )
# 
# Copy the indicated opinion data from the opinions table to the opinion
# cache.

sub updateOpinionCache {
    
    my ($dbh, $opinion_list) = @_;
    
    my $result;
    
    # First delete the old opinion data from $OPINION_CACHE and insert the new.
    # We have to explicitly delete because an opinion might have been deleted,
    # which means there would be no new row for that opinion_no.
    
    # Note that $OPINION_TABLE will not be correctly ordered after this, so we
    # cannot rely on its order during the rest of the update procedure.
    
    my $opfilter = '(' . join(',', @$opinion_list) . ')';
    
    $result = $dbh->do("LOCK TABLE $OPINIONS_TABLE as o read,
				   $REFS_TABLE as r read,
				   $AUTHORITIES_TABLE as a1 read,
				   $AUTHORITIES_TABLE as a2 read,
				   $OPINION_CACHE write");
    
    $result = $dbh->do("DELETE FROM $OPINION_CACHE WHERE opinion_no in $opfilter");
    
    populateOpinionCache($dbh, $OPINION_CACHE, $opfilter);
    
    $result = $dbh->do("UNLOCK TABLES");
    
    my $a = 1;		# we can stop here when debugging
}


# rebuildOpinionCache ( dbh )
# 
# Rebuild the opinion cache completely from the opinions table.

sub rebuildOpinionCache {
    
    my ($dbh) = @_;
    
    my ($result);
    
    logMessage(2, "rebuilding opinion cache (a)");
    $DB::single = 1;
    
    # In order to minimize interference with any other threads which might
    # need to access the opinion cache, we create a new table and then rename
    # it into the old one.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $OPINION_TEMP");
    $result = $dbh->do("CREATE TABLE $OPINION_TEMP
			  (opinion_no int unsigned not null,
			   orig_no int unsigned not null,
			   child_spelling_no int unsigned not null,
			   parent_no int unsigned not null,
			   parent_spelling_no int unsigned not null,
			   ri int not null,
			   pubyr varchar(4),
			   status enum('belongs to','subjective synonym of','objective synonym of','invalid subgroup of','misspelling of','replaced by','nomen dubium','nomen nudum','nomen oblitum','nomen vanum'),
			   spelling_reason enum('original spelling','recombination','reassignment','correction','rank change','misspelling'),
			   UNIQUE KEY (opinion_no))");
    
    # Populate this table with data from $OPINIONS_TABLE.  We will sort this
    # data properly for determining the best (most recent and reliable)
    # opinions, since this will speed up subsequent steps of the taxon trees
    # rebuild/update process.
    
    populateOpinionCache($dbh, $OPINION_TEMP);
    
    # Then index the newly populated opinion cache.
    
    $result = $dbh->do("ALTER TABLE $OPINION_TEMP ADD KEY (orig_no)");
    $result = $dbh->do("ALTER TABLE $OPINION_TEMP ADD KEY (parent_no)");
    
    # Now, we remove any backup table that might have been left in place, and
    # swap in the new table using an atomic rename operation
    
    $result = $dbh->do("DROP TABLE IF EXISTS $OPINION_BAK");
    
    $result = $dbh->do("RENAME TABLE
				$OPINION_CACHE to $OPINION_BAK,
				$OPINION_TEMP to $OPINION_CACHE");
    
    # ...and remove the backup
    
    $result = $dbh->do("DROP TABLE $OPINION_BAK");
    
    my $a = 1;		# we can stop here when debugging
}


# populateOpinionCache ( dbh, table_name, filter_expr, sort )
# 
# Insert records into the opinion cache table, under the given table name.  If
# $filter_expr is given, then it should be a string representing a list of
# opinion numbers to insert.  The results are sorted in the order necessary
# for selecting the most reliable and recent opinions.

sub populateOpinionCache {

    my ($dbh, $table_name, $filter_expr) = @_;
    
    my ($result);
    
    # If we were given a filter expression, create the necessary clause.
    
    my ($filter_clause) = "";
    
    if ( $filter_expr )
    {
	$filter_clause = "WHERE opinion_no in $filter_expr";
    }
    
    # This query is adapated from the old getMostRecentClassification()
    # routine, from TaxonInfo.pm line 2003.  We have to join with authorities
    # twice to look up the original combination (concept group id) of both
    # child and parent.  We can't trust the child_no and parent_no fields of
    # opinions.
    
    $result = $dbh->do("INSERT INTO $table_name
		SELECT o.opinion_no, a1.orig_no, o.child_spelling_no, a2.orig_no,
			o.parent_spelling_no, 
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
			o.status, o.spelling_reason
		FROM opinions o LEFT JOIN refs r USING (reference_no)
			JOIN authorities a1 ON a1.taxon_no = o.child_spelling_no
			JOIN authorities a2 ON a2.taxon_no = o.parent_spelling_no
		ORDER BY ri DESC, pubyr DESC, opinion_no DESC");
    
    my $a = 1;		# we can stop here when debugging
}


# getOpinionConcepts ( dbh, opinion_list )
# 
# Given a list of changed opinions, return the union of the set of concepts
# referred to by both the new versions (from the opinions table) and the old
# versions (still stored in the opinion cache, since we have not yet updated
# it).

sub getOpinionConcepts {

    my ($dbh, $opinion_list) = @_;
    
    my (%update_concepts);
    
    # First fetch the updated opinions, and figure out the "original
    # combination" mentioned in each one.
    
    my $opfilter = '(' . join(',', @$opinion_list) . ')';
    
    my $new_op_data = $dbh->prepare("
		SELECT child_no, parent_no
		FROM $OPINIONS_TABLE WHERE opinion_no in $opfilter");
    
    $new_op_data->execute();
    
    while ( my ($child_orig, $parent_orig) = $new_op_data->fetchrow_array() )
    {
	$update_concepts{$child_orig} = 1 if $child_orig > 0;
	$update_concepts{$parent_orig} = 1 if $parent_orig > 0;
    }
    
    # Now do the same with the corresponding old opinion records.
    
    my $old_op_data = $dbh->prepare("
		SELECT orig_no, parent_no
		FROM $OPINION_CACHE WHERE opinion_no in $opfilter");
    
    $old_op_data->execute();
    
    while ( my ($child_orig, $parent_orig) = $old_op_data->fetchrow_array() )
    {
	$update_concepts{$child_orig} = 1 if $child_orig > 0;
	$update_concepts{$parent_orig} = 1 if $parent_orig > 0;
    }
    
    return keys %update_concepts;
}


# updateOpinionConcepts ( dbh, concept_list )
# 
# This routine updates all of the orig_no, child_no and parent_no values in
# $OPINIONS_TABLE and $OPINION_CACHE that fall within the given list.

sub updateOpinionConcepts {

    my ($dbh, $concept_list) = @_;
    
    my $concept_filter = '(' . join(',', @$concept_list) . ')';
    
    my $result;
    
    # First, $OPINION_CACHE
    
    $result = $dbh->do("UPDATE $OPINION_CACHE o JOIN authorities a
				ON a.taxon_no = o.child_spelling_no
			WHERE a.orig_no in $concept_filter
			SET o.orig_no = a.orig_no");
    
    $result = $dbh->do("UPDATE $OPINION_CACHE o JOIN authorities a
				ON a.taxon_no = o.parent_spelling_no
			WHERE a.orig_no in $concept_filter
			SET o.parent_no = a.orig_no");
    
    # Next, $OPINIONS_TABLE
    
    $result = $dbh->do("UPDATE $OPINIONS_TABLE o o JOIN authorities a
				ON a.taxon_no = o.child_spelling_no
			WHERE a.orig_no in $concept_filter
			SET o.child_no = a.orig_no");
    
    $result = $dbh->do("UPDATE $OPINIONS_TABLE o JOIN authorities a
				ON a.taxon_no = o.parent_spelling_no
			WHERE a.orig_no in $concept_filter
			SET o.parent_no = a.orig_no");
    
    my $a = 1;		# we can stop here when debugging
}


# createTempTables ( dbh, concept_hash )
# 
# Create a new $TREE_TEMP table to hold the new rows that are being computed
# for $TREE_TABLE.  If $concept_hash is specified, it must be a hash whose
# keys represent a list of taxonomic concepts.  Otherwise, $TREE_TEMP will be
# created with one row for every concept in the database.
# 
# Also create some other necessary tables.

sub createTempTables {

    my ($dbh, $concept_hash) = @_;
    
    my ($result);
    
    logMessage(2, "creating temporary tables (a)");
    $DB::single = 1;
    
    # First create $TREE_TEMP, which will hold one row for every concept which
    # is being updated.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $TREE_TEMP");
    $result = $dbh->do("CREATE TABLE $TREE_TEMP 
			       (orig_no int unsigned not null,
				spelling_no int unsigned not null,
				synonym_no int unsigned not null,
				parent_no int unsigned not null,
				opinion_no int unsigned not null,
				lft int unsigned,
				rgt int unsigned,
				depth int unsigned,
				PRIMARY KEY (orig_no))");
    
    # If we were given a list of concepts, populate it with just those.
    # Otherwise, grab every concept in $AUTHORITIES_TABLE
    
    my $concept_filter = '';
    
    if ( ref $concept_hash eq 'HASH' )
    {
	$concept_filter = 'WHERE orig_no in (' . 
	    join(',', keys %$concept_hash) . ')';
    }
	
    $result = $dbh->do("INSERT INTO $TREE_TEMP
			SELECT distinct orig_no, 0, 0, 0, 0, null, null, null
			FROM $AUTHORITIES_TABLE $concept_filter");
    
    # Next, create a temporary table to record the best opinion for each concept.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $CLASS_TEMP");
    $result = $dbh->do("CREATE TABLE $CLASS_TEMP
			   (orig_no int unsigned not null,
			    opinion_no int unsigned not null,
			    parent_no int unsigned not null,
			    ri int unsigned not null,
			    pubyr varchar(4),
			    status enum('belongs to','subjective synonym of','objective synonym of','invalid subgroup of','misspelling of','replaced by','nomen dubium','nomen nudum','nomen oblitum','nomen vanum'),
			    UNIQUE KEY (orig_no))");
    
    # Also, create a temporary table to record suppressed opinions due to
    # cycle-breaking.  This information will eventually be moved into
    # $SUPPRESS_TABLE.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SUPPRESS_TEMP");
    $result = $dbh->do("CREATE TABLE $SUPPRESS_TEMP
			       (opinion_no int unsigned not null,
				suppress int unsigned not null,
				PRIMARY KEY (opinion_no))");
    
    # And finally, a temporary table to record the ancestry relation.  This
    # information will eventually be moved into $ANCESTOR_TABLE.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $ANCESTOR_TEMP");
    $result = $dbh->do("CREATE TABLE $ANCESTOR_TEMP
			       (child_no int unsigned not null,
				parent_no int unsigned not null,
				depth int unsigned not null,
				KEY (child_no))");
    
    my $a = 1;		# we can stop here when debugging
}


# computeSpelling ( dbh )
# 
# Fill in the spelling_no field of $TREE_TEMP.  This computes the "concept
# group leader" relation for all concepts represented in $TREE_TEMP.  We do
# this by selecting the best "spelling opinion" (most recent and reliable
# opinion, including those labelled "misspelling") for each concept group.
# Unless the best opinion is recorded as a misspelling, we take its spelling
# to be the accepted one.  Otherwise, we look for the best spelling match
# among the available opinions using the Jaro-Winkler metric.

sub computeSpelling {

    my ($dbh) = @_;
    
    my ($result);
    
    logMessage(2, "computing currently accepted spelling relation (b)");
    $DB::single = 1;
    
    # In order to select the group leader for each concept, we start by
    # choosing the spelling associated with the most recent and reliable
    # opinion for each concept group.  This "spelling opinion" may differ from
    # the "classification opinion" computed below, because we select from all
    # opinions including those marked as "misspelling" and those with
    # parent_no = child_no.
    
    # We choose a single opinion for each group by defining a temporary table
    # with a unique key on orig_no and using INSERT IGNORE with a properly
    # ordered selection.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SPELLING_TEMP");
    $result = $dbh->do("CREATE TABLE $SPELLING_TEMP
			    (orig_no int unsigned,
			     spelling_no int unsigned,
			     is_misspelling boolean,
			     PRIMARY KEY (orig_no))");
    
    $result = $dbh->do("
		INSERT IGNORE INTO $SPELLING_TEMP
		SELECT o.orig_no, o.child_spelling_no,
		       if(o.spelling_reason = 'misspelling', true, false)
		FROM $OPINION_CACHE o JOIN $TREE_TEMP USING (orig_no)
		ORDER BY o.ri DESC, o.pubyr DESC, o.opinion_no DESC");
    
    # The problematic cases are the ones where the best opinion is marked as a
    # misspelling.  For each of these cases, we will have to grab all opinions
    # for the concept group and find the closest spelling match.
    
    # First fetch the orig_no of each concept group for which this is the
    # case, along with the actual misspelled name.
    
    logMessage(2, "    looking for misspellings");
    
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
    
    my $count = scalar(keys %misspelling);
    
    logMessage(1, "    found $count misspelling(s)");
    
    # If we found any misspellings, fetch all of the candidates for the proper
    # spelling of each.  We select these in descending order of publication
    # year, so that if equal-weight misspellings are found then we choose the
    # most recent one.
    # 
    # For each possible spelling, we compute the candidate's Jaro-Winkler
    # coefficient against the known misspelling.  If this is better than the
    # best match previously found, it becomes the preferred spelling for this
    # taxon.  Note that the Jaro-Winkler coefficient runs from 0 (no similarity)
    # to 1 (perfect similarity).
    
    if ( $count )
    {
	logMessage(2, "    looking for good spellings");
	
	my $candidates = $dbh->prepare("
			SELECT DISTINCT o.orig_no, o.child_spelling_no, a.taxon_name
			FROM $OPINION_CACHE o JOIN $SPELLING_TEMP s USING (orig_no)
				JOIN authorities a ON a.taxon_no = o.child_spelling_no
			WHERE s.is_misspelling and o.spelling_reason != 'misspelling'
			ORDER BY o.pubyr DESC");
	
	$candidates->execute();
	
	while ( ($orig_no, $taxon_no, $taxon_name) = $candidates->fetchrow_array() )
	{
	    # We skip all spellings which are the same as the recent
	    # misspelling, and also anything we've seen before.
	    
	    next if $taxon_name eq $misspelling{$orig_no};
	    next if $seen{$orig_no . $taxon_name};
	    
	    # The strcmp95 routine requires a length parameter, so we give it
	    # the maximum of the lengths of the two arguments.
	    
	    my ($a_len) = length($taxon_name);
	    my ($b_len) = length($misspelling{$orig_no});
	    my ($coeff) = strcmp95( $taxon_name, $misspelling{$orig_no},
				    ($a_len > $b_len ? $a_len : $b_len) );
	
	    if ( !defined $best_coeff{$orig_no} or $coeff > $best_coeff{$orig_no} )
	    {
		$best_coeff{$orig_no} = $coeff;
		$best_match{$orig_no} = $taxon_no;
	    }
	    
	    $seen{$orig_no . $taxon_name} = 1;
	}
	
	# Now we fix all of the misspellings that we identified above.
	
	my $fix_spelling = $dbh->prepare("
		UPDATE $SPELLING_TEMP SET spelling_no = ?
		WHERE orig_no = ?");
	
	foreach $orig_no (keys %misspelling)
	{
	    my $spelling_no = $best_match{$orig_no} || $orig_no;
	    
	    $fix_spelling->execute($spelling_no, $orig_no);
	}
	
    }
    
    # Next, we copy all of the computed spelling_no values into $TREE_TEMP.
    
    logMessage(2, "    setting spelling_no");
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN $SPELLING_TEMP s USING (orig_no)
			SET t.spelling_no = s.spelling_no");
    
    # In every row of $TREE_TEMP that still has a spelling_no of 0, we set
    # spelling_no = orig_no.  In other words, for each concept group with no
    # associated opinions, the group leader is the original combination by
    # default.
    
    $result = $dbh->do("UPDATE $TREE_TEMP SET spelling_no = orig_no WHERE spelling_no = 0");
    
    # Now that we have set spelling_no, we can efficiently index it.
    
    logMessage(2, "    indexing spelling_no");
    
    $result = $dbh->do("ALTER TABLE $TREE_TEMP ADD INDEX (spelling_no)");
    
    my $a = 1;		# we can stop on this line when debugging
}


# expandToJuniors ( dbh )
# 
# Expand $TREE_TEMP to adding to it all rows from $TREE_TABLE which represent
# junior synonyms of the concepts already in $TREE_TEMP.  Note that we can't
# just use the synonym_no field of $TREE_TABLE, because it refers to the most
# senior synonym instead of the immediate senior synonym.  We might have a
# synonym chain A -> B -> C with B in $TREE_TEMP.  In such a case, we would
# miss A if we relied on synonym_no.

sub expandToJuniors {
    
    my ($dbh) = @_;
    
    # We need to repeat the following process until no new rows are added, so
    # that junior synonyms of junior synonyms, etc. also get added.
    
    while (1)
    {
	my $result = $dbh->do("
		INSERT IGNORE INTO $TREE_TEMP
		SELECT m.*
		FROM $TREE_TEMP t
			JOIN $OPINION_CACHE o ON o.parent_no = t.orig_no
				and o.status != 'belongs to'
			JOIN $TREE_TABLE m ON m.opinion_no = o.opinion_no");
	
	last if $result == 0;
    }
    
    my $a = 1;		# we can stop on this line when debugging
}


# expandToSeniors ( dbh )
# 
# Expand $TREE_TEMP by adding to it all rows from $TREE_TABLE which represent
# senior synonyms of the concepts already in $TREE_TEMP.

sub expandToSeniors {
    
    my ($dbh) = @_;
    
    # We need to repeat the following process until no new rows are added, so
    # that senior synonyms of senior synonyms, etc. also get added
    
    while (1)
    {
	my $result = $dbh->do("
		INSERT IGNORE INTO $TREE_TEMP
		SELECT m.* FROM $TREE_TEMP t
			JOIN $OPINION_CACHE o USING (opinion_no)
			JOIN $TREE_TABLE m ON o.parent_no = m.orig_no
				and o.status != 'belongs to'");
	
	last if $result == 0;
    }
    
    my $a = 1;		# we can stop on this line when debugging
}


# expandToChildren ( dbh )
# 
# Expand $TREE_TEMP to adding to it all rows from $TREE_TABLE which represent
# children of the concepts already in $TREE_TEMP.

sub expandToChildren {
    
    my ($dbh) = @_;
    
    my $result = $dbh->do("
		INSERT IGNORE INTO $TREE_TEMP
		SELECT m.* FROM $TREE_TABLE m JOIN $TREE_TEMP t
			ON m.parent_no = t.orig_no");
    
    logMessage(1, "adding children: $result concepts");
}


# computeSynonymy ( dbh )
# 
# Fill in the synonym_no field of $TREE_TEMP.  This computes the "synonymy"
# relation for all of the concepts represented in $TREE_TEMP.  We do this by
# choosing the "classification opinion" for each concept (the most reliable
# and recent opinion that specifies a different parent_no) and then looking at
# those concepts for which the opinion specifies synonymy.
# 
# Unfortunately, the database can contain synonymy cycles.  A typical case
# would be where the best opinion for A states that it is a junior synonym of
# B, and the best opinion for B states that it is a junior synonym of A.  We
# look for such cycles and break them by the following procedure:
# 
# a) Figure out the most reliable and recent opinion from among the best
#    opinions for all concepts in the cycle.  Whichever concept this opinion
#    specifies as a senior synonym is presumed to be so.  Call it "A".
# 
# b) Suppress A's classification opinion (which necessarily asserts that A is
#    a junior synonym, for otherwise there wouldn't have been a cycle) and
#    choose the next-best opinion for A.
# 
# c) Record the suppression in $SUPPRESS_TEMP
# 
# d) Repeat this procedure until no cycles are found.  This is necessary
#    because in rare cases the new opinion selected in step (b) might cause a
#    new cycle.
# 
# Once this has been done, we can set the synonym_no field based on the
# parent_no values in the classification opinions.

sub computeSynonymy {
    
    my ($dbh) = @_;
    
    my ($result, @check_taxa, %taxa_moved, $filter);
    
    logMessage(2, "computing synonymy relation (c)");
    $DB::single = 1;
    
    # We start by choosing the "classification opinion" for each concept in
    # $TREE_TEMP.  We use the same mechanism as we did previously with the
    # spelling opinions: use a table with a unique key on orig_no, and INSERT
    # IGNORE with a properly ordered selection.  We use slightly different
    # criteria to select these than we did for the "spelling opinions", which
    # is why we need a separate table.
    
    # We ignore any opinions where orig_no and parent_no are identical,
    # because those are irrelevant to the synonymy relation (they might simply
    # indicate variant spellings, for example).
    
    $result = $dbh->do("INSERT IGNORE INTO $CLASS_TEMP
			SELECT o.orig_no, o.opinion_no, o.parent_no,
			    o.ri, o.pubyr, o.status
			FROM $OPINION_CACHE o JOIN $TREE_TEMP USING (orig_no)
			WHERE o.orig_no != o.parent_no
			ORDER BY o.ri DESC, o.pubyr DESC, o.opinion_no DESC");
    
    # Now we download just those classification opinions which indicate
    # synonymy.
    
    logMessage(2, "    downloading synonymy opinions");
    
    my $synonym_opinions = $dbh->prepare("
		SELECT orig_no, parent_no, opinion_no, ri, pubyr
		FROM $CLASS_TEMP
		WHERE status != 'belongs to' AND parent_no != 0");
    
    $synonym_opinions->execute();
    
    # The %juniors array will list all of the taxa that are asserted to be
    # junior synonyms of a given taxon.  The %opinions array is indexed by the
    # junior taxon number, and holds the information necessary to determine
    # the most recent and reliable opinion from any given set.
    
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
    
    logMessage(2, "    checking for cycles");
    
    my @breaks = breakCycles($dbh, \%juniors, \%opinions);
    
    logMessage(1, "    found " . scalar(@breaks) . " cycle(s)") if @breaks;
    
    # As long as there are cycles to be broken, we suppress the indicated
    # opinions and do a re-check, just in case the next-best opinions cause a
    # new cycle.
    
    while ( @breaks )
    {
	# Go through the cycle-breaking list, and insert the indicated
	# opinions into the $SUPPRESS_TEMP table.  We also keep track of the
	# associated taxa in @check_taxa.  We will use that list to fetch the
	# changed opinions, and also we need only check these taxa in our
	# followup cycle check because any new cycle must involve one of them.
	
	@check_taxa = ();
	
	my $insert_suppress = $dbh->prepare("INSERT IGNORE INTO $SUPPRESS_TEMP
					     VALUES (?, 1)");
	
	foreach my $pair (@breaks)
	{
	    my ($check_taxon, $suppress_opinion) = @$pair;
	    
	    $result = $insert_suppress->execute($suppress_opinion);
	    
	    push @check_taxa, $check_taxon;
	    $taxa_moved{$check_taxon} = 0;
	}
	
	# Next, we update the $CLASS_TEMP table by deleting the suppressed
	# opinions, then replacing them with the next-best opinion.  We need
	# to delete first, because there may be no next-best opinion!  (Also
	# because that way we can use INSERT IGNORE again to pick the single
	# best opinion for each orig_no as above).
	
	$filter = '(' . join(',', @check_taxa) . ')';
	
	$result = $dbh->do("DELETE FROM $CLASS_TEMP WHERE orig_no in $filter");
	
	$result = $dbh->do("
		INSERT IGNORE INTO $CLASS_TEMP
		SELECT o.orig_no, o.opinion_no, o.parent_no,
		    o.ri, o.pubyr, o.status
		FROM $OPINION_CACHE o LEFT JOIN $SUPPRESS_TEMP USING (opinion_no)
		WHERE suppress is null and orig_no in $filter
			and o.orig_no != o.parent_no
		ORDER BY o.ri DESC, o.pubyr DESC, o.opinion_no DESC");
	
	# In order to repeat the cycle check, we need to grab these new
	# opinions.
	
	$synonym_opinions = $dbh->prepare("
		SELECT orig_no, parent_no, ri, pubyr, opinion_no
		FROM $CLASS_TEMP
		WHERE status != 'belongs to' and parent_no != 0
			and orig_no in $filter");
	
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
	
	logMessage(1, "    found " . scalar(@breaks) . " cycle(s)");
    }
    
    # Now that we have broken all of the cycles in $CLASS_TEMP, we can
    # fill in synonym_no in $TREE_TEMP.
    
    logMessage(2, "    setting synonym_no");
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN $CLASS_TEMP b USING (orig_no)
			SET t.synonym_no = b.parent_no
			WHERE status != 'belongs to'");
    
    # Now, for all taxa that don't have a senior synonym, we need to set
    # synonym_no = orig_no.  In other words, any taxa that have no senior
    # synonyms are their own most-senior synonym.  This will facilitate
    # lookups in the rest of the code.
    
    $result = $dbh->do("UPDATE $TREE_TEMP SET synonym_no = orig_no
			WHERE synonym_no = 0");
    
    # Now that we have set synonym_no, we can efficiently index it.
    
    logMessage(2, "    indexing synonym_no");
    
    $result = $dbh->do("ALTER TABLE $TREE_TEMP add index (synonym_no)");
    
    my $a = 1;		# we can stop here when debugging
}


# deChainSynonyms ( dbh )
# 
# Alter the synonym_no field as follows: whenever we have a -> b and b -> c,
# change the relation so that a -> c and b -> c.  This allows synonym_no to
# represent the most senior synonym, instead of just the immediate senior
# synonym.  Because the chains may be more than three taxa long, we need to
# repeat the process until no more rows are affected.

sub deChainSynonyms {

    my ($dbh) = @_;
    
    # Repeat the following process until no more rows are affected, with a
    # limit of 20 to avoid an infinite loop just in case our algorithm above
    # was faulty and some cycles have slipped through.
    
    my $count = 0;
    my $result;
    
    do
    {
	$result = $dbh->do("
		UPDATE $TREE_TEMP t1 JOIN $TREE_TEMP t2
			ON t1.synonym_no = t2.orig_no and t1.synonym_no != t2.synonym_no
		SET t1.synonym_no = t2.synonym_no");
    }
	while $result > 0 && ++$count < 20;
    
    if ( $count >= 20 )
    {
	logMessage(0,"WARNING - possible synonymy cycle detected during de-chaining");
    }
    
    my $a = 1;		# we can stop here when debugging
}


# computeHierarchy ( dbh )
# 
# Fill in the parent_no and opinion_no fields of $TREE_TEMP.  This computes
# the "hierarchy" relation for all of the concepts represented in $TREE_TEMP.
# 
# We start with the set of classification opinions chosen by
# computeSynonymy(), but we then recompute all of the ones that specify
# hierarchy (the 'belongs to' opinions).  This time, we consider all of the
# opinions for each senior synonym along with all of the opinions for its
# immediate junior synonyms, and choose the most recent and reliable from that
# set.  Note that we leave out nomina dubia, nomina vana, nomina nuda, nomina
# oblita, and invalid subgroups, because an opinion on any of those shouldn't
# affect the senior concept.
# 
# After this is done, we must check for cycles using the same procedure as
# computeSynonymy().  Only then can we set the parent_no field of $TREE_TEMP.
# Finally, we set opinion_no for each row of $TREE_TEMP, based on the modified
# set of classification opinions.

sub computeHierarchy {
    
    my ($dbh) = @_;
    
    my ($result, $filter, @check_taxa, %taxa_moved);
    
    logMessage(1, "computing hierarchy relation (d)");
    $DB::single = 1;
    
    # We already have the $CLASS_TEMP relation, but we need to adjust it by
    # grouping together all of the opinions for each senior synonym and its
    # immediate juniors and re-selecting the classification opinion for each
    # group.  Note that the junior synonyms already have their classification
    # opinion selected from computeSynonymy() above.
    
    # We need to create an auxiliary table to do this grouping.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SYNONYM_TEMP");
    $result = $dbh->do("CREATE TABLE $SYNONYM_TEMP
				(junior_no int unsigned,
				 senior_no int unsigned,
				 primary key (junior_no),
				 key (senior_no))");
    
    # First, we add all immediately junior synonyms, but only subjective and
    # objective synonyms and replaced taxa.  We leave out nomina dubia, nomina
    # vana, nomina nuda, nomina oblita, and invalid subgroups, because an
    # opinion on any of those shouldn't affect the senior taxon.  The last
    # clause excludes chained junior synonyms.
    
    $result = $dbh->do("INSERT IGNORE INTO $SYNONYM_TEMP
			SELECT t.orig_no, t.synonym_no
			FROM $TREE_TEMP t JOIN $CLASS_TEMP c USING (orig_no)
			WHERE c.status in ('subjective synonym of', 'objective synonym of',
						'replaced by')
				and t.synonym_no = c.parent_no");
    
    # Next, we add entries for all of the senior synonyms, because of course
    # their own opinions are considered as well.
    
    $result = $dbh->do("INSERT IGNORE INTO $SYNONYM_TEMP
    			SELECT DISTINCT senior_no, senior_no
			FROM $SYNONYM_TEMP");
    
    # Next, we delete the classification opinion for each taxon in
    # $SYNONYM_TEMP.
    
    $result = $dbh->do("DELETE QUICK FROM $CLASS_TEMP
			USING $CLASS_TEMP JOIN $SYNONYM_TEMP
				ON $CLASS_TEMP.orig_no = $SYNONYM_TEMP.senior_no");
    
    # Then we use the same INSERT IGNORE trick to select the best opinion for
    # these senior synonyms, considering all of the 'belongs to' opinions from
    # its synonym group.
    
    $result = $dbh->do("INSERT IGNORE INTO $CLASS_TEMP
			SELECT c.senior_no, o.opinion_no, o.parent_no, o.ri, 
				o.pubyr, o.status
			FROM $OPINION_CACHE o
			    JOIN $SYNONYM_TEMP c ON o.orig_no = c.junior_no
			WHERE o.status = 'belongs to' and c.senior_no != o.parent_no
			ORDER BY o.ri DESC, o.pubyr DESC, o.opinion_no DESC");
    
    # The next step is to check for cycles within $CLASS_TEMP, for which we
    # use the same algorithm as is used for the rebuild operation.  This won't
    # catch all cycles on updates, so we have to rely on the opinion-editing
    # code catching obvious cycles.  In any case, the tree will be fixed on
    # the next rebuild (if the user doesn't catch it and undo the change
    # themself).
    
    logMessage(2, "    downloading hierarchy opinions");
    
    my $best_opinions = $dbh->prepare("
		SELECT orig_no, parent_no, ri, pubyr, opinion_no
		FROM $CLASS_TEMP WHERE parent_no > 0");
    
    $best_opinions->execute();
    
    # The %children array lists all of the taxa that are asserted to belong to
    # a given taxon.  The %opinions array is indexed by the child taxon
    # number, and holds the information necessary to determine the most recent
    # and reliable opinion from any given set.
    
    my (%children, %opinions);
    
    while ( my ($child, $parent, $ri, $pub, $op) =
			$best_opinions->fetchrow_array() )
    {
	# The following should never occur, this check is put in to give
	# warning in case a subsequent change to the code causes this error.
	
	if ( $child == $parent )
	{
	    print STDERR "Update: WARNING - best opinion for $child has same parent (opinion $op)\n";
	    next;
	}

	$children{$parent} = [] unless exists $children{$parent};
	push @{$children{$parent}}, $child;
	
	$opinions{$child} = [$parent, $ri, $pub, $op];
    }
    
    logMessage(2, "    checking for cycles");
    
    my @breaks = breakCycles($dbh, \%children, \%opinions);
    
    logMessage(1, "    found " . scalar(@breaks) . " cycle(s)");
    
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
	
	my $insert_suppress = $dbh->prepare("INSERT IGNORE INTO $SUPPRESS_TEMP
					     VALUES (?, 1)");
	
	foreach my $pair (@breaks)
	{
	    my ($check_taxon, $suppress_opinion) = @$pair;
	    
	    $insert_suppress->execute($suppress_opinion);
	    
	    push @check_taxa, $check_taxon;
	    $taxa_moved{$check_taxon} = 0;
	}
	
	# We also have to clean up the $CLASS_TEMP table, so that we can
	# compute the next-best opinion for each suppressed one.  So we delete
	# the suppressed opinions, and replace them with the next-best
	# opinion.  We need to delete first, because there may be no next-best
	# opinion!
	
	$filter = '(' . join(',', @check_taxa) . ')';
	
	$result = $dbh->do("DELETE FROM $CLASS_TEMP WHERE orig_no in $filter");
	
	$result = $dbh->do("
		INSERT IGNORE INTO $CLASS_TEMP
		SELECT s.senior_no, o.opinion_no, o.parent_no, o.ri, 
			o.pubyr, o.status
		FROM $OPINION_CACHE o JOIN $SYNONYM_TEMP s ON o.orig_no = s.junior_no
			LEFT JOIN $SUPPRESS_TEMP USING (opinion_no)
		WHERE suppress is null and orig_no in $filter
			and o.status = 'belongs to' and s.senior_no != o.parent_no
		ORDER BY o.ri DESC, o.pubyr DESC, o.opinion_no DESC");
	
	# In order to repeat the cycle check, we need to grab these new
	# opinions.
	
	my $belongs_opinions = $dbh->prepare("
		SELECT orig_no, parent_no, ri, pubyr, opinion_no
		FROM $CLASS_TEMP WHERE parent_no > 0 and orig_no in $filter");
	
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
	
	logMessage(1, "    found " . scalar(@breaks) . " cycle(s)");
    }
    
    # Now we can set and then index opinion_no.
    
    logMessage(2, "    setting opinion_no");
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN $CLASS_TEMP c USING (orig_no)
			SET t.opinion_no = c.opinion_no");
    
    logMessage(2, "    indexing opinion_no");
    
    $result = $dbh->do("ALTER TABLE $TREE_TEMP add index (opinion_no)");
    
    # Now we can set parent_no.  All concepts in a synonym group will share
    # the same parent, so we need to join a second copy of $TREE_TEMP to look
    # up the senior synonym number.  In addition, parent_no must always point
    # to a senior synonym, so we need to join a third copy of $TREE_TEMP to
    # look up the parent's senior synonym number.
    # 
    # In other words, the parent_no value for any taxon will be the senior
    # synonym of the parent of the senior synonym.
    
    logMessage(2, "    setting parent_no");
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN $TREE_TEMP t2 ON t2.orig_no = t.synonym_no
				JOIN $OPINION_CACHE o ON o.opinion_no = t2.opinion_no
				JOIN $TREE_TEMP t3 ON t3.orig_no = o.parent_no
			SET t.parent_no = t3.synonym_no");
    
    # Once we have set parent_no for all concepts, we can efficiently index it.
    
    logMessage(2, "    indexing parent_no");
    
    $result = $dbh->do("ALTER TABLE $TREE_TEMP add index (parent_no)");
    
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
# we've seen already on any search, and stop whenever we encounter a node that
# we've seen already on this search.  In that case, we have a cycle.
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
	
	my @search = ($t);	# this is the breadth-first search queue
	my $cycle_found = 0;	# this flag indicates that we found a cycle
	
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


# deChainReferences ( dbh )
# 
# Update all synonym_no and parent_no values in $TREE_TEMP that point out of
# that table (in other words, those that do not correspond to orig_no values
# in $TREE_TEMP).  These might point to junior synonyms in $TREE_TABLE, and so
# they must be updated to point to the proper senior synonyms as indicated by
# the corresponding synonym_no values in $TREE_TABLE.
# 
# Note that all synonym_no values that point to other rows in $TREE_TEMP have
# already been updated by deChainSynonyms().  Note also that we only have to
# do this once because $TREE_TABLE and $TREE_TEMP each have no synonym_no
# chains internally.  We only have to worry about single-link chains between
# the two tables.
    
sub deChainReferences {
    
    my ($dbh) = @_;
    
    my $result;
    
    # First the synonym_no values
    
    $result = $dbh->do("
		UPDATE $TREE_TEMP as t1
			LEFT JOIN $TREE_TEMP as t2 ON t1.synonym_no = t2.orig_no
			JOIN $TREE_TABLE as m ON t1.synonym_no = m.orig_no
		SET t1.synonym_no = m.synonym_no
		WHERE t2.orig_no is null");
    
    # Then the parent_no values
    
    $result = $dbh->do("
		UPDATE $TREE_TEMP as t1
			LEFT JOIN $TREE_TEMP as t2 ON t1.parent_no = t2.orig_no
			JOIN $TREE_TABLE as m ON t1.parent_no = m.orig_no
		SET t1.parent_no = m.synonym_no
		WHERE t2.orig_no is null");
    
    my $a = 1;		# we can stop here when debugging
}


# treePerturbation ( dbh )
# 
# Determine which taxa have changed parents.  At the same time, estimate the
# amount of time it would take to adjust the tree sequence in-place to account
# for this.  Return the estimate, with a figure of 1 representing an estimate
# that it would take just as long to recompute the whole tree.
# 
# The reason we need to make this calculation is that nested-set trees are
# very expensive to alter.  A single moved taxon may require changing all of
# the rows of $TREE_TABLE.

sub estimateTreePerturbation {
    
    my ($dbh) = @_;
    
    my $result;
    
    # First, we get the tree size.
    
    my ($tree_size) = $dbh->selectrow_array("
		SELECT count(*) from $TREE_TABLE WHERE lft is not null");
    
    # Then determine which of the taxa in $TREE_TEMP have changed parents, and
    # grab some information about them.  We need to compute the total number
    # of taxa that are moving, plus the total perturbation (number of tree
    # rows that will have to be changed to move these taxa).  The latter is an
    # estimate, so does not need to be exact.
    
    my $changed_taxa = $dbh->prepare("
		SELECT orig_no, t2.lft, t2.rgt, t3.lft
		FROM $TREE_TEMP t1 JOIN $TREE_TABLE t2 USING (orig_no)
			JOIN $TREE_TABLE t3 ON t3.orig_no = t2.parent_no
		WHERE t1.parent_no != t2.parent_no");
    
    $changed_taxa->execute();
    
    my ($move_count, $perturbation) = @_;
    
    while ( my($orig_no, $old_lft, $old_rgt, 
	       $new_lft) = $changed_taxa->fetchrow_array() )
    {
	$move_count++;
	if ( $old_lft > 0 )
	{
	    $perturbation += ($old_rgt - $old_lft) + ($new_lft - $old_lft);
	}
	else
	{
	    $perturbation += ($tree_size - $new_lft);
	}
    }
    
    # If there is nothing to move, then the perturbation of the tree is 0.
    
    if ( $move_count == 0 )
    {
	return 0;
    }
    
    else
    {
	return $perturbation / $tree_size;
    }
}


our (%children, %parent, %tree);

# computeTreeSequence ( dbh )
# 
# Fill in the lft, rgt and depth fields of $TREE_TMP.  This has the effect of
# arranging the rows into a forest of Nested Set trees.  For more information,
# see: http://en.wikipedia.org/wiki/Nested_set_model.

sub computeTreeSequence {
    
    my ($dbh) = @_;
    
    my $result;
    
    logMessage(2, "traversing and marking taxon trees (e)");
    $DB::single = 1;
    
    logMessage(2, "    downloading hierarchy relation");
    
    my $pc_pairs = $dbh->prepare("SELECT orig_no, parent_no
				  FROM $TREE_TEMP"); 
    
    $pc_pairs->execute();
    
    while ( my ($child_no, $parent_no) = $pc_pairs->fetchrow_array() )
    {
	if ( $parent_no > 0 )
	{
	    $children{$parent_no} = [] unless defined $children{$parent_no};
	    push @{$children{$parent_no}}, $child_no;
	}
	$parent{$child_no} = $parent_no;
    }
    
    # Now we create the "main" tree, starting with taxon 1 'Eukaryota' at the
    # top of the tree with sequence=1 and depth=1.  The variable $sequence
    # gets the maximum sequence number from the newly created tree.  Thus, the
    # next tree (created below) uses $sequence+1 as its sequence number.
    
    logMessage(2, "    sequencing tree rooted at 'Eukaryota'");
    
    my $sequence = createNode(1, 1, 1);
    
    # Next, we go through all of the other taxa.  When we find a taxon with no
    # parent that we haven't visited yet, we create a new tree with it as the
    # root.  This takes care of all the taxon for which their relationship to
    # the main tree is not known.
    
    logMessage(2, "    sequencing all other taxon trees");
    
 taxon:
    foreach my $taxon (keys %parent)
    {
	next if $parent{$taxon} > 0;		# skip any taxa that aren't roots
	next if exists $tree{$taxon};		# skip any that we've already inserted
	
	$sequence = createNode($taxon, $sequence+1, 1);
    }
    
    # Now we need to upload all of the tree sequence data to the server so
    # that we can set lft, rgt, and depth in $TREE_TEMP.  To do this
    # efficiently, we use an auxiliary table and a large insert statement.
    
    logMessage(2, "    uploading tree sequence data");
    
    $dbh->do("DROP TABLE IF EXISTS tree_insert");
    $dbh->do("CREATE TABLE tree_insert
     		       (orig_no int unsigned,
     			lft int unsigned,
     			rgt int unsigned,
     			depth int unsigned)");
    
    my $insert_stmt = "INSERT INTO tree_insert VALUES ";
    my $comma = '';
    
    foreach my $taxon (keys %tree)
    {
	$insert_stmt .= $comma;
	$insert_stmt .= "($taxon," . join(',', @{$tree{$taxon}}) . ')';
	$comma = ',';
	
	if ( length($insert_stmt) > 200000 )
	{
	    $result = $dbh->do($insert_stmt);
	    $insert_stmt = "INSERT INTO tree_insert VALUES ";
	    $comma = '';
	}
    }
    
    $result = $dbh->do($insert_stmt) if $comma;
    
    # Now that we have uploaded the data, we can copy it into $TREE_TEMP and
    # then delete the temporary table.
    
    logMessage(2, "    setting lft, rgt, depth");
    
    $result = $dbh->do("ALTER TABLE tree_insert ADD INDEX (orig_no)");
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN tree_insert i USING (orig_no)
			SET t.lft = i.lft, t.rgt = i.rgt, t.depth = i.depth");
    
    $result = $dbh->do("DROP TABLE IF EXISTS tree_insert");
    
    # Now we can efficiently index $TREE_TEMP on lft, rgt and depth.
    
    logMessage(2, "    indexing lft, rgt, depth");
    
    $result = $dbh->do("ALTER TABLE $TREE_TEMP add index (lft)");
    $result = $dbh->do("ALTER TABLE $TREE_TEMP add index (rgt)");
    $result = $dbh->do("ALTER TABLE $TREE_TEMP add index (depth)");
    
    my $a = 1;
}


# createNode ( taxon, sequence, depth )
# 
# This recursive procedure creates a tree node for the given taxon, with the
# given sequence number and depth, and then goes on to recursively create
# nodes for all of its children.  Thus, the sequence numbers are set in a
# preorder traversal.
# 
# Each node created has its 'lft' field set to its sequence number, and its
# 'rgt' field set to the maximum sequence number of all its children.  Any
# taxon which has no children will get the same 'lft' and 'rgt' value.  The
# 'depth' field marks the distance from the root of the current tree, with
# top-level nodes having a depth of 1.

sub createNode {

    my ($taxon, $sequence, $depth) = @_;
    
    # First check that we haven't seen this taxon yet.  That should always be
    # true, unless there are cycles in the parent-child relation (which would
    # be bad!)
    
    unless ( exists $tree{$taxon} )
    {
	# Create a new node to represent the taxon.
	
	$tree{$taxon} = [$sequence, $sequence, $depth];
	
	# If this taxon has children, we must then iterate through them and
	# insert each one (and all its descendants) into the tree.  If there
	# are no children, we just leave the node as it is.
	
	if ( exists $children{$taxon} )
	{
	    foreach my $child ( @{$children{$taxon}} )
	    {
		$sequence = createNode($child, $sequence+1, $depth+1);
	    }
	    
	    # When we are finished with all of the children, fill in the 'rgt'
	    # field with the value returned from the last child.
	    
	    $tree{$taxon}[1] = $sequence;
	}
	
	return $sequence;
    }
    
    # If we have already seen this taxon, then we have a cycle!  Print a
    # warning, and otherwise ignore it.  This means that we have to return
    # $sequence-1, because we didn't actually insert any node.
    
    else
    {
	logMessage(0, "WARNING - tree cycle for taxon $taxon");
	return $sequence-1;
    }
}


# computeAncestry ( dbh )
# 
# Fill in $ANCESTOR_TEMP with the transitive closure of the hierarchy
# relation.  In other words: for every taxon t, for every ancestor u of t, the
# record (t, u, d) will be entered into $ANCESTOR_TEMP where d is the
# depth of u in its taxon tree.  This allows us to easily obtain a list of
# all of the ancestors of any taxon, in rank order.

sub computeAncestry {

    my ($dbh) = @_;
    
    my $result;
    
    logMessage(2, "computing ancestry relation (f)");
    $DB::single = 1;
    
    # Before we start building the ancestry relation, we need to know the
    # maximum taxon tree depth.
    
    my ($max_depth) = $dbh->selectrow_array("SELECT max(depth) FROM $TREE_TEMP");
    
    # It is also helpful to generate an auxiliary table which will contain the
    # hierarchy relation restricted to concept group leaders, along with the
    # depth of the parent node.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $PARENT_TEMP");
    $result = $dbh->do("CREATE TABLE $PARENT_TEMP
			       (child_no int unsigned,
				parent_no int unsigned,
				depth int unsigned)");
    
    $result = $dbh->do("INSERT INTO $PARENT_TEMP
			SELECT orig_no, parent_no, depth - 1
			FROM $TREE_TEMP WHERE parent_no != 0");
    
    $result = $dbh->do("ALTER TABLE $PARENT_TEMP ADD KEY (depth, parent_no)");
    
    # We will build the ancestry relation iteratively.  We start by slicing up
    # the auxiliary table by depth, and seeding the ancestor relation with
    # slice 1 (the top-most nodes of the taxon trees).
    #
    # Then we iterate through the rest of the slices.  Each slice in turn is
    # conjugated with the ancestor relation, and the resulting pairs are added
    # in.  The slice itself is then added as well.  This will have the result
    # of generating a transitive closure of the hierarchy relation, restricted
    # to concept group leaders.
    
    my $add_conjugation = $dbh->prepare("
			INSERT INTO $ANCESTOR_TEMP
			SELECT s.child_no, a.parent_no, a.depth
			FROM $PARENT_TEMP s JOIN $ANCESTOR_TEMP a
				ON s.parent_no = a.child_no and s.depth = ?");
    
    my $add_slice = $dbh->prepare("
			INSERT INTO $ANCESTOR_TEMP
			SELECT child_no, parent_no, depth
			FROM $PARENT_TEMP WHERE depth = ?");
    
    print STDERR "Rebuild:     depth 1\n";
    
    $add_slice->execute(1);
    
    for ( my $i = 2; $i < $max_depth; $i++ )
    {
	logMessage(2, "    depth $i");
	$result = $add_conjugation->execute($i);
	$result = $add_slice->execute($i);
    }
    
    my $a = 1;		# we can stop here when debugging
}


# updateTreeTables ( dbh )
# 
# Do the final steps in the update procedure.  These are as follows:
# 
# 1) copy everything from $TREE_TEMP into $TREE_TABLE, overwriting
#    corresponding rows
# 2) adjust the tree sequence in $TREE_TABLE
# 
# These two have to be quick, since we need to do them with $TREE_TABLE
# locked.
# 
# 3) copy everything from $SUPPRESS_TEMP into $SUPPRESS_TABLE, and clear
#    entries from $SUPPRESS_TABLE that should no longer be there.

sub updateTreeTables {

    my ($dbh) = @_;
    
    my $result;
    
    # The first thing to do is to determine which rows have a changed
    # parent_no value.  Each one will require an adjustment to the tree
    # sequence.  We have to do this now, before we copy the rows from
    # $TREE_TEMP over the corresponding rows from $TREE_TABLE below.
    
    # We order the rows in descending order of position to make sure that
    # children are moved before their parents.  This is necessary in order to
    # make sure that a parent is never moved underneath one of its existing
    # children (we have already eliminated cycles, so if this is going to
    # happen then the child must be moving as well).
    
    my $moved_taxa = $dbh->prepare("
		SELECT t.orig_no, t.parent_no
		FROM $TREE_TEMP as t LEFT JOIN $TREE_TABLE as m USING (orig_no)
		WHERE t.parent_no != m.parent_no
		ORDER BY m.lft DESC");
    
    $moved_taxa->execute();
    
    my (@move);
    
    while ( my ($orig_no, $new_parent) = $moved_taxa->fetchrow_array() )
    {
	push @move, [$orig_no, $new_parent];
    }
    
    # Next, we need to fill in $TREE_TEMP with the corresponding lft, rgt, and
    # depth values from $TREE_TABLE.  For all rows which aren't being moved to
    # a new location in the hierarcy, those values won't be changing.  Those
    # that are being moved will be updated below.
    
    $result = $dbh->do("UPDATE $TREE_TEMP as t JOIN $TREE_TABLE as m USING (orig_no)
			SET t.lft = m.lft, t.rgt = m.rgt, t.depth = m.depth");
    
    # Before we update $TREE_TABLE, we need to lock it for writing.  This will
    # ensure that all other threads see it in a consistent state, either pre-
    # or post-update.  This means that we also have to lock $TREE_TEMP for
    # read.
    
    $result = $dbh->do("LOCK TABLE $TREE_TABLE as m WRITE,
				   $TREE_TEMP as t READ");
    
    # Now write all rows of $TREE_TEMP over the corresponding rows of
    # $TREE_TABLE.

    $result = $dbh->do("REPLACE INTO $TREE_TABLE
			SELECT * FROM $TREE_TEMP");
    
    # Now adjust the tree sequence to take into account all rows that have
    # changed their position in the hierarchy.  Note that we couldn't do this
    # beforehand because the rows in $TREE_TEMP may have child rows that are
    # not themselves in $TREE_TEMP.  We had to wait until $TREE_TABLE was
    # otherwise completely updated and then do the adjustments in-place.
    
    foreach my $pair (@move)
    {
	adjustTreeSequence($dbh, @$pair);
    }
    
    # Now we can unlock the tables.
    
    $result = $dbh->do("UNLOCK TABLES");
    
    # Finally, we need to update $SUPPRESS_TABLE.  We need to add everything
    # that is in $SUPPRESS_TEMP, and remove everything that corresponds to a
    # classification opinion in $TREE_TEMP.  The latter represent opinions
    # which were previously suppressed, but now should not be.
    
    $result = $dbh->do("INSERT INTO $SUPPRESS_TABLE
			SELECT * FROM $SUPPRESS_TEMP");
    
    $result = $dbh->do("DELETE $SUPPRESS_TABLE FROM $SUPPRESS_TABLE
				JOIN $TREE_TEMP USING (opinion_no)");
    
    my $a = 1;		# we can stop here when debugging
}


# adjustTreeSequence ( dbh, orig_no, new_parent )
# 
# Adjust the tree sequence so that taxon $orig_no (along with all of its
# children) falls properly under its new parent.  The taxon will end up as the
# last child of the new parent.

sub adjustTreeSequence {
    
    my ($dbh, $orig_no, $new_parent) = @_;
    
    my $sql = '';
    my $change_depth = '';
    
    # First, find out where in the tree the taxon (and all its children)
    # previously fell.
    
    my ($old_pos, $old_rgt, $old_depth) = $dbh->selectrow_array("
		SELECT lft, rgt, depth
		FROM $TREE_TABLE WHERE orig_no = $orig_no");
    
    # With this information we can calculate the size of the subtree being
    # moved.
    
    my $width = $old_rgt - $old_pos + 1;
    
    # Next, figure out where in the tree the new parent is currently
    # located.  If its new parent is 0, move the subtree to the very end.
    
    my ($parent_pos, $parent_rgt, $new_depth);
    
    if ( $new_parent > 0 )
    {
	($parent_pos, $parent_rgt, $new_depth) = $dbh->selectrow_array("
		SELECT lft, rgt, depth+1 FROM $TREE_TABLE where orig_no = $new_parent");
	
	# Do nothing if we are instructed to move a taxon into its own subtree.
	
	if ( $parent_pos >= $old_pos && $parent_pos <= $old_rgt )
	{
	    return;
	}
    }
    
    else
    {
	$parent_pos = 0;
	$new_depth = 1;
	($parent_rgt) = $dbh->selectrow_array("SELECT max(lft) from $TREE_TABLE");
    }
    
    # if we're moving to higher sequence numbers we do it like so:
    
    if ( $parent_rgt >= $old_pos )
    {
	# First compute the displacement of the subtree
	
	my $disp = $parent_rgt - $old_rgt;
	
	# If we are changing depth, create an sql clause to do that
	
	if ( $new_depth != $old_depth )
	{
	    my $depth_diff = $new_depth - $old_depth;
	    $change_depth = ",depth = depth + if(lft <= $old_rgt, $depth_diff, 0)"
	}
	
	# Now update lft, rgt and depth values for every row lying between the
	# old position of the subtree and the new position (inclusive).
	
	$sql = "UPDATE $TREE_TABLE
		SET rgt = rgt + if(lft > $old_rgt,
				   if(rgt < $parent_rgt or lft > $parent_pos, -$width, 0),
				   $disp)
		    $change_depth,
		    lft = lft + if(lft > $old_rgt, -$width, $disp)
		WHERE lft >= $old_pos and lft <= $parent_rgt";
	
	$dbh->do($sql);
	
	# All rows which were parents of the taxon in its old position but are
	# no longer parents of it will need their rgt values to shrink by the
	# width of the tree.
	
	$sql = "UPDATE $TREE_TABLE
		SET rgt = rgt - $width
		WHERE lft < $old_pos and rgt >= $old_rgt and
			(lft > $parent_pos or rgt < $parent_rgt)";
	
	$dbh->do($sql);
    }
    
    # If we're moving to lower sequence numbers, we do it like so:
    
    elsif ( $parent_rgt < $old_pos )
    {
	# First compute the displacement of the subtree
	
	my $disp = ($parent_rgt + 1) - $old_pos;
	
	# If we are changing depth, create an sql clause to do that
	
	if ( $new_depth != $old_depth )
	{
	    my $depth_diff = $new_depth - $old_depth;
	    $change_depth = ",depth = depth + if(lft >= $old_pos, $depth_diff, 0)"
	}
	
	# Now update lft, rgt and depth values for every row lying between the
	# old position of the subtree and the new position (inclusive).
	
	$sql = "UPDATE $TREE_TABLE
		SET rgt = rgt + if(lft < $old_pos,
				   if(rgt < $old_pos, $width, 0),
				   -$disp)
		    $change_depth,
		    lft = lft + if(lft < $old_pos, $width, -$disp)
		WHERE lft > $parent_rgt and lft <= $old_rgt";
	
	$dbh->do($sql);
	
	# All rows which are parents of the taxon in its new position but were
	# not in its old position will need their rgt values to grow by the
	# width of the tree.
	
	$sql = "UPDATE $TREE_TABLE
		SET rgt = rgt + $width
		WHERE lft <= $parent_pos and rgt >= $parent_rgt and rgt < $old_pos";
	
	$dbh->do($sql);
    }
    
    my $a = 1;		# we can stop here when debugging
}


# activateNewTables ( dbh, keep_temps )
# 
# In one atomic operation, move the new taxon tables to active status and swap
# out the old ones.  Those old ones are then deleted.
# 
# If keep_temps is true, then keep around the temporary tables that we created
# during the rebuild process (this might be done for debugging purposes).
# Otherwise, these are deleted.

sub activateNewTables {

    my ($dbh, $keep_temps) = @_;
    
    my $result;
    
    logMessage(2, "activating new tables (g)");
    $DB::single = 1;
    
    # Delete any backup tables that might still be around
    
    $result = $dbh->do("DROP TABLE IF EXISTS $TREE_BAK");
    # $result = $dbh->do("DROP TABLE IF EXISTS $ANCESTOR_BAK");
    $result = $dbh->do("DROP TABLE IF EXISTS $SUPPRESS_BAK");
    
    # Create dummy versions of any of the main tables that might be currently
    # missing
    
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $TREE_TABLE LIKE $TREE_TEMP");
    #$result = $dbh->do("CREATE TABLE IF NOT EXISTS $ANCESTOR_TABLE LIKE $ANCESTOR_TEMP");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $SUPPRESS_TABLE LIKE $SUPPRESS_TEMP");

    
    # Now do the Atomic Table Swap (tm)
    
    $result = $dbh->do("RENAME TABLE
				$TREE_TABLE to $TREE_BAK,
				$TREE_TEMP to $TREE_TABLE,
				$SUPPRESS_TABLE to $SUPPRESS_BAK,
				$SUPPRESS_TEMP to $SUPPRESS_TABLE");

    #				$ANCESTOR_TABLE to $ANCESTOR_BAK,
    #				$ANCESTOR_TEMP to $ANCESTOR_TABLE,
    
    # Then we can get rid of the backup tables
    
    $result = $dbh->do("DROP TABLE $TREE_BAK");
    # $result = $dbh->do("DROP TABLE $ANCESTOR_BAK");
    $result = $dbh->do("DROP TABLE $SUPPRESS_BAK");
    
    # Delete the auxiliary tables too, unless we were told to keep them.
    
    unless ( $keep_temps )
    {
	logMessage(2, "removing temporary tables");
	
	$result = $dbh->do("DROP TABLE IF EXISTS $SPELLING_TEMP");
	$result = $dbh->do("DROP TABLE IF EXISTS $CLASS_TEMP");
	$result = $dbh->do("DROP TABLE IF EXISTS $PARENT_TEMP");
    }
    
    logMessage(1, "done rebuilding taxon trees");
    
    my $a = 1;		# we can stop here when debugging
}


# logMessage ( level, message )
# 
# If $level is greater than or equal to the package variable $MSG_LEVEL, then
# print $message to standard error.

sub logMessage {

    my ($level, $message) = @_;
    
    if ( $level <= $MSG_LEVEL )
    {
	print STDERR "$MSG_TAG: $message\n";
    }
}


sub clearSpelling {

    my ($dbh) = @_;
    
    $dbh->do("UPDATE $TREE_TEMP SET spelling_no = 0");
}


sub clearSynonymy {
    
    my ($dbh) = @_;
    
    $dbh->do("UPDATE $TREE_TEMP SET synonym_no = 0");
}


sub clearHierarchy {
    
    my ($dbh) = @_;
    
    $dbh->do("UPDATE $TREE_TEMP SET opinion_no = 0, parent_no = 0");
}


sub clearTreeSequence {

    my ($dbh) = @_;
    
    $dbh->do("UPDATE $TREE_TEMP SET lft = null, rgt = null, depth = null");
}


1;
