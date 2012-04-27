# 
# The Paleobiology Database
# 
#   TaxaTree.pm
# 

=head1 General Description

This module builds and maintains a hierarchy of taxonomic names.  This
hierarchy is based on the data in the C<opinions> and C<authorities> tables,
and is stored in the tables C<taxa_tree> and C<taxa_parents>.  These tables
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

The entries in C<taxa_tree> are in 1-1 correspondence with the entries in the
C<authorities> table, linked by the key field C<taxon_no>.  These taxa are further
organized according to four separate relations, each computed from data in the
C<opinions> table.  The names listed in parentheses are the fields in
C<taxa_tree> which record each relation:

=over 4

=item Original combination (orig_no)

This relation associates each taxon with the name/rank combination by which it
was originally known.

=item Taxonomic concept group (spelling_no)

This relation groups together all of the name/rank combinations that represent
the same taxonomic concept.  In other words, when a taxon's rank is changed,
or its spelling is changed, all of the resultant entries will have a different
C<taxon_no> but the same C<spelling_no>.

The value of C<spelling_no> is the C<taxon_no> of the currently accepted
name/rank combination for the given entry.  Thus, the currently accepted
combination for any taxon can be quickly and efficiently looked up.  We will
refer to this taxon as the "group leader" in the remainder of this
documentation.  A group leader always has C<taxon_no> = C<spelling_no>.

Note that this relation can also be taken as an equivalence relation, whereas
two taxa have the same C<spelling_no> if and only if they represent the same
taxonomic concept.  Note also that a taxon and its original combination always
have the same C<spelling_no>.

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
different C<spelling_no>.  This relation, like the one before, can also be
taken as an equivalence relation, whereas two taxa have the same C<synonym_no>
if and only if they are synonyms of each other.

=item Hierarchy (parent_no)

This relation associates lower with higher taxa.  It forms a collection of
trees, because (as noted above) there are a number of fossil taxa for which it
is not known within which higher taxa they fall.

Any taxon which does not fall within another taxon in the database (either
because no such relationship is known or because it is a maximally general
taxon) will have C<parent_no> = 0.

All taxa which are synonyms of each other will have the same C<parent_no> value,
and the C<parent_no> (if not 0) will always refer to a taxon which is a group
leader with no senior synonym.  Thus, a parent taxon will always have
C<synonym_no> = C<taxon_no> = C<spelling_no>.

This relation, like the previous ones, can be taken as an equivalence
relation, whereas two taxa have the same C<parent_no> if and only if they are
siblings of each other.

=back

=head2 Opinions

In addition to the fields listed above, each entry in C<taxa_tree> also has
an C<opinion_no> field.  This field points to the "best opinion" (most recent
and reliable) that has been algorithmically selected from the available
opinions for that taxon.

For a junior synonym, the value of opinion_no will be the opinion which
specifies its immediately senior synonym.  There may exist synonym chains in
the database, where A is a junior synonym of B which is a junior synonym of
C.  In any case, C<synonym_no> should always point to the most senior synonym
of each taxon.

For a senior synonym, or for any taxon which does not have synonyms, the
value of C<opinion_no> will be the opinion which specifies its immediately
higher taxon.

=head2 Tree structure

In order to facilitate logical operations on the taxa hierarchy, the entries
in C<taxa_cache> are marked via preorder tree traversal.  This is done with
fields C<lft> and C<rgt>.  The C<lft> field stores the traversal sequence, and
the C<rgt> field of a given entry stores the maximum C<lft> value of the entry
and all of its descendants.  An entry which has no descendants has C<lft> =
C<rgt>.  Using these fields, we can formulate simple and efficient SQL queries
to fetch all of the descendants of a given entry and other similar operations.
For more information, see L<http://en.wikipedia.org/wiki/Nested_set_model>.

All entries in the same concept group have the same C<lft> and C<rgt> values.

The one operation that is not easy to do using the preorder traversal sequence
is to compute the list of all parents of a given taxon.  Thus, we use a
separate table, C<taxa_parents> to store this information.  This table has
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
use Constants qw($TAXA_TREE_CACHE $TAXA_LIST_CACHE $IS_FOSSIL_RECORD);

use strict;

our $TREE_TABLE = "taxa_cache";
our $PARENT_TABLE = "taxa_parents";
our $SUPPRESS_TABLE = "suppress_opinions";

our $TREE_TEMP = "tn";
our $PARENT_TEMP = "pn";
our $SUPPRESS_TEMP = "sn";

our $TREE_BAK = "taxa_cache_bak";
our $PARENT_BAK = "taxa_parents_bak";
our $SUPPRESS_BAK = "suppress_bak";

=item rebuild ( dbh )

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
    
    my ($dbh, %step) = @_;
    
    # We start by saving the time at which this procedure starts.  Any
    # authorities or opinions added or updated after this time must be taken
    # into account at the very end.
    
    my ($start_time) = $dbh->selectrow_array("SELECT now()");
    
    $DB::single = 1;
    
    createTables($dbh) if $step{a};
    computeOriginal($dbh) if $step{b};
    computeSpelling($dbh) if $step{c};
    computeSynonymy($dbh) if $step{d};
    computeHierarchy($dbh) if $step{e};
    computeTreeFields($dbh) if $step{f};
    
    # group together everything related by a child_no/child_spelling_no pair,
    # and look for the best opinion from the set.  That will be the proper
    # opinion for the group, and its child_spelling_no will be the proper
    # spelling_no for the group.
    
    # if status != 'belongs_to' on best opinion, then synonym_no =
    # parent_spelling_no (except for 'misspelling'?)
    
    # all spelling variants should have same spelling_no, synonym_no,
    # opinion_no, lft, and rgt
    
}    

my $NEW_TABLE;		# For debugging purposes, this allows us to properly
                        # zero out the relevant columns if we are repeatedly
                        # re-running various steps in this process.


# createTables ( dbh )
# 
# Create the new tables needed for a taxa tree rebuild.
    
sub createTables {
  
    my ($dbh) = @_;
    
    my ($result);
    
    print STDERR "Rebuild: creating main tables\n";
    $DB::single = 1;
    
    # We start by re-creating all of the temporary tables, dropping the old ones
    # if they still exist.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $TREE_TEMP");
    $result = $dbh->do("CREATE TABLE $TREE_TEMP 
			       (taxon_no int unsigned,
				orig_no int unsigned,
				spelling_no int unsigned,
				synonym_no int unsigned,
				parent_no int unsigned,
				opinion_no int unsigned,
				lft int unsigned,
				rgt int unsigned
				PRIMARY KEY taxon_no,
				KEY orig_no,
				KEY spelling_no,
				KEY synonym_no,
				KEY parent_no,
				KEY opinion_no,
				KEY lft,
				KEY rgt)");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $PARENT_TEMP");
    $result = $dbh->do("CREATE TABLE $PARENT_TEMP
			       (parent_no int unsigned,
				child_no int unsigned,
				KEY parent_no,
				KEY child_no)");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SUPPRESS_TEMP");
    $result = $dbh->do("CREATE TABLE $SUPPRESS_TEMP
			       (opinion_no int unsigned,
				suppress int unsigned
				PRIMARY KEY opinion_no)");
    
    # Create and populare a temporary table to hold opinion data; this is much
    # more efficient than using a view.  This table will be the master from
    # which the taxa_tree tables will be built.
    
    print STDERR "Rebuild: creating opinion table\n";
    
    $result = $dbh->do("DROP TABLE IF EXISTS order_opinions");
    $result = $dbh->do("CREATE TABLE order_opinions
			  (taxon_no int unsigned not null,
			   opinion_no int unsigned not null,
			   pubyr varchar(4),
			   ri int not null,
			   status enum('belongs to','subjective synonym of','objective synonym of','invalid subgroup of','misspelling of','replaced by','nomen dubium','nomen nudum','nomen oblitum','nomen vanum'),
			   spelling_reason enum('original spelling','recombination','reassignment','correction','rank change','misspelling'),
			   child_no int unsigned not null,
			   child_spelling_no int unsigned not null,
			   parent_no int unsigned not null,
			   parent_spelling_no int unsigned not null,
			   KEY (taxon_no), KEY (opinion_no))");
    
    # We first collect the opinion information for all taxa, except for
    # 'belongs to' information about species and subspecies.  Each species
    # must be a child of the genus which makes up the first part of its name,
    # regardless of what the opinions say.  Similarly for subspecies.
    
    # This query is adapated from the old getMostRecentClassification(), from
    # TaxonInfo.pm line 2003.
    
    $result = $dbh->do("LOCK TABLE opinions read, authorities read");
    
    $result = $dbh->do("INSERT INTO order_opinions
		SELECT a.taxon_no, o.opinion_no, 
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
		FROM authorities a
			JOIN opinions o ON o.child_no = a.taxon_no or o.child_spelling_no = a.taxon_no
			LEFT JOIN refs r ON o.reference_no = r.reference_no
		ORDER BY ri DESC, pubyr DESC, opinion_no DESC");
    
    # Now, we need to override 'belongs to' information for subspecies,
    # species and subgenera.  These need to fit into the hierarchy in a manner
    # consistent with their names, regardless of what the opinions say.
    
    print STDERR "Rebuild: creating species table\n";
    
    $result = $dbh->do("DROP TABLE IF EXISTS species_temp");
    $result = $dbh->do("CREATE TABLE species_temp 
				(taxon_no int unsigned,
				 override_no int unsigned,
				 PRIMARY KEY (taxon_no))");
    
    $result = $dbh->do("INSERT INTO species_temp
			SELECT taxon_no, 0 FROM authorities
			WHERE taxon_rank in ('species', 'subspecies', 'subgenus')");
    
    my $update_row = $dbh->prepare("UPDATE species_temp SET override_no = ?
				    WHERE taxon_no = ?");
    
    my $lower_taxa = $dbh->prepare("SELECT taxon_no, taxon_name, taxon_rank
				    FROM authorities
				    WHERE taxon_rank in ('species', 'subspecies',
							 'genus', 'subgenus'");
    
    $lower_taxa->execute();
    
    # We must first fetch all of the entries, because they are in no
    # particular order and we need all of the genera before we can properly
    # process the subgenera, etc.
    
    my (%genus, %subgenus, %species, %subspecies);
    my ($taxon_no, $taxon_name, $taxon_rank, $first_part);
    
    while ( ($taxon_no, $taxon_name, $taxon_rank) = $lower_taxa->fetchrow_array() )
    {
	if ( $taxon_rank eq 'genus' )
	{
	    $genus{$taxon_name} = $taxon_no;
	}
	
	elsif ( $taxon_rank eq 'subgenus' )
	{
	    $subgenus{$taxon_name} = $taxon_no;
	}
	
	elsif ( $taxon_rank eq 'species' )
	{
	    $species{$taxon_name} = $taxon_no;
	}
	
	elsif ( $taxon_rank eq 'subspecies' )
	{
	    $subspecies{$taxon_name} = $taxon_no;
	}
    }
    
    # Now, we go through all the subgenera, species and subspecies and enter
    # the proper parent_no values into species_temp;
    
    foreach $taxon_name (keys %subgenus)
    {
	$taxon_no = $subgenus{$taxon_name};
	
	if ( $taxon_name =~ /^(\w+) \(\w+\)$/ )
	{
	    if ( exists $genus{$1} )
	    {
		$update_row->execute($genus{$1}, $taxon_no);
	    }
	    else
	    {
		print STDERR "Rebuild error: genus '$1' not found for subgenus $taxon_no\n";
	    }
	}
	else
	{
	    print STDERR "Rebuild error: improper name '$taxon_name' for subgenus $taxon_no\n";
	}
    }
    
    foreach $taxon_name (keys %species)
    {
	$taxon_no = $species{$taxon_name};
	
	if ( $taxon_name =~ /^(\w+ \(\w+\)) \w+$/ )
	{
	    if ( exists $subgenus{$1} )
	    {
		$update_row->execute($subgenus{$1}, $taxon_no);
	    }
	    else
	    {
		print STDERR "Rebuild error: subgenus '$1' not found for species $taxon_no\n";
	    }
	}
	elsif ( $taxon_name =~ /^(\w+)\s\w+$/ )
	{
	    if ( exists $genus{$1} )
	    {
		$update_row->execute($genus{$1}, $taxon_no);
	    }
	    else
	    {
		print STDERR "Rebuild error: genus '$1' not found for species $taxon_no\n";
	    }
	}
	else
	{
	    print STDERR "Rebuild error: improper name '$taxon_name' for species $taxon_no\n";
	}
    }
    
    # foreach $taxon_name (keys %subspecies)
    # {
    # 	$taxon_no = $subspecies{$taxon_name};
	
    # 	if ( $taxon_name =~ s/^(\w.*\w) \w+$/ )
    # 	{
    # 	    if ( exists $species{$1} )
    # 	    {
    # 		$update_row->execute($species{$1}, $taxon_no);
    # 	    }
    # 	    else
    # 	    {
    # 		print STDERR "Rebuild error: species '$1' not found for subspecies $taxon_no\n";
    # 	    }
    # 	}
    # 	else
    # 	{
    # 	    print STDERR "Rebuild error: improper name '$taxon_name' for subspecies $taxon_no\n";
    # 	}
    # }
    #
    #$result = $dbh->do("UPDATE order_opinions JOIN species_temp USING (taxon_no)
    #			SET parent_spelling_no = override_no
    #			WHERE status = 'belongs to'");
    
    $result = $dbh->do("UNLOCK TABLES");
    
    # $result = $dbh->do("SELECT taxon_no, taxon_name FROM authorities");
    
    $NEW_TABLE = 1;		# we can stop here when debugging.
}


# computeOriginal ( dbh )
# 
# Fill in the $TREE_TEMP table with $taxon_no and $orig_no.  This computes the
# "original combination" relation.

sub computeOriginal {

    my ($dbh) =  @_;
    
    my ($result);
    
    print STDERR "Rebuild: computing orig_no\n";
    $DB::single = 1;
    
    # The following algorithm is adapted from the old getOriginalCombination()
    # from TaxonInfo.pm, line 2139.
    
    # We start by checking for taxa that are mentioned in some opinion as
    # "child_spelling_no" and set the "original combination" as the
    # "child_no" from that opinion.  If there is more than one such opinion,
    # we take the oldest first, and if there are still conflicts, the one
    # one with the lowest opinion number (the one entered first.)
    
    $result = $dbh->do("INSERT IGNORE INTO $TREE_TEMP (taxon_no, orig_no)
			SELECT j.taxon_no, j.child_no FROM
			(SELECT a.taxon_no, o.child_no,
			       if(o.pubyr is not null and o.pubyr != '', o.pubyr, r.pubyr) as pubyr
			FROM authorities a JOIN opinions o ON a.taxon_no = o.child_spelling_no
					   LEFT JOIN refs r ON o.reference_no = r.reference_no
			ORDER BY pubyr ASC, opinion_no ASC) j");
    
    # Next, for we check for taxa that are mentioned in some opinion as
    # "child_no", but not in any opinion as "child_spelling_no".  These taxa
    # are their own "original combinations".  We filter out those which were
    # caught in the last statement by using INSERT IGNORE.  Since we're
    # looking for all taxa which meet this condition, we don't need to order
    # the result.

    $result = $dbh->do("INSERT IGNORE INTO $TREE_TEMP (taxon_no, orig_no)
			SELECT a.taxon_no, o.child_no
			FROM authorities a JOIN opinions o ON a.taxon_no = o.child_no
				LEFT JOIN $TREE_TEMP t USING (taxon_no)");
    
    # There are still some taxa we have not gotten yet, so we next check for
    # taxa that are mentioned in some opinion as a "parent_spelling_no", and
    # set the original combination to "parent_no" from that opinion.  Again,
    # we need to order by publication year and opinion_no in case there are
    # conflicts.
    
    $result = $dbh->do("INSERT IGNORE INTO $TREE_TEMP (taxon_no, orig_no)
			SELECT j.taxon_no, j.parent_no FROM
			(SELECT a.taxon_no, o.parent_no,
			       if(o.pubyr is not null and o.pubyr != '', o.pubyr, r.pubyr) as pubyr
			FROM authorities a JOIN opinions o ON a.taxon_no = o.parent_spelling_no
				LEFT JOIN $TREE_TEMP t USING (taxon_no)
				LEFT JOIN refs r ON o.reference_no = r.reference_no
			ORDER BY pubyr ASC, o.opinion_no ASC) j");

    # Next, any taxa that are mentioned in some opinion as a "parent_no"
    # but are not otherwise mentioned in any opinion are added as their own
    # "original combinations", similar to step 2.  Again, we don't need to
    # impose any order on the opinions in order to calculate this step.

    $result = $dbh->do("INSERT IGNORE INTO $TREE_TEMP (taxon_no, orig_no)
			SELECT a.taxon_no, o.parent_no
			FROM authorities a JOIN opinions o ON a.taxon_no = o.parent_no
				LEFT JOIN $TREE_TEMP t USING (taxon_no)");
    
    # Every taxon not caught so far is its own "original combination" by
    # default.
    
    $result = $dbh->do("INSERT IGNORE INTO $TREE_TEMP (taxon_no, orig_no)
			SELECT taxon_no, taxon_no
			FROM authorities a LEFT JOIN $TREE_TEMP t USING (taxon_no)
			WHERE t.orig_no is null");
    
    my $a = 1;		# we can stop here when debugging
};


# groupBySpelling ( dbh )
# 
# Fill in the $TREE_TEMP table with spelling_no.  This computes the "taxonomic
# concept" relation.

sub computeSpelling {

    my ($dbh) = @_;
    
    my ($result);
    
    print STDERR "Setting spelling_no\n";
    $DB::single = 1;
    
    # The following table will allow us to select the best opinion for each
    # taxon (most recent and reliable).  If the best opinion is a "nomen",
    # then we use the original spelling.  If it is recorded as a misspelling,
    # we look for the best spelling match among the available opinions.
    # Otherwise, we use the spelling given by that opinion.
    
    $result = $dbh->do("DROP TABLE IF EXISTS spelling_temp");
    $result = $dbh->do("CREATE TABLE spelling_temp
				(taxon_no int unsigned,
				 spelling_no int unsigned,
				 PRIMARY KEY (taxon_no))");
    
    $result = $dbh->do("INSERT IGNORE INTO spelling_temp
			SELECT taxon_no,
			       if(status in ('nomen dubium', 'nomen vanum', 'nomen nudum'),
				  orig_no, child_spelling_no)
			FROM order_opinions o
			WHERE spelling_reason != 'misspelling'
			ORDER BY ri DESC, pubyr DESC, o.opinion_no DESC");
    
    # Fill in the 
    
    $result = $dbh->do("INSERT IGNORE INTO spelling_temp
			SELECT taxon_no, child_no
			FROM order_opinions
			ORDER BY pubyr DESC, opinion_no DESC");
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN spelling_temp s ON s.taxon_no = t.orig_no
			SET t.spelling_no = s.spelling_no");
    
    $result = $dbh->do("UPDATE $TREE_TEMP SET spelling_no = taxon_no WHERE spelling_no = 0");
    
    my $a = 1;		# we can stop on this line when debugging
}


# setSynonymy ( dbh )
# 
# Fill in the proper synonym_no and opinion_no for each entry in the taxa tree
# cache which represents a junior synonym.  All other entries will have their
# synonym_no set equal to their spelling_no.
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
# the synonymy relation is downloaded again and another cycle check is
# performed.  This is necessary because whenever an opinion is suppressed, the
# next-best opinion might cause a new cycle.  Consequently, we repeat this
# process until there are no cycles remaining.
# 
# At that point, we can fill in the synonym_no field in the taxa tree cache,
# and the opinion_no field for junior synonyms.  The opinion_no for senior
# synonyms will be filled in by setBelongsTo.

sub setSynonymy {
    
    my ($dbh) = @_;
    
    my ($result);
    
    print STDERR "Setting synonym_no, opinion_no for synonyms\n";
    $DB::single = 1;
    
    $result = $dbh->do("UPDATE $TREE_TEMP SET synonym_no = 0, opinion_no = 0") unless $NEW_TABLE;
    
    # The following table selects the best opinion for each taxon, along with
    # the data needed to compare them by publication year and reliability.
    
    $result = $dbh->do("DROP TABLE IF EXISTS synonym_temp");
    $result = $dbh->do("CREATE TABLE synonym_temp
				(spelling_no int unsigned,
				 synonym_no int unsigned,
				 ri int unsigned,
				 pubyr int unsigned,
				 opinion_no int unsigned,
				 PRIMARY KEY (spelling_no))");
    
    # The following table will record which opinions must be suppressed in
    # order to avoid cycles in the synonymy and belongs-to relations.
    
    $result = $dbh->do("DROP TABLE IF EXISTS suppress_temp");
    $result = $dbh->do("CREATE TABLE suppress_temp
				(opinion_no int unsigned,
				 suppress boolean,
				 PRIMARY KEY (opinion_no))");
    
    # When an opinion is suppressed, the next most recent and reliable opinion
    # on the given taxon will be used instead.  This might cause a new cycle,
    # so we need to repeat the following procedure until no cycles are detected.
    
    my $cycle_count;
    
    do
    {
	# We start by selecting the best best opinion (most recent and
	# reliable) for each spelling group leader.  This is done by using
	# INSERT IGNORE, with synonym_temp having spelling_no as a primary
	# key.  We need to consider all opinions, including 'belongs to' ones,
	# because we want to know the single best opinion for each taxon.
	
	$result = $dbh->do("
		INSERT IGNORE INTO synonym_temp
		SELECT t.spelling_no, o.parent_spelling_no, o.ri, o.pubyr, o.opinion_no
		FROM order_opinions o LEFT JOIN suppress_temp USING (opinion_no)
			JOIN $TREE_TEMP t ON o.taxon_no = t.spelling_no
		WHERE suppress is null
		ORDER BY ri DESC, pubyr DESC, opinion_no DESC");
	
	# The following statement fetches just the opinions that indicate
	# synonymy rather than the 'belongs to' relation.  The taxon
	# represented by synonym_no is asserted to be the senior synonym, and
	# that represented by spelling_no is asserted to be the junior
	# synonym.
    
	my $synonym_opinions = $dbh->prepare("
		SELECT s.spelling_no, s.synonym_no, s.ri, s.pubyr, opinion_no
		FROM synonym_temp s JOIN opinions o USING (opinion_no)
		WHERE status != 'belongs to' AND synonym_no != spelling_no
			AND synonym_no != 0");
	
	$synonym_opinions->execute();
	
	# The %juniors array lists all of the taxa that are asserted to be
	# junior synonyms of a given taxon.  The %opinions array is indexed by
	# the junior taxon number, and holds the information necessary to
	# determine the most recent and reliable opinion from any given set.
	
	my (%juniors, %opinions);
	
	while ( my ($junior, $senior, $ri, $pub, $op) =
			$synonym_opinions->fetchrow_array() )
	{
	    $juniors{$senior} = [] unless exists $juniors{$senior};
	    push @{$juniors{$senior}}, $junior;
	    
	    $opinions{$junior} = [$senior, $ri, $pub, $op];
	}
	
	# Once we have all of the necessary information, the next step is to
	# go through all of the keys and look for cycles.
	
	$cycle_count = findAndSuppressCycles($dbh, \%juniors, \%opinions);
	
    } until $cycle_count = 0;
    
    # Now that we've broken all of the cycles in synonym_temp, we can use it
    # to set the synonym numbers in table $TREE_TEMP.
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN synonym_temp s USING (spelling_no)
				JOIN opinions o ON o.opinion_no = s.opinion_no
			SET t.synonym_no = s.synonym_no, t.opinion_no = s.opinion_no
			WHERE o.status != 'belongs to'");
    
    # All taxa not set by the previous step should have their synonym_no be
    # the same as their spelling_no
    
    $result = $dbh->do("UPDATE $TREE_TEMP SET synonym_no = spelling_no WHERE synonym_no = 0");
    
    my $a = 1;		# we can stop on this line when debugging
}


# findAndSuppressCycles ( dbh, juniors, opinions )
# 
# Given a record of junior-senior taxon pairs keyed on the senior taxon
# (%juniors) and a listing of opinions keyed on the junior taxon (%opinions),
# locate and eliminate cycles.  This routine is called both by setSynonymy and
# setBelongsTo.
# 
# Cycles are located by taking each key of %juniors in turn and doing a
# breadth-first search.  We prune the search whenever we encounter a node
# we've seen already, and stop whenever we encounter a node that we've seen
# already on *this search*.  In that case, we have a cycle.
# 
# 

sub findAndSuppressCycles {
  
    my ($dbh, $juniors, $opinions) = @_;
    
    my (%seen, $t, $u, $result);
    
 keys:
    foreach $t (keys %$juniors)
    {
	next if $seen{$t};
	
	my @search = ($t);
	my $cycle_found = 0;
	my $cycle_count = 0;
	
	# Look for a cycle starting at $t using breadth-first search.  Note that
	# any given taxon can be part of at most one cycle.
	
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
	    
	    push @search, @{$juniors->{$u}} if exists $juniors->{$u};
	}
	
	# If we have found a cycle, we then need to compare the set of
	# opinions that make it up, and determine which is the best (in
	# other words, the most recent and reliable.)  The *next* opinion
	# in the cycle will need to be suppressed, because it conflicts
	# directly with the best one.
	
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
	    
	    # Now that we've found the best opinion, suppress the next one
	    # in the cycle.
	    
	    my $suppress_taxon = $opinions->{$best}[0];
	    my $suppress_opinion = $opinions->{$suppress_taxon}[3];
	    
	    $result = $dbh->do("INSERT IGNORE INTO suppress_temp
				    VALUES ($suppress_opinion, 1)");
	    
	    # We also need to delete the record for this taxon from
	    # synonym_temp so that the INSERT IGNORE at the top of the
	    # outer loop will insert the new record.
	    
	    $result = $dbh->do("DELETE FROM synonym_temp
				    WHERE spelling_no = $suppress_taxon");
	    
	    $cycle_count++;
	}
	
	# Otherwise, we didn't find a cycle for this instance of $t, so
	# just go on to the next one.
    }
    
    # If we found at least one cycle, repeat the procedure again.  Otherwise,
    # we can move on to the next step.
    
    last if $cycle_count == 0;
}

# setBelongsTo ( dbh, tn )
# 
# Fill in the proper opinion_no for each taxon in table $tn which is not a
# junior synonym (those for which setSynonymy did not set an opinion) and
# which belongs to another taxon.

sub setBelongsTo {
    
    my ($dbh) = @_;
    
    my ($result);
    
    print STDERR "Setting opinion_nos for belongs-to relation\n";
    $DB::single = 1;
    
    $NEW_TABLE = 1;
    
    # First, we need to figure out the best opinion for each taxon.  We use
    # spelling_no since all taxa in a spelling group need to have the same
    # "best opinion".
    
    # $result = $dbh->do("DROP TABLE IF EXISTS opinion_temp");
    # $result = $dbh->do("CREATE TABLE opinion_temp
    # 				(taxon_no int unsigned,
    # 				 opinion_no int unsigned,
    # 				 PRIMARY KEY (taxon_no))");
    
    # $result = $dbh->do("INSERT IGNORE INTO opinion_temp
    # 			SELECT t.spelling_no, o.opinion_no
    # 			FROM order_opinions o JOIN $TREE_TEMP t USING (taxon_no)
    # 			WHERE status != 'misspelling of'
    # 			ORDER BY ri DESC, pubyr DESC, opinion_no DESC");
    
    # $result = $dbh->do("UPDATE $TREE_TEMP t JOIN opinion_temp o ON t.spelling_no = o.taxon_no
    # 			SET t.opinion_no = o.opinion_no");
    
    # One complication is that 'belongs to' opinions might be inherited from
    # junior synonyms.  So for each taxon we have to look for 'belongs to'
    # opinions for it *and its junior synonyms of equal rank* and if we find one
    # then substitute that as the best opinion.
    
    $result = $dbh->do("DROP TABLE IF EXISTS junior_temp");
    $result = $dbh->do("CREATE TABLE junior_temp
				(junior_no int unsigned,
				 senior_no int unsigned,
				 rank int unsigned,
				 primary key (junior_no),
				 key (senior_no))");
    
    $result = $dbh->do("INSERT IGNORE INTO junior_temp
			SELECT spelling_no, spelling_no, 1
			FROM $TREE_TEMP WHERE spelling_no = synonym_no");
    
    $result = $dbh->do("INSERT IGNORE INTO junior_temp
			SELECT t.spelling_no, t.synonym_no, 0
			FROM $TREE_TEMP t JOIN opinions o USING (opinion_no)
			WHERE status in ('subjective synonym of', 'objective synonym of',
						'replaced by')");
    
    $result = $dbh->do("DROP TABLE IF EXISTS belongs_temp");
    $result = $dbh->do("CREATE TABLE belongs_temp
				(spelling_no int unsigned,
				 opinion_no int unsigned,
				 PRIMARY KEY (spelling_no))");
    
    $result = $dbh->do("INSERT IGNORE INTO belongs_temp
			SELECT t.spelling_no, o.opinion_no
			FROM $TREE_TEMP t JOIN junior_temp j ON t.spelling_no = j.senior_no
				JOIN order_opinions o ON o.taxon_no = j.junior_no
			WHERE o.status = 'belongs to'
			ORDER BY j.rank DESC, o.ri DESC, o.pubyr DESC, o.opinion_no DESC");

    # $result = $dbh->do("INSERT IGNORE INTO belongs_temp
    # 			SELECT t.synonym_no, o.opinion_no
    # 			FROM $TREE_TEMP t JOIN order_opinions o ON o.taxon_no = t.spelling_no
    # 				JOIN authorities a1 ON a1.taxon_no = t.spelling_no
    # 				JOIN authorities a2 ON a2.taxon_no = t.synonym_no
    # 				LEFT JOIN opinions o2 ON o.opinion_no = t.opinion_no
    # 			WHERE o.status = 'belongs to' and a1.taxon_rank = a2.taxon_rank
    # 				and (t.spelling_no = t.synonym_no or
    # 					o2.status in ()
    # 			ORDER BY o.ri DESC, o.pubyr DESC, r2 DESC, o.opinion_no DESC");
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN belongs_temp b USING (spelling_no)
			SET t.opinion_no = b.opinion_no
			WHERE t.opinion_no = 0");
    
    my $a = 1;
}


my (%children, %parent, %tree);

# setTreeFields ( dbh )
# 
# Arrange the rows of taxa_tree_cache into a Nested Set tree by filling in the
# lft, rgt and depth fields.  For more information, see:
# http://en.wikipedia.org/wiki/Nested_set_model.

sub setTreeFields {
    
    my ($dbh) = @_;
    
    my ($parent_no, $child_no);
    
    print STDERR "Seting lft, rgt, depth\n";
    $DB::single = 1;
    
    $dbh->do("UPDATE $TREE_TEMP SET lft=0, rgt=0, depth=0") unless $NEW_TABLE;
    
    my $sth = $dbh->prepare("
		SELECT t2.spelling_no as parent_no, t1.spelling_no as child_no
		FROM $TREE_TEMP t1 JOIN opinions USING (opinion_no)
			JOIN $TREE_TEMP t2 ON t2.taxon_no = parent_spelling_no
		WHERE t1.taxon_no = t1.spelling_no and t1.taxon_no != parent_spelling_no");
    
    $sth->execute();
    
    print STDERR "A\n";
    
    while ( ($parent_no, $child_no) = $sth->fetchrow_array() )
    {
	$children{$parent_no} = [] unless defined $children{$parent_no};
	push @{$children{$parent_no}}, $child_no;
	print STDERR "ERROR: $child_no has multiple parents: $parent{$child_no}, $parent_no\n"
	    if exists $parent{$child_no};
	$parent{$child_no} = $parent_no;
    }
    
    print STDERR "B\n";
    
    # Now we build the tree, starting with taxon 1 'Eukaryota' at the top of
    # the tree with lft=1 and depth=1.
    
    my $lft = computeTreeValues(1, 1, 1, 0);
    
    # Next, we go through all of the other taxa and insert the ones that
    # haven't yet been inserted.  This is necessary because many taxa have no
    # known parents.
    
    print STDERR "C\n";
    
 taxon:
    foreach my $taxon (keys %children)
    {
	next if exists $tree{$taxon};
	
	my %seen;
	my $t = $taxon;
	
	while ( defined $parent{$t} )
	{
	    $seen{$t} = 1;
	    if ( $seen{$parent{$t}} )
	    {
		print STDOUT "Tree violation: $t => $parent{$t}\n";
		next taxon;
	    }
	    $t = $parent{$t};
	}
	
	$lft = computeTreeValues($taxon, $lft+1, 1, 0);
    }
    
    print STDERR "D\n";
    
    # Now we need to actually insert all of those values back into the
    # database. 
    
    my $sth = $dbh->prepare("UPDATE $TREE_TEMP SET lft=?, rgt=?, depth=? WHERE taxon_no=?");
    
    foreach my $taxon (keys %tree)
    {
	$sth->execute(@{$tree{$taxon}}, $taxon)
    }
    
    print STDERR "E\n";
    
    # Finally, we need to set lft, rgt and depth for all taxa which are not
    # spelling group leaders.
    
    my $result = $dbh->do("
		UPDATE $TREE_TEMP t1 JOIN $TREE_TEMP t2 ON t2.taxon_no = t1.spelling_no
		SET t1.lft = t2.lft, t1.rgt = t2.rgt, t1.depth = t2.depth");
    
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



1;
