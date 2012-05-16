# 
# The Paleobiology Database
# 
#   RebuildTaxonTrees.pm
# 


package TaxonTrees;

use strict;

# rebuild ( dbh, debug )
# 
# Completely rebuild the taxa tree from the C<opinions> and C<authorities>
# tables.  This is safe to run while the database is in active use.  If 'debug'
# is specified, it must be a hash reference indicating which of the steps in the
# rebuilding process to carry out.
# 
# In order to be safe to run while the database is in use, we operate on
# temporary tables which are then atomically swapped for the actual ones via a
# single RENAME TABLE operation.  While this has been going on, authorities
# and opinions may have been added or modified.  Therefore, we must keep track
# of which ones those are, and at the very end of the procedure we must alter
# the taxa tables to reflect the changed opinions.

# For debugging purposes only, we accept a second parameter %step to indicate
# which of the steps should be carried out.
# 
# Note that the steps taken in this routine must be carried out in the order
# given, for the following reasons:
# 
#   - group leader must be computed first, because senior_no and parent_no
#     always point to group leaders.
# 
#   - synonymy must be computed next, because the hierarchy is defined 
#     based on the best opinion for any taxon and its junior synonyms 
#     considered together
# 
#   - hierarchy must be computed next, because we use the hierarchy relation
#     to organize and sequence the taxon trees.
# 
#   - tree sequence must be computed next, because the ancestry table uses it
#     for proper ordering of ancestors.
#   
#   - once we have computed the hierarchy, sequenced the trees, and computed
#     the ancestor relation, we can swap the new tables for the current ones
#     in an atomic operation.

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
    computeSpelling($dbh) if $step->{b};
    computeSynonymy($dbh) if $step->{c};
    computeHierarchy($dbh) if $step->{d};
    computeTreeSequence($dbh) if $step->{e};
    computeAncestry($dbh) if $step->{f};
    activateNewTables($dbh, $step->{k}) if $step->{g};
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
    
    print STDERR "Rebuild: creating temporary tables (a)\n";
    $DB::single = 1;
    
    # We start by re-creating all of the temporary tables, dropping the old
    # ones if they still exist.  $TREE_TEMP is populated with one row for each
    # distinct orig_no value in authorities.
    
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
				PRIMARY KEY (taxon_no))");
    
    $result = $dbh->do("INSERT INTO $TREE_TEMP
			SELECT distinct orig_no, 0, 0, 0, 0
			FROM authorities");
    
    $result = $dbh->do("ALTER TABLE $TREE_TEMP ADD PRIMARY KEY (orig_no)");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $ANCESTOR_TEMP");
    $result = $dbh->do("CREATE TABLE $ANCESTOR_TEMP
			       (child_no int unsigned not null,
				parent_no int unsigned not null,
				depth int unsigned not null,
				KEY (child_no))");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SUPPRESS_TEMP");
    $result = $dbh->do("CREATE TABLE $SUPPRESS_TEMP
			       (opinion_no int unsigned not null,
				suppress int unsigned not null,
				PRIMARY KEY (opinion_no))");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $OPINION_TEMP");
    $result = $dbh->do("CREATE TABLE $OPINION_TEMP
			  (orig_no int unsigned not null,
			   opinion_no int unsigned not null,
			   ri int not null,
			   pubyr varchar(4),
			   status enum('belongs to','subjective synonym of','objective synonym of','invalid subgroup of','misspelling of','replaced by','nomen dubium','nomen nudum','nomen oblitum','nomen vanum'),
			   spelling_reason enum('original spelling','recombination','reassignment','correction','rank change','misspelling'),
			   child_spelling_no int unsigned not null,
			   parent_no int unsigned not null,
			   parent_spelling_no int unsigned not null)");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $BEST_TEMP");
    $result = $dbh->do("CREATE TABLE $BEST_TEMP
			    (orig_no int unsigned,
			     parent_no int unsigned,
			     opinion_no int unsigned,
			     ri int not null,
			     pubyr varchar(4),
			     status enum('belongs to','subjective synonym of','objective synonym of','invalid subgroup of','misspelling of','replaced by','nomen dubium','nomen nudum','nomen oblitum','nomen vanum'),
			     PRIMARY KEY (orig_no))");
    
    # Now we populate $OPINION_TEMP with opinion data in the proper order.
    # This is much more efficient than using a view (verified by
    # experimentation using MySQL 5.1).  This table will be used as an auxiliary
    # in computing the four taxon relations.
    
    print STDERR "Rebuild:     populating opinion table\n";
    
    $result = $dbh->do("LOCK TABLE opinions as o read, refs as r read, $OPINION_TEMP write");
    
    $result = $dbh->do("INSERT INTO $OPINION_TEMP
		SELECT a1.orig_no, o.opinion_no, 
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
			o.status, o.spelling_reason, a2.orig_no, o.child_spelling_no,
			o.parent_spelling_no
		FROM opinions o LEFT JOIN refs r USING (reference_no)
			JOIN authorities a1 ON a1.taxon_no = o.child_spelling_no
			JOIN authorities a2 ON a2.taxon_no = o.parent_spelling_no
		ORDER BY ri DESC, pubyr DESC, opinion_no DESC");
    
    $result = $dbh->do("UNLOCK TABLES");
    
    # Once we have populated $OPINION_TEMP, we can efficiently index it.
    
    print STDERR "Rebuild:     indexing opinion table\n";
    
    $result = $dbh->do("ALTER TABLE $OPINION_TEMP add primary key (opinion_no)");
    $result = $dbh->do("ALTER TABLE $OPINION_TEMP add key (orig_no)");
    $result = $dbh->do("ALTER TABLE $OPINION_TEMP add key (parent_no)");
    
    $NEW_TABLE = 1;		# we can stop here when debugging.
}


# computeSynonymy ( dbh )
# 
# Fill in the synonym_no field of $TREE_TEMP.  This computes the "synonymy"
# relation.  We also fill in the opinion_no field for each entry that
# represents a junior synonym.
# 
# Before setting the synonym numbers, we need to download the entire synonymy
# relation (the set of junior/senior synonym pairs) and look for cycles.
# Unfortunately, the database does contain some of these.  For example, in
# some opinion, A. might assert that "x is a junior synonym of y" while in
# another opinion B. asserts "y is a junior synonym of x".  Other cycles might
# involve 3 or more different taxa.  Whenever we find such a cycle, we must
# compare the relevant opinions to find the most recent and reliable one.
# Whichever taxon that opinion asserts to be the senior synonym will be taken
# to be so; the next opinion in the cycle, which asserts that this taxon is a
# junior synonym, will be suppressed.
# 
# The suppressed opinions are recorded in the table $SUPPRESS_TEMP.  Once all
# cycles are identified and the appropriate records added to $SUPPRESS_TEMP,
# a followup cycle check is performed.  This is necessary because whenever an
# opinion is suppressed, the next-best opinion might cause a new cycle.
# Consequently, we repeat this process until there are no cycles remaining.
# 
# At that point, we can fill in the synonym_no field, and the opinion_no field
# for junior synonyms.  The opinion_no for senior synonyms and for taxa which
# are not synonyms will be filled in by computeHierarchy.

sub computeSynonymy {
    
    my ($dbh) = @_;
    
    my ($result, $filter, @check_taxa, %taxa_moved);
    
    print STDERR "Rebuild: computing synonymy relation (d)\n";
    $DB::single = 1;
    
    $result = $dbh->do("UPDATE $TREE_TEMP SET synonym_no = 0, opinion_no = 0") unless $NEW_TABLE;
    
    # We start by computing the best opinion (most recent and reliable) for
    # each taxonomic concept group (identified by orig_no) so that we can then
    # fetch the subset of those opinions that indicate synonymy.  This is
    # selected using slightly different criteria than we used above in
    # selecting the best spellings, so we need a separate table for it.
    
    # We use the same mechanism to select the best opinion for each group as
    # we did previously with the spelling opinions.  The table has orig_no as
    # a unique key, and we INSERT IGNORE with a selection that is already
    # properly ordered.
    
    # We ignore opinions whose status is 'misspelling of', because they should
    # affect the choice of concept group leader only and not synonymy or
    # hierarchy.  We also ignore any opinion where child_no = parent_no or
    # child_spelling_no = parent_no, since we are only concerned here with
    # relationships between different taxonomic concept groups.
    
    $result = $dbh->do("INSERT IGNORE INTO $BEST_TEMP
			SELECT orig_no, parent_no, opinion_no, ri, pubyr, status
			FROM $OPINION_TEMP
			WHERE status != 'misspelling of' and child_no != parent_no
				and child_spelling_no != parent_no");
    
    # Now we download the subset of these "best opinions" which indicate synonymy.
    
    print STDERR "Rebuild:     downloading synonymy opinions\n";
    
    my $synonym_opinions = $dbh->prepare("
		SELECT orig_no, parent_no, opinion_no, ri, pubyr
		FROM $BEST_TEMP
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
    
    print STDERR "Rebuild:         found " . scalar(@breaks) . " cycle(s) \n";
    
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
	
	# Next, we update the $BEST_TEMP table by deleting the suppressed
	# opinions, then replacing them with the next-best opinion.  We need
	# to delete first, because there may be no next-best opinion!  (Also
	# because that way we can use INSERT IGNORE again to pick the single
	# best opinion for each orig_no as above).
	
	if ( @check_taxa )
	{
	    $filter = '(' . join(',', @check_taxa) . ')';
	    
	    $result = $dbh->do("DELETE FROM $BEST_TEMP WHERE orig_no in $filter");
	    
	    $result = $dbh->do("
		INSERT IGNORE INTO $BEST_TEMP
		SELECT orig_no, parent_no, opinion_no, ri, pubyr, status
		FROM $OPINION_TEMP LEFT JOIN $SUPPRESS_TEMP USING (opinion_no)
		WHERE suppress is null and orig_no in $filter
			and status != 'misspelling of' AND child_no != parent_no
				AND child_spelling_no != parent_no");
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
	
	print STDERR "Rebuild:         found " . scalar(@breaks) . " cycle(s) \n";
    }
    
    # Now that we've broken all of the cycles in $BEST_TEMP, we can use it
    # to set the synonym numbers in $TREE_TEMP.  We have to join with a second
    # copy of $TREE_TEMP to look up the spelling_no for each senior synonym,
    # because the synonym_no should always point to a spelling group leader no
    # matter what spelling the relevant opinion uses.
    
    print STDERR "Rebuild:     setting synonym_no, opinion_no\n";
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN $BEST_TEMP b USING (orig_no)
				JOIN $TREE_TEMP t2 ON b.parent_no = t2.orig_no
			SET t.synonym_no = t2.spelling_no, t.opinion_no = b.opinion_no
			WHERE status != 'belongs to'");
    
    # Taxa which are not junior synonyms of other taxa are their own best
    # synonym.  So, for each row in which synonym_no = 0, we set it to
    # spelling_no.
    
    $result = $dbh->do("UPDATE $TREE_TEMP SET synonym_no = spelling_no WHERE synonym_no = 0");
    
    # Now that we have set synonym_no for all taxa, we can efficiently index it.
    
    print STDERR "Rebuild:     indexing synonym_no\n";
    
    $result = $dbh->do("ALTER TABLE $TREE_TEMP add index (synonym_no)");
    
    # So far we have computed the immediate senior synonym for each taxon, but
    # what we want is the *most senior* synonym for each taxon.  Thus, we need
    # to look for instances where a -> b and b -> c, and change the relation
    # so that a -> c and b -> c.  Because the chains may be more than three
    # taxa long, we need to repeat the following process until no more rows
    # are affected, with a limit of 20 to avoid an infinite loop just in case
    # our algorithm above was faulty and some cycles have slipped through.
    
    print STDERR "Rebuild:     collapsing synonym chains\n";
    
    my $count = 0;
    
    do
    {
	$result = $dbh->do("
		UPDATE $TREE_TEMP t JOIN authorities a ON a.taxon_no = t.synonym_no
			JOIN $TREE_TEMP t2 ON a.orig_no = t2.orig_no and t.synonym_no != t2.synonym_no
		SET t.synonym_no = t2.synonym_no");
    }
	while $result > 0 && ++$count < 20;
    
    if ( $count >= 20 )
    {
	print STDERR "Rebuild:     WARNING - possible synonymy cycle detected during de-chaining";
    }
    
    my $a = 1;		# we can stop on this line when debugging
}


# computeHierarchy ( dbh )
# 
# Fill in the parent_no field of $TREE_TEMP.  The parent_no for each taxon
# which is not itself a junior synonym is computed by determining the best
# (most recent and reliable) opinion from among those for the taxon and all
# its junior synonyms.  The parent_no for a junior synonym will always be the
# same as for its most senior synonym.
# 
# This routine also fills in the opinion_no field for all entries which have
# opinions and are not junior synonyms (the junior synonyms were already given
# an opinion_no by the computeSynonymy routine).  After this is done, the only
# entries left which have an opinion_no of 0 will be those for which no
# opinion is found in the database.
# 
# Just as with computeSynonymy above, we must also perform a cycle check.  If
# we find any cycles, we break them by adding records to the $SUPPRESS_TEMP
# table, the same one used by computeSynonymy.  The check is then repeated in
# case new cycles have been created by the newly selected opinions.

sub computeHierarchy {
    
    my ($dbh) = @_;
    
    my ($result, $filter, @check_taxa, %taxa_moved);
    
    print STDERR "Rebuild: computing hierarchy relation (e)\n";
    $DB::single = 1;
    
    $dbh->do("UPDATE $TREE_TEMP SET parent_no = 0") unless $NEW_TABLE;
    
    # We already have the $BEST_TEMP relation, but some of the values in it
    # may be wrong because the best opinion for a senior synonym might
    # actually be associated with one of its junior synonyms.  So we need to
    # update the relation to take this into account.
    
    # The first step is to compute an auxiliary relation that associates the
    # spelling_no of each senior synonym with the the spelling_no values of
    # itself and all of its junior synonyms.  We will use the 'is_senior'
    # field to make sure that an opinion on a senior synonym is considered
    # before an opinion on a junior one, in case of ties in reliability index
    # and publication year.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $JUNIOR_TEMP");
    $result = $dbh->do("CREATE TABLE $JUNIOR_TEMP
				(junior_no int unsigned,
				 senior_no int unsigned,
				 is_senior int unsigned,
				 primary key (junior_no),
				 key (senior_no))");
    
    # We start by adding all senior synonyms into the table, with is_senior=1
    
    $result = $dbh->do("INSERT IGNORE INTO $JUNIOR_TEMP
			SELECT orig_no, orig_no, 1
			FROM $TREE_TEMP WHERE spelling_no = synonym_no");
    
    # Then, we add all immediately junior synonyms, but only subjective and
    # objective synonyms and replaced taxa.  We leave out nomina dubia, nomina
    # vana, nomina nuda, nomina oblita, and invalid subgroups, because an
    # opinion on any of those shouldn't affect the senior taxon.  All of these
    # entries have is_senior=0
    
    $result = $dbh->do("INSERT IGNORE INTO $JUNIOR_TEMP
			SELECT t.orig_no, o.parent_no, 0
			FROM $TREE_TEMP t JOIN $OPINION_TEMP o USING (opinion_no)
			WHERE status in ('subjective synonym of', 'objective synonym of',
						'replaced by')");
    
    # Now we can select the best 'belongs to' opinion for each senior synonym
    # from the opinions pertaining to it and its junior synonyms. We use a
    # temporary table with orig_no as a unique key and then use INSERT IGNORE
    # with an appropriately ordered set to select the best opinion for each
    # orig_no.  Once the best opinions are computed, we replace the belongs-to
    # entries from $BEST_TEMP (i.e. the entries representing senior synonyms)
    # with the newly computed ones while leaving the rest of the entries
    # (those that represent junior synonyms) alone.  Note that we ignore all
    # opinions where parent_no is the same as child_no or child_spelling_no,
    # because we are only interested in opinions which indicate relationships
    # between concept groups.
    
    print STDERR "Rebuild:     selecting best opinions\n";
    
    $result = $dbh->do("DROP TABLE IF EXISTS $BELONGS_TEMP");
    $result = $dbh->do("CREATE TABLE $BELONGS_TEMP
				(orig_no int unsigned,
				 opinion_no int unsigned,
				 PRIMARY KEY (orig_no))");
    
    $result = $dbh->do("INSERT IGNORE INTO $BELONGS_TEMP
			SELECT t.orig_no, o.opinion_no
			FROM $TREE_TEMP t JOIN $JUNIOR_TEMP j ON t.orig_no = j.senior_no
				JOIN $OPINION_TEMP o ON o.orig_no = j.junior_no
			WHERE o.status = 'belongs to' and t.orig_no != o.parent_no
				and o.child_no != o.parent_no and o.child_spelling_no != o.parent_no
			ORDER BY o.ri DESC, o.pubyr DESC, j.is_senior DESC, o.opinion_no DESC");
    
    $result = $dbh->do("UPDATE $BEST_TEMP b JOIN $BELONGS_TEMP x using (orig_no)
				JOIN $OPINION_TEMP o ON o.opinion_no = x.opinion_no
			SET b.opinion_no = o.opinion_no, b.ri = o.ri, b.pubyr = o.pubyr,
				b.parent_no = o.parent_no
			WHERE b.status = 'belongs to' and b.opinion_no != o.opinion_no");
    
    # Next, we download the entire set of "best opinions" so that we can look
    # for cycles (ignoring those with a parent_no of 0, because they obviously
    # cannot participate in a cycle).
    
    print STDERR "Rebuild:     downloading hierarchy opinions\n";
    
    my $best_opinions = $dbh->prepare("
		SELECT orig_no, parent_no, ri, pubyr, opinion_no
		FROM $BEST_TEMP WHERE parent_no > 0");
    
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
	    print STDERR "Rebuild: WARNING - best opinion for $child has same parent (opinion $op)\n";
	    next;
	}

	$children{$parent} = [] unless exists $children{$parent};
	push @{$children{$parent}}, $child;
	
	$opinions{$child} = [$parent, $ri, $pub, $op];
    }
    
    print STDERR "Rebuild:     checking for cycles...\n";
    
    my @breaks = breakCycles($dbh, \%children, \%opinions);
    
    print STDERR "Rebuild:         found " . scalar(@breaks) . " cycle(s) \n";
    
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
	
	# We also have to clean up the $BEST_TEMP table, so that we can
	# compute the next-best opinion for each suppressed one.  So we delete
	# the suppressed opinions, and replace them with the next-best
	# opinion.  We need to delete first, because there may be no next-best
	# opinion!
	
	if ( @check_taxa )
	{
	    $filter = '(' . join(',', @check_taxa) . ')';
	    
	    $result = $dbh->do("DELETE FROM $BEST_TEMP WHERE orig_no in $filter");
	    
	    $result = $dbh->do("
		INSERT IGNORE INTO $BEST_TEMP
		SELECT j.senior_no, o.parent_no, o.opinion_no, o.ri, o.pubyr, o.status
		FROM $OPINION_TEMP o JOIN $JUNIOR_TEMP j ON o.orig_no = j.junior_no
			LEFT JOIN $SUPPRESS_TEMP USING (opinion_no)
		WHERE suppress is null and orig_no in $filter
			and o.status = 'belongs to' and j.senior_no != o.parent_no
			and o.child_no != o.parent_no and o.child_spelling_no != o.parent_no
			ORDER BY o.ri DESC, o.pubyr DESC, j.is_senior DESC, o.opinion_no DESC");
	}
	
	# In order to repeat the cycle check, we need to grab these new
	# opinions.
	
	my $belongs_opinions = $dbh->prepare("
		SELECT orig_no, parent_no, ri, pubyr, opinion_no
		FROM $BEST_TEMP WHERE parent_no > 0 and orig_no in $filter");
	
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
	
	print STDERR "Rebuild:         found " . scalar(@breaks) . " cycle(s) \n";
    }
    
    # Now that we have eliminated all cycles, we can set the opinion_no field
    # for everything that hasn't been set already (i.e. all taxa that are not
    # junior synonyms).
    
    print STDERR "Rebuild:     setting opinion_no\n";
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN $BEST_TEMP b USING (orig_no)
			SET t.opinion_no = b.opinion_no
			WHERE t.opinion_no = 0");
    
    # Then, we set the parent_no for all taxa.  All taxa in a synonym group
    # will share the same parent, and the parent must point to a senior
    # synonym.  So, we start with $TREE_TEMP, look up the synonym_no, use a
    # second copy of $TREE_TEMP to look up the opinion_no corresponding to
    # that taxon, look up the parent_no stated by that opinion, and join with
    # a third copy of $TREE_TEMP to look up the synonym_no of that taxon.
    # 
    # In othe words, the parent_no value for any taxon will be the senior
    # synonym of the parent of the senior synonym.
    
    print STDERR "Rebuild:     setting parent_no\n";
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN $TREE_TEMP t2 ON t2.taxon_no = t.synonym_no
				JOIN $OPINION_TEMP o ON o.opinion_no = t2.opinion_no
				JOIN $TREE_TEMP t3 ON t3.taxon_no = o.parent_no
			SET t.parent_no = t3.synonym_no");
    
    # Now that we have set parent_no for all taxa, we can efficiently index
    # it. 
    
    print STDERR "Rebuild:     indexing parent_no\n";
    
    $result = $dbh->do("ALTER TABLE $TREE_TEMP add index (parent_no)");
    
    print STDERR "Rebuild:     indexing opinion_no\n";
    
    $result = $dbh->do("ALTER TABLE $TREE_TEMP add index (opinion_no)");
    
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


our (%children, %parent, %tree);

# computeTreeSequence ( dbh )
# 
# Fill in the lft, rgt and depth fields of $TREE_TMP.  This has the effect of
# arranging the rows into a forest of Nested Set trees.  For more information,
# see: http://en.wikipedia.org/wiki/Nested_set_model.

sub computeTreeSequence {
    
    my ($dbh) = @_;
    
    my $result;
    
    print STDERR "Rebuild: traversing and marking taxon trees (f)\n";
    $DB::single = 1;
    
    $dbh->do("UPDATE $TREE_TEMP SET lft=NULL, rgt=NULL, depth=NULL") unless $NEW_TABLE;
    
    print STDERR "Rebuild:     downloading hierarchy relation\n";
    
    my $pc_pairs = $dbh->prepare("SELECT spelling_no, parent_no
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
    
    # Now we create the "main" tree, starting with taxon 1 'Eukaryota' at the
    # top of the tree with sequence=1 and depth=1.  The variable $sequence
    # gets the maximum sequence number from the newly created tree.  Thus, the
    # next tree (created below) uses $sequence+1 as its sequence number.
    
    print STDERR "Rebuild:     traversing tree rooted at 'Eukaryota'\n";
    
    my $sequence = createNode(1, 1, 1);
    
    # Next, we go through all of the other taxa.  When we find a taxon with no
    # parent that we haven't visited yet, we create a new tree with it as the
    # root.  This takes care of all the taxon for which their relationship to
    # the main tree is not known.
    
    print STDERR "Rebuild:     traversing all other taxon trees\n";
    
 taxon:
    foreach my $taxon (keys %children)
    {
	next if exists $parent{$taxon};		# skip any taxa that aren't roots
	next if exists $tree{$taxon};		# skip any that we've already inserted
	
	$sequence = createNode($taxon, $sequence+1, 1);
    }
    
    # Now we need to upload all of the tree sequence data to the server so
    # that we can set lft, rgt, and depth in $TREE_TEMP.  To do this
    # efficiently, we use an auxiliary table and a large insert statement.
    
    print STDERR "Rebuild:     uploading tree sequence data\n";
    
    $dbh->do("DROP TABLE IF EXISTS tree_insert");
    $dbh->do("CREATE TABLE tree_insert
     		       (spelling_no int unsigned,
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
    
    print STDERR "Rebuild:     setting lft, rgt, depth\n";
    
    $result = $dbh->do("ALTER TABLE tree_insert ADD INDEX (spelling_no)");
    
    $result = $dbh->do("UPDATE $TREE_TEMP t JOIN tree_insert i USING (spelling_no)
			SET t.lft = i.lft, t.rgt = i.rgt, t.depth = i.depth");
    
    $result = $dbh->do("DROP TABLE IF EXISTS tree_insert");
    
    # Now we can efficiently index $TREE_TEMP on lft, rgt and depth.
    
    print STDERR "Rebuild:     indexing lft, rgt, depth\n";
    
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
	print STDERR "Rebuild: WARNING - tree cycle for taxon $taxon\n";
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
    
    print STDERR "Rebuild: computing ancestry relation (g)\n";
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
			SELECT spelling_no, parent_no, depth - 1
			FROM $TREE_TEMP
			WHERE taxon_no = spelling_no AND parent_no > 0");
    
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
	print STDERR "Rebuild:     depth $i\n";
	$result = $add_conjugation->execute($i);
	$result = $add_slice->execute($i);
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
    
    print STDERR "Rebuild: activating new tables (h)\n";
    $DB::single = 1;
    
    # Delete any backup tables that might still be around
    
    $result = $dbh->do("DROP TABLE IF EXISTS $TREE_BAK");
    $result = $dbh->do("DROP TABLE IF EXISTS $ANCESTOR_BAK");
    $result = $dbh->do("DROP TABLE IF EXISTS $OPINION_BAK");
    $result = $dbh->do("DROP TABLE IF EXISTS $SUPPRESS_BAK");
    
    # Create dummy versions of any of the main tables that might be currently
    # missing
    
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $TREE_TABLE LIKE $TREE_TEMP");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $ANCESTOR_TABLE LIKE $ANCESTOR_TEMP");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $OPINION_TABLE LIKE $OPINION_TEMP");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $SUPPRESS_TABLE LIKE $SUPPRESS_TEMP");

    
    # Now do the Atomic Table Swap (tm)
    
    $result = $dbh->do("RENAME TABLE
				$TREE_TABLE to $TREE_BAK,
				$TREE_TEMP to $TREE_TABLE,
				$ANCESTOR_TABLE to $ANCESTOR_BAK,
				$ANCESTOR_TEMP to $ANCESTOR_TABLE,
				$OPINION_TABLE to $OPINION_BAK,
				$OPINION_TEMP to $OPINION_TABLE,
				$SUPPRESS_TABLE to $SUPPRESS_BAK,
				$SUPPRESS_TEMP to $SUPPRESS_TABLE");
    
    # Then we can get rid of the backup tables
    
    $result = $dbh->do("DROP TABLE $TREE_BAK");
    $result = $dbh->do("DROP TABLE $ANCESTOR_BAK");
    $result = $dbh->do("DROP TABLE $OPINION_BAK");
    $result = $dbh->do("DROP TABLE $SUPPRESS_BAK");
    
    # Delete the auxiliary tables too, unless we were told to keep them.
    
    unless ( $keep_temps )
    {
	print STDERR "Rebuild: removing temporary tables\n";
	
	$result = $dbh->do("DROP TABLE IF EXISTS $SPELLING_TEMP");
	$result = $dbh->do("DROP TABLE IF EXISTS $BEST_TEMP");
	$result = $dbh->do("DROP TABLE IF EXISTS $JUNIOR_TEMP");
	$result = $dbh->do("DROP TABLE IF EXISTS $BELONGS_TEMP");
	$result = $dbh->do("DROP TABLE IF EXISTS $PARENT_TEMP");
    }
    
    print STDERR "Rebuild: done rebuilding taxon trees\n";
    
    my $a = 1;		# we can stop here when debugging
}


1;
