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

=item build ( dbh )

Build the taxon tree tables from scratch using the data in C<authorities> and
C<opinions>.  Although this is probably not absolutely necessary if
updateTaxonTrees() has been called consistently whenever the C<authorities>
and C<opinions> tables are changed, the author of this module suggests that it
be run on a daily basis at a convenient time when traffic on the database is
light.

This routine actually adds an entry to the taxon_queue table and waits for the
maintenance thread to do the build.  It returns the id number of the newly
added queue entry, which can be passed to the status() routine to determine
when that particular update has been completed.

=cut

package TaxonTrees;

# Controls for debug messages

our $MSG_TAG = 'unknown';
our $MSG_LEVEL = 0;

# Main table names

our $TREE_TABLE = "taxon_trees";
our $SUPPRESS_TABLE = "suppress_opinions";
our $OPINION_CACHE = "order_opinions";

our $QUEUE_TABLE = "taxon_queue";

our $AUTH_TABLE = "authorities";
our $OPINIONS_TABLE = "opinions";
our $REFS_TABLE = "refs";

# Backup names for use with RENAME TABLES

our $TREE_BAK = "taxon_trees_bak";
our $SUPPRESS_BAK = "suppress_opinions_bak";
our $OPINION_BAK = "order_opinions_bak";

# Working table names

our $TREE_TEMP = "tn";
our $SUPPRESS_TEMP = "sn";
our $OPINION_TEMP = "opn";

# Auxiliary table names

our $SPELLING_TEMP = "spelling_aux";
our $SYNONYM_TEMP = "synonym_aux";
our $CLASS_TEMP = "class_aux";
our $CHECK_TEMP = "check_aux";

# Modules needed

use Text::JaroWinkler qw( strcmp95 );

use strict;


# Variables and constants

our (@TREE_ERRORS);
my ($REPORT_THRESHOLD) = 20;

# This is used for proper signal handling

my ($ABORT) = 0;

# Constants used with $QUEUE_TABLE

use constant TYPE_CONCEPT => 1;
use constant TYPE_OPINION => 2;
use constant TYPE_REBUILD => 3;
use constant TYPE_CHECK => 4;
use constant TYPE_ERROR => 9;

use constant UPDATE_PENDING => 1;
use constant UPDATE_COMPLETE => 2;
use constant UPDATE_ERROR => 4;
use constant UPDATE_INVALID => 9;


=item updateConcepts ( dbh, orig_no ... )

Adjusts the taxon tree tables to take into account changes to the given
concepts.  Some of the listed concepts may disappear from the taxon tables, as
may happen if two concepts are merged, while others may need to be
created.  This routine should be called whenever C<orig_no> values in the
C<authorities> table are adjusted, and both the old and the new C<orig_no> values
should be included in the argument list.

This routine actually adds an entry to C<$QUEUE_TABLE> and waits for the
maintenance thread to do the update.  It returns the id number of the newly
added queue entry, which can be passed to the status() routine to determine
when that particular update has been completed.

=cut

sub updateConcepts {
    
    my ($dbh, @concepts) = @_;
    
    # Insert into $QUEUE_TABLE one row for each modified taxonomic concept.
    # Ignore any concepts that are not integers greater than zero.
    
    my $concept_op = TYPE_CONCEPT;
    my $insert_sql = "INSERT INTO $QUEUE_TABLE (type, param, time) VALUES ";
    my $comma = '';
    
    foreach my $orig_no (@concepts)
    {
	next unless $orig_no > 0;
	$insert_sql .= "$comma($concept_op, " . $orig_no + 0 . ', now())';
	$comma = ', ';
    }
    
    # If we didn't get any valid concepts, return false.
    
    return unless $comma;
    
    # Now do the insert.
    
    $dbh->do($insert_sql);
    
    # Return the id of the last inserted row.  This can then be passed to
    # status() to determine whether the requested update has been completed.
    # We only care about the last inserted row, since they will be processed in
    # order and under most circumstances are processed together as a group.  Once
    # the last one has been completed, we know that all of the others have.
    
    return $dbh->last_insert_id();
}


=item updateOpinions ( dbh, opinion_no ... )

Adjusts the taxon tree tables to take into account changes to the given
opinions.  Some of the listed opinions may disappear from the taxon tables, as
may happen if an opinion is deleted, while others may need to be created.
This routine should be called whenever the C<opinions> table is modified.

This routine actually adds an entry to C<$QUEUE_TABLE> and waits for the
maintenance thread to do the update.  It returns the id number of the newly
added queue entry, which can be passed to the status() routine to determine
when that particular update has been completed.

=cut

sub updateOpinions {
    
    my ($dbh, @opinions) = @_;
    
    # Insert into $QUEUE_TABLE one row for each modified opinion.  Ignore any
    # values that are not integers greater than zero.
    
    my $concept_op = TYPE_OPINION;
    my $insert_sql = "INSERT INTO $QUEUE_TABLE (type, param, time) VALUES ";
    my $comma = '';
    
    foreach my $opinion_no (@opinions)
    {
	next unless $opinion_no > 0;
	$insert_sql .= "$comma($concept_op, " . $opinion_no + 0 . ', now())';
	$comma = ', ';
    }
    
    # If we didn't get any valid concepts, return false.
    
    return unless $comma;
    
    # Now do the insert.
    
    $dbh->do($insert_sql);
    
    # Return the id of the last inserted row.  This can then be passed to
    # status() to determine whether the requested update has been completed.
    # We only care about the last inserted row, since they will be processed in
    # order and under most circumstances are processed together as a group.  Once
    # the last one has been completed, we know that all of the others have.
    
    return $dbh->last_insert_id();
}


=item rebuild ( dbh )

Requests a rebuild of the taxon trees.  This will be carried out by the
maintenance thread at the next opportunity.  Returns a request id that can be
passed to update_status() in order to determine whether the request has been
carried out.

=cut

sub rebuild {

    my ($dbh) = @_;
    
    my $rebuild_op = TYPE_REBUILD;
    
    $dbh->do("INSERT INTO $QUEUE_TABLE (type, param, time)
	      VALUES ($rebuild_op, 0, now())");
    
    return $dbh->last_insert_id();
}


=item check ( dbh )

Requests a check of the taxon trees.  This will be carried out by the
maintenance thread at the next opportunity.  Returns a request id that can be
passed to update_status() in order to determine whether the request has been
carried out.

=cut

sub check {

    my ($dbh) = @_;

    my $check_op = TYPE_CHECK;
    
    $dbh->do("INSERT INTO $QUEUE_TABLE (type, param, time)
	      VALUES ($check_op, 0, now())");
    
    return $dbh->last_insert_id();
}


=item updateStatus ( dbh, insert_id )

This routine can be called subsequently to update_concepts() or
update_opinions() to determine whether the requested updates have been
completed.  It returns a status value as follows:

=over 4

=item UPDATE_PENDING

The update has not yet been completed

=item UPDATE_COMPLETE

The update has been completed

=item UPDATE_ERROR

An error occurred while processing the update, so it was not completed.  In
this case, the error message is returned as the second item in the return
list. 

=item UPDATE_INVALID

The given id is not an integer greater than zero.

=back

Note that this routine will return UPDATE_COMPLETE if called with a parameter
that is not a return value from a previous call to update_concepts() or
update_opinions().

=cut

sub updateStatus {

    my ($dbh, $insert_id) = @_;
    
    # First make sure that the parameter value is valid.    
    
    unless ( $insert_id > 0 )
    {
	return UPDATE_INVALID;
    }
    
    # Then query the queue table.
    
    my ($type, $time, $comment) = $dbh->selectrow_array("
		SELECT type, time, comment FROM $QUEUE_TABLE
		WHERE id = ?", undef, $insert_id + 0);
    
    # Return the appropriate code.  If we didn't find anything, we can assume
    # that the update has completed.
    
    if ( $type == TYPE_ERROR )
    {
	return (UPDATE_ERROR, $comment);
    }
    
    elsif ( $type > 0 )
    {
	return (UPDATE_PENDING);
    }
    
    else
    {
	return (UPDATE_COMPLETE);
    }
}


=item daemon ( dbh )

This routine should be called by the taxon_trees maintenance daemon.  It goes
into a loop, checking the taxon_queue table for entries and carrying them out
in sequence.

=cut

sub daemon {
    
    my ($dbh) = @_;
    
    # Make sure that we respond to signals properly
    
    local $SIG{TERM} = \&handle_abort;
    local $SIG{QUIT} = \&handle_abort;
    local $SIG{INT} = \&handle_abort;
    
    # Loop until signalled to stop
    
    while ( !$ABORT )
    {
	# The following variables keep track of what we are being asked to do.
	
	my (@id_list, @concept_list, @opinion_list, $rebuild_tables, 
	    $check_tables, $msg_level);
	
	# Check $QUEUE_TABLE to see if any requests are pending.
	
	my $items = $dbh->prepare("
		SELECT id, type, param FROM $QUEUE_TABLE");
	
	$items->execute();
	
	while ( my($id, $type, $param, $time) = $items->fetchrow_array() )
	{	    
	    if ( $type == TYPE_CONCEPT )
	    {
		push @id_list, $id;
		push @concept_list, $param;
	    }
	    elsif ( $type == TYPE_OPINION )
	    {
		push @id_list, $id;
		push @opinion_list, $param;
	    }
	    elsif ( $type == TYPE_REBUILD )
	    {
		$rebuild_tables = 1;
		$msg_level = $param || 2;
	    }
	    elsif ( $type == TYPE_CHECK )
	    {
		$check_tables = 1;
		$msg_level = $param || 2;
	    }
	    elsif ( $type == TYPE_ERROR )
	    {
		# ignore these
	    }
	    else
	    {
		logMessage(0, "ERROR: invalid queue entry type '$type'");
	    }
	}
	
	my $id_filter = '(' . join(',', @id_list) . ')';
	
	# Also check to see if it is time for an automatic rebuild and if so
	# then trigger it.  We do an automatic rebuild every day at 9am Sydney
	# time (11pm GMT?), as long as it has been at least 18 hours
	# since the last rebuild for any reason.
	
	my ($sec, $min, $hour) = gmtime;
	
	my ($rebuild_interval) = $dbh->selectrow_array("
		SELECT timestampdiff(hour, time, now())
		FROM $QUEUE_TABLE WHERE type = " . TYPE_REBUILD);
	
	if ( $hour == 23 and ($rebuild_interval > 18 or $rebuild_interval == 0) )
	{
	    $rebuild_tables = 1;
	}
	
	# If a rebuild is pending, that takes precedence over everything else.
	
	if ( $rebuild_tables )
	{
	    logMessage(1, "Rebuilding tables at " . gmtime() . " GMT");
	    
	    # Put a record into the queue table, so that any threads that are
	    # tracking the status of update requests can inform their users
	    # that a rebuild is going on.  A param value of 1 means "in
	    # progress".
	    
	    my $rebuild_op = TYPE_REBUILD;
	    
	    $dbh->do("DELETE FROM $QUEUE_TABLE WHERE type = $rebuild_op");
	    $dbh->do("INSERT INTO $QUEUE_TABLE (type, time, param)
			  VALUES ($rebuild_op, now(), 1)");
	    
	    # Now, do the rebuild.
	    
	    eval {
		buildTables($dbh, $msg_level);
	    };
	    
	    # If an error occurred, we need to note this.
	    
	    if ( $@ )
	    {
		logMessage(0, "Error: $@");
	    }
	    
	    logMessage(1,"Done with rebuild at " . gmtime() . " GMT");
	    
	    # Record when this rebuild occurred, so that the next automatic
	    # one won't come too soon.  A param value of 2 means "complete".
	    # We also include the error message, if any.
	    
	    $dbh->do("DELETE FROM $QUEUE_TABLE WHERE type = $rebuild_op");
	    $dbh->do("INSERT INTO $QUEUE_TABLE (type, time, param, comment)
			  VALUES ($rebuild_op, now(), 2, $@)");
	    
	    # Then, we remove all update requests that were pending at the
	    # start of the rebuild operation, as they are made moot by the
	    # rebuild.  Any updates that may have been added since then should
	    # still be carried out.
	    
	    $dbh->do("DELETE FROM $QUEUE_TABLE WHERE id in $id_filter");
	    
	    # Also delete any error entries, since if the table is rebuilt
	    # properly then those entries are now irrelevant.  If an error has
	    # occurred in the rebuild, that is more serious anyway.
	    
	    $dbh->do("DELETE FROM $QUEUE_TABLE WHERE type = " . TYPE_ERROR);
	    
	    # Finally, we need to do a check to make sure that the rebuilt
	    # tables are okay.
	    
	    $check_tables = 1;
	}
	
	# If we are not rebuilding, but have one or more concepts or opinions to
	# update, do those operations.
	
	elsif ( @concept_list or @opinion_list )
	{
	    logMessage(1, "Updating tables at " . gmtime() . " GMT");
	    
	    eval {
		updateTables($dbh, \@concept_list, \@opinion_list);
	    };
	    
	    # If an error occurred during the update, leave those entries in
	    # the queue marked as errors.  This could be helpful in cleanup
	    # and debugging.
	    
	    if ( $@ )
	    {
		logMessage(0, "Error: $@");
		
		my $error_op = TYPE_ERROR;
		my $error_string = $dbh->quote($@);
		
		$dbh->do("UPDATE $QUEUE_TABLE
			  SET type = $error_op, comment = $error_string
			  WHERE id in $id_filter");
	    }
	    
	    # Otherwise, remove the entries representing the updates we have
	    # just carried out.  This will inform any threads watching the queue
	    # (i.e. those that requested the updates) that they have been
	    # accomplished.
	    
	    else
	    {
		$dbh->do("DELETE FROM $QUEUE_TABLE WHERE id in $id_filter");
	    }
	    
	    logMessage(1,"Done with update at " . gmtime() . " GMT");
	}
	
	# Now, if we were requested to check the tables (or if an update
	# occurred, which triggers an automatic check) then do the check.  We
	# want to do the check at the end, so that if a check request and some
	# update requests come in simultaneously, the updates get done first.
	
	if ( $check_tables )
	{
	    logMessage(1, "Checking tables at " . gmtime() . " GMT");
	    
	    # Put a record into the queue table, so that any threads that are
	    # tracking the status of update requests can inform their users
	    # that a rebuild is going on.  A param value of 1 means "in
	    # progress".
	    
	    my $check_op = TYPE_CHECK;
	    
	    $dbh->do("DELETE FROM $QUEUE_TABLE WHERE type = $check_op");
	    $dbh->do("INSERT INTO $QUEUE_TABLE (type, time, param)
			  VALUES ($check_op, now(), 1)");
	    
	    eval {
		checkTables($dbh, $msg_level);
	    };
	    
	    # If an error occurred, we need to note this.
	    
	    if ( $@ )
	    {
		logMessage(0, "Error: $@");
	    }
	    
	    # Now, record when this check occurred, so that the next automatic
	    # one won't come too soon.  A param value of 2 means "complete".
	    # We also include the error message, if any.
	    
	    my $check_op = TYPE_CHECK;
	    
	    $dbh->do("DELETE FROM $QUEUE_TABLE WHERE type = $check_op");
	    $dbh->do("INSERT INTO $QUEUE_TABLE (type, time, param, comment)
			  VALUES ($check_op, now(), 2, $@)");
	}
	
	# Finally, sleep and then loop around to check again.
	
	sleep(1);
    }
}


# handle_signal ( )
# 
# This is called as a signal handler, and directs the process to abort its
# current request and terminate immediately.

sub handle_signal {
    
    my ($sig) = @_;
    
    $ABORT = 1;
    die "Caught signal $sig: aborting";
}

# updateTables ( dbh, concept_list, opinion_list, msg_level )
# 
# This routine is called by daemon() whenever opinions are created, edited, or
# deleted, or when concept membership in the authorities table is changed.  It
# should NEVER BE CALLED FROM OUTSIDE THIS MODULE, because it has no
# concurrency control and so inconsistent updates and race conditions may
# occur.
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

sub updateTables {

    my ($dbh, $concept_list, $opinion_list, $msg_level, $keep_temps) = @_;
    
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
    
    $DB::single = 1;
    
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
    # relation, so that we can detect and break any synonymy cycles that might
    # be created by this update.
    
    expandToJuniors($dbh);
    
    # Then compute the synonymy relation for every concept in $TREE_TEMP.
    
    computeSynonymy($dbh);
    
    # Now that we have computed the synonymy relation, we need to expand
    # $TREE_TEMP to include senior synonyms of the concepts represented in it.
    # We need to do this before we update the hierarchy relation because the
    # classification of those senior synonyms might change; if one of the rows
    # in $TREE_TEMP is a new junior synonym, it might have a 'belongs to'
    # opinion that is more recent and reliable than the previous best opinion
    # for the senior.
    
    expandToSeniors($dbh);
    
    # At this point we remove synonym chains, so that synonym_no always points
    # to the most senior synonym of each taxonomic concept.  This needs to be
    # done before we update the hierarchy relation, because that computation
    # depends on this property of synonym_no.
    
    linkSynonyms($dbh);
    
    # Then compute the hierarchy relation for every concept in $TREE_TEMP.
    
    computeHierarchy($dbh);
    
    # Some parent_no values may not have been set properly, in particular
    # those whose classification points to a parent which is not itself in
    # $TREE_TEMP.  These must now be updated to their proper values.
    
    linkParents($dbh);
    
    # If the adjustments to the hierarchy are small, then we can now update
    # $TREE_TABLE using the rows from $TREE_TEMP and then adjust the tree
    # sequence in-place.  This will finish the procedure.  We set the
    # threshold at 3 because altering the tree in-place is several times more
    # efficient than rebuilding it even if we have to scan over the entire
    # table multiple times (3 is actually just a guess...)
    
    if ( treePerturbation($dbh) < 3 )
    {
	updateTreeTables($dbh, $keep_temps);
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
	
	# Then activate the temporary tables by renaming them over the previous
	# ones. 
	
	activateNewTables($dbh, $keep_temps);
    }
    
    my $a = 1;		# we can stop here when debugging
}


# buildTables ( dbh, msg_level )
# 
# Builds the taxon tree tables from scratch, using only the information in
# $AUTH_TABLE and $OPINIONS_TABLE.  If the 'msg_level' parameter is given, it
# controls the verbosity of log messages produced.  This routine is called by
# daemon(), and should NEVER BE CALLED FROM OUTSIDE THIS MODULE.  There is no
# concurrency control, and so race conditions and inconsistent updates may
# occur if you do that.
# 
# The $step_control parameter is for debugging only.  If specified it must be
# a hash reference that will control which steps are taken.

sub buildTables {

    my ($dbh, $msg_level, $step_control) = @_;
    
    # First, set the variables that control log output.
    
    $MSG_TAG = 'Rebuild'; $MSG_LEVEL = $msg_level || 1;
    
    unless ( ref $step_control eq 'HASH' and %$step_control ) {
	$step_control = { 'a' => 1, 'b' => 1, 'c' => 1,
			  'd' => 1, 'e' => 1, 'f' => 1, 'g' => 1 };
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
    
    # Update the synonymy relation so that synonym_no points to the most
    # senior synonym, instead of the immediate senior synonym.
    
    linkSynonyms($dbh) if $step_control->{c};
    
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
				   $AUTH_TABLE as a1 read,
				   $AUTH_TABLE as a2 read,
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
		$filter_clause
		ORDER BY ri DESC, pubyr DESC, opinion_no DESC");
    
    return;
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
    
    logMessage(2, "updating opinions table to reflect concept changes");
    
    # First, $OPINION_CACHE
    
    $result = $dbh->do("UPDATE $OPINION_CACHE o JOIN authorities a
				ON a.taxon_no = o.child_spelling_no
			SET o.orig_no = a.orig_no
			WHERE o.orig_no in $concept_filter");
    
    $result = $dbh->do("UPDATE $OPINION_CACHE o JOIN authorities a
				ON a.taxon_no = o.parent_spelling_no
			SET o.parent_no = a.orig_no
			WHERE o.parent_no in $concept_filter");
    
    # Next, $OPINIONS_TABLE
    
    $result = $dbh->do("UPDATE $OPINIONS_TABLE o JOIN authorities a
				ON a.taxon_no = o.child_spelling_no
			SET o.child_no = a.orig_no
			WHERE a.orig_no in $concept_filter");
    
    $result = $dbh->do("UPDATE $OPINIONS_TABLE o JOIN authorities a
				ON a.taxon_no = o.parent_spelling_no
			SET o.parent_no = a.orig_no
			WHERE a.orig_no in $concept_filter");
    
    return;
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
    # Otherwise, grab every concept in $AUTH_TABLE
    
    my $concept_filter = '';
    
    if ( ref $concept_hash eq 'HASH' )
    {
	$concept_filter = 'WHERE orig_no in (' . 
	    join(',', keys %$concept_hash) . ')';
    }
	
    $result = $dbh->do("INSERT INTO $TREE_TEMP
			SELECT distinct orig_no, 0, 0, 0, 0, null, null, null
			FROM $AUTH_TABLE $concept_filter");
    
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
        
    return;
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
    
    logMessage(1, "    found $count misspellings") if $count > 0;
    
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
# junior synonyms of the concepts already in $TREE_TEMP.

sub expandToJuniors {
    
    my ($dbh) = @_;
    
    # We need to repeat the following process until no new rows are added, so
    # that junior synonyms of junior synonyms, etc. also get added.
    
    # Note that we can't just use the synonym_no field of $TREE_TABLE, because
    # it refers to the most senior synonym instead of the immediate senior
    # synonym.  We might have a synonym chain A -> B -> C with B in
    # $TREE_TEMP.  In such a case, we would miss A if we relied on synonym_no.

    while (1)
    {
	my $result = $dbh->do("
		INSERT IGNORE INTO $TREE_TEMP (orig_no, spelling_no)
		SELECT m.orig_no, m.spelling_no
		FROM $TREE_TEMP t
			JOIN $OPINION_CACHE o ON o.parent_no = t.orig_no
				and o.status != 'belongs to'
			STRAIGHT_JOIN $TREE_TABLE m ON m.opinion_no = o.opinion_no");
	
	last if $result == 0;
    }
    
    my $a = 1;		# we can stop on this line when debugging
}


# expandToSeniors ( dbh )
# 
# Expand $TREE_TEMP by adding to it all rows from $TREE_TABLE which represent
# senior synonyms of the concepts already in $TREE_TEMP, and all rows from
# $TREE_TABLE representing concepts which used to be senior synonyms but might
# not be anymore.  All of these might undergo a change of classification as
# part of this update.
# 
# We have to expand $CLASS_TEMP in parallel, so that we will have the
# necessary information to execute computeHierarchy() later in this process.

sub expandToSeniors {
    
    my ($dbh) = @_;
    
    my ($count, $result);
    
    # We need to repeat the following process until no new rows are added, so
    # that senior synonyms of senior synonyms, etc. also get added
    
    do
    {
	# First the new synonyms (using $CLASS_TEMP)
	
	$count = $dbh->do("
		INSERT IGNORE INTO $TREE_TEMP (orig_no, spelling_no, synonym_no)
		SELECT m.orig_no, m.spelling_no, m.synonym_no
		FROM $CLASS_TEMP c
			JOIN $OPINION_CACHE o USING (opinion_no)
			STRAIGHT_JOIN $TREE_TABLE m ON o.parent_no = m.orig_no
				and o.status != 'belongs to'");
	
	# Then the old synonyms (using $TREE_TABLE)
	
	$count += $dbh->do("
		INSERT IGNORE INTO $TREE_TEMP (orig_no, spelling_no, synonym_no)
		SELECT m.orig_no, m.spelling_no, m.synonym_no
		FROM $TREE_TEMP t 
			JOIN $TREE_TABLE m USING (orig_no)
			JOIN $OPINION_CACHE o ON o.opinion_no = m.opinion_no
			JOIN $TREE_TABLE m2 ON o.parent_no = m2.orig_no
				and o.status != 'belongs to'");
	
	# Then expand $CLASS_TEMP with corresponding rows for every concept
	# that was added to $TREE_TEMP.
	
	$result = $dbh->do("
		INSERT IGNORE INTO $CLASS_TEMP
		SELECT m.orig_no, m.opinion_no, m.parent_no, o.ri, o.pubyr, o.status
		FROM $TREE_TEMP t JOIN $TREE_TABLE m USING (orig_no)
			JOIN $OPINION_CACHE o ON o.opinion_no = m.opinion_no
			LEFT JOIN $CLASS_TEMP c ON c.orig_no = t.orig_no
		WHERE c.opinion_no is null") if $count > 0;
	
    } while $count > 0;
    
    return;
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
    
    return;
}


# linkSynonyms ( dbh )
# 
# Alter the synonym_no field to remove synonym chains.  Whenever we have 
# a -> b and b -> c, change the relation so that a -> c and b -> c.  This
# makes synonym_no represent the most senior synonym, instead of just the
# immediate senior synonym.  Because the chains may be more than three taxa
# long, we need to repeat the process until no more rows are affected.

sub linkSynonyms {

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
	logMessage(0,"WARNING - possible synonymy cycle detected during synonym linking");
    }
    
    return;
}


# computeHierarchy ( dbh )
# 
# Fill in the opinion_no and parent_no fields of $TREE_TEMP.  This determines
# the classification of each taxonomic concept represented in $TREE_TEMP, and
# thus the Hierarchy relation as well.
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
    
    my ($dbh, $is_update) = @_;
    
    my ($result, $filter, @check_taxa, %taxa_moved);
    
    logMessage(2, "computing hierarchy relation (d)");
    
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
    
    logMessage(1, "    found " . scalar(@breaks) . " cycle(s)") if @breaks;
    
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
    # 
    # Note that if this is an update (as opposed to a rebuild) not all of the
    # parent_no values will be properly set.  We will have to fix that later
    # with linkParents().
    
    logMessage(2, "    setting parent_no");
    
    $result = $dbh->do("
		UPDATE $TREE_TEMP t JOIN $TREE_TEMP t2 ON t2.orig_no = t.synonym_no
		    JOIN $OPINION_CACHE o ON o.opinion_no = t2.opinion_no
		    JOIN $TREE_TEMP t3 ON t3.orig_no = o.parent_no
		SET t.parent_no = t3.synonym_no");
    
    # Once we have set parent_no for all concepts, we can efficiently index it.
    
    logMessage(2, "    indexing parent_no");
    
    $result = $dbh->do("ALTER TABLE $TREE_TEMP add index (parent_no)");
    
    my $a = 1;
    
    return;
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


# linkParents ( dbh )
# 
# Update the parent_no field of $TREE_TEMP to include parents which are not
# themselves represented in $TREE_TEMP.

sub linkParents {
    
    my ($dbh) = @_;
    
    my $result;
    
    # Set parent_no when the parent is not represented in $TREE_TEMP.
    
    $result = $dbh->do("
		UPDATE $TREE_TEMP t
		    JOIN $TREE_TEMP t2 ON t2.orig_no = t.synonym_no
		    JOIN $OPINION_CACHE o ON o.opinion_no = t2.opinion_no
		    JOIN $TREE_TABLE m ON m.orig_no = o.parent_no
		SET t.parent_no = m.synonym_no
		WHERE t.parent_no = 0 and o.parent_no != 0");
    
    return;
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


our (%children, %parent, %tree); our(%sseq);

# computeTreeSequence ( dbh )
# 
# Fill in the lft, rgt and depth fields of $TREE_TMP.  This has the effect of
# arranging the rows into a forest of Nested Set trees.  For more information,
# see: http://en.wikipedia.org/wiki/Nested_set_model.

sub computeTreeSequence {
    
    my ($dbh) = @_;
    
    my $result;
    
    logMessage(2, "traversing and marking taxon trees (e)");
    
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
	
	if ( exists $sseq{$sequence} )
	{
	    print STDERR "duplicate sequence for $sequence\n";
	}
	
	$sseq{$sequence} = $taxon;
	
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

    my ($dbh, $keep_temps) = @_;
    
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
				   $TREE_TABLE WRITE,
				   $TREE_TEMP as t READ,
				   $TREE_TEMP READ");

    #				   $TREE_TABLE as m2 WRITE,
    #				   $TREE_TABLE as m3 WRITE,
    #				   $OPINION_CACHE as o READ
    
    # Now write all rows of $TREE_TEMP over the corresponding rows of
    # $TREE_TABLE.
    
    logMessage(2, "copying into active tables (f)");
    
    $result = $dbh->do("REPLACE INTO $TREE_TABLE
			SELECT * FROM $TREE_TEMP");
    
    # At some point we may want to recompute the parent_no values of children
    # of concepts that have changed synonymy.  This would also involve
    # additional tree sequence modifications, so it might not be worth the
    # bother.  Unless we do this, some concepts may have their parent_no point
    # to a junior synonym.
    
    # $result = $dbh->do("
    # 		UPDATE $TREE_TABLE as m JOIN $TREE_TEMP as t ON m.parent_no = t.orig_no
    # 			JOIN $TREE_TABLE as m2 ON m2.orig_no = m.synonym_no
    # 			JOIN $OPINION_CACHE as o ON o.opinion_no = m2.opinion_no
    # 			JOIN $TREE_TABLE as m3 ON m3.orig_no = o.parent_no
    # 		SET m.parent_no = m3.synonym_no");
    
    # Now adjust the tree sequence to take into account all rows that have
    # changed their position in the hierarchy.  We couldn't do this until now,
    # because the procedure requires a table that holds the entire updated set
    # of taxon trees.
    
    my ($max_lft) = $dbh->selectrow_array("SELECT max(lft) from $TREE_TABLE");
    
    logMessage(2, "adjusting tree sequence (e)");
    logMessage(2, "    " . scalar(@move) . " concept(s) are moving within the hierarchy");
    
    foreach my $pair (@move)
    {
	adjustTreeSequence($dbh, $max_lft, @$pair);
    }
    
    # Now we can unlock the main tables.
    
    $result = $dbh->do("UNLOCK TABLES");
    
    # Finally, we need to update $SUPPRESS_TABLE.  We need to add everything
    # that is in $SUPPRESS_TEMP, and remove everything that corresponds to a
    # classification opinion in $TREE_TEMP.  The latter represent opinions
    # which were previously suppressed, but now should not be.
    
    # We don't bother to lock these tables, since no other part of the code
    # should be using $SUPPRESS_TABLE in a critical role.
    
    $result = $dbh->do("INSERT INTO $SUPPRESS_TABLE
			SELECT * FROM $SUPPRESS_TEMP");
    
    $result = $dbh->do("DELETE $SUPPRESS_TABLE FROM $SUPPRESS_TABLE
				JOIN $TREE_TEMP USING (opinion_no)");
    
    # Now, we can remove the temporary tables.

    unless ( $keep_temps )
    {
	logMessage(2, "removing temporary tables");
	
	$result = $dbh->do("DROP TABLE IF EXISTS $TREE_TEMP");
	$result = $dbh->do("DROP TABLE IF EXISTS $SUPPRESS_TEMP");
	
	$result = $dbh->do("DROP TABLE IF EXISTS $SPELLING_TEMP");
	$result = $dbh->do("DROP TABLE IF EXISTS $CLASS_TEMP");
	$result = $dbh->do("DROP TABLE IF EXISTS $SYNONYM_TEMP");
    }
    
    return;
}


# treePerturbation ( dbh )
# 
# Determine the amount by which the taxon tree will need to be perturbed in
# order to complete the current update.  This is expressed as a fraction of
# the tree size.  The return value will be used to determine whether to alter
# the tree sequence in-place or recompute it from scratch.

sub treePerturbation {
    
    my ($dbh) = @_;
    
    my ($max_seq) = $dbh->selectrow_array("SELECT max(lft) from $TREE_TABLE");
    
    my $moved_taxa = $dbh->prepare("
		SELECT t.orig_no, t.parent_no, m.parent_no, m2.lft, m.lft
		FROM $TREE_TEMP as t LEFT JOIN $TREE_TABLE as m USING (orig_no)
			LEFT JOIN $TREE_TABLE as m2 ON m2.orig_no = t.parent_no
		WHERE t.parent_no != m.parent_no
		ORDER BY m.lft DESC");
    
    $moved_taxa->execute();
    
    my $total = 0;
    
    while ( my ($orig_no, $new_parent, $old_parent, 
		$new_pos, $old_pos) = $moved_taxa->fetchrow_array() )
    {
	# If both new and old positions are defined
	
	if ( $new_pos && $old_pos )
	{
	    $total += abs($new_pos - $old_pos);
	}
	
	# If the new one is not (i.e. we are moving that taxon to the end of
	# the tree)
	
	elsif ( $old_pos )
	{
	    $total += ($max_seq - $old_pos);
	}
	
	# If the old one is not (i.e. we are moving that taxon into the tree)
	
	elsif ( $new_pos )
	{
	    $total += ($max_seq - $new_pos);
	}
    }
    
    return ($total / $max_seq);
}


# adjustTreeSequence ( dbh, max_lft, orig_no, new_parent )
# 
# Adjust the tree sequence so that taxon $orig_no (along with all of its
# children) falls properly under its new parent.  The taxon will end up as the
# last child of the new parent.  The parameter $max_lft should be the current
# maximum lft value in the table.

sub adjustTreeSequence {
    
    my ($dbh, $max_lft, $orig_no, $new_parent) = @_;
    
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
		SELECT lft, rgt, depth+1 FROM $TREE_TABLE
		WHERE orig_no = $new_parent");
	
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
	($parent_rgt) = $max_lft;
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
				   $disp)
		    $change_depth,
		    lft = lft + if(lft < $old_pos, $width, $disp)
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
    
    return;
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
    
    # Delete any backup tables that might still be around
    
    $result = $dbh->do("DROP TABLE IF EXISTS $TREE_BAK");
    $result = $dbh->do("DROP TABLE IF EXISTS $SUPPRESS_BAK");
    
    # Create dummy versions of any of the main tables that might be currently
    # missing
    
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $TREE_TABLE LIKE $TREE_TEMP");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $SUPPRESS_TABLE LIKE $SUPPRESS_TEMP");
    
    # Now do the Atomic Table Swap (tm)
    
    $result = $dbh->do("RENAME TABLE
			    $TREE_TABLE to $TREE_BAK,
			    $TREE_TEMP to $TREE_TABLE,
			    $SUPPRESS_TABLE to $SUPPRESS_BAK,
			    $SUPPRESS_TEMP to $SUPPRESS_TABLE");
    
    # Then we can get rid of the backup tables
    
    $result = $dbh->do("DROP TABLE $TREE_BAK");
    $result = $dbh->do("DROP TABLE $SUPPRESS_BAK");
    
    # Delete the auxiliary tables too, unless we were told to keep them.
    
    unless ( $keep_temps )
    {
	logMessage(2, "removing temporary tables");
	
	$result = $dbh->do("DROP TABLE IF EXISTS $SPELLING_TEMP");
	$result = $dbh->do("DROP TABLE IF EXISTS $CLASS_TEMP");
	$result = $dbh->do("DROP TABLE IF EXISTS $SYNONYM_TEMP");
    }
    
    logMessage(1, "done rebuilding taxon trees");
    
    my $a = 1;		# we can stop here when debugging
}


# check ( dbh )
# 
# Check the integrity of $TREE_TABLE.  Return true if the table is okay.  If
# not, return false, write out some error messages, and leave one or more error messages
# in the variable @TREE_ERRORS.

sub check {

    my ($dbh, $msg_level) = @_;
    
    my $result;
    
    # First, set the variables that control log output.
    
    $MSG_TAG = 'Check'; $MSG_LEVEL = $msg_level || 1;
    
    # Then, clear the error list.
    
    @TREE_ERRORS = ();
    
    # Next, make sure that every orig_no value in authorities corresponds to a
    # taxon_no value.
    
    logMessage(2, "checking concept numbers");
    
    my ($bad_orig_auth) = $dbh->selectrow_array("
		SELECT count(distinct a.orig_no)
		FROM $AUTH_TABLE a LEFT JOIN $AUTH_TABLE a2
			ON a.orig_no = a2.taxon_no
		WHERE a2.taxon_no is null");
    
    if ( $bad_orig_auth > 0 )
    {
	push @TREE_ERRORS, "Found $bad_orig_auth bad orig_no value(s) in $AUTH_TABLE";
	logMessage(1, "    found $bad_orig_auth bad orig_no value(s) in $AUTH_TABLE");
	
	if ( $bad_orig_auth < $REPORT_THRESHOLD )
	{
	    my ($list) = $dbh->selectcol_arrayref("
		SELECT distinct a.orig_no
		FROM $AUTH_TABLE a LEFT JOIN $AUTH_TABLE a2
			ON a.orig_no = a2.taxon_no
		WHERE a2.taxon_no is null");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    # Also make sure that the orig_no values are in 1-1 correspondence between
    # $AUTH_TABLE and $TREE_TABLE.
    
    my ($bad_orig) = $dbh->selectrow_array("
		SELECT count(t.orig_no)
		FROM $TREE_TABLE as t LEFT JOIN $AUTH_TABLE as a USING (orig_no)
		WHERE a.orig_no is null");
    
    if ( $bad_orig > 0 )
    {
	push @TREE_ERRORS, "Found $bad_orig concept(s) in $TREE_TABLE that do not match $AUTH_TABLE";
	logMessage(1, "    found $bad_orig concept(s) in $TREE_TABLE that do not match $AUTH_TABLE");
	
	if ( $bad_orig < $REPORT_THRESHOLD )
	{
	    my ($list) = $dbh->selectcol_arrayref("
		SELECT t.orig_no
		FROM $TREE_TABLE as t LEFT JOIN $AUTH_TABLE as a USING (orig_no)
		WHERE a.orig_no is null");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    my ($missing_orig) = $dbh->selectrow_array("
		SELECT count(distinct a.orig_no)
		FROM $AUTH_TABLE as a LEFT JOIN $TREE_TABLE as t USING (orig_no)
		WHERE t.orig_no is null");
    
    if ( $missing_orig > 0 )
    {
	push @TREE_ERRORS, "Found $missing_orig concept(s) missing from $TREE_TABLE";
	logMessage(1, "    found $missing_orig concept(s) missing from $TREE_TABLE");
	
	if ( $missing_orig < $REPORT_THRESHOLD )
	{
	    my ($list) = $dbh->selectcol_arrayref("
		SELECT distinct a.orig_no
		FROM $AUTH_TABLE as a LEFT JOIN $TREE_TABLE as t USING (orig_no)
		WHERE t.orig_no is null");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    # Make sure every concept is distinct
    
    my ($concept_chain) = $dbh->selectrow_array("
		SELECT count(a.orig_no)
		FROM $AUTH_TABLE as a JOIN $AUTH_TABLE a2
			ON a2.taxon_no = a.orig_no
		WHERE a2.taxon_no != a2.orig_no");
    
    if ( $concept_chain > 0 )
    {
	push @TREE_ERRORS, "Found $concept_chain concept(s) chained to other concept(s)";
	logMessage(1, "    found $concept_chain concept(s) chained to other concept(s)");
	
	if ( $missing_orig < $REPORT_THRESHOLD )
	{
	    my ($list) = $dbh->selectcol_arrayref("
		SELECT a.orig_no
		FROM $AUTH_TABLE as a JOIN $AUTH_TABLE a2
			ON a2.taxon_no = a.orig_no
		WHERE a2.taxon_no != a2.orig_no");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    # Make sure that every spelling number is a member of the proper concept
    # group.
    
    logMessage(2, "checking spelling numbers");
    
    my ($bad_spelling) = $dbh->selectrow_array("
		SELECT count(t.orig_no)
		FROM $TREE_TABLE as t LEFT JOIN $AUTH_TABLE as a ON a.taxon_no = t.spelling_no
		WHERE a.orig_no != t.orig_no or a.orig_no is null");
    
    if ( $bad_spelling > 0 )
    {
	push @TREE_ERRORS, "Found $bad_spelling entries with bad spelling numbers";
	logMessage(1, "    found $bad_spelling entries with bad spelling numbers");
	
	if ( $bad_spelling < $REPORT_THRESHOLD )
	{
	    my ($list) = $dbh->selectcol_arrayref("
		SELECT t.orig_no
		FROM $TREE_TABLE as t JOIN $AUTH_TABLE as a ON a.taxon_no = t.spelling_no
		WHERE a.orig_no != t.orig_no or a.orig_no is null");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    # Make sure that every synonym_no matches an orig_no.
    
    logMessage(2, "checking synonyms");
    
    my ($bad_synonym) = $dbh->selectrow_array("
		SELECT count(t.orig_no)
		FROM $TREE_TABLE as t LEFT JOIN $TREE_TABLE as t2
			ON t.synonym_no = t2.orig_no
		WHERE t2.orig_no is null");
    
    if ( $bad_synonym > 0 )
    {
	push @TREE_ERRORS, "Found $bad_synonym entries with bad synonym numbers";
	logMessage(1, "    found $bad_synonym entries with bad synonym numbers");
	
	if ( $bad_synonym < $REPORT_THRESHOLD )
	{
	    my ($list) = $dbh->selectcol_arrayref("
		SELECT t.orig_no
		FROM $TREE_TABLE as t LEFT JOIN $TREE_TABLE as t2
			ON t.synonym_no = t2.orig_no
		WHERE t2.orig_no is null");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    # Make sure that every parent_no matches an orig_no.
    
    logMessage(2, "checking parents");
    
    my ($bad_parent) = $dbh->selectrow_array("
		SELECT count(t.orig_no)
		FROM $TREE_TABLE as t LEFT JOIN $TREE_TABLE as t2
			ON t.parent_no = t2.orig_no
		WHERE t.parent_no != 0 and t2.orig_no is null");
    
    if ( $bad_parent > 0 )
    {
	push @TREE_ERRORS, "Found $bad_parent entries with bad parent numbers";
	logMessage(1, "    found $bad_parent entries with bad parent numbers");
	
	if ( $bad_parent < $REPORT_THRESHOLD )
	{
	    my ($list) = $dbh->selectcol_arrayref("
		SELECT t.orig_no
		FROM $TREE_TABLE as t LEFT JOIN $TREE_TABLE as t2
			ON t.parent_no = t2.orig_no
		WHERE t2.orig_no is null");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    # check that only senior synonyms are classified by 'belongs to' opinions
    
    my ($bad_class) = $dbh->selectrow_array("
		SELECT count(t.orig_no)
		FROM $TREE_TABLE as t JOIN $OPINION_CACHE o USING (opinion_no)
		WHERE (o.status = 'belongs to' and t.synonym_no != t.orig_no)
		   or (o.status != 'belongs to' and t.synonym_no = t.orig_no)");
    
    if ( $bad_class > 0 )
    {
	push @TREE_ERRORS, "Found $bad_class entries improperly classified";
	logMessage(1, "    found $bad_class entries improperly classified");
	
	if ( $bad_class < $REPORT_THRESHOLD )
	{
	    my ($list) = $dbh->selectcol_arrayref("
		SELECT t.orig_no
		FROM $TREE_TABLE as t JOIN $OPINION_CACHE o USING (opinion_no)
		WHERE (o.status = 'belongs to' and t.synonym_no != t.orig_no)
		   or (o.status != 'belongs to' and t.synonym_no = t.orig_no)");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    # Check the integrity of the tree sequence.
    
    logMessage(2, "checking tree sequence");
    
    my ($bad_seq) = $dbh->selectrow_array("
		SELECT count(t.orig_no)
		FROM $TREE_TABLE as t join $TREE_TABLE as p on p.orig_no = t.parent_no
		WHERE (t.lft < p.lft or t.lft > p.rgt)");
    
    if ( $bad_seq > 0 )
    {
	push @TREE_ERRORS, "Found $bad_seq entries out of sequence";
	logMessage(1, "    found $bad_seq entries out of sequence");

	if ( $bad_synonym < $REPORT_THRESHOLD )
	{
	    my ($list) = $dbh->selectcol_arrayref("
		SELECT t.orig_no
		FROM $TREE_TABLE as t join $TREE_TABLE as p on p.orig_no = t.parent_no
		WHERE (t.lft < p.lft or t.lft > p.rgt)");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    # Now report the results
    
    if (@TREE_ERRORS)
    {
	logMessage(1, "FOUND " . scalar(@TREE_ERRORS) . " ERROR(S)");
	return;
    }
    
    else
    {
	logMessage(1, "Everything OK");
	return 1;
    }
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


# The following routines are intended only for debugging.

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


=item getCurrentSpelling ( dbh, taxon_no )

Given the identifier of a spelling or of a taxonomic concept,
returns an object representing the currently accepted spelling.

=cut

sub getCurrentSpelling {
    
    my ($dbh, $taxon_no) = @_;
    
    return getTaxonParametrized($dbh, $taxon_no, 'spelling');
}


=item listCurrentSpelling ( dbh, taxon_no )

Given the identifier of a spelling or of a taxonomic concept, returns the
spelling number (taxon_no) of the currently accepted spelling.

=cut

sub listCurrentSpelling {

    my ($dbh, $taxon_no) = @_;
    
    return getTaxonIdParametrized($dbh, $taxon_no, 'spelling');
}


=item getAllSpellings ( dbh, taxon_no )

Given the identifier of a spelling or of a taxonomic concept, returns a list
of objects representing all spellings of the indicated concept.  The first
object in the list will be the currently accepted spelling.

=cut

sub getAllSpellings {
    
    my ($dbh, $taxon_no) = @_;
    
    return getTaxonListParametrized($dbh, $taxon_no, 'spelling');
}


=item listAllSpellings ( dbh, taxon_no )

Given the identifier of a spelling or of a taxonomic concept, returns a list
of spelling numbers (taxon_no) representing all spellings of the indicated
concept.  The first item in the list will be the currently accepted spelling.

=cut

sub listAllSpellings {

    my ($dbh, $taxon_no) = @_;
    
    return getTaxonIdListParametrized($dbh, $taxon_no, 'spelling');
}


=item getOriginalCombination ( dbh, taxon_no )

Returns an object representing the original spelling of the specified
taxonomic concept, which may be the specified taxon itself.

=cut

sub getOriginalCombination {
    
    my ($dbh, $taxon_no) = @_;
    
    return getTaxonParametrized($dbh, $taxon_no, 'orig');
}


=item listOriginalCombination ( dbh, taxon_no )

Returns the taxon_no for the original spelling of the given taxonomic concept,
which may be the specified taxon itself.

=cut

sub listOriginalCombination {

    my ($dbh, $taxon_no) = @_;
    
    return getTaxonIdParametrized($dbh, $taxon_no, 'orig');
}


=item getMostSeniorSynonym ( dbh, taxon_no )

Returns an object representing the most senior synonym of the specified
taxonomic concept.  If the concept does not have a senior synonym, an object
representing the concept itself is returned.

=cut

sub getMostSeniorSynonym {
    
    my ($dbh, $taxon_no) = @_;
    
    return getTaxonParametrized($dbh, $taxon_no, 'synonym');
}


=item listMostSeniorSynonym ( dbh, taxon_no )

Returns the taxon_no for the most senior synonym of the given taxonomic
concept, which may be the specified taxon itself.

=cut

sub listMostSeniorSynonym {

    my ($dbh, $taxon_no) = @_;
    
    return getTaxonIdParametrized($dbh, $taxon_no, 'synonym');
}


=item getAllSynonyms ( dbh, taxon_no )

Returns a list of objects representing all synonyms of the specified taxonomic
concept, in order of seniority.  If the specified concept has no synonyms, a single
object is returned representing the concept itself.

=cut

sub getAllSynonyms {

    my ($dbh, $taxon_no) = @_;
    
    return getTaxonListParametrized($dbh, $taxon_no, 'synonym');
}


=item listAllSynonyms ( dbh, taxon_no )

Returns a list of taxon identifiers, listing all synonyms (junior and senior)
of the given taxon as well as the taxon itself.  These are ordered senior to
junior.  If the concept has no synonyms, its own identifier is returned.

=cut

sub listAllSynonyms {

    my ($dbh, $taxon_no) = @_;
    
    return getTaxonIdListParametrized($dbh, $taxon_no, 'synonym');
}


=item getParent ( dbh, taxon_no )

Returns an object representing the parent of the given taxon.  If there is
none, then nothing is returned.

=cut

sub getParent {
    
    my ($dbh, $taxon_no) = @_;
    
    return getTaxonParametrized($dbh, $taxon_no, 'parent');
}


=item listParent ( dbh, taxon_no )

Returns the taxon identifier of the parent of the given taxon, or 0
if there is none.

=cut

sub listParent {

    my ($dbh, $taxon_no) = @_;
    
    return getTaxonIdParametrized($dbh, $taxon_no, 'parent');
}


=item getClassification ( dbh, taxon_no )

Returns an object representing the immediate parent of the given taxonomic
concept, which may not be the same as the one referred to by C<parent_no>.  It
may be a junior synonym instead, if the relevant opinions so state.

=cut

sub getClassification {

    my ($dbh, $taxon_no) = @_;
    
    return getTaxonParametrized($dbh, $taxon_no, 'classification');
}

=item listClassification ( dbh, taxon_no )

Returns the taxon identifier of the immediate parent of the given taxonomic
concept, which may not be the same as the one referred to by C<parent_no>.  It
may be a junior synonym instead, if the relevant opinions so state.

=cut

sub listClassification {

    my ($dbh, $taxon_no) = @_;
    
    return getTaxonIdParametrized($dbh, $taxon_no, 'classification');
}


=item getParents ( dbh, taxon_no )

Returns a list of objects representing all parents of the given taxon, in
order from highest to lowest.

=cut

sub getParents {

    my ($dbh, $taxon_no) = @_;
    
    return getTaxonListParametrized($dbh, $taxon_no, 'parent');
}


=item listParents ( dbh, taxon_no )

Returns a list of taxon identifiers representing all parents of the given
taxon, in order from highest to lowest.

=cut

sub listParents {

    my ($dbh, $taxon_no) = @_;
    
    return getTaxonIdListParametrized($dbh, $taxon_no, 'parent');
}


=item getChildren ( dbh, taxon_no, options )

Returns a list of objects representing the children of the given taxon.  If
the option "include_synonyms" is true, then synonyms of the children are
included as well.  If the option "all" is true, then all descendants are
included, not just immediate children.

=cut

sub getChildren {

    my ($dbh, $taxon_no) = @_;
    
    return getTaxonListParametrized($dbh, $taxon_no, 'child');
}


=item listChildren ( dbh, taxon_no, options )

Returns a list of taxon identifiers for all children of the given taxon.  If
the option "include_synonyms" is true, then synonyms of the children are
included as well.  If the option "all" is true, then all descendants are
included, not just immediate children.

=cut

sub listChildren {

    my ($dbh, $taxon_no) = @_;
    
    return getTaxonIdListParametrized($dbh, $taxon_no, 'child');
}


our ($INFO_EXPR) = "a.taxon_name, a.taxon_no, a.taxon_rank, a.orig_no, o.status";

# getTaxonParametrized ( dbh, taxon_no, parameter )
# 
# Returns a Taxon object corresponding to the given parameter of the base taxon
# specified by taxon_no.  If an error occurs, returns an Error object
# instead.  Valid parameters are:
# 
#    orig, spelling, synonym, parent, classification
# 
# For all but the first two, returns the current spelling of the indicated taxon.

sub getTaxonParametrized {
    
    my ($dbh, $taxon_no, $parameter) = @_;
    
    # Turn off the automatic error handling
    
    local($dbh->{RaiseError}) = 0;
    
    # Check arguments
    
    unless ( defined $taxon_no && $taxon_no > 0 )
    {
	return error("invalid taxon_no '$taxon_no'");
    }
    
    # Fetch the requested information
    
    my ($taxon);
    
    # Parameters spelling_no and orig_no require a simple look-up
    
    if ( $parameter eq 'orig' or $parameter eq 'spelling' )
    {
	$taxon = $dbh->selectrow_hashref("
		SELECT $INFO_EXPR
		FROM $AUTH_TABLE as a2 JOIN $TREE_TABLE as t USING (orig_no)
			JOIN $AUTH_TABLE as a ON a.taxon_no = t.${parameter}_no
			LEFT JOIN $OPINION_CACHE as o USING (opinion_no) 
		WHERE a2.taxon_no = ?", undef, $taxon_no + 0);
    }
    
    # Parameters synonym_no and parent_no require an extra join on $TREE_TABLE
    # to look up the current spelling.
    
    elsif ( $parameter eq 'synonym' or $parameter eq 'parent' )
    {
	$taxon = $dbh->selectrow_hashref("
		SELECT $INFO_EXPR
		FROM $AUTH_TABLE as a2 JOIN $TREE_TABLE as t2 USING (orig_no)
			JOIN $TREE_TABLE as t ON t.orig_no = t2.${parameter}_no
			JOIN $AUTH_TABLE as a ON a.taxon_no = t.spelling_no
			LEFT JOIN $OPINION_CACHE as o ON o.opinion_no = t.opinion_no
		WHERE a2.taxon_no = ?", undef, $taxon_no + 0);
    }
    
    # Parameter 'classification' requires the use of $OPINION_CACHE as well
    
    elsif ( $parameter eq 'classification' )
    {
	$taxon = $dbh->selectrow_hashref("
		SELECT $INFO_EXPR
		FROM $AUTH_TABLE as a2 JOIN $TREE_TABLE as t2 USING (orig_no)
			JOIN $OPINION_CACHE as o2 USING (opinion_no)
			JOIN $TREE_TABLE as t ON t.orig_no = o2.parent_no
			JOIN $AUTH_TABLE as a ON a.taxon_no = t.spelling_no
			LEFT JOIN $OPINION_CACHE as o ON o.opinion_no = t.opinion_no
		WHERE a2.taxon_no = ?", undef, $taxon_no + 0);
    }
    
    else
    {
	return error("invalid paramter '$parameter'");
    }
    
    if ( defined $taxon )
    {
	return bless $taxon, "Taxon";
    }
    
    elsif ( $dbh->err )
    {
	return error("database error: " . $dbh->errstr);
    }
    
    else
    {
	return error("taxon $taxon_no not found");
    }
}


# getTaxonIdParametrized ( dbh, taxon_no, parameter )
# 
# Returns the taxon identifier corresponding to the given parameter of the base
# taxon specified by taxon_no.  If an error occurs, returns the undefined
# value.  Valid parameters are:
# 
#    orig, spelling, synonym, parent
# 
# For the latter two, returns the current spelling of the indicated taxon.

sub getTaxonIdParametrized {
    
    my ($dbh, $taxon_no, $parameter) = @_;
    
    # Turn off the automatic error handling
    
    local($dbh->{RaiseError}) = 0;
    
    # Check arguments
    
    unless ( defined $taxon_no && $taxon_no > 0 )
    {
	error("invalid taxon_no '$taxon_no'");
	return;
    }
    
    # Fetch the requested information
    
    my ($value);
    
    # Parameters orig_no and spelling_no require a simple lookup.
    
    if ( $parameter eq 'orig' or $parameter eq 'spelling' )
    {
	($value) = $dbh->selectrow_array("
		SELECT t.${parameter}_no
		FROM $AUTH_TABLE as a2 JOIN $TREE_TABLE as t USING (orig_no)
		WHERE a2.taxon_no = ?", undef, $taxon_no + 0);
    }
    
    # Parameters synonym_no and parent_no require an extra join on $TREE_TABLE
    # to look up the spelling_no.
    
    elsif ( $parameter eq 'synonym' or $parameter eq 'parent' )
    {
	($value) = $dbh->selectrow_array("
		SELECT t.spelling_no
		FROM $AUTH_TABLE as a2 JOIN $TREE_TABLE as t2 USING (orig_no)
			JOIN $TREE_TABLE as t ON t.orig_no = t2.${parameter}_no
		WHERE a2.taxon_no = ?", undef, $taxon_no + 0);
    }
    
    # Parameter 'classification' requires the use of $OPINION_CACHE as well
    
    elsif ( $parameter eq 'classification' )
    {
	($value) = $dbh->selectrow_array("
		SELECT t.spelling_no
		FROM $AUTH_TABLE as a2 JOIN $TREE_TABLE as t2 USING (orig_no)
			JOIN $OPINION_CACHE o USING (opinion_no)
			JOIN $TREE_TABLE as t ON t.orig_no = o.parent_no
		WHERE a2.taxon_no = ?", undef, $taxon_no + 0);
    }
    
    else
    {
	error("invalid paramter '$parameter'");
	return;
    }
    
    # If we got a value, return it.
    
    if ($value)
    {
	return $value;
    }
    
    # If not, but an error occurred, note that.
    
    elsif ($dbh->err)
    {
	error("database error: " . $dbh->errstr);
	return;
    }
    
    # Otherwise, the indicated value does not exist.
    
    else
    {
	error("not found");
	return;
    }
}


# getTaxonListParametrized ( dbh, taxon_no, relationship )
# 
# Return a list of Taxon objects having the given
# relationship to the base taxon specified by taxon_no.  If an error occurs,
# returns an Error object instead.  Valid parameters are:
# 
#    'spelling', 'synonym', 'parent', 'child'
# 
# For all but the first of these, returns the current spelling of teach
# matching taxonomic concept.

sub getTaxonListParametrized {
    
    my ($dbh, $taxon_no, $parameter) = @_;
    
    # Turn off the automatic error handling
    
    local($dbh->{RaiseError}) = 0;
    
    # Check arguments
    
    unless ( defined $taxon_no && $taxon_no > 0 )
    {
	return error("invalid taxon_no '$taxon_no'");
    }
    
    # Fetch the requested information
    
    my ($result_list);
    
    # for parameter 'spelling', make sure to return the currently accepted
    # spelling first
    
    if ( $parameter eq 'spelling' )
    {
	$result_list = $dbh->selectall_arrayref("
		SELECT $INFO_EXPR
		FROM $AUTH_TABLE as a2 JOIN $AUTH_TABLE as a USING (orig_no)
			JOIN $TREE_TABLE as t USING (orig_no)
			LEFT JOIN $OPINION_CACHE as o USING (opinion_no)
		WHERE a2.taxon_no = ?
		ORDER BY if(a.taxon_no = t.spelling_no, 0, 1)", 
		{ Slice => {} }, $taxon_no + 0);
    }
    
    # for parameter 'synonym', make sure to return the most senior synonym first
    
    elsif ( $parameter eq 'synonym' )
    {
	$result_list = $dbh->selectall_arrayref("
		SELECT $INFO_EXPR
		FROM $AUTH_TABLE as a2 JOIN $TREE_TABLE as t2 USING (orig_no)
			JOIN $TREE_TABLE as t USING (synonym_no)
			JOIN $AUTH_TABLE as a ON a.taxon_no = t.spelling_no
			LEFT JOIN $OPINION_CACHE as o ON o.opinion_no = t.opinion_no
		WHERE a2.taxon_no = ?
		ORDER BY if(a.orig_no = t.synonym_no, 0, 1)", 
		{ Slice => {} }, $taxon_no + 0);
    }
    
    # for parameter 'parent', order the results by higher to lower taxa
    
    elsif ( $parameter eq 'parent' )
    {
	$result_list = $dbh->selectall_arrayref("
		SELECT $INFO_EXPR
		FROM $AUTH_TABLE as a2 JOIN $TREE_TABLE as t2 USING (orig_no)
			JOIN $TREE_TABLE as t ON t.lft <= t2.lft AND t.rgt >= t2.lft
			JOIN $AUTH_TABLE as a ON a.taxon_no = t.spelling_no
			LEFT JOIN $OPINION_CACHE as o ON o.opinion_no = t.opinion_no
		WHERE a2.taxon_no = ?
		ORDER BY t.lft",
		{ Slice => {} }, $taxon_no + 0);
    }
    
    # for parameter 'child', order the results by tree sequence
    
    elsif ( $parameter eq 'child' )
    {
	$result_list = $dbh->selectall_arrayref("
		SELECT $INFO_EXPR
		FROM $AUTH_TABLE as a2 JOIN $TREE_TABLE as t2 USING (orig_no)
			JOIN $TREE_TABLE as t ON t.lft > t2.lft AND t.lft <= t2.rgt
				AND t.depth = t2.depth + 1
			JOIN $AUTH_TABLE as a ON a.taxon_no = t.spelling_no
			LEFT JOIN $OPINION_CACHE as o ON o.opinion_no = t.opinion_no
		WHERE a2.taxon_no = ?
		ORDER BY t.lft", 
		{ Slice => {} }, $taxon_no + 0);
    }
    
    else
    {
	return error("invalid paramter '$parameter'");
    }
    
    # If an error occurred, note that.
    
    if ( $dbh->err )
    {
	return error("database error: " . $dbh->errstr);
    }
    
    # Otherwise, if we got no results (except for 'child') then we must have
    # had an invalid taxon.  Many taxa have no children, but all the other
    # parameters should return at least one result (the base taxon itself) for
    # a valid taxon_no.
    
    elsif ( scalar(@$result_list) == 0 and $parameter ne 'child' )
    {
	return error("taxon $taxon_no not found");
    }
    
    # Otherwise, we got results.  Bless all of the objects into the proper
    # package and return the list.
    
    else
    {
	foreach my $t (@$result_list)
	{
	    bless $t, "Taxon";
	}
	
	return @$result_list;
    } 
}


# getTaxonIdListParametrized ( dbh, taxon_no, parameter )
# 
# Returns a list of Taxon identifiers corresponding to the given
# parameter of the base taxon specified by taxon_no.  If an error occurs,
# returns an Error object instead.  Valid parameters are:
# 
#    'spelling', 'synonym', 'parent', 'child'
# 
# For all but the first of these, returns the current spelling of teach
# matching taxonomic concept.

sub getTaxonIdListParametrized {
    
    my ($dbh, $taxon_no, $parameter) = @_;
    
    # Turn off the automatic error handling
    
    local($dbh->{RaiseError}) = 0;
    
    # Check arguments
    
    unless ( defined $taxon_no && $taxon_no > 0 )
    {
	error("invalid taxon_no '$taxon_no'");
	return;
    }
    
    # Fetch the requested information
    
    my ($taxon_ids);
    
    # for parameter 'spelling', make sure to return the currently accepted
    # spelling first
    
    if ( $parameter eq 'spelling' )
    {
	$taxon_ids = $dbh->selectcol_arrayref("
		SELECT a.taxon_no
		FROM $AUTH_TABLE as a2 JOIN $AUTH_TABLE as a USING (orig_no)
			JOIN $TREE_TABLE as t USING (orig_no)
		WHERE a2.taxon_no = ?
		ORDER BY if(a.taxon_no = t.spelling_no, 0, 1)", undef, $taxon_no + 0);
    }
    
    # for parameter 'synonym', make sure to return the most senior synonym first
    
    elsif ( $parameter eq 'synonym' )
    {
	$taxon_ids = $dbh->selectcol_arrayref("
		SELECT a.taxon_no
		FROM $AUTH_TABLE as a2 JOIN $TREE_TABLE as t2 USING (orig_no)
			JOIN $TREE_TABLE as t USING (synonym_no)
			JOIN $AUTH_TABLE as a ON a.taxon_no = t.spelling_no
		WHERE a2.taxon_no = ?
		ORDER BY if(a.orig_no = t.synonym_no, 0, 1)", undef, $taxon_no + 0);
    }
    
    # for parameter 'parent', order the results by higher to lower taxa
    
    elsif ( $parameter eq 'parent' )
    {
	$taxon_ids = $dbh->selectcol_arrayref("
		SELECT a.taxon_no
		FROM $AUTH_TABLE as a2 JOIN $TREE_TABLE as t2 USING (orig_no)
			JOIN $TREE_TABLE as t ON t.lft <= t2.lft AND t.rgt >= t2.lft
			JOIN $AUTH_TABLE as a ON a.taxon_no = t.spelling_no
		WHERE a2.taxon_no = ?
		ORDER BY t.lft", undef, $taxon_no + 0);
    }
    
    # for parameter 'child', order the results by tree sequence
    
    elsif ( $parameter eq 'child' )
    {
	$taxon_ids = $dbh->selectcol_arrayref("
		SELECT a.taxon_no
		FROM $AUTH_TABLE as a2 JOIN $TREE_TABLE as t2 USING (orig_no)
			JOIN $TREE_TABLE as t ON t.lft > t2.lft AND t.lft <= t2.rgt
				AND t.depth = t2.depth + 1
			JOIN $AUTH_TABLE as a ON a.taxon_no = t.spelling_no
		WHERE a2.taxon_no = ?
		ORDER BY t.lft", undef, $taxon_no + 0);
    }
    
    else
    {
	error("invalid paramter '$parameter'");
	return;
    }
    
    # If an error occurred, note that.
    
    if ( $dbh->err )
    {
	error("database error: " . $dbh->errstr);
	return;
    }
    
    # Otherwise, if we got no results (except for 'child') then we must have
    # had an invalid taxon.  Many taxa have no children, but all the other
    # parameters should return at least one result (the base taxon itself) for
    # a valid taxon_no.
    
    elsif ( scalar(@$taxon_ids) == 0 and $parameter ne 'child' )
    {
	error("taxon $taxon_no not found");
	return;
    }
    
    # Otherwise, we got results.
    
    else
    {
	return @$taxon_ids;
    } 
}


=item getHistory ( dbh, taxon_no )

Returns a list of objects representing all taxa which have had the same
conceptual meaning as the given taxon at some point in history.  These are
sorted by publication date, and include some but not all synonyms (i.e. not
invalid subgroups of the given taxon).

=cut

sub getHistory {
    
    my ($dbh, $taxon_no) = @_;
    
    # Turn off the automatic error handling
    
    local($dbh->{RaiseError}) = 0;
    local($dbh->{PrintError}) = 0;
    
    # Check arguments
    
    unless ( defined $taxon_no && $taxon_no > 0 )
    {
	return error("invalid taxon_no '$taxon_no'");
    }
    
    # Then get the orig_no corresponding to the taxon we were given
    
    my ($orig_no) = getTaxonIdParametrized($dbh, $taxon_no, 'orig')
	or return error("taxon $taxon_no not found");
    
    # Then chain backward to find all related taxa.  This may be a tree, not
    # necessarily a linear list, so we do a breadth-first search with
    # @related_id_list as the search queue.
    
    my (@related_id_list) = $orig_no;
    
    my ($backward_sth) = $dbh->prepare("
		SELECT t.orig_no
		FROM $TREE_TABLE as t JOIN $OPINION_CACHE o USING (opinion_no)
		WHERE o.status in ('subjective synonym of', 'objective synonym of',
				   'replaced by') and o.parent_no = ?")
	
	or return error("database error: " . $dbh->errstr);
    
    foreach my $t (@related_id_list)
    {
	push @related_id_list, $dbh->selectrow_array($backward_sth, undef, $t);
    }
    
    # Then chain forward to find all related taxa.
    
    my ($forward_sth) = $dbh->prepare("
		SELECT o.parent_no
		FROM $TREE_TABLE as t JOIN $OPINION_CACHE o USING (opinion_no)
		WHERE o.status in ('subjective synonym of', 'objective synonym of',
				   'replaced by') and t.orig_no = ?")
    
	or return error("database error: " . $dbh->errstr);
    
    my ($t) = $orig_no;
    
    while ($t)
    {
	($t) = $dbh->selectrow_array($forward_sth, undef, $t);
	push @related_id_list, $t if $t;
    }
    
    # Now, fetch all of the indicated taxa.
    
    my ($taxon_filter) = '(' . join(', ', @related_id_list) . ')';
    
    my (@taxa) = $dbh->selectall_arrayref("
		SELECT $INFO_EXPR,
			if(a.pubyr IS NOT NULL AND a.pubyr != '', a.pubyr, r.pubyr) as pubyr
		FROM $TREE_TABLE as t JOIN $AUTH_TABLE as a ON a.orig_no = t.orig_no
			LEFT JOIN $OPINION_CACHE as o USING (opinion_no)
			LEFT JOIN $REFS_TABLE as r USING (reference_no)
		WHERE t.orig_no in $taxon_filter
		ORDER BY pubyr ASC", { Slice => {} });
    
    # If an error occurred, report it.
    
    if ( $dbh->err )
    {
	return error("database error: " . $dbh->errstr);
    }
    
    # Otherwise, we got results.  Bless all of the objects into the proper
    # package and return the list.
    
    else
    {
	foreach my $t (@taxa)
	{
	    bless $t, "Taxon";
	}
	
	return @taxa;
    } 
}


our($ERRSTR);

# error ( string )
# 
# Create an error object and return it.  Also save the error message for later
# querying. 

sub error {
    
    my ($msg) = @_;
    
    $ERRSTR = $msg;
    
    return bless { error => $msg }, "Error";
}


# errstr ( string )
# 
# Return the error message generated by the last operation.

sub errstr {
    
    return $ERRSTR;
}

1;
