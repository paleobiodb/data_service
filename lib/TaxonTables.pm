# 
# The Paleobiology Database
# 
#   TaxonTables.pm
# 

package TaxonTables;

use strict;

# Modules needed

use Carp qw(carp croak);
use Try::Tiny;

use TaxonDefs qw(@TREE_TABLE_LIST %TAXON_TABLE $CLASSIC_TREE_CACHE $CLASSIC_LIST_CACHE);
use TaxonPics qw(selectPics $TAXON_PICS $PHYLOPICS);

use CoreFunction qw(activateTables);
use ConsoleLog qw(initMessages logMessage);
use TableDefs qw($OCC_MATRIX $OCC_TAXON);

use base 'Exporter';

our (@EXPORT_OK) = qw(buildTaxonTables buildTaxaCacheTables populateOrig computeGenSp rebuildAttrsTable);


=head1 NAME

TaxonTables

=head1 SYNOPSIS

This module builds and maintains one or more hierarchies of taxonomic
concepts.  These hierarchies are based on the data in the C<opinions> and
C<authorities> tables, and are stored in C<taxon_trees> and other tables.  The
set of taxonomic names known to the database is stored in the C<authorities>
table, with primary key C<taxon_no>.  The taxon numbers from C<authorities>
are used extensively as foreign keys throughout the rest of the database,
because the taxonomic hierarchy is central to the organization of the entire
data set.

=head2 DEFINITIONS

For an explanation of the table structure, see L<Taxonomy.pm|./Taxonomy.pm>.

=head3 ALGORITHM

The algorithm for building or rebuilding a taxonomic hierarchy is as follows.
Note that this is an overview only, and you must inspect this module's code
and the accompanying comments for a full understanding.  Note also that the
taxonomic concept relation is stored in C<authorities> and so is not part of
the algorithm.  The procedures for editing entries in C<authorities> include
a mechanism for moving a taxonomic name from one concept group to another.

=over 4

=item 1. compute the accepted name relation

This is done by sorting the opinions by reliability and publication year, and
then selecting for each taxonomic concept the most reliable and recent opinion
which classifies it.  Whichever taxonomic name is used in that opinion will be
considered the accepted name, unless it is marked by some other opinion as a
misspelling.  In that case, we use the first valid name from the following
list.  In all cases, where more than one name is found, we choose the most
recently published one and break ties by choosing the most reliable opinion.

=over 4

=item a.

If a 'misspelling of' opinion exists for the selected name and the preferred
name indicated by that opinion is not elsewhere marked as a misspelling, use
that.

=item b.

From all of the other spellings found for this taxonomic concept, if any of them is
not elsewhere marked as a misspelling, use that.

=item c.

For each of the other spellings found for this taxonomic concept, count the number of
times it is mentioned in any opinion as a correct spelling, and subtract the
number of times it is mentioned in any opinion as a misspelling.  If any of
the spellings have a positive score, choose the one with the highest score.

=item d.

If no valid names have been found, use the original spelling even if some
opinion considers it to be a misspelling.

=back

=item 2. compute the synonymy relation

This is done by again choosing for each taxonomic concept the most reliable
and recent opinion which classifies it.  We then look at the subset of these
classification opinions that indicate a synonymy relationship.  This set of
opinions defines a directed graph, and we check to see if that graph contains
any cycles.  If it does, then for each cycle we find the most reliable and
recent opinion in that cycle, suppress whichever opinion conflicts with it,
and recompute the graph.  This step is repeated until no cycles remain.  The
synonymy relation is then computed for each taxonomic concept by starting with
the corresponding node and following the graph until it ends.

=item 3. compute the hierarchy relation

This is done by grouping the taxonomic concepts into synonym groups using the
synonymy relation computed above.  For each group, the most reliable and
recent opinion from among all of the 'belongs to' opinions that classify
members of the group is chosen to be the classification opinion for that
group.  This set of opinions defines a directed graph, and so we apply the
same procedure as above.  We check to see if the graph contains any cycles,
and if it does we look for the most reliable and recent opinion on each cycle
and suppress any conflicting opinion, then recompute the graph.  This step is
repeated until no cycles remain.  The hierarchy relation can then be read
directly off of the graph.

=item 4. sequence the tree

This is done using a standard preorder traversal of the hierarchy.  We start
with the tree rooted at the taxon "Eukaryota", and then continue through all
other taxonomic concepts which are not part of that tree.  As noted above, we
compute 'lft', 'rgt' and 'depth' for each taxonomic concept.

=item 5. compute the search table

This is done by creating entries for all of the known taxonomic names, and
then adding additional entries so that each species is listed under both its
genus and its subgenus if any, and also under all synonymous genera and
subgenera.  The idea is to allow maximum flexibility in searching for a
species name even if one knows only a partial classification or even only an
obsolete one.

=item 6. compute the derived attributes

Once the hierarchy relation has been computed, we can use that to compute the
hierarchically derived attributes.  We start with a bottom-up pass, which is
sufficient to compute 'minimum_body_mass' and 'maximum_body_mass' for all
higher taxa that contain at least one taxon having a value for these
attributes.  This pass also fills in 'yes' values on higher taxa for the
attribute 'extant'.  We then follow with a top-down pass, to fill in 'no'
values on lower taxa for the attribute 'extant'.  Because we do the passes in
this order, 'yes' values always trump 'no' values.

Note that the algorithm described in this section comes in two closely-related
versions: one for completely building the hierarchy from scratch, and one for
adjusting the hierarchy when the set of taxonomic names and opinions has
changed in any way.  The complete rebuild algorithm is more complete, and so
it should be invoked automatically at least once per day.  The adjustment
algorithm should be invoked immediately whenever authorities or opinions are
added, edited or removed, to give a good approximation of what the adjusted
hierarchy should look like.

=cut

# Main table names - if new entries are ever added to @TREE_TABLE_LIST,
# corresponding entries should be added to the hashes immediately below.  Each
# new tree table should have its own distinct name table, attrs table, etc.
# They could either point to the same authorities, opinions, etc. tables or to
# different ones depending upon the purpose for adding them to the system.

# Working table names - when the taxonomy tables are rebuilt, they are rebuilt
# using the following table names.  The last step of the rebuild is to replace
# the existing tables with the new ones in a single operation.  When the
# tables are updated, these names are used to hold the update entries.  The
# last step of the update operation is to insert these entries into the main
# tables.

our $TREE_WORK = "tn";
our $NAME_WORK = "nn";
our $ATTRS_WORK = "vn";
our $SEARCH_WORK = "sn";
our $INTS_WORK = "phyn";
our $LOWER_WORK = "lown";
our $COUNTS_WORK = "cntn";
our $OPINION_WORK = "opn";
our $TREE_CACHE_WORK = "ttcn";
our $LIST_CACHE_WORK = "tlcn";

# Auxiliary table names - these tables are creating during the process of
# computing the main tables, and then discarded.

our $SPELLING_AUX = "spelling_aux";
our $MISSPELLING_AUX = "misspelling_aux";
our $TRAD_AUX = "trad_aux";
our $SYNONYM_AUX = "synonym_aux";
our $CLASSIFY_AUX = "class_aux";
our $ADJUST_AUX = "adjust_aux";
our $SPECIES_AUX = "species_aux";
our $INTS_AUX = "ints_aux";

# Additional tables

our $QUEUE_TABLE = "taxon_queue";

# Other variables and constants

our (@TREE_ERRORS);
my ($REPORT_THRESHOLD) = 20;

our ($SYNONYM_STATUS) = "'subjective synonym of', 'objective synonym of', 'misspelling of', 'replaced by'";
our ($VALID_STATUS) = "'belongs to', 'subjective synonym of', 'objective synonym of', 'misspelling of', 'replaced by'";

# This is used for proper signal handling

my ($ABORT) = 0;

# Constants used with $QUEUE_TABLE

use constant TYPE_CONCEPT => 1;
use constant TYPE_OPINION => 2;
use constant TYPE_REBUILD => 3;
use constant TYPE_CHECK => 4;
use constant TYPE_ERROR => 9;

# This holds the last SQL command issued

our ($SQL_STRING);

=head1 INTERFACE

The interface to this module is as follows (note: I<this is a draft
specification>).  In the following documentation, the parameter C<dbh> is
always a database handle.

=head2 Action routines

The following routines are used to request that certain actions be taken by
the maintenance thread.  This mechanism is used in order to simplify the code
for updating, rebuilding and checking the taxon tables.  By ensuring that only
one thread will be writing to these tables at any given time, we avoid having
to mess around with table write locks.

Each routine returns a "request identifier" that can then be passed to
C<requestStatus> to determine whether or not the request has been carried
out. If necessary, C<requestStatus> can be called repeatedly until it returns
an affirmative result.

=head3 build ( dbh )

Requests a rebuild of the taxon trees.  This will be carried out by the
maintenance thread at the next opportunity.  Returns a request id that can be
passed to requestStatus() in order to determine whether the request has been
carried out.

=cut

sub build {

    my ($dbh) = @_;
    
    my $rebuild_op = TYPE_REBUILD;
    
    $dbh->do("INSERT INTO $QUEUE_TABLE (type, param, time)
	      VALUES ($rebuild_op, 0, now())");
    
    return $dbh->last_insert_id();
}


=head3 check ( dbh )

Requests a consistency check of the taxon trees.  This will be carried out by
the maintenance thread at the next opportunity.  Returns a request id that can
be passed to requestStatus() in order to determine whether the request has
been carried out.

=cut

sub check {

    my ($dbh) = @_;

    my $check_op = TYPE_CHECK;
    
    $dbh->do("INSERT INTO $QUEUE_TABLE (type, param, time)
	      VALUES ($check_op, 0, now())");
    
    return $dbh->last_insert_id();
}


=head3 updateConcepts ( dbh, orig_no ... )

Requests that the maintenance thread adjust the taxon tree tables to take
into account changes to the given concepts.  Some of the listed concepts may
disappear from the taxon tables, as may happen if two concepts are merged,
while others may need to be created.  This routine should be called whenever
C<orig_no> values in the C<authorities> table are adjusted, and both the old
and the new C<orig_no> values should be included in the argument list.

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


=head3 updateOpinions ( dbh, opinion_no ... )

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


=item requestStatus ( dbh, insert_id )

This routine can be called subsequently to update_concepts() or
update_opinions() to determine whether the requested updates have been
completed.  It returns a status value as follows:

=over 4

=item REQUEST_PENDING

The request has not yet been completed

=item REQUEST_COMPLETE

The request has been completed

=item REQUEST_ERROR

An error occurred while processing the update, so it was not completed.  In
this case, the error message is returned as the second item in the return
list. 

=item REQUEST_INVALID

The given id is not an integer greater than zero.

=back

Note that this routine will return REQUEST_COMPLETE if called with a parameter
that is not a return value from a previous call to one of the action routines.

=cut

sub requestStatus {

    my ($dbh, $insert_id) = @_;
    
    # First make sure that the parameter value is valid.    
    
    unless ( $insert_id > 0 )
    {
	return 'REQUEST_INVALID';
    }
    
    # Then query the queue table.
    
    my ($type, $time, $comment) = $dbh->selectrow_array("
		SELECT type, time, comment FROM $QUEUE_TABLE
		WHERE id = ?", undef, $insert_id + 0);
    
    # Return the appropriate code.  If we didn't find anything, we can assume
    # that the update has completed.
    
    if ( $type == TYPE_ERROR )
    {
	return ('REQUEST_ERROR', $comment);
    }
    
    elsif ( $type > 0 )
    {
	return ('REQUEST_PENDING');
    }
    
    else
    {
	return ('REQUEST_COMPLETE');
    }
}


=head3 maintenance ( dbh, options )

This routine should be called periodically (every few seconds) by the
taxon_trees maintenance daemon.  It checks the taxon_queue table for entries
and carries out the pending requests.  The taxon_trees tables should not be
written to except by this routine.

Options should be passed as a hash ref.  Valid keys are:

=over 4

=item msg_level

Override the default message level 

=item keep_temps

If true, do not delete temporary tables when done with a build.

=back

=cut

sub maintenance {
    
    my ($dbh, $options) = @_;
    
    $options ||= {};
    
    # The following variables keep track of what we are being asked to do.
    
    my (@id_list, @concept_list, @opinion_list, $rebuild_tables, 
	$check_tables);
    
    # Check $QUEUE_TABLE to see if any requests are pending.
    
    my $items = $dbh->prepare("SELECT id, type, param FROM $QUEUE_TABLE");
    
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
	}
	elsif ( $type == TYPE_CHECK )
	{
	    $check_tables = 1;
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
    
    # Also check to see if it is time for an automatic rebuild and if so then
    # trigger it.  We do an automatic rebuild every day at 9am Sydney time
    # (11pm GMT?), as long as it has been at least 18 hours since the last
    # rebuild for any reason.
    
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
	logMessage(1, "Rebuilding tree tables at " . gmtime() . " GMT");
	
	# Put a record into the queue table, so that any threads that are
	# tracking the status of update requests can inform their users
	# that a rebuild is going on.  A param value of 1 means "in
	# progress".
	
	my $rebuild_op = TYPE_REBUILD;
	
	$dbh->do("DELETE FROM $QUEUE_TABLE WHERE type = $rebuild_op");
	$dbh->do("INSERT INTO $QUEUE_TABLE (type, time, param)
			  VALUES ($rebuild_op, now(), 1)");
	
	# Now, rebuild each table in turn.

	foreach my $table (@TREE_TABLE_LIST)
	{
	    logMessage(1, "Rebuilding table '$table'");
	    
	    eval {
		buildTaxonTables($dbh, $table, $options);
	    };
	    
	    # If an error occurred, we need to note this.
	    
	    if ( $@ )
	    {
		logMessage(0, "Error: $@");
	    }
	    
	    logMessage(1, "Finished rebuild of '$table'");
	}
	
	logMessage(1,"Done with total rebuild at " . gmtime() . " GMT");
	
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
	logMessage(1, "Updating tree tables at " . gmtime() . " GMT");
	
	# Now, update each table in turn.
	
	foreach my $table (@TREE_TABLE_LIST)
	{
	    logMessage(1, "Updating table '$table'");
	    
	    eval {
		updateTables($dbh, $table, \@concept_list, \@opinion_list, $options);
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
	    
	    logMessage(1, "Finished update of '$table'");
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
	# that a check is going on.  A param value of 1 means "in
	# progress".
	
	my $check_op = TYPE_CHECK;
	
	$dbh->do("DELETE FROM $QUEUE_TABLE WHERE type = $check_op");
	$dbh->do("INSERT INTO $QUEUE_TABLE (type, time, param)
			  VALUES ($check_op, now(), 1)");
	
	foreach my $table (@TREE_TABLE_LIST)
	{
	    logMessage(1, "Checking table '$table'");
	    
	    eval {
		checkTables($dbh, $table, $options);
	    };
	    
	    # If an error occurred, we need to note this.
	    
	    if ( $@ )
	    {
		logMessage(0, "Error: $@");
	    }
	    
	    logMessage(1, "Finished check of '$table'");
	}
	
	logMessage(1,"Done with total check at " . gmtime() . " GMT");
	
	# Now, record when this check occurred, so that the next automatic
	# one won't come too soon.  A param value of 2 means "complete".
	# We also include the error message, if any.
	
	my $check_op = TYPE_CHECK;
	
	$dbh->do("DELETE FROM $QUEUE_TABLE WHERE type = $check_op");
	$dbh->do("INSERT INTO $QUEUE_TABLE (type, time, param, comment)
			  VALUES ($check_op, now(), 2, $@)");
    }
    
    # Now we're done, until we're called again.
    
    return;
}


# buildTaxonTables ( dbh, tree_table, options, msg_level )
# 
# Builds 'tree_table' and its associated tables from scratch, using only the
# information in $AUTH_TABLE and $OPINIONS_TABLE.  If the 'msg_level'
# parameter is given, it controls the verbosity of log messages produced.
# This routine should NEVER BE CALLED FROM OUTSIDE THIS MODULE when the
# database is live.  There is no concurrency control, and so race conditions
# and inconsistent updates may occur if you do that.
# 
# The $step_control parameter is for debugging only.  If specified it must be
# a hash reference that will control which steps are taken.

sub buildTaxonTables {

    my ($dbh, $tree_table, $options) = @_;
    
    $options ||= {};
    my $steps = $options->{taxon_steps};
    my $step_control;
    
    # First, determine which tables will be computed.
    
    my @steps = split(//, $steps || 'Aabcdefghi');
    $step_control->{$_} = 1 foreach @steps;
    
    $TREE_WORK = 'taxon_trees' unless $step_control->{a};
    
    # Now create the necessary tables, including generating the opinion cache
    # from the opinion table.
    
    buildOpinionCache($dbh, $tree_table) if $step_control->{A} && ! $options->{no_rebuild_cache};
    createWorkingTables($dbh, $tree_table) if $step_control->{a};
    
    # Next, determine the currently accepted spelling for each concept from
    # the data in the opinion cache, and the "spelling reason" for each
    # taxonomic name.
    
	clearSpelling($dbh) if $step_control->{x} and $step_control->{b};
    
    computeSpelling($dbh, $tree_table) if $step_control->{b};
    
    # Next, compute the synonymy relation from the data in the opinion cache.
    
	clearSynonymy($dbh) if $step_control->{x} and $step_control->{c};
    
    computeSynonymy($dbh, $tree_table) if $step_control->{c};
    
    # Update the synonymy relation so that synonym_no points to the most
    # senior synonym, instead of the immediate senior synonym.
    
    linkSynonyms($dbh, $tree_table) if $step_control->{c};
    
    # Next, compute the hierarchy relation from the data in the opinion cache.
    
	clearHierarchy($dbh) if $step_control->{x} and $step_control->{d};
    
    computeHierarchy($dbh, $tree_table) if $step_control->{d};
    
    # Update the taxon names stored in the tree table so that species,
    # subspecies and subgenus names match the genera under which they are
    # hierarchically placed.
    
    adjustHierarchicalNames($dbh, $tree_table) if $step_control->{d};
    
    # Next, sequence the taxon trees using the hierarchy relation.
    
	clearTreeSequence($dbh) if $step_control->{x} and $step_control->{e};
    
    computeTreeSequence($dbh, $tree_table) if $step_control->{e};
    
    # Next, compute the intermediate classification of each taxon: kingdom,
    # phylum, class, order, and family.
    
    computePhylogeny($dbh, $tree_table) if $step_control->{f};
    
    # Next, compute the name search table using the hierarchy relation.  At
    # this time we also update species and subgenus names stored in the tree
    # table.
    
    computeSearchTable($dbh, $tree_table) if $step_control->{g};
    
    # Next, compute the attributes table that keeps track of inherited
    # attributes such as extancy and mass ranges.
    
    computeAttrsTable($dbh, $tree_table) if $step_control->{h};
    
    # Finally, activate the new tables we have just computed by renaming them
    # over the previous ones.
    
    my $keep_temps = $step_control->{k} || $options->{keep_temps};
    
    activateNewTaxonomyTables($dbh, $tree_table, $keep_temps)
	if $step_control->{i};
    
    logMessage(1, "done building tree tables for '$tree_table'");
    
    my $a = 1;		# we can stop here when debugging
}


# updateTables ( dbh, table_name, concept_list, opinion_list, options )
# 
# This routine is called by maintenance() whenever opinions are created,
# edited, or deleted, or when concept membership in the authorities table is
# changed.  It should NEVER BE CALLED FROM OUTSIDE THIS MODULE when the
# database is live, because it has no concurrency control and so inconsistent
# updates and race conditions may occur.
# 
# The taxon tree tables will be updated to match the new state of authorities
# and opinions.  This involves first computing the list of taxonomic concepts
# that could possibly be affected by the change, and then recomputing the
# three organizing relations of "group leader", "synonymy" and "hierarchy" for
# those concepts.  Finally, the taxonomic tree is renumbered and the ancestor
# relationship adjusted to match.
# 
# The option 'msg_level', if given, specifies how verbose this routine will
# be in writing messages to the log.  It defaults to 1, which means a minimal
# message set.

sub updateTables {

    my ($dbh, $tree_table, $concept_list, $opinion_list, $options) = @_;
    
    $options ||= {};
    my $search_table = $TAXON_TABLE{$tree_table}{search};
    
    my %update_concepts;	# list of concepts to be updated
    
    # First, set the variables that control log output.
    
    #$MSG_TAG = 'Update'; $MSG_LEVEL = $options->{msg_level} || 1;
    
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
	
	updateOpinionConcepts($dbh, $tree_table, $concept_list);
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
	
	foreach my $t ( getOpinionConcepts($dbh, $tree_table, $opinion_list) )
	{
	    $update_concepts{$t} = 1;
	}
	
	logMessage(1, "notified opinions: " . 
	    join(', ', @$opinion_list) . "\n");
	
	updateOpinionCache($dbh, $tree_table, $opinion_list);
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
    
    # The rest of this routine updates the subset of $tree_table comprising
    # the concept groups listed in %update_concepts along with their junior
    # synonyms and children.
    
    # First create a temporary table to hold the new rows that will eventually
    # go into $tree_table.  To start with, we will need one row for each
    # concept in %update_concepts.  Create some auxiliary tables as well.
    
    createWorkingTables($dbh, $tree_table, \%update_concepts);
    
    # Next, compute the accepted name for every concept in $TREE_WORK and also
    # add corresponding entries to $NAME_WORK.
    
    computeSpelling($dbh, $tree_table);
    
    # Then compute the synonymy relation for every concept in $TREE_WORK.  In
    # the process, we need to expand $TREE_WORK to include junior synonyms.
    
    computeSynonymy($dbh, $tree_table, { expandToJuniors => 1 });
    
    # Now that we have computed the synonymy relation, we need to expand
    # $TREE_WORK to include senior synonyms of the concepts represented in it.
    # We need to do this before we update the hierarchy relation because the
    # classification of those senior synonyms might change; if one of the rows
    # in $TREE_WORK is a new junior synonym, it might have a 'belongs to'
    # opinion that is more recent and reliable than the previous best opinion
    # for the senior.
    
    expandToSeniors($dbh, $tree_table);
    
    # At this point we remove synonym chains, so that synonym_no always points
    # to the most senior synonym of each taxonomic concept.  This needs to be
    # done before we update the hierarchy relation, because that computation
    # depends on this property of synonym_no.
    
    linkSynonyms($dbh);
    
    # Then compute the hierarchy relation for every concept in $TREE_WORK.
    
    computeHierarchy($dbh);
    
    # Some parent_no values may not have been set properly, in particular
    # those whose classification points to a parent which is not itself in
    # $TREE_WORK.  These must now be updated to their proper values.
    
    # linkParents($dbh, $tree_table);     # This is no longer needed MM 2012-12-08
    
    # We can now update the search table.
    
    updateSearchTable($dbh, $search_table);
    
    # If the adjustments to the hierarchy are small, then we can now update
    # $tree_table using the rows from $TREE_WORK and then adjust the tree
    # sequence in-place.  This will finish the procedure.  We set the
    # threshold at 3 because altering the tree in-place is several times more
    # efficient than rebuilding it even if we have to scan over the entire
    # table multiple times (3 is actually just a guess...)
    
    if ( treePerturbation($dbh, $tree_table) < 3 )
    {
	updateTreeTable($dbh, $tree_table);
	updateSecondaryTables($dbh, $tree_table, $options->{keep_temps});
	logMessage(1, "done updating tree tables for '$tree_table'");
    }
    
    # Otherwise, we need to completely rebuild the tree sequence and then
    # activate the temporary table using atomic rename.
    
    else
    {
	# Copy all rows from $tree_table into $TREE_WORK that aren't already
	# represented there.
	
	my $result = $dbh->do("
		INSERT IGNORE INTO $TREE_WORK
		SELECT * FROM $tree_table");
	
	# Now that $TREE_WORK has the entire tree, resequence it.
	
	computeTreeSequence($dbh);
	
	# Then activate the new tree table by renaming it over the previous
	# one.  The secondary tables get updated as usual.
	
	activateTreeTable($dbh, $tree_table);
	updateSecondaryTables($dbh, $tree_table, $options->{keep_temps});
	logMessage(1, "done updating tree tables for '$tree_table'");
    }
    
    my $a = 1;		# we can stop here when debugging
}


# buildOpinionCache ( dbh )
# 
# Build the opinion cache completely from the opinions table.

sub buildOpinionCache {
    
    my ($dbh, $tree_table) = @_;
    
    my ($result);
    
    logMessage(2, "building opinion cache (a)");
    
    my $OPINION_CACHE = $TAXON_TABLE{$tree_table}{opcache};
    
    # In order to minimize interference with any other threads which might
    # need to access the opinion cache, we create a new table and then rename
    # it into the old one.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $OPINION_WORK");
    $result = $dbh->do("CREATE TABLE $OPINION_WORK
			  (opinion_no int unsigned not null,
			   orig_no int unsigned not null,
			   child_spelling_no int unsigned not null,
			   parent_no int unsigned not null,
			   parent_spelling_no int unsigned not null,
			   ri int not null,
			   pubyr varchar(4),
			   status enum('belongs to','subjective synonym of','objective synonym of','invalid subgroup of','misspelling of','replaced by','nomen dubium','nomen nudum','nomen oblitum','nomen vanum'),
			   spelling_reason enum('original spelling','recombination','reassignment','correction','rank change','misspelling'),
			   reference_no int unsigned not null,
			   suppress boolean,
			   UNIQUE KEY (opinion_no),
			   KEY (suppress)) ENGINE=MYISAM");
    
    # Populate this table with data from $OPINIONS_TABLE.  We will sort this
    # data properly for determining the best (most recent and reliable)
    # opinions, since this will speed up subsequent steps of the taxon trees
    # rebuild/update process.
    
    populateOpinionCache($dbh, $OPINION_WORK, $tree_table);
    
    # Then index the newly populated opinion cache.
    
    $result = $dbh->do("ALTER TABLE $OPINION_WORK ADD KEY (orig_no)");
    $result = $dbh->do("ALTER TABLE $OPINION_WORK ADD KEY (child_spelling_no)");
    $result = $dbh->do("ALTER TABLE $OPINION_WORK ADD KEY (parent_no)");
    $result = $dbh->do("ALTER TABLE $OPINION_WORK ADD KEY (parent_spelling_no)");
    
    # Now, we remove any backup table that might have been left in place, and
    # swap in the new table using an atomic rename operation
    
    $result = $dbh->do("DROP TABLE IF EXISTS ${OPINION_CACHE}_bak");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $OPINION_CACHE LIKE $OPINION_WORK");
    
    $result = $dbh->do("RENAME TABLE
				$OPINION_CACHE to ${OPINION_CACHE}_bak,
				$OPINION_WORK to $OPINION_CACHE");
    
    # ...and remove the backup
    
    $result = $dbh->do("DROP TABLE ${OPINION_CACHE}_bak");
    
    my $a = 1;		# we can stop here when debugging
}


# updateOpinionCache ( dbh, opinion_list )
# 
# Copy the indicated opinion data from the opinions table to the opinion
# cache.

sub updateOpinionCache {
    
    my ($dbh, $tree_table, $opinion_list) = @_;
    
    my $result;
    
    # First delete the old opinion data from $OPINION_CACHE and insert the new.
    # We have to explicitly delete because an opinion might have been deleted,
    # which means there would be no new row for that opinion_no.
    
    # Note that $OPINION_CACHE will not be correctly ordered after this, so we
    # cannot rely on its order during the rest of the update procedure.
    
    my $opfilter = join ',', @$opinion_list;
    
    $result = $dbh->do("LOCK TABLE $TAXON_TABLE{$tree_table}{opinions} as o read,
				   $TAXON_TABLE{$tree_table}{refs} as r read,
				   $TAXON_TABLE{$tree_table}{authorities} as a1 read,
				   $TAXON_TABLE{$tree_table}{authorities} as a2 read,
				   $TAXON_TABLE{$tree_table}{opcache} write");
    
    $result = $dbh->do("DELETE FROM $TAXON_TABLE{$tree_table}{opcache} WHERE opinion_no in ($opfilter)");
    
    populateOpinionCache($dbh, $TAXON_TABLE{$tree_table}{opcache}, $tree_table, $opfilter);
    
    $result = $dbh->do("UNLOCK TABLES");
    
    my $a = 1;		# we can stop here when debugging
}


# populateOpinionCache ( dbh, table_name, auth_table, opinions_table, refs_table, opinion_list )
# 
# Insert records into the opinion cache table, under the given table name.  If
# $opinion_list is given, then it should be a string containing a
# comma-separated list of opinion numbers to insert.  The results are sorted
# in the order necessary for selecting the most reliable and recent opinions.

sub populateOpinionCache {

    my ($dbh, $table_name, $tree_table, $opinion_list) = @_;
    
    my ($result);
    
    # If we were given a filter expression, create the necessary clause.
    
    my $filter_clause = $opinion_list ?
	"WHERE opinion_no in ($opinion_list) and (parent_no > 0 or child_no = 1)" :
	    "WHERE parent_no > 0 or child_no = 1";
    
    # This query is adapated from the old getMostRecentClassification()
    # routine, from TaxonInfo.pm line 2003.  We have to join with authorities
    # twice to look up the original combination (taxonomic concept id) of both
    # child and parent.  The authorities table is the canonical source of that
    # information, not the opinions table.
    
    $result = $dbh->do("INSERT INTO $table_name
		SELECT o.opinion_no, a1.orig_no,
			if(o.child_spelling_no > 0, o.child_spelling_no, o.child_no), 
			a2.orig_no,
			if(o.parent_spelling_no > 0, o.parent_spelling_no, o.parent_no),
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
			o.status, o.spelling_reason, o.reference_no, null
		FROM $TAXON_TABLE{$tree_table}{opinions} as o
			LEFT JOIN $TAXON_TABLE{$tree_table}{refs} as r using (reference_no)
			JOIN $TAXON_TABLE{$tree_table}{authorities} as a1
				on a1.taxon_no = if(o.child_spelling_no > 0, o.child_spelling_no, o.child_no)
			LEFT JOIN $TAXON_TABLE{$tree_table}{authorities} as a2
				on a2.taxon_no = if(o.parent_spelling_no > 0, o.parent_spelling_no, o.parent_no)
		$filter_clause
		ORDER BY ri DESC, pubyr DESC, opinion_no DESC");
    
    return;
}


# getOpinionConcepts ( dbh, tree_table, opinion_list )
# 
# Given a list of changed opinions, return the union of the set of concepts
# referred to by both the new versions (from the opinions table) and the old
# versions (from the opinion cache, which has not been modified since the last
# rebuild of the taxonomy tables).

sub getOpinionConcepts {

    my ($dbh, $tree_table, $opinion_list) = @_;
    
    my (%update_concepts);
    
    # First fetch the updated opinions, and figure out the "original
    # combination" mentioned in each one.
    
    my $opfilter = '(' . join(',', @$opinion_list) . ')';
    
    my $new_op_data = $dbh->prepare("
		SELECT child_no, parent_no
		FROM $TAXON_TABLE{$tree_table}{opinions} WHERE opinion_no in $opfilter");
    
    $new_op_data->execute();
    
    while ( my ($child_orig, $parent_orig) = $new_op_data->fetchrow_array() )
    {
	$update_concepts{$child_orig} = 1 if $child_orig > 0;
	$update_concepts{$parent_orig} = 1 if $parent_orig > 0;
    }
    
    # Now do the same with the corresponding old opinion records.
    
    my $old_op_data = $dbh->prepare("
		SELECT orig_no, parent_no
		FROM $TAXON_TABLE{$tree_table}{opcache} WHERE opinion_no in $opfilter");
    
    $old_op_data->execute();
    
    while ( my ($child_orig, $parent_orig) = $old_op_data->fetchrow_array() )
    {
	$update_concepts{$child_orig} = 1 if $child_orig > 0;
	$update_concepts{$parent_orig} = 1 if $parent_orig > 0;
    }
    
    return keys %update_concepts;
}


# updateOpinionConcepts ( dbh, tree_table, concept_list )
# 
# This routine updates all of the orig_no, child_no and parent_no values in
# $OPINIONS_TABLE and $OPINION_CACHE that fall within the given list.

sub updateOpinionConcepts {

    my ($dbh, $tree_table, $concept_list) = @_;
    
    my $concept_filter = join(',', @$concept_list);
    
    my $result;
    
    my $auth_table = $TAXON_TABLE{$tree_table}{authorities};
    my $opinion_cache = $TAXON_TABLE{$tree_table}{opcache};
    my $opinion_table = $TAXON_TABLE{$tree_table}{opinions};
    
    logMessage(2, "updating opinion cache to reflect concept changes");
    
    # First, $OPINION_CACHE
    
    $result = $dbh->do("UPDATE $opinion_table as o
				JOIN $auth_table as a on a.taxon_no = o.child_spelling_no
			SET o.orig_no = a.orig_no, o.modified = o.modified
			WHERE o.orig_no in ($concept_filter)");
    
    $result = $dbh->do("UPDATE $opinion_table as o
				JOIN $auth_table as a on a.taxon_no = o.parent_spelling_no
			SET o.parent_no = a.orig_no, o.modified = o.modified
			WHERE o.parent_no in ($concept_filter)");
    
    # Next, $OPINIONS_TABLE
    
    $result = $dbh->do("UPDATE $opinion_table as o
				JOIN $auth_table as a on a.taxon_no = o.child_spelling_no
			SET o.child_no = a.orig_no, o.modified = o.modified
			WHERE a.orig_no in ($concept_filter)");
    
    $result = $dbh->do("UPDATE $opinion_table as o
				JOIN $auth_table as a on a.taxon_no = o.parent_spelling_no
			SET o.parent_no = a.orig_no, o.modified = o.modified
			WHERE a.orig_no in ($concept_filter)");
    
    return;
}


# createWorkingTables ( dbh, tree_table, concept_hash )
# 
# Create a new $TREE_WORK table to hold the new rows that are being computed
# for the tree table being updated.  If $concept_hash is specified, it must be
# a hashref whose keys represent a list of taxonomic concepts.  Otherwise,
# $TREE_WORK will be created with one row for every concept in the database.
# The new table doesn't have any indices yet, because it is more efficient to
# add these later.
# 
# We also create the secondary tables associated with $TREE_WORK.

sub createWorkingTables {

    my ($dbh, $tree_table, $concept_hash) = @_;
    
    my ($result);
    
    logMessage(2, "creating working tables (a)");
    
    my $auth_table = $TAXON_TABLE{$tree_table}{authorities};
    
    # First create $TREE_WORK, which will hold one row for every concept that
    # is being updated.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $TREE_WORK");
    $result = $dbh->do("CREATE TABLE $TREE_WORK 
			       (orig_no int unsigned not null,
				name varchar(80) not null,
				imp boolean not null,
				rank tinyint not null,
				opinion_rank tinyint not null,
				status enum('belongs to','subjective synonym of','objective synonym of','invalid subgroup of','misspelling of','replaced by','nomen dubium','nomen nudum','nomen oblitum','nomen vanum'),
				spelling_no int unsigned not null,
				trad_no int unsigned not null,
				synonym_no int unsigned not null,
				parent_no int unsigned not null,
				parsen_no int unsigned not null,
				opinion_no int unsigned not null,
				species_no int unsigned,
				genus_no int unsigned,
				ints_no int unsigned not null,
				lft int,
				rgt int,
				bound int,
				depth int) ENGINE=MYISAM");
    
    # If we were given a list of concepts, populate it with just those.
    # Otherwise, grab every concept in $AUTH_TABLE
    
    my $concept_filter = '';
    
    if ( ref $concept_hash eq 'HASH' )
    {
	$concept_filter = 'WHERE orig_no in (' . 
	    join(',', keys %$concept_hash) . ')';
    }
	
    $result = $dbh->do("INSERT INTO $TREE_WORK (orig_no)
			SELECT distinct orig_no
			FROM $auth_table $concept_filter");
    
    $result = $dbh->do("ALTER TABLE $TREE_WORK ADD PRIMARY KEY (orig_no)");
    
    # Create a table to store the spelling information for each taxonomic
    # name. 
    
    $result = $dbh->do("DROP TABLE IF EXISTS $NAME_WORK");
    $result = $dbh->do("CREATE TABLE $NAME_WORK
			       (taxon_no int unsigned not null,
				orig_no int unsigned not null,
				spelling_reason enum('original spelling','recombination','reassignment','correction','rank change','misspelling'),
				opinion_no int unsigned not null,
				PRIMARY KEY (taxon_no)) ENGINE=MYISAM");
    
    return;
}


# computeSpelling ( dbh, tree_table )
# 
# Fill in the spelling_no field of $TREE_WORK.  This computes the "currently
# accepted name" or "concept group leader" relation for all taxonomic concepts
# represented in $TREE_WORK.  We do this by selecting the best "spelling
# opinion" (most recent and reliable opinion, including those labelled
# "misspelling") for each concept group.  Unless the best opinion is recorded
# as a misspelling, we take its spelling to be the accepted one.  Otherwise,
# we look for the best spelling match among the available opinions.
# 
# We then use this information fill in the taxon_name field of $TREE_WORK,
# although some of these values will be modified later.

sub computeSpelling {

    my ($dbh, $tree_table) = @_;
    
    my ($result);
    
    logMessage(2, "computing currently accepted spelling relation (b)");
    
    my $opinion_cache = $TAXON_TABLE{$tree_table}{opcache};
    my $auth_table = $TAXON_TABLE{$tree_table}{authorities};
    
    # In order to select the currently accepted name for each taxonomic
    # concept, we first need to determine which taxonomic names are marked as
    # misspellings by some (any) opinion in the database.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $MISSPELLING_AUX");
    $result = $dbh->do("CREATE TABLE $MISSPELLING_AUX
			   (spelling_no int unsigned,
			    PRIMARY KEY (spelling_no)) ENGINE=MYISAM");
    
    $result = $dbh->do("
		INSERT IGNORE INTO $MISSPELLING_AUX
		SELECT o.child_spelling_no FROM $opinion_cache as o
		WHERE spelling_reason = 'misspelling'");
    
    # Now, in order to select the currently accepted name for each taxonomic
    # concept, we start by choosing the spelling associated with the most
    # recent and reliable opinion for each concept group.  This "spelling
    # opinion" may differ from the "classification opinion" computed below,
    # because we select from all opinions including those marked as
    # "misspelling" and those with parent_no = child_no.
    
    # We choose a single spelling for each taxonomic concept by defining a
    # temporary table with a unique key on orig_no and using INSERT IGNORE
    # with a properly ordered selection.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SPELLING_AUX");
    $result = $dbh->do("CREATE TABLE $SPELLING_AUX
			   (orig_no int unsigned,
			    spelling_no int unsigned,
			    opinion_no int unsigned,
			    is_misspelling boolean,
			    PRIMARY KEY (orig_no)) ENGINE=MYISAM");
    
    $result = $dbh->do("
		INSERT IGNORE INTO $SPELLING_AUX
		SELECT o.orig_no, o.child_spelling_no, o.opinion_no,
		       if(o.spelling_reason = 'misspelling' or m.spelling_no is not null,
			  true, false)
		FROM $opinion_cache as o JOIN $TREE_WORK USING (orig_no)
			LEFT JOIN $MISSPELLING_AUX as m on o.child_spelling_no = m.spelling_no
		ORDER BY o.ri DESC, o.pubyr DESC, o.opinion_no DESC");
    
    # The next step is to fix those entries where the name indicated by the
    # best opinion is marked as a misspelling.  We can fix some of them by
    # looking for matching 'misspelling of' opinions where the indicated
    # correct spelling is not elsewhere marked as a misspelling.  We order by
    # pubyr first, breaking ties by reliabilty index and then by opinion_no.
    # The order is the opposite of what we used above, because we are
    # replacing rather than ignoring.
    
    $result = $dbh->do("
		REPLACE INTO $SPELLING_AUX
		SELECT s.orig_no, o.parent_spelling_no, o.opinion_no, false
		FROM $SPELLING_AUX as s JOIN $opinion_cache as o using (orig_no)
			LEFT JOIN $MISSPELLING_AUX as m on o.parent_spelling_no = m.spelling_no
		WHERE s.is_misspelling and o.status = 'misspelling of' and m.spelling_no is null
		ORDER BY o.pubyr ASC, o.ri ASC, o.opinion_no ASC");
    
    # We can fix a few more by looking through all of the relevant opinions
    # for alternate names that are nowhere marked as misspellings.
    
    $result = $dbh->do("
		REPLACE INTO $SPELLING_AUX
		SELECT s.orig_no, o.child_spelling_no, o.opinion_no, false
		FROM $SPELLING_AUX as s JOIN $opinion_cache as o using (orig_no)
			LEFT JOIN $MISSPELLING_AUX as m on o.child_spelling_no = m.spelling_no
		WHERE s.is_misspelling and o.spelling_reason in ('correction', 'rank change',
				'recombination', 'reassignment')
			and m.spelling_no is null
		ORDER BY o.pubyr ASC, o.ri ASC, o.opinion_no ASC");
    
    # For those taxonomic concepts which still don't have a good spelling, we
    # need to try a different approach.  We grab all of the possible names for
    # these concepts, and compute a score for each by adding +1 for each
    # opinion in which the name is considered to be a good spelling and -1 for
    # each opinion in which it is considered to be a bad spelling.  We can
    # then choose the best of these for each concept.
    
    my ($SPELLING_SCORE) = "spelling_score";
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SPELLING_SCORE");
    $result = $dbh->do("CREATE TABLE $SPELLING_SCORE
			   (spelling_no int unsigned,
			    orig_no int unsigned,
			    score int,
			    PRIMARY KEY (spelling_no)) ENGINE=MYISAM");
    
    $result = $dbh->do("
		INSERT INTO $SPELLING_SCORE
		SELECT o.child_spelling_no, o.orig_no, 
			sum(if(o.spelling_reason = 'misspelling',-1,+1)) as score
		FROM $SPELLING_AUX as s JOIN $opinion_cache as o using (orig_no)
		WHERE s.is_misspelling
		GROUP BY o.child_spelling_no");
    
    # Now choose the spellings with the best scores and use them to replace
    # the misspellings.
    
    $result = $dbh->do("
		REPLACE INTO $SPELLING_AUX
		SELECT s.orig_no, o.child_spelling_no, o.opinion_no, false
		FROM $SPELLING_AUX as s JOIN $opinion_cache as o using (orig_no)
			JOIN $SPELLING_SCORE as x on x.spelling_no = o.child_spelling_no
		WHERE s.is_misspelling and o.spelling_reason <> 'misspelling' and
			x.score > 0
		ORDER BY x.score ASC, o.pubyr ASC, o.ri ASC, o.opinion_no ASC");
    
    # If this fails, we try the original spelling as long as its score is not
    # negative.  First try a join on the opinion table, so that we can get an
    # opinion_no value, but also try just the spelling table alone since often
    # there is no opinion for the original name.
    
    $result = $dbh->do("
		REPLACE INTO $SPELLING_AUX
		SELECT s.orig_no, o.child_spelling_no, o.opinion_no, false
		FROM $SPELLING_AUX as s JOIN $opinion_cache as o on o.orig_no = s.orig_no
				and o.child_spelling_no = s.orig_no
			LEFT JOIN $SPELLING_SCORE as x on x.orig_no = s.orig_no
		WHERE s.is_misspelling and (x.score is null or x.score >= 0)");
    
    $result = $dbh->do("
		REPLACE INTO $SPELLING_AUX
		SELECT s.orig_no, s.orig_no, 0, false
		FROM $SPELLING_AUX as s LEFT JOIN $SPELLING_SCORE as x on x.spelling_no = s.orig_no
		WHERE s.is_misspelling and (x.score is null or x.score >= 0)");
    
    # For anything that falls through all of these, we just use the original
    # misspelling and call it a day.
    
    # Next, we copy all of the computed spelling_no values into $TREE_WORK.
    # For every taxonomic concept which doesn't have a spelling_no value, we
    # just use the orig_no.
    
    logMessage(2, "    setting spelling_no");
    
    $result = $dbh->do("UPDATE $TREE_WORK as t LEFT JOIN $SPELLING_AUX as s USING (orig_no)
			SET t.spelling_no = ifnull(s.spelling_no, t.orig_no)");
    
    # Now that we have set spelling_no, we can efficiently index it.
    
    logMessage(2, "    indexing spelling_no");
    
    $result = $dbh->do("ALTER TABLE $TREE_WORK ADD INDEX (spelling_no)");
    
    # As an aside, we can now compute and index the trad_no field, which might
    # be of interest to those who prefer the traditional taxonomic ranks
    # whenever possible.  Its value is the same as spelling_no except where
    # the spelling_no corresponds to a name whose rank is 'unranked clade' and
    # there is at least one taxon of the same name and a different rank, in
    # which case the most recent and reliable such name is used instead.
    
    logMessage(2, "    computing trad_no");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $TRAD_AUX");
    $result = $dbh->do("CREATE TABLE $TRAD_AUX LIKE $SPELLING_AUX");
    $result = $dbh->do("
		INSERT IGNORE INTO $TRAD_AUX (orig_no, spelling_no)
		SELECT t.orig_no, o.child_spelling_no
		FROM $auth_table as a
			JOIN $TREE_WORK as t ON a.taxon_no = t.spelling_no
				AND a.taxon_rank = 'unranked clade'
			JOIN $opinion_cache as o on o.orig_no = t.orig_no
			JOIN $auth_table as a2 on o.child_spelling_no = a2.taxon_no
		WHERE a2.taxon_rank <> 'unranked clade' and a2.taxon_name = a.taxon_name
		ORDER BY o.ri DESC, o.pubyr DESC, o.opinion_no DESC");
    
    logMessage(2, "    setting trad_no");
    
    $result = $dbh->do("UPDATE $TREE_WORK SET trad_no = spelling_no");
    $result = $dbh->do("UPDATE $TREE_WORK as t JOIN $TRAD_AUX as s USING (orig_no)
    			SET t.trad_no = s.spelling_no");
    
    logMessage(2, "    indexing trad_no");
    
    $result = $dbh->do("ALTER TABLE $TREE_WORK ADD INDEX (trad_no)");
    
    # We then copy the selected name and rank into $TREE_TABLE.  We use the
    # traditional rank instead of the currently accepted one, because 'unranked clade'
    # isn't really very useful.
    
    logMessage(2, "    setting name and rank");
    
    $result = $dbh->do("UPDATE $TREE_WORK as t JOIN $auth_table as a on a.taxon_no = t.spelling_no
			SET t.name = a.taxon_name, t.opinion_rank = a.taxon_rank");
    
    $result = $dbh->do("UPDATE $TREE_WORK as t JOIN $auth_table as a on a.taxon_no = t.trad_no
			SET t.rank = a.taxon_rank");
    
    logMessage(2, "    indexing by name");
    
    $result = $dbh->do("ALTER TABLE $TREE_WORK ADD INDEX (name)");
    
    # Then we can compute the name table, which records the best opinion
    # and spelling reason for each taxonomic name.
    
    logMessage(2, "    computing taxonomic name table");
    
    # First put in all of the names we've selected above.
    
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $NAME_WORK
			       (taxon_no int unsigned not null,
				orig_no int unsigned not null,
				spelling_reason enum('original spelling','recombination','reassignment','correction','rank change','misspelling'),
				opinion_no int unsigned not null,
				PRIMARY KEY (taxon_no)) ENGINE=MYISAM");
    
    $result = $dbh->do("
		INSERT IGNORE INTO $NAME_WORK
		SELECT s.spelling_no, s.orig_no, o.spelling_reason, o.opinion_no
		FROM $SPELLING_AUX as s JOIN $opinion_cache as o using (opinion_no)");
    
    # Then fill in the rest of the names that have opinions, using the best available
    # opinion for each one.
    
    $result = $dbh->do("
		INSERT IGNORE INTO $NAME_WORK
		SELECT o.child_spelling_no, o.orig_no, o.spelling_reason, o.opinion_no
		FROM $opinion_cache as o JOIN $TREE_WORK USING (orig_no)
		ORDER BY o.ri DESC, o.pubyr DESC, o.opinion_no DESC");
    
    # Then add dummy entries for all other names.
    
    $result = $dbh->do("
		INSERT IGNORE INTO $NAME_WORK
		SELECT a.taxon_no, a.orig_no, '', 0
		FROM $auth_table as a");
    
    # Finally, we can index it.
    
    logMessage(2, "    indexing taxonomic name table");
    
    $result = $dbh->do("ALTER TABLE $NAME_WORK ADD KEY (orig_no)");
    $result = $dbh->do("ALTER TABLE $NAME_WORK ADD KEY (opinion_no)");
    
    my $a = 1;		# we can stop on this line when debugging
}


# expandToJuniors ( dbh, tree_table )
# 
# Expand $TREE_WORK to adding to it all rows from $tree_table which represent
# junior synonyms of the concepts already in $TREE_WORK.

sub expandToJuniors {
    
    my ($dbh, $tree_table) = @_;
    
    my $opinion_cache = $TAXON_TABLE{$tree_table}{opcache};
    
    # We need to repeat the following process until no new rows are added, so
    # that junior synonyms of junior synonyms, etc. also get added.
    
    # Note that we can't just use the synonym_no field of $tree_table, because
    # it refers to the most senior synonym instead of the immediate senior
    # synonym.  We might have a synonym chain A -> B -> C with B in
    # $TREE_WORK.  In such a case, we would miss A if we relied on synonym_no.

    while (1)
    {
	my $result = $dbh->do("
		INSERT IGNORE INTO $TREE_WORK (orig_no, taxon_name, taxon_rank, spelling_no)
		SELECT m.orig_no, m.taxon_name, m.taxon_rank, m.spelling_no
		FROM $TREE_WORK as t
			JOIN $opinion_cache as o ON o.parent_no = t.orig_no
				and o.status != 'belongs to' and o.orig_no <> o.parent_no
			STRAIGHT_JOIN $tree_table as m ON m.opinion_no = o.opinion_no");
	
	last if $result == 0;
    }
    
    my $a = 1;		# we can stop on this line when debugging
}


# expandToChildren ( dbh, tree_table )
# 
# Expand $TREE_WORK to adding to it all rows from $tree_table which represent
# children of the concepts already in $TREE_WORK.  This is not needed unless
# we decide to have parent_no point to senior synonyms, but I will leave it in
# the code in case we later maek that decision.

sub expandToChildren {
    
    my ($dbh, $tree_table) = @_;
    
    my $result = $dbh->do("
		INSERT IGNORE INTO $TREE_WORK
		SELECT m.* FROM $tree_table m JOIN $TREE_WORK t
			ON m.parent_no = t.orig_no");
    
    logMessage(1, "adding children: $result concepts");
}


# computeSynonymy ( dbh, tree_table, options )
# 
# Fill in the synonym_no field of $TREE_WORK.  This computes the "synonymy"
# relation for all of the concepts represented in $TREE_WORK.  We do this by
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
# c) Record the suppression in $OPINION_CACHE.
# 
# d) Repeat this procedure until no cycles are found.  This is necessary
#    because in rare cases the new opinion selected in step (b) might cause a
#    new cycle.
# 
# Once this has been done, we can set the synonym_no field based on the
# parent_no values in the classification opinions.

sub computeSynonymy {
    
    my ($dbh, $tree_table, $options) = @_;
    
    my ($result, @check_taxa, %taxa_moved, $filter);
    $options ||= {};
    
    logMessage(2, "computing synonymy relation (c)");

    my $OPINION_CACHE = $TAXON_TABLE{$tree_table}{opcache};
    
    # We start by choosing the "classification opinion" for each concept in
    # $TREE_WORK.  We use the same mechanism as we did previously with the
    # spelling opinions: use a table with a unique key on orig_no, and INSERT
    # IGNORE with a properly ordered selection.  We use slightly different
    # criteria to select these than we did for the "spelling opinions", which
    # is why we need a separate table.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $CLASSIFY_AUX");
    $result = $dbh->do("CREATE TABLE $CLASSIFY_AUX
			   (orig_no int unsigned not null,
			    opinion_no int unsigned not null,
			    parent_no int unsigned not null,
			    ri int unsigned not null,
			    pubyr varchar(4),
			    status enum('belongs to','subjective synonym of','objective synonym of','invalid subgroup of','misspelling of','replaced by','nomen dubium','nomen nudum','nomen oblitum','nomen vanum'),
			    UNIQUE KEY (orig_no)) ENGINE=MYISAM");
    
    # We ignore any opinions where orig_no and parent_no are identical,
    # because those are irrelevant to the synonymy relation (they might simply
    # indicate variant spellings, for example).
    
    $result = $dbh->do("INSERT IGNORE INTO $CLASSIFY_AUX
			SELECT o.orig_no, o.opinion_no, o.parent_no,
			    o.ri, o.pubyr, o.status
			FROM $OPINION_CACHE o JOIN $TREE_WORK USING (orig_no)
			WHERE o.orig_no != o.parent_no
			ORDER BY o.ri DESC, o.pubyr DESC, o.opinion_no DESC");
    
    # Now we download just those classification opinions which indicate
    # synonymy.
    
    logMessage(2, "    downloading synonymy opinions");
    
    my $synonym_opinions = $dbh->prepare("
		SELECT orig_no, parent_no, opinion_no, ri, pubyr
		FROM $CLASSIFY_AUX
		WHERE status in ($VALID_STATUS) AND parent_no != 0");
    
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
    
    # We are now ready to check for cycles.  The result of the following call
    # is a list of pairs; each pair gives a taxon which is asserted to be a
    # junior synonym by an opinion which needs to be suppressed, along with
    # the opinion_no of the opinion in question.
    
    logMessage(2, "    checking for cycles");
    
    my @breaks = breakCycles($dbh, \%juniors, \%opinions);
    
    logMessage(1, "    found " . scalar(@breaks) . " cycle(s)") if @breaks;
    
    # As long as there are cycles to be broken, we suppress the indicated
    # opinions and do a re-check, just in case the next-best opinions cause a
    # new cycle.
    
    while ( @breaks )
    {
	# Go through the cycle-breaking list, and mark the indicated opinions
	# as suppressed.  We also keep track of the associated taxa in
	# @check_taxa.  We will use that list to fetch the changed opinions,
	# and also we need only check these taxa in our followup cycle check
	# because any new cycle must involve one of them.
	
	@check_taxa = ();
	
	my $suppress_stmt = $dbh->prepare("UPDATE $OPINION_CACHE SET suppress = 1
					   WHERE opinion_no = ?");
	
	foreach my $pair (@breaks)
	{
	    my ($check_taxon, $suppress_opinion) = @$pair;
	    
	    $result = $suppress_stmt->execute($suppress_opinion);
	    
	    push @check_taxa, $check_taxon;
	    $taxa_moved{$check_taxon} = 0;
	}
	
	# Next, we update the $CLASSIFY_AUX table by deleting the suppressed
	# opinions, then replacing them with the next-best opinion.  We need
	# to delete first, because there may be no next-best opinion!  (Also
	# because that way we can use INSERT IGNORE again to pick the single
	# best opinion for each orig_no as above).
	
	my $check_taxa = join(',', @check_taxa);
	
	$result = $dbh->do("DELETE FROM $CLASSIFY_AUX WHERE orig_no in ($check_taxa)");
	
	$result = $dbh->do("
		INSERT IGNORE INTO $CLASSIFY_AUX
		SELECT o.orig_no, o.opinion_no, o.parent_no,
		    o.ri, o.pubyr, o.status
		FROM $OPINION_CACHE o
		WHERE suppress is null and orig_no in ($check_taxa)
			and o.orig_no != o.parent_no
		ORDER BY o.ri DESC, o.pubyr DESC, o.opinion_no DESC");
	
	# In order to repeat the cycle check, we need to grab these new
	# opinions.
	
	$synonym_opinions = $dbh->prepare("
		SELECT orig_no, parent_no, ri, pubyr, opinion_no
		FROM $CLASSIFY_AUX
		WHERE status != 'belongs to' and parent_no != 0
			and orig_no in ($check_taxa)");
	
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
    
    # Now that we have broken all of the cycles in $CLASSIFY_AUX, we can
    # fill in the synonym_no field of $TREE_WORK.
    
    logMessage(2, "    setting synonym_no");
    
    $result = $dbh->do("
	UPDATE $TREE_WORK as t LEFT JOIN $CLASSIFY_AUX as b using (orig_no)
	SET t.synonym_no = if(b.status in ($SYNONYM_STATUS) and b.parent_no > 0, b.parent_no, orig_no)");
    
    logMessage(2, "    indexing synonym_no");
    
    $result = $dbh->do("ALTER TABLE $TREE_WORK add index (synonym_no)");
    
    my $a = 1;	# we can stop here when debugging
}


# expandToSeniors ( dbh, tree_table )
# 
# Expand $TREE_WORK by adding to it all rows from $tree_table which represent
# senior synonyms of the concepts already in $TREE_WORK, and all rows from
# $tree_table representing concepts which used to be senior synonyms but might
# not be anymore.  All of these might undergo a change of classification as
# part of this update.
# 
# We have to expand $CLASSIFY_AUX in parallel, so that we will have the
# necessary information to execute computeHierarchy() later in this process.

sub expandToSeniors {
    
    my ($dbh, $tree_table) = @_;
    
    my ($count, $result);
    
    my $OPINION_CACHE = $TAXON_TABLE{$tree_table}{opcache};
    
    # We need to repeat the following process until no new rows are added, so
    # that senior synonyms of senior synonyms, etc. also get added
    
    do
    {
	# First the new synonyms (using $CLASSIFY_AUX)
	
	$count = $dbh->do("
		INSERT IGNORE INTO $TREE_WORK (orig_no, spelling_no, synonym_no)
		SELECT m.orig_no, m.spelling_no, m.synonym_no
		FROM $CLASSIFY_AUX c
			JOIN $OPINION_CACHE o USING (opinion_no)
			STRAIGHT_JOIN $tree_table m ON o.parent_no = m.orig_no
				and o.status != 'belongs to'");
	
	# Then the old synonyms (using $tree_table)
	
	$count += $dbh->do("
		INSERT IGNORE INTO $TREE_WORK (orig_no, spelling_no, synonym_no)
		SELECT m.orig_no, m.spelling_no, m.synonym_no
		FROM $TREE_WORK t 
			JOIN $tree_table m USING (orig_no)
			JOIN $OPINION_CACHE o ON o.opinion_no = m.opinion_no
			JOIN $tree_table m2 ON o.parent_no = m2.orig_no
				and o.status != 'belongs to'");
	
	# Then expand $CLASSIFY_AUX with corresponding rows for every concept
	# that was added to $TREE_WORK.
	
	$result = $dbh->do("
		INSERT IGNORE INTO $CLASSIFY_AUX
		SELECT m.orig_no, m.opinion_no, m.parent_no, o.ri, o.pubyr, o.status
		FROM $TREE_WORK t JOIN $tree_table m USING (orig_no)
			JOIN $OPINION_CACHE o ON o.opinion_no = m.opinion_no
			LEFT JOIN $CLASSIFY_AUX c ON c.orig_no = t.orig_no
		WHERE c.opinion_no is null") if $count > 0;
	
    } while $count > 0;
    
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
    
    logMessage(2, "    removing synonym chains");
    
    # Repeat the following process until no more rows are affected, with a
    # limit of 20 to avoid an infinite loop just in case our algorithm above
    # was faulty and some cycles have slipped through.
    
    my $count = 0;
    my $result;
    
    do
    {
	$result = $dbh->do("
		UPDATE $TREE_WORK t1 JOIN $TREE_WORK t2
		    on t1.synonym_no = t2.orig_no and t1.synonym_no != t2.synonym_no
		SET t1.synonym_no = t2.synonym_no");
    }
	while $result > 0 && ++$count < 20;
    
    if ( $count >= 20 )
    {
	logMessage(0,"WARNING - possible synonymy cycle detected during synonym linking");
    }
    
    # Then the same for valid_no
    
    # $count = 0;
    
    # do
    # {
    # 	$result = $dbh->do("
    # 		UPDATE $TREE_WORK t1 JOIN $TREE_WORK t2
    # 		    on t1.valid_no = t2.orig_no and t1.valid_no != t2.valid_no
    # 		SET t1.valid_no = t2.valid_no");
    # }
    # 	while $result > 0 && ++$count < 20;
    
    # if ( $count >= 20 )
    # {
    # 	logMessage(0,"WARNING - possible synonymy cycle detected during synonym linking");
    # }
    
    my $a = 1;	# we can stop here when debugging
}


# computeHierarchy ( dbh )
# 
# Fill in the opinion_no, status, parent_no and parsen_no fields of
# $TREE_WORK.  This determines the classification of each taxonomic concept
# represented in $TREE_WORK, and thus determines the Hierarchy relation.
# 
# We start with the set of classification opinions chosen by
# computeSynonymy(), but we then recompute all of the ones that specify
# hierarchy (the 'belongs to' opinions).  This time, for each taxon at the
# genus level and above we consider all of the opinions for each senior
# synonym along with all of the opinions for its immediate junior synonyms,
# and choose the most recent and reliable from that set.  Note that we leave
# out nomina dubia, nomina vana, nomina nuda, nomina oblita, and invalid
# subgroups, because an opinion on any of those shouldn't affect the senior
# concept.
# 
# After this is done, we must check for cycles using the same procedure as
# computeSynonymy().  Only then can we set the parent_no field of $TREE_WORK.
# Finally, we set opinion_no for each row of $TREE_WORK, based on the modified
# set of classification opinions.

sub computeHierarchy {
    
    my ($dbh, $tree_table) = @_;
    
    my ($result, $filter, @check_taxa, %taxa_moved);
    
    logMessage(2, "computing hierarchy relation (d)");
    
    my $OPINION_CACHE = $TAXON_TABLE{$tree_table}{opcache};
    
    # We already have the $CLASSIFY_AUX relation, but we need to adjust it by
    # grouping together all of the opinions for each senior synonym and its
    # immediate juniors and re-selecting the classification opinion for each
    # group.  Note that the junior synonyms already have their classification
    # opinion selected from computeSynonymy() above.
    
    # We need to create an auxiliary table to do this grouping.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SYNONYM_AUX");
    $result = $dbh->do("CREATE TABLE $SYNONYM_AUX
				(junior_no int unsigned,
				 senior_no int unsigned,
				 primary key (junior_no),
				 key (senior_no)) ENGINE=MYISAM");
    
    # We consider all junior synonyms of genera and above, but only subjective
    # and objective synonyms and replaced taxa.  We leave out nomina dubia,
    # nomina vana, nomina nuda, nomina oblita, and invalid subgroups, because
    # an opinion on any of those shouldn't affect the senior taxon.
    
    $result = $dbh->do("INSERT IGNORE INTO $SYNONYM_AUX
			SELECT c.orig_no, c.parent_no
			FROM $CLASSIFY_AUX as c JOIN $TREE_WORK as t on t.orig_no = c.orig_no
				JOIN $TREE_WORK as t2 on t2.orig_no = c.parent_no
			WHERE c.status in ($SYNONYM_STATUS) and t.rank >= 5 and t2.rank >= 5");
    
    # Next, we add entries for all of the senior synonyms, because of course
    # their own opinions are considered as well.
    
    $result = $dbh->do("INSERT IGNORE INTO $SYNONYM_AUX
    			SELECT DISTINCT synonym_no, synonym_no
			FROM $TREE_WORK
			WHERE orig_no = synonym_no");
    
    # Next, we delete the classification opinion for each taxon that is known
    # to be a senior synonym.  This will clear the way for recomputing the
    # classification of these taxa.
    
    $result = $dbh->do("DELETE QUICK FROM c
			USING $CLASSIFY_AUX as c JOIN $TREE_WORK as t
				ON c.orig_no = t.synonym_no
			WHERE t.synonym_no <> t.orig_no");
    
    # Then we use the same INSERT IGNORE trick to select the best opinion for
    # these senior synonyms, considering all of the 'belongs to' opinions from
    # its synonym group.
    
    $result = $dbh->do("INSERT IGNORE INTO $CLASSIFY_AUX
			SELECT c.senior_no, o.opinion_no, o.parent_no, o.ri, 
				o.pubyr, o.status
			FROM $OPINION_CACHE o
			    JOIN $SYNONYM_AUX c ON o.orig_no = c.junior_no
			WHERE o.status = 'belongs to' and c.senior_no != o.parent_no
			ORDER BY o.ri DESC, o.pubyr DESC, o.opinion_no DESC");
    
    # The next step is to check for cycles within $CLASSIFY_AUX, for which we
    # use the same algorithm as is used for the rebuild operation.  This won't
    # catch all cycles on updates, so we have to rely on the opinion-editing
    # code catching obvious cycles.  In any case, the tree will be fixed on
    # the next rebuild (if the user doesn't catch it and undo the change
    # themself).
    
    logMessage(2, "    downloading hierarchy opinions");
    
    my $best_opinions = $dbh->prepare("
		SELECT orig_no, parent_no, ri, pubyr, opinion_no
		FROM $CLASSIFY_AUX WHERE parent_no > 0");
    
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
	    print STDERR "WARNING - best opinion for $child has same parent (opinion $op)\n";
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
	@check_taxa = ();
	
	my @suppress_list;
	
	foreach my $pair (@breaks)
	{
	    my ($check_taxon, $suppress_opinion) = @$pair;
	    
	    push @suppress_list, $suppress_opinion;
	    push @check_taxa, $check_taxon;
	    $taxa_moved{$check_taxon} = 0;
	}
	
	my $suppress_list = join(',', @suppress_list);
	
	$result = $dbh->do("UPDATE $OPINION_CACHE SET suppress = 1
			    WHERE opinion_no in ($suppress_list)");
	
	# We also have to clean up the $CLASSIFY_AUX table, so that we can
	# compute the next-best opinion for each suppressed one.  So we delete
	# the suppressed opinions, and replace them with the next-best
	# opinion.  We need to delete first, because there may be no next-best
	# opinion!
	
	my $check_taxa = join(',', @check_taxa);
	
	$result = $dbh->do("DELETE FROM $CLASSIFY_AUX WHERE orig_no in ($check_taxa)");
	
	$result = $dbh->do("
		INSERT IGNORE INTO $CLASSIFY_AUX
		SELECT s.senior_no, o.opinion_no, o.parent_no, o.ri, 
			o.pubyr, o.status
		FROM $OPINION_CACHE o JOIN $SYNONYM_AUX s ON o.orig_no = s.junior_no
		WHERE suppress is null and orig_no in ($check_taxa)
			and o.status = 'belongs to' and s.senior_no != o.parent_no
		ORDER BY o.ri DESC, o.pubyr DESC, o.opinion_no DESC");
	
	# In order to repeat the cycle check, we need to grab these new
	# opinions.
	
	my $belongs_opinions = $dbh->prepare("
		SELECT orig_no, parent_no, ri, pubyr, opinion_no
		FROM $CLASSIFY_AUX WHERE parent_no > 0 and orig_no in ($check_taxa)");
	
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
    
    # Now we can set and then index opinion_no and status.
    
    logMessage(2, "    setting opinion_no and status");
    
    $result = $dbh->do("UPDATE $TREE_WORK as t JOIN $CLASSIFY_AUX as c USING (orig_no)
			SET t.opinion_no = c.opinion_no, t.status = c.status");
    
    logMessage(2, "    indexing opinion_no and status");
    
    $result = $dbh->do("ALTER TABLE $TREE_WORK add index (opinion_no)");
    $result = $dbh->do("ALTER TABLE $TREE_WORK add index (status)");
    
    # Now we can set parent_no.  All concepts in a synonym group will share
    # the same parent, so we need to join a second copy of $TREE_WORK to look
    # up the senior synonym number.  In other words, the parent_no value for
    # any taxon will point to the parent of its most senior synonym.
    
    logMessage(2, "    setting parent_no");
    
    $result = $dbh->do("
		UPDATE $TREE_WORK as t JOIN $TREE_WORK as t2 ON t2.orig_no = t.synonym_no
		    JOIN $OPINION_CACHE o ON o.opinion_no = t2.opinion_no
		SET t.parent_no = o.parent_no");
    
    # Once we have set parent_no for all concepts, we can efficiently index it.
     
    logMessage(2, "    indexing parent_no");
    
    $result = $dbh->do("ALTER TABLE $TREE_WORK add index (parent_no)");
    
    # Then we can set and index parsen_no, which points to the senior synonym
    # of the parent taxon.
    
    logMessage(2, "    setting parsen_no");
    
    $result = $dbh->do("
		UPDATE $TREE_WORK as t JOIN $TREE_WORK as t2 ON t2.orig_no = t.parent_no
		SET t.parsen_no = t2.synonym_no");
    
    logMessage(2, "    indexing parsen_no");
    
    $result = $dbh->do("ALTER TABLE $TREE_WORK add index (parsen_no)");
    
    my $a = 1;	# we can stop here when debugging
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


# linkParents ( dbh, tree_table )
# 
# Update the parent_no field of $TREE_WORK to include parents which are not
# themselves represented in $TREE_WORK.  This is not needed currently, but
# might be in the future if we decide that parent_no should point to senior
# synonyms (so I'm going to keep it in the code).

sub linkParents {
    
    my ($dbh, $tree_table) = @_;
    
    my $result;
    
    # Set parent_no when the parent is not represented in $TREE_WORK.
    
    $result = $dbh->do("
		UPDATE $TREE_WORK t
		    JOIN $TREE_WORK t2 ON t2.orig_no = t.synonym_no
		    JOIN $TAXON_TABLE{$tree_table}{opcache} o ON o.opinion_no = t2.opinion_no
		    JOIN $tree_table m ON m.orig_no = o.parent_no
		SET t.parent_no = m.synonym_no
		WHERE t.parent_no = 0 and o.parent_no != 0");
    
    return;
}


# adjustHierarchicalNames ( dbh )
# 
# Update the taxonomic names stored in $TREE_WORK so that species, subspecies
# and subgenus names match the genera under which they are hierarchically
# placed.  If two or more genera are synonymized, then the names of all
# contained taxa should match the senior synonym.  The 'imp' flag is used to
# indicate an "implied" name which is derived in part from an opinion which
# synonymizes genera or subgenera in addition to the opinion which classifies
# the species name.

sub adjustHierarchicalNames {

    my ($dbh, $tree_table) = @_;
    
    my $result;
    
    logMessage(2, "adjusting taxonomic names to fit hierarchy (d)");
    
    # We start by creating an auxiliary table to help with the name changes.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $ADJUST_AUX");
    $result = $dbh->do("CREATE TABLE $ADJUST_AUX
			       (orig_no int unsigned not null,
				new_name varchar(80) not null) ENGINE=MYISAM");
    
    # The first thing we need to do is to fix subgenus names.  All of these
    # which are themselves senior synonyms must match the senior synonym of
    # the genus under which they are immediately classified.
    
    $SQL_STRING = "
		INSERT INTO $ADJUST_AUX (orig_no, new_name)
		SELECT t.orig_no,
		       concat(t1.name, ' (',
		              trim(trailing ')' from substring_index(t.name,'(',-1)), ')')
		FROM $TREE_WORK as t
			JOIN $TREE_WORK as p1 on p1.orig_no = t.parent_no
			JOIN $TREE_WORK as t1 on t1.orig_no = p1.synonym_no 
				and p1.rank = 5
		WHERE t.rank = 4 and t.orig_no = t.synonym_no";
    
    $result = $dbh->do($SQL_STRING);
    
    # If the computed name is different from the one already stored in
    # $TREE_WORK, then update the name and set the 'imp' flag.
    
    $SQL_STRING = "
		UPDATE $TREE_WORK as t JOIN $ADJUST_AUX as ax using (orig_no)
		SET t.name = ax.new_name,
		    t.imp = 1
		WHERE ax.new_name <> t.name";
    
    $result = $dbh->do($SQL_STRING);
        
    # Once we have subgenera dealt with, we can deal with species and
    # subspecies names.  For each one, we look up the tree for the first genus
    # or subgenus we come to and use that as the first part of the name
    # (making sure to look at senior synonyms).
    
    $result = $dbh->do("DELETE FROM $ADJUST_AUX");
    
    $SQL_STRING = "
		INSERT INTO $ADJUST_AUX (orig_no, new_name)
		SELECT t.orig_no,
		       concat(if(p1.rank in (4,5), p1.name,
			      if(p2.rank in (4,5), p2.name,
				 p3.name)), ' ',
			      if(t.name like '%)%',
				 trim(substring(t.name, locate(') ', t.name)+2)),
				 trim(substring(t.name, locate(' ', t.name)+1))))
		FROM $TREE_WORK as t
			LEFT JOIN $TREE_WORK as p1 on p1.orig_no = t.parent_no
			LEFT JOIN $TREE_WORK as t1 on t1.orig_no = p1.synonym_no 
			LEFT JOIN $TREE_WORK as p2 on p2.orig_no = p1.parent_no
			LEFT JOIN $TREE_WORK as t2 on t2.orig_no = p2.synonym_no
			LEFT JOIN $TREE_WORK as p3 on p3.orig_no = p2.parent_no
			LEFT JOIN $TREE_WORK as t3 on t3.orig_no = p3.synonym_no
		WHERE t.rank in (2, 3) and t.orig_no = t.synonym_no and
			(p1.rank in (4,5) or p2.rank in (4,5) or p3.rank in (4,5))";
    
    $result = $dbh->do($SQL_STRING);
    
    # If the computed name is different from the one already stored in
    # $TREE_WORK, then update the name and set the 'imp' flag.
    
    $SQL_STRING = "
		UPDATE $TREE_WORK as t JOIN $ADJUST_AUX as ax using (orig_no)
		SET t.name = ax.new_name,
		    t.imp = 1
		WHERE ax.new_name <> t.name";
    
    $result = $dbh->do($SQL_STRING);
    
    my $a = 1;		# we can stop here when debugging
}


# computeTreeSequence ( dbh )
# 
# Fill in the lft, rgt and depth fields of $TREE_WORK.  This has the effect of
# arranging the rows into a forest of Nested Set trees.  For more information,
# see: http://en.wikipedia.org/wiki/Nested_set_model.
# 
# The basic nested set model (numbering the nodes in a straight sequence) has
# the limitation that adding new nodes requires a total renumber.  In an
# effort to mitigate this, we leave empty space in the sequence after every
# node to provide space for additional children that may be added in later (or
# moved from one part of the tree to another).  More space is left after
# nodes representing taxa of higher ranks.

sub computeTreeSequence {
    
    my ($dbh) = @_;
    
    my $result;
    
    my $nodes = {};
    
    logMessage(2, "traversing and marking taxon trees (e)");
    
    logMessage(2, "    downloading hierarchy relation");
    
    # First we fetch the entire hierarchy relation.  We need to do this so
    # that we can do the sequence computation in Perl (because SQL is just not
    # a powerful enough language to do this efficiently).
    
    my $fetch_hierarchy = $dbh->prepare("
	SELECT t.orig_no, t.rank, c.parent_no,
	       if(c.status in ($SYNONYM_STATUS), 1, 0) as is_junior
	FROM $TREE_WORK as t JOIN $CLASSIFY_AUX as c using (orig_no)
	ORDER BY t.name");
    
    $fetch_hierarchy->execute();
    
    # Then we turn each row of $TREE_WORK into a node.  The contents of each
    # node indicate which taxon is its immediate parent (the senior synonym if
    # it is a junior synonym, or the containing taxon otherwise), the
    # taxonomic rank, and whether or not it is a junior synonym.
    
    while ( my ($child_no, $taxon_rank, $parent_no, $is_junior) = $fetch_hierarchy->fetchrow_array() )
    {
	$nodes->{$child_no} = { imm_parent => $parent_no, 
				rank => $taxon_rank, 
				is_junior => $is_junior };
    }
    
    # Then go through the nodes and create a 'children' list for each one.
    
    foreach my $child_no (keys %$nodes)
    {
	my $parent_no = $nodes->{$child_no}{imm_parent};
	push @{$nodes->{$parent_no}{children}}, $child_no if $parent_no > 0;
    }
    
    # Now we create the "main" tree, starting with taxon 28595 'Life' at the
    # top of the tree with sequence=1 and depth=1.  The variable $seq gets
    # the sequence number with which we should start the next tree.
    
    logMessage(2, "    sequencing tree rooted at 'Life'");
    
    my $seq = assignSequence($nodes, 28595, 1, 1);
    
    # Next, we go through all of the other taxa.  When we find a taxon with no
    # parent that we haven't visited yet, we create a new tree with it as the
    # root.  This takes care of all the taxa for which their relationship to
    # the main tree is not known.
    
    logMessage(2, "    sequencing all other taxon trees");
    
 taxon:
    foreach my $orig_no (keys %$nodes)
    {
	next if $nodes->{$orig_no}{imm_parent} > 0;	# skip any taxa that aren't roots
	next if $nodes->{$orig_no}{lft} > 0;		# skip any that we've already inserted
	
	$seq = assignSequence($nodes, $orig_no, $seq + 1, 1);
    }
    
    # Now we need to upload all of the tree sequence data to the server so
    # that we can set lft, rgt, and depth in $TREE_WORK.  To do this
    # efficiently, we use an auxiliary table and a large insert statement.
    
    logMessage(2, "    uploading tree sequence data");
    
    $dbh->do("DROP TABLE IF EXISTS tree_insert");
    $dbh->do("CREATE TABLE tree_insert
     		       (orig_no int unsigned,
     			lft int unsigned,
     			rgt int unsigned,
			bound int unsigned,
     			depth int unsigned) ENGINE=MYISAM");
    
    my $insert_stmt = "INSERT INTO tree_insert VALUES ";
    my $comma = '';
    
    foreach my $orig_no (keys %$nodes)
    {
	my $node = $nodes->{$orig_no};
	next unless $node->{lft} > 0;
	$insert_stmt .= $comma;
	$insert_stmt .= "($orig_no, $node->{lft}, $node->{rgt}, $node->{bound}, $node->{depth})";
	$comma = ',';
	
	if ( length($insert_stmt) > 200000 )
	{
	    $result = $dbh->do($insert_stmt);
	    $insert_stmt = "INSERT INTO tree_insert VALUES ";
	    $comma = '';
	}
    }
    
    $result = $dbh->do($insert_stmt) if $comma;
    
    # Now that we have uploaded the data, we can copy it into $TREE_WORK and
    # then delete the temporary table.
    
    logMessage(2, "    setting lft, rgt, depth");
    
    $result = $dbh->do("ALTER TABLE tree_insert ADD INDEX (orig_no)");
    
    $result = $dbh->do("UPDATE $TREE_WORK t JOIN tree_insert i USING (orig_no)
			SET t.lft = i.lft, t.rgt = i.rgt, t.bound = i.bound, t.depth = i.depth");
    
    $result = $dbh->do("DROP TABLE IF EXISTS tree_insert");
    
    # Now we can efficiently index $TREE_WORK on lft, rgt and depth.
    
    logMessage(2, "    indexing lft, rgt, depth");
    
    $result = $dbh->do("ALTER TABLE $TREE_WORK add index (lft)");
    $result = $dbh->do("ALTER TABLE $TREE_WORK add index (rgt)");
    $result = $dbh->do("ALTER TABLE $TREE_WORK add index (depth)");
    
    my $a = 1;
}


# updateTreeSequence ( dbh, tree_table )
# 
# This function is called during an update.  It generates new sequence numbers
# for everything in $TREE_WORK whose parent is different from the
# corresponding entry in $TREE_TABLE, and for all of their chidlren.  The
# basic algorithm is the same as in computeTreeSequence above, except that the
# space for sequence numbers is provided by the existing lft and rgt values of
# the top nodes.

sub updateTreeSequence {
    
    my ($dbh, $tree_table) = @_;
    
    my $result;
    my $count = 0;
    
    # We start by fetching the contents of $TREE_WORK, plus the orig_no, lft
    # and rgt values of the new parent entries.  $$$
}


our (%GAP_SIZE);

# assignSequence ( nodes, orig_no, next, depth )
# 
# This recursive procedure marks the tree node for the given taxon with
# sequence number $next and depth $depth, and then goes on to recursively
# sequence all of its children.  Thus, the sequence numbers are set in a preorder
# traversal.
# 
# Each node sequenced has its 'lft' field set to its sequence number, and its
# 'rgt' field set to the maximum sequence number of all its children.  Any
# taxon which has no children will get the same 'lft' and 'rgt' value.  The
# 'depth' field marks the distance from the root of the current tree, with
# top-level nodes having a depth of 1.  An additional gap is left in the
# sequence after each node, with size dependent on the node's taxonomic rank.
# This allows the tree to be adjusted later without a complete renumbering.
# The end of the gap is stored in the field 'bound'.
# 
# This routine returns the end of the last gap, so the next node should always
# be numbered with the return value plus one.

sub assignSequence {

    my ($nodes, $orig_no, $seq, $depth) = @_;
    
    # First check that we haven't seen this node yet.  That should always be
    # true, unless there are cycles in the parent-child relation (which would
    # be bad!)
    
    my $node = $nodes->{$orig_no};
    
    unless ( exists $node->{lft} )
    {
	# First store the 'lft' and 'depth' values.
	
	$node->{lft} = $seq;
	$node->{depth} = $depth;
	
	# If this taxon has children, we must then iterate through them and
	# insert each one (and all its descendants) into the tree.  All taxa
	# that are synonyms of each other should have the same depth.
	
	if ( exists $node->{children} )
	{
	    foreach my $child_no ( @{$node->{children}} )
	    {
		my $child_node = $nodes->{$child_no};
		my $child_depth = $child_node->{is_junior} ? $depth : $depth + 1;
		
		$seq = assignSequence($nodes, $child_no, $seq + 1, $child_depth);
	    }
	}
	
	# When we are finished with all of the children, fill in the 'rgt'
	# field with the value returned from the last child.
	    
	$node->{rgt} = $seq;
	
	# Now add a gap, which varies in size by the taxon rank.
	
	$node->{bound} = $seq + $GAP_SIZE{$node->{rank}};
	
	return $node->{bound};
    }
    
    # If we have already seen this taxon, then we have a cycle!  Print a
    # warning, and otherwise ignore it.  We return $seq-1 because we didn't
    # actually assign $seq on any node.
    
    else
    {
	logMessage(0, "WARNING - tree cycle for taxon $orig_no");
	return $seq - 1;
    }
}


%GAP_SIZE = ( 2 => 0, 3 => 5, 4 => 100, 5 => 100, 6 => 1000, 7 => 1000, 8 => 1000, 9 => 1000, 
	      10 => 10000, 11 => 10000, 12 => 10000, 13 => 10000, 14 => 50000, 15 => 50000, 
	      16 => 50000, 17 => 50000, 18 => 50000, 19 => 50000, 20 => 50000, 21 => 50000,
	      22 => 100000, 23 => 100000, 25 => 10000, 26 => 10000);

# treePerturbation ( dbh, tree_table )
# 
# Determine the amount by which the taxon tree will need to be perturbed in
# order to complete the current update.  This is expressed as a fraction of
# the tree size.  The return value will be used to determine whether to alter
# the tree sequence in-place or recompute it from scratch.

sub treePerturbation {
    
    my ($dbh, $tree_table) = @_;
    
    my ($max_seq) = $dbh->selectrow_array("SELECT max(lft) from $tree_table");
    
    my $moved_taxa = $dbh->prepare("
		SELECT t.orig_no, t.parent_no, m.parent_no, m2.lft, m.lft
		FROM $TREE_WORK as t LEFT JOIN $tree_table as m USING (orig_no)
			LEFT JOIN $tree_table as m2 ON m2.orig_no = t.parent_no
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


# updateTreeTable ( dbh, tree_table, keep_temps )
# 
# Do the final steps in the update procedure.  These are as follows:
# 
# 1) copy everything from $TREE_WORK into $tree_table, overwriting
#    corresponding rows
# 2) adjust the tree sequence in $tree_table
# 
# These two have to be quick, since we need to do them with $tree_table
# locked.
# 

sub updateTreeTable {

    my ($dbh, $tree_table) = @_;
    
    my $result;
    
    # The first thing to do is to determine which rows have a changed
    # parent_no value.  Each one will require an adjustment to the tree
    # sequence.  We have to do this now, before we copy the rows from
    # $TREE_WORK over the corresponding rows from $tree_table below.
    
    # We order the rows in descending order of position to make sure that
    # children are moved before their parents.  This is necessary in order to
    # make sure that a parent is never moved underneath one of its existing
    # children (we have already eliminated cycles, so if this is going to
    # happen then the child must be moving as well).
    
    my $moved_taxa = $dbh->prepare("
		SELECT t.orig_no, t.parent_no
		FROM $TREE_WORK as t LEFT JOIN $tree_table as m USING (orig_no)
		WHERE t.parent_no != m.parent_no
		ORDER BY m.lft DESC");
    
    $moved_taxa->execute();
    
    my (@move);
    
    while ( my ($orig_no, $new_parent) = $moved_taxa->fetchrow_array() )
    {
	push @move, [$orig_no, $new_parent];
    }
    
    # Next, we need to fill in $TREE_WORK with the corresponding lft, rgt, and
    # depth values from $tree_table.  For all rows which aren't being moved to
    # a new location in the hierarcy, those values won't be changing.  Those
    # that are being moved will be updated below.
    
    $result = $dbh->do("UPDATE $TREE_WORK as t JOIN $tree_table as m USING (orig_no)
			SET t.lft = m.lft, t.rgt = m.rgt, t.depth = m.depth");
    
    # Before we update $tree_table, we need to lock it for writing.  This will
    # ensure that all other threads see it in a consistent state, either pre-
    # or post-update.  This means that we also have to lock $TREE_WORK for
    # read.
    
    $result = $dbh->do("LOCK TABLE $tree_table as m WRITE,
				   $tree_table WRITE,
				   $TREE_WORK as t READ,
				   $TREE_WORK READ");
    
    # Now write all rows of $TREE_WORK over the corresponding rows of
    # $TREE_TABLE.
    
    logMessage(2, "copying into active tables (f)");
    
    $result = $dbh->do("REPLACE INTO $tree_table SELECT * FROM $TREE_WORK");
    
    # Finally, we adjust the tree sequence to take into account all rows that
    # have changed their position in the hierarchy.  We couldn't do this until
    # now, because the procedure requires a table that holds the entire
    # updated set of taxon trees.
    
    my ($max_lft) = $dbh->selectrow_array("SELECT max(lft) from $tree_table");
    
    logMessage(2, "adjusting tree sequence (e)");
    logMessage(2, "    " . scalar(@move) . " concept(s) are moving within the hierarchy");
    
    foreach my $pair (@move)
    {
	adjustTreeSequence($dbh, $tree_table, $max_lft, @$pair);
    }
    
    # Now we can unlock the tree table.
    
    $result = $dbh->do("UNLOCK TABLES");
    
    return;
}


# adjustTreeSequence ( dbh, tree_table, max_lft, orig_no, new_parent )
# 
# Adjust the tree sequence so that taxon $orig_no (along with all of its
# children) falls properly under its new parent.  The taxon will end up as the
# last child of the new parent.  The parameter $max_lft should be the current
# maximum lft value in the table.

sub adjustTreeSequence {
    
    my ($dbh, $tree_table, $max_lft, $orig_no, $new_parent) = @_;
    
    my $sql = '';
    my $change_depth = '';
    
    # First, find out where in the tree the taxon (and all its children)
    # previously fell.
    
    my ($old_pos, $old_rgt, $old_depth) = $dbh->selectrow_array("
		SELECT lft, rgt, depth
		FROM $tree_table WHERE orig_no = $orig_no");
    
    # With this information we can calculate the size of the subtree being
    # moved.
    
    my $width = $old_rgt - $old_pos + 1;
    
    # Next, figure out where in the tree the new parent is currently
    # located.  If its new parent is 0, move the subtree to the very end.
    
    my ($parent_pos, $parent_rgt, $new_depth);
    
    if ( $new_parent > 0 )
    {
	($parent_pos, $parent_rgt, $new_depth) = $dbh->selectrow_array("
		SELECT lft, rgt, depth+1 FROM $tree_table
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
	
	$sql = "UPDATE $tree_table
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
	
	$sql = "UPDATE $tree_table
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
	
	$sql = "UPDATE $tree_table
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
	
	$sql = "UPDATE $tree_table
		SET rgt = rgt + $width
		WHERE lft <= $parent_pos and rgt >= $parent_rgt and rgt < $old_pos";
	
	$dbh->do($sql);
    }
    
    return;
}


# updateSecondaryTables ( dbh, tree_table, keep_temps )
# 
# Update the secondary tables associated with the given tree table.  Unless
# $keep_temps is true, all temporary tables will be deleted.

sub updateSecondaryTables {
    
    my ($dbh, $tree_table, $keep_temps) = @_;
    
    my $result;
    
    my $auth_table = $TAXON_TABLE{$tree_table}{authorities};
    my $search_table = $TAXON_TABLE{$tree_table}{search};
    my $name_table = $TAXON_TABLE{$tree_table}{names};
    
    # For $search_table, we need to remove all entries that correspond to
    # taxonomic concepts in $TREE_WORK.  We then add in everything from
    # $SEARCH_WORK.
    
    $result = $dbh->do("DELETE $search_table
			FROM $search_table
				JOIN $auth_table using (taxon_no)
				JOIN $TREE_WORK using (orig_no)");
    
    $result = $dbh->do("REPLACE INTO $search_table SELECT * FROM $SEARCH_WORK");
    
    # For $name_table, we need to do the same thing.
    
    $result = $dbh->do("DELETE $name_table
			FROM $name_table
				JOIN $auth_table using (taxon_no)
				JOIN $TREE_WORK using (orig_no)");
    
    $result = $dbh->do("REPLACE INTO $name_table SELECT * FROM $NAME_WORK");
    
    # Finally, we remove the temporary tables since they are no longer needed.
    
    unless ( $keep_temps )
    {
	logMessage(2, "removing temporary tables");
	
	$result = $dbh->do("DROP TABLE IF EXISTS $TREE_WORK");
	$result = $dbh->do("DROP TABLE IF EXISTS $SEARCH_WORK");
	$result = $dbh->do("DROP TABLE IF EXISTS $NAME_WORK");
	$result = $dbh->do("DROP TABLE IF EXISTS $ATTRS_WORK");
	
	$result = $dbh->do("DROP TABLE IF EXISTS $SPELLING_AUX");
	$result = $dbh->do("DROP TABLE IF EXISTS $MISSPELLING_AUX");
	$result = $dbh->do("DROP TABLE IF EXISTS $TRAD_AUX");
	$result = $dbh->do("DROP TABLE IF EXISTS $CLASSIFY_AUX");
	$result = $dbh->do("DROP TABLE IF EXISTS $SYNONYM_AUX");
	$result = $dbh->do("DROP TABLE IF EXISTS $SPECIES_AUX");
    }
    
    my $a = 1;		# We can stop here when debugging.
}


# Kingdoms and labels

our(@KINGDOM_LIST) = ( 'Metazoa', 'Plantae', 'Metaphytae',
		       'Fungi', 'Eukaryota', 'Eubacteria', 'Archaea' );

our(%KINGDOM_LABEL) = ( 'Metaphytae' => 'Plantae' );


# computePhylogeny ( dbh )
# 
# Compute the intermediate classification for each taxon: the kingdom, phylum,
# class, order and family to which it belongs.  Not all taxa have values for
# each of these attributes, but we determine all that can be computed.  Our
# algorithm is derived from the old Collections::getClassOrderFamily routine,
# with extensions for phylum and kingdom.  Note that because of rank changes
# some taxa do not have any containing taxa which are currently ranked as
# class, order, etc.  Where we can, we use the containing taxonomic concept
# which has most often been ranked as such in the past.  When none can be
# found, the classification in question is null.
# 
# This information is stored in the table $INTS_TABLE, with orig_no as primary
# key.  There are only rows in this table for taxonomic concepts above genus
# level.  In order to link these two tables properly, the ic_no field in
# $TREE_TABLE contains the orig_no value for all taxa above genus level, and
# the orig_no of the smallest such containing taxonomic concept in all other
# cases.  This field can thus be used to join the two tables.
# 
# The computation starts with the set of all taxa above genus level.  We count
# for each taxonomic concept the number of opinions which classify it as, say,
# a phylum vs. the number of opinions which classify it otherwise.  Likewise
# for class and order.
# 
# We then start at the top of the trees (depth 1) and iterate down by depth
# level.  As we go down, we set the intermediate classification fields for
# each taxon based on its parent, overriding one of them whenever the current
# taxon would be a better match.  For kingdom, we take the first kingdom or
# subkingdom we encounter on the way down, except that Eukaryota is only kept
# if no other kingdom is found.  For phylum, class and order, we override if
# the current taxon has been classified more often at that rank and less often
# at any other than the parent taxon's corresponding classification.  For
# family, we use the most specific taxonomic concept on the way down that is
# either ranked as a family or whose name ends in 'idae'.

sub computePhylogeny {
    
    my ($dbh, $tree_table) = @_;
    
    my ($result, $sql);

    logMessage(2, "computing phylogeny above genus level (f)");
    
    my $auth_table = $TAXON_TABLE{$tree_table}{authorities};
    my $opinion_cache = $TAXON_TABLE{$tree_table}{opcache};
    
    $result = $dbh->do("DROP TABLE IF EXISTS $INTS_WORK");
    $result = $dbh->do("CREATE TABLE $INTS_WORK
			       (ints_no int unsigned primary key,
				common_name varchar(80),
				kingdom_no int unsigned,
				kingdom varchar(80),
				phylum_no int unsigned,
				phylum varchar(80),
				class_no int unsigned,
				class varchar(80),
				order_no int unsigned,
				`order` varchar(80),
				family_no int unsigned,
				family varchar(80)) ENGINE=MYISAM");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $INTS_AUX");
    $result = $dbh->do("CREATE TABLE $INTS_AUX
			       (orig_no int unsigned,
				parsen_no int unsigned,
				depth int unsigned,
				taxon_name varchar(80),
				common_name varchar(80),
				current_rank tinyint not null,
				aux_rank tinyint not null,
				was_phylum smallint,
				not_phylum smallint,
				phylum_yr smallint,
				was_class smallint,
				not_class smallint,
				class_yr smallint,
				was_order smallint,
				not_order smallint,
				order_yr smallint,
				primary key (orig_no)) ENGINE=MYISAM");
    
    $result = $dbh->do($SQL_STRING);
    
    # We first compute an auxiliary table to help in the computation.  We
    # insert a row for each non-junior taxonomic concept above genus level,
    # listing the current name and rank, as well as tree depth and parent
    # link.
    
    $SQL_STRING = "
		INSERT INTO $INTS_AUX (orig_no, parsen_no, depth, taxon_name, common_name, current_rank)
		SELECT t.orig_no, t.parsen_no, t.depth, t.name, a.common_name, t.rank
		FROM $TREE_WORK as t join $auth_table as a on a.taxon_no = t.spelling_no
		WHERE t.rank > 5 and t.orig_no = t.synonym_no";
    
    $result = $dbh->do($SQL_STRING);
    
    # Then, for each concept, we fill in a count of opinions which in the past
    # assigned the rank of 'phylum', 'class' or 'order' to the concept,
    # vs. those which didn't.
    
    logMessage(2, "    counting phyla");
    
    $SQL_STRING = "
		UPDATE $INTS_AUX as k JOIN
		       (SELECT a.orig_no, count(*) as c, min(if(a.pubyr<>'', a.pubyr, 9999)) as pubyr
			FROM $auth_table as a JOIN $opinion_cache as o on a.taxon_no = o.child_spelling_no
			WHERE taxon_rank = 'phylum'
			GROUP BY a.orig_no) as op using (orig_no)
		SET k.was_phylum = op.c, k.not_phylum = 0, k.phylum_yr = op.pubyr";
    
    $result = $dbh->do($SQL_STRING);
    
    $SQL_STRING = "
		UPDATE $INTS_AUX as k JOIN
		       (SELECT a.orig_no, count(*) as c
			FROM $auth_table as a join $opinion_cache as o on a.taxon_no = o.child_spelling_no
			WHERE taxon_rank <> 'phylum' and taxon_name <> 'Chordata'
			GROUP BY a.orig_no) as op using (orig_no)
		SET k.not_phylum = op.c WHERE k.not_phylum is not null";
    
    $result = $dbh->do($SQL_STRING);
    
    logMessage(2, "    counting classes");
    
    $SQL_STRING = "
		UPDATE $INTS_AUX as k JOIN
		       (SELECT a.orig_no, count(*) as c, min(if(a.pubyr<>'', a.pubyr, 9999)) as pubyr
			FROM $auth_table as a JOIN $opinion_cache as o on a.taxon_no = o.child_spelling_no
			WHERE taxon_rank = 'class'
			GROUP BY a.orig_no) as op using (orig_no)
		SET k.was_class = op.c, k.not_class = 0, k.class_yr = op.pubyr";
    
    $result = $dbh->do($SQL_STRING);
    
    $SQL_STRING = "
		UPDATE $INTS_AUX as k JOIN
		       (SELECT a.orig_no, count(*) as c
			FROM $auth_table as a join $opinion_cache as o on a.taxon_no = o.child_spelling_no
			WHERE taxon_rank <> 'class'
			GROUP BY a.orig_no) as op using (orig_no)
		SET k.not_class = op.c WHERE k.not_class is not null";
    
    $result = $dbh->do($SQL_STRING);
    
    logMessage(2, "    counting orders");
    
    $SQL_STRING = "
		UPDATE $INTS_AUX as k JOIN
		       (SELECT a.orig_no, count(*) as c, min(if(a.pubyr<>'', a.pubyr, 9999)) as pubyr
			FROM $auth_table as a JOIN $opinion_cache as o on a.taxon_no = o.child_spelling_no
			WHERE taxon_rank = 'order'
			GROUP BY a.orig_no) as op using (orig_no)
		SET k.was_order = op.c, k.not_order = 0, k.order_yr = op.pubyr";
    
    $result = $dbh->do($SQL_STRING);
    
    $SQL_STRING = "
		UPDATE $INTS_AUX as k JOIN
		       (SELECT a.orig_no, count(*) as c
			FROM $auth_table as a join $opinion_cache as o on a.taxon_no = o.child_spelling_no
			WHERE taxon_rank <> 'order'
			GROUP BY a.orig_no) as op using (orig_no)
		SET k.not_order = op.c WHERE k.not_order is not null";
    
    $result = $dbh->do($SQL_STRING);
    
    # Now we can fill in the phylogeny table itself.  We start by inserting
    # entries for the top (root) of each tree.
    
    logMessage(2, "    setting top level entries");
    
    $SQL_STRING = "
		INSERT INTO $INTS_WORK (ints_no, common_name, kingdom_no, phylum_no, class_no, order_no, family_no)
		SELECT orig_no, common_name,
			if(current_rank in (22,23), orig_no, null),
			if(current_rank = 20 or was_phylum > 0, orig_no, null),
			if(current_rank = 17 or was_class > 0, orig_no, null),
			if(current_rank = 13 or was_order > 0, orig_no, null),
			if(current_rank = 9 or (current_rank in (6,7,8,10,25) and taxon_name like '%idae'), orig_no, null)
		FROM $INTS_AUX WHERE depth = 1";
    
    $dbh->do($SQL_STRING);
    
    # Then iterate through the each remaining level of the trees.  For each new
    # entry, the classification values are copied from its immediate parent
    # unless the newly entered taxon is itself a better match for one of the
    # classifications.
    
    my ($max_depth) = $dbh->selectrow_array("SELECT max(depth) FROM $TREE_WORK");
    
    foreach my $depth (2..$max_depth)
    {
	logMessage(2, "    computing tree level $depth...") if $depth % 10 == 0;
	
	$SQL_STRING = "
		INSERT INTO $INTS_WORK (ints_no, common_name, kingdom_no, phylum_no, class_no, order_no, family_no)
		SELECT k.orig_no,
			if(k.common_name <> '', k.common_name, p.common_name),
			if(k.current_rank = 23 or (k.current_rank = 22 and ifnull(p.kingdom_no, 1) = 1), k.orig_no, p.kingdom_no) as nk,
			if(k.current_rank in (20,21) or (k.current_rank = 19 and p.phylum_no is null) or k.was_phylum - k.not_phylum > ifnull(xp.was_phylum - xp.not_phylum, 0), k.orig_no, p.phylum_no) as np,
			if(k.current_rank = 17 or k.was_class - k.not_class > ifnull(xc.was_class - xc.not_class, 0), k.orig_no, p.class_no) as nc,
			if(k.current_rank = 13 or k.was_order - k.not_order > ifnull(xo.was_order - xo.not_order, 0), k.orig_no, p.order_no) as no,
			if(k.current_rank = 9 or k.taxon_name like '%idae', k.orig_no, p.family_no) as nf
		FROM $INTS_AUX as k JOIN $INTS_WORK as p on p.ints_no = k.parsen_no
			LEFT JOIN $INTS_AUX as xp on xp.orig_no = p.phylum_no
			LEFT JOIN $INTS_AUX as xc on xc.orig_no = p.class_no
			LEFT JOIN $INTS_AUX as xo on xo.orig_no = p.order_no
		WHERE k.depth = $depth";
	
	$result = $dbh->do($SQL_STRING);
	
	$SQL_STRING = "
		UPDATE $INTS_WORK as i JOIN $INTS_AUX as k on i.ints_no = k.orig_no
				JOIN $INTS_WORK as p on p.ints_no = k.parsen_no
		SET k.aux_rank = if(ifnull(i.kingdom_no, 0) <> ifnull(p.kingdom_no, 0), 23,
				if(ifnull(i.phylum_no, 0) <> ifnull(p.phylum_no, 0), 20,
				 if(ifnull(i.class_no, 0) <> ifnull(p.class_no, 0), 17,
				  if(ifnull(i.order_no, 0) <> ifnull(p.order_no, 0), 13,
				   if(ifnull(i.family_no,0) <> ifnull(p.family_no, 0), 9, 0)))))
		WHERE k.depth = $depth";
	
	$result = $dbh->do($SQL_STRING);
    }
    
    # Then link this table up to the main table.  We start by setting
    # ints_no = orig_no for each row in $TREE_WORK that corresponds to a row in
    # $INTS_WORK.
    
    logMessage(2, "    linking to tree table...");
    
    $SQL_STRING = "UPDATE $TREE_WORK as t join $INTS_WORK as k on t.orig_no = k.ints_no
		   SET t.ints_no = t.orig_no";
    
    $result = $dbh->do($SQL_STRING);
    
    # Then iterate down through the levels of $TREE_WORK.  For each entry
    # that has an ints_no of 0, we set the value to that of the parent.
    
    foreach my $depth (2..$max_depth)
    {
	logMessage(2, "    linking tree level $depth...") if $depth % 10 == 0;
	
	$SQL_STRING = "
		UPDATE $TREE_WORK as t JOIN $TREE_WORK as pt on pt.orig_no = t.parsen_no
		SET t.ints_no = pt.ints_no
		WHERE t.depth = $depth and t.ints_no = 0 and pt.ints_no <> 0";
	
	$result = $dbh->do($SQL_STRING);
    }
    
    # $SQL_STRING = "UPDATE $TREE_WORK
    # 		   SET ints_no = if(ints_no > 0, \@a := ints_no, \@a)
    # 		   ORDER by lft";
    
    # $result = $dbh->do($SQL_STRING);
    
    # We now look for anomalies generated by the algorithm:
    
    # Find everything considered as a family that is higher up the tree than
    # an order, and remove it from the $INTS_WORK table.
    
    $SQL_STRING = "SELECT distinct family_no FROM $INTS_WORK as i join $INTS_AUX as k on k.orig_no = i.ints_no
		   WHERE aux_rank = 13 and family_no is not null";
    
    my $family_list = $dbh->selectcol_arrayref($SQL_STRING);
    
    if ( ref $family_list eq 'ARRAY' and @$family_list > 0 )
    {
	my $family_string = join(q{,}, @$family_list);
	
	$SQL_STRING = "UPDATE $INTS_WORK SET family_no = null
		       WHERE family_no in ($family_string)";
	
	$result = $dbh->do($SQL_STRING);
	
	$SQL_STRING = "UPDATE $TREE_WORK SET rank = if(opinion_rank=9, 25, opinion_rank)
		       WHERE orig_no in ($family_string) and rank = 9";
	
	$result = $dbh->do($SQL_STRING);
	
	logMessage(2, "    removed " . scalar(@$family_list) . " anomalous rank 'family' assignments");
    }
    
    # Find everything considered as an order that is higher up the tree than a
    # class, and remove it from the $INTS_WORK table.
    
    $SQL_STRING = "SELECT distinct order_no FROM $INTS_WORK as i join $INTS_AUX as k on k.orig_no = i.ints_no
		   WHERE aux_rank = 17 and order_no is not null";
    
    my $order_list = $dbh->selectcol_arrayref($SQL_STRING);
    
    if ( ref $order_list eq 'ARRAY' and @$order_list > 0 )
    {
	my $order_string = join(q{,}, @$order_list);
	
	$SQL_STRING = "UPDATE $INTS_WORK SET order_no = null
		       WHERE order_no in ($order_string)";
	
	$result = $dbh->do($SQL_STRING);
	
	$SQL_STRING = "UPDATE $TREE_WORK SET rank = if(opinion_rank=13, 25, opinion_rank)
		       WHERE orig_no in ($order_string) and rank = 13";
	
	$result = $dbh->do($SQL_STRING);
	
	logMessage(2, "    removed " . scalar(@$order_list) . " anomalous rank 'order' assignments");
    }
    
    # Find everything considered as a class that is higher up the tree than a
    # phylum, and remove it from the $INTS_WORK table.
    
    $SQL_STRING = "SELECT distinct class_no FROM $INTS_WORK as i join $INTS_AUX as k on k.orig_no = i.ints_no
		   WHERE aux_rank = 20 and class_no is not null";
    
    my $class_list = $dbh->selectcol_arrayref($SQL_STRING);
    
    if ( ref $class_list eq 'ARRAY' and @$class_list > 0 )
    {
	my $class_string = join(q{,}, @$class_list);
	
	$SQL_STRING = "UPDATE $INTS_WORK SET class_no = null
		       WHERE class_no in ($class_string)";
	
	$result = $dbh->do($SQL_STRING);
	
	$SQL_STRING = "UPDATE $TREE_WORK SET rank = if(opinion_rank=17, 25, opinion_rank)
		       WHERE orig_no in ($class_string) and rank = 17";
	
	$result = $dbh->do($SQL_STRING);
	
	logMessage(2, "    removed " . scalar(@$class_list) . " anomalous 'class' assignments");
    }
    
    # Then fill in the name of each classification taxon.  This will enable
    # us to query for those names later without joining to five separate
    # copies of the authorities table.
    
    $SQL_STRING = "
		UPDATE $INTS_WORK as k
			LEFT JOIN $INTS_AUX as xk on xk.orig_no = k.kingdom_no
			LEFT JOIN $INTS_AUX as xp on xp.orig_no = k.phylum_no
			LEFT JOIN $INTS_AUX as xc on xc.orig_no = k.class_no
			LEFT JOIN $INTS_AUX as xo on xo.orig_no = k.order_no
			LEFT JOIN $INTS_AUX as xf on xf.orig_no = k.family_no
		SET	k.kingdom = xk.taxon_name,
			k.phylum = xp.taxon_name,
			k.class = xc.taxon_name,
			k.`order` = xo.taxon_name,
			k.family = xf.taxon_name";
    
    $result = $dbh->do($SQL_STRING);
    
    # Now we add some extra indices
    
    logMessage(2, "    indexing by classical rank, kingdom, phylum, class, order, family");
    
    $dbh->do("ALTER TABLE $INTS_WORK add key (kingdom_no)");
    $dbh->do("ALTER TABLE $INTS_WORK add key (phylum_no)");
    $dbh->do("ALTER TABLE $INTS_WORK add key (class_no)");
    $dbh->do("ALTER TABLE $INTS_WORK add key (order_no)");
    $dbh->do("ALTER TABLE $INTS_WORK add key (family_no)");
    
    # Then we need to compute the number of species, subgenera, genera, tribes,
    # families, orders, classes and phyla at each level.
    
    logMessage(2, "    computing subtaxon counts");
    
    # Start by initializing the counts.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $COUNTS_WORK");
    $result = $dbh->do("CREATE TABLE $COUNTS_WORK
			       (orig_no int unsigned,
				imm_count int unsigned not null,
				is_phylum tinyint unsigned not null,
				phylum_count int unsigned not null,
				is_class tinyint unsigned not null,
				class_count int unsigned not null,
				is_order tinyint unsigned not null,
				order_count int unsigned not null,
				is_family tinyint unsigned not null,
				family_count int unsigned not null,
				is_genus tinyint unsigned not null,
				genus_count int unsigned not null,
				is_species tinyint unsigned not null,
				species_count int unsigned not null,
				primary key (orig_no)) ENGINE=MYISAM");
    
    $SQL_STRING = "INSERT INTO $COUNTS_WORK
		   SELECT orig_no, 0, 
			rank=20, 0, 
			rank=17, 0, 
			rank=13, 0, 
			rank=9, 0,
			rank=5, 0, 
			rank=3, 0
		   FROM $TREE_WORK
		   WHERE synonym_no = orig_no";
    
    $result = $dbh->do($SQL_STRING);
    
    # $SQL_STRING = "UPDATE $INTS_WORK as i JOIN 
    # 			(SELECT ints_no, count(*) as genus_count FROM $TREE_WORK
    # 			 WHERE rank = 5 GROUP BY ints_no) as g using (ints_no)
    # 		   SET i.genus_count = g.genus_count";
    
    # $result = $dbh->do($SQL_STRING);
    
    # Then iterate up the $COUNTS_WORK table by level from bottom to top,
    # summing each kind of taxon.
    
    foreach my $depth (reverse 2..$max_depth)
    {
	logMessage(2, "    computing tree level $depth...") if $depth % 10 == 0;
	
	$SQL_STRING = "
		UPDATE $COUNTS_WORK as c JOIN
		(SELECT t.parsen_no,
			count(*) as imm_count,
			sum(c.is_species) + sum(c.species_count) as species_count,
			sum(c.is_genus) + sum(c.genus_count) as genus_count,
			sum(c.is_family) + sum(c.family_count) as family_count,
			sum(c.is_order) + sum(c.order_count) as order_count,
			sum(c.is_class) + sum(c.class_count) as class_count,
			sum(c.is_phylum) + sum(c.phylum_count) as phylum_count
		 FROM $COUNTS_WORK as c JOIN $TREE_WORK as t using (orig_no)
		 WHERE t.depth = $depth
		 GROUP BY t.parsen_no) as nc on c.orig_no = nc.parsen_no
		SET c.imm_count = nc.imm_count,
		    c.species_count = nc.species_count,
		    c.genus_count = nc.genus_count,
		    c.family_count = nc.family_count,
		    c.class_count = nc.class_count,
		    c.order_count = nc.order_count,
		    c.phylum_count = nc.phylum_count";
	
	$result = $dbh->do($SQL_STRING);
	
	$SQL_STRING = "
		UPDATE $TREE_WORK as t JOIN $COUNTS_WORK as c using (orig_no)
			LEFT JOIN $INTS_AUX as k using (orig_no)
		SET t.rank = case
				when k.aux_rank = 20 and c.phylum_count = 0 then 20
				when k.aux_rank = 17 and c.class_count = 0 then 17
				when k.aux_rank = 13 and c.order_count = 0 then 13
				when k.aux_rank = 9 and c.family_count = 0 then 9
				else 25
			     end,
		    c.is_phylum = if(k.aux_rank = 20 and c.phylum_count = 0, 1, 0),
		    c.is_class = if(k.aux_rank = 17 and c.class_count = 0, 1, 0),
		    c.is_order = if(k.aux_rank = 13 and c.order_count = 0, 1, 0),
		    c.is_family = if(k.aux_rank = 9 and c.family_count = 0, 1, 0)
		WHERE t.rank = 25 and t.depth = $depth - 1";
	
	$result = $dbh->do($SQL_STRING);
    }
    
    logMessage(2, "computing genera and subgenera (f)");
    
    # Now create a table to hold the "lower" phylogeny information - the genus
    # and subgenus corresponding to each taxon at or below genus level.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $LOWER_WORK");
    $result = $dbh->do("CREATE TABLE $LOWER_WORK
			       (orig_no int unsigned primary key,
				rank tinyint not null,
				genus_no int unsigned,
				genus varchar(80),
				subgenus_no int unsigned,
				subgenus varchar(80)) ENGINE=MYISAM");
    
    logMessage(2, "    adding entries to the lower phylogeny table...");
    
    # Insert all genera into the table
    
    $SQL_STRING = "
		INSERT INTO $LOWER_WORK (orig_no, rank, genus_no, genus)
		SELECT t.orig_no, t.rank, t.orig_no, t.name
		FROM $TREE_WORK as t
		WHERE t.rank = 5";
    
    $result = $dbh->do($SQL_STRING);
    
    logMessage(2, "        added $result genera");
    
    # Then all subgenera
    
    $SQL_STRING = "
		INSERT INTO $LOWER_WORK (orig_no, rank, subgenus_no, subgenus, genus_no, genus)
		SELECT t.orig_no, t.rank, t.orig_no, t.name, t1.orig_no, t1.name
		FROM $TREE_WORK as t LEFT JOIN $TREE_WORK as t1 on t1.orig_no = t.parsen_no
		WHERE t.rank = 4";
    
    $result = $dbh->do($SQL_STRING);
    
    logMessage(2, "        added $result subgenera");
    
    # Then all species
    
    $SQL_STRING = "
		INSERT INTO $LOWER_WORK (orig_no, rank, subgenus_no, subgenus, genus_no, genus)
		SELECT t.orig_no, t.rank,
		       if(t1.rank = 4, t1.orig_no, null),
		       if(t1.rank = 4, t1.name, null),
		       if(t1.rank = 5, t1.orig_no, t2.orig_no),
		       if(t1.rank = 5, t1.name, t2.name)
		FROM $TREE_WORK as t JOIN $TREE_WORK as t1 on t1.orig_no = t.parsen_no
			LEFT JOIN $TREE_WORK as t2 on t2.orig_no = t1.parsen_no
		WHERE t.rank = 3";
    
    $result = $dbh->do($SQL_STRING);
    
    logMessage(2, "        added $result species");
    
    # Then all subspecies
    
    $SQL_STRING = "
		INSERT INTO $LOWER_WORK (orig_no, rank, subgenus_no, subgenus, genus_no, genus)
		SELECT t.orig_no, t.rank,
		       if(t1.rank = 4, t1.orig_no, if(t2.rank = 4, t2.orig_no, null)),
		       if(t1.rank = 4, t1.name, if(t2.rank = 4, t2.name, null)),
		       if(t1.rank = 5, t1.orig_no, if(t2.rank = 5, t2.orig_no, t3.orig_no)),
		       if(t1.rank = 5, t1.name, if(t2.rank = 5, t2.name, t3.name))
		FROM $TREE_WORK as t JOIN $TREE_WORK as t1 on t1.orig_no = t.parsen_no
			LEFT JOIN $TREE_WORK as t2 on t2.orig_no = t1.parsen_no
			LEFT JOIN $TREE_WORK as t3 on t3.orig_no = t2.parsen_no
		WHERE t.rank = 2";
    
    $result = $dbh->do($SQL_STRING);
    
    logMessage(2, "        added $result subspecies");
    
    # Now index the table
    
    logMessage(2, "    indexing the table...");
    
    $dbh->do("ALTER TABLE $LOWER_WORK add key (genus_no)");
    $dbh->do("ALTER TABLE $LOWER_WORK add key (subgenus_no)");
    
    my $a = 1;		# we can stop here when debugging.
}


# computeSearchTable ( dbh )
# 
# Create a table through which taxa can be efficiently and completely found by
# name.  For higher taxa this is trivial, but for species level and below we
# need to deal with sub-genera and synonymous genera.  The goal is to be able
# to find a species name in conjunction with any of its associated genera and
# sub-genera.  To this end, the table includes two kinds of entries: exact and
# inexact.  The exact entries represent actual taxonomic names from the
# authorities table, and the inexact entries represent constructed species
# names where the genus is a synonym of the species' actual genus.
# 
# Queries on this table will return two values: match_no and result_no.  For
# exact entries these identify respectively the taxonomic name that was
# actually matched, and the taxonomic concept to which it belongs.  For
# inexact entries, these identify respectively the matched genus and the
# most senior synonym of the species name.

sub computeSearchTable {

    my ($dbh, $tree_table) = @_;
    
    logMessage(2, "computing search table (g)");
    
    my ($result, $count);
    
    my $auth_table = $TAXON_TABLE{$tree_table}{authorities};
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SEARCH_WORK");
    $result = $dbh->do("CREATE TABLE $SEARCH_WORK
			       (genus varchar(80) not null,
				taxon_name varchar(80) not null,
				taxon_rank enum('','subspecies','species','subgenus','genus','subtribe','tribe','subfamily','family','superfamily','infraorder','suborder','order','superorder','infraclass','subclass','class','superclass','subphylum','phylum','superphylum','subkingdom','kingdom','superkingdom','unranked clade','informal'),
				taxon_no int unsigned not null,
				orig_no int unsigned not null,
				synonym_no int unsigned not null,
				is_current boolean not null,
				is_exact boolean not null,
				common char(2) not null,
				KEY (taxon_name, genus),
				UNIQUE KEY (taxon_no, genus, common)) ENGINE=MYISAM");
    
    # We start by copying all higher taxa into the search table.  That's the
    # easy part.
    
    logMessage(2, "    adding higher taxa...");
    
    $count = $dbh->do("
	INSERT INTO $SEARCH_WORK (taxon_name, taxon_rank, taxon_no, orig_no, 
				  synonym_no, is_current, is_exact)
	SELECT taxon_name, taxon_rank, taxon_no, orig_no, synonym_no, (taxon_no = spelling_no), 1
	FROM $auth_table as a join $TREE_WORK as t using (orig_no)
	WHERE taxon_rank not in ('subgenus', 'species', 'subspecies')");
    
    logMessage(2, "      found $count names");
    
    # The subgenera are a bit trickier, because most (but not all) are named
    # as "Genus (Subgenus)".  The following expression will extract the
    # subgenus name from this pattern, and will also properly treat the
    # (incorrect) names that lack a subgenus component by simply returning the
    # whole name.
    
    logMessage(2, "    adding subgenera...");
    
    $count = $dbh->do("
	INSERT INTO $SEARCH_WORK (genus, taxon_name, taxon_rank, taxon_no, orig_no,
				  synonym_no, is_current, is_exact)
	SELECT substring_index(taxon_name, ' ', 1),
		trim(trailing ')' from substring_index(taxon_name,'(',-1)),
	        taxon_rank, taxon_no, orig_no, synonym_no, (taxon_no = spelling_no), 1
	FROM $auth_table as a join $TREE_WORK as t using (orig_no)
	WHERE taxon_rank = 'subgenus'");
    
    logMessage(2, "      found $count names");
    
    # For species and sub-species, we split off the first component of each
    # name as the genus name and add the resulting entries to the table.  Note
    # that some species names also have a subgenus component which has to be
    # split out as well.
    
    logMessage(2, "    adding species by name...");
    
    # Species which aren't in a subgenus
    
    $count = $dbh->do("
	INSERT IGNORE INTO $SEARCH_WORK (genus, taxon_name, taxon_rank, taxon_no, orig_no,
					 synonym_no, is_current, is_exact)
	SELECT substring_index(taxon_name, ' ', 1),
		trim(substring(taxon_name, locate(' ', taxon_name)+1)),
		taxon_rank, taxon_no, orig_no, synonym_no, (taxon_no = spelling_no), 1
	FROM $auth_table as a join $TREE_WORK as t using (orig_no)
	WHERE taxon_rank in ('species', 'subspecies') and taxon_name not like '%(%'");
    
    # Species which do have a subgenus
    
    $count += $dbh->do("
	INSERT IGNORE INTO $SEARCH_WORK (genus, taxon_name, taxon_rank, taxon_no, orig_no,
					 synonym_no, is_current, is_exact)
	SELECT substring_index(taxon_name, ' ', 1),
		trim(substring(taxon_name, locate(') ', taxon_name)+2)),
		taxon_rank, taxon_no, orig_no, synonym_no, (taxon_no = spelling_no), 1
	FROM $auth_table as a join $TREE_WORK as t using (orig_no)
	WHERE taxon_rank in ('species', 'subspecies') and taxon_name like '%(%'");
    
    # And again with the subgenus name treated as if it was a genus
    
    $count += $dbh->do("
	INSERT IGNORE INTO $SEARCH_WORK (genus, taxon_name, taxon_rank, taxon_no, orig_no,
					 synonym_no, is_current, is_exact)
	SELECT substring_index(substring_index(taxon_name, '(', -1), ')', 1),
		trim(substring(taxon_name, locate(') ', taxon_name)+2)),
		taxon_rank, taxon_no, orig_no, synonym_no, (taxon_no = spelling_no), 1
	FROM $auth_table as a join $TREE_WORK as t using (orig_no)
	WHERE taxon_rank in ('species', 'subspecies') and taxon_name like '%(%'");
    
    logMessage(2, "      found $count names");
    
    # Now comes the really tricky part.  For the purposes of "loose matching"
    # we also want to list each species under any genera and subgenera
    # synonymous with the actual genus and/or subgenus.  Note that the genus
    # under which a species is placed in the hierarchy may not be in accord
    # with its listed name!
    # 
    # In order to do this efficiently, we first need to create an auxiliary
    # table associating each species and subspecies with a genus/subgenus.
    
    logMessage(2, "    adding species with synonym genera...");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SPECIES_AUX");
    $result = $dbh->do("CREATE TABLE $SPECIES_AUX
			       (genus varchar(80) not null,
				taxon_name varchar(80) not null,
				taxon_no int unsigned not null,
				orig_no int unsigned not null,
				unique key (taxon_name, genus)) ENGINE=MYISAM");
    
    # Now for each species and subspecies, create an entry corresponding to each
    # genus that is synonymous to its current genus.  The result_no will be the
    # current senior synonym of the species/subspecies.
    
    $SQL_STRING = "
		INSERT IGNORE INTO $SPECIES_AUX (taxon_no, orig_no, genus, taxon_name)
		SELECT a.taxon_no, a.orig_no,
			ifnull(p1.taxon_name, ifnull(p2.taxon_name, p3.taxon_name)),
			if(a.taxon_name like '%(%',
			   trim(substring(a.taxon_name, locate(') ', a.taxon_name)+2)),
			   trim(substring(a.taxon_name, locate(' ', a.taxon_name)+1)))
		FROM $auth_table as a JOIN $TREE_WORK as t using (orig_no)
			LEFT JOIN $TREE_WORK as t1 on t1.orig_no = t.parent_no
			LEFT JOIN $auth_table as p1 on p1.taxon_no = t1.spelling_no
				and p1.taxon_rank = 'genus'
			LEFT JOIN $TREE_WORK as t2 on t2.orig_no = t1.parent_no
			LEFT JOIN $auth_table as p2 on p2.taxon_no = t2.spelling_no
				and p2.taxon_rank = 'genus'
			LEFT JOIN $TREE_WORK as t3 on t3.orig_no = t2.parent_no
			LEFT JOIN $auth_table as p3 on p3.taxon_no = t3.spelling_no
				and p3.taxon_rank = 'genus'
		WHERE a.taxon_rank in ('species', 'subspecies') and
			(p1.taxon_no is not null or
			 p2.taxon_no is not null or
			 p3.taxon_no is not null)";
    
    $result = $dbh->do($SQL_STRING);
    
    # Then do the same for each subgenus that is synonymous to its current
    # subgenus (if any).
    
    $SQL_STRING = "
		INSERT IGNORE INTO $SPECIES_AUX (taxon_no, orig_no, genus, taxon_name)
		SELECT a.taxon_no, a.orig_no,
			ifnull(trim(trailing ')' from substring_index(p1.taxon_name,'(',-1)),
			       trim(trailing ')' from substring_index(p2.taxon_name,'(',-1))),
			if(a.taxon_name like '%(%',
			   trim(substring(a.taxon_name, locate(') ', a.taxon_name)+2)),
			   trim(substring(a.taxon_name, locate(' ', a.taxon_name)+1)))
		FROM $auth_table as a JOIN $TREE_WORK as t using (orig_no)
			LEFT JOIN $TREE_WORK as t1 on t1.orig_no = t.parent_no
			LEFT JOIN $auth_table as p1 on p1.taxon_no = t1.spelling_no
				and p1.taxon_rank = 'subgenus'
			LEFT JOIN $TREE_WORK as t2 on t2.orig_no = t1.parent_no
			LEFT JOIN $auth_table as p2 on p2.taxon_no = t2.spelling_no
				and p2.taxon_rank = 'subgenus'
		WHERE a.taxon_rank in ('species', 'subspecies') and
			(p1.taxon_no is not null or
			 p2.taxon_no is not null)";
    
    $result = $dbh->do($SQL_STRING);
    
    # Now that we have this auxiliary table, we can add additional "inexact"
    # entries to be used for loose matching.  This way, a loose search on a
    # species name will hit if the specified genus is synonymous to the actual
    # current genus.
    
    # First delete all entries that match anything in $SEARCH_WORK, because we
    # only want to do loose matching where there isn't already an exact match.
    
    $SQL_STRING = "
	DELETE $SPECIES_AUX
	FROM $SPECIES_AUX JOIN $SEARCH_WORK
		on $SPECIES_AUX.genus = $SEARCH_WORK.genus and
		   $SPECIES_AUX.taxon_name = $SEARCH_WORK.taxon_name";
    
    $result = $dbh->do($SQL_STRING);
    
    # Then generate new entries in $SEARCH_WORK.
    
    $SQL_STRING = "
	INSERT IGNORE INTO $SEARCH_WORK (genus, taxon_name, taxon_rank, taxon_no, orig_no,
					 synonym_no, is_current, is_exact)
	SELECT sx.genus, sx.taxon_name, a.taxon_rank, sx.taxon_no, sx.orig_no,
	       t.synonym_no, (t.spelling_no = sx.taxon_no), 0
	FROM $SPECIES_AUX as sx JOIN $auth_table as a on a.taxon_no = sx.taxon_no
		JOIN $TREE_WORK as t on sx.orig_no = t.orig_no";
    
    $count = $dbh->do($SQL_STRING);
    
    logMessage(2, "      found $count implied names");
    
    # Next, we add all of the common names.  Each of these gets a 'language'
    # value, to distinguish them from latin names.
    
    logMessage(2, "    adding common names...");
    
    $SQL_STRING = "
	INSERT IGNORE INTO $SEARCH_WORK (taxon_name, taxon_rank, taxon_no, orig_no,
					 synonym_no, is_current, is_exact, common)
	SELECT common_name, rank, spelling_no, orig_no, synonym_no, 0, 1, 'EN'
	FROM $auth_table as a join $TREE_WORK as t using (orig_no)
	WHERE common_name <> '' and taxon_no = spelling_no";
    
    $count = $dbh->do($SQL_STRING);
    
    logMessage(2, "      found $count names");
    
    # We can stop here when debugging.
    
    my $a = 1;
}


# updateSearchTable ( dbh, search_table )
# 
# Update the specified search table to contain entries for each taxon
# represented in $TREE_WORK.

sub updateSearchTable {

    my ($dbh, $search_table, $tree_table) = @_;
    
    logMessage(2, "computing search table (f)");
    
    my $result;
    
    my $auth_table = $TAXON_TABLE{$tree_table}{authorities};
    
    # We start by copying all higher taxa into the search table.
    
    logMessage(2, "    adding higher taxa");
    
    $result = $dbh->do("
		INSERT IGNORE INTO $SEARCH_WORK (genus, subgenus, taxon_name, taxon_no)
		SELECT null, null, taxon_name, taxon_no
		FROM $auth_table JOIN $TREE_WORK using (orig_no)
		WHERE taxon_rank not in ('subgenus', 'species', 'subspecies')");
    
    # The subgenera are a bit trickier, because most (but not all) are named
    # as "Genus (Subgenus)".  The following expression will extract the
    # subgenus name from this pattern, and will also properly treat taxa
    # without a subgenus component by simply returning the whole name.
    
    $result = $dbh->do("
		INSERT IGNORE INTO $SEARCH_WORK (genus, subgenus, taxon_name, taxon_no)
		SELECT substring_index(taxon_name, ' ', 1), null,
			trim(trailing ')' from substring_index(taxon_name,'(',-1)),
			taxon_no
		FROM $auth_table JOIN $TREE_WORK using (orig_no)
		WHERE taxon_rank = 'subgenus'");
    
    # For species and sub-species, we split off the first component of each
    # name as the genus name and add the resulting entries to the table.  Note
    # that some species names also have a subgenus component, which must be
    # skipped.
    
    logMessage(2, "    adding species by name");
    
    $result = $dbh->do("
		INSERT IGNORE INTO $SEARCH_WORK (genus, subgenus, taxon_name, taxon_no)
		SELECT substring_index(taxon_name, ' ', 1),
			substring_index(substring_index(taxon_name, '(', -1), ')', 1)
			trim(substring(taxon_name, locate(') ', taxon_name)+2)),
			taxon_no
		FROM $auth_table JOIN $TREE_WORK using (orig_no)
		WHERE taxon_rank in ('species', 'subspecies')
			and taxon_name like '%(%'");
    
    $result = $dbh->do("
		INSERT IGNORE INTO $SEARCH_WORK (genus, subgenus, taxon_name, taxon_no)
		SELECT substring_index(taxon_name, ' ', 1),
			null,
			trim(substring(taxon_name, locate(' ', taxon_name)+1),
			taxon_no
		FROM $auth_table JOIN $TREE_WORK using (orig_no)
		WHERE taxon_rank in ('species', 'subspecies')
			and taxon_name not like '%(%'");
    
    # We will wait until the next table rebuild to add in entries based on the
    # hierarchy for synonymous genera and subgenera.
    
    # We can stop here when debugging.
    
    my $a = 1;
}


# computeAttrsTable ( dbh )
# 
# Create a table by which bottom-up attributes such as max_body_mass and
# min_body_mass may be looked up.  These attributes propagate upward through
# the taxonomic hierarchy; for example, the value of min_body_mass for a taxon
# would be the mininum value of that attribute for all of its children.

sub computeAttrsTable {

    my ($dbh, $tree_table) = @_;
    
    logMessage(2, "computing attrs table (h)");
    
    my ($result, $sql);
    
    my $auth_table = $TAXON_TABLE{$tree_table}{authorities};
    my $opinion_cache = $TAXON_TABLE{$tree_table}{opcache};
    
    # Create the taxon summary table if it doesn't already exist.
    
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $OCC_TAXON (
				orig_no int unsigned primary key,
				n_occs int unsigned not null,
				n_colls int unsigned not null,
				first_early_age decimal(9,5),
				first_late_age decimal(9,5),
				last_early_age decimal(9,5),
				last_late_age decimal(9,5),
				early_occ int unsigned,
				late_occ int unsigned) ENGINE=MyISAM");
    
    # Same for the image selection table.
    
    selectPics($dbh);
    
    # Create a table through which bottom-up attributes such as body_mass and
    # extant_children can be looked up.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $ATTRS_WORK");
    $result = $dbh->do("CREATE TABLE $ATTRS_WORK
			       (orig_no int unsigned not null,
				is_valid boolean,
				is_senior boolean,
				is_extant boolean,
				extant_children smallint,
				distinct_children smallint,
				extant_size int unsigned,
				taxon_size int unsigned,
				n_occs int unsigned not null,
				n_colls int unsigned not null,
				min_body_mass float,
				max_body_mass float,
				precise_age boolean,
				not_trace boolean,
				first_early_age decimal(9,5),
				first_late_age decimal(9,5),
				last_early_age decimal(9,5),
				last_late_age decimal(9,5),
				early_occ int unsigned,
				late_occ int unsigned,
				image_no int unsigned,
				PRIMARY KEY (orig_no)) ENGINE=MYISAM");
    
    # Prime the new table with values from the authorities table and
    # tree table.
    
    logMessage(2, "    seeding table with initial taxon information...");
    
    $sql = "    INSERT IGNORE INTO $ATTRS_WORK 
			(orig_no, is_valid, is_senior, is_extant, extant_children, distinct_children, 
			 extant_size, taxon_size, n_colls, n_occs, not_trace)
		SELECT a.orig_no,
			if(t.status in ($VALID_STATUS), 1, 0) as is_valid,
			if(t.status not in ($SYNONYM_STATUS), 1, 0) as is_senior,
			sum(if(a.extant = 'yes', 1, if(a.extant = 'no', 0, null))) as is_extant,
			0, 0, 0, 0, 0, 0, a.preservation <> 'trace' or a.preservation is null
		FROM $auth_table as a JOIN $TREE_WORK as t using (orig_no)
		GROUP BY a.orig_no";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      added $result rows.");
    
    $sql = "    UPDATE $ATTRS_WORK
		SET is_extant = 1 WHERE is_extant > 1";
    
    $result = $dbh->do($sql);
    
    # Test whether the $OCC_TAXON table exists and has any entries in it.  If
    # so, add information from that table into $ATTRS_WORK.
    
    my ($occ_taxon) = eval {
	local($dbh->{PrintError}) = 0;
	$dbh->selectrow_array("SELECT count(*) FROM $OCC_TAXON");
    };
    
    if ( $occ_taxon > 0 )
    {
	logMessage(2, "      adding taxon summary information from table '$OCC_TAXON'...");
	
	$sql = "UPDATE $ATTRS_WORK as v JOIN $OCC_TAXON as tsum using (orig_no)
		SET v.n_occs = tsum.n_occs,
		    v.first_early_age = tsum.first_early_age,
		    v.first_late_age = tsum.first_late_age,
		    v.last_early_age = tsum.last_early_age,
		    v.last_late_age = tsum.last_late_age,
		    v.precise_age = tsum.precise_age,
		    v.early_occ = tsum.early_occ,
		    v.late_occ = tsum.late_occ";
	
	$result = $dbh->do($sql);
	
	logMessage(2, "      updated $result rows.");
    }
    
    else
    {
	logMessage(2, "      skipping taxon summary information: table '$OCC_TAXON'");
	logMessage(2, "          does not exist or does not contain any rows");
    }
    
    # Test whether the ecotaph table exists and has any entries in it.  If so,
    # add information from that table into $ATTRS_WORK.
    
    my ($ecotaph) = eval {
	local($dbh->{PrintError}) = 0;
	$dbh->selectrow_array("SELECT count(*) FROM ecotaph");
    };
    
    if ( $ecotaph > 0 )
    {
	logMessage(2, "      adding body mass information from table 'ecotaph'...");
	
	$sql = "UPDATE $ATTRS_WORK as v JOIN
		   (SELECT a.orig_no, coalesce(e.minimum_body_mass, e.body_mass_estimate) as min,
			   coalesce(e.maximum_body_mass, e.body_mass_estimate) as max
		    FROM $auth_table as a JOIN ecotaph as e using (taxon_no)
		    GROUP BY a.orig_no) as e using (orig_no)
		SET v.min_body_mass = e.min,
		    v.max_body_mass = e.max";
	
	$result = $dbh->do($sql);
	
	logMessage(2, "      updated $result rows.");
    }
    
    else
    {
	logMessage(2, "      skipping body mass information: table 'ecotaph'");
	logMessage(2, "          does not exist or does not contain any rows");
    }
    
    # Test whether the $TAXON_PICS table exists and has any entries in it.  If
    # so, add information from that table into $ATTRS_WORK.
    
    my ($taxon_pics) = eval {
	local($dbh->{PrintError}) = 0;
	$dbh->selectrow_array("SELECT count(*) FROM $TAXON_PICS");
    };
    
    if ( $taxon_pics > 0 )
    {
	logMessage(2, "      adding taxon picture information from table '$TAXON_PICS'...");
	
	$sql = "UPDATE $ATTRS_WORK as v JOIN $TAXON_PICS as pic using (orig_no)
		SET v.image_no = if(pic.priority is null or pic.priority <> -1, pic.image_no, null)";
	
	$result = $dbh->do($sql);	
	
	logMessage(2, "      updated $result rows.");
    }
    
    else
    {
	logMessage(2, "      skipping taxon picture information: table '$TAXON_PICS'");
	logMessage(2, "          does not exist or does not contain any rows");
    }
    
    # Then coalesce the basic attributes across each synonym group, using
    # these to set the attributes for the senior synonym in each group.
    
    logMessage(2, "    coalescing attributes to senior synonyms");
    
    $sql = "	UPDATE $ATTRS_WORK as v JOIN 
		(SELECT t.synonym_no,
			max(v.is_extant) as is_extant,
			min(v.min_body_mass) as min_body_mass,
			max(v.max_body_mass) as max_body_mass,
			sum(v.n_occs) as n_occs,
    			max(v.first_early_age) as first_early_age,
			max(v.first_late_age) as first_late_age,
			min(v.last_early_age) as last_early_age,
			min(v.last_late_age) as last_late_age,
			max(v.precise_age) as precise_age,
			max(v.not_trace) as not_trace,
			v.image_no
		 FROM $ATTRS_WORK as v JOIN $TREE_WORK as t using (orig_no)
		 GROUP BY t.synonym_no) as nv on v.orig_no = nv.synonym_no
		SET     v.is_extant = nv.is_extant,
			v.n_occs = nv.n_occs,
			v.min_body_mass = nv.min_body_mass,
			v.max_body_mass = nv.max_body_mass,
			v.first_early_age = nv.first_early_age,
			v.first_late_age = nv.first_late_age,
			v.last_early_age = nv.last_early_age,
			v.last_late_age = nv.last_late_age,
			v.precise_age = nv.precise_age,
			v.not_trace = nv.not_trace,
			v.image_no = ifnull(v.image_no, nv.image_no)";
    
    $result = $dbh->do($sql);
    
    # Redo the calculation of age ranges just for those taxa with a precise
    # age.  If the result for a given taxon is defined (i.e. at least one
    # synonym has a precise age) then substitute it.
    
    $sql = "	UPDATE $ATTRS_WORK as V JOIN
		(SELECT t.synonym_no,
    			max(v.first_early_age) as first_early_age,
			max(v.first_late_age) as first_late_age,
			min(v.last_early_age) as last_early_age,
			min(v.last_late_age) as last_late_age
		 FROM $ATTRS_WORK as v JOIN $TREE_WORK as t using (orig_no)
		 WHERE v.precise_age
		 GROUP BY t.synonym_no) as nv on v.orig_no = nv.synonym_no
		SET	v.first_early_age = ifnull(nv.first_early_age, v.first_early_age),
			v.first_late_age = ifnull(nv.first_late_age, v.first_late_age),
			v.last_early_age = ifnull(nv.last_early_age, v.last_early_age),
			v.last_late_age = ifnull(nv.last_late_age, v.last_late_age)";
    
    $result = $dbh->do($sql);
    
    # Now figure out how deep the table goes.
    
    my ($max_depth) = $dbh->selectrow_array("SELECT max(depth) FROM $TREE_WORK");
    
    # We then iterate from that depth up to the top of the tree, computing
    # each row from its immediate children and then coalescing the attributes
    # across synonym groups.
    
    for (my $depth = $max_depth; $depth > 0; $depth--)
    {
	logMessage(2, "    computing tree level $depth...") if $depth % 10 == 0;
	
	my $child_depth = $depth + 1;
	
	# First coalesce the attributes of each parent with those of all of
	# its children that are not junior synonyms (except for the first/last
	# interval numbers).
	
	my $sql = "
		UPDATE $ATTRS_WORK as v JOIN
		(SELECT t.parent_no,
			if(sum(v.is_extant) > 0, 1, pv.is_extant) as is_extant,
			coalesce(sum(v.is_extant), 0) as extant_children,
			count(v.orig_no) as distinct_children,
			sum(v.extant_size) as extant_size,
			sum(v.taxon_size) as taxon_size,
			sum(v.n_occs) + pv.n_occs as n_occs,
			coalesce(least(min(v.min_body_mass), pv.min_body_mass), 
					min(v.min_body_mass), pv.min_body_mass) as min_body_mass, 
			coalesce(greatest(max(v.max_body_mass), pv.max_body_mass),
					max(v.max_body_mass), pv.max_body_mass) as max_body_mass,
			if(max(v.not_trace) > 0, 1, pv.not_trace) as not_trace
		 FROM $ATTRS_WORK as v JOIN $TREE_WORK as t using (orig_no)
			LEFT JOIN $ATTRS_WORK as pv on pv.orig_no = t.parent_no 
		 WHERE t.depth = $child_depth and v.is_valid and v.is_senior
		 GROUP BY t.parent_no) as nv on v.orig_no = nv.parent_no
		SET     v.is_extant = nv.is_extant,
			v.extant_children = nv.extant_children,
			v.distinct_children = nv.distinct_children,
			v.extant_size = nv.extant_size,
			v.n_occs = nv.n_occs,
			v.taxon_size = nv.taxon_size,
			v.min_body_mass = nv.min_body_mass,
			v.max_body_mass = nv.max_body_mass,
			v.not_trace = nv.not_trace";
	
	$result = $dbh->do($sql);
	
	# Now do the same for the first/last interval numbers.  The reason
	# this needs to be a separate statement is so that we can ignore taxa
	# whose ages are imprecise.
	
	# my $sql = "
	# 	UPDATE $ATTRS_WORK as v JOIN
	# 	(SELECT t.parent_no,
	# 		coalesce(greatest(max(v.first_early_age), pv.first_early_age), 
	# 			max(v.first_early_age), pv.first_early_age) as first_early_age,
	# 		coalesce(greatest(max(v.first_late_age), pv.first_late_age), 
	# 			max(v.first_late_age), pv.first_late_age) as first_late_age,
	# 		coalesce(least(min(v.last_early_age), pv.last_early_age), 
	# 			min(v.last_early_age), pv.last_early_age) as last_early_age,
	# 		coalesce(least(min(v.last_late_age), pv.last_late_age), 
	# 			min(v.last_late_age), pv.last_late_age) as last_late_age,
	# 		if(pv.precise_age = true, true, max(v.precise_age)) as precise_age
	# 	 FROM $ATTRS_WORK as v JOIN $TREE_WORK as t using (orig_no)
	# 		LEFT JOIN $ATTRS_WORK as pv on pv.orig_no = t.parent_no
	# 	 WHERE t.depth = $child_depth and v.is_valid and v.is_senior and v.not_trace
	# 	 GROUP BY t.parent_no) as nv on v.orig_no = nv.parent_no
	# 	SET	v.first_early_age = nv.first_early_age,
	# 		v.first_late_age = nv.first_late_age,
	# 		v.last_early_age = nv.last_early_age,
	# 		v.last_late_age = nv.last_late_age,
	# 		v.precise_age = nv.precise_age";
	
	# $result = $dbh->do($sql);
	
	# Then coalesce attributes across synonym groups.
	
	$sql = "
		UPDATE $ATTRS_WORK as v JOIN 
		(SELECT t.synonym_no,
			max(v.is_extant) as is_extant,
			sum(v.extant_children) as extant_children_sum,
			sum(v.distinct_children) as distinct_children_sum,
			sum(v.extant_size) as extant_size_sum,
			sum(v.taxon_size) as taxon_size_sum,
			sum(v.n_occs) as n_occs,
			min(v.min_body_mass) as min_body_mass,
			max(v.max_body_mass) as max_body_mass,
			max(v.not_trace) as not_trace
		FROM $ATTRS_WORK as v JOIN $TREE_WORK as t using (orig_no)
		WHERE t.depth = $depth and v.is_valid
		GROUP BY t.synonym_no) as nv on v.orig_no = nv.synonym_no
		SET     v.is_extant = nv.is_extant,
			v.extant_children = nv.extant_children_sum,
			v.distinct_children = nv.distinct_children_sum,
			v.extant_size = nv.extant_size_sum + if(nv.is_extant, 1, 0),
			v.taxon_size = nv.taxon_size_sum + 1,
			v.n_occs = nv.n_occs,
			v.min_body_mass = nv.min_body_mass,
			v.max_body_mass = nv.max_body_mass,
			v.not_trace = nv.not_trace";
	
	$result = $dbh->do($sql);
	
	# Now propagate appearance ages and occurrences.  The logic involved
	# in this is too complicated for SQL, so we must download the
	# occurrences, do the computation, then upload them again.
	
	my $parent_depth = $depth - 1;
	
	$sql = "SELECT t.orig_no, t.synonym_no, t.parent_no, t.depth, v.taxon_size,
		       v.first_early_age, v.first_late_age, v.last_early_age, v.last_late_age,
		       v.early_occ, v.late_occ, v.precise_age, v.not_trace
		FROM $ATTRS_WORK as v join $TREE_WORK as t using (orig_no)
		WHERE t.depth in ($depth, $child_depth)";
	
	my $sth = $dbh->prepare($sql);
	$sth->execute;
	
	my $rows = $sth->fetchall_arrayref({});
	
	my %coalesce;
	
	foreach my $row (@$rows)
	{
	    # First, determine which entry each row should be coalesced into.
	    
	    my $orig_no = $row->{depth} == $depth ? $row->{synonym_no} : $row->{parent_no};
	    
	    # If the coalesce cell is empty, set it to the current row and go
	    # on to the next even if the age is not precise.
	    
	    unless ( $coalesce{$orig_no} )
	    {
		$coalesce{$orig_no} = $row;
		next;
	    }
	    
	    # If it does not have a precise age, and the current one either
	    # does have a precise age or is the parent cell, then replace it.
	    
	    if ( ($row->{depth} == $depth or $row->{precise_age}) and not $coalesce{$orig_no}{precise_age} )
	    {
		$coalesce{$orig_no} = $row;
		next;
	    }
	    
	    # Otherwise, ignore this row unless it has a precise age.
	    
	    next unless $row->{precise_age};
	    
	    # If the age bounds are outside what is already recorded in the
	    # coalesce cell, set the new bounds and representative
	    # occurrences.
	    
	    my $coalesce = $coalesce{$orig_no};
	    
	    if ( $row->{first_early_age} > $coalesce->{first_early_age} )
	    {
		$coalesce->{first_early_age} = $row->{first_early_age};
	    }
	    
	    if ( $row->{first_late_age} > $coalesce->{first_late_age} )
	    {
		$coalesce->{first_late_age} = $row->{first_late_age};
		$coalesce->{early_occ} = $row->{early_occ};
	    }
	    
	    if ( $row->{last_early_age} < $coalesce->{last_early_age} )
	    {
		$coalesce->{last_early_age} = $row->{last_early_age};
		$coalesce->{late_occ} = $row->{late_occ};
	    }
	    
	    if ( $row->{last_late_age} < $coalesce->{last_late_age} )
	    {
		$coalesce->{last_late_age} = $row->{last_late_age};
	    }
	}
	
	# Now we set all of the coalesced values.
	
	foreach my $orig_no (keys %coalesce)
	{
	    my $pa = $coalesce{$orig_no}{precise_age} // 'NULL';
	    my $fea = $coalesce{$orig_no}{first_early_age} // 'NULL';
	    my $fla = $coalesce{$orig_no}{first_late_age} // 'NULL';
	    my $lea = $coalesce{$orig_no}{last_early_age} // 'NULL';
	    my $lla = $coalesce{$orig_no}{last_late_age} // 'NULL';
	    my $eocc = $coalesce{$orig_no}{early_occ} // 'NULL';
	    my $locc = $coalesce{$orig_no}{late_occ} // 'NULL';
	    
	    $result = $dbh->do("
		UPDATE $ATTRS_WORK
		SET precise_age = $pa,
		    first_early_age = $fea,
		    first_late_age = $fla,
		    last_early_age = $lea,
		    last_late_age = $lla,
		    early_occ = $eocc,
		    late_occ = $locc
		WHERE orig_no = $orig_no");
	    
	    my $a = 1;	# we can stop here when debugging
	}
	
	# However, we also want to sum these up using valid_no, so that
	# invalid subgroups, etc. will have accurate counts.
	
	# $sql = "
	# 	UPDATE $ATTRS_WORK as v JOIN 
	# 	(SELECT t.valid_no,
	# 		if(sum(v.is_extant) > 0, 1, coalesce(is_extant)) as is_extant,
	# 		sum(v.extant_children) as extant_children_sum,
	# 		sum(v.distinct_children) as distinct_children_sum,
	# 		sum(v.extant_size) as extant_size_sum,
	# 		sum(v.taxon_size) as taxon_size_sum,
	# 		sum(v.n_occs) as n_occs,
	# 		min(v.min_body_mass) as min_body_mass,
	# 		max(v.max_body_mass) as max_body_mass,
	# 		max(v.first_early_age) as first_early_age,
	# 		max(v.first_late_age) as first_late_age,
	# 		min(v.last_early_age) as last_early_age,
	# 		min(v.last_late_age) as last_late_age,
	# 		max(v.precise_age) as precise_age,
	# 		if(sum(v.not_trace) > 0, 1, 0) as not_trace
	# 	FROM $ATTRS_WORK as v JOIN $TREE_WORK as t using (orig_no)
	# 	WHERE t.depth = $depth and t.valid_no <> t.synonym_no
	# 	GROUP BY t.valid_no) as nv on v.orig_no = nv.valid_no
	# 	SET     v.is_extant = nv.is_extant,
	# 		v.extant_children = nv.extant_children_sum,
	# 		v.distinct_children = nv.distinct_children_sum,
	# 		v.extant_size = nv.extant_size_sum + if(nv.is_extant, 1, 0),
	# 		v.taxon_size = nv.taxon_size_sum + 1,
	# 		v.n_occs = nv.n_occs,
	# 		v.min_body_mass = nv.min_body_mass,
	# 		v.max_body_mass = nv.max_body_mass,
	# 		v.first_early_age = nv.first_early_age,
	# 		v.first_late_age = nv.first_late_age,
	# 		v.last_early_age = nv.last_early_age,
	# 		v.last_late_age = nv.last_late_age,
	# 		v.precise_age = nv.precise_age,
	# 		v.not_trace = nv.not_trace";
	
	# $result = $dbh->do($sql);
    }
    
    # Finally, we iterate from the top of the tree back down, computing those
    # attributes that propagate downward.  For now, these include:
    # - a value of extant=0 (which overrides any values of extant=null)
    # - image_no values
    
    for (my $row = 2; $row <= $max_depth; $row++)
    {
	logMessage(2, "    computing tree level $row...") if $row % 10 == 0;
	
	# Children inherit the attributes of the senior synonym of the parent.
	
	my $sql = "
		UPDATE $ATTRS_WORK as v JOIN $TREE_WORK as t using (orig_no)
			JOIN $ATTRS_WORK as pv on pv.orig_no = t.parsen_no and t.depth = $row
		SET v.is_extant = 0 WHERE pv.is_extant = 0";
	
	$dbh->do($sql);
	
	# my $sql = "
	# 	UPDATE $ATTRS_WORK as v JOIN $TREE_WORK as t using (orig_no)
	# 		JOIN $ATTRS_WORK as pv on pv.orig_no = t.parsen_no and t.depth = $row
	# 	SET v.first_early_age = pv.first_early_age,
	# 	    v.first_late_age = pv.first_late_age,
	# 	    v.last_early_age = pv.last_early_age,
	# 	    v.last_late_age = pv.last_late_age,
	# 	    v.precise_age = false
	# 	WHERE not(v.precise_age) and pv.first_early_age is not null";
	
	# $dbh->do($sql);
	
	my $sql = "
		UPDATE $ATTRS_WORK as v JOIN $TREE_WORK as t using (orig_no)
			JOIN $ATTRS_WORK as pv on pv.orig_no = t.parsen_no and t.depth = $row
		SET v.image_no = pv.image_no WHERE v.image_no is null or v.image_no = 0";
	
	$dbh->do($sql);
    }
    
    # Now we have to copy the attributes of senior synonyms to all of their
    # junior synonyms.
    
    logMessage(2, "    setting attributes for junior synonyms");
    
    $result = $dbh->do("
		UPDATE $ATTRS_WORK as v JOIN $TREE_WORK as t on v.orig_no = t.orig_no
			JOIN $ATTRS_WORK as sv on sv.orig_no = t.synonym_no and sv.orig_no <> v.orig_no
		SET	v.is_extant = sv.is_extant,
			v.extant_children = sv.extant_children,
			v.distinct_children = sv.distinct_children,
			v.extant_size = sv.extant_size,
			v.taxon_size = sv.taxon_size,
			v.n_occs = sv.n_occs,
			v.min_body_mass = sv.min_body_mass,
			v.max_body_mass = sv.max_body_mass,
			v.first_early_age = sv.first_early_age,
			v.first_late_age = sv.first_late_age,
			v.last_early_age = sv.last_early_age,
			v.last_late_age = sv.last_late_age,
			v.precise_age = sv.precise_age,
			v.early_occ = sv.early_occ,
			v.late_occ = sv.late_occ,
			v.not_trace = sv.not_trace");
    
    # We can stop here when debugging.
    
    my $a = 1;
}


sub rebuildAttrsTable {
    
    my ($dbh, $tree_table) = @_;
    
    local($TREE_WORK) = $tree_table;
    
    computeAttrsTable($dbh, $tree_table);
    activateTables($dbh, $ATTRS_WORK => $TAXON_TABLE{$tree_table}{attrs});
    
    my $a = 1;	# we can stop here when debugging
}


# computeCollectionCounts ( dbh )
# 
# For each taxon, compute the number of distinct collections in which it or
# any of its subtaxa appears.  This is currently unused, and perhaps should be
# eliminated.

sub computeCollectionCounts {

    my ($dbh) = @_;
    
    logMessage(2, "computing collection counts (h)");
    
    # First figure out how deep the table goes, and grab the bottom level.
    
    my ($TREE_WORK) = 'taxon_trees';
    my ($ATTRS_WORK) = 'taxon_attrs';
    my ($sql, $result);
    
    my ($max_depth) = $dbh->selectrow_array("SELECT max(depth) FROM $TREE_WORK");
    
    $result = $dbh->do("DROP TABLE IF EXISTS ROW$max_depth");
    
    $sql = "	CREATE TABLE ROW$max_depth (
			orig_no int unsigned,
			collection_no int unsigned,
			primary key (orig_no, collection_no)) Engine=MEMORY
		IGNORE SELECT m.orig_no, m.collection_no
		FROM $OCC_MATRIX as m JOIN $TREE_WORK as t using (orig_no)
		WHERE t.depth = $max_depth";
    
    $result = $dbh->do($sql);
    
    # Now iterate up from the bottom level to 1, computing each level by
    # copying from $OCC_MATRIX and then merging in the level below.  Once we
    # have merged the lower level, we can copy its counts over to $ATTRS_WORK
    # and then drop the table.
    
    for ( my $depth = $max_depth - 1; $depth > 0; $depth-- )
    {
	logMessage(2, "    computing tree level $depth...") if $depth % 10 == 0;
	
	my $child_depth = $depth + 1;
	
	# Grab the next level from $OCC_MATRIX
	
	$result = $dbh->do("DROP TABLE IF EXISTS ROW$depth");
	
	$sql = "CREATE TABLE ROW$depth (
			orig_no int unsigned,
			collection_no int unsigned,
			primary key (orig_no, collection_no)) Engine=MEMORY
		IGNORE SELECT m.orig_no, m.collection_no 
		FROM $OCC_MATRIX as m JOIN $TREE_WORK as t using (orig_no)
		WHERE t.depth = $depth";
	
	$result = $dbh->do($sql);
	
	$sql = "INSERT IGNORE INTO ROW$depth (orig_no, collection_no)
		SELECT t.parent_no, c.collection_no
		FROM ROW$child_depth as c JOIN $TREE_WORK as t using (orig_no)";
	
	$result = $dbh->do($sql);
	
	# Now copy the child-level collection counts into $ATTRS_WORK.
	
	$sql = "UPDATE $ATTRS_WORK as v JOIN
		(SELECT orig_no, count(*) as n_colls FROM ROW$child_depth
		 GROUP BY orig_no) as c using (orig_no)
		SET v.n_colls = c.n_colls";
	
	$result = $dbh->do($sql);
	
	# Then drop the table.
	
	$result = $dbh->do("DROP TABLE IF EXISTS ROW$child_depth");
    }
    
    # Then finish off with row 1

    $sql = "	UPDATE $ATTRS_WORK as v JOIN
		(SELECT orig_no, count(*) as n_colls FROM ROW1
		 GROUP BY orig_no) as c using (orig_no)
		SET v.n_colls = c.n_colls";
    
    $result = $dbh->do($sql);
    
    $result = $dbh->do("DROP TABLE IF EXISTS ROW1");
    
    logMessage(2, "    done");
}



# computeTaxonProminenceTables ( dbh )
# 
# Use the occurrence matrix, the collection matrix, and the taxon trees to
# compute summary tables for the prevalence and diversity of phyla and classes
# throughout space and time.

sub computeTaxonProminenceTables {

    my ($dbh) = @_;
    
    



}


our $RANK_MAP = 'rank_map';

sub createRankMap {

    my ($dbh, $force) = @_;
    
    my $result;

    # First make sure we have a clean table.
    
    if ( $force )
    {
	$dbh->do("DROP TABLE IF EXISTS $RANK_MAP");
    }
    
    $result = $dbh->do("
	CREATE TABLE IF NOT EXISTS $RANK_MAP (
		rank_no tinyint unsigned primary key,
		rank enum('','subspecies','species','subgenus','genus','subtribe','tribe','subfamily','family','superfamily','infraorder','suborder','order','superorder','infraclass','subclass','class','superclass','subphylum','phylum','superphylum','subkingdom','kingdom','superkingdom','unranked clade','informal'),
		key (rank)) Engine=MyISAM");
    
    # Then populate it if necessary.  Abort if there are any rows already in
    # the table.
    
    my ($count) = $dbh->selectrow_array("SELECT count(*) FROM $RANK_MAP");
    
    return if $count;
    
    logMessage(2, "    rebuilding rank map");
    
    my $sql = "INSERT INTO $RANK_MAP (rank_no, rank) VALUES
		(2, 'subspecies'), (3, 'species'), (4, 'subgenus'), (5, 'genus'),
		(6, 'subtribe'), (7, 'tribe'), (8, 'subfamily'), (9, 'family'),
		(10, 'superfamily'), (11, 'infraorder'), (12, 'suborder'),
		(13, 'order'), (14, 'superorder'), (15, 'infraclass'), (16, 'subclass'),
		(17, 'class'), (18, 'superclass'), (19, 'subphylum'), (20, 'phylum'),
		(21, 'superphylum'), (22, 'subkingdom'), (23, 'kingdom'),
		(25, 'unranked clade'), (26, 'informal')";
    
    $dbh->do($sql);
    
    my $a = 1;	# we can stop here when debugging
}


# activateNewTaxonomyTables ( dbh, new_tree_table, keep_temps )
# 
# In one atomic operation, move the new taxon tables to active status and swap
# out the old ones.  Those old ones are then deleted.
# 
# If keep_temps is true, then keep around the temporary tables that we created
# during the rebuild process (this might be done for debugging purposes).
# Otherwise, these are deleted.

sub activateNewTaxonomyTables {

    my ($dbh, $tree_table, $keep_temps) = @_;
    
    # Activate the new tables
    
    activateTables($dbh, $TREE_WORK => $tree_table,
		         $SEARCH_WORK => $TAXON_TABLE{$tree_table}{search},
			 $NAME_WORK => $TAXON_TABLE{$tree_table}{names},
			 $ATTRS_WORK => $TAXON_TABLE{$tree_table}{attrs},
			 $INTS_WORK => $TAXON_TABLE{$tree_table}{ints},
			 $LOWER_WORK => $TAXON_TABLE{$tree_table}{lower},
			 $COUNTS_WORK => $TAXON_TABLE{$tree_table}{counts});
    
    # Delete the auxiliary tables, unless we were told to keep them.
    
    my $result;
    
    unless ( $keep_temps )
    {
	logMessage(2, "removing temporary tables");
	
	$result = $dbh->do("DROP TABLE IF EXISTS $SPELLING_AUX");
	$result = $dbh->do("DROP TABLE IF EXISTS $TRAD_AUX");
	$result = $dbh->do("DROP TABLE IF EXISTS $CLASSIFY_AUX");
	$result = $dbh->do("DROP TABLE IF EXISTS $SYNONYM_AUX");
	$result = $dbh->do("DROP TABLE IF EXISTS $SPECIES_AUX");
	$result = $dbh->do("DROP TABLE IF EXISTS $INTS_AUX");
    }
    
    my $a = 1;		# we can stop here when debugging
}


# activateTreeTables ( dbh, new_tree_table )
# 
# In one atomic operation, move the new tree table to active status and swap
# out the old one.  This routine does not do anything with the secondary tables.

sub activateTreeTables {

    my ($dbh, $tree_table) = @_;
    
    my $result;
    
    logMessage(2, "activating new version of table '$tree_table' (g)");
    
    # Compute the backup names of all the tables to be activated
    
    my $tree_bak = "${tree_table}_bak";
    
    # Delete any backup tables that might still be around
    
    $result = $dbh->do("DROP TABLE IF EXISTS $tree_bak");
    
    # Create dummy versions of any of the tree table if it is missing
    # (otherwise the rename will fail; the dummy will be deleted below anyway,
    # after it is renamed to the backup name).
    
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $tree_table LIKE $TREE_WORK");
    
    # Now do the Atomic Table Swap (tm)
    
    $result = $dbh->do("RENAME TABLE
			    $tree_table to $tree_bak,
			    $TREE_WORK to $tree_table");
    
    # Then we can get rid of the backup table
    
    $result = $dbh->do("DROP TABLE $tree_bak");
    
    my $a = 1;		# we can stop here when debugging
}


# buildTaxaCacheTables ( dbh, tree_table )
# 
# Rebuild the tables 'taxa_tree_cache' and 'taxa_list_cache' using the
# specified tree table.  This allows the old pbdb code to function properly
# using the recomputed taxonomy tree.

sub buildTaxaCacheTables {

    my ($dbh, $tree_table) = @_;
    
    my ($auth_table) = $TAXON_TABLE{$tree_table}{authorities};
    
    my $result;
    
    # Create a new working table for taxa_tree_cache
    
    logMessage(2, "computing tree cache and list cache tables");
    
    logMessage(2, "    creating tree cache");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $TREE_CACHE_WORK");
    
    $result = $dbh->do("
	CREATE TABLE $TREE_CACHE_WORK
	       (taxon_no int unsigned primary key,
		lft int unsigned not null,
		rgt int unsigned not null,
		spelling_no int unsigned not null,
		synonym_no int unsigned not null,
		opinion_no int unsigned not null,
		max_interval_no int unsigned not null,
		min_interval_no int unsigned not null,
		mass float) ENGINE=MYISAM");
    
    # Populate it using the authorities table and taxon_trees table
    
    logMessage(2, "    populating tree cache");
    
    $result = $dbh->do("
	INSERT INTO $TREE_CACHE_WORK (taxon_no, lft, rgt, spelling_no, synonym_no, opinion_no)
	SELECT a.taxon_no, t.lft, t.rgt, t.spelling_no, t2.spelling_no, t.opinion_no
	FROM $auth_table as a JOIN $tree_table as t using (orig_no)
		JOIN $tree_table as t2 on t2.orig_no = t.synonym_no");
    
    # Add the necessar indices
    
    logMessage(2, "    indexing tree cache");
    
    $result = $dbh->do("ALTER TABLE $TREE_CACHE_WORK add index (lft)");
    $result = $dbh->do("ALTER TABLE $TREE_CACHE_WORK add index (rgt)");
    $result = $dbh->do("ALTER TABLE $TREE_CACHE_WORK add index (spelling_no)");
    $result = $dbh->do("ALTER TABLE $TREE_CACHE_WORK add index (synonym_no)");
    $result = $dbh->do("ALTER TABLE $TREE_CACHE_WORK add index (opinion_no)");
    
    # Create a new working table for taxa_list_cache
    
    logMessage(2, "    creating list cache");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $LIST_CACHE_WORK");
    
    $result = $dbh->do("
	CREATE TABLE $LIST_CACHE_WORK
	       (parent_no int unsigned not null,
		child_no int unsigned not null,
		PRIMARY KEY (child_no, parent_no)) ENGINE=MYISAM");
    
    # Populate it using the taxon_trees table, 
    
    logMessage(2, "    populating list cache");
    
    my ($max_depth) = $dbh->selectrow_array("SELECT max(depth) FROM $tree_table");
    
    foreach my $depth (reverse 2..$max_depth)
    {
	logMessage(2, "    computing tree level $depth...") if $depth % 10 == 0;
	
	$result = $dbh->do("
		INSERT IGNORE INTO $LIST_CACHE_WORK (parent_no, child_no)
		SELECT t.parent_no, l.child_no
		FROM $tree_table as t JOIN $LIST_CACHE_WORK as l on t.orig_no = l.parent_no
		WHERE t.depth = $depth");
	
	$result = $dbh->do("
		INSERT IGNORE INTO $LIST_CACHE_WORK (parent_no, child_no)
		SELECT t.parent_no, t.orig_no
		FROM $tree_table as t
		WHERE t.depth = $depth");
    }
    
    # Update it to show spelling_no values instead of the corresponding
    # orig_no values.
    
    logMessage(2, "    setting parent spelling_no values");
    
    $result = $dbh->do("
		UPDATE IGNORE $LIST_CACHE_WORK as l
			JOIN $tree_table as pt on pt.orig_no = l.parent_no
		SET l.parent_no = pt.spelling_no");
    
    logMessage(2, "    adding entries for child spellings");
    
    $result = $dbh->do("
		INSERT IGNORE INTO $LIST_CACHE_WORK (parent_no, child_no)
		SELECT l.parent_no, a.taxon_no
		FROM $auth_table as a JOIN $LIST_CACHE_WORK as l on l.child_no = a.orig_no");
    
    logMessage(2, "      $result new entries.");
    
    # Add the necessary indices
    
    logMessage(2, "    indexing list cache");
    
    $result = $dbh->do("ALTER TABLE $LIST_CACHE_WORK add index (parent_no)");
    
    # Now swap in the new tables.
    
    logMessage(2, "   activating tables '$CLASSIC_TREE_CACHE', '$CLASSIC_LIST_CACHE'");
    
    # Compute the backup names of all the tables to be activated
    
    my $tree_bak = "${CLASSIC_TREE_CACHE}_bak";
    my $list_bak = "${CLASSIC_LIST_CACHE}_bak";
    
    # Drop those tables if any are still around
    
    $result = $dbh->do("DROP TABLE IF EXISTS $tree_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $list_bak");
    
    # Recreate any of the existing tables that may not exist for some reason
    
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $CLASSIC_TREE_CACHE like $TREE_CACHE_WORK");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $CLASSIC_LIST_CACHE like $LIST_CACHE_WORK");
    
    # Now swap in the new tables
    
    $result = $dbh->do("RENAME TABLE
		$CLASSIC_TREE_CACHE to $tree_bak,
		$TREE_CACHE_WORK to $CLASSIC_TREE_CACHE,
		$CLASSIC_LIST_CACHE to $list_bak,
		$LIST_CACHE_WORK to $CLASSIC_LIST_CACHE");
    
    my $a = 1;	# We can stop here when debugging
}


# check ( dbh )
# 
# Check the integrity of $TREE_TABLE.  Return true if the table is okay.  If
# not, return false, write out some error messages, and leave one or more error messages
# in the variable @TREE_ERRORS.

sub check {

    my ($dbh, $tree_table, $options) = @_;
    
    my $result;
    my $options ||= {};
    
    my $auth_table = $TAXON_TABLE{$tree_table}{authorities};
    my $opinion_cache = $TAXON_TABLE{$tree_table}{opcache};
    
    # First, set the variables that control log output.
    
    #$MSG_TAG = 'Check'; $MSG_LEVEL = $options->{msg_level} || 1;
    
    # Then, clear the error list.
    
    @TREE_ERRORS = ();
    
    # Next, make sure that every orig_no value in authorities corresponds to a
    # taxon_no value.
    
    logMessage(2, "checking concept numbers");
    
    my ($bad_orig_auth) = $dbh->selectrow_array("
		SELECT count(distinct a.orig_no)
		FROM $auth_table a LEFT JOIN $auth_table a2
			ON a.orig_no = a2.taxon_no
		WHERE a2.taxon_no is null");
    
    if ( $bad_orig_auth > 0 )
    {
	push @TREE_ERRORS, "Found $bad_orig_auth bad orig_no value(s) in $auth_table";
	logMessage(1, "    found $bad_orig_auth bad orig_no value(s) in $auth_table");
	
	if ( $bad_orig_auth < $REPORT_THRESHOLD )
	{
	    my ($list) = $dbh->selectcol_arrayref("
		SELECT distinct a.orig_no
		FROM $auth_table a LEFT JOIN $auth_table a2
			ON a.orig_no = a2.taxon_no
		WHERE a2.taxon_no is null");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    # Also make sure that the orig_no values are in 1-1 correspondence between
    # $auth_table and $tree_table.
    
    my ($bad_orig) = $dbh->selectrow_array("
		SELECT count(t.orig_no)
		FROM $tree_table as t LEFT JOIN $auth_table as a USING (orig_no)
		WHERE a.orig_no is null");
    
    if ( $bad_orig > 0 )
    {
	push @TREE_ERRORS, "Found $bad_orig concept(s) in $tree_table that do not match $auth_table";
	logMessage(1, "    found $bad_orig concept(s) in $tree_table that do not match $auth_table");
	
	if ( $bad_orig < $REPORT_THRESHOLD )
	{
	    my ($list) = $dbh->selectcol_arrayref("
		SELECT t.orig_no
		FROM $tree_table as t LEFT JOIN $auth_table as a USING (orig_no)
		WHERE a.orig_no is null");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    my ($missing_orig) = $dbh->selectrow_array("
		SELECT count(distinct a.orig_no)
		FROM $auth_table as a LEFT JOIN $tree_table as t USING (orig_no)
		WHERE t.orig_no is null");
    
    if ( $missing_orig > 0 )
    {
	push @TREE_ERRORS, "Found $missing_orig concept(s) missing from $tree_table";
	logMessage(1, "    found $missing_orig concept(s) missing from $tree_table");
	
	if ( $missing_orig < $REPORT_THRESHOLD )
	{
	    my ($list) = $dbh->selectcol_arrayref("
		SELECT distinct a.orig_no
		FROM $auth_table as a LEFT JOIN $tree_table as t USING (orig_no)
		WHERE t.orig_no is null");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    # Make sure every concept is distinct
    
    my ($concept_chain) = $dbh->selectrow_array("
		SELECT count(a.orig_no)
		FROM $auth_table as a JOIN $auth_table a2
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
		FROM $auth_table as a JOIN $auth_table a2
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
		FROM $tree_table as t LEFT JOIN $auth_table as a ON a.taxon_no = t.spelling_no
		WHERE a.orig_no != t.orig_no or a.orig_no is null");
    
    if ( $bad_spelling > 0 )
    {
	push @TREE_ERRORS, "Found $bad_spelling entries with bad spelling numbers";
	logMessage(1, "    found $bad_spelling entries with bad spelling numbers");
	
	if ( $bad_spelling < $REPORT_THRESHOLD )
	{
	    my ($list) = $dbh->selectcol_arrayref("
		SELECT t.orig_no
		FROM $tree_table as t JOIN $auth_table as a ON a.taxon_no = t.spelling_no
		WHERE a.orig_no != t.orig_no or a.orig_no is null");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    # Make sure that every synonym_no matches an orig_no.
    
    logMessage(2, "checking synonyms");
    
    my ($bad_synonym) = $dbh->selectrow_array("
		SELECT count(t.orig_no)
		FROM $tree_table as t LEFT JOIN $tree_table as t2
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
		FROM $tree_table as t LEFT JOIN $tree_table as t2
			ON t.synonym_no = t2.orig_no
		WHERE t2.orig_no is null");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    # Make sure that every parent_no matches an orig_no.
    
    logMessage(2, "checking parents");
    
    my ($bad_parent) = $dbh->selectrow_array("
		SELECT count(t.orig_no)
		FROM $tree_table as t LEFT JOIN $tree_table as t2
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
		FROM $tree_table as t LEFT JOIN $tree_table as t2
			ON t.parent_no = t2.orig_no
		WHERE t2.orig_no is null");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    # check that only senior synonyms are classified by 'belongs to' opinions
    
    my ($bad_class) = $dbh->selectrow_array("
		SELECT count(t.orig_no)
		FROM $tree_table as t JOIN $opinion_cache o USING (opinion_no)
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
		FROM $tree_table as t JOIN $opinion_cache o USING (opinion_no)
		WHERE (o.status = 'belongs to' and t.synonym_no != t.orig_no)
		   or (o.status != 'belongs to' and t.synonym_no = t.orig_no)");
	    
	    logMessage(1, "        " . join(', ', @$list));
	}
    }
    
    # Check the integrity of the tree sequence.
    
    logMessage(2, "checking tree sequence");
    
    my ($bad_seq) = $dbh->selectrow_array("
		SELECT count(t.orig_no)
		FROM $tree_table as t join $tree_table as p on p.orig_no = t.parent_no
		WHERE (t.lft < p.lft or t.lft > p.rgt)");
    
    if ( $bad_seq > 0 )
    {
	push @TREE_ERRORS, "Found $bad_seq entries out of sequence";
	logMessage(1, "    found $bad_seq entries out of sequence");

	if ( $bad_synonym < $REPORT_THRESHOLD )
	{
	    my ($list) = $dbh->selectcol_arrayref("
		SELECT t.orig_no
		FROM $tree_table as t join $tree_table as p on p.orig_no = t.parent_no
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



# ensureOrig ( dbh )
# 
# Unless the authorities table has an 'orig_no' field, create one.

sub ensureOrig {
    
    my ($dbh) = @_;
    
    # Check the table definition, and return if it already has 'orig_no'.
    
    my ($table_name, $table_definition) = $dbh->selectrow_array("SHOW CREATE TABLE authorities"); 
    
    return if $table_definition =~ /`orig_no` int/;
    
    print STDERR "Creating 'orig_no' field...\n";
    
    # Create the 'orig_no' field.
    
    $dbh->do("ALTER TABLE authorities
	      ADD COLUMN orig_no INT UNSIGNED NOT NULL AFTER taxon_no");
    
    return;
}


# populateOrig ( dbh )
# 
# If there are any entries where 'orig_no' is not set, fill them in.

sub populateOrig {

    my ($dbh) = @_;
    
    # First make sure that we have a field to populate.
    
    ensureOrig($dbh);
    
    # Start by zeroing any orig_no entries that no longer correspond to
    # taxon_no values.
    
    $dbh->do("
	UPDATE authorities as a LEFT JOIN authorities as a2 on a2.taxon_no = a.orig_no
	SET a.orig_no = 0 WHERE a2.taxon_no is null");
    
    # Then check to see if we have any unset orig_no entries, and return if we
    # do not.
    
    my ($count) = $dbh->selectrow_array("
	SELECT count(*) from authorities
	WHERE orig_no = 0");
    
    return unless $count > 0;
    
    # Populate all unset orig_no entries.  This algorithm is taken from
    # TaxonInfo::getOriginalCombination() in the old code.
    
    print STDERR "Populating 'orig_no' field...\n";
    
    $count = $dbh->do("
	UPDATE authorities as a JOIN opinions as o on a.taxon_no = o.child_spelling_no
	SET a.orig_no = o.child_no WHERE a.orig_no = 0");
    
    print STDERR "   child_spelling_no: $count\n";
    
    $count = $dbh->do("
	UPDATE authorities as a JOIN opinions as o on a.taxon_no = o.child_no
	SET a.orig_no = o.child_no WHERE a.orig_no = 0");
    
    print STDERR "   child_no: $count\n";
    
    $count = $dbh->do("
	UPDATE authorities as a JOIN opinions as o on a.taxon_no = o.parent_spelling_no
	SET a.orig_no = o.parent_no WHERE a.orig_no = 0");
        
    print STDERR "   parent_spelling_no: $count\n";
    
    $count = $dbh->do("
	UPDATE authorities as a JOIN opinions as o on a.taxon_no = o.parent_no
	SET a.orig_no = o.parent_no WHERE a.orig_no = 0");
    
    print STDERR "   parent_no: $count\n";
    
    $count = $dbh->do("
	UPDATE authorities as a
	SET a.orig_no = a.taxon_no WHERE a.orig_no = 0");
    
    print STDERR "   self: $count\n";
    
    # Index the field, unless there is already an index.
    
    my ($table_name, $table_definition) = $dbh->selectrow_array("SHOW CREATE TABLE authorities"); 
    
    return if $table_definition =~ /KEY `orig_no`/;
    
    $dbh->do("ALTER TABLE authorities
              ADD KEY (orig_no)");
    
    print STDERR "  done.\n";
}



# UTILITY ROUTINES

# The following routines are intended only for debugging.

sub clearSpelling {

    my ($dbh) = @_;
    
    $dbh->do("UPDATE $TREE_WORK SET spelling_no = 0");
}


sub clearSynonymy {
    
    my ($dbh) = @_;
    
    $dbh->do("UPDATE $TREE_WORK SET synonym_no = 0");
}


sub clearHierarchy {
    
    my ($dbh) = @_;
    
    $dbh->do("UPDATE $TREE_WORK SET opinion_no = 0, parent_no = 0");
}


sub clearTreeSequence {

    my ($dbh) = @_;
    
    $dbh->do("UPDATE $TREE_WORK SET lft = null, rgt = null, depth = null");
}


1;
