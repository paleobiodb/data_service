# 
# The Paleobiology Database
# 
#   TaxonTrees.pm
# 

=head1 NAME

TaxonTrees

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

package TaxonTrees;

# Modules needed

use Constants qw($FINE_BIN_SIZE $COARSE_BIN_SIZE);
use Carp qw(carp croak);
use Try::Tiny;

use strict;


# Controlling variables for debug messages

our $MSG_TAG = 'unknown';
our $MSG_LEVEL = 1;

# Main table names - if new entries are ever added to @TREE_TABLE_LIST,
# corresponding entries should be added to the hashes immediately below.  Each
# new tree table should have its own distinct name table, attrs table, etc.
# They could either point to the same authorities, opinions, etc. tables or to
# different ones depending upon the purpose for adding them to the system.

our (@TREE_TABLE_LIST) = ("taxon_trees");

our (%NAME_TABLE) = ("taxon_trees" => "taxon_names");
our (%ATTRS_TABLE) = ("taxon_trees" => "taxon_attrs");
our (%SEARCH_TABLE) = ("taxon_trees" => "taxon_search");
our (%INTS_TABLE) = ("taxon_trees" => "taxon_ints");

our (%AUTH_TABLE) = ("taxon_trees" => "authorities");
our (%OPINION_TABLE) = ("taxon_trees" => "opinions");
our (%OPINION_CACHE) = ("taxon_trees" => "order_opinions");
our (%REFS_TABLE) = ("taxon_trees" => "refs");

our $COLL_MATRIX = "coll_matrix";
our $COLL_INTS = "coll_ints";
our $COLL_BINS = "coll_bins";
our $COLL_CLUST = "clusters";
our $OCC_MATRIX = "occ_matrix";
our $TAXON_SUMMARY = "taxon_summary";
our $REF_SUMMARY = "ref_summary";
our $INTERVAL_MAP = "interval_map";
our $CONTAINER_MAP = "interval_container_map";

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
our $INTS_WORK = "intn";
our $OPINION_WORK = "opn";
our $COLL_MATRIX_WORK = "cmn";
our $COLL_INTS_WORK = "cin";
our $COLL_BINS_WORK = "cbn";
our $COLL_CLUST_WORK = "kmcn";
our $OCC_MATRIX_WORK = "omn";
our $TAXON_SUMMARY_WORK = "tsn";
our $REF_SUMMARY_WORK = "rsn";
our $INTERVAL_MAP_WORK = "imn";
our $CONTAINER_MAP_WORK = "icn";

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
our $COLL_AUX = "coll_aux";
our $CLUST_AUX = "clust_aux";

# Additional tables

our $QUEUE_TABLE = "taxon_queue";
our $COUNTRY_MAP = "country_map";

# Other variables and constants

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
		buildTables($dbh, $table, $options);
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


# buildTables ( dbh, tree_table, options, msg_level )
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

sub buildTables {

    my ($dbh, $tree_table, $options, $steps) = @_;
    
    $options ||= {};
    my $step_control;
    
    # First, set the variables that control log output and also determine
    # which tables will be computed.
    
    $MSG_TAG = 'Rebuild';
    
    my @steps = split(//, $steps || 'Aabcdefghi');
    $step_control->{$_} = 1 foreach @steps;
    
    $TREE_WORK = 'taxon_trees' unless $step_control->{a};
    
    # Now create the necessary tables, including generating the opinion cache
    # from the opinion table.
    
    buildOpinionCache($dbh, $tree_table) if $step_control->{A};
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
    
    computeIntermediates($dbh, $tree_table) if $step_control->{f};
    
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
    my $search_table = $SEARCH_TABLE{$tree_table};
    
    my %update_concepts;	# list of concepts to be updated
    
    # First, set the variables that control log output.
    
    $MSG_TAG = 'Update'; $MSG_LEVEL = $options->{msg_level} || 1;
    
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
    
    my $OPINION_CACHE = $OPINION_CACHE{$tree_table};
    
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
    $result = $dbh->do("ALTER TABLE $OPINION_WORK ADD KEY (parent_no)");
    
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
    
    $result = $dbh->do("LOCK TABLE $OPINION_TABLE{$tree_table} as o read,
				   $REFS_TABLE{$tree_table} as r read,
				   $AUTH_TABLE{$tree_table} as a1 read,
				   $AUTH_TABLE{$tree_table} as a2 read,
				   $OPINION_CACHE{$tree_table} write");
    
    $result = $dbh->do("DELETE FROM $OPINION_CACHE{$tree_table} WHERE opinion_no in ($opfilter)");
    
    populateOpinionCache($dbh, $OPINION_CACHE{$tree_table}, $tree_table, $opfilter);
    
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
			o.status, o.spelling_reason, null
		FROM $OPINION_TABLE{$tree_table} as o
			LEFT JOIN $REFS_TABLE{$tree_table} as r using (reference_no)
			JOIN $AUTH_TABLE{$tree_table} as a1 on a1.taxon_no = o.child_spelling_no
			LEFT JOIN $AUTH_TABLE{$tree_table} as a2 on a2.taxon_no = o.parent_spelling_no
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
		FROM $OPINION_TABLE{$tree_table} WHERE opinion_no in $opfilter");
    
    $new_op_data->execute();
    
    while ( my ($child_orig, $parent_orig) = $new_op_data->fetchrow_array() )
    {
	$update_concepts{$child_orig} = 1 if $child_orig > 0;
	$update_concepts{$parent_orig} = 1 if $parent_orig > 0;
    }
    
    # Now do the same with the corresponding old opinion records.
    
    my $old_op_data = $dbh->prepare("
		SELECT orig_no, parent_no
		FROM $OPINION_CACHE{$tree_table} WHERE opinion_no in $opfilter");
    
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
    
    my $auth_table = $AUTH_TABLE{$tree_table};
    my $opinion_cache = $OPINION_CACHE{$tree_table};
    my $opinion_table = $OPINION_TABLE{$tree_table};
    
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
    
    my $auth_table = $AUTH_TABLE{$tree_table};
    
    # First create $TREE_WORK, which will hold one row for every concept that
    # is being updated.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $TREE_WORK");
    $result = $dbh->do("CREATE TABLE $TREE_WORK 
			       (orig_no int unsigned not null,
				name varchar(80) not null,
				rank tinyint not null,
				imp boolean not null,
				spelling_no int unsigned not null,
				trad_no int unsigned not null,
				valid_no int unsigned not null,
				synonym_no int unsigned not null,
				parent_no int unsigned not null,
				opinion_no int unsigned not null,
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
    
    my $opinion_cache = $OPINION_CACHE{$tree_table};
    my $auth_table = $AUTH_TABLE{$tree_table};
    
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
    $result = $dbh->do("UPDATE $TREE_WORK t JOIN $TRAD_AUX s USING (orig_no)
			SET t.trad_no = s.spelling_no");
    
    logMessage(2, "    indexing trad_no");
    
    $result = $dbh->do("ALTER TABLE $TREE_WORK ADD INDEX (trad_no)");
    
    # We then copy the selected name and rank into $TREE_TABLE.  We use the
    # traditional rank instead of the currently accepted one, because 'unranked clade'
    # isn't really very useful.
    
    logMessage(2, "    setting name and rank");
    
    $result = $dbh->do("UPDATE $TREE_WORK as t JOIN $auth_table as a on taxon_no = trad_no
			SET t.name = a.taxon_name, t.rank = a.taxon_rank");
    
    # Then we can compute the name table, which records the best opinion
    # and spelling reason for each taxonomic name.
    
    logMessage(2, "    computing taxonomic name table");
    
    # First put in all of the names we've selected above.
    
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
    
    my $opinion_cache = $OPINION_CACHE{$tree_table};
    
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

    my $OPINION_CACHE = $OPINION_CACHE{$tree_table};
    
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
    # fill in synonym_no and valid_no in $TREE_WORK.
    
    logMessage(2, "    setting synonym_no and valid_no");
    
    $result = $dbh->do("UPDATE $TREE_WORK as t LEFT JOIN $CLASSIFY_AUX as b using (orig_no)
			SET t.synonym_no = if(status <> 'belongs to' and b.parent_no > 0,
					      b.parent_no, orig_no),
			    t.valid_no = if(status in ('subjective synonym of',
						       'objective synonym of',
						       'replaced by') and b.parent_no > 0,
					    b.parent_no, orig_no)");
    
    # Now that we have set synonym_no and valid_no, we can efficiently index them.
    
    logMessage(2, "    indexing synonym_no and valid_no");
    
    $result = $dbh->do("ALTER TABLE $TREE_WORK add index (synonym_no)");
    $result = $dbh->do("ALTER TABLE $TREE_WORK add index (valid_no)");
    
    return;
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
    
    my $OPINION_CACHE = $OPINION_CACHE{$tree_table};
    
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
    
    $count = 0;
    
    do
    {
	$result = $dbh->do("
		UPDATE $TREE_WORK t1 JOIN $TREE_WORK t2
		    on t1.valid_no = t2.orig_no and t1.valid_no != t2.valid_no
		SET t1.valid_no = t2.valid_no");
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
# Fill in the opinion_no and parent_no fields of $TREE_WORK.  This determines
# the classification of each taxonomic concept represented in $TREE_WORK, and
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
# computeSynonymy().  Only then can we set the parent_no field of $TREE_WORK.
# Finally, we set opinion_no for each row of $TREE_WORK, based on the modified
# set of classification opinions.

sub computeHierarchy {
    
    my ($dbh, $tree_table) = @_;
    
    my ($result, $filter, @check_taxa, %taxa_moved);
    
    logMessage(2, "computing hierarchy relation (d)");
    
    my $OPINION_CACHE = $OPINION_CACHE{$tree_table};
    
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
    
    # First, we add all immediately junior synonyms, but only subjective and
    # objective synonyms and replaced taxa.  We leave out nomina dubia, nomina
    # vana, nomina nuda, nomina oblita, and invalid subgroups, because an
    # opinion on any of those shouldn't affect the senior taxon.  The last
    # clause excludes chained junior synonyms.
    
    $result = $dbh->do("INSERT IGNORE INTO $SYNONYM_AUX
			SELECT t.orig_no, t.synonym_no
			FROM $TREE_WORK t JOIN $CLASSIFY_AUX c USING (orig_no)
			WHERE c.status in ('subjective synonym of', 'objective synonym of',
						'replaced by')
				and t.synonym_no = c.parent_no");
    
    # Next, we add entries for all of the senior synonyms, because of course
    # their own opinions are considered as well.
    
    $result = $dbh->do("INSERT IGNORE INTO $SYNONYM_AUX
    			SELECT DISTINCT senior_no, senior_no
			FROM $SYNONYM_AUX");
    
    # Next, we delete the classification opinion for each taxon in
    # $SYNONYM_AUX.
    
    $result = $dbh->do("DELETE QUICK FROM $CLASSIFY_AUX
			USING $CLASSIFY_AUX JOIN $SYNONYM_AUX
				ON $CLASSIFY_AUX.orig_no = $SYNONYM_AUX.senior_no");
    
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
    
    # Now we can set and then index opinion_no.
    
    logMessage(2, "    setting opinion_no");
    
    $result = $dbh->do("UPDATE $TREE_WORK t JOIN $CLASSIFY_AUX c USING (orig_no)
			SET t.opinion_no = c.opinion_no");
    
    logMessage(2, "    indexing opinion_no");
    
    $result = $dbh->do("ALTER TABLE $TREE_WORK add index (opinion_no)");
    
    # Now we can set parent_no.  All concepts in a synonym group will share
    # the same parent, so we need to join a second copy of $TREE_WORK to look
    # up the senior synonym number.  In other words, the parent_no value for
    # any taxon will point to the parent of its most senior synonym.
    
    logMessage(2, "    setting parent_no");
    
    $result = $dbh->do("
		UPDATE $TREE_WORK t JOIN $TREE_WORK t2 ON t2.orig_no = t.synonym_no
		    JOIN $OPINION_CACHE o ON o.opinion_no = t2.opinion_no
		SET t.parent_no = o.parent_no");
    
    # Once we have set parent_no for all concepts, we can efficiently index it.
    
    logMessage(2, "    indexing parent_no");
    
    $result = $dbh->do("ALTER TABLE $TREE_WORK add index (parent_no)");
    
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
		    JOIN $OPINION_CACHE{$tree_table} o ON o.opinion_no = t2.opinion_no
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
    # must match the genus under which they are immediately classified, or its
    # senior synonym if the immediate genus is a junior synonym.
    
    $SQL_STRING = "
		INSERT INTO $ADJUST_AUX (orig_no, new_name)
		SELECT t.orig_no,
		       concat(t1.name, ' (',
		              trim(trailing ')' from substring_index(t.name,'(',-1)), ')')
		FROM $TREE_WORK as t
			JOIN $TREE_WORK as p1 on p1.orig_no = t.parent_no
			JOIN $TREE_WORK as t1 on t1.orig_no = p1.synonym_no 
				and p1.rank = 5
		WHERE t.rank = 4";
    
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
		WHERE t.rank in (2, 3) and
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
    my $count = 0;
    
    logMessage(2, "traversing and marking taxon trees (e)");
    
    logMessage(2, "    downloading hierarchy relation");
    
    # First we fetch the entire hierarchy relation.  We need to do this so
    # that we can do the sequence computation in Perl (because SQL is just not
    # a powerful enough language to do this efficiently).
    
    my $fetch_hierarchy = $dbh->prepare("SELECT orig_no, rank, synonym_no, parent_no
					   FROM $TREE_WORK");
    
    $fetch_hierarchy->execute();
    
    while ( my ($child_no, $taxon_rank, $synonym_no, $parent_no) = $fetch_hierarchy->fetchrow_array() )
    {
	my $immediate_parent = $child_no != $synonym_no ? $synonym_no : $parent_no;
	$nodes->{$child_no} = { parent_no => $immediate_parent, rank => $taxon_rank };
	$count++;
    }
    
    foreach my $child_no (keys %$nodes)
    {
	my $parent_no = $nodes->{$child_no}{parent_no};
	push @{$nodes->{$parent_no}{children}}, $child_no if $parent_no > 0;
    }
    
    # Now we create the "main" tree, starting with taxon 1 'Eukaryota' at the
    # top of the tree with sequence=1 and depth=1.  The variable $next gets
    # the sequence number with which we should start the next tree.
    
    logMessage(2, "    sequencing tree rooted at 'Eukaryota'");
    
    my $seq = assignSequence($nodes, 1, 1, 1);
    
    # Next, we go through all of the other taxa.  When we find a taxon with no
    # parent that we haven't visited yet, we create a new tree with it as the
    # root.  This takes care of all the taxa for which their relationship to
    # the main tree is not known.
    
    logMessage(2, "    sequencing all other taxon trees");
    
 taxon:
    foreach my $orig_no (keys %$nodes)
    {
	next if $nodes->{$orig_no}{parent_no} > 0;	# skip any taxa that aren't roots
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
	# insert each one (and all its descendants) into the tree.
	
	if ( exists $node->{children} )
	{
	    foreach my $child_no ( @{$node->{children}} )
	    {
		$seq = assignSequence($nodes, $child_no, $seq + 1, $depth + 1);
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
    
    my $auth_table = $AUTH_TABLE{$tree_table};
    my $search_table = $SEARCH_TABLE{$tree_table};
    my $name_table = $NAME_TABLE{$tree_table};
    
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


# computeIntermediates ( dbh )
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

sub computeIntermediates {
    
    my ($dbh, $tree_table) = @_;
    
    my ($result, $sql);

    logMessage(2, "computing intermediate classification (f)");
    
    my $auth_table = $AUTH_TABLE{$tree_table};
    my $opinion_cache = $OPINION_CACHE{$tree_table};
    
    $result = $dbh->do("DROP TABLE IF EXISTS $INTS_WORK");
    $result = $dbh->do("CREATE TABLE $INTS_WORK
			       (ints_no int unsigned,
				kingdom_no int unsigned,
				kingdom varchar(80),
				phylum_no int unsigned,
				phylum varchar(80),
				class_no int unsigned,
				class varchar(80),
				order_no int unsigned,
				`order` varchar(80),
				family_no int unsigned,
				family varchar(80),
				primary key (ints_no)) ENGINE=MYISAM");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $INTS_AUX");
    $result = $dbh->do("CREATE TABLE $INTS_AUX
			       (orig_no int unsigned,
				parent_no int unsigned,
				depth int unsigned,
				taxon_name varchar(80),
				current_rank enum('','subspecies','species','subgenus','genus','subtribe','tribe','subfamily','family','superfamily','infraorder','suborder','order','superorder','infraclass','subclass','class','superclass','subphylum','phylum','superphylum','subkingdom','kingdom','superkingdom','unranked clade','informal'),
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
    
    # We first compute an auxiliary table to help in the computation.  We
    # insert a row for each taxonomic concept above genus level, listing the
    # current name and rank, as well as tree depth and parent link.
    
    $SQL_STRING = "
		INSERT INTO $INTS_AUX (orig_no, parent_no, depth, taxon_name, current_rank)
		SELECT a.orig_no, t2.synonym_no, t.depth, a.taxon_name, a.taxon_rank
		FROM $auth_table as a JOIN $TREE_WORK as t on a.taxon_no = t.spelling_no
			LEFT JOIN $TREE_WORK as t2 on t2.orig_no = t.parent_no
		WHERE a.taxon_rank > 5 and t.orig_no = t.synonym_no";
    
    $result = $dbh->do($SQL_STRING);
    
    # Then, for each concept, we fill in a count of opinions which in the past
    # assigned the rank of 'phylum', 'class' or 'order' to the concept,
    # vs. those which didn't.  We add an ugly hack to count 'Chordata' as a
    # phylum, even though it is (at the time this comment was written) not
    # recorded as such in this database.
    
    logMessage(2, "    counting phyla");
    
    $SQL_STRING = "
		UPDATE $INTS_AUX as k JOIN
		       (SELECT a.orig_no, count(*) as c, min(if(a.pubyr<>'', a.pubyr, 9999)) as pubyr
			FROM $auth_table as a JOIN $opinion_cache as o on a.taxon_no = o.child_spelling_no
			WHERE taxon_rank = 'phylum' or taxon_name = 'Chordata'
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
    
    # Now we can fill in the intermediate classification table itself.  We
    # start by inserting entries for the top (root) of each tree.
    
    logMessage(2, "    setting top level entries");
    
    $SQL_STRING = "
		INSERT INTO $INTS_WORK (ints_no, kingdom_no, phylum_no, class_no, order_no, family_no)
		SELECT orig_no, if(current_rank in ('kingdom', 'subkingdom'), orig_no, null),
			if(current_rank = 'phylum' or was_phylum > 0, orig_no, null),
			if(current_rank = 'class' or was_class > 0, orig_no, null),
			if(current_rank = 'order' or was_order > 0, orig_no, null),
			if(current_rank = 'family' or taxon_name like '%idae', orig_no, null)
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
		INSERT INTO $INTS_WORK (ints_no, kingdom_no, phylum_no, class_no, order_no, family_no)
		SELECT k.orig_no,
			if(k.current_rank = 'kingdom' or (k.current_rank = 'subkingdom' and ifnull(p.kingdom_no, 1) = 1), k.orig_no, p.kingdom_no) as nk,
			if(k.current_rank in ('phylum','superphylum') or (k.current_rank = 'subphylum' and p.phylum_no is null) or k.was_phylum - k.not_phylum > ifnull(xp.was_phylum - xp.not_phylum, 0), k.orig_no, p.phylum_no) as np,
			if(k.current_rank = 'class' or k.was_class - k.not_class > ifnull(xc.was_class - xc.not_class, 0), k.orig_no, p.class_no) as nc,
			if(k.current_rank = 'order' or k.was_order - k.not_order > ifnull(xo.was_order - xo.not_order, 0), k.orig_no, p.order_no) as no,
			if(k.current_rank = 'family' or k.taxon_name like '%idae', k.orig_no, p.family_no) as nf
		FROM $INTS_AUX as k JOIN $INTS_WORK as p on p.ints_no = k.parent_no
			LEFT JOIN $INTS_AUX as xp on xp.orig_no = p.phylum_no
			LEFT JOIN $INTS_AUX as xc on xc.orig_no = p.class_no
			LEFT JOIN $INTS_AUX as xo on xo.orig_no = p.order_no
		WHERE k.depth = $depth";
	
	$dbh->do($SQL_STRING);
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
    
    # Finally, link this table up to the main table.  We start by setting
    # ic_no = orig_no for each row in $TREE_WORK that corresponds to a row in
    # $INTS_WORK.
    
    logMessage(2, "    linking to tree table...");
    
    $SQL_STRING = "UPDATE $TREE_WORK as t join $INTS_WORK as k on t.orig_no = k.ints_no
		   SET t.ints_no = t.orig_no";
    
    $result = $dbh->do($SQL_STRING);
    
    # Then we go through $TREE_WORK in tree sequence order.  For any row that
    # has ic_no = 0 we set it to the most recently encountered non-zero value.
    # Thus, the row for each genus, species, etc. points to the row for the
    # most specific containing taxon in $INTS_WORK.
    
    $SQL_STRING = "UPDATE $TREE_WORK
		   SET ints_no = if(ints_no > 0, \@a := ints_no, \@a)
		   ORDER by lft";
    
    $result = $dbh->do($SQL_STRING);
    
    my $a = 1;
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
    
    my ($result);
    
    my $auth_table = $AUTH_TABLE{$tree_table};
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SEARCH_WORK");
    $result = $dbh->do("CREATE TABLE $SEARCH_WORK
			       (genus varchar(80) not null,
				taxon_name varchar(80) not null,
				taxon_rank enum('','subspecies','species','subgenus','genus','subtribe','tribe','subfamily','family','superfamily','infraorder','suborder','order','superorder','infraclass','subclass','class','superclass','subphylum','phylum','superphylum','subkingdom','kingdom','superkingdom','unranked clade','informal'),
				match_no int unsigned not null,
				result_no int unsigned not null,
				is_exact boolean not null,
				KEY (taxon_name, genus),
				UNIQUE KEY (match_no, genus)) ENGINE=MYISAM");
    
    # We start by copying all higher taxa into the search table.  That's the
    # easy part.
    
    logMessage(2, "    adding higher taxa");
    
    $result = $dbh->do("
		INSERT INTO $SEARCH_WORK (genus, taxon_name, taxon_rank, match_no, result_no, is_exact)
		SELECT null, taxon_name, taxon_rank, taxon_no, synonym_no, 1
		FROM $auth_table as a join $TREE_WORK as t using (orig_no)
		WHERE taxon_rank not in ('subgenus', 'species', 'subspecies')");
    
    # The subgenera are a bit trickier, because most (but not all) are named
    # as "Genus (Subgenus)".  The following expression will extract the
    # subgenus name from this pattern, and will also properly treat the
    # (incorrect) names that lack a subgenus component by simply returning the
    # whole name.
    
    logMessage(2, "    adding subgenera");
    
    $result = $dbh->do("
		INSERT INTO $SEARCH_WORK (genus, taxon_name, taxon_rank, match_no, result_no, is_exact)
		SELECT substring_index(taxon_name, ' ', 1),
			trim(trailing ')' from substring_index(taxon_name,'(',-1)),
		        taxon_rank, taxon_no, synonym_no, 1
		FROM $auth_table as a join $TREE_WORK as t using (orig_no)
		WHERE taxon_rank = 'subgenus'");
    
    # For species and sub-species, we split off the first component of each
    # name as the genus name and add the resulting entries to the table.  Note
    # that some species names also have a subgenus component which has to be
    # split out as well.
    
    logMessage(2, "    adding species by name");
    
    # Species which don't have a subgenus
    
    $result = $dbh->do("
		INSERT IGNORE INTO $SEARCH_WORK (genus, taxon_name, taxon_rank, match_no, result_no, is_exact)
		SELECT substring_index(taxon_name, ' ', 1),
			trim(substring(taxon_name, locate(' ', taxon_name)+1)),
			taxon_rank, taxon_no, orig_no, 1
		FROM $auth_table WHERE taxon_rank in ('species', 'subspecies')
			and taxon_name not like '%(%'");
    
    # Species which do have a subgenus
    
    $result = $dbh->do("
		INSERT IGNORE INTO $SEARCH_WORK (genus, taxon_name, taxon_rank, match_no, result_no, is_exact)
		SELECT substring_index(taxon_name, ' ', 1),
			trim(substring(taxon_name, locate(') ', taxon_name)+2)),
			taxon_rank, taxon_no, orig_no, 1
		FROM $auth_table WHERE taxon_rank in ('species', 'subspecies')
			and taxon_name like '%(%'");
    
    # And again with the subgenus name treated as if it was a genus
    
    $result = $dbh->do("
		INSERT IGNORE INTO $SEARCH_WORK (genus, taxon_name, taxon_rank, match_no, result_no, is_exact)
		SELECT substring_index(substring_index(taxon_name, '(', -1), ')', 1),
			trim(substring(taxon_name, locate(') ', taxon_name)+2)),
			taxon_rank, taxon_no, orig_no, 1
		FROM $auth_table WHERE taxon_rank in ('species', 'subspecies')
			and taxon_name like '%(%'");
    
    # Now comes the really tricky part.  For the purposes of "loose matching"
    # we also want to list each species under any genera and subgenera
    # synonymous with the actual genus and/or subgenus.  Note that the genus
    # under which a species is placed in the hierarchy may not be in accord
    # with its listed name!
    # 
    # In order to do this efficiently, we first need to create an auxiliary
    # table associating each species and subspecies with a genus/subgenus.
    
    logMessage(2, "    adding species by hierarchy");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SPECIES_AUX");
    $result = $dbh->do("CREATE TABLE $SPECIES_AUX
			       (genus varchar(80) not null,
				taxon_name varchar(80) not null,
				match_no int unsigned not null,
				result_no int unsigned not null,
				unique key (taxon_name, genus)) ENGINE=MYISAM");
    
    # Now for each species and subspecies, create an entry corresponding to each
    # genus that is synonymous to its current genus.  The result_no will be the
    # current senior synonym of the species/subspecies.
    
    $SQL_STRING = "
		INSERT IGNORE INTO $SPECIES_AUX (match_no, result_no, genus, taxon_name)
		SELECT a.taxon_no, t.synonym_no, ifnull(p1.taxon_name, ifnull(p2.taxon_name, p3.taxon_name)),
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
		INSERT IGNORE INTO $SPECIES_AUX (match_no, result_no, genus, taxon_name)
		SELECT a.taxon_no, t.synonym_no,
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
		INSERT IGNORE INTO $SEARCH_WORK (genus, taxon_name, taxon_rank, match_no, result_no, is_exact)
		SELECT sx.genus, sx.taxon_name, a.taxon_rank, sx.match_no, sx.result_no, 0
		FROM $SPECIES_AUX as sx JOIN $auth_table as a on a.taxon_no = sx.match_no";
    
    $result = $dbh->do($SQL_STRING);
    
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
    
    my $auth_table = $AUTH_TABLE{$tree_table};
    
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
    
    my $auth_table = $AUTH_TABLE{$tree_table};
    my $opinion_cache = $OPINION_CACHE{$tree_table};
    
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
				first_early_int_seq int unsigned,
				first_late_int_seq int unsigned,
				last_early_int_seq int unsigned,
				last_late_int_seq int unsigned,
				not_trace boolean,
				PRIMARY KEY (orig_no)) ENGINE=MYISAM");
    
    # Prime the table with the values actually stored in the authorities
    # table, ecotaph table and first appearance tables.
    
    $sql = "    INSERT IGNORE INTO $ATTRS_WORK 
			(orig_no, is_valid, is_senior, is_extant, extant_children, distinct_children, 
			 extant_size, taxon_size, n_occs, n_colls, min_body_mass, max_body_mass, 
			 first_early_int_seq, first_late_int_seq, last_early_int_seq, last_late_int_seq,
			 not_trace)
		SELECT a.orig_no,
			t.valid_no = t.synonym_no as is_valid,
			(t.orig_no = t.synonym_no) or (t.orig_no = t.valid_no) as is_senior,
			case coalesce(a.extant) 
				when 'yes' then 1 when 'no' then 0 else null end as is_extant,
			0, 0, 0, 0, occ.n_occs, 0,
			coalesce(e.minimum_body_mass, e.body_mass_estimate) as min,
			coalesce(e.maximum_body_mass, e.body_mass_estimate) as max,
			occ.first_early_int_seq, occ.first_late_int_seq,
			occ.last_early_int_seq, occ.last_late_int_seq,
			(a.preservation <> 'trace' or a.preservation is null)
		FROM $auth_table as a JOIN $TREE_WORK as t using (orig_no)
			LEFT JOIN ecotaph as e using (taxon_no)
			LEFT JOIN $TAXON_SUMMARY as occ using (orig_no)
		GROUP BY a.orig_no";
    
    $result = $dbh->do($sql);
    
    # Then coalesce the basic attributes across each synonym group, using
    # these to set the attributes for the senior synonym in each group.
    
    logMessage(2, "    coalescing attributes to senior synonyms");
    
    $sql = "	UPDATE $ATTRS_WORK as v JOIN 
		(SELECT t.synonym_no,
			if(sum(v.is_extant) > 0, 1, coalesce(is_extant)) as is_extant,
			min(v.min_body_mass) as min_body_mass,
			max(v.max_body_mass) as max_body_mass,
			sum(v.n_occs) as n_occs,
			if(sum(v.not_trace) > 0, 1, 0) as not_trace,
			min(v.first_early_int_seq) as first_early_int_seq,
			min(v.first_late_int_seq) as first_late_int_seq,
			min(v.last_early_int_seq) as last_early_int_seq,
			min(v.last_late_int_seq) as last_late_int_seq
		 FROM $ATTRS_WORK as v JOIN $TREE_WORK as t using (orig_no)
		 GROUP BY t.synonym_no) as nv on v.orig_no = nv.synonym_no
		SET     v.is_extant = nv.is_extant,
			v.n_occs = nv.n_occs,
			v.min_body_mass = nv.min_body_mass,
			v.max_body_mass = nv.max_body_mass,
			v.first_early_int_seq = nv.first_early_int_seq,
			v.first_late_int_seq = nv.first_late_int_seq,
			v.last_early_int_seq = nv.last_early_int_seq,
			v.last_late_int_seq = nv.last_late_int_seq,
			v.not_trace = nv.not_trace";
    
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
			if(sum(v.not_trace) > 0, 1, pv.not_trace) as not_trace
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
	# which are only known from trace fossils.
	
	my $sql = "
		UPDATE $ATTRS_WORK as v JOIN
		(SELECT t.parent_no,
			coalesce(least(min(v.first_early_int_seq), pv.first_early_int_seq), 
				min(v.first_early_int_seq), pv.first_early_int_seq) as first_early_int_seq,
			coalesce(least(min(v.first_late_int_seq), pv.first_late_int_seq), 
				min(v.first_late_int_seq), pv.first_late_int_seq) as first_late_int_seq,
			coalesce(least(min(v.last_early_int_seq), pv.last_early_int_seq), 
				min(v.last_early_int_seq), pv.last_early_int_seq) as last_early_int_seq,
			coalesce(least(min(v.last_late_int_seq), pv.last_late_int_seq), 
				min(v.last_late_int_seq), pv.last_late_int_seq) as last_late_int_seq
		 FROM $ATTRS_WORK as v JOIN $TREE_WORK as t using (orig_no)
			LEFT JOIN $ATTRS_WORK as pv on pv.orig_no = t.parent_no
		 WHERE t.depth = $child_depth and v.is_valid and v.is_senior and v.not_trace
		 GROUP BY t.parent_no) as nv on v.orig_no = nv.parent_no
		SET	v.first_early_int_seq = nv.first_early_int_seq,
			v.first_late_int_seq = nv.first_late_int_seq,
			v.last_early_int_seq = nv.last_early_int_seq,
			v.last_late_int_seq = nv.last_late_int_seq";
	
	$result = $dbh->do($sql);
	
	# Then coalesce attributes across synonym groups among the parents.
	# We use synonym_no instead of valid_no, because we want to count
	# children of invalid subgroups, etc. as being part of their valid
	# containing taxon.
	
	$sql = "
		UPDATE $ATTRS_WORK as v JOIN 
		(SELECT t.synonym_no,
			if(sum(v.is_extant) > 0, 1, coalesce(is_extant)) as is_extant,
			sum(v.extant_children) as extant_children_sum,
			sum(v.distinct_children) as distinct_children_sum,
			sum(v.extant_size) as extant_size_sum,
			sum(v.taxon_size) as taxon_size_sum,
			sum(v.n_occs) as n_occs,
			min(v.min_body_mass) as min_body_mass,
			max(v.max_body_mass) as max_body_mass,
			min(v.first_early_int_seq) as first_early_int_seq,
			min(v.first_late_int_seq) as first_late_int_seq,
			min(v.last_early_int_seq) as last_early_int_seq,
			min(v.last_late_int_seq) as last_late_int_seq,
			if(sum(v.not_trace) > 0, 1, 0) as not_trace
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
			v.first_early_int_seq = nv.first_early_int_seq,
			v.first_late_int_seq = nv.first_late_int_seq,
			v.last_early_int_seq = nv.last_early_int_seq,
			v.last_late_int_seq = nv.last_late_int_seq,
			v.not_trace = nv.not_trace";
	
	$result = $dbh->do($sql);
	
	# However, we also want to sum these up using valid_no, so that
	# invalid subgroups, etc. will have accurate counts.
	
	$sql = "
		UPDATE $ATTRS_WORK as v JOIN 
		(SELECT t.valid_no,
			if(sum(v.is_extant) > 0, 1, coalesce(is_extant)) as is_extant,
			sum(v.extant_children) as extant_children_sum,
			sum(v.distinct_children) as distinct_children_sum,
			sum(v.extant_size) as extant_size_sum,
			sum(v.taxon_size) as taxon_size_sum,
			sum(v.n_occs) as n_occs,
			min(v.min_body_mass) as min_body_mass,
			max(v.max_body_mass) as max_body_mass,
			min(v.first_early_int_seq) as first_early_int_seq,
			min(v.first_late_int_seq) as first_late_int_seq,
			min(v.last_early_int_seq) as last_early_int_seq,
			min(v.last_late_int_seq) as last_late_int_seq,
			if(sum(v.not_trace) > 0, 1, 0) as not_trace
		FROM $ATTRS_WORK as v JOIN $TREE_WORK as t using (orig_no)
		WHERE t.depth = $depth and t.valid_no <> t.synonym_no
		GROUP BY t.valid_no) as nv on v.orig_no = nv.valid_no
		SET     v.is_extant = nv.is_extant,
			v.extant_children = nv.extant_children_sum,
			v.distinct_children = nv.distinct_children_sum,
			v.extant_size = nv.extant_size_sum + if(nv.is_extant, 1, 0),
			v.taxon_size = nv.taxon_size_sum + 1,
			v.n_occs = nv.n_occs,
			v.min_body_mass = nv.min_body_mass,
			v.max_body_mass = nv.max_body_mass,
			v.first_early_int_seq = nv.first_early_int_seq,
			v.first_late_int_seq = nv.first_late_int_seq,
			v.last_early_int_seq = nv.last_early_int_seq,
			v.last_late_int_seq = nv.last_late_int_seq,
			v.not_trace = nv.not_trace";
	
	$result = $dbh->do($sql);
    }
    
    # Finally, we iterate from the top of the tree back down, computing those
    # attributes that propagate downward.  For the time being, the only one of
    # these is a value of extant=0 (which overrides any values of
    # extant=null).
    
    for (my $row = 2; $row <= $max_depth; $row++)
    {
	logMessage(2, "    computing tree level $row...") if $row % 10 == 0;
	
	# Children inherit the attributes of the senior synonym of the parent.
	
	my $sql = "
		UPDATE $ATTRS_WORK as v JOIN $TREE_WORK as t using (orig_no)
			JOIN $TREE_WORK as t2 on t2.orig_no = t.parent_no and t.depth = $row
			JOIN $ATTRS_WORK as pv on pv.orig_no = t.synonym_no
		SET v.is_extant = 0 WHERE pv.is_extant = 0";
	
	$dbh->do($sql);
    }
    
    # Now we have to copy the attributes of senior synonyms to all of their
    # junior synonyms.  We use valid_no rather than synonym_no, because a
    # junior synonym should only take on the attributes of another taxon that
    # it is substantially equivalent to.  We don't want, say, a nomen dubium
    # to have the same attributes as its containing taxon.
    
    logMessage(2, "    setting attributes for junior synonyms");
    
    $result = $dbh->do("
		UPDATE $ATTRS_WORK as v JOIN $TREE_WORK as t on v.orig_no = t.orig_no
			JOIN $ATTRS_WORK as sv on sv.orig_no = t.valid_no and sv.orig_no <> v.orig_no
		SET	v.is_extant = sv.is_extant,
			v.extant_children = sv.extant_children,
			v.distinct_children = sv.distinct_children,
			v.extant_size = sv.extant_size,
			v.taxon_size = sv.taxon_size,
			v.n_occs = sv.n_occs,
			v.min_body_mass = sv.min_body_mass,
			v.max_body_mass = sv.max_body_mass,
			v.first_early_int_seq = sv.first_early_int_seq,
			v.first_late_int_seq = sv.first_late_int_seq,
			v.last_early_int_seq = sv.last_early_int_seq,
			v.last_late_int_seq = sv.last_late_int_seq,
			v.not_trace = sv.not_trace");
    
    # We can stop here when debugging.
    
    my $a = 1;
}


# computeCollectionCounts ( dbh )
# 
# For each taxon, compute the number of distinct collections in which it or
# any of its subtaxa appears.

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



# computeCollectionTables ( dbh )
# 
# Group the set of collections into bins, on two different levels of
# resolution.  The fine-grained resolution is generated by simply binning the
# collections into bins of size $FINE_BIN_SIZE (expressed in degrees of
# latitude/longitude), while the coarse-grained resolution is generated by
# apply the k-means clustering algorithm to the set of fine bins.  One cluster
# is generated for each non-empty square of size $COARSE_BIN_SIZE on the
# surface of the earth, but the clusters may overlap the coarse bin
# boundaries.  This process generates a tractable set of data points (order of
# magitude close to 1000) for displaying large-scale and medium-scale maps of
# our collections.  This routine is called whenever the set of clusters needs
# to be initially generated or completely rebuilt.  Otherwise,
# updateCollectionBins() can be used to add newly entered collections to the
# existing cluster tables.

sub computeCollectionTables {

    my ($dbh) = @_;
    
    my ($result, $sql);
    
    $MSG_TAG = "Rebuild";
    
    # Make sure that the country code lookup table and interval map are the
    # database.
    
    createCountryMap($dbh);
    computeIntervalTables($dbh);
    
    # Now create a clean working table which will become the new collection
    # matrix.
    
    logMessage(1, "rebuilding collection tables");
    
    $dbh->do("DROP TABLE IF EXISTS $COLL_MATRIX_WORK");
    
    $dbh->do("CREATE TABLE $COLL_MATRIX_WORK (
		collection_no int unsigned primary key,
		bin_lng smallint unsigned not null,
		bin_lat smallint unsigned not null,
		bin_id int unsigned,
		clust_id int unsigned,
		lng float,
		lat float,
		loc point not null,
		cc char(2),
		early_int_no int unsigned not null,
		late_int_no int unsigned not null,
		early_st_seq int unsigned not null,
		late_st_seq int unsigned not null,
		early_age float,
		late_age float,
		n_occs int unsigned not null,
		reference_no int unsigned not null,
		access_level tinyint unsigned not null) Engine=MYISAM");
    
    logMessage(2, "    inserting collections...");
    
    $sql = "	INSERT INTO $COLL_MATRIX_WORK
		       (collection_no, bin_lng, bin_lat, lng, lat, loc, cc,
			early_int_no, late_int_no, early_age, late_age, 
			reference_no, access_level)
		SELECT c.collection_no, 
			if(c.lng between -180.0 and 180.0, floor((c.lng+180.0)/$FINE_BIN_SIZE), null) as bin_lng,
			if(c.lat between -90.0 and 90.0, floor((c.lat+90.0)/$FINE_BIN_SIZE), null) as bin_lat,
			c.lng, c.lat, point(c.lng, c.lat), map.cc,
			imax.interval_no, imin.interval_no,
			imax.base_age, imin.top_age, c.reference_no,
			case c.access_level
				when 'database members' then if(c.release_date < now(), 0, 1)
				when 'research group' then if(c.release_date < now(), 0, 2)
				when 'authorizer only' then if(c.release_date < now(), 0, 2)
				else 0
			end
		FROM collections as c
			JOIN $INTERVAL_MAP as imax on imax.interval_no = c.max_interval_no
			JOIN $INTERVAL_MAP as imin on imin.interval_no = 
				if(c.min_interval_no > 0, c.min_interval_no, c.max_interval_no)
			LEFT JOIN country_map as map on map.name = c.country";
    
    my $count = $dbh->do($sql);
    
    logMessage(2, "      $count collections");
    
    logMessage(2, "    counting occurrences for each collection");
    
    $sql = "UPDATE $COLL_MATRIX_WORK as m JOIN
		(SELECT collection_no, count(*) as n_occs
		FROM occurrences GROUP BY collection_no) as sum using (collection_no)
	    SET m.n_occs = sum.n_occs";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "    setting standard interval sequence numbers");
    
    # First switch early/late intervals if they were given in the wrong order
    
    $sql = "UPDATE $COLL_MATRIX_WORK as m
		JOIN $INTERVAL_MAP as ei on ei.interval_no = m.early_int_no
		JOIN $INTERVAL_MAP as li on li.interval_no = m.late_int_no
	    SET early_int_no = (\@tmp := early_int_no), early_int_no = late_int_no, late_int_no = \@tmp
	    WHERE ei.base_age < li.base_age";
    
    $result = $dbh->do($sql);
    
    # Then look up the sequence numbers
    
    $sql = "UPDATE $COLL_MATRIX_WORK as m
		JOIN $INTERVAL_MAP as ei on ei.interval_no = m.early_int_no
		JOIN $INTERVAL_MAP as ei2 on ei2.interval_no = ei.early_st_no
		JOIN $INTERVAL_MAP as li on li.interval_no = m.late_int_no
		JOIN $INTERVAL_MAP as li2 on li2.interval_no = li.late_st_no
	    SET m.early_st_seq = ei2.older_seq, m.late_st_seq = li2.younger_seq";
    
    $result = $dbh->do($sql);
    
    # Then we can create a table to indicate which collections correspond to
    # which intervals from the standard set.
    
    logMessage(2, "    computing collection interval table");
    
    $dbh->do("DROP TABLE IF EXISTS $COLL_INTS_WORK");
    
    $dbh->do("CREATE TABLE $COLL_INTS_WORK (
		collection_no int unsigned not null,
		interval_no int unsigned not null,
		unique key (collection_no, interval_no))");
    
    $sql = "INSERT IGNORE INTO $COLL_INTS_WORK
		SELECT m.collection_no, i.interval_no FROM $COLL_MATRIX_WORK as m
			JOIN $INTERVAL_MAP as li on li.younger_seq = m.late_st_seq
			JOIN $INTERVAL_MAP as ei on ei.older_seq = m.early_st_seq
			JOIN $INTERVAL_MAP as i on i.younger_seq >= m.late_st_seq and i.top_age < ei.top_age
				and i.level = li.level
		WHERE li.level <= ei.level";
    
    $result = $dbh->do($sql);
    
    $sql = "INSERT IGNORE INTO $COLL_INTS_WORK
		SELECT m.collection_no, i.interval_no FROM $COLL_MATRIX_WORK as m
			JOIN $INTERVAL_MAP as li on li.younger_seq = m.late_st_seq
			JOIN $INTERVAL_MAP as ei on ei.older_seq = m.early_st_seq
			JOIN $INTERVAL_MAP as i on i.older_seq >= m.early_st_seq and i.base_age > li.base_age
				and i.level = ei.level
		WHERE li.level > ei.level";
    
    $result = $dbh->do($sql);
    
    $sql = "DELETE FROM $COLL_INTS_WORK WHERE interval_no = 0";
    
    $result = $dbh->do($sql);
    
    # Now that the table is full, we can add the necessary indices much more
    # efficiently than if we had defined them at the start.
    
    logMessage(2, "    indexing by bin identifier");
    
    $result = $dbh->do("UPDATE $COLL_MATRIX_WORK SET bin_id = 200000000 + bin_lng * 10000 + bin_lat");
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (bin_id)");
    
    logMessage(2, "    indexing by geographic coordinates (spatial)");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD SPATIAL INDEX (loc)");
    
    logMessage(2, "    indexing by geographic coordinates (separate)");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (lng, lat)");
    
    logMessage(2, "    indexing by country");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (cc)");
    
    logMessage(2, "    indexing by reference_no");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (reference_no)");
    
    logMessage(2, "    indexing by chronological interval");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (early_int_no)");
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (late_int_no)");
    
    logMessage(2, "    indexing by early and late age");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (early_age)");
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (late_age)");
    
    # We then group the collections into fine-resolution bins with integer
    # coordinates, taking the centroid of each bin and counting the number of
    # collections that fall within it.  We will be able to use this set of
    # bins to generate medium-scale maps, in order to reduce the number of
    # data points that need to be displayed.  We will also use this set of
    # bins as the individual data points for the k-means algorithm, which will
    # allow us to efficiently generate the coarse-grained clusters that we
    # need for displaying global maps.
    
    logMessage(2, "    grouping collections into fine bins...");
    
    $dbh->do("DROP TABLE IF EXISTS $COLL_BINS_WORK");
    
    $dbh->do("CREATE TABLE $COLL_BINS_WORK (
		bin_lng smallint unsigned not null,
		bin_lat smallint unsigned not null,
		bin_id int unsigned primary key,
		clust_id int unsigned,
		n_colls int unsigned,
		n_occs int unsigned,
		lng float,
		lat float,
		lng_min float,
		lng_max float,
		lat_min float,
		lat_max float,
		std_dev float,
		access_level tinyint unsigned not null,
		unique key (bin_lng, bin_lat)) Engine=MyISAM");
    
    my ($sql, $result);
    
    $sql = "	INSERT IGNORE INTO $COLL_BINS_WORK
			(bin_lng, bin_lat, bin_id, n_colls, n_occs, lng, lat, 
			 lng_min, lng_max, lat_min, lat_max, std_dev,
			 access_level)
		SELECT bin_lng, bin_lat, bin_id, count(*), sum(n_occs), avg(lng), avg(lat),
		       round(min(lng),2) as lng_min, round(max(lng),2) as lng_max,
		       round(min(lat),2) as lat_min, round(max(lat),2) as lat_max,
		       sqrt(var_pop(lng)+var_pop(lat)),
		       min(access_level)
		FROM $COLL_MATRIX_WORK
		GROUP BY bin_lng, bin_lat";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "    generated $result non-empty bins.");
    
    # Next, we create a table to describe the coarse clusters.
    
    $dbh->do("DROP TABLE IF EXISTS $COLL_CLUST_WORK");
    
    $dbh->do("CREATE TABLE $COLL_CLUST_WORK (
		clust_lng smallint unsigned not null,
		clust_lat smallint unsigned not null,
		clust_id int unsigned not null,
		n_colls int unsigned,
		n_occs int unsigned,
		lng float,
		lat float,
		lng_min float,
		lng_max float,
		lat_min float,
		lat_max float,
		std_dev float,
		access_level tinyint unsigned,
		unique key (clust_lng, clust_lat)) Engine=MyISAM");
    
    # And finally, an auxiliary table for use in computing cluster assignments
    
    $dbh->do("DROP TABLE IF EXISTS $CLUST_AUX");
    
    $dbh->do("CREATE TABLE $CLUST_AUX (
		bin_id int unsigned primary key,
		clust_id int unsigned not null)");
    
    # We must now seed the k-means algorithm by choosing an initial set of
    # collections.  This set will determine the number of clusters in the
    # final result, and its geographical distribution will have a dramatic
    # influence on the shape of the final set of clusters.  So we need to
    # choose carefully.  We want to limit the maximum geographic extent of any
    # particular cluster, and to ensure that geographically isolated
    # collections are always formed into their own clusters.  To accomplish
    # this, we re-bin the collections more coarsely into 5-degree-square bins
    # (still with integer coordinates) and choose as our initial cluster
    # coordinates the centroid of each bin (note that this requires a weighted
    # average of the centroids of the constituent bins weighted by the number
    # of collections in each one).  The number of non-empty bins will then
    # determine the number of clusters.  We won't fill in the number of
    # collections for each cluster until later, because the k-means algorithm
    # will move collections from cluster to cluster until it stabilizes.
    
    my $bin_ratio = $FINE_BIN_SIZE / $COARSE_BIN_SIZE;
    
    $sql = "	INSERT IGNORE INTO $COLL_CLUST_WORK (clust_lng, clust_lat, lng, lat)
		SELECT clust_lng, clust_lat, 
		       sum(lng * n_colls)/sum(n_colls), sum(lat * n_colls)/sum(n_colls)
		FROM (SELECT floor(bin_lng * $bin_ratio) as clust_lng,
			     floor(bin_lat * $bin_ratio) as clust_lat,
			     lng, lat, n_colls FROM $COLL_BINS_WORK) as c
		GROUP BY clust_lng, clust_lat";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "    seeded the cluster table with $result data points (bins)");
    
    $result = $dbh->do("UPDATE $COLL_CLUST_WORK SET clust_id = 1000000 + clust_lng * 1000 + clust_lat");
    
    # Now we make initial assignments of bins to clusters, by assigning each
    # bin to the cluster whose centroid is closest to it (which may be on the
    # other side of a coarse-bin boundary).  We use the $CLUST_AUX table along
    # with an ORDER BY clause to select the closest cluster for each bin, and
    # then copy the cluster identifiers clust_lng and clust_lat back into
    # $COLL_BINS_WORK.
    
    logMessage(2, "    assigning bins to clusters...");
    
    $sql = "	INSERT IGNORE INTO $CLUST_AUX
		SELECT b.bin_id, k.clust_id
		FROM $COLL_BINS_WORK as b JOIN $COLL_CLUST_WORK as k
			on k.clust_lng between floor(bin_lng * $bin_ratio)-1
				and floor(bin_lng * $bin_ratio)+1
			and k.clust_lat between floor(bin_lat * $bin_ratio)-1
				and floor(bin_lat * $bin_ratio)+1
		ORDER BY POW(k.lat-b.lat,2)+POW(k.lng-b.lng,2) ASC";
    
    $dbh->do($sql);
    
    $sql = "    UPDATE $COLL_BINS_WORK as cb JOIN $CLUST_AUX as k using (bin_id)
		SET cb.clust_id = k.clust_id";
    
    # $sql = "	UPDATE $COLL_BINS_WORK as c SET c.clust_no = 
    # 		(SELECT k.clust_no from $COLL_CLUST_WORK as k 
    # 		 ORDER BY POW(k.lat-c.lat, 2) + POW(k.lng-c.lng, 2) ASC LIMIT 1)";
    
    my ($rows_changed) = $dbh->do($sql);
    my ($bound) = 0;
    
    # Then we update the centroid of each cluster and recompute the cluster
    # assignments.  Repeat until no points move to a different cluster, or at
    # most the specified number of rounds.
    
    while ( $rows_changed > 0 and $bound < 10 )
    {
	# Compute the centroid of each cluster based on the data points (bins)
	# assigned to it.
	
	logMessage(2, "    computing cluster centroids...");
	
	$sql = "UPDATE $COLL_CLUST_WORK as k JOIN 
		(SELECT clust_id,
			sum(lng * n_colls)/sum(n_colls) as lng_avg,
			sum(lat * n_colls)/sum(n_colls) as lat_avg
		 FROM $COLL_BINS_WORK GROUP BY clust_id) as cluster
			using (clust_id)
		SET k.lng = cluster.lng_avg, k.lat = cluster.lat_avg";
	
	$result = $dbh->do($sql);
	
	# Then reassign each point (bin) to the closest cluster.
	
	logMessage(2, "    recomputing cluster assignments...");
	
	$dbh->do("DELETE FROM $CLUST_AUX");
	
	$sql = "INSERT IGNORE INTO $CLUST_AUX
		SELECT b.bin_id, k.clust_id
		FROM $COLL_BINS_WORK as b JOIN $COLL_CLUST_WORK as k
			on k.clust_lng between floor(bin_lng * $bin_ratio)-1
				and floor(bin_lng * $bin_ratio)+1
			and k.clust_lat between floor(bin_lat * $bin_ratio)-1
				and floor(bin_lat * $bin_ratio)+1
		ORDER BY POW(k.lat-b.lat,2)+POW(k.lng-b.lng,2) ASC";
	
	$dbh->do($sql);
	
	$sql = "UPDATE $COLL_BINS_WORK as cb JOIN $CLUST_AUX as k using (bin_id)
		SET cb.clust_id = k.clust_id";
	
	# $sql = "UPDATE $COLL_BINS_WORK as c SET c.clust_no = 
	# 	(SELECT k.clust_no from $COLL_CLUST_WORK as k 
	# 	 ORDER BY POW(k.lat-c.lat,2)+POW(k.lng-c.lng,2) ASC LIMIT 1)";
	
	($rows_changed) = $dbh->do($sql);
	
	logMessage(2, "    $rows_changed rows changed");
	
	$bound++;
    }
    
    logMessage(2, "    setting collection cluster numbers...");
    
    # Now that we have a stable assignment of bins to clusters, we can record
    # the cluster assignments for each individual collection.
    
    $sql = "	UPDATE $COLL_MATRIX_WORK as c 
		JOIN $COLL_BINS_WORK as cb using (bin_id)
		SET c.clust_id = cb.clust_id";
    
    $result = $dbh->do($sql);
    
    $result = $dbh->do("ALTER TABLE $COLL_BINS_WORK ADD INDEX (clust_id)");
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (clust_id)");
    
    # Now we can compute the total number of collections and occurrences, the
    # geographic extent, the access level, and the standard deviation for each
    # cluster (the cluster centroids have already been computed above).
    
    logMessage(2, "   setting collection statistics for each cluster...");
    
    $sql = "    UPDATE $COLL_CLUST_WORK as k JOIN
		(SELECT clust_id, sum(n_colls) as n_colls,
			sum(n_occs) as n_occs,
			sqrt(var_pop(lng)+var_pop(lat)) as std_dev,
			min(lng_min) as lng_min, max(lng_max) as lng_max,
			min(lat_min) as lat_min, max(lat_max) as lat_max,
			min(access_level) as access_level
		FROM $COLL_BINS_WORK GROUP BY clust_id) as agg
			using (clust_id)
		SET k.n_colls = agg.n_colls, k.n_occs = agg.n_occs,
		    k.std_dev = agg.std_dev, k.access_level = agg.access_level,
		    k.lng_min = agg.lng_min, k.lng_max = agg.lng_max,
		    k.lat_min = agg.lat_min, k.lat_max = agg.lat_max";
    
    $result = $dbh->do($sql);
    
    # Finally, we swap in the new tables for the old ones.
    
    logMessage(2, "activating tables '$COLL_MATRIX', '$COLL_BINS', '$COLL_CLUST'");
    
    # Compute the backup names of all the tables to be activated
    
    my $coll_matrix_bak = "${COLL_MATRIX}_bak";
    my $coll_bins_bak = "${COLL_BINS}_bak";
    my $coll_clust_bak = "${COLL_CLUST}_bak";
    
    # Delete any old tables that might have been left around.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $coll_matrix_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $coll_bins_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $coll_clust_bak");
    
    # Do the swap.
    
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $COLL_MATRIX LIKE $COLL_MATRIX_WORK");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $COLL_BINS LIKE $COLL_BINS_WORK");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $COLL_CLUST LIKE $COLL_CLUST_WORK");
    
    $result = $dbh->do("RENAME TABLE
			    $COLL_MATRIX to $coll_matrix_bak,
			    $COLL_MATRIX_WORK to $COLL_MATRIX,
			    $COLL_BINS to $coll_bins_bak,
			    $COLL_BINS_WORK to $COLL_BINS,
			    $COLL_CLUST to $coll_clust_bak,
			    $COLL_CLUST_WORK to $COLL_CLUST");
    
    # Delete the old tables.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $coll_matrix_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $coll_bins_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $coll_clust_bak");
    
    my $a = 1;		# We can stop here when debugging
}


# computeOccurrenceTables ( dbh, options )
# 
# Compute the occurrence matrix, recording which taxonomic concepts are
# associated with which collections in which geological and chronological
# locations.  This table is used to satisfy the bulk of the queries from the
# front-end application.  This function also computes an occurrence summary
# table, summarizing occurrence information by taxon.

sub computeOccurrenceTables {
    
    my ($dbh, $options) = @_;
    
    my ($sql, $result, $count);
    
    $MSG_TAG = "Rebuild";
    
    # Create a clean working table which will become the new occurrence
    # matrix.
    
    logMessage(1, "Rebuilding occurrence tables");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $OCC_MATRIX_WORK");
    $result = $dbh->do("CREATE TABLE $OCC_MATRIX_WORK (
				occurrence_no int unsigned primary key,
				collection_no int unsigned not null,
				reid_no int unsigned not null,
				taxon_no int unsigned not null,
				orig_no int unsigned not null,
				reference_no int unsigned not null) ENGINE=MyISAM");
    
    # Add one row for every occurrence in the database.
    
    logMessage(2, "    inserting occurrences...");
    
    $sql = "	INSERT INTO $OCC_MATRIX_WORK
		       (occurrence_no, collection_no, taxon_no, orig_no, reference_no)
		SELECT o.occurrence_no, o.collection_no, o.taxon_no, a.orig_no,
			if(o.reference_no > 0, o.reference_no, c.reference_no)
		FROM occurrences as o JOIN collections as c using (collection_no)
			LEFT JOIN authorities as a using (taxon_no)";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count occurrences");
    
    # Update each occurrence entry as necessary to take into account the latest
    # reidentification if any.
    
    $sql = "	UPDATE $OCC_MATRIX_WORK as m
			JOIN reidentifications as re on re.occurrence_no = m.occurrence_no and re.most_recent = 'YES'
			JOIN authorities as a on a.taxon_no = re.taxon_no
		SET m.reid_no = re.reid_no,
		    m.taxon_no = re.taxon_no,
		    m.orig_no = a.orig_no,
		    m.reference_no = if(re.reference_no > 0, re.reference_no, m.reference_no)";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count re-identifications");
    
    # Add some indices to the main occurrence relation, which is more
    # efficient to do now that the table is populated.
    
    logMessage(2, "    indexing by collection");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (collection_no)");
    
    logMessage(2, "    indexing by taxonomic concept");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (orig_no)");
    
    logMessage(2, "    indexing by reference_no");
    
    $result = $dbh->do("ALTER TABLE $OCC_MATRIX_WORK ADD INDEX (reference_no)");
    
    # We now summarize the occurrence matrix by taxon.  We use the older_seq and
    # younger_seq interval identifications instead of interval_no, in order
    # that we can use the min() function to find the temporal bounds for each taxon.
    
    logMessage(2, "    summarizing by taxon");
    
    # Then create working tables which will become the new occurrence summary
    # table and reference summary table.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $TAXON_SUMMARY_WORK");
    $result = $dbh->do("CREATE TABLE $TAXON_SUMMARY_WORK (
				orig_no int unsigned primary key,
				n_occs int unsigned not null,
				n_colls int unsigned not null,
				first_early_int_seq int unsigned not null,
				first_late_int_seq int unsigned not null,
				last_early_int_seq int unsigned not null,
				last_late_int_seq int unsigned not null) ENGINE=MyISAM");
    
    $sql = "	INSERT INTO $TAXON_SUMMARY_WORK (orig_no, n_occs, n_colls,
			first_early_int_seq, first_late_int_seq, last_early_int_seq, last_late_int_seq)
		SELECT m.orig_no, count(*), count(distinct collection_no),
			min(ei.older_seq), min(li.older_seq), min(ei.younger_seq), min(li.younger_seq)
		FROM $OCC_MATRIX_WORK as m JOIN $COLL_MATRIX as c using (collection_no)
			JOIN interval_map as ei on ei.interval_no = c.early_int_no
			JOIN interval_map as li on li.interval_no = c.late_int_no
		GROUP BY m.orig_no";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count taxa");
    
    # Then index the symmary table by earliest and latest interval number, so
    # that we can quickly query for which taxa began or ended at a particular
    # time. 
    
    logMessage(2, "    indexing the summary table");
    
    $result = $dbh->do("ALTER TABLE $TAXON_SUMMARY_WORK ADD INDEX (n_occs)");
    $result = $dbh->do("ALTER TABLE $TAXON_SUMMARY_WORK ADD INDEX (n_colls)");
    
    # We now summarize the occurrence matrix by reference_no.  For each
    # reference, we record the range of time periods it covers, plus the
    # number of occurrences and collections that refer to it.
    
    logMessage(2, "    summarizing by reference_no");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $REF_SUMMARY_WORK");
    $result = $dbh->do("CREATE TABLE $REF_SUMMARY_WORK (
				reference_no int unsigned primary key,
				n_occs int unsigned not null,
				n_colls int unsigned not null,
				early_int_seq int unsigned not null,
				late_int_seq int unsigned not null) ENGINE=MyISAM");
    
    $sql = "	INSERT INTO $REF_SUMMARY_WORK (reference_no, n_occs, n_colls,
			early_int_seq, late_int_seq)
		SELECT m.reference_no, count(*), count(distinct collection_no),
			min(ei.older_seq), min(li.younger_seq)
		FROM $OCC_MATRIX_WORK as m JOIN $COLL_MATRIX as c using (collection_no)
			JOIN interval_map as ei on ei.interval_no = c.early_int_no
			JOIN interval_map as li on li.interval_no = c.late_int_no
		GROUP BY m.reference_no";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      $count references");
    
    # Then index the reference summary table by numbers of collections and
    # occurrences, so that we can quickly query for the most heavily used ones.
    
    logMessage(2, "    indexing the summary table");
    
    $result = $dbh->do("ALTER TABLE $REF_SUMMARY_WORK ADD INDEX (n_occs)");
    $result = $dbh->do("ALTER TABLE $REF_SUMMARY_WORK ADD INDEX (n_colls)");
    
    # Now swap in the new tables:
    
    logMessage(2, "activating tables '$OCC_MATRIX', '$TAXON_SUMMARY', '$REF_SUMMARY'");
    
    # Compute the backup names of all the tables to be activated
    
    my $occ_matrix_bak = "${OCC_MATRIX}_bak";
    my $taxon_summary_bak = "${TAXON_SUMMARY}_bak";
    my $ref_summary_bak = "${REF_SUMMARY}_bak";
    
    # Delete any old tables that might have been left around.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $occ_matrix_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $taxon_summary_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $ref_summary_bak");
    
    # Do the swap.
    
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $OCC_MATRIX LIKE $OCC_MATRIX_WORK");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $TAXON_SUMMARY LIKE $TAXON_SUMMARY_WORK");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $REF_SUMMARY LIKE $REF_SUMMARY_WORK");
    
    $result = $dbh->do("RENAME TABLE
			    $OCC_MATRIX to $occ_matrix_bak,
			    $OCC_MATRIX_WORK to $OCC_MATRIX,
			    $TAXON_SUMMARY to $taxon_summary_bak,
			    $TAXON_SUMMARY_WORK to $TAXON_SUMMARY,
			    $REF_SUMMARY to $ref_summary_bak,
			    $REF_SUMMARY_WORK to $REF_SUMMARY");
    
    # Delete the old tables.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $occ_matrix_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $taxon_summary_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $ref_summary_bak");
}



# createCountryMap ( dbh, force )
# 
# Create the country_map table if it does not already exist.
# Still need to fix: Zaire, U.A.E., UAE, Czechoslovakia, Netherlands Antilles

sub createCountryMap {

    my ($dbh, $force) = @_;
    
    # First make sure we have a clean table.
    
    if ( $force )
    {
	$dbh->do("DROP TABLE IF EXISTS $COUNTRY_MAP");
    }
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $COUNTRY_MAP (
		cc char(2) primary key,
		continent char(2),
		name varchar(80) not null,
		INDEX (name),
		INDEX (continent)) Engine=MyISAM");
    
    # Then populate it if necessary.
    
    my ($count) = $dbh->selectrow_array("SELECT count(*) FROM $COUNTRY_MAP");
    
    return if $count;
    
    logMessage(2, "    rebuilding country map");
    
    $dbh->do("INSERT INTO $COUNTRY_MAP (cc, continent, name) VALUES
	('AU', 'AU', 'Australia'),
	('DZ', 'AF', 'Algeria'),
	('AO', 'AF', 'Angola'),
	('BW', 'AF', 'Botswana'),
	('CM', 'AF', 'Cameroon'),
	('CV', 'AF', 'Cape Verde'),
	('TD', 'AF', 'Chad'),
	('CG', 'AF', 'Congo-Brazzaville'),
	('CD', 'AF', 'Congo-Kinshasa'),
	('CI', 'AF', 'Cote D\\'Ivoire'),
	('DJ', 'AF', 'Djibouti'),
	('EG', 'AF', 'Egypt'),
	('ER', 'AF', 'Eritrea'),
	('ET', 'AF', 'Ethiopia'),
	('GA', 'AF', 'Gabon'),
	('GH', 'AF', 'Ghana'),
	('GN', 'AF', 'Guinea'),
	('KE', 'AF', 'Kenya'),
	('LS', 'AF', 'Lesotho'),
	('LY', 'AF', 'Libya'),
	('MW', 'AF', 'Malawi'),
	('ML', 'AF', 'Mali'),
	('MR', 'AF', 'Mauritania'),
	('MA', 'AF', 'Morocco'),
	('MZ', 'AF', 'Mozambique'),
	('NA', 'AF', 'Namibia'),
	('NE', 'AF', 'Niger'),
	('NG', 'AF', 'Nigeria'),
	('SH', 'AF', 'Saint Helena'),
	('SN', 'AF', 'Senegal'),
	('SO', 'AF', 'Somalia'),
	('ZA', 'AF', 'South Africa'),
	('SD', 'AF', 'Sudan'),
	('SZ', 'AF', 'Swaziland'),
	('TZ', 'AF', 'Tanzania'),
	('TG', 'AF', 'Togo'),
	('TN', 'AF', 'Tunisia'),
	('UG', 'AF', 'Uganda'),
	('EH', 'AF', 'Western Sahara'),
	('ZM', 'AF', 'Zambia'),
	('ZW', 'AF', 'Zimbabwe'),
	('AR', 'SA', 'Argentina'),
	('BO', 'SA', 'Bolivia'),
	('BR', 'SA', 'Brazil'),
	('CL', 'SA', 'Chile'),
	('CO', 'SA', 'Colombia'),
	('EC', 'SA', 'Ecuador'),
	('FA', 'SA', 'Falkland Islands (Malvinas)'),
	('GY', 'SA', 'Guyana'),
	('PY', 'SA', 'Paraguay'),
	('PE', 'SA', 'Peru'),
	('SR', 'SA', 'Suriname'),
	('UY', 'SA', 'Uruguay'),
	('VE', 'SA', 'Venezuela'),
	('AE', 'AS', 'United Arab Emirates'),
	('AM', 'AS', 'Armenia'),
	('AZ', 'AS', 'Azerbaijan'),
	('BH', 'AS', 'Bahrain'),
	('KH', 'AS', 'Cambodia'),
	('TL', 'AS', 'East Timor'),
	('GE', 'AS', 'Georgia'),
	('ID', 'AS', 'Indonesia'),
	('IR', 'AS', 'Iran'),
	('IQ', 'AS', 'Iraq'),
	('IL', 'AS', 'Israel'),
	('JO', 'AS', 'Jordan'),
	('KW', 'AS', 'Kuwait'),
	('KG', 'AS', 'Kyrgyzstan'),
	('LB', 'AS', 'Lebanon'),
	('KP', 'AS', 'North Korea'),
	('OM', 'AS', 'Oman'),
	('PS', 'AS', 'Palestinian Territory'),
	('QA', 'AS', 'Qatar'),
	('SA', 'AS', 'Saudi Arabia'),
	('KR', 'AS', 'South Korea'),
	('SY', 'AS', 'Syria'),
	('TR', 'AS', 'Turkey'),
	('YE', 'AS', 'Yemen'),
	('AF', 'AS', 'Afghanistan'),
	('BD', 'AS', 'Bangladesh'),
	('BT', 'AS', 'Bhutan'),
	('IN', 'AS', 'India'),
	('KZ', 'AS', 'Kazakstan'),
	('MY', 'AS', 'Malaysia'),
	('MM', 'AS', 'Myanmar'),
	('NP', 'AS', 'Nepal'),
	('PK', 'AS', 'Pakistan'),
	('PH', 'AS', 'Philippines'),
	('LK', 'AS', 'Sri Lanka'),
	('TW', 'AS', 'Taiwan'),
	('TJ', 'AS', 'Tajikistan'),
	('TH', 'AS', 'Thailand'),
	('TM', 'AS', 'Turkmenistan'),
	('TU', 'AS', 'Tuva'),
	('UZ', 'AS', 'Uzbekistan'),
	('VN', 'AS', 'Vietnam'),
	('CN', 'AS', 'China'),
	('HK', 'AS', 'Hong Kong'),
	('JP', 'AS', 'Japan'),
	('MN', 'AS', 'Mongolia'),
	('LA', 'AS', 'Laos'),
	('AA', 'AA', 'Antarctica'),
	('AL', 'EU', 'Albania'),
	('AT', 'EU', 'Austria'),
	('BY', 'EU', 'Belarus'),
	('BE', 'EU', 'Belgium'),
	('BG', 'EU', 'Bulgaria'),
	('HR', 'EU', 'Croatia'),
	('CY', 'EU', 'Cyprus'),
	('CZ', 'EU', 'Czech Republic'),
	('DK', 'EU', 'Denmark'),
	('EE', 'EU', 'Estonia'),
	('FI', 'EU', 'Finland'),
	('FR', 'EU', 'France'),
	('DE', 'EU', 'Germany'),
	('GR', 'EU', 'Greece'),
	('HU', 'EU', 'Hungary'),
	('IS', 'EU', 'Iceland'),
	('IE', 'EU', 'Ireland'),
	('IT', 'EU', 'Italy'),
	('LV', 'EU', 'Latvia'),
	('LT', 'EU', 'Lithuania'),
	('LU', 'EU', 'Luxembourg'),
	('MK', 'EU', 'Macedonia'),
	('MT', 'EU', 'Malta'),
	('MD', 'EU', 'Moldova'),
	('NL', 'EU', 'Netherlands'),
	('NO', 'EU', 'Norway'),
	('PL', 'EU', 'Poland'),
	('PT', 'EU', 'Portugal'),
	('RO', 'EU', 'Romania'),
	('RU', 'EU', 'Russian Federation'),
	('SM', 'EU', 'San Marino'),
	('RS', 'EU', 'Serbia and Montenegro'),
	('SK', 'EU', 'Slovakia'),
	('SI', 'EU', 'Slovenia'),
	('ES', 'EU', 'Spain'),
	('SJ', 'EU', 'Svalbard and Jan Mayen'),
	('SE', 'EU', 'Sweden'),
	('CH', 'EU', 'Switzerland'),
	('UA', 'EU', 'Ukraine'),
	('UK', 'EU', 'United Kingdom'),
	('BA', 'EU', 'Bosnia and Herzegovina'),
	('GL', 'NA', 'Greenland'),
	('US', 'NA', 'United States'),
	('CA', 'NA', 'Canada'),
	('MX', 'NA', 'Mexico'),
	('AI', 'NA', 'Anguilla'),
	('AG', 'NA', 'Antigua and Barbuda'),
	('BS', 'NA', 'Bahamas'),
	('BB', 'NA', 'Barbados'),
	('BM', 'NA', 'Bermuda'),
	('KY', 'NA', 'Cayman Islands'),
	('CU', 'NA', 'Cuba'),
	('DO', 'NA', 'Dominican Republic'),
	('GP', 'NA', 'Guadeloupe'),
	('HT', 'NA', 'Haiti'),
	('JM', 'NA', 'Jamaica'),
	('PR', 'NA', 'Puerto Rico'),
	('BZ', 'NA', 'Belize'),
	('CR', 'NA', 'Costa Rica'),
	('SV', 'NA', 'El Salvador'),
	('GD', 'NA', 'Grenada'),
	('GT', 'NA', 'Guatemala'),
	('HN', 'NA', 'Honduras'),
	('NI', 'NA', 'Nicaragua'),
	('PA', 'NA', 'Panama'),
	('TT', 'NA', 'Trinidad and Tobago'),
	('AW', 'NA', 'Aruba'),
	('CW', 'NA', 'Curaçao'),
	('SX', 'NA', 'Sint Maarten'),
	('CK', 'OC', 'Cook Islands'),
	('FJ', 'OC', 'Fiji'),
	('PF', 'OC', 'French Polynesia'),
	('GU', 'OC', 'Guam'),
	('MH', 'OC', 'Marshall Islands'),
	('NC', 'OC', 'New Caledonia'),
	('NZ', 'OC', 'New Zealand'),
	('MP', 'OC', 'Northern Mariana Islands'),
	('PW', 'OC', 'Palau'),
	('PG', 'OC', 'Papua New Guinea'),
	('PN', 'OC', 'Pitcairn'),
	('TO', 'OC', 'Tonga'),
	('TV', 'OC', 'Tuvalu'),
	('UM', 'OC', 'United States Minor Outlying Islands'),
	('VU', 'OC', 'Vanuatu'),
	('TF', 'IO', 'French Southern Territories'),
	('MG', 'IO', 'Madagascar'),
	('MV', 'IO', 'Maldives'),
	('MU', 'IO', 'Mauritius'),
	('YT', 'IO', 'Mayotte'),
	('SC', 'IO', 'Seychelles')");
}


# computeIntervalTables ( dbh, force )
# 
# Create a new table for time intervals, containing all of the necessary
# information for each interval, and numbering them in two different
# sequences: younger to older, and older to younger.

sub computeIntervalTables {

    my ($dbh, $force) = @_;
    
    my ($sql, $result, $count);

    # If $force was not specified, abort if there is already an interval table
    # and it has data in it.
    
    unless ( $force )
    {
	$dbh->do("CREATE TABLE IF NOT EXISTS $INTERVAL_MAP (a int unsigned)");
	($count) = $dbh->do("SELECT count(*) FROM $INTERVAL_MAP");
	return if $count;
    }
    
    # Otherwise, create a working table and populate it.
    
    $dbh->do("DROP TABLE IF EXISTS $INTERVAL_MAP_WORK");
    
    $dbh->do("CREATE TABLE $INTERVAL_MAP_WORK (
		interval_no int unsigned primary key,
		interval_name varchar(80) not null,
		abbrev varchar(10),
		level tinyint unsigned,
		parent_no int unsigned,
		early_st_no int unsigned not null,
		late_st_no int unsigned not null,
		older_seq int unsigned not null,
		younger_seq int unsigned not null,
		base_age float not null,
		top_age float not null,
		color varchar(10),
		reference_no int unsigned not null,
		INDEX (interval_name),
		INDEX (parent_no),
		INDEX (older_seq),
		INDEX (younger_seq),
		INDEX (base_age),
		INDEX (top_age)) Engine=MyISAM");
    
    logMessage(2, "building interval tables");
    
    $count = 0;
    
    $count += $dbh->do("
		INSERT INTO $INTERVAL_MAP_WORK (interval_no, interval_name, base_age, top_age, reference_no)
		SELECT i.interval_no, concat('Early ', i.interval_name),
			il.base_age, il.top_age, i.reference_no
		FROM intervals as i JOIN interval_lookup as il using (interval_no)
		WHERE i.eml_interval = 'Early/Lower'");
    
    $count += $dbh->do("
		INSERT INTO $INTERVAL_MAP_WORK (interval_no, interval_name, base_age, top_age, reference_no)
		SELECT i.interval_no, concat('Late ', i.interval_name),
			il.base_age, il.top_age, i.reference_no
		FROM intervals as i JOIN interval_lookup as il using (interval_no)
		WHERE i.eml_interval = 'Late/Upper'");
    
    $count += $dbh->do("
		INSERT INTO $INTERVAL_MAP_WORK (interval_no, interval_name, base_age, top_age, reference_no)
		SELECT i.interval_no,
			if(i.eml_interval <> '', concat(i.eml_interval, ' ', i.interval_name), i.interval_name),
			il.base_age, il.top_age, i.reference_no
		FROM intervals as i JOIN interval_lookup as il using (interval_no)
		WHERE i.eml_interval not in ('Early/Lower', 'Late/Upper')");
    
    logMessage(2, "    added $count intervals from 'intervals' table");
    
    # Now copy in the additional information from the 'standard_ints' table.
    
    $result = $dbh->do("
		UPDATE $INTERVAL_MAP_WORK as i JOIN standard_ints as s using (interval_no)
		SET i.abbrev = s.abbrev, i.level = s.level, i.parent_no = s.parent_no,
			i.color = s.color, i.base_age = s.base_age, i.top_age = s.top_age,
			i.reference_no = s.reference_no");
    
    # Include any additional intervals that are not already in the "intervals" table.
    
    $count = $dbh->do("INSERT IGNORE INTO $INTERVAL_MAP_WORK (interval_no, interval_name, abbrev, level, parent_no,
				color, base_age, top_age, reference_no)
			SELECT interval_no, interval_name, abbrev, level, parent_no, color, base_age, top_age, reference_no
			FROM standard_ints");
    
    logMessage(2, "    added $count additional intervals from 'standard_ints' table");
    
    # Now sequence the intervals from oldest to youngest.
    
    logMessage(2, "    sequencing interval map");
    
    $result = $dbh->do("SET \@interval_seq := 0");
    $result = $dbh->do("UPDATE $INTERVAL_MAP_WORK
			SET younger_seq = (\@interval_seq := \@interval_seq + 1)
			ORDER BY top_age asc, base_age asc");
    $result = $dbh->do("SET \@interval_seq := 0");
    $result = $dbh->do("UPDATE $INTERVAL_MAP_WORK
			SET older_seq = (\@interval_seq := \@interval_seq + 1)
			ORDER BY base_age desc, top_age desc");
    
    # Now figure out the standard equivalents for non-standard intervals.
    # This involves the creation of two additional tables.
    
    logMessage(2, "    mapping non-standard intervals to standard ones");
    
    $result = $dbh->do("DROP TABLE IF EXISTS intervals_aux");
    
    $result = $dbh->do("
		CREATE TABLE intervals_aux (
			interval_no int unsigned not null,
			level tinyint unsigned not null,
			early_no int unsigned not null,
			late_no int unsigned not null,
			early_age float,
			late_age float) Engine=MyISAM");
    
    $result = $dbh->do("DROP TABLE IF EXISTS intervals_aux_2");
    
    $result = $dbh->do("
		CREATE TABLE intervals_aux_2 (
			interval_no int unsigned primary key,
			early_no int unsigned not null,
			late_no int unsigned not null) Engine=MyISAM");
    
    foreach my $level (1..5)
    {
	$sql = "INSERT INTO intervals_aux (interval_no, level, early_no, late_no, early_age, late_age)
		SELECT i.interval_no, $level, ei.interval_no as early_no, li.interval_no as late_no, ei.base_age, li.top_age
		FROM $INTERVAL_MAP_WORK as i JOIN $INTERVAL_MAP_WORK as li on li.top_age <= i.top_age + 0.1 and li.base_age >= i.top_age + 0.1
			JOIN $INTERVAL_MAP_WORK as ei on ei.base_age >= i.base_age - 0.1 and ei.top_age <= i.base_age - 0.1
		WHERE i.level is null and li.level = $level and ei.level = $level";

	$result = $dbh->do($sql);
    }
    
    $sql = "INSERT IGNORE INTO intervals_aux_2
	    SELECT interval_no, early_no, late_no from intervals_aux
	    ORDER BY (early_age - late_age), level";
    
    $result = $dbh->do($sql);
    
    $sql = "UPDATE $INTERVAL_MAP_WORK as i JOIN intervals_aux_2 as a using (interval_no)
	    SET i.early_st_no = a.early_no, i.late_st_no = a.late_no";
    
    $result = $dbh->do($sql);
    
    $sql = "UPDATE $INTERVAL_MAP_WORK SET early_st_no = interval_no, late_st_no = interval_no
	    WHERE level is not null";
    
    $result = $dbh->do($sql);
    
    # Then create a "container map" which for each possible starting and
    # ending point in the standard interval set determines the most specific
    # containing interval.  This is used to generate a single containing
    # interval for collections and clusters.
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $CONTAINER_MAP_WORK (
		early_seq int unsigned not null,
		late_seq int unsigned not null,
		container_no int unsigned not null,
		PRIMARY KEY (early_seq, late_seq)) Engine=MyISAM");
    
    $sql = "INSERT IGNORE INTO $CONTAINER_MAP_WORK
		SELECT p.early_seq, p.late_seq, i.interval_no
		FROM (SELECT ei.older_seq as early_seq, li.younger_seq as late_seq 
			FROM $INTERVAL_MAP_WORK as ei JOIN $INTERVAL_MAP_WORK as li where ei.level is not null
			    and li.level is not null 
			    and (ei.base_age > li.base_age or ei.top_age > li.top_age)) as p
		    JOIN $INTERVAL_MAP_WORK as i on i.younger_seq <= p.late_seq and i.older_seq <= p.early_seq
		        and i.level is not null
		ORDER BY i.level desc";
    
    $result = $dbh->do($sql);
    
    # Now swap in the new tables.
    
    logMessage(2, "activating tables '$INTERVAL_MAP', '$CONTAINER_MAP'");
    
    # Compute the backup names of all the tables to be activated
    
    my $interval_map_bak = "${INTERVAL_MAP}_bak";
    my $container_map_bak = "${CONTAINER_MAP}_bak";
    
    # Delete any old tables that might have been left around.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $interval_map_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $container_map_bak");
    
    # Do the swap.
    
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $INTERVAL_MAP LIKE $INTERVAL_MAP_WORK");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $CONTAINER_MAP LIKE $CONTAINER_MAP_WORK");
    
    $result = $dbh->do("RENAME TABLE
			    $INTERVAL_MAP to $interval_map_bak,
			    $INTERVAL_MAP_WORK to $INTERVAL_MAP,
			    $CONTAINER_MAP to $container_map_bak,
			    $CONTAINER_MAP_WORK to $CONTAINER_MAP");
    
    # Delete the old tables.
    
    $result = $dbh->do("DROP TABLE IF EXISTS $interval_map_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $container_map_bak");
    
    my $a = 1;		# we can stop here when debugging
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
    
    my $result;
    
    # Determine the names of subordinate tables
    
    my $search_table = $SEARCH_TABLE{$tree_table};
    my $name_table = $NAME_TABLE{$tree_table};
    my $attrs_table = $ATTRS_TABLE{$tree_table};
    my $ints_table = $INTS_TABLE{$tree_table};
    
    logMessage(2, "activating tables '$tree_table', '$search_table', '$name_table', '$attrs_table', '$ints_table' (i)");
    
    # Compute the backup names of all the tables to be activated
    
    my $tree_bak = "${tree_table}_bak";
    my $search_bak = "${search_table}_bak";
    my $name_bak = "${name_table}_bak";
    my $attrs_bak = "${attrs_table}_bak";
    my $ints_bak = "${ints_table}_bak";
    
    # Delete any backup tables that might still be around
    
    $result = $dbh->do("DROP TABLE IF EXISTS $tree_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $search_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $name_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $attrs_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $ints_bak");
    
    # Create dummy versions of any of the main tables that might be currently
    # missing (otherwise the rename will fail; the dummies will be deleted
    # below anyway, after they are renamed to the backup names).
    
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $tree_table LIKE $TREE_WORK");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $search_table LIKE $SEARCH_WORK");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $name_table LIKE $NAME_WORK");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $attrs_table LIKE $ATTRS_WORK");
    $result = $dbh->do("CREATE TABLE IF NOT EXISTS $ints_table LIKE $INTS_WORK");
    
    # Now do the Atomic Table Swap (tm)
    
    $SQL_STRING =	   "RENAME TABLE
			    $tree_table to $tree_bak,
			    $TREE_WORK to $tree_table,
			    $search_table to $search_bak,
			    $SEARCH_WORK to $search_table,
			    $name_table to $name_bak,
			    $NAME_WORK to $name_table,
			    $attrs_table to $attrs_bak,
			    $ATTRS_WORK to $attrs_table,
			    $ints_table to $ints_bak,
			    $INTS_WORK to $ints_table";
    
    $result = $dbh->do($SQL_STRING);
    
    # Then we can get rid of the backup tables
    
    $result = $dbh->do("DROP TABLE IF EXISTS $tree_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $search_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $name_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $attrs_bak");
    $result = $dbh->do("DROP TABLE IF EXISTS $ints_bak");
    
    # Delete the auxiliary tables too, unless we were told to keep them.
    
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


# check ( dbh )
# 
# Check the integrity of $TREE_TABLE.  Return true if the table is okay.  If
# not, return false, write out some error messages, and leave one or more error messages
# in the variable @TREE_ERRORS.

sub check {

    my ($dbh, $tree_table, $options) = @_;
    
    my $result;
    my $options ||= {};
    
    my $auth_table = $AUTH_TABLE{$tree_table};
    my $opinion_cache = $OPINION_CACHE{$tree_table};
    
    # First, set the variables that control log output.
    
    $MSG_TAG = 'Check'; $MSG_LEVEL = $options->{msg_level} || 1;
    
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


# UTILITY ROUTINES

# initMessages ( msg_level )
# 
# Initialize a timer, so that we can tell how long each step takes.  Also set
# the $MSG_LEVEL parameter.  

our ($START_TIME);

sub initMessages {
    
    my ($level) = @_;
    
    $MSG_LEVEL = $level if defined $level;    
    $START_TIME = time;
}


# logMessage ( level, message )
# 
# If $level is greater than or equal to the package variable $MSG_LEVEL, then
# print $message to standard error.

sub logMessage {

    my ($level, $message) = @_;
    
    return if $level > $MSG_LEVEL;
    
    my $elapsed = time - $START_TIME;    
    my $elapsed_str = sprintf("%2dm %2ds", $elapsed / 60, $elapsed % 60);
    
    print STDERR "$MSG_TAG: [ $elapsed_str ]  $message\n";
}


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
