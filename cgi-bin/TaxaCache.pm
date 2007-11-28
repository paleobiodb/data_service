#
# This module build and builds, maintains and accesses two tables (taxa_tree_cache,taxa_list_cache)
# for the purposes of speeding up taxonomic lookups (both of children and parents of a taxon).  The
# taxa_tree_cache holds a modified preorder traversal tree, and the taxa_list_cache holds a 
# adjacency list.  The modified preorder traversal tree is used to trees of children of a taxon
# in constant time, while the adjacency list is used to get parents of a taxon in constant time
# Google these terms for a detailed explanation.
#
# The taxa_list_cache table is very simple and has only two fields: parent_no and child_no. These
# are exactly what they sound like, denoting that a certain child_no is a descent in the taxonomic
# hierarchy of the parent_no.  There has to be one pair for each possible combination (which is a lot)
#
# The taxa_tree_cache has 5 fields: taxon_no (primary_key), lft (left value), rgt (right value), 
# spelling_no (the taxon_no for the most recent spelling of the taxon_no - if there are two or three
# different spellings for a taxon, all of them will have the same spelling_no. All taxa with the
# same lft value should have the same spelling_no, and the taxon_no will equal the spelling_no for
# the most recently used names), and synonym_no (the taxon_no for the most recent spelling of the 
# most senior synonym - this will always be equal to spelling_no except for junior synonyms). The
# spelling_no and synonym_no fields exist for optimation purposes - its now very easy to filter
# out old spellings and junior synonyms.
#
# PS 09/22/2005
#

package TaxaCache;

use Data::Dumper;
use CGI::Carp;
use TaxonInfo;
use Constants qw($TAXA_TREE_CACHE $TAXA_LIST_CACHE $IS_FOSSIL_RECORD);

use strict;

my $DEBUG = 0;

# This function rebuilds the entire cache from scratch, meant to 
# first use, when the cache gets screwed up, or perhaps weekly to be safe
# (at a time when no opinions are likely to be entered, opinions/authorities entered
# concurrently when this is running might be left out)
sub rebuildCache {
    my $dbt = shift;
    my $list_only = shift;
    my $dbh = $dbt->dbh;
    my $result;

    my $sql = "SELECT taxon_no FROM authorities";
    my $sth = $dbh->prepare($sql);
    $sth->execute();

    # Now do the main loop
    my @rows = ();
    while (my $row = $sth->fetchrow_hashref()) {
        push @rows,$row;
    }

    # Save the time. since this whole process takes a while, we'll
    # want to reudate anything that might have changed during the process as the end
    $sql = "SELECT now() t";
    $sth = $dbh->prepare($sql);
    $sth->execute();
    my $time_row = $sth->fetchrow_hashref();
    my $time = $time_row->{'t'};

    if (!$list_only)  {
        # We're going to create a brand new table from scratch, then swap it
        # to be the normal table once its complete
        $result = $dbh->do("DROP TABLE IF EXISTS ${TAXA_TREE_CACHE}_new");
        $result = $dbh->do("CREATE TABLE ${TAXA_TREE_CACHE}_new (taxon_no int(10) unsigned NOT NULL default '0',lft int(10) unsigned NOT NULL default '0',rgt int(10) unsigned NOT NULL default '0', spelling_no int(10) unsigned NOT NULL default '0', synonym_no int(10) unsigned NOT NULL default '0', max_interval_no int(10) unsigned NOT NULL default '0',min_interval_no int(10) unsigned NOT NULL default '0', PRIMARY KEY  (taxon_no), KEY lft (lft), KEY rgt (rgt), KEY synonym_no (synonym_no)) TYPE=MyISAM");

        # Keep track of which nodes we've processed
        my $next_lft = 1;
        my %processed;

        foreach my $row (@rows) {
            if (!$processed{$row->{'taxon_no'}}) {
                my $ancestor_no = TaxonInfo::getOriginalCombination($dbt,$row->{'taxon_no'});
                # Get the topmost ancestor     
                for(my $i=0;$i<100;$i++) { # max out at 100;
                    my $opinion = TaxonInfo::getMostRecentClassification($dbt,$ancestor_no);
                    if ($opinion && $opinion->{'parent_no'}) {
                        $ancestor_no=$opinion->{'parent_no'};
                    } else {
                        last;
                    }
                }
                print "found ancestor $ancestor_no for $row->{taxon_no}<BR>\n" if ($DEBUG);

                # Now insert that topmost ancestor, which will recursively insert all its children as well
                # marking them as processed to boot, so we won't readd them later
                $next_lft = rebuildAddChild($dbt,$ancestor_no,$next_lft,\%processed);
                $next_lft++;
            }
        }
        $result = $dbh->do("RENAME TABLE $TAXA_TREE_CACHE TO ${TAXA_TREE_CACHE}_old, ${TAXA_TREE_CACHE}_new TO $TAXA_TREE_CACHE");
        $result = $dbh->do("DROP TABLE ${TAXA_TREE_CACHE}_old");
        setSyncTime($dbt,$time);
        undef %processed;
    }
    # Now build the taxa_list_cache - just edit it in place
    #$result = $dbh->do("CREATE TABLE $TAXA_LIST_CACHE (parent_no int(10) unsigned NOT NULL default '0',child_no int(10) unsigned NOT NULL default '0', PRIMARY KEY  (child_no,parent_no), KEY parent_no (parent_no)) TYPE=MyISAM");
    my %link_cache = ();
    my %spellings = ();
    my %syns = ();
    my %syn_done = ();
    foreach my $row (@rows) {
        my $orig_no = TaxonInfo::getOriginalCombination($dbt,$row->{'taxon_no'});
        my $child_no = ($orig_no) ? $orig_no : $row->{'taxon_no'};
        my %visits = ();
        for(my $i=0;$child_no;$i++) {
            if ($i > 100) {
                print STDERR "i > 100 for $child_no\n"; last;
            }
            # bail if we've already gotten this hiearchy on a previous run
            last if (exists $link_cache{$child_no});
            # bail if we have a loop due to circular synonyms
            last if ($visits{$child_no});
            $visits{$child_no} = 1; 

            # Belongs to should always point to original combination
            my $parent_row = TaxonInfo::getMostRecentClassification($dbt,$child_no);

            my ($parent_no,$status);
            if ($parent_row) {
                if ($parent_row->{'child_spelling_no'} != $parent_row->{'child_no'}) {
                    $spellings{$parent_row->{'child_no'}} = $parent_row->{'child_spelling_no'};
                }
                $parent_no  = $parent_row->{'parent_no'};
                $status = $parent_row->{'status'};
            } else {
                # No parent was found. This means we're at end of classification, 
                $parent_no=0;
                $status = "";
            }

            if ($status =~ /^(?:replaced|subjective|objective|invalid subgroup)/o) {
                $syns{$child_no} = $parent_no;
            } else {
                $link_cache{$child_no} = $parent_no;
            }
            # Already climbed this part
            last if (exists $link_cache{$parent_no});
            $child_no = $parent_no;
        }
        while (my ($junior,$senior) = each %syns) {
            if (!$syn_done{$junior}) {
                $syn_done{$junior} = 1;
                my $i = 0;
                while ($syns{$senior} && $i < 10) {
                    $senior = $syns{$senior};
                    $i++;
                }
                $link_cache{$junior} = $link_cache{$senior};
            } 
        }
    
        if ($orig_no && $row->{'taxon_no'} != $orig_no) {
            $link_cache{$row->{'taxon_no'}} = $link_cache{$orig_no};
        }

        my $taxon_no = $row->{'taxon_no'};
        my @parents = ();
        %visits = ();
        while ($link_cache{$taxon_no}) {
            last if ($visits{$taxon_no});
            $visits{$taxon_no} = 1; 
            my $next_parent = $link_cache{$taxon_no};
            my $i = 0;
            while ($syns{$next_parent} && $i < 10) {
                $next_parent = $syns{$next_parent};
                $i++;
            }
            my $parent_spelling = ($spellings{$next_parent}) ? $spellings{$next_parent} : $next_parent;
            push @parents, $parent_spelling;
            $taxon_no = $next_parent;
        }
        if (@parents) {
            my $sql_i = "INSERT IGNORE INTO $TAXA_LIST_CACHE (parent_no,child_no) VALUES ";
            foreach my $parent_no (@parents) {
                $sql_i .= "($parent_no,$row->{taxon_no}),";
            }
            $sql_i =~ s/,$//;
            print $sql_i."<BR>\n" if ($DEBUG);
            $dbh->do($sql_i);
            my $sql_d = "DELETE FROM $TAXA_LIST_CACHE WHERE child_no=$row->{taxon_no} AND parent_no NOT IN (".join(",",@parents).")";
            print $sql_d."<BR>\n" if ($DEBUG);
            $dbh->do($sql_d);
        } else {
            my $sql_d = "DELETE FROM $TAXA_LIST_CACHE WHERE child_no=$row->{taxon_no}";
            print $sql_d."<BR>\n" if ($DEBUG);
            $dbh->do($sql_d);
        }
    }
}


sub getSyncTime {
    my $dbt = shift;
    my $sql = "SELECT sync_time FROM tc_sync WHERE sync_id=1";
    my $time = ${$dbt->getData($sql)}[0]->{'sync_time'};
    return $time;
}

sub setSyncTime {
    my ($dbt,$time) = @_;
    my $dbh = $dbt->dbh;
    my $sync_id = 1;
    if ($IS_FOSSIL_RECORD) {
        $sync_id = 2;
    }
    my $sql = "REPLACE INTO tc_sync (sync_id,sync_time) VALUES ($sync_id,'$time')";
    $dbh->do($sql); 
}


# Utility function meant to be used by rebuildCache above only
# Adds a taxon_no into the cache with left value lft.  $processed is a hash reference to
# a hash which keeps track of which taxon_nos have been processed already, so we don't reprocess them
# Note that spelling_no is the taxon_no of the most recent spelling of the SENIOR synonym
sub rebuildAddChild {
    my ($dbt,$taxon_no,$lft,$processed) = @_;
    my $dbh = $dbt->dbh;

    # Loop prevention
    if ($processed->{$taxon_no}) {
        print "Seemed to encounter a loop with $taxon_no, skipping<BR>\n" if ($DEBUG);
        return $lft;
    } else {
        $processed->{$taxon_no} = 1;
    }
    
    # now get recombinations, and corrections for current child and insert at the same 
    # place as the child.  $taxon_no should already be the senior synonym if there are synonyms
    my @all_taxa = TaxonInfo::getAllSpellings($dbt,$taxon_no);
    my %all_hash; $all_hash{$_} = 1 for @all_taxa;

    # get a list of children for the current node. second part to deal with lapsus records
    my $sql = "SELECT DISTINCT o.child_no FROM opinions o WHERE o.parent_no IN (".join(",",@all_taxa).") AND o.child_no != o.parent_no";
    my @results = @{$dbt->getData($sql)};
    my @children = ();
    foreach my $row (@results) {
        next if ($processed->{$row->{'child_no'}});
        my $opinion = TaxonInfo::getMostRecentClassification($dbt,$row->{'child_no'});
        # Note there's no distinction between synonyms and belongs to - both just considered children
        if ($opinion && $all_hash{$opinion->{'parent_no'}}) {
            push @children,$row->{'child_no'};
        }
        # child of X may currently be a synonym of Y, but have more
        #  authoritative belongs to opinions that are used for Y by
        #  getMostRecentClassification, so add Y if is now placed in a
        #  spelling of the focal taxon_no JA 4.8.07
        elsif ( $opinion->{'status'} !~ /belongs|nomen/ && ! $processed->{$opinion->{'parent_no'}} )	{
            my $parentopinion = TaxonInfo::getMostRecentClassification($dbt,$opinion->{'parent_no'});
            if ($parentopinion && $all_hash{$parentopinion->{'parent_no'}}) {
                push @children,$opinion->{'parent_no'};
            }
        }
    }
    print "list of children for $taxon_no: ".join(",",@children)."<BR>\n" if ($DEBUG);

    # Now add all those children
    my $next_lft = $lft + 1;
    my %all_intervals = ();
    foreach my $child_no (@children) {
        $next_lft = rebuildAddChild($dbt,$child_no,$next_lft,$processed);
        $next_lft++;
    }
    my $rgt=$next_lft;

    print "rebuildAddChild: $taxon_no $lft $rgt<BR>\n" if ($DEBUG);

    # Find the name that was last used so we can mark it
    my $spelling = TaxonInfo::getMostRecentSpelling($dbt,$taxon_no);
    my $spelling_no = $spelling->{'taxon_no'};

    my $synonym_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no);
    if ($synonym_no) {
        $synonym_no = TaxonInfo::getSeniorSynonym($dbt,$synonym_no);
    } else {
        $synonym_no = TaxonInfo::getSeniorSynonym($dbt,$taxon_no);
    }
    my $range_op = TaxonInfo::getMostRecentClassification($dbt,$synonym_no,{'strat_range'=>1});
    my ($max_interval_no,$min_interval_no) = (0,0);
    if ($range_op) {
        $max_interval_no = $range_op->{'max_interval_no'};
        $min_interval_no = $range_op->{'min_interval_no'};
        if (!$min_interval_no) {
            $min_interval_no=$max_interval_no;
        }
    }

    my $synonym_spelling = TaxonInfo::getMostRecentSpelling($dbt,$synonym_no);
    $synonym_no = $synonym_spelling->{'taxon_no'};


    # Now insert all the names
    # This is insert ignore instead of insert to to deal with bad records
    $sql = "INSERT IGNORE INTO ${TAXA_TREE_CACHE}_new (taxon_no,lft,rgt,spelling_no,synonym_no,max_interval_no,min_interval_no) VALUES ";
    foreach my $t (@all_taxa) {
        $sql .= "($t,$lft,$rgt,$spelling_no,$synonym_no,$max_interval_no,$min_interval_no),";
        $processed->{$t} = 1;
    }    
    $sql =~ s/,$//;
    print "$sql<BR>\n" if ($DEBUG);
    $dbh->do($sql);

    return $next_lft;
}

# This will add a new taxonomic name to the datbaase that doesn't currently 
# belong anywhere.  Should be called when creating a new authority (Taxon.pm) 
# and Opinion.pm (when creating a new spelling on fly)
sub addName {
    my ($dbt,$taxon_no) = @_;
    my $dbh = $dbt->dbh;
   
    my $sql = "SELECT max(rgt) m FROM $TAXA_TREE_CACHE";
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my $row = $sth->fetchrow_arrayref();
    my $lft = $row->[0] + 1; 
    my $rgt = $row->[0] + 2; 
  
    $sql = "INSERT IGNORE INTO $TAXA_TREE_CACHE (taxon_no,lft,rgt,spelling_no,synonym_no,max_interval_no,min_interval_no) VALUES ($taxon_no,$lft,$rgt,$taxon_no,$taxon_no,0,0)";
    print "Adding name: $taxon_no: $sql<BR>\n" if ($DEBUG);
    $dbh->do($sql); 
    $sql = "SELECT * FROM $TAXA_TREE_CACHE WHERE taxon_no=$taxon_no";
    $row = ${$dbt->getData($sql)}[0];
    
    return $row;
}

# This wil do its best to synchronize the two taxa_cache tables with the opinions table
# This function should be called whenever a new opinion is added into the database, whether
# its from Taxon.pm or Opinion.pm.  Its smart enough not to move stuff around if it doesn't have
# to.  The code is broken into two main sections.  The first section combines any alternate
# spellings that have with the original combination, and the second section deals with the
# the taxon changing parents and thus shifting its left and right values.
# Also add newly entered names with this.  Procedure is addName(taxon_no) .. insert opinion into db .. updateCache(taxon_no);
#
# Arguments:
#   $child_no is the taxon_no of the child to be updated (if necessary)

# IMPORTANT
# this function is only ever called by /../scripts/taxa_cached.pl, which
#  runs continously, so you need to kill it and restart it to debug (JA)
sub updateCache {
    my ($dbt,$child_no) = @_;
    my $dbh=$dbt->dbh;
    return if (!$child_no);
    $child_no = TaxonInfo::getOriginalCombination($dbt,$child_no);

    my $sql;
    my $updateList = 0;

    # This table doesn't have any rows in it - it acts as a mutex so writes to the taxa_tree_cache table
    # will be serialized, which is important to prevent corruption.  Note reads can still happen
    # concurrently with the writes, but any additional writes beyond the first will block on this mutex
    # Don't respect mutexes more than a 2 minutes old, this script shouldn't execute for more than about 15 seconds
# JA: there's a horrible bug somewhere in the system causing taxa_tree_cache
#  to be corrupted, possibly when a user is entering opinions rapidly and
#  (maybe) when the load average is high, so for lack of any better ideas
#  I am pushing up the "respect" period to 20 minutes 28.11.07
    while(1) {
        $dbh->do("LOCK TABLES tc_mutex WRITE");
        my @results = @{$dbt->getData("SELECT * FROM tc_mutex WHERE created > DATE_ADD(NOW(), INTERVAL -20 MINUTE)")};
        if (@results) {
            print "$$: Failed to get lock, trying again in 5 seconds\n" if ($DEBUG);
            $dbh->do("UNLOCK TABLES");
            sleep(5);
        } else {
            print "$$: Got lock\n" if ($DEBUG);
            $dbh->do("DELETE FROM tc_mutex");
            $dbh->do("INSERT INTO tc_mutex (mutex_id,created) VALUES ($$,NOW())");
            $dbh->do("UNLOCK TABLES");
            last;
        }
    }

    # New most recent opinion
    $sql = "SELECT taxon_no,lft,rgt,spelling_no,synonym_no FROM $TAXA_TREE_CACHE WHERE taxon_no=$child_no";
    my $cache_row = ${$dbt->getData($sql)}[0];
    if (!$cache_row) {
        $cache_row = addName($dbt,$child_no);
    }
    my @spellings = TaxonInfo::getAllSpellings($dbt,$child_no);

    # First section: combine any new spellings that have been added into the original combination
#    $sql = "SELECT DISTINCT o.child_spelling_no FROM opinions o WHERE o.child_spelling_no != o.child_no AND o.child_no=$child_no";
#    my @results = @{$dbt->getData($sql)};
    my @upd_rows = ();
    foreach my $spelling_no (@spellings) {
        my $sql = "SELECT taxon_no,lft,rgt,spelling_no,synonym_no FROM $TAXA_TREE_CACHE WHERE taxon_no=$spelling_no";
        my $spelling_row = ${$dbt->getData($sql)}[0];
        if (!$spelling_row) {
            $spelling_row = addName($dbt,$spelling_no);
        }
        
        # If a spelling no hasn't been combined yet, combine it now
        if ($spelling_row->{'lft'} != $cache_row->{'lft'}) {
            # if the alternate spelling had children (not too likely), get a list of them

            if (($spelling_row->{'rgt'} - $spelling_row->{'lft'}) > 2) {
                my $tree = getChildren($dbt,$spelling_no,'tree',1);
                my @children = @{$tree->{'children'}};
                foreach my $child (@children) {
                    moveChildren($dbt,$child->{'taxon_no'},$child_no);
                    #push @upd_rows,$child->{'taxon_no'};
                }
            } 
    
            # Refresh he cache row from the db since it may have been changed above
            $sql = "SELECT taxon_no,lft,rgt,spelling_no,synonym_no FROM $TAXA_TREE_CACHE WHERE taxon_no=$child_no";
            $cache_row = ${$dbt->getData($sql)}[0];
            $sql = "SELECT taxon_no,lft,rgt,spelling_no,synonym_no FROM $TAXA_TREE_CACHE WHERE taxon_no=$spelling_no";
            $spelling_row = ${$dbt->getData($sql)}[0];
           
            # Combine the spellings
            $sql = "UPDATE $TAXA_TREE_CACHE SET lft=$cache_row->{lft},rgt=$cache_row->{rgt} WHERE lft=$spelling_row->{lft}";
            print "Combining spellings $spelling_no with $child_no: $sql\n" if ($DEBUG);
            $dbh->do($sql);
            $updateList = 1;

            # Now after they're combined, make sure the spelling_no and synonym_nos get updated as well
            # For moved children
            $sql = "UPDATE $TAXA_TREE_CACHE SET synonym_no=$cache_row->{synonym_no} WHERE lft=$cache_row->{lft} OR (lft >= $cache_row->{lft} AND rgt <= $cache_row->{rgt} AND synonym_no=$spelling_row->{synonym_no})"; 
            
        }
    }
    # Uncombine children.  This may happen if an enterer adds says X is corrected or recombined as Y by accident, but later
    # changes it TBD

    # New most recent opinion
    my $spelling = TaxonInfo::getMostRecentSpelling($dbt,$child_no);
    my $spelling_no = $spelling->{'taxon_no'};
       
    # Change the most current spelling_no and max and min
    my $range_op = TaxonInfo::getMostRecentClassification($dbt,$child_no,{'strat_range'=>1});
    my ($max_interval_no,$min_interval_no) = (0,0);
    if ($range_op) {
        $max_interval_no = $range_op->{'max_interval_no'};
        $min_interval_no = $range_op->{'min_interval_no'};
        if (!$min_interval_no) {
            $min_interval_no=$max_interval_no;
        }
    }
    if ( ! $max_interval_no )	{
        $max_interval_no = 0;
    }
    if ( ! $min_interval_no )	{
        $min_interval_no = 0;
    }
    $sql = "UPDATE $TAXA_TREE_CACHE SET max_interval_no=$max_interval_no,min_interval_no=$min_interval_no,spelling_no=$spelling_no WHERE lft=$cache_row->{lft}"; 
    print "Updating max,min,spelling with $max_interval_no,$min_interval_no,$spelling_no: $sql\n" if ($DEBUG);
    $dbh->do($sql);

    # Change it so the senior synonym no points to the senior synonym's most correct name
    # for this taxa and any of ITs junior synonyms
    my $senior_synonym_no = TaxonInfo::getSeniorSynonym($dbt,$child_no);
    my $senior_synonym_spelling = TaxonInfo::getMostRecentSpelling($dbt,$senior_synonym_no);
    $senior_synonym_no = $senior_synonym_spelling->{'taxon_no'};
    $sql = "UPDATE $TAXA_TREE_CACHE SET synonym_no=$senior_synonym_no WHERE lft=$cache_row->{lft} OR (lft >= $cache_row->{lft} AND rgt <= $cache_row->{rgt} AND synonym_no=$cache_row->{synonym_no})"; 
    print "Updating synonym with $senior_synonym_no: $sql\n" if ($DEBUG);
    $dbh->do($sql);



    # Second section: Now we check if the parents have been changed by a recent opinion, and only update
    # it if that is the case
    $sql = "SELECT spelling_no parent_no FROM $TAXA_TREE_CACHE WHERE lft < $cache_row->{lft} AND rgt > $cache_row->{rgt} ORDER BY lft DESC LIMIT 1";
    # BUG: may be multiple parents, compare most recent spelling:
    my $row = ${$dbt->getData($sql)}[0];
    my $mrpo = TaxonInfo::getMostRecentClassification($dbt,$child_no);
    my $new_parent_no = ($mrpo && $mrpo->{'parent_no'}) ? $mrpo->{'parent_no'} : 0;
    if ($new_parent_no) {
        # Compare most recent spellings of the names, for consistency
        my $parent_spelling = TaxonInfo::getMostRecentSpelling($dbt,$new_parent_no);
        $new_parent_no = $parent_spelling->{'taxon_no'};
    }
    my $old_parent_no = ($row && $row->{'parent_no'}) ? $row->{'parent_no'} : 0;

    if ($new_parent_no != $old_parent_no) {
        print "Parents have been changed: new parent $new_parent_no: $sql\n" if ($DEBUG);
        
        if ($cache_row) {
            moveChildren($dbt,$cache_row->{'taxon_no'},$new_parent_no);
        } else {
            carp "Missing child_no from $TAXA_TREE_CACHE: child_no: $child_no";
        }
        #updateListCache($dbt,$cache_row->{'taxon_no'});
        $updateList = 1;
    } else {
        print "Parents are the same: new parent $new_parent_no old parent $old_parent_no\n" if ($DEBUG);
    }
#    if ($updateList) {
        updateListCache($dbt,$cache_row->{'taxon_no'});
#    }

    # Unlock tables
    $dbh->do("DELETE FROM tc_mutex");
    print "$$: Released lock\n" if ($DEBUG);
}


# This is a utility function that moves a block of children in the taxa_tree_cache from
# their old parent to their new parent.  We specify the lft and rgt values of the 
# children we want ot move rather than just passing in the child_no to make this function
# a bit more flexible (it can move blocks of children and their descendents instead of 
# just one child).  The general steps are:
#   * Create a new open space where we're going to be moving the children
#   * Add the difference between the old location and new location to the children
#     so all their values get adjusted to be in the new spot
#   * Remove the old "vacuum" where the children used to be
sub moveChildren {
    my ($dbt,$child_no,$parent_no) = @_;
    my $dbh = $dbt->dbh;
    my $sql;
    my $p_row;
    my $c_row;
    if ($parent_no) {
        $sql = "SELECT lft,rgt,spelling_no FROM $TAXA_TREE_CACHE WHERE taxon_no=$parent_no";
        $p_row = ${$dbt->getData($sql)}[0];
        if (!$p_row) {
            $p_row = addName($dbt,$parent_no);
        }
    }

    $sql = "SELECT lft,rgt,spelling_no FROM $TAXA_TREE_CACHE WHERE taxon_no=$child_no";
    $c_row = ${$dbt->getData($sql)}[0];
    if (!$c_row) {
        return;
    }
    my $lft = $c_row->{'lft'};
    my $rgt = $c_row->{'rgt'};

    if ($parent_no && $c_row->{'lft'} == $p_row->{'lft'}) {
        print "moveChildren skipped, child and parent appear to be the same" if ($DEBUG);
    }
    # if PARENT && PARENT.RGT BTWN LFT AND RGT
    # If a loop occurs (the insertion point where we're going to move the child is IN the child itself
    # then we have some special logic: Move to the end so it has no parents, then move the child to 
    # the parent, so we avoid loops
    # this is actually a little more complicated: once you move the parent
    #  to outer space and the child into it, you have to move the parent back,
    #  which you can set off by messing with the modified date of the parent's
    #  most recent parent opinion
    # this does not result in endless looping because getJuniorSynonyms,
    #  getSeniorSynonym, and getMostRecentClassification are all now able
    #  to resolve such conflicts JA 14-15.6.07
    if ($parent_no && $p_row->{'lft'} > $c_row->{'lft'} && $p_row->{'rgt'} < $c_row->{'rgt'}) {
        print "Loop found, moving parent $parent_no to 0\n" if ($DEBUG);
        moveChildren($dbt,$parent_no,0);
        my $popinion = TaxonInfo::getMostRecentClassification($dbt,$parent_no,{'use_synonyms'=>'no'});
        $sql = "UPDATE opinions SET modified=now() WHERE opinion_no=" . $popinion->{'opinion_no'};
        $dbh->do($sql);
        $sql = "SELECT lft,rgt,spelling_no FROM $TAXA_TREE_CACHE WHERE taxon_no=$parent_no";
        $p_row = ${$dbt->getData($sql)}[0];
        if (!$p_row) { return; }
        $sql = "SELECT lft,rgt,spelling_no FROM $TAXA_TREE_CACHE WHERE taxon_no=$child_no";
        $c_row = ${$dbt->getData($sql)}[0];
        if (!$c_row) { return; }
        
        $lft = $c_row->{'lft'};
        $rgt = $c_row->{'rgt'};
        print "End dealing w/loop\n" if ($DEBUG);
    }

    my $child_tree_size = 1+$rgt-$lft;
    print "moveChildren called: child_no $child_no lft $lft rgt $rgt parent $parent_no\n" if ($DEBUG);

    # Find out where we're going to insert the new child. Just add it as the last child of the parent,
    # or put it at the very end if there is no parent
    my $insert_point;
    if ($parent_no) {
        $insert_point = $p_row->{'rgt'};

        # Now add a space at the location of the new nodes will be and
        $sql = "UPDATE $TAXA_TREE_CACHE SET lft=IF(lft >= $insert_point,lft+$child_tree_size,lft),rgt=IF(rgt >= $insert_point,rgt+$child_tree_size,rgt)";
        print "moveChildren: create new spot at $p_row->{rgt}, sql ($sql)\n" if ($DEBUG);
        $dbh->do($sql);
    } else {
        $sql = "SELECT max(rgt) m FROM $TAXA_TREE_CACHE tc_w";
        my $sth = $dbh->prepare($sql);
        $sth->execute();
        my $row = $sth->fetchrow_arrayref();
        $insert_point = $row->[0] + 1;
        print "moveChildren: create spot at end, blank parent, $insert_point\n" if ($DEBUG);
    }

    # The child's lft and rgt values may be been just been adjusted by the update ran above, so
    # adjust accordingly
    my $child_rgt = ($insert_point <= $rgt) ? $rgt + $child_tree_size : $rgt;
    my $child_lft  = ($insert_point <= $lft) ? $lft + $child_tree_size : $lft;
    # Adjust their lft and rgt values accordingly by adding/subtracting the difference between where the
    # children and are where we're moving them
    my $diff = abs($insert_point - $child_lft);
    my $sign = ($insert_point < $child_lft) ? "-" : "+";
    $sql = "UPDATE $TAXA_TREE_CACHE SET lft=lft $sign $diff, rgt=rgt $sign $diff WHERE lft >= $child_lft AND rgt <= $child_rgt";
    print "moveChildren: move to new spot: $sql\n" if ($DEBUG);
    $dbh->do($sql);

    # Now shift everything down into the old space thats now vacant
    # These have to be separate queries
    $sql = "UPDATE $TAXA_TREE_CACHE SET lft=IF(lft > $child_lft,lft-$child_tree_size,lft),rgt=IF(rgt > $child_lft,rgt-$child_tree_size,rgt)"; 
    print "moveChildren: remove old spot: $sql\n" if ($DEBUG);
    $dbh->do($sql);

    # Think about this some more
    # Pass back where we moved them to
    my $new_lft = ($insert_point > $child_lft) ? ($insert_point-$child_tree_size) : $insert_point;
    my $new_rgt = ($insert_point > $child_lft) ? ($insert_point-1) : ($insert_point+$child_tree_size-1);
    return ($new_lft,$new_rgt);
}

# Updates the taxa_list_cache for a range of children getting a list
# of parents of those children, adding them into the db, and deleting
# any old parents that the children might have had
# The senior synonym_no is the senior synonym of the top level children passed
# in - we don't want to mark a senior synonym as a "parent" so we filter those out
sub updateListCache {
    my ($dbt,$taxon_no) = @_; 
    my $dbh = $dbt->dbh;

    # Get the row from the db
    my $sql = "SELECT taxon_no,lft,rgt,spelling_no,synonym_no FROM $TAXA_TREE_CACHE WHERE taxon_no=$taxon_no";
    my $cache_row = ${$dbt->getData($sql)}[0];
    my $senior_synonym_no = $cache_row->{'synonym_no'};
                                                         
    print "updateListCache called taxon_no $taxon_no lft $cache_row->{lft} rgt $cache_row->{rgt}\n" if ($DEBUG);


    # Update all the children of the taxa to have the same parents as the current
    # taxon, in case the classification has changed in some way
    $sql = "SELECT taxon_no FROM $TAXA_TREE_CACHE WHERE lft < $cache_row->{lft} AND rgt > $cache_row->{rgt} AND synonym_no=taxon_no AND synonym_no != $senior_synonym_no";
    my @parents = map {$_->{'taxon_no'}} @{$dbt->getData($sql)};

    $sql = "SELECT taxon_no FROM $TAXA_TREE_CACHE WHERE synonym_no != $cache_row->{synonym_no} AND (lft > $cache_row->{lft} AND lft < $cache_row->{rgt}) AND (rgt > $cache_row->{lft} AND rgt < $cache_row->{rgt})";
    my @children = map {$_->{'taxon_no'}} @{$dbt->getData($sql)};
    
    $sql = "(SELECT taxon_no FROM $TAXA_TREE_CACHE WHERE synonym_no=$cache_row->{synonym_no}) UNION (SELECT taxon_no FROM $TAXA_TREE_CACHE WHERE lft=$cache_row->{lft})";
    my @me = map {$_->{'taxon_no'}} @{$dbt->getData($sql)};

    print "updateListCache children(".join(", ",@children).") parents(".join(", ",@parents).")\n" if ($DEBUG == 2);

    if (@children || @me) {
        my @results = ();
        # Taxon might have been reclassified, so change it up for self and 
        # all its children
        foreach my $child_no (@children,@me) {
            foreach my $parent_no (@parents) {
                push @results, "($parent_no,$child_no)";
            }
        }
        # Update children with MY senior synonym no in case the current axon
        # has just been synonymized/corrected
        foreach my $child_no (@children) {
            push @results, "($senior_synonym_no,$child_no)";
        }

        # Break it up so we don't run into any query size limit 
        # (which should be very large (16 MB) by default, but play it safe)
        while (@results) {
            my @subr = splice(@results,0,5000);
            $sql = "INSERT IGNORE INTO $TAXA_LIST_CACHE (parent_no,child_no) VALUES ".join(",",@subr);
            print "updateListCache insert sql: ".$sql."\n" if ($DEBUG == 2);
            $dbh->do($sql);
        }

        # Since we're updating the trees for a big pile of children potentially, some children can be parents of 
        # other children. Don't delete those links, just delete higher ordered ones. Breaking this up shouldn't be necessary, or possible
        if (@children) {
            $sql = "DELETE FROM $TAXA_LIST_CACHE WHERE child_no IN (".join(",",@children).") AND parent_no NOT IN (".join(",",$senior_synonym_no,@children,@parents).")";
            print "updateListCache: delete1 sql: ".$sql."\n" if ($DEBUG == 2);
            $dbh->do($sql);
        }
        if (@me) {
            $sql = "DELETE FROM $TAXA_LIST_CACHE WHERE child_no IN (".join(",",@me).")";
            if (@parents) { 
                $sql .= " AND parent_no NOT IN (".join(",",@parents).")";
            }
            print "updateListCache: delete2 sql: ".$sql."\n" if ($DEBUG == 2);
            $dbh->do($sql);
        }
    } 
}


# Returns all the descendents of a taxon in various forms.  
#  return_type may be:
#    tree - a sorted tree structure, returns the root note (TREE_NODE datastructure, described below)
#       TREE_NODE is a hash with the following keys:
#       TREE_NODE: hash: { 
#           'taxon_no'=> integer, taxon_no of most current name
#           'taxon_name'=> most current name of taxon
#           'children'=> ref to array of TREE_NODEs
#           'synonyms'=> ref to array of TREE_NODEs
#           'spellings'=> ref to array of TREE_NODEs 
#       }
#    array - *default* - an array of taxon_nos, in no particular order
sub getChildren {
    my $dbt = shift;
    my $taxon_no = int(shift);
    my $return_type = shift;
    # This option exists for updateCache above, nasty bug if this isn't set since senior synonyms
    # children will be moved to junior synonym if we do the resolution!
    my $dont_resolve_senior_syn = shift;
    my $exclude_list = shift;
    
    return undef unless $taxon_no;

    # First get the senior synonym
    unless ($dont_resolve_senior_syn) {
        my $ss = getSeniorSynonym($dbt,$taxon_no);
        if ($ss) {
            $taxon_no = $ss->{'taxon_no'};
        }
    }

    my $sql = "SELECT lft,rgt,synonym_no FROM $TAXA_TREE_CACHE WHERE taxon_no=$taxon_no";
    my $root_vals = ${$dbt->getData($sql)}[0];
    return undef unless $root_vals;
    my $lft = $root_vals->{'lft'};
    my $rgt = $root_vals->{'rgt'};
    my $synonym_no = $root_vals->{'synonym_no'};
    
    my @exclude = ();
    if (ref $exclude_list eq 'ARRAY' && @$exclude_list) {
        my $excluded = join(",",map {int} @$exclude_list);
        my $sql = "SELECT lft,rgt FROM $TAXA_TREE_CACHE WHERE taxon_no IN ($excluded)";
        foreach my $row (@{$dbt->getData($sql)}) {
            if ($row->{'lft'} > $lft && $row->{'rgt'} < $rgt) {
                push @exclude, [$row->{'lft'},$row->{'rgt'}];
            }
        }
    }

    if ($return_type eq 'tree' || $return_type eq 'immediate_children') {
        my $child_nos;
        if ($return_type eq 'immediate_children') {
            my $sql = "SELECT taxon_no FROM $TAXA_TREE_CACHE WHERE synonym_no=$synonym_no";
            my $synonym_nos = join(",",-1,map {$_->{'taxon_no'}} @{$dbt->getData($sql)});
            $sql = "SELECT DISTINCT child_no,child_spelling_no FROM opinions WHERE parent_no IN ($synonym_nos)";
            $child_nos = join(",",-1,map {($_->{'child_no'},$_->{'child_spelling_no'})} @{$dbt->getData($sql)});
        }
        # Ordering is very important. 
        # The ORDER BY tc2.lft makes sure results are returned in hieracharical order, so we can build the tree in one pass below
        # The (tc2.taxon_no != tc2.spelling_no) term ensures the most recent name always comes first (this simplfies later algorithm)
        # use between and both values so we'll use a key for a smaller tree;
        my $sql = "SELECT tc.taxon_no, a1.type_taxon_no, a1.taxon_rank, a1.taxon_name, tc.spelling_no, tc.lft, tc.rgt, tc.synonym_no "
                . " FROM $TAXA_TREE_CACHE tc, authorities a1"
                . " WHERE a1.taxon_no=tc.taxon_no"
                . " AND (tc.lft BETWEEN $lft AND $rgt)"
                . " AND (tc.rgt BETWEEN $lft AND $rgt)";
        foreach my $exclude (@exclude) {
            $sql .= " AND (tc.lft NOT BETWEEN $exclude->[0] AND $exclude->[1])";
            $sql .= " AND (tc.rgt NOT BETWEEN $exclude->[0] AND $exclude->[1])";
        }
        if ($return_type eq 'immediate_children') {
             $sql .= " AND tc.synonym_no IN ($synonym_no,$child_nos)"
        }
        $sql .= " ORDER BY tc.lft, (tc.taxon_no != tc.spelling_no)";
        my @results = @{$dbt->getData($sql)};

        my $root = shift @results;
        $root->{'children'}  = [];
        $root->{'synonyms'}  = [];
        $root->{'spellings'} = [];
        my @parents = ($root);
        my %p_lookup = ($root->{taxon_no}=>$root);
        foreach my $row (@results) {
            if (!@parents) {
                last;
            }
            my $p = $parents[0];

            if ($row->{synonym_no} == $row->{taxon_no}) {
                $p_lookup{$row->{taxon_no}} = $row;
            }

            if ($row->{'lft'} == $p->{'lft'}) {
                # This is a correction/recombination/rank change
                push @{$p->{'spellings'}},$row;
#                print "New spelling of parent $p->{taxon_name}: $row->{taxon_name}\n";
            } else {
                $row->{'children'}  = [];
                $row->{'synonyms'}  = [];
                $row->{'spellings'} = [];

                while ($row->{'rgt'} > $p->{'rgt'}) {
                    shift @parents;
                    last if (!@parents);
                    $p = $parents[0];
                }
                if ($row->{'synonym_no'} != $row->{'spelling_no'}) {
                    my $ss = $p_lookup{$row->{synonym_no}};
                    push @{$ss->{synonyms}}, $row;

                    #push @{$p->{'synonyms'}},$row;
#                    print "New synonym of parent $p->{taxon_name}: $row->{taxon_name}\n";
                } else {
                    push @{$p->{'children'}},$row;
#                    print "New child of parent $p->{taxon_name}: $row->{taxon_name}\n";
                }
                unshift @parents, $row;
            }
        }

        # Now go through and sort stuff in tree
        my @nodes_to_sort = ($root);
        while(@nodes_to_sort) {
            my $node = shift @nodes_to_sort;
            my @children = sort {$a->{'taxon_name'} cmp $b->{'taxon_name'}} @{$node->{'children'}};
            $node->{'children'} = \@children;
            unshift @nodes_to_sort,@children;
        }
        if ($return_type eq 'immediate_children') {
            my @all_children = ();
            push @all_children, @{$root->{children}};
            foreach my $row (@{$root->{synonyms}}) {
                push @all_children, @{$row->{'children'}};
            }
            return \@all_children;
        } else {
            return $root;
        }
    } else {
        # use between and both values so we'll use a key for a smaller tree;
        my $sql = "SELECT tc.taxon_no FROM $TAXA_TREE_CACHE tc WHERE "
                . "tc.lft BETWEEN $lft AND $rgt "
                . "AND tc.rgt BETWEEN $lft AND $rgt";  
        foreach my $exclude (@exclude) {
            $sql .= " AND (tc.lft NOT BETWEEN $exclude->[0] AND $exclude->[1])";
            $sql .= " AND (tc.rgt NOT BETWEEN $exclude->[0] AND $exclude->[1])";
        }
        #my $sql = "SELECT l.child_no FROM $TAXA_LIST_CACHE l WHERE l.parent_no=$taxon_no";
        my @taxon_nos = map {$_->{'taxon_no'}} @{$dbt->getData($sql)};
        return @taxon_nos;
    }
}

# Returns an ordered array of ancestors for a given taxon_no. Doesn't return synonyms of those ancestors 
#  b/c that functionality not needed anywhere
# return type may be:
#   array_full - an array of hashrefs, in order by lowest to highest class. Hash ref has following keys:
#       taxon_no (integer), taxon_name (string), spellings (arrayref to array of same) synonyms (arrayref to array of same)
#   array - *default* - an array of taxon_nos, in order from lowest to higher class
#   rank xxxx - returns taxon resolved to a specific rank
sub getParents {
    my ($dbt,$taxon_nos_ref,$return_type) = @_;

    my %hash = ();
    my $rank;
    if ($return_type =~ /rank (\w+)/) {
        $rank = $dbt->dbh->quote($1);
    }
    foreach my $taxon_no (@$taxon_nos_ref) {
        if ($rank) {
            my $sql = "SELECT a.taxon_no,a.taxon_name,a.taxon_rank FROM $TAXA_LIST_CACHE l, $TAXA_TREE_CACHE t, authorities a WHERE t.taxon_no=l.parent_no AND a.taxon_no=l.parent_no AND l.child_no=$taxon_no AND a.taxon_rank=$rank ORDER BY t.lft DESC";
            my @results = @{$dbt->getData($sql)};
            $hash{$taxon_no} = $results[0];
        } elsif ($return_type eq 'array_full') {
            my $sql = "SELECT a.taxon_no,a.taxon_name,a.taxon_rank,a.common_name, IF (a.pubyr IS NOT NULL AND a.pubyr != '' AND a.pubyr != '0000', a.pubyr, IF (a.ref_is_authority='YES', r.pubyr, '')) pubyr FROM $TAXA_LIST_CACHE l, $TAXA_TREE_CACHE t, authorities a,refs r WHERE t.taxon_no=l.parent_no AND a.taxon_no=l.parent_no AND l.child_no=$taxon_no AND a.reference_no=r.reference_no ORDER BY t.lft DESC";
            $hash{$taxon_no} = $dbt->getData($sql);
        } else {
            my $sql = "SELECT l.parent_no FROM $TAXA_LIST_CACHE l, $TAXA_TREE_CACHE t WHERE t.taxon_no=l.parent_no AND l.child_no=$taxon_no ORDER BY t.lft DESC";
            my @taxon_nos = map {$_->{'parent_no'}} @{$dbt->getData($sql)};
            $hash{$taxon_no} = \@taxon_nos;
        }
    }
    return \%hash;
}

# Simplified version of the above function which just returns the most senior name of the most immediate
# parent, as a hashref
sub getParent {
    my $dbt = shift;
    my $taxon_no = shift;
    my $taxon_rank = shift;

    my $rank_sql = ($taxon_rank) ? "AND a.taxon_rank=".$dbt->dbh->quote($taxon_rank) : "";
    my $sql = "SELECT a.taxon_no,a.taxon_name,a.taxon_rank FROM $TAXA_LIST_CACHE l, $TAXA_TREE_CACHE t, authorities a WHERE t.taxon_no=l.parent_no AND a.taxon_no=l.parent_no AND l.child_no=$taxon_no $rank_sql ORDER BY t.lft DESC LIMIT 1";
    return ${$dbt->getData($sql)}[0];
}

# made from a copy of the above JA 23.8.07
# returns a hash where keys are the taxon nos that have been passed in,
#   and values are parent nos at the specified rank
sub getParentHash {
    my $dbt = shift;
    my $taxon_nos_ref = shift;
    my $taxon_rank = shift;

    my $sql = "SELECT l.child_no,a.taxon_no FROM $TAXA_LIST_CACHE l, authorities a WHERE a.taxon_no=l.parent_no AND l.child_no IN (" . join(',',@$taxon_nos_ref) . ") AND a.taxon_rank='" . $taxon_rank . "'";
    my @parent_rows = @{$dbt->getData($sql)};
    my %parent_hash;
    for my $p ( @parent_rows )	{
        $parent_hash{$p->{'child_no'}} =  $p->{'taxon_no'};
    }

    return %parent_hash;
}

sub getSeniorSynonym {
    my $dbt = shift;
    my $taxon_no = shift;
    my $sql = "SELECT a.taxon_no, a.taxon_name, a.taxon_rank"
            . " FROM $TAXA_TREE_CACHE t, authorities a"
            . " WHERE t.synonym_no=a.taxon_no"
            . " AND t.taxon_no=$taxon_no";


    my @results = @{$dbt->getData($sql)};
    if (@results) {
        return $results[0];
    }
}

sub getMetaData {
    my ($dbt,$taxon_no,$spelling_no,$synonym_no) = @_;
    my $orig_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no);
    my $last_op = TaxonInfo::getMostRecentClassification($dbt,$orig_no);

    my $invalid_reason = 'valid';
    my $nomen_parent_no = 0;
    if ($synonym_no != $spelling_no || ($last_op && $last_op->{'status'} !~ /belongs to/)) {
        if ($last_op->{'status'} =~ /nomen/) {
            my $last_parent_op = TaxonInfo::getMostRecentClassification($dbt,$orig_no,{'exclude_nomen'=>1}); 
            $nomen_parent_no = $last_parent_op->{'parent_no'} || "0";
        } 
        $invalid_reason = $last_op->{'status'};
    } elsif ($taxon_no != $spelling_no) {
        $invalid_reason = $last_op->{'spelling_reason'};
    }
    return ($invalid_reason,$nomen_parent_no);
}


1;
