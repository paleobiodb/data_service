package Classification;

use TaxonInfo;
use strict;
use Data::Dumper;
use Debug qw(dbg);

my $DEBUG = 0;

# Travel up the classification tree
# Rewritten 01/11/2004 PS. Function is much more flexible, can do full upward classification of any 
# of the taxon rank's, with caching to keep things fast for large arrays of input data. 
#   * Use a upside-down tree data structure internally
#  Arguments:
#   * 0th arg: $dbt object
#   * 1st arg: comma separated list of the ranks you want,i.e(class,order,family) 
#              OR keyword 'parent' => gets first parent of taxon passed in 
#              OR keyword 'all' => get full classification (not imp'd yet, not used yet);
#   * 2nd arg: Takes an array of taxon names or numbers
#   * 3rd arg: What to return: will either be comma-separated taxon_nos (numbers), taxon_names (names), or an ordered array ref hashes (array)(like $dbt->getData returns)
#   * 4th arg: Restrict the search to a certain reference_no.  This is used by the type_taxon part of the Opinions scripts, so
#              an authority can be a type taxon for multiple possible higher taxa (off the same ref).
#  Return:
#   * Returns a hash whose key is input (no or name), value is comma-separated lists
#     * Each comma separated list is in the same order as the '$ranks' (1nd arg)input variable was in
sub get_classification_hash{
	my $dbt = shift;
	my $ranks = shift; #all OR parent OR comma sep'd ranks i.e. 'class,order,family'
	my $taxon_names_or_nos = shift;
	my $return_type = shift || 'names'; #names OR numbers OR array;
    my $restrict_to_reference_no = shift;


	my @taxon_names_or_nos = @{$taxon_names_or_nos};
    $ranks =~ s/\s+//g; #NO whitespace
	my @ranks = split(',', $ranks);
    my %rank_hash = ();
   
    my %link_cache = (); #for speeding up 
    my %link_head = (); #our master upside-down tree. imagine a table of pointers to linked lists, 
                        #except the lists converge into each other as we climb up the hierarchy

	my $highest_level = 21;
	my %taxon_rank_order = ('superkingdom'=>0,'kingdom'=>1,'subkingdom'=>2,'superphylum'=>3,'phylum'=>4,'subphylum'=>5,'superclass'=>6,'class'=>7,'subclass'=>8,'infraclass'=>9,'superorder'=>10,'order'=>11,'suborder'=>12,'infraorder'=>13,'superfamily'=>14,'family'=>15,'subfamily'=>16,'tribe'=>17,'subtribe'=>18,'genus'=>19,'subgenus'=>20,'species'=>21,'subspecies'=>22);
    # this gets the 'min' number, or highest we climb
    if ($ranks[0] eq 'parent') {
        $highest_level = 0;
    } else {
        foreach (@ranks) {
            if ($taxon_rank_order{$_} && $taxon_rank_order{$_} < $highest_level) {
                $highest_level = $taxon_rank_order{$_};
            }
            if ($taxon_rank_order{$_}) {
                $rank_hash{$_} = 1;
            }    
        }
    }

    #dbg("get_classification_hash called");
    #dbg('ranks'.Dumper(@ranks));
    #dbg('highest_level'.$highest_level);
    #dbg('return_type'.$return_type);
    #dbg('taxon names or nos'.Dumper(@taxon_names_or_nos));

    foreach my $hash_key (@taxon_names_or_nos){
        my ($taxon_no, $taxon_name, $parent_no, $child_no, $child_spelling_no);
       
        # We're using taxon_nos as input
        if ($hash_key =~ /^\d+$/) {
            $taxon_no = $hash_key;
        # We're using taxon_names as input    
        } else {    
            my @taxon_nos = TaxonInfo::getTaxonNos($dbt,$hash_key);

            # If the name is ambiguous (multiple authorities entries), taxon_no/child_no are undef so nothing gets set
            if (scalar(@taxon_nos) == 1) {
                $taxon_no = $taxon_nos[0];
            }    
            $taxon_name = $hash_key;
        }
        
        if ($taxon_no) {
            # Get original combination so we can move upward in the tree
            $taxon_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no);
        }

        $child_no = $taxon_no;
        
        my $loopcount = 0;
        my $sql;

        # start the link with child_no;
        my $link = {};
        $link_head{$hash_key} = $link;
        my %visits = ();
        my $found_parent = 0;

        #if ($child_no == 14513) {
        #    $DEBUG = 1;
        #}

        # Bug fix: prevent a senior synonym from being considered a parent
        $child_no = TaxonInfo::getSeniorSynonym($dbt,$child_no,$restrict_to_reference_no);
        # prime the pump 
        my $parent_row = TaxonInfo::getMostRecentClassification($dbt,$child_no,{'reference_no'=>$restrict_to_reference_no});
        if ($DEBUG) { print "Start:".Dumper($parent_row)."<br>"; }
        my $status = $parent_row->{'status'};
        $child_no = $parent_row->{'parent_no'};
        #if ($child_no == 14505) {
        #    $DEBUG = 1;
        #}

        # Loop at least once, but as long as it takes to get full classification
        for(my $i=0;$child_no && !$found_parent;$i++) {
            #hasn't been necessary yet, but just in case
            if ($i >= 100) { my $msg = "Infinite loop for $child_no in get_classification_hash";carp $msg; last;} 

            # bail if we have a loop
            $visits{$child_no}++;
            last if ($visits{$child_no} > 1);

            # A belongs to, rank changed as, corrected as, OR recombined as  - If the previous iterations status
            # was one of these values, then we're found a valid parent (classification wise), so we can terminate
            # at the end of this loop (after we've added the parent into the tree)
            if ($status =~ /^(?:bel|ran|cor|rec)/o && $ranks[0] eq 'parent') {
                $found_parent = 1;
            }

            # Belongs to should always point to original combination
            $parent_row = TaxonInfo::getMostRecentClassification($dbt,$child_no,{'reference_no'=>$restrict_to_reference_no});
            if ($DEBUG) { print "Loop:".Dumper($parent_row)."<br>"; }
       
            # No parent was found. This means we're at end of classification, althought
            # we don't break out of the loop till the end of adding the node since we
            # need to add the current child still
            my ($taxon_rank);
            if ($parent_row) {
                my $taxon= TaxonInfo::getMostRecentSpelling($dbt,$child_no,{'reference_no'=>$restrict_to_reference_no});
                $parent_no  = $parent_row->{'parent_no'};
                $status = $parent_row->{'status'};
                $child_spelling_no = $taxon->{'taxon_no'};
                $taxon_name = $taxon->{'taxon_name'};
                $taxon_rank = $taxon->{'taxon_rank'};
            } else {
                $parent_no=0;
                $child_spelling_no=$child_no;
                my $taxon = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$child_no});
                $taxon_name=$taxon->{'taxon_name'};
                $taxon_rank=$taxon->{'taxon_rank'};
                $status = "";
            }

            # bail because we've already climbed up this part o' the tree and its cached
            if ($parent_no && exists $link_cache{$parent_no}) {
                $link->{'taxon_no'} = $child_no;
                $link->{'taxon_name'} = $taxon_name;
                $link->{'taxon_rank'} = $taxon_rank;
                $link->{'taxon_spelling_no'} = $child_spelling_no;
                $link->{'next_link'} = $link_cache{$parent_no};
                if ($DEBUG) { print "Found cache for $parent_no:".Dumper($link)."<br>";}
                last;
            # populate this link, then set the link to be the next_link, climbing one up the tree
            } else {
                # Synonyms are tricky: We don't add the child (junior?) synonym onto the chain, only the parent
                # Thus the child synonyms get their node values replace by the parent, with the old child data being
                # saved into a "synonyms" field (an array of nodes)
                if ($DEBUG) { print "Traverse $parent_no:".Dumper($link)."<br>";}
                if ($status =~ /^(?:replaced|subjective|objective|invalid)/o) {
                    if ($DEBUG) { print "Synonym node<br>";}
                    my %node = (
                        'taxon_no'=>$child_no,
                        'taxon_name'=>$taxon_name,
                        'taxon_rank'=>$taxon_rank,
                        'taxon_spelling_no'=>$child_spelling_no
                    );
                    push @{$link->{'synonyms'}}, \%node;
                    $link_cache{$child_no} = $link;
                } else {
                    if ($DEBUG) { print "Reg. node<br>";}
                    if (exists $rank_hash{$taxon_rank} || $ranks[0] eq 'parent' || $ranks[0] eq 'all') {
                        my $next_link = {};
                        $link->{'taxon_no'} = $child_no;
                        $link->{'taxon_name'} = $taxon_name;
                        $link->{'taxon_rank'} = $taxon_rank;
                        $link->{'taxon_spelling_no'} = $child_spelling_no;
                        $link->{'next_link'}=$next_link;
                        if ($DEBUG) { print Dumper($link)."<br>";}
                        $link_cache{$child_no} = $link;
                        $link = $next_link;
                    }
                }
            }

            # bail if we've reached the maximum possible rank
            last if($ranks[0] ne 'all' && ($taxon_rank && $taxon_rank_order{$taxon_rank} && 
                    $taxon_rank_order{$taxon_rank} <= $highest_level));

            # end of classification
            last if (!$parent_row);
            
            # bail if its a junk nomem * relationship
            last if ($status =~ /^nomen/);

            # set this var to set up next loop
            $child_no = $parent_no;
        }
    }

    #print "</center><pre>link cache\n".Dumper(\%link_cache);
    #print "\n\nlink head\n".Dumper(\%link_head).'</pre><center>';

    # flatten the linked list before passing it back, either into:
    #  return_type is numbers : comma separated taxon_nos, in order
    #  return_type is names   : comma separated taxon_names, in order
    #  return_type is array   : array reference to array of hashes, in order.  
    while(my ($hash_key, $link) = each(%link_head)) {
        my %list= ();
        my %visits = ();
        my $list_ordered;
        if ($return_type eq 'array') {
            $list_ordered = [];
        } else {
            $list_ordered = '';
        }
        # Flatten out data, but first prepare it all
        if ($ranks[0] eq 'parent') {
            if ($return_type eq 'array') {
                push @$list_ordered,$link;
            } elsif ($return_type eq 'names') {
                $list_ordered .= ','.$link->{'taxon_name'};
            } else {
                $list_ordered .= ','.$link->{'taxon_spelling_no'};
            }
        } else {
            while (%$link) {
                #if ($count++ > 5) { print "link: ".Dumper($link)."<br>"}
                #if ($count++ > 12) { last; }
                # Loop prevention by marking where we've been
                if (exists $visits{$link->{'taxon_no'}}) { 
                    last; 
                } else {
                    $visits{$link->{'taxon_no'}} = 1;
                }
                if ($return_type eq 'array') {
                    push @$list_ordered,$link;
                } elsif ($return_type eq 'names') {
                    $list{$link->{'taxon_rank'}} = $link->{'taxon_name'}; 
                } else {
                    $list{$link->{'taxon_rank'}} = $link->{'taxon_spelling_no'}; 
                }
                my $link_next = $link->{'next_link'};
                #delete $link->{'next_link'}; # delete this to make Data::Dumper output look nice 
                $link = $link_next;
            }
            # The output list will be in the same order as the input list
            # by looping over this array
            if ($return_type ne 'array') {
                foreach my $rank (@ranks) {
                    $list_ordered .= ','.$list{$rank};
                }
            }
        }
        if ($return_type ne 'array') {
            $list_ordered =~ s/^,//g;
        }
        $link_head{$hash_key} = $list_ordered;
    }


    return \%link_head;
}

# Pass in a taxon_no and this function returns all taxa that are  a part of that taxon_no, recursively
# This function isn't meant to be called itself but is a recursive utility function for taxonomic_search
# deprecated, see taxonomic_search. moved here from PBDBUtil.pm
sub new_search_recurse {
    # Start with a taxon_name:
    my $dbt = shift;
    my $passed = shift;
    my $parent_no = shift;
    my $parent_child_spelling_no = shift;
	$passed->{$parent_no} = 1 if ($parent_no);
	$passed->{$parent_child_spelling_no} = 1 if ($parent_child_spelling_no);
    return if (!$parent_no);

    # Get the children. Second bit is for lapsus opinions
    my $sql = "SELECT DISTINCT child_no FROM opinions WHERE parent_no=$parent_no AND child_no != parent_no";
    my @results = @{$dbt->getData($sql)};

    #my $debug_msg = "";
    if(scalar @results > 0){
        # Validate all the children
        foreach my $child (@results){
			# Don't revisit same child. Avoids loops in data structure, and speeds things up
            if (exists $passed->{$child->{'child_no'}}) {
                #print "already visited $child->{child_no}<br>";
                next;    
            }
            # (the taxon_nos in %$passed will always be original combinations since orig. combs always have all the belongs to links)
            my $parent_row = TaxonInfo::getMostRecentClassification($dbt, $child->{'child_no'});

            if($parent_row->{'parent_no'} == $parent_no){
                my $sql = "SELECT DISTINCT child_spelling_no FROM opinions WHERE child_no=$child->{'child_no'}";
                my @results = @{$dbt->getData($sql)}; 
                foreach my $row (@results) {
                    if ($row->{'child_spelling_no'}) {
                        $passed->{$row->{'child_spelling_no'}}=1;
                    }
                }
                $sql = "SELECT DISTINCT parent_spelling_no FROM opinions WHERE child_no=$child->{'child_no'} AND status='misspelling of'";
                @results = @{$dbt->getData($sql)}; 
                foreach my $row (@results) {
                    if ($row->{'parent_spelling_no'}) {
                        $passed->{$row->{'parent_spelling_no'}}=1;
                    }
                }
                undef @results;
                new_search_recurse($dbt,$passed,$child->{'child_no'},$child->{'child_spelling_no'});
            } 
        }
    } 
}

##
# Recursively find all taxon_nos or genus names belonging to a taxon
# deprecated PS 10/10/2005 - use TaxaCache::getChildren instead
##
sub taxonomic_search{
	my $dbt = shift;
	my $taxon_name_or_no = (shift or "");
    my $taxon_no;

    # We need to resolve it to be a taxon_no or we're done    
    if ($taxon_name_or_no =~ /^\d+$/) {
        $taxon_no = $taxon_name_or_no;
    } else {
        my @taxon_nos = TaxonInfo::getTaxonNos($dbt,$taxon_name_or_no);
        if (scalar(@taxon_nos) == 1) {
            $taxon_no = $taxon_nos[0];
        }       
    }
    if (!$taxon_no) {
        return wantarray ? (-1) : "-1"; # bad... ambiguous name or none
    }
    # Make sure its an original combination
    $taxon_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no);

    my $passed = {};
    
    # get alternate spellings of focal taxon. all alternate spellings of
    # children will be found by the new_search_recurse function
    my $sql = "SELECT child_spelling_no FROM opinions WHERE child_no=$taxon_no";
    my @results = @{$dbt->getData($sql)};
    foreach my $row (@results) {
        if ($row->{'child_spelling_no'}) {
            $passed->{$row->{'child_spelling_no'}} = 1;
        }
    }
    $sql = "SELECT DISTINCT parent_spelling_no FROM opinions WHERE child_no=$taxon_no AND status='misspelling of'";
    @results = @{$dbt->getData($sql)}; 
    foreach my $row (@results) {
        if ($row->{'parent_spelling_no'}) {
            $passed->{$row->{'parent_spelling_no'}}=1;
        }
    }

    # get all its children
	new_search_recurse($dbt,$passed,$taxon_no);

    return (wantarray) ? keys(%$passed) : join(', ', keys(%$passed));
}

# Gets the childen of a taxon, sorted/output in various fashions
# Algorithmically, this behaves more or less identically to taxonomic_search,
# except its slower since it can potentially return much more data and is much more flexible
# Data is kept track of internally in a tree format. Additional data is kept track of as well
#  -- Alternate spellings get stored in a "spellings" field
#  -- Synonyms get stored in a "synonyms" field
# Separated 01/19/2004 PS. 
# Moved here from PBDBUtil
#  Inputs:
#   * 1st arg: $dbt
#   * 2nd arg: taxon name or taxon number
#   * 3th arg: what we want the data to look like. possible values are:
#       tree: a tree-like data structure, more general and the format used internally
#       sort_hierarchical: an array sorted in hierarchical fashion, suitable for PrintHierarchy.pm
#       sort_alphabetical: an array sorted in alphabetical fashion, suitable for TaxonInfo.pm or Confidence.pm
#   * 4nd arg: max depth: no of iterations to go down
# 
#  Outputs: an array of hash (record) refs
#    See 'my %new_node = ...' line below for what the hash looks like
sub getChildren {
    my $dbt = shift; 
    my $taxon_no = int(shift);
    my $return_type = (shift || "sort_hierarchical");
    my $max_depth = (shift || 999);
    my $restrict_to_ref = (shift || undef);

    if (!$taxon_no) {
        return undef; # bad... ambiguous name or none
    } 
    
    # described above, return'd vars
    my $orig_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no,$restrict_to_ref);
    my $ss_no = TaxonInfo::getSeniorSynonym($dbt,$orig_no,$restrict_to_ref);
    my $tree_root = createNode($dbt,$ss_no, $restrict_to_ref, 0);

    # The sorted records are sorted in a hierarchical fashion suitable for passing to printHierachy
    my @sorted_records = ();
    getChildrenRecurse($dbt, $tree_root, $max_depth, 1, \@sorted_records, 0, $restrict_to_ref);
    #pop (@sorted_records); # get rid of the head
   
    if ($return_type eq 'tree') {
        return $tree_root;
    } elsif ($return_type eq 'sort_alphabetical') {
        @sorted_records = sort {$a->{'taxon_name'} cmp $b->{'taxon_name'}} @sorted_records;
        return \@sorted_records;
    } else { # default 'sort_hierarchical'
        return \@sorted_records;
    }
   
}

sub getChildrenRecurse { 
    my $dbt = shift;
    my $node = shift;
    my $max_depth = shift;
    my $depth = shift;
    my $sorted_records = shift;
    my $parent_is_synonym = (shift || 0);
    my $restrict_to_ref = (shift || undef);
    
    return if (!$node->{'orig_no'});

    # find all children of this parent, do a join so we can do an order by on it
    my $sql = "SELECT DISTINCT child_no FROM opinions o, authorities a WHERE o.child_spelling_no=a.taxon_no AND o.parent_no=$node->{orig_no} AND o.child_no != o.parent_no ORDER BY a.taxon_name";
    my @children = @{$dbt->getData($sql)};
    
    # Create the children and add them into the children array
    foreach my $row (@children) {
        # (the taxon_nos will always be original combinations since orig. combs always have all the belongs to links)
        # go back up and check each child's parent(s)
        my $orig_no = $row->{'child_no'};
        my $parent_row = TaxonInfo::getMostRecentClassification($dbt,$orig_no,{'reference_no'=>$restrict_to_ref});
        if ($parent_row->{'parent_no'}==$node->{'orig_no'}) {

            # Create the node for the new child - note its taxon_no is always the original combination,
            # but its name/rank are from the corrected name/recombined name
            my $new_node = createNode($dbt, $orig_no,$restrict_to_ref,$depth);
          
            # Populate the new node and place it in its right place
            if ( $parent_row->{'status'} =~ /^(?:belongs)/o ) {
                return if ($max_depth && $depth > $max_depth);
                # Hierarchical sort, in depth first order
                push @$sorted_records, $new_node if (!$parent_is_synonym);
                getChildrenRecurse($dbt,$new_node,$max_depth,$depth+1,$sorted_records,0,$restrict_to_ref);
                push @{$node->{'children'}}, $new_node;
            } elsif ($parent_row->{'status'} =~ /^(?:subjective|objective|replaced|invalid subgroup)/o) {
                getChildrenRecurse($dbt,$new_node,$max_depth,$depth,$sorted_records,1,$restrict_to_ref);
                push @{$node->{'synonyms'}}, $new_node;
            }
        }
    }

    if (0) {
    print "synonyms for $node->{taxon_name}:";
    print "$_->{taxon_name} " for (@{$node->{'synonyms'}}); 
    print "\n<br>";
    print "spellings for $node->{taxon_name}:";
    print "$_->{taxon_name} " for (@{$node->{'spellings'}}); 
    print "\n<br>";
    print "children for $node->{taxon_name}:";
    print "$_->{taxon_name} " for (@{$node->{'children'}}); 
    print "\n<br>";
    }
}

sub createNode {
    my ($dbt,$orig_no,$restrict_to_ref,$depth) = @_;
    my $taxon = TaxonInfo::getMostRecentSpelling($dbt,$orig_no,{'reference_no'=>$restrict_to_ref});
    my $new_node = {'orig_no'=>$orig_no,
                    'taxon_no'=>$taxon->{'taxon_no'},
                    'taxon_name'=>$taxon->{'taxon_name'},
                    'taxon_rank'=>$taxon->{'taxon_rank'},
                    'depth'=>$depth,
                    'children'=>[],
                    'synonyms'=>[]};

    # Get alternate spellings
    my $sql = "(SELECT DISTINCT a.taxon_no, a.taxon_name, a.taxon_rank FROM opinions o, authorities a".
              " WHERE o.child_spelling_no=a.taxon_no".
              " AND o.child_no = $orig_no".
              " AND o.child_spelling_no != $taxon->{taxon_no})".
              " UNION ".
              "(SELECT DISTINCT a.taxon_no, a.taxon_name, a.taxon_rank FROM opinions o, authorities a".
              " WHERE o.parent_spelling_no=a.taxon_no".
              " AND o.child_no = $orig_no".
              " AND o.status='misspelling of')".
              " ORDER BY taxon_name"; 

    $new_node->{spellings} = $dbt->getData($sql);
    return $new_node;
}


1;
