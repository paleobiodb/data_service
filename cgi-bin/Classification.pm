package Classification;

use TaxonInfo;
use Data::Dumper;

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
#   * 3rd arg: What to return: will either be comma-separated taxon_nos (numbers), taxon_names (names), or a linked_list (linked_list) 
#  Return:
#   * Returns a hash whose key is input (no or name), value is comma-separated lists
#     * Each comma separated list is in the same order as the '$ranks' (1nd arg)input variable was in
sub get_classification_hash{
	my $dbt = shift;
	my $ranks = shift; #all OR parent OR comma sep'd ranks i.e. 'class,order,family'
	my $taxon_names_or_nos = shift;
	my $return_type = shift || 'names'; #names OR numbers OR linked_list;

	my @taxon_names_or_nos = @{$taxon_names_or_nos};
    $ranks =~ s/\s+//g; #NO whitespace
	my @ranks = split(',', $ranks);
    %rank_hash = ();
   
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

    #main::dbg("get_classification_hash called");
    #main::dbg('ranks'.Dumper(@ranks));
    #main::dbg('highest_level'.$highest_level);
    #main::dbg('return_type'.$return_type);
    #main::dbg('taxon names or nos'.Dumper(@taxon_names_or_nos));

    foreach my $hash_key (@taxon_names_or_nos){
        my ($taxon_no, $taxon_name, $parent_no, $child_no, $child_spelling_no);
       
        # We're using taxon_nos as input
        if ($hash_key =~ /^\d+$/) {
            $taxon_no = $hash_key;
        # We're using taxon_names as input    
        } else {    
            @taxon_nos = TaxonInfo::getTaxonNos($dbt,$hash_key);

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
        my $prev_link = $link;
        $link_head{$hash_key} = $link;
        my %visits = ();
        my $debug_print_link = 0;
        my $found_parent = 0;

        # prime the pump 
        my $parent_row = TaxonInfo::getMostRecentParentOpinion($dbt,$child_no);
        my $status = $parent_row->{'status'};
        $child_no = $parent_row->{'parent_no'};

        # Loop at least once, but as long as it takes to get full classification
        for(my $i=0;$child_no && !$found_parent;$i++) {
            #hasn't been necessary yet, but just in case
            if ($i >= 30) { $msg = "Infinite loop for $child_no in get_classification_hash";carp $msg; last;} 

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
            $parent_row = TaxonInfo::getMostRecentParentOpinion($dbt,$child_no,1);
       
            # No parent was found. This means we're at end of classification, althought
            # we don't break out of the loop till the end of adding the node since we
            # need to add the current child still
            if ($parent_row) {
                $parent_no  = $parent_row->{'parent_no'};
                $child_spelling_no= $parent_row->{'child_spelling_no'};
                $taxon_name = $parent_row->{'child_name'};
                $taxon_rank = $parent_row->{'child_rank'};
                $status = $parent_row->{'status'};
            } else {
                $parent_no=0;
                $child_spelling_no=$child_no;
                my @results = TaxonInfo::getTaxon($dbt,'taxon_no'=>$child_no);
                $taxon_name=$results[0]->{'taxon_name'};
                $taxon_rank=$results[0]->{'taxon_rank'};
                $status = "";
            }

            # bail because we've already climbed up this part o' the tree and its cached
            if (exists $link_cache{$parent_no}) {
                    #print "Found cache";
                %{$link} = %{$link_cache{$parent_no}};
                last;
            # populate this link, then set the link to be the next_link, climbing one up the tree
            } else {
                # Synonyms are tricky: We don't add the child (junior?) synonym onto the chain, only the parent
                # Thus the child synonyms get their node values replace by the parent, with the old child data being
                # placed into a "synonyms" field (an array of nodes)
                if ($status =~ /^(?:repl|subj|obje)/o) {
                    my %node = (
                        'child_no'=>$prev_link->{'child_no'},
                        'parent_no'=>$prev_link->{'parent_no'},
                        'taxon_name'=>$prev_link->{'taxon_name'},
                        'taxon_rank'=>$prev_link->{'taxon_rank'},
                        'child_spelling_no'=>$prev_link->{'child_spelling_no'}
                    );
                    $prev_link->{'child_no'} = $child_no;
                    $prev_link->{'parent_no'} = $parent_no;
                    $prev_link->{'taxon_name'} = $taxon_name;
                    $prev_link->{'taxon_rank'} = $taxon_rank;
                    $prev_link->{'child_spelling_no'} = $child_spelling_no;
                    push @{$prev_link->{'synonyms'}}, \%node;
                } else {
                    if (exists $rank_hash{$taxon_rank} || $ranks[0] eq 'parent' || $ranks[0] eq 'all') {
                        my %node = (
                            'child_no'=>$child_no,
                            'parent_no'=>$parent_no,
                            'taxon_name'=>$taxon_name,
                            'taxon_rank'=>$taxon_rank,
                            'child_spelling_no' => $child_spelling_no,
                            'next_link'=>{}
                        );
                        %{$link} = %node;
                        $prev_link = $link;
                        $link = $node{'next_link'};
                        $link_cache{$parent_no} = \%node;

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
        ##if ($debug_print_link) {
        #    use Data::Dumper; print "</center>orig child $orig_child new child $new_child<pre>".Dumper($link_head{$hash_key}).'</pre><center>';
        #}
    }

    #print "</center><pre>link cache\n".Dumper(\%link_cache);
    #print "\n\nlink head\n".Dumper(\%link_head).'</pre><center>';


    # flatten the links before passing it back
    if ($return_type ne 'linked_list') {
        while(($hash_key, $link) = each(%link_head)) {
            my %list= ();
            my %visits = ();
            my $list_ordered = '';
            # Flatten out data, but first prepare it all
            if ($ranks[0] eq 'parent') {
                if ($return_type eq 'names') {
                    $list_ordered .= ','.$link->{'taxon_name'};
                } else {
                    $list_ordered .= ','.$link->{'child_spelling_no'};
                }
            } else {
                while (%$link) {
                    #if ($count++ > 5) { print "link: ".Dumper($link)."<br>"}
                    #if ($count++ > 12) { last; }
                    # Loop prevention by marking where we've been
                    if (exists $visits{$link->{'parent_no'}}) { 
                        last; 
                    } else {
                        $visits{$link->{'parent_no'}} = 1;
                    }
                    if ($return_type eq 'names') {
                        $list{$link->{'taxon_rank'}} = $link->{'taxon_name'}; 
                    } else {
                        $list{$link->{'taxon_rank'}} = $link->{'child_spelling_no'}; 
                    }
                    $link = $link->{'next_link'};
                }
                # The output list will be in the same order as the input list
                # by looping over this array
                foreach $rank (@ranks) {
                    $list_ordered .= ','.$list{$rank};
                }
            }
            $list_ordered =~ s/^,//g;
            $link_head{$hash_key} = $list_ordered;
        }
    }

    return \%link_head;
}

1;
