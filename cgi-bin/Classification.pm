package Classification;

use TaxonInfo;

# Travel up the classification tree
# Rewritten 01/11/2004 PS. Function is much more flexible, can do full upward classification of any 
# of the taxon rank's, with caching to keep things fast for large arrays of input data. 
#   * Use a upside-down tree data structure internally
#  Arguments:
#   * 1st arg: comma separated list of the ranks you want,i.e(class,order,family) 
#              OR keyword 'parent' => gets first parent of taxon passed in 
#              OR keyword 'all' => get full classification (not imp'd yet, not used yet);
#   * 2nd arg: Takes an array of taxon names or numbers
#   * 3rd arg: What to return: will either be comma-separated taxon_nos (numbers), taxon_names (names), or a linked_list (linked_list) 
#  Return:
#   * Returns a hash whose key is input (no or name), value is comma-separated lists
#     * Each comma separated list is in the same order as the '$ranks' input variable was in
sub get_classification_hash{
	my $dbt = shift;
	my $ranks = shift;
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
    if (@ranks[0] eq 'parent') {
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
        my ($taxon_no, $taxon_name, $parent_no, $child_no);
       
        # We're using taxon_nos as input
        if ($hash_key =~ /^\d+$/) {
            $taxon_no = $hash_key;
        # We're using taxon_names as input    
        } else {    
            @taxon_nos = TaxonInfo::getTaxonNos($dbt,$hash_key);

            # If the name is ambiguous (multiple authorities entries), taxon_no/child_no are undef so nothing gets set
            if (scalar(@taxon_nos) == 1) {
                $taxon_no = @taxon_nos[0];
            }    
            $taxon_name = $hash_key;
        }
        
        my $orig_child = $taxon_no;
        my $new_name;
        if ($taxon_no) {
            # Get original combination so we can move upward in the tree
            #$taxon_no = TaxonInfo::getOriginalCombination($dbt, $taxon_no);
#            ($new_name, $taxon_no) = TaxonInfo::verify_chosen_taxon('',$taxon_no,$dbt);
            $taxon_no = TaxonInfo::getOriginalCombination($dbt,$taxon_no);
        }

        my $new_child = $taxon_no;
        $child_no = $taxon_no;
        
        my $loopcount = 0;
        my $sql;
        my @parents;

        # start the link with child_no;
        my $link = {};
        my $prev_link = $link;
        $link_head{$hash_key} = $link;
        my %visits = ();
        my $debug_print_link = 0;
        my $found_parent = 0;

        # Loop at least once, but as long as it takes to get full classification
        # Keep looping while we have more levels to go up - the $INIT deal is just a weird way of emulating
        # a do { } while (scalar(@parents)); block.  DON'T use do...while cause 'last' doesn't work in it
        for(my $INIT=0;$INIT==0||scalar(@parents) && !$found_parent;$INIT++) {
            $loopcount++;
            #hasn't been necessary yet, but just in case
            if ($loopcount >= 30) { $msg = "Infinite loop for $child_no?";croak $msg;} 

            # bail if we couldn't find exactly one taxon_no for taxon_name entered above
            last if (!$child_no);
       
            # bail if we have a loop
            $visits{$child_no}++;
            last if ($visits{$child_no} > 1);

            $sql = "SELECT DISTINCT a.taxon_no, a.taxon_name, a.taxon_rank, o.status, o.pubyr, o.reference_no " 
                 . " FROM authorities a,opinions o" 
                 . " WHERE a.taxon_no=o.parent_no" 
                 . " AND o.status NOT IN ('rank corrected as','recombined as','corrected as')" 
                 . " AND o.child_no=".$child_no;

            #use Data::Dumper; print Dumper($sql);
         
            @parents = @{$dbt->getData($sql)};
            if(scalar(@parents)){
                $parent_index=TaxonInfo::selectMostRecentParentOpinion($dbt,\@parents,1);
                #main::dbg("parent idx $parent_index");
                #main::dbg(Dumper($parents[$parent_index]));

                # bail if its a junk nomem * relationship
                last if ($parents[$parent_index]->{'status'} =~ /^nomen/);
                #main::dbg(Dumper($parents[$parent_index]));
 
                # Belongs to should always point to original combination
                $parent_no   = $parents[$parent_index]->{'taxon_no'};
                if ($parent_no) {
                    $correct_name = PBDBUtil::getCorrectedName($dbt,$parent_no);
                    $parent_name = $correct_name->{'taxon_name'};
                    $parent_rank = $correct_name->{'taxon_rank'};
                } else { # This shouldn't happen but might bc of bad opinions
                    $parent_name = $parents[$parent_index]->{'taxon_name'};
                    $parent_rank = $parents[$parent_index]->{'taxon_rank'};
                }
                #main::dbg("parent no $parent_no name $parent_name rank $parent_rank");

                if ($parents[$parent_index]->{'status'} eq 'belongs to' && $ranks[0] eq 'parent') {
                    $found_parent = 1;
                }
                #if ($parents[$parent_index]->{'status'} eq 'recombined as') {
                #    $debug_print_link = 1;
                #    use Data::Dumper; print Dumper($parents[$parent_index]);
                #    print $sql;
                #}

                # bail because we've already climbed up this part o' the tree and its cached
                if (exists $link_cache{$parent_no}) {
                    %{$link} = %{$link_cache{$parent_no}};
                    last;
                # populate this link, then set the link to be the next_link, climbing one up the tree
                } else {
                    # Synonyms are tricky: We don't add the child (junior?) synonym onto the chain, only the parent
                    # Thus the child synonyms get their node values replace by the parent, with the old child data being
                    # placed into a "synonyms" field (an array of nodes)
                    if ($parents[$parent_index]->{'status'} =~ /^(repl|subj|obje)/) {
                        my %node = (
                            'number'=>$prev_link->{'number'},
                            'name'=>$prev_link->{'name'},
                            'rank'=>$prev_link->{'rank'}
                        );
                        $prev_link->{'number'} = $parent_no;
                        $prev_link->{'name'} = $parent_name;
                        $prev_link->{'rank'} = $parent_rank;
                        push @{$prev_link->{'synonyms'}}, \%node;
                    } else {
                        if (exists $rank_hash{$parent_rank} || $ranks[0] eq 'parent' || $ranks[0] eq 'all') {
                            my %node = (
                                'number'=>$parent_no,
                                'name'=>$parent_name,
                                'rank'=>$parent_rank,
                                'next_link'=>{}
                            );
                            %{$link} = %node;
                            $prev_link = $link;
                            $link = $node{'next_link'};
                            $link_cache{$parent_no} = \%node;

                        }
                    }
                }

                # bail if we're already above the rank of 'class'
                last if($ranks[0] ne 'all' && ($parent_rank && $taxon_rank_order{$parent_rank} && 
                        $taxon_rank_order{$parent_rank} < $highest_level));

                # set this var to set up next loop
                $child_no = $parent_no;
            } 
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
                    $list_ordered .= ','.$link->{'name'};
                } else {
                    $list_ordered .= ','.$link->{'number'};
                }
            } else {
                while (%$link) {
                    #if ($count++ > 5) { print "link: ".Dumper($link)."<br>"}
                    #if ($count++ > 12) { last; }
                    # Loop prevention by marking where we've been
                    if (exists $visits{$link->{'number'}}) { 
                        last; 
                    } else {
                        $visits{$link->{'number'}} = 1;
                    }
                    if ($return_type eq 'names') {
                        $list{$link->{'rank'}} = $link->{'name'}; 
                    } else {
                        $list{$link->{'rank'}} = $link->{'number'}; 
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
