# the following functions were moved into EcologyEntry.pm by JA 5.6.13:
# populateEcologyForm, processEcologyForm, gramsToKg, kgToGrams

package Ecology;

use TaxaCache;
use Debug qw(dbg);
use Constants qw($TAXA_TREE_CACHE $TAXA_LIST_CACHE);

# written by JA 27-31.7,1.8.03

my @fields = ('composition1', 'composition2', 'entire_body', 'body_part', 'adult_length', 'adult_width', 'adult_height', 'adult_area', 'adult_volume', 'thickness', 'architecture', 'form', 'reinforcement', 'folds', 'ribbing', 'spines', 'internal_reinforcement', 'polymorph', 'ontogeny', 'grouping', 'clonal', 'taxon_environment', 'locomotion', 'attached', 'epibiont', 'life_habit', 'depth_habitat', 'diet1', 'diet2', 'vision', 'reproduction', 'asexual', 'brooding', 'dispersal1', 'dispersal2', 'comments','minimum_body_mass','minimum_body_mass_unit','maximum_body_mass','maximum_body_mass_unit','body_mass_comment','body_mass_estimate','body_mass_estimate_unit','body_mass_source','body_mass_type');

sub getEcology {
    my $dbt = shift;
    my $classification_hash = shift;
    my $ecology_fields = shift;
    my $get_basis = shift;
    my $get_preservation = shift;
    my @ecology_fields = @$ecology_fields;
    
    my @taxon_nos = map{int($_)} grep {int($_)} keys(%$classification_hash);
    # This first section gets ecology data for all taxa and parent taxa
    # Dealing with recombinatins/corrections is tricky.  Strategy I use is this: 
    # Store an array of alternative taxon_nos to a taxon in %alt_taxon_nos hash
    # Iterate through this array when getting eco_data for the taxon_no as well. 
    # Synonyms hopefully dealt with as well
    my %all_taxon_nos = (-1=>1); # so we don't crash if there are no taxon nos. 
    my %alt_taxon_nos;
    for my $taxon_no ( @taxon_nos ) {
        if ($taxon_no) {
            $all_taxon_nos{$taxon_no} = 1;
            foreach my $parent (@{$classification_hash->{$taxon_no}}) {
                $all_taxon_nos{$parent->{'taxon_no'}} = 1;
            }
        }
    }

    # Get a list of alternative names of existing taxa as well
    my $sql = "SELECT taxon_no,synonym_no FROM $TAXA_TREE_CACHE WHERE taxon_no != synonym_no AND synonym_no IN (".join(",",keys %all_taxon_nos).")";
    my @results = @{$dbt->getData($sql)};
    foreach my $row (@results) {
        # Synonym_no in this case means senior synonym_no. taxon_no is junior synonym or recombination to add
        push @{$alt_taxon_nos{$row->{'synonym_no'}}},$row->{'taxon_no'};
        $all_taxon_nos{$row->{'taxon_no'}} = 1;
    }
  
    my %taxon_metadata = ();
    if (@ecology_fields) {
        $sql = "SELECT taxon_no,reference_no,".join(", ",@ecology_fields)." FROM ecotaph WHERE taxon_no IN (".join(", ",keys %all_taxon_nos).")";
        dbg("Ecology sql: $sql");
        @results = @{$dbt->getData($sql)};
        foreach my $row (@results) {
            $taxon_metadata{$row->{'taxon_no'}} = $row;
        }
    }

    if ($get_basis || $get_preservation) {
        if (@taxon_nos) {
            $sql = "SELECT taxon_no,taxon_rank,preservation,form_taxon FROM authorities WHERE taxon_no IN (".join(",",keys %all_taxon_nos).")";
            my @results = @{$dbt->getData($sql)};
            foreach my $row (@results) {
                $taxon_metadata{$row->{'taxon_no'}}{'taxon_rank'} = $row->{'taxon_rank'};
                $taxon_metadata{$row->{'taxon_no'}}{'preservation'} = $row->{'preservation'};
                $taxon_metadata{$row->{'taxon_no'}}{'form_taxon'} = $row->{'form_taxon'};
            }
        }
    }


    my @fields = ();

    # Recurse fields are fields whose properties are inheritied from children, not just parents
    my $get_body_mass = 0;
    foreach my $field (@ecology_fields) {
        if ($field =~ /maximum_body_mass|minimum_body_mass|body_mass_estimate/) {
            $get_body_mass = 1;
        } else {
            push @fields, $field;
        }
    }
    if ($get_preservation) {
        push @fields,('preservation','form_taxon');
    }
    
    my %child_taxa;
    if ($get_body_mass) {
        my %all_child_taxon_nos = (-1=>1);
        foreach my $taxon_no (@taxon_nos) {
            if ($taxon_metadata{$taxon_no}{'rank'} =~ /species/) {
                # Optimization: if its species, don't call function, just get the species itself and its alternate spellings
                $child_taxa{$taxon_no} = [$taxon_no,@{$alt_taxon_nos{$taxon_no}}];
            } else {
                my @child_taxon_nos = TaxaCache::getChildren($dbt,$taxon_no);
                $child_taxa{$taxon_no} = \@child_taxon_nos;
            }
            $all_child_taxon_nos{$_} = 1 foreach @{$child_taxa{$taxon_no}}; 
        }
    
        $sql = "SELECT taxon_no,reference_no,minimum_body_mass,maximum_body_mass,body_mass_estimate FROM ecotaph WHERE taxon_no IN (".join(", ",keys %all_child_taxon_nos).")";
        dbg("Ecology recurse sql: $sql");
        @results = @{$dbt->getData($sql)};
        foreach my $row (@results) {
            # do it this way instead of assigning the whole row so as not to obliterate previous entries
            $taxon_metadata{$row->{'taxon_no'}}{'minimum_body_mass'}  = $row->{'minimum_body_mass'};
            $taxon_metadata{$row->{'taxon_no'}}{'maximum_body_mass'}  = $row->{'maximum_body_mass'};
            $taxon_metadata{$row->{'taxon_no'}}{'body_mass_estimate'} = $row->{'body_mass_estimate'};
            $taxon_metadata{$row->{'taxon_no'}}{'reference_no'} = $row->{'reference_no'};
        }
        if ($get_basis) {
            $sql = "SELECT taxon_no,taxon_rank FROM authorities WHERE taxon_no IN (".join(",",keys %all_child_taxon_nos).")";
            my @results = @{$dbt->getData($sql)};
            foreach my $row (@results) {
                $taxon_metadata{$row->{'taxon_no'}}{'taxon_rank'} = $row->{'taxon_rank'};
            }
        }
    }

    # Now we want to crawl upwards in the higherarchy of each taxon, using the first category value
    # we find then stopping. Example: if the taxon has a value for diet1, use that. If it doesn't, and family
    # and order have values, use the family (lower rank'd) value
    my %ecotaph;
    for my $taxon_no (@taxon_nos) {
        my %refs_for_taxon = ();

        # Create a sort of execution plan: start off with recombinations of the taxon, then the taxon itself, then each
        # of its parent, starting with the lowest ranked parent first
        my @exec_order = ();
        push @exec_order, $taxon_no; #taxon/most current combination first
        push @exec_order, @{$alt_taxon_nos{$taxon_no}}; #then syns/recombs
        foreach my $parent (@{$classification_hash->{$taxon_no}}) {
            push @exec_order, $parent->{'taxon_no'}; #ditto as above
            push @exec_order, @{$alt_taxon_nos{$parent->{'taxon_no'}}}; 
        }
        
        my ($seen_diet,$seen_composition,$seen_adult) = (0,0,0);
        foreach my $use_taxon_no (@exec_order) {
            my $taxon = $taxon_metadata{$use_taxon_no};
            foreach my $field (@fields) {
                # never get comments for anything other than the focal taxon
                # maybe a little extreme, but it can save major embarassment
                #  JA 11.1.08
                next if ($field eq "comments" && $use_taxon_no != $exec_order[0]);
                if ($taxon->{$field} && !$ecotaph{$taxon_no}{$field}) {
                    # The following three next's deal with linked fields. We can't mix and match
                    # diet1/diet2 from different family/classes, so if one of them is set, skip messing with the whole group
                    next if ($field =~ /^composition/ && $seen_composition);
                    next if ($field =~ /^diet/ && $seen_diet);
                    next if ($field =~ /^adult_/ && $seen_adult);
                    $ecotaph{$taxon_no}{$field} = $taxon->{$field};
                    # If we want to know what class this data is based off of 
                    # (used in displayCollectionEcology), get that as well
                    $ecotaph{$taxon_no}{$field.'basis'} = $taxon->{'taxon_rank'} if ($get_basis);
                    my $reference_no = $taxon->{'reference_no'};
                    $refs_for_taxon{$reference_no} = 1;
                }
            }
            $seen_composition = 1 if ($taxon->{'composition1'} ||
                                      $taxon->{'composition2'});
            $seen_diet = 1 if ($taxon->{'diet1'} ||
                               $taxon->{'diet2'});
            $seen_adult = 1 if ($taxon->{'adult_length'} ||
                                $taxon->{'adult_width'} ||
                                $taxon->{'adult_height'} ||
                                $taxon->{'adult_area'} ||
                                $taxon->{'adult_volume'});
        }

        # Now get minimum and maximum body mass. Algorithm is:
        # For a given taxon, iterate though all its children.  Out of itself and all its children, use the 
        # minimum and maximum values values for min and max body mass respectively. body_mass_estimate
        # can also count as a minimum or maximum value
        if ($get_body_mass){
            foreach my $use_taxon_no (@{$child_taxa{$taxon_no}}) {
                my $taxon = $taxon_metadata{$use_taxon_no};
                my @values = ($taxon->{'minimum_body_mass'},
                              $taxon->{'maximum_body_mass'},
                              $taxon->{'body_mass_estimate'});
                foreach my $v (@values) {
                    if ($v && 
                          (!exists $ecotaph{$taxon_no}{'minimum_body_mass'} ||
                           $v < $ecotaph{$taxon_no}{'minimum_body_mass'})) {
                        $ecotaph{$taxon_no}{'minimum_body_mass'} = $v;
                        my $reference_no = $taxon->{'reference_no'};
                        $refs_for_taxon{$reference_no} = 1;
                        $ecotaph{$taxon_no}{'minimum_body_mass'.'basis'} = $taxon->{'taxon_rank'} if ($get_basis);
                    }
                    if ($v && 
                           (!exists $ecotaph{$taxon_no}{'maximum_body_mass'} ||
                           $v > $ecotaph{$taxon_no}{'maximum_body_mass'})) {
                        $ecotaph{$taxon_no}{'maximum_body_mass'} = $v;
                        my $reference_no = $taxon->{'reference_no'};
                        $refs_for_taxon{$reference_no} = 1;
                        $ecotaph{$taxon_no}{'maximum_body_mass'.'basis'} = $taxon->{'taxon_rank'} if ($get_basis);
                    }
                    # Note the distinction between exists and empty string ""
                    #  if it doesn't exist, we have no value for it yet, so we're safe to use one
                    #  if it does exist, then replace any value that might be there with a blank string 
                    #       because since we have multiple values, we're now going to use a min -- max range instead
                    # Note that we can have both a range and point estimate if both the min and max equal the point estimate
                    # but as soon as the min and max aren't equal, the point estimate goes away
                    if ($v && ! $ecotaph{$taxon_no}{'body_mass_estimate'}) {
                        $ecotaph{$taxon_no}{'body_mass_estimate'} = $v;
                    } 
                }
            }

            if (exists $ecotaph{$taxon_no}) {
                # if we have a range, then no body mass estimate
                if ($ecotaph{$taxon_no}{'minimum_body_mass'} != $ecotaph{$taxon_no}{'maximum_body_mass'}) {
                    $ecotaph{$taxon_no}{'body_mass_estimate'} = '';
                }
            }
        }
        
        # Get all the references that were used in getting this ecology data
        if (exists $ecotaph{$taxon_no}) {
            my @refs = keys %refs_for_taxon;
            $ecotaph{$taxon_no}{'references'} = \@refs;
        }
    }

    return \%ecotaph;
}


# JA 17.4.12
# creates one ecotaph attribute lookup for all taxa in a group
sub fastEcologyLookup	{
	my ($dbt,$field,$lft,$rgt) = @_;
	my $sql = "(SELECT t.taxon_no,lft,rgt,e.$field FROM $TAXA_TREE_CACHE t,ecotaph e WHERE t.taxon_no=e.taxon_no AND lft>=$lft AND rgt<=$rgt) UNION (SELECT t.taxon_no,lft,rgt,NULL FROM $TAXA_TREE_CACHE t LEFT JOIN ecotaph e ON t.taxon_no=e.taxon_no WHERE lft>=$lft AND rgt<=$rgt AND e.ecotaph_no IS NULL)";
	my @taxa = @{$dbt->getData($sql)};
	my (%lookup,%from,%att);
	for my $t ( @taxa )	{
		if ( $t->{$field} ne "" )	{
			for my $pos ( $t->{lft}..$t->{rgt} )	{
				if ( $att{$pos} eq "" || $from{$pos} < $t->{lft} )	{
					$att{$pos} = $t->{$field};
					$from{$pos} = $t->{lft};
				}
			}
		}
	}
	my $missing;
	for my $t ( @taxa )	{
		$lookup{$t->{taxon_no}} = $att{$t->{lft}};
		if ( ! $lookup{$t->{taxon_no}} )	{
			$missing++;
		}
	}
	# if needed, use a default value taken from the closest scored parent
	#  (or from the passed in taxon itself)
	# unfortunately, the parents are in scrambled order...
	#  JA 14.5.12
	if ( $missing )	{
		$sql = "SELECT e.$field FROM $TAXA_TREE_CACHE t,$TAXA_TREE_CACHE t2,ecotaph e,$TAXA_LIST_CACHE l WHERE t.lft=$lft AND t.taxon_no=child_no AND parent_no=e.taxon_no AND parent_no=t2.taxon_no ORDER BY t2.lft DESC";
		my $value = ${$dbt->getData($sql)}[0]->{$field};
		for my $t ( @taxa )	{
			if ( ! $lookup{$t->{taxon_no}} )	{
				$lookup{$t->{taxon_no}} = $value;
			}
		}
	}
	return \%lookup;
}


1;
