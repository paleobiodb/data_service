package Classification;

use TaxonInfo;

# Does a sort of wierd reverse tree (from nodes up) using hashes and hash refs.
sub get_classification_hash{
	my $dbt = shift;
	my $levels = shift;
    my $taxon_names = shift;
	my @taxon_names = @{$taxon_names};
	my @levels = split(',', $levels);
	my $highest_level = $levels[-1];

	my %taxon_rank_order = ('superkingdom'=>0,'kingdom'=>1,'subkingdom'=>2,'superphylum'=>3,'phylum'=>4,'subphylum'=>5,'superclass'=>6,'class'=>7,'subclass'=>8,'infraclass'=>9,'superorder'=>10,'order'=>11,'suborder'=>12,'infraorder'=>13,'superfamily'=>14,'family'=>15,'subfamily'=>16,'tribe'=>17,'subtribe'=>18,'genus'=>19,'subgenus'=>20,'species'=>21,'subspecies'=>22);

    my %family = ();
    my %order = ();
    my %class = ();
    my %full_class = ();

  foreach my $item (@taxon_names){
	# Rank will start as genus
	my $rank = "";
	my $taxon_name = $item;
	$full_class{$item} = "";
    my $child_no = -1;
    my $parent_no = -1;
    my %parent_no_visits = ();
    my %child_no_visits = ();

    my $status = "";
    my $first_time = 1;
    # Loop at least once, but as long as it takes to get full classification
    while($parent_no){
        # Keep $child_no at -1 if no results are returned.
        my $sql = "SELECT taxon_no, taxon_rank FROM authorities WHERE ".
                  "taxon_name='$taxon_name'";
		if($rank){
			$sql .= " AND taxon_rank = '$rank'";
		}
        my @results = @{$dbt->getData($sql)};
        if(defined $results[0]){
            # Save the taxon_no for keying into the opinions table.
            $child_no = $results[0]->{taxon_no};

            # Insurance for self referential / bad data in database.
            # NOTE: can't use the tertiary operator with hashes...
            # How strange...
            if(exists $child_no_visits{$child_no}){
                $child_no_visits{$child_no} += 1;
            }
            else{
                $child_no_visits{$child_no} = 1;
            }
            last if($child_no_visits{$child_no}>1);
			# bail if we're already above the rank of 'class'
			last if($results[0]->{taxon_rank} && $taxon_rank_order{$results[0]->{taxon_rank}} < $taxon_rank_order{$highest_level});
        }
        # no taxon number: give up.
        else{
			last;
        }

        # Now see if the opinions table has a parent for this child
        my $sql_opin =  "SELECT status, parent_no, pubyr, reference_no ".
                        "FROM opinions ".
                        "WHERE child_no=$child_no AND status='belongs to'";
        @results = @{$dbt->getData($sql_opin)};

        $first_time = 0;

        if(scalar @results){
            $parent_no=TaxonInfo::selectMostRecentParentOpinion($dbt,\@results);
                
            # Insurance for self referential or otherwise bad data in database.
            if($parent_no_visits{$parent_no}){
                $parent_no_visits{$parent_no} += 1;
            }       
            else{
                $parent_no_visits{$parent_no}=1;
            }           
            last if($parent_no_visits{$parent_no}>1);
                    
            if($parent_no){
                # Get the name and rank for the parent
                my $sql_auth = "SELECT taxon_name, taxon_rank ".
                           "FROM authorities ".
                           "WHERE taxon_no=$parent_no";
                @results = @{$dbt->getData($sql_auth)};
                if(scalar @results){
                    $auth_hash_ref = $results[0];
                    # reset name and rank for next loop pass
                    $rank = $auth_hash_ref->{"taxon_rank"};
                    $taxon_name = $auth_hash_ref->{"taxon_name"};
					# Quit if we're already at 'class' rank level or higher.
					last if($results[0]->{taxon_rank} && $taxon_rank_order{$results[0]->{taxon_rank}} < $taxon_rank_order{$highest_level});
					if($rank eq "family"){
						unless(exists $family{$taxon_name}){
							$family{$taxon_name} = $taxon_name;
						}
						$full_class{$item} = \$family{$taxon_name};
						# Skip ahead if we've already got the hierarchy
						# $order_ref will either be a scalar or a hash ref.
						if(UNIVERSAL::isa($full_class{$item},"HASH")){
							my ($family, $order_ref)=each %{$full_class{$item}};
							# If the $order_ref is a hash, we've got the whole
							# hierarchy.
							if(UNIVERSAL::isa($order_ref,"HASH") && $full_class{$item}->{$family}->{${$order_ref}} ne ""){
								last;	
							}
						}
					}
					elsif($rank eq "order"){
						unless(exists $order{$taxon_name}){
							$order{$taxon_name} = $taxon_name;
						}
						else{ 
						}
						# if we found an order, the family must already be known
						my $family = $full_class{$item};
						if(UNIVERSAL::isa($family,"SCALAR")){
							$full_class{$item} = {};
							$full_class{$item}->{${$family}} =\$order{$taxon_name};
						}
						elsif(UNIVERSAL::isa($family, "HASH")){
							my ($fam_str, $order_ref)=each %{$full_class{$item}};
                            # If the $order_ref is a hash, we've got the whole
                            # hierarchy.
                            if(UNIVERSAL::isa($order_ref,"HASH")){
								my ($order_str, $class_ref)=each %{$order_ref};
								if(UNIVERSAL::isa($class_ref, "HASH")){
									last;   
								}
                            }
						}
					}
					elsif($rank eq "class"){
						unless(exists $class{$taxon_name}){
							$class{$taxon_name} = $taxon_name;
							#print "added CLASS $taxon_name\n";
						}
						else{ 
							#print "CLASS $taxon_name already stashed\n";
						}
						# if we found a class, the family and order must 
						# already be known
						# $full_class{$item} is a hash lookup whose value is
						# a hash reference from the %family hash.  Dereferencing
						# that hash gives me the family key and the value,
						# which should be a hash reference from the %order
						# hash. If the %order reference's value is a scalar,
						# we just have the order key=value, so we put in a ref
						# to a %class hash (so we know if we've got an order
						# scalar or a class scalar.
						my ($family, $order_ref) = each %{$full_class{$item}};
						if(UNIVERSAL::isa($order_ref,"SCALAR")){
							my $order = $full_class{$item}->{$family};
							$full_class{$item}->{$family} = {};
							$full_class{$item}->{$family}->{${$order}} = \$class{$taxon_name};
						}
					}
                }       
                else{   
                    # No results might not be an error: 
                    # it might just be lack of data
                    # print "ERROR in sql: $sql_auth<br>";
                    last;
                }
            }                    
            # If we didn't get a parent or status ne 'belongs to'
            else{                
                $parent_no = 0;
            }
        }   
        else{   
            # No results might not be an error: it might just be lack of data
            # print "ERROR in sql: $sql_opin<br>";
            last;                 
        }       
    }       
  }
  return \%full_class;
}

1;
