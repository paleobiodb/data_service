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
    my $base_taxon_name = $item;
    my %family = ();
    my %order = ();
    my %class = ();
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
            my $base_rank = $results[0]->{taxon_rank};
            if ( $base_rank =~ /family/ )	{
              $family{$taxon_name} = $taxon_name;
            } elsif ( $base_rank =~ /order/ )	{
              $order{$taxon_name} = $taxon_name;
            } elsif ( $base_rank =~ /class/ )	{
              $class{$taxon_name} = $taxon_name;
            }

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
				$family{$base_taxon_name} = $taxon_name;
				# Skip ahead if we've already got the hierarchy
				# $order_ref will either be a scalar or a hash ref.
				if ( $order{$taxon_name} )	{
					last;
				}
			}
			elsif($rank eq "order"){
				$order{$base_taxon_name} = $taxon_name;
				$order{$family{$base_taxon_name}} = $taxon_name;
				if ( $class{$taxon_name} )	{
					last;
				}
			}
			elsif($rank eq "class"){
				$class{$base_taxon_name} = $taxon_name;
				$class{$family{$base_taxon_name}} = $taxon_name;
				$class{$order{$base_taxon_name}} = $taxon_name;
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
# DOESN'T HELP
  # if ( ! $order{$base_taxon_name} )	{
  #   $order{$base_taxon_name} = $order{$family{$base_taxon_name}};
  # }
  # if ( ! $class{$base_taxon_name} )	{
  #   $class{$base_taxon_name} = $class{$order{$base_taxon_name}};
  # }
    $full_class{$base_taxon_name} = $class{$base_taxon_name} .",". $order{$base_taxon_name} .",". $family{$base_taxon_name};
  }
  return \%full_class;
}

1;
