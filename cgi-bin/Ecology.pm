package Ecology;

# written by JA 27-31.7,1.8.03

my @fields = ('composition1', 'composition2', 'entire_body', 'body_part', 'adult_length', 'adult_width', 'adult_height', 'adult_area', 'adult_volume', 'thickness', 'architecture', 'form', 'reinforcement', 'folds', 'ribbing', 'spines', 'internal_reinforcement', 'polymorph', 'ontogeny', 'grouping', 'clonal', 'taxon_environment', 'locomotion', 'attached', 'epibiont', 'life_habit', 'depth_habitat', 'diet1', 'diet2', 'reproduction', 'asexual', 'brooding', 'dispersal1', 'dispersal2', 'comments','minimum_body_mass','maximum_body_mass');

sub populateEcologyForm	{
	my $dbh = shift;
	my $dbt = shift;
	my $hbo = shift;
	my $q = shift;
	my $s = shift;
	my $exec_url = shift;

    # We need a taxon_no passed in, cause taxon_name is ambiguous
	if ( ! $q->param('taxon_no')) {
		print "<center><h3>Sorry, the taxon's name is unknown</h3></center>\n";
		exit;
	}
    $taxon_no = int($q->param('taxon_no'));

    # For form display purposes
	$sql = "SELECT taxon_name FROM authorities WHERE taxon_no=" . $taxon_no;
	$taxon_name =  ${$dbt->getData($sql)}[0]->{'taxon_name'};


	# query the ecotaph table for the old data
	$sql = "SELECT * FROM ecotaph WHERE taxon_no=" . $taxon_no;
	my $ecotaph = ${$dbt->getData($sql)}[0];
    my @values = ();
    if (!$ecotaph) {
        # This is a new entry
        if (!$q->param('skip_ref_check') || !$s->get('reference_no')) {
                # Make them choose a reference first
                my $toQueue = "action=startPopulateEcologyForm&skip_ref_check=1&goal=".$q->param('goal')."&taxon_no=$taxon_no";
                $s->enqueue( $dbh, $toQueue );
                $q->param( "type" => "select" );
                main::displaySearchRefs("Please choose a reference before adding ecological/taphonomic data",1);
                return;
        } else {
            push @values, '' for @fields;
	        push (@fields,'taxon_no','taxon_name','reference_no','ecotaph_no');
	        push (@values,$taxon_no ,$taxon_name ,$s->get('reference_no'),'-1');
        }
    } else {
        # This is an edit, use fields from the DB
	    for my $field ( @fields )	{
			if ( $ecotaph->{$field} )	{
	    	    push @values, $ecotaph->{$field};
            } else {
                push @values, '';
            }
        }
        # some additional fields not from the form row
	    push (@fields, 'taxon_no','taxon_name','reference_no','ecotaph_no');
	    push (@values, $taxon_no,$taxon_name,$ecotaph->{'reference_no'},$ecotaph->{'ecotaph_no'});
    }

	# populate the form
    if ($q->param('goal') eq 'ecovert') {
        # For the vertebrate ecology form, we need to rename these three fields to alternate versions
        # (ecovert_diet1, ecovert_diet2, ecovert_life_habit) so that HTMLBuilder will populate them with
        # the alternate versions of the select lists.  For processEcologyForm we need to tranlate back 
        # to DB-friendly names as well
        for(my $i=0;$i<scalar(@fields);$i++) {
            if ($fields[$i] =~ /^life_habit|diet1|diet2|reproduction$/) {
                $fields[$i] = 'ecovert_'.$fields[$i];
            }
        }
	    print $hbo->populateHTML('ecovert_form', \@values, \@fields);
    } else {
	    print $hbo->populateHTML('ecotaph_form', \@values, \@fields);
    }
	return;
}

sub processEcologyForm	{
	my $dbh = shift;
	my $dbt = shift;
	my $q = shift;
	my $s = shift;
	my $exec_url = shift;

	# can't proceed without a taxon no
	if (!$q->param('taxon_no'))	{
		print "<center><h3>Sorry, the ecology/taphonomy table can't be updated because the taxon is unknown</h3></center>\n";
		return;
	}
	my $taxon_no = int($q->param('taxon_no'));
	my $sql;

	# if ecotaph is blank but taxon no actually is in the ecotaph table,
	#  something is really wrong, so exit
	if ( $q->param('ecotaph_no') < 1 )	{
    	# query the ecotaph table
		$sql = "SELECT ecotaph_no FROM ecotaph WHERE taxon_no=" . $taxon_no;
		my $ecotaph = ${$dbt->getData($sql)}[0];

    	# result is found, so bomb out
		if ( $ecotaph )	{
			print "<center><h3>Sorry, ecology/taphonomy information already exists for this taxon, please edit the old record instead of creating a new one.</h3></center>\n";
            return;
		}
	}

	# get the taxon's name
	$sql = "SELECT taxon_name FROM authorities WHERE taxon_no=" . $taxon_no;
	my $taxon_name = ${$dbt->getData($sql)}[0]->{'taxon_name'};

	# if ecotaph no exists, update the record
    my %fields = $q->Vars();
    if ($q->param('goal') eq 'ecovert') {
        # Translate the special fields back to their names in the DB
        $fields{'reproduction'} = $fields{'ecovert_reproduction'};
        $fields{'life_habit'} = $fields{'ecovert_life_habit'};
        $fields{'diet1'} = $fields{'ecovert_diet1'};
        $fields{'diet2'} = $fields{'ecovert_diet2'};
    }
	if ( $q->param('ecotaph_no') > 0 )	{
        $dbt->updateRecord($s,'ecotaph','ecotaph_no',$q->param('ecotaph_no'),\%fields);
		print "<center><h3>Ecological/taphonomic data for $taxon_name have been updated</h3></center>\n";
	} else {
        # Set the reference_no
        $fields{'reference_no'} = $s->get('reference_no');
        $dbt->insertRecord($s,'ecotaph',\%fields);
		print "<center><h3>Ecological/taphonomic data for $taxon_name have been added</h3></center>\n";
	}

    my $action = ($q->param('goal') eq 'ecovert') ? 'startStartEcologyVertebrateSearch' : 'startStartEcologyTaphonomySearch';
	print "<center><p><a href=\"$exec_url?action=startPopulateEcologyForm&taxon_no=$taxon_no&goal=".$q->param('goal')."\">Edit data for this taxon</a> - \n";
	print "<a href=\"$exec_url?action=$action\">Enter data for another taxon</a></p></center>\n";
	return;
}


# PS 08/31/2005
# This will return ecology data for a taxon
# This process is a bit tricky because ecology data can be inherited from parents.  
# The second parameter must thus be a classification hash as returned by get_classification_hash with a type of 'array'
# The third parameter must be the fields you want returned. 
# The fourth parameter is essentially a boolean - ($get_basis) - which determines if you also want to return
#  what taxonomic rank the  ecology data is based off (i.e. class,order,family). Access this data as another hash field
#  with the string basis appended (see example below)
# 
# It'll return a hash where the keys are taxon_nos and the value is a hash of ecology data
# example: $class_hash = Classification::get_classification_hash($dbt,'all',[$taxon_no],'array');
#          @ecotaph_fields = $dbt->tableColumns('ecotaph');
#          $eco_hash = Ecology::getEcology($dbt,$eco_hash,\@ecotaph_fields,1);
#          $life_habit_for_taxon_no = $eco_hash->{$taxon_no}{'life_habit'};
#          @refs_for_taxon_no = @{$eco_hash->{$taxon_no}{'references'}};
#          $based_off_rank = $eco_hash{$taxon_no}{'life_habit'.'basis'};

sub getEcology {
    my $dbt = shift;
    my $classification_hash = shift;
    my $user_fields = shift;
    my $get_basis = shift;
    
    my @fields = @$user_fields;

    my @taxon_nos = keys(%$classification_hash);
    # This first section gets ecology data for all taxa and parent taxa
    # Dealing with recombinatins/corrections is tricky.  Strategy I use is this: 
    # Store an array of alternative taxon_nos to a taxon in %alt_taxon_nos hash
    # Iterate through this array when getting eco_data for the taxon_no as well. 
    # Synonyms hopefully dealth with as well
    my %all_taxon_nos = (-1); # so we don't crash if there are no taxon nos. 
    my %alt_taxon_nos;
    for my $taxon_no ( @taxon_nos ) {
        if ($taxon_no) {
            $all_taxon_nos{$taxon_no} = 1;
            foreach my $parent (@{$classification_hash->{$taxon_no}}) {
                $all_taxon_nos{$parent->{'taxon_no'}} = 1;
                foreach my $synonym (@{$parent->{'synonyms'}}) {
                    $all_taxon_nos{$synonym->{'taxon_no'}} = 1;
                }
            }
        }
    }  

    # Get a list of alternative names of existing taxa as well
    my $sql = "SELECT child_no,child_spelling_no FROM opinions WHERE child_no != child_spelling_no AND child_no IN (".join(", ",keys %all_taxon_nos).")";
    my @results = @{$dbt->getData($sql)};
    foreach my $row (@results) {
        push @{$alt_taxon_nos{$row->{'child_no'}}},$row->{'child_spelling_no'};
        $all_taxon_nos{$row->{'child_spelling_no'}} = 1;
    }
    $sql = "SELECT child_no,child_spelling_no FROM opinions WHERE child_no != child_spelling_no AND child_spelling_no IN (".join(", ",keys %all_taxon_nos).")";
    @results = @{$dbt->getData($sql)};
    foreach my $row (@results) {
        push @{$alt_taxon_nos{$row->{'child_spelling_no'}}},$row->{'child_no'};
        $all_taxon_nos{$row->{'child_no'}} = 1;
    }

    $sql = "SELECT taxon_no,reference_no,".join(", ",@fields)." FROM ecotaph WHERE taxon_no IN (".join(", ",keys %all_taxon_nos).")";
    main::dbg("Ecology sql: $sql");
    @results = @{$dbt->getData($sql)};
    my %all_ecologies;
    foreach my $row (@results) {
        $all_ecologies{$row->{'taxon_no'}} = $row;
    }

    my %ranks;
    if ($get_basis) {
        if (@taxon_nos) {
            $sql = "SELECT taxon_no,taxon_rank FROM authorities WHERE taxon_no IN (".join(",",keys %all_taxon_nos).")";
            my @results = @{$dbt->getData($sql)};
            foreach my $row (@results) {
                $ranks{$row->{'taxon_no'}} = $row->{'taxon_rank'};
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
        push @exec_order, @{$alt_taxon_nos{$taxon_no}}; #recombinations first
        push @exec_order, $taxon_no; #then the taxon
        foreach my $parent (@{$classification_hash->{$taxon_no}}) {
            push @exec_order, @{$alt_taxon_nos{$parent->{'taxon_no'}}}; # then parent recombinations
            push @exec_order, $parent->{'taxon_no'}; #then the parent
            foreach my $synonym (reverse @{$parent->{'synonyms'}}) {
                push @exec_order, $synonym->{'taxon_no'}; # then junior synonyms last
            }
        }
        
        my ($seen_diet,$seen_composition,$seen_adult) = (0,0,0);
        foreach my $use_taxon_no (@exec_order) {
            foreach my $field (@fields) {
                if ($all_ecologies{$use_taxon_no}{$field} && !$ecotaph{$taxon_no}{$field}) {
                    # The following three next's deal with linked fields. We can't mix and match
                    # diet1/diet2 from different family/classes, so if one of them is set, skip messing with the whole group
                    next if ($field =~ /^composition/ && $seen_composition);
                    next if ($field =~ /^diet/ && $seen_diet);
                    next if ($field =~ /^adult_/ && $seen_adult);
                    $ecotaph{$taxon_no}{$field} = $all_ecologies{$use_taxon_no}{$field};
                    # If we want to know what class this data is based off of 
                    # (used in displayCollectionEcology), get that as well
                    $ecotaph{$taxon_no}{$field.'basis'} = $ranks{$use_taxon_no} if ($get_basis);
                    my $reference_no = $all_ecologies{$use_taxon_no}{'reference_no'};
                    $refs_for_taxon{$reference_no} = 1;
                }
            }
            $seen_composition = 1 if ($all_ecologies{$use_taxon_no}{'composition1'} ||
                                      $all_ecologies{$use_taxon_no}{'composition2'});
            $seen_diet = 1 if ($all_ecologies{$use_taxon_no}{'diet1'} ||
                               $all_ecologies{$use_taxon_no}{'diet2'});
            $seen_adult = 1 if ($all_ecologies{$use_taxon_no}{'adult_length'} ||
                                $all_ecologies{$use_taxon_no}{'adult_width'} ||
                                $all_ecologies{$use_taxon_no}{'adult_height'} ||
                                $all_ecologies{$use_taxon_no}{'adult_area'} ||
                                $all_ecologies{$use_taxon_no}{'adult_volume'});
        }
        # Get all the references that were used in getting this ecology data
        if (exists $ecotaph{$taxon_no}) {
            my @refs = keys %refs_for_taxon;
            $ecotaph{$taxon_no}{'references'} = \@refs;
        }
    }

    return \%ecotaph;
}                      

1;
