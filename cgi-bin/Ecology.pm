package Ecology;

use TaxaCache;
use Debug qw(dbg);
use Constants qw($WRITE_URL $TAXA_TREE_CACHE);

# written by JA 27-31.7,1.8.03

my @fields = ('composition1', 'composition2', 'entire_body', 'body_part', 'adult_length', 'adult_width', 'adult_height', 'adult_area', 'adult_volume', 'thickness', 'architecture', 'form', 'reinforcement', 'folds', 'ribbing', 'spines', 'internal_reinforcement', 'polymorph', 'ontogeny', 'grouping', 'clonal', 'taxon_environment', 'locomotion', 'attached', 'epibiont', 'life_habit', 'depth_habitat', 'diet1', 'diet2', 'reproduction', 'asexual', 'brooding', 'dispersal1', 'dispersal2', 'comments','minimum_body_mass','minimum_body_mass_unit','maximum_body_mass','maximum_body_mass_unit','body_mass_comment','body_mass_estimate','body_mass_estimate_unit','body_mass_source','body_mass_type');

sub populateEcologyForm	{
	my $dbt = shift;
	my $hbo = shift;
	my $q = shift;
	my $s = shift;
    my $dbh = $dbt->dbh;

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
        if (!$s->get('reference_no')) {
            # Make them choose a reference first
            $s->enqueue($q->query_string());
            main::displaySearchRefs("Please choose a reference before adding ecological/taphonomic data",1);
            return;
        } else {
            push @values, '' for @fields;
            for (my $i = 0;$i<scalar(@fields);$i++) { # default to kg for units
                if ($fields[$i] =~ /_unit$/) {
                    $values[$i] = 'kg';
                }
            }
	        push (@fields,'taxon_no','taxon_name','reference_no','ecotaph_no');
	        push (@values,$taxon_no ,$taxon_name ,$s->get('reference_no'),'-1');
        }
    } else {
        # This is an edit, use fields from the DB
        if ($ecotaph->{'minimum_body_mass'} && $ecotaph->{'minimum_body_mass'} < 1) {
            $ecotaph->{'minimum_body_mass'} = kgToGrams($ecotaph->{'minimum_body_mass'});
            $ecotaph->{'minimum_body_mass_unit'} = 'g';
        } else {
            $ecotaph->{'minimum_body_mass_unit'} = 'kg';
        }
        if ($ecotaph->{'maximum_body_mass'} && $ecotaph->{'maximum_body_mass'} < 1) {
            $ecotaph->{'maximum_body_mass'} = kgToGrams($ecotaph->{'maximum_body_mass'});
            $ecotaph->{'maximum_body_mass_unit'} = 'g';
        } else {
            $ecotaph->{'maximum_body_mass_unit'} = 'kg';
        }
        if ($ecotaph->{'body_mass_estimate'} && $ecotaph->{'body_mass_estimate'} < 1) {
            $ecotaph->{'body_mass_estimate'} = kgToGrams($ecotaph->{'body_mass_estimate'});
            $ecotaph->{'body_mass_estimate_unit'} = 'g';
        } else {
            $ecotaph->{'body_mass_estimate_unit'} = 'kg';
        }
        
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
	    print $hbo->populateHTML('ecovert_form', \@values, \@fields);
    } else {
	    print $hbo->populateHTML('ecotaph_form', \@values, \@fields);
    }
	return;
}

sub processEcologyForm	{
	my $dbt = shift;
	my $q = shift;
	my $s = shift;
    my $dbh = $dbt->dbh;

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

    # This is an edit, use fields from the DB
    if ($fields{'minimum_body_mass'} && $fields{'minimum_body_mass_unit'} eq 'g') {
        $fields{'minimum_body_mass'} = gramsToKg($fields{'minimum_body_mass'});
    } 
    if ($fields{'maximum_body_mass'} && $fields{'maximum_body_mass_unit'} eq 'g') {
        $fields{'maximum_body_mass'} = gramsToKg($fields{'maximum_body_mass'});
    } 
    if ($fields{'body_mass_estimate'} && $fields{'body_mass_estimate_unit'} eq 'g') {
        $fields{'body_mass_estimate'} = gramsToKg($fields{'body_mass_estimate'});
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
	print "<center><p><a href=\"$WRITE_URL?action=startPopulateEcologyForm&taxon_no=$taxon_no&goal=".$q->param('goal')."\">Edit data for this taxon</a> - \n";
	print "<a href=\"$WRITE_URL?action=$action\">Enter data for another taxon</a></p></center>\n";
	return;
}


# PS 08/31/2005
# This will return ecology data for a taxon
# This process is a bit tricky because most ecology data can be inherited from parents.  Body size data is actually
#  inherited from the chlidren and can either be a point estimate (single value) or a range of values.  Multiple
#  point estimates can turn into a range.
# The second parameter must thus be a classification hash as returned by get_classificaton_hash or TaxaCache::getParents with a type of 'array'
# The third parameter must be the fields you want returned. 
# The fourth parameter is essentially a boolean - ($get_basis) - which determines if you also want to return
#  what taxonomic rank the  ecology data is based off (i.e. class,order,family). Access this data as another hash field
#  with the string "basis" appended (see example below)
# 
# It'll return a hash where the keys are taxon_nos and the value is a hash of ecology data
# example: $class_hash = TaxaCache::getParents($dbt,[$taxon_no],'array');
#          @ecotaph_fields = $dbt->getTableColumns('ecotaph');
#          $eco_hash = Ecology::getEcology($dbt,$eco_hash,\@ecotaph_fields,1);
#          $life_habit_for_taxon_no = $eco_hash->{$taxon_no}{'life_habit'};
#          @refs_for_taxon_no = @{$eco_hash->{$taxon_no}{'references'}};
#          $based_off_rank = $eco_hash{$taxon_no}{'life_habit'.'basis'};

# Can return authorities.preservation if $get_preservation is 1 as well PS 8/2006

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
            $sql = "SELECT taxon_no,taxon_rank,preservation FROM authorities WHERE taxon_no IN (".join(",",keys %all_taxon_nos).")";
            my @results = @{$dbt->getData($sql)};
            foreach my $row (@results) {
                $taxon_metadata{$row->{'taxon_no'}}{'taxon_rank'} = $row->{'taxon_rank'};
                $taxon_metadata{$row->{'taxon_no'}}{'preservation'} = $row->{'preservation'};
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
        push @fields,'preservation';
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


# Converts an floating point number (in grams) into a text string (in kilograms)
# that preserves the precision of the number, for insertion into the database
# I.e 42.30 grams would become .04230 grams.  Note the 0 at the end, which preseres the precision
sub gramsToKg {
    my $text = shift;
    my $decimal_offset = index($text,'.');
    if ($decimal_offset >= 0) {
        $text =~ s/\.//g;
        my $float;
        if ($decimal_offset <= 3) {
            $float = "0.";
            for (1..(3-$decimal_offset)) {
                $float .= "0";
            }
            $float .= $text;
        } else {
            $float = substr($text,0,$decimal_offset-3).".".substr($text,$decimal_offset-3);
        }
        return $float;
    } else {
        return ($text/1000);
    }
}

# The opposite of the above function, get back the human readable version that was originally entered
sub kgToGrams{
    my $text = shift;
    my $decimal_offset = index($text,'.');
    if ($decimal_offset >= 0) {
        my $float;
        if ((length($text)-$decimal_offset) > 4) {
            $text =~ s/\.//g;
            $float = substr($text,0,$decimal_offset+3).".".substr($text,$decimal_offset+3);
            $float =~ s/^[0]+//g;
            $float = "0".$float if ($float =~ /^\./);
        } else {
            $float = ($text*1000);
        }
        return $float;
    } else {
        return ($text*1000);
    }
} 

1;
