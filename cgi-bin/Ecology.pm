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
1;
