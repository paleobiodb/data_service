package Ecology;

$DEBUG = 0;

# written by JA 27-31.7,1.8.03

my @fields = ('taxon_no', 'composition1', 'composition2', 'entire_body', 'body_part', 'adult_length', 'adult_width', 'adult_height', 'adult_area', 'adult_volume', 'thickness', 'architecture', 'form', 'reinforcement', 'folds', 'ribbing', 'spines', 'internal_reinforcement', 'polymorph', 'ontogeny', 'grouping', 'clonal', 'taxon_environment', 'locomotion', 'attached', 'epibiont', 'life_habit', 'depth_habitat', 'diet1', 'diet2', 'reproduction', 'asexual', 'brooding', 'dispersal1', 'dispersal2', 'comments');

sub startEcologySearch	{
	my $dbh = shift;
	my $hbo = shift;
	my $session = shift;
	my $exec_url = shift;
	my $revisits = shift;

	# Have to have a reference #
	my $reference_no = $session->get("reference_no");
	if ( ! $reference_no )  {
		$session->enqueue( $dbh, "action=startStartEcologySearch" );
		main::displaySearchRefs ( "Please choose a reference first" );
		exit;
	}


	# Have to be logged in
	if ($session->get('enterer') eq "Guest" or $session->get('enterer') eq "
")	{
		$session->enqueue($dbh, "action=startStartEcologySearch");
		exit;
	}

	# print the search form
	print main::stdIncludes("std_page_top");

	if ( $revisits > 0 )	{
		print "<center><h3>Sorry, that taxon is unknown</h3></center>\n";
	}

	my $form = $hbo->populateHTML('search_taxonomy_form','','');

	# reword the form and redirect the destination to populateEcologyForm
	# WARNING: this is a low-down dirty trick; very inelegant
	$form =~ s/%%taxon_what_to_do%%/to describe/;
	$form =~ s/processTaxonomySearch/startPopulateEcologyForm/;

	print $form;

	print main::stdIncludes("std_page_bottom");

	return;

}

sub populateEcologyForm	{
	my $dbh = shift;
	my $dbt = shift;
	my $hbo = shift;
	my $q = shift;
	my $session = shift;
	my $exec_url = shift;

	# if there's no taxon no OR name, something's totally wrong,
	#  so bomb out
	if ( ! $q->param('taxon_no') && ! $q->param('taxon_name') )	{
		print "<center><h3>Sorry, the taxon's name is unknown</h3></center>\n";
		exit;
	}

	# if there's no taxon no but there's a taxon name, query the
	#  authorities table for the taxon no

	my $sql;
	my $taxon_no;
	if ( ! $q->param('taxon_no') )	{
		$sql = "SELECT taxon_no FROM authorities WHERE taxon_name='" . $q->param('taxon_name') . "'";
		my $taxref = @{$dbt->getData($sql)}[0];
		if ( $taxref )	{
			$taxon_no = $taxref->{taxon_no};
		}
	} else	{
		$taxon_no = $q->param('taxon_no');
	}

	# if there's still no taxon name, get it
	if ( ! $q->param('taxon_name') )	{
	
		$sql = "SELECT taxon_name FROM authorities WHERE taxon_no=" . $taxon_no;
		my $taxref = @{$dbt->getData($sql)}[0];
		$q->param('taxon_name' => $taxref->{taxon_name});
	}

	# if the search failed, print the search form and exit

	if ( ! $taxon_no )	{
		&startEcologySearch($dbh,$hbo,$session,$exec_url,'1');
		exit;
	}

	# query the ecotaph table for the old data

	$sql = "SELECT * FROM ecotaph WHERE taxon_no=" . $taxon_no;
	my $ecotaph = @{$dbt->getData($sql)}[0];

	# find the fields with actual values

	my @fieldNames;
	my @fieldValues;
	if ( $ecotaph )	{
		push @fieldNames, 'ecotaph_no';
		push @fieldValues, $ecotaph->{ecotaph_no};
		for my $field ( @fields )	{
			if ( $ecotaph->{$field} )	{
				push @fieldNames, $field;
				push @fieldValues, $ecotaph->{$field};
			} else	{
				push @fieldNames, $field;
				push @fieldValues, '';
			}
		}
	} else	{
		for my $field ( @fields )	{
			push @fieldNames, $field;
			push @fieldValues, '';
		}
	}

	# make sure the taxon no is carried over as a hidden value
	unshift @fieldNames,'taxon_no';
	unshift @fieldValues,$taxon_no;

	# populate the form

	print main::stdIncludes("std_page_top");

	my $form = $hbo->populateHTML(ecotaph_form, \@fieldValues, \@fieldNames);

	my $taxon_name = $q->param('taxon_name');
	$form =~ s/taxon_name/$taxon_name/;

	print $form;

	print main::stdIncludes("std_page_bottom");

	return;

}

sub processEcologyForm	{
	my $dbh = shift;
	my $dbt = shift;
	my $q = shift;
	my $s = shift;
	my $exec_url = shift;

	# print the header
	print main::stdIncludes("std_page_top");

	# can't proceed without a taxon no
	if ( ! $q->param('taxon_no') )	{
		print "<center><h3>Sorry, the ecology/taphonomy table can't be updated because the taxon is unknown</h3></center>\n";
		print main::stdIncludes("std_page_bottom");
		exit;
	}
	my $taxon_no = $q->param('taxon_no');

	my $sql;

	# if ecotaph is blank but taxon no actually is in the ecotaph table,
	#  something is really wrong, so exit
	if ( $q->param('ecotaph_no') < 1 )	{
	# query the ecotaph table
		$sql = "SELECT ecotaph_no FROM ecotaph WHERE taxon_no=" . $taxon_no;
		my $ecotaph = @{$dbt->getData($sql)}[0];

	# result is found, so bomb out
		if ( $ecotaph )	{
			print "<center><h3>Sorry, the ecology/taphonomy table can't be updated because the taxon is unknown</h3></center>\n";
			print main::stdIncludes("std_page_bottom");
			exit;
		}
	}

	# get the taxon's name
	$sql = "SELECT taxon_name FROM authorities WHERE taxon_no=" . $taxon_no;
	my $taxref = @{$dbt->getData($sql)}[0];
	my $taxon_name = $taxref->{taxon_name};

	# stash the field names and values for populated fields
	my @fieldNames;
	my @fieldValues;
	for my $f ( @fields )	{
		my @temp = $q->param($f);
		# set fields will start with a blank param inserted by
		#  HTMLBuilder, so strip it
		if ( ! $temp[0] )	{
			shift @temp;
		}
		if ( $temp[0] )	{
			push @fieldNames, $f;
		# there might be multiple values, so get an array
			my $v = join ',',@temp;
		# values could be strings, so escape single quotes and pad
		#  with single quotes
			#$v =~ s/'/\'/;
			#$v = "'" . $v . "'";
			#push @fieldValues, $v;
            push @fieldValues, $dbh->quote($v);
		}
	}

	# if ecotaph no exists, update the record
	if ( $q->param('ecotaph_no') > 0 )	{
	# set the modifier name
		unshift @fieldNames, 'modifier_no';
		unshift @fieldValues, $s->get('enterer_no');

		$sql = "UPDATE ecotaph SET ";
		for $i (0..$#fieldNames)	{
			$sql .= $fieldNames[$i] . "=" . $fieldValues[$i];
			if ( $i < $#fieldNames )	{
				$sql .= ",";
			}
		}
		$sql .= " WHERE ecotaph_no=" . $q->param('ecotaph_no') ;
		$dbt->getData($sql);

		print "<center><h3>Ecological/taphonomic data for $taxon_name have been updated</h3></center>\n";
	}

	# if there's no ecotaph no, insert a new record
	else	{
	# set the authorizer and enterer and reference no
		unshift @fieldNames, 'authorizer_no';
		unshift @fieldValues, $s->get('authorizer_no');

		unshift @fieldNames, 'enterer_no';
		unshift @fieldValues, $s->get('enterer_no');

		unshift @fieldNames, 'reference_no';
		unshift @fieldValues, $s->get('reference_no');

	# insert the record
		$sql = "INSERT INTO ecotaph (" . join(',',@fieldNames) . ") VALUES (" . join(',',@fieldValues) . ")";
		$dbt->getData($sql);

	# set the created date
		$sql = "SELECT modified FROM ecotaph WHERE taxon_no=" . $taxon_no;
		my @modifieds = @{$dbt->getData($sql)};
		$sql = "UPDATE ecotaph SET modified=modified,created=";
		$sql .= $dbh->quote($modifieds[0]->{modified}) . " WHERE taxon_no=" . $taxon_no;
		$dbt->getData($sql);
		print "<center><h3>Ecological/taphonomic data for $taxon_name have been added</h3></center>\n";
	}

	print "<center><p><a href=\"$exec_url?action=startPopulateEcologyForm&taxon_no=$taxon_no\">Edit data for this taxon</a> - \n";
	print "<a href=\"$exec_url?action=startStartEcologySearch\">Enter data for another taxon</a></p></center>\n";

	print main::stdIncludes("std_page_bottom");
	return;
}



