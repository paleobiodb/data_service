# includes entry functions extracted from Ecology.pm JA 5.6.13

package EcologyEntry;

use Debug qw(dbg);
use Constants qw($WRITE_URL);
use Reference;

# written by JA 27-31.7,1.8.03

my @fields = ('composition1', 'composition2', 'entire_body', 'body_part', 'adult_length', 'adult_width', 'adult_height', 'adult_area', 'adult_volume', 'thickness', 'architecture', 'form', 'reinforcement', 'folds', 'ribbing', 'spines', 'internal_reinforcement', 'polymorph', 'ontogeny', 'grouping', 'clonal', 'taxon_environment', 'locomotion', 'attached', 'epibiont', 'life_habit', 'depth_habitat', 'diet1', 'diet2', 'vision', 'reproduction', 'asexual', 'brooding', 'dispersal1', 'dispersal2', 'comments','minimum_body_mass','minimum_body_mass_unit','maximum_body_mass','maximum_body_mass_unit','body_mass_comment','body_mass_estimate','body_mass_estimate_unit','body_mass_source','body_mass_type');

sub populateEcologyForm	{
	my $dbt = shift;
	my $hbo = shift;
	my $q = shift;
	my $s = shift;
    my $dbh = $dbt->dbh;

    # We need a taxon_no passed in, cause taxon_name is ambiguous
	if ( ! $q->param('taxon_no')) {
		print "<center><div class=\"pageTitle\" style=\"margin-top: 1em;\">Sorry, the taxon's name is not in the system</div></center>\n";
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

	if ( $ecotaph->{'reference_no'} )	{
		push @fields , "primary_reference";
		my $sql = "SELECT * FROM refs WHERE reference_no=".$ecotaph->{'reference_no'};
		push @values , '<center><div class="small" style="width: 60em; text-align: left; text-indent: -1em;">Primary reference: ' . Reference::formatLongRef(${$dbt->getData($sql)}[0]) . '</div></center>';
		# ditch authorizer/enterer/modifier
		$values[$#values] =~ s/ \[.*\]//;
	}

	# populate the form
	if ($q->param('goal') eq 'ecovert')	{
		print $hbo->populateHTML('ecovert_form', \@values, \@fields);
	} else	{
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
		print "<center><div class=\"pageTitle\" style=\"margin-top: 1em;\">Sorry, the ecology/taphonomy table can't be updated because the taxon's name is not in the system</div></center>\n";
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
			print "<center><div class=\"pageTitle\" style=\"margin-top: 1em;\">Sorry, ecology/taphonomy information already exists for this taxon; please edit the old record instead of creating a new one</div></center>\n";
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
		print "<center><div class=\"pageTitle\" style=\"margin-top: 1em;\">Ecological/taphonomic data for $taxon_name have been updated</div></center>\n";
	} else {
        # Set the reference_no
        $fields{'reference_no'} = $s->get('reference_no');
        $dbt->insertRecord($s,'ecotaph',\%fields);
		print "<center><div class=\"pageTitle\" style=\"margin-top: 1em;\">Ecological/taphonomic data for $taxon_name have been added</div></center>\n";
	}

    my $action = ($q->param('goal') eq 'ecovert') ? 'startStartEcologyVertebrateSearch' : 'startStartEcologyTaphonomySearch';
	print "<center><p><a href=\"$WRITE_URL?action=startPopulateEcologyForm&taxon_no=$taxon_no&goal=".$q->param('goal')."\">Edit data for this taxon</a> - \n";
	print "<a href=\"$WRITE_URL?action=$action\">Enter data for another taxon</a></p></center>\n";
	return;
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

