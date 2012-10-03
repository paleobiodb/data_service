package Ecology;

use TaxaCache;
use Debug qw(dbg);
use Constants qw($WRITE_URL $TAXA_TREE_CACHE $TAXA_LIST_CACHE);
use Reference;

use strict;

# written by JA 27-31.7,1.8.03

my @fields = ('composition1', 'composition2', 'entire_body', 'body_part', 'adult_length', 'adult_width', 'adult_height', 'adult_area', 'adult_volume', 'thickness', 'architecture', 'form', 'reinforcement', 'folds', 'ribbing', 'spines', 'internal_reinforcement', 'polymorph', 'ontogeny', 'grouping', 'clonal', 'taxon_environment', 'locomotion', 'attached', 'epibiont', 'life_habit', 'depth_habitat', 'diet1', 'diet2', 'vision', 'reproduction', 'asexual', 'brooding', 'dispersal1', 'dispersal2', 'comments','minimum_body_mass','minimum_body_mass_unit','maximum_body_mass','maximum_body_mass_unit','body_mass_comment','body_mass_estimate','body_mass_estimate_unit','body_mass_source','body_mass_type');

sub populateEcologyForm	{
    my ($dbt, $taxonomy, $hbo, $q, $s) = @_;
    
    my $dbh = $dbt->dbh;

    # We need a taxon_no passed in, cause taxon_name is ambiguous
	if ( ! $q->param('taxon_no')) {
		print "<center><div class=\"pageTitle\" style=\"margin-top: 1em;\">Sorry, the taxon's name is not in the system</div></center>\n";
		exit;
	}
    $taxon_no = int($q->param('taxon_no'));

    # For form display purposes
    my $taxon = $taxonomy->getTaxon($taxon_no);

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
	        push (@fields,'taxon_no', 'taxon_name', 'reference_no', 'ecotaph_no');
	        push (@values,$taxon_no, $taxon->{taxon_name}, $s->get('reference_no'), '-1');
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
	    push (@values, $taxon_no,$taxon->{taxon_name},$ecotaph->{'reference_no'},$ecotaph->{'ecotaph_no'});
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
    my ($dbt, $taxonomy, $q, $s) = @_;
    
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
    my $taxon = $taxonomy->getTaxon($taxon_no);
    
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
		print "<center><div class=\"pageTitle\" style=\"margin-top: 1em;\">Ecological/taphonomic data for $taxon->{taxon_name} have been updated</div></center>\n";
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

our (%FIELD_MAP) = ('minimum_body_mass' => 'min_body_mass',
		    'maximum_body_mass' => 'max_body_mass');

sub getEcology {
    
    my ($dbt, $taxonomy, $taxon, $field_list, $options) = @_;
    
    my $dbh = $dbt->dbh;
    
    my $tree_table = $taxonomy->{tree_table};
    
    # First, figure out the options
    
    $options ||= {};
    
    my $get_basis = $options->{get_basis};
    my $get_preservation = $options->{get_preservation};
    
    # Next, figure out the field list.
    
    my %field_names;
    
    unless ( ref $fields eq 'ARRAY' )
    {
	croak "parameter 'fields' must be an arrayref";
    }
    
    foreach $f (@$field_list)
    {
	next if $f eq 'body_mass_estimate';
	
	$f = $FIELD_MAP{$f} if exists $FIELD_MAP{$f};
	
	$field_names{"qe.$f"} = 1;
    }
    
    my $field_string = join(', ', 'qe.reference_no', keys %field_names);
    
    # Then, grab those fields from the indicated taxon plus all of its parents.
    
    my $list = 
	$taxonomy->getRelatedTaxa($taxon, 
				  'all_parents',
				  { select => 'orig',
				    join_tables => 
				    'LEFT JOIN ecotaph as qe on qe.taxon_no = [taxon_no] ' .
				    'LEFT JOIN $attrs_table as v using v.orig_no = [taxon_no]',
				    extra_fields => $field_string });
    
    # Now we can iterate through the list of rows (each representing a
    # containing taxon of our base taxon, going up the taxonomic hierarchy)
    # and pick out the first value appearing for each field.  That way, for
    # each field, we pick out the value from as low as possible in the
    # hierarchy.  For example, if 'diet' is defined at the genus level, we
    # ignore it at all higher levels.
    
    return unless ref $list eq 'ARRAY';
    
    my (%ecotaph) = { references => {} };
    
    foreach my $row (@$list)
    {
	# For each row, we look through all of the fields and grab any values
	# that are defined for that row.  We then delete the names of the
	# fields whose values we found, so that we don't look for them further
	# up the hierarchy.
	
	my @f = keys %field_names;
	
	foreach my $f (@f)
	{
	    if ( defined $row->{$f} )
	    {
		$ecotaph{$f}{value} = $row->{$f};
		$ecotaph{$f}{rank} = $row->{taxon_rank};
		$ecotaph{$f}{reference_no} = $row->{reference_no};
		$ecotaph{references}{$row->{reference_no}} = 1;
		
		delete $field_names{$f};
		
		# For each of these groups, if one is found, we stop looking for
		# the others as well.
		
		if ( $f =~ /^(diet|dispseral|composition)/ )
		{
		    delete $field_names{$1.'1'};
		    delete $field_names{$1.'2'};
		}
	    }
	}
    }
    
    # Turn the hash of references into a list.
    
    $ecotaph{references} = [ keys %{$ecotaph{references}} ];
    
    # Return the result.
    
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

# JA 17.4.12 creates one ecotaph attribute lookup for all taxa in a group
# IMPORTANT: when the changes from Measurement.pm are merged, change the call
# to this routine to pass in one taxon_no value instead of a (lft, rgt) pair!!!
sub fastEcologyLookup	{
    my ($dbt, $taxonomy, $base_taxon, $field) = @_;
    
    my $dbh = $dbt->dbh;
    
    # First, retrieve the specified attribute for the base taxon and all of
    # its children.
    
    my $result_list = 
	$taxonomy->getRelatedTaxa($base_taxon, 
				  'all_children',
				  { select => 'orig', 
				    include => 'lft',
				    join_tables => 'LEFT JOIN ecotaph as qe on qe.taxon_no = [taxon_no]',
				    extra_fields => "qe.$field" });
    
    # If the first row (the base taxon) doesn't have a value for the
    # attribute, we will have to look up its parents.
    
    unless ( $result_list->[0]{$field} )
    {
	my $parent_list = 
	    $taxonomy->getRelatedTaxa($base_taxon,
				      'all_parents',
				      { select => 'orig',
					exclude_self => 1,
					join_tables => 'LEFT JOIN ecotaph as qe on qe.taxon_no = [taxon_no]',
					extra_fields => "qe.$field" });
	
        foreach $row ( @$parent_list )
	{
	    if ( $row->{$field} )
	    {
		$result_list->[0]{$field} = $row->{$field};
		last;
	    }
	}
    }
    
    my (%lookup, @default);
    
    return \%lookup unless ref $result_list eq 'ARRAY';
    
    foreach my $t (@$result_list)
    {
	if ( $t->{$field} ne "" )	{
	    for my $pos ( $t->{lft}..$t->{rgt} )	{
		if ( $att{$pos} eq "" || $from{$pos} < $t->{lft} )	{
		    $att{$pos} = $t->{$field};
		    $from{$pos} = $t->{lft};
		}
	    }
	}
    }
    
    for my $t ( @taxa )	{
	$lookup{$t->{taxon_no}} = $att{$t->{lft}};
    }

    return \%lookup;
}


1;
