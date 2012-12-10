package Ecology;

use Debug qw(dbg);
use Constants qw($WRITE_URL $TAXA_TREE_CACHE $TAXA_LIST_CACHE);
use Reference;
use Taxonomy;

use CGI::Carp;

use strict;

# written by JA 27-31.7,1.8.03

our @fields = ('composition1', 'composition2', 'entire_body', 'body_part', 'adult_length', 'adult_width', 'adult_height', 'adult_area', 'adult_volume', 'thickness', 'architecture', 'form', 'reinforcement', 'folds', 'ribbing', 'spines', 'internal_reinforcement', 'polymorph', 'ontogeny', 'grouping', 'clonal', 'taxon_environment', 'locomotion', 'attached', 'epibiont', 'life_habit', 'depth_habitat', 'diet1', 'diet2', 'vision', 'reproduction', 'asexual', 'brooding', 'dispersal1', 'dispersal2', 'comments','minimum_body_mass','minimum_body_mass_unit','maximum_body_mass','maximum_body_mass_unit','body_mass_comment','body_mass_estimate','body_mass_estimate_unit','body_mass_source','body_mass_type');


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

sub getEcology { # $$$$$ need to fix this - must return hash keyed to base_taxa!!!
    
    my ($dbt, $taxonomy, $base_taxa, $field_list, $options) = @_;
    
    my $dbh = $dbt->dbh;
    
    my $tree_table = $taxonomy->{tree_table};
    
    # First, figure out the options
    
    $options ||= {};
    
    my $get_basis = $options->{get_basis};
    my $get_preservation = $options->{get_preservation};
    
    # Next, figure out the field list.
    
    my %field_names;
    
    unless ( ref $field_list eq 'ARRAY' )
    {
	croak "parameter 'field_list' must be an arrayref";
    }
    
    foreach my $f (@$field_list)
    {
	next if $f eq 'body_mass_estimate';
	
	$f = $FIELD_MAP{$f} if exists $FIELD_MAP{$f};
	
	$field_names{$f} = 1;
    }
    
    my $field_string = join(', ', 'qe.reference_no', map { "qe.$_" } keys(%field_names));
    
    # Then, grab those fields from the indicated taxa plus all of their parents.
    
    my $list = 
	$taxonomy->getTaxa('all_parents', $base_taxa, 
			   { select => 'spelling',
			     join_tables => 
				'LEFT JOIN ecotaph as qe on qe.taxon_no = [taxon_no] ' .
				'LEFT JOIN $attrs_table as v using v.orig_no = [taxon_no]',
			     extra_fields => $field_string });
    
    # Now we can iterate through the list of rows (each representing a
    # containing taxon of one or more of our base taxa, going up the taxonomic
    # hierarchy) and propagate the values downward.
    
    return {} unless ref $list eq 'ARRAY';
    
    my ($ecotaph) = {};
    my (@fields) = keys %field_names;
    my (%references);
    my ($row_cache) = {};
    
    # Now, we go through the rows one by one.  We are guaranteed that parents
    # come before children, by the semantics of getTaxa().
    
    foreach my $row (@$list)
    {
	my $taxon_no = $row->{taxon_no};
	my %seen;
	
	# Start by cacheing parent rows, so that we can get back to them for
	# inheritance purposes later.
	
	$row_cache->{$taxon_no} = $row;
	
	# Now look through all of the fields and grab any values that are
	# defined for that row.  All of the others get inherited from the
	# parent (if there is no parent, use an empty hash as a placeholder).
	
	my $parent_row = $row_cache->{$row->{parent_taxon_no}} || {};
	
	foreach my $f (@fields)
	{
	    # We compute derivatives of the field name: $b is the 'basis
	    # field', which indicates from which taxonomic rank this value
	    # derives.  $r is the 'reference field', which gives a reference
	    # number in which this value was given.  $s is the "short name" of
	    # the field, with any trailing digit removed.  This is used so
	    # that, e.g. if we see the field 'diet1' we do not try to inherit
	    # the parent's 'diet2'.
	    
	    my $b = "$f.basis";
	    my $r = "$f.ref";
	    my $s = $1 if $f =~ /^(\w+)\d$/;
	    $s = 'adult' if $f =~ /^adult/;
	    
	    # If the given field is defined for the given row, we set the
	    # auxiliary fields and note the short name.
	    
	    if ( defined $row->{$f} )
	    {
		$ecotaph->{$taxon_no}{$f} = $row->{$f};
		$ecotaph->{$taxon_no}{$b} = $row->{taxon_rank} if $get_basis;
		$ecotaph->{$taxon_no}{$r} = $row->{reference_no} if $get_basis;
		$references{$row->{reference_no}} = 1;
		$seen{$s} = 1 if defined $s;
	    }
	    
	    # Otherwise, if the given field is defined for the parent, we copy
	    # its value and auxiliary fields.  But we only do this if we
	    # haven't already seen the short name.
	    
	    elsif ( defined $parent_row->{$f} and (not defined $s or not $seen{$s}) )
	    {
		$ecotaph->{$taxon_no}{$f} = $parent_row->{$f};
		$ecotaph->{$taxon_no}{$b} = $parent_row->{$b} if $get_basis;
		$ecotaph->{$taxon_no}{$f} = $parent_row->{$f} if $get_basis;
	    }
	}
    }
    
    # $$$$ we still have to deal with min_body_mass, max_body_mass,
    # body_mass_estimate!!!   Also with preservation.
    
    # Turn the hash of references into a list.
    
    $ecotaph->{references} = [ keys %references ];
    
    # Return the result.
    
    return $ecotaph;
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
	$taxonomy->getTaxa('all_children', $base_taxon, 
			   { select => 'orig', 
			     fields => 'lft',
			     join_tables => 'LEFT JOIN ecotaph as qe on qe.taxon_no = [taxon_no]',
			     extra_fields => "qe.$field" });
    
    # If the first row (the base taxon) doesn't have a value for the
    # attribute, we will have to look up its parents.
    
    unless ( $result_list->[0]{$field} )
    {
	my $parent_list = 
	    $taxonomy->getTaxa('all_parents', $base_taxon, 
			       { select => 'orig',
				 exclude_self => 1,
				 join_tables => 'LEFT JOIN ecotaph as qe on qe.taxon_no = [taxon_no]',
				 extra_fields => "qe.$field" });
	
        foreach my $row ( @$parent_list )
	{
	    if ( $row->{$field} )
	    {
		$result_list->[0]{$field} = $row->{$field};
		last;
	    }
	}
    }
    
    my (%lookup, @default, %att, %from);
    
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
    
    for my $t ( @$result_list )	{
	$lookup{$t->{taxon_no}} = $att{$t->{lft}};
    }

    return \%lookup;
}


1;
