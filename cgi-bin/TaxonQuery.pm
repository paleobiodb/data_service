#
# TaxonQuery
# 
# A class that returns information from the PaleoDB database about a single
# taxon or a category of taxa.  This is a subclass of DataQuery.
# 
# Author: Michael McClennen

package TaxonQuery;

use strict;
use base 'DataQuery';


our ($PARAM_DESC_SINGLE) = <<DONE;
  name - return information about the named taxon (scientific name)
  rank - look for the taxon name only at the specified rank (taxon rank)
  id - return information about the specified taxon (positive integer identifier)
  taxon_name - synonym for 'name' (scientific name)
  taxon_rank - synonym for 'rank' (taxon rank)
  taxon_no - synonym for 'id' (positive integer identifier)
  exact - return information about the taxon specified, rather than about a preferred variant or synonym (boolean)
  
  show - return some or all of the specified information (comma-separated list)
            ref - include the publication reference for each taxon
            attr - include the attribution for each taxon
            code - include the nomenclatural code under which each taxon falls
            all - include all available information
DONE

our ($PARAM_REQS_SINGLE) = "You must specify either taxon_name or taxon_no.\n\nNote that unless 'exact' is specified, the returned taxon will be the currently preferred variant or synonym if one exists.";

our ($PARAM_CHECK_SINGLE) = { taxon_name => 1, taxon_rank => 1, taxon_no => 1, 
			      name => 1, rank => 1, id => 1, exact => 1, show => 1 };

our ($PARAM_DESC_MULTIPLE) = <<DONE;
  type - only return information about taxa of this type ('valid', 'synonyms', invalid', 'all')
  extant - only return information about extant or non-extant taxa (boolean)
  match - return information about all taxa whose names match this string (string with wildcards)
  rank - only return information about taxa whose rank falls within the specified range (taxonomic rank or range)
  id - return information about the specified taxa (comma-separated list of identifiers)
  taxon_no - synonym for 'id' (comma-separated list of identifiers)
  base_name - return information about the named taxon and its subtaxa (scientific name)
  base_rank - look for the base taxon name only at the specified rank (taxon rank)
  base_no - return information about the specified taxon and its subtaxa (positive integer identifier)
  
  show - return some or all of the specified information (comma-separated list)
            ref - include the publication reference for each taxon
            attr - include the attribution for each taxon
            code - include the nomenclatural code under which each taxon falls
            all - include all available information
DONE

#  leaf_name - return information about the named taxon and its supertaxa (scientific name)
#  leaf_rank - look for the child taxon name only at the specified rank (taxon rank)
#  leaf_no - return information about the named taxon and its supertaxa (positive integer identifier)


our ($PARAM_REQS_MULTIPLE) = "You must specify at least one of: type, match, rank, id, base_name, base_no, child_name, child_no.\n\nNote that 'type' defaults to 'valid' unless otherwise specified.";

our ($PARAM_CHECK_MULTIPLE) = { type => 1, extant => 1, match => 1, rank => 1,
				base_name => 1, base_rank => 1, base_no => 1,
				child_name => 1, child_rank => 1, child_no => 1, 
				id => 1, show => 1 };



# setParameters ( params )
# 
# This method accepts a hash of parameter values, filters them for correctness,
# and sets the appropriate fields of the query object.  It is designed to be
# called from a Dancer route, although that is not a requirement.

sub setParameters {
    
    my ($self, $params) = @_;
    
    # First tell our superclass to set any parameters it recognizes.
    
    $self->SUPER::setParameters($params);
    
    # If we are dealing with a 'single' query, set the appropriate parameters
    
    if ( $self->{version} eq 'single' )
    {
	$self->setParametersSingle($params);
    }
    
    # Otherwise, set the parameters for a 'multiple' query
    
    else
    {
	$self->setParametersMultiple($params);
    }
    
    # Then set common parameters
    
    # If 'id_only' is specified, then return only a list of id numbers.
    
    if ( defined $params->{id_only} and $params->{id_only} ne '' )
    {
	$self->{fetch_ids_only} = $self->parseBooleanParam($params->{id_only}, 'id_only');
    }
    
    # If 'show' is specified, then include additional information.
    
    if ( defined $params->{show} and $params->{show} ne '' )
    {
	my (@show) = split /\s*,\s*/, lc($params->{show});
	
	foreach my $s (@show)
	{
	    if ( $s eq 'ref' )
	    {
		$self->{show_ref} = 1;
	    }
	    
	    elsif ( $s eq 'code' )
	    {
		$self->{show_code} = 1;
	    }
	    
	    elsif ( $s eq 'attr' )
	    {
		$self->{show_attribution} = 1;
	    }
	    
	    elsif ( $s eq 'all' )
	    {
		$self->{show_ref} = 1;
		$self->{show_code} = 1;
		$self->{show_attribution} = 1;
	    }
	    
	    else
	    {
		$self->warn("Unknown value '$s' for show");
	    }
	}
    }
    
    return 1;
}


sub setParametersSingle {
    
    my ($self, $params) = @_;
    
    # The parameter 'id' is a synonym for 'taxon_no'.
    
    if ( defined $params->{id} and $params->{id} ne '' )
    {
	if ( defined $params->{taxon_no} and $params->{taxon_no} ne '' and 
	     $params->{id} ne $params->{taxon_no} )
	{
	    die "400 You cannot specify different values for 'id' and 'taxon_no'.\n"
	}
	
	$params->{taxon_no} = $params->{id};
    }
    
    # The parameter 'name' is a synonym for 'taxon_name'.
    
    if ( defined $params->{name} and $params->{name} ne '' )
    {
	if ( defined $params->{taxon_name} and $params->{taxon_name} ne '' and 
	     $params->{name} ne $params->{taxon_name} )
	{
	    die "400 You cannot specify different values for 'name' and 'taxon_name'.\n"
	}
	
	$params->{taxon_name} = $params->{name};
    }
    
    # The parameter 'rank' is a synonym for 'taxon_rank'.
    
    if ( defined $params->{rank} and $params->{rank} ne '' )
    {
	if ( defined $params->{taxon_rank} and $params->{taxon_rank} ne '' and 
	     $params->{rank} ne $params->{taxon_rank} )
	{
	    die "400 You cannot specify different values for 'rank' and 'taxon_rank'.\n"
	}
	
	$params->{taxon_rank} = $params->{rank};
    }
        
    # If 'taxon_name' is specified, then information is returned about the
    # specified taxon.  This parameter cannot be used at the same time as
    # 'taxon_no'.
    
    if ( defined $params->{taxon_name} and $params->{taxon_name} ne '' )
    {
	# Check to make sure that taxon_no was not specified at the same time
	
	if ( defined $params->{taxon_no} and $params->{taxon_no} ne '' )
	{
	    die "400 You may not specify 'taxon_name' and 'taxon_no' together.\n";
	}
	
	# Clean the parameter of everything except alphabetic characters
	# and spaces, since only those are valid in taxonomic names.
	
	if ( $params->{taxon_name} =~ /[^a-zA-Z\s]/ )
	{
	    die "400 The parameter 'taxon_name' may contain only characters from the Roman alphabet plus whitespace.\n";
	}
	
	$self->{base_taxon_name} = $params->{taxon_name};
	$self->{base_taxon_name} =~ s/\s+/ /g;
    }
    
    # If "taxon_no" is specified, then information is returned about the
    # specified taxon.  This parameter cannot be used at the same time as
    # 'taxon_name'.
    
    elsif ( defined $params->{taxon_no} and $params->{taxon_no} ne '' )
    {
	# First check to make sure that a valid value was provided
	
	if ( $params->{taxon_no} =~ /[^0-9]/ )
	{
	    die "400 You must provide a positive integer value for 'taxon_no'.\n";
	}
	
	$self->{base_taxon_no} = $params->{taxon_no} + 0;
    }
    
    # If neither was specified, return the "help" message.
    
    else
    {
	die "400 help\n";
    }
    
    # If "taxon_rank" is specified, then we only match the name if it matches
    # the specified rank.
    
    if ( defined $params->{taxon_rank} and $params->{taxon_rank} ne '' )
    {
	my $rank = lc $params->{taxon_rank};
	
	# This parameter cannot be used with taxon_no (since taxon_no uniquely
	# identifies a taxon, it cannot be used with any other taxon-selecting
	# parameters).
	
	if ( defined $params->{taxon_no} and $params->{taxon_no} ne '' )
	{
	    die "400 You may not use the 'taxon_rank' and 'taxon_no' parameters together.\n";
	}
	
	# Make sure that the value is one of the accepted ones.
	
	unless ( $DataQuery::ACCEPTED_RANK{$rank} )
	{
	    die "400 Unrecognized taxon rank '$rank'.";
	}
	
	$self->{base_taxon_rank} = $rank;
    }
    
    # If "exact" is specified, then we report information about the exact
    # taxon specified rather than its preferred variant or synonym.
    
    if ( defined $params->{exact} and $params->{exact} ne '' )
    {
	$self->{base_exact} = $self->parseBooleanParam($params->{exact}, 'exact');
    }
}


sub setParametersMultiple {
    
    my ($self, $params) = @_;
    
    # Make sure we have at least one parameter from the required set.
    
    my $required_param = 0;
    
    # If 'type' is specified, the results are restricted to certain types
    # of taxa.  Defaults to 'valid'.
    
    if ( defined $params->{type} and $params->{type} ne '' )
    {
	my $type = lc $params->{type};
	$required_param = 1;
	
	unless ( $type eq 'valid' or $type eq 'invalid' or $type eq 'synonyms' or $type eq 'all' )
	{
	    die "400 Unrecognized taxon type '$type'.\n";
	}
	
	$self->{filter_taxon_type} = $type;
    }
    
    else
    {
	$self->{filter_taxon_type} = 'valid';
    }
    
    # If 'extant' is specified, the results are restricted to taxa which are
    # either extant or not extant, depending upon the specified value.
    
    if ( defined $params->{extant} and $params->{extant} ne '' )
    {
	$self->{filter_extant} = $self->parseBooleanParam($params->{extant}, 'extant');
    }
    
    # If 'match' is specified, the results are restricted to taxa whose names
    # match the specified string.  SQL wildcards are allowed.
    
    if ( defined $params->{match} and $params->{match} ne '' )
    {
	$required_param = 1;
	
	# First check to make sure that a valid value was provided.
	
	if ( $params->{match} =~ /[^a-zA-Z_%\s]/ )
	{
	    die "400 The parameter 'taxon_match' may contain only characters from the Roman alphabet plus whitespace and the SQL wildcards '%' and '_'.\n";
	}
	
	$self->{filter_taxon_match} = $params->{match};
    }
    
    # If 'rank' is specified, the results are restricted to taxa whose rank
    # falls within the specified set.
    
    if ( defined $params->{rank} and $params->{rank} ne '' )
    {
	$required_param = 1;
	$self->{filter_taxon_rank} = $self->parseRankParam($params->{rank}, 'rank');
    }
    
    # If 'base_name' is specified, the results are restricted to the
    # specified taxon and its children.
    
    if ( defined $params->{base_name} and $params->{base_name} ne '' )
    {
	$required_param = 1;
	
	# Check to make sure that base_no was not specified at the same time
	
	if ( defined $params->{base_no} and $params->{base_no} ne '' )
	{
	    die "400 You may not specify 'base_name' and 'base_no' together.\n";
	}
	
	# Make sure the parameter contains nothing except alphabetic
	# characters and spaces, since only those are valid in taxonomic
	# names.
	
	if ( $params->{base_name} =~ /[^a-zA-Z\s]/ )
	{
	    die "400 The parameter 'base_name' may contain only characters from the Roman alphabet plus whitespace.\n";
	}
	
	$self->{base_taxon_name} = $params->{base_name};
	$self->{base_taxon_name} =~ s/\s+/ /g;
    }
    
    # If 'base_rank' is specified, then we only match the name if it
    # matches the specified rank.
    
    if ( defined $params->{base_rank} and $params->{base_rank} ne '' )
    {
	my $rank = lc $params->{base_rank};
	$required_param = 1;
	
	# This parameter cannot be used with taxon_no (since taxon_no uniquely
	# identifies a taxon, it cannot be used with any other taxon-selecting
	# parameters).
	
	if ( defined $params->{base_no} and $params->{base_no} ne '' )
	{
	    die "400 You may not use the 'base_rank' and 'base_no' parameters together.\n";
	}
	
	# Make sure that the value is one of the accepted ones.
	
	unless ( $DataQuery::ACCEPTED_RANK{$rank} )
	{
	    die "400 Unrecognized taxon rank '$rank'.";
	}
	
	$self->{base_taxon_rank} = $rank;
    }
    
    # If 'base_no' is specified, then the results are restricted to the
    # specified taxon and its children.
    
    if ( defined $params->{base_no} and $params->{base_no} ne '' )
    {
	$required_param = 1;
	
	# First check to make sure that a valid value was provided
	
	if ( $params->{base_no} =~ /[^0-9]/ )
	{
	    die "400 You must provide a positive integer value for 'taxon_no'.\n";
	}
	
	$self->{base_taxon_no} = $params->{base_no} + 0;
    }
    
    # If 'child_name' is specified, then the results are restricted to the
    # specified taxon and its parents.
    
    if ( defined $params->{child_name} and $params->{child_name} ne '' )
    {
	$required_param = 1;
	
	# Check to make sure that base_no was not specified at the same time
	
	if ( defined $params->{child_no} and $params->{child_no} ne '' )
	{
	    die "400 You may not specify 'child_name' and 'child_no' together.\n";
	}
	
	# Make sure the parameter contains nothing except alphabetic
	# characters and spaces, since only those are valid in taxonomic
	# names.
	
	if ( $params->{child_name} =~ /[^a-zA-Z\s]/ )
	{
	    die "400 The parameter 'child_name' may contain only characters from the Roman alphabet plus whitespace.\n";
	}
	
	$self->{child_taxon_name} = $params->{child_name};
	$self->{child_taxon_name} =~ s/\s+/ /g;	    
    }
    
    # If 'child_rank' is specified, then we only match the name if it
    # matches the specified rank.
    
    if ( defined $params->{child_rank} and $params->{child_rank} ne '' )
    {
	my $rank = lc $params->{child_rank};
	$required_param = 1;
	
	# This parameter cannot be used with taxon_no (since taxon_no uniquely
	# identifies a taxon, it cannot be used with any other taxon-selecting
	# parameters).
	
	if ( defined $params->{child_no} and $params->{child_no} ne '' )
	{
	    die "400 You may not use the 'child_rank' and 'child_no' parameters together.\n";
	}
	
	# Make sure that the value is one of the accepted ones.
	
	unless ( $DataQuery::ACCEPTED_RANK{$rank} )
	{
	    die "400 Unrecognized taxon rank '$rank'.";
	}
	
	$self->{child_taxon_rank} = $rank;
    }
    
    # If 'child_no' is specified, then the results are restricted to the
    # specified taxon and its parents.
    
    if ( defined $params->{child_no} and $params->{child_no} ne '' )
    {
	$required_param = 1;
	
	# First check to make sure that a valid value was provided
	
	if ( $params->{child_no} =~ /[^0-9]/ )
	{
	    die "400 You must provide a positive integer value for 'taxon_no'.\n";
	}
	
	$self->{child_taxon_no} = $params->{base_no} + 0;
    }
    
    # Now make sure we've specified at least one of the required parameters.
    
    unless ( $required_param )
    {
	die "400 help\n";
    }
}


# fetchSingle ( taxon_requested )
# 
# Query for all relevant information about the requested taxon.
# 
# Options may have been set previously by methods of this class or of the
# parent class DataQuery.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub fetchSingle {

    my ($self) = @_;
    my ($sql, @extra_fields);
    
    # Get ahold of a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # Then figure out which taxon we are looking for.  If we have a taxon_no,
    # we can use that.
    
    my $taxon_no;
    my $not_found_msg;
    
    if ( defined $self->{base_taxon_no} )
    {
	$taxon_no = $self->{base_taxon_no};
	
	# Unless we were directed to use the exact taxon specified, we need to
	# look up the senior synonym.  the senior synonym.  Because the
	# synonym_no field is not always correct, we may need to iterate this
	# query.  For efficiency, we do three iterations in one query.
	
	unless ( $self->{base_exact} )
	{
	    my $sql = "SELECT t3.synonym_no
	     FROM taxa_tree_cache t1 JOIN taxa_tree_cache t2 ON t2.taxon_no = t1.synonym_no
				     JOIN taxa_tree_cache t3 ON t3.taxon_no = t2.synonym_no
	     WHERE t1.taxon_no = ?";
	    
	    ($taxon_no) = $dbh->selectrow_array($sql, undef, $taxon_no);
	}
	
	$not_found_msg = "Taxon number $self->{base_taxon_no} was not found in the database";
    }
    
    # Otherwise, we must have a taxon name.  So look for that.
    
    else
    {
	my $rank_clause = '';
	my @args = $self->{base_taxon_name};
	
	$not_found_msg = "Taxon '$self->{base_taxon_name}' was not found in the database";
	
	# If a rank was specified, add a clause to narrow it down.
	
	if ( defined $self->{base_taxon_rank} )
	{
	    $rank_clause = "and a.taxon_rank = ?";
	    push @args, $self->{base_taxon_rank};
	    
	    $not_found_msg .= " at rank '$self->{base_taxon_rank}'";
	}
	
	# If we were directed to return the exact taxon specified, we just
	# look it up in the authorities table.
	
	if ( $self->{base_exact} )
	{
	    my $sql = "SELECT a.taxon_no
		FROM authorities a WHERE a.taxon_name = ? $rank_clause";
	    
	    ($taxon_no) = $dbh->selectrow_array($sql, undef, @args);
	}
	
	# Otherwise, we need to find the senior synonym (see above).
	
	else
	{
	    my $sql = "SELECT t3.synonym_no
		FROM authorities a JOIN taxa_tree_cache t1 USING (taxon_no)
				JOIN taxa_tree_cache t2 ON t2.taxon_no = t1.synonym_no
				JOIN taxa_tree_cache t3 ON t3.taxon_no = t2.synonym_no
		WHERE a.taxon_name = ? $rank_clause";
	    
	    ($taxon_no) = $dbh->selectrow_array($sql, undef, @args);
	}
    }
    
    # If we haven't found a record, the result set will be empty.
    
    unless ( defined $taxon_no and $taxon_no > 0 )
    {
	return;
    }
    
    # Now add the fields necessary to show the requested info.
    
    if ( defined $self->{show_ref} )
    {
	push @extra_fields, "r.author1init r_ai1", "r.author1last r_al1",
	    "r.author2init r_ai2", "r.author2last r_al2", "r.otherauthors r_oa",
		"r.pubyr r_pubyr", "r.reftitle r_reftitle", "r.pubtitle r_pubtitle",
		    "r.editors r_editors", "r.pubvol r_pubvol", "r.pubno r_pubno",
			"r.firstpage r_fp", "r.lastpage r_lp";
    }
    
    if ( defined $self->{show_attribution} )
    {
	push @extra_fields, 
	    "if (a.pubyr IS NOT NULL AND a.pubyr != '' AND a.pubyr != '0000', a.pubyr, IF (a.ref_is_authority = 'YES', r.pubyr, '')) a_pubyr",
		"if (a.ref_is_authority = 'YES', r.author1last, a.author1last) a_al1",
		    "if (a.ref_is_authority = 'YES', r.author2last, a.author2last) a_al2",
			"if (a.ref_is_authority = 'YES', r.otherauthors, a.otherauthors) a_ao";
    }
    
    # If we need to show the nomenclatural code under which the taxon falls,
    # include the necessary fields.  We also need to get the tree-range for
    # Metazoa and Plantae.
    
    if ( defined $self->{show_code} )
    {
	push @extra_fields, "t.lft";
	$self->getCodeRanges();
    }
    
    my $extra_fields = join(', ', @extra_fields);
    $extra_fields = ", " . $extra_fields if $extra_fields ne '';
    
    # Next, fetch basic info about the taxon.
    
    $sql = 
	"SELECT a.taxon_no, a.taxon_rank, a.taxon_name, a.common_name, a.extant, o.status, o.spelling_reason,
		pa.taxon_no as parent_no, pa.taxon_name as parent_name, t.spelling_no, t.synonym_no, t.orig_no,
		xa.taxon_no as accepted_no, xa.taxon_name as accepted_name
		$extra_fields
         FROM authorities a JOIN taxa_tree_cache t USING (taxon_no)
	       	LEFT JOIN refs r USING (reference_no)
		LEFT JOIN opinions o USING (opinion_no)
		LEFT JOIN authorities xa ON (xa.taxon_no = CASE
		    WHEN status <> 'belongs to' AND status <> 'invalid subgroup of' THEN o.parent_spelling_no
		    WHEN t.taxon_no <> t.synonym_no THEN t.synonym_no END)
		LEFT JOIN (taxa_tree_cache pt JOIN authorities pa ON (pa.taxon_no = pt.synonym_no))
		    ON (pt.taxon_no = if(o.status = 'belongs to', o.parent_spelling_no, null))
         WHERE a.taxon_no = ?";
    
    $self->{main_sth} = $dbh->prepare($sql);
    $self->{main_sth}->execute($taxon_no);
    
    return 1;
}


# fetchMultiple ( )
# 
# Query the database for basic info about all taxa satisfying the conditions
# previously specified by a call to setParameters.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub fetchMultiple {

    my ($self) = @_;
    my ($sql);
    my (@filter_list, @extra_tables, @extra_fields);
    my ($taxon_limit) = "";
    my ($process_as_parent_list) = 0;
    
    # Get ahold of a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # If a query limit has been specified, construct the appropriate SQL string.
    
    if ( defined $self->{limit_results} )
    {
	$taxon_limit = "LIMIT " . ($self->{limit_results} + 0);
    }
    
    # Now process the various parameters that limit the scope of the query:
    
    # If a restriction has been entered on the category of taxa to be
    # returned, add the appropriate filter.
    
    if ( defined $self->{filter_taxon_type} )
    {
	if ( $self->{filter_taxon_type} eq 'valid' ) {
	    push @filter_list, "o.status = 'belongs to'";
	    push @filter_list, "t.taxon_no = t.synonym_no";
	}
	elsif ( $self->{filter_taxon_type} eq 'synonyms' ) {
	    push @filter_list, "o.status in ('belongs to', 'subjective synonym of', 'objective synonym of')";
	    push @filter_list, "t.taxon_no = t.spelling_no";
	}
	elsif ( $self->{filter_taxon_type} eq 'invalid' ) {
	    push @filter_list, "o.status not in ('belongs to', 'subjective synonym of', 'objective synonym of')";
	}
    }
    
    # If a restriction has been specified for extant or non-extant taxa, add
    # the appropriate filter.
    
    if ( defined $self->{filter_extant} )
    {
	if ( $self->{filter_extant} )
	{
	    push @filter_list, "a.extant = 'yes'";
	}
	
	else
	{
	    push @filter_list, "a.extant = 'no'";
	}
    }
    
    # If a text match has been specified, add the appropriate filter.
    
    if ( defined $self->{filter_taxon_match} )
    {
	push @filter_list, "a.taxon_name like '$self->{filter_taxon_match}'";
    }
    
    # If a rank or range of ranks has been specified, add the appropriate
    # filter. 
    
    if ( ref $self->{filter_taxon_rank} eq 'ARRAY' )
    {
	my (@disjunction, @in_list);
	
	foreach my $r (@{$self->{filter_taxon_rank}})
	{
	    if ( ref $r eq 'ARRAY' )
	    {
		push @disjunction, "a.taxon_rank >= $r->[0] and a.taxon_rank <= $r->[1]";
	    }
	    
	    else
	    {
		push @in_list, $r;
	    }
	}
	
	if ( @in_list )
	{
	    push @disjunction, "a.taxon_rank in (" . join(',', @in_list) . ")";
	}
	
	if ( @disjunction )
	{
	    push @filter_list, '(' . join(' or ', @disjunction) . ')';
	}
    }
    
    # If a base taxon has been specified by taxon_no, find it and add the appropriate
    # filter.
    
    if ( defined $self->{base_taxon_no} )
    {
	my $sql = "SELECT t3.lft, t3.rgt
	     FROM taxa_tree_cache t1 JOIN taxa_tree_cache t2 ON t2.taxon_no = t1.synonym_no
				     JOIN taxa_tree_cache t3 ON t3.taxon_no = t2.synonym_no
	     WHERE t1.taxon_no = ?";
	
	my ($lft, $rgt) = $dbh->selectrow_array($sql, undef, $self->{base_taxon_no});
	
	# If we can't find the base taxon, the result set will be empty.
	
	unless ( defined $lft and $lft > 0 )
	{
	    return;
	}
	
	# Otherwise, we select the appropriate range of taxa.
	
	push @filter_list, "t.lft >= $lft", "t.lft <= $rgt";
    }
    
    # If a base taxon has been specified by taxon_name, find it and add the
    # appropriate filter.
    
    if ( defined $self->{base_taxon_name} )
    {
	my $rank_clause = '';
	my @args = $self->{base_taxon_name};
	
	my $not_found_msg = "Taxon '$self->{base_taxon_name}' was not found in the database";
	
	# If a rank was specified, add a clause to narrow it down.
	
	if ( defined $self->{base_taxon_rank} )
	{
	    $rank_clause = "and a.taxon_rank = ?";
	    push @args, $self->{base_taxon_rank};
	    
	    $not_found_msg .= " at rank '$self->{base_taxon_rank}'";
	}
	
	my $sql = "SELECT t3.lft, t3.rgt
		FROM authorities a JOIN taxa_tree_cache t1 USING (taxon_no)
				JOIN taxa_tree_cache t2 ON t2.taxon_no = t1.synonym_no
				JOIN taxa_tree_cache t3 ON t3.taxon_no = t2.synonym_no
		WHERE a.taxon_name = ? $rank_clause";
	    
	my ($lft, $rgt) = $dbh->selectrow_array($sql, undef, @args);
	
	# If we can't find the base taxon, the result set will be empty.
	
	unless ( defined $lft and $lft > 0 )
	{
	    return;
	}
	
	# Otherwise, select the appropriate range of taxa.
	
	push @filter_list, "t.lft >= $lft", "t.lft <= $rgt";
    }
    
    # $$$ add child_taxon_no/child_taxon_name
    
    # Add the extra fields necessary to show the requested info
    
    if ( defined $self->{show_ref} )
    {
	push @extra_fields, "r.author1init r_ai1", "r.author1last r_al1",
	    "r.author2init r_ai2", "r.author2last r_al2", "r.otherauthors r_oa",
		"r.pubyr r_pubyr", "r.reftitle r_reftitle", "r.pubtitle r_pubtitle",
		    "r.editors r_editors", "r.pubvol r_pubvol", "r.pubno r_pubno",
			"r.firstpage r_fp", "r.lastpage r_lp";
    }
    
    if ( defined $self->{show_attribution} )
    {
	push @extra_fields, 
	    "if (a.pubyr IS NOT NULL AND a.pubyr != '' AND a.pubyr != '0000', a.pubyr, IF (a.ref_is_authority = 'YES', r.pubyr, '')) a_pubyr",
		"if (a.ref_is_authority = 'YES', r.author1last, a.author1last) a_al1",
		    "if (a.ref_is_authority = 'YES', r.author2last, a.author2last) a_al2",
			"if (a.ref_is_authority = 'YES', r.otherauthors, a.otherauthors) a_ao";
    }
    
    if ( defined $self->{show_code} )
    {
	push @extra_fields, "t.lft";
	$self->getCodeRanges();
    }
    
    # Now construct the filter expression and extra_tables expression
    
    my $taxon_filter = join(' and ', @filter_list);
    $taxon_filter = "WHERE $taxon_filter" if $taxon_filter ne '';
    
    # and the extra_fields expression
    
    my $extra_fields = join(', ', @extra_fields);
    $extra_fields = ", " . $extra_fields if $extra_fields ne '';
    
    # Now construct and execute the SQL statement that will be used to fetch
    # the desired information from the database.
    
    $self->{main_sql} = "
	SELECT a.taxon_no, a.taxon_rank, a.taxon_name, a.common_name, a.extant, o.status, o.spelling_reason,
		pa.taxon_no as parent_no, pa.taxon_name as parent_name, t.spelling_no, t.synonym_no, t.orig_no,
		xa.taxon_no as accepted_no, xa.taxon_name as accepted_name
		$extra_fields
        FROM authorities a JOIN taxa_tree_cache t USING (taxon_no)
		LEFT JOIN refs r USING (reference_no)
		LEFT JOIN opinions o USING (opinion_no)
		LEFT JOIN authorities xa ON (xa.taxon_no = CASE
		    WHEN status <> 'belongs to' AND status <> 'invalid subgroup of' THEN o.parent_spelling_no
		    WHEN t.taxon_no <> t.synonym_no THEN t.synonym_no END)
		LEFT JOIN (taxa_tree_cache pt JOIN authorities pa ON (pa.taxon_no = pt.synonym_no))
		    ON (pt.taxon_no = if(o.status = 'belongs to', o.parent_spelling_no, null))
	$taxon_filter ORDER BY t.lft ASC $taxon_limit";
    
    # Also construct a statement to fetch the result count if necessary.
    
    $self->{count_sql} = "
	SELECT count(*)
	FROM authorities a JOIN taxa_tree_cache t USING (taxon_no)
		LEFT JOIN opinions o USING (opinion_no)
	$taxon_filter";
    
    # Now prepare and execute the main statement.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    return 1;
}


sub processParents {

    # Make sure we know the senior synonym of this taxon (this is relevant
    # only if the 'fetch_exact' flag is true) so that the parent list will
    # turn out properly.
    
    # my ($senior_synonym) = $main_row->{synonym_no};
    
    # # Now fetch all parent info
    
    # $sql = 
    #    "SELECT a.taxon_no, a.taxon_name, a.taxon_rank, a.common_name, a.extant, 
    # 		pt.synonym_no as parent_no, t.orig_no,
    # 		IF (a.pubyr IS NOT NULL AND a.pubyr != '' AND a.pubyr != '0000', 
    # 		    a.pubyr, IF (a.ref_is_authority = 'YES', r.pubyr, '')) pubyr,
    # 		IF (a.ref_is_authority = 'YES', r.author1last, a.author1last) author1,
    # 		IF (a.ref_is_authority = 'YES', r.author2last, a.author2last) author2,
    # 		IF (a.ref_is_authority = 'YES', r.otherauthors, a.otherauthors) otherauthors
    # 	FROM taxa_list_cache l JOIN taxa_tree_cache t ON (t.taxon_no = l.parent_no)
    # 		JOIN authorities a ON a.taxon_no = t.taxon_no
    # 		LEFT JOIN refs r USING (reference_no)
    # 		LEFT JOIN opinions o USING (opinion_no)
    # 		LEFT JOIN taxa_tree_cache pt ON (pt.taxon_no = o.parent_spelling_no)
    # 	WHERE l.child_no = ? AND l.parent_no <> l.child_no ORDER BY t.lft ASC";

    # $sth = $dbh->prepare($sql);
    # $sth->execute($senior_synonym);
    
    # my $parent_list = $sth->fetchall_arrayref({});
    
    # # Run through the parent list and note when we reach the last
    # # kingdom-level taxon.  Any entries before that point are dropped 
    # # [note: TaxonInfo.pm, line 1316]
    
    # my $last_kingdom = 0;
    
    # for (my $i = 0; $i < scalar(@$parent_list); $i++)
    # {
    # 	$last_kingdom = $i if $parent_list->[$i]{taxon_rank} eq 'kingdom';
    # }
    
    # splice(@$parent_list, 0, $last_kingdom) if $last_kingdom > 0;
    
    # $self->{parents} = $parent_list;
    
    # # If this is a species or genus, we may need to rewrite the parent list
    # # according to the name given for the main taxon.
    
    # if ( $self->{rewrite_classification} )
    # {
    # 	my ($genus, $subgenus, $species, $subspecies) = 
    # 	    interpretSpeciesName($main_row->{taxon_name});
	
    # 	# Create a new parent list, and copy all taxa of family or higher rank
    # 	# to it.  Taxa of genus and lower rank are used to populate the
    # 	# taxon_no_by_rank table.
	
    # 	$self->{parents} = [];
    # 	my ($taxon_row_by_rank) = {};
	
    # 	foreach my $row (@$parent_list)
    # 	{
    # 	    unless ( $row->{taxon_rank} =~ /species|genus/ )
    # 	    {
    # 		push @{$self->{parents}}, $row;
    # 	    }
	    
    # 	    else
    # 	    {
    # 		$taxon_row_by_rank->{$row->{taxon_rank}} = $row;
    # 	    }
    # 	}
	
    # 	# Now add the genus, but with the genus name extracted from the current
    # 	# taxon's name.  If one wasn't found, use the current taxon.
	
    # 	if ( defined $taxon_row_by_rank->{'genus'} )
    # 	{
    # 	    my $genus_row = $taxon_row_by_rank->{'genus'};
    # 	    $genus_row->{taxon_name} = $genus;
    # 	    push @{$self->{parents}}, $genus_row;
    # 	}
	
    # 	else
    # 	{
    # 	    push @{$self->{parents}}, $main_row;
    # 	}
	
    # 	# Then, add the subgenus, species and subspecies only if they occur in
    # 	# the current taxon's name.
	
    # 	if ( defined $subgenus and $subgenus ne '' )
    # 	{
    # 	    my $subgenus_row = ( defined $taxon_row_by_rank->{'subgenus'} ?
    # 				 $taxon_row_by_rank->{'subgenus'} :
    # 				 { taxon_rank => 'subgenus' } );
    # 	    $subgenus_row->{taxon_name} = "$genus ($subgenus)";
    # 	    push @{$self->{parents}}, $subgenus_row;
    # 	}
	
    # 	if ( defined $species and $species ne '' )
    # 	{
    # 	    my $species_row = ( defined $taxon_row_by_rank->{'species'} ?
    # 				$taxon_row_by_rank->{'species'} :
    # 				{ taxon_rank => 'species' } );
    # 	    $species_row->{taxon_name} = "$genus $species";
    # 	    if ( defined $subgenus and $subgenus ne '' ) {
    # 		$species_row->{taxon_name} = "$genus ($subgenus) $species";
    # 	    }
    # 	    push @{$self->{parents}}, $species_row;
    # 	}
	
    # 	if ( defined $subspecies and $subspecies ne '' )
    # 	{
    # 	    my $subspecies_row = ( defined $taxon_row_by_rank->{'subspecies'} ?
    # 				   $taxon_row_by_rank->{'subspecies'} :
    # 				   { taxon_rank => 'subspecies' } );
    # 	    $subspecies_row->{taxon_name} = $main_row->{taxon_name};
    # 	    push @{$self->{parents}}, $subspecies_row;
    # 	}
    # }
    
    # # Take off the last entry on the parent list if it is a duplicate of the
    # # main row, unless we have been told not to.
    
    # unless ( $self->{dont_trim_parent_list} or scalar(@{$self->{parents}}) == 0 )
    # {
    # 	pop @{$self->{parents}} if $self->{parents}[-1]{taxon_rank} eq $self->{main_row}{taxon_rank};
    # }
    
    # Return success
    

}

# getCodeRanges ( )
# 
# Fetch the ranges necessary to determine which nomenclatural code (i.e. ICZN,
# ICN) applies to any given taxon.  This is only done if that information is
# asked for.

my @codes = ('Metazoa', 'Animalia', 'Plantae', 'Biliphyta', 'Metaphytae',
	     'Fungi', 'Cyanobacteria');

my $codes = { Metazoa => { code => 'ICZN'}, 
	      Animalia => { code => 'ICZN'},
	      Plantae => { code => 'ICN'}, 
	      Biliphyta => { code => 'ICN'},
	      Metaphytae => { code => 'ICN'},
	      Fungi => { code => 'ICN'},
	      Cyanobacteria => { code => 'ICN' } };

sub getCodeRanges {

    my ($self) = @_;
    my ($dbh) = $self->{dbh};
    
    $self->{code_ranges} = $codes;
    $self->{code_list} = \@codes;
    
    my $code_name_list = "'" . join("','", @codes) . "'";
    
    my $code_range_query = $dbh->prepare("
	SELECT taxon_name, lft, rgt
	FROM taxa_tree_cache join authorities using (taxon_no)
	WHERE taxon_name in ($code_name_list)");
    
    $code_range_query->execute();
    
    while ( my($taxon, $lft, $rgt) = $code_range_query->fetchrow_array() )
    {
	$codes->{$taxon}{lft} = $lft;
	$codes->{$taxon}{rgt} = $rgt;
    }
}


# processRecord ( row )
# 
# This routine takes a hash representing one result row, and does some
# processing before the output is generated.  The information fetched from the
# database needs to be refactored a bit in order to match the Darwin Core
# standard we are using for output.

sub processRecord {
    
    my ($self, $row) = @_;
    
    # Interpret the status info based on the code stored in the database.  The
    # code as stored in the database encompasses both taxonomic and
    # nomenclatural status info, which needs to be separated out.  In
    # addition, we need to know whether to report an "acceptedUsage" taxon
    # (i.e. senior synonym or proper spelling).
    
    my ($taxonomic, $report_accepted, $nomenclatural) = interpretStatusCode($row->{status});
    
    # Override the status code if the synonym_no is different from the
    # taxon_no.  This is necessary because sometimes the opinion record that
    # was used to build this part of the hierarchy indicates a 'belongs to'
    # relationship (which normally indicates a valid taxon) but the
    # taxa_tree_cache record indicates a different synonym number.  In this
    # case, the taxon is in fact no valid but is a junior synonym or
    # misspelling.  If spelling_no and synonym_no are equal, it's a
    # misspelling.  Otherwise, it's a junior synonym.
    
    if ( $taxonomic eq 'valid' && $row->{synonym_no} ne $row->{taxon_no} )
    {
	if ( $row->{spelling_no} eq $row->{synonym_no} )
	{
	    $taxonomic = 'invalid' unless $row->{spelling_reason} eq 'recombination';
	    $nomenclatural = $row->{spelling_reason};
	}
	else
	{
	    $taxonomic = 'synonym';
	}
    }
    
    # If no status was found, assume 'valid'.  We need to do this because of
    # the many entries in taxa_tree_cache that don't reference an opinion.
    
    $taxonomic = 'valid' unless $taxonomic;
    
    # Put the two status strings into the row record.
    
    $row->{taxonomic} = $taxonomic;
    $row->{nomenclatural} = $nomenclatural;
    
    # Determine the nomenclatural code that has jurisidiction, if that was
    # requested. 
    
    if ( defined $row->{lft} )
    {
	$self->determineNomenclaturalCode($row);
    }
    
    # Create a publication reference if that data was included in the query
    
    if ( exists $row->{r_pubtitle} )
    {
	$self->generateReference($row);
    }
    
    # Create an attribution if that data was incluced in the query
    
    if ( exists $row->{a_pubyr} )
    {
	$self->generateAttribution($row);
    }
}


# determineNomenclaturalCode ( row )
# 
# Determine which nomenclatural code the given row's taxon falls under

sub determineNomenclaturalCode {
    
    my ($self, $row) = @_;

    my ($lft) = $row->{lft} || return;
    
    # Anything with a rank of 'unranked clade' falls under PhyloCode.
    
    if ( defined $row->{taxon_rank} && $row->{taxon_rank} eq 'unranked clade' )
    {
	$row->{nom_code} = 'PhyloCode';
	return;
    }
    
    # For all other taxa, we go through the list of known ranges in
    # taxa_tree_cache and use the appropriate code.
    
    foreach my $taxon (@{$self->{code_list}})
    {
	my $range = $self->{code_ranges}{$taxon};
	
	if ( $lft >= $range->{lft} && $lft <= $range->{rgt} )
	{
	    $row->{nom_code} = $range->{code};
	    last;
	}
    }
    
    # If this taxon does not fall within any of the ranges, we leave the
    # nom_code field empty.
}


# generateRecord ( row, options )
# 
# Return a string representing one row of the result, in the selected output
# format.  The option 'is_first' indicates that this is the first
# record, which is significant for JSON output (it controls whether or not to
# output an initial comma, in that case).

sub generateRecord {

    my ($self, $row, %options) = @_;
    
    # If the content type is XML, then we need to check whether the result
    # includes a list of parents.  If so, because of the inflexibility of XML
    # and Darwin Core, we cannot output a hierarchical list.  The best we can
    # do is to output all of the parent records first, before the main record
    # (see http://eol.org/api/docs/hierarchy_entries).
    
    if ( $self->{output_format} eq 'xml' )
    {
	my $output = '';
	
	# If there are any parents, output them first with the "short record"
	# flag to indicate that many of the fields should be elided.  These
	# are not the primary object of the query, so less information needs
	# to be provided about them.
	
	if ( defined $self->{parents} && ref $self->{parents} eq 'ARRAY' )
	{
	    foreach my $parent_row ( @{$self->{parents}} )
	    {
		$output .= $self->emitTaxonXML($parent_row, 1);
	    }
	}
    
	# Now, we output the main record.
	
	$output .= $self->emitTaxonXML($row, 0);
	
	return $output;
    }
    
    # Otherwise, it must be JSON.  In that case, we need to insert a comma if
    # this is not the first record.  The subroutine emitTaxonJSON() will also
    # output the parent records, if there are any, as a sub-array.
    
    my $insert = ($options{is_first} ? '' : ',');
    
    return $insert . $self->emitTaxonJSON($row, $self->{parents});
}


# emitTaxonXML ( row, short_record )
# 
# Returns a string representing the given record (row) in Darwin Core XML
# format.  If 'short_record' is true, suppress certain fields.

my %fixbracket = ( '((' => '<', '))' => '>' );

sub emitTaxonXML {
    
    no warnings;
    
    my ($self, $row, $short_record) = @_;
    my $output = '';
    my @remarks = ();
    
    $output .= '  <dwc:Taxon>' . "\n";
    $output .= '    <dwc:taxonID>' . $row->{taxon_no} . '</dwc:taxonID>' . "\n";
    $output .= '    <dwc:taxonRank>' . $row->{taxon_rank} . '</dwc:taxonRank>' . "\n";
    
    # Taxon names shouldn't contain any invalid characters, but just in case...
    
    $output .= '    <dwc:scientificName>' . DataQuery::xml_clean($row->{taxon_name}) . 
	'</dwc:scientificName>' . "\n";
    
    # species have extra fields to indicate which genus, etc. they belong to
    
    if ( $row->{taxon_rank} =~ /species/ && !$short_record ) {
	my ($genus, $subgenus, $species, $subspecies) = interpretSpeciesName($row->{taxon_name});
	$output .= '    <dwc:genus>' . $genus . '</dwc:genus>' . "\n" if defined $genus;
	$output .= '    <dwc:subgenus>' . "$genus ($subgenus)" . '</dwc:subgenus>' . "\n" if defined $subgenus;
	$output .= '    <dwc:specificEpithet>' . $species . '</dwc:specificEpithet>' . "\n" if defined $species;
	$output .= '    <dwc:infraSpecificEpithet>' . $subspecies . '</dwc:infraSpecificEpithet>' if defined $subspecies;
    }
    
    if ( defined $row->{parent_no} && $row->{parent_no} > 0 ) {
	$output .= '    <dwc:parentNameUsageID>' . $row->{parent_no} . '</dwc:parentNameUsageID>' . "\n";
    }
    
    if ( defined $row->{parent_name} && $self->{show_parent_names} ) {
    	$output .= '    <dwc:parentNameUsage>' . DataQuery::xml_clean($row->{parent_name}) . 
	    '</dwc:parentNameUsage>' . "\n";
    }
    
    if ( defined $row->{attribution} && $row->{attribution} ne '' )
    {
	$output .= '    <dwc:scientificNameAuthorship>' . DataQuery::xml_clean($row->{attribution}) .
	    '</dwc:scientificNameAuthorship>' . "\n";
    }
    
    if ( defined $row->{author1} && $row->{author1} ne '' ) {
	my $authorship = formatAuthorName($row->{author1}, $row->{author2}, $row->{otherauthors},
					  $row->{pubyr});
	$authorship = "($authorship)" if defined $row->{orig_no} &&
	    $row->{orig_no} > 0 && $row->{orig_no} != $row->{taxon_no};
	$authorship = DataQuery::xml_clean($authorship);
	$output .= '    <dwc:scientificNameAuthorship>' . $authorship . '</dwc:scientificNameAuthorship>' . "\n";
    }
    
    if ( defined $row->{taxonomic} ) {
	$output .= '    <dwc:taxonomicStatus>' . $row->{taxonomic} . '</dwc:taxonomicStatus>' . "\n";
    }
    
    if ( defined $row->{nomenclatural} ) {
	$output .= '    <dwc:nomenclaturalStatus>' . $row->{nomenclatural} . '</dwc:nomenclaturalStatus>' . "\n";
    }
    
    if ( defined $row->{nom_code} ) {
	$output .= '    <dwc:nomenclaturalCode>' . $row->{nom_code} . '</dwc:nomenclaturalCode>' . "\n";
    }
    
    if ( defined $row->{accepted_no} ) {
	$output .= '    <dwc:acceptedNameUsageID>' . $row->{accepted_no} . '</dwc:acceptedNameUsageID>' . "\n";
    }
    
    if ( defined $row->{accepted_name} ) {
	# taxon names shouldn't contain any wide characters, but just in case...
	$output .= '    <dwc:acceptedNameUsage>' . DataQuery::xml_clean($row->{accepted_name}) . 
	    '</dwc:acceptedNameUsage>' . "\n";
    }
    
    if ( defined $row->{pubref} ) {
	my $pubref = DataQuery::xml_clean($row->{pubref});
	# We now need to translate, i.e. ((b)) to <b>.  This is done after
	# xml_clean, because otherwise <b> would be turned into &lt;b&gt;
	if ( $pubref =~ /\(\(/ )
	{
	    #$row->{pubref} =~ s/(\(\(|\)\))/$fixbracket{$1}/eg;
	    #actually, we're just going to take them out for now
	    $pubref =~ s/\(\(\/?\w*\)\)//g;
	}
	$output .= '    <dwc:namePublishedIn>' . $pubref . '</dwc:namePublishedIn>' . "\n";
    }
    
    if ( defined $row->{common_name} && $row->{common_name} ne '' ) {
	$output .= '    <dwc:vernacularName xml:lang="en">' . 
	    DataQuery::xml_clean($row->{common_name}) . '</dwc:vernacularName>' . "\n";
    }
    
    if ( defined $row->{extant} and $row->{extant} =~ /(yes|no)/i ) {
	push @remarks, "extant: $1";
    }
    
    if ( @remarks ) {
	my $remarks = join('; ', @remarks);
	$output .= '    <dwc:taxonRemarks>' . $remarks . '</dwc:taxonRemarks>' . "\n";
    }
    
    $output .= '  </dwc:Taxon>' . "\n";
}


# emitTaxonJSON ( row, parents, short_record )
# 
# Return a string representing the given taxon record (row) in JSON format.
# If 'parents' is specified, it should be an array of hashes each representing
# a parent taxon record.  If 'short_record' is true, then some fields will be
# suppressed.

sub emitTaxonJSON {
    
    no warnings;
    
    my ($self, $row, $parents, $short_record) = @_;
    
    my $output = '';
    
    $output .= '{"taxonID":"' . $row->{taxon_no} . '"'; 
    $output .= ',"taxonRank":"' . $row->{taxon_rank} . '"';
    $output .= ',"scientificName":"' . DataQuery::json_clean($row->{taxon_name}) . '"';
    
    if ( defined $row->{attribution} && $row->{attribution} ne '' )
    {
	$output .= ',"scientificNameAuthorship":"' . DataQuery::json_clean($row->{attribution}) . '"';
    }
    
    if ( defined $row->{author1} && $row->{author1} ne '' ) {
	my $authorship = formatAuthorName($row->{author1}, $row->{author2}, $row->{otherauthors},
					  $row->{pubyr});
	$authorship = "($authorship)" if defined $row->{orig_no} &&
	    $row->{orig_no} > 0 && $row->{orig_no} != $row->{taxon_no};
	$output .= ',"scientificNameAuthorship":"' . DataQuery::json_clean($authorship) . '"';
    }
    
    if ( defined $row->{parent_no} && $row->{parent_no} > 0 ) {
	$output .= ',"parentNameUsageID":"' . $row->{parent_no} . '"';
    }
    
    if ( defined $row->{parent_name} && $self->{show_parent_names} ) {
	$output .= ',"parentNameUsage":"' . DataQuery::json_clean($row->{parent_name}) . '"';
    }
    
    if ( defined $row->{taxonomic} ) {
	$output .= ',"taxonomicStatus":"' . $row->{taxonomic} . '"';
    }
    
    if ( defined $row->{nomenclatural} ) {
	$output .= ',"nomenclaturalStatus":"' . $row->{nomenclatural} . '"';
    }
    
    if ( defined $row->{nom_code} ) {
	$output .= ',"nomenclaturalCode":"' . $row->{nom_code} . '"';
    }
    
    if ( defined $row->{accepted_no} ) {
	$output .= ',"acceptedNameUsageID":"' . $row->{accepted_no} . '"';
    }
    
    if ( defined $row->{accepted_name} ) {
	$output .= ',"acceptedNameUsage":"' . DataQuery::json_clean($row->{accepted_name}) . '"';
    }
    
    if ( defined $row->{pubref} )
    {
	$output .= ',"namePublishedIn":"' . DataQuery::json_clean($row->{pubref}) . '"';
    }
    
    if ( defined $row->{common_name} ne '' && $row->{common_name} ne '' ) {
	$output .= ',"vernacularName":"' . DataQuery::json_clean($row->{common_name}) . '"';
    }
    
    if ( defined $row->{extant} and $row->{extant} =~ /(yes|no)/i ) {
	$output .= ',"extant":"' . $1 . '"';
    }
    
    if ( defined $parents && ref $parents eq 'ARRAY' )
    {
	my $is_first_parent = 1;
	$output .= ',"ancestors":[';
	
	foreach my $parent_row ( @{$self->{parents}} )
	{
	    $output .= ( $is_first_parent ? "\n" : "\n," );
	    $output .= $self->emitTaxonJSON($parent_row);
	    $is_first_parent = 0;
	}
	
	$output .= ']';
    }
    
    $output .= "}";
    return $output;
}


# formatAuthorName ( author1last, author2last, otherauthors, pubyr, orig_no )
# 
# Format the given info into a single string that can be output as the
# attribution of a taxon.

sub formatAuthorName {
    
    no warnings;
    
    my ($author1last, $author2last, $otherauthors, $pubyr) = @_;
    
    my $a1 = defined $author1last ? $author1last : '';
    my $a2 = defined $author2last ? $author2last : '';
    
    $a1 =~ s/( Jr)|( III)|( II)//;
    $a1 =~ s/\.$//;
    $a1 =~ s/,$//;
    $a1 =~ s/\s*$//;
    $a2 =~ s/( Jr)|( III)|( II)//;
    $a2 =~ s/\.$//;
    $a2 =~ s/,$//;
    $a2 =~ s/\s*$//;
    
    my $shortRef = $a1;
    
    if ( $otherauthors ne '' ) {
        $shortRef .= " et al.";
    } elsif ( $a2 ) {
        # We have at least 120 refs where the author2last is 'et al.'
        if ( $a2 !~ /^et al/i ) {
            $shortRef .= " and $a2";
        } else {
            $shortRef .= " et al.";
        }
    }
    if ($pubyr) {
	$shortRef .= " " . $pubyr;
    }
    
    return $shortRef;
}


# interpretSpeciesName ( taxon_name )
# 
# Separate the given name into genus, subgenus, species and subspecies.

sub interpretSpeciesName {

    my ($taxon_name) = @_;
    my @components = split(/\s+/, $taxon_name);
    
    my ($genus, $subgenus, $species, $subspecies);
    
    # If the first character is a space, the first component will be blank;
    # ignore it.
    
    shift @components if @components && $components[0] eq '';
    
    # If there's nothing left, we were given bad input-- return nothing.
    
    return unless @components;
    
    # The first component is always the genus.
    
    $genus = shift @components;
    
    # If the next component starts with '(', it is a subgenus.
    
    if ( @components && $components[0] =~ /^\((.*)\)$/ )
    {
	$subgenus = $1;
	shift @components;
    }
    
    # The next component must be the species
    
    $species = shift @components if @components;
    
    # The last component, if there is one, must be the subspecies.  Strip
    # parentheses if there are any.
    
    $subspecies = shift @components if @components;
    
    if ( defined $subspecies && $subspecies =~ /^\((.*)\)$/ ) {
	$subspecies = $1;
    }
    
    return ($genus, $subgenus, $species, $subspecies);
}


# The following hashes map the status codes stored in the opinions table of
# PaleoDB into taxonomic and nomenclatural status codes in compliance with
# Darwin Core.  The third one, %REPORT_ACCEPTED_TAXON, indicates which status
# codes should trigger the "acceptedUsage" and "acceptedUsageID" fields in the
# output.

our (%TAXONOMIC_STATUS) = (
	'belongs to' => 'valid',
	'subjective synonym of' => 'heterotypic synonym',
	'objective synonym of' => 'homotypic synonym',
	'invalid subgroup of' => 'invalid',
	'misspelling of' => 'invalid',
	'replaced by' => 'invalid',
	'nomen dubium' => 'invalid',
	'nomen nudum' => 'invalid',
	'nomen oblitum' => 'invalid',
	'nomen vanum' => 'invalid',
);


our (%NOMENCLATURAL_STATUS) = (
	'invalid subgroup of' => 'invalid subgroup',
	'misspelling of' => 'misspelling',
	'replaced by' => 'replaced by',
	'nomen dubium' => 'nomen dubium',
	'nomen nudum' => 'nomen nudum',
	'nomen oblitum' => 'nomen oblitum',
	'nomen vanum' => 'nomen vanum',
);


our (%REPORT_ACCEPTED_TAXON) = (
	'subjective synonym of' => 1,
	'objective synonym of' => 1,
	'misspelling of' => 1,
	'replaced by' => 1,
);


# interpretStatusCode ( pbdb_status )
# 
# Use the hashes given above to interpret a status code from the opinions
# table of PaleoDB.  Returns: taxonomic status, whether we should report an
# "acceptedUsage" taxon, and the nomenclatural status.

sub interpretStatusCode {

    my ($pbdb_status) = @_;
    
    # If the status is empty, return nothing.
    
    unless ( defined $pbdb_status and $pbdb_status ne '' )
    {
	return '', '', '';
    }
    
    # Otherwise, interpret the status code according to the mappings specified
    # above.
    
    return $TAXONOMIC_STATUS{$pbdb_status}, $REPORT_ACCEPTED_TAXON{$pbdb_status}, 
	$NOMENCLATURAL_STATUS{$pbdb_status};
}


1;
