#
# TreeQuery
# 
# A class that returns information from the PaleoDB database about a single
# taxon or a category of taxa.  This is a subclass of DataQuery.
# 
# Author: Michael McClennen

package TreeQuery;

use strict;
use parent 'DataQuery';
use Carp qw(croak);


our ($PARAM_DESC_1_0) = "=over 4

=item B<base_name>

return the portion of the hierarchy rooted at the given taxon (scientific name)

=item B<base_rank>

look for the taxon name only at the specified rank (taxon rank)

=item B<base_no>

return the portion of the hierarchy rooted at the given taxon (positive integer identifier)

=item B<taxon_name>

synonym for base_name (scientific name)

=item B<taxon_rank>

synonym for base_rank (taxon rank)

=item B<taxon_no>

synonym for base_no (positive integer identifier)

=item B<rank>

only return taxa whose rank falls within the specified range (taxonomic rank(s))

=item B<extant>

only return information about extant or non-extant taxa (boolean)";

our ($PARAM_REQS_1_0) = "You must specify either C<base_name> or C<base_no> (alternatively C<taxon_name> or C<taxon_no>).";

our ($PARAM_CHECK_1_0) = { taxon_name => 1, taxon_rank => 1, taxon_no => 1, 
		       base_name => 1, base_rank => 1, base_no => 1, rank => 1,
		       type => 1, extant => 1 };

our ($FIELD_DESC_1_0) = "=over 4

=item B<scientificName>

The name of this taxon.  This value can be used with parameter C<taxon_name> in subsequent queries.

=item B<taxonRank>

The rank of this taxon.  Values range from 'subspecies' through 'kingdom', plus 'unranked clade' and 'informal'.  This value can be used with parameters C<taxon_rank> and C<rank> in subsequent queries.

=item B<taxonID>

The unique identifier for this taxon in the Paleobiology Database.  Values are positive integers, and can be used with parameter C<taxon_no> in subsequent queries.

=item B<scientificNameAuthorship>

The author(s) of this taxon, together with the year it was first published.  Requires C<show=attr>.

=item B<children>

A list of records consisting of the immediate children of this taxon.  Note that this field only occurs in JSON output.

=back
";


# setParameters ( params )
# 
# This method accepts a hash of parameter values, filters them for correctness,
# and sets the appropriate fields of the query object.  It is designed to be
# called from a Dancer route, although that is not a requirement.

sub setParameters {
    
    my ($self, $params) = @_;
    
    # First tell our superclass to process any parameters it recognizes.
    
    $self->SUPER::setParameters($params);
    
    # The parameter 'taxon_no' is a synonym for 'base_no'.
    
    if ( defined $params->{taxon_no} and $params->{taxon_no} ne '' )
    {
	if ( defined $params->{base_no} and $params->{base_no} ne '' and 
	     $params->{taxon_no} ne $params->{base_no} )
	{
	    die "400 You cannot specify different values for 'taxon_no' and 'base_no'.\n"
	}
	
	$params->{base_no} = $params->{taxon_no};
    }
    
    # The parameter 'taxon_name' is a synonym for 'base_name'.

    if ( defined $params->{taxon_name} and $params->{taxon_name} ne '' )
    {
	if ( defined $params->{base_name} and $params->{base_name} ne '' and 
	     $params->{taxon_name} ne $params->{base_name} )
	{
	    die "400 You cannot specify different values for 'taxon_name' and 'base_name'.\n"
	}
	
	$params->{base_name} = $params->{taxon_name};
    }
    
    # The parameter 'taxon_rank' is a synonym for 'base_rank'.

    if ( defined $params->{taxon_rank} and $params->{taxon_rank} ne '' )
    {
	if ( defined $params->{base_rank} and $params->{base_rank} ne '' and 
	     $params->{taxon_rank} ne $params->{base_rank} )
	{
	    die "400 You cannot specify different values for 'taxon_rank' and 'base_rank'.\n"
	}
	
	$params->{base_rank} = $params->{taxon_rank};
    }
    
    # If 'base_name' is specified, then information is returned about the
    # hierarchy rooted at the specified taxon.  This parameter cannot be used
    # at the same time as 'base_no'.
    
    if ( defined $params->{base_name} and $params->{base_name} ne '' )
    {
	# Check to make sure that base_no was not specified at the same time
	
	if ( defined $params->{base_no} and $params->{base_no} ne '' )
	{
	    die "400 You may not specify 'base_name' and 'base_no' together.\n";
	}
	
	# Clean the parameter of everything except alphabetic characters
	# and spaces, since only those are valid in taxonomic names.
	
	if ( $params->{base_name} =~ /[^a-zA-Z()\s]/ )
	{
	    die "400 The parameter 'base_name' may contain only characters from the Roman alphabet plus whitespace.\n";
	}
	
	$self->{base_taxon_name} = $params->{base_name};
	$self->{base_taxon_name} =~ s/\s+/ /g;
    }
    
    # If "base_no" is specified, then information is returned about the
    # hierarchy rooted at the specified taxon.  This parameter cannot be used at
    # the same time as 'base_name'.
    
    elsif ( defined $params->{base_no} and $params->{base_no} ne '' )
    {
	# First check to make sure that a valid value was provided
	
	if ( $params->{base_no} =~ /[^0-9]/ )
	{
	    die "400 You must provide a positive integer value for 'base_no'.\n";
	}
	
	$self->{base_taxon_no} = $params->{base_no} + 0;
    }
    
    # If neither was specified, return the "help" message.
    
    else
    {
	die "400 help\n";
    }
    
    # If "base_rank" is specified, then we only match the name if it matches
    # the specified rank.
    
    if ( defined $params->{base_rank} and $params->{base_rank} ne '' )
    {
	my $rank = lc $params->{base_rank};
	
	# This parameter cannot be used with base_no (since base_no uniquely
	# identifies a taxon, it cannot be used with any other taxon-selecting
	# parameters).
	
	if ( defined $params->{base_no} )
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
    
    # If "rank" is specified, then we ignore all taxa that do not fall
    # into the specified set of ranks.
    
    if ( defined $params->{rank} and $params->{rank} ne '' )
    {
	my $limit = lc $params->{rank};
	
	$self->{filter_rank} = $self->parseRankParam($limit, 'rank');
    }
    
    # If 'type' is specified, the results are restricted to certain types
    # of taxa.  Defaults to 'valid'.
    
    if ( defined $params->{type} and $params->{type} ne '' )
    {
	my $type = lc $params->{type};
	
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
    
    # Turn the following on always for now.  This will likely be changed later.
    
    $self->{show_attribution} = 1;
    $self->{show_extant} = 1;
    
    return 1;
}


# fetchMultiple ( )
# 
# Query the database using the parameters specified by a previous call to
# setParameters.
# 
# Returns true if the fetch succeeded, dies if an error occurred.

sub fetchMultiple {

    my ($self) = @_;
    my ($sql);
    my (@filter_list, @extra_tables, @extra_fields);
    my ($limit_sql) = "";
    my ($lft, $rgt);
    
    # Get ahold of a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # If a query limit has been specified, construct the appropriate SQL clause.
    
    if ( defined $self->{limit_results} )
    {
	$limit_sql = "LIMIT " . ($self->{limit_results} + 0);
    }
    
    if ( ref $self->{filter_rank} eq 'ARRAY' )
    {
	my (@disjunction, @in_list);
	
	foreach my $r (@{$self->{filter_rank}})
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
    
    # If the target taxon was specified by name and rank, look for it.
    
    if ( defined $self->{base_taxon_name} && defined $self->{base_taxon_rank} )
    {
	($lft, $rgt) = $dbh->selectrow_array("
		SELECT t2.lft, t2.rgt
		FROM taxa_tree_cache t1 JOIN authorities a USING (taxon_no)
			JOIN taxa_tree_cache t2 ON t2.taxon_no = t1.synonym_no
		WHERE a.taxon_name = ? and a.taxon_rank = ?", {}, 
				$self->{base_taxon_name}, $self->{base_taxon_rank});
	
	# If we can't find the base taxon, the result set will be empty.
	
	unless ( defined $lft && $lft > 0 )
	{
	    return;
	}
    }
    
    # If the target taxon was specified by name, look for it.
    
    elsif ( defined $self->{base_taxon_name} )
    {
	($lft, $rgt) = $dbh->selectrow_array("
		SELECT t2.lft, t2.rgt
		FROM taxa_tree_cache t1 JOIN authorities a USING (taxon_no)
			JOIN taxa_tree_cache t2 ON t2.taxon_no = t1.synonym_no
		WHERE a.taxon_name = ?", {}, $self->{base_taxon_name});
	
	# If we can't find the base taxon, the result set will be empty.
	
	unless ( defined $lft && $lft > 0 )
	{
	    return;
	}
    }
    
    # Otherwise, if a taxon_no was specified, use that.
    
    elsif ( defined $self->{base_taxon_no} )
    {
	($lft, $rgt) = $dbh->selectrow_array("
		SELECT t2.lft, t2.rgt FROM taxa_tree_cache t2
			JOIN taxa_tree_cache t1 ON t2.taxon_no = t1.synonym_no
		WHERE t1.taxon_no = ?", {}, $self->{base_taxon_no});
	
	# If we can't find the base taxon, the result set will be empty.
	
	unless ( defined $lft && $lft > 0 )
	{
	    return;
	}
    }
    
    # Otherwise, we have a problem.
    
    else
    {
	croak "No base taxon was specified";
    }
    
    # Put together the filter clause.
    
    my $filter_clause = (@filter_list ? 'and ' . join(' and ', @filter_list) : '');
    
    # Now construct and execute the SQL statement that will be used to fetch
    # the desired information from the database.
    
    $self->{main_sql} = "
	SELECT t.taxon_no, t.orig_no, t.lft, t.rgt,
	       a.taxon_rank, a.taxon_name, a.common_name, a.extant, o.status,
	           if (a.pubyr IS NOT NULL AND a.pubyr != '' AND a.pubyr != '0000', 
			a.pubyr, IF (a.ref_is_authority = 'YES', r.pubyr, '')) a_pubyr,
		   if (a.ref_is_authority = 'YES', r.author1last, a.author1last) a_al1,
		   if (a.ref_is_authority = 'YES', r.author2last, a.author2last) a_al2,
		   if (a.ref_is_authority = 'YES', r.otherauthors, a.otherauthors) a_ao
	FROM taxa_tree_cache t JOIN authorities a USING (taxon_no)
			       LEFT JOIN opinions o USING (opinion_no)
			       LEFT JOIN refs r ON (a.reference_no = r.reference_no)
	WHERE t.lft >= $lft and t.rgt <= $rgt
	  $filter_clause
	ORDER BY t.lft $limit_sql";
    
    # Also construct a statement to fetch the result count if necessary.
    
    $self->{count_sql} = "
	SELECT count(*)
	FROM taxa_tree_cache t JOIN authorities a USING (taxon_no)
			       LEFT JOIN opinions o USING (opinion_no)
	WHERE t.lft >= $lft and t.rgt <= $rgt
	  $filter_clause";
    
    # Now prepare and execute the main statement.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    return 1;
}


# initOutput ( )
# 
# This is called before the first record is generated.  We use it to set up
# the stacks that will be used to generate the proper tree structure.

sub initOutput {

    my ($self) = @_;
    
    $self->{tree_stack} = [];
    $self->{comma_stack} = [];
    
    return;
}


# processRecord ( row )
# 
# Do the necessary processing on the given row to make it ready to use.

sub processRecord {

    my ($self, $row) = @_;
    
    $self->generateAttribution($row);
}


# generateRecord ( row, options )
# 
# Return a string representing one row of the result, in the selected output
# format.  The option 'is_first' indicates that this is the first record,
# which is significant for JSON output (it controls whether or not to output
# an initial comma, in that case).

sub generateRecord {

    my ($self, $row, %options) = @_;
    
    # If the selected output format is XML, we just dispatch the appropriate
    # method.  In this case, the client of this service will get a preorder
    # traversal of the relevant portion of the taxon tree, and will have to
    # infer the tree structure from the taxon ranks.
    
    return $self->emitTaxonXML($row) if $self->{output_format} eq 'xml';
    
    # Otherwise, it must be JSON.  So we need to deal with the hierarchical
    # structure.
    
    unless ( @{$self->{tree_stack}} > 0 )
    {
	return $self->emitTaxonJSON($row);
    }
    
    my $prefix = '';
    
    while ( @{$self->{tree_stack}} > 0 && $row->{lft} > $self->{tree_stack}[0] )
    {
	$prefix .= "]}";
	shift @{$self->{tree_stack}};
	shift @{$self->{comma_stack}};
    }
    
    if ( $self->{comma_stack}[0] )
    {
	$prefix .= "\n,";
    }
    
    else
    {
	$prefix .= ($prefix ne '' ? "\n," : "\n");
	$self->{comma_stack}[0] = 1;
    }
    
    return $prefix . $self->emitTaxonJSON($row);
}


sub finishOutput {

    my ($self) = @_;
    
    my $output = '';
    
    foreach ( @{$self->{tree_stack}} )
    {
	$output .= ']}';
    }
    
    $self->{tree_stack} = [];
    $self->{comma_stack} = [];
    return $output;
}
    

# emitTaxonJSON ( row )
# 
# Return a string representing the given taxon record (row) in JSON format.
# If 'parents' is specified, it should be an array of hashes each representing
# a parent taxon record.  If 'is_first_record' is true, then the result will
# not start with a comma.  If 'short_record' is true, then some fields will be
# suppressed.

sub emitTaxonJSON {
    
    no warnings;
    
    my ($self, $row) = @_;
    
    my $output = '';
    
    $output .= '{"scientificName":"' . DataQuery::json_clean($row->{taxon_name}) . '"';
    $output .= ',"taxonRank":"' . $row->{taxon_rank} . '"';
    $output .= ',"taxonID":"' . $row->{taxon_no} . '"';
    
    if ( defined $row->{common_name} && $row->{common_name} ne '' ) {
	$output .= ',"vernacularName":"' . DataQuery::json_clean($row->{common_name}) . '"';
    }
    
    if ( defined $self->{show_attribution} && defined $row->{attribution} &&
	 $row->{attribution} ne '' )
    {
	my $attr = $row->{attribution};
	
	$output .= ',"scientificNameAuthorship":"' . DataQuery::json_clean($attr) . '"';
    }
    
    if ( defined $self->{show_extant} and defined $row->{extant} 
	 and $row->{extant} =~ /(yes|no)/i ) {
	$output .= ',"extant":"' . $1 . '"';
    }
    
    unshift @{$self->{tree_stack}}, $row->{rgt};
    unshift @{$self->{comma_stack}}, 0;
    $output .= ',"children":[';
    
    return $output;
}


# emitTaxonXML ( row, short_record )
# 
# Returns a string representing the given record (row) in Darwin Core XML
# format.  If 'short_record' is true, suppress certain fields.

sub emitTaxonXML {
    
    no warnings;
    
    my ($self, $row) = @_;
    my $output = '';
    my @remarks = ();
    
    $output .= '  <dwc:Taxon>' . "\n";
    $output .= '    <dwc:scientificName>' . DataQuery::xml_clean($row->{taxon_name}) . 
	'</dwc:scientificName>' . "\n";
    $output .= '    <dwc:taxonRank>' . $row->{taxon_rank} . '</dwc:taxonRank>' . "\n";
    
    $output .= '    <dwc:taxonID>' . $row->{taxon_no} . '</dwc:taxonID>' . "\n";
    
    if ( defined $self->{show_attribution} && defined $row->{attribution} &&
	 $row->{attribution} ne '' )
    {
	my $attr = $row->{attribution};
	
	$output .= '    <dwc:scientificNameAuthorship>' . DataQuery::xml_clean($attr) . 
	    '</dwc:scientificNameAuthorship>' . "\n";
    }

    $output .= '  </dwc:Taxon>' . "\n";
}



1;
