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


our ($PARAM_DESC) = <<DONE;
  taxon_name - return the portion of the hierarchy rooted at the given taxon (scientific name)
  taxon_no - return the portion of the hierarchy rooted at the given taxon (positive integer identifier)
  limit_rank - only return taxa of this rank or higher ('species', 'genus', or 'family')
DONE

our ($PARAM_CHECK) = { taxon_name => 1, taxon_no => 1, limit_rank => 1 };

# setParameters ( params )
# 
# This method accepts a hash of parameter values, filters them for correctness,
# and sets the appropriate fields of the query object.  It is designed to be
# called from a Dancer route, although that is not a requirement.

sub setParameters {
    
    my ($self, $params) = @_;
    
    # First tell our superclass to process any parameters it recognizes.
    
    $self->SUPER::setParameters($params);
    
    # If 'taxon_name' is specified, then information is returned about the taxon
    # hierarchy rooted at the specified taxon.  This parameter cannot be used
    # at the same time as 'id'.
    
    if ( defined $params->{taxon_name} )
    {
	# Check to make sure that taxon_no was not specified at the same time
	
	if ( defined $params->{taxon_no} )
	{
	    die "400 You may not specify 'taxon_name' and 'taxon_no' together\n";
	}
	
	# Check to make sure we actually have a value
	
	if ( $params->{taxon_name} eq '' )
	{ 
	    die "400 You must provide a non-empty value for 'taxon_name'\n";
	}
	
	# Clean the parameter of everything except alphabetic characters
	# and spaces, since only those are valid in taxonomic names.
	
	if ( $params->{taxon_name} =~ /[^a-zA-Z\s]/ )
	{
	    die "400 The parameter 'taxon_name' may contain only characters from the Roman alphabet plus whitespace\n";
	}
	
	$self->{taxon_name} = $params->{taxon_name};
	$self->{taxon_name} =~ s/\s+/ /g;
    }
    
    # If "taxon_no" is specified, then information is returned about the taxon
    # hierarchy rooted at the specified taxon.  This parameter cannot be used
    # at the same time as 'name'.
    
    elsif ( defined $params->{taxon_no} )
    {
	# First check to make sure that a valid value was provided
	
	if ( $params->{taxon_no} =~ /[^0-9]/ )
	{
	    die "400 You must provide a positive integer value for 'taxon_no'\n";
	}
	
	$self->{taxon_no} = $params->{taxon_no} + 0;
    }
    
    # If neither was specified, return the "help" message.
    
    else
    {
	die "400 help\n";
    }
    
    # If "limit_rank" is specified, then we ignore all taxa below the
    # specified rank.
    
    if ( defined $params->{limit_rank} )
    {
	my $limit = $params->{limit_rank};
	
	if ( $limit eq 'species' or $limit eq 'genus' or $limit eq 'family' or $limit eq 'order' or $limit eq 'class' )
	{
	    $self->{limit_rank} = $limit;
	}
	
	else
	{
	    die "400 The parameter 'limit_rank' must be either 'species', 'genus', or 'family'\n";
	}
    }
    
    # Turn the following on always for now.  This will likely be changed later.
    
    $self->{show_attribution} = 1;
    $self->{show_extant} = 1;
    
    return 1;
}


# fetchMultiple ( )
# 
# Query the database using the parameters specified by a previous call to
# setParameters.  This class only returns a compound result, even if the given
# taxon has no children.
# 
# Returns true if the fetch succeeded, dies if an error occurred.

sub fetchMultiple {

    my ($self) = @_;
    my ($sql);
    my (@taxon_filter, @extra_tables, @extra_fields);
    my ($limit_sql) = "";
    my ($rank_clause) = "";
    my ($lft, $rgt);
    
    # Get ahold of a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # If a query limit has been specified, construct the appropriate SQL clause.
    
    if ( defined $self->{limit_results} )
    {
	$limit_sql = "LIMIT " . ($self->{limit_results} + 0);
    }
    
    if ( defined $self->{limit_rank} )
    {
	if ( $self->{limit_rank} eq 'species' )
	{
	    $rank_clause = "\n	  AND taxon_rank not in ('subspecies')";
	}
	
	elsif ( $self->{limit_rank} eq 'genus' )
	{
	    $rank_clause = "\n	  AND taxon_rank not in ('subspecies', 'species', 'subgenus')";
	}

	elsif ( $self->{limit_rank} eq 'family' )
	{
	    $rank_clause = "\n	  AND taxon_rank not in ('subspecies', 'species', 'subgenus', 'genus', 'subtribe', 'tribe', 'subfamily')";
	}
    }
    
    # If the target taxon was specified by name, find it.
    
    if ( defined $self->{taxon_name} )
    {
	($lft, $rgt) = $dbh->selectrow_array("
		SELECT t.lft, t.rgt FROM taxa_tree_cache t JOIN authorities a USING (taxon_no)
		WHERE a.taxon_name = ?", {RaiseError=>0}, $self->{taxon_name});
	
	unless ( defined $lft && $lft > 0 )
	{
	    die "404 taxon '$self->{taxon_name}' was not found in the database\n";
	}
    }
    
    # Otherwise, use the id.
    
    elsif ( defined $self->{taxon_no} )
    {
	($lft, $rgt) = $dbh->selectrow_array("
		SELECT t.lft, t.rgt FROM taxa_tree_cache t
		WHERE t.taxon_no = ?", {RaiseError=>0}, $self->{taxon_no});
	
	unless ( defined $lft && $lft > 0 )
	{
	    die "404 taxon $self->{taxon_no} was not found in the database\n";
	}
    }
    
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
	  AND t.synonym_no = t.taxon_no
	  AND (o.status is null OR o.status = 'belongs to') $rank_clause
	ORDER BY t.lft $limit_sql";
    
    # Also construct a statement to fetch the result count if necessary.
    
    $self->{count_sql} = "
	SELECT count(*)
	FROM taxa_tree_cache t LEFT JOIN opinions o USING (opinion_no)
	WHERE t.lft >= $lft and t.rgt <= $rgt
	  AND t.synonym_no = t.taxon_no
	  AND (o.status is null OR o.status = 'belongs to')";
    
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


# generateRecord ( row, is_first_record )
# 
# Return a string representing one row of the result, in the selected output
# format.  The parameter $is_first_record indicates whether this is the first
# record, which is significant for JSON output (it controls whether or not to
# output an initial comma, in that case).

sub generateRecord {

    my ($self, $row, $is_first_record) = @_;
    
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
	$prefix .= "\n";
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
	
	if ( defined $row->{orig_no} && $row->{taxon_no} != $row->{orig_no} )
	{
	    $attr = "($attr)";
	}
	
	$output .= ',"nameAccordingTo":"' . DataQuery::json_clean($attr) . '"';
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
	
	if ( defined $row->{orig_no} && $row->{taxon_no} != $row->{orig_no} )
	{
	    $attr = "($attr)";
	}
	
	$output .= '    <dwc:nameAccordingTo>' . DataQuery::xml_clean($attr) . 
	    '</dwc:nameAccordingTo>' . "\n";
    }

    $output .= '  </dwc:Taxon>' . "\n";
}



1;
