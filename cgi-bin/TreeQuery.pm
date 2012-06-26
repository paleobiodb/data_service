#
# TreeQuery
# 
# A class that returns information from the PaleoDB database about a single
# taxon or a category of taxa.  This is a subclass of DataQuery.
# 
# Author: Michael McClennen

package TreeQuery;

use strict;
use base 'DataQuery';


# setParameters ( params )
# 
# This method accepts a hash of parameter values, filters them for correctness,
# and sets the appropriate fields of the query object.  It is designed to be
# called from a Dancer route, although that is not a requirement.

sub setParameters {
    
    my ($self, $params) = @_;
    
    # First tell our superclass to set any parameters it recognizes.
    
    $self->DataQuery::setParameters($params);
    
    # If 'taxon_name' is specified, then information is returned about the taxon
    # hierarchy rooted at the specified taxon.  This parameter cannot be used
    # at the same time as 'id'.
    
    if ( defined $params->{taxon_name} && $params->{taxon_name} ne '' )
    {
	# Clean the parameter of everything except alphabetic characters
	# and spaces, since only those are valid in taxonomic names.
	
	$self->{taxon_name} = $params->{taxon_name};
	$self->{taxon_name} =~ s/[^a-zA-Z ]//;
    }
    
    # If "taxon_no" is specified, then information is returned about the taxon
    # hierarchy rooted at the specified taxon.  This parameter cannot be used
    # at the same time as 'name'.
    
    if ( defined $params->{taxon_no} && $params->{taxon_no} > 0 )
    {
	$self->{taxon_no} = $params->{taxon_no} + 0;
    }
    
    # If "synonyms" is specified, then include junior synonyms.  The default
    # is to leave them out.
    
    if ( defined $params->{synonyms} )
    {
	$self->{include_synonyms} = 1;
    }
    
    # Turn the following on always for now.  This will likely be changed later.
    
    $self->{show_attribution} = 1;
    $self->{show_extant} = 1;
    
    return 1;
}


# fetchMultiple ( )
# 
# Query the database using the parameters previously specified (i.e. by a call
# to setParameters).  This class only returns a compound result, even if the
# given taxon has no children.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub fetchMultiple {

    my ($self) = @_;
    my ($sql);
    my (@taxon_filter, @extra_tables, @extra_fields);
    my ($taxon_limit) = "";
    my ($lft, $rgt);
    
    # Get ahold of a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # If a query limit has been specified, construct the appropriate SQL string.
    
    if ( defined $self->{limit} )
    {
	if ( $self->{limit} eq 'default' )
	{
	    $taxon_limit = "LIMIT 500";
	}
	elsif ( $self->{limit} > 0 )
	{
	    $taxon_limit = "LIMIT " . $self->{limit};
	}
    }
    
    # If the target taxon was specified by name, find it.
    
    if ( defined $self->{taxon_name} )
    {
	($lft, $rgt) = $dbh->selectrow_array("
		SELECT t.lft, t.rgt FROM taxa_tree_cache t JOIN authorities a USING (taxon_no)
		WHERE a.taxon_name = ?", {RaiseError=>0}, $self->{taxon_name});
	
	unless ( $lft > 0 )
	{
	    $self->{error} = "taxon '$self->{taxon_name}' was not found in the database";
	    return;
	}
    }
    
    # Otherwise, use the id.
    
    elsif ( defined $self->{taxon_no} )
    {
	($lft, $rgt) = $dbh->selectrow_array("
		SELECT t.lft, t.rgt FROM taxa_tree_cache t
		WHERE t.taxon_no = ?", {RaiseError=>0}, $self->{taxon_no});
	
	unless ( $lft > 0 )
	{
	    $self->{error} = "taxon $self->{taxon_no} was not found in the database";
	    return;
	}
    }
    
    # If neither, we have a problem.
    
    else
    {
	$self->{error} = "you must specify either the parameter 'taxon_name' or the parameter 'taxon_no'";
	return;
    }
    
    # Now construct and execute the SQL statement that will be used to fetch
    # the desired information from the database.
    
    $sql = "	SELECT t.taxon_no, t.orig_no, t.lft, t.rgt,
		       a.taxon_rank, a.taxon_name, a.common_name, a.extant, o.status,
	               if (a.pubyr IS NOT NULL AND a.pubyr != '' AND a.pubyr != '0000', 
			   a.pubyr, IF (a.ref_is_authority = 'YES', r.pubyr, '')) a_pubyr,
		       if (a.ref_is_authority = 'YES', r.author1last, a.author1last) a_al1,
		       if (a.ref_is_authority = 'YES', r.author2last, a.author2last) a_al2,
		       if (a.ref_is_authority = 'YES', r.otherauthors, a.otherauthors) a_ao
		FROM taxa_tree_cache t JOIN authorities a USING (taxon_no)
				       LEFT JOIN opinions o USING (opinion_no)
				       LEFT JOIN refs r ON (a.reference_no = r.reference_no)
		WHERE t.lft >= ? and t.rgt <= ?
		  AND t.synonym_no = t.taxon_no
		  AND (o.status is null OR o.status = 'belongs to')
		ORDER BY t.lft";
    
    $self->{main_sth} = $dbh->prepare($sql);
    $self->{main_sth}->execute($lft, $rgt);
    
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
	
	$output .= ',"nameAccordingTo":"' . $attr . '"';
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
	
	$output .= '    <dwc:nameAccordingTo>' . $attr . '</dwc:nameAccordingTo>' . "\n";
    }

    $output .= '  </dwc:Taxon>' . "\n";
}



1;
