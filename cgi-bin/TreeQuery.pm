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
    
    # If 'name' is specified, then information is returned about the taxon
    # hierarchy rooted at the specified taxon.  This parameter cannot be used
    # at the same time as 'id'.
    
    if ( defined $params->{name} && $params->{name} ne '' )
    {
	# Clean the parameter of everything except alphabetic characters
	# and spaces, since only those are valid in taxonomic names.
	
	$self->{taxon_name} = $params->{name};
	$self->{taxon_name} =~ s/[^a-zA-Z ]//;
    }
    
    # If 'exact' is specified, then information is returned about the exact taxon
    # specified.  Otherwise (default) information is returned about the senior
    # synonym or correct spelling of the specified taxon if such exist.
    
    if ( defined $params->{exact} && $params->{exact} ne '' )
    {
	$self->{fetch_exact} = 1;
    }
    
    # If "id" is specified, then information is returned about the taxon
    # hierarchy rooted at the specified taxon.  This parameter cannot be used
    # at the same time as 'name'.
    
    if ( defined $params->{id} && $params->{id} > 0 )
    {
	$self->{taxon_id} = $params->{id} + 0;
    }
    
    # If "synonyms" is specified, then include junior synonyms.  The default
    # is to leave them out.
    
    if ( defined $params->{synonyms} )
    {
	$self->{include_synonyms} = 1;
    }
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
    
    elsif ( defined $self->{taxon_id} )
    {
	($lft, $rgt) = $dbh->selectrow_array("
		SELECT t.lft, t.rgt FROM taxa_tree_cache t
		WHERE t.taxon_no = ?", {RaiseError=>0}, $self->{taxon_id});
	
	unless ( $lft > 0 )
	{
	    $self->{error} = "taxon id $self->{taxon_id} was not found in the database";
	    return;
	}
    }
    
    # If neither, we have a problem.
    
    else
    {
	$self->{error} = "you must specify either the parameter 'name' or 'id'";
	return;
    }
    
    # Now construct and execute the SQL statement that will be used to fetch
    # the desired information from the database.
    
    $sql = "	SELECT t.taxon_no, t.lft, t.rgt, 
		       a.taxon_rank, a.taxon_name, a.common_name, a.extant, o.status
		FROM taxa_tree_cache t JOIN authorities a USING (taxon_no)
				       JOIN opinions o USING (opinion_no)
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


# generateRecord ( row, is_first_record )
# 
# Return a string representing one record, in the selected output format.  The
# parameter $is_first_record indicates whether this is the first record, which
# is significant for JSON output (it controls whether or not to output an
# initial comma, in that case).

sub generateRecord {

    my ($self, $row) = @_;
    
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
    $output .= ',"taxonID":"' . $row->{taxon_no} . '"' if $self->{show_id};
    
    if ( defined $row->{common_name} && $row->{common_name} ne '' ) {
	$output .= ',"vernacularName":"' . DataQuery::json_clean($row->{common_name}) . '"';
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
    
    $output .= '    <dwc:taxonID>' . $row->{taxon_no} . '</dwc:taxonID>' . "\n"
	if $self->{show_id};
    
    $output .= '  </dwc:Taxon>' . "\n";
}



1;
