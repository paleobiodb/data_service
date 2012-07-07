#
# PBCollectionQuery
# 
# A class that returns information from the PaleoDB database about a single
# collection or a category of collections.  This is a subclass of PBDataQuery.
# 
# Author: Michael McClennen

package CollectionQuery;

use strict;
use base 'DataQuery';


our ($PARAM_DESC_MULTIPLE) = <<DONE;
  taxon_name - list all collections that contain an example of the given taxon or any of its descendants (scientific name)
  taxon_no - list all collections that contain an example of the given taxon or any of its descendants (positive integer identifier)
  loc - list all collections that were found within the given geographic coordinates (bounding box)

  show - return some or all of the specified information (comma-separated list)
          ref - include the publication reference for each collection
          taxa - include a list of taxa represented in the collection
	  all - include all available information
DONE

our ($PARAM_REQS_MULTIPLE) = "You must specify at least one of: taxon_name, taxon_no, loc.";

our ($PARAM_CHECK_MULTIPLE) = { taxon_name => 1, taxon_no => 1, loc => 1, show => 1 };

our ($PARAM_DESC_SINGLE) = <<DONE;
  collection_no - provide details about the given collection (positive integer identifier)

  show - return some or all of the specified information (comma-separated list)
          ref - include the publication reference for the collection
          taxa - include a list of taxa represented in the collection
	  all - include all available information
DONE

our ($PARAM_REQS_SINGLE) = "You must specify collection_no.";

our ($PARAM_CHECK_SINGLE) = { collection_no => 1, show => 1 };


# setParameters ( params )
# 
# This method accepts a hash of parameter values, filters them for correctness,
# and sets the appropriate fields of the query object.  It is designed to be
# called from a Dancer route, although that is not a requirement.

sub setParameters {
    
    my ($self, $params) = @_;
    
    # First tell our superclass (PBDataQuery) to set any parameters it
    # recognizes.
    
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
    
    # Now set common parameters.
    
    if ( defined $params->{show} and $params->{show} ne '' )
    {
	my (@show) = split /\s*,\s*/, lc($params->{show});
	
	foreach my $s (@show)
	{
	    if ( $s eq 'ref' )
	    {
		$self->{show_ref} = 1;
	    }
	    
	    elsif ( $s eq 'taxa' )
	    {
		$self->{show_taxa} = 1;
	    }
	    
	    elsif ( $s eq 'all' )
	    {
		$self->{show_ref} = 1;
		$self->{show_taxa} = 1;
	    }
	    
	    else
	    {
		$self->warn("Unknown value '$s' for show");
	    }
	}

    }
}


sub setParametersSingle {

    my ($self, $params) = @_;
    
    if ( defined $params->{collection_no} and $params->{collection_no} ne '' )
    {
	# First check to make sure that a valid value was provided
	
	if ( $params->{collection_no} =~ /[^0-9]/ )
	{
	    die "400 You must provide a positive integer value for 'collection_no'.\n";
	}
	
	$self->{collection_no} = $params->{collection_no} + 0;
    }
    
    else
    {
	die "400 help\n";
    }
}


sub setParametersMultiple {
    
    my ($self, $params) = @_;
    
    # The 'taxon_name' parameter restricts the output to collections that
    # contain occurrences of the given taxa or any of their children.
    # Multiple taxa can be specified, separated by commas.
    
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
	    die "400 The parameter 'taxon_name' may contain only characters from the Roman alphabet plus whitespace\n";
	}
	
	$self->{taxon_filter} = $params->{taxon_name};
	$self->{taxon_filter} =~ s/\s+/ /g;
    }
    
    # The 'taxon_no' parameter is similar to 'taxon_name' but takes a unique
    # integer identifier instead.
    
    elsif ( defined $params->{taxon_no} and $params->{taxon_no} ne '' )
    {
	# First check to make sure that a valid value was provided
	
	if ( $params->{taxon_no} =~ /[^0-9]/ )
	{
	    die "400 You must provide a positive integer value for 'taxon_no'.\n";
	}
	
	$self->{filter_taxon_no} = $params->{taxon_no} + 0;
    }
    
    # The 'loc' parameter restricts the output to collections whose
    # location falls within the bounding box of the given geometry, specified
    # in WKT format.
    
    elsif ( defined $params->{loc} )
    {
	$self->{filter_location} = $params->{loc};
    }
    
    # If we don't have one of these parameters, give the help message.
    
    else
    {
	die "400 help\n";
    }
}


# fetchInfoManyCollections ( )
# 
# Query the database for basic info about all collections satisfying the
# conditions previously specified (i.e. by a call to setParameters).
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub fetchMultiple {

    my ($self) = @_;
    my ($sql);
    my (@filters, @extra_tables, @extra_fields);
    my ($limit_stmt) = "";
    my ($taxon_tables) = "";
    
    # Get ahold of a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # If a query limit has been specified, construct the appropriate SQL string.
    
    if ( defined $self->{limit} && $self->{limit} > 0 )
    {
	$limit_stmt = "LIMIT " . $self->{limit};
    }
    
    # If we are directed to show publication references, add the appopriate fields.
    
    if ( defined $self->{show_ref} )
    {
	push @extra_fields, "r.author1init r_ai1", "r.author1last r_al1",
	    "r.author2init r_ai2", "r.author2last r_al2", "r.otherauthors r_oa",
		"r.pubyr r_pubyr", "r.reftitle r_reftitle", "r.pubtitle r_pubtitle",
		    "r.editors r_editors", "r.pubvol r_pubvol", "r.pubno r_pubno",
			"r.firstpage r_fp", "r.lastpage r_lp";
    }
    
    # If a taxon filter has been defined, apply it now.
    
    if ( defined $self->{taxon_filter} )
    {
	my @specs = split /,/, $self->{taxon_filter};
	my ($sql, @taxa);
	
	my $taxa_name_query = $dbh->prepare(
		"SELECT DISTINCT t3.synonym_no
		 FROM authorities join taxa_tree_cache t1 using (taxon_no)
			join taxa_tree_cache t2 on t2.taxon_no = t1.synonym_no
			join taxa_tree_cache t3 on t3.taxon_no = t2.synonym_no
		 WHERE taxon_name = ?");
	
	my $taxa_rank_query = $dbh->prepare(
		"SELECT DISTINCT t3.synonym_no
		 FROM authorities join taxa_tree_cache t1 using (taxon_no)
			join taxa_tree_cache t2 on t2.taxon_no = t1.synonym_no
			join taxa_tree_cache t3 on t3.taxon_no = t2.synonym_no
		 WHERE taxon_name = ? AND taxon_rank = ?");
	
	foreach my $spec (@specs)
	{
	    my $result;
	    
	    if ( $spec =~ /^(\w+)\.(\w+)/ )
	    {
		$result = $taxa_rank_query->execute($1, $2);
		push @taxa, $taxa_rank_query->fetchrow_array();
	    }
	    else
	    {
		$result = $taxa_name_query->execute($spec);
		push @taxa, $taxa_name_query->fetchrow_array();
	    }
	}
	
	# If no taxa were found, then the result set will be empty.
	
	unless (@taxa)
	{
	    return;
	}
	
	# Otherwise, construct the appropriate filter clause.
	
	$taxon_tables = "JOIN taxa_list_cache l ON l.child_no = o.taxon_no";
	my $taxa_list = join(',', @taxa);
	push @filters, "l.parent_no in ($taxa_list)";
    }
    
    # If a location filter has been defined, apply it now
    
    if ( defined $self->{location_filter} )
    {
	my ($bound_rect) =
	    $dbh->selectrow_array("SELECT astext(envelope(geomfromtext('$self->{location_filter}')))");
	
	unless ( defined $bound_rect )
	{
	    die "400 Bad bounding box value '$self->{location_filter}'.\n";
	}
	
	if ( $bound_rect =~ /POLYGON\(\((-?\d*\.?\d*) (-?\d*\.?\d*),[^,]+,(-?\d*\.?\d*) (-?\d*\.?\d*)/ )
	{
	    push @filters, "c.lng >= $1";
	    push @filters, "c.lat >= $2";
	    push @filters, "c.lng <= $3";
	    push @filters, "c.lat <= $4";
	}
    }
    
    # Now construct the filter expression and extra_tables expression
    
    my $taxon_filter = join(' AND ', @filters);
    $taxon_filter = "WHERE $taxon_filter" if $taxon_filter ne '';
    
    my $extra_tables = join('', @extra_tables);
    
    # and the extra_fields expression
    
    my $extra_fields = join(', ', @extra_fields);
    $extra_fields = ", " . $extra_fields if $extra_fields ne '';
    
    # Now construct and execute the SQL statement that will be used to fetch
    # the desired information from the database.
    
    $self->{main_sql} = "
	SELECT DISTINCT c.collection_no, c.collection_name, c.lat, c.lng $extra_fields
        FROM collections c JOIN occurrences o using (collection_no) $taxon_tables
			LEFT JOIN refs r on r.reference_no = c.reference_no
	$taxon_filter ORDER BY c.collection_no $limit_stmt";
    
    # Also construct an SQL statement that will be used if necessary to
    # determine the result count.
    
    $self->{count_sql} = "
	SELECT count(*) FROM collections c JOIN occurrences o using (collection_no)
	$taxon_filter";
    
    # Then prepare and execute the main query and the secondary query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were directed to show associated taxa, construct an SQL statement
    # that will be used to grab that list.
    
    if ( $self->{show_taxa} )
    {
	$self->{second_sql} = "
	SELECT DISTINCT c.collection_no, a.taxon_name
	FROM collections c JOIN occurrences o using (collection_no) $taxon_tables
			JOIN authorities a using (taxon_no)
	$taxon_filter ORDER BY c.collection_no";
	
	$self->{second_sth} = $dbh->prepare($self->{second_sql});
	$self->{second_sth}->execute();
    }
    
    return 1;
}


# fetchInfoSingleCollection ( collection_requested )
# 
# Query for all relevant information about the requested taxon.
# 
# Options may have been set previously by methods of this class or of the
# parent class PBDataQuery.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub fetchSingle {

    my ($self) = @_;
    my ($sql, @extra_fields);
    
    # Get ahold of a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # If we are directed to show publication references, add the appopriate fields.
    
    if ( defined $self->{show_ref} )
    {
	push @extra_fields, "r.author1init r_ai1", "r.author1last r_al1",
	    "r.author2init r_ai2", "r.author2last r_al2", "r.otherauthors r_oa",
		"r.pubyr r_pubyr", "r.reftitle r_reftitle", "r.pubtitle r_pubtitle",
		    "r.editors r_editors", "r.pubvol r_pubvol", "r.pubno r_pubno",
			"r.firstpage r_fp", "r.lastpage r_lp";
    }
    
    # Next, fetch basic info about the collection.
    
    my $extra_fields = join(', ', @extra_fields);
    $extra_fields = ", " . $extra_fields if $extra_fields ne '';
    
    $self->{main_sql} = "
	SELECT c.collection_no, c.collection_name, c.lat, c.lng $extra_fields
	FROM collections c JOIN occurrences o using (collection_no)
			LEFT JOIN refs r using (reference_no)
        WHERE c.collection_no = ?";
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute($self->{collection_no});
    
    # If we were directed to show associated taxa, construct an SQL statement
    # that will be used to grab that list.
    
    if ( $self->{show_taxa} )
    {
	$self->{second_sql} = "
	SELECT DISTINCT o.collection_no, a.taxon_name
	FROM occurrences o JOIN authorities a USING (taxon_no)
		JOIN taxa_tree_cache t ON t.taxon_no = a.taxon_no
	WHERE o.collection_no = ? ORDER BY t.lft ASC";
    
	$self->{second_sth} = $dbh->prepare($self->{second_sql});
	$self->{second_sth}->execute($self->{collection_no});
    }
    
    return 1;
}


# processRecord ( row )
# 
# This routine takes a hash representing one result row, and does some
# processing before the output is generated.  The information fetched from the
# database needs to be refactored a bit in order to match the Darwin Core
# standard we are using for output.

sub processRecord {
    
    my ($self, $row) = @_;
    
    # If there's a secondary statement handle, read from it to get a list of
    # taxa for this record.  If there's a stashed record that matches this
    # row, use it first.
    
    my (@taxon_names);
    my ($row2);
        
    if ( defined $self->{stash_second} and 
	 $self->{stash_second}{collection_no} == $row->{collection_no} )
    {
	push @taxon_names, $self->{stash_second}{taxon_name};
	$self->{stash_second} = undef;
    }
    
    if ( defined $self->{second_sth} and not defined $self->{stash_record} )
    {
	while ( $row2 = $self->{second_sth}->fetchrow_hashref() )
	{
	    if ( $row2->{collection_no} != $row->{collection_no} )
	    {
		$self->{stash_second} = $row2;
		last;
	    }
	    
	    else
	    {
		push @taxon_names, $row2->{taxon_name};
	    }
	}
    }
    
    $row->{taxa} = \@taxon_names if @taxon_names > 0;
    
    # Create a publication reference if that data was included in the query
    
    if ( exists $row->{r_pubtitle} )
    {
	$self->generateReference($row);
    }
}


# generateRecord ( row, options )
# 
# This method is passed two parameters: a hash of values representing one
# record, and an indication of whether this is the first record to be output
# (which is ignored in this case).  It returns a string representing the
# record in Darwin Core XML format.

sub generateRecord {

    my ($self, $row, %options) = @_;
    
    # Output according to the proper content type.
    
    if ( $self->{output_format} eq 'xml' )
    {
	return $self->emitCollectionXML($row);
    }
    
    else
    {
	return ($options{is_first} ? "\n" : "\n,") . $self->emitCollectionJSON($row);
    }
}


# emitCollectionXML ( row, short_record )
# 
# Returns a string representing the given record (row) in Darwin Core XML
# format.  If 'short_record' is true, suppress certain fields.

sub emitCollectionXML {
    
    no warnings;
    
    my ($self, $row) = @_;
    my $output = '';
    my @remarks = ();
    
    $output .= '  <Collection>' . "\n";
    $output .= '    <dwc:collectionID>' . $row->{collection_no} . '</dwc:collectionID>' . "\n";
    
    $output .= '    <dwc:collectionCode>' . DataQuery::xml_clean($row->{collection_name}) . 
	'</dwc:collectionCode>' . "\n";
    
    if ( defined $row->{lat} )
    {
	$output .= '    <dwc:decimalLongitude>' . $row->{lng} . '</dwc:decimalLongitude>' . "\n";
	$output .= '    <dwc:decimalLatitude>' . $row->{lat} . '</dwc:decimalLatitude>' . "\n";
    }
    
    if ( ref $row->{taxa} eq 'ARRAY' and @{$row->{taxa}} )
    {
	$output .= '    <dwc:associatedTaxa>';
	$output .= DataQuery::xml_clean(join(', ', @{$row->{taxa}}));
	$output .= '</dwc:associatedTaxa>' . "\n";
    }
    
    if ( defined $row->{pubref} )
    {
	my $pubref = DataQuery::xml_clean($row->{pubref});
	$output .= '    <dwc:associatedReferences>' . $pubref . '</dwc:associatedReferences>' . "\n";
    }
    
    if ( @remarks ) {
	$output .= '    <collectionRemarks>' . DataQuery::xml_clean(join('; ', @remarks)) . 
	    '</collectionRemarks>' . "\n";
    }
    
    $output .= '  </Collection>' . "\n";
}


# emitCollectionJSON ( row, options )
# 
# Return a string representing the given taxon record (row) in JSON format.
# If 'parents' is specified, it should be an array of hashes each representing
# a parent taxon record.  If 'is_first_record' is true, then the result will
# not start with a comma.  If 'short_record' is true, then some fields will be
# suppressed.

sub emitCollectionJSON {
    
    no warnings;
    
    my ($self, $row) = @_;
    
    my $output = '';
    
    $output .= '{"collectionID":"' . $row->{collection_no} . '"'; 
    $output .= ',"collectionCode":"' . DataQuery::json_clean($row->{collection_name}) . '"';
    
    if ( defined $row->{lat} )
    {
	$output .= ',"decimalLongitude":"' . $row->{lng} . '"';
	$output .= ',"decimalLatitude":"' . $row->{lat} . '"';
    }
    
    if ( ref $row->{taxa} eq 'ARRAY' and @{$row->{taxa}} )
    {
	$output .= ',"associatedTaxa":["';
	$output .= DataQuery::xml_clean(join('","', @{$row->{taxa}}));
	$output .= '"]';
    }
    
    if ( defined $row->{pubref} )
    {
	my $pubref = DataQuery::json_clean($row->{pubref});
	$output .= ',"associatedReferences":"' . $pubref . '"';
    }
    
    # if ( defined $parents && ref $parents eq 'ARRAY' )
    # {
    # 	my $is_first_parent = 1;
    # 	$output .= ',"ancestors":[';
	
    # 	foreach my $parent_row ( @{$self->{parents}} )
    # 	{
    # 	    $output .= $self->emitTaxonJSON($parent_row, undef, $is_first_parent);
    # 	    $is_first_parent = 0;
    # 	}
	
    # 	$output .= ']';
    # }
    
    $output .= "}";
    return $output;
}


1;
