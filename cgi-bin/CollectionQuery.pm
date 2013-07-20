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

use Carp qw(carp croak);


our (%OUTPUT, %PROC);

our ($SINGLE_FIELDS) = "c.collection_no, c.collection_name, c.collection_subset, c.collection_aka, c.lat, c.lng, c.reference_no";

our ($MULT_FIELDS) = "c.collection_no, c.collection_name, c.collection_subset, c.lat, c.lng, c.reference_no";

$OUTPUT{single} = 
   [
    { rec => 'collection_no', dwc => 'collectionID', com => 'cid',
	doc => "A positive integer that uniquely identifies the collection"},
    { rec => 'lng', dwc => 'decimalLongitude', com => 'lng',
	doc => "The longitude at which the collection is located (in degrees)" },
    { rec => 'lat', dwc => 'decimalLatitude', com => 'lat',
	doc => "The latitude at which the collection is located (in degrees)" },
    { rec => 'collection_name', dwc => 'collectionCode', com => 'cna',
	doc => "An arbitrary name which identifies the collection, not necessarily unique" },
    { rec => 'collection_subset', com => 'csu',
	doc => "If this collection is a part of another one, this field specifies which part" },
    { rec => 'reference_no', com => 'prn',
        doc => "The id of the primary reference associated with the collection" },
   ];

our ($REF_FIELDS) = ", r.author1init as r_ai1, r.author1last as r_al1, r.author2init as r_ai2, r.author2last as r_al2, r.otherauthors as r_oa, r.pubyr as r_pubyr, r.reftitle as r_reftitle, r.pubtitle as r_pubtitle, r.editors as r_editors, r.pubvol as r_pubvol, r.pubno as r_pubno, r.firstpage as r_fp, r.lastpage as r_lp";

$PROC{ref} = 
   [
    { rec => 'r_al1', add => 'ref_list', use_main => 1, code => \&DataQuery::generateReference },
    { rec => 'sec_refs', add => 'ref_list', use_each => 1, code => \&DataQuery::generateReference },
    { rec => 'reference_no', add => 'refno_list' },
    { rec => 'sec_refs', add => 'refno_list', subfield => 'reference_no' },
   ];

$OUTPUT{ref} =
   [
    { rec => 'r_pubyr', com => 'pby',
	doc => "The year of publication of the primary reference associated with this collection" },
    { rec => 'ref_list', pbdb => 'references', dwc => 'associatedReferences', com => 'ref', xml_list => '; ',
	doc => "The reference(s) associated with this collection (as formatted text)" },
    { rec => 'refno_list', pbdb => 'reference_no', com => 'rid',
	doc => "A list of reference identifiers, exactly corresponding to the references listed under {pubref}" },
   ];

our ($SHORTREF_FIELDS) = ", r.author1init as r_ai1, r.author1last as r_al1, r.author2init as r_ai2, r.author2last as r_al2, r.otherauthors as r_oa, r.pubyr as r_pubyr";

$OUTPUT{sref} = 
   [
    { rec => 'pubyr', com => 'pby',
	doc => "The year of publication of the primary reference associated with this collection" },
    { rec => 'ref_list', pbdb => 'references', dwc => 'associatedReferences', com => 'ref', xml_list => '; ',
	doc => "The reference(s) associated with this collection (pubyr and authors only)" },
    { rec => 'refno_list', pbdb => 'reference_no', com => 'rid', keep => 1,
	doc => "A list of reference identifiers, exactly corresponding to the references listed under {pubref}" },
   ];

our ($TIME_FIELDS) = ", ei.interval_name as early_int, ei.base_age as early_age, li.interval_name as late_int, li.top_age as late_age";

$OUTPUT{time} =
   [
    { rec => 'early_int', com => 'int',
	doc => "The geologic time range associated with this collection, or the period that begins the range if {late_int} is also given" },
    { rec => 'late_int', com => 'lin', dedup => 'early_int',
	doc => "The period that ends the geologic time range associated with this collection" },
    { rec => 'early_age', com => 'eag',
	doc => "The early bound of the geologic time range associated with this collection (in Ma)" },
    { rec => 'late_age', com => 'lag',
	doc => "The late bound of the geologic time range associated with this collection (in Ma)" },
   ];

our ($PERS_FIELDS) = ", authorizer_no, ppa.name as authorizer, enterer_no, ppe.name as enterer";

our ($LOC_FIELDS) = ", c.country, c.state, c.county, c.paleolat, c.paleolng, c.latlng_precision";

$OUTPUT{loc} = 
   [
    { rec => 'country', com => 'cc2',
	doc => "The country in which this collection is located (ISO-3166-1 alpha-2)" },
    { rec => 'state', com => 'sta',
	doc => "The state or province in which this collection is located [not available for all collections]" },
    { rec => 'county', com => 'cny',
	doc => "The county in which this collection is located [not available for all collections]" },
    { rec => 'latlng_precision', com => 'gpr',
	doc => "The precision of the collection location (degrees/minutes/seconds/#digits)" },
   ];

$OUTPUT{rem} = 
   [
    { rec => 'collection_aka', dwc => 'collectionRemarks', com => 'crm', xml_list => '; ',
	doc => "Any additional remarks that were entered about the colection"},
   ];

our (%DOC_ORDER);

$DOC_ORDER{'single'} = ['single', 'ref', 'time', 'loc', 'rem'];
$DOC_ORDER{'list'} = ();
$DOC_ORDER{'summary'} = ();


# getOutputFields ( )
# 
# Determine the list of output fields, given the name of a section to display.

sub getOutputFields {
    
    my ($self, $section) = @_;
    
    return @{$OUTPUT{$section}}
	if ref $OUTPUT{$section} eq 'ARRAY';
    return;
}


# getProcFields ( )
# 
# Determine the list of processing fields, given the name of a section to display

sub getProcFields {

    my ($self, $section) = @_;
    
    return @{$PROC{$section}}
	if ref $PROC{$section} eq 'ARRAY';
    return;
}


# fetchSingle ( )
# 
# Query for all relevant information about the collection specified by the
# 'id' parameter.  Returns true if the query succeeded, false otherwise.

sub fetchSingle {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # Make sure we have a valid id number.
    
    my $id = $self->{params}{id};
    
    die "Bad identifier '$id'" unless defined $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my ($extra_fields, $tables) = $self->generateQueryFields($self->{show_order});
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('c', $tables);
    
    # Generate the main query.
    
    $self->{main_sql} = "
	SELECT $SINGLE_FIELDS $extra_fields
	FROM collections c
		$join_list
        WHERE c.collection_no = $id";
    
    $self->{main_record} = $dbh->selectrow_hashref($self->{main_sql});
    
    # Abort if we couldn't retrieve the record.
    
    return unless $self->{main_record};
    
    # If we were directed to show references, grab any secondary references.
    
    if ( $self->{show}{ref} or $self->{show}{sref} )
    {
	my (@fields) = 'sref' if $self->{show}{sref};
	@fields = 'ref' if $self->{show}{ref};
	
	($extra_fields) = $self->generateQueryFields(\@fields);
	
	$self->{aux_sql}[0] = "
	SELECT s.reference_no $extra_fields
	FROM secondary_refs as s JOIN refs as r using (reference_no)
	WHERE s.collection_no = $id";
	
	$self->{main_record}{sec_refs} = $dbh->selectall_arrayref($self->{aux_sql}[0], { Slice => {} });
    }
    
    # If we were directed to show associated taxa, grab them too.
    
    if ( $self->{show}{taxa} )
    {
	my $auth_table = $self->{taxonomy}{auth_table};
	my $tree_table = $self->{taxonomy}{tree_table};
	
	$self->{aux_sql}[1] = "
	SELECT DISTINCT a.taxon_no, a.taxon_name, a.taxon_rank, a.orig_no, t.name, t.rank, t.spelling_no
	FROM occurrences as o JOIN $auth_table as a USING (taxon_no)
		LEFT JOIN $tree_table as t using (orig_no)
	WHERE o.collection_no = $id ORDER BY t.lft ASC";
	
	$self->{main_record}{taxa} = $dbh->selectall_arrayref($self->{aux_sql}[1], { Slice => {} });
    }
    
    return 1;
}


# fetchMultiple ( )
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


# processRecord ( row )
# 
# This routine takes a hash representing one result row, and does some
# processing before the output is generated.  The information fetched from the
# database needs to be refactored a bit in order to match the Darwin Core
# standard we are using for output.

# sub processRecord {
    
#     my ($self, $row) = @_;
    
#     # If there's a secondary statement handle, read from it to get a list of
#     # taxa for this record.  If there's a stashed record that matches this
#     # row, use it first.
    
#     my (@taxon_names);
#     my ($row2);
        
#     if ( defined $self->{stash_second} and 
# 	 $self->{stash_second}{collection_no} == $row->{collection_no} )
#     {
# 	push @taxon_names, $self->{stash_second}{taxon_name};
# 	$self->{stash_second} = undef;
#     }
    
#     if ( defined $self->{second_sth} and not defined $self->{stash_record} )
#     {
# 	while ( $row2 = $self->{second_sth}->fetchrow_hashref() )
# 	{
# 	    if ( $row2->{collection_no} != $row->{collection_no} )
# 	    {
# 		$self->{stash_second} = $row2;
# 		last;
# 	    }
	    
# 	    else
# 	    {
# 		push @taxon_names, $row2->{taxon_name};
# 	    }
# 	}
#     }
    
#     $row->{taxa} = \@taxon_names if @taxon_names > 0;
    
#     # Create a publication reference if that data was included in the query
    
#     if ( exists $row->{r_pubtitle} and $self->{show}{ref} )
#     {
# 	$self->generateReference($row);
#     }
# }


# generateRecord ( row, options )
# 
# This method is passed two parameters: a hash of values representing one
# record, and an indication of whether this is the first record to be output.
# (which is ignored in this case).  It returns a string representing the
# record in Darwin Core XML format.

# sub generateRecord {

#     my ($self, $row, %options) = @_;
    
#     # Output according to the proper content type.
    
#     if ( $self->{output_format} eq 'xml' )
#     {
# 	return $self->emitCollectionXML($row);
#     }
    
#     elsif ( $self->{output_format} eq 'json' )
#     {
# 	return ($options{is_first} ? "\n" : "\n,") . $self->emitCollectionJSON($row);
#     }
    
#     elsif ( $self->{output_format} eq 'txt' or $self->{output_format} eq 'csv' )
#     {
# 	return $self->emitCollectionText($row);
#     }
# }


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
	$output .= DataQuery::xml_clean(join(', ', map { $_->{taxon_name} } @{$row->{taxa}}));
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
    
    $output .= '{"collectionID":"' . DataQuery::json_clean($row->{collection_no}) . '"'; 
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


# generateQueryFields ( fields )
# 
# The parameter 'fields' should be a hash whose keys are strings, or a
# comma-separated list of strings.
# 
# This routine returns a field string and a hash which lists extra tables to
# be joined in the query.

sub generateQueryFields {

    my ($self, $fields_ref) = @_;
    
    # Return the default if our parameter is undefined.
    
    unless ( ref $fields_ref eq 'ARRAY' )
    {
	return '', {};
    }
    
    # Now go through the list of strings and add the appropriate fields and
    # tables for each.
    
    my $fields = '';
    my %tables;
    
    foreach my $inc (@$fields_ref)
    {
	if ( $inc eq 'ref' )
	{
	    $fields .= $REF_FIELDS;
	    $tables{ref} = 1;
	}
	
	elsif ( $inc eq 'pers' )
	{
	    $fields .= $PERS_FIELDS;
	    $tables{ppa} = 1;
	    $tables{ppe} = 1;
	}
	
	elsif ( $inc eq 'loc' )
	{
	    $fields .= $LOC_FIELDS;
	}
	
	elsif ( $inc eq 'time' )
	{
	    $fields .= $TIME_FIELDS;
	    $tables{int} = 1;
	}
	
	elsif ( $inc eq 'taxa' )
	{
	    # nothing needed here
	}
	
	elsif ( $inc eq 'occ' )
	{
	    # nothing needed here
	}
	
	elsif ( $inc eq 'det' )
	{
	    #$fields .= $DETAIL_FIELDS;
	}
	
	else
	{
	    carp "unrecognized value '$inc' for option 'fields'";
	}
    }
    
    return ($fields, \%tables);
}


# generateJoinList ( tables )
# 
# Generate the actual join string indicated by the table hash.

sub generateJoinList {

    my ($self, $mt, $tables) = @_;
    
    my $join_list = '';
    
    # Return an empty string unless we actually have some joins to make
    
    return $join_list unless ref $tables eq 'HASH' and %$tables;
    
    # Create the necessary join expressions.
    
    $join_list .= "LEFT JOIN refs as r on r.reference_no = $mt.reference_no\n" 
	if $tables->{ref};
    $join_list .= "JOIN interval_map as ei on ei.interval_no = $mt.max_interval_no
		JOIN interval_map as li on li.interval_no = \
		if($mt.min_interval_no > 0, $mt.min_interval_no, $mt.max_interval_no)\n"
	if $tables->{int};
    $join_list .= "LEFT JOIN person as ppa on ppa.person_no = $mt.authorizer_no\n"
	if $tables->{ppa};
    $join_list .= "LEFT JOIN person as ppe on ppe.person_no = $mt.enterer_no\n"
	if $tables->{ppe};
    
    return $join_list;
}


1;
