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


# setParameters ( params )
# 
# This method accepts a hash of parameter values, filters them for correctness,
# and sets the appropriate fields of the query object.  It is designed to be
# called from a Dancer route, although that is not a requirement.

sub setParameters {
    
    my ($self, $params) = @_;
    
    # First tell our superclass (PBDataQuery) to set any parameters it
    # recognizes.
    
    $self->PBDataQuery::setParameters($params);
    
    # The 'taxon' parameter restricts the output to collections that contain
    # occurrences of the given taxa or any of their children.  Multiple taxa
    # can be specified, separated by commas.  Each taxon may be qualified by
    # one or more ranks, following the taxon and preceded by a period.
    # Otherwise, all matching taxa will be used.  Matches are all
    # case-insensitive.  Examples: taxon=Pinnipedia,Bovidae
    # taxon=Pinnipedia.order,Bovidae.family
    
    if ( defined $params->{taxon} )
    {
	$self->{taxon_filter} = $params->{taxon};
    }
    
    # The 'geoloc' parameter restricts the output to collections whose
    # location falls within the bounding box of the given geometry, specified
    # in WKT format.
    
    if ( defined $params->{loc} )
    {
	$self->{location_filter} = $params->{loc};
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
	    $self->{error} = "parameter error";
	    return;
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
    
    $sql = 
	"SELECT DISTINCT c.collection_no, c.collection_name, c.lat, c.lng $extra_fields
         FROM collections c JOIN occurrences o using (collection_no)
			    $taxon_tables
	 $taxon_filter $limit_stmt";
    
    $self->{main_sth} = $dbh->prepare($sql);
    $self->{main_sth}->execute();
    
    # Indicate that output should be streamed rather than assembled and
    # returned immediately.  This will avoid a huge burden on the server to
    # marshall a single result string from a potentially large set of data.
    
    $self->{streamOutput} = 1;
    $self->{processMethod} = 'processRow';
    
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

    my ($self, $collection_requested) = @_;
    my ($sql, @extra_fields);
    
    # Get ahold of a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    my $extra_fields = join(', ', @extra_fields);
    $extra_fields = ", " . $extra_fields if $extra_fields ne '';
    
    # Next, fetch basic info about the collection.
    
    $sql = 
	"SELECT c.collection_no, c.collection_name, c.lat, c.lng $extra_fields
	 FROM collections c JOIN occurrences o using (collection_no)
         WHERE c.collection_no = ?";
    
    my $sth = $dbh->prepare($sql);
    $sth->execute($collection_requested);
    
    my ($main_row) = $sth->fetchrow_hashref();
    
    $self->processRow($main_row);
    $self->{main_row} = $main_row;
    
    # Now fetch all info about the taxa represented in this collection
    
    $sql = 
       "SELECT DISTINCT a.taxon_no, a.taxon_name, a.taxon_rank, a.common_name, a.extant
	FROM occurrences o JOIN taxa_list_cache l ON o.taxon_no = l.child_no
		JOIN authorities a ON a.taxon_no = l.parent_no
		JOIN taxa_tree_cache t ON t.taxon_no = l.parent_no
	WHERE o.collection_no = ? AND l.parent_no <> l.child_no ORDER BY t.lft ASC";
    
    $sth = $dbh->prepare($sql);
    $sth->execute($collection_requested);
    
    my $taxa_list = $sth->fetchall_arrayref({});
    
    # Run through the parent list and note when we reach the last
    # kingdom-level taxon.  Any entries before that point are dropped 
    # [note: TaxonInfo.pm, line 1316]
    
    my $last_kingdom = 0;
    
    for (my $i = 0; $i < scalar(@$taxa_list); $i++)
    {
	$last_kingdom = $i if $taxa_list->[$i]{taxon_rank} eq 'kingdom';
    }
    
    splice(@$taxa_list, 0, $last_kingdom) if $last_kingdom > 0;
    
    $self->{taxa} = $taxa_list;
    
    # Return success
    
    return 1;
}


# processRow ( row )
# 
# This routine takes a hash representing one result row, and does some
# processing before the output is generated.  The information fetched from the
# database needs to be refactored a bit in order to match the Darwin Core
# standard we are using for output.

sub processRow {
    
    my ($self, $row) = @_;
    
   # Create a publication reference if that data was included in the query
    
    if ( exists $row->{r_pubtitle} )
    {
	$self->add_reference($row);
    }
}


# emitRecordXML ( main_row, is_first )
# 
# This method is passed two parameters: a hash of values representing one
# record, and an indication of whether this is the first record to be output
# (which is ignored in this case).  It returns a string representing the
# record in Darwin Core XML format.

sub emitRecordXML {

    my ($self, $main_row, $is_first) = @_;
    
    my $output = '';
    
    # If our query included parent info, then it was a single-record query.
    # Because of the inflexibility of XML, the best we can do is to output all
    # of the parent records first, before the main record.  We have no way of
    # indicating a hierarchy (see http://eol.org/api/docs/hierarchy_entries).
    
    if ( defined $self->{parents} && ref $self->{parents} eq 'ARRAY' )
    {
	foreach my $parent_row ( @{$self->{parents}} )
	{
	    $output .= $self->emitCollectionXML($parent_row, 1);
	}
    }
    
    # Now, we output the main record.
    
    $output .= $self->emitCollectionXML($main_row, 0);
    return $output;
}


# emitCollectionXML ( row, short_record )
# 
# Returns a string representing the given record (row) in Darwin Core XML
# format.  If 'short_record' is true, suppress certain fields.

sub emitCollectionXML {
    
    no warnings;
    
    my ($self, $row, $short_record) = @_;
    my $output = '';
    my @remarks = ();
    
    $output .= '  <Collection>' . "\n";
    $output .= '    <dwc:collectionID>' . $row->{collection_no} . '</dwc:collectionID>' . "\n";
    
    $output .= '    <dwc:collectionCode>' . PBXML::xml_clean($row->{collection_name}) . 
	'</dwc:collectionCode>' . "\n";
    
    if ( defined $row->{lat} )
    {
	$output .= '  <dwc:decimalLongitude>' . $row->{lng} . '</dwc:decimalLongitude>' . "\n";
	$output .= '  <dwc:decimalLatitude>' . $row->{lat} . '</dwc:decimalLatitude>' . "\n";
    }
    
    if ( defined $row->{pubref} )
    {
	my $pubref = PBXML::xml_clean($row->{pubref});
	# We now need to translate, i.e. ((b)) to <b>.  This is done after
	# xml_clean, because otherwise <b> would be turned into &lt;b&gt;
	if ( $pubref =~ /\(\(/ )
	{
	    #$row->{pubref} =~ s/(\(\(|\)\))/$fixbracket{$1}/eg;
	    #actually, we're just going to take them out for now
	    $pubref =~ s/\(\(\/?\w*\)\)//g;
	}
	$output .= '    <dwc:associatedReferences>' . $pubref . '</dwc:associatedReferences>' . "\n";
    }
    
    if ( @remarks ) {
	$output .= '    <collectionRemarks>' . join('; ', @remarks) . '</collectionRemarks>' . "\n";
    }
    
    $output .= '  </Collection>' . "\n";
}


# emitRecordJSON ( row, is_first_record )
# 
# Return a string representing the given record in JSON format.

sub emitRecordJSON {
    
    my ($self, $main_row, $is_first_record) = @_;
    
    return $self->emitCollectionJSON($main_row, $self->{parents}, $is_first_record, 0);
}


# emitTaxonJSON ( row, parents, is_first_record, short_record )
# 
# Return a string representing the given taxon record (row) in JSON format.
# If 'parents' is specified, it should be an array of hashes each representing
# a parent taxon record.  If 'is_first_record' is true, then the result will
# not start with a comma.  If 'short_record' is true, then some fields will be
# suppressed.

sub emitCollectionJSON {
    
    no warnings;
    
    my ($self, $row, $parents, $is_first_record, $short_record) = @_;
    
    my $output = ($is_first_record ? "\n" : "\n,");
    
    $output .= '{"collectionID":"' . $row->{collection_no} . '"'; 
    $output .= ',"collectionCode":"' . PBJSON::json_clean($row->{collection_name}) . '"';
    
    if ( defined $row->{lat} )
    {
	$output .= ',"decimalLongitude":"' . $row->{lng} . '"';
	$output .= ',"decimalLatitude":"' . $row->{lat} . '"';
    }
    
    if ( defined $row->{pubref} )
    {
	my $pubref = PBJSON::json_clean($row->{pubref});
	$pubref =~ s/\(\(\/?\w*\)\)//g;
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
