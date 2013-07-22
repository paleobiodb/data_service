#
# CollectionQuery
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

our ($SINGLE_FIELDS) = "c.collection_no, cc.collection_name, cc.collection_subset, cc.collection_aka, c.lat, c.lng, c.reference_no";

our ($LIST_FIELDS) = "c.collection_no, cc.collection_name, cc.collection_subset, c.lat, c.lng, c.reference_no";

our ($SUMMARY_1) = "s.clust_id as sum_id, s.n_colls, s.n_occs, s.lat, s.lng";

our ($SUMMARY_1A) = "s.clust_id as sum_id, count(distinct collection_no) as n_colls, count(distinct occurrence_no) as n_occs, s.lat, s.lng";

our ($SUMMARY_2) = "s.bin_id as sum_id, s.n_colls, s.n_occs, s.lat, s.lng";

our ($SUMMARY_2A) = "s.bin_id as sum_id, count(distinct collection_no) as n_colls, count(distinct occurrence_no) as n_occs, s.lat, s.lng";

$OUTPUT{single} = $OUTPUT{list} = 
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

$OUTPUT{summary} = 
   [
    { rec => 'sum_id', com => 'bin', doc => "A positive integer that identifies the summary bin" },
    { rec => 'n_colls', com => 'nco', doc => "The number of collections in this bin or cluster" },
    { rec => 'n_occs', com => 'noc', doc => "The number of occurrences in this bin or cluster" },
    { rec => 'lng', com => 'lng', doc => "The longitude of the centroid of this bin or cluster" },
    { rec => 'lat', com => 'lat', doc => "The latitude of the centroid of this bin or cluster" },
   ];

our ($BIN_FIELDS) = ", c.bin_id, c.clust_id";

$OUTPUT{bin} = 
   [
    { rec => 'bin_id', com => 'bin', doc => "The identifier of the bin in which this collection is located" },
    { rec => 'clust_id', com => 'clu', doc => "The identifier of the cluster in which this collection is located" },
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
    { rec => 'r_pubyr', com => 'pby',
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

$OUTPUT{taxa} = 
   [
    { rec => 'taxa', com => 'tax',
      doc => "A list of records describing the taxa that have been identified as appearing in this collection",
      rule => [{ rec => 'taxon_name', com => 'tna',
		 doc => "The scientific name of the taxon" },
	       { rec => 'taxon_rank', com => 'trn',
		 doc => "The taxonomic rank" },
	       { rec => 'taxon_no', com => 'tid',
		 doc => "A positive integer that uniquely identifies the taxon" },
	       { rec => 'ident_name', com => 'ina', dedup => 'taxon_name',
		 doc => "The name under which the occurrence was actually identified" },
	       { rec => 'ident_rank', com => 'irn', dedup => 'taxon_rank',
		 doc => "The taxonomic rank as actually identified" },
	       { rec => 'ident_no', com => 'iid', dedup => 'taxon_no',
		 doc => "A positive integer that uniquely identifies the name as identified" }]
    }
   ];

$OUTPUT{rem} = 
   [
    { rec => 'collection_aka', dwc => 'collectionRemarks', com => 'crm', xml_list => '; ',
	doc => "Any additional remarks that were entered about the colection"},
   ];

our ($EXT_FIELDS) = ", s.lng_min, s.lng_max, s.lat_min, s.lat_max";

$OUTPUT{ext} =
   [
    { rec => 'lng_min', com => 'lg1', doc => "The mimimum longitude for collections in this bin or cluster" },
    { rec => 'lng_max', com => 'lg2', doc => "The maximum longitude for collections in this bin or cluster" },
    { rec => 'lat_min', com => 'la1', doc => "The mimimum latitude for collections in this bin or cluster" },
    { rec => 'lat_max', com => 'la2', doc => "The maximum latitude for collections in this bin or cluster" },
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
    
    my $tables = {};
    
    my $extra_fields = $self->generateQueryFields($self->{show_order}, $tables);
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('c', $tables);
    
    # Generate the main query.
    
    $self->{main_sql} = "
	SELECT $SINGLE_FIELDS $extra_fields
	FROM coll_matrix as c JOIN collections as cc using (collection_no)
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
	SELECT DISTINCT t.spelling_no as taxon_no, t.name as taxon_name, rm.rank as taxon_rank, 
		a.taxon_no as ident_no, a.taxon_name as ident_name, a.taxon_rank as ident_rank
	FROM occ_matrix as o JOIN $auth_table as a USING (taxon_no)
		LEFT JOIN $tree_table as t on t.orig_no = o.orig_no
		LEFT JOIN rank_map as rm on rm.rank_no = t.rank
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
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    my $tables = {};
    my $calc = '';
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = $self->generateQueryFilters($dbh, $tables);
    
    croak "No filters were specified for fetchMultiple" unless @filters;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $extra_fields = $self->generateQueryFields($self->{show_order}, $tables);
    
   # Determine the necessary joins.
    
    my $join_list = $self->generateJoinList('c', $tables);
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->generateLimitClause();
    
    # If we were asked to count rows, modify the query accordingly
    
    if ( $self->{params}{count} )
    {
	$calc = 'SQL_CALC_FOUND_ROWS';
    }
    
    # Generate the main query.
    
    my $filter_list = join(' and ', @filters);
    
    if ( $self->{op} eq 'summary' and $self->{params}{level} == 2 ) 
    {
	my $fields = $tables->{c} ? $SUMMARY_2A : $SUMMARY_2;
	
	$self->{main_sql} = "
	SELECT $calc $fields $extra_fields
	FROM coll_bins as s $join_list
	WHERE $filter_list
	GROUP BY s.bin_id
	ORDER BY s.bin_id
	$limit";
    }
    
    elsif ( $self->{op} eq 'summary' ) 
    {
	my $fields = $tables->{c} ? $SUMMARY_1A : $SUMMARY_1;
	
	$self->{main_sql} = "
	SELECT $calc $fields $extra_fields
	FROM clusters as s $join_list
	WHERE $filter_list
	GROUP BY s.clust_id
	ORDER BY s.clust_id
	$limit";
    }
    
    else
    {
	$self->{main_sql} = "
	SELECT $calc $LIST_FIELDS $extra_fields
	FROM coll_matrix as c join collections as cc using (collection_no)
		$join_list
        WHERE $filter_list
	GROUP BY c.collection_no
	ORDER BY c.collection_no
	$limit";
    }
    
    # Then prepare and execute the main query and the secondary query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    if ( $calc )
    {
	($self->{result_count}) = $dbh->selectrow_array("SELECT FOUND_ROWS()");
    }
    
    # If we were directed to show references, grab any secondary references.
    
    # if ( $self->{show}{ref} or $self->{show}{sref} )
    # {
    # 	my (@fields) = 'sref' if $self->{show}{sref};
    # 	@fields = 'ref' if $self->{show}{ref};
	
    # 	($extra_fields) = $self->generateQueryFields(\@fields);
	
    # 	$self->{aux_sql}[0] = "
    # 	SELECT s.reference_no $extra_fields
    # 	FROM secondary_refs as s JOIN refs as r using (reference_no)
    # 	WHERE s.collection_no = ?";
	
    # 	$self->{aux_sth}[0] = $dbh->prepare($self->{aux_sql}[0]);
    # 	$self->{aux_sth}[0]->execute();
    # }
    
    # If we were directed to show associated taxa, construct an SQL statement
    # that will be used to grab that list.
    
    # if ( $self->{show}{taxa} )
    # {
    # 	my $auth_table = $self->{taxonomy}{auth_table};
    # 	my $tree_table = $self->{taxonomy}{tree_table};
	
    # 	$self->{aux_sql}[1] = "
    # 	SELECT DISTINCT t.spelling_no as taxon_no, t.name as taxon_name, rm.rank as taxon_rank, 
    # 		a.taxon_no as ident_no, a.taxon_name as ident_name, a.taxon_rank as ident_rank
    # 	FROM occ_matrix as o JOIN $auth_table as a USING (taxon_no)
    # 		LEFT JOIN $tree_table as t on t.orig_no = o.orig_no
    # 		LEFT JOIN rank_map as rm on rm.rank_no = t.rank
    # 	WHERE o.collection_no = ? ORDER BY t.lft ASC";
		
    # 	$self->{aux_sth}[1] = $dbh->prepare($self->{aux_sql}[1]);
    # 	$self->{aux_sth}[1]->execute();
    # }
    
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


# emitCollectionXML ( row, short_record )
# 
# Returns a string representing the given record (row) in Darwin Core XML
# format.  If 'short_record' is true, suppress certain fields.

# sub emitCollectionXML {
    
#     no warnings;
    
#     my ($self, $row) = @_;
#     my $output = '';
#     my @remarks = ();
    
#     $output .= '  <Collection>' . "\n";
#     $output .= '    <dwc:collectionID>' . $row->{collection_no} . '</dwc:collectionID>' . "\n";
    
#     $output .= '    <dwc:collectionCode>' . DataQuery::xml_clean($row->{collection_name}) . 
# 	'</dwc:collectionCode>' . "\n";
    
#     if ( defined $row->{lat} )
#     {
# 	$output .= '    <dwc:decimalLongitude>' . $row->{lng} . '</dwc:decimalLongitude>' . "\n";
# 	$output .= '    <dwc:decimalLatitude>' . $row->{lat} . '</dwc:decimalLatitude>' . "\n";
#     }
    
#     if ( ref $row->{taxa} eq 'ARRAY' and @{$row->{taxa}} )
#     {
# 	$output .= '    <dwc:associatedTaxa>';
# 	$output .= DataQuery::xml_clean(join(', ', map { $_->{taxon_name} } @{$row->{taxa}}));
# 	$output .= '</dwc:associatedTaxa>' . "\n";
#     }
    
#     if ( defined $row->{pubref} )
#     {
# 	my $pubref = DataQuery::xml_clean($row->{pubref});
# 	$output .= '    <dwc:associatedReferences>' . $pubref . '</dwc:associatedReferences>' . "\n";
#     }
    
#     if ( @remarks ) {
# 	$output .= '    <collectionRemarks>' . DataQuery::xml_clean(join('; ', @remarks)) . 
# 	    '</collectionRemarks>' . "\n";
#     }
    
#     $output .= '  </Collection>' . "\n";
# }


# generateQueryFields ( fields )
# 
# The parameter 'fields' should be a hash whose keys are strings, or a
# comma-separated list of strings.
# 
# This routine returns a field string and a hash which lists extra tables to
# be joined in the query.

sub generateQueryFields {

    my ($self, $fields_ref, $tables_ref) = @_;
    
    # Return the default if our parameter is undefined.
    
    unless ( ref $fields_ref eq 'ARRAY' )
    {
	return '';
    }
    
    # Now go through the list of strings and add the appropriate fields and
    # tables for each.
    
    my $fields = '';
    
    foreach my $inc (@$fields_ref)
    {
	if ( $inc eq 'bin' )
	{
	    $fields .= $BIN_FIELDS;
	}
	
	elsif ( $inc eq 'ref' )
	{
	    $fields .= $REF_FIELDS;
	    $tables_ref->{ref} = 1;
	}
	
	elsif ( $inc eq 'pers' )
	{
	    $fields .= $PERS_FIELDS;
	    $tables_ref->{ppa} = 1;
	    $tables_ref->{ppe} = 1;
	}
	
	elsif ( $inc eq 'loc' )
	{
	    $fields .= $LOC_FIELDS;
	}
	
	elsif ( $inc eq 'time' )
	{
	    $fields .= $TIME_FIELDS;
	    $tables_ref->{int} = 1;
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
	
	elsif ( $inc eq 'ext' )
	{
	    $fields .= $EXT_FIELDS;
	}
	
	else
	{
	    carp "unrecognized value '$inc' for option 'fields'";
	}
    }
    
    return $fields;
}


# generateQueryFilters ( tables )
# 
# Generate a list of filter clauses that will be used to generate the
# appropriate result set.

sub generateQueryFilters {

    my ($self, $dbh, $tables_ref) = @_;
    
    my @filters;
    
    # Check for parameter 'id'
    
    if ( ref $self->{params}{id} eq 'ARRAY' and
	 @{$self->{params}{id}} )
    {
	my $id_list = join(',', @{$self->{params}{id}});
	push @filters, "c.collection_no in ($id_list)";
    }
    
    # Check for parameter 'bin_id'
    
    if ( ref $self->{params}{bin_id} eq 'ARRAY' )
    {
	my @bins = grep { $_ > 0 } @{$self->{params}{bin_id}};
	my $first_bin = $bins[0];
	my $list = join(',', @bins);
	
	if ( $first_bin >= 200000000 )
	{
	    push @filters, "c.bin_id in ($list)";
	}
	
	elsif ( $first_bin >= 1000000 )
	{
	    push @filters, "c.clust_id in ($list)";
	}
    }
    
    # Check for parameters 'taxon_name', 'base_name', 'taxon_id', 'base_id'
    
    my $taxon_name = $self->{params}{taxon_name} || $self->{params}{base_name};
    my $taxon_no = $self->{params}{taxon_id} || $self->{params}{base_id};
    my @taxa;
    
    # First get the relevant taxon records
    
    if ( $taxon_name )
    {
	(@taxa) = $self->{taxonomy}->getTaxaByName($taxon_name, { fields => 'lft' });
    }
    
    elsif ( $taxon_no )
    {
	(@taxa) = $self->{taxonomy}->getRelatedTaxa('self', $taxon_no, { fields => 'lft' });
    }
    
    # Then construct the necessary filters
    
    if ( @taxa and ($self->{params}{base_name} or $self->{params}{base_id}) )
    {
	my $taxon_filters = join ' or ', map { "t.lft between $_->{lft} and $_->{rgt}" } @taxa;
	push @filters, "($taxon_filters)";
	$tables_ref->{t} = 1;
    }
    
    elsif ( @taxa )
    {
	my $taxon_list = join ',', map { $_->{orig_no} } @taxa;
	push @filters, "o.orig_no in ($taxon_list)";
	$tables_ref->{o} = 1;
    }
    
    # Check for parameters 'lngmin', 'lngmax', 'latmin', 'latmax'
    
    if ( defined $self->{params}{lngmin} )
    {
	my $x1 = $self->{params}{lngmin};
	my $x2 = $self->{params}{lngmax};
	my $y1 = $self->{params}{latmin};
	my $y2 = $self->{params}{latmax};
	
	if ( $self->{op} eq 'summary' )
	{
	    push @filters, "s.lng between $x1 and $x2 and s.lat between $y1 and $y2";
	}
	
	else
	{
	    my $polygon = "'POLYGON(($x1 $y1,$x2 $y1,$x2 $y2,$x1 $y2,$x1 $y1))'";
	    push @filters, "mbrwithin(loc, geomfromtext($polygon))";
	}
    }
    
    if ( $self->{params}{loc} )
    {
	push @filters, "st_within(loc, geomfromtext($self->{params}{loc})";
    }
    
    # Check for parameters 'min_ma', 'max_ma', 'interval'
    
    my $min_age = $self->{params}{min_ma};
    my $max_age = $self->{params}{max_ma};
    
    if ( $self->{params}{interval} )
    {
	my $quoted_name = $dbh->quote($self->{params}{interval});
	
	my $sql = "SELECT base_age, top_age FROM interval_map
		   WHERE interval_name like $quoted_name";
	
	($max_age, $min_age) = $dbh->selectrow_array($sql);
    }
    
    if ( defined $min_age and $min_age > 0 )
    {
	my $min_filt = $self->{params}{time_strict} ? "c.late_age" : "c.early_age";
	push @filters, "$min_filt > $min_age";
    }
    
    if ( defined $max_age and $max_age > 0 )
    {
	my $max_filt = $self->{params}{time_strict} ? "c.early_age" : "c.late_age";
	push @filters, "$max_filt < $max_age";
    }
    
    # Return the list
    
    return @filters;
}


# generateJoinList ( tables )
# 
# Generate the actual join string indicated by the table hash.

sub generateJoinList {

    my ($self, $mt, $tables) = @_;
    
    my $join_list = '';
    
    # Return an empty string unless we actually have some joins to make
    
    return $join_list unless ref $tables eq 'HASH' and %$tables;
    
    # Some tables imply others.
    
    $tables->{o} = 1 if $tables->{t};
    $tables->{c} = $self->{params}{level} if $tables->{o} and $self->{op} eq 'summary';
    
    # Create the necessary join expressions.
    
    $join_list .= "JOIN coll_matrix as c using (bin_id)\n"
	if $tables->{c} == 2;
    $join_list .= "JOIN coll_matrix as c using (clust_id)\n"
	if $tables->{c} == 1;
    $join_list .= "JOIN occ_matrix as o using (collection_no)\n"
	if $tables->{o};
    $join_list .= "JOIN taxon_trees as t using (orig_no)\n"
	if $tables->{t};
    $join_list .= "LEFT JOIN refs as r on r.reference_no = $mt.reference_no\n" 
	if $tables->{ref};
    $join_list .= "JOIN interval_map as ei on ei.interval_no = $mt.early_int_no
		JOIN interval_map as li on li.interval_no = $mt.late_int_no\n"
	if $tables->{int};
    $join_list .= "LEFT JOIN person as ppa on ppa.person_no = $mt.authorizer_no\n"
	if $tables->{ppa};
    $join_list .= "LEFT JOIN person as ppe on ppe.person_no = $mt.enterer_no\n"
	if $tables->{ppe};
    
    return $join_list;
}


sub generateLimitClause {

    my ($self) = @_;
    
    my $limit = $self->{params}{limit};
    my $offset = $self->{params}{offset};
    
    if ( defined $offset and $offset > 0 )
    {
	$offset += 0;
	$limit = $limit eq 'all' ? 10000000 : $limit + 0;
	return "LIMIT $offset,$limit";
    }
    
    elsif ( defined $limit and $limit ne 'all' )
    {
	return "LIMIT " . ($limit + 0);
    }
    
    return '';
}

1;
