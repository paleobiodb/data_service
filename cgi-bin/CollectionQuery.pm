
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


our (%SELECT, %TABLES, %PROC, %OUTPUT);

$SELECT{single} = "c.collection_no, cc.collection_name, cc.collection_subset, cc.collection_aka, cc.formation, c.lat, c.lng, cc.latlng_basis as llb, cc.latlng_precision as llp, c.n_occs, icm.container_no, c.reference_no, group_concat(sr.reference_no) as sec_ref_nos";

$SELECT{list} = "c.collection_no, cc.collection_name, cc.collection_subset, cc.formation, c.lat, c.lng, cc.latlng_basis as llb, cc.latlng_precision as llp, c.n_occs, icm.container_no, c.reference_no, group_concat(sr.reference_no) as sec_ref_nos";

our ($SUMMARY_1) = "s.clust_id as sum_id, s.n_colls, s.n_occs, s.lat, s.lng, icm.container_no";

our ($SUMMARY_2) = "s.bin_id as sum_id, s.clust_id, s.n_colls, s.n_occs, s.lat, s.lng, icm.container_no";

our ($SUMMARY_S) = "s.clust_id, s.n_colls, s.n_occs, s.lat, s.lng, s.early_seq, s.late_seq";

our ($SUMMARY_C) = "s.clust_id, count(distinct c.collection_no) as n_colls, sum(c.n_occs) as n_occs, s.lat, s.lng, min(c.early_seq) as early_seq, min(c.late_seq) as late_seq";

our ($SUMMARY_M) = "s.clust_id, count(distinct c.collection_no) as n_colls, count(distinct m.occurrence_no) as n_occs, s.lat, s.lng, min(c.early_seq) as early_seq, min(c.late_seq) as late_seq";


$PROC{single} = $PROC{list} =
   [
    { rec => 'sec_ref_nos', add => 'reference_no', split => ',' },
   ];

$OUTPUT{single} = $OUTPUT{list} = 
   [
    { rec => 'collection_no', dwc => 'collectionID', com => 'oid',
	doc => "A positive integer that uniquely identifies the collection"},
    { rec => 'record_type', com => 'typ', com_value => 'col', dwc_value => 'Occurrence', value => 'collection',
        doc => "The type of this object: 'col' for a collection" },
    { rec => 'formation', com => 'fmm', doc => "The formation in which this collection was found" },
    { rec => 'lng', dwc => 'decimalLongitude', com => 'lng',
	doc => "The longitude at which the collection is located (in degrees)" },
    { rec => 'lat', dwc => 'decimalLatitude', com => 'lat',
	doc => "The latitude at which the collection is located (in degrees)" },
    { rec => 'llp', com => 'prc', use_main => 1, code => \&CollectionQuery::generateBasisCode,
        doc => "A two-letter code indicating the basis and precision of the geographic coordinates." },
    { rec => 'collection_name', dwc => 'collectionCode', com => 'nam',
	doc => "An arbitrary name which identifies the collection, not necessarily unique" },
    { rec => 'collection_subset', com => 'nm2',
	doc => "If this collection is a part of another one, this field specifies which part" },
    { rec => 'n_occs', com => 'noc',
        doc => "The number of occurrences in this collection" },
    { rec => 'container_no', com => 'cxi',
        doc => "The identifier of the most specific standard interval covering the entire time range associated with this collection" },
    { rec => 'reference_no', com => 'rid', json_list => 1,
        doc => "The identifier(s) of the references from which this data was entered" },
   ];

$TABLES{single} = $TABLES{list} = $TABLES{summary} = ['icm'];

$OUTPUT{summary} = 
   [
    { rec => 'sum_id', com => 'oid', doc => "A positive integer that identifies the cluster" },
    { rec => 'clust_id', com => 'cl1', doc => "A positive integer that identifies the containing cluster, if any" },
    { rec => 'record_type', com => 'typ', value => 'clu',
        doc => "The type of this object: 'clu' for a collection cluster" },
    { rec => 'n_colls', com => 'nco', doc => "The number of collections in cluster" },
    { rec => 'n_occs', com => 'noc', doc => "The number of occurrences in this cluster" },
    { rec => 'lng', com => 'lng', doc => "The longitude of the centroid of this cluster" },
    { rec => 'lat', com => 'lat', doc => "The latitude of the centroid of this cluster" },
    { rec => 'container_no', com => 'cxi',
        doc => "The identifier of the most specific standard interval covering the entire time range associated with this collection" },
   ];

$SELECT{bin} = "c.bin_id, c.clust_id";

$OUTPUT{bin} = 
   [
    { rec => 'clust_id', com => 'lv1', doc => "The identifier of the level-1 cluster in which this collection is located" },
    { rec => 'bin_id', com => 'lv2', doc => "The identifier of the level-2 cluster in which this collection is located" },
   ];

$SELECT{ref} = "r.author1init as r_ai1, r.author1last as r_al1, r.author2init as r_ai2, r.author2last as r_al2, r.otherauthors as r_oa, r.pubyr as r_pubyr, r.reftitle as r_reftitle, r.pubtitle as r_pubtitle, r.editors as r_editors, r.pubvol as r_pubvol, r.pubno as r_pubno, r.firstpage as r_fp, r.lastpage as r_lp";

$TABLES{ref} = 'r';

$PROC{ref} = 
   [
    { rec => 'r_al1', add => 'ref_list', use_main => 1, code => \&DataQuery::generateReference },
    { rec => 'sec_refs', add => 'ref_list', use_each => 1, code => \&DataQuery::generateReference },
   ];

$OUTPUT{ref} =
   [
    #{ rec => 'r_pubyr', com => 'pby',
    #	doc => "The year of publication of the primary reference associated with this collection" },
    { rec => 'ref_list', pbdb => 'references', dwc => 'associatedReferences', com => 'ref', xml_list => '; ',
	doc => "The reference(s) associated with this collection (as formatted text)" },
   ];

$SELECT{attr} = "r.author1init as a_ai1, r.author1last as a_al1, r.author2init as a_ai2, r.author2last as a_al2, r.otherauthors as a_oa, r.pubyr as a_pubyr";

$TABLES{attr} = 'r';

$OUTPUT{attr} = 
   [
    { rec => 'r_pubyr', com => 'pby',
	doc => "The year of publication of the primary reference associated with this collection" },
    { rec => 'ref_list', pbdb => 'references', dwc => 'associatedReferences', com => 'ref', 
        json_list => 1, xml_list => '; ',
	doc => "The reference(s) associated with this collection (pubyr and authors only)" },
   ];

$SELECT{summary_time} = "ei.base_age as early_age, li.top_age as late_age, icm.container_no";

$SELECT{time} = "ei.interval_name as early_int, ei.base_age as early_age, li.interval_name as late_int, li.top_age as late_age, group_concat(distinct ci.interval_no) as interval_list";

$TABLES{time} = ['ei', 'li', 'ci'];

$OUTPUT{time} =
   [
    { rec => 'early_age', com => 'eag',
	doc => "The early bound of the geologic time range associated with this collection (in Ma)" },
    { rec => 'late_age', com => 'lag',
	doc => "The late bound of the geologic time range associated with this collection (in Ma)" },
    { rec => 'interval_list', com => 'lti', json_list_literal => 1,
        doc => "A minimal list of standard intervals covering the time range associated with this collection" },
   ];

$SELECT{pers} = "authorizer_no, ppa.name as authorizer, enterer_no, ppe.name as enterer";

$TABLES{pers} = ['ppa', 'ppe'];

$SELECT{loc} = "cc.country, cc.state, cc.county";

$OUTPUT{loc} = 
   [
    { rec => 'country', com => 'cc2',
	doc => "The country in which this collection is located (ISO-3166-1 alpha-2)" },
    { rec => 'state', com => 'sta',
	doc => "The state or province in which this collection is located [not available for all collections]" },
    { rec => 'county', com => 'cny',
	doc => "The county in which this collection is located [not available for all collections]" },
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

$SELECT{ext} = "s.lng_min, lng_max, s.lat_min, s.lat_max, s.std_dev";

$OUTPUT{ext} =
   [
    { rec => 'lng_min', com => 'lg1', doc => "The mimimum longitude for collections in this bin or cluster" },
    { rec => 'lng_max', com => 'lg2', doc => "The maximum longitude for collections in this bin or cluster" },
    { rec => 'lat_min', com => 'la1', doc => "The mimimum latitude for collections in this bin or cluster" },
    { rec => 'lat_max', com => 'la2', doc => "The maximum latitude for collections in this bin or cluster" },
    { rec => 'std_dev', com => 'std', doc => "The standard deviation of the coordinates in this cluster" },
   ];

$OUTPUT{det} = 
   [
    { rec => 'early_int', com => 'int',
	doc => "The specific geologic time range associated with this collection (not necessarily a standard interval), or the interval that begins the range if {late_int} is also given" },
    { rec => 'late_int', com => 'lin', dedup => 'early_int',
	doc => "The interval that ends the specific geologic time range associated with this collection" },
   ];

our (%DOC_ORDER);

$DOC_ORDER{'single'} = ['single', 'ref', 'time', 'loc', 'rem'];
$DOC_ORDER{'list'} = ();
$DOC_ORDER{'summary'} = ();


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
    
    my $fields = join(', ', @{$self->{select_list}});
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('c', $self->{select_tables});
    
    # Generate the main query.
    
    $self->{main_sql} = "
	SELECT $fields
	FROM coll_matrix as c JOIN collections as cc using (collection_no)
		LEFT JOIN secondary_refs as sr using (collection_no)
		$join_list
        WHERE c.collection_no = $id and c.access_level = 0
	GROUP BY c.collection_no";
    
    print $self->{main_sql} . "\n\n" if $PBDB_Data::DEBUG;
    
    $self->{main_record} = $dbh->selectrow_hashref($self->{main_sql});
    
    # Abort if we couldn't retrieve the record.
    
    return unless $self->{main_record};
    
    # If we were directed to show references, grab any secondary references.
    
    if ( $self->{show}{ref} or $self->{show}{sref} )
    {
	my $extra_fields = $SELECT{ref};
	
        $self->{aux_sql}[0] = "
        SELECT sr.reference_no, $extra_fields
        FROM secondary_refs as sr JOIN refs as r using (reference_no)
        WHERE sr.collection_no = $id
	ORDER BY sr.reference_no";
        
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
    
    my $calc = '';
    my $mt = $self->{op} eq 'summary' ? 's' : 'c';
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my $filter_tables = {};
    
    my @filters = $self->generateQueryFilters($mt, $filter_tables);
    
    push @filters, "$mt.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = join(', ', @{$self->{select_list}});
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->generateLimitClause();
    
    # If we were asked to count rows, modify the query accordingly
    
    if ( $self->{params}{count} )
    {
	$calc = 'SQL_CALC_FOUND_ROWS';
    }
    
    # If the operation is 'summary', generate a query on the summary tables.
    
    if ( $self->{op} eq 'summary' ) 
    {
	my ($base_fields, $inner_query_fields, $summary_table, $group_field, $base_joins, $inner_query_joins);
	
	if ( $self->{params}{level} == 2 )
	{
	    $base_fields = $SUMMARY_2;
	    $summary_table = 'coll_bins';
	    $group_field = 'bin_id';
	}
	
	else
	{
	    $base_fields = $SUMMARY_1;
	    $summary_table = 'clusters';
	    $group_field = 'clust_id';
	}
	
	$base_fields .= ', ' . $fields if $fields;
	
	$base_joins = $self->generateJoinList('s', $self->{select_tables});
	
	$inner_query_fields = $filter_tables->{m} ? $SUMMARY_M : 
			      $filter_tables->{c} ? $SUMMARY_C :
						    $SUMMARY_S;
	
	$inner_query_fields .= ", s.bin_id" if $self->{params}{level} == 2;
	
	$inner_query_joins = $self->generateJoinList('s', $filter_tables, $group_field);
	
	$self->{main_sql} = "
	SELECT $calc $base_fields
	FROM (SELECT $inner_query_fields
	      FROM $summary_table as s $inner_query_joins
	      WHERE $filter_string
	      GROUP BY s.$group_field
	      ORDER BY s.$group_field
	      $limit) as s
		$base_joins";
    }
    
    # If the operation is 'list', generate a query on the collection matrix
    
    else
    {
	my ($base_joins);
	
	$base_joins = $self->generateJoinList($mt, $self->{select_tables});
	
	$self->{main_sql} = "
	SELECT $calc $fields
	FROM coll_matrix as c join collections as cc using (collection_no)
		LEFT JOIN secondary_refs as sr using (collection_no)
		$base_joins
        WHERE $filter_string
	GROUP BY c.collection_no
	ORDER BY c.collection_no
	$limit";
    }
    
    print $self->{main_sql} . "\n\n" if $PBDB_Data::DEBUG;
    
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


# generateQueryFilters ( tables )
# 
# Generate a list of filter clauses that will be used to generate the
# appropriate result set.

sub generateQueryFilters {

    my ($self, $mt, $tables_ref) = @_;
    
    my $dbh = $self->{dbh};
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
	    push @filters, "$mt.bin_id in ($list)";
	}
	
	elsif ( $first_bin >= 1000000 )
	{
	    push @filters, "$mt.clust_id in ($list)";
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
	(@taxa) = $self->{taxonomy}->getTaxa('self', $taxon_no, { fields => 'lft' });
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
    
    # Check for parameters 'person_no', 'person_name'
    
    elsif ( $self->{params}{person_no} )
    {
	if ( ref $self->{params}{person_no} eq 'ARRAY' )
	{
	    my $person_string = join(q{,}, @{$self->{params}{person_no}} );
	    push @filters, "(c.authorizer_no in ($person_string) or c.enterer_no in ($person_string))";
	    $tables_ref->{c} = 1;
	}
	
	else
	{
	    my $person_string = $self->{params}{person_no};
	    push @filters, "(c.authorizer_no in ($person_string) or c.enterer_no in ($person_string))";
	    $tables_ref->{c} = 1;
	}
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
	$tables_ref->{c} = 1;
	$min_age -= 0.001;
	push @filters, "$min_filt > $min_age";
    }
    
    if ( defined $max_age and $max_age > 0 )
    {
	my $max_filt = $self->{params}{time_strict} ? "c.early_age" : "c.late_age";
	$tables_ref->{c} = 1;
	$max_age += 0.001;
	push @filters, "$max_filt < $max_age";
    }
    
    # Return the list
    
    return @filters;
}


# generateJoinList ( tables )
# 
# Generate the actual join string indicated by the table hash.

sub generateJoinList {

    my ($self, $mt, $tables, $summary_join_field) = @_;
    
    my $join_list = '';
    
    # Return an empty string unless we actually have some joins to make
    
    return $join_list unless ref $tables eq 'HASH' and %$tables;
    
    # Some tables imply others.
    
    $tables->{o} = 1 if $tables->{t};
    $tables->{c} = 1 if ($tables->{o} or $tables->{ci}) and $self->{op} eq 'summary';
    
    # Create the necessary join expressions.
    
    $join_list .= "JOIN coll_matrix as c using ($summary_join_field)\n"
	if $tables->{c} and defined $summary_join_field;
    $join_list .= "JOIN occ_matrix as o using (collection_no)\n"
	if $tables->{o};
    $join_list .= "JOIN taxon_trees as t using (orig_no)\n"
	if $tables->{t};
    $join_list .= "LEFT JOIN refs as r on r.reference_no = $mt.reference_no\n" 
	if $tables->{r};
    $join_list .= "LEFT JOIN person as ppa on ppa.person_no = $mt.authorizer_no\n"
	if $tables->{ppa};
    $join_list .= "LEFT JOIN person as ppe on ppe.person_no = $mt.enterer_no\n"
	if $tables->{ppe};
    $join_list .= "LEFT JOIN coll_ints as ci on ci.collection_no = c.collection_no\n"
	if $tables->{ci};
    
    if ( $self->{op} eq 'summary' )
    {
	$join_list .= "JOIN interval_map as ei on ei.older_seq = $mt.early_seq\n"
	    if $tables->{ei};
	$join_list .= "JOIN interval_map as li on li.younger_seq = $mt.late_seq\n"
	    if $tables->{li};
    }
    
    else
    {
	$join_list .= "JOIN interval_map as ei on ei.interval_no = $mt.early_int_no\n"
	    if $tables->{ei};
	$join_list .= "JOIN interval_map as li on li.interval_no = $mt.late_int_no\n"
	    if $tables->{li};
    }
    
    if ( $tables->{icm} )
    {
	$join_list .= "LEFT JOIN interval_container_map as icm 
			on icm.early_seq = $mt.early_seq and icm.late_seq = $mt.late_seq\n"
    }
    
    return $join_list;
}


# generateBasisCode ( record )
# 
# Generate a geographic basis code for the specified record.

our %BASIS_CODE = 
    ('stated in text' => 'T',
     'based on nearby landmark' => 'L',
     'based on political unit' => 'P',
     'estimated from map' => 'M',
     'unpublished field data' => 'U',
     '' => '_');

our %PREC_CODE = 
    ('degrees' => 'D',
     'minutes' => 'M',
     'seconds' => 'S',
     '1' => '1', '2' => '2', '3' => '3', '4' => '4',
     '5' => '5', '6' => '6', '7' => '7', '8' => '8',
     '' => '_');

sub generateBasisCode {

    my ($self, $record) = @_;
    
    return $BASIS_CODE{$record->{llb}||''} . $PREC_CODE{$record->{llp}||''};
}

1;
