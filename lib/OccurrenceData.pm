# CollectionData
# 
# A class that returns information from the PaleoDB database about a single
# collection or a category of collections.  This is a subclass of PBDataQuery.
# 
# Author: Michael McClennen

package OccurrenceData;

use strict;

use parent 'Web::DataService::Request';

use Web::DataService qw(:validators);

use CommonData;
use OccurrenceTables qw($OCC_MATRIX);
use CollectionTables qw($COLL_MATRIX $COLL_BINS @BIN_LEVEL);
use CollectionData;
use IntervalTables qw($INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP);
use TaxonDefs qw(@TREE_TABLE_LIST %TAXON_RANK %RANK_STRING);
use Taxonomy;

use Carp qw(carp croak);
use POSIX qw(floor ceil);


our (%SELECT, %TABLES, %PROC, %OUTPUT);

$SELECT{basic} = "o.occurrence_no, o.collection_no, o.taxon_no as actual_no, a.taxon_name as actual_name, ts.spelling_no as taxon_no, ts.name as taxon_name, ts.rank as taxon_rank, o.early_age, o.late_age, o.reference_no";

$TABLES{basic} = ['t', 'ts'];

$OUTPUT{basic} =
   [
    { rec => 'occurrence_no', dwc => 'occurrenceID', com => 'oid',
	doc => "A positive integer that uniquely identifies the occurrence"},
    { rec => 'record_type', com => 'typ', com_value => 'occ', dwc_value => 'Occurrence', value => 'occurrence',
        doc => "The type of this object: 'occ' for an occurrence" },
    { rec => 'collection_no', com => 'cid', dwc => 'CollectionId',
        doc => "The identifier of the collection with which this occurrence is associated." },
    { rec => 'taxon_name', com => 'tna', dwc => 'associatedTaxa',
        doc => "The taxonomic name with which this occurrence has been identified, with synonymy taken into account." },
    { rec => 'taxon_rank', dwc => 'taxonRank', com => 'rnk', pbdb_code => \%RANK_STRING,
	doc => "The taxonomic rank of this name" },
    { rec => 'taxon_no', com => 'tid', pbdb => 'taxon_no',
	doc => "The identifier corresponding to the taxonomic name." },
    { rec => 'actual_name', com => 'atn', dedup => 'taxon_name',
        doc => "The actual taxonomic name with which this occurrence has been identified, regardless of synonymy" },
    { rec => 'actual_no', com => 'ati', pbdb => 'actual_taxon_no', dedup => 'taxon_no',
        doc => "The identifier corresponding to the actual taxonomic name" },
    { rec => 'early_age', com => 'eag',
	doc => "The early bound of the geologic time range associated with this occurrence (in Ma)" },
    { rec => 'late_age', com => 'lag',
	doc => "The late bound of the geologic time range associated with this occurrence (in Ma)" },
    # { rec => 'attribution', dwc => 'recordedBy', com => 'att', show => 'attr',
    # 	doc => "The attribution (author and year) of this occurrence" },
    # { rec => 'pubyr', com => 'pby', show => 'attr',
    # 	doc => "The year in which this collection was published" },
    { rec => 'reference_no', com => 'rid', json_list => 1,
        doc => "The identifier(s) of the references from which this data was entered" },
   ];

$SELECT{attr} = "r.author1init as a_ai1, r.author1last as a_al1, r.author2init as a_ai2, r.author2last as a_al2, r.otherauthors as a_oa, r.pubyr as a_pubyr";

$TABLES{attr} = 'r';

$PROC{attr} = [
    { rec => 'a_al1', set => 'attribution', use_main => 1, code => \&PBDBData::generateAttribution },
    { rec => 'a_pubyr', set => 'pubyr' },
   ];

$SELECT{ref} = "r.author1init as r_ai1, r.author1last as r_al1, r.author2init as r_ai2, r.author2last as r_al2, r.otherauthors as r_oa, r.pubyr as r_pubyr, r.reftitle as r_reftitle, r.pubtitle as r_pubtitle, r.editors as r_editors, r.pubvol as r_pubvol, r.pubno as r_pubno, r.firstpage as r_fp, r.lastpage as r_lp";

$TABLES{ref} = 'r';

$PROC{ref} = 
   [
    { rec => 'r_al1', add => 'ref_list', use_main => 1, code => \&PBDBData::generateReference },
    { rec => 'sec_refs', add => 'ref_list', use_each => 1, code => \&PBDBData::generateReference },
   ];

$OUTPUT{ref} =
   [
    { rec => 'ref_list', pbdb => 'references', dwc => 'associatedReferences', com => 'ref', xml_list => "\n\n",
	doc => "The reference(s) associated with this collection (as formatted text)" },
   ];

$SELECT{geo} = "c.lng, c.lat";

$OUTPUT{geo} = 
   [
    { rec => 'lng', dwc => 'decimalLongitude', com => 'lng',
	doc => "The longitude at which the occurrence is located (in degrees)" },
    { rec => 'lat', dwc => 'decimalLatitude', com => 'lat',
	doc => "The latitude at which the occurrence is located (in degrees)" },
   ];

$TABLES{coll} = ['cc', 'ei', 'li'];

$SELECT{coll} = "cc.collection_name, cc.collection_subset, cc.formation, cc.latlng_basis as llb, cc.latlng_precision as llp, ei.interval_name as early_int, li.interval_name as late_int";

$OUTPUT{coll} = 
   [
    { rec => 'llp', com => 'prc', use_main => 1, code => \&CollectionData::generateBasisCode,
        doc => "A two-letter code indicating the basis and precision of the geographic coordinates." },
    { rec => 'collection_name', dwc => 'collectionCode', com => 'nam',
	doc => "An arbitrary name which identifies the collection, not necessarily unique" },
    { rec => 'collection_subset', com => 'nm2',
	doc => "If this collection is a part of another one, this field specifies which part" },
    { rec => 'early_int', com => 'oei', pbdb => 'early_interval',
	doc => "The specific geologic time range associated with this collection (not necessarily a standard interval), or the interval that begins the range if C<end_interval> is also given" },
    { rec => 'late_int', com => 'oli', pbdb => 'late_interval', dedup => 'early_int',
	doc => "The interval that ends the specific geologic time range associated with this collection" },
   ];

$TABLES{loc} = 'cc';

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

$SELECT{time} = "im.cx_int_no, im.early_int_no, im.late_int_no";

$TABLES{time} = ['im'];

$OUTPUT{time} =
   [
    { rec => 'cx_int_no', com => 'cxi',
        doc => "The identifier of the most specific single interval from the selected timescale that covers the entire time range associated with this occurrence." },
    { rec => 'early_int_no', com => 'ein',
	doc => "The beginning of a range of intervals from the selected timescale that most closely brackets the time range associated with this occurrence (with C<late_int_no>)" },
    { rec => 'late_int_no', com => 'lin',
	doc => "The end of a range of intervals from the selected timescale that most closely brackets the time range associated with this occurrence (with C<early_int_no>)" },
   ];

$SELECT{ent} = "o.authorizer_no, ppa.name as authorizer, o.enterer_no, ppe.name as enterer, o.modifier_no, ppm.name as modifier";

$TABLES{ent} = ['ppa', 'ppe', 'ppm'];

$OUTPUT{ent} = 
   [
    { rec => 'authorizer_no', com => 'ath', 
      doc => 'The identifier of the database contributor who authorized the entry of this record.' },
    { rec => 'authorizer', vocab => 'pbdb', 
      doc => 'The name of the database contributor who authorized the entry of this record.' },
    { rec => 'enterer_no', com => 'ent', dedup => 'authorizer_no',
      doc => 'The identifier of the database contributor who entered this record.' },
    { rec => 'enterer', vocab => 'pbdb', 
      doc => 'The name of the database contributor who entered this record.' },
    { rec => 'modifier_no', com => 'mfr', dedup => 'authorizer_no',
      doc => 'The identifier of the database contributor who last modified this record.' },
    { rec => 'modifier', vocab => 'pbdb', 
      doc => 'The name of the database contributor who last modified this record.' },
   ];

$SELECT{crmod} = "o.created, o.modified";

$OUTPUT{crmod} = 
   [
    { rec => 'created', com => 'dcr',
      doc => "The date and time at which this record was created." },
    { rec => 'modified', com => 'dmd',
      doc => "The date and time at which this record was last modified." },
   ];

$OUTPUT{rem} = 
   [
    { rec => 'collection_aka', dwc => 'collectionRemarks', com => 'crm', xml_list => '; ',
	doc => "Any additional remarks that were entered about the colection"},
   ];


# configure ( )
# 
# This routine is called by the DataService module, and is passed the
# configuration data as a hash ref.

sub configure {
    
    my ($self, $ds, $dbh, $config) = @_;
    
$ds->define_ruleset('1.1:occ_specifier' =>
    [param => 'id', POS_VALUE, { alias => 'occ_id' }],
    "The identifier of the occurrence you wish to retrieve");

$ds->define_ruleset('1.1:occ_selector' =>
    [param => 'id', POS_VALUE, { list => ',', alias => 'occ_id' }],
    "Return occurrences identified by the specified identifier(s).  The value of this parameter may be a comma-separated list.",
    [param => 'coll_id', POS_VALUE, { list => ',' }],
    "Return occurences associated with the specified collections.  The value of this parameter may be a single collection",
    "identifier or a comma-separated list.");

$ds->define_ruleset('1.1:occ_display' =>
    "The following parameter indicates which information should be returned about each resulting occurrence:",
    [param => 'show', ENUM_VALUE('attr','ref','ent','geo','loc','coll','time','rem','crmod'), { list => ',' }],
    "The value of this parameter should be a comma-separated list of section names drawn",
    "From the list given below.  It defaults to C<basic>.",
    [ignore => 'level']);

$ds->define_ruleset('1.1/occs/single' =>
    [require => '1.1:occ_specifier', { error => "you must specify an occurrence identifier, either in the URL or with the 'id' parameter" }],
    [allow => '1.1:occ_display'],
    [allow => '1.1:common_params'],
    "!>You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");

$ds->define_ruleset('1.1/occs/list' => 
    [require_one => '1.1:occ_selector', '1.1:main_selector'],
    [allow => '1.1:occ_display'],
    [allow => '1.1:common_params'],
    "!>You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");


}


# get ( )
# 
# Query for all relevant information about the collection specified by the
# 'id' parameter.  Returns true if the query succeeded, false otherwise.

sub get {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # Make sure we have a valid id number.
    
    my $id = $self->{params}{id} || '';
    
    die "Bad identifier '$id'" unless defined $self->{params}{id} and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->generate_query_fields('o');
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('o', $self->{select_tables});
    
    # Generate the main query.
    
    $self->{main_sql} = "
	SELECT $fields
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c using (collection_no)
		JOIN authorities as a using (taxon_no)
		$join_list
        WHERE o.occurrence_no = $id and c.access_level = 0
	GROUP BY o.occurrence_no";
    
    print $self->{main_sql} . "\n\n" if $PBDB_Data::DEBUG;
    
    $self->{main_record} = $dbh->selectrow_hashref($self->{main_sql});
    
    # Abort if we couldn't retrieve the record.
    
    return unless $self->{main_record};
        
    return 1;
}


# list ( )
# 
# Query the database for basic info about all occurrences satisfying the
# conditions specified by the query parameters.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub list {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = CollectionData::generateMainFilters($self, 'list', 'c', $self->{select_tables});
    push @filters, $self->generateOccFilters($self->{select_tables});
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->generateLimitClause();
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $self->{params}{count} ? 'SQL_CALC_FOUND_ROWS' : '';
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->generate_query_fields('o');
    
    my $join_list = $self->generateJoinList('c', $self->{select_tables});
    
    $self->{main_sql} = "
	SELECT $calc $fields
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c using (collection_no)
		JOIN authorities as a using (taxon_no)
		$join_list
        WHERE $filter_string
	GROUP BY o.occurrence_no
	ORDER BY o.occurrence_no
	$limit";
    
    print $self->{main_sql} . "\n\n" if $PBDB_Data::DEBUG;
    
    # Then prepare and execute the main query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    if ( $calc )
    {
	($self->{result_count}) = $dbh->selectrow_array("SELECT FOUND_ROWS()");
    }
    
    return 1;
}


# generateOccFilters ( tables_ref )
# 
# Generate a list of filter clauses that will be used to compute the
# appropriate result set.  This routine handles only parameters that are specific
# to occurrences.
# 
# Any additional tables that are needed will be added to the hash specified by
# $tables_ref.

sub generateOccFilters {

    my ($self, $tables_ref) = @_;
    
    my $dbh = $self->{dbh};
    my @filters;
    
    # Check for parameter 'id'
    
    if ( ref $self->{params}{id} eq 'ARRAY' and
	 @{$self->{params}{id}} )
    {
	my $id_list = join(',', @{$self->{params}{id}});
	push @filters, "o.occurrence_no in ($id_list)";
    }
    
    elsif ( $self->{params}{id} )
    {
	push @filters, "o.occurrence_no = $self->{params}{id}";
    }
    
    # Check for parameter 'coll_id'
    
    if ( ref $self->{params}{coll_id} eq 'ARRAY' and
	 @{$self->{params}{coll_id}} )
    {
	my $id_list = join(',', @{$self->{params}{coll_id}});
	push @filters, "o.collection_no in ($id_list)";
    }
    
    elsif ( $self->{params}{coll_id} )
    {
	push @filters, "o.collection_no = $self->{params}{coll_id}";
    }
    
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
    
    # Create the necessary join expressions.
    
    $join_list .= "LEFT JOIN collections as cc on c.collection_no = cc.collection_no\n"
	if $tables->{cc};
    $join_list .= "LEFT JOIN taxon_trees as t on t.orig_no = o.orig_no\n"
	if $tables->{t};
    $join_list .= "LEFT JOIN taxon_trees as ts on ts.orig_no = t.synonym_no\n"
	if $tables->{ts};
    $join_list .= "LEFT JOIN refs as r on r.reference_no = c.reference_no\n" 
	if $tables->{r};
    $join_list .= "LEFT JOIN person as ppa on ppa.person_no = c.authorizer_no\n"
	if $tables->{ppa};
    $join_list .= "LEFT JOIN person as ppe on ppe.person_no = c.enterer_no\n"
	if $tables->{ppe};
    $join_list .= "LEFT JOIN person as ppm on ppm.person_no = c.modifier_no\n"
	if $tables->{ppm};
    $join_list .= "LEFT JOIN $INTERVAL_MAP as im on im.early_age = $mt.early_age and im.late_age = $mt.late_age and scale_no = 1\n"
	if $tables->{im};
    
    $join_list .= "LEFT JOIN $INTERVAL_DATA as ei on ei.interval_no = o.early_int_no\n"
	if $tables->{ei};
    $join_list .= "LEFT JOIN $INTERVAL_DATA as li on li.interval_no = o.late_int_no\n"
	if $tables->{li};
    
    return $join_list;
}


1;
