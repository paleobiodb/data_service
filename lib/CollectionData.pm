# CollectionData
# 
# A class that returns information from the PaleoDB database about a single
# collection or a category of collections.  This is a subclass of PBDataQuery.
# 
# Author: Michael McClennen

package CollectionData;

use strict;

use parent 'Web::DataService::Request';

use Web::DataService qw( :validators );

use CommonData qw(generateReference generateAttribution);
use CollectionTables qw($COLL_MATRIX $COLL_BINS @BIN_LEVEL);
use IntervalTables qw($INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP $INTERVAL_BUFFER);
use Taxonomy;

use Carp qw(carp croak);
use POSIX qw(floor ceil);


our ($MAX_BIN_LEVEL) = 0;


# initialize ( )
# 
# This routine is called once by Web::DataService in order to initialize this
# class.

sub initialize {
    
    my ($class, $ds, $config, $dbh) = @_;
    
    # First read the configuration information that describes how the
    # collections are organized into summary clusters (bins).
    
    if ( ref $config->{bins} eq 'ARRAY' )
    {
	my $bin_string = '';
	my $bin_level = 0;
	
	foreach (@{$config->{bins}})
	{
	    $bin_level++;
	    $bin_string .= ", " if $bin_string;
	    $bin_string .= "bin_id_$bin_level";
	}
	
	$MAX_BIN_LEVEL = $bin_level;
    }
    
    # Define an output map listing the blocks of information that can be
    # returned by the operations in this class.
    
    $ds->define_set('1.1:colls:basic_map' =>
        { value => 'basic', maps_to => '1.1:colls:basic', fixed => 1 },
	{ value => 'bin', maps_to => '1.1:colls:bin' },
	    "The list of geographic clusters to which the collection belongs.",
        { value => 'attr', maps_to => '1.1:colls:attr' },
	    "The attribution of the collection: the author name(s) from",
	    "the primary reference, and the year of publication.",
        { value => 'ref', maps_to => '1.1:colls:ref' },
	    "The primary reference for the collection, as formatted text.",
        { value => 'loc', maps_to => '1.1:colls:loc' },
	    "Additional information about the geographic locality of the collection",
        { value => 'time', maps_to => '1.1:colls:time' },
	    "Additional information about the temporal locality of the",
	    "collection.",
        { value => 'crmod', maps_to => '1.1:colls:crmod' },
	    "The C<created> and C<modified> timestamps for the collection",
        { value => 'rem', maps_to => '1.1:colls:rem' },
	    "Any additional remarks that were entered about the collection.");
    
    # Then define the output blocks that it mentions.
    
    $ds->define_block( '1.1:colls:basic' =>
      { select => ['c.collection_no', 'cc.collection_name', 'cc.collection_subset', 'cc.formation',
		   'c.lat', 'c.lng', 'cc.latlng_basis as llb', 'cc.latlng_precision as llp',
		   'c.n_occs', 'ei.interval_name as early_interval', 'li.interval_name as late_interval',
		   'c.reference_no', 'group_concat(sr.reference_no) as sec_ref_nos'], 
	tables => ['ei', 'li', 'sr'] },
      { output => 'collection_no', dwc_name => 'collectionID', com_name => 'oid' },
	  "A positive integer that uniquely identifies the collection",
      { output => 'record_type', value => 'collection', com_name => 'typ', com_value => 'col', 
	dwc_value => 'Occurrence' },
	  "type of this object: 'col' for a collection",
      { output => 'formation', com_name => 'fmm', if_block => 'strat' },
	  "The formation in which this collection was found",
      { output => 'lng', dwc_name => 'decimalLongitude', com_name => 'lng' },
	  "The longitude at which the collection is located (in degrees)",
      { output => 'lat', dwc_name => 'decimalLatitude', com_name => 'lat' },
	  "The latitude at which the collection is located (in degrees)",
      { set => 'llp', from_record => 1, code => \&generateBasisCode },
      { output => 'llp', com_name => 'prc' },
	  "A two-letter code indicating the basis and precision of the geographic coordinates.",
      { output => 'collection_name', dwc_name => 'collectionCode', com_name => 'nam' },
	  "An arbitrary name which identifies the collection, not necessarily unique",
      { output => 'collection_subset', com_name => 'nm2' },
	  "If this collection is a part of another one, this field specifies which part",
      { output => 'attribution', dwc_name => 'recordedBy', com_name => 'att', if_block => 'attr' },
	  "The attribution (author and year) of this collection name",
      { output => 'pubyr', com_name => 'pby', if_block => 'attr' },
	  "The year in which this collection was published",
      { output => 'n_occs', com_name => 'noc' },
	  "The number of occurrences in this collection",
      { output => 'early_interval', com_name => 'oei', pbdb_name => 'early_interval' },
	  "The specific geologic time range associated with this collection (not necessarily a",
	  "standard interval), or the interval that begins the range if C<late_inervalt> is also given",
      { output => 'late_interval', com_name => 'oli', pbdb_name => 'late_interval', dedup => 'early_interval' },
	  "The interval that ends the specific geologic time range associated with this collection",
      { set => 'reference_no', append => 1, from => 'sec_ref_nos', split => ',' },
      { output => 'reference_no', com_name => 'rid', text_join => ', ' },
	  "The identifier(s) of the references from which this data was entered");
    
    #defined_fields $ds 'toprank' =>
    #	{ select, "sum(c.n_occs) as n_occs, count(*) as n_colls" };

    $ds->define_block( '1.1:colls:bin' =>
      { output => 'bin_id_1', com_name => 'lv1' },
	  "The identifier of the level-1 cluster in which this collection is located",
      { output => 'bin_id_2', com_name => 'lv2' },
	  "The identifier of the level-2 cluster in which this collection is located",
      { output => 'bin_id_3', com_name => 'lv3' },
	  "The identifier of the level-3 cluster in which this collection is located");
    
    $ds->define_block( '1.1:colls:attr' =>
      { select => ['r.author1init as a_ai1', 'r.author1last as a_al1', 'r.author2init as a_ai2', 
		   'r.author2last as a_al2', 'r.otherauthors as a_oa', 'r.pubyr as a_pubyr'],
        tables => ['r'] },
      { set => 'attribution', from_record => 1, code => \&generateAttribution },
      { set => 'pubyr', from => 'a_pubyr', });
    
    $ds->define_block( '1.1:colls:ref' =>
      { select => ['r.author1init as r_ai1', 'r.author1last as r_al1', 'r.author2init as r_ai2', 
		   'r.author2last as r_al2', 'r.otherauthors as r_oa', 'r.pubyr as r_pubyr', 
		   'r.reftitle as r_reftitle', 'r.pubtitle as r_pubtitle', 
		   'r.editors as r_editors', 'r.pubvol as r_pubvol', 'r.pubno as r_pubno', 
		   'r.firstpage as r_fp', 'r.lastpage as r_lp', 'r.publication_type as r_pubtype', 
		   'r.language as r_language', 'r.doi as r_doi'],
	tables => ['r'] },
      { set => 'ref_list', from_record => 1, code => \&generateReference },
      #{ set => 'ref_list', append => 1, from_each => 'sec_refs', code => \&generateReference },
      #{ set => 'ref_list', join => "\n\n", if_format => 'txt,tsv,csv,xml' },
      { output => 'ref_list', pbdb_name => 'primary_reference', dwc_name => 'associatedReferences', com_name => 'ref' },
	  "The primary reference associated with this collection (as formatted text)");
    
    $ds->define_block( '1.1:colls:loc' =>
      { select => ['cc.country', 'cc.state', 'cc.county', 'cc.geogscale'] },
      { output => 'country', com_name => 'cc2' },
	  "The country in which this collection is located (ISO-3166-1 alpha-2)",
      { output => 'state', com_name => 'sta' },
	  "The state or province in which this collection is located [not available for all collections]",
      { output => 'county', com_name => 'cny' },
	  "The county in which this collection is located [not available for all collections]",
      { output => 'geogscale', com_name => 'gsc' },
	  "The geographic scale of this collection.");

    $ds->define_block( '1.1:colls:time' =>
      { select => ['$mt.early_age', '$mt.late_age', 'im.cx_int_no', 'im.early_int_no', 'im.late_int_no'],
	tables => ['im'] },
      { set => '*', code => \&fixTimeOutput },
      { output => 'early_age', com_name => 'eag' },
	  "The early bound of the geologic time range associated with this collection or cluster (in Ma)",
      { output => 'late_age', com_name => 'lag' },
	  "The late bound of the geologic time range associated with this collection or cluster (in Ma)",
      { output => 'cx_int_no', com_name => 'cxi' },
	  "The identifier of the most specific single interval from the selected timescale that",
	  "covers the entire time range associated with this collection or cluster.",
      { output => 'early_int_no', com_name => 'ein' },
	  "The beginning of a range of intervals from the selected timescale that most closely",
	  "brackets the time range associated with this collection or cluster (with C<late_int_no>)",
      { output => 'late_int_no', com_name => 'lin' },
	  "The end of a range of intervals from the selected timescale that most closely brackets",
	  "the time range associated with this collection or cluster (with C<early_int_no>)");

    $ds->define_block( '1.1:colls:ent' =>
      { select => ['$mt.authorizer_no', 'ppa.name as authorizer', '$mt.enterer_no', 
		   'ppe.name as enterer', '$mt.modifier_no', 'ppm.name as modifier'],
	tables => ['ppa', 'ppe', 'ppm'] },
      { output => 'authorizer_no', com_name => 'ath', if_format => 'json' },
	  "The identifier of the database contributor who authorized the entry of this record.",
      { output => 'authorizer', if_format => 'csv,tsv,txt' },
	  "The name of the database contributor who authorized the entry of this record.",
      { output => 'enterer_no', com_name => 'ent', dedup => 'authorizer_no', if_format => 'json' },
	  "The identifier of the database contributor who entered this record.",
      { output => 'enterer', if_format => 'csv,tsv,txt' },
	  "The name of the database contributor who entered this record.",
      { output => 'modifier_no', com_name => 'mfr', dedup => 'authorizer_no', if_format => 'json' },
	  "The identifier of the database contributor who last modified this record.",
      { output => 'modifier', if_format => 'csv,tsv,txt' },
	  "The name of the database contributor who last modified this record.");
    
    $ds->define_block( 'taxon_record' =>
      { output => 'taxon_name', com_name => 'tna' },
	  "The scientific name of the taxon",
      { output => 'taxon_rank', com_name => 'trn' },
	  "The taxonomic rank",
      { output => 'taxon_no', com_name => 'tid' },
	  "A positive integer that uniquely identifies the taxon",
      { output => 'ident_name', com_name => 'ina', dedup => 'taxon_name' },
	  "The name under which the occurrence was actually identified",
      { output => 'ident_rank', com_name => 'irn', dedup => 'taxon_rank' },
	  "The taxonomic rank as actually identified",
      { output => 'ident_no', com_name => 'iid', dedup => 'taxon_no' },
	  "A positive integer that uniquely identifies the name as identified");

    $ds->define_block( 'taxa' =>
      { output => 'taxa', com_name => 'tax', rule => 'taxon_record' },
	  "A list of records describing the taxa that have been identified",
	  "as appearing in this collection");
    
    $ds->define_block( '1.1:colls:rem' =>
      { set => 'collection_aka', join => '; ', if_format => 'txt,tsv,csv,xml' },
      { output => 'collection_aka', dwc_name => 'collectionRemarks', com_name => 'crm' },
	  "Any additional remarks that were entered about the colection");
    
    $ds->define_block( '1.1:colls:strat' =>
      { select => ['cc.member', 'cc.stratscale', 'cc.stratcomments'] },
      { output => 'formation', com_name => 'frm' },
	  "The formation in which the collection was found",
      { output => 'member', com_name => 'mmb' },
	  "The member in which the collection was found",
      { output => 'stratscale', com_name => 'ssc' },
	  "The stratigraphic scale of the collection",
      { output => 'stratcomments', com_name => 'scm' },
	  "Stratigraphic comments/notes about the collection, if any");

    $ds->define_block( '1.1:colls:stratext' =>
      { select => ['cc.zone', 'cc.geological_group', 'cc.localsection', 'cc.localbed', 
		   'cc.localorder', 'cc.regionalsection', 'cc.regionalbed', 'cc.regionalorder'] },
      { output => 'zone', com_name => 'zon' }, 
	  "The zone in which the collection was found",
      { output => 'geological_group', com_name => 'ggr' },
	  "The geological group in which the collection was found",
      { output => 'localsection', com_name => 'lsc' },
	  "The local section in which the collection was found");

    $ds->define_block( '1.1:colls:refbase' =>
      { select => ['r.reference_no', '[ref]', 'r.comments as r_comments'] },
      { set => 'formatted', code => \&generateReference },
      { output => 'reference_no', com_name => 'rid' }, 
	  "Numeric identifier for this document reference in the database",
      { output => 'record_type', com_name => 'typ', com_value => 'ref', value => 'reference' },
	  "The type of this object: 'ref' for a document reference",
      { output => 'formatted', com_name => 'ref' },
	  "Formatted reference",
      { output => 'r_ai1', com_name => 'ai1', pbdb_name => 'author1init', not_block => 'formonly' },
	  "First initial of the first author",
      { output => 'r_al1', com_name => 'al1', pbdb_name => 'author1last', not_block => 'formonly' },
	  "Last name of the second author",
      { output => 'r_ai2', com_name => 'ai2', pbdb_name => 'author2init', not_block => 'formonly' },
	  "First initial of the second author",
      { output => 'r_al2', com_name => 'al2', pbdb_name => 'author2last', not_block => 'formonly' },
	  "Last name of the second author",
      { output => 'r_oa', com_name => 'oau', pbdb_name => 'otherauthors', not_block => 'formonly' },
	  "The names of the remaining authors",
      { output => 'r_pubyr', com_name => 'pby', pbdb_name => 'pubyr', not_block => 'formonly' },
	  "The year in which the document was published",
      { output => 'r_reftitle', com_name => 'tit', pbdb_name => 'reftitle', not_block => 'formonly' },
	  "The title of the document",
      { output => 'r_pubtitle', com_name => 'pbt', pbdb_name => 'pubtitle', not_block => 'formonly' },
	  "The title of the publication in which the document appears",
      { output => 'r_editors', com_name => 'eds', pbdb_name => 'editors', not_block => 'formonly' },
	  "Names of the editors, if any",
      { output => 'r_pubvol', com_name => 'vol', pbdb_name => 'pubvol', not_block => 'formonly' },
	  "The volume number, if any",
      { output => 'r_pubno', com_name => 'num', pbdb_name => 'pubno', not_block => 'formonly' },
	  "The series number within the volume, if any",
      { output => 'r_fp', com_name => 'pgf', pbdb_name => 'firstpage', not_block => 'formonly' },
	  "First page number",
      { output => 'r_lp', com_name => 'pgl', pbdb_name => 'lastpage', not_block => 'formonly' },
	  "Last page number",
      { output => 'r_pubtype', com_name => 'pbt', pbdb_name => 'publication_type', not_block => 'formonly' },
	  "Publication type",
      { output => 'r_language', com_name => 'lng', pbdb_name => 'language', not_block => 'formonly' },
	  "Language",
      { output => 'r_doi', com_name => 'doi', pbdb_name => 'doi' },
	  "The DOI for this document, if known",
      { output => 'r_comments', com_name => 'cmt', pbdb_name => 'comments' },
	  "Additional comments about this reference, if any");

    $ds->define_block( 'comments' =>
      { output => 'r_comments', com_name => 'cmt', pbdb_name => 'comments' },
	  "Additional comments about this reference, if any");
    
    $ds->define_block( '1.1:colls:crmod' =>
      { select => ['cc.created', 'cc.modified'] },
      { output => 'created', com_name => 'dcr' },
	  "The date and time at which this record was created.",
      { output => 'modified', com_name => 'dmd' },
	  "The date and time at which this record was last modified.");
    
    # Finally, define rulesets to interpret the parmeters used with operations
    # defined by this class.
    
    $ds->define_ruleset('1.1:main_selector' =>
    [param => 'clust_id', POS_VALUE, { list => ',' }],
    "Return only records associated with the specified geographic clusters.",
    "You may specify one or more cluster ids, separated by commas.",
    [param => 'taxon_name', \&TaxonData::validNameSpec],
    "Return only records associated with the specified taxonomic name(s).  You may specify multiple names, separated by commas.",
    [param => 'taxon_id', POS_VALUE, { list => ','}],
    "Return only records associated with the specified taxonomic name(s), specified by numeric identifier.",
    "You may specify multiple identifiers, separated by commas.",
    [param => 'taxon_actual', FLAG_VALUE],
    "Return only records that were actually identified with the specified taxonomic name, not those which match due to synonymy",
    "or other correspondences between taxa",
    [param => 'base_name', \&TaxonData::validNameSpec, { list => ',' }],
    "Return only records associated with the specified taxonomic name(s), or I<any of their children>.",
    "You may specify multiple names, separated by commas.",
    [param => 'base_id', POS_VALUE, { list => ',' }],
    "Return only records associated with the specified taxonomic name(s), specified by numeric identifier, or I<any of their children>.",
    "You may specify multiple identifiers, separated by commas.",
    "Note that you may specify at most one of 'taxon_name', 'taxon_id', 'base_name', 'base_id'.",
    [at_most_one => 'taxon_name', 'taxon_id', 'base_name', 'base_id'],
    [param => 'exclude_id', POS_VALUE, { list => ','}],
    "Exclude any records whose associated taxonomic name is a child of the given name or names, specified by numeric identifier.",
    [param => 'person_id', POS_VALUE, { list => ','}],
    "Return only records whose entry was authorized by the given person or people, specified by numeric identifier.",
    [param => 'lngmin', DECI_VALUE],
    [param => 'lngmax', DECI_VALUE],
    [param => 'latmin', DECI_VALUE],
    [param => 'latmax', DECI_VALUE],
    "Return only records whose geographic location falls within the given bounding box.",
    "The longitude boundaries will be normalized to fall between -180 and 180, and will generate",
    "two adjacent bounding boxes if the range crosses the antimeridian.",
    "Note that if you specify C<lngmin> then you must also specify C<lngmax>.",
    [together => 'lngmin', 'lngmax',
	{ error => "you must specify both of 'lngmin' and 'lngmax' if you specify either of them" }],
    [param => 'loc', ANY_VALUE],		# This should be a geometry in WKT format
    "Return only records whose geographic location falls within the specified geometry, specified in WKT format.",
    [param => 'continent', ANY_VALUE],
    "Return only records whose geographic location falls within the specified continents.  The list of accepted",
    "continents can be retrieved via L</data1.1/config>.",
    [param => 'min_ma', DECI_VALUE(0)],
    "Return only records whose temporal locality is at least this old, specified in Ma.",
    [param => 'max_ma', DECI_VALUE(0)],
    "Return only records whose temporal locality is at most this old, specified in Ma.",
    [param => 'interval_id', POS_VALUE],
    "Return only records whose temporal locality falls within the given geologic time interval, specified by numeric identifier.",
    [param => 'interval', ANY_VALUE],
    "Return only records whose temporal locality falls within the named geologic time interval.",
    [at_most_one => 'interval_id', 'interval', 'min_ma'],
    [at_most_one => 'interval_id', 'interval', 'max_ma'],
    [optional => 'timerule', ENUM_VALUE('contain','overlap','buffer')],
    "Resolve temporal locality according to the specified rule:", "=over 4",
    "=item contain", "Return only records whose temporal locality is strictly contained in the specified time range.",
    "=item overlap", "Return only records whose temporal locality overlaps the specified time range.",
    "=item buffer", "Return only records whose temporal locality overlaps the specified range and is contained",
    "within the specified time range plus a buffer on either side.  If an interval from one of the timescales known to the database is",
    "given, then the default buffer will be the intervals immediately preceding and following at the same level.",
    "Otherwise, the buffer will default to 10 million years on either side.  This can be overridden using the parameters",
    "C<earlybuffer> and C<latebuffer>.  This is the default value for this option.",
    [optional => 'earlybuffer', POS_VALUE],
    "Override the default buffer period for the beginning of the time range when resolving temporal locality.",
    "The value is given in millions of years.  This option is only relevant if C<timerule> is C<buffer> (which is the default).",
    [optional => 'latebuffer', POS_VALUE],
    "Override the default buffer period for the end of the time range when resolving temporal locality.",
    "The value is given in millions of years.  This option is only relevant if C<timerule> is C<buffer> (which is the default).");

    $ds->define_ruleset('1.1:coll_specifier' =>
	"The following required parameter selects a record to retrieve:",
	{ param => 'id', valid => POS_VALUE, alias => 'coll_id' },
	    "The identifier of the collection you wish to retrieve");
    
    $ds->define_ruleset('1.1:coll_selector' =>
	{ param => 'id', valid => INT_VALUE, list => ',', alias => 'coll_id' },
	    "A comma-separated list of collection identifiers.");
    
    $ds->define_ruleset('1.1:coll_display' =>
	"You can use the following parameter to request additional information about each",
	"retrieved collection:",
	{ param => 'show', list => q{,},
	  valid => $ds->valid_set('1.1:colls:basic_map') },
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each collection.  Its value should be",
	    "one or more of the following, separated by commas:",
	    $ds->document_set('1.1:colls:basic_map'),
	{ ignore => 'level' });
    
    $ds->define_ruleset('1.1:colls:single' => 
    	{ require => '1.1:coll_specifier', 
	  error => "you must specify a collection identifier, either in the URL or with the 'id' parameter" },
    	{ allow => '1.1:coll_display' },
    	{ allow => '1.1:common_params' },
	    "^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");

    $ds->define_ruleset('1.1:colls:list' => 
	">You can use the following parameter if you wish to retrieve information about",
	"a known list of collections, or to filter a known list against other criteria such as location or time.",
	"Only the records which match the other parameters that you specify will be returned.",
    	{ allow => '1.1:coll_selector' },
        ">The following parameters can be used to query for collections by a variety of criteria.",
	"Except as noted below, you may use these in any combination.",
    	{ allow => '1.1:main_selector' },
    	{ allow => '1.1:coll_display' },
    	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");
    
    $ds->define_ruleset('1.1:colls:refs' =>
    	{ allow => '1.1:coll_selector' },
    ">The following parameters can be used to specify which records to return.",
    "Except as specified below, you can use these in any combination:",
    	{ allow => '1.1:main_selector' },
    	{ allow => '1.1:refs_display' },
    	{ allow => '1.1:common_params' },
	    "^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");
    
    $ds->define_ruleset('1.1:toprank_selector' =>
	{ param => 'show', valid => ENUM_VALUE('formation', 'ref', 'author'), list => ',' });
    
    $ds->define_ruleset('1.1:colls/toprank' => 
    	{ require => '1.1:main_selector' },
    	{ require => '1.1:toprank_selector' },
    	{ allow => '1.1:common_params' });
    
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
    
    my $id = $self->{params}{id};
    
    die "Bad identifier '$id'" unless defined $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = join(', ', $self->select_list({ mt => 'c' }));
    
    $self->adjustCoordinates(\$fields);
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('c', $self->tables_hash);
    
    # Generate the main query.
    
    $self->{main_sql} = "
	SELECT $fields
	FROM $COLL_MATRIX as c JOIN collections as cc using (collection_no)
		LEFT JOIN secondary_refs as sr using (collection_no)
		$join_list
        WHERE c.collection_no = $id and c.access_level = 0
	GROUP BY c.collection_no";
    
    print STDERR $self->{main_sql} . "\n\n" if $self->debug;
    
    $self->{main_record} = $dbh->selectrow_hashref($self->{main_sql});
    
    # Abort if we couldn't retrieve the record.
    
    return unless $self->{main_record};
    
    # If we were directed to show references, grab any secondary references.
    
    # if ( $self->{show}{ref} )
    # {
    # 	my $extra_fields = $request->select_list('ref');
	
    #     $self->{aux_sql}[0] = "
    #     SELECT sr.reference_no, $extra_fields
    #     FROM secondary_refs as sr JOIN refs as r using (reference_no)
    #     WHERE sr.collection_no = $id
    # 	ORDER BY sr.reference_no";
        
    #     $self->{main_record}{sec_refs} = $dbh->selectall_arrayref($self->{aux_sql}[0], { Slice => {} });
    # }
    
    # If we were directed to show associated taxa, grab them too.
    
    if ( $self->{show}{taxa} )
    {
	my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
	
	my $auth_table = $taxonomy->{auth_table};
	my $tree_table = $taxonomy->{tree_table};
	
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


# list ( )
# 
# Query the database for basic info about all collections satisfying the
# conditions specified by the query parameters.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub list {

    my ($self, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = $self->generateMainFilters('list', 'c', $self->tables_hash);
    push @filters, $self->generateCollFilters($self->tables_hash);
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $self->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->select_string({ mt => 'c' });
    
    $self->adjustCoordinates(\$fields);
    
    # If the operation is 'toprank', generate a query on the collection matrix
    # joined with whichever other tables are relevant

    if ( defined $arg && $arg eq 'toprank' )
    {
	my $base_joins = $self->generateJoinList('c', $self->tables_hash);
	
	my $group_field = $self->{show}{formation} ? 'formation' :
			  $self->{show}{author}    ? 'main_author' :
			  $self->{show}{ref}	   ? 'c.reference_no' : '';
	
	die "No group field specified" unless $group_field;
	
	$self->{main_sql} = "
	SELECT $calc $fields
	FROM coll_matrix as c join collections as cc using (collection_no)
		$base_joins
	WHERE $filter_string
	GROUP BY $group_field
	ORDER BY n_occs DESC $limit";
    }
    
    # If the operation is 'list', generate a query on the collection matrix
    
    else
    {
	my $base_joins = $self->generateJoinList('c', $self->tables_hash);
	
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
    
    print $self->{main_sql} . "\n\n" if $self->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $self->sql_count_rows;
    
    return 1;
}


# refs ( )
# 
# Query the database for info about the references for all collections
# satisfying the conditions specified by the query parameters.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub refs {
    
    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->{dbh};
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = $self->generateMainFilters('list', 'c', $self->{select_tables});
    push @filters, $self->generateCollFilters($self->{select_tables});
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->generateLimitClause();
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $self->{params}{count} ? 'SQL_CALC_FOUND_ROWS' : '';
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->generate_query_fields('r');
    
    $self->adjustCoordinates(\$fields);
    
    my $base_joins = $self->generateJoinList('c', $self->{select_tables});
    
    $self->{main_sql} = "
	SELECT $calc $fields
	FROM refs as r JOIN
	       (SELECT DISTINCT sr.reference_no
		FROM coll_matrix as c join collections as cc using (collection_no)
			LEFT JOIN secondary_refs as sr using (collection_no)
			$base_joins
		WHERE $filter_string) as cr using (reference_no)
	$limit";
    
    print $self->{main_sql} . "\n\n" if $self->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    if ( $calc )
    {
	($self->{result_count}) = $dbh->selectrow_array("SELECT FOUND_ROWS()");
    }

}

# fixTimeOutput ( record )
# 
# Adjust the time output by truncating unneeded digits and adding
# default containing-interval values.

sub fixTimeOutput {
    
    my ($self, $record) = @_;
    
    $record->{early_age} =~ s/\.?0+$// if defined $record->{early_age};
    $record->{late_age} =~ s/\.?0+$// if defined $record->{late_age};
}


# generateCollFilters ( tables_ref )
# 
# Generate a list of filter clauses that will be used to compute the
# appropriate result set.  This routine handles only parameters that are specific
# to collections.
# 
# Any additional tables that are needed will be added to the hash specified by
# $tables_ref.  The parameter $op is the operation being carried out, while
# $mt indicates the main table on which to join ('c' for coll_matrix, 's' for
# coll_bins, 'o' for occ_matrix).

sub generateCollFilters {

    my ($self, $tables_ref) = @_;
    
    my $dbh = $self->{dbh};
    my @filters;
    
    # Check for parameter 'id'
    
    if ( ref $self->{params}{id} eq 'ARRAY' and
	 @{$self->{params}{id}} )
    {
	my $id_list = join(',', @{$self->{params}{id}});
	push @filters, "c.collection_no in ($id_list)";
    }
    
    elsif ( $self->{params}{id} )
    {
	push @filters, "c.collection_no = $self->{params}{id}";
    }
    
    return @filters;
}


# generateMainFilters ( op, mt, tables_ref )
# 
# Generate a list of filter clauses that will be used to generate the
# appropriate result set.  This routine handles parameters that are part of
# the 'main_selector' ruleset, applicable to both collections and occurrences.
# 
# Any additional tables that are needed will be added to the hash specified by
# $tables_ref.  The parameter $op is the operation being carried out, while
# $mt indicates the main table on which to join ('c' for coll_matrix, 's' for
# coll_bins, 'o' for occ_matrix).

sub generateMainFilters {

    my ($self, $op, $mt, $tables_ref) = @_;
    
    my $dbh = $self->{dbh};
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    my @filters;
    my $non_geo_filter;
    
    # Check for parameter 'clust_id'
    
    if ( ref $self->{params}{clust_id} eq 'ARRAY' )
    {
	# If there aren't any bins, include a filter that will return no
	# results. 
	
	if ( $MAX_BIN_LEVEL == 0 )
	{
	    push @filters, "c.collection_no = 0";
	}
	
	elsif ( $op eq 'summary' )
	{
	    my @clusters = grep { $_ > 0 } @{$self->{params}{clust_id}};
	    my $list = join(q{,}, @clusters);
	    push @filters, "s.bin_id in ($list)";
	}
	
	else
	{
	    my %clusters;
	    my @clust_filters;
	    
	    foreach my $cl (@{$self->{params}{clust_id}})
	    {
		my $cl1 = substr($cl, 0, 1);
		push @{$clusters{$cl1}}, $cl if $cl1 =~ /[0-9]/;
	    }
	    
	    foreach my $k (keys %clusters)
	    {
		next unless @{$clusters{$k}};
		my $list = join(q{,}, @{$clusters{$k}});
		push @clust_filters, "c.bin_id_$k in ($list)";
	    }
	    
	    # If no valid filter was generated, then add one that will return
	    # 0 results.
	    
	    push @clust_filters, "c.collection_no = 0" unless @clust_filters;
	    push @filters, @clust_filters;
	}
    }
    
    # Check for parameters 'taxon_name', 'base_name', 'taxon_id', 'base_id',
    # 'exclude_name', 'exclude_id'
    
    my $taxon_name = $self->{params}{taxon_name} || $self->{params}{base_name};
    my $taxon_no = $self->{params}{taxon_id} || $self->{params}{base_id};
    my $exclude_no = $self->{params}{exclude_id};
    my (@taxa, @exclude_taxa);
    
    # First get the relevant taxon records for all included taxa
    
    if ( $taxon_name )
    {
	@taxa = $taxonomy->getTaxaByName($taxon_name, { fields => 'lft' });
    }
    
    elsif ( $taxon_no )
    {
	@taxa = $taxonomy->getTaxa('self', $taxon_no, { fields => 'lft' });
    }
    
    # Then get the records for excluded taxa.  But only if there are any
    # included taxa in the first place.
    
    if ( $exclude_no && $exclude_no ne 'undefined' )
    {
	@exclude_taxa = $taxonomy->getTaxa('self', $exclude_no, { fields => 'lft' });
    }
    
    # Then construct the necessary filters for included taxa
    
    if ( @taxa and ($self->{params}{base_name} or $self->{params}{base_id}) )
    {
	my $taxon_filters = join ' or ', map { "t.lft between $_->{lft} and $_->{rgt}" } @taxa;
	push @filters, "($taxon_filters)";
	$tables_ref->{t} = 1;
	$non_geo_filter = 1;
    }
    
    elsif ( @taxa )
    {
	my $taxon_list = join ',', map { $_->{orig_no} } @taxa;
	push @filters, "o.orig_no in ($taxon_list)";
	$tables_ref->{o} = 1;
	$non_geo_filter = 1;
    }
    
    # If no matching taxa were found, add a filter clause that will return no results.
    
    elsif ( $taxon_name || $taxon_no )
    {
	push @filters, "o.orig_no = -1";
    }
    
    # ...and for excluded taxa 
    
    if ( @exclude_taxa and @taxa )
    {
	push @filters, map { "t.lft not between $_->{lft} and $_->{rgt}" } @exclude_taxa;
	$tables_ref->{t} = 1;
    }
    
    # Check for parameters 'person_no', 'person_name'
    
    if ( $self->{params}{person_id} )
    {
	if ( ref $self->{params}{person_id} eq 'ARRAY' )
	{
	    my $person_string = join(q{,}, @{$self->{params}{person_id}} );
	    push @filters, "(c.authorizer_no in ($person_string) or c.enterer_no in ($person_string))";
	    $tables_ref->{c} = 1;
	    $non_geo_filter = 1;
	}
	
	else
	{
	    my $person_string = $self->{params}{person_id};
	    push @filters, "(c.authorizer_no in ($person_string) or c.enterer_no in ($person_string))";
	    $tables_ref->{c} = 1;
	    $non_geo_filter = 1;
	}
    }
    
    # Check for parameters 'lngmin', 'lngmax', 'latmin', 'latmax'
    
    if ( defined $self->{params}{lngmin} )
    {
	my $x1 = $self->{params}{lngmin};
	my $x2 = $self->{params}{lngmax};
	my $y1 = $self->{params}{latmin};
	my $y2 = $self->{params}{latmax};
	
	# If the longitude coordinates do not fall between -180 and 180, adjust
	# them so that they do.
	
	if ( $x1 < -180.0 )
	{
	    $x1 = $x1 + ( floor( (180.0 - $x1) / 360.0) * 360.0);
	}
	
	if ( $x2 < -180.0 )
	{
	    $x2 = $x2 + ( floor( (180.0 - $x2) / 360.0) * 360.0);
	}
	
	if ( $x1 > 180.0 )
	{
	    $x1 = $x1 - ( floor( ($x1 + 180.0) / 360.0 ) * 360.0);
	}
	
	if ( $x2 > 180.0 )
	{
	    $x2 = $x2 - ( floor( ($x2 + 180.0) / 360.0 ) * 360.0);
	}
	
	# If $x1 < $x2, then we query on a single bounding box defined by
	# those coordinates.
	
	if ( $x1 < $x2 )
	{
	    # if ( defined $self->{op} && $self->{op} eq 'summary' )
	    # {
	    # 	push @filters, "s.lng between $x1 and $x2 and s.lat between $y1 and $y2";
	    # }
	    
	    # else
	    # {
		my $polygon = "'POLYGON(($x1 $y1,$x2 $y1,$x2 $y2,$x1 $y2,$x1 $y1))'";
		push @filters, "contains(geomfromtext($polygon), $mt.loc)";
	    # }
	}
	
	# Otherwise, our bounding box crosses the antimeridian and so must be
	# split in two.  The latitude bounds must always be between -90 and
	# 90, regardless.
	
	else
	{
	    # if ( defined $self->{op} && $self->{op} eq 'summary' )
	    # {
	    # 	push @filters, "(s.lng between $x1 and 180.0 or s.lng between -180.0 and $x2) and s.lat between $y1 and $y2";
	    # }
	    
	    # else
	    # {
		my $polygon = "'MULTIPOLYGON((($x1 $y1,180.0 $y1,180.0 $y2,$x1 $y2,$x1 $y1)),((-180.0 $y1,$x2 $y1,$x2 $y2,-180.0 $y2,-180.0 $y1)))'";
		push @filters, "contains(geomfromtext($polygon), $mt.loc)";
	    #}
	}
    }
    
    if ( $self->{params}{loc} )
    {
	push @filters, "contains(geomfromtext($self->{params}{loc}), $mt.loc)";
    }
    
    # Check for parameters , 'interval_id', 'interval', 'min_ma', 'max_ma'.
    # If no time rule was given, it defaults to 'buffer'.
    
    my $time_rule = $self->{params}{timerule} || 'buffer';
    my $summary_interval = 0;
    my ($early_age, $late_age, $early_bound, $late_bound);
    
    # If an interval was specified, use that.
    
    if ( $self->{params}{interval_id} or $self->{params}{interval} )
    {
	my ($interval_no, $scale_no, $level);
	
	# First figure out the parameters of the specified interval
	
	if ( $self->{params}{interval_id} )
	{
	    $interval_no = $self->{params}{interval_id} + 0;
	    
	    my $sql = "
		SELECT early_age, late_age, scale_no, level, early_bound, late_bound
		FROM $INTERVAL_DATA JOIN $SCALE_MAP using (interval_no)
			JOIN $INTERVAL_BUFFER using (interval_no)
		WHERE interval_no = $interval_no ORDER BY scale_no LIMIT 1";
	    
	    ($early_age, $late_age, $scale_no, $level, $early_bound, $late_bound) = $dbh->selectrow_array($sql);
	}
	
	else
	{
	    my $quoted_name = $dbh->quote($self->{params}{interval});
	    
	    my $sql = "SELECT early_age, late_age, interval_no, scale_no, early_bound, late_bound
		   FROM $INTERVAL_DATA JOIN $SCALE_MAP using (interval_no)
			JOIN $INTERVAL_BUFFER using (interval_no)
		   WHERE interval_name like $quoted_name ORDER BY scale_no";
	
	    ($early_age, $late_age, $interval_no, $scale_no, $early_bound, $late_bound) = $dbh->selectrow_array($sql);
	}
	
	# If the requestor wants to override the time bounds, do that.
	
	if ( defined $self->{params}{earlybuffer} )
	{
	    $early_bound = $early_age + $self->{params}{earlybuffer};
	}
	
	if ( defined $self->{params}{latebuffer} )
	{
	    $late_bound = $late_age - $self->{params}{latebuffer};
	    $late_bound = 0 if $late_bound < 0;
	}
	
	# If we are querying for summary clusters, we can use the cluster
	# table row corresponding to that interval number.  Unless we are
	# using the 'overlap' rule, in which case we need the unrestricted
	# cluster table row (the one for interval_no = 0).
	
	if ( $op eq 'summary' and $time_rule ne 'overlap' )
	{
	    $summary_interval = $interval_no;
	}
    }
    
    # Otherwise, if a range of years was specified, use that.
    
    elsif ( defined $self->{params}{max_ma} and defined $self->{params}{min_ma} )
    {
	my $range = $self->{params}{max_ma} - $self->{params}{min_ma};
	my $buffer = $range * 0.5;
	
	$early_age = $self->{params}{max_ma} + 0;
	$early_bound = defined $self->{params}{earlybuffer} ? 
		$early_age + $self->{params}{earlybuffer} :
		    $early_age + $buffer;
	
	$late_age = $self->{params}{min_ma} + 0;
	$late_bound = defined $self->{params}{latebuffer} ?
	    $late_age - $self->{params}{latebuffer} :
		$late_age - $buffer;
	
	$late_bound = 0 if $late_bound < 0;
    }
    
    # Otherwise, handle either a min or max filter alone.
    
    else
    {
	if ( defined $self->{params}{max_ma} )
	{
	    $early_age = $self->{params}{max_ma} + 0;
	    $early_bound = $early_age;
	}
	
	if ( defined $self->{params}{min_ma} )
	{
	    $late_age = $self->{params}{min_ma} + 0;
	    $late_bound = $late_age;
	}
    }
    
    # Now, if we are summarizing then add the appropriate interval filter.
    
    if ( $op eq 'summary' )
    {
	push @filters, "s.interval_no = $summary_interval";
    }
    
    # Then, if a time filter was specified and we need one, apply it.  If we
    # are were given a summary interval and no non-geographic filters were
    # specified, then we don't need one because the necessary filtering has
    # already been done by selecting the appropriate interval_no in the summary table.
    
    if ( defined $early_age or defined $late_age )
    {
	unless ( $op eq 'summary' and not $non_geo_filter and $time_rule eq 'buffer' )
	{
	    $tables_ref->{c} = 1;
	    
	    # The exact filter we use will depend upon the time rule that was
	    # selected.
	    
	    if ( $time_rule eq 'contain' )
	    {
		if ( defined $late_age and $late_age > 0 )
		{
		    push @filters, "c.late_age >= $late_age";
		}
		
		if ( defined $early_age and $early_age > 0 )
		{
		    push @filters, "c.early_age <= $early_age";
		}
	    }
	    
	    elsif ( $time_rule eq 'overlap' )
	    {
		if ( defined $late_age and $late_age > 0 )
		{
		    push @filters, "c.early_age > $late_age";
		}
		
		if ( defined $early_age and $early_age > 0 )
		{
		    push @filters, "c.late_age < $early_age";
		}
	    }
	    
	    else # $time_rule eq 'buffer'
	    {
		if ( defined $late_age and defined $early_age and 
		     defined $late_bound and defined $early_bound )
		{
		    push @filters, "c.early_age <= $early_bound and c.late_age >= $late_bound";
		    push @filters, "(c.early_age < $early_bound or c.late_age > $late_bound)";
		    push @filters, "c.early_age > $late_age";
		    push @filters, "c.late_age < $early_age";
		}
		
		else
		{
		    if ( defined $late_age and defined $late_bound )
		    {
			push @filters, "c.late_age >= $late_bound and c.early_age > $late_age";
		    }
		    
		    if ( defined $early_age and defined $early_bound )
		    {
			push @filters, "c.early_age <= $early_bound and c.late_age < $early_age";
		    }
		}
	    }
	}
    }
    
    # Return the list
    
    return @filters;
}


# adjustCoordinates ( fields_ref )
# 
# Alter the output coordinate fields to match the longitude/latitude bounds.

sub adjustCoordinates {

    my ($self, $fields_ref) = @_;
    
    return unless $self->{params}{lngmin};
    
    my $x1 = $self->{params}{lngmin};
    my $x2 = $self->{params}{lngmax};
    
    # Adjust the output coordinates to fall within the range indicated by the
    # input parameters.
    
    my $x1_offset = 0;
    my $x2_offset = 0;
    
    if ( $x1 < -180.0 )
    {
	$x1_offset = -1 * floor( (180.0 - $x1) / 360.0) * 360.0;
    }
    
    elsif ( $x1 > 180.0 )
    {
	$x1_offset = floor( ($x1 + 180.0) / 360.0 ) * 360.0;
    }
    
    if ( $x2 < -180.0 )
    {
	$x2_offset = -1 * floor( (180.0 - $x2) / 360.0) * 360.0;
    }
    
    elsif ( $x2 > 180.0 )
    {
	$x2_offset = floor( ($x2 + 180.0) / 360.0 ) * 360.0;
    }
    
    # Now make sure we have an actual expression.
    
    $x1_offset = "+$x1_offset" unless $x1_offset < 0;
    $x2_offset = "+$x2_offset" unless $x2_offset < 0;
    
    # If the longitude bounds do not cross the antimeridian, we just need to
    # add the specified offset.
    
    if ( $x1_offset == $x2_offset )
    {
	$$fields_ref =~ s/([a-z]\.lng)/$1$x1_offset as lng/;
    }
    
    # Otherwise, we have to use one offset for positive coords and the other
    # for negative ones.
    
    else
    {
	$$fields_ref =~ s/([a-z]\.lng)/if($1<0,$1$x2_offset,$1$x1_offset) as lng/;
    }
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
    $tables->{c} = 1 if $tables->{o};
    
    # Create the necessary join expressions.
    
    $join_list .= "JOIN occ_matrix as o using (collection_no)\n"
	if $tables->{o};
    $join_list .= "JOIN taxon_trees as t using (orig_no)\n"
	if $tables->{t};
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
    
    $join_list .= "LEFT JOIN $INTERVAL_DATA as ei on ei.interval_no = $mt.early_int_no\n"
	if $tables->{ei};
    $join_list .= "LEFT JOIN $INTERVAL_DATA as li on li.interval_no = $mt.late_int_no\n"
	if $tables->{li};
        
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
