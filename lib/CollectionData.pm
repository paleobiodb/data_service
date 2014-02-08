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

our (@REQUIRES_CLASS) = qw(CommonData ReferenceData);

our ($MAX_BIN_LEVEL) = 0;


# initialize ( )
# 
# This routine is called once by Web::DataService in order to initialize this
# class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # First read the configuration information that describes how the
    # collections are organized into summary clusters (bins).
    
    my $config = $ds->get_config;
    
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
    
    $ds->define_output_map('1.1:colls:basic_map' =>
        { value => 'basic', maps_to => '1.1:colls:basic', fixed => 1 },
	{ value => 'bin', maps_to => '1.1:colls:bin' },
	    "The list of geographic clusters to which the collection belongs.",
        { value => 'attr', maps_to => '1.1:colls:attr' },
	    "The attribution of the collection: the author name(s) from",
	    "the primary reference, and the year of publication.",
        { value => 'ref', maps_to => '1.1:refs:primary' },
	    "The primary reference for the collection, as formatted text.",
        { value => 'loc', maps_to => '1.1:colls:loc' },
	    "Additional information about the geographic locality of the collection",
	{ value => 'prot', maps_to => '1.1:colls:prot' },
	    "Indicate whether the collection is on protected land",
        { value => 'time', maps_to => '1.1:colls:time' },
	    "Additional information about the temporal locality of the",
	    "collection.",
	{ value => 'strat', maps_to => '1.1:colls:strat' },
	    "Basic information about the stratigraphic context of the collection.",
	{ value => 'stratext', maps_to => '1.1:colls:stratext' },
	    "Detailed information about the stratigraphic context of collection.",
	    "This includes all of the information from C<strat> plus extra fields.",
	{ value => 'lith', maps_to => '1.1:colls:lith' },
	    "Basic information about the lithological context of the collection.",
	{ value => 'lithext', maps_to => '1.1:colls:lithext' },
	    "Detailed information about the lithological context of the collection.",
	    "This includes all of the information from C<lith> plus extra fields.",
	{ value => 'geo', maps_to => '1.1:colls:geo' },
	    "Information about the geological context of the collection",
        { value => 'rem', maps_to => '1.1:colls:rem' },
	    "Any additional remarks that were entered about the collection.",
	{ value => 'ent', maps_to => '1.1:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.1:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.1:common:crmod' },
	    "The C<created> and C<modified> timestamps for the collection record");
    
    # Then define the output blocks that it mentions.
    
    $ds->define_block('1.1:colls:basic' =>
      { select => ['c.collection_no', 'cc.collection_name', 'cc.collection_subset', 'cc.formation',
		   'c.lat', 'c.lng', 'cc.latlng_basis as llb', 'cc.latlng_precision as llp',
		   'c.n_occs', 'ei.interval_name as early_interval', 'li.interval_name as late_interval',
		   'c.reference_no', 'group_concat(sr.reference_no) as reference_nos'], 
	tables => ['cc', 'ei', 'li', 'sr'] },
      { output => 'collection_no', dwc_name => 'collectionID', com_name => 'oid' },
	  "A positive integer that uniquely identifies the collection",
      { output => 'record_type', value => 'collection', com_name => 'typ', com_value => 'col', 
	dwc_value => 'Occurrence' },
	  "type of this object: 'col' for a collection",
      { output => 'formation', com_name => 'sfm', not_block => 'strat' },
	  "The formation in which the collection was found",
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
	  "If the collection is a part of another one, this field specifies which part",
      { output => 'attribution', dwc_name => 'recordedBy', com_name => 'att', if_block => 'attr' },
	  "The attribution (author and year) of the collection",
      { output => 'pubyr', com_name => 'pby', if_block => 'attr' },
	  "The year in which the collection was published",
      { output => 'n_occs', com_name => 'noc' },
	  "The number of occurrences in the collection",
      { output => 'early_interval', com_name => 'oei', pbdb_name => 'early_interval' },
	  "The specific geologic time range associated with the collection (not necessarily a",
	  "standard interval), or the interval that begins the range if C<late_interval> is also given",
      { output => 'late_interval', com_name => 'oli', pbdb_name => 'late_interval', dedup => 'early_interval' },
	  "The interval that ends the specific geologic time range associated with the collection",
      { set => 'reference_no', from_record => 1, code => \&set_collection_refs },
      { output => 'reference_no', com_name => 'rid', text_join => ', ' },
	  "The identifier(s) of the references from which this data was entered");
    
    $ds->define_block('1.1:colls:bin' =>
      { output => 'bin_id_1', com_name => 'lv1' },
	  "The identifier of the level-1 cluster in which the collection is located",
      { output => 'bin_id_2', com_name => 'lv2' },
	  "The identifier of the level-2 cluster in which the collection is located",
      { output => 'bin_id_3', com_name => 'lv3' },
	  "The identifier of the level-2 cluster in which the collection is located",
      { output => 'bin_id_4', com_name => 'lv4' },
	  "The identifier of the level-2 cluster in which the collection is located",
      { output => 'bin_id_5', com_name => 'lv5' },
	  "The identifier of the level-3 cluster in which the collection is located");

    
    $ds->define_block('1.1:colls:attr' =>
        { select => ['r.author1init as a_ai1', 'r.author1last as a_al1', 'r.author2init as a_ai2', 
	  	     'r.author2last as a_al2', 'r.otherauthors as a_oa', 'r.pubyr as a_pubyr'],
          tables => ['r'] },
        { set => 'attribution', from_record => 1, code => \&generateAttribution },
        { set => 'pubyr', from => 'a_pubyr' });
    
    $ds->define_block('1.1:colls:ref' =>
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
	  "The primary reference associated with the collection (as formatted text)");
    
    $ds->define_block('1.1:colls:loc' =>
      { select => ['c.cc', 'cc.state', 'cc.county', 'cc.geogscale'],
	tables => ['cc'] },
      { output => 'cc', com_name => 'cc2' },
	  "The country in which the collection is located, encoded as",
	  "L<ISO-3166-1 alpha-2|https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2>",
      { output => 'state', com_name => 'sta' },
	  "The state or province in which the collection is located, if known",
      { output => 'county', com_name => 'cny' },
	  "The county or municipal area in which the collection is located, if known",
      { output => 'geogscale', com_name => 'gsc' },
	  "The geographic scale of the collection.");
    
    $ds->define_block('1.1:colls:prot' =>
	{ select => ['c.cc', 'c.protected'] },
	{ output => 'cc', com_name => 'cc2', not_block => 'loc' },
	    "The country in which the collection is located, encoded as",
	    "L<ISO-3166-1 alpha-2|https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2>",
	{ output => 'protected', com_name => 'ptd' },
	    "The protected status of the land on which the collection is located, if any");
    
    $ds->define_block('1.1:colls:time' =>
      { select => ['$mt.early_age', '$mt.late_age', 'im.cx_int_no', 'im.early_int_no', 'im.late_int_no'],
	tables => ['im'] },
      { set => '*', code => \&fixTimeOutput },
      { output => 'early_age', com_name => 'eag' },
	  "The early bound of the geologic time range associated with the collection or cluster (in Ma)",
      { output => 'late_age', com_name => 'lag' },
	  "The late bound of the geologic time range associated with the collection or cluster (in Ma)",
      { output => 'cx_int_no', com_name => 'cxi' },
	  "The identifier of the most specific single interval from the selected timescale that",
	  "covers the entire time range associated with the collection or cluster.",
      { output => 'early_int_no', com_name => 'ein' },
	  "The beginning of a range of intervals from the selected timescale that most closely",
	  "brackets the time range associated with the collection or cluster (with C<late_int_no>)",
      { output => 'late_int_no', com_name => 'lin' },
	  "The end of a range of intervals from the selected timescale that most closely brackets",
	  "the time range associated with the collection or cluster (with C<early_int_no>)");
    
    $ds->define_block('1.1:colls:strat' =>
	{ select => ['cc.formation', 'cc.geological_group', 'cc.member'], tables => 'cc' },
	{ output => 'formation', com_name => 'sfm' },
	    "The stratigraphic formation in which the collection is located, if known",
	{ output => 'geological_group', pbdb_name => 'stratgroup', com_name => 'sgr' },
	    "The stratigraphic group in which the collection is located, if known",
	{ output => 'member', com_name => 'smb' },
	    "The stratigraphic member in which the collection is located, if known");
    
    $ds->define_block('1.1:colls:stratext' =>
	{ include => '1.1:colls:strat' },
	{ select => [ qw(cc.zone cc.localsection cc.localbed cc.localorder
		         cc.regionalsection cc.regionalbed cc.regionalorder
		         cc.stratscale cc.stratcomments) ], tables => 'cc' },
	{ output => 'stratscale', com_name => 'ssc' },
	    "The stratigraphic range covered by this collection",
	{ output => 'zone', com_name => 'szn' },
	    "The stratigraphic zone in which the collection is located, if known",
	{ output => 'localsection', com_name => 'sls' },
	    "The local section in which the collection is located, if known",
	{ output => 'localbed', com_name => 'slb' },
	    "The local bed in which the collection is located, if known",
	{ output => 'localorder', com_name => 'slo' },
	    "The order in which local beds were described, if known",
	{ output => 'regionalsection', com_name => 'srs' },
	    "The regional section in which the collection is located, if known",
	{ output => 'regionalbed', com_name => 'srb' },
	    "The regional bed in which the collection is located, if known",
	{ output => 'regionalorder', com_name => 'sro' },
	    "The order in which regional beds were described, if known",
	{ output => 'stratcomments', com_name => 'scm' },
	    "Additional comments about the stratigraphic context of the collection, if any");
    
    $ds->define_block('1.1:colls:lith' =>
	{ select => [ qw(cc.lithdescript cc.lithification cc.minor_lithology cc.lithology1
			 cc.lithification2 cc.minor_lithology2 cc.lithology2) ], tables => 'cc' },
	{ output => 'lithdescript', com_name => 'ldc' },
	    "Detailed description of the collection site in terms of lithology",
	{ output => 'lithology1', com_name => 'lt1' },
	    "The first lithology described for the collection site; the database can",
	    "represent up to two different lithologies per collection",
	{ output => 'lithadj', pbdb_name => 'lithadj1', com_name => 'la1', if_block => 'lithext' },
	    "Adjective(s) describing the first lithology",
	{ output => 'lithification', pbdb_name => 'lithification1', com_name => 'lf1' },
	    "Lithification state of the first lithology described for the site",
	{ output => 'minor_lithology', pbdb_name => 'minor_lithology1', com_name => 'lm1' },
	    "Minor lithology associated with the first lithology described for the site",
	{ output => 'fossilsfrom1', com_name => 'ff1', if_block => 'lithext' },
	    "Whether or not fossils were taken from the first described lithology",
	{ output => 'lithology2', com_name => 'lt2' },
	    "The second lithology described for the collection site, if any",
	{ output => 'lithadj2', com_name => 'la2', if_block => 'lithext' },
	    "Adjective(s) describing the second lithology, if any",
	{ output => 'lithification2', com_name => 'lf2' },
	    "Lithification state of the second lithology described for the site.  See above for values.",
	{ output => 'minor_lithology2', com_name => 'lm2' },
	    "Minor lithology associated with the second lithology described for the site, if any",
	{ output => 'fossilsfrom2', com_name => 'ff2', if_block => 'lithext' },
	    "Whether or not fossils were taken from the second described lithology");
    
    $ds->define_block('1.1:colls:lithext' =>
	{ select => [ qw(cc.lithadj cc.fossilsfrom1 cc.lithadj2 cc.fossilsfrom2) ], tables => 'cc' },
	{ include => '1.1:colls:lith' });
    
    $ds->define_block('1.1:colls:geo' =>
	{ select => [ qw(cc.environment cc.tectonic_setting cc.geology_comments) ], tables => 'cc' },
	{ output => 'environment', com_name => 'env' },
	    "The paleoenvironment of the collection site",
	{ output => 'tectonic_setting', com_name => 'tec' },
	    "The tectonic setting of the collection site",
	{ output => 'geology_comments', com_name => 'gcm' },
	    "General comments about the geology of the collection site");
    
    $ds->define_block('taxon_record' =>
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

    $ds->define_block( '1.1:colls:taxa' =>
      { output => 'taxa', com_name => 'tax', rule => 'taxon_record' },
	  "A list of records describing the taxa that have been identified",
	  "as appearing in the collection");
    
    $ds->define_block( '1.1:colls:rem' =>
      { set => 'collection_aka', join => '; ', if_format => 'txt,tsv,csv,xml' },
      { output => 'collection_aka', dwc_name => 'collectionRemarks', com_name => 'crm' },
	  "Any additional remarks that were entered about the collection");
    
    # Finally, define rulesets to interpret the parmeters used with operations
    # defined by this class.
    
    $ds->define_set('1.1:colls:order' =>
	{ value => 'earlyage' },
	    "Results are ordered chronologically by early age bound, oldest to youngest unless you add C<.asc>",
	{ value => 'earlyage.asc', undoc => 1 },
	{ value => 'earlyage.desc', undoc => 1 },
	{ value => 'lateage' },
	    "Results are ordered chronologically by late age bound, oldest to youngest unless you add C<.asc>",
	{ value => 'lateage.asc', undoc => 1 },
	{ value => 'lateage.desc', undoc => 1 },
	{ value => 'agespread' },
	    "Results are ordered based on the difference between the early and late age bounds, starting",
	    "with occurrences with the largest spread (least precise temporal resolution) unless you add C<.asc>",
	{ value => 'agespread.asc', undoc => 1 },
	{ value => 'agespread.desc', undoc => 1 },
	{ value => 'formation' },
	    "Results are ordered by the stratigraphic formation in which they were found, sorted alphabetically.",
	{ value => 'formation.asc', undoc => 1 },
	{ value => 'formation.desc', undoc => 1 },
	{ value => 'stratgroup' },
	    "Results are ordered by the stratigraphic group in which they were found, sorted alphabetically.",
	{ value => 'stratgroup.asc', undoc => 1 },
	{ value => 'stratgroup.desc', undoc => 1 },
	{ value => 'member' },
	    "Results are ordered by the stratigraphic member in which they were found, sorted alphabetically.",
	{ value => 'member.asc', undoc => 1 },
	{ value => 'member.desc', undoc => 1 },
	{ value => 'created' },
	    "Results are ordered by the date the record was created, most recent first",
	    "unless you add C<.asc>.",
	{ value => 'created.asc', undoc => 1 },
	{ value => 'created.desc', undoc => 1 },
	{ value => 'modified' },
	    "Results are ordered by the date the record was last modified",
	    "most recent first unless you add C<.asc>",
	{ value => 'modified.asc', undoc => 1 },
	{ value => 'modified.desc', undoc => 1 });
    
    $ds->define_ruleset('1.1:main_selector' =>
	{ param => 'clust_id', valid => POS_VALUE, list => ',' },
	    "Return only records associated with the specified geographic clusters.",
	    "You may specify one or more cluster ids, separated by commas.",
	{ param => 'taxon_name', valid => \&TaxonData::validNameSpec },
	    "Return only records associated with the specified taxonomic name(s).  You may specify multiple names, separated by commas.",
	{ param => 'taxon_id', valid => POS_VALUE, list => ','},
	    "Return only records associated with the specified taxonomic name(s), specified by numeric identifier.",
	    "You may specify multiple identifiers, separated by commas.",
	{ param => 'taxon_actual', valid => FLAG_VALUE },
	    "If this parameter is specified, then only records that were actually identified with the",
	    "specified taxonomic name and not those which match due to synonymy",
	    "or other correspondences between taxa.  This is a flag parameter, which does not need any value.",
	{ param => 'base_name', valid => \&TaxonData::validNameSpec, list => ',' },
	    "Return only records associated with the specified taxonomic name(s), or I<any of their children>.",
	    "You may specify multiple names, separated by commas.",
	{ param => 'base_id', valid => POS_VALUE, list => ',' },
	    "Return only records associated with the specified taxonomic name(s), specified by numeric identifier, or I<any of their children>.",
	    "You may specify multiple identifiers, separated by commas.",
	    "Note that you may specify at most one of 'taxon_name', 'taxon_id', 'base_name', 'base_id'.",
	{ at_most_one => ['taxon_name', 'taxon_id', 'base_name', 'base_id'] },
	{ param => 'exclude_id', valid => POS_VALUE, list => ','},
	    "Exclude any records whose associated taxonomic name is a child of the given name or names, specified by numeric identifier.",
	{ param => 'person_id', valid => POS_VALUE, list => ','},
	    "Return only records whose entry was authorized by the given person or people, specified by numeric identifier.",
	{ param => 'lngmin', valid => DECI_VALUE },
	{ param => 'lngmax', valid => DECI_VALUE },
	{ param => 'latmin', valid => DECI_VALUE },
	{ param => 'latmax', valid => DECI_VALUE },
	    "Return only records whose geographic location falls within the given bounding box.",
	    "The longitude boundaries will be normalized to fall between -180 and 180, and will generate",
	    "two adjacent bounding boxes if the range crosses the antimeridian.",
	    "Note that if you specify C<lngmin> then you must also specify C<lngmax>.",
	{ together => ['lngmin', 'lngmax'],
	  error => "you must specify both of 'lngmin' and 'lngmax' if you specify either of them" },
	{ param => 'loc', valid => ANY_VALUE },		# This should be a geometry in WKT format
	    "Return only records whose geographic location falls within the specified geometry, specified in WKT format.",
	{ param => 'continent', valid => ANY_VALUE, list => ',' },
	    "Return only records whose geographic location falls within the specified continent(s).  The list of accepted",
	    "continents can be retrieved via a L<config|/data1.1/config> request.",
	{ param => 'formation', valid => ANY_VALUE, list => ',' },
	    "Return only records that fall within the named stratigraphic formation(s).  You may",
	    "specify more than one, separated by commas.",
	{ param => 'stratgroup', valid => ANY_VALUE, list => ',' },
	    "Return only records that fall within the named stratigraphic group(s).  You may",
	    "specify more than one, separated by commas.",
	{ param => 'member', valid => ANY_VALUE, list => ',' },
	    "Return only records that fall within the named stratigraphic member(s).  You may",
	    "specify more than one, separated by commas.",
	{ param => 'min_ma', valid => DECI_VALUE(0) },
	    "Return only records whose temporal locality is at least this old, specified in Ma.",
	{ param => 'max_ma', valid => DECI_VALUE(0) },
	    "Return only records whose temporal locality is at most this old, specified in Ma.",
	{ param => 'interval_id', valid => POS_VALUE },
	    "Return only records whose temporal locality falls within the given geologic time interval, specified by numeric identifier.",
	{ param => 'interval', valid => ANY_VALUE },
	    "Return only records whose temporal locality falls within the named geologic time interval.",
	{ at_most_one => ['interval_id', 'interval', 'min_ma'] },
	{ at_most_one => ['interval_id', 'interval', 'max_ma'] },
	{ optional => 'timerule', valid => ENUM_VALUE('contain','overlap','buffer') },
	    "Resolve temporal locality according to the specified rule:", "=over 4",
	    "=item contain", "Return only records whose temporal locality is strictly contained in the specified time range.",
	    "=item overlap", "Return only records whose temporal locality overlaps the specified time range.",
	    "=item buffer", "Return only records whose temporal locality overlaps the specified range and is contained",
	    "within the specified time range plus a buffer on either side.  If an interval from one of the timescales known to the database is",
	    "given, then the default buffer will be the intervals immediately preceding and following at the same level.",
	    "Otherwise, the buffer will default to 10 million years on either side.  This can be overridden using the parameters",
	    "C<earlybuffer> and C<latebuffer>.  This is the default value for this option.",
	{ optional => 'earlybuffer', valid => POS_VALUE },
	    "Override the default buffer period for the beginning of the time range when resolving temporal locality.",
	    "The value is given in millions of years.  This option is only relevant if C<timerule> is C<buffer> (which is the default).",
	{ optional => 'latebuffer', valid => POS_VALUE },
	    "Override the default buffer period for the end of the time range when resolving temporal locality.",
	    "The value is given in millions of years.  This option is only relevant if C<timerule> is C<buffer> (which is the default).");

    $ds->define_ruleset('1.1:colls:specifier' =>
	{ param => 'id', valid => POS_VALUE, alias => 'coll_id' },
	    "The identifier of the collection you wish to retrieve (REQUIRED)");
    
    $ds->define_ruleset('1.1:colls:selector' =>
	{ param => 'id', valid => INT_VALUE, list => ',', alias => 'coll_id' },
	    "A comma-separated list of collection identifiers.");
    
    $ds->define_ruleset('1.1:colls:display' =>
	"You can use the following parameter to request additional information about each",
	"retrieved collection:",
	{ optional => 'show', list => q{,},
	  valid => $ds->valid_set('1.1:colls:basic_map') },
	    "Selects additional information to be returned",
	    "along with the basic record for each collection.  Its value should be",
	    "one or more of the following, separated by commas:",
	    $ds->document_set('1.1:colls:basic_map'),
	{ optional => 'order', valid => $ds->valid_set('1.1:colls:order'), split => ',' },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  Accepted values are:",
	    $ds->document_set('1.1:colls:order'),
	    "If no order is specified, results are sorted by collection identifier.",
	{ ignore => 'level' });
    
    $ds->define_ruleset('1.1:colls:single' => 
	"The following required parameter selects a record to retrieve:",
    	{ require => '1.1:colls:specifier', 
	  error => "you must specify a collection identifier, either in the URL or with the 'id' parameter" },
    	{ allow => '1.1:colls:display' },
    	{ allow => '1.1:common_params' },
        "^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");

    $ds->define_ruleset('1.1:colls:list' => 
	">You can use the following parameter if you wish to retrieve information about",
	"a known list of collections, or to filter a known list against other criteria such as location or time.",
	"Only the records which match the other parameters that you specify will be returned.",
    	{ allow => '1.1:colls:selector' },
        ">The following parameters can be used to query for collections by a variety of criteria.",
	"Except as noted below, you may use these in any combination.",
	"These parameters can all be used to select either occurrences, collections, or associated references.",
   	{ allow => '1.1:main_selector' },
	{ allow => '1.1:common:select_crmod' },
	{ allow => '1.1:common:select_ent' },
	{ require_any => ['1.1:colls:selector', '1.1:main_selector',
			  '1.1:common:select_crmod', '1.1:common:select_ent'] },
	">You can also specify any of the following parameters:",
    	{ allow => '1.1:colls:display' },
    	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request");
    
    $ds->define_ruleset('1.1:colls:refs' =>
	">You can use the following parameters if you wish to retrieve the references associated",
	"with a known list of collections, or to filter a known list against",
	"other criteria such as location or time.",
	"Only the records which match the other parameters that you specify will be returned.",
	{ allow => '1.1:colls:selector' },
        ">The following parameters can be used to retrieve the references associated with occurrences",
	"selected by a variety of criteria.  Except as noted below, you may use these in any combination.",
	"These parameters can all be used to select either occurrences, collections, or associated references.",
	{ allow => '1.1:main_selector' },
	{ allow => '1.1:common:select_crmod' },
	{ allow => '1.1:common:select_ent' },
	{ require_any => ['1.1:colls:selector', '1.1:main_selector',
			  '1.1:common:select_crmod', '1.1:common:select_ent'] },
	">You can also specify any of the following parameters:",
	{ allow => '1.1:refs:filter' },
	{ allow => '1.1:refs:display' },
	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.",
	">If the parameter C<order> is not specified, the results are sorted alphabetically by",
	"the name of the primary author.");
    
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
    
    my $dbh = $self->get_dbh;
    
    # Make sure we have a valid id number.
    
    my $id = $self->{params}{id};
    
    die "Bad identifier '$id'" unless defined $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = join(', ', $self->select_list({ mt => 'c', bt => 'cc' }));
    
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
    
    my $dbh = $self->get_dbh;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my $tables = $self->tables_hash;
    
    my @filters = $self->generateMainFilters('list', 'c', $tables);
    push @filters, $self->generateCollFilters($tables);
    push @filters, CommonData::generate_crmod_filters($self, 'cc', $tables);
    push @filters, CommonData::generate_ent_filters($self, 'cc', $tables);
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $self->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->select_string({ mt => 'c', bt => 'cc' });
    
    $self->adjustCoordinates(\$fields);
    
    # Determine the order in which the results should be returned.
    
    my $order_clause = $self->generate_order_clause($tables, { at => 'c', bt => 'cc' }) || 'c.collection_no';
    
    # Determine if any extra tables need to be joined in.
    
    my $base_joins = $self->generateJoinList('c', $self->tables_hash);
	
    $self->{main_sql} = "
	SELECT $calc $fields
	FROM coll_matrix as c join collections as cc using (collection_no)
		LEFT JOIN secondary_refs as sr using (collection_no)
		$base_joins
        WHERE $filter_string
	GROUP BY c.collection_no
	ORDER BY $order_clause
	$limit";
    
    print STDERR $self->{main_sql} . "\n\n" if $self->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $self->sql_count_rows;
    
    return 1;
}


# refs ( )
# 
# Query the database for the references associated with occurrences satisfying
# the conditions specified by the parameters.

sub refs {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_dbh;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my $inner_tables = {};
    
    my @filters = CollectionData::generateMainFilters($self, 'list', 'c', $inner_tables);
    push @filters, $self->generateCollFilters($inner_tables);
    push @filters, CommonData::generate_crmod_filters($self, 'cc', $inner_tables);
    push @filters, CommonData::generate_ent_filters($self, 'cc', $inner_tables);
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # Construct another set of filter expressions to act on the references.
    
    my @ref_filters = $self->ReferenceData::generate_filters($self->tables_hash);
    push @ref_filters, "1=1" unless @ref_filters;
    
    my $ref_filter_string = join(' and ', @ref_filters);
    
    # Figure out the order in which we should return the references.  If none
    # is selected by the options, sort by rank descending.
    
    my $order = ReferenceData::generate_order_clause($self, { rank_table => 's' }) ||
	"r.author1last, r.author1init";
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $self->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->select_string({ mt => 'r', bt => 'r' });
    
    $self->adjustCoordinates(\$fields);
    
    my $inner_join_list = $self->generateJoinList('c', $inner_tables);
    my $outer_join_list = $self->ReferenceData::generate_join_list($self->tables_hash);
    
    $self->{main_sql} = "
	SELECT $calc $fields, s.reference_rank FROM refs as r JOIN
	   (SELECT sr.reference_no, count(*) as reference_rank
	    FROM $COLL_MATRIX as c JOIN collections as cc using (collection_no)
		LEFT JOIN secondary_refs as sr using (collection_no)
		$inner_join_list
            WHERE $filter_string
	    GROUP BY sr.reference_no) as s using (reference_no)
	$outer_join_list
	WHERE $ref_filter_string
	ORDER BY $order
	$limit";
    
    print STDERR $self->{main_sql} . "\n\n" if $self->debug;
    
    # Then prepare and execute the main query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $self->sql_count_rows;
}


# fixTimeOutput ( record )
# 
# Adjust the time output by truncating unneeded digits.

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
    
    my $dbh = $self->get_dbh;
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
    
    my $dbh = $self->get_dbh;
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
    
    # Check for parameters 'formation', 'stratgroup', 'member'
    
    if ( my @formations = $self->clean_param_list('formation') )
    {
	foreach my $f (@formations)
	{
	    $f =~ s/%/.*/g;
	    $f =~ s/_/./g;
	}
	my $pattern = '^(' . join('|', @formations) . ')$';
	my $quoted = $dbh->quote($pattern);
	push @filters, "cc.formation rlike $quoted";
	$tables_ref->{cc} = 1;
    }
    
    if ( my @stratgroups = $self->clean_param_list('stratgroup') )
    {
	foreach my $f (@stratgroups)
	{
	    $f =~ s/%/.*/g;
	    $f =~ s/_/./g;
	}
	my $pattern = '^(' . join('|', @stratgroups) . ')$';
	my $quoted = $dbh->quote($pattern);
	push @filters, "cc.geological_group rlike $quoted";
	$tables_ref->{cc} = 1;
    }
    
    if ( my @members = $self->clean_param_list('member') )
    {
	foreach my $f (@members)
	{
	    $f =~ s/%/.*/g;
	    $f =~ s/_/./g;
	}
	my $pattern = '^(' . join('|', @members) . ')$';
	my $quoted = $dbh->quote($pattern);
	push @filters, "cc.member rlike $quoted";
	$tables_ref->{cc} = 1;
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


# generate_order_clause ( options )
# 
# Return the order clause for the list of references, or the empty string if
# none was selected.  If the option 'allow_taxon' is true, then allow ordering
# based on taxon.

sub generate_order_clause {
    
    my ($self, $tables, $options) = @_;
    
    $options ||= {};
    my $at = $options->{at} || 'c';
    my $bt = $options->{bt} || 'cc';
    my $tt = $options->{tt};
    
    my $order = $self->clean_param('order');
    my @terms = ref $order eq 'ARRAY' ? @$order : $order;
    my @exprs;
    
    # Now generate the corresponding expression for each term.
    
    foreach my $term ( @terms )
    {
	my $dir = '';
	next unless $term;
	
	if ( $term =~ /^(\w+)[.](asc|desc)$/ )
	{
	    $term = $1;
	    $dir = $2;
	}
	
	if ( $term eq 'earlyage' )
	{
	    $dir ||= 'desc';
	    push @exprs, "$at.early_age $dir";
	    $tables->{$at} = 1;
	}
	
	elsif ( $term eq 'lateage' )
	{
	    $dir ||= 'desc';
	    push @exprs, "$at.late_age $dir";
	    $tables->{$at} = 1;
	}
	
	elsif ( $term eq 'agespread' )
	{
	    push @exprs, "($at.early_age - $at.late_age) $dir",
	    $tables->{$at} = 1;
	}
	
	elsif ( $term eq 'taxon' && $tt )
	{
	    push @exprs, "$tt.lft $dir";
	    $tables->{$tt} = 1;
	}
	
	elsif ( $term eq 'reso' && $tt )
	{
	    $dir ||= 'desc';
	    push @exprs, "$tt.rank $dir";
	    $tables->{$tt} = 1;
	}
	
	elsif ( $term eq 'formation' )
	{
	    push @exprs, "cc.formation $dir";
	    $tables->{cc} = 1;
	}
	
	elsif ( $term eq 'stratgroup' )
	{
	    push @exprs, "cc.geological_group $dir";
	    $tables->{cc} = 1;
	}
	
	elsif ( $term eq 'member' )
	{
	    push @exprs, "cc.member $dir";
	    $tables->{cc} = 1;
	}
	
	elsif ( $term eq 'created' )
	{
	    $dir ||= 'desc';
	    push @exprs, "$bt.reference_no $dir";
	    $tables->{$bt} = 1;
	}
	
	elsif ( $term eq 'modified' )
	{
	    $dir ||= 'desc';
	    push @exprs, "$bt.modified $dir";
	    $tables->{$bt} = 1;
	}
	
	else
	{
	    die "400 bad value for parameter 'order' (was '$term')";
	}
    }
    
    return join(', ', @exprs);
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


# set_collection_references ( record )
# 
# Set the reference_no field based on reference_no and reference_nos.  The
# latter holds all of the reference numbers as a comma-separated list, the
# former holds the primary reference number which should always be reported
# first.  The result must be a listref, even if there is only one reference
# number reported.

sub set_collection_refs {
    
    my ($self, $record) = @_;
    
    my @refs = split qr{,}, $record->{reference_nos};
    
    foreach my $i (0..$#refs)
    {
	if ( $refs[$i] == $record->{reference_no} )
	{
	    splice(@refs, $i, 1);
	    unshift @refs, $record->{reference_no};
	}
    }
    
    return \@refs;
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



# cache_still_good ( key, created )
# 
# Return true if the specified cache entry should be treated as still good,
# false otherwise.  The parameter $created will be the epoch time when the
# entry was created.

sub cache_still_good {
    
    my ($key, $created) = @_;
    
    # For the moment, the cache entries stay good until they naturally expire.
    
    return 1;
}


1;
