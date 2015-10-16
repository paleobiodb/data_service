# CollectionData
# 
# A class that returns information from the PaleoDB database about a single
# collection or a category of collections.  This is a subclass of PBDataQuery.
# 
# Author: Michael McClennen

use strict;

package PB1::CollectionData;

use HTTP::Validate qw(:validators);

use PB1::CommonData qw(generateAttribution);
use PB1::ReferenceData qw(format_reference);

use TableDefs qw($COLL_MATRIX $COLL_BINS $COLL_STRATA $COUNTRY_MAP $PALEOCOORDS $GEOPLATES
		 $INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP $INTERVAL_BUFFER);
use TaxonomyOld;

use Carp qw(carp croak);
use POSIX qw(floor ceil);

use Moo::Role;

no warnings 'numeric';


our (@REQUIRES_ROLE) = qw(PB1::CommonData PB1::ConfigData PB1::ReferenceData);

our ($MAX_BIN_LEVEL) = 0;
our (%COUNTRY_NAME, %CONTINENT_NAME);
    

# initialize ( )
# 
# This routine is called once by Web::DataService in order to initialize this
# class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # First read the configuration information that describes how the
    # collections are organized into summary clusters (bins).
    
    my $bins = $ds->config_value('bins');
    
    if ( ref $bins eq 'ARRAY' )
    {
	my $bin_string = '';
	my $bin_level = 0;
	
	foreach (@$bins)
	{
	    $bin_level++;
	    $bin_string .= ", " if $bin_string;
	    $bin_string .= "bin_id_$bin_level";
	}
	
	$MAX_BIN_LEVEL = $bin_level;
    }
    
    # Then get a list of countries and continents.
    
    foreach my $r ( @$PB1::ConfigData::COUNTRIES )
    {
	$COUNTRY_NAME{$r->{cc}} = $r->{name};
    }
    
    foreach my $r ( @$PB1::ConfigData::CONTINENTS )
    {
	$CONTINENT_NAME{$r->{continent_code}} = $r->{continent_name};
    }
    
    # Define an output map listing the blocks of information that can be
    # returned about collections.
    
    $ds->define_output_map('1.1:colls:basic_map' =>
	{ value => 'bin', maps_to => '1.1:colls:bin' },
	    "The list of geographic clusters to which the collection belongs.",
        { value => 'attr', maps_to => '1.1:colls:attr' },
	    "The attribution of the collection: the author name(s) from",
	    "the primary reference, and the year of publication.",
        { value => 'ref', maps_to => '1.1:refs:primary' },
	    "The primary reference for the collection, as formatted text.",
        { value => 'loc', maps_to => '1.1:colls:loc' },
	    "Additional information about the geographic locality of the collection",
	{ value => 'paleoloc', maps_to => '1.1:colls:paleoloc' },
	    "Information about the paleogeographic locality of the collection,",
	    "evaluated according to the model(s) specified by the parameter C<pgm>.",
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
    
    # Then a second block for geographic summary clusters.
    
    $ds->define_output_map('1.1:colls:summary_map' =>
        { value => 'ext', maps_to => '1.1:colls:ext' },
	    "Additional information about the geographic extent of each cluster.",
        { value => 'time', maps_to => '1.1:colls:time' },
	  # This block is defined in our parent class, CollectionData.pm
	    "Additional information about the temporal range of the",
	    "cluster.");
    
    # Then define the output blocks which these mention.
    
    $ds->define_block('1.1:colls:basic' =>
      { select => ['c.collection_no', 'cc.collection_name', 'cc.collection_subset', 'cc.formation',
		   'c.lat', 'c.lng', 'cc.latlng_basis as llb', 'cc.latlng_precision as llp',
		   'c.n_occs', 'ei.interval_name as early_interval', 'li.interval_name as late_interval',
		   'c.reference_no', 'group_concat(distinct sr.reference_no) as reference_nos'], 
	tables => ['cc', 'ei', 'li', 'sr'] },
      { output => 'collection_no', dwc_name => 'collectionID', com_name => 'oid' },
	  "A unique identifier for the collection.  For now, these are positive integers,",
	  "but this might change and should B<not be relied on>.",
      { output => 'record_type', value => 'collection', com_name => 'typ', com_value => 'col', 
	dwc_value => 'Occurrence' },
	  "type of this object: 'col' for a collection",
      { output => 'formation', com_name => 'sfm', not_block => 'strat' },
	  "The formation in which the collection was found",
      { output => 'lng', dwc_name => 'decimalLongitude', com_name => 'lng', data_type => 'dec' },
	  "The longitude at which the collection is located (in degrees)",
      { output => 'lat', dwc_name => 'decimalLatitude', com_name => 'lat', data_type => 'dec' },
	  "The latitude at which the collection is located (in degrees)",
      { set => 'llp', from => '*', code => \&generateBasisCode },
      { output => 'llp', com_name => 'prc' },
	  "A two-letter code indicating the basis and precision of the geographic coordinates.",
      { output => 'collection_name', dwc_name => 'collectionCode', com_name => 'nam' },
	  "An arbitrary name which identifies the collection, not necessarily unique",
      { output => 'collection_subset', com_name => 'nm2' },
	  "If the collection is a part of another one, this field specifies which part",
      { output => 'attribution', dwc_name => 'recordedBy', com_name => 'att', if_block => 'attr' },
	  "The attribution (author and year) of the collection",
      { output => 'pubyr', com_name => 'pby', if_block => 'attr', data_type => 'pos' },
	  "The year in which the collection was published",
      { output => 'n_occs', com_name => 'noc', data_type => 'pos' },
	  "The number of occurrences in the collection",
      { output => 'early_interval', com_name => 'oei', pbdb_name => 'early_interval' },
	  "The specific geologic time range associated with the collection (not necessarily a",
	  "standard interval), or the interval that begins the range if C<late_interval> is also given",
      { output => 'late_interval', com_name => 'oli', pbdb_name => 'late_interval', dedup => 'early_interval' },
	  "The interval that ends the specific geologic time range associated with the collection",
      { set => 'reference_no', from => '*', code => \&set_collection_refs },
      { output => 'reference_no', com_name => 'rid', text_join => ', ' },
	  "The identifier(s) of the references from which this data was entered.  For",
	  "now these are positive integers, but this could change and should B<not be relied on>.");
    
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
        { set => 'attribution', from => '*', code => \&generateAttribution },
        { set => 'pubyr', from => 'a_pubyr' });
    
    $ds->define_block('1.1:colls:ref' =>
      { select => ['r.author1init as r_ai1', 'r.author1last as r_al1', 'r.author2init as r_ai2', 
		   'r.author2last as r_al2', 'r.otherauthors as r_oa', 'r.pubyr as r_pubyr', 
		   'r.reftitle as r_reftitle', 'r.pubtitle as r_pubtitle', 
		   'r.editors as r_editors', 'r.pubvol as r_pubvol', 'r.pubno as r_pubno', 
		   'r.firstpage as r_fp', 'r.lastpage as r_lp', 'r.publication_type as r_pubtype', 
		   'r.language as r_language', 'r.doi as r_doi'],
	tables => ['r'] },
      { set => 'ref_list', from => '*', code => \&generateReference },
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
    
    $ds->define_block('1.1:colls:paleoloc' =>
	{ select => 'PALEOCOORDS' },
	{ output => 'paleomodel', com_name => 'pm1' },
	    "The primary model specified by the parameter C<pgm>.  This",
	    "field will only be included if more than one model is indicated.",
	{ output => 'paleolng', com_name => 'pln', data_type => 'dec' },
	    "The paleolongitude of the collection, evaluated according to the",
	    "primary model indicated by the parameter C<pgm>.",
	{ output => 'paleolat', com_name => 'pla', data_type => 'dec' },
	    "The paleolatitude of the collection, evaluated according to the",
	    "primary model indicated by the parameter C<pgm>.",
	{ output => 'geoplate', com_name => 'gpl' },
	    "The identifier of the geological plate on which the collection lies,",
	    "evaluated according to the primary model indicated by the parameter C<pgm>.",
	    "This might be either a number or a string.",
	{ output => 'paleomodel2', com_name => 'pm2' },
	    "An alternate model specified by the parameter C<pgm>.  This",
	    "field will only be included if more than one model is indicated.",
	    "There may also be C<paleomodel3>, etc.",
	{ output => 'paleolng2', com_name => 'pn2', data_type => 'dec' },
	    "An alternate paleolongitude for the collection, if the C<pgm> parameter",
	    "indicates more than one model.  There may also be C<paleolng3>, etc.",
	{ output => 'paleolat2', com_name => 'pa2', data_type => 'dec' },
	    "An alternate paleolatitude for the collection, if the C<pgm> parameter",
	    "indicates more than one model.  There may also be C<paleolat3>, etc.",
	{ output => 'geoplate2', com_name => 'gp2' },
	    "An alternate geological plate identifier, if the C<pgm> parameter",
	    "indicates more than one model.  There may also be C<geoplate3>, etc.",
	{ output => 'paleomodel3', com_name => 'pm3' }, "! these do not need to be documented separately...",
	{ output => 'paleolng3', com_name => 'pn3' }, "!",
	{ output => 'paleolat3', com_name => 'pa3' }, "!",
	{ output => 'paleomodel4', com_name => 'pm4' }, "!",
	{ output => 'paleolng4', com_name => 'pn4' }, "!",
	{ output => 'paleolat4', com_name => 'pa4' }, "!");
    
#	    "L<list|ftp://ftp.earthbyte.org/earthbyte/GPlates/SampleData/FeatureCollections/Rotations/Global_EarthByte_PlateIDs_20071218.pdf>",
#	    "established by the L<Earthbyte Group|http://www.earthbyte.org/>.");
    
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
      { output => 'early_age', com_name => 'eag', data_type => 'dec' },
	  "The early bound of the geologic time range associated with the collection or cluster (in Ma)",
      { output => 'late_age', com_name => 'lag', data_type => 'dec' },
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
	  "A unique identifier for the taxon.  These are currently positive integers,",
	  "but this could change and should B<not be relied on>.",
      { output => 'ident_name', com_name => 'ina', dedup => 'taxon_name' },
	  "The name under which the occurrence was actually identified",
      { output => 'ident_rank', com_name => 'irn', dedup => 'taxon_rank' },
	  "The taxonomic rank as actually identified",
      { output => 'ident_no', com_name => 'iid', dedup => 'taxon_no' },
	  "A unique identifier for the taxonomic name.");

    $ds->define_block( '1.1:colls:taxa' =>
      { output => 'taxa', com_name => 'tax', sub_record => 'taxon_record' },
	  "A list of records describing the taxa that have been identified",
	  "as appearing in the collection");
    
    $ds->define_block( '1.1:colls:rem' =>
      { set => 'collection_aka', join => '; ', if_format => 'txt,tsv,csv,xml' },
      { output => 'collection_aka', dwc_name => 'collectionRemarks', com_name => 'crm' },
	  "Any additional remarks that were entered about the collection");
    
    # Then define an output block for displaying stratigraphic results
    
    $ds->define_block('1.1:colls:strata' =>
	{ select => ['count(*) as n_colls', 'sum(n_occs) as n_occs'] },
	{ output => 'record_type', com_name => 'typ', value => 'stratum', com_value => 'str' },
	    "The type of this record: 'str' for a stratum",
	{ output => 'name', com_name => 'nam' },
	    "The name of the stratum",
	{ output => 'rank', com_name => 'rnk' },
	    "The rank of the stratum: formation, group or member",
	{ output => 'n_colls', com_name => 'nco', data_type => 'pos' },
	    "The number of fossil collections in the database that are associated with this stratum.",
	    "Note that if your search is limited to a particular geographic area, then",
	    "only collections within the selected area are counted.",
	{ output => 'n_occs', com_name => 'noc', data_type => 'pos' },
	    "The number of fossil occurrences in the database that are associated with this stratum.",
	    "The above note about geographic area selection also applies.");
    
    # And a block for basic geographic summary cluster info
    
    $ds->define_block( '1.1:colls:summary' =>
      { select => ['s.bin_id', 's.n_colls', 's.n_occs', 's.lat', 's.lng'] },
      { output => 'bin_id', com_name => 'oid' }, 
	  "A unique identifier for the cluster.  For now, these are positive",
	  "integers, but this might change and should B<not be relied on>.",
      { output => 'bin_id_1', com_name => 'lv1' }, 
	  "The identifier of the containing level-1 cluster, if any",
      { output => 'bin_id_2', com_name => 'lv2' }, 
	  "The identifier of the containing level-2 cluster, if any",
      { output => 'bin_id_3', com_name => 'lv3' },
	  "The identifier of the containing level-3 cluster, if any",
      { output => 'bin_id_4', com_name => 'lv4' },
	  "The identifier of the containing level-4 cluster, if any",
      { output => 'record_type', com_name => 'typ', value => 'clu' },
	  "The type of this object: 'clu' for a collection cluster",
      { output => 'n_colls', com_name => 'nco', data_type => 'pos' },
	  "The number of collections in this cluster",
      { output => 'n_occs', com_name => 'noc', data_type => 'pos' },
	  "The number of occurrences in this cluster",
      { output => 'lng', com_name => 'lng', data_type => 'dec' },
	  "The longitude of the centroid of this cluster",
      { output => 'lat', com_name => 'lat', data_type => 'dec' },
	  "The latitude of the centroid of this cluster");
    
    # Plus one for summary cluster extent
    
    $ds->define_block( '1.1:colls:ext' =>
      { select => ['s.lng_min', 'lng_max', 's.lat_min', 's.lat_max', 's.std_dev'] },
      { output => 'lng_min', com_name => 'lg1', data_type => 'dec' },
	  "The mimimum longitude for collections in this cluster",
      { output => 'lng_max', com_name => 'lg2', data_type => 'dec' },
	  "The maximum longitude for collections in this cluster",
      { output => 'lat_min', com_name => 'la1', data_type => 'dec' },
	  "The mimimum latitude for collections in this cluster",
      { output => 'lat_max', com_name => 'la2', data_type => 'dec' },
	  "The maximum latitude for collections in this cluster",
      { output => 'std_dev', com_name => 'std', data_type => 'dec' },
	  "The standard deviation of the coordinates in this cluster");
    
    # Finally, define rulesets to interpret the parmeters used with operations
    # defined by this class.
    
    $ds->define_set('1.1:colls:order' =>
	{ value => 'earlyage' },
	    "Results are ordered chronologically by early age bound, oldest to youngest unless you add C<.asc>",
	{ value => 'earlyage.asc', undocumented => 1 },
	{ value => 'earlyage.desc', undocumented => 1 },
	{ value => 'lateage' },
	    "Results are ordered chronologically by late age bound, oldest to youngest unless you add C<.asc>",
	{ value => 'lateage.asc', undocumented => 1 },
	{ value => 'lateage.desc', undocumented => 1 },
	{ value => 'agespread' },
	    "Results are ordered based on the difference between the early and late age bounds, starting",
	    "with occurrences with the largest spread (least precise temporal resolution) unless you add C<.asc>",
	{ value => 'agespread.asc', undocumented => 1 },
	{ value => 'agespread.desc', undocumented => 1 },
	{ value => 'formation' },
	    "Results are ordered by the stratigraphic formation in which they were found, sorted alphabetically.",
	{ value => 'formation.asc', undocumented => 1 },
	{ value => 'formation.desc', undocumented => 1 },
	{ value => 'stratgroup' },
	    "Results are ordered by the stratigraphic group in which they were found, sorted alphabetically.",
	{ value => 'stratgroup.asc', undocumented => 1 },
	{ value => 'stratgroup.desc', undocumented => 1 },
	{ value => 'member' },
	    "Results are ordered by the stratigraphic member in which they were found, sorted alphabetically.",
	{ value => 'member.asc', undocumented => 1 },
	{ value => 'member.desc', undocumented => 1 },
	{ value => 'plate' },
	    "Results are ordered by the geological plate on which they are located, sorted numerically by identifier.",
	{ value => 'plate.asc', undocumented => 1 },
	{ value => 'plate.desc', undocumented => 1 },
	{ value => 'created' },
	    "Results are ordered by the date the record was created, most recent first",
	    "unless you add C<.asc>.",
	{ value => 'created.asc', undocumented => 1 },
	{ value => 'created.desc', undocumented => 1 },
	{ value => 'modified' },
	    "Results are ordered by the date the record was last modified",
	    "most recent first unless you add C<.asc>",
	{ value => 'modified.asc', undocumented => 1 },
	{ value => 'modified.desc', undocumented => 1 });
    
    $ds->define_set('1.1:colls:ident_select' =>
	{ value => 'latest' },
	    "Return the most recently published identification of each selected occurrence,",
	    "as long as it matches the other specified criteria.",
	{ value => 'orig' },
	    "Return the originally published identification of each selected occurence,",
	    "as long as it matches the other specified criteria.",
	{ value => 'all' },
	    "Return all matching identifications of each selected occurrence, each as a separate record");
    
    $ds->define_set('1.1:colls:pgmodel' =>
	{ value => 'scotese' },
	    "Use the paleogeographic model defined by L<C. R. Scotese|http://scotese.com/> (2002), which is the",
	    "one that this database has been using historically.",
	{ value => 'gplates' },
	    "Use the paleogeographic model defined by the GPlates software from the",
	    "L<EarthByte group|http://www.earthbyte.org/>.  By default, the coordinates",
	    "for each collection are calculated at the midpoint of its age range.",
	{ value => 'gp_early' },
	    "Use the GPlates model, calculating the rotations at the early end of each",
	    "collection's age range",
	{ value => 'gp_mid' },
	    "A synonym for the value C<gplates>.",
	{ value => 'gp_late' },
	    "Use the GPlates model, calculating the rotations at the late end of each",
	    "collection's age range");
    
    $ds->define_ruleset('1.1:main_selector' =>
	{ param => 'clust_id', valid => POS_VALUE, list => ',' },
	    "Return only records associated with the specified geographic clusters.",
	    "You may specify one or more cluster ids, separated by commas.",
	{ param => 'taxon_name', valid => \&PB1::TaxonData::validNameSpec },
	    "Return only records associated with the specified taxonomic name(s).  You may specify multiple names, separated by commas.",
	{ param => 'taxon_id', valid => POS_VALUE, list => ','},
	    "Return only records associated with the specified taxonomic name(s), specified by numeric identifier.",
	    "You may specify multiple identifiers, separated by commas.",
	{ param => 'taxon_actual', valid => FLAG_VALUE },
	    "If this parameter is specified, then only records that were actually identified with the",
	    "specified taxonomic name and not those which match due to synonymy",
	    "or other correspondences between taxa.  This is a flag parameter, which does not need any value.",
	{ param => 'base_name', valid => \&PB1::TaxonData::validNameSpec, list => ',' },
	    "Return only records associated with the specified taxonomic name(s), I<including subtaxa>.",
	    "You may specify multiple names, separated by commas.",
	{ param => 'base_id', valid => POS_VALUE, list => ',' },
	    "Return only records associated with the specified taxonomic name(s), specified by numeric identifier, I<including subtaxa>.",
	    "You may specify multiple identifiers, separated by commas.",
	    "Note that you may specify at most one of 'taxon_name', 'taxon_id', 'base_name', 'base_id'.",
	{ at_most_one => ['taxon_name', 'taxon_id', 'base_name', 'base_id'] },
	{ param => 'exclude_id', valid => POS_VALUE, list => ','},
	    "Exclude any records whose associated taxonomic name is a child of the given name or names, specified by numeric identifier.",
	{ param => 'ident', valid => $ds->valid_set('1.1:colls:ident_select'), default => 'latest' },
	    "If more than one taxonomic identification is recorded for some or all of the selected occurrences,",
	    "this parameter specifies which are to be returned.  Values include:",
	    $ds->document_set('1.1:colls:ident_select'),
	{ param => 'lngmin', valid => DECI_VALUE },
	{ param => 'lngmax', valid => DECI_VALUE },
	    "Return only records whose present longitude falls within the given bounds.",
	    "If you specify one of these parameters then you must specify both.",
	    "If you provide bounds outside of the range -180\N{U+00B0} to 180\N{U+00B0}, they will be",
	    "wrapped into the proper range.  For example, if you specify C<lngmin=270 & lngmax=360>,",
	    "the query will be processed as if you had said C<lngmin=-90 & lngmax=0 >.  In this",
	    "case, all longitude values in the query result will be adjusted to fall within the actual",
	    "numeric range you specified.",
	{ param => 'latmin', valid => DECI_VALUE },
	    "Return only records whose present latitude is at least the given value.",
	{ param => 'latmax', valid => DECI_VALUE },
	    "Return only records whose present latitude is at most the given value.",
	{ together => ['lngmin', 'lngmax'],
	  error => "you must specify both of 'lngmin' and 'lngmax' if you specify either of them" },
	{ param => 'loc', valid => ANY_VALUE },		# This should be a geometry in WKT format
	    "Return only records whose present location (longitude and latitude) falls within",
	    "the specified shape, which must be given in L<WKT|https://en.wikipedia.org/wiki/Well-known_text> format",
	    "with the coordinates being longitude and latitude values.",
	{ param => 'plate', valid => POS_VALUE, list => "," },
	    "Return only records located on the specified geological plate(s), according",
	    "to the primary paleogeographic model specified by the parameter C<pgm>.  The value of",
	    "this parameter may be a comma-separated list of numeric plate identifiers.",
	{ optional => 'pgm', valid => $ds->valid_set('1.1:colls:pgmodel'), list => "," },
	    "Specify which paleogeographic model(s) to use when evaluating paleocoordinates.",
	    "You may specify one or more from the following list, separated by commas.",
	    "If you do not specify a value for this parameter, the default model is C<gplates>.",
	    $ds->document_set('1.1:colls:pgmodel'),
	{ param => 'country', valid => \&valid_country, list => ',', alias => 'cc', bad_value => '_' },
	    "Return only records whose geographic location falls within the specified country or countries.",
	    "The value of this parameter should be one or more",
	    "L<two-character country codes|http://en.wikipedia.org/wiki/ISO_3166-1_alpha-2> as a comma-separated list.",
	{ param => 'continent', valid => \&valid_continent, list => ',', bad_value => '_' },
	    "Return only records whose geographic location falls within the specified continent or continents.",
	    "The value of this parameter should be a comma-separated list of ",
	    "L<continent codes|op:config.txt?show=continents>.",
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
	    "=item overlap", "Return only records whose temporal locality overlaps the specified time range by any amount, no matter how small.",
	    "=item buffer", "Return only records whose temporal locality overlaps the specified range and is contained",
	    "within the specified time range plus a buffer on either side.  If an interval from one of the timescales known to the database is",
	    "given, then the default buffer will be the intervals immediately preceding and following at the same level.",
	    "Otherwise, the buffer will default to 10 million years on either side.  This can be overridden using the parameters",
	    "C<earlybuffer> and C<latebuffer>.  This is the default value for this option.",
	{ optional => 'earlybuffer', valid => POS_VALUE },
	    "Override the default buffer period for the beginning of the time range when resolving temporal locality.",
	    "The value must be given in millions of years.  This option not relevant if C<timerule> is set to either C<contain> or C<overlap>.",
	{ optional => 'latebuffer', valid => POS_VALUE },
	    "Override the default buffer period for the end of the time range when resolving temporal locality.",
	    "The value must be given in millions of years.  This option not relevant if C<timerule> is set to either C<contain> or C<overlap>.");
    
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
    	{ allow => '1.1:special_params' },
        "^You can also use any of the L<special parameters|node:special> with this request");

    $ds->define_ruleset('1.1:colls:list' => 
	"You can use the following parameter if you wish to retrieve information about",
	"a known list of collections, or to filter a known list against other criteria such as location or time.",
	"Only the records which match the other parameters that you specify will be returned.",
    	{ allow => '1.1:colls:selector' },
        ">>The following parameters can be used to query for collections by a variety of criteria.",
	"Except as noted below, you may use these in any combination.",
	"These parameters can all be used to select either occurrences, collections, or associated references.",
   	{ allow => '1.1:main_selector' },
	{ allow => '1.1:common:select_crmod' },
	{ allow => '1.1:common:select_ent' },
	{ require_any => ['1.1:colls:selector', '1.1:main_selector',
			  '1.1:common:select_crmod', '1.1:common:select_ent'] },
	">>You can also specify any of the following parameters:",
    	{ allow => '1.1:colls:display' },
    	{ allow => '1.1:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.1:colls:refs' =>
	"You can use the following parameters if you wish to retrieve the references associated",
	"with a known list of collections, or to filter a known list against",
	"other criteria such as location or time.",
	"Only the records which match the other parameters that you specify will be returned.",
	{ allow => '1.1:colls:selector' },
        ">>The following parameters can be used to retrieve the references associated with occurrences",
	"selected by a variety of criteria.  Except as noted below, you may use these in any combination.",
	"These parameters can all be used to select either occurrences, collections, or associated references.",
	{ allow => '1.1:main_selector' },
	{ allow => '1.1:common:select_crmod' },
	{ allow => '1.1:common:select_ent' },
	{ require_any => ['1.1:colls:selector', '1.1:main_selector',
			  '1.1:common:select_crmod', '1.1:common:select_ent'] },
	">>You can also specify any of the following parameters:",
	{ allow => '1.1:refs:filter' },
	{ allow => '1.1:refs:display' },
	{ allow => '1.1:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.1:toprank_selector' =>
	{ param => 'show', valid => ENUM_VALUE('formation', 'ref', 'author'), list => ',' });
    
    $ds->define_ruleset('1.1:colls:toprank' => 
    	{ require => '1.1:main_selector' },
    	{ require => '1.1:toprank_selector' },
    	{ allow => '1.1:special_params' });
    
    $ds->define_ruleset('1.1:strata:selector' =>
	{ param => 'name', valid => ANY_VALUE },
	    "A full or partial name.  You can use % and _ as wildcards, but the query",
	    "will be very slow if you put a wildcard at the beginning",
	{ optional => 'rank', valid => ENUM_VALUE('formation','group','member') },
	    "Return only strata of the specified rank: formation, group or member",
	{ param => 'lngmin', valid => DECI_VALUE },
	{ param => 'lngmax', valid => DECI_VALUE },
	{ param => 'latmin', valid => DECI_VALUE },
	{ param => 'latmax', valid => DECI_VALUE },
	    "Return only strata associated with some occurrence whose geographic location falls within the given bounding box.",
	    "The longitude boundaries will be normalized to fall between -180 and 180, and will generate",
	    "two adjacent bounding boxes if the range crosses the antimeridian.",
	    "Note that if you specify C<lngmin> then you must also specify C<lngmax>.",
	{ together => ['lngmin', 'lngmax'],
	  error => "you must specify both of 'lngmin' and 'lngmax' if you specify either of them" },
	{ param => 'loc', valid => ANY_VALUE },		# This should be a geometry in WKT format
	    "Return only strata associated with some occurrence whose geographic location falls",
	    "within the specified geometry, specified in WKT format.");
    
    $ds->define_ruleset('1.1:strata:list' =>
	{ require => '1.1:strata:selector' },
	{ allow => '1.1:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.1:strata:auto' =>
	{ require => '1.1:strata:selector' },
	{ allow => '1.1:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.1:summary_display' =>
	"You can use the following parameter to request additional information about each",
	"retrieved cluster:",
	{ param => 'show', list => q{,},
	  valid => $ds->valid_set('1.1:colls:summary_map') },
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each cluster.  Its value should be",
	    "one or more of the following, separated by commas:",
	    $ds->document_set('1.1:colls:summary_map'),);
    
    $ds->define_ruleset('1.1:colls:summary' => 
	"The following required parameter selects from one of the available clustering levels:",
	{ param => 'level', valid => POS_VALUE, default => 1 },
	    "Return records from the specified cluster level.  You can find out which",
	    "levels are available by means of the L<config|node:config> URL path.",
	">>You can use the following parameters to query for summary clusters by",
	"a variety of criteria.  Except as noted below, you may use these in any combination.",
    	{ allow => '1.1:main_selector' },
	">>You can use the following parameter if you wish to retrieve information about",
	"the summary clusters which contain a specified collection or collections.",
	"Only the records which match the other parameters that you specify will be returned.",
    	{ allow => '1.1:colls:selector' },
	{ allow => '1.1:common:select_crmod' },
	{ allow => '1.1:common:select_ent' },
    	{ allow => '1.1:summary_display' },
    	{ allow => '1.1:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
}


# get ( )
# 
# Query for all relevant information about the collection specified by the
# 'id' parameter.  Returns true if the query succeeded, false otherwise.

sub get {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $self->clean_param('id');
    
    die "Bad identifier '$id'" unless defined $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $self->substitute_select( mt => 'c', cd => 'cc' );
    
    my $fields = $self->select_string;
    
    $self->adjustCoordinates(\$fields);
    $self->selectPaleoModel(\$fields, $self->tables_hash) if $fields =~ /PALEOCOORDS/;
    
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
    
    if ( $self->has_block('taxa') )
    {
	my $taxonomy = TaxonomyOld->new($dbh, 'taxon_trees');
	
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
    
    my $dbh = $self->get_connection;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my $tables = $self->tables_hash;
    
    my @filters = $self->generateMainFilters('list', 'c', $tables);
    push @filters, $self->generateCollFilters($tables);
    push @filters, $self->generate_crmod_filters('cc', $tables);
    push @filters, $self->generate_ent_filters('cc', $tables);
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $self->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $self->substitute_select( mt => 'c', cd => 'cc' );
    
    my $fields = $self->select_string;
    
    $self->adjustCoordinates(\$fields);
    $self->selectPaleoModel(\$fields, $self->tables_hash) if $fields =~ /PALEOCOORDS/;
    
    if ( $tables->{tf} )
    {
	$fields =~ s{ c.n_occs }{count(distinct o.occurrence_no) as n_occs}xs;
    }
    
    # Determine the order in which the results should be returned.
    
    my $order_clause = $self->generate_order_clause($tables, { at => 'c', cd => 'cc' }) || 'c.collection_no';
    
    # Determine if any extra tables need to be joined in.
    
    my $base_joins = $self->generateJoinList('c', $self->tables_hash);
    
    $self->{main_sql} = "
	SELECT $calc $fields
	FROM coll_matrix as c JOIN collections as cc using (collection_no)
		LEFT JOIN secondary_refs as sr using (collection_no)
		$base_joins
        WHERE $filter_string
	GROUP BY c.collection_no
	ORDER BY $order_clause
	$limit";
    
    print STDERR "$self->{main_sql}\n\n" if $self->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $self->sql_count_rows;
}


# summary ( )
# 
# This operation queries for geographic summary clusters matching the
# specified parameters.

sub summary {
    
    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_connection;
    my $tables = $self->tables_hash;
    
    # Figure out which bin level we are being asked for.  The default is 1.    

    my $bin_level = $self->clean_param('level') || 1;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = $self->generateMainFilters('summary', 's', $tables);
    push @filters, $self->generateCollFilters($tables);
    push @filters, $self->generate_crmod_filters('cc', $tables);
    push @filters, $self->generate_ent_filters('cc', $tables);
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $self->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $self->substitute_select( mt => 's' );
    
    my $fields = $self->select_string;
    
    $self->adjustCoordinates(\$fields);
    
    if ( $tables->{tf} )
    {
	$fields =~ s{ s[.]n_colls }{count(distinct c.collection_no) as n_colls}xs;
	$fields =~ s{ s[.]n_occs }{count(distinct o.occurrence_no) as n_occs}xs;
    }
    
    my $summary_joins = '';
    
    $summary_joins .= "JOIN $COLL_MATRIX as c on s.bin_id = c.bin_id_${bin_level}\n"
	if $tables->{c} || $tables->{cc} || $tables->{t} || $tables->{o} || $tables->{oc} || $tables->{tf};
    
    $summary_joins .= "JOIN collections as cc using (collection_no)\n" if $tables->{cc};
    
    $summary_joins .= $self->generateJoinList('s', $tables);
    
    # if ( $self->{select_tables}{o} )
    # {
    # 	$fields =~ s/s.n_colls/count(distinct c.collection_no) as n_colls/;
    # 	$fields =~ s/s.n_occs/count(distinct o.occurrence_no) as n_occs/;
    # }
    
    # elsif ( $self->{select_tables}{c} )
    # {
    # 	$fields =~ s/s.n_colls/count(distinct c.collection_no) as n_colls/;
    # 	$fields =~ s/s.n_occs/sum(c.n_occs) as n_occs/;
    # }
    
    push @filters, "s.access_level = 0", "s.bin_level = $bin_level";
    
    my $filter_string = join(' and ', @filters);
    
    $self->{main_sql} = "
		SELECT $calc $fields
		FROM $COLL_BINS as s $summary_joins
		WHERE $filter_string
		GROUP BY s.bin_id
		ORDER BY s.bin_id $limit";
    
    # Then prepare and execute the query..
    
    print STDERR $self->{main_sql} . "\n\n" if $self->debug;
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # Get the result count, if we were asked to do so.
    
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
    
    my $dbh = $self->get_connection;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my $inner_tables = {};
    
    my @filters = $self->generateMainFilters('list', 'c', $inner_tables);
    push @filters, $self->generateCollFilters($inner_tables);
    push @filters, $self->generate_crmod_filters('cc', $inner_tables);
    push @filters, $self->generate_ent_filters('cc', $inner_tables);
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # Construct another set of filter expressions to act on the references.
    
    my @ref_filters = $self->PB1::ReferenceData::generate_filters($self->tables_hash);
    push @ref_filters, "1=1" unless @ref_filters;
    
    my $ref_filter_string = join(' and ', @ref_filters);
    
    # Figure out the order in which we should return the references.  If none
    # is selected by the options, sort by rank descending.
    
    my $order = PB1::ReferenceData::generate_order_clause($self, { rank_table => 's' }) ||
	"r.author1last, r.author1init";
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $self->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $self->substitute_select( mt => 'r', cd => 'r' );
    
    my $fields = $self->select_string;
    
    $self->adjustCoordinates(\$fields);
    
    my $inner_join_list = $self->generateJoinList('c', $inner_tables);
    my $outer_join_list = $self->PB1::ReferenceData::generate_join_list($self->tables_hash);
    
    $self->{main_sql} = "
	SELECT $calc $fields, s.reference_rank, is_primary, 1 as is_coll
	FROM (SELECT sr.reference_no, count(*) as reference_rank, if(sr.reference_no = c.reference_no, 1, 0) as is_primary
	    FROM $COLL_MATRIX as c JOIN collections as cc on cc.collection_no = c.collection_no
		LEFT JOIN secondary_refs as sr on c.collection_no = sr.collection_no
		$inner_join_list
            WHERE $filter_string
	    GROUP BY sr.reference_no) as s STRAIGHT_JOIN refs as r on r.reference_no = s.reference_no
	$outer_join_list
	WHERE $ref_filter_string
	ORDER BY $order
	$limit";
    
    print STDERR "$self->{main_sql}\n\n" if $self->debug;
    
    # Then prepare and execute the main query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $self->sql_count_rows;
}


# strata ( arg )
# 
# Query the database for geological strata.  If the arg is 'auto', then treat
# this query as an auto-completion request.

sub strata {
    
    my ($self, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_connection;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    $self->substitute_select( mt => 'cs' );
    
    my $tables = $self->tables_hash;
    
    my @filters = $self->generateMainFilters('list', 'cs', $tables);
    push @filters, "1=1" unless @filters;
    
    my @outer_filters = $self->generateStrataFilters($tables, $arg);
    push @outer_filters, "1=1" unless @outer_filters;
    
    my $inner_filters = join(' and ', @filters);
    my $outer_filters = join(' and ', @outer_filters);
    
    # Modify the query according to the common parameters.
    
    my $limit = $self->sql_limit_clause(1);
    my $calc = $self->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->select_string;
    
    #$self->adjustCoordinates(\$fields);
    
    # Determine if any extra tables need to be joined in.
    
    my $base_joins = $self->generateJoinList('cs', $tables);
    
    $self->{main_sql} = "
	SELECT * FROM
	((SELECT $calc $fields, cs.grp as name, 'group' as rank
	FROM coll_strata as cs
		$base_joins
        WHERE $inner_filters and cs.grp <> ''
	GROUP BY cs.grp)
	UNION
	(SELECT $calc $fields, cs.formation as name, 'formation' as rank
	FROM coll_strata as cs
		$base_joins
        WHERE $inner_filters and cs.formation <> ''
	GROUP BY cs.formation)
	UNION
	(SELECT $calc $fields, cs.member as name, 'member' as rank
	FROM coll_strata as cs
		$base_joins
        WHERE $inner_filters and cs.member <> ''
	GROUP BY cs.member)) as strata
	WHERE $outer_filters
	ORDER BY name
	$limit";
    
    print STDERR "$self->{main_sql}\n\n" if $self->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
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
    
    my $dbh = $self->get_connection;
    my @filters;
    
    # Check for parameter 'id', If the parameter was given but no value was
    # found, then add a clause that will generate an empty result set.
    
    my $id = $self->clean_param('id');
    
    if ( ref $id eq 'ARRAY' and @$id )
    {
	my $id_list = join(',', @$id);
	push @filters, "c.collection_no in ($id_list)";
    }
    
    elsif ( defined $id && $id ne '' )
    {
	push @filters, "c.collection_no = $id";
    }
    
    # If our tables include the occurrence matrix, we must check the 'ident'
    # parameter. 
    
    if ( $tables_ref->{o} || $tables_ref->{tf} || $tables_ref->{t} || $tables_ref->{oc} )
    {
	my $ident = $self->clean_param('ident');
	
	if ( $ident eq 'orig' )
	{
	    push @filters, "o.reid_no = 0";
	}
	
	elsif ( $ident eq 'all' )
	{
	    # we need do nothing in this case
	}
	
	else # default: 'latest'
	{
	    push @filters, "o.latest_ident = true";
	}
    }
    
    return @filters;
}


# generateStrataFilters ( tables_ref, $is_auto )
# 
# Generate a list of filter clauses that will help to select the appropriate
# set of records.  This routine only handles parameters that are specific to
# strata.  If $is_auto is 'auto', then add a % wildcard to the end of the name.

sub generateStrataFilters {

    my ($self, $tables_ref, $is_auto) = @_;
    
    my $dbh = $self->get_connection;
    my @filters;
    
    # Check for parameter 'name'.
    
    if ( my $name = $self->clean_param('name') )
    {
	$name .= '%' if defined $is_auto && $is_auto eq 'auto';
	my $quoted = $dbh->quote($name);
	push @filters, "name like $quoted";
    }
    
    # Check for parameter 'rank'.
    
    if ( my $rank = $self->clean_param('rank') )
    {
	my $quoted = $dbh->quote($rank);
	push @filters, "rank = $quoted";
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
    
    my $dbh = $self->get_connection;
    my $taxonomy = TaxonomyOld->new($dbh, 'taxon_trees');
    my @filters;
    
    # Check for parameter 'clust_id'
    
    my $clust_id = $self->clean_param('clust_id');
    
    if ( ref $clust_id eq 'ARRAY' )
    {
	# If there aren't any bins, include a filter that will return no
	# results. 
	
	if ( $MAX_BIN_LEVEL == 0 )
	{
	    push @filters, "c.collection_no = 0";
	}
	
	elsif ( $op eq 'summary' )
	{
	    my @clusters = grep { $_ > 0 } @$clust_id;
	    my $list = join(q{,}, @clusters);
	    push @filters, "s.bin_id in ($list)";
	}
	
	else
	{
	    my %clusters;
	    my @clust_filters;
	    
	    foreach my $cl (@$clust_id)
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
    
    my $taxon_name = $self->clean_param('taxon_name') || $self->clean_param('base_name');
    my $taxon_no = $self->clean_param('taxon_id') || $self->clean_param('base_id');
    my $exclude_no = $self->clean_param('exclude_id');
    my (@taxa, @exclude_taxa);
    
    # First get the relevant taxon records for all included taxa
    
    if ( $taxon_name )
    {
	#my (@starttime) = Time::HiRes::gettimeofday();
	@taxa = &PB1::TaxonData::get_taxa_by_name($self, $taxon_name, { return => 'range', common => 1 });
	#my (@endtime) = Time::HiRes::gettimeofday();
	#my $elapsed = Time::HiRes::tv_interval(\@starttime, \@endtime);
	#print STDERR $TaxonomyOld::SQL_STRING . "\n\n";
	#print STDERR "Name Query Elapsed: $elapsed\n\n";
    }
    
    elsif ( $taxon_no )
    {
	@taxa = $taxonomy->getTaxa('self', $taxon_no, { fields => 'lft', status => 'all' });
    }
    
    # Then get the records for excluded taxa.  But only if there are any
    # included taxa in the first place.
    
    if ( $exclude_no && $exclude_no ne 'undefined' )
    {
	@exclude_taxa = $taxonomy->getTaxa('self', $exclude_no, { fields => 'lft' });
    }
    
    # Then construct the necessary filters for included taxa
    
    if ( @taxa and ($self->clean_param('base_name') or $self->clean_param('base_id')) )
    {
	my $taxon_filters = join ' or ', map { "t.lft between $_->{lft} and $_->{rgt}" } @taxa;
	push @filters, "($taxon_filters)";
	$tables_ref->{tf} = 1;
	$tables_ref->{non_geo_filter} = 1;
    }
    
    elsif ( @taxa )
    {
	my $taxon_list = join ',', map { $_->{orig_no} } @taxa;
	push @filters, "o.orig_no in ($taxon_list)";
	$tables_ref->{o} = 1;
	$tables_ref->{non_geo_filter} = 1;
    }
    
    # If a name was given and no matching taxa were found, we need to query by
    # genus_name/species_name instead.
    
    elsif ( $taxon_name )
    {
	my @exact_genera;
	my @name_clauses;
	
	foreach my $name (ref $taxon_name eq 'ARRAY' ? @$taxon_name : $taxon_name)
	{
	    if ( $name =~ qr{ ^\s*([A-Za-z_.%]+)(?:\s+\(([A-Za-z_.%]+)\))?(?:\s+([A-Za-z_.%]+))?(?:\s+([A-Za-z_.%]+))? }xs )
	    {
		my @name_filters;
		
		my $main = $1;
		my $subgenus = $2 if defined $2;
		my $species = (defined $4 ? "$3 $4" : $3) if defined $3;
		
		$main =~ s/\./%/g;
		$subgenus =~ s/\./%/g if defined $subgenus;
		$species =~ s/\./%/g if defined $species;
		
		unless ( $subgenus || $species || $main =~ /[%_]/ )
		{
		    push @exact_genera, $dbh->quote($main);
		    next;
		}
		
		my $quoted_genus = $dbh->quote($main);
		push @name_filters, "o.genus_name like $quoted_genus";
		
		if ( $subgenus )
		{
		    my $quoted_subgenus = $dbh->quote($subgenus);
		    push @name_filters, "o.subgenus_name like $quoted_subgenus";
		}
		
		if ( $species )
		{
		    my $quoted_species = $dbh->quote($species);
		    push @name_filters, "o.species_name like $quoted_species";
		}
		
		if ( @name_filters > 1 )
		{
		    push @name_clauses, '(' . join(' and ', @name_filters) . ')';
		}
		
		elsif ( @name_filters )
		{
		    push @name_clauses, $name_filters[0];
		}
	    }
	}
	
	# All of the exact genus names can be combined into a single 'in' clause.
	
	if ( @exact_genera )
	{
	    my $list = join(',', @exact_genera);
	    push @name_clauses, "o.genus_name in ($list)";
	}
	
	# If we have more than one clause, add their disjunction to the filter
	# list.  We need to add table 'oc' to the join set, and we also set
	# the 'unknown_taxon' flag so that the code for generating the query
	# string will know to do any necessary reformatting.
	
	if ( @name_clauses > 1 )
	{
	    push @filters, '(' . join(' or ', @name_clauses) . ')';
	}
	
	# If we have a single clause, just add it to the filter list.
	
	elsif ( @name_clauses )
	{
	    push @filters, $name_clauses[0];
	}
	
	# If we did not find any valid names, then add a filter clause that
	# will guarantee an empty result set.
	
	else
	{
	    push @filters, "o.orig_no = -1";
	}
	
	$tables_ref->{o} = 1;
    }
    
    # If a number was given but it does not exist in the hierarchy, add a
    # filter that will guarantee no results.
    
    elsif ( $taxon_no )
    {
	push @filters, "o.orig_no = -1";
	$tables_ref->{o} = 1;
    }

    # ...and for excluded taxa 
    
    if ( @exclude_taxa and @taxa )
    {
	push @filters, map { "t.lft not between $_->{lft} and $_->{rgt}" } @exclude_taxa;
	$tables_ref->{tf} = 1;
    }
    
    # Check for parameters 'continent', 'country'
    
    if ( my @ccs = $self->clean_param_list('country') )
    {
	if ( $ccs[0] eq '_' )
	{
	    push @filters, "c.collection_no = 0";
	}
	else
	{
	    my $cc_list = "'" . join("','", @ccs) . "'";
	    push @filters, "c.cc in ($cc_list)";
	}
    }
    
    if ( my @continents = $self->clean_param_list('continent') )
    {
	if ( $continents[0] eq '_' )
	{
	    push @filters, "c.collection_no = 0";
	}
	else
	{
	    my $cont_list = "'" . join("','", @continents) . "'";
	    push @filters, "ccmap.continent in ($cont_list)";
	    $tables_ref->{ccmap} = 1;
	}
    }
    
    # Check for parameters 'lngmin', 'lngmax', 'latmin', 'latmax', 'loc',
    
    my $x1 = $self->clean_param('lngmin');
    my $x2 = $self->clean_param('lngmax');
    my $y1 = $self->clean_param('latmin');
    my $y2 = $self->clean_param('latmax');
    
    if ( $x1 ne '' && $x2 ne '' )
    {
	$y1 //= -90.0;
	$y2 //= 90.0;
	
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
	    my $polygon = "'POLYGON(($x1 $y1,$x2 $y1,$x2 $y2,$x1 $y2,$x1 $y1))'";
	    push @filters, "contains(geomfromtext($polygon), $mt.loc)";
	}
	
	# Otherwise, our bounding box crosses the antimeridian and so must be
	# split in two.  The latitude bounds must always be between -90 and
	# 90, regardless.
	
	else
	{
	    my $polygon = "'MULTIPOLYGON((($x1 $y1,180.0 $y1,180.0 $y2,$x1 $y2,$x1 $y1))," .
					"((-180.0 $y1,$x2 $y1,$x2 $y2,-180.0 $y2,-180.0 $y1)))'";
	    push @filters, "contains(geomfromtext($polygon), $mt.loc)";
	}
    }
    
    elsif ( $y1 ne '' || $y2 ne '' )
    {
	$y1 //= -90;
	$y2 //= 90;
	
	my $polygon = "'POLYGON((-180.0 $y1,180.0 $y1,180.0 $y2,-180.0 $y2,-180.0 $y1))'";
	push @filters, "contains(geomfromtext($polygon), $mt.loc)";
    }
    
    if ( my $loc = $self->clean_param('loc') )
    {
	push @filters, "contains(geomfromtext($loc), $mt.loc)";
    }
    
    # Check for parameter 'plate'
    
    if ( $self->clean_param('plate') )
    {
	my $plate_list = join(q{,}, $self->clean_param_list('plate'));
	my ($primary_model) = $self->clean_param_list('pgm');
	$primary_model //= 'gplates';
	
	if ( $plate_list && $primary_model eq 'scotese' )
	{
	    push @filters, "cc.plate in ($plate_list)";
	    $tables_ref->{cc} = 1;
	}
	
	elsif ( $plate_list )
	{
	    push @filters, "pc.plate_no in ($plate_list)";
	    $tables_ref->{pc} = 1;
	}
    }
    
    # Check for parameters 'p_lngmin', 'p_lngmax', 'p_latmin', 'p_latmax', 'p_loc',
    
    # my $px1 = $self->clean_param('lngmin');
    # my $px2 = $self->clean_param('lngmax');
    # my $py1 = $self->clean_param('latmin');
    # my $py2 = $self->clean_param('latmax');
    
    # if ( defined $px1 && defined $px2 )
    # {
    # 	$py1 //= -90.0;
    # 	$py2 //= 90.0;
	
    # 	# If the longitude coordinates do not fall between -180 and 180, adjust
    # 	# them so that they do.
	
    # 	if ( $px1 < -180.0 )
    # 	{
    # 	    $px1 = $px1 + ( floor( (180.0 - $px1) / 360.0) * 360.0);
    # 	}
	
    # 	if ( $px2 < -180.0 )
    # 	{
    # 	    $px2 = $px2 + ( floor( (180.0 - $px2) / 360.0) * 360.0);
    # 	}
	
    # 	if ( $px1 > 180.0 )
    # 	{
    # 	    $px1 = $px1 - ( floor( ($px1 + 180.0) / 360.0 ) * 360.0);
    # 	}
	
    # 	if ( $px2 > 180.0 )
    # 	{
    # 	    $px2 = $px2 - ( floor( ($px2 + 180.0) / 360.0 ) * 360.0);
    # 	}
	
    # 	# If $px1 < $px2, then we query on a single bounding box defined by
    # 	# those coordinates.
	
    # 	if ( $px1 < $px2 )
    # 	{
    # 	    my $polygon = "'POLYGON(($px1 $py1,$px2 $py1,$px2 $py2,$px1 $py2,$px1 $py1))'";
    # 	    push @filters, "contains(geomfromtext($polygon), pc.early_loc)";
    # 	}
	
    # 	# Otherwise, our bounding box crosses the antimeridian and so must be
    # 	# split in two.  The latitude bounds must always be between -90 and
    # 	# 90, regardless.
	
    # 	else
    # 	{
    # 	    my $polygon = "'MULTIPOLYGON((($px1 $py1,180.0 $py1,180.0 $py2,$px1 $py2,$px1 $py1))," .
    # 					"((-180.0 $py1,$px2 $py1,$px2 $py2,-180.0 $py2,-180.0 $py1)))'";
    # 	    push @filters, "contains(geomfromtext($polygon), $mt.loc)";
    # 	}
    # }
    
    # elsif ( defined $py1 || defined $py2 )
    # {
    # 	$py1 //= -90;
    # 	$py2 //= 90;
	
    # 	my $polygon = "'POLYGON((-180.0 $py1,180.0 $py1,180.0 $py2,-180.0 $py2,-180.0 $py1))'";
    # 	push @filters, "contains(geomfromtext($polygon), $mt.loc)";
    # }
    
    # if ( $self->{clean_params}{loc} )
    # {
    # 	push @filters, "contains(geomfromtext($self->{clean_params}{loc}), $mt.loc)";
    # }
    
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
    
    my $time_rule = $self->clean_param('timerule') || 'buffer';
    my $summary_interval = 0;
    my ($early_age, $late_age, $early_bound, $late_bound);
    my $interval_no = $self->clean_param('interval_id') + 0;
    my $interval_name = $self->clean_param('interval');
    my $earlybuffer = $self->clean_param('earlybuffer');
    my $latebuffer = $self->clean_param('latebuffer');
    
    # If an interval was specified, use that.
    
    if ( $interval_no || $interval_name )
    {
	my ($scale_no, $level);
	
	# First figure out the parameters of the specified interval
	
	if ( $interval_no )
	{
	    my $sql = "
		SELECT early_age, late_age, scale_no, scale_level, early_bound, late_bound
		FROM $INTERVAL_DATA LEFT JOIN $SCALE_MAP using (interval_no)
			LEFT JOIN $INTERVAL_BUFFER using (interval_no)
		WHERE interval_no = $interval_no ORDER BY scale_no LIMIT 1";
	    
	    ($early_age, $late_age, $scale_no, $level, $early_bound, $late_bound) = $dbh->selectrow_array($sql);
	    
	    # If the interval was not found, signal an error.
	    
	    die "400 Unknown interval id $interval_no\n" unless defined $early_age;
	}
	
	else
	{
	    my $quoted_name = $dbh->quote($interval_name);
	    
	    my $sql = "SELECT early_age, late_age, interval_no, scale_no, early_bound, late_bound
		   FROM $INTERVAL_DATA LEFT JOIN $SCALE_MAP using (interval_no)
			LEFT JOIN $INTERVAL_BUFFER using (interval_no)
		   WHERE interval_name like $quoted_name ORDER BY scale_no";
	
	    ($early_age, $late_age, $interval_no, $scale_no, $early_bound, $late_bound) = $dbh->selectrow_array($sql);
	    
	    # If the interval was not found, signal an error.
	    
	    die "400 Unknown interval '$interval_name'\n" unless defined $early_age;
	}
	
	# If no early and late bounds are found, generate them by default.
	
	unless ( defined $early_bound )
	{
	    $early_bound = $early_age + ( $early_age > 65 ? 12 : 5 );
	}
	
	unless ( defined $late_bound )
	{
	    $late_bound = $late_age - ( $late_age > 65 ? 12 : 5 );
	}
	
	# If the requestor wants to override the time bounds, do that.
	
	if ( $earlybuffer )
	{
	    $early_bound = $early_age + $earlybuffer;
	}
	
	if ( $latebuffer )
	{
	    $late_bound = $late_age - $latebuffer;
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
    
    else
    {
	my $max_ma = $self->clean_param('max_ma');
	my $min_ma = $self->clean_param('min_ma');
	
	if ( $max_ma && $min_ma )
	{
	    my $range = $max_ma - $min_ma;
	    my $buffer = $range * 0.5;
	    
	    $early_age = $max_ma + 0;
	    $early_bound = defined $earlybuffer ? 
		$early_age + $earlybuffer :
		    $early_age + $buffer;
	
	    $late_age = $max_ma + 0;
	    $late_bound = defined $latebuffer ?
		$late_age - $latebuffer :
		    $late_age - $buffer;
	
	    $late_bound = 0 if $late_bound < 0;
	}
	
	# Otherwise, handle either a min or max filter alone.
	
	elsif ( $max_ma )
	{
	    $early_age = $max_ma + 0;
	    $early_bound = $early_age;
	}
	
	if ( $max_ma )
	{
	    $late_age = $min_ma + 0;
	    $late_bound = $late_age;
	}
    }
    
    # Now, if we are summarizing then add the appropriate interval filter.  If
    # $summary_interval is not an integer (i.e. the client didn't specify a
    # valid interval), use -1 instead which will cause the result set to be empty.
    
    if ( $op eq 'summary' )
    {
	$summary_interval = '-1' unless $summary_interval =~ qr{^[0-9]+$};
	push @filters, "s.interval_no = $summary_interval";
    }
    
    # Then, if a time filter was specified and we need one, apply it.  If we
    # are were given a summary interval and no non-geographic filters were
    # specified, then we don't need one because the necessary filtering has
    # already been done by selecting the appropriate interval_no in the summary table.
    
    if ( defined $early_age or defined $late_age )
    {
	unless ( $op eq 'summary' and not $tables_ref->{non_geo_filter} and $time_rule eq 'buffer' )
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
    
    my $x1 = $self->clean_param('lngmin');
    my $x2 = $self->clean_param('lngmax');
    
    return unless $x1 || $x2;
    
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


# selectPaleoModel ( fields_ref, tables_ref )
# 
# Adjust the field list and table hash to select the proper paleocoordinate
# fields according to the parameter 'pgm'.

sub selectPaleoModel {
    
    my ($self, $fields_ref, $tables_ref) = @_;
    
    # Go through each specified paleogeographicmodel and construct a list of the necessary
    # fields.  If no models were specified, use 'gplates' as the default.
    
    my @models = $self->clean_param_list('pgm');
    
    @models = 'gplates' unless @models;
    
    my (@fields, %plate_version_shown);
    my ($lng_field, $lat_field, $plate_field, $model_field);
    my ($model_no, $model_label);
    
    foreach my $model (@models)
    {
	$model_no++;
	$model_label = $model_no > 1 ? $model_no : '';
	
	$lng_field = 'paleolng' . $model_label;
	$lat_field = 'paleolat' . $model_label;
	$plate_field = 'geoplate' . $model_label;
	$model_field = 'paleomodel' . $model_label;
	
	if ( $model eq 'scotese' )
	{
	    push @fields, "cc.paleolng as $lng_field", "cc.paleolat as $lat_field";
	    push @fields, "cc.plate as $plate_field" unless $plate_version_shown{'scotese'};
	    push @fields, "'scotese' as $model_field" if @models > 1;
	    $tables_ref->{cc} = 1;
	    $plate_version_shown{'scotese'} = 1;
	}
	
	elsif ( $model eq 'gplates' || $model eq 'gp_mid' )
	{
	    push @fields, "pc.mid_lng as $lng_field", "pc.mid_lat as $lat_field";
	    push @fields, "pc.plate_no as $plate_field" unless $plate_version_shown{'gplates'};
	    push @fields, "'gp_mid' as $model_field" if @models > 1;
	    $tables_ref->{pc} = 1;
	    $plate_version_shown{'gplates'} = 1;
	}
	
	elsif ( $model eq 'gp_early' )
	{
	    push @fields, "pc.early_lng as $lng_field", "pc.early_lat as $lat_field";
	    push @fields, "pc.plate_no as $plate_field" unless $plate_version_shown{'gplates'};
	    push @fields, "'gp_early' as $model_field" if @models > 1;
	    $tables_ref->{pc} = 1;
	    $plate_version_shown{'gplates'} = 1;
	}
	
	elsif ( $model eq 'gp_late' )
	{
	    push @fields, "pc.late_lng as $lng_field", "pc.late_lat as $lat_field";
	    push @fields, "pc.plate_no as $plate_field" unless $plate_version_shown{'gplates'};
	    push @fields, "'gp_late' as $model_field" if @models > 1;
	    $tables_ref->{pc} = 1;
	    $plate_version_shown{'gplates'} = 1;
	}
    }
    
    # Now substitute this list into the field string.
    
    my $paleofields = join(", ", @fields);
    
    $$fields_ref =~ s/PALEOCOORDS/$paleofields/;
}


# adjustPCIntervals ( string... )
# 
# Adjust the specified strings (portions of SQL statements) to reflect the
# proper interval endpoint when selecting or displaying paleocoordinates.

sub adjustPCIntervals {
    
    my ($self, @stringrefs) = @_;
    
    # Each of the subsequent arguments are references to strings to be
    # altered.
    
    my $selector = $self->clean_param('pcis');
    
    if ( $selector eq 'start' )
    {
	foreach my $sref (@stringrefs)
	{
	    $$sref =~ s/\.mid_(lng|lat|plate_id)/\.early_$1/g;
	}
    }
    
    elsif ( $selector eq 'end' )
    {
	foreach my $sref (@stringrefs)
	{
	    $$sref =~ s/\.mid_(lng|lat|plate_id)/\.late_$1/g;
	}
    }
    
    my $a = 1;	# we can stop here when debugging
}    


# generate_order_clause ( options )
# 
# Return the order clause for the list of references, or the empty string if
# none was selected.

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
	
	elsif ( $term eq 'agespan' )
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
	
	elsif ( $term eq 'plate' )
	{
	    my ($pgm) = $self->clean_param_list('pgm');
	    $pgm //= 'gplates';
	    
	    if ( $pgm eq 'scotese' )
	    {
		push @exprs, "cc.plate $dir";
		$tables->{cc} = 1;
	    }
	    
	    else
	    {
		push @exprs, "pc.plate_no $dir";
		$tables->{pc} = 1;
	    }
	}
	
	elsif ( $term eq 'created' )
	{
	    $dir ||= 'desc';
	    push @exprs, "$bt.created $dir";
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
	    die "400 bad value for parameter 'order' (was '$term')\n";
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
    
    $tables->{o} = 1 if $tables->{t} || $tables->{tf} || $tables->{oc};
    $tables->{c} = 1 if $tables->{o};
    
    # Create the necessary join expressions.
    
    $join_list .= "JOIN occ_matrix as o on o.collection_no = c.collection_no\n"
	if $tables->{o};
    $join_list .= "JOIN occurrences as oc using (occurrence_no)\n"
	if $tables->{oc};
    $join_list .= "JOIN taxon_trees as t using (orig_no)\n"
	if $tables->{t} || $tables->{tf};
    $join_list .= "LEFT JOIN $PALEOCOORDS as pc on pc.collection_no = c.collection_no\n"
	if $tables->{pc};
    $join_list .= "LEFT JOIN $GEOPLATES as gp on gp.plate_no = pc.mid_plate_id\n"
	if $tables->{gp};
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
    $join_list .= "LEFT JOIN $COUNTRY_MAP as ccmap on ccmap.cc = c.cc"
	if $tables->{ccmap};
    
    return $join_list;
}


# set_collection_refs ( record )
# 
# Set the reference_no field based on reference_no and reference_nos.  The
# latter holds all of the reference numbers as a comma-separated list, the
# former holds the primary reference number which should always be reported
# first.  The result must be a listref, even if there is only one reference
# number reported.

sub set_collection_refs {
    
    my ($self, $record) = @_;
    
    my @refs; @refs = split qr{,}, $record->{reference_nos} if $record->{reference_nos};
    
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


# valid_country ( )
# 
# Validate values for the 'country' parameter.

my $country_error = "bad value {value} for {param}: must be a country code from ISO-3166-1 alpha-2";

sub valid_country {
    
    my ($value, $context) = @_;
    
    # Start with a simple syntactic check.
    
    return { error => $country_error }
	unless $value =~ /^[a-zA-Z]{2}$/;
    
    # Then check it against the database.
    
    my $valid = exists $COUNTRY_NAME{uc $value};
    
    return $valid ? { value => uc $value }
		  : { error => $country_error };
}


# valid_continent ( )
# 
# Validate values for the 'continent' parameter.

sub valid_continent {
    
    my ($value, $context) = @_;
    
    # Start with a simple syntactic check.
    
    return { error => continent_error() }
	unless $value =~ /^[a-zA-Z]{3}$/;
    
    # Then check it against the database.
    
    my $valid = exists $CONTINENT_NAME{uc $value};
    
    return $valid ? { value => uc $value }
		  : { error => continent_error() };
}

sub continent_error {
    
    my $list = "'" . join("', '", keys %CONTINENT_NAME) . "'";
    return "bad value {value} for {param}, must be one of: $list";
}


# prune_field_list ( )
# 
# This routine is called as a hook after the request is configured.  It
# deletes any unnecessary fields from the field list, so they will not show up
# in fixed-format output.

sub prune_field_list {
    
    my ($self) = @_;
    
    my $field_list = $self->output_field_list;
    
    # If the '1.1:colls:paleoloc' block is selected, then trim any unused
    # fields.
    
    if ( $self->block_selected('1.1:colls:paleoloc') )
    {
	my (@pgmodels) = $self->clean_param_list('pgm');
	my $model_count = scalar(@pgmodels) || 1;
	
	my @good_fields;
	
	foreach my $f ( @$field_list )
	{
	    if ( defined $f->{field} && $f->{field} =~ /^(?:paleolat|paleolng|paleomodel|geoplate)(\d)/ )
	    {
		next if $1 > $model_count;
	    }
	    
	    push @good_fields, $f;
	}
	
	if ( scalar(@good_fields) < scalar(@$field_list) )
	{
	    @$field_list = @good_fields;
	}
    }
    
    my $a = 1;	# we can stop here when debugging
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
