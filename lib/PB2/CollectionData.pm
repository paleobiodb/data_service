# CollectionData
# 
# A class that returns information from the PaleoDB database about a single
# collection or a category of collections.  This is a subclass of PBDataQuery.
# 
# Author: Michael McClennen

use strict;

use lib '..';

use POSIX ();

package PB2::CollectionData;

use HTTP::Validate qw(:validators);

use PB2::CommonData qw(generateAttribution);
use PB2::ReferenceData qw(format_reference);

use TableDefs qw($COLL_MATRIX $COLL_BINS $COLL_STRATA $COUNTRY_MAP $PALEOCOORDS $GEOPLATES
		 $INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP $INTERVAL_BUFFER $PVL_MATRIX);
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);
use Taxonomy;

use Try::Tiny;
use Carp qw(carp croak);

use Moo::Role;

no warnings 'numeric';


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::ConfigData PB2::ReferenceData PB2::IntervalData);

our ($MAX_BIN_LEVEL) = 0;
our (%COUNTRY_NAME, %CONTINENT_NAME);
our (%ENVALUE);

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
    
    foreach my $r ( @$PB2::ConfigData::COUNTRIES )
    {
	$COUNTRY_NAME{$r->{cc}} = $r->{name};
    }
    
    foreach my $r ( @$PB2::ConfigData::CONTINENTS )
    {
	$CONTINENT_NAME{$r->{continent_code}} = $r->{continent_name};
    }
    
    # Define an output map listing the blocks of information that can be
    # returned about collections.
    
    $ds->define_output_map('1.2:colls:basic_map' =>
        { value => 'attr', maps_to => '1.2:colls:attr' },
	    "The attribution of the collection: the author name(s) from",
	    "the primary reference, and the year of publication.",
        { value => 'loc', maps_to => '1.2:colls:loc' },
	    "Additional information about the geographic locality of the collection",
	{ value => 'paleoloc', maps_to => '1.2:colls:paleoloc' },
	    "Information about the paleogeographic locality of the collection,",
	    "evaluated according to the model(s) specified by the parameter C<pgm>.",
	{ value => 'prot', maps_to => '1.2:colls:prot' },
	    "Indicate whether the collection is on protected land",
        { value => 'time', maps_to => '1.2:colls:time' },
	    "This block is obsolete, and is included only for compatibility reasons.",
	    "It does not include any fields in the response.",
	{ value => 'strat', maps_to => '1.2:colls:strat' },
	    "Basic information about the stratigraphic context of the collection.",
	{ value => 'stratext', maps_to => '1.2:colls:stratext' },
	    "Detailed information about the stratigraphic context of collection.",
	    "This includes all of the information from C<strat> plus extra fields.",
	{ value => 'lith', maps_to => '1.2:colls:lith' },
	    "Basic information about the lithological context of the collection.",
	{ value => 'lithext', maps_to => '1.2:colls:lithext' },
	    "Detailed information about the lithological context of the collection.",
	    "This includes all of the information from C<lith> plus extra fields.",
	{ value => 'geo', maps_to => '1.2:colls:geo' },
	    "Information about the geological context of the collection",
	{ value => 'methods', maps_to => '1.2:colls:methods' },
	    "Information about the collection methods used",
        { value => 'rem', maps_to => '1.2:colls:rem' },
	    "Any additional remarks that were entered about the collection.",
	{ value => 'bin', maps_to => '1.2:colls:bin' },
	    "The list of geographic clusters to which the collection belongs.",
	{ value => 'resgroup', maps_to => '1.2:colls:group' },
	    "The research group(s), if any, associated with this collection.",
        { value => 'ref', maps_to => '1.2:refs:primary' },
	    "The primary reference for the collection, as formatted text.",
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the collection record",
	">I<The following will return all of the information available about",
	"the collection itself, as opposed to its context.  If any more such information",
	"is added to this data service, this function will be adjusted accordingly.",
	"You can therefore include it in published URLs, knowing that it will always provide",
	"all of the available information about the collection(s) of interest.>",
	{ value => 'full', maps_to => '1.2:colls:full_info' },
	    "Includes all of the information from the following blocks: B<attr>, B<loc>,",
	    "B<paleoloc>, B<prot>, B<stratext>, B<lithext>, B<geo>, B<methods>, B<rem>.");
    
    # Then a second block for geographic summary clusters.
    
    $ds->define_output_map('1.2:colls:summary_map' =>
        { value => 'ext', maps_to => '1.2:colls:ext' },
	    "Additional information about the geographic extent of each cluster.",
        { value => 'time', maps_to => '1.2:colls:time' },
	  # This block is defined in our parent class, CollectionData.pm
	    "Additional information about the temporal range of the",
	    "cluster.");
    
    # Then define the output blocks which these mention.
    
    $ds->define_block('1.2:colls:basic' =>
	{ select => ['c.collection_no', 'cc.collection_name', 'cc.collection_subset', 'cc.formation',
		     'c.lat', 'c.lng', 'c.n_occs', 'c.early_age', 'c.late_age',
		     'ei.interval_name as early_interval', 'li.interval_name as late_interval',
		     'c.reference_no', 'group_concat(distinct sr.reference_no) as reference_nos'], 
	  tables => ['cc', 'ei', 'li', 'sr'] },
	{ output => 'collection_no', dwc_name => 'collectionID', com_name => 'oid' },
	    "A unique identifier for the collection.  This will be a string if the result",
	    "format is JSON.  For backward compatibility, all identifiers in text format",
	    "results will continue to be integers.",
	{ output => 'record_type', com_name => 'typ', value => $IDP{COL},
	  dwc_value => 'Occurrence' },
	    "type of this object: C<$IDP{COL}> for a collection",
	{ output => 'formation', com_name => 'sfm', not_block => 'strat' },
	    "The formation in which the collection was found",
	{ output => 'lng', dwc_name => 'decimalLongitude', com_name => 'lng', data_type => 'dec' },
	    "The longitude at which the collection is located (in degrees)",
	{ output => 'lat', dwc_name => 'decimalLatitude', com_name => 'lat', data_type => 'dec' },
	    "The latitude at which the collection is located (in degrees)",
	{ output => 'collection_name', dwc_name => 'collectionCode', com_name => 'nam' },
	    "An arbitrary name which identifies the collection, not necessarily unique",
	{ output => 'collection_subset', com_name => 'nm2' },
	    "If the collection is a part of another one, this field specifies which part",
	{ output => 'attribution', dwc_name => 'recordedBy', com_name => 'att', if_block => '1.2:colls:attr' },
	    "The attribution (author and year) of the collection",
	{ output => 'pubyr', com_name => 'pby', if_block => '1.2:colls:attr', data_type => 'pos' },
	    "The year in which the collection was published",
	{ output => 'n_occs', com_name => 'noc', data_type => 'pos' },
	    "The number of occurrences in the collection",
	{ output => 'early_interval', com_name => 'oei', pbdb_name => 'early_interval' },
	    "The specific geologic time range associated with the collection (not necessarily a",
	    "standard interval), or the interval that begins the range if C<late_interval> is also given",
	{ output => 'late_interval', com_name => 'oli', pbdb_name => 'late_interval', dedup => 'early_interval' },
	    "The interval that ends the specific geologic time range associated with the collection",
	{ output => 'early_age', com_name => 'eag', pbdb_name => 'max_ma' },
	    "The early bound of the geologic time range associated with this collection (in Ma)",
	{ output => 'late_age', com_name => 'lag', pbdb_name => 'min_ma' },
	    "The late bound of the geologic time range associated with this collection (in Ma)",
	{ set => 'reference_no', from => '*', code => \&set_collection_refs },
	{ output => 'reference_no', com_name => 'rid', text_join => ', ' },
	    "The identifier(s) of the references from which this data was entered.  For",
	    "now these are positive integers, but this could change and should B<not be relied on>.",
	{ set => '*', code => \&process_coll_com, if_vocab => 'com' });
    
    my @bin_levels;
    
    foreach my $level ( 1..$MAX_BIN_LEVEL )
    {
	push @bin_levels, { output => "bin_id_$level", com_name => "lv$level" };
	push @bin_levels, "The identifier of the level-$level cluster in which the collection is located";
    }
    
    $ds->define_block('1.2:colls:bin' => @bin_levels);
    
    $ds->define_block('1.2:colls:attr' =>
        { select => ['r.author1init as a_ai1', 'r.author1last as a_al1', 'r.author2init as a_ai2', 
	  	     'r.author2last as a_al2', 'r.otherauthors as a_oa', 'r.pubyr as a_pubyr'],
          tables => ['r'] },
        { set => 'attribution', from => '*', code => \&generateAttribution },
        { set => 'pubyr', from => 'a_pubyr' });
    
    $ds->define_block('1.2:colls:ref' =>
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
    
    $ds->define_block('1.2:colls:loc' =>
	{ select => ['c.cc', 'cc.state', 'cc.county', 'cc.geogscale', 'cc.geogcomments',
		     'cc.latlng_basis', 'cc.latlng_precision', 'cc.altitude_value', 'cc.altitude_unit'],
	  tables => ['cc'] },
	{ output => 'cc', com_name => 'cc2' },
	    "The country in which the collection is located, encoded as",
	    "L<ISO-3166-1 alpha-2|https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2>",
	{ output => 'state', com_name => 'stp' },
	    "The state or province in which the collection is located, if known",
	{ output => 'county', com_name => 'cny' },
	    "The county or municipal area in which the collection is located, if known",
	{ output => 'latlng_basis', if_vocab => 'pbdb' },
	    "The basis of the reported location of the collection.  Follow this link for a",
	    "L<list of basis and precision codes|node:basis_precision>.  This field and",
	    "the next are only included in responses using the pbdb vocabulary.",
	{ output =>'latlng_precision', if_vocab => 'pbdb' },
	    "The precision of the collection coordinates.  Follow the above",
	    "link for a list of the code values.",
	{ set => 'prc', from => '*', code => \&generateBasisCode, if_vocab => 'com' },
	{ output => 'prc', pbdb_name => 'I<n/a>', com_name => 'prc', if_vocab => 'com' },
	    "A two-letter code indicating the basis and precision of the geographic coordinates.",
	    "This field is reported instead of C<latlng_basis> and C<latlng_precision> in",
	    "responses that use the compact vocabulary.  Follow the above link for a list of the code values.",
	{ output => 'geogscale', com_name => 'gsc' },
	    "The geographic scale of the collection.",
	{ output => 'geogcomments', com_name => 'ggc' },
	    "Additional comments about the geographic location of the collection");

     $ds->define_block('1.2:colls:paleoloc' =>
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
    
    $ds->define_block('1.2:colls:prot' =>
	{ select => ['c.cc', 'c.protected'] },
	{ output => 'cc', com_name => 'cc2', not_block => 'loc' },
	    "The country in which the collection is located, encoded as",
	    "L<ISO-3166-1 alpha-2|https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2>",
	{ output => 'protected', com_name => 'ptd' },
	    "The protected status of the land on which the collection is located, if known.");
    
    $ds->define_block('1.2:colls:time');
    
    #   { select => ['$mt.early_age', '$mt.late_age', 'im.cx_int_no', 'im.early_int_no', 'im.late_int_no'],
    # 	tables => ['im'] },
    #   { set => '*', code => \&fixTimeOutput },
    #   { output => 'early_age', com_name => 'eag', data_type => 'dec' },
    # 	  "The early bound of the geologic time range associated with the collection or cluster (in Ma)",
    #   { output => 'late_age', com_name => 'lag', data_type => 'dec' },
    # 	  "The late bound of the geologic time range associated with the collection or cluster (in Ma)",
    #   { output => 'cx_int_no', com_name => 'cxi' },
    # 	  "The identifier of the most specific single interval from the selected timescale that",
    # 	  "covers the entire time range associated with the collection or cluster.",
    #   { output => 'early_int_no', com_name => 'ein' },
    # 	  "The beginning of a range of intervals from the selected timescale that most closely",
    # 	  "brackets the time range associated with the collection or cluster (with C<late_int_no>)",
    #   { output => 'late_int_no', com_name => 'lin' },
    # 	  "The end of a range of intervals from the selected timescale that most closely brackets",
    # 	  "the time range associated with the collection or cluster (with C<early_int_no>)");
    
    $ds->define_block('1.2:colls:strat' =>
	{ select => ['cc.formation', 'cc.geological_group', 'cc.member'], tables => 'cc' },
	{ output => 'formation', com_name => 'sfm' },
	    "The stratigraphic formation in which the collection is located, if known",
	{ output => 'geological_group', pbdb_name => 'stratgroup', com_name => 'sgr' },
	    "The stratigraphic group in which the collection is located, if known",
	{ output => 'member', com_name => 'smb' },
	    "The stratigraphic member in which the collection is located, if known");
    
    $ds->define_block('1.2:colls:stratext' =>
	{ include => '1.2:colls:strat' },
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
    
    $ds->define_block('1.2:colls:lith' =>
	{ select => [ qw(cc.lithdescript cc.lithification cc.minor_lithology cc.lithology1
			 cc.lithification2 cc.minor_lithology2 cc.lithology2) ], tables => 'cc' },
	{ output => 'lithdescript', com_name => 'ldc' },
	    "Detailed description of the collection site in terms of lithology",
	{ output => 'lithology1', com_name => 'lt1' },
	    "The first lithology described for the collection site; the database can",
	    "represent up to two different lithologies per collection",
	{ output => 'lithadj', pbdb_name => 'lithadj1', com_name => 'la1', if_block => '1.2:colls:lithext' },
	    "Adjective(s) describing the first lithology",
	{ output => 'lithification', pbdb_name => 'lithification1', com_name => 'lf1' },
	    "Lithification state of the first lithology described for the site",
	{ output => 'minor_lithology', pbdb_name => 'minor_lithology1', com_name => 'lm1' },
	    "Minor lithology associated with the first lithology described for the site",
	{ output => 'fossilsfrom1', com_name => 'ff1', if_block => '1.2:colls:lithext' },
	    "Whether or not fossils were taken from the first described lithology",
	{ output => 'lithology2', com_name => 'lt2' },
	    "The second lithology described for the collection site, if any",
	{ output => 'lithadj2', com_name => 'la2', if_block => '1.2:colls:lithext' },
	    "Adjective(s) describing the second lithology, if any",
	{ output => 'lithification2', com_name => 'lf2' },
	    "Lithification state of the second lithology described for the site.  See above for values.",
	{ output => 'minor_lithology2', com_name => 'lm2' },
	    "Minor lithology associated with the second lithology described for the site, if any",
	{ output => 'fossilsfrom2', com_name => 'ff2', if_block => '1.2:colls:lithext' },
	    "Whether or not fossils were taken from the second described lithology");
    
    $ds->define_block('1.2:colls:lithext' =>
	{ select => [ qw(cc.lithadj cc.fossilsfrom1 cc.lithadj2 cc.fossilsfrom2) ], 
	  tables => 'cc' },
	{ include => '1.2:colls:lith' });
    
    $ds->define_block('1.2:colls:geo' =>
	{ select => [ qw(cc.environment cc.tectonic_setting cc.geology_comments) ], 
	  tables => 'cc' },
	{ output => 'environment', com_name => 'env' },
	    "The paleoenvironment of the collection site",
	{ output => 'tectonic_setting', com_name => 'tec' },
	    "The tectonic setting of the collection site",
	{ output => 'geology_comments', com_name => 'gcm' },
	    "General comments about the geology of the collection site");
    
    $ds->define_block('1.2:colls:methods' => 
	{ select => [ 'cc.collection_type', 'cc.coll_meth as collection_methods', 'cc.museum',
		      'cc.collection_coverage', 'cc.collection_size', 'cc.collection_size_unit',
		      'cc.rock_censused', 'cc.rock_censused_unit',
		      'cc.collectors', 'cc.collection_dates', 'cc.collection_comments',
		      'cc.taxonomy_comments' ],
	  tables => 'cc' },
	{ set => '*', code => \&process_methods }, 
	{ output => 'collection_type', com_name => 'cct' },
	    "The type or purpose of the collection.",
	{ output => 'collection_methods', com_name => 'ccx' },
	    "The method or methods employed.",
	{ output => 'museum', com_name => 'ccu' },
	    "The museum or museums which hold the specimens.",
	{ output => 'collection_coverage', com_name => 'ccv' },
	    "Fossils that were present but not specifically listed.",
	{ output => 'collection_size', com_name => 'ccs' },
	    "The number of fossils actually collected.",
	{ output => 'rock_censused', com_name => 'ccr' },
	    "The amount of rock censused.",
	{ output => 'collectors', com_name => 'ccc' },
	    "Names of the collectors.",
	{ output => 'collection_dates', com_name => 'ccd' },
	    "Dates on which the collection was done.",
	{ output => 'collection_comments', com_name => 'ccm' },
	    "Comments about the collecting methods.",
	{ output => 'taxonomy_comments', com_name => 'tcm' },
	    "Comments about the taxonomy of what was found.");
    
    $ds->define_block('1.2:colls:group' =>
	{ select => [ 'cc.research_group' ],
	  tables => 'cc' },
	{ output => 'research_group', com_name => 'rgp' },
	    "The research group(s), if any, associated with this collection.");
    
    $ds->define_block( '1.2:colls:rem' =>
	{ set => 'collection_aka', join => '; ', if_format => 'txt,tsv,csv,xml' },
	{ output => 'collection_aka', dwc_name => 'collectionRemarks', com_name => 'crm' },
	    "Any additional remarks that were entered about the collection");
    
    $ds->define_block( '1.2:colls:full_info' =>
	{ include => '1.2:colls:attr' },
	{ include => '1.2:colls:loc' },
	{ include => '1.2:colls:paleoloc' },
	{ include => '1.2:colls:prot' },
	{ include => '1.2:colls:stratext' },
	{ include => '1.2:colls:lithext' },
	{ include => '1.2:colls:geo' },
	{ include => '1.2:colls:methods' },
	{ include => '1.2:colls:rem' });
    
    # Then define an output block for displaying stratigraphic results
    
    $ds->define_block('1.2:strata:basic' =>
	{ select => ['cs.name', 'cs.rank', 'count(*) as n_colls', 'sum(n_occs) as n_occs'] },
	{ output => 'record_type', com_name => 'typ', value => 'str' },
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
    
    $ds->define_block('1.2:strata:occs' =>
	{ select => ['count(distinct cc.collection_no) as n_colls',
		     'count(*) as n_occs', 'cc.geological_group as `group`',
		     'cc.formation', 'cc.member', 'min(c.early_age) as early_age',
		     'min(c.late_age) as late_age'],
	  tables => [ 'cc' ] },
	{ output => 'record_type', com_name => 'typ', value => 'str' },
	    "The type of this record: 'str' for a stratum",
	{ output => 'group', com_name => 'sgr' },
	    "The name of the group in which occurrences were found",
	{ output => 'formation', com_name => 'sfm' },
	    "The name of the formation in which occurences were found",
	{ output => 'member', com_name => 'smb' },
	    "The name of the member in which occurrences were found",
	{ output => 'early_age', com_name => 'eag', pbdb_name => 'max_ma' },
	    "The early bound of the geologic time range associated with the selected occurrences (in Ma)",
	{ output => 'late_age', com_name => 'lag', pbdb_name => 'min_ma' },
	    "The late bound of the geologic time range associated with the selected occurrences (in Ma)",
	{ output => 'n_colls', com_name => 'nco', data_type => 'pos' },
	    "The number of fossil collections in the database from this",
	    "stratum that contain occurrences from the selected set and",
	    "are listed as being part of this stratum",
	{ output => 'n_occs', com_name => 'noc', data_type => 'pos' },
	    "The number of fossil occurrences in the database that are",
	    "listed as being part of this stratum");
    
    # And a block for basic geographic summary cluster info
    
    $ds->define_block( '1.2:colls:summary' =>
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
      { output => 'record_type', com_name => 'typ', value => $IDP{CLU} },
	  "The type of this object: C<$IDP{CLU}> for a collection cluster",
      { output => 'n_colls', com_name => 'nco', data_type => 'pos' },
	  "The number of collections in this cluster",
      { output => 'n_occs', com_name => 'noc', data_type => 'pos' },
	  "The number of occurrences in this cluster",
      { output => 'lng', com_name => 'lng', data_type => 'dec' },
	  "The longitude of the centroid of this cluster",
      { output => 'lat', com_name => 'lat', data_type => 'dec' },
	  "The latitude of the centroid of this cluster");
    
    # Plus one for summary cluster extent
    
    $ds->define_block( '1.2:colls:ext' =>
      { select => ['s.lng_min', 'lng_max', 's.lat_min', 's.lat_max', 's.std_dev'] },
      { output => 'lng_min', com_name => 'lx1', data_type => 'dec' },
	  "The mimimum longitude for collections in this cluster",
      { output => 'lng_max', com_name => 'lx2', data_type => 'dec' },
	  "The maximum longitude for collections in this cluster",
      { output => 'lat_min', com_name => 'ly1', data_type => 'dec' },
	  "The mimimum latitude for collections in this cluster",
      { output => 'lat_max', com_name => 'ly2', data_type => 'dec' },
	  "The maximum latitude for collections in this cluster",
      { output => 'std_dev', com_name => 'std', data_type => 'dec' },
	  "The standard deviation of the coordinates in this cluster");
    
    # Finally, define rulesets to interpret the parmeters used with operations
    # defined by this class.
    
    $ds->define_set('1.2:colls:order' =>
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
    
    $ds->define_set('1.2:colls:ident_select' =>
	{ value => 'latest' },
	    "Select the most recently published identification of each selected occurrence,",
	    "as long as it matches the other specified criteria.  This is the B<default> unless",
	    "you specify otherwise.",
	{ value => 'orig' },
	    "Select the originally published identification of each selected occurence,",
	    "as long as it matches the other specified criteria.",
	{ value => 'all' },
	    "Select all matching identifications of each selected occurrence, each as a separate record");
    
    $ds->define_set('1.2:colls:pgmodel' =>
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
    
    $ds->define_set('1.2:colls:environment' =>
	{ value => 'terr' },
	    "Any terrestrial environment",
	{ value => 'marine' },
	    "Any marine environment",
	{ value => 'carbonate' },
	    "Carbonate environment",
	{ value => 'silici' },
	    "Siliciclastic environment",
	{ value => 'unknown' },
	    "Unknown or indeterminate environment");
    
    $ds->define_set('1.2:colls:envzone' => 
	{ value => 'lacust' },
	    "Lacustrine zone",
	{ value => 'fluvial' },
	    "Fluvial zone",
	{ value => 'karst' },
	    "Karst zone",
	{ value => 'terrother' },
	    "Other terrestrial zone",
	{ value => 'marginal' },
	    "Marginal marine zone",
	{ value => 'reef' },
	    "Reef zone",
	{ value => 'stshallow' },
	    "Shallow subtidal zone",
	{ value => 'stdeep' },
	    "Deep subtidal zone",
	{ value => 'offshore' },
	    "Offshore zone",
	{ value => 'slope' },
	    "Slope/basin zone",
	{ value => 'marindet'
	    "Marine indeterminate zone");
	
    $ds->define_ruleset('1.2:main_selector' =>
	{ param => 'coll_id', valid => VALID_IDENTIFIER('COL'), list => ',' },
	    "A comma-separated list of collection identifiers.  All records associated with",
	    "the specified collections are returned, provided they satisfy the other parameters",
	    "given with this request.",
	{ param => 'clust_id', valid => VALID_IDENTIFIER('CLU'), list => ',' },
	    "Return only records associated with the specified geographic clusters.",
	    "You may specify one or more cluster ids, separated by commas.",
	{ param => 'base_name', valid => \&PB2::TaxonData::validNameSpec },
	    "Return only records associated with the specified taxonomic name(s),",
	    "I<including all subtaxa and synonyms>.  You may specify multiple names, separated",
	    "by commas.  You may append one or more exclusions",
	    "to any name, using the C<^> character.  For example, C<Osteichthyes^Tetrapoda> would select",
	    "the fish excluding the tetrapods.",
	{ param => 'taxon_name', valid => \&PB2::TaxonData::validNameSpec },
	    "Return only records associated with the specified taxonomic name(s),",
	    "I<including any synonyms>.",
	    "You may specify multiple names, separated by commas.  Names may",
	    "include wildcards, but if more than one name matches then only the",
	    "one with the largest number of occurrences in the database will be used.",
	{ param => 'match_name', valid => \&PB2::TaxonData::validNameSpec },
	    "Return only records associated with the specified taxonomic name(s).",
	    "You may specify multiple names, separated by commas.  Names may include",
	    "wildcards, and occurrences associated with all matching names will",
	    "be returned.  Synonyms will be ignored.  This is a syntactic rather",
	    "than a taxonomic match.",
	{ param => 'immediate', valid => FLAG_VALUE },
	    "You may specify this parameter along with C<base_name>, C<base_id>, or C<taxon_name>.",
	    "If you do, then synonyms of the specified name(s) will",
	    "be ignored.  No value is necessary for this parameter, just include the parameter name.",
	{ param => 'base_id', valid => VALID_IDENTIFIER('TID'), list => ',' },
	    "Return only records associated with the specified taxa,",
	    "I<including subtaxa and synonyms>.  You may specify multiple taxon identifiers,",
	    "separated by commas.  Note that you may specify at most one of 'taxon_name', 'taxon_id', 'base_name', 'base_id'.",
	{ param => 'taxon_id', valid => VALID_IDENTIFIER('TID'), list => ','},
	    "Return only records associated with the specified taxa, not including",
	    "subtaxa or synonyms.  You may specify multiple taxon identifiers, separated by commas.",
	{ at_most_one => ['taxon_name', 'taxon_id', 'base_name', 'base_id'] },
	{ param => 'exclude_id', valid => VALID_IDENTIFIER('TID'), list => ','},
	    "Exclude any records whose associated taxonomic name is a child of the given name or names,",
	    "specified by numeric identifier.  This is an alternative to the use of the C<^> character",
	    "in names.",
	{ param => 'ident', valid => $ds->valid_set('1.2:colls:ident_select'), default => 'latest' },
	    "If more than one taxonomic identification is recorded for some or all of the selected occurrences,",
	    "this parameter specifies which are to be returned.  Values include:",
	    $ds->document_set('1.2:colls:ident_select'),
	{ param => 'lngmin', valid => COORD_VALUE('lng') },
	{ param => 'lngmax', valid => COORD_VALUE('lng') },
	    "Return only records whose present longitude falls within the given bounds.",
	    "If you specify one of these parameters then you must specify both.",
	    "If you provide bounds outside of the range -180\N{U+00B0} to 180\N{U+00B0}, they will be",
	    "wrapped into the proper range.  For example, if you specify C<lngmin=270 & lngmax=360>,",
	    "the query will be processed as if you had said C<lngmin=-90 & lngmax=0 >.  In this",
	    "case, all longitude values in the query result will be adjusted to fall within the actual",
	    "numeric range you specified.",
	{ param => 'latmin', valid => COORD_VALUE('lat') },
	    "Return only records whose present latitude is at least the given value.",
	{ param => 'latmax', valid => COORD_VALUE('lat') },
	    "Return only records whose present latitude is at most the given value.",
	{ together => ['lngmin', 'lngmax'],
	  error => "you must specify both of 'lngmin' and 'lngmax' if you specify either of them" },
	{ param => 'loc', valid => ANY_VALUE },		# This should be a geometry in WKT format
	    "Return only records whose present location (longitude and latitude) falls within",
	    "the specified shape, which must be given in L<WKT|https://en.wikipedia.org/wiki/Well-known_text> format",
	    "with the coordinates being longitude and latitude values.",
	{ param => 'plate', valid => ANY_VALUE },
	    "Return only records located on the specified geological plate(s).  If the value",
	    "of this parameter starts with 'G', then these will be interpreted as plate numbers",
	    "from the GPlates model.  If the value starts with 'S', then these will be",
	    "interpreted as plate numbers from the Scotese model.  Otherwise, they will",
	    "be interpreted according to the value of the parameter C<pgm>.  If the",
	    "value continues with C<^>, then all records located on the specified plates",
	    "are instead B<excluded>.  The remainder of the value must be a",
	    "list of plate numbers.",
	{ optional => 'pgm', valid => $ds->valid_set('1.2:colls:pgmodel'), list => "," },
	    "Specify which paleogeographic model(s) to use when evaluating paleocoordinates.",
	    "You may specify one or more from the following list, separated by commas.",
	    "If you do not specify a value for this parameter, the default model is C<gplates>.",
	    $ds->document_set('1.2:colls:pgmodel'),
	{ param => 'cc', valid => \&valid_cc, list => qr{[\s,]+}, alias => 'country', bad_value => '_' },
	    "Return only records whose location falls within the specified geographic regions.",
	    "The value of this parameter should be one or more",
	    "L<two-character country codes|http://en.wikipedia.org/wiki/ISO_3166-1_alpha-2> and/or ",
	    "L<three-character continent codes|op:config.txt?show=continents> as a comma-separated list.",
	    "If the symbol C<^> appears in the parameter value, then records falling into regions",
	    "listed thereafter are B<excluded>.  Examples:",
	    ">    NOA,SOA      EUR,^UK,IE    ^ATA",
	{ param => 'continent', valid => \&valid_continent, list => qr{[\s,]+}, bad_value => '_' },
	    "Return only records whose geographic location falls within the specified continent or continents.",
	    "The value of this parameter should be a comma-separated list of ",
	    "L<continent codes|op:config.txt?show=continents>.",
	{ param => 'strat', valid => ANY_VALUE, list => ',' },
	    "Return only records that fall within the named geological stratum or strata.  You",
	    "may specify more than one, separated by commas.  Names may include the standard",
	    "SQL wildcards C<%> and C<_>, and may be followed by any of",
	    "'fm', 'gp', 'mbr'.  If none of these suffixes is given, then all matching",
	    "stratigraphic names will be selected.  Note that this parameter is resolved through",
	    "string matching only.  Stratigraphic nomenclature is not currently standardized in",
	    "the database, so misspellings may occur.", 
	    ">This parameter replaces the parameters C<formation>, C<stratgroup>, and C<member",
	    "which are now deprecated.",
	{ param => 'formation', valid => ANY_VALUE, list => ',' },
	    "Return only records that fall within the named stratigraphic formation(s).",
	    "This parameter is deprecated; use C<strat> instead.",
	{ param => 'stratgroup', valid => ANY_VALUE, list => ',' },
	    "Return only records that fall within the named stratigraphic group(s).",
	    "This parameter is deprecated; use C<strat> instead.",
	{ param => 'member', valid => ANY_VALUE, list => ',' },
	    "Return only records that fall within the named stratigraphic member(s).",
	    "This parameter is deprecated; use C<strat> instead.",
	{ param => 'envtype', valid => $ds->valid_set('1.2:colls:environment'), list => qr{[\s,]+} },
	    "Return only records recorded as belonging to one of the specified environments.",
	    "If the parameter value starts with C<^> then records belonging to the specified",
	    "environments will be B<excluded> instead.",
	    "You may specify one or more of the following values, as a comma-separated list:",
	{ param => 'envzone' valid => $ds->valid_set('1.2:colls:envzone'), list => qr{[\s,]+} },
	    "Return only records recorded as belonging to one of the specified environmental",
	    "zones.  You can use this either alone or in conjunction with the parameter",
	    "C<envtype> to precisely select the records you want.  If the parameter",
	    "value starts with C<^> then records belonging to the specified zones will",
	    "be B<excluded> instead.  You may specify",
	    "one or more of the following values, as a comma-separated list:",
	# { param => 'min_ma', valid => DECI_VALUE(0) },
	#     "Return only records whose temporal locality is at least this old, specified in Ma.",
	# { param => 'max_ma', valid => DECI_VALUE(0) },
	#     "Return only records whose temporal locality is at most this old, specified in Ma.",
	# { param => 'interval_id', valid => VALID_IDENTIFIER('INT'), list => ',' },
	#     "Return only records whose temporal locality falls within the given geologic time",
	#     "interval or intervals, specified by numeric identifier.  If you specify more",
	#     "than one interval, the time range used will be the contiguous period from the",
	#     "beginning of the earliest to the end of the latest specified interval.",
	# { param => 'interval', valid => ANY_VALUE },
	#     "Return only records whose temporal locality falls within the named geologic time",
	#     "interval or intervals, specified by name.  If you specify more than one interval,",
	#     "the time range used will be the contiguous period from the beginning of the",
	#     "earliest to the end of the latest specified interval.",
	# { at_most_one => ['interval_id', 'interval', 'min_ma'] },
	# { at_most_one => ['interval_id', 'interval', 'max_ma'] },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ allow => '1.2:timerule_selector' });
    
    $ds->define_ruleset('1.2:colls:specifier' =>
	{ param => 'coll_id', valid => VALID_IDENTIFIER('COL'), alias => 'id' },
	    "The identifier of the collection you wish to retrieve (REQUIRED).  You",
	    "may instead use the parameter name C<id>.");
    
    $ds->define_ruleset('1.2:colls:selector' =>
	{ param => 'id', valid => VALID_IDENTIFIER('COL'), key => 'coll_id'},
	    "!",
	{ param => 'all_records', valid => FLAG_VALUE },
	    "This parameter needs no value. If included, all records will be selected. By default, all records are",
	    "always included whenever any other parameter is specified.");
    
    $ds->define_ruleset('1.2:colls:display' =>
	"You can use the following parameters to request additional information about each",
	"retrieved collection:",
	{ optional => 'show', list => q{,},
	  valid => $ds->valid_set('1.2:colls:basic_map') },
	    "Selects additional information to be returned",
	    "along with the basic record for each collection.  Its value should be",
	    "one or more of the following, separated by commas:",
	    $ds->document_set('1.2:colls:basic_map'),
	{ optional => 'order', valid => $ds->valid_set('1.2:colls:order'), split => ',' },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  Accepted values are:",
	    $ds->document_set('1.2:colls:order'),
	    "If no order is specified, results are returned as they appear in the C<collections> table.",
	{ ignore => 'level' });
        
    $ds->define_ruleset('1.2:summary_display' =>
	"You can use the following parameter to request additional information about each",
	"retrieved cluster:",
	{ param => 'show', list => q{,},
	  valid => $ds->valid_set('1.2:colls:summary_map') },
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each cluster.  Its value should be",
	    "one or more of the following, separated by commas:",
	    $ds->document_set('1.2:colls:summary_map'),);
    
    $ds->define_ruleset('1.2:colls:single' => 
	"The following required parameter selects a record to retrieve:",
    	{ require => '1.2:colls:specifier', 
	  error => "you must specify a collection identifier, either in the URL or with the 'id' parameter" },
    	{ allow => '1.2:colls:display' },
    	{ allow => '1.2:special_params' },
        "^You can also use any of the L<special parameters|node:special> with this request");

    $ds->define_ruleset('1.2:colls:list' => 
	"You can use the following parameter if you wish to retrieve the entire set of",
	"collection records stored in this database.  Please use this with care, since the",
	"result set will contain more than 100,000 records and will be at least 20 megabytes in size.",
	"You may also specify any of the parameters listed below.",
    	{ allow => '1.2:colls:selector' },
        ">>The following parameters can be used to query for collections by a variety of criteria.",
	"Except as noted below, you may use these in any combination.",
	"These parameters can all be used to select either occurrences, collections, or associated references.",
	"You can use the paramter C<coll_id> in conjunction with other parameters to filter",
	"a known list of collections against other criteria.",
   	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ allow => '1.2:common:select_colls_crmod' },
	{ allow => '1.2:common:select_colls_ent' },
	{ require_any => ['1.2:colls:selector', '1.2:main_selector', '1.2:interval_selector', '1.2:ma_selector'] },
#	">>You can also specify any of the following parameters:",
    	{ allow => '1.2:colls:display' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:colls:summary' => 
	"The following required parameter selects from the available clustering levels:",
	{ param => 'level', valid => POS_VALUE, default => 1 },
	    "Return records from the specified cluster level.  You can find out which",
	    "levels are available by means of the L<config|node:config> URL path. (REQUIRED)",
	">>You can use the following parameters to query for summary clusters by",
	"a variety of criteria.  Except as noted below, you may use these in any combination.",
    	{ allow => '1.2:colls:selector' },
    	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ allow => '1.2:common:select_colls_crmod' },
	{ allow => '1.2:common:select_colls_ent' },
	{ require_any => ['1.2:colls:selector', '1.2:main_selector', '1.2:interval_selector', '1.2:ma_selector'] },
	">>You can use the following parameter if you wish to retrieve information about",
	"the summary clusters which contain a specified collection or collections.",
	"Only the records which match the other parameters that you specify will be returned.",
    	{ allow => '1.2:summary_display' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:colls:refs' =>
	"You can use the following parameters if you wish to retrieve all of the references",
	"with collections entered into the database.",
	{ allow => '1.2:colls:selector' },
        ">>The following parameters can be used to retrieve the references associated with occurrences",
	"selected by a variety of criteria.  Except as noted below, you may use these in any combination.",
	"These parameters can all be used to select either occurrences, collections, or associated references.",
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ allow => '1.2:common:select_colls_crmod' },
	{ allow => '1.2:common:select_colls_ent' },
	{ require_any => ['1.2:colls:selector', '1.2:main_selector', '1.2:interval_selector', '1.2:ma_selector'] },
	">>You can also specify any of the following parameters:",
	{ allow => '1.2:refs:filter' },
	{ allow => '1.2:refs:display' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.2:toprank_selector' =>
	{ param => 'show', valid => ENUM_VALUE('formation', 'ref', 'author'), list => ',' });
    
    $ds->define_ruleset('1.2:colls:toprank' => 
    	{ require => '1.2:main_selector' },
    	{ require => '1.2:toprank_selector' },
    	{ allow => '1.2:special_params' });
    
    $ds->define_ruleset('1.2:strata:selector' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "Return all stratum names known to the database.",
	{ param => 'name', valid => ANY_VALUE, list => ',' },
	    "A full or partial name.  You can use % and _ as wildcards.",
	{ optional => 'rank', valid => ENUM_VALUE('formation','group','member') },
	    "Return only strata of the specified rank: formation, group or member",
	{ param => 'lngmin', valid => DECI_VALUE },
	{ param => 'lngmax', valid => DECI_VALUE },
	{ param => 'latmin', valid => DECI_VALUE },
	{ param => 'latmax', valid => DECI_VALUE },
	    "Return only strata associated with at least one occurrence whose geographic location falls within the given bounding box.",
	    "The longitude boundaries will be normalized to fall between -180 and 180, and will generate",
	    "two adjacent bounding boxes if the range crosses the antimeridian.",
	    "Note that if you specify C<lngmin> then you must also specify C<lngmax>.",
	{ together => ['lngmin', 'lngmax'],
	  error => "you must specify both of 'lngmin' and 'lngmax' if you specify either of them" },
	{ param => 'loc', valid => ANY_VALUE },		# This should be a geometry in WKT format
	    "Return only strata associated with some occurrence whose geographic location falls",
	    "within the specified geometry, specified in WKT format.");
    
    $ds->define_ruleset('1.2:strata:list' =>
	{ require => '1.2:strata:selector' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.2:strata:auto' =>
	{ require => '1.2:strata:selector' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
}


sub max_bin_level {

    return $MAX_BIN_LEVEL;
}


# get ( )
# 
# Query for all relevant information about the collection specified by the
# 'id' parameter.  Returns true if the query succeeded, false otherwise.

sub get {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $request->clean_param('coll_id');
    
    die "400 Bad identifier '$id'" unless defined $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'c', cd => 'cc' );
    
    my $fields = $request->select_string;
    
    $request->adjustCoordinates(\$fields);
    $request->selectPaleoModel(\$fields, $request->tables_hash) if $fields =~ /PALEOCOORDS/;
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generateJoinList('c', $request->tables_hash);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $fields
	FROM $COLL_MATRIX as c JOIN collections as cc using (collection_no)
		LEFT JOIN secondary_refs as sr using (collection_no)
		$join_list
        WHERE c.collection_no = $id and c.access_level = 0
	GROUP BY c.collection_no";
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    die "404 Not found\n" unless $request->{main_record};
}


# list ( )
# 
# Query the database for basic info about all collections satisfying the
# conditions specified by the query parameters.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub list {

    my ($request, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my $tables = $request->tables_hash;
    
    my @filters = $request->generateMainFilters('list', 'c', $tables);
    push @filters, $request->generateCollFilters($tables);
    push @filters, $request->generate_common_filters( { colls => 'cc', bare => 'cc' }, $tables );
    push @filters, '1=1' if $request->clean_param('all_records');
    # push @filters, $request->generate_crmod_filters('cc', $tables);
    # push @filters, $request->generate_ent_filters('cc', $tables);
    
    # Do a final check to make sure that all records are only returned if
    # 'all_records' was specified.
    
    if ( @filters == 0 )
    {
	die "400 You must specify 'all_records' if you want to retrieve the entire set of records.\n"
	    unless $request->clean_param('all_records');
    }
    
    # Until we provide for authenticated data service access, we had better
    # restrict results to publicly accessible records.
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'c', cd => 'cc' );
    
    my $fields = $request->select_string;
    
    $request->adjustCoordinates(\$fields);
    $request->selectPaleoModel(\$fields, $request->tables_hash) if $fields =~ /PALEOCOORDS/;
    
    if ( $tables->{tf} )
    {
	$fields =~ s{ c.n_occs }{count(distinct o.occurrence_no) as n_occs}xs;
    }
    
    # Determine the order in which the results should be returned.
    
    my $order_clause = $request->generate_order_clause($tables, { at => 'c', cd => 'cc' }) || 'NULL';
    
    # Determine if any extra tables need to be joined in.
    
    my $base_joins = $request->generateJoinList('c', $request->tables_hash);
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM coll_matrix as c JOIN collections as cc using (collection_no)
		LEFT JOIN secondary_refs as sr using (collection_no)
		$base_joins
        WHERE $filter_string
	GROUP BY c.collection_no
	ORDER BY $order_clause
	$limit";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


# summary ( )
# 
# This operation queries for geographic summary clusters matching the
# specified parameters.

sub summary {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $tables = $request->tables_hash;
    
    # Figure out which bin level we are being asked for.  The default is 1.    

    my $bin_level = $request->clean_param('level') || 1;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = $request->generateMainFilters('summary', 's', $tables);
    push @filters, $request->generateCollFilters($tables);
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 's' );
    
    my $fields = $request->select_string;
    
    $request->adjustCoordinates(\$fields);
    
    if ( $tables->{tf} )
    {
	$fields =~ s{ s[.]n_colls }{count(distinct c.collection_no) as n_colls}xs;
	$fields =~ s{ s[.]n_occs }{count(distinct o.occurrence_no) as n_occs}xs;
    }
    
    my $summary_joins = '';
    
    $summary_joins .= "JOIN $COLL_MATRIX as c on s.bin_id = c.bin_id_${bin_level}\n"
	if $tables->{c} || $tables->{cc} || $tables->{t} || $tables->{o} || $tables->{oc} || $tables->{tf};
    
    $summary_joins .= "JOIN collections as cc using (collection_no)\n" if $tables->{cc};
    
    $summary_joins .= $request->generateJoinList('s', $tables);
    
    # if ( $request->{select_tables}{o} )
    # {
    # 	$fields =~ s/s.n_colls/count(distinct c.collection_no) as n_colls/;
    # 	$fields =~ s/s.n_occs/count(distinct o.occurrence_no) as n_occs/;
    # }
    
    # elsif ( $request->{select_tables}{c} )
    # {
    # 	$fields =~ s/s.n_colls/count(distinct c.collection_no) as n_colls/;
    # 	$fields =~ s/s.n_occs/sum(c.n_occs) as n_occs/;
    # }
    
    push @filters, "s.access_level = 0", "s.bin_level = $bin_level";
    
    my $filter_string = join(' and ', @filters);
    
    $request->{main_sql} = "
		SELECT $calc $fields
		FROM $COLL_BINS as s $summary_joins
		WHERE $filter_string
		GROUP BY s.bin_id
		ORDER BY s.bin_id $limit";
    
    # Then prepare and execute the query..
    
    print STDERR $request->{main_sql} . "\n\n" if $request->debug;
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # Get the result count, if we were asked to do so.
    
    $request->sql_count_rows;
    
    return 1;
}


# prevtaxa ( )
# 
# This operation queries for the most-occurring taxa found in the geographic
# clusters matching the specified parameters.

# $$$$ MUST ADD: alternate query for when summary bins don't work,
# i.e. 'formation' or 'authorizer'
# 
# $$$$ MUST ADD: global query

# sub prevtaxa {

#     my ($request) = @_;
    
#     # Get a database handle by which we can make queries.
    
#     my $dbh = $request->get_connection;
#     my $tables = $request->tables_hash;
    
#     # Figure out which bin level we are being asked for.  The default is 1.    
    
#     my $bin_level = $request->clean_param('level') || 1;
    
#     # Construct a list of filter expressions that must be added to the query
#     # in order to select the proper result set.
    
#     my @filters = $request->generateMainFilters('summary', 's', $tables);
#     push @filters, $request->generateCollFilters($tables);
    
#     # If the 'strict' parameter was given, make sure we haven't generated any
#     # warnings. 
    
#     $request->strict_check;
    
#     # If a query limit has been specified, modify the query accordingly.
    
#     my $limit = $request->sql_limit_clause(1);
    
#     # If we were asked to count rows, modify the query accordingly
    
#     my $calc = $request->sql_count_clause;
    
#     # Determine which fields and tables are needed to display the requested
#     # information.
    
#     $request->substitute_select( mt => 's' );
    
#     my $fields = $request->select_string;
    
#     $request->adjustCoordinates(\$fields);
    
#     my $summary_joins = '';
    
#     $summary_joins .= "JOIN $COLL_MATRIX as c on s.bin_id = c.bin_id_${bin_level}\n"
# 	if $tables->{c} || $tables->{cc} || $tables->{o} || $tables->{oc} || $tables->{pc};
    
#     $summary_joins .= "JOIN collections as cc using (collection_no)\n" if $tables->{cc};
    
#     my $other_joins .= $request->generateJoinList('s', $tables);
    
#     push @filters, "s.access_level = 0", "s.bin_level = $bin_level";
    
#     my $filter_string = join(' and ', @filters);
    
#     $request->{main_sql} = "
# 		SELECT $calc $fields
# 		FROM $COLL_BINS as s $summary_joins
# 			JOIN $PVL_MATRIX as ds on ds.bin_id = s.bin_id and ds.interval_no = s.interval_no
# 			$other_joins
# 		WHERE $filter_string
# 		GROUP BY ds.orig_no
# 		ORDER BY n_occs desc $limit";
    
#     # Then prepare and execute the query.
    
#     print STDERR $request->{main_sql} . "\n\n" if $request->debug;
    
#     $request->{main_sth} = $dbh->prepare($request->{main_sql});
#     $request->{main_sth}->execute();
    
#     # Get the result count, if we were asked to do so.
    
#     $request->sql_count_rows;
    
#     return 1;
# }


# refs ( )
# 
# Query the database for the references associated with occurrences satisfying
# the conditions specified by the parameters.

sub refs {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my $inner_tables = {};
    
    my @filters = $request->generateMainFilters('list', 'c', $inner_tables);
    push @filters, $request->generateCollFilters($inner_tables);
    push @filters, $request->generate_common_filters( { colls => 'cc' }, $inner_tables );
    # push @filters, $request->generate_crmod_filters('cc', $inner_tables);
    # push @filters, $request->generate_ent_filters('cc', $inner_tables);
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # Construct another set of filter expressions to act on the references.
    
    my $outer_tables = $request->tables_hash;
    
    my @ref_filters = $request->generate_ref_filters($outer_tables);
    push @ref_filters, $request->generate_common_filters( { refs => 'r' }, $outer_tables );
    push @ref_filters, "1=1" unless @ref_filters;
    
    my $ref_filter_string = join(' and ', @ref_filters);
    
    # Figure out the order in which we should return the references.  If none
    # is selected by the options, sort by rank descending.
    
    my $order = PB2::ReferenceData::generate_order_clause($request, { rank_table => 's' }) ||
	"r.author1last, r.author1init";
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'r', cd => 'r' );
    
    my $fields = $request->select_string;
    
    $request->adjustCoordinates(\$fields);
    
    my $inner_join_list = $request->generateJoinList('c', $inner_tables);
    my $outer_join_list = $request->PB2::ReferenceData::generate_join_list($outer_tables);
    
    $request->{main_sql} = "
	SELECT $calc $fields, s.reference_rank, is_primary, if(s.is_primary, 'P', 'S') as ref_type
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
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


# list_strata ( arg )
# 
# Query the database for geological strata.  If the arg is 'auto', then treat
# this query as an auto-completion request.

sub list_strata {
    
    my ($request, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    $request->substitute_select( mt => 'cs' );
    
    my $tables = $request->tables_hash;
    
    my @filters = $request->generateMainFilters('list', 'cs', $tables);
    push @filters, $request->generateStrataFilters($tables, $arg);
    push @filters, "1=1" unless @filters;
    
    my $filter_string = join(' and ', @filters);
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    
    # Modify the query according to the common parameters.
    
    my $limit = $request->sql_limit_clause(1);
    my $calc = $request->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $request->select_string;
    
    #$request->adjustCoordinates(\$fields);
    
    # Determine if any extra tables need to be joined in.
    
    my $base_joins = $request->generateJoinList('cs', $tables);
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM coll_strata as cs
		$base_joins
        WHERE $filter_string
	GROUP BY cs.name, cs.rank
	ORDER BY cs.name
	$limit";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


# fixTimeOutput ( record )
# 
# Adjust the time output by truncating unneeded digits.

sub fixTimeOutput {
    
    my ($request, $record) = @_;
    
    no warnings 'uninitialized';
    
    $record->{early_age} =~ s{ (?: [.] 0+ $ | ( [.] \d* [1-9] ) 0+ $ ) }{$1}sxe
	if defined $record->{early_age};
    $record->{late_age} =~ s{ (?: [.] 0+ $ | ( [.] \d* [1-9] ) 0+ $ ) }{$1}sxe
	if defined $record->{late_age};
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

    my ($request, $tables_ref) = @_;
    
    my $dbh = $request->get_connection;
    my @filters;
    
    # If our tables include the occurrence matrix, we must check the 'ident'
    # parameter. 
    
    if ( ($tables_ref->{o} || $tables_ref->{tf} || $tables_ref->{t} || $tables_ref->{oc}) &&
         ! $tables_ref->{ds} )
    {
	my $ident = $request->clean_param('ident');
	
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

    my ($request, $tables_ref, $is_auto) = @_;
    
    my $dbh = $request->get_connection;
    my @filters;
    
    # Check for parameter 'name'.
    
    if ( my @names = $request->clean_param_list('name') )
    {
	push @filters, $request->generate_stratname_filter('cs', \@names);
    }
    
    # if ( my $name = $request->clean_param('name') )
    # {
    # 	$name .= '%' if defined $is_auto && $is_auto eq 'auto';
    # 	my $quoted = $dbh->quote($name);
    # 	push @filters, "cs.name like $quoted";
    # }
    
    # Check for parameter 'rank'.
    
    if ( my $rank = $request->clean_param('rank') )
    {
	my $quoted = $dbh->quote($rank);
	push @filters, "cs.rank = $quoted";
    }
    
    return @filters;
}


sub generate_stratname_filter {
    
    my ($request, $mt, $names_ref) = @_;
    
    my $dbh = $request->get_connection;
    my (@unqualified, @clauses);
    my $negate;
    
    if ( $names_ref->[0] =~ qr{ ^ ! (.*) }xs )
    {
	$negate = 1; $names_ref->[0] = $1;
    }
    
    foreach my $name ( @$names_ref )
    {
	$name =~ s/^\s+//;
	$name =~ s/\s+$//;
	$name =~ s/\s+/ /g;
	
	next unless defined $name && $name ne '';
	
	if ( $name =~ qr{ ^ (.*?) \s+ (fm|mbr|gp) $ }xsi )
	{
	    $name = $1;
	    my $rank = $2;
	    
	    unless ( $name =~ qr{[a-z]}xi )
	    {
		$request->add_warning("bad value '$name' for parameter 'strat', must contain at least one letter");
		next;
	    }
	    
	    my $quoted = $dbh->quote($name);
	    
	    if ( lc $rank eq 'fm' )
	    {
		push @clauses, "(cs.name like $quoted and cs.rank = 'formation')";
	    }
	    
	    elsif ( lc $rank eq 'mbr' )
	    {
		push @clauses, "(cs.name like $quoted and cs.rank = 'member')";
	    }
	    
	    else # ( lc $rank eq 'gp' )
	    {
		push @clauses, "(cs.name like $quoted and cs.rank = 'group')";
	    }
	}
	
	elsif ( $name =~ qr{[%_]}xs )
	{
	    unless ( $name =~ qr{[a-z]}xi )
	    {
		$request->add_warning("bad value '$name' for parameter 'strat', must contain at least one letter");
		next;
	    }
	    
	    my $quoted = $dbh->quote($name);
	    push @clauses, "(cs.name like $quoted)";
	}
	
	else
	{
	    push @unqualified, $dbh->quote($name);
	}
    }
    
    if ( @unqualified )
    {
	push @clauses, "cs.name in (" . join(',', @unqualified) . ")";
    }
    
    # If no valid values were found, then add a clause that will select nothing.
    
    unless ( @clauses )
    {
	push @clauses, "cs.name = '--'";
    }
    
    my $clause = '(' . join( ' or ', @clauses ) . ')';
    $clause = "not " . $clause if $negate;
    
    return $clause;
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

    my ($request, $op, $mt, $tables_ref) = @_;
    
    my $dbh = $request->get_connection;
    my $taxonomy = $request->{my_taxonomy} ||= Taxonomy->new($dbh, 'taxon_trees');
    my @filters;
    
    # Check for parameter 'clust_id'
    
    if ( my @clusters = $request->safe_param_list('clust_id') )
    {
	my $id_list = $request->check_values($dbh, \@clusters, 'bin_id', 'coll_bins', 
					     "Unknown summary cluster '%'");
	
	# my $id_list = join(q{,}, @clusters);
	# my %id_hash = map { $_ => 1 } @clusters;
	
	# # Check for invalid identifiers.
	
	# unless ( $id_list eq '-1' )
	# {
	#     my $check_result = $dbh->selectcol_arrayref("
	# 	SELECT bin_id FROM coll_bins WHERE bin_id in ($id_list)");
	    
	#     foreach my $id ( @$check_result )
	#     {
	# 	delete $id_hash{$id};
	#     }
	    
	#     foreach my $id ( keys %id_hash )
	#     {
	# 	$request->add_warning("Unknown collection '$id'");
	#     }
	    
	#     my $id_list = join(q{,}, @$check_result) || '-1';
	# }
	
	# If there aren't any bins, or no valid cluster ids were specified,
	# include a filter that will return no results.
	
	if ( $op eq 'summary' )
	{
	    push @filters, "s.bin_id in ($id_list)";
	}
	
	else
	{
	    my %clusters;
	    my @clust_filters;
	    
	    foreach my $cl (@clusters)
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
	    
	    if ( @clusters == 0 || $MAX_BIN_LEVEL == 0 )
	    {
		push @filters, "c.collection_no = -1";
	    }
	    
	    else
	    {
		push @filters, @clust_filters;
	    }
	    
	    $tables_ref->{non_summary} = 1;
	}
    }
    
    # Check for parameter 'coll_id'
    
    # Check for parameter 'coll_id', If the parameter was given but no value was
    # found, then add a clause that will generate an empty result set.
    
    if ( my @colls = $request->safe_param_list('coll_id') )
    {
	my $id_list = $request->check_values($dbh, \@colls, 'collection_no', 'collections', 
					     "Unknown collection '%'");
	
	# If there aren't any bins, or no valid cluster ids were specified,
	# include a filter that will return no results.
	
	if ( $op eq 'summary' )
	{
	    $tables_ref->{non_summary} = 1;
	    $tables_ref->{cm} = 1;
	    push @filters, "cm.collection_no in ($id_list)";
	}
	
	else
	{
	    push @filters, "c.collection_no in ($id_list)";
	    $tables_ref->{non_summary} = 1;
	}
    }
    
    # Check for parameters 'base_name', 'taxon_name', 'match_name',
    # 'base_id', 'taxon_id'
    
    my ($taxon_name, @taxon_nos, $value, @values);
    my (@include_taxa, @exclude_taxa, $no_synonyms, $all_children, $do_match);
    my (@taxon_warnings);
    
    if ( $value = $request->clean_param('base_name') )
    {
	$taxon_name = $value;
	$all_children = 1;
	$no_synonyms = $request->clean_param('immediate');
    }
    
    elsif ( $value = $request->clean_param('match_name') )
    {
	$taxon_name = $value;
	$do_match = 1;
	$no_synonyms = 1;
    }
    
    elsif ( $value = $request->clean_param('taxon_name') )
    {
	$taxon_name = $value;
	$no_synonyms = $request->clean_param('immediate');
    }
    
    elsif ( @values = $request->safe_param_list('base_id') )
    {
	@taxon_nos = @values;
	$no_synonyms = $request->clean_param('immediate');
    }
    
    elsif ( @values = $request->safe_param_list('taxon_id') )
    {
	@taxon_nos = @values;
	$no_synonyms = $request->clean_param('immediate');
    }
    
    # If a name was specified, we start by resolving it.  The resolution is
    # slightly different for 'match_name' than for the others.
    
    if ( $taxon_name )
    {
	# If we are doing a syntactic match, get all matching names and ignore exclusions.
	
	if ( $do_match )
	{
	    my @taxa;
	    
	    try {
		@taxa = $taxonomy->resolve_names($taxon_name, { fields => 'RANGE', all_names => 1 });
	    }
	    
	    catch {
		print STDERR $taxonomy->last_sql . "\n\n" if $request->debug;
		die $_;
	    };
	    
	    push @taxon_warnings, $taxonomy->list_warnings;
	    
	    @include_taxa = grep { ! $_->{exclude} } @taxa;
	}
	
	# Otherwise, we get the best match for each given name (generally the
	# one with the most occurrences in the database).  We will need to
	# collect included and excluded names separately.
	
	else
	{
	    my @taxa;
	    
	    try {
		@taxa = $taxonomy->resolve_names($taxon_name, { fields => 'RANGE' });
	    }
	    
	    catch {
		print STDERR $taxonomy->last_sql . "\n\n" if $request->debug;
		die $_;
	    };
	    
	    push @taxon_warnings, $taxonomy->list_warnings;
	    
	    @include_taxa = grep { ! $_->{exclude} } @taxa;
	    @exclude_taxa = grep { $_->{exclude} } @taxa;
	    
	    # If 'exact' was specified, then we just use the names as specified.  But if we are
	    # going to be looking at the whole subtree, we also need to exclude junior synonyms
	    # explicitly.  This is because junior synonyms are linked into the tree as if they
	    # were children.
	    
	    if ( $no_synonyms )
	    {
		if ( $all_children )
		{
		    push @exclude_taxa, $taxonomy->list_taxa('juniors', \@include_taxa, { fields => 'RANGE' });
		}
	    }
	    
	    # Otherwise, we need to consider all synonyms.  If we are looking at the whole
	    # subtree, it is sufficient to just take the senior synonyms.
	    
	    elsif ( $all_children )
	    {
		@include_taxa = $taxonomy->list_taxa('senior', \@include_taxa, { fields => 'RANGE' });
		@exclude_taxa = $taxonomy->list_taxa('senior', \@exclude_taxa, { fields => 'RANGE' }) if @exclude_taxa;
	    }
	    
	    # Otherwise, we want all synonyms of the included taxa.  We can ignore exclusions.
	    
	    else
	    {
		@include_taxa = $taxonomy->list_taxa('synonyms', \@include_taxa, { fields => 'RANGE' });
	    }
	}
    }
    
    elsif ( @taxon_nos )
    {
	if ( $no_synonyms )
	{
	    @include_taxa = $taxonomy->list_taxa('exact', \@taxon_nos, { fields => 'RANGE' });
	}
	
	elsif ( $all_children )
	{
	    @include_taxa = $taxonomy->list_taxa('senior', \@taxon_nos, { fields => 'RANGE' });
	}
	
	else
	{
	    @include_taxa = $taxonomy->list_taxa('synonyms', \@taxon_nos, { fields => 'RANGE' });
	}
    }
    
    # Then get the records for excluded taxa.  But only if there are any
    # included taxa in the first place.
    
    if ( @include_taxa )
    {
	if ( my @exclude_nos = $request->clean_param_list('exclude_id') )
	{
	    push @exclude_taxa, $taxonomy->list_taxa('exact', \@exclude_nos, { fields => 'RANGE' });
	}
    }
    
    # Then construct the necessary filters for included taxa
    
    if ( @include_taxa && $all_children )
    {
	my $taxon_filters = join ' or ', map { "t.lft between $_->{lft} and $_->{rgt}" } @include_taxa;
	push @filters, "($taxon_filters)";
	$tables_ref->{tf} = 1;
	$tables_ref->{non_geo_filter} = 1;
	$request->{my_base_taxa} = \@include_taxa;
    }
    
    elsif ( @include_taxa )
    {
	my $taxon_list = join ',', map { $_->{orig_no} } @include_taxa;
	push @filters, "o.orig_no in ($taxon_list)";
	$tables_ref->{o} = 1 unless $tables_ref->{ds};
	$tables_ref->{non_geo_filter} = 1;
	$tables_ref->{non_summary} = 1;
	$request->{my_taxa} = \@include_taxa;
    }
    
    # If a name was given and no matching taxa were found, we need to query by
    # genus_name/species_name instead.  But if the operation is "prevalence"
    # or "diversity" then just abort with a warning.
    
    elsif ( $taxon_name )
    {
	if ( $op eq 'prevalence' || $op eq 'diversity' )
	{
	    $request->add_warning(@taxon_warnings) if @taxon_warnings;
	    return "t.lft = -1";
	}
	
	my @exact_genera;
	my @name_clauses;
	
	my @raw_names = ref $taxon_name eq 'ARRAY' ? @$taxon_name : split qr{\s*,\s*}, $taxon_name;
	
	foreach my $name ( @raw_names )
	{
	    # Ignore any name with a selector
	    
	    next if $name =~ qr{:};
	    
	    # Remove exclusions
	    
	    $name =~ s{ \^ .* }{}xs;
	    
	    # Then test for syntax and add the corresponding filters.
	    
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
	$tables_ref->{non_summary} = 1;
    }
    
    # If a number was given but it does not exist in the hierarchy, add a
    # filter that will guarantee no results.
    
    elsif ( $request->param_given('base_name') || $request->param_given('base_id') ||
	    $request->param_given('taxon_name') || $request->param_given('taxon_id') )
    {
	push @filters, "o.orig_no = -1";
	$tables_ref->{non_summary} = 1;
    }

    # Now add filters for excluded taxa.  But only if there is at least one
    # included taxon as well.
    
    if ( @exclude_taxa && @include_taxa )
    {
	push @filters, map { "t.lft not between $_->{lft} and $_->{rgt}" } @exclude_taxa;
	$request->{my_excluded_taxa} = \@exclude_taxa;
	$tables_ref->{tf} = 1;
    }
    
    # If any warnings occurreed, pass them on.
    
    $request->add_warning(@taxon_warnings) if @taxon_warnings;
    
    # Check for parameter 'cc'
    
    if ( my @ccs = $request->clean_param_list('cc') )
    {
	push @ccs, $request->clean_param_list('continent');
	
	if ( $ccs[0] eq '_' )
	{
	    push @filters, "c.collection_no = -1";
	}
	else
	{
	    my (@cc2, @cc3, @cc2x, @cc3x, $exclude);
	    
	    foreach my $value (@ccs)
	    {
		if ( $value =~ qr{ ^ \^ (.*) }xs )
		{
		    $exclude = 1;
		    $value = $1;
		}
		
		if ( length($value) == 2 && $exclude ) {
		    push @cc2x, $value;
		} elsif ( length($value) == 2 ) {
		    push @cc2, $value;
		} elsif ( $exclude ) {
		    push @cc3x, $value;
		} else {
		    push @cc3, $value;
		}
	    }
	    
	    my @cc_filters;
	    
	    if ( @cc2 )
	    {
		push @cc_filters, "c.cc in ('" . join("','", @cc2) . "')";
	    }
	    
	    if ( @cc3 )
	    {
		push @cc_filters, "ccmap.continent in ('" . join("','", @cc3) . "')";
		$tables_ref->{ccmap} = 1;
	    }
	    
	    my $cc_string = '(' . join(' or ', @cc_filters) . ')';
	    
	    push @filters, $cc_string if $cc_string ne '()';
	    
	    if ( @cc2x )
	    {
		push @filters, "c.cc not in ('" . join("','", @cc2x) . "')";
	    }
	    
	    if ( @cc3x )
	    {
		push @filters, "ccmap.continent not in ('" . join("','", @cc3x) . "')";
		$tables_ref->{ccmap} = 1;
	    }
	}
	
	$tables_ref->{non_summary} = 1;
    }
    
    # if ( my @continents = $request->clean_param_list('continent') )
    # {
    # 	if ( $continents[0] eq '_' )
    # 	{
    # 	    push @filters, "c.collection_no = 0";
    # 	}
    # 	else
    # 	{
    # 	    my $cont_list = "'" . join("','", @continents) . "'";
    # 	    push @filters, "ccmap.continent in ($cont_list)";
    # 	    $tables_ref->{ccmap} = 1;
    # 	}
    # 	$tables_ref->{non_summary} = 1;
    # }
    
    # Check for parameters 'lngmin', 'lngmax', 'latmin', 'latmax', 'loc',
    
    my $x1 = $request->clean_param('lngmin');
    my $x2 = $request->clean_param('lngmax');
    my $y1 = $request->clean_param('latmin');
    my $y2 = $request->clean_param('latmax');
    
    # If longitude bounds were specified, create a bounding box using them.
    
    if ( $x1 ne '' && $x2 ne '' && ! ( $x1 == -180 && $x2 == 180 ) )
    {
	# If no latitude bounds were specified, set them to -90, 90.
	
	$y1 = -90.0 unless defined $y1 && $y1 ne '';
	$y2 = 90.0 unless defined $y2 && $y2 ne '';
	
	# If the longitude coordinates do not fall between -180 and 180,
	# adjust them so that they do.
	
	if ( $x1 < -180.0 )
	{
	    $x1 = $x1 + ( POSIX::floor( (180.0 - $x1) / 360.0) * 360.0);
	}
	
	if ( $x2 < -180.0 )
	{
	    $x2 = $x2 + ( POSIX::floor( (180.0 - $x2) / 360.0) * 360.0);
	}
	
	if ( $x1 > 180.0 )
	{
	    $x1 = $x1 - ( POSIX::floor( ($x1 + 180.0) / 360.0 ) * 360.0);
	}
	
	if ( $x2 > 180.0 )
	{
	    $x2 = $x2 - ( POSIX::floor( ($x2 + 180.0) / 360.0 ) * 360.0);
	}
	
	# If $x1 < $x2, then we query on a single bounding box defined by
	# those coordinates.
	
	if ( $x1 <= $x2 )
	{
	    $request->add_warning("The values of 'lngmin' and 'lngmax' are equal, " .
				  "so only records with that exact longitude will be selected")
		if $x1 == $x2;
	    
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
    
    # If only latitude bounds were specified then create a bounding box
    # with longitude ranging from -180 to 180.
    
    elsif ( $y1 ne '' || $y2 ne '' && ! ( $y1 == -90 && $y2 == 90 ) )
    {
	# If one of the bounds was not specified, set it to -90 or 90.
	
	$y1 = -90.0 unless defined $y1 && $y1 ne '';
	$y2 = 90.0 unless defined $y2 && $y2 ne '';
	
	my $polygon = "'POLYGON((-180.0 $y1,180.0 $y1,180.0 $y2,-180.0 $y2,-180.0 $y1))'";
	push @filters, "contains(geomfromtext($polygon), $mt.loc)";
    }
    
    # If the latitude bounds are such as to select no records, then add a warning.
    
    if ( defined $y1 && defined $y2 && $y1 >= 90.0 && $y2 >= 90.0 )
    {
	$request->add_warning("The latitude bounds lie beyond +90 degrees, so no records will be selected");
    }
    
    elsif ( defined $y2 && defined $y2 && $y1 <= -90.0 && $y2 <= -90.0 )
    {
	$request->add_warning("The latitude bounds lie beyond -90 degrees, so no records will be selected");
    }
    
    # If 'loc' was specified, use that geometry.
    
    if ( my $loc = $request->clean_param('loc') )
    {
	push @filters, "contains(geomfromtext($loc), $mt.loc)";
    }
    
    # Check for parameter 'plate'
    
    my $plate_param = $request->clean_param('plate');
    my @pgm = $request->clean_param_list('pgm');
    
    if ( $plate_param && $plate_param ne '' )
    {
	my ($model, $exclude);
	
	if ( $plate_param =~ qr{ ^ \s* ( [gs] )? \^ (.*) $ }xsi )
	{
	    $exclude = 1;
	    $plate_param = ($1 // '') . $2;
	}
	
	if ( $plate_param =~ qr{ ^ ( [gs] ) (.*) }xsi )
	{
	    $model = uc $1;
	    $plate_param = $2;
	}
	
	elsif ( $plate_param =~ qr{ ^ [^0-9] }xs )
	{
	    die "400 Bad value '$plate_param' for parameter 'plate' - must start wtih 'G', 'S', or a plate number\n";
	}
	
	elsif ( @pgm && $pgm[0] eq 'scotese' )
	{
	    $model = 'S';
	}
	
	else
	{
	    $model = 'G';
	}
	
	my @raw_ids = split qr{[\s,]+}, $plate_param;
	my @plate_ids;
	
	foreach my $id ( @raw_ids )
	{
	    next unless defined $id && $id ne '';
	    
	    if ( $id =~ qr{ ^ [0-9]+ $ }xs )
	    {
		push @plate_ids, $id;
	    }
	    
	    else
	    {
		$request->add_warning("Bad value '$id' for 'plate': must be a positive integer");
	    }
	}
	
	my $not = $exclude ? 'not ' : '';
	
	unless (@plate_ids)
	{
	    push @plate_ids, -1;
	    $not = '';
	}
	
	my $plate_list = join(',', @plate_ids);
	
	if ( $model eq 'S' )
	{
	    push @filters, "c.s_plate_no ${not}in ($plate_list)";
	}
	
	else
	{
	    push @filters, "c.g_plate_no ${not}in ($plate_list)";
	}
	
	$tables_ref->{non_summary} = 1;
    }
    
    # Check for parameters 'envtype', 'envzone'
    
    my @envtype = $request->clean_param_list('envtype');
    my @envzone = $request->clean_param_list('envzone');
    
    my $eexclude;
    my $zexclude;
    
    if ( $enviros[0] =~ qr{ ^ \^ (.*) } )
    {
	$enviros[0] = $1;
	$eexclude = 1;
    }
    
    if ( $envzones[0] =~ qr{ ^ \^ (.*) } )
    {
	$envzones[0] = $1;
	$zexclude = 1;
    }    
    
    foreach my $e ( @enviros, @envzones )
    
    # Check for parameters 'p_lngmin', 'p_lngmax', 'p_latmin', 'p_latmax', 'p_loc',
    
    # my $px1 = $request->clean_param('lngmin');
    # my $px2 = $request->clean_param('lngmax');
    # my $py1 = $request->clean_param('latmin');
    # my $py2 = $request->clean_param('latmax');
    
    # if ( defined $px1 && defined $px2 )
    # {
    # 	$py1 //= -90.0;
    # 	$py2 //= 90.0;
	
    # 	# If the longitude coordinates do not fall between -180 and 180, adjust
    # 	# them so that they do.
	
    # 	if ( $px1 < -180.0 )
    # 	{
    # 	    $px1 = $px1 + ( POSIX::floor( (180.0 - $px1) / 360.0) * 360.0);
    # 	}
	
    # 	if ( $px2 < -180.0 )
    # 	{
    # 	    $px2 = $px2 + ( POSIX::floor( (180.0 - $px2) / 360.0) * 360.0);
    # 	}
	
    # 	if ( $px1 > 180.0 )
    # 	{
    # 	    $px1 = $px1 - ( POSIX::floor( ($px1 + 180.0) / 360.0 ) * 360.0);
    # 	}
	
    # 	if ( $px2 > 180.0 )
    # 	{
    # 	    $px2 = $px2 - ( POSIX::floor( ($px2 + 180.0) / 360.0 ) * 360.0);
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
    
    # if ( $request->{clean_params}{loc} )
    # {
    # 	push @filters, "contains(geomfromtext($request->{clean_params}{loc}), $mt.loc)";
    # }
    
    # Check for parameters 'formation', 'stratgroup', 'member'
    
    if ( my @formations = $request->clean_param_list('formation') )
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
	$tables_ref->{non_summary} = 1;
    }
    
    if ( my @stratgroups = $request->clean_param_list('stratgroup') )
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
	$tables_ref->{non_summary} = 1;
    }
    
    if ( my @members = $request->clean_param_list('member') )
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
	$tables_ref->{non_summary} = 1;
    }
    
    # Check for parameter 'strat'.
    
    if ( my @strata = $request->clean_param_list('strat') )
    {
	push @filters, $request->generate_stratname_filter('cs', \@strata);
	$tables_ref->{cs} = 1;
	$tables_ref->{non_summary} = 1;
    }
    
    # Check for parameters , 'interval_id', 'interval', 'min_ma', 'max_ma'.
    # If no time rule was given, it defaults to 'buffer'.
    
    my $time_rule = $request->clean_param('timerule') || 'major';
    # my $summary_interval = 0;
    # my ($early_age, $late_age, $early_bound, $late_bound);
    # my $interval_no = $request->clean_param('interval_id') + 0;
    # my $interval_name = $request->clean_param('interval');
    # my $earlybuffer = $request->clean_param('earlybuffer');
    # my $latebuffer = $request->clean_param('latebuffer');
    
    # Check for interval parameters.
    
    my ($early_age, $late_age, $early_interval_no, $late_interval_no) = $request->process_interval_params;
    
    my $early_bound = $early_age;
    my $late_bound = $late_age;
    my $buffer;
    
    if ( $early_age )
    {
	$buffer = $early_age > 66 ? 12 : 5;
	$early_bound = $early_age + $buffer;
	$late_bound = $late_age - $buffer;
    }
    
    my $summary_interval = 0;
    
    # If the requestor wants to override the time bounds, do that.
    
    if ( my $earlybuffer = $request->clean_param('earlybuffer') )
    {
	$early_bound = $early_age + $earlybuffer;
    }
    
    if ( my $latebuffer = $request->clean_param('latebuffer') )
    {
	$late_bound = $late_age - $latebuffer;
	$late_bound = 0 if $late_bound < 0;
    }
    
    # If $late_bound is less than zero, correct it to zero.
    
    $late_bound = 0 if defined $late_bound && $late_bound < 0;
    
    # If we are querying for summary clusters, and only one interval was
    # specified, we can use the cluster table row corresponding to that
    # interval number.  But only if timerule is the default value of 'buffer'.
    # Otherwise, we need the unrestricted cluster table row (the one for
    # interval_no = 0) plus additional filters.
    
    if ( ($op eq 'summary' || $op eq 'prevalence') && $time_rule eq 'buffer' &&
	 ($early_interval_no && $late_interval_no && $early_interval_no == $late_interval_no) )
    {
	push @filters, "s.interval_no = $summary_interval";
    }
    
    # Otherwise, if a range of years was specified, use that.
    
    # else
    # {
    # 	my $max_ma = $request->clean_param('max_ma');
    # 	my $min_ma = $request->clean_param('min_ma');
	
    # 	if ( $max_ma && $min_ma )
    # 	{
    # 	    my $range = $max_ma - $min_ma;
    # 	    my $buffer = $range * 0.5;
	    
    # 	    $early_age = $max_ma + 0;
    # 	    $early_bound = defined $earlybuffer ? 
    # 		$early_age + $earlybuffer :
    # 		    $early_age + $buffer;
	
    # 	    $late_age = $max_ma + 0;
    # 	    $late_bound = defined $latebuffer ?
    # 		$late_age - $latebuffer :
    # 		    $late_age - $buffer;
	
    # 	    $late_bound = 0 if $late_bound < 0;
    # 	}
	
    # 	# Otherwise, handle either a min or max filter alone.
	
    # 	elsif ( $max_ma )
    # 	{
    # 	    $early_age = $max_ma + 0;
    # 	    $early_bound = $early_age;
    # 	}
	
    # 	if ( $max_ma )
    # 	{
    # 	    $late_age = $min_ma + 0;
    # 	    $late_bound = $late_age;
    # 	}
    # }
    
    # Then, if a time filter was specified and we need one, apply it.  If we
    # are were given a summary interval and no non-geographic filters were
    # specified, then we don't need one because the necessary filtering has
    # already been done by selecting the appropriate interval_no in the summary table.
    
    elsif ( $early_age || $late_age )
    {
	unless ( ($op eq 'summary' and not $tables_ref->{non_geo_filter} and $time_rule eq 'buffer') or
	         $tables_ref->{ds} or $op eq 'prevalence' )
	{
	    $tables_ref->{c} = 1;
	    
	    # The exact filter we use will depend upon the time rule that was
	    # selected.
	    
	    if ( $time_rule eq 'contain' )
	    {
		if ( defined $late_age and $late_age > 0 )
		{
		    push @filters, "$mt.late_age >= $late_age";
		}
		
		if ( defined $early_age and $early_age > 0 )
		{
		    push @filters, "$mt.early_age <= $early_age";
		}
	    }
	    
	    elsif ( $time_rule eq 'overlap' )
	    {
		if ( defined $late_age and $late_age > 0 )
		{
		    push @filters, "$mt.early_age > $late_age";
		}
		
		if ( defined $early_age and $early_age > 0 )
		{
		    push @filters, "$mt.late_age < $early_age";
		}
	    }
	    
	    elsif ( $time_rule eq 'major' )
	    {
		my $ea = ($early_age + 0 || 5000);
		my $la = $late_age + 0;
		
		push @filters, "if($mt.late_age >= $la,
			if($mt.early_age <= $ea, $mt.early_age - $mt.late_age, $ea - $mt.late_age),
			if($mt.early_age > $ea, $ea - $la, $mt.early_age - $la)) / ($mt.early_age - $mt.late_age) >= 0.5"
	    }
	    
	    else # $time_rule eq 'buffer'
	    {
		if ( defined $late_age and defined $early_age and 
		     defined $late_bound and defined $early_bound )
		{
		    push @filters, "$mt.early_age <= $early_bound and $mt.late_age >= $late_bound";
		    push @filters, "($mt.early_age < $early_bound or $mt.late_age > $late_bound)";
		    push @filters, "$mt.early_age > $late_age";
		    push @filters, "$mt.late_age < $early_age";
		}
		
		else
		{
		    if ( defined $late_age and defined $late_bound )
		    {
			push @filters, "$mt.late_age >= $late_bound and $mt.early_age > $late_age";
		    }
		    
		    if ( defined $early_age and defined $early_bound )
		    {
			push @filters, "$mt.early_age <= $early_bound and $mt.late_age < $early_age";
		    }
		}
	    }
	}
	
	$request->{early_age} = $early_age;
	$request->{late_age} = $late_age;
	$tables_ref->{non_summary} = 1;
    }
    
    # Return the list
    
    return @filters;
}


# adjustCoordinates ( fields_ref )
# 
# Alter the output coordinate fields to match the longitude/latitude bounds.

sub adjustCoordinates {

    my ($request, $fields_ref) = @_;
    
    my $x1 = $request->clean_param('lngmin');
    my $x2 = $request->clean_param('lngmax');
    
    return unless $x1 || $x2;
    
    # Adjust the output coordinates to fall within the range indicated by the
    # input parameters.
    
    my $x1_offset = 0;
    my $x2_offset = 0;
    
    if ( $x1 < -180.0 )
    {
	$x1_offset = -1 * POSIX::floor( (180.0 - $x1) / 360.0) * 360.0;
    }
    
    elsif ( $x1 > 180.0 )
    {
	$x1_offset = POSIX::floor( ($x1 + 180.0) / 360.0 ) * 360.0;
    }
    
    if ( $x2 < -180.0 )
    {
	$x2_offset = -1 * POSIX::floor( (180.0 - $x2) / 360.0) * 360.0;
    }
    
    elsif ( $x2 > 180.0 )
    {
	$x2_offset = POSIX::floor( ($x2 + 180.0) / 360.0 ) * 360.0;
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


# For the 'prevalence' operation, this one should be tried first.  If the only
# parameters provided are interval and taxon, then we return a simple set of
# filters that will make an easy query.  Otherwise, we return the empty list
# which will result in the full filter routine above being called.

sub generatePrevalenceFilters {

    my ($request, $tables_ref) = @_;
    
    my $dbh = $request->{dbh};
    my $taxonomy = $request->{my_taxonomy} ||= Taxonomy->new($dbh, 'taxon_trees');
    
    # If any 'complicated' parameters were specified, return the empty list.
    
    my $x1 = $request->clean_param('lngmin');
    my $x2 = $request->clean_param('lngmax');
    my $y1 = $request->clean_param('latmin');
    my $y2 = $request->clean_param('latmax');
    
    return () if defined $x1 & $x1 ne '' && $x1 != -180 ||
	defined $x2 && $x2 ne '' && $x2 != 180 ||
	    defined $y1 && $y1 ne '' && $y1 != -90 ||
		defined $y2 && $y2 ne '' && $y2 != 90;
    
    return () if $request->clean_param('clust_id') || $request->clean_param('cc') ||
	$request->clean_param('continent') || $request->clean_param('loc') ||
	    $request->clean_param('plate') || $request->clean_param('formation') ||
		$request->clean_param('stratgroup') || $request->clean_param('member') || 
		    $request->clean_param('max_ma') || $request->clean_param('min_ma');
    
    # Otherwise, we can proceed to construct a filter list.
    
    my @filters;
    
    my $interval_no = $request->clean_param('interval_id') + 0;
    my $interval_name = $request->clean_param('interval');
    
    if ( $interval_name )
    {
	my $quoted_name = $dbh->quote($interval_name);
	
	my $sql = "
		SELECT interval_no FROM $INTERVAL_DATA
		WHERE interval_name like $quoted_name";
	
	($interval_no) = $dbh->selectrow_array($sql);
	
	unless ( $interval_no )
	{
	    $request->add_warning("unknown time interval '$interval_name'");
	    $interval_no = -1;
	}
    }
    
    $interval_no ||= 751;
    
    push @filters, "p.interval_no = $interval_no";
    
    my $base_name = $request->clean_param('base_name');
    my @base_nos = $request->clean_param_list('base_id');
    my @exclude_nos = $request->clean_param_list('exclude_id');
    
    my (@taxa, @includes, @excludes);
    
    if ( $base_name )
    {
	@taxa = $taxonomy->resolve_names($base_name, { fields => 'RANGE' });
    }
    
    elsif ( @base_nos )
    {
	@taxa = $taxonomy->list_taxa_simple(\@base_nos, { fields => 'RANGE' });
    }
    
    if ( @exclude_nos )
    {
	push @taxa, $taxonomy->list_taxa_simple(\@exclude_nos, { fields => 'RANGE', exclude => 1 });
    }
    
    # Now add these to the proper list.
    
    foreach my $t (@taxa)
    {
	if ( $t->{exclude} )
	{
	    push @excludes, "t.lft between $t->{lft} and $t->{rgt}";
	}
	
	else
	{
	    push @includes, "t.lft between $t->{lft} and $t->{rgt}";
	}
    }
    
    if ( @includes )
    {
	push @filters, '(' . join(' or ', @includes) . ')';
    }
    
    if ( @excludes )
    {
	push @filters, 'not (' . join(' or ', @excludes) . ')';
    }
    
    $request->{my_base_taxa} = \@taxa;
    
    return @filters;
}


# selectPaleoModel ( fields_ref, tables_ref )
# 
# Adjust the field list and table hash to select the proper paleocoordinate
# fields according to the parameter 'pgm'.

sub selectPaleoModel {
    
    my ($request, $fields_ref, $tables_ref) = @_;
    
    # Go through each specified paleogeographicmodel and construct a list of the necessary
    # fields.  If no models were specified, use 'gplates' as the default.
    
    my @models = $request->clean_param_list('pgm');
    
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
	    push @fields, "'scotese' as $model_field";
	    $tables_ref->{cc} = 1;
	    $plate_version_shown{'scotese'} = 1;
	}
	
	elsif ( $model eq 'gplates' || $model eq 'gp_mid' )
	{
	    push @fields, "pc.mid_lng as $lng_field", "pc.mid_lat as $lat_field";
	    push @fields, "pc.plate_no as $plate_field" unless $plate_version_shown{'gplates'};
	    push @fields, "'gp_mid' as $model_field";
	    $tables_ref->{pc} = 1;
	    $plate_version_shown{'gplates'} = 1;
	}
	
	elsif ( $model eq 'gp_early' )
	{
	    push @fields, "pc.early_lng as $lng_field", "pc.early_lat as $lat_field";
	    push @fields, "pc.plate_no as $plate_field" unless $plate_version_shown{'gplates'};
	    push @fields, "'gp_early' as $model_field";
	    $tables_ref->{pc} = 1;
	    $plate_version_shown{'gplates'} = 1;
	}
	
	elsif ( $model eq 'gp_late' )
	{
	    push @fields, "pc.late_lng as $lng_field", "pc.late_lat as $lat_field";
	    push @fields, "pc.plate_no as $plate_field" unless $plate_version_shown{'gplates'};
	    push @fields, "'gp_late' as $model_field";
	    $tables_ref->{pc} = 1;
	    $plate_version_shown{'gplates'} = 1;
	}
    }
    
    # Now substitute this list into the field string.
    
    my $paleofields = join(", ", @fields);
    
    $$fields_ref =~ s/PALEOCOORDS/$paleofields/;
    
    # Delete unnecessary output fields.
    
    foreach my $i (2..4)
    {
	next if $i <= $model_no;
	
	$request->delete_output_field("paleomodel$i");
	$request->delete_output_field("paleolng$i");
	$request->delete_output_field("paleolat$i");
	$request->delete_output_field("geoplate$i");
    }
}


# adjustPCIntervals ( string... )
# 
# Adjust the specified strings (portions of SQL statements) to reflect the
# proper interval endpoint when selecting or displaying paleocoordinates.

sub adjustPCIntervals {
    
    my ($request, @stringrefs) = @_;
    
    # Each of the subsequent arguments are references to strings to be
    # altered.
    
    my $selector = $request->clean_param('pcis');
    
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
    
    my ($request, $tables, $options) = @_;
    
    $options ||= {};
    my $at = $options->{at} || 'c';
    my $bt = $options->{bt} || 'cc';
    my $tt = $options->{tt};
    
    my $order = $request->clean_param('order');
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
	    my ($pgm) = $request->clean_param_list('pgm');
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


# generate_strata_order_clause ( options )
# 
# Return the order clause for the list of strata, or the empty string if
# none was selected.

sub generate_strata_order_clause {
    
    my ($request, $tables, $options) = @_;
    
    $options ||= {};
    my $at = $options->{at} || 'c';
    my $bt = $options->{bt} || 'cc';
    my $tt = $options->{tt};
    
    my @terms = $request->clean_param_list('order');
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
	
	if ( $term eq 'max_ma' )
	{
	    $dir ||= 'desc';
	    push @exprs, "c.early_age $dir";
	}
	
	elsif ( $term eq 'min_ma' )
	{
	    $dir ||= 'desc';
	    push @exprs, "c.late_age $dir";
	}
	
	elsif ( $term eq 'name' )
	{
	    push @exprs, "coalesce(cc.geological_group, cc.formation, cc.member) $dir, cc.formation $dir, cc.member $dir"
	}
	
	elsif ( $term eq 'n_occs' )
	{
	    $dir ||= 'desc';
	    push @exprs, "n_occs $dir";
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

    my ($request, $mt, $tables) = @_;
    
    my $join_list = '';
    
    # Return an empty string unless we actually have some joins to make
    
    return $join_list unless ref $tables eq 'HASH' and %$tables;
    
    # Some tables imply others.
    
    $tables->{o} = 1 if ($tables->{t} || $tables->{tf} || $tables->{oc}) && ! $tables->{ds};
    $tables->{c} = 1 if $tables->{o} || $tables->{pc} || $tables->{cs};
    
    # Create the necessary join expressions.
    
    $join_list .= "JOIN occ_matrix as o on o.collection_no = c.collection_no\n"
	if $tables->{o};
    $join_list .= "JOIN occurrences as oc using (occurrence_no)\n"
	if $tables->{oc};
    $join_list .= "JOIN taxon_trees as t using (orig_no)\n"
	if $tables->{t} || $tables->{tf};
    $join_list .= "JOIN coll_map as cm using (bin_id)\n"
	if $tables->{cm};
    $join_list .= "JOIN coll_strata as cs on cs.collection_no = c.collection_no\n"
	if $tables->{cs};
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
    
    my ($request, $record) = @_;
    
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


# process_methods ( record )
# 
# Process some of the fields for '1.2:colls:methods', appending units to
# values. 

sub process_methods {
    
    my ($request, $record) = @_;
    
    if ( $record->{collection_size} && $record->{collection_size_unit} )
    {
	$record->{collection_size} .= " " . $record->{collection_size_unit};
    }
    
    if ( $record->{rock_censused} && $record->{rock_censused_unit} )
    {
	$record->{rock_censused} .= " " . $record->{rock_censused_unit};
    }
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

    my ($request, $record) = @_;
    
    return $BASIS_CODE{$record->{llb}||''} . $PREC_CODE{$record->{llp}||''};
}


# valid_cc ( )
# 
# Validate values for the 'cc' parameter.  These must be either ISO-3166-1 codes or continent
# codes from our database.  Values may start with ^, indicating exclusion.

my $country_error = "bad value {value} for {param}: must be a country code from ISO-3166-1 alpha-2 or a 3-letter continent code";

sub valid_cc {
    
    my ($value, $context) = @_;
    
    # Start with a simple syntactic check.
    
    return { error => $country_error }
	unless $value =~ qr{ ^ ( \^? ) ( [a-z]{2,3} ) $ }xsi;
    
    my $exclude = $1 // '';
    my $code = $2;
    
    # Then check it against the database.
    
    my $valid = exists $COUNTRY_NAME{uc $code} || $CONTINENT_NAME{uc $code};
    
    return $valid ? { value => $exclude . uc $code }
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


# Set up the hashes that are used to evaluate the parameters 'environment' and
# 'envzone'. 

$ENVALUE{terr} = {'terrestrial indet.' => 1, 'fluvial indet.' => 1, 'alluvial fan' => 1, 'channel lag' => 1, 'coarse channel fill' => 1, 'fine channel fill' => 1, '"channel"' => 1, 'wet floodplain' => 1, 'dry floodplain' => 1, '"floodplain"' => 1, 'crevasse splay' => 1, 'levee' => 1, 'mire/swamp' => 1, 'fluvial-lacustrine indet.' => 1, 'delta plain' => 1, 'fluvial-deltaic indet.' => 1, 'lacustrine - large' => 1, 'lacustrine - small' => 1, 'pond' => 1, 'crater lake' => 1, 'lacustrine delta plain' => 1, 'lacustrine interdistributary bay' => 1, 'lacustrine delta front' => 1, 'lacustrine prodelta' => 1, 'lacustrine deltaic indet.' => 1, 'lacustrine indet.' => 1, 'dune' => 1, 'interdune' => 1, 'loess' => 1, 'eolian indet.' => 1, 'cave' => 1, 'fissure fill' => 1, 'sinkhole' => 1, 'karst indet.' => 1, 'tar' => 1, 'mire/swamp' => 1, 'spring' => 1, 'glacial' => 1};

$ENVALUE{carbonate} = {'carbonate indet.' => 1, 'peritidal' => 1, 'shallow subtidal indet.' => 1, 'open shallow subtidal' => 1, 'lagoonal/restricted shallow subtidal' => 1, 'sand shoal' => 1, 'reef => 1, buildup or bioherm' => 1, 'perireef or subreef' => 1, 'intrashelf/intraplatform reef' => 1, 'platform/shelf-margin reef' => 1, 'slope/ramp reef' => 1, 'basin reef' => 1, 'deep subtidal ramp' => 1, 'deep subtidal shelf' => 1, 'deep subtidal indet.' => 1, 'offshore ramp' => 1, 'offshore shelf' => 1, 'offshore indet.' => 1, 'slope' => 1, 'basinal (carbonate)' => 1, 'basinal (siliceous)' => 1 };

$ENVALUE{silici} = {'marginal marine indet.' => 1, 'coastal indet.' => 1, 'estuary/bay' => 1, 'lagoonal' => 1, 'paralic indet.' => 1, 'delta plain' => 1, 'interdistributary bay' => 1, 'delta front' => 1, 'prodelta' => 1, 'deltaic indet.' => 1, 'foreshore' => 1, 'shoreface' => 1, 'transition zone/lower shoreface' => 1, 'offshore' => 1, 'coastal indet.' => 1, 'submarine fan' => 1, 'basinal (siliciclastic)' => 1, 'basinal (siliceous)' => 1, 'basinal (carbonate)' => 1, 'deep-water indet.' => 1 };

$ENVALUE{lacust} = {'lacustrine - large' => 1, 'lacustrine - small' => 1, 'pond' => 1, 'crater lake' => 1, 'lacustrine delta plain' => 1, 'lacustrine interdistributary bay' => 1, 'lacustrine delta front' => 1, 'lacustrine prodelta' => 1, 'lacustrine deltaic indet.' => 1, 'lacustrine indet.' => 1 };

$ENVALUE{fluvial} = {'fluvial indet.' => 1, 'alluvial fan' => 1, 'channel lag' => 1, 'coarse channel fill' => 1, 'fine channel fill' => 1, '"channel"' => 1, 'wet floodplain' => 1, 'dry floodplain' => 1, '"floodplain"' => 1, 'crevasse splay' => 1, 'levee' => 1, 'mire/swamp' => 1, 'fluvial-lacustrine indet.' => 1, 'delta plain' => 1, 'fluvial-deltaic indet.' => 1 };

$ENVALUE{karst} = {'cave' => 1, 'fissure fill' => 1, 'sinkhole' => 1, 'karst indet.' => 1 };

$ENVALUE{terrother} = {'dune' => 1, 'interdune' => 1, 'loess' => 1, 'eolian indet.' => 1, 'tar' => 1, 'spring' => 1, 'glacial' => 1 };

$ENVALUE{marginal} = {'marginal marine indet.' => 1, 'peritidal' => 1, 'lagoonal/restricted shallow subtidal' => 1, 'estuary/bay' => 1, 'lagoonal' => 1, 'paralic indet.' => 1, 'delta plain' => 1, 'interdistributary bay' => 1 };

$ENVALUE{reef} = {'reef => 1,  buildup or bioherm' => 1, 'perireef or subreef' => 1, 'intrashelf/intraplatform reef' => 1, 'platform/shelf-margin reef' => 1, 'slope/ramp reef' => 1, 'basin reef' => 1 };

$ENVALUE{stshallow} = {'shallow subtidal indet.' => 1, 'open shallow subtidal' => 1, 'delta front' => 1, 'foreshore' => 1, 'shoreface' => 1, 'sand shoal' => 1 }

$ENVALUE{stdeep} = {'transition zone/lower shoreface' => 1, 'deep subtidal ramp' => 1, 'deep subtidal shelf' => 1, 'deep subtidal indet.' => 1 };

$ENVALUE{offshore} = {'offshore ramp' => 1, 'offshore shelf' => 1, 'offshore indet.' => 1, 'prodelta' => 1, 'offshore' => 1 };

$ENVALUE{slope} = {'slope' => 1, 'basinal (carbonate)' => 1, 'basinal (siliceous)' => 1, 'submarine fan' => 1, 'basinal (siliciclastic)' => 1, 'basinal (siliceous)' => 1, 'basinal (carbonate)' => 1, 'deep-water indet.' => 1 };

$ENVALUE{marindet} = {'marine indet.' => 1, 'carbonate indet.' => 1, 'coastal indet.' => 1, 'deltaic indet.' => 1 };


# prune_field_list ( )
# 
# This routine is called as a hook after the request is configured.  It
# deletes any unnecessary fields from the field list, so they will not show up
# in fixed-format output.

sub prune_field_list {
    
    my ($request) = @_;
    
    my $field_list = $request->output_field_list;
    
    # If the '1.2:colls:paleoloc' block is selected, then trim any unused
    # fields.
    
    if ( $request->block_selected('1.2:colls:paleoloc') )
    {
	my (@pgmodels) = $request->clean_param_list('pgm');
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


sub process_coll_com {
    
    my ($request, $record) = @_;
    
    foreach my $f ( qw(collection_no) )
    {
	$record->{$f} = generate_identifier('COL', $record->{$f}) if defined $record->{$f};
	# $record->{$f} = $record->{$f} ? "$IDP{COL}:$record->{$f}" : '';
    }
    
    foreach my $f ( qw(interval_no) )
    {
	$record->{$f} = generate_identifier('INT', $record->{$f}) if defined $record->{$f};
	# $record->{$f} = $record->{$f} ? "$IDP{INT}:$record->{$f}" : '';
    }
    
    $record->{reference_no} = generate_identifier('REF', $record->{reference_no})
	if defined $record->{reference_no};
}


# validate latitude and longitude values

sub COORD_VALUE {
    
    my ($dir) = @_;
    
    croak "COORD_VALUE argument must be 'lat' or 'lng'" unless
	defined $dir && ($dir eq 'lat' || $dir eq 'lng');
    
    return sub { return coord_value(shift, shift, $dir) };
}


sub coord_value {
    
    my ($value, $context, $dir) = @_;
   
    # The value may be a decimal number, optionally preceded by a sign.
    
    if ( $value =~ qr{ ^ [+-]? (?: \d+\.\d* | \d*\.\d+ | \d+ )$}xs )
    {
	return { value => $value + 0 };
    }
    
    # The value may also be a non-negative decimal number followed by one of E, W,
    # N, or S.  This suffix must correspond with the value of $dir.
    
    elsif ( $dir eq 'lat' && $value =~ qr{ ^ ( \d+.\d* | \d*\.\d+ | \d+ ) ( [nNsS] ) $ }xs )
    {
	if ( lc $2 eq 'n' ) {
	    return { value => 0 + $value };
	} else {
	    return { value => 0 - $value };
	}
    }
    
    elsif ( $dir eq 'lng' && $value =~ qr{ ^ ( \d+.\d* | \d*\.\d+ | \d+ ) ( [eEwW] ) $ }xs )
    {
	if ( lc $2 eq 'e' ) {
	    return { value => 0 + $value };
	} else {
	    return { value => 0 - $value };
	}
    }
    
    # Otherwise, the value is bad.
    
    else
    {
	my $suffix = $dir eq 'lat' ? "an unsigned decimal number followed by 'N' or 'S'"
	    : "an unsigned decimal number followed by 'E' or 'W'";
	
	return { error => "bad value '$value' for {param}: must be a decimal number with optional sign or $suffix" };
    }
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
