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

use TableDefs qw($COLL_MATRIX $COLL_BINS $COLL_LITH $COLL_STRATA $COUNTRY_MAP $PALEOCOORDS $GEOPLATES
		 $INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP $INTERVAL_BUFFER $PVL_MATRIX);
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);
use Taxonomy;

use Try::Tiny;
use Carp qw(carp croak);

use Moo::Role;

no warnings 'numeric';


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::ConfigData PB2::ReferenceData PB2::IntervalData PB2::TaxonData);

our ($MAX_BIN_LEVEL) = 0;
our (%COUNTRY_NAME, %CONTINENT_NAME);
our (%ETVALUE, %EZVALUE);
our (%LITH_VALUE, %LITH_QUOTED, %LITHTYPE_VALUE);


# initialize ( )
# 
# This routine is called once by Web::DataService in order to initialize this
# class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    use utf8;
    
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
	{ value => 'full', maps_to => '1.2:colls:full_info' },
	    "This is a shortcut for including all of the information that defines this record.  Currently, this",
	    "includes the following blocks: C<B<attr>>, C<B<loc>>,",
	    "C<B<paleoloc>>, C<B<prot>>, C<B<stratext>>, C<B<lithext>>, C<B<geo>>, C<B<ctaph>>,",
	    "C<B<comps>>, C<B<methods>>, C<B<rem>>, C<B<refattr>>.",
	    "If we later add new data fields to the collection records, these will be included",
	    "by this block.  Therefore, if you are publishing a URL, it might be a good idea",
	    "to include B<C<show=full>>.",
        { value => 'loc', maps_to => '1.2:colls:loc' },
	    "Additional information about the geographic locality of the collection",
	{ value => 'bin', maps_to => '1.2:colls:bin' },
	    "The list of geographic clusters to which the collection belongs.",
	{ value => 'paleoloc', maps_to => '1.2:colls:paleoloc' },
	    "Information about the paleogeographic locality of the collection,",
	    "evaluated according to the model(s) specified by the parameter C<pgm>.",
	{ value => 'prot', maps_to => '1.2:colls:prot' },
	    "Indicate whether the collection is on protected land",
        { value => 'time', maps_to => '1.2:colls:time' },
	    "This block is includes the field 'cx_int_no', which is needed by Navigator.",
	{ value => 'timebins', maps_to => '1.2:colls:timebins' },
	    "Shows a list of temporal bins into which each occurrence falls according",
	    "to the timerule selected for this request. You may select one using the",
	    "B<C<timerule>> parameter, or it will default to C<B<major>>.",
	{ value => 'timecompare', maps_to => '1.2:colls:timecompare' },
	    "Like B<C<timebins>>, but shows this information for all available",
	    "timerules.",
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
	{ value => 'env', maps_to => '1.2:colls:env' },
	    "The paleoenvironment associated with this collection.",
	{ value => 'geo', maps_to => '1.2:colls:geo' },
	    "Information about the geological context of the collection (includes C<env>).",
	{ value => 'ctaph', maps_to =>'1.2:colls:taphonomy' },
	    "Information about the taphonomy of the collection and the mode of",
	    "preservation of the constituent fossils.",
	{ value => 'comps', maps_to => '1.2:colls:components' },
	    "Information about the various kinds of body parts and other things",
	    "found as part of this collection.",
	{ value => 'methods', maps_to => '1.2:colls:methods' },
	    "Information about the collection methods used",
        { value => 'rem', maps_to => '1.2:colls:rem', undocumented => 1 },
	    "Any additional remarks that were entered about the collection.",
	{ value => 'resgroup', maps_to => '1.2:colls:group' },
	    "The research group(s), if any, associated with this collection.",
        { value => 'ref', maps_to => '1.2:refs:primary' },
	    "The primary reference for the collection, as formatted text.",
        { value => 'refattr', maps_to => '1.2:refs:attr' },
	    "The author(s) and year of publication of the primary reference for the collection.",
	{ value => 'secref', maps_to => '1.2:colls:secref' },
	    "Include the identifiers of the secondary references for the collection.",
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the collection record");
    
    # Then a second map for geographic summary clusters.
    
    $ds->define_output_map('1.2:colls:summary_map' =>
	{ value => 'bin' },
	    "The list of larger-scale clusters to which this cluster belongs.",
        { value => 'ext', maps_to => '1.2:colls:ext' },
	    "Additional information about the geographic extent of each cluster.",
        { value => 'time', maps_to => '1.2:colls:time' },
	  # This block is defined in our parent class, CollectionData.pm
	    "Additional information about the temporal range of the",
	    "cluster.");
    
    # Then a map for geological strata.
    
    $ds->define_output_map('1.2:strata:basic_map' =>
	{ value => 'coords', maps_to => '1.2:strata:coords' },
	    "The geographic bounds (latitude and longitude) of the selected",
	    "occurrences in each stratum",
	{ value => 'gplates', maps_to => '1.2:strata:gplates' },
	    "The identifier(s) of the geological plate(s) on which the selected",
	    "occurrences in each strataum are located, from the GPlates model",
	{ value => 'splates', maps_to => '1.2:strata:splates' },
	    "The identifier(s) of the geological plate(s) on which the selected",
	    "occurrences in each strataum are located, from the Scotese model");
    
    # Then define the output blocks which these mention.
    
    $ds->define_block('1.2:colls:basic' =>
	{ select => ['c.collection_no', 'cc.collection_name', 'cc.collection_subset', 'cc.collection_aka',
		     'cc.formation', 'c.lat', 'c.lng', 'c.n_occs', 'c.early_age', 'c.late_age',
		     'ei.interval_name as early_interval', 'li.interval_name as late_interval',
		     'c.reference_no'], 
	  tables => ['cc', 'ei', 'li', 'sr', 'r'] },
	{ output => 'collection_no', dwc_name => 'collectionID', com_name => 'oid' },
	    "A unique identifier for the collection.  This will be a string if the result",
	    "format is JSON.  For backward compatibility, all identifiers in text format",
	    "results will continue to be integers.",
	{ output => 'record_type', com_name => 'typ', value => $IDP{COL} },
	    "The type of this object: C<$IDP{COL}> for a collection",
	{ output => 'permissions', com_name => 'prm' },
	    "The accessibility of this record.  If empty, then the record is",
	    "public.  Otherwise, the value of this record will be one",
	    "of the following:", "=over",
	    "=item members", "The record is accessible to database members only.",
	    "=item authorizer", "The record is accessible to its authorizer group,",
	    "and to any other authorizer groups given permission.",
	    "=item group(...)", "The record is accessible to",
	    "members of the specified research group(s) only.",
	    "=back",
	{ set => 'permissions', from => '*', code => \&process_permissions },
	{ output => 'formation', com_name => 'sfm', not_block => 'strat' },
	    "The formation in which the collection was found",
	{ output => 'lng', dwc_name => 'decimalLongitude', com_name => 'lng', data_type => 'dec' },
	    "The longitude at which the collection is located (in degrees)",
	{ output => 'lat', dwc_name => 'decimalLatitude', com_name => 'lat', data_type => 'dec' },
	    "The latitude at which the collection is located (in degrees)",
	{ output => 'collection_name', dwc_name => 'collectionCode', com_name => 'nam' },
	    "The name which identifies the collection, not necessarily unique",
	{ output => 'collection_subset', com_name => 'nm2' },
	    "If the collection is a part of another one, this field specifies which part",
	{ output => 'collection_aka', com_name => 'aka' },
	    "An alternate name for the collection, or additional remarks about it.",
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
	{ output => 'ref_author', dwc_name => 'recordedBy', com_name => 'aut', if_block => '1.2:refs:attr' },
	    "The attribution (author and year) of the collection",
	{ output => 'ref_pubyr', com_name => 'pby', if_block => '1.2:refs:attr' },
	    "The year in which the collection was published",
	{ set => '*', code => \&fixTimeOutput },
	# { set => 'reference_no', from => '*', code => \&set_collection_refs, if_block => 'secref' },
	{ output => 'reference_no', com_name => 'rid' },
	    "The identifier of the primary reference from which this data was entered.",
	{ output => 'reference_nos', com_name => 'rfs', if_block => '1.2:colls:secref' },
	    "A list of identifiers of all of the references associated with this collection.",
	    "In general, these include the primary reference plus any other references from which",
	    "occurrences were entered.",
	{ set => '*', code => \&process_coll_ids });
    
    my (@bin_levels, @bin_fields);
    
    foreach my $level ( reverse 1..$MAX_BIN_LEVEL )
    {
	push @bin_levels, { output => "bin_id_$level", com_name => "lv$level" };
	push @bin_levels, "The identifier of the level-$level cluster in which the collection or cluster is located";
	push @bin_fields, "c.bin_id_$level";
    }
    
    $ds->define_block('1.2:colls:bin' => 
	{ select =>  \@bin_fields, tables => [ 'c' ] },
	@bin_levels);
    
    $ds->define_block('1.2:colls:rem');	# this block is deprecated, but we don't want to return an error
                                        # if specified
    
    $ds->define_block('1.2:colls:name' =>
	{ select => ['cc.collection_name', 'cc.collection_subset', 'cc.collection_aka' ],
	  tables => ['cc'] },
	{ output => 'collection_name', dwc_name => 'collectionCode', com_name => 'cnm' },
	    "An arbitrary name which identifies the collection, not necessarily unique",
	{ output => 'collection_subset', com_name => 'cns' },
	    "If the collection is a part of another one, this field specifies which part",
	{ output => 'collection_aka', com_name => 'aka' },
	    "An alternate name for the collection, or additional remarks about it.");
    
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
	    "L<list of basis and precision codes|node:general/basis_precision>.  This field and",
	    "the next are only included in responses using the pbdb vocabulary.",
	{ output =>'latlng_precision', if_vocab => 'pbdb' },
	    "The precision of the collection coordinates.  Follow the above",
	    "link for a list of the code values.",
	{ set => 'prc', from => '*', code => \&generateBasisCode, if_vocab => 'com' },
	{ output => 'prc', pbdb_name => '', com_name => 'prc', if_vocab => 'com' },
	    "A two-letter code indicating the basis and precision of the geographic coordinates.",
	    "This field is reported instead of C<latlng_basis> and C<latlng_precision> in",
	    "responses that use the compact vocabulary.  Follow the above link for a list of the code values.",
	{ output => 'geogscale', com_name => 'gsc' },
	    "The geographic scale of the collection.",
	{ output => 'geogcomments', com_name => 'ggc' },
	    "Additional comments about the geographic location of the collection");

     $ds->define_block('1.2:colls:paleoloc' =>
	{ select => 'PALEOCOORDS' },
	{ set => '*', code => \&process_paleocoords },
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
    
    $ds->define_block('1.2:colls:time' =>
	{ select => 'im.cx_int_no', tables => 'im' },
	{ output => 'cx_int_no', com_name => 'cxi' },
    	    "The identifier of the most specific single interval from the selected timescale that",
    	    "covers the entire time range associated with the collection or cluster.");
    
    $ds->define_block('1.2:colls:timebins' =>
	{ set => '*', code => \&generate_timebins },
	{ output => 'time_bins', com_name => 'tbl' },
	    "A list of time intervals into which this occurrence or collection is placed",
	    "according to the timerule selected for this operation. You can see which",
	    "rule is selected by including the B<C<datainfo>> parameter. A value of",
	    "- means that the time range is too large to match any bin under",
	    "this timerule.");
    
    $ds->define_block('1.2:colls:timecompare' =>
	{ set => '*', code => \&generate_timecompare },
	{ output => 'time_contain', com_name => 'tbc' },
	    "List of time intervals into which this occurrence or collection would be placed",
	    "according to the C<B<contain>> timerule, or - if the range is too large.",
	{ output => 'time_major', com_name => 'tbm' },
	    "List of time intervals into which this occurrence or collection would be placed",
	    "according to the C<B<major>> timerule, or - if the range is too large.",
	{ output => 'time_buffer', com_name => 'tbb' },
	    "List of time intervals into which this occurrence or collection would be placed",
	    "according to the C<B<buffer>> timerule, or - if the range is too large.",
	{ output => 'time_overlap', com_name => 'tbo' },
	    "List of time intervals into which this occurrence or collection would be placed",
	    "according to the C<B<overlap>> timerule.");
    
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
    
    $ds->define_block('1.2:colls:env' =>
	{ select => [ qw(cc.environment) ], tables => 'cc' },
	{ output => 'environment', com_name => 'env', not_block => '1.2:colls:geo' },
	    "The paleoenvironment associated with the collection site");
    
    $ds->define_block('1.2:colls:taphonomy' =>
	{ select => [ qw(cc.pres_mode cc.spatial_resolution cc.temporal_resolution cc.lagerstatten
			cc.concentration cc.orientation cc.preservation_quality cc.bioerosion
			cc.abund_in_sediment cc.sorting cc.fragmentation cc.encrustation
			cc.preservation_comments) ], tables => 'cc' },
	{ output => 'pres_mode', com_name => 'tpm' },
	    "This field reports the modes of preservation, occurrence, and mineralization",
	    "from the 'Preservation' tab on the PBDB collection form.",
	{ output => 'preservation_quality', com_name => 'tpq' },
	    "Quality of the anatomical detail preserved.",
	{ output => 'spatial_resolution', com_name => 'tps' },
	    "Spatial resolution of the preservation information.",
	{ output => 'temporal_resolution', com_name => 'tpt' },
	    "Temporal resolution of the preservation information.",
	{ output => 'lagerstatten', com_name => 'tpl' },
	    "Type of lagerstätten found in this collection.",
	{ output => 'concentration', com_name => 'tpc' },
	    "Degree of concentration of the fossils found in this collection.",
	{ output => 'orientation', com_name => 'tpo' },
	    "Orientation of the fossil(s) found in this collection.",
	{ output => 'abund_in_sediment', com_name => 'tpa' },
	    "Abundance in sediment",
	{ output => 'sorting', com_name => 'tpr' },
	    "Degree of size sorting",
	{ output => 'fragmentation', com_name => 'tpf' },
	    "Degree of fragmentation",
	{ output => 'bioerosion', com_name => 'tpb' },
	    "Degree of bioerosion",
	{ output => 'encrustation', com_name => 'tpe' },
	    "Degree of encrustation",
	{ output => 'preservation_comments', com_name => 'pcm' },
	    "Preservation comments, if any.");
    
    $ds->define_block('1.2:colls:components' =>
	{ select => [ qw(cc.assembl_comps cc.articulated_parts cc.associated_parts
			 cc.common_body_parts cc.rare_body_parts cc.feed_pred_traces
			 cc.artifacts cc.component_comments) ], tables => 'cc' },
	{ output => 'assembl_comps', com_name => 'cps' },
	    "The size classes found in this collection.  The value of this field",
	    "will be one or more of: C<B<macrofossils>, C<B<mesofossils>>, C<B<microfossils>>.",
	{ output => 'articulated_parts', com_name => 'cpa' },
	    "The prevalence of articulated body parts in this collection.",
	{ output => 'associated_parts', com_name => 'cpb' },
	    "The prevalence of associated body parts in this collection.",
	{ output => 'common_body_parts', com_name => 'cpc' },
	    "A list of body parts that are common in this collection.",
	{ output => 'rare_body_parts', com_name => 'cpd' },
	    "A list of body parts that are rare in this collection.",
	{ output => 'feed_pred_traces', com_name => 'cpt' },
	    "A list of feeding/predation traces found in this collection, if any.",
	{ output => 'artifacts', com_name => 'cpf' },
	    "A list of artifacts found in this collection, if any.",
	{ output => 'component_comments', com_name => 'acm' },
	    "Component comments, if any.");
    
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
    
    $ds->define_block('1.2:colls:secref' =>
	{ select => ['group_concat(distinct sr.reference_no order by sr.reference_no) as reference_nos'] });
    
    $ds->define_block( '1.2:colls:full_info' =>
	{ include => '1.2:colls:loc' },
	{ include => '1.2:colls:paleoloc' },
	{ include => '1.2:colls:prot' },
	{ include => '1.2:colls:stratext' },
	{ include => '1.2:colls:lithext' },
	{ include => '1.2:colls:geo' },
	{ include => '1.2:colls:components' },
	{ include => '1.2:colls:taphonomy' },
	{ include => '1.2:colls:methods' },
	{ include => '1.2:refs:attr' });
    
    # Then define an output block for displaying stratigraphic results
    
    $ds->define_block('1.2:strata:basic' =>
	{ select => ['max(c.early_age) as max_ma', 'min(c.late_age) as min_ma',
		     'count(*) as n_colls', 'sum(cs.n_occs) as n_occs', 
		     'group_concat(distinct cs.cc) as cc_list',
		     'group_concat(distinct cs.lithology) as lithology']},
	{ output => 'record_type', com_name => 'typ', value => 'str' },
	    "The type of this record: 'str' for a stratum",
	{ output => 'grp', com_name => 'sgr', pbdb_name => 'group' },
	    "The stratigraphic group associated with this stratum, if any.",
	{ output => 'formation', com_name => 'sfm' },
	    "The stratigraphic formation associated with this stratum, if any.",
	{ output => 'member', com_name => 'smb' },
	    "The stratigraphic member associated with this stratum, if any.",
	{ output => 'lithology', com_name => 'lth' },
	    "The litholog(ies) recorded for this stratum in the database, if any.",
        { output => 'max_ma', com_name => 'eag' },
	    "The early bound of the geologic time range associated with the selected",
	    "occurrences (in Ma)",
	{ output => 'min_ma', com_name => 'lag' },
	    "The late bound of the geologic time range associated with the selected",
	    "occurrences (in Ma)",
	{ set => '*', code => \&fixTimeOutput },
	{ output => 'cc_list', com_name => 'cc2' },
	    "A comma-separated list of the country codes in which this stratum is found",
	{ output => 'n_colls', com_name => 'nco', data_type => 'pos' },
	    "The number of fossil collections in the database that are associated with this stratum.",
	    "Note that if your search is limited to a particular geographic area, then",
	    "only collections within the selected area are counted.",
	{ output => 'n_occs', com_name => 'noc', data_type => 'pos' },
	    "The number of fossil occurrences in the database that are associated with this stratum.",
	    "The above note about geographic area selection also applies.");
    
    $ds->define_block('1.2:strata:occs' =>
	{ select => ['count(distinct c.collection_no) as n_colls',
		     'count(*) as n_occs', 'cs.grp', 'cs.formation', 'cs.member',
		     'min(c.early_age) as max_ma', 'min(c.late_age) as min_ma',
		     'group_concat(distinct cs.cc) as cc_list',
		     'group_concat(distinct cs.lithology) as lithology'],
	  tables => [ 'cs' ] },
	{ output => 'record_type', com_name => 'typ', value => 'str' },
	    "The type of this record: 'str' for a stratum",
	{ output => 'grp', com_name => 'sgr', pbdb_name => 'group' },
	    "The name of the group in which occurrences were found",
	{ output => 'formation', com_name => 'sfm' },
	    "The name of the formation in which occurences were found",
	{ output => 'member', com_name => 'smb' },
	    "The name of the member in which occurrences were found",
	{ output => 'lithology', com_name => 'lth' },
	    "The litholog(ies) recorded for this stratum in the database, if any.",
	{ output => 'max_ma', com_name => 'eag' },
	    "The early bound of the geologic time range associated with the selected occurrences (in Ma)",
	{ output => 'min_ma', com_name => 'lag' },
	    "The late bound of the geologic time range associated with the selected occurrences (in Ma)",
	{ set => '*', code => \&fixTimeOutput },
	{ output => 'cc_list', com_name => 'cc2' },
	    "A comma-separated list of the country codes in which this stratum is found",
	{ output => 'n_colls', com_name => 'nco', data_type => 'pos' },
	    "The number of fossil collections in the database from this",
	    "stratum that contain occurrences from the selected set and",
	    "are listed as being part of this stratum",
	{ output => 'n_occs', com_name => 'noc', data_type => 'pos' },
	    "The number of fossil occurrences in the database that are",
	    "listed as being part of this stratum");
    
    $ds->define_block('1.2:strata:coords' =>
	{ select => [ 'min(c.lat) as min_lat', 'min(c.lng) as min_lng',
		      'max(c.lat) as max_lat', 'max(c.lng) as max_lng' ] },
	{ output => 'min_lng', com_name => 'lx1', pbdb_name => 'lng_min' },
	    "The minimum longitude for selected occurrences in this stratum",
	{ output => 'max_lng', com_name => 'lx2', pbdb_name => 'lng_max' },
	    "The maximum longitude for selected occurrences in this stratum",
	{ output => 'min_lat', com_name => 'ly1', pbdb_name => 'lat_min' },
	    "The minimum latitude for selected occurrences in this stratum",
	{ output => 'max_lat', com_name => 'ly2', pbdb_name => 'lat_max' },
	    "The maximum latitude for selected occurrences in this stratum");
    
    $ds->define_block('1.2:strata:gplates' =>
	{ select => [ 'group_concat(distinct c.g_plate_no) as gplate_no' ] },
	{ output => 'gplate_no', com_name => 'gpl' },
	    "The identifier(s) of the geological plate(s) on which the selected",
	    "occurrences in this stratum lie, from the GPlates model.");
    
    $ds->define_block('1.2:strata:splates' =>
	{ select => [ 'group_concat(distinct c.s_plate_no) as splate_no' ] },
	{ output => 'splate_no', com_name => 'scp' },
	    "The identifier(s) of the geological plate(s) on which the selected",
	    "occurrences in this stratum lie, from the Scotese model.");
    
    $ds->define_block('1.2:strata:auto' =>
	{ select => [ 'name', 'type', 'n_colls', 'n_occs', 'cc_list' ] },
	{ output => 'record_type', com_name => 'typ', value => 'str' },
	    "The type of this record: 'str' for a stratum",
	{ output => 'name', com_name => 'nam' },
	    "The name of a matching stratum",
	{ output => 'type', com_name => 'rnk' },
	    "The type of stratum: group, formation, or member.",
	{ output => 'cc_list', com_name => 'cc2' },
	    "The country or countries in which this stratum lies, as ISO-3166 country codes.",
	{ output => 'n_colls', com_name => 'nco', data_type => 'pos' },
	    "The number of fossil collections in the database that are associated with this stratum.",
	    "Note that if your search is limited to a particular geographic area, then",
	    "only collections within the selected area are counted.",
	{ output => 'n_occs', com_name => 'noc', data_type => 'pos' },
	    "The number of fossil occurrences in the database that are associated with this stratum.",
	    "The above note about geographic area selection also applies.");
    
    # And a block for basic geographic summary cluster info
    
    $ds->define_block( '1.2:colls:summary' =>
      { select => ['s.bin_id', 's.n_colls', 's.n_occs', 's.lat', 's.lng'] },
      { set => '*', code => \&process_summary_ids },
      { output => 'bin_id', com_name => 'oid' }, 
	  "A unique identifier for the cluster.",
      { output => 'bin_id_3', com_name => 'lv3', if_block => 'bin' },
	  "The identifier of the containing level-3 cluster, if any",
      { output => 'bin_id_2', com_name => 'lv2', if_block => 'bin' }, 
	  "The identifier of the containing level-2 cluster, if any",
      { output => 'bin_id_1', com_name => 'lv1', if_block => 'bin' }, 
	  "The identifier of the containing level-1 cluster, if any",
      { output => 'record_type', com_name => 'typ', value => $IDP{CLU} },
	  "The type of this object: C<$IDP{CLU}> for a collection cluster",
      { output => 'n_colls', com_name => 'nco', data_type => 'pos' },
	  "The number of collections from the selected set that map to this cluster",
      { output => 'n_occs', com_name => 'noc', data_type => 'pos' },
	  "The number of occurrences from the selected set that map to this cluster",
      { output => 'lng', com_name => 'lng', data_type => 'dec' },
	  "The longitude of the centroid of this cluster",
      { output => 'lat', com_name => 'lat', data_type => 'dec' },
	  "The latitude of the centroid of this cluster");
    
    # Plus one for summary cluster extent
    
    $ds->define_block( '1.2:colls:ext' =>
      { select => ['s.lng_min', 'lng_max', 's.lat_min', 's.lat_max', 's.std_dev'] },
      { output => 'lng_min', com_name => 'lx1', pbdb_name => 'min_lng', data_type => 'dec' },
	  "The mimimum longitude for collections in this cluster",
      { output => 'lng_max', com_name => 'lx2', pbdb_name => 'max_lng', data_type => 'dec' },
	  "The maximum longitude for collections in this cluster",
      { output => 'lat_min', com_name => 'ly1', pbdb_name => 'min_lat', data_type => 'dec' },
	  "The mimimum latitude for collections in this cluster",
      { output => 'lat_max', com_name => 'ly2', pbdb_name => 'max_lat', data_type => 'dec' },
	  "The maximum latitude for collections in this cluster",
      { output => 'std_dev', com_name => 'std' },
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
    
    $ds->define_set('1.2:occs:abund_type' =>
	{ value => 'count' },
	    "Select only occurrences with an abundance type of 'individuals', 'specimens'",
	    "'grid-count', 'elements', or 'fragments'",
	{ value => 'coverage' },
	    "Select only occurrences with an abundance type of '%-...'",
	{ value => 'any' },
	    "Select only occurrences with some type of abundance information");
    
    $ds->define_set('1.2:occs:ident_type' =>
	{ value => 'latest' },
	    "Select only the latest identification of each occurrence, and",
	    "ignore any previous ones.  This is the default.",
	{ value => 'orig' },
	    "Select only the original identification of each occurrence, and",
	    "ignore any later ones.",
	{ value => 'reid' },
	    "Select all identifications of occurrences that have been reidentified,",
	    "including the original.  Ignore occurrences for which no",
	    "reidentification has been entered in the database.  This may",
	    "result in multiple records being returned each occurrence.",
	    "I<Note, however, that if you also specify a taxon name then identifications",
	    "that do not fall under that name will be ignored.>  You can find these",
	    "by specifically querying for the occurrences you are interested in by identifier,",
	    "with C<B<idtype=all>>.",
	{ value => 'all' },
	    "Select every identification that matches the other",
	    "query parameters.  This may result in multiple records being",
	    "returned for a given occurrence.  See also the note given for",
	    "C<B<reid>> above.");
    
    $ds->define_set('1.2:occs:ident_single' =>
	{ value => 'orig' },
	    "Select the original identification of the specified occurrence.",
	{ value => 'latest' },
	    "Select the latest identification of the specified occurrence.");
    
    # $ds->define_set('1.2:occs:ident_qualification' =>
    # 	{ value => 'any' },
    # 	    "Select all occurrences regardless of modifiers.  This is the default.",
    # 	{ value => 'certain' },
    # 	    "Exclude all occurrences marked with any of the following modifiers:",
    # 	    "C<aff.> / C<cf.> / C<?> / C<\"\"> / C<informal> / C<sensu lato>.",
    # 	{ value => 'genus_certain' },
    # 	    "Like C<B<certain>>, but look only at the genus/subgenus and ignore species modifiers.",
    # 	{ value => 'uncertain' },
    # 	    "Select only occurrences marked with one of the following modifiers:",
    # 	    "C<aff.> / C<cf.> / C<?> / C<\"\"> / C<sensu lato> / C<informal>.",
    # 	{ value => 'new' },
    # 	    "Select only occurrences marked with one of the following:",
    # 	    "C<n. gen.> / C<n. subgen.> / C<n. sp.>");
    
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
    
    $ds->define_set('1.2:colls:envtype' =>
	{ value => 'terr' },
	    "Any terrestrial environment",
	{ value => 'marine' },
	    "Any marine environment",
	{ value => 'carbonate' },
	    "Carbonate environment",
	{ value => 'silicic' },
	    "Siliciclastic environment",
	{ value => 'unknown' },
	    "Unknown or indeterminate environment",
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
	{ value => 'marindet' },
	    "Marine indeterminate zone");
     
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
	{ value => 'marindet' },
	    "Marine indeterminate zone",
	{ value => 'unknown' },
	    "Unknown or indeterminate environment");
    
    $ds->define_ruleset('1.2:main_selector' =>
	{ param => 'clust_id', valid => VALID_IDENTIFIER('CLU'), list => ',' },
	    "Return only records associated with the specified geographic clusters.",
	    "You may specify one or more cluster ids, separated by commas.",
	{ param => 'coll_match', valid => ANY_VALUE },
	    "A string which will be matched against the C<collection_name> and",
	    "C<collection_aka> fields.  Records will be returned only if they belong to a",
	    "matching collection.  This string may contain the wildcards C<%> and C<_>.",
	    "In fact, it will probably not match anything unless you include a C<%> at the",
	    "beginning and/or the end.",
	{ param => 'coll_re', valid => ANY_VALUE },
	    "This is like B<C<coll_match>>, except that it takes a regular expression.",
	    "You can specify two or more alternatives separated by",
	    "the vertical bar character C<|>, and you can use all of the other standard",
	    "regular expression syntax including the backslash C<\\>.",
	{ at_most_one => [ 'coll_match', 'coll_re' ] },
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
	    "the wildcards C<%> and C<_>, and occurrences associated with all matching names will",
	    "be returned.  Synonyms will be ignored.  This is a syntactic rather",
	    "than a taxonomic match.",
	# { param => 'match_ident', valid => ANY_VALUE },
	#     "Return only records whose identification matches the specified name(s).",
	#     "You may specify multiple names, separated by commas.  Names may include",
	#     "the wildcards C<%> and C<_>.  This parameter differs from C<B<match_name>>",
	#     "in that its value is matched against the exact identification as opposed",
	#     "to the corresponding record from the taxonomy table.  This allows you, for example, to",
	#     "select identifications using old combinations that are no longer accepted.",
	{ optional => 'immediate', valid => FLAG_VALUE },
	    "You may specify this parameter along with F<base_name>, F<base_id>, or F<taxon_name>.",
	    "If you do, then synonyms of the specified name(s) will",
	    "be ignored.  No value is necessary for this parameter, just include the parameter name.",
	{ param => 'base_id', valid => VALID_IDENTIFIER('TID'), list => ',' },
	    "Return only records associated with the specified taxa,",
	    "I<including all subtaxa and synonyms>.  You may specify multiple taxon identifiers,",
	    "separated by commas.  Note that you may specify at most one of B<C<taxon_name>>,",
	    "B<< C<taxon_id> >>, B<C<< base_name >>>, B<C<base_id>>.",
	{ param => 'taxon_id', valid => VALID_IDENTIFIER('TID'), list => ','},
	    "Return only records associated with the specified taxa, not including",
	    "subtaxa or synonyms.  You may specify multiple taxon identifiers, separated by commas.",
	{ at_most_one => ['taxon_name', 'taxon_id', 'base_name', 'base_id'] },
	{ optional => 'exclude_id', valid => VALID_IDENTIFIER('TID'), list => ','},
	    "Exclude any records whose associated taxonomic name is a child of the given name or names,",
	    "specified by taxon identifier.  This is an alternative to the use of the C<^> character",
	    "in names.",
	{ param => 'idreso', valid => '1.2:taxa:resolution', alias => 'taxon_reso' },
	    "Select only occurrences that are identified to the specified taxonomic",
	    "resolution, and possibly lump together occurrences of the same genus or",
	    "family.  Accepted values are:",
	{ optional => 'idtype', valid => '1.2:occs:ident_type', alias => ['ident', 'ident_type'] },
	    "This parameter specifies how re-identified occurrences should be treated.",
	    "Allowed values include:",
	{ optional => 'idqual', valid => '1.2:taxa:ident_qualification' },
	    "This parameter selects or excludes occurrences based on their taxonomic modifiers.",
	    "Allowed values include:",
	{ optional => 'idmod', valid => ANY_VALUE },
	    "This parameter selects or excludes occurrences based on any combination of",
	    "taxonomic modifiers.  You can use this parameter and/or C<B<idgen>> and C<B<idspc>>",
	    "if you need to select a combination of modifiers not available through C<B<idqual>>.",
	    "You can specify one or more of the following codes, separated by commas.",
	    "If the first one is preceded by C<!> then they are excluded.",
	    "otherwise, only occurrences marked with at least one are included:", 
	    "=over",
	    "=item ns", "n. sp.", "=item ng", "n. gen. or n. subgen.",
	    "=item af", "aff.", "=item cf", "cf.", "=item sl", "sensu lato", "=item if", "informal",
	    "=item eg", "ex gr.", "=item qm", "question mark (?)", "=item qu", "quotes (\"\")",
	    "=back",
	{ optional => 'idgenmod', valid => ANY_VALUE },
	    "This parameter selects or excludes occurrences based on any combination of taxonomic",
	    "modifiers on the genus and/or subgenus name.  See C<B<idmod>> above.",
	{ optional => 'idspcmod', valid => ANY_VALUE },
	    "This parameter selects or excludes occurrences based on any combination of taxonomic",
	    "modifiers on the species name.  See C<B<idmod>> above.",
	{ param => 'abundance', valid => ANY_VALUE },
	    "This parameter selects only occurrences that have particular kinds of abundance",
	    "values.  Accepted values are:", $ds->document_set('1.2:occs:abund_type'),
	    "You may also append a colon followed by a decimal number.  This will select",
	    "only occurrences whose abundance is at least the specified minimum value.",
	{ param => 'lngmin', valid => COORD_VALUE('lng') },
	{ param => 'lngmax', valid => COORD_VALUE('lng') },
	    "Return only records whose present longitude falls within the given bounds.",
	    "If you specify one of these parameters then you must specify both.",
	    "If you provide bounds outside of the range -180\N{U+00B0} to 180\N{U+00B0}, they will be",
	    "wrapped into the proper range.  For example, if you specify C<lngmin=270 & lngmax=360>,",
	    "the query will be processed as if you had said C<lngmin=-90 & lngmax=0 >.  In this",
	    "case, I<all longitude values in the query result will be adjusted to fall within the actual",
	    "numeric range you specified.>",
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
	    "of this parameter starts with C<!>, then all records on the specified plates",
	    "are instead excluded.  If the value of this parameter continues with C<G>,",
	    "then the values will be interpreted as plate numbers",
	    "from the GPlates model.  If C<S>, then they will be",
	    "interpreted as plate numbers from the Scotese model.  Otherwise, they will",
	    "be interpreted according to the value of the parameter C<pgm>.",
	    "The remainder of the value must be a list of plate numbers.",
	{ optional => 'pgm', valid => $ds->valid_set('1.2:colls:pgmodel'), list => ',' },
	    "Specify which paleogeographic model(s) to use when evaluating paleocoordinates.",
	    "You may specify one or more from the following list, separated by commas.",
	    "If you do not specify a value for this parameter, the default model is C<gplates>.",
	    $ds->document_set('1.2:colls:pgmodel'),
	{ param => 'cc', valid => \&valid_cc, list => ',', alias => 'country', bad_value => '_' },
	    "Return only records whose location falls within the specified geographic regions.",
	    "The value of this parameter should be one or more",
	    "L<two-character country codes|http://en.wikipedia.org/wiki/ISO_3166-1_alpha-2> and/or ",
	    "L<three-character continent codes|op:config.txt?show=continents> as a comma-separated list.",
	    "If the parameter value starts with C<!>, then records falling into these regions are excluded",
	    "instead of included.  Any country codes starting with C<^> are",
	    "subtracted from the filter.  For example:", "=over",
	    "=item ATA,AU", "Select occurrences from Antarctica and Australia",
	    "=item NOA,SOA,^AR,^BO", "Select occurrences from North and South America, but not Argentina or Bolivia",
	    "=item !EUR,^IS", "Exclude occurrences from Europe, except those from Iceland", "=back",
	{ param => 'state', valid => ANY_VALUE, list => ',', bad_value => '_' },
	    "Return only records from collections that are indicated as falling within the specified",
	    "state or province. This information is not recorded for all collections, and has not",
	    "been checked for accuracy. Given that state names are sometimes duplicated between",
	    "countries, it is recommended to also specify the country using the B<C<cc>> parameter.",
	{ param => 'county', valid => ANY_VALUE, list => ',', bad_value => '_' },
	    "Return only records from collections that are indicated as falling within the specified",
	    "county or other sub-state administrative division. This information is not recorded",
	    "for all collections, and has not been checked for accuracy. Given that county names are",
	    "often duplicated between states and countries, it is recommended that you also specify",
	    "the state using the B<C<state>> parameter and the country using the B<C<cc>> parameter.",
	{ param => 'continent', valid => \&valid_continent, list => qr{[\s,]+}, bad_value => '_' },
	    "Return only records whose geographic location falls within the specified continent or continents.",
	    "The value of this parameter should be a comma-separated list of ",
	    "L<continent codes|op:config.txt?show=continents>.  This parameter is deprecated;",
	    "use F<cc> instead.",
	{ param => 'strat', valid => ANY_VALUE, list => ',' },
	    "Return only records that fall within the named geological stratum or strata.  You",
	    "may specify more than one, separated by commas.  Names may include the standard",
	    "SQL wildcards C<%> and C<_>, and may be followed by any of",
	    "'fm', 'gp', 'mbr'.  If none of these suffixes is given, then all matching",
	    "stratigraphic names will be selected.  If the parameter value begins with C<!>,",
	    "then records associated with this stratum or strata are excluded instead of included.",
	    "Note that this parameter is resolved through",
	    "string matching only.  Stratigraphic nomenclature is not currently standardized in",
	    "the database, so misspellings may occur.", 
	{ param => 'formation', valid => ANY_VALUE, list => ',' },
	    "Return only records that fall within the named stratigraphic formation(s).",
	    "This parameter is deprecated; use F<strat> instead.",
	{ param => 'stratgroup', valid => ANY_VALUE, list => ',' },
	    "Return only records that fall within the named stratigraphic group(s).",
	    "This parameter is deprecated; use F<strat> instead.",
	{ param => 'member', valid => ANY_VALUE, list => ',' },
	    "Return only records that fall within the named stratigraphic member(s).",
	    "This parameter is deprecated; use F<strat> instead.",
	{ param =>'lithology', valid => ANY_VALUE, list => ',' },
	    "Return only records recorded as coming from any of the specified lithologies and/or",
	    "lithology types.  If the paramter value string starts with C<!> then matching records",
	    "will be B<excluded> instead.  If the symbol C<^> occurs at the beginning of any",
	    "lithology name, then all subsequent values will be subtracted from the filter.",
	    "Example: carbonate,^bafflestone.",
	{ param => 'envtype', valid => ANY_VALUE, list => ',' },
	    "Return only records recorded as belonging to any of the specified environments",
	    "and/or environmental zones.  If the parameter value string starts with C<!> then",
	    "matching records will be B<excluded> instead.",
	    "If the symbol C<^> occurs at the beginning of any environment code, then all subsequent",
	    "values will be subtracted from the filter.  Examples: C<terr,^fluvial,lacustrine> or",
	    "C<!slope,^carbonate>.  You may specify one or more of the following values,",
	    "as a comma-separated list:", 
	    $ds->document_set('1.2:colls:envtype'),
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ allow => '1.2:timerule_selector' });
    
    $ds->define_ruleset('1.2:colls:specifier' =>
	{ param => 'coll_id', valid => VALID_IDENTIFIER('COL'), alias => 'id' },
	    "The identifier of the collection you wish to retrieve (REQUIRED).  You",
	    "may instead use the parameter name B<C<id>>.");
    
    $ds->define_ruleset('1.2:colls:selector' =>
	{ param => 'coll_id', valid => VALID_IDENTIFIER('COL'), alias => 'id', list => ',' },
	    "A comma-separated list of collection identifiers.  The specified collections",
	    "are selected, provided they satisfy the other parameters",
	    "given with this request.  You may also use the parameter name B<C<id>>.",
	    "You can also use this parameter along with any of the other parameters",
	    "to filter a known list of collections according to other criteria.",
	{ param => 'occ_id', valid => VALID_IDENTIFIER('OCC'), list => ',' },
	    "A comma-separated list of occurrence identifiers.  The collections associated with the",
	    "specified occurrences are selected, provided they satisfy the other parameters given",
	    "with this request.");
    
    $ds->define_ruleset('1.2:colls:all_records' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "Select all collections entered in the database, subject to any other parameters you may specify.",
	    "This parameter does not require any value.");
    
    $ds->define_ruleset('1.2:colls:show' =>
	{ optional => 'show', list => q{,},
	  valid => '1.2:colls:basic_map' },
	    "This parameter is used to select additional blocks of information to be returned",
	    "along with the basic record for each collection.  Its value should be",
	    "one or more of the following, separated by commas:");
    
	# { optional => 'order', valid => $ds->valid_set('1.2:colls:order'), split => ',' },
	#     "Specifies the order in which the results are returned.  You can specify multiple values",
	#     "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  Accepted values are:",
	#     $ds->document_set('1.2:colls:order'),
	#     "If no order is specified, results are returned as they appear in the C<collections> table.",
		
    $ds->define_ruleset('1.2:summary_display' =>
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
	">>The following parameter specifies that additional blocks of data should be returned,",
	"along with the basic collection record.",
	">>You may also use the following parameters to specify what information you wish to retrieve:",
	{ optional => 'pgm', valid => '1.2:colls:pgmodel', list => "," },
	    "Specify which paleogeographic model(s) to use when evaluating paleocoordinates.",
	    "You may specify one or more from the following list, separated by commas.",
	    "If you do not specify a value for this parameter, the default model is C<B<gplates>>.",
    	{ allow => '1.2:colls:show' },
    	{ allow => '1.2:special_params' },
        "^You can also use any of the L<special parameters|node:special> with this request");

    $ds->define_ruleset('1.2:colls:list' => 
	"You can use the following parameter if you wish to retrieve the entire set of",
	"collection records stored in this database.  Please use this with care, since the",
	"result set will contain more than 100,000 records and will be at least 20 megabytes in size.",
	"You may also select subsets of this list by specifying some combination of the parameters listed below.",
    	{ allow => '1.2:colls:all_records' },
        ">>The following parameters can be used to query for collections by a variety of criteria.",
	"Except as noted below, you may use these in any combination.  If you do not specify B<C<all_records>>,",
	"you must specify at least one selection parameter from the following list.",
	"The parameters referring to taxonomy",
	"select those collections that contain at least one matching occurrence.",
	{ allow => '1.2:colls:selector' },
   	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:colls:all_records', '1.2:colls:selector', '1.2:main_selector', 
			  '1.2:interval_selector', '1.2:ma_selector' ] },
	">>The following parameters can be used to filter the selection.",
	"If you wish to use one of them and have not specified any of the selection parameters",
	"listed above, use B<C<all_records>>.",
	{ allow => '1.2:common:select_colls_crmod' },
	{ allow => '1.2:common:select_colls_ent' },
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	">>The following parameters can be used to further filter the selection, based on the",
	"taxonomy of the selected occurrences.  These are only relevant if you have also specified",
	"one of the taxonomic parameters above.  In this case, collections are only selected if they",
	"contain at least one occurrence matching the specified parameters.",
	{ allow => '1.2:taxa:occ_list_filter' },
	">>You can use the following parameters to select extra information you wish to retrieve,",
	"and the order in which you wish to get the records:",
    	{ allow => '1.2:colls:show' },
	{ optional => 'order', valid => '1.2:colls:order', split => ',' },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  If this",
	    "parameter is not given, the returned collections are displayed in the order in which they",
	    "were entered into the database.  Accepted values are:",
	{ ignore => 'level' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:colls:byref' => 
	"You can use the following parameter if you wish to retrieve the entire set of",
	"collection records stored in this database.  Please use this with care, since the",
	"result set will contain more than 100,000 records and will be at least 20 megabytes in size.",
	"You may also specify any of the parameters listed below.",
    	{ allow => '1.2:colls:all_records' },
        ">>The following parameters can be used to query for collections by a variety of criteria.",
	"Except as noted below, you may use these in any combination.  If you do not specify B<C<all_records>>,",
	"you must specify at least one selection parameter from the following list.",
	"The parameters referring to taxonomy",
	"select those collections that contain at least one matching occurrence.",
	{ allow => '1.2:colls:selector' },
   	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:colls:all_records', '1.2:colls:selector', '1.2:main_selector', 
			  '1.2:interval_selector', '1.2:ma_selector' ] },
	{ ignore => [ 'level', 'ref_type', 'select' ] },
	">>You can use the following parameters to filter the result set based on attributes",
	"of the bibliographic references.  If you wish to use one of them and have not specified",
	"any of the selection parameters listed above, use B<C<all_records>>.",
	{ allow => '1.2:refs:aux_selector' },
	">>The following parameters can also be used to filter the selection.",
	{ allow => '1.2:common:select_colls_crmod' },
	{ allow => '1.2:common:select_colls_ent' },
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ allow => '1.2:common:select_refs_crmod' },
	{ allow => '1.2:common:select_refs_ent' },
	">>The following parameters can be used to further filter the selection, based on the",
	"taxonomy of the selected occurrences.  These are only relevant if you have also specified",
	"one of the taxonomic parameters above.  In this case, collections are only selected if they",
	"contain at least one occurrence matching the specified parameters.",
	{ allow => '1.2:taxa:occ_list_filter' },
	">>You can use the following parameters to select extra information you wish to retrieve,",
	"and the order in which you wish to get the records:",
    	{ allow => '1.2:colls:show' },
	{ optional => 'order', valid => '1.2:colls:order', split => ',' },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  If this",
	    "parameter is not given, the returned collections are ordered by reference.  If",
	    "B<C<all_records>> is specified, the references will be sorted in the order they were",
	    "entered in the database.  Otherwise, they will be sorted by default by the name of the",
	    "first and second author.  Accepted values include:",
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:colls:refs' =>
	"You can use the following parameter if you wish to retrieve all of the references",
	"from which collections were entered into the database.",
	{ allow => '1.2:colls:all_records' },
        ">>The following parameters can be used to retrieve the references associated with collections",
	"selected by a variety of criteria.  Except as noted below, you may use these in any combination.",
	"These parameters can all be used to select either occurrences, collections, or associated references.",
	"The taxonomic parameters select all collections that contain at least one matching occurrence.",
	{ allow => '1.2:colls:selector' },
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:colls:all_records', '1.2:colls:selector', '1.2:main_selector', 
			  '1.2:interval_selector', '1.2:ma_selector'] },
	">>You can use the following parameters to filter the result set based on attributes",
	"of the bibliographic references.  If you wish to use one of them and have not specified",
	"any of the selection parameters listed above, use B<C<all_records>>.",
	{ allow => '1.2:refs:aux_selector' },
	">>The following parameters further filter the selection:",
	{ allow => '1.2:common:select_colls_crmod' },
	{ allow => '1.2:common:select_colls_ent' },
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ allow => '1.2:common:select_refs_crmod' },
	{ allow => '1.2:common:select_refs_ent' },
	">>The following parameters can be used to further filter the selection, based on the",
	"taxonomy of the selected occurrences.  These are only relevant if you have also specified",
	"one of the taxonomic parameters above.  In this case, collections are only selected if they",
	"contain at least one occurrence matching the specified parameters.",
	{ allow => '1.2:taxa:occ_list_filter' },
	">>The following parameter allows you to request additional blocks of information",
	"beyond the basic reference record.",
	{ allow => '1.2:refs:display' },
	">>The following parameter specifies the order in which the results should be returned.",
	{ allow => '1.2:refs:order' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.2:colls:summary' => 
	"The following required parameter selects from the available resolution levels.",
	"You can get a L<list of available resolution levels|op:config.txt?show=clusters>.",
	{ param => 'level', valid => POS_VALUE, default => 1 },
	    "Return records from the specified cluster level.  (REQUIRED)",
	">>You can use the following parameter if you wish to retrieve a geographic summary",
	"of the entire set of collections entered in the database.",
    	{ allow => '1.2:colls:all_records' },
	">>You can use the following parameters to query for collections by",
	"a variety of criteria.  Except as noted below, you may use these in any combination.",
	"The resulting list will be mapped onto summary clusters at the selected level of",
	"resolution.",
    	{ allow => '1.2:colls:selector' },
    	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:colls:all_records', '1.2:colls:selector', '1.2:main_selector', 
			  '1.2:interval_selector', '1.2:ma_selector'] },
	">>The following parameters filter the result set.  If you wish to use one of them and",
	"have not specified any of the selection parameters listed above, use B<C<all_records>>.",
	{ allow => '1.2:common:select_colls_crmod' },
	{ allow => '1.2:common:select_colls_ent' },
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	">>The following parameters can be used to further filter the selection, based on the",
	"taxonomy of the selected occurrences.  These are only relevant if you have also specified",
	"one of the taxonomic parameters above.  In this case, collections are only selected if they",
	"contain at least one occurrence matching the specified parameters.",
	{ allow => '1.2:taxa:occ_list_filter' },
	">>You can use the following parameter to request additional information",
	"beyond the basic summary cluster records.",
    	{ allow => '1.2:summary_display' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    # $ds->define_ruleset('1.2:toprank_selector' =>
    # 	{ param => 'show', valid => ENUM_VALUE('formation', 'ref', 'author'), list => ',' });
    
    # $ds->define_ruleset('1.2:colls:toprank' => 
    # 	{ require => '1.2:main_selector' },
    # 	{ require => '1.2:toprank_selector' },
    # 	{ allow => '1.2:special_params' });
    
    $ds->define_ruleset('1.2:strata:selector' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "Return all stratum names known to the database.",
	{ param => 'name', valid => ANY_VALUE, list => ',' },
	    "A full or partial name.  You can use % and _ as wildcards.",
	{ optional => 'rank', valid => ENUM_VALUE('formation','group','member') },
	    "Return only strata of the specified rank: formation, group or member",
	{ param => 'lngmin', valid => COORD_VALUE('lng') },
	{ param => 'lngmax', valid => COORD_VALUE('lng') },
	{ param => 'latmin', valid => COORD_VALUE('lat') },
	{ param => 'latmax', valid => COORD_VALUE('lat') },
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
	{ optional => 'show', list => q{,},
	  valid => $ds->valid_set('1.2:strata:basic_map') },
	    "Selects additional information to be returned",
	    "along with the basic record for each stratum.  Its value should be",
	    "one or more of the following, separated by commas:",
	    $ds->document_set('1.2:strata:basic_map'),
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.2:strata:auto' =>
	{ param => 'name', valid => ANY_VALUE },
	    "A full or partial name.  It must have at least 3 significant characters,",
	    "and may end in a space followed by either 'g' or 'f' to indicate",
	    "that you are looking for a group or formation.",
	{ optional => 'rank', valid => ENUM_VALUE('formation','group') },
	    "Return only strata of the specified rank: formation or group.",
	    "This may be overridden by a suffix on the value of B<C<name>>.",
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
	# { param => 'loc', valid => ANY_VALUE },		# This should be a geometry in WKT format
	#     "Return only strata associated with some occurrence whose geographic location falls",
	#     "within the specified geometry, specified in WKT format.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    if ( ref $PB2::ConfigData::LITHOLOGIES eq 'ARRAY' )
    {
	foreach my $record ( @$PB2::ConfigData::LITHOLOGIES )
	{
	    my $lithology = $record->{lithology};
	    my $lith_type = $record->{lith_type};
	    
	    if ( $lithology =~ /^"/ )
	    {
		$lithology =~ s/"//g;
		$LITH_QUOTED{$lithology} = 1;
	    }
	    
	    $LITH_VALUE{$lithology} = 1;
	    $LITHTYPE_VALUE{$lith_type} = 1;
	}
    }

    my $a = 1;	# we can stop here when debugging
}


sub max_bin_level {

    return $MAX_BIN_LEVEL;
}


# get ( )
# 
# Query for all relevant information about the collection specified by the
# 'id' parameter.  Returns true if the query succeeded, false otherwise.

sub get_coll {

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
    $request->extid_check;
    
    # Figure out what information we need to determine access permissions.
    
    my ($access_filter, $access_fields) = $request->generateAccessFilter('cc', { });
    
    $fields .= $access_fields if $access_fields;
    
    $request->delete_output_field('permissions') unless $access_fields;
    
    my ($join_list) = $request->generateJoinList('c', $request->tables_hash);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $fields, if($access_filter, 1, 0) as access_ok
	FROM $COLL_MATRIX as c JOIN collections as cc using (collection_no)
		LEFT JOIN secondary_refs as sr using (collection_no)
		$join_list
        WHERE c.collection_no = $id
	GROUP BY c.collection_no";
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    die $request->exception(404, "Not found") unless $request->{main_record};
    
    die $request->exception(403, "Access denied") 
	unless $request->{main_record}{access_ok};
}


# list ( )
# 
# Query the database for basic info about all collections satisfying the
# conditions specified by the query parameters.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub list_colls {

    my ($request, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my $tables = $request->tables_hash;
    
    my @filters = $request->generateMainFilters('list', 'c', $tables);
    push @filters, $request->generateCollFilters($tables);
    push @filters, $request->PB2::OccurrenceData::generateOccFilters($tables, 'o');
    push @filters, $request->generate_ref_filters($tables);
    push @filters, $request->generate_refno_filter('c');
    push @filters, $request->generate_common_filters( { occs => 'o', colls => 'cc', bare => 'cc' }, $tables );
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
    
    # Figure out what information we need to determine access permissions.
    
    my ($access_filter, $access_fields) = $request->generateAccessFilter('cc', $tables);
    
    push @filters, $access_filter;
    
    my $filter_string = join(' and ', @filters);
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    $request->extid_check;
    
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
    
    $fields .= $access_fields if $access_fields;
    
    $request->delete_output_field('permissions') unless $access_fields;
    
    # Determine the order in which the results should be returned.
    
    my $order_clause = $request->generate_order_clause($tables, { at => 'c', bt => 'c' }) || 'NULL';
    
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
    
    # Figure out which bin level we are being asked for.  The default is 1.    a
    
    my $bin_level = $request->clean_param('level') || 1;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = $request->generateMainFilters('summary', 's', $tables);
    push @filters, $request->generateCollFilters($tables);
    push @filters, $request->PB2::OccurrenceData::generateOccFilters($tables, 'o');
    push @filters, $request->generate_common_filters( { occs => 'o', colls => 'cc' }, $tables );
    
    # Figure out the filter we need for determining access permissions.  We can ignore the extra
    # fields, since we are not returning records of type 'collection' or 'occurrence'.
    
    my ($access_filter, $access_fields) = $request->generateAccessFilter('cc', $tables);
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 's' );
    
    my $fields = $request->select_string;
    
    $request->adjustCoordinates(\$fields);
    
    # if ( $tables->{tf} )
    # {
    # 	$fields =~ s{ s[.]n_colls }{count(distinct c.collection_no) as n_colls}xs;
    # 	$fields =~ s{ s[.]n_occs }{count(distinct o.occurrence_no) as n_occs}xs;
    # 	$tables->{c} = 1;
    # }
    
    if ( $tables->{cc} || $tables->{t} || $tables->{tf} || $tables->{o} || $tables->{oc} )
    {
	$tables->{c} = 1;
    }
    
    if ( $tables->{im} )
    {
	if ( $tables->{c} )
	{
	    $fields =~ s{ im[.]cx_int_no }{max(c.early_age) as early_age, min(c.late_age) as late_age}xs;
	}
	
	else
	{
	    $fields =~ s{ im[.]cx_int_no}{s.early_age, s.late_age}xs;
	}
	
	delete $tables->{im};
    }
    
    if ( $tables->{o} )
    {
	$fields =~ s{ \bs.n_colls\b }{count(distinct o.collection_no) as n_colls}xs;
	$fields =~ s{ \bs.n_occs\b }{count(distinct o.occurrence_no) as n_occs}xs;
    }

    elsif ( $tables->{c} )
    {
	$fields =~ s{ \bs.n_colls\b }{count(distinct c.collection_no) as n_colls}xs;
	$fields =~ s{ \bs.n_occs\b }{sum(c.n_occs) as n_occs}xs;
    }
    
    if ( $request->has_block('bin') )
    {
	$request->delete_output_field('bin_id_1') if $bin_level < 2;
	$request->delete_output_field('bin_id_2') if $bin_level < 3;
	$request->delete_output_field('bin_id_3') if $bin_level < 4;
	
	$fields .= ", s.bin_id_1" if $bin_level > 1;
	$fields .= ", s.bin_id_2" if $bin_level > 2;
	$fields .= ", s.bin_id_3" if $bin_level > 3;
    }
    
    my $summary_joins = '';
    
    $summary_joins .= "JOIN $COLL_MATRIX as c on s.bin_id = c.bin_id_${bin_level}\n"
	if $tables->{c};
    
    $summary_joins .= "JOIN collections as cc using (collection_no)\n" if $tables->{cc};
    
    $summary_joins .= $request->generateJoinList('s', $tables);
    
    if ( $tables->{cc} )
    {
	push @filters, $access_filter;
    }
    
    elsif ( $tables->{c} )
    {
	push @filters, 'c.access_level = 0';
    }
    
    else
    {
	push @filters, 's.access_level = 0';
    }
    
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
    
    push @filters, "s.bin_level = $bin_level and s.bin_id > 0";
    
    my $filter_string = join(' and ', @filters);
    
    unless ( $filter_string =~ qr{ \bs.interval_no \s* = }xs )
    {
	$filter_string .= ' and s.interval_no = 0';
    }
    
    # If we want the containing interval numbers, we have to specify this as
    # an inner and an outer query.
    
    if ( $request->has_block('1.2:colls:time') )
    {
	$request->{main_sql} = "
	SELECT $calc innerq.*, im.cx_int_no
	FROM (SELECT $fields FROM $COLL_BINS as s $summary_joins
		WHERE $filter_string
		GROUP BY s.bin_id
		ORDER BY s.bin_id $limit) as innerq
	join $INTERVAL_MAP as im on im.early_age = innerq.early_age and im.late_age = innerq.late_age";
    }
    
    # Otherwise we just need a single query.
    
    else
    {
	$request->{main_sql} = "
	SELECT $calc $fields
	FROM $COLL_BINS as s $summary_joins
	WHERE $filter_string
	GROUP BY s.bin_id
	ORDER BY s.bin_id $limit";
    }
    
    # Then prepare and execute the query..
    
    print STDERR $request->{main_sql} . "\n\n" if $request->debug;
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # Get the result count, if we were asked to do so.
    
    $request->sql_count_rows;
    
    return 1;
}


# refs ( )
# 
# Query the database for the references associated with collections satisfying
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
    push @filters, $request->PB2::OccurrenceData::generateOccFilters($inner_tables, 'o');
    push @filters, $request->generate_common_filters( { colls => 'cc' }, $inner_tables );
    # push @filters, $request->generate_crmod_filters('cc', $inner_tables);
    # push @filters, $request->generate_ent_filters('cc', $inner_tables);
    
    # Figure out the filter we need for determining access permissions.  We can ignore the extra
    # fields, since we are not returning records of type 'collection' or 'occurrence'.
    
    my ($access_filter, $access_fields) = $request->generateAccessFilter('c', $inner_tables);
    
    push @filters, $access_filter;
    
    # Then construct the inner filter string, for selecting collection records.
    
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
    $request->extid_check;
    
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
    
    # Construct the main query.
    
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

sub list_coll_strata {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries. $$$
    
    my $dbh = $request->get_connection;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    $request->substitute_select( mt => 'cs' );
    
    my $tables = $request->tables_hash;
    my $group_expr = "cs.grp, cs.formation, cs.member";
    my $strata_fields = "cs.grp, cs.formation, cs.member";
    
    my @filters = $request->generateMainFilters('list', 'c', $tables);
    push @filters, $request->generateCollFilters($tables);
    push @filters, $request->PB2::OccurrenceData::generateOccFilters($tables, 'o');
    # push @filters, $request->generateStrataFilters($tables, $arg);
    
    # Figure out the filter we need for determining access permissions.  We can ignore the extra
    # fields, since we are not returning records of type 'collection' or 'occurrence'.
    
    my ($access_filter, $access_fields) = $request->generateAccessFilter('c', $tables);
    
    push @filters, $access_filter;
    
    # If the 'name' parameter was given, then add a filter for the stratigraphic name.
    
    my $rank = $request->clean_param('rank');
    
    if ( my @names = $request->clean_param_list('name') )
    {
	push @filters, $request->generate_stratname_filter('cs', \@names, $rank);
    }
    
    push @filters, "1=1" unless @filters;
    
    my $filter_string = join(' and ', @filters);
    
    
    if ( defined $rank && $rank eq 'group' )
    {
	$group_expr = "cs.grp";
	$strata_fields = "cs.grp, '' as formation, '' as member";
    }
    
    elsif ( defined $rank && $rank eq 'formation' )
    {
	$group_expr = "cs.grp, cs.formation";
	$strata_fields = "cs.grp, cs.formation, '' as member";
    }
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    $request->extid_check;
    
    # Modify the query according to the common parameters.
    
    my $limit = $request->sql_limit_clause(1);
    my $calc = $request->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $request->select_string;
    
    #$request->adjustCoordinates(\$fields);
    
    # Determine if any extra tables need to be joined in.
    
    my $base_joins = $request->generateJoinList('cs', $tables);
    
    # Add the collections table if we are doing access control.
    
    $base_joins = "JOIN collections as cc using (collection_no)\n" . $base_joins if $tables->{cc};
    
    $request->{main_sql} = "
	SELECT $calc $fields, $strata_fields
	FROM coll_strata as cs JOIN coll_matrix as c using (collection_no)
		$base_joins
        WHERE $filter_string
	GROUP BY $group_expr
	ORDER BY $group_expr
	$limit";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


# strata_auto ( )
#
# List strata by name for the purposes of auto-completion.

sub strata_auto {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my $tables = $request->tables_hash;
    
    my @filters = $request->generate_strata_auto_filters('sn', $tables);
    push @filters, '1=1' unless @filters;
    
    my $filter_string = join(' and ', @filters);
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. We skip the extid_check, since strata don't have identifiers.
    
    $request->strict_check;
    
    # Modify the query according to the common parameters.
    
    my $limit = $request->sql_limit_clause(1);
    my $calc = $request->sql_count_clause;
    
    my $fields = $request->select_string;
    
    #$request->adjustCoordinates(\$fields);
    
    # Determine if any extra tables need to be joined in.
    
    # my $base_joins = $request->generateJoinList('sn', $tables);
    
    # Add the collections table if we are doing access control.
    
    # $base_joins = "JOIN collections as cc using (collection_no)\n" . $base_joins if
    # $tables->{cc};
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM strata_names as sn
        WHERE $filter_string
	ORDER BY n_occs desc
	$limit";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;   
    
}


# auto_complete_str ( name, limit )
# 
# This is an alternate operation for auto-completion of geological strata names, designed to be
# called by the combined auto-complete operation.

sub auto_complete_str {
    
    my ($request, $name, $limit, $options) = @_;
    
    # Reject obvious mismatches
    
    # return if $name =~ qr{ ^ \w [.] \s+ \w }xsi;
    
    my $dbh = $request->get_connection();
    
    $limit ||= 10;
    $options ||= { };
    my @filters;
    
    my $quoted_name = $dbh->quote("${name}%");
    
    push @filters, "sn.name like $quoted_name";
    
    my $filter_string = join(' and ', @filters);
    
    my $country_field = $options->{countries} ? 'country_list' : 'cc_list';
    
    my $sql = "
	SELECT name, type, $country_field as cc_list, n_colls, n_occs, 'str' as record_type, 'str' as record_id
	FROM strata_names as sn
	WHERE $filter_string
	ORDER BY n_occs desc LIMIT $limit";
    
    print STDERR "$sql\n\n" if $request->debug;
    
    my $result_list = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    return ref $result_list eq 'ARRAY' ? @$result_list : ( );
}


# auto_complete_col ( name, limit )
# 
# This operation provides for auto-completion of collection names, designed to be
# called by the combined auto-complete operation.

sub auto_complete_col {
    
    my ($request, $name, $limit, $options) = @_;
    
    # Reject obvious mismatches
    
    # return if $name =~ qr{ ^ \w [.] \s+ \w }xsi;
    
    my $dbh = $request->get_connection();
    
    $limit ||= 10;
    $options ||= { };
    my @filters;
    
    # Add a filter to select collections that match the specified name. Special-case it so that a
    # ^ will leave off the initial % wildcard.
    
    $name = "${name}%";
    
    my $quoted_name = $dbh->quote($name);
    
    push @filters, "cc.collection_name like $quoted_name";
    
    my $use_extids = $request->has_block('extids');
    
    # If we are given early and/or late age bounds, select only collections whose age *overlaps*
    # this range.
    
    if ( $options->{early_age} )
    {
	push @filters, "c.late_age <= $options->{early_age}";
    }
    
    if ( $options->{late_age} )
    {
	push @filters, "c.early_age >= $options->{late_age}";
    }
    
    # Construct the query.
    
    my $filter_string = join(' and ', @filters);
    
    my $country_field = $options->{countries} ? 'cm.name' : 'c.cc';
    
    my $sql = "
	SELECT collection_no, 'col' as record_type, collection_name as name,
		$country_field as cc_list, n_occs,
		ei.interval_name as early_interval, li.interval_name as late_interval
	FROM collections as cc join $COLL_MATRIX as c using (collection_no)
		left join $COUNTRY_MAP as cm using (cc)
		left join $INTERVAL_DATA as ei on ei.interval_no = c.early_int_no
		left join $INTERVAL_DATA as li on li.interval_no = c.late_int_no
	WHERE $filter_string
	ORDER BY n_occs desc LIMIT $limit";
    
    print STDERR "$sql\n\n" if $request->debug;
    
    my $result_list = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    if ( ref $result_list eq 'ARRAY' )
    {
	foreach my $r ( @$result_list )
	{
	    $r->{record_id} = $use_extids ? generate_identifier('COL', $r->{collection_no}) :
		$r->{collection_no};
	}
	
	return @$result_list;
    }
    
    return;
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
    $record->{max_ma} =~ s{ (?: [.] 0+ $ | ( [.] \d* [1-9] ) 0+ $ ) }{$1}sxe
	if defined $record->{max_ma};
    $record->{min_ma} =~ s{ (?: [.] 0+ $ | ( [.] \d* [1-9] ) 0+ $ ) }{$1}sxe
	if defined $record->{min_ma};
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
    
    # The following has been moved to 'generateOccFilters' in OccurrenceData.pm.
    
    # If our tables include the occurrence matrix, we must check the 'idtype'
    # parameter. 
    
    # if ( ($tables_ref->{o} || $tables_ref->{tf} || $tables_ref->{t} || $tables_ref->{oc}) &&
    #      ! $tables_ref->{ds} )
    # {
    # 	my $ident = $request->clean_param('idtype');
	
    # 	if ( $ident eq '' || $ident eq 'latest' )
    # 	{
    # 	    push @filters, "o.latest_ident = true";
    # 	}
	
    # 	elsif ( $ident eq 'orig' )
    # 	{
    # 	    push @filters, "o.reid_no = 0";
    # 	}
	
    # 	elsif ( $ident eq 'reid' )
    # 	{
    # 	    push @filters, "(o.reid_no > 0 or (o.reid_no = 0 and o.latest_ident = false))";
    # 	}
	
    # 	elsif ( $ident eq 'all' )
    # 	{
    # 	    # we need do nothing in this case
    # 	}
    # }
    
    # Check for a 'ref_id' parameter.
    
    if ( my @reflist = $request->clean_param_list('ref_id') )
    {
	my $refstring = join(',', @reflist);
	push @filters, "c.reference_no in ($refstring)";
	$tables_ref->{c} = 1;
    }
    
    return @filters;
}


# generateStrataFilters ( tables_ref )
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
    
    my ($request, $mt, $names_ref, $rank) = @_;
    
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
	    
	    unless ( $name =~ qr{[a-z%]}xi )
	    {
		$request->add_warning("bad value '$name' for parameter 'strat', must contain at least one letter");
		next;
	    }
	    
	    my $quoted = $dbh->quote($name);
	    
	    if ( lc $rank eq 'fm' )
	    {
		push @clauses, "cs.formation like $quoted";
	    }
	    
	    elsif ( lc $rank eq 'mbr' )
	    {
		push @clauses, "cs.member like $quoted";
	    }
	    
	    else # ( lc $rank eq 'gp' )
	    {
		push @clauses, "cs.grp like $quoted";
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
	    
	    if ( defined $rank && $rank eq 'group' )
	    {
		push @clauses, "cs.grp like $quoted";
	    }
	    
	    elsif ( defined $rank && $rank eq 'formation' )
	    {
		push @clauses, "cs.formation like $quoted";
	    }
	    
	    elsif ( defined $rank && $rank eq 'member' )
	    {
		push @clauses, "cs.member like $quoted";
	    }
	    
	    else
	    {
		push @clauses, "cs.grp like $quoted";
		push @clauses, "cs.formation like $quoted";
		push @clauses, "cs.member like $quoted";
	    }
	}
	
	else
	{
	    push @unqualified, $dbh->quote($name);
	}
    }
    
    if ( @unqualified )
    {
	my $quoted = join(',', @unqualified);
	
	if ( defined $rank && $rank eq 'group' )
	{
	    push @clauses, "cs.grp in ($quoted)";
	}
	
	elsif ( defined $rank && $rank eq 'formation' )
	{
	    push @clauses, "cs.formation in ($quoted)";
	}
	
	elsif ( defined $rank && $rank eq 'member' )
	{
	    push @clauses, "cs.member in ($quoted)";
	}
	
	else
	{
	    push @clauses, "cs.grp in ($quoted)";
	    push @clauses, "cs.formation in ($quoted)";
	    push @clauses, "cs.member in ($quoted)";
	}
    }
    
    # If no valid values were found, then add a clause that will select nothing.
    
    unless ( @clauses )
    {
	push @clauses, "cs.formation = '!NOTHING!'";
    }
    
    my $clause = '(' . join( ' or ', @clauses ) . ')';
    $clause = "not " . $clause if $negate;
    
    return $clause;
}


# generate_strata_auto_filters ( main_table )
# 
# Generate filters for strata auto-completion (as opposed to listing, which is handled above).

sub generate_strata_auto_filters {
    
    my ($request, $mt) = @_;
    
    my $dbh = $request->get_connection;
    
    my @filters;
    
    my $name = $request->clean_param('name');
    my $rank = $request->clean_param('rank');
    
    unless ( $name && length($name) >= 3 )
    {
	return $request->exception(400, "You must specify at least 3 characters to be matched against strata names");
    }
    
    if ( $name =~ /(.*)\s+([fgm])\w*$/i )
    {
	my $r = lc $2;
	$name = $1;
	
	if ( $r eq 'f' ) {
	    $rank = 'formation';
	} elsif ( $r eq 'g' ) {
	    $rank = 'group';
	} elsif ( $r eq 'm' ) {
	    $rank = 'member';
	}
    }
    
    if ( $rank eq 'formation' || $rank eq 'group' || $rank eq 'member' )
    {
	push @filters, "type = '$rank'";
    }
    
    $name =~ s/\s+$//;
    
    my $quoted_name = $dbh->quote("${name}%");
    
    push @filters, "$mt.name like $quoted_name";
    
    # Now check for the lat/lng parameters and add the necessary filters.  Note that we are
    # filtering for any stratum whose range *overlaps* the specified region.
    
    my $x1 = $request->clean_param('lngmin');
    my $x2 = $request->clean_param('lngmax');
    my $y1 = $request->clean_param('latmin');
    my $y2 = $request->clean_param('latmax');
    
    # If longitude bounds were specified, add filters for them.
    
    if ( $x1 ne '' && $x2 ne '' && ! ( $x1 == -180 && $x2 == 180 ) )
    {
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
	
	# If $x1 < $x2, then we query on a single range defined by
	# those coordinates.
	
	if ( $x1 <= $x2 )
	{
	    $request->add_warning("The values of 'lngmin' and 'lngmax' are equal, " .
				  "so only records with that exact longitude will be selected")
		if $x1 == $x2;
	    
	    push @filters, "$mt.lng_max > $x1 and $mt.lng_min < $x2";
	}
	
	# Otherwise, our range crosses the antimeridian and so must be
	# split in two.
	
	else
	{
	    push @filters, "($mt.lng_max > $x1 or $mt.lng_min < $x2)";
	}
    }
    
    # If latitude bounds were specified, add filters for them.
    
    if ( $y1 && $y1 < 90.0 && $y1 > -90.0 )
    {
	push @filters, "$mt.lat_max > $y1";
    }
    
    if ( $y2 && $y2 < 90.0 && $y2 > -90.0 )
    {
	push @filters, "$mt.lat_min < $y2";
    }
    
    if ( defined $y1 && defined $y2 && $y1 ne '' && $y2 ne '' && $y1 > $y2 )
    {
	$request->add_warning("The minimum latitude specified is greater than the maximum latitude specified, so no records will be selected");
    }
    
    return @filters;
}

# generateAccessFilter ( mt, tables_ref )
# 
# Generate a filter clause that will select only collections that the
# requestor is allowed to access.  This determination is made by checking for
# a PBDB Classic login cookie.

sub generateAccessFilter {
    
    my ($request, $mt, $tables_ref) = @_;
    
    # First check to see if the 'private' parameter was included in this
    # request.  If not, then return a filter that will select only public
    # data. 
    
    unless ( $request->clean_param('private') )
    {
	return ("c.access_level = 0", '');
    }
    
    # Next see if we have a login cookie from Classic.  If so, extract the
    # authorizer_no and is_super values from the corresponding record in the
    # 'session_data' table.
    
    my ($authorizer_no, $is_super);
    
    my $dbh = $request->get_connection;
    
    if ( my $cookie_id = Dancer::cookie('session_id') )
    {
	my $session_id = $dbh->quote($cookie_id);
	
	my $sql = "
		SELECT authorizer_no, superuser FROM session_data
		WHERE session_id = $session_id";
	
	($authorizer_no, $is_super) = $dbh->selectrow_array($sql);
    }
    
    else
    {
	print STDERR "cookie: NONE\n";
    }
    
    # If we don't have a recognizable cookie that corresponds to a session
    # still in the table, then abort!
    
    unless ( $authorizer_no && $authorizer_no =~ /^\d+$/ )
    {
	die $request->exception(401, "You must be logged in to use a URL that contains the parameter 'private'");
    }
    
    # If we get here, then the requestor has some ability to see private data.  We need to select
    # additional fields so that the records can be checked before being sent to the requestor.
    
    $request->{my_authorizer_no} = $is_super ? -1 : $authorizer_no;
    $tables_ref->{$mt} = 1;
    
    my $fields = ", c.access_level, $mt.authorizer_no as access_no, " .
	"if($mt.access_level = 'group members', $mt.research_group, '') as access_resgroup";
    
    # If the requestor has superuser privilege, they can access anything.  But we still need the
    # access-control fields so that we can report the permissions on each individual record.
    
    if ( $is_super )
    {
	return ("c.access_level = c.access_level", $fields);
    }
    
    # Otherwise, we need to filter by authorizer_no and/or research group.
    
    else
    {
	my @clauses = "c.access_level <= 1";
	
	my $sql = "SELECT authorizer_no FROM permissions WHERE modifier_no=$authorizer_no";
	
	my ($permlist) = $dbh->selectcol_arrayref($sql);
	
	if ( ref $permlist eq 'ARRAY' && @$permlist )
	{
	    my $perm_string = join(',', @$permlist);
	    push @clauses, "$mt.authorizer_no in ($perm_string)";
	}
	
	$sql = "SELECT research_group FROM person WHERE person_no=$authorizer_no";
	
	my ($grouplist) = $dbh->selectrow_array($sql);
	
	if ( $grouplist && $grouplist =~ /^[\w,-]+$/ )
	{
	    $grouplist =~ s/,/|/g;
	    push @clauses, "$mt.access_level = 'group members' and $mt.research_group rlike '$grouplist'";
	}
	
	my $filter = "(" . join(' or ', @clauses) . ")";
	
	return ( $filter, $fields );
    }
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
	
	# If the result is -1, that means that only invalid identifiers were
	# specified.  So add a filter that will select nothing.
	
	if ( $id_list eq '-1' )
	{
	    $request->add_warning("no valid cluster identifiers were given");
	    push @filters, "c.collection_no = -1";
	}
	
	elsif ( $op eq 'summary' )
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
    
    # Check for parameter 'coll_id', If the parameter was given but no value was
    # found, then add a clause that will generate an empty result set.
    
    if ( my @colls = $request->safe_param_list('coll_id') )
    {
	my $id_list = $request->check_values($dbh, \@colls, 'collection_no', 'collections', 
					     "Unknown collection '%'");
	
	# If the result is -1, that means that only invalid identifiers were
	# specified.
	
	if ( $id_list eq '-1' )
	{
	    $request->add_warning("no valid collection identifiers were given");
	    push @filters, "c.collection_no = -1";
	}
	
	# If there aren't any bins, or no valid cluster ids were specified,
	# include a filter that will return no results.
	
	elsif ( $op eq 'summary' )
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
    
    # Check for parameters 'coll_match' and 'coll_re'.
    
    if ( my $coll_match = $request->clean_param('coll_match') )
    {
	my $quoted = $dbh->quote($coll_match);
	
	$tables_ref->{cc} = 1;
	push @filters, "(cc.collection_name like $quoted or cc.collection_aka like $quoted)";
    }
    
    if ( my $coll_re = $request->clean_param('coll_re') )
    {
	my $quoted = $dbh->quote($coll_re);
	
	$tables_ref->{cc} = 1;
	push @filters, "(cc.collection_name rlike $quoted or cc.collection_aka rlike $quoted)";
    }
    
    # Check for parameters 'base_name', 'taxon_name', 'match_name',
    # 'base_id', 'taxon_id'
    
    my ($taxon_name, @taxon_nos, $value, @values);
    my (@include_taxa, @exclude_taxa, $no_synonyms, $all_children, $do_match, $ident_used);
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
	$all_children = 1;
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
	    my $debug_out; $debug_out = sub { $request->{ds}->debug_line($_[0]); } if $request->debug;
	    
	    try {
		@taxa = $taxonomy->resolve_names($taxon_name, { fields => 'RANGE', all_names => 1, 
								current => 1, debug_out => $debug_out });
	    };
	    
	    # catch {
	    # 	print STDERR $taxonomy->last_sql . "\n\n" if $request->debug;
	    # 	die $_;
	    # };
	    
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
	$tables_ref->{o} = 1;
	$tables_ref->{tf} = 1;
	$tables_ref->{non_geo_filter} = 1;
	$request->{my_base_taxa} = [ @include_taxa, @exclude_taxa ];
    }
    
    elsif ( @include_taxa )
    {
	my $taxon_list = join ',', map { $_->{orig_no} } @include_taxa;
	push @filters, "(t.accepted_no in ($taxon_list) or t.orig_no in ($taxon_list))";
	$tables_ref->{o} = 1;
	$tables_ref->{tf} = 1;
	$tables_ref->{non_geo_filter} = 1;
	$tables_ref->{non_summary} = 1;
	$request->{my_taxa} = [ @include_taxa, @exclude_taxa ];
    }
    
    # If a name was given and no matching taxa were found, we need to query by
    # genus_name/species_name instead.  But if the operation is "prevalence"
    # or "diversity" then just abort with a warning.  We call this the
    # "identification branch".
    
    elsif ( $taxon_name )
    {
	if ( $op eq 'prevalence' || $op eq 'diversity' )
	{
	    $request->add_warning(@taxon_warnings) if @taxon_warnings;
	    return "t.lft = -1";
	}
	
	$ident_used = 1;
	
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
    
    # Check for parameter 'taxon_reso'.  But not if we took the "identification
    # branch" because in that case it has already been handled.
    
    my $taxonres = $request->clean_param('idreso');
    
    if ( $taxonres && ! $ident_used )
    {
	if ( $taxonres eq 'species' )
	{
	    push @filters, "tv.rank <= 3";
	    $tables_ref->{tv} = 1;
	    $tables_ref->{o} = 1;
	}
	
	elsif ( $taxonres eq 'genus' || $taxonres eq 'lump_genus' || $taxonres eq 'lump_gensub' )
	{
	    push @filters, "tv.rank <= 5";
	    $tables_ref->{tv} = 1;
	    $tables_ref->{o} = 1;
	}
	
	elsif ( $taxonres eq 'family' || $taxonres eq 'lump_family' )
	{
	    push @filters, "(tv.rank <= 9 or ph.family_no is not null)";
	    $tables_ref->{tv} = 1;
	    $tables_ref->{ph} = 1;
	    $tables_ref->{o} = 1;
	}
    }
    
    # Check for parameter 'taxon_status'
    
    my $taxon_status = $request->clean_param('taxon_status');
    
    if ( $tables_ref->{tf} && $taxon_status )
    {
	my $filter = $Taxonomy::STATUS_FILTER{$taxon_status};
	
	die "bad taxon status '$taxon_status'\n" unless $filter;
	
	push @filters, $filter;
	$tables_ref->{o} = 1;
    }
    
    # Check for parameter 'pres'
    
    my @pres = $request->clean_param_list('pres');
    
    if ( @pres )
    {
	my (%pres);
	
	foreach my $v (@pres)
	{
	    $pres{$v} = 1 if $v && $v ne '';
	}
	
	if ( %pres && ! $pres{all} )
	{
	    $tables_ref->{v} = 1;
	    $tables_ref->{o} = 1;
	    
	    my @keys = keys %pres;
	    
	    if ( @keys == 1 )
	    {
		if ( $keys[0] eq 'regular' )
		{
		    push @filters, "not(v.is_trace or v.is_form)";
		    $tables_ref->{v} = 1;
		}
		
		elsif ( $keys[0] eq 'form' )
		{
		    push @filters, "v.is_form";
		    $tables_ref->{v} = 1;
		}
		
		elsif ( $keys[0] eq 'ichno' )
		{
		    push @filters, "v.is_trace";
		    $tables_ref->{v} = 1;
		}
		
		else
		{
		    push @filters, "t.orig_no = -1";
		    die "400 bad value '$keys[0]' for option 'pres'\n";
		}
	    }
	    
	    elsif ( @keys == 2 )
	    {
		if ( $pres{form} && $pres{ichno} )
		{
		    push @filters, "(v.is_form or v.is_trace)";
		}
		
		elsif ( $pres{form} && $pres{regular} )
		{
		    push @filters, "(v.is_form or not(v.is_trace))";
		}
		
		elsif ( $pres{ichno} && $pres{regular} )
		{
		    push @filters, "(v.is_trace or not(v.is_form))";
		}
		
		else
		{
		    croak "bad value '$keys[0]', '$keys[1]' for option 'pres'\n";
		}
	    }
	    
	    else
	    {
		# No filter to add in this case, since @keys must be at least
		# 3 and so all 3 classes of taxa are selected.
	    }
	}
    }
    
    # Check for parameter 'extant'
    
    my $extant = $request->clean_param('extant');
	
    if ( defined $extant && $extant ne '' )
    {
	$tables_ref->{v} = 1;
	$tables_ref->{o} = 1;
	push @filters, ($extant ? "v.is_extant = 1" : "v.is_extant = 0");
    }
    
    # Check for parameter 'cc'
    
    my @ccs = $request->clean_param_list('cc');
    push @ccs, $request->clean_param_list('continent');
    
    if ( @ccs )
    {
	if ( $ccs[0] eq '_' )
	{
	    push @filters, "c.collection_no = -1";
	}
	
	else
	{
	    my (@cc2, @cc3, @cc2x, $invert);
	    
	    if ( $ccs[0] =~ qr{ ^ ! (.*) }xs )
	    {
		$ccs[0] = $1;
		$invert = 1;
	    }
	    
	    foreach my $value (@ccs)
	    {
		next unless $value;
		
	    	if ( $value =~ qr{ ^ \^ (\w\w) }xs )
	    	{
		    push @cc2x, $1;
	    	}
		
		elsif ( $value =~ qr{ ^ \w\w $ }xs )
		{
		    push @cc2, $value;
		}
		
		elsif ( $value =~ qr{ ^ \w\w\w $ }xs )
		{
		    push @cc3, $value;
		}
		
		else
		{
		    $request->add_warning("bad value '$value' for parameter 'cc'");
		}
	    }
	    
	    my (@cc_filters, @disjoint_filters, $disjunction);
	    
	    if ( @cc2 )
	    {
	    	push @disjoint_filters, "c.cc in ('" . join("','", @cc2) . "')";
		$tables_ref->{c} = 1;
	    }
	    
	    if ( @cc3 )
	    {
	    	push @disjoint_filters, "ccmap.continent in ('" . join("','", @cc3) . "')";
		$tables_ref->{c} = 1;
	    	$tables_ref->{ccmap} = 1;
	    }
	    
	    if ( @disjoint_filters > 1 )
	    {
		push @cc_filters, '(' . join(' or ', @disjoint_filters) . ')';
	    }
	    
	    elsif ( @disjoint_filters == 1 )
	    {
		push @cc_filters, $disjoint_filters[0];
	    }
	    
	    if ( @cc2x )
	    {
	     	push @cc_filters, "c.cc not in ('" . join("','", @cc2x) . "')";
	    }
	    
	    if ( $invert )
	    {
		push @filters, 'not( ' . join(' and ', @cc_filters) . ')';
	    }
	    
	    else
	    {
		push @filters, @cc_filters;
	    }
	}
	
	$tables_ref->{non_summary} = 1;
    }
    
    # Check for parameter 'state'
    
    my @states = $request->clean_param_list('state');
    
    if ( @states )
    {
	my $invert;
	
	# Look for an ! flag at the beginning, signalling that the user wants to invert this
	# filter.
	
	if ( $states[0] =~ qr{ ^ ! (.*) }xs )
	{
	    $states[0] = $1;
	    $invert = 1;
	}

	# Construct a quoted list using the parameter values, and add warnings for each value that
	# does not appear in the database.

	my $state_list = $request->verify_coll_param($dbh, 'state', \@states, 'state');
	
	if ( $invert )
	{
	    push @filters, "cc.state not in ($state_list)";
	}
	
	else
	{
	    push @filters, "cc.state in ($state_list)";
	}
	
	$tables_ref->{cc} = 1;
	$tables_ref->{non_summary} = 1;
    }
    
    # Check for parameter 'county'
    
    my @counties = $request->clean_param_list('county');
    
    if ( @counties )
    {
	my $invert;
	
	# Look for an ! flag at the beginning, signalling that the user wants to invert this
	# filter.
	
	if ( $counties[0] =~ qr{ ^ ! (.*) }xs )
	{
	    $counties[0] = $1;
	    $invert = 1;
	}

	# Construct a quoted list using the parameter values.
	
	my $county_list = $request->verify_coll_param($dbh, 'county', \@counties, 'county');
	
	if ( $invert )
	{
	    push @filters, "cc.county not in ($county_list)";
	}
	
	else
	{
	    push @filters, "cc.county in ($county_list)";
	}
	
	$tables_ref->{cc} = 1;
	$tables_ref->{non_summary} = 1;
    }
    
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
	    push @filters, "st_contains(geomfromtext($polygon), $mt.loc)";
	}
	
	# Otherwise, our bounding box crosses the antimeridian and so must be
	# split in two.  The latitude bounds must always be between -90 and
	# 90, regardless.
	
	else
	{
	    my $polygon = "'MULTIPOLYGON((($x1 $y1,180.0 $y1,180.0 $y2,$x1 $y2,$x1 $y1))," .
					"((-180.0 $y1,$x2 $y1,$x2 $y2,-180.0 $y2,-180.0 $y1)))'";
	    push @filters, "st_contains(geomfromtext($polygon), $mt.loc)";
	}
    }
    
    # If only latitude bounds were specified then create a bounding box
    # with longitude ranging from -180 to 180.
    
    elsif ( ($y1 ne '' || $y2 ne '') && ! ( $y1 == -90 && $y2 == 90 ) )
    {
	# If one of the bounds was not specified, set it to -90 or 90.
	
	$y1 = -90.0 unless defined $y1 && $y1 ne '';
	$y2 = 90.0 unless defined $y2 && $y2 ne '';
	
	my $polygon = "'POLYGON((-180.0 $y1,180.0 $y1,180.0 $y2,-180.0 $y2,-180.0 $y1))'";
	push @filters, "st_contains(geomfromtext($polygon), $mt.loc)";
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
	die "400 the value of parameter 'loc' is too large\n"
	    if length($loc) > 5000;
	my $quoted = $dbh->quote($loc);
	push @filters, "st_contains(geomfromtext($quoted), $mt.loc)";
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
    
    # Check for parameter 'lithology'
    
    my @lithology = $request->clean_param_list('lithology');
    
    my (%lith_values, %exlith_values, %lt_values, $lith_invert, $lith_unknown);
    
    if ( @lithology && $lithology[0] =~ qr{ ^ ! (.*) }xs )
    {
	$lithology[0] = $1;
	$lith_invert = 1;
    }
    
    foreach my $lith ( @lithology )
    {
	my $lith_exclude;
	my $lith_quoted;
	
	if ( $lith =~ qr{ ^ \^ \s* (.*) }xs )
	{
	    $lith = $1;
	    $lith_exclude = 1;
	}
	
	if ( $lith =~ qr{ ^ " (.*) " $ }xs )
	{
	    $lith = $1;
	    $lith_quoted = 1;
	}
	
	next unless $lith;
	
	$lith = lc $lith;
	
	if ( $lith eq 'unknown' )
	{
	    $lith_unknown = $lith_exclude ? 'exclude' : 'include';
	}
	
	elsif ( $LITHTYPE_VALUE{$lith} && ! $lith_quoted )
	{
	    if ( $lith_exclude )
	    {
		$request->add_warning("parameter 'lithology': you cannot exclude lithology types, only lithologies");
	    }
	    
	    else
	    {
		$lt_values{$lith} = 1;
	    }
	}
	
	elsif ( $LITH_VALUE{$lith} )
	{
	    if ( $lith_exclude )
	    {
		$exlith_values{$lith} = 1;
		delete $lith_values{$lith};
	    }
	    
	    else
	    {
		$lith_values{$lith} = 1;
	    }
	}
	
	else
	{
	    $request->add_warning("there are no records with lithology or lithology type '$lith' in the database");
	}
    }
    
    my $type_string = join(q{','}, keys %lt_values);
    my $lith_string = join(q{','}, map { $LITH_QUOTED{$_} ? "\"$_\"" : $_ } keys %lith_values);
    my $exlith_string = join(q{','}, map { $LITH_QUOTED{$_} ? "\"$_\"" : $_ } keys %exlith_values);
    
    if ( $type_string || $lith_string || $exlith_string || $lith_unknown )
    {
	$tables_ref->{cl} = 1;
	
	my (@include, @lith_filters);
	
	if ( $type_string )
	{
	    push @include, "cl.lith_type in ('$type_string')";
	}
	
	if ( $lith_string )
	{
	    push @include, "cl.lithology in ('$lith_string')";
	}
	
	if ( $lith_unknown && $lith_unknown eq 'include' )
	{
	    push @include, "cl.lithology is null";
	}
	
	if ( @include > 1 )
	{
	    my $include = join(' or ', @include);
	    push @lith_filters, "($include)";
	}
	
	elsif ( @include )
	{
	    push @lith_filters, @include;
	}
	
	elsif ( $lith_unknown && $lith_unknown eq 'exclude' )
	{
	    push @lith_filters, "cl.lithology is not null";
	}
	
	if ( $exlith_string )
	{
	    push @lith_filters, "cl.lithology not in ('$exlith_string')";
	}
	
	if ( $lith_invert )
	{
	    my $filter_string = @lith_filters > 1 ? join(' and ', @lith_filters) : $lith_filters[0];
	    
	    if ( $lith_unknown )
	    {
		push @filters, "not($filter_string)";
	    }
	    
	    else
	    {
		push @filters, "(not($filter_string) or cl.lithology is null)";
	    }
	}
	
	else
	{
	    push @filters, @lith_filters;
	}
    }
    
    # Check for parameter 'envtype'
    
    my @envtype = $request->clean_param_list('envtype');
    
    my (%env_values, %exc_values, $et_invert, $et_unknown);
    
    if ( @envtype && $envtype[0] =~ qr{ ^ ! (.*) }xs )
    {
	$envtype[0] = $1;
	$et_invert = 1;
    }
    
    foreach my $e ( @envtype )
    {
	my $et_exclude;
	
	$e = lc $e;
	
	if ( $e =~ qr{ ^ \^ \s* (.*) }xs )
	{
	    $e = $1;
	    $et_exclude = 1;
	}
	
	next unless $e;
	
	if ( $ETVALUE{$e} )
	{
	    foreach my $k ( keys %{$ETVALUE{$e}} )
	    {
		if ( $et_exclude ) {
		    delete $env_values{$k};
		} else {
		    $env_values{$k} = 1;
		}
	    }
	}
	
	elsif ( $EZVALUE{$e} )
	{
	    foreach my $k ( keys %{$EZVALUE{$e}} )
	    {
		if ( $et_exclude ) {
		    delete $env_values{$k};
		} else {
		    $env_values{$k} = 1;
		}
	    }
	}
	
	elsif ( $e eq 'unknown' )
	{
	    if ( $et_exclude ) {
		delete $env_values{''};
	    } else {
		$env_values{''} = 1;
	    }
	    $et_unknown = not $et_exclude;
	}
	
	elsif ( $e ne '_' )
	{
	    $request->add_warning("bad value '$e' for parameter 'envtype'");
	}
    }
    
    # if ( @envzone && $envzone[0] =~ qr{ ^ \^ (.*) }xs )
    # {
    # 	$envzone[0] = $1;
    # 	$ez_invert = 1;
    # }    
    
    # elsif ( @envzone & $envzone[0] =~ qr{ ^ \+ (.*) }xs )
    # {
    # 	$envzone[0] = $1;
    # 	$ez_add = 1;
    # 	$request->add_warning("the '+' modifier on parameter 'envzone' is typically used along with at least one value for 'envtype'") unless @envtype;
    # }
    
    # foreach my $e ( @envzone )
    # {
    # 	$e = lc $e;
	
    # 	if ( $EZVALUE{$e} )
    # 	{
    # 	    foreach my $k ( keys %{$ETVALUE{$e}} )
    # 	    {
    # 		$mod_values{$k} = 1;
    # 	    }
    # 	}
	
    # 	elsif ( $e eq 'unknown' )
    # 	{
    # 	    $env_values{''} = 1;
    # 	    $et_unknown = 1;
    # 	}
	
    # 	else
    # 	{
    # 	    $request->add_warning("bad value '$e' for parameter 'envtype'");
    # 	}
    # }
    
    # If @envzone was specified, then add, remove or restrict the values according to
    # $ez_add and $ez_invert.
    
    # if ( $ez_add && @envzone )
    # {
    # 	foreach my $k ( keys %mod_values )
    # 	{
    # 	    $env_values{$k} = 1;
    # 	}
    # }
    
    # elsif ( $ez_invert && @envzone )
    # {
    # 	foreach my $k ( keys %env_values )
    # 	{
    # 	    delete $env_values{$k} if $mod_values{$k};
    # 	}
    # }
    
    # elsif ( @envzone )
    # {
    # 	foreach my $k ( keys %env_values )
    # 	{
    # 	    delete $env_values{$k} unless $mod_values{$k};
    # 	}
    # }
    
    # If we ended up with at last one environment value, then construct a
    # filter expression.
    
    if ( %env_values )
    {   
	my $env_list = "'" . join("','", keys %env_values) . "'";
	
	if ( $et_invert && $et_unknown )
	{
	    push @filters, "cc.environment not in ($env_list)";
	    $tables_ref->{cc} = 1;
	}
	
	elsif ( $et_invert )
	{
	    push @filters, "(cc.environment not in ($env_list) or cc.environment is null)";
	    $tables_ref->{cc} = 1;
	}
	
	elsif ( $et_unknown )
	{
	    push @filters, "(cc.environment in ($env_list) or cc.environment is null)";
	    $tables_ref->{cc} = 1;
	}
	
	else
	{
	    push @filters, "cc.environment in ($env_list)";
	    $tables_ref->{cc} = 1;
	}
	
	# $tables_ref->{cc} = 1;
    }
    
    # Otherwise, if a parameter value was specified but the list is empty,
    # then add a filter that will ensure an empty selection.
    
    elsif ( @envtype )
    {
	push @filters, "c.collection_no = -1";
    }
    
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
	$tables_ref->{c} = 1;
	$tables_ref->{non_summary} = 1;
    }
    
    # Now we need to figure out if the 'c' table is needed.

    if ( $tables_ref->{o} || $tables_ref->{t} || $tables_ref->{tf} || $tables_ref->{v} )
    {
	$tables_ref->{o} = 1;
	$tables_ref->{c} = 1;
    }

    elsif ( $tables_ref->{cc} )
    {
	$tables_ref->{c} = 1;
    }
    
    # Check for interval parameters. If no time rule is specified, it defaults to 'major'.
    
    my $time_rule = $request->clean_param('timerule') || 'major';
    
    my ($early_age, $late_age, $early_interval_no, $late_interval_no) = $request->process_interval_params;
    
    # If this is a summary or prevalence operation and no interval bounds were given at all, then
    # we can just query on interval_no = 0 in the summary bin table.

    if ( ( $op eq 'summary' || $op eq 'prevalence' ) &&
	 ( $time_rule eq 'major' ) &&
	 ( ! $tables_ref->{c} ) && 
	 ( ! $early_interval_no && ! $early_age && ! $late_age ) )
    {
	push @filters, "s.interval_no = 0";
    }
    
    # If this is a summary or prevalence operation using the 'major' time rule and covering a
    # single interval, then we can just query on the appropriate interval_no.
    
    elsif ( ( $op eq 'summary' || $op eq 'prevalence' ) &&
	    ( $time_rule eq 'major' ) &&
	    ( ! $tables_ref->{c} ) && 
	    ( $early_interval_no && ( ! $late_interval_no || $early_interval_no == $late_interval_no ) ) )
    {
	push @filters, "s.interval_no = $early_interval_no";
    }
    
    # Otherwise, if age bounds were given we need to join to the collection table and filter on
    # actual collection ages.
    
    elsif ( $early_age || $late_age )
    {
	$tables_ref->{c} = 1;
	$tables_ref->{non_summary} = 1;
	
	$request->{early_age} = $early_age;
	$request->{late_age} = $late_age;

	push @filters, "s.interval_no = 0" if $op eq 'summary' || $op eq 'prevalence';
	
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

	elsif ( $time_rule eq 'major' )
	{
	    my $ea = $early_age ? $early_age + 0 : 5000;
	    my $la = $late_age ? $late_age + 0 : 0;

	    push @filters, "if(c.late_age >= $la,
		    if(c.early_age <= $ea, c.early_age - c.late_age, $ea - c.late_age),
		    if(c.early_age > $ea, $ea - $la, c.early_age - $la)) / (c.early_age - c.late_age) >= 0.5"
	}

	else # $time_rule eq 'buffer'
	{
	    my $early_buffer = $request->clean_param('timebuffer');
	    my $late_buffer = $request->clean_param('latebuffer');
	    
	    my ($early_bound, $late_bound);
	    
	    if ( $early_age )
	    {
		unless ( $early_buffer )
		{
		    $early_buffer = $early_age > 66 ? 12 : 5;
		}

		$early_bound = $early_age + $early_buffer;
	    }

	    if ( $late_age )
	    {
		$late_buffer ||= $early_buffer;
		
		unless ( $late_buffer )
		{
		    $late_buffer = $late_age > 66 ? 12 : 5;
		}

		$late_bound = $late_age - $late_buffer;
		$late_bound = 0 if $late_bound < 0;
	    }
	    
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
    
    # Return the list
    
    return @filters;
}


sub verify_coll_param {

    my ($request, $dbh, $api_field, $values_ref, $db_field) = @_;
    
    $db_field ||= $api_field;
    
    # Construct a list of quoted values.
    
    my $value_list = '';
    my $sep = '';
    
    foreach my $v ( @$values_ref )
    {
	$value_list .= $sep;
	$value_list .= $dbh->quote($v);
	$sep = ',';
    }
    
    # Now verify that these values actually appear in the database, and add warnings for
    # those which do not.
    
    my $sql = "
	SELECT distinct $db_field FROM collections
	WHERE $db_field in ($value_list)";
    
    print STDERR "$sql\n\n" if $request->debug;
    
    my $result = $dbh->selectcol_arrayref($sql);
    
    my %verified;
    my @bad;
    
    if ( ref $result eq 'ARRAY' )
    {
	%verified = map { lc $_ => 1 } @$result;
    }
    
    foreach my $v ( @$values_ref )
    {
	push @bad, $v unless $verified{lc $v};
    }

    if ( @bad )
    {
	my $bad_list = join("', '", @bad);

	if ( @bad == 1 )
	{
	    $request->add_warning("Field '$api_field': the value '$bad_list' was not found in the database");
	}

	else
	{
	    $request->add_warning("Field '$api_field': the values '$bad_list' were not found in the database");
	}
    }
    
    return $value_list;
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
    
    my $interval_no = $request->clean_param('interval_id');
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
	    push @fields, "cc.plate as $plate_field";
	    push @fields, "'scotese' as $model_field";
	    $tables_ref->{cc} = 1;
	    # $plate_version_shown{'scotese'} = 1;
	}
	
	elsif ( $model eq 'gplates' || $model eq 'gp_mid' )
	{
	    push @fields, "pc.mid_lng as $lng_field", "pc.mid_lat as $lat_field";
	    push @fields, "pc.plate_no as $plate_field";
	    push @fields, "'gp_mid' as $model_field";
	    $tables_ref->{pc} = 1;
	    # $plate_version_shown{'gplates'} = 1;
	}
	
	elsif ( $model eq 'gp_early' )
	{
	    push @fields, "pc.early_lng as $lng_field", "pc.early_lat as $lat_field";
	    push @fields, "pc.plate_no as $plate_field";
	    push @fields, "'gp_early' as $model_field";
	    $tables_ref->{pc} = 1;
	    # $plate_version_shown{'gplates'} = 1;
	}
	
	elsif ( $model eq 'gp_late' )
	{
	    push @fields, "pc.late_lng as $lng_field", "pc.late_lat as $lat_field";
	    push @fields, "pc.plate_no as $plate_field";
	    push @fields, "'gp_late' as $model_field";
	    $tables_ref->{pc} = 1;
	    # $plate_version_shown{'gplates'} = 1;
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
    my $tt = $options->{tt} || 't';
    
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

	if ( $term eq 'id' )
	{
	    $dir ||= 'asc';
	    
	    if ( $bt eq 'o' )
	    {
		push @exprs, "o.occurrence_no $dir";
	    }
	    
	    elsif ( $bt eq 'ss' )
	    {
		push @exprs, "ss.specimen_no $dir";
	    }

	    elsif ( $bt eq 'c' || $bt eq 'cc' )
	    {
		push @exprs, "$bt.collection_no $dir";
	    }

	    $tables->{$bt} = 1;
	}

	elsif ( $term eq 'ref' )
	{
	    $dir ||= 'asc';
	    
	    push @exprs, "$bt.reference_no $dir";
	}

	elsif ( $term eq 'hierarchy' )
	{
	    if ( defined $dir && $dir eq 'desc' )
	    {
		push @exprs, "if($tt.lft > 0, 0, 1) desc, $tt.left desc";
	    }

	    else
	    {
		push @exprs, "if($tt.lft > 0, 0, 1), $tt.lft asc";
	    }

	    $tables->{$tt} = 1;
	}

	elsif ( $term eq 'identification' )
	{
	    $dir ||= 'asc';

	    push @exprs, "ifnull($tt.name, concat($bt.genus_name, ' ', $bt.species_name)) $dir";
	    $tables->{$tt} = 1;
	    $tables->{$bt} = 1;
	}
	
	elsif ( $term eq 'max_ma' )
	{
	    $dir ||= 'desc';
	    push @exprs, "$at.early_age $dir";
	    $tables->{$at} = 1;
	}
	
	elsif ( $term eq 'min_ma' )
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
		push @exprs, "c.s_plate_no $dir";
		# $tables->{cc} = 1;
	    }
	    
	    else
	    {
		push @exprs, "c.g_plate_no $dir";
		# $tables->{pc} = 1;
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
	    push @exprs, "coalesce(cs.grp, cs.formation, cs.member) $dir, cs.formation $dir, cs.member $dir"
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
    $tables->{t} = 1 if $tables->{ph} || $tables->{pl} || $tables->{tv};
    
    my $t = $tables->{tv} ? 'tv' : 't';
    
    # Create the necessary join expressions.
    
    $join_list .= "JOIN occ_matrix as o on o.collection_no = c.collection_no\n"
	if $tables->{o};
    $join_list .= "JOIN occurrences as oc using (occurrence_no)\n"
	if $tables->{oc};
    $join_list .= "JOIN taxon_trees as t using (orig_no)\n"
	if $tables->{t} || $tables->{tf} || $tables->{v};
    $join_list .= "JOIN taxon_trees as tv on tv.orig_no = t.accepted_no\n"
	if $tables->{tv};
    $join_list .= "JOIN taxon_attrs as v on v.orig_no = $t.orig_no\n"
	if $tables->{v};
    $join_list .= "LEFT JOIN taxon_lower as pl on pl.orig_no = $t.orig_no\n"
	if $tables->{pl};
    $join_list .= "LEFT JOIN taxon_ints as ph on ph.ints_no = $t.ints_no\n"
	if $tables->{ph};
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
    
    $join_list .= "LEFT JOIN $COLL_LITH as cl on cl.collection_no = c.collection_no\n"
	if $tables->{cl};
    
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


sub process_summary_ids {

    my ($request, $record) = @_;
    
    return unless $request->{block_hash}{extids};
    
    # my $make_ids = $request->clean_param('extids');
    # $make_ids = 1 if ! $request->param_given('extids') && $request->output_vocab eq 'com';
    
    # return unless $make_ids;
    
    # $request->delete_output_field('record_type');
    
    foreach my $f ( qw(bin_id bin_id_1 bin_id_2 bin_id_3 bin_id_4) )
    {
	$record->{$f} = generate_identifier('CLU', $record->{$f})
	    if $record->{$f};
    }
}


# process_paleocoords ( record )
# 
# If any of the paleocoords are blank, add to the corresponding 'geoplate' field a message to the
# effect that the coordinates for this collection cannot be computed using this model. We put the
# message in the 'geoplate' field because this is an unstructured text field, while the lat/lng
# fields should either contain numbers or be blank.

sub process_paleocoords {
    
    my ($request, $record) = @_;
    
    foreach my $label ( '', '2', '3', '4' )
    {
	last unless $record->{ 'paleomodel' . $label };
	
	unless ( $record->{ 'paleolat' . $label } || $record->{ 'paleolng' . $label } )
	{
	    $record->{ 'geoplate' . $label } = "coordinates not computable using this model";
	}
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
	unless $value =~ qr{ ^ ( [!^]? ) ( [a-z]{2,3} ) $ }xsi;
    
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

$ETVALUE{terr} = {'terrestrial indet.' => 1, 'fluvial indet.' => 1, 'alluvial fan' => 1, 'channel lag' => 1, 'coarse channel fill' => 1, 'fine channel fill' => 1, '"channel"' => 1, 'wet floodplain' => 1, 'dry floodplain' => 1, '"floodplain"' => 1, 'crevasse splay' => 1, 'levee' => 1, 'mire/swamp' => 1, 'fluvial-lacustrine indet.' => 1, 'delta plain' => 1, 'fluvial-deltaic indet.' => 1, 'lacustrine - large' => 1, 'lacustrine - small' => 1, 'pond' => 1, 'crater lake' => 1, 'lacustrine delta plain' => 1, 'lacustrine interdistributary bay' => 1, 'lacustrine delta front' => 1, 'lacustrine prodelta' => 1, 'lacustrine deltaic indet.' => 1, 'lacustrine indet.' => 1, 'dune' => 1, 'interdune' => 1, 'loess' => 1, 'eolian indet.' => 1, 'cave' => 1, 'fissure fill' => 1, 'sinkhole' => 1, 'karst indet.' => 1, 'tar' => 1, 'mire/swamp' => 1, 'spring' => 1, 'glacial' => 1};

$ETVALUE{carbonate} = {'carbonate indet.' => 1, 'peritidal' => 1, 'shallow subtidal indet.' => 1, 'open shallow subtidal' => 1, 'lagoonal/restricted shallow subtidal' => 1, 'sand shoal' => 1, 'reef, buildup or bioherm' => 1, 'perireef or subreef' => 1, 'intrashelf/intraplatform reef' => 1, 'platform/shelf-margin reef' => 1, 'slope/ramp reef' => 1, 'basin reef' => 1, 'deep subtidal ramp' => 1, 'deep subtidal shelf' => 1, 'deep subtidal indet.' => 1, 'offshore ramp' => 1, 'offshore shelf' => 1, 'offshore indet.' => 1, 'slope' => 1, 'basinal (carbonate)' => 1, 'basinal (siliceous)' => 1 };

$ETVALUE{silicic} = {'marginal marine indet.' => 1, 'coastal indet.' => 1, 'estuary/bay' => 1, 'lagoonal' => 1, 'paralic indet.' => 1, 'delta plain' => 1, 'interdistributary bay' => 1, 'delta front' => 1, 'prodelta' => 1, 'deltaic indet.' => 1, 'foreshore' => 1, 'shoreface' => 1, 'transition zone/lower shoreface' => 1, 'offshore' => 1, 'coastal indet.' => 1, 'submarine fan' => 1, 'basinal (siliciclastic)' => 1, 'basinal (siliceous)' => 1, 'basinal (carbonate)' => 1, 'deep-water indet.' => 1 };

$ETVALUE{marine} = { 'marine indet.' => 1, 'carbonate indet.' => 1, 'peritidal' => 1, 'shallow subtidal indet.' => 1, 'open shallow subtidal' => 1, 'lagoonal/restricted shallow subtidal' => 1, 'sand shoal' => 1, 'reef, buildup or bioherm' => 1, 'perireef or subreef' => 1, 'intrashelf/intraplatform reef' => 1, 'platform/shelf-margin reef' => 1, 'slope/ramp reef' => 1, 'basin reef' => 1, 'deep subtidal ramp' => 1, 'deep subtidal shelf' => 1, 'deep subtidal indet.' => 1, 'offshore ramp' => 1, 'offshore shelf' => 1, 'offshore indet.' => 1, 'slope' => 1, 'basinal (carbonate)' => 1, 'basinal (siliceous)' => 1, 'marginal marine indet.' => 1, 'coastal indet.' => 1, 'estuary/bay' => 1, 'lagoonal' => 1, 'paralic indet.' => 1, 'delta plain' => 1, 'interdistributary bay' => 1, 'delta front' => 1, 'prodelta' => 1, 'deltaic indet.' => 1, 'foreshore' => 1, 'shoreface' => 1, 'transition zone/lower shoreface' => 1, 'offshore' => 1, 'coastal indet.' => 1, 'submarine fan' => 1, 'basinal (siliciclastic)' => 1, 'basinal (siliceous)' => 1, 'basinal (carbonate)' => 1, 'deep-water indet.' => 1 };

$EZVALUE{lacust} = {'lacustrine - large' => 1, 'lacustrine - small' => 1, 'pond' => 1, 'crater lake' => 1, 'lacustrine delta plain' => 1, 'lacustrine interdistributary bay' => 1, 'lacustrine delta front' => 1, 'lacustrine prodelta' => 1, 'lacustrine deltaic indet.' => 1, 'lacustrine indet.' => 1 };

$EZVALUE{fluvial} = {'fluvial indet.' => 1, 'alluvial fan' => 1, 'channel lag' => 1, 'coarse channel fill' => 1, 'fine channel fill' => 1, '"channel"' => 1, 'wet floodplain' => 1, 'dry floodplain' => 1, '"floodplain"' => 1, 'crevasse splay' => 1, 'levee' => 1, 'mire/swamp' => 1, 'fluvial-lacustrine indet.' => 1, 'delta plain' => 1, 'fluvial-deltaic indet.' => 1 };

$EZVALUE{karst} = {'cave' => 1, 'fissure fill' => 1, 'sinkhole' => 1, 'karst indet.' => 1 };

$EZVALUE{terrother} = {'dune' => 1, 'interdune' => 1, 'loess' => 1, 'eolian indet.' => 1, 'tar' => 1, 'spring' => 1, 'glacial' => 1 };

$EZVALUE{terrindet} = { 'terrestrial indet.' => 1 };

$EZVALUE{marginal} = {'marginal marine indet.' => 1, 'peritidal' => 1, 'lagoonal/restricted shallow subtidal' => 1, 'estuary/bay' => 1, 'lagoonal' => 1, 'paralic indet.' => 1, 'delta plain' => 1, 'interdistributary bay' => 1 };

$EZVALUE{reef} = {'reef, buildup or bioherm' => 1, 'perireef or subreef' => 1, 'intrashelf/intraplatform reef' => 1, 'platform/shelf-margin reef' => 1, 'slope/ramp reef' => 1, 'basin reef' => 1 };

$EZVALUE{stshallow} = {'shallow subtidal indet.' => 1, 'open shallow subtidal' => 1, 'delta front' => 1, 'foreshore' => 1, 'shoreface' => 1, 'sand shoal' => 1 };

$EZVALUE{stdeep} = {'transition zone/lower shoreface' => 1, 'deep subtidal ramp' => 1, 'deep subtidal shelf' => 1, 'deep subtidal indet.' => 1 };

$EZVALUE{offshore} = {'offshore ramp' => 1, 'offshore shelf' => 1, 'offshore indet.' => 1, 'prodelta' => 1, 'offshore' => 1 };

$EZVALUE{slope} = {'slope' => 1, 'basinal (carbonate)' => 1, 'basinal (siliceous)' => 1, 'submarine fan' => 1, 'basinal (siliciclastic)' => 1, 'basinal (siliceous)' => 1, 'basinal (carbonate)' => 1, 'deep-water indet.' => 1 };

$EZVALUE{marindet} = {'marine indet.' => 1, 'carbonate indet.' => 1, 'coastal indet.' => 1, 'deltaic indet.' => 1 };


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


sub process_coll_ids {
    
    my ($request, $record) = @_;
    
    return unless $request->{block_hash}{extids};
    
    # my $make_ids = $request->clean_param('extids');
    # $make_ids = 1 if ! $request->param_given('extids') && $request->output_vocab eq 'com';
    
    # return unless $make_ids;
    
    # $request->delete_output_field('record_type');
    
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
    
    if ( defined $record->{reference_nos} )
    {
	my @list = split(',', $record->{reference_nos});
	
	foreach my $f (@list)
	{
	    $f = generate_identifier('REF', $f);
	}
	
	$record->{reference_nos} = \@list;
    }
    
    if ( defined $record->{bin_id_1} )
    {
	foreach my $f ( qw(bin_id_1 bin_id_2 bin_id_3) )
	{
	    $record->{$f} = generate_identifier('CLU', $record->{$f}) if defined $record->{$f}
	}
    }
}


sub process_permissions {
    
    my ($request, $record) = @_;
    
    return unless $record->{access_level};
    
    if ( $record->{access_level} == 1 )
    {
	return 'members';
    }
    
    elsif ( $record->{access_resgroup} )
    {
	return "group($record->{access_resgroup})";
    }
    
    else
    {
	return "authorizer";
    }
}


# subroutines for generating time bin lists

sub generate_timebins {
    
    my ($request, $record) = @_;
    
    unless ( $request->{my_binrule} )
    {
	$request->setup_time_variables;
    }
    
    my @bin_list = $request->bin_by_interval($record, $request->{my_boundary_list}, $request->{my_binrule},
					     $request->{my_timebuffer}, $request->{my_latebuffer});
    
    $record->{time_bins} = $request->generate_bin_names(@bin_list);
}


sub generate_timecompare {

    my ($request, $record) = @_;
    
    unless ( $request->{my_binrule} )
    {
	$request->setup_time_variables;
    }
    
    my @bin_contain = $request->bin_by_interval($record, $request->{my_boundary_list}, 'contain');
    
    $record->{time_contain} = $request->generate_bin_names(@bin_contain);
    
    my @bin_major = $request->bin_by_interval($record, $request->{my_boundary_list}, 'major');
    
    $record->{time_major} = $request->generate_bin_names(@bin_major);
    
    my @bin_buffer = $request->bin_by_interval($record, $request->{my_boundary_list}, 'buffer',
					     $request->{my_timebuffer}, $request->{my_latebuffer});
    
    $record->{time_buffer} = $request->generate_bin_names(@bin_buffer);
    
    my @bin_overlap = $request->bin_by_interval($record, $request->{my_boundary_list}, 'overlap');
    
    $record->{time_overlap} = $request->generate_bin_names(@bin_overlap);
}


sub setup_time_variables {

    my ($request) = @_;
    
    $request->{my_binrule} = $request->clean_param('timerule') || 'major';
    $request->{my_timebuffer} = $request->clean_param('timebuffer');
    $request->{my_latebuffer} = $request->clean_param('latebuffer');
    
    my ($early, $late) = $request->process_interval_params;
    
    $request->{my_early} = $early;
    $request->{my_late} = $late;
    
    my $scale_no = $request->clean_param('scale_id') || 1;
    my $scale_level = $request->clean_param('reso') || $PB2::IntervalData::SDATA{$scale_no}{levels};
    
    if ( ! defined $scale_level ) { $scale_level = 1 }
	elsif ( $scale_level eq 'epoch' ) { $scale_level = 4 }
    elsif ( $scale_level eq 'period' ) { $scale_level = 3 }
    elsif ( $scale_level eq 'era' ) { $scale_level = 2 }
    
    $request->{my_scale_no} = $scale_no;
    $request->{my_scale_level} = $scale_level;
    
    my @bins;
    
    foreach my $b ( @{$PB2::IntervalData::BOUNDARY_LIST{$scale_no}{$scale_level}} )
    {
	push @bins, $b unless defined $early && $b > $early || defined $late && $b < $late; 
    }
    
    $request->{my_boundary_list} = \@bins;
    $request->{my_boundary_map} = $PB2::IntervalData::BOUNDARY_MAP{$scale_no}{$scale_level};
}


sub generate_bin_names {

    my $request = shift;
    
    if ( @_ == 0 )
    {
	return '-';
    }
    
    elsif ( my $bm = $request->{my_boundary_map} )
    {    
	return join ', ', map { $bm->{$_}{interval_name} || 'x' } @_;
    }
    
    else
    {
	return 'UNKNOWN TIMESCALE';
    }
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


# check_access ( record )
# 
# Check to make sure that this record can properly be accessed by the
# requestor.  Return 1 if so, false otherwise.

# sub check_access {
    
#     my ($request, $record) = @_;
    
#     # If the record does not include an access level field, or if the value is
#     # zero, then the record is accessible.
    
#     return 1 unless $record->{access_level};
    
#     # If the requestor is authenticated as a database member, the record is
#     # accessible if its level is 1 or less.  The 'access_level' field will
#     # only be part of the record if the requestor is authenticated.
    
#     return 1 if $record->{access_level} <= 1;
    
#     # Otherwise, we will need to check individual access rights.  If the
#     # record's access_no (i.e. authorizer_no) field is the same as the
#     # requestor's, then the record is accessible.
    
#     return 1 if $record->{access_no} && $request->{my_authorizer_no} &&
# 	$record->{access_no} eq $request->{my_authorizer_no};
    
#     # Otherwise, we have to check whether permission was granted by the
#     # record's authorizer.  If we haven't yet done so, grab a list of all
#     # authorizers who have permitted the requestor to edit their collections
#     # and also a list of the requestor's research groups.
    
#     unless ( $request->{my_permissions} )
#     {
# 	my (%permissions, %groups);
	
# 	my $dbh = $request->get_connection;
# 	my $auth_no = $request->{my_authorizer_no};
# 	my $sql = "SELECT authorizer_no FROM permissions WHERE modifier_no=$auth_no";
	
# 	my ($permlist) = $dbh->selectcol_arrayref($sql);
	
# 	%permissions = map { $_ => 1 } @$permlist if ref $permlist eq 'ARRAY';
	
# 	$sql = "SELECT research_group FROM person WHERE person_no=$auth_no";
	
# 	my ($grouplist) = $dbh->selectrow_array($sql);
	
# 	%groups = map { $_ => 1 } split( /\s*,\s*/, $grouplist ) if $grouplist;
	
# 	$request->{my_permissions} = \%permissions;
# 	$request->{my_groups} = \%groups;
#     }
    
#     return 1 if $request->{my_permissions}{$record->{access_no}};
    
#     # Otherwise, we have to check whether permission was granted for the
#     # requestor's research group(s).
    
#     if ( $record->{access_resgroup} )
#     {
# 	my @groups = split( /\s*,\s*/, $record->{access_resgroup} );
	
# 	foreach my $g ( @groups )
# 	{
# 	    return 1 if $request->{my_groups}{$g};
# 	}
#     }
    
#     # If none of these rules grant access, then access is denied.
    
#     return 0;
# }


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
