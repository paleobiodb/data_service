#
# TaxonData
# 
# A class that returns information from the PaleoDB database about a single
# taxon or a category of taxa.  This is a subclass of DataService::Base.
# 
# Author: Michael McClennen

use strict;

package PB1::TaxonData;

use HTTP::Validate qw(:validators);
use Carp qw(carp croak);

use TaxonDefs qw(%TAXON_TABLE %TAXON_RANK %RANK_STRING);
use TableDefs qw($PHYLOPICS $PHYLOPIC_NAMES);
use TaxonomyOld;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB1::CommonData PB1::ReferenceData);

our (%DB_FIELD);

# This routine is called by the data service in order to initialize this
# class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # First define an output map to specify which output blocks are going to
    # be used to generate output from the operations defined in this class.
    
    $ds->define_output_map('1.1:taxa:output_map' =>
	{ value => 'attr', maps_to => '1.1:taxa:attr' },
	    "The attribution of this taxon (author and year)",
	# { value => 'ref', maps_to => '1.1:common:ref' },
	#     "The source from which this taxon was entered into the database",
	{ value => 'app', maps_to => '1.1:taxa:app' },
	    "The age of first and last appearance of this taxon from the occurrences",
	    "recorded in this database",
	{ value => 'size', maps_to => '1.1:taxa:size' },
	    "The number of subtaxa appearing in this database",
	{ value => 'phylo', maps_to => '1.1:taxa:phylo' },
	    "The classification of this taxon: kingdom, phylum, class, order, family.",
	    "This information is also included in the C<nav> block, so do not specify both at once.",
	{ value => 'nav', maps_to => '1.1:taxa:nav' },
	    "Additional information for the PBDB Navigator taxon browser.",
	    "This block should only be selected in conjunction with the JSON format.",
	{ value => 'img', maps_to => '1.1:taxa:img' },
	    "The identifier of the image (if any) associated with this taxon.",
	    "These images are sourced from L<phylopic.org>.",
	{ value => 'ent', maps_to => '1.1:taxa:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.1:taxa:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.1:taxa:crmod' },
	    "The C<created> and C<modified> timestamps for the collection record");
    
    # Now define all of the output blocks that were not defined elsewhere.
    
    $ds->define_block('1.1:taxa:basic' =>
	{ select => 'link' },
	{ output => 'taxon_no', dwc_name => 'taxonID', com_name => 'oid' },
	    "A positive integer that uniquely identifies this taxonomic name",
	{ output => 'orig_no', com_name => 'gid' },
	    "A positive integer that uniquely identifies the taxonomic concept",
	{ output => 'record_type', com_name => 'typ', com_value => 'txn', 
	  dwc_value => 'Taxon', value => 'taxon' },
	    "The type of this record.  By vocabulary:", "=over",
	    "=item pbdb", "taxon", "=item com", "txn", "=item dwc", "Taxon", "=back",
	{ output => 'associated_records', com_name => 'rct' },
	    "The number of records (occurrences, references, etc. depending upon which URL path you used) associated with this taxonomic name",
	{ set => 'rank', if_vocab => 'pbdb,dwc', lookup => \%RANK_STRING },
	{ output => 'rank', dwc_name => 'taxonRank', com_name => 'rnk' },
	    "The rank of this taxon, ranging from subspecies up to kingdom",
	{ output => 'taxon_name', dwc_name => 'scientificName', com_name => 'nam' },
	    "The scientific name of this taxon",
	{ output => 'common_name', dwc_name => 'vernacularName', com_name => 'nm2' },
	    "The common (vernacular) name of this taxon, if any",
	{ set => 'attribution', if_field => 'a_al1', from => '*', 
	  code => \&PB1::CommonData::generateAttribution },
	{ output => 'attribution', if_block => 'attr', 
	  dwc_name => 'scientificNameAuthorship', com_name => 'att' },
	    "The attribution (author and year) of this taxonomic name",
	{ output => 'pubyr', if_block => 'attr', 
	  dwc_name => 'namePublishedInYear', com_name => 'pby' },
	    "The year in which this name was published",
	{ output => 'status', com_name => 'sta' },
	    "The taxonomic status of this name",
	{ output => 'parent_no', dwc_name => 'parentNameUsageID', com_name => 'par' }, 
	    "The identifier of the parent taxonomic concept, if any",
	{ output => 'synonym_no', dwc_name => 'acceptedNameUsageID', pbdb_name => 'senior_no', 
	  com_name => 'snr', dedup => 'orig_no' },
	    "The identifier of the senior synonym of this taxonomic concept, if any",
	{ output => 'reference_no', com_name => 'rid', show_as_list => 1 },
	    "A list of identifiers indicating the source document(s) from which this name was entered.",
	{ output => 'is_extant', com_name => 'ext', dwc_name => 'isExtant' },
	    "True if this taxon is extant on earth today, false if not, not present if unrecorded");
    
    $ds->define_block('1.1:taxa:attr' =>
	{ select => 'attr' });
    
    $ds->define_block('1.1:taxa:size' =>
	{ select => 'size' },
	{ output => 'size', com_name => 'siz' },
	    "The total number of taxa in the database that are contained within this taxon, including itself",
	{ output => 'extant_size', com_name => 'exs' },
	    "The total number of extant taxa in the database that are contained within this taxon, including itself");
    
    $ds->define_block('1.1:taxa:app' =>
	{ select => 'app' },
	{ output => 'firstapp_ea', com_name => 'fea', dwc_name => 'firstAppearanceEarlyAge', 
	  if_block => 'app' },
	    "The early age bound for the first appearance of this taxon in the database",
	{ output => 'firstapp_la', com_name => 'fla', dwc_name => 'firstAppearanceLateAge', 
	  if_block => 'app' }, 
	    "The late age bound for the first appearance of this taxon in the database",
	{ output => 'lastapp_ea', com_name => 'lea', dwc_name => 'lastAppearanceEarlyAge',
	  if_block => 'app' },
	    "The early age bound for the last appearance of this taxon in the database",
	{ output => 'lastapp_la', com_name => 'lla', dwc_name => 'lastAppearanceLateAge',
	  if_block => 'app' }, 
	    "The late age bound for the last appearance of this taxon in the database");
    
    $ds->define_block('1.1:taxa:subtaxon' =>
	{ output => 'taxon_no', com_name => 'oid', dwc_name => 'taxonID' },
	{ output => 'orig_no', com_name => 'gid' },
	{ output => 'record_type', com_name => 'typ', com_value => 'txn' },
	{ output => 'taxon_rank', com_name => 'rnk', dwc_name => 'taxonRank' },
	{ output => 'taxon_name', com_name => 'nam', dwc_name => 'scientificName' },
	{ output => 'synonym_no', com_name => 'snr', pbdb_name => 'senior_no', 
	  dwc_name => 'acceptedNameUsageID', dedup => 'orig_no' },
	{ output => 'size', com_name => 'siz' },
	{ output => 'extant_size', com_name => 'exs' },
	{ output => 'firstapp_ea', com_name => 'fea' });
    
    $ds->define_block('1.1:taxa:phylo' =>
	{ select => 'phylo' },
	{ output => 'kingdom', com_name => 'kgl' },
	    "The name of the kingdom in which this taxon occurs",
	{ output => 'phylum', com_name => 'phl' },
	    "The name of the phylum in which this taxon occurs",
	{ output => 'class', com_name => 'cll' },
	    "The name of the class in which this taxon occurs",
	{ output => 'order', com_name => 'odl' },
	    "The name of the order in which this taxon occurs",
	{ output => 'family', com_name => 'fml' },
	    "The name of the family in which this taxon occurs");
    
    $ds->define_block('1.1:taxa:nav' =>
	{ select => ['link', 'parent', 'phylo', 'counts'] },
	{ output => 'parent_name', com_name => 'prl', dwc_name => 'parentNameUsage' },
	    "The name of the parent taxonomic concept, if any",
	{ output => 'parent_rank', com_name => 'prr' },
	    "The rank of the parent taxonomic concept, if any",
	{ output => 'parent_txn', com_name => 'prt', sub_record => '1.1:taxa:subtaxon' },
	{ output => 'kingdom_no', com_name => 'kgn' },
	    "The identifier of the kingdom in which this taxon occurs",
	{ output => 'kingdom', com_name => 'kgl' },
	    "The name of the kingdom in which this taxon occurs",
	{ output => 'kingdom_txn', com_name => 'kgt', sub_record => '1.1:taxa:subtaxon' },
	{ output => 'phylum_no', com_name => 'phn' },
	    "The identifier of the phylum in which this taxon occurs",
	{ output => 'phylum', com_name => 'phl' },
	    "The name of the phylum in which this taxon occurs",
	{ output => 'phylum_txn', com_name => 'pht', sub_record => '1.1:taxa:subtaxon' },
	{ output => 'phylum_count', com_name => 'phc' },
	    "The number of phyla within this taxon",
	{ output => 'class_no', com_name => 'cln' },
	    "The identifier of the class in which this taxon occurs",
	{ output => 'class', com_name => 'cll' },
	    "The name of the class in which this taxon occurs",
	{ output => 'class_txn', com_name => 'clt', sub_record => '1.1:taxa:subtaxon' },
	{ output => 'class_count', com_name => 'clc' },
	    "The number of classes within this taxon",
	{ output => 'order_no', com_name => 'odn' },
	    "The identifier of the order in which this taxon occurs",
	{ output => 'order', com_name => 'odl' },
	    "The name of the order in which this taxon occurs",
	{ output => 'order_txn', com_name => 'odt', sub_record => '1.1:taxa:subtaxon' },
	{ output => 'order_count', com_name => 'odc' },
	    "The number of orders within this taxon",
	{ output => 'family_no', com_name => 'fmn' },
	    "The identifier of the family in which this taxon occurs",
	{ output => 'family', com_name => 'fml' },
	    "The name of the family in which this taxon occurs",
	{ output => 'family_txn', com_name => 'fmt', sub_record => '1.1:taxa:subtaxon' },
	{ output => 'family_count', com_name => 'fmc' },
	    "The number of families within this taxon",
	{ output => 'genus_count', com_name => 'gnc' },
	    "The number of genera within this taxon",
    
	{ output => 'children', com_name => 'chl', sub_record => '1.1:taxa:subtaxon' },
	    "The immediate children of this taxonomic concept, if any",
	{ output => 'phylum_list', com_name => 'phs', sub_record => '1.1:taxa:subtaxon' },
	    "A list of the phyla within this taxonomic concept",
	{ output => 'class_list', com_name => 'cls', sub_record => '1.1:taxa:subtaxon' },
	    "A list of the classes within this taxonomic concept",
	{ output => 'order_list', com_name => 'ods', sub_record => '1.1:taxa:subtaxon' },
	    "A list of the orders within this taxonomic concept",
	{ output => 'family_list', com_name => 'fms', sub_record => '1.1:taxa:subtaxon' },
	    "A list of the families within this taxonomic concept",
	{ output => 'genus_list', com_name => 'gns', sub_record => '1.1:taxa:subtaxon' },
	    "A list of the genera within this taxonomic concept",
	{ output => 'subgenus_list', com_name => 'sgs', sub_record => '1.1:taxa:subtaxon' },
	    "A list of the subgenera within this taxonomic concept",
	{ output => 'species_list', com_name => 'sps', sub_record => '1.1:taxa:subtaxon' },
	    "A list of the species within this taxonomic concept",
 	{ output => 'subspecies_list', com_name => 'sss', sub_record => '1.1:taxa:subtaxon' },
	    "A list of the subspecies within this taxonomic concept");
    
    $ds->define_block('1.1:taxa:img' =>
	{ select => 'img' },
	{ output => 'image_no', com_name => 'img' },
    	    "If this value is non-zero, you can use it to construct image URLs",
	    "using L<taxa/thumb|node:taxa/thumb> and L<taxa/icon|node:taxa/icon>.");
    
    $ds->define_block('1.1:taxa:crmod' =>
	{ select => 'created' },
	{ output => 'created', com_name => 'dcr' },
	  "The date and time at which this record was created.",
	{ output => 'modified', com_name => 'dmd' },
	  "The date and time at which this record was last modified.");
    
    $ds->define_block('1.1:taxa:ent' =>
	{ select => 'person' },
	{ output => 'authorizer_no', com_name => 'ati', if_block => 'ent,entname' },
	    "The identifier of the person who authorized the entry of this record",
	{ output => 'enterer_no', com_name => 'eni', if_block => 'ent,entname' },
	    "The identifier of the person who actually entered this record.",
	{ output => 'modifier_no', com_name => 'mdi', if_block => 'ent,entname' },
	    "The identifier of the person who last modified this record, if it has been modified.");
    
    $ds->define_block('1.1:taxa:entname' =>
	{ select => 'person' },
	{ output => 'authorizer_name', pbdb_name => 'authorizer', com_name => 'ath' },
	    "The name of the person who authorized the entry of this record",
	{ output => 'enterer_name', pbdb_name => 'enterer', com_name => 'ent' },
	    "The name of the person who actually entered this record",
	{ output => 'modifier_name', pbdb_name => 'modifier', com_name => 'mdf' },
	    "The name of the person who last modified this record, if it has been modified.");
    
    $ds->define_block('1.1:taxa:auto' =>
	{ output => 'taxon_no', dwc_name => 'taxonID', com_name => 'oid' },
	    "A positive integer that uniquely identifies this taxonomic name",
	{ output => 'record_type', com_name => 'typ', com_value => 'txn', dwc_value => 'Taxon', value => 'taxon' },
	    "The type of this object: {value} for a taxonomic name",
	{ set => 'taxon_rank', if_vocab => 'com', lookup => \%TAXON_RANK },
	{ output => 'taxon_rank', dwc_name => 'taxonRank', com_name => 'rnk' },
	    "The taxonomic rank of this name",
	{ output => 'taxon_name', dwc_name => 'scientificName', com_name => 'nam' },
	    "The scientific name of this taxon",
	{ output => 'misspelling', com_name => 'msp' },
	    "If this name is marked as a misspelling, then this field will be included with the value '1'",
	{ output => 'n_occs', com_name => 'noc' },
	    "The number of occurrences of this taxon in the database");
    
    $ds->define_block('1.1:taxa:imagedata' =>
	{ select => [ 'image_no', 'uid', 'modified', 'credit', 'license' ] },
	{ output => 'image_no', com_name => 'oid' },
	    "A unique identifier for this image, generated locally by this database",
	{ output => 'type', value => 'image', com_name => 'typ', com_value => 'img' },
	    "The type of this record: 'img' for an image",
	{ output => 'taxon_no', com_name => 'tid' },
	    "The identifier of the taxon with which this image is associated.  This",
	    "field will only appear in results generated by L<taxa/list_images|node:taxa/list_images>.",
	{ output => 'taxon_name', com_name => 'tna' },
	    "The taxonomic name with which this image is associated.  This field",
	    "will only appear in results generated by L<taxa/list_images|node:taxa/list_images>.",
	{ output => 'uid', com_name => 'uid' },
	    "A unique identifier for this image generated by phylopic.org",
	{ output => 'modified', com_name => 'dmd' },
	    "The date and time at which this image was last modified on phylopic.org",
	{ output => 'credit', com_name => 'crd' },
	    "The name to which this image should be credited if used",
	{ output => 'license', com_name => 'lic' },
	    "A URL giving the license terms under which this image may be used");
    
    # Finally, we define some rulesets to specify the parameters accepted by
    # the operations defined in this class.
    
    $ds->define_ruleset('1.1:taxa:specifier' => 
	{ param => 'name', valid => \&PB1::TaxonData::validNameSpec, 
	  alias => 'taxon_name' },
	    "Return information about the most fundamental taxonomic name matching this string.",
	    "The C<%> and C<_> characters may be used as wildcards.",
	{ param => 'id', valid => POS_VALUE, 
	  alias => 'taxon_id' },
	    "Return information about the taxonomic name corresponding to this identifier.",
	{ at_most_one => ['name', 'id'] },
	    "You may not specify both C<name> and C<id> in the same query.");
    
    $ds->define_set('1.1:taxa:rel' =>
	{ value => 'self' },
	    "Select just the base taxon or taxa themselves.  This is the default.",
	{ value => 'synonyms' },
	    "Select all synonyms of the base taxon or taxa.",
	{ value => 'children' },
	    "Select the taxa immediately contained within the base taxon or taxa.",
	{ value => 'all_children' },
	    "Select all taxa contained within the base taxon or taxa.",
	{ value => 'parents' },
	    "Select the immediate containing taxa of the base taxon or taxa.",
	{ value => 'all_parents' },
	    "Select all taxa that contain the base taxon or taxa.",
	{ value => 'common_ancestor' },
	    "Select the most specific taxon that contains all of the base taxa",
	{ value => 'all_taxa' },
	    "Select all of the taxa in the database.  In this case you do not have",
	    "to specify C<name> or C<id>.  Use with caution, because the maximum",
	    "data set returned may be as much as 80 MB.");
    
    $ds->define_set('1.1:taxa:status' =>
	{ value => 'valid' },
	    "Select only taxonomically valid names",
	{ value => 'senior' },
	    "Select only taxonomically valid names that are not junior synonyms",
	{ value => 'invalid' },
	    "Select only taxonomically invalid names, e.g. nomina dubia",
	{ value => 'all' },
	    "Select all taxonomic names matching the other specified criteria");
    
    $ds->define_set('1.1:taxa:refselect' =>
	{ value => 'authority' },
	    "Select only the references associated with the authority records for these taxa",
	{ value => 'classification' },
	    "Select only the references associated with the classification opinions for these taxa",
	{ value => 'both' },
	    "Select the references associated with both the authority records and the classification",
	    "opinions for these taxa",
	{ value => 'opinions' },
	    "Select the references associated with all opinions on these taxa",
	{ value => 'all' },
	    "Select the references associated with both the authority records and all opinions on",
	    "these taxa");
    
    $ds->define_set('1.1:taxa:refspelling' =>
	{ value => 'current' },
	    "Select only the references associated with the currently accepted spelling of each taxonomic name",
	{ value => 'all' },
	    "Select the references associated with all spellings of each taxonomic name");
    
    $ds->define_set('1.1:taxa:summary_rank' =>
	{ value => 'ident' },
	    "Group occurrences together by their taxonomic identification, ignoring modifiers.",
	    "This is the default.",
	{ value => 'exact' },
	    "Group occurrences together by their exact taxonomic identification, including",
	    "modifiers such as 'sensu lato' or 'n. sp.'.",
	">If you choose any of the following values, then all occurrences whose identified",
	"taxon has not been entered into this database will be skipped.",
	{ value => 'taxon' },
	    "Group occurrences together if they are identified as belonging to the same taxon,",
	    "ignoring synonymy.",
	{ value => 'synonym' },
	    "Group occurrences together if they are identified as belonging to synonymous taxa.",
	    "All of the following options also take synonymy into account.",
	{ value => 'species' },
	    "Group occurrences together if they are identified as belonging to the same species",
	{ value => 'genus' },
	    "Group occurrences together if they are identified as belonging to the same genus",
	{ value => 'family' },
	    "Group occurrences together if they are identified as belonging to the same family",
	{ value => 'order' },
	    "Group occurrences together if they are identified as belonging to the same order",
	{ value => 'class' },
	    "Group occurrences together if they are identified as belonging to the same class",
	{ value => 'phylum' },
	    "Group occurrences together if they are identified as belonging to the same phylum",
	{ value => 'kingdom' },
	    "Group occurrences together if they are identified as belonging to the same kingdom");
    
    $ds->define_set('1.1:taxa:order' =>
	{ value => 'hierarchy' },
	    "Results are ordered hierarchically by taxonomic identification.",
	    "The order of sibling taxa is arbitrary, but children will always follow",
	    "after parents.  This is the default.",
	{ value => 'hierarchy.asc', undocumented => 1 },
	{ value => 'hierarchy.desc', undocumented => 1 },
	{ value => 'name' },
	    "Results are ordered alphabetically by taxon name.",
	{ value => 'name.asc', undocumented => 1 },
    	{ value => 'name.desc', undocumented => 1 },
	{ value => 'firstapp' },
	    "Results are ordered chronologically by first appearance, oldest to youngest unless you add C<.asc>",
	{ value => 'firstapp.asc', undocumented => 1 },
	{ value => 'firstapp.desc', undocumented => 1 },
	{ value => 'lastapp' },
	    "Results are ordered chronologically by last appearance, oldest to youngest unless you add C<.asc>",
	{ value => 'lastapp.asc', undocumented => 1 },
	{ value => 'lastapp.desc', undocumented => 1 },
	{ value => 'agespan' },
	    "Results are ordered based on the difference between the first and last appearances, starting",
	    "with occurrences with the smallest spread (most precise temporal resolution) unless you add C<.desc>",
	{ value => 'agespan.asc', undocumented => 1 },
	{ value => 'agespan.desc', undocumented => 1 },
	{ value => 'n_occs' },
	    "Results are ordered by the number of fossil occurrences of this taxon entered in this database,",
	    "largest to smallest unless you add C<.asc>",
	{ value => 'n_occs.asc', undocumented => 1 },
	{ value => 'n_occs.desc', undocumented => 1 },
	{ value => 'size' },
	    "Results are ordered by the number of contained subtaxa, largest to smallest unless you add C<.asc>",
	{ value => 'size.asc', undocumented => 1 },
	{ value => 'size.desc', undocumented => 1 },
	{ value => 'extant_size' },
	    "Results are ordered by the number of extant subtaxa, largest to smallest unless you add C<.asc>",
	{ value => 'extant_size.asc', undocumented => 1 },
	{ value => 'extant_size.desc', undocumented => 1 },
	{ value => 'extant' },
	    "Results are ordered by whether or not the taxon is extant, with extant ones first unless you add C<.asc>",
	{ value => 'extant.asc', undocumented => 1 },
	{ value => 'extant.desc', undocumented => 1 },
	{ value => 'created' },
	    "Results are ordered by the date the record was created, most recent first",
	    "unless you add C<.asc>.",
	{ value => 'created.asc', undocumented => 1 },
	{ value => 'created.desc', undocumented => 1 },
	{ value => 'modified' },
	    "Results are ordered by the date the record was last modified",
	    "most recent first unless you add C<.asc>",
	{ value => 'modified.asc', undocumented => 1 },
	{ value => 'modified.desc', undocumented => 1 },
	{ value => 'rank' },
	    "Results are ordered by the number of associated records, highest first unless you add C<.asc>.",
	    "This is only useful when querying for taxa associated with occurrences, etc.",
	{ value => 'rank.asc', undocumented => 1 },
	{ value => 'rank.desc', undocumented => 1 });
    
    $ds->define_ruleset('1.1:taxa:selector' =>
	"The following parameters are used to select the base set of taxonomic names to return.",
	"If you wish to download the entire taxonomy, use C<rel=all_taxa> and see also the",
	"L<limit|node:special#limit> parameter.",
	{ param => 'name', valid => \&PB1::TaxonData::validNameSpec, list => ',', 
	  alias => 'taxon_name' },
	    "Select the all taxa matching each of the specified name(s).",
	    "To specify more than one, separate them by commas.",
	    "The C<%> character may be used as a wildcard.",
	{ param => 'base_name', valid => \&PB1::TaxonData::validNameSpec, list => ',' },
	    "Selects all taxa matching each of the specified name(s), plus",
	    "all of their subtaxa.  Equivalent to specifying C<rel=all_children>.",
	{ param => 'id', valid => POS_VALUE, list => ',' },
	    "Selects the taxa corresponding to the specified identifier(s).",
	    "You may specify more than one, separated by commas.",
	{ param => 'base_id', valid => POS_VALUE, list => ',' },
	    "Selects all taxa corresponding to the specified identifier(s), plus",
	    "all of their subtaxa.  Equivalent to specifying C<rel=all_children>.",
	{ optional => 'exact', valid => FLAG_VALUE },
	    "If this parameter is specified, then the taxon exactly matching",
	    "the specified name or identifier is selected, rather than the",
	    "senior synonym which is the default.",
	">The following parameters indicate which related taxonomic names to return:",
	{ param => 'rel', valid => '1.1:taxa:rel', default => 'self' },
	    "Indicates which taxa are to be selected.  Accepted values include:",
	{ param => 'status', valid => '1.1:taxa:status', default => 'valid' },
	    "Return only names that have the specified status.  Accepted values include:");
    
    $ds->define_ruleset('1.1:taxa:filter' => 
	"The following parameters further filter the list of return values:",
	{ optional => 'rank', valid => \&PB1::TaxonData::validRankSpec },
	    "Return only taxonomic names at the specified rank, e.g. C<genus>.",
	{ optional => 'extant', valid => BOOLEAN_VALUE },
	    "Return only extant or non-extant taxa.",
	    "Accepted values are: C<yes>, C<no>, C<1>, C<0>, C<true>, C<false>.",
	{ optional => 'depth', valid => POS_VALUE },
	    "Return only taxa no more than the specified number of levels above or",
	     "below the base taxa in the hierarchy");
    
    $ds->define_ruleset('1.1:taxa:occ_filter' =>
	{ optional => 'extant', valid => BOOLEAN_VALUE },
	    "Return only extant or non-extant taxa.",
	    "Accepted values are: C<yes>, C<no>, C<1>, C<0>, C<true>, C<false>.");
    
    $ds->define_ruleset('1.1:taxa:summary_selector' => 
	{ optional => 'rank', valid => '1.1:taxa:summary_rank', alias => 'summary_rank',
	  default => 'ident' },
	    "Summarize the results by grouping them as follows:");
    
    $ds->define_ruleset('1.1:taxa:display' => 
	"The following parameter indicates which information should be returned about each resulting name:",
	{ optional => 'show', valid => '1.1:taxa:output_map', list => ','},
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each taxon.  Its value should be",
	    "one or more of the following, separated by commas:",
	{ optional => 'order', valid => '1.1:taxa:order', split => ',' },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  Accepted values are:");
    
    $ds->define_ruleset('1.1:taxa:single' => 
	{ require => '1.1:taxa:specifier',
	  error => "you must specify either 'name' or 'id'" },
	{ allow => '1.1:taxa:display' }, 
	{ allow => '1.1:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.1:taxa:list' => 
	{ require => '1.1:taxa:selector',
	  error => "you must specify either of 'name', 'id'" },
	{ allow => '1.1:taxa:filter' },
	{ allow => '1.1:taxa:display' }, 
	{ allow => '1.1:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.1:taxa:refs' =>
	">You can use the following parameters if you wish to retrieve the references associated",
	"with a specified list of taxa.",
	"Only the records which also match the other parameters that you specify will be returned.",
	{ allow => '1.1:taxa:selector' },
	{ allow => '1.1:common:select_crmod' },
	{ allow => '1.1:common:select_ent' },
	{ require_any => ['1.1:taxa:selector', 
			  '1.1:common:select_crmod', '1.1:common:select_ent'] },
	">You can also specify any of the following parameters:",
	{ optional => 'select', valid => '1.1:taxa:refselect' },
	    "You can use this parameter to specify which kinds of references to retrieve.",
	    "The accepted values include:",
	{ optional => 'spelling', valid => '1.1:taxa:refspelling' },
	    "You can use this parameter to specify which variants of the matching taxonomic name(s) to retrieve.",
	    "The accepted values include:",
	{ allow => '1.1:refs:filter' },
	{ allow => '1.1:refs:display' },
	{ allow => '1.1:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.",
	">If the parameter C<order> is not specified, the results are sorted alphabetically by",
	"the name of the primary author.");
    
    $ds->define_ruleset('1.1:taxa:match' =>
	{ param => 'name', valid => \&PB1::TaxonData::validNameSpec, list => ',', alias => 'taxon_name' },
	    "A valid taxonomic name, or a common abbreviation such as 'T. rex'.",
	    "The name may include the wildcard characters % and _.",
	{ optional => 'rank', valid => \&PB1::TaxonData::validRankSpec },
	    "Return only taxonomic names at the specified rank, e.g. C<genus>.",
	{ optional => 'extant', valid => BOOLEAN_VALUE },
	    "Return only extant or non-extant taxa.",
	    "Accepted values are: C<yes>, C<no>, C<1>, C<0>, C<true>, C<false>.",
	{ param => 'status', valid => '1.1:taxa:status', default => 'valid' },
	    "Return only names that have the specified status.  Accepted values include:",
	{ allow => '1.1:taxa:display' }, 
	{ allow => '1.1:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.1:taxa:auto' =>
	{ param => 'name', valid => ANY_VALUE, alias => 'taxon_name' },
	    "A partial name or prefix.  It must have at least 3 significant characters, and may include both a genus",
	    "(possibly abbreviated) and a species.  Examples:\n    t. rex, tyra, rex", 
	{ allow => '1.1:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.1:taxa:thumb' =>
	{ param => 'id', valid => POS_VALUE },
	    "A positive number identifying a taxon image",
	{ optional => 'SPECIAL(save)' },
	{ ignore => 'splat' });
    
    $ds->define_ruleset('1.1:taxa:icon' =>
	{ require => '1.1:taxa:thumb' });
    
    $ds->define_ruleset('1.1:taxa:list_images' =>
	{ param => 'name', valid => ANY_VALUE },
	    "List images belonging to the specified taxonomic name.  If multiple",
	    "names match what you specified, the images for all of them will be listed.",
	{ param => 'id', valid => POS_VALUE },
	    "List images belonging to the taxonomic name corresponding to the specified",
	    "identifier.",
	{ at_most_one => ['id', 'name'] },
	    "You may not specify both C<name> and C<id> in the same query.",
	{ optional => 'rel', valid => ENUM_VALUE('all_children') },
	    "If this parameter is specified with the value C<all_children>, then",
	    "all images matching the specified taxon or any of its children are",
	    "returned.  In this case, the fields C<taxon_id> and C<taxon_name>",
	    "will be included in the result.",
	{ optional => 'depth', valid => POS_VALUE },
	    "Return only images whose depth in the tree is at most the specified",
	    "number of levels different from the base taxon or taxa.");
    
    # Determine which fields are available in this version of the database.
    
    my $dbh = $ds->get_connection;
    
    my $record;
    
    eval {
	$record = $dbh->selectrow_hashref("SELECT * from $TAXON_TABLE{taxon_trees}{search}");
    };
    
    if ( ref $record eq 'HASH' )
    {
	$DB_FIELD{common} = 1 if exists $record->{common};
	$DB_FIELD{orig_no} = 1 if exists $record->{orig_no};
	$DB_FIELD{is_current} = 1 if exists $record->{is_current};
	$DB_FIELD{accepted_no} = 1 if exists $record->{accepted_no};
    }
}


# get ( )
# 
# Return a single taxon record, specified by name or number.  If name, then
# return the matching taxon with the largest size.

sub get {

    my ($self) = @_;
    
    my $dbh = $self->get_connection;
    my $taxonomy = TaxonomyOld->new($dbh, 'taxon_trees');
    my $valid = $self->{valid};
    my $taxon_no;
    
    # Then figure out which taxon we are looking for.  If we have a taxon_no,
    # we can use that.
    
    my $not_found_msg = '';
    
    if ( $valid->value('id') )
    {    
	$taxon_no = $valid->value('id');
	$not_found_msg = "Taxon number $taxon_no was not found in the database";
    }
    
    # Otherwise, we must have a taxon name.  So look for that.
    
    elsif ( defined $valid->value('name') )
    {
	$not_found_msg = "Taxon '$valid->value('name')' was not found in the database";
	my $name_select = { order => 'size.desc', spelling => 'exact', return => 'id', limit => 1 };
	
	if ( defined $valid->value('rank') )
	{
	    $name_select->{rank} = $valid->value('rank');
	    $not_found_msg .= " at rank '$name_select->{rank}'";
	}
	
	($taxon_no) = $self->get_taxa_by_name($valid->value('name'), $name_select);
    }
    
    # If we haven't found a record, the result set will be empty.
    
    unless ( defined $taxon_no and $taxon_no > 0 )
    {
	return;
    }
    
    # Now add the fields necessary to show the requested info.
    
    my $options = $self->generate_query_options;
    
    # my @fields;
    
    # push @fields, 'ref' if $self->output_key('ref');
    # push @fields, 'attr' if $self->output_key('attr');
    # push @fields, 'size' if $self->output_key('size');
    # push @fields, 'app' if $self->output_key('app');
    # push @fields, 'img' if $self->output_key('img');
    
    # push @fields, 'link' if $self->output_key('nav');
    # push @fields, 'parent' if $self->output_key('nav');
    # push @fields, 'phylo' if $self->output_key('nav');
    # push @fields, 'counts' if $self->output_key('nav');
    
    # $options->{fields} = \@fields;
    
    # If we were asked for the senior synonym, choose it.
    
    my $rel = $valid->value('exact') ? 'self' : 'senior';
    
    # Next, fetch basic info about the taxon.
    
    ($self->{main_record}) = $taxonomy->getRelatedTaxon($rel, $taxon_no, $options);
    
    $self->{main_sql} = $TaxonomyOld::SQL_STRING;
    
    # If we were asked for 'nav' info, also show the various categories
    # of subtaxa and whether or not each of the parents are extinct.
    
    if ( $self->has_block('nav') )
    {
	my $r = $self->{main_record};
	
	# First get taxon records for all of the relevant supertaxa.
	
	if ( $r->{kingdom_no} )
	{
	    $r->{kingdom_txn} = $taxonomy->getTaxon($r->{kingdom_no}, { fields => ['size'] });
	}
	
	if ( $r->{phylum_no} )
	{
	    $r->{phylum_txn} = $taxonomy->getTaxon($r->{phylum_no}, { fields => ['size'] });
	}
	
	if ( $r->{class_no} )
	{
	    $r->{class_txn} = $taxonomy->getTaxon($r->{class_no}, { fields => ['size'] });
	}
	
	if ( $r->{order_no} )
	{
	    $r->{order_txn} = $taxonomy->getTaxon($r->{order_no}, { fields => ['size'] });
	}
	
	if ( $r->{family_no} )
	{
	    $r->{family_txn} = $taxonomy->getTaxon($r->{family_no}, { fields => ['size'] });
	}
	
	if ( $r->{parsen_no} || $r->{parent_no} )
	{
	    my $parent_no = $r->{parsen_no} || $r->{parent_no};
	    $r->{parent_txn} = $taxonomy->getTaxon($parent_no, { fields => ['size'] });
	}
	
	# Then add the various lists of subtaxa.
	
	unless ( $r->{phylum_no} or (defined $r->{rank} && $r->{rank} <= 20) )
	{
	    $r->{phylum_list} = [ $taxonomy->getTaxa('all_children', $taxon_no, 
						     { status => 'senior', limit => 10, order => 'size.desc', 
						       rank => 20, fields => ['size', 'app'] } ) ];
	}
	
	unless ( $r->{class_no} or $r->{rank} <= 17 )
	{
	    $r->{class_list} = [ $taxonomy->getTaxa('all_children', $taxon_no, 
						    { status => 'senior', limit => 10, order => 'size.desc', 
						      rank => 17, fields => ['size', 'app'] } ) ];
	}
	
	unless ( $r->{order_no} or $r->{rank} <= 13 )
	{
	    my $order = defined $r->{order_count} && $r->{order_count} > 100 ? undef : 'size.desc';
	    $r->{order_list} = [ $taxonomy->getTaxa('all_children', $taxon_no, 
						    { status => 'senior', limit => 10, order => $order, 
						      rank => 13, fields => ['size', 'app'] } ) ];
	}
	
	unless ( $r->{family_no} or $r->{rank} <= 9 )
	{
	    my $order = defined $r->{family_count} && $r->{family_count} > 100 ? undef : 'size.desc';
	    $r->{family_list} = [ $taxonomy->getTaxa('all_children', $taxon_no, 
						     { status => 'senior', limit => 10, order => $order, 
						       rank => 9, fields => ['size', 'app'] } ) ];
	}
	
	if ( $r->{rank} > 5 )
	{
	    my $order = defined $r->{genus_count} && $r->{order_count}> 100 ? undef : 'size.desc';
	    $r->{genus_list} = [ $taxonomy->getTaxa('all_children', $taxon_no,
						    { status => 'senior', limit => 10, order => $order, 
						      rank => 5, fields => ['size', 'app'] } ) ];
	}
	
	if ( $r->{rank} == 5 )
	{
	    $r->{subgenus_list} = [ $taxonomy->getTaxa('all_children', $taxon_no,
						       { status => 'senior', limit => 10, order => 'size.desc', 
							 rank => 4, fields => ['size', 'app'] } ) ];
	}
	
	if ( $r->{rank} == 5 or $r->{rank} == 4 )
	{
	    $r->{species_list} = [ $taxonomy->getTaxa('all_children', $taxon_no,
						       { status => 'senior', limit => 10, order => 'size.desc', 
							 rank => 3, fields => ['size', 'app'] } ) ];
	}
	
	$r->{children} = 
	    [ $taxonomy->getTaxa('children', $taxon_no, { status => 'senior', limit => 10, order => 'size.desc', 
							  fields => ['size', 'app'] } ) ];
    }
    
    return 1;
}


# list ( )
# 
# Query the database for basic info about all taxa matching the specified
# parameters.  If the argument 'refs' is given, then return matching
# references instead of matching taxa.

sub list {

    my ($self, $arg) = @_;
    
    my $dbh = $self->get_connection;
    my $taxonomy = TaxonomyOld->new($dbh, 'taxon_trees');
    
    # First, figure out what info we need to provide
    
    my $options = $self->generate_query_options();
    
    # Then, figure out which taxa we are looking for.
    
    my $name_list = $self->clean_param('name');
    my $id_list = $self->clean_param('id');
    my $rel = $self->clean_param('rel');
    
    if ( my $base_name = $self->clean_param('base_name') )
    {
	$name_list = $base_name;
	$rel = 'all_children';
    }
    
    elsif ( my $base_id = $self->clean_param('base_id') )
    {
	$id_list = $base_id;
	$rel = 'all_children';
    }
    
    # If we are listing by name (as opposed to id) then go through each name and
    # find the largest matching taxon.
    
    if ( $name_list )
    {
	my @names = ref $name_list eq 'ARRAY' ? @$name_list : $name_list;
	my @ids;
	my $name_options = { order => 'size.desc', spelling => 'exact', return => 'id', limit => 1 };
	
	foreach my $name (@names)
	{
	    push @ids, $self->get_taxa_by_name($name, $name_options);
	}
	
	return unless @ids;
	$id_list = \@ids;
    }
    
    # Now do the main query and return a result:
    
    # If the argument is 'refs', then return matching references.
    
    if ( defined $arg && $arg eq 'refs' && $rel eq 'self' )
    {
	my @result = $taxonomy->getTaxonReferences('self', $id_list, $options);
	$self->{main_result} = \@result;
	$self->{main_sql} = $TaxonomyOld::SQL_STRING;
	$self->{result_count} = scalar(@result);
    }
    
    elsif ( defined $arg && $arg eq 'refs' )
    {
	$options->{return} = 'stmt';
	($self->{main_sth}) = $taxonomy->getTaxonReferences($rel, $id_list, $options);
	$self->{main_sql} = $TaxonomyOld::SQL_STRING;
	$self->sql_count_rows;
    }
    
    # Otherwise, return matching taxa.  If the relationship is 'self' (the
    # default) then just return the list of matches.
    
    elsif ( $rel eq 'self' )
    {
	my @result = $taxonomy->getTaxa('self', $id_list, $options);
	$self->{main_result} = \@result;
	$self->{main_sql} = $TaxonomyOld::SQL_STRING;
	$self->{result_count} = scalar(@result);
    }
    
    # If the relationship is 'common_ancestor', we have just one result.
    
    elsif ( $rel eq 'common_ancestor' )
    {
	$options->{return} = 'list';
	
	($self->{main_record}) = $taxonomy->getTaxa('common_ancestor', $id_list, $options);	
	$self->{main_sql} = $TaxonomyOld::SQL_STRING;
	$self->{result_count} = defined $self->{main_record} ? 1 : 0;
    }
    
    # Otherwise, we just call getTaxa and return the result.
    
    else
    {
	$options->{return} = 'stmt';
	$rel ||= 'self';
	
	($self->{main_sth}) = $taxonomy->getTaxa($rel, $id_list, $options);
	$self->{main_sql} = $TaxonomyOld::SQL_STRING;
	$self->sql_count_rows;
    }
    
    print STDERR $self->{main_sql} . "\n\n" if $self->debug;
    
    # Otherwise, we have an empty result.
    
    return;
}


# match ( )
# 
# Query the database for basic info about all taxa matching the specified name
# or names (as well as any other conditions specified by the parameters).

sub match {
    
    my ($self) = @_;
    
    my $dbh = $self->get_connection;
    my $taxonomy = TaxonomyOld->new($dbh, 'taxon_trees');
    
    # Make sure we have at least one valid name.
    
    my $name_list = $self->clean_param('name');
    
    return unless $name_list;
    
    # Figure out the proper query options.
    
    my $options = $self->generate_query_options();
    
    $options->{exact} = 1;
    
    # Get the list of matches.
    
    my @name_matches = $self->get_taxa_by_name($name_list, $options);
    
    $self->{main_result} = \@name_matches if scalar(@name_matches);
    $self->{result_count} = scalar(@name_matches);
}


# list_refs ( )
# 
# Query the database for basic info about all references associated with taxa
# that meet the specified parameters.

sub list_refs {

    my ($self) = @_;
    
    my $dbh = $self->get_connection;
    my $taxonomy = TaxonomyOld->new($dbh, 'taxon_trees');
    
    # First, figure out what info we need to provide
    
    my $options = $self->generate_query_options('ref');
    
    my $rel = $self->clean_param('rel') || 'self';
    
    # If the parameter 'name' was given, then fetch all matching taxa.  Order
    # them in descending order by size.
    
    my @taxon_list;
    
    if ( $self->clean_param('name') )
    {
	my $name = $self->clean_param('name');
	my $name_select = { order => 'size.desc', spelling => 'exact', return => 'id', limit => 1 };
	
	@taxon_list = $self->get_taxa_by_name($name, $name_select);
	return unless @taxon_list;
    }
    
    # Now do the main query and return a result:
    
    # If a name was given and the relationship is 'self' (or not specified,
    # being the default) then just return the list of matches.
    
    if ( $self->clean_param('name') and $rel eq 'self' )
    {
	my @result = $taxonomy->getTaxonReferences('self', \@taxon_list, $options);
	$self->{main_result} = \@result;
	$self->{main_sql} = $TaxonomyOld::SQL_STRING;
	$self->{result_count} = scalar(@result);
    }
    
    # If a name was given and some other relationship was specified, use the
    # first matching name.
    
    elsif ( $self->clean_param('name') )
    {
	$options->{return} = 'stmt';
	my $id = $taxon_list[0];
	my $rel = $self->clean_param('rel') || 'self';
	
	($self->{main_sth}) = $taxonomy->getTaxonReferences($rel, $id, $options);
	$self->{main_sql} = $TaxonomyOld::SQL_STRING;
	$self->sql_count_rows;
    }
    
    # Otherwise, we just call getTaxa with a list of ids. 
    
    elsif ( $self->clean_param('id') )
    {
	$options->{return} = 'stmt';
	my $id_list = $self->clean_param('id');
	
    }
    
    # Otherwise, we have an empty result.
    
    return;
}


# get_taxa_by_name ( names, options )
# 
# Given a taxon name (or list of names), return either a list of ids or a
# range expression that can be used to select the corresponding taxa.

our ($NAME_SQL) = '';

sub get_taxa_by_name {

    my ($self, $names, $options) = @_;
    
    $options ||= {};
    my $dbh = $self->get_connection;
    
    # We start with some common query clauses, depending on the options.
    
    my (@clauses);
    my $order_string = 'ORDER BY v.taxon_size';
    my $limit_string = '';
    my $fields = 't.orig_no';
    
    # Do we accept common names?
    
    if ( $DB_FIELD{common} && defined $options->{common} && $options->{common} eq 'only' )
    {
	push @clauses, "common = 'EN'";
    }
    
    elsif ( $DB_FIELD{common} && $options->{common} )
    {
	push @clauses, "common = ''";
    }
    
    # Invalid names?
    
    my $status = $options->{status} // 'any';
    
    if ( $status eq 'valid' )
    {
	push @clauses, "status in ('belongs to', 'objective synonym of', 'subjective synonym of')";
    }
    
    elsif ( $status eq 'senior' )
    {
	push @clauses, "status in ('belongs to')";
    }
    
    elsif ( $status eq 'invalid' )
    {
	push @clauses, "status not in ('belongs to', 'objective synonym of', 'subjective synonym of')";
    }
    
    elsif ( $status ne 'any' )
    {
	push @clauses, "status = 'bad_value'";
    }
    
    # Number of results
    
    unless ( $options->{all_names} )
    {
	$limit_string = "LIMIT 1";
    }
    
    # Result fields
    
    if ( $options->{return} eq 'range' )
    {
	$fields = "t.name, t.lft, t.rgt, t.orig_no";
    }
    
    elsif ( $options->{return} eq 'id' )
    {
	$fields = $options->{exact} ? 's.taxon_no' : 't.orig_no';
    }
    
    else
    {
	$fields = "s.taxon_name as match_name, t.name as taxon_name, t.rank as taxon_rank, t.status, v.taxon_size, t.orig_no, t.trad_no as taxon_no";
    }
    
    # The names might be given as a list, a hash, or a single string (in which
    # case it will be split into comma-separated items).
    
    my @name_list;
    
    if ( ref $names eq 'ARRAY' )
    {
	@name_list = @$names;
    }
    
    elsif ( ref $names eq 'HASH' )
    {
	@name_list = keys %$names;
    }
    
    elsif ( ref $names )
    {
	croak "get_taxa_by_name: parameter 'names' may not be a blessed reference";
    }
    
    else
    {
	@name_list = split( qr{\s*,\s*}, $names );
    }
    
    # Now that we have a list, we evaluate the names one by one.
    
    my (@result);
    
 NAME:
    foreach my $tn ( @name_list )
    {
	my @filters;
	
	# We start by removing any bad characters and trimming leading and
	# trailing spaces.  Also translate all whitespace to a single space
	# and '.' to the wildcard '%'.  For example, "T.  rex" goes to
	# "T% rex";
	
	$tn =~ s/^\s+//;
	$tn =~ s/\s+$//;
	$tn =~ s/\s+/ /g;
	$tn =~ s/\./% /g;
	$tn =~ tr{a-zA-Z%_: }{}cd;
	
	# If we have a selection prefix, evaluate it and add the proper range
	# filter.
	
	if ( $tn =~ qr { [:] }xs )
	{
	    my $range = '';
	    
	    while ( $tn =~ qr{ ^ ([^:]+) : \s* (.*) }xs )
	    {
		my $prefix = $1;
		$tn = $2;
		
		# A prefix is only valid if it's a single word.  Otherwise, we
		# skip this name entirely because with an invalid prefix it cannot
		# evaluate to any actual name entry.
		
		if ( $prefix =~ qr{ ^ \s* ([a-zA-Z][a-zA-Z%]+) \s* $ }xs )
		{
		    $range = $self->get_taxon_range($1, $range);  
		}
		
		else
		{
		    next NAME;
		}
	    }
	    
	    # If we get here, we have evaluated all prefixes.  So add the
	    # resulting range to the list of filters.
	    
	    push @filters, $range if $range;
	}
	
	# Now, we determine the query necessary to find each name.
	
	# If we have a species name, we need to filter on both genus and
	# species name.  The name is not valid unless we have at least one
	# alphabetic character in the genus and one in the species.
	
	if ( $tn =~ qr{ ^ ([^\s]+) \s+ (.*) }xs )
	{
	    my $genus = $1;
	    my $species = $2;
	    
	    next unless $genus =~ /[a-zA-Z]/ && $species =~ /[a-zA-Z]/;
	    
	    # We don't have to quote these, because we have already eliminated
	    # all characters except alphabetic and wildcards.
	    
	    push @filters, "genus like '$genus'";
	    push @filters, "taxon_name like '$species'";
	}
	
	# If we have a higher taxon name, we just need to filter on that.  The
	# name is not valid unless it contains at least two alphabetic
	# characters. 
	
	elsif ( $tn =~ qr{ ^ ([^\s]+) $ }xs )
	{
	    my $higher = $1;
	    
	    next unless $higher =~ qr< [a-zA-Z]{2} >xs;
	    
	    push @filters, "taxon_name like '$higher' and taxon_rank >= 5";
	}
	
	# Otherwise, we have an invalid name so just skip it.
	
	else
	{
	    next NAME;
	}
	
	# Now, construct the query.
	
	my $filter_string = join(' and ', @clauses, @filters);
	$filter_string = '1=1' unless $filter_string;
	
	my $s_field = $DB_FIELD{orig_no} ? 'orig_no' : 'result_no';
	my $current_clause = $DB_FIELD{is_current} ? 's.is_current desc,' : '';
	
	$NAME_SQL = "
		SELECT $fields
		FROM taxon_search as s join taxon_trees as t on t.orig_no = s.$s_field
			join taxon_attrs as v on v.orig_no = t.orig_no
		WHERE $filter_string
		ORDER BY $current_clause v.taxon_size desc
		$limit_string";
	
	print STDERR $NAME_SQL . "\n\n" if $self->debug;
	
	my $records;
	
	if ( $options->{return} eq 'id' )
	{
	    $records = $dbh->selectcol_arrayref($NAME_SQL);
	}
	
	else
	{
	    $records = $dbh->selectall_arrayref($NAME_SQL, { Slice => {} });
	}
	
	push @result, @$records if ref $records eq 'ARRAY';
    }
    
    return @result;
}


sub get_taxon_range {
    
    my ($self, $name, $range) = @_;
    
    my $dbh = $self->get_connection;
    my $range_filter = $range ? "and $range" : "";
    
    my $sql = "
		SELECT t.lft, t.rgt
		FROM taxon_search as s JOIN taxon_trees as t on t.orig_no = s.synonym_no
			JOIN taxon_attrs as v on v.orig_no = t.orig_no
		WHERE s.taxon_name like '$name' $range_filter
		ORDER BY v.taxon_size LIMIT 1";
    
    my ($lft, $rgt) = $dbh->selectrow_array($sql);
    
    return $lft ? "t.lft between $lft and $rgt" : "t.lft = 0";
}


# valid_name_spec ( name )

# generate_query_options ( )
# 
# Return an options hash, based on the parameters, which can be passed to
# getTaxaByName or getTaxa.

sub generate_query_options {
    
    my ($self, $operation) = @_;
    
    my @rawfields = $self->select_list() unless defined $operation && $operation eq 'ref';
    my $limit = $self->result_limit;
    my $offset = $self->result_offset(1);
    
    my @fields;
    
    foreach my $f (@rawfields)
    {
	next if $f eq '$bt.modified';
	$f = 'created' if $f eq '$bt.created';
	push @fields, $f;
    }
    
    my $options = { fields => \@fields,
		    order => 'size.desc' };
    
    $options->{limit} = $limit if defined $limit;	# $limit may be 0
    $options->{offset} = $offset if $offset;
    $options->{count} = 1 if $self->clean_param('count');
    
    my $exact = $self->clean_param('exact');
    my $extant = $self->clean_param('extant');
    my $rank = $self->clean_param('rank');
    my $status = $self->clean_param('status');
    my $select = $self->clean_param('select');
    my $spelling = $self->clean_param('spelling');
    
    $options->{exact} = 1 if $exact;
    $options->{extant} = $extant if $extant ne '';	# $extant may be 0, 1, or undefined
    $options->{rank} = $rank if $rank ne '';
    $options->{status} = $status if $status ne '';
    $options->{select} = $select if $select ne '';
    $options->{spelling} = $spelling if $spelling ne '';
    
    # Use just the first order specified.  This should be fixed at some point.
    
    if ( my ($term) = $self->clean_param_list('order') )
    {
	my $dir = '';
	
	if ( $term =~ /^(\w+)[.](asc|desc)$/ )
	{
	    $term = $1;
	    $dir = $2;
	}
	
	# The following options default to ascending.
	
	if ( $term eq 'hierarchy' )
	{
	    $options->{order} = "lft.$dir";
	}
	
	elsif ( $term eq 'pubyr' || $term eq 'created' || $term eq 'modified' || $term eq 'name' )
	{
	    $options->{order} = $term;
	}
	
	# The following options default to descending.
	
	elsif ( $term eq 'firstapp' || $term eq 'lastapp' || $term eq 'agespan' || 
		$term eq 'size' || $term eq 'extant_size' || $term eq 'n_occs' || $term eq 'extant' )
	{
	    $term .= ".desc" unless $term =~ /\./;
	    $options->{order} = $term;
	}
	
	# If we find an unrecognized option, throw an error.
	
	else
	{
	    $self->add_warning("unrecognized order option '$term'");
	}
    }
    
    return $options;
}


# auto ( )
# 
# Return an auto-complete list, given a partial name.

sub auto {
    
    my ($self) = @_;
    
    my $dbh = $self->get_connection;
    my $partial = $self->clean_param('name');
    
    my $search_table = $TAXON_TABLE{taxon_trees}{search};
    my $names_table = $TAXON_TABLE{taxon_trees}{names};
    my $attrs_table = $TAXON_TABLE{taxon_trees}{attrs};
    
    my $sql;
    
    # Strip out any characters that don't appear in names.  But allow SQL wildcards.
    
    $partial =~ tr/[a-zA-Z_%. ]//dc;
    
    # Construct and execute an SQL statement.
    
    my $limit = $self->sql_limit_clause(1);
    my $calc = $self->sql_count_clause;
    
    my $result_field = $DB_FIELD{accepted_no} ? 's.accepted_no' : 's.synonym_no';
    my $match_field = $DB_FIELD{orig_no} ? 's.taxon_no' : 's.match_no';
    
    my $fields = "taxon_rank, $match_field as taxon_no, n_occs, if(spelling_reason = 'misspelling', 1, null) as misspelling";
    
    # If we are given a genus (possibly abbreviated), generate a search on
    # genus and species name.
    
    if ( $partial =~ qr{^([a-zA-Z_]+)(\.|[.%]? +)([a-zA-Z_]+)} )
    {
	my $genus = $2 ? $dbh->quote("$1%") : $dbh->quote($1);
	my $species = $dbh->quote("$3%");
	
	$sql = "SELECT $calc concat(genus, ' ', taxon_name) as taxon_name, $fields
		FROM $search_table as s JOIN $attrs_table as v on v.orig_no = $result_field
			JOIN $names_table as n on n.taxon_no = $match_field
		WHERE genus like $genus and taxon_name like $species ORDER BY n_occs desc $limit";
    }
    
    # If we are given a single name followed by one or more spaces and nothing
    # else, take it as a genus name.
    
    elsif ( $partial =~ qr{^([a-zA-Z]+)([.%])? +$} )
    {
	my $genus = $2 ? $dbh->quote("$1%") : $dbh->quote($1);
	
	$sql = "SELECT $calc concat(genus, ' ', taxon_name) as taxon_name, $fields
		FROM $search_table as s JOIN $attrs_table as v on v.orig_no = $result_field
			JOIN $names_table as n on n.taxon_no = $match_field
		WHERE genus like $genus ORDER BY n_occs desc $limit";
    }
    
    # Otherwise, if it has no spaces then just search for the name.  Turn all
    # periods into wildcards.
    
    elsif ( $partial =~ qr{^[a-zA-Z_%.]+$} )
    {
	return if length($partial) < 3;
	
	$partial =~ s/\./%/g;
	
	my $name = $dbh->quote("$partial%");
	
	$sql = "SELECT $calc if(genus <> '', concat(genus, ' ', taxon_name), taxon_name) as taxon_name, $fields
	        FROM $search_table as s JOIN $attrs_table as v on v.orig_no = $result_field
			JOIN $names_table as n on n.taxon_no = $match_field
	        WHERE taxon_name like $name ORDER BY n_occs desc $limit";
    }
    
    # Otherwise, return an empty result.
    
    else
    {
	return;
    }
    
    $self->{main_sql} = $sql;
    
    print STDERR $sql . "\n\n" if $self->debug;
    
    $self->{main_sth} = $dbh->prepare($sql);
    $self->{main_sth}->execute();
}


# get_image ( )
# 
# Given an id (image_no) value, return the corresponding image if the format
# is 'png', and information about it if the format is 'json'.

sub get_image {
    
    my ($self, $type) = @_;
    
    $type ||= '';
    
    my $dbh = $self->get_connection;
    my ($sql, $result);
    
    croak "invalid type '$type' for get_image"
	unless $type eq 'icon' || $type eq 'thumb';
    
    my $image_no = $self->clean_param('id');
    my $format = $self->output_format;
    
    # If the output format is 'png', then query for the image.  If found,
    # return it in $self->{main_data}.  Otherwise, we throw a 404 error.
    
    if ( $format eq 'png' )
    {
	$self->{main_sql} = "
		SELECT $type FROM $PHYLOPICS as p
		WHERE image_no = $image_no";
	
	print STDERR "$self->{main_sql}\n\n" if $self->debug;
	
	($self->{main_data}) = $dbh->selectrow_array($self->{main_sql});
	
	return if $self->{main_data};
	die "404 Image not found\n";	# otherwise
    }
    
    # If the output format is 'json' or one of the text formats, then query
    # for information about the image.  Return immediately regardless of
    # whether or not a record was found.  If not, an empty response will be
    # generated.
    
    else
    {
	my $fields = $self->select_string();
	
	$self->{main_sql} = "
		SELECT $fields FROM $PHYLOPICS
		WHERE image_no = $image_no";
	
	print STDERR "$self->{main_sql}\n\n" if $self->debug;
	
	$self->{main_record} = $dbh->selectrow_hashref($self->{main_sql});
	
	return;
    }
}


# list_images ( )
# 
# Return a list of images that meet the specified criteria.

sub list_images {

    my ($self) = @_;
    
    my $dbh = $self->get_connection;
    my $taxonomy = TaxonomyOld->new($dbh, 'taxon_trees');
    my ($sql, $result);
    
    my @filters;
    
    # If the parameter 'name' was given, then fetch all matching taxa.  Order
    # them in descending order by size.
    
    my @taxon_list;
    
    if ( my $name = $self->clean_param('name') )
    {
	my $name_select = { spelling => 'exact', return => 'id' };
	
	@taxon_list = $self->get_taxa_by_name($name, $name_select);
	return unless @taxon_list;
    }
    
    else
    {
	@taxon_list = $self->clean_param_list('id');
    }
    
    # Now add any other filters that were specified by the parameters.
    
    if ( $self->clean_param('rel') eq 'all_children' )
    {
	push @filters, '';
    }
    
    if ( my $depth = $self->clean_param('depth') )
    {
	push @filters, '';
    }
    
    # Construct a query. $$$
    
    my $fields = $self->select_string();
    
    $self->{main_sql} = "
	SELECT $fields FROM $PHYLOPICS as p JOIN $PHYLOPIC_NAMES as n using (uid)
		JOIN authorities as a using (taxon_name) #etc
	WHERE image_no = image_no";
	
	$self->{main_record} = $dbh->selectrow_hashref($self->{main_sql});
	
	return;
    
}


# SQL generation auxiliary routines
# ---------------------------------

# generate_filters ( tables_ref )
# 
# Generate a list of filters that will be used to compute the appropriate
# result set.  Any additional tables that are needed will be indicated in
# $tables_ref.

sub generate_filters {

    my ($self, $tables_ref) = @_;
    
    my @filters;
    
    my $extant = $self->clean_param('extant');
    
    if ( defined $extant && $extant ne '' )
    {
	push @filters, "at.is_extant = $extant";
	$tables_ref->{at} = 1;
    }
    
    my @taxon_ranks = $self->clean_param_list('taxon_rank');
    my $rank_list = $self->generate_rank_list(@taxon_ranks) if @taxon_ranks;
    
    if ( defined $rank_list )
    {
	push @filters, "t.rank in ($rank_list)";
    }
    
    return @filters;
}


# generate_summary_expr ( summary_rank, occs_table, tree_table, ints_table )
# 
# Generate an expression to compute the appropriate summary level.

sub generate_summary_expr {
    
    my ($self, $summary_rank, $o, $t, $i) = @_;
    
    if ( $summary_rank eq 'exact' )
    {
	return "concat_ws(' ', $o.genus_name, $o.genus_reso, if($o.subgenus_name <> '', concat('(', concat_ws(' ', $o.subgenus_name, $o.subgenus_reso), ')'), null), $o.species_name, $o.species_reso)";
    }
    
    elsif ( $summary_rank eq 'ident' )
    {
	return "concat_ws(' ', $o.genus_name, if($o.subgenus_name <> '', concat('(', $o.subgenus_name, ')'), null), $o.species_name)";
    }
    
    elsif ( $summary_rank eq 'taxon' )
    {
	return "$o.orig_no";
    }
    
    elsif ( $summary_rank eq 'synonym' )
    {
	return "$t.synonym_no";
    }
    
    elsif ( $summary_rank eq 'species' )
    {
	return "ifnull($t.species_no, 0)";
    }
    
    elsif ( $summary_rank eq 'genus' )
    {
	return "ifnull($t.genus_no, 0)";
    }
    
    else
    {
	return "ifnull($i.${summary_rank}_no, 0)";
    }
}


# generate_query_fields ( tables_ref )
# 
# Generate a list of query fields based on the request parameters.  Add keys
# to $tables_ref as necessary to indicate tables that must be joined in.

sub generate_query_fields {
    
    my ($self, $tables) = @_;
    
    my (%select) = $self->select_hash();
    
    my $fields .= "a.taxon_name as exact_name, a.taxon_no, a.taxon_rank, a.common_name, at.is_extant, a.orig_no, t.status, a.reference_no, t.name as taxon_name, t.rank";
    
    if ( $select{link} )
    {
	$fields .= $TaxonomyOld::LINK_FIELDS;
    }
    
    if ( $select{attr} )
    {
	$fields .= $TaxonomyOld::ATTR_FIELDS;
	$tables->{ref} = 1;
    }
    
    if ( $select{size} )
    {
	$fields .= $TaxonomyOld::SIZE_FIELDS;
	$tables->{at} = 1;
    }
    
    if ( $select{app} )
    {
	$fields .= $TaxonomyOld::APP_FIELDS;
	$tables->{at} = 1;
    }
    
    if ( $select{parent} )
    {
	$fields .= $TaxonomyOld::PARENT_FIELDS;
	$tables->{pa} = 1;
    }
    
    if ( $select{phylo} )
    {
	$fields .= $TaxonomyOld::INT_PHYLO_FIELDS;
	$tables->{pi} = 1;
    }
    
    if ( $select{counts} )
    {
	$fields .= $TaxonomyOld::COUNT_PHYLO_FIELDS;
	$tables->{pc} = 1;
    }
    
    if ( $select{'$bt.created'} )
    {
	$fields .= $TaxonomyOld::CREATED_FIELDS;
    }
    
    $fields =~ s/v\./at./g;
    
    return $fields;
}

# generate_order_clause ( rank_table )
# 
# Generate an SQL order expression for the result set.

sub generate_order_clause {

    my ($self, $tables, $options) = @_;
    
    $options ||= {};
    
    my @terms = $self->clean_param_list('order');
    my @exprs;
    
    foreach my $term (@terms)
    {
	my $dir = '';
	next unless $term;
	
	if ( $term =~ /^(\w+)[.](asc|desc)$/ )
	{
	    $term = $1;
	    $dir = $2;
	}
	
	if ( $term eq 'hierarchy' )
	{
	    push @exprs, "t.lft $dir";
	}
	
	elsif ( $term eq 'name' )
	{
	    push @exprs, "taxon_name $dir";
	}
	
	elsif ( $term eq 'pubyr' )
	{
	    push @exprs, "a.pubyr $dir";
	}
	
	elsif ( $term eq 'created' )
	{
	    push @exprs, "a.created $dir";
	}
	
	elsif ( $term eq 'modified' )
	{
	    push @exprs, "a.modified $dir";
	}
	
	elsif ( $term eq 'firstapp' )
	{
	    $dir ||= 'desc';
	    push @exprs, "at.first_early_age $dir";
	    $tables->{at} = 1;
	}
	
	elsif ( $term eq 'lastapp' )
	{
	    $dir ||= 'desc';
	    push @exprs, "at.last_late_age $dir";
	    $tables->{at} = 1;
	}
	
	elsif ( $term eq 'agespan' )
	{
	    push @exprs, "(at.first_early_age - at.last_late_age) $dir",
	    $tables->{at} = 1;
	}
	
	elsif ( $term eq 'size' )
	{
	    $dir ||= 'desc';
	    push @exprs, "at.taxon_size $dir";
	    $tables->{at} = 1;
	}
	
	elsif ( $term eq 'extant_size' )
	{
	    $dir ||= 'desc';
	    push @exprs, "at.extant_size $dir";
	    $tables->{at} = 1;
	}
	
	elsif ( $term eq 'extant' )
	{
	    push @exprs, "at.is_extant $dir";
	    $tables->{at} = 1;
	}
	
	elsif ( $term eq 'n_occs' )
	{
	    $dir ||= 'desc';
	    push @exprs, "at.n_occs $dir";
	    $tables->{at} = 1;
	}
	
	elsif ( $term eq 'rank' )
	{
	    my $rank_table = $options->{rank_table};
	    
	    die "400 You cannot select the order option 'rank' with this request"
		unless $rank_table;
	    
	    $dir ||= 'desc';
	    push @exprs, "associated_records $dir";
	}
	
	else
	{
	    die "400 unrecognized order option '$term'";
	}
    }
    
    return join(', ', @exprs);
}


# generate_join_list ( tables_ref )
# 
# Generate a list of SQL join expressions that will complete the query.

sub generate_join_list {

    my ($self, $tables, $tree_table) = @_;
    
    my $join_list = '';
    
    # Return an empty string unless we actually have some joins to make
    
    return $join_list unless ref $tables eq 'HASH' and %$tables;
    
    # Create the necessary join expressions.
    
    $join_list .= "LEFT JOIN $TAXON_TABLE{$tree_table}{attrs} as at on at.orig_no = t.orig_no\n"
	if $tables->{at};
    $join_list .= "LEFT JOIN $TAXON_TABLE{$tree_table}{refs} as r on r.reference_no = a.reference_no\n"
	if $tables->{ref};
    $join_list .= "LEFT JOIN $TAXON_TABLE{$tree_table}{ints} as pi on pi.ints_no = t.ints_no\n"
	if $tables->{pi};
    $join_list .= "LEFT JOIN $TAXON_TABLE{$tree_table}{counts} as pc on pc.orig_no = t.orig_no\n"
	if $tables->{pc};
    
    if ( $tables->{pa} )
    {
	$join_list .= "LEFT JOIN $tree_table as pt on pt.orig_no = t.parsen_no\n";
	$join_list .= "LEFT JOIN $TAXON_TABLE{$tree_table}{authorities} as pa on pa.taxon_no = pt.spelling_no\n";
    }
    
    if ( $tables->{pp} )
    {
	$join_list .= "
		LEFT JOIN person as pp1 on pp1.person_no = a.authorizer_no
		LEFT JOIN person as pp2 on pp2.person_no = a.enterer_no
		LEFT JOIN person as pp3 on pp3.person_no = a.modifier_no\n";
    }
    
    $join_list .= "LEFT JOIN person as ppd on ppd.person_no = a.discussed_by\n"
	if $tables->{ppd};
    
    return $join_list;
}


# Utility routines
# ----------------

# validNameSpec ( name )
# 
# Returns true if the given value is a valid taxonomic name specifier.  We
# allow not only single names, but also lists of names and extra modifiers as
# follows: 
# 
# valid_spec:	name_spec [ , name_spec ... ]
# 
# name_spec:	[ single_name : ] general_name [ < exclude_list > ]
# 
# single_name:	no spaces, but may include wildcards
# 
# general_name: may include up to four components, second component may
#		include parentheses, may include wildcards
# 
# exclude_list:	general_name [ , general_name ]

sub validNameSpec {
    
    my ($value, $context) = @_;
    
    return;	# for now
    
}


sub validRankSpec {
    
    my ($value, $context) = @_;
    
    return;
}


# This routine will be called if necessary in order to properly process the
# results of a query for taxon parents.

sub processResultSet {
    
    my ($self, $rowlist) = @_;
    
    # Run through the parent list and note when we reach the last
    # kingdom-level taxon.  Any entries before that point are dropped 
    # [see TaxonInfo.pm, line 1252 as of 2012-06-24]
    # 
    # If the leaf entry is of rank subgenus or lower, we may need to rewrite the
    # last few entries so that their names properly match the higher level entries.
    # [see TaxonInfo.pm, lines 1232-1271 as of 2012-06-24]
    
    my @new_list;
    my ($genus_name, $subgenus_name, $species_name, $subspecies_name);
    
    for (my $i = 0; $i < scalar(@$rowlist); $i++)
    {
	# Only keep taxa from the last kingdom-level entry on down.
	
    	@new_list = () if $rowlist->[$i]{taxon_rank} eq 'kingdom';
	
	# Skip junior synonyms, we only want a list of 'belongs to' entries.
	
	next unless $rowlist->[$i]{status} eq 'belongs to';
	
	# Note genus, subgenus, species and subspecies names, and rewrite as
	# necessary anything lower than genus in order to match the genus, etc.
	
	my $taxon_name = $rowlist->[$i]{taxon_name};
	my $taxon_rank = $rowlist->[$i]{taxon_rank};
	
	if ( $taxon_rank eq 'genus' )
	{
	    $genus_name = $taxon_name;
	}
	
	elsif ( $taxon_rank eq 'subgenus' )
	{
	    if ( $taxon_name =~ /^(\w+)\s*\((\w+)\)/ )
	    {
		$subgenus_name = "$genus_name ($2)";
		$rowlist->[$i]{taxon_name} = $subgenus_name;
	    }
	}
	
	elsif ( $taxon_rank eq 'species' )
	{
	    if ( $taxon_name =~ /^(\w+)\s*(\(\w+\)\s*)?(\w+)/ )
	    {
		$species_name = $subgenus_name || $genus_name;
		$species_name .= " $3";
		$rowlist->[$i]{taxon_name} = $species_name;
	    }
	}
	
	elsif ( $taxon_rank eq 'subspecies' )
	{
	    if ( $taxon_name =~ /^(\w+)\s*(\(\w+\)\s*)?(\w+)\s+(\w+)/ )
	    {
		$subspecies_name = "$species_name $4";
		$rowlist->[$i]{taxon_name} = $subspecies_name;
	    }
	}
	
	# Now add the (possibly rewritten) entry to the list
	
	push @new_list, $rowlist->[$i];
    }
    
    # Now substitute the processed list for the raw one.
    
    @$rowlist = @new_list;
}


# processRecord ( row )
# 
# This routine takes a hash representing one result row, and does some
# processing before the output is generated.  The information fetched from the
# database needs to be refactored a bit in order to match the Darwin Core
# standard we are using for output.

sub oldProcessRecord {
    
    my ($self, $row) = @_;
    
    # The strings stored in the author fields of the database are encoded in
    # utf-8, and need to be decoded (despite the utf-8 configuration flag).
    
    $self->decodeFields($row);
    
    # Interpret the status info based on the code stored in the database.  The
    # code as stored in the database encompasses both taxonomic and
    # nomenclatural status info, which needs to be separated out.  In
    # addition, we need to know whether to report an "acceptedUsage" taxon
    # (i.e. senior synonym or proper spelling).
    
    my ($taxonomic, $report_accepted, $nomenclatural) = interpretStatusCode($row->{status});
    
    # Override the status code if the synonym_no is different from the
    # taxon_no.  This is necessary because sometimes the opinion record that
    # was used to build this part of the hierarchy indicates a 'belongs to'
    # relationship (which normally indicates a valid taxon) but the
    # taxa_tree_cache record indicates a different synonym number.  In this
    # case, the taxon is in fact no valid but is a junior synonym or
    # misspelling.  If spelling_no and synonym_no are equal, it's a
    # misspelling.  Otherwise, it's a junior synonym.
    
    if ( $taxonomic eq 'valid' && $row->{synonym_no} ne $row->{taxon_no} )
    {
	if ( $row->{spelling_no} eq $row->{synonym_no} )
	{
	    $taxonomic = 'invalid' unless $row->{spelling_reason} eq 'recombination';
	    $nomenclatural = $row->{spelling_reason};
	}
	else
	{
	    $taxonomic = 'synonym';
	}
    }
    
    # Put the two status strings into the row record.  If no value exists,
    # leave it blank.
    
    $row->{taxonomic} = $taxonomic || '';
    $row->{nomenclatural} = $nomenclatural || '';
    
    # Determine the nomenclatural code that has jurisidiction, if that was
    # requested.
    
    if ( $self->{show_code} and defined $row->{lft} )
    {
	$self->determineNomenclaturalCode($row);
    }
    
    # Determine the first appearance data, if that was requested.
    
    if ( $self->{show_firstapp} )
    {
	$self->determineFirstAppearance($row);
    }
    
    # Create a publication reference if that data was included in the query
    
    if ( exists $row->{r_pubtitle} )
    {
	$self->generateReference($row);
    }
    
    # Create an attribution if that data was incluced in the query
    
    if ( exists $row->{a_pubyr} )
    {
	$self->generateAttribution($row);
    }
}


# getCodeRanges ( )
# 
# Fetch the ranges necessary to determine which nomenclatural code (i.e. ICZN,
# ICN) applies to any given taxon.  This is only done if that information is
# asked for.

sub getCodeRanges {

    my ($self) = @_;
    my ($dbh) = $self->{dbh};
    
my @codes = ('Metazoa', 'Animalia', 'Plantae', 'Biliphyta', 'Metaphytae',
	     'Fungi', 'Cyanobacteria');

my $codes = { Metazoa => { code => 'ICZN'}, 
	      Animalia => { code => 'ICZN'},
	      Plantae => { code => 'ICN'}, 
	      Biliphyta => { code => 'ICN'},
	      Metaphytae => { code => 'ICN'},
	      Fungi => { code => 'ICN'},
	      Cyanobacteria => { code => 'ICN' } };

    $self->{code_ranges} = $codes;
    $self->{code_list} = \@codes;
    
    my $code_name_list = "'" . join("','", @codes) . "'";
    
    my $code_range_query = $dbh->prepare("
	SELECT taxon_name, lft, rgt
	FROM taxa_tree_cache join authorities using (taxon_no)
	WHERE taxon_name in ($code_name_list)");
    
    $code_range_query->execute();
    
    while ( my($taxon, $lft, $rgt) = $code_range_query->fetchrow_array() )
    {
	$codes->{$taxon}{lft} = $lft;
	$codes->{$taxon}{rgt} = $rgt;
    }
}


# determineNomenclaturalCode ( row )
# 
# Determine which nomenclatural code the given row's taxon falls under

sub determineNomenclaturalCode {
    
    my ($self, $row) = @_;

    my ($lft) = $row->{lft} || return;
    
    # Anything with a rank of 'unranked clade' falls under PhyloCode.
    
    if ( defined $row->{taxon_rank} && $row->{taxon_rank} eq 'unranked clade' )
    {
	$row->{nom_code} = 'PhyloCode';
	return;
    }
    
    # For all other taxa, we go through the list of known ranges in
    # taxa_tree_cache and use the appropriate code.
    
    foreach my $taxon (@{$self->{code_list}})
    {
	my $range = $self->{code_ranges}{$taxon};
	
	if ( $lft >= $range->{lft} && $lft <= $range->{rgt} )
	{
	    $row->{nom_code} = $range->{code};
	    last;
	}
    }
    
    # If this taxon does not fall within any of the ranges, we leave the
    # nom_code field empty.
}


# determineFirstAppearance ( row )
# 
# Calculate the first appearance of this taxon.

sub determineFirstAppearance {
    
    my ($self, $row) = @_;
    
    my $dbh = $self->{dbh};
    
    # Generate a parameter hash to pass to calculateFirstAppearance().
    
    my $params = { taxonomic_precision => $self->{firstapp_precision},
		   types_only => $self->{firstapp_types_only},
		   traces => $self->{firstapp_include_traces},
		 };
    
    # Get the results.
    
    my $results = calculateFirstAppearance($dbh, $row->{taxon_no}, $params);
    return unless ref $results eq 'HASH';
    
    # Check for error
    
    if ( $results->{error} )
    {
	$self->{firstapp_error} = "An error occurred while calculating the first apperance";
	return;
    }
    
    # If we got results, copy each field into the row.
    
    foreach my $field ( keys %$results )
    {
	$row->{$field} = $results->{$field};
    }
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
