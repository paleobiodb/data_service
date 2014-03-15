#
# TaxonData
# 
# A class that returns information from the PaleoDB database about a single
# taxon or a category of taxa.  This is a subclass of DataService::Base.
# 
# Author: Michael McClennen

use strict;

package TaxonData;

use base 'Web::DataService::Request';
use Carp qw(carp croak);
use Try::Tiny;

use Web::DataService qw(:validators);

use CommonData qw(generateReference generateAttribution);
use TaxonDefs qw(%TAXON_TABLE %TAXON_RANK %RANK_STRING);
use TaxonPics qw($PHYLOPICS $PHYLOPIC_NAMES);
use Taxonomy;

our (@REQUIRES_CLASS) = qw(CommonData ReferenceData);

# This routine is called by the data service in order to initialize this
# class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # First define an output map to specify which output blocks are going to
    # be used to generate output from the operations defined in this class.
    
    $ds->define_output_map('1.1:taxa:output_map' =>
	{ value => 'basic', maps_to => '1.1:taxa:basic', fixed => 1 },
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
	    "The phylogenetic classification of this taxon: kingdom, phylum, class, order, family.",
	    "This information is also included in the C<nav> block, so do not specify both at once.",
	{ value => 'nav', maps_to => '1.1:taxa:nav' },
	    "Additional information for the PBDB Navigator taxon browser.",
	    "This block should only be selected in conjunction with the JSON format.",
	{ value => 'img', maps_to => '1.1:taxa:img' },
	    "The identifier of the image (if any) associated with this taxon.",
	    "These images are sourced from L<phylopic.org>.",
	{ value => 'ent', maps_to => '1.1:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.1:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.1:common:crmod' },
	    "The C<created> and C<modified> timestamps for the collection record");
    
    # Define a second map to handle the URL path /data1.1/taxa/auto, used to
    # implement auto-completion.
    
    $ds->define_output_map('1.1:taxa:auto_map' =>
	{ value => 'basic', maps_to => '1.1:taxa:auto', fixed => 1 });
    
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
	{ set => 'rank', if_vocab => 'pbdb,dwc', lookup => \%RANK_STRING },
	{ output => 'rank', dwc_name => 'taxonRank', com_name => 'rnk' },
	    "The rank of this taxon, ranging from subspecies up to kingdom",
	{ output => 'taxon_name', dwc_name => 'scientificName', com_name => 'nam' },
	    "The scientific name of this taxon",
	{ output => 'common_name', dwc_name => 'vernacularName', com_name => 'nm2' },
	    "The common (vernacular) name of this taxon, if any",
	{ set => 'attribution', if_field => 'a_al1', from_record => 1, 
	  code => \&generateAttribution },
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
	{ output => 'parent_txn', com_name => 'prt', rule => '1.1:taxa:subtaxon' },
	{ output => 'kingdom_no', com_name => 'kgn' },
	    "The identifier of the kingdom in which this taxon occurs",
	{ output => 'kingdom', com_name => 'kgl' },
	    "The name of the kingdom in which this taxon occurs",
	{ output => 'kingdom_txn', com_name => 'kgt', rule => '1.1:taxa:subtaxon' },
	{ output => 'phylum_no', com_name => 'phn' },
	    "The identifier of the phylum in which this taxon occurs",
	{ output => 'phylum', com_name => 'phl' },
	    "The name of the phylum in which this taxon occurs",
	{ output => 'phylum_txn', com_name => 'pht', rule => '1.1:taxa:subtaxon' },
	{ output => 'phylum_count', com_name => 'phc' },
	    "The number of phyla within this taxon",
	{ output => 'class_no', com_name => 'cln' },
	    "The identifier of the class in which this taxon occurs",
	{ output => 'class', com_name => 'cll' },
	    "The name of the class in which this taxon occurs",
	{ output => 'class_txn', com_name => 'clt', rule => '1.1:taxa:subtaxon' },
	{ output => 'class_count', com_name => 'clc' },
	    "The number of classes within this taxon",
	{ output => 'order_no', com_name => 'odn' },
	    "The identifier of the order in which this taxon occurs",
	{ output => 'order', com_name => 'odl' },
	    "The name of the order in which this taxon occurs",
	{ output => 'order_txn', com_name => 'odt', rule => '1.1:taxa:subtaxon' },
	{ output => 'order_count', com_name => 'odc' },
	    "The number of orders within this taxon",
	{ output => 'family_no', com_name => 'fmn' },
	    "The identifier of the family in which this taxon occurs",
	{ output => 'family', com_name => 'fml' },
	    "The name of the family in which this taxon occurs",
	{ output => 'family_txn', com_name => 'fmt', rule => '1.1:taxa:subtaxon' },
	{ output => 'family_count', com_name => 'fmc' },
	    "The number of families within this taxon",
	{ output => 'genus_count', com_name => 'gnc' },
	    "The number of genera within this taxon",
    
	{ output => 'children', com_name => 'chl', rule => '1.1:taxa:subtaxon' },
	    "The immediate children of this taxonomic concept, if any",
	{ output => 'phylum_list', com_name => 'phs', rule => '1.1:taxa:subtaxon' },
	    "A list of the phyla within this taxonomic concept",
	{ output => 'class_list', com_name => 'cls', rule => '1.1:taxa:subtaxon' },
	    "A list of the classes within this taxonomic concept",
	{ output => 'order_list', com_name => 'ods', rule => '1.1:taxa:subtaxon' },
	    "A list of the orders within this taxonomic concept",
	{ output => 'family_list', com_name => 'fms', rule => '1.1:taxa:subtaxon' },
	    "A list of the families within this taxonomic concept",
	{ output => 'genus_list', com_name => 'gns', rule => '1.1:taxa:subtaxon' },
	    "A list of the genera within this taxonomic concept",
	{ output => 'subgenus_list', com_name => 'sgs', rule => '1.1:taxa:subtaxon' },
	    "A list of the subgenera within this taxonomic concept",
	{ output => 'species_list', com_name => 'sps', rule => '1.1:taxa:subtaxon' },
	    "A list of the species within this taxonomic concept",
 	{ output => 'subspecies_list', com_name => 'sss', rule => '1.1:taxa:subtaxon' },
	    "A list of the subspecies within this taxonomic concept");
    
    $ds->define_block('1.1:taxa:img' =>
	{ select => 'img' },
	{ output => 'image_no', com_name => 'img' },
    	    "If this value is non-zero, you can use it to construct image URLs",
	    "using L</data1.1/taxa/thumb_doc|/data1.1/taxa/thumb> and L</data1.1/taxa/icon_doc|/data1.1/taxa/icon>.");
    
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
	    "field will only appear in results generated by C</data1.1/taxa/list_images>.",
	{ output => 'taxon_name', com_name => 'tna' },
	    "The taxonomic name with which this image is associated.  This field",
	    "will only appear in results generated by C</data1.1/taxa/list_images>.",
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
	{ param => 'name', valid => \&TaxonData::validNameSpec, 
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
    
    $ds->define_ruleset('1.1:taxa:selector' =>
	"The following parameters are used to indicate a base taxon or taxa:",
	{ param => 'name', valid => \&TaxonData::validNameSpec, list => ',', 
	  alias => 'taxon_name' },
	    "Select the all taxa matching each of the specified name(s).",
	    "To specify more than one, separate them by commas.",
	    "The C<%> character may be used as a wildcard.",
	{ param => 'base_name', valid => \&TaxonData::validNameSpec, list => ',' },
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
	{ param => 'rel', valid => $ds->valid_set('1.1:taxa:rel'), default => 'self' },
	    "Accepted values include:", $ds->document_set('1.1:taxa:rel'),
	{ param => 'status', valid => $ds->valid_set('1.1:taxa:status'), default => 'valid' },
	    "Return only names that have the specified status.  Accepted values include:",
	    $ds->document_set('1.1:taxa:status'));
    
    $ds->define_ruleset('1.1:taxa:filter' => 
	"The following parameters further filter the list of return values:",
	{ optional => 'rank', valid => \&TaxonData::validRankSpec },
	    "Return only taxonomic names at the specified rank, e.g. C<genus>.",
	{ optional => 'extant', valid => BOOLEAN_VALUE },
	    "Return only extant or non-extant taxa.",
	    "Accepted values include C<yes>, C<no>, C<1>, C<0>, C<true>, C<false>.",
	{ optional => 'depth', valid => POS_VALUE },
	    "Return only taxa no more than the specified number of levels above or",
	     "below the base taxa in the hierarchy");
    
    $ds->define_ruleset('1.1:taxa:display' => 
	"The following parameter indicates which information should be returned about each resulting name:",
	{ optional => 'show', valid => $ds->valid_set('1.1:taxa:output_map'), list => ','},
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each taxon.  Its value should be",
	    "one or more of the following, separated by commas:",
	    $ds->document_set('1.1:taxa:output_map'));

    $ds->define_ruleset('1.1:taxa:single' => 
	{ require => '1.1:taxa:specifier',
	  error => "you must specify either 'name' or 'id'" },
	{ allow => '1.1:taxa:display' }, 
	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.");
    
    $ds->define_ruleset('1.1:taxa:list' => 
	{ require => '1.1:taxa:selector',
	  error => "you must specify either of 'name', 'id'" },
	{ allow => '1.1:taxa:filter' },
	{ allow => '1.1:taxa:display' }, 
	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.");
    
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
	{ optional => 'select', valid => $ds->valid_set('1.1:taxa:refselect') },
	    "You can use this parameter to specify which kinds of references to retrieve.",
	    "The accepted values include:", $ds->document_set('1.1:taxa:refselect'),
	{ optional => 'spelling', valid => $ds->valid_set('1.1:taxa:refspelling') },
	    "You can use this parameter to specify which variants of the matching taxonomic name(s) to retrieve.",
	    "The accepted values include:", $ds->document_set('1.1:taxa:refspelling'),
	{ allow => '1.1:refs:filter' },
	{ allow => '1.1:refs:display' },
	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.",
	">If the parameter C<order> is not specified, the results are sorted alphabetically by",
	"the name of the primary author.");
    
    $ds->define_ruleset('1.1:taxa:match' =>
	{ param => 'name', valid => \&TaxonData::validNameSpec, list => ',', alias => 'taxon_name' },
	    "A valid taxonomic name, or a common abbreviation such as 'T. rex'.",
	    "The name may include the wildcard characters % and _.",
	{ optional => 'rank', valid => \&TaxonData::validRankSpec },
	    "Return only taxonomic names at the specified rank, e.g. C<genus>.",
	{ optional => 'extant', valid => BOOLEAN_VALUE },
	    "Return only extant or non-extant taxa.",
	    "Accepted values include C<yes>, C<no>, C<1>, C<0>, C<true>, C<false>.",
	{ param => 'status', valid => $ds->valid_set('1.1:taxa:status'), default => 'valid' },
	    "Return only names that have the specified status.  Accepted values include:",
	    $ds->document_set('1.1:taxa:status'),
	{ allow => '1.1:taxa:display' }, 
	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.");
    
    $ds->define_ruleset('1.1:taxa:auto' =>
	{ param => 'name', valid => ANY_VALUE, alias => 'taxon_name' },
	    "A partial name or prefix.  It must have at least 3 significant characters, and may include both a genus",
	    "(possibly abbreviated) and a species.  Examples:\n    t. rex, tyra, rex", 
	{ allow => '1.1:common_params' },
	"^You can also use any of the L<common parameters|/data1.1/common_doc.html> with this request.");
    
    $ds->define_ruleset('1.1:taxa:thumb' =>
	{ param => 'image', valid => POS_VALUE },
	    "A positive number identifying a taxon image",
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

}


# get ( )
# 
# Return a single taxon record, specified by name or number.  If name, then
# return the matching taxon with the largest size.

sub get {

    my ($self) = @_;
    
    my $dbh = $self->get_dbh;
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
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
	    $not_found_msg .= " at rank '$self->{base_taxon_rank}'";
	}
	
	($taxon_no) = $taxonomy->getTaxaByName($valid->value('name'), $name_select);
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
    
    $self->{main_sql} = $Taxonomy::SQL_STRING;
    
    # If we were asked for 'nav' info, also show the various categories
    # of subtaxa and whether or not each of the parents are extinct.
    
    if ( $self->output_key('nav') )
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
	
	if ( $r->{parent_no} )
	{
	    $r->{parent_txn} = $taxonomy->getTaxon($r->{parent_no}, { fields => ['size'] });
	}
	
	# Then add the various lists of subtaxa.
	
	unless ( $r->{phylum_no} or (defined $r->{rank} && $r->{rank} <= 20) )
	{
	    $r->{phylum_list} = [ $taxonomy->getTaxa('all_children', $taxon_no, 
						     { limit => 10, order => 'size.desc', rank => 20, fields => ['size', 'app'] } ) ];
	}
	
	unless ( $r->{class_no} or $r->{rank} <= 17 )
	{
	    $r->{class_list} = [ $taxonomy->getTaxa('all_children', $taxon_no, 
						    { limit => 10, order => 'size.desc', rank => 17, fields => ['size', 'app'] } ) ];
	}
	
	unless ( $r->{order_no} or $r->{rank} <= 13 )
	{
	    my $order = defined $r->{order_count} && $r->{order_count} > 100 ? undef : 'size.desc';
	    $r->{order_list} = [ $taxonomy->getTaxa('all_children', $taxon_no, 
						    { limit => 10, order => $order, rank => 13, fields => ['size', 'app'] } ) ];
	}
	
	unless ( $r->{family_no} or $r->{rank} <= 9 )
	{
	    my $order = defined $r->{family_count} && $r->{family_count} > 100 ? undef : 'size.desc';
	    $r->{family_list} = [ $taxonomy->getTaxa('all_children', $taxon_no, 
						     { limit => 10, order => $order, rank => 9, fields => ['size', 'app'] } ) ];
	}
	
	if ( $r->{rank} > 5 )
	{
	    my $order = defined $r->{genus_count} && $r->{order_count}> 100 ? undef : 'size.desc';
	    $r->{genus_list} = [ $taxonomy->getTaxa('all_children', $taxon_no,
						    { limit => 10, order => $order, rank => 5, fields => ['size', 'app'] } ) ];
	}
	
	if ( $r->{rank} == 5 )
	{
	    $r->{subgenus_list} = [ $taxonomy->getTaxa('all_children', $taxon_no,
						       { limit => 10, order => 'size.desc', rank => 4, fields => ['size', 'app'] } ) ];
	}
	
	if ( $r->{rank} == 5 or $r->{rank} == 4 )
	{
	    $r->{species_list} = [ $taxonomy->getTaxa('all_children', $taxon_no,
						       { limit => 10, order => 'size.desc', rank => 3, fields => ['size', 'app'] } ) ];
	}
	
	$r->{children} = 
	    [ $taxonomy->getTaxa('children', $taxon_no, { limit => 10, order => 'size.desc', fields => ['size', 'app'] } ) ];
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
    
    my $dbh = $self->get_dbh;
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    
    # First, figure out what info we need to provide
    
    my $options = $self->generate_query_options();
    
    # Then, figure out which taxa we are looking for.
    
    my $name_list = $self->clean_param('name');
    my $id_list = $self->clean_param('id');
    my $rel = $self->clean_param('rel');
    
    if ( my $base_name = $self->clean_param('base_name') )
    {
	$name_list = $base_name;
	$rel ||= 'all_children';
    }
    
    elsif ( my $base_id = $self->clean_param('base_id') )
    {
	$id_list = $base_id;
	$rel ||= 'all_children';
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
	    push @ids, $taxonomy->getTaxaByName($name, $name_options);
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
	$self->{main_sql} = $Taxonomy::SQL_STRING;
	$self->{result_count} = scalar(@result);
    }
    
    elsif ( defined $arg && $arg eq 'refs' )
    {
	$options->{return} = 'stmt';
	($self->{main_sth}) = $taxonomy->getTaxonReferences($rel, $id_list, $options);
	$self->{main_sql} = $Taxonomy::SQL_STRING;
	$self->sql_count_rows;
    }
    
    # Otherwise, return matching taxa.  If the relationship is 'self' (the
    # default) then just return the list of matches.
    
    elsif ( $rel eq 'self' )
    {
	my @result = $taxonomy->getTaxa('self', $id_list, $options);
	$self->{main_result} = \@result;
	$self->{main_sql} = $Taxonomy::SQL_STRING;
	$self->{result_count} = scalar(@result);
    }
    
    # If the relationship is 'common_ancestor', we have just one result.
    
    elsif ( $rel eq 'common_ancestor' )
    {
	$options->{return} = 'list';
	
	($self->{main_record}) = $taxonomy->getTaxa('common_ancestor', $id_list, $options);	
	$self->{main_sql} = $Taxonomy::SQL_STRING;
	$self->{result_count} = defined $self->{main_record} ? 1 : 0;
    }
    
    # Otherwise, we just call getTaxa and return the result.
    
    else
    {
	$options->{return} = 'stmt';
	my $rel = $self->clean_param('rel') || 'self';
	
	($self->{main_sth}) = $taxonomy->getTaxa($rel, $id_list, $options);
	$self->{main_sql} = $Taxonomy::SQL_STRING;
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
    
    my $dbh = $self->get_dbh;
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    
    # Make sure we have at least one valid name.
    
    my $name_list = $self->clean_param('name');
    
    return unless $name_list;
    
    # Figure out the proper query options.
    
    my $options = $self->generate_query_options();
    
    $options->{exact} = 1;
    
    # Get the list of matches.
    
    my @name_matches = $taxonomy->getTaxaByName($name_list, $options);
    
    $self->{main_result} = \@name_matches if scalar(@name_matches);
    $self->{result_count} = scalar(@name_matches);
}


# list_refs ( )
# 
# Query the database for basic info about all references associated with taxa
# that meet the specified parameters.

sub list_refs {

    my ($self) = @_;
    
    my $dbh = $self->get_dbh;
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    
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
	
	@taxon_list = $taxonomy->getTaxaByName($name, $name_select);
	return unless @taxon_list;
    }
    
    # Now do the main query and return a result:
    
    # If a name was given and the relationship is 'self' (or not specified,
    # being the default) then just return the list of matches.
    
    if ( $self->clean_param('name') and $rel eq 'self' )
    {
	my @result = $taxonomy->getTaxonReferences('self', \@taxon_list, $options);
	$self->{main_result} = \@result;
	$self->{main_sql} = $Taxonomy::SQL_STRING;
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
	$self->{main_sql} = $Taxonomy::SQL_STRING;
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


# generate_query_options ( )
# 
# Return an options hash, based on the parameters, which can be passed to
# getTaxaByName or getTaxa.

sub generate_query_options {
    
    my ($self, $operation) = @_;
    
    my @fields = $self->select_list() unless defined $operation && $operation eq 'ref';
    my $limit = $self->result_limit;
    my $offset = $self->result_offset(1);
    
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
    
    return $options;
}


# auto ( )
# 
# Return an auto-complete list, given a partial name.

sub auto {
    
    my ($self) = @_;
    
    my $dbh = $self->get_dbh;
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
    
    my $fields = "taxon_rank, match_no as taxon_no, n_occs, if(spelling_reason = 'misspelling', 1, null) as misspelling";
    
    # If we are given a genus (possibly abbreviated), generate a search on
    # genus and species name.
    
    if ( $partial =~ qr{^([a-zA-Z_]+)(\.|[.%]? +)([a-zA-Z_]+)} )
    {
	my $genus = $2 ? $dbh->quote("$1%") : $dbh->quote($1);
	my $species = $dbh->quote("$3%");
	
	$sql = "SELECT $calc concat(genus, ' ', taxon_name) as taxon_name, $fields
		FROM $search_table as s JOIN $attrs_table as v on v.orig_no = s.result_no
			JOIN $names_table as n on n.taxon_no = s.match_no
		WHERE genus like $genus and taxon_name like $species ORDER BY n_occs desc $limit";
    }
    
    # If we are given a single name followed by one or more spaces and nothing
    # else, take it as a genus name.
    
    elsif ( $partial =~ qr{^([a-zA-Z]+)([.%])? +$} )
    {
	my $genus = $2 ? $dbh->quote("$1%") : $dbh->quote($1);
	
	$sql = "SELECT $calc concat(genus, ' ', taxon_name) as taxon_name, $fields
		FROM $search_table as s JOIN $attrs_table as v on v.orig_no = s.result_no
			JOIN $names_table as n on n.taxon_no = s.match_no
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
	        FROM $search_table as s JOIN $attrs_table as v on v.orig_no = s.result_no
			JOIN $names_table as n on n.taxon_no = s.match_no
	        WHERE taxon_name like $name ORDER BY n_occs desc $limit";
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
    
    my $dbh = $self->get_dbh;
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
	
	$self->{main_record} = $dbh->selectrow_hashref($self->{main_sql});
	
	return;
    }
}


# list_images ( )
# 
# Return a list of images that meet the specified criteria.

sub list_images {

    my ($self) = @_;
    
    my $dbh = $self->get_dbh;
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    my ($sql, $result);
    
    my @filters;
    
    # If the parameter 'name' was given, then fetch all matching taxa.  Order
    # them in descending order by size.
    
    my @taxon_list;
    
    if ( my $name = $self->clean_param('name') )
    {
	my $name_select = { spelling => 'exact', return => 'id' };
	
	@taxon_list = $taxonomy->getTaxaByName($name, $name_select);
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
