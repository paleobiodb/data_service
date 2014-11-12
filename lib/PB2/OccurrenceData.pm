# 
# OccurrenceData
# 
# A class that returns information from the PaleoDB database about a single
# occurence or a category of occurrences.  It is a subclass of 'CollectionData'.
# 
# Author: Michael McClennen

use strict;

package PB2::OccurrenceData;

use HTTP::Validate qw(:validators);

use TableDefs qw($OCC_MATRIX $COLL_MATRIX $COLL_BINS $COUNTRY_MAP $PALEOCOORDS $GEOPLATES
		 $INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP $INTERVAL_BUFFER);

use TaxonDefs qw(@TREE_TABLE_LIST %TAXON_TABLE %TAXON_RANK %RANK_STRING);
use Taxonomy;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::ReferenceData PB2::TaxonData PB2::CollectionData);


# initialize ( )
# 
# This routine is called by the DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($self, $ds) = @_;
    
    # We start by defining an output map for this class.
    
    $ds->define_output_map('1.2:occs:basic_map' =>
	{ value => 'coords', maps_to => '1.2:occs:geo' },
	     "The latitude and longitude of this occurrence",
        { value => 'attr', maps_to => '1.2:colls:attr' },
	    "The attribution of the occurrence: the author name(s) from",
	    "the primary reference, and the year of publication.  If no reference",
	    "is recorded for this occurrence, the reference for its collection is used.",
	{ value => 'ident', maps_to => '1.2:occs:ident' },
	    "The actual taxonomic name by which this occurrence was identified",
	{ value => 'phylo', maps_to => '1.2:occs:phylo' },
	    "Additional information about the taxonomic classification of the occurence",
	{ value => 'genus', maps_to => '1.2:occs:genus' },
	    "The genus (if known) and subgenus (if any) corresponding to each occurrence.",
	    "This is a subset of the information provided by C<phylo>.",
        { value => 'loc', maps_to => '1.2:colls:loc' },
	    "Additional information about the geographic locality of the occurrence",
	{ value => 'paleoloc', maps_to => '1.2:colls:paleoloc' },
	    "Information about the paleogeographic locality of the occurrence,",
	    "evaluated according to the model specified by the parameter C<pgm>.",
	{ value => 'prot', maps_to => '1.2:colls:prot' },
	    "Indicate whether the containing collection is on protected land",
        { value => 'time', maps_to => '1.2:colls:time' },
	    "Additional information about the temporal locality of the occurrence",
	{ value => 'strat', maps_to => '1.2:colls:strat' },
	    "Basic information about the stratigraphic context of the occurrence.",
	{ value => 'stratext', maps_to => '1.2:colls:stratext' },
	    "Detailed information about the stratigraphic context of the occurrence.",
	    "This includes all of the information from C<strat> plus extra fields.",
	{ value => 'lith', maps_to => '1.2:colls:lith' },
	    "Basic information about the lithological context of the occurrence.",
	{ value => 'lithext', maps_to => '1.2:colls:lithext' },
	    "Detailed information about the lithological context of the occurrence.",
	    "This includes all of the information from C<lith> plus extra fields.",
	{ value => 'abund', maps_to => '1.2:occs:abund' },
	    "Information about the abundance of this occurrence in the collection",
	{ value => 'geo', maps_to => '1.2:colls:geo' },
	    "Information about the geological context of the occurrence",
        { value => 'rem', maps_to => '1.2:colls:rem' },
	    "Any additional remarks that were entered about the containing collection.",
        { value => 'ref', maps_to => '1.2:refs:primary' },
	    "The primary reference for the occurrence, as formatted text.",
	    "If no reference is recorded for this occurrence, the primary reference for its",
	    "collection is returned.",
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the occurrence record");
    
    # Then define those blocks which are not already defined in
    # CollectionData.pm 
    
    $ds->define_block('1.2:occs:basic' =>
	{ select => ['o.occurrence_no', 'o.reid_no', 'o.latest_ident', 'o.collection_no', 'o.taxon_no',
		     'ts.spelling_no as matched_no', 'ts.name as matched_name', 'ts.rank as matched_rank', 'ts.lft as tree_seq',
		     'ei.interval_name as early_interval', 'li.interval_name as late_interval',
		     'o.genus_name', 'o.genus_reso', 'o.subgenus_name', 'o.subgenus_reso', 'o.species_name', 'o.species_reso',
		     'o.early_age', 'o.late_age', 'o.reference_no'],
	  tables => ['o', 't', 'ts', 'ei', 'li'] },
	{ set => '*', from => '*', code => \&process_basic_record },
	{ output => 'occurrence_no', dwc_name => 'occurrenceID', com_name => 'oid' },
	    "A positive integer that uniquely identifies the occurrence",
	{ output => 'record_type', value => 'occurrence', com_name => 'typ', com_value => 'occ', 
	  dwc_value => 'Occurrence',  },
	    "The type of this object: 'occ' for an occurrence",
	{ output => 'reid_no', com_name => 'eid', if_field => 'reid_no' },
	    "If this occurrence was reidentified, a positive integer that uniquely identifies the reidentification",
	{ output => 'superceded', com_name => 'sps', value => 1, not_field => 'latest_ident' },
	    "The value of this field will be true if this occurrence was later identified under a different taxon",
	{ output => 'collection_no', com_name => 'cid', dwc_name => 'CollectionId' },
	    "The identifier of the collection with which this occurrence is associated.",
	{ output => 'taxon_name', com_name => 'tna', dwc_name => 'associatedTaxa' },
	    "The taxonomic name by which this occurrence is identified",
	{ output => 'taxon_rank', dwc_name => 'taxonRank', com_name => 'rnk' },
	    "The taxonomic rank of the name, if this can be determined",
	{ set => 'taxon_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb' },
	{ output => 'taxon_no', com_name => 'tid' },
	    "The unique identifier of the identified taxonomic name.  If this is empty, then",
	    "the name was never entered into the taxonomic hierarchy stored in this database and",
	    "we have no further information about the classification of this occurrence.",
	{ output => 'matched_name', com_name => 'mna', dedup => 'taxon_base' },
	    "The senior synonym and/or currently accepted spelling of the closest matching name in",
	    "the database to the identified taxonomic name, if any is known, and if this name",
	    "is different from the value of C<taxon_name>.",
	{ output => 'matched_rank', com_name => 'mra', dedup => 'taxon_rank' },
	    "The taxonomic rank of the matched name, if different from the value of C<taxon_rank>",
	{ output => 'matched_no', com_name => 'mid', if_field => 'mid', dedup => 'taxon_no' },
	    "The unique identifier of the closest matching name in the database to the identified",
	    "taxonomic name, if any is known.",
	{ set => '*', code => \&PB2::CollectionData::fixTimeOutput },
	{ output => 'early_interval', com_name => 'oei', pbdb_name => 'early_interval' },
	    "The specific geologic time range associated with this collection (not necessarily a",
	    "standard interval), or the interval that begins the range if C<late_interval> is also given",
	{ output => 'late_interval', com_name => 'oli', pbdb_name => 'late_interval', dedup => 'early_interval' },
	    "The interval that ends the specific geologic time range associated with this collection,",
	    "if different from the value of C<early_interval>",
	{ output => 'early_age', com_name => 'eag' },
	    "The early bound of the geologic time range associated with this occurrence (in Ma)",
	{ output => 'late_age', com_name => 'lag' },
	    "The late bound of the geologic time range associated with this occurrence (in Ma)",
	{ set => 'reference_no', append => 1 },
	{ output => 'reference_no', com_name => 'rid' },
	    "The identifier(s) of the references from which this data was entered");
    
    $ds->define_block('1.2:occs:ident' =>
	{ select => ['o.genus_name', 'o.genus_reso', 'o.subgenus_name',
		     'o.subgenus_reso', 'o.species_name', 'o.species_reso'],
	  tables => 'o' },
	{ output => 'genus_name', com_name => 'idt' },
	    "The taxonomic name (less species) by which this occurrence was identified.",
	    "This is often a genus, but may be a higher taxon.",
	{ output => 'genus_reso', com_name => 'rst' },
	    "The resolution of this taxonomic name, i.e. C<sensu lato> or C<aff.>",
	{ output => 'subgenus_name', com_name => 'idf' },
	    "The subgenus name (if any) by which this occurrence was identified",
	{ output => 'subgenus_reso', com_name => 'rsf' },
	    "The resolution of the subgenus name, i.e. C<informal> or C<n. subgen.>",
	{ output => 'species_name', com_name => 'ids' },
	    "The species name (if any) by which this occurrence was identified",
	{ output => 'species_reso', com_name => 'rss' },
	    "The resolution of the species name, i.e. C<sensu lato> or C<n. sp.>");
    
    $ds->define_block('1.2:occs:genus' =>
	{ select => ['o.genus_name', 'o.subgenus_name',
		     'pl.genus', 'pl.genus_no', 'pl.subgenus', 'pl.subgenus_no'],
	  tables => ['t', 'pl'] },
	{ output => 'subgenus', com_name => 'sgl' },
	    "The name of the genus in which this occurrence is classified",
	{ output => 'subgenus_no', com_name => 'sgn' },
	    "The identifier of the genus in which this occurrence is classified",
	{ output => 'genus', com_name => 'gnl' },
	    "The name of the genus in which this occurrence is classified",
	{ output => 'genus_no', com_name => 'gnn' },
	    "The identifier of the genus in which this occurrence is classified");
    
    $ds->define_block('1.2:occs:phylo' =>
	{ select => ['ph.family', 'ph.family_no', 'ph.order', 'ph.order_no',
		     'ph.class', 'ph.class_no', 'ph.phylum', 'ph.phylum_no',
		     'pl.genus', 'pl.genus_no', 'pl.subgenus', 'pl.subgenus_no'],
	  tables => ['ph', 't', 'pl'] },
	{ output => 'subgenus', com_name => 'sgl', not_block => '1.2:occs:genus' },
	    "The name of the genus in which this occurrence is classified",
	{ output => 'subgenus_no', com_name => 'sgn', not_block => '1.2:occs:genus' },
	    "The identifier of the genus in which this occurrence is classified",
	{ output => 'genus', com_name => 'gnl', not_block => '1.2:occs:genus' },
	    "The name of the genus in which this occurrence is classified",
	{ output => 'genus_no', com_name => 'gnn', not_block => '1.2:occs:genus' },
	    "The identifier of the genus in which this occurrence is classified",
	{ output => 'family', com_name => 'fml' },
	    "The name of the family in which this occurrence is classified",
	{ output => 'family_no', com_name => 'fmn' },
	    "The identifier of the family in which this occurrence is classified",
	{ output => 'order', com_name => 'odl' },
	    "The name of the order in which this occurrence is classified",
	{ output => 'order_no', com_name => 'odn' },
	    "The identifier of the order in which this occurrence is classified",
	{ output => 'class', com_name => 'cll' },
	    "The name of the class in which this occurrence is classified",
	{ output => 'class_no', com_name => 'cln' },
	    "The identifier of the class in which this occurrence is classified",
	{ output => 'phylum', com_name => 'phl' },
	    "The name of the phylum in which this occurrence is classified",
	{ output => 'phylum_no', com_name => 'phn' },
	    "The identifier of the phylum in which this occurrence is classified");
    
    $ds->define_block('1.2:occs:geo' =>
	{ select => ['c.lat', 'c.lng'], tables => 'c' },
        { output => 'lng', dwc_name => 'decimalLongitude', com_name => 'lng' },
	    "The longitude at which the occurrence was found (in degrees)",
        { output => 'lat', dwc_name => 'decimalLatitude', com_name => 'lat' },
	    "The latitude at which the occurrence was found (in degrees)");
    
    $ds->define_block('1.2:occs:abund' =>
	{ select => ['oc.abund_unit', 'oc.abund_value'], tables => ['oc'] },
	{ output => 'abund_value', com_name => 'abv' },
	    "The abundance of this occurrence within its containing collection",
	{ output => 'abund_unit', com_name => 'abu' },
	    "The unit in which this abundance is expressed");
    
    # The following block specifies the output for diversity matrices.
    
    $ds->define_block('1.2:occs:diversity' =>
	{ output => 'interval_no', com_name => 'oid' },
	    "The identifier of the time interval represented by this record",
	{ output => 'interval_name', com_name => 'nam' },
	    "The name of the time interval represented by this record",
	{ output => 'early_age', com_name => 'eag' },
	    "The beginning age of this interval, in Ma",
	{ output => 'late_age', com_name => 'lag' },
	    "The ending age of this interval, in Ma",
	{ output => 'originations', pbdb_name => 'X_Ft', com_name => 'xft' },
	    "The number of distinct taxa whose first known occurrence lies in this interval,",
	    "and whose range crosses the top boundary of the interval:",
	    "either species, genera, families, or orders,",
	    "depending upon the value you provided for the parameter C<count>.",
	    "The terminology for this field and the next three comes from:",
	    "M. Foote. The Evolution of Morphological Diversity.",
	    "I<Annual Review of Ecology and Systematics>, Vol. 28 (1997)",
	    "pp. 129-152. L<http://www.jstor.org/stable/2952489>.",
	{ output => 'extinctions', pbdb_name =>'X_bL', com_name => 'xbl' },
	    "The number of distinct taxa whose last known occurrence lies in this interval,",
	    "and whose range crosses the bottom boundary of the interval.",
	{ output => 'singletons', pbdb_name => 'X_FL', com_name => 'xfl' },
	    "The number of distinct taxa that are found only in this interval, so",
	    "that their range of occurrence does not cross either boundary.",
	{ output => 'range_throughs', pbdb_name => 'X_bt', com_name => 'xbt' },
	    "The number of distinct taxa whose first occurrence falls before",
	    "this interval and whose last occurrence falls after it, so that",
	    "the range of occurrence crosses both boundaries.  Note that",
	    "these taxa may or may not actually occur within the interval.",
	{ output => 'sampled_in_bin', com_name => 'dsb' },
	    "The number of distinct taxa found in this interval.  This is",
	    "equal to the sum of the previous four fields, minus the number",
	    "of taxa from Xbt that do not actually occur in this interval.",
	{ output => 'n_occs', com_name => 'noc' },
	    "The total number of occurrences that are resolved to this interval");
    
    # The following block specifies the summary output for diversity matrices.
    
    $ds->define_block('1.2:occs:diversity:summary' =>
	{ output => 'total_count', pbdb_name => 'n_occs', com_name => 'noc' },
	    "The number of occurrences that were scanned in the process of",
	    "computing this diversity result.",
	{ output => 'bin_count', pbdb_name => 'bin_total', com_name => 'tbn' },
	    "The sum of occurrence counts in all of the bins.  This value may be larger than",
	    "the number of occurrences scanned, since some may be counted in multiple",
	    "bins (see C<timerule>).  This value might also be smaller",
	    "than the number of occurrences scanned, since some occurrences may",
	    "not have a temporal locality that is precise enough to put in any bin.",
	{ output => 'imprecise_time', com_name => 'itm' },
	    "The number of occurrences skipped because their temporal locality",
	    "was not sufficiently precise.  You can adjust this number by selecting",
	    "a different time rule and/or a different level of temporal resolution.",
	{ output => 'imprecise_taxon', com_name => 'itx' },
	    "The number of occurrences skipped because their taxonomic identification",
	    "was not sufficiently precise.  You can adjust this number by",
	    "counting at a higher or lower taxonomic level.",
	{ output => 'missing_taxon', com_name => 'mtx' },
	    "The number of occurrences skipped because the taxonomic hierarchy",
	    "in this database is incomplete.  For example, some genera have not",
	    "been placed in their proper family, so occurrences in these genera",
	    "will be skipped if you are counting families.");
    
    # The following block specifies the output for phylogenies.
    
    $ds->define_block('1.2:occs:phylogeny' =>
	{ output => 'taxon_no', com_name => 'oid' },
	    "The identifier of the taxon represented by this record",
	{ output => 'parent_no', com_name => 'pid' },
	    "The identifier of the parent taxon.  You can use this field to assemble",
	    "these records into one or more taxonomic trees.  A value of 0",
	    "indicates a root of one of these trees.  By default, records representing",
	    "classes have a value of 0 in this field.",
	{ output => 'taxon_rank', com_name => 'rnk' },
	    "The rank of the taxon represented by this record",
	{ set => 'taxon_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb' },
	{ output => 'taxon_name', com_name => 'nam' },
	    "The name of the taxon represented by this record",
	{ output => 'n_occs', com_name => 'noc' },
	    "The number of occurrences of this taxon (or any of its subtaxa) in the",
	    "set of fossil occurrences being analyzed",
	{ output => 'n_orders', com_name => 'odc' },
	    "The number of orders from within this taxon that appear in the set of",
	    "fossil occurrences being analyzed",
	{ output => 'n_families', com_name => 'fmc' },
	    "The number of families from within this taxon that appear in the set of",
	    "fossil occurrences being analyzed",
	{ output => 'n_genera', com_name => 'gnc' },
	    "The number of genera from within this taxon that appear in the set of",
	    "fossil occurrences being analyzed",
	{ output => 'n_species', com_name => 'spc' },
	    "The number of species from within this taxon that appear in the set of",
	    "fossil occurrences being analyzed");
    
    # Then define parameter rulesets to validate the parameters passed to the
    # operations implemented by this class.
    
    $ds->define_set('1.2:occs:div_count' =>
	{ value => 'species' },
	    "Count species.",
	{ value => 'genera' },
	    "Count genera.  You can also use the value C<genus>.",
	{ value => 'genus', undocumented => 1 },
	{ value => 'genera_plus' },
	    "Count genera, with subgenera promoted to genera.  You can also use the value C<genus_plus>.",
	{ value => 'genus_plus', undocumented => 1 },
	{ value => 'families' },
	    "Count families.  You can also use the value C<family>.",
	{ value => 'family', undocumented => 1 },
	{ value => 'orders' },
	    "Count orders.  You can also use the value C<order>.",
	{ value => 'order', undocumented => 1 });
    
    $ds->define_set('1.2:occs:div_reso' =>
	{ value => 'stage', maps_to => 5 },
	    "Count by stage",
	{ value => 'epoch', maps_to => 4 },
	    "Count by epoch",
	{ value => 'period', maps_to => 3 },
	    "Count by period");
    
    $ds->define_set('1.2:occs:order' =>
	{ value => 'earlyage' },
	    "Results are ordered chronologically by early age bound, oldest to youngest unless you add C<.asc>",
	{ value => 'earlyage.asc', undocumented => 1 },
	{ value => 'earlyage.desc', undocumented => 1 },
	{ value => 'lateage' },
	    "Results are ordered chronologically by late age bound, oldest to youngest unless you add C<.asc>",
	{ value => 'lateage.asc', undocumented => 1 },
	{ value => 'lateage.desc', undocumented => 1 },
	{ value => 'agespan' },
	    "Results are ordered based on the difference between the early and late age bounds, starting",
	    "with occurrences with the smallest spread (most precise temporal resolution) unless you add C<.desc>",
	{ value => 'agespan.asc', undocumented => 1 },
	{ value => 'agespan.desc', undocumented => 1 },
	{ value => 'taxon' },
	    "Results are ordered hierarchically by taxonomic identification.",
	    "The order of sibling taxa is arbitrary, but children will follow immediately",
	    "after parents.",
	{ value => 'taxon.asc', undocumented => 1 },
	{ value => 'taxon.desc', undocumented => 1 },
	{ value => 'reso' },
	    "Results are ordered according to the taxonomic rank to which they are resolved.  Unless",
	    "you add C<.desc>, this would start with subspecies, species, genus, ...",
	{ value => 'reso.asc', undocumented => 1 },
	{ value => 'reso.desc', undocumented => 1 },
	{ value => 'formation' },
	    "Results are ordered by the geological formation in which they were found, sorted alphabetically.",
	{ value => 'formation.asc', undocumented => 1 },
	{ value => 'formation.desc', undocumented => 1 },
	{ value => 'geogroup' },
	    "Results are ordered by the geological group in which they were found, sorted alphabetically.",
	{ value => 'geogroup.asc', undocumented => 1 },
	{ value => 'geogroup.desc', undocumented => 1 },
	{ value => 'member' },
	    "Results are ordered by the geological member in which they were found, sorted alphabetically.",
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
    
    $ds->define_ruleset('1.2:occs:specifier' =>
	{ param => 'id', valid => POS_VALUE, alias => 'occ_id' },
	    "The identifier of the occurrence you wish to retrieve (REQUIRED)");
    
    $ds->define_ruleset('1.2:occs:selector' =>
	{ param => 'id', valid => POS_VALUE, list => ',', alias => 'occ_id' },
	    "A comma-separated list of occurrence identifiers.",
	{ param => 'coll_id', valid => POS_VALUE, list => ',' },
	    "A comma-separated list of collection identifiers.  All occurrences associated with",
	    "the specified collections are returned, provided they satisfy the other parameters",
	    "given with this request.");
    
    $ds->define_ruleset('1.2:occs:display' =>
	"You can use the following parameters to select what information you wish to retrieve,",
	"and the order in which you wish to get the records:",
	{ optional => 'show', list => q{,}, valid => '1.2:occs:basic_map' },
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each occurrence.  Its value should be",
	    "one or more of the following, separated by commas:",
	{ optional => 'order', valid => '1.2:occs:order', split => ',', no_set_doc => 1 },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  Accepted values are:",
	    $ds->document_set('1.2:occs:order'),
	    "If no order is specified, results are sorted by occurrence identifier.",
	{ ignore => 'level' });
    
    $ds->define_ruleset('1.2:occs:single' => 
	"The following parameter selects a record to retrieve:",
    	{ require => '1.2:occs:specifier', 
	  error => "you must specify an occurrence identifier, either in the URL or with the 'id' parameter" },
	">>You may also use the following parameter to specify what information you wish to retrieve:",
    	{ optional => 'SPECIAL(show)', valid => '1.2:occs:basic_map' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:list' => 
	"You can use the following parameters if you wish to retrieve information about",
	"a known list of occurrences or collections, or to filter a known list against",
	"other criteria such as location or time.",
	"Only the records which match the other parameters that you specify will be returned.",
	{ allow => '1.2:occs:selector' },
        ">>The following parameters can be used to query for occurrences by a variety of criteria.",
	"Except as noted below, you may use these in any combination.",
	"These same parameters can all be used to select either occurrences, collections, or associated references or taxa.",
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:common:select_crmod' },
	{ allow => '1.2:common:select_ent' },
	{ require_any => ['1.2:occs:selector', '1.2:main_selector',
			  '1.2:common:select_crmod', '1.2:common:select_ent'] },
	{ allow => '1.2:occs:display' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:div_params' =>
	{ param => 'count', valid => '1.2:occs:div_count' },
	    "This parameter specifies the taxonomic level at which to count.  If not",
	    "specified, it defaults to C<genera>.  The accepted values are:",
	{ param => 'subgenera', valid => FLAG_VALUE },
	    "You can use this parameter as a shortcut, equivalent to specifying",
	    "C<count=genera_plus>.  Just include its name, no value is needed.",
	{ param => 'recent', valid => FLAG_VALUE },
	    "If this parameter is specified, then taxa that are known to be extant",
	    "are considered to range through to the present, regardless of the age",
	    "of their last known fossil occurrence.",
	{ param => 'reso', valid => '1.2:occs:div_reso' },
	    "This parameter specifies the temporal resolution at which to count.  If not",
	    "specified, it defaults to C<stage>.  Accepted values are:");
    
    $ds->define_ruleset('1.2:occs:diversity' =>
	"The following parameters specify what to count and at what temporal resolution:",
	{ allow => '1.2:occs:div_params' }, 
        ">>The following parameters select which occurrences to analyze.",
	"Except as noted below, you may use these in any combination.",
	"All of these parameters can be used with L<occs/list|node:occs/list> as well, to retrieve",
	"the exact list of occurrences used to compute this diversity matrix.",
	{ allow => '1.2:main_selector' },
	#{ mandatory => 'base_name', errmsg => 'You must include the parameter "base_name", ' .
	#      'in order to specify the taxonomic range to analyze.' },
	{ allow => '1.2:common:select_crmod' },
	{ allow => '1.2:common:select_ent' },
	{ require_any => ['1.2:main_selector',
			  '1.2:common:select_crmod', '1.2:common:select_ent'] },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:phylo_params' =>
	{ param => 'count', valid => '1.2:occs:div_count' },
	    "This parameter specifies the taxonomic level at which to count.  If not",
	    "specified, it defaults to C<genera>.  The accepted values are:",
	{ param => 'subgenera', valid => FLAG_VALUE },
	    "You can use this parameter as a shortcut, equivalent to specifying",
	    "C<count=genera_plus>.  Just include its name, no value is needed.",
	{ param => 'reso', valid => ANY_VALUE },
	    "This parameter specifies the temporal resolution at which to count.  If not",
	    "specified, it defaults to C<families>.  Accepted values are:");
    
    $ds->define_ruleset('1.2:occs:phylogeny' =>
	"The following parameters specify what to count and at what taxonomic resolution:",
	{ allow => '1.2:occs:phylo_params' }, 
        ">>The following parameters select which occurrences to analyze.",
	"Except as noted below, you may use these in any combination.",
	"All of these parameters can be used with L<occs/list|node:occs/list> as well, to retrieve",
	"the exact list of occurrences used to compute this phylogeny.",
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:common:select_crmod' },
	{ allow => '1.2:common:select_ent' },
	{ require_any => ['1.2:main_selector',
			  '1.2:common:select_crmod', '1.2:common:select_ent'] },
	#{ allow => '1.2:occs:display' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:refs' =>
	"You can use the following parameters if you wish to retrieve the references associated",
	"with a known list of occurrences or collections, or to filter a known list against",
	"other criteria such as location or time.",
	"Only the records which match the other parameters that you specify will be returned.",
	{ allow => '1.2:occs:selector' },
        ">>The following parameters can be used to retrieve the references associated with occurrences",
	"selected by a variety of criteria.  Except as noted below, you may use these in any combination.",
	"These same parameters can all be used to select either occurrences, collections, or associated references or taxa.",
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:common:select_crmod' },
	{ allow => '1.2:common:select_ent' },
	{ require_any => ['1.2:occs:selector', '1.2:main_selector',
			  '1.2:common:select_crmod', '1.2:common:select_ent'] },
	"You can also specify any of the following parameters:",
	{ allow => '1.2:refs:filter' },
	{ allow => '1.2:refs:display' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");

    $ds->define_ruleset('1.2:occs:taxa' =>
	"You can use the following parameters if you wish to retrieve the taxa associated",
	"with a known list of occurrences or collections, or to filter a known list against",
	"other criteria such as location or time.",
	"Only the records which match the other parameters that you specify will be returned.",
	{ allow => '1.2:occs:selector' },
        "The following parameters can be used to retrieve the taxa associated with a specified set of occurrences,",
	"selected by a variety of criteria.  Except as noted below, you may use these in any combination.",
	"These same parameters can all be used to select either occurrences, collections, or associated references or taxa.",
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:common:select_crmod' },
	{ allow => '1.2:common:select_ent' },
	{ require_any => ['1.2:occs:selector', '1.2:main_selector',
			  '1.2:common:select_crmod', '1.2:common:select_ent'] },
	"The following parameters select the particular set of results that should be returned:",
	{ allow => '1.2:taxa:summary_selector' },
	{ allow => '1.2:taxa:occ_filter' },
	{ allow => '1.2:taxa:display' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request",
	">>If the parameter C<order> is not specified, the results are sorted alphabetically by",
	"taxonomic name.");    
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
    
    die "Bad identifier '$id'" unless $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $self->substitute_select( mt => 'o', cd => 'oc' );
    
    my $fields = $self->select_string;
    
    $self->adjustCoordinates(\$fields);
    $self->selectPaleoModel(\$fields, $self->tables_hash) if $fields =~ /PALEOCOORDS/;
    
    # Determine the necessary joins.
    
    my ($join_list) = $self->generateJoinList('c', $self->tables_hash);
    
    # Generate the main query.
    
    $self->{main_sql} = "
	SELECT $fields
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c on o.collection_no = c.collection_no
		$join_list
        WHERE o.occurrence_no = $id and c.access_level = 0
	GROUP BY o.occurrence_no";
    
    print STDERR $self->{main_sql} if $self->debug;
    
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
    
    my $dbh = $self->get_connection;
    my $tables = $self->tables_hash;
    
    $self->substitute_select( mt => 'o', cd => 'oc' );
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = PB2::CollectionData::generateMainFilters($self, 'list', 'c', $tables);
    push @filters, PB2::OccurrenceData::generateOccFilters($self, $tables);
    push @filters, PB2::CommonData::generate_crmod_filters($self, 'o', $tables);
    push @filters, PB2::CommonData::generate_ent_filters($self, 'o', $tables);
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    $self->add_table('oc');
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $self->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $self->add_output_block('1.2:occs:unknown_taxon') if $tables->{unknown_taxon};
    
    my $fields = $self->select_string;
    
    $self->adjustCoordinates(\$fields);
    $self->selectPaleoModel(\$fields, $self->tables_hash) if $fields =~ /PALEOCOORDS/;
        
    # Determine the order in which the results should be returned.
    
    my $tt = $tables->{ts} ? 'ts' : 't';
    
    my $order_clause = $self->PB2::CollectionData::generate_order_clause($tables, { at => 'c', cd => 'cc', tt => $tt }) || 'o.occurrence_no';
    
    # Determine which extra tables, if any, must be joined to the query.  Then
    # construct the query.
    
    my $join_list = $self->generateJoinList('c', $tables);
    
    my $extra_group = $tables->{group_by_reid} ? ', o.reid_no' : '';
    
    $self->{main_sql} = "
	SELECT $calc $fields
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c on o.collection_no = c.collection_no
		$join_list
        WHERE $filter_string
	GROUP BY o.occurrence_no $extra_group
	ORDER BY $order_clause
	$limit";
    
    print STDERR "$self->{main_sql}\n\n" if $self->debug;
    
    # Then prepare and execute the main query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $self->sql_count_rows;
}


# diversity ( )
# 
# Like 'list', but processes the resulting list of occurrences into a
# diversity matrix.

sub diversity {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_connection;
    my $tables = $self->tables_hash;
    
    # Make sure that we have already loaded the interval data.
    
    PB2::IntervalData->read_interval_data($dbh);
    
    # Set up the diversity-computation options
    
    my $options = {};
    
    $options->{timerule} = $self->clean_param('timerule') || 'buffer';
    $options->{timebuffer} = $self->clean_param('timebuffer');
    
    my $reso_param = $self->clean_param('reso');
    
    my %level_map = ( stage => 5, epoch => 4, period => 3 );
    
    $options->{timereso} = $level_map{$reso_param} || 5;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = PB2::CollectionData::generateMainFilters($self, 'list', 'c', $tables);
    push @filters, PB2::OccurrenceData::generateOccFilters($self, $tables);
    push @filters, PB2::CommonData::generate_crmod_filters($self, 'o', $tables);
    push @filters, PB2::CommonData::generate_ent_filters($self, 'o', $tables);
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $tables->{ph} = 1;
    $tables->{pl} = 1;
    $tables->{t} = 1;
    $tables->{c} = 1;
    $tables->{im} = 1;
    $tables->{ei} = 1;
    $tables->{li} = 1;
    
    my @fields = ('o.occurrence_no', 't.synonym_no', 't.rank', 'im.cx_int_no as interval_no, o.early_age, o.late_age',
		  'ei.interval_name as early_name', 'li.interval_name as late_name', 'o.genus_name');
    
    if ( $self->clean_param('recent') )
    {
	$tables->{v} = 1;
	push @fields, 'v.is_extant';
	$options->{use_recent} = 1;
    }
    
    my $count_what = $self->clean_param('count') || 'genera';
    $count_what = 'genera_plus' if $self->clean_param('subgenera');
    
    my $count_rank;
    
    if ( $count_what eq 'species' )
    {
	push @fields, 't.synonym_no as taxon1';
	$options->{count_rank} = 3;
    }
    
    elsif ( $count_what eq 'genera' )
    {
	push @fields, 'pl.genus_no as taxon1';
	$options->{count_rank} = 5;
    }
    
    elsif ( $count_what eq 'genera_plus' )
    {
	push @fields, 'if(pl.subgenus_no, pl.subgenus_no, pl.genus_no) as taxon1';
	$options->{count_rank} = 5;
    }
    
    elsif ( $count_what eq 'families' )
    {
	push @fields, 'ph.family_no as taxon1';
	$options->{count_rank} = 9;
    }
    
    elsif ( $count_what eq 'orders' )
    {
	push @fields, 'ph.order_no as taxon1';
	$options->{count_rank} = 13;
    }
    
    else
    {
	die "400 unknown value '$count_what' for parameter 'count'\n";
    }
    
    my $fields = join(', ', @fields);
    
    # Determine which extra tables, if any, must be joined to the query.  Then
    # construct the query.
    
    my $join_list = $self->generateJoinList('c', $tables);
    
    my $extra_group = $tables->{group_by_reid} ? ', o.reid_no' : '';
    
    $self->{main_sql} = "
	SELECT $fields
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c on o.collection_no = c.collection_no
		$join_list
        WHERE $filter_string
	GROUP BY o.occurrence_no $extra_group";
    
    print STDERR "$self->{main_sql}\n\n" if $self->debug;
    
    # Then prepare and execute the main query.
    
    my $sth = $dbh->prepare($self->{main_sql});
    $sth->execute();
    
    # Now fetch all of the rows, and process them into a diversity matrix.
    
    my $result = $self->generate_diversity_matrix($sth, $options);
    
    $self->list_result($result);
}


# phylogeny ( )
# 
# Like 'list', but processes the resulting list of occurrences into a
# phylogenetic tree.

sub phylogeny {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_connection;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $tables = $self->tables_hash;
    
    $tables->{ph} = 1;
    $tables->{t} = 1;
    #$tables->{ts} = 1;
    $tables->{pl} = 1;
    
    # Set up the phylogeny-computation options
    
    my $options = {};
    
    my $full = $self->clean_param('full');
    my $resolution = $self->clean_param('reso') || 'families';
    my $count_what = $self->clean_param('count') || 'genera';
    
    my $get_rank;
    
    # Determine the necessary set of query fields.
    
    my @fields = ('o.occurrence_no', 't.rank', 't.ints_no', 'ph.class', 'ph.class_no', 'ph.order', 'ph.order_no');
    
    # Add the fields necessary for counting down to the specified rank
    
    if ( $count_what eq 'species' )
    {
	push @fields, 'ph.family_no', 'pl.genus_no', 'pl.subgenus_no', 'pl.species_no';
	$options->{count_rank} = 3;
    }
    
    elsif ( $count_what eq 'genera' || $count_what eq 'genus' )
    {
	push @fields, 'ph.family_no', 'pl.genus_no';
	$options->{count_rank} = 5;
    }
    
    elsif ( $count_what eq 'genera_plus' || $count_what eq 'genus_plus' )
    {
	push @fields, 'ph.family_no', 'pl.genus_no', 'pl.subgenus_no';
	$options->{count_rank} = 5;
	$options->{promote_subgenera} = 1;
    }
    
    elsif ( $count_what eq 'families' || $count_what eq 'family' )
    {
	push @fields, 'ph.family_no';
	$options->{count_rank} = 9;
    }
    
    elsif ( $count_what eq 'orders' || $count_what eq 'order' )
    {
	$options->{count_rank} = 13;
    }
    
    else
    {
	die "400 unknown value '$count_what' for parameter 'count'\n";
    }
    
    # Add the fields necessary for resolving the phylogeny down to the
    # specified rank
    
    if ( $resolution eq 'species' )
    {
	push @fields, 'ph.family', 'ph.genus', 'ph.subgenus', 'ph.species' unless $full;
	$options->{reso_rank} = 3;
    }
    
    elsif ( $resolution eq 'subgenera' || $resolution eq 'subgenus' )
    {
	push @fields, 'ph.family', 'pl.genus', 'pl.subgenus' unless $full;
	$options->{reso_rank} = 4;
    }
    
    elsif ( $resolution eq 'genera' || $resolution eq 'genus' )
    {
	push @fields, 'ph.family', 'pl.genus' unless $full;
	$options->{reso_rank} = 5;
    }
    
    elsif ( $resolution eq 'families' || $resolution eq 'family' )
    {
	push @fields, 'ph.family' unless $full;
	$options->{reso_rank} = 9;
    }
    
    elsif ( $resolution eq 'orders' || $resolution eq 'order' )
    {
	$options->{reso_rank} = 13;
    }
    
    else
    {
	die "400 unknown value '$resolution' for parameter 'reso'\n";
    }
    
    my $fields = join(', ', @fields);
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = PB2::CollectionData::generateMainFilters($self, 'list', 'c', $tables);
    push @filters, PB2::OccurrenceData::generateOccFilters($self, $tables);
    push @filters, PB2::CommonData::generate_crmod_filters($self, 'o', $tables);
    push @filters, PB2::CommonData::generate_ent_filters($self, 'o', $tables);
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # Determine which extra tables, if any, must be joined to the query.  Then
    # construct the query.
    
    my $join_list = $self->generateJoinList('c', $tables);
    
    my $extra_group = $tables->{group_by_reid} ? ', o.reid_no' : '';
    
    $self->{main_sql} = "
	SELECT $fields
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c on o.collection_no = c.collection_no
		$join_list
        WHERE $filter_string
	GROUP BY o.occurrence_no $extra_group";
    
    print STDERR "$self->{main_sql}\n\n" if $self->debug;
    
    # Then prepare and execute the main query.
    
    my $sth = $dbh->prepare($self->{main_sql});
    $sth->execute();
    
    # Now fetch all of the rows, and process them into a phylogenetic tree.
    
    my $result = $self->generate_phylogeny($sth, $options);
    
    $self->list_result($result);
}


# refs ( )
# 
# Query the database for the references associated with occurrences satisfying
# the conditions specified by the parameters.

sub refs {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_connection;
    
    $self->substitute_select( mt => 'r', cd => 'r' );
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my $inner_tables = {};
    
    my @filters = PB2::CollectionData::generateMainFilters($self, 'list', 'c', $inner_tables);
    push @filters, PB2::CommonData::generate_crmod_filters($self, 'o');
    push @filters, PB2::CommonData::generate_ent_filters($self, 'o');
    push @filters, $self->generateOccFilters($inner_tables);
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # Construct another set of filter expressions to act on the references.
    
    my @ref_filters = PB2::ReferenceData::generate_filters($self, $self->tables_hash);
    push @ref_filters, "1=1" unless @ref_filters;
    
    my $ref_filter_string = join(' and ', @ref_filters);
    
    # Figure out the order in which we should return the references.  If none
    # is selected by the options, sort by rank descending.
    
    my $order = $self->PB2::ReferenceData::generate_order_clause({ rank_table => 's' }) ||
	"r.author1last, r.author1init";
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $self->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $self->select_string;
    
    $self->adjustCoordinates(\$fields);
    
    my $inner_join_list = $self->generateJoinList('c', $inner_tables);
    my $outer_join_list = $self->PB2::ReferenceData::generate_join_list($self->tables_hash);
    
    $self->{main_sql} = "
	SELECT $calc $fields, count(distinct occurrence_no) as reference_rank, 1 as is_occ
	FROM (SELECT o.reference_no, o.occurrence_no
	    FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c using (collection_no)
		$inner_join_list
            WHERE $filter_string) as s STRAIGHT_JOIN refs as r on r.reference_no = s.reference_no
	$outer_join_list
	WHERE $ref_filter_string
	GROUP BY r.reference_no ORDER BY $order
	$limit";
    
    print STDERR "$self->{main_sql}\n\n" if $self->debug;
    
    # Then prepare and execute the main query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $self->sql_count_rows;
}


# taxa ( )
# 
# Query the database for the taxa associated with occurrences satisfying
# the conditions specified by the parameters.

sub taxa {

    my ($self) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $self->get_connection;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my $inner_tables = { t => 1 };
    my $outer_tables = { at => 1 };
    
    my @filters = PB2::CollectionData::generateMainFilters($self, 'list', 'c', $inner_tables);
    push @filters, PB2::CommonData::generate_crmod_filters($self, 'o');
    push @filters, PB2::CommonData::generate_ent_filters($self, 'o');
    push @filters, $self->generateOccFilters($inner_tables);
    
    push @filters, "c.access_level = 0";
    
    my $filter_string = join(' and ', @filters);
    
    # Construct another set of filter expressions to act on the taxa.
    
    my @taxa_filters = $self->PB2::TaxonData::generate_filters($outer_tables);
    push @taxa_filters, "1=1" unless @taxa_filters;
    
    my $taxa_filter_string = join(' and ', @taxa_filters);
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $self->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $self->sql_count_clause;
    
    # Figure out the order in which we should return the taxa.  If none
    # is selected by the options, sort by the tree sequence number.
    
    my $order_expr = $self->PB2::TaxonData::generate_order_clause($outer_tables, {rank_table => 's'});
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $TREE_TABLE = 'taxon_trees';
    my $INTS_TABLE = $TAXON_TABLE{$TREE_TABLE}{ints};
    my $AUTH_TABLE = $TAXON_TABLE{$TREE_TABLE}{authorities};
    
    my $fields = $self->PB2::TaxonData::generate_query_fields($outer_tables, 1);
    
    my $inner_join_list = $self->generateJoinList('c', $inner_tables);
    my $outer_join_list = $self->PB2::TaxonData::generate_join_list($outer_tables, $TREE_TABLE);
    
    # Depending upon the summary level, we need to use different templates to generate the query.
    
    my $summary_rank = $self->clean_param('rank');
    my $summary_expr = $self->PB2::TaxonData::generate_summary_expr($summary_rank, 'o', 't', 'i');
    
    if ( $summary_rank eq 'exact' or $summary_rank eq 'ident' )
    {
	$order_expr ||= "s.taxon_name";
	$fields =~ s{t\.name}{s.taxon_name};
	
	$self->{main_sql} = "
	SELECT $calc $fields, count(distinct s.occurrence_no) as associated_records
	FROM (SELECT $summary_expr as taxon_name, o.orig_no, o.occurrence_no
	    FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c using (collection_no)
		$inner_join_list
            WHERE $filter_string) as s LEFT JOIN $TREE_TABLE as t2 on t2.orig_no = s.orig_no
		LEFT JOIN $TREE_TABLE as t on t.orig_no = t2.synonym_no
		LEFT JOIN $AUTH_TABLE as a on a.taxon_no = t.spelling_no
		$outer_join_list
	WHERE $taxa_filter_string
	GROUP BY s.taxon_name ORDER BY $order_expr
	$limit";
    }
    
    elsif ( $summary_rank eq 'taxon' or $summary_rank eq 'synonym' )
    {
	$order_expr ||= "t.name";
	
	$self->{main_sql} = "
	SELECT $calc a.taxon_name as exact_name, $fields, count(distinct s.occurrence_no) as associated_records
	FROM (SELECT $summary_expr as orig_no, o.occurrence_no
	    FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c using (collection_no)
		$inner_join_list
            WHERE $filter_string) as s STRAIGHT_JOIN $TREE_TABLE as t on t.orig_no = s.orig_no
		LEFT JOIN $AUTH_TABLE as a on a.taxon_no = t.spelling_no
		$outer_join_list
	WHERE $taxa_filter_string
	GROUP BY t.orig_no ORDER BY $order_expr
	$limit";
    }
    
    else
    {
	$order_expr ||= "t.name";
	
	$self->{main_sql} = "
	SELECT $calc $fields, count(distinct s.occurrence_no) as associated_records
	FROM (SELECT $summary_expr as orig_no, o.occurrence_no
	    FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c using (collection_no)
		$inner_join_list
		LEFT JOIN $INTS_TABLE as i using (ints_no)
            WHERE $filter_string) as s LEFT JOIN $TREE_TABLE as t using (orig_no)
		LEFT JOIN $AUTH_TABLE as a on a.taxon_no = t.spelling_no
		$outer_join_list
	WHERE $taxa_filter_string
	GROUP BY t.orig_no ORDER BY $order_expr
	$limit";
    }
    
    print STDERR "$self->{main_sql}\n\n" if $self->debug;
    
    # Then prepare and execute the main query.
    
    $self->{main_sth} = $dbh->prepare($self->{main_sql});
    $self->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $self->sql_count_rows;
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
    
    if ( ref $self->{clean_params}{id} eq 'ARRAY' and
	 @{$self->{clean_params}{id}} )
    {
	my $id_list = join(',', @{$self->{clean_params}{id}});
	push @filters, "o.occurrence_no in ($id_list)";
    }
    
    elsif ( $self->{clean_params}{id} )
    {
	push @filters, "o.occurrence_no = $self->{clean_params}{id}";
    }
    
    # Check for parameter 'coll_id'
    
    if ( ref $self->{clean_params}{coll_id} eq 'ARRAY' and
	 @{$self->{clean_params}{coll_id}} )
    {
	my $id_list = join(',', @{$self->{clean_params}{coll_id}});
	push @filters, "o.collection_no in ($id_list)";
    }
    
    elsif ( $self->{clean_params}{coll_id} )
    {
	push @filters, "o.collection_no = $self->{clean_params}{coll_id}";
    }
    
    # Check for parameter 'ident'.  In cases of reidentified occurrences, it
    # specifies which identifications should be returned.  The default is
    # 'latest'.
    
    my $ident = $self->clean_param('ident');
    
    if ( $ident eq 'orig' )
    {
	push @filters, "o.reid_no = 0";
    }
    
    elsif ( $ident eq 'all' )
    {
	$tables_ref->{group_by_reid} = 1;
    }
    
    else # default: 'latest'
    {
	push @filters, "o.latest_ident = true";
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
    
    $join_list .= "JOIN collections as cc on c.collection_no = cc.collection_no\n"
	if $tables->{cc};
    $join_list .= "JOIN occurrences as oc on o.occurrence_no = oc.occurrence_no\n"
	if $tables->{oc};
    $join_list .= "LEFT JOIN taxon_trees as t on t.orig_no = o.orig_no\n"
	if $tables->{t} || $tables->{tf};
    $join_list .= "LEFT JOIN taxon_trees as ts on ts.orig_no = t.synonym_no\n"
	if $tables->{ts};
    $join_list .= "LEFT JOIN taxon_lower as pl on pl.orig_no = t.orig_no\n"
	if $tables->{pl};
    $join_list .= "LEFT JOIN taxon_ints as ph on ph.ints_no = t.ints_no\n"
	if $tables->{ph};
    $join_list .= "LEFT JOIN taxon_attrs as v on v.orig_no = t.synonym_no\n"
	if $tables->{v};
    $join_list .= "LEFT JOIN $PALEOCOORDS as pc on pc.collection_no = c.collection_no\n"
	if $tables->{pc};
    $join_list .= "LEFT JOIN $GEOPLATES as gp on gp.plate_no = pc.mid_plate_id\n"
	if $tables->{gp};
    $join_list .= "LEFT JOIN refs as r on r.reference_no = o.reference_no\n" 
	if $tables->{r};
    $join_list .= "LEFT JOIN person as ppa on ppa.person_no = c.authorizer_no\n"
	if $tables->{ppa};
    $join_list .= "LEFT JOIN person as ppe on ppe.person_no = c.enterer_no\n"
	if $tables->{ppe};
    $join_list .= "LEFT JOIN person as ppm on ppm.person_no = c.modifier_no\n"
	if $tables->{ppm};
    $join_list .= "LEFT JOIN $INTERVAL_MAP as im on im.early_age = $mt.early_age and im.late_age = $mt.late_age and scale_no = 1\n"
	if $tables->{im};
    
    $join_list .= "LEFT JOIN $INTERVAL_DATA as ei on ei.interval_no = c.early_int_no\n"
	if $tables->{ei};
    $join_list .= "LEFT JOIN $INTERVAL_DATA as li on li.interval_no = c.late_int_no\n"
	if $tables->{li};
    $join_list .= "LEFT JOIN $COUNTRY_MAP as ccmap on ccmap.cc = c.cc"
	if $tables->{ccmap};
    
    return $join_list;
}


# process_basic_record ( )
# 
# If the taxonomic name stored in the occurrence record is not linked in to
# the taxonomic hierarchy, construct it using the genus_name, genus_reso,
# species_name and species_reso fields.  Also figure out the taxonomic rank if
# possible.

sub process_basic_record {
    
    my ($request, $record) = @_;
    
    # If the taxonomic name field is empty, try to construct one.  Build this
    # in two versions: with and without the 'reso' fields.
    
    unless ( $record->{taxon_name} )
    {
	my $taxon_name = $record->{genus_name} || 'UNKNOWN';
	$taxon_name .= " $record->{genus_reso}" if $record->{genus_reso};
	my $taxon_base = $record->{genus_name} || 'UNKNOWN';
	
	if ( $record->{subgenus_name} )
	{
	    $taxon_name .= " ($record->{subgenus_name}";
	    $taxon_name .= " $record->{subgenus_reso}" if $record->{subgenus_reso};
	    $taxon_name .= ")";
	    $taxon_base .= " ($record->{subgenus_name})";
	}
	
	if ( $record->{species_name} )
	{
	    $taxon_name .= " $record->{species_name}";
	    $taxon_name .= " $record->{species_reso}" if $record->{species_reso};
	    $taxon_base .= " $record->{species_name}" if $record->{species_name} !~ /\.$/;
	}
	
	$record->{taxon_name} = $taxon_name;
	$record->{taxon_base} = $taxon_base;
	
    }
    
    # If the taxonomic rank field is empty, try to determine one.
    
    unless ( $record->{taxon_rank} )
    {
	if ( defined $record->{species_name} && $record->{species_name} =~ qr{[a-z]$} )
	{
	    $record->{taxon_rank} = 3;
	}
	
	elsif ( defined $record->{subgenus_name} && $record->{subgenus_name} =~ qr{[a-z]$} )
	{
	    $record->{taxon_rank} = 4;
	}
	
	elsif ( defined $record->{genus_name} && defined $record->{matched_name} && 
		$record->{genus_name} eq $record->{matched_name} )
	{
	    $record->{taxon_rank} = $record->{matched_rank};
	}
    }
}


sub generate_diversity_matrix {

    my ($self, $sth, $options) = @_;
    
    my $ds = $self->ds;
    
    # First figure out which timescale (and thus which list of intervals) we
    # will be using in order to bin the occurrences.
    
    my $scale_no = $options->{scale_no} || 1;	# eventually we will add other scale options
    my $scale_level = $options->{timereso} || 5;
    
    my $debug_mode = $self->debug;
    
    # Figure out the parameters to use in the binning process.
    
    my $timerule = $options->{timerule};
    my $timebuffer = $options->{timebuffer};
    
    # Declare variables to be used in this process.
    
    my $intervals = $PB2::IntervalData::INTERVAL_DATA{$scale_no};
    my $boundary_list = $PB2::IntervalData::BOUNDARY_LIST{$scale_no}{$scale_level};
    my $boundary_map = $PB2::IntervalData::BOUNDARY_MAP{$scale_no}{$scale_level};
    
    my ($starting_age, $ending_age, %taxon_first, %taxon_last, %occurrences, %unique_in_bin);
    my ($total_count, $imprecise_time_count, $imprecise_taxon_count, $missing_taxon_count, $bin_count);
    my (%imprecise_interval, %imprecise_taxon);
    my (%interval_report, %taxon_report);
    
    # Now scan through the occurrences.  We cache the lists of matching
    # intervals from the selected scale, under the name of the interval(s)
    # recorded for the occurrence (which may or may not be in the standard
    # timescale).
    
    my (%interval_cache);
    
 OCCURRENCE:
    while ( my $r = $sth->fetchrow_hashref )
    {
	$total_count++;
	
	# Start by figuring out the interval(s) in which to bin this
	# occurrence.  Depending upon the value of $timerule, there may be
	# more than one.
	
	# The first step is to compute the key under which to cache lists of
	# matching intervals.
	
	my $interval_key = $r->{early_name} || 'UNKNOWN';
	$interval_key .= '-' . $r->{late_name}
	    if defined $r->{late_name} && defined $r->{early_name} && $r->{late_name} ne $r->{early_name};
	
	# If we have already figured out which intervals match this, we're
	# done.  Otherwise, we must do this computation.
	
	my $bins = $interval_cache{$interval_key};
	
	my $occ_early = $r->{early_age} + 0;
	my $occ_late = $r->{late_age} + 0;
	
	unless ( $bins )
	{
	    $bins = $interval_cache{$interval_key} = [];
	    
	    # Scan the entire list of intervals for the selected timescale,
	    # looking for those that match according to the value of
	    # $timerule.
	    
	INTERVAL:
	    foreach my $early_bound ( @$boundary_list )
	    {
		# Skip all intervals that do not overlap with the occurrence
		# range, and stop the scan when we have passed that range.
		
		last INTERVAL if $early_bound <= $occ_late;
		
		my $int = $boundary_map->{$early_bound};
		my $late_bound = $int->{late_age};
		
		next INTERVAL if $late_bound >= $occ_early;
		
		# Skip any interval that is not selected by the specified
		# timerule.  Note that the 'overlap' timerule includes
		# everything that overlaps.
		
		if ( $timerule eq 'contain' )
		{
		    last INTERVAL if $occ_early > $early_bound || $occ_late < $late_bound;
		}
		
		elsif ( $timerule eq 'major' )
		{
		    my $overlap;
		    
		    if ( $occ_late >= $late_bound )
		    {
			if ( $occ_early <= $early_bound )
			{
			    $overlap = $occ_early - $occ_late;
			}
			
			else
			{
			    $overlap = $early_bound - $occ_late;
			}
		    }
		    
		    elsif ( $occ_early > $early_bound )
		    {
			$overlap = $early_bound - $late_bound;
		    }
		    
		    else
		    {
			$overlap = $occ_early - $late_bound;
		    }
		    
		    next INTERVAL if $occ_early != $occ_late && $overlap / ($occ_early - $occ_late) < 0.5;
		}
		
		elsif ( $timerule eq 'buffer' )
		{
		    my $buffer = $timebuffer || ($early_bound > 66 ? 12 : 5);
		    
		    next INTERVAL if $occ_early > $early_bound + $buffer || 
			$occ_late < $late_bound - $buffer;
		}
		
		# If we are not skipping this interval, add it to the list.
		
		push @$bins, $early_bound;
		
		# If we are using timerule 'major' or 'contains', then stop
		# the scan because each occurrence gets assigned to only one
		# bin. 
		
		last INTERVAL if $timerule eq 'contains' || $timerule eq 'major';
	    }
	}
	
	# If we did not find at least one bin to assign this occurrence to,
	# report that fact and go on to the next occurrence.
	
	unless ( @$bins )
	{
	    $imprecise_time_count++;
	    $imprecise_interval{$interval_key}++;
	    if ( $debug_mode )
	    {
		$interval_key .= " [$occ_early - $occ_late]";
		$interval_report{'0 IMPRECISE <= ' . $interval_key}++;
	    }
	    next OCCURRENCE;
	}

	# Otherwise, count this occurrence in each selected bin.  Then adjust
	# the range of bins that we are reporting to reflect this occurrence.
	
	foreach my $b ( @$bins )
	{
	    $occurrences{$b}++;
	    $bin_count++;
	}
	
	$starting_age = $bins->[0] unless defined $starting_age && $starting_age >= $bins->[0];
	$ending_age = $bins->[-1] unless defined $ending_age && $ending_age <= $bins->[-1];
	
	# If we are in debug mode, also count it in the %interval_report hash.
	
	if ( $debug_mode )
	{
	    my $report_key = join(',', @$bins) . ' <= ' . $interval_key . " [$occ_early - $occ_late]";
	    $interval_report{$report_key}++;
	}
	
	# Now check to see if the occurrence is taxonomically identified
	# precisely enough to count further.
	
	my $taxon_no = $r->{taxon1};
	
	unless ( $taxon_no )
	{
	    $taxon_report{$r->{genus_name}}++;
	    
	    if ( $r->{rank} > $options->{count_rank} )
	    {
		$imprecise_taxon_count++;
	    }
	    else
	    {
		$missing_taxon_count++;
	    }
	    
	    next;
	}
	
	# If this is the oldest occurrence of the taxon that we have found so
	# far, mark it as originating in the first (oldest) matching bin.
	
	unless ( defined $taxon_first{$taxon_no} && $taxon_first{$taxon_no} >= $bins->[0] )
	{
	    $taxon_first{$taxon_no} = $bins->[0];
	}
	
	# If this is the youngest occurrence of the taxon that we have found
	# so far, mark it as ending in the last (youngest) matching bin.
	
	unless ( defined $taxon_last{$taxon_no} && $taxon_last{$taxon_no} <= $bins->[-1] )
	{
	    $taxon_last{$taxon_no} = $bins->[-1];
	}
	
	# If the 'use_recent' option was given, and the taxon is known to be
	# extant, then mark it as ending at the present (0 Ma).
	
	if ( $options->{use_recent} && $r->{is_extant} )
	{
	    $taxon_last{$taxon_no} = 0;
	}
	
	# Now count the taxon in each selected bin.
	
	foreach my $b ( @$bins )
	{
	    $unique_in_bin{$b}{$taxon_no} ||= 1;
	}
    }
    
    # At this point we are done scanning the occurrence list.  Unless
    # $starting_age has a value, we don't have any results.
    
    unless ( $starting_age )
    {
	return;
    }
    
    # Now we need to compute the four diversity statistics defined by Foote:
    # XFt, XFL, XbL, Xbt.  So we start by running through the bins and
    # initializing the counts.  We also keep track of all the bins between
    # $starting_age and $ending_age.
    
    my (%X_Ft, %X_FL, %X_bL, %X_bt);
    my (@bins, $is_last);
    
    foreach my $age ( @$boundary_list )
    {
	next if $age > $starting_age;
	last if $age < $ending_age;
	
	push @bins, $age;
	
	$X_Ft{$age} = 0;
	$X_FL{$age} = 0;
	$X_bL{$age} = 0;
	$X_bt{$age} = 0;
    }
    
    # Then we scan through the taxa.  For each one, we scan through the bins
    # from the taxon's origination to its ending and mark the appropriate
    # counts.  This step takes time o(MN) where M is the number of taxa and N
    # the number of intervals.
    
    foreach my $taxon_no ( keys %taxon_first )
    {
	my $first_bin = $taxon_first{$taxon_no};
	my $last_bin = $taxon_last{$taxon_no};
	
	# If the interval of first appearance is the same as the interval of
	# last appearance, then this is a singleton.
	
	if ( $first_bin == $last_bin )
	{
	    $X_FL{$first_bin}++;
	    next;
	}
	
	# Otherwise, we mark the bin where the taxon starts and the bin where
	# it ends, and then scan through the bins between to mark
	# rangethroughs.
	
	$X_Ft{$first_bin}++;
	$X_bL{$last_bin}++;
	
	foreach my $bin (@bins)
	{
	    last if $bin <= $last_bin;
	    $X_bt{$bin}++ if $bin < $first_bin;
	}
    }
    
    # If we are in debug mode, report the interval assignments.
    
    if ( $self->debug ) 
    {
	# $self->add_warning("Skipped $imprecise_time_count occurrences because of imprecise temporal locality:")
	#     if $imprecise_time_count;
	
	# foreach my $key ( sort { $b cmp $a } keys %interval_report )
	# {
	#     $self->add_warning("    $key ($interval_report{$key})");
	# }
	
	foreach my $key ( sort { $a cmp $b } keys %taxon_report )
	{
	    $self->add_warning("    $key ($taxon_report{$key})");
	}
    }
    
    # Add a summary record with counts.
    
    $self->summary_data({ total_count => $total_count,
			  bin_count => $bin_count,
			  imprecise_time => $imprecise_time_count,
			  imprecise_taxon => $imprecise_taxon_count,
			  missing_taxon => $missing_taxon_count });
    
    # Now we scan through the bins again and prepare the data records.
    
    my @result;
    
    foreach my $age (@bins)
    {
	my $r = { interval_no => $boundary_map->{$age}{interval_no},
		  interval_name => $boundary_map->{$age}{interval_name},
		  early_age => $age,
		  late_age => $boundary_map->{$age}{late_age},
		  originations => $X_Ft{$age},
		  extinctions => $X_bL{$age},
		  singletons => $X_FL{$age},
		  range_throughs => $X_bt{$age},
		  sampled_in_bin => scalar(keys %{$unique_in_bin{$age}}) || 0,
		  n_occs => $occurrences{$age} || 0 };
	
	push @result, $r;
    }
    
    $self->list_result(@result);
}


# The following variables are visible to all of the subroutines in the
# remainder of this file.  This is done to reduce the number of parameters
# that must be passed to &count_subtaxa and &add_result_record.

our ($reso_rank, $count_rank, %taxon_name, %uns_count, @result);



sub generate_phylogeny {

    my ($self, $sth, $options) = @_;
    
    my $ds = $self->ds;
    
    # First figure out the level to which we will be resolving the phylogeny.
    
    $reso_rank = $options->{reso_rank} || 9;		# visible to called subroutines
    $count_rank = $options->{count_rank} || 5;		# visible to called subroutines
    my $promote = $options->{promote_subgenera};
    my $full = $options->{full};
    
    # Make sure we aren't trying to resolve more finely than we are counting.
    
    $reso_rank = $count_rank if $reso_rank < $count_rank;
    
    # Delete unnecessary output fields, so they won't appear as empty columns
    # in text-format output.
    
    $self->delete_output_field('n_species') if $count_rank > 3;
    $self->delete_output_field('n_genera') if $count_rank > 5;
    $self->delete_output_field('n_families') if $count_rank > 9;
    
    # Then go through the occurrences one by one, putting together a tree
    # and counting at the specified taxonomic levels.
    
    my (%occ_tree, $total_count);
    local %taxon_name = ( 0 => '~' );
    
 OCCURRENCE:
    while ( my $r = $sth->fetchrow_hashref )
    {
	$total_count++;
	
	# First pin down the various tree levels.
	
	my $rank = $r->{rank};
	
	my ($species_no, $subgenus_no, $genus_no, $family_no, $order_no, $class_no);
	
	$class_no = $r->{class_no} || 0;
	$order_no = $r->{order_no} || 0;
	$family_no = $r->{family_no} || 0;
	
	$genus_no = $r->{subgenus_no} if $promote;
	$genus_no ||= $r->{genus_no} || 0;
	
	$species_no = $r->{species_no} || 0;
	
	# Then create any tree nodes that don't already exist, and increment
	# the occurrence counts at all levels.
	
	my ($class_node, $order_node, $family_node, $genus_node, $species_node);
	
	$class_node = $occ_tree{$class_no} //= { rank => 17, occs => 0 };
	$class_node->{occs}++;
	$taxon_name{$class_no} //= $r->{class} if !$full && $r->{class};
	
	$order_node = $class_node->{chld}{$order_no} //=  { rank => 13, occs => 0 };
	$order_node->{occs}++;
	$taxon_name{$order_no} //= $r->{order} if !$full && $r->{order};
	
	if ( $count_rank <= 9 && $rank <= 9 )
	{
	    $family_node = $order_node->{chld}{$family_no} //= { rank => 9, occs => 0 };
	    $family_node->{occs}++;
	    $taxon_name{$family_no} //= $r->{family} if !$full && $r->{family};
	    
	    if ( $count_rank <= 5 && $rank <= 5 )
	    {
		$genus_node = $family_node->{chld}{$genus_no} //= { rank => 5, occs => 0 };
		$genus_node->{occs}++;
		$taxon_name{$genus_no} //= $r->{genus} if !$full && $r->{genus};
		$taxon_name{$subgenus_no} //= $r->{subgenus} if !$full && $r->{subgenus};
		
		if ( $count_rank <= 3 && $rank <= 3 )
		{
		    $species_node = $genus_node->{chld}{$species_no} //= { rank => 3, occs => 0 };
		    $species_node->{occs}++ if $count_rank <= 3;
		    $taxon_name{$species_no} //= $r->{species} if !$full && $r->{species};
		}
	    }
	}
    }
    
    # Now that we have the occurrence counts, recursively traverse the tree
    # and fill in taxon counts at the higher levels (i.e. number of species or
    # genera).
    
    foreach my $class_no ( keys %occ_tree )
    {
	count_subtaxa($occ_tree{$class_no});
    }
    
    # Now traverse the tree again and produce the appropriate output.
    
    local @result;					# visible to called subroutines
    local %uns_count;					# visible to called subroutines
    
    my (@sorted_classes) = sort { $taxon_name{$a} cmp $taxon_name{$b} } keys %occ_tree;
    
    foreach my $class_no ( @sorted_classes )
    {
	add_result_record($occ_tree{$class_no}, $class_no, 0);
    }
    
    $self->list_result(@result);
}


# count_subtaxa ( node )
# 
# This function recursively counts taxa in all of the subnodes of the given
# node, and adds up the totals.  Note: the variable $count_rank is local to
# &generate_phylogeny, from which this routine is called.

sub count_subtaxa {
    
    my ($node) = @_;
    
    $node->{orders} = 0;
    $node->{families} = 0 if $count_rank <= 9;
    $node->{genera} = 0 if $count_rank <= 5;
    $node->{species} = 0 if $count_rank <= 3;
    
    return unless ref $node->{chld};
    
    foreach my $child ( values %{$node->{chld}} )
    {
	count_subtaxa($child, $count_rank);
	
	my $child_orders = $child->{rank} == 13    ? 1
		         : $child->{orders}        ? $child->{orders}
					           : 0;
	
	$node->{orders} += $child_orders if $node->{rank} > 13;
	
	if ( $count_rank <= 9 )
	{
	    my $child_families = $child->{rank} == 9    ? 1
			       : $child->{families}     ? $child->{families}
						        : 0;
	    
	    $node->{families} += $child_families if $node->{rank} > 9;
	}
	
	if ( $count_rank <= 5 )
	{
	    my $child_genera = $child->{rank} == 5    ? 1
			     : $child->{genera}       ? $child->{genera}
						      : 0;
	    
	    $node->{genera} += $child_genera if $node->{rank} > 5;
	}
	
	if ( $count_rank <= 3 )
	{
	    my $child_species = $child->{rank} == 3    ? 1
			      : $child->{species}      ? $child->{species}
						       : 0;
	    
	    $node->{species} += $child_species if $node->{rank} > 3;
	}
    }
}


my %uns_name = ( 3 => 'NO_SPECIES_SPECIFIED', 5 => 'NO_GENUS_SPECIFIED',
		 9 => 'NO_FAMILY_SPECIFIED', 13 => 'NO_ORDER_SPECIFIED',
		 0 => 'NO_TAXON_SPECIFIED' );

my %uns_prefix = ( 3 => 'UF', 5 => 'UG', 9 => 'UF', 13 => 'UO', 0 => 'UU' );

sub add_result_record {
    
    my ($node, $taxon_no, $parent_no) = @_;
    
    my $rank = $node->{rank};
    
    return if $rank < $reso_rank;
    
    my $name = $taxon_name{$taxon_no} || '~';
    $name = $uns_name{$rank || 0} if $name eq '~';
    
    unless ( $taxon_no )
    {
	$uns_count{$rank}++;
	$taxon_no = $uns_prefix{$rank} . $uns_count{$rank};
    }
    
    my $taxon_record = { taxon_no => $taxon_no,
			 parent_no => $parent_no,
			 taxon_name => $name,
			 taxon_rank => $rank,
		         n_occs => $node->{occs} };
    
    $taxon_record->{n_orders} = $node->{orders} if defined $node->{orders} && $rank > 13;
    $taxon_record->{n_families} = $node->{families} if defined $node->{families} && $rank > 9;
    $taxon_record->{n_genera} = $node->{genera} if defined $node->{genera} && $rank > 5;
    $taxon_record->{n_species} = $node->{species} if defined $node->{species} && $rank > 3;
    
    push @result, $taxon_record;
    
    return if $rank == $reso_rank;
    
    my @children = keys %{$node->{chld}};
    
    foreach my $child_no ( sort { ($taxon_name{$a} // '~') cmp ($taxon_name{$b} // '~') } @children )
    {
	my $child = $node->{chld}{$child_no};
	next if $child->{rank} < $reso_rank;
	add_result_record($child, $child_no, $taxon_no);
    }
}


sub prune_count_fields {
    
    my ($self, $count_rank) = @_;
    
    my $field_list = $self->output_field_list;
    
    my @good_fields;
    
    foreach my $f ( @$field_list )
    {
	next if $f eq 'n_species' && $count_rank > 3;
	next if $f eq 'n_genera' && $count_rank > 5;
	next if $f eq 'n_families' && $count_rank > 9;
	push @good_fields, $f;
    }
    
    @$field_list = @good_fields;
}

1;
