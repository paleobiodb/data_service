#  
# OccurrenceData
# 
# A role that returns information from the PaleoDB database about a single
# occurence or a category of occurrences.
# 
# Author: Michael McClennen

use strict;

use lib '..';

use POSIX ();

package PB2::OccurrenceData;

use HTTP::Validate qw(:validators);

use TableDefs qw($OCC_MATRIX $COLL_MATRIX $COLL_BINS $PVL_MATRIX $PVL_GLOBAL $BIN_LOC $COUNTRY_MAP $PALEOCOORDS $GEOPLATES
		 $INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP $INTERVAL_BUFFER $DIV_GLOBAL $DIV_MATRIX);

use TaxonDefs qw(%RANK_STRING);

use Carp qw(carp croak);

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::DiversityData PB2::CommonData PB2::ReferenceData PB2::TaxonData PB2::CollectionData PB2::IntervalData);


# initialize ( )
# 
# This routine is called by the DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($self, $ds) = @_;
    
    # We start by defining an output map for this class.
    
    $ds->define_output_map('1.2:occs:basic_map' =>
        # { value => 'attr', maps_to => '1.2:colls:attr' },
	#     "The attribution of the occurrence: the author name(s) from",
	#     "the primary reference, and the year of publication.  If no reference",
	#     "is recorded for this occurrence, the reference for its collection is used.",
	{ value => 'ident', maps_to => '1.2:occs:ident' },
	    "The individual components of the taxonomic identification of the occurrence.",
	    "These values correspond to the value of C<identified_name> in the basic record,",
	    "and so this additional section will rarely be needed.",
	{ value => 'taxo', maps_to => '1.2:occs:taxo' },
	    "Additional information about the taxonomic classification of the occurence",
	{ value => 'phylo', maps_to => '1.2:occs:taxo', undocumented => 1 },
	{ value => 'genus', maps_to => '1.2:occs:genus' },
	    "The genus (if known) and subgenus (if any) corresponding to each occurrence.",
	    "This is a subset of the information provided by C<phylo>.",
	{ value => 'plant', maps_to => '1.2:occs:plant' },
	    "The plant organ(s), if any, associated with this occurrence.  These fields",
	    "will be empty unless the occurrence is a plant fossil.",
	{ value => 'coords', maps_to => '1.2:occs:geo' },
	     "The latitude and longitude of this occurrence",
        { value => 'loc', maps_to => '1.2:colls:loc' },
	    "Additional information about the geographic locality of the occurrence",
	{ value => 'paleoloc', maps_to => '1.2:colls:paleoloc' },
	    "Information about the paleogeographic locality of the occurrence,",
	    "evaluated according to the model specified by the parameter C<pgm>.",
	{ value => 'prot', maps_to => '1.2:colls:prot' },
	    "Indication of whether the occurrence is located on protected land.",
        { value => 'time', maps_to => '1.2:colls:time' },
	    "This block is obsolete, and is included only for compatibility reasons.",
	    "It does not include any fields in the response.",
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
	{ value => 'resgroup', maps_to => '1.2:colls:group' },
	    "The research group(s), if any, associated with the occurrence's collection.",
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the occurrence record");
    
    # Then define those blocks which are not already defined in
    # CollectionData.pm 
    
    $ds->define_block('1.2:occs:basic' =>
	{ select => ['o.occurrence_no', 'o.reid_no', 'o.latest_ident', 'o.collection_no', 'o.taxon_no as identified_no',
		     't.rank as identified_rank', 't.status as taxonomic_reason', 't.spelling_no',
		     'nm.spelling_reason', 'ns.spelling_reason as accepted_reason',
		     'tv.spelling_no as accepted_no', 'tv.name as accepted_name', 'tv.rank as accepted_rank',
		     'ei.interval_name as early_interval', 'li.interval_name as late_interval',
		     'o.genus_name', 'o.genus_reso', 'o.subgenus_name', 'o.subgenus_reso',
		     'o.species_name', 'o.species_reso',
		     'o.early_age', 'o.late_age', 'o.reference_no'],
	  tables => ['o', 'tv', 'ts', 'nm', 'ei', 'li'] },
	{ set => '*', from => '*', code => \&process_basic_record },
	{ output => 'occurrence_no', dwc_name => 'occurrenceID', com_name => 'oid' },
	    "A positive integer that uniquely identifies the occurrence",
	{ output => 'record_type', value => 'occurrence', com_name => 'typ', com_value => 'occ', 
	  dwc_value => 'Occurrence' },
	    "The type of this object: C<occ> for an occurrence in the compact vocabulary,",
	    "C<occurrence> in the PBDB vocabulary.",
	{ output => 'reid_no', com_name => 'eid', if_field => 'reid_no' },
	    "If this occurrence was reidentified, a positive integer that uniquely identifies the reidentification",
	{ output => 'obsolete', com_name => 'obs', value => 1, not_field => 'latest_ident' },
	    "The value of this field will be true if this occurrence was later reidentified.",
	{ output => 'collection_no', com_name => 'cid', dwc_name => 'CollectionId' },
	    "The identifier of the collection with which this occurrence is associated.",
	{ output => 'identified_name', com_name => 'idn', dwc_name => 'associatedTaxa', dedup => 'accepted_name' },
	    "The taxonomic name by which this occurrence was identified.  This field will",
	    "be omitted for responses in the compact voabulary if it is identical",
	    "to the value of C<accepted_name>.",
	{ output => 'identified_rank', dwc_name => 'taxonRank', com_name => 'idr', dedup => 'accepted_rank' },
	    "The taxonomic rank of the identified name, if this can be determined.  This field will",
	    "be omitted for responses in the compact voabulary if it is identical",
	    "to the value of C<accepted_rank>.",
	{ set => 'identified_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb' },
	{ output => 'identified_no', com_name => 'iid', dedup => 'accepted_no' },
	    "The unique identifier of the identified taxonomic name.  If this is empty, then",
	    "the name was never entered into the taxonomic hierarchy stored in this database and",
	    "we have no further information about the classification of this occurrence.  In some cases,",
	    "the genus has been entered into the taxonomic hierarchy but not the species.  This field will",
	    "be omitted for responses in the compact voabulary if it is identical",
	    "to the value of C<accepted_no>.",
	{ output => 'taxonomic_reason', com_name => 'idu' },
	    "If the identified name is different from the accepted name, this field gives",
	    "the reason why.  This field will be present if, for example, the identified name",
	    "is a junior synonym or nomen dubium, or if the species has been recombined, or",
	    "if the identification is misspelled.",
	{ output => 'accepted_name', com_name => 'tna', if_field => 'identified_no' },
	    "The value of this field will be the accepted taxonomic name corresponding",
	    "to the identified name.",
	{ output => 'accepted_rank', com_name => 'rnk', if_field => 'identified_no' },
	    "The taxonomic rank of the accepted name.  This may be different from the",
	    "identified rank if the identified name is a nomen dubium or otherwise invalid,",
	    "or if the identified name has not been fully entered into the taxonomic hierarchy",
	    "of this database.",
	{ set => 'accepted_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb' },
	{ output => 'accepted_no', com_name => 'tid', if_field => 'identified_no' },
	    "The unique identifier of the accepted taxonomic name in this database.",
	{ set => '*', code => \&PB2::CollectionData::fixTimeOutput },
	{ output => 'early_interval', com_name => 'oei', pbdb_name => 'early_interval' },
	    "The specific geologic time range associated with this collection (not necessarily a",
	    "standard interval), or the interval that begins the range if C<late_interval> is also given",
	{ output => 'late_interval', com_name => 'oli', pbdb_name => 'late_interval', dedup => 'early_interval' },
	    "The interval that ends the specific geologic time range associated with this collection,",
	    "if different from the value of C<early_interval>",
	{ output => 'early_age', com_name => 'eag', pbdb_name => 'max_ma' },
	    "The early bound of the geologic time range associated with this occurrence (in Ma)",
	{ output => 'late_age', com_name => 'lag', pbdb_name => 'min_ma' },
	    "The late bound of the geologic time range associated with this occurrence (in Ma)",
	# { set => 'reference_no', append => 1 },
	{ output => 'reference_no', com_name => 'rid', show_as_list => 1 },
	    "The identifier(s) of the references from which this data was entered");
    
    $ds->define_block('1.2:occs:ident' =>
	{ select => ['o.genus_name', 'o.genus_reso',
		     'o.subgenus_name', 'o.subgenus_reso', 'o.species_name', 'o.species_reso'],
	  tables => 'o' },
	{ output => 'genus_name', com_name => 'idg', pbdb_name => 'primary_name' },
	    "The taxonomic name (less species) by which this occurrence was identified.",
	    "This is often a genus, but may be a higher taxon.",
	{ output => 'genus_reso', com_name => 'rsg', pbdb_name => 'primary_reso' },
	    "The resolution of the primary name, i.e. C<sensu lato> or C<n. gen.>",
	{ output => 'subgenus_name', com_name => 'idf' },
	    "The subgenus name (if any) by which this occurrence was identified",
	{ output => 'subgenus_reso', com_name => 'rsf' },
	    "The resolution of the subgenus name, i.e. C<aff.> or C<n. subgen.>",
	{ output => 'species_name', com_name => 'ids' },
	    "The species name (if any) by which this occurrence was identified",
	{ output => 'species_reso', com_name => 'rss' },
	    "The resolution of the species name, i.e. C<cf.> or C<n. sp.>");
    
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
    
    $ds->define_block('1.2:occs:taxo' =>
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
    
    $ds->define_block('1.2:occs:plant' =>
	{ select => ['o.plant_organ', 'o.plant_organ2'] },
	{ output => 'plant_organ', com_name => 'pl1' },
	    "The plant organ, if any, associated with this occurrence.  This field",
	    "will be empty unless the occurrence is a plant fossil.",
	{ output => 'plant_organ2', com_name => 'pl2' },
	    "An additional plant organ, if any, associated with this occurrence.");
    
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
	{ output => 'early_age', com_name => 'eag', pbdb_name => 'max_ma' },
	    "The beginning age of this interval, in Ma",
	{ output => 'late_age', com_name => 'lag', pbdb_name => 'min_ma' },
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
    
    # The following block specifies the summary output for diversity plots.
    
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
    
    # The following block specifies the output for "quick" diversity plots.
    
    $ds->define_block('1.2:occs:quickdiv' =>
	{ output => 'interval_no', com_name => 'oid' },
	    "The identifier of the time interval represented by this record",
	{ output => 'interval_name', com_name => 'nam' },
	    "The name of the time interval represented by this record",
	{ output => 'early_age', com_name => 'eag', pbdb_name => 'max_ma' },
	    "The beginning age of this interval, in Ma",
	{ output => 'late_age', com_name => 'lag', pbdb_name => 'min_ma' },
	    "The ending age of this interval, in Ma",
	{ output => 'sampled_in_bin', com_name => 'dsb' },
	    "The number of distinct taxa found in this interval.  By default,",
	    "distinct genera are counted.  You can override this using the",
	    "parameter C<count>.",
	{ output => 'n_occs', com_name => 'noc' },
	    "The total number of occurrences that are resolved to this interval");
    
    # The following block specifies the output for taxon records representing
    # occurrence taxonomies.
    
    $ds->define_block('1.2:occs:taxa' =>
	{ output => 'taxon_no', com_name => 'oid' },
	    "The identifier of the taxon represented by this record",
	{ output => 'parent_no', com_name => 'par' },
	    "The identifier of the parent taxon.  You can use this field to assemble",
	    "these records into one or more taxonomic trees.  A value of 0",
	    "indicates a root of one of these trees.  By default, records representing",
	    "classes have a value of 0 in this field.",
	{ output => 'taxon_rank', com_name => 'rnk' },
	    "The rank of the taxon represented by this record",
	{ set => 'taxon_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb' },
	{ output => 'taxon_name', com_name => 'nam' },
	    "The name of the taxon represented by this record",
	{ output => 'attribution', if_block => 'attr', com_name => 'att' },
	    "The attribution (author and year) of this taxonomic name",
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
	    "fossil occurrences being analyzed",
	{ output => 'specific_occs', com_name => 'soc' },
	    "The number of occurrences that are identified to this specific taxon",
	    "in the set of fossil occurrences being analyzed",
	{ output => 'n_occs', com_name => 'noc' },
	    "The total number of occurrences of this taxon or any of its subtaxa in the",
	    "set of fossil occurrences being analyzed");
    
    $ds->define_output_map('1.2:occs:taxa_opt' =>
	{ value => 'attr', maps_to => '1.2:taxa:attr' },
	    "The attribution of each taxon (author and year)",
	{ value => 'occapp', maps_to => '1.2:taxa:occapp' },
	    "The age of first and last appearance of each taxon from the set",
	    "of occurrences being analyzed (not the absolute first and last",
	    "occurrence ages).",
	@PB2::TaxonData::BASIC_MAP);
    
    $ds->define_block('1.2:occs:taxa_summary' =>
	{ output => 'total_count', pbdb_name => 'n_occs', com_name => 'noc' },
	    "The number of occurrences that were scanned in the process of",
	    "computing this taxonomic tree.");
    
    $ds->define_block('1.2:occs:prevalence' =>
	{ output => 'orig_no', com_name => 'oid', pbdb_name => 'taxon_no' },
	    "The identifier of the taxon.",
	{ output => 'name', com_name => 'nam', pbdb_name => 'taxon_name' },
	    "The scientific name of the taxon.",
	{ set => 'rank', if_vocab => 'pbdb,dwc', lookup => \%PB2::TaxonData::RANK_STRING },
	{ output => 'rank', com_name => 'rnk', pbdb_name => 'taxon_rank' },
	    "The rank of the taxon.",
	{ output => 'image_no', com_name => 'img' },
    	    "If this value is non-zero, you can use it to construct image URLs",
	    "using L<taxa/thumb|node:taxa/thumb> and L<taxa/icon|node:taxa/icon>.",
	{ output => 'class_no', com_name => 'cln' },
	    "The class (if any) to which this taxon belongs.  This will let you",
	    "exclude an order from the list if its class has already been listed.",
	{ output => 'phylum_no', com_name => 'phn' },
	    "The phylum (if any) to which this taxon belongs.  This will let you",
	    "exclude a class or order from the list if its phylum has already been listed.",
	{ output => 'n_occs', com_name => 'noc' },
	    "The number of occurrences of this taxon that match the specified",
	    "parameters.  The list is sorted on this field, from highest",
	    "to lowest.");
    
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
	{ value => 'stage' },
	    "Count by stage",
	{ value => 'epoch' },
	    "Count by epoch",
	{ value => 'period' },
	    "Count by period",
	{ value => 'era' },
	    "Count by era");
    
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
	"the exact list of occurrences used to compute this diversity tabulation.  Note, however, that",
	"some occurrences may be skipped when tabulating diversity because they are imprecisely",
	"characterized temporally or taxonomically.",
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:common:select_crmod' },
	{ allow => '1.2:common:select_ent' },
	{ require_any => ['1.2:main_selector',
			  '1.2:common:select_crmod', '1.2:common:select_ent'] },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:quickdiv' =>
	"The following parameters specify what to count and at what temporal resolution:",
	{ allow => '1.2:occs:div_params' }, 
        ">>The following parameters select which occurrences to analyze.",
	"Except as noted below, you may use these in any combination.",
	"All of these parameters can be used with L<occs/list|node:occs/list> as well, to retrieve",
	"the list of occurrences used to compute this diversity tabulation.  Note, however, that",
	"some occurrences may be skipped when tabulating diversity because they are imprecisely",
	"characterized temporally or taxonomically.",
	{ allow => '1.2:main_selector' },
	#{ allow => '1.2:common:select_crmod' },
	#{ allow => '1.2:common:select_ent' },
	#{ require_any => ['1.2:main_selector',
	#		  '1.2:common:select_crmod', '1.2:common:select_ent'] },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");

    
    $ds->define_ruleset('1.2:occs:taxa_params' =>
	{ param => 'count', valid => '1.2:occs:div_count' },
	    "This parameter specifies the taxonomic level at which to count.  If not",
	    "specified, it defaults to C<genera>.  The accepted values are:",
	{ param => 'subgenera', valid => FLAG_VALUE },
	    "You can use this parameter as a shortcut, equivalent to specifying",
	    "C<count=genera_plus>.  Just include its name, no value is needed.",
	{ param => 'full', valid => BOOLEAN_VALUE },
	    "!If you specify this parameter, a complete taxonomic subtree will",
	    "be reported instead of just the classes, orders, families, etc.",
	{ param => 'reso', valid => ANY_VALUE },
	    "This parameter specifies the temporal resolution at which to count.  If not",
	    "specified, it defaults to C<families>.  Accepted values are:");
    
    $ds->define_ruleset('1.2:occs:taxa' =>
	"The following parameters specify what to count and at what taxonomic resolution:",
	{ allow => '1.2:occs:taxa_params' }, 
        ">>The following parameters select which occurrences to analyze.",
	"Except as noted below, you may use these in any combination.",
	"All of these parameters can be used with L<occs/list|node:occs/list> as well, to retrieve",
	"the exact list of occurrences used to compute this phylogeny.",
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:common:select_crmod' },
	{ allow => '1.2:common:select_ent' },
	{ require_any => ['1.2:main_selector',
			  '1.2:common:select_crmod', '1.2:common:select_ent'] },
	{ optional => 'SPECIAL(show)', valid => '1.2:occs:taxa_opt' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:prevalence' =>
	">>You can use the following parameter to select how detailed you wish the result to be:",
	{ optional => 'detail', valid => POS_VALUE },
	    "Accepted values for this parameter are 1, 2, and 3.  Higher numbers differentiate",
	    "the results more finely, displaying lower-level taxa.",
	">>You can use the following parameters to query for summary clusters by",
	"a variety of criteria.  Except as noted below, you may use these in any combination.",
    	{ allow => '1.2:main_selector' },
	{ allow => '1.2:common:select_crmod' },
	{ allow => '1.2:common:select_ent' },
	{ require_any => ['1.2:main_selector',
			  '1.2:common:select_crmod', '1.2:common:select_ent'] },
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
    
    print STDERR "$self->{main_sql}\n\n" if $self->debug;
    
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
    
    #$self->add_output_block('1.2:occs:unknown_taxon') if $tables->{unknown_taxon};
    
    my $fields = $self->select_string;
    
    $self->adjustCoordinates(\$fields);
    $self->selectPaleoModel(\$fields, $self->tables_hash) if $fields =~ /PALEOCOORDS/;
        
    # Determine the order in which the results should be returned.
    
    my $tt = $tables->{tv} ? 'ts' : 't';
    
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
    $tables->{tv} = 1;
    $tables->{c} = 1;
    $tables->{im} = 1;
    $tables->{ei} = 1;
    $tables->{li} = 1;
    
    my @fields = ('o.occurrence_no', 'tv.orig_no', 'tv.rank', 'im.cx_int_no as interval_no, o.early_age, o.late_age',
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
	push @fields, 'tv.orig_no as taxon1';
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
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c using (collection_no)
		$join_list
        WHERE $filter_string
	GROUP BY o.occurrence_no $extra_group";
    
    print STDERR "$self->{main_sql}\n\n" if $self->debug;
    
    # Then prepare and execute the main query.
    
    my $sth = $dbh->prepare($self->{main_sql});
    $sth->execute();
    
    # Now fetch all of the rows, and process them into a diversity matrix.
    
    my $result = $self->generate_diversity_table($sth, $options);
    
    $self->list_result($result);
}


# quickdiv ( )
# 
# Generates a "quick" diversity result, according to the specified set of
# parameters.  The only parameters allowed are geography, time and taxon.

sub quickdiv {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $tables = {};
    
    my $taxonomy = ($request->{my_taxonomy} ||= Taxonomy->new($dbh, 'taxon_trees'));
    my $TREE_TABLE = $taxonomy->{TREE_TABLE};
    my $INTS_TABLE = $taxonomy->{INTS_TABLE};
    
    my ($sql, $result);
    
    # Check to see if we can use the 'div_matrix' table to compute the
    # result. This is possible if the only filters are geographic, temporal,
    # and/or taxonomic.
    
    my @filters = $request->generateQuickDivFilters('d', $tables);
    
    # If an empty list of filters was returned, redirect to the 'diversity'
    # route to use the less efficient but more flexible procedure.
    
    unless ( @filters )
    {
	return $request->diversity();
    }
    
    # Determine the taxonomic level at which we should be counting.  If not
    # specified, or if 'genera_plus' was specified, use 'genera'.  Construct
    # the appropriate SQL statement to generate this count.
    
    my $count_what = $request->clean_param('count') || 'genera';
    my $filter_expr = join(' and ', @filters);
    
    my $scale_id = $request->clean_param('scale_id') || 1;
    my $reso = $request->clean_param('reso');
    
    # If no value was given for 'reso', use the maximum level of the selected scale.
    
    if ( $scale_id == 1 )
    {
	if ( $reso eq 'era' )
	{
	    $reso = 2;
	}
	elsif ( $reso eq 'period' )
	{
	    $reso = 3;
	}
	elsif ( $reso eq 'epoch' )
	{
	    $reso = 4;
	}
	else
	{
	    $reso = 5;
	}
    }
    
    else
    {
	$reso = $PB2::IntervalData::SCALE_DATA{$scale_id}{levels};
    }
    
    # Now check for parameters 'interval', 'interval_id', 'min_ma', 'max_ma'.
    
    my $interval_no = $request->clean_param('interval_id');
    my $interval_name = $request->clean_param('interval');
    my $min_ma = $request->clean_param('min_ma');
    my $max_ma = $request->clean_param('max_ma');
    my $age_clause = ''; my $age_join = '';
    
    if ( $interval_no )
    {
	my $no = $interval_no + 0;
	my $sql = "SELECT early_age, late_age FROM interval_data WHERE interval_no = $no";
	
	my ($max_ma, $min_ma) = $dbh->selectrow_array($sql);
	
	unless ( $max_ma )
	{
	    $max_ma = 0;
	    $min_ma = 0;
	    $request->add_warning("unknown interval id '$interval_no'");
	}
	
	$age_clause = "and i.early_age <= $max_ma and i.late_age >= $min_ma";
	$age_join = "join interval_data as i using (interval_no)";
    }
    
    elsif ( $interval_name )
    {
	my $name = $dbh->quote($interval_name);
	my $sql = "SELECT early_age, late_age FROM interval_data WHERE interval_name like $name";
	
	my ($max_ma, $min_ma) = $dbh->selectrow_array($sql);
	
	unless ( $max_ma )
	{
	    $max_ma = 0;
	    $min_ma = 0;
	    $request->add_warning("unknown interval '$interval_name'");
	}
	
	$age_clause = "and i.early_age <= $max_ma and i.late_age >= $min_ma";
	$age_join = "join interval_data as i using (interval_no)";
    }
    
    elsif ( $min_ma || $max_ma )
    {
	$max_ma += 0;
	$min_ma += 0;
	
	$age_clause .= "and i.early_age <= $max_ma " if $max_ma > 0;
	$age_clause .= "and i.late_age >= $min_ma" if $min_ma > 0;
	$age_join = "join interval_data as i using (interval_no)";
    }
    
    # Now generate the appropriate SQL expression based on what we are trying
    # to count.
    
    my $main_table = $tables->{use_global} ? $DIV_GLOBAL : $DIV_MATRIX;
    my $other_joins = $request->generateQuickDivJoins('d', $tables, $taxonomy);
    
    if ( $count_what eq 'genera' || $count_what eq 'genera_plus' )
    {
	$sql = "SELECT d.interval_no, count(distinct d.genus_no) as sampled_in_bin, sum(d.n_occs) as n_occs
		FROM $main_table as d JOIN $SCALE_MAP as sm using (interval_no) $age_join
			$other_joins
		WHERE $filter_expr and sm.scale_no = $scale_id and sm.scale_level = $reso $age_clause
		GROUP BY interval_no";

		$request->add_warning("The option 'genera_plus' is not supported with '/occs/quickdiv'.  If you want to promote subgenera to genera, use the operation '/occs/diversity' instead.") if $count_what eq 'genera_plus';
    }
    
    elsif ( $count_what eq 'families' )
    {
	$sql = "SELECT d.interval_no, count(distinct ph.family_no) as sampled_in_bin, sum(d.n_occs) as n_occs
		FROM $main_table as d JOIN $SCALE_MAP as sm using (interval_no) $age_join
			JOIN $INTS_TABLE as ph using (ints_no)
			$other_joins
		WHERE $filter_expr and sm.scale_no = $scale_id and sm.scale_level = $reso $age_clause
		GROUP BY interval_no";
    }
    
    elsif ( $count_what eq 'orders' )
    {
	$sql = "SELECT d.interval_no, count(distinct ph.order_no) as sampled_in_bin, sum(d.n_occs) as n_occs
		FROM $main_table as d JOIN $SCALE_MAP as sm using (interval_no) $age_join
			JOIN $INTS_TABLE as ph using (ints_no)
			$other_joins
		WHERE $filter_expr and sm.scale_no = $scale_id and sm.scale_level = $reso $age_clause
		GROUP BY interval_no";
    }
    
    else
    {
	$request->add_warning("unimplemented parameter value '$count_what'");
	return $request->list_result();
    }
    
    my $outer_sql = "
		SELECT interval_no, interval_name, early_age, late_age, d.sampled_in_bin, d.n_occs
		FROM $INTERVAL_DATA JOIN $SCALE_MAP as sm using (interval_no)
		    LEFT JOIN ($sql) as d using (interval_no)
		WHERE sm.scale_no = $scale_id and sm.scale_level = $reso
		ORDER BY early_age";
    
    print STDERR "$outer_sql\n\n" if $request->debug;
    
    $result = $dbh->selectall_arrayref($outer_sql, { Slice => {} });
    
    # Now trim empty bins from the start and end of the list.
    
    my ($start, $end);
    
    foreach my $i ( 0..$#$result )
    {
	$start = $i if $result->[$i]{n_occs} and not defined $start;
	$end = $i if $result->[$i]{n_occs};
	$result->[$i]{n_occs} //= 0;
	$result->[$i]{sampled_in_bin} //= 0;
    }
    
    if ( defined $start )
    {
	splice(@$result, $end + 1, 9999) if $end;	# must do this step first, because
	splice(@$result, 0, $start) if $start;		# this step makes $end inaccurate
    }
    
    else
    {
	$result = [];
    }
    
    # Then return the result.
    
    return $request->list_result($result);
}


# taxa ( )
# 
# Like 'list', but processes the resulting list of occurrences into a
# taxonomic tree.

sub taxa {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $tables = $request->tables_hash;
    
    $tables->{ph} = 1;
    $tables->{t} = 1;
    $tables->{tv} = 1;
    $tables->{pl} = 1;
    
    # Set up the phylogeny-computation options
    
    my $resolution = $request->clean_param('reso') || 'genera';
    my $count_what = $request->clean_param('count') || 'species';
    
    $request->{my_promote} = $request->clean_param('subgenera');
    $request->{my_attr} = $request->has_block('attr');
    
    # Determine the necessary set of query fields.
    
    my @fields = ('o.occurrence_no', 'tv.rank', 'tv.ints_no', 'ph.class', 'ph.class_no', 'ph.order', 'ph.order_no');
    
    if ( $request->has_block('occapp') )
    {
	push @fields, 'o.early_age', 'o.late_age';
	$request->{my_track_time} = 1;
    }
    
    # Interpret the rank parameter.
    
    if ( $count_what eq 'species' )
    {
	$request->{my_count_rank} = 3;
    }
    
    elsif ( $count_what eq 'genera' || $count_what eq 'genus' )
    {
	$request->{my_count_rank} = 5;
    }
    
    elsif ( $count_what eq 'genera_plus' || $count_what eq 'genus_plus' )
    {
	$request->{my_count_rank} = 5;
	$request->{my_promote} = 1;
    }
    
    elsif ( $count_what eq 'families' || $count_what eq 'family' )
    {
	$request->{my_count_rank} = 9;
    }
    
    elsif ( $count_what eq 'orders' || $count_what eq 'order' )
    {
	$request->{my_count_rank} = 13;
    }
    
    else
    {
	die "400 unknown value '$count_what' for parameter 'count'\n";
    }
    
    # Add the fields necessary for resolving the phylogeny down to the
    # specified rank
    
    if ( $resolution eq 'species' )
    {
	push @fields, 'ph.family', 'pl.genus', 'pl.subgenus', 'pl.species';
	$request->{my_reso_rank} = 3;
	$request->{my_count_rank} = 3;
    }
    
    elsif ( $resolution eq 'subgenera' || $resolution eq 'subgenus' )
    {
	push @fields, 'ph.family', 'pl.genus', 'pl.subgenus';
	$request->{my_reso_rank} = 4;
	$request->{my_count_rank} = 5 if $request->{my_count_rank} > 5;
    }
    
    elsif ( $resolution eq 'genera' || $resolution eq 'genus' )
    {
	push @fields, 'ph.family', 'pl.genus';
	$request->{my_reso_rank} = 5;
	$request->{my_count_rank} = 5 if $request->{my_count_rank} > 5;
    }
    
    elsif ( $resolution eq 'families' || $resolution eq 'family' )
    {
	push @fields, 'ph.family';
	$request->{my_reso_rank} = 9;
	$request->{my_count_rank} = 9 if $request->{my_count_rank} > 9;
    }
    
    elsif ( $resolution eq 'orders' || $resolution eq 'order' )
    {
	$request->{my_reso_rank} = 13;
    }
    
    else
    {
	die "400 unknown value '$resolution' for parameter 'reso'\n";
    }
    
    # Now add the fields necessary for counting down to the specified rank
    
    if ( $request->{my_count_rank} == 3 )
    {
	push @fields, 'ph.family_no', 'pl.genus_no', 'pl.subgenus_no', 'pl.species_no';
	$request->{my_count_rank} = 3;
    }
    
    elsif ( $request->{my_count_rank} == 5 )
    {
	push @fields, 'ph.family_no', 'pl.genus_no';
	$request->{my_count_rank} = 5;
    }
    
    elsif ( $request->{my_count_rank} == 9 )
    {
	push @fields, 'ph.family_no';
	$request->{my_count_rank} = 9;
    }
    
    # Delete unnecessary output fields, so they won't appear as empty columns
    # in text-format output.
    
    $request->delete_output_field('n_species') if $request->{my_count_rank} > 3;
    $request->delete_output_field('n_genera') if $request->{my_count_rank} > 5;
    $request->delete_output_field('n_families') if $request->{my_count_rank} > 9;
    
    my $fields = join(', ', @fields);
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = PB2::CollectionData::generateMainFilters($request, 'list', 'c', $tables);
    push @filters, PB2::OccurrenceData::generateOccFilters($request, $tables);
    push @filters, PB2::CommonData::generate_crmod_filters($request, 'o', $tables);
    push @filters, PB2::CommonData::generate_ent_filters($request, 'o', $tables);
    
    push @filters, "c.access_level = 0", "o.orig_no <> 0";
    
    my $filter_string = join(' and ', @filters);
    
    # Determine which extra tables, if any, must be joined to the query.  Then
    # construct the query.
    
    my $join_list = $request->generateJoinList('c', $tables);
    
    my $extra_group = $tables->{group_by_reid} ? ', o.reid_no' : '';
    
    $request->{main_sql} = "
	SELECT $fields
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c on o.collection_no = c.collection_no
		$join_list
        WHERE $filter_string
	GROUP BY o.occurrence_no $extra_group";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query.
    
    my $sth = $dbh->prepare($request->{main_sql});
    $sth->execute();
    
    # Now fetch all of the rows, and process them into a phylogenetic tree.
    # If the set of occurrences was generated from a base taxon, then we can
    # easily create a full phylogeny.  Otherwise, we generate an abbreviated
    # one using the information from the taxon_ints table.
    
    if ( ref $request->{my_base_taxa} eq 'ARRAY' && @{$request->{my_base_taxa}} )
    {
	$request->generate_taxon_table_full($sth, $request->{my_base_taxa});
    }
    
    else
    {
	$request->generate_taxon_table_ints($sth);
    }
}


# prevalence ( )
# 
# Returns the most prevalent taxa among the specified set of occurrences.

sub prevalence {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $taxonomy = ($request->{my_taxonomy} ||= Taxonomy->new($dbh, 'taxon_trees'));
    
    # my $fields = "p.orig_no, p.rank, t.name, t.lft, t.rgt, v.image_no, sum(p.n_occs) as n_occs";
    
    my $fields = "p.order_no, p.class_no, p.phylum_no, sum(p.n_occs) as n_occs";
    
    my $limit = $request->result_limit || 10;
    $limit += $request->result_offset;
    my $raw_limit = $limit * 10;
    
    my $detail = $request->clean_param("detail") || 1;
    $detail = 1 if $detail < 1;
    $detail = 3 if $detail > 3;
    
    #$request->substitute_select( mt => 'o', cd => 'oc' );
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.  First see if we can generate
    # a simple (and quick) expression based on the request parameters.
    
    my $tables = { };
    
    my @filters = $request->generateQuickDivFilters('p', $tables);
    
    if ( @filters )
    {
	# Add an interval filter if one was specified.
	
	my $interval_name = $request->clean_param('interval');
	my $interval_no;
	
	if ( $interval_name )
	{
	    my $quoted = $dbh->quote($interval_name);
	    
	    my $sql = "
		SELECT interval_no FROM $INTERVAL_DATA
		WHERE interval_name like $quoted";
	    
	    ($interval_no) = $dbh->selectrow_array($sql);
	    
	    unless ( $interval_no )
	    {
		$interval_no = -1;
		$request->add_warning("unknown interval '$interval_name'");
	    }
	}
	
	else
	{
	    $interval_no = $request->clean_param('interval_id');
	}
	
	$interval_no ||= 751;
	push @filters, "p.interval_no = $interval_no";
	
	# Then generate the required SQL statement.
	
	my $filter_string = join(' and ', @filters);
	
	no warnings 'uninitialized';
	
	if ( $tables->{use_global} )
	{
	    $request->{main_sql} = "
		SELECT $fields
		FROM $PVL_GLOBAL as p
		    JOIN $taxonomy->{TREE_TABLE} as t on t.orig_no = coalesce(order_no, class_no, phylum_no)
		    JOIN $taxonomy->{ATTRS_TABLE} as v using (orig_no)
		WHERE $filter_string
		GROUP BY orig_no
		ORDER BY n_occs desc LIMIT $raw_limit";
	}
	
	else
	{
	    $request->{main_sql} = "
		SELECT $fields
		FROM $PVL_MATRIX as p
		    JOIN $BIN_LOC as bl using (bin_id)
		    JOIN $taxonomy->{TREE_TABLE} as t on t.orig_no = coalesce(order_no, class_no, phylum_no)
		    JOIN $taxonomy->{ATTRS_TABLE} as v using (orig_no)
		WHERE $filter_string
		GROUP BY orig_no
		ORDER BY n_occs desc LIMIT $raw_limit";
	}
	
	print STDERR "$request->{main_sql}\n\n" if $request->debug;
	
	my $result = $dbh->selectall_arrayref($request->{main_sql}, { Slice => {} });
	
	$request->generate_prevalence($result, $limit, $detail);
	return;
    }
    
    # If the simple filters don't work, we must generate an expression linking
    # to the summary table.
    
    $tables = { };
    
    @filters = $request->generateMainFilters('summary', 's', $tables);
    push @filters, $request->generate_crmod_filters('o', $tables);
    push @filters, $request->generate_ent_filters('o', $tables);
    
    #$request->add_table('oc');
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.  If the given parameters can be fulfilled by just querying
    # for summary bins, we do so.  Otherwise, we have to go through the entire
    # set of occurrences again.
    
    if ( $tables->{o} || $tables->{cc} || $tables->{c} || $tables->{non_summary} )
    {
	$tables = { };
	
	@filters = $request->generateMainFilters('list', 'c', $tables);
	push @filters, $request->generate_crmod_filters('o', $tables);
	push @filters, $request->generate_ent_filters('o', $tables);
	
	my $fields = "ph.phylum_no, ph.class_no, ph.order_no, count(*) as n_occs";
	
	$tables->{t} = 1;
	$tables->{ph} = 1;
	
	push @filters, "c.access_level = 0";
	# @filters = grep { $_ !~ qr{^s.interval_no} } @filters;
	
	my $filter_string = join(' and ', @filters);
	
	# Determine which extra tables, if any, must be joined to the query.  Then
	# construct the query.
	
	my $join_list = $request->generateJoinList('c', $tables);
	
	$request->{main_sql} = "
	SELECT $fields
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c on o.collection_no = c.collection_no
		$join_list
        WHERE $filter_string
	GROUP BY ph.phylum_no, ph.class_no, ph.order_no
	ORDER BY n_occs desc LIMIT $raw_limit";
	
	print STDERR "$request->{main_sql}\n\n" if $request->debug;
	
	# Then prepare and execute the main query.
	
	my $result = $dbh->selectall_arrayref($request->{main_sql}, { Slice => {} });
	
	$request->generate_prevalence($result, $limit, $detail);
	return;
	
	# my $sth = $dbh->prepare($request->{main_sql});
	# $sth->execute();
	
	# return $request->generate_prevalence($sth, 'taxon_trees');
    }
    
    # Summary
    
    else
    {
	push @filters, "s.access_level = 0";
	my $filter_string = join(' and ', @filters);
	
	$request->{main_sql} = "
		SELECT $fields
		FROM $PVL_MATRIX as p JOIN $COLL_BINS as s using (bin_id, interval_no)
		    JOIN $taxonomy->{TREE_TABLE} as t on t.orig_no = coalesce(order_no, class_no, phylum_no)
		    JOIN $taxonomy->{ATTRS_TABLE} as v using (orig_no)
		WHERE $filter_string
		GROUP BY orig_no
		ORDER BY n_occs desc LIMIT $raw_limit";
	
	print STDERR "$request->{main_sql}\n\n" if $request->debug;
	
	my $result = $dbh->selectall_arrayref($request->{main_sql}, { Slice => {} });
	
	$request->generate_prevalence($result, $limit, $detail);
	return;
    }
}


sub generate_prevalence_joins {
    
    my ($request, $tables_ref) = @_;
    
    my $join_list = '';
    
    # Return an empty string unless we actually have some joins to make
    
    return $join_list unless ref $tables_ref eq 'HASH' and %$tables_ref;
    
    # Create the necessary join expressions.
        
    return $join_list;
    
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
	$tables_ref->{o} = 1;
	$tables_ref->{non_summary} = 1;
    }
    
    elsif ( $self->{clean_params}{id} )
    {
	push @filters, "o.occurrence_no = $self->{clean_params}{id}";
	$tables_ref->{non_summary} = 1;
    }
    
    # Check for parameter 'coll_id'
    
    if ( ref $self->{clean_params}{coll_id} eq 'ARRAY' and
	 @{$self->{clean_params}{coll_id}} )
    {
	my $id_list = join(',', @{$self->{clean_params}{coll_id}});
	push @filters, "o.collection_no in ($id_list)";
	$tables_ref->{non_summary} = 1;
    }
    
    elsif ( $self->{clean_params}{coll_id} )
    {
	push @filters, "o.collection_no = $self->{clean_params}{coll_id}";
	$tables_ref->{non_summary} = 1;
    }
    
    # Check for parameter 'ident'.  In cases of reidentified occurrences, it
    # specifies which identifications should be returned.  The default is
    # 'latest'.
    
    my $ident = $self->clean_param('ident');
    
    if ( $ident eq 'orig' )
    {
	push @filters, "o.reid_no = 0";
	$tables_ref->{non_summary} = 1;
    }
    
    elsif ( $ident eq 'all' )
    {
	$tables_ref->{group_by_reid} = 1;
	$tables_ref->{non_summary} = 1;
    }
    
    else # default: 'latest'
    {
	push @filters, "o.latest_ident = true";
    }
    
    return @filters;
}


# generateQuickDivFilters ( main_table, tables_ref )
# 
# Generate a list of filter clauses that will be used to compute the
# appropriate result set.  This routine should be called only when using the
# 'div_matrix' table, either for diversity tabulations or to return a set of
# geographic summary clusters.  If any parameter has been specified that would
# preclude using this table, the empty list is returned.
# 
# Any additional tables that are needed will be added to the hash specified by
# $tables_ref.

sub generateQuickDivFilters {

    my ($self, $mt, $tables_ref) = @_;
    
    my $dbh = $self->get_connection;
    my $taxonomy = $self->{my_taxonomy} ||= Taxonomy->new($dbh, 'taxon_trees');
    my @filters;
    
    # First check for parameters 'formation', 'stratgroup', 'member'.  If any
    # of these are specified, we abort.
    
    return () if $self->clean_param('formation') || $self->clean_param('stratgroup') || $self->clean_param('member');
    
    # Same with 'plate'.
    
    return () if $self->clean_param('plate');
    
    # Same with the date or authorizer parameters.
    
    return () if $self->clean_param('authorized_by') || $self->clean_param('entered_by') || $self->clean_param('modified_by');
    return () if $self->clean_param('created_before') || $self->clean_param('created_after');
    return () if $self->clean_param('modified_before') || $self->clean_param('modified_after');
    
    # Then check for geographic parameters, including 'clust_id', 'continent',
    # 'country', 'latmin', 'latmax, 'lngmin', 'lngmax', 'loc'
    
    my $clust_id = $self->clean_param('clust_id');
    
    if ( ref $clust_id eq 'ARRAY' && @$clust_id )
    {
	my @clusters = grep { $_ > 0 } @$clust_id;
	push @clusters, -1 unless @clusters;
	my $list = join(q{,}, @clusters);
	push @filters, "$mt.bin_id in ($list)";
    }
    
    if ( my @ccs = $self->clean_param_list('country') )
    {
	my $cc_list = "'" . join("','", @ccs) . "'";
	push @filters, "bl.cc in ($cc_list)";
	$tables_ref->{bl} = 1;
    }
    
    if ( my @continents = $self->clean_param_list('continent') )
    {
	my $cont_list = "'" . join("','", @continents) . "'";
	push @filters, "bl.continent in ($cont_list)";
	$tables_ref->{bl} = 1;
    }
    
    my $x1 = $self->clean_param('lngmin');
    my $x2 = $self->clean_param('lngmax');
    my $y1 = $self->clean_param('latmin');
    my $y2 = $self->clean_param('latmax');
    
    if ( $x1 ne '' && $x2 ne '' && ! ( $x1 == -180 && $x2 == 180 ) )
    {
	$y1 //= -90.0;
	$y2 //= 90.0;
	
	# If the longitude coordinates do not fall between -180 and 180, adjust
	# them so that they do.
	
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
	
	if ( $x1 < $x2 )
	{
	    my $polygon = "'POLYGON(($x1 $y1,$x2 $y1,$x2 $y2,$x1 $y2,$x1 $y1))'";
	    push @filters, "contains(geomfromtext($polygon), bl.loc)";
	    $tables_ref->{bl} = 1;
	}
	
	# Otherwise, our bounding box crosses the antimeridian and so must be
	# split in two.  The latitude bounds must always be between -90 and
	# 90, regardless.
	
	else
	{
	    my $polygon = "'MULTIPOLYGON((($x1 $y1,180.0 $y1,180.0 $y2,$x1 $y2,$x1 $y1))," .
					"((-180.0 $y1,$x2 $y1,$x2 $y2,-180.0 $y2,-180.0 $y1)))'";
	    push @filters, "contains(geomfromtext($polygon), bl.loc)";
	    $tables_ref->{bl} = 1;
	}
    }
    
    elsif ( ($y1 ne '' || $y2 ne '') && ! ( $y1 == -90 && $y2 == 90 ) )
    {
	$y1 //= -90;
	$y2 //= 90;
	
	my $polygon = "'POLYGON((-180.0 $y1,180.0 $y1,180.0 $y2,-180.0 $y2,-180.0 $y1))'";
	push @filters, "contains(geomfromtext($polygon), bl.loc)";
	$tables_ref->{bl} = 1;
    }
    
    if ( my $loc = $self->clean_param('loc') )
    {
	push @filters, "contains(geomfromtext($loc), bl.loc)";
	$tables_ref->{bl} = 1;
    }
    
    # At this point, if no geographic parameters have been specified then we
    # indicate to use the $DIV_GLOBAL table instead of $DIV_MATRIX.
    
    $tables_ref->{use_global} = 1 unless @filters;
    
    # Now check for taxonomic filters.
    
    my $taxon_name = $self->clean_param('taxon_name') || $self->clean_param('base_name');
    my @taxon_nos = $self->clean_param_list('taxon_id') || $self->clean_param_list('base_id');
    my @exclude_nos = $self->clean_param_list('exclude_id');
    my (@include_taxa, @exclude_taxa);
    my $bad_taxa;
    
    # First get the relevant taxon records for all included taxa
    
    if ( $taxon_name )
    {
	my @taxa = $taxonomy->resolve_names($taxon_name, { fields => 'RANGE' });
	
	# Now add these to the proper list.
	
	foreach my $t (@taxa)
	{
	    if ( $t->{exclude} )
	    {
		push @exclude_taxa, $t;
	    }
	    
	    else
	    {
		push @include_taxa, $t;
	    }
	}
	
	$bad_taxa = 1 unless @include_taxa || @exclude_taxa;
    }
    
    elsif ( @taxon_nos )
    {
	@include_taxa = $taxonomy->list_taxa('exact', \@taxon_nos, { fields => 'RANGE' });
	$bad_taxa = 1 unless @include_taxa;
    }
    
    # Then get the records for excluded taxa.
    
    if ( @exclude_nos )
    {
	my @taxa = $taxonomy->list_taxa('exact', \@exclude_nos, { fields => 'RANGE' });
	push @exclude_taxa, @taxa;
    }
    
    # Then construct the necessary filters for included taxa
    
    if ( @include_taxa )
    {
	my $taxon_filters = join ' or ', map { "t.lft between $_->{lft} and $_->{rgt}" } @include_taxa;
	push @filters, "($taxon_filters)";
	$tables_ref->{t} = 1;
	$self->{my_base_taxa} = \@include_taxa;
    }
    
    # If a bad taxon name or id was given, add a filter that will exclude
    # everything.  Also make sure that the warning generated by the Taxonomy
    # module is returned to the client.
    
    elsif ( $bad_taxa )
    {
	push @filters, "t.lft = 0";
	$tables_ref->{t} = 1;
	
	my @warnings = $taxonomy->list_warnings;
	$self->add_warning(@warnings);
    }
    
    # Now add filters for excluded taxa.  But only if there is at least one
    # included taxon as well.
    
    if ( @exclude_taxa && @include_taxa )
    {
	push @filters, map { "t.lft not between $_->{lft} and $_->{rgt}" } @exclude_taxa;
	$self->{my_excluded_taxa} = \@exclude_taxa;
	$tables_ref->{t} = 1;
    }
    
    # Return the list, and make sure it includes at least one clause.
    
    push @filters, "1=1" unless @filters;
    
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
    
    $tables->{t} = 1 if $tables->{pl} || $tables->{ph} || $tables->{v} || $tables->{tv} || $tables->{tf};
    
    my $t = $tables->{tv} ? 'tv' : 't';
    
    $join_list .= "JOIN collections as cc on c.collection_no = cc.collection_no\n"
	if $tables->{cc};
    $join_list .= "JOIN occurrences as oc on o.occurrence_no = oc.occurrence_no\n"
	if $tables->{oc};
    $join_list .= "LEFT JOIN taxon_trees as t on t.orig_no = o.orig_no\n"
	if $tables->{t};
    $join_list .= "LEFT JOIN taxon_trees as tv on tv.orig_no = t.accepted_no\n"
	if $tables->{tv};
    $join_list .= "LEFT JOIN taxon_lower as pl on pl.orig_no = $t.orig_no\n"
	if $tables->{pl};
    $join_list .= "LEFT JOIN taxon_ints as ph on ph.ints_no = $t.ints_no\n"
	if $tables->{ph};
    $join_list .= "LEFT JOIN taxon_attrs as v on v.orig_no = $t.orig_no\n"
	if $tables->{v};
    $join_list .= "LEFT JOIN taxon_names as nm on nm.taxon_no = o.taxon_no\n"
	if $tables->{nm};
    $join_list .= "LEFT JOIN taxon_names as ns on ns.taxon_no = t.spelling_no\n"
	if $tables->{nm} && $tables->{t};
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


sub generateQuickDivJoins {

    my ($self, $mt, $tables) = @_;
    
    my $join_list = '';
    
    # Return an empty string unless we actually have some joins to make
    
    return $join_list unless ref $tables eq 'HASH' and %$tables;
    
    # Create the necessary join expressions.
    
    $join_list .= "JOIN bin_loc as bl using (bin_id)\n"
	if $tables->{bl};
    
    $join_list .= "JOIN taxon_trees as t on t.orig_no = $mt.ints_no\n"
	if $tables->{t};
    
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
    
    no warnings 'uninitialized';
    
    # Construct the 'identified_name' field using the '_name' and '_reso'
    # fields from the occurrence record.  Also build 'taxon_name' using just
    # the '_name' fields.
    
    my $ident_name = $record->{genus_name} || 'UNKNOWN';
    my $taxon_name = $ident_name;
    
    $ident_name .= " $record->{genus_reso}" if $record->{genus_reso};
    
    if ( $record->{subgenus_name} )
    {
	$ident_name .= " ($record->{subgenus_name}";
	$ident_name .= " $record->{subgenus_reso}" if $record->{subgenus_reso};
	$ident_name .= ")";
	
	$taxon_name .= " ($record->{subgenus_name})";
    }
    
    if ( $record->{species_name} )
    {
	$ident_name .= " $record->{species_name}";
	$ident_name .= " $record->{species_reso}" if $record->{species_reso};

	$taxon_name .= " $record->{species_name}" if $record->{species_name} !~ /\.$/;
    }
    
    $record->{identified_name} = $ident_name;
    $record->{taxon_name} = $taxon_name;
    
    # If the 'identified_rank' field is not set properly, try to determine it.
    
    if ( defined $record->{species_name} && $record->{species_name} =~ qr{[a-z0-9]$} )
    {
	$record->{identified_rank} = 3;
    }
    
    elsif ( defined $record->{subgenus_name} && $record->{subgenus_name} =~ qr{[a-z0-9]$} )
    {
	$record->{identified_rank} = 4;
    }
    
    elsif ( defined $record->{species_name} && $record->{species_name} eq 'sp.' )
    {
	$record->{identified_rank} = 5;
    }
    
    elsif ( defined $record->{genus_name} && defined $record->{accepted_name} && 
	    $record->{genus_name} eq $record->{accepted_name} )
    {
	$record->{identified_rank} = $record->{accepted_rank};
    }
    
    # If the 'taxonomic_reason' value is 'belongs to', this is the common case
    # and there is no need to report it.  If 'taxon_name' is different from
    # 'accepted_name', then substitute 'spelling_reason'.  If the current
    # variant of the name has a spelling reason of 'recombination', then
    # use that.  Otherwise just clear the field.
    
    if ( $record->{taxonomic_reason} && $record->{taxonomic_reason} eq 'belongs to' )
    {
	if ( $record->{taxon_name} && $record->{accepted_name} &&
	     $record->{taxon_name} ne $record->{accepted_name} )
	{
	    my $len = length($record->{accepted_name});
	    
	    if ( $record->{accepted_name} eq substr($record->{taxon_name}, 0, $len) )
	    {
		$record->{taxonomic_reason} = 'taxon not fully entered';
	    }
	    
	    else
	    {
		$record->{taxonomic_reason} = $record->{spelling_reason};
		
		if ( $record->{accepted_reason} && $record->{accepted_reason} eq 'recombination' )
		{
		    $record->{taxonomic_reason} = 'recombination';
		}
		
		elsif ( $record->{accepted_reason} && $record->{accepted_reason} eq 'correction' &&
			$record->{taxonomic_reason} ne 'correction' )
		{
		    $record->{taxonomic_reason} = 'correction';
		}
		
		elsif ( $record->{taxonomic_reason} eq 'original spelling' &&
			$record->{identified_no} eq $record->{spelling_no} )
		{
		    $record->{taxonomic_reason} = '';
		}
		
		elsif ( $record->{identified_rank} && $record->{identified_rank} ne $record->{accepted_rank} )
		{
		    $record->{taxonomic_reason} ||= 'taxon not fully entered';
		}
	    }
	}
	
	else
	{
	    $record->{taxonomic_reason} = '';
	}
    }
    
    # If we don't have a value for 'taxonomic_reason' at all, then use 'taxon not
    # entered'.
    
    elsif ( ! $record->{identified_no} )
    {
	$record->{taxonomic_reason} = 'taxon not entered';
    }
}


1;
