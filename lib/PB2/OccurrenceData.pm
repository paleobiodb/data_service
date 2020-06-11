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

use TableDefs qw($OCC_MATRIX $SPEC_MATRIX $COLL_MATRIX $COLL_BINS $COLL_LITH $PVL_MATRIX $PVL_GLOBAL
		 $BIN_LOC $COUNTRY_MAP $PALEOCOORDS $GEOPLATES $COLL_STRATA
		 $INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP $INTERVAL_BUFFER $DIV_GLOBAL $DIV_MATRIX);
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);

use TaxonDefs qw(%RANK_STRING %TAXON_RANK %UNS_RANK %UNS_NAME);

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::DiversityData PB2::CommonData PB2::ReferenceData PB2::TaxonData PB2::CollectionData PB2::IntervalData);


# initialize ( )
# 
# This routine is called by the DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # We start by defining an output map for this class.
    
    $ds->define_output_map('1.2:occs:basic_map' =>
	{ value => 'full', maps_to => '1.2:occs:full_info' },
	    "This is a shortcut for including all of the information that defines this record.  Currently, this",
	    "includes the following blocks: B<attr>, B<class>, B<plant>, B<abund>, B<coords>, B<coll>",
	    "B<loc>, B<paleoloc>, B<prot>, B<stratext>, B<lithext>,",
	    "B<geo>, B<comps>, B<methods>, B<ctaph>, B<ecospace>, B<ttaph>, B<refattr>.",
	    "If we subsequently add new data fields to this record",
	    "then B<full> will include those as well.  So if you are publishing a URL,",
	    "it might be a good idea to include C<show=full>.",
	{ value => 'acconly' },
	    "Suppress the exact taxonomic identification of each occurrence,",
	    "and show only the accepted name.",
	{ value => 'attr', maps_to => '1.2:occs:attr' },
	    "The attribution (author and year) of the accepted name for this occurrence.",
	{ value => 'class', maps_to => '1.2:occs:class' },
	    "The taxonomic classification of the occurence: phylum, class, order, family,",
	    "genus.",
	{ value => 'classext', maps_to => '1.2:occs:class' },
	    "Like F<class>, but also includes the relevant taxon identifiers.",
	{ value => 'phylo', maps_to => '1.2:occs:class', undocumented => 1 },
	{ value => 'genus', maps_to => '1.2:occs:genus' },
	    "The genus corresponding to each occurrence, if the occurrence has been",
	    "identified to the genus level.  This block is redundant if F<class> or",
	    "F<classext> are used.",
	{ value => 'subgenus', maps_to => '1.2:occs:genus' },
	    "The genus corresponding to each occurrence, plus the subgenus if any.",
	    "This can be added to F<class> or F<classext> in order to display",
	    "subgenera, or used instead of F<genus> to display both the genus",
	    "and the subgenus if any.",
	{ value => 'ident', maps_to => '1.2:occs:ident' },
	    "Show the individual components of the taxonomic identification of the occurrence.",
	    "These values correspond to the value of F<identified_name> in the basic record,",
	    "and so this additional section will rarely be needed.",
        { value => 'rem', maps_to => '1.2:occs:rem' },
	    "Any additional remarks that were entered about the occurrence.",
 	{ value => 'img', maps_to => '1.2:taxa:img' },
	    "The identifier of the image (if any) associated with this taxon.",
	    "These images are sourced from L<phylopic.org>.",
	{ value => 'plant', maps_to => '1.2:occs:plant' },
	    "The plant organ(s), if any, associated with this occurrence.  These fields",
	    "will be empty unless the occurrence is a plant fossil.",
	{ value => 'abund', maps_to => '1.2:occs:abund' },
	    "Information about the abundance of this occurrence in the collection",
	{ value => 'ecospace', maps_to => '1.2:taxa:ecospace' },
	    "Information about ecological space that this organism occupies or occupied.",
	    "This has only been filled in for a relatively few taxa.  Here is a",
	    "L<list of values|node:general/ecotaph#Ecospace>.",
	{ value => 'ttaph', maps_to => '1.2:taxa:taphonomy' },
	    "Information about the taphonomy of this organism.  You can also use",
	    "the alias C<B<taphonomy>>.  Here is a",
	    "L<list of values|node:general/ecotaph#Taphonomy>.",
	{ value => 'taphonomy', maps_to => '1.2:taxa:taphonomy', undocumented => 1 },
	{ value => 'etbasis', maps_to => '1.2:taxa:etbasis' },
	    "Annotates the output block F<ecospace>, indicating at which",
	    "taxonomic level each piece of information was entered.",
	{ value => 'pres', undocumented => 1 },
	# The above has been deprecated, its information is now included in
	# the 'flags' field.  But we keep it here to avoid errors.
	{ value => 'coll', maps_to => '1.2:colls:name' },
	    "The name of the collection in which the occurrence was found, plus any",
	    "additional remarks entered about it.",
	{ value => 'coords', maps_to => '1.2:occs:coords' },
	    "The latitude and longitude of this occurrence",
        { value => 'loc', maps_to => '1.2:colls:loc' },
	    "Additional information about the geographic locality of the occurrence",
	{ value => 'bin', maps_to => '1.2:colls:bin' },
	    "The list of geographic clusters to which the collection belongs.",
	{ value => 'paleoloc', maps_to => '1.2:colls:paleoloc' },
	    "Information about the paleogeographic locality of the occurrence,",
	    "evaluated according to the model specified by the parameter F<pgm>.",
	{ value => 'prot', maps_to => '1.2:colls:prot' },
	    "Indication of whether the occurrence is located on protected land.",
        { value => 'time', maps_to => '1.2:colls:time' },
	    "Additional information about the temporal locality of the occurrence.",
	{ value => 'timebins', maps_to => '1.2:colls:timebins' },
	    "Shows a list of temporal bins into which each occurrence falls according",
	    "to the timerule selected for this request. You may select one using the",
	    "B<C<timerule>> parameter, or it will default to C<B<major>>.",
	{ value => 'timecompare', maps_to => '1.2:colls:timecompare' },
	    "Like B<C<timebins>>, but shows this information for all available",
	    "timerules.",
	{ value => 'strat', maps_to => '1.2:colls:strat' },
	    "Basic information about the stratigraphic context of the occurrence.",
	{ value => 'stratext', maps_to => '1.2:colls:stratext' },
	    "Detailed information about the stratigraphic context of the occurrence.",
	    "This includes all of the information from F<strat> plus extra fields.",
	{ value => 'lith', maps_to => '1.2:colls:lith' },
	    "Basic information about the lithological context of the occurrence.",
	{ value => 'lithext', maps_to => '1.2:colls:lithext' },
	    "Detailed information about the lithological context of the occurrence.",
	    "This includes all of the information from F<lith> plus extra fields.",
	{ value => 'methods', maps_to => '1.2:colls:methods' },
	    "Information about the collection methods used",
	{ value => 'env', maps_to => '1.2:colls:env' },
	    "The paleoenvironment associated with the occurrence.",
	{ value => 'geo', maps_to => '1.2:colls:geo' },
	    "Information about the geological context of the occurrence (includes C<B<env>>).",
	{ value => 'ctaph', maps_to =>'1.2:colls:taphonomy' },
	    "Information about the taphonomy of the occurrence and the mode of",
	    "preservation of the fossils in the associated collection.",
	{ value => 'comps', maps_to => '1.2:colls:components' },
	    "Information about the various kinds of body parts and other things",
	    "found as part of the associated collection.",
	{ value => 'ref', maps_to => '1.2:refs:primary' },
	    "The reference from which the occurrence was entered, as formatted text.",
	    "If no reference is recorded for this occurrence, the primary reference for its",
	    "associated collection is returned.",
        { value => 'refattr', maps_to => '1.2:refs:attr' },
	    "The author(s) and year of publication of the reference from which the occurrence",
	    "was entered.  If no reference is recorded for this occurrence, the information",
	    "from the primary reference for its associated collection is returned.",
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
		     't.rank as identified_rank', 't.status as taxon_status', 't.orig_no', 't.spelling_no', 't.accepted_no',
		     'nm.spelling_reason', 'ns.spelling_reason as accepted_reason',
		     'tv.spelling_no as accepted_spelling', 'tv.name as accepted_name', 'tv.rank as accepted_rank',
		     'ei.interval_name as early_interval', 'li.interval_name as late_interval',
		     'o.genus_name', 'o.genus_reso', 'o.subgenus_name', 'o.subgenus_reso',
		     'o.species_name', 'o.species_reso',
		     'o.early_age', 'o.late_age', 'o.reference_no', 'r.pubyr', 'v.is_trace', 'v.is_form'],
	  tables => ['o', 'tv', 'ts', 'nm', 'ei', 'li', 'r', 'v'] },
	{ set => '*', from => '*', code => \&process_basic_record },
	{ set => '*', code => \&process_occ_com, if_vocab => 'com' },
	{ set => '*', code => \&process_occ_ids },
	{ output => 'occurrence_no', dwc_name => 'occurrenceID', com_name => 'oid' },
	    "A positive integer that uniquely identifies the occurrence",
	{ output => 'record_type', com_name => 'typ', value => $IDP{OCC}, dwc_value => 'Occurrence' },
	    "The type of this object: C<$IDP{OCC}> for an occurrence.",
	{ output => 'reid_no', com_name => 'eid', if_field => 'reid_no' },
	    "If this occurrence was reidentified, a unique identifier for the reidentification.",
	{ output => 'flags', com_name => 'flg' },
	    "This field will be empty for most records.  Otherwise, it will contain one or more",
	    "of the following letters:", "=over",
	    "=item R", "This identification has been superceded by a more recent one.",
		"In other words, this occurrence has been reidentified.",
	    "=item I", "This identification is an ichnotaxon",
	    "=item F", "This identification is a form taxon",
	{ output => 'collection_no', com_name => 'cid', dwc_name => 'CollectionId' },
	    "The identifier of the collection with which this occurrence is associated.",
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
	{ set => 'permissions', from => '*', code => \&PB2::CollectionData::process_permissions },
	{ output => 'identified_name', com_name => 'idn', dwc_name => 'associatedTaxa', not_block => 'acconly' },
	    "The taxonomic name by which this occurrence was identified.  This field will",
	    "be omitted for responses in the compact voabulary if it is identical",
	    "to the value of F<accepted_name>.",
	{ output => 'identified_rank', dwc_name => 'taxonRank', com_name => 'idr', not_block => 'acconly' },
	    "The taxonomic rank of the identified name, if this can be determined.  This field will",
	    "be omitted for responses in the compact voabulary if it is identical",
	    "to the value of F<accepted_rank>.",
	{ set => 'identified_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb', not_block => 'acconly' },
	{ output => 'identified_no', com_name => 'iid', not_block => 'acconly' },
	    "The unique identifier of the identified taxonomic name.  If this is empty, then",
	    "the name was never entered into the taxonomic hierarchy stored in this database and",
	    "we have no further information about the classification of this occurrence.  In some cases,",
	    "the genus has been entered into the taxonomic hierarchy but not the species.  This field will",
	    "be omitted for responses in the compact voabulary if it is identical",
	    "to the value of F<accepted_no>.",
	{ output => 'difference', com_name => 'tdf', not_block => 'acconly' },
	    "If the identified name is different from the accepted name, this field gives",
	    "the reason why.  This field will be present if, for example, the identified name",
	    "is a junior synonym or nomen dubium, or if the species has been recombined, or",
	    "if the identification is misspelled.",
	{ output => 'accepted_name', com_name => 'tna', if_field => 'accepted_no' },
	    "The value of this field will be the accepted taxonomic name corresponding",
	    "to the identified name.",
	{ output => 'accepted_attr', if_block => '1.2:occs:attr', 
	  dwc_name => 'scientificNameAuthorship', com_name => 'att' },
	    "The attribution (author and year) of the accepted name",
	{ output => 'accepted_rank', com_name => 'rnk', if_field => 'accepted_no' },
	    "The taxonomic rank of the accepted name.  This may be different from the",
	    "identified rank if the identified name is a nomen dubium or otherwise invalid,",
	    "or if the identified name has not been fully entered into the taxonomic hierarchy",
	    "of this database.",
	{ set => 'accepted_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb' },
	{ output => 'accepted_no', com_name => 'tid', if_field => 'accepted_no' },
	    "The unique identifier of the accepted taxonomic name in this database.",
	{ set => '*', code => \&PB2::CollectionData::fixTimeOutput },
	{ output => 'early_interval', com_name => 'oei', pbdb_name => 'early_interval' },
	    "The specific geologic time range associated with this occurrence (not necessarily a",
	    "standard interval), or the interval that begins the range if F<late_interval> is also given",
	{ output => 'late_interval', com_name => 'oli', pbdb_name => 'late_interval', dedup => 'early_interval' },
	    "The interval that ends the specific geologic time range associated with this occurrence,",
	    "if different from the value of F<early_interval>",
	{ output => 'early_age', com_name => 'eag', pbdb_name => 'max_ma' },
	    "The early bound of the geologic time range associated with this occurrence (in Ma)",
	{ output => 'late_age', com_name => 'lag', pbdb_name => 'min_ma' },
	    "The late bound of the geologic time range associated with this occurrence (in Ma)",
	{ output => 'ref_author', dwc_name => 'recordedBy', com_name => 'aut', if_block => '1.2:refs:attr' },
	    "The author(s) of the reference from which this data was entered.",
	{ output => 'ref_pubyr', com_name => 'pby', if_block => '1.2:refs:attr' },
	    "The year of publication of the reference from which this data was entered",
	{ output => 'reference_no', com_name => 'rid' },
	    "The identifier of the reference from which this data was entered");
    
    $ds->define_block('1.2:occs:attr' =>
	{ select => ['v.attribution', 'v.pubyr'], tables => 'v' });
    
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
    
    $ds->define_block('1.2:occs:class' =>
	{ select => ['ph.family', 'ph.family_no', 'ph.order', 'ph.order_no',
		     'ph.class', 'ph.class_no', 'ph.phylum', 'ph.phylum_no',
		     'pl.genus', 'pl.genus_no', 'pl.subgenus', 'pl.subgenus_no'],
	  tables => ['ph', 't', 'pl'] },
	{ set => '*', code => \&process_classification },
	{ output => 'phylum', com_name => 'phl' },
	    "The name of the phylum in which this occurrence is classified.",
	{ output => 'phylum_no', com_name => 'phn', if_block => 'classext' },
	    "The identifier of the phylum in which this occurrence is classified.",
	    "This is only included with the block F<classext>.",
	{ output => 'class', com_name => 'cll' },
	    "The name of the class in which this occurrence is classified.",
	{ output => 'class_no', com_name => 'cln', if_block => 'classext' },
	    "The identifier of the class in which this occurrence is classified.",
	    "This is only included with the block F<classext>.",
	{ output => 'order', com_name => 'odl' },
	    "The name of the order in which this occurrence is classified.",
	{ output => 'order_no', com_name => 'odn', if_block => 'classext' },
	    "The identifier of the order in which this occurrence is classified.",
	    "This is only included with the block F<classext>.",
	{ output => 'family', com_name => 'fml' },
	    "The name of the family in which this occurrence is classified.",
	{ output => 'family_no', com_name => 'fmn', if_block => 'classext' },
	    "The identifier of the family in which this occurrence is classified.",
	    "This is only included with the block F<classext>.",
	{ output => 'genus', com_name => 'gnl' },
	    "The name of the genus in which this occurrence is classified.",
	    "If the block F<subgenus> is specified, this will include the subgenus",
	    "name if any.",
	{ output => 'genus_no', com_name => 'gnn', if_block => 'classext' },
	    "The identifier of the genus in which this occurrence is classified",
	{ output => 'subgenus_no', com_name => 'sgn', if_block => 'classext', dedup => 'genus_no' },
	    "The identifier of the subgenus in which this occurrence is classified,",
	    "if any.",
	{ set => '*', code => \&process_occ_subgenus });
    
    $ds->define_block('1.2:occs:genus' =>
	{ select => ['pl.genus', 'pl.genus_no', 'pl.subgenus', 'pl.subgenus_no'],
	  tables => ['t', 'pl'] },
	{ output => 'genus', com_name => 'gnl', not_block => 'class,classext' },
	    "The name of the genus in which this occurrence is classified.",
	    "If the block F<subgenus> is specified, this will include the subgenus",
	    "name if any.",
	# { output => 'genus_no', com_name => 'gnn', not_block => 'class,classext' },
	#     "The identifier of the genus in which this occurrence is classified",
	# { output => 'subgenus_no', com_name => 'sgn', not_block => 'class,classext' },
	#     "The identifier of the genus in which this occurrence is classified,",
	#     "if any.",
	{ set => '*', code => \&process_occ_subgenus, not_block => 'class,classext' });
    
    $ds->define_block('1.2:occs:plant' =>
	{ select => ['o.plant_organ', 'o.plant_organ2'] },
	{ output => 'plant_organ', com_name => 'pl1' },
	    "The plant organ, if any, associated with this occurrence.  This field",
	    "will be empty unless the occurrence is a plant fossil.",
	{ output => 'plant_organ2', com_name => 'pl2' },
	    "An additional plant organ, if any, associated with this occurrence.");
    
    $ds->define_block('1.2:occs:coords' =>
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
    
    $ds->define_block('1.2:occs:rem' =>
	{ select => ['oc.comments'], tables => ['oc'] },
	{ output => 'comments', pbdb_name => 'occurrence_comments', com_name => 'ocm' },
	    "Additional comments about this occurrence, if any.");
    
    $ds->define_block( '1.2:occs:full_info' =>
	{ include => '1.2:occs:attr' },
	{ include => '1.2:occs:class' },
	{ include => '1.2:occs:plant' },
	{ include => '1.2:occs:abund' },
	{ include => '1.2:occs:coords' },
	{ include => '1.2:occs:rem' },
	{ include => '1.2:colls:name' },
	{ include => '1.2:colls:loc' },
	{ include => '1.2:colls:paleoloc' },
	{ include => '1.2:colls:prot' },
	{ include => '1.2:colls:stratext' },
	{ include => '1.2:colls:lithext' },
	{ include => '1.2:colls:geo' },
	{ include => '1.2:colls:components' },
	{ include => '1.2:colls:taphonomy' },
	{ include => '1.2:colls:methods' },
	{ include => '1.2:taxa:ecospace' },
	{ include => '1.2:taxa:taphonomy' },
	{ include => '1.2:refs:attr' });
    
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
	    "depending upon the value you provided for the parameter F<count>.",
	    "The terminology for this field and the next three comes from:",
	    "M. Foote. Origination and Extinction Components of Taxonomic Diversity: General Problems.",
	    "I<Paleobiology>, Vol. 26(4). 2000.",
	    "pp. 74-102. L<http://www.jstor.org/stable/1571654>.",
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
	{ output => 'implied_in_bin', com_name => 'dib' },
	    "The number of additional distinct taxa implied in this interval,",
	    "as a result of imprecisely identified occurrences. For example, if",
	    "you are counting species, an occurrence identified only to the genus level",
	    "to a genus not otherwise appearing in this interval would add one",
	    "to this count. This is an experimental feature, and the number reported",
	    "in this field does not affect the other statistics. You can feel free",
	    "to ignore it if you want.",
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
	    "bins (see F<timerule>).  This value might also be smaller",
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
	    "parameter F<count>.",
	{ output => 'n_occs', com_name => 'noc' },
	    "The total number of occurrences that are resolved to this interval");
    
    # The following block specifies the output for taxon records representing
    # occurrence taxonomies.
    
    # $ds->define_block('1.2:occs:taxa' =>
    # 	{ output => 'taxon_no', com_name => 'oid' },
    # 	    "The identifier of the taxon represented by this record",
    # 	{ output => 'parent_no', com_name => 'par' },
    # 	    "The identifier of the parent taxon.  You can use this field to assemble",
    # 	    "these records into one or more taxonomic trees.  A value of 0",
    # 	    "indicates a root of one of these trees.  By default, records representing",
    # 	    "classes have a value of 0 in this field.",
    # 	{ output => 'taxon_rank', com_name => 'rnk' },
    # 	    "The rank of the taxon represented by this record",
    # 	{ set => 'taxon_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb' },
    # 	{ output => 'taxon_name', com_name => 'nam' },
    # 	    "The name of the taxon represented by this record",
    # 	{ output => 'taxon_attr', if_block => 'attr', com_name => 'att' },
    # 	    "The attribution (author and year) of this taxonomic name",
    # 	{ output => 'n_orders', com_name => 'odc' },
    # 	    "The number of orders from within this taxon that appear in the set of",
    # 	    "fossil occurrences being analyzed",
    # 	{ output => 'n_families', com_name => 'fmc' },
    # 	    "The number of families from within this taxon that appear in the set of",
    # 	    "fossil occurrences being analyzed",
    # 	{ output => 'n_genera', com_name => 'gnc' },
    # 	    "The number of genera from within this taxon that appear in the set of",
    # 	    "fossil occurrences being analyzed",
    # 	{ output => 'n_species', com_name => 'spc' },
    # 	    "The number of species from within this taxon that appear in the set of",
    # 	    "fossil occurrences being analyzed",
    # 	{ output => 'specific_occs', com_name => 'soc' },
    # 	    "The number of occurrences that are identified to this specific taxon",
    # 	    "in the set of fossil occurrences being analyzed",
    # 	{ output => 'n_occs', com_name => 'noc' },
    # 	    "The total number of occurrences of this taxon or any of its subtaxa in the",
    # 	    "set of fossil occurrences being analyzed");
    
    $ds->define_output_map('1.2:occs:taxa_opt' =>
	@PB2::TaxonData::BASIC_1,
	{ value => 'occapp', maps_to => '1.2:taxa:occapp' },
	    "The age of first and last appearance of each taxon from the set",
	    "of occurrences being analyzed (not the absolute first and last",
	    "occurrence ages).",
	@PB2::TaxonData::BASIC_2,
	@PB2::TaxonData::BASIC_3);
    
    $ds->define_block('1.2:occs:taxa_summary' =>
	{ output => 'total_count', pbdb_name => 'n_occs', com_name => 'noc' },
	    "The number of occurrences that were scanned in the process of",
	    "computing this taxonomic tree.",
	{ output => 'missing_taxon', com_name => 'mtx' },
	    "The number of occurrences skipped because the taxonomic hierarchy",
	    "in this database is incomplete.  For example, some genera have not",
	    "been placed in their proper family, so occurrences in these genera",
	    "will be skipped if you are counting families.");
    
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
    
    # One more block for checking the diversity output.
    
    $ds->define_block('1.2:occs:checkdiv' =>
	{ output => 'interval_no', com_name => 'iid' },
	    "The identifier of an interval from the diversity output.",
	{ output => 'interval_name', com_name => 'nam' },
	    "The name of an interval from the diversity output.",
	{ output => 'occurrence_no', com_name => 'oid' },
	    "The identifier of an occurrence from the database.",
	{ output => 'diagnosis', com_name => 'dgn' },
	    "A message indicating how, and if, the occurence or taxon was counted. This will",
	    "be one of the following:", "=over",
	    "=item counted", "This occurrence or taxon contributed to the diversity count for this interval.",
	    "=item implied", "This occurrence or taxon was counted as an \"implied\" taxon, because",
	         "it represented a higher taxon which did not otherwise appear in this interval.",
	    "=item imprecise age", "This occurrence was not counted because its age range was too large",
	         "to match any of the reported intervals under the time rule used for this operation.",
	    "=item imprecise taxon", "This occurrence was not counted because it was not identified to a",
	         "low enough rank to be counted at the level selected for this operation. For example,",
	         "genera are being counted and this occurrence was only identified to the family level.",
	    "=item missing taxon", "This occurrence was not counted because it was not identified to",
	         "a taxon represented in the taxonomic tree.",
	{ output => 'early_age', com_name => 'eag', pbdb_name => 'max_ma' },
	    "The early end of the age range for this occurrence.",
	{ output => 'late_age', com_name => 'lag', pbdb_name => 'min_ma' },
	    "The late end of the age ragne for this occurrence.",
	{ output => 'orig_no', com_name => 'tid' },
	    "The identifier of a taxon from the database.",
	{ output => 'count_name', com_name => 'con', pbdb_name => 'counted_name' },
	    "The name under which this taxon or occurrence was counted.",
	{ output => 'accepted_name', com_name => 'acn' },
	    "A list of the accepted names of the occurrences that were counted for this taxon.",
	{ output => 'occ_ids', com_name => 'ocs' },
	    "A list of the occurrence identifiers that were counted for this taxon.");
    
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
    
    $ds->define_set('1.2:occs:quickdiv_count' =>
	{ value => 'genera' },
	    "Count genera.  You can also use the value C<genus>.",
	{ value => 'genus', undocumented => 1 },
	{ value => 'genera_plus', undocumented => 1 },
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
	{ value => 'id' },
	    "Results are ordered by identifier, so they are reported in the order",
	    "in which they were entered into the database.  This is the default",
	    "if you select B<C<all_records>>.",
	{ value => 'id.asc', undocumented => 1 },
	{ value => 'id.desc', undocumented => 1 },
	{ value => 'hierarchy' },
	    "Results are ordered hierarchically by taxonomic identification.",
	    "The order of sibling taxa is arbitrary, but children will always",
	    "follow after parents.",
	{ value => 'hierarchy.asc', undocumented => 1 },
	{ value => 'hierarchy.desc', undocumented => 1 },
	{ value => 'identification' },
	    "Results are ordered alphabetically by taxonomic identification.",
	{ value => 'identification.asc', undocumented => 1 },
	{ value => 'identification.desc', undocumented => 1 },
	{ value => 'ref' },
	    "Results are ordered by reference id so that occurrences entered from",
	    "the same reference are listed together.",
	{ value => 'ref.asc', undocumented => 1 },
	{ value => 'ref.desc', undocumented => 1 },
	{ value => 'max_ma' },
	    "Results are ordered chronologically by early age bound, oldest to youngest unless you add C<.asc>",
	{ value => 'max_ma.asc', undocumented => 1 },
	{ value => 'max_ma.desc', undocumented => 1 },
	{ value => 'min_ma' },
	    "Results are ordered chronologically by late age bound, oldest to youngest unless you add C<.asc>",
	{ value => 'min_ma.asc', undocumented => 1 },
	{ value => 'min_ma.desc', undocumented => 1 },
	{ value => 'agespan' },
	    "Results are ordered based on the difference between the early and late age bounds, starting",
	    "with occurrences with the smallest spread (most precise temporal resolution) unless you add C<.desc>",
	{ value => 'agespan.asc', undocumented => 1 },
	{ value => 'agespan.desc', undocumented => 1 },
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
	{ param => 'occ_id', valid => VALID_IDENTIFIER('OID'), alias => 'id' },
	    "The identifier of the occurrence or re-identification you wish to retrieve (REQUIRED).",
	    "You may instead use the parameter name B<C<id>>.  If the value of this",
	    "parameter is a numeric identifier or an extended identifier of type C<B<occ>>,",
	    "then the latest identification of the specified occurrence will be returned.  If it is",
	    "an identifier of type C<B<rei>>, then the specified re-identification will be",
	    "returned.  See also B<C<idtype>> below.",
	">>The following optional parameter may occasionally be useful:",
	{ optional => 'idtype', valid => '1.2:occs:ident_single', alias => 'ident_type', no_set_doc => 1 },
	    "If the value of this parameter is C<B<orig>>, then the original",
	    "identification of the specified occurrence is returned.  This overrides",
	    "any specification of a particular re-identification.");
    
    $ds->define_ruleset('1.2:occs:selector' =>
	{ param => 'occ_id', valid => VALID_IDENTIFIER('OCC'), list => ',', alias => 'id' },
	    "A comma-separated list of occurrence identifiers.  The specified occurrences",
	    "are selected, provided they satisfy the other parameters",
	    "given with this request.  You may also use the parameter name B<C<id>>.",
	    "You can also use this parameter along with any of the other parameters",
	    "to filter a known list of occurrences according to other criteria.",
	{ param => 'coll_id', valid => VALID_IDENTIFIER('COL'), list => ',' },
	    "A comma-separated list of collection identifiers.  All occurrences associated with the",
	    "specified collections are selected, provided they satisfy the other parameters given",
	    "with this request.");
    
    $ds->define_ruleset('1.2:occs:all_records' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "Select all occurrences entered in the database, subject to any other parameters you may specify.",
	    "This parameter does not require any value.");
    
    $ds->define_ruleset('1.2:occs:id' =>
	{ param => 'occ_id', valid => VALID_IDENTIFIER('OCC'), list => ',', alias => 'id' },
	    "A comma-separated list of occurrence identifiers.  You may instead",
	    "use the parameter name F<id>.");
    
    $ds->define_ruleset('1.2:occs:display' =>
	{ optional => 'show', list => q{,}, valid => '1.2:occs:basic_map' },
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each occurrence.  Its value should be",
	    "one or more of the following, separated by commas:");
    
	# { optional => 'order', valid => '1.2:occs:order', split => ',', no_set_doc => 1 },
	#     "Specifies the order in which the results are returned.  You can specify multiple values",
	#     "separated by commas, and each value may be appended with F<.asc> or F<.desc>.  Accepted values are:",
	#     $ds->document_set('1.2:occs:order'),
	#     "If no order is specified, results are sorted by occurrence identifier.",
	# { ignore => 'level' });
    
    $ds->define_ruleset('1.2:occs:single' => 
	"The following parameter selects a record to retrieve:",
    	{ require => '1.2:occs:specifier', 
	  error => "you must specify an occurrence identifier, either in the URL or with the 'id' parameter" },
	">>You may also use the following parameters to specify what information you wish to retrieve:",
	{ optional => 'pgm', valid => '1.2:colls:pgmodel', list => "," },
	    "Specify which paleogeographic model(s) to use when evaluating paleocoordinates.",
	    "You may specify one or more from the following list, separated by commas.",
	    "If you do not specify a value for this parameter, the default model is C<gplates>.",
    	{ allow => '1.2:occs:display' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:list' => 
	"You can use the following parameter if you wish to retrieve the entire set of",
	"occurrence records entered in this database.  Please use this with care, since the",
	"result set will contain more than 1 million records and will be at least 100 megabytes in size.",
	"You may also select subsets of this list by specifying some combination of the parameters listed below.",
    	{ allow => '1.2:occs:all_records' },
        ">>The following parameters can be used to query for occurrences by a variety of criteria.",
	"Except as noted below, you may use these in any combination.  If you do not specify B<C<all_records>>,",
	"you must specify at least one selection parameter from the following list.",
	{ allow => '1.2:occs:selector' },
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:occs:all_records', '1.2:occs:selector', '1.2:main_selector', 
			  '1.2:interval_selector', '1.2:ma_selector'] },
	{ ignore => 'level' },
	">>The following parameters can be used to filter the selection.",
	"If you wish to use one of them and have not specified any of the selection parameters",
	"listed above, use B<C<all_records>>.",
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ allow => '1.2:common:select_colls_ent' },
	">>The following parameters can also be used to filter the result list based on taxonomy:",
	{ allow => '1.2:taxa:occ_list_filter' },
	">>You can use the following parameters to select extra information you wish to retrieve,",
	"and the order in which you wish to get the records:",
	{ allow => '1.2:occs:display' },
	{ optional => 'order', valid => '1.2:occs:order', split => ',', no_set_doc => 1 },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with F<.asc> or F<.desc>.  Accepted values are:",
	    $ds->document_set('1.2:occs:order'),
	    "If no order is specified, results are sorted by occurrence identifier.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:byref' => 
	"You can use the following parameter if you wish to retrieve the entire set of",
	"occurrence records entered in this database.  Please use this with care, since the",
	"result set will contain more than 1 million records and will be at least 100 megabytes in size.",
	"You may also select subsets of this list by specifying some combination of the parameters listed below.",
    	{ allow => '1.2:occs:all_records' },
        ">>The following parameters can be used to query for occurrences by a variety of criteria.",
	"Except as noted below, you may use these in any combination.  If you do not specify B<C<all_records>>,",
	"you must specify at least one selection parameter from the following list.",
	{ allow => '1.2:occs:selector' },
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:occs:all_records', '1.2:occs:selector', '1.2:main_selector', 
			  '1.2:interval_selector', '1.2:ma_selector'] },
	{ ignore => ['level', 'ref_type', 'select'] },
	">>You can use the following parameters to filter the result set based on attributes",
	"of the bibliographic references.  If you wish to use one of them and have not specified",
	"any of the selection parameters listed above, use B<C<all_records>>.",
	{ allow => '1.2:refs:aux_selector' },
	">>The following parameters can also be used to filter the selection.",
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ allow => '1.2:common:select_refs_crmod' },
	{ allow => '1.2:common:select_refs_ent' },
	">>The following parameters can also be used to further filter the selection based on taxonomy:",
	{ allow => '1.2:taxa:occ_list_filter' },
	">>You can use the following parameter to select extra information you wish to retrieve,",
	"and the order in which you wish to get the records:",
	{ allow => '1.2:occs:display' },
	{ optional => 'order', valid => '1.2:occs:order', split => ',' },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  If this",
	    "parameter is not given, the returned occurrences are ordered by reference.  If",
	    "B<C<all_records>> is specified, the references will be sorted in the order they were",
	    "entered in the database.  Otherwise, they will be sorted by default by the name of the",
	    "first and second author.  Accepted values include:",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");

    # $$$ need to add occs:order_byref, including 'ref'.  Also need to
    # add occurrence id to occs:order and occs:order_byref.
    
    $ds->define_ruleset('1.2:occs:geosum' =>
	"The following required parameter selects from the available resolution levels.",
	"You can get a L<list of available resolution levels|op:config.txt?show=clusters>.",
	{ param => 'level', valid => POS_VALUE, default => 1 },
	    "Return records from the specified cluster level.  (REQUIRED)",
	">>You can use the following parameter if you wish to retrieve a geographic summary",
	"of the entire set of occurrences entered in the database.",
    	{ allow => '1.2:occs:all_records' },
	">>You can use the following parameters to query for occurrences by",
	"a variety of criteria.  Except as noted below, you may use these in any combination.",
	"The resulting list will be mapped onto summary clusters at the selected level of",
	"resolution.",
    	{ allow => '1.2:occs:selector' },
    	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:occs:all_records', '1.2:occs:selector', '1.2:main_selector',
			  '1.2:interval_selector', '1.2:ma_selector'] },
	">>The following parameters filter the result set.  If you wish to use one of them and",
	"have not specified any of the selection parameters listed above, use B<C<all_records>>.",
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	">>The following parameters can be used to further filter the selection, based on the",
	"taxonomy of the selected occurrences.",
	{ allow => '1.2:taxa:occ_list_filter' },
	">>You can use the following parameter to request additional information",
	"beyond the basic summary cluster records.",
   	{ allow => '1.2:summary_display' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:taxa' =>
	"You can use the following parameter if you wish to retrieve taxonomic tree corresponding",
	"to the entire set of occurrence records entered in this database.  Please use this with care,",
	"since the result set will contain more than 250,000 records and will be at least 50 megabytes in size.",
	"You may also select subsets of this list by specifying some combination of the parameters listed below.",
    	{ allow => '1.2:occs:all_records' },
        ">>The following parameters can be used to query for occurrences by a variety of criteria.",
	"Except as noted below, you may use these in any combination.  If you do not specify B<C<all_records>>,",
	"you must specify at least one selection parameter from the following list.",
	{ allow => '1.2:occs:selector' },
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:occs:all_records', '1.2:occs:selector', '1.2:main_selector', 
			  '1.2:interval_selector', '1.2:ma_selector' ] },
	{ ignore => 'level' },
	">>The following parameters can be used to filter the selection.",
	"If you wish to use one of them and have not specified any of the selection parameters",
	"listed above, use B<C<all_records>>.",
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ allow => '1.2:common:select_taxa_crmod' },
	{ allow => '1.2:common:select_taxa_ent' },
	">>The following parameters can also be used to filter the result list based on taxonomy:",
	{ allow => '1.2:taxa:occ_aux_filter' },
	">>You can use the following parameters to select extra information you wish to retrieve,",
	"and the order in which you wish to get the records:",
	{ optional => 'SPECIAL(show)', valid => '1.2:occs:taxa_opt' },
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each taxon.  Its value should be",
	    "one or more of the following, separated by commas:",
	{ optional => 'order', valid => ANY_VALUE },
	    "This parameter is currently nonfunctional.  It will eventually allow you",
	    "to to set the order in which the taxa are returned.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    # $ds->define_ruleset('1.2:occs:ttest' =>
    # 	# "The following parameters specify what to count and at what
    # 	# taxonomic resolution:", { allow => '1.2:occs:taxa_params' },
    #     ">>The following parameters select which occurrences to analyze.",
    # 	"Except as noted below, you may use these in any combination.",
    # 	"All of these parameters can be used with L<occs/list|node:occs/list> as well, to retrieve",
    # 	"the exact list of occurrences used to compute this phylogeny.",
    # 	{ allow => '1.2:main_selector' },
    # 	{ allow => '1.2:interval_selector' },
    # 	{ allow => '1.2:ma_selector' },
    # 	{ allow => '1.2:taxa:occ_filter' },
    # 	{ allow => '1.2:common:select_occs_crmod' },
    # 	{ allow => '1.2:common:select_occs_ent' },
    # 	{ allow => '1.2:common:select_taxa_crmod' },
    # 	{ allow => '1.2:common:select_taxa_ent' },
    # 	{ require_any => ['1.2:main_selector', '1.2:interval_selector', '1.2:main_selector',
    # 			  '1.2:common:select_occs_crmod', '1.2:common:select_occs_ent',
    # 			  '1.2:common:select_taxa_crmod', '1.2:common:select_taxa_ent'] },
    # 	{ optional => 'SPECIAL(show)', valid => '1.2:occs:taxa_opt' },
    # 	{ allow => '1.2:special_params' },
    # 	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:div_params' =>
	{ param => 'count', valid => '1.2:occs:div_count', default => 'genera' },
	    "This parameter specifies the taxonomic level at which to count.  If not",
	    "specified, it defaults to C<genera>.  The accepted values are:",
	{ param => 'subgenera', valid => FLAG_VALUE },
	    "You can use this parameter as a shortcut, equivalent to specifying",
	    "C<count=genera_plus>.  Just include its name, no value is needed.",
	{ param => 'recent', valid => FLAG_VALUE },
	    "If this parameter is specified, then taxa that are known to be extant",
	    "are considered to range through to the present, regardless of the age",
	    "of their last known fossil occurrence.",
	{ param => 'time_reso', valid => '1.2:occs:div_reso', alias => 'reso', default => 'stage' },
	    "This parameter specifies the temporal resolution at which to count.  If not",
	    "specified, it defaults to C<stage>.  You can also use the parameter name",
	    "F<reso>.  Accepted values are:");
    
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
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ require_any => ['1.2:main_selector', '1.2:interval_selector', '1.2:ma_selector',
			  '1.2:common:select_occs_crmod', '1.2:common:select_occs_ent'] },
	">>The following parameters can be used to filter the selection of occurrences",
	"that are analyzed for diversity:",
	{ optional => 'pres', valid => '1.2:taxa:preservation', list => ',' },
	    "This parameter indicates whether to select occurrences that are identified as",
	    "ichnotaxa, form taxa, or regular taxa.  The default is C<B<all>>, which will select",
	    "all records that meet the other specified criteria.  You can specify one or more",
	    "of the following values as a list:",
	{ ignore => 'level' },
	{ ignore => 'show' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:checkdiv_params' =>
	{ param => 'list', valid => ANY_VALUE },
	    "The value of this parameter must be the name of a time interval or two",
	    "interval names separated by a hyphen. For each interval from the corresponding",
	    "diversity output that lies within this range, the output will include a list",
	    "of all counted and implied taxa.",
	{ param => 'diag', valid => ANY_VALUE },
	    "The value of this parameter must be either a time interval or two interval",
	    "names separated by a hyphen, or else one or more occurrence identifiers",
	    "as a comma-separated list.",
	    "The output will be a list of all occurrences from the specified time range or",
	    "else all occurrences matching one or more of the specified identifiers. The",
	    "output will report whether each occurrence was counted, and the taxon name",
	    "under which it was counted. Under certain time rules, an occurrence may be",
	    "counted under more than one interval.",
	{ at_most_one => [ 'dump', 'diag' ] });
    
    $ds->define_ruleset('1.2:occs:checkdiv' =>
	"One of the following parameters is required:",
	{ require => '1.2:occs:checkdiv_params' },
	"The following parameters specify what to count and at what temporal resolution:",
	{ allow => '1.2:occs:div_params' }, 
        ">>The following parameters select which occurrences to analyze.",
	"Except as noted below, you may use these in any combination.",
	"All of these parameters can be used with L<occs/list|node:occs/list> as well, to retrieve",
	"the exact list of occurrences used to compute this diversity tabulation.  Note, however, that",
	"some occurrences may be skipped when tabulating diversity because they are imprecisely",
	"characterized temporally or taxonomically.",
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ require_any => ['1.2:main_selector', '1.2:interval_selector', '1.2:ma_selector',
			  '1.2:common:select_occs_crmod', '1.2:common:select_occs_ent'] },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:quickdiv_params' =>
	{ param => 'count', valid => '1.2:occs:quickdiv_count', default => 'genera' },
	    "This parameter specifies the taxonomic level at which to count.  If not",
	    "specified, it defaults to C<genera>.  The accepted values are:",
	{ param => 'time_reso', valid => '1.2:occs:div_reso', alias => 'reso', default => 'stage' },
	    "This parameter specifies the temporal resolution at which to count.  If not",
	    "specified, it defaults to C<stage>.  You can also use the parameter name",
	    "F<reso>.  Accepted values are:");
    
    $ds->define_ruleset('1.2:occs:quickdiv' =>
	"The following parameters specify what to count and at what temporal resolution:",
	{ allow => '1.2:occs:quickdiv_params' }, 
        ">>The following parameters select which occurrences to analyze.",
	"Except as noted below, you may use these in any combination.",
	"All of these parameters can be used with L<occs/list|node:occs/list> as well, to retrieve",
	"the list of occurrences used to compute this diversity tabulation.  Note, however, that",
	"some occurrences may be skipped when tabulating diversity because they are imprecisely",
	"characterized temporally or taxonomically.",
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	#{ require_any => ['1.2:main_selector',
	#		  '1.2:common:select_crmod', '1.2:common:select_ent'] },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:prev_selector' =>
	{ param => 'coll_id', valid => VALID_IDENTIFIER('COL'), list => ',' },
	    "Show the prevalence of taxa across the listed collections.",
	{ param => 'all_records', valid => FLAG_VALUE },
	    "Show the prevalence of taxa across all of the occurrences in the database");
    
    $ds->define_ruleset('1.2:occs:prevalence' =>
	# ">>You can use the following parameter to select how detailed you wish the result to be:",
	# { optional => 'detail', valid => POS_VALUE },
	#     "Accepted values for this parameter are 1, 2, and 3.  Higher numbers differentiate",
	#     "the results more finely, displaying lower-level taxa.",
	">>The parameters accepted by this operation are the same as those accepted by",
	"L<node:occs/list>.  Except as noted, you can use them in any combination.",
	{ allow => '1.2:occs:prev_selector' },
    	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ require_any => ['1.2:occs:prev_selector',
			  '1.2:main_selector', '1.2:interval_selector', '1.2:ma_selector',
			  '1.2:common:select_occs_crmod', '1.2:common:select_occs_ent'] },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:refs' =>
	"You can use the following parameter if you wish to retrieve the references corresponding",
	"to the entire set of occurrence records entered in this database.  Please use this with care,",
	"since the result set will contain more than 50,000 records and will be at least 10 megabytes in size.",
	"You may also select subsets of this list by specifying some combination of the parameters listed below.",
    	{ allow => '1.2:occs:all_records' },
	">>The following B<very important parameter> allows you to select references that",
	"have particular relationships to the taxa they mention, and skip others:",
	{ optional => 'ref_type', valid => '1.2:taxa:refselect', alias => 'select', list => ',',
	  bad_value => '_' },
	    "You can use this parameter to specify which kinds of references to retrieve.",
	    "The default is C<B<occs>>, which selects only those references from which",
	    "occurrences were entered.",
	    "The value of this attribute can be one or more of the following, separated by commas:",
        ">>The following parameters can be used to query for occurrences by a variety of criteria.",
	"Except as noted below, you may use these in any combination.  If you do not specify B<C<all_records>>,",
	"you must specify at least one selection parameter from the following list.",
	{ allow => '1.2:occs:selector' },
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:occs:all_records', '1.2:occs:selector', '1.2:main_selector', 
			  '1.2:interval_selector', '1.2:ma_selector' ] },
	{ ignore => 'level' },
	">>You can use the following parameters to filter the result set based on attributes",
	"of the bibliographic references.  If you wish to use one of them and have not specified",
	"any of the selection parameters listed above, use B<C<all_records>>.",
	{ allow => '1.2:refs:aux_selector' },
	">>The following parameters can also be used to filter the selection.",
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ allow => '1.2:common:select_refs_crmod' },
	{ allow => '1.2:common:select_refs_ent' },
	">>The following parameters can also be used to further filter the selection based on taxonomy:",
	{ allow => '1.2:taxa:occ_aux_filter' },
	"You can also specify any of the following parameters:",
	{ allow => '1.2:refs:display' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:taxabyref' =>
	"You can use the following parameter if you wish to retrieve taxonomic tree corresponding",
	"to the entire set of occurrence records entered in this database.  Please use this with care,",
	"since the result set will contain more than 250,000 records and will be at least 50 megabytes in size.",
	"You may also select subsets of this list by specifying some combination of the parameters listed below.",
    	{ allow => '1.2:occs:all_records' },
	">>The following B<very important parameter> allows you to select references that",
	"have particular relationships to the taxa they mention, and skip others:",
	{ optional => 'ref_type', valid => '1.2:taxa:refselect', alias => 'select', list => ',',
	  bad_value => '_' },
	    "You can use this parameter to specify which kinds of references to retrieve.",
	    "The default is C<B<taxonomy>>, which selects only those references which provide the",
	    "authority and classification opinions for the selected taxa.",
	    "The value of this attribute can be one or more of the following, separated by commas:",
        ">>The following parameters can be used to query for occurrences by a variety of criteria.",
	"Except as noted below, you may use these in any combination.  If you do not specify B<C<all_records>>,",
	"you must specify at least one selection parameter from the following list.",
	{ allow => '1.2:occs:selector' },
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:occs:all_records', '1.2:occs:selector', '1.2:main_selector', 
			  '1.2:interval_selector', '1.2:ma_selector' ] },
	{ ignore => 'level' },
	">>You can use the following parameters to filter the result set based on attributes",
	"of the bibliographic references.  If you wish to use one of them and have not specified",
	"any of the selection parameters listed above, use B<C<all_records>>.",
	{ allow => '1.2:refs:aux_selector' },
	">>The following parameters can also be used to filter the selection.",
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ allow => '1.2:common:select_taxa_crmod' },
	{ allow => '1.2:common:select_taxa_ent' },
	{ allow => '1.2:common:select_refs_crmod' },
	{ allow => '1.2:common:select_refs_ent' },
	">>The following parameters can also be used to further filter the selection based on taxonomy:",
	{ allow => '1.2:taxa:occ_aux_filter' },
	"You can also specify any of the following parameters:",
	{ allow => '1.2:taxa:show' },
	{ allow => '1.2:taxa:order' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:opinions' =>
	"You can use the following parameter if you wish to retrieve the taxonomic opinions corresponding",
	"to the entire set of occurrence records entered in this database.  Please use this with care,",
	"since the result set will contain more than 400,000 records and will be at least 50 megabytes in size.",
	"You may also select subsets of this list by specifying some combination of the parameters listed below.",
    	{ allow => '1.2:occs:all_records' },
	">>The following B<very important parameter> allows you to select which kinds of opinions",
	"you wish to retrieve:",
	{ optional => 'op_type', valid => '1.2:opinions:select', alias => 'select' },
	    "You can use this parameter to retrieve all opinions, or only the classification opinions,",
	    "or only certain types of opinions.  Accepted values include:",
        ">>The following parameters can be used to query for occurrences by a variety of criteria.",
	"Except as noted below, you may use these in any combination.  If you do not specify B<C<all_records>>,",
	"you must specify at least one selection parameter from the following list.",
	{ allow => '1.2:occs:selector' },
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:occs:all_records', '1.2:occs:selector', '1.2:main_selector', 
			  '1.2:interval_selector', '1.2:ma_selector' ] },
	{ ignore => 'level' },
	">>You can use the folowing parameters to filter the result set based on attributes",
	"of the opinions and the bibliographic references from which they were entered.",
	"If you wish to use one of them and have not specified any of the",
	"selection parameters listed above, use B<C<all_records>>.",
	{ allow => '1.2:opinions:filter' },
	{ allow => '1.2:refs:aux_selector' },
	">>The following parameters can also be used to filter the result list based on taxonomy:",
	{ allow => '1.2:taxa:occ_aux_filter' },
	">>The following parameters further filter the list of selected records:",
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ allow => '1.2:common:select_taxa_crmod' },
	{ allow => '1.2:common:select_taxa_ent' },
	{ allow => '1.2:common:select_ops_crmod' },
	{ allow => '1.2:common:select_ops_ent' },
	">>You can use the following parameters specify what information should be returned about each",
	"resulting opinion, and the order in which the results should be returned:",
	{ allow => '1.2:opinions:display' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.",
	">If the parameter C<order> is not specified, the results are sorted alphabetically by",
	"the name of the primary author.");
    
    $ds->define_set('1.2:strata:order' =>
	{ value => 'name' },
	    "Results are ordered by the name of the formation, sorted alphabetically.",
	{ value => 'name.asc', undocumented => 1 },
	{ value => 'name.desc', undocumented => 1 },
	{ value => 'max_ma' },
	    "Results are ordered chronologically by early age bound, oldest to youngest unless you add C<.asc>",
	{ value => 'max_ma.asc', undocumented => 1 },
	{ value => 'max_ma.desc', undocumented => 1 },
	{ value => 'min_ma' },
	    "Results are ordered chronologically by late age bound, oldest to youngest unless you add C<.asc>",
	{ value => 'min_ma.asc', undocumented => 1 },
	{ value => 'min_ma.desc', undocumented => 1 },
	{ value => 'n_occs' },
	    "Results are ordered by the number of occurrences, in descending order unless you add C<.asc>",
	{ value => 'n_occs.asc', undocumented => 1 },
	{ value => 'n_occs.desc', undocumented => 1 });
    
    $ds->define_ruleset('1.2:strata:display' =>
	"You can use the following parameters to select what information you wish to retrieve,",
	"and the order in which you wish to get the records:",
	{ optional => 'show', list => q{,}, valid => '1.2:strata:basic_map' },
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each occurrence.  Its value should be",
	    "one or more of the following, separated by commas:",
	{ optional => 'order', valid => '1.2:strata:order', split => ',', no_set_doc => 1 },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  Accepted values are:",
	    $ds->document_set('1.2:strata:order'),
	    "If no order is specified, results are sorted alphabetically by name.",
	{ ignore => 'level' });
    
    $ds->define_ruleset('1.2:occs:strata' =>
	"You can use the following parameter if you wish to retrieve the stratigraphy",
	"of a known list of occurrences.",
	{ allow => '1.2:occs:id' },
	">>The following parameters can be used to select occurrences by a variety of criteria.",
	"Except as noted below, you may use these in any combination.",
	"These same parameters can all be used to select either occurrences, collections,",
	"or associated strata, references, or taxa.",
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ allow => '1.2:refs:aux_selector' },
	{ require_any => ['1.2:occs:id', '1.2:main_selector', '1.2:interval_selector',
			  '1.2:ma_selector', '1.2:refs:aux_selector',
			  '1.2:common:select_occs_crmod', '1.2:common:select_occs_ent'] },
	">>The following parameters can be used to select extra information you wish to retrieve,",
	"and the order in which you wish to get the records:",
	{ allow => '1.2:strata:display' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
}


# get ( )
# 
# Query for all relevant information about the collection specified by the
# 'id' parameter.  Returns true if the query succeeded, false otherwise.

sub get_occ {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $request->clean_param('occ_id');
    
    die "400 Bad identifier '$id'\n" unless $id and $id =~ /^\d+$/;
    
    # If this is an extended identifier, select the proper parameter based on identifier type.
    
    my $specifier;
    
    if ( ref $id eq 'PBDB::ExtIdent' && $id->{type} eq 'rei' )
    {
	$specifier = "o.reid_no = $id";
    }
    
    else
    {
	$specifier = "o.occurrence_no = $id and latest_ident = true";
    }
    
    # If the parameter 'idtype' is given with the value 'orig', and if a 'rei' identifier was
    # given, we must look up the corresponding occurrence_no value.  Otherwise, we just add 'and
    # reid_no = 0' to select the original identification.
    
    my $idtype = $request->clean_param('idtype');
    
    if ( $idtype eq 'orig' )
    {
	if ( ref $id eq 'PBDB::ExtIdent' && $id->{type} eq 'rei' )
	{
	    my $sql = "SELECT occurrence_no FROM $OCC_MATRIX WHERE reid_no = $id";
	    
	    $request->{ds}->debug_line("$sql\n") if $request->debug;
	    
	    $id = $dbh->selectrow_array($sql);

	    die $request->exception(404, "Not found") unless $id;
	}
	
	$specifier = "o.occurrence_no = $id and o.reid_no = 0";
    }
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'o', cd => 'oc' );
    
    my @raw_fields = $request->select_list;
    my @fields;
    
    foreach my $f ( @raw_fields )
    {
	if ( ref $Taxonomy::FIELD_LIST{$f} eq 'ARRAY' )
	{
	    push @fields, @{$Taxonomy::FIELD_LIST{$f}};
	    $request->add_table($_) foreach (@{$Taxonomy::FIELD_TABLES{$f}});
	}
	
	else
	{
	    push @fields, $f;
	}
    }
    
    my $tables = $request->tables_hash;
    
    my $fields = join(', ', @fields);
    
    $request->adjustCoordinates(\$fields);
    $request->selectPaleoModel(\$fields, $tables) if $fields =~ /PALEOCOORDS/;
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    $request->extid_check;
    
    # Figure out what information we need to determine access permissions.
    
    my ($access_filter, $access_fields) = $request->generateAccessFilter('cc', $tables);
    
    $fields .= $access_fields if $access_fields;
    
    $request->delete_output_field('permissions') unless $access_fields;
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generateJoinList('c', $tables);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $fields, if($access_filter, 1, 0) as access_ok
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c on o.collection_no = c.collection_no
		$join_list
        WHERE $specifier
	GROUP BY o.occurrence_no";
    
    $request->{ds}->debug_line("$request->{main_sql}\n") if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    # Return an error response if we couldn't retrieve the record.
    
    die $request->exception(404, "Not found") unless $request->{main_record};
    
    # Return an error if we could retrieve the record but the user is not authorized to access it.
    
    die $request->exception(403, "Access denied") 
	unless $request->{main_record}{access_ok};
}


# list ( )
# 
# Query the database for basic info about all occurrences satisfying the
# conditions specified by the query parameters.
# 
# Returns true if the fetch succeeded, false if an error occurred.

sub list_occs {

    my ($request, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $tables = $request->tables_hash;
    
    $request->substitute_select( mt => 'o', cd => 'o' );
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = $request->generateMainFilters('list', 'c', $tables);
    push @filters, $request->generateOccFilters($tables, 'o');
    push @filters, $request->generate_ref_filters($tables);
    push @filters, $request->generate_refno_filter('o');
    push @filters, $request->generate_common_filters( { occs => 'o', colls => 'cc', bare => 'o' }, $tables );
    
    # Do a final check to make sure that all records are only returned if
    # 'all_records' was specified.
    
    if ( @filters == 0 )
    {
	die "400 You must specify 'all_records' if you want to retrieve the entire set of records.\n"
	    unless $request->clean_param('all_records');
    }
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    $request->extid_check;
    
    # Figure out what information we need to determine access permissions.
    
    my ($access_filter, $access_fields) = $request->generateAccessFilter('cc', $tables);
    
    $request->delete_output_field('permissions') unless $access_fields;
    
    push @filters, $access_filter;
    
    my $filter_string = join(' and ', @filters);
    
    $request->add_table('oc');
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # By default, we group by occurrence_no.  But if all identifications were
    # selected, we will need to group by reid_no as well.
    
    my $group_expr = "o.occurrence_no";
    $group_expr .= ', o.reid_no' if $tables->{group_by_reid};
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    #$request->add_output_block('1.2:occs:unknown_taxon') if $tables->{unknown_taxon};
    
    my @raw_fields = $request->select_list;
    my @fields;
    # my %taxa_block;
    
    foreach my $f ( @raw_fields )
    {
	if ( ref $Taxonomy::FIELD_LIST{$f} eq 'ARRAY' )
	{
	    # $taxa_block{$f} = 1;
	    push @fields, @{$Taxonomy::FIELD_LIST{$f}};
	    foreach my $t (@{$Taxonomy::FIELD_TABLES{$f}})
	    {
		$request->add_table($t);
	    }
	}
	
	else
	{
	    push @fields, $f;
	}
    }
    
    # If we were requested to lump by genus, we need to modify the query
    # accordingly.  This includes substituting a different GROUP BY expression.
    
    my $taxonres = $request->clean_param('idreso');
    
    if ( $taxonres =~ qr{^lump} )
    {
	$request->delete_output_field('identified_name');
	$request->delete_output_field('identified_rank');
	$request->delete_output_field('identified_no');
	$request->delete_output_field('difference');
	
	if ( $taxonres eq 'lump_gensub' )
	{
	    $group_expr = "o.collection_no, pl.genus_no, pl.subgenus_no";
	    $tables->{lump} = 'subgenus';
	}
	
	else
	{
	    $group_expr = "o.collection_no, pl.genus_no";
	    $tables->{lump} = 'genus';
	}
    }
    
    # Now generate the field list.
    
    my $fields = join(', ', @fields);
    
    $request->adjustCoordinates(\$fields);
    $request->selectPaleoModel(\$fields, $request->tables_hash) if $fields =~ /PALEOCOORDS/;
        
    $fields .= $access_fields if $access_fields;
    
    # Determine the order in which the results should be returned.
    
    my $tt = $tables->{tv} ? 'tv' : 't';
    
    my $order_clause = $request->PB2::CollectionData::generate_order_clause($tables, { at => 'c', bt => 'o', tt => $tt });
    
    unless ( $order_clause )
    {
	$order_clause = defined $arg && $arg eq 'byref' ? 
	    "r.reference_no, o.occurrence_no" : 
		'o.occurrence_no';
    }
    
    # Determine which extra tables, if any, must be joined to the query.  Then
    # construct the query.
    
    my $join_list = $request->generateJoinList('c', $tables);
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c on o.collection_no = c.collection_no
		$join_list
        WHERE $filter_string
	GROUP BY $group_expr
	ORDER BY $order_clause
	$limit";
    
    $request->{ds}->debug_line("$request->{main_sql}\n") if $request->debug;
    
    # Then prepare and execute the main query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


# diversity ( )
# 
# Like 'list', but processes the resulting list of occurrences into a
# diversity matrix.

sub diversity {

    my ($request, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $tables = $request->tables_hash;
    
    # Set up the diversity-computation options
    
    my $options = {};
    
    $options->{timerule} = $request->clean_param('timerule') || 'major';
    $options->{timebuffer} = $request->clean_param('timebuffer');
    $options->{latebuffer} = $request->clean_param('latebuffer');
    $options->{implied} = $request->clean_param('implied');
    $options->{generate_list} = $request->clean_param('list');
    $options->{generate_diag} = $request->clean_param('diag');
    
    # if ( my @occs = $request->clean_param_list('check') )
    
    my $reso_param = $request->clean_param('time_reso');
    
    my %level_map = ( stage => 5, epoch => 4, period => 3 );
    
    $options->{timereso} = $level_map{$reso_param} || 5;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.  We must add 'o' to the table
    # hash, so that the proper identification filter (idtype) will be added to
    # the query.  This will have no effect on the join list, because table 'o'
    # is already part of it.
    
    $tables->{o} = 1;
    
    my @filters = $request->generateMainFilters('list', 'c', $tables);
    push @filters, $request->generateOccFilters($tables, 'o', 1);
    push @filters, $request->generate_common_filters( { occs => 'o', colls => 'cc', bare => 'o' }, $tables );
    # push @filters, PB2::CommonData::generate_crmod_filters($request, 'o', $tables);
    # push @filters, PB2::CommonData::generate_ent_filters($request, 'o', $tables);
    
    # Figure out what information we need to determine access permissions.  We
    # can ignore $access_fields, since we are not displaying either occurrence
    # or collection records.
    
    my ($access_filter, $access_fields) = $request->generateAccessFilter('cc', $tables);
    
    push @filters, $access_filter;
    
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
		  'ei.interval_name as early_name', 'li.interval_name as late_name', 'tv.name as accepted_name');
    
    if ( $request->clean_param('recent') )
    {
	$tables->{v} = 1;
	push @fields, 'v.is_extant';
	$options->{use_recent} = 1;
    }
    
    my $count_what = $request->clean_param('count') || 'genera';
    $count_what = 'genera_plus' if $request->clean_param('subgenera');
    
    my $count_rank;
    
    if ( $count_what eq 'species' )
    {
	push @fields, 'pl.species_no as count_no', 'pl.species as count_name', 
		      'pl.genus_no as implied_no', 'pl.genus as implied_name';
	$options->{count_rank} = 3;
    }
    
    elsif ( $count_what eq 'genera' )
    {
	push @fields, 'pl.genus_no as count_no', 'pl.genus as count_name',
		      'ph.family_no as implied_no', 'ph.family as implied_name';
	$options->{count_rank} = 5;
    }
    
    elsif ( $count_what eq 'genera_plus' )
    {
	push @fields, 'if(pl.subgenus_no, pl.subgenus_no, pl.genus_no) as count_no', 
		      'if(pl.subgenus_no, pl.subgenus, pl.genus) as count_name',
		      'ph.family_no as implied_no', 'ph.family as implied_name';
	$options->{count_rank} = 5;
    }
    
    elsif ( $count_what eq 'families' )
    {
	push @fields, 'ph.family_no as count_no', 'ph.family as count_name',
		      'ph.order_no as implied_no', 'ph.order as implied_name';
	$options->{count_rank} = 9;
    }
    
    elsif ( $count_what eq 'orders' )
    {
	push @fields, 'ph.order_no as count_no', 'ph.order as count_name',
		      'ph.class_no as implied_no', 'ph.class as implied_name';
	$options->{count_rank} = 13;
    }
    
    else
    {
	die "400 unknown value '$count_what' for parameter 'count'\n";
    }
    
    my $fields = join(', ', @fields);
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    $request->extid_check;
    
    # Determine which extra tables, if any, must be joined to the query.  Then
    # construct the query.
    
    my $join_list = $request->generateJoinList('c', $tables);

    my $group_expr = 'o.occurrence_no';
    $group_expr .= ', o.reid_no' if $tables->{group_by_reid};
    
    $request->{main_sql} = "
	SELECT $fields
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c using (collection_no)
		$join_list
        WHERE $filter_string
	GROUP BY $group_expr";
    
    $request->{ds}->debug_line("$request->{main_sql}\n") if $request->debug;
    
    # Then prepare and execute the main query.
    
    my $sth = $dbh->prepare($request->{main_sql});
    $sth->execute();
    
    # Now fetch all of the rows, and process them into a diversity matrix.
    
    $request->generate_diversity_table($sth, $options);
    
    # $request->list_result($result);
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
    
    # If the 'private' parameter was specified, add a warning.
    
    if ( $request->clean_param('private') )
    {
	$request->add_warning("The parameter 'private' does not work with this operation.");
    }
    
    # Determine the taxonomic level at which we should be counting.  If not
    # specified, or if 'genera_plus' was specified, use 'genera'.  Construct
    # the appropriate SQL statement to generate this count.
    
    my $count_what = $request->clean_param('count') || 'genera';
    my $filter_expr = join(' and ', @filters);
    
    my $scale_id = $request->clean_param('scale_id') || 1;
    my $reso = $request->clean_param('time_reso');
    
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
    
    my ($max_ma, $min_ma, $early_interval_no, $late_interval_no) = $request->process_interval_params;
    
    # my @interval_nos = $request->safe_param_list('interval_id');
    # my $interval_name = $request->clean_param('interval');
    # my $min_ma = $request->clean_param('min_ma');
    # my $max_ma = $request->clean_param('max_ma');
    # my $age_clause = ''; my $age_join = '';
    
    # if ( @interval_nos )
    # {
    # 	my $no = $interval_no + 0;
    # 	my $sql = "SELECT early_age, late_age FROM interval_data WHERE interval_no = $no";
	
    # 	my ($max_ma, $min_ma) = $dbh->selectrow_array($sql);
	
    # 	unless ( $max_ma )
    # 	{
    # 	    $max_ma = 0;
    # 	    $min_ma = 0;
    # 	    $request->add_warning("unknown interval id '$interval_no'");
    # 	}
	
    # 	$age_clause = "and i.early_age <= $max_ma and i.late_age >= $min_ma";
    # 	$age_join = "join interval_data as i using (interval_no)";
    # }
    
    # elsif ( $interval_name )
    # {
    # 	my $name = $dbh->quote($interval_name);
    # 	my $sql = "SELECT early_age, late_age FROM interval_data WHERE interval_name like $name";
	
    # 	my ($max_ma, $min_ma) = $dbh->selectrow_array($sql);
	
    # 	unless ( $max_ma )
    # 	{
    # 	    $max_ma = 0;
    # 	    $min_ma = 0;
    # 	    $request->add_warning("unknown interval '$interval_name'");
    # 	}
	
    # 	$age_clause = "and i.early_age <= $max_ma and i.late_age >= $min_ma";
    # 	$age_join = "join interval_data as i using (interval_no)";
    # }
    
    # elsif ( $min_ma || $max_ma )
    # {
    # 	$max_ma += 0;
    # 	$min_ma += 0;
	
    # 	$age_clause .= "and i.early_age <= $max_ma " if $max_ma > 0;
    # 	$age_clause .= "and i.late_age >= $min_ma" if $min_ma > 0;
    # 	$age_join = "join interval_data as i using (interval_no)";
    # }
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    $request->extid_check;
    
    # Now generate the appropriate SQL expression based on what we are trying
    # to count.
    
    my $age_clause = ''; my $age_join = '';
    
    my $main_table = $tables->{use_global} ? $DIV_GLOBAL : $DIV_MATRIX;
    my $other_joins = $request->generateQuickDivJoins('d', $tables, $taxonomy);
    
    if ( $count_what eq 'genera' || $count_what eq 'genera_plus' || $count_what eq 'genus' ||
	 $count_what eq 'genus_plus' )
    {
	$sql = "SELECT d.interval_no, count(distinct d.genus_no) as sampled_in_bin, sum(d.n_occs) as n_occs
		FROM $main_table as d JOIN $SCALE_MAP as sm using (interval_no) $age_join
			$other_joins
		WHERE $filter_expr and sm.scale_no = $scale_id and sm.scale_level = $reso
		GROUP BY interval_no";

		$request->add_warning("The option 'genera_plus' is not supported with '/occs/quickdiv'.  If you want to promote subgenera to genera, use the operation '/occs/diversity' instead.") if $count_what eq 'genera_plus';
    }
    
    elsif ( $count_what eq 'families' || $count_what eq 'family' )
    {
	$sql = "SELECT d.interval_no, count(distinct ph.family_no) as sampled_in_bin, sum(d.n_occs) as n_occs
		FROM $main_table as d JOIN $SCALE_MAP as sm using (interval_no) $age_join
			JOIN $INTS_TABLE as ph using (ints_no)
			$other_joins
		WHERE $filter_expr and sm.scale_no = $scale_id and sm.scale_level = $reso $age_clause
		GROUP BY interval_no";
    }
    
    elsif ( $count_what eq 'orders' || $count_what eq 'order' )
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
    
    my $age_limit = '';
    $age_limit .= " and early_age <= $max_ma" if defined $max_ma && $max_ma > 0;
    $age_limit .= " and late_age >= $min_ma" if defined $min_ma && $min_ma > 0;
    
    my $outer_sql = "
		SELECT interval_no, interval_name, early_age, late_age, d.sampled_in_bin, d.n_occs
		FROM $INTERVAL_DATA JOIN $SCALE_MAP as sm using (interval_no)
		    LEFT JOIN ($sql) as d using (interval_no)
		WHERE sm.scale_no = $scale_id and sm.scale_level = $reso $age_limit
		ORDER BY early_age";
    
    $request->{ds}->debug_line("$outer_sql\n") if $request->debug;
    
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

sub list_occs_taxa {

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
    
    my $resolution = $request->clean_param('reso') || 'species';
    my $count_what = $request->clean_param('count') || 'species';
    
    $request->{my_promote} = $request->clean_param('subgenera');
    $request->{my_attr} = $request->has_block('attr');

    # $$$ add 'variant=all' as an option?  We need to be as compatible as we
    # can be with taxa/list.  This option would show all variants of every
    # name that is found, in case people want to search through the list for a
    # known name that happens to be non-current.
    
    # Determine the necessary set of query fields.
    
    my @fields = ('o.occurrence_no', 't.orig_no', 
		  'tv.rank', 'tv.name as ident_name', 
		  'tv.ints_no', 'ph.phylum', 'ph.phylum_no', 
		  'ph.class', 'ph.class_no', 'ph.order', 'ph.order_no');
    
    # my @fields = ('o.occurrence_no', 't.taxon_no', 't.orig_no', 
    # 		  't.rank as taxon_rank', 't.name as taxon_name', 
    # 		  't.accepted_no', 'tv.rank as accepted_rank', 'tv.name as accepted_name',
    # 		  'tv.ints_no', 'ph.phylum', 'ph.phylum_no', 
    # 		  'ph.class', 'ph.class_no', 'ph.order', 'ph.order_no');
    
    my $taxon_status = $request->clean_param('taxon_status');
    
    # If we were asked for the block 'occapp', then we need age ranges for the
    # individual occurrences.
    
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
    # in order to select the proper result set.  We must add 'o' to the table
    # hash, so that the proper identification filter (idtype) will be added to
    # the query.
    
    $tables->{o} = 1;
    
    my @filters = $request->generateMainFilters('list', 'c', $tables);
    push @filters, $request->generateOccFilters($tables, 'o');
    push @filters, $request->generate_common_filters( { occs => 'o', colls => 'cc', bare => 'o' }, $tables );
    # push @filters, PB2::CommonData::generate_crmod_filters($request, 'o', $tables);
    # push @filters, PB2::CommonData::generate_ent_filters($request, 'o', $tables);
    
    # Figure out what information we need to determine access permissions.  We
    # can ignore $access_fields, because we are not generating occurrence or
    # collection records.
    
    my ($access_filter, $access_fields) = $request->generateAccessFilter('cc', $tables);
    
    push @filters, $access_filter, "o.orig_no <> 0";
    
    my $filter_string = join(' and ', @filters);
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    $request->extid_check;
    
    # Determine which extra tables, if any, must be joined to the query.  Then
    # construct the query.
    
    my $join_list = $request->generateJoinList('c', $tables);

    my $group_expr = 'o.occurrence_no';
    $group_expr .= ', o.reid_no' if $tables->{group_by_reid};
    
    $request->{main_sql} = "
	SELECT $fields
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c on o.collection_no = c.collection_no
		$join_list
        WHERE $filter_string
	GROUP BY $group_expr";
    
    $request->{ds}->debug_line("$request->{main_sql}\n") if $request->debug;
    
    # Then prepare and execute the main query.
    
    my $sth = $dbh->prepare($request->{main_sql});
    $sth->execute();
    
    # Remove unnecessary output fields.
    
    $request->delete_output_field('exclude');
    
    # Now fetch all of the rows, and process them into a phylogenetic tree.
    # If the set of occurrences was generated from a base taxon, then we can
    # easily create a full phylogeny.  Otherwise, we generate an abbreviated
    # one using the information from the taxon_ints table.
    
    if ( ref $request->{my_base_taxa} eq 'ARRAY' && @{$request->{my_base_taxa}} )
    {
	$request->generate_taxon_table_full($sth, $taxon_status, $request->{my_base_taxa});
    }
    
    else
    {
	$request->generate_taxon_table_ints($sth, $taxon_status);
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
    
    # If the 'private' parameter was specified, add a warning.
    
    if ( $request->clean_param('private') )
    {
	$request->add_warning("The parameter 'private' does not work with this operation.");
    }
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.  First see if we can generate
    # a simple (and quick) expression based on the request parameters.
    
    my $tables = { };
    
    my @filters = $request->generateQuickDivFilters('p', $tables);
    
    if ( @filters )
    {
	# Add an interval filter if one was specified.
	
	my $interval_name = $request->clean_param('interval');
	my $interval_string;
	
	if ( $interval_name )
	{
	    my $quoted = $dbh->quote($interval_name);
	    
	    my $sql = "
		SELECT interval_no FROM $INTERVAL_DATA
		WHERE interval_name like $quoted";
	    
	    ($interval_string) = $dbh->selectrow_array($sql);
	    
	    unless ( $interval_string )
	    {
		$interval_string = -1;
		$request->add_warning("unknown interval '$interval_name'");
	    }
	}
	
	else
	{
	    my @interval_nos = $request->safe_param_list('interval_id');
	    $interval_string = join(',', @interval_nos);
	}
	
	$interval_string ||= '751';
	push @filters, "p.interval_no in ($interval_string)";
	
	# If the 'strict' parameter was given, make sure we haven't generated any
	# warnings. 
	
	$request->strict_check;
	$request->extid_check;
	
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
	
	$request->{ds}->debug_line("$request->{main_sql}\n") if $request->debug;
	
	my $result = $dbh->selectall_arrayref($request->{main_sql}, { Slice => {} });
	
	$request->generate_prevalence($result, $limit, $detail);
	return;
    }
    
    # If the simple filters don't work, we must generate an expression linking
    # to the summary table.
    
    $tables = { };
    
    @filters = $request->generateMainFilters('summary', 's', $tables);
    push @filters, $request->generateOccFilters($tables, 'o', 1);
    push @filters, $request->generate_common_filters( { occs => 'o', colls => 'cc', bare => 'o' }, $tables );
    # push @filters, $request->generate_crmod_filters('o', $tables);
    # push @filters, $request->generate_ent_filters('o', $tables);
    
    #$request->add_table('oc');
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.  If the given parameters can be fulfilled by just querying
    # for summary bins, we do so.  Otherwise, we have to go through the entire
    # set of occurrences again.  In this case, we must include 'o' in the table
    # hash, so that the proper identification filter (idtype) is added to the
    # query.
    
    if ( $tables->{o} || $tables->{cc} || $tables->{c} || $tables->{non_summary} )
    {
	$tables = { o => 1 };
	
	@filters = $request->generateMainFilters('list', 'c', $tables);
	push @filters, $request->generateOccFilters($tables, 'o', 1);
	push @filters, $request->generate_common_filters( { occs => 'o', colls => 'cc', bare => 'o' }, $tables );
	# push @filters, $request->generate_crmod_filters('o', $tables);
	# push @filters, $request->generate_ent_filters('o', $tables);
	
	my $fields = "ph.phylum_no, ph.class_no, ph.order_no, count(*) as n_occs";
	
	$tables->{t} = 1;
	$tables->{ph} = 1;
	
	push @filters, "c.access_level = 0";
	# @filters = grep { $_ !~ qr{^s.interval_no} } @filters;
	
	my $filter_string = join(' and ', @filters);
	
	# If the 'strict' parameter was given, make sure we haven't generated any
	# warnings. 
	
	$request->strict_check;
	$request->extid_check;
	
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
	
	$request->{ds}->debug_line("$request->{main_sql}\n") if $request->debug;
	
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
	# If the 'strict' parameter was given, make sure we haven't generated any
	# warnings. 
	
	$request->strict_check;
	$request->extid_check;
	
	# Construct and execute the necessary SQL statement.
	
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
	
	$request->{ds}->debug_line("$request->{main_sql}\n") if $request->debug;
	
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

sub list_occs_associated {

    my ($request, $record_type) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $tables = $request->tables_hash;
    
    $request->substitute_select( mt => 'r', cd => 'r' );
    
    # $request->delete_output_field('n_opinions');
    
    # First figure out if we just want occurrence/collection references, or if
    # we also want taxonomy references.
    
    my @select = $request->clean_param_list('ref_type');
    my ($sql, $use_taxonomy, %select);
    
    foreach my $s ( @select )
    {
	$use_taxonomy = 1 if $s ne 'occs' && $s ne 'colls' && $s ne 'specs';
	$select{$s} = 1;
    }
    
    $use_taxonomy = 1 if $record_type eq 'taxa' || $record_type eq 'opinions';
    $select{occs} = 1 unless %select;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.  We must include table 'o', so
    # that the proper identification filter (idtype) will be added to the query.
    
    my $inner_tables = { o => 1 };
    
    my @filters = $request->generateMainFilters('list', 'c', $inner_tables);
    push @filters, $request->generate_ref_filters($tables);
    push @filters, $request->generate_refno_filter('o');
    push @filters, $request->generate_common_filters( { occs => 'o', colls => 'cc', refs => 'r' }, $inner_tables );
    push @filters, $request->generateOccFilters($inner_tables, 'o');
    
    # Figure out what information we need to determine access permissions.  We
    # can ignore $access_fields since we are not generating occurrence or
    # collection records.
    
    my ($access_filter, $access_fields) = $request->generateAccessFilter('cc', $inner_tables);
    
    push @filters, $access_filter;
    
    my $filter_string = join(' and ', @filters);
    
    # If we do want taxonomy references, we must constuct a temporary table of
    # occurrences and pass that to Taxonomy::list_associated.
    
    if ( $use_taxonomy )
    {
	# If the 'strict' parameter was given, make sure we haven't generated any
	# warnings. 
	
	$request->strict_check;
	$request->extid_check;
	
	$dbh->do("DROP TABLE IF EXISTS occ_list");
	$dbh->do("CREATE TEMPORARY TABLE occ_list (
			occurrence_no int unsigned not null primary key,
			taxon_no int unsigned not null,
			orig_no int unsigned not null ) engine=memory");
	
	my $inner_join_list = $request->generateJoinList('c', $inner_tables);
	
	try {
	    $sql = "
		INSERT IGNORE INTO occ_list
		SELECT o.occurrence_no, o.taxon_no, o.orig_no FROM $OCC_MATRIX as o
			JOIN $COLL_MATRIX as c using (collection_no)
			$inner_join_list
		WHERE $filter_string";
	
	    $dbh->do($sql);
	}
	
	catch {
	    $dbh->do("DROP TEMPORARY TABLE IF EXISTS occ_list");
	    die $_;
	}
	
	finally {
	    $request->{ds}->debug_line("$sql\n") if $request->debug;
	};
	
	my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
	
	# Then generate a set of query options based on the request parameters.
	# This routine will only take into account parameters relevant to
	# selecting taxa.
	
	my $options = PB2::TaxonData::generate_query_options($request, $record_type);
	
	# We need to remove the options 'min_ma' and 'max_ma' if they were
	# specified, because these overlap with the parameters of the same name
	# used to select occurrences and have already been taken care of above.
	
	delete $options->{min_ma};
	delete $options->{max_ma};
	
	# If debug mode is turned on, generate a closure which will be able to output debug
	# messages. 
	
	if ( $request->debug )
	{
	    $options->{debug_out} = sub {
		$request->{ds}->debug_line($_[0]);
	    };
	}
	
	# Indicate that we want a DBI statement handle in return, and that we will
	# be using the table 'occ_list'.
	
	$options->{return} = 'stmt';
	$options->{table} = 'occ_list';
	
	try {
	    my ($result) = $taxonomy->list_associated('occs', $request->{my_base_taxa}, $options);
	    my @warnings = $taxonomy->list_warnings;
	    
	    $request->sth_result($result) if $result;
	    $request->add_warning(@warnings) if @warnings;
	}
	
	catch {
	    die $_;
	}
	
	finally {
	    $dbh->do("DROP TABLE IF EXISTS occ_list");
	    $request->{ds}->debug_line($taxonomy->last_sql . "\n") if $request->debug;
	};
	
	$request->set_result_count($taxonomy->last_rowcount) if $options->{count};
	return;
    }
    
    # Otherwise, we can construct a query ourselves.
    
    else
    {
	$request->delete_output_field('n_auth');
	$request->delete_output_field('n_class');
	$request->delete_output_field('n_unclass');
	
	# If a query limit has been specified, modify the query accordingly.
	
	my $limit = $request->sql_limit_clause(1);
	
	# If we were asked to count rows, modify the query accordingly
	
	my $calc = $request->sql_count_clause;
	
	# Determine which fields and tables are needed to display the requested
	# information.
	
	my $fields = $request->select_string;
	
	$request->adjustCoordinates(\$fields);
	
	my $inner_join_list = $request->generateJoinList('c', $inner_tables);
	my $outer_join_list = $request->PB2::ReferenceData::generate_join_list($request->tables_hash);
	
	# Construct another set of filter expressions to act on the references.
	
	my @ref_filters = $request->generate_ref_filters($request->tables_hash);
	push @ref_filters, $request->generate_common_filters( { refs => 'r', occs => 'ignore' } );
	push @ref_filters, "1=1" unless @ref_filters;
	
	my $ref_filter_string = join(' and ', @ref_filters);
	
	# Figure out the order in which we should return the references.  If none
	# is selected by the options, sort by rank descending.
	
	my $order = $request->PB2::ReferenceData::generate_order_clause({ rank_table => 's' }) ||
	    "r.author1last, r.author1init, ifnull(r.author2last, ''), ifnull(r.author2init, ''), r.reference_no";
	
	# If the 'strict' parameter was given, make sure we haven't generated any
	# warnings. 
	
	$request->strict_check;
	$request->extid_check;
	
	# Now collect up all of the requested references.
	
	$dbh->do("DROP TABLE IF EXISTS ref_collect");
	
	my $temp = ''; $temp = 'TEMPORARY' unless $Web::DataService::ONE_PROCESS;
	
	$dbh->do("CREATE $temp TABLE ref_collect (
		reference_no int unsigned not null,
		ref_type varchar(10),
		taxon_no int unsigned null,
		occurrence_no int unsigned null,
		specimen_no int unsigned null,
		collection_no int unsigned null,
		UNIQUE KEY (reference_no, ref_type, occurrence_no, specimen_no, collection_no)) engine=memory");
	
	if ( $select{occs} )
	{
	    $sql = "INSERT IGNORE INTO ref_collect
		SELECT o.reference_no, 'O' as ref_type, o.taxon_no, o.occurrence_no, 
			null as specimen_no, null as collection_no
		FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c using (collection_no)
			$inner_join_list
		WHERE $filter_string";
	    
	    $request->{ds}->debug_line("$sql\n") if $request->debug;
	    
	    $dbh->do($sql);
	}
	
	if ( $select{colls} )
	{
	    $sql = "INSERT IGNORE INTO ref_collect
		SELECT c.reference_no, 'P' as ref_type, null as taxon_no, 
			null as occurrence_no, null as specimen_no, c.collection_no
		FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c using (collection_no)
			$inner_join_list
		WHERE $filter_string";
	    
	    $request->{ds}->debug_line("$sql\n") if $request->debug;
	    
	    $dbh->do($sql);
	}
	
	if ( $select{specs} )
	{
	    $sql = "INSERT IGNORE INTO ref_collect
		SELECT ss.reference_no, 'S' as ref_type, ss.taxon_no, null as occurrence_no,
			ss.specimen_no, null as collection_no
		FROM $SPEC_MATRIX as ss JOIN $OCC_MATRIX as o using (occurrence_no)
			JOIN $COLL_MATRIX as c using (collection_no)
			$inner_join_list
		WHERE $filter_string";
	    
	    $request->{ds}->debug_line("$sql\n") if $request->debug;
	    
	    $dbh->do($sql);
	}
	
	$request->{main_sql} = "SELECT $calc $fields, group_concat(distinct ref_type) as ref_type,
			count(distinct taxon_no) as n_reftaxa, 
			count(distinct occurrence_no) as n_refoccs,
			count(distinct specimen_no) as n_refspecs,
			count(distinct collection_no) as n_refcolls
		FROM ref_collect as base
			LEFT JOIN refs as r using (reference_no)
			$outer_join_list
		WHERE $ref_filter_string
		GROUP BY base.reference_no ORDER BY $order $limit";
	
	$request->{ds}->debug_line("$request->{main_sql}\n") if $request->debug;
	
	# Then prepare and execute the main query.
	
	try 
	{
	    $request->{main_sth} = $dbh->prepare($request->{main_sql});
	    $request->{main_sth}->execute();
	}
	
	catch
	{
	    die $_;
	}
	
	finally
	{
	    $dbh->do("DROP TABLE IF EXISTS ref_collect");
	};
	
	# If we were asked to get the count, then do so
	
	$request->sql_count_rows;
    }
}


# strata ( )
# 
# Query the database for the strata associated with occurrences satisfying
# the conditions specified by the parameters.

sub list_occs_strata {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    $request->substitute_select( mt => 'r', cd => 'r' );
    
    $request->delete_output_field('n_opinions');
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.  We must include table 'o' so
    # that the proper identification filter (idtype) is added to the query.
    
    my $inner_tables = { o => 1 };
    
    my @filters = $request->generateMainFilters('list', 'c', $inner_tables);
    push @filters, $request->generate_common_filters( { occs => 'o', colls => 'cc' }, $inner_tables );
    push @filters, $request->generate_ref_filters($inner_tables);
    push @filters, $request->generate_refno_filter('o');
    # push @filters, PB2::CommonData::generate_crmod_filters($request, 'o');
    # push @filters, PB2::CommonData::generate_ent_filters($request, 'o');
    push @filters, $request->generateOccFilters($inner_tables, 'o');
    
    # Figure out what information we need to determine access permissions.  We
    # can ignore $access_fields since we are not generating occurrence or
    # collection records.
    
    my ($access_filter, $access_fields) = $request->generateAccessFilter('cc', $inner_tables);
    
    push @filters, $access_filter;
    
    my $filter_string = join(' and ', @filters);
    
    # Figure out the order in which we should return the strata.  If none
    # is selected by the options, sort by name ascending.
    
    my $order_clause = $request->PB2::CollectionData::generate_strata_order_clause({ rank_table => 's' }) ||
	"coalesce(cs.grp, cs.formation, cs.member), cs.formation, cs.member";
    
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
    
    my $fields = $request->select_string;
    
    $request->adjustCoordinates(\$fields);
    
    delete $inner_tables->{cs};
    
    my $join_list = $request->generateJoinList('c', $inner_tables);
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM $OCC_MATRIX as o JOIN $COLL_MATRIX as c using (collection_no)
		JOIN $COLL_STRATA as cs using (collection_no)
		$join_list
        WHERE $filter_string
	GROUP BY cs.grp, cs.formation, cs.member
	ORDER BY $order_clause
	$limit";
    
    $request->{ds}->debug_line("$request->{main_sql}\n") if $request->debug;
    
    # Then prepare and execute the main query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


# generateOccFilters ( tables_ref, table_name, conditional )
# 
# Generate a list of filter clauses that will be used to compute the
# appropriate result set.  This routine handles only parameters that are specific
# to occurrences.
# 
# Any additional tables that are needed will be added to the hash specified by
# $tables_ref.

sub generateOccFilters {

    my ($request, $tables_ref, $tn) = @_;
    
    my $dbh = $request->{dbh};
    my @filters;

    $tn ||= 'o';
    
    # Check for parameter 'occ_id'
    
    if ( my @occs = $request->safe_param_list('occ_id') )
    {
	my $id_list = $request->check_values($dbh, \@occs, 'occurrence_no', 'occurrences', 
					     "Unknown occurrence '%'");
	
	push @filters, "$tn.occurrence_no in ($id_list)";
	$tables_ref->{$tn} = 1;
	$tables_ref->{non_summary} = 1;

	if ( $id_list eq '-1' )
	{
	    $request->add_warning("no valid occurrence identifiers were given");
	}
    }
    
    # Then check for 'idqual', 'idmod', 'idgenmod' and 'idspcmod'.
    
    my $idqual = $request->clean_param('idqual');
    my $idmod = $request->clean_param('idmod');
    my $idgen = $request->clean_param('idgenmod');
    my $idspc = $request->clean_param('idspcmod');
    
    # If any of these parameters are given, add the appropriate filters. If
    # $tn is 'ss', substitute 'o'. This is because the specimen matrix doesn't
    # have the fields 'genus_reso', etc.  These are only in 'o'.
    
    if ( $idqual || $idmod || $idspc || $idgen )
    {
	my $idtn = $tn eq 'ss' ? 'o' : $tn;
	push @filters, $request->PB2::OccurrenceData::generateIdentFilters($idtn, $idqual, $idmod, $idgen, $idspc);
	$tables_ref->{$idtn} = 1;
	$tables_ref->{non_summary} = 1;
    }
    
    # Now check for 'abundance'. This requires the table 'oc', since abundance
    # data is not stored anywhere else.
    
    if ( my $abundance = $request->clean_param('abundance') )
    {
	$tables_ref->{oc} = 1;
	$tables_ref->{non_summary} = 1;
	
	my $abund_min;
	
	if ( $abundance =~ qr{ ^ ( \w+ ) \s* [:] \s* ( .* ) $ }xs )
	{
	    $abundance = lc $1;
	    $abund_min = $2;
	}
	
	else
	{
	    $abundance = lc $abundance;
	}
	
	if ( $abundance eq 'count' )
	{
	    push @filters, "oc.abund_unit in ('individuals', 'specimens', 'elements', 'fragments', 'grid-count')";
	}
	
	elsif ( $abundance eq 'coverage' )
	{
	    push @filters, "oc.abund_unit like '\\%%'";
	}
	
	elsif ( $abundance eq 'any' )
	{
	    push @filters, "oc.abund_unit <> ''";
	}
	
	elsif ( $abundance )
	{
	    die $request->exception(400, "parameter 'abundance': unknown type '$abundance'");
	}
	
	if ( defined $abund_min && $abund_min ne '' )
	{
	    die $request->exception(400, "parameter 'abundance': '$abund_min' is not a positive integer")
		unless $abund_min =~ qr{ ^ \d+ $ }xs;
	    
	    push @filters, "oc.abund_value >= $abund_min";
	}
    }
    
    # At the very end, we check for parameter 'idtype'.  In cases of reidentified occurrences, it
    # specifies which identifications should be returned.  The default is 'latest', but this
    # should only be applied if table $tn is already part of the query.  The summary tables are
    # already computed using the filter "latest_ident = true", so it would needlessly slow down
    # the query to specifically add this by default.
    
    my $idtype = $request->clean_param('idtype') || 'latest';
    
    if ( $idtype eq 'latest' )
    {
	if ( $tables_ref->{$tn} )
	{
	    push @filters, "$tn.latest_ident = true";
	}

	else
	{
	    # If table $tn is not already in the query do nothing, because the query either does
	    # not refer to occurrences at all or else it will use summary tables which have
	    # already been computed using latest_ident = true.
	}
    }
    
    elsif ( $idtype eq 'orig' )
    {
	push @filters, "$tn.reid_no = 0";
	$tables_ref->{non_summary} = 1;
	$tables_ref->{$tn} = 1;
    }
    
    elsif ( $idtype eq 'reid' )
    {
	push @filters, "($tn.reid_no > 0 or ($tn.reid_no = 0 and $tn.latest_ident = false))";
	$tables_ref->{group_by_reid} = 1;
	$tables_ref->{non_summary} = 1;
	$tables_ref->{$tn} = 1;
    }
    
    else # ( $idtype eq 'all' )
    {
	# no filter is needed, just select all records
	$tables_ref->{group_by_reid} = 1;
	$tables_ref->{non_summary} = 1;
	$tables_ref->{$tn} = 1;
    }
    
    return @filters;
}


our $IDENT_UNCERTAIN = "'aff.', 'cf.', '?', '\"', 'sensu lato', 'informal'";

# generateIdentFilters ( tn, idstr, idspc, idgen )
# 
# Generate filters using the table name given by $tn, from the values of $idtype, $idspc and
# $idgen. 

sub generateIdentFilters {
    
    my ($request, $tn, $idqual, $idmod, $idgen, $idspc) = @_;
    
    my @filters;
    
    if ( $idqual )
    {
	if ( $idqual eq 'certain' || $idqual eq 'genus_certain' )
	{
	    push @filters, "$tn.genus_reso not in ($IDENT_UNCERTAIN)";
	    push @filters, "$tn.subgenus_reso not in ($IDENT_UNCERTAIN)";
	    push @filters, "$tn.species_reso not in ($IDENT_UNCERTAIN)" if $idqual eq 'certain';
	}
	
	elsif ( $idqual eq 'uncertain' )
	{
	    push @filters, "($tn.genus_reso in ($IDENT_UNCERTAIN) or " .
		"$tn.subgenus_reso in ($IDENT_UNCERTAIN) or $tn.species_reso in ($IDENT_UNCERTAIN))";
	}
	
	elsif ( $idqual eq 'new' )
	{
	    push @filters, "($tn.genus_reso = 'n. gen.' or $tn.subgenus_reso = 'n. subgen.' or $tn.species_reso = 'n. sp.')";
	}
	
	# otherwise $idqual is 'any', so do nothing
    }
    
    if ( $idmod )
    {
	my ($op, $value_str) = $request->id_mod_filter('idmod', $idmod);
	
	if ( $op eq 'in' )
	{
	    push @filters, "($tn.genus_reso in ('$value_str') or " .
		"$tn.subgenus_reso in ('$value_str') or " .
		"$tn.species_reso in ('$value_str'))";
	}
	
	else
	{
	    push @filters, "$tn.genus_reso not in ('$value_str')";
	    push @filters, "$tn.subgenus_reso not in ('$value_str')";
	    push @filters, "$tn.species_reso not in ('$value_str')";
	}
    }
    
    if ( $idspc )
    {
	my ($op, $value_str) = $request->id_mod_filter('idspc', $idspc);
	push @filters, "$tn.species_reso $op ('$value_str')";
    }
    
    if ( $idgen )
    {
	my ($op, $value_str) = $request->id_mod_filter('idgen', $idgen);
	push @filters, "$tn.genus_reso $op ('$value_str')";
    }
    
    return @filters;
}


our (%IDENT_MODIFIER) = ( ns => 'n. sp.', ng => "n. gen.','n. subgen.", af => 'aff.', cf => 'cf.',
			  eg => 'ex gr.', sl => 'sensu lato', if => 'informal',
			  qm => '?', qu => '"' );

our ($IDENT_MOD_LIST) = "'ns', 'ng', 'af', 'cf', 'eg', 'sl', 'if', 'qm', 'qu'";

sub id_mod_filter {
    
    my ($request, $param_name, $modifier_list) = @_;
    
    my $op = 'in';
    my @values;
    
    if ( $modifier_list =~ / ^ [!] (.*) /xs )
    {
	$op = 'not in';
	$modifier_list = $1;
    }
    
    foreach my $code ( split( /\s*,\s*/, $modifier_list ) )
    {
	if ( $IDENT_MODIFIER{$code} )
	{
	    push @values, $IDENT_MODIFIER{$code};
	}
	
	else
	{
	    $request->add_warning("bad value '$code' for parameter '$param_name', must be one of $IDENT_MOD_LIST");
	}
    }
    
    unless ( @values )
    {
	push @values, "SELECT_NOTHING";
    }
    
    return $op, join("','", @values);
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

    my ($request, $mt, $tables_ref) = @_;
    
    my $dbh = $request->get_connection;
    my $taxonomy = $request->{my_taxonomy} ||= Taxonomy->new($dbh, 'taxon_trees');
    my @filters;
    
    # First check for parameters 'formation', 'stratgroup', 'member'.  If any
    # of these are specified, we abort.
    
    return () if $request->clean_param('formation') || $request->clean_param('stratgroup') || $request->clean_param('member');
    
    # Same with 'plate'.
    
    return () if $request->clean_param('plate');
    
    # Same with the date or authorizer parameters.
    
    return () if $request->clean_param('authorized_by') || $request->clean_param('entered_by') || $request->clean_param('modified_by');
    return () if $request->clean_param('created_before') || $request->clean_param('created_after');
    return () if $request->clean_param('modified_before') || $request->clean_param('modified_after');
    
    # Same with coll_id or clusts_id
    
    return () if $request->param_given('coll_id') || $request->param_given('occ_id') ||
	$request->param_given('coll_re');
    
    # Same with ref_id
    
    return () if $request->param_given('ref_id');
    
    # Then check for geographic parameters, including 'clust_id', 'continent',
    # 'country', 'latmin', 'latmax, 'lngmin', 'lngmax', 'loc'
    
    my $clust_id = $request->clean_param('clust_id');
    
    if ( ref $clust_id eq 'ARRAY' && @$clust_id )
    {
	my @clusters = grep { $_ > 0 } @$clust_id;
	push @clusters, -1 unless @clusters;
	my $list = join(q{,}, @clusters);
	push @filters, "$mt.bin_id in ($list)";
    }
    
    if ( my @ccs = $request->clean_param_list('cc') ) # $$$
    {
	my $cc_list = "'" . join("','", @ccs) . "'";
	push @filters, "bl.cc in ($cc_list)";
	$tables_ref->{bl} = 1;
    }
    
    if ( my @continents = $request->clean_param_list('continent') )
    {
	my $cont_list = "'" . join("','", @continents) . "'";
	push @filters, "bl.continent in ($cont_list)";
	$tables_ref->{bl} = 1;
    }
    
    my $x1 = $request->clean_param('lngmin');
    my $x2 = $request->clean_param('lngmax');
    my $y1 = $request->clean_param('latmin');
    my $y2 = $request->clean_param('latmax');
    
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
    
    if ( my $loc = $request->clean_param('loc') )
    {
	push @filters, "contains(geomfromtext($loc), bl.loc)";
	$tables_ref->{bl} = 1;
    }
    
    # At this point, if no geographic parameters have been specified then we
    # indicate to use the $DIV_GLOBAL table instead of $DIV_MATRIX.
    
    $tables_ref->{use_global} = 1 unless @filters;
    
    # Now check for taxonomic filters.
    
    my $taxon_name = $request->clean_param('taxon_name') || $request->clean_param('base_name');
    my @taxon_nos = $request->clean_param_list('taxon_id') || $request->clean_param_list('base_id');
    my @exclude_nos = $request->clean_param_list('exclude_id');
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
	$request->{my_base_taxa} = [ @include_taxa, @exclude_taxa ];
    }
    
    # If a bad taxon name or id was given, add a filter that will exclude
    # everything.  Also make sure that the warning generated by the Taxonomy
    # module is returned to the client.
    
    elsif ( $bad_taxa )
    {
	push @filters, "t.lft = 0";
	$tables_ref->{t} = 1;
	
	my @warnings = $taxonomy->list_warnings;
	$request->add_warning(@warnings);
    }
    
    # Now add filters for excluded taxa.  But only if there is at least one
    # included taxon as well.
    
    if ( @exclude_taxa && @include_taxa )
    {
	push @filters, map { "t.lft not between $_->{lft} and $_->{rgt}" } @exclude_taxa;
	$request->{my_excluded_taxa} = \@exclude_taxa;
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

    my ($request, $mt, $tables) = @_;
    
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
    $join_list .= "JOIN coll_strata as cs on cs.collection_no = c.collection_no\n"
	if $tables->{cs};
    
    if ( $tables->{lump} )
    {
	delete $tables->{pl};
	$join_list .= "LEFT JOIN taxon_lower as pl on pl.orig_no = o.orig_no\n";
	
	if ( $tables->{lump} eq 'subgenus' )	{
	    $join_list .= "LEFT JOIN taxon_trees as t on t.orig_no = coalesce(pl.subgenus_no, pl.genus_no)\n";
	} else {
	    $join_list .= "LEFT JOIN taxon_trees as t on t.orig_no = pl.genus_no\n";
	}
    }
    
    else
    {
	$join_list .= "LEFT JOIN taxon_trees as t on t.orig_no = o.orig_no\n"
	    if $tables->{t};
    }
    
    $join_list .= "LEFT JOIN taxon_trees as tv on tv.orig_no = t.accepted_no\n"
	if $tables->{tv} || $tables->{e};
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
    
    $join_list .= "\t\tLEFT JOIN taxon_ecotaph as e on e.orig_no = tv.orig_no\n"
	if $tables->{e};
    $join_list .= "\t\tLEFT JOIN taxon_etbasis as etb on etb.orig_no = tv.orig_no\n"
	if $tables->{etb};
    
    $join_list .= "LEFT JOIN $COLL_LITH as cl on cl.collection_no = c.collection_no\n"
	if $tables->{cl};
    
    return $join_list;
}


sub generateQuickDivJoins {

    my ($request, $mt, $tables) = @_;
    
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
    
    # Set the flags as appropriate.
    
    $record->{flags} = "R" unless $record->{latest_ident};
    
    if ( $record->{is_trace} || $record->{is_form} )
    {
	$record->{flags} ||= '';
	$record->{flags} .= 'I' if $record->{is_trace};
	$record->{flags} .= 'F' if $record->{is_form};
    }
    
    # Generate the identified name from the occurrence fields.
    
    $request->process_identification($record);
    
    # Now generate the 'difference' field if the accepted name and identified
    # name are different.
    
    $request->process_difference($record);
    
    my $a = 1;	# we can stop here when debugging
}


sub process_occ_subgenus {
    
    my ($request, $record) = @_;
    
    if ( $record->{subgenus} )
    {
	$record->{genus} = $record->{subgenus};
    }
}


sub process_identification {
    
    my ($request, $record) = @_;
    
    # Construct the 'identified_name' field using the '_name' and '_reso'
    # fields from the occurrence record.  Also build 'taxon_name' using just
    # the '_name' fields.
    
    my $ident_name = combine_modifier($record->{genus_name}, $record->{genus_reso}) || 'UNKNOWN';
    my $taxon_name = $record->{genus_name} || 'UNKNOWN';
    
    # $ident_name .= " $record->{genus_reso}" if $record->{genus_reso};
    
    if ( $record->{subgenus_name} )
    {
	$ident_name .= " (" . combine_modifier($record->{subgenus_name}, $record->{subgenus_reso}) . ")";
	# $ident_name .= " $record->{subgenus_reso}" if $record->{subgenus_reso};
	# $ident_name .= ")";
	
	$taxon_name .= " ($record->{subgenus_name})";
    }
    
    if ( $record->{species_name} )
    {
	$ident_name .= " " . combine_modifier($record->{species_name}, $record->{species_reso});
	# $ident_name .= " $record->{species_reso}" if $record->{species_reso};

	$taxon_name .= " $record->{species_name}" if $record->{species_name} !~ /\.$|^[?]$/;
	$taxon_name =~ s/[ ]?[?]$//;
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
    
    my $a = 1;	# we can stop here when debugging
}


sub combine_modifier {
    
    my ($name, $modifier) = @_;
    
    return $name unless defined $modifier && $modifier ne '';
    
    if ( $modifier eq '?' || $modifier eq 'sensu lato' || $modifier eq 'informal' )
    {
	return "$name $modifier";
    }
    
    elsif ( $modifier eq '"' )
    {
	return qq{"$name"};
    }
    
    else
    {
	return "$modifier $name";
    }
}

sub process_difference {
    
    my ($request, $record) = @_;
    
    # If the 'taxon_name' and 'accepted_name' fields are different, then
    # create a 'difference' field.  This may contain one or more relevant reasons.
    # If there is no accepted name, then the taxon was not entered at all.
    
    if ( ! $record->{accepted_name} )
    {
	if ( ! $record->{accepted_no} )
	{
	    $record->{difference} = 'taxon not entered';
	}
	
	else
	{
	    $record->{difference} = 'error';
	}
    }
    
    # If the accepted name exists and is different from the identified name,
    # there will be one or more reasons why.
    
    elsif ( $record->{taxon_name} && $record->{accepted_name} &&
	    $record->{taxon_name} ne $record->{accepted_name} )
    {
	my @reasons;
	
	# my $len = length($record->{accepted_name});
	
	# if ( $record->{accepted_name} eq substr($record->{taxon_name}, 0, $len) &&
	#      $record->{identified_rank} < 5 )
	# {
	#     $record->{taxonomic_reason} = 'taxon not fully entered';
	# }
	
	# If the species was not entered then report that as the primary difference.
	
	if ( defined $record->{identified_rank} && $record->{identified_rank} < 4 &&
	     defined $record->{accepted_rank} && $record->{accepted_rank} >= 4 &&
	     defined $record->{taxon_status} &&
	     ( $record->{taxon_status} eq 'belongs to' || 
	       $record->{taxon_status} eq 'subjective synonym of' ||
	       $record->{taxon_status} eq 'objective synonym of' ||
	       $record->{taxon_status} eq 'replaced by' ||
	       $record->{taxon_status} eq '' ) )
	{
	    push @reasons, $record->{taxon_status} if defined $record->{taxon_status} &&
		$record->{taxon_status} ne 'belongs to' && $record->{taxon_status} ne '';
	    push @reasons, 'species not entered';
	}
	
	# Otherwise, if the orig_no and accepted_no are the same, then the two
	# names are variants.  So try to figure out why they differ.  If we
	# can't find anything else, just report 'variant'.
	
	elsif ( $record->{orig_no} && $record->{accepted_no} && 
	     $record->{orig_no} eq $record->{accepted_no} )
	{
	    if ( $record->{accepted_reason} && $record->{accepted_reason} eq 'recombination' ||
	         $record->{spelling_reason} && $record->{spelling_reason} eq 'recombination' )
	    {
		push @reasons, 'recombined as';
	    }
	    
	    elsif ( $record->{accepted_reason} && $record->{accepted_reason} eq 'reassignment' ||
		    $record->{spelling_reason} && $record->{spelling_reason} eq 'reassignment' )
	    {
		push @reasons, 'reassigned as';
	    }
	    
	    elsif ( $record->{accepted_reason} && $record->{accepted_reason} eq 'correction' &&
		    $record->{spelling_reason} ne 'correction' )
	    {
		push @reasons, 'corrected to';
	    }
	    
	    else
	    {
		push @reasons, 'obsolete variant of';
	    }
	}
	
	# Otherwise, we report the taxonomic status of the identified name as
	# the difference.  If this record is a specimen record, then we may
	# have to override this.
	
	else
	{
	    if ( $record->{taxon_status} && $record->{taxon_status} eq 'belongs to' )
	    {
		if ( $record->{specimen_no} )
		{
		    push @reasons, 'specimen and occurrence identified differently'
		}
		
		else
		{
		    push @reasons, 'error';
		}
	    }
	    
	    else
	    {	    
		push @reasons, $record->{taxon_status};
	    }
	}
	
	# If the identified name is a misspelling, report that right away in 
	# front of any other differences there might be.
	
	if ( $record->{spelling_reason} && $record->{spelling_reason} eq 'misspelling' )
	{
	    unshift @reasons, 'misspelling of';
	}
	
	# Now join all of the reasons together.
	
	$record->{difference} = join(q{, }, grep { defined $_ && $_ ne '' } @reasons);
    }
    
    # Otherwise, the accepted name and identified name are the same so there
    # is no difference.
    
    my $a = 1;	# we can stop here when debugging
}


my %ID_TYPE = ( orig_no => 'TXN',
		identified_no => 'TXN',
		accepted_no => 'TXN',
		kingdom_no => 'TXN',
		phylum_no => 'TXN',
		class_no => 'TXN',
		order_no => 'TXN',
		family_no => 'TXN',
		genus_no => 'TXN',
		synonym_no => 'TXN',
		subgenus_no => 'TXN',
		taxon_no => 'TXN',
		spelling_no => 'VAR',
		interval_no => 'INT',
		occurrence_no => 'OCC',
		specimen_no => 'SPM',
		collection_no => 'COL',
		reid_no => 'REI',
		reference_no => 'REF',
		bin_id_1 => 'CLU',
		bin_id_2 => 'CLU',
		bin_id_3 => 'CLU',
		bin_id_4 => 'CLU', );


sub process_occ_com {
    
    my ($request, $record) = @_;
    
    # Remove duplicate fields.
    
    delete $record->{identified_no} if $record->{identified_no} && $record->{accepted_spelling} &&
	$record->{identified_no} eq $record->{accepted_spelling};
    
    delete $record->{identified_no} if $record->{taxon_name} && $record->{accepted_name} &&
	$record->{taxon_name} eq $record->{accepted_name};
    
    delete $record->{identified_name} if $record->{identified_name} && $record->{accepted_name} &&
	$record->{identified_name} eq $record->{accepted_name};
    
    delete $record->{identified_rank} if $record->{identified_rank} && $record->{accepted_rank} &&
	$record->{identified_rank} eq $record->{accepted_rank};
    
    # foreach my $f ( qw(interval_no) )
    # {
    # 	$record->{$f} = "$IDP{INT}:$record->{$f}" if defined $record->{$f};
    # }
    
    # foreach my $f ( qw(occurrence_no) )
    # {
    # 	$record->{$f} = "$IDP{OCC}:$record->{$f}" if defined $record->{$f};
    # }
    
    # foreach my $f ( qw(collection_no) )
    # {
    # 	$record->{$f} = "$IDP{COL}:$record->{$f}" if defined $record->{$f};
    # }
    
    # foreach my $f ( qw(reid_no) )
    # {
    # 	$record->{$f} = "$IDP{REI}:$record->{$f}" if defined $record->{$f};
    # }
    
    # if ( ref $record->{reference_no} eq 'ARRAY' )
    # {
    # 	map { $_ = "$IDP{REF}:$_" } @{$record->{reference_no}};
    # }
    
    # elsif ( defined $record->{reference_no} )
    # {
    # 	$record->{reference_no} = "$IDP{REF}:$record->{reference_no}";
    # }
    
    # if ( $record->{is_trace} || $record->{is_form} )
    # {
    # 	$record->{preservation} = '';
    # 	$record->{preservation} .= 'I' if $record->{is_trace};
    # 	$record->{preservation} .= 'F' if $record->{is_form};
    # }
}


# Alter all object identifiers from the numeric values stored in the database to the text form
# reported externally.

sub process_occ_ids {
    
    my ($request, $record) = @_;
    
    return unless $request->{block_hash}{extids};
    
    # my $make_ids = $request->clean_param('extids');
    # $make_ids = 1 if ! $request->param_given('extids') && $request->output_vocab eq 'com';
    
    # return unless $make_ids;
    
    # $request->delete_output_field('record_type');
    
    foreach my $f ( qw(orig_no taxon_no accepted_no phylum_no
		       class_no order_no family_no genus_no subgenus_no
		       interval_no specimen_no occurrence_no collection_no
		       reid_no reference_no bin_id_1 bin_id_2 bin_id_3 bin_id_4) )
    {
	$record->{$f} = generate_identifier($ID_TYPE{$f}, $record->{$f})
	    if defined $record->{$f};
    }
    
    if ( defined $record->{identified_no} && defined $record->{spelling_no} &&
	 $record->{identified_no} ne $record->{spelling_no} )
    {
	$record->{identified_no} = generate_identifier('VAR', $record->{identified_no});
    }
    
    else
    {
	$record->{identified_no} = generate_identifier('TXN', $record->{identified_no});
    }
}


sub process_classification {
    
    my ($request, $record) = @_;
    
    return unless $record->{accepted_rank};
    
    foreach my $u ( qw(NP NC NO NF NG) )
    {
	if ( $record->{accepted_rank} =~ /^\d/ )
	{
	    last if $record->{accepted_rank} >= $UNS_RANK{$u};
	}
	
	else
	{
	    last if $TAXON_RANK{$record->{accepted_rank}} >= $UNS_RANK{$u};
	}
	
	$record->{$PB2::TaxonData::UNS_FIELD{$u}} ||= $PB2::TaxonData::UNS_NAME{$u};
	
	if ( $request->{block_hash}{extids} )
	{
	    $record->{$PB2::TaxonData::UNS_ID{$u}} ||= generate_identifier('TXN', $u);
	}
	
	else
	{
	    $record->{$PB2::TaxonData::UNS_ID{$u}} ||= $u;
	}
    }
}

1;
