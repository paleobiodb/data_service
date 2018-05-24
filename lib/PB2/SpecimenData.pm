#  
# SpecimenData
# 
# A role that returns information from the PaleoDB database about a single
# specimen or a category of specimens.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::SpecimenData;

use HTTP::Validate qw(:validators);

use TableDefs qw($OCC_MATRIX $SPEC_MATRIX $COLL_MATRIX $COLL_BINS
		 $BIN_LOC $COUNTRY_MAP $PALEOCOORDS $GEOPLATES $COLL_STRATA
		 $SPECELT_DATA $SPECELT_MAP
		 $INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP $INTERVAL_BUFFER $DIV_GLOBAL $DIV_MATRIX);

use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);

use TaxonDefs qw(%RANK_STRING);

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::ReferenceData PB2::OccurrenceData PB2::TaxonData PB2::CollectionData PB2::IntervalData);


# initialize ( )
# 
# This routine is called by the DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # We start with the basic output block for specimens.
    
    $ds->define_block('1.2:specs:basic' =>
	{ select => [ 'ss.specimen_no', 'sp.specimen_id', 'sp.is_type', 'sp.specimen_side',
		      'sp.specimen_part', 'sp.sex as specimen_sex', 'sp.specimens_measured as n_measured',
		      'sp.measurement_source', 'sp.magnification', 'sp.comments',
		      'sp.occurrence_no', 'ss.reid_no', 'ss.taxon_no as identified_no',
		      'a.taxon_name as identified_name', 'a.orig_no as spec_orig_no',
		      't.rank as identified_rank', 't.status as taxon_status', 't.orig_no',
		      'nm.spelling_reason', 'ns.spelling_reason as accepted_reason',
		      't.spelling_no', 't.accepted_no',
		      'tv.spelling_no as accepted_spelling', 'tv.name as accepted_name', 'tv.rank as accepted_rank',
		      'ei.interval_name as early_interval', 'li.interval_name as late_interval',
		      'o.genus_name', 'o.genus_reso', 'o.subgenus_name', 'o.subgenus_reso',
		      'o.species_name', 'o.species_reso', 'v.is_form', 'v.is_trace',
		      'o.early_age', 'o.late_age', 'sp.reference_no'],
	  tables => [ 'ss', 'o', 't', 'nm', 'ns', 'tv', 'ei', 'li', 'o', 'v' ] },
	{ set => '*', from => '*', code => \&process_basic_record },
	{ set => '*', code => \&PB2::OccurrenceData::process_occ_ids },
	{ output => 'specimen_no', com_name => 'oid' },
	    "The unique identifier of this specimen in the database",
	{ output => 'record_type', com_name => 'typ', value => $IDP{SPM} },
	    "The type of this object: C<$IDP{SPM}> for a specimen.",
	{ output => 'flags', com_name => 'flg' },
	    "This field will be empty for most records.  Otherwise, it will contain one or more",
	    "of the following letters:", "=over",
	    "=item N", "This specimen is not associated with an occurrence.",
	    "=item D", "The identification entered for this specimen is different from the",
	        "identification entered for the corresponding occurrence.",
	    "=item R", "This identification has been superceded by a more recent one.",
		"In other words, this specimen has been reidentified.",
	    "=item I", "This identification is an ichnotaxon",
	    "=item F", "This identification is a form taxon",
	    "This field will be empty for most records.  A record representing a specimen",
	    "not associated with an occurrence will have an C<N> in this field.  A record",
	    "representing a specimen whose identification is different than its associated",
	    "occurrence will have an C<I> in this field.",
	{ output => 'occurrence_no', com_name => 'qid' },
	    "The identifier of the occurrence, if any, with which this specimen is associated",
	{ output => 'reid_no', com_name => 'eid' },
	    "If the associated occurrence was reidentified, a unique identifier for the",
	    "reidentification.",
	{ output => 'collection_no', com_name => 'cid' },
	    "The identifier of the collection, if any, with which this specimen is associated",
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
	{ output => 'specimen_id', com_name => 'smi', data_type => 'str' },
	    "The identifier for this specimen according to its custodial institution",
	{ output => 'is_type', com_name => 'smt' },
	    "Indicates whether this specimen is a holotype or paratype",
	{ output => 'specimen_side', com_name => 'sms' },
	    "The side of the body to which the specimen part corresponds",
	{ output => 'specimen_part', com_name => 'smp' },
	    "The part of the body of which this specimen consists",
	{ output => 'specimen_sex', com_name => 'smx' },
	    "The sex of the specimen, if known",
	{ output => 'n_measured', com_name => 'smn' },
	    "The number of specimens measured",
	{ output => 'measurement_source', com_name => 'mms' },
	    "How the measurements were obtained, if known",
	{ output => 'magnification', com_name => 'mmg' },
	    "The magnification used in the measurement, if known",
	{ output => 'comments', com_name => 'smc' },
	    "Comments on this specimen, often author and publication year",
	{ output => 'identified_name', com_name => 'idn', dwc_name => 'associatedTaxa', not_block => 'acconly' },
	    "The taxonomic name by which this occurrence was identified.  This field will",
	    "be omitted for responses in the compact voabulary if it is identical",
	    "to the value of C<accepted_name>.",
	{ output => 'identified_rank', dwc_name => 'taxonRank', com_name => 'idr', not_block => 'acconly' },
	    "The taxonomic rank of the identified name, if this can be determined.  This field will",
	    "be omitted for responses in the compact voabulary if it is identical",
	    "to the value of C<accepted_rank>.",
	{ set => 'identified_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb', not_block => 'acconly' },
	{ output => 'identified_no', com_name => 'iid', not_block => 'acconly' },
	    "The unique identifier of the identified taxonomic name.  If this is empty, then",
	    "the name was never entered into the taxonomic hierarchy stored in this database and",
	    "we have no further information about the classification of this occurrence.  In some cases,",
	    "the genus has been entered into the taxonomic hierarchy but not the species.  This field will",
	    "be omitted for responses in the compact voabulary if it is identical",
	    "to the value of C<accepted_no>.",
	{ output => 'difference', com_name => 'tdf', not_block => 'acconly' },
	    "If the identified name is different from the accepted name, this field gives",
	    "the reason why.  This field will be present if, for example, the identified name",
	    "is a junior synonym or nomen dubium, or if the species has been recombined, or",
	    "if the identification is misspelled.",
	{ output => 'accepted_name', com_name => 'tna', if_field => 'accepted_no' },
	    "The value of this field will be the accepted taxonomic name corresponding",
	    "to the identified name.",
	{ output => 'accepted_attr', if_block => 'attr', dwc_name => 'scientificNameAuthorship', com_name => 'att' },
	    "The attribution (author and year) of the accepted taxonomic name",
	{ output => 'accepted_rank', com_name => 'rnk', if_field => 'accepted_no' },
	    "The taxonomic rank of the accepted name.  This may be different from the",
	    "identified rank if the identified name is a nomen dubium or otherwise invalid,",
	    "or if the identified name has not been fully entered into the taxonomic hierarchy",
	    "of this database.",
	{ set => 'accepted_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb' },
	{ output => 'accepted_no', com_name => 'tid', if_field => 'accepted_no' },
	    "The unique identifier of the accepted taxonomic name in this database.",
	{ set => '*', code => \&PB2::CollectionData::fixTimeOutput },
	{ output => 'early_age', com_name => 'eag', pbdb_name => 'max_ma' },
	    "The early bound of the geologic time range associated with this occurrence (in Ma)",
	{ output => 'late_age', com_name => 'lag', pbdb_name => 'min_ma' },
	    "The late bound of the geologic time range associated with this occurrence (in Ma)",
	{ output => 'ref_author', dwc_name => 'recordedBy', com_name => 'aut', if_block => '1.2:refs:attr' },
	    "The attribution of the specimen: the author name(s) from",
	    "the specimen reference, and the year of publication.",
	{ output => 'ref_pubyr', com_name => 'pby', if_block => '1.2:refs:attr' },
	    "The year of publication of the reference from which this data was entered",
	{ output => 'reference_no', com_name => 'rid' },
	    "The identifier of the reference from which this data was entered");
    
    # Then the optional output map for specimens.
    
    $ds->define_output_map('1.2:specs:basic_map' =>
	{ value => 'full', maps_to => '1.2:specs:full_info' },
	    "This is a shortcut for including all of the information that defines this record.  Currently, this",
	    "includes the following blocks: B<attr>, B<class>, B<plant>, B<ecospace>, B<taphonomy>",
	    "B<abund>, B<coll>, B<coords>, B<loc>, B<paleoloc>, B<prot>, B<stratext>, B<lithext>,",
	    "B<geo>, B<methods>, B<rem>, B<refattr>.  If we subsequently add new data fields to the",
	    "specimen record, then B<full> will include those as well.  So if you are publishing a URL,",
	    "it might be a good idea to include C<show=full>.",
	{ value => 'acconly' },
	    "Suppress the exact taxonomic identification of each specimen,",
	    "and show only the accepted name.",
	{ value => 'attr', maps_to => '1.2:occs:attr' },
	    "The attribution (author and year) of the accepted name for this specimen.",
	{ value => 'class', maps_to => '1.2:occs:class' },
	    "The taxonomic classification of the specimen: phylum, class, order, family,",
	    "genus.",
	{ value => 'classext', maps_to => '1.2:occs:class' },
	    "Like C<class>, but also includes the relevant taxon identifiers.",
	{ value => 'phylo', maps_to => '1.2:occs:class', undocumented => 1 },
	{ value => 'genus', maps_to => '1.2:occs:genus' },
	    "The genus corresponding to each specimen, if the specimen has been",
	    "identified to the genus level.  This block is redundant if C<class> or",
	    "C<classext> are used.",
	{ value => 'subgenus', maps_to => '1.2:occs:genus' },
	    "The genus corresponding to each specimen, plus the subgenus if any.",
	    "This can be added to C<class> or C<classext> in order to display",
	    "subgenera, or used instead of C<genus> to display both the genus",
	    "and the subgenus if any.",
	{ value => 'plant', maps_to => '1.2:occs:plant' },
	    "The plant organ(s), if any, associated with this specimen.  These fields",
	    "will be empty unless the specimen is a plant fossil.",
	{ value => 'abund', maps_to => '1.2:occs:abund' },
	    "Information about the abundance of the associated occurrence,",
	    "if any, in its collection",
	{ value => 'ecospace', maps_to => '1.2:taxa:ecospace' },
	    "Information about ecological space that this organism occupies or occupied.",
	    "This has only been filled in for a relatively few taxa.  Here is a",
	    "L<list of values|node:general/ecotaph#Ecospace>.",
	{ value => 'taphonomy', maps_to => '1.2:taxa:taphonomy' },
	    "Information about the taphonomy of this organism.  Here is a",
	    "L<list of values|node:general/ecotaph#Taphonomy>.",
	{ value => 'etbasis', maps_to => '1.2:taxa:etbasis' },
	    "Annotates the output block C<ecospace>, indicating at which",
	    "taxonomic level each piece of information was entered.",
	{ value => 'pres', undocumented => 1 },
	    # "Indicates whether the identification of this specimen is a regular",
	    # "taxon, a form taxon, or an ichnotaxon.",
	{ value => 'coll', maps_to => '1.2:colls:name' },
	    "The name of the collection in which the associated occurrence was found, plus any",
	    "additional remarks entered about it.",
	{ value => 'coords', maps_to => '1.2:occs:coords' },
	     "The latitude and longitude of the associated occurrence, if any.",
        { value => 'loc', maps_to => '1.2:colls:loc' },
	    "Additional information about the geographic locality of the",
	    "associated occurrence, if any.",
	{ value => 'paleoloc', maps_to => '1.2:colls:paleoloc' },
	    "Information about the paleogeographic locality of the associated occurrence,",
	    "evaluated according to the model specified by the parameter C<pgm>.",
	{ value => 'strat', maps_to => '1.2:colls:strat' },
	    "Basic information about the stratigraphic context of the associated",
	    "occurrence.",
	{ value => 'stratext', maps_to => '1.2:colls:stratext' },
	    "Detailed information about the stratigraphic context of the associated",
	    "occurrence.",
	    "This includes all of the information from C<strat> plus extra fields.",
	{ value => 'lith', maps_to => '1.2:colls:lith' },
	    "Basic information about the lithological context of the associated",
	    "occurrence.",
	{ value => 'lithext', maps_to => '1.2:colls:lithext' },
	    "Detailed information about the lithological context of the occurrence.",
	    "This includes all of the information from C<lith> plus extra fields.",
	{ value => 'methods', maps_to => '1.2:colls:methods' },
	    "Information about the collection methods used",
	{ value => 'env', maps_to => '1.2:colls:env' },
	    "The paleoenvironment associated with the associated collection, if any.",
	{ value => 'geo', maps_to => '1.2:colls:geo' },
	    "Information about the geological context of the associated occurrence (includes C<env>).",
        { value => 'rem', maps_to => '1.2:colls:rem', undocumented => 1 },
	    "Any additional remarks that were entered about the associated collection.",
        { value => 'ref', maps_to => '1.2:refs:primary' },
	    "The reference from which the specimen data was entered, as formatted text.",
	    "If no reference is recorded for this specimen, the primary reference for its",
	    "associated occurrence or collection is returned instead.",
        { value => 'refattr', maps_to => '1.2:refs:attr' },
	    "The author(s) and year of publication of the reference from which this data",
	    "was entered.  If no reference is recorded for this specimen, the information from",
	    "the associated occurrence or collection reference is returned instead.",
	{ value => 'resgroup', maps_to => '1.2:colls:group' },
	    "The research group(s), if any, associated with the associated collection.",
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the specimen record");
    
    $ds->define_block( '1.2:specs:full_info' =>
	{ include => '1.2:occs:attr' },
	{ include => '1.2:occs:class' },
	{ include => '1.2:occs:plant' },
	{ include => '1.2:taxa:ecospace' },
	{ include => '1.2:taxa:taphonomy' },
	{ include => '1.2:occs:abund' },
	{ include => '1.2:colls:name' },
	{ include => '1.2:occs:coords' },
	{ include => '1.2:colls:loc' },
	{ include => '1.2:colls:paleoloc' },
	{ include => '1.2:colls:prot' },
	{ include => '1.2:colls:stratext' },
	{ include => '1.2:colls:lithext' },
	{ include => '1.2:colls:geo' },
	{ include => '1.2:colls:methods' },
	{ include => '1.2:colls:rem' },
	{ include => '1.2:refs:attr' });
    
    # Output blocks for measurements
    
    $ds->define_block('1.2:measure:basic' =>
	{ select => [ 'ms.measurement_no', 'ms.specimen_no', 'sp.specimens_measured as n_measured',
		      'ms.position', 'ms.measurement_type as measurement', 'ms.average',
		      'ms.min', 'ms.max' ] },
	{ set => '*', code => \&process_measurement_ids },
	{ output => 'measurement_no', com_name => 'oid' },
	    "The unique identifier of this measurement in the database",
	{ output => 'specimen_no', com_name => 'sid' },
	    "The identifier of the specimen with which this measurement is associated",
	{ output => 'record_type', com_name => 'typ', value => $IDP{MEA} },
	    "The type of this object: C<$IDP{MEA}> for a measurement.",
	{ output => 'n_measured', com_name => 'smn' },
	    "The number of items measured",
	{ output => 'position', com_name => 'mpo' },
	    "The position of the measured item(s), if recorded",
	{ output => 'measurement', com_name => 'mty' },
	    "The actual measurement performed",
	{ output => 'average', com_name => 'mva' },
	    "The average measured value, or the single value if only one item was measured",
	{ output => 'min', com_name => 'mvl' },
	    "The minimum measured value, if recorded",
	{ output => 'max', com_name => 'mvu' },
	    "The maximum measured value, if recorded");
    
    $ds->define_output_map('1.2:measure:output_map' =>
	{ value => 'full', maps_to => '1.2:measure:full_info' },
	    "This is a shortcut for including all of the information that defines this record.  Currently, this",
	    "includes the following blocks: B<spec>, B<attr>, B<class>, B<plant>, B<ecospace>, B<taphonomy>",
	    "B<abund>, B<coll>, B<coords>, B<loc>, B<paleoloc>, B<prot>, B<stratext>, B<lithext>,",
	    "B<geo>, B<methods>, B<rem>, B<refattr>.  If we subsequently add new data fields to the",
	    "specimen record or the measurement record, then B<full> will include those as well.  If you are publishing a URL",
	    "you could include this block, or you could publish two URLs: one to download all of the",
	    "specimen information, and one to download all the measurements.  Users would then use the",
	    "B<C<specimen_no>> field to match up the two downloads, preventing an enormous duplication",
	    "of information in each measurement row.",
	{ value => 'spec', maps_to => '1.2:measure:spec_info' },
	    "Includes all of the core fields describing the specimen from which this measurement was taken.",
	{ value => 'acconly' },
	    "Suppress the exact taxonomic identification of each specimen,",
	    "and show only the accepted name.",
	{ value => 'attr', maps_to => '1.2:occs:attr' },
	    "The attribution (author and year) of the accepted name for this specimen.",
	{ value => 'class', maps_to => '1.2:occs:class' },
	    "The taxonomic classification of the specimen: phylum, class, order, family,",
	    "genus.",
	{ value => 'classext', maps_to => '1.2:occs:class' },
	    "Like C<class>, but also includes the relevant taxon identifiers.",
	{ value => 'phylo', maps_to => '1.2:occs:class', undocumented => 1 },
	{ value => 'genus', maps_to => '1.2:occs:genus' },
	    "The genus corresponding to each specimen, if the specimen has been",
	    "identified to the genus level.  This block is redundant if C<class> or",
	    "C<classext> are used.",
	{ value => 'subgenus', maps_to => '1.2:occs:genus' },
	    "The genus corresponding to each specimen, plus the subgenus if any.",
	    "This can be added to C<class> or C<classext> in order to display",
	    "subgenera, or used instead of C<genus> to display both the genus",
	    "and the subgenus if any.",
	{ value => 'plant', maps_to => '1.2:occs:plant' },
	    "The plant organ(s), if any, associated with this specimen.  These fields",
	    "will be empty unless the specimen is a plant fossil.",
	{ value => 'abund', maps_to => '1.2:occs:abund' },
	    "Information about the abundance of the associated occurrence,",
	    "if any, in its collection",
	{ value => 'ecospace', maps_to => '1.2:taxa:ecospace' },
	    "Information about ecological space that this organism occupies or occupied.",
	    "This has only been filled in for a relatively few taxa.  Here is a",
	    "L<list of values|node:general/ecotaph#Ecospace>.",
	{ value => 'taphonomy', maps_to => '1.2:taxa:taphonomy' },
	    "Information about the taphonomy of this organism.  Here is a",
	    "L<list of values|node:general/ecotaph#Taphonomy>.",
	{ value => 'etbasis', maps_to => '1.2:taxa:etbasis' },
	    "Annotates the output block C<ecospace>, indicating at which",
	    "taxonomic level each piece of information was entered.",
	{ value => 'pres', undocumented => 1 },
	    # "Indicates whether the identification of this specimen is a regular",
	    # "taxon, a form taxon, or an ichnotaxon.",
	{ value => 'coll', maps_to => '1.2:colls:name' },
	    "The name of the collection in which the associated occurrence was found, plus any",
	    "additional remarks entered about it.",
	{ value => 'coords', maps_to => '1.2:occs:coords' },
	     "The latitude and longitude of the associated occurrence, if any.",
        { value => 'loc', maps_to => '1.2:colls:loc' },
	    "Additional information about the geographic locality of the",
	    "associated occurrence, if any.",
	{ value => 'paleoloc', maps_to => '1.2:colls:paleoloc' },
	    "Information about the paleogeographic locality of the associated occurrence,",
	    "evaluated according to the model specified by the parameter C<pgm>.",
	{ value => 'strat', maps_to => '1.2:colls:strat' },
	    "Basic information about the stratigraphic context of the associated",
	    "occurrence.",
	{ value => 'stratext', maps_to => '1.2:colls:stratext' },
	    "Detailed information about the stratigraphic context of the associated",
	    "occurrence.",
	    "This includes all of the information from C<strat> plus extra fields.",
	{ value => 'lith', maps_to => '1.2:colls:lith' },
	    "Basic information about the lithological context of the associated",
	    "occurrence.",
	{ value => 'lithext', maps_to => '1.2:colls:lithext' },
	    "Detailed information about the lithological context of the occurrence.",
	    "This includes all of the information from C<lith> plus extra fields.",
	{ value => 'methods', maps_to => '1.2:colls:methods' },
	    "Information about the collection methods used",
	{ value => 'env', maps_to => '1.2:colls:env' },
	    "The paleoenvironment associated with the associated collection, if any.",
	{ value => 'geo', maps_to => '1.2:colls:geo' },
	    "Information about the geological context of the associated occurrence (includes C<env>).",
        { value => 'rem', maps_to => '1.2:colls:rem', undocumented => 1 },
	    "Any additional remarks that were entered about the associated collection.",
        { value => 'ref', maps_to => '1.2:refs:primary' },
	    "The reference from which the specimen data was entered, as formatted text.",
	    "If no reference is recorded for this specimen, the primary reference for its",
	    "associated occurrence or collection is returned instead.",
        { value => 'refattr', maps_to => '1.2:refs:attr' },
	    "The author(s) and year of publication of the reference from which this data",
	    "was entered.  If no reference is recorded for this specimen, the information from",
	    "the associated occurrence or collection reference is returned instead.",
	{ value => 'resgroup', maps_to => '1.2:colls:group' },
	    "The research group(s), if any, associated with the associated collection.",
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the specimen record");
    
    $ds->define_block('1.2:measure:spec_info' =>
	{ select => [ 'ss.specimen_no', 'sp.is_type', 'sp.specimen_side',
		      'sp.specimen_part', 'sp.sex as specimen_sex', 'sp.specimens_measured as n_measured',
		      'sp.measurement_source', 'sp.magnification', 'sp.comments',
		      'sp.occurrence_no', 'ss.reid_no', 'ss.taxon_no as identified_no',
		      'a.taxon_name as identified_name', 'a.orig_no as spec_orig_no',
		      't.rank as identified_rank', 't.status as taxon_status', 't.orig_no',
		      'nm.spelling_reason', 'ns.spelling_reason as accepted_reason',
		      't.spelling_no', 't.accepted_no',
		      'tv.spelling_no as accepted_spelling', 'tv.name as accepted_name', 'tv.rank as accepted_rank',
		      'ei.interval_name as early_interval', 'li.interval_name as late_interval',
		      'o.genus_name', 'o.genus_reso', 'o.subgenus_name', 'o.subgenus_reso',
		      'o.species_name', 'o.species_reso', 'v.is_form', 'v.is_trace',
		      'o.early_age', 'o.late_age', 'sp.reference_no'],
	  tables => [ 'ss', 'o', 't', 'nm', 'ns', 'tv', 'ei', 'li', 'o', 'v' ] },
	{ set => '*', from => '*', code => \&process_basic_record },
	{ set => '*', code => \&PB2::OccurrenceData::process_occ_ids },
	{ output => 'flags', com_name => 'flg' },
	    "This field will be empty for most records.  Otherwise, it will contain one or more",
	    "of the following letters:", "=over",
	    "=item N", "This specimen is not associated with an occurrence.",
	    "=item D", "The identification entered for this specimen is different from the",
	        "identification entered for the corresponding occurrence.",
	    "=item R", "This identification has been superceded by a more recent one.",
		"In other words, this specimen has been reidentified.",
	    "=item I", "This identification is an ichnotaxon",
	    "=item F", "This identification is a form taxon",
	    "This field will be empty for most records.  A record representing a specimen",
	    "not associated with an occurrence will have an C<N> in this field.  A record",
	    "representing a specimen whose identification is different than its associated",
	    "occurrence will have an C<I> in this field.",
	{ output => 'occurrence_no', com_name => 'qid' },
	    "The identifier of the occurrence, if any, with which this specimen is associated",
	{ output => 'reid_no', com_name => 'eid' },
	    "If the associated occurrence was reidentified, a unique identifier for the",
	    "reidentification.",
	{ output => 'collection_no', com_name => 'cid' },
	    "The identifier of the collection, if any, with which this specimen is associated",
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
	{ output => 'specimen_id', com_name => 'smi', data_type => 'str' },
	    "The identifier for this specimen according to its custodial institution",
	{ output => 'is_type', com_name => 'smt' },
	    "Indicates whether this specimen is a holotype or paratype",
	{ output => 'specimen_side', com_name => 'sms' },
	    "The side of the body to which the specimen part corresponds",
	{ output => 'specimen_part', com_name => 'smp' },
	    "The part of the body of which this specimen consists",
	{ output => 'specimen_sex', com_name => 'smx' },
	    "The sex of the specimen, if known",
	{ output => 'measurement_source', com_name => 'mms' },
	    "How the measurements were obtained, if known",
	{ output => 'magnification', com_name => 'mmg' },
	    "The magnification used in the measurement, if known",
	{ output => 'comments', com_name => 'smc' },
	    "Comments on this specimen, often author and publication year",
	{ output => 'identified_name', com_name => 'idn', dwc_name => 'associatedTaxa', not_block => 'acconly' },
	    "The taxonomic name by which this occurrence was identified.  This field will",
	    "be omitted for responses in the compact voabulary if it is identical",
	    "to the value of C<accepted_name>.",
	{ output => 'identified_rank', dwc_name => 'taxonRank', com_name => 'idr', not_block => 'acconly' },
	    "The taxonomic rank of the identified name, if this can be determined.  This field will",
	    "be omitted for responses in the compact voabulary if it is identical",
	    "to the value of C<accepted_rank>.",
	{ set => 'identified_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb', not_block => 'acconly' },
	{ output => 'identified_no', com_name => 'iid', not_block => 'acconly' },
	    "The unique identifier of the identified taxonomic name.  If this is empty, then",
	    "the name was never entered into the taxonomic hierarchy stored in this database and",
	    "we have no further information about the classification of this occurrence.  In some cases,",
	    "the genus has been entered into the taxonomic hierarchy but not the species.  This field will",
	    "be omitted for responses in the compact voabulary if it is identical",
	    "to the value of C<accepted_no>.",
	{ output => 'difference', com_name => 'tdf', not_block => 'acconly' },
	    "If the identified name is different from the accepted name, this field gives",
	    "the reason why.  This field will be present if, for example, the identified name",
	    "is a junior synonym or nomen dubium, or if the species has been recombined, or",
	    "if the identification is misspelled.",
	{ output => 'accepted_name', com_name => 'tna', if_field => 'accepted_no' },
	    "The value of this field will be the accepted taxonomic name corresponding",
	    "to the identified name.",
	{ output => 'accepted_attr', if_block => 'attr', dwc_name => 'scientificNameAuthorship', com_name => 'att' },
	    "The attribution (author and year) of the accepted taxonomic name",
	{ output => 'accepted_rank', com_name => 'rnk', if_field => 'accepted_no' },
	    "The taxonomic rank of the accepted name.  This may be different from the",
	    "identified rank if the identified name is a nomen dubium or otherwise invalid,",
	    "or if the identified name has not been fully entered into the taxonomic hierarchy",
	    "of this database.",
	{ set => 'accepted_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb' },
	{ output => 'accepted_no', com_name => 'tid', if_field => 'accepted_no' },
	    "The unique identifier of the accepted taxonomic name in this database.",
	{ set => '*', code => \&PB2::CollectionData::fixTimeOutput },
	{ output => 'early_age', com_name => 'eag', pbdb_name => 'max_ma' },
	    "The early bound of the geologic time range associated with this occurrence (in Ma)",
	{ output => 'late_age', com_name => 'lag', pbdb_name => 'min_ma' },
	    "The late bound of the geologic time range associated with this occurrence (in Ma)",
	{ output => 'ref_author', dwc_name => 'recordedBy', com_name => 'aut', if_block => '1.2:refs:attr' },
	    "The attribution of the specimen: the author name(s) from",
	    "the specimen reference, and the year of publication.",
	{ output => 'ref_pubyr', com_name => 'pby', if_block => '1.2:refs:attr' },
	    "The year of publication of the reference from which this data was entered",
	{ output => 'reference_no', com_name => 'rid' },
	    "The identifier of the reference from which this data was entered");
    
    $ds->define_block( '1.2:measure:full_info' =>
	{ include => '1.2:measure:spec_info' },
	{ include => '1.2:occs:attr' },
	{ include => '1.2:occs:class' },
	{ include => '1.2:occs:plant' },
	{ include => '1.2:taxa:ecospace' },
	{ include => '1.2:taxa:taphonomy' },
	{ include => '1.2:occs:abund' },
	{ include => '1.2:colls:name' },
	{ include => '1.2:occs:coords' },
	{ include => '1.2:colls:loc' },
	{ include => '1.2:colls:paleoloc' },
	{ include => '1.2:colls:prot' },
	{ include => '1.2:colls:stratext' },
	{ include => '1.2:colls:lithext' },
	{ include => '1.2:colls:geo' },
	{ include => '1.2:colls:methods' },
	{ include => '1.2:colls:rem' },
	{ include => '1.2:refs:attr' });
    
    # Parameter value definitions
    
    $ds->define_set('1.2:specs:type' =>
	{ value => 'holo' },
	    "Select only holotypes.",
	{ value => 'para' },
	    "Select only holotypes and paratypes.",
	{ value => "any" },
	    "Select all specimens.  This is the default.");
    
    # Rulesets for the various operations defined by this package
    
    $ds->define_ruleset('1.2:specs:specifier' =>
	{ param => 'spec_id', valid => VALID_IDENTIFIER('SPM'), alias => 'id' },
	    "The identifier of the occurrence you wish to retrieve (REQUIRED).",
	    "You may instead use the parameter name C<id>.");
    
    $ds->define_ruleset('1.2:specs:selector' =>
	{ param => 'spec_id', valid => VALID_IDENTIFIER('SPM'), list => ',', alias => 'id' },
	    "A comma-separated list of specimen identifiers.  Specimens identified by",
	    "these identifiers are selected, provided they satisfy any other parameters",
	    "given with this request.",
	{ param => 'occ_id', valid => VALID_IDENTIFIER('OCC'), list => ',' },
	    "A comma-separated list of occurrence identifiers.  Specimens corresponding",
	    "to the specified occurrences are selected, provided they satisfy any other",
	    "parameters given with this request.",
	{ param => 'coll_id', valid => VALID_IDENTIFIER('COL'), list => ',' },
	    "A comma-separated list of collection identifiers.  Specimens corresponding",
	    "to occurrences in the specified collections are selected, provided they satisfy any other",
	    "parameters given with this request.",
	{ at_most_one => [ 'all_records', 'spec_id', 'occ_id' ] });
    
    $ds->define_ruleset('1.2:specs:all_records' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "Select all occurrences entered in the database, subject to any other parameters you may specify.",
	    "This parameter does not require any value.");
    
    $ds->define_ruleset('1.2:specs:filter' =>
	{ optional => 'spectype', valid => '1.2:specs:type' },
	    "Select specimens according to whether they are a paratype or holotype.",
	    "Accepted values include:");
    
    $ds->define_ruleset('1.2:specs:display' =>
	{ optional => 'show', list => q{,}, valid => '1.2:specs:basic_map' },
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each occurrence.  Its value should be",
	    "one or more of the following, separated by commas:",
	{ optional => 'order', valid => '1.2:occs:order', split => ',', no_set_doc => 1 },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  Accepted values are:",
	    $ds->document_set('1.2:occs:order'),
	    "If no order is specified, results are sorted by specimen identifier.",
	{ ignore => 'level' });
    
    $ds->define_ruleset('1.2:specs:single' =>
	"The following parameter selects a record to retrieve:",
    	{ require => '1.2:specs:specifier', 
	  error => "you must specify a specimen identifier, either in the URL or with the 'id' parameter" },
	">>You may also use the following parameter to specify what information you wish to retrieve:",
	{ optional => 'pgm', valid => $ds->valid_set('1.2:colls:pgmodel'), list => "," },
	    "Specify which paleogeographic model(s) to use when evaluating paleocoordinates.",
	    "You may specify one or more from the following list, separated by commas.",
	    "If you do not specify a value for this parameter, the default model is C<gplates>.",
	    $ds->document_set('1.2:colls:pgmodel'),
    	{ optional => 'SPECIAL(show)', valid => '1.2:specs:basic_map' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:specs:list' =>
	"You can use the following parameter if you wish to retrieve the entire set of",
	"specimen records entered in this database.  Please use this with care, since the",
	"result set will contain more than 130,000 records and will be at least 25 megabytes in size.",
    	{ allow => '1.2:specs:all_records' },
	">>The following parameters can be used to specify which kinds of specimens you are interested in:",
	{ allow => '1.2:specs:filter' },
        ">>The following parameters can be used to query for specimens by a variety of criteria.",
	"Except as noted below, you may use these in any combination.  If you do not specify B<C<all_records>>,",
	"you must specify at least one selection parameter from the following list.",
	{ allow => '1.2:specs:selector' },
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:specs:all_records', '1.2:specs:selector', '1.2:main_selector', 
			  '1.2:interval_selector', '1.2:ma_selector'] },
	{ ignore => 'level' },
	">>The following parameters can be used to further filter the selection.",
	"If you wish to use one of them and have not specified any of the selection parameters",
	"listed above, use B<C<all_records>>.",
	{ allow => '1.2:common:select_specs_crmod' },
	{ allow => '1.2:common:select_specs_ent' },
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	">>The following parameters can also be used to filter the result list based on taxonomy:",
	{ allow => '1.2:taxa:occ_list_filter' },
	">>You can use the following parameters to select extra information you wish to retrieve,",
	"and the order in which you wish to get the records:",
	{ allow => '1.2:specs:display' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:specs:byref' =>
	"You can use the following parameter if you wish to retrieve the entire set of",
	"specimens records entered in this database.  Please use this with care, since the",
	"result set will contain more than 7,000 records and will be at least 2 megabytes in size.",
    	{ allow => '1.2:specs:all_records' },
	">>The following parameters can be used to specify which kinds of specimens you are interested in:",
	{ allow => '1.2:specs:filter' },
        ">>The following parameters can be used to query for specimens by a variety of criteria.",
	"Except as noted below, you may use these in any combination.  If you do not specify B<C<all_records>>,",
	"you must specify at least one selection parameter from the following list.",
	{ allow => '1.2:specs:selector' },
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:specs:all_records', '1.2:specs:selector', '1.2:main_selector', 
			  '1.2:interval_selector', '1.2:ma_selector'] },
	{ ignore => ['level', 'ref_type', 'select'] },
	">>You can use the following parameters to filter the result set based on attributes",
	"of the bibliographic references.  If you wish to use one of them and have not specified",
	"any of the selection parameters listed above, use B<C<all_records>>.",
	{ allow => '1.2:refs:aux_selector' },
	">>The following parameters can be used to further filter the selection.",
	{ allow => '1.2:common:select_specs_crmod' },
	{ allow => '1.2:common:select_specs_ent' },
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ allow => '1.2:common:select_refs_crmod' },
	{ allow => '1.2:common:select_refs_ent' },
	">>The following parameters can also be used to filter the result list based on taxonomy:",
	{ allow => '1.2:taxa:occ_list_filter' },
	">>You can use the following parameters to select extra information you wish to retrieve,",
	"and the order in which you wish to get the records:",
	{ allow => '1.2:specs:display' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:specs:refs' =>
	"You can use the following parameter if you wish to retrieve the references associated with the entire set of",
	"occurrence records entered in this database.  Please use this with care, since the",
	"result set will contain more than 20,000 records and will be at least 20 megabytes in size.",
    	{ allow => '1.2:specs:all_records' },
	">>The following parameters can be used to specify which kinds of specimens you are interested in:",
	{ allow => '1.2:specs:filter' },
	">>The following B<very important parameter> allows you to select references that",
	"have particular relationships to the taxa they mention, and skip others:",
	{ optional => 'ref_type', valid => '1.2:taxa:refselect', alias => 'select', list => ',',
	  bad_value => '_' },
	    "You can use this parameter to specify which kinds of references to retrieve.",
	    "The default is C<B<specs>>, which selects only those references from which",
	    "specimens were entered.",
	    "The value of this attribute can be one or more of the following, separated by commas:",
        ">>The following parameters can be used to query for specimens by a variety of criteria.",
	"Except as noted below, you may use these in any combination.  If you do not specify B<C<all_records>>,",
	"you must specify at least one selection parameter from the following list.",
	{ allow => '1.2:specs:selector' },
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:specs:all_records', '1.2:specs:selector', '1.2:main_selector', 
			  '1.2:interval_selector', '1.2:ma_selector'] },
	{ ignore => 'level' },
	">>You can use the following parameters to filter the result set based on attributes",
	"of the bibliographic references.  If you wish to use one of them and have not specified",
	"any of the selection parameters listed above, use B<C<all_records>>.",
	{ allow => '1.2:refs:aux_selector' },
	">>The following parameters can be used to further filter the selection.",
	{ allow => '1.2:common:select_specs_crmod' },
	{ allow => '1.2:common:select_specs_ent' },
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ allow => '1.2:common:select_refs_crmod' },
	{ allow => '1.2:common:select_refs_ent' },
	">>The following parameters can also be used to filter the result list based on taxonomy:",
	{ allow => '1.2:taxa:occ_aux_filter' },
	">>You can use the following parameters to select extra information you wish to retrieve,",
	"and the order in which you wish to get the records:",
	{ allow => '1.2:refs:display' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:specs:measurements' =>
	"You can use the following parameter if you wish to retrieve the entire set of",
	"measurement records entered in this database.  Please use this with care, since the",
	"result set will contain more than 300,000 records and will be at least 17 megabytes in size.",
    	{ allow => '1.2:specs:all_records' },
	">>The following parameters can be used to specify which kinds of specimens you are interested in:",
	{ allow => '1.2:specs:filter' },
	">>You can use the following parameters if you wish to retrieve measurements from",
	"a known list of specimens, occurrences, or collections.",
	"Only the records which match the other parameters that you specify will be returned.",
	{ allow => '1.2:specs:selector' },
        ">>The following parameters can be used to query for specimens by a variety of criteria.",
	"Except as noted below, you may use these in any combination.",
	"These same parameters can all be used to select either occurrences, collections, or associated references or taxa.",
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ require_any => ['1.2:specs:all_records', '1.2:specs:selector', '1.2:main_selector', 
			  '1.2:interval_selector', '1.2:ma_selector'] },
	{ ignore => 'level' },
	">>The following parameters can be used to further filter the selection.",
	"If you wish to use one of them and have not specified any of the selection parameters",
	"listed above, use B<C<all_records>>.",
	{ allow => '1.2:common:select_specs_crmod' },
	{ allow => '1.2:common:select_specs_ent' },
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	">>The following parameters can also be used to filter the result list based on taxonomy:",
	{ allow => '1.2:taxa:occ_list_filter' },
	">>You can also use the following parameter to include additional information about the specimen",
	"from which each measurement was taken. However, in many situations you may want instead to download",
	"the specimen information separately and use the B<C<specimen_no>> field to match up the two",
	"downloads. That method avoids an enormous duplication of information in each measurement row.",
	{ optional => 'show', list => q{,}, valid => '1.2:measure:output_map' },
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each occurrence.  Its value should be",
	    "one or more of the following, separated by commas:",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    # Specimen element definitions
    
    $ds->define_block('1.2:specs:element' => 
	{ select => ['e.specelt_no', 'e.element_name', 'e.parent_name', 'e.taxon_name', 
		     'e.alternate_names', 'm.exclude'] },
	{ set => '*', code => \&process_element_record },
	{ output => 'specelt_no', com_name => 'oid' },
	    "The unique identifier of this specimen element in the database",
	{ output => 'record_type', com_name => 'typ', value => $IDP{ELT} },
	    "The type of this object: C<$IDP{ELT}> for a specimen element.",
	{ output => 'element_name', com_name => 'nam' },
	    "The name of this specimen element",
	{ output => 'parent_name', com_name => 'par' },
	    "The name of the parent element, if any. This can be used to display",
	    "the elements in a collapsed list where individual elements can be",
	    "expanded to show their children.",
	{ output => 'alternate_names', com_name => 'alt' },
	    "Alternate names for this element, if any.",	      
	{ output => 'taxon_name', com_name => 'tna' },
	    "The name of the base taxon for which this element is defined.");
    
    $ds->define_output_map('1.2:specs:element_map' =>
	{ value => 'seq', maps_to => '1.2:specs:element_seq' },
	    "Includes the sequence numbers from a preorder traversal of",
	    "the taxon tree which bracket the taxa for which each element",
	    "is defined.");
    
    $ds->define_set('1.2:specs:element_order' =>
	{ value => 'name' },
	    "Return the elements in alphabetical order by name.",
	{ value => 'name.asc', undocumented => 1 },
	{ value => 'name.desc', undocumented => 1 },
	{ value => 'hierarchy' },
	    "Return the elements that are defined for higher taxa first,",
	    "more specific ones following. Elements that are defined at",
	    "the same taxonomic level are sorted alphabetically by name.",
	{ value => 'hierarchy.asc', undocumented => 1 },
	{ value => 'hierarchy.desc', undocumented => 1 });
    
    $ds->define_block('1.2:specs:element_seq' =>
	{ select => ['m.lft', 'm.rgt'] },
	{ output => 'lft', com_name => 'lsq' },
	    "The base taxon's position in the preorder traversal of the taxonomic",
	    "tree. If you display elements in the order specified by this field,",
	    "then elements defined for higher taxa will appear higher in the list",
	    "and more specific elements lower down.",
	{ output => 'rgt', com_name => 'rsq' },
	    "The end of the range of subtaxa for which this specimen element is",
	    "valid, in the preorder traversal sequence.");
    
    $ds->define_ruleset('1.2:specs:element_selector' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "Return all specimen element records known to the database, subject",
	    "to any other parameters given.",
	{ param => 'taxon_name', valid => ANY_VALUE },
	    "Return only elements that are valid for the named taxon.",
	{ param => 'taxon_id', valid => VALID_IDENTIFIER('TXN') },
	    "Return only elements that are valid for the specified taxon,",
	    "given by its identifier in the database.",
	{ param => 'name_re', valid => ANY_VALUE },
	    "Return only elements whose name or alternate name matches the given regular expression.",
	{ at_most_one => ['all_records', 'taxon_name', 'taxon_id'] });
    
    $ds->define_ruleset('1.2:specs:element_display' =>
	{ optional => 'show', list => q{,}, valid => '1.2:specs:element_map' },
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each element.  Its value should be",
	    "one or more of the following, separated by commas:",
	{ optional => 'order', valid => '1.2:specs:element_order' },
	    "Specifies the order in which the results are returned. If this",
	    "parameter is not given, the results are ordered alphabetically by",
	    "name. The value of this parameter should be one of the following:");
    
    $ds->define_ruleset('1.2:specs:elements' =>
	"The following parameters select which records to return:",
	{ require => '1.2:specs:element_selector' },
	"The following parameters specify what information should be returned:",
	{ allow => '1.2:specs:element_display' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
}


# get_specimen ( )
# 
# Query for all relevant information about the specimen specified by the
# 'id' parameter.

sub get_specimen {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $request->clean_param('spec_id');
    
    die "400 Bad identifier '$id'\n" unless $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'sp', cd => 'sp' );
    
    my $tables = $request->tables_hash;
    
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
    
    my $fields = join(', ', @fields);
    
    $request->adjustCoordinates(\$fields);
    $request->selectPaleoModel(\$fields, $request->tables_hash) if $fields =~ /PALEOCOORDS/;
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    $request->extid_check;
    
    # Figure out what information we need to determine access permissions.
    
    my ($access_filter, $access_fields) = $request->generateAccessFilter('cc', $tables);
    
    $fields .= $access_fields if $access_fields;
    
    $request->delete_output_field('permissions') unless $access_fields;
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generateJoinList('c', $request->tables_hash);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $fields, if($access_filter, 1, 0) as access_ok
	FROM $SPEC_MATRIX as ss JOIN specimens as sp using (specimen_no)
		LEFT JOIN $OCC_MATRIX as o on o.occurrence_no = ss.occurrence_no and o.reid_no = ss.reid_no
		LEFT JOIN $COLL_MATRIX as c on c.collection_no = o.collection_no
		LEFT JOIN authorities as a on a.taxon_no = ss.taxon_no
		$join_list
        WHERE ss.specimen_no = $id and (c.access_level = 0 or o.occurrence_no is null)
	GROUP BY ss.specimen_no";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    # Return an error response if we couldn't retrieve the record.
    
    die "404 Not found\n" unless $request->{main_record};
    
    # Return an error response if we could retrieve the record but the user is not authorized to
    # access it.  Any specimen not tied to an occurrence record is public by definition.
    
    die $request->exception(403, "Access denied") 
	unless $request->{main_record}{access_ok} || ! $request->{main_record}{occurrence_no};
    
    return 1;
}


# list_specimens ( )
# 
# Query for all relevant information about the specimen(s) matching the
# specified filters.

sub list_specimens {
    
    my ($request, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $tables = $request->tables_hash;
    
    $request->substitute_select( mt => 'ss', cd => 'ss' );
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = $request->generateMainFilters('list', 'c', $tables);
    push @filters, $request->generateOccFilters($tables, 'ss');
    push @filters, $request->generate_spec_filters($tables);
    push @filters, $request->generate_ref_filters($tables);
    push @filters, $request->generate_refno_filter('ss');
    push @filters, $request->generate_common_filters( { specs => 'ss', occs => 'o', bare => 'ss' } );
    
    if ( my @ids = $request->clean_param_list('spec_id') )
    {
	my $id_list = join(',', @ids);
	push @filters, "ss.specimen_no in ($id_list)";
    }
    
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
    
    # Figure out what information we need to determine access permissions.  Any specimen not tied
    # to an occurrence is public by definition.
    
    my ($access_filter, $access_fields) = $request->generateAccessFilter('cc', $tables);
    
    $request->delete_output_field('permissions') unless $access_fields;
    
    push @filters, "(ss.occurrence_no = 0 or $access_filter)";
    
    my $filter_string = join(' and ', @filters);
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # By default, we group by specimen_no and occurrence_no.
    
    my $group_expr = "ss.specimen_no";
    
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
    
    # If all identifications were selected, we will need to group by reid_no
    # as well as occurrence_no.
    
    if ( $tables->{group_by_reid} )
    {
	$group_expr .= ', ss.reid_no';
    }
    
    # If we were requested to lump by genus, we need to modify the query
    # accordingly.
    
    # my $taxonres = $request->clean_param('taxon_reso');
    
    # Now generate the field list.
    
    my $fields = join(', ', @fields);
    
    $request->adjustCoordinates(\$fields);
    $request->selectPaleoModel(\$fields, $request->tables_hash) if $fields =~ /PALEOCOORDS/;
    
    $fields .= $access_fields if $access_fields;
    
    # Determine the order in which the results should be returned.
    
    my $tt = $tables->{tv} ? 'ts' : 't';
    
    my $order_clause = $request->PB2::CollectionData::generate_order_clause($tables, { at => 'c', bt => 'ss', tt => $tt });
    
    if ( $order_clause )
    {
	$order_clause .= ", ss.specimen_no";
    }
    
    elsif ( defined $arg && $arg eq 'byref' )
    {
	$order_clause = "r.reference_no, ss.specimen_no";
	$tables->{r} = 1;
    }
    
    else
    {
	$order_clause = "ss.specimen_no";
    }
    
    # Determine which extra tables, if any, must be joined to the query.  Then
    # construct the query.
    
    my $join_list = $request->generateJoinList('c', $tables);
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM $SPEC_MATRIX as ss JOIN specimens as sp using (specimen_no)
		LEFT JOIN $OCC_MATRIX as o on o.occurrence_no = ss.occurrence_no and o.reid_no = ss.reid_no
		LEFT JOIN $COLL_MATRIX as c on o.collection_no = c.collection_no
		LEFT JOIN authorities as a on a.taxon_no = ss.taxon_no
		$join_list
        WHERE $filter_string
	GROUP BY $group_expr
	ORDER BY $order_clause
	$limit";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


# list_specimens_associated
# 
# List bibliographic references for specimens

sub list_specimens_associated {

    my ($request, $record_type) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
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
    $select{specs} = 1 unless %select;
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.  We have to include table 'o'
    # so that the proper identification filter (idtype) is added to the query
    # by generateOccFilters.
    
    my $inner_tables = { o => 1 };
    
    my @filters = $request->generateMainFilters('list', 'c', $inner_tables);
    push @filters, $request->generate_common_filters( { specs => 'ss', occs => 'o', refs => 'ignore' } );
    push @filters, $request->generateOccFilters($inner_tables, 'ss');
    push @filters, $request->generate_spec_filters($inner_tables);
    
    if ( my @ids = $request->clean_param_list('spec_id') )
    {
	my $id_list = join(',', @ids);
	push @filters, "ss.specimen_no in ($id_list)";
    }
    
    # Do a final check to make sure that all records are only returned if
    # 'all_records' was specified.
    
    if ( @filters == 0 )
    {
	die "400 You must specify 'all_records' if you want to retrieve the entire set of records.\n"
	    unless $request->clean_param('all_records');
    }
    
    # Figure out what information we need to determine access permissions.  We
    # can ignore $access_fields since we are not generating occurrence or
    # collection records.
    
    my ($access_filter, $access_fields) = $request->generateAccessFilter('cc', $inner_tables);
    
    push @filters, "(ss.occurrence_no = 0 or $access_filter)";
    
    my $filter_string = join(' and ', @filters);
    
    # If we do want taxonomy references, we must constuct a temporary table of
    # occurrences and pass that to Taxonomy::list_associated.
    
    if ( $use_taxonomy )
    {
	# If the 'strict' parameter was given, make sure we haven't generated any
	# warnings. 
	
	$request->strict_check;
	$request->extid_check;
	
	$dbh->do("DROP TABLE IF EXISTS spec_list");
	$dbh->do("CREATE TEMPORARY TABLE spec_list (
			specimen_no int unsigned not null primary key,
			occurrence_no int unsigned not null,
			taxon_no int unsigned not null,
			orig_no int unsigned not null ) engine=memory");
	
	my $inner_join_list = $request->generateJoinList('c', $inner_tables);
	
	try {
	    $sql = "
		INSERT IGNORE INTO spec_list
		SELECT ss.specimen_no, ss.occurrence_no, ss.taxon_no, ss.orig_no FROM $SPEC_MATRIX as ss
			JOIN specimens as sp using (specimen_no)
			JOIN $OCC_MATRIX as o using (occurrence_no)
			JOIN $COLL_MATRIX as c using (collection_no)
			LEFT JOIN authorities as a on a.taxon_no = ss.taxon_no
			$inner_join_list
		WHERE $filter_string";
	
	    $dbh->do($sql);
	}
	
	catch {
	    $dbh->do("DROP TEMPORARY TABLE IF EXISTS spec_list");
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
	# be using the table 'spec_list'.
	
	$options->{return} = 'stmt';
	$options->{table} = 'spec_list';
	
	try {
	    my ($result) = $taxonomy->list_associated('specs', $request->{my_base_taxa}, $options);
	    my @warnings = $taxonomy->list_warnings;
	    
	    $request->sth_result($result) if $result;
	    $request->add_warning(@warnings) if @warnings;
	}
	
	catch {
	    die $_;
	}
	
	finally {
	    $dbh->do("DROP TABLE IF EXISTS spec_list");
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
	
	if ( $select{specs} )
	{
	    $sql = "INSERT IGNORE INTO ref_collect
		SELECT ss.reference_no, 'S' as ref_type, ss.taxon_no, NULL as occurrence_no, 
			ss.specimen_no, null as collection_no
		FROM $SPEC_MATRIX as ss LEFT JOIN $OCC_MATRIX as o using (occurrence_no)
			JOIN specimens as sp using (specimen_no)
			LEFT JOIN $COLL_MATRIX as c using (collection_no)
			$inner_join_list
		WHERE $filter_string";
	    
	    $request->{ds}->debug_line("$sql\n") if $request->debug;
	    
	    $dbh->do($sql);
	}
	
	if ( $select{occs} )
	{
	    $sql = "INSERT IGNORE INTO ref_collect
		SELECT o.reference_no, 'O' as ref_type, ss.taxon_no, o.occurrence_no, 
			null as specimen_no, null as collection_no
		FROM $SPEC_MATRIX as ss LEFT JOIN $OCC_MATRIX as o using (occurrence_no)
			JOIN specimens as sp using (specimen_no)
			LEFT JOIN $COLL_MATRIX as c using (collection_no)
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
		FROM $SPEC_MATRIX as ss LEFT JOIN $OCC_MATRIX as o using (occurrence_no)
			JOIN specimens as sp using (specimen_no)
			LEFT JOIN $COLL_MATRIX as c using (collection_no)
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


# list_measurements
# 
# Query for all measurements associated with the specimen(s) matching the
# specified filters.

sub list_measurements {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $tables = $request->tables_hash;
    
    $request->substitute_select( mt => 'ss', cd => 'ss' );
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.  We must include the table 'o'
    # so that the proper identification filter (idtype) is added to the query.
    
    $tables->{o} = 1;
    
    my @filters = $request->generateMainFilters('list', 'c', $tables);
    push @filters, $request->generateOccFilters($tables, 'ss');
    push @filters, $request->generate_spec_filters($tables);
    push @filters, $request->generate_common_filters( { specs => 'ss', occs => 'o', bare => 'ss' } );
    
    if ( my @ids = $request->clean_param_list('spec_id') )
    {
	my $id_list = join(',', @ids);
	push @filters, "ss.specimen_no in ($id_list)";
    }
    
    # Do a final check to make sure that all records are only returned if
    # 'all_records' was specified.
    
    if ( @filters == 0 )
    {
	die "400 You must specify 'all_records' if you want to retrieve the entire set of records.\n"
	    unless $request->clean_param('all_records');
    }
    
    # Until we provide for authenticated data service access, we had better
    # restrict results to publicly accessible records.  But if no occurrence
    # number was given for this specimen, we must assume it is public since
    # access levels are only specified for collections (and thus occurrences).
    
    push @filters, "(c.access_level = 0 or o.occurrence_no is null)";
    
    my $filter_string = join(' and ', @filters);
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. 
    
    $request->strict_check;
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # By default, we group by measurement_no.
    
    my $group_expr = "ms.measurement_no";
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
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
    
    # Now generate the field list.
    
    my $fields = join(', ', @fields);
    
    $request->adjustCoordinates(\$fields);
    $request->selectPaleoModel(\$fields, $request->tables_hash) if $fields =~ /PALEOCOORDS/;
    
    # Determine the order in which the results should be returned.
    
    my $tt = $tables->{tv} ? 'ts' : 't';
    
    my $order_clause = $request->PB2::CollectionData::generate_order_clause($tables, { at => 'c', bt => 'ss', tt => $tt });
    
    if ( $order_clause )
    {
	$order_clause .= ", ss.specimen_no";
    }
    
    else
    {
	$order_clause = "ss.specimen_no";
    }
    
    $order_clause .= ', ms.measurement_no';
    
    # Determine which extra tables, if any, must be joined to the query.  Then
    # construct the query.
    
    my $join_list = $request->generateJoinList('c', $tables);
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM measurements as ms JOIN $SPEC_MATRIX as ss using (specimen_no)
		JOIN specimens as sp using (specimen_no)
		LEFT JOIN $OCC_MATRIX as o on o.occurrence_no = ss.occurrence_no and o.reid_no = ss.reid_no
		LEFT JOIN $COLL_MATRIX as c on o.collection_no = c.collection_no
		LEFT JOIN authorities as a on a.taxon_no = ss.taxon_no
		$join_list
        WHERE $filter_string
	GROUP BY $group_expr
	ORDER BY $order_clause
	$limit";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


# generate_spec_filters ( tables )
# 
# Generate filters based on parameters relevant to specimens only.

sub generate_spec_filters {
    
    my ($request, $tables_ref) = @_;
    
    my $dbh = $request->{dbh};
    my @filters;
    
    # Check for parameter 'spectype'
    
    if ( my $spectype = $request->clean_param('spectype') )
    {
	if ( $spectype eq 'holo' )
	{
	    push @filters, "sp.is_type = 'holotype'";
	    $tables_ref->{sp} = 1;
	}
	
	elsif ( $spectype eq 'para' )
	{
	    push @filters, "sp.is_type in ('holotype', 'paratype', 'some paratypes')";
	    $tables_ref->{sp} = 1;
	}
    }
    
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
    
    $join_list .= "LEFT JOIN collections as cc on c.collection_no = cc.collection_no\n"
	if $tables->{cc};
    $join_list .= "LEFT JOIN occurrences as oc on o.occurrence_no = oc.occurrence_no\n"
	if $tables->{oc};
    $join_list .= "LEFT JOIN coll_strata as cs on cs.collection_no = c.collection_no\n"
	if $tables->{cs};
    
    $join_list .= "LEFT JOIN taxon_trees as t on t.orig_no = ss.orig_no\n"
	if $tables->{t};
    
    $join_list .= "LEFT JOIN taxon_trees as tv on tv.orig_no = t.accepted_no\n"
	if $tables->{tv} || $tables->{e};
    $join_list .= "LEFT JOIN taxon_lower as pl on pl.orig_no = $t.orig_no\n"
	if $tables->{pl};
    $join_list .= "LEFT JOIN taxon_ints as ph on ph.ints_no = $t.ints_no\n"
	if $tables->{ph};
    $join_list .= "LEFT JOIN taxon_attrs as v on v.orig_no = $t.orig_no\n"
	if $tables->{v};
    $join_list .= "LEFT JOIN taxon_names as nm on nm.taxon_no = ss.taxon_no\n"
	if $tables->{nm};
    $join_list .= "LEFT JOIN taxon_names as ns on ns.taxon_no = t.spelling_no\n"
	if $tables->{nm} && $tables->{t};
    $join_list .= "LEFT JOIN $PALEOCOORDS as pc on pc.collection_no = c.collection_no\n"
	if $tables->{pc};
    $join_list .= "LEFT JOIN $GEOPLATES as gp on gp.plate_no = pc.mid_plate_id\n"
	if $tables->{gp};
    $join_list .= "LEFT JOIN refs as r on r.reference_no = ss.reference_no\n" 
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
    
    return $join_list;
}


# list_elements ( )
# 
# Return lists of specimen elements.

sub list_elements {

    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters;
    my $tables_hash = $request->tables_hash;
    my $ignore_exclude;
    my $taxon;
    
    if ( $request->clean_param('all_records') )
    {
	push @filters, "1=1";
	$ignore_exclude = 1;
    }
    
    elsif ( my $taxon_no = $request->clean_param('taxon_id') )
    {
	($taxon) = $taxonomy->list_taxa_simple($taxon_no, { fields => 'SEARCH' });
	
	    # $dbh->selectrow_array("
	    # 	SELECT t.lft FROM taxon_trees as t
	    # 	WHERE t.orig_no = $id");
	
	die $request->exception(404, "Not found") unless $taxon;
	
	push @filters, "$taxon->{lft} between m.lft and m.rgt";
    }
    
    elsif ( my $taxon_name = $request->clean_param('taxon_name') )
    {
	($taxon) = $taxonomy->resolve_names($taxon_name, { fields => 'SEARCH' });
	
	die $request->exception(404, "Not found") unless $taxon;
	
	push @filters, "$taxon->{lft} between m.lft and m.rgt";
    }
    
    if ( my $name_re = $request->clean_param('name_re') )
    {
	my $quoted = $dbh->quote($name_re);

	push @filters, "(e.element_name rlike $quoted or e.alternate_names rlike $quoted)";
    }
    
    # Do a final check to make sure that all records are only returned if
    # 'all_records' was specified.
    
    if ( @filters == 0 )
    {
	die $request->exception(400, "You must specify 'all_records' if you want to retrieve the entire set of records.");
    }
    
    push @filters, "not m.exclude";

    if ( $taxon )
    {
	push @filters, "m.specelt_no not in (SELECT specelt_no 
		FROM $SPECELT_MAP as exc WHERE exc.exclude and $taxon->{lft} between exc.lft and exc.rgt)"
    }
    
    my $filter_string = join(' and ', @filters);
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check for the 'extids' parameter.
    
    $request->strict_check;
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Now generate the field list.
    
    my $fields = $request->select_string;
	
    # Determine the order in which the results should be returned.
    
    my $order_clause = 'element_name';
    
    if ( my $order_param = $request->clean_param('order') )
    {
	$order_param = lc $order_param;
	
	if ( $order_param eq 'hierarchy' || $order_param eq 'hierarchy.asc' )
	{
	    $order_clause = 'lft, element_name';
	}
	
	elsif ( $order_param eq 'hierarchy.desc' )
	{
	    $order_clause = 'lft desc, element_name';
	}
	
	elsif ( $order_param eq 'name' || $order_param eq 'name.asc' )
	{
	    $order_clause = 'element_name';
	}
	
	elsif ( $order_param eq 'name.desc' )
	{
	    $order_clause = 'element_name desc';
	}
    }
    
    # Then construct the query.
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM $SPECELT_MAP as m join $SPECELT_DATA as e using (specelt_no)
        WHERE $filter_string
	GROUP BY specelt_no
	ORDER BY $order_clause
	$limit";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query.
    
    my ($unfiltered) = $dbh->selectall_arrayref($request->{main_sql}, { Slice => { } });
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
    
    # If there are no results, just return.
    
    return unless ref $unfiltered eq 'ARRAY' && @$unfiltered;
    
    # Otherwise go through the result set and remove all exclusions.
    
    my (@results, %exclude);
    
    foreach my $r ( @$unfiltered )
    {
	$exclude{$r->{specelt_no}} = 1 if $r->{exclude};
    }
    
    foreach my $r ( @$unfiltered )
    {
	push @results, $r unless $exclude{$r->{specelt_no}} && ! $ignore_exclude;
    }
    
    return $request->list_result(\@results);
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
    
    $record->{flags} = "N" unless $record->{occurrence_no};
    
    $record->{flags} = "D" if $record->{spec_orig_no} && $record->{orig_no} && 
	$record->{spec_orig_no} ne $record->{orig_no};
    
    if ( $record->{is_trace} || $record->{is_form} )
    {
	$record->{flags} ||= '';
	$record->{flags} .= 'I' if $record->{is_trace};
	$record->{flags} .= 'F' if $record->{is_form};
    }
    
    # If no taxon name is given for this occurrence, generate it from the
    # occurrence fields.
    
    $request->process_identification($record);
    
    # Now generate the 'difference' field if the accepted name and identified
    # name are different.
    
    $request->process_difference($record);
    
    my $a = 1;	# we can stop here when debugging
}


sub process_measurement_ids {
    
    my ($request, $record) = @_;
    
    return unless $request->{block_hash}{extids};
    
    # my $make_ids = $request->clean_param('extids');
    # $make_ids = 1 if ! $request->param_given('extids') && $request->output_vocab eq 'com';
    
    # return unless $make_ids;
        
    # $request->delete_output_field('record_type');
    
    $record->{specimen_no} = generate_identifier('SPM', $record->{specimen_no})
	if defined $record->{specimen_no} && $record->{specimen_no} ne '';

    $record->{measurement_no} = generate_identifier('MEA', $record->{measurement_no})
	if defined $record->{measurement_no} && $record->{measurement_no} ne '';
}


sub process_element_record {
    
    my ($request, $record) = @_;
    
    if ( $request->{block_hash}{extids} )
    {
	$record->{specelt_no} = generate_identifier('ELT', $record->{specelt_no})
	    if defined $record->{specelt_no} && $record->{specelt_no} ne '';
    }
}
