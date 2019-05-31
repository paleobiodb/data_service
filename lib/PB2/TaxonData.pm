
#  
# TaxonData
# 
# A class that returns information from the PaleoDB database about a single
# taxon or a category of taxa.  This is a subclass of DataService::Base.
# 
# Author: Michael McClennen

use strict;
use feature qw(unicode_strings fc say);

use lib '..';

package PB2::TaxonData;

use HTTP::Validate qw(:validators);
use Carp qw(carp croak);
use Try::Tiny;

use TaxonDefs qw(%TAXON_TABLE %TAXON_RANK %RANK_STRING %TAXONOMIC_STATUS %NOMENCLATURAL_STATUS
		 %UNS_NAME %UNS_RANK);
use TableDefs qw($PHYLOPICS $PHYLOPIC_NAMES);
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);
use Taxonomy;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::ReferenceData PB2::IntervalData);

our (%DB_FIELD);

our (@BASIC_1, @BASIC_2, @BASIC_3);

our (%UNS_FIELD) = ( 'NG' => 'genus', 'NF' => 'family',
		     'NO' => 'order', 'NC', => 'class', 'NP' => 'phylum' );

our (%UNS_ID) = ( 'NG' => 'genus_no', 'NF' => 'family_no',
		  'NO' => 'order_no', 'NC' => 'class_no', 'NP' => 'phylum_no' );


our (%LANGUAGE) = ( 'S' => 1 );

# This routine is called by the data service in order to initialize this
# class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    my $language_string = $ds->config_value('languages') || 'en';
    my @language_list = split(/\s*,\s*/, uc $language_string);
    $LANGUAGE{$_} = 1 foreach @language_list;
    
    # First define an output map to specify which output blocks are going to
    # be used to generate output from the operations defined in this class.
    
    @BASIC_1 = (
	{ value => 'full', maps_to => '1.2:taxa:full_info' },
	    "This is a shortcut for including all of the information that defines this record.  Currently, this",
	    "includes the following blocks: B<attr>, B<app>, B<common>, B<parent>",
	    "B<size>, B<class>, B<ecospace>, B<otaph>, B<etbasis>, B<refattr>.",
	    "If we subsequently add new data fields to this record",
	    "then B<full> will include those as well.  So if you are publishing a URL,",
	    "it might be a good idea to include B<C<show=full>>.",
	{ value => 'attr', maps_to => '1.2:taxa:attr' },
	    "The attribution of this taxonomic name (author and year)",
	{ value => 'app', maps_to => '1.2:taxa:app' },
	    "The age of first and last appearance of this taxon from all of the occurrences",
	    "recorded in this database");
    
    @BASIC_2 = (
	{ value => 'common' },
	    "The common name of this taxon, if one is entered in the database.",
	{ value => 'parent', maps_to => '1.2:taxa:parent' },
	    "If the classification of this taxon has been entered into the database,",
	    "the name of the parent taxon, or its senior synonym if there is one.",
	{ value => 'immparent', maps_to => '1.2:taxa:immpar' },
	    "You can use this instead of C<parent> if you wish to know the immediate",
	    "parent taxon.  If the immediate parent is a junior synonym, both it and",
	    "its senior synonym will be displayed.", 
	{ value => 'acconly' },
	    "Only return accepted names, and suppress the fields C<difference>,",
	    "C<accepted_name>, C<accepted_rank>, and C<accepted_no>, because they",
	    "are only relevant for non-accepted names.",
	{ value => 'size', maps_to => '1.2:taxa:size' },
	    "The number of subtaxa appearing in this database, including the taxon itself.",
	{ value => 'class', maps_to => '1.2:taxa:class' },
	    "The classification of this taxon: phylum, class, order, family, genus.  Also",
	    "includes the type taxon, if one is entered in the database.",
	    "This information is also included in the C<nav> block, so do not specify both at once.",
	{ value => 'classext', maps_to => '1.2:taxa:class' },
	    "Like C<class>, but also includes the relevant taxon identifiers.",
	{ value => 'phylo', maps_to => '1.2:taxa:class', undocumented => 1 },
	{ value => 'genus', maps_to => '1.2:taxa:genus', undocumented => 1 },
	    "The genus into which this taxon is classified, if its rank is genus or below.",
	{ value => 'subgenus', maps_to => '1.2:taxa:genus', undocumented => 1 },
	    "The genus into which this taxon is classified, including the subgenus if any.",
	{ value => 'subcounts', maps_to => '1.2:taxa:subcounts' },
	    "The number of subtaxa known to this database, summarized by rank.",
	{ value => 'ecospace', maps_to => '1.2:taxa:ecospace' },
	    "Information about ecological space that this organism occupies or occupied.",
	    "This has only been filled in for a relatively few taxa.  Here is a",
	    "L<list of values|node:general/ecotaph#Ecospace>.",
	{ value => 'ttaph', maps_to => '1.2:taxa:taphonomy' },
	    "Information about the taphonomy of this organism.  You can also use",
	    "the alias C<B<taphonomy>>. Here is a",
	    "L<list of values|node:general/ecotaph#Taphonomy>.",
	{ value => 'taphonomy', maps_to => '1.2:taxa:taphonomy', undocumented => 1 },
	{ value => 'etbasis', maps_to => '1.2:taxa:etbasis' },
	    "Annotates the output block C<ecospace>, indicating at which",
	    "taxonomic level each piece of information was entered.",
	{ value => 'pres', undocumented => 1 });
	# The above has been deprecated, its information is now included in
	# the 'flags' field.  But we keep it here to avoid errors.
    
    @BASIC_3 = (
	{ value => 'seq', maps_to => '1.2:taxa:seq' },
	    "The sequence numbers that mark this taxon's position in the tree.",
	{ value => 'img', maps_to => '1.2:taxa:img' },
	    "The identifier of the image (if any) associated with this taxon.",
	    "These images are sourced from L<phylopic.org|http://phylopic.org/>.",
        { value => 'ref', maps_to => '1.2:refs:primary' },
	    "The reference from which this taxonomic name was entered, as formatted text.",
        { value => 'refattr', maps_to => '1.2:refs:attr' },
	    "The author(s) and year of publication of the reference.  Note that this",
	    "may be different from the attribution of the name itself, if the reference",
	    "is a secondary source.",
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the collection record");
     
    $ds->define_output_map('1.2:taxa:single_output_map' =>
	@BASIC_1,
	@BASIC_2,
	{ value => 'nav', maps_to => '1.2:taxa:nav' },
	    "Additional information for the PBDB Navigator taxon browser.",
	    "This block should only be selected if the output format is C<json>.", 
	@BASIC_3);
    
    $ds->define_output_map('1.2:taxa:mult_output_map' =>
	@BASIC_1,
	@BASIC_2,
	@BASIC_3);
    
    # Now define all of the output blocks that were not defined elsewhere.
    
    $ds->define_block('1.2:taxa:basic' =>
	{ select => ['DATA'] },
	{ set => '*', code => \&process_difference },
	{ set => '*', code => \&process_pbdb, if_vocab => 'pbdb' },
	{ set => '*', code => \&process_com, if_vocab => 'com' },
	{ set => '*', code => \&process_taxon_ids },
	{ output => 'orig_no', dwc_name => 'taxonID', com_name => 'oid' },
	    "A unique identifier for this taxonomic name",
	{ output => 'taxon_no', com_name => 'vid', not_field => 'no_variant' },
	    "A unique identifier for the selected variant",
	    "of this taxonomic name.  By default, this is the variant currently",
	    "accepted as most correct.",
	{ output => 'record_type', com_name => 'typ', value => $IDP{TXN}, dwc_value => 'Taxon' },
	    "The type of this object: C<$IDP{TXN}> for a taxon.",
	{ output => 'flags', com_name => 'flg' },
	    "This field will be empty for most records.  Otherwise, it will contain one or more",
	    "of the following letters:", "=over",
	    "=item B", "This taxon is one of the ones specified explicitly in the query.  If the result",
	        "is a subtree, this represents the 'base'.",
	    "=item E", "This taxon was specified in the query as an exclusion.",
	    "=item V", "This taxonomic name is a variant that is not currently accepted.",
	    "=item I", "This taxon is an ichnotaxon.",
	    "=item F", "This taxon is a form taxon.",
	{ set => 'taxon_rank', if_vocab => 'pbdb,dwc', lookup => \%RANK_STRING },
	{ set => 'accepted_rank', if_vocab => 'pbdb,dwc', lookup => \%RANK_STRING },
	{ output => 'taxon_rank', dwc_name => 'taxonRank', com_name => 'rnk' },
	    "The rank of this taxon, ranging from subspecies up to kingdom",
	{ output => 'taxon_name', dwc_name => 'scientificName', com_name => 'nam' },
	    "The scientific name of this taxon",
	{ output => 'taxon_attr', if_block => 'attr', 
	  dwc_name => 'scientificNameAuthorship', com_name => 'att' },
	    "The attribution (author and year) of this taxonomic name",
	{ output => 'common_name', dwc_name => 'vernacularName', com_name => 'nm2', if_block => 'common,full' },
	    "The common (vernacular) name of this taxon, if any",
	{ output => 'difference', com_name => 'tdf', not_block => 'acconly' },
	    "If this name is either a junior synonym or is invalid for some reason,",
	    "this field gives the reason.  The fields C<accepted_no>",
	    "and C<accepted_name> then specify the name that should be used instead.",
	{ set => 'tax_status', from => 'status', lookup => \%TAXONOMIC_STATUS, if_vocab => 'dwc' },
	{ output => 'tax_status', dwc_name => 'taxonomicStatus', if_vocab => 'dwc' },
	    "The taxonomic status of this name, in the Darwin Core vocabulary.",
	    "This field only appears if that vocabulary is selected.",
	{ set => 'nom_status', from => 'status', lookup => \%NOMENCLATURAL_STATUS, if_vocab => 'dwc' },
	{ output => 'nom_status', dwc_name => 'nomenclaturalStatus', if_vocab => 'dwc' },
	    "The nomenclatural status of this name, in the Darwin Core vocabulary.",
	    "This field only appears if that vocabulary is selected.",
	{ output => 'accepted_no', dwc_name => 'acceptedNameUsageID', pbdb_name => 'accepted_no', 
	  com_name => 'acc', not_block => 'acconly' },
	    "If this name is either a junior synonym or an invalid name, this field gives",
	    "the identifier of the accepted name to be used in its place.  Otherwise, its value",
	    "will be the same as C<orig_no>.  In the compact vocabulary, this field",
	    "will be omitted in that case.",
	{ output => 'accepted_rank', com_name => 'acr', not_block => 'acconly' },
	    "If C<accepted_no> is different from C<orig_no>, this field",
	    "gives the rank of the accepted name.  Otherwise, its value will",
	    "be the same as C<taxon_rank>.  In the compact voabulary, this field",
	    "will be omitted in that case.",
	{ output => 'accepted_name', dwc_name => 'acceptedNameUsage', pbdb_name => 'accepted_name',
	  com_name => 'acn', not_block => 'acconly' },
	    "If C<accepted_no> is different from C<orig_no>, this field gives the",
	    "accepted name.  Otherwise, its value will be",
	    "the same as C<taxon_name>.  In the compact vocabulary, this field",
	    "will be omitted in that case.",
	{ output => 'senpar_no', pbdb_name => 'parent_no', dwc_name => 'parentNameUsageID', com_name => 'par' }, 
	    "The identifier of the parent taxon, or of its senior synonym if there is one.",
	    "This field and those following are only available if the classification of",
	    "this taxon is known to the database.",
	{ output => 'senpar_name', com_name => 'prl', pbdb_name => 'parent_name', if_block => 'parent,immparent' },
	    "The name of the parent taxon, or of its senior synonym if there is one.",
	{ output => 'immpar_no', dwc_name => 'parentNameUsageID', com_name => 'ipn',
	  pbdb_name => 'immpar_no', if_block => 'immparent', dedup => 'senpar_no' },
	    "The identifier of the immediate parent taxon, even if it is a junior synonym.",
	{ output => 'immpar_name', dwc_name => 'parentNameUsageID', com_name => 'ipl',
	  if_block => 'immparent', dedup => 'senpar_name' },
	    "The name of the immediate parent taxon, even if it is a junior synonym.",
	{ output => 'container_no', com_name => 'ctn', dedup => 'senpar_no' },
	    "The identifier of a taxon from the result set containing this one, which",
	    "may or may not be the parent.  This field will only appear in the result",
	    "of the L<occs/taxa|node:occs/taxa> operation, where no base taxon is",
	    "specified.  The taxa reported in this case are the \"classical\" ranks,",
	    "rather than the full taxonomic hierarcy.",
	{ output => 'ref_author', dwc_name => 'recordedBy', com_name => 'aut', if_block => '1.2:refs:attr' },
	    "The author(s) of the reference from which this name was entered.  Note that",
	    "the author of the name itself may be different if the reference is a secondary source.",
	{ output => 'ref_pubyr', com_name => 'pby', if_block => '1.2:refs:attr' },
	    "The year of publication of the reference from which this name was entered.  Note that",
	    "the publication year of the name itself may be different if the reference is a secondary source.",
	{ output => 'reference_no', com_name => 'rid' },
	    "The identifier of the reference from which this name was entered.",
	{ output => 'is_extant', com_name => 'ext', dwc_name => 'isExtant' },
	    "True if this taxon is extant on earth today, false if not, not present if unrecorded",
	{ output => 'n_occs', com_name => 'noc' },
	    "The number of fossil occurrences in this database that are identified",
	    "as belonging to this taxon or any of its subtaxa.");
    
    $ds->define_block('1.2:taxa:reftaxa' =>
	{ select => ['REFTAXA_DATA'] },
	{ set => '*', code => \&process_difference },
	{ set => '*', code => \&process_pbdb, if_vocab => 'pbdb' },
	{ set => '*', code => \&process_com, if_vocab => 'com' },
	{ set => '*', code => \&process_taxon_ids },
	{ set => 'ref_type', from => '*', code => \&PB2::ReferenceData::set_reference_type, 
	  if_vocab => 'pbdb' },
	{ output => 'orig_no', dwc_name => 'taxonID', com_name => 'tid' },
	    "A unique identifier for this taxonomic name",
	{ output => 'taxon_no', com_name => 'vid', not_field => 'no_variant' },
	    "A unique identifier for the variant of this taxonomic name that was actually",
	    "mentioned in the reference.",
	{ output => 'record_type', com_name => 'typ', value => $IDP{TXN}, dwc_value => 'Taxon' },
	    "The type of this object: C<$IDP{TXN}> for an occurrence.",
	{ output => 'reference_no', com_name => 'rid' },
	    "=for wds_anchor reference_no",
	    "The identifier of a reference in which this taxonomic name was mentioned.",
	{ output => 'ref_type', com_name => 'rtp' },
	    "=for wds_anchor ref_type",
	    "The relationship between this name and the indicated reference.  Values will be one or",
	    "more of the following, as a comma-separated list:",
	    $ds->document_set('1.2:refs:reftype'),
	{ output => 'flags', com_name => 'flg' },
	    "This field will be empty for most records.  In a record representing a name variant",
	    "that is not the one currently accepted, the field will contain C<V>.  This",
	    "last is suppressed in the compact vocabulary, because one can simply check",
	    "for the presence of the field C<vid> instead.  In a record representing a",
	    "name variant that was not actually mentioned in the corresponding reference,",
	    "the field will contain C<A>.  This will only happen if the parameter",
	    "C<variant=all> was given.",
	{ set => 'taxon_rank', if_vocab => 'com', lookup => \%TAXON_RANK },
	{ set => 'accepted_rank', if_vocab => 'pbdb', lookup => \%RANK_STRING },
	{ output => 'taxon_rank', dwc_name => 'taxonRank', com_name => 'rnk' },
	    "The rank of this taxon as mentioned in the reference, ranging from subspecies up to kingdom",
	{ output => 'taxon_name', dwc_name => 'scientificName', com_name => 'nam' },
	    "The taxonomic name actually mentioned in the reference.",
	{ output => 'taxon_attr', if_block => 'attr', 
	  dwc_name => 'scientificNameAuthorship', com_name => 'att' },
	    "The attribution (author and year) of this taxonomic name",
	{ output => 'common_name', dwc_name => 'vernacularName', com_name => 'nm2', if_block => 'common,full' },
	    "The common (vernacular) name of this taxon, if any",
	{ output => 'difference', com_name => 'tdf' },
	    "If this name is either a junior synonym or is invalid for some reason,",
	    "this field gives the reason.  The fields C<accepted_no>",
	    "and C<accepted_name> then specify the name that should be used instead.",
	{ set => 'tax_status', from => 'status', lookup => \%TAXONOMIC_STATUS, if_vocab => 'dwc' },
	{ output => 'tax_status', dwc_name => 'taxonomicStatus', if_vocab => 'dwc' },
	    "The taxonomic status of this name, in the Darwin Core vocabulary.",
	    "This field only appears if that vocabulary is selected.",
	{ set => 'nom_status', from => 'status', lookup => \%NOMENCLATURAL_STATUS, if_vocab => 'dwc' },
	{ output => 'nom_status', dwc_name => 'nomenclaturalStatus', if_vocab => 'dwc' },
	    "The nomenclatural status of this name, in the Darwin Core vocabulary.",
	    "This field only appears if that vocabulary is selected.",
	{ output => 'accepted_no', dwc_name => 'acceptedNameUsageID', pbdb_name => 'accepted_no', 
	  com_name => 'acc' },
	    "If this name is either a junior synonym or an invalid name, this field gives",
	    "the identifier of the accepted name to be used in its place.  Otherwise, its value",
	    "will be the same as C<orig_no>.  In the compact vocabulary, this field",
	    "will be omitted in that case.",
	{ output => 'accepted_rank', com_name => 'acr' },
	    "If C<accepted_no> is different from C<orig_no>, this field",
	    "gives the rank of the accepted name.  Otherwise, its value will",
	    "be the same as C<taxon_rank>.  In the compact voabulary, this field",
	    "will be omitted in that case.",
	{ output => 'accepted_name', dwc_name => 'acceptedNameUsage', pbdb_name => 'accepted_name',
	  com_name => 'acn' },
	    "If C<accepted_no> is different from C<orig_no>, this field gives the",
	    "accepted name.  Otherwise, its value will be",
	    "the same as C<taxon_name>.  In the compact vocabulary, this field",
	    "will be omitted in that case.",
	{ output => 'senpar_no', pbdb_name => 'parent_no',
	  dwc_name => 'parentNameUsageID', com_name => 'par' }, 
	    "The identifier of the parent taxon, or of its senior synonym if there is one.",
	    "This field and those following are only available if the classification of",
	    "this taxon is known to the database.",
	{ output => 'senpar_name', com_name => 'prl', pbdb_name => 'parent_name', if_block => 'parent,immparent' },
	    "The name of the parent taxon, or of its senior synonym if there is one.",
	{ output => 'immpar_no', pbdb_name => 'immpar_no', dwc_name => 'parentNameUsageID', 
	  com_name => 'ipn', if_block => 'full,immparent', dedup => 'senpar_no' },
	    "The identifier of the immediate parent taxon, even if it is a junior synonym.",
	{ output => 'immpar_name', dwc_name => 'parentNameUsageID', com_name => 'ipl',
	  if_block => 'immparent', dedup => 'senpar_name' },
	    "The name of the immediate parent taxon, even if it is a junior synonym.",
	{ output => 'ref_author', dwc_name => 'recordedBy', com_name => 'aut', if_block => '1.2:refs:attr' },
	    "The author(s) of the reference from which this name was entered.  Note that",
	    "the author of the name itself may be different if the reference is a secondary source.",
	{ output => 'ref_pubyr', com_name => 'pby', if_block => '1.2:refs:attr' },
	    "The year of publication of the reference from which this name was entered.  Note that",
	    "the publication year of the name itself may be different if the reference is a secondary source.",
	{ output => 'is_extant', com_name => 'ext', dwc_name => 'isExtant' },
	    "True if this taxon is extant on earth today, false if not, not present if unrecorded",
	{ output => 'n_occs', com_name => 'noc' },
	    "The number of fossil occurrences in this database that are identified",
	    "as belonging to this taxon or any of its subtaxa.");
    
    $ds->define_block('1.2:taxa:attr' =>
	{ select => 'ATTR' });
    
    $ds->define_block('1.2:taxa:parent' =>
	{ select => 'SENPAR' });
    
    $ds->define_block('1.2:taxa:immpar' =>
	{ select => 'SENPAR,IMMPAR' });
    
    $ds->define_block('1.2:taxa:size' =>
	{ select => 'SIZE' },
	{ output => 'taxon_size', com_name => 'siz' },
	    "The total number of taxa in the database that are contained within this taxon, including itself",
	{ output => 'extant_size', com_name => 'exs' },
	    "The total number of extant taxa in the database that are contained within this taxon, including itself");
    
    $ds->define_block('1.2:taxa:app' =>
	{ select => 'APP' },
	{ set => '*', code => \&process_ages },
	{ output => 'firstapp_ea', name => 'firstapp_max_ma', com_name => 'fea', dwc_name => 'firstAppearanceEarlyAge', 
	  if_block => 'app' },
	    "The early age bound for the first appearance of this taxon in the database",
	{ output => 'firstapp_la', name => 'firstapp_min_ma', com_name => 'fla', dwc_name => 'firstAppearanceLateAge', 
	  if_block => 'app' }, 
	    "The late age bound for the first appearance of this taxon in the database",
	{ output => 'lastapp_ea', name => 'lastapp_max_ma', com_name => 'lea', dwc_name => 'lastAppearanceEarlyAge',
	  if_block => 'app' },
	    "The early age bound for the last appearance of this taxon in the database",
	{ output => 'lastapp_la', name => 'lastapp_min_ma', com_name => 'lla', dwc_name => 'lastAppearanceLateAge',
	  if_block => 'app' }, 
	    "The late age bound for the last appearance of this taxon in the database",
	{ output => 'early_interval', com_name => 'tei' },
	    "The name of the interval in which this taxon first appears, or the start of its range.",
	{ output => 'late_interval', com_name => 'tli', dedup => 'early_interval' },
	    "The name of the interval in which this taxon last appears, if different from C<early_interval>.");
    
    $ds->define_block('1.2:taxa:occapp' =>
	{ set => '*', code => \&process_ages },
	{ output => 'firstocc_ea', name => 'firstocc_max_ma', com_name => 'foa', dwc_name => 'firstAppearanceEarlyAge' },
	    "The early age bound for the first appearance of this taxon in the set of",
	    "occurrences being analyzed.",
	{ output => 'firstocc_la', name => 'firstocc_min_ma', com_name => 'fpa', dwc_name => 'firstAppearanceLateAge' }, 
	    "The late age bound for the first appearance of this taxon in the set of",
	    "occurrences being analyzed.",
	{ output => 'lastocc_ea', name => 'lastocc_max_ma', com_name => 'loa', dwc_name => 'lastAppearanceEarlyAge' },
	    "The early age bound for the last appearance of this taxon in the set of",
	    "occurrences being analyzed.",
	{ output => 'lastocc_la', name => 'lastocc_min_ma', com_name => 'lpa', dwc_name => 'lastAppearanceLateAge' }, 
	    "The late age bound for the last appearance of this taxon in the set of",
	    "occurrences being analyzed.");
	# { output => 'occ_early_interval', com_name => 'oei' },
	#     "The name of the interval in which this taxon first appears, or the start of its range.",
	# { output => 'occ_late_interval', com_name => 'oli', dedup => 'early_interval' },
	#     "The name of the interval in which this taxon last appears, if different from C<early_interval>.");
    
    $ds->define_block('1.2:taxa:subtaxon' =>
	{ output => 'orig_no', com_name => 'oid', dwc_name => 'taxonID' },
	{ output => 'taxon_no', com_name => 'vid', not_field => 'no_variant' },
	{ output => 'record_type', com_name => 'typ', com_value => 'txn', not_field => 'no_recordtype' },
	{ output => 'taxon_rank', com_name => 'rnk', dwc_name => 'taxonRank' },
	{ output => 'taxon_name', com_name => 'nam', dwc_name => 'scientificName' },
	{ output => 'accepted_no', com_name => 'acc', dwc_name => 'acceptedNameUsageID', dedup => 'orig_no' },
	{ output => 'taxon_size', com_name => 'siz' },
	{ output => 'extant_size', com_name => 'exs' },
	{ output => 'firstapp_ea', pbdb_name => 'firstapp_max_ma', com_name => 'fea' });
    
    $ds->define_block('1.2:taxa:class' =>
	{ select => [ 'CLASS', 'GENUS', 'TYPE_TAXON' ] },
	{ set => '*', code => \&process_classification },
	#{ output => 'kingdom', com_name => 'kgl' },
	#    "The name of the kingdom in which this taxon occurs",
	{ output => 'phylum', com_name => 'phl' },
	    "The name of the phylum in which this taxon is classified",
	{ output => 'phylum_no', com_name => 'phn', if_block => 'classext' },
	    "The identifier of the phylum in which this taxon is classified.",
	    "This is only included with the block C<classext>.",
	{ output => 'class', com_name => 'cll' },
	    "The name of the class in which this taxon is classified",
	{ output => 'class_no', com_name => 'cln', if_block => 'classext' },
	    "The identifier of the class in which this taxon is classified.",
	    "This is only included with the block C<classext>.",
	{ output => 'order', com_name => 'odl' },
	    "The name of the order in which this taxon is classified",
	{ output => 'order_no', com_name => 'odn', if_block => 'classext' },
	    "The identifier of the order in which this occurrence is classified.",
	    "This is only included with the block C<classext>.",
	{ output => 'family', com_name => 'fml' },
	    "The name of the family in which this taxon is classified",
	{ output => 'family_no', com_name => 'fmn', if_block => 'classext' },
	    "The identifier of the family in which this occurrence is classified.",
	    "This is only included with the block C<classext>.",
	{ output => 'genus', com_name => 'gnl' },
	    "The name of the genus in which this taxon is classified.  A genus may",
	    "be listed as occurring in a different genus if it is a junior synonym; a species may",
	    "be listed as occurring in a different genus than its name would",
	    "indicate if its genus is synonymized but no synonymy opinion has",
	    "been entered for the species.",
	{ output => 'genus_no', com_name => 'gnn', if_block => 'classext' },
	    "The identifier of the genus in which this occurrence is classified.",
	    "This is only included with the block C<classext>.",
	{ output => 'subgenus_no', com_name => 'sgn', if_block => 'classext', dedup => 'genus_no' },
	    "The identifier of the subgenus in which this occurrence is classified,",
	    "if any.  This is only included with the block C<classext>.",
	{ output => 'type_taxon', com_name => 'ttl' },
	    "The name of the type taxon for this taxon, if known.",
	{ output => 'type_taxon_no', com_name => 'ttn', if_block => 'classext' },
	    "The identifier of the type taxon for this taxon, if known.",
	{ set => '*', code => \&process_subgenus, if_block => 'subgenus' });
    
    $ds->define_block('1.2:taxa:genus' =>
	{ select => 'GENUS' },
	{ set => '*', code => \&process_subgenus, if_block => 'subgenus' },
	{ output => 'genus', com_name => 'gnl' },
	    "The name of the genus in which this taxon occurs.  If the block C<subgenus>",
	    "was included, the value of this field will include the subgenus if any.");
    
    $ds->define_block('1.2:taxa:subcounts' => 
	{ select => 'COUNTS' },
	{ output => 'n_orders', com_name => 'odc' },
	    "The number of orders within this taxon.  For lists of taxa derived",
	    "from a set of occurrences, this will be the number of orders that",
	    "appear within that set.  Otherwise, this will be the total number",
	    "of orders within this taxon that are known to the database.",
	{ output => 'n_families', com_name => 'fmc' },
	    "The number of families within this taxon, according to the same rules",
	    "as C<n_orders> above.",
	{ output => 'n_genera', com_name => 'gnc' },
	    "The number of genera within this taxon, according to the same rules",
	    "as C<n_orders> above.",
	{ output => 'n_species', com_name => 'spc' },
	    "The number of species within this taxon, according to the same rules",
	    "as C<n_orders> above.");
    
    $ds->define_block('1.2:taxa:nav' =>
	{ select => ['SENPAR', 'IMMPAR', 'CLASS', 'COUNTS'] },
	{ output => 'senpar_name', com_name => 'prl' },
	    "The name of the parent taxon or its senior synonym if any",
	{ output => 'senpar_rank', com_name => 'prr' },
	    "The rank of the parent taxon or its senior synonym if any",
	{ output => 'immpar_name', com_name => 'ipl', dedup => 'prl' },
	    "The name of the immediate parent taxon if it is a junior synonym",
	{ output => 'parent_txn', com_name => 'prt', sub_record => '1.2:taxa:subtaxon' },
	{ output => 'kingdom_no', com_name => 'kgn' },
	    "The identifier of the kingdom in which this taxon occurs",
	{ output => 'kingdom', com_name => 'kgl' },
	    "The name of the kingdom in which this taxon occurs",
	{ output => 'kingdom_txn', com_name => 'kgt', sub_record => '1.2:taxa:subtaxon' },
	{ output => 'phylum_no', com_name => 'phn' },
	    "The identifier of the phylum in which this taxon occurs",
	{ output => 'phylum', com_name => 'phl' },
	    "The name of the phylum in which this taxon occurs",
	{ output => 'phylum_txn', com_name => 'pht', sub_record => '1.2:taxa:subtaxon' },
	{ output => 'phylum_count', com_name => 'phc' },
	    "The number of phyla within this taxon",
	{ output => 'class_no', com_name => 'cln' },
	    "The identifier of the class in which this taxon occurs",
	{ output => 'class', com_name => 'cll' },
	    "The name of the class in which this taxon occurs",
	{ output => 'class_txn', com_name => 'clt', sub_record => '1.2:taxa:subtaxon' },
	{ output => 'class_count', com_name => 'clc' },
	    "The number of classes within this taxon",
	{ output => 'order_no', com_name => 'odn' },
	    "The identifier of the order in which this taxon occurs",
	{ output => 'order', com_name => 'odl' },
	    "The name of the order in which this taxon occurs",
	{ output => 'order_txn', com_name => 'odt', sub_record => '1.2:taxa:subtaxon' },
	{ output => 'order_count', com_name => 'odc' },
	    "The number of orders within this taxon",
	{ output => 'family_no', com_name => 'fmn' },
	    "The identifier of the family in which this taxon occurs",
	{ output => 'family', com_name => 'fml' },
	    "The name of the family in which this taxon occurs",
	{ output => 'family_txn', com_name => 'fmt', sub_record => '1.2:taxa:subtaxon' },
	{ output => 'family_count', com_name => 'fmc' },
	    "The number of families within this taxon",
	{ output => 'genus_count', com_name => 'gnc' },
	    "The number of genera within this taxon",
    
	{ output => 'children', com_name => 'chl', sub_record => '1.2:taxa:subtaxon' },
	    "The immediate children of this taxonomic concept, if any",
	{ output => 'phylum_list', com_name => 'phs', sub_record => '1.2:taxa:subtaxon' },
	    "A list of the phyla within this taxonomic concept",
	{ output => 'class_list', com_name => 'cls', sub_record => '1.2:taxa:subtaxon' },
	    "A list of the classes within this taxonomic concept",
	{ output => 'order_list', com_name => 'ods', sub_record => '1.2:taxa:subtaxon' },
	    "A list of the orders within this taxonomic concept",
	{ output => 'family_list', com_name => 'fms', sub_record => '1.2:taxa:subtaxon' },
	    "A list of the families within this taxonomic concept",
	{ output => 'genus_list', com_name => 'gns', sub_record => '1.2:taxa:subtaxon' },
	    "A list of the genera within this taxonomic concept",
	{ output => 'subgenus_list', com_name => 'sgs', sub_record => '1.2:taxa:subtaxon' },
	    "A list of the subgenera within this taxonomic concept",
	{ output => 'species_list', com_name => 'sps', sub_record => '1.2:taxa:subtaxon' },
	    "A list of the species within this taxonomic concept",
 	{ output => 'subspecies_list', com_name => 'sss', sub_record => '1.2:taxa:subtaxon' },
	    "A list of the subspecies within this taxonomic concept");
    
    $ds->define_block('1.2:taxa:img' =>
	{ select => 'image_no' },
	{ output => 'image_no', com_name => 'img' },
    	    "If this value is non-zero, you can use it to construct image URLs",
	    "using L<taxa/thumb|node:taxa/thumb> and L<taxa/icon|node:taxa/icon>.");
    
    $ds->define_block('1.2:taxa:auto' =>
	{ output => 'taxon_no', dwc_name => 'taxonID', com_name => 'oid' },
	    "A positive integer that uniquely identifies this taxonomic name",
	{ output => 'record_type', com_name => 'typ', com_value => 'txn', dwc_value => 'Taxon', value => $IDP{TXN} },
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
    
    $ds->define_block('1.2:taxa:imagedata' =>
	{ select => [ 'p.image_no', 'p.uid', 'p.modified', 'p.credit', 'p.license' ] },
	{ set => '*', code => \&process_com },
	{ set => '*', code => \&process_image_ids },
	{ output => 'image_no', com_name => 'oid' },
	    "A unique identifier for this image, generated locally by this database",
	{ output => 'record_type', com_name => 'typ', com_value => 'img', value => $IDP{PHP} },
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
    
    $ds->define_block('1.2:taxa:ecospace' =>
	{ select => 'ECOSPACE' },
	{ output => 'taxon_environment', com_name => 'jev' },
	    "The general environment or environments in which this life form is found.",
	    "See L<ecotaph vocabulary|node:general/ecotaph#Ecospace>.",
	{ output => 'environment_basis', com_name => 'jec' },
	    "Specifies the taxon from which the environment information is",
	    "inherited.",
	#     "Here is a L<list of values|node:taxa/ecotaph_values>.",
	{ output => 'motility', com_name => 'jmo' },
	    "Whether the organism is motile, attached and/or epibiont, and its",
	    "mode of locomotion if any.",
	    "See L<ecotaph vocabulary|node:general/ecotaph#Ecospace>.",
	{ output => 'motility_basis', com_name => 'jmc',
	  if_block => 'etbasis' }, # , if_format => ['txt', 'csv', 'tsv']
	    "Specifies the taxon for which the motility information was set.",
	    "The taphonomy and ecospace information are inherited from parent",
	    "taxa unless specific values are set.",
	    # "For L<JSON|node:formats/json> responses, the fields 'jmb' and 'jmn'",
	    # "give the taxon identifier and taxon name respectively, while for",
	    # "L<text|node:formats/text> responses, the field 'motility_basis'",
	    # "provides both.  These fields are only included if the C<ecospace> output",
	    # "block is also included.  Similar annotation fields are included",
	    # "for the following, if the C<etbasis> output block is included.",
	# { output => 'motility_basis_no', com_name => 'jmb',
	#   if_block => 'etbasisext', if_format => 'json' },
	# { output => 'motility_basis', com_name => 'jmn',
	#   if_block => 'etbasis', if_format => 'json' },
	{ output => 'life_habit', com_name => 'jlh' },
	    "The general life mode and locality of this organism.",
	    "See L<ecotaph vocabulary|node:general/ecotaph#Ecospace>.",
	{ output => 'life_habit_basis', com_name => 'jhc',
	  if_block => 'etbasis' }, # , if_format => ['txt', 'csv', 'tsv']
	    "Specifies the taxon for which the life habit information was set.",
	    "See B<motility_basis> above.  These fields are only included if the",
	    "C<ecospace> block is also included.",
	# { output => 'life_habit_basis_no', com_name => 'jhb',
	#   if_block => 'etbasisext', if_format => 'json' },
	# { output => 'life_habit_basis', com_name => 'jhn',
	#   if_block => 'etbasis', if_format => 'json' },
	{ output => 'vision', com_name => 'jvs' },
	    "The degree of vision possessed by this organism.",
	    "See L<ecotaph vocabulary|node:general/ecotaph#Ecospace>.",
	{ output => 'vision_basis', com_name => 'jvc', if_block => 'etbasis' },
	    "Specifies the taxon for which the vision information was set.",
	    "See B<motility_basis> above.  These fields are only included if the",
	    "C<ecospace> block is also included.",
	{ output => 'diet', com_name => 'jdt' },
	    "The general diet or feeding mode of this organism.",
	    "See L<ecotaph vocabulary|node:general/ecotaph#Ecospace>.",
	{ output => 'diet_basis', com_name => 'jdc',
	  if_block => 'etbasis' }, # , if_format => ['txt', 'csv', 'tsv']
	    "Specifies the taxon for which the diet information was set.",
	    "See B<motility_basis> above.  These fields are only included if the",
	    "C<ecospace> block is also included.",
	{ output => 'reproduction', com_name => 'jre' },
	    "The mode of reproduction of this organism.",
	    "See L<ecotaph vocabulary|node:general/ecotaph#Ecospace>.",
	{ output => 'reproduction_basis', com_name => 'jrc', if_block => 'etbasis' },
	    "Specifies the taxon for which the reproduction information was set.",
	    "See B<motility_basis> above.  These fields are only included if the",
	    "C<ecospace> block is also included.",
	{ output => 'ontogeny', com_name => 'jon' },
	    "Briefly describes the ontogeny of this organism.",
	    "See L<ecotaph vocabulary|node:general/ecotaph#Ecospace>.",
	{ output => 'ontogeny_basis', com_name => 'joc', if_block => 'etbasis' },
	    "Specifies the taxon for which the ontogeny information was set.",
	    "See B<motility_basis> above.  These fields are only included if the",
	    "C<ecospace> block is also included.",
	{ output => 'ecospace_comments', com_name => 'jcm' },
	    "Additional remarks about the ecospace, if any.",
	# { output => 'diet_basis_no', com_name => 'jdb',
	#   if_block => 'etbasisext', if_format => 'json' },
	# { output => 'diet_basis', com_name => 'jdn',
	#   if_block => 'etbasis', if_format => 'json' }
	);
    
    $ds->define_block('1.2:taxa:taphonomy' =>
	{ select => 'TAPHONOMY' },
	{ output => 'composition', com_name => 'jco' },
	    "The composition of the skeletal parts of this organism.",
	    "See L<taphonomy vocabulary|node:general/ecotaph#Taphonomy>.",
	{ output => 'architecture', com_name => 'jsa' },
	    "An indication of the internal skeletal architecture.",
	    "See L<taphonomy vocabulary|node:general/ecotaph#Taphonomy>.",
	{ output => 'thickness', com_name => 'jth' },
	    "An indication of the relative thickness of the skeleton.",
	    "See L<taphonomy vocabulary|node:general/ecotaph#Taphonomy>.",
	{ output => 'reinforcement', com_name => 'jsr' },
	    "An indication of the skeletal reinforcement, if any.",
	    "See L<taphonomy vocabulary|node:general/ecotaph#Taphonomy>.",
	{ output => 'taphonomy_basis', com_name => 'jtc',
	  if_block => 'etbasis' }, # , if_format => ['txt', 'csv', 'tsv']
	    "Specifies the taxon for which the taphonomy information was set.",
	    "See B<motility_basis> above.  These fields are only included if the",
	    "C<otaph> block is also included.",
	# { output => 'taphonomy_basis_no', com_name => 'jtb', if_field => 'taphonomy_basis_no', 
	#   if_block => 'etbasisext', if_format => 'json' },
	# { output => 'taphonomy_basis', com_name => 'jtn',
	#   if_block => 'etbasis', if_format => 'json' }
	);
    
    $ds->define_block('1.2:taxa:etbasis' =>
	{ select => 'TAPHBASIS', if_block => '1.2:taxa:taphonomy' },
	{ select => 'ECOBASIS', if_block => '1.2:taxa:ecospace' },
	# { output => 'environment_basis_no', com_name => 'jnb',
	#   if_block => 'etbasis', if_format => 'json' },
	#     "Specifies the taxon for which the ",
	# { output => 'environment_basis', com_name => 'jnn',
	#   if_block => 'etbasis', if_format => 'json' },
	# { output => 'environment_basis', com_name => 'jnc',
	#   if_block => 'etbasis', if_format => ['txt', 'csv', 'tsv'] },
	#{ set => '*', code => \&PB2::TaxonData::consolidate_basis, if_format => ['txt', 'csv', 'tsv'] }
	);
    
    # $ds->define_block('1.2:taxa:pres' =>
    # 	{ select => 'PRES' },
    # 	{ output => 'preservation', com_name => 'prs' },
    # 	    "Indicates whether this is an C<ichnotaxon> or a C<form taxon>.",
    # 	    "if blank, then this is a regular taxon.  In the compact vocabulary,",
    # 	    "the values are C<I> and C<F>.");
    
    $ds->define_block('1.2:taxa:seq' =>
	{ output => 'lft', com_name => 'lsq' },
	    "This number gives the taxon's position in a preorder traversal of the taxonomic tree.",
	{ output => 'rgt', com_name => 'rsq' },
	    "This number greater than or equal to the maximum of the sequence numbers of all of",
	    "this taxon's subtaxa, and less than the sequence of any succeeding taxon in the sequence.",
	    "You can use this, along with C<lft>, to determine subtaxon relationships.  If the pair",
	    "C<lft,rgt> for taxon <A> is bracketed by the pair C<lft,rgt> for taxon <B>, then C<A> is",
	    "a subtaxon of C<B>.");
    
    $ds->define_block( '1.2:taxa:full_info' =>
	{ include => '1.2:taxa:attr' },
	{ include => '1.2:taxa:app' },
	{ include => '1.2:taxa:parent' },
	{ include => '1.2:taxa:size' },
	{ include => '1.2:taxa:class' },
	{ include => '1.2:taxa:ecospace' },
	{ include => '1.2:taxa:taphonomy' },
	{ include => '1.2:taxa:etbasis' });
	# { include => '1.2:taxa:pres' });
    
    # Now define output blocks for opinions
    
    $ds->define_output_map('1.2:opinions:output_map' =>
	{ value => 'full', maps_to => '1.2:opinions:full_info' },
	    "This is a shortcut for including all of the information that defines this record.  Currently, this",
	    "includes the following blocks: B<basis>, B<refattr>.",
	{ value => 'attr', maps_to => '1.2:opinions:attr' },
	    "The attribution of the taxonomic names mentioned in this opinion, by author and year.",
	{ value => 'basis' },
	    "The basis of the opinion, which will be one of the following:",
	    "=over", "=item stated with evidence", "=item stated without evidence",
			   "=item implied", "=item second hand", "=back",
	{ value => 'seq', maps_to => '1.2:opinions:seq' },
	    "The sequence numbers that mark the subject taxon's position in the tree.",
        { value => 'ref', maps_to => '1.2:refs:primary' },
	    "The reference from which the opinion was entered, as formatted text.",
        { value => 'refattr', maps_to => '1.2:refs:attr' },
	    "The author(s) and year of publication of the reference from which",
	    "the opinion was entered.",
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
	{ value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the opinion record");
    
    our (%pbdb_opinion_code) = ( 'C' => 'class', 'U' => 'unsel', 'X' => 'suppressed' );
    our (%basis_code) = ( 3 => 'stated with evidence', 2 => 'stated without evidence',
			  1 => 'implied', 0 => 'second hand' );
    
    $ds->define_block('1.2:opinions:basic' =>
	{ select => [ 'OP_DATA' ] },
	{ set => '*', code => \&process_pbdb, if_vocab => 'pbdb' },
	{ set => '*', code => \&process_com, if_vocab => 'com' },
	{ set => '*', code => \&process_op },
	{ set => '*', code => \&process_taxon_ids },
	{ output => 'opinion_no', com_name => 'oid' },
	    "A unique identifier for this opinion record.",
	{ output => 'record_type', com_name => 'typ', value => $IDP{OPN}, com_value => 'opn' },
	    "The type of this record.",
	{ output => 'opinion_type', com_name => 'otp' },
	    "The type of opinion represented: B<C> for a",
	    "classification opinion, B<U> for an opinion which was not selected",
	    "as a classification opinion.",
	{ set => 'opinion_type', lookup => \%pbdb_opinion_code, if_vocab => 'pbdb' },
	{ output => 'taxon_rank', com_name => 'rnk' },
	    "The rank to which this opinion assigns the taxonomic name that is the subject of",
	    "this opinion.",
	{ output => 'taxon_name', com_name => 'nam' },
	    "The taxonomic name that is the subject of this opinion.",
	{ output => 'taxon_attr', com_name => 'att', if_block => 'attr' },
	    "The attribution (author and year) of the taxonomic name that is the subject",
	    "of this opinion.",
	{ output => 'orig_no', com_name => 'tid' },
	    "The identifier of the taxonomic name that is the subject of this opinion.",
	{ output => 'child_name', dedup => 'taxon_name', com_name => 'cnm' },
	    "The particular variant of the name that is the subject of this opinion,",
	    "if different from the currently accepted one.",
	{ output => 'child_spelling_no', com_name => 'vid', not_field => 'no_variant' },
	    "The identifier of the particular variant that is the subject of this opinion.",
	{ set => 'taxon_rank', if_vocab => 'pbdb,dwc', lookup => \%RANK_STRING },
	{ output => 'status', com_name => 'sta' },
	    "The taxonomic status of this name, as expressed by this opinion.",
	{ output => 'parent_name', com_name => 'prl' },
	    "The taxonomic name under which the subject is being placed (the \"parent\" taxonomic name).",
	    "Note that the value of this field is the particular variant of the name that was given",
	    "in the opinion, not necessarily the currently accepted variant.",
	{ output => 'parent_attr', com_name => 'atp', if_block => 'attr' },
	    "The attribution (author and year) of the parent taxonomic name.",
	{ output => 'parent_no', com_name => 'par' },
	    "The identifier of the parent taxonomic name.",
	{ output => 'parent_spelling_no', com_name => 'pva', dedup => 'parent_current_no' },
	    "The identifier of the variant of the parent name that was given in the opinion,",
	    "if this is different from the currently accepted variant of that name.",
	{ output => 'spelling_reason', com_name => 'spl' },
	    "An indication of why this name was given.",
	{ set => 'basis', from => 'ri', lookup => \%basis_code },
	{ output => 'basis', com_name => 'bas', if_block => 'basis,full' },
	    "The basis of the opinion, see above for a list.",
	{ output => 'author', com_name => 'oat' },
	    "The author of the opinion.",
	{ output => 'pubyr', com_name => 'opy' },
	    "The year in which the opinion was published.",
	{ output => 'ref_author', dwc_name => 'recordedBy', com_name => 'aut', if_block => '1.2:refs:attr' },
	    "The author(s) of the reference from which this opinion was entered.  Note that",
	    "the author of the opinion itself may be different if the reference is a secondary source.",
	{ output => 'ref_pubyr', com_name => 'pby', if_block => '1.2:refs:attr' },
	    "The year of publication of the reference from which this opinion was entered.  Note that",
	    "the year of the opinion itself may be different if the reference is a secondary source.",
	{ output => 'reference_no', com_name => 'rid' },
	    "The identifier of the reference from which this opinion was entered.");
    
    $ds->define_block('1.2:opinions:full_info' =>
	{ include => '1.2:refs:attr' });
    
    $ds->define_block('1.2:opinions:attr' =>
	{ select => [ 'OP_ATTR' ] });

    $ds->define_block('1.2:opinions:seq' =>
	{ select => [ 'SEQ' ] },
	{ output => 'lft', com_name => 'lsq' },
	    "This number gives the subject taxon's position in a preorder traversal",
	    "of the taxonomic tree.",
	{ output => 'rgt', com_name => 'rsq' },
	    "This number greater than or equal to the maximum of the sequence numbers of all of",
	    "this taxon's subtaxa, and less than the sequence of any succeeding taxon in the sequence.",
	    "You can use this, along with C<lft>, to determine subtaxon relationships.  If the pair",
	    "C<lft,rgt> for taxon <A> is bracketed by the pair C<lft,rgt> for taxon <B>, then C<A> is",
	    "a subtaxon of C<B>.");
    
    # Finally, we define some rulesets to specify the parameters accepted by
    # the operations defined in this class.
    
    $ds->define_ruleset('1.2:taxa:specifier' => 
	">>You must specify one of the following parameters for this operation:",
	{ param => 'taxon_name', valid => \&PB2::TaxonData::validNameSpec, 
	  alias => 'name' },
	    "Return information about the taxonomic name matching the specified string.",
	    "You may also use the alias B<C<name>> for this parameter.",
	    "If more than one name matches, the one with the largest number of occurrences",
	    "in the database will be returned. The characters C<.> C<%> C<_> may be used as wildcards.",
	    "If no taxonomic name matches the",
	    "value of this parameter, you will get an HTTP 404 (Not Found) response.",
	{ param => 'taxon_id', valid => VALID_IDENTIFIER('TID'), alias => 'id' },
	    "Return information about the taxonomic name corresponding to the specified identifier.",
	    "You may also use the alias B<C<id>> for this parameter. If the value is",
	    "syntactically correct but does not correspond to any taxonomic name in the database,",
	    "you will get an HTTP 404 (Not Found) response.",
	    "The value can have any of the following forms:", "=over",
	    "=item L<txn:285777|op:taxa/single.json?id=txn:285777>",
	    "Return the currently accepted variant of the taxonomic name identified by C<285777>.",
	    "This may be taxonomic name 285777, or it may be a different name that has been",
	    "grouped with it such as a recombination or corrected spelling.",
	    "=item L<var:285777|op:taxa/single.json?id=var:285777>",
	    "Return the exact name variant identified by C<285777>.",
	    "=item L<285777|op:taxa/single.json?id=285777>",
	    "Return the currently accepted variant. Purely numeric identifiers are",
	    "deprecated, and will not be accepted by the next version of the data service.",
	    "=item L<285777&exact|op:taxa/single.json?id=285777&exact>",
	    "If the paramter B<C<exact>> is also given, return the exact variant.",
	    "=back",
	{ at_most_one => ['taxon_name', 'name', 'taxon_id', 'id'] },
	    "You may not specify both B<C<taxon_name>> and B<C<taxon_id>> in the same query.",
	">>You may also specify any of the following parameters:",
	{ optional => 'exact', valid => FLAG_VALUE },
            "As indicated above, if you provide a purely numeric value for the B<C<taxon_id>> parameter,",
	    "you can also include this parameter to specify that the exact matching",
	    "taxonomic name variant should be returned. Identifiers of the form C<var:nnn>",
	    "or C<txn:nnn> are not affected by this parameter. This parameter is deprecated,",
	    "and will be removed in the next version of the data service.",
	{ optional => 'common', valid => ANY_VALUE, list => ',', bad_value => 'X' },
	    "This parameter indicates that name lookup should be done on common names instead of",
	    "(or in addition to) scientific names. The value should be one or more two-character",
	    "language codes as a comma-separated list, indicating which language(s) to match.",
	    "For example:", "=over", "=item", "L<op:taxa/single.json?taxon_name=whale&common=EN>");
    
    $ds->define_set('1.2:taxa:status' =>
	{ value => 'all' },
	    "Select all taxonomic names matching the other specified criteria.  This",
	    "is the default.",
	{ value => 'valid' },
	    "Select only taxonomically valid names",
	{ value => 'accepted' },
	    "Select only taxonomically valid names that are not junior synonyms",
	{ value => 'senior' },
	    "! an alias for 'accepted",
	{ value => 'junior' },
	    "Select only taxonomically valid names that are junior synonyms",
	{ value => 'invalid' },
	    "Select only taxonomically invalid names, e.g. nomina dubia");
    
    $ds->define_set('1.2:opinions:select' =>
	{ value => 'class' },
	    "Return just the classification opinions for the selected set of taxa.",
	    "If you wish to select only certain kinds of classification",
	    "opinions, e.g. those which classify the taxon as valid, or those which",
	    "classify it as a synonym, you can use the parameter B<C<taxon_status>> as well.",
	{ value => 'all' },
	    "Return all opinions about the selected set of taxa, regardless of",
	    "what they state about their subject taxon, and including all of the opinions that",
	    "were not selected as classification opinions.  This is the default.",
	{ value => 'valid' },
	    "Return only opinions which assert that their subject taxon is a valid",
	    "name.  These opinions state one of the following about their subject taxon:",
	    "C<B<belongs to>>, C<B<subjective synonym of>>, C<B<objective synonym of>>, C<B<replaced by>>.",
	{ value => 'accepted' },
	    "Return only opinions which assert that their subject taxon is a valid",
	    "name and not a junior synonym.  These opinions state C<B<belongs to>> about their subject taxon.",
	{ value => 'junior' },
	    "Select only opinions which assert that their subject taxon is a junior synonym.",
	    "These opinions state one of the following about their subject taxon:",
	    "C<B<subjective synonym of>>, C<B<objective synonym of>>, C<B<replaced by>>.",
	{ value => 'invalid' },
	    "Select only opinions which assert that their subject taxon is an invalid name.",
	    "These opinions state one of the following about their subject taxon:",
	    "C<B<nomen dubium>>, C<B<nomen nudum>>, C<B<nomen vanum>>, C<B<nomen oblitum>>, C<B<invalid subgroup of>>,",
	    "C<B<misspelling of>>.");
    
    $ds->define_set('1.2:taxa:preservation' =>
	{ value => 'regular' },
	    "Select regular taxa",
	{ value => 'form' },
	    "Select form taxa",
	{ value => 'ichno' },
	    "Select ichnotaxa",
	{ value => 'all' },
	    "Select all taxa");
    
    $ds->define_set('1.2:taxa:extant' =>
	{ value => 'yes' },
	    "Select extant taxa.",
	{ value => 'no' },
	    "Select extinct taxa.",
	{ value => 'any' },
	    "Select taxa regardless of extancy.  This is the default.",
	{ value => 'not_entered' },
	    "Select taxa whose extancy is not entered in the database",
	{ value => 'yes_fuzzy' },
	    "Select taxa which are not entered as extinct.",
	{ value => 'no_fuzzy' },
	    "Select taxa which are not entered as extant.");
    
    $ds->define_set('1.2:taxa:refselect' =>
	{ value => 'auth' },
	    "Select the references which provide the authority for the selected taxa.",
	{ value => 'var' },
	    "Select the references from which all name variants of the selected taxa were entered.",
	{ value => 'class' },
	    "Select the references which provide the classification opinions for the selected taxa",
	{ value => 'taxonomy' },
	    "Select both the authority references and the classification references for",
	    "the selected taxa.  This is the default.",
	{ value => 'ops' },
	    "Select the references providing all of the entered opinions about the selected taxa, including",
	    "those not used for classification.",
	{ value => 'occs' },
	    "Select the references from which the selected occurrences, or all occurrences",
	    "of the selected taxa, were entered.",
	{ value => 'specs' },
	    "Select the references from which the selected specimens, or all specimens associated",
	    "with the selected occurrences, or all specimens of the selected taxa, were entered.",
	{ value => 'colls' },
	    "Select the primary references of all collections containing the selected occurrences,",
	    "or occurrences of the selected taxa.",
	{ value => 'all' },
	    "Select all of the above.");
    
    # $ds->define_set('1.2:taxa:opselect' =>
    # 	{ value => 'class' },
    # 	    "Select only opinions that have been selected as classification opinions.  This is the default",
    # 	    "for L<taxa/opinions|node:taxa/opinions>.",
    # 	{ value => 'all' },
    # 	    "Select all matching opinions, including those that are not used",
    # 	    "as classification opinions.  This is the default for L<opinions/list|node:opinions/list>.");
    
    $ds->define_set('1.2:taxa:variants' =>
	{ value => 'current' },
	    "Select only the records associated with the currently accepted variant of each taxonomic name",
	{ value => 'all' },
	    "Select the records associated with all variants of each taxonomic name");
    
    $ds->define_set('1.2:taxa:resolution' =>
	{ value => 'species' },
	    "Select only occurrences which are identified to a species.",
	{ value => 'genus' },
	    "Select only occurrences which are identified to a genus or species.",
	{ value => 'family' },
	    "Select only occurrences which are identified to a family, genus or species.",
	{ value => 'lump_genus' },
	    "Select only occurrences identified as a genus or species, and also",
	    "coalesce all occurrences of the same genus in a given collection into",
	    "a single record.",
	{ value => 'lump_gensub' },
	    "Select only occurrences identified as a genus or species, and also",
	    "coalesce all occurrences of the same genus/subgenus in a given collection",
	    "into a single record.");
    
    $ds->define_set('1.2:taxa:ident_type' =>
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
	    "result in multiple records being returned for each occurrence.",
	{ value => 'all' },
	    "Select every identification that matches the other",
	    "query parameters.  This may result in multiple records being",
	    "returned for a given occurrence.");
    
    $ds->define_set('1.2:taxa:ident_qualification' =>
	{ value => 'any' },
	    "Select all occurrences regardless of modifiers.  This is the default.",
	{ value => 'certain' },
	    "Exclude all occurrences marked with any of the following modifiers:",
	    "C<aff.> / C<cf.> / C<?> / C<\"\"> / C<informal> / C<sensu lato>.",
	{ value => 'genus_certain' },
	    "Like C<B<certain>>, but look only at the genus/subgenus and ignore species modifiers.",
	{ value => 'uncertain' },
	    "Select only occurrences marked with one of the following modifiers:",
	    "C<aff.> / C<cf.> / C<?> / C<\"\"> / C<informal> / C<sensu lato>.",
	{ value => 'new' },
	    "Select only occurrences marked with one of the following:",
	    "C<n. gen.> / C<n. subgen.> / C<n. sp.>");
    
    # { value => 'latest' },
    #     "Select only references associated with most recently published identification of",
    #     "each matching occurrence.  This is the B<default> unless you specify otherwise."
    # { value => 'orig' },
    #     "Select only references associated with the original identification of each matching occurence.",
    # { value => 'all' },
    #     "Select the references associated with all identifications of each matching occurrence.",
    #     "Note that for any of these options, only identifications that match the taxonomic name",
    #     "parameters will be selected");
    
    # $ds->define_set('1.2:taxa:usetaxon' =>
    # 	{ value => 'ident' },
    # 	    "Use the taxonomic name with which each occurrence was actually identified",
    # 	{ value => 'accepted' },
    # 	    "Use the accepted taxonomic name corresponding to each identification",
    # 	{ value => 'higher' },
    # 	    "Use the accepted taxonomic name corresponding to each identification,",
    # 	    "along with all higher taxa up to the base taxon name.",
    # 	{ value => 'ident,higher' },
    # 	    "Use the taxonomic name with which each occurrence was actually identified,",
    # 	    "along with all higher taxa up to the base taxon name.",
    # 	{ value => 'accepted,higher' },
    # 	    "Same as C<higher>");
    
    $ds->define_set('1.2:taxa:order' =>
	{ value => 'hierarchy' },
	    "Results are ordered hierarchically by taxonomic identification.",
	    "The order of sibling taxa is arbitrary, but children will always follow",
	    "after parents.",
	{ value => 'hierarchy.asc', undocumented => 1 },
	{ value => 'hierarchy.desc', undocumented => 1 },
	{ value => 'ref' },
	    "Results are ordered by reference id, so that taxa entered from the same",
	    "reference are listed together.",
	{ value => 'ref.asc', undocumented => 1 },
	{ value => 'ref.desc', undocumented => 1 },
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
	{ value => 'pubyr' },
	    "Results are ordered by the year in which the name was first published, oldest first unless",
	    "you add C<.asc>",
	{ value => 'pubyr.asc', undocumented => 1 },
	{ value => 'pubyr.desc', undocumented => 1 },
	{ value => 'author' },
	    "Results are ordered alphabetically by the last name of the primary author",
	{ value => 'author.asc', undocumented => 1 },
	{ value => 'author.desc', undocumented => 1 },
	{ value => 'rank', undocumented => 1 },
	    "Results are ordered by the number of associated records, highest first unless you add C<.asc>.",
	    "This is only useful when querying for taxa associated with occurrences, etc.",
	{ value => 'rank.asc', undocumented => 1 },
	{ value => 'rank.desc', undocumented => 1 },
	{ value => 'size' },
	    "Results are ordered by the number of contained subtaxa, largest to smallest unless you add C<.asc>",
	{ value => 'size.asc', undocumented => 1 },
	{ value => 'size.desc', undocumented => 1 },
	{ value => 'extsize' },
	    "Results are ordered by the number of extant subtaxa, largest to smallest unless you add C<.asc>",
	{ value => 'extsize.asc', undocumented => 1 },
	{ value => 'extsize.desc', undocumented => 1 },
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
	{ value => 'modified.desc', undocumented => 1 });
    
    $ds->define_set('1.2:opinions:order' =>
	{ value => 'hierarchy' },
	    "Results are ordered hierarchically by taxonomic identification.",
	    "The order of sibling taxa is arbitrary, but children will always follow",
	    "after parents.  If no secondary ordering is specified, opinions are",
	    "further ordered such that classification opinions come first and the",
	    "remaining opinions on each taxon are ordered by year of publication in",
	    "descending order.  This is the default.",
	{ value => 'hierarchy.asc', undocumented => 1 },
	{ value => 'hierarchy.desc', undocumented => 1 },
	{ value => 'optype' },
	    "Classification opinions are ordered before non-classification opinions,",
	    "or the reverse if you append C<.desc>.",
	{ value => 'optype.asc', undocumented => 1 },
	{ value => 'optype.desc', undocumented => 1 },
	{ value => 'name' },
	    "Results are ordered alphabetically by taxon name.",
	{ value => 'name.asc', undocumented => 1 },
    	{ value => 'name.desc', undocumented => 1 },
	{ value => 'childname' },
	    "Results are ordered alphabetically by the name variant actually",
	    "referred to by the opinion, as opposed to the currently accepted",
	    "variant of the name.",
	{ value => 'childname.asc', undocumented => 1 },
	{ value => 'childname.desc', undocumented => 1 },
	{ value => 'ref' },
	    "Results are ordered by the primary and secondary authors of the associated reference,",
	    "so that opinions entered from the same reference are listed together.",
	{ value => 'ref.asc', undocumented => 1 },
	{ value => 'ref.desc', undocumented => 1 },
	{ value => 'pubyr' },
	    "Results are ordered by the year in which the opinion was published,",
	    "newest first unless you add '.asc'",
	{ value => 'pubyr.asc', undocumented => 1 },
	{ value => 'pubyr.desc', undocumented => 1 },
	{ value => 'author' },
	    "Results are ordered alphabetically by the last name of the primary and secondary authors.",
	{ value => 'author.asc', undocumented => 1 },
	{ value => 'author.desc', undocumented => 1 },
	{ value => 'basis' },
	    "Results are ordered according to the basis of the opinion, highest first.",
	{ value => 'basis.asc', undocumented => 1 },
	{ value => 'basis.desc', undocumented => 1 },
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
    
    $ds->define_set('1.2:taxa:rel' =>
	{ value => 'exact' },
	    "Select the matching taxonomic name(s) themselves.  This is the default if you use B<C<taxon_name>>,",
	    "B<C<taxon_id>>, or B<C<match_name>>.",
	{ value => 'current' },
	    "Select the currently accepted variant of each of the matching taxonomic name(s).",
	{ value => 'variants' },
	    "Select all variants of the matching taxonomic name(s) that are known to this",
	    "database.  These may be variant spellings, or previous ranks.  For example",
	    "a taxon currently ranked as a suborder might have been previously ranked",
	    "as an order, which would count as a different variant.",
	{ value => 'senior' },
	    "Select the senior name corresponding to each matching taxonomic name. If a matching",
	    "name is a junior synonym then the corresponding senior synonym will be returned.",
	    "Otherwise, the currently accepted variant of the matching name will be returned.",
	{ value => 'accepted' },
	    "Select the accepted name corresponding to each matching taxonomic name.",
	    "If a matching name is a junior synonym, its senior synonym will be returned.",
	    "If a matching name is invalid (i.e. a nomen dubium or nomen nudum) then the",
	    "corresponding valid name will be returned. Otherwise, the currently accepted",
	    "variant of the matching name will be returned.",
	{ value => 'synonyms' },
	    "Select all synonyms of the matching name(s) which are known to this database.",
	{ value => 'children' },
	    "Select all the taxa immediately contained within each matching taxon",
	    "and within all synonymous taxa.",
	{ value => 'all_children' },
	    "Select all taxa contained within each matching taxon and within all",
	    "synonymous taxa.  This selects an entire subtree of the taxonomic hierarchy.",
	    "It is the default if you use B<C<base_name>> or B<C<base_id>>.",
	{ value => 'parent' },
	    "Select the taxon immediately containing each matching taxon.  If an",
	    "immediate parent taxon is a junior synonym, the corresponding senior synonym",
	    "will be returned. You can also use the alias C<B<senpar>>.",
	{ value => 'immpar' },
	    "Select the taxon immediately containing each matching taxon,",
	    "even if it is a junior synonym.  This is equivalent to B<C<rel=parent&immediate>>.",
	{ value => 'senpar', undocumented => 1 },
	{ value => 'all_parents' },
	    "Select all taxa that contain any of the matching taxa.  The senior",
	    "synonym of each name will be returned, unless you also specify.",
	    "the parameter B<C<immediate>> in which case each actual containing",
	    "taxon will be returned even if it is a junior synonym.",
	{ value => 'common' },
	    "Select the most specific taxon that contains all of the matching taxa.",
	{ value => 'crown', undocumented => 1 },
	    "Select the taxon corresponding to the crown-group of the specified taxa",
	{ value => 'pan', undocumented => 1 },
	    "Select the taxon corresponding to the pan-group of the specified taxa",
	{ value => 'stem', undocumented => 1 },
	    "Select all of the highest-level taxa that make up the stem-group",
	    "of the specified taxa");

    $ds->define_set('1.2:taxa:aux_rel' =>
	{ value => 'exact' },
	    "! Not relevant for refs and opinions",
	{ value => 'current' },
	    "! Not relevant for refs and opinions",
	{ value => 'variants' },
	    "! Not relevant for refs and opinions",
	{ value => 'senior' },
	    "! Not relevant for refs and opinions",
	{ value => 'accepted' },
	    "! Not relevant for refs and opinions",
	{ value => 'synonyms' },
	    "Select all synonyms of the matching name(s) which are known to this database.",
	{ value => 'children' },
	    "Select all the taxa immediately contained within each matching taxon",
	    "and within all synonymous taxa.",
	{ value => 'all_children' },
	    "Select all taxa contained within each matching taxon and within all",
	    "synonymous taxa.  This selects an entire subtree of the taxonomic hierarchy.",
	    "It is the default if you use B<C<base_name>> or B<C<base_id>>.",
	{ value => 'parent' },
	    "! Not relevant for refs and opinions",
	{ value => 'immpar' },
	    "! Not relevant for refs and opinions",
	{ value => 'senpar', undocumented => 1 },
	{ value => 'all_parents' },
	    "Select all taxa that contain any of the matching taxa.  The senior",
	    "synonym of each name will be returned, unless you also specify.",
	    "the parameter B<C<immediate>> in which case each actual containing",
	    "taxon will be returned even if it is a junior synonym.",
	{ value => 'common' },
	    "! Not relevant for refs and opinions",
	{ value => 'crown', undocumented => 1 },
	{ value => 'pan', undocumented => 1 },
	{ value => 'stem', undocumented => 1 });
    
    $ds->define_ruleset('1.2:taxa:selector_1' =>
	{ param => 'taxon_name', valid => \&PB2::TaxonData::validNameSpec, 
	  alias => 'name' },
	    "For each of the specified names, select the taxon that most closely matches it.",
	    "You may specify more than one name, separated by commas.",
	    "The characters C<%> C<.> and C<_> may be used as wildcards, however only",
	    "one match will be returned for each name.  Whenever more than,",
	    "one taxon matches a given name, the one with the most occurrences",
	    "in the database will be selected.  You may instead use the alias",
	    "B<C<name>> for this parameter.  If you wish to select by common",
	    "name instead of scientific name, include the parameter B<C<common>>",
	    "as well.  See the documentation page on",
	    "L<specifying taxonomic names|node:general/taxon_names> for more information.",
	{ param => 'match_name', valid => \&PB2::TaxonData::validNameSpec },
	    "Selects all taxonomic names matching the given pattern.  The characters",
	    "C<%> C<.> and C<_> may be used as wildcards.  The first two will match any number of characters,",
	    "while the last will match any single character.  If you wish to",
	    "select by common name instead of scientific name, include the parameter",
	    "B<C<common>> as well.",
	{ param => 'base_name', valid => \&PB2::TaxonData::validNameSpec },
	    "Selects the valid taxonomic name most closely matching the specified name(s), plus",
	    "all synonyms and subtaxa.  You can specify more than one name, separated by",
	    "commas.  This is a shortcut, equivalent to specifying B<C<taxon_name>>",
	    "and B<C<rel=all_children>>.  If you wish to select by common name",
	    "instead of scientific name, include the paramter B<C<common>> as well.",
	{ param => 'taxon_id', valid => VALID_IDENTIFIER('TID'), list => ',', 
	  bad_value => '-1', alias => 'id' },
	    "Selects the taxonomic names represented by the specified identifier(s).",
	    "You may specify more than one, separated by commas.  You may",
	    "instead use the alias B<C<id>> for this parameter.",
	{ param => 'base_id', valid => VALID_IDENTIFIER('TID'), list => ',', bad_value => '-1' },
	    "Selects the valid taxonomic names most closely matching the specified identifier(s),",
	    "plus all synonyms and subtaxa.  You can specify more than one identifier, separated",
	    "by commas.  This is a shortcut, equivalent to specifying B<C<taxon_id>> and",
	    "B<C<rel=all_children>>.",
	{ param => 'all_taxa', valid => FLAG_VALUE },
	    "Selects the current variant of every taxonomic name from the database.",
	    "Be careful when using this, since",
	    "the full result set if you don't specify any other parameters can exceed",
	    "80 megabytes.  This parameter does not need any value.",
	{ param => 'all_records', valid => FLAG_VALUE },
	    "Selects all taxonomic names from the database, including all variants.",
	    "This is equivalent to specifying B<C<all_taxa&variant=all>>.",
	    "Be careful when using this, since",
	    "the full result set if you don't specify any other parameters can exceed",
	    "80 megabytes.  This parameter does not need any value.",
	{ param => 'extra_ref_id', alias => 'ref_id', undocumented => 1 },
	{ at_most_one => ['taxon_name', 'taxon_id', 'base_name', 'base_id', 
			  'match_name', 'all_taxa', 'all_records'] });
	
    $ds->define_ruleset('1.2:taxa:selector_2' =>
	">>The following parameters modify the selection of taxonomic names:",
	{ optional => 'exclude_id', valid => VALID_IDENTIFIER('TID'), list => ',' },
	    "Excludes the taxonomic subtree(s) corresponding to the taxon or taxa",
	    "represented by the specified identifier(s).  This is only relevant if you also specify",
	    "B<C<base_name>>, B<C<base_id>>, or B<C<rel=all_children>>.  If you are using B<C<base_name>>,",
	    "you can also exclude subtaxa by name using the C<^> symbol, as in C<B<dinosauria^aves>> or",
	    "C<B<osteichthyes^tetrapoda>>.",
	{ optional => 'immediate', valid => FLAG_VALUE },
	    "If you specify this parameter along with B<C<base_name>> or B<C<base_id>>,",
	    "then only children of the specified taxa are listed, and not synonyms or the",
	    "children of synonyms.  If you specify this parameter along with B<C<rel=parent>>",
	    "or B<C<rel=all_parents>>, then the taxa immediately containing the matching",
	    "taxa are returned even if they are junior synonyms.",
	{ optional => 'common', valid => ANY_VALUE, list => ',', bad_value => 'X' },
	    "This parameter indicates that name lookup should be done on common names instead of",
	    "(or in addition to) scientific names. The value should be one or more two-character",
	    "language codes as a comma-separated list, indicating which language(s) to match. You can also",
	    "include the value C<S>, to match scientific names as well as common names.",
	    "For example: L<op:taxa/list.json?taxon_name=whale&common=EN>",
	{ optional => 'taxon_status', valid => '1.2:taxa:status', alias => 'status' },
	    "Selects only names that have the specified status.  The default is C<all>.",
	    "Accepted values include:",
	">>You can select names that are B<related> to the ones you specify, by using the following parameter:",
	{ optional => 'rel', valid => '1.2:taxa:rel' },
	    "Indicates the relationship between the names that match your specification and the",
	    "names to be selected. Accepted values include:");
    
    $ds->define_ruleset('1.2:taxa:aux_selector_1' =>
	{ param => 'taxon_name', valid => \&PB2::TaxonData::validNameSpec, alias => 'name' },
	    "For each of the specified names, select records associated with the taxon that most",
	    "closely matches it. You may specify more than one name, separated by commas.",
	    "The characters C<%> C<.> and C<_> may be used as wildcards, however only",
	    "one match will be returned for each name.  Whenever more than,",
	    "one taxon matches a given name, the one with the most occurrences",
	    "in the database will be selected.  You may instead use the alias",
	    "B<C<name>> for this parameter.  If you wish to select by common name",
	    "instead of scientific name, use the parameter B<C<common>> as well.  See the documentation page on",
	    "L<specifying taxonomic names|node:general/taxon_names> for more information.",
	{ param => 'match_name', valid => \&PB2::TaxonData::validNameSpec },
	    "Selects records associated with all taxonomic names matching the given pattern.  The characters",
	    "C<%> C<.> and C<_> may be used as wildcards.  The first two will match any number of characters,",
	    "while the last will match any single character. If you wish to",
	    "select by common name instead of scientific name, use the parameter",
	    "B<C<common>> as well.",
	{ param => 'base_name', valid => \&PB2::TaxonData::validNameSpec },
	    "Selects records associated with the valid taxonomic name most closely matching",
	    "the specified name(s), plus",
	    "all synonyms and subtaxa.  You can specify more than one name, separated by",
	    "commas.  This is a shortcut, equivalent to specifying B<C<taxon_name>>",
	    "and B<C<rel=all_children>>.  If you wish to select by common name instead",
	    "of scientific name, use the paramter B<C<common>> as well.",
	{ param => 'taxon_id', valid => VALID_IDENTIFIER('TID'), list => ',', bad_value => '-1', alias => 'id' },
	    "Selects records associated with the taxonomic names represented by the specified identifier(s).",
	    "You may specify more than one, separated by commas.  You may",
	    "instead use the alias B<C<id>> for this parameter.",
	{ param => 'base_id', valid => VALID_IDENTIFIER('TID'), list => ',', bad_value => '-1' },
	    "Selects records associated with the valid taxonomic names most closely matching the",
	    "specified identifier(s),",
	    "plus all synonyms and subtaxa.  You can specify more than one identifier, separated",
	    "by commas.  This is a shortcut, equivalent to specifying B<C<taxon_id>> and",
	    "B<C<rel=all_children>>.",
	{ param => 'all_taxa', valid => FLAG_VALUE },
	    "Selects records associated with every taxonomic name from the database.",
	    "Be careful when using this, since",
	    "the full result set if you don't specify any other parameters can exceed",
	    "80 megabytes.  This parameter does not need any value.",
	{ param => 'all_records', valid => FLAG_VALUE },
	    "Selects records associated with every taxonomic name from the database,",
	    "including all name variants.  This is equivalent to specifying B<C<all_taxa&variant=all>>.",
	    "Be careful when using this, since",
	    "the full result set if you don't specify any other parameters can exceed",
	    "80 megabytes.  This parameter does not need any value.",
	{ param => 'extra_ref_id', alias => 'ref_id', undocumented => 1 },
	{ at_most_one => ['taxon_name', 'taxon_id', 'base_name', 'base_id', 
			  'match_name', 'all_taxa', 'all_records'] });
    
    $ds->define_ruleset('1.2:taxa:aux_selector_2' =>
	{ optional => 'exclude_id', valid => VALID_IDENTIFIER('TID'), list => ',' },
	    "Excludes the taxonomic subtree(s) corresponding to the taxon or taxa",
	    "represented by the specified identifier(s).  This is only relevant if you also specify",
	    "B<C<base_name>>, B<C<base_id>>, or B<C<rel=all_children>>.  If you are using B<C<base_name>>,",
	    "you can also exclude subtaxa by name using the C<^> symbol, as in C<B<dinosauria^aves>> or",
	    "C<B<osteichthyes^tetrapoda>>.",
	{ optional => 'immediate', valid => FLAG_VALUE },
	    "If you specify this parameter along with B<C<base_name>> or B<C<base_id>>,",
	    "then only records associated with children of the specified taxa are listed, and not synonyms or the",
	    "children of synonyms.  If you specify this parameter along with B<C<rel=parent>>",
	    "or B<C<rel=all_parents>>, then the taxa immediately containing the matching",
	    "taxa are selected even if they are junior synonyms.",
	{ optional => 'common', valid => ANY_VALUE, list => ',', bad_value => 'X' },
	    "This parameter indicates that name lookup should be done on common names instead of",
	    "(or in addition to) scientific names. The value should be one or more two-character",
	    "language codes as a comma-separated list, indicating which language(s) to match. You can also",
	    "include the value C<S>, to match scientific names as well.",
	    "For example: L<op:taxa/refs.json?taxon_name=whale&common=EN>",
	{ param => 'taxon_status', valid => '1.2:taxa:status', default => 'all' },
	    "Selects only records associated with names that have the specified status.  The default is C<B<all>>.",
	    "Accepted values include:",
	">>You can select records associated with names that are B<related> to the ones you specify,",
	"by using the following parameter:",
	{ param => 'rel', valid => '1.2:taxa:aux_rel' },
	    "Indicates the relationship between the names that match your specification and the",
	    "names whose associated records are to be selected.  This parameter accepts the same",
	    "set of values as the B<C<rel>> parameter of the L<taxa/list|node:taxa/list> operation,",
	    "But in general only the following values are useful:");
    
    $ds->define_ruleset('1.2:taxa:filter' => 
	{ optional => 'rank', valid => \&PB2::TaxonData::validRankSpec },
	    "Select only taxonomic names at the specified rank, e.g. C<B<genus>>.",
	{ optional => 'extant', valid => BOOLEAN_VALUE },
	    "Select only extant or non-extant taxa.",
	    "Accepted values are: C<B<yes>>, C<B<no>>.",
	{ optional => 'pres', valid => '1.2:taxa:preservation', split => qr{[\s,]+} },
	    "This parameter indicates whether to select",
	    "ichnotaxa, form taxa, or regular taxa.  The default is C<B<all>>, which will select",
	    "all taxa that meet the other specified criteria.  You can specify one or more",
	    "of the following values as a list:",
	{ optional => 'max_ma', valid => DECI_VALUE },
	    "Select only taxa which have at least one occurrence in the database no older than the given age in Ma.",
	    "B<Please note> that this does not necessarily correspond to the actual first appearance of",
	    "these taxa in the fossil record, just in that portion of the fossil record that has been",
	    "entered into the database.",
	    "By using the parameters C<max_ma> and C<min_ma> together, you can select only taxa",
	    "whose appearance in the database overlaps a particular age range.  Note that this",
	    "may include taxa without an actual occurrence in that age range.  If you wish",
	    "to select only taxa which actually have a recorded occurrence in a particular",
	    "time range, or if you wish to use a time resolution rule other than C<B<overlap>>,",
	    "use the L<occs/taxa|node:occs/taxa> operation instead.",
	{ optional => 'min_ma', valid => DECI_VALUE },
	    "Select only taxa which have at least one occurence in the database no younger than",
	    "the given age in Ma.  See B<C<max_ma>> above.",
	{ optional => 'interval', valid => ANY_VALUE },
	    "Select only taxa whose occurrences in the database overlap the",
	    "specified time interval or intervals, given by name.  You may give more than one",
	    "interval name, separated either by hyphens or commas.  The selected time range will",
	    "stretch from the beginning of the oldest specified interval to the end of the",
	    "youngest specified, with no gaps.  Note that this may include taxa without any",
	    "occurrences that actually fall within the specified interval or intervals.",
	{ optional => 'interval_id', valid => VALID_IDENTIFIER('INT'), split => ',' },
	    "Select only taxa whose occurrences in the database overlap the specified time interval",
	    "or intervals, given by identifier.  See the parameter B<C<interval>> above.",
	{ optional => 'depth', valid => POS_VALUE },
	    "Select only taxa no more than the specified number of levels below or",
	     "above the base taxon or taxa in the taxonomic hierarchy.  You can use this",
	     "parameter if you want to print out a portion of the taxonomic hierarchy centered",
	     "upon a higher taxon wihtout printing out its entire subtree.");
    
    $ds->define_ruleset('1.2:taxa:occ_list_filter' =>
	{ param => 'taxon_status', valid => '1.2:taxa:status', default => 'all' },
	    "Select only occurrences identified to taxa that have the specified status.",
	    "The default is C<B<all>>.  Accepted values include:",
	# { param => 'ident_select', valid => '1.2:taxa:ident_select', default => 'latest', alias => 'ident' },
	#     "If more than one taxonomic identification is recorded for some or all of the selected occurrences,",
	#     "this parameter specifies which are to be returned.  Values include:",
	{ param => 'pres', valid => '1.2:taxa:preservation', list => ',' },
	    "This parameter indicates whether to select occurrences that are identified as",
	    "ichnotaxa, form taxa, or regular taxa.  The default is C<B<all>>, which will select",
	    "all records that meet the other specified criteria.  You can specify one or more",
	    "of the following values as a list:",
	{ optional => 'extant', valid => BOOLEAN_VALUE },
	    "Select only occurrences identified to extant or to non-extant taxa.",
	    "Accepted values are: C<B<yes>>, C<B<no>>.");
    
    $ds->define_ruleset('1.2:taxa:occ_aux_filter' =>
	{ param => 'taxon_status', valid => '1.2:taxa:status', default => 'all' },
	    "Selects only records associated with taxa that have the specified status.",
	    "The default is C<B<all>>.  Accepted values include:",
	{ param => 'pres', valid => '1.2:taxa:preservation', list => ',' },
	    "This parameter indicates whether to select records associated with taxa that are identified as",
	    "ichnotaxa, form taxa, or regular taxa.  The default is C<B<all>>, which will select",
	    "all records that meet the other specified criteria.  You can specify one or more",
	    "of the following values as a list:",
	{ optional => 'rank', valid => \&PB2::TaxonData::validRankSpec },
	    "Return only records associated with taxonomic names at the specified rank, e.g. C<B<genus>>.",
	{ optional => 'depth', valid => POS_VALUE },
	    "Return only taxa no more than the specified number of levels below",
	     "the base taxon or taxa in the hierarchy.",
	{ optional => 'extant', valid => BOOLEAN_VALUE },
	    "Selects only records associated with extant or with non-extant taxa.",
	    "Accepted values are: C<B<yes>>, C<B<no>>.");
    
    # $ds->define_ruleset('1.2:taxa:summary_selector' => 
    # 	{ optional => 'rank', valid => '1.2:taxa:summary_rank', alias => 'summary_rank',
    # 	  default => 'ident' },
    # 	    "Summarize the results by grouping them as follows:");
    
    $ds->define_ruleset('1.2:taxa:show' =>
	{ optional => 'SPECIAL(show)', valid => '1.2:taxa:mult_output_map', list => ','},
	    "This parameter is used to select additional blocks of information to be returned",
	    "along with the basic record for each taxon.  Its value should be",
	    "one or more of the following, separated by commas:");
    
    $ds->define_ruleset('1.2:taxa:order' => 
	{ optional => 'order', valid => '1.2:taxa:order', split => ',' },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  Accepted values are:");
    
    $ds->define_ruleset('1.2:opinions:filter' =>
	# { optional => 'op_status', valid => '1.2:opinions:status' },
	#     "You can use this parameter to specify which kinds of opinions to select.",
	#     "Accepted values include:",
	{ param => 'op_author', valid => ANY_VALUE },
	    "Selects only opinions attributed to the specified author.  Note that the opinion",
	    "author(s) may be different from the author(s) of the reference from which the opinion",
	    "was entered.  This parameter accepts last names only, no first initials.  You can",
	    "specify more than one author name separated by commas, in which case all opinions",
	    "which match any of these will be selected.",
	{ param => 'op_pubyr', valid => ANY_VALUE },
	    "Selects only opinions published during the indicated year or range of years.",
	    "Note that the opinion publication year may be different from the publication",
	    "year of the reference from which the opinion was entered.");
    
    $ds->define_ruleset('1.2:opinions:display' => 
	{ optional => 'SPECIAL(show)', valid => '1.2:opinions:output_map', list => ','},
	    "This parameter is used to select additional information to be returned",
	    "along with the basic record for each taxon.  Its value should be",
	    "one or more of the following, separated by commas:",
	{ optional => 'order', valid => '1.2:opinions:order', split => ',' },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  If this",
	    "parameter is not specified, the order defaults to C<B<author>>. Accepted values are:");
    
    $ds->define_ruleset('1.2:taxa:single' => 
	{ require => '1.2:taxa:specifier',
	  error => "you must specify either 'name' or 'id'" },
	">>The following parameter indicates which information should be returned about the selected name:",
	{ optional => 'SPECIAL(show)', valid => '1.2:taxa:single_output_map', list => ','},
	    "This parameter is used to select additional blocks of information to be returned",
	    "along with the basic record for each taxon.  Its value should be",
	    "one or more of the following, separated by commas:",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.2:taxa:list' => 
	"The following parameters are used to select the base set of taxonomic names to return.",
	"Any query using this operation must include exactly one of the parameters from the following list.",
	"If you wish to download the entire taxonomy, use B<C<all_taxa>> or B<C<all_records>> and see also the",
	"L<limit|node:special#limit> parameter.",
	"If you want to select taxa that are mentioned in particular bibliographic references,",
	"use the L<taxa/byref|node:taxa/byref> operation instead.",
	{ require => '1.2:taxa:selector_1',
	  error => "you must specify one of 'name', 'id', 'base_name', 'base_id', 'match_name', 'all_taxa', or 'all_records'" },
	{ allow => '1.2:taxa:selector_2' },
	{ optional => 'variant', valid => '1.2:taxa:variants', no_set_doc => 1 },
	    "If you specify B<C<variant=all>>, then all variants of the selected names will be returned.",
	    "This may include variant spellings, previous combinations, and previous ranks.",
	    "You may use this in combination with any of the other parameters and relationship values.",
	    "If not specified, then whichever variants are selected by the other parameters will be",
	    "returned.",
	">>The following parameters further filter the list of selected records:",
	{ allow => '1.2:taxa:filter' },
	{ allow => '1.2:common:select_taxa_crmod' },
	{ allow => '1.2:common:select_taxa_ent' },
	">>The following parameters indicate which information should be returned about each resulting name,",
	"and the order in which you wish the records to be returned.",
	{ allow => '1.2:taxa:show' }, 
	{ optional => 'order', valid => '1.2:taxa:order', split => ',' },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  The",
	    "default is C<B<hierarchy>>. Accepted values are:",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.2:taxa:refs' =>
	">>You can use the following parameters to retrieve the references associated",
	"with a specified list of taxa:",
	{ require => '1.2:taxa:aux_selector_1',
	  error => "you must specify one of 'name', 'id', 'base_name', 'base_id', 'match_name', 'ref_id', 'all_taxa', or 'all_records'" },
	">>The following B<very important parameter> allows you to select references that",
	"have particular relationships to the taxa they mention, and skip others:",
	{ optional => 'ref_type', valid => '1.2:taxa:refselect', alias => 'select', list => ',',
	  bad_value => '_' },
	    "You can use this parameter to specify which kinds of references to retrieve.",
	    "The default is C<B<taxonomy>>, which selects only those references which provide the",
	    "authority and classification opinions for the selected taxa.",
	    "The value of this attribute can be one or more of the following, separated by commas:",
	">>The following parameters modify the selection of taxonomic names:",
	{ allow => '1.2:taxa:aux_selector_2' },
	{ optional => 'variant', valid => '1.2:taxa:variants' },
	    "This parameter is relevant only when retrieving authority references.",
	    "It specifies whether to retrieve the reference for just the current variant",
	    "of each matching taxonomic name (the default) or for all variants.  The",
	    "accepted values include:",
	">>If you are retrieving occurrence and/or collection references, the following",
	"parameters are also relevant.  The occurrence/collection references returned will be those associated",
	"with the selected identifications of the selected occurrences.",
	{ optional => 'idtype', valid => '1.2:taxa:ident_type', alias => 'ident' },
	    "This parameter specifies how re-identified occurrences should be treated.",
	    "The default is C<B<latest>> unless you specify otherwise.  I<Note that any",
	    "identifications not falling into the set of taxonomic names selected by this query",
	    "will be ignored.>  So if an earlier identification of a particular occurrence",
	    "falls into the set of taxonomic names selected for this query but the latest",
	    "one does not, then by default that occurrence's reference will not be part of the result.",
	    "Allowed values include:",
	{ optional => 'idqual', valid => "1.2:taxa:ident_qualification" },
	    "This parameter selects or excludes identifications based on their taxonomic modifiers.",
	    "Allowed values include:",
	">>You can use the following parameters in addition to (or instead of) the ones above,",
	"to select references with particular authors, published within a specified range of years, etc.:",
	{ allow => '1.2:refs:aux_selector' },
	">>The following parameters further filter the list of selected records:",
	{ allow => '1.2:taxa:filter' },
	{ allow => '1.2:common:select_taxa_crmod' },
	{ allow => '1.2:common:select_taxa_ent' },
	{ allow => '1.2:common:select_refs_crmod' },
	{ allow => '1.2:common:select_refs_ent' },
	">>You can use the following parameters specify what information should be returned about each",
	"resulting reference, and the order in which the results should be returned:",
	{ allow => '1.2:refs:display' },
	{ optional => 'order', valid => '1.2:refs:order', split => ',', no_set_doc => 1 },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  Accepted values are:",
	    $ds->document_set('1.2:refs:order'),
	    ">If no order is specified, the results are sorted alphabetically according to",
	    "the name of the primary author, unless B<C<all_taxa>> or B<C<all_records>> is specified in which",
	    "case they are returned by default in the order they occur in the database.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.2:taxa:byref' =>
	">>You can use the following parameters to retrieve a specified list or range of taxa.",
	"Any query using this operation must include exactly one parameter from this",
	"list and/or at least one parameter from the list immediately below it.",
	{ require => '1.2:taxa:selector_1',
	  error => "you must specify one of 'name', 'id', 'base_name', 'base_id', 'match_name', 'ref_id', 'all_taxa', or 'all_records'" },
	">>You can use the following parameters in addition to (or instead of) the ones above,",
	"to select taxa mentioned in particular bibliographic references:",
	{ allow => '1.2:refs:aux_selector' },
	">>The following B<very important parameter> indicates which relationships between references",
	"and taxa should be selected, and which should be skipped:",
	{ optional => 'ref_type', valid => '1.2:taxa:refselect', list => ',', alias => 'select',
	    bad_value => '_' },
	    "You can use this parameter to filter the set of returned records according to the",
	    "relationship between taxon and reference. So, for example, if you are just interested in the",
	    "taxonomic classification of the selected names you can let this parameter default",
	    "to C<B<taxonomy>>. On the other hand, if you are interested in the sources for",
	    "occurrences of these taxa, you can specify C<B<occs>>.",
	{ allow => '1.2:taxa:selector_2' },
	{ optional => 'variant', valid => '1.2:taxa:variants', no_set_doc => 1 },
	    "This parameter specifies whether to return all variants of the selected taxonomic",
	    "name(s) or only the currently accepted one.", "=over",
	    "=item all", 
	    "Show all variants of the selected names. This is the default if you specify one",
	    "of the reference selection parameters above, so that you will get whichever name variants",
	    "the selected reference(s) happened to use.",
	    "=item current",
	    "Show only the currently accepted variant of each  name. This is the default if you do not specify",
	    "a reference selection parameter, but you can override it by explicitly including",
	    "B<C<variant=all>>.",
	">>If you are retrieving occurrence and/or collection references, the following",
	"parameters are also relevant.  The occurrence/collection references returned will be those associated",
	"with the selected identifications of the selected occurrences.",
	{ optional => 'idtype', valid => '1.2:taxa:ident_type', alias => 'ident' },
	    "This parameter specifies how re-identified occurrences should be treated.",
	    "The default is C<B<latest>> unless you specify otherwise.  I<Note that any",
	    "identifications not falling into the set of taxonomic names selected by this query",
	    "will be ignored.>  So if an earlier identification of a particular occurrence",
	    "falls into the set of taxonomic names selected for this query but the latest",
	    "one does not, then by default that occurrence's reference will not be part of the result.",
	    "Allowed values include:",
	{ optional => 'idqual', valid => "1.2:taxa:ident_qualification" },
	    "This parameter selects or excludes identifications based on their taxonomic modifiers.",
	    "Allowed values include:",
	">>The following parameters further filter the list of selected records:",
	{ allow => '1.2:taxa:filter' },
	{ allow => '1.2:common:select_taxa_crmod' },
	{ allow => '1.2:common:select_taxa_ent' },
	{ allow => '1.2:common:select_refs_crmod' },
	{ allow => '1.2:common:select_refs_ent' },
	">>You can use the following parameters specify what information should be returned about each resulting name,",
	"and the order in which the results should be returned:",
	{ allow => '1.2:taxa:show' },
	{ optional => 'order', valid => '1.2:taxa:order', split => ',' },
	    "Specifies the order in which the results are returned.  You can specify multiple values",
	    "separated by commas, and each value may be appended with C<.asc> or C<.desc>.  The",
	    "default is C<B<ref, hierarchy>>. Accepted values are:",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.",
	">If the parameter B<C<order>> is not specified, the results are sorted alphabetically by",
	"the name of the primary author.");
    
    $ds->define_ruleset('1.2:taxa:opinions' =>
	">>You can use the following parameters if you wish to select opinions associated",
	"with a specified list or range of taxa.  You may specify at most one parameter",
	"from this section, and must specify at least one from either this section or the next.",
	{ require => '1.2:taxa:aux_selector_1',
	  error => "you must specify one of 'name', 'id', 'base_name', 'base_id', 'match_name', 'ref_id', 'all_taxa', or 'all_records'" },
	">>The following B<very important parameter> allows you to select certain types",
	"of opinions and skip others:",
	{ optional => 'op_type', valid => '1.2:opinions:select', alias => 'select' },
	    "You can use this parameter to retrieve all opinions, or only the classification opinions,",
	    "or only certain kinds of opinions.  The default is C<B<all>>, which selects all",
	    "opinions associated with the selected taxa.  To retrieve just those opinions",
	    "that have been algorithmically chosen as the classification opinions for",
	    "their subject taxa, specify B<C<op_type=class>>.  Accepted values for this parameter include:",
	">>You can use the following parameters in addition to (or instead of) the ones above,",
	"to select opinions entered from particular bibliographic references:",
	{ allow => '1.2:refs:aux_selector' },
	">>The following parameters modify the selection of taxonomic names:",
	{ allow => '1.2:taxa:aux_selector_2' },
	{ optional => 'variant', valid => ANY_VALUE },
	    "! This parameter is not useful for opinions, but is included for consistency",
	    "with 1.2:taxa:list and 1.2:taxa:refs.",
	">>The following parameters further filter the list of selected records:",
	{ allow => '1.2:opinions:filter' },
	{ allow => '1.2:taxa:filter' },
	{ allow => '1.2:common:select_taxa_crmod' },
	{ allow => '1.2:common:select_taxa_ent' },
	{ allow => '1.2:common:select_ops_crmod' },
	{ allow => '1.2:common:select_ops_ent' },
	">>You can use the following parameters specify what information should be returned about each",
	"resulting opinion, and the order in which the results should be returned:",
	{ allow => '1.2:opinions:display' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.2:opinions:specifier' =>
	{ param => 'id', valid => VALID_IDENTIFIER('OPN'), alias => 'opinion_id' },
	    "Return information about the taxonomic opinion corresponding to this identifier.");
    
    $ds->define_ruleset('1.2:opinions:selector' =>
	{ param => 'id', valid => VALID_IDENTIFIER('OPN'), list => ',', bad_value => '-1',
	  alias => 'opinion_id' },
	    "Selects the opinions corresponding to the specified identifier(s).",
	    "You may provide more than one, separated by commas.",
	{ param => 'all_records', valid => FLAG_VALUE },
	    "Selects all opinions entered in the database.  This parameter does not",
	    "need a value.  Be careful when using this parameter, since the result",
	    "may total more than 50 megabytes.");
    
    # $ds->define_ruleset('1.2:opinions:aux_selector' =>
    # 	{ param => 'opinion_id', valid => VALID_IDENTIFIER('OPN'), list => ',', bad_value => '-1' },
    # 	    "Selects the opinions corresponding to the specified identifier(s).",
    # 	    "You may provide more than one, separated by commas.");
    
    # $ds->define_ruleset('1.2:taxa:match' =>
    # 	{ param => 'name', valid => \&PB2::TaxonData::validNameSpec, list => ',', alias => 'taxon_name' },
    # 	    "A valid taxonomic name, or a common abbreviation such as 'T. rex'.",
    # 	    "The name may include the wildcard characters % and _.",
    # 	{ optional => 'rank', valid => \&PB2::TaxonData::validRankSpec },
    # 	    "Return only taxonomic names at the specified rank, e.g. <genus>.",
    # 	{ optional => 'extant', valid => BOOLEAN_VALUE },
    # 	    "Return only extant or non-extant taxa.",
    # 	    "Accepted values are: C<B<yes>>, C<B<no>>, C<B<1>>, C<B<0>>, C<B<true>>, C<B<false>>.",
    # 	{ param => 'status', valid => '1.2:taxa:status', default => 'all' },
    # 	    "Return only names that have the specified status.  Accepted values include:",
    # 	{ allow => '1.2:taxa:display' }, 
    # 	{ allow => '1.2:special_params' },
    # 	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.2:taxa:auto' =>
	{ param => 'name', valid => ANY_VALUE, alias => 'taxon_name' },
	    "A partial name or prefix.  It must have at least 3 significant characters, and may include both a genus",
	    "(possibly abbreviated) and a species.  Examples:\n    t. rex, tyra, rex", 
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.2:taxa:thumb' =>
	{ param => 'id', valid => VALID_IDENTIFIER('PHP') },
	    "Return the image corresponding to the specified image identifier,",
	    "or information about the image.",
	{ param => 'taxon_id', valid => VALID_IDENTIFIER('TID') },
	    "Return the image corresponding to the specified taxon, or information",
	    "about the image.",
	{ param => 'taxon_name', valid => \&PB2::TaxonData::validNameSpec, alias => 'name' },
	    "Return the image corresponding to the specified taxonomic name, or information",
	    "about the image.  If more",
	    "than one name matches the parameter value, the one with the largest number",
	    "of occurrences in the database will be used.  You can also use the parameter",
	    "alias B<C<name>>.",
	{ at_most_one => ['id', 'taxon_id', 'taxon_name'] },
	    "You may not specify both B<C<taxon_name>> and B<C<taxon_id>> in the same query.",
	{ optional => 'SPECIAL(save)' },
	{ ignore => 'splat' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.2:taxa:icon' =>
	{ require => '1.2:taxa:thumb' });
    
    $ds->define_ruleset('1.2:taxa:list_images' =>
	{ param => 'name', valid => ANY_VALUE },
	    "List images belonging to the specified taxonomic name.  If multiple",
	    "names match what you specified, the images for all of them will be listed.",
	{ param => 'id', valid => VALID_IDENTIFIER('TID') },
	    "List images belonging to the taxonomic name corresponding to the specified",
	    "identifier.",
	{ at_most_one => ['id', 'name'] },
	    "You may not specify both 'name' and 'id' in the same query.",
	{ optional => 'rel', valid => ENUM_VALUE('all_children') },
	    "If this parameter is specified with the value C<B<all_children>>, then",
	    "all images matching the specified taxon or any of its children are",
	    "returned.  In this case, the fields C<taxon_id> and C<taxon_name>",
	    "will be included in the result.",
	{ optional => 'depth', valid => POS_VALUE },
	    "Return only images whose depth in the tree is at most the specified",
	    "number of levels different from the base taxon or taxa.");
    
    $ds->define_ruleset('1.2:opinions:single' =>
	"The following parameter selects a record to retrieve:",
    	{ require => '1.2:opinions:specifier', 
	  error => "you must specify an opinion identifier, either in the URL or with the 'id' parameter" },
	">>You may also use the following parameter to specify what information you wish to retrieve:",
    	{ optional => 'SPECIAL(show)', valid => '1.2:opinions:output_map' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:opinions:list' =>
	{ allow => '1.2:opinions:selector' },
	{ allow => '1.2:opinions:filter' },
	{ allow => '1.2:common:select_ops_crmod' },
	{ allow => '1.2:common:select_ops_ent' },
	{ require_any => ['1.2:opinions:selector', '1.2:opinions:filter', 
			  '1.2:common:select_ops_crmod', '1.2:common:select_ops_ent'] },
	{ optional => 'op_type', valid => '1.2:opinions:select', alias => 'select', default => 'all' },
	    "You can use this parameter to retrieve all opinions, or only the classification opinions,",
	    "or only certain kinds of opinions.  The default is all opinions.  Accepted values include:",
	{ allow => '1.2:opinions:display' }, 
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.",
	"If the parameter B<C<order>> is not specified, the results are ordered by year of",
	"publication and the last name of the author.");
    
    # Determine which fields are available in this version of the database.
    
    my $dbh = $ds->get_connection;
    
    my $record;
    
    eval {
	$record = $dbh->selectrow_hashref("SELECT * from $TAXON_TABLE{taxon_trees}{search} LIMIT 1");
    };
    
    if ( ref $record eq 'HASH' )
    {
	$DB_FIELD{common} = 1 if exists $record->{common};
	$DB_FIELD{orig_no} = 1 if exists $record->{orig_no};
	$DB_FIELD{is_current} = 1 if exists $record->{is_current};
	$DB_FIELD{accepted_no} = 1 if exists $record->{accepted_no};
    }
}


# get_taxon ( )
# 
# Return a single taxon record, specified by name or identifier.  If name, then
# return the matching taxon with the largest number of occurrences.

sub get_taxon {

    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    my ($taxon_no);
    
    $request->delete_output_field('container_no');
    
    # First determine the fields necessary to show the requested info.
    
    my $options = $request->generate_query_options('taxa');
    
    # Then figure out which taxon we are looking for.  If we were given a taxon_no,
    # we can use that.
    
    my $not_found_msg = '';
    
    if ( $taxon_no = $request->clean_param('taxon_id') )
    {
	die $request->exception(400, "Invalid taxon id '$taxon_no'")
	    unless $taxon_no > 0 || $taxon_no =~ qr{ ^ [UN][A-Z] \d* $ }xs;
	
	if ( ! ref $taxon_no || $taxon_no->{type} eq 'unk' )
	{
	    $options->{exact} = 1 if $request->clean_param('exact');
	}
	
	elsif ( $taxon_no->{type} eq 'var' )
	{
	    $options->{exact} = 1;
	}
    }
    
    # Otherwise, we must have a taxon name.  So look for that.
    
    elsif ( my $taxon_name = $request->clean_param('taxon_name') )
    {
	# Return an immediate error if more than one name was specified.
	
	die $request->exception(400, "The value of 'taxon_name' must be a single taxon name")
	    if $taxon_name =~ qr{,};
	
	# Return an immediate error if an exclusion was specified.
	
	if ( $taxon_name =~ qr{\^} )
	{
	    $request->add_warning("Exclusions are not allowed with this operation");
	    die $request->exception(400, "Invalid taxon name '$taxon_name'");
	}
	
	# Create a hash of options for name resolution.
	
	my $name_select = { return => 'id', exact => 1 };
	
	# Check for the parameter 'common'
	
	if ( my @lang = $request->clean_param_list('common') )
	{
	    my @common;
	    
	    foreach my $l ( @lang )
	    {
		$l = uc $l;
		
		if ( $LANGUAGE{$l} )
		{
		    push @common, $l;
		}
		elsif ( $l ne 'X' )
		{
		    $request->add_warning("Unknown language code '$l'");
		}
	    }
	    
	    die $request->exception(400, "You must specify at least one valid language code")
		unless @common;
	    
	    $name_select->{common} = \@common;
	}
	
	if ( my $rank = $request->clean_param('rank') )
	{
	    $name_select->{rank} = $rank;
	    $not_found_msg .= " at specified rank";
	}

	# Check for debug mode

	if ( $request->debug )
	{
	    $name_select->{debug_out} = sub {
		$request->{ds}->debug_line($_[0]);
	    };
	}

	# Now look up the name to find the corresponding taxon_no.
	
	($taxon_no) = $taxonomy->resolve_names($taxon_name, $name_select);
	
	my @warnings = $taxonomy->list_warnings;
	$request->add_warning(@warnings) if @warnings;
	
	die $request->exception(400, "Invalid taxon name '$taxon_name'")
	    if $taxonomy->has_warning('W_BAD_NAME');
	
	die $request->exception(404, "Taxon '$taxon_name' was not found in the database")
	    unless defined $taxon_no && $taxon_no > 0;
	
	$options->{exact} = 1;
    }
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings.
    
    $request->strict_check;
    $request->extid_check;
    
    # If this is an 'unknown taxon', return a synthesized record.

    if ( $taxon_no && $taxon_no =~ qr{ ^ ( [UN][A-Z] \d* ) }xs )
    {
	$request->single_result( generate_unknown_taxon($1) );
	return;
    }
    
    # Otherwise, attempt to fetch the record for the specified taxon.
    
    my ($r);
    
    try {
	($r) = $taxonomy->list_taxa_simple($taxon_no, $options);
    }
    
    catch { die $_ }
    
    finally { print STDERR $taxonomy->last_sql . "\n\n" if $request->debug };
    
    # Return a 404 error if we did not find anything.
    
    unless ( $r )
    {
	die $request->exception(404, "Unknown taxon id '$taxon_no'")
	    unless ref $r;
    }
    
    # Otherwise, register this result and add any requested auxiliary info.
    # If there is a 'B' flag, remove it since this makes no sense for a single result.
    
    if ( $r->{flags} )
    {
	$r->{flags} =~ s/B//;
    }
    
    $request->single_result($r);
    $request->{main_sql} = $taxonomy->last_sql;
    
    return unless ref $r;
    
    # If we were asked for 'nav' info, add the necessary fields.
    
    if ( $request->has_block('nav') )
    {
	# First get taxon records for all of the relevant supertaxa.
	
	if ( $r->{kingdom_no} )
	{
	    ($r->{kingdom_txn}) = $taxonomy->list_taxa_simple($r->{kingdom_no}, { fields => ['SIMPLE','SIZE'] });
	}
	
	if ( $r->{phylum_no} )
	{
	    ($r->{phylum_txn}) = $taxonomy->list_taxa_simple($r->{phylum_no}, { fields => ['SIMPLE','SIZE'] });
	}
	
	if ( $r->{class_no} )
	{
	    ($r->{class_txn}) = $taxonomy->list_taxa_simple($r->{class_no}, { fields => ['SIMPLE','SIZE'] });
	}
	
	if ( $r->{order_no} )
	{
	    ($r->{order_txn}) = $taxonomy->list_taxa_simple($r->{order_no}, { fields => ['SIMPLE','SIZE'] });
	}
	
	if ( $r->{family_no} )
	{
	    ($r->{family_txn}) = $taxonomy->list_taxa_simple($r->{family_no}, { fields => ['SIMPLE','SIZE'] });
	}
	
	if ( $r->{immpar_no} || $r->{senpar_no} )
	{
	    my $parent_no = $r->{immpar_no} || $r->{senpar_no};
	    ($r->{parent_txn}) = $taxonomy->list_taxa_simple($parent_no, { fields => ['SIMPLE','SIZE'] });
	}
	
	# Then add the various lists of subtaxa.
	
	my $data = ['SIMPLE','SIZE','APP'];
	
	unless ( $r->{phylum_no} or (defined $r->{taxon_rank} && $r->{taxon_rank} <= 20) )
	{
	    $r->{phylum_list} = [ $taxonomy->list_taxa($taxon_no, 'all_children',
						     { limit => 10, order => 'size.desc', rank => 20, fields => $data } ) ];
	}
	
	unless ( $r->{class_no} or $r->{taxon_rank} <= 17 )
	{
	    $r->{class_list} = [ $taxonomy->list_taxa('all_children', $taxon_no, 
						    { limit => 10, order => 'size.desc', rank => 17, fields => $data } ) ];
	}
	
	unless ( $r->{order_no} or $r->{taxon_rank} <= 13 )
	{
	    my $order = defined $r->{order_count} && $r->{order_count} > 100 ? undef : 'size.desc';
	    $r->{order_list} = [ $taxonomy->list_taxa('all_children', $taxon_no, 
						    { limit => 10, order => $order, rank => 13, fields => $data } ) ];
	}
	
	unless ( $r->{family_no} or $r->{taxon_rank} <= 9 )
	{
	    my $order = defined $r->{family_count} && $r->{family_count} > 100 ? undef : 'size.desc';
	    $r->{family_list} = [ $taxonomy->list_taxa('all_children', $taxon_no, 
						     { limit => 10, order => $order, rank => 9, fields => $data } ) ];
	}
	
	if ( $r->{taxon_rank} > 5 )
	{
	    my $order = defined $r->{genus_count} && $r->{order_count}> 100 ? undef : 'size.desc';
	    $r->{genus_list} = [ $taxonomy->list_taxa('all_children', $taxon_no,
						    { limit => 10, order => $order, rank => 5, fields => $data } ) ];
	}
	
	if ( $r->{taxon_rank} == 5 )
	{
	    $r->{subgenus_list} = [ $taxonomy->list_taxa('all_children', $taxon_no,
						       { limit => 10, order => 'size.desc', rank => 4, fields => $data } ) ];
	}
	
	if ( $r->{taxon_rank} == 5 or $r->{taxon_rank} == 4 )
	{
	    $r->{species_list} = [ $taxonomy->list_taxa('all_children', $taxon_no,
						       { limit => 10, order => 'size.desc', rank => 3, fields => $data } ) ];
	}
	
	$r->{children} = 
	    [ $taxonomy->list_taxa('children', $taxon_no, { limit => 10, order => 'size.desc', fields => $data } ) ];
    }
    
    return 1;
}


# get_opinion ( )
# 
# Retrieve a single opinion record from the database, selected by identifier.

sub get_opinion {
    
    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    
    # First figure out which opinion we are looking for.  If none was
    # specified (this shouldn't happen), then we return a 400 error.
    
    my $opinion_no = $request->clean_param('id') or
	die $request->exception(400, "No opinion id was specified");
    
    my $options = $request->generate_query_options('opinions');
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings.
    
    $request->strict_check;
    $request->extid_check;
    
    # Next fetch the requested info about the opinion.
    
    my ($r);
    
    try {
	($r) = $taxonomy->list_opinions($opinion_no, $options);
    }
    
    catch {
	die $_;
    }
	
    finally {
	print STDERR $taxonomy->last_sql . "\n\n" if $request->debug;
    };
    
    die $request->exception(404, "Unknown opinion id '$opinion_no'")
	unless ref $r;
    
    $request->single_result($r);
    $request->{main_sql} = $taxonomy->last_sql;
}


# list_taxa ( )
# 
# Query the database for basic info about all taxa matching the specified
# parameters.  If the argument 'refs' is given, then return matching
# references instead of matching taxa.

sub list_taxa {
    
    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');

    $request->delete_output_field('container_no');
    
    # If the 'ref_id' parameter was included, signal an error.  I had to include it in the
    # ruleset which is used by both taxa/list and taxa/byref, but it is only relevant for the
    # second operation.
    
    if ( $request->clean_param('extra_ref_id') )
    {
	die $request->exception(400, "If you wish to use the parameter 'ref_id', use the operation 'taxa/byref' instead.");
    }
    
    # First, figure out the basic set of taxa we are being asked for.
    
    my ($rel, $base, $unknown) = $request->generate_query_base($taxonomy, 'taxa');
    
    $request->{my_rel} = $rel;
    
    if ( ref $unknown eq 'ARRAY' && @$unknown )
    {
	$request->add_result(@$unknown);
    }
    
    # Then determine any other filters to be applied, and also figure what
    # fields are necessary to show the requested info.
    
    my $options = $request->generate_query_options('taxa');
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings.
    
    $request->strict_check;
    $request->extid_check;
    
    # For relationships that could return a long list of taxa, we ask for a
    # DBI statement handle.
    
    if ( $rel eq 'all_children' || $rel eq 'all_taxa' || $rel eq 'all_records' )
    {
	$options->{return} = 'stmt';
    }
    
    # Otherwise, we ask for a list of taxon records.
    
    else
    {
	$options->{return} = 'list';
    }
    
    # Now execute the query.
    
    try {
	my @result = $taxonomy->list_taxa($rel, $base, $options);
	my @warnings = $taxonomy->list_warnings;
	
	$request->add_warning(@warnings) if @warnings;
	
	if ( $options->{return} eq 'stmt' )
	{
	    $request->sth_result($result[0]) if $result[0];
	}
	
	else
	{
	    $request->add_result(@result);
	}
    }
    
    catch {
	die $_;
    }
	
    finally {
	print STDERR $taxonomy->last_sql . "\n\n" if $request->debug;
    };
    
    $request->set_result_count($taxonomy->last_rowcount) if $options->{count};
    $request->{main_sql} = $taxonomy->last_sql;
}


sub list_associated {

    my ($request, $arg) = @_;
    
    my $dbh = $request->get_connection;
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    
    $request->delete_output_field('container_no') if $arg eq 'taxa';
    
    # First, figure out the set of taxa we are being asked for.
    
    my ($rel, $base) = $request->generate_query_base($taxonomy, $arg);
    
    # Then, determine the fields and options depending upon the type of
    # associated records we will be listing.
    
    my $options = $request->generate_query_options($arg);
    
    my @ref_filters = $request->generate_ref_filters;
    $options->{extra_filters} = \@ref_filters if @ref_filters;
    
    # Delete unused reference count fields.
    
    my @types = ref $options->{ref_type} eq 'ARRAY' ? @{$options->{ref_type}}
	: ('auth', 'class');
    
    my %type = map { $_ => 1 } @types;
    
    $request->{my_reftype} = \%type;
    
    if ( $request->has_block('counts') )
    {
	unless ( $type{all} )
	{
	    $request->delete_output_field('n_refauth') unless $type{auth} || $type{var} || $type{taxonomy};
	    $request->delete_output_field('n_refvar') unless $type{var};
	    $request->delete_output_field('n_refclass') unless $type{ops} || $type{class} || $type{taxonomy};
	    $request->delete_output_field('n_refunclass') unless $type{ops};
	    $request->delete_output_field('n_refoccs') unless $type{occs};
	    $request->delete_output_field('n_refspecs') unless $type{specs};
	    $request->delete_output_field('n_refcolls') unless $type{colls};
	}
    }
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings.
    
    $request->strict_check;
    $request->extid_check;
    
    # If debug mode is turned on, generate a closure which will be able to output debug
    # messages. 
    
    if ( $request->debug )
    {
	$options->{debug_out} = sub {
	    $request->{ds}->debug_line($_[0]);
	};
    }
    
    # For relationships that could return a long list of opinions, we ask for a
    # DBI statement handle.
    
    if ( $rel eq 'all_children' || $rel eq 'all_taxa' )
    {
	$options->{return} = 'stmt';
    }
    
    # Otherwise, we ask for a list of opinion records.
    
    else
    {
	$options->{return} = 'list';
    }
    
    # Now execute the query.
    
    try {
	my @result = $taxonomy->list_associated($rel, $base, $options);
	my @warnings = $taxonomy->list_warnings;
	
	$request->add_warning(@warnings) if @warnings;
	
	if ( $options->{return} eq 'stmt' )
	{
	    $request->sth_result($result[0]) if $result[0];
	}
	
	else
	{
	    $request->list_result(\@result);
	}
    }
    
    catch {
	die $_;
    };
	
    # finally {
    # 	print STDERR $taxonomy->last_sql . "\n\n" if $request->debug;
    # };
    
    $request->set_result_count($taxonomy->last_rowcount) if $options->{count};
    $request->{main_sql} = $taxonomy->last_sql;
}


sub list_opinions {
    
    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    
    my @opinion_nos = $request->safe_param_list('id');
    
    my $options = $request->generate_query_options('opinions');
    my $base;
    
    if ( @opinion_nos )
    {
	$base = \@opinion_nos;
    }
    
    else
    {
	$base = 'all_records';
    }
    
    $options->{return} = 'stmt';
    # $options->{assoc_type} ||= 'all';
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings.
    
    $request->strict_check;
    $request->extid_check;
    
    # Next, fetch the list of opinion records.
    
    my $sth;
    
    try {
	$sth = $taxonomy->list_opinions($base, $options);
	
	my @warnings = $taxonomy->list_warnings;
	$request->add_warning(@warnings) if @warnings;
	
	$request->sth_result($sth);
	$request->set_result_count($taxonomy->last_rowcount);
    }
    
    catch { die $_ }
    
    finally { print STDERR $taxonomy->last_sql . "\n\n" if $request->debug };
}


# get_taxa_by_name ( names, options )
# 
# Given a taxon name (or list of names), return either a list of ids or a
# range expression that can be used to select the corresponding taxa.

our ($NAME_SQL) = '';

# sub get_taxa_by_name {

#     my ($request, $names, $options) = @_;
    
#     $options ||= {};
#     my $dbh = $request->get_connection;
    
#     # We start with some common query clauses, depending on the options.
    
#     my (@clauses);
#     my $order_string = 'ORDER BY v.taxon_size';
#     my $limit_string = '';
#     my $fields = 't.orig_no';
    
#     # Do we accept common names?
    
#     if ( $DB_FIELD{common} && defined $options->{common} && $options->{common} eq 'only' )
#     {
# 	push @clauses, "common = 'EN'";
#     }
    
#     elsif ( $DB_FIELD{common} && $options->{common} )
#     {
# 	push @clauses, "common = ''";
#     }
    
#     # Invalid names?
    
#     my $status = $options->{status} // 'any';
    
#     if ( $status eq 'valid' )
#     {
# 	push @clauses, "status in ('belongs to', 'objective synonym of', 'subjective synonym of')";
#     }
    
#     elsif ( $status eq 'senior' )
#     {
# 	push @clauses, "status in ('belongs to')";
#     }
    
#     elsif ( $status eq 'invalid' )
#     {
# 	push @clauses, "status not in ('belongs to', 'objective synonym of', 'subjective synonym of')";
#     }
    
#     elsif ( $status ne 'any' && $status ne 'all' )
#     {
# 	push @clauses, "status = 'bad_value'";
#     }
    
#     # Number of results
    
#     unless ( $options->{all_names} )
#     {
# 	$limit_string = "LIMIT 1";
#     }
    
#     # Result fields
    
#     if ( $options->{return} eq 'range' )
#     {
# 	$fields = "t.orig_no, t.name, t.lft, t.rgt";
#     }
    
#     elsif ( $options->{return} eq 'id' )
#     {
# 	$fields = $options->{exact} ? 's.taxon_no' : 't.orig_no';
#     }
    
#     else
#     {
# 	$fields = "s.taxon_name as match_name, t.orig_no, t.name as taxon_name, t.rank as taxon_rank, t.status, v.taxon_size, t.orig_no, t.trad_no as taxon_no";
#     }
    
#     # The names might be given as a list, a hash, or a single string (in which
#     # case it will be split into comma-separated items).
    
#     my @name_list;
    
#     if ( ref $names eq 'ARRAY' )
#     {
# 	@name_list = @$names;
#     }
    
#     elsif ( ref $names eq 'HASH' )
#     {
# 	@name_list = keys %$names;
#     }
    
#     elsif ( ref $names )
#     {
# 	croak "get_taxa_by_name: parameter 'names' may not be a blessed reference";
#     }
    
#     else
#     {
# 	@name_list = split( qr{\s*,\s*}, $names );
#     }
    
#     # Now that we have a list, we evaluate the names one by one.
    
#     my (@result);
    
#  NAME:
#     foreach my $tn ( @name_list )
#     {
# 	my @filters;
	
# 	# We start by removing any bad characters and trimming leading and
# 	# trailing spaces.  Also translate all whitespace to a single space
# 	# and '.' to the wildcard '%'.  For example, "T.  rex" goes to
# 	# "T% rex";
	
# 	$tn =~ s/^\s+//;
# 	$tn =~ s/\s+$//;
# 	$tn =~ s/\s+/ /g;
# 	$tn =~ s/\./% /g;
# 	$tn =~ tr{a-zA-Z%_: }{}cd;
	
# 	# If we have a selection prefix, evaluate it and add the proper range
# 	# filter.
	
# 	if ( $tn =~ qr { [:] }xs )
# 	{
# 	    my $range = '';
	    
# 	    while ( $tn =~ qr{ ^ ([^:]+) : \s* (.*) }xs )
# 	    {
# 		my $prefix = $1;
# 		$tn = $2;
		
# 		# A prefix is only valid if it's a single word.  Otherwise, we
# 		# skip this name entirely because with an invalid prefix it cannot
# 		# evaluate to any actual name entry.
		
# 		if ( $prefix =~ qr{ ^ \s* ([a-zA-Z][a-zA-Z%]+) \s* $ }xs )
# 		{
# 		    $range = $request->get_taxon_range($1, $range);  
# 		}
		
# 		else
# 		{
# 		    next NAME;
# 		}
# 	    }
	    
# 	    # If we get here, we have evaluated all prefixes.  So add the
# 	    # resulting range to the list of filters.
	    
# 	    push @filters, $range if $range;
# 	}
	
# 	# Now, we determine the query necessary to find each name.
	
# 	# If we have a species name, we need to filter on both genus and
# 	# species name.  The name is not valid unless we have at least one
# 	# alphabetic character in the genus and one in the species.
	
# 	if ( $tn =~ qr{ ^ ([^\s]+) \s+ (.*) }xs )
# 	{
# 	    my $genus = $1;
# 	    my $species = $2;
	    
# 	    next unless $genus =~ /[a-zA-Z]/ && $species =~ /[a-zA-Z]/;
	    
# 	    # We don't have to quote these, because we have already eliminated
# 	    # all characters except alphabetic and wildcards.
	    
# 	    push @filters, "genus like '$genus'";
# 	    push @filters, "taxon_name like '$species'";
# 	}
	
# 	# If we have a higher taxon name, we just need to filter on that.  The
# 	# name is not valid unless it contains at least two alphabetic
# 	# characters. 
	
# 	elsif ( $tn =~ qr{ ^ ([^\s]+) $ }xs )
# 	{
# 	    my $higher = $1;
	    
# 	    next unless $higher =~ qr< [a-zA-Z]{2} >xs;
	    
# 	    push @filters, "taxon_name like '$higher' and taxon_rank >= 5";
# 	}
	
# 	# Otherwise, we have an invalid name so just skip it.
	
# 	else
# 	{
# 	    next NAME;
# 	}
	
# 	# Now, construct the query.
	
# 	my $filter_string = join(' and ', @clauses, @filters);
# 	$filter_string = '1=1' unless $filter_string;
	
# 	my $s_field = $DB_FIELD{orig_no} ? 'orig_no' : 'result_no';
# 	my $current_clause = $DB_FIELD{is_current} ? 's.is_current desc,' : '';
	
# 	$NAME_SQL = "
# 		SELECT $fields
# 		FROM taxon_search as s join taxon_trees as t on t.orig_no = s.$s_field
# 			join taxon_attrs as v on v.orig_no = t.orig_no
# 		WHERE $filter_string
# 		ORDER BY $current_clause v.taxon_size desc
# 		$limit_string";
	
# 	print STDERR $NAME_SQL . "\n\n" if $request->debug;
	
# 	my $records;
	
# 	if ( $options->{return} eq 'id' )
# 	{
# 	    $records = $dbh->selectcol_arrayref($NAME_SQL);
# 	}
	
# 	else
# 	{
# 	    $records = $dbh->selectall_arrayref($NAME_SQL, { Slice => {} });
# 	}
	
# 	push @result, @$records if ref $records eq 'ARRAY';
#     }
    
#     return @result;
# }


# sub get_taxon_range {
    
#     my ($request, $name, $range) = @_;
    
#     my $dbh = $request->get_connection;
#     my $range_filter = $range ? "and $range" : "";
    
#     my $sql = "
# 		SELECT t.lft, t.rgt
# 		FROM taxon_search as s JOIN taxon_trees as t on t.orig_no = s.synonym_no
# 			JOIN taxon_attrs as v on v.orig_no = t.orig_no
# 		WHERE s.taxon_name like '$name' $range_filter
# 		ORDER BY v.taxon_size LIMIT 1";
    
#     my ($lft, $rgt) = $dbh->selectrow_array($sql);
    
#     return $lft ? "t.lft between $lft and $rgt" : "t.lft = 0";
# }


# generate_query_fields ( )
# 
# Add a 'fields' option to the specified query options hash, according to the
# request parameters.  Then return the options hashref.

# sub generate_query_fields {

#     my ($request, $operation, $options) = @_;
    
#     my @fields = $request->select_list_for_taxonomy($operation);
    
#     if ( $operation eq 'refs' )
#     {
# 	push @fields, 'REF_COUNTS' if $request->has_block('counts') ;
#     }
    
#     $options->{fields} = \@fields;
    
#     return $options;
# }


# generate_query_options ( )
# 
# Return an options hash, based on the parameters, which can be passed to
# getTaxaByName or getTaxa.

sub generate_query_options {
    
    my ($request, $record_type) = @_;
    
    # Start with an empty hash
    
    my $options = { };
    
    # Figure out which fields we need
    
    my @fields = $request->select_list_for_taxonomy($record_type);
    
    if ( $record_type eq 'refs' )
    {
	push @fields, 'REF_COUNTS' if $request->has_block('counts') || $request->output_format eq 'ris';
    }
    
    $options->{fields} = \@fields;
    
    $options->{record_type} = $record_type if $record_type;
    
    # Handle some basic options
    
    my $limit = $request->result_limit;
    my $offset = $request->result_offset(1);
    
    $options->{limit} = $limit if defined $limit;	# $limit may be 0
    $options->{offset} = $offset if $offset;
    $options->{count} = 1 if $request->display_counts;
    
    my $status = $request->clean_param('taxon_status');
    my @pres = $request->clean_param_list('pres');
    
    if ( $request->has_block('acconly') )
    {
	die "400 you cannot specify 'taxon_status=$status' and 'show=acconly' in the same request.\n"
	    if defined $status && $status ne '' && $status ne 'accepted';
	$status = 'accepted';
    }
    
    elsif ( $status eq 'accepted' )
    {
	$request->delete_output_field('difference');
	$request->delete_output_field('accepted_no');
	$request->delete_output_field('accepted_rank');
	$request->delete_output_field('accepted_name');
    }
    
    $options->{status} = $status if $status ne '';
    
    my $extant = $request->clean_param('extant');
    
    if ( defined $extant && $extant ne '' )
    {
	$options->{extant} = $extant;
    }
    
    if ( my $rank = $request->clean_param('rank') )
    {
	$options->{rank} = $rank;
    }
    
    if ( $request->clean_param('immediate') )
    {
	$options->{immediate} = 1;
    }
    
    if ( my $depth = $request->clean_param('depth') )
    {
	$options->{depth} = $depth;
    }

    if ( my $idtype = $request->clean_param('idtype') )
    {
	$options->{ident_select} = $idtype;
    }
    
    if ( my $idqual = $request->clean_param('idqual') )
    {
	$options->{ident_qual} = $idqual;
    }
    
    # if ( my $occ_name = $request->clean_param('usetaxon') )
    # {
    # 	$options->{exact} = 1 if $occ_name =~ qr{ident};
    # 	$options->{higher} = 1 if $occ_name =~ qr{higher};
    # }

    # Now we look for the 'ref_type' parameter.  If we get a bad value,
    # generate an exception.
    
    if ( my @select = $request->clean_param_list('ref_type') )
    {
	if ( $select[0] && $select[0] eq '_' )
	{
	    die $request->exception(400, "No valid reference type was specified");
	}
	
	$options->{ref_type} = \@select;
    }
    
    if ( my $select = $request->clean_param('op_type') )
    {
	$options->{op_type} = $select;
    }
    
    if ( @pres )
    {
	my $pres_options = {};
	
	foreach my $v ( @pres )
	{
	    if ( $v eq 'regular' )
	    {
		$pres_options->{regular} = 1;
	    }
	    
	    elsif ( $v eq 'form' )
	    {
		$pres_options->{form} = 1;
	    }
	    
	    elsif ( $v eq 'ichno' )
	    {
		$pres_options->{ichno} = 1;
	    }
	    
	    elsif ( $v eq 'all' )
	    {
		$pres_options->{all} = 1;
	    }
	    
	    else
	    {
		die "400 Bad value '$v' for option 'pres': must be one of 'regular', 'form', 'ichno', 'all'\n";
	    }
	}
	
	if ( %$pres_options && ! $pres_options->{all} )
	{
	    $options->{pres} = $pres_options;
	}
    }
    
    # Handle variant=all and variant=current
    
    if ( my $var = $request->clean_param('variant') )
    {
	$options->{all_variants} = 1 if $var eq 'all';
	$options->{current_only} = 1 if $var eq 'current';
    }
    
    # If the user specified 'interval' or 'interval_id', then figure out the
    # corresponding max_ma and min_ma values.
    
    my ($int_max_ma, $int_min_ma) = $request->process_interval_params();
    
    # Check if the user specified these directly.  If so, they will override.
    
    my $max_ma = $request->clean_param('max_ma');
    my $min_ma = $request->clean_param('min_ma');
    
    if ( $max_ma ne '' )
    {
	die "400 bad value '$max_ma' for 'max_ma', must be greater than zero"
	    unless $max_ma > 0;
    }
    
    if ( $min_ma ne '' )
    {
	die "400 bad value '$min_ma' for 'min_ma', must be greater than or equal to zero"
	    unless $min_ma >= 0;
    }
    
    $max_ma = $int_max_ma if $max_ma eq '';
    $min_ma = $int_min_ma if $min_ma eq '';
    
    $options->{max_ma} = $max_ma if defined $max_ma && $max_ma > 0;
    $options->{min_ma} = $min_ma if defined $min_ma && $min_ma > 0;
    
    # Now check for author & publication date
    
    # my $op_status = $request->clean_param('op_status');
    my $pubyr = $request->clean_param('op_pubyr');
    my $author = $request->clean_param('op_author');
    
    # if ( $op_status )
    # {
    # 	$options->{op_status} = $op_status;
    # 	print STDERR "OP STATUS: $op_status\n";
    # }
    
    my ($min_pubyr, $max_pubyr);
    
    if ( $pubyr =~ qr{ ^ ( \d\d\d\d ) (?: \s* - \s* ( \d+ ) )? $ }xs )
    {
	$min_pubyr = $1;
	$max_pubyr = $2 || $1;
	
	if ( length($max_pubyr) < 4 )
	{
	    $max_pubyr = substr($min_pubyr, 0, 4 - length($max_pubyr)) . $max_pubyr;
	}
    }
    
    elsif ( $pubyr )
    {
	die "400 the parameter 'published' must be a year or range of years (was '$pubyr')\n";
    }
    
    if ( $max_pubyr )
    {
	$options->{op_max_pubyr} = $max_pubyr;
    }
    
    if ( $min_pubyr )
    {
	$options->{op_min_pubyr} = $min_pubyr;
    }
    
    if ( $author )
    {
	my @authors = split qr{\s*,\s*}, $author;
	$options->{op_author} = \@authors;
    }
    
    # Check for reference_id.
    
    if ( my @reflist = $request->clean_param_list('ref_id') )
    {
	$options->{reference_no} = \@reflist;
    }
    
    # Check for created, modified, authorized_by, etc.
    
    my @params = $request->param_keys();
    
    foreach my $key ( @params )
    {
	next unless $key =~ $PB2::CommonData::COMMON_OPT_RE;
	
	my $prefix = $1 // '';
	my $selector = $2;
	
	my $value = $request->clean_param($key);
	next unless defined $value && $value ne '';    
	
	if ( $selector =~ /_by/ )
	{
	    $value = $request->ent_filter('id_list', $key, $value);
	}
	
	if ( !$prefix)
	{
	    die $request->exception(400, "Invalid option '$key'");
	}
	
	elsif ( $prefix eq 'ops' )
	{
	    $options->{"op_$selector"} = $value;
	}
	
	elsif ( $prefix eq 'refs' )
	{
	    $options->{"ref_$selector"} = $value;
	}
	
	elsif ( $prefix eq 'taxa' )
	{
	    $options->{$selector} = $value;
	}
	
	else
	{
	    # ignore 'occs' and 'colls' if found
	}
    }
    
    # If we have any ordering terms, then apply them.
    
    my (@orders);
    
    if ( $record_type ne 'refs' )
    {
	foreach my $term ( $request->clean_param_list('order') )
	{
	    next unless $term;
	
	    my $dir;
	
	    if ( $term =~ /^(\w+)[.](asc|desc)$/ )
	    {
		$term = $1;
		$dir = $2;
	    }
	
	    # The following options default to ascending.
	
	    if ( $term eq 'hierarchy' || $term eq 'name' || $term eq 'childname' ||
		 $term eq 'author' || $term eq 'ref' || $term eq 'optype' )
	    {
		$dir ||= 'asc';
	    }
	
	    # The following options default to descending.
	
	    elsif ( $term eq 'pubyr' || $term eq 'firstapp' || $term eq 'lastapp' || $term eq 'agespan' || $term eq 'basis' ||
		    $term eq 'size' || $term eq 'extsize' || $term eq 'n_occs' || $term eq 'extant' ||
		    $term eq 'created' || $term eq 'modified' )
	    {
		$dir ||= 'desc';
	    }
	
	    # If we find an unrecognized option, throw an error.
	
	    else
	    {
		$request->add_warning("unrecognized order option '$term'");
		next;
	    }
	
	    # Add the direction (asc or desc) if one was specified.
	
	    push @orders, "$term.$dir";
	}
    }
    
    else	# $record_type eq 'refs' 
    {
	foreach my $term ( $request->clean_param_list('order') )
	{
	    next unless $term;
	
	    my $dir;
	
	    if ( $term =~ /^(\w+)[.](asc|desc)$/ )
	    {
		$term = $1;
		$dir = $2;
	    }
	
	    # The following options default to ascending.
	    
	    if ( $term eq 'author' || $term eq 'reftitle' || $term eq 'pubtitle' || $term eq 'pubtype' ||
		 $term eq 'language' )
	    {
		$dir ||= 'asc';
	    }
	    
	    # The following options default to descending.
	    
	    elsif ( $term eq 'pubyr' || $term eq 'created' || $term eq 'modified' )
	    {
		$dir ||= 'desc';
	    }
	    
	    # If we find an unrecognized option, throw an error.
	
	    else
	    {
		$request->add_warning("unrecognized order option '$term'");
		next;
	    }
	
	    # Add the direction (asc or desc) if one was specified.
	
	    push @orders, "$term.$dir";
	}
    }
    
    # If no explicit order was specified, use a default.  But if we are listing all taxa in the
    # database, do not specify any order.  In this case, the most efficient way to list these
    # records is in the order they occur in the taxon_trees table.
    
    unless ( @orders )
    {
	unless ( $record_type eq 'refs' || $record_type eq 'opinions' ||
		 $request->{my_rel} && $request->{my_rel} =~ /^all_/ )
	{
	    push @orders, "hierarchy.asc" unless @orders;
	}
    }
    
    # Set the selected order option.
    
    $options->{order} = \@orders if @orders;
    
    return $options;
}


# select_list_for_taxonomy ( type )
# 
# Retrieve the selection list for the current request, and then translate any
# field names coming from this module into the proper field specifiers for the
# routines in Taxonomy.pm.
# 
# The parameter $type indicates what type of record is being requested.
# Allowed values are 'refs' for references, 'opinions' for opinions, or 'taxa' for
# taxa (the default if not specified).

sub select_list_for_taxonomy {

    my ($request, $operation) = @_;
    
    my @fields;
    
    $operation //= 'taxa';
    
    croak "bad value '$operation' for 'operation': must be 'refs', 'opinions', or 'taxa'"
	if $operation ne 'refs' && $operation ne 'opinions' && $operation ne 'taxa';
    
    foreach my $f ( $request->select_list )
    {
	if ( $f =~ qr{^[A-Z_]+$} )
	{
	    push @fields, $f;
	}
	
	elsif ( $f =~ qr{^\$cd\.created} )
	{
	    push @fields, $operation eq 'refs'     ? 'REF_CRMOD'
			: $operation eq 'opinions' ? 'OP_CRMOD'
					      : 'CRMOD';
	}
	
	elsif ( $f =~ qr{^\$cd\.authorizer_no} )
	{
	    push @fields, $operation eq 'refs'     ? 'REF_AUTHENT'
			: $operation eq 'opinions' ? 'OP_AUTHENT'
					      : 'AUTHENT';
	}
	
	# The following will only happen if the block 1.2:refs:basic is included.
	
	elsif ( $f =~ qr{^r[.]author1last} )
	{
	    push @fields, 'REF_DATA';
	}
	
	elsif ( $f =~ qr{^rs[.]n_reftaxa} || $f eq 'COUNTS' )
	{
	    push @fields, 'REF_COUNTS' if $operation eq 'refs';
	}
	
	elsif ( $f !~ qr{^\$cd\.|^r[.]|^rs[.]} )
	{
	    push @fields, $f;
	}
    }
    
    return @fields;
}


# generate_query_base ( taxonomy )
# 
# Generate the set of base taxon identifiers and the relationship code that
# will be used to satisfy the request for which this routine has been called.

sub generate_query_base {
    
    my ($request, $taxonomy, $record_type) = @_;
    
    my ($taxon_names, @taxon_ids, @exclude_ids, $rel);
    my $specified_rel = $request->clean_param('rel');
    my $resolve_options = {};
    
    if ( $taxon_names = $request->clean_param('base_name') )
    {
	$rel = $specified_rel || 'all_children';
    }
    
    elsif ( $taxon_names = $request->clean_param('match_name') )
    {
	$rel = $specified_rel || 'exact';
	$resolve_options->{all_names} = 1;
    }
    
    elsif ( $taxon_names = $request->clean_param('taxon_name') )
    {
	$rel = $specified_rel || 'exact';
    }
    
    elsif ( @taxon_ids = $request->safe_param_list('base_id') )
    {
	$rel = $specified_rel || 'all_children';
    }
    
    elsif ( @taxon_ids = $request->safe_param_list('taxon_id') )
    {
	$rel = $specified_rel || 'exact';
    }
    
    elsif ( $request->clean_param('all_taxa') )
    {
	return 'all_taxa';
    }
    
    elsif ( $request->clean_param('all_records') || $request->clean_param('ref_id') )
    {
	return 'all_records';
    }
    
    else
    {
	die "400 No taxa specified.\n";
    }
    
    # If 'common' was specified, then add the appropriate option. $$$ this needs
    # to be done in list_taxa, so we can also add common name to the output
    # where appropriate.
    
    if ( my @lang = $request->clean_param_list('common') )
    {
	my @common;
	
	foreach my $l ( @lang )
	{
	    $l = uc $l;
	    
	    if ( $LANGUAGE{$l} )
	    {
		push @common, $l;
	    }
	    elsif ( $l ne 'X' )
	    {
		$request->add_warning("Unknown language code '$l'");
	    }
	}
	
	return ($rel, []) unless @common;
	
	$resolve_options->{common} = \@common;
    }
    
    # If 'taxon_status' was specified, then add
    
    # Now figure out the base taxa and excluded taxa if any.
    
    my (@taxa, @unknown_taxa);
    
    # If we are listing by name (as opposed to id) then resolve the specified
    # string into one or more taxonomic name records. Some of these will
    # represent base taxa, others may represent taxa to be excluded. These
    # latter will have the 'exclude' flag set.
    
    if ( $taxon_names )
    {
	@taxa = $taxonomy->resolve_names($taxon_names, $resolve_options);
	
	my @warnings = $taxonomy->list_warnings;	
	$request->add_warning(@warnings) if @warnings;
	
	my $sql = $taxonomy->last_sql;
	print STDERR "$sql\n\n" if $sql && $request->debug;
    }
    
    # Otherwise, identifiers were given.  Resolve the list of identifiers
    # into one or more taxonomic name records.
    
    else
    {
	my @clean_ids = grep { $_ > 0 } @taxon_ids;
	
	@taxa = $taxonomy->list_taxa('exact', \@clean_ids);
	
	my @warnings = $taxonomy->list_warnings;	
	$request->add_warning(@warnings) if @warnings;

	if ( $rel eq 'exact' || $rel eq 'current' )
	{
	    my @unknown_ids = grep { /^[UN]/ } @taxon_ids;
	    @unknown_taxa = map { generate_unknown_taxon($_) } @unknown_ids;
	}
    }
    
    # Now see if the 'exclude_id' parameter was given.  If so, list all of
    # these taxa (and set the 'exclude' flag on each one) and add this list to
    # @taxa.
    
    if ( @exclude_ids = $request->clean_param_list('exclude_id') )
    {
	push @taxa, $taxonomy->list_taxa_simple(\@exclude_ids, { exclude => 1 });
	
	my @warnings = $taxonomy->list_warnings;
	$request->add_warning(@warnings) if @warnings;
    }
    
    # Then see if any of the taxonomic identifiers specified in the request
    # were bad. Because of the way that 'list_taxa' and 'list_taxa_simple' are
    # implemented, we have to check this directly.
    
    my %bad_nos = map { $_ => 1 } grep { $_ && $_ > 0 } @taxon_ids, @exclude_ids;
    
    foreach my $t ( @taxa )
    {
	delete $bad_nos{$t->{taxon_no}} if $t->{taxon_no};
	delete $bad_nos{$t->{orig_no}} if $t->{orig_no};
    }
    
    # If we have found any bad identifiers, make sure that a warning is
    # returned for each one.
    
    foreach my $t ( keys %bad_nos )
    {
	$request->add_warning("Unknown taxon '$t'");
    }
    
    # Return the specified relationship and list of taxa.
    
    return $rel, \@taxa, \@unknown_taxa;
}


# auto ( )
# 
# Return an auto-complete list, given a partial name.

sub auto {
    
    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');

    $request->delete_output_field('container_no');
    
    my $partial = $request->clean_param('name');
    
    my $search_table = $taxonomy->{SEARCH_TABLE};
    my $names_table = $taxonomy->{NAMES_TABLE};
    my $attrs_table = $taxonomy->{ATTRS_TABLE};
    
    my $sql;
    
    # Strip out any characters that don't appear in names.  But allow SQL wildcards.
    
    $partial =~ tr/[a-zA-Z_%. ]//dc;
    
    # Construct and execute an SQL statement.
    
    my $limit = $request->sql_limit_clause(1);
    my $calc = $request->sql_count_clause;
    
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
    
    $request->{main_sql} = $sql;
    
    print STDERR $sql . "\n\n" if $request->debug;
    
    $request->{main_sth} = $dbh->prepare($sql);
    $request->{main_sth}->execute();
}


# auto_complete_txn ( name, limit )
# 
# This method provides an alternate matching operation, designed to be called from the combined
# auto-completion operation for client applications. It is passed a name, which is taken to be a
# prefix, and a limit on the number of results to return.

sub auto_complete_txn {
    
    my ($request, $name, $limit) = @_;
    
    my $dbh = $request->get_connection();
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    
    my $search_table = $taxonomy->{SEARCH_TABLE};
    my $names_table = $taxonomy->{NAMES_TABLE};
    my $attrs_table = $taxonomy->{ATTRS_TABLE};
    my $ints_table = $taxonomy->{INTS_TABLE};
    my $tree_table = $taxonomy->{TREE_TABLE};
    
    $limit ||= 10;
    my @filters;
    
    my $use_extids = $request->has_block('extids');
    
    my $fields = "s.full_name as name, s.taxon_rank, s.taxon_no, s.accepted_no, s.orig_no, t.status, tv.spelling_no, " .
	"tv.name as accepted_name, v.n_occs, n.spelling_reason, acn.spelling_reason as accepted_reason, " .
	"if(ph.class <> '', ph.class, if(ph.phylum <> '', ph.phylum, ph.kingdom)) as higher_taxon";
    my $sql;
    my $filter;
    
    # Strip out any characters that don't appear in taxonomic names.  But allow SQL wildcards.
    
    $name =~ tr/[a-zA-Z_%. ]//dc;
    
    # If we are given a genus (possibly abbreviated), generate a search on
    # genus and species name.
    
    if ( $name =~ qr{ ^ ([a-zA-Z_]+) ( [.] | [.%]? \s+ ) ([a-zA-Z_%]+) }xs )
    {
	my $genus = ($2 ne ' ') ? $dbh->quote("$1%") : $dbh->quote($1);
	my $species = $dbh->quote("$3%");
	
	$filter = "s.genus like $genus and s.taxon_name like $species";
    }
    
    # If we are given a name like '% somespecies', then generate a search on species name only.
    
    elsif ( $name =~ qr{ ^ %[.]? \s+ ([a-zA-Z_%]+) $ }xs )
    {
	my $species = $dbh->quote("$1%");
	
	$filter = "s.taxon_name like $species and s.taxon_rank = 'species'";
    }
    
    # If we are given a single name followed by one or more spaces and nothing
    # else, take it as a genus name.
    
    elsif ( $name =~ qr{ ^ ([a-zA-Z]+) ([.%]+)? \s+ $ }xs )
    {
	my $genus = $2 ? $dbh->quote("$1%") : $dbh->quote($1);
	
	$filter = "s.genus like $genus and s.taxon_rank = 'genus'";
    }
    
    # Otherwise, if it has no spaces then just search for the name.  Turn all
    # periods into wildcards.
    
    elsif ( $name =~ qr{^[a-zA-Z_%.]+$} )
    {
	return if length($name) < 3;
	
	$name =~ s/\./%/g;
	
	$filter = "s.full_name like " . $dbh->quote("$name%");
    }
    
    # If none of these patterns are matched, return an empty result.
    
    else
    {
	return;
    }
    
    # Now execute the query.
    
    $sql = "SELECT $fields
		FROM $search_table as s JOIN $tree_table as t using (orig_no)
			JOIN $tree_table as tv on tv.orig_no = t.accepted_no
			JOIN $attrs_table as v on v.orig_no = t.accepted_no
			JOIN $ints_table as ph on ph.ints_no = tv.ints_no
			JOIN $names_table as n on n.taxon_no = s.taxon_no
			JOIN $names_table as acn on acn.taxon_no = t.spelling_no
		WHERE $filter
		ORDER BY s.taxon_no = tv.spelling_no desc, n_occs desc LIMIT $limit";
    
    print STDERR "$sql\n\n" if $request->debug;
    
    my $result_list = $dbh->selectall_arrayref($sql, { Slice => { } });
    my %found_taxon;
    my @results;
    
    # If we found some results, go through the list and process each record. The method
    # 'process_difference' is called to generate the 'difference' (tdf) field. The 'oid' and 'vid'
    # fields are converted to external identifiers if appropriate. Finally, we keep track of the
    # orig_no of each record, and skip any repeats. This filters out multiple records in cases
    # where a name was changed in rank, and possibly other cases.
    
    if ( ref $result_list eq 'ARRAY' )
    {
	foreach my $r ( @$result_list )
	{
	    next if $found_taxon{$r->{orig_no}};
	    $found_taxon{$r->{orig_no}} = 1;
	    
	    $request->process_difference($r);
	    $r->{record_id} = $use_extids ? generate_identifier('TXN', $r->{accepted_no}) :
		$r->{accepted_no};
	    $r->{taxon_no} = generate_identifier('VAR', $r->{taxon_no}) if $use_extids;
	    $r->{record_type} = 'txn' unless $use_extids;
	    
	    push @results, $r;
	}
    }
    
    return @results;
}


# get_image ( )
# 
# Given an id (image_no) value, taxon_id, or taxon_name, return the
# corresponding image if the format is 'png', and information about it if the
# format is 'json'.

sub get_image {
    
    my ($request, $type) = @_;
    
    $type ||= '';
    
    my $dbh = $request->get_connection;
    my ($sql, $result);
    
    croak "invalid type '$type' for get_image"
	unless $type eq 'icon' || $type eq 'thumb';
    
    $request->strict_check;
    $request->extid_check;
    
    my $format = $request->output_format;
    
    my $joins = "";
    my @clauses;
    
    # If we are given an image_no value, then select that particular image.
    
    if ( my $image_no = $request->clean_param('id') )
    {
	push @clauses, "image_no = $image_no";
    }

    # If we are given a taxon_no value, then select the image corresponding to
    # that taxon.

    elsif ( my $taxon_no = $request->clean_param('taxon_id') )
    {
	my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
	
	$joins = "
		join $taxonomy->{ATTRS_TABLE} as v using (image_no)
		join $taxonomy->{TREE_TABLE} as t on v.orig_no = t.accepted_no
		join $taxonomy->{AUTH_TABLE} as a on t.orig_no = a.orig_no";
	
	push @clauses, "a.taxon_no = $taxon_no";
    }

    elsif ( my $taxon_name = $request->clean_param('taxon_name') )
    {
	my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
	
	# Return an immediate error if more than one name was specified.
	
	die $request->exception(400, "The value of 'taxon_name' must be a single taxon name")
	    if $taxon_name =~ qr{,};
	
	# Look up the identifier corresponding to the name (if more than one name matches, the one
	# with the highest number of occurrences in the database is chosen).
	
	my $options = { fields => 'SIMPLE' };
	
	if ( $request->debug )
	{
	    $options->{debug_out} = sub {
		$request->{ds}->debug_line($_[0]);
	    };
	}
	
	my ($taxon) = $taxonomy->resolve_names($taxon_name, $options);
	
	my @warnings = $taxonomy->list_warnings;
	$request->add_warning(@warnings) if @warnings;
	
	die $request->exception(400, "Invalid taxon name '$taxon_name'")
	    if $taxonomy->has_warning('W_BAD_NAME');
	
	die $request->exception(404, "Taxon '$taxon_name' was not found in the database")
	    unless $taxon;
	
	$joins = "
		join $taxonomy->{ATTRS_TABLE} as v using (image_no)";
	
	push @clauses, "v.orig_no = $taxon->{accepted_no}";
    }
    
    # Generate the proper filter.
    
    my $filter = join( q{ and }, @clauses );
    
    croak "No filter clauses were generated\n" unless $filter;
    
    # If the output format is 'png', then query for the image.  If found,
    # return it in $request->{main_data}.  Otherwise, we throw a 404 error.
    
    if ( $format eq 'png' )
    {
	$request->{main_sql} = "
		SELECT $type FROM $PHYLOPICS as p $joins
		WHERE $filter";
	
	$request->{ds}->debug_line($request->{main_sql}) if $request->debug;
	
	($request->{main_data}) = $dbh->selectrow_array($request->{main_sql});
	
	die $request->exception(404, "Image not found") unless $request->{main_data};
    }
    
    # If the output format is 'json' or one of the text formats, then query
    # for information about the image.  Return immediately regardless of
    # whether or not a record was found.  If not, an empty response will be
    # generated.
    
    else
    {
	my $fields = $request->select_string();
	
	$request->{main_sql} = "
		SELECT $fields FROM $PHYLOPICS as p $joins
		WHERE $filter";
	
	$request->{ds}->debug_line($request->{main_sql}) if $request->debug;
	
	$request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
	
	die $request->exception(404, "Image not found") unless $request->{main_record};
    }
}


# list_images ( )
# 
# Return a list of images that meet the specified criteria.

sub list_images {

    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    my $taxonomy = TaxonomyOld->new($dbh, 'taxon_trees');
    my ($sql, $result);
    
    my @filters;
    
    # If the parameter 'name' was given, then fetch all matching taxa.  Order
    # them in descending order by size.
    
    my @taxon_list;
    
    if ( my $name = $request->clean_param('name') )
    {
	my $name_select = { spelling => 'exact', return => 'id' };
	
	@taxon_list = $request->get_taxa_by_name($name, $name_select);
	return unless @taxon_list;
    }
    
    else
    {
	@taxon_list = $request->clean_param_list('id');
    }
    
    # Now add any other filters that were specified by the parameters.
    
    if ( $request->clean_param('rel') eq 'all_children' )
    {
	push @filters, '';
    }
    
    if ( my $depth = $request->clean_param('depth') )
    {
	push @filters, '';
    }
    
    # Construct a query. $$$
    
    my $fields = $request->select_string();
    
    $request->{main_sql} = "
	SELECT $fields FROM $PHYLOPICS as p JOIN $PHYLOPIC_NAMES as n using (uid)
		JOIN authorities as a using (taxon_name) #etc
	WHERE image_no = image_no";
	
	$request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
	
	return;
    
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
    
    no warnings 'numeric';
    no warnings 'uninitialized';
    
    my ($value, $context) = @_;
    
    my @selectors = split qr{\s*,\s*}, $value;
    my @rank_errors;
    my @errors;
    my @ranks;
    
    foreach my $s (@selectors)
    {
	next unless $s;		# skip any blank entries
	
	if ( $s =~ qr{ ^ \s* ( above_ | below_ | min_ | max_ )? ( \w+ ) \s* $ }xsi )
	{
	    if ( $TAXON_RANK{$2} )
	    {
		push @ranks, lc($1 // '') . $TAXON_RANK{$2};
	    }
	    
	    else
	    {
		push @rank_errors, $2;
	    }
	}
	
	elsif ( $s =~ qr{ ^ \s* ( [^-]+ ) \s* - \s* ( .+ ) \s* $ }xsi )
	{
	    my $bottom = $1;
	    my $top = $2;
	    my $range = '';
	    
	    if ( $bottom =~ qr{ ^ ( above_ | below_ | min_ | max_ )? ( \w+ ) $ }xsi )
	    {
		my $prefix = $1;
		
		if ( $TAXON_RANK{$2} )
		{
		    $bottom = lc($1 || 'min_') . $TAXON_RANK{$2};
		}
		
		else
		{
		    push @rank_errors, $2;
		}
		
		push @errors, "invalid use of '$prefix'" if defined $prefix &&
		    $prefix =~ qr{ ^below | ^max }xsi;
	    }
	    
	    else
	    {
		push @rank_errors, $bottom;
	    }
	    
	    if ( $top =~ qr{ ^ ( above_ | below_ | min_ | max_ )? ( \w+ ) $ }xsi )
	    {
		my $prefix = $1;
		
		if ( $TAXON_RANK{$2} )
		{
		    $top = lc($1 || 'max_') . $TAXON_RANK{$2};
		}
		
		else
		{
		    push @rank_errors, $2;
		}
		
		push @errors, "invalid use of '$prefix'" if defined $prefix &&
		    $prefix =~ qr{ ^above | ^min }xsi;
	    }
	    
	    else
	    {
		push @rank_errors, $top;
	    }
	    
	    push @ranks, "$bottom-$top";
	}
	
	else
	{
	    push @rank_errors, $s;
	}
    }
    
    # If any errors were detected, return the appropriate result.
    
    if ( @rank_errors )
    {
	my $errstr = join( q{', '}, @rank_errors );
	push @errors, "invalid taxonomic rank '$errstr'";
    }
    
    if ( @errors )
    {
	my $errstr = join( q{; }, @errors );
	return { error => $errstr };
    }
    
    # If we have at least one selected rank, return the list of ranks.
    
    if ( @ranks )
    {
	return { value => \@ranks };
    }
    
    # Otherwise, return a result that will select nothing.
    
    else
    {
	return { value => [ 0 ] };
    }
}


sub generate_unknown_taxon {
    
    my ($taxon_no) = @_;
    
    my $code = substr($taxon_no, 0, 2);
    $code =~ s/^U/N/;
    
    return { orig_no => $taxon_no, taxon_rank => $UNS_RANK{$code},
	     taxon_name => $UNS_NAME{$code} };
};


# This routine will be called if necessary in order to properly process the
# results of a query for taxon parents.

sub processResultSet {
    
    my ($request, $rowlist) = @_;
    
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


# For each record, do any necessary processing.

sub process_op {
    
    my ($request, $record) = @_;
    
    if ( $record->{author} && $record->{pubyr} )
    {
	$record->{attribution} = $record->{author} . ' ' . $record->{pubyr};
    }
    
    else
    {
	$record->{attribution} = $record->{author} || $record->{pubyr};
    }
}


sub process_pbdb {
    
    my ($request, $record) = @_;
    
    if ( defined $record->{attribution} && defined $record->{taxon_no} && defined $record->{orig_no} &&
	 $record->{orig_no} eq $record->{taxon_no} )
    {
	if ( $record->{attribution} =~ qr{ ^ [(] (.*) [)] $ }xs )
	{
	    $record->{attribution} = $1;
	}
    }
    
    $record->{is_extant} = ! defined $record->{is_extant} ? ''
			 : $record->{is_extant} eq '1'    ? 'extant'
			 : $record->{is_extant} eq '0'    ? 'extinct'
							  : '?';
    
    $record->{n_orders} = undef if defined $record->{n_orders} && 
	$record->{n_orders} == 0 && $record->{taxon_rank} <= 13;
    
    $record->{n_families} = undef if defined $record->{n_families} &&
	$record->{n_families} == 0 && $record->{taxon_rank} <= 9;
    
    $record->{n_genera} = undef if defined $record->{n_genera} &&
	$record->{n_genera} == 0 && $record->{taxon_rank} <= 5;
    
    $record->{n_species} = undef if defined $record->{n_species} &&
	$record->{n_species} == 0 && $record->{taxon_rank} <= 3;
    
    # if ( $record->{is_trace} )
    # {
    # 	$record->{preservation} = 'ichnotaxon';
    # 	$record->{preservation} .= '+form taxon' if $record->{is_form};
    # }
    
    # elsif ( $record->{is_form} )
    # {
    # 	$record->{preservation} = 'form taxon';
    # }
    
    $request->process_flags($record);
    
    my $a = 1;	# we can stop here when debugging
}


sub process_com {
    
    my ($request, $record) = @_;
    
    # Deduplicate accepted_no and accepted name, only for 'com' vocabulary
    
    if ( defined $record->{accepted_no} && defined $record->{orig_no} &&
	 $record->{accepted_no} eq $record->{orig_no} )
    {
	unless ( defined $record->{taxon_no} && defined $record->{spelling_no} &&
		 $record->{taxon_no} ne $record->{spelling_no} )
	{
	    delete $record->{accepted_no};
	    delete $record->{accepted_rank};
	    delete $record->{accepted_name};
	}
    }
    
    if ( defined $record->{attribution} && defined $record->{taxon_no} && defined $record->{orig_no} &&
	 $record->{orig_no} eq $record->{taxon_no} )
    {
	if ( $record->{attribution} =~ qr{ ^ [(] (.*) [)] $ }xs )
	{
	    $record->{attribution} = $1;
	}
    }
    
    $record->{no_variant} = 1 if $record->{taxon_no} && $record->{orig_no} &&
    	$record->{taxon_no} eq $record->{orig_no};
    
    # $record->{no_variant} = 1 unless defined $record->{spelling_no};
    
    # $record->{no_variant} = 1 if defined $record->{orig_no} && defined $record->{child_spelling_no} &&
    # 	$record->{orig_no} eq $record->{child_spelling_no};
    
    # $record->{no_variant} = 0 if defined $request->{my_rel} && 
    # 	($request->{my_rel} eq 'variants' || $request->{my_rel} eq 'exact' ||
    # 	 $request->{my_rel} eq 'current');
    
    $record->{n_orders} = undef if defined $record->{n_orders} && 
	$record->{n_orders} == 0 && $record->{taxon_rank} <= 13;
    
    $record->{n_families} = undef if defined $record->{n_families} &&
	$record->{n_families} == 0 && $record->{taxon_rank} <= 9;
    
    $record->{n_genera} = undef if defined $record->{n_genera} &&
	$record->{n_genera} == 0 && $record->{taxon_rank} <= 5;
    
    $record->{n_species} = undef if defined $record->{n_species} &&
	$record->{n_species} == 0 && $record->{taxon_rank} <= 3;
    
    # if ( $record->{is_trace} )
    # {
    # 	$record->{preservation} = 'I';
    # 	$record->{preservation} .= 'F' if $record->{is_form};
    # }
    
    # elsif ( $record->{is_form} )
    # {
    # 	$record->{preservation} = 'F';
    # }
    
    $request->process_flags($record);
    
    my $a = 1;	# we can stop here when debugging
}


sub process_flags {

    my ($request, $record) = @_;
    
    if ( $record->{exclude} )
    {
	$record->{flags} = 'E';
    }
    
    elsif ( defined $record->{base_no} && defined $record->{orig_no} && $record->{base_no} eq $record->{orig_no} )
    {
	$record->{flags} = 'B';
    }
    
    elsif ( $record->{is_base} )
    {
	$record->{flags} = 'B';
    }
    
    else 
    {
	$record->{flags} = '';
    }
    
    if ( defined $record->{spelling_no} && defined $record->{taxon_no} &&
	 $record->{taxon_no} ne $record->{spelling_no} )
    {
	$record->{flags} .= 'V';
    }
    
    if ( $record->{is_trace} )
    {
	$record->{flags} .= 'I';
    }
    
    if ( $record->{is_form} )
    {
	$record->{flags} .= 'F';
    }
    
    # if ( $record->{is_ident} && $record->{orig_no} ne $record->{accepted_no} )
    # {
    # 	$record->{flags} .= 'I';
    # }
    
    my $a = 1;	# we can stop here when debugging
}


sub process_taxon_ids {
    
    my ($request, $record) = @_;
    
    return unless $request->{block_hash}{extids};
    
    # my $make_ids = $request->clean_param('extids');
    # $make_ids = 1 if ! $request->param_given('extids') && $request->output_vocab eq 'com';
    
    # return unless $make_ids;
    
    # $request->delete_output_field('record_type');
    
    foreach my $f ( qw(orig_no child_no parent_no immpar_no senpar_no accepted_no base_no
		       kingdom_no phylum_no class_no order_no family_no genus_no
		       subgenus_no type_taxon_no container_no) )
    {
	$record->{$f} = generate_identifier('TXN', $record->{$f}) if defined $record->{$f};
	# $record->{$f} = $record->{$f} ? "$IDP{TXN}:$record->{$f}" : '';
    }
    
    foreach my $f ( qw(taxon_no spelling_no child_spelling_no parent_spelling_no parent_current_no) )
    {
	$record->{$f} = generate_identifier('VAR', $record->{$f}) if defined $record->{$f};
	# $record->{$f} = $record->{$f} ? "$IDP{VAR}:$record->{$f}" : '';
    }
    
    if ( ref $record->{parent_txn} )
    {
	foreach my $f ( qw(parent_txn kingdom_txn phylum_txn class_txn order_txn family_txn) )
	{
	    # This has to be first, because the two fields won't be equal once external
	    # identifiers have been generated.
	    $record->{$f}{no_variant} = 1 if $record->{$f}{orig_no} && $record->{$f}{taxon_no} && 
		$record->{$f}{orig_no} eq $record->{$f}{taxon_no};
	    $record->{$f}{no_recordtype} = 1;
	    
	    delete $record->{$f}{accepted_no};
	    
	    $record->{$f}{orig_no} = generate_identifier('TXN', $record->{$f}{orig_no}) if defined $record->{$f}{orig_no};
	    $record->{$f}{taxon_no} = generate_identifier('VAR', $record->{$f}{taxon_no}) if defined $record->{$f}{taxon_no};
	}
	
	foreach my $f ( qw(children phylum_list class_list order_list family_list genus_list subgenus_list species_list subspecies_list) )
	{
	    next unless ref $record->{$f} eq 'ARRAY';
	    
	    foreach my $t ( @{$record->{$f}} )
	    {
		# This has to be first, because the two fields won't be equal once external
		# identifiers have been generated.
		$t->{no_variant} = 1 if $t->{orig_no} && $t->{taxon_no} && $t->{orig_no} eq $t->{taxon_no};
		$t->{no_recordtype} = 1;
		
		delete $t->{accepted_no};
		
		$t->{orig_no} = generate_identifier('TXN', $t->{orig_no}) if defined $t->{orig_no};
		$t->{taxon_no} = generate_identifier('VAR', $t->{taxon_no}) if defined $t->{taxon_no};
	    }
	}
    }
    
    foreach my $f ( qw(opinion_no) )
    {
	$record->{$f} = generate_identifier('OPN', $record->{$f}) if defined $record->{$f};
	# $record->{$f} = $record->{$f} ? "$IDP{OPN}:$record->{$f}" : '';
    }
    
    foreach my $f ( qw(image_no) )
    {
	$record->{$f} = generate_identifier('PHP', $record->{$f}) if defined $record->{$f};
    }
    
    # foreach my $f ( qw(authorizer_no enterer_no modifier_no) )
    # {
    # 	$record->{$f} = $record->{$f} ? generate_identifier('PRS', $record->{$f}) : '';
    # }
    
    if ( ref $record->{reference_no} eq 'ARRAY' )
    {
	my @extids = map { generate_identifier('REF', $_) } @{$record->{reference_no}};
	$record->{reference_no} = \@extids;
    }
    
    elsif ( defined $record->{reference_no} )
    {
	$record->{reference_no} = generate_identifier('REF', $record->{reference_no});
	# $record->{reference_no} = "$IDP{REF}:$record->{reference_no}";
    }
}


sub process_difference {
    
    my ($request, $record) = @_;
    
    # If the orig_no and accepted_no are different, then the name is either
    # invalid or a junior synonym.  So we use the status as the reason.
    
    if ( defined $record->{orig_no} && defined $record->{accepted_no} &&
	 $record->{orig_no} ne $record->{accepted_no} )
    {
	$record->{difference} = $record->{status};
    }
    
    # If  the accepted name is a different variant of the same
    # orig_no, it takes a bit more work to figure out the reason.
    
    elsif ( defined $record->{spelling_no} && $record->{taxon_no} ne $record->{spelling_no} )
    {
	if ( $record->{accepted_reason} && $record->{accepted_reason} eq 'recombination' ||
	     $record->{spelling_reason} && $record->{spelling_reason} eq 'recombination' )
	{
	    $record->{difference} = 'recombined as';
	}
	
	elsif ( $record->{accepted_reason} && $record->{accepted_reason} eq 'reassignment' ||
		$record->{spelling_reason} && $record->{spelling_reason} eq 'reassignment' )
	{
	    $record->{difference} = 'reassigned as';
	}
	
	elsif ( $record->{accepted_reason} && $record->{accepted_reason} eq 'correction' &&
		! ($record->{spelling_reason} && $record->{spelling_reason} eq 'correction' ) )
	{
	    $record->{difference} = 'corrected to';
	}
	
	elsif ( $record->{spelling_reason} && $record->{spelling_reason} eq 'misspelling' )
	{
	    $record->{difference} = 'misspelling of';
	}
	
	else
	{
	    $record->{difference} = 'obsolete variant of';
	}
    }
    
    my $a = 1; # we can stop here when debugging
}


sub process_subgenus {
    
    my ($request, $record) = @_;
    
    if ( $record->{subgenus} )
    {
	$record->{genus} = $record->{subgenus};
    }
}


sub process_ages {
    
    my ($request, $record ) = @_;
    
    return if $record->{ages_processed};
    
    foreach my $field ( qw(firstapp_ea firstapp_la lastapp_ea lastapp_la
			   firstocc_ea firstocc_la lastocc_ea lastocc_la) )
    {
	if ( $record->{$field} )
	{
	    $record->{$field} =~ s{ (?: [.] 0+ $ | ( [.] \d* [1-9] ) 0+ $ ) }{$1 // ''}sxe
	}
    }
    
    $record->{ages_processed} = 1;
}


sub process_image_ids {
    
    my ($request, $record) = @_;
    
    if ( $request->{block_hash}{extids} )
    {
	$record->{image_no} = generate_identifier('PHP', $record->{image_no});
    }
}


sub process_classification {
    
    my ($request, $record) = @_;
    
    return unless $record->{taxon_rank};
    
    foreach my $u ( qw(NP NC NO NF NG) )
    {
	if ( $record->{taxon_rank} =~ /^\d/ )
	{
	    last if $record->{taxon_rank} >= $UNS_RANK{$u};
	}

	else
	{
	    last if $TAXON_RANK{$record->{taxon_rank}} >= $UNS_RANK{$u};
	}
	
	$record->{$UNS_FIELD{$u}} ||= $UNS_NAME{$u};
	
	if ( $request->{block_hash}{extids} )
	{
	    $record->{$UNS_ID{$u}} ||= generate_identifier('TXN', $u);
	}

	else
	{
	    $record->{$UNS_ID{$u}} ||= $u;
	}
    }
}


# The following hashes map the status codes stored in the opinions table of
# PaleoDB into taxonomic and nomenclatural status codes in compliance with
# Darwin Core.  The third one, %REPORT_ACCEPTED_TAXON, indicates which status
# codes should trigger the "acceptedUsage" and "acceptedUsageID" fields in the
# output.

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


# consolidate_basis ( record )
# 
# Generate consolidated 'basis' values from the '_basis_no' and '_basis'
# fields.

sub consolidate_basis {
    
    my ($request, $record ) = @_;
    
    foreach my $f ( qw(motility life_habit diet taphonomy) )
    {
	my $basis_no = $record->{"${f}_basis_no"};
	my $basis_name = $record->{"${f}_basis"};
	
	$record->{"${f}_basis"} = "$basis_name ($basis_no)" if $basis_no;
    }
}


1;
