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
		 $INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP $INTERVAL_BUFFER $DIV_GLOBAL $DIV_MATRIX);

use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);

use TaxonDefs qw(%RANK_STRING);

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::OccurrenceData PB2::TaxonData PB2::CollectionData PB2::IntervalData);


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
		      'sp.occurrence_no', 'ss.taxon_no as identified_no', 'a.taxon_name as identified_name',
		      'a.orig_no as spec_orig_no',
		      't.rank as identified_rank', 't.status as taxon_status', 't.orig_no',
		      'nm.spelling_reason', 'ns.spelling_reason as accepted_reason',
		      't.spelling_no', 't.accepted_no',
		      'tv.spelling_no as accepted_spelling', 'tv.name as accepted_name', 'tv.rank as accepted_rank',
		      'ei.interval_name as early_interval', 'li.interval_name as late_interval',
		      'o.genus_name', 'o.genus_reso', 'o.subgenus_name', 'o.subgenus_reso',
		      'o.species_name', 'o.species_reso',
		      'o.early_age', 'o.late_age', 'sp.reference_no'],
	  tables => [ 'o', 't', 'nm', 'ns', 'tv', 'ei', 'li', 'o' ] },
	{ set => '*', from => '*', code => \&process_basic_record },
	{ set => '*', code => \&PB2::OccurrenceData::process_occ_ids },
	{ output => 'specimen_no', com_name => 'oid' },
	    "The unique identifier of this specimen in the database",
	{ output => 'record_type', com_name => 'typ', value => $IDP{SPM} },
	    "The type of this object: C<$IDP{SPM}> for a specimen.",
	{ output => 'flags', com_name => 'flg' },
	    "This field will be empty for most records.  A record representing a specimen",
	    "not associated with an occurrence will have an C<N> in this field.  A record",
	    "representing a specimen whose identification is different than its associated",
	    "occurrence will have an C<I> in this field.",
	{ output => 'occurrence_no', com_name => 'qid' },
	    "The identifier of the occurrence, if any, with which this specimen is associated",
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
	    "L<list of values|node:taxa/ecotaph_values>.",
	{ value => 'taphonomy', maps_to => '1.2:taxa:taphonomy' },
	    "Information about the taphonomy of this organism.  Here is a",
	    "L<list of values|node:taxa/ecotaph_values>.",
	{ value => 'etbasis', maps_to => '1.2:taxa:etbasis' },
	    "Annotates the output block C<ecospace>, indicating at which",
	    "taxonomic level each piece of information was entered.",
	{ value => 'pres', maps_to => '1.2:taxa:pres' },
	    "Indicates whether the identification of this specimen is a regular",
	    "taxon, a form taxon, or an ichnotaxon.",
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
	{ include => '1.2:taxa:pres' },
	{ include => '1.2:colls:geo' },
	{ include => '1.2:colls:methods' },
	{ include => '1.2:colls:rem' },
	{ include => '1.2:refs:attr' });
    
    # Rulesets for the various operations defined by this package
    
    $ds->define_ruleset('1.2:specs:specifier' =>
	{ param => 'spec_id', valid => VALID_IDENTIFIER('SPM'), alias => 'id' },
	    "The identifier of the occurrence you wish to retrieve (REQUIRED).",
	    "You may instead use the parameter name C<id>.");
    
    $ds->define_ruleset('1.2:specs:selector' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "List all specimen records known to the database, subject",
	    "to any other parameters you may specify.  This parameter needs",
	    "no value.  Please note that specifying this parameter alone will",
	    "result in a download of over 20 MB of data.",
	{ param => 'spec_id', valid => VALID_IDENTIFIER('SPM'), list => ',', alias => 'id' },
	{ param => 'occ_id', valid => VALID_IDENTIFIER('OCC'), list => ',' },
	    "A comma-separated list of occurrence identifiers.");
    
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
	  error => "you must specify an occurrence identifier, either in the URL or with the 'id' parameter" },
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
	"You can use the following parameters if you wish to retrieve information about",
	"a known list of specimens, occurrences, or collections, or to filter a known list against",
	"other criteria such as location or time.",
	"Only the records which match the other parameters that you specify will be returned.",
	{ allow => '1.2:specs:selector' },
        ">>The following parameters can be used to query for occurrences by a variety of criteria.",
	"Except as noted below, you may use these in any combination.",
	"These same parameters can all be used to select either occurrences, collections, or associated references or taxa.",
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ allow => '1.2:common:select_specs_crmod' },
	{ allow => '1.2:common:select_specs_ent' },
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ allow => '1.2:refs:aux_selector' },
	{ require_any => ['1.2:specs:selector', '1.2:main_selector', '1.2:interval_selector', '1.2:ma_selector',
			  '1.2:common:select_specs_crmod', '1.2:common:select_specs_ent',
			  '1.2:common:select_occs_crmod', '1.2:common:select_occs_crmod',
			  '1.2:refs:aux_selector'] },
	">>The following parameters can be used to further filter the result list.",
	{ allow => '1.2:taxa:occ_list_filter' },
	">>You can use the following parameters to select extra information you wish to retrieve,",
	"and the order in which you wish to get the records:",
	{ allow => '1.2:specs:display' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:specs:measurements' =>
	"You can use the following parameters if you wish to retrieve information about",
	"a known list of specimens, occurrences, or collections, or to filter a known list against",
	"other criteria such as location or time.",
	"Only the records which match the other parameters that you specify will be returned.",
	{ allow => '1.2:specs:selector' },
        ">>The following parameters can be used to query for occurrences by a variety of criteria.",
	"Except as noted below, you may use these in any combination.",
	"These same parameters can all be used to select either occurrences, collections, or associated references or taxa.",
	{ allow => '1.2:main_selector' },
	{ allow => '1.2:interval_selector' },
	{ allow => '1.2:ma_selector' },
	{ allow => '1.2:common:select_specs_crmod' },
	{ allow => '1.2:common:select_specs_ent' },
	{ allow => '1.2:common:select_occs_crmod' },
	{ allow => '1.2:common:select_occs_ent' },
	{ allow => '1.2:refs:aux_selector' },
	{ require_any => ['1.2:specs:selector', '1.2:main_selector', '1.2:interval_selector', '1.2:ma_selector',
			  '1.2:common:select_specs_crmod', '1.2:common:select_specs_ent',
			  '1.2:common:select_occs_crmod', '1.2:common:select_occs_crmod',
			  '1.2:refs:aux_selector'] },
	">>The following parameters can be used to further filter the result list.",
	{ allow => '1.2:taxa:occ_list_filter' },
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
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generateJoinList('c', $request->tables_hash);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $fields
	FROM $SPEC_MATRIX as ss JOIN specimens as sp using (specimen_no)
		LEFT JOIN $OCC_MATRIX as o on o.occurrence_no = ss.occurrence_no and o.latest_ident = 1
		LEFT JOIN $COLL_MATRIX as c on c.collection_no = o.collection_no
		LEFT JOIN authorities as a on a.taxon_no = ss.taxon_no
		$join_list
        WHERE ss.specimen_no = $id and (c.access_level = 0 or o.occurrence_no is null)
	GROUP BY ss.specimen_no";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    # Return an error response if we couldn't retrieve the record.
    
    die "404 Not found\n" unless $request->{main_record};
    
    return 1;
}


# list_specimens ( )
# 
# Query for all relevant information about the specimen(s) matching the
# specified filters.

sub list_specimens {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $tables = $request->tables_hash;
    
    $request->substitute_select( mt => 'ss', cd => 'ss' );
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = $request->generateMainFilters('list', 'c', $tables);
    push @filters, $request->generateOccFilters($tables, 'ss');
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
    # in order to select the proper result set.
    
    my @filters = $request->generateMainFilters('list', 'c', $tables);
    push @filters, $request->generateOccFilters($tables, 'ss');
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
    
    # $request->adjustCoordinates(\$fields);
    # $request->selectPaleoModel(\$fields, $request->tables_hash) if $fields =~ /PALEOCOORDS/;
    
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
    
    $record->{flags} = "N" unless $record->{occurrence_no};
    
    $record->{flags} = "I" if $record->{spec_orig_no} && $record->{orig_no} && 
	$record->{spec_orig_no} ne $record->{orig_no};
    
    # Set the 'preservation' field.
    
    if ( $record->{is_trace} )
    {
	$record->{preservation} = 'ichnotaxon';
    }
    
    elsif ( $record->{is_form} )
    {
	$record->{preservation} = 'form taxon';
    }
    
    # If no taxon name is given for this occurrence, generate it from the
    # occurrence fields.
    
    $request->process_identification($record) unless $record->{orig_no};
    
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
