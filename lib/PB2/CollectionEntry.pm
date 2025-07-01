#  
# CollectionEntry
# 
# This role provides operations for entry and editing of fossil collections.
# 
# Author: Michael McClennen

use strict;

package PB2::CollectionEntry;

use HTTP::Validate qw(:validators);

use TableDefs qw(%TABLE);

use CoreTableDefs;
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);
use HTTP::Validate qw(FLAG_VALUE);
use TableData qw(complete_ruleset add_to_ruleset complete_valueset);
use TaxonDefs qw(%RANK_STRING %TAXON_RANK);

use CollectionEdit;

use MatrixBase qw(initializeBins);
use OccurrenceBase qw(initializeModifiers parseIdentifiedName constructIdentifiedName
		      matchIdentifiedName);

use Moo::Role;

our (@REQUIRES_ROLE) = qw(PB2::Authentication PB2::CommonData PB2::CommonEntry
			  PB2::CollectionData PB2::OccurrenceData PB2::ReferenceData);


# initialize ( )
# 
# This routine is called by the Web::DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # Value sets for specifying data entry options
    
    $ds->define_set('1.2:colls:allowances' =>
	{ insert => '1.2:common:std_allowances' },
	{ value => 'DUPLICATE' },
	    "Allow this operation even if it may lead to a duplicate record in the database.");
    
    $ds->define_set('1.2:occs:allowances' =>
	{ insert => '1.2:common:std_allowances' });
    
    $ds->define_ruleset('1.2:colls:add' =>
	{ optional => 'SPECIAL(show)', valid => '1.2:colls:basic_map' },
	{ optional => 'allow', valid => '1.2:colls:allowances', list => ',' },
	    "Allows the operation to proceed with certain conditions or properties:",
	{ allow => '1.2:special_params' },
	    "^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:colls:update' =>
	{ optional => 'collection_id', valid => VALID_IDENTIFIER('COL'),
	  alias => ['coll_id', 'id', 'oid'] },
	    "The identifier of a collection to update. If this parameter is specified,",
	    "then there should be only a single body record which does not contain a",
	    "collection identifier.",
	{ optional => 'SPECIAL(show)', valid => '1.2:colls:basic_map' },
	{ optional => 'allow', valid => '1.2:colls:allowances', list => ',' },
	    "Allows the operation to proceed with certain conditions or properties:",
	{ allow => '1.2:special_params' },
	    "^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:colls:addupdate_body' =>
	">>The body of this request must be either a single JSON object, or an array of",
	"JSON objects, or else a single record in C<application/x-www-form-urlencoded> format.",
	"The following fields are allowed in each record. If no specific documentation is given",
	"the value must match the corresponding column from the C<B<$TABLE{COLLECTION_DATA}>> table",
	"in the database. Any columns that are required must be given a nonempty value in every new",
	"record, and may not be set to empty or null in an update.",
	{ optional => 'collection_id', valid => VALID_IDENTIFIER('COL'),
	  alias => ['collection_no', 'id', 'coll_id', 'oid'] },
	    "If this field is empty, a record will be inserted into the database",
	    "and a new identifier will be returned. If it is non-empty, it must match",
	    "the identifier of an existing record. That record will be updated.",
	{ allow => '1.2:common:entry_fields' });
    
    $ds->define_ruleset('1.2:colls:delete' =>
	{ param => 'collection_id', valid => VALID_IDENTIFIER('COL'), list => ',',
	  alias => ['collection_no', 'id', 'coll_id', 'oid'] },
	    "The identifier(s) of the record(s) to be deleted. You may specify",
	    "multiple identifiers as a comma-separated list.",
	{ optional => 'allow', valid => '1.2:colls:allowances', list => ',' },
	    "Allow the operation to proceed with certain conditions or properties.",
	{ allow => '1.2:special_params' },
	    "^You can also use any of the L<special parameters|node:special>  with this request");
    
    my $dbh = $ds->get_connection;
    
    complete_ruleset($ds, $dbh, '1.2:colls:addupdate_body', 'COLLECTION_DATA',
	{ max_interval_no => 'IGNORE', min_interval_no => 'IGNORE',
	  reference_no => { optional => 'reference_id', alias => ['reference_no'] },
	  collection_no => 'IGNORE', source_database => 'IGNORE', license => 'IGNORE',
	  coordinate => 'IGNORE', lat => 'IGNORE', lng => 'IGNORE',
	  latlng_precision => 'IGNORE', ma_interval_no => 'IGNORE',
	  paleolng => 'IGNORE', paleolat => 'IGNORE', plate => 'IGNORE',
	  emlperiod_max => 'IGNORE', period_max => 'IGNORE',
	  emlperiod_min => 'IGNORE', period_min => 'IGNORE',
	  emlepoch_max => 'IGNORE', epoch_max => 'IGNORE',
	  emlepoch_min => 'IGNORE', epoch_min => 'IGNORE',
	  emlintage_max => 'IGNORE', intage_max => 'IGNORE',
	  emlintage_min => 'IGNORE', intage_min => 'IGNORE',
	  emllocage_max => 'IGNORE', locage_max => 'IGNORE',
	  emllocage_min => 'IGNORE', locage_min => 'IGNORE',
	  collection_name => { doc => "The name must contain at least one letter, " .
			       "with maximum length %{size}.", note => 'textarea',
			       before => 'research_group' },
	  collection_aka => { note => 'textarea', before => 'research_group' },
	  country => { doc => "The value must be a country name appearing in the C<country_map> " .
		       "table." },
	  latdeg => { doc => "It accepts an integer between 0-89." },
	  lngdeg => { doc => "It accepts an integer between 0-180." },
	  latmin => { doc => "It accepts an integer between 0-59." },
	  latsec => { doc => "It accepts an integer between 0-59." },
	  lngmin => { doc => "It accepts an integer between 0-59." },
	  lngsec => { doc => "It accepts an integer between 0-59." },
	  latdec => { doc => "It is not allowed for this column to have a non-empty value together " .
		      "with either C<latmin> or C<latsec>. It accepts a string of at most 10 digits." },
	  lngdec => { doc => "It is not allowed for this column to have a non-empty value together " .
		      "with either C<lngmin> or C<lngsec>. It accepts a string of at most 10 digits." },
	  direct_ma => { alias => 'direct_ma_value',
			 doc => "It accepts an unsigned decimal number of at most 8 digits." },
	  direct_ma_error => { doc => "It accepts an unsigned decimal number of at most 8 digits." },
	  max_ma => { alias => 'max_ma_value',
		      doc => "It accepts an unsigned decimal number of at most 8 digits." },
	  max_ma_error => { doc => "It accepts an unsigned decimal number of at most 8 digits." },
	  min_ma => { alias => 'min_ma_value',
		      doc => "It accepts an unsigned decimal number of at most 8 digits." },
	  min_ma_error => { doc => "It accepts an unsigned decimal number of at most 8 digits." },
	  geogcomments => { note => 'textarea' },
	  stratcomments => { note => 'textarea' },
	  lithdescript => { note => 'textarea' },
	  lithadj => { alias => 'lithadj1' },
	  lithification => { alias => 'lithification1' },
	  minor_lithology => { alias => 'minor_lithology1' },
	  fossilsfrom1 => { valid => BOOLEAN_VALUE, doc => "It accepts a value from the following " .
			    "list: 'true','false','1','0','yes','no'." },
	  fossilsfrom2 => { valid => BOOLEAN_VALUE, doc => "It accepts a value from the following " .
			    "list: 'true','false','1','0','yes','no'." },
	  geology_comments => { note => 'textarea' },
	  assembl_comps => { alias => 'size_classes' },
	  pres_mode => { note => 'textarea' },
	  coll_meth => { note => 'textarea', alias => 'collection_methods' },
	  common_body_parts => { note => 'textarea' },
	  rare_body_parts => { note => 'textarea' },
	  component_comments => { note => 'textarea' },
	  collection_comments => { note => 'textarea' },
	  taxonomy_comments => { note => 'textarea' },
	  access_level => { before => 'research_group' },
	  release_date => { doc => "It accepts either 'immediate', or else a string of the form " .
			    "'n months' or 'n years' where n is a digit. The maximum is 3 years.",
			    before => 'research_group' },
	});
    
    add_to_ruleset($ds, '1.2:colls:addupdate_body',
	{ optional => 'reference_add', valid => VALID_IDENTIFIER('REF'), list => ',',
	  before => 'country', },
	    "One or more reference identifiers to add as secondary references for this collection.",
	    "You may specify more than one, as a comma-separated list.",
	{ optional => 'reference_delete', valid => VALID_IDENTIFIER('REF'), list => ',',
	  before => 'country', },
	    "One or more reference identifiers to remove as secondary references for this collection.",
	    "You may specify more than one, as a comma-separated list. A reference will B<not>",
	    "be removed if it is the primary reference for this collection, or if it is the source",
	    "for any occurrences or specimens associated with this collection.",
	{ optional => 'max_interval', alias => 'early_interval',
	  valid => [VALID_IDENTIFIER('INT'), ANY_VALUE], before => 'zone_type' },
	    "This parameter sets the value of C<max_interval_no> in the C<collections> table.",
	    "It accepts an interval name or number, or an external identifier of type 'int'.",
	{ optional => 'min_interval', alias => 'late_interval',
	  valid => [VALID_IDENTIFIER('INT'), ANY_VALUE], before => 'zone_type' },
	    "This parameter sets the value of C<min_interval_no> in the C<collections> table.",
	    "It accepts an interval name or number, or an external identifier of type 'int'.",
	    "If this collection is associated with a single interval, leave this field null.");
    
    $ds->define_ruleset('1.2:occs:update' =>
	{ optional => 'collection_id', valid => VALID_IDENTIFIER('COL'),
	  alias => ['coll_id', 'id', 'oid'] },
	    "The identifier of a collection whose occurrences are being updated. If this",
	    "parameter is specified, then the body record(s) should not contain a collection",
	    "identifier.",
	{ optional => 'SPECIAL(show)', valid => '1.2:occs:display_map' },
	{ optional => 'allow', valid => '1.2:colls:allowances', list => ',' },
	    "Allows the operation to proceed with certain conditions or properties:",
	{ allow => '1.2:special_params' },
	    "^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:addupdate_body' =>
	">>The body of this request must be either a single JSON object, or an array of",
	"JSON objects, or else a single record in C<application/x-www-form-urlencoded> format.",
	"The following fields are allowed in each record.",
	"Any columns that are B<required> must be given a nonempty value in every new",
	"record, and may not be set to empty or null in an update.",
	{ param => 'collection_id', valid => VALID_IDENTIFIER('COL'),
	  alias => ['collection_no', 'cid'] },
	    "This field is required for a new occurrence, specifying the collection",
	    "with which that occurrence will be associated. If specified for an existing",
	    "occurrence, it must match that occurrence's collection.",
	{ param => 'occurrence_id', valid => VALID_IDENTIFIER('OCC'),
	  alias => ['occurrence_no', 'occ_id', 'oid'] },
	    "This field B<must> be included when submitting occurrences. If its value is empty,",
	    "an occurrence record will be inserted into the database",
	    "and a new identifier will be returned. If it is non-empty, it must match",
	    "the identifier of an existing record. That record will be updated.",
	{ param => 'reid_id', valid => VALID_IDENTIFIER('REI'),
	  alias => ['reid_no', 'reid'] },
	    "If this field occurs with an empty value, and if 'occurrence_id' is not",
	    "empty, a reidentification record will be inserted into the database and a",
	    "new identifier will be returned. If it is non-empty, it must match the",
	    "identifier of an existing record. That record will be updated.",
	{ allow => '1.2:common:entry_fields' },
	{ optional => '_delete', valid => ANY_VALUE },
	    "If this field is specified with a non-empty value, this occurrence or",
	    "reidentification will be",
	    "deleted. The deletion will only happen if the user is the enterer or",
	    "authorizer of the occurrence or its associated collection.",
	{ optional => 'identified_name', valid => ANY_VALUE, alias => 'taxon_name' },
	    "The taxonomic name identifying this occurrence, optionally with modifiers.",
	    "This field is required.",
	{ optional => 'taxon_no', valid => VALID_IDENTIFIER('TID'),
	  alias => ['taxon_id'] },
	    "This field should only be specified when choosing between homonyms.");
    
    complete_ruleset($ds, $dbh, '1.2:occs:addupdate_body', 'OCCURRENCE_DATA',
		     { reference_no => { alias => ['reference_id'] },
		       genus_reso => 'IGNORE', genus_name => 'IGNORE',
		       subgenus_reso => 'IGNORE', subgenus_name => 'IGNORE',
		       species_reso => 'IGNORE', species_name => 'IGNORE',
		       subspecies_reso => 'IGNORE', subspecies_name => 'IGNORE',
		       plant_organ2 => 'IGNORE' });
    
    # add_to_ruleset($ds, '1.2:occs:addupdate_body', 
    # 		   { optional => 'identified_name', before => 'abund_value' },
    # 		   "This parameter is parsed in order to set the value for C<genus_reso>,",
    # 		   "C<genus_name>, etc. in the C<$TABLE{OCCURRENCE_DATA}> or",
    # 		   "C<$TABLE{REID_DATA}> table as appropriate.");

    # Output block for the 'checknames' operation:

    $ds->define_block('1.2:occs:checknames' =>
	{ select => ['a.taxon_no as matched_no', 'a.taxon_name as matched_name',
		     'a.taxon_rank as matched_rank', 't.status as taxon_status',
		     't.orig_no', 't.spelling_no', 't.accepted_no',
		     'tv.name as accepted_name', 'tv.rank as accepted_rank',
		     'nm.spelling_reason', 'ns.spelling_reason as accepted_reason',
		     'ph.phylum', 'ph.class', 'ph.order', 'ph.family',
		     'if(a.ref_is_authority <> \'\', r.author1last, a.author1last) as r_al1',
		     'if(a.ref_is_authority <> \'\', r.author2last, a.author2last) as r_al2',
		     'if(a.ref_is_authority <> \'\', r.otherauthors, a.otherauthors) as r_oa',
		     'if(a.ref_is_authority <> \'\', r.pubyr, a.pubyr) as r_pubyr',
		     'v.is_trace', 'v.is_form'] },
	{ set => '*', from => '*', code => \&process_checknames_record },
	{ output => 'record_type', com_name => 'typ', value => $IDP{TXN} },
	    "The type of this object: C<$IDP{TXN}> for a taxonomic name.",
        { output => '_label', com_name => 'rlb' },
	    "For data entry operations, this field will report the record",
	    "label value, if any, that was submitted with each record.",
	{ output => 'error', com_name => 'err' },
	    "If the submitted name is not valid, this field will contain an error message",
	    "describing the reason.",
	{ output => 'classification', com_name => 'cof' },
	    "The class, order, and family in which this name is located. The value of this",
	    "field contains such of those terms as have non-empty values, separated by dashes.",
	{ output => 'identified_name', com_name => 'idn' },
	    "The taxonomic name that was submitted, possibly with syntax errors corrected.",
	{ set => 'identified_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb' },
	{ output => 'identified_rank', com_name => 'idr' },
	    "The rank of the taxonomic name that was submitted.",
	{ output => 'matched_no', com_name => 'mid' },
	    "The unique identifier of a taxonomic name matching the submitted name.",
	{ output => 'matched_name', com_name => 'mtn' },
	    "The matching name.",
	{ output => 'matched_rank', com_name => 'mtr', data_type => 'mix' },
	    "The taxonomic rank of the matching name.",
	{ set => 'matched_rank', lookup => \%TAXON_RANK, if_vocab => 'com' },
        { set => 'matched_attr', from => '*', code => \&PB2::ReferenceData::format_authors },
	{ output => 'matched_attr', com_name => 'atr' },
	    "The attribution of the matching name: author(s) and year.",
	{ output => 'difference', com_name => 'tdf', not_block => 'acconly' },
	    "If the matched name is different from the accepted name, this field gives",
	    "the reason why.  This field will be present if, for example, the matched name",
	    "is a junior synonym or nomen dubium, or if the species has been recombined, or",
	    "if the identification is misspelled.",
	{ output => 'accepted_no', com_name => 'tid', if_field => 'accepted_no' },
	    "The unique identifier of the accepted name corresponding to the matched name.",
	{ output => 'accepted_name', com_name => 'tna', if_field => 'accepted_no' },
	    "The accepted taxonomic name corresponding to the matched name.",
	{ output => 'accepted_rank', com_name => 'rnk', if_field => 'accepted_no', 
	  data_type => 'mix' },
	    "The taxonomic rank of the accepted name.  This may be different from the",
	    "identified rank if the identified name is a nomen dubium or otherwise invalid,",
	    "or if the identified name has not been fully entered into the taxonomic hierarchy",
	    "of this database.",
	{ set => 'accepted_rank', lookup => \%RANK_STRING, if_vocab => 'pbdb' },
	{ output => 'is_type_locality', pbdb_name => 'type_locality', com_name => 'tlc' },
	    "The value of this field will be C<B<yes>> if the collection to which this",
	    "occurrence belongs is the type locality for the matched name. It will be",
	    "C<B<possible>> if the matched name does not currently have a type locality.",
	{ output => 'flags', com_name => 'flg' },
	    "This field will be empty for most records.  Otherwise, it will contain one or more",
	    "of the following letters:", "=over",
	    "=item I", "This identification is an ichnotaxon",
	    "=item F", "This identification is a form taxon");
    
    # Rulesets for the 'checknames' operation:
    
    $ds->define_ruleset('1.2:occs:checknames' => 
	{ optional => 'name', valid => ANY_VALUE },
	    "The taxonomic name (optionally with modifiers) to be checked.",
	{ optional => 'collection_id', valid => VALID_IDENTIFIER('COL'), 
	  alias => ['collection_no', 'coll_id'] },
	    "If you are checking the name of an occurrence or reidentification, you may",
	    "optionally specify the corresponding collection identifier.",
	{ optional => 'loose', valid => FLAG_VALUE },
	    "If this parameter is specified, bad capitalization will be accepted.",
	{ allow => '1.2:special_params' },
	    "^You can use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:occs:checknames_body' =>
	{ param => 'name', valid => ANY_VALUE },
	    "The name to be checked.",
	{ optional => '_label', valid => ANY_VALUE },
	    "An optional label which will be associated with any return records matching",
	    "this input record.",
	{ optional => 'collection_id', valid => VALID_IDENTIFIER('COL'),
	  alias => 'collection_no' },
	    "If you are checking the name of an occurrence or reidentification, you may",
	    "optionally specify the corresponding collection identifier. This allows",
	    "the response record to properly report whether this is the type location of",
	    "the matched taxonomic name.");
    
    # Initialize the libraries we depend on.
    
    MatrixBase::initializeBins($ds);
    OccurrenceBase::initializeModifiers($dbh);
    
    my $a = 1;	# we can stop here when debugging
}


# addupdate_colls ( request, arg )
# 
# Execute one or more database operations involving bibliographic references. The request body is
# decoded into a list of one or more records, each of which specifies an insert, update, replace,
# delete, or auxiliary operation. These are all processed together as part of a single transaction.

my $addupdate_config = { class => 'CollectionEdit',
			 main_table => 'COLLECTION_DATA',
			 url_ruleset => '1.2:colls:addupdate', 
			 body_ruleset => '1.2:refs:addupdate_body',
			 primary_identifier => 'collection_id',
			 record_selector => [ { ruleset => '1.2:abc:def', table => 'AUXILIARY_DATA',
						keys => 'abc, def, ghi' } ]
		       };

sub addupdate_colls {
    
    my ($request, $operation) = @_;
    
    $operation ||= '';
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single record only.
    
    my $perms = $request->require_authentication('COLLECTION_DATA');
    
    my ($allowances, $main_params) = $request->parse_main_params('1.2:colls:addupdate',
								 'collection_id');
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->parse_body_records($main_params, '1.2:colls:addupdate_body');
		# ['REID_DATA', '1.2:reids:addupdate_body', 'reid_id'],
		# ['OCCURRENCE_DATA', '1.2:occs:addupdate_body', 'occurrence_id'],
		# ['COLLECTION_DATA', '1.2:colls:addupdate_body', 'collection_id', 'collection_name'],
		# 'NO_MATCH');
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad data");
    }
    
    # If we get here without any errors being detected so far, create a new EditTransaction object to
    # handle this operation.
    
    my $edt = CollectionEdit->new($request, { permission => $perms, 
					      table => 'COLLECTION_DATA', 
					      allows => $allowances } );
    
    # Now go through the records and handle each one in turn. This will check every record and
    # queue them up for the specified operation.
    
    foreach my $r (@records)
    {
	# If the field '_delete' was specified with a true value, delete the record.
	
	if ( $r->{_delete} )
	{
	    # if ( exists $r->{reid_id} )
	    # {
	    # 	$edt->delete_record('REID_DATA', $r);
	    # }

	    # elsif ( exists $r->{occurrence_id} )
	    # {
	    # 	$edt->delete_record('OCCURRENCE_DATA', $r);
	    # }
	    
	    # else
	    # {
		$edt->delete_record('COLLECTION_DATA', $r);
	    # }
	}
	
	# # Otherwise, if 'reid_id' was specified then insert or update this
	# # record in the REID_DATA table.
	
	# elsif ( exists $r->{reid_id} )
	# {
	#     $edt->insert_update_record('REID_DATA', $r);
	# }
	
	# # Otherwise, if 'occurrence_id' was specified then insert or update this
	# # record in the OCCURRENCE_DATA table.
	
	# elsif ( exists $r->{occurrence_id} )
	# {
	#     $edt->insert_update_record('OCCURRENCE_DATA', $r);
	# }
	
	# Otherwise, if this operation was called as 'colls/add' then insert
	# this record in the COLLECTION_DATA table.
	
	elsif ( $operation eq 'insert' )
	{
	    $edt->insert_record('COLLECTION_DATA', $r);
	}
	
	# If this operation was called as 'colls/update' then update this record
	# in the COLLECTION_DATA table.
	
	elsif ( $operation eq 'update' )
	{
	    $edt->update_record('COLLECTION_DATA', $r);
	}
	
	# elsif ( $operation eq 'replace' )
	# {
	#     $edt->replace_record('COLLECTION_DATA', $r);
	# }
	
	# If this operation was called as 'colls/delete' then delete this record
	# from the COLLECTION_DATA table.
	
	elsif ( $operation eq 'delete' )
	{
	    $edt->delete_record('COLLECTION_DATA', $r);
	}
	
	# If we have no operation information available, insert or update this
	# record in the COLLECTION_DATA table based on the presence or absence
	# of a value for 'collection_id'.
	
	else
	{
	    $edt->insert_update_record('COLLECTION_DATA', $r);
	}
    }
    
    # If no errors have been detected so far, execute the queued actions inside
    # a database transaction. If any errors occur during that process, the
    # transaction will be automatically rolled back unless the NOT_FOUND or
    # PROCEED allowance was given. Otherwise, it will be automatically
    # committed.
    
    $edt->commit;
    
    # Now handle any errors or warnings that may have been generated.
    
    $request->collect_edt_warnings($edt);
    $request->collect_edt_errors($edt);
    
    if ( $edt->fatals )
    {
    	die $request->exception(400, "Bad request");
    }
    
    unless ( $request->has_block('none') )
    {
	my @deleted_keys = $edt->deleted_keys();
	
	$request->list_deleted_colls($dbh, \@deleted_keys, $edt->key_labels('COLLECTION_DATA'))
	    if @deleted_keys;
	
	my @existing_keys = ($edt->inserted_keys('COLLECTION_DATA'),
			       $edt->updated_keys('COLLECTION_DATA'));
			       
	$request->list_updated_colls($dbh, \@existing_keys, $edt->key_labels('COLLECTION_DATA'))
	    if @existing_keys;
    }
}


sub list_updated_colls {
    
    my ($request, $dbh, $coll_ids, $key_labels) = @_;
    
    # Get a list of the collection_no values to return.
    
    my @ids = grep { $_ > 0 } $coll_ids->@*;
    
    return unless @ids;
    
    my $id_list = join(',', @ids);
    
    my $filter_string = "collection_no in ($id_list)";
    
    # Fetch the collection records.
    
    $request->extid_check;
    
    $request->substitute_select( mt => 'c', cd => 'cc' );
    
    my $fields = $request->select_string;
    my $tables = $request->tables_hash;
    
    $request->adjustCoordinates(\$fields);
    $request->selectPaleoModel(\$fields, $request->tables_hash) if $fields =~ /PALEOCOORDS/ ||
	$request->{my_plate_model};
    
    if ( $tables->{tf} )
    {
	$fields =~ s{ c.n_occs }{count(distinct o.occurrence_no) as n_occs}xs;
    }
    
    my $base_joins = $request->generateJoinList('c', $tables);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    $request->{main_sql} = "
	SELECT $calc $fields
	FROM coll_matrix as c JOIN collections as cc using (collection_no)
		LEFT JOIN secondary_refs as sr using (collection_no)
		$base_joins
        WHERE $filter_string
	GROUP BY c.collection_no";
    
    $request->debug_line("$request->{main_sql}\n\n") if $request->debug;
    
    # Then return the result.

    my $result = $dbh->selectall_arrayref($request->{main_sql}, { Slice => { } });
    
    foreach my $r ( @$result )
    {
	if ( $r->{collection_no} && $key_labels->{$r->{collection_no}} )
	{
	    $r->{_label} = $key_labels->{$r->{collection_no}};
	}
    }
    
    $request->add_result($result);
}


sub update_occs {
    
    my ($request, $operation) = @_;
    
    $operation ||= '';
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single record only.
    
    my $perms = $request->require_authentication('COLLECTION_DATA');
    
    my ($allowances, $main_params) = $request->parse_main_params('1.2:occs:addupdate',
								 'collection_id');
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->parse_body_records($main_params, '1.2:occs:addupdate_body');
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad data");
    }
    
    # If we get here without any errors being detected so far, create a new EditTransaction object to
    # handle this operation.
    
    my $edt = CollectionEdit->new($request, { permission => $perms, 
					      table => 'OCCURRENCE_DATA', 
					      allows => $allowances } );
    
    # Now go through the records and extract the occurrence numbers and
    # reidentification numbers. This will allow the code in CollectionEdit.pm to
    # fetch the existing data for those occurrences and reids in order to
    # prevent duplicates and for other checks. We stringify the ids because at
    # this point they may be objects generated by external identifiers.
    
    my (%occurrence_nos, %reid_nos);
    
    foreach my $r (@records)
    {
	if ( $r->{occurrence_id} )
	{
	    $occurrence_nos{"$r->{occurrence_id}"} = 1;
	}
	
	if ( $r->{reid_id} )
	{
	    $reid_nos{"$r->{reid_id}"} = 1;
	}
    }
    
    $edt->initialize_occs(\%occurrence_nos, \%reid_nos);
    
    # Then go through the records again and handle each one in turn. This will
    # check every record and queue them up for the specified operation.
    
    foreach my $r (@records)
    {
	# If the field '_delete' was specified with a true value, delete the record.
	
	if ( $r->{_delete} )
	{
	    if ( exists $r->{reid_id} )
	    {
		$edt->delete_record('REID_DATA', $r);
	    }
	    
	    elsif ( exists $r->{occurrence_id} )
	    {
		$edt->delete_record('OCCURRENCE_DATA', $r);
	    }
	}
	
	# Otherwise, if 'reid_id' was specified then insert or update this
	# record in the REID_DATA table.
	
	elsif ( exists $r->{reid_id} )
	{
	    $edt->insert_update_record('REID_DATA', $r);
	}
	
	# Otherwise, if 'occurrence_id' was specified then insert or update this
	# record in the OCCURRENCE_DATA table.
	
	elsif ( exists $r->{occurrence_id} )
	{
	    $edt->insert_update_record('OCCURRENCE_DATA', $r);
	}
	
	else
	{
	    $request->add_error("E_BAD_DATA: a body record did not contain either 'reid_id' or 'occurrence_id'");
	    die $request->exception(400, "Bad data");
	}
    }
    
    # If no errors have been detected so far, execute the queued actions inside
    # a database transaction. If any errors occur during that process, the
    # transaction will be automatically rolled back unless the NOT_FOUND or
    # PROCEED allowance was given. Otherwise, it will be automatically
    # committed.
    
    $edt->commit;
    
    # Now handle any errors or warnings that may have been generated.
    
    $request->collect_edt_warnings($edt);
    $request->collect_edt_errors($edt);
    
    if ( $edt->fatals )
    {
    	die $request->exception(400, "Bad request");
    }
    
    unless ( $request->has_block('none') )
    {
	my @deleted_occs = $edt->deleted_keys('OCCURRENCE_DATA');
	
	$request->list_deleted_items('occurrence_no', \@deleted_occs,
				     $edt->key_labels('OCCURRENCE_DATA'))
	    if @deleted_occs;
	
	my @deleted_reids = $edt->deleted_keys('REID_DATA');
	
	$request->list_deleted_items('reid_no', \@deleted_reids,
				     $edt->key_labels('REID_DATA'))
	    if @deleted_reids;
	
	if ( $request->has_block('all') )
	{
	    my @coll_nos = $edt->get_attr_keys('show_collection');

	    $request->list_updated_occs($dbh, \@coll_nos);
	}

	else
	{
	    my @occ_nos = ($edt->inserted_keys('OCCURRENCE_DATA'),
			   $edt->updated_keys('OCCURRENCE_DATA'));
	    
	    my @reid_nos = ($edt->inserted_keys('REID_DATA'),
			    $edt->updated_keys('REID_DATA'));
	    
	    my $occ_labels = $edt->key_labels('OCCURRENCE_DATA');
	    my $reid_labels = $edt->key_labels('REID_DATA');
	    
	    $request->list_updated_occs($dbh, undef, \@occ_nos, \@reid_nos, $occ_labels, $reid_labels)
		if @occ_nos || @reid_nos;
	}
    }
}


sub list_updated_occs {

    my ($request, $dbh, $coll_nos, $occ_nos, $reid_nos, $occ_labels, $reid_labels) = @_;
    
    $request->extid_check;
    
    my $tables = $request->tables_hash;
    
    $request->substitute_select( cd => 'oc' );
    
    my ($access_filter) = $request->generateAccessFilter('cc', $tables, 1);
    
    my $occ_fields = $request->select_string;
    my $reid_fields = $occ_fields;
    
    $occ_fields =~ s/oc.reid_no/0 as reid_no/;
    
    $reid_fields =~ s/oc[.]/re./g;
    $reid_fields =~ s/re[.](abund\w+)/'' as $1/g;
    $reid_fields =~ s/re[.]plant_organ2/'' as plant_organ2/;
    $reid_fields =~ s/sc[.]n_specs/0 as n_specs/;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Create a join list of all the tables we will need.
    
    my $other_joins = <<END_JOINS;
left join $TABLE{AUTHORITY_DATA} as a using (taxon_no)
	    left join $TABLE{TAXON_NAMES} as nm using (taxon_no)
	    left join $TABLE{TAXON_TREES} as t on t.orig_no = a.orig_no
	    left join $TABLE{TAXON_TREES} as tv on tv.orig_no = t.accepted_no
	    left join $TABLE{TAXON_NAMES} as ns on ns.taxon_no = tv.spelling_no
	    left join $TABLE{TAXON_ATTRS} as v on v.orig_no = t.orig_no
	    left join $TABLE{TAXON_INTS} as ph on ph.ints_no = t.ints_no
END_JOINS
    
    my $result;    
    
    # If we were given a list of collection_no values, select all occurrences
    # and reidentifications in those collections (typically there will be only one).
    
    if ( $coll_nos  )
    {
	push @$coll_nos, 0 unless @$coll_nos;
	
    	my $coll_list = join("','", @$coll_nos);
	
	my $filter_string = "cc.collection_no in ('$coll_list') and $access_filter";
	
	$request->{main_sql} = "
	SELECT $calc $occ_fields, if(oc.reid_no > 0, '', 1) as latest_ident
	FROM $TABLE{OCCURRENCE_DATA} as oc join $TABLE{COLLECTION_DATA} as cc using (collection_no)
	    left join $TABLE{REFERENCE_DATA} as r on r.reference_no = oc.reference_no
	    left join (SELECT occurrence_no, count(*) as n_specs FROM $TABLE{SPECIMEN_DATA}
		       GROUP BY occurrence_no) as sc on sc.occurrence_no = oc.occurrence_no
	    $other_joins
        WHERE $filter_string
        UNION ALL
	SELECT $reid_fields, if(re.most_recent = 'YES', 1, '') as latest_ident
	FROM $TABLE{REID_DATA} as re join $TABLE{COLLECTION_DATA} as cc using (collection_no)
	    left join $TABLE{REFERENCE_DATA} as r on r.reference_no = re.reference_no
	    $other_joins
	WHERE $filter_string
	ORDER BY collection_no, occurrence_no, reid_no
	$limit";
    
	$request->{ds}->debug_line("$request->{main_sql}\n") if $request->debug;
	
	$result = $dbh->selectall_arrayref($request->{main_sql}, {Slice => { }});
    }

    # Otherwise, select all occurrences and reidentifications specified in the
    # given lists.

    else
    {
	push @$occ_nos, 0 unless @$occ_nos;

	my $occ_list = join("','", @$occ_nos);

	my $occ_filter = "oc.occurrence_no in ('$occ_list') and $access_filter";
	
	push @$reid_nos, 0 unless @$reid_nos;

	my $reid_list = join("','", @$reid_nos);
	
	my $reid_filter = "re.reid_no in ('$reid_list') and $access_filter";

	$request->{main_sql} = "
	SELECT $calc $occ_fields, if(oc.reid_no > 0, '', 1) as latest_ident
	FROM $TABLE{OCCURRENCE_DATA} as oc join $TABLE{COLLECTION_DATA} as cc using (collection_no)
	    left join $TABLE{REFERENCE_DATA} as r on r.reference_no = oc.reference_no
	    left join (SELECT occurrence_no, count(*) as n_specs FROM $TABLE{SPECIMEN_DATA}
		       GROUP BY occurrence_no) as sc on sc.occurrence_no = oc.occurrence_no
	    $other_joins
        WHERE $occ_filter
        UNION ALL
	SELECT $reid_fields, if(re.most_recent = 'YES', 1, '') as latest_ident
	FROM $TABLE{REID_DATA} as re join $TABLE{COLLECTION_DATA} as cc using (collection_no)
	    left join $TABLE{REFERENCE_DATA} as r on r.reference_no = re.reference_no
	    $other_joins
	WHERE $reid_filter
	ORDER BY collection_no, occurrence_no, reid_no
	$limit";
    
	$request->{ds}->debug_line("$request->{main_sql}\n") if $request->debug;
	
	$result = $dbh->selectall_arrayref($request->{main_sql}, {Slice => { }});
    }	

    # If we were given key labels, apply them.

    foreach my $r ( @$result )
    {
	if ( $reid_labels && $r->{reid_no} )
	{
	    $r->{_label} = $reid_labels->{$r->{reid_no}};
	}

	if ( $occ_labels && ! $r->{reid_no} )
	{
	    $r->{_label} = $occ_labels->{$r->{occurrence_no}};
	}
    }
    
    # If show=edit was specified, return the results in the order retrieved,
    # which is sorted by collection_no, then occurrence_no, then reid_no.
    
    if ( $request->has_block('edit') )
    {
	$request->list_result($result);
    }
    
    # Otherwise, group them by class, order, and family.
    
    else
    {
	$request->list_result($request->process_occs_for_display($result));
    }
}


sub check_taxonomic_names {

    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    
    my $tables = $request->tables_hash;
    
    $request->substitute_select( cd => 'oc' );
    
    my $fields = $request->select_string;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether to generate external identifiers.
    
    $request->strict_check;
    $request->extid_check;

    # Parse the main parameters.

    my $main_params = { name => $request->clean_param('name'),
			collection_id => $request->clean_param('collection_id'),
			loose => $request->clean_param('loose') };
    
    if ( Dancer::request->method eq 'GET' && ! $main_params->{name} )
    {
	$request->add_error("E_PARAM: you must specify a value for 'name'");
	die $request->exception(400, "Bad request");
    }

    elsif ( Dancer::request->method =~ /^POST/ && $main_params->{name} )
    {
	$request->add_error("E_PARAM: you must not specify a value for 'name' in a 'POST' request");
	die $request->exception(400, "Bad request");
    }
    
    # Decode the body and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->parse_body_records($main_params, '1.2:occs:checknames_body');
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad data");
    }
    
    # Iterate through the submitted records and check each specified name. For
    # each input record, we will add one or more response records to @final. All
    # of the response records for a given input record will have the same label.

    my ($identified_name, @matches, $sql, $result, @final);
    my $debug_out; $debug_out = $request if $request->debug;
    my $record_count = 0;
    
    foreach my $r (@records)
    {
	# If no label was given, use the index of the record in the request
	# body starting with 1.
	
	my $label = $r->{_label} || ("#" . ++$record_count);
	
	my $loose = $r->{loose} ? 1 : undef;
	
	my $submitted_name = $r->{name};
	
	# Parse the submitted name.
	
	my ($genus_name, $genus_reso, $subgenus_name, $subgenus_reso,
	    $species_name, $species_reso, $subspecies_name, $subspecies_reso) =
		parseIdentifiedName($submitted_name, { debug_out => ( $request->debug ? $request :
								      undef ),
						       loose => $loose });
	
	# If the submitted name threw a syntax error, generate a result record
	# reflecting that and go on to the next input record.
	
	if ( ref $genus_name )
	{
	    push @final, { _label => $label,
			   identified_name => $submitted_name,
			   matched_no => 0,
			   error => $genus_name->{error} };
	    next;
	}
	
	# If the submitted name has an informal genus, there won't be any
	# matches. So add a result record reflecting that.
	
	if ( $genus_reso && $genus_reso eq 'informal' )
	{
	    push @final, { _label => $label,
			   identified_name => $submitted_name,
			   matched_no => 0 };
	    next;
	}
	
	# Otherwise, put the name together again from the individual components.
	# This will make sure that it is syntactically correct, since the parse
	# routine corrects some errors.
	
	my $cleaned_name = constructIdentifiedName($genus_name, $genus_reso,
						   $subgenus_name, $subgenus_reso,
						   $species_name, $species_reso,
						   $subspecies_name, $subspecies_reso);
	
	my $cleaned_rank = 'genus';
	
	# If the submitted name has an informal subgenus, leave it out when
	# querying for matches.
	
	if ( $subgenus_reso && $subgenus_reso eq 'informal' )
	{
	    $subgenus_name = '';
	}
	
	elsif ( $subgenus_name )
	{
	    $cleaned_rank = 'subgenus';
	}
	
	# If the submitted name has an informal species, or more commonly 'sp.'
	# or 'indet.', leave it out when querying for matches and also leave out
	# any subspecies that may have been specified. This will often
	# result in a match at the genus level.
	
	if ( $species_reso && $species_reso eq 'informal' ||
	     $species_name && $species_name =~ /[.]$/ )
	{
	    $species_name = '';
	    $subspecies_name = '';
	}
	
	elsif ( $species_name )
	{
	    $cleaned_rank = 'species';
	}
	
	# Otherwise, if the submitted name has an informal subspecies, or more
	# commonly 'ssp.', leave it out when querying for matches. This will
	# often result in a match at the species level.
	
	elsif ( $subspecies_reso && $subspecies_reso eq 'informal' ||
		$subspecies_name && $subspecies_name =~ /[.]$/ )
	{
	    $subspecies_name = '';
	}

	elsif ( $subspecies_name )
	{
	    $cleaned_rank = 'subspecies';
	}
	
	# Do a complex query for matches in the authorities table.
	
	@matches = matchIdentifiedName($dbh, $debug_out, $genus_name, $subgenus_name,
				       $species_name, $subspecies_name);
	
	# If we find any, select the information necessary to put together full
	# response records.
	
	if ( @matches )
	{
	    my $taxon_list = join("','", @matches);
	    
	    $sql = "SELECT $fields
		FROM $TABLE{AUTHORITY_DATA} as a left join $TABLE{TAXON_NAMES} as nm using (taxon_no)
		    left join $TABLE{REFERENCE_DATA} as r using (reference_no)
		    left join $TABLE{TAXON_TREES} as t on t.orig_no = a.orig_no
		    left join $TABLE{TAXON_TREES} as tv on tv.orig_no = t.accepted_no
		    left join $TABLE{TAXON_NAMES} as ns on ns.taxon_no = tv.spelling_no
		    left join $TABLE{TAXON_ATTRS} as v on v.orig_no = t.orig_no
		    left join $TABLE{TAXON_INTS} as ph on ph.ints_no = t.ints_no
		WHERE a.taxon_no in ('$taxon_list')";
		
	    $request->debug_line("$sql\n") if $request->debug;
	    
	    $result = $dbh->selectall_arrayref($sql, { Slice => {} });
	}
	
	# If there are no matches, generate a single response record with
	# matched_no = 0.
	
	else
	{
	    $result = [{ matched_no => 0 }];
	}
	
	# Fill in the identified name, identified rank, and label on each of the
	# response records, and add them to the final list.
	
	foreach my $m ( @$result )
	{
	    $m->{identified_name} = $cleaned_name;
	    $m->{identified_rank} = $cleaned_rank;
	    $m->{_label} = $label;
	    
	    push @final, $m;
	}
    }
    
    $request->list_result(\@final);
}


sub process_checknames_record {

    my ($request, $record) = @_;
    
    # If this is an error record, do nothing.
    
    return if $record->{error};
    
    # Set the flags as appropriate.
    
    if ( $record->{is_trace} || $record->{is_form} )
    {
	$record->{flags} ||= '';
	$record->{flags} .= 'I' if $record->{is_trace};
	$record->{flags} .= 'F' if $record->{is_form};
    }
    
    # Check the type locality.
    
    if ( $record->{collection_no} && $record->{type_locality} &&
	 $record->{collection_no} eq $record->{type_locality} )
    {
	$record->{is_type_locality} = 'yes';
    }
    
    elsif ( $record->{identified_name} =~ /n[.] (gen|subgen|sp|ssp)[.]/ )
    {
	$record->{is_type_locality} = 'possible';
    }
    
    # Now generate the 'difference' field if the accepted name and identified
    # name are different.
    
    $request->PB2::TaxonData::process_difference($record);
    
    # Generate the class-order-family string.
    
    my @cof;
    push @cof, $record->{class} if $record->{class};
    push @cof, $record->{order} if $record->{order};
    push @cof, $record->{family} if $record->{family};
    push @cof, $record->{phylum} if $record->{phylum} && ! @cof;
    push @cof, "Unclassified" unless @cof;
    
    $record->{classification} = join(' - ', @cof);
    
    # Generate external ids if requested.

    if ( $request->has_block('extids') )
    {
	foreach my $f ( qw(matched_no accepted_no) )
	{
	    $record->{$f} = generate_identifier('TXN', $record->{$f})
		if defined $record->{$f};
	}
    }
    
    my $a = 1;	# we can stop here when debugging
}



sub addupdate_sandbox {
    
    my ($request, $operation) = @_;

    if ( $operation eq 'insert' )
    {
	$request->generate_sandbox({ operation => 'colls/add',
				     ruleset => '1.2:colls:addupdate_body',
				     allowances => '1.2:colls:allowances',
				     extra_params => 'vocab=pbdb&private&extids&show=edit' });
    }

    elsif ( $operation eq 'update' )
    {
	$request->generate_sandbox({ operation => 'colls/update',
				     ruleset => '1.2:colls:addupdate_body',
				     allowances => '1.2:colls:allowances',
				     extra_params => 'vocab=pbdb&private&extids&show=edit' });
    }
    
    else
    {
	$request->generate_sandbox({ operation => 'unknown' });
    }
}


sub update_occs_sandbox {

    my ($request, $operation) = @_;

    $request->generate_sandbox({ operation => 'occs/update',
				 ruleset => '1.2:occs:addupdate_body',
				 multiplicity => 3,
				 allowances => '1.2:occs:allowances',
			         extra_params => 'vocab=pbdb&private&extids&show=edit' });
}


sub check_names_sandbox {

    my ($request, $operation) = @_;

    $request->generate_sandbox({ operation => 'occs/checknames',
				 ruleset => '1.2:occs:checknames_body',
				 multiplicity => 5,
				 extra_params => 'vocab=pbdb&extids' });
}

1;
