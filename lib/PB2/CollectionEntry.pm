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

use CollectionEdit;

use Carp qw(carp croak);

use Moo::Role;

our (@REQUIRES_ROLE) = qw(PB2::Authentication PB2::CommonData PB2::CommonEntry PB2::CollectionData);


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
	    "then the body record should not contain a collection identifier.",
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
		       collection_name => { doc => "The name must contain at least one letter, with maximum length %{size}.", note => 'textarea', before => 'research_group' },
		       collection_aka => { note => 'textarea', before => 'research_group' },
		       country => { doc => "The value must be a country name appearing in the C<country_map> table." },
		       latdeg => { doc => "It accepts an integer between 0-89." },
		       lngdeg => { doc => "It accepts an integer between 0-180." },
		       latmin => { doc => "It accepts an integer between 0-59." },
		       latsec => { doc => "It accepts an integer between 0-59." },
		       lngmin => { doc => "It accepts an integer between 0-59." },
		       lngsec => { doc => "It accepts an integer between 0-59." },
		       latdec => { doc => "It is not allowed for this column to have a non-empty value together with either C<latmin> or C<latsec>. It accepts a string of at most 10 digits." },
		       lngdec => { doc => "It is not allowed for this column to have a non-empty value together with either C<lngmin> or C<lngsec>. It accepts a string of at most 10 digits." },
		       direct_ma => { doc => "It accepts an unsigned decimal number of at most 8 digits." },
		       direct_ma_error => { doc => "It accepts an unsigned decimal number of at most 8 digits." },
		       max_ma => { doc => "It accepts an unsigned decimal number of at most 8 digits." },
		       max_ma_error => { doc => "It accepts an unsigned decimal number of at most 8 digits." },
		       min_ma => { doc => "It accepts an unsigned decimal number of at most 8 digits." },
		       min_ma_error => { doc => "It accepts an unsigned decimal number of at most 8 digits." },
		       geogcomments => { note => 'textarea' },
		       stratcomments => { note => 'textarea' },
		       lithdescript => { note => 'lithdescript' },
		       fossilsfrom1 => { valid => BOOLEAN_VALUE, doc => "It accepts a value from the following list: 'true','false','1','0','yes','no'." },
		       fossilsfrom2 => { valid => BOOLEAN_VALUE, doc => "It accepts a value from the following list: 'true','false','1','0','yes','no'." },
		       geology_comments => { note => 'textarea' },
		       pres_mode => { note => 'textarea' },
		       common_body_parts => { note => 'textarea' },
		       rare_body_parts => { note => 'textarea' },
		       component_comments => { note => 'textarea' },
		       collection_comments => { note => 'textarea' },
		       taxonomy_comments => { note => 'textarea' },
		       access_level => { before => 'research_group' },
		       release_date => { doc => "It accepts either 'immediate', or else a string of the form 'n months' or 'n years' where n is a digit. The maximum is 5 years.", before => 'research_group' },
		     });
    
    add_to_ruleset($ds, '1.2:colls:addupdate_body',
	{ optional => 'max_interval', valid => [VALID_IDENTIFIER('INT'), ANY_VALUE], 
	  before => 'zone_type' },
	    "This parameter sets the value of C<max_interval_no> in the C<collections> table. It accepts an interval name or number, or an external identifier of type 'int'.",
	{ optional => 'min_interval', valid => [VALID_IDENTIFIER('INT'), ANY_VALUE],
	  before => 'zone_type' },
	    "This parameter sets the value of C<min_interval_no> in the C<collections> table. It accepts an interval name or number, or an external identifier of type 'int'. If this collection is associated with a single interval, leave this field null.");
    
    $ds->define_ruleset('1.2:occs:addupdate_body' =>
	">>The body of this request must be either a single JSON object, or an array of",
	"JSON objects, or else a single record in C<application/x-www-form-urlencoded> format.",
	"The following fields are allowed in each record.",
	"Any columns that are B<required> must be given a nonempty value in every new",
	"record, and may not be set to empty or null in an update.",
	{ optional => 'collection_id', valid => VALID_IDENTIFIER('COL'),
	  alias => ['collection_no'] },
	    "This B<required> field specifies the collection with which this occurrence",
	    "is or will be associated.",
	{ optional => 'occurrence_id', valid => VALID_IDENTIFIER('OCC'),
	  alias => ['occurrence_no', 'id', 'occ_id', 'oid'] },
	    "If this field is empty, an occurrence record will be inserted into the database",
	    "and a new identifier will be returned. If it is non-empty, it must match",
	    "the identifier of an existing record. That record will be updated.",
	{ optional => 'reid_id', valid => VALID_IDENTIFIER('REI'),
	  alias => ['reid_no'] },
	    "If this field occurs with an empty value, and if 'occurrence_id' is not",
	    "empty, a reidentification record will be inserted into the database and a",
	    "new identifier will be returned. If it is non-empty, it must match the",
	    "identifier of an existing record. That record will be updated.",
	{ allow => '1.2:common:entry_fields' });

    complete_ruleset($ds, $dbh, '1.2:occs:addupdate_body', 'OCCURRENCE_DATA',
		     { taxon_no => 'IGNORE', collection_no => 'IGNORE',
		       occurrence_no => 'IGNORE', reid_no => 'IGNORE',
		       genus_reso => 'IGNORE', genus_name => 'IGNORE',
		       subgenus_reso => 'IGNORE', subgenus_name => 'IGNORE',
		       species_reso => 'IGNORE', species_name => 'IGNORE',
		       subspecies_reso => 'IGNORE', subspecies_name => 'IGNORE' });
    
    add_to_ruleset($ds, '1.2:occs:addupdate_body', 
	{ optional => 'taxon_name', before => 'abund_value' },
	    "This parameter is parsed in order to set the value for C<genus_reso>,",
	    "C<genus_name>, etc. in the C<$TABLE{OCCURRENCE_DATA}> or",
	    "C<$TABLE{REID_DATA}> table as appropriate.");
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
    
    my ($allowances, $main_params) = $request->parse_main_params('1.2:colls:addupdate', 'collection_id');
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->parse_body_records($main_params,
		['REID_DATA', '1.2:reids:addupdate_body', 'reid_id'],
		['OCCURRENCE_DATA', '1.2:occs:addupdate_body', 'occurrence_id'],
		['COLLECTION_DATA', '1.2:colls:addupdate_body', 'collection_id', 'collection_name'],
		'NO_MATCH');
    
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
	if ( exists $r->{reid_id} )
	{
	    $edt->insert_update_record('REID_DATA', $r);
	}
	
	elsif ( exists $r->{occurrence_id} )
	{
	    $edt->insert_update_record('OCCURRENCE_DATA', $r);
	}
	
	elsif ( $operation eq 'insert' )
	{
	    $edt->insert_record('COLLECTION_DATA', $r);
	}

	elsif ( $operation eq 'update' )
	{
	    $edt->update_record('COLLECTION_DATA', $r);
	}

	elsif ( $operation eq 'replace' )
	{
	    $edt->replace_record('COLLECTION_DATA', $r);
	}

	elsif ( $operation eq 'delete' )
	{
	    $edt->delete_record('COLLECTION_DATA', $r);
	}
	
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
    
    my @existing_keys = ($edt->updated_keys, $edt->inserted_keys);
    
    unless ( $request->has_block('none') )
    {
	$request->list_updated_colls($dbh, \@existing_keys, $edt->key_labels) if @existing_keys;
    }
}


sub list_updated_colls {
    
    my ($request, $dbh, $coll_ids, $ref_labels) = @_;
    
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
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


sub addupdate_sandbox {
    
    my ($request, $operation) = @_;

    if ( $operation eq 'insert' )
    {
	$request->generate_sandbox({ operation => 'colls/add',
				     ruleset => '1.2:colls:addupdate_body',
				     allowances => '1.2:colls:allowances',
				     extra_params => 'vocab=pbdb&private&show=edit' });
    }

    elsif ( $operation eq 'update' )
    {
	$request->generate_sandbox({ operation => 'colls/update',
				     ruleset => '1.2:colls:addupdate_body',
				     allowances => '1.2:colls:allowances',
				     extra_params => 'vocab=pbdb&private&show=edit' });
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
				 multiplicity => 5,
				 allowances => '1.2:colls:allowances' });
}


1;
