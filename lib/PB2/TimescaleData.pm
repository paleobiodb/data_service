#  
# TimescaleData
# 
# A role that returns information from the PaleoDB database about geological timescales
# and time intervals.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::TimescaleData;

use HTTP::Validate qw(:validators);

use TableDefs qw(%TABLE);

use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::ReferenceData);

our ($INTERVAL_ATTRS, $INT_BOUND_ATTRS);

# initialize ( )
# 
# This routine is called by the DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # We start with the basic output blocks for timescales, intervals, and bounds.
    
    $ds->define_set('1.2:timescales:bound_types' =>
	{ value => 'absolute' },
	    "A boundary whose age is specified absolutely, in Ma",
	{ value => 'spike' },
	    "An absolute boundary which is fixed by a GSSP",
	{ value => 'same' },
	    "A boundary whose age is defined to be the same as some other boundary. For",
	    "example, the beginning of the Triassic is defined to be the same as",
	    "the beginning of the Induan.",
	{ value => 'fraction' },
	    "A boundary whose age is specified as a fraction of the age span between",
	    "two other boundaries.");
    
    $ds->define_set('1.2:timescales:interval_types' =>
	{ value => 'supereon' },
	{ value => 'eon' },
	{ value => 'era' },
	{ value => 'period' },
	{ value => 'superepoch' },
	{ value => 'epoch' },
	{ value => 'subepoch' },
	{ value => 'stage' },
	{ value => 'substage' },
	{ value => 'zone' },
	{ value => 'chron' },
	{ value => 'other' },
	{ value => 'multi' });
    
    # $ds->define_set('1.2:timescales:status' => 
    # 	{ value => 'deleted' }, "The database row corresponding to this record has been deleted");
    
    $ds->define_block('1.2:timescales:basic' =>
	{ select => [ 'ts.timescale_no', 'ts.timescale_name', 'ts.max_age', 'ts.min_age',
		      'ts.has_error', 'ts.is_visible', 'ts.is_enterable', 'ts.admin_lock',
		      'ts.priority', 'ts.reference_no' ] },
	{ set => '*', code => \&process_ids },
	{ set => '*', code => \&process_timescale_ages },
	{ output => 'timescale_no', com_name => 'oid' },
	    "The unique identifier of this timescale in the database",
	{ output => 'record_type', com_name => 'typ', value => $IDP{TSC} },
	    "The type of this object: C<$IDP{TSC}> for a timescale. This",
	    "field will only appear if the object identifier is",
	    "being returned as a number.",
	{ output => '_label', com_name => 'rlb' },
	    "For newly added or updated records, this field will report the record",
	    "label value, if any, that was submitted with the record.",
	{ output => 'status', com_name => 'sta' },
	    "In the output of a deletion operation, each deleted record will have the value",
	    "'deleted'.",
	{ output => 'timescale_name', com_name => 'nam' },
	    "The name of this timescale",
	{ output => 'min_age', com_name => 'lag', data_type => 'str' },
	    "The late bound of this timescale, in Ma",
	{ output => 'max_age', com_name => 'eag', data_type => 'str' },
	    "The early bound of this timescale, in Ma",
	{ output => 'has_error', com_name => 'err' },
	    "True if the set of boundaries associated with this timescale is inconsistent.",
	{ output => 'is_visible', com_name => 'vis' },
	    "True if this timescale is visible to all users, false otherwise.",
	{ output => 'is_enterable', com_name => 'enc' },
	    "True if this timescale can be used when entering collections, false otherwise.",
	{ output => 'admin_lock', com_name => 'lck' },
	    "True if the attributes and bounds of this timescale are locked.",
	{ output => 'priority', com_name => 'pri' },
	    "If non-zero, this represents the priority used when an interval",
	    "appears in more than one timescale with different age bounds. The",
	    "bounds defined by the timescale with the highest value for this",
	    "attribute are considered to be definitive.",
	{ output => 'reference_no', com_name => 'rid' },
	    "The unique identifier of the main bibliographic reference for this",
	    "timescale, if any.");
    
    $ds->define_output_map('1.2:timescales:optional_basic' =>
	{ value => 'desc', maps_to => '1.2:timescales:desc' },
	    "The geographic and taxonomic extent of this timescale, and the",
	    "type of intervals it contains",
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifier of the person who authorized this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The name of the person who authorized this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the timescale record");

    $ds->define_block('1.2:timescales:desc' =>
	{ select => [ 'ts.timescale_extent', 'ts.timescale_taxon', 'ts.timescale_type', 'ts.timescale_comments' ],
	  tables => 'ts' },
	{ output => 'timescale_extent', com_name => 'ext' },
	    "The geographic extent over which the timescale is valid.",
	    "This will typically be 'global', but could also be the name",
	    "of a continent or other geographic region. It should be expressed",
	    "as an adjective, for example: 'North American'",
	{ output => 'timescale_taxon', com_name => 'txn' },
	    "The taxonomic group, if any, on which the timescale is defined.",
	    "This should be a singular common name, for example: 'ammonite' or 'conodont'.",
	{ output => 'timescale_type', com_name => 'typ' },
	    "The type of interval defined by this time scale, which will be one of the following:",
	    $ds->document_set('1.2:timescales:interval_types'),
	{ output => 'timescale_comments', com_name => 'tsc' },
	    "Comments about this timescale, if any");
    
    $ds->define_block('1.2:timescales:bound' =>
	{ select => [ 'tsb.bound_no', 'tsb.bound_type', 'tsb.age', 'tsb.age_error', 'tsb.age_prec', 'tsb.age_error_prec',
		      'tsb.timescale_no', 'coalesce(tsb.interval_type, ts.timescale_type) as interval_type',
		      'tsb.fraction','tsb.fraction_prec', 'tsb.interval_name',
		      'tsb.base_no', 'tsb.range_no', 'tsb.top_no', 'tsb.color_no',
		      'tsb.has_error', 'tsb.is_modeled', 'tsb.is_spike', 'tsb.color', 'tsi.early_age as int_early' ],
	  tables => ['tsb', 'ts', 'tsi'] },
	{ set => '*', code => \&process_ids },
	{ set => '*', code => \&process_bound_ages },
	{ output => 'bound_no', com_name => 'oid' },
	    "The unique identifier of this boundary in the database",
	{ output => 'record_type', com_name => 'typ', value => $IDP{BND} },
	    "The type of this object: C<$IDP{BND}> for an interval boundary. This",
	    "field will only appear if the object identifier is",
	    "being returned as a number.",
	{ output => '_label', com_name => 'rlb' },
	    "For newly added or updated records, this field will report the record",
	    "label value, if any, that was submitted with the record.",
	{ output => 'status', com_name => 'sta' },
	    "In the output of a deletion operation, each newly deleted record will have",
	    "the value 'deleted'.",
	{ output => 'timescale_no', com_name => 'sid' },
	    "The identifier of the timescale which this bound partially defines.",
	{ output => 'interval_name', com_name => 'inm' },
	    "The name of the interval lying immediately above this boundary. If",
	    "empty, then this boundary marks the top of one segment of this timescale.",
	{ output => 'interval_type', com_name => 'itp', if_block => 'desc' },
	    "The interval type. This will be one of the following:", $ds->document_set(''),
	{ output => 'age', com_name => 'age', data_type => 'str' },
	    "The age at which this boundary is fixed or calculated, in Ma",
	{ output => 'age_error', com_name => 'ger', data_type => 'str' },
	    "The uncertainty (+/-) if any associated with this age",
	{ output => 'bound_type', com_name => 'btp' },
	    "The boundary type, which will be one of the following:",
		      $ds->document_set('1.2:timescales:bound_types'),
	{ output => 'is_modeled', com_name => 'mdl' },
	    "True if this boundary was modeled relative to the international",
	    "chronostratigraphic intervals.",
	{ output => 'is_spike', com_name => 'spk' },
	    "True if this boundary is a GSSP.",
	# { output => 'interval_no', com_name => 'iid' },
	#     "If the interval name is not empty and this timescale is active,",
	#     "then this field contains the identifier corresponding to this interval name.",
	{ output => 'top_no', com_name => 'uid' },
	    "The identifier of the top boundary of the interval lying immediately",
	    "above this boundary.",
	{ output => 'base_no', com_name => 'bid' },
	    "If the boundary type is C<B<same> or C<B<fraction>>, this specifies",
	    "the identifier of the base boundary with respect to which this boundary is",
	    "being defined.",
	{ output => 'range_no', com_name => 'tid' },
	    "If the boundary type is C<B<fraction>>, this specifies",
	    "the top boundary of the pair between which this boundary lies. The",
	    "C<B<base_no>> field specifies the bottom boundary.",
	{ output => 'color_no', com_name => 'cid' },
	    "If this field is non-empty, it indicates a boundary in another",
	    "timescale from which the color for this boundary is taken.",
	{ output => 'fraction', com_name => 'frc', data_type => 'str' },
	    "For boundaries of type 'fraction', the boundary lies this much",
	    "of the way through the age span indicated by the base and range",
	    "boundaries.",
	{ output => 'has_error', com_name => 'err' },
	    "True if this boundary is inconsistent with the other boundaries in",
	    "its timescale, for example in overlapping with another boundary.",
	{ output => 'is_different', com_name => 'dif' },
	    "This field will contain a true value if the bound age is different from the",
	    "bottom age of the corresponding interval in the active interval table.",
	{ output => 'color', com_name => 'col' },
	    "The standard color (if any) that should be assigned to the upper",
	    "interval associated with this boundary.");

    $ds->define_output_map('1.2:timescales:optional_bound' =>
	{ value => 'desc', maps_to => '1.2:timescales:bound_desc' },
	    "Additional descriptive attributes for this bound");

    $ds->define_block('1.2:timescales:bound_desc' =>
	{ select => ['ts.timescale_extent', 'ts.timescale_taxon'],
	  tables => 'ts' },
	{ output => 'timescale_extent', com_name => 'ext' },
	    "The geographic extent over which this bound is valid.",
	{ output => 'timescale_taxon', com_name => 'txn' },
	    "The taxonomic group, if any, on which this bound is defined.");
    
    $ds->define_block('1.2:timescales:interval' =>
	{ select => [ 'INTERVAL_ATTRS' ] },
	{ set => '*', code => \&process_ids },
	{ set => '*', code => \&process_int_ages },
	{ output => 'interval_no', com_name => 'oid' },
	    "The unique identifier of this interval in the database",
	{ output => 'record_type', com_name => 'typ', value => $IDP{INT} },
	    "The type of this object: C<$IDP{INT}> for an interval. This",
	    "field will only appear if the object identifier is",
	    "being returned as a number.",
	{ output => '_label', com_name => 'rlb' },
	    "For newly added or updated records, this field will report the record",
	    "label value, if any, that was submitted with the record.",
	{ output => 'status', com_name => 'sta' },
	    "In the output of a deletion operation, each newly deleted record will have",
	    "the value 'deleted'.",
	{ output => 'timescale_no', com_name => 'sid' },
	    "The identifier of the timescale from which this interval definition is taken.",
	{ output => 'bound_no', com_name => 'bid' },
	    "The identifier of the bound from which this interval definition is taken.",
	{ output => 'interval_name', com_name => 'nam' },
	    "The name of the interval",
	{ output => 'interval_type', com_name => 'itp', if_block => 'type, desc' },
	    "The type of interval, e.g. stage, epoch, zone",
	{ output => 'abbrev', com_name => 'abr' },
	    "The standard abbreviation for the interval, if any",
	{ output => 'late_age', com_name => 'lag', data_type => 'str' },
	    "The late age bound for this interval, according to the",
	    "most authoritative timescale in which it is contained",
	{ output => 'early_age', com_name => 'eag', data_type => 'str' },
	    "The early age bound for this interval, according to the",
	    "most authoritative timescale in which it is contained",
	{ output => 'is_different', com_name => 'dif' },
	    "This field will contain a true value if the early and/or late ages for this",
	    "interval as expressed in the selected timescale are different from the ones",
	    "specified for this interval in the active interval table.",
	{ output => 'color', com_name => 'col' },
	    "The standard color for this interval, if any");
    
    $INTERVAL_ATTRS = "tsi.interval_name, tsi.interval_type, tsi.timescale_no, tsi.bound_no, tsi.interval_no, 
		tsi.abbrev, tsi.early_age, tsi.early_age_prec, tsi.late_age, tsi.late_age_prec, tsi.color";
    $INT_BOUND_ATTRS = "tsb.interval_name, tsb.bound_no, tsi.interval_no, tsi.abbrev, tsb.timescale_no, tsb.interval_type, 
		tsb.age as early_age, tsb.age_prec as early_age_prec, btp.age as late_age, btp.age_prec as late_age_prec,
		tsi.early_age as int_early, tsi.late_age as int_late, if(tsb.color <> '', tsb.color, tsi.color) as color";
    
    $ds->define_output_map('1.2:timescales:optional_interval' =>
	{ value => 'type' },
	    "The interval type",
	{ value => 'desc', maps_to => '1.2:timescales:interval_desc' },
	    "Additional descriptive attributes for this interval");
    
    $ds->define_block('1.2:timescales:interval_desc' =>
	{ select => [ 'ts.timescale_name', 'ts.timescale_extent', 'ts.timescale_taxon', 'tsb.interval_type' ],
	  tables => ['tsb', 'ts'] },
	{ output => 'timescale_name', com_name => 'tsn' },
	    "The name of the timescale in which this interval is defined.",
	{ output => 'timescale_extent', com_name => 'ext' },
	    "The geographic extent of the timescale in which this interval is defined, if any",
	{ output => 'timescale_taxon', com_name => 'txn' },
	    "The taxonomic group associated with the timescale in which this interval is defined,",
	    "if any (e.g. ammonite, condont, mammal)");

    # Now define ruleset building blocks that are used in defining the operation rulesets.
    
    $ds->define_ruleset('1.2:timescales:specifier' =>
	{ param => 'timescale_id', valid => VALID_IDENTIFIER('TSC'), alias => 'id' },
	    "The unique identifier of the timescale you wish to retrieve (REQIRED).",
	    "You may instead use the parameter name B<C<id>>.");
    
    # $ds->define_ruleset('1.2:timescales:bound_specifier' =>
    # 	{ param => 'bound_id', valid => VALID_IDENTIFIER('BND'), alias => 'id' },
    # 	    "The unique identifier of the bound you wish to retrieve (REQIRED).",
    # 	    "You may instead use the parameter name B<C<id>>.");
    
    $ds->define_ruleset('1.2:timescales:ts_selector' =>
	{ param => 'timescale_id', valid => VALID_IDENTIFIER('TSC'), list => ',', alias => 'id', bad_value => '0' },
	    "Return only the specified timescales. You may provide one or more",
	    "timescale identifiers, separated by commas.",
	{ param => 'bound_id', valid => VALID_IDENTIFIER('BND'), list => ',', bad_value => '0' },
	    "Return only timescales containing the specified bounds. You may provide one or more",
	    "bound identifiers, separated by commas.",
	{ param => 'interval_id', valid => VALID_IDENTIFIER('INT'), list => ',', bad_value => '0' },
	    "Return only timescales containing the specified intervals. You may provide one or more",
	    "interval identifiers, separated by commas.");
    
    $ds->define_ruleset('1.2:timescales:tsb_selector' =>
	{ param => 'bound_id', valid => VALID_IDENTIFIER('BND'), list => ',', alias => 'id', bad_value => '0' },
	    "Return only the specified bounds. You may provide one or more",
	    "bound identifiers, separated by commas.",
	{ param => 'timescale_id', valid => VALID_IDENTIFIER('TSC'), list => ',', bad_value => '0' },
	    "Return only bounds contained in the specified timescales. You may provide one or more",
	    "timescale identifiers, separated by commas.",
	{ param => 'interval_id', valid => VALID_IDENTIFIER('INT'), list => ',', bad_value => '0' },
	    "Return only bounds corresponding to the specified intervals. You may provide one or more",
	    "interval identifiers, separated by commas.");
    
    $ds->define_ruleset('1.2:timescales:tsi_selector' =>
	{ param => 'interval_id', valid => VALID_IDENTIFIER('INT'), list => ',', alias => 'id', bad_value => '0' },
	    "Return only the specified intervals. You may provide one or more",
	    "interval identifiers, separated by commas.",
	{ param => 'bound_id', valid => VALID_IDENTIFIER('BND'), list => ',', bad_value => '0' },
	    "Return only intervals corresponding to the specified bounds. You may provide one or more",
	    "bound identifiers, separated by commas.",
	{ param => 'timescale_id', valid => VALID_IDENTIFIER('TSC'), list => ',', bad_value => '0' },
	    "Return only intervals contained in the specified timescales. You may provide one or more",
	    "timescale identifiers, separated by commas.");
    
    $ds->define_ruleset('1.2:timescales:common_selector' =>
	{ optional => 'active', valid => FLAG_VALUE },
	    "If specified, return only records associated with active timescales.",
	{ param => 'type', valid => '1.2:timescales:interval_types', list => ',', bad_value => '0' },
	    "Return only timescales, intervals, or bounds of the specified type(s).",
	    "Accepted values include:",
	{ param => 'taxon', valid => ANY_VALUE, list => ',' },
	    "Return only timescales, intervals, or bounds associated with the specified",
	    "organism(s). You may specify more than one name, separated by commas. Examples:",
	    "ammonite, conodont, mammal",
	{ param => 'extent', valid => ANY_VALUE, list => ',' },
	    "Return only timescales, intervals, or bounds with the specified extent. You",
	    "may specify more than one, separated by commas. Example: global, north american,",
	    "european",
	{ param => 'timescale_match', valid => ANY_VALUE, list => ',', alias => 'timescale_name' },
	    "Return only timescales whose name matches the specified value(s),",
	    "or intervals or bounds contained within them.",
	    "You may specify more than one value separated by commas,",
	    "and you may use the wildcards C<%> and C<_>.",
	{ param => 'interval_name', valid => ANY_VALUE, list => ',' },
	    "Return only intervals whose name one of the specified string(s),
	     or timescales or bounds that contain or refer to them.",
	{ param => 'interval_match', valid => ANY_VALUE, list => ',' },
	    "Return only intervals whose name matches the specified value(s),",
	    "or timescales or bounds that contain or refer to them.",
	    "You may specify more than one value separated by commas,",
	    "and you may use the wildcards C<%> and C<_>.",
	{ param => 'max_ma', valid => DECI_VALUE(0) },
	    "Return only timescales, intervals, or bounds whose lower bound is at most this old.",
	{ param => 'min_ma', valid => DECI_VALUE(0) },
	    "Return only timescales, intervals, or bounds whose upper bound is at least this old.");
    
    $ds->define_ruleset('1.2:timescales:tsi_specifier' =>
	{ param => 'interval_id', valid => VALID_IDENTIFIER('INT'), alias => 'id' },
	    "The identifier of the interval you wish to retrieve.",
	    "You may instead use the parameter name B<C<id>>.",
	{ param => 'interval_name', valid => ANY_VALUE, alias => 'name' },
	    "The name of the interval you wish to retrieve. You may specify either this",
	    "parameter or B<C<interval_id>>, but not both.",
	{ at_most_one => ['interval_id', 'interval_name'] });
	# { optional => 'timescale_id', valid => VALID_IDENTIFIER('TSC') },
	#     "You may optinally specify the identifier of a timescale in which this interval is",
	#     "defined. The reported interval attributes, including age bounds, will be taken from",
	#     "the specified timescale, even if they are otherwise superseded by the definition",
	#     "of this interval in some other timescale with higher priority.");
    
    $ds->define_ruleset('1.2:timescales:all_records' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "Select all timescales (or intervals, or boundaries) entered in the database,",
	    "subject to any other parameters you may specify.",
	    "This parameter does not require any value.");
    
    # Now define rulesets for the operations defined in this module.
    
    $ds->define_ruleset('1.2:timescales:single' =>
	{ require => '1.2:timescales:specifier' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_basic' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:timescales:list' =>
	{ allow => '1.2:timescales:all_records' },
	{ allow => '1.2:timescales:ts_selector' },
	{ allow => '1.2:timescales:common_selector' },
	{ require_any => ['1.2:timescales:all_records', '1.2:timescales:ts_selector',
			  '1.2:timescales:common_selector'] },
	">>The following parameters can be used to filter the selection.",
	"If you wish to use one of them and have not specified any of the selection parameters",
	"listed above, use B<C<all_records>>.",
	{ allow => '1.2:common:select_ent' },
	{ allow => '1.2:common:select_crmod' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_basic' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    # $ds->define_ruleset('1.2:bounds:single' =>
    # 	{ require => '1.2:timescales:bound_specifier' },
    # 	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_bound' },
    # 	{ allow => '1.2:special_params' },
    # 	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:timescales:bounds' =>
	{ allow => '1.2:timescales:all_records' },
	{ allow => '1.2:timescales:tsb_selector' },
	{ allow => '1.2:timescales:common_selector' },
	{ require_any => ['1.2:timescales:all_records', '1.2:timescales:tsb_selector',
			  '1.2:timescales:common_selector'] },
	">>The following parameters can be used to filter the selection.",
	"If you wish to use one of them and have not specified any of the selection parameters",
	"listed above, use B<C<all_records>>.",
	{ allow => '1.2:common:select_crmod' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_bound' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:timescales:intervals' =>
	{ allow => '1.2:timescales:all_records' },
	{ optional => 'all_timescales', valid => FLAG_VALUE },
	    "If this parameter is true, then a record is returned for each timescale",
	    "in which this interval is defined. Otherwise, only a single record will be",
	    "returned, indicating the timescale from which the interval definition is",
	    "taken.",
	{ allow => '1.2:timescales:tsi_selector' },
	{ allow => '1.2:timescales:common_selector' },
	{ require_any => ['1.2:timescales:all_records', '1.2:timescales:tsi_selector',
			  '1.2:timescales:common_selector'] },
	">>The following parameters can be used to filter the selection.",
	"If you wish to use one of them and have not specified any of the selection parameters",
	"listed above, use B<C<all_records>>.",
	{ allow => '1.2:common:select_crmod' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_bound' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:tsi:single' =>
	{ require => '1.2:timescales:tsi_specifier' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_interval' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:tsi:list' =>
	{ allow => '1.2:timescales:all_records' },
	{ allow => '1.2:timescales:tsi_selector' },
	{ allow => '1.2:timescales:common_selector' },
	{ require_one => ['1.2:timescales:all_records', '1.2:timescales:tsi_selector',
			  '1.2:timescales:common_selector'] },
	{ allow => '1.2:common:select_crmod' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_interval' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
}


# get_single ( type )
# 
# Return a record representing a single timescale, interval, or bound. The data type
# to be returned must be indicated by $type.

sub get_record {

    my ($request, $type) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $tables = $request->tables_hash;
    
    # Make sure we have a valid id number.
    
    my ($id, $name, $timescale_id, $mt);
    
    if ( $type eq 'timescales' )
    {
	$id = $request->clean_param('timescale_id');
    }
    
    elsif ( $type eq 'bounds' )
    {
	$id = $request->clean_param('bound_id');
    }
    
    elsif ( $type eq 'intervals' || $type eq 'tsi' )
    {
	$id = $request->clean_param('interval_id');
	$name = $request->clean_param('interval_name');
	$timescale_id = $request->clean_param('timescale_id');
    }

    $id ||= '';
    die "400 Bad identifier '$id'\n" unless ($id and $id =~ /^\d+$/) or $name;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'ts', cd => 'ts' );
    
    my @fields = $request->select_list;
    
    my $fields = join(', ', @fields);
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also respond properly to the 'extid' parameter, if given.
    
    $request->strict_check;
    $request->extid_check;
    
    $request->delete_output_field('_label');
    $request->delete_output_field('status');
    
    # Generate the main query. We will already have thrown an error above if $type is not one of
    # the following values.
    
    if ( $type eq 'timescales' )
    {
	my $join_list = $request->generate_join_list('ts', $tables);
	
	$request->{main_sql} = "
	SELECT $fields
	FROM $TABLE{TIMESCALE_DATA} as ts
        WHERE ts.timescale_no = $id
	GROUP BY ts.timescale_no";
    }
    
    elsif ( $type eq 'bounds' )
    {
	my $join_list = $request->generate_join_list('tsb', $tables);
	
	$request->{main_sql} = "
	SELECT $fields
	FROM $TABLE{TIMESCALE_BOUNDS} as tsb $join_list
        WHERE tsb.bound_no = $id
	GROUP BY tsb.bound_no";
    }
    
    elsif ( $type eq 'intervals' && $timescale_id )
    {
	$fields =~ s/INTERVAL_ATTRS/$INT_BOUND_ATTRS/;

	my $filter = $name ? "tsb.interval_name = " . $dbh->quote($name)
			   : "tsi.interval_no = $id";

	my $join_list = $request->generate_join_list('tsb', $tables);
	
	$request->{main_sql} = "
	SELECT $fields
	FROM $TABLE{TIMESCALE_BOUNDS} as tsb
		left join $TABLE{TIMESCALE_INTS} as tsi using (interval_name)
		left join $TABLE{TIMESCALE_BOUNDS} as btp on btp.bound_no = tsb.top_no $join_list
	WHERE $filter and tsb.timescale_no = $timescale_id and tsb.interval_name <> ''
	GROUP BY tsb.interval_name, tsb.timescale_no";
    }
    
    elsif ( $type eq 'tsi' || $type eq 'intervals' )
    {
	$fields =~ s/INTERVAL_ATTRS/$INTERVAL_ATTRS/;
	
	my $join_list = $request->generate_join_list('tsi', $tables);
	
	my $filter = $name ? "tsi.interval_name = " . $dbh->quote($name)
			   : "tsi.interval_no = $id";
	
	$request->{main_sql} = "
	SELECT $fields
	FROM $TABLE{TIMESCALE_INTS} as tsi $join_list
        WHERE $filter
	GROUP BY tsi.interval_no";
    }
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    my $record = $dbh->selectrow_hashref($request->{main_sql});
    
    # Return an error response if we couldn't retrieve the record.
    
    die "404 Not found\n" unless $record;
    
    # Return the result otherwise.
    
    return $request->single_result($record);
}


# list_records ( type )
# 
# Return a list of records representing timescales, intervals, or bounds. The data type to be
# returned must be indicated by $type. The list is selected according to the request parameters.

sub list_records {
    
    my ($request, $type) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    my $tables = $request->tables_hash;
    
    # Figure out which kind of record we are being asked for.
    
    my ($mt, $index, $relative);
    
    if ( $type eq 'timescales' )
    {
	$mt = 'ts';
    }
    
    elsif ( $type eq 'bounds' )
    {
	$mt = 'tsb';
    }
    
    elsif ( $type eq 'intervals' )
    {
	if ( $request->clean_param('all_timescales') )
	{
	    $mt = 'tsb';
	    $tables->{btp} = 1;
	    $tables->{tsi} = 1;
	}
	
	else
	{
	    $type = 'tsi';
	    $mt = 'tsi';
	}
    }
    
    elsif ( $type eq 'tsi' )
    {
	$mt = 'tsi';
    }
    
    die "unknown record type '$type'\n" unless $mt;
    
    $request->substitute_select( mt => $mt, cd => $mt );
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = $request->generate_timescale_filters($mt, $type, $tables);
    push @filters, $request->generate_common_filters( { bare => 'ts' } );
    
    # # If we are querying for intervals, figure out if we are doing so absolutely or relative to
    # # the timescales in which they are contained.
    
    # if ( $type eq 'intervals' && ! $request->clean_param('absolute') )
    # {
    # 	$tables->{tsb} = 1;
    # }
    
    # Do a final check to make sure that all records are only returned if
    # 'all_records' was specified.
    
    if ( @filters == 0 )
    {
	die "400 You must specify 'all_records' if you want to retrieve the entire set of records.\n"
	    unless $request->clean_param('all_records');
	
	push @filters, "1=1";
    }
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also handle the parameter 'extids' if it was given.
    
    $request->strict_check;
    $request->extid_check;
    
    $request->delete_output_field('_label');
    $request->delete_output_field('status');
    
    my $filter_string = join(' and ', @filters);
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $request->select_string;
    
    # Determine the order in which the results should be returned.
    
    my $order_expr = '';
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list($mt, $tables);
    
    # Generate the main query. We will already have thrown an error above if $type is not one of
    # the following values.
    
    if ( $type eq 'timescales' )
    {
	$order_expr ||= 'order by ts.timescale_no';
	
	$request->{main_sql} = "
	SELECT $calc $fields
	FROM $TABLE{TIMESCALE_DATA} as ts $join_list
        WHERE $filter_string
	GROUP BY ts.timescale_no $order_expr";
    }
    
    elsif ( $type eq 'bounds' )
    {
	$order_expr ||= 'order by tsb.timescale_no, tsb.age, if(tsb.top_no = 0, 0, 1), tsb.bound_no';
	
	$request->{main_sql} = "
	SELECT $calc $fields
	FROM $TABLE{TIMESCALE_BOUNDS} as tsb $join_list
        WHERE $filter_string
	GROUP BY tsb.bound_no $order_expr";
    }
    
    elsif ( $type eq 'intervals' )
    {
	$order_expr ||= 'order by tsb.age';
	
	$fields =~ s/INTERVAL_ATTRS/$INT_BOUND_ATTRS/;
	
	$request->{main_sql} = "
	SELECT $calc $fields
	FROM $TABLE{TIMESCALE_BOUNDS} as tsb $join_list
	WHERE $filter_string and tsb.interval_name <> ''
	GROUP BY tsb.interval_name, tsb.timescale_no $order_expr";
    }
    
    elsif ( $type eq 'tsi' )
    {
	$order_expr ||= 'order by early_age';
	
	$fields =~ s/INTERVAL_ATTRS/$INTERVAL_ATTRS/;
	
	$request->{main_sql} = "
	SELECT $calc $fields
	FROM $TABLE{TIMESCALE_INTS} as tsi $join_list
        WHERE $filter_string
	GROUP BY tsi.interval_name $order_expr";
    }
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


sub generate_timescale_filters {
    
    my ($request, $mt, $type, $tables) = @_;
    
    my $dbh = $request->get_connection;
    
    my @filters;
    
    # Check for the id parameters
    
    if ( my @timescale_nos = $request->safe_param_list('timescale_id') )
    {
	my $id_list = $request->check_values($dbh, \@timescale_nos, 'timescale_no', $TABLE{TIMESCALE_DATA}, 
					     "Unknown timescale 'tsc:%'");
	
	$request->add_warning("no valid timescale identifiers were given") if $id_list eq '-1';

	push @filters, "$mt.timescale_no in ($id_list)";
	
	# if ( $type eq 'timescales' )
	# {
	#     push @filters, "ts.timescale_no in ($id_list)";
	# }
	
	# else
	# {
	#     push @filters, "tsb.timescale_no in ($id_list)";
	#     $tables->{tsb} = 1;
	# }
    }
    
    if ( my @bound_nos = $request->clean_param_list('bound_id') )
    {
	my $id_list = $request->check_values($dbh, \@bound_nos, 'bound_no', $TABLE{TIMESCALE_BOUNDS}, 
					     "Unknown timescale bound 'bnd:%'");
	
	$request->add_warning("no valid bound identifiers were given") if $id_list eq '-1';
	
	push @filters, "tsb.bound_no in ($id_list)";
	$tables->{tsb} = 1;
    }
    
    if ( my @interval_nos = $request->clean_param_list('interval_id') )
    {
	my $id_list = $request->check_values($dbh, \@interval_nos, 'interval_no', $TABLE{TIMESCALE_INTS}, 
					     "Unknown timescale interval 'int:%'");
	
	$request->add_warning("no valid interval identifiers were given") if $id_list eq '-1';
	
	push @filters, "tsi.interval_no in ($id_list)";
	$tables->{tsi} = 1;
	
	# if ( $type eq 'intervals' )
	# {
	#     push @filters, "tsi.interval_no in ($id_list)";
	# }
	
	# else
	# {	
	#     push @filters, "tsb.interval_no in ($id_list)";
	#     $tables->{tsb} = 1;
	# }
    }
    
    # Check for name matches
    
    if ( my @names = $request->clean_param_list('interval_name') )
    {
	my $tbl = $type eq 'tsi' ? 'tsi' : 'tsb';
	push @filters, $request->generate_match_like($dbh, "$tbl.interval_name", \@names);
	$tables->{$tbl} = 1;
    }
    
    if ( my @names = $request->clean_param_list('interval_match') )
    {
	my $tbl = $type eq 'tsi' ? 'tsi' : 'tsb';
	push @filters, $request->generate_match_regex($dbh, "$tbl.interval_name", \@names);
	$tables->{$tbl} = 1;
    }
    
    if ( my @names = $request->clean_param_list('timescale_match') )
    {
	push @filters, $request->generate_match_regex($dbh, 'ts.timescale_name', \@names);
	$tables->{ts} = 1;
    }
    
    # Check for 'max_ma' and 'min_ma'
    
    if ( my $max_ma = $request->clean_param('max_ma') )
    {
	if ( $type eq 'timescales' )
	{
	    push @filters, "ts.max_age <= $max_ma";
	}
	
	elsif ( $type eq 'bounds' )
	{
	    push @filters, "tsb.age <= $max_ma";
	    $tables->{tsb} = 1;
	}

	else
	{
	    push @filters, "tsi.early_age <= $max_ma";
	}
    }
    
    if ( my $min_ma = $request->clean_param('min_ma') )
    {
	if ( $type eq 'timescales' )
	{
	    push @filters, "ts.min_age >= $min_ma";
	}
	
	elsif ( $type eq 'bounds' )
	{
	    push @filters, "btp.age >= $min_ma";
	    $tables->{btp} = 1;
	}
	
	else
	{
	    push @filters, "tsi.late_age >= $min_ma";
	}
    }
    
    # Check for 'type', 'extent', and 'taxon'.
    
    if ( my @types = $request->clean_param_list('type') )
    {
	my $field;
	
	if ( $type eq 'timescales' )
	{
	    $field = "ts.timescale_type";
	}
	
	else
	{
	    $field = "$mt.interval_type";
	}
	
	# foreach my $t ( @types )
	# {
	#     $t = 'zone' if lc $t eq 'chron';
	# }
	
	push @filters, $request->generate_match_list($dbh, $field, \@types);
    }
    
    if ( my @names = $request->clean_param_list('taxon') )
    {
	push @filters, $request->generate_match_list($dbh, "ts.timescale_taxon", \@names);
	$tables->{ts} = 1;
    }
    
    if ( my @extents = $request->clean_param_list('extent') )
    {
	foreach my $e ( @extents )
	{
	    $e = 'global|international' if $e eq 'global' || $e eq 'international';
	}
	
	push @filters, $request->generate_match_regex($dbh, "ts.timescale_extent", \@extents);
	$tables->{ts} = 1;
    }
    
    return @filters;
}


sub generate_join_list {
    
    my ($request, $mt, $tables) = @_;
    
    my $joins = '';
    
    if ( $mt eq 'ts' )
    {
	$joins .= "\tleft join $TABLE{TIMESCALE_BOUNDS} as tsb on tsb.timescale_no = ts.timescale_no\n"
	    if $tables->{tsi} || $tables->{tsb};
	$joins .= "\tleft join $TABLE{TIMESCALE_INTS} as tsi on tsi.interval_name = tsb.interval_name\n"
	    if $tables->{tsi};
    }
    
    elsif ( $mt eq 'tsb' )
    {
	$joins .= "\tjoin $TABLE{TIMESCALE_DATA} as ts on ts.timescale_no = $mt.timescale_no\n"
	    if $tables->{ts};
	$joins .= "\tleft join $TABLE{TIMESCALE_BOUNDS} as btp on btp.bound_no = tsb.top_no\n"
	    if $tables->{btp};
	$joins .= "\tleft join $TABLE{TIMESCALE_INTS} as tsi on tsi.interval_name = $mt.interval_name\n"
	    if $tables->{tsi};
    }
    
    elsif ( $mt eq 'tsi' )
    {
	$joins .= "\tjoin $TABLE{TIMESCALE_DATA} as ts on ts.timescale_no = $mt.timescale_no\n"
	    if $tables->{ts};
	$joins .= "\tjoin $TABLE{TIMESCALE_BOUNDS} as tsb on tsb.interval_name = $mt.interval_name and tsb.timescale_no = $mt.timescale_no\n"
	    if $tables->{tsb};
    }
    
    return $joins;
}


sub process_ids {
    
    my ($request, $record) = @_;
    
    return unless $request->{block_hash}{extids};
    
    foreach my $k ( qw(timescale_no source_timescale_no base_timescale_no) )
    {
	$record->{$k} = generate_identifier('TSC', $record->{$k})
	    if defined $record->{$k} && $record->{$k} ne '';
    }
    
    foreach my $k ( qw(interval_no lower_no) )
    {
	$record->{$k} = generate_identifier('INT', $record->{$k})
	    if defined $record->{$k} && $record->{$k} ne '';
    }
    
    foreach my $k ( qw(bound_no base_no top_no range_no color_no refsource_no) )
    {
	$record->{$k} = generate_identifier('BND', $record->{$k})
	    if defined $record->{$k} && $record->{$k} ne '';
    }
    
    foreach my $k ( qw(reference_no) )
    {
	$record->{$k} = generate_identifier('REF', $record->{$k})
	    if defined $record->{$k} && $record->{$k} ne '';
    }
    
    # foreach my $k ( qw(authorizer_no enterer_no modifier_no) )
    # {
    # 	$record->{$k} = generate_identifier('PRS', $record->{$k})
    # 	    if defined $record->{$k} && $record->{$k} ne '';
    # }
}


sub process_timescale_ages {
    
    my ($request, $record) = @_;
    
    $record->{min_age} = precise_value($record->{min_age}, $record->{min_age_prec});
    $record->{max_age} = precise_value($record->{max_age}, $record->{max_age_prec});
    
    # $record->{min_age} =~ s/0+$// if defined $record->{min_age};
    # $record->{max_age} =~ s/0+$// if defined $record->{max_age};
    
    delete $record->{is_visible} unless $record->{is_visible};
    delete $record->{is_enterable} unless $record->{is_enterable};
    delete $record->{has_error} unless $record->{has_error};
    delete $record->{admin_lock} unless $record->{admin_lock};
}


sub process_bound_ages {
    
    my ($request, $record) = @_;

    if ( (defined $record->{int_early} && defined $record->{age} && $record->{int_early} ne $record->{age}) )
    {
	$record->{is_different} = 1;
    }
    
    $record->{age} = precise_value($record->{age}, $record->{age_prec});
    $record->{age_error} = precise_value($record->{age_error}, $record->{age_error_prec});
    $record->{fraction} = precise_value($record->{fraction}, $record->{fraction_prec});
    # $record->{fraction_error} = precise_value($record->{fraction_error_prec});
    
    delete $record->{is_modeled} unless $record->{is_modeled};
    delete $record->{is_spike} unless $record->{is_spike};
    delete $record->{has_error} unless $record->{has_error};
}


sub process_int_ages {

    my ($request, $record) = @_;
    
    if ( (defined $record->{int_early} && defined $record->{early_age} && $record->{int_early} ne $record->{early_age}) ||
	 (defined $record->{int_late} && defined $record->{late_age} && $record->{int_late} ne $record->{late_age}) )
    {
	$record->{is_different} = 1;
    }
    
    $record->{early_age} = precise_value($record->{early_age}, $record->{early_age_prec});
    $record->{late_age} = precise_value($record->{late_age}, $record->{late_age_prec});
}


sub precise_value {
    
    my ($value, $prec) = @_;
    
    if ( defined $prec && defined $value && $value =~ qr{ (\d+) (?: [.] (\d*) )? }xs )
    {
	my $whole = $1;
	my $point = $prec ? '.' : '';
	my $frac = $2 // '';
	my $len = length($frac);

	if ( $prec == 0 )
	{
	    return $whole;
	}
	
	elsif ( $len > $prec )
	{
	    $frac = substr($frac, 0, $prec);
	}
	
	elsif ( $len < $prec )
	{
	    $frac = $frac . '0' x ($prec - $len);
	}
	
	return "$whole$point$frac";
    }

    elsif ( defined $value && $value =~ qr{ ^ \d }xs )
    {
	$value =~ s/ [.]? 0+ $ //xs;
    }
    
    return $value;
}


1;
