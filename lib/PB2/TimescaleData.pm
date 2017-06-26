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

use TableDefs qw($TIMESCALE_DATA $TIMESCALE_INTS $TIMESCALE_BOUNDS);

use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::ReferenceData);

our ($INTERVAL_ATTRS_ABSOLUTE, $INTERVAL_ATTRS_RELATIVE);

# initialize ( )
# 
# This routine is called by the DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # We start with the basic output blocks for timescales, intervals, and bounds.
    
    $ds->define_set('1.2:timescales:types' =>
	{ value => 'eon' },
	{ value => 'era' },
	{ value => 'period' },
	{ value => 'epoch' },
	{ value => 'stage' },
	{ value => 'substage' },
	{ value => 'zone' },
	{ value => 'chron' }, "synonym for C<B<zone>>",
	{ value => 'multi' }, "used for timescales containing multiple interval types",
	{ value => 'other' });
    
    $ds->define_block('1.2:timescales:basic' =>
	{ select => [ 'ts.timescale_no', 'ts.timescale_name',
		      'ts.timescale_extent', 'ts.timescale_taxon', 'ts.timescale_type',
		      'ts.source_timescale_no', 'ts.max_age', 'ts.min_age',
		      'ts.is_active', 'ts.authority_level', 'ts.reference_no' ],
	  tables => 'ts' },
	{ set => '*', code => \&process_ids },
	{ output => 'timescale_no', com_name => 'oid' },
	    "The unique identifier of this timescale in the database",
	{ output => 'record_type', com_name => 'typ', value => $IDP{TSC} },
	    "The type of this object: C<$IDP{TSC}> for a timescale. This",
	    "field will only appear if the object identifier is",
	    "being returned as a number.",
	{ output => 'timescale_name', com_name => 'nam' },
	    "The name of this timescale",
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
	    $ds->document_set('1.2:timescales:types'),
	{ output => 'source_timescale_no', com_name => 'bid' },
	    "Identifier of the timescale from which this one was derived, if any",
	{ output => 'min_age', com_name => 'lag' },
	    "The late bound of this timescale, in Ma",
	{ output => 'max_age', com_name => 'eag' },
	    "The early bound of this timescale, in Ma",
	{ output => 'is_active', com_name => 'vsb' },
	    "True if this timescale is active for general use, false otherwise.",
	{ output => 'authority_level', com_name => 'ath' },
	    "If non-zero, this represents the priority used when an interval",
	    "appears in more than one timescale with different age bounds. The",
	    "bounds defined by the timescale with the highest value for this",
	    "attribute are considered to be definitive.",
	{ output => 'reference_no', com_name => 'rid' },
	    "The unique identifier of the main bibliographic reference for this",
	    "timescale, if any.");
    
    $ds->define_output_map('1.2:timescales:optional_basic' =>
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifier of the person who authorized this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The name of the person who authorized this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the timescale record");
    
    $ds->define_block('1.2:timescales:interval' =>
	{ select => [ 'tsi.interval_no', 'tsi.interval_name', 'tsi.abbrev',
		      'INTERVAL_ATTRS' ],
	  tables => 'tsi' },
	{ set => '*', code => \&process_ids },
	{ output => 'interval_no', com_name => 'oid' },
	    "The unique identifier of this interval in the database",
	{ output => 'record_type', com_name => 'typ', value => $IDP{INT} },
	    "The type of this object: C<$IDP{INT}> for an interval. This",
	    "field will only appear if the object identifier is",
	    "being returned as a number.",
	{ output => 'timescale_no', com_name => 'sid' },
	    "The unique identifier of the timescale in which this interval is contained.",
	{ output => 'interval_name', com_name => 'nam' },
	    "The name of the interval",
	{ output => 'abbrev', com_name => 'abr' },
	    "The standard abbreviation for the interval, if any",
	{ output => 'late_age', com_name => 'lag' },
	    "The late age bound for this interval, according to the",
	    "most authoritative timescale in which it is contained",
	{ output => 'early_age', com_name => 'eag' },
	    "The early age bound for this interval, according to the",
	    "most authoritative timescale in which it is contained",
	{ output => 'is_error', com_name => 'err' },
	    "True if this boundary is inconsistent with the other boundaries in",
	    "its timescale, for example in overlapping with another boundary.",
	{ output => 'is_locked', com_name => 'lck' },
	    "True if this boundary is locked. If the age of the base boundary",
	    "and/or range boundary is changed, then the age of this boundary will be",
	    "updated only if it is not locked. The same is true of color",
	    "and bibliographic reference identifier.",
	{ output => 'is_different', com_name => 'dfa' },
	    "True if this boundary is locked and its age is different from",
	    "the value that would be computed from the base and/or range boundary,",
	    "or if its color or bibliographic reference are different from those",
	    "indicated by its source boundaries.",
	{ output => 'color', com_name => 'col' },
	    "The standard color for this interval, if any");
    
    $ds->define_block('1.2:timescales:interval_desc' =>
	{ select => [ 'tsb.interval_extent', 'tsb.interval_taxon', 'tsb.interval_type' ], tables => 'tsb' },
	{ output => 'interval_extent', com_name => 'iex' },
	    "The geographic extent of the timescale, if any",
	{ output => 'interval_taxon', com_name => 'itx' },
	    "The taxonomic group on which the timescale is based, if any (e.g. ammonite, mammal)",
	{ output => 'interval_type', com_name => 'itp' },
	    "The type of interval contained in the timescale, e.g. stage, epoch, zone");
    
    $ds->define_block('1.2:timescales:interval_scale' =>
	{ select => [ 'ts.timescale_name' ], tables => 'ts' },
	{ output => 'timescale_name', com_name => 'tsn' },
	    "The name of the timescale in which this interval or bound is contained.");
    
    $INTERVAL_ATTRS_RELATIVE = "tsb.age as early_age, tsbu.age as late_age, tsb.color, " .
	"tsb.is_error, tsb.is_locked, tsb.is_different, tsb.timescale_no";
    $INTERVAL_ATTRS_ABSOLUTE = "tsi.early_age, tsi.late_age, tsi.color";
    
    $ds->define_output_map('1.2:timescales:optional_interval' =>
	{ value => 'desc', maps_to => '1.2:timescales:interval_desc' },
	    "Descriptive attributes for this interval",
	{ value => 'scale', maps_to => '1.2:timescales:interval_scale' },
	    "Descriptive attributes for the timescale",
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifier of the person who authorized this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The name of the person who authorized this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the timescale record");
    
    $ds->define_set('1.2:timescales:bound_types' =>
	{ value => 'absolute' },
	    "A boundary which is specified absolutely, in Ma",
	{ value => 'spike' },
	    "An absolute boundary which is fixed by a GSSP",
	{ value => 'same' },
	    "A boundary which has the same value as some other base boundary. For",
	    "example, the beginning of the Triassic might be defined by reference to",
	    "the beginning of the Mesozoic.",
	{ value => 'percent' },
	    "A boundary which is fixed between two other boundaries, a base and a top, as a",
	    "percentage of the span between them.",
	{ value => 'offset' },
	    "A boundary which is defined by a specified offset in millions of years",
	    "with respect to some other base boundary.");
    
    $ds->define_block('1.2:timescales:bound' =>
	{ select => [ 'tsb.bound_no', 'tsb.age', 'tsb.age_error', 'tsb.bound_type', 
		      'tsb.timescale_no', 'coalesce(tsb.interval_type, ts.timescale_type) as interval_type',
		      'tsb.offset', 'tsb.offset_error', 'tsb.lower_no', 'tsb.interval_no',
		      'tsb.base_no', 'tsb.range_no', 'tsb.color_no', 'tsb.is_error',
		      'tsb.is_locked', 'tsb.is_different', 'tsb.color', 'tsb.reference_no',
		      'tsil.interval_name as lower_name', 'tsi.interval_name' ],
	  tables => [ 'tsb', 'tsi', 'tsil' ] },
	{ set => '*', code => \&process_ids },
	{ output => 'bound_no', com_name => 'oid' },
	    "The unique identifier of this boundary in the database",
	{ output => 'record_type', com_name => 'typ', value => $IDP{BND} },
	    "The type of this object: C<$IDP{BND}> for an interval boundary. This",
	    "field will only appear if the object identifier is",
	    "being returned as a number.",
	{ output => 'timescale_no', com_name => 'sid' },
	    "The identifier of the timescale containing this bound",
	{ output => 'age', com_name => 'age' },
	    "The age at which this boundary is fixed or calculated, in Ma",
	{ output => 'age_error', com_name => 'ger' },
	    "The error (+/-) associated with the age",
	{ output => 'interval_type', com_name => 'typ' },
	    "The type of the upper interval, if any",
	{ output => 'bound_type', com_name => 'btp' },
	    "The boundary type, which will be one of the following:",
	    $ds->document_set('1.2:timescales:bound_types'),
	{ output => 'offset', com_name => 'ofs' },
	    "The offset or percentage, depending on the boundary type,",
	    "by which this boundary differs from the base boundary.",
	{ output => 'offset_error', com_name => 'oer' },
	    "The error (+/-) associated with the offset or percentage",
	{ output => 'interval_no', com_name => 'iid' },
	    "The identifier of the upper interval bounded by this boundary.",
	    "If this field is empty, then the boundary lies at the top of",
	    "its timescale.",
	{ output => 'lower_no', com_name => 'lid' },
	    "The identifier of the lower interval bounded by this boundary.",
	    "If this field is empty, then the boundary lies at the bottom of",
	    "its timescale.",
	{ output => 'interval_name', com_name => 'inm' },
	    "The name of the upper interval bounded by this boundary.",
	{ output => 'lower_name', com_name => 'lnm' },
	    "The name of the lower interval bounded by this boundary.",
	{ output => 'range_no', com_name => 'tid' },
	    "If the boundary type is 'percent', this field specifies the other",
	    "boundary of the pair between which this boundary lies.",
	{ output => 'base_no', com_name => 'bid' },
	    "If the boundary type is 'reference', 'percent', or 'offset', this field specifies",
	    "the identifier of the base boundary with respect to which",
	    "this boundary is defined.",
	{ output => 'color_no', com_name => 'cid' },
	    "If this field is not empty, it specifies the identifier of a boundary",
	    "from which the color for this boundary is taken. Note that this might",
	    "be different than the base boundary.",
	{ output => 'is_error', com_name => 'err' },
	    "True if this boundary is inconsistent with the other boundaries in",
	    "its timescale, for example in overlapping with another boundary.",
	{ output => 'is_locked', com_name => 'lck' },
	    "True if this boundary is locked. If the age of the base boundary",
	    "and/or range boundary is changed, then the age of this boundary will be",
	    "updated only if it is not locked. The same is true of color",
	    "and bibliographic reference identifier.",
	{ output => 'is_different', com_name => 'dfa' },
	    "True if this boundary is locked and its age is different from",
	    "the value that would be computed from the base and/or range boundary,",
	    "or if its color or bibliographic reference are different from those",
	    "indicated by its source boundaries.",
	{ output => 'color', com_name => 'col' },
	    "The standard color (if any) that should be assigned to the upper",
	    "interval associated with this boundary.",
	{ output => 'reference_no', com_name => 'rid' },
	    "The identifier of the bibliographic reference for this",
	    "boundary, if any. If this field is empty, the reference",
	    "should be taken to be the main reference for the timescale",
	    "in which this boundary is contained.");
    
    $ds->define_output_map('1.2:timescales:optional_bound' =>
	{ value => 'derived', maps_to => '1.2:timescales:derived' },
	    "The values for age, color, and bibliographic reference",
	    "as derived from the base boundary and color source",
	    "boundary. These values will only be different from",
	    "the actual values for this boundary if this boundary",
	    "is locked and the source values have changed.",
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifiers of the people who authorized, entered, and modified this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The names of the people who authorized, entered, and modified this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the boundary record");
    
    $ds->define_block('1.2:timescales:derived' =>
	{ output => 'derived_age', com_name => 'dag' },
	    "The age of this boundary as computed from the base and/or",
	    "range boundaries, and from the offset or percentage (if any)",
	    "specified for htis boundary. This will differ from the actual",
	    "age only if this boundary is locked and the ages of the other",
	    "boundaries have changed. And the same is true of the following",
	    "fields.",
	{ output => 'derived_age_error', com_name => 'dgr' },
	    "The age error of this boundary as computed from the base and/or",
	    "range boundaries.",
	{ output => 'derived_color', com_name => 'dco' },
	    "The color of this boundary as derived from the color source",
	    "boundary.",
	{ output => 'derived_reference_no', com_name => 'dri' },
	    "The identifier of the bibliographic reference for this boundary",
	    "derived from the base boundary (if any).");
    
    $ds->define_ruleset('1.2:timescales:specifier' =>
	{ param => 'timescale_id', valid => VALID_IDENTIFIER('TSC'), alias => 'id' },
	    "The unique identifier of the timescale you wish to retrieve (REQIRED).",
	    "You may instead use the parameter name B<C<id>>.");
    
    $ds->define_ruleset('1.2:timescales:int_specifier' =>
	{ param => 'interval_id', valid => VALID_IDENTIFIER('INT'), alias => 'id' },
	    "The unique identifier of the interval you wish to retrieve (REQIRED).",
	    "You may instead use the parameter name B<C<id>>.",
	{ optional => 'timescale_id', valid => VALID_IDENTIFIER('TSC') },
	    "The unique identifier of a timescale in which this interval is",
	    "mentioned. The reported interval attributes, including age bounds, will be taken from",
	    "the specified timescale.");
    
    $ds->define_ruleset('1.2:timescales:bound_specifier' =>
	{ param => 'bound_id', valid => VALID_IDENTIFIER('BND'), alias => 'id' },
	    "The unique identifier of the bound you wish to retrieve (REQIRED).",
	    "You may instead use the parameter name B<C<id>>.");
    
    $ds->define_ruleset('1.2:timescales:common_selector' =>
	{ param => 'type', valid => '1.2:timescales:types', list => ',', bad_value => '_' },
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
	{ param => 'timescale_id', valid => VALID_IDENTIFIER('TSC'), list => ',' },
	    "Return only timescales with the specified identifier(s), or intervals",
	    "or bounds contained within them. You may specify more",
	    "than one value, separated by commas.",
	{ param => 'interval_name', valid => ANY_VALUE, list => ',' },
	    "Return only intervals whose name one of the specified string(s),
	     or timescales or bounds that contain or refer to them.",
	{ param => 'interval_match', valid => ANY_VALUE, list => ',' },
	    "Return only intervals whose name matches the specified value(s),",
	    "or timescales or bounds that contain or refer to them.",
	    "You may specify more than one value separated by commas,",
	    "and you may use the wildcards C<%> and C<_>.",
	{ param => 'interval_id', valid => VALID_IDENTIFIER('INT'), list => ',' },
	    "Return only intervals with the specified identifier(s), or timescales",
	    "or bounds that contain or refer to them. You may specify more",
	    "than one value, separated by commas.",
	{ param => 'bound_id', valid => VALID_IDENTIFIER('BND'), list => ',' },
	    "Return only bounds with the specified identifier(s), or timescales",
	    "or intervals that contain or refer to them. You may specify more",
	    "than one value, separated by commas.",
	{ param => 'max_ma', valid => DECI_VALUE(0) },
	    "Return only timescales, intervals, or bounds whose lower bound is at most this old.",
	{ param => 'min_ma', valid => DECI_VALUE(0) },
	    "Return only timescales, intervals, or bounds whose upper bound is at least this old.");
    
    $ds->define_ruleset('1.2:timescales:all_records' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "Select all timescales (or intervals, or boundaries) entered in the database,",
	    "subject to any other parameters you may specify.",
	    "This parameter does not require any value.");
    
    $ds->define_ruleset('1.2:timescales:single' =>
	{ require => '1.2:timescales:specifier' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_basic' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:timescales:list' =>
	{ allow => '1.2:timescales:all_records' },
	{ allow => '1.2:timescales:common_selector' },
	{ require_one => ['1.2:timescales:all_records', '1.2:timescales:common_selector'] },
	{ allow => '1.2:common:select_crmod' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_basic' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:intervals2:single' =>
	{ require => '1.2:timescales:int_specifier' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_interval' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");

    $ds->define_ruleset('1.2:intervals2:list' =>
	{ allow => '1.2:timescales:all_records' },
	{ optional => 'absolute', valid => 'FLAG_VALUE' },
	    "Return the age bounds for each interval computed from the highest priority",
	    "timescale.",
	{ allow => '1.2:timescales:common_selector' },
	{ require_one => ['1.2:timescales:all_records', '1.2:timescales:common_selector'] },
	{ allow => '1.2:common:select_crmod' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_interval' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:bounds:single' =>
	{ require => '1.2:timescales:bound_specifier' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_bound' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:bounds:list' =>
	{ allow => '1.2:timescales:all_records' },
	{ allow => '1.2:timescales:common_selector' },
	{ require_one => ['1.2:timescales:all_records', '1.2:timescales:common_selector'] },
	{ allow => '1.2:common:select_crmod' },
    	{ optional => 'SPECIAL(show)', valid => '1.2:timescales:optional_bound' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
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
    
    my ($id, $timescale_id, $mt);
    
    if ( $type eq 'timescales' )
    {
	$id = $request->clean_param('timescale_id');
	$mt = 'ts';
    }
    
    elsif ( $type eq 'intervals' )
    {
	$id = $request->clean_param('interval_id');
	$mt = 'tsi';
	
	$timescale_id = $request->clean_param('timescale_id');
    }
    
    elsif ( $type eq 'bounds' )
    {
	$id = $request->clean_param('bound_id');
	$mt = 'tsb';
    }
    
    die "400 Bad identifier '$id'\n" unless $id and $id =~ /^\d+$/;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => $mt, cd => ($mt eq 'tsi' ? 'tsb' : $mt) );
    
    my @fields = $request->select_list;
    
    my $fields = join(', ', @fields);
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also respond properly to the 'extid' parameter, if given.
    
    $request->strict_check;
    $request->extid_check;
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list($mt, $request->tables_hash);
    
    # Generate the main query. We will already have thrown an error above if $type is not one of
    # the following values.
    
    if ( $type eq 'timescales' )
    {
	$request->{main_sql} = "
	SELECT $fields
	FROM $TIMESCALE_DATA as ts $join_list
        WHERE ts.timescale_no = $id
	GROUP BY ts.timescale_no";
    }
    
    elsif ( $type eq 'bounds' )
    {
	$request->{main_sql} = "
	SELECT $fields
	FROM $TIMESCALE_BOUNDS as tsb $join_list
        WHERE tsb.bound_no = $id
	GROUP BY tsb.bound_no";
    }
    
    elsif ( $type eq 'intervals' && $timescale_id )
    {
	$fields =~ s/INTERVAL_ATTRS/$INTERVAL_ATTRS_RELATIVE/;
	
	$request->{main_sql} = "
	SELECT $fields
	FROM $TIMESCALE_INTS as tsi
	    join $TIMESCALE_BOUNDS as tsb on (tsb.interval_no = tsi.interval_no)
	    left join $TIMESCALE_BOUNDS as tsbu on (tsbu.lower_no = tsi.interval_no and tsbu.timescale_no = tsb.timescale_no)
	WHERE tsi.interval_no = $id and tsb.timescale_no = $timescale_id
	GROUP BY tsi.interval_no, tsb.timescale_no";
    }
    
    elsif ( $type eq 'intervals' )
    {
	$fields =~ s/INTERVAL_ATTRS/$INTERVAL_ATTRS_ABSOLUTE/;
	
	$request->{main_sql} = "
	SELECT $fields
	FROM $TIMESCALE_INTS as tsi $join_list
        WHERE tsi.interval_no = $id
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
	$index = 'ts.timescale_no';
    }
    
    elsif ( $type eq 'intervals' )
    {
	$mt = 'tsi';
	$index = 'tsi.interval_no';
    }
    
    elsif ( $type eq 'bounds' )
    {
	$mt = 'tsb';
	$index = 'tsb.bound_no';
    }
    
    die "unknown record type '$type'\n" unless $mt;
    
    $request->substitute_select( mt => $mt, cd => ($mt eq 'tsi' ? 'tsb' : $mt) );
    
    # Construct a list of filter expressions that must be added to the query
    # in order to select the proper result set.
    
    my @filters = $request->generate_timescale_filters($mt, $type, $tables);
    push @filters, $request->generate_common_filters( { bare => $mt } );
    
    # If we are querying for intervals, figure out if we are doing so absolutely or relative to
    # the timescales in which they are contained.
    
    if ( $type eq 'intervals' && ! $request->clean_param('absolute') )
    {
	$tables->{tsb} = 1;
	$relative = 1;
    }
    
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
	$order_expr ||= 'order by ts.min_age';
	
	$request->{main_sql} = "
	SELECT $calc $fields
	FROM $TIMESCALE_DATA as ts $join_list
        WHERE $filter_string
	GROUP BY $index $order_expr";
    }
    
    elsif ( $type eq 'bounds' )
    {
	$order_expr ||= 'order by age';
	
	$request->{main_sql} = "
	SELECT $calc $fields
	FROM $TIMESCALE_BOUNDS as tsb $join_list
        WHERE $filter_string
	GROUP BY $index $order_expr";
    }
    
    elsif ( $type eq 'intervals' && $relative )
    {
	$order_expr ||= 'order by early_age';
	
	$fields =~ s/INTERVAL_ATTRS/$INTERVAL_ATTRS_RELATIVE/;
	
	$request->{main_sql} = "
	SELECT $calc $fields
	FROM $TIMESCALE_INTS as tsi $join_list
	WHERE $filter_string
	GROUP BY tsi.interval_no, tsb.timescale_no $order_expr";
    }
    
    elsif ( $type eq 'intervals' )
    {
	$order_expr ||= 'order by early_age';
	
	$fields =~ s/INTERVAL_ATTRS/$INTERVAL_ATTRS_ABSOLUTE/;
	
	$request->{main_sql} = "
	SELECT $calc $fields
	FROM $TIMESCALE_INTS as tsi $join_list
        WHERE $filter_string
	GROUP BY $index $order_expr";
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
	my $id_list = $request->check_values($dbh, \@timescale_nos, 'timescale_no', $TIMESCALE_DATA, 
					     "Unknown timescale 'tsc:%'");
	
	$request->add_warning("no valid timescale identifiers were given") if $id_list eq '-1';
	
	if ( $type eq 'timescales' )
	{
	    push @filters, "ts.timescale_no in ($id_list)";
	}
	
	else
	{
	    push @filters, "tsb.timescale_no in ($id_list)";
	    $tables->{tsb} = 1;
	}
    }
    
    if ( my @bound_nos = $request->clean_param_list('bound_id') )
    {
	my $id_list = $request->check_values($dbh, \@bound_nos, 'bound_no', $TIMESCALE_BOUNDS, 
					     "Unknown timescale bound 'bnd:%'");
	
	$request->add_warning("no valid bound identifiers were given") if $id_list eq '-1';
	push @filters, "tsb.bound_no in ($id_list)";
	
	$tables->{tsb} = 1;
    }
    
    if ( my @interval_nos = $request->clean_param_list('interval_id') )
    {
	my $id_list = $request->check_values($dbh, \@interval_nos, 'interval_no', $TIMESCALE_INTS, 
					     "Unknown timescale interval 'int:%'");
	
	$request->add_warning("no valid interval identifiers were given") if $id_list eq '-1';
	
	if ( $type eq 'intervals' )
	{
	    push @filters, "tsi.interval_no in ($id_list)";
	}
	
	else
	{	
	    push @filters, "tsb.interval_no in ($id_list)";
	    $tables->{tsb} = 1;
	}
    }
    
    # Check for name matches
    
    if ( my @names = $request->clean_param_list('interval_name') )
    {
	push @filters, $request->generate_match_like($dbh, 'tsi.interval_name', \@names);
	$tables->{tsi} = 1;
    }
    
    if ( my @names = $request->clean_param_list('interval_match') )
    {
	push @filters, $request->generate_match_regex($dbh, 'tsi.interval_name', \@names);
	$tables->{tsi} = 1;
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
	
	else
	{
	    push @filters, "tsb.age <= $max_ma";
	    $tables->{tsb} = 1;
	}
    }
    
    if ( my $min_ma = $request->clean_param('min_ma') )
    {
	if ( $type eq 'timescales' )
	{
	    push @filters, "ts.min_age >= $min_ma";
	}
	
	elsif ( $type eq 'intervals' )
	{
	    push @filters, "tsbu.age >= $min_ma";
	    $tables->{tsbu} = 1;
	}
	
	else
	{
	    push @filters, "tsb.age >= $min_ma";
	    $tables->{tsb} = 1;
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
	    $field = "tsb.interval_type";
	    $tables->{tsb} = 1;
	}
	
	foreach my $t ( @types )
	{
	    $t = 'zone' if lc $t eq 'chron';
	}
	
	push @filters, $request->generate_match_list($dbh, $field, \@types);
    }
    
    if ( my @names = $request->clean_param_list('taxon') )
    {
	my $field;
	
	if ( $type eq 'timescales' )
	{
	    $field = "ts.timescale_taxon";
	}
	
	else
	{
	    $field = "tsb.interval_taxon";
	    $tables->{tsb} = 1;
	}
	
	push @filters, $request->generate_match_list($dbh, $field, \@names);
    }
    
    if ( my @extents = $request->clean_param_list('extent') )
    {
	my $field;
	
	if ( $type eq 'timescales' )
	{
	    $field = "ts.timescale_extent";
	}
	
	else
	{
	    $field = "tsb.interval_extent";
	    $tables->{tsb} = 1;
	}
	
	foreach my $e ( @extents )
	{
	    $e = 'global|international' if $e eq 'global';
	}
	
	push @filters, $request->generate_match_regex($dbh, $field, \@extents);
    }
    
    return @filters;
}


sub generate_join_list {
    
    my ($request, $mt, $tables) = @_;
    
    my $joins = '';
    
    if ( $mt eq 'ts' )
    {
	$joins .= "\tjoin $TIMESCALE_BOUNDS as tsb using (timescale_no)\n"
	    if $tables->{tsb} || $tables->{tsi};
	$joins .= "\tjoin $TIMESCALE_INTS as tsi on tsi.interval_no = tsb.interval_no\n"
	    if $tables->{tsi};
    }
    
    elsif ( $mt eq 'tsb' )
    {
	$joins .= "\tjoin $TIMESCALE_DATA as ts using (timescale_no)\n";
	$joins .= "\tleft join $TIMESCALE_INTS as tsi on tsi.interval_no = tsb.interval_no\n";
	$joins .= "\tleft join $TIMESCALE_INTS as tsil on tsil.interval_no = tsb.lower_no\n"
	    if $tables->{tsil};
    }
    
    elsif ( $mt eq 'tsi' )
    {
	$joins .= "\tjoin $TIMESCALE_BOUNDS as tsb on tsb.interval_no = tsi.interval_no\n"
	    if $tables->{tsb} || $tables->{ts};
	$joins .= "\tjoin $TIMESCALE_DATA as ts using (timescale_no)\n"
	    if $tables->{ts};
	$joins .= "\tleft join $TIMESCALE_BOUNDS as tsbu on tsbu.lower_no = tsi.interval_no and tsbu.timescale_no = tsb.timescale_no\n"
	    if $tables->{tsb};
    }
    
    return $joins;
}


sub process_ids {
    
    my ($request, $record) = @_;
    
    return unless $request->{block_hash}{extids};
    
    # my $make_ids = $request->clean_param('extids');
    # $make_ids = 1 if ! $request->param_given('extids') && $request->output_vocab eq 'com';
    
    # return unless $make_ids;
        
    # $request->delete_output_field('record_type');
    
    foreach my $k ( qw(timescale_no source_timescale_no) )
    {
	$record->{$k} = generate_identifier('TSC', $record->{$k})
	    if defined $record->{$k} && $record->{$k} ne '';
    }
    
    foreach my $k ( qw(interval_no lower_no) )
    {
	$record->{$k} = generate_identifier('INT', $record->{$k})
	    if defined $record->{$k} && $record->{$k} ne '';
    }
    
    foreach my $k ( qw(bound_no base_no range_no color_no) )
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

1;
