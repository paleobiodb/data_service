#  
# PlaceData.pm
# 
# A role that returns information from the PaleoDB database about a single
# place or a list of places matching various criteria.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::PlaceData;

use HTTP::Validate qw(:validators);

use TableDefs qw($COLL_MATRIX $COLLECTIONS $LOCALITIES $WOF_PLACES $INTERVAL_DATA);

use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::IntervalData);


# initialize ( )
# 
# This routine is called by the DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # We start with the basic output block for places.
    
    $ds->define_block('1.2:places:basic' =>
	{ set => '*', code => \&process_identifiers },
	{ output => 'collection_no', com_name => 'oid' },
	    "For records that correspond to collections, this field will contain",
	    "the identifier of the collection.",
	{ output => 'locality_no', com_name => 'oid' },
	    "For records that correspond to localities, this field will contain",
	    "the identifier of the locality.",
	{ output => 'geoname_no', com_name => 'oid' },
	    "For records that correspond to geonames, this field will contain",
	    "the identifier of the geographic name.",
	{ output => 'loc_geoname_no', com_name => 'gid' },
	    "For records that correspond to localities, this field contains",
	    "the identifier of the geographic name record associated with the locality.",
	{ output => 'site_name', com_name => 'nam' },
	    "This field reports the collection name or locality name.",
	{ output => 'record_type', com_name => 'rtp' },
	    "If external identifiers are disabled, this field will specify whether",
	    "each record comes from a C<B<collection>> or a C<B<locality>>.",
	{ output => 'country_name', com_name => 'cou' },
	    "This field reports the name of the country in which the place is located.",
	{ output => 'region_name', com_name => 'stp' },
	    "This field reports the name of the region (usually a state or province) in which the place is located.",
	{ output => 'county_name', com_name => 'cny' },
	    "This field reports the name of the county or other administrative division in which the place is located.",
	{ output => 'locality_name', com_name => 'loc' },
	    "This field reports the specific name of the place.",
	{ output => 'verbatim_location', com_name => 'vbl' },
	    "This field reports the verbatim location, if any was specified.",
	{ output => 'early_interval', com_name => 'oei' },
	    "The specific geologic time range associated with the collection or locality (not necessarily a",
	    "standard interval), or the interval that begins the range if C<late_interval> is also given.",
	{ output => 'late_interval', com_name => 'oli' },
	    "The interval that ends the specific geologic time range associated with the collection",
	    "or locality.",
	{ output => 'early_age', com_name => 'eag', pbdb_name => 'max_ma', data_type => 'dec' },
	    "The early bound of the geologic time range associated with this collection or locality (in Ma)",
	{ output => 'late_age', com_name => 'lag', pbdb_name => 'min_ma', data_type => 'dec' },
	    "The late bound of the geologic time range associated with this collection or locality (in Ma)",
	{ output => 'stratgroup', com_name => 'sgr' },
	    "The stratigraphic group in which the collection is located, if known",
	{ output => 'formation', com_name => 'sfm' },
	    "The stratigraphic formation in which the collection is located, if known",
	{ output => 'member', com_name => 'smb' },
	    "The stratigraphic member in which the collection is located, if known");
    
    $ds->define_set('1.2:places:record_types' =>
	{ value => 'col' },
	    "A PBDB collection, which represents a geographic locality together with a collection event",
	{ value => 'loc' },
	    "A PBDB locality, which represents only a geographic locality",
	{ value => 'gnm' },
	    "A geographic name record, which may or may not be associated with a collection or locality");
    
    $ds->define_ruleset('1.2:places:specifier' =>
	{ param => 'place_id', valid => VALID_IDENTIFIER('PLC') },
	    "This parameter specifies a collection, locality, or geoname record",
	    "by its unique identifier. If this identifier is given as a number,",
	    "you must also use the B<C<type>> parameter to specify which kind of",
	    "record you mean.",
	{ optional => 'type', valid => '1.2:places:record_types' },
	    "Use this field to specify the record type you wish to retrieve.",
	    "This is not necessary if the identifier you provide to B<C<place_id>>",
	    "includes the type.");
    
    $ds->define_ruleset('1.2:places:single' =>
	{ require => '1.2:places:specifier' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
}


sub get_place {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $request->clean_param('place_id');
    
    die "400 Bad identifier '$id'\n" unless $id and $id =~ /^\d+$/;
    
    my $type = ref $id eq 'PBDB::ExtIdent' ? $id->type : $request->clean_param('type');
    
    die "400 You must specify a record type to retrieve" unless $type;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my ($fields, $join_list, $selector, $group_expr);
    my $quoted_id = $dbh->quote($id);
    
    if ( $type eq 'col' )
    {
	$fields = "collection_no, cc.collection_name as site_name, 'col' as record_type,
			cc.country as country_name, cc.state as region_name, cc.county as county_name,
			cc.geogcomments as locality_name, ei.interval_name as early_interval,
			li.interval_name as late_interval, c.early_age, c.late_age,
			if(cc.lithology2 <> '', concat(cc.lithology1,'/',cc.lithology2), cc.lithology1) as lithology";
	
	$join_list = "$COLL_MATRIX as c join $COLLECTIONS as cc using (collection_no)
		    left join $INTERVAL_DATA as ei on ei.interval_no = c.early_int_no
		    left join $INTERVAL_DATA as li on li.interval_no = c.late_int_no";
	
	$selector = "collection_no = $quoted_id";
	$group_expr = "collection_no";
    }
    
    elsif ( $type eq 'loc' )
    {
	$fields = "loc.locality_no, wof.wof_id as loc_geoname_no, 'loc' as record_type,
			wcountry.name_eng as country_name, wregion.name_eng as region_name,
			wcounty.name_eng as county_name, wlocal.name_eng as locality_name,
			loc.verbatim_location, ei.interval_name as early_interval,
			li.interval_name as late_interval, loc.grp as stratgroup,
			loc.formation, loc.member, loc.lithology";
	
	$join_list = "$LOCALITIES as loc join $WOF_PLACES as wof using (wof_id)
		    left join $WOF_PLACES as wcountry on wcountry.wof_id = wof.country
		    left join $WOF_PLACES as wregion on wregion.wof_id = wof.region
		    left join $WOF_PLACES as wcounty on wcounty.wof_id = wof.county
		    left join $WOF_PLACES as wlocal on wlocal.wof_id = wof.locality
		    left join $INTERVAL_DATA as ei on ei.interval_no = loc.early_int_no
		    left join $INTERVAL_DATA as li on li.interval_no = loc.late_int_no";
	
	$selector = "locality_no = $quoted_id";
	$group_expr = "locality_no";
    }
    
    elsif ( $type eq 'wof' )
    {
	$fields = "wof.wof_id as geoname_no, 'wof' as record_type,
			wcountry.name_eng as country_name, wregion.name_eng as region_name,
			wcounty.name_eng as county_name, wlocal.name_eng as locality_name";
	
	$join_list = "$WOF_PLACES as wof
		    left join $WOF_PLACES as wcountry on wcountry.wof_id = wof.country
		    left join $WOF_PLACES as wregion on wregion.wof_id = wof.region
		    left join $WOF_PLACES as wcounty on wcounty.wof_id = wof.county
		    left join $WOF_PLACES as wlocal on wlocal.wof_id = wof.locality";
	
	$selector = "wof.wof_id = $quoted_id";
	$group_expr = "wof.wof_id";
    }
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether we are supposed to produce external identifiers.
    
    $request->strict_check;
    $request->extid_check;
    
    # Figure out what information we need to determine access permissions.
    
    my ($access_filter, $access_fields);
    my $tables = { };
    
    if ( $type eq 'col' )
    {
	($access_filter, $access_fields) = $request->PB2::CollectionData::generateAccessFilter('cc', $tables);
    }
    
    $fields .= $access_fields if $access_fields;
    
    my $access_ok = $access_filter ? ", if($access_filter, 1, 0) as access_ok" : "";
    
    $request->delete_output_field('permissions') unless $access_fields;
    
    # Determine the necessary joins.
    
    # my ($join_list) = $request->generateJoinList('c', $request->tables_hash);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $fields $access_ok
	FROM $join_list
        WHERE $selector
	GROUP BY $group_expr";
    
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


sub list_places {



}


sub process_identifiers {

    my ($request, $record) = @_;
    
    return unless $request->{block_hash}{extids};
    
    foreach my $f ( qw(collection_no) )
    {
	$record->{$f} = generate_identifier('COL', $record->{$f}) if defined $record->{$f};
    }
    
    foreach my $f ( qw(locality_no) )
    {
	$record->{$f} = generate_identifier('LOC', $record->{$f}) if defined $record->{$f};
    }
    
    foreach my $f ( qw(geoname_no loc_geoname_no) )
    {
	$record->{$f} = generate_identifier('WOF', $record->{$f}) if defined $record->{$f};
    }
}

1;
