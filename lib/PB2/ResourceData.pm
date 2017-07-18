#  
# ResourceData
# 
# A role that returns information from the PaleoDB database about a single
# educational resource or a category of educational resources.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::ResourceData;

use HTTP::Validate qw(:validators);

use TableDefs qw($RESOURCE_DATA $RESOURCE_QUEUE $RESOURCE_IMAGES $RESOURCE_TAG_NAMES $RESOURCE_TAGS);

use ExternalIdent qw(VALID_IDENTIFIER generate_identifier);
use PB2::TableData qw(complete_output_block);

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::Authentication);

our ($RESOURCE_ACTIVE, $RESOURCE_IDFIELD, $RESOURCE_IMG_DIR, $RESOURCE_IMG_PATH);

our (%TAG_VALUE);

sub initialize {

    my ($class, $ds) = @_;
    
    # Value sets
    
    $ds->define_set('1.2:eduresources:status' =>
	{ value => 'active' },
	    "An active resource is one that is visible on the Resources page",
	    "of this website.",
	{ value => 'changes' },
	    "A resource with this status is active, but changes have been made to the",
	    "record in the queue table that have not been copied to the active table.",
	    "The status of an active record is set to this value automatically if any",
	    "changes are made to it.",
	{ value => 'pending' },
	    "A pending resource is one that is not currently active on the Resources page,",
	    "and has not yet been reviewed for possible activation.",
	{ value => 'inactive' },
	    "An inactive resource is one that has been reviewed, and the reviewer did",
	    "not choose to activate it.");
    
    $ds->define_set('1.2:eduresources:enterer' =>
	{ value => 'me' },
	    "Select only records that were entered by the current requestor.",
	{ value => 'auth' },
	    "Select only records that were entered by the anyone in the current",
	    "requestor's authorizer group.");
    
    # Optional output
    
    $ds->define_output_map('1.2:eduresources:optional_output' =>
	{ value => 'image', maps_to => '1.2:eduresources:image_data' },
	    "The image data, if any, associated with this record.",
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the collection record");
    
    # Output blocks
    
    $ds->define_block('1.2:eduresources:basic' =>
	{ output => 'eduresource_no', com_name => 'oid' },
	    "The unique identifier of this record in the database.",
	{ set => '*', code => \&process_record},
	{ set => 'eduresource_no', code => sub {
		    my ($request, $value) = @_;
		    return $value unless $request->{block_hash}{extids};
		    return generate_identifier('EDR', $value);
		} },
	{ output => 'status', com_name => 'sta' },
	    "The status will be one of the following codes:",
	    $ds->document_set('1.2:eduresources:status'));
    
    $ds->define_block('1.2:eduresources:image_data' =>
	{ select => 'edi.image_data', tables => 'edi' },
	{ output => 'image_data', com_name => 'image_data' },
	    "The image data, if any, associated with this record. If it was properly",
	    "uploaded, it should be encoded in base64.");
    
    # Rulesets
    
    $ds->define_ruleset('1.2:eduresources:specifier' =>
	{ param => 'eduresource_id', valid => VALID_IDENTIFIER('EDR'), alias => 'id' },
	    "Return the educational resource record corresponding to the specified identifier");
    
    $ds->define_ruleset('1.2:eduresources:selector' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "If this parameter is specified, then all records in the database",
	    "will be returedn, subject to any other parameters that are also specified.",
	{ param => 'eduresource_id', valid => VALID_IDENTIFIER('EDR'), alias => 'id', list => ',' },
	    "Return the educational resource record(s) corresponding to the specified",
	    "identifier(s). You can specify more than one, as a comma-separated list.",
	{ param => 'status', valid => '1.2:eduresources:status', list => ',' },
	    "Return only resource records with the specified status or statuses.",
	{ param => 'enterer', valid => '1.2:eduresources:enterer' },
	    "If this parameter is specified, then only resources created by the requestor",
	    "are shown. Accepted values include:",
	{ param => 'title', valid => ANY_VALUE },
	    "Return only records with the given word or phrase in the title",
	{ param => 'tag', valid => ANY_VALUE, list => ',' },
	    "Return only records associated with the specified tag or tags. You can",
	    "specify either the integer identifiers or the tag names.",
	{ param => 'keyword', valid => ANY_VALUE, list => ',' },
	    "Return only records associated with the given keyword(s). You can",
	    "specify more than one, as a comma-separated list. Only records",
	    "with all of the listed keywords will be returned.");
    
    $ds->define_ruleset('1.2:eduresources:single' =>
	{ require => '1.2:eduresources:specifier' },
	{ optional => 'active', valid => FLAG_VALUE },
	    "If this parameter is included, then the active version of the record is",
	    "returned if one exists, and a 'not found' error otherwise. If this parameter",
	    "is not included, or is included with the value C<B<no>>, then the master",
	    "version of the record is returned (if one exists).",
	{ optional => 'SPECIAL(show)', valid => '1.2:eduresources:optional_output' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:eduresources:list' =>
	{ require => '1.2:eduresources:selector' },
	{ optional => 'active', valid => FLAG_VALUE },
	    "If this parameter is included, then the active version of the record is",
	    "returned if one exists, and a 'not found' error otherwise. If this parameter",
	    "is not included, or is included with the value C<B<no>>, then the master",
	    "version of the record is returned (if one exists).",
	{ optional => 'SPECIAL(show)', valid => '1.2:eduresources:optional_output' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:eduresources:active' =>
	{ allow => '1.2:eduresources:selector' },
	{ optional => 'SPECIAL(show)', valid => '1.2:eduresources:optional_output' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $RESOURCE_ACTIVE = $ds->config_value('eduresources_active') || $RESOURCE_DATA;
    $RESOURCE_IDFIELD = $ds->config_value('eduresources_idfield') || 'id';
    $RESOURCE_IMG_DIR = $ds->config_value('eduresources_img_dir');
    $RESOURCE_IMG_PATH = $ds->config_value('eduresources_img_path');
    
    die "You must provide a configuration value for 'eduresources_active' and 'eduresources_tags'"
	unless $RESOURCE_ACTIVE && $RESOURCE_TAGS;

    my $dbh = $ds->get_connection;
    
    complete_output_block($ds, $dbh, '1.2:eduresources:basic', $RESOURCE_QUEUE);
}


sub get_resource {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $request->clean_param('eduresource_id');
    
    die "400 Bad identifier '$id'\n" unless $id and $id =~ /^\d+$/;
    
    my $active = $request->clean_param('active');
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'edr', cd => 'edr' );
    
    my $tables = $request->tables_hash;
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether we should generate external identifiers.
    
    $request->strict_check;
    $request->extid_check;
    
    # Figure out what information we need to determine access permissions.
    
    # $$$
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list($request->tables_hash, $active);
    
    my $extra_fields = $request->select_string;
    $extra_fields = ", $extra_fields" if $extra_fields;
    
    # Generate the main query.
    
    if ( $active )
    {
	$request->{main_sql} = "
	SELECT edr.* $extra_fields FROM $RESOURCE_ACTIVE as edr $join_list
        WHERE edr.eduresource_no = $id
	GROUP BY edr.eduresource_no";
    }
    
    else
    {
	$request->{main_sql} = "
	SELECT edr.*, act.image as active_image, edi.eduresource_no as has_image $extra_fields
	FROM $RESOURCE_QUEUE as edr
		left join $RESOURCE_IMAGES as edi using (eduresource_no)
		left join $RESOURCE_ACTIVE as act on edr.eduresource_no = act.$RESOURCE_IDFIELD
		$join_list
        WHERE edr.eduresource_no = $id
	GROUP BY edr.eduresource_no";
    }
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    # Return an error response if we couldn't retrieve the record.
    
    die "404 Not found\n" unless $request->{main_record};
    
    # Return an error response if we could retrieve the record but the user is not authorized to
    # access it.  Any specimen not tied to an occurrence record is public by definition.
    
    # die $request->exception(403, "Access denied") 
    # 	unless $request->{main_record}{access_ok};
    
    return 1;
}


sub list_resources {
    
    my ($request, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    my $active; $active = 1 if ($arg && $arg eq 'active') || $request->clean_param('active');
    
    my $tables = $request->tables_hash;
    
    # my $main_table = $active ? $RESOURCE_ACTIVE : $RESOURCE_QUEUE;
    
    # Generate a list of filter expressions.
    
    my @filters;
    
    if ( my @id_list = $request->safe_param_list('eduresource_id') )
    {
	my $primary_key = $active ? $RESOURCE_IDFIELD : 'eduresource_no';
	my $id_string = join(',', @id_list);
	push @filters, "edr.$primary_key in ($id_string)";
    }
    
    if ( my @status_list = $request->safe_param_list('status', 'SELECT_NONE') )
    {
	my $status_string = join("','", @status_list);
	push @filters, "edr.status in ('$status_string')";
    }
    
    if ( my $enterer = $request->clean_param('enterer') )
    {
	my $auth_info = $request->get_auth_info($dbh);
	
	if ( $enterer eq 'me' )
	{
	    my $enterer_no = $dbh->quote($auth_info->{enterer_no} || '0');
	    push @filters, "edr.enterer_no = $enterer_no";
	}
	
	elsif ( $enterer eq 'auth' )
	{
	    my $authorizer_no = $dbh->quote($auth_info->{authorizer_no} || '0');
	    push @filters, "edr.authorizer_no = $authorizer_no";
	}
	
	else
	{
	    push @filters, "edr.authorizer_no = -1";	# select nothing
	}
    }
    
    if ( my $title = $request->clean_param('title') )
    {
	my $quoted = $dbh->quote("%${title}%");
	push @filters, "edr.title like $quoted";
    }
    
    if ( my @tags = $request->clean_param_list('tag') )
    {
	$request->cache_tag_values();
	my @tag_ids;
	
	foreach my $t ( @tags )
	{
	    if ( $t =~ /^\d+$/ )
	    {
		push @tag_ids, $t;
	    }
	    
	    elsif ( $TAG_VALUE{lc $t} )
	    {
		push @tag_ids, $TAG_VALUE{lc $t};
	    }
	    
	    else
	    {
		$request->add_warning("unknown resource tag '$t'");
	    }
	}
	
	my $id_string = join(',', @tag_ids) || '-1';
	
	push @filters, "edt.tag_id in ($id_string)";
	$tables->{edt} = 1;
    }
    
    if ( my @keywords = $request->clean_param_list('keywords') )
    {
	# This will need to be implemented later.
	push @filters, "edr.title like 'SELECT_NONE'";
    }
    
    # Make sure that we have either one filter expression or 'all_records' was selected or else
    # that we are listing active resources.
    
    unless ( @filters || $request->clean_param('all_records') || ($arg && $arg eq 'active') )
    {
	die $request->exception(400, "Bad request");
    }
    
    push @filters, "1=1" unless @filters;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( mt => 'edr', cd => 'edr' );
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether we should generate external identifiers.
    
    $request->strict_check;
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Figure out what information we need to determine access permissions.
    
    # $$$
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list($tables, $active);
    
    my $filter_string = join( q{ and }, @filters );
    
    my $extra_fields = $request->select_string;
    $extra_fields = ", $extra_fields" if $extra_fields;
    
    # Generate the main query.
    
    if ( $active )
    {
	$request->{main_sql} = "
	SELECT $calc edr.* $extra_fields
	FROM $RESOURCE_ACTIVE as edr $join_list
        WHERE $filter_string
	GROUP BY edr.$RESOURCE_IDFIELD $limit";
    }
    
    else
    {
	$request->{main_sql} = "
	SELECT $calc edr.*, act.image as active_image, edi.eduresource_no as has_image $extra_fields
	FROM $RESOURCE_QUEUE as edr
		left join $RESOURCE_IMAGES as edi using (eduresource_no)
		left join $RESOURCE_ACTIVE as act on edr.eduresource_no = act.$RESOURCE_IDFIELD
		$join_list
        WHERE $filter_string
	GROUP BY edr.eduresource_no";
    }
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    # Then prepare and execute the main query and the secondary query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


sub generate_join_list {
    
    my ($request, $tables_ref, $active) = @_;
    
    my $joins = '';
    
    if ( $tables_ref->{edi} && $active )
    {
	$joins .= "left join $RESOURCE_IMAGES as edi on edi.eduresource_no = edr.$RESOURCE_IDFIELD\n";
    }
    
    if ( $tables_ref->{edt} )
    {
	$joins .= "left join $RESOURCE_TAGS as edt on edt.resource_id = edr.$RESOURCE_IDFIELD\n";
    }
    
    return $joins;
}


sub cache_tag_values {
    
    my ($request, $dbh) = @_;
    
    $dbh ||= $request->get_connection;
    
    # If the contents of this hash have been updated within the past 10 minutes, assume they are good.
    
    return if $TAG_VALUE{_timestamp} && $TAG_VALUE{_timestamp} > (time - 600);
    
    # Otherwise, fill in the hash from the database.
    
    my $result = $dbh->selectall_arrayref("SELECT * FROM $RESOURCE_TAG_NAMES", { Slice => { } });
    
    if ( $result && ref $result eq 'ARRAY' )
    {
	foreach my $r ( @$result )
	{
	    if ( defined $r->{id} && $r->{id} ne '' && $r->{name} )
	    {
		$TAG_VALUE{lc $r->{name}} = $r->{id};
	    }
	}
	
	$TAG_VALUE{_timestamp} = time;
    }
}


sub process_record {
    
    my ($request, $record) = @_;
    
    $record->{image} ||= $record->{active_image};
    
    if ( $record->{image} && $RESOURCE_IMG_PATH )
    {
	$record->{image} = "$RESOURCE_IMG_PATH/$record->{image}";
    }
    
    elsif ( $record->{has_image} )
    {
	$record->{image} ||= 1;
    }
}


1;
