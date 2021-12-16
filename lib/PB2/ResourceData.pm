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

use TableDefs qw(%TABLE);
use ResourceDefs; # qw($RESOURCE_ACTIVE $RESOURCE_QUEUE $RESOURCE_IMAGES $RESOURCE_TAG_NAMES $RESOURCE_TAGS);
use ExternalIdent qw(VALID_IDENTIFIER generate_identifier);
use TableData qw(complete_output_block);

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::Authentication);

our ($RESOURCE_IMG_PATH);

our (%TAG_VALUE, %TAG_NAME, %TAG_COUNT);

sub initialize {

    my ($class, $ds) = @_;
    
    # Value sets
    
    $ds->define_set('1.2:eduresources:status' =>
	{ value => 'active' },
	    "An active resource is visible on the Resources page of this website.",
	{ value => 'inactive' },
	    "An inactive resource is one that is available for future use, but not",
	    "currently visible on the Resources page.",
	{ value => 'pending' },
	    "A pending resource is one that is newly submitted,",
	    "and has not yet been reviewed.",
	{ value => 'changed' },
	    "A resource with this status has a version that is either active or inactive,",
	    "and either the submitter or a reviewer has updated one or more of the fields.",
	    "When these changes are approved, they will be applied to the active or inactive",
	    "record.",
	{ value => 'deleted' },
	    "A resource that has just been deleted from the database is reported",
	    "with this status code. Subsequent queries will return a Not Found error.");
    
    $ds->define_set('1.2:eduresources:enterer' =>
	{ value => 'me' },
	    "Select only records that were entered by the current requestor.",
	{ value => 'auth' },
	    "Select only records that were entered by the current requestor",
	    "or one of their authorized enterers.");
    
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
	{ output => '_label', com_name => 'rlb' },
	    "For all data entry operations, this field will report the record",
	    "label value, if any, that was submitted with each record.",
	{ output => 'status', com_name => 'sta' },
	    "The status will be one of the following codes:",
	    $ds->document_set('1.2:eduresources:status'));
    
    $ds->define_block('1.2:eduresources:larkin' =>
	{ output => 'id', data_type => 'pos' },
	    "The unique identifier of this record in the database.",
	{ set => '*', code => \&process_larkin},
	{ output => 'title' },
	{ output => 'description' },
	{ output => 'url' },
	{ output => 'is_video', data_type => 'pos' },
	{ output => 'author' },
	{ output => 'created_on' },
	{ output => 'image', always => 1 },
	{ output => 'tags' });
    
    $ds->define_block('1.2:eduresources:tag' =>
	{ output => 'id', com_name => 'oid', data_type => 'mix' },
	{ output => 'name', com_name => 'nam' },
	{ output => 'resources', com_name => 'nrs', data_type => 'pos' });
    
    $ds->define_block('1.2:eduresources:image_data' =>
	{ select => 'edi.image_data', tables => 'edi' },
	{ output => 'image_data', com_name => 'image_data' },
	    "The image data, if any, associated with this record. If it was properly",
	    "uploaded, it should be encoded in base64.");
    
    # Rulesets
    
    $ds->define_ruleset('1.2:eduresources:specifier' =>
	{ param => 'eduresource_id', valid => VALID_IDENTIFIER('EDR'), alias => 'id' },
	    "Return the educational resource record corresponding to the specified identifier");
    
    $ds->define_ruleset('1.2:eduresources:single' =>
	{ require => '1.2:eduresources:specifier' },
	{ optional => 'active', valid => FLAG_VALUE },
	    "If this parameter is included, then the version of the record in the active",
	    "table is returned if one exists, and a 'not found' error otherwise. If this",
	    "parameter is not included, or is included with the value C<B<no>>, then the",
	    "queue version of the record is returned if one exists, and a 'not found' error",
	    "otherwise.",
	{ optional => 'SPECIAL(show)', valid => '1.2:eduresources:optional_output' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:eduresources:selector' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "If this parameter is specified, then all records in the database",
	    "will be returned, subject to any other parameters that are also specified.",
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
    
    $ds->define_ruleset('1.2:eduresources:list' =>
	{ allow => '1.2:eduresources:selector' },
	{ optional => 'active', valid => FLAG_VALUE },
	    "If this parameter is included, then the active record table is queried.",
	    "All records with a status of 'active' are returned, unless the 'status'",
	    "parameter is also given.",
	    "If it is not included, or is included with the value C<B<no>>, then the",
	    "queue table is queried. This will by default return all records regardless",
	    "of status.",
	{ optional => 'queue', valid => FLAG_VALUE },
	    "If this parameter is included, then the queue table is queried and only",
	    "records with a status of 'pending' or 'changed' are returned.",
	{ at_most_one => [ 'active', 'queue' ] },
	{ optional => 'SPECIAL(show)', valid => '1.2:eduresources:optional_output' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:eduresources:active' =>
	{ allow => '1.2:eduresources:selector' },
	{ optional => 'SPECIAL(show)', valid => '1.2:eduresources:optional_output' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:eduresources:inactive' =>
	{ allow => '1.2:eduresources:selector' },
	{ optional => 'SPECIAL(show)', valid => '1.2:eduresources:optional_output' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $RESOURCE_IMG_PATH = $ds->config_value('eduresources_img_path');
    
    die "You must provide a configuration value for 'eduresources_active' and 'eduresources_tags'"
	unless $TABLE{RESOURCE_ACTIVE} && $TABLE{RESOURCE_TAGS};
    
    my $dbh = $ds->get_connection;
    
    complete_output_block($ds, $dbh, '1.2:eduresources:basic', 'RESOURCE_QUEUE');
}


sub get_resource {
    
    my ($request) = @_;
    
    # Make sure we have a valid id number, or throw an exception.
    
    my $id = $request->clean_param('eduresource_id');
    
    die $request->exception("400", "Bad identifier '$id'") unless $id and $id =~ /^\d+$/;
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether we should generate external identifiers.
    
    $request->strict_check;
    $request->extid_check;

    # Delete unnecessary output fields, and process the enterer_id if necessary.
    
    $request->delete_output_field('_label');
    
    if ( $request->has_block('1.2:common:ent') || $request->has_block('1.2:common:entname') )
    {
	$request->add_output_blocks('main', '1.2:common:ent_guest');
    }
    
    # Select the proper tables from which to draw the resource records.

    my $RESOURCE_TABLE = $TABLE{RESOURCE_QUEUE};
    my $IMAGE_TABLE = $TABLE{RESOURCE_IMAGES};
    
    # If the parameter 'active' was given, use the active tables.
    
    if ( $request->clean_param('active') )
    {
	$RESOURCE_TABLE = $TABLE{RESOURCE_ACTIVE};
	$IMAGE_TABLE = $TABLE{RESOURCE_IMAGES_ACTIVE};
    }
    
    # Otherwise, the user must be logged in and must have permission to view the record.
    
    else
    {
	my $perms = $request->require_authentication('RESOURCE_QUEUE', "Login Required");
	
	unless ( $perms->check_record_permission('RESOURCE_QUEUE', 'view',
						 'eduresource_no', $id) eq 'view' )
	{
	    die $request->exception('401', 'Permission denied');
	}
    }
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have correct tag information.
    
    $request->cache_tag_values($dbh);
    
    # Determine the necessary extra fields.
    
    my $extra_fields = $request->select_string;
    $extra_fields = ", $extra_fields" if $extra_fields;
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT edr.*, edi.eduresource_no as has_image $extra_fields
	FROM $RESOURCE_TABLE as edr left join $IMAGE_TABLE as edi using (eduresource_no)
        WHERE edr.eduresource_no = $id";
    
    $request->debug_line("$request->{main_sql}\n") if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    # Return an error response if we couldn't retrieve the record.
    
    die $request->exception('404') unless $request->{main_record};
    
    return 1;
}


# sub get_active {
    
#     my ($request, $id) = @_;
    
#     my $dbh = $request->get_connection;
    
#     # Make sure we have correct tag information.
    
#     $request->cache_tag_values($dbh);
    
#     # Determine the necessary extra fields.
    
#     my $extra_fields = $request->select_string;
#     $extra_fields = ", $extra_fields" if $extra_fields;
    
#     # Generate the main query.
    
#     $request->{main_sql} = "
# 	SELECT edr.*, edi.eduresource_no as has_image $extra_fields
# 	FROM $TABLE{RESOURCE_ACTIVE} as edr
# 		left join $TABLE{RESOURCE_IMAGES_ACTIVE} as edi using (eduresource_no)
#         WHERE edr.eduresource_no = $id and edr.status = 'active'";
    
#     $request->debug_line("$request->{main_sql}\n") if $request->debug;
    
#     $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
#     # Return an error response if we couldn't retrieve the record.
    
#     die $request->exception('404') unless $request->{main_record};
    
#     return 1;
# }


sub list_resources {
    
    my ($request, $arg) = @_;
    
    my ($active_table, $show_queue, $perms);
    
    $arg ||= '';
    
    # If the request is for active resources, no authentication is required.
    
    if ( $arg eq 'active' || $request->clean_param('active') )
    {
	$active_table = 1;
	
	# If the requested output format is 'larkin', we can execute a much simpler query.
	
	if ( $request->output_format eq 'larkin' )
	{
	    return $request->larkin_resources();
	}
    }

    elsif ( $arg eq 'inactive' )
    {
	$active_table = 1;
    }
    
    # Otherwise, the user must be logged in. If the parameter 'queue' is included in the
    # request, note that for use below.
    
    else
    {
	$perms = $request->require_authentication('RESOURCE_QUEUE');

	$show_queue = $request->clean_param('queue');
    }
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have good tag values.
    
    $request->cache_tag_values();
    
    # Generate a list of filter expressions.
    
    my @filters;
    
    if ( my @id_list = $request->safe_param_list('eduresource_id') )
    {
	my $id_string = join(',', @id_list);
	push @filters, "edr.eduresource_no in ($id_string)";
    }
    
    if ( my @status_list = $request->safe_param_list('status', 'SELECT_NONE') )
    {
	my $status_string = join("','", @status_list);
	push @filters, "edr.status in ('$status_string')";
    }

    elsif ( $active_table && $arg eq 'inactive' )
    {
	push @filters, "edr.status = 'inactive'";
    }
    
    elsif ( $active_table )
    {
	push @filters, "edr.status = 'active'";
    }

    elsif ( $show_queue )
    {
	push @filters, "edr.status in ('pending','changed')";
    }
    
    if ( my $title = $request->clean_param('title') )
    {
	my $quoted = $dbh->quote("${title}");
	push @filters, "edr.title like $quoted";
    }
    
    if ( my @tags = $request->clean_param_list('tag') )
    {
	my @tag_ids;
	
	foreach my $t ( @tags )
	{
	    if ( $t =~ /^\d+$/ )
	    {
		push @tag_ids, "\\\\b$t\\\\b";
	    }
	    
	    elsif ( $TAG_VALUE{lc $t} )
	    {
		push @tag_ids, "\\\\b$TAG_VALUE{lc $t}\\\\b";
	    }
	    
	    else
	    {
		$request->add_warning("unknown resource tag '$t'");
	    }
	}
	
	my $regexp = join('|', @tag_ids) || 'SELECT_NOTHING';
	
	push @filters, "edr.tags rlike '$regexp'";
    }
    
    if ( my @keywords = $request->clean_param_list('keyword') )
    {
	my (@kwfilters, @matches);
	
	foreach my $k (@keywords)
	{
	    if ( $k )
	    {
		push @matches, "\\\\b$k\\\\b";
	    }
	}
	
	if ( @matches )
	{
	    my $regexp = join('|', @matches);
	    push @kwfilters, "edr.title rlike '$regexp'";
	    push @kwfilters, "edr.description rlike '$regexp'";
	    push @kwfilters, "edr.topics rlike '$regexp'";
	}

	else
	{
	    push @kwfilters, "edr.title = 'SELECT_NOTHING'";
	}
	
	# {	    
	#     push @kwfilters, "edr.title rlike '\\\\b$k\\\\b'";
	#     push @kwfilters, "edr.description rlike '\\\\b$k\\\\b'";
	# }
	
	# push @kwfilters, "edr.title = 'SELECT_NOTHING'" unless @kwfilters;

	push @filters, '(' . join(' or ', @kwfilters) . ')';
    }
    
    my $enterer = $request->clean_param('enterer');
    
    # If we are returning queue records instead of active ones, and if the user has 'post' but
    # not 'view' permission on the table, then implicitly select only those records entered by the
    # user.
    
    if ( $perms && $perms->check_table_permission('RESOURCE_QUEUE', 'view') eq 'own' )
    {
	die $request->exception('401', 'Permission denied') if $enterer &&
	    $enterer ne 'me' && $enterer ne 'auth';
	$enterer ||= 'me';
    }
    
    # If we have an enterer filter, apply it.
    
    if ( $enterer )
    {
	$perms ||= $request->authenticate('RESOURCE_QUEUE');
	
	if ( $enterer eq 'me' )
	{
	    my $enterer_no = $dbh->quote($perms->enterer_no);
	    my $user_id = $dbh->quote($perms->user_id);
	    
	    my @clauses;
	    
	    push @clauses, "edr.enterer_no = $enterer_no" if $enterer_no;
	    push @clauses, "edr.enterer_id = $user_id" if $user_id;
	    
	    my $filter_str = join(' or ', @clauses) || "edr.enterer_no = -1";
	    push @filters, "($filter_str)";
	}
	
	elsif ( $enterer eq 'auth' )
	{
	    my $enterer_no = $dbh->quote($perms->enterer_no);
	    my $user_id = $dbh->quote($perms->user_id);
	    
	    my @clauses;
	    
	    push @clauses, "edr.enterer_no = $enterer_no" if $enterer_no;
	    push @clauses, "edr.authorizer_no = $enterer_no" if $enterer_no;
	    push @clauses, "edr.enterer_id = $user_id" if $user_id;
	    
	    my $filter_str = join(' or ', @clauses) || "edr.enterer_no = -1";
	    push @filters, "($filter_str)";
	}
	
	else
	{
	    push @filters, "edr.authorizer_no = -1";	# select nothing
	}
    }
    
    # Make sure that we have either one filter expression or 'all_records' was selected or else
    # that we are listing active resources.
    
    unless ( @filters || $request->clean_param('all_records') )
    {
	die $request->exception(400, "Bad request");
    }
    
    # Delete unnecessary output fields, and select the enterer id if appropriate.
    
    $request->delete_output_field('_label');
    
    if ( $request->has_block('1.2:common:ent') || $request->has_block('1.2:common:entname') )
    {
	$request->add_output_blocks('main', '1.2:common:ent_guest');
    }
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether we should generate external identifiers.
    
    $request->strict_check;
    $request->extid_check;
    
    # Make sure we are querying the right tables.

    my ($RESOURCE_TABLE, $IMAGE_TABLE);

    if ( $active_table )
    {
	$RESOURCE_TABLE = $TABLE{RESOURCE_ACTIVE};
	$IMAGE_TABLE = $TABLE{RESOURCE_IMAGES_ACTIVE};
    }

    else
    {
	$RESOURCE_TABLE = $TABLE{RESOURCE_QUEUE};
	$IMAGE_TABLE = $TABLE{RESOURCE_IMAGES};
    }
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    my $filter_string = join(' and ', @filters) || '1=1';
    
    my $extra_fields = $request->select_string;
    $extra_fields = ", $extra_fields" if $extra_fields;
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $calc edr.*, edi.eduresource_no as has_image $extra_fields
	FROM $RESOURCE_TABLE as edr left join $IMAGE_TABLE as edi using (eduresource_no)
        WHERE $filter_string $limit";
    
    $request->debug_line("$request->{main_sql}\n") if $request->debug;
    
    # Then prepare and execute the query.
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}


# sub generate_join_list {
    
#     my ($request, $tables_ref, $active) = @_;
    
#     my $joins = '';
#     my $idfield = $active ? $RESOURCE_IDFIELD : 'eduresource_no';
    
#     if ( $tables_ref->{edi} && $active )
#     {
# 	$joins .= "left join $TABLE{RESOURCE_IMAGES} as edi on edi.eduresource_no = edr.$idfield\n";
#     }
    
#     if ( $tables_ref->{edt} )
#     {
# 	$joins .= "left join $TABLE{RESOURCE_TAGS} as edt on edt.resource_id = edr.$idfield\n";
#     }
    
#     return $joins;
# }


sub larkin_resources {
    
    my ($request) = @_;
    
    my $dbh = $request->get_connection();
    
    $request->add_output_blocks('main', '1.2:eduresources:larkin');
    
    my $limit = $request->sql_limit_clause(1);
    
    $request->{main_sql} = "
    	SELECT * FROM $TABLE{RESOURCE_ACTIVE} $limit
	WHERE status = 'active'";
    
    $request->debug_line("$request->{main_sql}\n") if $request->debug;
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
}


sub list_tags {
    
    my ($request) = @_;
    
    $request->cache_tag_values();
    
    $request->extid_check;
    
    my @result;
    
    foreach my $id ( sort { $a <=> $b } keys %TAG_NAME )
    {
	my $record = { id => $id, name => $TAG_NAME{$id}, resources => $TAG_COUNT{$id} || 0 };
	
	if ( $request->{block_hash}{extids} )
	{
	    $record->{id} = generate_identifier('ETG', $record->{id});
	}
	
	push @result, $record;
    }
    
    $request->list_result(@result);
}


sub cache_tag_values {
    
    my ($request, $dbh) = @_;
    
    # If the contents of this hash have been updated within the past 10 minutes, assume they are good.
    
    return if $TAG_VALUE{_timestamp} && $TAG_VALUE{_timestamp} > (time - 600);
    
    # Otherwise, fill in the hash from the database.
    
    $dbh ||= $request->get_connection;
    
    my $result = $dbh->selectall_arrayref("
	SELECT tag.id, tag.name, count(res.tag_id) as resources
	FROM $TABLE{RESOURCE_TAG_NAMES} as tag LEFT JOIN $TABLE{RESOURCE_TAGS} as res on tag.id = res.tag_id
	GROUP BY tag.id", { Slice => { } });
    
    if ( $result && ref $result eq 'ARRAY' )
    {
	foreach my $r ( @$result )
	{
	    if ( defined $r->{id} && $r->{id} ne '' && $r->{name} )
	    {
		$TAG_VALUE{lc $r->{name}} = $r->{id};
		$TAG_NAME{$r->{id}} = $r->{name};
		$TAG_COUNT{$r->{id}} = $r->{resources};
	    }
	}
	
	$TAG_VALUE{_timestamp} = time;
    }
}


# process_record ( request, record )
# 
# This procedure is called automatically for each record that is expressed via the output block
# '1.2:eduresources:basic'. It cleans up some of the data fields.

sub process_record {
    
    my ($request, $record) = @_;

    # If we have an 'id' field instead of an 'eduresource_no' field, copy the
    # value over.
    
    if ( ! $record->{eduresource_no} && $record->{id} )
    {
	$record->{eduresource_no} = $record->{id};
    }
    
    # If we have a record label hash, fill in those values.
    
    if ( my $label = $request->{my_record_label}{$record->{eduresource_no}} )
    {
	$record->{_label} = $label;
    }
    
    # Generate the proper external identifiers.
    
    if ( $request->{block_hash}{extids} )
    {
	$record->{eduresource_no} = generate_identifier('EDR', $record->{eduresource_no})
	    if $record->{eduresource_no};
    }
    
    # The 'image' filename might be in either of these fields. Append it to the proper path, read
    # from the configuration file. But not for larkin-style requests.
    
    $record->{image} = $record->{active_image} if $record->{active_image};
    
    if ( $record->{image} && $RESOURCE_IMG_PATH )
    {
	$record->{image} = "$RESOURCE_IMG_PATH/$record->{image}";
    }
    
    # If we don't have an image name, just us '1' to indicate that there is an image but it is not
    # stored in a file.
    
    elsif ( $record->{has_image} )
    {
	$record->{image} ||= 1;
    }
    
    # If we have tags, run through the numbers and convert them to names.
    
    if ( my $tag_list = $record->{tags} )
    {
	$request->cache_tag_values();
	
	my @id_list = split(/\s*,\s*/, $tag_list);
	my @tag_names;
	
	foreach my $id (@id_list)
	{
	    push @tag_names, $TAG_NAME{$id} if $TAG_NAME{$id};
	}
	
	$record->{tags} = join(', ', @tag_names);
    }
}


# process_larkin ( request, record )
#
# Process a record if the request format is 'larkin'.

sub process_larkin {

    my ($request, $record) = @_;

    # If we have an eduresource_no field, copy it over.

    if ( ! $record->{id} && $record->{eduresource_no} )
    {
	$record->{id} = $record->{eduresource_no};
    }
    
    # Truncate the created_on field so that it just contains the date.
    
    $record->{created_on} =~ s{\s.*}{} if $record->{created_on};
    
    # The 'image' filename might be in either of these fields.
    
    $record->{image} = $record->{active_image} if $record->{active_image};
    
    # If we have tags, run through the numbers and convert them to names.
    
    if ( my $tag_list = $record->{tags} )
    {
	$request->cache_tag_values();
	
	my @id_list = split(/\s*,\s*/, $tag_list);
	my @tag_names;
	
	foreach my $id (@id_list)
	{
	    push @tag_names, $TAG_NAME{$id} if $TAG_NAME{$id};
	}

	$record->{tags} = \@tag_names;
    }
}


1;
