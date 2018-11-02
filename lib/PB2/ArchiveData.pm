#  
# ArchiveData
# 
# A role that returns information from the PaleoDB database about a single
# data archive or a category of data archives.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::ArchiveData;

use HTTP::Validate qw(:validators);

use TableDefs qw(%TABLE);
use ExternalIdent qw(VALID_IDENTIFIER generate_identifier);
# use TableData qw(complete_output_block);

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::Authentication);


sub initialize {

    my ($class, $ds) = @_;
    
    # Value sets
    
    $ds->define_set('1.2:archives:status' =>
	{ value => 'complete' },
	    "The archive has been properly constructed and is ready for use.",
	{ value => 'fail' },
	    "An error occurred during the construction of the archive.",
	{ value => 'loading' },
	    "The archive is in the process of being constructed. This status",
	    "should only exist for a few seconds, and should then automatically",
	    "be replaced by C<complete> or C<fail>.");
    
    $ds->define_set('1.2:archives:enterer' =>
	{ value => 'me' },
	    "Select only records that were entered by the current requestor.",
	{ value => 'auth' },
	    "Select only records that were entered by the current requestor",
	    "or one of their authorized enterers.");
    
    # Optional output
    
    $ds->define_output_map('1.2:archives:optional_output' =>
	{ value => 'ent', maps_to => '1.2:common:ent' },
	    "The identifiers of the people who authorized, entered and modified this record",
	{ value => 'entname', maps_to => '1.2:common:entname' },
	    "The names of the people who authorized, entered and modified this record",
        { value => 'crmod', maps_to => '1.2:common:crmod' },
	    "The C<created> and C<modified> timestamps for the collection record");
    
    # Output blocks
    
    $ds->define_block('1.2:archives:basic' =>
	{ output => 'archive_no', com_name => 'oid' },
	    "The unique identifier of this record in the database.",
	{ set => '*', code => \&process_record},
	{ output => '_label', com_name => 'rlb' },
	    "For all data entry operations, this field will report the record",
	    "label value, if any, that was submitted with each record.",
	{ output => 'status', com_name => 'sta' },
	    "The status will be one of the following codes:",
	    $ds->document_set('1.2:archives:status'));
    
    # Rulesets
    
    $ds->define_ruleset('1.2:archives:specifier' =>
	{ param => 'archive_id', valid => VALID_IDENTIFIER('DAR'), alias => 'id' },
	    "Return the data archive record corresponding to the specified identifier");
    
    $ds->define_ruleset('1.2:archives:selector' =>
	{ param => 'all_records', valid => FLAG_VALUE },
	    "If this parameter is specified, then all records in the database",
	    "will be returned, subject to any other parameters that are also specified.",
	{ param => 'archive_id', valid => VALID_IDENTIFIER('DAR'), alias => 'id', list => ',' },
	    "Return the educational resource record(s) corresponding to the specified",
	    "identifier(s). You can specify more than one, as a comma-separated list.",
	{ param => 'status', valid => '1.2:eduresources:status', list => ',' },
	    "Return only resource records with the specified status or statuses.",
	{ param => 'enterer', valid => ANY_VALUE, list => ',' },
	    "If this parameter is specified, then only resources created by the specified",
	    "person are shown. Only archives that have a DOI assigned to them are viewable",
	    "by anyone except their creator and his or her authorizer group, and the",
	    "database administrators. The value of this parameter may be the",
	    "identifier(s) of one or more database contributors, or else either of the following values:",
	{ param => 'search_re', valid => ANY_VALUE },
	    "Return only records for which the specified regular expression matches",
	    "either the title or the description.");
    
    $ds->define_ruleset('1.2:archives:single' =>
	{ require => '1.2:archives:specifier' },
	{ optional => 'SPECIAL(show)', valid => '1.2:archives:optional_output' },
	    "Include one or more of the following optional output blocks in the result:",
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:archives:list' =>
	{ require => '1.2:archives:selector' },
	{ optional => 'SPECIAL(show)', valid => '1.2:eduresources:optional_output' },
    	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    my $dbh = $ds->get_connection;
    
    complete_output_block($ds, $dbh, '1.2:archives:basic', 'DATA_ARCHIVES');
}


sub get_resource {
    
    my ($request) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    # Make sure we have a valid id number.
    
    my $id = $request->clean_param('eduresource_id');
    
    die "400 Bad identifier '$id'\n" unless $id and $id =~ /^\d+$/;
    
    # Determine if we are asked to show the active version of the resource record or the master
    # version.
    
    my $active = $request->clean_param('active');
    
    # If the user is not logged in, only show information about the active version.
    
    unless ( $active )
    {
	my $perms = $request->authenticate;
	$active = 1 unless $perms->role ne 'none';
    }
    
    # Delete unnecessary output fields, and select the enterer id if appropriate.
    
    $request->delete_output_field('record_label');
    
    if ( $request->has_block('1.2:common:ent') || $request->has_block('1.2:common:entname') )
    {
	my $ds = $request->ds;
	$ds->add_output_block($request, { }, '1.2:common:ent_guest');
    }
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    $request->substitute_select( cd => 'edr' );
    # $request->check_entname($TABLE{RESOURCE_QUEUE} => 'edr');
    
    my $tables = $request->tables_hash;
    
    # If the 'strict' parameter was given, make sure we haven't generated any
    # warnings. Also check whether we should generate external identifiers.
    
    $request->strict_check;
    $request->extid_check;
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list($request->tables_hash, $active);
    
    my $extra_fields = $request->select_string;
    $extra_fields = ", $extra_fields" if $extra_fields;
    
    # Generate the main query.
    
    if ( $active )
    {
	$request->{main_sql} = "
	SELECT edr.*, group_concat(tg.tag_id) as tags $extra_fields FROM $TABLE{RESOURCE_ACTIVE} as edr
		left join $TABLE{RESOURCE_TAGS} as tg on tg.resource_id = edr.$RESOURCE_IDFIELD
		$join_list
        WHERE edr.$RESOURCE_IDFIELD = $id
	GROUP BY edr.$RESOURCE_IDFIELD";
    }
    
    else
    {
	$request->{main_sql} = "
	SELECT edr.*, act.image as active_image, edi.eduresource_no as has_image $extra_fields
	FROM $TABLE{RESOURCE_QUEUE} as edr
		left join $TABLE{RESOURCE_IMAGES} as edi using (eduresource_no)
		left join $TABLE{RESOURCE_ACTIVE} as act on edr.eduresource_no = act.$RESOURCE_IDFIELD
		$join_list
        WHERE edr.eduresource_no = $id
	GROUP BY edr.eduresource_no";
    }
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_record} = $dbh->selectrow_hashref($request->{main_sql});
    
    # Return an error response if we couldn't retrieve the record.
    
    die "404 Not found\n" unless $request->{main_record};
    
    return 1;
}


sub list_resources {
    
    my ($request, $arg) = @_;
    
    # Get a database handle by which we can make queries.
    
    my $dbh = $request->get_connection;
    
    my $active; $active = 1 if ($arg && $arg eq 'active') || $request->clean_param('active');
    
    my $tables = $request->tables_hash;
    
    # If the user is not logged in, only show information about active resources. If we are asked
    # for anything but active resources, return a 401 error.
    
    my $perms;
    
    unless ( $active )
    {
	$perms = $request->require_authentication('RESOURCE_QUEUE', "Login Required");
    }
    
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
	my $status_table = $active ? 'edq' : 'edr';
	push @filters, "$status_table.status in ('$status_string')";
    }

    # If we have 'post' but not 'view' permission on the table, then implicitly select only those
    # records entered by the user.
    
    my $enterer = $request->clean_param('enterer');
    $enterer = 'me' if $perms && $perms->check_table_permission($TABLE{RESOURCE_QUEUE}, 'view') eq 'own';
    
    if ( $enterer )
    {
	$perms ||= $request->authenticate($TABLE{RESOURCE_QUEUE});
	
	if ( $enterer eq 'me' )
	{
	    my $enterer_no = $dbh->quote($perms->enterer_no);
	    my $user_id = $dbh->quote($perms->user_id);
	    
	    my @clauses;
	    
	    push @clauses, "edr.enterer_no = $enterer_no" if $enterer_no;
	    push @clauses, "edr.enterer_id = $user_id" if $user_id;
	    push @clauses, "edr.enterer_no = -1" unless @clauses;
	    
	    my $filter_str = join(' or ', @clauses);
	    push @filters, $filter_str;
	}
	
	elsif ( $enterer eq 'auth' )
	{
	    my $enterer_no = $dbh->quote($perms->enterer_no);
	    my $user_id = $dbh->quote($perms->user_id);
	    
	    my @clauses;
	    
	    push @clauses, "edr.enterer_no = $enterer_no" if $enterer_no;
	    push @clauses, "edr.authorizer_no = $enterer_no" if $enterer_no;
	    push @clauses, "edr.enterer_id = $user_id" if $user_id;
	    push @clauses, "edr.enterer_no = -1" unless @clauses;
	    
	    my $filter_str = join(' or ', @clauses);
	    push @filters, $filter_str;
	}
	
	else
	{
	    push @filters, "edr.authorizer_no = -1";	# select nothing
	}
    }
    
    # Check for other filter parameters.
    
    if ( my $title = $request->clean_param('title') )
    {
	my $quoted = $dbh->quote("${title}");
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
    
    if ( my @keywords = $request->clean_param_list('keyword') )
    {
	my @kwfilters;
	
	foreach my $k (@keywords)
	{
	    push @kwfilters, "edr.title rlike '\\\\b$k\\\\b'";
	    push @kwfilters, "edr.description rlike '\\\\b$k\\\\b'";
	}
	
	push @kwfilters, "edr.title = 'SELECT_NOTHING'" unless @kwfilters;

	push @filters, '(' . join(' or ', @kwfilters) . ')';
    }
    
    # Make sure that we have either one filter expression or 'all_records' was selected or else
    # that we are listing active resources.
    
    unless ( @filters || $request->clean_param('all_records') || ($arg && $arg eq 'active') )
    {
	die $request->exception(400, "Bad request");
    }
    
    push @filters, "1=1" unless @filters;
    
    # Delete unnecessary output fields, and select the enterer id if appropriate.
    
    $request->delete_output_field('record_label');

    if ( $request->has_block('1.2:common:ent') || $request->has_block('1.2:common:entname') )
    {
	my $ds = $request->ds;
	$ds->add_output_block($request, { }, '1.2:common:ent_guest');
    }
    
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
    
    # Determine the necessary joins.
    
    my ($join_list) = $request->generate_join_list($tables, $active);
    
    my $filter_string = join( q{ and }, @filters );
    
    my $extra_fields = $request->select_string;
    $extra_fields = ", $extra_fields" if $extra_fields;

    # Remove fields that don't exist in the active table, if we are doing an active-table query.

    if ( $active && $extra_fields )
    {
	$extra_fields =~ s/, *edr.(?:modifier_no|enterer_no)//g;
    }
    
    # Generate the main query.
    
    if ( $active )
    {
	$request->{main_sql} = "
	SELECT $calc edr.*, group_concat(tg.tag_id) as tags $extra_fields
	FROM $TABLE{RESOURCE_ACTIVE} as edr
		left join $TABLE{RESOURCE_TAGS} as tg on tg.resource_id = edr.$RESOURCE_IDFIELD
		left join $TABLE{RESOURCE_QUEUE} as edq on edr.$RESOURCE_IDFIELD = edq.eduresource_no
		$join_list
        WHERE $filter_string
	GROUP BY edr.$RESOURCE_IDFIELD $limit";
    }
    
    else
    {
	$request->{main_sql} = "
	SELECT $calc edr.*, act.image as active_image, edi.eduresource_no as has_image $extra_fields
	FROM $TABLE{RESOURCE_QUEUE} as edr
		left join $TABLE{RESOURCE_IMAGES} as edi using (eduresource_no)
		left join $TABLE{RESOURCE_ACTIVE} as act on edr.eduresource_no = act.$RESOURCE_IDFIELD
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
    my $idfield = $active ? $RESOURCE_IDFIELD : 'eduresource_no';
    
    if ( $tables_ref->{edi} && $active )
    {
	$joins .= "left join $TABLE{RESOURCE_IMAGES} as edi on edi.eduresource_no = edr.$idfield\n";
    }
    
    if ( $tables_ref->{edt} )
    {
	$joins .= "left join $TABLE{RESOURCE_TAGS} as edt on edt.resource_id = edr.$idfield\n";
    }
    
    return $joins;
}


sub cache_tag_values {
    
    my ($request, $dbh) = @_;
    
    # If the contents of this hash have been updated within the past 10 minutes, assume they are good.
    
    return if $TAG_VALUE{_timestamp} && $TAG_VALUE{_timestamp} > (time - 600);
    
    # Otherwise, fill in the hash from the database.
    
    $dbh ||= $request->get_connection;
    
    my $result = $dbh->selectall_arrayref("SELECT * FROM $TABLE{RESOURCE_TAG_NAMES}", { Slice => { } });
    
    if ( $result && ref $result eq 'ARRAY' )
    {
	foreach my $r ( @$result )
	{
	    if ( defined $r->{id} && $r->{id} ne '' && $r->{name} )
	    {
		$TAG_VALUE{lc $r->{name}} = $r->{id};
		$TAG_NAME{$r->{id}} = $r->{name};
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
	$record->{record_label} = $label;
    }
    
    # Generate the proper external identifiers.
    
    if ( $request->{block_hash}{extids} )
    {
	$record->{eduresource_no} = generate_identifier('EDR', $record->{eduresource_no})
	    if $record->{eduresource_no};
    }
    
	# { set => 'eduresource_no', code => sub {
	#       my ($request, $value) = @_;
	# 	    return $value unless $request->{block_hash}{extids};
	# 	    return generate_identifier('EDR', $value);
	# 	} },
    
    # The 'image' filename might be in either of these fields. Append it to the proper path, read
    # from the configuration file.
    
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


1;
