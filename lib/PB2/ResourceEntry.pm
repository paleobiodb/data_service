#  
# ResourceEntry
# 
# A role that allows entry and manipulation of records representing educational resources. This is
# a template for general record editing, in situations where the records in a table are
# independent of the rest of the database.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::ResourceEntry;

use HTTP::Validate qw(:validators);

use TableDefs qw($RESOURCE_ACTIVE $RESOURCE_QUEUE $RESOURCE_IMAGES $RESOURCE_TAGS);
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);
use PB2::TableData qw(complete_ruleset);
use File::Temp qw(tempfile);

use EditTransaction;

use Carp qw(carp croak);
use Try::Tiny;
use MIME::Base64;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::Authentication PB2::CommonData PB2::CommonEntry PB2::ResourceData);

our ($RESOURCE_IDFIELD, $RESOURCE_IMG_DIR);

our (%RESOURCE_IGNORE) = ( 'image_data' => 1 );

our (%TAG_VALUES);

our ($IMAGE_IDENTIFY_COMMAND, $IMAGE_CONVERT_COMMAND, $IMAGE_MAX);

# initialize ( )
# 
# This routine is called by the Web::DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # Value sets for specifying data entry options.
    
    # Rulesets for entering and updating data.
    
    $ds->define_ruleset('1.2:eduresources:entry' =>
	{ param => 'record_label', valid => ANY_VALUE },
	    "This parameter is only necessary in body records, and then only if",
	    "more than one record is included in a given request. This allows",
	    "you to associate any returned error messages with the records that",
	    "generated them. You may provide any non-empty value.",
	{ param => 'eduresource_id', valid => VALID_IDENTIFIER('EDR'), alias => 'id' },
	    "The identifier of the educational resource record to be updated. If it is",
	    "empty, a new record will be created. You can also use the alias B<C<id>>.",
	{ optional => 'status', valid => '1.2:eduresources:status' },
	    "This parameter should only be given if the logged-in user has administrator",
	    "privileges on the educational resources table. It allows the resource to be",
	    "activated or inactivated, controlling whether or not it appears on the",
	    "Resources page of the website. Newly added resources are given the status",
	    "C<B<pending>> by default. If an active resource is later updated, its",
	    "status is automatically changed to C<B<changes>>. If the record's status",
	    "is later set to C<B<active>> once again, the new values will be copied",
	    "over to the table that drives the Resources page. Accepted values for",
	    "this parameter are:",
	{ optional => 'tags', valid => ANY_VALUE },
	    "The value of this parameter should be a list of tag names, identifying",
	    "the tags/headings with which this resource should be associated. You",
	    "can specify this as either a comma-separated list in a string, or as a",
	    "JSON list of strings. Alternatively, you can use the integer identifiers",
	    "corresponding to the tags.");
    
    $ds->define_ruleset('1.2:eduresources:addupdate' =>
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every resource specified in the body.",
	{ allow => '1.2:eduresources:entry' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:eduresources:addupdate_body' =>
	">>You may include one or more records in the body of the request, in JSON form.",
	"The body must be either a single JSON object, or an array of objects. The fields",
	"in each object must be as specified below. If no specific documentation is given",
	"the value must match the corresponding column in the C<B<$RESOURCE_QUEUE>> table",
	"in the database.",
	{ allow => '1.2:eduresources:entry' },
	{ optional => 'image_data', valid => ANY_VALUE },
	    "An image to be associated with this record, encoded into base64. The",
	    "data may begin with the HTML prefix C<data:image/[type]; base64,>.");
    
    $ds->define_ruleset('1.2:eduresources:update' =>
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every resource specified in the body.",
	{ allow => '1.2:eduresources:entry' },
	">>You may include one or more records in the body, in JSON form. The fields",
	"given in the body must match the C<B<eduresources>> table definition in the database.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:eduresources:update_body' =>
	">>You may include one or more records in the body of the request, in JSON form.",
	"The body must be either a single JSON object, or an array of objects. The fields",
	"in each object must be as specified below. If no specific documentation is given",
	"the value must match the corresponding column in the C<B<eduresources>> table",
	"in the database. For this operation, every record must include a value for",
	"B<C<eduresource_id>>.",
	{ allow => '1.2:eduresources:entry' },
	{ optional => 'image_data', valid => ANY_VALUE },
	    "An image to be associated with this record, encoded into base64. The",
	    "data may begin with the HTML prefix C<data:image/[type]; base64,>.");
    
    $ds->define_ruleset('1.2:eduresources:delete' =>
	">>The following parameter may be given either in the URL or in",
	"the request body. Either way, you may specify more than one value,",
	"as a comma-separated list.",
	{ param => 'eduresource_id', valid => VALID_IDENTIFIER('EDR'), list => ',', alias => 'id' },
	    "The identifier(s) of the resource record(s) to delete.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $RESOURCE_IDFIELD = $ds->config_value('eduresources_idfield') || 'id';
    $RESOURCE_IMG_DIR = $ds->config_value('eduresources_img_dir');
    
    $IMAGE_IDENTIFY_COMMAND = $ds->config_value('image_identify_cmd') || '/opt/local/bin/identify';
    $IMAGE_CONVERT_COMMAND = $ds->config_value('image_convert_cmd') || '/opt/local/bin/convert';
    $IMAGE_MAX = $ds->config_value('image_max_dimension') || 150;
    
    die "You must provide a configuration value for 'eduresources_active' and 'eduresources_tags'"
	unless $RESOURCE_ACTIVE && $RESOURCE_TAGS;
    
    my $dbh = $ds->get_connection;
    
    complete_ruleset($ds, $dbh, '1.2:eduresources:addupdate_body', $RESOURCE_QUEUE);
    complete_ruleset($ds, $dbh, '1.2:eduresources:update_body', $RESOURCE_QUEUE);
}


our (%IGNORE_PARAM) = ( 'allow' => 1, 'return' => 1, 'record_label' => 1 );


sub update_resources {
    
    my ($request, $arg) = @_;
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single record only.
    
    my %conditions;
    
    $conditions{CREATE_RECORDS} = 1 if $arg && $arg eq 'add';
    
    my $main_params = $request->get_main_params(\%conditions);
    my $auth_info = $request->require_auth($dbh, $RESOURCE_QUEUE);
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->unpack_input_records($main_params);
    
    # Now go through the records and validate each one in turn.
    
    my %record_activation;
    my @good_records;
    
    foreach my $r ( @records )
    {
	my $record_label = $r->{record_label} || $r->{eduresource_id} || '';
	my $op = $r->{eduresource_id} ? 'update' : 'add';
	
	# If we are updating an existing record, we need to validate its fields.
	
	if ( my $record_id = $r->{eduresource_id} || $r->{eduresource_no} )
	{
	    $r->{eduresource_no} = $record_id = $request->validate_extident('EDR', $record_id, 'eduresource_id');
	    delete $r->{eduresource_id};
	    
	    # Fetch the current authorization and status information.
	    
	    # my ($current_status, $record_authno, $record_entno, $record_entid) = 
	    # 	$request->fetch_record_values($dbh, $RESOURCE_QUEUE, "eduresource_no = $record_id", 
	    # 				      'status, authorizer_no, enterer_no, enterer_id');
	    
	    # Make sure that we have authorization to modify this record, and that it actually exists.
	    
	    my ($current) = $request->fetch_record($dbh, $RESOURCE_QUEUE, "eduresource_no=$record_id",
						   'eduresource_no, status, authorizer_no, enterer_no, enterer_id');
	    
	    # If we cannot find the record, then add an error to the request and continue on to
	    # the next.
	    
	    unless ( $current )
	    {
		$request->add_record_error('E_NOT_FOUND', $record_label, "record not found");
		next;
	    }

	    # Otherwise, check the permission that the current user has on this record. If they
	    # have the 'admin' permission, they can modify the record and also adjust its status.

	    my $permission = $request->check_record_permission($current, $RESOURCE_QUEUE, 'edit', 'eduresource_no');
	    
	    if ( $permission eq 'admin' )
	    {
		# If the status is being explicitly set to 'active', then the new record
		# values should be copied into the active resources table. This is a valid
		# operation even if a previous version of the record is already in that table.
		
		if ( $r->{status} && $r->{status} eq 'active' )
		{
		    $record_activation{$record_id} = 'copy';
		}
		
		# Otherwise, if this record has a status that marks it as already having been
		# copied to the active table, then we need to check and see if it is being set
		# to some inactive status. If so, it will need to be removed from the active
		# table.
		
		elsif ( $r->{status} && $r->{status} ne 'changes' )
		{
		    $record_activation{$record_id} = 'delete';
		}
		
		else
		{
		    $r->{status} = 'changes';
		}
		
		# Otherwise, the status can be set or left unchanged according to the value of
		# $r->{status}.
	    }
	    
	    # If we have the 'edit' role on this record, then we can update its values but not its
	    # status. If the current status is 'active', then it will be automatically changed to
	    # 'changes'.
	    
	    elsif ( $permission eq 'edit' )
	    {
		if ( $r->{status} )
		{
		    $request->add_record_warning('W_PERM', $record_label, 
				"you do not have permission to change the status of this record");
		    next;
		}
		
		if ( $current->{status} eq 'active' )
		{
		    $r->{status} = 'changes';
		}
	    }
	    
	    # Otherwise, we do not have permission to edit this record.
	    
	    else
	    {
		$request->add_record_error('E_PERM', $record_label, "you do not have permission to edit this record");
		next;
	    }
	}
	
	# If we do not have a record identifier, then we are adding a new record. This requires
	# different validation checks.
	
	else
	{
	    # Make sure that this operation allows us to create records in the first place.
	    
	    unless ( $conditions{CREATE_RECORDS} )
	    {
		$request->add_record_error('C_CREATE', $record_label, "missing record identifier; this operation cannot create new records");
		next;
	    }
	    
	    # Make sure that we have authorization to add records to this table.
	    
	    my $permission = $request->check_table_permission($dbh, $RESOURCE_QUEUE, 'post');
	    
	    # If we have 'admin' privileges on the resource queue table, then we can add a new
	    # record with any status we choose. The status will default to 'pending' if not
	    # explicitly set.
	    
	    if ( $permission eq 'admin' )
	    {
		$r->{status} ||= 'pending';
	    }
	    
	    # If we have 'post' privileges, we can create a new record. The status will
	    # automatically be set to 'pending', regardless of what is specified in the record.
	    
	    elsif ( $permission eq 'post' )
	    {
		$r->{status} = 'pending';
	    }
	    
	    # Otherwise, we have no ability to do anything at all.
	    
	    else
	    {
		$request->add_record_error('E_PERM', undef, 
				    "you do not have permission to add records");
		next;
	    }
	}
	
	# If $r has a 'tags' field, then look up the tag definitions if necessary and translate
	# the value into a list of integers.
	
	if ( my $tag_list = $r->{tags} )
	{
	    $request->cache_tag_values();
	    
	    my @tags = ref $tag_list eq 'ARRAY' ? @$tag_list : split (/\s*,\s*/, $tag_list);
	    my @tag_ids;
	    
	    foreach my $t ( @tags )
	    {
		if ( $t =~ /^\d+$/ )
		{
		    push @tag_ids, $t;
		}
		
		elsif ( $PB2::ResourceData::TAG_VALUE{lc $t} )
		{
		    push @tag_ids, $PB2::ResourceData::TAG_VALUE{lc $t};
		}
		
		else
		{
		    $request->add_record_warning('E_TAG', $record_label, "unknown resource tag '$t'");
		}
	    }
	    
	    $r->{tags} = join(',', @tag_ids);
	}
	
	# Now validate the fields and construct the lists that will be used to generate an SQL
	# statement to add or update the record.
	
	$request->validate_against_table($dbh, $RESOURCE_QUEUE, $op, $r, 'eduresource_no', \%RESOURCE_IGNORE);
	
	push @good_records, $r;
    }
    
    # If any errors were found in the parameters, stop now and return an HTTP 400 response.
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad request");
    }
    
    # If no good records were found, stop now and return an HTTP 400 response.
    
    unless ( @good_records )
    {
	$request->add_error("E_NO_RECORDS: no valid records for add or update");
	die $request->exception(400, "Bad request");
    }
    
    # Now go through and try to actually add or update these records.
    
    my $edt = EditTransaction->new($dbh, { conditions => \%conditions,
					   debug => $request->debug,
					   auth_info => $auth_info });
    
    # $request->check_edt($edt);
    
    my (%updated_records);
    
    try {

	foreach my $r ( @records )
	{
	    my $record_label = $r->{record_label} || $r->{eduresource_no} || '';
	    
	    # If we have a value for eduresource_no, update the corresponding record.
	    
	    if ( my $record_id = $r->{eduresource_no} )
	    {
		$request->do_update($dbh, $RESOURCE_QUEUE, "eduresource_no = $record_id", $r, \%conditions);
		
		$request->store_image($dbh, $record_id, $r) if $r->{image_data};
		
		$updated_records{$record_id} = 1;
		
		# If this record should be added to or removed from the active table, do so now.
		
		if ( $record_activation{$record_id} )
		{
		    $request->activate_resource($dbh, $r->{eduresource_no}, $record_activation{$record_id});
		}		
	    }
	    
	    else
	    {
		my $new_id = $request->do_add($dbh, $RESOURCE_QUEUE, $r, \%conditions);
		
		$request->store_image($dbh, $new_id, $r) if $new_id && $r->{image_data};
		
		$updated_records{$new_id} = 1 if $new_id;
		
		$request->{my_record_label}{$new_id} = $r->{record_label} if $r->{record_label};
		
		# If this record should be added to the active table, do so now.
		
		if ( $r->{status} eq 'active' )
		{
		    $request->activate_resource($dbh, $new_id, 'copy');
		}
	    }
	}
    }
    
    # If an exception is caught, we roll back the transaction before re-throwing it as an internal
    # error. This will generate an HTTP 500 response.
    
    catch {

	$edt->rollback;
	die $_;
    };
    
    # If any warnings (non-fatal conditions) were detected, add them to the
    # request record so they will be communicated back to the user.
    
    $request->add_edt_warnings($edt);
    
    # If we completed the procedure without any exceptions, but error conditions were detected
    # nonetheless, we also roll back the transaction.
    
    if ( $edt->errors )
    {
	$edt->rollback;
	$request->add_edt_errors($edt);
	die $request->exception(400, "Bad request");
    }
    
    # If the parameter 'strict' was given and warnings were generated, also roll back the
    # transaction.
    
    elsif ( $request->clean_param('strict') && $request->warnings )
    {
	$edt->rollback;
	die $request->exceptions(400, "E_STRICT: warnings were generated");
    }
    
    else
    {
	# If we get here, we're good to go! Yay!!!
	
	$edt->commit;
	
	# Return the indicated information. This will generally be the updated record.
	
	my ($id_string) = join(',', keys %updated_records);
	
	$request->list_updated_resources($dbh, $id_string) if $id_string;
    }
}


sub store_image {
    
    my ($request, $dbh, $eduresource_no, $r) = @_;
    
    return unless $eduresource_no && $r->{image_data};
    
    print STDERR "Storing image:\n" if $request->debug;
    
    my $fh = File::Temp->new( UNLINK => 1 );
    
    unless ( $fh )
    {
	print STDERR "ERROR: could not create temporary file for image\n"
	    if $request->debug;
	return;
    }
    
    my $filename = $fh->filename;
    
    print STDERR "Writing image to $filename\n" if $request->debug;
    
    binmode($fh);
    
    my $base64_data = $r->{image_data};
    $base64_data =~ s/ ^ data: .*? , //xsi;
    
    my $raw_data = MIME::Base64::decode($base64_data);
    
    my $store_data;
    
    print $fh $raw_data;
    
    close $fh;
    
    my $output = `$IMAGE_IDENTIFY_COMMAND $filename`;
    
    print STDERR "Executing: $IMAGE_IDENTIFY_COMMAND $filename\nOutput: $output\n" if $request->debug;
    
    if ( $output =~ qr{ \s (\d+) x (\d+) }xs )
    {
	my $width = $1;
	my $height = $2;
	
	if ( $width > 0 && $height > 0 && $width <= $IMAGE_MAX && $height <= $IMAGE_MAX )
	{
	    $store_data = $r->{image_data};
	}
	
	else
	{
	    my $newsize = $width >= $height ? $IMAGE_MAX : "x$IMAGE_MAX";
	    my $resize_cmd = "convert $filename -resize $newsize -format png -";
	    
	    print STDERR "Executing: $resize_cmd\n" if $request->debug;
	    
	    my $converted_data = `$resize_cmd`;
	    $store_data = "data:image/png;base64," . MIME::Base64::encode_base64($converted_data);
	    
	    print STDERR 'Output: [' . length($store_data) . " chars converted to base64]\n" if $request->debug;
	}
    }
    
    else
    {
	$request->add_error("E_IMAGEDATA: the value of 'image_data' was not an image in a recognized format");
    }
    
    my $sql = "REPLACE INTO $RESOURCE_IMAGES (eduresource_no, image_data) values ($eduresource_no, ?)";
    
    my $result = $dbh->do($sql, { }, $store_data);
    
    my $a = 1;	# we can stop here when debugging
}


sub activate_resource {
    
    my ($request, $dbh, $eduresource_no, $action) = @_;
    
    $dbh ||= $request->get_connection;
    
    my $quoted_id = $dbh->quote($eduresource_no);
    
    unless ( $eduresource_no && $eduresource_no =~ /^\d+$/ )
    {
	die "error activating or inactivating resource: bad resource id";
    }
    
    # If we are directed to delete this resource from the active table, do so.
    
    if ( $action eq 'delete' )
    {
	my $sql = "
		DELETE FROM $RESOURCE_ACTIVE
		WHERE $RESOURCE_IDFIELD = $quoted_id";
	
	print STDERR "$sql\n\n" if $request->debug;
	
	my $result = $dbh->do($sql);
	
	$sql = "	DELETE FROM $RESOURCE_TAGS
		WHERE resource_id = $quoted_id";
	
	print STDERR "$sql\n\n" if $request->debug;
	
	$result = $dbh->do($sql);
	
	# Delete the image file, if any.
	
	$request->delete_image_file($eduresource_no);
		
	my $a = 1;	# we can stop here when debugging
    }
    
    # If the action is 'copy', then we copy the record from the queue table to the active
    # table. The active table has a somewhat different set of fields.
    
    elsif ( $action eq 'copy' )
    {
	# First get the record values to copy.
	
	my $sql = "
		SELECT e.eduresource_no as id, i.image_data, e.*
		FROM $RESOURCE_QUEUE as e left join $RESOURCE_IMAGES as i using (eduresource_no)
		WHERE eduresource_no = $quoted_id";
	
	print STDERR "$sql\n\n" if $request->debug;
	
	my ($r) = $dbh->selectrow_hashref($sql);
	
	# If we can't find the requested resource, throw an exception.
	
	unless ( ref $r eq 'HASH' )
	{
	    die $request->exception(500, "internal error");
	}
	
	# We want the 'authorizer_no' and 'created' fields to be filled in automatically.
	
	delete $r->{authorizer_no};
	delete $r->{created};
	
	# Then handle the image, if any. We need to decode the image data and write it to a file
	# in the proper directory, then stuff the file name into the 'image' field of the mirrored
	# record.
	
	if ( $r->{image_data} )
	{
	    $r->{image} = $request->write_image_file($r->{id}, $r->{image_data});
	}
	
	# Now check the field values.
	
	$request->validate_against_table($dbh, $RESOURCE_ACTIVE, 'mirror', $r, 'id');
	
	# Then copy the record over to the active table.
	
	$request->do_replace($dbh, $RESOURCE_ACTIVE, $r, { });
	
	# Then handle the tags. We first delete any which are there, then create new records for
	# the ones we know are supposed to be there.
	
	$sql =  "	DELETE FROM $RESOURCE_TAGS
		WHERE resource_id = $quoted_id";
	
	print STDERR "$sql\n\n" if $request->debug;
	
	my $result = $dbh->do($sql);
	
	if ( $r->{tags} )
	{
	    my @tags = split /,\s*/, $r->{tags};
	    my @insert;
	    
	    foreach my $t ( @tags )
	    {
		next unless $t =~ /^\d+$/ && $t;
		
		push @insert, "($eduresource_no, $t)";
	    }
	    
	    if ( @insert )
	    {
		my $insert_str = join(', ', @insert);
		
		$sql = "	INSERT INTO $RESOURCE_TAGS (resource_id, tag_id) VALUES $insert_str";
		
		print STDERR "$sql\n\n" if $request->debug;
		
		my $result = $dbh->do($sql);
		
		my $a = 1;	# we can stop here when debugging
	    }
	}
    }
    
    else
    {
	die "invalid activation action '$action'";
    }
}


sub write_image_file {
    
    my ($request, $record_id, $image_data) = @_;
    
    return unless $RESOURCE_IMG_DIR;
    return unless $image_data;
    
    # First, decode the image and determine the type.
    
    my $suffix;
    
    if ( $image_data =~ qr{ ^ data: .*? image/(\w+) }xsi )
    {
	$suffix = $1;
	$image_data =~ s/ ^ data: .*? , //xsi;
    }
    
    my $raw_data = MIME::Base64::decode($image_data);
    
    unless ( $suffix )
    {
	my $first_four = unpack("l", $raw_data);
	
	if ( $first_four == 1196314761 )
	{
	    $suffix = 'png';
	}
	
	elsif ( $first_four == 944130375 )
	{
	    $suffix = 'gif';
	}
	
	elsif ( $first_four == 544099650 )
	{
	    $suffix = 'bmp';
	}
	
	elsif ( $first_four == -520103681 )
	{
	    $suffix = 'jpg';
	}
	
	else
	{
	    $suffix = ".image";
	}
    }
    
    my $filename = "eduresource_$record_id.$suffix";
    my $filepath = "$RESOURCE_IMG_DIR/$filename";

    my $result = open( my $fout, ">", $filepath );

    unless ( $result )
    {
	print STDERR "ERROR: could not open '$filepath' for writing: $!\n";
	$request->add_error("E_INTERNAL ($record_id): could not write image file");
	return;
    }
    
    binmode($fout);
    
    print $fout $raw_data;
    
    close $fout || warn "ERROR: could not write '$filename': $!";
    
    return $filename;
}


sub delete_image_file {

    my ($request, $record_id) = @_;
    
    my $filename = "eduresource_$record_id.png";
    my $filepath = "$RESOURCE_IMG_DIR/$filename";

    if ( -e $filepath )
    {
	unless ( unlink($filepath) )
	{
	    print STDERR "ERROR: could not unlink '$filepath': $!\n";
	    $request->add_error("E_INTERNAL ($record_id): could not delete image file");
	}
    }
}



sub delete_resources {

    my ($request) = @_;

    my $dbh = $request->get_connection;

    # Get the resources to delete from the URL paramters. This operation takes no body.

    my (@id_list) = $request->clean_param_list('eduresource_id');

    # Determine our authentication info.

    my $auth_info = $request->get_auth_info($dbh, $RESOURCE_QUEUE);

    # Then go through the records and validate each one in turn.

    my %delete_ids;
    
    foreach my $record_id ( @id_list )
    {
	next unless $record_id =~ /^\d+$/;
	
	my ($current) = $request->fetch_record($dbh, $RESOURCE_QUEUE, "eduresource_no=$record_id",
					       'eduresource_no, status, authorizer_no, enterer_no, enterer_id');
	
	# If we cannot find the record, then add an error to the request and continue on to
	# the next.
	
	unless ( $current )
	{
	    $request->add_record_warning('W_NOT_FOUND', $record_id, "record not found");
	    next;
	}
	
	my ($permission) = $request->check_record_permission($current, $RESOURCE_QUEUE, 'edit', 'eduresource_no');
	
	# If we have either the 'admin' or 'edit' role on this record, we can delete it.
	
	if ( $permission eq 'admin' || $permission eq 'edit' )
	{
	    $delete_ids{$record_id} = 1;
	}
	
	# Otherwise, we do not have permission to delete this record.
	
	else
	{
	    $request->add_record_warning('W_PERM', $record_id, "you do not have permission to delete this record");
	}
    }

    # Unless we have records that we can delete, return immediately.

    unless ( %delete_ids )
    {
	$request->add_warning('W_NOTHING: nothing to delete');
	return;
    }

    # Otherwise, we create a new EditTransaction and then try to delete the records.
    
    my %conditions;
    
    my $edt = EditTransaction->new($dbh, { conditions => \%conditions,
					   debug => $request->debug,
					   auth_info => $auth_info });

    try {

	my $id_list = join(',', keys %delete_ids);

	my $sql = "
		DELETE FROM $RESOURCE_QUEUE
		WHERE eduresource_no in ($id_list)";

	print STDERR "$sql\n\n" if $request->debug;

	my $result = $dbh->do($sql);
	
	$sql = "
		DELETE FROM $RESOURCE_ACTIVE
		WHERE $RESOURCE_IDFIELD in ($id_list)";
	
	print STDERR "$sql\n\n" if $request->debug;
	
	$result = $dbh->do($sql);
	
	$sql = "
		DELETE FROM $RESOURCE_TAGS
		WHERE resource_id in ($id_list)";
	
	print STDERR "$sql\n\n" if $request->debug;
	
	$result = $dbh->do($sql);
	
	$sql = "
		DELETE FROM $RESOURCE_IMAGES
		WHERE eduresource_no in ($id_list)";
	
	print STDERR "$sql\n\n" if $request->debug;
	
	$result = $dbh->do($sql);
    }
    
    # If an exception is caught, roll back the transaction before re-throwing it as an internal
    # error. This will generate an HTTP 500 response.
	
    catch {

	$edt->rollback;
	die $_;
    };

    # If any warnings (non-fatal conditions) were detected, add them to the
    # request record so they will be communicated back to the user.
    
    $request->add_edt_warnings($edt);
    
    # If we completed the procedure without any exceptions, but error conditions were detected
    # nonetheless, we also roll back the transaction.
    
    if ( $edt->errors )
    {
	$edt->rollback;
	$request->add_edt_errors($edt);
	die $request->exception(400, "Bad request");
    }
    
    # If the parameter 'strict' was given and warnings were generated, also roll back the
    # transaction.
    
    elsif ( $request->clean_param('strict') && $request->warnings )
    {
	$edt->rollback;
	die $request->exceptions(400, "E_STRICT: warnings were generated");
    }
    
    else
    {
	# If we get here, we're good to go! Yay!!!
	
	$edt->commit;
	
	# Return a list of records that were deleted.

	my @results;
	
	foreach my $record_id ( sort keys %delete_ids )
	{
	    push @results, { eduresource_no => generate_identifier('EDR', $record_id),
			     status => 'deleted' };
	}
	
	$request->{main_result} = \@results;
	$request->{result_count} = scalar(@results);
    }
}


sub list_updated_resources {
    
    my ($request, $dbh, $list) = @_;
    
    $request->substitute_select( mt => 'edr', cd => 'edr' );
    
    my $tables = $request->tables_hash;
    
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine the necessary joins.
    
    # my ($join_list) = $request->generate_join_list('tsb', $tables);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT edr.* FROM $RESOURCE_QUEUE as edr
	WHERE edr.eduresource_no in ($list)
	GROUP BY edr.eduresource_no";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    $request->{main_sth} = $dbh->prepare($request->{main_sql});
    $request->{main_sth}->execute();
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
}

1;
