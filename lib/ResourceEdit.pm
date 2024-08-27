# 
# The Paleobiology Database
# 
#   ResourceEdit.pm - a class for handling updates to educational resource records
#   
#   This class is a subclass of EditTransaction, and encapsulates the logic for adding, updating,
#   and deleting educational resource records.
#   
#   Each instance of this class is responsible for initiating a database
#   transaction, checking a set of records for insertion, update, or deletion,
#   and either committing or rolling back the transaction depending on the
#   results of the checks. If the object is destroyed, the transaction will be
#   rolled back. This is all handled by code in EditTransaction.pm.
#   
#   To use it, first call $edt = EditResource->new() with appropriate arguments (see
#   EditTransaction.pm). Then you can call EditResource->add_resource, etc.


package ResourceEdit;

use strict;

use parent 'EditTransaction';

use Carp qw(carp croak);

use TableDefs qw(%TABLE is_test_mode);
use ResourceDefs;

use Role::Tiny::With;

with 'EditTransaction::Mod::MariaDB';
with 'EditTransaction::Mod::PaleoBioDB';

use namespace::clean;

our ($IMAGE_IDENTIFY_COMMAND, $IMAGE_CONVERT_COMMAND, $IMAGE_MAX);
our ($RESOURCE_IDFIELD, $RESOURCE_IMG_DIR);

our (%TAG_ID);


{
    ResourceEdit->register_conditions(
	E_PERM => { status => "You do not have permission to change the status of this record" },
	W_TAG_NOT_FOUND => "Unrecognized resource tag '&1'",
	W_PERM => { status => "The status of this record has been set to 'pending'" });
}

# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# validate_action ( table, operation, action )
# 
# This method is called from EditTransaction.pm to validate each insert and update action.
# We override it to do additional checks before calling validate_against_schema.

sub validate_action {
    
    my ($edt, $action, $operation, $table, $keyexpr) = @_;
    
    my $record = $action->record;
    my $permission = $action->permission;
    
    # First, handle the status.
    
    if ( $operation eq 'insert' )
    {
	if ( $permission eq 'admin' )
	{
	    $record->{status} ||= 'pending';
	    
	    if ( $record->{status} eq 'active' )
	    {
		$action->set_attr(activation => 'copy');
	    }
	}
	
	else
	{
	    if ( $record->{status} && $record->{status} ne 'pending' )
	    {
		$edt->add_condition('W_PERM', 'status');
	    }
	    
	    $record->{status} = 'pending';
	}
    }
    
    elsif ( $operation eq 'update' )
    {
	if ( $permission eq 'admin' )
	{
	    if ( $record->{status} && $record->{status} eq 'active' )
	    {
		$action->set_attr(activation => 'copy');
	    }
	    
	    elsif ( $record->{status} && $record->{status} ne 'changes' )
	    {
		$action->set_attr(activation => 'remove');
	    }
	    
	    else
	    {
		$record->{status} = 'changes';
	    }
	}
	
	else
	{
	    if ( $record->{status} && $record->{status} ne 'changes' )
	    {
		$edt->add_condition('E_PERM', 'status');
	    }
	    
	    $record->{status} = 'changes';
	}
    }

    elsif ( $operation eq 'delete' )
    {
	return;
    }
    
    else
    {
	die "invalid operation '$operation'";
    }
    
    # Then check the tags. Strip out any that aren't known to the database, and add warnings. The
    # rest are translated into the corresponding id numbers.
    
    if ( $record->{tags} )
    {
	my @tags;
	
	foreach my $t ( split /\s*,\s*/, $record->{tags} )
	{
	    if ( my $id = $TAG_ID{lc $t} )
	    {
		push @tags, $id;
	    }
	    
	    else
	    {
		$edt->add_condition('W_TAG_NOT_FOUND', $t);
	    }
	}
	
	$record->{tags} = join(',', @tags) || '';
    }
    
    # If an ORCID is specified, make sure it matches the proper pattern.
    
    if ( defined $record->{orcid} && $record->{orcid} ne '' )
    {
	unless ( $record->{orcid} =~ qr{ ^ \d\d\d\d -? \d\d\d\d -? \d\d\d\d -? \d\d\d\d $ }xs )
	{
	    $edt->add_condition('E_FORMAT', 'orcid', 'not a valid ORCID');
	}
    }
    
    # If image data was specified, check if it needs resizing. The field must be ignored in any
    # case, or else it will trigger an E_BAD_FIELD error.
    
    if ( $record->{image_data} )
    {
	$record->{image_data} = $edt->convert_image($action, $record->{image_data});
    }
    
    $action->ignore_field('image_data');
}


# after_action ( action, operation, table, result )
#
# This method is called from EditTransaction.pm after each successful action.  We override it to handle
# adding and removing records from the active resource table as appropriate.

sub after_action {
    
    my ($edt, $action, $operation, $table, $result) = @_;
    
    # For insert and update operations, we need to deal with image data and
    # record activation.
    
    if ( $operation eq 'insert' || $operation eq 'update' || $operation eq 'replace' )
    {
	my $keyval = $action->keyval;
	my $record = $action->record;
	
	# If the record specifies image data, store it now.
	
	if ( $record->{image_data} )
	{
	    $edt->store_image($keyval, $record->{image_data});
	}
	
	# If the record should be activated or deactivated, do so now. 
	
	if ( my $a = $action->get_attr('activation') )
	{
	    $edt->activate_resource($keyval, $a);
	}
    }
    
    # For delete operations, we need to delete any auxiliary records corresponding to the deleted
    # records in the $RESOURCE_QUEUE table. We also delete any corresponding image files from 
    # the active image directory.
    
    elsif ( $operation eq 'delete' )
    {
	my $dbh = $edt->dbh;
	my $keylist = $action->keylist();
	
	# First get the names of the image files, if any
	
	my $sql = "
		SELECT image FROM $TABLE{RESOURCE_ACTIVE}
		WHERE $RESOURCE_IDFIELD in ($keylist) AND image <> ''";
	
	my $images = $dbh->selectcol_arrayref($sql);
	
	# Then delete the active records, if any
	
	my $sql = "
		DELETE FROM $TABLE{RESOURCE_ACTIVE}
		WHERE $RESOURCE_IDFIELD in ($keylist)";
	
	print STDERR "$sql\n\n" if $edt->debug_mode;
	
	my $res = $dbh->do($sql);
	
	# Then delete the tag assignments, if any.
	
	$sql = "
		DELETE FROM $TABLE{RESOURCE_TAGS}
		WHERE resource_id in ($keylist)";
	
	print STDERR "$sql\n\n" if $edt->debug_mode;
	
	$res = $dbh->do($sql);
	
	# Then delete the image data, if any.
	
	$sql = "
		DELETE FROM $TABLE{RESOURCE_IMAGES}
		WHERE eduresource_no in ($keylist)";
	
	print STDERR "$sql\n\n" if $edt->debug_mode;
	
	$res = $dbh->do($sql);

	# Then unlink the image files, if any.
	
	if ( ref $images eq 'ARRAY' && @$images )
	{
	    foreach my $image_file ( @$images )
	    {
		if ( $image_file && $image_file =~ qr{ ^ T? eduresource_ \d+ }xs )
		{
		    $edt->delete_image_file($image_file);
		}
	    }
	}
    }
}


sub convert_image {
    
    my ($edt, $action, $image_data) = @_;
    
    # return unless $eduresource_no && $r->{image_data};
    
    print STDERR "Converting image:\n" if $edt->debug_mode;
    
    my $fh = File::Temp->new( UNLINK => 1 );
    
    unless ( $fh )
    {
	print STDERR "ERROR: could not create temporary file for image\n"
	    if $edt->debug_mode;
	$edt->add_condition('E_EXECUTE', 'convert image');
	return;
    }
    
    my $filename = $fh->filename;
    
    print STDERR "Writing image to $filename\n" if $edt->debug_mode;
    
    binmode($fh);
    
    my $base64_data = $image_data;
    $base64_data =~ s/ ^ data: .*? , //xsi;
    
    my $raw_data = MIME::Base64::decode($base64_data);
    
    my $store_data;
    
    print $fh $raw_data;
    
    close $fh;
    
    my $output = `$IMAGE_IDENTIFY_COMMAND $filename`;
    
    print STDERR "Executing: $IMAGE_IDENTIFY_COMMAND $filename\nOutput: $output\n" 
	if $edt->debug_mode;
    
    if ( $output =~ qr{ \s (\w+) \s (\d+) x (\d+) }xs )
    {
	my $format = lc $1;
	my $width = $2;
	my $height = $3;
	
	unless ( $format eq 'png' || $format eq 'gif' || $format eq 'gif89a' || $format eq 'bmp' || $format eq 'jpeg' )
	{
	    $edt->add_condition('E_FORMAT', 'image_data', "image format '$format' is not accepted");
	    return;
	}
	
	if ( $width > 0 && $height > 0 && $width <= $IMAGE_MAX && $height <= $IMAGE_MAX )
	{
	    return $image_data;
	}
	
	else
	{
	    my $newsize = $width >= $height ? $IMAGE_MAX : "x$IMAGE_MAX";
	    my $resize_cmd = "convert $filename -resize $newsize -";
	    
	    print STDERR "Executing: $resize_cmd\n" if $edt->debug_mode;
	    
	    my $converted_data = `$resize_cmd`;
	    
	    unless ( $converted_data )
	    {
		$edt->add_condition('E_FORMAT', 'image_data', 'could not convert image');
		return;
	    }
	    
	    $store_data = "data:image/$format;base64," . MIME::Base64::encode_base64($converted_data);
	    
	    print STDERR 'Output: [' . length($store_data) . " chars converted to base64]\n" 
		if $edt->debug_mode;
	    
	    return $store_data;
	}
    }
    
    else
    {
	$edt->add_condition('E_FORMAT', 'image_data', 'unrecognized image format');
	return;
    }
}


sub store_image {
    
    my ($edt, $eduresource_no, $store_data) = @_;
    
    return unless $eduresource_no;
    
    my $dbh = $edt->dbh;
    
    my $sql = "REPLACE INTO $TABLE{RESOURCE_IMAGES} (eduresource_no, image_data) 
		values ($eduresource_no, ?)";
    
    print STDERR "$sql\n\n" if $edt->debug_mode;
    
    my $result = $dbh->do($sql, { }, $store_data);
    
    my $a = 1;	# we can stop here when debugging
}


sub activate_resource {
    
    my ($edt, $eduresource_no, $operation) = @_;
    
    my $dbh = $edt->dbh;
    
    my $quoted_id = $dbh->quote($eduresource_no);
    
    unless ( $eduresource_no && $eduresource_no =~ /^\d+$/ )
    {
	die "error activating or inactivating resource: bad resource id";
    }
    
    # If we are directed to delete this resource from the active table, do so.
    
    if ( $operation eq 'remove' )
    {
	# Start by deleting the image file, if any.
	
	my $sql = "
		SELECT image FROM $TABLE{RESOURCE_ACTIVE}
		WHERE $RESOURCE_IDFIELD = $quoted_id";
	
	print STDERR "$sql\n\n" if $edt->debug_mode;
	
	my ($filename) = $dbh->selectrow_array($sql);
	
	if ( $filename && $filename =~ qr{ ^ T? eduresource_ \d+ }xs )
	{
	    $edt->delete_image_file($filename);
	}
	
	# Then delete the active resource record and the tag assignments.
	
	my $sql = "
		DELETE FROM $TABLE{RESOURCE_ACTIVE}
		WHERE $RESOURCE_IDFIELD = $quoted_id";
	
	print STDERR "$sql\n\n" if $edt->debug_mode;
	
	my $result = $dbh->do($sql);
	
	$sql = "	DELETE FROM $TABLE{RESOURCE_TAGS}
		WHERE resource_id = $quoted_id";
	
	print STDERR "$sql\n\n" if $edt->debug_mode;
	
	$result = $dbh->do($sql);
	
	my $a = 1;	# we can stop here when debugging
    }
    
    # If the action is 'copy', then we copy the record from the queue table to the active
    # table. The active table has a somewhat different set of fields.
    
    elsif ( $operation eq 'copy' )
    {
	# First get the record values to copy.
	
	my $sql = "
		SELECT e.eduresource_no as id, i.image_data, e.*
		FROM $TABLE{RESOURCE_QUEUE} as e left join $TABLE{RESOURCE_IMAGES} as i using (eduresource_no)
		WHERE eduresource_no = $quoted_id";
	
	print STDERR "$sql\n\n" if $edt->debug_mode;
	
	my ($r) = $dbh->selectrow_hashref($sql);
	
	# If we can't find the requested resource, throw an exception.
	
	unless ( ref $r eq 'HASH' )
	{
	    die "resource activation error";
	}
	
	# We want the 'authorizer_no' and 'created' fields to be filled in automatically.
	
	delete $r->{authorizer_no};
	delete $r->{created};
	
	# Then handle the image, if any. We need to decode the image data and write it to a file
	# in the proper directory, then stuff the file name into the 'image' field of the mirrored
	# record.
	
	my $quoted_imagename = "''";
	
	if ( $r->{image_data} )
	{
	    my $image_filename = $edt->write_image_file($r->{id}, $r->{image_data});
	    $quoted_imagename = $dbh->quote($image_filename);
	}
	
	# Now create a new action for this activation, and check the field values. We already know
	# that the user has admin permission on $RESOURCE_QUEUE, so we record them as having admin
	# permission for this action as well.
	
	# my $activation_action = $edt->child_action('replace', 'RESOURCE_ACTIVE', $r);
	
	# $activation_action->ignore_bad_fields;
	
	# $edt->validate_against_schema($activation_action);
	
	# $edt->_handle_action($activation_action);
	
	# Then copy the record over to the active table.
	
	# $edt->execute_replace($activation_action);
	
	my $authorizer_no = $edt->permission->{enterer_no};
	
	my $quoted_auth = $dbh->quote($authorizer_no);
	
	$sql = "REPLACE INTO $TABLE{RESOURCE_ACTIVE} (id, title, description, url,
		    is_video, author, submitter, authorizer_no, created_on, modified, image)
		SELECT eduresource_no, title, description, url, is_video, author,
		    submitter, $quoted_auth, now(), now(), $quoted_imagename
		FROM $TABLE{RESOURCE_QUEUE} WHERE eduresource_no = $quoted_id";
	
	print STDERR "$sql\n\n" if $edt->debug_mode;
	
	my $result = $dbh->do($sql);
	
	# Then handle the tags. We first delete any which are there, then create new records for
	# the ones we know are supposed to be there.
	
	$sql =  "	DELETE FROM $TABLE{RESOURCE_TAGS}
		WHERE resource_id = $quoted_id";
	
	print STDERR "$sql\n\n" if $edt->debug_mode;
	
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
		
		$sql = "	INSERT INTO $TABLE{RESOURCE_TAGS} (resource_id, tag_id) VALUES $insert_str";
		
		print STDERR "$sql\n\n" if $edt->debug_mode;
		
		my $result = $dbh->do($sql);
		
		my $a = 1;	# we can stop here when debugging
	    }
	}
    }
    
    else
    {
	die "invalid activation operation '$operation'";
    }
}


sub write_image_file {
    
    my ($edt, $record_id, $image_data) = @_;
    
    return unless $image_data;
    
    # First, decode the image and determine the type.
    
    my $suffix;
    
    if ( $image_data =~ qr{ ^ data: .*? image/(\w+) }xsi )
    {
	$suffix = $1;
	$image_data =~ s/ ^ data: .*? , //xsi;
    }
    
    my $raw_data = MIME::Base64::decode($image_data);
    
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
	print STDERR "ERROR: cannot decode image format\n";
	$edt->add_condition('E_EXECUTE', 'store image');
	return;
    }
    
    my $filename = "eduresource_$record_id.$suffix";
    
    if ( is_test_mode ) { $filename = "T$filename"; }
    
    my $filepath = "../images/$filename";

    my $result = open( my $fout, ">", $filepath );

    unless ( $result )
    {
	print STDERR "ERROR: could not open '$filepath' for writing: $!\n";
	$edt->add_condition("E_EXECUTE", 'store image');
	return;
    }
    
    binmode($fout);
    
    print $fout $raw_data;
    
    close $fout || warn "ERROR: could not write '$filename': $!";

    print STDERR "\nWrote image file '$filepath'\n\n" if $edt->debug_mode;
    
    return $filename;
}



sub delete_image_file {

    # my ($edt, $record_id) = @_;
    # my $filename = "Teduresource_$record_id.png";
    
    my ($edt, $filename) = @_;

    my $filepath = "../images/$filename";
    
    if ( -e $filepath )
    {
	if ( unlink($filepath) )
	{
	    print STDERR "\nDeleted image file '$filepath'\n\n" if $edt->debug_mode;
	}
	
	else
	{
	    print STDERR "ERROR: could not unlink '$filepath': $!\n";
	    $edt->add_condition('E_EXECUTE', 'delete image file');
	}
    }

    elsif ( $edt->debug_mode )
    {
	print STDERR "\nCould not find image file '$filepath'\n\n";
    }
}


sub configure {
    
    my ($class, $dbh, $config) = @_;
    
    $RESOURCE_IMG_DIR = $config->{eduresource_img_dir};
    
    $IMAGE_IDENTIFY_COMMAND = $config->{image_identify_cmd} || 'identify';
    $IMAGE_CONVERT_COMMAND = $config->{image_convert_cmd} || 'convert';
    $IMAGE_MAX = $config->{image_max_dimension} || 150;
    
    $RESOURCE_IDFIELD = 'id';
    
    # For now, we execute the following in an eval block, so that if something goes wrong it
    # doesn't prevent the entire data service from running.
    
    my $sql = "SELECT * from $TABLE{RESOURCE_TAG_NAMES}";
    
    eval {
	my $taglist = $dbh->selectall_arrayref($sql, { Slice => { } });
	
	foreach my $t ( @$taglist )
	{
	    $TAG_ID{lc $t->{name}} = $t->{id} if $t->{name} && $t->{id};
	}
    }
}


1;
