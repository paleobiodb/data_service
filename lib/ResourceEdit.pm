# 
# The Paleobiology Database
# 
#   ResourceEdit.pm - a class for handling updates to educational resource records
#   
#   This class is a subclass of EditTransaction, and encapsulates the logic for adding, updating,
#   and deleting educational resource records.
#   
#   Each instance of this class is responsible for initiating a database transaction, checking a
#   set of records for insertion, update, or deletion, and either committing or rolling back the
#   transaction depending on the results of the checks. If the object is destroyed, the transaction will
#   be rolled back. This is all handled by code in EditTransaction.pm.
#   
#   To use it, first call $edt = EditResource->new() with appropriate arguments (see
#   EditTransaction.pm). Then you can call EditResource->add_resource, etc.


package ResourceEdit;

use strict;

use Carp qw(carp croak);
use Try::Tiny;

use TableDefs qw($RESOURCE_ACTIVE $RESOURCE_QUEUE $RESOURCE_IMAGES $RESOURCE_TAG_NAMES $RESOURCE_TAGS is_test_mode);
use ResourceTables;

use base 'EditTransaction';


our ($IMAGE_IDENTIFY_COMMAND, $IMAGE_CONVERT_COMMAND, $IMAGE_MAX);
our ($RESOURCE_IDFIELD, $RESOURCE_IMG_DIR);

our (%TAG_ID);

our (%CONDITION_TEMPLATE) = (
		E_IMAGEDATA => "Image data error: %1",
		E_PERM => "You do not have permission to change the status of this record",
		W_TAG_NOT_FOUND => "Unrecognized resource tag '%1'",
		W_PERM => "The status of this record has been set to 'pending'");


# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# get_condition_template ( code, table, selector )
#
# Return the proper template for error and warning conditions defined by this subclass. If no
# matching template can be found, we call the parent method. Other subclasses of EditTransaction
# should do something similar.

sub get_condition_template {
    
    my ($edt, $code, $table, $selector) = @_;

    if ( $CONDITION_TEMPLATE{$code} )
    {
	return $CONDITION_TEMPLATE{$code};
    }
    
    else
    {
	return $edt->SUPER::get_condition_template($code, $table, $selector);
    }
}


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
		$edt->add_record_condition('W_PERM', 'status');
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
		$action->set_attr(activation => 'delete');
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
		$edt->add_record_condition('E_PERM', 'status');
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
		$edt->add_record_condition('W_TAG_NOT_FOUND', $t);
	    }
	}
	
	$record->{tags} = join(',', @tags) || '';
    }
    
    # Then call the regular validation routine.
    
    $edt->validate_against_schema($action, $operation, $table);
}


# after_action ( action, operation, table, result )
#
# This method is called from EditTransaction.pm after each action, whether or not it is
# successful.  We override it to handle adding and removing records from the active resource table
# as appropriate.

sub after_action {
    
    my ($edt, $action, $operation, $table, $result) = @_;
    
    # For insert and update operations, we need to deal with image data and
    # record activation.
    
    if ( $operation ne 'delete' )
    {
	my $keyval = $action->keyval;
	my $record = $action->record;
	
	# If the record specifies image data, store it now.
	
	if ( $record->{image_data} )
	{
	    $edt->store_image($keyval, $record);
	}
	
	# If the record should be activated or deactivated, do so now. 
	
	if ( my $a = $action->get_attr('activation') )
	{
	    $edt->activate_resource($keyval, $a);
	}
    }
    
    else
    {
	my $dbh = $edt->dbh;
	my $keylist = $edt->get_keylist($action);
	
	my $sql = "
		DELETE FROM $RESOURCE_ACTIVE
		WHERE $RESOURCE_IDFIELD in ($keylist)";
	
	print STDERR "$sql\n\n" if $edt->debug;
	
	$result = $dbh->do($sql);
	
	$sql = "
		DELETE FROM $RESOURCE_TAGS
		WHERE resource_id in ($keylist)";
	
	print STDERR "$sql\n\n" if $edt->debug;
	
	$result = $dbh->do($sql);
	
	$sql = "
		DELETE FROM $RESOURCE_IMAGES
		WHERE eduresource_no in ($keylist)";
	
	print STDERR "$sql\n\n" if $edt->debug;
	
	$result = $dbh->do($sql);
    }
}


sub store_image {
    
    my ($edt, $eduresource_no, $r) = @_;
    
    return unless $eduresource_no && $r->{image_data};
    
    print STDERR "Storing image:\n" if $edt->debug;
    
    my $dbh = $edt->dbh;
    
    my $fh = File::Temp->new( UNLINK => 1 );
    
    unless ( $fh )
    {
	print STDERR "ERROR: could not create temporary file for image\n"
	    if $edt->debug;
	return;
    }
    
    my $filename = $fh->filename;
    
    print STDERR "Writing image to $filename\n" if $edt->debug;
    
    binmode($fh);
    
    my $base64_data = $r->{image_data};
    $base64_data =~ s/ ^ data: .*? , //xsi;
    
    my $raw_data = MIME::Base64::decode($base64_data);
    
    my $store_data;
    
    print $fh $raw_data;
    
    close $fh;
    
    my $output = `$IMAGE_IDENTIFY_COMMAND $filename`;
    
    print STDERR "Executing: $IMAGE_IDENTIFY_COMMAND $filename\nOutput: $output\n" if $edt->debug;
    
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
	    
	    print STDERR "Executing: $resize_cmd\n" if $edt->debug;
	    
	    my $converted_data = `$resize_cmd`;
	    $store_data = "data:image/png;base64," . MIME::Base64::encode_base64($converted_data);
	    
	    print STDERR 'Output: [' . length($store_data) . " chars converted to base64]\n" if $edt->debug;
	}
    }
    
    else
    {
	$edt->add_record_condition('E_IMAGEDATA', 'unrecognized image format');
    }
    
    my $sql = "REPLACE INTO $RESOURCE_IMAGES (eduresource_no, image_data) values ($eduresource_no, ?)";
    
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
    
    if ( $operation eq 'delete' )
    {
	my $sql = "
		DELETE FROM $RESOURCE_ACTIVE
		WHERE $RESOURCE_IDFIELD = $quoted_id";
	
	print STDERR "$sql\n\n" if $edt->debug;
	
	my $result = $dbh->do($sql);
	
	$sql = "	DELETE FROM $RESOURCE_TAGS
		WHERE resource_id = $quoted_id";
	
	print STDERR "$sql\n\n" if $edt->debug;
	
	$result = $dbh->do($sql);
	
	# Delete the image file, if any.
	
	$edt->delete_image_file($eduresource_no);
		
	my $a = 1;	# we can stop here when debugging
    }
    
    # If the action is 'copy', then we copy the record from the queue table to the active
    # table. The active table has a somewhat different set of fields.
    
    elsif ( $operation eq 'copy' )
    {
	# First get the record values to copy.
	
	my $sql = "
		SELECT e.eduresource_no as id, i.image_data, e.*
		FROM $RESOURCE_QUEUE as e left join $RESOURCE_IMAGES as i using (eduresource_no)
		WHERE eduresource_no = $quoted_id";
	
	print STDERR "$sql\n\n" if $edt->debug;
	
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
	
	if ( $r->{image_data} )
	{
	    $r->{image} = $edt->write_image_file($r->{id}, $r->{image_data});
	}
	
	# Now create a new action for this activation, and check the field values.
	
	my $activation_action = EditAction->new($RESOURCE_ACTIVE, 'replace', $r);
	
	$edt->set_permission($activation_action);
	$edt->validate_against_schema($activation_action);
	
	# Then copy the record over to the active table.
	
	$edt->execute_action($activation_action);
	
	# Then handle the tags. We first delete any which are there, then create new records for
	# the ones we know are supposed to be there.
	
	$sql =  "	DELETE FROM $RESOURCE_TAGS
		WHERE resource_id = $quoted_id";
	
	print STDERR "$sql\n\n" if $edt->debug;
	
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
		
		print STDERR "$sql\n\n" if $edt->debug;
		
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
    
    if ( is_test_mode ) { $filename = "T$filename"; }
    
    my $filepath = "$RESOURCE_IMG_DIR/$filename";

    my $result = open( my $fout, ">", $filepath );

    unless ( $result )
    {
	print STDERR "ERROR: could not open '$filepath' for writing: $!\n";
	$edt->add_record_condition("E_EXECUTE", 'store image');
	return;
    }
    
    binmode($fout);
    
    print $fout $raw_data;
    
    close $fout || warn "ERROR: could not write '$filename': $!";
    
    return $filename;
}



sub delete_image_file {

    my ($edt, $record_id) = @_;
    
    my $filename = "eduresource_$record_id.png";
    my $filepath = "$RESOURCE_IMG_DIR/$filename";

    if ( -e $filepath )
    {
	unless ( unlink($filepath) )
	{
	    print STDERR "ERROR: could not unlink '$filepath': $!\n";
	    $edt->add_record_condition("E_EXECUTE", 'delete image');
	}
    }
}


sub configure {
    
    my ($class, $dbh, $config) = @_;
    
    $RESOURCE_IMG_DIR = $config->{eduresources_img_dir};
    
    $IMAGE_IDENTIFY_COMMAND = $config->{image_identify_cmd} || '/opt/local/bin/identify';
    $IMAGE_CONVERT_COMMAND = $config->{image_convert_cmd} || '/opt/local/bin/convert';
    $IMAGE_MAX = $config->{image_max_dimension} || 150;
    
    $RESOURCE_IDFIELD = 'id';
    
    # For now, we execute the following in an eval block, so that if something goes wrong it
    # doesn't prevent the entire data service from running.
    
    my $sql = "SELECT * from $RESOURCE_TAG_NAMES";
    
    eval {
	my $taglist = $dbh->selectall_arrayref($sql, { Slice => { } });
	
	foreach my $t ( @$taglist )
	{
	    $TAG_ID{lc $t->{name}} = $t->{id} if $t->{name} && $t->{id};
	}
    }
}


1;
