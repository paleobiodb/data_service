#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the EditTransaction client modules, the subclasses of
# EditTransaction that handle edits to particular kinds of Paleobiology Database records.
#
# edtpc-04-resources.t : Test ResourceEdit, which handles educational resource records.
#                        Anybody who is logged in may submit a resource, which is queued
#                        with the status 'pending'. Only an administrator can change the
#                        status. Making a resource 'active' copies it to the active table
#                        along with its tags, and changing the status to anything else
#                        except 'changes' removes it from there.
#
# Added by Claude under the guidance of Michael McClennen.
#
# The conversion of submitted images requires ImageMagick, so it is tested only if the
# 'identify' and 'convert' commands are available. They are available in the pbapi container.
# The image files written when a resource is activated are tested, using a temporary
# directory as the image directory.

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 12;

use File::Temp qw(tempdir);
use MIME::Base64 qw(encode_base64 decode_base64);

use TableDefs qw(%TABLE);

use EDTPCTest;
use PBDBTester;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_has_condition ok_has_one_condition
		  ok_no_conditions clear_table sql_command sql_fetchrow ok_found_record
		  ok_no_record);


# Establish the tester and select the test tables.

my $T = PBDBTester->new('ResourceEdit');

EDTPCTest->select_test_tables('ResourceEdit');

my @TABLES = qw(RESOURCE_QUEUE RESOURCE_ACTIVE RESOURCE_IMAGES RESOURCE_TAGS RESOURCE_TAG_NAMES);

my $IMG_DIR = tempdir(CLEANUP => 1);

# A one-pixel PNG image, as a data URI.

my $PNG = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==';

sub perm {
    
    my ($name) = @_;
    
    return $T->new_perm("SESSION-$name", 'RESOURCE_QUEUE');
}

sub new_edt {
    
    my ($name, @rest) = @_;
    
    return ok_new_edt(class => 'ResourceEdit', permission => perm($name), @rest);
}


# Return the active record for the specified resource as a string, or the empty string if
# there is none.

sub active_record {
    
    my ($id) = @_;
    
    my ($title, $authorizer_no) = $T->dbh->selectrow_array("
		SELECT title, authorizer_no FROM $TABLE{RESOURCE_ACTIVE} WHERE id = $id");
    
    return defined $title ? "$title | $authorizer_no" : '';
}

sub active_tags {
    
    my ($id) = @_;
    
    my $tags = $T->dbh->selectcol_arrayref("
		SELECT tag_id FROM $TABLE{RESOURCE_TAGS} WHERE resource_id = $id ORDER BY tag_id");
    
    return join(',', @$tags);
}


# Clear the tables and establish two tags. ResourceEntry.pm calls 'configure' before
# creating any transactions, and that is where the tag names are read.

subtest 'setup' => sub {
    
    clear_table($_) foreach @TABLES;
    
    sql_command("INSERT INTO <<RESOURCE_TAG_NAMES>> (id, name) VALUES (1, 'Fossils'), (2, 'Teaching')");
    
    is( $ResourceEdit::RESOURCE_IDFIELD, 'id', "the id field is defined before 'configure' is called" );
    
    ResourceEdit->configure($T->dbh, { eduresources_img_dir => $IMG_DIR });
    
    is( $ResourceEdit::TAG_ID{fossils}, 1, "the tag names were read" );
    
    is( $ResourceEdit::RESOURCE_IMG_DIR, $IMG_DIR, "the image directory was configured" );
};


# A logged-in user may submit a resource, which is always given the status 'pending'.

subtest 'submit' => sub {
    
    my $edt = new_edt('ENTERER');
    
    $edt->insert_record('RESOURCE_QUEUE', { title => 'first resource', orcid => '0000-0002-1825-0097' });
    
    ok_commit("a member can submit a resource");
    
    ok_no_conditions("there were no warnings");
    
    ok_found_record('RESOURCE_QUEUE', "title = 'first resource' and status = 'pending'
		    and enterer_no = 39997");
    
    $edt = new_edt('ENTERER');
    
    $edt->insert_record('RESOURCE_QUEUE', { title => 'wants active', status => 'active' });
    
    ok_commit("a member can submit a resource with a status");
    
    ok_has_one_condition('W_PERM', "a warning was given that the status was changed");
    
    ok_found_record('RESOURCE_QUEUE', "title = 'wants active' and status = 'pending'");
    
    is( active_record(2), '', "the resource was not activated" );
    
    $edt = new_edt('GUEST');
    
    $edt->insert_record('RESOURCE_QUEUE', { title => 'guest resource' });
    
    ok_commit("a logged-in guest can submit a resource");
    
    ok_found_record('RESOURCE_QUEUE', "title = 'guest resource' and enterer_id = 'USERID-GUEST'");
};


# Check the field validation done by ResourceEdit: the title is required, the ORCID must
# have the proper form, and the tags are translated into id numbers.

subtest 'validation' => sub {
    
    my $edt = new_edt('ENTERER');
    
    $edt->insert_record('RESOURCE_QUEUE', { description => 'no title' });
    
    ok_failed_commit("a resource must have a title");
    
    ok_has_one_condition('E_REQUIRED', qr/'title'/, "the missing title was reported");
    
    $edt = new_edt('ENTERER');
    
    $edt->insert_record('RESOURCE_QUEUE', { title => 'bad orcid', orcid => '1234-5678' });
    
    ok_failed_commit("a malformed ORCID was rejected");
    
    ok_has_one_condition('E_FORMAT', qr/'orcid'/, "the malformed ORCID was reported");
    
    $edt = new_edt('ENTERER');
    
    $edt->insert_record('RESOURCE_QUEUE', { title => 'tagged', tags => 'teaching, FOSSILS, unknown' });
    
    ok_commit("a resource with tags was submitted");
    
    ok_has_one_condition('W_TAG_NOT_FOUND', qr/'unknown'/, "the unknown tag was reported");
    
    ok_found_record('RESOURCE_QUEUE', "title = 'tagged' and tags = '2,1'",
		    "the known tags were stored as id numbers");
};


# The owner of a resource may change it, which sets its status to 'changes', but may not
# set the status to anything else. Other users may not change it at all.

subtest 'update by owner' => sub {
    
    my $edt = new_edt('ENTERER');
    
    $edt->update_record('RESOURCE_QUEUE', { eduresource_no => 1, title => 'first changed' });
    
    ok_commit("the owner can change a resource");
    
    ok_found_record('RESOURCE_QUEUE', "eduresource_no = 1 and title = 'first changed'
		    and status = 'changes'");
    
    $edt = new_edt('ENTERER');
    
    $edt->update_record('RESOURCE_QUEUE', { eduresource_no => 1, status => 'active' });
    
    ok_failed_commit("the owner cannot change the status");
    
    ok_has_one_condition('E_PERMISSION', qr/status/, "the refusal was reported");
    
    $edt = new_edt('OTHER');
    
    $edt->update_record('RESOURCE_QUEUE', { eduresource_no => 1, title => 'not mine' });
    
    ok_failed_commit("another member cannot change the resource");
    
    ok_has_one_condition('E_PERMISSION', "the refusal was reported");
    
    $edt = new_edt('GUEST');
    
    $edt->update_record('RESOURCE_QUEUE', { eduresource_no => 3, title => 'guest changed' });
    
    ok_commit("a guest can change a resource they submitted");
    
    ok_found_record('RESOURCE_QUEUE', "eduresource_no = 3 and title = 'guest changed'");
};


# An administrator can activate a resource, which copies it and its tags into the active
# table. Changing it without specifying a status sets the status to 'changes' and leaves
# the active copy alone. Setting any other status removes the active copy.

subtest 'activation' => sub {
    
    my $edt = new_edt('SUPERUSER');
    
    $edt->update_record('RESOURCE_QUEUE', { eduresource_no => 1, status => 'active',
					    tags => 'Fossils, Teaching' });
    
    ok_commit("an administrator can activate a resource");
    
    ok_found_record('RESOURCE_QUEUE', "eduresource_no = 1 and status = 'active'");
    
    is( active_record(1), 'first changed | 39999',
	"the resource was copied to the active table, with the administrator as authorizer" );
    
    is( active_tags(1), '1,2', "the tags were copied" );
    
    $edt = new_edt('SUPERUSER');
    
    $edt->update_record('RESOURCE_QUEUE', { eduresource_no => 1, title => 'first again' });
    
    ok_commit("an administrator can change an active resource");
    
    ok_found_record('RESOURCE_QUEUE', "eduresource_no = 1 and title = 'first again'
		    and status = 'changes'");
    
    is( active_record(1), 'first changed | 39999', "the active copy was not changed" );
    
    $edt = new_edt('SUPERUSER');
    
    $edt->update_record('RESOURCE_QUEUE', { eduresource_no => 1, status => 'pending' });
    
    ok_commit("an administrator can set the status back to 'pending'");
    
    is( active_record(1), '', "the active copy was removed" );
    
    is( active_tags(1), '', "the active tags were removed" );
    
    $edt = new_edt('SUPERUSER');
    
    $edt->insert_record('RESOURCE_QUEUE', { title => 'admin active', status => 'active' });
    
    ok_commit("an administrator can submit an active resource");
    
    my ($key) = $edt->inserted_keys;
    
    is( active_record($key), 'admin active | 39999', "the new resource was activated" );
    
    $edt = new_edt('SUPERUSER');
    
    $edt->insert_record('RESOURCE_QUEUE', { title => 'admin pending' });
    
    ok_commit("an administrator can submit a resource without a status");
    
    ok_found_record('RESOURCE_QUEUE', "title = 'admin pending' and status = 'pending'");
};


# A user with the 'admin' permission on RESOURCE_QUEUE counts as an administrator.

subtest 'table admin' => sub {
    
    $T->set_specific_permission('RESOURCE_QUEUE', perm('WITH-ADMIN'), 'admin');
    
    my $edt = new_edt('WITH-ADMIN');
    
    $edt->update_record('RESOURCE_QUEUE', { eduresource_no => 2, status => 'active' });
    
    ok_commit("a table administrator can activate a resource");
    
    is( active_record(2), 'wants active | 39991', "the resource was activated" );
    
    $T->clear_specific_permissions('RESOURCE_QUEUE');
};


# Submitted images are checked with ImageMagick. Images in an accepted format that are no
# larger than 150 pixels in either dimension are stored as they are. Larger ones are reduced
# to fit, and images in other formats are rejected.

# Return a solid-color image of the specified size and format, as a data URI.

sub make_image {
    
    my ($size, $format) = @_;
    
    my $raw = `$ResourceEdit::IMAGE_CONVERT_COMMAND -size $size xc:red $format:- 2>/dev/null`;
    
    return $raw ? "data:image/$format;base64," . encode_base64($raw, '') : '';
}

# Return the format and size of the image stored for the specified resource.

sub stored_image {
    
    my ($key) = @_;
    
    my ($data) = $T->dbh->selectrow_array("
		SELECT image_data FROM $TABLE{RESOURCE_IMAGES} WHERE eduresource_no = $key");
    
    return '' unless $data;
    
    $data =~ s/ ^ data: .*? , //xs;
    
    my $tmp = File::Temp->new;
    binmode $tmp;
    print $tmp decode_base64($data);
    close $tmp;
    
    my $filename = $tmp->filename;
    
    my ($format, $size) = `$ResourceEdit::IMAGE_IDENTIFY_COMMAND $filename` =~ / \s (\w+) \s (\d+x\d+) \s /xs;
    
    return "$format $size";
}

subtest 'image conversion' => sub {
    
    plan skip_all => "ImageMagick is not available"
	unless `$ResourceEdit::IMAGE_IDENTIFY_COMMAND -version 2>/dev/null` &&
	`$ResourceEdit::IMAGE_CONVERT_COMMAND -version 2>/dev/null`;
    
    my $small = make_image('20x10', 'png');
    
    ok( $small, "a test image was generated" ) || return;
    
    my $edt = new_edt('ENTERER');
    
    $edt->insert_record('RESOURCE_QUEUE', { title => 'small image', image_data => $small });
    
    ok_commit("a resource with a small image was submitted");
    
    my ($key) = $edt->inserted_keys;
    
    ok_found_record('RESOURCE_IMAGES', "eduresource_no = $key and image_data = '$small'",
		    "the small image was stored unchanged");
    
    $edt = new_edt('ENTERER');
    
    $edt->insert_record('RESOURCE_QUEUE', { title => 'wide image',
					    image_data => make_image('300x200', 'png') });
    $edt->insert_record('RESOURCE_QUEUE', { title => 'tall image',
					    image_data => make_image('100x400', 'jpeg') });
    
    ok_commit("resources with large images were submitted");
    
    my ($wide, $tall) = $edt->inserted_keys;
    
    is( stored_image($wide), "PNG 150x100", "the wide image was reduced to fit" );
    is( stored_image($tall), "JPEG 38x150", "the tall image was reduced to fit" );
    
    $edt = new_edt('ENTERER');
    
    $edt->insert_record('RESOURCE_QUEUE', { title => 'tiff image',
					    image_data => make_image('20x20', 'tiff') });
    
    ok_failed_commit("an image in an unaccepted format was rejected");
    
    ok_has_one_condition('E_FORMAT', qr/'tiff' is not accepted/, "the format was reported");
    
    $edt = new_edt('ENTERER');
    
    $edt->insert_record('RESOURCE_QUEUE', { title => 'not an image',
					    image_data => 'data:image/png;base64,' .
					    encode_base64('not an image', '') });
    
    ok_failed_commit("data that is not an image was rejected");
    
    ok_has_one_condition('E_FORMAT', qr/unrecognized image format/, "the problem was reported");
    
    ok_no_record('RESOURCE_QUEUE', "title in ('tiff image', 'not an image')");
};


# When a resource with an image is activated, the image is written to a file in the image
# directory and the file name is stored in the active record. In test mode the file name
# starts with 'T'. The file is deleted when the resource is deactivated or deleted.

subtest 'images' => sub {
    
    my $edt = new_edt('ENTERER');
    
    $edt->insert_record('RESOURCE_QUEUE', { title => 'with image' });
    
    ok_commit("a resource was submitted");
    
    my ($key) = $edt->inserted_keys;
    
    sql_command("INSERT INTO <<RESOURCE_IMAGES>> (eduresource_no, image_data) VALUES ($key, '$PNG')");
    
    $edt = new_edt('SUPERUSER');
    
    $edt->update_record('RESOURCE_QUEUE', { eduresource_no => $key, status => 'active' });
    
    ok_commit("the resource was activated");
    
    my $filename = "Teduresource_$key.png";
    
    ok_found_record('RESOURCE_ACTIVE', "id = $key and image = '$filename'",
		    "the image file name was stored in the active record");
    
    ok( -s "$IMG_DIR/$filename", "the image file was written to the image directory" );
    
    $edt = new_edt('SUPERUSER');
    
    $edt->update_record('RESOURCE_QUEUE', { eduresource_no => $key, status => 'pending' });
    
    ok_commit("the resource was deactivated");
    
    ok( ! -e "$IMG_DIR/$filename", "the image file was deleted" );
    
    $edt = new_edt('SUPERUSER');
    
    $edt->update_record('RESOURCE_QUEUE', { eduresource_no => $key, status => 'active' });
    
    ok_commit("the resource was activated again");
    
    ok( -s "$IMG_DIR/$filename", "the image file was written again" );
    
    $edt = new_edt('SUPERUSER');
    
    $edt->delete_record('RESOURCE_QUEUE', $key);
    
    ok_commit("the resource was deleted");
    
    ok( ! -e "$IMG_DIR/$filename", "the image file was deleted" );
    
    ok_no_record('RESOURCE_IMAGES', "eduresource_no = $key");
};


# Deleting a resource also deletes its active copy, its tag assignments, and its image
# data. The owner of a resource may delete it, but other members may not.

subtest 'delete' => sub {
    
    sql_command("INSERT INTO <<RESOURCE_IMAGES>> (eduresource_no, image_data) VALUES (2, 'xyz')");
    
    is( active_tags(2), '', "the resource has no tags" );
    
    sql_command("INSERT INTO <<RESOURCE_TAGS>> (resource_id, tag_id) VALUES (2, 1)");
    
    my $edt = new_edt('SUPERUSER');
    
    $edt->delete_record('RESOURCE_QUEUE', 2);
    
    ok_commit("an administrator can delete a resource");
    
    ok_no_record('RESOURCE_QUEUE', "eduresource_no = 2");
    
    is( active_record(2), '', "the active copy was deleted" );
    
    is( active_tags(2), '', "the tag assignments were deleted" );
    
    ok_no_record('RESOURCE_IMAGES', "eduresource_no = 2");
    
    $edt = new_edt('ENTERER');
    
    $edt->insert_record('RESOURCE_QUEUE', { title => 'to be deleted by owner' });
    
    ok_commit("a resource was submitted");
    
    my ($key) = $edt->inserted_keys;
    
    $edt = new_edt('OTHER');
    
    $edt->delete_record('RESOURCE_QUEUE', $key);
    
    ok_failed_commit("another member cannot delete the resource");
    
    ok_has_one_condition('E_PERMISSION', "the refusal was reported");
    
    $edt = new_edt('ENTERER');
    
    $edt->delete_record('RESOURCE_QUEUE', $key);
    
    ok_commit("the owner can delete the resource");
    
    ok_no_record('RESOURCE_QUEUE', "eduresource_no = $key");
    
    $edt = new_edt('GUEST');
    
    $edt->delete_record('RESOURCE_QUEUE', 3);
    
    ok_commit("a guest can delete a resource they submitted");
    
    ok_no_record('RESOURCE_QUEUE', "eduresource_no = 3");
};


# A resource can be identified by an external identifier of type 'EDR', using either the
# primary key column or the field 'eduresource_id'.

subtest 'external identifiers' => sub {
    
    my $edt = new_edt('ENTERER');
    
    $edt->update_record('RESOURCE_QUEUE', { eduresource_id => 'edr:1', title => 'by extid' });
    $edt->update_record('RESOURCE_QUEUE', { eduresource_no => 'edr:4', title => 'by extid too' });
    
    ok_commit("resources were updated using external identifiers");
    
    is( join(',', sort $edt->updated_keys), '1,4', "the keys were unpacked" );
    
    $edt = new_edt('ENTERER');
    
    $edt->update_record('RESOURCE_QUEUE', { eduresource_id => 'ref:1', title => 'wrong type' });
    
    ok_failed_commit("an identifier of the wrong type was rejected");
    
    ok_has_one_condition('E_EXTID', "the wrong identifier type was reported");
};


# ResourceEntry.pm uses insert_update_record, with positional arguments to 'new'.

subtest 'insert_update_record' => sub {
    
    my $edt = ResourceEdit->new($T->dbh, perm('ENTERER'), 'RESOURCE_QUEUE', { });
    
    isa_ok( $edt, 'ResourceEdit', "the transaction" );
    
    $edt->insert_update_record('RESOURCE_QUEUE', { title => 'insupdate new' });
    $edt->insert_update_record('RESOURCE_QUEUE', { eduresource_no => 1, title => 'insupdate old' });
    
    ok_commit($edt, "a resource was inserted and another updated");
    
    ok_found_record('RESOURCE_QUEUE', "title = 'insupdate new' and status = 'pending'");
    ok_found_record('RESOURCE_QUEUE', "eduresource_no = 1 and title = 'insupdate old'
		    and status = 'changes'");
};


# A replace operation is handled as an update if the record exists, and as an insert if it
# does not. The fields that are not specified are cleared, but the ownership is kept.

subtest 'replace' => sub {
    
    my $edt = new_edt('SUPERUSER');
    
    $edt->replace_record('RESOURCE_QUEUE', { eduresource_no => 1, title => 'replaced',
					     status => 'active' });
    
    ok_commit("an administrator can replace a resource");
    
    is( active_record(1), 'replaced | 39999', "the replaced resource was activated" );
    
    $edt = new_edt('ENTERER');
    
    $edt->replace_record('RESOURCE_QUEUE', { eduresource_no => 1, title => 'owner replaced',
					     tags => 'teaching' });
    
    ok_commit("the owner can replace a resource");
    
    ok_found_record('RESOURCE_QUEUE', "eduresource_no = 1 and title = 'owner replaced'
		    and status = 'changes' and tags = '2' and description = ''
		    and enterer_no = 39997", "the record was replaced and the owner kept");
    
    is( active_record(1), 'replaced | 39999', "the active copy was not changed" );
    
    $edt = new_edt('ENTERER');
    
    $edt->replace_record('RESOURCE_QUEUE', { eduresource_no => 1, title => 'owner active',
					     status => 'active' });
    
    ok_failed_commit("the owner cannot set the status with a replace");
    
    ok_has_one_condition('E_PERMISSION', qr/status/, "the refusal was reported");
    
    $edt = new_edt('ENTERER');
    
    $edt->replace_record('RESOURCE_QUEUE', { eduresource_no => 1, description => 'no title' });
    
    ok_failed_commit("a replacement must have a title");
    
    ok_has_one_condition('E_REQUIRED', qr/'title'/, "the missing title was reported");
    
    $edt = new_edt('SUPERUSER', 'INSERT_KEY');
    
    $edt->replace_record('RESOURCE_QUEUE', { eduresource_no => 500, title => 'new by replace' });
    $edt->replace_record('RESOURCE_QUEUE', { eduresource_no => 501, title => 'new active',
					     status => 'active' });
    
    ok_commit("an administrator can create resources with a replace");
    
    ok_found_record('RESOURCE_QUEUE', "eduresource_no = 500 and status = 'pending'",
		    "a new resource was given the status 'pending'");
    
    is( active_record(500), '', "it was not activated" );
    
    is( active_record(501), 'new active | 39999', "a new resource with status 'active' was activated" );
};
