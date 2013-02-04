#
# NexusEdit.pm
# 
# Created by Michael McClennen, 2013-01-31
# 
# The purpose of this module is to provide for uploading, downloading and
# editing for taxonomic nexus files.

package Nexusfile;
use strict;

use Nexusfile;
use Constants qw($READ_URL $WRITE_URL $HTML_DIR $PAGE_TOP $PAGE_BOTTOM);

use CGI;
#use CGI::Carp;
use Digest::MD5;
use Data::Dumper;
use Class::Date qw(now date);
#use Debug qw(dbg);
#use Person;

my $UPLOAD_LIMIT = 1048576;

our ($SQL_STRING, $ERROR_STRING);

# displayUploadPage ( dbt, hbo, q, s )
#
# Display the web page that allows an authorized user to upload nexus files to
# the database.

sub displayUploadPage {
    
    my ($dbt, $hbo, $q, $s) = @_;
    
    # First make sure that the user is properly authenticated.
    
    unless ($s->isDBMember())
    {
	login( "Please log in first.");
	exit;
    }
    
    # Then produce the upload page.
    
    my ($vars) = { page_title => "Upload a nexus file" };
    
    print $hbo->stdIncludes($PAGE_TOP);
    print $hbo->populateHTML('nexus_upload', $vars);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}


# processUpload ( dbt, hbo, q, s )
#
# Display the web page that allows an authorized user to upload nexus files to
# the database.

sub processUpload {
    
    my ($dbt, $hbo, $q, $s) = @_;
    
    # First make sure that the user is properly authenticated.
    
    unless ($s->isDBMember())
    {
	login( "Please log in first.");
	exit;
    }
    
    # Then extract the upload values and file contents.
    
    my $upload_file = $q->param('nexus_file');
    my $upload_fh = $q->upload('nexus_file');
    my $notes = $q->param('notes');
    my $replace = $q->param('replace');
    
    my $values = { notes => $notes, replace => $replace };
    
    # Test that we actually got a file, and that no error occurred.
    
    unless ( $upload_file )
    {
	return uploadError($hbo, $q, "Please choose a file to upload", $values);
    }
    
    if( $q->cgi_error )
    {
	print $q->header(-status=>$q->cgi_error);
	exit;
    }
    
    # Read the data into memory, to a maximum of 1 MB.
    
    my ($nexus_data, $buffer, $bytes_read, $total_bytes);
    
    while ( $bytes_read = read($upload_fh, $buffer, $UPLOAD_LIMIT) )
    {
	$nexus_data .= $buffer;
	$total_bytes += $bytes_read;
	
	if ( $total_bytes >= $UPLOAD_LIMIT )
	{
	    return uploadError($hbo, $q, "The file was too big.<br/>Please choose a file smaller than 1 MB.",
			       $values);
	}
    }
    
    # Make sure this is an actual nexus file.  If it is, make sure each line
    # ends with a Unix newline.
    
    unless ( $nexus_data =~ /^\#nexus/i )
    {
	return uploadError($hbo, $q, "The file you chose was not a valid nexus file.<br/>Please choose a different file.",
			   $values);
    }
    
    $nexus_data =~ s/\r\n?/\n/g;
    $values->{nexus_file} = $upload_file;
    
    # Compute the MD5 checksum, as a further check for uniqueness.
    
    my $digester = Digest::MD5->new()->add($buffer);
    my $md5_digest = $digester->hexdigest();
    
    #my ($n) = getNexusFileInfo($dbt, undef, { md5_digest => $md5_digest,
    #					      authorizer_no => $s->{authorizer_no} });
    
    #if ( $n and not $replace and $n->{filename} ne $upload_filename )
    #{
    #	my $filename = $n->{filename};
    #	$values->{replace_box} = 1;
    #	return uploadError($hbo, $q, "The file you chose was already uploaded to the database as $filename.<br/>Check the box below if you want to rename it.", $values);
    #}
    
    # At this point, we have a valid upload.  Now we need to save the file.
    # The first step is to create the necessary directory if it doesn't
    # already exist (one directory per authorizer):
    
    $values->{nexus_file} = $upload_file;
    
    my $nexus_dir = checkUploadDirectory($s);
    
    unless ( $nexus_dir )
    {
	return uploadError($hbo, $q, "Error: $!<br/>Please contact the administrator.",
			   $values);
    }
    
    # Next, we need to check whether there is an existing file by that name.
    # If so, the user must choose whether to replace it.
    
    my ($nexusfile) = getNexusFileInfo($dbt, undef, { filename => $upload_file, 
						      authorizer_no => $s->{authorizer_no} });
    
    if ( $nexusfile and not $replace )
    {
	$values->{replace_box} = 1;
	return uploadError($hbo, $q, "You have already uploaded a file with that name.<br/>Check the box below if you want to replace the existing one.", $values);
    }
    
    # Create the new file, and write the data to it.
    
    my $nexus_filename = "$nexus_dir/$upload_file";
    my ($out_fh);
    
    unless ( open($out_fh, ">$nexus_filename") )
    {
	return uploadError($hbo, $q, "Error: $!<br/>Please contact the administrator.");
    }
    
    print $out_fh $nexus_data;
    
    unless ( close($out_fh) )
    {
	return uploadError($hbo, $q, "Error: $!<br/>Please contact the administrator.");
    }
    
    # Create a new record for this file, or replace the existing one.
    
    my $nexusfile_no = addNexusFile($dbt, $upload_file, $s->{authorizer_no}, $s->{enterer_no}, 
				    $notes, $md5_digest);
    
    generateNexusTaxa($dbt, $nexusfile_no, $nexus_data) if $nexusfile_no > 0;
    
    unless ( $nexusfile_no )
    {
	return uploadError($hbo, $q, "Error: $ERROR_STRING<br/>Please contact the administrator.");
    }
    
    # If we succeeded, view the new file for editing (so that the user can
    # confirm that the data was uploaded properly, and can add any publication
    # references). 
    
    print $q->redirect( -uri => "$READ_URL?a=editNexusFile&nexusfile_no=$nexusfile_no");
}


sub uploadError {

    my ($hbo, $q, $message, $oldvalues) = @_;
    
    $oldvalues = {} unless ref $oldvalues eq 'HASH';
    
    my ($vars) = { page_title => "Upload a nexus file", error_message => $message, %$oldvalues };
    
    print $q->header(-type => "text/html", 
                     -Cache_Control=>'no-cache',
                     -expires =>"now" );
    
    print $hbo->stdIncludes($PAGE_TOP);
    print $hbo->populateHTML('nexus_upload', $vars);
    print $hbo->stdIncludes($PAGE_BOTTOM);
    
    return;
}


# checkUploadDirectory ( s )
# 
# Make sure that an upload directory exists for the current user.  Return the
# directory name if it exists and is writeable, and the undefined value
# (false) otherwise.

sub checkUploadDirectory {

    my ($s) = @_;
    
    # First make sure that the main nexus file directory exists and is writeable.
    
    my $base_name = "$HTML_DIR/public/nexus";
    
    return undef unless -w $base_name;
    
    # Then see if we have a directory for the current authorizer.  If it
    # exists but is not writeable, return false (signalling an error).
    
    my $authorizer_no = $s->get('authorizer_no');
    return undef unless $authorizer_no;
    
    my $dir_name = "$base_name/$authorizer_no";
    
    return $dir_name if -w $dir_name;
    return undef if -e $dir_name;
    
    # If the directory doesn't already exist, create it and check to see that
    # it exists and is writeable.
    
    my $result = mkdir($dir_name);
    
    return undef unless $result;
    return $dir_name if -w $dir_name;
    return undef; # if we get here, the operation failed.
}


# editFile ( dbt, hbo, q, s )
# 
# Edit the file given by the URL arguments.

sub editFile {

    my ($dbt, $hbo, $q, $s) = @_;
    
    my $dbh = $dbt->{dbh};
    
    # First figure out which file we're supposed to be editing.
    
    my $nexusfile_no = $q->param('nexusfile_no');
    
    unless ( $nexusfile_no > 0 )
    {
	return editError($hbo, $q, "No matching nexus file was found.");
    }
    
    # Check that we have permission to edit the file.  If not, redirect to the
    # "view file" page.
    
    my $current_auth = $s->get('authorizer_no');
    my ($file_auth) = $dbh->selectrow_array("
		SELECT authorizer_no FROM nexus_files WHERE nexusfile_no=$nexusfile_no");
    
    unless ( $current_auth == $file_auth && $current_auth > 0 )
    {
	$q->param('noperm', 1);
	viewFile($dbt, $hbo, $q, $s);
	exit;
    }
    
    # If we are returning from selecting a reference to add, then add it.
    
    if ( $q->param('add_ref') )
    {
	my $reference_no = $q->param('reference_no') || $s->get('reference_no');
	
	addNexusReference($dbt, $nexusfile_no, $reference_no);
    }
    
    # Otherwise, check all of the taxa that are not exactly linked up to see
    # if we can link any of them more exactly.  (We don't need to do this when
    # returning from editing a reference, because in that case we already did
    # it when the edit page was initially displayed).
    
    else
    {
	updateNexusTaxa($dbt, $nexusfile_no);
    }
    
    # Grab all of the info currently known about the file.
    
    my ($nexusfile) = getNexusFileInfo($dbt, $nexusfile_no, { fields => 'all' });
    
    unless ( $nexusfile )
    {
	return editError($hbo, $q, "No matching nexus file was found.");
    }
    
    my $auth_no = $nexusfile->{authorizer_no};
    my $filename = $nexusfile->{filename};
    
    my $vars = { page_title => "Edit nexus file",
		 filename => $filename,
		 date_created => $nexusfile->{created},
		 date_modified => $nexusfile->{modified},
		 download_link => "/public/nexus/$auth_no/$filename",
	         nexusfile_no => $nexusfile_no };
    
    my $content = readFile($dbt, $nexusfile_no);
    my ($reference_string) = extractRefString($content);
    
    if ( $reference_string )
    {
	$vars->{reference_string} = $reference_string;
    }
    
    # If there are associated references, list them.
    
    if ( ref $nexusfile->{refs} eq 'ARRAY' and @{$nexusfile->{refs}} )
    {
	$vars->{references} = "<ul>\n";
	
	foreach my $ref (@{$nexusfile->{refs}})
	{
	    my $ref_no = $ref->{reference_no};
	    $vars->{references} .= "<li>" . 
		qq%(<a href="$READ_URL?a=displayRefResults&reference_no=$ref_no">$ref_no</a>)&nbsp;% .
		    Reference::formatAsHTML($ref) . 
			    qq%&nbsp; [<a href="#" onclick="delRef($ref_no)">delete</a>]% .
				qq%&nbsp; [<a href="#" onclick="moveRef($ref_no)">top</a>]% .
				    "</li>\n";
	}
	
	$vars->{references} .= "</ul>\n";
    }
    
    else
    {
	$vars->{references} = '<b>No references have been linked to this nexus file</b>';
    }
    
    # If there are associated taxa, list them.
    
    if ( ref $nexusfile->{taxa} eq 'ARRAY' )
    {
	$vars->{taxa} = "<ul>\n";
	my $asterisk;
	my $double;
	
	foreach my $t (@{$nexusfile->{taxa}})
	{
	    my $name;
	    
	    if ( $t->{taxon_no} > 0 )
	    {
		$name = qq%<a href="$READ_URL?a=basicTaxonInfo&taxon_no=$t->{taxon_no}">$t->{taxon_name}</a>%;
		
		if ( $t->{inexact} )
		{
		    $name .= " **";
		    $double = 1;
		}
	    }
	    
	    else
	    {
		$name = $t->{taxon_name} . " *";
		$asterisk = 1;
	    }
	    
	    $vars->{taxa} .= "<li>$name</li>\n";
	}
	
	$vars->{taxa} .= "</ul>\n";
	
	if ( $asterisk )
	{
	    $vars->{taxa} .= "<p>* No matching taxon was found in the database</p>\n";
	}
	
	if ( $double )
	{
	    $vars->{taxa} .= "<p>** A matching taxon was found, but not an exact match</p>\n";
	}
    }
    
    else
    {
	$vars->{linked_taxa} = '<b>No taxa were found in this nexus file</b>';
    }
    
    if ( $nexusfile->{notes} )
    {
	$vars->{notes} = $nexusfile->{notes};
    }
    
    # Now generate the editing page.
    
    print $hbo->stdIncludes($PAGE_TOP);
    print $hbo->populateHTML('nexus_edit', $vars);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}


sub editError {

    my ($hbo, $q, $message, $oldvalues) = @_;
    
    $oldvalues = {} unless ref $oldvalues eq 'HASH';
    
    my ($vars) = { page_title => "Edit nexus file", error_message => $message, %$oldvalues };
    
#    print $q->header(-type => "text/html", 
#                     -Cache_Control=>'no-cache',
#                     -expires =>"now" );
    
    print $hbo->stdIncludes($PAGE_TOP);
    print $hbo->populateHTML('nexus_edit', $vars);
    print $hbo->stdIncludes($PAGE_BOTTOM);
    
    return;
}


# viewFile ( dbt, hbo, q, s )
# 
# Edit the file given by the URL arguments.

sub viewFile {

    my ($dbt, $hbo, $q, $s) = @_;
    
    # First figure out which file we're supposed to be viewing, and grab all
    # of the info currently known about the file.
    
    my $nexusfile_no = $q->param('nexusfile_no');
    my $vars;
    
    my ($nexusfile) = getNexusFileInfo($dbt, $nexusfile_no, { fields => 'all' });
    
    unless ( $nexusfile )
    {
	$vars = { page_title => "View nexus file", 
		  error_message => "No matching nexus file was found" };
	$nexusfile = {};
    }
    
    else
    {
	my $auth_no = $nexusfile->{authorizer_no};
	my $filename = $nexusfile->{filename};
	
	$vars = { page_title => "View nexus file",
		  filename => $filename,
		  date_created => $nexusfile->{created},
		  date_modified => $nexusfile->{modified},
		  nexusfile_no => $nexusfile_no,
		  download_link => "/public/nexus/$auth_no/$filename" };
	
	if ( $q->param('noperm') )
	{
	    $vars->{error_message} = "You do not have permission to edit this file.";
	}
    }
    
    my $content = readFile($dbt, $nexusfile_no);
    my ($reference_string) = extractRefString($content);
    
    if ( $reference_string )
    {
	$vars->{reference_string} = $reference_string;
    }
    
    # If there are associated references, list them.
    
    if ( ref $nexusfile->{refs} eq 'ARRAY' and @{$nexusfile->{refs}} )
    {
	$vars->{references} = "<ul>\n";
	
	foreach my $ref (@{$nexusfile->{refs}})
	{
	    my $ref_no = $ref->{reference_no};
	    $vars->{references} .= "<li>" . 
		qq%(<a href="$READ_URL?a=displayRefResults&reference_no=$ref_no">$ref_no</a>)&nbsp;% .
		    Reference::formatAsHTML($ref) . "</li>\n";
	}
	
	$vars->{references} .= "</ul>\n";
    }
    
    elsif ( $nexusfile->{nexusfile_no} )
    {
	$vars->{references} = '<b>No references have been linked to this nexus file</b>';
    }
    
    # If there are associated taxa, list them.
    
    if ( ref $nexusfile->{taxa} eq 'ARRAY' )
    {
	$vars->{taxa} = "<ul>\n";
	my $asterisk;
	my $double;
	
	foreach my $t (@{$nexusfile->{taxa}})
	{
	    my $name;
	    
	    if ( $t->{taxon_no} > 0 )
	    {
		$name = qq%<a href="$READ_URL?a=basicTaxonInfo&taxon_no=$t->{taxon_no}">$t->{taxon_name}</a>%;
		
		if ( $t->{inexact} )
		{
		    $name .= " **";
		    $double = 1;
		}
	    }
	    
	    else
	    {
		$name = $t->{taxon_name} . " *";
		$asterisk = 1;
	    }
	    
	    $vars->{taxa} .= "<li>$name</li>\n";
	}
	
	$vars->{taxa} .= "</ul>\n";
	
	if ( $asterisk )
	{
	    $vars->{taxa} .= "<p>* No matching taxon was found in the database</p>\n";
	}
	
	if ( $double )
	{
	    $vars->{taxa} .= "<p>** A matching taxon was found, but not an exact match</p>\n";
	}
    }
    
    else
    {
	$vars->{linked_taxa} = '<b>No taxa were found in this nexus file</b>';
    }
    
    if ( $nexusfile->{notes} )
    {
	$vars->{notes} = $nexusfile->{notes};
    }
    
    # Now generate the editing page.
    
    print $hbo->stdIncludes($PAGE_TOP);
    print $hbo->populateHTML('nexus_view', $vars);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}


# processEdit ( dbt, hbo, q, s )
# 
# Update the record for the given file, as indicated by the URL arguments.
# This routine is called when a button is clicked on the "edit nexus file"
# page.

sub processEdit {
    
    my ($dbt, $hbo, $q, $s) = @_;
    
    my $dbh = $dbt->{dbh};
    
    # First make sure we know which file we're working with.  If we don't have
    # a nexusfile_no value, go to the main menu.
    
    my $nexusfile_no = $q->param('nexusfile_no');
    $nexusfile_no =~ tr/0-9//dc;
    
    unless ( $nexusfile_no > 0 )
    {
	print $q->redirect( -uri => "$READ_URL");
    }
    
    # Then make sure that the user has permission to edit this file.  If not,
    # redirect to the view page with a 'no permission' message.
    
    my $current_auth = $s->get('authorizer_no');
    my ($file_auth) = $dbh->selectrow_array("
		SELECT authorizer_no FROM nexus_files WHERE nexusfile_no=$nexusfile_no");
    
    unless ( $current_auth == $file_auth && $current_auth > 0 )
    {
	print $q->redirect( -uri => "$READ_URL?a=viewNexusFile&nexusfile_no=$nexusfile_no&noperm=1");
    }
    
    # Are we adding a new reference?  If so, display the reference-selection
    # page and queue up the editing page so that it will display as soon as a
    # reference is selected.
    
    if ( $q->param('addReference') )
    {
        print $q->header(-type => "text/html", 
                     -Cache_Control=>'no-cache',
                     -expires =>"now" );
	$s->enqueue("action=editNexusFile&nexusfile_no=$nexusfile_no&add_ref=1");
	main::displaySearchRefs("Select a reference to associate with the nexus file:");
	exit;
    }
    
    # Are we deleting an existing reference?  If so, do it and redisplay the
    # edit page.
    
    elsif ( my $reference_no = $q->param('deleteReference') )
    {
	deleteNexusReference($dbt, $nexusfile_no, $reference_no);
	print $q->redirect( -uri => "$READ_URL?a=editNexusFile&nexusfile_no=$nexusfile_no");
	exit;
    }
    
    # Are we moving an existing reference?  If so, do it and redisplay the
    # edit page.
    
    elsif ( my $reference_no = $q->param('moveReference') )
    {
	moveNexusReference($dbt, $nexusfile_no, $reference_no);
	print $q->redirect( -uri => "$READ_URL?a=editNexusFile&nexusfile_no=$nexusfile_no");
	exit;
    }
    
    # Are we deleting the entire nexus file?  If so, do it and redirect to the
    # main menu.
    
    elsif ( $q->param('deleteNexusFile') )
    {
	deleteNexusFile($dbt, $nexusfile_no);
	print $q->redirect( -uri => "$READ_URL");
	exit;
    }
    
    # Are we saving a new version of the notes?  If so, do that and redirect
    # to the main menu.
    
    elsif ( $q->param('saveNexusInfo') )
    {
	my $notes = $q->param('notes');
	updateFileInfo($dbt, $nexusfile_no, $notes);
	print $q->redirect( -uri => "$READ_URL");
	exit;
    }
    
    # Otherwise, just redirect to the main menu.
    
    else
    {
	print $q->redirect( -uri => "$READ_URL");
	exit;
    }
}


# addNexusFile ( dbt, filename, authorizer_no, enterer_no, notes, md5_digest )
# 
# Add a nexus file using the given information.  If there is an existing
# record with the given filename and authorizer_no, it will be updated.
# Otherwise, a new record will be created.  In either case, return the
# nexusfile_no.  If an error occurs, return 0.

sub addNexusFile {
    
    my ($dbt, $filename, $authorizer_no, $enterer_no, $notes, $md5_digest) = @_;
    
    my $dbh = $dbt->{dbh};
    my $result;
    
    # Do some basic checking
    
    unless ( $filename =~ /\w/ )
    {
	$ERROR_STRING = "The filename must not be empty";
	return 0;
    }
    
    unless ( $authorizer_no > 0 )
    {
	$ERROR_STRING = "You must be logged in";
	return 0;
    }
    
    # First, see if there is already a matching record.
    
    my $dbh = $dbt->{dbh};
    my $quoted_name = $dbh->quote($filename);
    $authorizer_no =~ tr/0-9//dc;
    $enterer_no =~ tr/0-9//dc; $enterer_no += 0;
    
    $SQL_STRING = "
		SELECT nexusfile_no FROM nexus_files
		WHERE filename=$quoted_name and authorizer_no=$authorizer_no";
    
    my ($nexusfile_no) = $dbh->selectrow_array($SQL_STRING);
    
    # If there is, then we update it.
    
    my $quoted_notes = $dbh->quote($notes);
    my $quoted_digest = $dbh->quote($md5_digest);
    
    if ( $nexusfile_no > 0 )
    {
	$SQL_STRING = "
		UPDATE nexus_files
		SET enterer_no=$enterer_no, notes=$quoted_notes, md5_digest=$quoted_digest, modified=now()
		WHERE nexusfile_no=$nexusfile_no";
	
	$result = $dbh->do($SQL_STRING);
	
	unless ( $result )
	{
	    $ERROR_STRING = "Update error A in NexusEdit.pm: nexusfile_no=$nexusfile_no, filename=$quoted_name, authorizer_no=$authorizer_no";
	    return 0;
	}
	
	else
	{
	    return $nexusfile_no;
	}
    }
    
    # Otherwise, we create a new record.
    
    else
    {
	$SQL_STRING = "
		INSERT INTO nexus_files (filename, authorizer_no, enterer_no, notes, md5_digest, created, modified)
		VALUES ($quoted_name, $authorizer_no, $enterer_no, $quoted_notes, $quoted_digest, now(), now())";
	
	$result = $dbh->do($SQL_STRING);
	
	unless ( $result )
	{
	    $ERROR_STRING = "Insert error A in NexusEdit.pm: filename=$quoted_name, authorizer_no=$authorizer_no";
	    return 0;
	}
	
	else
	{
	    $nexusfile_no = $dbh->last_insert_id(undef, undef, undef, undef);
	    return $nexusfile_no;
	}
    }
}


# deleteNexusFile ( dbt, nexusfile_no )
# 
# Delete the specified nexus file, plus all of its associated information.
# This operation will not be carried out unless the currently logged-in user
# has the same authorizer_no as is associated with the file.

sub deleteNexusFile {
    
    my ($dbt, $nexusfile_no) = @_;
    
    $nexusfile_no =~ tr/0-9//dc;
    return unless $nexusfile_no > 0;
    
    # First get the filename, and authorizer_no, so we can remove the file
    # from the directory where it is stored.
    
    my $dbh = $dbt->{dbh};
    
    my ($filename, $authorizer_no) = $dbh->selectrow_array("
		SELECT filename, authorizer_no FROM nexus_files
		WHERE nexusfile_no=$nexusfile_no");
    
    return unless $filename;
    
    # Now remove the actual file.
    
    unlink("$HTML_DIR/public/nexus/$authorizer_no/$filename");
    
    # Then remove the database rows.
    
    my $result;
    
    $result = $dbh->do("DELETE FROM nexus_files WHERE nexusfile_no=$nexusfile_no");
    $result = $dbh->do("DELETE FROM nexus_refs WHERE nexusfile_no=$nexusfile_no");
    $result = $dbh->do("DELETE FROM nexus_taxa WHERE nexusfile_no=$nexusfile_no");
    
    my $a = 1;		# we can stop here when debugging.
}


# updateFileInfo ( dbt, nexusfile_no, new_notes )
# 
# Update the specified nexus file record with the new 'notes' string.

sub updateFileInfo {

    my ($dbt, $nexusfile_no, $new_notes) = @_;
    
    my $dbh = $dbt->{dbh};
    
    $nexusfile_no =~ tr/0-9//dc;
    return unless $nexusfile_no > 0;
    
    my $quoted = $dbh->quote($new_notes);
    
    $SQL_STRING = "UPDATE nexus_files SET notes=$quoted, modified=now()
		   WHERE nexusfile_no=$nexusfile_no";
    
    my $result = $dbh->do($SQL_STRING);
    
    my $a = 1;	# we can stop here when debugging
}


# addNexusReference ( dbt, nexusfile_no, reference_no )
# 
# Associated the specified reference with the specified file.

sub addNexusReference {
    
    my ($dbt, $nexusfile_no, $reference_no) = @_;
    
    my $dbh = $dbt->{dbh};
    
    $nexusfile_no =~ tr/0-9//dc;
    $reference_no =~ tr/0-9//dc;
    
    return unless $nexusfile_no > 0 and $reference_no > 0;
    
    my ($lastindex) = $dbh->selectrow_array("SELECT max(index_no) FROM nexus_refs
					     WHERE nexusfile_no=$nexusfile_no");
    
    my $index_no = $lastindex + 1;
    
    my $result = $dbh->do("INSERT IGNORE INTO nexus_refs (nexusfile_no, reference_no, index_no)
			   VALUES ($nexusfile_no, $reference_no, $index_no)");
    
    $result = $dbh->do("UPDATE nexus_files SET modified=now()
			WHERE nexusfile_no=$nexusfile_no");
    
    return;
}


# deleteNexusReference ( dbt, nexusfile_no, reference_no )
# 
# Un-associate the specified reference with the specified file.

sub deleteNexusReference {

    my ($dbt, $nexusfile_no, $reference_no) = @_;
    
    my $dbh = $dbt->{dbh};
    
    $nexusfile_no =~ tr/0-9//dc;
    $reference_no =~ tr/0-9//dc;
    
    return unless $nexusfile_no > 0 and $reference_no > 0;
    
    my ($del_index) = $dbh->selectrow_array("
		SELECT index_no FROM nexus_refs
		WHERE nexusfile_no=$nexusfile_no and reference_no=$reference_no");
    
    my $result = $dbh->do("
		DELETE FROM nexus_refs
		WHERE nexusfile_no=$nexusfile_no and reference_no=$reference_no");
    
    if ( $del_index > 0 )
    {
	$result = $dbh->do("
		UPDATE nexus_refs SET index_no=index_no-1
		WHERE nexusfile_no=$nexusfile_no and index_no>$del_index");
    }
    
    $result = $dbh->do("UPDATE nexus_files SET modified=now()
			WHERE nexusfile_no=$nexusfile_no");
    
    return;
}


# moveNexusReference ( dbt, nexusfile_no, reference_no )
# 
# Reorder the references for the specified nexus file so that the specified
# reference is the first one (i.e. the primary reference).  By executing a
# sequence of such actions, any desired order may be obtained.

sub moveNexusReference {

    my ($dbt, $nexusfile_no, $reference_no) = @_;
    
    my $dbh = $dbt->{dbh};
    
    $nexusfile_no =~ tr/0-9//dc;
    $reference_no =~ tr/0-9//dc;
    
    return unless $nexusfile_no > 0 and $reference_no > 0;
    
    my ($del_index) = $dbh->selectrow_array("
		SELECT index_no FROM nexus_refs
		WHERE nexusfile_no=$nexusfile_no and reference_no=$reference_no");
    
    my $result;
    
    if ( $del_index > 0 )
    {
	$result = $dbh->do("
		UPDATE nexus_refs SET index_no=index_no+1
		WHERE nexusfile_no=$nexusfile_no and index_no<$del_index");
    }
    
    else
    {
	$result = $dbh->do("
		UPDATE nexus_refs SET index_no=index_no+1
		WHERE nexusfile_no=$nexusfile_no");
    }
    
    $result = $dbh->do("
		UPDATE nexus_refs SET index_no=1
		WHERE nexusfile_no=$nexusfile_no and reference_no=$reference_no");
    
    $result = $dbh->do("UPDATE nexus_files SET modified=now()
			   WHERE nexusfile_no=$nexusfile_no");
    
    return;
}


# generateNexusTaxa ( dbt, nexusfile_no, content )
# 
# Given the content of a nexus file, generate a list of associated taxa.
# Delete the old associated taxa and insert the new list the order they are
# specified in the file.

sub generateNexusTaxa {

    my ($dbt, $nexusfile_no, $contents) = @_;
    
    my $dbh = $dbt->{dbh};
    
    # First delete all existing associated taxa for this file.
    
    $SQL_STRING = "DELETE FROM nexus_taxa WHERE nexusfile_no = $nexusfile_no";
    
    my $result = $dbh->do($SQL_STRING);
    
    # Then parse the content to get a list of included taxa.
    
    my @file_taxa;
    my $matrix_block;
    
    foreach my $line (split /\n/, $contents)
    {
	if ( $matrix_block ne 'done' and $line =~ /^MATRIX/i )
	{
	    $matrix_block = 'in';
	}
	
	elsif ( $matrix_block eq 'in' )
	{
	    if ( $line =~ /^END;/i )
	    {
		$matrix_block = 'done';
		last;	# take this out if we need ever need to look for other stuff
			# after the end of the matrix block
	    }
	    
	    elsif ( $line =~ /^(\w+)/ or $line =~ /^'([^']+)'/ )
	    {
		my $taxon_name = $1;
		$taxon_name =~ s/_+/ /g;
		
		push @file_taxa, $taxon_name;
	    }
	}
    }
    
    my $index_no = 1;
    my @values;
    
    foreach my $name (@file_taxa)
    {
	my ($t) = TaxonInfo::getTaxa($dbt, { taxon_name => $name });
	my $inexact = 'false';
	
	unless ( $t )
	{
	    $inexact = 'true';
	    
	    if ( $name =~ /^(\w+)\s+\w/ )
	    {
		($t) = TaxonInfo::getTaxa($dbt, { taxon_name => $1 });
		
		unless ( $t )
		{
		    ($t) = TaxonInfo::getTaxa($dbt, { taxon_name => $1, match_subgenera => 1 });
		}
	    }
	    
	    else
	    {
		($t) = TaxonInfo::getTaxa($dbt, { taxon_name => $name, match_subgenera => 1 });
	    }
	}
	
	my $quoted = $dbh->quote($name);
	my $taxon_no = $t->{taxon_no} || "0";
	
	push @values, "($nexusfile_no, $quoted, $taxon_no, $index_no, $inexact)";
	$index_no++;
    }
    
    if ( @values )
    {
	$SQL_STRING = "
		INSERT INTO nexus_taxa (nexusfile_no, taxon_name, orig_no, index_no, inexact)
		VALUES " . join(',', @values);
	
	$result = $dbh->do($SQL_STRING);
    }
    
    $a = 1;	# we can stop here when debugging
}


# updateNexusTaxa ( dbt, nexusfile_no )
# 
# For the specified nexus file, find all of its associated taxa where the
# match was inexact and try again to match them against the list of known taxa.

sub updateNexusTaxa {

    my ($dbt, $nexusfile_no) = @_;
    
    my $dbh = $dbt->{dbh};
    
    $SQL_STRING = "
		SELECT t.orig_no as taxon_no, t.taxon_name, t.index_no
		FROM nexus_taxa as t
		WHERE t.nexusfile_no=$nexusfile_no and (t.inexact or t.orig_no=0)";
    
    my ($taxa_list) = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
    
    return unless ref $taxa_list eq 'ARRAY';
    
    foreach my $old_t (@$taxa_list)
    {
	my $name = $old_t->{taxon_name};
	my $inexact = 'false';
	
	my ($t) = TaxonInfo::getTaxa($dbt, { taxon_name => $name });
	
	unless ( $t )
	{
	    $inexact = 'true';
	    
	    if ( $name =~ /^(\w+)\s+\w/ )
	    {
		($t) = TaxonInfo::getTaxa($dbt, { taxon_name => $1 });
		
		unless ( $t )
		{
		    ($t) = TaxonInfo::getTaxa($dbt, { taxon_name => $1, match_subgenera => 1 });
		}
	    }
	    
	    else
	    {
		($t) = TaxonInfo::getTaxa($dbt, { taxon_name => $name, match_subgenera => 1 });
	    }
	}
	
	next unless $t;
	next if $t->{taxon_no} == $old_t->{taxon_no};
	
	# If we get here, then we have found a better match than we had before.
	
	my $quoted = $dbh->quote($name);
	
	$SQL_STRING = "UPDATE nexus_taxa SET orig_no=$t->{taxon_no}, inexact=$inexact
		       WHERE nexusfile_no=$nexusfile_no and taxon_name=$quoted";
	
	my $result = $dbh->do($SQL_STRING);
	
	my $a = 1;	# we can stop here when debugging
    }

}


sub readFile {
    
    my ($dbt, $nexusfile_no) = @_;
    
    my $dbh = $dbt->{dbh};
    
    # First, get the basic info.
    
    my ($nexusfile) = getNexusFileInfo($dbt, $nexusfile_no);
    
    return unless $nexusfile;
    
    # Next, open the file and read it in.  Look for a comment indicating a
    # list of references, and also look for taxa in the MATRIX block.
    
    my $filename = $nexusfile->{filename};
    my $authorizer_no = $nexusfile->{authorizer_no};
    
    my $pathname = "$HTML_DIR/public/nexus/$authorizer_no/$filename";
    my $in_fh;
    
    return unless open($in_fh, $pathname);
    
    my $line;
    my $contents = '';
    
    while ( defined($line = <$in_fh>) )
    {
	$contents .= $line;
    }
    
    close($in_fh); # we don't care what the result is, because we're just reading
    
    return $contents;
}


sub extractRefString {

    my ($contents) = @_;
    
    my ($ref_block);
    my $reference_string = '';
    
    foreach my $line (split /\n/, $contents)
    {
	my ($refstring);
	
	if ( $ref_block ne 'done' and $line =~ /^\[!(.*)/ )
	{
	    $refstring = $1;
	    $ref_block = 'in';
	}
	
	elsif ( $ref_block eq 'in' )
	{
	    $refstring = $line;
	}
	
	if ( $refstring )
	{
	    if ( $refstring =~ /^(.*)\]$/ )
	    {
		$ref_block = 'done';
		$refstring = $1;
	    }
	    
	    if ( $refstring =~ /\w/ )
	    {
		$reference_string .= "<p><i>$refstring</i></p>\n";
	    }
	}
    }
    
    return $reference_string;
}


# displaySearchPage ( dbt, hbo, q, s )
#
# Display the web page that allows anyone to search for nexus files.

sub displaySearchPage {
    
    my ($dbt, $hbo, $q, $s) = @_;
    
    # Produce the search page.
    
    my ($vars) = { page_title => "Search for nexus files",
		   current_ref => $s->get("reference_no"),
		   enterer_me => $s->get('enterer_reversed'),
		   file_name => scalar($q->param('file_name')),
		   taxon_name => scalar($q->param('taxon_name')),
		   person_reversed => scalar($q->param('person_reversed')),
		   reference_no => scalar($q->param('reference_no')),
		 };
    
    print $hbo->stdIncludes($PAGE_TOP);
    print $hbo->populateHTML('nexus_search', $vars);
    print $hbo->stdIncludes($PAGE_BOTTOM);
}


# processSearch ( dbt, hbo, q, s )
# 
# Process a search request

sub processSearch {

    my ($dbt, $hbo, $q, $s) = @_;
    
    my $dbh = $dbt->{dbh};
    
    # First get the parameters.
    
    my $filename = $q->param('file_name');
    my $taxon_name = $q->param('taxon_name');
    my $authorizer = $q->param('person_reversed');
    my $reference_no = $q->param('reference_no');
    
    my $values = { file_name => $filename, taxon_name => $taxon_name, 
		   person_reversed => $authorizer, reference_no => $reference_no };
    
    my $valid_criteria;
    my $options = {};
    
    if ( $filename )
    {
	$options->{filename} = "%$filename%";
	$valid_criteria = 1;
    }
    
    if ( $taxon_name )
    {
	$options->{base_name} = $taxon_name;
	$valid_criteria = 1;
    }
    
    if ( $authorizer )
    {
	my $quoted = $dbh->quote($authorizer);
	$SQL_STRING = "SELECT person_no FROM person WHERE reversed_name like $quoted";
	my ($authorizer_no) = $dbh->selectrow_array($SQL_STRING);
	if ( $authorizer_no > 0 )
	{
	    $options->{authorizer_no} = $authorizer_no;
	    $valid_criteria = 1;
	}
    }
    
    if ( $reference_no )
    {
	$options->{reference_no} = $reference_no;
	$valid_criteria = 1;
    }
    
    # Check to see if we have a valid search.
    
    unless ( $valid_criteria )
    {
	return searchError($hbo, $s, "Please enter search criteria", $values);
    }
    
    # Do the search.  If nothing was found, redisplay the form.
    
    my (@file_list) = getNexusFileInfo($dbt, undef, $options);
    
    unless ( @file_list )
    {
	return searchError($hbo, $s, "No nexus files were found.  Please try a different search.", $values);
    }
    
    # Otherwise, we display the search results.
    
    my $search_output = qq%<table border="0" cellpadding="8">\n%;
    $search_output .= qq%<tr><th align="left">File name</th><th>Uploaded by</th><th></th></tr>\n%;

    foreach my $f (@file_list)
    {
	my ($a, $b, $c) = describeFileTable($s, $f);
	$search_output .= "<tr><td>$a</td><td>$b</td><td>$c</td></tr>\n";
    }
    
    $search_output .= "</table>\n";
    
    my $vars = { page_title => "Nexus file search results",
	         search_results => $search_output,
	         %$values };
    
    print $hbo->stdIncludes($PAGE_TOP);
    print $hbo->populateHTML('nexus_search_results', $vars);
    print $hbo->stdIncludes($PAGE_BOTTOM);

}


sub describeFileTable {

    my ($s, $nexusfile) = @_;
    
    my $nexusfile_no = $nexusfile->{nexusfile_no};
    my $auth_no = $nexusfile->{authorizer_no};
    my $authorizer = $nexusfile->{authorizer};
    my $filename = $nexusfile->{filename};
    
    my $first = qq%<a href="$READ_URL?a=viewNexusFile&nexusfile_no=$nexusfile_no">$filename</a>%;
    my $second = qq%$authorizer%;
    my $third = '';
    
    if ( $s->get('authorizer_no') == $nexusfile->{authorizer_no} )
    {
	$third = qq%[<a href="$READ_URL?a=editNexusFile&nexusfile_no=$nexusfile_no">edit</a>]%;
    }
    
    return $first, $second, $third;
}


sub searchError {

    my ($hbo, $s, $message, $oldvalues) = @_;
    
    $oldvalues = {} unless ref $oldvalues eq 'HASH';
    
    my ($vars) = { page_title => "Nexus file search form", error_message => $message,
		   current_ref => $s->get("reference_no"),
		   enterer_me => $s->get('enterer_reversed'), %$oldvalues };
    
    print $hbo->stdIncludes($PAGE_TOP);
    print $hbo->populateHTML('nexus_search', $vars);
    print $hbo->stdIncludes($PAGE_BOTTOM);
    
    return;
}

1;
