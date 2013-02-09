#
# NexusfileWeb.pm
# 
# Created by Michael McClennen, 2013-01-31
# 
# The purpose of this module is to provide for uploading, downloading and
# editing for taxonomic nexus files.

package NexusfileWeb;
use strict;

use Nexusfile;
use NexusfileWrite;
use Constants qw($READ_URL $WRITE_URL $HTML_DIR $PAGE_TOP $PAGE_BOTTOM);

use Encode;
use CGI;
use Data::Dumper;
use Class::Date qw(now date);

my $UPLOAD_LIMIT = 102400;

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
    
    # Check if the data is in utf8.
    
    if ( $q->charset() =~ /utf-?8/i )
    {
	$upload_file = decode_utf8($upload_file);
	$notes = decode_utf8($notes);
    }
    
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
    
    # Read the data into memory, to a maximum of 100KB.
    
    my ($nexus_data, $buffer, $bytes_read, $total_bytes);
    
    while ( $bytes_read = read($upload_fh, $buffer, $UPLOAD_LIMIT) )
    {
	$nexus_data .= $buffer;
	$total_bytes += $bytes_read;
	
	if ( $total_bytes >= $UPLOAD_LIMIT )
	{
	    return uploadError($hbo, $q, "The file was too big.<br/>Please choose a file smaller than 100 KB.",
			       $values);
	}
    }
    
    # Make sure this is an actual nexus file.
    
    unless ( $nexus_data =~ /^\#nexus/i )
    {
	return uploadError($hbo, $q, 
			   "The file you chose was not a valid nexus file.<br/>Please choose a different file.",
			   $values);
    }
    
    # If it was a MacClade file, we need to translate it out of the Macintosh
    # character set.
    
    if ( $nexus_data =~ /\[MacClade \d/ )
    {
	$nexus_data = decode("MacRoman", $nexus_data);
    }
    
    # At this point, we have a valid upload.  Now we need to check whether
    # there is an existing file by that name.  If so, the user must choose
    # whether to replace it (unless $replace is true, which means that they
    # already check the 'replace file' box).
    
    $values->{nexus_file} = $upload_file;
    
    my ($nexusfile) = Nexusfile::getFileInfo($dbt, undef, { filename => $upload_file, 
							    authorizer_no => $s->{authorizer_no} });
    
    if ( $nexusfile and not $replace )
    {
	$values->{replace_box} = 1;
	return uploadError($hbo, $q, "You have already uploaded a file with that name.<br/>Check the box below if you want to replace the existing one.", $values);
    }
    
    # Store the file info and file contents, and link up the taxa specified in
    # the file to taxa in our database.
    
    my $nexusfile_no = Nexusfile::addFile($dbt, $upload_file, $s->{authorizer_no}, $s->{enterer_no}, 
					  { notes => $notes });
    
    my ($data_result, $taxa_result);
    
    if ( $nexusfile_no )
    {
	$data_result = Nexusfile::setFileData($dbt, $nexusfile_no, $nexus_data);
	$taxa_result = Nexusfile::generateTaxa($dbt, $nexusfile_no, $nexus_data);
    }
    
    unless ( $nexusfile_no and $data_result and $taxa_result )
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


# viewFile ( dbt, hbo, q, s )
# 
# Edit the file given by the URL arguments.

sub viewFile {

    my ($dbt, $hbo, $q, $s) = @_;
    
    # First figure out which file we're supposed to be viewing, and grab all
    # of the info currently known about the file.  If we can't find a file,
    # then send the user an error page.
    
    my $nf = findFile($dbt, $q, $s);
    my ($vars, $nexusfile, $nexusfile_no, $auth_no, $filename);
    
    unless ( ref $nf )
    {
	$vars = { page_title => "View nexus file", 
		  error_message => "No matching nexus file was found" };
	$nexusfile = {};
    }
    
    else
    {
	$nexusfile_no = $nf->{nexusfile_no};
	($nexusfile) = Nexusfile::getFileInfo($dbt, $nexusfile_no, { fields => 'all' });
	
	$auth_no = $nexusfile->{authorizer_no};
	$filename = $nexusfile->{filename};
	
	$vars = { page_title => "View nexus file",
		  filename => $filename,
		  date_created => $nexusfile->{created},
		  date_modified => $nexusfile->{modified},
		  nexusfile_no => $nexusfile_no,
		  download_link => generateURL($nexusfile) };
	
	if ( $q->param('noperm') )
	{
	    $vars->{error_message} = "You do not have permission to edit this file.";
	}
    }
    
    my $content = Nexusfile::getFileData($dbt, $nexusfile_no);
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
    
    unless ( $s->get('superuser') or
	     Nexusfile::checkWritePermission($dbt, $nexusfile_no, $s->get('authorizer_no')) )
    {
	$q->param('noperm', 1);
	viewFile($dbt, $hbo, $q, $s);
	exit;
    }
    
    # If we are returning from selecting a reference to add, then add it.
    
    if ( $q->param('add_ref') )
    {
	my $reference_no = $q->param('reference_no') || $s->get('reference_no');
	
	Nexusfile::addReference($dbt, $nexusfile_no, $reference_no);
    }
    
    # Grab all of the info currently known about the file.
    
    my ($nexusfile) = Nexusfile::getFileInfo($dbt, $nexusfile_no, { fields => 'all' });
    
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
		 download_link => generateURL($nexusfile),
	         nexusfile_no => $nexusfile_no };
    
    my $content = Nexusfile::getFileData($dbt, $nexusfile_no);
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
	    my ($name, $rest, @rest, $genus, $species, $line);
	    
	    if ( $t->{taxon_no} > 0 )
	    {
		if ( $t->{inexact} )
		{
		    ($name, @rest) = split /\s+/, $t->{taxon_name};
		    $rest = join(' ', @rest);
		}
		
		else
		{
		    ($genus, $species, @rest) = split /\s+/, $t->{taxon_name};
		    $name = "$genus $species";
		    $rest = join(' ', @rest);
		}
		
		$line = qq%<a href="$READ_URL?a=basicTaxonInfo&taxon_no=$t->{taxon_no}">$name</a> $rest%;
		
		if ( $t->{inexact} )
		{
		    $line .= " **";
		    $double = 1;
		}
	    }
	    
	    else
	    {
		$line = $t->{taxon_name} . " *";
		$asterisk = 1;
	    }
	    
	    $vars->{taxa} .= "<li>$line</li>\n";
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
    
    # Check that we have permission to edit the file.  If not, redirect to the
    # "view file" page with a "you do not have permission" message.
    
    unless ( $s->get('superuser') or
	     Nexusfile::checkWritePermission($dbt, $nexusfile_no, $s->get('authorizer_no')) )
    {
	print $q->redirect( -uri => "$READ_URL?a=viewNexusFile&nexusfile_no=$nexusfile_no&noperm=1");
	exit;
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
	Nexusfile::deleteReference($dbt, $nexusfile_no, $reference_no);
	print $q->redirect( -uri => "$READ_URL?a=editNexusFile&nexusfile_no=$nexusfile_no");
	exit;
    }
    
    # Are we moving an existing reference?  If so, do it and redisplay the
    # edit page.
    
    elsif ( my $reference_no = $q->param('moveReference') )
    {
	Nexusfile::moveReference($dbt, $nexusfile_no, $reference_no);
	print $q->redirect( -uri => "$READ_URL?a=editNexusFile&nexusfile_no=$nexusfile_no");
	exit;
    }
    
    # Are we rescanning the file contents for taxonomic names?
    
    elsif ( $q->param('scanTaxa') )
    {
	my $data = Nexusfile::getFileData($dbt, $nexusfile_no);
	Nexusfile::generateTaxa($dbt, $nexusfile_no, $data) if $data;
	print $q->redirect( -uri => "$READ_URL?a=editNexusFile&nexusfile_no=$nexusfile_no");
	exit;
    }
    
    # Are we deleting the entire nexus file?  If so, do it and redirect to the
    # main menu.
    
    elsif ( $q->param('deleteNexusFile') )
    {
	Nexusfile::deleteFile($dbt, $nexusfile_no);
	print $q->redirect( -uri => "$READ_URL");
	exit;
    }
    
    # Are we saving a new version of the notes?  If so, do that and redirect
    # to the main menu.
    
    elsif ( $q->param('saveNexusInfo') )
    {
	my $notes = $q->param('notes');
	
	if ( $q->charset() =~ /utf-?8/i )
	{
	    $notes = decode_utf8($notes);
	}
	
	Nexusfile::setFileInfo($dbt, $nexusfile_no, { notes => $notes });
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
    
    if ( $q->charset() =~ /utf-?8/i )
    {
	$filename = decode_utf8($filename);
    }
    
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
    
    my (@file_list) = Nexusfile::getFileInfo($dbt, undef, $options);
    
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
    
    if ( $s->get('authorizer_no') == $nexusfile->{authorizer_no} or $s->get('superuser') )
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


# generateURL ( nexusfile )
# 
# Given a Nexusfile object, generate the URL by which it can be fetched.

sub generateURL {

    my ($nf) = @_;
    
    my $auth_no = $nf->{authorizer_no};
    my $filename = $nf->{filename};
    my $nexusfile_no = $nf->{nexusfile_no};
    
    # Restore these if we choose to use filenames in URL paths again
    #$filename =~ s/ /%20/g;
    #$filename =~ s/&/%26/g;
    
    return "$READ_URL?a=getNexusFile&nexusfile_no=$nexusfile_no";
}


# sendFile ( dbt, q, s )
# 
# Send the nexus file indicated by the URL arguments, with the proper header.

sub sendFile {

    my ($dbt, $q, $s) = @_;
    
    # First see if we can find a nexus file based on our arguments.  If not,
    # then send the user an error page.
    
    my $nf = findFile($dbt, $q, $s);
    
    unless ( ref $nf )
    {
	findError($q, $nf);
	exit;
    }
    
    # Otherewise, send out the specified file.
    
    print $q->header(-type => "text/plain", -charset => 'utf-8' );
    print encode_utf8(Nexusfile::getFileData($dbt, $nf->{nexusfile_no}));
    exit;
}


# findFile ( dbt, q, s )
# 
# Attempt to find a nexus file in the database, according to the URL arguments
# and session information.  If we can find one, return a Nexusfile object.
# Otherwise, return an error message.  The caller can test whether the return
# value is a reference.

sub findFile {
    
    my ($dbt, $q, $s) = @_;
    
    my $nf;
    
    # If we have been given a nexusfile_no value, just use that.  Either we
    # find a nexus file with the given nexusfile_no, or not.
    
    if ( (my $nexusfile_no = $q->param('nexusfile_no')) > 0 )
    {
	($nf) = Nexusfile::getFileInfo($dbt, $nexusfile_no);
    }
    
    # If no nexusfile_no was found, then look for an authorizer_no value and
    # filename.  These might occur in the path info, or as an argument called
    # 'path' or as individual arguments.
    
    else
    {
	my ($authorizer_no, $filename);
	
	if ( $q->path_info() =~ m{^/nexus/([0-9]+)/(.*)} )
	{
	    $authorizer_no = $1;
	    $filename = $2;
	}
	
	elsif ( $q->param('path') =~ m{^([0-9]+)/(.*)} )
	{
	    $authorizer_no = $1;
	    $filename = $2;
	}
	
	elsif ( $q->param('authorizer_no') =~ /^[0-9]/ and $q->param('filename') )
	{
	    $authorizer_no = $q->param('authorizer_no');
	    $authorizer_no =~ tr/0-9//dc;
	    $filename = $q->param('filename');
	}
	
	else
	{
	    return '400 Bad Request';
	}
	
	# If we get here, then we have found an authorizer_no and filename.
	# So try to retrieve a file based on that information.
	
	($nf) = Nexusfile::getFileInfo($dbt, undef, { authorizer_no => $authorizer_no,
							 filename => decode_utf8($filename) });
	
	# If we fail, try again without utf8-decoding the filename.
	
	unless ( $nf )
	{
	    ($nf) = Nexusfile::getFileInfo($dbt, undef, { authorizer_no => $authorizer_no,
							filename => $filename });
	}
    }
    
    # If we found something, return it.  Otherwise, return an error message.
    
    return (ref $nf ? $nf : '404 Not Found');
}


# findError ( q, message )
#
# Send out an error page.

sub findError {

    my ($q, $message) = @_;
    
    my ($body);
    
    if ( $message =~ /^400/ )
    {
	$body = "This URL does not contain the proper parameters for retrieving a nexus file";
    }
    
    elsif ( $message =~ /^404/ )
    {
	$body = "The requested nexus file was not found";
    }
    
    elsif ( $message =~ /^[0-9]+/ )
    {
	$body = '';
    }
    
    else
    {
	$message = '400 Bad Request';
	$body = '';
    }
    
    print $q->header(-status => $message);
    print "<html><head><title>$message</title><body>\n";
    print "<h1>$message</h1>\n";
    print "<h2>$body</h2>\n";
    print "</body></html>\n";
    exit;
}


1;
