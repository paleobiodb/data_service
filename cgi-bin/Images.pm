package Images;

$DEBUG = 0;

###
# UPLOADING
###

sub startLoadImage{
	my $dbh = shift;
	my $dbt = shift;
	my $session = shift;
	my $exec_url = shift;

	# Check that user is logged in (session -> enterer and authorizer BOTH)
		# route to login if not
	if($session->get('enterer') eq "Guest" or $session->get('enterer') eq ""){
		$session->enqueue($dbh, "action=startLoadImage");
		main::displayLoginPage("Please log in first.");
		exit;
	}
	# Have user check that the taxonomic name exists in Authorities
		# route to 'add taxonomic info' if not (NOTE: for "Carnivora sp." the
		#	user may get stuck having to enter info on "Carnivora".
		# save away destination on the queue?
	print main::stdIncludes("std_page_top");
	print "<center><h2>Image upload</h2></center>";
	print "<p>This is the starting point for uploading images. Note that we ".
		  "must already have taxonomic information on the subject of the image".
		  " before you will be allowed to upload the image.  If we don't have ".
		  " this information, you will be asked to ".
		  "<a href=\"$exec_url?action=startTaxonomy\"> provide the ".
		  "taxonomic information</a> on the subject of the image.";
	print "<center><p><form name=\"image_form\" action=\"$exec_url\" ".
		  "method=\"POST\"><table><tr><td>";
	print "<b>Taxonomic name of specimen in image:&nbsp</b>";
	print "<input type=text size=30 name=\"taxon_name\">".
		  "<input type=hidden name=\"action\" value=\"processStartImage\">".
		  "</td></tr>";
	print "<tr><td><center><input type=submit></center></td></tr></table></center>";
	print main::stdIncludes("std_page_bottom");

}

sub processStartLoadImage{
	my $dbt = shift;
	my $q = shift;
	my $exec_url = shift;
	my $taxon_name = $q->param('taxon_name');

	if(!$taxon_name){
		main::startImage();
		exit;
	}
	
	# Strip off 'indet' or 'sp' or whatever. Use only the genus name in those
	# cases. 
	my $taxon_name =~ s/indet\.{0,1}|s[p]+\.{0,1}//;

	my $sql = "SELECT taxon_no FROM authorities WHERE taxon_name='$taxon_name'";
	my @results = @{$dbt->getData($sql)};
	my $taxon_no = $results[0]->{taxon_no};
	if(!$taxon_no){
		print main::stdIncludes("std_page_top");
		print "<center><h2>$taxon_name not found</h2>";
		print "<p><a href=\"$exec_url?action=startTaxonomy\"><b>Enter ".
			  "taxonomic information on $taxon_name</b></a>&nbsp;&#149;&nbsp;";
		print "<a href=\"$exec_url?action=startImage\"><b>Begin image upload ".
			  "from the start</b></a><p>&nbsp;";
		print main::stdIncludes("std_page_bottom");
		exit;
	}
	# If logged in and name exists in Authorities, go to image upload page.
	displayLoadImageForm($exec_url, $taxon_name);
}

sub displayLoadImageForm{
	my $exec_url = shift;
	my $taxon_name;

	# Spit out upload html page
	# list constraints: image size and image type

	print main::stdIncludes("std_page_top");
	print "<center><h2>Image upload form</h2>";
	print "<p>Files must be smaller than 1 MB and must either be of type ".
		  "jpg, gif or png.";
	print "<p><form name=\"load_image_form\" action=\"$exec_url\" ".
		  "method=\"POST\" enctype=\"multipart/form-data\">";
	print "<b>File to upload:</b>&nbsp;<input type=file name=\"image_file\" ".
		  "accept=\"image/*\">".
		  "<input type=hidden name=\"taxon_name\" value=\"$taxon_name\">".
		  "<input type=hidden name=\"action\" value=\"processLoadImage\">";
	print "<table><tr><td valign=top><p><b>Caption:</b></td><td>".
		  "<textarea cols=60 rows 4 name=\"caption\">".
		  "Optional image description here.</textarea></td></tr></table>";
	print "<br><input type=submit></form></center>";
	print main::stdIncludes("std_page_bottom");
}

sub processLoadImageForm{
	my $dbt = shift;
	my $q = shift;
	my $file_name = $q->param('image_file');
	my $caption = $q->param('caption');

	# test that we actually got a file
	if(!$file_name && $q->cgi_error){
		print $q->header(-status=>$->cgi_error);
		exit 0;
	}

	# check the file type: we only want images
	my $type = $q->uploadInfo($file_name)->{'Content-Type'};
	if($type !~ /image/){
		die "Image files of type jpg, png or gif only please!";
	}

	# find file's suffix:
	my $file_name =~ /.*?\.(\w)+$/;
	# write it to a temp location
	my $temp_file = "/tmp/uploaded_fossil_image.$1";
	open(OUTFILE, ">$temp_file") or die "Couldn't write out image ($!)";
	my $fh = $query->upload('uploaded_file');
	while ($_ = <$fh>) {
		print OUTFILE $_;
	}
	close OUTFILE;

	# test the file size
		# unlink if too big,
		# send back error message


	# find the image's md5 checksum (to check uniqueness)
	require Digest::MD5;

	my $image_digest = Digest::MD5->new();

	open(FILE, $temp_file) or die "can't open $temp_file ($!)";
	binmode(FILE);
	while(<FILE>){
		$image_digest->add($_);
	}
	close FILE;

	my $digest =  $image_digest->hexdigest();
	
	# test this checksum against other files' in the db image table
	# if this checksum already exists, 
		# unlink file
		# send back an error message

	# once we're sure this is a new, valid image
	#	insert a new record into the db
	#	copy the image from /tmp to its final resting place.
	#	(unlink from /tmp, or are we doing a mv?)
}

sub checkLoadedImage{
	# Check size of uploaded image
		# if too big, send back a rejection message explaining it was too big
		#	and delete the file
		# if wrong file type (has to be .jpg, .gif or .png) send back a 
		#	rejection message...
	# Do an md5 on the uploaded file and see if we already have it (compare
	# with md5s - in db - from other numbered files of the same taxonomic name).
	# Determine where to store it: enterer/taxonomic_name_digit.suffix
	# save original filename
	# write file to directory where it belongs:  write out a thumbnail too
	# 	or do we do that on the fly with imagemagick?
	# add record to the images table with all collected info
}


###
# VIEWING
###

sub viewImages{
	# Enter in a taxonomic name for images you wish to view.
	# Query the IMAGES table to see what we have.
	#	If we have nothing, return a message saying so
	# 	If we have images, build a page:
	#		nav bar with thumbnails 
	#		clicking nav bar brings up images: alone on a page with a "back"
	#			link?  Or on same page in main space with navbar still on
	#			the side/top? If the latter, what fills the space when the
	#			page first comes up - the first image in the series?
}


1;
