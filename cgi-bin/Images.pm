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
	print "<DIV class=\"title\">Image upload</DIV>";
	print "<p>You can't upload an image unless we have taxonomic information ".
		  "on the subject of the image. If this search doesn't find it, you ".
		  "will be asked to ".
		  "<a href=\"$exec_url?action=startTaxonomy\"> provide the ".
		  "taxonomic information</a> yourself.";
	print "<center><p><form name=\"image_form\" action=\"$exec_url\" ".
		  "method=\"POST\"><table><tr><td>";
	print "<b>Taxonomic name of specimen in image:&nbsp</b>";
	print "<input type=text size=30 name=\"taxon_name\">".
		  "<input type=hidden name=\"action\" value=\"processStartImage\">".
		  "</td></tr>";
	print "<tr><td><BR><center><input type=submit></center></td></tr></table></center>";
	print main::stdIncludes("std_page_bottom");

}

sub processStartLoadImage{
	my $dbt = shift;
	my $q = shift;
	my $s = shift;
	my $exec_url = shift;
	my $taxon_name = $q->param('taxon_name');

	if(!$taxon_name){
		main::startImage();
		exit;
	}
	
	# Strip off 'indet' or 'sp' or whatever. Use only the genus name in those
	# cases. 
	$taxon_name =~ s/\s+(indet|sp|spp)\.{0,1}$//;

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
	displayLoadImageForm($exec_url, $taxon_name, $taxon_no, $s);
}

sub displayLoadImageForm{
	my $exec_url = shift;
	my $taxon_name = shift;
	my $taxon_no = shift;
	my $s = shift;

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
		  "<input type=hidden name=\"taxon_no\" value=\"$taxon_no\">".
		  "<input type=hidden name=\"action\" value=\"processLoadImage\">";
	print "<table><tr><td valign=top><p><b>Caption:</b></td><td>".
		  "<textarea cols=60 rows 4 name=\"caption\">".
		  "Optional image description here.</textarea></td></tr></table>";
	my $reference_no = $s->get("reference_no");
	if ( $reference_no )	{
		print  "<input type=checkbox name=reference_no value=$reference_no> <b>Check if image is from the current reference</b><p>\n\n";
	}
	print "<br><input type=submit></form></center>";
	print main::stdIncludes("std_page_bottom");
}

sub processLoadImageForm{
	my $dbt = shift;
	my $q = shift;
	my $s = shift;
	my $exec_url = shift;
	my $MEGABYTE = 1048576;
	my $file_name = $q->param('image_file');
	my $taxon_name = $q->param('taxon_name');
	$taxon_name =~ s/\s+/_/g;
	my $taxon_no = $q->param('taxon_no');
	my $reference_no = $q->param('reference_no');

	if($DEBUG){
		print "FILE NAME: $file_name<br>";
	}

	my $caption = $q->param('caption');
	if($caption eq "Optional image description here."){
		$caption = "";
	}

	# test that we actually got a file
	if(!$file_name && $q->cgi_error){
		print $q->header(-status=>$->cgi_error);
		exit;
	}

	# check the file type: we only want images
	my $type = $q->uploadInfo($file_name)->{'Content-Type'};
	if($type !~ /image/){
		print main::stdIncludes("std_page_top");
		print "<center><p>Image files of type jpg, png or gif only please!<br>";
		print "<p><a href=\"$exec_url?action=startImage\">".
			  "<b>Enter another image</b></a></center>";
		print main::stdIncludes("std_page_bottom");
		exit;
	}

	# find file's suffix:
	$file_name =~ /.*?\.(\w+)$/;
	my $suffix = $1;
	if($file_name !~ /(jpg|gif|png)/i){
		print main::stdIncludes("std_page_top");
		print "<center><p>Please upload only images of type jpg, gif or png.<br>";
		print "<p><a href=\"$exec_url?action=startImage\">".
			  "<b>Enter another image</b></a></center>";
		print main::stdIncludes("std_page_bottom");
		exit;
	}

	# read the image into memory
	my $buffer;
	# Read 1 MB into $buffer
	my $bytes_read = read($file_name, $buffer, $MEGABYTE); 

	# test the file size, send back error message if too big
	if($bytes_read > $MEGABYTE-1){
		print main::stdIncludes("std_page_top");
		print "<center><p>Image is too large.  Please only upload images ".
			  "less than one megabyte in size<br>";
		print "<p><a href=\"$exec_url?action=startImage\">".
			  "<b>Enter another image</b></a></center>";
		print main::stdIncludes("std_page_bottom");
		exit;
	}

	# find the image's md5 checksum (to check uniqueness)
	require Digest::MD5;

	my $image_digest = Digest::MD5->new();

	$image_digest->add($buffer);

	my $digest =  $image_digest->hexdigest();

	if($DEBUG){
		print "DIGEST: $digest<br>";
	}
	
	# test this checksum against other files' in the db image table
	# if this checksum already exists send back an error message
	my $sql = "SELECT file_md5_hexdigest,path_to_image FROM images ".
			  "WHERE file_md5_hexdigest='$digest'";
	my @results = @{$dbt->getData($sql)};
	if(@results){
		print main::stdIncludes("std_page_top");
		print "<center><p>This image <a href=\"".$results[0]->{path_to_image}.
			  "\">already exists</a> in the database<br>";
		print "<p><a href=\"$exec_url?action=startImage\">".
			  "<b>Enter another image</b></a></center>";
		print main::stdIncludes("std_page_bottom");
		exit; 
	}

	# Need authorizer/enterer numbers from names
	$sql = "SELECT person_no FROM person WHERE name='".$s->get('enterer')."'";
	my $enterer = @{$dbt->getData($sql)}[0]->{person_no};
	$sql = "SELECT person_no FROM person WHERE name='".$s->get('authorizer')."'";
	my $authorizer = @{$dbt->getData($sql)}[0]->{person_no};
	if(!$enterer or !$authorizer){
		main::displayLoginPage("Please log in first.");
		exit;
	}

	# create a timestamp for record creation
	my @timing = localtime(time);
	my $year = 1900+$timing[5];
	my $month = $timing[4] < 10?"0".$timing[4]:$timing[4];
	my $day = $timing[3] < 10?"0".$timing[3]:$timing[3];
	my $hour = $timing[2] < 10?"0".$timing[2]:$timing[2];
	my $minute = $timing[1] < 10?"0".$timing[1]:$timing[1];
	my $sec = $timing[0] < 10?"0".$timing[0]:$timing[0];
	my $now = "$year-$month-$day $hour:$minute:$sec";

	# Once we're sure this is a new, valid image
	#	write the image to the filesystem
	my $enterer_name = $s->get('enterer');
	$enterer_name =~ s/\s+//g;
	$enterer_name =~ s/\.//g;
	my $docroot = $ENV{DOCUMENT_ROOT};
	my $subdirs = "/images/$enterer_name";
	my $base = "${docroot}$subdirs";
	my $number = 1; 
	# open enterer's directory.  if it doesn't exist, create it.

	if(! -e $base){
		my $success = mkdir($base);	
		if(!$success){
			die "couldn't create enterer's directory. ".
				"Please notify the webmaster.<br>";
		}
	}
	else{ # read all the files in the directory
		opendir(DIR,$base) or die "can't open $base ($!)";
		my @files = grep { /$taxon_name/ } readdir(DIR);
		closedir(DIR);

		# find any files with the same taxon_name, 
		# and get their maximum number suffix
		if(@files){
			foreach my $file (@files){
				$file =~ /(\d+)\./;
				my $temp_num = $1;
				if($temp_num > $number){
					$number = $temp_num;
				}
			}
			# increment the count for the new file
			$number++;
		}
	}

	# Write the file out to the filesystem.
	my $new_file = "${taxon_name}_$number.$suffix";
	open(NEWFILE,">$docroot$subdirs/$new_file") or die "couldn't create $docroot$subdirs/$new_file ($!)\n";
	print NEWFILE $buffer;
	close NEWFILE;

	# Write out a thumbnail
	require Image::Magick;
	my ($image, $x);
	$image = Image::Magick->new;
	$x = $image->Read(filename=>"$docroot$subdirs/$new_file");
	warn "$x" if "$x";
	$x = $image->Scale('50x50');
	warn "$x" if "$x";
	my $new_thumb = "${taxon_name}_${number}_thumb.$suffix";
	$x = $image->Write("$docroot$subdirs/$new_thumb");
	warn "$x" if "$x";

	my @values = ($authorizer, $enterer, $reference_no, $taxon_no, "'$now'", "'$subdirs/$new_file'", "'$file_name'", "'$caption'", "'$digest'");

	#	insert a new record into the db
	$sql = "INSERT INTO images (authorizer_no, enterer_no, reference_no, taxon_no, ".
		   "created, path_to_image, original_filename, caption, file_md5_hexdigest) ".
		   "VALUES (".join(',',@values).")";
	if(!$dbt->getData($sql)){
		print $dbt->getErr() ;
		# If we had an error inserting, remove the image from the filesystem too
		my $removed = unlink($new_file);
		if($removed != 1){
			die "<p>Image db insert and file removal both failed. Please ".
				"notify the webmaster.<br>";
		}
	}
	else{
		print main::stdIncludes("std_page_top");
		my $clean_name = $taxon_name;
		$clean_name =~ s/_/ /g;
		print "<center><p>The image of $clean_name was uploaded successfully</p>";
		print "<p><a href=\"$exec_url?action=startImage\">".
			  "<b>Enter another image</b></a></p></center>";
		print main::stdIncludes("std_page_bottom");
	}
}


###
# VIEWING
###
sub processViewImages{
	my $dbt = shift;
	my $q = shift;
	my $s = shift;
	my $in_list = shift;
	my $taxon_name = $q->param('genus_name');

	my @results;
	my @taxa;
	if ( $in_list )	{
		@taxa = split /,/,$in_list;
	} else	{
		push @taxa,"'".$taxon_name."'";
	}

	for my $t ( @taxa )	{
		my $sql = "SELECT authorities.taxon_no, taxon_name, image_no, images.reference_no, path_to_image, caption, ".
			  " original_filename ".
			  "FROM authorities, images ".
			  "WHERE authorities.taxon_no = images.taxon_no AND ".
			  "taxon_name=$t";
		push @results , @{$dbt->getData($sql)};
	}
	
	return @results;
}

1;
