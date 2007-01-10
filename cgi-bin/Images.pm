package Images;

use Reference;
use TaxaCache;

$DEBUG = 0;

###
# UPLOADING
###

sub displayLoadImageForm{
    my $dbt = shift;
    my $q = shift;
	my $s = shift;

	# Spit out upload html page
	# list constraints: image size and image type
    my $exec_url = $q->url();
    my ($taxon_no,$taxon_name);
    my @results = TaxonInfo::getTaxa($dbt,{'taxon_no'=>$q->param('taxon_no')});
    if (@results) {
        $taxon_no = $results[0]->{'taxon_no'}; 
        $taxon_name = $results[0]->{'taxon_name'}; 
    } else {
        print "<div align=center><h2>Could not find taxon in database</h2></div>";
        return;
    }

	print "<center><h2>Image upload form: $taxon_name</h2>";
	print "<p>Files must be smaller than 1 MB and must either be of type jpg, gif or png.";
	print "<p><form name=\"load_image_form\" action=\"$exec_url\" method=\"POST\" enctype=\"multipart/form-data\">";
	print "<b>File to upload:</b>&nbsp;<input type=file name=\"image_file\" accept=\"image/*\">".
		  "<input type=hidden name=\"taxon_no\" value=\"$taxon_no\">".
		  "<input type=hidden name=\"taxon_name\" value=\"$taxon_name\">".
		  "<input type=hidden name=\"action\" value=\"processLoadImage\">";
	print "<table><tr><td valign=top><p><b>Caption:</b></td><td>".
		  "<textarea cols=60 rows 4 name=\"caption\">".
		  "Optional image description here.</textarea></td></tr></table>";
	my $reference_no = $s->get("reference_no");
	if ( $reference_no )	{
		print  "<input type=checkbox name=reference_no value=$reference_no> <b>Check if image is from the current reference</b><p>\n\n";
	}
	print "<br><input type=\"submit\"></form></center>";
}

sub processLoadImage{
	my $dbt = shift;
	my $q = shift;
	my $s = shift;
    my $exec_url = $q->url();
	my $MEGABYTE = 1048576;
	my $file_name = $q->param('image_file');
	my $taxon_name = $q->param('taxon_name');
	$taxon_name =~ s/\s+/_/g;
	my $taxon_no = $q->param('taxon_no');
	my $reference_no = $q->param('reference_no') || 'NULL';
	if($DEBUG){
		print "FILE NAME: $file_name<br>";
	}

	my $caption = $q->param('caption');
	if($caption eq "Optional image description here."){
		$caption = "";
	}

	# test that we actually got a file
	if(!$file_name && $q->cgi_error){
		print $q->header(-status=>$q->cgi_error);
		exit;
	}

	# check the file type: we only want images
	my $type = $q->uploadInfo($file_name)->{'Content-Type'};
	if($type !~ /image/){
		print "<center><p>Image files of type jpg, png or gif only please!<br>";
		print "<p><a href=\"$exec_url?action=startImage\">".
			  "<b>Enter another image</b></a></center>";
		return;
	}

	# find file's suffix:
	$file_name =~ /.*?\.(\w+)$/;
	my $suffix = $1;
	if($file_name !~ /(jpg|gif|png)/i){
		print "<center><p>Please upload only images of type jpg, gif or png.<br>";
		print "<p><a href=\"$exec_url?action=startImage\">".
			  "<b>Enter another image</b></a></center>";
		return;
	}

	# read the image into memory
	my $buffer;
	# Read 1 MB into $buffer
	my $bytes_read = read($file_name, $buffer, $MEGABYTE); 

	# test the file size, send back error message if too big
	if($bytes_read > $MEGABYTE-1){
		print "<center><p>Image is too large.  Please only upload images ".
			  "less than one megabyte in size<br>";
		print "<p><a href=\"$exec_url?action=startImage\">".
			  "<b>Enter another image</b></a></center>";
		return;
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
		print "<center><p>This image <a href=\"".$results[0]->{path_to_image}.
			  "\">already exists</a> in the database<br>";
		print "<p><a href=\"$exec_url?action=startImage\">".
			  "<b>Enter another image</b></a></center>";
		return; 
	}

	# Need authorizer/enterer numbers from names
	my $enterer = $s->get('enterer_no');
	my $authorizer = $s->get('authorizer_no');
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
	$enterer_name =~ s/[^a-zA-Z]//g;
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
    my $width = $image->Get('width') || 0;
    my $height = $image->Get('height') || 0;
	warn "$x" if "$x";
	$x = $image->Scale('100x100');
	warn "$x" if "$x";
	my $new_thumb = "${taxon_name}_${number}_thumb.$suffix";
	$x = $image->Write("$docroot$subdirs/$new_thumb");
	warn "$x" if "$x";

	my @values = ($authorizer, $enterer, $reference_no, $taxon_no, "'$now'", "'$subdirs/$new_file'", $width, $height, "'$file_name'", "'$caption'", "'$digest'");

	#	insert a new record into the db
	$sql = "INSERT INTO images (authorizer_no, enterer_no, reference_no, taxon_no, ".
		   "created, path_to_image, width, height, original_filename, caption, file_md5_hexdigest) ".
		   "VALUES (".join(',',@values).")";
    main::dbg("Image sql: $sql");
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
		my $clean_name = $taxon_name;
		$clean_name =~ s/_/ /g;
		print "<center><p>The image of $clean_name was uploaded successfully</p>";
		print "<p><a href=\"$exec_url?action=startImage\">".
			  "<b>Enter another image</b></a></p></center>";
	}
}


###
# VIEWING
###
sub getImageList {
	my $dbt = shift;
	my $taxa_list = shift;
    
	my @results = ();

    foreach $taxon_no (@$taxa_list) {
        if ($taxon_no =~ /^\d+$/) {
            push @taxon_no_list,$taxon_no;
        }
    }
    if (scalar @taxon_no_list) {
		my $sql = "SELECT a.taxon_no, a.taxon_name, i.*".
			  " FROM authorities a, images i".
			  " WHERE a.taxon_no = i.taxon_no" . 
			  " AND i.taxon_no IN (".join(",",@taxon_no_list).")";
		@results = @{$dbt->getData($sql)};
    }
	return @results;
}

sub displayImage {
    my ($dbt,$image_no,$height,$width) = @_;

    my $sql = "SELECT a.taxon_no,a.taxon_name,i.* FROM images i, authorities a where i.taxon_no=a.taxon_no AND i.image_no=$image_no";
    my $row = ${$dbt->getData($sql)}[0];
    if (!$row) {
        print "<div class=errorMessage>Error, no image to display</div>";
    } else {
        my $ss = TaxaCache::getSeniorSynonym($dbt,$row->{'taxon_no'});
        
        print "<div align=\"center\">";
        print "<img src=\"".$row->{'path_to_image'}."\" height=\"$height\" width=\"$width\" border=1><br>\n";
        print "<i>".$row->{'caption'}."</i><br>\n";
        print "<div class=\"small\">";
        print "<br><table border=0 cellpadding=2 cellspacing=0>";
        print "<tr><td><b>Original name of image:</b></td><td>".$row->{'original_filename'}."</td></tr>\n";
        if ( $ss && $row->{'taxon_no'} != $ss->{'taxon_no'} ) {
            print "<tr><td><b>Original identification:</b></td><td><a target=\"_blank\" href=\"bridge.pl?action=checkTaxonInfo&taxon_no=$row->{taxon_no}\">".$row->{'taxon_name'}."</a></td></tr>\n";
            print "<tr><td><b>Current identification:</b></td><td><a target=\"_blank\" href=\"bridge.pl?action=checkTaxonInfo&taxon_no=$ss->{taxon_no}\">".$ss->{'taxon_name'}."</a></td></tr>\n";
        } else {
            print "<tr><td><b>Current identification:</b></td><td><a target=\"_blank\" href=\"bridge.pl?action=checkTaxonInfo&taxon_no=$row->{taxon_no}\">".$row->{'taxon_name'}."</a></td></tr>\n";
        }
        if ( $row->{reference_no} > 0 ) {
            my $sql = "SELECT reference_no,author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=$row->{reference_no}";
            my $ref = ${$dbt->getData($sql)}[0];
            $ref_string = Reference::formatShortRef($ref,'link_id'=>1);  
            $ref_string =~ s/<a /<a target="_blank" /;
            print "<tr><td><b>Reference:</b></td><td> $ref_string</td></tr>\n";
        }
        print "</table>";
        print "</div>";
    }   
}

1;
