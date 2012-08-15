package Images;

use PBDBUtil;
use Reference;
use TaxaCache;
use TaxonInfo;
use Debug qw(dbg);
use Constants qw($READ_URL $WRITE_URL $HTML_DIR $PAGE_TOP $PAGE_BOTTOM);
use strict;

###
# UPLOADING
###

sub displayLoadImageForm{
    my ($dbt, $taxonomy, $q, $s) = @_;
    
	# Spit out upload html page
	# list constraints: image size and image type
    my ($taxon_no,$taxon_name);
    my @results = $taxonomy->getTaxon($q->param('taxon_no'));
    if (@results) {
        $taxon_no = $results[0]->{'taxon_no'}; 
        $taxon_name = $results[0]->{'taxon_name'}; 
    } else {
        print "<div align=center><p class=\"pageTitle\">Could not find taxon in database</p></div>";
        return;
    }

	print "<div style=\"text-align: center;\"><p class=\"pageTitle\">Image upload form: $taxon_name</p>";
	print "<div class=\"displayPanel\" style=\"width: 40em; margin-left: 6em; text-align: left; padding-top: 1em; padding-left: 2em;\"><p><form name=\"load_image_form\" action=\"$WRITE_URL\" method=\"POST\" enctype=\"multipart/form-data\">";
	print "<p class=\"medium\">File to upload:&nbsp;<input type=file name=\"image_file\" accept=\"image/*\"></p>".
		  "<input type=hidden name=\"taxon_no\" value=\"$taxon_no\">".
		  "<input type=hidden name=\"taxon_name\" value=\"$taxon_name\">".
		  "<input type=hidden name=\"action\" value=\"processLoadImage\">";
	print "<table><tr><td valign=top><p class=\"medium\">Caption:</p></td><td>".
		  "<textarea cols=60 rows 4 name=\"caption\">".
		  "Optional image description here</textarea></td></tr></table>";
	my $reference_no = $s->get("reference_no");
	if ( $reference_no )	{
		print  "<p class=\"medium\"><input type=checkbox name=reference_no value=$reference_no> Check if image is from the current reference</p>\n\n";
	}
	print "<p class=\"verysmall\">Files must be smaller than 1 MB and must either be of type jpg, gif or png.</p>";
	print "<br><span style=\"margin-left: 30em;\"><input type=\"submit\"></span></form></div></div>";
}

sub processLoadImage{
	my $dbt = shift;
	my $q = shift;
	my $s = shift;
	my $MEGABYTE = 1048576;
	my $file_name = $q->param('image_file');
	my $taxon_name = $q->param('taxon_name');
	$taxon_name =~ s/\s+/_/g;
	my $taxon_no = $q->param('taxon_no');
	my $reference_no = $q->param('reference_no') || 'NULL';
	dbg("FILE NAME: $file_name");

	my $caption = $q->param('caption');
	if($caption =~ /Optional image description/i){
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
		print "<p><a href=\"$WRITE_URL?action=startImage\">".
			  "<b>Enter another image</b></a></center>";
		return;
	}

	# find file's suffix:
	$file_name =~ /.*?\.(\w+)$/;
	my $suffix = $1;
	if($file_name !~ /(jpg|gif|png)/i){
		print "<center><p>Please upload only images of type jpg, gif or png.<br>";
		print "<p><a href=\"$WRITE_URL?action=startImage\">".
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
		print "<p><a href=\"$WRITE_URL?action=startImage\">".
			  "<b>Enter another image</b></a></center>";
		return;
	}

	# find the image's md5 checksum (to check uniqueness)
	require Digest::MD5;

	my $image_digest = Digest::MD5->new();

	$image_digest->add($buffer);

	my $digest =  $image_digest->hexdigest();

	dbg("DIGEST: $digest");
	
	# test this checksum against other files' in the db image table
	# if this checksum already exists send back an error message
	my $sql = "SELECT file_md5_hexdigest,path_to_image FROM images ".
			  "WHERE file_md5_hexdigest='$digest'";
	my @results = @{$dbt->getData($sql)};
	if(@results){
		print "<center><p>This image <a href=\"".$results[0]->{path_to_image}.
			  "\">already exists</a> in the database<br>";
		print "<p><a href=\"$WRITE_URL?action=startImage\">".
			  "<b>Enter another image</b></a></center>";
		return; 
	}

	# create a timestamp for record creation
	my @timing = localtime(time);
	my $year = 1900+$timing[5];
	# localtime returns 0-11 for months, but MySQL numbers them 1-12,
	#  so the value needs to be incremented JA 5.2.07
	$timing[4]++;
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
    my $subdirs = "/public/upload_images/$enterer_name";
	my $base = $HTML_DIR.$subdirs;
	my $number = 1; 
	# open enterer's directory.  if it doesn't exist, create it.

	if(! -e $base){
		my $success = mkdir($base);	
		if(!$success){
			die "couldn't create enterer's directory. ".
				"Please notify the webmaster.<br>";
		}
	} else { # read all the files in the directory
		opendir(DIR,$base) or die "can't open $base ($!)";
		my @files = grep { /\Q$taxon_name\E/ } readdir(DIR);
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
	open(NEWFILE,">$base/$new_file") or die "couldn't create $base/$new_file ($!)\n";
	print NEWFILE $buffer;
	close NEWFILE;

	# Write out a thumbnail
	require Image::Magick;
	my ($image, $x);
	$image = Image::Magick->new;
	$x = $image->Read(filename=>"$base/$new_file");
	my $width = $image->Get('width') || 0;
	my $height = $image->Get('height') || 0;
	warn "$x" if "$x";
	$x = $image->Scale('200x200');
	warn "$x" if "$x";
	my $new_thumb = "${taxon_name}_${number}_thumb.$suffix";
	$x = $image->Write("$base/$new_thumb");
	warn "$x" if "$x";

	my %vars = (
        authorizer_no   => $s->get('authorizer_no'),
        enterer_no      => $s->get('enterer_no'), 
        reference_no    => $reference_no, 
        taxon_no        => $taxon_no, 
        host            => $ENV{PRIMARY_HOST},
        path_to_image   => "$subdirs/$new_file", 
        width           => $width, 
        height          => $height, 
        caption         => $caption, 
        original_filename=> $file_name, 
        file_md5_hexdigest=>$digest
    );

    my ($result,$id) = $dbt->insertRecord($s,'images',\%vars);
    if (!$result) {
		# If we had an error inserting, remove the image from the filesystem too
		my $removed = unlink($new_file);
		if($removed != 1){
			die("Image db insert and file removal both failed. Please notify the webmaster");
		} else {
			die("Database insert failed. Please notify the webmaster");
        }
	} else{
		my $clean_name = $taxon_name;
		$clean_name =~ s/_/ /g;
		print "<center><p>The image of $clean_name was uploaded successfully</p>";
		print "<p><a href=\"$WRITE_URL?action=startImage\">".
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
	my @taxon_no_list = ();
	foreach my $taxon_no (@$taxa_list) {
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

# JA 21.4.12
sub gallery	{
	my ($q,$s,$dbt,$hbo) = @_;
	my ($is_real_user,$not_bot) = (1,1);
	if ( ! PBDBUtil::checkForBot() && (  $q->request_method() eq 'POST' || $q->param('is_real_user') || $s->isDBMember() ) )	{
		main::logRequest($s,$q);
	} else	{
		($is_real_user,$not_bot) = (0,0);
	}
	print $hbo->stdIncludes($PAGE_TOP);
	my $taxon_name = ( $q->param('taxon_name') ) ? $q->param('taxon_name') : $q->param('common_name');
	my $error_style = "padding-top: 1.5em; padding-bottom: 0.5em;";
	if ( ! $taxon_name )	{
		my ($page_title,$error) = ("Image gallery search form",qq|<div class="medium" style="$error_style"><i>Please enter a taxon name</i></div>|);
		print $hbo->populateHTML('search_taxoninfo_form' , [$page_title,$error,1,1], ['page_title','page_subtitle','gallery_form','basic_fields']);
		print $hbo->stdIncludes($PAGE_BOTTOM);
		exit;
	} elsif ( $taxon_name !~ /^[A-Za-z]+$/ )	{
		my ($page_title,$error) = ("Image gallery search form",qq|<div class="medium" style="$error_style"><i>The taxon name seems misformatted</i></div>|);
		print $hbo->populateHTML('search_taxoninfo_form' , [$page_title,$error,1,1], ['page_title','page_subtitle','gallery_form','basic_fields']);
		print $hbo->stdIncludes($PAGE_BOTTOM);
		exit;
	}
	my @matches = TaxonInfo::getTaxonNos($dbt,$taxon_name,'','',$q->param('author'),$q->param('pubyr'),$q->param('type_body_part'),$q->param('preservation'));
	my @taxon_nos = TaxaCache::getChildren($dbt,$matches[0]);
	my @thumbs = getImageList($dbt,\@taxon_nos);
	if ( ! @thumbs )	{
		my ($page_title,$error) = ("Image gallery search form",qq|<div class="medium" style="$error_style"><i>Sorry, we have no images of this taxon</i></div>|);
		print $hbo->populateHTML('search_taxoninfo_form' , [$page_title,$error,1,1], ['page_title','page_subtitle','gallery_form','basic_fields']);
		print $hbo->stdIncludes($PAGE_BOTTOM);
		exit;
	}
	# Safari requires some weird fudging, who knows why
	my $width = ( $#thumbs < 4 ) ? ( $#thumbs + 1 ) * 10.5 + 1.5 : 54.5;
	$width .= "em";
	my $borderWidth = ( $#thumbs < 3 ) ? "0px" : "1px";

	print qq|<center><p class="pageTitle">Images of $taxon_name</p>\n\n<div style="width: $width; margin-left: auto; margin-right: auto; overflow: hidden; border: $borderWidth solid LightGray;">\n|;
	for my $t ( @thumbs )	{
		my $path = $t->{path_to_image};
		$path =~ s/(.*)?(\d+)(.*)$/$1$2_thumb$3/;
		print qq|<div class="verysmall" style="float: left; clear; none; padding: 1.25em;">\n<a href="$READ_URL?a=basicTaxonInfo&taxon_no=$t->{taxon_name}"><img src="$path" style="width: 10em;"></a>\n<br>\n<i><a href="$READ_URL?a=basicTaxonInfo&taxon_no=$t->{taxon_no}">$t->{taxon_name}</a></i>\n</div>\n\n|;
	}
	print qq|</div>\n<p class="medium"><a href="$READ_URL?a=searchGallery">Search again</a></p></center>\n\n|;
	print $hbo->stdIncludes($PAGE_BOTTOM);
}


sub displayImage {
    my ($dbt, $taxonomy, $image_no, $height, $width) = @_;

    my $sql = "SELECT a.taxon_no,a.taxon_name,i.* FROM images i, authorities a where i.taxon_no=a.taxon_no AND i.image_no=$image_no";
    my $row = ${$dbt->getData($sql)}[0];
    if (!$row) {
        print "<div class=errorMessage>Error, no image to display</div>";
    } else {
        my $ss = $taxonomy->getRelatedTaxon('synonym', $row->{'taxon_no'});
        
        print "<div align=\"center\" style=\"padding-top: 20px;\">";
        print "<img src=\"".$row->{'path_to_image'}."\" height=\"$height\" width=\"$width\" border=1><br>\n";
        print "<i>".$row->{'caption'}."</i><br>\n";
        print "<div class=\"small\">";
        print "<br><table border=0 cellpadding=2 cellspacing=0>";
        print "<tr><td>Original name of image:</td><td>".$row->{'original_filename'}."</td></tr>\n";
        if ( $ss && $row->{'taxon_no'} != $ss->{'taxon_no'} ) {
            print "<tr><td>Original identification:</td><td><a target=\"_blank\" href=\"$READ_URL?action=basicTaxonInfo&taxon_no=$row->{taxon_no}\">".$row->{'taxon_name'}."</a></td></tr>\n";
            print "<tr><td>Current identification:</td><td><a target=\"_blank\" href=\"$READ_URL?action=basicTaxonInfo&taxon_no=$ss->{taxon_no}\">".$ss->{'taxon_name'}."</a></td></tr>\n";
        } else {
            print "<tr><td>Current identification:</td><td><a target=\"_blank\" href=\"$READ_URL?action=basicTaxonInfo&taxon_no=$row->{taxon_no}\">".$row->{'taxon_name'}."</a></td></tr>\n";
        }
        if ( $row->{reference_no} > 0 ) {
            my $sql = "SELECT reference_no,author1last,author2last,otherauthors,pubyr FROM refs WHERE reference_no=$row->{reference_no}";
            my $ref = ${$dbt->getData($sql)}[0];
            my $ref_string = Reference::formatShortRef($ref,'link_id'=>1);  
            $ref_string =~ s/<a /<a target="_blank" /;
            print "<tr><td>Reference:</td><td> $ref_string</td></tr>\n";
        }
        print "</table>";
        print "</div>";
    }   
}

1;
