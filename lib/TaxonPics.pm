# 
# The Paleobiology Database
# 
#   TaxonPics.pm
# 

package TaxonPics;

use strict;

use feature 'try';
no warnings 'experimental::try';

# Modules needed

use Carp qw(carp croak);
use LWP::UserAgent;
use URL::Encode qw(url_encode_utf8);
use JSON qw(decode_json);
use POSIX qw(strftime);
use Storable;

use CoreFunction qw(activateTables);
use TableDefs qw(%TABLE);
use CoreTableDefs;

use ConsoleLog qw(logMessage);

use base 'Exporter';

our (@EXPORT_OK) = qw(getPics selectPics loadPic);

our ($TAXON_PICS_WORK) = 'tpn';

our ($PHYLOPIC_API_URL) = 'https://api.phylopic.org/';
our ($PHYLOPIC_IMAGE_URL) = 'https://images.phylopic.org/';
our ($PHYLOPIC_HEADER) = 'application/vnd.phylopic.v2+json';
our ($PHYLOPIC_BUILD);

our ($store_filename) = '.phylopic_data';

# getPics ( dbh )
# 
# Fetch any taxon pictures that have been added or updated since the last
# fetch. 

sub getPics {

    my ($dbh, $options) = @_;
    
    my ($list);
    my ($raw_count) = 0;
    
    $options ||= { };
    
    # Figure out the date of the last fetch that we did.  If no images are in
    # the table, use a date that will cause all available images to be fetched.
    
    my ($since_date) = $dbh->selectrow_array("
		SELECT cast(modified as date) FROM $TABLE{PHYLOPIC_DATA}
		WHERE uid = 'LAST_FETCH'");
    
    if ( $options->{fetch_all} || !$since_date )
    {
	$since_date = "2001-01-01 00:00:00";
    }

    $since_date .= " 00:00:00" unless $since_date =~ /:\d\d$/;
    $since_date =~ s/ /T/;
    $since_date .= 'Z' if $since_date =~ /[.]\d{3}$/;
    $since_date .= '.000Z' unless $since_date =~ /[.]\d{3}Z$/;
    
    my (@images, @names, $retrieved_from_disk, $new_timestamp);
    
    # If we have saved phylopic data, use that.
    
    if ( -r $store_filename )
    {
	logMessage(2, "    loading saved data...");
	my $saved_data = retrieve($store_filename);
	@images = $saved_data->{images}->@*;
	@names = $saved_data->{names}->@*;
	$retrieved_from_disk = 1;
    }
    
    # Otherwise, list all images modified since the last fetch date.

    else
    {
	my $word = $options->{fetch_all} ? 'all' : 'new';
	
	logMessage(2, "    querying for $word phylopics...");
	
	my $ua = LWP::UserAgent->new();
	$ua->agent("Paleobiology Database Updater");
	
	# Fetch the build timestamp.
	
	my $response = phylopicRequest($ua, 'json', '');
	
	$new_timestamp = $response->{buildTimestamp};
	
	$new_timestamp =~ s/T/ /;
	$new_timestamp =~ s/[.]\d{3}Z$//;
	
	# Fetch the number of images and pages that have been modified since the
	# last fetch (or ever).
	
	my $response = phylopicRequest($ua, 'json', 'images',
				       [ filter_modified_after => $since_date ]);
	
	my $image_count = $response->{totalItems};
	my $page_count = $response->{totalPages};
	
	logMessage(2, "    fetching $image_count image records ($page_count pages)...");
	
	# Now fetch each page of the results.
	
	for ( my $page = 0; $page < $page_count; $page++ )
	{
	    # Fetch one page of items.
	    
	    my $response = phylopicRequest($ua, 'json', 'images',
					   [ filter_modified_after => $since_date,
					     embed_items => 'true',
					     embed_nodes => 'true',
					     embed_contributor => 'true',
					     page => $page ]);
	    
	    my $items = $response->{_embedded}{items};
	    
	    # Iterate through all of the items on the page.
	    
	    foreach my $i ( $items->@* )
	    {
		# Extract deeply buried attributes.
		
		$i->{credit} = $i->{_embedded}{contributor}{name};
		$i->{license} = $i->{_links}{license}{href};
		
		# Convert timestamps to the format that MariaDB expects.
		
		foreach my $field ( qw(created modified modifiedFile) )
		{
		    $i->{$field} =~ s/T/ /;
		    $i->{$field} =~ s/[.]\d{3}Z$//;
		}
		
		# If there are any nodes (taxonomic concepts) associated with this item,
		# then we can look for taxonomic names. The taxonomic concepts defined by
		# phylopic.org are not the same as the ones defined by the PBDB. For
		# example, if there is a genus with one species represented in the Phylopics
		# database, they both belong to the same node because they are represented
		# by the same image. In the PBDB, by contrast, they would belong to different
		# taxonomic concepts.
		
		if ( my $nodes = $i->{_embedded}{nodes} )
		{
		    # Iterate through all of the nodes associated with this item.
		    
		    foreach my $n ( $nodes->@* )
		    {
			my %pbdb_orig_no;
			
			# If we have an external link that looks like
			# /resolve/paleobiodb.org/txn/... then we can extract from that
			# link the PBDB orig_no value corresponding to the name which is
			# the value of the 'title' attribute.
			
			if ( my $external = $n->{_links}{external} )
			{
			    foreach my $e ( $external->@* )
			    {
				if ( $e->{href} =~ qr{^/resolve/paleobiodb[.]org/txn/(\d+)} &&
				     $e->{title} )
				{
				    $pbdb_orig_no{$e->{title}} = $1;
				}
			    }
			}
			
			# If there are any taxonomic names associated with this node,
			# iterate through them.
			
			if ( my $names = $n->{names} )
			{
			    foreach my $nn ( $names->@* )
			    {
				# Create a new record for each name.
				
				my %name = ( uuid => $i->{uuid} );
				
				# Each taxonomic name in the API response consists of a
				# list of records, differentiated by the value of
				# 'class'. Iterate through them and extract the relevant
				# information from each.
				
				foreach my $nnn ( $nn->@* )
				{
				    # A 'scientific' record gives the actual taxonomic name.
				    
				    if ( $nnn && $nnn->{class} eq 'scientific' )
				    {
					$name{taxon_name} = $nnn->{text};
					$name{orig_no} = $pbdb_orig_no{$nnn->{text}}
					    if $pbdb_orig_no{$nnn->{text}};
				    }
				    
				    # A 'citation' record gives the attribution of the name.
				    
				    elsif ( $nnn && $nnn->{class} eq 'citation' )
				    {
					$name{taxon_attr} = $nnn->{text};
				    }
				    
				    # A 'vernacular' record gives the common name.
				    
				    elsif ( $nnn && $nnn->{class} eq 'vernacular' )
				    {
					$name{common_name} = $nnn->{text};
				    }
				}
				
				# We ignore names for which we don't have orig_no values, so
				# that we don't have to worry about linking names up to PBDB
				# taxonomic concepts on our own.
				
				if ( $name{taxon_name} && $name{orig_no} )
				{
				    push @names, \%name;
				}
			    }
			}
		    }
		}
		
		# Every image should have at least a uuid and a creation time.
		
		if ( $i->{uuid} && $i->{created} )
		{
		    push @images, $i;
		}
	    }
	}
	
	# Now iterate through the image records and fetch the thumbnail for each one.
	
	my $png_count = scalar(@images);
	
	logMessage(2, "    fetching $png_count thumbnails...");
	
	foreach my $image ( @images )
	{
	    $image->{thumb} = phylopicRequest($ua, 'png',
					      "images/$image->{uuid}/thumbnail/64x64.png");
	}

	# Store this data on disk, just in case something goes wrong while storing it
	# into the database.
	
	logMessage(2, "    storing data in a temporary file...");
	
	store { images => \@images, names => \@names }, $store_filename;
    }
    
    # Now insert the data into the database as a single transaction.
    
    $dbh->begin_work;
    
    try {
	
	# If $options->{fetch_all} is true, then delete all records from the tables that
	# we will be inserting into. We use "DELETE FROM" instead of "TRUNCATE" because
	# that will be reversed if the transaction is rolled back.
	
	if ( $options->{fetch_all} )
	{
	    logMessage(2, "    emptying tables '$TABLE{PHYLOPIC_DATA}', " .
		       "'$TABLE{PHYLOPIC_NAMES}', '$TABLE{PHYLOPIC_CHOICE}'...");
	    
	    $dbh->do("DELETE FROM $TABLE{PHYLOPIC_DATA} WHERE uid <> 'INVALID_TAXON'");
	    $dbh->do("ALTER TABLE $TABLE{PHYLOPIC_DATA} auto_increment=1");
	    $dbh->do("INSERT INTO $TABLE{PHYLOPIC_DATA} (uid, modified)
			VALUES ('LAST_FETCH', '$new_timestamp')");
	    $dbh->do("DELETE FROM $TABLE{PHYLOPIC_NAMES}");
	}
	
	else
	{
	    logMessage(2, "    updating last fetch timestamp...");
	    
	    $dbh->do("REPLACE INTO $TABLE{PHYLOPIC_DATA} (uid, modified)
			VALUES ('LAST_FETCH', '$new_timestamp')");
	}
	
	logMessage(2, "    storing image data...");
	
	# Iterate through the image records we have collected. Skip any which don't have
	# thumbnail data, because we can't use those.
	
	my %image_no_map;
	
	foreach my $image ( @images )
	{
	    next unless $image->{thumb};
	    
	    my $uid = $dbh->quote($image->{uuid});
	    my $credit = $dbh->quote($image->{credit});
	    my $attribution = $dbh->quote($image->{attribution});
	    my $license = $dbh->quote($image->{license});
	    my $created = $dbh->quote($image->{created});
	    my $modified = $dbh->quote($image->{modified});
	    my $mod_file = $dbh->quote($image->{modifiedFile});
	    my $thumb = $dbh->quote($image->{thumb}, DBI::SQL_BINARY);
	    
	    # Create a new record, or update the existing one. Try an update first, and
	    # if that doesn't match any rows then do an insert ignore. That way, if the
	    # update matched a row but didn't actually change any fields then the insert
	    # won't change anything. Our goal is to not create new records if we don't
	    # have to, so that the image_no values don't change.
	    
	    my $result = $dbh->do("
		UPDATE $TABLE{PHYLOPIC_DATA}
		SET credit = $credit, attribution = $attribution, license = $license,
			created = $created, modified = $modified, modified_file = $mod_file,
			thumb = $thumb
		WHERE uid = $uid");
	    
	    if ( $result =~ /^0/ )
	    {
		$dbh->do("
		INSERT IGNORE INTO $TABLE{PHYLOPIC_DATA} (uid, credit, attribution, license,
			created, modified, modified_file, thumb)
		VALUES ($uid, $credit, $attribution, $license,
			$created, $modified, $mod_file, $thumb)");
	    }
	    
	    my ($image_no) = $dbh->selectrow_array("
		SELECT image_no FROM $TABLE{PHYLOPIC_DATA} WHERE uid = $uid");
	    
	    # Then delete the old set of names corresponding to this image. The new ones
	    # will be inserted immediately below. This is unnecessary if we are fetching
	    # all images.
	    
	    my $img = $dbh->quote($image_no);
	    
	    $result = $dbh->do("DELETE FROM $TABLE{PHYLOPIC_NAMES} WHERE image_no = $img")
		unless $options->{fetch_all};
	    
	    $image_no_map{$uid} = $img;
	}
	
	logMessage(2, "    storing name data...");
	
	# Iterate through all of the name records we have collected.
	
	foreach my $name ( @names )
	{
	    my $uid = $dbh->quote($name->{uuid});
	    my $orig_no = $dbh->quote($name->{orig_no});
	    my $taxon_name = $dbh->quote($name->{taxon_name});
	    my $taxon_attr = $dbh->quote($name->{taxon_attr} || '');
	    my $common_name = $dbh->quote($name->{common_name});
	    
	    $dbh->do("REPLACE INTO $TABLE{PHYLOPIC_NAMES}
			(image_no, taxon_name, taxon_attr, common_name, orig_no)
		      VALUES ($image_no_map{$uid}, $taxon_name, $taxon_attr, $common_name, $orig_no)");
	}
    }
    
    catch ($e)
    {
	logMessage(2, "    rolling back...");
	$dbh->rollback;
	die $@;
    }
    
    logMessage(2, "    committing...");
    $dbh->commit;
    
    unlink($store_filename);
    
    logMessage(2, "    done.");
    
    # Finally, add a new record to the PHYLOPIC_CHOICE table for each orig_no that
    # doesn't already have a choice. Order them by date created so that the choices will
    # be stable.
    
    logMessage(2, "    adding new images to table '$TABLE{PHYLOPIC_CHOICE}'...");
    
    my $sql = "INSERT IGNORE INTO $TABLE{PHYLOPIC_CHOICE} (orig_no, image_no)
		SELECT pn.orig_no, pd.image_no
		FROM $TABLE{PHYLOPIC_NAMES} as pn join $TABLE{PHYLOPIC_DATA} as pd using (uid)
		ORDER BY pd.created";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    my $result = $dbh->do($sql);
    
    logMessage(2, "    done.");
    
    return;
    
    # my $req = HTTP::Request->new(GET => "https://api.phylopic.org/api/a/image/list/modified/$since_date?options=taxa+licenseURL+string+modified+credit+citationStart", [ Accept => 'application/vnd.phylopic.v2+json' ]);
    
    # my $response = $ua->request($req);
    
    # if ( $response->is_success )
    # {
    # 	$list = from_json($response->content, { latin1 => 1 });
    # 	$raw_count = scalar(@{$list->{result}}) if ref $list->{result} eq 'ARRAY';
    # }
    
    # else
    # {
    # 	logMessage(2, "      FAILED.");
    # 	return;
    # }
    
    # # If we get here, then the fetch succeeded.
    
    # logMessage(2, "      fetched $raw_count records.");
    
    # # Mark the date of last fetch.  We subtract 2 minutes in case a new
    # # phylopic came in while we were decoding the JSON response above.
    
    # $dbh->do("INSERT IGNORE INTO $TABLE{PHYLOPIC_DATA} (uid, modified) VALUES ('LAST_FETCH', date_sub(now(), interval 2 minute))");
    # $dbh->do("UPDATE $TABLE{PHYLOPIC_DATA} SET modified = date_sub(now(), interval 2 minute) WHERE uid = 'LAST_FETCH'");
    
    # # Go through the records, and store one record for each pic and an
    # # associated record for each name.
    
    # foreach my $r ( @{$list->{result}} )
    # {
    # 	next unless ref $r->{taxa} eq 'ARRAY';
	
    # 	my $uid = $dbh->quote($r->{uid});
    # 	my $modified = $dbh->quote($r->{modified});
    # 	my $credit = $dbh->quote($r->{credit});
    # 	my $license = $dbh->quote($r->{licenseURL});
    # 	my $pd = $license =~ /pub/ ? 1 : 0;
	
    # 	# Create a new record, or update the existing one.  We try an update
    # 	# first and if that doesn't match any rows then we do a replace (just
    # 	# to forestall any strange errors, it's better to create a new record
    # 	# than to die with a "duplicate key" error.  But our goal is to not
    # 	# create new records if we don't have to, so that the image_no values
    # 	# don't change.
	
    # 	my $result = $dbh->do("
    # 		UPDATE $TABLE{PHYLOPIC_DATA}
    # 		SET modified = $modified, credit = $credit, license = $license
    # 		WHERE uid = $uid");
	
    # 	if ( $result =~ /^0/ )
    # 	{
    # 	    $dbh->do("
    # 		REPLACE INTO $TABLE{PHYLOPIC_DATA} (uid, modified, credit, license)
    # 		VALUES ($uid, $modified, $credit, $license)");
    # 	}
	
    # 	logMessage(2, "      found image $uid modified $modified");
	
    # 	# Make sure we have an image_no value for this record.
	
    # 	my ($image_no) = $dbh->selectrow_array("
    # 		SELECT image_no FROM $TABLE{PHYLOPIC_DATA} WHERE uid = $uid");
	
    # 	next unless $image_no;
	
    # 	# Fetch the binary data for the thumbnail.
	
    # 	my $url = "http://phylopic.org/assets/images/submissions/$r->{uid}.thumb.png";
    # 	my $req = HTTP::Request->new(GET => $url );
	
    # 	my $response = $ua->request($req);
	
    # 	if ( $response->is_success )
    # 	{
    # 	    my $content = $response->content;
	    
    # 	    my $stmt = $dbh->prepare("UPDATE $TABLE{PHYLOPIC_DATA} SET thumb = ? WHERE UID = $uid");
    # 	    $result = $stmt->execute($response->content);
    # 	}
	
    # 	else
    # 	{
    # 	    logMessage(2, "        thumb FAILED: $url");
    # 	    return;
    # 	}
	
    # 	# Fetch the binary data for the icon.
	
    # 	$url = "http://phylopic.org/assets/images/submissions/$r->{uid}.icon.png";
    # 	$req = HTTP::Request->new(GET => $url );
	
    # 	$response = $ua->request($req);
	
    # 	if ( $response->is_success )
    # 	{
    # 	    my $content = $response->content;
	    
    # 	    my $stmt = $dbh->prepare("UPDATE $TABLE{PHYLOPIC_DATA} SET icon = ? WHERE UID = $uid");
    # 	    $result = $stmt->execute($response->content);
    # 	}
	
    # 	else
    # 	{
    # 	    logMessage(2, "        icon FAILED: $url");
    # 	    return;
    # 	}
	
    # 	# Figure out which taxonomic names, if any, this pic is associated
    # 	# with.  Delete all of the existing ones and store a new set.
	
    # 	$result = $dbh->do("DELETE FROM PHYLOPIC_NAMES WHERE uid = $uid");
	
    # 	foreach my $t ( @{$r->{taxa}} )
    # 	{
    # 	    my $name = $t->{canonicalName}{string};
    # 	    my $name_len = $t->{canonicalName}{citationStart};
	    
    # 	    next unless defined $name && $name ne '';
	    
    # 	    # Split off the attribution from the taxonomic name.
	    
    # 	    my ($taxon_name, $taxon_attr);
	    
    # 	    if ( $name_len > 0 )
    # 	    {
    # 		$taxon_name = $dbh->quote(substr($name, 0, $name_len - 1));
    # 		$taxon_attr = $dbh->quote(substr($name, $name_len));
    # 	    }
	    
    # 	    else
    # 	    {
    # 		$taxon_name = $dbh->quote($name);
    # 		$taxon_attr = "''";
    # 	    }
	    
    # 	    $result = $dbh->do("
    # 		INSERT IGNORE INTO $TABLE{PHYLOPIC_NAMES} (uid, taxon_name, taxon_attr)
    # 		VALUES ($uid, $taxon_name, $taxon_attr)");
    # 	}
    # }
}


# phylopicRequest ( ua, format, path, params )
#
# Make a request to api.phylopic.org with the specified path and parameters. The
# value of $format must be either 'json' or 'png'. The value of $params if
# non-empty must be an arrayref with alternating parameters and values.
#
# Returns the request content, decoded from JSON if $format is 'json'.

sub phylopicRequest {

    my ($ua, $format, $path, $params) = @_;
    
    # Assemble the request URL. Parameter values are URL-encoded.
    
    my @params;
    
    if ( $PHYLOPIC_BUILD && $format eq 'json' )
    {
	push @params, "build=$PHYLOPIC_BUILD";
    }

    if ( $params )
    {
	my @raw = $params->@*;
	
	while ( @raw )
	{
	    my $p = shift @raw;
	    my $v = shift @raw;
	    push @params, "$p=" . url_encode_utf8($v);
	}
    }
    
    my $param_string = join('&', @params);
    
    my $url = $format eq 'json' ? "$PHYLOPIC_API_URL$path" : "$PHYLOPIC_IMAGE_URL$path";
    $url .= "?$param_string" if $param_string;
    
    # If the message level is 4, the following message will be output for
    # debugging purposes.
    
    logMessage(4, "FETCH: $url");
    
    # Make the request. If the response is a success, decode the JSON content
    # and return it as a Perl reference. If the response code is 5xx, wait 5
    # seconds and try again up to 3 times. For any other response code, we abort.
    
    my $tries = 0;
    
    while (1)
    {
	my $request = HTTP::Request->new(GET => $url, [ Accept => $PHYLOPIC_HEADER ]);
	my $response = $ua->request($request);
        
	if ( $response->is_success )
	{
	    my $content;
	    
	    if ( $format eq 'json' )
	    {
		$content = decode_json($response->content);
		
		if ( $content->{build} > 0 && !$PHYLOPIC_BUILD )
		{
		    $PHYLOPIC_BUILD = $content->{build};
		}
	    }
	    
	    else
	    {
		$content = $response->content;
	    }
	    
	    unless ( $content )
	    {
		logMessage(2, "EMPTY RESPONSE from $url");
		die "Aborting";
	    }
	    
	    return $content;
	}
	
	elsif ( $response->code =~ /^5/ && ++$tries < 3 )
	{
	    my $code = $response->code;
	    logMessage(2, "CODE $code FROM $url");
	    sleep 3 * $tries;
	    next;
	}
	
	else
	{
	    my $code = $response->code;
	    logMessage(2, "CODE $code from $url");
	    die "Aborting";
	}
    }
}


# loadPic ( dbh, uid, filename )
# 
# Read image data from the specified file, and insert it into the
# PHYLOPIC_DATA table.

sub loadPic {
    
    my ($dbh, $uid, $filename) = @_;
    
    my ($image_fh, $image_data, $created, $modified, $dummy);
    
    open $image_fh, '<', $filename or die "Could not open '$filename': $!\n";
    
    binmode $image_fh, ':raw';
    
    my @stat = stat $image_fh;
    
    read $image_fh, $image_data, $stat[7] or die "Could not read '$filename': $!\n";
    
    my $len = length($image_data);
    
    logMessage(2, "Read $len bytes from '$filename'");
    
    my $quid = $dbh->quote($uid);
    my $qmodified = $dbh->quote(strftime "%Y-%m-%d %H:%M:%S", localtime($stat[9]));
    my $qimg = $dbh->quote($image_data, DBI::SQL_BINARY);
    
    my $sql = "REPLACE INTO $TABLE{PHYLOPIC_DATA} (uid, thumb, modified)
		VALUES ($quid, $qimg, $qmodified)";
    
    $dbh->do($sql) && logMessage(2, "Loaded image data into '$uid'");
}


# selectPics ( dbh )
# 
# This is now a no-op, because entries are added to the PHYLOPIC_CHOICE table by &getPics.

sub selectPics {
    
    # my ($dbh, $tree_table) = @_;
    
    # my ($phylopics) = eval {
    # 	local($dbh->{PrintError}) = 0;
    # 	$dbh->selectrow_array("SELECT count(*) from $TABLE{PHYLOPIC_DATA}");
    # };
    
    # unless ( $phylopics > 0 )
    # {
    # 	logMessage(2, "    skipping phylopics because table '$TABLE{PHYLOPIC_DATA}' " .
    # 		   "does not exist in this database");
    # 	return;
    # }
    
    # # Now copy all of the new records into the PHYLOPIC_CHOICE table with
    # # priority=1.  That priority can be adjusted later.
    
    # logMessage(2, "    adding new records to pic choice table...");
    
    # my $sql = "
    # 	INSERT IGNORE INTO $TABLE{PHYLOPIC_CHOICE} (orig_no, uid, taxon_name, taxon_attr, priority)
    # 	SELECT t.orig_no, ppn.uid, 1
    # 	FROM $tree_table as t JOIN $TABLE{PHYLOPIC_NAMES} as ppn on t.name = pqn.taxon_name";
    
    # my $result;
    
    # eval {
    # 	$result = $dbh->do($sql);
    # };
    
    # # if ( $@ && $@ =~ /Illegal mix of collations/ )
    # # {
    # # 	$dbh->do("ALTER TABLE $TABLE{PHYLOPIC_NAMES} modify column taxon_name varchar(80) collate utf8_general_ci");
	
    # # 	$result = $dbh->do($sql);
    # # }
    
    # logMessage(2, "      added $result records");
    
    # logMessage(2, "    selecting the phylopic for each taxon...");
    
    # my ($result, $sql);
    
    # # Create a working table with which to do our selection.
    
    # $dbh->do("DROP TABLE IF EXISTS $TAXON_PICS_WORK");
    
    # $dbh->do("CREATE TABLE $TAXON_PICS_WORK (
    # 		orig_no int unsigned primary key,
    # 		image_no int unsigned not null) Engine=InnoDB");
    
    # # Select images by priority number, or by earlier modification date
    # # otherwise.
    
    # $result = $dbh->do("
    # 		INSERT IGNORE INTO $TAXON_PICS_WORK (orig_no, image_no)
    # 		SELECT orig_no, image_no
    # 		FROM $TABLE{PHYLOPIC_CHOICE} join $TABLE{PHYLOPIC_DATA} using (uid)
    # 		WHERE priority > 0
    # 		ORDER BY priority desc");
    
    # # Activate the new table.
    
    # activateTables($dbh, $TAXON_PICS_WORK => $TABLE{TAXON_PICS});
}


# ensureTables ( dbh )
# 
# If the proper table does not exist, create it.

sub ensureTables {

    my ($dbh, $force) = @_;
    
    my ($sql, $result);
    
    if ( $force )
    {
	$dbh->do("DROP TABLE IF EXISTS $TABLE{PHYLOPIC_DATA}");
	$dbh->do("DROP TABLE IF EXISTS $TABLE{PHYLOPIC_NAMES}");
	$dbh->do("DROP TABLE IF EXISTS $TABLE{PHYLOPIC_CHOICE}");
    }
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $TABLE{PHYLOPIC_DATA} (
	`uid` varchar(80) NOT NULL PRIMARY KEY,
	`image_no` int unsigned NOT NULL AUTO_INCREMENT,
	`contributor` varchar(255) DEFAULT NULL,
	`credit` varchar(255) DEFAULT NULL,
	`license` varchar(255) DEFAULT NULL,
	`created` timestamp NOT NULL,
	`modified` timestamp NOT NULL,
	`modified_file` timestamp NOT NULL,
	`thumb` blob DEFAULT NULL,
	key (`image_no`),
	key (`created`)) Engine=InnoDB CHARACTER SET utf8 COLLATE utf8_general_ci");
    
    $dbh->do("INSERT INTO $TABLE{PHYLOPIC_DATA} (uid, modified)
	      VALUES ('LAST_FETCH', '2001-01-01')") if $force;
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $TABLE{PHYLOPIC_NAMES} (
	`image_no` int unsigned NOT NULL,
	`taxon_name` varchar(80) NOT NULL,
	`taxon_attr` varchar(100) DEFAULT NULL,
	`common_name` varchar(100) DEFAULT NULL,
	`orig_no` int(11) unsigned DEFAULT NULL,
	primary key (`image_no`, `taxon_name`, `taxon_attr`),
	key (`orig_no`)) Engine=InnoDB CHARACTER SET utf8 COLLATE utf8_general_ci");
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $TABLE{PHYLOPIC_CHOICE} (
	`orig_no` int unsigned NOT NULL PRIMARY KEY,
	`image_no` int unsigned NOT NULL,
	`modifier_no` int unsigned NULL,
	`modified` timestamp NULL,
	key (`image_no`)) Engine=InnoDB CHARACTER SET utf8 COLLATE utf8_general_ci");
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $TABLE{PHYLOPIC_SEEN} (
	`person_no` int unsigned NOT NULL,
	`orig_no` int unsigned NOT NULL,
	`image_no` int unsigned NOT NULL,
	PRIMARY KEY (`person_no`, `orig_no`, `image_no`)
    ) Engine=InnoDB CHARACTER SET utf8 COLLATE utf8_general_ci");
    
    my $a = 1;	# we can stop here when debugging
}

1;
