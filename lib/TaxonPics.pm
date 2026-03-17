# 
# The Paleobiology Database
# 
#   TaxonPics.pm
# 

package TaxonPics;

use strict;

# Modules needed

use Carp qw(carp croak);
use LWP::UserAgent;
use URL::Encode qw(url_encode_utf8);
use JSON qw(decode_json);

use CoreFunction qw(activateTables);
use TableDefs qw(%TABLE);
use CoreTableDefs;

use ConsoleLog qw(logMessage);

use base 'Exporter';

our (@EXPORT_OK) = qw(getPics selectPics $PHYLOPICS $PHYLOPIC_NAMES $PHYLOPIC_CHOICE $TAXON_PICS);

our ($PHYLOPICS) = 'phylopics';
our ($PHYLOPIC_NAMES) = 'phylopic_names';
our ($PHYLOPIC_CHOICE) = 'phylopic_choice';
our ($TAXON_PICS) = 'taxon_pics';

our ($TAXON_PICS_WORK) = 'tpn';

our ($PHYLOPIC_API_URL) = 'https://api.phylopic.org/';
our ($PHYLOPIC_IMAGE_URL) = 'https://images.phylopic.org/';
our ($PHYLOPIC_HEADER) = 'application/vnd.phylopic.v2+json';
our ($PHYLOPIC_BUILD);


# getPics ( dbh )
# 
# Fetch any taxon pictures that have been added or updated since the last
# fetch. 

sub getPics {

    my ($dbh, $fetch_all) = @_;
    
    my ($list);
    my ($raw_count) = 0;
    
    # Figure out the date of the last fetch that we did.  If no images are in
    # the table, use a date that will cause all available images to be fetched.
    
    my ($since_date) = $dbh->selectrow_array("
		SELECT cast(modified as date) FROM $TABLE{PHYLOPIC_DATA}
		WHERE uid = 'LAST_FETCH'");
    
    if ( $fetch_all || !$since_date )
    {
	$since_date = "2001-01-01 00:00:00";
    }

    $since_date .= " 00:00:00" unless $since_date =~ /:\d\d$/;
    $since_date =~ s/ /T/;
    $since_date .= 'Z' if $since_date =~ /[.]\d{3}$/;
    $since_date .= '.000Z' unless $since_date =~ /[.]\d{3}Z$/;
    
    # List all images modified since that date.
    
    logMessage(2, "    querying for new phylopics...");
    
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database Updater");
    
    # Fetch the build timestamp.
    
    my $response = phylopicRequest($ua, 'json', '');
    
    my $new_timestamp = $response->{buildTimestamp};

    $new_timestamp =~ s/T/ /;
    $new_timestamp =~ s/[.]\d{3}Z$//;
    
    # Fetch the number of images and pages that have been modified since the
    # last fetch (or ever).
    
    my $response = phylopicRequest($ua, 'json', 'images',
				   [ filter_license_by => 'false',
				     filter_modified_after => $since_date ]);
    
    my $image_count = $response->{totalItems};
    my $page_count = $response->{totalPages};
    
    logMessage(2, "    fetching $image_count image records ($page_count pages)...");

    # Now fetch each page of the results.

    my @images;
    my @names;
    
    for ( my $page = 0; $page < $page_count; $page++ )
    {
	my $response = phylopicRequest($ua, 'json', 'images',
				       [ filter_license_by => 'false',
					 filter_modified_after => $since_date,
					 embed_items => 'true',
					 embed_nodes => 'true',
					 embed_contributor => 'true',
					 page => $page ]);
	
	my $items = $response->{_embedded}{items};
	
	foreach my $i ( $items->@* )
	{
	    my %image;
	    
	    $image{credit} = $i->{attribution} || $i->{_embedded}{contributor}{name};
	    $image{license} = $i->{_links}{license}{href};
	    $image{modified} = $i->{modified};
	    
	    $image{modified} =~ s/T/ /;
	    $image{modified} =~ s/[.]\d{3}Z$//;
	    
	    $image{uuid} = $i->{uuid};
	    
	    if ( my $nodes = $i->{_embedded}{nodes} )
	    {
		foreach my $n ( $nodes->@* )
		{
		    my %pbdb_orig_no;
		    
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
		    
		    if ( my $names = $n->{names} )
		    {
			foreach my $nn ( $names->@* )
			{
			    my %name = ( uuid => $image{uuid} );
			    
			    foreach my $nnn ( $nn->@* )
			    {
				if ( $nnn && $nnn->{class} eq 'scientific' )
				{
				    $name{taxon_name} = $nnn->{text};
				    $name{orig_no} = $pbdb_orig_no{$nnn->{text}}
					if $pbdb_orig_no{$nnn->{text}};
				}
				
				elsif ( $nnn && $nnn->{class} eq 'citation' )
				{
				    $name{taxon_attr} = $nnn->{text};
				}
				
				elsif ( $nnn && $nnn->{class} eq 'vernacular' )
				{
				    $name{common_name} = $nnn->{text};
				}
			    }
			    
			    if ( $name{taxon_name} && $name{uuid} )
			    {
				push @names, \%name;
			    }
			}
		    }
		}
	    }
	    
	    if ( $image{uuid} && $image{modified} )
	    {
		push @images, \%image;
	    }
	}
    }
    
    # $$$ TEMPORARY $$$
    
    logMessage(2, "    storing names only...");
    
    foreach my $name (@names )
    {
	my $uid = $dbh->quote($name->{uuid});
	my $taxon_name = $dbh->quote($name->{taxon_name});
	my $taxon_attr = $dbh->quote($name->{taxon_attr});
	my $orig_no = $dbh->quote($name->{orig_no});
	my $common_name = $dbh->quote($name->{common_name});

	$dbh->do("REPLACE INTO $TABLE{PHYLOPIC_NAMES}
		  (uid, taxon_name, taxon_attr, common_name, orig_no)
		  VALUES ($uid, $taxon_name, $taxon_attr, $common_name, $orig_no)");
    }
    
    return;
    
    # $$$ END $$$
    
    # Now fetch the thumbnail for each image
    
    my $png_count = scalar(@images);
    
    logMessage(2, "    fetching $png_count thumbnails...");
    
    foreach my $image ( @images )
    {
	$image->{thumb} = phylopicRequest($ua, 'png',
					  "images/$image->{uuid}/thumbnail/64x64.png");
    }
    
    # Now insert the data into the database as a single transaction.
    
    $dbh->{AutoCommit} = 0;
    $dbh->{RaiseError} = 1;
    
    $@ = '';
    
    eval {
	
	# If $fetch_all is true, then truncate both tables that we will be
	# inserting into.
	
	if ( $fetch_all )
	{
	    logMessage(2, "    emptying tables $TABLE{PHYLOPIC_DATA}, $TABLE{PHYLOPIC_NAMES}...");
	    
	    $dbh->do("DELETE FROM $TABLE{PHYLOPIC_DATA}");
	    $dbh->do("ALTER TABLE $TABLE{PHYLOPIC_DATA} auto_increment=1");
	    $dbh->do("INSERT INTO $TABLE{PHYLOPIC_DATA} (uid, modified)
			VALUES ('LAST_FETCH', '$new_timestamp')");
	    $dbh->do("DELETE FROM $TABLE{PHYLOPIC_NAMES}");
	}
	
	logMessage(2, "    storing image data...");
	
	foreach my $image ( @images )
	{
	    my $uid = $dbh->quote($image->{uuid});
	    my $modified = $dbh->quote($image->{modified});
	    my $credit = $dbh->quote($image->{credit});
	    my $license = $dbh->quote($image->{licenseURL});
	    
	    # Create a new record, or update the existing one.  We try an update
	    # first and if that doesn't match any rows then we do a replace (just
	    # to forestall any strange errors, it's better to create a new record
	    # than to die with a "duplicate key" error.  But our goal is to not
	    # create new records if we don't have to, so that the image_no values
	    # don't change.
	    
	    my $result = $dbh->do("
		UPDATE $PHYLOPICS
		SET modified = $modified, credit = $credit, license = $license
		WHERE uid = $uid");
	    
	    if ( $result =~ /^0/ )
	    {
		$dbh->do("
		REPLACE INTO $PHYLOPICS (uid, modified, credit, license)
		VALUES ($uid, $modified, $credit, $license)");
	    }
	    
	    # Then update the record to add (or replace) the thumbnail data. We
	    # do it this way because the thumbnail data is a byte string, not a
	    # character string.
	    
	    my $stmt = $dbh->prepare("UPDATE $TABLE{PHYLOPIC_DATA}
					SET thumb = ? WHERE uid = $uid");
	    $result = $stmt->execute($image->{thumb});

	    # Finally, delete the old set of names corresponding to this image.
	    # The new ones will be inserted immediately below.
	    
	    $result = $dbh->do("DELETE FROM $TABLE{PHYLOPIC_NAMES} WHERE uid = $uid");
	}
	
	logMessage(2, "    storing name data...");
	
	foreach my $name ( @names )
	{
	    my $uid = $dbh->quote($name->{uuid});
	    my $taxon_name = $dbh->quote($name->{taxon_name});
	    my $taxon_attr = $dbh->quote($name->{taxon_attr} || '');
	    
	    $dbh->do("REPLACE INTO $TABLE{PHYLOPIC_NAMES} (uid, taxon_name, taxon_attr)
			VALUES ($uid, $taxon_name, $taxon_attr)");
	}
    };

    if ( $@ )
    {
	logMessage(2, "    rolling back...");
	$dbh->rollback;
	die $@;
    }

    else
    {
	logMessage(2, "    committing...");
	$dbh->commit;
	logMessage(2, "    done.");
    }
    
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
    
    # $dbh->do("INSERT IGNORE INTO $PHYLOPICS (uid, modified) VALUES ('LAST_FETCH', date_sub(now(), interval 2 minute))");
    # $dbh->do("UPDATE $PHYLOPICS SET modified = date_sub(now(), interval 2 minute) WHERE uid = 'LAST_FETCH'");
    
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
    # 		UPDATE $PHYLOPICS
    # 		SET modified = $modified, credit = $credit, license = $license
    # 		WHERE uid = $uid");
	
    # 	if ( $result =~ /^0/ )
    # 	{
    # 	    $dbh->do("
    # 		REPLACE INTO $PHYLOPICS (uid, modified, credit, license)
    # 		VALUES ($uid, $modified, $credit, $license)");
    # 	}
	
    # 	logMessage(2, "      found image $uid modified $modified");
	
    # 	# Make sure we have an image_no value for this record.
	
    # 	my ($image_no) = $dbh->selectrow_array("
    # 		SELECT image_no FROM $PHYLOPICS WHERE uid = $uid");
	
    # 	next unless $image_no;
	
    # 	# Fetch the binary data for the thumbnail.
	
    # 	my $url = "http://phylopic.org/assets/images/submissions/$r->{uid}.thumb.png";
    # 	my $req = HTTP::Request->new(GET => $url );
	
    # 	my $response = $ua->request($req);
	
    # 	if ( $response->is_success )
    # 	{
    # 	    my $content = $response->content;
	    
    # 	    my $stmt = $dbh->prepare("UPDATE $PHYLOPICS SET thumb = ? WHERE UID = $uid");
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
	    
    # 	    my $stmt = $dbh->prepare("UPDATE $PHYLOPICS SET icon = ? WHERE UID = $uid");
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
    # 		INSERT IGNORE INTO $PHYLOPIC_NAMES (uid, taxon_name, taxon_attr)
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


# selectPics ( dbh )
# 
# Select a single image per taxon.

sub selectPics {
    
    my ($dbh, $tree_table) = @_;
    
    my ($phylopics) = eval {
	local($dbh->{PrintError}) = 0;
	$dbh->selectrow_array("SELECT count(*) from $PHYLOPICS");
    };
    
    unless ( $phylopics > 0 )
    {
	logMessage(2, "    skipping phylopics because table '$PHYLOPICS' does not exist in this database");
	return;
    }
    
    # Now copy all of the new records into the PHYLOPIC_CHOICE table with
    # priority=1.  That priority can be adjusted later.
    
    logMessage(2, "    adding new records to pic choice table...");
    
    my $sql = "
	INSERT IGNORE INTO $PHYLOPIC_CHOICE (orig_no, uid, priority)
	SELECT t.orig_no, pqn.uid, 1
	FROM $tree_table as t JOIN $PHYLOPIC_NAMES as pqn on t.name = pqn.taxon_name";
    
    my $result;
    
    eval {
	$result = $dbh->do($sql);
    };
    
    if ( $@ && $@ =~ /Illegal mix of collations/ )
    {
	$dbh->do("ALTER TABLE $PHYLOPIC_NAMES modify column taxon_name varchar(80) collate utf8_general_ci");
	
	$result = $dbh->do($sql);
    }
    
    logMessage(2, "      added $result records");
    
    logMessage(2, "    selecting the phylopic for each taxon...");
    
    my ($result, $sql);
    
    # Create a working table with which to do our selection.
    
    $dbh->do("DROP TABLE IF EXISTS $TAXON_PICS_WORK");
    
    $dbh->do("CREATE TABLE $TAXON_PICS_WORK (
		orig_no int unsigned primary key,
		image_no int unsigned not null) Engine=InnoDB");
    
    # Select images by priority number, or by earlier modification date
    # otherwise.
    
    $result = $dbh->do("
		INSERT IGNORE INTO $TAXON_PICS_WORK (orig_no, image_no)
		SELECT orig_no, image_no
		FROM $PHYLOPIC_CHOICE join $PHYLOPICS using (uid)
		WHERE priority > 0
		ORDER BY priority desc");
    
    # Activate the new table.
    
    activateTables($dbh, $TAXON_PICS_WORK => $TAXON_PICS);
}


# ensureTables ( dbh )
# 
# If the proper table does not exist, create it.

sub ensureTables {

    my ($dbh, $force) = @_;
    
    my ($sql, $result);
    
    if ( $force )
    {
	$dbh->do("DROP TABLE IF EXISTS $PHYLOPICS");
	$dbh->do("DROP TABLE IF EXISTS $PHYLOPIC_NAMES");
	$dbh->do("DROP TABLE IF EXISTS $PHYLOPIC_CHOICE");
    }
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $PHYLOPICS (
		uid varchar(80) primary key,
		image_no int unsigned auto_increment not null,
		modified datetime,
		credit varchar(255),
		license varchar(255),
		thumb blob,
		icon blob,
		key (image_no)) Engine=InnoDB CHARACTER SET utf8 COLLATE utf8_unicode_ci");
    
    $dbh->do("INSERT INTO $PHYLOPICS (uid, modified)
	      VALUES ('LAST_FETCH', '2001-01-01')") if $force;
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $PHYLOPIC_NAMES (
		uid varchar(80) not null,
		taxon_name varchar(100) not null,
		taxon_attr varchar(100) not null,
		unique key (uid, taxon_name, taxon_attr),
		key (uid),
		key (taxon_name)) Engine=InnoDB CHARACTER SET utf8 COLLATE utf8_unicode_ci");
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $PHYLOPIC_CHOICE (
		orig_no int unsigned not null,
		uid varchar(80) not null,
		priority tinyint not null,
		unique key (orig_no, uid),
		key (uid)) Engine=InnoDB CHARACTER SET utf8 COLLATE utf8_unicode_ci");
    
    my $a = 1;	# we can stop here when debugging
}

1;
