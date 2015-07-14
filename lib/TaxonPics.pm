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
use JSON qw(from_json);

use CoreFunction qw(activateTables);
use TaxonDefs qw(@TREE_TABLE_LIST %TAXON_TABLE);

use ConsoleLog qw(logMessage);

use base 'Exporter';

our (@EXPORT_OK) = qw(getPics selectPics $PHYLOPICS $PHYLOPIC_NAMES $PHYLOPIC_CHOICE $TAXON_PICS);

our ($PHYLOPICS) = 'phylopics';
our ($PHYLOPIC_NAMES) = 'phylopic_names';
our ($PHYLOPIC_CHOICE) = 'phylopic_choice';
our ($TAXON_PICS) = 'taxon_pics';

our ($TAXON_PICS_WORK) = 'tpn';


# getPics ( dbh )
# 
# Fetch any taxon pictures that have been added or updated since the last
# fetch. 

sub getPics {

    my ($dbh, $tree_table, $force) = @_;
    
    my ($list);
    my ($raw_count) = 0;
    
    ensureTables($dbh, $force);
    
    # Figure out the date of the last fetch that we did.  If no images are in
    # the table, use a date that will cause all available images to be fetched.
    
    my ($since_date) = $dbh->selectrow_array("
		SELECT cast(modified as date) FROM $PHYLOPICS
		WHERE uid = 'LAST_FETCH'");
    
    $since_date ||= "2001-01-01";
    
    # List all images modified since that date.
    
    logMessage(2, "    listing new phylopics...");
    
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database/0.1");
    
    my $req = HTTP::Request->new(GET => "http://phylopic.org/api/a/image/list/modified/$since_date?options=taxa+licenseURL+string+modified+credit+citationStart");
    
    my $response = $ua->request($req);
    
    if ( $response->is_success )
    {
	$list = from_json($response->content, { latin1 => 1 });
	$raw_count = scalar(@{$list->{result}}) if ref $list->{result} eq 'ARRAY';
    }
    
    else
    {
	logMessage(2, "      FAILED.");
	return;
    }
    
    # If we get here, then the fetch succeeded.
    
    logMessage(2, "      fetched $raw_count records.");
    
    # Mark the date of last fetch.  We subtract 2 minutes in case a new
    # phylopic came in while we were decoding the JSON response above.
    
    $dbh->do("INSERT IGNORE INTO $PHYLOPICS (uid, modified) VALUES ('LAST_FETCH', date_sub(now(), interval 2 minute))");
    $dbh->do("UPDATE $PHYLOPICS SET modified = date_sub(now(), interval 2 minute) WHERE uid = 'LAST_FETCH'");
    
    # Go through the records, and store one record for each pic and an
    # associated record for each name.
    
    foreach my $r ( @{$list->{result}} )
    {
	next unless ref $r->{taxa} eq 'ARRAY';
	
	my $uid = $dbh->quote($r->{uid});
	my $modified = $dbh->quote($r->{modified});
	my $credit = $dbh->quote($r->{credit});
	my $license = $dbh->quote($r->{licenseURL});
	my $pd = $license =~ /pub/ ? 1 : 0;
	
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
	
	logMessage(2, "      found image $uid modified $modified");
	
	# Make sure we have an image_no value for this record.
	
	my ($image_no) = $dbh->selectrow_array("
		SELECT image_no FROM $PHYLOPICS WHERE uid = $uid");
	
	next unless $image_no;
	
	# Fetch the binary data for the thumbnail.
	
	my $url = "http://phylopic.org/assets/images/submissions/$r->{uid}.thumb.png";
	my $req = HTTP::Request->new(GET => $url );
	
	my $response = $ua->request($req);
	
	if ( $response->is_success )
	{
	    my $content = $response->content;
	    
	    my $stmt = $dbh->prepare("UPDATE $PHYLOPICS SET thumb = ? WHERE UID = $uid");
	    $result = $stmt->execute($response->content);
	}
	
	else
	{
	    logMessage(2, "        thumb FAILED: $url");
	    return;
	}
	
	# Fetch the binary data for the icon.
	
	$url = "http://phylopic.org/assets/images/submissions/$r->{uid}.icon.png";
	$req = HTTP::Request->new(GET => $url );
	
	$response = $ua->request($req);
	
	if ( $response->is_success )
	{
	    my $content = $response->content;
	    
	    my $stmt = $dbh->prepare("UPDATE $PHYLOPICS SET icon = ? WHERE UID = $uid");
	    $result = $stmt->execute($response->content);
	}
	
	else
	{
	    logMessage(2, "        icon FAILED: $url");
	    return;
	}
	
	# Figure out which taxonomic names, if any, this pic is associated
	# with.  Delete all of the existing ones and store a new set.
	
	$result = $dbh->do("DELETE FROM PHYLOPIC_NAMES WHERE uid = $uid");
	
	foreach my $t ( @{$r->{taxa}} )
	{
	    my $name = $t->{canonicalName}{string};
	    my $name_len = $t->{canonicalName}{citationStart};
	    
	    next unless defined $name && $name ne '';
	    
	    # Split off the attribution from the taxonomic name.
	    
	    my ($taxon_name, $taxon_attr);
	    
	    if ( $name_len > 0 )
	    {
		$taxon_name = $dbh->quote(substr($name, 0, $name_len - 1));
		$taxon_attr = $dbh->quote(substr($name, $name_len));
	    }
	    
	    else
	    {
		$taxon_name = $dbh->quote($name);
		$taxon_attr = "''";
	    }
	    
	    $result = $dbh->do("
		INSERT IGNORE INTO $PHYLOPIC_NAMES (uid, taxon_name, taxon_attr)
		VALUES ($uid, $taxon_name, $taxon_attr)");
	}
    }
    
    my $a = 1;	# we can stop here when debugging
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
		image_no int unsigned not null) Engine=MyISAM");
    
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
		key (image_no)) Engine=MyISAM CHARACTER SET utf8 COLLATE utf8_unicode_ci");
    
    $dbh->do("INSERT INTO $PHYLOPICS (uid, modified)
	      VALUES ('LAST_FETCH', '2001-01-01')") if $force;
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $PHYLOPIC_NAMES (
		uid varchar(80) not null,
		taxon_name varchar(100) not null,
		taxon_attr varchar(100) not null,
		unique key (uid, taxon_name, taxon_attr),
		key (uid),
		key (taxon_name)) Engine=MyISAM CHARACTER SET utf8 COLLATE utf8_unicode_ci");
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $PHYLOPIC_CHOICE (
		orig_no int unsigned not null,
		uid varchar(80) not null,
		priority tinyint not null,
		unique key (orig_no, uid),
		key (uid)) Engine=MyISAM CHARACTER SET utf8 COLLATE utf8_unicode_ci");
    
    my $a = 1;	# we can stop here when debugging
}
