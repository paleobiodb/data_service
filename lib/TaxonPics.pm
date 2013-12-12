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

use TaxonDefs qw(@TREE_TABLE_LIST %TAXON_TABLE $CLASSIC_TREE_CACHE $CLASSIC_LIST_CACHE);

use ConsoleLog qw(initMessages logMessage);

use base 'Exporter';

our (@EXPORT_OK) = qw(getPics $TAXON_IMAGES);


our ($TAXON_IMAGES) = 'taxon_images';


# getPics ( dbh )
# 
# Fetch any taxon pictures that have been added or updated since the last
# fetch. 

sub getPics {

    my ($dbh, $tree_table, $force) = @_;
    
    my ($list);
    my ($raw_count) = 0;
    
    ensureTable($dbh, $force);
    
    # Figure out the date of the last fetch that we did.  If no images are in
    # the table, use a date that will cause all available images to be fetched.
    
    my ($since_date) = $dbh->selectrow_array("
		SELECT cast(modified as date) FROM $TAXON_IMAGES
		WHERE uid = 'LAST_FETCH'");
    
    $since_date ||= "2013-12-01";
    
    # List all images modified since that date.
    
    logMessage(2, "    listing new images...");
    
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
    
    return unless $raw_count > 0;
    
    # Mark the date of last fetch.
    
    $dbh->do("REPLACE INTO $TAXON_IMAGES (uid, modified) VALUES ('LAST_FETCH', now())");
    
    # Go through the records, rejecting those which are not in the public
    # domain, or which do not have any associated taxa.  Also ignore any that
    # are below the family level.
    
    my (@fetch_list);
    
    foreach my $r ( @{$list->{result}} )
    {
	next unless $r->{licenseURL} =~ qr{publicdomain|licenses/by/};
	next unless ref $r->{taxa} eq 'ARRAY';
	
	my $uid = $dbh->quote($r->{uid});
	my $modified = $dbh->quote($r->{modified});
	my $credit = $dbh->quote($r->{credit});
	my $license = $dbh->quote($r->{licenseURL});
	my $pd = $license =~ /pub/ ? 1 : 0;
	
	my @names;
	
	foreach my $t ( @{$r->{taxa}} )
	{
	    my $name = $t->{canonicalName}{string};
	    
	    # Ignore all but the first word of each name, since we are not
	    # interested in individual species and also need to chop off the
	    # attribution.
	    
	    if ( $name =~ /^([^ ]+)/ )
	    {
		push @names, "'$1'";
	    }
	}
	
	my $name_string = join(q{,}, @names);
	
	my $result = $dbh->selectall_arrayref("
		SELECT orig_no, taxon_rank, taxon_name FROM authorities
		WHERE taxon_name in ($name_string) and taxon_rank >= 9
		GROUP BY orig_no", { Slice => {} });
	
	next unless ref $result eq 'ARRAY';
	
	foreach my $p ( @$result )
	{
	    my $orig_no = $p->{orig_no};
	    my $name = $p->{taxon_name};
	    my $rank = $p->{taxon_rank};
	    
	    logMessage(2, "      found pic for $rank $name ($orig_no)");
	    
	    # Create a new record, or update the existing one.
	    
	    $result = $dbh->do("
		REPLACE INTO $TAXON_IMAGES (uid, orig_no, modified, credit, license, pd)
		VALUES ($uid, $orig_no, $modified, $credit, $license, $pd)");
	    
	    # Fetch the binary data for the thumbnail.
	    
	    my $url = "http://phylopic.org/assets/images/submissions/$r->{uid}.thumb.png";
	    my $req = HTTP::Request->new(GET => $url );
	    
	    my $response = $ua->request($req);
	    
	    if ( $response->is_success )
	    {
		my $content = $response->content;
		
		my $stmt = $dbh->prepare("UPDATE $TAXON_IMAGES SET thumb = ? WHERE UID = $uid");
		$result = $stmt->execute($response->content);
		logMessage(2, "        set thumb.");
	    }
	    
	    else
	    {
		logMessage(2, "        thumb FAILED: $url");
		return;
	    }
	    
	    # Fetch the binary data for the icon.
	    
	    my $url = "http://phylopic.org/assets/images/submissions/$r->{uid}.icon.png";
	    my $req = HTTP::Request->new(GET => $url );
	    
	    my $response = $ua->request($req);
	    
	    if ( $response->is_success )
	    {
		my $content = $response->content;
		
		my $stmt = $dbh->prepare("UPDATE $TAXON_IMAGES SET icon = ? WHERE UID = $uid");
		$result = $stmt->execute($response->content);
		logMessage(2, "        set icon.");
	    }
	    
	    else
	    {
		logMessage(2, "        icon FAILED: $url");
		return;
	    }
	}
    }
}


# ensureTable ( dbh )
# 
# If the proper table does not exist, create it.

sub ensureTable {

    my ($dbh, $force) = @_;
    
    my ($sql, $result);
    
    if ( $force )
    {
	$dbh->do("DROP TABLE IF EXISTS $TAXON_IMAGES");
    }
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $TAXON_IMAGES (
		uid varchar(80) primary key,
		orig_no int unsigned not null,
		priority tinyint not null,
		modified datetime,
		credit varchar(255),
		license varchar(255),
		pd boolean,
		thumb blob,
		icon blob) Engine=MyISAM");
    
    my $a = 1;	# we can stop here when debugging
}
