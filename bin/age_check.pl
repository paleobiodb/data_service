#!/usr/bin/env perl



use strict;

use feature 'say';
use lib 'lib';

use TableDefs qw(%TABLE);
use CoreTableDefs;
use CoreFunction qw(connectDB);
use LWP::UserAgent;





my $command = shift @ARGV;
my $argument = shift @ARGV;
my $table;

if ( $argument eq 'genera' )
{
    $table = 'age_check_genera';
    $argument = shift @ARGV;
}

elsif ( $argument eq 'species' )
{
    $table = 'age_check_species';
    $argument = shift @ARGV;
}

else
{
    die "You must specify either 'genera' or 'species'\n";
}

if ( $command eq 'fetch' && $argument )
{
    &FetchFromSource($argument);
}

elsif ( $command eq 'fill' && $argument )
{
    &FillFromSource($argument);
}

elsif ( $command eq 'clear' && $argument )
{
    &ClearData($argument);
}

elsif ( $command eq 'add' && $argument eq 'pbdb' )
{
    &AddPBDBOccs;
}

elsif ( $command eq 'add' && $argument eq 'flags' )
{
    &AddFlags;
}

elsif ( $command eq 'write' && $argument )
{
    &WriteTable($argument);
}

elsif ( $command )
{
    die "Invalid command '$command'\n";
}

else
{
    die "You must specify a subcommand\n";
}



sub FetchFromSource {
    
    my ($taxgroup) = @_;
    
    # Get a database handle.
    
    my $dbh = connectDB("config.yml", 'pbdb');
    
    die "Could not connect to database: $!\n" unless $dbh;
    
    # Get a user agent for making web queries.
    
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database Updater/0.1");
    
    my $taxon_id;
    
    # If we were given a taxon_id value instead of a taxon_group value, grab the
    # taxonomic group.
    
    if ( $taxgroup =~ /^\d+$/ )
    {
	$taxon_id = $taxgroup;
	($taxgroup) = $dbh->selectrow_array("SELECT taxon_group FROM $table WHERE taxon_id=$taxon_id");
    }
    
    # Figure out the URL template for the taxonomic group we are checking.
    
    my ($source_url, $alt_url);
    
    if ( $taxgroup eq 'radiolarians' )
    {
	$source_url = 'https://www.mikrotax.org/radiolaria/index.php?taxon=%%&module=rads_cenozoic';
	$alt_url = 'https://www.mikrotax.org/radiolaria/index.php?taxon=%%&module=rads_mesozoic';
    }
    
    elsif ( $taxgroup eq 'planktic_forams' )
    {
	$source_url = 'https://www.mikrotax.org/system/index.php?taxon=%%&module=pf_cenozoic';
	$alt_url = 'https://www.mikrotax.org/system/index.php?taxon=%%&module=pf_mesozoic';
    }
    
    elsif ( $taxgroup eq 'benthic_forams' )
    {
	$source_url = 'https://www.mikrotax.org/bforams/index.php?taxon=%%&module=bf_main';
	$alt_url = '';
    }
    
    elsif ( $taxgroup eq 'nannofossils' )
    {
	$source_url = 'https://www.mikrotax.org/Nannotax3/index.php?taxon=%%&module=ntax_cenozoic';
	$alt_url = 'https://www.mikrotax.org/Nannotax3/index.php?taxon=%%&module=ntax_mesozoic';
    }
    
    else
    {
	die "You must add a source url for '$taxgroup'\n";
    }
    
    # Get all names from the specified taxonomic group, or else a single name by taxon_id
    
    my $sql;
    
    if ( $taxon_id )
    {
	$sql = "SELECT taxon_id, taxon_name FROM $table WHERE taxon_id=$taxon_id";
    }
    
    else
    {
	$sql = "SELECT taxon_id, taxon_name FROM $table WHERE taxon_group='$taxgroup'
		and source is null";
    }
    
    my $result = $dbh->selectall_arrayref($sql);
    
    foreach my $r ( @$result )
    {
	my ($taxon_no, $taxon_name) = @$r;
	
	# Make a request using the template $source_url.
	
	my $url = $source_url;
	$url =~ s/%%/$taxon_name/;
	
	say "Requesting: $url";
	
	my $req = HTTP::Request->new(GET => $url);
	my $resp = $ua->request($req);
	my $content_ref = $resp->content_ref;
	
	# If we've used the wrong "module", try the other one.
	
	if ( $$content_ref !~ m{</body>} && $alt_url )
	{
	    $url = $alt_url;
	    $url =~ s/%%/$taxon_name/;
	    
	    say "Requesting alt: $url";
	    
	    $req = HTTP::Request->new(GET => $url);
	    $resp = $ua->request($req);
	    $content_ref = $resp->content_ref;
	}
	
	else
	{
	    my $a = 1; # we can stop here when debugging
	}
	
	# Extract the minimum and maximum ages from the response.
	
	my ($min_age, $max_age, $extract);
	
	if ( $$content_ref =~ / Last \s occurrence (.*?) <br> /xi )
	{
	    $extract = $1;
	    
	    if ( $extract =~ /Extant|[(]-Ma/ )
	    {
		$min_age = '0.0';
	    }
	    
	    elsif ( $extract =~ / (\d[\d.]+) - \d[\d.]+ Ma /xi )
	    {
		$min_age = $1;
	    }
	    
	    elsif ( $extract =~ / \s(\d[\d.]+) Ma /xi )
	    {
		$min_age = $1;
	    }
	    
	    if ( defined $min_age )
	    {
		say "Found min_age: $min_age";
	    }
	}
	
	if ( $$content_ref =~ / <em> First \s occurrence (.*?) <br> /xi )
	{
	    $extract = $1;
	    
	    if ( $extract =~ / no \s known \s fossil \s record /xi )
	    {
		$max_age = '0.0110';
	    }
	    
	    elsif ( $extract =~ / \d[\d.]+ - (\d[\d.]+) Ma/xi )
	    {
		$max_age = $1;
	    }
	    
	    elsif ( $extract =~ / \s(\d[\d.]+) Ma/xi )
	    {
		$max_age = $1;
	    }
	    
	    if ( defined $max_age )
	    {
		say "Found max_age: $max_age";
	    }
	}
	
	# If the max_age is close to the beginning of the cenozoic, and we
	# fetched the cenozoic information on this taxon, then try the mesozoic
	# module as well. If that reports a greater max age, use that.
	
	if ( defined $max_age && $max_age > 63.0 && $max_age < 67.0 && $alt_url &&
	     $url ne $alt_url )
	{
	    my $url = $alt_url;
	    $url =~ s/%%/$taxon_name/;
	    
	    say "Requesting additionally: $url";
	    
	    my $req = HTTP::Request->new(GET => $url);
	    my $resp = $ua->request($req);
	    my $content_ref = $resp->content_ref;
	    
	    if ( $$content_ref =~ / First \s occurrence .*? \d[\d.]+ - (\d[\d.]+) Ma/xi )
	    {
		$max_age = $1;
		say "Found max_age: $max_age";
	    }
	    
	    elsif ( $$content_ref =~ / First \s occurrence .*? \s(\d[\d.]+) Ma/xi )
	    {
		$max_age = $1;
		say "Found max_age: $max_age";
	    }
	}
	
	if ( defined $min_age || defined $max_age )
	{
	    my $qtax = $dbh->quote($taxon_no);
	    my $qmin = $dbh->quote($min_age);
	    my $qmax = $dbh->quote($max_age);
	    my $qurl = $dbh->quote($url);
	    
	    $sql = "UPDATE $table SET source_min = $qmin, source_max = $qmax, source = $qurl
		WHERE taxon_id = $qtax";
	    
	    my $count = $dbh->do($sql);
	    
	    if ( $count > 0 )
	    {
		say "Updated $count rows";
	    }
	}
    }
}


sub AddPBDBOccs {
    
    # Get a database handle.
    
    my $dbh = connectDB("config.yml", 'pbdb');
    
    die "Could not connect to database: $!\n" unless $dbh;
    
    # Add pbdb occurrence counts and max and min ages to the table.
    
    my $sql = "update $table as k join (select k.taxon_no, count(*) as n_occs, min(c.late_age) as min_age, max(c.early_age) as max_age from $table as k join authorities as base using (taxon_no) join taxon_trees as t1 using (orig_no) join taxon_trees as t2 on t2.lft between t1.lft and t1.rgt join occ_matrix as o on o.orig_no = t2.orig_no join coll_matrix as c using (collection_no) join collections as cc using (collection_no) where k.taxon_no > 0 and latest_ident and (research_group <> 'eODP' or research_group is null) group by taxon_no) as p using (taxon_no) set k.pbdb_occs = p.n_occs, k.pbdb_min = p.min_age, k.pbdb_max = p.max_age";
    
    my $count = $dbh->do($sql);
    
    say "Updated $count rows";
}


sub AddFlags {
    
    # Get a database handle.
    
    my $dbh = connectDB("config.yml", 'pbdb');
    
    die "Could not connect to database: $!\n" unless $dbh;
    
    # Clear all flags
    
    my $sql = "UPDATE $table SET f0='', f1='', f2='', f3=''";
    
    my $result = $dbh->do($sql);
    
    say "Cleared flags from $result rows";
    
    # Set flags according to the min and max ages from the eodp dataset.
    
    $sql = "UPDATE $table SET f0='*' WHERE eodp_min < source_min - 1";
    
    $result = $dbh->do($sql);
    
    say "Set f0 to '*' on $result rows";
        
    $sql = "UPDATE $table SET f0='**' WHERE eodp_min < source_min - 10";
    
    $result = $dbh->do($sql);
    
    say "Set f0 to '**' on $result rows";
    
    $sql = "UPDATE $table SET f1='*' WHERE eodp_max > source_max + 1";
    
    $result = $dbh->do($sql);
    
    say "Set f1 to '*' on $result rows";
        
    $sql = "UPDATE $table SET f1='**' WHERE eodp_max > source_max + 10 or eodp_max < source_min";
    
    $result = $dbh->do($sql);
    
    say "Set f1 to '**' on $result rows";
    
    $sql = "UPDATE $table SET f1='' WHERE eodp_max > 66 and taxon_group = 'radiolarians'";
    
    $result = $dbh->do($sql);
    
    say "Cleared f1 on $result rows (mesozoic radiolarians)";
    
    # Set flags according to the min and max ages from the pbdb dataset.
    
    $sql = "UPDATE $table SET f2='*' WHERE pbdb_min < source_min - 1";
    
    $result = $dbh->do($sql);
    
    say "Set f2 to '*' on $result rows";
        
    $sql = "UPDATE $table SET f2='**' WHERE pbdb_min < source_min - 10";
    
    $result = $dbh->do($sql);
    
    say "Set f2 to '**' on $result rows";
    
    $sql = "UPDATE $table SET f3='*' WHERE pbdb_max > source_max + 1";
    
    $result = $dbh->do($sql);
    
    say "Set f3 to '*' on $result rows";
        
    $sql = "UPDATE $table SET f3='**' WHERE pbdb_max > source_max + 10 or pbdb_max < source_min";
    
    $result = $dbh->do($sql);
    
    say "Set f3 to '**' on $result rows";
    
    $sql = "UPDATE $table SET f3='' WHERE pbdb_max > 66 and taxon_group = 'radiolarians'";
    
    $result = $dbh->do($sql);
    
    say "Cleared f3 on $result rows (mesozoic radiolarians)";
}


sub ClearData {
    
    my ($which) = @_;
    
    # Get a database handle.
    
    my $dbh = connectDB("config.yml", 'pbdb');
    
    die "Could not connect to database: $!\n" unless $dbh;
    
    # Clear the specified type of data.
    
    my $sql;
    
    if ( $which eq 'pbdb' )
    {
	$sql = "UPDATE $table SET pbdb_occs = null, pbdb_min = null, pbdb_max = null";
    }
    
    elsif ( $which eq 'source' )
    {
	$sql = "UPDATE $table SET source_min = null, source_max = null, source = null";
    }
    
    else
    {
	die "Invalid data type '$which'";
    }
    
    my $result = $dbh->do($sql);
    
    say "Updated $result rows";
}


sub WriteTable {
    
    my ($filename) = @_;
    
    # Get a database handle.
    
    my $dbh = connectDB("config.yml", 'pbdb');
    
    die "Could not connect to database: $!\n" unless $dbh;
    
    # Write the table.
    
    my $sql = "SELECT * INTO OUTFILE '/var/log/mysql/$filename' fields terminated by ',' optionally enclosed by '\"' lines terminated by '\n' FROM $table WHERE source is not null";
    
    my $result = $dbh->do($sql);
    
    say "Wrote $result rows to $filename";
}


sub FillFromSource {
    
    my ($taxgroup) = @_;
    
    # Get a database handle.
    
    my $dbh = connectDB("config.yml", 'pbdb');
    
    die "Could not connect to database: $!\n" unless $dbh;
    
    # Get a user agent for making web queries.
    
    my $ua = LWP::UserAgent->new();
    $ua->agent("Paleobiology Database Updater/0.1");
    
    # Figure out the URL template for the taxonomic group we are checking.
    
    my ($source_url, $alt_url);
    
    if ( $taxgroup eq 'radiolarians' )
    {
	$source_url = 'https://www.mikrotax.org/radiolaria/index.php?taxon=%%&module=rads_cenozoic';
	$alt_url = 'https://www.mikrotax.org/radiolaria/index.php?taxon=%%&module=rads_mesozoic';
    }
    
    elsif ( $taxgroup eq 'planktic_forams' )
    {
	$source_url = 'https://www.mikrotax.org/system/index.php?taxon=%%&module=pf_cenozoic';
	$alt_url = 'https://www.mikrotax.org/system/index.php?taxon=%%&module=pf_mesozoic';
    }
    
    elsif ( $taxgroup eq 'benthic_forams' )
    {
	return;
    }
    
    elsif ( $taxgroup eq 'nannofossils' )
    {
	$source_url = 'https://www.mikrotax.org/Nannotax3/index.php?taxon=%%&module=ntax_cenozoic';
	$alt_url = 'https://www.mikrotax.org/Nannotax3/index.php?taxon=%%&module=ntax_mesozoic';
    }
    
    else
    {
	die "You must add a source url for '$taxgroup'\n";
    }
    
    # Get all species names from that taxonomic group where the earliest
    # appearance is near the beginning of the Cenozoic.
    
    my $sql = "SELECT taxon_id, taxon_name FROM $table WHERE taxon_group='$taxgroup'
		and source_max between 62.0 and 67.0";
    
    my $result = $dbh->selectall_arrayref($sql);
    
    foreach my $r ( @$result )
    {
	my ($taxon_no, $taxon_name) = @$r;
	
	# Make a request using the template $alt_url, in case this taxon shows
	# up in the Mesozoic as well.
	
	my $url = $alt_url;
	$url =~ s/%%/$taxon_name/;
	
	say "Requesting: $url";
	
	my $req = HTTP::Request->new(GET => $url);
	my $resp = $ua->request($req);
	my $content_ref = $resp->content_ref;
	
	my ($min_age, $max_age);
	
	if ( $$content_ref =~ / Last \s occurrence .*? (\d[\d.]+) - (\d[\d.]+) Ma /xi )
	{
	    $min_age = $1;
	    say "Found min_age: $min_age";
	}
	
	elsif ( $$content_ref =~ / Last \s occurrence .*? <b>Extant /xi )
	{
	    $min_age = '0.0';
	    say "Found min_age: $min_age";
	}
	
	elsif ( $$content_ref =~ / Last \s occurrence .*? \s(\d[\d.]+) Ma /xi )
	{
	    $min_age = $1;
	    say "Found min_age: $min_age";
	}
	
	if ( $$content_ref =~ / First \s occurrence .*? \d[\d.]+ - (\d[\d.]+) Ma/xi )
	{
	    $max_age = $1;
	    say "Found max_age: $max_age";
	}
	
	elsif ( $$content_ref =~ / First \s occurrence .*? \s(\d[\d.]+) Ma/xi )
	{
	    $max_age = $1;
	    say "Found max_age: $max_age";
	}
	
	elsif ( $$content_ref =~ / First \s occurrence .* no \s known \s fossil \s record /xi )
	{
	    $max_age = '0.0110';
	    say "Found max_age: $max_age";
	}
	
	if ( defined $max_age )
	{
	    my $qtax = $dbh->quote($taxon_no);
	    my $qmax = $dbh->quote($max_age);
	    my $qurl = $dbh->quote($url);
	    
	    $sql = "UPDATE $table SET source_max = $qmax WHERE taxon_id = $qtax";
	    
	    my $count = $dbh->do($sql);
	    
	    if ( $count > 0 )
	    {
		say "Updated $count rows";
	    }
	}
    }
}

