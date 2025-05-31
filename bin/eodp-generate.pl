#!/usr/bin/env perl

use strict;

use lib 'lib';
use utf8;

use CoreFunction qw(loadConfig configData connectDB);
use TableDefs qw(%TABLE);
use CoreTableDefs;
use Carp qw(carp croak);

use Getopt::Long qw(:config bundling no_auto_abbrev permute);
use YAML;
use feature 'say';

# use Carp qw(croak);
# use List::Util qw(any max min);



my ($opt_quiet, $opt_verbose,  $opt_force, $opt_debug, $opt_help);
my ($opt_config);

GetOptions("quiet|q" => \$opt_quiet,
	   "verbose|v" => \$opt_verbose,
	   "config|f" => \$opt_config,
	   "force" => \$opt_force,
	   "help|h" => \$opt_help,
	   "debug|D" => \$opt_debug) or die;

our ($mstr, $pbdb, $EXECUTE_MODE);

loadConfig($opt_config);

my $dbconf = configData('Database');

if ( $ENV{PWD} ne '/var/paleomacro/pbapi' )
{
    $dbconf->{host} = '127.0.0.1';
}

$mstr = connectDB($opt_config, 'macrostrat');
$pbdb = connectDB($opt_config, 'pbdb');

die "Could not connect to database: $DBI::errstr" unless $mstr && $pbdb;

my ($occs) = DBRowQuery($mstr, "SELECT count(*) from offshore_occs");

say "Found $occs occurrences in table `offshore_occs`.";

if ( $ARGV[0] eq 'check' )
{
    AdjustTable();
    exit;
}

elsif ( $ARGV[0] eq 'adjust' )
{
    $EXECUTE_MODE = 1;
    AdjustTable();
    exit;
}

elsif ( $ARGV[0] eq 'checkunits' )
{
    MatchUnits();
    exit;
}

elsif ( $ARGV[0] eq 'units' )
{
    $EXECUTE_MODE = 1;
    MatchUnits();
    exit;
}

elsif ( $ARGV[0] eq 'compare' )
{
    $EXECUTE_MODE = 1;
    CompareLatLng();
    exit;
}

# elsif ( $ARGV[0] eq 'constraints' )
# {
#     if ( $ARGV[1] eq 'init' )
#     {
# 	$EXECUTE_MODE = 1;
# 	InitConstraints();
# 	exit;
#     }
    
#     elsif ( $ARGV[1] eq 'clear' )
#     {
# 	$EXECUTE_MODE = 1;
# 	ClearConstraints();
# 	exit;
#     }
    
#     else
#     {
# 	say "You must specify either 'add' or 'remove'";
#     }
# }

elsif ( $ARGV[0] eq 'insert' )
{
    $EXECUTE_MODE = 1;
    InsertData();
    exit;
}

elsif ( $ARGV[0] eq 'remove' )
{
    $EXECUTE_MODE = 1;
    RemoveData();
    exit;
}

else
{
    say "You must specify a valid subcommand.";
}

exit;



# AdjustTable ( )
# 
# Either check or alter the structure and contents of the tables `offshore_collections`
# and `offshore_occs` to prepare them for ingestion. If the global variable $EXECUTE_MODE
# has a true value, actually make the changes. Otherwise, just print out what we would do.

sub AdjustTable {
    
    say "Checking structure of table `offshore_collections`...";

    my $has_country = 1;
    
    # Step I: check/update the structure of `offshore_collections`.
    
    # Add the column `country`, which will be set to indicate in which ocean basin each
    # collection is located.
    
    my $check = DBTextQuery($mstr, "SHOW COLUMNS FROM offshore_collections LIKE 'country'");
    
    unless ( $check =~ /country/ )
    {
	DBCommand($mstr, "ALTER TABLE offshore_collections ADD COLUMN IF NOT EXISTS `country` \
	varchar(255) not null default ''");
	$has_country = 0;
    }
    
    # Step II: check/update the contents of `offshore_collections`.
    
    say "Checking contents of table `offshore_collections`...";
    
    # If all records have 0 in the column `taxa_count`, set that field by joining with 
    # `offshore_occs`.
    
    my ($tcount) = DBRowQuery($mstr, "SELECT count(*) FROM offshore_collections \
	WHERE taxa_count > 0");
    
    unless ( $tcount > 0 )
    {
	DBCommand($mstr, "UPDATE offshore_collections as oc JOIN \
	(SELECT sample_id, count(*) as count FROM offshore_occs GROUP BY sample_id) as oo \
		ON oc.id = oo.sample_id \
	SET oc.taxa_count = oo.count");
    }
    
    # If any records have an empty value for country, set that now based on the latitude
    # and longitude of each collection.
    
    my ($ccount) = $has_country && DBRowQuery($mstr, "SELECT count(*) FROM offshore_collections WHERE country=''");
    
    if ( $ccount > 0 )
    {
	DBCommand($mstr, "UPDATE offshore_collections as c JOIN cols on col_id = cols.id \
	SET c.country = 'Southern Ocean' WHERE country = '' and cols.lat <= -60");
	
	DBCommand($mstr, "UPDATE offshore_collections as c JOIN cols on col_id = cols.id \
	SET c.country = 'Arctic Ocean' WHERE country = '' and cols.lat >= 65");
	
	DBCommand($mstr, "UPDATE offshore_collections as c JOIN cols on col_id = cols.id \
	SET c.country = 'North Atlantic' WHERE country = '' and cols.lat > 0 and \
		cols.lng < 25 and (cols.lat >= 18 and cols.lng >= -98 or \
		cols.lat < 18 and cols.lat > (cols.lng * -0.5625 - 37.125))");
	
	DBCommand($mstr, "UPDATE offshore_collections as c JOIN cols on col_id = cols.id \
	SET c.country = 'South Atlantic' WHERE country = '' and cols.lat <= 0 and \
		cols.lng < 25 and cols.lng >= -66");
	
	DBCommand($mstr, "UPDATE offshore_collections as c JOIN cols on col_id = cols.id \
	SET c.country = 'North Pacific' WHERE country = '' and cols.lat > 0 and \
		(cols.lng > 104 or (cols.lat >= 18 and cols.lng < -98 or \
		cols.lat < 18 and cols.lat < (cols.lng * -0.5625 - 37.125)))");
	
	DBCommand($mstr, "UPDATE offshore_collections as c JOIN cols on col_id = cols.id \
	SET c.country = 'South Pacific' WHERE country = '' and cols.lat <= 0 and \
		(cols.lng <= -98 or (cols.lat < -20 and cols.lng > 145) or \
		(cols.lat >= -20 and cols.lat > (cols.lng * -0.357 + 31.765)))");
	
	DBCommand($mstr, "UPDATE offshore_collections as c JOIN cols on col_id = cols.id \
	SEt c.country = 'Indian Ocean' WHERE country = '' and cols.lng >= 25 and \
		(cols.lat < 19 and cols.lng < 80 or cols.lat < 23 and cols.lng >= 80) and \
		(cols.lng < 103 or cols.lat < -20 and cols.lng <= 145 or \
		 cols.lat >= -20 and cols.lat <= (cols.lng * -0.357 + 31.765))");
	
	($ccount) = DBRowQuery($mstr, "SELECT count(*) FROM offshore_collections WHERE country=''");
	
	if ( $ccount > 0 )
	{
	    say "  There are still $ccount collections with no country name.";
	}
    }
    
    # Step III: check/update the structure of `offshore_occs`.
    
    say "Checking structure of table `offshore_occs`...";
    
    # If there is a column named `subgenera_name`, rename it to `subgenus_name`.
    
    $check = DBTextQuery($mstr, "SHOW COLUMNS FROM offshore_occs LIKE 'subgenera_name'");
    
    if ( $check !~ /subenera_name/ )
    {
	DBCommand($mstr, "ALTER TABLE offshore_occs CHANGE `subgenera_name` `subgenus_name` \
	varchar(100) NOT NULL default ''");
    }
    
    # Step IV: check/update the contents of `offshore_occs`.
    
    say "Checking contents of table `offshore_occs`...";
    
    # Fix occurrences where the genus is empty.
    
    my ($count) = DBRowQuery($mstr, "SELECT count(*) FROM offshore_occs WHERE genus_name = ''");
    
    if ( $count )
    {
	say "There are $count rows with genus_name = ''";
	
	# If the taxonomic name is 'xxx indet.', set the genus to 'xxx'.
	
	DBCommand($mstr, "UPDATE offshore_occs SET genus_name=regexp_substr(name, '^\\w+'), \
	species_name='indet.' WHERE name rlike '^\\w+ indet.' and genus_name = ''");
	
	# If the taxonomic name starts with '"xxx"', set the genus name to 'xxx' and the
	# genus modifier to 'informal'. I (MM) have determined that the modifier
	# 'informal' more closely matches how quotes are used in this dataset than the
	# modifier '"'.
	
	DBCommand($mstr, "UPDATE offshore_occs SET genus_name=regexp_substr(name, '(?<=\").*?(?=\")'), \
	genus_modifier='informal' WHERE name rlike '^\"' and genus_name = ''");
	
	# Do the same for some occurrences where the taxonomic name is '"Forma T"'.
	
	DBCommand($mstr, "UPDATE offshore_occs SET genus_name = 'Forma T', genus_modifier = 'informal' \
	WHERE name = '\"Forma T\"'");
	
	# Verify that there are now 0 rows with an empty genus name.
	
	my ($newcount) = DBRowQuery($mstr, "SELECT count(*) FROM offshore_occs WHERE genus_name = ''");
	
	say "There are now $newcount rows with genus_name = ''";
    }
    
    # Fix abbreviated species and subspecies modifiers.
    
    my ($varcount) = DBRowQuery($mstr, "SELECT count(*) FROM offshore_occs \
	WHERE subspecies_modifier in ('f.')");
    
    if ( $varcount )
    {
	say "There are $varcount rows with subspecies modifier 'f.'";
	
	# DBCommand($mstr, "UPDATE offshore_occs \
	# SET comments = concat(subspecies_modifier, ' ', subspecies_name, '; ', comments), \
	#     subspecies_modifier = '', \
	#     subspecies_name = '' \
	# WHERE subspecies_modifier in ('var.', 'morph', 'f.')");
	
	# DBCommand($mstr, "UPDATE offshore_occs SET subspecies_modifier = 'var' \
	# WHERE subspecies_modifier = 'var.'");
	
	DBCommand($mstr, "UPDATE offshore_occs SET subspecies_modifier = 'forma' \
	WHERE subspecies_modifier = 'f.'");
    }
    
    my ($scount) = DBRowQuery($mstr, "SELECT count(*) FROM offshore_occs \
	WHERE species_modifier in ('s.s.', 's.l.')");
    
    if ( $scount )
    {
	say "There are $scount rows with species modifiers 's.s.', 's.l.'";
	
	DBCommand($mstr, "UPDATE offshore_occs SET species_modifier = 'sensu lato' \
	WHERE species_modifier = 's.l.'");
	
	DBCommand($mstr, "UPDATE offshore_occs SET species_modifier = 'sensu stricto' \
	WHERE species_modifier = 's.s.'");
    }
    
    # Step V: check for the table `macrostrat_colls` and create it if it doesn't already exist.
    
    my ($ucheck) = DBRowQuery($pbdb, "SHOW TABLES LIKE 'macrostrat_colls'");
    
    unless ( $ucheck =~ /macrostrat_colls/ )
    {
	DBCommand($pbdb, "CREATE TABLE IF NOT EXISTS `macrostrat_colls` (
		`collection_no` int unsigned not null,
		`column_id` int unsigned not null default '0',
		`unit_id` int unsigned not null default '0'
		PRIMARY KEY (`collection_no`, `column_id`, `unit_id`)) Engine=InnoDB");
    }
}


sub MatchUnits {

    # First, look for collections where the top_depth and bottom_depth are identical and which
    # have no matching unit but are located exactly at the top of an existing unit in the
    # same column. These should be matched to the existing unit.
    
    my ($mcount) = DBRowQuery($mstr, "SELECT count(*) FROM offshore_collections as c 
			JOIN units as u on u.col_id = c.col_id 
			     and u.position_top = c.top_depth and u.position_top = c.bottom_depth
			WHERE c.unit_id = 0");
    
    if ( $mcount > 0 )
    {
	say "Matching collections with top_depth = bottom_depth to their units...";
	
	DBCommand($mstr, "UPDATE offshore_collections as c 
			      JOIN units as u on u.col_id = c.col_id 
			      and u.position_top = c.top_depth and u.position_top = c.bottom_depth
			SET c.unit_id = u.id
			WHERE c.unit_id = 0");
    }
    
    # Then look for other collections with unit_id = 0.
    
    my ($ucount) = DBRowQuery($mstr, "SELECT count(*) from offshore_collections as c
				WHERE unit_id = 0");
    
    if ( $ucount )
    {
	say "Matching other collections with missing units...";

	my @columns = DBColumnQuery($mstr, "SELECT distinct col_id from offshore_collections
		      WHERE unit_id = 0");

	my $colstring = join("','", @columns);

	my $colsects = DBArrayQuery($mstr, "SELECT * FROM offshore_colsects
		       WHERE col_id in ('$colstring')");
	
	my $current_col = 0;
	my $updated = 0;
	my $skipped = 0;
	my (%core_unit, %core_lith);
	
	foreach my $r ( @$colsects )
	{
	    if ( $r->{col_id} != $current_col )
	    {
		$current_col = $r->{col_id};
		%core_unit = ();
		%core_lith = ();
	    }

	    my $collection_id = $r->{collection_id};
	    my $core_no = $r->{core};
	    my $unit_id = $r->{unit_id};
	    my $lith = $r->{pbdb_lith};

	    if ( $unit_id == 0 )
	    {
		my @prev_liths = $core_lith{$core_no} ? keys $core_lith{$core_no}->%* : ();
		my $prev_unit = $core_unit{$core_no};
		
		if ( $prev_unit && @prev_liths == 1 )
		{
		    my $result = DBCommand($mstr, "UPDATE offshore_collections SET unit_id = '$prev_unit'
		    WHERE id = '$collection_id'");
		    
		    $updated += $result;
		}
		
		elsif ( $prev_unit )
		{
		    say "Could not update collection $collection_id in column $current_col: multiple lithologies";
		    $skipped++;
		}
		
		else
		{
		    say "Could not update collection $collection_id in column $current_col: no previous unit";
		    $skipped++;
		}
	    }
	    
	    else
	    {
		$core_unit{$core_no} = $unit_id;
		$core_lith{$core_no}{$lith} = 1;
	    }
	}
	
	say "\nUpdated $updated collections.\nSkipped $skipped collections.\n";
    }

    else
    {
	say "\nNothing to update.\n";
    }
}


sub CompareLatLng {
    
    my $dbresult = $mstr->selectall_arrayref("SELECT offshore_collections.id, lat, lng \
	FROM offshore_collections JOIN cols on offshore_collections.col_id = cols.id");
    
    die $mstr->errstr unless ref $dbresult eq 'ARRAY';
    
    my %found;
    my $duplicates = 0;
    
    foreach my $row ( @$dbresult )
    {
	my ($id, $lat, $lng) = @$row;
	
	unless ( $found{$lat}{$lng} )
	{
	    $found{$lat}{$lng} = 1;
	    
	    my $query = "SELECT collection_no, collection_name FROM collections WHERE \
	abs(lat - '$lat') < 0.1 and abs(lng - '$lng') < 0.1";
	    
	    my $result = $pbdb->selectall_arrayref($query);
	    
	    if ( ref $result eq 'ARRAY' && @$result )
	    {
		foreach my $r ( @$result )
		{
		    my ($collection_no, $collection_name) = @$r;
		    
		    say "Possible duplicate: $collection_no '$collection_name' <-> $id";
		    $duplicates++;
		}
	    }
	}
    }
    
    say "Found $duplicates potential duplicates.";    
}


# sub InitConstraints {
    
#     my ($count) = DBRowQuery($mstr, "SELECT count(*) FROM offshore_constraints");
    
#     if ( $count > 0 )
#     {
# 	die "There are already constraints in this database.\n";
#     }
    
#     DBCommand($mstr, "
# 	INSERT INTO offshore_constraints (col_id, section_id, unit_id, 
# 		depth, relation, bound, type, label)
# 	SELECT c.col_id, c.section_id, c.unit_id, c.bottom_depth, 'gt', a.source_min,
# 		'genus', a.taxon_name
# 	FROM offshore_occs as o JOIN offshore_collections as c on o.sample_id = c.id
# 	    JOIN pbdb.age_check_genera as a on a.taxon_name = o.genus_name
# 	WHERE c.bottom_depth > 0 and a.source_min > 0");
    
#     DBCommand($mstr, "
# 	INSERT INTO offshore_constraints (col_id, section_id, unit_id, 
# 		depth, relation, bound, type, label)
# 	SELECT c.col_id, c.section_id, c.unit_id, c.top_depth, 'lt', a.source_max,
# 		'genus', a.taxon_name
# 	FROM offshore_occs as o JOIN offshore_collections as c on o.sample_id = c.id
# 	    JOIN pbdb.age_check_genera as a on a.taxon_name = o.genus_name
# 	WHERE (c.top_depth > 0 or a.source_max > 0) and a.source_max is not null");
    
# }


# sub ClearConstraints {
    
#     my ($count) = DBRowQuery($mstr, "SELECT count(*) FROM offshore_constraints");
    
#     if ( $count == 0 )
#     {
# 	die "There are no constraints.\n";
#     }
    
#     DBCommand($mstr, "DELETE FROM offshore_constraints");
# }


sub InsertData {
    
    my ($count) = DBRowQuery($pbdb, "SELECT count(*) from collections WHERE upload='eODP'");
    
    if ( $count > 0 )
    {
	die "There is already eODP data in this database.\n";
    }
    
    ($count) = DBRowQuery($mstr, "SELECT count(*) FROM offshore_occs WHERE genus_name = ''");
    
    if ( $count > 0 )
    {
	die "You must execute the 'adjust' subcommand first.\n";
    }
    
    say "Starting transaction...";
    
    $pbdb->begin_work or die $pbdb->errstr;
    
    # Insert the collections
    
    DBCommand($pbdb, <<END_STMT);
INSERT INTO collections (authorizer, enterer, authorizer_no, enterer_no, upload, upload_id,
	research_group, license, reference_no, collection_name, country,
	lat, lng, latlng_basis, gps_datum, geogscale, 
	localsection, localbed, localbedunit, localorder, stratscale, environment, tectonic_setting,
	pres_mode, assembl_comps, collection_type, collection_coverage, coll_meth, access_level,
	max_interval_no, direct_ma, direct_ma_unit, direct_ma_method,
	lithology1, fossilsfrom1, lithification, 
	lithadj, minor_lithology, lithology2, fossilsfrom2,
	collectors, preservation_quality, fragmentation, abund_in_sediment, taxonomy_comments)
SELECT 'A. Fraass', 'S. Peters', '919', '136', 'eODP', oc.id,
	'eODP', 'CC BY', '82981', coll_name, country, 
	cols.lat, cols.lng, 'stated in text', 'WGS84', 'hand sample',
	site_hole, mid_depth, 'mbsf', 'top to bottom', 'bed', 'basinal (carbonate)', 'deep ocean basin',
	'body', 'microfossils', 'biostratigraphic', 'some microfossils', 'core', 'the public',
	pbdb_interval_no, round(ma, 2), 'Ma', 'age-depth',
	if(pbdb_lith<>'',pbdb_lith,'not reported'), 'Y', pbdb_lithification,
        pbdb_lith_adj, pbdb_minor_lith,	pbdb_lith_2, if(pbdb_lith_2<>'', 'Y', NULL),
	'IODP', pbdb_pres, pbdb_frag, pbdb_abund, data_source_notes
FROM macrostrat.offshore_collections as oc
	JOIN macrostrat.cols as cols on col_id=cols.id
WHERE taxa_count > 0
END_STMT
    
    # Insert the newly added collections into the `coll_units` table
    
    DBCommand($pbdb, <<END_STMT);
INSERT INTO coll_units (collection_no, col_id, unit_id)
SELECT c.collection_no, oc.col_id, oc.unit_id
FROM collections as c JOIN macrostrat.offshore_collections as oc
	on oc.id = c.upload_id and c.upload = 'eODP'
END_STMT
    
    # Set the latitude and longitude fields from the raw lat/lng numbers.
    
    DBCommand($pbdb, <<END_STMT);
UPDATE collections 
SET lngdeg=floor(abs(lng)), lngdir=if(lng<0, 'West', 'East'), 
    latdeg=floor(abs(lat)), latdir=if(lat<0, 'South', 'North')
WHERE upload='eODP'
END_STMT
    
    DBCommand($pbdb, <<END_STMT);
UPDATE collections
SET lngmin=floor((abs(lng)-lngdeg)*60), latmin=floor((abs(lat)-latdeg)*60)
WHERE upload='eODP'
END_STMT
    
    DBCommand($pbdb, <<END_STMT);
UPDATE collections
SET lngsec=floor(((abs(lng)-lngdeg)*60-lngmin)*60), latsec=floor(((abs(lat)-latdeg)*60-latmin)*60)
WHERE upload='eODP'
END_STMT
    
    # Insert occurrences
    
    DBCommand($pbdb, <<END_STMT);
INSERT INTO occurrences (authorizer, enterer, authorizer_no, enterer_no, upload, upload_id,
	reference_no, collection_no, taxon_no, genus_reso, genus_name, subgenus_name, 
	species_reso, species_name, subspecies_reso, subspecies_name, 
	abund_value, abund_unit, comments)
SELECT 'A. Fraass', 'S. Peters', '919', '136', 'eODP', oo.id, '82981',
	collection_no, pbdb_taxon_id, genus_modifier, genus_name, subgenus_name,
	species_modifier, species_name, subspecies_modifier, subspecies_name,
	cleaned_code, code_unit, comments
FROM macrostrat.offshore_occs as oo
	JOIN collections as c on oo.sample_id = c.upload_id and c.upload='eODP'
END_STMT
    
    $pbdb->commit;
}


sub RemoveData {
    
    say "Removing eODP data from the PBDB...";
    
    DBCommand($pbdb, "DELETE FROM occurrences WHERE upload='eODP'");
    
    my ($next) = DBRowQuery($pbdb, "SELECT max(occurrence_no)+1 FROM occurrences");
    
    DBCommand($pbdb, "ALTER TABLE occurrences AUTO_INCREMENT = $next");
    
    DBCommand($pbdb, "DELETE FROM collections WHERE upload='eODP'");
    
    ($next) = DBRowQuery($pbdb, "SELECT max(collection_no)+1 FROM collections");
    
    DBCommand($pbdb, "ALTER TABLE collections AUTO_INCREMENT = $next");
}


# sub AddUnits {
    
#     my (@gapcols) = DBColumnQuery($mstr, "SELECT distinct col_id FROM offshore_collections
# 					WHERE ma = ''");
    
#     foreach my $col_id ( @gapcols )
#     {
# 	my $sql = "
# 	SELECT col_id, unit_id, min(top_depth) as top_depth, max(bottom_depth) as bottom_depth,
# 	    count(*) as n_colls, group_concat(distinct ms_lith) as lith
# 	FROM offshore_collections
# 	WHERE col_id = '$col_id' and unit_id > 0 GROUP BY unit_id
# 	UNION SELECT u.col_id, u.id as unit_id, position_top as top_depth,
# 	    position_bottom as bottom_depth, 0 as n_colls, l.lith
# 	FROM units as u LEFT JOIN unit_liths as ul on ul.unit_id = u.id
# 		JOIN liths as l on l.id = ul.lith_id
# 		LEFT JOIN offshore_collections as c on c.col_id = u.col_id and c.unit_id = u.id
# 	WHERE u.col_id = '$col_id' and c.unit_id is null
# 	UNION SELECT c.col_id, c.unit_id, top_depth, bottom_depth, count(*) as n_colls, '?' as lith
# 	FROM offshore_collections as c
# 	WHERE col_id = '$col_id' and unit_id = 0 GROUP BY top_depth, bottom_depth
# 	ORDER BY top_depth, bottom_depth";
	
# 	my $dbresult = $dbh->selectall_arrayref($sql);
	
# 	foreach my $layer ( @$dbresult )
# 	{
	    
	    
	    
	    
# 	}
#     }
    
# }


sub DBTextQuery {
    
    my ($dbh, $query) = @_;
    
    my $dbresult = $dbh->selectall_arrayref($query);
    
    my $result = '';
    
    if ( ref $dbresult eq 'ARRAY' )
    {
	foreach my $row ( @$dbresult )
	{
	    $result .= join("\t", @$row) . "\n";
	}
    }
    
    return $result;
}


sub DBArrayQuery {
    
    my ($dbh, $query) = @_;

    my $dbresult = $dbh->selectall_arrayref($query, { Slice => { } });

    if ( ref $dbresult eq 'ARRAY' )
    {
	return $dbresult;
    }

    else
    {
	return [ ];
    }
}


sub DBRowQuery {
    
    my ($dbh, $query) = @_;
    
    my @dbresult = eval { $dbh->selectrow_array($query) };
    
    if ( $@ )
    {
	my ($package, $filename, $line) = caller;
	    
	$@ .= "called from line $line of $filename\n";
	die $@;
    }
    
    return @dbresult;    
}


sub DBColumnQuery {
    
    my ($dbh, $query) = @_;
    
    my $dbresult = eval { $dbh->selectcol_arrayref($query) };
    
    if ( $@ )
    {
	my ($package, $filename, $line) = caller;
	    
	$@ .= "called from line $line of $filename\n";
	die $@;
    }
    
    return @$dbresult;
}


sub DBCommand {
    
    my ($dbh, $command) = @_;
    
    $command =~ s/\\/\\\\/g;
    $command =~ s/\n$//;
    
    say $command;
    
    if ( $EXECUTE_MODE )
    {
	my $result;
	
	eval { $result = $dbh->do($command) };
	
	if ( defined $result && ! $@ )
	{
	    say "Changed $result rows.";
	    say "";
	}
	
	elsif ( $@ )
	{
	    my ($package, $filename, $line) = caller;
	    
	    $@ .= "called from line $line of $filename\n";
	    die $@;
	}
	
	return $result;
    }
}




