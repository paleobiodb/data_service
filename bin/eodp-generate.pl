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
my ($opt_config, $opt_filter, $opt_match, $opt_age, $opt_depth);

GetOptions("quiet|q" => \$opt_quiet,
	   "verbose|v" => \$opt_verbose,
	   "config|f" => \$opt_config,
	   "filter|F=s" => \$opt_filter,
	   "matchdepth=f" => \$opt_match,
	   "intage=f" => \$opt_age,
	   "force" => \$opt_force,
	   "help|h" => \$opt_help,
	   "debug|D" => \$opt_debug) or die;

our ($mstr, $pbdb, $EXECUTE_MODE);
our ($MATCH_DIST_LIMIT) = 0.25;
our ($INTP_AGE_LIMIT) = 2.0;
our ($COLLECTIONS_TABLE) = 'offshore_collections';
our ($OCCURRENCES_TABLE) = 'offshore_occs';

$MATCH_DIST_LIMIT = $opt_match if defined $opt_match;
$INTP_AGE_LIMIT = $opt_age if defined $opt_age;

loadConfig($opt_config);

my $dbconf = configData('Database');

if ( $ENV{PWD} ne '/var/paleomacro/pbapi' )
{
    $dbconf->{host} = '127.0.0.1';
}

$mstr = connectDB($opt_config, 'macrostrat');
$pbdb = connectDB($opt_config, 'pbdb');

die "Could not connect to database: $DBI::errstr" unless $mstr && $pbdb;

my $coll_table = configData('offshore-collections');
$COLLECTIONS_TABLE = $coll_table if $coll_table;

# my ($occs) = DBRowQuery($mstr, "SELECT count(*) from offshore_occs");

# say "Found $occs occurrences in table `offshore_occs`.";

if ( $ARGV[0] eq 'check' && $ARGV[1] eq 'table' )
{
    AdjustTable();
}

elsif ( $ARGV[0] eq 'adjust' && $ARGV[1] eq 'table' )
{
    $EXECUTE_MODE = 1;
    AdjustTable();
}

elsif ( $ARGV[0] =~ /^check$|^update$/ && $ARGV[1] eq 'liths' )
{
    $EXECUTE_MODE = 1 if $ARGV[0] eq 'update';
    my $subcommand = shift @ARGV;
    shift @ARGV;
    UpdateLiths($subcommand, @ARGV);
}

elsif ( $ARGV[0] eq 'match' && $ARGV[1] eq 'tops' )
{
    $EXECUTE_MODE = 1;
    MatchTops('match');
}

elsif ( $ARGV[0] =~ /^show$|^check$|^match$|^summarize$/ && $ARGV[1] eq 'units' )
{
    $EXECUTE_MODE = 1 if $ARGV[0] eq 'match';
    my $subcommand = shift @ARGV;
    shift @ARGV;
    MatchUnits($subcommand, @ARGV);
}

elsif ( $ARGV[0] =~ /^show$|^check$|^do$/ && $ARGV[1] =~ /^interp/ )
{
    $EXECUTE_MODE = 1 if $ARGV[0] eq 'do';
    my $subcommand = shift @ARGV;
    shift @ARGV;
    InterpolateCollections($subcommand, @ARGV);
}

elsif ( $ARGV[0] eq 'compare' )
{
    $EXECUTE_MODE = 1;
    CompareLatLng();
}

elsif ( $ARGV[0] eq 'show' && $ARGV[1] eq 'column' )
{
    shift @ARGV;
    shift @ARGV;
    ShowColumn(@ARGV);
}

elsif ( $ARGV[0] eq 'show' && $ARGV[1] eq 'missing' )
{
    shift @ARGV;
    shift @ARGV;
    ShowMissing(@ARGV);
}

elsif ( $ARGV[0] =~ /^check$|^update$/ && $ARGV[1] eq 'ages' )
{
    my $subcommand = shift @ARGV;
    shift @ARGV;
    $EXECUTE_MODE = 1 if $subcommand eq 'update';
    UpdateAges($subcommand, @ARGV);
}

elsif ( $ARGV[0] eq 'set' && $ARGV[1] eq 'pbdb' && $ARGV[2] eq 'liths' )
{
    $EXECUTE_MODE = 1;
    SetPBDBLiths();
}

elsif ( $ARGV[0] =~ /^show$|^generate$|^set$/ && $ARGV[1] eq 'pbdb' && $ARGV[2] eq 'attrs' )
{
    my $subcommand = shift @ARGV;
    shift @ARGV;
    shift @ARGV;
    $EXECUTE_MODE = 1 if $subcommand eq 'set' || $subcommand eq 'generate';
    SetPBDBAttrs($subcommand, @ARGV);
}

elsif ( $ARGV[0] =~ /^set$/ && $ARGV[1] eq 'pbdb' && $ARGV[2] eq 'intervals' )
{
    my $subcommand = shift @ARGV;
    shift @ARGV;
    shift @ARGV;
    $EXECUTE_MODE = 1 if $subcommand eq 'set';
    SetPBDBIntervals($subcommand, @ARGV);
}

elsif ( $ARGV[0] =~ /^check$|^update$/ && $ARGV[1] eq 'genera' )
{
    my $subcommand = shift @ARGV;
    shift @ARGV;
    $EXECUTE_MODE = 1 if $subcommand eq 'update';
    UpdateGeneraReport(@ARGV);
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

elsif ( $ARGV[0] eq 'insert' && $ARGV[1] eq 'data' )
{
    $EXECUTE_MODE = 1;
    InsertData();
}

elsif ( $ARGV[0] eq 'update' && $ARGV[1] eq 'data' && $ARGV[2] )
{
    shift @ARGV;
    shift @ARGV;
    $EXECUTE_MODE = 1;
    UpdateData(@ARGV);
}

elsif ( $ARGV[0] eq 'remove' && $ARGV[1] eq 'data' )
{
    $EXECUTE_MODE = 1;
    RemoveData();
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
    
    my $check = DBTextQuery($mstr, "SHOW COLUMNS FROM $COLLECTIONS_TABLE LIKE 'country'");
    
    unless ( $check =~ /country/ )
    {
	DBCommand($mstr, "ALTER TABLE $COLLECTIONS_TABLE ADD COLUMN IF NOT EXISTS `country` \
	varchar(255) not null default ''");
	$has_country = 0;
    }
    
    # Step II: check/update the contents of `offshore_collections`.
    
    say "Checking contents of table `offshore_collections`...";
    
    # If all records have 0 in the column `taxa_count`, set that field by joining with 
    # `offshore_occs`.
    
    my ($tcount) = DBRowQuery($mstr, "SELECT count(*) FROM $COLLECTIONS_TABLE \
	WHERE taxa_count > 0");
    
    unless ( $tcount > 0 )
    {
	DBCommand($mstr, "UPDATE $COLLECTIONS_TABLE as oc JOIN \
	(SELECT sample_id, count(*) as count FROM $OCCURRENCES_TABLE GROUP BY sample_id) as oo \
		ON oc.id = oo.sample_id \
	SET oc.taxa_count = oo.count");
    }
    
    # If any records have an empty value for country, set that now based on the latitude
    # and longitude of each collection.
    
    my ($ccount) = $has_country && DBRowQuery($mstr, "SELECT count(*) FROM $COLLECTIONS_TABLE WHERE country=''");
    
    if ( $ccount > 0 )
    {
	DBCommand($mstr, "UPDATE $COLLECTIONS_TABLE as c JOIN cols on col_id = cols.id \
	SET c.country = 'Southern Ocean' WHERE country = '' and cols.lat <= -60");
	
	DBCommand($mstr, "UPDATE $COLLECTIONS_TABLE as c JOIN cols on col_id = cols.id \
	SET c.country = 'Arctic Ocean' WHERE country = '' and cols.lat >= 65");
	
	DBCommand($mstr, "UPDATE $COLLECTIONS_TABLE as c JOIN cols on col_id = cols.id \
	SET c.country = 'North Atlantic' WHERE country = '' and cols.lat > 0 and \
		cols.lng < 25 and (cols.lat >= 18 and cols.lng >= -98 or \
		cols.lat < 18 and cols.lat > (cols.lng * -0.5625 - 37.125))");
	
	DBCommand($mstr, "UPDATE $COLLECTIONS_TABLE as c JOIN cols on col_id = cols.id \
	SET c.country = 'South Atlantic' WHERE country = '' and cols.lat <= 0 and \
		cols.lng < 25 and cols.lng >= -66");
	
	DBCommand($mstr, "UPDATE $COLLECTIONS_TABLE as c JOIN cols on col_id = cols.id \
	SET c.country = 'North Pacific' WHERE country = '' and cols.lat > 0 and \
		(cols.lng > 104 or (cols.lat >= 18 and cols.lng < -98 or \
		cols.lat < 18 and cols.lat < (cols.lng * -0.5625 - 37.125)))");
	
	DBCommand($mstr, "UPDATE $COLLECTIONS_TABLE as c JOIN cols on col_id = cols.id \
	SET c.country = 'South Pacific' WHERE country = '' and cols.lat <= 0 and \
		(cols.lng <= -98 or (cols.lat < -20 and cols.lng > 145) or \
		(cols.lat >= -20 and cols.lat > (cols.lng * -0.357 + 31.765)))");
	
	DBCommand($mstr, "UPDATE $COLLECTIONS_TABLE as c JOIN cols on col_id = cols.id \
	SEt c.country = 'Indian Ocean' WHERE country = '' and cols.lng >= 25 and \
		(cols.lat < 19 and cols.lng < 80 or cols.lat < 23 and cols.lng >= 80) and \
		(cols.lng < 103 or cols.lat < -20 and cols.lng <= 145 or \
		 cols.lat >= -20 and cols.lat <= (cols.lng * -0.357 + 31.765))");
	
	($ccount) = DBRowQuery($mstr, "SELECT count(*) FROM $COLLECTIONS_TABLE WHERE country=''");
	
	if ( $ccount > 0 )
	{
	    say "  There are still $ccount collections with no country name.";
	}
    }
    
    # Step III: check/update the structure of `offshore_occs`.
    
    say "Checking structure of table `offshore_occs`...";
    
    # If there is a column named `subgenera_name`, rename it to `subgenus_name`.
    
    $check = DBTextQuery($mstr, "SHOW COLUMNS FROM $OCCURRENCES_TABLE LIKE 'subgenera_name'");
    
    if ( $check !~ /subgenera_name/ )
    {
	DBCommand($mstr, "ALTER TABLE $OCCURRENCES_TABLE CHANGE `subgenera_name` `subgenus_name` \
	varchar(100) NOT NULL default ''");
    }
    
    # Step IV: check/update the contents of `offshore_occs`.
    
    say "Checking contents of table `offshore_occs`...";
    
    # Fix occurrences where the genus is empty.
    
    my ($count) = DBRowQuery($mstr, "SELECT count(*) FROM $OCCURRENCES_TABLE WHERE genus_name = ''");
    
    if ( $count )
    {
	say "There are $count rows with genus_name = ''";
	
	# If the taxonomic name is 'xxx indet.', set the genus to 'xxx'.
	
	DBCommand($mstr, "UPDATE $OCCURRENCES_TABLE
	SET genus_name=regexp_substr(name, '^\\w+'),
	    species_name='indet.'
	WHERE name rlike '^\\w+ indet.' and genus_name = ''");
	
	# If the taxonomic name starts with '"xxx"', set the genus name to 'xxx' and the
	# genus modifier to 'informal'. I (MM) have determined that the modifier
	# 'informal' more closely matches how quotes are used in this dataset than the
	# modifier '"'.
	
	DBCommand($mstr, "UPDATE $OCCURRENCES_TABLE
	SET genus_name=regexp_substr(name, '(?<=\").*?(?=\")'),
	    genus_modifier='informal'
	WHERE name rlike '^\"' and genus_name = ''");
	
	# Do the same for some occurrences where the taxonomic name is '"Forma T"'.
	
	DBCommand($mstr, "UPDATE $OCCURRENCES_TABLE
	SET genus_name = 'Forma T', genus_modifier = 'informal'
	WHERE name = '\"Forma T\"'");
	
	# Verify that there are now 0 rows with an empty genus name.
	
	my ($newcount) = DBRowQuery($mstr, "SELECT count(*) FROM $OCCURRENCES_TABLE
					WHERE genus_name = ''");
	
	say "There are now $newcount rows with genus_name = ''";
    }
    
    # Fix abbreviated species and subspecies modifiers.
    
    my ($varcount) = DBRowQuery($mstr, "SELECT count(*) FROM $OCCURRENCES_TABLE
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
    
    my ($scount) = DBRowQuery($mstr, "SELECT count(*) FROM $OCCURRENCES_TABLE
	WHERE species_modifier in ('s.s.', 's.l.')");
    
    if ( $scount )
    {
	say "There are $scount rows with species modifiers 's.s.', 's.l.'";
	
	DBCommand($mstr, "UPDATE $OCCURRENCES_TABLE SET species_modifier = 'sensu lato'
	WHERE species_modifier = 's.l.'");
	
	DBCommand($mstr, "UPDATE $OCCURRENCES_TABLE SET species_modifier = 'sensu stricto' \
	WHERE species_modifier = 's.s.'");
    }
    
    # Step V: check for the table `coll_units` and create it if it doesn't already exist.
    
    my ($ucheck) = DBRowQuery($pbdb, "SHOW TABLES LIKE '$TABLE{COLLECTION_UNITS}'");
    
    unless ( $ucheck =~ /coll_units/ )
    {
	DBCommand($pbdb, "CREATE TABLE IF NOT EXISTS `$TABLE{COLLECTION_UNITS}` (
		`id` int unsigned  not null PRIMARY KEY,
		`collection_no` int unsigned not null,
		`column_id` int unsigned not null default '0',
		`unit_id` int unsigned not null default '0',
		UNIQUE KEY (`collection_no`, `column_id`, `unit_id`),
		KEY (`col_id`, `unit_id`),
		KEY (`unit_id`)) Engine=InnoDB");
    }
}


# UpdateLiths ( subcommand, col_id )

sub UpdateLiths {
    
    my ($subcommand, @col_ids) = @_;

    my $column_list = join(',', @col_ids);
    
    if ( $subcommand eq 'check' )
    {
	my $sql = "SELECT c.col_id, count(*) as c
	FROM $COLLECTIONS_TABLE as c left join
		(SELECT unit_id, group_concat(distinct lith ORDER BY ul.comp_prop desc) as liths
		 FROM unit_liths as ul join liths as l on l.id = ul.lith_id
		 GROUP BY unit_id) as ul using (unit_id)
	WHERE c.ms_lith <> regexp_replace(ul.liths, ',.*', '')
	GROUP BY c.col_id";
	
	my @diff_cols = DBArrayQuery($mstr, $sql)->@*;
	my $n_cols = scalar(@diff_cols);
	my $n_colls = 0;
	
	foreach my $r ( @diff_cols )
	{
	    my ($col_id, $c) = $r->@*;
	    $n_colls += $c;
	}
	
	say "Primary lithology differs in $n_colls collections in $n_cols columns";
	
	$sql = "SELECT c.col_id, count(*) as c 
	FROM $COLLECTIONS_TABLE as c left join
		(SELECT unit_id, group_concat(distinct lith ORDER BY ul.comp_prop desc) as liths
		 FROM unit_liths as ul join liths as l on l.id = ul.lith_id
		 GROUP BY unit_id) as ul using (unit_id)
	WHERE c.ms_lith_2 <> regexp_replace(regexp_replace(ul.liths, '^.*?,', ''), ',.*', '')
	GROUP BY c.col_id";
	
	my @diff2_cols = DBArrayQuery($mstr, $sql)->@*;
	my $n_cols2 = scalar(@diff2_cols);
	my $n_colls2 = 0;
	
	foreach my $r ( @diff2_cols )
	{
	    my ($col_id, $c) = $r->@*;
	    $n_colls2 += $c;
	}
			
	say "Secondary lithology differs in $n_colls2 collections in $n_cols2 columns";
    }

    elsif ( $subcommand eq 'update' )
    {
	my $sql = "UPDATE $COLLECTIONS_TABLE as c left join
		(SELECT unit_id,
		   group_concat(distinct lith ORDER BY comp_prop desc, lith_id LIMIT 1) as lith1,
		   group_concat(distinct lith ORDER BY comp_prop desc, lith_id LIMIT 1,1) as lith2
		 FROM unit_liths as ul join liths as l on l.id = ul.lith_id
		 GROUP BY unit_id) as ul using (unit_id)
	SET c.ms_lith = lith1,
	    c.ms_lith_2 = lith2
	WHERE unit_id > 0";
	
	DBCommand($mstr, $sql);
	
	$sql = "UPDATE $COLLECTIONS_TABLE as c left join
	    (SELECT unit_id, group_concat(lith_atts limit 1) as att1,
			group_concat(lith_atts limit 1,1) as att2
	     FROM (SELECT unit_id, group_concat(distinct lith_att) as lith_atts
		   FROM unit_liths as ul join unit_liths_atts as ua on ua.unit_lith_id = ul.id
		     join lith_atts as la on la.id = ua.lith_att_id
		   GROUP BY ul.id ORDER BY ul.comp_prop desc, lith_id) as ua
	     GROUP BY unit_id) as ua using (unit_id)
	SET c.ms_lith_att = ua.att1,
	    c.ms_lith_att_2 = ua.att2
	WHERE unit_id > 0";

	DBCommand($mstr, $sql);
    }
}


# MatchTops ( )
#
# Look for collections where the top_depth and bottom_depth are identical and
# which have no matching unit but are located exactly at the top of an existing
# unit in the same column. Assign these collections to those units.

sub MatchTops {

    my ($mcount) = DBRowQuery($mstr, "SELECT count(*) FROM $COLLECTIONS_TABLE as c 
			JOIN units as u on u.col_id = c.col_id 
			     and u.position_top = c.top_depth and u.position_top = c.bottom_depth
			WHERE c.unit_id = 0");
    
    if ( $mcount > 0 )
    {
	say "Matching collections with top_depth = bottom_depth to their units...";
	
	DBCommand($mstr, "UPDATE $COLLECTIONS_TABLE as c 
			      JOIN units as u on u.col_id = c.col_id 
			      and u.position_top = c.top_depth and u.position_top = c.bottom_depth
			SET c.unit_id = u.id
			WHERE c.unit_id = 0");
    }
    
    else
    {
	say "No unmatched top units found.\n";
    }
    
    return;
}


# MatchUnits ( subcommand, col_id )
#
# Attempt to match offshore collections that are outside of any unit to the closest unit.
# If an unmatched collection is within $MATCH_DIST_LIMIT of some unit, it will be taken to
# belong to that unit.

sub MatchUnits {
    
    my ($subcommand, @col_ids) = @_;
    
    unless ( $col_ids[0] )
    {
	die "You must specify a column id or 'all'\n";
    }
    
    my (@columns, @summary, $max_distance_all, $matched_all, $stretched_all, $unmatched_all);
    my ($updated, @updates_all, $max_depth_gap, $max_age_gap);
    
    # If the argument 'all' was specified, iterate over every column that has unmatched
    # collections.
    
    if ( $col_ids[0] eq 'all' )
    {
	@columns = DBColumnQuery($mstr, "SELECT distinct col_id from $COLLECTIONS_TABLE
		      			WHERE unit_id = 0 ORDER BY col_id");
    }
    
    # Otherwise, iterate over the specified columns.
    
    elsif ( $col_ids[0] > 0 )
    {
	@columns = @col_ids;
    }
    
    else
    {
	die "Invalid column id '$col_ids[0]'\n";
    }
    
    # For each column, gnerate an SQL statement that will find all of the collections in
    # each selected column, together with all of the units that don't have any matching
    # collections. This will let us detect when a collection which has no unit is very
    # close to an existing unit.

    foreach my $col ( @columns )
    {
	my $sql = "SELECT id as collection_id, col_id, unit_id, max_interval_id, min_interval_id,
			ma, top_depth, bottom_depth, ms_lith
	FROM $COLLECTIONS_TABLE
	WHERE col_id  = '$col'
	UNION SELECT 0 as collection_id, u.col_id, u.id as unit_id,
		0 as max_interval_id, 0 as min_interval_id, 0 as ma,
		position_top as top_depth, position_bottom as bottom_depth, l.lith as ms_lith
	FROM units as u
	     left join unit_liths as ul on ul.unit_id = u.id join liths as l on l.id = ul.lith_id
	     left join $COLLECTIONS_TABLE as c on c.col_id = u.col_id and c.unit_id = u.id
	WHERE u.col_id = '$col' and c.unit_id is null
	ORDER BY col_id, top_depth, bottom_depth";
	
	# If the subcommand is 'show', then display the result of the query as a table.
	
	if ( $subcommand eq 'show' )
	{
	    my $result = DBArrayQuery($mstr, $sql);
	    
	    my $output = FormatTable(['collection_id', 'col_id', 'unit_id', 'max_int', 'min_int',
				      'ma', 'top_depth', 'bottom_depth', 'ms_lith'], @$result);
	    
	    $output .= "Found " . scalar(@$result) . " rows\n";
	    
	    print "$output\n";
	}
	
	# Otherwise, go through the result and attempt to match all of the collections with no
	# unit to the closest unit.
	
	else
	{
	    my $result = DBHashQuery($mstr, $sql);
	    
	    my (@updates, $max_distance);
	    my $matched = 0;
	    my $stretched = 0;
	    my $unmatched = 0;
	    my $unit_rows = 0;
	    my $nonunit_rows = 0;
	    
	    # Scan for collections that have no matching unit.
	    
	    foreach my $i ( 0..$#$result )
	    {
		my $collection_id = $result->[$i]{collection_id};
		my $col_id = $result->[$i]{col_id};

		if ( $subcommand eq 'summarize' )
		{
		    my $a = $result->[$i]{unit_id} ? $unit_rows++ : $nonunit_rows++;
		}
		
		next unless $collection_id > 0 && $result->[$i]{unit_id} == 0;
		
		my ($j, $k, $up_unit, $up_distance, $down_unit, $down_distance);
		my ($sel_unit, $sel_dir, $sel_distance, $sel_lith);
		
		# When we find one, scan upwards and downwards until we find the closest
		# unit in each direction.
		
		for ($j = $i-1; $j >= 0; $j--)
		{
		    last if $j < 0 || $result->[$j]{unit_id} > 0;
		}
		
		if ( $j >= 0 && $result->[$j]{unit_id} > 0 )
		{
		    $up_unit = $result->[$j]{unit_id};
		    $up_distance = $result->[$i]{top_depth} - $result->[$j]{bottom_depth};
		    $up_distance = int($up_distance * 10000 + 0.0000004) / 10000;
		}
		
		for ($k = $i+1; $k <= $#$result; $k++)
		{
		    last if $result->[$k]{unit_id} > 0;
		}
		
		if ( $k <= $#$result && $result->[$k]{unit_id} > 0 )
		{
		    $down_unit = $result->[$k]{unit_id};
		    $down_distance = $result->[$k]{top_depth} - $result->[$i]{bottom_depth};
		    $down_distance = int($down_distance * 10000 + 0.0000004) / 10000;
		}
		
		if ( defined $up_distance && $up_distance <= $MATCH_DIST_LIMIT ||
		     defined $down_distance && $down_distance <= $MATCH_DIST_LIMIT )
		{
		    if ( defined $up_distance && $up_distance <= 0 )
		    {
			$sel_unit = $up_unit;
			$sel_distance = $up_distance;
			$sel_dir = 'up';
			$sel_lith = $result->[$j]{ms_lith};
			$matched++;
		    }
		    
		    elsif ( defined $down_distance && $down_distance <= 0 )
		    {
			$sel_unit = $down_unit;
			$sel_distance = $down_distance;
			$sel_dir = 'down';
			$sel_lith = $result->[$k]{ms_lith};
			$matched++;
		    }
		    
		    elsif ( $up_distance < $down_distance || ! defined $down_distance )
		    {
			$sel_unit = $up_unit;
			$sel_distance = $up_distance;
			$sel_dir = 'up';
			$stretched++;
		    }
		    
		    elsif ( ! defined $up_distance || $down_distance <= $up_distance )
		    {
			$sel_unit = $down_unit;
			$sel_distance = $down_distance;
			$sel_dir = 'down';
			$stretched++;
		    }

		    else
		    {
			print "NO MATCH FOR $collection_id ($col_id)\n";
		    }
		    
		    push @updates, [$col_id, $collection_id, $sel_unit, $sel_dir, $sel_distance];
		    
		    $max_distance = $sel_distance if ! $max_distance ||
			$sel_distance > $max_distance;
		    $max_distance_all = $sel_distance if ! $max_distance_all ||
			$sel_distance > $max_distance_all;
		}

		else
		{
		    $unmatched++;
		}
	    }
	    
	    # If the subcommand is 'check', then print out the updates table.
	    
	    if ( $subcommand eq 'check' )
	    {
		my $output = FormatTable(['col_id', 'collection_id', 'unit_id', 'direction',
					  'distance'], @updates);
		
		$output .= "Matched $matched collections\n";
		$output .= "Stretched $stretched collections\n";
		$output .= "No match for $unmatched collections\n";
		$output .= "Max distance = $max_distance\n";
		
		print "$output\n";
	    }
	    
	    # If the command is 'summarize', then add a row to the summary table.
	    
	    elsif ( $subcommand eq 'summarize' )
	    {
		if ( $opt_filter eq 'nonunits' )
		{
		    next unless $unit_rows == 0 || ($nonunit_rows / $unit_rows >= 0.5);
		}
		
		push @summary, [$col, scalar(@$result), $unit_rows, $nonunit_rows, $matched,
				$stretched, $unmatched, $max_distance];
		$matched_all += $matched;
		$stretched_all += $stretched;
		$unmatched_all += $unmatched;
	    }
	    
	    # Otherwise, do the updates.
	    
	    elsif ( $subcommand eq 'match' )
	    {
		foreach my $r ( @updates )
		{
		    my ($col_id, $collection_id, $unit_id, $direction, $distance, $ms_lith) = @$r;
		    
		    my $sql = "UPDATE $COLLECTIONS_TABLE SET unit_id = '$unit_id'
			       WHERE id = '$collection_id' LIMIT 1";

		    my $result = DBCommand($mstr, $sql, 1);
		    $updated++ if $result;
		}

		$matched_all += $matched;
		$stretched_all += $stretched;
		$unmatched_all += $unmatched;
	    }
       	}
    }

    if ( $subcommand eq 'summarize' )
    {
	my $output = FormatTable(['col_id', 'rows', 'unit_rows', 'nonunit_rows', 'matched',
				  'stretched', 'unmatched', 'max_distance'], @summary);

	$output .= "Matched $matched_all collections\n";
	$output .= "Stetched $stretched_all collections\n";
	$output .= "No match for $unmatched_all collections\n";
	$output .= "Max distance = $max_distance_all\n";
	
	print "$output\n";
    }

    elsif ( $subcommand eq 'match' )
    {
	my $output = '';
	
	$output .= "Matched $matched_all collections\n";
	$output .= "Stretched $stretched_all collections\n";
	$output .= "Updated $updated collections\n";
	$output .= "No match for $unmatched_all collections\n";
	$output .= "Max distance = $max_distance_all\n";

	print "$output\n";
    }
}


# InterpolateCollections ( subcommand, columns... )
#
# Interpolate collections that are unmatched and lie between units. If an unmatched
# collection lies between two units whose age gap is <= $INTP_AGE_LIMIT, interpolate
# its age and lithology based on the ages and lithologies of the units above and below.

sub InterpolateCollections {

    my ($subcommand, @col_ids) = @_;
    
    unless ( $col_ids[0] )
    {
	die "You must specify a column id or 'all'\n";
    }
    
    my (@columns, @summary, $max_distance_all, $matched_all, $stretched_all, $unmatched_all);
    my ($updated, @updates_all, $max_depth_gap, $max_age_gap);
    
    # If the argument 'all' was specified, iterate over every column that has unmatched
    # collections.
    
    if ( $col_ids[0] eq 'all' )
    {
	@columns = DBColumnQuery($mstr, "SELECT distinct col_id from $COLLECTIONS_TABLE
		      			WHERE unit_id = 0 ORDER BY col_id");
    }
    
    # Otherwise, iterate over the specified columns.
    
    elsif ( $col_ids[0] > 0 )
    {
	@columns = @col_ids;
    }
    
    else
    {
	die "Invalid column id '$col_ids[0]'\n";
    }
    
    my @updates_all;
    my $matched_all = 0;
    my $unmatched_all = 0;
    
    # For each column, generate an SQL statement that will find all of the collections in
    # that column. We iterate over these collections to do the interpolation.

    foreach my $col ( @columns )
    {
	my $sql = "SELECT id as collection_id, col_id, unit_id, top_depth, bottom_depth,
			mid_depth, ma, ms_lith, ms_lith_2, ms_lith_att, ms_lith_att_2,
			max_interval_id, min_interval_id
		FROM $COLLECTIONS_TABLE
		WHERE col_id  = '$col'
		ORDER BY col_id, top_depth, bottom_depth";
	
	my $result = DBHashQuery($mstr, $sql);

	# Scan for collections that have no matching unit.
	
	foreach my $i ( 0..$#$result )
	{
	    my $collection_id = $result->[$i]{collection_id};
	    my $col_id = $result->[$i]{col_id};
	    my $mid_depth = $result->[$i]{mid_depth};
	    
	    next unless $collection_id > 0 && $result->[$i]{unit_id} == 0;
	    next if $result->[$i]{ms_lith};
	    
	    my $set_lith = 'sedimentary';
	    my $set_att = '';
	    my $set_lith_2 = '';
	    my $set_att_2 = '';
	    my ($set_interval, $set_ma, $depth_gap, $age_gap, $j, $k);
	    my ($up_unit, $up_depth, $up_lith_1, $up_att_1, $up_lith_2, $up_att_2,
		$up_interval, $up_ma);
	    my ($down_unit, $down_depth, $down_lith_1, $down_att_1, $down_lith_2, $down_att_2,
		$down_interval, $down_ma);
	    my ($j, $k);
	    
	    # When we find one, scan upwards and downwards until we find the closest
	    # unit in each direction.
	    
	    for ($j = $i-1; $j >= 0; $j--)
	    {
		last if $j < 0 || $result->[$j]{unit_id} > 0;
	    }
	    
	    if ( $j >= 0 && $result->[$j]{unit_id} > 0 )
	    {
		$up_unit = $result->[$j]{unit_id};
		$up_depth = $result->[$j]{mid_depth};
		$up_lith_1 = $result->[$j]{ms_lith};
		$up_att_1 = $result->[$j]{ms_lith_att};
		$up_lith_2 = $result->[$j]{ms_lith_2};
		$up_att_2 = $result->[$j]{ms_lith_att_2};
		$up_interval = $result->[$j]{max_interval_id};
		$up_ma = $result->[$j]{ma};
	    }
	    
	    for ($k = $i+1; $k <= $#$result; $k++)
	    {
		last if $result->[$k]{unit_id} > 0;
	    }
	    
	    if ( $k <= $#$result && $result->[$k]{unit_id} > 0 )
	    {
		$down_unit = $result->[$k]{unit_id};
		$down_depth = $result->[$k]{mid_depth};
		$down_lith_1 = $result->[$k]{ms_lith};
		$down_att_1 = $result->[$k]{ms_lith_att};
		$down_lith_2 = $result->[$k]{ms_lith_2};
		$down_att_2 = $result->[$k]{ms_lith_att_2};
		$down_interval = $result->[$k]{min_interval_id} || $result->[$k]{max_interval_id};
		$down_ma = $result->[$k]{ma};
	    }
	    
	    if ( $up_lith_1 && ($up_lith_1 eq $down_lith_1 || $up_lith_1 eq $down_lith_2) )
	    {
		$set_lith = $up_lith_1;
		$set_att = &CommonAtts($up_att_1, ($up_lith_1 eq $down_lith_1) ? $down_att_1
				       : $down_att_2);
		
		if ( $up_lith_2 && ($up_lith_2 eq $down_lith_1 || $up_lith_2 eq $down_lith_2) )
		{
		    $set_lith_2 = $up_lith_2;
		    $set_att_2 = &CommonAtts($up_att_2, ($up_lith_2 eq $down_lith_1) ? $down_att_1
						 : $down_att_2);
		}
	    }
	    
	    elsif ( $up_lith_2 && ($up_lith_2 eq $down_lith_1 || $up_lith_2 eq $down_lith_2) )
	    {
		$set_lith = $up_lith_2;
		$set_att = &CommonAtts($up_att_2, ($up_lith_2 eq $down_lith_1) ? $down_att_1
				       : $down_att_2);
	    }
	    
	    if ( $up_interval eq $down_interval )
	    {
		$set_interval = $up_interval;
	    }
	    
	    else
	    {
		$set_interval = 0;
	    }
	    
	    if ( defined $up_ma && $up_ma ne '' && $down_ma )
	    {
		$set_ma = $up_ma + ($down_ma - $up_ma) * ($mid_depth - $up_depth) /
		    ($down_depth - $up_depth);
		
		$set_ma = int($set_ma * 1000 + 0.5) / 1000;
		
		$depth_gap = int(($down_depth - $up_depth) * 1000 + 0.5) / 1000;
		$age_gap = int(($down_ma - $up_ma) * 1000 + 0.5) / 1000;
		
		if ( $age_gap <= $INTP_AGE_LIMIT )
		{
		    push @updates_all, [$col_id, $collection_id, $set_interval, $set_ma,
					$set_lith, $set_att, $set_lith_2, $set_att_2,
					$depth_gap, $age_gap];
		    
		    $matched_all++;
		    
		    $max_depth_gap = $depth_gap if ! $max_depth_gap || $depth_gap > $max_depth_gap;
		    $max_age_gap = $age_gap if ! $max_age_gap || $age_gap > $max_age_gap;
		}
		
		else
		{
		    if ( $subcommand eq 'show' )
		    {
			push @updates_all, [$col_id, $collection_id, 'gap too large', '',
					    '', '', '', '', $depth_gap, $age_gap];
		    }
		    
		    $unmatched_all++;
		}
	    }
	    
	    else
	    {
		if ( $subcommand eq 'show' )
		{
		    my $reason = $up_ma && $up_ma ne '' ? 'missing down' : 'missing up';
		    push @updates_all, [$col_id, $collection_id, $reason, '',
					'', '', '', '', '', ''];
		}
		
		$unmatched_all++;
	    }
	}
    }
    
    if ( $subcommand eq 'show' )
    {
	my $output = FormatTable(['col_id', 'collection_id', 'interval', 'ma',
				  'ms_lith', 'lith_att', 'ms_lith_2', 'lith_att_2',
				  'depth gap', 'age gap'],
				 @updates_all);
	
	my $rows = scalar(@updates_all);
	
	$output .= "Interpolated $matched_all collections\n";
	$output .= "No match for $unmatched_all collections\n";
	$output .= "Max depth gap = $max_depth_gap\n";
	$output .= "Max age gap = $max_age_gap\n";
	
	print $output;
    }

    elsif ( $subcommand eq 'check' || $subcommand eq 'do' )
    {
	my $updated = 0;
	
	foreach my $u ( @updates_all )
	{
	    my ($col_id, $collection_id, $interval, $ma, $ms_lith, $lith_att,
		$ms_lith_2, $lith_att_2) = $u->@*;
	    
	    my $sql = "UPDATE $COLLECTIONS_TABLE
		SET max_interval_id = '$interval', ma = '$ma',
		    ms_lith = '$ms_lith', ms_lith_att = '$lith_att',
		    ms_lith_2 = '$ms_lith_2', ms_lith_att_2 = '$lith_att_2'
		WHERE id = '$collection_id'";
	    
	    my $result = DBCommand($mstr, $sql);
	    $updated += $result;
	}
	
	my $sql = "UPDATE $COLLECTIONS_TABLE
		SET ma_direct = ma + 0
		WHERE has_age";
	
	my $result = DBCommand($mstr, $sql);
    }
}


# CommonAtts ( attributes1, attributes2 )
#
# Return a comma-separated list of all the attributes common to $attributes1 and
# $attributes2.

sub CommonAtts {

    my ($att_list_1, $att_list_2) = @_;
    
    my %attr1 = map { $_ => 1 } split /\s*,\s*/, $att_list_1;
    my %attr2 = map { $_ => 1 } split /\s*,\s*/, $att_list_2;
    my %common;
    
    foreach my $k ( keys %attr1 )
    {
	$common{$k} = 1 if $attr2{$k};
    }

    return join(',', keys %common);
}


    # 	my $current_col = 0;
    # 	my $updated = 0;
    # 	my $skipped = 0;
    # 	my (%core_unit, %core_lith);
	
    # 	foreach my $r ( @$colsects )
    # 	{
    # 	    if ( $r->{col_id} != $current_col )
    # 	    {
    # 		$current_col = $r->{col_id};
    # 		%core_unit = ();
    # 		%core_lith = ();
    # 	    }

    # 	    my $collection_id = $r->{collection_id};
    # 	    my $core_no = $r->{core};
    # 	    my $unit_id = $r->{unit_id};
    # 	    my $lith = $r->{pbdb_lith};

    # 	    if ( $unit_id == 0 )
    # 	    {
    # 		my @prev_liths = $core_lith{$core_no} ? keys $core_lith{$core_no}->%* : ();
    # 		my $prev_unit = $core_unit{$core_no};
		
    # 		if ( $prev_unit && @prev_liths == 1 )
    # 		{
    # 		    my $result = DBCommand($mstr, "UPDATE $COLLECTIONS_TABLE SET unit_id = '$prev_unit'
    # 		    WHERE id = '$collection_id'");
		    
    # 		    $updated += $result;
    # 		}
		
    # 		elsif ( $prev_unit )
    # 		{
    # 		    say "Could not update collection $collection_id in column $current_col: multiple lithologies";
    # 		    $skipped++;
    # 		}
		
    # 		else
    # 		{
    # 		    say "Could not update collection $collection_id in column $current_col: no previous unit";
    # 		    $skipped++;
    # 		}
    # 	    }
	    
    # 	    else
    # 	    {
    # 		$core_unit{$core_no} = $unit_id;
    # 		$core_lith{$core_no}{$lith} = 1;
    # 	    }
    # 	}
	
    # 	say "\nUpdated $updated collections.\nSkipped $skipped collections.\n";
    # }

    # else
    # {
    # 	say "\nNothing to update.\n";
    # }


sub CompareLatLng {
    
    my $dbresult = $mstr->selectall_arrayref("SELECT $COLLECTIONS_TABLE.id, lat, lng \
	FROM $COLLECTIONS_TABLE JOIN cols on $COLLECTIONS_TABLE.col_id = cols.id");
    
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


sub ShowColumn {

    my ($column_id) = @_;

    unless ( defined $column_id && $column_id > 0 )
    {
	die "You must specify a column identifier\n";
    }

    my $sql = "SELECT col_id, unit_id, sect, min(top_depth) as top_depth,
		max(bottom_depth) as bottom_depth, count(*) as n_colls,
		group_concat(distinct ms_lith) as lith
	FROM $COLLECTIONS_TABLE
	WHERE col_id = $column_id and unit_id > 0 group by unit_id
	UNION SELECT u.col_id, u.id as unit_id, s.sect, position_top as top_depth,
		position_bottom as bottom_depth, 0 as n_colls, l.lith
	FROM units as u left
	     join unit_liths as ul on ul.unit_id = u.id join liths as l on l.id = ul.lith_id
	     left join offshore_sections as s on s.col_id = u.col_id and s.top_mbsf <= u.position_top
			and s.bottom_mbsf >= u.position_bottom
	     left join $COLLECTIONS_TABLE as c on c.col_id = u.col_id and c.unit_id = u.id
	WHERE u.col_id = $column_id and c.unit_id is null
	UNION SELECT c.col_id, c.unit_id, c.sect, top_depth, bottom_depth, count(*) as n_colls,
		'?' as lith
	FROM $COLLECTIONS_TABLE as c
	WHERE col_id = $column_id and unit_id = 0
	GROUP BY top_depth, bottom_depth
	ORDER BY top_depth, bottom_depth;";
    
    my $result = DBArrayQuery($mstr, $sql);
    
    my $output = FormatTable(['col_id', 'unit_id', 'section', 'top_depth',
			      'bottom_depth', 'n_colls', 'lith'], @$result);

    print $output;
}


sub ShowMissing {

    my (@args) = @_;
    
    my $limit_clause = '';
    
    if ( $args[0] && $args[0] =~ /limit=(\d+)/ )
    {
	$limit_clause = "LIMIT $1";
    }
    
    my $sql = "SELECT col_id, count(*) as n_colls
	FROM $COLLECTIONS_TABLE
	WHERE ms_lith = ''
	GROUP BY col_id $limit_clause";
    
    my $result = DBArrayQuery($mstr, $sql);
    
    my $output = FormatTable(['col_id', 'n_colls'], @$result);
    
    my $rows = scalar(@$result);
    $output .= "$rows rows.\n";
    
    print $output;
}


sub UpdateAges {

    my ($subcommand, @args) = @_;
    
    my ($sql, $result);
    
    if ( $subcommand eq 'update' )
    {
	$sql = "UPDATE $COLLECTIONS_TABLE
		SET has_age = 0, ma = null, ma_direct = '',
		    max_interval_no = null, min_interval_no = null";
	
	$result = DBCommand($mstr, $sql);
	
	$sql = "UPDATE $COLLECTIONS_TABLE
		SET ma = 0.0, has_age = 1
		WHERE mid_depth = 0 and unit_id > 0";
	
	$result = DBCommand($mstr, $sql);

	$sql = "UPDATE $COLLECTIONS_TABLE as c
		    join unit_boundaries as bmin on bmin.unit_id = c.unit_id and bmin.unit_id_2 = 0
		SET max_interval_no = bmin.t1
		WHERE mid_depth = 0 and c.unit_id > 0";
	
	$result = DBCommand($mstr, $sql);
	
	$sql = "UPDATE $COLLECTIONS_TABLE as c join units as u on u.id = c.unit_id
		    join unit_boundaries as bmin using (unit_id)
		    join unit_boundaries as bmax on bmax.unit_id_2 = c.unit_id
		SET ma = bmax.t1_age - (bmax.t1_age - bmin.t1_age) *
			(position_bottom - mid_depth) / (position_bottom - position_top),
		    has_age = 1, min_interval_no = bmin.t1, max_interval_no = bmax.t1
		WHERE c.mid_depth > 0 and c.unit_id > 0";
	
	$result = DBCommand($mstr, $sql);
	
	$sql = "UPDATE $COLLECTIONS_TABLE
		SET ma_direct = ma + 0
		WHERE has_age";
	
	$result = DBCommand($mstr, $sql);
    }

    elsif ( $subcommand eq 'check' )
    {
	$sql = "SELECT distinct c.col_id, c.id as collection_id, c.ma,
		    if(c.ma > imax.age_bottom, 'B', 'T') as dir
		FROM $COLLECTIONS_TABLE as c
		    join intervals as imax on imax.id = c.max_interval_id
		    left join intervals as imin on imin.id = c.min_interval_id
		WHERE c.ma > imax.age_bottom or c.ma < coalesce(imin.age_top, imax.age_top)
		ORDER BY col_id, top_depth, bottom_depth";
	
	$result = DBArrayQuery($mstr, $sql);
	
	my $output = FormatTable(['col_id', 'collection_id', 'ma', 'dir'], @$result);
	my $rows = scalar(@$result);
	
	$output .= "Found $rows units where the age was outside the max/min interval\n\n";
	
	# $sql = "SELECT distinct col_id, unit_id FROM $COLLECTIONS_TABLE as c
	# 	    join intervals as imin on imin.id = c.min_interval_id
	# 	WHERE c.ma < imin.age_top
	# 	ORDER BY col_id, top_depth, bottom_depth";
	
	# $result = DBArrayQuery($mstr, $sql);
	
	# $output .= FormatTable(['col_id', 'unit_id'], @$result);
	# $rows = scalar(@$result);
	
	# $output .= "Found $rows units where the age was outside the min interval\n\n";
	
	print $output;
    }
}


sub UpdateGeneraReport {

    my ($subcommand) = @_;
    
    my $REPORT = "pbdb.age_check_genera";
    
    my $sql = "UPDATE $REPORT SET eodp_min = null, eodp_max = null, f0 = null, f1 = null";
    
    DBCommand($mstr, $sql);
    
    $sql = "UPDATE $REPORT as r join
		(SELECT o.genus_name, min(c.ma) as min_ma, max(c.ma) as max_ma
		 FROM $OCCURRENCES_TABLE as o join $COLLECTIONS_TABLE as c on c.id = o.sample_id
		 GROUP BY o.genus_name) as o on r.taxon_name = o.genus_name
	    SET r.eodp_min = o.min_ma, r.eodp_max = o.max_ma";
    
    DBCommand($mstr, $sql);
    
    $sql = "UPDATE $REPORT SET f0 = '*' WHERE source_min > 0 and eodp_min < source_min - 1.0";
    
    DBCommand($mstr, $sql);
    
    $sql = "UPDATE $REPORT SET f0 = '**' WHERE source_min > 0 and eodp_min < source_min - 10.0";
    
    DBCommand($mstr, $sql);
    
    $sql = "UPDATE $REPORT SET f1 = '*' WHERE source_max > 0 and eodp_max > source_max + 1.0";
    
    DBCommand($mstr, $sql);
    
    $sql = "UPDATE $REPORT SET f1 = '**' WHERE source_max > 0 and eodp_max > source_max + 10.0";
    
    DBCommand($mstr, $sql);
    
    $sql = "UPDATE $REPORT SET f1 = '*' WHERE source_min > 0 and eodp_max < source_min";
    
    DBCommand($mstr, $sql);

    $sql = "UPDATE $REPORT SET f1 = '**' WHERE source_min > 0 and eodp_max < source_min - 10.0";
    
    DBCommand($mstr, $sql);
}


sub SetPBDBLiths {
    
    my $sql = "UPDATE $COLLECTIONS_TABLE as c join pbdb_lith_map as m using (ms_lith)
	    SET c.pbdb_lith = m.pbdb_lith, c.pbdb_lithification = m.pbdb_lithification";

    DBCommand($mstr, $sql);

    $sql = "UPDATE $COLLECTIONS_TABLE as c join pbdb_lith_map as m on m.ms_lith = c.ms_lith_2
	    SET c.pbdb_lith_2 = m.pbdb_lith, c.pbdb_lithification_2 = m.pbdb_lithification";
    
    DBCommand($mstr, $sql);
}


sub SetPBDBAttrs {

    my ($subcommand) = @_;
    
    my $sql;
    
    $sql = "SHOW COLUMNS FROM collections LIKE 'minor_lithology'";
    
    my ($field, $type) = $pbdb->selectrow_array($sql);
    
    my %minor_lith = map { $_ => $_ } $type =~ /'(.*?)'/g;
    
    $minor_lith{cherty} = 'cherty/siliceous';
    $minor_lith{siliceous} = 'cherty/siliceous';
    
    $sql = "SHOW COLUMNS FROM collections LIKE 'lithadj'";
    
    ($field, $type) = $pbdb->selectrow_array($sql);
    
    my %lith_adj = map { $_ => $_ } $type =~ /'(.*?)'/g;
    
    $sql = "SELECT distinct ms_lith_att FROM $COLLECTIONS_TABLE WHERE ms_lith_att <> ''
	    UNION
	    SELECT distinct ms_lith_att_2 FROM $COLLECTIONS_TABLE WHERE ms_lith_att_2 <> ''";
    
    my @lith_att_values = DBColumnQuery($mstr, $sql);
    
    my %attrs;
    
    foreach my $v ( @lith_att_values )
    {
	foreach my $a ( split /,/, $v )
	{
	    $attrs{$a} = 1;
	}
    }
    
    foreach my $k ( sort keys %attrs )
    {
	$lith_adj{$k} = 'bioturbation' if $k =~ /bioturbated/;
	$lith_adj{$k} = 'brown' if $k =~ /brown/;
	$lith_adj{$k} = 'gray' if $k =~ /gray/;
	$lith_adj{$k} = 'shelly/skeletal' if $k =~ /shelly|skeletal/;
	$lith_adj{$k} = 'diatomaceous' if $k eq 'diatom';
    }

    if ( $subcommand eq 'show' )
    {
	foreach my $k ( sort keys %attrs )
	{
	    my $minorlith = $minor_lith{$k} // '';
	    my $lithadj = $lith_adj{$k} // '';
	    say sprintf("%-20s%-20s%-20s", $k, $minorlith, $lithadj);
	}
    }
    
    elsif ( $subcommand eq 'generate' )
    {
	DBCommand($mstr, "DELETE FROM pbdb_attr_map");
	
	$sql = "SELECT distinct ms_lith_att FROM $COLLECTIONS_TABLE WHERE ms_lith_att <> ''
		UNION
		SELECT distinct ms_lith_att_2 FROM $COLLECTIONS_TABLE WHERE ms_lith_att_2 <> ''";
	
	my @lith_att_values = DBColumnQuery($mstr, $sql);
	
	foreach my $v ( @lith_att_values )
	{
	    my $qv = $mstr->quote($v);
	    
	    my %minors = map { $minor_lith{$_} => 1 } grep { $minor_lith{$_} } split /,/, $v;
	    my $qminorlith = "null";
	    $qminorlith = $mstr->quote(join(',', keys %minors)) if %minors;
	    
	    my %adjs = map { $lith_adj{$_} => 1 } grep { $lith_adj{$_} } split /,/, $v;
	    my $qlithadj = "null";
	    $qlithadj = $mstr->quote(join(',', keys %adjs)) if %adjs;
	    
	    $sql = "INSERT INTO pbdb_attr_map (ms_lith_att, pbdb_minor_lith, pbdb_lith_adj)
		VALUES ($qv, $qminorlith, $qlithadj)";
	    
	    DBCommand($mstr, $sql);
	}
    }
    
    elsif ( $subcommand eq 'set' )
    {
	$sql = "UPDATE $COLLECTIONS_TABLE as c join pbdb_attr_map as m using (ms_lith_att)
		SET c.pbdb_minor_lith = m.pbdb_minor_lith,
		    c.pbdb_lith_adj = m.pbdb_lith_adj";
	
	DBCommand($mstr, $sql);

	$sql = "UPDATE $COLLECTIONS_TABLE as c join pbdb_attr_map as m on
			m.ms_lith_att = c.ms_lith_att_2
		SET c.pbdb_minor_lith_2 = m.pbdb_minor_lith,
		    c.pbdb_lith_adj_2 = m.pbdb_lith_adj";
	
	DBCommand($mstr, $sql);

	$sql = "UPDATE $COLLECTIONS_TABLE
		SET pbdb_lith_2 = null, pbdb_minor_lith_2 = null,
		    pbdb_lith_adj_2 = null, pbdb_lithification_2 = null
		WHERE pbdb_lith_2 = ''";
	
	DBCommand($mstr, $sql);
    }
}


sub SetPBDBIntervals {
    
    my ($subcommand) = @_;
    
    my $sql = "UPDATE $COLLECTIONS_TABLE as c join intervals as i on i.id = c.max_interval_id
		left join pbdb.interval_data as pi using (interval_name)
	    SET c.pbdb_max_interval_no = pi.interval_no";
    
    DBCommand($mstr, $sql);
    
    $sql = "UPDATE $COLLECTIONS_TABLE as c join intervals as i on i.id = c.min_interval_id
		left join pbdb.interval_data as pi using (interval_name)
	    SET c.pbdb_min_interval_no = pi.interval_no";
    
    DBCommand($mstr, $sql);
    
    $sql = "UPDATE $COLLECTIONS_TABLE
	    SET pbdb_min_interval_no = null
	    WHERE pbdb_max_interval_no = pbdb_min_interval_no";
    
    DBCommand($mstr, $sql);
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
# 	FROM offshore_occs as o JOIN $COLLECTIONS_TABLE as c on o.sample_id = c.id
# 	    JOIN pbdb.age_check_genera as a on a.taxon_name = o.genus_name
# 	WHERE c.bottom_depth > 0 and a.source_min > 0");
    
#     DBCommand($mstr, "
# 	INSERT INTO offshore_constraints (col_id, section_id, unit_id, 
# 		depth, relation, bound, type, label)
# 	SELECT c.col_id, c.section_id, c.unit_id, c.top_depth, 'lt', a.source_max,
# 		'genus', a.taxon_name
# 	FROM offshore_occs as o JOIN $COLLECTIONS_TABLE as c on o.sample_id = c.id
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
    
    ($count) = DBRowQuery($mstr, "SELECT count(*) FROM $OCCURRENCES_TABLE WHERE genus_name = ''");
    
    if ( $count > 0 )
    {
	die "You must execute the 'adjust' subcommand first.\n";
    }
    
    say "Starting transaction...";
    
    $pbdb->begin_work or die $pbdb->errstr;
    
    # Insert the collections
    
    DBCommand($pbdb, <<END_STMT);
INSERT INTO $TABLE{COLLECTION_DATA} (authorizer, enterer, authorizer_no, enterer_no,
	upload, upload_id, research_group, license, reference_no, collection_name, country,
	lat, lng, latlng_basis, gps_datum, geogscale, 
	localsection, localbed, localbedunit, localorder, stratscale, environment, tectonic_setting,
	pres_mode, assembl_comps, collection_type, collection_coverage, coll_meth, access_level,
	max_interval_no, min_interval_no, direct_ma, direct_ma_unit, direct_ma_method,
	lithology1, fossilsfrom1, lithification, 
	lithadj, minor_lithology, lithology2, fossilsfrom2, lithification2, lithadj2,
	minor_lithology2,
	collectors, preservation_quality, fragmentation, abund_in_sediment, taxonomy_comments)
SELECT 'A. Fraass', 'S. Peters', '919', '136', 'eODP', oc.id,
	'eODP', 'CC BY', '82981', coll_name, country, 
	cols.lat, cols.lng, 'stated in text', 'WGS84', 'hand sample',
	site_hole, mid_depth, 'mbsf', 'top to bottom', 'bed', 'basinal (carbonate)', 'deep ocean basin',
	'body', 'microfossils', 'biostratigraphic', 'some microfossils', 'core', 'the public',
	pbdb_max_interval_no, pbdb_min_interval_no, round(ma, 2), 'Ma', 'age-depth',
	if(pbdb_lith<>'',pbdb_lith,'not reported'), 'Y', pbdb_lithification,
        pbdb_lith_adj, pbdb_minor_lith,	pbdb_lith_2, if(pbdb_lith_2<>'', 'Y', NULL),
	pbdb_lithification_2, pbdb_lith_adj_2, pbdb_minor_lith_2,
	'IODP', pbdb_pres, pbdb_frag, pbdb_abund, data_source_notes
FROM macrostrat.$COLLECTIONS_TABLE as oc
	JOIN macrostrat.cols as cols on col_id=cols.id
WHERE taxa_count > 0 and ms_lith <> ''
END_STMT
    
    # Insert the newly added collections into the `coll_units` table
    
    DBCommand($pbdb, <<END_STMT);
INSERT INTO $TABLE{COLLECTION_UNITS} (collection_no, col_id, unit_id)
SELECT c.collection_no, oc.col_id, oc.unit_id
FROM collections as c JOIN macrostrat.$COLLECTIONS_TABLE as mc
	on mc.id = c.upload_id and c.upload = 'eODP'
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
INSERT INTO $TABLE{OCCURRENCE_DATA} (authorizer, enterer, authorizer_no, enterer_no,
	upload, upload_id,
	reference_no, collection_no, taxon_no, genus_reso, genus_name, subgenus_name, 
	species_reso, species_name, subspecies_reso, subspecies_name, 
	abund_value, abund_unit, comments)
SELECT 'A. Fraass', 'S. Peters', '919', '136', 'eODP', oo.id, '82981',
	collection_no, pbdb_taxon_id, genus_modifier, genus_name, subgenus_name,
	species_modifier, species_name, subspecies_modifier, subspecies_name,
	cleaned_code, code_unit, comments
FROM macrostrat.$OCCURRENCES_TABLE as oo
	JOIN collections as c on oo.sample_id = c.upload_id and c.upload='eODP'
END_STMT
    
    $pbdb->commit;
}


sub UpdateData {

    my (@sections) = @_;
    
    my %selector = map { $_ => 1 } grep { $_ } @sections;
    
    my $updates = 0;
    
    say "Starting transaction...";
    
    $pbdb->begin_work or die $pbdb->errstr;
    
    # Update the collections
    
    if ( $selector{countries} || $selector{colls} )
    {
	$updates++;
	DBCommand($pbdb, <<END_STMT);
UPDATE $TABLE{COLLECTION_DATA} as c
	join macrostrat.$COLLECTIONS_TABLE as mc on c.upload_id = mc.id and c.upload = 'eODP'
SET c.country = mc.country
END_STMT
    }
    
    if ( $selector{latlng} || $selector{colls} )
    {
	$updates++;
	DBCommand($pbdb, <<END_STMT);
UPDATE $TABLE{COLLECTION_DATA} as c
	join macrostrat.$COLLECTIONS_TABLE as mc on c.upload_id = mc.id and c.upload = 'eODP'
	join macrostrat.cols as cols on mc.col_id = cols.id
SET c.lat = cols.lat,
    c.lng = cols.lng
END_STMT
	
	DBCommand($pbdb, <<END_STMT);
UPDATE $TABLE{COLLECTION_DATA}
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
    }
    
    if ( $selector{depths} || $selector{colls} )
    {
	$updates++;
	DBCommand($pbdb, <<END_STMT);
UPDATE $TABLE{COLLECTION_DATA} as c
	join macrostrat.$COLLECTIONS_TABLE as mc on c.upload_id = mc.id and c.upload = 'eODP'
	join macrostrat.cols as cols on mc.col_id = cols.id
SET c.localbed = mc.mid_depth
END_STMT
    }
    
    if ( $selector{intervals} || $selector{colls} )
    {
	$updates++;
	DBCommand($pbdb, <<END_STMT);
UPDATE $TABLE{COLLECTION_DATA} as c
	join macrostrat.$COLLECTIONS_TABLE as mc on c.upload_id = mc.id and c.upload = 'eODP'
SET c.max_interval_no = mc.pbdb_max_interval_no,
    c.min_interal_no = mc.pbdb_min_interval_no,
    c.direct_ma = round(mc.ma, 2)
END_STMT
    }
    
    if ( $selector{lithologies} || $selector{colls} )
    {
	$updates++;
	DBCommand($pbdb, <<END_STMT);
UPDATE $TABLE{COLLECTION_DATA} as c
	join macrostrat.$COLLECTIONS_TABLE as mc on c.upload_id = mc.id and c.upload = 'eODP'
SET c.lithology1 = if(mc.pbdb_lith<>'', mc.pbdb_lith, 'not reported'),
    c.lithification = mc.pbdb_lithification,
    c.lithadj = mc.pbdb_lith_adj,
    c.lithification = mc.pbdb_minor_lith,
    c.lithology2 = mc.pbdb_lith_2,
    c.fossilsfrom2 = if(pbdb_lith_2<>'', 'Y', NULL),
    c.lithification2 = mc.pbdblithification_2,
    c.lithadj2 = mc.pbdb_lith_adj_2,
    c.minor_lithology2 = mc.pbdb_minor_lith_2
END_STMT
    }
    
    if ( $selector{preservation} || $selector{colls} )
    {
	$updates++;
	DBCommand($pbdb, <<END_STMT);
UPDATE $TABLE{COLLECTION_DATA} as c
	join macrostrat.$COLLECTIONS_TABLE as mc on c.upload_id = mc.id and c.upload = 'eODP'
SET c.preservation_quality = mc.pbdb_pres,
    c.fragmentation = mc.pbdb_frag,
    c.abund_in_sediment = mc.pbdb_abund
END_STMT
    }
    
    if ( $selector{comments} || $selector{colls} )
    {
	$updates++;
	DBCommand($pbdb, <<END_STMT);
UPDATE $TABLE{COLLECTION_DATA} as c
	join macrostrat.$COLLECTIONS_TABLE as mc on c.upload_id = mc.id and c.upload = 'eODP'
SET c.taxonomy_comments = mc.data_source_notes
END_STMT
    }
    
    if ( $selector{units} || $selector{colls} )
    {
	$updates++;
	DBCommand($pbdb, <<END_STMT);
UPDATE $TABLE{COLLECTION_UNITS} as cu join $TABLE{COLLECTION_DATA} as c
	    on cu.collection_no = c.collection_no and c.upload = 'eODP'
	join macrostrat.$COLLECTIONS_TABLE as mc on c.upload_id = mc.id
SET cu.unit_id = mc.unit_id,
    cu.col_id = mc.col_id
END_STMT
    }
    
    # Update occurrences
    
    if ( $selector{taxon_names} || $selector{'taxon-names'} || $selector{occs} )
    {
	$updates++;
	DBCommand($pbdb, <<END_STMT);
UPDATE $TABLE{OCCURRENCE_DATA} as o
	join macrostrat.$OCCURRENCES_TABLE as oo on o.upload_id = oo.id and o.upload = 'eODP'
SET o.genus_reso = oo.genus_modifier,
    o.genus_name = oo.genus_name,
    o.subgenus_reso = oo.subgenus_modifier,
    o.subgenus_name = oo.subgenus_name,
    o.species_reso = oo.species_modifier,
    o.species_name = oo.species_name,
    o.subspecies_reso = oo.subspecies_modifier,
    o.subspecies_name = oo.subspecies_name
END_STMT
    }
    
    if ( $selector{taxon_nos} || $selector{'taxon-nos'} || $selector{occs} )
    {
	$updates++;
	DBCommand($pbdb, <<END_STMT);
UPDATE $TABLE{OCCURRENCE_DATA} as o
	join macrostrat.$OCCURRENCES_TABLE as oo on o.upload_id = oo.id and o.upload = 'eODP'
SET o.taxon_no = oo.pbdb_taxon_id
END_STMT
    }
    
    if ( $selector{abundances} || $selector{occs} )
    {
	$updates++;
	DBCommand($pbdb, <<END_STMT);
UPDATE $TABLE{OCCURRENCE_DATA} as o
	join macrostrat.$OCCURRENCES_TABLE as oo on o.upload_id = oo.id and o.upload = 'eODP'
SET o.abund_value = oo.cleaned_code,
    o.abund_unit = oo.code_unit
END_STMT
    }
    
    if ( $selector{occ_comments} || $selector{'occ-comments'} || $selector{occs} )
    {
	$updates++;
	DBCommand($pbdb, <<END_STMT);
UPDATE $TABLE{OCCURRENCE_DATA} as o
	join macrostrat.$OCCURRENCES_TABLE as oo on o.upload_id = oo.id and o.upload = 'eODP'
SET o.comments = oo.comments
END_STMT
    }
    
    if ( $updates )
    {
	$pbdb->commit;
    }

    else
    {
	say "You did not specify a valid section to update";
    }
}


sub RemoveData {
    
    say "Removing eODP data from the PBDB...";
    
    DBCommand($pbdb, "DELETE FROM $TABLE{OCCURRENCE_DATA} WHERE upload='eODP'");
    
    my ($next) = DBRowQuery($pbdb, "SELECT max(occurrence_no)+1 FROM $TABLE{OCCURRENCE_DATA}");
    
    DBCommand($pbdb, "ALTER TABLE $TABLE{OCCURRENCE_DATA} AUTO_INCREMENT = $next");
    
    DBCommand($pbdb, "DELETE FROM $TABLE{COLLECTION_UNITS} as cu
			  join $TABLE{COLLECTION_DATA} as c
			  on cu.collection_no = c.collection_no and c.upload = 'eODP'");
    
    ($next) = DBRowQuery($pbdb, "SELECT max(collection_no)+1 FROM $TABLE{COLLECTION_DATA}");
    
    DBCommand($pbdb, "ALTER TABLE $TABLE{COLLECTION_DATA} AUTO_INCREMENT = $next");
}


# sub AddUnits {
    
#     my (@gapcols) = DBColumnQuery($mstr, "SELECT distinct col_id FROM $COLLECTIONS_TABLE
# 					WHERE ma = ''");
    
#     foreach my $col_id ( @gapcols )
#     {
# 	my $sql = "
# 	SELECT col_id, unit_id, min(top_depth) as top_depth, max(bottom_depth) as bottom_depth,
# 	    count(*) as n_colls, group_concat(distinct ms_lith) as lith
# 	FROM $COLLECTIONS_TABLE
# 	WHERE col_id = '$col_id' and unit_id > 0 GROUP BY unit_id
# 	UNION SELECT u.col_id, u.id as unit_id, position_top as top_depth,
# 	    position_bottom as bottom_depth, 0 as n_colls, l.lith
# 	FROM units as u LEFT JOIN unit_liths as ul on ul.unit_id = u.id
# 		JOIN liths as l on l.id = ul.lith_id
# 		LEFT JOIN $COLLECTIONS_TABLE as c on c.col_id = u.col_id and c.unit_id = u.id
# 	WHERE u.col_id = '$col_id' and c.unit_id is null
# 	UNION SELECT c.col_id, c.unit_id, top_depth, bottom_depth, count(*) as n_colls, '?' as lith
# 	FROM $COLLECTIONS_TABLE as c
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
    
    say STDERR "$query\n" if $opt_debug;
    
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


sub DBHashQuery {
    
    my ($dbh, $query) = @_;

    say STDERR "$query\n" if $opt_debug;
    
    my $dbresult = eval { $dbh->selectall_arrayref($query, { Slice => { } }) };
    
    if ( $@ )
    {
	my ($package, $filename, $line) = caller;
	    
	$@ .= "called from line $line of $filename\n";
	die $@;
    }
    
    elsif ( ref $dbresult eq 'ARRAY' )
    {
	return $dbresult;
    }

    else
    {
	return [ ];
    }
}


sub DBArrayQuery {
    
    my ($dbh, $query) = @_;

    say STDERR "$query\n" if $opt_debug;
    
    my $dbresult = $dbh->selectall_arrayref($query);

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
    
    say STDERR "$query\n" if $opt_debug;
    
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
    
    my ($dbh, $command, $silent) = @_;
    
    $command =~ s/\\/\\\\/g;
    $command =~ s/\n$//;
    
    say $command unless $silent;
    
    if ( $EXECUTE_MODE )
    {
	my $result;
	
	eval { $result = $dbh->do($command) };
	
	if ( defined $result && ! $@ && ! $silent )
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


sub FormatTable {
    
    my ($header, @rows) = @_;
    
    my @min_width;
    my @format;
    
    foreach my $row ( $header, @rows )
    {
	foreach my $col ( 0..$#$row )
	{
	    my $width = length($row->[$col]);
	    $min_width[$col] = $width if ! $min_width[$col] || $min_width[$col] < $width;
	}
    }
    
    my $output = FormatBoundary(\@min_width, \@format);
    $output .= FormatCells($header, \@min_width, \@format);
    $output .= FormatBoundary(\@min_width, \@format);
    
    foreach my $row ( @rows )
    {
	# $output .= FormatBoundary(\@min_width, \@format);
	$output .= FormatCells($row, \@min_width, \@format);
    }
    
    $output .= FormatBoundary(\@min_width, \@format);
    
    return $output;    
}


sub FormatBoundary {

    my ($min_width, $format) = @_;

    my $output = '+';

    foreach my $col ( 0..$#$min_width )
    {
	$output .= '-' x ($min_width->[$col] + 2);
	$output .= '+';
    }

    return "$output\n";
}


sub FormatCells {

    my ($row, $min_width, $format) = @_;
    
    my $output = '|';

    foreach my $col ( 0..$#$min_width )
    {
	my $width = $min_width->[$col];
	$output .= sprintf(" %-${width}s |", "$row->[$col]");
    }
    
    return "$output\n";
}
