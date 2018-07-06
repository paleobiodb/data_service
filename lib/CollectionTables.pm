# The Paleobiology Database
# 
#   CollectionTables.pm
# 

package CollectionTables;

use strict;

use base 'Exporter';

our (@EXPORT_OK) = qw(buildCollectionTables buildStrataTables buildLithTables
		      deleteProtLandData startProtLandInsert insertProtLandRecord finishProtLandInsert);

use Carp qw(carp croak);
use Try::Tiny;

use TableDefs qw($COLLECTIONS $COLL_MATRIX $COLL_LOC $COLL_BINS $COLL_STRATA $COLL_INTS $STRATA_NAMES
		 $MACROSTRAT_LITHS
		 $BIN_KEY $BIN_LOC $BIN_CONTAINER
		 $COUNTRY_MAP $CONTINENT_DATA
		 $PALEOCOORDS $GEOPLATES
		 $INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP $INTERVAL_BUFFER $COLL_LITH $COLL_ENV);
use CoreFunction qw(activateTables);
use ConsoleLog qw(logMessage);

our $COLL_MATRIX_WORK = "cmw";
our $COLL_BINS_WORK = "cbw";
our $COLL_STRATA_WORK = "csw";
our $STRATA_NAMES_WORK = "snw";
our $COLL_INTS_WORK = "ciw";

our $COLL_LITH_WORK = 'clw';
our $COLL_ENV_WORK = 'cew';

our $CLUST_AUX = "clust_aux";

our $PROTECTED_LAND = "protected_land";
our $PROTECTED_WORK = "pln";

# Constants

my $MOVE_THRESHOLD = 300;
my $MAX_ROUNDS = 15;

# buildCollectionTables ( dbh, cluster_flag, bin_list )
# 
# Compute the collection matrix.  If the $bin_list argument is not empty,
# also compute collection bin tables at the specified resolutions.  If
# $cluster_flag is true, then execute k-means clustering on any bin level
# for which that attribute is specified.

sub buildCollectionTables {

    my ($dbh, $bin_list, $options) = @_;
    
    my ($result, $sql);
    
    # Make sure that the country code lookup table is in the database.
    
    createCountryMap($dbh);
    
    # If we were given a list of bins, make sure it is formatted properly.
    
    my @bin_reso;
    my @bin_tables;
    
    my $bin_lines = '';
    my $parent_lines = '';
    my $next_line = '';
    my $level = 0;
    
    if ( ref $bin_list eq 'ARRAY' )
    {
	foreach my $bin (@$bin_list)
	{
	    next unless defined $bin->{resolution} && $bin->{resolution} > 0;
	    
	    $level++;
	    push @bin_reso, $bin->{resolution};
	    $bin_lines .= "bin_id_$level int unsigned not null,\n";
	    $next_line = "bin_id_$level int unsigned not null,\n";
	    $parent_lines .= $next_line;
	}
    }
    
    # Now create a clean working table which will become the new collection
    # matrix.
    
    logMessage(1, "Building collection tables");
    
    logMessage(2, "    copying enumerations from collections table...");
    
    my (@create) = $dbh->selectrow_array("SHOW CREATE TABLE `collections`");
    
    my ($env_enum) = $create[1] =~ qr{ `environment` \s+ ( enum [(] .*? [)] ) \s+ DEFAULT \s+ NULL }xmi;
    
    $env_enum ||= 'varchar(30)';
    
    logMessage(2, "    creating collection matrix...");
    
    $dbh->do("DROP TABLE IF EXISTS $COLL_MATRIX_WORK");
    
    $dbh->do("CREATE TABLE $COLL_MATRIX_WORK (
		collection_no int unsigned primary key,
		$bin_lines
		clust_id int unsigned not null,
		lng decimal(9,6),
		lat decimal(9,6),
		g_plate_no smallint unsigned not null,
		s_plate_no smallint unsigned not null,
		loc geometry not null,
		cc char(2),
		protected varchar(255),
		early_age decimal(9,5),
		late_age decimal(9,5),
		early_int_no int unsigned not null,
		late_int_no int unsigned not null,
		environment $env_enum default null,
		n_occs int unsigned not null,
		reference_no int unsigned not null,
		access_level tinyint unsigned not null) Engine=MYISAM");
    
    logMessage(2, "    inserting collections...");
    
    $sql = "	INSERT INTO $COLL_MATRIX_WORK
		       (collection_no, lng, lat, loc, cc,
			early_int_no, late_int_no, environment,
			reference_no, access_level)
		SELECT c.collection_no, c.lng, c.lat,
			if(c.lng is null or c.lat is null, point(1000.0, 1000.0), point(c.lng, c.lat)), 
			map.cc,
			c.max_interval_no, if(c.min_interval_no > 0, c.min_interval_no, c.max_interval_no),
			c.environment,
			c.reference_no,
			case c.access_level
				when 'database members' then if(c.release_date < now(), 0, 1)
				when 'research group' then if(c.release_date < now(), 0, 2)
				when 'authorizer only' then if(c.release_date < now(), 0, 2)
				else 0
			end
		FROM collections as c
			LEFT JOIN $COUNTRY_MAP as map on map.name = c.country";
    
    my $count = $dbh->do($sql);
    
    logMessage(2, "      $count collections");
    
    # Count the number of occurrences in each collection.
    
    logMessage(2, "    counting occurrences for each collection...");
    
    $sql = "UPDATE $COLL_MATRIX_WORK as m JOIN
		(SELECT collection_no, count(*) as n_occs
		FROM occurrences GROUP BY collection_no) as sum using (collection_no)
	    SET m.n_occs = sum.n_occs";
    
    $result = $dbh->do($sql);
    
    # Set the age boundaries for each collection.
    
    logMessage(2, "    setting age ranges...");
    
    $sql = "UPDATE $COLL_MATRIX_WORK as m
		JOIN $INTERVAL_DATA as ei on ei.interval_no = m.early_int_no
		JOIN $INTERVAL_DATA as li on li.interval_no = m.late_int_no
	    SET m.early_age = ei.early_age,
		m.late_age = li.late_age
	    WHERE ei.early_age >= li.early_age";
    
    $result = $dbh->do($sql);
    
    # Interchange the early/late intervals if they were given in the wrong order.
    
    $sql = "UPDATE $COLL_MATRIX_WORK as m
		JOIN $INTERVAL_DATA as ei on ei.interval_no = m.early_int_no
		JOIN $INTERVAL_DATA as li on li.interval_no = m.late_int_no
	    SET m.early_int_no = (\@tmp := early_int_no), early_int_no = late_int_no, late_int_no = \@tmp,
		m.early_age = li.early_age,
		m.late_age = ei.late_age
	    WHERE ei.early_age < li.early_age";
    
    $result = $dbh->do($sql);
    
    # Determine which collections fall into protected land, if that data is available.
    
    my ($prot_available) = eval {
	$dbh->selectrow_array("SELECT count(*) FROM protected_land");
    };
    
    if ( $prot_available )
    {
	logMessage(2, "    determining location information...");
	
	updateLocationTable($dbh);
	
	$result = $dbh->do("
		UPDATE $COLL_MATRIX_WORK as m JOIN $COLL_LOC as cl using (collection_no)
		SET m.protected = cl.protected");
    }
    
    else
    {
	logMessage(2, "    skipping protection status: table 'protected_land' not found");
    }
    
    # Setting plate_no using the Scotese model
    
    logMessage(2, "    setting geoplates using Scotese model...");
    
    $sql = "UPDATE $COLL_MATRIX_WORK as m JOIN $COLLECTIONS as cc using (collection_no)
	    SET m.s_plate_no = cc.plate";
    
    $result = $dbh->do($sql);
    
    # Setting paleocoordinates using GPlates, if available
    
    my ($paleo_available) = eval {
	$dbh->selectrow_array("SELECT count(*) from $PALEOCOORDS");
    };
    
    if ( $paleo_available )
    {
	logMessage(2, "    setting geoplates using GPlates...");
	
	$sql = "UPDATE $COLL_MATRIX_WORK as m JOIN $PALEOCOORDS as pc using (collection_no)
		SET m.g_plate_no = pc.plate_no";
	
	$result = $dbh->do($sql);
    }
    
    else
    {
	logMessage(2, "    skipping geoplates from GPlates: table '$PALEOCOORDS' not found");
    }
    
    # Assign the collections to bins at the various binning levels.
    
    if ( @bin_reso )
    {
	logMessage(2, "    assigning collections to bins...");
	
	$sql = "UPDATE $COLL_MATRIX_WORK SET";
	
	foreach my $i (0..$#bin_reso)
	{
	    my $level = $i + 1;
	    my $reso = $bin_reso[$i];
	    
	    next unless $level > 0 && $reso > 0;
	    
	    die "invalid resolution $reso: must evenly divide 180 degrees"
		unless int(180/$reso) == 180/$reso;
	    
	    logMessage(2, "      bin level $level: $reso degrees square");
	    
	    my $id_base = $reso < 1.0 ? $level . '00000000' : $level . '000000';
	    my $lng_base = $reso < 1.0 ? '10000' : '1000';
	    
	    $sql .= $i > 0 ? ",\n" : "\n";
	    
	    $sql .= "bin_id_$level = if(lng between -180.0 and 180.0 and lat between -90.0 and 90.0,
			$id_base + $lng_base * floor((lng+180.0)/$reso) + floor((lat+90.0)/$reso), 0)\n";
	}
	
	$result = $dbh->do($sql);
    }
    
    # Now that the table is full, we can add the necessary indices much more
    # efficiently than if we had defined them at the start.
    
    foreach my $i (0..$#bin_reso)
    {
	my $level = $i + 1;
	my $reso = $bin_reso[$i];
	
	next unless $level > 0 && $reso > 0;
	
	logMessage(2, "    indexing by bin level $level...");
	
	$result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (bin_id_$level)");
    }
    
    logMessage(2, "    indexing by geographic coordinates (spatial)...");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD SPATIAL INDEX (loc)");
    
    logMessage(2, "    indexing by country...");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (cc)");
    
    logMessage(2, "    indexing by reference_no...");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (reference_no)");
    
    logMessage(2, "    indexing by early and late age...");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (early_age)");
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (late_age)");
    
    logMessage(2, "    indexing by environment...");
    
    $result = $dbh->do("UPDATE $COLL_MATRIX_WORK SET environment = null
			WHERE environment = ''");
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (environment)");
    
    # We then create summary table for each binning level, counting the
    # number of collections and occurrences in each bin and computing the
    # centroid and age boundaries (adjusted to the standard intervals).
    
    logMessage(2, "    creating geography/time summary tables...");
    
    $dbh->do("DROP TABLE IF EXISTS $COLL_BINS_WORK");
    
    $dbh->do("CREATE TABLE $COLL_BINS_WORK (
		bin_id int unsigned not null,
		bin_level tinyint unsigned,
		$bin_lines
		interval_no int unsigned not null,
		n_colls int unsigned,
		n_occs int unsigned,
		early_age decimal(9,5),
		late_age decimal(9,5), 
		lng decimal(9,6),
		lat decimal(9,6),
		loc geometry not null,
		lng_min decimal(9,6),
		lng_max decimal(9,6),
		lat_min decimal(9,6),
		lat_max decimal(9,6),
		std_dev float,
		access_level tinyint unsigned not null,
		primary key (bin_id, interval_no)) Engine=MyISAM");
    
    $dbh->do("DROP TABLE IF EXISTS $COLL_INTS_WORK");
    
    $dbh->do("CREATE TABLE $COLL_INTS_WORK (
		collection_no int unsigned not null,
		interval_no int unsigned not null) Engine=MyISAM");
    
    my $set_lines = '';
    my @index_stmts;
    
    # Now summarize at each level in turn.
    
    foreach my $i (0..$#bin_reso)
    {
	my $level = $i + 1;
	my $reso = $bin_reso[$i];
	my $bin_str = 'bin_id';
	my $bin_select_str = "bin_id_$level";
	
	next unless $level > 0 && $reso > 0;
	
	if ( $level > 1 )
	{
	    foreach my $j (1..$level-1)
	    {
		$bin_str .= ", bin_id_$j";
		$bin_select_str .= ", bin_id_$j";
	    }
	}
	
	logMessage(2, "      summarizing at level $level by geography...");
	
	$sql = "INSERT IGNORE INTO $COLL_BINS_WORK
			($bin_str, bin_level, interval_no,
			 n_colls, n_occs, early_age, late_age, lng, lat,
			 lng_min, lng_max, lat_min, lat_max, std_dev,
			 access_level)
		SELECT $bin_select_str, $level, 0, count(*), sum(n_occs),
		       max(early_age), min(late_age),
		       avg(lng), avg(lat),
		       round(min(lng),5) as lng_min, round(max(lng),5) as lng_max,
		       round(min(lat),5) as lat_min, round(max(lat),5) as lat_max,
		       sqrt(var_pop(lng)+var_pop(lat)),
		       min(access_level)
		FROM $COLL_MATRIX_WORK as m
		GROUP BY bin_id_$level";
	
	# my $level = $i + 1;
	# my $reso = $bin_reso[$i];
	
	# next unless $level > 0 && $reso > 0;
	
	# logMessage(2, "      summarizing at level $level by geography...");
	
	# $sql = "INSERT IGNORE INTO $COLL_BINS_WORK
	# 		(bin_id, bin_level, interval_no,
	# 		 n_colls, n_occs, early_age, late_age, lng, lat,
	# 		 lng_min, lng_max, lat_min, lat_max, std_dev,
	# 		 access_level)
	# 	SELECT bin_id_$level, $level, 0, count(*), sum(n_occs),
	# 	       max(early_age), min(late_age),
	# 	       avg(lng), avg(lat),
	# 	       round(min(lng),5) as lng_min, round(max(lng),5) as lng_max,
	# 	       round(min(lat),5) as lat_min, round(max(lat),5) as lat_max,
	# 	       sqrt(var_pop(lng)+var_pop(lat)),
	# 	       min(access_level)
	# 	FROM $COLL_MATRIX_WORK as m
	# 	GROUP BY bin_id_$level";
	
	$result = $dbh->do($sql);
	
	logMessage(2, "      generated $result non-empty bins.");
	
	# Add a special row indicating the bin resolution for each level.
	
	my $coded_reso = 360 / $reso;
	
	$sql = "REPLACE INTO $COLL_BINS_WORK (bin_id, interval_no, bin_level, n_colls)
		VALUES ($level, $BIN_KEY, $level, $coded_reso)";
	
	$result = $dbh->do($sql);
	
	logMessage(2, "      summarizing at level $level by geography and interval...");
	
	$sql = "INSERT IGNORE INTO $COLL_BINS_WORK
			(bin_id, bin_level, interval_no,
			 n_colls, n_occs, early_age, late_age, lng, lat,
			 lng_min, lng_max, lat_min, lat_max, std_dev,
			 access_level)
		SELECT bin_id_$level, $level, interval_no, count(*), sum(n_occs),
		       if(max(m.early_age) > i.early_age, i.early_age, max(m.early_age)),
		       if(min(m.late_age) < i.late_age, i.late_age, min(m.late_age)),
		       avg(lng), avg(lat),
		       round(min(lng),5) as lng_min, round(max(lng),5) as lng_max,
		       round(min(lat),5) as lat_min, round(max(lat),5) as lat_max,
		       sqrt(var_pop(lng)+var_pop(lat)),
		       min(access_level)
		FROM $COLL_MATRIX_WORK as m JOIN $INTERVAL_DATA as i
			JOIN $SCALE_MAP as s using (interval_no)
			JOIN $INTERVAL_BUFFER as ib using (interval_no)
		WHERE if(m.late_age >= i.late_age,
			if(m.early_age <= i.early_age, m.early_age - m.late_age, i.early_age - m.late_age),
			if(m.early_age > i.early_age, i.early_age - i.late_age, m.early_age - i.late_age)) / (m.early_age - m.late_age) >= 0.5
		GROUP BY interval_no, bin_id_$level";
	
		# WHERE m.early_age <= ib.early_bound and m.late_age >= ib.late_bound
		# 	and (m.early_age < ib.early_bound or m.late_age > ib.late_bound)
		# 	and (m.early_age > i.late_age and m.late_age < i.early_age)

	$result = $dbh->do($sql);
	
	logMessage(2, "      generated $result non-empty bins.");
    }
    
    logMessage(2, "    setting point geometries for spatial index...");
    
    $sql = "UPDATE $COLL_BINS_WORK set loc =
		if(lng is null or lat is null, point(1000.0, 1000.0), point(lng, lat))";
    
    $result = $dbh->do($sql);
    
    # Now index the table just created
    
    logMessage(2, "    indexing summary table...");
    
    $result = $dbh->do("ALTER TABLE $COLL_BINS_WORK ADD SPATIAL INDEX (loc)");
    $result = $dbh->do("ALTER TABLE $COLL_BINS_WORK ADD INDEX (interval_no, lng, lat)");
    
    # Then create a table that maps each bin to the set of countries and
    # continents that it overlaps.
    
    logMessage(2, "    mapping summary clusters to countries and continents");
    
    $dbh->do("DROP TABLE IF EXISTS $BIN_LOC");
    
    $dbh->do("CREATE TABLE $BIN_LOC (
		bin_id int unsigned not null,
		bin_level tinyint unsigned,
		cc char(2),
		continent char(3),
		loc geometry not null,
		PRIMARY KEY (bin_id, cc),
		KEY (cc),
		KEY (continent)) Engine=MyISAM");
    
    foreach my $i (0..$#bin_reso)
    {
	my $level = $i + 1;
	
	logMessage(2, "      mapping bin level $level...");
	
	$sql = "INSERT INTO $BIN_LOC
		SELECT bin_id, bin_level, cc, continent, s.loc
		FROM $COLL_BINS_WORK as s JOIN $COLL_MATRIX_WORK as c on s.bin_id = c.bin_id_$level
			LEFT JOIN $COUNTRY_MAP as ccmap using (cc)
		WHERE bin_id > 0 and interval_no = 0
		GROUP BY bin_id, cc";

	try {
	    $result = $dbh->do($sql);
	}

	catch {
	    if ( $_ !~ /error on delete/si )
	    {
		die $_;
	    }
	};
	
	logMessage(2, "        generated $result rows");
    }
    
    logMessage(2, "    indexing summary cluster location table spatially");
    
    $result = $dbh->do("ALTER TABLE $COLL_BINS_WORK ADD SPATIAL INDEX (loc)");
    
    # Then create a table to map bins to containing bins.
    
    $bin_lines =~ s/,$//;
    
    $dbh->do("DROP TABLE IF EXISTS $BIN_CONTAINER");
    
    $dbh->do("CREATE TABLE $BIN_CONTAINER (
		bin_id int unsigned not null PRIMARY KEY,
		$bin_lines) Engine=MyISAM");
    
    logMessage(2, "    mapping bin containership");
    
    my $bin_string = "";
    
    foreach my $i (0..$#bin_reso)
    {
	my $level = $i + 1;
	
	$bin_string .= ", bin_id_${level}";
	
	logMessage(2, "      bin level $level...");
	
	$sql = "INSERT INTO $BIN_CONTAINER (bin_id${bin_string})
		SELECT distinct bin_id_${level}${bin_string}
		FROM $COLL_MATRIX WHERE bin_id_${level} > 0";
	
	$result = $dbh->do($sql);
    }
    
    # We then create a mapping table which allows us to look up, for each
    # collection, the time intervals which it encompasses (with the usual
    # buffer rule applied).
    
    logMessage(2, "    creating collection interval map...");
    
    $sql = "
    		INSERT IGNORE INTO $COLL_INTS_WORK (collection_no, interval_no)
    		SELECT collection_no, interval_no
    		FROM $COLL_MATRIX as m JOIN $INTERVAL_DATA as i
    			JOIN $SCALE_MAP as s using (interval_no)
    			JOIN $INTERVAL_BUFFER as ib using (interval_no)
    		WHERE m.early_age <= ib.early_bound and m.late_age >= ib.late_bound
    			and (m.early_age < ib.early_bound or m.late_age > ib.late_bound)
    			and (m.early_age > i.late_age and m.late_age < i.early_age)
    			and m.access_level = 0";
	
    $result = $dbh->do($sql);
    
    logMessage(2, "      generated $result rows.");

    logMessage(2, "    indexing interval/bin/collection map...");
    
    $dbh->do("ALTER TABLE $COLL_INTS_WORK ADD KEY (collection_no)");
    $dbh->do("ALTER TABLE $COLL_INTS_WORK ADD KEY (interval_no)");
    
    # If we were asked to apply the K-means clustering algorithm, do so now.
    
    # applyClustering($dbh, $bin_list) if $options->{colls_cluster} and @bin_reso;
    
    # Then we build a table listing all of the different geological strata.
    
    buildStrataTables($dbh);
    buildLithTables($dbh);
    buildEnvTables($dbh);
    
    # Finally, we swap in the new tables for the old ones.
    
    activateTables($dbh, $COLL_MATRIX_WORK => $COLL_MATRIX, $COLL_BINS_WORK => $COLL_BINS,
		         $COLL_INTS_WORK => $COLL_INTS);
    
    $dbh->do("REPLACE INTO last_build (name) values ('collections')");
    
    $dbh->do("DROP TABLE IF EXISTS protected_aux");
    
    my $a = 1;		# We can stop here when debugging
}


sub updateLocationTable {
    
    my ($dbh) = @_;
    
    my ($sql, $result, $count);
    
    # Make sure that we have a clean table in which to store lookup results.
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $COLL_LOC (
		collection_no int unsigned primary key,
		lng decimal(9,6),
		lat decimal(9,6),
		cc char(2),
		protected varchar(255)) Engine=MyISAM CHARACTER SET utf8 COLLATE utf8_unicode_ci");
    
    # If there is anything in the table, delete from it any row corresponding
    # to a collection whose coordinates have been nulled out.  This will occur
    # exceedingly rarely if ever, but is a boundary case we need to check.
    
    $sql =     "DELETE cl FROM $COLLECTIONS as c JOIN $COLL_LOC as cl using (collection_no)
		WHERE c.lng not between -180.0 and 180.0 or c.lng is null or
		      c.lat not between -90.0 and 90.0 or c.lat is null";
    
    $result = $dbh->do($sql);
    
    if ( $result > 0 )
    {
	logMessage(2, "      deleted $result records corresponding to collections with invalid coordinates");
    }
    
    # Then add a fresh row to the table for every collection that has valid
    # coordinates but either doesn't already appear there or or has different
    # coordinates than appear there.  Any row already there will be replaced,
    # as it is not valid any more (i.e. its collection's longitude and/or
    # latitude have been modified).
    
    $sql = "	REPLACE INTO $COLL_LOC (collection_no, cc, lat, lng)
		SELECT c.collection_no, c.cc, c.lat, c.lng
		FROM $COLL_MATRIX_WORK as c LEFT JOIN $COLL_LOC as cl using (collection_no)
		WHERE c.lng between -180.0 and 180.0 and c.lng is not null and
		      c.lat between -90.0 and 90.0 and c.lat is not null and
		      (c.lng <> cl.lng or cl.lng is null or
		       c.lat <> cl.lat or cl.lat is null)";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      added $result new rows to the table")
	if $result > 0;
    
    # Prepare some statements.

    my $lookup_sth = $dbh->prepare("
		SELECT group_concat(category)
		FROM protected_land as p
		WHERE st_within(point(?,?), p.shape)");
    
    my $update_sth = $dbh->prepare("
		UPDATE $COLL_LOC SET protected = ?
		WHERE collection_no = ?");
    
    # Then search for records where 'protection' is null.  These have
    # been newly added, and need to be looked up.
    
    my $fetch_sth = $dbh->prepare("
		SELECT collection_no, lng, lat FROM $COLL_LOC
		WHERE protected is null");
    
    $fetch_sth->execute();
    $count = 0;
    
    # For each of these records, look up the protection status and set it.
    
    while ( my ($coll_no, $lng, $lat) = $fetch_sth->fetchrow_array() )
    {
	my ($prot) = $dbh->selectrow_array($lookup_sth, {}, $lng, $lat);
	
	$prot ||= '';	# if the result is null, set it to the empty string
	
	$update_sth->execute($prot, $coll_no);
	$count++;
	
	#     SELECT collection_no, group_concat(category)
	#     FROM coll_matrix as m join protected_land as p on st_within(m.loc, p.shape)
	#     GROUP BY collection_no";

    }
    
    logMessage(2, "      updated $count entries");
    
    my $a = 1;	# we can stop here when debugging
}


# buildStrataTables
# 
# Compute a table that can be used to query for geological strata by some
# combination of partial name and geographic coordinates.

sub buildStrataTables {
    
    my ($dbh, $options) = @_;
    
    $options ||= {};
    
    # Check for the existence of working tables.
    
    my ($coll_matrix, $cmw_count);
    
    eval {
	($cmw_count) = $dbh->selectrow_array("SELECT count(*) FROM $COLL_MATRIX_WORK");
    };
    
    $coll_matrix = $cmw_count ? $COLL_MATRIX_WORK : $COLL_MATRIX;
    
    # Create a new working table.
    
    $dbh->do("DROP TABLE IF EXISTS $COLL_STRATA_WORK");
    
    $dbh->do("CREATE TABLE $COLL_STRATA_WORK (
		grp varchar(255) not null,
		formation varchar(255) not null,
		member varchar(255) not null,
		maybe boolean not null,
		lithology varchar(255),
		collection_no int unsigned not null,
		access_level tinyint unsigned not null,
		n_occs int unsigned not null,
		cc char(2),
		lat decimal(9,6),
		lng decimal(9,6),
		g_plate_no smallint unsigned not null,
		s_plate_no smallint unsigned not null,
		loc geometry not null) Engine=MyISAM");
    
    $DB::single = 1;
    
    # Fill it from the collections and coll_matrix tables.
    
    logMessage(2, "    computing strata table...");
    
    my ($sql, $result, $count);
    
    $sql = "	INSERT INTO $COLL_STRATA_WORK (grp, formation, member, lithology,
			collection_no, access_level, n_occs, cc, lat, lng, 
			g_plate_no, s_plate_no, loc)
		SELECT cc.geological_group, cc.formation, cc.member, 
			if(lithology1 <> '' and lithology2 <> '' and lithology1 <> lithology2,
			  concat(lithology1,'/',lithology2),
			    if(lithology1 <> '' and lithology1 <> 'not reported', lithology1, null)),
			collection_no, c.access_level, c.n_occs, c.cc, c.lat, c.lng,
			c.g_plate_no, c.s_plate_no, c.loc
		FROM $coll_matrix as c JOIN collections as cc using (collection_no)";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      $result collections");
    
    logMessage(2, "    cleaning stratum names...");
    
    # $sql = "    UPDATE $COLL_STRATA_WORK
    # 		SET name = replace(name, '\"', '')";
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "      removed $result quote-marks");
    
    # Remove redundant suffixes
    
    $sql = "    UPDATE $COLL_STRATA_WORK
		SET member = left(member, length(member)-3)
		WHERE member like '\%Mb.'";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      removed $result final 'Mb.'");
    
    $sql = "    UPDATE $COLL_STRATA_WORK
		SET formation = left(formation, length(formation)-3)
		WHERE formation like '\%Fm.'";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      removed $result final 'Fm.'");
    
    $sql = "    UPDATE $COLL_STRATA_WORK
		SET formation = left(formation, length(formation)-9)
		WHERE formation like '\%Formation'";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      removed $result final 'Formation'");
    
    $sql = "    UPDATE $COLL_STRATA_WORK
		SET grp = left(grp, length(grp)-5)
		WHERE grp like '\%Group'";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "      removed $result final 'Group'");
    
    # Then remove question marks and set the 'maybe' field to true for those
    # records.
    
    $sql = "    UPDATE $COLL_STRATA_WORK
		SET member = substring(member, 2), maybe = true
		WHERE member like '?%'";
    
    $result = $dbh->do($sql);
    
    $sql = "    UPDATE $COLL_STRATA_WORK
		SET formation = substring(formation, 2), maybe = true
		WHERE formation like '?%'";
    
    $result += $dbh->do($sql);
    
    $sql = "    UPDATE $COLL_STRATA_WORK
		SET grp = substring(grp, 2), maybe = true
		WHERE grp like '?%'";
    
    $result += $dbh->do($sql);
    
    logMessage(2, "      removed $result initial question-marks");
    
    $sql = "	UPDATE $COLL_STRATA_WORK
		SET member = left(member, length(member)-1), maybe = true
		WHERE member like '%?'";
    
    $result = $dbh->do($sql);
    
    $sql = "	UPDATE $COLL_STRATA_WORK
		SET formation = left(formation, length(formation)-1), maybe = true
		WHERE formation like '%?'";
    
    $result += $dbh->do($sql);
    
    $sql = "	UPDATE $COLL_STRATA_WORK
		SET grp = left(grp, length(grp)-1), maybe = true
		WHERE grp like '%?'";
    
    $result += $dbh->do($sql);
    
    $sql = "	UPDATE $COLL_STRATA_WORK
		SET member = left(member, length(member)-3), maybe = true
		WHERE member like '%(?)'";
    
    $result += $dbh->do($sql);
    
    $sql = "	UPDATE $COLL_STRATA_WORK
		SET formation = left(formation, length(formation)-3), maybe = true
		WHERE formation like '%(?)'";
    
    $result += $dbh->do($sql);
    
    $sql = "	UPDATE $COLL_STRATA_WORK
		SET grp = left(grp, length(grp)-3), maybe = true
		WHERE grp like '%(?)'";
    
    $result += $dbh->do($sql);
    
    logMessage(2, "      removed $result final question-marks");
    
    # $sql = "	UPDATE $COLL_STRATA_WORK
    # 		SET name = replace(name, '(?)', ''), maybe = true
    # 		WHERE name like '%(?)%'";
    
    # $result = $dbh->do($sql);
    
    # $sql = "	UPDATE $COLL_STRATA_WORK
    # 		SET name = replace(name, '?', ''), maybe = true
    # 		WHERE name like '%?%'";
    
    # $result += $dbh->do($sql);
    
    # logMessage(2, "      removed $result middle question-marks");
    
    # Trim empty spaces at beginning and end.
    
    $sql = "	UPDATE $COLL_STRATA_WORK
		SET member = trim(member),
		    formation = trim(formation),
		    grp = trim(grp)";
    
    $result = $dbh->do($sql);
    
    logMessage(2, "    trimmed $result names");
    
    logMessage(2, "    indexing by member, formation, and group...");
    
    $dbh->do("ALTER TABLE $COLL_STRATA_WORK ADD INDEX (member)");
    $dbh->do("ALTER TABLE $COLL_STRATA_WORK ADD INDEX (formation)");
    $dbh->do("ALTER TABLE $COLL_STRATA_WORK ADD INDEX (grp)");
    
    logMessage(2, "    indexing by collection_no...");
    
    $dbh->do("ALTER TABLE $COLL_STRATA_WORK ADD INDEX (collection_no)");
    
    logMessage(2, "    indexing by geographic location...");
    
    $dbh->do("ALTER TABLE $COLL_STRATA_WORK ADD SPATIAL INDEX (loc)");
    
    # Now create a separate table just listing all of the names, primarily for use in
    # auto-completion.
    
    logMessage(2, "    creating strata names table...");
    
    $dbh->do("DROP TABLE IF EXISTS $STRATA_NAMES_WORK");
    
    $dbh->do("CREATE TABLE $STRATA_NAMES_WORK (
		name varchar(255) not null,
		type enum('group', 'formation', 'member'),
		cc_list varchar(255) not null,
		country_list varchar(255) not null,
		n_colls int unsigned not null,
		n_occs int unsigned not null,
		lng_min decimal(9,6),
		lng_max decimal(9,6),
		lat_min decimal(9,6),
		lat_max decimal(9,6),
		UNIQUE KEY (name, type)) Engine=MyISAM");
    
    logMessage(2, "    inserting groups...");
    
    $result = $dbh->do("INSERT INTO $STRATA_NAMES_WORK (name, type, n_colls, n_occs, cc_list,
			country_list, lng_min, lng_max, lat_min, lat_max)
		SELECT grp, 'group', count(*), sum(n_occs), group_concat(distinct cc),
		       group_concat(distinct cm.name), min(lng), max(lng), min(lat), max(lat)
		FROM $COLL_STRATA_WORK join $COUNTRY_MAP as cm using (cc)
		WHERE grp <> ''	and grp not like 'unnamed' GROUP BY grp");
    
    logMessage(2, "      $result groups");
    
    logMessage(2, "    inserting formations...");
    
    $result = $dbh->do("INSERT INTO $STRATA_NAMES_WORK (name, type, n_colls, n_occs, cc_list,
			country_list, lng_min, lng_max, lat_min, lat_max)
		SELECT formation, 'formation', count(*), sum(n_occs), group_concat(distinct cc),
			group_concat(distinct cm.name), min(lng), max(lng), min(lat), max(lat)
		FROM $COLL_STRATA_WORK join $COUNTRY_MAP as cm using (cc)
		WHERE formation <> '' and formation not like 'unnamed' GROUP BY formation");
    
    logMessage(2, "      $result formations");
    
    activateTables($dbh, $COLL_STRATA_WORK => $COLL_STRATA,
			 $STRATA_NAMES_WORK => $STRATA_NAMES);
}


# buildLithTables ( dbh )
#
# Build a table that relates collections to lithologies.

sub buildLithTables {
    
    my ($dbh, $options) = @_;
    
    $options ||= { };

    logMessage(2, "    building collection lithology table...");
    
    $dbh->do("DROP TABLE IF EXISTS $COLL_LITH_WORK");
    
    $dbh->do("CREATE TABLE $COLL_LITH_WORK (
	collection_no int unsigned not null,
	lithology varchar(30) not null,
	macros_lith varchar(30) not null,
	lith_type varchar(30) not null,
	UNIQUE KEY (collection_no, lithology),
	KEY (lithology),
	KEY (macros_lith)) ENGINE=MYISAM");
    
    my ($sql, $count);
    
    $sql = "
	INSERT IGNORE INTO $COLL_LITH_WORK (collection_no, lithology, macros_lith, lith_type)
	SELECT collection_no, lithology1, lith, lith_type
	FROM $COLLECTIONS join $MACROSTRAT_LITHS on lithology1 = lith
	WHERE fossilsfrom1 = 'Y' or fossilsfrom2 = '' or fossilsfrom2 is null";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      found $count matches for lithology1");
    
    $sql = "
	INSERT IGNORE INTO $COLL_LITH_WORK (collection_no, lithology, macros_lith, lith_type)
	SELECT collection_no, lithology1, lith, lith_type
	FROM $COLLECTIONS join $MACROSTRAT_LITHS on lithology1 = concat('\"',lith,'\"')
	WHERE fossilsfrom1 = 'Y' or fossilsfrom2 = '' or fossilsfrom2 is null";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      found $count matches for lithology1 with \"\"");

    # $sql = "
    # 	INSERT IGNORE INTO $COLL_LITH_WORK (collection_no, lithology, lith_type)
    # 	SELECT collection_no, lithology1, 'other'
    # 	FROM $COLLECTIONS left join $MACROSTRAT_LITHS on lithology1 = lith
    # 	WHERE lithology1 is not null and lithology1 <> ''
    # 		and lithology1 not like '\"%' and lith is null
    # 		and (fossilsfrom1 = 'Y' or fossilsfrom2 = '' or fossilsfrom2 is null)";
    
    $sql = "
	INSERT IGNORE INTO $COLL_LITH_WORK (collection_no, lithology, lith_type)
	SELECT collection_no, lithology1, 'other'
	FROM $COLLECTIONS
	WHERE lithology1 is not null and lithology1 <> '' and lithology1 <> 'not reported'
		and (fossilsfrom1 = 'Y' or fossilsfrom2 = '' or fossilsfrom2 is null)";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      found $count collections with no match for lithology1");
    
    $sql = "
	INSERT IGNORE INTO $COLL_LITH_WORK (collection_no, lithology, macros_lith, lith_type)
	SELECT collection_no, lithology2, lith, lith_type
	FROM $COLLECTIONS join $MACROSTRAT_LITHS on lithology2 = lith
	WHERE fossilsfrom2 = 'Y' or fossilsfrom1 = '' or fossilsfrom1 is null";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      found $count matches for lithology2");
    
    $sql = "
	INSERT IGNORE INTO $COLL_LITH_WORK (collection_no, lithology, macros_lith, lith_type)
	SELECT collection_no, lithology2, lith, lith_type
	FROM $COLLECTIONS join $MACROSTRAT_LITHS on lithology2 = concat('\"',lith,'\"')
	WHERE fossilsfrom2 = 'Y' or fossilsfrom1 = '' or fossilsfrom1 is null";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      found $count matches for lithology2 with \"\"");
    
    # $sql = "
    # 	INSERT IGNORE INTO $COLL_LITH_WORK (collection_no, lithology, lith_type)
    # 	SELECT collection_no, lithology2, 'other'
    # 	FROM $COLLECTIONS left join $MACROSTRAT_LITHS on lithology2 = lith
    # 	WHERE lithology2 is not null and lithology2 <> '' 
    # 		and lithology2 not like '\"%' and lith is null
    # 		and (fossilsfrom2 = 'Y' or fossilsfrom1 = '' or fossilsfrom1 is null)";
    
    $sql = "
	INSERT IGNORE INTO $COLL_LITH_WORK (collection_no, lithology, lith_type)
	SELECT collection_no, lithology2, 'other'
	FROM $COLLECTIONS
	WHERE lithology2 is not null and lithology2 <> '' and lithology2 <> 'not reported'
		and (fossilsfrom2 = 'Y' or fossilsfrom1 = '' or fossilsfrom1 is null)";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      found $count collections with no match for lithology2");
    
    $sql = "
	UPDATE $COLL_LITH_WORK SET lith_type = 'mixed'
	WHERE lithology like '\%carbonate%' and lithology like '\%siliciclastic%' or lithology = 'marl'";
    
    $count = $dbh->do($sql);
    
    logMessage(2, "      updated lith_type to 'mixed' on $count rows");
    
    activateTables($dbh, $COLL_LITH_WORK => $COLL_LITH);
}


sub buildEnvTables {

}

# applyClustering ( bin_list )
# 
# For each of the bin levels which specify clustering, apply the k-means
# algorithm to adjust the contents of the bins.

sub applyClustering {
    
    my ($dbh, $bin_list) = @_;
    
    my ($sql, $result);
    
    # Start by creating an auxiliary table for use in computing cluster
    # assignments.
    
    $dbh->do("DROP TABLE IF EXISTS $CLUST_AUX");
    
    $dbh->do("CREATE TABLE $CLUST_AUX (
		bin_id int unsigned primary key,
		clust_id int unsigned not null) ENGINE=MYISAM");
    
    # Go through the bin levels, skipping any that don't specify clustering.
    # We will need to cluster the finer bins first, then go to coarser.  Thus
    # we end up reversing the list.  It is an error for clustering to be
    # enabled on the finest level of binning.
    
    my $level = 0;
    my @CLUSTER_LEVELS;
    
    foreach my $bin (@$bin_list)
    {
	next unless defined $bin->{resolution} && $bin->{resolution} > 0;
	
	$level++;
	next unless $bin->{cluster};
	
	push @CLUSTER_LEVELS, $level;
    }
    
    # Skip if no levels are to be clustered.
    
    return unless @CLUSTER_LEVELS;
    
    # Now we reverse the list and cluster each bin level in turn.
    
    foreach my $level (reverse @CLUSTER_LEVELS)
    {
	logMessage(2, "    applying k-means algorithm to bin level $level");
	
	my $child_level = $level + 1;
	my $CLUSTER_WORK = "${COLL_BINS_WORK}_${level}";
	my $CHILD_WORK = "${COLL_BINS_WORK}_${child_level}";
	my $CLUSTER_FIELD = "bin_id_${level}";
	
	$sql = "UPDATE $CHILD_WORK SET clust_id = $CLUSTER_FIELD";
	
	my $rows_changed = $dbh->do($sql);
	my $rounds_executed = 0;
	
	# The initial cluster assignments ("seeds") have already been made on
	# the basis of the geographic coordinates of the child bins.  So we
	# iterate the process of assigning each child bin to the cluster with
	# the nearest centroid, then re-computing the cluster centroids.  We
	# repeat until the number of points that move to a different cluster
	# drops below our threshold, or at most the specified number of rounds.
	
	while ( $rows_changed > $MOVE_THRESHOLD and $rounds_executed < $MAX_ROUNDS )
	{
	    # Reassign each child bin to the closest cluster.
	
	    logMessage(2, "      recomputing cluster assignments...");
	    
	    $dbh->do("DELETE FROM $CLUST_AUX");
	    
	    $sql = "INSERT IGNORE INTO $CLUST_AUX
		SELECT b.bin_id, k.bin_id
		FROM $CHILD_WORK as b JOIN $CLUSTER_WORK as k
		ORDER BY POW(k.lat-b.lat,2)+POW(k.lng-b.lng,2) ASC";

			# on k.clust_lng between floor(bin_lng * $bin_ratio)-1
			# 	and floor(bin_lng * $bin_ratio)+1
			# and k.clust_lat between floor(bin_lat * $bin_ratio)-1
			# 	and floor(bin_lat * $bin_ratio)+1
	    
	    $result = $dbh->do($sql);
	    
	    $sql = "UPDATE $CHILD_WORK as cb JOIN $CLUST_AUX as k using (bin_id)
		    SET cb.clust_id = k.clust_id";
	
	    # $sql = "UPDATE $COLL_BINS_WORK as c SET c.clust_no = 
	    # 	(SELECT k.clust_no from $COLL_CLUST_WORK as k 
	    # 	 ORDER BY POW(k.lat-c.lat,2)+POW(k.lng-c.lng,2) ASC LIMIT 1)";
	    
	    ($rows_changed) = $dbh->do($sql);
	    
	    logMessage(2, "      $rows_changed rows changed");
	    
	    # Then recompute the centroid of each cluster based on the data points
	    # (bins) assigned to it.
	    
	    $sql = "UPDATE $CLUSTER_WORK as k JOIN 
		(SELECT clust_id,
			sum(lng * n_colls)/sum(n_colls) as lng_avg,
			sum(lat * n_colls)/sum(n_colls) as lat_avg
		 FROM $CHILD_WORK GROUP BY clust_id) as cluster
			on k.bin_id = cluster.clust_id
		SET k.lng = cluster.lng_avg, k.lat = cluster.lat_avg";
	    
	    $result = $dbh->do($sql);
	    
	    $rounds_executed++;
	}
	
	# Now we need to index the cluster assignments, for both the child
	# bins and the collection matrix.
	
	$result = $dbh->do("ALTER TABLE $CHILD_WORK ADD INDEX (clust_id)");
	$result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (clust_id)");
 	
	# Finally we recompute the summary fields for the cluster table
	# (except the centroid, which has already been computed above).
    
	logMessage(2, "    setting collection statistics for each cluster...");
	
	$sql = "    UPDATE $CLUSTER_WORK as k JOIN
		(SELECT clust_id, sum(n_colls) as n_colls,
			sum(n_occs) as n_occs,
			max(early_age) as early_age,
			min(late_age) as late_age,
			sqrt(var_pop(lng)+var_pop(lat)) as std_dev,
			min(lng_min) as lng_min, max(lng_max) as lng_max,
			min(lat_min) as lat_min, max(lat_max) as lat_max,
			min(access_level) as access_level
		FROM $CHILD_WORK GROUP BY clust_id) as agg
			using (clust_id)
		SET k.n_colls = agg.n_colls, k.n_occs = agg.n_occs,
		    k.early_age = agg.early_age, k.late_age = agg.late_age,
		    k.std_dev = agg.std_dev, k.access_level = agg.access_level,
		    k.lng_min = agg.lng_min, k.lng_max = agg.lng_max,
		    k.lat_min = agg.lat_min, k.lat_max = agg.lat_max";
    
	$result = $dbh->do($sql);
    }
    
    # Clean up the auxiliary table.
    
    $dbh->do("DROP TABLE IF EXISTS $CLUST_AUX");
    
    my $a = 1;	# we can stop here when debugging
}


# Createcountrymap ( dbh, force )
# 
# Create the country_map table if it does not already exist.

sub createCountryMap {

    my ($dbh, $force) = @_;
    
    # First make sure we have a clean table.
    
    if ( $force )
    {
	$dbh->do("DROP TABLE IF EXISTS $COUNTRY_MAP");
    }
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $COUNTRY_MAP (
		cc char(2) primary key,
		continent char(3),
		name varchar(80) not null,
		INDEX (name),
		INDEX (continent)) Engine=MyISAM");
    
    # Then populate it if necessary.
    
    my ($count) = $dbh->selectrow_array("SELECT count(*) FROM $COUNTRY_MAP");
    
    return if $count;
    
    logMessage(2, "    rebuilding country map");
    
    $dbh->do("INSERT INTO $COUNTRY_MAP (cc, continent, name) VALUES
	('AU', 'AUS', 'Australia'),
	('DZ', 'AFR', 'Algeria'),
	('AO', 'AFR', 'Angola'),
	('BW', 'AFR', 'Botswana'),
	('CM', 'AFR', 'Cameroon'),
	('CV', 'AFR', 'Cape Verde'),
	('TD', 'AFR', 'Chad'),
	('CG', 'AFR', 'Congo-Brazzaville'),
	('CD', 'AFR', 'Congo-Kinshasa'),
	('CI', 'AFR', 'Cote D\\'Ivoire'),
	('DJ', 'AFR', 'Djibouti'),
	('EG', 'AFR', 'Egypt'),
	('ER', 'AFR', 'Eritrea'),
	('ET', 'AFR', 'Ethiopia'),
	('GA', 'AFR', 'Gabon'),
	('GH', 'AFR', 'Ghana'),
	('GN', 'AFR', 'Guinea'),
	('KE', 'AFR', 'Kenya'),
	('LS', 'AFR', 'Lesotho'),
	('LY', 'AFR', 'Libya'),
	('MW', 'AFR', 'Malawi'),
	('ML', 'AFR', 'Mali'),
	('MR', 'AFR', 'Mauritania'),
	('MA', 'AFR', 'Morocco'),
	('MZ', 'AFR', 'Mozambique'),
	('NA', 'AFR', 'Namibia'),
	('NE', 'AFR', 'Niger'),
	('NG', 'AFR', 'Nigeria'),
	('SH', 'AFR', 'Saint Helena'),
	('SN', 'AFR', 'Senegal'),
	('SO', 'AFR', 'Somalia'),
	('ZA', 'AFR', 'South Africa'),
	('SS', 'AFR', 'South Sudan'),
	('SD', 'AFR', 'Sudan'),
	('SZ', 'AFR', 'Swaziland'),
	('TZ', 'AFR', 'Tanzania'),
	('TG', 'AFR', 'Togo'),
	('TN', 'AFR', 'Tunisia'),
	('UG', 'AFR', 'Uganda'),
	('EH', 'AFR', 'Western Sahara'),
	('ZM', 'AFR', 'Zambia'),
	('ZW', 'AFR', 'Zimbabwe'),
	('AR', 'SOA', 'Argentina'),
	('BO', 'SOA', 'Bolivia'),
	('BR', 'SOA', 'Brazil'),
	('CL', 'SOA', 'Chile'),
	('CO', 'SOA', 'Colombia'),
	('EC', 'SOA', 'Ecuador'),
	('FA', 'SOA', 'Falkland Islands (Malvinas)'),
	('GY', 'SOA', 'Guyana'),
	('PY', 'SOA', 'Paraguay'),
	('PE', 'SOA', 'Peru'),
	('SR', 'SOA', 'Suriname'),
	('UY', 'SOA', 'Uruguay'),
	('VE', 'SOA', 'Venezuela'),
	('AE', 'ASI', 'United Arab Emirates'),
	('AM', 'ASI', 'Armenia'),
	('AZ', 'ASI', 'Azerbaijan'),
	('BH', 'ASI', 'Bahrain'),
	('KH', 'ASI', 'Cambodia'),
	('TL', 'ASI', 'East Timor'),
	('GE', 'ASI', 'Georgia'),
	('ID', 'ASI', 'Indonesia'),
	('IR', 'ASI', 'Iran'),
	('IQ', 'ASI', 'Iraq'),
	('IL', 'ASI', 'Israel'),
	('JO', 'ASI', 'Jordan'),
	('KW', 'ASI', 'Kuwait'),
	('KG', 'ASI', 'Kyrgyzstan'),
	('LB', 'ASI', 'Lebanon'),
	('KP', 'ASI', 'North Korea'),
	('OM', 'ASI', 'Oman'),
	('PS', 'ASI', 'Palestinian Territory'),
	('QA', 'ASI', 'Qatar'),
	('SA', 'ASI', 'Saudi Arabia'),
	('KR', 'ASI', 'South Korea'),
	('SY', 'ASI', 'Syria'),
	('TR', 'ASI', 'Turkey'),
	('YE', 'ASI', 'Yemen'),
	('AF', 'ASI', 'Afghanistan'),
	('BD', 'ASI', 'Bangladesh'),
	('BT', 'ASI', 'Bhutan'),
	('IN', 'ASI', 'India'),
	('KZ', 'ASI', 'Kazakstan'),
	('MY', 'ASI', 'Malaysia'),
	('MM', 'ASI', 'Myanmar'),
	('NP', 'ASI', 'Nepal'),
	('PK', 'ASI', 'Pakistan'),
	('PH', 'ASI', 'Philippines'),
	('LK', 'ASI', 'Sri Lanka'),
	('TW', 'ASI', 'Taiwan'),
	('TJ', 'ASI', 'Tajikistan'),
	('TH', 'ASI', 'Thailand'),
	('TM', 'ASI', 'Turkmenistan'),
	('TU', 'ASI', 'Tuva'),
	('UZ', 'ASI', 'Uzbekistan'),
	('VN', 'ASI', 'Vietnam'),
	('CN', 'ASI', 'China'),
	('HK', 'ASI', 'Hong Kong'),
	('JP', 'ASI', 'Japan'),
	('MN', 'ASI', 'Mongolia'),
	('LA', 'ASI', 'Laos'),
	('AA', 'ATA', 'Antarctica'),
	('AL', 'EUR', 'Albania'),
	('AT', 'EUR', 'Austria'),
	('BY', 'EUR', 'Belarus'),
	('BE', 'EUR', 'Belgium'),
	('BG', 'EUR', 'Bulgaria'),
	('HR', 'EUR', 'Croatia'),
	('CY', 'EUR', 'Cyprus'),
	('CZ', 'EUR', 'Czech Republic'),
	('DK', 'EUR', 'Denmark'),
	('EE', 'EUR', 'Estonia'),
	('FI', 'EUR', 'Finland'),
	('FR', 'EUR', 'France'),
	('DE', 'EUR', 'Germany'),
	('GR', 'EUR', 'Greece'),
	('HU', 'EUR', 'Hungary'),
	('IS', 'EUR', 'Iceland'),
	('IE', 'EUR', 'Ireland'),
	('IT', 'EUR', 'Italy'),
	('LV', 'EUR', 'Latvia'),
	('LT', 'EUR', 'Lithuania'),
	('LU', 'EUR', 'Luxembourg'),
	('MK', 'EUR', 'Macedonia'),
	('MT', 'EUR', 'Malta'),
	('MD', 'EUR', 'Moldova'),
	('NL', 'EUR', 'Netherlands'),
	('NO', 'EUR', 'Norway'),
	('PL', 'EUR', 'Poland'),
	('PT', 'EUR', 'Portugal'),
	('RO', 'EUR', 'Romania'),
	('RU', 'EUR', 'Russian Federation'),
	('SM', 'EUR', 'San Marino'),
	('RS', 'EUR', 'Serbia and Montenegro'),
	('SK', 'EUR', 'Slovakia'),
	('SI', 'EUR', 'Slovenia'),
	('ES', 'EUR', 'Spain'),
	('SJ', 'EUR', 'Svalbard and Jan Mayen'),
	('SE', 'EUR', 'Sweden'),
	('CH', 'EUR', 'Switzerland'),
	('UA', 'EUR', 'Ukraine'),
	('UK', 'EUR', 'United Kingdom'),
	('BA', 'EUR', 'Bosnia and Herzegovina'),
	('GL', 'NOA', 'Greenland'),
	('US', 'NOA', 'United States'),
	('CA', 'NOA', 'Canada'),
	('MX', 'NOA', 'Mexico'),
	('AI', 'NOA', 'Anguilla'),
	('AG', 'NOA', 'Antigua and Barbuda'),
	('BS', 'NOA', 'Bahamas'),
	('BB', 'NOA', 'Barbados'),
	('BM', 'NOA', 'Bermuda'),
	('KY', 'NOA', 'Cayman Islands'),
	('CU', 'NOA', 'Cuba'),
	('DO', 'NOA', 'Dominican Republic'),
	('GP', 'NOA', 'Guadeloupe'),
	('HT', 'NOA', 'Haiti'),
	('JM', 'NOA', 'Jamaica'),
	('PR', 'NOA', 'Puerto Rico'),
	('BZ', 'NOA', 'Belize'),
	('CR', 'NOA', 'Costa Rica'),
	('SV', 'NOA', 'El Salvador'),
	('GD', 'NOA', 'Grenada'),
	('GT', 'NOA', 'Guatemala'),
	('HN', 'NOA', 'Honduras'),
	('NI', 'NOA', 'Nicaragua'),
	('PA', 'NOA', 'Panama'),
	('TT', 'NOA', 'Trinidad and Tobago'),
	('AW', 'NOA', 'Aruba'),
	('CW', 'NOA', 'Curaçao'),
	('SX', 'NOA', 'Sint Maarten'),
	('CK', 'OCE', 'Cook Islands'),
	('FJ', 'OCE', 'Fiji'),
	('PF', 'OCE', 'French Polynesia'),
	('GU', 'OCE', 'Guam'),
	('MH', 'OCE', 'Marshall Islands'),
	('NC', 'OCE', 'New Caledonia'),
	('NZ', 'OCE', 'New Zealand'),
	('MP', 'OCE', 'Northern Mariana Islands'),
	('PW', 'OCE', 'Palau'),
	('PG', 'OCE', 'Papua New Guinea'),
	('PN', 'OCE', 'Pitcairn'),
	('TO', 'OCE', 'Tonga'),
	('TV', 'OCE', 'Tuvalu'),
	('UM', 'OCE', 'United States Minor Outlying Islands'),
	('VU', 'OCE', 'Vanuatu'),
	('TF', 'IOC', 'French Southern Territories'),
	('MG', 'IOC', 'Madagascar'),
	('MV', 'IOC', 'Maldives'),
	('MU', 'IOC', 'Mauritius'),
	('YT', 'IOC', 'Mayotte'),
	('SC', 'IOC', 'Seychelles')");
    
    # Now the continents.

    $dbh->do("DROP TABLE IF EXISTS $CONTINENT_DATA");
    
    $dbh->do("CREATE TABLE IF NOT EXISTS $CONTINENT_DATA (
		continent char(3) primary key,
		name varchar(80) not null,
		INDEX (name)) Engine=MyISAM");
    
    $dbh->do("INSERT INTO $CONTINENT_DATA (continent, name) VALUES
	('ATA', 'Antarctica'),
	('AFR', 'Africa'),
	('ASI', 'Asia'),
	('AUS', 'Australia'),
	('EUR', 'Europe'),
	('IOC', 'Indian Ocean'),
	('NOA', 'North America'),
	('OCE', 'Oceania'),
	('SOA', 'South America')");
    
    my $a = 1;		# we can stop here when debugging
}


# deleteProtLandData ( dbh, cc, category )
# 
# Delete all of the protected land data corresponding to the specified country
# code and category.  Specify 'all' for $cc to delete all data.  If no
# category is given, then delete all records corresponding to the given cc.

sub deleteProtLandData {
    
    my ($dbh, $cc, $category) = @_;
    
    # Make sure we have a proper country code.
    
    croak "you must specify a country code" unless $cc;
    
    my $quoted_cc = $dbh->quote($cc);
    my $quoted_cat = $dbh->quote($category) if $category;
    
    my ($sql, $result, $count);
    
    # Disable the index, then delete the data, then re-enable it.  This will
    # be much faster than deleting the rows from the index one at a time.
    
    $result = $dbh->do("ALTER TABLE $PROTECTED_LAND DISABLE KEYS");
    
    # If the country code was given as 'all', just delete every record.
    
    if ( $cc eq 'all' )
    {
	$result = $dbh->do("DELETE FROM $PROTECTED_LAND");
    }
    
    # If a category was given, delete all records whose cc and category match
    # the specified arguments.
    
    elsif ( $quoted_cat )
    {
	$result = $dbh->do("DELETE FROM $PROTECTED_LAND
			    WHERE cc=$quoted_cc and category=$quoted_cat");
    }
    
    # Otherwise, delete all records whose cc matches the specified argument.
    
    else
    {
	$result = $dbh->do("DELETE FROM $PROTECTED_LAND
			    WHERE cc=$quoted_cc");
    }
    
    $result = $dbh->do("ALTER TABLE $PROTECTED_LAND ENABLE KEYS");
}


my (%PROT_LAND_CC, %PROT_LAND_CAT, $BAD_COUNT);

# startProtLandInsert ( dbh )
# 
# Prepare to insert protected land data.  This involves creating a working
# table into which the records will be put before being copied to the main
# table. 

sub startProtLandInsert {
    
    my ($dbh) = @_;
    
    my ($result);
    
    # Create a working table.
    
    $dbh->do("DROP TABLE IF EXISTS $PROTECTED_WORK");
    
    $dbh->do("CREATE TABLE $PROTECTED_WORK (
		shape GEOMETRY not null,
		cc char(2) not null,
		category varchar(10) not null) Engine=MyISAM");
    
    # Empty the hash that keeps track of what countries we are loading data
    # for. 
    
    %PROT_LAND_CC = ();
    %PROT_LAND_CAT = ();
    $BAD_COUNT = 0;
    
    my $a = 1;	# we can stop here when debugging
}


# insertProtLandRecord ( dbh, cc, category, wkt )
# 
# Insert a new shape record with the specified attributes.  The parameter
# $wkt should be a string representing a polygon in WKT format.

sub insertProtLandRecord {
    
    my ($dbh, $cc, $category, $wkt) = @_;
    
    my ($result, $sql);
    
    # Suppress warning messages when inserting records.
    
    local($dbh->{RaiseError}) = 0;
    
    # Make sure we have a properly quoted string for the country code and
    # category.  This also makes sure that we have a record of which ones were
    # mentioned in this insert operation.
    
    $category ||= '';
    
    my $quoted_cc = $PROT_LAND_CC{$cc} || ($PROT_LAND_CC{$cc} = $dbh->quote($cc));
    my $quoted_cat = $PROT_LAND_CAT{$category} || ($PROT_LAND_CAT{$category} = $dbh->quote($category));
    
    # Insert the record into the working table.
    
    $sql = "	INSERT INTO $PROTECTED_WORK (cc, category, shape)
		VALUES ($quoted_cc, $quoted_cat, PolyFromText('$wkt'))";
    
    eval {
	$result = $dbh->do($sql);
    };
    
    unless ( $result )
    {
	$BAD_COUNT++;
    }
    
    my $a = 1;	# we can stop here when debugging
}


# finishProtLandInsert( dbh, cc_list, category_list )
# 
# Copy everything from the working table into the main protected-land table.
# If $cc_list and/or $category_list are not empty (they should each be a
# listref or hashref), then first delete everything that corresponds to one of
# those values.  $category_list is ignored if $cc_list is empty.

sub finishProtLandInsert {
    
    my ($dbh, $cc_list, $category_list) = @_;
    
    my ($result, $sql, @where);
    
    logMessage(2, "    skipped $BAD_COUNT bad polygons.") if $BAD_COUNT > 0;
    
    # First collect up the lists of country codes and possibly categories to
    # delete.  Establish clauses that will exclude these when copying the data
    # over to the new table.
    
    my @ccs = @$cc_list if ref $cc_list eq 'ARRAY';
    @ccs    = keys %$cc_list if ref $cc_list eq 'HASH';
    
    my @cats = @$category_list if ref $category_list eq 'ARRAY';
    @cats    = keys %$category_list if ref $category_list eq 'HASH';
    
    if ( @ccs )
    {
	my $exclude_clause = '';
	$exclude_clause .= 'cc not in (';
	$exclude_clause .= join(q{,}, map { $dbh->quote($_) } @ccs);
	$exclude_clause .= ')';
	
	if ( @cats )
	{
	    $exclude_clause .= ' or category not in (';
	    $exclude_clause .= join(q{,}, map { $dbh->quote($_) } @cats);
	    $exclude_clause .= ')';
	}
	
	push @where, $exclude_clause if $exclude_clause;
    }
    
    my $where = '';
    $where .= 'WHERE ' . join(q{ and }, @where) . "\n" if @where;
    
    # Now check to see if we have an existing table that has any records in it.
    
    my $old_record_count;
    
    eval {
	local($dbh->{PrintError}) = 0;
	
	($old_record_count) = $dbh->selectrow_array("SELECT count(*) FROM $PROTECTED_LAND");
    };
    
    # If we do, add all of the existing data to the working table, except for
    # what is specified to exclude.
    
    if ( $old_record_count )
    {
	$sql = "INSERT INTO $PROTECTED_WORK (shape, cc, category)
		SELECT shape, cc, category
		FROM $PROTECTED_LAND
		$where";
	
	$result = $dbh->do($sql);
    }
    
    # Index the table.
    
    logMessage(2, "indexing by coordinates...");
    
    $result = $dbh->do("ALTER TABLE $PROTECTED_WORK ADD SPATIAL INDEX (shape)");
    
    logMessage(2, "indexing by cc and category...");
    
    $result = $dbh->do("ALTER TABLE $PROTECTED_WORK ADD INDEX (cc, category)");
    
    # Now activate the working table as the new protected land table.
    
    activateTables($dbh, $PROTECTED_WORK => $PROTECTED_LAND);
}
    
1;
