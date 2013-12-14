# 
# The Paleobiology Database
# 
#   CollectionTables.pm
# 

package CollectionTables;

use strict;

use base 'Exporter';

our (@EXPORT_OK) = qw(buildCollectionTables $COLL_MATRIX $COLL_BINS $COLL_INT_BINS $COUNTRY_MAP $CONTINENT_DATA @BIN_LEVEL);

use Carp qw(carp croak);
use Try::Tiny;

use CoreFunction qw(activateTables);
use IntervalTables qw($INTERVAL_DATA $SCALE_MAP $INTERVAL_MAP $INTERVAL_BUFFER);
use ConsoleLog qw(logMessage);

our $COLL_MATRIX = "coll_matrix";
our $COLL_BINS = "coll_bins";

our $COLL_MATRIX_WORK = "cmn";
our $COLL_BINS_WORK = "cbn";

our $COUNTRY_MAP = "country_map";
our $CONTINENT_DATA = "continent_data";
our $CLUST_AUX = "clust_aux";

our @BIN_LEVEL;

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
	    push @BIN_LEVEL, $bin;
	    $bin_lines .= "bin_id_$level int unsigned not null,\n";
	    $next_line = "bin_id_$level int unsigned not null,\n";
	    $parent_lines .= $next_line;
	}
    }
    
    # Now create a clean working table which will become the new collection
    # matrix.
    
    logMessage(1, "Building collection tables");
    
    $dbh->do("DROP TABLE IF EXISTS $COLL_MATRIX_WORK");
    
    $dbh->do("CREATE TABLE $COLL_MATRIX_WORK (
		collection_no int unsigned primary key,
		$bin_lines
		clust_id int unsigned not null,
		lng decimal(9,6),
		lat decimal(9,6),
		loc geometry not null,
		cc char(2),
		early_age decimal(9,5),
		late_age decimal(9,5),
		early_int_no int unsigned not null,
		late_int_no int unsigned not null,
		n_occs int unsigned not null,
		n_spec int unsigned not null,
		reference_no int unsigned not null,
		authorizer_no int unsigned not null,
		enterer_no int unsigned not null,
		access_level tinyint unsigned not null) Engine=MYISAM");
    
    logMessage(2, "    inserting collections...");
    
    $sql = "	INSERT INTO $COLL_MATRIX_WORK
		       (collection_no, lng, lat, loc, cc,
			early_int_no, late_int_no,
			reference_no, authorizer_no, enterer_no, access_level)
		SELECT c.collection_no, c.lng, c.lat, 
			if(c.lng is null or c.lat is null, point(1000.0, 1000.0), point(c.lng, c.lat)), 
			map.cc,
			c.max_interval_no, if(c.min_interval_no > 0, c.min_interval_no, c.max_interval_no),
			c.reference_no,	c.authorizer_no, c.enterer_no,
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
    
    logMessage(2, "    setting age ranges...");
    
    # Set the age boundaries for each collection.
    
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
    
    logMessage(2, "    indexing by geographic coordinates (separate)...");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (lng, lat)");
    
    logMessage(2, "    indexing by country...");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (cc)");
    
    logMessage(2, "    indexing by reference_no...");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (reference_no)");
    
    logMessage(2, "    indexing by chronological interval...");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (early_int_no)");
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (late_int_no)");
    
    logMessage(2, "    indexing by early and late age...");
    
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (early_age)");
    $result = $dbh->do("ALTER TABLE $COLL_MATRIX_WORK ADD INDEX (late_age)");
    
    # We then create summary table for each binning level, counting the
    # number of collections and occurrences in each bin and computing the
    # centroid and age boundaries (adjusted to the standard intervals).
    
    logMessage(2, "    creating geography/time summary table...");
    
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
    
    my $set_lines = '';
    my @index_stmts;
    
    # Now summarize at each level in turn.
    
    foreach my $i (0..$#bin_reso)
    {
	my $level = $i + 1;
	my $reso = $bin_reso[$i];
	
	next unless $level > 0 && $reso > 0;
	
	logMessage(2, "      summarizing at level $level by geography...");
	
	$sql = "INSERT IGNORE INTO $COLL_BINS_WORK
			(bin_id, bin_level, interval_no,
			 n_colls, n_occs, early_age, late_age, lng, lat,
			 lng_min, lng_max, lat_min, lat_max, std_dev,
			 access_level)
		SELECT bin_id_$level, $level, 0, count(*), sum(n_occs),
		       max(early_age), min(late_age),
		       avg(lng), avg(lat),
		       round(min(lng),5) as lng_min, round(max(lng),5) as lng_max,
		       round(min(lat),5) as lat_min, round(max(lat),5) as lat_max,
		       sqrt(var_pop(lng)+var_pop(lat)),
		       min(access_level)
		FROM $COLL_MATRIX_WORK as m
		GROUP BY bin_id_$level";
	
	$result = $dbh->do($sql);
	
	logMessage(2, "      generated $result non-empty bins.");
	
	# Add a special row indicating the bin resolution for each level.
	
	my $coded_reso = 360 / $reso;
	
	$sql = "REPLACE INTO $COLL_BINS_WORK (bin_id, interval_no, bin_level, n_colls)
		VALUES ($level, 999999, $level, $coded_reso)";
	
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
		WHERE m.early_age <= ib.early_bound and m.late_age >= ib.late_bound
			and (m.early_age < ib.early_bound or m.late_age > ib.late_bound)
			and (m.early_age > i.late_age and m.late_age < i.early_age)
		GROUP BY interval_no, bin_id_$level";
	
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
    
    # If we were asked to apply the K-means clustering algorithm, do so now.
    
    # applyClustering($dbh, $bin_list) if $options->{colls_cluster} and @bin_reso;
    
    # Finally, we swap in the new tables for the old ones.
    
    activateTables($dbh, $COLL_MATRIX_WORK => $COLL_MATRIX, $COLL_BINS_WORK => $COLL_BINS);
    
    my $a = 1;		# We can stop here when debugging
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
# Still need to fix: Zaire, U.A.E., UAE, Czechoslovakia, Netherlands Antilles

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


1;
