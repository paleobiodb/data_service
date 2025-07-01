# The Paleobiology Database
# 
#   MatrixBase.pm
#
# The routines in this module update the occurrence matrix and collection matrix
# when the base tables have changed. It is designed to be used both by the API
# and by Classic.

package MatrixBase;

use strict;

use base 'Exporter';

our (@EXPORT_OK) = qw(initializeBins updateCollectionMatrix
		      updateOccurrenceMatrix updateOccurrenceMatrixReids
		      deleteFromCollectionMatrix deleteFromOccurrenceMatrix
		      deleteReidsFromOccurrenceMatrix updateOccurrenceCounts);

use Carp qw(carp croak);
use Scalar::Util qw(blessed);

use TableDefs qw(%TABLE);
use CoreTableDefs;

use namespace::clean;

our ($HAS_INITIALIZED, @BIN_RESO, $BIN_SQL);


# initializeBins ( )
#
# If we haven't yet done so, construct the SQL expression for updating the bin
# ids. This requires info from either the Classic configuration file 'pbdb.conf'
# or the API configuration file 'config.yml'.

sub initializeBins {

    my ($ds) = @_;
    
    # If we have already initialized, there is nothing more to do.
    
    return if $HAS_INITIALIZED;
    
    # If we were called from a Classic server process, see if the configuration
    # variable 'BIN_RESO' has been set.

    if ( %PBDB::Constants::CONFIG )
    {
	if ( my $reso_list = $PBDB::Constants::CONFIG{BIN_RESO} )
	{
	    @BIN_RESO = split /\s*,\s*/, $reso_list;
	}
    }

    # If we were given a value for $ds, that means we were called from an API
    # server process. Read the necessary information out of the configuration
    # hash.

    elsif ( $ds && blessed($ds) && $ds->isa('Web::DataService') )
    {
	my $bin_list = $ds->{_config}{bins};

	if ( ref $bin_list eq 'ARRAY' )
	{
	    foreach my $bin ( @$bin_list )
	    {
		if ( defined $bin->{resolution} && $bin->{resolution} > 0 )
		{
		    push @BIN_RESO, $bin->{resolution};
		}
	    }
	}
    }

    # Whether or not we actually have any bin resolutions, we have now
    # finished initialization.

    $HAS_INITIALIZED = 1;
    $BIN_SQL = '';

    # Generate the bin update sql.

    my $level = 0;
    
    foreach my $reso ( @BIN_RESO )
    {
	$level++;
	next unless $level > 0 && $reso > 0;
	
	die "invalid resolution $reso: must evenly divide 180 degrees"
	    unless int(180/$reso) == 180/$reso;
	
	my $id_base = $reso < 1.0 ? $level . '00000000' : $level . '000000';
	my $lng_base = $reso < 1.0 ? '10000' : '1000';
	
	$BIN_SQL .= "," if $BIN_SQL;
	
	$BIN_SQL .= "bin_id_$level = if(lng between -180.0 and 180.0 and lat between -90.0 and 90.0,
			$id_base + $lng_base * floor((lng+180.0)/$reso) + floor((lat+90.0)/$reso), 0)\n";
    }
}


# updateCollectionMatrix ( dbh, collection_nos, debug_mode )
# 
# This routine must be called from anywhere in pbapi or classic where the
# COLLECTION_DATA ('collections') table is updated. It updates the corresponding
# entries in the COLLECTION_MATRIX ('coll_matrix') table. The argument must be
# either a single collection_no value or else an arrayref whose elements are
# collection_no values.

sub updateCollectionMatrix {

    my ($dbh, $collection_nos, $debug_out) = @_;

    my @collection_nos;

    if ( ref $collection_nos eq 'ARRAY' )
    {
	@collection_nos = @$collection_nos;
    }

    else
    {
	@collection_nos = $collection_nos;
    }
    
    # Update the collection matrix, at most 1000 records at a time.
    
    while ( @collection_nos )
    {
	my @work_list = splice(@collection_nos, 0, 1000);
	
	my $collection_list = join(',', map { $dbh->quote($_) } @work_list);
	
	my $sql = "REPLACE INTO $TABLE{COLLECTION_MATRIX}
		       (collection_no, lng, lat, loc, cc, continent,
			protected, early_age, late_age,
			early_int_no, late_int_no, 
			reference_no, n_occs, access_level)
		SELECT c.collection_no, c.lng, c.lat,
			if(c.lng is null or c.lat is null, point(1000.0, 1000.0), point(c.lng, c.lat)), 
			map.cc, map.continent, cl.protected,
			if(ei.early_age > li.late_age, ei.early_age, li.late_age),
			if(ei.early_age > li.late_age, li.late_age, ei.early_age),
			c.max_interval_no, if(c.min_interval_no > 0, c.min_interval_no, 
									c.max_interval_no),
			c.reference_no, if(occs.n_occs > 0, occs.n_occs, 0),
			case c.access_level
				when 'database members' then if(c.release_date < now(), 0, 1)
				when 'research group' then if(c.release_date < now(), 0, 2)
				when 'authorizer only' then if(c.release_date < now(), 0, 2)
				else 0
			end
		FROM collections as c
		    left join $TABLE{COLLECTION_LOC} as cl using (collection_no)
		    left join (SELECT collection_no, count(*) as n_occs
			       FROM $TABLE{OCCURRENCE_DATA} GROUP BY collection_no) as occs
			using (collection_no)
		    left join $TABLE{COUNTRY_MAP} as map on map.name = c.country
		    left join $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.max_interval_no
		    left join $TABLE{INTERVAL_DATA} as li on li.interval_no = 
				if(c.min_interval_no > 0, c.min_interval_no, c.max_interval_no)
		WHERE collection_no in ($collection_list)";
	
	debug_line($debug_out, "$sql\n") if $debug_out;
	
	$dbh->do($sql);
	
	if ( $BIN_SQL )
	{
	    $sql = "UPDATE $TABLE{COLLECTION_MATRIX}
		SET $BIN_SQL
		WHERE collection_no in ($collection_list)";
	    
	    debug_line($debug_out, "$sql\n") if $debug_out;
	    
	    $dbh->do($sql);
	}
    }
}


sub deleteFromCollectionMatrix {
    
    my ($dbh, $collection_nos, $debug_out) = @_;
    
    my @collection_nos;
    
    if ( ref $collection_nos eq 'ARRAY' )
    {
	@collection_nos = @$collection_nos;
    }
    
    else
    {
	@collection_nos = $collection_nos;
    }
    
    # Update the collection matrix, at most 1000 records at a time.
    
    my $sql;
    
    while ( @collection_nos )
    {
	my @work_list = splice(@collection_nos, 0, 1000);
	
	my $collection_list = join(',', map { $dbh->quote($_) } @work_list);
	
	$sql = "DELETE FROM $TABLE{COLLECTION_MATRIX} WHERE collection_no in ($collection_list)";
	
	debug_line($debug_out, "$sql\n") if $debug_out;
	
	$dbh->do($sql);
    }
}


sub updateOccurrenceMatrix {

    my ($dbh, $occurrence_nos, $debug_out) = @_;

    my @occurrence_nos;

    if ( ref $occurrence_nos eq 'ARRAY' )
    {
	@occurrence_nos = @$occurrence_nos;
    }

    else
    {
	@occurrence_nos = $occurrence_nos;
    }
    
    # Update the occurrence matrix, at most 1000 records at a time.

    my $sql;
    
    while ( @occurrence_nos )
    {
	my @work_list = splice(@occurrence_nos, 0, 1000);
	
	my $occurrence_list = join(',', map { $dbh->quote($_) } @work_list);
	
	$sql = "REPLACE INTO $TABLE{OCCURRENCE_MATRIX}
		       (occurrence_no, reid_no, latest_ident, collection_no, taxon_no, orig_no,
			genus_name, genus_reso, subgenus_name, subgenus_reso, 
			species_name, species_reso, subspecies_name, subspecies_reso,
			plant_organ, plant_organ2,
			early_age, late_age, reference_no,
			authorizer_no, enterer_no, modifier_no, created, modified)
		SELECT o.occurrence_no, 0, if(o.reid_no > 0, false, true),
			o.collection_no, o.taxon_no, a.orig_no, 
			o.genus_name, coalesce(o.genus_reso,''),
			coalesce(o.subgenus_name,''), coalesce(o.subgenus_reso,''),
			o.species_name, coalesce(o.species_reso,''),
			coalesce(o.subspecies_name,''), coalesce(o.subspecies_reso,''),
			o.plant_organ, o.plant_organ2,
			ei.early_age, li.late_age,
			if(o.reference_no > 0, o.reference_no, 0),
			o.authorizer_no, o.enterer_no, o.modifier_no, o.created, o.modified
		FROM $TABLE{OCCURRENCE_DATA} as o JOIN $TABLE{COLLECTION_MATRIX} as c
			    using (collection_no)
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
			LEFT JOIN $TABLE{AUTHORITY_DATA} as a using (taxon_no)
		WHERE occurrence_no in ($occurrence_list)";
	
	debug_line($debug_out, "$sql\n") if $debug_out;
	
	$dbh->do($sql);
	
	$sql = "REPLACE INTO $TABLE{OCCURRENCE_MATRIX}
		       (occurrence_no, reid_no, latest_ident, collection_no, taxon_no, orig_no,
			genus_name, genus_reso, subgenus_name, subgenus_reso,
			species_name, species_reso, subspecies_name, subspecies_reso,
			plant_organ,
			early_age, late_age, reference_no,
			authorizer_no, enterer_no, modifier_no, created, modified)
		SELECT re.occurrence_no, re.reid_no, if(re.most_recent = 'YES', true, false),
			re.collection_no, re.taxon_no, a.orig_no, 
			re.genus_name, coalesce(re.genus_reso,''),
			coalesce(re.subgenus_name,''), coalesce(re.subgenus_reso,''),
			re.species_name, coalesce(re.species_reso,''),
			coalesce(re.subspecies_name,''), coalesce(re.subspecies_reso,''),
			re.plant_organ,
			ei.early_age, li.late_age, if(re.reference_no > 0, re.reference_no, 0),
			re.authorizer_no, re.enterer_no, re.modifier_no, re.created, re.modified
		FROM $TABLE{REID_DATA} as re JOIN coll_matrix as c using (collection_no)
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
			LEFT JOIN $TABLE{AUTHORITY_DATA} as a using (taxon_no)
		WHERE occurrence_no in ($occurrence_list)";
	
	debug_line($debug_out, "$sql\n") if $debug_out;
	
	$dbh->do($sql);
    }
}


sub updateOccurrenceMatrixReids {

    my ($dbh, $reid_nos, $debug_out) = @_;

    my @reid_nos;

    if ( ref $reid_nos eq 'ARRAY' )
    {
	@reid_nos = @$reid_nos;
    }

    else
    {
	@reid_nos = $reid_nos;
    }
    
    # Update the occurrence matrix, at most 1000 records at a time.

    my $sql;
    
    while ( @reid_nos )
    {
	my @work_list = splice(@reid_nos, 0, 1000);
	
	my $reid_list = join(',', map { $dbh->quote($_) } @work_list);
	
	$sql = "REPLACE INTO $TABLE{OCCURRENCE_MATRIX}
		       (occurrence_no, reid_no, latest_ident, collection_no, taxon_no, orig_no,
			genus_name, genus_reso, subgenus_name, subgenus_reso,
			species_name, species_reso, subspecies_name, subspecies_reso,
			plant_organ,
			early_age, late_age, reference_no,
			authorizer_no, enterer_no, modifier_no, created, modified)
		SELECT re.occurrence_no, re.reid_no, if(re.most_recent = 'YES', true, false),
			re.collection_no, re.taxon_no, a.orig_no, 
			re.genus_name, coalesce(re.genus_reso,''),
			coalesce(re.subgenus_name,''), coalesce(re.subgenus_reso,''),
			re.species_name, coalesce(re.species_reso,''),
			coalesce(re.subspecies_name,''), coalesce(re.subspecies_reso,''),
			re.plant_organ,
			ei.early_age, li.late_age, if(re.reference_no > 0, re.reference_no, 0),
			re.authorizer_no, re.enterer_no, re.modifier_no, re.created, re.modified
		FROM $TABLE{REID_DATA} as re JOIN coll_matrix as c using (collection_no)
			LEFT JOIN $TABLE{INTERVAL_DATA} as ei on ei.interval_no = c.early_int_no
			LEFT JOIN $TABLE{INTERVAL_DATA} as li on li.interval_no = c.late_int_no
			LEFT JOIN $TABLE{AUTHORITY_DATA} as a using (taxon_no)
		WHERE reid_no in ($reid_list)";
	
	debug_line($debug_out, "$sql\n") if $debug_out;
	
	$dbh->do($sql);
    }
}


sub deleteFromOccurrenceMatrix {
    
    my ($dbh, $occurrence_nos, $debug_out) = @_;
    
    my @occurrence_nos;

    if ( ref $occurrence_nos eq 'ARRAY' )
    {
	@occurrence_nos = @$occurrence_nos;
    }

    else
    {
	@occurrence_nos = $occurrence_nos;
    }
    
    # Update the occurrence matrix, at most 1000 records at a time.
    
    my $sql;
    
    while ( @occurrence_nos )
    {
	my @work_list = splice(@occurrence_nos, 0, 1000);
	
	my $occurrence_list = join(',', map { $dbh->quote($_) } @work_list);
	
	$sql = "DELETE FROM $TABLE{OCCURRENCE_MATRIX} WHERE occurrence_no in ($occurrence_list)";
	
	debug_line($debug_out, "$sql\n") if $debug_out;
	
	$dbh->do($sql);
    }
}


sub deleteReidsFromOccurrenceMatrix {

    my ($dbh, $reid_nos, $debug_out) = @_;
    
    my @reid_nos;

    if ( ref $reid_nos eq 'ARRAY' )
    {
	@reid_nos = @$reid_nos;
    }

    else
    {
	@reid_nos = $reid_nos;
    }
    
    # Update the occurrence matrix, at most 1000 records at a time.
    
    my $sql;
    
    while ( @reid_nos )
    {
	my @work_list = splice(@reid_nos, 0, 1000);
	
	my $reid_list = join(',', map { $dbh->quote($_) } @work_list);
	
	$sql = "DELETE FROM $TABLE{OCCURRENCE_MATRIX} WHERE reid_no in ($reid_list)";
	
	debug_line($debug_out, "$sql\n") if $debug_out;
	
	$dbh->do($sql);
    }
}


sub updateOccurrenceCounts {

    my ($dbh, $collection_nos, $debug_out) = @_;

    my @collection_nos;

    if ( ref $collection_nos eq 'ARRAY' )
    {
	@collection_nos = @$collection_nos;
    }

    else
    {
	@collection_nos = $collection_nos;
    }

    # Update the n_occs field of the collection matrix, at most 1000 records at
    # a time.

    my $sql;
    
    while ( @collection_nos )
    {
	my @work_list = splice(@collection_nos, 0, 1000);
	
	my $collection_list = join(',', map { $dbh->quote($_) } @work_list);
	
	$sql = "UPDATE $TABLE{COLLECTION_MATRIX} as c
		    left join (SELECT collection_no, count(*) as n_occs
			       FROM $TABLE{OCCURRENCE_DATA} GROUP BY collection_no) as occs
			using (collection_no)
		SET c.n_occs = if(occs.n_occs > 0, occs.n_occs, 0)
		WHERE collection_no in ($collection_list)";
	
	debug_line($debug_out, "$sql\n") if $debug_out;
	
	$dbh->do($sql);
    }
}    
	

sub debug_line {

    my ($debug_out, $message) = @_;
    
    if ( $debug_out && blessed($debug_out) && $debug_out->can('debug_line') )
    {
	$debug_out->debug_line($message);
    }

    elsif ( $debug_out )
    {
	print STDERR "$message\n";
    }
}

1;
