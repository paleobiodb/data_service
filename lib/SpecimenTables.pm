# 
# The Paleobiology Database
# 
#   SpecimenTables.pm
# 
# Build the tables needed by the data service for satisfying queries about
# specimens.


package SpecimenTables;

use strict;

use base 'Exporter';

use Carp qw(carp croak);
use Try::Tiny;
use Text::CSV_XS;

use CoreFunction qw(activateTables);

use TableDefs qw($OCC_MATRIX $SPEC_MATRIX $SPECELT_DATA $SPECELT_MAP $SPECELT_EXC
		 $OCCURRENCES $LOCALITIES $WOF_PLACES $COLL_EVENTS);
use TaxonDefs qw(@TREE_TABLE_LIST);
use ConsoleLog qw(logMessage);

our (@EXPORT_OK) = qw(buildSpecimenTables buildMeasurementTables
		      establish_specelt_tables load_specelt_tables build_specelt_map);

our $SPEC_MATRIX_WORK = "smw";
our $SPEC_ELTS_WORK = "sew";
our $ELT_MAP_WORK = "semw";
our $SPECELT_WORK = "seltw";
our $SPECELT_EXCLUSIONS_WORK = "sexw";
our $SPECELT_MAP_WORK = "semw";
our $TREE_TABLE = 'taxon_trees';
our $LOCALITY_WORK = "locw";
our $PLACES_WORK = "placw";
our $COLL_EVENT_WORK = "cevw";


# buildSpecimenTables ( dbh )
# 
# Build the specimen matrix, recording which the necessary information for
# efficiently satisfying queries about specimens.

sub buildSpecimenTables {
    
    my ($dbh, $options) = @_;
    
    my ($sql, $result, $count, $extra);
    
    # Create a clean working table which will become the new specimen
    # matrix.
    
    logMessage(1, "Building specimen tables");
    
    $result = $dbh->do("DROP TABLE IF EXISTS $SPEC_MATRIX_WORK");
    $result = $dbh->do("CREATE TABLE $SPEC_MATRIX_WORK (
				specimen_no int unsigned not null,
				occurrence_no int unsigned not null,
				reid_no int unsigned not null,
				latest_ident boolean not null,
				taxon_no int unsigned not null,
				orig_no int unsigned not null,
				reference_no int unsigned not null,
				authorizer_no int unsigned not null,
				enterer_no int unsigned not null,
				modifier_no int unsigned not null,
				created timestamp null,
				modified timestamp null,
				primary key (specimen_no, reid_no)) ENGINE=MyISAM");
    
    # Add one row for every specimen in the database.  For specimens tied to
    # occurrences that have multiple identifications, we create a separate row
    # for each identification.
    
    logMessage(2, "    inserting specimens...");
    
    $sql = "	INSERT INTO $SPEC_MATRIX_WORK
		       (specimen_no, occurrence_no, reid_no, latest_ident, taxon_no, orig_no,
			reference_no, authorizer_no, enterer_no, modifier_no, created, modified)
		SELECT s.specimen_no, s.occurrence_no, o.reid_no, ifnull(o.latest_ident, 1), 
		       if(s.taxon_no is not null and s.taxon_no > 0, s.taxon_no, o.taxon_no),
		       if(a.orig_no is not null and a.orig_no > 0, a.orig_no, o.orig_no),
		       s.reference_no, s.authorizer_no, s.enterer_no, s.modifier_no,
		       s.created, s.modified
		FROM specimens as s LEFT JOIN authorities as a using (taxon_no)
			LEFT JOIN $OCC_MATRIX as o on o.occurrence_no = s.occurrence_no";
    
    $count = $dbh->do($sql);
    
        # Now add some indices to the main occurrence relation, which is more
    # efficient to do now that the table is populated.
    
    logMessage(2, "    indexing by occurrence and reid...");
    
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX selection (occurrence_no, reid_no)");
    
    logMessage(2, "    indexing by taxon...");
    
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (taxon_no)");
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (orig_no)");
    
    logMessage(2, "    indexing by reference...");
    
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (reference_no)");
    
    logMessage(2, "    indexing by person...");
    
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (authorizer_no)");
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (enterer_no)");
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (modifier_no)");
    
    logMessage(2, "    indexing by timestamp...");
    
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (created)");
    $result = $dbh->do("ALTER TABLE $SPEC_MATRIX_WORK ADD INDEX (modified)");
    
    # Then activate the new tables.
    
    activateTables($dbh, $SPEC_MATRIX_WORK => $SPEC_MATRIX);
    
    my $a = 1;	# we can stop here when debugging
}


# buildMeasurementTables ( dbh )
# 
# Build the measurement matrix, recording which the necessary information for
# efficiently satisfying queries about measurements.

sub buildMeasurementTables {
    
}


# build_specelt_map ( dbh )
# 
# Build the specimen element map, according to the current taxonomy.

sub build_specelt_map {
    
    my ($dbh, $tree_table, $options) = @_;
    
    $options ||= { };
    
    $dbh->do("DROP TABLE IF EXISTS $SPECELT_MAP_WORK");
    
    $dbh->do("CREATE TABLE $SPECELT_MAP_WORK (
    		specelt_no int unsigned not null,
    		base_no int unsigned not null,
    		exclude boolean not null,
		check_value tinyint unsigned not null,
    		lft int unsigned not null,
    		rgt int unsigned not null,
    		KEY (lft, rgt, exclude))");
    
    my ($sql, $result);
    
    # First add all of the elements directly.
    
    $sql = "INSERT INTO $SPECELT_MAP_WORK (specelt_no, base_no, check_value, lft, rgt)
	    SELECT e.specelt_no, t.orig_no, count(distinct t.orig_no), t.lft, t.rgt
	    FROM $SPECELT_DATA as e join $tree_table as t1 on t1.name = e.taxon_name
		join $tree_table as t on t.orig_no = t1.accepted_no
	    WHERE t.rank > 5 and e.status = 'active' GROUP BY e.specelt_no";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    # Then add any exclusions.
    
    $sql = "INSERT INTO $SPECELT_MAP_WORK (specelt_no, base_no, exclude, check_value, lft, rgt)
	    SELECT x.specelt_no, t.orig_no, 1, count(distinct t.orig_no), t.lft, t.rgt
	    FROM $SPECELT_EXC as x join $SPECELT_DATA as e using (specelt_no)
		join $tree_table as t1 on t1.name = x.taxon_name
		join $tree_table as t on t.orig_no = t1.accepted_no
	    WHERE t.rank > 5 and e.status = 'active' GROUP BY x.specelt_no, x.taxon_name";

    print STDERR "$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    my ($check_count) = $dbh->selectrow_array("
	SELECT count(*) FROM $SPECELT_MAP_WORK
	WHERE check_value > 1");
    
    logMessage(2, "    found $check_count grouped entries") if $check_count && $check_count > 0;
    
    activateTables($dbh, $SPECELT_MAP_WORK => $SPECELT_MAP);
}


# establish_extra_specimen_tables ( dbh )
# 
# Create additional tables necessary for specimen entry.

sub establish_extra_specimen_tables {

    my ($dbh, $options) = @_;
    
    $options ||= { };
    
    logMessage(1, "Adding tables and columns for new specimens system");
    
    $dbh->do("DROP TABLE IF EXISTS $PLACES_WORK");
    
    $dbh->do("CREATE TABLE $PLACES_WORK (
		wof_id int unsigned PRIMARY KEY,
		name varchar(255) not null,
		name_formal varchar(255) not null,
		name_eng varchar(255) not null,
		placetype enum ('continent','country','region','county','locality'),
		iso2 varchar(2) not null,
		iso3 varchar(3) not null,
		continent int unsigned not null,
		country int unsigned not null,
		region int unsigned not null,
		county int unsigned not null,
		locality int unsigned not null,
		geom geometry not null,
		wkt longtext,
		INDEX (name),
		INDEX (name_eng),
		SPATIAL INDEX (geom)) engine=MyISAM");
    
    $dbh->do("DROP TABLE IF EXISTS $LOCALITY_WORK");
    
    $dbh->do("CREATE TABLE $LOCALITY_WORK (
		locality_no int unsigned auto_increment PRIMARY KEY,
		wof_id int unsigned not null,
		site_name varchar(255) not null,
		verbatim_location text not null,
		early_int_no int unsigned not null,
		late_int_no int unsigned not null,
		stratgroup varchar(255) not null,
		formation varchar(255) not null,
		member varchar(255) not null,
		lithology varchar(255) not null,
		INDEX (wof_id))");
    
    $dbh->do("DROP TABLE IF EXISTS $COLL_EVENT_WORK");
    
    $dbh->do("CREATE TABLE $COLL_EVENT_WORK (
    		coll_event_no int unsigned auto_increment PRIMARY KEY,
		locality_no int unsigned not null,
    		field_site varchar(255) not null,
    		year varchar(4),
    		coll_date date,
    		collector_name varchar(255) not null,
    		collection_comments text,
    		geology_comments text,
    		INDEX (year))");

    # $dbh->do("DROP TABLE IF EXISTS $SPEC_OCCURRENCES");
    
    # my ($reso_list) = "'','aff.','cf.','ex gr.','n. gen.','sensu lato','?','\"','informal'";
    
    # $dbh->do("CREATE TABLE $SPEC_OCCURRENCES (
    # 		spec_occurrence_no int unsigned auto_increment PRIMARY KEY,
    # 		spec_reid_no int unsigned not null,
    # 		locality_no int unsigned not null,
    # 		authorizer_no int unsigned not null,
    # 		enterer_no int unsigned not null,
    # 		modifier_no int unsigned not null,
    # 		taxon_no int unsigned not null,
    # 		genus_reso enum($reso_list),
    # 		genus_name varchar(255) not null,
    # 		subgenus_reso enum($reso_list),
    # 		subgenus_name varchar(255) not null,
    # 		species_reso enum($reso_list),
    # 		species_name varchar(255) not null,
    # 		subspecies_reso enum($reso_list),
    # 		subspecies_name vachar(255) not null,
    # 		reference_no int unsigned not null,
    # 		created timestamp DEFAULT CURRENT_TIMESTAMP,
    # 		modified DEFAULT CURRENT_TIMESTAMP,
    # 		INDEX (locality_no),
    # 		INDEX (reference_no),
    # 		INDEX (authorizer_no),
    # 		INDEX (enterer_no),
    # 		INDEX (modifier_no),
    # 		INDEX (genus_name),
    # 		INDEX (species_name))");
    
    activateTables($dbh, $PLACES_WORK => $WOF_PLACES, $LOCALITY_WORK => $LOCALITIES,
			 $COLL_EVENT_WORK => $COLL_EVENTS);
    
    # Now add columns to the 'occurrences' table, if they aren't already there.
    
    my ($occ_columns) = $dbh->selectall_arrayref("SHOW COLUMNS FROM $OCCURRENCES", { Slice => { } });
    my (%occ_column, $result);
    
    if ( ref $occ_columns eq 'ARRAY' )
    {
	foreach my $col ( @$occ_columns )
	{
	    my $field_name = $col->{Field};
	    my $type = $col->{Type};
	    $occ_column{$field_name} = $type;
	}
	
	if ( $occ_column{locality_no} )
	{
	    logMessage(2, "Column 'locality_no' already present in $OCCURRENCES")
	}
	
	else
	{
	    $result = $dbh->do("ALTER TABLE $OCCURRENCES add locality_no int unsigned not null after collection_no");
	    logMessage(2, "Added column 'locality_no' to $OCCURRENCES");
	}
	
	if ( $occ_column{coll_event_no} )
	{
	    logMessage(2, "Column 'coll_event_no' already present in $OCCURRENCES");
	}
	
	else
	{
	    $result = $dbh->do("ALTER TABLE $OCCURRENCES add coll_event_no int unsigned not null after locality_no");
	    logMessage(2, "Added column 'coll_event_no' to $OCCURRENCES");
	}
    }
    
    else
    {
	logMessage(1, "ERROR: could not query columns from $OCCURRENCES");
    }
    
    # Now add columns for the 'specimens' table, if they aren't already there.
    
    my ($spec_columns) = $dbh->selectall_arrayref("SHOW COLUMNS FROM $SPECIMENS", { Slice => { } });
    my (%spec_column);
    
    my (@new_columns) = qw(specelt_no);
    
    my (%new_column) = ( instcoll_no => 'int unsigned not null',
			 inst_code => 'varchar(20) not null',
			 coll_code => 'varchar(20) not null',
			 specelt_no => 'int unsigned not null' );
    
    my (%new_after) = ( instcoll_no => 'specimen_coverage',
			inst_code => 'instcoll_no',
			coll_code => 'inst_code',
			specelt_no => 'specimen_id' );
    
    if ( ref $spec_columns eq 'ARRAY' )
    {
	foreach my $col ( @$spec_columns )
	{
	    my $field_name = $col->{Field};
	    my $type = $col->{Type};
	    $spec_column{$field_name} = $type;
	}
	
	foreach my $new ( @new_columns )
	{
	    if ( $spec_column{$new} )
	    {
		logMessage(2, "Column '$new' already present in $SPECIMENS")
	    }
	    
	    else
	    {
		my $spec = "$new_column{$new} after $new_after{$new}";
		$result = $dbh->do("ALTER TABLE $SPECIMENS add $new $spec");
		logMessage(2, "Added column '$new' to $SPECIMENS");
	    }
	}
    }
    
    else
    {
	logMessage(1, "ERROR: could not query columns from $SPECIMENS");
    }
    
# # add to specimens

# instcoll_no
# occurrence_no
# inst_code
# coll_code
# specelt_no
    
}


# establish_specimen_element_tables ( dbh )
# 
# Create the tables for specimen elements.

sub establish_spec_element_tables {
    
    my ($dbh, $options) = @_;

    $options ||= { };
    
    my ($sql, $result);
    
    $dbh->do("DROP TABLE IF EXISTS $SPECELT_WORK");
    
    $dbh->do("CREATE TABLE $SPECELT_WORK (
		specelt_no int unsigned PRIMARY KEY AUTO_INCREMENT,
		element_name varchar(80) not null,
		alternate_names varchar(255) not null default '',
		parent_name varchar(80) not null default '',
		taxon_name varchar(80) not null,
		status enum ('active', 'inactive') not null default 'active',
		has_number boolean not null default 0,
		neotoma_element_id int unsigned not null default 0,
		neotoma_element_type_id int unsigned not null default 0,
		comments varchar(255) null,
		KEY (element_name),
		KEY (neotoma_element_id),
		KEY (neotoma_element_type_id))");
    
}


sub add_element_line {
    
    my ($dbh, $line, $options) = @_;
    
    my @fields = split /\s*,\s*/, $line;
    
    my $taxon_name = line_value('Taxon', \@fields);
    my $elt_name = line_value('SpecimenElement', \@fields);
    my $alt_names = line_value('AlternateNames', \@fields);
    my $parent_elt = line_value('ParentElement', \@fields);
    my $neotoma_no = line_value('NeotomaElementID', \@fields) || "0";
    my $neotoma_type_no = line_value('NeotomaElementTypeID', \@fields) || "0";
    my $has_number = line_value('HasNumber', \@fields);
    my $inactive = line_value('Inactive', \@fields);
    
    next if $inactive;
    
    $alt_names = '' if $alt_names eq $elt_name;
    
    # Fix Eukarya
    
    $taxon_name = 'Eukaryota' if $taxon_name eq 'Eukarya';
    
    # Look up the taxon name in the database.
    
    my ($orig_no, $lft, $rgt) = lookup_taxon($dbh, $taxon_name);
    
    my $quoted_name = $dbh->quote($elt_name);
    my $quoted_alt = $alt_names ? $dbh->quote($alt_names) : "''";
    my $quoted_parent = $parent_elt ? $dbh->quote($parent_elt) : "''";
    my $quoted_hasnum = $has_number ? "1" : "0";
    my $quoted_neo = $dbh->quote($neotoma_no);
    my $quoted_neotype = $dbh->quote($neotoma_type_no);
    
    # Insert the record into the database.
=======
    my $inserter = "
	INSERT INTO $SPECELT_DATA (element_name, alternate_names, parent_name, status, taxon_name,
		has_number, neotoma_element_id, neotoma_element_type_id, comments)
	VALUES (";
    
    my @columns = qw(element_name alternate_names parent_name inactive taxon_name
		     has_number neotoma_element_id neotoma_element_type_id comments exclude_names);
>>>>>>> bd99c85b36d762ea4a97b55f2b23d76e5b83a821
    
    my $sql = "	INSERT INTO $SPEC_ELEMENTS (element_name, alternate_names, orig_no, parent_elt_name,
			has_number, neotoma_element_id, neotoma_element_type_id)
		VALUES ($quoted_name, $quoted_alt, $orig_no, $quoted_parent,
			$quoted_hasnum, $quoted_neo, $quoted_neotype)";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    my $result = $dbh->do($sql);
    
    my $insert_id = $dbh->last_insert_id(undef, undef, $SPEC_ELEMENTS, undef);
    
    unless ( $insert_id )
    {
	print STDERR "Error: element not inserted\n";
	next;
    }
    
    return $result;
}


sub line_value {
    
    my ($column, $fields_ref) = @_;
    
    my $i = $FIELD_MAP{$column};
    croak "Column '$column' not found.\n" unless defined $i;
    
    return $fields_ref->[$i];
}


sub lookup_taxon {
    
    my ($dbh, $taxon_name) = @_;
    
    unless ( $TAXON_CACHE{$taxon_name} )
    {
	my $quoted = $dbh->quote($taxon_name);
	
	my $sql = "	SELECT orig_no, lft, rgt, name FROM $TREE_TABLE_LIST[0]
			WHERE name = $quoted";
	
	my ($orig_no, $lft, $rgt, $name) = $dbh->selectrow_array($sql);
	
	$orig_no ||= 0;
	
	print STDERR "WARNING: could not find taxon '$taxon_name'\n" unless $orig_no;
	
	$TAXON_CACHE{$taxon_name} = [ $orig_no, $lft, $rgt, $name ];
    }
    
    return @{$TAXON_CACHE{$taxon_name}};
}

1;
