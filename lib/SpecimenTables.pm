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

use CoreFunction qw(activateTables);
use TableDefs qw($OCC_MATRIX $SPEC_MATRIX $SPEC_ELEMENTS $LOCALITIES $WOF_PLACES);
use TaxonDefs qw(@TREE_TABLE_LIST);
use ConsoleLog qw(logMessage);

our (@EXPORT_OK) = qw(buildSpecimenTables buildMeasurementTables
			establish_spec_element_tables load_spec_element_tables);

our $SPEC_MATRIX_WORK = "smw";
our $SPEC_ELTS_WORK = "sew";
our $ELT_MAP_WORK = "semw";

our $LOCALITY_WORK = "locw";
our $PLACES_WORK = "placw";


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


# establish_extra_specimen_tables ( dbh )
# 
# Create additional tables necessary for specimen entry.

sub establish_extra_specimen_tables {

    my ($dbh, $options) = @_;
    
    $options ||= { };
    
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
		geom geometry,
		INDEX (name),
		INDEX (name_eng),
		SPATIAL INDEX (geom))");
    
    $dbh->do("DROP TABLE IF EXISTS $LOCALITY_WORK");
    
    $dbh->do("CREATE TABLE $LOCALITY_WORK (
		locality_no int unsigned auto_increment PRIMARY KEY,
		collection_name varchar(255) not null,
		wof_id int unsigned not null,
		early_int_no int unsigned not null,
		late_int_no int unsigned not null,
		grp varchar(255) not null,
		formation varchar(255) not null,
		member varchar(255) not null,
		lithology varchar(255) not null,
		INDEX (wof_id))");
    
    # $dbh->do("DROP TABLE IF EXISTS $COLLEVENT_WORK");
    
    # $dbh->do("CREATE TABLE $COLLEVENT_WORK (
    # 		collevent_no int unsigned auto_increment PRIMARY KEY,
    # 		field_site varchar(255) not null,
    # 		year varchar(4),
    # 		coll_date date,
    # 		collector_name varchar(255) not null,
    # 		collection_comments text,
    # 		geology_comments text,
    # 		INDEX (year))");

# # add to specimens

# instcoll_no
# occurrence_no
# inst_code
# coll_code
# spec_elt_no

# # create table "spec_occurrences" with occurrence_no > 5000000

# occurrence_no
# reid_no?
# authorizer_no
# enterer_no
# modifier_no
# locality_no
# orig_no
# genus_name
# genus_reso
# subgenus_name
# subgenus_reso
# species_name
# species_reso
# subspecies_name
# subspecies_reso
# created
# modified

}


# establish_specimen_element_tables ( dbh )
# 
# Create the tables for specimen elements.

sub establish_spec_element_tables {
    
    my ($dbh, $options) = @_;
    
    my ($sql, $result);
    
    $options ||= { };
    
    $dbh->do("DROP TABLE IF EXISTS $SPEC_ELTS_WORK");
    
    $dbh->do("CREATE TABLE $SPEC_ELTS_WORK (
		spec_elt_no int unsigned PRIMARY KEY AUTO_INCREMENT,
		element_name varchar(80) not null,
		alternate_names varchar(80) not null,
		orig_no int unsigned not null,
		parent_elt_name varchar(80) not null,
		has_number boolean,
		neotoma_element_id int unsigned not null,
		neotoma_element_type_id int unsigned not null,
		KEY (element_name),
		KEY (neotoma_element_id),
		KEY (neotoma_element_type_id))");
    
    # $dbh->do("DROP TABLE IF EXISTS $SPEC_ELT_EXCLUSIONS");
    
    # $dbh->do("CREATE TABLE IF EXISTS $SPEC_ELT_EXCLUSIONS (
    # 		spec_elt_no int unsigned not null,
    # 		taxon_no int unsigned not null,
    # 		KEY (spec_elt_no)");
    
    # $dbh->do("DROP TABLE IF EXISTS $ELT_MAP_WORK");
    
    # $dbh->do("CREATE TABLE $ELT_MAP_WORK (
    # 		spec_elt_no int unsigned not null,
    # 		lft int unsigned not null,
    # 		rgt int unsigned not null,
    # 		KEY (lft, rgt))");
    
    # Then activate the new tables.
    
    activateTables($dbh, $SPEC_ELTS_WORK => $SPEC_ELEMENTS);
}


my %TAXON_CACHE;
my %FIELD_MAP;

# load_spec_element_tables
# 
# 

sub load_spec_element_tables {

    my ($dbh, $new_contents, $options) = @_;
    
    my ($sql, $result, $header);
    
    $options ||= { };
    
    # First grab and parse the header line
    
    if ( ref $new_contents eq 'ARRAY' )
    {
	$header = shift @$new_contents;
	chomp $header;
    }
    
    elsif ( ref $new_contents eq 'GLOB' )
    {
	$header = <$new_contents>;
	chomp $header;
    }
    
    else
    {
	croak "new contents must be an array ref or file handle\n";
    }
    
    my @fields = split /\s*,\s*/, $header;
    %FIELD_MAP = ( );
    
    foreach my $i ( 0 .. $#fields )
    {
	$FIELD_MAP{$fields[$i]} = $i;
    }
    
    # Delete existing contents of the specimen elements table and the map table.
    
    $result = $dbh->do("TRUNCATE TABLE $SPEC_ELEMENTS");
    # $result = $dbh->do("TRUNCATE TABLE $SPEC_ELT_MAP");
    
    # Then go through the lines one by one and add the contents to these tables.
    
    if ( ref $new_contents eq 'ARRAY' )
    {
	foreach my $line ( @$new_contents )
	{
	    chomp $line;
	    next unless defined $line && $line ne '';
	    
	    add_element_line($dbh, $line, $options);
	}
    }
    
    elsif ( ref $new_contents eq 'GLOB' )
    {
	my $line;
	
	logMessage(2, "    Reading data from standard input...");
	
	while ( defined( $line = <$new_contents> ) )
	{
	    chomp $line;
	    next if $line eq '';
	    
	    add_element_line($dbh, $line, $options);
	}
	
	logMessage(2, "    done.");
    }
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
    
    # If we know the taxon number, also insert a record into the element map.
    
    # if ( $orig_no )
    # {
    # 	$sql = "	INSERT INTO $SPEC_ELT_MAP (spec_elt_no, lft, rgt)
    # 		VALUES ($insert_id, $lft, $rgt)";
	
    # 	print STDERR "$sql\n\n" if $options->{debug};
	
    # 	$result = $dbh->do($sql);
    # }
    
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
