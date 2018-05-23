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
use TableDefs qw($OCC_MATRIX $SPEC_MATRIX $SPECELT_DATA $SPECELT_MAP $SPECELT_EXC);
use TaxonDefs qw(@TREE_TABLE_LIST);
use ConsoleLog qw(logMessage);

our (@EXPORT_OK) = qw(buildSpecimenTables buildMeasurementTables
		      init_specelt_tables load_specelt_tables build_specelt_map);

our $SPEC_MATRIX_WORK = "smw";

our $SPECELT_WORK = "seltw";
our $SPECELT_EXCLUSIONS_WORK = "sexw";
our $SPECELT_MAP_WORK = "semw";
our $TREE_TABLE = 'taxon_trees';


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


# init_specimen_element_tables ( dbh )
# 
# Create the tables for specimen elements.

sub init_specelt_tables {
    
    my ($dbh, $options) = @_;
    
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
    
    $dbh->do("DROP TABLE IF EXISTS $SPECELT_EXCLUSIONS_WORK");
    
    $dbh->do("CREATE TABLE $SPECELT_EXCLUSIONS_WORK (
		specelt_no int unsigned not null,
		taxon_name varchar(80) not null,
		KEY (specelt_no))");
    
    # $dbh->do("DROP TABLE IF EXISTS $SPECELT_MAP_WORK");
    
    # $dbh->do("CREATE TABLE $SPECELT_MAP_WORK (
    # 		specelt_no int unsigned not null,
    # 		base_no int unsigned not null,
    # 		exclude boolean,
    # 		lft int unsigned not null,
    # 		rgt int unsigned not null,
    # 		KEY (lft, rgt))");
    
    activateTables($dbh, $SPECELT_WORK => $SPECELT_DATA,
			 # $SPECELT_MAP_WORK => $SPECELT_MAP,
			 $SPECELT_EXCLUSIONS_WORK => $SPECELT_EXC);
}


our (%COLUMN_MAP) = (Taxon => 'taxon_name',
		     ExcludeTaxa => 'exclude_names',
		     SpecimenElement => 'element_name',
		     AlternateNames => 'alternate_names',
		     ParentElement => 'parent_name',
		     HasNumber => 'has_number',
		     Inactive => 'inactive',
		     NeotomaElementID => 'neotoma_element_id',
		     NeotomaElementTypeID => 'neotoma_element_type_id',
		     Comments => 'comments');

our (%TAXON_FIX) = (Eukarya => 'Eukaryota', Nympheaceae => 'Nymphaeaceae');

sub load_specelt_tables {

    my ($dbh, $filename, $options) = @_;
    
    $options ||= { };
    
    my ($sql, $insert_line, $result);
    
    my $csv = Text::CSV_XS->new();
    my ($fh, $count, $header, @rows, %column, %taxon_no_cache);
    
    if ( $filename eq '-' )
    {
	open $fh, "<&STDIN" or die "cannot read standard input: $!";
    }
    
    else
    {
	open $fh, "<", $filename or die "cannot read '$filename': $!";
    }
    
    while ( my $row = $csv->getline($fh) )
    {
	unless ( $header )
	{
	    $header = $row;
	}
	
	else
	{
	    push @rows, $row;
	    $count++;
	}
    }
    
    logMessage(2, "    read $count lines from '$filename'");
    
    foreach my $i ( 0..$#$header )
    {
	my $load_col = $COLUMN_MAP{$header->[$i]};
	$column{$load_col} = $i if $load_col;
    }
    
    my $inserter = "
	INSERT INTO $SPECELT_DATA (element_name, alternate_names, parent_name, status, taxon_name,
		has_number, neotoma_element_id, neotoma_element_type_id, comments)
	VALUES (";
    
    my @columns = qw(element_name alternate_names parent_name inactive taxon_name
		     has_number neotoma_element_id neotoma_element_type_id comments exclude_names);
    
    foreach my $k (@columns)
    {
	croak "could not find column for '$k' in input"
	    unless defined $column{$k};
    }
    
    my $rowcount = 0;
    
  ROW:
    foreach my $r ( @rows )
    {
	$rowcount++;
	
	my $taxon_name = $r->[$column{taxon_name}];
	my $base_no;
	
	unless ( $taxon_name )
	{
	    logMessage(2, "    WARNING: no taxon name for row $rowcount");
	    next ROW;
	}
	
	unless ( $base_no = $taxon_no_cache{$taxon_name} )
	{
	    next ROW if defined $base_no && $base_no == 0;
	    
	    my $quoted_name = $dbh->quote($taxon_name);
	    my $alternate = '';

	    if ( $TAXON_FIX{$taxon_name} )
	    {
		$alternate = 'or name = ' . $dbh->quote($TAXON_FIX{$taxon_name});
	    }
	    
	    ($base_no) = $dbh->selectrow_array("
		SELECT accepted_no FROM $TREE_TABLE WHERE name = $quoted_name $alternate");
	    
	    $taxon_no_cache{$taxon_name} = $base_no || 0;
	    
	    unless ( $base_no )
	    {
		logMessage(2, "    WARNING: could not find taxon name '$taxon_name'");
		next ROW;
	    }
	}
	
	my @values;
	my @exclude_names;
	
	foreach my $k (@columns)
	{
	    my $v = $r->[$column{$k}];
	    
	    # if ( $k eq 'taxon_name' )
	    # {
	    # 	push @values, $dbh->quote($base_no);
	    # 	next;
	    # }
	    
	    if ( $k eq 'inactive' )
	    {
		my $enum = $v ? 'inactive' : 'active';
		push @values, $dbh->quote($enum);
	    }
	    
	    elsif ( $k eq 'taxon_name' )
	    {
		$v = $TAXON_FIX{$v} if $TAXON_FIX{$v};

		unless ( $v )
		{
		    logMessage(2, "    WARNING: no taxon name for row $rowcount");
		    next ROW;
		}
		
		push @values, $dbh->quote($v);
	    }
	    
	    elsif ( $k eq 'exclude_names' )
	    {
		@exclude_names = split(/\s*[,;]\s*/, $v) if $v;
	    }
	    
	    elsif ( $k eq 'alternate_names' )
	    {
		my $main = $r->[$column{element_name}];
		my @names;
		
		@names = grep { $_ ne $main } split(/\s*[,;]\s*/, $v) if $v;

		my $list = join('; ', @names);
		
		push @values, $dbh->quote($list || '');
	    }
	    
	    elsif ( defined $v )
	    {
		push @values, $dbh->quote($v);
	    }
	    
	    else
	    {
		if ( $k eq 'element_name' )
		{
		    logMessage(2, "    WARNING: empty element name in row $rowcount");
		    next ROW;
		}
		
		push @values, 'NULL';
	    }
	}
	
	$sql = $inserter . join(',', @values) . ")";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$result = $dbh->do($sql);
	
	unless ( $result )
	{
	    my $errstr = $dbh->errstr;
	    logMessage(2, "    ERROR inserting row $rowcount: $errstr");
	}
	
	my $id = $dbh->last_insert_id(undef, undef, undef, undef);
	
	if ( $id && @exclude_names )
	{
	    $sql = "INSERT INTO $SPECELT_EXC (specelt_no, taxon_name) VALUES ";
	    
	    my @excludes;
	    
	    foreach my $n (@exclude_names)
	    {
		push @excludes, "($id," . $dbh->quote($n) . ")";
	    }
	    
	    $sql .= join(',', @excludes);
	    
	    print STDERR "$sql\n\n" if $options->{debug};
	    
	    $result = $dbh->do($sql);
	    
	    unless ( $result )
	    {
		my $errstr = $dbh->errstr;
		logMessage(2, "    ERROR inserting excludes for row $rowcount: $errstr");
	    }
	}
    }
    
    my ($count1) = $dbh->selectrow_array("SELECT count(*) FROM $SPECELT_DATA");
    my ($count2) = $dbh->selectrow_array("SELECT count(*) FROM $SPECELT_EXC");
    
    logMessage(2, "    inserted $count1 elements, $count2 exclusions");
}


# sub add_element_line {

#     my ($dbh, $line, $options) = @_;
    
#     my @fields = split /\s*,\s*/, $line;
    
#     my $taxon_name = line_value('Taxon', \@fields);
#     my $elt_name = line_value('SpecimenElement', \@fields);
#     my $alt_names = line_value('AlternateNames', \@fields);
#     my $parent_elt = line_value('ParentElement', \@fields);
#     my $neotoma_no = line_value('NeotomaElementID', \@fields) || "0";
#     my $neotoma_type_no = line_value('NeotomaElementTypeID', \@fields) || "0";
#     my $has_number = line_value('HasNumber', \@fields);
#     my $inactive = line_value('Inactive', \@fields);
    
#     next if $inactive;
    
#     $alt_names = '' if $alt_names eq $elt_name;
    
#     # Fix Eukarya
    
#     $taxon_name = 'Eukaryota' if $taxon_name eq 'Eukarya';
    
#     # Look up the taxon name in the database.
    
#     my ($orig_no, $lft, $rgt) = lookup_taxon($dbh, $taxon_name);
    
#     my $quoted_name = $dbh->quote($elt_name);
#     my $quoted_alt = $alt_names ? $dbh->quote($alt_names) : "''";
#     my $quoted_parent = $parent_elt ? $dbh->quote($parent_elt) : "''";
#     my $quoted_hasnum = $has_number ? "1" : "0";
#     my $quoted_neo = $dbh->quote($neotoma_no);
#     my $quoted_neotype = $dbh->quote($neotoma_type_no);
    
#     # Insert the record into the database.
    
#     my $sql = "	INSERT INTO $SPEC_ELEMENTS (element_name, alternate_names, orig_no, parent_elt_name,
# 			has_number, neotoma_element_id, neotoma_element_type_id)
# 		VALUES ($quoted_name, $quoted_alt, $orig_no, $quoted_parent,
# 			$quoted_hasnum, $quoted_neo, $quoted_neotype)";
    
#     print STDERR "$sql\n\n" if $options->{debug};
    
#     my $result = $dbh->do($sql);
    
#     my $insert_id = $dbh->last_insert_id(undef, undef, $SPEC_ELEMENTS, undef);
    
#     unless ( $insert_id )
#     {
# 	print STDERR "Error: element not inserted\n";
# 	next;
#     }
    
#     # If we know the taxon number, also insert a record into the element map.
    
#     # if ( $orig_no )
#     # {
#     # 	$sql = "	INSERT INTO $SPEC_ELT_MAP (spec_elt_no, lft, rgt)
#     # 		VALUES ($insert_id, $lft, $rgt)";
	
#     # 	print STDERR "$sql\n\n" if $options->{debug};
	
#     # 	$result = $dbh->do($sql);
#     # }
    
#     return $result;
# }


# sub line_value {
    
#     my ($column, $fields_ref) = @_;
    
#     my $i = $FIELD_MAP{$column};
#     croak "Column '$column' not found.\n" unless defined $i;
    
#     return $fields_ref->[$i];
# }


# sub lookup_taxon {
    
#     my ($dbh, $taxon_name) = @_;
    
#     unless ( $TAXON_CACHE{$taxon_name} )
#     {
# 	my $quoted = $dbh->quote($taxon_name);
	
# 	my $sql = "	SELECT orig_no, lft, rgt, name FROM $TREE_TABLE_LIST[0]
# 			WHERE name = $quoted";
	
# 	my ($orig_no, $lft, $rgt, $name) = $dbh->selectrow_array($sql);
	
# 	$orig_no ||= 0;
	
# 	print STDERR "WARNING: could not find taxon '$taxon_name'\n" unless $orig_no;
	
# 	$TAXON_CACHE{$taxon_name} = [ $orig_no, $lft, $rgt, $name ];
#     }
    
#     return @{$TAXON_CACHE{$taxon_name}};
# }


# init_specimen_element_tables ( dbh )
# 
# Create the tables for specimen elements.

# sub init_specimen_element_tables {
    
#     my ($dbh) = @_;
    
#     my ($sql, $result);
    
#     $dbh->do("DROP TABLE IF EXISTS $SPEC_ELT_WORK");
    
#     $dbh->do("CREATE TABLE $SPEC_ELT_WORK (
# 		spec_elt_no int unsigned PRIMARY KEY,
# 		element_name varchar(80) not null,
# 		parent_elt_no int unsigned not null,
# 		base_no int unsigned not null,
# 		neotoma_element_id int unsigned not null,
# 		neotoma_element_type_id int unsigned not null,
# 		KEY (element_name),
# 		KEY (neotoma_element_id),
# 		KEY (neotoma_element_type_id))");
    
#     $dbh->do("DROP TABLE IF EXISTS $SPEC_ELT_EXCLUSIONS_WORK");
    
#     $dbh->do("CREATE TABLE IF EXISTS $SPEC_ELT_EXCLUSIONS_WORK (
# 		spec_elt_no int unsigned not null,
# 		taxon_no int unsigned not null,
# 		KEY (spec_elt_no)");
    
#     $dbh->do("DROP TABLE IF EXISTS $SPEC_ELT_MAP_WORK");
    
#     $dbh->do("CREATE TABLE $SPEC_ELT_MAP_WORK (
# 		spec_elt_no int unsigned not null,
# 		lft int unsigned not null,
# 		rgt int unsigned not null,
# 		KEY (lft, rgt))");
    
    
# }

1;
