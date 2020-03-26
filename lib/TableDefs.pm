# 
# The Paleobiology Database
# 
#   TableDefs.pm
#
# This file specifies the database tables to be used by the data service code.
#
# This allows for tables to be later renamed without having to search laboriously through the code
# for each statement referring to them. It also provides for operating the data service in "test
# mode", which can select alternate tables for use in running the unit tests.

package TableDefs;

use strict;

use Carp qw(carp croak);
use Hash::Util qw(lock_value unlock_value lock_keys unlock_keys);

use base 'Exporter';

our (@EXPORT_OK) = qw($COLLECTIONS $AUTHORITIES $OPINIONS $REFERENCES $OCCURRENCES $REIDS $SPECIMENS
		      $PERSON_DATA $SESSION_DATA $TABLE_PERMS $WING_USERS
		      $COLL_MATRIX $COLL_BINS $COLL_STRATA $COUNTRY_MAP $CONTINENT_DATA
		      $COLL_LITH $COLL_ENV $STRATA_NAMES
		      $BIN_KEY $BIN_LOC $BIN_CONTAINER
		      $PALEOCOORDS $GEOPLATES $COLL_LOC $COLL_INTS
		      $DIV_MATRIX $DIV_GLOBAL $PVL_MATRIX $PVL_GLOBAL
		      $OCC_MATRIX $OCC_TAXON $REF_SUMMARY $SPEC_MATRIX
		      $SPEC_ELEMENTS $SPEC_ELT_MAP $LOCALITIES $WOF_PLACES $COLL_EVENTS
		      $OCC_BUFFER_MAP $OCC_MAJOR_MAP $OCC_CONTAINED_MAP $OCC_OVERLAP_MAP
		      $SPECELT_DATA $SPECELT_MAP $SPECELT_EXC
		      $INTERVAL_DATA $INTERVAL_MAP $INTERVAL_BRACKET $INTERVAL_BUFFER
		      $SCALE_DATA $SCALE_LEVEL_DATA $SCALE_MAP
		      $PHYLOPICS $PHYLOPIC_NAMES $PHYLOPIC_CHOICE $TAXON_PICS
		      $IDIGBIO
		      $MACROSTRAT_LITHS
		      $MACROSTRAT_INTERVALS $MACROSTRAT_SCALES $MACROSTRAT_SCALES_INTS
		      $TIMESCALE_DATA $TIMESCALE_ARCHIVE
		      $TIMESCALE_REFS $TIMESCALE_INTS $TIMESCALE_BOUNDS $TIMESCALE_PERMS
		      $TEST_DB %TABLE
		      %COMMON_FIELD_SPECIAL %COMMON_FIELD_IDTYPE %FOREIGN_KEY_TABLE %FOREIGN_KEY_COL 
		      init_table_names enable_test_mode disable_test_mode is_test_mode
		      get_table_name set_table_name set_table_group get_table_group
		      change_table_db change_table_name restore_table_name original_table
		      set_table_property get_table_property
		      set_column_property get_column_properties list_column_properties
		      substitute_table alternate_table);


# Define the properties that are allowed to be specified for tables and table columns.

our (%TABLE_PROP_NAME) = ( CAN_POST => 1,		# specifies who is allowed to add new records
			   CAN_VIEW => 1,		# specifies who is allowed to view records
			   CAN_MODIFY => 1,		# specifies who is allowed to modify existing records
			   ALLOW_DELETE => 1,		# if true, then deletion is allowed without admin
			   ALLOW_INSERT_KEY => 1,	# if true, then insertion allowed with specified primary key
			   CASCADE_DELETE => 1,		# specifies additional tables whose entries should be deleted
                                                        # when a record in this table is deleted.
			   BY_AUTHORIZER => 1,		# if true, then editing permission by authorizer_no
			   SUPERIOR_TABLE => 1,	# specifies table which controls permissions for this one
			   SUPERIOR_KEY => 1,		# specifies a field to be matched to the p.t. primary key
			   PRIMARY_KEY => 1,		# specifies the primary key column(s) for this table
			   PRIMARY_FIELD => 1,		# specifies the primary key record field for this table
			   NO_LOG => 1,			# specifies that changes to this table should not be logged
			   TABLE_COMMENT => 1 );	# specifies a comment relating to the table

our (%COLUMN_PROP_NAME) = ( ALTERNATE_NAME => 1,
			    ALTERNATE_ONLY => 1,
			    FOREIGN_KEY => 1,
			    FOREIGN_TABLE => 1,
			    EXTID_TYPE => 1,
			    ALLOW_TRUNCATE => 1,
			    VALUE_SEPARATOR => 1,
			    VALIDATOR => 1,
			    REQUIRED => 1,
			    NOT_NULL => 1,
			    ADMIN_SET => 1,
			    IGNORE => 1,
			    COLUMN_COMMENT => 1 );


# Define the properties of certain fields that are common to many tables in the PBDB.

our (%FOREIGN_KEY_TABLE) = ( taxon_no => 'AUTHORITY_DATA',
			     resource_no => 'REFERENCE_DATA',
			     collection_no => 'COLLECTION_DATA',
			     occurrence_no => 'OCCURRENCE_DATA',
			     specimen_no => 'SPECIMEN_DATA',
			     measurement_no => 'MEASUREMENT_DATA',
			     specelt_no => 'SPECELT_DATA',
			     reid_no => 'REID_DATA',
			     opinion_no => 'OPINION_DATA',
			     interval_no => 'INTERVAL_DATA',
			     timescale_no => 'TIMESCALE_DATA',
			     bound_no => 'TIMESCALE_BOUNDS',
			     eduresource_no => 'RESOURCE_QUEUE',
			     person_no => 'WING_USERS',
			     authorizer_no => 'WING_USERS',
			     enterer_no => 'WING_USERS',
			     modifier_no => 'WING_USERS');

our (%FOREIGN_KEY_COL) = ( authorizer_no => 'person_no',
			   enterer_no => 'person_no',
			   modifier_no => 'person_no' );

our (%COMMON_FIELD_IDTYPE) = ( taxon_no => 'TID',
			       orig_no => 'TXN',
			       reference_no => 'REF',
			       collection_no => 'COL',
			       occurrence_no => 'OCC',
			       specimen_no => 'SPM',
			       measurement_no => 'MEA',
			       specelt_no => 'ELS',
			       reid_no => 'REI',
			       opinion_no => 'OPN',
			       interval_no => 'INT',
			       timescale_no => 'TSC',
			       bound_no => 'BND',
			       eduresource_no => 'EDR',
			       person_no => 'PRS',
			       authorizer_no => 'PRS',
			       enterer_no => 'PRS',
			       modifier_no => 'PRS' );

our (%COMMON_FIELD_SPECIAL) = ( authorizer_no => 'authent',
				enterer_no => 'authent',
				modifier_no => 'authent',
				enterer_id => 'authent',
				created => 'crmod',
				modified => 'crmod',
				admin_lock => 'admin',
			        owner_lock => 'owner' );


# Define the mechanism for substituting test tables instead of real ones.

our ($TEST_MODE, $TEST_DB);


# init_table_names ( config, test_mode )
# 
# If this subroutine is run with $test_mode true, then it enables the flag that allows switching
# over to the test tables using 'select_test_tables'.

sub init_table_names
{
    my ($config, $test_mode) = @_;
    
    if ( $test_mode )
    {
	$TEST_MODE = $test_mode;
	$TEST_DB = $config->{test_db};
    }
}


sub is_test_mode
{
    return $TEST_MODE;
}


# classic tables

our $COLLECTIONS = "collections";
our $AUTHORITIES = "authorities";
our $OPINIONS = "opinions";
our $REFERENCES = "refs";
our $OCCURRENCES = "occurrences";
our $SPECIMENS = "specimens";
our $REIDS = "reidentifications";
our $SPECIMENS = "specimens";
our $MEASUREMENTS = "measurements";

set_table_property($COLLECTIONS, PRIMARY_KEY => 'collection_no');
set_table_property($AUTHORITIES, PRIMARY_KEY => 'taxon_no');
set_table_property($OPINIONS, PRIMARY_KEY => 'opinion_no');
set_table_property($REFERENCES, PRIMARY_KEY => 'reference_no');
set_table_property($OCCURRENCES, PRIMARY_KEY => 'occurrence_no');
set_table_property($REIDS, PRIMARY_KEY => 'reid_no');
set_table_property($SPECIMENS, PRIMARY_KEY => 'specimen_no');


# Authentication and permission tables

# our $PERSON_DATA = "person";
# our $TABLE_PERMS = "table_permissions";
# our $SESSION_DATA = "session_data";
# our $WING_USERS = "pbdb_wing.users";

set_table_name(PERSON_DATA => 'person');
set_table_name(TABLE_PERMS => 'table_permissions');
set_table_name(SESSION_DATA => 'session_data');
set_table_name(WING_USERS => 'pbdb_wing.users');

set_table_group('session_data' => 'PERSON_DATA', 'TABLE_PERMS', 'SESSION_DATA', 'WING_USERS');

set_table_property(PERSON_DATA => PRIMARY_KEY => 'person_no');
set_table_property(SESSION_DATA => PRIMARY_KEY => 'session_id');
set_table_property(WING_USERS => PRIMARY_KEY => 'id');

# new collection tables

our $COLL_MATRIX = "coll_matrix";
our $COLL_BINS = "coll_bins";
our $COLL_INTS = "coll_ints";
our $COLL_STRATA = "coll_strata";
our $STRATA_NAMES = "strata_names";
our $COLL_LOC = "coll_loc";
our $COUNTRY_MAP = "country_map";
our $CONTINENT_DATA = "continent_data";
our $BIN_LOC = "bin_loc";
our $BIN_CONTAINER = "bin_container";
our $PALEOCOORDS = 'paleocoords';
our $GEOPLATES = 'geoplates';

our $COLL_LITH = 'coll_lith';
our $COLL_ENV = 'coll_env';

our $BIN_KEY = "999999";

# new occurrence tables

our $OCC_MATRIX = "occ_matrix";
our $OCC_EXTRA = "occ_extra";
our $OCC_TAXON = "occ_taxon";
our $REF_SUMMARY = "ref_summary";

our $OCC_BUFFER_MAP = 'occ_buffer_map';
our $OCC_MAJOR_MAP = 'occ_major_map';
our $OCC_CONTAINED_MAP = 'occ_contained_map';
our $OCC_OVERLAP_MAP = 'occ_overlap_map';

# new specimen tables

our $SPEC_MATRIX = "spec_matrix";
our $SPECELT_DATA = "specelt_data";
our $SPECELT_EXC = "specelt_exc";
our $SPECELT_MAP = "specelt_map";

our $LOCALITIES = "localities";
our $WOF_PLACES = "wof_places";
our $COLL_EVENTS = "coll_events";

# new interval tables

our $INTERVAL_DATA = "interval_data";
our $SCALE_DATA = "scale_data";
our $SCALE_LEVEL_DATA = "scale_level_data";
our $SCALE_MAP = "scale_map";
our $INTERVAL_BRACKET = "interval_bracket";
our $INTERVAL_MAP = "interval_map";
our $INTERVAL_BUFFER = "interval_buffer";

set_table_name(INTERVAL_DATA => 'interval_data');

# taxon pic tables

our $PHYLOPICS = 'phylopics';
our $PHYLOPIC_NAMES = 'phylopic_names';
our $PHYLOPIC_CHOICE = 'phylopic_choice';
our $TAXON_PICS = 'taxon_pics';

# taxon diversity and prevalence tables

our $DIV_MATRIX = 'div_matrix';
our $DIV_GLOBAL = 'div_global';
our $PVL_MATRIX = 'pvl_matrix';
our $PVL_GLOBAL = 'pvl_global';

# iDigBio external info table

our $IDIGBIO = 'idigbio';

# Macrostrat tables that we use

our $MACROSTRAT_LITHS = 'macrostrat.liths';
our $MACROSTRAT_INTERVALS = 'macrostrat.intervals';
our $MACROSTRAT_SCALES = 'macrostrat.timescales';
our $MACROSTRAT_SCALES_INTS = 'macrostrat.timescales_intervals';

# New timescale system

our $TIMESCALE_DATA = 'timescales';
our $TIMESCALE_REFS = 'timescale_refs';
our $TIMESCALE_INTS = 'timescale_ints';
our $TIMESCALE_BOUNDS = 'timescale_bounds';
our $TIMESCALE_QUEUE = 'timescale_queue';
our $TIMESCALE_PERMS = 'timescale_perms';


# Define a routine to print debugging output.

# debug_line ( line, debug )
#
# Print out the specified line according to the parameter $debug. If the value is 'test', then we
# are running under the Test::More framework and should use the 'diag' routine from that
# module. If the value is an object with a 'debug_line' method, call that method. An example of
# such an object would be a Web::DataService request.

sub debug_line {

    my ($line, $debug) = @_;
    
    return unless $debug;
    
    if ( $debug eq 'test' )
    {
	Test::More::diag($line);
    }
    
    elsif ( ref $debug && $debug->can('debug_line') )
    {
	$debug->debug_line($line) if $debug->debug;
    }
    
    else
    {
	print STDERR $line;
    }
}


# Define global hash variables to hold table properties and column properties, in a way that can
# be accessed by other modules. Routines for getting and setting these appear below.

our (%TABLE_PROPERTIES, %COLUMN_PROPERTIES);


# Now define routines for getting and setting table properties. We ignore any database prefix on
# the table name, because we want the properties to be the same regardless of whether they are in
# the main database, the test database, or some other database we have subsequently defined. We
# are operating under the assumption that two tables with the same name in different databases are
# meant to be alternatives to each other, i.e. a main table and a test table.


our (%TABLE, %TABLE_GROUP, %TABLE_NAME_MAP);


sub set_table_name {

    my ($table_specifier, $table_name) = @_;
    
    unlock_keys(%TABLE);
    unlock_value(%TABLE, $table_specifier) if exists $TABLE{$table_specifier};
    $TABLE{$table_specifier} = $table_name;
    lock_value(%TABLE, $table_specifier);
    lock_keys(%TABLE);
    
    return $table_name;
}


sub get_table_name {

    return $TABLE{$_[0]} || croak "unknown table '$_[0]'";
}


sub change_table_name {

    my ($table_specifier, $new_name) = @_;

    if ( exists $TABLE{$table_specifier} )
    {
	my $orig_name = original_table($TABLE{$table_specifier});
	
	$TABLE_NAME_MAP{$new_name} = $orig_name;
	
	unlock_value(%TABLE, $table_specifier);
	$TABLE{$table_specifier} = $new_name;
	lock_value(%TABLE, $table_specifier);
	
	return $new_name;
    }
    
    else
    {
	croak "no table name has been set for '$table_specifier'";
    }
}


sub change_table_db {
    
    my ($table_specifier, $new_db) = @_;
    
    croak "you must specify an alternate database name" unless $new_db;

    if ( exists $TABLE{$table_specifier} )
    {
	my $table_name = $TABLE{$table_specifier};
	my $orig_name = original_table($table_name);
	
	$table_name =~ s/^.+[.]//;
	
	my $new_name = "$new_db.$table_name";
	
	$TABLE_NAME_MAP{$new_name} = $orig_name;
	
	# unlock_value(%TABLE, $table_specifier);
	# $TABLE{$table_specifier} = $new_name;
	# lock_value(%TABLE, $table_specifier);

	set_table_name($table_specifier, $new_name);
	set_table_name("==$table_specifier", $orig_name);
	
	return $new_name;
    }
    
    else
    {
	croak "no table name has been set for '$table_specifier'";
    }
}


sub restore_table_name {

    my ($table_specifier) = @_;

    if ( exists $TABLE{$table_specifier} )
    {
	unlock_value(%TABLE, $table_specifier);
	$TABLE{$table_specifier} = original_table($TABLE{$table_specifier});
	lock_value(%TABLE, $table_specifier);
	
	return $TABLE{$table_specifier};
    }
    
    else
    {
	croak "no table name has been set for '$table_specifier'";
    }
}


sub original_table {
    
    return $TABLE_NAME_MAP{$_[0]} || $_[0];
}


sub set_table_group {
    
    my ($group_name, @table_specifiers) = @_;
    
    $TABLE_GROUP{$group_name} = \@table_specifiers;
}


sub get_table_group {

    my ($group_name) = @_;

    return @{$TABLE_GROUP{$group_name}} if $TABLE_GROUP{$group_name};
    return;
}


sub enable_test_mode {
    
    my ($group_name, $debug) = @_;
    
    croak "You must set test mode using 'init_table_names'" unless $TEST_MODE;
    croak "You must define 'test_db' in the configuration file" unless $TEST_DB;
    croak "unknown table group '$group_name'" unless $TABLE_GROUP{$group_name};
    
    foreach my $t ( @{$TABLE_GROUP{$group_name}} )
    {
	change_table_db($t, $TEST_DB);
    }
    
    debug_line("TEST MODE: enable '$group_name'\n", $debug) if $debug;
    
    return 1;
}


sub disable_test_mode {

    my ($group_name, $debug) = @_;
    
    croak "unknown table group '$group_name'" unless $TABLE_GROUP{$group_name};
    
    foreach my $t ( @{$TABLE_GROUP{$group_name}} )
    {
	restore_table_name($t);
    }
    
    debug_line("TEST MODE: enable '$group_name'\n", $debug) if $debug;
        
    return 2;
}


sub set_table_property {
    
    my ($table_specifier, $property, $value) = @_;
    
    croak "Invalid table property '$property'" unless $TABLE_PROP_NAME{$property};
    
    $TABLE_PROPERTIES{$table_specifier}{$property} = $value;
}


sub get_table_property {
    
    my ($table_specifier, $property) = @_;
    
    croak "Invalid table property '$property'" unless $TABLE_PROP_NAME{$property};
    
    if ( exists $TABLE_PROPERTIES{$table_specifier}{$property} )
    {
	return $TABLE_PROPERTIES{$table_specifier}{$property}
    }
    
    elsif ( defined $TABLE_PROPERTIES{$table_specifier} )
    {
	return;
    }
    
    else
    {
	carp "No properties set for table '$table_specifier'";
    }
}


sub set_column_property {
    
    my ($table_specifier, $column_name, $property, $value) = @_;
    
    croak "Invalid column property '$property'" unless $COLUMN_PROP_NAME{$property};
    
    # my $base_name = $TABLE_NAME_MAP{$table_name} || $table_name;
    
    if ( ref $value )
    {
	if ( $property eq 'VALIDATOR' )
	{	
	    croak "value must be either a code ref or a string" unless ref $value eq 'CODE';
	}
	
	elsif ( $property eq 'VALUE_SEPARATOR' )
	{
	    croak "value must be a regexp" unless ref $value eq 'Regexp';
	}
    }
    
    $COLUMN_PROPERTIES{$table_specifier}{$column_name}{$property} = $value;
}


sub get_column_property {
    
    my ($table_specifier, $column_name, $property) = @_;
    
    if ( exists $COLUMN_PROPERTIES{$table_specifier}{$column_name}{$property} )
    {
	return $COLUMN_PROPERTIES{$table_specifier}{$column_name}{$property};
    }
    
    elsif ( defined $TABLE_PROPERTIES{$table_specifier} )
    {
	return;
    }
    
    else
    {
	carp "No properties set for table '$table_specifier'";	
    }
}


sub get_column_properties {

    my ($table_specifier, $column_name) = @_;
    
    if ( defined $COLUMN_PROPERTIES{$table_specifier}{$column_name} )
    {
	return %{$COLUMN_PROPERTIES{$table_specifier}{$column_name}};
    }

    elsif ( defined $TABLE_PROPERTIES{$table_specifier} )
    {
	return;
    }
    
    else
    {
	carp "No properties set for table '$table_specifier'";	
    }
}


sub list_column_properties {
    
    my ($table_specifier) = @_;
    
    if ( defined $COLUMN_PROPERTIES{$table_specifier} )
    {
	return map { $_ => 1 } keys %{$COLUMN_PROPERTIES{$table_specifier}};
    }
    
    elsif ( defined $TABLE_PROPERTIES{$table_specifier} )
    {
	return;
    }
    
    else
    {
	carp "No properties set for table '$table_specifier'";	
    }
}


sub substitute_table {

    my ($new_name, $old_name) = @_;
    
    $TABLE_NAME_MAP{$new_name} = $old_name;
    return $new_name;
}


sub alternate_table {
    
    my ($new_db, $table_name) = @_;
    
    croak "you must specify an alternate database name" unless $new_db;
    
    my $orig_name = original_table($table_name);
    my $new_name = "$new_db.$orig_name";
    
    $TABLE_NAME_MAP{$new_name} = $orig_name;
    return $new_name;
}


1;

