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

no warnings 'uninitialized';

use Carp qw(carp croak);
use Hash::Util qw(lock_value unlock_value lock_keys unlock_keys);

use base 'Exporter';

our (@EXPORT_OK) = qw($COLLECTIONS $AUTHORITIES $OPINIONS $REFERENCES $OCCURRENCES $REIDS $SPECIMENS
		      $PERSON_DATA $SESSION_DATA $TABLE_PERMS $WING_USERS
		      $COLL_MATRIX $COLL_BINS $COLL_STRATA $COUNTRY_MAP $CONTINENT_DATA
		      $COLL_LITH $COLL_ENV $STRATA_NAMES
		      $BIN_KEY $BIN_LOC $BIN_CONTAINER
		      $PALEOCOORDS $GEOPLATES $COLL_LOC $COLL_INTS
		      $DIV_MATRIX $DIV_GLOBAL $PVL_MATRIX $PVL_GLOBAL $PVL_COLLS
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
		      init_table_names enable_test_mode disable_test_mode is_test_mode
		      change_table_name change_table_db restore_table_name original_table_name
		      set_table_name get_table_name set_table_group get_table_group
		      set_table_property get_table_property set_column_property get_column_property
		      set_table_property_name list_table_property_names get_table_properties
		      set_column_property_name list_column_property_names get_column_properties
		      is_table_property is_column_property);


# List the properties that can be specified for tables. All of the CAN_ properties default
# to 'anybody' unless explicitly specified. If CAN_INSERT is not specified, it defaults to
# the value of CAN_POST.

our (%TABLE_PROP_NAME) = (
    CAN_VIEW => 1,	      # specifies who can view records
    CAN_POST => 1,	      # specifies who can add records with auto_insert keys
    CAN_INSERT => 1,          # specifies who can add records with specified keys
    CAN_DELETE => 1,          # specifies who can delete records
    CAN_MODIFY => 1,	      # specifies who can modify existing records they do not own
    CAN_ALTER_TRAIL => 1,     # specifies who can change record creator/modifier dates
    BY_AUTHORIZER => 1,       # if true, check modification permission according to authorizer_no
    CASCADE_DELETE => 1,      # specifies additional table(s) for cascade deletion from this one
    PRIMARY_KEY => 1,         # specifies the primary key column(s) for this table
    PRIMARY_FIELD => 1,       # specifies the primary key field(s) for this table
    AUTH_FIELDS => 1,	      # specifies the fields that determine record ownership
    AUTH_TABLE => 1,          # authorization should be performed on the specified table
    SUPERIOR_TABLE => 1,      # if non-empty, the specified table controls access to this one
    SUPERIOR_KEY => 1,	      # if non-empty, the specified column links this table to its superior
    NO_LOG => 1,              # if true, changes to this table will not be logged
    LOG_CHANGES => 1,	      # if true, changes to this table will be logged
    SPECIAL_COLS => 1,        # specifies a list of columns to be handled by special directives
    REQUIRED_COLS => 1,	      # specifies a list of columns whose values must be non-empty
    TABLE_COMMENT => 1,       # provides a comment or documentation string for this table
);

# List the properties that can be specified for columns.

our (%COLUMN_PROP_NAME) = (
    REQUIRED => 1,          # non-empty value is required; more strict than NOT_NULL (bool)
    NOT_NULL => 1,          # non-null value is required; overrides database (bool)
    ADMIN_SET => 1,         # only admins can set or modify the value of this column (bool)
    ALLOW_TRUNCATE => 1,    # large client provided values will be truncated to fit column (bool)
    VALUE_SEPARATOR => 1,   # override regexp for splitting column value into list of values
    ALTERNATE_NAME => 1,    # client provided records may provide column value under this name
    ALTERNATE_ONLY => 1,    # client provided records must use alternate name (bool)
    FOREIGN_KEY => 1,       # table for which this column is a foreign key
    EXTID_TYPE => 1,       # external identifier type(s) accepted by this column
    DIRECTIVE => 1,         # special handling for this column by the EditTransaction system
    IGNORE => 1,            # this column will be ignored as if it didn't exist (bool)
    COLUMN_COMMENT => 1,    # provides a comment or documentation string for this column
);


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
our $PVL_COLLS = 'pvl_collections';

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

# Table names
# -----------

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

    return exists $TABLE{$_[0]} ? $TABLE{$_[0]} : undef;
}


sub change_table_name {

    my ($table_specifier, $new_name) = @_;
    
    if ( exists $TABLE{$table_specifier} )
    {
	my $orig_name = original_table_name($TABLE{$table_specifier});
	
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
	my $orig_name = original_table_name($table_name);
	
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
	$TABLE{$table_specifier} = original_table_name($TABLE{$table_specifier});
	lock_value(%TABLE, $table_specifier);
	
	return $TABLE{$table_specifier};
    }
    
    else
    {
	croak "no table name has been set for '$table_specifier'";
    }
}


sub original_table_name {
    
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


# Table properties
# ----------------

sub list_table_property_names {

    return keys %TABLE_PROP_NAME;
}


sub is_table_property {

    return $_[0] && $TABLE_PROP_NAME{$_[0]} ? 1 : '';
}


sub set_table_property_name {
    
    my ($property_name, $status) = @_;

    if ( $status )
    {
	$TABLE_PROP_NAME{$property_name} = 1;
    }

    else
    {
	$TABLE_PROP_NAME{$property_name} = '';
    }
}


sub set_table_property {
    
    my ($table_specifier, $property, $value) = @_;
    
    croak "Invalid table property '$property'" unless $TABLE_PROP_NAME{$property};
    
    $TABLE_PROPERTIES{$table_specifier}{$property} = $value;
    
    # if ( $property eq 'PRIMARY_KEY' && $value =~ / (.*) _no $ /xs )
    # {
    # 	$TABLE_PROPERTIES{$table_specifier}{PRIMARY_FIELD} = "${1}_id";
    # }
}


sub get_table_property {
    
    my ($table_specifier, $property) = @_;
    
    croak "Invalid table property '$property'" unless $TABLE_PROP_NAME{$property};
    
    if ( exists $TABLE_PROPERTIES{$table_specifier}{$property} )
    {
	return $TABLE_PROPERTIES{$table_specifier}{$property}
    }
    
    # elsif ( ! defined $TABLE_PROPERTIES{$table_specifier} )
    # {
    # 	carp "No properties set for table '$table_specifier'";
    # }
    
    else
    {
	return undef;
    }
}


sub get_table_properties {
    
    my ($table_specifier) = @_;
    
    if ( defined $TABLE_PROPERTIES{$table_specifier} )
    {
	return $TABLE_PROPERTIES{$table_specifier}->%*;
    }
    
    else
    {
	return ();
    }
}


# Column properties
# -----------------

sub list_column_property_names {

    return keys %COLUMN_PROP_NAME;
}


sub is_column_property {

    return $_[0] && $COLUMN_PROP_NAME{$_[0]} ? 1 : '';
}


sub set_column_property_name {
    
    my ($property_name, $status) = @_;

    if ( $status )
    {
	$COLUMN_PROP_NAME{$property_name} = 1;
    }

    else
    {
	$COLUMN_PROP_NAME{$property_name} = '';
    }
}


sub set_column_property {
    
    my ($table_specifier, $column_name, $property, $value) = @_;
    
    croak "Invalid column property '$property'" unless $COLUMN_PROP_NAME{$property};
    
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
    
    return unless $table_specifier && $column_name && $property;
    
    if ( exists $COLUMN_PROPERTIES{$table_specifier}{$column_name}{$property} )
    {
	return $COLUMN_PROPERTIES{$table_specifier}{$column_name}{$property};
    }

    else
    {
	return undef;
    }
}


sub has_column_properties {

    my ($table_specifier) = @_;

    return $COLUMN_PROPERTIES{$table_specifier} ? 1 : '';
}


sub get_column_properties {

    my ($table_specifier, $column_name) = @_;
    
    if ( defined $column_name )
    {
	if ( my $column_ref = $COLUMN_PROPERTIES{$table_specifier}{$column_name} )
	{
	    return map { $_ => copy_property_value($column_ref->{$_}) } keys $column_ref->%*;
	}
    }
    
    elsif ( my $cols_ref = $COLUMN_PROPERTIES{$table_specifier} )
    {
	my @result;
	
	foreach my $colname ( keys $cols_ref->%* )
	{
	    if ( ref $cols_ref->{$colname} eq 'HASH' )
	    {
		push @result, $colname;
		my %propvals = map { $_ => copy_property_value($cols_ref->{$colname}{$_}) }
		    keys $cols_ref->{$colname}->%*;
		push @result, \%propvals;
	    }
	}
	
	return @result;
    }
    
    else
    {
	return ();
    }
}


sub copy_property_value {

    if ( ref $_[0] eq 'HASH' )
    {
	return { $_[0]->%* };
    }
    
    elsif ( ref $_[0] eq 'ARRAY' )
    {
	return [ $_[0]->@* ];
    }
    
    else
    {
	return $_[0];
    }
}


1;

