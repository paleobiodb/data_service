#
# Paleobiology Database library modules
# -------------------------------------
#
# This file contains unit tests for library modules that are used by the EditTransaction
# client modules and by the data service.
#
# lib-07-tabledefs.t : Test TableDefs.pm, which holds the names of the database tables,
# the table groups, the properties of tables and columns, and the switch that lets the
# tests use tables in the test database.
#
# These tests use table names, groups, and property names defined only for them, so the
# names of the real tables are not changed. They do not use the database.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 8;

use CoreFunction qw(loadConfig);
use CoreTableDefs;

use TableDefs qw(%TABLE $TEST_DB init_table_names enable_test_mode disable_test_mode
		 is_test_mode change_table_name change_table_db restore_table_name
		 original_table_name set_table_name get_table_name set_table_group
		 get_table_group set_table_property get_table_property set_column_property
		 get_column_property set_table_property_name list_table_property_names
		 get_table_properties set_column_property_name list_column_property_names
		 get_column_properties is_table_property is_column_property);


# Turn on test mode with the test database named in the configuration file, as the other
# tests do.

my $CONFIG = loadConfig();

init_table_names($CONFIG, 1);


# The property names that are defined, and new ones defined by the tests.

subtest 'property names' => sub {
    
    my @table_props = list_table_property_names();
    
    ok( (grep { $_ eq 'CAN_POST' } @table_props) && (grep { $_ eq 'PRIMARY_KEY' } @table_props),
	"the table property names include CAN_POST and PRIMARY_KEY" );
    is( is_table_property('CAN_POST'), 1, "is_table_property" );
    is( is_table_property('NO_SUCH_PROP'), '', "an unknown table property" );
    is( is_table_property(undef), '', "no name" );
    
    ok( (grep { $_ eq 'REQUIRED' } list_column_property_names()), "the column property names include REQUIRED" );
    is( is_column_property('FOREIGN_KEY'), 1, "is_column_property" );
    is( is_column_property('NO_SUCH_PROP'), '', "an unknown column property" );
    
    set_table_property_name('LIB07_TPROP', 1);
    set_column_property_name('LIB07_CPROP', 1);
    
    is( is_table_property('LIB07_TPROP'), 1, "a new table property name" );
    is( is_column_property('LIB07_CPROP'), 1, "a new column property name" );
    
    set_table_property_name('LIB07_TPROP', 0);
    set_column_property_name('LIB07_CPROP', 0);
    
    is( is_table_property('LIB07_TPROP'), '', "a table property name can be turned off" );
    is( is_column_property('LIB07_CPROP'), '', "and a column property name" );
    
    eval { set_table_property('LIB07_A', LIB07_TPROP => 1) };
    
    like( $@, qr{Invalid table property 'LIB07_TPROP'}, "a property that is turned off cannot be set" );
};


# Table properties are stored by table specifier.

subtest 'table properties' => sub {
    
    set_table_property('LIB07_A', PRIMARY_KEY => 'a_no');
    set_table_property('LIB07_A', CAN_POST => 'AUTHORIZED');
    set_table_property('LIB07_A', CASCADE_DELETE => [ 'LIB07_B' ]);
    
    is( get_table_property('LIB07_A', 'PRIMARY_KEY'), 'a_no', "get_table_property" );
    is( get_table_property('LIB07_A', 'CAN_VIEW'), undef, "a property that was not set" );
    is( get_table_property('LIB07_NONE', 'CAN_VIEW'), undef, "a table with no properties" );
    is_deeply( get_table_property('LIB07_A', 'CASCADE_DELETE'), [ 'LIB07_B' ], "a list value" );
    
    my %props = get_table_properties('LIB07_A');
    
    is( join(',', sort keys %props), 'CAN_POST,CASCADE_DELETE,PRIMARY_KEY', "get_table_properties" );
    is( scalar(() = get_table_properties('LIB07_NONE')), 0, "a table with no properties has none" );
    
    set_table_property('LIB07_A', CAN_POST => undef);
    
    is( get_table_property('LIB07_A', 'CAN_POST'), undef, "a property can be set to undef" );
    
    eval { set_table_property('LIB07_A', NO_SUCH_PROP => 1) };
    like( $@, qr{Invalid table property 'NO_SUCH_PROP'}, "an unknown property cannot be set" );
    
    eval { get_table_property('LIB07_A', 'NO_SUCH_PROP') };
    like( $@, qr{Invalid table property 'NO_SUCH_PROP'}, "or read" );
};


# Column properties are stored by table specifier and column name. The values returned by
# get_column_properties are copies.

subtest 'column properties' => sub {
    
    set_column_property('LIB07_A', 'name', REQUIRED => 1);
    set_column_property('LIB07_A', 'name', ALTERNATE_NAME => 'title');
    set_column_property('LIB07_A', 'kind', VALUE_SEPARATOR => qr{/});
    set_column_property('LIB07_A', 'kind', DIRECTIVE => [ 'a', 'b' ]);
    
    is( get_column_property('LIB07_A', 'name', 'ALTERNATE_NAME'), 'title', "get_column_property" );
    is( get_column_property('LIB07_A', 'name', 'IGNORE'), undef, "a property that was not set" );
    is( scalar(() = get_column_property('LIB07_A', 'name')), 0, "no property name" );
    
    my %name = get_column_properties('LIB07_A', 'name');
    
    is( join(',', map { "$_=$name{$_}" } sort keys %name), 'ALTERNATE_NAME=title,REQUIRED=1',
	"the properties of one column" );
    
    my %all = get_column_properties('LIB07_A');
    
    is( join(',', sort keys %all), 'kind,name', "the properties of every column" );
    is( ref $all{kind}{VALUE_SEPARATOR}, 'Regexp', "a regexp value" );
    
    push $all{kind}{DIRECTIVE}->@*, 'c';
    
    is( join(',', get_column_property('LIB07_A', 'kind', 'DIRECTIVE')->@*), 'a,b',
	"the values returned are copies" );
    
    is( TableDefs::has_column_properties('LIB07_A'), 1, "has_column_properties" );
    is( TableDefs::has_column_properties('LIB07_NONE'), '', "a table without column properties" );
    is( scalar(() = get_column_properties('LIB07_NONE')), 0, "it has no column properties" );
    
    eval { set_column_property('LIB07_A', 'name', NO_SUCH_PROP => 1) };
    like( $@, qr{Invalid column property 'NO_SUCH_PROP'}, "an unknown property cannot be set" );
    
    eval { set_column_property('LIB07_A', 'kind', VALUE_SEPARATOR => [ '/' ]) };
    like( $@, qr{must be a regexp}, "VALUE_SEPARATOR must be a regexp if it is a reference" );
    
    is( scalar(() = get_column_properties('LIB07_A', 'nocol')), 0,
	"a column without properties has none" );
};


# Table names are stored in %TABLE, which is locked: only set_table_name can add a
# specifier, and its value cannot be assigned to directly.

subtest 'table names' => sub {
    
    set_table_name(LIB07_A => 'lib07_a');
    
    is( $TABLE{LIB07_A}, 'lib07_a', "set_table_name" );
    is( get_table_name('LIB07_A'), 'lib07_a', "get_table_name" );
    is( get_table_name('LIB07_NONE'), undef, "an unknown specifier" );
    
    eval { my $x = $TABLE{LIB07_NONE} };
    like( $@, qr{disallowed key}, "reading an unknown specifier from %TABLE dies" );
    
    eval { $TABLE{LIB07_A} = 'other' };
    like( $@, qr{read-only}, "assigning to %TABLE dies" );
    
    eval { $TABLE{LIB07_NEW} = 'other' };
    like( $@, qr{disallowed key}, "adding to %TABLE dies" );
    
    set_table_name(LIB07_A => 'lib07_a2');
    
    is( $TABLE{LIB07_A}, 'lib07_a2', "set_table_name can change a name" );
    
    set_table_name(LIB07_A => 'lib07_a');
    
    # A name can be changed, and restored to the original.
    
    is( change_table_name('LIB07_A', 'lib07_alt'), 'lib07_alt', "change_table_name" );
    is( $TABLE{LIB07_A}, 'lib07_alt', "the new name is in %TABLE" );
    is( original_table_name('lib07_alt'), 'lib07_a', "original_table_name" );
    is( original_table_name('lib07_other'), 'lib07_other', "a name that was not changed is its own original" );
    
    change_table_name('LIB07_A', 'lib07_alt2');
    
    is( original_table_name('lib07_alt2'), 'lib07_a', "a second change keeps the first original" );
    is( restore_table_name('LIB07_A'), 'lib07_a', "restore_table_name" );
    is( $TABLE{LIB07_A}, 'lib07_a', "the original is back in %TABLE" );
    
    # A table can be moved to another database.
    
    is( change_table_db('LIB07_A', 'otherdb'), 'otherdb.lib07_a', "change_table_db" );
    is( original_table_name($TABLE{LIB07_A}), 'lib07_a', "the original of the moved table" );
    is( $TABLE{'==LIB07_A'}, 'lib07_a', "the original name is also kept under '==LIB07_A'" );
    is( change_table_db('LIB07_A', 'thirddb'), 'thirddb.lib07_a', "a second move replaces the database" );
    
    restore_table_name('LIB07_A');
    
    is( $TABLE{LIB07_A}, 'lib07_a', "and it can be restored" );
    
    foreach my $f ( [ 'change_table_name', \&change_table_name, 'x' ],
		    [ 'change_table_db', \&change_table_db, 'x' ],
		    [ 'restore_table_name', \&restore_table_name ] )
    {
	my ($name, $sub, @args) = @$f;
	
	eval { $sub->('LIB07_NONE', @args) };
	like( $@, qr{no table name has been set for 'LIB07_NONE'}, "$name of an unknown specifier" );
    }
    
    eval { change_table_db('LIB07_A') };
    like( $@, qr{must specify an alternate database name}, "change_table_db requires a database" );
};


# A table group lists the specifiers that are switched to the test database together.

subtest 'test mode' => sub {
    
    ok( is_test_mode(), "test mode is on" );
    is( $TEST_DB, $CONFIG->{test_db}, "the test database is the one in config.yml" );
    
    set_table_name(LIB07_B => 'lib07_b');
    set_table_group('lib07_group', 'LIB07_A', 'LIB07_B');
    
    is( join(',', get_table_group('lib07_group')), 'LIB07_A,LIB07_B', "get_table_group" );
    is( scalar(() = get_table_group('lib07_nogroup')), 0, "an unknown group" );
    
    my $real_auth = $TABLE{AUTHORITY_DATA};
    
    is( enable_test_mode('lib07_group'), 1, "enable_test_mode" );
    is( "$TABLE{LIB07_A} $TABLE{LIB07_B}", "$TEST_DB.lib07_a $TEST_DB.lib07_b",
	"the tables of the group are in the test database" );
    is( $TABLE{AUTHORITY_DATA}, $real_auth, "other tables are not" );
    
    is( disable_test_mode('lib07_group'), 2, "disable_test_mode" );
    is( "$TABLE{LIB07_A} $TABLE{LIB07_B}", "lib07_a lib07_b", "the original tables are back" );
    
    eval { enable_test_mode('lib07_nogroup') };
    like( $@, qr{unknown table group 'lib07_nogroup'}, "enable_test_mode with an unknown group" );
    
    eval { disable_test_mode('lib07_nogroup') };
    like( $@, qr{unknown table group 'lib07_nogroup'}, "disable_test_mode with an unknown group" );
    
    {
	local $TableDefs::TEST_MODE;
	
	eval { enable_test_mode('lib07_group') };
	like( $@, qr{You must set test mode}, "test mode must be turned on first" );
    }
    
    {
	local $TableDefs::TEST_DB;
	
	eval { enable_test_mode('lib07_group') };
	like( $@, qr{You must define 'test_db'}, "and a test database must be configured" );
    }
};


# debug_line prints to STDERR, or calls the debug_line method of an object whose debug
# method is true.

{
    package LIB07Debug;
    
    sub new { bless { lines => [], debug => $_[1] }, $_[0] }
    sub debug { $_[0]{debug} }
    sub debug_line { push $_[0]{lines}->@*, $_[1] }
}

subtest 'debugging output' => sub {
    
    my $obj = LIB07Debug->new(1);
    
    TableDefs::debug_line("one\n", $obj);
    
    is( join('', $obj->{lines}->@*), "one\n", "to an object" );
    
    my $quiet = LIB07Debug->new(0);
    
    TableDefs::debug_line("two\n", $quiet);
    
    is( scalar($quiet->{lines}->@*), 0, "not to an object whose debug method is false" );
    
    my $stderr = '';
    
    {
	local *STDERR;
	open STDERR, '>', \$stderr;
	
	TableDefs::debug_line("three\n", 1);
	TableDefs::debug_line("four\n", 0);
	enable_test_mode('lib07_group', 1);
	disable_test_mode('lib07_group', 1);
    }
    
    like( $stderr, qr{^three\n}, "to STDERR" );
    unlike( $stderr, qr{four}, "nothing if debugging is off" );
    like( $stderr, qr{TEST MODE: enable 'lib07_group'}, "enable_test_mode reports the group" );
    
    like( $stderr, qr{TEST MODE: disable 'lib07_group'}, "disable_test_mode reports the group" );
};


# The variables for the table names of the classic tables, which some older code uses.

subtest 'table name variables' => sub {
    
    is( $TableDefs::COLLECTIONS, 'collections', '$COLLECTIONS' );
    is( $TableDefs::AUTHORITIES, 'authorities', '$AUTHORITIES' );
    is( $TableDefs::REFERENCES, 'refs', '$REFERENCES' );
    is( $TableDefs::MACROSTRAT_LITHS, 'macrostrat.liths', '$MACROSTRAT_LITHS' );
};


# The tables defined in CoreTableDefs have names in %TABLE.

subtest 'core tables' => sub {
    
    foreach my $t ( qw(AUTHORITY_DATA COLLECTION_DATA OCCURRENCE_DATA REFERENCE_DATA SPECIMEN_DATA) )
    {
	ok( get_table_name($t), "$t has a name" );
    }
    
    is( get_table_property('REFERENCE_DATA', 'PRIMARY_KEY'), 'reference_no',
	"and properties" );
};
