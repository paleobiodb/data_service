#
# EditTransaction project
# -----------------------
#
# EDTPCTest.pm : Switch the tables used by the EditTransaction client modules (the
#                Paleobiology Database subclasses of EditTransaction) over to the test
#                database, and create empty copies of them there for testing.
#
# Added by Claude under the guidance of Michael McClennen.
#
# The client modules are tested as they are, so unlike EDTPBTest this is not a subclass of
# EditTransaction. Every table that a module writes to, or whose contents the tests need to
# control, is listed below under that module. Those tables are switched over to the test
# database and created there with the structure of the real tables. Tables that a module
# only reads, such as the taxonomy tables, are left in the main database.
#
# All of the table names and properties are defined in CoreTableDefs.pm (which also loads
# ResourceDefs.pm), so the client modules can be loaded in any order. [Changed by Claude under
# the guidance of Michael McClennen: the client modules used to set some table names when
# they were loaded, which could switch a table back to the main database.]

use strict;

package EDTPCTest;

use TableDefs qw(%TABLE $TEST_DB change_table_db original_table_name);
use CoreTableDefs;

use PBDBEdit;
use ArchiveEdit;
use PhylopicEdit;
use ResourceEdit;
use SpecimenEdit;
use ReferenceEdit;
use CollectionEdit;

use Carp qw(croak);


# The tables used by each client module.

our (%MODULE_TABLES) = (
    PBDBEdit => [ 'PUBLICATIONS' ],
    ArchiveEdit => [ 'ARCHIVES' ],
    PhylopicEdit => [ 'PHYLOPIC_CHOICE', 'PHYLOPIC_SEEN', 'PHYLOPIC_NAMES', 'TAXON_ATTRS' ],
    ResourceEdit => [ 'RESOURCE_QUEUE', 'RESOURCE_ACTIVE', 'RESOURCE_IMAGES', 'RESOURCE_TAGS',
		      'RESOURCE_TAG_NAMES' ],
    SpecimenEdit => [ 'SPECIMEN_DATA', 'SPECIMEN_MATRIX', 'MEASUREMENT_DATA', 'OCCURRENCE_DATA',
		      'OCCURRENCE_MATRIX', 'COLLECTION_DATA', 'COLLECTION_MATRIX' ],
    ReferenceEdit => [ 'REFERENCE_DATA', 'REFERENCE_AUTHORS', 'REFERENCE_EDITORS' ],
    CollectionEdit => [ 'COLLECTION_DATA', 'COLLECTION_MATRIX', 'COLLECTION_REFS',
			'OCCURRENCE_DATA', 'OCCURRENCE_MATRIX', 'REID_DATA',
			'AUTHORITY_DATA' ],
    );


# client_modules ( )
#
# Return the names of the client modules, in the order in which they are tested.

sub client_modules {
    
    return qw(PBDBEdit ArchiveEdit PhylopicEdit ResourceEdit SpecimenEdit ReferenceEdit
	      CollectionEdit);
}


# module_tables ( module... )
#
# Return the tables used by the specified modules, or by all of them if none are specified,
# with no duplicates.

sub module_tables {
    
    my ($class, @modules) = @_;
    
    @modules = client_modules() unless @modules;
    
    my (%seen, @tables);
    
    foreach my $m ( @modules )
    {
	croak "unknown client module '$m'" unless $MODULE_TABLES{$m};
	
	push @tables, grep { ! $seen{$_}++ } $MODULE_TABLES{$m}->@*;
    }
    
    return @tables;
}


# select_test_tables ( module... )
#
# Switch the tables used by the specified modules, or by all of them if none are specified,
# over to the test database. Throw an exception if any of them could not be switched.

sub select_test_tables {
    
    my ($class, @modules) = @_;
    
    croak "no test database has been configured" unless $TEST_DB;
    
    my @tables = $class->module_tables(@modules);
    
    foreach my $t ( @tables )
    {
	change_table_db($t, $TEST_DB) unless $TABLE{$t} =~ /^$TEST_DB[.]/;
    }
    
    if ( my @bad = $class->tables_not_in_test_db(@modules) )
    {
	croak "could not select the test database for: @bad";
    }
    
    return 1;
}


# tables_not_in_test_db ( module... )
#
# Return the tables used by the specified modules, or by all of them if none are specified,
# that are not in the test database. This should be checked before any test that writes to
# the database.

sub tables_not_in_test_db {
    
    my ($class, @modules) = @_;
    
    return grep { ! ( $TEST_DB && $TABLE{$_} =~ /^$TEST_DB[.]/ ) } $class->module_tables(@modules);
}


# establish_test_tables ( dbh, module... )
#
# Create empty copies in the test database of the tables used by the specified modules, or by
# all of them if none are specified. Each one is created with the structure of the
# corresponding table in the main database.
# Any existing copy is dropped first.

sub establish_test_tables {
    
    my ($class, $dbh, @modules) = @_;
    
    if ( my @bad = $class->tables_not_in_test_db(@modules) )
    {
	croak "these tables have not been switched over to the test database: @bad";
    }
    
    foreach my $t ( $class->module_tables(@modules) )
    {
	my $test_name = $TABLE{$t};
	my $real_name = original_table_name($test_name);
	
	croak "could not find the name of the real table for '$t'"
	    unless $real_name && $real_name ne $test_name;
	
	$dbh->do("DROP TABLE IF EXISTS $test_name");
	$dbh->do("CREATE TABLE $test_name LIKE $real_name");
    }
    
    return 1;
}


1;
