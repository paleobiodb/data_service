# 
# The Paleobiology Database
# 
#   TableDefs.pm
#
# This file provides routines for establishing test tables .
#
# This allows for tables to be later renamed without having to search laboriously through the code
# for each statement referring to them. It also provides for operating the data service in "test
# mode", which can select alternate tables for use in running the unit tests.

package TestTables;

use strict;

use Carp qw(carp croak);

use TableDefs qw(%TABLE $TEST_DB original_table);

use base 'Exporter';

our (@EXPORT_OK) = qw(establish_test_tables fill_test_table);




# The following routine is used to actually set up new tables in the test database. In general,
# the tables in the test database should exactly match the main database schema.

# establish_test_tables ( dbh, table_group, debug )
# 
# Establish a table in the test database for each table in $table_group, using the same table
# definitions as the original tables. The $debug argument can take the same values as the
# corresponding argument to 'enable_test_mode'.

sub establish_test_tables {
    
    my ($dbh, $group_name, $debug) = @_;
    
    croak "unknown table group '$group_name'" unless $TableDefs::TABLE_GROUP{$group_name};
    croak "you must specify a test database name in the configuration file under 'test_db'" unless $TEST_DB;
    
    foreach my $table_specifier ( @{$TableDefs::TABLE_GROUP{$group_name}} )
    {
	my $table_name = $TABLE{$table_specifier};
	my $orig_name = original_table($table_name);
	my $test_name = $orig_name;
	
	if ( $orig_name =~ /^\w+[.]/ )
	{
	    $test_name =~ s/^\w+[.]/"${TEST_DB}."/e;
	}
	
	else
	{
	    $test_name = "$TEST_DB.$orig_name";
	    $orig_name = "pbdb.$orig_name";
	}
	
	my $sql = "CREATE OR REPLACE TABLE $test_name LIKE $orig_name";
	
	&TableDefs::debug_line("$sql\n", $debug) if $debug;

	$dbh->do($sql);
    }

    return 1;
}


sub fill_test_table {
    
    my ($dbh, $table_specifier, $expr, $debug) = @_;

    croak "unknown table specifier '$table_specifier'" unless $TABLE{$table_specifier};
    croak "you must specify a test database name in the configuration file under 'test_db'" unless $TEST_DB;
    
    my $orig_name = original_table($TABLE{$table_specifier});
    my $test_name = $orig_name;
    
    if ( $orig_name =~ /^\w+[.]/ )
    {
	$test_name =~ s/^\w+[.]/"${TEST_DB}."/e;
    }
    
    else
    {
	$test_name = "$TEST_DB.$orig_name";
	$orig_name = "pbdb.$orig_name";
    }
    
    my $selector = $expr && $expr ne '1' ? "WHERE $expr" : "";
    
    my $sql = "INSERT INTO $test_name SELECT * FROM $orig_name $selector";
    
    &TableDefs::debug_line("$sql\n", $debug) if $debug;
    
    $dbh->do($sql);

    return 1;
}

1;
