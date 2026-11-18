#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction whose
# purpose is to implement these tests.
#
# edt-39-mariadb.t : Test the parts of EditTransaction::Mod::MariaDB that are not covered
#                    elsewhere: checking the database handle, unpacking column types,
#                    and the database query routines.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use ETBasicTest;
use EditTester qw(ok_new_edt clear_table sql_command capture_mode captured_output
		  clear_captured_output);


# Establish an EditTester instance.

my $T = EditTester->new('ETBasicTest');


# Check that the necessary methods are defined.

subtest 'required methods' => sub {
    
    can_ok('ETBasicTest', 'db_validate_dbh', 'db_unpack_column_type', 'db_count_matching_rows',
	   'fetch_record', 'get_old_values')
	
	|| BAIL_OUT "EditTransaction and related modules are missing some required methods";
};


# Check db_validate_dbh with handles that are not MariaDB handles.

subtest 'db_validate_dbh' => sub {
    
    my $edt = ok_new_edt;
    
    is( $edt->db_validate_dbh($T->dbh), 'ok', "a MariaDB handle is accepted" );
    
    {
	package ETFakeHandle;
	sub new { bless { }, $_[0] }
	sub quote { }
	sub selectrow_array { }
    }
    
    like( $edt->db_validate_dbh(ETFakeHandle->new), qr/is not a MariaDB database handle/,
	  "a handle for another database is rejected" );
    
    like( $edt->db_validate_dbh(bless { }, 'ETNotAHandle'), qr/is not a valid database handle/,
	  "an object that is not a database handle is rejected" );
    
    like( $edt->db_validate_dbh(undef), qr/is not a valid database handle/,
	  "an undefined handle is rejected" );
};


# Check the unpacking of column types that are not covered by the tests of value validation.

subtest 'column types' => sub {
    
    my $edt = ok_new_edt;
    
    my %expected = ( 'point' => [ 'geometry', 'point' ],
		     'multipolygon' => [ 'geometry', 'multipolygon' ],
		     'geometry' => [ 'geometry', 'geometry' ],
		     'geometrycollection' => [ 'geometry', 'geometrycollection' ],
		     'bit(3)' => [ 'bits', 3 ],
		     'json' => [ 'unknown' ] );
    
    foreach my $type ( sort keys %expected )
    {
	my $cr = { };
	
	$edt->db_unpack_column_type($cr, $type);
	
	is_deeply( [ $cr->{TypeMain}, ($cr->{TypeParams} || [ ])->@* ], $expected{$type},
		   "column type '$type' is unpacked" );
    }
    
    is( EditTransaction::Mod::MariaDB::unpack_enum("'a','b',c"), 'ERROR',
	"a badly formed list of enum values is rejected" );
};


# Check the database query routines, including their debugging output and their
# responses to bad arguments.

subtest 'query routines' => sub {
    
    clear_table('EDT_TEST');
    
    sql_command("INSERT INTO <<EDT_TEST>> (test_no, string_req) VALUES (5, 'five')");
    
    clear_captured_output();
    capture_mode(1);
    
    my $edt = ok_new_edt('DEBUG_MODE');
    
    is( $edt->db_count_matching_rows('EDT_TEST', 'test_no = 5'), 1, "db_count_matching_rows counts rows" );
    
    is( $edt->fetch_record('EDT_TEST', 'test_no = 5')->{string_req}, 'five', "fetch_record fetches a record" );
    
    is( ($edt->get_old_values('EDT_TEST', 'test_no = 5', 'string_req'))[0], 'five',
	"get_old_values fetches column values" );
    
    $edt->clear_table_cache('EDT_TEST');
    
    ok( $edt->table_info_ref('EDT_TEST'), "the table schema is fetched again" );
    
    my $output = captured_output();
    
    capture_mode(0);
    
    like( $output, qr/SELECT count\(\*\) FROM .* WHERE test_no = 5/, "db_count_matching_rows writes debug output" );
    like( $output, qr/SELECT \* FROM .* WHERE test_no = 5 LIMIT 1/, "fetch_record writes debug output" );
    like( $output, qr/SELECT string_req FROM .* WHERE test_no = 5 LIMIT 1/, "get_old_values writes debug output" );
    like( $output, qr/SHOW TABLES/, "checking that a table exists writes debug output" );
    like( $output, qr/SHOW FULL COLUMNS/, "fetching the column information writes debug output" );
    
    ok( ! defined $edt->fetch_record('EDT_NOSUCH', 'a = 1'), "fetch_record returns nothing for an unknown table" );
    
    is_deeply( [ $edt->get_old_values('EDT_NOSUCH', 'a = 1', 'a') ], [ ],
	       "get_old_values returns nothing for an unknown table" );
    
    is_deeply( [ $edt->get_old_values('EDT_TEST', '', 'string_req') ], [ ],
	       "get_old_values returns nothing without a key expression" );
};


# Check that db_count_matching_rows returns an error condition if the expression is bad.

subtest 'query errors' => sub {
    
    my $edt = ok_new_edt;
    
    my $result = $edt->db_count_matching_rows('EDT_TEST', 'no_such_column = 1');
    
    is( ref $result && $result->[0], 'E_EXECUTE', "a bad expression returns E_EXECUTE" );
    
    like( ref $result && $result->[1], qr/no_such_column/, "the message reports the problem" );
    
    unlike( ref $result && $result->[1], qr/ at .* line \d+/, "the message does not include a file location" );
};
