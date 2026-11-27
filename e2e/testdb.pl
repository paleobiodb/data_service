# -*- mode: cperl -*-
#
# PBDB 1.2
# --------
#
# The purpose of this file is to prepare the test database for the browser tests in this
# directory, and to read it back for them. It is run inside the pbapi container, in the
# pbapi directory, by the Playwright tests:
#
#     perl e2e/testdb.pl setup colls      establish the test tables used by CollectionEdit,
#                                         add a test collection, and print its number
#     perl e2e/testdb.pl row TABLE N COLS print the named columns of record N in the test
#                                         copy of TABLE
#     perl e2e/testdb.pl release colls    switch the test tables back to the real ones
#
# Each command prints one line starting with 'RESULT ', followed by JSON. The test output
# from the Tester modules goes to STDERR.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib qw(lib t);

use Test::More;
use JSON;

use TableDefs qw(%TABLE original_table_name);
use CoreTableDefs;

use Tester;
use PBDBTester;

Test::More->builder->output(\*STDERR);
Test::More->builder->todo_output(\*STDERR);

my ($COMMAND, @ARGS) = @ARGV;

my %CLASS = ( colls => 'CollectionEdit' );

# The taxa and the collection used by the tests, as in pe2-30-collections.t.

my $GENUS_NO = 38613;
my $SPECIES_NO = 54833;

my %COLL = ( collection_name => 'Browser test collection', reference_id => 'ref:5',
	     access_level => 'the public', release_date => 'immediate',
	     collection_type => 'general faunal/floral', country => 'United States',
	     state => 'Wisconsin', latdeg => 45, latmin => 30, latdir => 'North',
	     lngdeg => 100, lngdec => 25, lngdir => 'West',
	     latlng_basis => 'stated in text', lithology1 => 'sandstone',
	     environment => 'fluvial-lacustrine indet.', assembl_comps => 'macrofossils',
	     max_interval => 'Maastrichtian' );

my $T = Tester->new({ prefix => 'data1.2' });

if ( $COMMAND eq 'setup' )
{
    my $class = $CLASS{$ARGS[0]} || BAIL_OUT("unknown table set '$ARGS[0]'");

    # Make SURE that the server is using the test session tables and the test copies of
    # every table written by this class, as the API tests do. We DO NOT WANT to change the
    # data in the main tables.

    $T->test_mode('session_data', 'enable') || BAIL_OUT("could not select test session data");

    my $ET = PBDBTester->new($class);

    $ET->establish_session_data;

    # Select reference 5 for data entry in the test session, as the Collections application
    # needs a selected reference to add a collection.

    $ET->dbh->do("UPDATE $TABLE{SESSION_DATA} SET reference_no = 5
		WHERE session_id = 'SESSION-ENTERER'");

    my @groups = $ET->select_module_tables($T, $class);

    foreach my $group ( @groups )
    {
	$ET->establish_test_tables($group, 'test') ||
	    BAIL_OUT("could not establish test tables for '$group'");
    }

    $ET->fill_test_table('AUTHORITY_DATA', "taxon_no in ($GENUS_NO, $SPECIES_NO)", 'test');

    # Number the test records above every real one, so that they cannot be confused with
    # real records.

    foreach my $table ( qw(COLLECTION_DATA OCCURRENCE_DATA REID_DATA) )
    {
	my $real = original_table_name($TABLE{$table});
	my $key = $table eq 'REID_DATA' ? 'reid_no' : lc($table) =~ s/_data$/_no/r;

	my ($max) = $ET->dbh->selectrow_array("SELECT max($key) FROM $real");

	$ET->dbh->do("ALTER TABLE $TABLE{$table} AUTO_INCREMENT = " . ($max + 1000));
    }

    $T->set_cookie("session_id", "SESSION-ENTERER");

    my ($r) = $T->send_records("/colls/add.json", "add the test collection",
			       json => [ \%COLL ]);

    BAIL_OUT("could not add the test collection") unless $r && $r->{oid};

    result({ collection_id => $r->{oid}, groups => \@groups });
}

elsif ( $COMMAND eq 'row' )
{
    my ($table, $no, $columns) = @ARGS;

    $T->test_mode('session_data', 'enable') || BAIL_OUT("could not select test session data");

    my $ET = PBDBTester->new('CollectionEdit');

    my @groups = $ET->select_module_tables($T, 'CollectionEdit');

    BAIL_OUT("unknown table '$table'") unless $TABLE{$table};
    BAIL_OUT("bad record number '$no'") unless $no =~ /^\d+$/;
    BAIL_OUT("bad column list '$columns'") unless $columns =~ /^\w+(,\w+)*$/;

    my $key = $table eq 'REID_DATA' ? 'reid_no' : lc($table) =~ s/_data$/_no/r;

    my $row = $ET->dbh->selectrow_hashref("SELECT $columns FROM $TABLE{$table}
		WHERE $key = $no");

    result($row || {});
}

elsif ( $COMMAND eq 'release' )
{
    my $class = $CLASS{$ARGS[0]} || BAIL_OUT("unknown table set '$ARGS[0]'");

    my $ET = PBDBTester->new('CollectionEdit');

    $ET->select_module_tables($T, $class);
    $ET->release_module_tables($T);

    result({ released => 1 });
}

else
{
    BAIL_OUT("unknown command '$COMMAND'");
}

done_testing();


sub result {

    my ($value) = @_;

    print "RESULT " . JSON->new->canonical->encode($value) . "\n";
}
