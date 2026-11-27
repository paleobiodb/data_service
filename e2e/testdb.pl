# -*- mode: cperl -*-
#
# PBDB 1.2
# --------
#
# The purpose of this file is to prepare the test database for the browser tests in this
# directory, and to read it back for them. It is run inside the pbapi container, in the
# pbapi directory, by the Playwright tests:
#
#     perl e2e/testdb.pl setup SET               establish the test tables of SET, and print
#                                                what was set up
#     perl e2e/testdb.pl row SET TABLE N COLS    print the named columns of the record whose
#                                                primary key is N, in the test copy of TABLE
#     perl e2e/testdb.pl rows SET TABLE KEY N COLS
#                                                print the named columns of every record
#                                                whose column KEY is N, as a list
#     perl e2e/testdb.pl release SET             switch the test tables of SET back to the
#                                                real ones
#
# The table sets are 'colls', 'refs', 'pubs', and 'eduresources'. Each command prints one
# line starting with 'RESULT ', followed by JSON. The test output from the Tester modules
# goes to STDERR.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib qw(lib t);

use Test::More;
use JSON;

use TableDefs qw(%TABLE original_table_name);
use CoreTableDefs;
use TestTables qw(fill_test_table);

use Tester;
use PBDBTester;

Test::More->builder->output(\*STDERR);
Test::More->builder->todo_output(\*STDERR);

my ($COMMAND, $SET, @ARGS) = @ARGV;

# The class whose tables each set uses, and the tables whose records are numbered above
# every real one, so that they cannot be confused with real records.

my %CLASS = ( colls => 'CollectionEdit', refs => 'ReferenceEdit', pubs => 'PBDBEdit',
	      eduresources => 'ResourceEdit' );

my %NUMBERED = ( colls => [ qw(COLLECTION_DATA OCCURRENCE_DATA REID_DATA) ],
		 refs => [ qw(REFERENCE_DATA) ],
		 pubs => [ qw(PUBLICATION_DATA) ],
		 eduresources => [ qw(RESOURCE_QUEUE) ] );

# The taxa and the collection used by the collection tests, as in pe2-30-collections.t.

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

my $class = $CLASS{$SET // ''} || BAIL_OUT("unknown table set '" . ($SET // '') . "'");

my $T = Tester->new({ prefix => 'data1.2' });

# Make SURE that the server is using the test session tables and the test copies of every
# table written by this class, as the API tests do. We DO NOT WANT to change the data in the
# main tables. select_module_tables bails out if any of them cannot be switched.

$T->test_mode('session_data', 'enable') || BAIL_OUT("could not select test session data");

my $ET = PBDBTester->new($class);

my @groups = $ET->select_module_tables($T, $class);

if ( $COMMAND eq 'setup' )
{
    $ET->establish_session_data;
    
    foreach my $group ( @groups )
    {
	$ET->establish_test_tables($group, 'test') ||
	    BAIL_OUT("could not establish test tables for '$group'");
    }
    
    foreach my $table ( $NUMBERED{$SET}->@* )
    {
	my $key = primary_key($table);
	my $real = original_table_name($TABLE{$table});
	
	my ($max) = $ET->dbh->selectrow_array("SELECT max($key) FROM $real");
	
	$ET->dbh->do("ALTER TABLE $TABLE{$table} AUTO_INCREMENT = " . (($max // 0) + 1000));
    }
    
    my $result = { groups => \@groups };
    
    if ( $SET eq 'colls' )
    {
	# Select reference 5 for data entry in the test session, as the Collections
	# application needs a selected reference to add a collection.
	
	$ET->dbh->do("UPDATE $TABLE{SESSION_DATA} SET reference_no = 5
		WHERE session_id = 'SESSION-ENTERER'");
	
	$ET->fill_test_table('AUTHORITY_DATA', "taxon_no in ($GENUS_NO, $SPECIES_NO)", 'test');
	
	$T->set_cookie("session_id", "SESSION-ENTERER");
	
	my ($r) = $T->send_records("/colls/add.json", "add the test collection",
				   json => [ \%COLL ]);
	
	BAIL_OUT("could not add the test collection") unless $r && $r->{oid};
	
	$result->{collection_id} = $r->{oid};
    }
    
    elsif ( $SET eq 'eduresources' )
    {
	# The tag names are copied from the real table, as the resource pages offer them.
	
	fill_test_table($ET->dbh, 'RESOURCE_TAG_NAMES', 1, 'test') ||
	    BAIL_OUT("could not copy the resource tag names");
    }
    
    result($result);
}

elsif ( $COMMAND eq 'row' )
{
    my ($table, $no, $columns) = @ARGS;
    
    check_args($table, $columns, $no);
    
    my $key = primary_key($table);
    
    my $row = $ET->dbh->selectrow_hashref("SELECT $columns FROM $TABLE{$table}
		WHERE $key = $no");
    
    result($row || {});
}

elsif ( $COMMAND eq 'rows' )
{
    my ($table, $key, $no, $columns) = @ARGS;
    
    check_args($table, $columns, $no);
    BAIL_OUT("bad key column '$key'") unless $key =~ /^\w+$/;
    
    my $order = primary_key($table, 'optional');
    my $order_by = $order ? "ORDER BY $order" : "";
    
    my $rows = $ET->dbh->selectall_arrayref("SELECT $columns FROM $TABLE{$table}
		WHERE $key = $no $order_by", { Slice => {} });
    
    result($rows || []);
}

elsif ( $COMMAND eq 'release' )
{
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


sub check_args {
    
    my ($table, $columns, $no) = @_;
    
    BAIL_OUT("unknown table '$table'") unless $table && $TABLE{$table};
    BAIL_OUT("table '$table' is not in the test database")
	unless $TABLE{$table} =~ /^pbdbTEST[.]/;
    BAIL_OUT("bad record number '$no'") unless defined $no && $no =~ /^\d+$/;
    BAIL_OUT("bad column list '$columns'") unless $columns && $columns =~ /^\w+(,\w+)*$/;
}


# Return the first column of the primary key of the specified table. If the table has none,
# bail out, unless $optional is true.

sub primary_key {
    
    my ($table, $optional) = @_;
    
    my $keys = $ET->dbh->selectall_arrayref("SHOW KEYS FROM $TABLE{$table}
		WHERE Key_name = 'PRIMARY'", { Slice => {} });
    
    return if $optional && ! ($keys && @$keys);
    
    BAIL_OUT("table '$table' has no primary key") unless $keys && @$keys;
    
    return $keys->[0]{Column_name};
}
