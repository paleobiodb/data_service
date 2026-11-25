#
# Paleobiology Database library modules
# -------------------------------------
#
# This file contains unit tests for library modules that are used by the EditTransaction
# client modules and by the data service.
#
# lib-02-matrixbase.t : Test MatrixBase.pm, which keeps the collection, occurrence, and
# specimen matrices up to date with the tables they are derived from. CollectionEdit and
# SpecimenEdit call it whenever they change those tables.
#
# The test data is a set of collections, occurrences, reidentifications, and specimens
# written to the test database. The taxa Tyrannosaurus and T. rex are copied from the real
# authorities table. The ages of the intervals and the country codes are read from the
# real interval and country tables, which are not written to.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 12;

use TableDefs qw(%TABLE original_table_name);
use CoreFunction qw(loadConfig);

use EDTPCTest;
use PBDBTester;
use EditTester qw(clear_table sql_command);

use MatrixBase qw(initializeBins updateCollectionMatrix deleteFromCollectionMatrix
		  updateOccurrenceMatrix updateOccurrenceMatrixReids deleteFromOccurrenceMatrix
		  deleteReidsFromOccurrenceMatrix deleteCollsFromOccurrenceMatrix
		  updateSpecimenMatrix deleteFromSpecimenMatrix updateOccurrenceCounts);


# Establish the tester and select the test tables of the two modules that use MatrixBase.

my $T = PBDBTester->new('CollectionEdit');

my @MODULES = ('CollectionEdit', 'SpecimenEdit');

EDTPCTest->select_test_tables(@MODULES);

my $dbh = $T->dbh;


# The bin resolutions are read from the API configuration file config.yml, as the data
# service does. The other values are read from the real tables.

my $CONFIG = loadConfig();

my @RESO = map { $_->{resolution} } grep { $_->{resolution} && $_->{resolution} > 0 }
    ref $CONFIG->{bins} eq 'ARRAY' ? $CONFIG->{bins}->@* : ();

my (%AGE, %CC, %ORIG);


# This class collects the debugging output of the MatrixBase functions, which is the SQL
# they execute.

{
    package SQLCollector;
    
    sub new { return bless { lines => [] }, shift }
    sub debug_line { push $_[0]{lines}->@*, $_[1] }
    sub statements { return scalar $_[0]{lines}->@* }
    sub output { return join('', $_[0]{lines}->@*) }
}


# Return the rows of a table that match the condition, as a hash keyed on the first
# column, or on the first two columns joined by '/' if $two_keys is true.

sub rows {
    
    my ($table, $columns, $where, $two_keys) = @_;
    
    my $sql = "SELECT $columns FROM $TABLE{$table}" . ($where ? " WHERE $where" : "");
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    my ($k1, $k2) = $columns =~ / ^ \s* (\w+) \s* , \s* (\w+) /x;
    
    return { map { ($two_keys ? "$_->{$k1}/$_->{$k2}" : $_->{$k1}) => $_ } @$result };
}


# Return the keys of the rows of a table that match the condition, as a string.

sub keys_of {
    
    my ($table, $columns, $where) = @_;
    
    my $sql = "SELECT concat_ws('/', $columns) FROM $TABLE{$table}" . ($where ? " WHERE $where" : "") .
	" ORDER BY 1";
    
    return join(' ', sort { $a <=> $b || $a cmp $b } @{$dbh->selectcol_arrayref($sql)});
}


# Compute the bin of a location at the specified level, the way the bin SQL does. A
# resolution under one degree has a larger base.

sub bin_id {
    
    my ($level, $lng, $lat) = @_;
    
    my $reso = $RESO[$level-1];
    
    return 0 unless $reso && defined $lng && defined $lat;
    
    my ($id_base, $lng_base) = $reso < 1 ? (100000000, 10000) : (1000000, 1000);
    
    return $level * $id_base + $lng_base * int(($lng + 180) / $reso) + int(($lat + 90) / $reso);
}


# Create the test tables if they do not exist, clear them, and fill them with the test
# data. Collection 3 has more than 1000 occurrences, so the batches can be tested.

subtest 'setup' => sub {
    
    EDTPCTest->establish_test_tables($dbh, @MODULES);
    
    clear_table($_) foreach EDTPCTest->module_tables(@MODULES);
    
    my $real_auth = original_table_name($TABLE{AUTHORITY_DATA});
    
    sql_command("INSERT INTO <<AUTHORITY_DATA>> SELECT * FROM $real_auth
		WHERE taxon_no in (38613, 54833)");
    
    %ORIG = map { $_->[0] => $_->[1] }
	@{$dbh->selectall_arrayref("SELECT taxon_no, orig_no FROM $TABLE{AUTHORITY_DATA}")};
    
    ok( $ORIG{38613} && $ORIG{54833}, "the test taxa are in place" ) || BAIL_OUT("no test taxa");
    
    # The ages of the Campanian (113) and Maastrichtian (112), and the codes of the United
    # States, are read from the real tables.
    
    foreach my $int ( 112, 113 )
    {
	$AGE{$int} = $dbh->selectrow_hashref("SELECT early_age, late_age FROM $TABLE{INTERVAL_DATA}
		WHERE interval_no = $int");
    }
    
    ok( $AGE{112}{early_age} && $AGE{112}{early_age} == $AGE{113}{late_age},
	"the Maastrichtian follows the Campanian" ) || BAIL_OUT("no intervals");
    
    my ($cc, $continent) = $dbh->selectrow_array("SELECT cc, continent FROM $TABLE{COUNTRY_MAP}
		WHERE name = 'United States'");
    
    %CC = ( cc => $cc, continent => $continent );
    
    ok( $cc, "the country code of the United States was found" );
    
    # Collections 2, 3, and 5 are not yet released; 4 was released in the past. Collection
    # 2 has no coordinates and an unknown country, and no minimum interval.
    
    sql_command("INSERT INTO <<COLLECTION_DATA>> (collection_no, collection_name, reference_no,
		country, lat, lng, max_interval_no, min_interval_no, access_level, release_date)
	VALUES (1, 'lib02 one', 5, 'United States', 45.5, -100.25, 113, 112, 'the public', '2020-01-01'),
	       (2, 'lib02 two', 6, 'Nowhere', NULL, NULL, 112, 0, 'database members', '2099-01-01'),
	       (3, 'lib02 three', 7, 'United States', 10, 20, 113, 113, 'group members', '2099-01-01'),
	       (4, 'lib02 four', 5, 'United States', -10, -20, 112, 0, 'authorizer only', '2020-01-01'),
	       (5, 'lib02 five', 5, 'United States', -89.5, 179.5, 112, 0, 'authorizer only', '2099-01-01')");
    
    sql_command("INSERT INTO <<COLLECTION_LOC>> (collection_no, protected) VALUES (1, 'NPS')");
    
    # Occurrence 1 has been reidentified twice, and its latest identification is reid 2.
    # Occurrence 3 has no taxon. Occurrences 11-1210 are the batch occurrences in
    # collection 3.
    
    sql_command("INSERT INTO <<OCCURRENCE_DATA>> (occurrence_no, collection_no, taxon_no,
		genus_reso, genus_name, species_name, reference_no, authorizer_no, enterer_no, reid_no)
	VALUES (1, 1, 38613, NULL, 'Tyrannosaurus', 'sp.', 5, 39998, 39997, 2),
	       (2, 1, 54833, 'cf.', 'Tyrannosaurus', 'rex', 0, 39998, 39997, 0),
	       (3, 2, 0, NULL, 'Nosuchgenus', 'thing', 6, 39998, 39997, 0)");
    
    my $batch = join(',', map { "($_, 3, 38613, 'Tyrannosaurus', 'sp.', 7, 39998, 39997, 0)" } 11 .. 1210);
    
    sql_command("INSERT INTO <<OCCURRENCE_DATA>> (occurrence_no, collection_no, taxon_no, genus_name,
		species_name, reference_no, authorizer_no, enterer_no, reid_no) VALUES $batch");
    
    sql_command("INSERT INTO <<REID_DATA>> (reid_no, occurrence_no, collection_no, taxon_no,
		genus_name, species_name, reference_no, most_recent, authorizer_no, enterer_no)
	VALUES (1, 1, 1, 38613, 'Tyrannosaurus', 'sp.', 6, 'NO', 39998, 39997),
	       (2, 1, 1, 54833, 'Tyrannosaurus', 'rex', 7, 'YES', 39998, 39997)");
    
    # Specimen 1 belongs to occurrence 1 and gives its own taxon, specimen 2 belongs to
    # occurrence 2 and does not, and specimen 3 has no occurrence.
    
    sql_command("INSERT INTO <<SPECIMEN_DATA>> (specimen_no, occurrence_no, taxon_no, reference_no,
		authorizer_no, enterer_no, modifier_no, specimen_id)
	VALUES (1, 1, 54833, 5, 39998, 39997, 0, 'lib02-1'),
	       (2, 2, NULL, 6, 39998, 39997, 0, 'lib02-2'),
	       (3, NULL, 38613, 7, 39998, 39997, 0, 'lib02-3')");
    
    is( keys_of('COLLECTION_MATRIX', 'collection_no'), '', "the collection matrix is empty" );
};


# initializeBins reads the bin resolutions from the API configuration, or from the Classic
# configuration if there is one, and generates the SQL that computes the bins of each
# collection. It does this only once. The data service object is stood in for by an object
# that holds the contents of config.yml, which is where Web::DataService keeps its
# configuration.

subtest 'initializeBins' => sub {
    
    ok( @RESO >= 2, "config.yml has at least two bin resolutions" ) || BAIL_OUT("no bins in config.yml");
    
    my $ds = bless { _config => $CONFIG }, 'Web::DataService';
    
    initializeBins($ds);
    
    my $n = scalar(@RESO);
    my $reso1 = quotemeta($RESO[0]);
    
    ok( $MatrixBase::HAS_INITIALIZED, "the module was initialized" );
    is_deeply( \@MatrixBase::BIN_RESO, \@RESO, "the resolutions were read from the API configuration" );
    like( $MatrixBase::BIN_SQL, qr{ bin_id_1 .* floor[(][(]lng[+]180[.]0[)]/$reso1 [)] }xs,
	  "the first bin has the first resolution" );
    like( $MatrixBase::BIN_SQL, qr{ bin_id_$n \s* = }x, "the bin SQL computes $n bins" );
    unlike( $MatrixBase::BIN_SQL, qr{ bin_id_${\($n+1)} }x, "and no more" );
    
    my $saved_sql = $MatrixBase::BIN_SQL;
    
    initializeBins(bless { _config => { bins => [ { resolution => 5 } ] } }, 'Web::DataService');
    
    is( $MatrixBase::BIN_SQL, $saved_sql, "a second call does nothing" );
    
    # The Classic configuration takes precedence over the API configuration. A resolution
    # that does not divide 180 degrees evenly is an error.
    
    {
	local $MatrixBase::HAS_INITIALIZED;
	local @MatrixBase::BIN_RESO;
	local $MatrixBase::BIN_SQL;
	local %PBDB::Constants::CONFIG = ( BIN_RESO => '5, 0.5' );
	
	initializeBins($ds);
	
	is_deeply( \@MatrixBase::BIN_RESO, [5, 0.5], "the resolutions were read from the Classic configuration" );
	like( $MatrixBase::BIN_SQL, qr{ bin_id_2 \s* = .* 200000000 \s* [+] \s* 10000 }xs,
	      "a resolution under one degree has a larger base" );
	
	$MatrixBase::HAS_INITIALIZED = 0;
	@MatrixBase::BIN_RESO = ();
	
	%PBDB::Constants::CONFIG = ( BIN_RESO => '7' );
	
	eval { initializeBins($ds) };
	
	like( $@, qr{invalid resolution 7}, "a resolution of 7 degrees was rejected" );
    }
    
    is( $MatrixBase::BIN_SQL, $saved_sql, "the bin SQL of the API configuration is back" );
};


# updateCollectionMatrix writes a row for each collection, with its location, country,
# ages, interval numbers, reference, occurrence count, access level, and bins.

subtest 'collection matrix' => sub {
    
    updateCollectionMatrix($dbh, [1, 2, 3, 4, 5]);
    
    my $m = rows('COLLECTION_MATRIX', 'collection_no, lng, lat, ST_X(loc) as x, ST_Y(loc) as y,
		cc, continent, protected, early_age, late_age, early_int_no, late_int_no,
		reference_no, n_occs, access_level, bin_id_1, bin_id_2, bin_id_3');
    
    is( keys_of('COLLECTION_MATRIX', 'collection_no'), '1 2 3 4 5', "each collection has a row" );
    
    # Collection 1.
    
    my $c = $m->{1};
    
    cmp_ok( $c->{lng}, '==', -100.25, "longitude" );
    cmp_ok( $c->{lat}, '==', 45.5, "latitude" );
    ok( $c->{x} == -100.25 && $c->{y} == 45.5, "the location point" );
    is( "$c->{cc} $c->{continent}", "$CC{cc} $CC{continent}", "the country and continent" );
    is( $c->{protected}, 'NPS', "the protection status from the location table" );
    cmp_ok( $c->{early_age}, '==', $AGE{113}{early_age}, "the early age is that of the maximum interval" );
    cmp_ok( $c->{late_age}, '==', $AGE{112}{late_age}, "the late age is that of the minimum interval" );
    is( "$c->{early_int_no} $c->{late_int_no}", "113 112", "the interval numbers" );
    is( $c->{reference_no}, 5, "the reference" );
    is( $c->{n_occs}, 2, "the number of occurrences" );
    is( $c->{access_level}, 0, "a public collection has access level 0" );
    is( $c->{bin_id_1}, bin_id(1, -100.25, 45.5), "the first bin" );
    is( $c->{bin_id_2}, bin_id(2, -100.25, 45.5), "the second bin" );
    is( $c->{bin_id_3}, bin_id(3, -100.25, 45.5), "the third bin" );
    
    # Collection 2 has no location and no minimum interval.
    
    $c = $m->{2};
    
    ok( ! defined $c->{lng} && ! defined $c->{lat}, "a collection without coordinates has none" );
    ok( $c->{x} == 1000 && $c->{y} == 1000, "its location point is (1000, 1000)" );
    ok( ! defined $c->{cc} && ! defined $c->{continent}, "an unknown country has no codes" );
    ok( $c->{bin_id_1} == 0 && $c->{bin_id_2} == 0 && $c->{bin_id_3} == 0, "it has no bins" );
    is( "$c->{early_int_no} $c->{late_int_no}", "112 112", "without a minimum interval, both are the maximum" );
    cmp_ok( $c->{early_age}, '==', $AGE{112}{early_age}, "the early age is that of the maximum interval" );
    cmp_ok( $c->{late_age}, '==', $AGE{112}{late_age}, "the late age is that of the maximum interval" );
    
    # The access levels, before and after the release date.
    
    is( $m->{2}{access_level}, 1, "an unreleased collection for database members has access level 1" );
    is( $m->{3}{access_level}, 2, "an unreleased collection for group members has access level 2" );
    is( $m->{5}{access_level}, 2, "an unreleased collection for the authorizer only has access level 2" );
    is( $m->{4}{access_level}, 0, "a released collection has access level 0" );
    is( $m->{3}{n_occs}, 1200, "collection 3 has 1200 occurrences" );
    is( $m->{5}{bin_id_3}, bin_id(3, 179.5, -89.5), "a collection near the corner of the map has a bin" );
    
    # A changed collection gets a new row.
    
    sql_command("UPDATE <<COLLECTION_DATA>> SET lat = 46.5, access_level = 'group members'
		WHERE collection_no = 1");
    
    updateCollectionMatrix($dbh, 1);
    
    $c = rows('COLLECTION_MATRIX', 'collection_no, lat, access_level, bin_id_2', 'collection_no = 1')->{1};
    
    cmp_ok( $c->{lat}, '==', 46.5, "the changed latitude" );
    is( $c->{bin_id_2}, bin_id(2, -100.25, 46.5), "the new bin" );
    is( $c->{access_level}, 0, "a released collection still has access level 0" );
};


# updateOccurrenceMatrix writes a row for each identification of each occurrence: the
# original one with reid_no 0, and one for each reidentification. The latest one is
# marked.

subtest 'occurrence matrix' => sub {
    
    updateOccurrenceMatrix($dbh, [1, 2, 3]);
    
    my $m = rows('OCCURRENCE_MATRIX', 'occurrence_no, reid_no, latest_ident, collection_no, taxon_no,
		orig_no, genus_name, genus_reso, subgenus_name, species_name, early_age, late_age,
		reference_no, authorizer_no, enterer_no', '', 1);
    
    is( join(' ', sort keys %$m), '1/0 1/1 1/2 2/0 3/0', "each identification has a row" );
    
    is( join(' ', map { $m->{$_}{latest_ident} } '1/0', '1/1', '1/2'), '0 0 1',
	"only the reidentification named by the occurrence is the latest" );
    is( $m->{'2/0'}{latest_ident}, 1, "an occurrence that was not reidentified is the latest" );
    
    is( "$m->{'1/0'}{taxon_no} $m->{'1/2'}{taxon_no}", "38613 54833", "each identification has its taxon" );
    is( $m->{'1/2'}{orig_no}, $ORIG{54833}, "the orig_no is read from the authorities table" );
    is( "$m->{'3/0'}{taxon_no} $m->{'3/0'}{orig_no}", "0 0", "an occurrence without a taxon has 0" );
    is( $m->{'1/1'}{reference_no}, 6, "a reidentification has its own reference" );
    is( $m->{'2/0'}{reference_no}, 0, "a missing reference is 0" );
    is( $m->{'2/0'}{genus_reso}, 'cf.', "the genus modifier" );
    is( $m->{'1/0'}{genus_reso}, '', "a missing modifier is empty" );
    is( $m->{'1/0'}{subgenus_name}, '', "a missing subgenus is empty" );
    is( $m->{'1/2'}{collection_no}, 1, "the collection" );
    is( $m->{'1/0'}{authorizer_no}, 39998, "the authorizer" );
    cmp_ok( $m->{'1/1'}{early_age}, '==', $AGE{113}{early_age}, "the early age of the collection" );
    cmp_ok( $m->{'1/1'}{late_age}, '==', $AGE{112}{late_age}, "the late age of the collection" );
    cmp_ok( $m->{'3/0'}{early_age}, '==', $AGE{112}{early_age}, "the early age of another collection" );
    
    is( keys_of('SPECIMEN_MATRIX', 'specimen_no, reid_no'), '1/0 1/1 1/2 2/0',
	"the specimens of these occurrences were written to the specimen matrix" );
};


# The reidentifications can be removed from the occurrence matrix and put back separately.

subtest 'reidentifications' => sub {
    
    deleteReidsFromOccurrenceMatrix($dbh, 1);
    
    is( keys_of('OCCURRENCE_MATRIX', 'occurrence_no, reid_no', 'occurrence_no < 10'), '1/0 1/2 2/0 3/0',
	"the row of reidentification 1 was removed" );
    
    updateOccurrenceMatrixReids($dbh, [1]);
    
    my $m = rows('OCCURRENCE_MATRIX', 'occurrence_no, reid_no, latest_ident, taxon_no, reference_no',
		 'occurrence_no = 1', 1);
    
    is( join(' ', sort keys %$m), '1/0 1/1 1/2', "it was put back" );
    is( "$m->{'1/1'}{latest_ident} $m->{'1/1'}{taxon_no} $m->{'1/1'}{reference_no}", "0 38613 6",
	"with its values" );
    
    deleteReidsFromOccurrenceMatrix($dbh, [1, 2]);
    
    is( keys_of('OCCURRENCE_MATRIX', 'occurrence_no, reid_no', 'occurrence_no = 1'), '1/0',
	"two reidentifications were removed at once" );
    
    updateOccurrenceMatrixReids($dbh, [1, 2]);
    
    is( keys_of('OCCURRENCE_MATRIX', 'occurrence_no, reid_no', 'occurrence_no = 1'), '1/0 1/1 1/2',
	"and put back" );
};


# updateSpecimenMatrix writes a row for each identification of the occurrence of each
# specimen, because a specimen may be labeled with any of them. A specimen that gives its
# own taxon has that taxon; otherwise it has the taxon of the identification. A specimen
# with no occurrence has a single row.

subtest 'specimen matrix' => sub {
    
    # The rows written by updateOccurrenceMatrix are deleted first, so that the number of
    # rows written can be checked.
    
    deleteFromSpecimenMatrix($dbh, [1, 2]);
    
    is( keys_of('SPECIMEN_MATRIX', 'specimen_no'), '', "the specimen matrix is empty" );
    
    my $count = updateSpecimenMatrix($dbh, [1, 2, 3]);
    
    is( $count, 5, "five rows were written" );
    
    my $m = rows('SPECIMEN_MATRIX', 'specimen_no, reid_no, occurrence_no, latest_ident, taxon_no,
		orig_no, reference_no, authorizer_no', '', 1);
    
    is( join(' ', sort keys %$m), '1/0 1/1 1/2 2/0 3/0', "each identification of each specimen has a row" );
    
    is( join(' ', map { $m->{$_}{latest_ident} } '1/0', '1/1', '1/2'), '0 0 1',
	"the latest identification is marked" );
    is( join(' ', map { $m->{$_}{taxon_no} } '1/0', '1/1', '1/2'), '54833 54833 54833',
	"a specimen that gives its own taxon has it in every row" );
    is( $m->{'1/0'}{orig_no}, $ORIG{54833}, "and its orig_no" );
    is( "$m->{'2/0'}{taxon_no} $m->{'2/0'}{orig_no}", "54833 $ORIG{54833}",
	"a specimen without a taxon has the taxon of its occurrence" );
    is( "$m->{'3/0'}{occurrence_no} $m->{'3/0'}{latest_ident} $m->{'3/0'}{taxon_no}", "0 1 38613",
	"a specimen without an occurrence has one row" );
    is( $m->{'2/0'}{reference_no}, 6, "the reference is that of the specimen" );
    
    ok( updateSpecimenMatrix($dbh, 3), "writing the same row again reports a change" );
    
    is( updateSpecimenMatrix($dbh, 999), 0, "a specimen that does not exist writes nothing" );
    
    deleteFromSpecimenMatrix($dbh, 1);
    
    is( keys_of('SPECIMEN_MATRIX', 'specimen_no, reid_no'), '2/0 3/0', "every row of specimen 1 was deleted" );
    
    deleteFromSpecimenMatrix($dbh, [2, 3]);
    
    is( keys_of('SPECIMEN_MATRIX', 'specimen_no'), '', "the others were deleted" );
};


# The specimen matrix follows the identifications of the occurrences. When an occurrence
# is written to the occurrence matrix, the rows of its specimens are written again, and
# when a reidentification is removed, the rows of its specimens for it are removed too.

subtest 'specimens follow identifications' => sub {
    
    updateSpecimenMatrix($dbh, [1, 2]);
    
    is( keys_of('SPECIMEN_MATRIX', 'specimen_no, reid_no'), '1/0 1/1 1/2 2/0', "the starting rows" );
    
    # Reidentify occurrence 2, as CollectionEdit does: add the reidentification, mark it
    # as the latest, and update the occurrence matrix.
    
    sql_command("INSERT INTO <<REID_DATA>> (reid_no, occurrence_no, collection_no, taxon_no,
		genus_name, species_name, reference_no, most_recent, authorizer_no, enterer_no)
	VALUES (3, 2, 1, 38613, 'Tyrannosaurus', 'sp.', 7, 'YES', 39998, 39997)");
    sql_command("UPDATE <<OCCURRENCE_DATA>> SET reid_no = 3 WHERE occurrence_no = 2");
    
    updateOccurrenceMatrix($dbh, 2);
    
    my $m = rows('SPECIMEN_MATRIX', 'specimen_no, reid_no, latest_ident, taxon_no', 'specimen_no = 2', 1);
    
    is( join(' ', sort keys %$m), '2/0 2/3', "the specimen has a row for the new identification" );
    is( "$m->{'2/0'}{latest_ident} $m->{'2/3'}{latest_ident}", "0 1", "which is now the latest" );
    is( $m->{'2/3'}{taxon_no}, 38613, "with the taxon of the new identification" );
    
    # Remove the reidentification again.
    
    sql_command("DELETE FROM <<REID_DATA>> WHERE reid_no = 3");
    sql_command("UPDATE <<OCCURRENCE_DATA>> SET reid_no = 0 WHERE occurrence_no = 2");
    
    deleteReidsFromOccurrenceMatrix($dbh, 3);
    
    is( keys_of('SPECIMEN_MATRIX', 'specimen_no, reid_no', 'specimen_no = 2'), '2/0',
	"the row for the removed identification was deleted" );
    
    updateOccurrenceMatrix($dbh, 2);
    
    is( rows('SPECIMEN_MATRIX', 'specimen_no, reid_no, latest_ident', 'specimen_no = 2', 1)->{'2/0'}{latest_ident},
	1, "the original identification is the latest again" );
    
    # A stale row, for an identification that no longer exists, is removed when the
    # occurrence is written again.
    
    sql_command("INSERT INTO <<SPECIMEN_MATRIX>> (specimen_no, occurrence_no, reid_no, latest_ident,
		taxon_no, orig_no, reference_no, authorizer_no, enterer_no, modifier_no)
	VALUES (2, 2, 99, 1, 38613, 38613, 6, 39998, 39997, 0)");
    
    updateOccurrenceMatrix($dbh, 2);
    
    is( keys_of('SPECIMEN_MATRIX', 'specimen_no, reid_no', 'specimen_no = 2'), '2/0', "a stale row was removed" );
    
    # Writing reidentifications to the occurrence matrix also writes the rows of their
    # specimens.
    
    deleteFromSpecimenMatrix($dbh, 1);
    
    updateOccurrenceMatrixReids($dbh, 2);
    
    is( keys_of('SPECIMEN_MATRIX', 'specimen_no, reid_no', 'specimen_no = 1'), '1/0 1/1 1/2',
	"the rows of the specimen of a reidentification were written" );
    
    deleteFromSpecimenMatrix($dbh, [1, 2]);
};


# updateOccurrenceCounts sets the number of occurrences of each collection.

subtest 'occurrence counts' => sub {
    
    sql_command("INSERT INTO <<OCCURRENCE_DATA>> (occurrence_no, collection_no, taxon_no, genus_name,
		species_name, reference_no, authorizer_no, enterer_no, reid_no)
	VALUES (4, 4, 38613, 'Tyrannosaurus', 'sp.', 5, 39998, 39997, 0)");
    
    sql_command("DELETE FROM <<OCCURRENCE_DATA>> WHERE occurrence_no = 3");
    
    updateOccurrenceCounts($dbh, [2, 4]);
    
    my $m = rows('COLLECTION_MATRIX', 'collection_no, n_occs');
    
    is( $m->{4}{n_occs}, 1, "an occurrence was counted" );
    is( $m->{2}{n_occs}, 0, "a collection without occurrences has 0" );
    is( $m->{1}{n_occs}, 2, "a collection that was not named is unchanged" );
    
    sql_command("DELETE FROM <<OCCURRENCE_DATA>> WHERE occurrence_no = 4");
    
    updateOccurrenceCounts($dbh, 4);
    
    is( rows('COLLECTION_MATRIX', 'collection_no, n_occs', 'collection_no = 4')->{4}{n_occs}, 0,
	"a single collection number" );
};


# Occurrences and collections can be removed from the matrices.

subtest 'deletions' => sub {
    
    deleteFromOccurrenceMatrix($dbh, 1);
    
    is( keys_of('OCCURRENCE_MATRIX', 'occurrence_no, reid_no', 'occurrence_no < 10'), '2/0 3/0',
	"every identification of occurrence 1 was removed" );
    
    deleteFromOccurrenceMatrix($dbh, [2, 3]);
    
    is( keys_of('OCCURRENCE_MATRIX', 'occurrence_no', 'occurrence_no < 10'), '', "two occurrences at once" );
    
    updateOccurrenceMatrix($dbh, [1, 2]);
    
    deleteCollsFromOccurrenceMatrix($dbh, 1);
    
    is( keys_of('OCCURRENCE_MATRIX', 'occurrence_no', 'occurrence_no < 10'), '',
	"the occurrences of collection 1 were removed" );
    
    deleteFromCollectionMatrix($dbh, 2);
    
    is( keys_of('COLLECTION_MATRIX', 'collection_no'), '1 3 4 5', "collection 2 was removed" );
    
    deleteFromCollectionMatrix($dbh, [4, 5]);
    
    is( keys_of('COLLECTION_MATRIX', 'collection_no'), '1 3', "two collections at once" );
};


# Each function handles at most 1000 records per statement, so a longer list takes more
# than one. An empty list does nothing.

subtest 'batches' => sub {
    
    my @occs = (11 .. 1210);
    
    my $collector = SQLCollector->new;
    
    updateOccurrenceMatrix($dbh, \@occs, $collector);
    
    is( $collector->statements, 8, "1200 occurrences took two batches of four statements: the " .
	"occurrences, their reidentifications, and the rows of their specimens" );
    
    is( $dbh->selectrow_array("SELECT count(*) FROM $TABLE{OCCURRENCE_MATRIX} WHERE collection_no = 3"),
	1200, "all 1200 were written" );
    
    is( scalar(@occs), 1200, "the list passed in is unchanged" );
    
    $collector = SQLCollector->new;
    
    deleteCollsFromOccurrenceMatrix($dbh, 3, $collector);
    
    is( $collector->statements, 1, "deleting them by collection took one" );
    
    updateOccurrenceMatrix($dbh, \@occs);
    
    $collector = SQLCollector->new;
    
    deleteFromOccurrenceMatrix($dbh, \@occs, $collector);
    
    is( $collector->statements, 2, "deleting them by occurrence took two" );
    
    is( $dbh->selectrow_array("SELECT count(*) FROM $TABLE{OCCURRENCE_MATRIX} WHERE collection_no = 3"),
	0, "all 1200 were deleted" );
    
    $collector = SQLCollector->new;
    
    foreach my $function ( \&updateCollectionMatrix, \&deleteFromCollectionMatrix,
			   \&updateOccurrenceMatrix, \&updateOccurrenceMatrixReids,
			   \&deleteFromOccurrenceMatrix, \&deleteReidsFromOccurrenceMatrix,
			   \&deleteCollsFromOccurrenceMatrix, \&updateSpecimenMatrix,
			   \&deleteFromSpecimenMatrix, \&updateOccurrenceCounts )
    {
	eval { $function->($dbh, [], $collector) };
	
	ok( ! $@, "an empty list is accepted" ) || diag("exception: $@");
    }
    
    is( $collector->statements, 0, "an empty list executes no statements" );
};


# The SQL is reported to an object with a 'debug_line' method, or else printed to STDERR.

subtest 'debugging output' => sub {
    
    my $collector = SQLCollector->new;
    
    updateCollectionMatrix($dbh, 1, $collector);
    
    is( $collector->statements, 2, "updating a collection takes two statements" );
    like( $collector->output, qr{ REPLACE \s+ INTO \s+ \S*coll_matrix .* UPDATE \s+ \S*coll_matrix .* SET \s+ bin_id_1 }xs,
	  "the row is written and then its bins are set" );
    
    my $stderr = '';
    
    {
	local *STDERR;
	open STDERR, '>', \$stderr;
	
	deleteFromSpecimenMatrix($dbh, 1, 1);
    }
    
    like( $stderr, qr{ DELETE \s+ FROM \s+ \S*spec_matrix \s+ WHERE \s+ specimen_no \s+ in \s+ [(]'1'[)] }xs,
	  "with a plain true value, the SQL is printed to STDERR" );
};


# Remove the test data.

subtest 'cleanup' => sub {
    
    clear_table($_) foreach EDTPCTest->module_tables(@MODULES);
    
    is( keys_of('COLLECTION_MATRIX', 'collection_no'), '', "the test data was removed" );
};
