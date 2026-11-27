#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the EditTransaction client modules, the subclasses of
# EditTransaction that handle edits to particular kinds of Paleobiology Database records.
#
# edtpc-06-collections.t : Test CollectionEdit, which handles collections, occurrences, and
#                          reidentifications: coordinates and intervals, validation,
#                          secondary references, taxonomic names, the collection,
#                          occurrence, and specimen matrices, type localities, deletion,
#                          permissions, and logging.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 19;

use File::Temp qw(tempdir);

use TableDefs qw(%TABLE original_table_name);

use EDTPCTest;
use PBDBTester;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_has_one_condition ok_has_condition
		  clear_table sql_command sql_fetchrow ok_found_record ok_no_record);

use OccurrenceBase;
use IntervalBase;
use MatrixBase qw(updateSpecimenMatrix deleteFromSpecimenMatrix);


# Establish the tester and select the test tables.

my $T = PBDBTester->new('CollectionEdit');

EDTPCTest->select_test_tables('CollectionEdit');

sub perm {
    
    my ($name, $table) = @_;
    
    return $T->new_perm("SESSION-$name", $table || 'COLLECTION_DATA');
}


# The taxa used by these tests: the genus Tyrannosaurus and the species T. rex. Their
# authority records are copied from the real table. The real type locality of T. rex is
# a real collection, so it does not match any of the test collections.

my $GENUS_NO = 38613;
my $SPECIES_NO = 54833;


# References 5, 6, and 7 are real references. The test collections and occurrences refer to
# them, and REFERENCE_DATA is not switched to the test database for CollectionEdit.

my %COLL = ( collection_name => 'Test collection', reference_id => 5,
	     access_level => 'the public', release_date => 'immediate',
	     collection_type => 'general faunal/floral', country => 'United States',
	     latdeg => 45, latmin => 30, latdir => 'North',
	     lngdeg => 100, lngdec => 25, lngdir => 'West',
	     latlng_basis => 'stated in text', lithology1 => 'sandstone',
	     environment => 'fluvial-lacustrine indet.', assembl_comps => 'macrofossils',
	     max_interval => 'Maastrichtian' );


# Insert a collection, and return its key.

sub new_collection {
    
    my (%fields) = @_;
    
    my $edt = CollectionEdit->new($T->dbh, { permission => perm('ENTERER') });
    
    $edt->insert_record('COLLECTION_DATA', { %COLL, %fields });
    $edt->commit;
    
    return ($edt->inserted_keys)[0];
}


# Return the rows of a table that match an expression, as a string.

sub rows {
    
    my ($table, $columns, $expr) = @_;
    
    my $rows = $T->dbh->selectall_arrayref("SELECT $columns FROM $TABLE{$table}
		WHERE $expr ORDER BY $columns");
    
    return join('; ', map { join(',', map { $_ // 'NULL' } @$_) } @$rows);
}


# Read the contents of a log file.

sub read_log {
    
    my ($logfile) = @_;
    
    open(my $fh, '<', $logfile) || return '';
    local $/;
    return <$fh>;
}


# Start with empty tables, plus the two taxa. The occurrence modifiers and the interval
# data must be loaded before any occurrences or collections are validated.

subtest 'setup' => sub {
    
    clear_table($_) foreach EDTPCTest->module_tables('CollectionEdit');
    
    my $real_auth = original_table_name($TABLE{AUTHORITY_DATA});
    
    sql_command("INSERT INTO <<AUTHORITY_DATA>> SELECT * FROM $real_auth
		WHERE taxon_no in ($GENUS_NO, $SPECIES_NO)");
    
    is( rows('AUTHORITY_DATA', 'taxon_no, taxon_name', '1'),
	"$GENUS_NO,Tyrannosaurus; $SPECIES_NO,Tyrannosaurus rex", "the taxa are in place" );
    
    OccurrenceBase::initializeModifiers($T->dbh);
    IntervalBase->cache_interval_data($T->dbh);
    
    ok( IntervalBase::int_defined('Maastrichtian'), "the interval data was loaded" );
};


# Insert a collection. The latitude and longitude are computed from the degrees, minutes,
# and decimal digits, the interval names are looked up, the primary reference and the
# additional references become secondary references, and a row is added to the collection
# matrix.

subtest 'insert collection' => sub {
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('COLLECTION_DATA', { %COLL, reference_add => [6] });
    
    ok_commit("the collection was inserted");
    
    my ($key) = $edt->inserted_keys;
    
    ok( $key, "the new key was returned" ) || return;
    
    my ($lat, $lng, $prec, $max_int, $enterer, $release, $created) =
	sql_fetchrow("SELECT lat, lng, latlng_precision, max_interval_no, enterer_no,
			release_date, created
		FROM <<COLLECTION_DATA>> WHERE collection_no = $key");
    
    is( $lat + 0, 45.5, "the latitude was computed from degrees and minutes" );
    is( $lng + 0, -100.25, "the longitude was computed from degrees and decimal digits" );
    is( $prec, 'minutes', "the precision is the finest one given" );
    is( $max_int, IntervalBase::int_defined('Maastrichtian'), "the interval was looked up" );
    is( $enterer, 39997, "the enterer was filled in" );
    
    is( rows('COLLECTION_REFS', 'reference_no', "collection_no = $key"), "5; 6",
	"the primary and additional references are secondary references" );
    
    ok_found_record('COLLECTION_MATRIX', "collection_no = $key");
    
    is( $release, $created, "a public collection is released when it is created" );
    
    # A collection with a later release date is released that long after it is created.
    
    my $later = new_collection(access_level => 'database members', release_date => '2 years');
    
    ok( $later, "a collection with a later release date was inserted" );
    
    ok_found_record('COLLECTION_DATA', "collection_no = '$later' and
		release_date = created + interval 2 year", "the release date is 2 years after creation");
    
    # Until a collection is released, the collection matrix restricts access to it according
    # to its access level.
    
    foreach my $t ( [ 'database members', 1 ], [ 'group members', 2 ], [ 'authorizer only', 2 ] )
    {
	my ($level, $expected) = @$t;
	
	my $coll = new_collection(access_level => $level, release_date => '1 year');
	
	my ($matrix_level) = sql_fetchrow("SELECT access_level FROM <<COLLECTION_MATRIX>>
		WHERE collection_no = '$coll'");
	
	is( $matrix_level, $expected, "an unreleased collection for '$level' has matrix access level $expected" );
    }
    
    my ($mlat, $mlng) = sql_fetchrow("SELECT lat, lng FROM <<COLLECTION_MATRIX>>
		WHERE collection_no = $key");
    
    is( "$mlat|$mlng", "$lat|$lng", "the collection matrix has the coordinates" );
};


# Check the validation of the collection fields.

subtest 'collection validation' => sub {
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('COLLECTION_DATA', { %COLL, latdeg => 95, latmin => 75,
					     max_interval => 'Nosuchian' });
    
    ok_failed_commit("a collection with bad values was rejected");
    
    my @conds = $edt->conditions;
    
    is( scalar(grep { /E_BAD_VALUE.*'latdeg'/ } @conds), 1, "the bad latitude was reported" );
    is( scalar(grep { /E_BAD_VALUE.*'latmin'/ } @conds), 1, "the bad minutes were reported" );
    is( scalar(grep { /E_BAD_VALUE.*'max_interval'/ } @conds), 1,
	"the unknown interval was reported" );
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('COLLECTION_DATA', { %COLL, latmin => 30, latdec => 5 });
    
    ok_failed_commit("minutes and decimal digits cannot both be given");
    
    ok_has_one_condition('E_NOT_BOTH', "the conflict was reported");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('COLLECTION_DATA', { %COLL, access_level => 'the public',
					     release_date => '2 years' });
    
    ok_failed_commit("a public collection must be released immediately");
    
    ok_has_one_condition('E_BAD_VALUE', "the bad release date was reported");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('COLLECTION_DATA', { %COLL, access_level => 'database members',
					     release_date => '5 years' });
    
    ok_failed_commit("a release date more than 3 years away was rejected");
    
    ok_has_one_condition('E_BAD_VALUE', "the bad release date was reported");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('COLLECTION_DATA', { %COLL, collection_name => '',
					     collection_type => '', assembl_comps => '' });
    
    ok_failed_commit("a collection without required fields was rejected");
    
    is( scalar(grep { /^E_REQUIRED/ } $edt->conditions), 3,
	"each missing field was reported" );
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('COLLECTION_DATA', { %COLL, reference_id => 99999999 });
    
    ok_failed_commit("a collection with an unknown reference was rejected");
    
    ok_has_one_condition('E_BAD_VALUE', "the unknown reference was reported");
    
    ok_no_record('COLLECTION_DATA', "collection_name = ''");
    
    # A release date may be given in months or years, with a maximum of 3 years.
    
    my $months = new_collection(access_level => 'database members', release_date => '18 months');
    
    ok( $months, "a release date of 18 months was accepted" );
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('COLLECTION_DATA', { %COLL, access_level => 'database members',
					     release_date => '48 months' });
    
    ok_failed_commit("a release date of 48 months was rejected");
    
    ok_has_one_condition('E_BAD_VALUE', "the bad release date was reported");
};


# Update a collection. A new latitude is computed from the new values and the stored ones.
# If the minimum interval is older than the maximum, the two are switched.

subtest 'update collection' => sub {
    
    my $key = new_collection();
    
    ok( $key, "a collection was inserted" ) || return;
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->update_record('COLLECTION_DATA', { collection_id => $key, latdeg => 10,
					     latdir => 'South', max_interval => 'Campanian',
					     min_interval => 'Maastrichtian' });
    
    ok_commit("the collection was updated");
    
    my ($lat, $lng, $max_int, $min_int) =
	sql_fetchrow("SELECT lat, lng, max_interval_no, min_interval_no
		FROM <<COLLECTION_DATA>> WHERE collection_no = $key");
    
    is( $lat + 0, -10.5, "the latitude was computed with the stored minutes" );
    is( $lng + 0, -100.25, "the longitude was not changed" );
    
    is( "$max_int|$min_int", IntervalBase::int_defined('Campanian') . '|' .
	IntervalBase::int_defined('Maastrichtian'), "the intervals were stored" );
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->update_record('COLLECTION_DATA', { collection_id => $key,
					     max_interval => 'Maastrichtian',
					     min_interval => 'Campanian' });
    
    ok_commit("the collection was updated with the intervals in the wrong order");
    
    ($max_int, $min_int) = sql_fetchrow("SELECT max_interval_no, min_interval_no
		FROM <<COLLECTION_DATA>> WHERE collection_no = $key");
    
    is( "$max_int|$min_int", IntervalBase::int_defined('Campanian') . '|' .
	IntervalBase::int_defined('Maastrichtian'), "the intervals were switched" );
    
    # The ages and their errors must be decimal numbers.
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('COLLECTION_DATA', { %COLL, direct_ma => 'abc' });
    
    ok_failed_commit("a direct age that is not a number was rejected");
    
    ok_has_one_condition('E_FORMAT', "the bad age was reported");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('COLLECTION_DATA', { %COLL, direct_ma => '70.5',
					    direct_ma_error => 'abc' });
    
    ok_failed_commit("a direct age error that is not a number was rejected");
    
    ok_has_one_condition('E_FORMAT', "the bad error value was reported");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->update_record('COLLECTION_DATA', { collection_id => $key, direct_ma => 'abc' });
    
    ok_failed_commit("an update with a direct age that is not a number was rejected");
    
    ok_has_one_condition('E_FORMAT', "the bad age was reported");
};


# Secondary references can be added and removed. The primary reference is never removed,
# and neither is a reference that is used by an occurrence in the collection. Each change
# is logged. A removal is logged with a statement that would put back the removed rows.

subtest 'secondary references' => sub {
    
    my $key = new_collection();
    
    ok( $key, "a collection was inserted" ) || return;
    
    my $logdir = tempdir(CLEANUP => 1);
    my $logfile = "$logdir/edtpc-06.log";
    
    my $save_filename = CollectionEdit->log_filename;
    
    CollectionEdit->log_filename($logfile);
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->update_record('COLLECTION_DATA', { collection_id => $key, reference_add => [6, 7] });
    
    ok_commit("references were added");
    
    is( rows('COLLECTION_REFS', 'reference_no', "collection_no = $key"), "5; 6; 7",
	"the references were added" );
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->update_record('COLLECTION_DATA', { collection_id => $key,
					     reference_delete => [5, 6] });
    
    ok_commit("references were removed");
    
    is( rows('COLLECTION_REFS', 'reference_no', "collection_no = $key"), "5; 7",
	"the primary reference was kept and the other was removed" );
    
    CollectionEdit->log_filename($save_filename);
    
    my $log = read_log($logfile);
    
    like( $log, qr/^# .* [|] INSERT [|] \S*secondary_refs [|] collection_no = '$key'.*\n
		   ^INSERT/mx, "the addition was logged with no rows to put back" );
    
    like( $log, qr/^# .* [|] DELETE [|] \S*secondary_refs [|] collection_no = '$key'.*\n
		   ^\# \s REPLACE \s INTO \s \S*secondary_refs \s .* VALUES \s [(][^)]*'6'[)]\n/mx,
	  "the removal was logged with only the removed row" );
};


# Insert occurrences. Each identified name is split into its components, and is matched
# against the authorities table to find its taxon. A name that matches no taxon gets the
# taxon number 0. A name may not duplicate another occurrence in the same collection.

subtest 'occurrences' => sub {
    
    my $coll = new_collection();
    
    ok( $coll, "a collection was inserted" ) || return;
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'Tyrannosaurus rex',
					     abund_value => 3, abund_unit => 'specimens' });
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'Tyrannosaurus cf. rex' });
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 6,
					     identified_name => 'Unknownus bar' });
    
    ok_commit("the occurrences were inserted");
    
    my @keys = $edt->inserted_keys;
    
    is( scalar(@keys), 3, "three keys were returned" );
    
    is( rows('OCCURRENCE_DATA', 'genus_name, species_reso, species_name, taxon_no',
	     "collection_no = $coll"),
	"Tyrannosaurus,NULL,rex,$SPECIES_NO; Tyrannosaurus,cf.,rex,$SPECIES_NO; Unknownus,NULL,bar,0",
	"the names were split and matched to taxa" );
    
    is( rows('OCCURRENCE_MATRIX', 'taxon_no, latest_ident', "collection_no = $coll"),
	"0,1; $SPECIES_NO,1; $SPECIES_NO,1", "the occurrences were added to the matrix" );
    
    is( rows('COLLECTION_REFS', 'reference_no', "collection_no = $coll"), "5; 6",
	"the occurrence references are secondary references" );
    
    my ($n_occs) = sql_fetchrow("SELECT n_occs FROM <<COLLECTION_MATRIX>>
		WHERE collection_no = $coll");
    
    is( $n_occs, 3, "the occurrence count was updated" );
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'Tyrannosaurus rex' });
    
    ok_failed_commit("a duplicate name was rejected");
    
    ok_has_one_condition('E_BAD_VALUE', "the duplicate was reported");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'Tyrannosaurus foo',
					     taxon_id => $SPECIES_NO });
    
    ok_failed_commit("a taxon that does not match the name was rejected");
    
    ok_has_one_condition('E_BAD_VALUE', "the mismatch was reported");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => 99999999, reference_id => 5,
					     identified_name => 'Tyrannosaurus rex' });
    
    ok_failed_commit("an occurrence in an unknown collection was rejected");
    
    ok_has_one_condition('E_BAD_VALUE', "the unknown collection was reported");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { reference_id => 5,
					     identified_name => 'Tyrannosaurus rex' });
    
    ok_failed_commit("an occurrence without a collection was rejected");
    
    ok_has_condition(qr/E_REQUIRED.*'collection_id'/, "the missing collection_id was reported");
    
    # Update an occurrence. The existing occurrences must first be loaded with
    # 'initialize_occs'.
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->initialize_occs({ $keys[0] => 1 }, { });
    
    $edt->update_record('OCCURRENCE_DATA', { occurrence_id => $keys[0], abund_value => 5 });
    
    ok_commit("the occurrence was updated");
    
    ok_found_record('OCCURRENCE_DATA', "occurrence_no = $keys[0] and abund_value = '5'");
};


# Reidentifications. The most recent one, according to the publication year of its
# reference, becomes the current identification of the occurrence.

subtest 'reidentifications' => sub {
    
    my $coll = new_collection();
    
    ok( $coll, "a collection was inserted" ) || return;
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'Tyrannosaurus rex' });
    
    ok_commit("an occurrence was inserted") || return;
    
    my ($occ) = $edt->inserted_keys;
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'REID_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { });
    
    $edt->insert_record('REID_DATA', { occurrence_id => $occ, collection_id => $coll,
				       reference_id => 6, identified_name => 'Tyrannosaurus cf. rex' });
    $edt->insert_record('REID_DATA', { occurrence_id => $occ, collection_id => $coll,
				       reference_id => 7, identified_name => 'Tyrannosaurus aff. rex' });
    
    ok_commit("two reidentifications were inserted");
    
    my @reids = $edt->inserted_keys;
    
    is( scalar(@reids), 2, "two keys were returned" );
    
    my ($pubyr6, $pubyr7) = map { sql_fetchrow("SELECT pubyr FROM $TABLE{REFERENCE_DATA}
		WHERE reference_no = $_") } 6, 7;
    
    my ($latest) = $pubyr7 >= $pubyr6 ? $reids[1] : $reids[0];
    
    my ($occ_reid) = sql_fetchrow("SELECT reid_no FROM <<OCCURRENCE_DATA>>
		WHERE occurrence_no = $occ");
    
    is( $occ_reid, $latest, "the occurrence points to the most recent reidentification" );
    
    is( rows('REID_DATA', 'reid_no, most_recent', "occurrence_no = $occ"),
	join('; ', map { "$_," . ($_ == $latest ? 'YES' : 'NO') } @reids),
	"the most recent reidentification is marked" );
    
    is( rows('OCCURRENCE_MATRIX', 'reid_no, latest_ident', "occurrence_no = $occ"),
	join('; ', "0,0", map { "$_," . ($_ == $latest ? 1 : 0) } @reids),
	"the occurrence matrix has a row for each identification" );
    
    # The ages in the occurrence matrix rows come from the collection matrix.
    
    my ($early_age, $late_age) = sql_fetchrow("SELECT early_age, late_age FROM <<COLLECTION_MATRIX>>
		WHERE collection_no = '$coll'");
    
    my ($n_ages, $occ_early, $occ_late) = sql_fetchrow("SELECT count(distinct early_age, late_age),
		min(early_age), min(late_age) FROM <<OCCURRENCE_MATRIX>> WHERE occurrence_no = '$occ'");
    
    is( "$n_ages|$occ_early|$occ_late", "1|$early_age|$late_age",
	"every identification has the ages of the collection" );
    
    # An occurrence may not use the same reference as one of its reidentifications.
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { });
    
    $edt->update_record('OCCURRENCE_DATA', { occurrence_id => $occ, reference_id => 6 });
    
    ok_failed_commit("an occurrence cannot use the reference of one of its reidentifications");
    
    ok_has_one_condition(qr/E_BAD_VALUE.*'reference_id'/, "the conflict was reported for 'reference_id'");
    
    # Delete one of the reidentifications. The other becomes the current identification.
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'REID_DATA'));
    
    $edt->initialize_occs({ }, { $latest => 1 });
    
    $edt->delete_record('REID_DATA', { reid_id => $latest });
    
    ok_commit("a reidentification was deleted");
    
    my ($other) = grep { $_ != $latest } @reids;
    
    ($occ_reid) = sql_fetchrow("SELECT reid_no FROM <<OCCURRENCE_DATA>>
		WHERE occurrence_no = $occ");
    
    is( $occ_reid, $other, "the other reidentification is now the current one" );
    
    ok_no_record('OCCURRENCE_MATRIX', "reid_no = $latest");
    
    # A reidentification may be given with only its occurrence. Its collection is that of
    # the occurrence.
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'REID_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { });
    
    $edt->insert_record('REID_DATA', { occurrence_id => $occ, reference_id => 8,
				       identified_name => 'Tyrannosaurus sp.' });
    
    ok_commit("a reidentification was inserted without a collection_id");
    
    my ($no_coll_reid) = $edt->inserted_keys;
    
    ok_found_record('REID_DATA', "reid_no = '$no_coll_reid' and collection_no = '$coll'",
		    "the reidentification was stored in the collection of the occurrence");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('GUEST', 'REID_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { });
    
    $edt->insert_record('REID_DATA', { occurrence_id => $occ, reference_id => 9,
				       identified_name => 'Tyrannosaurus indet.' });
    
    ok_failed_commit("a guest cannot insert a reidentification");
    
    ok_has_condition('E_PERMISSION', "the refusal was reported");
};


# The specimen matrix has a row for each identification of the occurrence of each
# specimen. When a reidentification is inserted, updated, or deleted, the rows of the
# specimens of its occurrence follow. The specimen is written directly to the test tables,
# as SpecimenEdit would write it. [Added by Claude under the guidance of Michael
# McClennen.]

subtest 'specimens of reidentified occurrences' => sub {
    
    my $coll = new_collection();
    
    ok( $coll, "a collection was inserted" ) || return;
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'Tyrannosaurus sp.' });
    
    ok_commit("an occurrence was inserted") || return;
    
    my ($occ) = $edt->inserted_keys;
    
    # A specimen of the occurrence, which does not give its own taxon.
    
    sql_command("INSERT INTO <<SPECIMEN_DATA>> (occurrence_no, taxon_no, reference_no,
		authorizer_no, enterer_no, specimen_id) VALUES ($occ, NULL, 5, 39998, 39997, 'edtpc06-reid')");
    
    my ($spec) = sql_fetchrow("SELECT specimen_no FROM <<SPECIMEN_DATA>> WHERE specimen_id = 'edtpc06-reid'");
    
    updateSpecimenMatrix($T->dbh, $spec);
    
    is( rows('SPECIMEN_MATRIX', 'reid_no, latest_ident, taxon_no', "specimen_no = $spec"), "0,1,$GENUS_NO",
	"the specimen has one row, for the identification of its occurrence" );
    
    # Reidentify the occurrence.
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'REID_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { });
    
    $edt->insert_record('REID_DATA', { occurrence_id => $occ, collection_id => $coll,
				       reference_id => 6, identified_name => 'Tyrannosaurus rex' });
    
    ok_commit("a reidentification was inserted") || return;
    
    my ($reid) = $edt->inserted_keys;
    
    is( rows('SPECIMEN_MATRIX', 'reid_no, latest_ident, taxon_no', "specimen_no = $spec"),
	"0,0,$GENUS_NO; $reid,1,$SPECIES_NO",
	"the specimen has a row for the reidentification, which is the latest" );
    
    # Change the name of the reidentification.
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'REID_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { $reid => 1 });
    
    $edt->update_record('REID_DATA', { reid_id => $reid, identified_name => 'Tyrannosaurus indet.' });
    
    ok_commit("the reidentification was updated");
    
    is( rows('SPECIMEN_MATRIX', 'reid_no, latest_ident, taxon_no', "specimen_no = $spec"),
	"0,0,$GENUS_NO; $reid,1,$GENUS_NO",
	"the row for the reidentification has its new taxon" );
    
    # Delete the reidentification.
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'REID_DATA'));
    
    $edt->initialize_occs({ }, { $reid => 1 });
    
    $edt->delete_record('REID_DATA', { reid_id => $reid });
    
    ok_commit("the reidentification was deleted");
    
    is( rows('SPECIMEN_MATRIX', 'reid_no, latest_ident, taxon_no', "specimen_no = $spec"), "0,1,$GENUS_NO",
	"the row for the reidentification is gone, and the original identification is the latest again" );
    
    # Remove the specimen, so that it does not prevent the deletion of the occurrence
    # later.
    
    deleteFromSpecimenMatrix($T->dbh, $spec);
    
    sql_command("DELETE FROM <<SPECIMEN_DATA>> WHERE specimen_no = $spec");
};


# An occurrence or reidentification identified only to the genus has the species name
# 'sp.', and is a duplicate of another occurrence with the same name and 'sp.'. [Added by
# Claude under the guidance of Michael McClennen.]

subtest 'names without a species' => sub {
    
    my $coll = new_collection();
    
    ok( $coll, "a collection was inserted" ) || return;
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'cf. Tyrannosaurus' });
    
    ok_commit("an occurrence without a species was inserted") || return;
    
    my ($occ) = $edt->inserted_keys;
    
    is( rows('OCCURRENCE_DATA', 'genus_reso, genus_name, species_name, taxon_no', "occurrence_no = $occ"),
	"cf.,Tyrannosaurus,sp.,$GENUS_NO", "its species name is 'sp.'" );
    
    is( rows('OCCURRENCE_MATRIX', 'species_name', "occurrence_no = $occ"), "sp.",
	"and so it is in the occurrence matrix" );
    
    # The same name with 'sp.' duplicates it.
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { });
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'cf. Tyrannosaurus sp.' });
    
    ok_failed_commit("the same name with 'sp.' was rejected");
    
    ok_has_one_condition(qr/E_BAD_VALUE.*duplicates another occurrence/, "as a duplicate");
    
    # A reidentification without a species.
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'REID_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { });
    
    $edt->insert_record('REID_DATA', { occurrence_id => $occ, reference_id => 6,
				       identified_name => 'aff. Tyrannosaurus' });
    
    ok_commit("a reidentification without a species was inserted") || return;
    
    my ($reid) = $edt->inserted_keys;
    
    is( rows('REID_DATA', 'genus_reso, species_name, taxon_no', "reid_no = $reid"), "aff.,sp.,$GENUS_NO",
	"its species name is 'sp.'" );
};


# An occurrence marked as a new species links the collection to that species as its type
# locality, unless the species already has one. With '_set_locality', the existing type
# locality is replaced.

subtest 'type locality' => sub {
    
    my $coll = new_collection();
    
    ok( $coll, "a collection was inserted" ) || return;
    
    my ($real_locality) = sql_fetchrow("SELECT type_locality FROM <<AUTHORITY_DATA>>
		WHERE taxon_no = $SPECIES_NO");
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'Tyrannosaurus n. sp. rex' });
    
    ok_commit("an occurrence of a new species was inserted");
    
    ok_found_record('AUTHORITY_DATA', "taxon_no = $SPECIES_NO and type_locality = " .
		    ($real_locality || 0), "the existing type locality was kept");
    
    my $coll2 = new_collection();
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll2, reference_id => 5,
					     identified_name => 'Tyrannosaurus n. sp. rex',
					     _set_locality => 1 });
    
    ok_commit("an occurrence of a new species was inserted with '_set_locality'");
    
    ok_found_record('AUTHORITY_DATA', "taxon_no = $SPECIES_NO and type_locality = $coll2",
		    "the type locality was replaced");
    
    # Deleting the collection clears the type locality.
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->delete_record('COLLECTION_DATA', $coll2);
    
    ok_commit("the collection was deleted");
    
    ok_found_record('AUTHORITY_DATA', "taxon_no = $SPECIES_NO and type_locality is null",
		    "the type locality was cleared");
};


# A collection or occurrence cannot be deleted while it has specimens. Deleting an
# occurrence also deletes its reidentifications.

subtest 'delete occurrences' => sub {
    
    my $coll = new_collection();
    
    ok( $coll, "a collection was inserted" ) || return;
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'Tyrannosaurus rex' });
    
    ok_commit("an occurrence was inserted") || return;
    
    my ($occ) = $edt->inserted_keys;
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'REID_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { });
    
    $edt->insert_record('REID_DATA', { occurrence_id => $occ, collection_id => $coll,
				       reference_id => 6, identified_name => 'Tyrannosaurus cf. rex' });
    
    ok_commit("a reidentification was inserted");
    
    sql_command("INSERT INTO <<SPECIMEN_DATA>> (occurrence_no, specimen_id, reference_no)
		VALUES ($occ, 'edtpc-06', 5)");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { });
    
    $edt->delete_record('OCCURRENCE_DATA', { occurrence_id => $occ });
    
    ok_failed_commit("an occurrence with a specimen cannot be deleted");
    
    ok_has_one_condition('E_CANNOT_DELETE', "the refusal was reported");
    
    like( ($edt->conditions)[0], qr/associated specimens$/, "the message is spelled correctly" );
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->delete_record('COLLECTION_DATA', $coll);
    
    ok_failed_commit("a collection with a specimen cannot be deleted");
    
    ok_has_one_condition('E_CANNOT_DELETE', "the refusal was reported");
    
    clear_table('SPECIMEN_DATA');
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { });
    
    $edt->delete_record('OCCURRENCE_DATA', { occurrence_id => $occ, identified_name => '' });
    
    ok_commit("the occurrence was deleted, and the other fields of the record were not checked");
    
    ok_no_record('OCCURRENCE_DATA', "occurrence_no = $occ");
    ok_no_record('REID_DATA', "occurrence_no = $occ");
    ok_no_record('OCCURRENCE_MATRIX', "occurrence_no = $occ");
};


# Any authorized database member may update a collection, but only its owner may delete it.
# A guest may not insert one.

subtest 'permissions' => sub {
    
    my $coll = new_collection();
    
    ok( $coll, "a collection was inserted" ) || return;
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('OTHER'));
    
    $edt->update_record('COLLECTION_DATA', { collection_id => $coll,
					     collection_name => 'Renamed by other' });
    
    ok_commit("another member updated the collection");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('OTHER'));
    
    $edt->delete_record('COLLECTION_DATA', $coll);
    
    ok_failed_commit("another member cannot delete the collection");
    
    ok_has_one_condition('E_PERMISSION', "the refusal was reported");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('GUEST'));
    
    $edt->insert_record('COLLECTION_DATA', { %COLL, collection_name => 'Guest collection' });
    
    ok_failed_commit("a guest cannot insert a collection");
    
    ok_has_condition('E_PERMISSION', "the refusal was reported");
    
    ok_no_record('COLLECTION_DATA', "collection_name = 'Guest collection'");
};


# Deleting a collection deletes its occurrences, reidentifications, secondary references,
# and matrix rows. Each deletion is logged with a statement that would put the rows back.

subtest 'delete collection' => sub {
    
    my $coll = new_collection(reference_add => [6]);
    
    ok( $coll, "a collection was inserted" ) || return;
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'Tyrannosaurus rex' });
    
    ok_commit("an occurrence was inserted");
    
    my $logdir = tempdir(CLEANUP => 1);
    my $logfile = "$logdir/edtpc-06.log";
    
    my $save_filename = CollectionEdit->log_filename;
    
    CollectionEdit->log_filename($logfile);
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->delete_record('COLLECTION_DATA', "col:$coll");
    
    ok_commit("the collection was deleted");
    
    CollectionEdit->log_filename($save_filename);
    
    foreach my $table ( 'COLLECTION_DATA', 'COLLECTION_MATRIX', 'COLLECTION_REFS',
			'OCCURRENCE_DATA', 'OCCURRENCE_MATRIX' )
    {
	ok_no_record($table, "collection_no = $coll");
    }
    
    my $log = read_log($logfile);
    
    like( $log, qr/^# .* [|] DELETE [|] \S*occurrences [|] collection_no in [(]'$coll'[)].*\n
		   ^\# \s REPLACE \s INTO \s \S*occurrences \s/mx,
	  "the deletion of the occurrences was logged with the deleted rows" );
    
    like( $log, qr/^# .* [|] DELETE [|] \S*secondary_refs [|] collection_no in [(]'$coll'[)].*\n
		   ^\# \s REPLACE \s INTO \s \S*secondary_refs \s .* '5' .* '6'/mx,
	  "the deletion of the secondary references was logged with the deleted rows" );
};


# Clean up the authority records, so that no type locality points to a test collection.

# [Added by Claude under the guidance of Michael McClennen: the following five subtests.]

# The minutes and seconds of the coordinates must be between 0 and 59, and the ages and the
# altitude must be numbers of the proper form.

subtest 'collection field checks' => sub {
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('COLLECTION_DATA', { %COLL, latmin => 30, latsec => 75,
					     lngdec => undef, lngmin => 75, lngsec => 75 });
    
    ok_failed_commit("a collection with bad minutes and seconds was rejected");
    
    my @conds = $edt->conditions;
    
    foreach my $field ( qw(latsec lngmin lngsec) )
    {
	is( scalar(grep { /E_BAD_VALUE.*'$field'.*0-59/ } @conds), 1, "the bad '$field' was reported" );
    }
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('COLLECTION_DATA', { %COLL, max_ma => 'abc', max_ma_error => '123456789',
					     min_ma => '1234567890', min_ma_error => 'x',
					     altitude_value => 'high' });
    
    ok_failed_commit("a collection with bad ages and altitude was rejected");
    
    @conds = $edt->conditions;
    
    # Each is reported once. The column checks of EditTransaction would report some of them
    # again, but they skip a field that already has an error.
    
    foreach my $f ( [ E_FORMAT => 'max_ma' ], [ E_WIDTH => 'max_ma_error' ],
		    [ E_WIDTH => 'min_ma' ], [ E_FORMAT => 'min_ma_error' ],
		    [ E_FORMAT => 'altitude_value' ] )
    {
	my ($code, $field) = @$f;
	
	is( scalar(grep { /'$field'/ } @conds), 1, "the bad '$field' was reported once" );
	is( scalar(grep { /^$code.*'$field'/ } @conds), 1, "with $code" );
    }
};


# The coordinates are computed from degrees, minutes, and seconds, or from degrees and
# decimal digits. The precision is that of the most precise value given.

subtest 'coordinates' => sub {
    
    my $c1 = new_collection(latmin => 30, latsec => 36, lngdec => undef, lngmin => 15, lngsec => 18);
    
    ok( $c1, "a collection with minutes and seconds was inserted" ) || return;
    
    is( rows('COLLECTION_DATA', 'round(lat, 4), round(lng, 4), latlng_precision', "collection_no = $c1"),
	"45.5100,-100.2550,seconds", "the coordinates were computed from the seconds" );
    
    my $c2 = new_collection(latmin => undef, latdec => '123', lngdec => '45');
    
    ok( $c2, "a collection with decimal digits was inserted" ) || return;
    
    is( rows('COLLECTION_DATA', 'round(lat, 4), round(lng, 4), latlng_precision', "collection_no = $c2"),
	"45.1230,-100.4500,3", "the precision is the larger number of digits" );
    
    my $c3 = new_collection(latmin => undef, lngdec => undef);
    
    ok( $c3, "a collection with whole degrees was inserted" ) || return;
    
    is( rows('COLLECTION_DATA', 'round(lat, 4), round(lng, 4), latlng_precision', "collection_no = $c3"),
	"45.0000,-100.0000,degrees", "the precision is 'degrees'" );
};


# The fields 'fossilsfrom1' and 'fossilsfrom2' are stored as 'Y' or empty. A collection that
# is replaced keeps its creation date as the release date if it is public.

subtest 'geology and replace' => sub {
    
    my $coll = new_collection(fossilsfrom1 => 1, fossilsfrom2 => 0);
    
    ok( $coll, "a collection was inserted" ) || return;
    
    is( rows('COLLECTION_DATA', "fossilsfrom1, fossilsfrom2", "collection_no = $coll"), "Y,NULL",
	"the fossilsfrom fields were stored as 'Y' and empty" );
    
    sql_command("UPDATE <<COLLECTION_DATA>> SET created = '2001-02-03 04:05:06'
		WHERE collection_no = $coll");
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER'));
    
    $edt->replace_record('COLLECTION_DATA', { %COLL, collection_id => $coll,
					      collection_name => 'Replaced collection' });
    
    ok_commit("the collection was replaced");
    
    is( rows('COLLECTION_DATA', "collection_name, release_date", "collection_no = $coll"),
	"Replaced collection,2001-02-03 04:05:06",
	"the release date of the public collection is its creation date" );
};


# The abundance unit is ignored without a value, two plant organs are stored in separate
# fields and more are refused, a badly formed name is refused, and a taxon_id given with a
# validation hash that matches the name is accepted as it is.

subtest 'occurrence fields and validation' => sub {
    
    my $coll = new_collection();
    
    ok( $coll, "a collection was inserted" ) || return;
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { _label => 'a', collection_id => $coll, reference_id => 5,
					     identified_name => 'Tyrannosaurus rex',
					     abund_unit => 'specimens', plant_organ => 'leaf, wood' });
    
    ok_commit("an occurrence with an abundance unit and two plant organs was inserted") || return;
    
    ok_has_condition(qr/W_DISCARDED.*abund_unit/, "the unit without a value was discarded");
    
    my ($occ) = $edt->inserted_keys;
    
    is( rows('OCCURRENCE_DATA', "coalesce(abund_unit, ''), plant_organ, plant_organ2",
	     "occurrence_no = $occ"), ",leaf,wood",
	"the unit without a value was ignored, and the organs were stored separately" );
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { });
    
    $edt->update_record('OCCURRENCE_DATA', { occurrence_id => $occ, plant_organ => 'root' });
    
    ok_commit("the plant organ was updated");
    
    is( rows('OCCURRENCE_DATA', "plant_organ, coalesce(plant_organ2, 'NONE')", "occurrence_no = $occ"),
	"root,NONE", "one organ clears the second field" );
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { });
    
    $edt->update_record('OCCURRENCE_DATA', { occurrence_id => $occ,
					     plant_organ => 'leaf, wood, root' });
    
    ok_failed_commit("three plant organs were refused");
    
    ok_has_one_condition(qr/E_BAD_VALUE.*'plant_organ'.*two/, "the error was reported once");
    
    # An empty plant organ clears both fields.
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { });
    
    $edt->update_record('OCCURRENCE_DATA', { occurrence_id => $occ, plant_organ => 'leaf, wood' });
    
    ok_commit("two plant organs were stored again");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->initialize_occs({ $occ => 1 }, { });
    
    $edt->update_record('OCCURRENCE_DATA', { occurrence_id => $occ, plant_organ => '' });
    
    ok_commit("the plant organ was updated with an empty value");
    
    is( rows('OCCURRENCE_DATA', "coalesce(plant_organ, 'NONE'), coalesce(plant_organ2, 'NONE')",
	     "occurrence_no = $occ"), "NONE,NONE", "both plant organ fields were cleared" );
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'Tyrannosaurus cf.' });
    
    ok_failed_commit("a badly formed name was refused");
    
    ok_has_one_condition(qr/E_FORMAT.*'identified_name'/, "the error was reported");
    
    # A taxon_id with a validation hash computed from the name and that taxon_id is used as
    # it is. Without the hash, the name is matched instead.
    
    my $hash = OccurrenceBase::hashIdentification("Tyrannosaurus||rex|", $GENUS_NO);
    
    my $coll2 = new_collection();
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll2, reference_id => 5,
					     identified_name => 'Tyrannosaurus rex',
					     taxon_id => $GENUS_NO, validation => $hash });
    
    ok_commit("an occurrence with taxon_id and validation was inserted");
    
    is( rows('OCCURRENCE_DATA', "species_reso, taxon_no", "collection_no = $coll2"),
	"NULL,$GENUS_NO", "the validated taxon_id was kept" );
    
    # A taxon_id with a validation hash that does not match must match the name itself.
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll2, reference_id => 6,
					     identified_name => 'Tyrannosaurus aff. rex',
					     taxon_id => $GENUS_NO, validation => 'wrong' });
    
    ok_failed_commit("a taxon_id that does not match the name was refused");
    
    ok_has_one_condition(qr/E_BAD_VALUE.*'taxon_id'.*does not match/, "the error was reported");
    
    # An update that gives only a taxon_id is checked against the stored name.
    
    my ($first) = sql_fetchrow("SELECT occurrence_no FROM <<OCCURRENCE_DATA>>
		WHERE collection_no = $coll2 and species_reso is null");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->initialize_occs({ $first => 1 }, { });
    
    $edt->update_record('OCCURRENCE_DATA', { occurrence_id => $first, taxon_id => $SPECIES_NO });
    
    ok_commit("an occurrence was updated with only a taxon_id");
    
    is( rows('OCCURRENCE_DATA', "taxon_no", "occurrence_no = $first"), $SPECIES_NO,
	"the taxon_id that matches the stored name was stored" );
};


# Type localities are also set for new genera, subgenera, and subspecies, and are removed
# when the occurrence that set them is renamed or deleted. The taxa for the subgenus and
# subspecies are copied from the real table.

subtest 'type localities at other ranks' => sub {
    
    my $real_auth = original_table_name($TABLE{AUTHORITY_DATA});
    
    sql_command("INSERT IGNORE INTO <<AUTHORITY_DATA>> SELECT * FROM $real_auth
		WHERE taxon_no in (59111, 59110, 10760, 5291, 110926)");
    
    sql_command("UPDATE <<AUTHORITY_DATA>> SET type_locality = null
		WHERE taxon_no in ($GENUS_NO, $SPECIES_NO, 59111, 5291)");
    
    my $coll = new_collection();
    
    ok( $coll, "a collection was inserted" ) || return;
    
    my $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'n. gen. Tyrannosaurus n. sp. rex' });
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'Ancilla staminea n. ssp. punctulifera' });
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
		identified_name => 'Cystiphylloides n. subgen. (Cladionophyllum) n. sp. testus' });
    
    ok_commit("occurrences of a new genus, subspecies, and subgenus were inserted") || return;
    
    my ($gen_occ, $ssp_occ, $sg_occ) = $edt->inserted_keys;
    
    is( rows('AUTHORITY_DATA', 'taxon_no, coalesce(type_locality, 0)',
	     "taxon_no in ($GENUS_NO, $SPECIES_NO, 59111, 5291)"),
	"5291,$coll; $GENUS_NO,$coll; $SPECIES_NO,$coll; 59111,$coll",
	"the type locality was set for the genus, species, subspecies, and subgenus" );
    
    # Renaming the first occurrence removes its type localities, and deleting the others
    # removes theirs.
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->initialize_occs({ $gen_occ => 1, $ssp_occ => 1, $sg_occ => 1 }, { });
    
    $edt->update_record('OCCURRENCE_DATA', { occurrence_id => $gen_occ,
					     identified_name => 'Tyrannosaurus rex' });
    $edt->delete_record('OCCURRENCE_DATA', { occurrence_id => $ssp_occ });
    $edt->delete_record('OCCURRENCE_DATA', { occurrence_id => $sg_occ });
    
    ok_commit("one occurrence was renamed and two were deleted");
    
    is( rows('AUTHORITY_DATA', 'taxon_no, coalesce(type_locality, 0)',
	     "taxon_no in ($GENUS_NO, $SPECIES_NO, 59111, 5291)"),
	"5291,0; $GENUS_NO,0; $SPECIES_NO,0; 59111,0", "the type localities were removed" );
    
    # An update that gives only '_set_locality' makes this collection the type locality of
    # the occurrence's new taxon, replacing one that was already set.
    
    my $other = new_collection();
    
    sql_command("UPDATE <<AUTHORITY_DATA>> SET type_locality = $other WHERE taxon_no = $SPECIES_NO");
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->insert_record('OCCURRENCE_DATA', { collection_id => $coll, reference_id => 5,
					     identified_name => 'Tyrannosaurus n. sp. rex' });
    
    ok_commit("an occurrence of a new species was inserted") || return;
    
    my ($sp_occ) = $edt->inserted_keys;
    
    is( rows('AUTHORITY_DATA', 'type_locality', "taxon_no = $SPECIES_NO"), $other,
	"the existing type locality was kept" );
    
    $edt = ok_new_edt(class => 'CollectionEdit', permission => perm('ENTERER', 'OCCURRENCE_DATA'));
    
    $edt->initialize_occs({ $sp_occ => 1 }, { });
    
    $edt->update_record('OCCURRENCE_DATA', { occurrence_id => $sp_occ, _set_locality => 1 });
    
    ok_commit("the occurrence was updated with only '_set_locality'");
    
    is( rows('AUTHORITY_DATA', 'type_locality', "taxon_no = $SPECIES_NO"), $coll,
	"the type locality was replaced" );
    
    sql_command("DELETE FROM <<AUTHORITY_DATA>> WHERE taxon_no in (59111, 59110, 10760, 5291, 110926)");
};


subtest 'cleanup' => sub {
    
    clear_table('AUTHORITY_DATA');
    
    ok_no_record('AUTHORITY_DATA');
};
