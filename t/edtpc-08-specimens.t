#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the EditTransaction client modules, the subclasses of
# EditTransaction that handle edits to particular kinds of Paleobiology Database records.
#
# edtpc-08-specimens.t : Test SpecimenEdit, which handles fossil specimens and their
#                        measurements: taxonomic identification, the occurrences that are
#                        created for specimens in a collection, the specimen and occurrence
#                        matrices, measurements that follow their specimen, updates,
#                        deletion, permissions, and rollback.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 14;

use TableDefs qw(%TABLE $TEST_DB original_table_name);

use ExternalIdent;

use EDTPCTest;
use PBDBTester;
use EditTester qw(ok_new_edt ok_commit ok_failed_commit ok_has_one_condition ok_has_condition
		  clear_table sql_command sql_fetchrow ok_found_record ok_no_record);


# Establish the tester and select the test tables.

my $T = PBDBTester->new('SpecimenEdit');

EDTPCTest->select_test_tables('SpecimenEdit');

my $dbh = $T->dbh;

sub perm {
    
    my ($name, $table) = @_;
    
    return $T->new_perm("SESSION-$name", $table || 'SPECIMEN_DATA');
}


# The taxa used by these tests, copied from the real authorities table, and the test
# collection.

my $GENUS_NO = 38613;		# Tyrannosaurus
my $SPECIES_NO = 54833;		# Tyrannosaurus rex
my $SUBSPECIES_NO = 47920;	# Martes caurina nobilis
my $FAMILY_NO = 38606;		# Tyrannosauridae
my $COLL = 100;
my $REF = 5;


# Return the rows of a table that match an expression, as a string.

sub rows {
    
    my ($table, $columns, $expr) = @_;
    
    my $rows = $dbh->selectall_arrayref("SELECT $columns FROM $TABLE{$table}
		WHERE $expr ORDER BY $columns");
    
    return join('; ', map { join(',', map { $_ // 'NULL' } @$_) } @$rows);
}


# Insert one or more specimen records in a single transaction, and return the new keys.

sub insert_specimens {
    
    my (@records) = @_;
    
    my $edt = SpecimenEdit->new($dbh, { permission => perm('ENTERER') });
    
    $edt->insert_record('SPECIMEN_DATA', $_) foreach @records;
    $edt->commit;
    
    return $edt->inserted_keys;
}


# Start with empty tables, plus the two taxa and a collection with a row in the collection
# matrix.

subtest 'setup' => sub {
    
    clear_table($_) foreach EDTPCTest->module_tables('SpecimenEdit');
    
    my $real_auth = original_table_name($TABLE{AUTHORITY_DATA});
    
    sql_command("INSERT INTO <<AUTHORITY_DATA>> SELECT * FROM $real_auth
		WHERE taxon_no in ($GENUS_NO, $SPECIES_NO, $SUBSPECIES_NO, $FAMILY_NO)");
    
    sql_command("INSERT INTO <<COLLECTION_DATA>> (collection_no, authorizer_no, enterer_no,
			collection_name, reference_no)
		VALUES ($COLL, 39998, 39997, 'edtpc-08 collection', $REF)");
    
    sql_command("INSERT INTO <<COLLECTION_MATRIX>> (collection_no, reference_no, loc,
			early_int_no, late_int_no, access_level)
		VALUES ($COLL, $REF, POINT(0, 0), 0, 0, 0)");
    
    is( rows('AUTHORITY_DATA', 'taxon_no', '1'), "$FAMILY_NO; $GENUS_NO; $SUBSPECIES_NO; $SPECIES_NO",
	"the taxa are in place" );
};


# A specimen identified only by taxon_id is inserted with no occurrence, and gets a row in
# the specimen matrix.

subtest 'specimen without a collection' => sub {
    
    my $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'S-1', taxon_id => $SPECIES_NO,
					   reference_id => $REF });
    
    ok_commit("a specimen identified by taxon_id was inserted") || return;
    
    my ($key) = $edt->inserted_keys;
    
    is( rows('SPECIMEN_DATA', 'taxon_no, ifnull(occurrence_no, 0), enterer_no', "specimen_no = $key"),
	"$SPECIES_NO,0,39997", "the specimen was stored without an occurrence" );
    
    is( rows('SPECIMEN_MATRIX', 'occurrence_no, reid_no, taxon_no, orig_no', "specimen_no = $key"),
	"0,0,$SPECIES_NO,$SPECIES_NO", "the specimen matrix has a row for it" );
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'S-2', taxon_name => 'Tyrannosaurus rex',
					   reference_id => $REF });
    
    ok_commit("a specimen identified by a known taxon name was inserted") || return;
    
    ($key) = $edt->inserted_keys;
    
    ok_found_record('SPECIMEN_DATA', "specimen_no = $key and taxon_no = $SPECIES_NO",
		    "the taxon name was resolved");
};


# A specimen must have a taxonomic identification, and the identifying fields may not be
# combined.

subtest 'identification' => sub {
    
    my $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'S-x', reference_id => $REF });
    
    ok_failed_commit("a specimen with no identification was rejected");
    
    ok_has_one_condition('E_MISSING_IDENTIFICATION', "the missing identification was reported");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'S-x', taxon_name => 'Nosuchtaxon foo',
					   reference_id => $REF });
    
    ok_failed_commit("a specimen with an unknown taxon name and no collection was rejected");
    
    ok_has_one_condition('E_UNREGISTERED_TAXON', "the unknown taxon was reported");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'S-x', taxon_id => $SPECIES_NO,
					   occurrence_id => 1, reference_id => $REF });
    
    ok_failed_commit("a specimen with both taxon_id and occurrence_id was rejected");
    
    ok_has_one_condition(qr/^E_PARAM .*Field 'occurrence_id': you may not specify both/,
			 "the conflict was reported with the field name and the message");
    
    ok_no_record('SPECIMEN_DATA', "specimen_id = 'S-x'");
};


# A specimen in a collection belongs to an occurrence of its taxon in that collection. If
# there is none, one is created, with the name of the taxon, and added to the occurrence
# matrix. Each further specimen of the same taxon adds one to its abundance.

subtest 'occurrences' => sub {
    
    my ($key) = insert_specimens({ specimen_code => 'S-3', taxon_id => $SPECIES_NO,
				   collection_id => $COLL, reference_id => PBDB::ExtIdent->new('ref', $REF) });
    
    ok( $key, "a specimen in a collection was inserted" ) || return;
    
    my ($occ) = sql_fetchrow("SELECT occurrence_no FROM <<SPECIMEN_DATA>> WHERE specimen_no = $key");
    
    ok( $occ, "the specimen was linked to an occurrence" ) || return;
    
    is( rows('OCCURRENCE_DATA', 'collection_no, taxon_no, genus_name, species_name, reference_no, abund_value, abund_unit',
	     "occurrence_no = $occ"),
	"$COLL,$SPECIES_NO,Tyrannosaurus,rex,$REF,1,specimens",
	"the occurrence has the name of the taxon and the reference" );
    
    ok_found_record('OCCURRENCE_MATRIX', "occurrence_no = $occ and taxon_no = $SPECIES_NO",
		    "the occurrence was added to the occurrence matrix");
    
    ok_found_record('SPECIMEN_MATRIX', "specimen_no = $key and occurrence_no = $occ",
		    "the specimen matrix links the specimen to the occurrence");
    
    my ($n_occs) = sql_fetchrow("SELECT n_occs FROM <<COLLECTION_MATRIX>> WHERE collection_no = $COLL");
    
    is( $n_occs, 1, "the occurrence count of the collection was updated" );
    
    # Give the occurrence a second identification in the occurrence matrix, which becomes
    # the latest one. A specimen of the occurrence gets a row in the specimen matrix for each
    # identification, because it may be labeled with any of them.
    
    sql_command("CREATE TEMPORARY TABLE $TEST_DB.edtpc08_reid SELECT * FROM <<OCCURRENCE_MATRIX>>
		WHERE occurrence_no = $occ");
    sql_command("UPDATE $TEST_DB.edtpc08_reid SET reid_no = 9001");
    sql_command("INSERT INTO <<OCCURRENCE_MATRIX>> SELECT * FROM $TEST_DB.edtpc08_reid");
    sql_command("UPDATE <<OCCURRENCE_MATRIX>> SET latest_ident = (reid_no = 9001)
		WHERE occurrence_no = $occ");
    sql_command("DROP TEMPORARY TABLE $TEST_DB.edtpc08_reid");
    
    my ($key2) = insert_specimens({ specimen_code => 'S-4', taxon_id => $SPECIES_NO,
				    collection_id => PBDB::ExtIdent->new('col', $COLL), reference_id => $REF });
    
    ok_found_record('SPECIMEN_DATA', "specimen_no = $key2 and occurrence_no = $occ",
		    "a second specimen of the taxon uses the same occurrence");
    
    ok_found_record('OCCURRENCE_DATA', "occurrence_no = $occ and abund_value = '2'",
		    "the abundance was increased");
    
    is( rows('SPECIMEN_MATRIX', 'reid_no, latest_ident', "specimen_no = $key2"), "0,0; 9001,1",
	"the specimen matrix has a row for each identification of the occurrence" );
    
    # Two specimens of a taxon with no occurrence yet, in the same transaction, share one new
    # occurrence.
    
    my @keys = insert_specimens({ specimen_code => 'S-5', taxon_id => $GENUS_NO,
				  collection_id => $COLL, reference_id => $REF },
				{ specimen_code => 'S-6', taxon_id => $GENUS_NO,
				  collection_id => $COLL, reference_id => $REF });
    
    is( scalar(@keys), 2, "two specimens of the genus were inserted" ) || return;
    
    my ($occ_count, $genus_occ) = sql_fetchrow("SELECT count(distinct occurrence_no), max(occurrence_no)
		FROM <<SPECIMEN_DATA>> WHERE specimen_no in ($keys[0], $keys[1])");
    
    is( $occ_count, 1, "they share one occurrence" );
    
    is( rows('OCCURRENCE_DATA', 'genus_name, species_name, abund_value', "occurrence_no = $genus_occ"),
	"Tyrannosaurus,sp.,2", "the genus occurrence has the species name 'sp.' and a count of 2" );
    
    # A specimen with an unknown taxon name may be added to a collection with the
    # UNKNOWN_TAXON allowance. The occurrence has the name, with no taxon.
    
    my $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'S-7', taxon_name => 'Nosuchtaxon foo',
					   collection_id => $COLL, reference_id => $REF });
    
    ok_failed_commit("an unknown taxon name needs the UNKNOWN_TAXON allowance");
    
    ok_has_one_condition('C_UNKNOWN_TAXON', "the caution was reported");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'), 'UNKNOWN_TAXON');
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'S-7', taxon_name => 'Nosuchtaxon foo',
					   collection_id => $COLL, reference_id => $REF });
    
    ok_commit("the specimen was inserted with the UNKNOWN_TAXON allowance");
    
    ok_found_record('OCCURRENCE_DATA', "genus_name = 'Nosuchtaxon' and species_name = 'foo'
					and taxon_no = 0", "the occurrence has the name");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'S-x', taxon_id => $SPECIES_NO,
					   collection_id => 99999, reference_id => $REF });
    
    ok_failed_commit("a specimen in an unknown collection was rejected");
    
    ok_has_one_condition('E_KEY_NOT_FOUND', "the unknown collection was reported");
};


# The occurrence of a specimen in a collection has all of the name components and modifiers
# of the taxon, including the subspecies. A taxon_name is parsed with parseIdentifiedName.

subtest 'names' => sub {
    
    my @keys = insert_specimens({ specimen_code => 'N-1', taxon_id => $SUBSPECIES_NO,
				  collection_id => $COLL, reference_id => $REF },
				{ specimen_code => 'N-2', taxon_name => 'Martes caurina nobilis',
				  collection_id => $COLL, reference_id => $REF });
    
    is( scalar(@keys), 2, "two specimens of a subspecies were inserted" ) || return;
    
    my ($occ_count, $occ) = sql_fetchrow("SELECT count(distinct occurrence_no), max(occurrence_no)
		FROM <<SPECIMEN_DATA>> WHERE specimen_no in ($keys[0], $keys[1])");
    
    is( $occ_count, 1, "the subspecies given by taxon_id and by name share one occurrence" );
    
    is( rows('OCCURRENCE_DATA', 'genus_name, species_name, subspecies_name, taxon_no, abund_value',
	     "occurrence_no = $occ"),
	"Martes,caurina,nobilis,$SUBSPECIES_NO,2", "the occurrence has the subspecies" );
    
    ok_found_record('OCCURRENCE_MATRIX', "occurrence_no = $occ and subspecies_name = 'nobilis'",
		    "the occurrence matrix has the subspecies");
    
    # A taxon_name is matched to a taxon with matchIdentifiedName, as in CollectionEdit, so a
    # name with a modifier is matched to the taxon without it. In a collection, the occurrence
    # has the name as given, with its modifier, and the taxon_no of the matching taxon.
    
    my ($cf_key) = insert_specimens({ specimen_code => 'N-cf', taxon_name => 'Tyrannosaurus cf. rex',
				      collection_id => $COLL, reference_id => $REF });
    
    ok( $cf_key, "a specimen of 'Tyrannosaurus cf. rex' was inserted without UNKNOWN_TAXON" ) || return;
    
    my ($cf_taxon, $cf_occ) = sql_fetchrow("SELECT taxon_no, occurrence_no FROM <<SPECIMEN_DATA>>
		WHERE specimen_no = $cf_key");
    
    is( $cf_taxon, $SPECIES_NO, "the name was matched to Tyrannosaurus rex" );
    
    is( rows('OCCURRENCE_DATA', 'genus_name, species_reso, species_name, taxon_no', "occurrence_no = $cf_occ"),
	"Tyrannosaurus,cf.,rex,$SPECIES_NO", "the occurrence has the modifier and the taxon" );
    
    ok_no_record('SPECIMEN_DATA', "specimen_id = 'S-3' and occurrence_no = $cf_occ",
		 "it is a different occurrence from that of Tyrannosaurus rex");
    
    my ($sp_key) = insert_specimens({ specimen_code => 'N-sp', taxon_name => 'Tyrannosaurus sp.',
				      reference_id => $REF });
    
    ok_found_record('SPECIMEN_DATA', "specimen_no = '$sp_key' and taxon_no = $GENUS_NO",
		    "'Tyrannosaurus sp.' was matched to the genus");
    
    my ($cf2_key) = insert_specimens({ specimen_code => 'N-cf2', taxon_name => 'Tyrannosaurus cf. rex',
				       reference_id => $REF });
    
    ok_found_record('SPECIMEN_DATA', "specimen_no = '$cf2_key' and taxon_no = $SPECIES_NO",
		    "a specimen with a modifier and no collection was matched to the taxon");
    
    # An occurrence of a taxon above the genus level has the species name 'indet.', and one at
    # the genus level has 'sp.'. A taxon_name without a species gets the same occurrence as the
    # taxon_id of the matching taxon.
    
    my ($fam_key) = insert_specimens({ specimen_code => 'N-fam', taxon_id => $FAMILY_NO,
				       collection_id => $COLL, reference_id => $REF });
    
    my ($fam_occ) = sql_fetchrow("SELECT occurrence_no FROM <<SPECIMEN_DATA>> WHERE specimen_no = $fam_key");
    
    is( rows('OCCURRENCE_DATA', 'genus_name, species_name, taxon_no', "occurrence_no = $fam_occ"),
	"Tyrannosauridae,indet.,$FAMILY_NO", "a family-level occurrence has the species name 'indet.'" );
    
    my ($fam_key2) = insert_specimens({ specimen_code => 'N-fam2', taxon_name => 'Tyrannosauridae',
					collection_id => $COLL, reference_id => $REF });
    
    ok_found_record('SPECIMEN_DATA', "specimen_no = '$fam_key2' and occurrence_no = $fam_occ",
		    "the family name given as taxon_name uses the same occurrence");
    
    my ($gen_occ) = sql_fetchrow("SELECT occurrence_no FROM <<OCCURRENCE_DATA>>
		WHERE genus_name = 'Tyrannosaurus' and species_name = 'sp.'");
    
    my ($gen_key) = insert_specimens({ specimen_code => 'N-gen', taxon_name => 'Tyrannosaurus',
				       collection_id => $COLL, reference_id => $REF });
    
    ok_found_record('SPECIMEN_DATA', "specimen_no = '$gen_key' and occurrence_no = '$gen_occ'",
		    "the genus name given as taxon_name uses the 'Tyrannosaurus sp.' occurrence");
    
    my $edt0 = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'), 'UNKNOWN_TAXON');
    
    $edt0->insert_record('SPECIMEN_DATA', { specimen_code => 'N-unk', taxon_name => 'Nosuchgenus',
					    collection_id => $COLL, reference_id => $REF });
    
    ok_commit("a specimen of an unknown one-word name was inserted");
    
    ok_found_record('OCCURRENCE_DATA', "genus_name = 'Nosuchgenus' and species_name = 'sp.'",
		    "an unknown one-word name is taken to be a genus");
    
    # Names that are not in the database need the UNKNOWN_TAXON allowance. Modifiers are kept,
    # and a name with a modifier is a different occurrence from the same name without one.
    
    my %occ_of;
    
    foreach my $name ( 'Nosuchtaxon foo bar', 'Nosuchtaxon cf. foo', 'Nosuchtaxon foo' )
    {
	my $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'), 'UNKNOWN_TAXON');
	
	$edt->insert_record('SPECIMEN_DATA', { specimen_code => "N-$name", taxon_name => $name,
					       collection_id => $COLL, reference_id => $REF });
	
	ok_commit("a specimen of '$name' was inserted") || next;
	
	my ($key) = $edt->inserted_keys;
	
	($occ_of{$name}) = sql_fetchrow("SELECT occurrence_no FROM <<SPECIMEN_DATA>>
		WHERE specimen_no = $key");
    }
    
    is( rows('OCCURRENCE_DATA', 'genus_name, species_reso, species_name, subspecies_name',
	     "occurrence_no in (" . join(',', map { $_ || 0 } values %occ_of) . ")"),
	"Nosuchtaxon,,foo,; Nosuchtaxon,,foo,bar; Nosuchtaxon,cf.,foo,",
	"each name has its own occurrence, with its modifier and subspecies" );
    
    my $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'), 'UNKNOWN_TAXON');
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'N-x', taxon_name => 'nosuchtaxon foo',
					   collection_id => $COLL, reference_id => $REF });
    
    ok_failed_commit("a badly formed name was rejected");
    
    ok_has_one_condition(qr/^E_FORMAT .*Field 'taxon_name': Bad capitalization/,
			 "the message from parseIdentifiedName was reported");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'), 'UNKNOWN_TAXON');
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'N-x', taxon_name => 'Nosuchtaxon foo bar baz',
					   collection_id => $COLL, reference_id => $REF });
    
    ok_failed_commit("a name with an extra word was rejected");
    
    ok_has_one_condition(qr/^E_FORMAT .*Unexpected text after the subspecies name: 'baz'/,
			 "the extra word was reported");
};


# A measurement may follow its specimen in the same transaction, without a specimen_id. Anyone
# who can edit measurements can also delete them.

subtest 'measurements' => sub {
    
    my $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'S-8', taxon_id => $SPECIES_NO,
					   reference_id => $REF });
    $edt->insert_record('MEASUREMENT_DATA', { measurement_type => 'length', average => '12.5 mm' });
    $edt->insert_record('MEASUREMENT_DATA', { measurement_type => 'width', min => '3', max => '4.5' });
    
    ok_commit("a specimen and two measurements were inserted") || return;
    
    my ($specimen) = $edt->inserted_keys('SPECIMEN_DATA');
    
    is( rows('MEASUREMENT_DATA', 'measurement_type, specimen_no, real_average, real_min, real_max',
	     "specimen_no = $specimen"),
	"length,$specimen,12.5,NULL,NULL; width,$specimen,NULL,3,4.5",
	"the measurements belong to the new specimen and have numeric values" );
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->update_record('SPECIMEN_DATA', { specimen_id => $specimen, comments => 'updated' });
    $edt->insert_record('MEASUREMENT_DATA', { measurement_type => 'height', average => '7' });
    
    ok_commit("a measurement following a specimen update was inserted");
    
    ok_found_record('MEASUREMENT_DATA', "specimen_no = $specimen and measurement_type = 'height'",
		    "the measurement belongs to the updated specimen");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('MEASUREMENT_DATA', { measurement_type => 'length', average => '1' });
    
    ok_failed_commit("a measurement with no specimen was rejected");
    
    ok_has_condition(qr/^E_REQUIRED .*Field 'specimen_id': a value is required/,
		     "the missing specimen_id was reported");
    
    # Add three measurements to delete.
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER', 'MEASUREMENT_DATA'));
    
    $edt->insert_record('MEASUREMENT_DATA', { specimen_id => $specimen, measurement_type => $_,
					       average => '1' }) foreach qw(circumference diagonal diameter);
    
    ok_commit("three more measurements were inserted") || return;
    
    my @m = $edt->inserted_keys('MEASUREMENT_DATA');
    
    foreach my $t ( [ 'ENTERER', $m[0], 'the owner' ], [ 'OTHER', $m[1], 'another member' ] )
    {
	my ($who, $m, $label) = @$t;
	
	$edt = ok_new_edt(class => 'SpecimenEdit', permission => perm($who, 'MEASUREMENT_DATA'));
	
	$edt->update_record('MEASUREMENT_DATA', { measurement_id => $m, average => '2' });
	$edt->delete_record('MEASUREMENT_DATA', $m);
	
	ok_commit("$label can edit and delete a measurement");
	
	ok_no_record('MEASUREMENT_DATA', "measurement_no = $m");
    }
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('GUEST', 'MEASUREMENT_DATA'));
    
    $edt->delete_record('MEASUREMENT_DATA', $m[2]);
    
    ok_failed_commit("a guest cannot delete a measurement");
    
    ok_has_condition('E_PERMISSION', "the refusal was reported");
    
    ok_found_record('MEASUREMENT_DATA', "measurement_no = $m[2]", "the measurement is still there");
    
    ok_found_record('SPECIMEN_DATA', "specimen_no = $specimen",
		    "deleting measurements did not delete the specimen");
};


# The taxon, occurrence, and collection of a specimen cannot be changed by an update. Other
# fields can. Repeating the existing values is allowed.

subtest 'update' => sub {
    
    my ($key) = sql_fetchrow("SELECT specimen_no FROM <<SPECIMEN_DATA>> WHERE specimen_id = 'S-3'");
    my ($occ) = sql_fetchrow("SELECT occurrence_no FROM <<SPECIMEN_DATA>> WHERE specimen_no = $key");
    
    my $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->update_record('SPECIMEN_DATA', { specimen_id => $key, comments => 'same place',
					   occurrence_id => $occ, collection_id => $COLL });
    
    ok_commit("an update that repeats the occurrence and collection was accepted");
    
    ok_found_record('OCCURRENCE_DATA', "occurrence_no = $occ and abund_value = '2'",
		    "the update did not count the specimen again");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->update_record('SPECIMEN_DATA', { specimen_id => $key, occurrence_id => $occ + 1000 });
    
    ok_failed_commit("an update that changes the occurrence was rejected");
    
    ok_has_one_condition(qr/^E_CANNOT_CHANGE .*'occurrence_id'/, "the change was reported");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->update_record('SPECIMEN_DATA', { specimen_id => $key, collection_id => $COLL + 1 });
    
    ok_failed_commit("an update that changes the collection was rejected");
    
    ok_has_one_condition(qr/^E_CANNOT_CHANGE .*'collection_id'/, "the change was reported");
    
    # The specimen S-1 was identified by taxon_id as Tyrannosaurus rex.
    
    my ($s1) = sql_fetchrow("SELECT specimen_no FROM <<SPECIMEN_DATA>> WHERE specimen_id = 'S-1'");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->update_record('SPECIMEN_DATA', { specimen_id => $s1, taxon_id => $GENUS_NO });
    
    ok_failed_commit("an update that changes the taxon_id was rejected");
    
    ok_has_one_condition(qr/^E_CANNOT_CHANGE .*'taxon_id'/, "the change was reported");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->update_record('SPECIMEN_DATA', { specimen_id => $s1, taxon_name => 'Tyrannosaurus' });
    
    ok_failed_commit("an update that changes the taxon by name was rejected");
    
    ok_has_one_condition(qr/^E_CANNOT_CHANGE .*'taxon_name'/, "the change was reported");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->update_record('SPECIMEN_DATA', { specimen_id => $s1, taxon_name => 'Nosuchtaxon foo' });
    
    ok_failed_commit("an update to an unknown taxon name was rejected");
    
    ok_has_one_condition(qr/^E_CANNOT_CHANGE .*'taxon_name'/, "the change was reported");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->update_record('SPECIMEN_DATA', { specimen_id => $s1, taxon_id => $SPECIES_NO,
					   comments => 'same taxon by id' });
    $edt->update_record('SPECIMEN_DATA', { specimen_id => $s1, taxon_name => 'Tyrannosaurus rex',
					   comments => 'same taxon by name' });
    
    ok_commit("updates that repeat the existing taxon were accepted");
    
    ok_found_record('SPECIMEN_DATA', "specimen_no = $s1 and taxon_no = $SPECIES_NO
			and comments = 'same taxon by name'", "the specimen kept its taxon");
    
    # A specimen identified through its occurrence has no taxon of its own, so giving it one
    # is a change. This specimen is removed directly afterward, because it was not counted in
    # the abundance of the occurrence, which the tests below rely on.
    
    my ($s10) = insert_specimens({ specimen_code => 'S-10', occurrence_id => $occ,
				   reference_id => $REF });
    
    ok( $s10, "a specimen identified by occurrence_id was inserted" ) || return;
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->update_record('SPECIMEN_DATA', { specimen_id => $s10, taxon_id => $SPECIES_NO });
    
    ok_failed_commit("a taxon cannot be added to a specimen identified through its occurrence");
    
    ok_has_one_condition(qr/^E_CANNOT_CHANGE .*'taxon_id'/, "the change was reported");
    
    sql_command("DELETE FROM <<SPECIMEN_MATRIX>> WHERE specimen_no = $s10");
    sql_command("DELETE FROM <<SPECIMEN_DATA>> WHERE specimen_no = $s10");
};


# A replacement record must identify the specimen, as an inserted record must, and must
# repeat its taxon and its occurrence, because the fields it leaves out are cleared. The
# occurrence may be repeated with occurrence_id, or with the same collection_id.

subtest 'replace' => sub {
    
    my ($s1) = sql_fetchrow("SELECT specimen_no FROM <<SPECIMEN_DATA>> WHERE specimen_id = 'S-1'");
    
    my $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->replace_record('SPECIMEN_DATA', { specimen_id => $s1, specimen_code => 'S-1',
					    reference_id => $REF, comments => 'no taxon' });
    
    ok_failed_commit("a replacement with no identification was rejected");
    
    ok_has_condition('E_MISSING_IDENTIFICATION', "the missing identification was reported");
    ok_has_condition(qr/^E_CANNOT_CHANGE .*'taxon_id'/, "the change to the taxon was reported");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->replace_record('SPECIMEN_DATA', { specimen_id => $s1, specimen_code => 'S-1',
					    taxon_id => $GENUS_NO, reference_id => $REF });
    
    ok_failed_commit("a replacement with a different taxon was rejected");
    
    ok_has_one_condition(qr/^E_CANNOT_CHANGE .*'taxon_id'/, "the change was reported");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->replace_record('SPECIMEN_DATA', { specimen_id => $s1, specimen_code => 'S-1',
					    taxon_name => 'Tyrannosaurus rex', reference_id => $REF,
					    comments => 'replaced' });
    
    ok_commit("a replacement that repeats the taxon was accepted");
    
    ok_found_record('SPECIMEN_DATA', "specimen_no = $s1 and taxon_no = $SPECIES_NO
			and comments = 'replaced'", "the specimen was replaced and kept its taxon");
    
    ok_found_record('SPECIMEN_MATRIX', "specimen_no = $s1 and taxon_no = $SPECIES_NO",
		    "the specimen matrix was updated");
    
    # S-3 belongs to an occurrence in the test collection.
    
    my ($s3, $occ) = sql_fetchrow("SELECT specimen_no, occurrence_no FROM <<SPECIMEN_DATA>>
		WHERE specimen_id = 'S-3'");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->replace_record('SPECIMEN_DATA', { specimen_id => $s3, specimen_code => 'S-3',
					    taxon_id => $SPECIES_NO, reference_id => $REF });
    
    ok_failed_commit("a replacement that leaves out the occurrence was rejected");
    
    ok_has_one_condition(qr/^E_CANNOT_CHANGE .*'occurrence_id'/, "the change was reported");
    
    foreach my $where ( [ occurrence_id => $occ ], [ collection_id => $COLL ] )
    {
	$edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
	
	$edt->replace_record('SPECIMEN_DATA', { specimen_id => $s3, specimen_code => 'S-3',
						taxon_id => $SPECIES_NO, reference_id => $REF,
						@$where });
	
	ok_commit("a replacement that repeats the occurrence with $where->[0] was accepted");
	
	ok_found_record('SPECIMEN_DATA', "specimen_no = $s3 and occurrence_no = $occ",
			"the specimen kept its occurrence");
    }
    
    ok_found_record('OCCURRENCE_DATA', "occurrence_no = $occ and abund_value = '2'",
		    "the replacements did not change the abundance");
};


# Every specimen must have a reference. A missing reference is reported as 'reference_id',
# which is the field that records use.

subtest 'reference' => sub {
    
    my ($occ_before) = sql_fetchrow("SELECT count(*) FROM <<OCCURRENCE_DATA>>");
    
    my $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'R-1', taxon_id => $SPECIES_NO });
    
    ok_failed_commit("a specimen without a reference was rejected");
    
    ok_has_one_condition(qr/^E_REQUIRED .*Field 'reference_id'/, "the missing reference was reported");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'R-2', taxon_id => $SPECIES_NO,
					   reference_id => '' });
    
    ok_failed_commit("a specimen with an empty reference was rejected");
    
    ok_has_one_condition(qr/^E_REQUIRED .*Field 'reference_id'/, "the empty reference was reported");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'R-3', taxon_id => $SPECIES_NO,
					   collection_id => $COLL });
    
    ok_failed_commit("a specimen in a collection without a reference was rejected");
    
    ok_has_one_condition(qr/^E_REQUIRED .*Field 'reference_id'/, "the missing reference was reported");
    
    my ($occ_after) = sql_fetchrow("SELECT count(*) FROM <<OCCURRENCE_DATA>>");
    
    is( $occ_after, $occ_before, "no occurrence was created" );
    
    ok_no_record('SPECIMEN_DATA', "specimen_id like 'R-%'");
    
    # S-1 has the reference $REF.
    
    my ($s1) = sql_fetchrow("SELECT specimen_no FROM <<SPECIMEN_DATA>> WHERE specimen_id = 'S-1'");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->update_record('SPECIMEN_DATA', { specimen_id => $s1, reference_id => '' });
    
    ok_failed_commit("an update that empties the reference was rejected");
    
    ok_has_one_condition(qr/^E_REQUIRED .*Field 'reference_id'/, "the empty reference was reported");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->update_record('SPECIMEN_DATA', { specimen_id => $s1, comments => 'no reference given' });
    
    ok_commit("an update that does not mention the reference was accepted");
    
    ok_found_record('SPECIMEN_DATA', "specimen_no = $s1 and reference_no = $REF",
		    "the specimen kept its reference");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->replace_record('SPECIMEN_DATA', { specimen_id => $s1, specimen_code => 'S-1',
					    taxon_id => $SPECIES_NO });
    
    ok_failed_commit("a replacement without a reference was rejected");
    
    ok_has_one_condition(qr/^E_REQUIRED .*Field 'reference_id'/, "the missing reference was reported");
};


# Only the owner of a specimen, or an administrator, may delete it. Its measurements and its
# row in the specimen matrix are deleted with it. An occurrence that was created for
# specimens has its abundance reduced, and is deleted with its last specimen.

subtest 'delete' => sub {
    
    my ($key) = sql_fetchrow("SELECT specimen_no FROM <<SPECIMEN_DATA>> WHERE specimen_id = 'S-8'");
    
    my $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('OTHER'));
    
    $edt->delete_record('SPECIMEN_DATA', $key);
    
    ok_failed_commit("another member cannot delete the specimen");
    
    ok_has_one_condition('E_PERMISSION', "the refusal was reported");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->delete_record('SPECIMEN_DATA', $key);
    
    ok_commit("the owner deleted the specimen");
    
    ok_no_record('SPECIMEN_DATA', "specimen_no = $key");
    ok_no_record('SPECIMEN_MATRIX', "specimen_no = $key");
    ok_no_record('MEASUREMENT_DATA', "specimen_no = $key");
    
    my ($s3, $occ) = sql_fetchrow("SELECT specimen_no, occurrence_no FROM <<SPECIMEN_DATA>>
		WHERE specimen_id = 'S-3'");
    my ($s4) = sql_fetchrow("SELECT specimen_no FROM <<SPECIMEN_DATA>> WHERE specimen_id = 'S-4'");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->delete_record('SPECIMEN_DATA', $s3);
    
    ok_commit("one of two specimens of an occurrence was deleted");
    
    ok_found_record('OCCURRENCE_DATA', "occurrence_no = $occ and abund_value = '1'",
		    "the abundance was reduced");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->delete_record('SPECIMEN_DATA', $s4);
    
    ok_commit("the last specimen of the occurrence was deleted");
    
    ok_no_record('OCCURRENCE_DATA', "occurrence_no = $occ");
    ok_no_record('OCCURRENCE_MATRIX', "occurrence_no = $occ");
    
    # Deleting all of the specimens of the genus occurrence at once deletes it. N-gen, from the
    # 'names' subtest, also belongs to it.
    
    my @pair = map { $_->[0] } @{$dbh->selectall_arrayref("SELECT specimen_no FROM $TABLE{SPECIMEN_DATA}
		WHERE specimen_id in ('S-5', 'S-6', 'N-gen')")};
    my ($genus_occ) = sql_fetchrow("SELECT max(occurrence_no) FROM <<SPECIMEN_DATA>>
		WHERE specimen_id in ('S-5', 'S-6')");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'));
    
    $edt->delete_record('SPECIMEN_DATA', join(',', @pair));
    
    ok_commit("all of the specimens of the genus occurrence were deleted at once");
    
    ok_no_record('OCCURRENCE_DATA', "occurrence_no = $genus_occ");
    ok_no_record('OCCURRENCE_MATRIX', "occurrence_no = $genus_occ");
};


# When a transaction fails, nothing it inserted is kept, including the measurements and any
# occurrence created for a specimen.

subtest 'rollback' => sub {
    
    my ($occ_before) = sql_fetchrow("SELECT count(*) FROM <<OCCURRENCE_DATA>>");
    
    my $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'), 'PROCEED');
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'S-9', taxon_id => $SPECIES_NO,
					   collection_id => $COLL, reference_id => $REF });
    $edt->insert_record('MEASUREMENT_DATA', { measurement_type => 'length', average => '9' });
    $edt->execute;
    $edt->rollback;
    
    ok_no_record('SPECIMEN_DATA', "specimen_id = 'S-9'");
    ok_no_record('MEASUREMENT_DATA', "measurement_type = 'length' and average = '9'");
    
    my ($occ_after) = sql_fetchrow("SELECT count(*) FROM <<OCCURRENCE_DATA>>");
    
    is( $occ_after, $occ_before, "no occurrence was kept" );
};


# With the PROCEED allowance, a transaction goes on after an action fails. A specimen whose
# insert or delete fails must not change any occurrence, because the occurrences are changed
# only after the specimen statement has succeeded. To make the statements fail, a unique index
# on specimen_id and then a trigger that refuses deletions are added to the test table, and
# removed afterward.

subtest 'proceed' => sub {
    
    my ($occ_count) = sql_fetchrow("SELECT count(*) FROM <<OCCURRENCE_DATA>>");
    my ($n_occs) = sql_fetchrow("SELECT n_occs FROM <<COLLECTION_MATRIX>> WHERE collection_no = $COLL");
    my ($n1, $martes_occ) = sql_fetchrow("SELECT specimen_no, occurrence_no FROM <<SPECIMEN_DATA>>
		WHERE specimen_id = 'N-1'");
    my ($martes_abund) = sql_fetchrow("SELECT abund_value FROM <<OCCURRENCE_DATA>>
		WHERE occurrence_no = $martes_occ");
    
    is( $martes_abund, 2, "the Martes occurrence has two specimens" );
    
    sql_command("ALTER TABLE <<SPECIMEN_DATA>> ADD UNIQUE INDEX edtpc08_unique_id (specimen_id)");
    
    my $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'),
			 'PROCEED', 'UNKNOWN_TAXON');
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'P-1', taxon_id => $SPECIES_NO,
					   reference_id => $REF });
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'S-1', taxon_name => 'Nosuchtaxon proceedus',
					   collection_id => $COLL, reference_id => $REF });
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'N-1', taxon_id => $SUBSPECIES_NO,
					   collection_id => $COLL, reference_id => $REF });
    
    $edt->commit;
    
    sql_command("ALTER TABLE <<SPECIMEN_DATA>> DROP INDEX edtpc08_unique_id");
    
    is( $edt->status, 'committed', "the transaction was committed" );
    
    is( join(',', map { $_->status } $edt->{action_list}->@*), 'executed,failed,failed',
	"the two inserts with duplicate specimen codes failed" );
    
    ok_found_record('SPECIMEN_DATA', "specimen_id = 'P-1'", "the other specimen was inserted");
    
    ok_no_record('OCCURRENCE_DATA', "genus_name = 'Nosuchtaxon' and species_name = 'proceedus'",
		 "no occurrence was created for the failed insert");
    
    ok_found_record('OCCURRENCE_DATA', "occurrence_no = $martes_occ and abund_value = '$martes_abund'",
		    "the existing occurrence was not counted for the failed insert");
    
    my ($occ_count2) = sql_fetchrow("SELECT count(*) FROM <<OCCURRENCE_DATA>>");
    my ($n_occs2) = sql_fetchrow("SELECT n_occs FROM <<COLLECTION_MATRIX>> WHERE collection_no = $COLL");
    
    is( "$occ_count2|$n_occs2", "$occ_count|$n_occs", "the occurrence counts are unchanged" );
    
    # Now make deletions fail. The count of the Martes occurrence is read again, so that this
    # check does not depend on the one above.
    
    ($martes_abund) = sql_fetchrow("SELECT abund_value FROM <<OCCURRENCE_DATA>>
		WHERE occurrence_no = $martes_occ");
    
    sql_command("CREATE TRIGGER $TEST_DB.edtpc08_no_delete BEFORE DELETE ON <<SPECIMEN_DATA>>
		FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'deletion refused for testing'");
    
    $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('ENTERER'), 'PROCEED');
    
    $edt->delete_record('SPECIMEN_DATA', $n1);
    $edt->update_record('SPECIMEN_DATA', { specimen_id => $n1, comments => 'still here' });
    
    $edt->commit;
    
    sql_command("DROP TRIGGER $TEST_DB.edtpc08_no_delete");
    
    is( join(',', map { $_->status } $edt->{action_list}->@*), 'failed,executed',
	"the delete failed and the update was executed" );
    
    ok_found_record('SPECIMEN_DATA', "specimen_no = $n1 and comments = 'still here'",
		    "the specimen was not deleted");
    
    ok_found_record('OCCURRENCE_DATA', "occurrence_no = $martes_occ and abund_value = '$martes_abund'",
		    "the occurrence of the specimen was not changed");
    
    ok_found_record('OCCURRENCE_MATRIX', "occurrence_no = $martes_occ",
		    "the occurrence is still in the occurrence matrix");
};


# A guest may not insert specimens.

subtest 'permissions' => sub {
    
    my $edt = ok_new_edt(class => 'SpecimenEdit', permission => perm('GUEST'));
    
    $edt->insert_record('SPECIMEN_DATA', { specimen_code => 'S-g', taxon_id => $SPECIES_NO,
					   reference_id => $REF });
    
    ok_failed_commit("a guest cannot insert a specimen");
    
    ok_has_condition('E_PERMISSION', "the refusal was reported");
    
    ok_no_record('SPECIMEN_DATA', "specimen_id = 'S-g'");
};


# Clean up the authority records and the test collection.

subtest 'cleanup' => sub {
    
    clear_table($_) foreach EDTPCTest->module_tables('SpecimenEdit');
    
    ok_no_record('AUTHORITY_DATA');
};
