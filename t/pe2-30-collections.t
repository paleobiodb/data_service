# -*- mode: cperl -*-
#
# PBDB 1.2
# --------
#
# The purpose of this file is to test data entry and editing for collections, occurrences,
# and reidentifications, through the operations colls/add, colls/update, colls/delete,
# occs/update, and occs/checknames.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use feature 'unicode_strings';

use Test::More;

use lib qw(lib ../lib t);

use TableDefs qw(%TABLE original_table_name);
use CoreTableDefs;

use Tester;
use PBDBTester;


# Create an instance of the Tester class, with which to make requests to the server.

my $T = Tester->new({ prefix => 'data1.2' });

# Then check to MAKE SURE that the server is in test mode, and that every table written by
# CollectionEdit is switched to the test database. This is very important, because we DO
# NOT WANT to change the data in the main tables. select_module_tables bails out if any of
# them cannot be switched.

$T->test_mode('session_data', 'enable') || BAIL_OUT("could not select test session data");

my $ET = PBDBTester->new('CollectionEdit');

my @GROUPS = $ET->select_module_tables($T, 'CollectionEdit');


# The taxa used by these tests: the genus Tyrannosaurus and the species T. rex. Their
# authority records are copied from the real table. References 5, 6, and 7 are real
# references, which are read but not changed.

my $GENUS_NO = 38613;
my $SPECIES_NO = 54833;

my %COLL = ( collection_name => 'Test collection', reference_id => 'ref:5',
	     access_level => 'the public', release_date => 'immediate',
	     collection_type => 'general faunal/floral', country => 'United States',
	     latdeg => 45, latmin => 30, latdir => 'North',
	     lngdeg => 100, lngdec => 25, lngdir => 'West',
	     latlng_basis => 'stated in text', lithology1 => 'sandstone',
	     environment => 'fluvial-lacustrine indet.', assembl_comps => 'macrofossils',
	     max_interval => 'Maastrichtian' );


# Return the number from an identifier such as 'col:12'.

sub id_no {
    
    my ($oid) = @_;
    
    return $oid && $oid =~ /(\d+)$/ ? $1 : 0;
}


# Return the rows of a table that match an expression, as a string.

sub rows {
    
    my ($table, $columns, $expr) = @_;
    
    my $rows = $ET->dbh->selectall_arrayref("SELECT $columns FROM $TABLE{$table}
		WHERE $expr ORDER BY $columns");
    
    return join('; ', map { join(',', map { $_ // 'NULL' } @$_) } @$rows);
}


# Add a collection through the data service as the specified user, and return its number.

sub new_collection {
    
    my ($session, %fields) = @_;
    
    $T->set_cookie("session_id", $session || "SESSION-ENTERER");
    
    my ($r) = $T->send_records("/colls/add.json", "add a collection",
			       json => [ { %COLL, %fields } ]);
    
    return id_no($r && $r->{oid});
}


# Establish empty test tables, with the two taxa. The test collections, occurrences, and
# reidentifications are numbered above every real one, so that they cannot be confused
# with real records.

subtest 'establish tables' => sub {
    
    foreach my $group ( @GROUPS )
    {
	$ET->establish_test_tables($group, 'test') ||
	    BAIL_OUT("could not establish test tables for '$group'");
    }
    
    $ET->fill_test_table('AUTHORITY_DATA', "taxon_no in ($GENUS_NO, $SPECIES_NO)", 'test');
    
    is( rows('AUTHORITY_DATA', 'taxon_no, taxon_name', '1'),
	"$GENUS_NO,Tyrannosaurus; $SPECIES_NO,Tyrannosaurus rex", "the taxa are in place" );
    
    foreach my $table ( qw(COLLECTION_DATA OCCURRENCE_DATA REID_DATA) )
    {
	my $real = original_table_name($TABLE{$table});
	my $key = $table eq 'REID_DATA' ? 'reid_no' : lc($table) =~ s/_data$/_no/r;
	
	my ($max) = $ET->dbh->selectrow_array("SELECT max($key) FROM $real");
	
	$ET->dbh->do("ALTER TABLE $TABLE{$table} AUTO_INCREMENT = " . ($max + 1000));
    }
    
    pass("test records are numbered above the real ones");
};


# A database member may add a collection. Its coordinates are computed, its primary
# reference becomes a secondary reference, and it is added to the collection matrix.

subtest 'add collection' => sub {
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    my ($r1) = $T->send_records("/colls/add.json", "enterer add",
				json => [ { _label => 'c1', %COLL } ]);
    
    ok( $r1, "an enterer added a collection" ) || return;
    
    like( $r1->{oid}, qr{^col:\d+$}, "it has a proper identifier" );
    is( $r1->{rlb}, 'c1', "it has its label" );
    
    my $no = id_no($r1->{oid});
    
    is( rows('COLLECTION_DATA', 'collection_name, lat, lng', "collection_no = $no"),
	"Test collection,45.500000,-100.250000", "the name and coordinates were stored" );
    
    is( rows('COLLECTION_REFS', 'reference_no', "collection_no = $no"), "5",
	"the primary reference is a secondary reference" );
    
    $ET->ok_record_count(1, 'COLLECTION_MATRIX', "collection_no = $no",
			 "the collection is in the matrix");
    
    # A collection must have a name, and nobody may add one without logging in.
    
    my %no_name = %COLL;
    delete $no_name{collection_name};
    
    my ($m2) = $T->send_data_nocheck("/colls/add.json", "add without a name", json => [ \%no_name ]);
    
    $T->ok_response_code($m2, '400', "a collection without a name gets 400");
    $T->ok_error_like($m2, qr{E_REQUIRED.*collection_name}, "and E_REQUIRED");
    
    $T->set_cookie("session_id", "SESSION-GUEST");
    
    my ($m3) = $T->send_data_nocheck("/colls/add.json", "guest add",
				     json => [ { %COLL, collection_name => 'Guest collection' } ]);
    
    isnt( $T->get_response_code($m3), '200', "a guest may not add a collection" );
    
    $T->set_cookie("session_id", "");
    
    my ($m4) = $T->send_data_nocheck("/colls/add.json", "add without logging in",
				     json => [ { %COLL, collection_name => 'Nobody collection' } ]);
    
    $T->ok_response_code($m4, '401', "adding without logging in gets 401");
    
    $ET->ok_record_count(0, 'COLLECTION_DATA',
			 "collection_name in ('Guest collection', 'Nobody collection')",
			 "neither was stored");
};


# A collection can be updated by any authorized member, with its identifier in the record
# or in the URL. colls/update does not add a collection without an identifier.

subtest 'update collection' => sub {
    
    my $no = new_collection(undef, collection_name => 'Update test');
    
    ok( $no, "a collection was added" ) || return;
    
    my ($r1) = $T->send_records("/colls/update.json", "update the name",
				json => [ { collection_id => "col:$no",
					    collection_name => 'Update test 1' } ]);
    
    is( $r1 && $r1->{oid}, "col:$no", "the same collection was returned" );
    
    $T->set_cookie("session_id", "SESSION-OTHER");
    
    my ($r2) = $T->send_records("/colls/update.json?collection_id=col:$no",
				"update by another member, with the id in the URL",
				json => [ { collection_name => 'Update test 2' } ]);
    
    is( $r2 && $r2->{oid}, "col:$no", "another member updated the collection" );
    
    is( rows('COLLECTION_DATA', 'collection_name', "collection_no = $no"), 'Update test 2',
	"the name was changed" );
    
    my ($m3) = $T->send_data_nocheck("/colls/update.json", "update with no identifier",
				     json => [ { %COLL, collection_name => 'No identifier' } ]);
    
    $T->ok_response_code($m3, '400', "an update with no identifier gets 400");
    
    my ($m4) = $T->send_data_nocheck("/colls/update.json", "update a nonexistent collection",
				     json => [ { collection_id => 'col:999999999',
						 collection_name => 'Nonexistent' } ]);
    
    $T->ok_response_code($m4, '400', "updating a collection that does not exist gets 400");
    
    $ET->ok_record_count(0, 'COLLECTION_DATA',
			 "collection_name in ('No identifier', 'Nonexistent')", "nothing was stored");
};


# Occurrences are added to a collection with occs/update, by records that have an empty
# occurrence_id. Their names are split and matched to taxa, a name without a species gets
# 'sp.', and the collection matrix and secondary references are updated.

my ($OCC_COLL, @OCCS);

subtest 'add occurrences' => sub {
    
    $OCC_COLL = new_collection(undef, collection_name => 'Occurrence test');
    
    ok( $OCC_COLL, "a collection was added" ) || return;
    
    my @records = ( { _label => 'o1', identified_name => 'Tyrannosaurus rex',
		      abund_value => 3, abund_unit => 'specimens' },
		    { _label => 'o2', identified_name => 'Tyrannosaurus cf. rex' },
		    { _label => 'o3', identified_name => 'Unknownus bar', reference_id => 'ref:6' },
		    { _label => 'o4', identified_name => 'Tyrannosaurus' } );
    
    $_->{occurrence_id} = '', $_->{reference_id} ||= 'ref:5' foreach @records;
    
    my (@r) = $T->send_records("/occs/update.json?collection_id=col:$OCC_COLL",
			       "add four occurrences", json => \@records);
    
    cmp_ok( scalar(@r), '==', 4, "four occurrences were returned" ) || return;
    
    my %by_label = map { $_->{rlb} => $_ } @r;
    
    ok( $by_label{$_}, "occurrence $_ is reported with its label" ) foreach qw(o1 o2 o3 o4);
    
    @OCCS = map { id_no($by_label{$_}{oid}) } qw(o1 o2 o3 o4);
    
    is( rows('OCCURRENCE_DATA', 'genus_name, species_reso, species_name, taxon_no',
	     "collection_no = $OCC_COLL"),
	"Tyrannosaurus,NULL,rex,$SPECIES_NO; Tyrannosaurus,NULL,sp.,$GENUS_NO; " .
	"Tyrannosaurus,cf.,rex,$SPECIES_NO; Unknownus,NULL,bar,0",
	"the names were split and matched to taxa" );
    
    $ET->ok_record_count(4, 'OCCURRENCE_MATRIX', "collection_no = $OCC_COLL",
			 "the occurrences are in the matrix");
    
    is( rows('COLLECTION_REFS', 'reference_no', "collection_no = $OCC_COLL"), "5; 6",
	"the occurrence references are secondary references" );
    
    is( rows('COLLECTION_MATRIX', 'n_occs', "collection_no = $OCC_COLL"), 4,
	"the occurrence count was updated" );
};


# An occurrence is updated by its identifier. Changing the name rematches it.

subtest 'update occurrence' => sub {
    
    ok( $OCCS[3], "found the occurrence to update" ) || return;
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    # The new name must not duplicate another occurrence in the collection.
    
    my ($r1) = $T->send_records("/occs/update.json", "change a name",
				json => [ { occurrence_id => "occ:$OCCS[3]",
					    identified_name => 'Tyrannosaurus aff. rex' } ]);
    
    is( $r1 && $r1->{oid}, "occ:$OCCS[3]", "the occurrence was returned" );
    
    is( rows('OCCURRENCE_DATA', 'species_reso, species_name, taxon_no', "occurrence_no = $OCCS[3]"),
	"aff.,rex,$SPECIES_NO", "it was matched to the new taxon" );
    
    my ($m2) = $T->send_data_nocheck("/occs/update.json", "change to a duplicate name",
				     json => [ { occurrence_id => "occ:$OCCS[3]",
						 identified_name => 'Tyrannosaurus rex' } ]);
    
    $T->ok_response_code($m2, '400', "a name that duplicates another occurrence gets 400");
    $T->ok_error_like($m2, qr{duplicates another occurrence}, "and is reported");
    
    is( rows('OCCURRENCE_MATRIX', 'taxon_no', "occurrence_no = $OCCS[3]"), $SPECIES_NO,
	"and the matrix was updated" );
};


# A reidentification is added with an empty reid_id, and becomes the latest identification
# of its occurrence.

subtest 'reidentifications' => sub {
    
    ok( $OCCS[0], "found the occurrence to reidentify" ) || return;
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    my (@r) = $T->send_records("/occs/update.json", "add a reidentification",
			       json => [ { _label => 'e1', reid_id => '', occurrence_id => "occ:$OCCS[0]",
					   collection_id => "col:$OCC_COLL", reference_id => 'ref:7',
					   identified_name => 'Tyrannosaurus aff. rex' } ]);
    
    my ($reid) = grep { $_->{rlb} && $_->{rlb} eq 'e1' } @r;
    
    ok( $reid && $reid->{eid}, "the reidentification was reported with its label" ) || return;
    
    is( rows('REID_DATA', 'species_reso, species_name, reference_no',
	     "occurrence_no = $OCCS[0]"), "aff.,rex,7", "it was stored" );
    
    is( rows('OCCURRENCE_MATRIX', 'reid_no > 0, latest_ident', "occurrence_no = $OCCS[0]"),
	"0,0; 1,1", "it is the latest identification of the occurrence" );
};


# Occurrences and reidentifications are deleted with '_delete'. A record with neither
# occurrence_id nor reid_id is an error.

subtest 'delete occurrences' => sub {
    
    ok( $OCCS[0] && $OCCS[2], "found the occurrences" ) || return;
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    my ($reid_no) = $ET->dbh->selectrow_array("SELECT reid_no FROM $TABLE{REID_DATA}
		WHERE occurrence_no = $OCCS[0]");
    
    my (@d) = $T->send_records("/occs/update.json", "delete a reid and an occurrence",
			       json => [ { _label => 'x1', reid_id => "rei:$reid_no", _delete => 1 },
					 { _label => 'x2', occurrence_id => "occ:$OCCS[2]",
					   _delete => 1 } ]);
    
    cmp_ok( scalar(@d), '==', 2, "both deletions were reported" );
    
    $ET->ok_record_count(0, 'REID_DATA', "reid_no = $reid_no", "the reidentification was deleted");
    $ET->ok_record_count(0, 'OCCURRENCE_DATA', "occurrence_no = $OCCS[2]", "the occurrence was deleted");
    $ET->ok_record_count(0, 'OCCURRENCE_MATRIX', "occurrence_no = $OCCS[2]",
			 "and its matrix row");
    
    is( rows('OCCURRENCE_MATRIX', 'reid_no, latest_ident', "occurrence_no = $OCCS[0]"), "0,1",
	"the original identification of the reidentified occurrence is the latest again" );
    
    is( rows('COLLECTION_MATRIX', 'n_occs', "collection_no = $OCC_COLL"), 3,
	"the occurrence count was updated" );
    
    my ($m) = $T->send_data_nocheck("/occs/update.json", "a record with no identifier",
				    json => [ { identified_name => 'Tyrannosaurus rex',
						collection_id => "col:$OCC_COLL" } ]);
    
    $T->ok_response_code($m, '400', "a record with neither identifier gets 400");
};


# occs/checknames reports how names would be parsed and matched, without storing anything.

subtest 'checknames' => sub {
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    my ($r1) = $T->fetch_records("/occs/checknames.json?name=Tyrannosaurus%20cf.%20rex",
				 "check one name");
    
    ok( $r1, "the name was checked" );
    
    my ($m2) = $T->fetch_nocheck("/occs/checknames.json?name=tyrannosaurus%20Rex",
				 "check a badly formed name");
    
    ok( $T->get_errors($m2) || $T->get_warnings($m2) ||
	grep( { $_->{err} || $_->{wrn} } $T->extract_records($m2, "", { no_check => 1 }) ),
	"a badly formed name is reported" );
    
    my $before = $ET->count_records('OCCURRENCE_DATA');
    
    my (@r3) = $T->send_records("/occs/checknames.json", "check names in a request body",
				json => [ { _label => 'n1', name => 'Tyrannosaurus rex' },
					  { _label => 'n2', name => 'Unknownus bar' } ]);
    
    cmp_ok( scalar(@r3), '>=', 2, "both names were checked" );
    
    is( $ET->count_records('OCCURRENCE_DATA'), $before, "nothing was stored" );
};


# Only the owner of a collection may delete it, and deleting it deletes its occurrences.
# Collections can be deleted in the URL and in a request body.

subtest 'delete collections' => sub {
    
    my $no1 = new_collection(undef, collection_name => 'Delete test 1');
    my $no2 = new_collection(undef, collection_name => 'Delete test 2');
    
    ok( $no1 && $no2, "two collections were added" ) || return;
    
    $T->set_cookie("session_id", "SESSION-OTHER");
    
    my ($m0) = $T->fetch_nocheck("/colls/delete.json?collection_id=col:$no1",
				 "delete by somebody else");
    
    $T->ok_response_code($m0, '400', "a delete by somebody else gets 400");
    $T->ok_error_like($m0, qr{E_PERM}, "and E_PERMISSION");
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    $T->fetch_url("/colls/delete.json?collection_id=col:$no1", "delete in the URL");
    
    $ET->ok_record_count(0, 'COLLECTION_DATA', "collection_no = $no1", "the collection was deleted");
    $ET->ok_record_count(0, 'COLLECTION_MATRIX', "collection_no = $no1", "and its matrix row");
    
    my (@d) = $T->send_records("/colls/delete.json", "delete with a request body",
			       json => [ { _label => 'x2', collection_id => "col:$no2" } ]);
    
    $ET->ok_record_count(0, 'COLLECTION_DATA', "collection_no = $no2",
			 "the collection in the body was deleted");
    
    # POST is accepted as well as PUT.
    
    my $no3 = new_collection(undef, collection_name => 'Delete test 3');
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    $T->send_records("/colls/delete.json", "delete with a request body and POST",
		     json => [ { collection_id => "col:$no3" } ], { http_method => 'post' });
    
    $ET->ok_record_count(0, 'COLLECTION_DATA', "collection_no = $no3",
			 "the collection was deleted with POST");
    
    my $no4 = new_collection(undef, collection_name => 'Delete test 4');
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    $T->server_request('delete', "/colls/delete.json?collection_id=col:$no4",
		       "delete with method DELETE");
    
    $ET->ok_record_count(0, 'COLLECTION_DATA', "collection_no = $no4",
			 "the collection was deleted with method DELETE");
    
    # Deleting the collection with occurrences deletes them as well.
    
    $T->fetch_url("/colls/delete.json?collection_id=col:$OCC_COLL",
		  "delete the collection with occurrences");
    
    $ET->ok_record_count(0, 'OCCURRENCE_DATA', "collection_no = $OCC_COLL",
			 "its occurrences were deleted");
    $ET->ok_record_count(0, 'OCCURRENCE_MATRIX', "collection_no = $OCC_COLL",
			 "and their matrix rows");
    
    my ($m3) = $T->fetch_nocheck("/colls/delete.json?collection_id=col:999999999",
				 "delete a collection that does not exist");
    
    $T->ok_response_code($m3, '400', "deleting a collection that does not exist gets 400");
};


# Remove the test records.

subtest 'cleanup' => sub {
    
    foreach my $group ( @GROUPS )
    {
	$ET->clear_table($_) foreach $TableDefs::TABLE_GROUP{$group}->@*;
    }
    
    $ET->ok_record_count(0, 'COLLECTION_DATA', '*', "the test collection table is empty");
};


done_testing();


# Switch the test tables back to the real ones when this file is done, so that later
# test files are not affected.

END {
    $ET->release_module_tables($T) if $ET && $T;
}
