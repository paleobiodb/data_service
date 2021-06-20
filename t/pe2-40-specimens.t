# -*- mode: cperl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test data entry and editing for specimens.
# 

use strict;

use feature 'unicode_strings';
use feature 'fc';

use Test::More tests => 13;

use lib qw(lib ../lib t);

use CoreFunction qw(connectDB configData);
use TableDefs qw(%TABLE);
use CoreTableDefs;

use Tester;
use EditTester;

# use Data::Dumper::Concise;


# Use the following values from the database in test instances. If these ever become invalid, they
# can be replaced with valid values here.

my $TAXON_1 = 'Dascillidae';
my $TID_1 = '69296';

my $TAXON_2 = 'Dascillus flatus';
my $TID_2 = 'txn:241269';
my $TAXON_NO_2 = '241269';

my $TAXON_3 = 'Dascillidae';
my $TID_3 = 'txn:69296';
my $TAXON_NO_3 = '69296';

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });

# Then check to MAKE SURE that the server is in test mode and the test timescale tables are
# enabled. This is very important, because we DO NOT WANT to change the data in the main
# tables. If we don't get the proper response back, we need to bail out. These count as the first
# two tests in this file.

$T->test_mode('session_data', 'enable') || BAIL_OUT("could not select test session data");
$T->test_mode('specimen_data', 'enable') || BAIL_OUT("could not select test specimen tables");
$T->test_mode('occurrence_data', 'enable') || BAIL_OUT("could not select test occurrence tables");

# Finally, grab a database connection and check to make sure that we are actually looking at the
# test tables. That will give us confidence to clear the tables between tests.

my $ET = EditTester->new('SpecimenEdit');


# The first testing task it so establish the specimen tables in the test database and select
# them.

subtest 'establish tables' => sub {

    $ET->establish_test_tables('specimen_data', 'test') ||
	BAIL_OUT("could not establish test tables for 'specimen_data'");

    $ET->establish_test_tables('occurrence_data', 'test') ||
	BAIL_OUT("could not establish test tables for 'occurrence_data'");
    
    $ET->fill_test_table('OCCURRENCE_DATA', "taxon_no = $TAXON_NO_2", 'test');
    $ET->fill_test_table('OCCURRENCE_MATRIX', "taxon_no = $TAXON_NO_2", 'test');
    
    $ET->start_test_mode('specimen_data') || BAIL_OUT "could not select test specimen tables locally";
    $ET->start_test_mode('occurrence_data') || BAIL_OUT "could not select test occurrence tables locally";
    
    pass('test tables established');
};


# First check that the table schemas for specimens and measurements in the test database match the
# corresponding schemas in the main database.

# subtest 'check schemas' => sub {

#     $ET->check_test_schema('SPECIMEN_DATA');
#     $ET->check_test_schema('MEASUREMENT_DATA');

#     # # Double check to make sure we aren't pointing at the main occurrence table.

#     # my ($count) = $ET->dbh->selectrow_array("SELECT count(*) FROM $TABLE{OCCURRENCE_DATA}");

#     # if ( $count > 1000 )
#     # {
#     # 	BAIL_OUT "test occurrence table contains too many entries - is it the real one?";
#     # }

#     # # If it is okay, then clear the table.
    
#     my ($sql, $result);
    
#     # $sql = "DELETE FROM $TABLE{OCCURRENCE_DATA}";
    
#     # print STDERR "$sql\n\n" if $ET->debug;
    
#     # $result = $ET->dbh->do($sql);
    
#     # $sql = "DELETE FROM $TABLE{OCCURRENCE_MATRIX}";
    
#     # print STDERR "$sql\n\n" if $ET->debug;
    
#     # $result = $ET->dbh->do($sql);
    
#     # Safely clear the occurrence data and matrix tables.

#     $ET->safe_clear_table('OCCURRENCE_DATA', 'enterer_no');
#     $ET->safe_clear_table('OCCURRENCE_MATRIX', 'enterer_no');
    
#     # Copy over some occurrences from the main table to the test table.
    
#     $sql = "REPLACE INTO $TABLE{OCCURRENCE_DATA}
# 		SELECT * FROM $TABLE{'==OCCURRENCE_DATA'} WHERE taxon_no = $TAXON_NO_2";
    
#     print STDERR "$sql\n\n" if $ET->debug;
    
#     $result = $ET->dbh->do($sql);
    
#     diag("Replaced $result items into test occurrence table");
    
#     $sql = "REPLACE INTO $TABLE{OCCURRENCE_MATRIX}
# 		SELECT * FROM $TABLE{'==OCCURRENCE_MATRIX'} WHERE taxon_no = $TAXON_NO_2";
    
#     print STDERR "$sql\n\n" if $ET->debug;
    
#     $result = $ET->dbh->do($sql);
    
#     diag("Replaced $result items into test occurrence matrix");
# };


# Check that we can add records, and that the returned records contain proper identifiers and
# labels. If record addition fails, we bail out of the entire test, because there is no point in
# continuing.

subtest 'add simple' => sub {

    # Safely clear the SPECIMEN_DATA and MEASUREMENT_DATA tables.
    
    $ET->safe_clear_table('MEASUREMENT_DATA', 'enterer_no', 'SPECIMEN_DATA', 'specimen_no');
    $ET->safe_clear_table('SPECIMEN_DATA', 'enterer_no');
    $ET->safe_clear_table('SPECIMEN_MATRIX', 'enterer_no');
    
    # Next, see if a user with ordinary authorizer privileges can add. If this fails, there is no
    # reason to go any further.
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    my $record1 = [
	       { _label => 'a1',
		 specimen_code => 'TEST SIMPLE 1',
		 # taxon_id => '71894',
		 taxon_name => $TAXON_1,
		 reference_id => 'ref:5041',
		 specimen_side => 'right?',
		 sex => 'male',
		 measurement_source => 'Direct',
		 specelt_id => 'els:500' },
	       { _label => 'm1',
		 specimen_id => '@a1',
		 measurement_type => 'length',
		 max => '1.5 mm',
		 min => '1.2 mm' },
	       { _label => 'm2',
		 specimen_id => '@a1',
		 measurement_type => 'length',
		 max => '1.0 mm',
		 min => '1.0 mm' },
	       ];
    
    my (@r1) = $T->send_records("/specs/addupdate.json?show=crmod,ent", "simple insert 1", json => $record1);
    
    unless ( @r1 )
    {
	BAIL_OUT("adding new records failed");
    }
    
    like( $r1[0]{oid}, qr{^spm:\d+$}, "added specimen record has properly formatted oid" ) &&
	diag("New specimen oid: $r1[0]{oid}");
    is( $r1[0]{rlb}, 'a1', "added specimen record has proper label" );
    
    # print STDERR $T->{last_response}->content;
    
    like( $r1[1]{oid}, qr{^mea:\d+$}, "added measurement record has properly formatted oid" ) &&
    	diag("New measurement oid: $r1[1]{oid}");
    is( $r1[1]{rlb}, "m1", "added measurement record has proper label" );
    
    # Make sure the inserted specimen record has non-zero values in all of the following fields:
    # authorizer_no, enterer_no, created, modified
    
    ok( $r1[0]{ati}, "added specimen record has non-empty authorizer id" );
    ok( $r1[0]{eni}, "added specimen record has non-empty enterer id" );
    ok( $r1[0]{dcr}, "added specimen record has non-empty creation date" ) &&
	cmp_ok( $r1[0]{dcr}, 'gt', "2019-01-01", "added specimen record has non-zero creation date" );
    ok( $r1[0]{dmd}, "added specimen record has non-empty modification date" ) &&
	cmp_ok( $r1[0]{dmd}, 'gt', "2019-01-01", "added specimen record has non-zero modification date" );    
    
    # Check the inserted records against the output of the data retrieval operation.
    
    my ($authorizer_no, $enterer_no) = $ET->get_session_authinfo('SESSION-AUTHORIZER');
    
    my (@check1) = $T->fetch_records("/specs/list.json?all_records&specs_entered_by=$enterer_no&show=crmod,ent",
				     "fetch entered specimens");
    
    # Delete the label from the record returned by the insertion operation, and also the record
    # type if any. Currently, this field appears unnecessarily in the returned measurement
    # records. Tthen compare the retrieved record to that, to make sure that the insertion
    # operation return matches the data retrieval return.
    
    foreach my $r (@r1)
    {
	delete $r->{rlb};
	delete $r->{typ};
    }
    
    cmp_ok(@check1, '==', 1, "retrieved single inserted specimen record") &&
	is_deeply($check1[0], $r1[0], "retrieved record matches result of insertion");
    
    my (@check2) = $T->fetch_records("/specs/measurements.json?spec_id=$check1[0]{oid}",
				     "fetch entered measurements");
    
    if ( cmp_ok(@check2, '==', 2, "retrieved two inserted measurement records") )
    {
	is_deeply($check2[0], $r1[1], "first retrieved measurement matches result of insertion");
	is_deeply($check2[1], $r1[2], "second retrieved measurement matches result of insertion");
    }
    
    # Now try adding a specimen tied to an existing occurrence.
    
    my ($occ1) = $T->fetch_records("/occs/list.json?taxon_name=$TAXON_2&limit=1", "fetched an occurrence");
    
    my $occ_id = $occ1->{oid};
    
    diag("Fetched occurrence oid: $occ_id");
    
    my $record2 = { _label => 'a2',
		    specimen_code => 'TEST SIMPLE 2',
		    occurrence_id => $occ_id,
		    reference_id => 'ref:5041',
		    specimen_side => 'left',
		    sex => 'male',
		    measurement_source => 'Direct',
		    specelt_id => 'els:500' };
    
    my (@r1) = $T->send_records("/specs/addupdate.json", "simple insert 2", json => $record2);
    
    if ( @r1 )
    {
	is( $r1[0]{qid}, $occ_id, "added specimen has proper occurrence id" );
	is( $r1[0]{tna}, $TAXON_2, "added specimen has proper taxon name" );
    }

};


# Check the various formats allowed for the request body. The first format, an array of object
# records, was already checked above.

subtest 'request body' => sub {
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    # Safely clear the SPECIMEN_DATA and MEASUREMENT_DATA tables.
    
    $ET->safe_clear_table('MEASUREMENT_DATA', 'enterer_no', 'SPECIMEN_DATA', 'specimen_no');
    $ET->safe_clear_table('SPECIMEN_DATA', 'enterer_no');
    $ET->safe_clear_table('SPECIMEN_MATRIX', 'enterer_no');
    
    # Try a single object record.
    
    my $format2 = { _label => 'a1',
		    specimen_code => 'TEST FORMAT 2',
		    taxon_name => $TAXON_1,
		    reference_id => 'ref:5041',
		    specimen_side => 'right?',
		    sex => 'male',
		    measurement_source => 'Direct',
		    specelt_id => 'els:500' };
    
    my (@r2) = $T->send_records("/specs/addupdate.json", "superuser add 2", json => $format2);
    
    cmp_ok( @r2, '==', 1, "inserted one record with single record request body" ) &&
	is( $r2[0]{smi}, 'TEST FORMAT 2' );

    # Now try a list of object records with some common parameters.
    
    my $format3 = { all => { taxon_name => $TAXON_1,
			     reference_id => 'ref:5041' },
		    records => [{ _label => 'a2',
				  specimen_code => 'TEST FORMAT 3a',
				  specimen_side => 'right?',
				  sex => 'male',
				  measurement_source => 'Direct',
				  specelt_id => 'els:500' },
			        { _label => 'a3',
				  specimen_code => 'TEST FORMAT 3b',
				  specimen_side => 'left',
				  sex => 'male',
				  measurement_source => 'Direct',
				  specelt_id => 'els:500' }],
		  };
    
    my (@r3) = $T->send_records("/specs/addupdate.json", "superuser add 3", json => $format3);
    
    if ( cmp_ok( @r3, '==', 2, "inserted two records as a JSON array" ) )
    {
	is( $r3[0]{smi}, 'TEST FORMAT 3a', "record had proper specimen code" );
	is( $r3[0]{tna}, $TAXON_1, "record had proper taxonomic name" );
	is( $r3[0]{rid}, 'ref:5041', "record had proper reference id");
	is( $r3[1]{smi}, 'TEST FORMAT 3b', "record had proper specimen code" );
	is( $r3[1]{tna}, $TAXON_1, "record had proper taxonomic name");
	is( $r3[1]{rid}, 'ref:5041', "record had proper reference id");
    }
    
    # Now try a list of form fields to be URL encoded. This will simulate an insertion from a web
    # form.

    my $format4 = [ _label => 'a2',
    		    specimen_code => 'TEST FORMAT 4',
    		    taxon_name => $TAXON_1,
    		    reference_id => 'ref:5041',
    		    specimen_side => 'right?',
    		    sex => 'male',
    		    measurement_source => 'Direct',
    		    specelt_id => 'els:500' ];
    
    my (@r4) = $T->send_records("/specs/addupdate.json", "superuser add 4", form => $format4);
    
    cmp_ok( @r4, '==', 1, "inserted one record with form body" ) &&
    	is( $r4[0]{smi}, 'TEST FORMAT 4', "record had proper specimen code" );

    # Now verify that the proper number of records were actually inserted.

    $ET->ok_count_records(4, 'SPECIMEN_DATA', '1', "three specimen records were inserted");
};


# Test that we can insert and delete records, 

subtest 'insert and delete' => sub {

    # Get a count of the number of specimens and measurements first.

    my $spec_count = $ET->count_records('SPECIMEN_DATA');
    my $matr_count = $ET->count_records('SPECIMEN_MATRIX');
    my $meas_count = $ET->count_records('MEASUREMENT_DATA');
    
    is($spec_count, $matr_count, "specimen count and matrix count are the same");
    
    # Then see if a user with the admin privileges can add. If this fails, there is no
    # reason to go any further.
    
    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    my $input = [
		 { _label => 'a1',
		   specimen_code => 'TEST.a1',
		   taxon_name => $TAXON_1,
		   reference_id => 'ref:5041',
		   specimen_side => 'right?',
		   sex => 'male',
		   measurement_source => 'Direct',
		   specelt_id => 'els:500' },
		 { _label => 'm1',
		   specimen_id => '@a1',
		   measurement_type => 'length',
		   max => '2.5 mm',
		   min => '2.2 mm' },
		 { _label => 'a2',
		   specimen_code => 'TEST.a2',
		   taxon_name => $TAXON_1,
		   reference_id => 'ref:5041',
		   specimen_side => 'right?',
		   sex => 'male',
		   measurement_source => 'Direct',
		   specelt_id => 'els:500' },
		 { _label => 'm2',
		   specimen_id => '@a1',
		   measurement_type => 'length',
		   max => '3.5 mm',
		   min => '3.2 mm' },
		 { _label => 'm3',
		   specimen_id => '@a1',
		   measurement_type => 'length',
		   max => '4.5 mm',
		   min => '4.2 mm' }
		];
    
    my (@r1) = $T->send_records("/specs/addupdate.json", "add records", json => $input);
    
    unless ( @r1 )
    {
	BAIL_OUT("adding new records failed");
    }
    
    my (@spec, @meas);
    
    foreach my $r ( @r1 )
    {
	if ( $r->{oid} && $r->{oid} =~ /^spm/ )
	{
	    push @spec, $r->{oid};
	}

	elsif ( $r->{oid} && $r->{oid} =~ /^mea/ )
	{
	    push @meas, $r->{oid};
	}

	else
	{
	    diag "bad oid: '$r->{oid}'";
	}
    }
    
    my $new_spec_count = $ET->count_records('SPECIMEN_DATA');
    my $new_matr_count = $ET->count_records('SPECIMEN_MATRIX');
    my $new_meas_count = $ET->count_records('MEASUREMENT_DATA');
    
    is($new_spec_count, $spec_count+2, "specimen count added two records");
    is($new_meas_count, $meas_count+3, "measurement count added three records");
    is($new_spec_count, $new_matr_count, "specimen count and matrix count are the same");

    # Check that we inserted the correct number of records. If not, then there is no point in
    # continuing with the test.
    
    cmp_ok( @meas, '==', 3, "inserted three measurement records" );
    cmp_ok( @spec, '==', 2, "inserted two specimen records" ) || return;
	
    # Check that the specimen matrix was properly updated to match the inserted records.
    
    my (@matr) = $ET->fetch_records_by_key('SPECIMEN_MATRIX', @spec);
    
    cmp_ok( @matr, '==', @spec, "matrix records found for each inserted specimen record" );

    # $$$ we need to check the individual fields
    
    # Now try deleting the inserted records.
    
    my $spec_ids = join(',', @spec);
    
    my (@m1) = $T->fetch_records("/specs/measurements.json?spec_id=$spec_ids", "fetch measurements");
    
    cmp_ok(@m1, '==', 3, "fetched three inserted measurements");
    
    my (@d1) = $T->fetch_records("/specs/delete.json?specimen_id=$spec[0]", "delete record with url param");
    
    # print STDERR Dumper(@d1);
    
    ok( @d1, "deleted a record with url param") &&
	is( $d1[0]{oid}, $spec[0], "deleted proper record with url param" );
    
    my (@d2) = $T->send_records("/specs/delete.json", "delete record with body",
				json => { specimen_id => $spec[1] });
    
    ok( @d2, "deleted a record with body") &&
	is( $d2[0]{oid}, $spec[1], "deleted proper record with body" );
    
    my (@m2) = $T->fetch_records("/specs/measurements.json?spec_id=$spec_ids",
				 "fetch measurements", { no_records_ok => 1 });
    
    cmp_ok(@m2, '==', 0, "all three measurements were deleted along with specimens");
    
    $ET->ok_count_records($spec_count, 'SPECIMEN_DATA', 1, "insert and delete left same number of specimens");
    $ET->ok_count_records($matr_count, 'SPECIMEN_MATRIX', 1, "insert and delete left same number of matrix rows");
    $ET->ok_count_records($meas_count, 'MEASUREMENT_DATA', 1, "insert and delete left same number of measurements");
    
};
    

# Now insert some more records, and then test that we can update them in place.

subtest 'update' => sub {

    $T->set_cookie("session_id", "SESSION-AUTHORIZER");
    
    my $input = [
		 { _label => 'a1',
		   specimen_code => 'TEST.b1',
		   taxon_name => $TAXON_1,
		   reference_id => 'ref:5041',
		   specimen_side => 'right?',
		   sex => 'male',
		   measurement_source => 'Direct',
		   specelt_id => 'els:500' },
		 { _label => 'm1',
		   specimen_id => '@a1',
		   measurement_type => 'length',
		   max => '2.5 mm',
		   min => '2.2 mm' },
		 { _label => 'a2',
		   specimen_code => 'TEST.b2',
		   taxon_name => $TAXON_1,
		   reference_id => 'ref:5041',
		   specimen_side => 'right?',
		   sex => 'male',
		   measurement_source => 'Direct',
		   specelt_id => 'els:500' },
		 { _label => 'm2',
		   specimen_id => '@a1',
		   measurement_type => 'length',
		   max => '3.5 mm',
		   min => '3.2 mm' },
		 { _label => 'm3',
		   specimen_id => '@a1',
		   measurement_type => 'length',
		   max => '4.5 mm',
		   min => '4.2 mm' }
		];
    
    my (@r1) = $T->send_records("/specs/addupdate.json", "add records", json => $input);
    
    cmp_ok(@r1, '==', 5, "inserted five records") || return;
    
    # print STDERR Dumper(@r1);

    my (@spec, @meas);
    
    foreach my $r (@r1)
    {
	push @spec, $r->{oid} if $r->{oid} =~ /^spm/;
	push @meas, $r->{oid} if $r->{oid} =~ /^mea/;
    }
    
    my $update = [{ _label => 'a2',
		    specimen_id => $spec[1],
		    sex => 'female' },
	          { _label => 'm2',
		    measurement_id => $meas[1],
		    measurement_type => 'width' },
	          { _label => 'm3',
		    measurement_id => $meas[2],
		    measurement_type => 'width' }];
    
    my (@r2) = $T->send_records("/specs/addupdate.json", "update records", json => $update);
    
    cmp_ok(@r2, '==', 3, "updated three records") || return;

    is( $r2[0]{smx}, 'female', "specimen record was updated" );
    is( $r2[0]{rid}, 'ref:5041', "unupdated fields were preserved" );
    is( $r2[1]{mty}, 'width', "measurement record 1 was updated" );
    is( $r2[1]{mvu}, '3.5 mm', "measurement record 1 was properly updated" );
    is( $r2[2]{mty}, 'width', "measurement record 2 was updated" );
    is( $r2[2]{mvl}, '4.2 mm', "unupdated fields were preserved" );
};


# Check that insertion and deletion of specimens in a known collection properly creates and
# removes occurrence records.

subtest 'insert and delete from collection' => sub {
    
    # First fetch a collection.
    
    my ($coll1) = $T->fetch_records("/colls/list.json?interval=permian&limit=1", "fetched an arbitrary collection");
    
    my $coll_id = $coll1->{oid};

    # diag("SKIPPING") && return;
    
    diag("Fetched collection oid: $coll_id");
    
    # Then add an specimen record linked to that collection.
    
    my $record = { _label => 'a2',
		    specimen_code => 'TEST COLL 1',
		    taxon_id => $TID_2,
		    collection_id => $coll_id,
		    reference_id => 'ref:5041',
		    specimen_side => 'left',
		    sex => 'male',
		    measurement_source => 'Direct',
		    specelt_id => 'els:500' };
    
    my (@r1) = $T->send_records("/specs/addupdate.json", "test coll 1", json => $record);
    
    # We can abort this subtest if the addition does not succeed.
    
    return unless @r1;
    
    is( $r1[0]{cid}, $coll_id, "added specimen has proper collection id" );
    is( $r1[0]{tid}, $TID_2, "added specimen has proper taxon id" );

    # Now check that an occurrence was created for this specimen. Abort the subtest if not.
    
    my $occ_id = $r1[0]{qid};
    
    ok($occ_id, "added specimen has an occurrence id") || return;
    
    my (@f2) = $T->fetch_records("/occs/single.json?occ_id=$occ_id&show=abund");

    is( $f2[0]{oid}, $occ_id, "fetched record has proper occurrence id" );
    is( $f2[0]{abv}, '1', "fetched occurrence has an abundance of 1" );
    
    # Now add a second specimen record with the same taxon id.
    
    $record = { _label => 'a2',
		specimen_code => 'TEST COLL 2',
		taxon_id => $TID_2,
		collection_id => $coll_id,
		reference_id => 'ref:5041',
		specimen_side => 'left',
		sex => 'male',
		measurement_source => 'Direct',
		specelt_id => 'els:500' };
    
    my (@r2) = $T->send_records("/specs/addupdate.json", "test coll 2", json => $record);

    is( $r2[0]{qid}, $occ_id, "second added specimen had the same occurrence id" );
    
    my (@f2) = $T->fetch_records("/occs/single.json?occ_id=$occ_id&show=abund");
    
    is( $f2[0]{abv}, '2', "fetched occurrence now has an abundance of 2" );

    # Now add a third specimen record with a different taxon id.
    
    $record = { _label => 'a2',
		specimen_code => 'TEST COLL 3',
		taxon_id => $TID_3,
		collection_id => $coll_id,
		reference_id => 'ref:5041',
		specimen_side => 'left',
		sex => 'male',
		measurement_source => 'Direct',
		specelt_id => 'els:500' };

    my (@r3) = $T->send_records("/specs/addupdate.json", "test coll 3", json => $record);
    
    my $occ_2 = $r3[0]{qid};
    
    ok( $occ_2, "third added specimen had a non-empty occurrence id" ) || return;
    
    isnt( $occ_2, $occ_id, "third added specimen had a different occurrence id than the first two" );
    
    # Now we delete the third specimen, and check that its occurrence is deleted as well.

    my (@d3) = $T->fetch_records("/specs/delete.json?spec_id=$r3[0]{oid}");

    my $x1 = $T->fetch_nocheck("/occs/list.json?occ_id=$occ_2");

    $T->ok_no_records($x1, "second added occurrence is now gone");
    
    my (@f3) = $T->fetch_nocheck("/occs/list.json?occ_id=$occ_id", "first added occurrence is still there");
    
    # Now we delete the second specimen, and check that its occurrence remains with an abundance
    # count reduced to 1.

    my (@d2) = $T->fetch_records("/specs/delete.json?spec_id=$r2[0]{oid}");

    my (@f4) = $T->fetch_records("/occs/list.json?occ_id=$occ_id&show=abund");
    
    is( $f4[0]{abv}, '1', "fetched occurrence now has an abundance of 1" );

    # Then we delete the first specimen, and check that its occurrence has now vanished.

    my (@d1) = $T->fetch_records("/specs/delete.json?spec_id=$r1[0]{oid}");

    my $x2 = $T->fetch_nocheck("/occs/list.json?occ_id=$occ_id");

    $T->ok_no_records($x2, "first added occurrence is now gone");
};


# Check that insertions and updates with bad values are caught and properly reported.

subtest 'bad values' => sub {
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    # Send a record with nothing in it at all.
    
    my $bad_insert = { _label => 'a1' };
    
    $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
		     { no_check => 1 });
    
    $T->ok_error_like(qr/E_BAD_RECORD/, "got 'E_BAD_RECORD' with nearly empty record");
    $T->cmp_ok_errors('==', 1, "got one error");

    # Now send one that is clearly a specimen but has missing fields.
    
    $bad_insert = { _label => 'a1',
		    taxon_name => 'Stegosaurus' };
    
    $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
		     { no_check => 1 });

    $T->ok_error_like(qr/E_REQUIRED.*a1.*reference_id/, "got missing reference_id");
    $T->ok_error_like(qr/E_REQUIRED.*specimen_code/, "got missing specimen_code");
    $T->cmp_ok_errors('==', 2, "got 2 errors");
    
    # Now send one that has all required fields but a bad collection_id.
    
    $bad_insert = { _label => 'a1',
		    specimen_code => 'BAD 1',
		    reference_id => 'ref:5041',
		    collection_id => 'col:99999999',
		    taxon_name => 'Stegosaurus' };
    
    $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
		     { no_check => 1 });
    $T->ok_error_like(qr/E_KEY_NOT_FOUND.*collection_id/, "got bad collection_id");
    $T->cmp_ok_errors('==', 1, "got one error");

    # Now send one that has all required fields but a bad occurrence_id.

    $bad_insert = { _label => 'a1',
		    specimen_code => 'BAD 1',
		    reference_id => 'ref:5041',
		    occurrence_id => 'occ:99999999' };
    
    $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
		     { no_check => 1 });
    $T->ok_error_like(qr/E_KEY_NOT_FOUND.*occurrence_id/, "got bad occurrence_id");
    $T->cmp_ok_errors('==', 1, "got one error");
    
    # Now send one with both collection_id and occurrence_id
    
    $bad_insert = { _label => 'a1',
		    specimen_code => 'BAD 1',
		    reference_id => 'ref:5041',
		    occurrence_id => 'occ:123',
		    collection_id => 'col:123' };
    
    $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
		     { no_check => 1 });
    $T->ok_error_like(qr/E_PARAM.*collection_id.*occurrence_id/, "flagged parameter conflict");

    # And one with both taxon_name and taxon_id

    $bad_insert = { _label => 'a1',
		    specimen_code => 'BAD 1',
		    reference_id => 'ref:5041',
		    taxon_id => 'txn:69296',
		    taxon_name => 'Stegosaurus' };
    
    $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
		     { no_check => 1 });
    $T->ok_error_like(qr/E_PARAM.*taxon_name.*taxon_id/, "flagged parameter conflict");
    
    # Now send some records with improper external identifiers.
    
    $bad_insert = { _label => 'a1',
		    specimen_code => 'BAD 1',
		    reference_id => 'col:5041',
		    taxon_name => 'Stegosaurus' };
    
    $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
		     { no_check => 1 });
    $T->ok_error_like(qr/(E_FORMAT|E_PARAM|E_EXTTYPE).*reference_id/, "flagged bad exttype");
    
    $bad_insert = { _label => 'a1',
		    specimen_code => 'BAD 1',
		    collection_id => 'occ:123',
		    taxon_name => 'Stegosaurus' };
    
    $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
		     { no_check => 1 });
    $T->ok_error_like(qr/(E_FORMAT|E_PARAM|E_EXTTYPE).*collection_id/, "flagged bad exttype");
    
    $bad_insert = { _label => 'a1',
		    specimen_code => 'BAD 1',
		    occurrence_id => 'col:123',
		    taxon_name => 'Stegosaurus' };
    
    $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
		     { no_check => 1 });
    $T->ok_error_like(qr/(E_FORMAT|E_PARAM|E_EXTTYPE).*occurrence_id/, "flagged bad exttype");
    
    $bad_insert = { _label => 'a1',
		    specimen_code => 'BAD 1',
		    specelt_id => 'occ:123',
		    taxon_name => 'Stegosaurus' };
    
    $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
		     { no_check => 1 });
    $T->ok_error_like(qr/(E_FORMAT|E_PARAM|E_EXTTYPE).*specelt_id/, "flagged bad exttype");

    # Now the same with misformatted values.
    
    $bad_insert = { _label => 'a1',
		    specimen_code => 'BAD 1',
		    reference_id => 'abcd',
		    taxon_name => 'Stegosaurus' };
    
    $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
		     { no_check => 1 });
    $T->ok_error_like(qr/(E_FORMAT|E_PARAM|E_EXTTYPE).*reference_id/, "flagged misformatted value");
    
    $bad_insert = { _label => 'a1',
		    specimen_code => 'BAD 1',
		    collection_id => 'abcd',
		    taxon_name => 'Stegosaurus' };
    
    $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
		     { no_check => 1 });
    $T->ok_error_like(qr/(E_FORMAT|E_PARAM|E_EXTTYPE).*collection_id/, "flagged misformatted value");
    
    $bad_insert = { _label => 'a1',
		    specimen_code => 'BAD 1',
		    occurrence_id => 'abcd',
		    taxon_name => 'Stegosaurus' };
    
    $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
		     { no_check => 1 });
    $T->ok_error_like(qr/(E_FORMAT|E_PARAM|E_EXTTYPE).*occurrence_id/, "flagged misformatted value");
    
    $bad_insert = { _label => 'a1',
		    specimen_code => 'BAD 1',
		    specelt_id => 'abcd',
		    taxon_name => 'Stegosaurus' };
    
    $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
		     { no_check => 1 });
    $T->ok_error_like(qr/(E_FORMAT|E_PARAM|E_EXTTYPE).*specelt_id/, "flagged misformatted value");

    # Now try bad values for the non-identifier parameters.

    # $bad_insert = { _label => 'a1',
    # 		    specimen_code => 'BAD 1',
    # 		    taxon_name => 'Bad!!!' };

    # $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
    # 		     { no_check => 1 });
    # $T->ok_error_like(qr/(E_FORMAT|E_PARAM|E_EXTTYPE).*taxon_name/, "flagged misformatted value");
    
    
};


# Check that changing a collection id or occurrence id is not allowed.

subtest 'change occurrence' => sub {

    # First fetch a collection.
    
    my ($coll2, $coll1) = $T->fetch_records("/colls/list.json?interval=permian&limit=2",
					    "fetched an arbitrary collection");
    
    my $coll_id = $coll1->{oid};
    
    diag("Fetched collection oid: $coll_id");
    
    # Then add an specimen record linked to that collection.
    
    my $record = { _label => 'a2',
		    specimen_code => 'TEST UPDATE CHECK 1',
		    taxon_id => $TID_2,
		    collection_id => $coll_id,
		    reference_id => 'ref:5041',
		    specimen_side => 'left',
		    sex => 'male',
		    measurement_source => 'Direct',
		    specelt_id => 'els:500' };
    
    my (@r1) = $T->send_records("/specs/addupdate.json", "test update 1", json => $record);
    
    # We can abort this subtest if the addition does not succeed.
    
    return unless @r1;
    
    is( $r1[0]{cid}, $coll_id, "added specimen has proper collection id" );
    is( $r1[0]{tid}, $TID_2, "added specimen has proper taxon id" );
    
    # Now check that we can update the record by presenting the same fields a second time.

    $record->{specimen_id} = $r1[0]{oid};
    $record->{occurrence_id} = $r1[0]{qid};
    
    my (@r2) = $T->send_records("/specs/update.json", "test update 2", json => $record);
    
    # $T->test_mode('debug', 'enable');
    
    # But check that changing the collection_id is rejected.
    
    my $record2 = { %$record, collection_id => $coll2->{oid} };
    
    my ($m3) = $T->send_records("/specs/update.json", "test update 3", json => $record2, { no_check => 1 });
    
    $T->ok_error_like(qr/E_CANNOT_CHANGE/, "got 'E_CANNOT_CHANGE' when attempting to update collection_id");
    
    # Check that changing the occurrence_id is also rejected.

    my $record3 = { %$record, occurrence_id => 'occ:12345' };

    my ($m4) = $T->send_records("/specs/update.json", "test update 4", json => $record3, { no_check => 1 });
    
    $T->ok_error_like(qr/E_CANNOT_CHANGE/, "got 'E_CANNOT_CHANGE' when attempting to update occurrence_id");
    
    # $T->test_mode('debug', 'disable');
};


# Check that other errors are reported properly.

subtest 'other errors' => sub {

    pass('placeholder');

    # check for /specs/update with no id
    # check for bad ids
    # check for 
};
    

subtest 'unknown taxon' => sub {

    pass('placeholder');
    
    my $record2 = { _label => 'a1',
    		    specimen_code => 'TEST.2',
    		    taxon_name => 'Foo (baff) bazz',
    		    reference_id => 'ref:5041',
    		    collection_id => 1003
    		  };
    
    my (@r2) = $T->send_records("/specs/addupdate.json?allow=UNKNOWN_TAXON", "add with unknown taxon",
				json => $record2);

    if ( @r2 )
    {
	like($r2[0]{oid}, qr{^spm:\d+$}, "added record has properly formatted oid") &&
	    diag("New specimen oid: $r2[0]{oid}");
	
	is( $r2[0]{idn}, "Foo (baff) bazz", "added record has proper identified name" );
	is( $r2[0]{cid}, 'col:1003' );
    }
};
