# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test data entry and editing for specimens.
# 

use strict;

use feature 'unicode_strings';
use feature 'fc';

use Test::More tests => 9;

use lib qw(lib ../lib t);

use CoreFunction qw(connectDB configData);
use CoreTableDefs;

use Tester;
use EditTester;

use Data::Dumper::Concise;


# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });

# Then check to MAKE SURE that the server is in test mode and the test timescale tables are
# enabled. This is very important, because we DO NOT WANT to change the data in the main
# tables. If we don't get the proper response back, we need to bail out. These count as the first
# two tests in this file.

$T->test_mode('session_data', 'enable') || BAIL_OUT("could not select test session data");
$T->test_mode('specimen_data', 'enable') || BAIL_OUT("could not select test specimen tables");

# Finally, grab a database connection and check to make sure that we are actually looking at the
# test tables. That will give us confidence to clear the tables between tests.

my $ET = EditTester->new;

$ET->start_test_mode('specimen_data') || BAIL_OUT "could not select test specimen tables locally";


# Check that we can add records, and that the returned records contain proper identifiers and
# labels. If record addition fails, we bail out of the entire test, because there is no point in
# continuing.

subtest 'add simple' => sub {

    # Safely clear the SPECIMEN_DATA and MEASUREMENT_DATA tables.
    
    $ET->safe_clear_table('MEASUREMENT_DATA', 'enterer_no', 'SPECIMEN_DATA', 'specimen_no');
    $ET->safe_clear_table('SPECIMEN_DATA', 'enterer_no');
    $ET->safe_clear_table('SPECIMEN_MATRIX', 'enterer_no');
    
    # Next, see if a user with the admin privileges can add. If this fails, there is no
    # reason to go any further.
    
    $T->set_cookie("session_id", "SESSION-SUPERUSER");
    
    # my (@r1a) = $T->send_records("/specs/addupdate_measurements.json", "add measurement", json => $meas1);
    
    # unless ( @r1a )
    # {
    # 	BAIL_OUT("adding a new record failed");
    # }
    
    # return;
    
    my $record1 = [
	       { _label => 'a1',
		 specimen_code => 'TEST.1',
		 # taxon_id => '71894',
		 taxon_name => 'Dascillidae',
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
    
    my (@r1) = $T->send_records("/specs/addupdate.json", "superuser add", json => $record1);
    
    unless ( @r1 )
    {
	BAIL_OUT("adding new records failed");
    }
    
    like($r1[0]{oid}, qr{^spm:\d+$}, "added specimen record has properly formatted oid") &&
	diag("New specimen oid: $r1[0]{oid}");
    is($r1[0]{rlb}, 'a1', "added specimen record has proper label");
    
    # print STDERR $T->{last_response}->content;
    
    like($r1[1]{oid}, qr{^mea:\d+$}, "added measurement record has properly formatted oid") &&
    	diag("New measurement oid: $r1[1]{oid}");
    is($r1[1]{rlb}, "m1", "added measurement record has proper label");

    # Check the inserted records against the output of the data retrieval operation.
    
    my ($authorizer_no, $enterer_no) = $ET->get_session_authinfo('SESSION-SUPERUSER');
    
    my (@check1) = $T->fetch_records("/specs/list.json?all_records&specs_entered_by=$enterer_no",
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
	
};


# Check the various formats allowed for the request body. The first format, an array of object
# records, was already checked above.

subtest 'request body' => sub {
    
    $T->set_cookie("session_id", "SESSION-ENTERER");

    # Try a single object record.
    
    my $format2 = { _label => 'a1',
		    specimen_code => 'TEST FORMAT 2',
		    taxon_name => 'Dascillidae',
		    reference_id => 'ref:5041',
		    specimen_side => 'right?',
		    sex => 'male',
		    measurement_source => 'Direct',
		    specelt_id => 'els:500' };
    
    my (@r2) = $T->send_records("/specs/addupdate.json", "superuser add 2", json => $format2);
    
    cmp_ok( @r2, '==', 1, "inserted one record with single record request body" ) &&
	is( $r2[0]{smi}, 'TEST FORMAT 2' );

    # Now try a list of object records with some common parameters.
    
    my $format3 = { all => { taxon_name => 'Dascillidae' },
		    records => [{ _label => 'a2',
				  specimen_code => 'TEST FORMAT 3',
				  taxon_name => 'Dascillidae',
				  reference_id => 'ref:5041',
				  specimen_side => 'right?',
				  sex => 'male',
				  measurement_source => 'Direct',
				  specelt_id => 'els:500' }] };
    
    my (@r3) = $T->send_records("/specs/addupdate.json", "superuser add 3", json => $format3);
    
    cmp_ok( @r3, '==', 1, "inserted one record with single record request body" ) &&
	is( $r3[0]{smi}, 'TEST FORMAT 3' );
    
    # Now try a list of form fields to be URL encoded. This will simulate an insertion from a web
    # form.

    # my $format4 = [ _label => 'a2',
    # 		    specimen_code => 'TEST FORMAT 4',
    # 		    taxon_name => 'Dascillidae',
    # 		    reference_id => 'ref:5041',
    # 		    specimen_side => 'right?',
    # 		    sex => 'male',
    # 		    measurement_source => 'Direct',
    # 		    specelt_id => 'els:500' ];
    
    # my (@r4) = $T->send_records("/specs/addupdate.json", "superuser add 4", form => $format4);
    
    # cmp_ok( @r4, '==', 1, "inserted one record with form body" ) &&
    # 	is( $r4[0]{smi}, 'TEST FORMAT 4' );
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
		   taxon_name => 'Dascillidae',
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
		   taxon_name => 'Dascillidae',
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
    
    cmp_ok( @meas, '==', 3, "inserted three measurement records" );
    
    if ( cmp_ok( @spec, '==', 2, "inserted two specimen records" ) )
    {
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
    }
    
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
		   taxon_name => 'Dascillidae',
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
		   taxon_name => 'Dascillidae',
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


# Check that insertions and updates with bad values are caught and properly reported.

subtest 'bad values' => sub {
    
    $T->set_cookie("session_id", "SESSION-ENTERER");
    
    my $bad_insert = { _label => 'a1',
			specimen_code => 'BAD.a1',
			taxon_name => 'Stegosaurus' };
    
    my (@r1) = $T->send_records("/specs/addupdate.json", "add records", json => $bad_insert,
			        { no_check => 1 });
    
    

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
    
    # my $record2 = { _label => 'a1',
    # 		    specimen_code => 'TEST.2',
    # 		    taxon_name => 'Foo (baff) bazz',
    # 		    reference_id => 'ref:5041',
    # 		    collection_id => 1003
    # 		  };
    
    # my (@r2) = $T->send_records("/specs/addupdate.json?allow=UNKNOWN_TAXON", "superuser add", json => $record2);
    
    # my $oid = @r2 ? $r2[0]{oid} : '';
    
    # like($oid, qr{^spm:\d+$}, "added record has properly formatted oid") &&
    # 	diag("New specimen oid: $oid");
    

};
