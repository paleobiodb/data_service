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

use Test::More tests => 2;

use lib qw(lib ../lib t);

use Tester;

use Test::Selection;


# If we provided any command-line arguments, run only subtests whose names match.

choose_subtests(@ARGV);

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });

my ($perm_a, $perm_e);

# Then check to MAKE SURE that the server is in test mode and the test timescale tables are
# enabled. This is very important, because we DO NOT WANT to change the data in the main
# tables. If we don't get the proper response back, we need to bail out. These count as the first
# two tests in this file.

$T->test_mode('session_data', 'enable') || BAIL_OUT("could not select test session data");
# $T->test_mode('timescales', 'enable') || BAIL_OUT("could not select test timescale tables");


subtest 'add simple' => sub {
    
    select_subtest || return;
    
    # First, see if a user with the admin privileges can add. If this fails, there is no
    # reason to go any further.
    
    $T->set_cookie("session_id", "SESSION-SUPERUSER");
    
    my $meas1 = { record_label => 'm1',
		  specimen_id => 156947,
		  measurement_type => 'length',
		  max => '1.5 mm',
		  min => '1.2 mm' };

    my (@r1a) = $T->send_records("/specs/addupdate_measurements.json", "add measurement", json => $meas1);

    unless ( @r1a )
    {
	BAIL_OUT("adding a new record failed");
    }
    
    return;
    
    my $record1 = [
	       { record_label => 'a1',
		 specimen_code => 'TEST.1',
		 # taxon_id => '71894',
		 taxon_name => 'Dascillidae',
		 reference_id => 'ref:5041',
		 specimen_side => 'right?',
		 sex => 'male',
		 measurement_source => 'DIrect',
		 specelt_id => 'els:500',
	       },
	       { record_label => 'm1',
		 measurement_type => 'length',
		 average => '2.3 mm'
	       },
		  ];
    
    my (@r1) = $T->send_records("/specs/addupdate.json", "superuser add", json => $record1);
    
    unless ( @r1 )
    {
	BAIL_OUT("adding a new record failed");
    }

    my $oid = $r1[0]{oid};
    
    like($oid, qr{^spm:\d+$}, "added record has properly formatted oid") &&
	diag("New specimen oid: $oid");

    return;
    
    my $record2 = { record_label => 'a1',
		    specimen_code => 'TEST.2',
		    taxon_name => 'Foo (baff) bazz',
		    reference_id => 'ref:5041',
		    collection_id => 1003
		  };
    
    my (@r2) = $T->send_records("/specs/addupdate.json?allow=UNKNOWN_TAXON", "superuser add", json => $record2);
    
    my $oid = @r2 ? $r2[0]{oid} : '';
    
    like($oid, qr{^spm:\d+$}, "added record has properly formatted oid") &&
	diag("New specimen oid: $oid");
    
}
