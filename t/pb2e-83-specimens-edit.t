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

use Test::More tests => 1;

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

# $T->test_mode('specimen_data', 'enable') || BAIL_OUT("could not select test session data");
# $T->test_mode('timescales', 'enable') || BAIL_OUT("could not select test timescale tables");


subtest 'add simple' => sub {
    
    select_subtest || return;
    
    # First, see if a user with the admin privileges can add. If this fails, there is no
    # reason to go any further.
    
    $T->set_cookie("session_id", "SESSION-SUPERUSER");
    
    my $record1 = { record_label => 'a1',
		    specimen_code => 'TEST.1',
		    # taxon_id => '71894',
		    taxon_name => 'Dascillidae',
		    reference_id => 'ref:5041'
		  };
    
    my (@r1) = $T->send_records("/specs/addupdate.json", "superuser add", json => $record1);
    
    unless ( @r1 )
    {
	BAIL_OUT("adding a new record failed");
    }
    
    like($r1[0]{oid}, qr{^spm:\d+$}, "added record has properly formatted oid");
}
