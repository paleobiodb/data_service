# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test all of the /data1.2/occs operations.
# 

use strict;

use feature 'unicode_strings';
use feature 'fc';

use Test::Most tests => 5;

# use CoreFunction qw(connectDB configData);
# use Taxonomy;

use lib 't';
use Tester;
use Test::Conditions;


# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });



# Also set up a direct channel to the database, so we can compare what is in
# the database to the results from the data service.

# my ($dbh, $taxonomy);

# eval {
#     $dbh = connectDB("config.yml");
# };

# unless ( ok(defined $dbh, "dbh acquired") )
# {
#     diag("message was: $@");
#     BAIL_OUT;
# }


# Then define the values we will be using to check the results of the data service.

# my $NAME1 = 'Dascillidae';
# my $TID1 = '69296';

# my $OID1 = '1054042';
# my $CID1 = '128551';
# my $OTX1 = '241265';
# my $IDN1 = 'Dascillus shandongianus n. sp.';
# my $TNA1 = 'Dascillus shandongianus';
# my $INT1 = 'Middle Miocene';

# my $OID2 = '1054041';

# # my $OID2 = '154322';	# For testing 'ident'

# my ($EAG1, $LAG1);

# eval {
#     my $sql = "SELECT early_age, late_age FROM interval_data WHERE interval_name = '$INT1'";
#     my ($eag, $lag) = $dbh->selectrow_array($sql);
    
#     $eag =~ s/0+$//;
#     $lag =~ s/0+$//;
    
#     $EAG1 = qr{^${eag}0*$};
#     $LAG1 = qr{^${lag}0*$};
# };

# unless ( ok($EAG1 && $LAG1, "found ages for test interval '$INT1'") )
# {
#     diag("message was: $@");
# }

# my $OCC1j = {
#     oid => "occ:$OID1",
#     typ => "occ",
#     cid => "col:$CID1",
#     idn => $IDN1,
#     tna => $TNA1,
#     rnk => 3,
#     tid => "txn:$OTX1",
#     oei => $INT1 };

# if ( $EAG1 && $LAG1 )
# {
#     $OCC1j->{eag} = $EAG1;
#     $OCC1j->{lag} = $LAG1;
# }

# my $OCC1t = {
#     occurrence_no => $OID1,
#     record_type => "occ",
#     collection_no => $CID1,
#     identified_name => $IDN1,
#     identified_rank => 'species',
#     identified_no => $OTX1,
#     accepted_name => $TNA1,
#     accepted_rank => 'species',
#     accepted_no => $OTX1,
#     early_interval => $INT1,
#     reference_no => qr{^\d+$} };

# if ( $EAG1 && $LAG1 )
# {
#     $OCC1t->{max_ma} = $EAG1;
#     $OCC1t->{min_ma} = $LAG1;
# }


# my $TEST_NAME_2 = 'Dascilloidea';

# my $TEST_NAME_3 = 'Felidae';
# my $TEST_NAME_3a = 'Felinae';
# my $TEST_NAME_3b = 'Pantherinae';
# my $TEST_NAME_3c = 'Felis catus';
# my $TEST_NAME_3P = 'Aeluroidea';

# my $TEST_NAME_4 = 'Canidae';
# my $TEST_NAME_4P = 'Canoidea';

# my $TEST_NAME_COMMON = 'Carnivora';

# my $TEST_NAME_5 = 'Caviidae';

# my $TEST_NAME_6 = 'Tyrannosauridae';

# my $LIMIT_1 = 5;
# my $OFFSET_1 = 4;


# We start by checking the basic 'single occurrence' response, in both .json
# and .txt formats.  We will assume that if .txt works okay then .csv and .tsv
# do too, because that is handled by Web::DataService and has already been
# tested by earlier files in this series.

subtest 'single json by id' => sub {
    
    my $single_json = $T->fetch_url("/data1.2/occs/single.json?id=$OID1&show=phylo",
				    "single json request OK");
    
    unless ( $single_json )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Check the json response in detail
    
    my (@r) = $T->extract_records($single_json, 'single json by id' );
    
    ok( ref $r[0] eq 'HASH', 'single json content decoded') or return;
    
    $T->check_fields($r[0], $OCC1j, 'single occ json');
};


subtest 'single text by id' => sub {
    
    my $single_txt = $T->fetch_url("/data1.2/occs/single.txt?id=$OID1&show=phylo",
				    "single txt request OK");
    
    unless ( $single_txt )
    {
	diag("skipping remainder of subtest");
	return;
    }

    # Check the txt response in detail
    
    my (@r) = $T->extract_records($single_txt, 'single txt by id' );
    
    ok( ref $r[0] eq 'HASH', 'single txt content decoded') or return;
    
    $T->check_fields($r[0], $OCC1t, 'single occ txt');
};


subtest 'list json by id' => sub {

    my $list_json = $T->fetch_url("/data1.2/occs/list.json?id=$OID1,$OID2",
				    "single json request OK");
    
    unless ( $list_json )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Check the json response to make sure we have two records with the proper oids.
    
    my (@r) = $T->extract_records($list_json, 'list json by id' );
    
    my %found_occ;
    
    foreach my $r ( @r )
    {
	$found_occ{$r->{oid}} = 1;
    }
    
    ok( $found_occ{"occ:$OID1"}, "found occ 1" );
    ok( $found_occ{"occ:$OID2"}, "found occ 2" );
};
