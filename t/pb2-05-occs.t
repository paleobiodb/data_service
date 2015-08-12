# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test all of the /data1.2/occs operations.
# 

use lib 'lib';

use Test::Most tests => 3;

use JSON;
use Text::CSV_XS;
use CoreFunction qw(connectDB configData);
use Taxonomy;

use lib 't';
use Tester;


# We turn off warnings for uninitialized variables and non-numeric
# comparisons, because we don't want to clutter up our testing code with
# "defined $t1 && $t1->{foo} eq ...

no warnings 'uninitialized';
no warnings 'numeric';


# Create a Tester object with which to fetch URLs and decode the results.

my $T = Tester->new();

$T->set_url_check('/data1.2', qr{^/data1.2});


# Also set up a direct channel to the database, so we can compare what is in
# the database to the results from the data service.

my ($dbh, $taxonomy);

eval {
    $dbh = connectDB("config.yml");
};

unless ( ok(defined $dbh, "dbh acquired") )
{
    diag("message was: $@");
    BAIL_OUT;
}


# Then define the values we will be using to check the results of the data service.

my $NAME1 = 'Dascillidae';
my $TID1 = '69296';

my $OID1 = '1054042';
my $CID1 = '128551';
my $OTX1 = '241265';
my $IDN1 = 'Dascillus shandongianus n. sp.';
my $TNA1 = 'Dascillus shandongianus';
my $INT1 = 'Middle Miocene';

my $OID2 = '154322';	# For testing 'ident'

my ($EAG1, $LAG1);

eval {
    my $sql = "SELECT early_age, late_age FROM interval_data WHERE interval_name = '$INT1'";
    my ($eag, $lag) = $dbh->selectrow_array($sql);
    
    $eag =~ s/0+$//;
    $lag =~ s/0+$//;
    
    $EAG1 = qr{^${eag}0*$};
    $LAG1 = qr{^${lag}0*$};
};

unless ( ok($EAG1 && $LAG1, "found ages for test interval '$INT1'") )
{
    diag("message was: $@");
}

my $OCC1j = {
    oid => "occ:$OID1",
    typ => "occ",
    cid => "col:$CID1",
    idn => $IDN1,
    tna => $TNA1,
    rnk => 3,
    tid => "txn:$OTX1",
    oei => $INT1 };

if ( $EAG1 && $LAG1 )
{
    $OCC1j->{eag} = $EAG1;
    $OCC1j->{lag} = $LAG1;
}

my $OCC1t = {
    occurrence_no => $OID1,
    record_type => "occurrence",
    collection_no => $CID1,
    identified_name => $IDN1,
    identified_rank => 'species',
    identified_no => $OTX1,
    accepted_name => $TNA1,
    accepted_rank => 'species',
    accepted_no => $OTX1,
    early_interval => $INT1,
    late_interval => $INT1,
    reference_no => qr{^\d+$} };

if ( $EAG1 && $LAG1 )
{
    $OCC1t->{max_ma} = $EAG1;
    $OCC1t->{min_ma} = $LAG1;
}


my $TEST_NAME_2 = 'Dascilloidea';

my $TEST_NAME_3 = 'Felidae';
my $TEST_NAME_3a = 'Felinae';
my $TEST_NAME_3b = 'Pantherinae';
my $TEST_NAME_3c = 'Felis catus';
my $TEST_NAME_3P = 'Aeluroidea';

my $TEST_NAME_4 = 'Canidae';
my $TEST_NAME_4P = 'Canoidea';

my $TEST_NAME_COMMON = 'Carnivora';

my $TEST_NAME_5 = 'Caviidae';

my $TEST_NAME_6 = 'Tyrannosauridae';

my $LIMIT_1 = 5;
my $OFFSET_1 = 4;

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
    
    my ($response, $r);
    
    eval {
	$response = decode_json( $single_json->content );
	$r = $response->{records}[0];
    };
    
    ok( ref $r eq 'HASH' && keys %$r, 'single json content decoded') or return;
    
    $T->check_fields($r, $OCC1j, 'single occ');
};


# subtest 'single txt by name' => sub {
    
#     my $single_txt = $T->fetch_url("/data1.2/taxa/single.txt?name=$TEST_NAME_1&show=attr,app,size,phylo",
# 				   "single txt request OK");
    
#     unless ( $single_txt )
#     {
# 	diag("skipping remainder of subtest");
# 	return;
#     }
    
#     # Now check the txt response in detail 
    
#     my ($r) = $T->extract_records($single_txt, 'single txt extract records', { type => 'header' } );
    
#     # Check the data fields
    
#     $T->check_fields($r, $t1t, "single txt");
# };


# Now we check for a request using the 'id' parameter, with the value
# retrieved from the first request.  We also fetch the parent of our test
# taxon, and make sure that it gets retrieved correctly as well.

# subtest 'single json by id' => sub {
    
#     my $response = $T->fetch_url("/data1.2/taxa/single.json?id=$taxon_id",
# 				 "single json by id request OK");
    
#     unless ( $response )
#     {
# 	diag("skipping remainder of subtest");
# 	return;
#     }
    
#     my ($r) = $T->extract_records($response, "single json by id extract records");
    
#     return unless $r;
    
#     my $taxon_name = $r->{nam};
    
#     ok( defined $taxon_name, 'single json by id taxon name' ) or return;
#     is( $taxon_name, $TEST_NAME_1, "single json by id retrieves proper record" );
# };


