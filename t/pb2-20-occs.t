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

use Test::Most tests => 1;

# use CoreFunction qw(connectDB configData);
# use Taxonomy;

use lib 'lib', '../lib';

use TaxonDefs;

use lib 't';
use Tester;
use Test::Conditions;


# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });


my %INTERVAL_NAME;


# Now try listing occurrences from a couple of taxa.  Check for basic consistency.

subtest 'subtree basic' => sub {
    
    my $NAME_1 = 'Canis';
    
    my @o1 = $T->fetch_records("/occs/list.json?base_name=$NAME_1", "occs list json '$NAME_1'");
    
    unless ( @o1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my %tid1 = $T->fetch_record_values("/taxa/list.json?base_name=$NAME_1", 'oid', "taxa oids '$NAME_1'");
    
    my %o_tid1 = $T->extract_values( \@o1, 'tid' );
    
    $T->cmp_sets_ok( \%o_tid1, '<=', \%tid1, "occ tids match taxa list" );
    
    my %o_tdf1 = $T->extract_values( \@o1, 'tdf' );
    
    my %test1 = ( 'species not entered' => 1, 'subjective synonym of' => 1,
		 'obsolete variant of' => 1 );
    
    $T->cmp_sets_ok( \%o_tdf1, '>=', \%test1, "found sample 'tdf' values" );
    
    my %tna1 = $T->fetch_record_values("/taxa/list.json?base_name=$NAME_1", 'nam', "taxa names '$NAME_1'");
    
    my %o_tna1 = $T->extract_values( \@o1, 'tna' );
    
    $T->cmp_sets_ok( \%o_tna1, '<=', \%tna1, "occ taxa names match taxa list" );
    
    %INTERVAL_NAME = $T->fetch_record_values("/intervals/list.json?all_records", 'nam', "interval names");
    
    my $tc = Test::Conditions->new;
    
    foreach my $o ( @o1 )
    {
	my $oid = $o->{oid};
	
	$tc->flag('oid', 'MISSING VALUE') unless $oid && $oid =~ /^occ:\d+$/;
	$tc->flag('typ', $oid) unless $o->{typ} && $o->{typ} eq 'occ';
	$tc->flag('cid', $oid) unless $o->{cid} && $o->{cid} =~ /^col:\d+$/;
	$tc->flag('eid', $oid) if $o->{eid} && $o->{eid} !~ /^rei:\d+$/;
	$tc->flag('tna', $oid) unless $o->{tna} =~ /\w/;
	$tc->flag('rnk', $oid) unless $o->{rnk} && $o->{rnk} =~ /^\d+$/;
	$tc->flag('tna/rnk', $oid) if $o->{rnk} && $o->{rnk} < 5 && $o->{tna} !~ /\w [\w(]/;
	$tc->flag('oei', $oid) unless $o->{oei} && $INTERVAL_NAME{$o->{oei}};
	$tc->flag('eag', $oid) unless defined $o->{eag} && $o->{eag} =~ /^\d+$|^\d+[.]\d+$/;
	$tc->flag('lag', $oid) unless defined $o->{lag} && $o->{lag} =~ /^\d+$|^\d+[.]\d+$/;
	$tc->flag('rid', $oid) unless $o->{rid} && $o->{rid} =~ /^ref:\d+$/;
	$tc->flag('iid', $oid) if $o->{iid} && $o->{iid} !~ /^(var|txn):\d+$/;
	$tc->flag('iid/idn', $oid) if $o->{iid} && ! ( $o->{idn} && $o->{idn} =~ /\w/ );
	
	$tc->flag('nsp', $oid) if $o->{idn} && $o->{idn} =~ /n\. sp\./;
	$tc->flag('ngen', $oid) if $o->{idn} && $o->{idn} =~ /n\. gen\./;
	$tc->flag('cf', $oid) if $o->{idn} && $o->{idn} =~ /cf\./;
    }
    
    $tc->expect('nsp', 'ngen', 'cf');
    
    $tc->ok_all("json records have proper values");
    
    my @o2 = $T->fetch_records("/occs/list.txt?base_name=$NAME_1", "occs list txt '$NAME_1'");
    
    if ( cmp_ok( @o2, '==', @o1, "occs list txt fetches same number of records" ) )
    {
	foreach my $i ( 0..$#o2 )
	{
	    my $o1 = $o1[$i];
	    my $o2 = $o2[$i];
	    my $oid = $o2->{occurrence_no};
	    
	    $tc->flag('occurrence_no', $oid) unless $o2->{occurrence_no} &&
		"occ:" . $o2->{occurrence_no} eq $o1->{oid};
	    $tc->flag('collection_no', $oid) unless $o2->{collection_no} &&
		"col:" . $o2->{collection_no} eq $o1->{cid};
	    $tc->flag('reference_no', $oid) unless $o2->{reference_no} &&
		"ref:" . $o2->{reference_no} eq $o1->{rid};
	    $tc->flag('record_type', $oid) unless $o2->{record_type} && $o2->{record_type} eq 'occ';
	    
	    $tc->flag('reid_no/eid', $oid) if $o2->{reid_no} xor $o1->{eid};
	    
	    $tc->flag('reid_no', $oid) if $o2->{reid_no} && $o1->{eid} &&
		"rei:" . $o2->{reid_no} ne $o1->{eid};
	    $tc->flag('accepted_no', $oid) unless $o2->{accepted_no} &&
		"txn:" . $o2->{accepted_no} eq $o1->{tid};
	    $tc->flag('identified_no', $oid) if $o2->{identified_no} && $o1->{iid} &&
		not("txn:" . $o2->{identified_no} eq $o1->{iid} || "var:" . $o2->{identified_no} eq $o1->{iid});
	    
	    $tc->flag('difference', $oid) if $o2->{difference} ne ( $o1->{tdf} || '' );
	    
	    $tc->flag('accepted_name', $oid) if $o2->{accepted_name} ne $o1->{tna};
	    $tc->flag('identified_name/idn', $oid) if $o2->{identified_name} ne ( $o1->{idn} || $o1->{tna} );
	    
	    $tc->flag('accepted_rank', $oid) if $o2->{accepted_rank} ne $TaxonDefs::RANK_STRING{$o1->{rnk}};
	    $tc->flag('identified_rank', $oid) if $o2->{identified_rank} ne $o2->{accepted_rank} &&
		$o2->{identified_rank} ne ($TaxonDefs::RANK_STRING{$o1->{idr}} || '');
	    
	    $tc->flag('early_interval', $oid) if $o2->{early_interval} ne $o1->{oei};
	    $tc->flag('late_interval', $oid) if $o2->{late_interval} ne $o2->{early_interval} && 
		$o2->{late_interval} ne ( $o1->{oli} || '' );
	    $tc->flag('max_ma', $oid) if $o2->{max_ma} ne $o1->{eag};
	    $tc->flag('min_ma', $oid) if $o2->{min_ma} ne $o1->{lag};
	}
    }
    
    $tc->ok_all("txt records have proper values");
};




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

# subtest 'single json by id' => sub {
    
#     my $OID1 = '1054042';
    
#     my (@o1) = $T->fetch_records("/data1.2/occs/single.json?id=$OID1&show=phylo",
# 				 "single json request OK");
    
#     unless ( @o1 )
#     {
# 	diag("skipping remainder of subtest");
# 	return;
#     }
    
#     cmp_ok( @o1, '==', 1, "single json got one record" );
    
#     # $T->check_fields($r[0], $OCC1j, 'single occ json');
# };


# subtest 'single text by id' => sub {
    
#     my $OID1 = '1054042';
    
#     my (@o1) = $T->fetch_url("/data1.2/occs/single.txt?id=$OID1&show=phylo",
# 			     "single txt request OK");
    
#     unless ( @o1 )
#     {
# 	diag("skipping remainder of subtest");
# 	return;
#     }

#     cmp_ok( @o1, '==', 1, "single txt got one record" );
    
#     # $T->check_fields($r[0], $OCC1t, 'single occ txt');
# };


# subtest 'list json by id' => sub {

#     my $OID1 = '1054042';
#     my $OID2 = '154322';
    
#     my (@o1) = $T->fetch_url("/data1.2/occs/list.json?id=$OID1,$OID2",
# 			     "list two records request OK");
    
#     unless ( @o1 == 2 )
#     {
# 	diag("skipping remainder of subtest");
# 	return;
#     }
    
#     cmp_ok( @o1, '==', 2, "list two records got two records" );
    
#     # Check the json response to make sure we have two records with the proper oids.
    
#     my %found_occ = ( $o1[0]{oid} => 1, $o1[1]{oid} => 2 );
    
#     ok( $found_occ{"occ:$OID1"}, "found occ 1" );
#     ok( $found_occ{"occ:$OID2"}, "found occ 2" );
# };


