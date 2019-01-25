# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test data entry and editing for timescales.
# 

use strict;

use feature 'unicode_strings';
use feature 'fc';

use Data::Dump qw(pp);

use Test::More tests => 7;

use lib qw(lib ../lib t);

use Tester;
use EditTester;

use TableDefs qw(%TABLE);
use TimescaleDefs;
use ExternalIdent qw(%IDRE);

use Test::Selection;
use Test::Conditions;


# If we provided any command-line arguments, run only subtests whose names match.

choose_subtests(@ARGV);

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });

# Then check to MAKE SURE that the server is in test mode and the test timescale tables are
# enabled. This is very important, because we DO NOT WANT to change the data in the main
# tables. If we don't get the proper response back, we need to bail out. These count as the first
# two tests in this file.

$T->test_mode('session_data', 'enable') || BAIL_OUT "could not select test session data";
$T->test_mode('timescale_data', 'enable') || BAIL_OUT "could not select test timescale tables";

# Then grab a database connection and check to make sure that we are actually looking at the
# test tables. That will give us confidence to clear the tables between tests.

my $ET = EditTester->new;

$ET->start_test_mode('timescale_data') || BAIL_OUT "could not select test timescale tables locally";

# The following variables save state that is necessary for proper cleanup.

my $TS1;
my $ALBIAN;
my $APTIAN;



# Start by checking that we can actually add a new timescale using admin privileges. If this
# fails, there is no point in continuing with the test.

subtest 'basic' => sub {
    
    # First, see if a user with the superuser privileges can add. If this fails, there is no
    # reason to go any further.
    
    $T->set_cookie("session_id", "SESSION-WITH-ADMIN");
    
    my $new_js = [ { timescale_name => 'EDIT TEST A' } ];
    
    my (@t1) = $T->send_records("/timescales/addupdate.json", "add test timescale",
				json => $new_js);
    
    unless ( $t1[0]{oid} )
    {
	BAIL_OUT("cannot add new timescale");
    }
    
    # save the id of the newly created timescale for later use.

    $TS1 = $t1[0]{oid};

    # Fetch some basic interval bounds for use in subsequent tests.
    
    ($ALBIAN) = $T->fetch_records("/timescales/intervals.json?interval_name=Albian",
				  "fetch interval record for 'Albian'");
    ($APTIAN) = $T->fetch_records("/timescales/intervals.json?interval_name=Aptian",
				  "fetch interval record for 'Aptian'");
    
    # Make sure that the test tables have the same schema as the main ones.
    
    $ET->check_test_schema('TIMESCALE_DATA');
    $ET->check_test_schema('TIMESCALE_BOUNDS');
    $ET->check_test_schema('TIMESCALE_INTS');
};


# Now check that we can add, update, and delete both timescales and bounds.

subtest 'add, update, delete' => sub {

    select_subtest || return;
    
    # First add a timescale and some bounds.
    
    my $new_js = [ { timescale_name => 'EDIT TEST B', _label => 'a1' },
		   { timescale_id => '@a1', bound_type => 'absolute', age => '100.0' },
		   { timescale_id => '@a1', bound_type => 'absolute', age => '200.0' } ];
    
    my (@t1) = $T->send_records("/timescales/addupdate.json", "add timescale and bounds",
				json => $new_js);
    
    return unless @t1;

    cmp_ok( @t1, '==', 3, "got three records back from add" );
    
    like( $t1[0]{oid}, $IDRE{TSC}, "found proper timescale id in first record" );
    like( $t1[1]{oid}, $IDRE{BND}, "found proper bound id in second record" );
    like( $t1[2]{oid}, $IDRE{BND}, "found proper bound id in third record" );
    
    is( $t1[1]{sid}, $t1[0]{oid}, "timescale id matches second record" );
    is( $t1[2]{sid}, $t1[0]{oid}, "timescale id matches third record" );
    
    is( $t1[0]{nam}, "EDIT TEST B", "got correct timescale name" );
    is( $t1[1]{age}, "100.0", "got correct age in second record" );
    is( $t1[2]{age}, "200.0", "got correct age in third record" );
    is( $t1[1]{btp}, "absolute", "got correct bound type in second record" );
    is( $t1[2]{btp}, "absolute", "got correct bound type in third record" );

    # Check that the bounds actually made it into the database.

    my $timescale_no;
    
    if ( $t1[0]{oid} =~ /(\d+)/ )
    {
	$timescale_no = $1;
	
	$ET->ok_count_records(2, 'TIMESCALE_BOUNDS', "timescale_no=$timescale_no");
    }
    
    # Now update the timescale and bound records.

    return unless $t1[0]{oid} =~ $IDRE{TSC} && $t1[1]{oid} =~ $IDRE{BND};
    
    my $update_js = [ { timescale_id => $t1[0]{oid}, timescale_type => 'stage' },
		      { bound_id => $t1[1]{oid}, bound_type => 'spike' },
		      { bound_id => $t1[2]{oid}, top_id => $t1[1]{oid} } ];

    my @t2 = $T->send_records("/timescales/addupdate.json?show=desc", "update timescale and bounds",
			      json => $update_js);
    
    # print STDERR pp(\@t2);
    
    cmp_ok( @t2, '==', 3, "got three records back from update" );
    
    foreach my $i ( 0..2 )
    {
	is( $t2[$i]{oid}, $t1[$i]{oid}, "got proper oid from record $i" );
    }
    
    is( $t2[0]{typ}, 'stage', "timescale type updated properly" );
    is( $t2[1]{btp}, 'spike', "bound type updated properly" );
    is( $t2[2]{uid}, $t2[1]{oid}, "top id updated properly" );

    # Now try deleting a bound record.
    
    my @t3 = $T->fetch_records("/timescales/delete.json?bound_id=$t2[1]{oid}", "delete one bound");
    
    cmp_ok( @t3, '==', 1, "got one record back from delete bound" );
    
    is( $t3[0]{oid}, $t2[1]{oid}, "deleted proper bound" );
    is( $t3[0]{sta}, 'deleted', "got proper status from deletion" );

    my @t4 = $T->fetch_records("/timescales/bounds.json?timescale_id=$t1[0]{oid}", "fetch undeleted bounds");

    cmp_ok( @t4, '==', 1, "one bound remains for timescale '$t1[0]{oid}'" );

    # Now try deleting a timescale record using the _operation field in a body record.
    
    my $delete_js = [ { timescale_id => $t1[0]{oid}, _operation => 'delete' } ];

    my @t5 = $T->send_records("/timescales/addupdate.json", "delete timescale from body record",
			      json => $delete_js);

    cmp_ok( @t5, '==', 1, "got one record back from delete timescale" );

    # Now check that the remaining bound was deleted as well.

    if ( $timescale_no )
    {
	$ET->ok_count_records(0, 'TIMESCALE_BOUNDS', "timescale_no=$timescale_no");
    }
};


# Create two timescales and set the bounds of one to depend on the other. Then update the first
# and make sure that the second updates accordingly. Then try to delete the first one, both with
# and without the appropriate allowance.

subtest 'dependencies' => sub {

    select_subtest || return;
    
    # First add a timescale and some bounds.
    
    my $new_js = [
	      { timescale_name => 'EDIT TEST C', _label => 'a1' },
	      { timescale_id => '@a1', bound_type => 'absolute', age => '48.2', _label => 'a2' },
	      { timescale_id => '@a1', bound_type => 'absolute', age => '100.0', ger => '1.2',
	        top_id => '@a2' },
	      { timescale_id => '@a1', bound_type => 'absolute', age => '200.0' },
	      { timescale_id => '@a1', bound_type => 'spike', age => '300.0' },
	      { timescale_id => '@a1', bound_type => 'absolute', age => '400.0' } ];
    
    my (@t1) = $T->send_records("/timescales/addupdate.json", "add timescale and bounds",
				json => $new_js);
    
    return unless @t1;
    
    cmp_ok( @t1, '==', 6, "got six records back from add" );

    my $ts1 = $t1[0]{oid};
    my $bound1 = $t1[2]{oid};
    my $bound2 = $t1[3]{oid};
    my $bound3 = $t1[4]{oid};
    my $bound4 = $t1[5]{oid};
    
    like( $bound1, $IDRE{BND}, "bound 1 okay" );
    like( $bound2, $IDRE{BND}, "bound 2 okay" );
    like( $bound3, $IDRE{BND}, "bound 3 okay" );
    like( $bound4, $IDRE{BND}, "bound 4 okay" );
    
    # Then add a second timescale with bound dependencies.
    
    my $new_js2 = [
	       { timescale_name => 'EDIT TEST D', _label => 'b1' },
	       { timescale_id => '@b1', bound_type => 'absolute', age => '50.10', _label => 'b2' },
	       { timescale_id => '@b1', bound_type => 'same', base_id => $bound1, color_id => $bound1,
		 age => '110.0', interval_name => 'dep1', top_id => '@b2', _label => 'b3' },
	       { timescale_id => '@b1', bound_type => 'fraction', base_id => $bound2,
		 range_id => $bound1, age => '150.0', ger => '1.3', top_id => '@b3',
		 interval_name => 'dep2', _label => 'b4' },
	       { timescale_id => '@b1', bound_type => 'same', base_id => $bound3,
		 color_id => $bound3, interval_name => 'dep3', top_id => '@b4', _label => 'b5' },
	       { timescale_id => '@b1', bound_type => 'fraction', base_id => $bound4,
		 range_id => $bound3, age => '325', interval_name => 'dep4',
		 top_id => '@b5', _label => 'b6' } ];
    
    my (@t2) = $T->send_records("/timescales/addupdate.json", "add second timescale and bounds",
				json => $new_js2);

    return unless @t2;
    
    cmp_ok( @t2, '==', 6, "got six records back from second add" );

    my $ts2 = $t2[0]{oid};
    my $dep1 = $t2[2]{oid};
    my $dep2 = $t2[3]{oid};
    my $dep3 = $t2[4]{oid};
    my $dep4 = $t2[5]{oid};
    
    is( $t2[2]{age}, '100.0', "bound 1 got dependent age" );
    is( $t2[2]{ger}, '1.2', "bound 1 got dependent age error" );
    is( $t2[2]{btp}, 'same', "bound 1 got proper bound type" );
    is( $t2[2]{bid}, $bound1, "bound 1 got proper base id" );
    is( $t2[2]{cid}, $bound1, "bound 1 got proper color id" );
    is( $t2[2]{uid}, $t2[1]{oid}, "bound 1 got proper top id" );
    ok( ! $t2[2]{spk}, "bound 1 not spike" );
    
    is( $t2[3]{age}, '150.0', "bound 2 got age that was set, with proper precision" );
    is( $t2[3]{frc}, '0.5', "bound 2 fraction was calculated properly" );
    is( $t2[3]{ger}, '1.3', "bound 2 got age error that was set" );
    is( $t2[3]{btp}, 'fraction', "bound 2 got proper bound type" );
    is( $t2[3]{bid}, $bound2, "bound 2 got proper base id" );
    is( $t2[3]{tid}, $bound1, "bound 2 got proper range id" );
    ok( ! $t2[3]{spk}, "bound 2 not spike" );
    
    is( $t2[4]{age}, '300.0', "bound 3 got dependent age" );
    ok( $t2[4]{spk}, "bound 3 is spike" );
    is( $t2[4]{btp}, 'same', "bound 3 got proper bound type" );
    is( $t2[4]{bid}, $bound3, "bound 3 got proper base id" );
    ok( ! $t2[4]{tid}, "bound 3 no range id" );
    
    is( $t2[5]{age}, '325', "bound 4 got age that was set, with proper precision" );
    is( $t2[5]{frc}, '0.75', "bound 4 fraction was calculated properly" );
    
    # Now we update the base bounds and check that the dependent ones update properly. Check
    # colors, too.

    unless ( $ALBIAN )
    {
	fail("Fetch of interval record for 'Albian' succeeded in 'basic' subtest");
	return;
    }
    
    my $update_js = [
		 { bound_id => $bound1, bound_type => 'same', base_id => $ALBIAN->{bid},
		   color_id => $ALBIAN->{bid}, interval_name => 'abc' } ];
    
    my (@t3) = $T->send_records("/timescales/addupdate.json?return=updated", "update bound, point to 'Albian'",
				json => $update_js);

    return unless @t3;
    
    cmp_ok(@t3, '==', 1, "got one record back from update");
    is($t3[0]{age}, $ALBIAN->{eag}, "updated bound age was set properly");
    is($t3[0]{col}, $ALBIAN->{col}, "updated bound color was set properly");
    is($t3[0]{inm}, 'abc', "updated bound interval name was set properly");
    
    my (@t4) = $T->fetch_records("/timescales/bounds.json?bound_id=$dep1", "fetch dependent bound");
    
    is($t4[0]{age}, $ALBIAN->{eag}, "age of second bound in chain was updated properly");
    is($t4[0]{col}, $ALBIAN->{col}, "color of second bound in chain was updated properly");
    is($t4[0]{inm}, $t2[2]{inm}, "interval name of second bound in chain did not change");

    # Now try deleting a bound that has dependencies. This should fail.
    
    $T->fetch_nocheck("/timescales/delete.json?bound_id=$bound1", "delete expecting caution");
    $T->ok_response_code("400", "got 400 response code");
    $T->has_error_like(qr{C_BREAK_DEPENDENCIES}, "got caution code 'C_BREAK_DEPENDENCIES'");

    # Make sure the bound is still there.

    $T->fetch_records("/timescales/bounds.json?bound_id=$bound1", "bound is still there");
    
    # Now try again with 'BREAK_DEPENDENCIES'. This should succeed.
    
    $T->fetch_records("/timescales/delete.json?bound_id=$bound1&allow=BREAK_DEPENDENCIES",
		      "delete with allowance");
    
    # Make sure the bound we deleted is not there, and also that the dependent bounds have had
    # their references set to zero.
    
    $T->fetch_nocheck("/timescales/bounds.json?bound_id=$bound1", "check for deletion");
    $T->ok_no_records("confirm deletion");
    
    my (@t5) = $T->fetch_records("/timescales/bounds.json?timescale_id=$ts2", "fetch dependent bounds");
    
    ok( ! $t5[2]{bid}, "base id of dep2 was set to zero" );
    ok( ! $t5[2]{cid}, "color id of dep2 was set to zero" );
    ok( ! $t5[3]{tid}, "range id of dep3 was set to zero" );
    ok( $t5[4]{bid}, "base id of dep4 was unchanged" );
};


# Now create a timescale with interval names and then create new interval records from it.

subtest 'intervals' => sub {
    
    select_subtest || return;
    
    unless ( $ALBIAN && $APTIAN )
    {
	fail("Fetch of interval records for 'Albian' and 'Aptian' succeeded in 'basic' subtest");
	return;
    }
    
    # First add a timescale and some bounds.
    
    my $new_js = [ { timescale_name => 'EDIT TEST E', _label => 't1', priority => 1 },
		   { timescale_id => '@t1', bound_type => 'absolute', age => '98.8', _label => 'b1' },
		   { timescale_id => '@t1', bound_type => 'same', base_id => $ALBIAN->{bid},
		     top_id => '@b1', interval_name => 'INT ABC', _label => 'b2' },
		   { timescale_id => '@t1', bound_type => 'same', base_id => $APTIAN->{bid},
		     top_id => '@b2', interval_name => 'INT DEF', _label => 'b3' },
		   { timescale_id => '@t1', bound_type => 'absolute', age => '150.20',
		     top_id => '@b3', interval_name => 'INT GHI', _label => 'b4' } ];
    
    my (@t1) = $T->send_records("/timescales/addupdate.json", "add timescale and bounds",
				json => $new_js);
    
    return unless @t1;
    
    cmp_ok( @t1, '==', 5, "got five records back from add" );
    
    # Now 
    
    
};


# Now clean up all timescales we have added, plus any bounds and intervals associated with them.

subtest cleanup => sub {

    pass("placeholder");
    
    my $dbh = $ET->dbh;
    
    my ($test_idlist) = $dbh->selectrow_array("
	SELECT group_concat(timescale_no) FROM $TABLE{TIMESCALE_DATA}
	WHERE timescale_name like 'EDIT TEST%'");

    my $result;

    diag("Cleanup:");
    
    if ( $test_idlist )
    {
	$result = $dbh->do("DELETE FROM $TABLE{TIMESCALE_DATA} WHERE timescale_no in ($test_idlist)");
	
	diag("    deleted $result timescales.");

	$result = $dbh->do("DELETE FROM $TABLE{TIMESCALE_BOUNDS} WHERE timescale_no in ($test_idlist)");
	print STDERR "DELETE FROM $TABLE{TIMESCALE_BOUNDS} WHERE timescale_no in ($test_idlist)\n";
	diag("    deleted $result bounds.");

	$result = $dbh->do("DELETE FROM $TABLE{TIMESCALE_INTS} WHERE timescale_no in ($test_idlist)");

	diag("    deleted $result intervals.");
    }

    else
    {
	diag("    no timescales were found matching the cleanup pattern.");
    }

};
