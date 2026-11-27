# -*- mode: cperl -*-
#
# PBDB 1.2
# --------
#
# The purpose of this file is to test the operation prefs/set, which sets a logged-in
# user's preferences and field defaults. They are stored in the 'preferences' column of the
# person table, as a list of 'name=value' settings and flags, separated by ' -:- '.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use feature 'unicode_strings';

use Test::More tests => 9;

use lib qw(lib ../lib t);

use TableDefs qw(%TABLE);
use CoreTableDefs;

use Tester;
use PBDBTester;


# Create an instance of the Tester class, with which to make requests to the server.

my $T = Tester->new({ prefix => 'data1.2' });

# Then check to MAKE SURE that the server is in test mode and the test session tables, which
# include the person table, are enabled. This is very important, because we DO NOT WANT to
# change the data in the main tables. This counts as the first test in this file.

$T->test_mode('session_data', 'enable') || BAIL_OUT("could not select test session data");

my $ET = PBDBTester->new;

BAIL_OUT("the person table is not in the test database")
    unless $TABLE{PERSON_DATA} =~ /^pbdbTEST[.]/;


# The person whose preferences are set, and the session that logs in as that person.

my $SESSION = "SESSION-ENTERER";
my ($PERSON) = $ET->get_session_authinfo($SESSION);

($PERSON) = $ET->dbh->selectrow_array("SELECT enterer_no FROM $TABLE{SESSION_DATA}
		WHERE session_id = '$SESSION'");


# Set the stored preferences of the test person, and return them.

sub set_stored {
    
    my ($value) = @_;
    
    $ET->dbh->do("UPDATE $TABLE{PERSON_DATA} SET preferences = " . $ET->dbh->quote($value) .
		 " WHERE person_no = $PERSON");
}

sub stored {
    
    my ($value) = $ET->dbh->selectrow_array("SELECT preferences FROM $TABLE{PERSON_DATA}
		WHERE person_no = $PERSON");
    
    return join(' | ', sort split / -:- /, $value // '');
}


# Return the response records as a hash of preference => [value, status]. An empty value is
# left out of the response, so a missing value counts as empty.

sub settings {
    
    return map { $_->{nam} => [ $_->{val} // '', $_->{sta} // '' ] } @_;
}


subtest 'setup' => sub {
    
    ok( $PERSON, "found the test person" ) || BAIL_OUT("no test person");
    
    set_stored("color=red -:- size=10 -:- taphonomy");
    
    is( stored(), "color=red | size=10 | taphonomy", "the stored preferences are in place" );
};


# A preference can be set in the URL, which adds it or changes it.

subtest 'set in the URL' => sub {
    
    $T->set_cookie("session_id", $SESSION);
    
    my (@r) = $T->fetch_records("/prefs/set.json?field=shape&value=round", "set a new preference");
    
    my %s = settings(@r);
    
    is_deeply( $s{shape}, [ 'round', 'set' ], "the new preference was reported" );
    is( stored(), "color=red | shape=round | size=10 | taphonomy", "and stored" );
    
    (@r) = $T->fetch_records("/prefs/set.json?field=color&value=blue", "change a preference");
    
    %s = settings(@r);
    
    is_deeply( $s{color}, [ 'blue', 'set' ], "the changed preference was reported" );
    is( stored(), "color=blue | shape=round | size=10 | taphonomy", "and stored" );
};


# Several preferences can be set in a request body. An empty value deletes a preference.

subtest 'set in the body' => sub {
    
    $T->set_cookie("session_id", $SESSION);
    
    my (@r) = $T->send_records("/prefs/set.json", "set and delete preferences",
			       json => [ { field => 'size', value => '12' },
					 { field => 'shape', value => '' } ]);
    
    my %s = settings(@r);
    
    is_deeply( $s{size}, [ '12', 'set' ], "the changed preference was reported" );
    is_deeply( $s{shape}, [ '', 'deleted' ], "and the deleted one" );
    is( stored(), "color=blue | size=12 | taphonomy", "both changes were stored" );
};


# With show=all, the preferences that were not changed are reported as well.

subtest 'show all' => sub {
    
    $T->set_cookie("session_id", $SESSION);
    
    my (@r) = $T->fetch_records("/prefs/set.json?field=color&value=green&show=all",
				"set a preference and show all");
    
    my %s = settings(@r);
    
    is_deeply( $s{color}, [ 'green', 'set' ], "the changed preference was reported" );
    is_deeply( $s{size}, [ '12', '' ], "a preference that was not changed was reported" );
    is_deeply( $s{taphonomy}, [ 'true', '' ], "and so was a flag" );
};


# Some preferences are flags, which are stored without a value. Setting one stores the flag,
# and an empty value removes it.

subtest 'flags' => sub {
    
    $T->set_cookie("session_id", $SESSION);
    
    my (@r) = $T->fetch_records("/prefs/set.json?field=subgenera&value=yes", "set a new flag");
    
    my %s = settings(@r);
    
    is_deeply( $s{subgenera}, [ 'yes', 'set' ], "the flag was reported as set" );
    is( stored(), "color=green | size=12 | subgenera | taphonomy", "and stored without a value" );
    
    # Setting a flag that is already set leaves it set once.
    
    (@r) = $T->fetch_records("/prefs/set.json?field=taphonomy&value=yes", "set an existing flag");
    
    %s = settings(@r);
    
    is( stored(), "color=green | size=12 | subgenera | taphonomy",
	"the existing flag is stored once" );
    
    is_deeply( $s{taphonomy}, [ 'yes', 'set' ], "the flag was reported as set" );
    
    set_stored("color=green -:- size=12 -:- subgenera -:- taphonomy");
    
    (@r) = $T->fetch_records("/prefs/set.json?field=taphonomy&value=", "remove a flag");
    
    %s = settings(@r);
    
    is_deeply( $s{taphonomy}, [ '', 'deleted' ], "the flag was reported as deleted" );
    is( stored(), "color=green | size=12 | subgenera", "and removed" );
};


# A guest may not set preferences, and nobody may without logging in.

subtest 'not allowed' => sub {
    
    $T->set_cookie("session_id", "SESSION-GUEST");
    
    my ($m1) = $T->fetch_nocheck("/prefs/set.json?field=color&value=black", "guest sets a preference");
    
    $T->ok_response_code($m1, '400', "a guest gets 400");
    $T->ok_error_like($m1, qr{Guest users cannot set preferences}, "with the reason");
    
    $T->set_cookie("session_id", "");
    
    my ($m2) = $T->fetch_nocheck("/prefs/set.json?field=color&value=black",
				 "set a preference without logging in");
    
    $T->ok_response_code($m2, '401', "setting a preference without logging in gets 401");
    
    is( stored(), "color=green | size=12 | subgenera", "nothing was changed" );
};


# A long list of preferences is stored whole, as the column in the real person table is a
# longtext. [Added by Claude under the guidance of Michael McClennen]

subtest 'long preferences' => sub {
    
    $T->set_cookie("session_id", $SESSION);
    
    my $long = 'x' x 100;
    
    $T->fetch_records("/prefs/set.json?field=long_value&value=$long", "set a long preference");
    
    like( stored(), qr{long_value=$long}, "the long preference was stored whole" );
};


# A preference given a null value in a request body is deleted, as one given the empty
# string is. [Added by Claude under the guidance of Michael McClennen]

subtest 'delete with null' => sub {
    
    $T->set_cookie("session_id", $SESSION);
    
    my (@r) = $T->send_records("/prefs/set.json", "delete a preference with null",
			       json => [ { field => 'long_value', value => undef } ]);
    
    my %s = settings(@r);
    
    is_deeply( $s{long_value}, [ '', 'deleted' ], "the preference was reported as deleted" );
    unlike( stored(), qr{long_value}, "and was removed" );
};
