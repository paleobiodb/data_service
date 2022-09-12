#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of
# EditTransaction whose purpose is to implement these tests.
# 
# constructor.t : Test that the EditTester class loads properly, then test the
#                 constructor and basic accessor methods
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 4;

use ETBasicTest;
use EditTester qw(connect_to_database ok_eval ok_exception);


# Establish a connection to the database.

$DB::single = 1;

my $dbh = connect_to_database();


# Test the EditTransaction constructor method.

subtest 'constructor arguments' => sub {
    
    ok_eval( sub { ETBasicTest->new($dbh) }, 
	     "created ETBasicTest instance" ) || BAIL_OUT "minimal instance create failed";
    
    ok_eval( sub { ETBasicTest->new($dbh, { }) },
	     "instance with empty options" );
    
    ok_eval( sub { ETBasicTest->new($dbh, { table => 'EDT_TEST' }) },
	     "instance with table 'EDT_TEST'" );
    
    ok_eval( sub { ETBasicTest->new($dbh, { permission => 'anything' }) },
	     "instance with permission" );
    
    ok_eval( sub { ETBasicTest->new($dbh, { allows => 'CREATE' }) },
	     "instance with allows" ) || BAIL_OUT "create with allows failed";
    
    ok_eval( sub { ETBasicTest->new($dbh, { allows => { CREATE => 1 } }) },
	     "instance with allows hash" );
    
    ok_eval( sub { ETBasicTest->new($dbh, { allows => ['CREATE'] }) },
	     "instance with allows list" );
    
    ok_eval( sub { ETBasicTest->new($dbh, { table => 'EDT_TEST', allows => 'CREATE' }) },
	     "instance with allows scalar" );
    
    my $indirect = TestIndirect->new($dbh);
    
    ok_eval( sub { ETBasicTest->new($indirect) }, "indirect database argument" ) ||
	BAIL_OUT "instance with indirect database argument failed";
};


# Check for the proper exceptions and conditions if bad arguments are given.

subtest 'constructor bad arguments' => sub {
    
    ok_exception( sub { ETBasicTest->new() }, qr/database connection/i,
		  "no database connection" );
    
    my $x = bless { }, 'XYZ';
    
    ok_exception( sub { ETBasicTest->new($dbh, $x) }, qr/invalid parameter/i,
		  "invalid parameter hash" );
    
    ok_exception( sub { ETBasicTest->new($dbh, { table => 'EDT_TEST', foo => 'bar' }) },
		  qr/unrecognized option/i, "bad option key" );
    
    ok_exception( sub { ETBasicTest->new($x) }, qr/database/,
		  "bad database argument" );
    
    my $edt;
    
    ok_eval( sub { $edt = ETBasicTest->new($dbh, { allows => $x }) }, 
	     "new with bad allowance" ) &&
    
		 ok( $edt->has_condition('W_BAD_ALLOWANCE'), "bad allowance recognized" );
    
    ok_eval( sub { $edt = ETBasicTest->new($dbh, { allows => 'NOT_AN_ALLOWANCE' }) },
	     "new with bad allowance 2" ) &&
    
		 ok( $edt->has_condition('W_BAD_ALLOWANCE'), "bad allowance recognized 2" );
    
    ok_eval( sub { $edt = ETBasicTest->new($dbh, { table => 'TABLE_X' }) },
	     "new with bad table specifier" ) &&
    
		 ok( $edt->has_condition('E_BAD_TABLE'), "bad table recognized" );
    
    my $bad_indirect = TestIndirect->new({ });
    
    ok_eval( sub { $edt = ETBasicTest->new($bad_indirect) },
	     "new with bad indirect" ) &&
		 
		 ok( $edt->has_condition('E_BAD_CONNECTION'), "bad connection recognized" );
};


# Check the values of the basic accessor methods.

subtest 'accessors' => sub {
    
    my $edt = ETBasicTest->new($dbh);
    
    is( $edt->dbh, $dbh, "dbh method" );
    is( $edt->status, 'init', "status method" );
    is( $edt->transaction, '', "transaction method" );
    is( $edt->permission, 'unrestricted', "permission method" );
    ok( ! $edt->has_started, "has_started method" );
    ok( ! $edt->is_active, "is_active method" );
    ok( ! $edt->is_executing, "is_executing method" );
    ok( ! $edt->has_finished, "has_finished method" );
    ok( ! $edt->has_committed, "has_committed method" );
    ok( ! $edt->has_failed, "has_failed method" );
    ok( $edt->can_accept, "can_accept method" );
    ok( $edt->can_proceed, "can_proceed method" );
    
    my $indirect = TestIndirect->new($dbh);
    
    $edt = ETBasicTest->new($indirect);
    
    is( $edt->request, $indirect, "request method" );
    is( $edt->dbh, $dbh, "dbh method with indirect" );
};


# Check instance creation and accessors with IMMEDIATE_MODE.

subtest 'immediate mode' => sub {
    
    ok_eval( sub { ETBasicTest->new($dbh, { allows => 'IMMEDIATE_MODE' }) },
	     "new with immediate mode" ) || BAIL_OUT "new with immediate mode failed";
    
    my $edt = ETBasicTest->new($dbh, { allows => 'IMMEDIATE_MODE' });
    
    is( $edt->transaction, 'active', "transaction method" );
    ok( $edt->has_started, "has_started method" );
    ok( $edt->is_active, "is_active method" );
    ok( $edt->is_executing, "is_executing method" );
    ok( ! $edt->has_finished, "has_finished method" );
    ok( ! $edt->has_committed, "has_committed method" );
    ok( ! $edt->has_failed, "has_failed method" );
    ok( $edt->can_accept, "can_accept method" );
    ok( $edt->can_proceed, "can_proceed method" );
};


package TestIndirect;

sub new {
    
    return bless { dbh => $_[1] };
}


sub get_connection {
    
    my ($self) = @_;
    
    return $self->{dbh};
}


1;
		  
