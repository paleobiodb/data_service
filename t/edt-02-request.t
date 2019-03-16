#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-02-request.t : Test that an EditTransaction object can be created using an object that looks
# like a data service request.
#



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 1;

use EditTest;
use EditTester;


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;



my ($perm_a);


subtest 'request' => sub {
    
    my $request = Request->new($T);
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');

    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) || BAIL_OUT;
    
    my ($edt, $result);
    
    eval {

	my $debug = $T->debug;
	
	$edt = EditTest->new($request, $perm_a, 'EDT_TEST',
			     { CREATE => 1, DEBUG_MODE => $debug });

	$edt->insert_record('EDT_TEST', { string_req => 'request test' });
	$result = $edt->execute;
    };
    
    if ( $@ )
    {
	diag("ERROR: $@");
	fail("created edt from request");
	return;
    }
    
    ok( $result, "inserted one record using request edt" );
    $T->ok_found_record('EDT_TEST', "string_req='request test'");
    
    cmp_ok( $edt->request, '==', $request, "fetch request ref" );
};




package Request;

sub new {

    my ($class, $T) = @_;
    
    return bless { dbh => $T->dbh }, $class;
}


sub get_connection {

    my ($r) = @_;

    return $r->{dbh};
}


sub debug_line {
    
    my ($r, $line) = @_;

    print STDERR "$line\n";
}


1;
