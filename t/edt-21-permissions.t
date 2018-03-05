#
# PBDB Data Service
# -----------------
#
# This file contains unit tests for the EditTransaction class.
#
# edt-21-permissions.t : Test that table operations can only be carried out
# when the proper permissions are obtained first, and that the various table
# properties relating to permissions work properly.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 2;

use TableDefs qw($EDT_TEST get_table_property);

use EditTest;
use EditTester;

use Carp qw(croak);


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;



my ($perm_a, $perm_e, $perm_g, $perm_n, $primary);
my (@testkeys);


subtest 'setup' => sub {
    
    # Grab the permissions that we will need for testing.
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) or die;
    
    $perm_e = $T->new_perm('SESSION-ENTERER');

    ok( $perm_e && $perm_e->role eq 'enterer', "found enterer permission" ) or die;
    
    $perm_g = $T->new_perm('SESSION-GUEST');
    
    ok( $perm_g && $perm_g->role eq 'guest', "found guest permission" ) or die;
    
    $perm_n = $T->new_perm('NO_LOGIN');

    ok( $perm_n && $perm_n->role eq 'none', "found no-login permissin" ) or die;
    
    # Grab the name of the primary key of our test table.
    
    $primary = get_table_property($EDT_TEST, 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || die;
    
    # Clear the table so we can check for proper record insertion.
    
    $T->clear_table($EDT_TEST);
    
    # Then insert some test records, which we will later use to test updates,
    # replacements, and deletions.
    
    my $edt = $T->new_edt($perm_a);
    
    foreach my $i (1..30)
    {
	$edt->insert_record($EDT_TEST, { string_req => 'permission test' });
    }
    
    ok( $edt->commit, "setup insertransaction succeeded" ) || BAIL_OUT;
    
    @testkeys = $edt->inserted_keys;
};


# Now check the differences in table access between the different permissions.

subtest 'basic' => sub {

    my ($edt, $result);
    
    # Start with the authorizer, as a control, and then check enterer, guest and no login. The
    # enterer fails at update and delete because they do not have 
    
    test_permissions($EDT_TEST, $perm_a, 'all', 'succeeds', "authorizer succeeded");
    test_permissions($EDT_TEST, $perm_e, 'all', 'succeeds', "enterer succeeded");
    test_permissions($EDT_TEST, $perm_g, 'all', 'fails', "guest failed");
    test_permissions($EDT_TEST, $perm_n, 'all', 'fails', "no login failed");
};


# test_permissions ( table, insert_perm, [check_perm,] test, result, label )
# 
# We now define a subroutine which will check the ability to do all four operations on a
# particular table, or some subset, according to a particular set of permissions. This subroutine
# will pass or fail a test depending on the result of these operations.

sub test_permissions {
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    # First check the arguments.
    
    my $table = shift;
    my $insert_perm = shift;
    my $check_perm = $insert_perm;
    
    croak "bad insert permission '" . ref $insert_perm . "'"
	unless ref $insert_perm eq 'Permissions';
    
    if ( ref $_[0] eq 'Permissions' )
    {
	$check_perm = shift;
    }
    
    elsif ( ref $_[0] )
    {
	croak "bad check permission '" . ref $_[0] . "'";
    }
    
    my ($test, $result, $label) = @_;
    
    croak "bad test '$test'" unless $test &&
	($test eq 'all' || $test =~ qr{ ^ [IDRU]+ $ }xs );
    
    if ( $result && $result eq 'succeeds' )
    {
	$label ||= 'transaction succeeded';
    }
    
    elsif ( $result && $result eq 'fails' )
    {
	$label ||= 'transaction failed with E_PERM';
    }

    else
    {
	croak "bad check '$result', must be one of 'succeeds', 'fails'";
    }

    # First test insertion. We use the first specified permission to do this.
    
    my $edt = $T->new_edt($insert_perm, { IMMEDIATE_MODE => 1 });
    
    # my ($update_key, $replace_key, $delete_key, $was_there);
    
    # if ( $test eq 'all' || $test eq 'non-delete' )
    # {
    # 	$edt->insert_record($table, { string_req => 'insert permission test' });
	
    # 	$update_key = shift @testkeys;
	
    # 	$edt->update_record($table, { $primary => $update_key, string_req => 'permission updated' });

    # 	$replace_key = shift @testkeys;

    # 	$edt->replace_record($table, { $primary => $replace_key, string_req => 'permission replaced' });
    # }
    
    # if ( $test eq 'all' || $test eq 'delete' )
    # {
    # 	$delete_key = shift @testkeys;
	
    # 	# $T->ok_found_record($table, "$primary=$delete_key") || return;
	
    # 	$edt->delete_record($table, $delete_key);
    # }
    
    # If the transaction succeeds, then return the proper result.
    
    if ( $edt->execute )
    {
	# If the transaction was supposed to succeed, then the test passes. Otherwise, it fails.
	
	return ok( $result eq 'succeeds', $label );
    }
    
    # If the transaction fails, we have to look at the reasons behind the failure.
    
    my ($good_code, $bad_code);
    
    foreach my $e ( $edt->errors )
    {
	if ( $e->code eq 'E_PERM' )
	{
	    $good_code = 1;
	}

	else
	{
	    $bad_code = 1;
	    diag($edt->generate_msg($e));
	}
    }

    # If we have any errors other than E_PERM, then the test fails regardless of what the fourth
    # argument was.
    
    if ( $bad_code )
    {
	return fail( $label );
    }
    
    # Otherwise, if we have at least one E_PERM, then the test succeeds if the transaction was
    # supposed to fail.
    
    elsif ( $good_code )
    {
	return ok( $result eq 'fails', $label );
    }

    # If we don't get at least one E_PERM or other error, something has gone very wrong.
    
    else
    {
	diag( "The transaction failed, but no errors were found." );
	return fail( $label );
    }
}


