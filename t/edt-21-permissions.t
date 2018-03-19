#
# PBDB Data Service
# -----------------
# 
# This file contains unit tests for the EditTransaction class.
# 
# edt-21-permissions.t : Test that table operations can only be carried out when the proper
# permissions are obtained first, and that the various table properties relating to permissions
# work properly.
# 



use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 7;

use TableDefs qw($EDT_TEST $EDT_AUX $EDT_ANY get_table_property set_table_property);

use EditTest;
use EditTester;

use Carp qw(croak);


# The following call establishes a connection to the database, using EditTester.pm.

my $T = EditTester->new;



my ($perm_a, $perm_e, $perm_e2, $perm_d2, $perm_u, $perm_g, $perm_s, $perm_n, $primary);
# my (@testkeys);


subtest 'setup' => sub {
    
    # Grab the permissions that we will need for testing.
    
    $perm_a = $T->new_perm('SESSION-AUTHORIZER');
    
    ok( $perm_a && $perm_a->role eq 'authorizer', "found authorizer permission" ) or die;
    
    $perm_e = $T->new_perm('SESSION-ENTERER');
    
    ok( $perm_e && $perm_e->role eq 'enterer', "found enterer permission" ) or die;
    
    $perm_e2 = $T->new_perm('SESSION-OTHER');
    
    ok( $perm_e2 && $perm_e2->role eq 'enterer' && $perm_e2->{authorizer_no} ne $perm_e->{authorizer_no},
	"found second enterer permission" ) or die;
    
    $perm_d2 = $T->new_perm('SESSION-STUDENT');
    
    ok( $perm_d2 && $perm_d2->role eq 'student', "found student permission" ) or die;
    
    $perm_u = $T->new_perm('SESSION-UNAUTH');
    
    ok( $perm_u && $perm_u->{enterer_no} && ! $perm_u->{authorizer_on},
	"found no-authorizer permission" ) or die;
    
    $perm_g = $T->new_perm('SESSION-GUEST');
    
    ok( $perm_g && $perm_g->role eq 'guest', "found guest permission" ) or die;
    
    $perm_s = $T->new_perm('SESSION-SUPERUSER');
    
    ok( $perm_s && $perm_s->is_superuser, "found superuser permission" ) or die;
    
    $perm_n = $T->new_perm('NO_LOGIN');

    ok( $perm_n && $perm_n->role eq 'none', "found no-login permission" ) or die;
    
    # Grab the name of the primary key of our test table.
    
    $primary = get_table_property($EDT_TEST, 'PRIMARY_KEY');
    ok( $primary, "found primary key field" ) || die;
    
    # Clear the tables so we can check for proper record insertion.
    
    $T->clear_table($EDT_TEST);
    $T->clear_table($EDT_AUX);
    $T->clear_table($EDT_ANY);

    # Also clear the specific table permissions.

    $T->clear_specific_permissions;
};


# Start by checking the differences in table access between the different permissions using the
# most common value for CAN_POST, namely 'AUTHORIZED'.

subtest 'basic' => sub {

    my ($edt, $result);
    
    set_table_property($EDT_TEST, CAN_POST => 'AUTHORIZED');

    $perm_a->clear_cached_permissions;
    $perm_e->clear_cached_permissions;
    $perm_s->clear_cached_permissions;
    $perm_u->clear_cached_permissions;
    $perm_g->clear_cached_permissions;
    $perm_n->clear_cached_permissions;
    
    # Start with the authorizer, as a control, and then check enterer, guest and no login.
    
    $T->test_permissions($EDT_TEST, $perm_a, 'basic', 'succeeds', "authorizer succeeded");
    $T->test_permissions($EDT_TEST, $perm_e, 'basic', 'succeeds', "enterer succeeded");
    $T->test_permissions($EDT_TEST, $perm_s, 'basic', 'succeeds', "superuser succeeded");
    $T->test_permissions($EDT_TEST, $perm_u, 'basic', 'fails', "no-authorizer failed");
    $T->test_permissions($EDT_TEST, $perm_g, 'basic', 'fails', "guest failed");
    $T->test_permissions($EDT_TEST, $perm_n, 'basic', 'fails', "no login failed");
    
    # Test that the authorizer can update and delete records that the enterer put in, but not vice
    # versa.
    
    $T->test_permissions($EDT_TEST, $perm_a, $perm_e, 'basic', 'fails', "enterer cannot edit authorizer");
    $T->test_permissions($EDT_TEST, $perm_e, $perm_a, 'basic', 'succeeds', "authorizer can edit enterer");

    # Test that the superuser can update and delete records that a regular user put in, but not
    # vice versa.

    $T->test_permissions($EDT_TEST, $perm_s, $perm_e, 'basic', 'fails', "enterer cannot edit superuser");
    $T->test_permissions($EDT_TEST, $perm_e, $perm_s, 'basic', 'succeeds', "superuser can edit enterer");
    
    # For completeness, check that guest and no login cannot edit records that somebody else put in.
    
    $T->test_permissions($EDT_TEST, $perm_a, $perm_u, 'basic', 'fails', "no-authorizer cannot edit records");
    $T->test_permissions($EDT_TEST, $perm_a, $perm_g, 'basic', 'fails', "guest cannot edit records");
    $T->test_permissions($EDT_TEST, $perm_a, $perm_n, 'basic', 'fails', "no login cannot edit records");

    # Specifically deny a user access to the table, and check that this now fails.

    $T->set_specific_permission($EDT_TEST, $perm_a, 'none');

    $perm_a->clear_cached_permissions;

    $T->test_permissions($EDT_TEST, $perm_a, 'basic', 'fails', "authorizer fails with specific denial");
};


# Then check the results of setting table property CAN_POST to other values. We use the table
# $EDT_ANY, because it has the field 'enterer_id' which can record the user_id of a non-member
# user who enters a record.

subtest 'can_post' => sub {

    # Start by clearing all specific permissions.
    
    $T->clear_specific_permissions($EDT_TEST);
    
    # The initial value for the table $EDT_ANY should be 'LOGGED_IN', but we set this explicitly just
    # to make sure we are testing what we think we are testing.
    
    set_table_property($EDT_ANY, CAN_POST => 'LOGGED_IN');
    
    $perm_a->clear_cached_permissions;
    $perm_e->clear_cached_permissions;
    $perm_s->clear_cached_permissions;
    $perm_u->clear_cached_permissions;
    $perm_g->clear_cached_permissions;
    $perm_n->clear_cached_permissions;
    
    $T->test_permissions($EDT_ANY, $perm_a, 'basic', 'succeeds', "authorizer succeeded");
    $T->test_permissions($EDT_ANY, $perm_e, 'basic', 'succeeds', "enterer succeeded");
    $T->test_permissions($EDT_ANY, $perm_s, 'basic', 'succeeds', "superuser succeeded");
    $T->test_permissions($EDT_ANY, $perm_u, 'basic', 'succeeds', "no-authorizer succeeded");
    $T->test_permissions($EDT_ANY, $perm_g, 'basic', 'succeeds', "guest succeeded");
    $T->test_permissions($EDT_ANY, $perm_n, 'basic', 'fails', "no login failed");

    # Now try the value 'MEMBERS', which allows all database members to post even if they do not
    # have an assigned authorizer.
    
    set_table_property($EDT_ANY, CAN_POST => 'MEMBERS');
    
    $perm_a->clear_cached_permissions;
    $perm_e->clear_cached_permissions;
    $perm_s->clear_cached_permissions;
    $perm_u->clear_cached_permissions;
    $perm_g->clear_cached_permissions;
    $perm_n->clear_cached_permissions;
    
    $T->test_permissions($EDT_ANY, $perm_a, 'basic', 'succeeds', "authorizer succeeded");
    $T->test_permissions($EDT_ANY, $perm_e, 'basic', 'succeeds', "enterer succeeded");
    $T->test_permissions($EDT_ANY, $perm_s, 'basic', 'succeeds', "superuser succeeded");
    $T->test_permissions($EDT_ANY, $perm_u, 'basic', 'succeeds', "no-authorizer succeeded");
    $T->test_permissions($EDT_ANY, $perm_g, 'basic', 'fails', "guest failed");
    $T->test_permissions($EDT_ANY, $perm_n, 'basic', 'fails', "no login failed");

    # Now we try 'NONE', which will prevent anybody but the superuser from posting and editing.
    
    set_table_property($EDT_ANY, CAN_POST => 'NOBODY');
    
    $perm_a->clear_cached_permissions;
    $perm_e->clear_cached_permissions;
    $perm_s->clear_cached_permissions;
    $perm_u->clear_cached_permissions;
    $perm_g->clear_cached_permissions;
    $perm_n->clear_cached_permissions;
    
    $T->test_permissions($EDT_ANY, $perm_a, 'basic', 'fails', "authorizer failed");
    $T->test_permissions($EDT_ANY, $perm_e, 'basic', 'fails', "enterer failed");
    $T->test_permissions($EDT_ANY, $perm_s, 'basic', 'succeeds', "superuser succeeded");
    $T->test_permissions($EDT_ANY, $perm_u, 'basic', 'fails', "no-authorizer failed");
    $T->test_permissions($EDT_ANY, $perm_g, 'basic', 'fails', "guest failed");
    $T->test_permissions($EDT_ANY, $perm_n, 'basic', 'fails', "no login failed");

    # Then we add a specific permission for one user, and check that this user can now post and
    # edit but others still can't.
    
    $T->set_specific_permission($EDT_ANY, $perm_e, 'post');
    
    $perm_e->clear_cached_permissions;
    
    $T->test_permissions($EDT_ANY, $perm_a, 'basic', 'fails', "authorizer failed");
    $T->test_permissions($EDT_ANY, $perm_e, 'basic', 'succeeds', "enterer succeeded with specific permission");
    $T->test_permissions($EDT_ANY, $perm_s, 'basic', 'succeeds', "superuser succeeded");
    $T->test_permissions($EDT_ANY, $perm_u, 'basic', 'fails', "no-authorizer failed");
    $T->test_permissions($EDT_ANY, $perm_g, 'basic', 'fails', "guest failed");
    $T->test_permissions($EDT_ANY, $perm_n, 'basic', 'fails', "no login failed");

    # And check that an 'admin' permission works too.

    $T->set_specific_permission($EDT_ANY, $perm_e, 'admin');
    
    $perm_e->clear_cached_permissions;
    
    $T->test_permissions($EDT_ANY, $perm_e, 'basic', 'succeeds', "enterer succeeded with admin permission");

    # But check that a permission of 'none' prevents access.

    $T->set_specific_permission($EDT_ANY, $perm_e, 'none');

    $perm_e->clear_cached_permissions;

    $T->test_permissions($EDT_ANY, $perm_e, 'basic', 'fails', "enterer failed with specific denial");
};


# Now check the results of setting table property CAN_MODIFY.

subtest 'can_modify' => sub {
    
    # Clear all table permissions, so we know where we are starting from.
    
    $T->clear_specific_permissions;

    # First change the properties for $EDT_TEST to set CAN_POST to AUTHORIZED and CAN_MODIFY to
    # NOBODY. Check that this means that records cannot be modified by somebody with a different
    # authorizer.
    
    set_table_property($EDT_TEST, CAN_POST => 'AUTHORIZED');
    set_table_property($EDT_TEST, CAN_MODIFY => 'NOBODY');
    
    $perm_a->clear_cached_permissions;
    $perm_e2->clear_cached_permissions;
    
    $T->test_permissions($EDT_TEST, $perm_a, $perm_e2, 'basic', 'fails', "cannot modify non-owned records");

    # Then change CAN_MODIFY to AUTHORIZED. This should allow anybody to edit anybody else's
    # records.

    set_table_property($EDT_TEST, CAN_MODIFY => 'AUTHORIZED');

    $perm_a->clear_cached_permissions;
    $perm_e2->clear_cached_permissions;
    
    $T->test_permissions($EDT_TEST, $perm_a, $perm_e2, 'basic', 'succeeds', "can now modify non-owned records");
    
    # Then change CAN_POST to NOBODY. This should allow only the superuser to insert records, and
    # anybody with an authorizer_no to modify anybody else's records.
    
    set_table_property($EDT_TEST, CAN_POST => 'NOBODY');
    
    # Check that authorized users can edit other people's records (i.e. the superuser's), but
    # cannot post their own.
    
    $perm_a->clear_cached_permissions;
    $perm_e->clear_cached_permissions;
    $perm_u->clear_cached_permissions;
    $perm_g->clear_cached_permissions;
    
    $T->test_permissions($EDT_TEST, $perm_s, $perm_a, 'basic', 'succeeds', "authorizer can modify");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_e, 'basic', 'succeeds', "enterer can modify");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_u, 'basic', 'fails', "no-authorizer cannot modify");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_g, 'basic', 'fails', "guest cannot modify");
    
    $T->test_permissions($EDT_TEST, $perm_a, 'I', 'fails', "authorizer cannot post");
    $T->test_permissions($EDT_TEST, $perm_e, 'I', 'fails', "enterer cannot post");
    $T->test_permissions($EDT_TEST, $perm_u, 'I', 'fails', "no-authorizer cannot post");
    $T->test_permissions($EDT_TEST, $perm_g, 'I', 'fails', "guest cannot post");
    
    # Now change CAN_MODIFY to 'MEMBERS' and check that the no-authorizer user can now modify.
    
    set_table_property($EDT_TEST, CAN_MODIFY => 'MEMBERS');
    
    $perm_a->clear_cached_permissions;
    $perm_e->clear_cached_permissions;
    $perm_u->clear_cached_permissions;
    $perm_g->clear_cached_permissions;
    
    $T->test_permissions($EDT_TEST, $perm_s, $perm_a, 'basic', 'succeeds', "authorizer can modify");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_e, 'basic', 'succeeds', "enterer can modify");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_u, 'basic', 'succeeds', "no-authorizer can modify");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_g, 'basic', 'fails', "guest cannot modify");

    # Then change CAN_MODIFY to 'LOGGED_IN' and check that guest can now modify.
    
    set_table_property($EDT_TEST, CAN_MODIFY => 'LOGGED_IN');
    
    $perm_a->clear_cached_permissions;
    $perm_e->clear_cached_permissions;
    $perm_u->clear_cached_permissions;
    $perm_g->clear_cached_permissions;
    
    $T->test_permissions($EDT_TEST, $perm_s, $perm_a, 'basic', 'succeeds', "authorizer can modify");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_e, 'basic', 'succeeds', "enterer can modify");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_u, 'basic', 'succeeds', "no-authorizer can modify");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_g, 'basic', 'succeeds', "guest can modify");

    # Then change CAN_MODIFY to 'NOBODY' and check that nobody can modify.

    set_table_property($EDT_TEST, CAN_MODIFY => 'NOBODY');
    
    $perm_a->clear_cached_permissions;
    $perm_e->clear_cached_permissions;
    $perm_u->clear_cached_permissions;
    $perm_g->clear_cached_permissions;
    
    $T->test_permissions($EDT_TEST, $perm_s, $perm_a, 'basic', 'fails', "authorizer cannot modify");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_e, 'basic', 'fails', "enterer cannot modify");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_u, 'basic', 'fails', "no-authorizer cannot modify");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_g, 'basic', 'fails', "guest cannot modify");
    
    # Then we add a specific permission for one user, and check that this user can now post and
    # edit but others still can't.
    
    $T->set_specific_permission($EDT_TEST, $perm_e, 'modify');
    
    $perm_e->clear_cached_permissions;
    
    $T->test_permissions($EDT_TEST, $perm_s, $perm_a, 'basic', 'fails', "authorizer cannot modify");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_e, 'basic', 'succeeds', "enterer can modify with specific permission");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_u, 'basic', 'fails', "no-authorizer cannot modify");
    $T->test_permissions($EDT_TEST, $perm_s, $perm_g, 'basic', 'fails', "guest cannot modify");
    
    # And check that an 'admin' permission works too.
    
    $T->set_specific_permission($EDT_TEST, $perm_e, 'admin');
    
    $perm_e->clear_cached_permissions;
    
    $T->test_permissions($EDT_TEST, $perm_s, $perm_e, 'basic', 'succeeds', "enterer can modify with admin permission");
    
    # But check that a permission of 'none' prevents access.
    
    $T->set_specific_permission($EDT_TEST, $perm_e, 'none');
    
    $perm_e->clear_cached_permissions;

    $T->test_permissions($EDT_TEST, $perm_s, $perm_e, 'basic', 'fails', "enterer cannot modify with specific denial");

};


# Check that ALLOW_DELETE allows deletion of records, and turning it off prevents this.

subtest 'allow_delete' => sub {

    # Clear all permissions that were set by earlier subtests.
    
    $T->clear_specific_permissions;

    # Make sure that authorized users can post, but nobody can delete. Test that deletion is
    # disallowed.
    
    set_table_property($EDT_TEST, 'CAN_POST' => 'AUTHORIZED');
    set_table_property($EDT_TEST, 'ALLOW_DELETE' => 0);
    
    $perm_a->clear_cached_permissions;
    
    $T->test_permissions($EDT_TEST, $perm_a, 'D', 'fails', "cannot delete without ALLOW_DELETE");
    
    # Check that a table administrator can override this.
    
    $T->set_specific_permission($EDT_TEST, $perm_a, 'admin');
    
    $perm_a->clear_cached_permissions;
    
    $T->test_permissions($EDT_TEST, $perm_a, 'D', 'succeeds', "admin can delete regardless");

    # Check that a specific 'delete' privilege can also override this.

    $T->set_specific_permission($EDT_TEST, $perm_a, 'post,modify,delete');
    
    $T->test_permissions($EDT_TEST, $perm_a, 'D', 'succeeds', "can delete with specific permission");
    
    # Then turn deletion back on and test that it works.
    
    set_table_property($EDT_TEST, 'ALLOW_DELETE' => 1);
    
    $perm_a->clear_cached_permissions;
    $perm_e->clear_cached_permissions;
    
    $T->test_permissions($EDT_TEST, $perm_a, 'D', 'succeeds', "can delete with table permission");
    $T->test_permissions($EDT_TEST, $perm_e, $perm_a, 'D', 'succeeds', "authorizer can delete enterer's records");
    $T->test_permissions($EDT_TEST, $perm_a, $perm_e, 'D', 'fails', "enterer cannot delete authorizer's records");
};


# Check that ALLOW_INSERT_KEY allows the use of 'replace' to create new records with
# specific keys, and that turning it off prevents this.

subtest 'allow_insert_key' => sub {
    
    # Clear all permissions that were set by earlier subtests.
    
    $T->clear_specific_permissions;

    # Clear the table as well.

    $T->clear_table($EDT_TEST);
    
    # Make sure that authorized users can post, but nobody can insert specific keys. Test that
    # this is disallowed.
    
    set_table_property($EDT_TEST, CAN_POST => 'AUTHORIZED');
    set_table_property($EDT_TEST, ALLOW_INSERT_KEY => 0);
    
    $perm_a->clear_cached_permissions;
    $perm_e->clear_cached_permissions;
    $perm_g->clear_cached_permissions;
    $perm_s->clear_cached_permissions;
    
    $T->test_permissions($EDT_TEST, $perm_a, 'K', 'fails', "authorizer cannot insert key");
    $T->test_permissions($EDT_TEST, $perm_e, 'K', 'fails', "enterer cannot insert key");
    $T->test_permissions($EDT_TEST, $perm_g, 'K', 'fails', "guest cannot insert key");
    $T->test_permissions($EDT_TEST, $perm_s, 'K', 'succeeds', "superuser can insert key");

    # Check that a table administrator can override this.

    $T->set_specific_permission($EDT_TEST, $perm_e, 'admin');

    $perm_e->clear_cached_permissions;

    $T->test_permissions($EDT_TEST, $perm_e, 'K', 'succeeds', "admin can insert key");
    
    # Check that a specific permission can override this.
    
    $T->set_specific_permission($EDT_TEST, $perm_e, 'post,insert_key');
    
    $perm_e->clear_cached_permissions;
    
    $T->test_permissions($EDT_TEST, $perm_e, 'K', 'succeeds', "specific permission allows insert key");
    
    # Now turn ALLOW_INSERT_KEY on and re-test.

    set_table_property($EDT_TEST, ALLOW_INSERT_KEY => 1);

    $perm_a->clear_cached_permissions;
    $perm_e->clear_cached_permissions;
    $perm_g->clear_cached_permissions;
    
    $T->test_permissions($EDT_TEST, $perm_a, 'K', 'succeeds', "authorizer can now insert key");
    $T->test_permissions($EDT_TEST, $perm_e, 'K', 'succeeds', "enterer can now insert key");
    $T->test_permissions($EDT_TEST, $perm_g, 'K', 'fails', "guest still cannot insert key");
};


# Check that BY_AUTHORIZER allows two users in the same authorizer group to modify each other's records.

subtest 'by_authorizer' => sub {
    
    # Start by clearing specific table permissions.

    $T->clear_specific_permissions;

    # Set the table property, and check that people in the same authorizer group can modify each
    # other's records but people in different groups cannot.
    
    set_table_property($EDT_TEST, BY_AUTHORIZER => 1);

    $perm_a->clear_cached_permissions;
    $perm_e->clear_cached_permissions;
    $perm_e2->clear_cached_permissions;
    $perm_d2->clear_cached_permissions;

    $T->test_permissions($EDT_TEST, $perm_a, $perm_e, 'basic', 'succeeds',
			 "enterer can modify authorizer's records");
    $T->test_permissions($EDT_TEST, $perm_e2, $perm_d2, 'basic', 'succeeds',
			 "enterer can modify records from same authorizer group");
    $T->test_permissions($EDT_TEST, $perm_e, $perm_e2, 'basic', 'fails',
			 "enterer cannot modify records from different authorizer group");

    # Now clear the table property, and check that all of these fail.

    set_table_property($EDT_TEST, BY_AUTHORIZER => 0);

    $perm_a->clear_cached_permissions;
    $perm_e->clear_cached_permissions;
    $perm_e2->clear_cached_permissions;
    $perm_d2->clear_cached_permissions;

    $T->test_permissions($EDT_TEST, $perm_a, $perm_e, 'basic', 'fails',
			 "enterer cannot modify authorizer's records");
    $T->test_permissions($EDT_TEST, $perm_e2, $perm_d2, 'basic', 'fails',
			 "enterer cannot modify records from same authorizer group");
    $T->test_permissions($EDT_TEST, $perm_e, $perm_e2, 'basic', 'fails',
			 "enterer cannot modify records from different authorizer group");
};

