#
# EditTransaction project
# -----------------------
#
# This file contains unit tests for the EditTransaction client modules, the subclasses of
# EditTransaction that handle edits to particular kinds of Paleobiology Database records.
#
# edtpc-07-names.t : Test matchIdentifiedName from OccurrenceBase.pm, which CollectionEdit
#                    uses to find the taxon that matches the identified name of an
#                    occurrence or reidentification. The test data is the genus
#                    Drepanomeryx, which has two subgenera, copied from the real
#                    authorities table.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 5;

use TableDefs qw(%TABLE original_table_name);

use EDTPCTest;
use PBDBTester;
use EditTester qw(clear_table sql_command);

use OccurrenceBase;


# Establish the tester and select the test tables. CollectionEdit is the module that uses
# matchIdentifiedName, and AUTHORITY_DATA is one of its test tables.

my $T = PBDBTester->new('CollectionEdit');

EDTPCTest->select_test_tables('CollectionEdit');

my $dbh = $T->dbh;


# The authority records used by these tests.

my %TAXON = ( 42634 => 'Drepanomeryx',
	      138945 => 'Drepanomeryx (Drepanomeryx)',
	      138948 => 'Drepanomeryx (Matthomeryx)',
	      138946 => 'Drepanomeryx (Drepanomeryx) falciformis',
	      45925 => 'Drepanomeryx falciformis',
	      45926 => 'Drepanomeryx (Matthomeryx) matthewi' );


# Call matchIdentifiedName with the specified name components, and return the names of
# the matching taxa as a string.

sub match {
    
    my ($genus, $subgenus, $species) = @_;
    
    my @matches = OccurrenceBase::matchIdentifiedName($dbh, undef, $genus, $subgenus,
						      $species, undef);
    
    return join('; ', sort map { $TAXON{$_} // $_ } @matches);
}


# This class collects the debugging output of matchIdentifiedName, which includes the
# queries it generates.

{
    package SQLCollector;
    
    sub new { return bless { lines => [] }, shift }
    sub debug_line { push $_[0]{lines}->@*, $_[1] }
    sub output { return join('', $_[0]{lines}->@*) }
}


# Copy the test taxa from the real authorities table.

subtest 'setup' => sub {
    
    clear_table('AUTHORITY_DATA');
    
    my $real_auth = original_table_name($TABLE{AUTHORITY_DATA});
    my $taxon_list = join(',', keys %TAXON);
    
    sql_command("INSERT INTO <<AUTHORITY_DATA>> SELECT * FROM $real_auth
		WHERE taxon_no in ($taxon_list)");
    
    my $rows = $dbh->selectall_arrayref("SELECT taxon_no, taxon_name FROM $TABLE{AUTHORITY_DATA}");
    
    is_deeply( { map { $_->[0] => $_->[1] } @$rows }, \%TAXON, "the test taxa are in place" );
    
    OccurrenceBase::initializeModifiers($dbh);
};


# Names without wildcards match exactly.

subtest 'exact names' => sub {
    
    is( match('Drepanomeryx'), 'Drepanomeryx', "a genus" );
    
    is( match('Drepanomeryx', 'Matthomeryx'), 'Drepanomeryx (Matthomeryx)', "a subgenus" );
    
    is( match('Drepanomeryx', 'Matthomeryx', 'matthewi'), 'Drepanomeryx (Matthomeryx) matthewi',
	"a species in a subgenus" );
    
    is( match('Drepanomeryx', '', 'falciformis'), 'Drepanomeryx falciformis',
	"a species without its subgenus" );
};


# A name with a wildcard is matched with patterns instead of exactly. A name with a
# wildcard used to throw an exception, because the result of the exact match was checked
# even though that match was skipped.

subtest 'wildcards' => sub {
    
    my $result = eval { match('Drepanomeryx', 'Matth%', 'matthewi') };
    
    ok( ! $@, "a name with a wildcard does not throw an exception" ) ||
	diag("exception: $@");
    
    is( $result, 'Drepanomeryx (Matthomeryx) matthewi', "a species with a '%' in the subgenus" );
    
    is( match('Drepanomeryx', 'M_tthomeryx', 'matthewi'), 'Drepanomeryx (Matthomeryx) matthewi',
	"a species with a '_' in the subgenus" );
    
    is( match('Drepanomeryx', 'Drep%', 'falciformis'),
	'Drepanomeryx (Drepanomeryx) falciformis; Drepanomeryx falciformis',
	"a species with a '%' in the subgenus, which matches it both with and without" );
};


# The patterns are generated from the wildcard name components. Check the subgenus
# pattern in the generated queries, since the final results do not depend on it in the
# cases above.

subtest 'subgenus pattern' => sub {
    
    foreach my $species ( 'matthewi', undef )
    {
	my $collector = SQLCollector->new;
	
	OccurrenceBase::matchIdentifiedName($dbh, $collector, 'Drepanomeryx', 'Matth%',
					    $species, undef);
	
	my $sql = $collector->output;
	my $which = $species ? "with a species" : "without a species";
	
	like( $sql, qr{ [(] \]? Matth\[a-zA-Z\]\* }xs,
	      "the subgenus pattern was generated from the subgenus name, $which" ) ||
	    diag($sql);
	
	unlike( $sql, qr{ [(] \]? \\? \[ \) \] }xs,
		"no query has an empty subgenus pattern, $which" ) ||
	    diag($sql);
    }
};


# Wildcard name components are not used when scoring the candidates, because
# computeTaxonMatch compares each component exactly. So a candidate that only matches
# through a wildcard is discarded.

subtest 'wildcard scoring' => sub {
  
  TODO: {
	local $TODO = "computeTaxonMatch does not handle wildcards";
	
	is( match('Drepanomeryx', 'Matth%'), 'Drepanomeryx (Matthomeryx)',
	    "a subgenus with a '%'" );
	
	is( match('Drepano%', '', 'matthewi'), 'Drepanomeryx (Matthomeryx) matthewi',
	    "a species with a '%' in the genus" );
    }
    
    clear_table('AUTHORITY_DATA');
};
