#
# Paleobiology Database library modules
# -------------------------------------
#
# This file contains unit tests for library modules that are used by the EditTransaction
# client modules and by the data service.
#
# lib-01-occurrencebase.t : Test OccurrenceBase.pm, which parses identified names into
# their components, puts them back together, and matches them against the authorities
# table. CollectionEdit and SpecimenEdit use it to find the taxon of each occurrence and
# specimen.
#
# The test data is a set of real authority records copied into the test database: the
# genus Drepanomeryx, which has two subgenera, Tyrannosaurus and T. rex, the family
# Tyrannosauridae, and the subspecies Martes caurina nobilis. The classification is read
# from the real taxon_trees table.
#
# Added by Claude under the guidance of Michael McClennen. The tests that were in
# edtpc-07-names.t have been moved here.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 18;

use TableDefs qw(%TABLE $TEST_DB original_table_name);

use EDTPCTest;
use PBDBTester;
use EditTester qw(clear_table sql_command);

use OccurrenceBase qw(parseIdentifiedName constructIdentifiedName matchIdentifiedName
		      matchExactName findParentTaxon splitTaxonName computeTaxonMatch
		      hashIdentification defaultSpeciesName);


# Establish the tester and select the test tables. AUTHORITY_DATA is one of the test
# tables of CollectionEdit, which is the main user of this module.

my $T = PBDBTester->new('CollectionEdit');

EDTPCTest->select_test_tables('CollectionEdit');

my $dbh = $T->dbh;


# The authority records used by these tests.

my %TAXON = ( 42634 => 'Drepanomeryx',
	      138945 => 'Drepanomeryx (Drepanomeryx)',
	      138948 => 'Drepanomeryx (Matthomeryx)',
	      138946 => 'Drepanomeryx (Drepanomeryx) falciformis',
	      45925 => 'Drepanomeryx falciformis',
	      45926 => 'Drepanomeryx (Matthomeryx) matthewi',
	      38613 => 'Tyrannosaurus',
	      54833 => 'Tyrannosaurus rex',
	      38606 => 'Tyrannosauridae',
	      47920 => 'Martes caurina nobilis' );


# Parse a name, and return the eight fields joined by '|', or the error message.

sub parse {
    
    my ($name, $options) = @_;
    
    my @fields = parseIdentifiedName($name, $options);
    
    return ref $fields[0] ? "ERROR: $fields[0]{error}" : join('|', map { $_ // '' } @fields);
}


# Call matchIdentifiedName with the specified name components, and return the names of the
# matching taxa as a string.

sub match {
    
    my ($genus, $subgenus, $species, $subspecies) = @_;
    
    my @matches = matchIdentifiedName($dbh, undef, $genus, $subgenus, $species, $subspecies);
    
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


# Create the test copy of the authorities table if it does not exist, and copy the test
# taxa into it. The modifiers are read from the definition of the occurrences table.

subtest 'setup' => sub {
    
    my $real_auth = original_table_name($TABLE{AUTHORITY_DATA});
    
    sql_command("CREATE TABLE IF NOT EXISTS <<AUTHORITY_DATA>> LIKE $real_auth");
    
    clear_table('AUTHORITY_DATA');
    
    my $taxon_list = join(',', keys %TAXON);
    
    sql_command("INSERT INTO <<AUTHORITY_DATA>> SELECT * FROM $real_auth
		WHERE taxon_no in ($taxon_list)");
    
    my $rows = $dbh->selectall_arrayref("SELECT taxon_no, taxon_name FROM $TABLE{AUTHORITY_DATA}");
    
    is_deeply( { map { $_->[0] => $_->[1] } @$rows }, \%TAXON, "the test taxa are in place" );
    
    OccurrenceBase::initializeModifiers($dbh);
    
    ok( $OccurrenceBase::OCC_RESO_RE{genus} && $OccurrenceBase::OCC_RESO_RE{subspecies},
	"the modifier patterns were generated" );
};


# A name without modifiers is split by quickParseIdentifiedName. A name with modifiers is
# left for the full parser.

subtest 'quick parsing' => sub {
    
    my %quick = ( 'Tyrannosaurus' => 'Tyrannosaurus|||||||',
		  'Tyrannosaurus rex' => 'Tyrannosaurus||||rex|||',
		  'Drepanomeryx (Matthomeryx) matthewi' => 'Drepanomeryx||Matthomeryx||matthewi|||',
		  'Martes caurina nobilis' => 'Martes||||caurina||nobilis|',
		  'Tyrannosaurus sp.' => 'Tyrannosaurus||||sp.|||',
		  'Tyrannosaurus spp.' => 'Tyrannosaurus||||spp.|||',
		  'Tyrannosaurus indet.' => 'Tyrannosaurus||||indet.|||' );
    
    foreach my $name ( sort keys %quick )
    {
	is( join('|', map { $_ // '' } OccurrenceBase::quickParseIdentifiedName($name)), $quick{$name},
	    "quick parse of '$name'" );
    }
    
    foreach my $name ( 'cf. Tyrannosaurus rex', 'Tyrannosaurus cf. rex', 'Martes caurina var nobilis',
		       'Tyrannosaurus var', '<Foo> rex', 'tyrannosaurus rex', 'Tyrannosaurus re',
		       'Martes caurina no' )
    {
	my @fields = OccurrenceBase::quickParseIdentifiedName($name);
	
	is( scalar(@fields), 0,
	    "'$name' is left for the full parser" );
    }
};


# Each name component may have a modifier. The full parser recognizes them, along with
# informal names in angle brackets and names in quotation marks.

subtest 'modifiers' => sub {
    
    my %ok = (
	'cf. Tyrannosaurus rex' => 'Tyrannosaurus|cf.|||rex|||',
	'aff. Tyrannosaurus rex' => 'Tyrannosaurus|aff.|||rex|||',
	'? Tyrannosaurus rex' => 'Tyrannosaurus|?|||rex|||',
	'ex gr. Tyrannosaurus rex' => 'Tyrannosaurus|ex gr.|||rex|||',
	'ex. gr. Tyrannosaurus rex' => 'Tyrannosaurus|ex gr.|||rex|||',
	'"Tyrannosaurus" rex' => 'Tyrannosaurus|"|||rex|||',
	'<Tyrannosaur A> rex' => 'Tyrannosaur A|informal|||rex|||',
	'Tyrannosaurus (<Foo A>) rex' => 'Tyrannosaurus||Foo A|informal|rex|||',
	'Tyrannosaurus ("Foo") rex' => 'Tyrannosaurus||Foo|"|rex|||',
	'Tyrannosaurus cf. rex' => 'Tyrannosaurus||||rex|cf.||',
	'Tyrannosaurus aff. rex' => 'Tyrannosaurus||||rex|aff.||',
	'Tyrannosaurus ? rex' => 'Tyrannosaurus||||rex|?||',
	'Tyrannosaurus ex gr. rex' => 'Tyrannosaurus||||rex|ex gr.||',
	'Tyrannosaurus "rex"' => 'Tyrannosaurus||||rex|"||',
	'Tyrannosaurus <form A>' => 'Tyrannosaurus||||form A|informal||',
	'Tyrannosaurus sp. cf. T. rex' => 'Tyrannosaurus||||rex|cf.||',
	'Tyrannosaurus cf. T. rex' => 'Tyrannosaurus||||rex|cf.||',
	'Tyrannosaurus ind.' => 'Tyrannosaurus||||indet.|||',
	'cf. Tyrannosaurus' => 'Tyrannosaurus|cf.||||||',
	'? Tyrannosaurus' => 'Tyrannosaurus|?||||||',
	'"Tyrannosaurus"' => 'Tyrannosaurus|"||||||',
	'<Tyrannosaur A>' => 'Tyrannosaur A|informal||||||',
	'Drepanomeryx ("Matthomeryx")' => 'Drepanomeryx||Matthomeryx|"||||',
	'Tyrannosaurus s.l.' => 'Tyrannosaurus|sensu lato||||||',
	'cf. Drepanomeryx (Matthomeryx) s.l.' => 'Drepanomeryx|cf.|Matthomeryx|sensu lato||||',
	'cf. Tyrannosaurus sp' => 'Tyrannosaurus|cf.|||sp.|||',
	'cf. Tyrannosaurus spp' => 'Tyrannosaurus|cf.|||spp.|||',
	'cf. Tyrannosaurus sspp.' => 'Tyrannosaurus|cf.|||spp.|||',
	'Martes caurina cf. nobilis' => 'Martes||||caurina||nobilis|cf.',
	'Martes caurina var. nobilis' => 'Martes||||caurina||nobilis|var',
	'Martes caurina forma nobilis' => 'Martes||||caurina||nobilis|forma',
	'cf. Martes caurina var nobilis' => 'Martes|cf.|||caurina||nobilis|var',
	'cf. Martes caurina forma. nobilis' => 'Martes|cf.|||caurina||nobilis|forma',
	'cf. Martes caurina morph nobilis' => 'Martes|cf.|||caurina||nobilis|morph',
	'cf. Martes caurina mut. nobilis' => 'Martes|cf.|||caurina||nobilis|mut',
	'Martes caurina v. nobilis' => 'Martes||||caurina||nobilis|var',
	'cf. Martes caurina va nobilis' => 'Martes|cf.|||caurina||nobilis|var',
	'cf. Martes caurina f. nobilis' => 'Martes|cf.|||caurina||nobilis|forma',
	'cf. Martes caurina form nobilis' => 'Martes|cf.|||caurina||nobilis|forma',
	'cf. Martes caurina mo. nobilis' => 'Martes|cf.|||caurina||nobilis|morph',
	'cf. Martes caurina mor. nobilis' => 'Martes|cf.|||caurina||nobilis|morph',
	'cf. Martes caurina mu. nobilis' => 'Martes|cf.|||caurina||nobilis|mut',
	'Martes caurina "nobilis"' => 'Martes||||caurina||nobilis|"',
	'Martes caurina <undescribed form>' => 'Martes||||caurina||undescribed form|informal',
	'Tyrannosaurus rex sensu stricto' => 'Tyrannosaurus|sensu stricto|||rex|||',
	'Tyrannosaurus rex s.s.' => 'Tyrannosaurus|sensu stricto|||rex|||',
	'Tyrannosaurus rex sensu lato' => 'Tyrannosaurus|sensu lato|||rex|||',
	'cf. Tyrannosaurus rex s. l.' => 'Tyrannosaurus|cf.|||rex|sensu lato||',
	'Tyrannosaurus cf. rex sensu stricto' => 'Tyrannosaurus|sensu stricto|||rex|cf.||',
	'Tyrannosaurus n. sp. foo' => 'Tyrannosaurus||||foo|n. sp.||',
	'Tyrannosaurus foo n. sp.' => 'Tyrannosaurus||||foo|n. sp.||',
	'Tyrannosaurus foo sp. nov.' => 'Tyrannosaurus||||foo|n. sp.||',
	'Novogenus n. gen. novus n. sp.' => 'Novogenus|n. gen.|||novus|n. sp.||',
	'Tyrannosaurus (Novosub) n. subgen. novus n. sp.' => 'Tyrannosaurus||Novosub|n. subgen.|novus|n. sp.||',
	'Martes caurina n. ssp. novella' => 'Martes||||caurina||novella|n. ssp.',
	'  Martes caurina nobilis  ' => 'Martes||||caurina||nobilis|' );
    
    foreach my $name ( sort keys %ok )
    {
	is( parse($name), $ok{$name}, "parsed '$name'" );
    }
    
    is( parse('cf. Tyrannosaurus spp.'), 'Tyrannosaurus|cf.|||spp.|||',
	"'spp.' is kept by the full parser" );
};


# Names that cannot be parsed, or that have conflicting or misplaced modifiers, produce an
# error message.

subtest 'errors' => sub {
    
    my %error = (
	'1234' => "Names must contain at least two letters in a row",
	'T' => "Names must contain at least two letters in a row",
	'Tyrann% rex' => "Wildcards are not allowed",
	'T. rex' => "Wildcards are not allowed",
	'tyrannosaurus rex' => "Bad capitalization on genus name",
	'Tyrannosaurus Rex' => "Bad capitalization on species name",
	'Tyrannosaurus (dinosaurus) rex' => "Bad capitalization on subgenus",
	'Tyrannosaurus (Foo rex' => "Missing ')' after the subgenus name 'Foo'",
	'Tyrannosaurus (cf. Foo) rex' => "The modifier 'cf.' must come before the parenthesis, as in 'cf. (Foo)'",
	'Tyrannosaurus (F00) rex' => "Invalid subgenus name 'F00'",
	'Tyrannosaurus (Foo Bar) rex' => "Invalid subgenus name 'Foo Bar'",
	'Tyrannosaurus () rex' => "Missing subgenus name",
	'Tyrann0saurus rex' => "Invalid genus name 'Tyrann0saurus'",
	'cf. Tyrannosaurus rex0' => "Invalid species name 'rex0'",
	'cf. Martes caurina nob1lis' => "Invalid subspecies name 'nob1lis'",
	'cf. aff. Tyrannosaurus rex' => "You cannot specify 'cf.' and 'aff.' together",
	'? cf. Tyrannosaurus rex' => "You cannot specify '?' and 'cf.' together",
	'Tyrannosaurus cf. aff. (Foo) rex' => "You cannot specify 'cf.' and 'aff.' together",
	'Tyrannosaurus cf. aff. rex' => "You cannot specify 'cf.' and 'aff.' together",
	'Martes caurina cf. aff. nobilis' => "You cannot specify 'cf.' and 'aff.' together",
	'Martes caurina cf. var. nobilis' => "You cannot specify 'cf.' and 'var.' together",
	'Martes caurina m. nobilis' => "Ambiguous subspecies modifier 'm.': use 'morph' or 'mut'",
	'cf. Martes caurina m nobilis' => "Ambiguous subspecies modifier 'm': use 'morph' or 'mut'",
	'Tyrannosaurus cf. X. rex' => "Genus initial after 'cf.' did not match genus",
	'Tyrannosaurus var' => "Missing species name",
	'Tyrannosaurus x' => "Invalid species name 'x'",
	'cf. Tyrannosaurus spx.' => "Invalid species name 'spx.'",
	'Tyrannosaurus re' => "Invalid species name 're'",
	'Martes caurina no' => "Invalid subspecies name 'no'",
	'Tyrannosaurus ?' => "Missing species name",
	'Tyrannosaurus (Novosub) n. subgen.' => "Missing species name",
	'cf. Tyrannosaurus s.l.' => "Conflicting modifiers 'cf.' and 'sensu lato'",
	'cf. Tyrannosaurus sp. s.l.' => "Conflicting modifiers 'cf.' and 'sensu lato'",
	'Novogenus n. gen.' => "Missing species name",
	'Novogenus n. gen. novus' => "You forgot to include 'n. sp.'",
	'Tyrannosaurus rex n. gen.' => "You forgot to include 'n. sp.'",
	'Martes caurina nobilis n. sp.' => "Invalid modifier 'n. sp.'",
	'Tyrannosaurus n. sp.' => "Missing species name",
	'<Foo> n. sp. bar' => "Conflicting modifiers '<>' and 'n. sp.'",
	'Tyrannosaurus cf. n. sp. rex' => "Conflicting species modifiers 'n. sp.' and 'cf.'",
	'Tyrannosaurus n. sp. rex s.s.' => "Conflicting modifiers 'n. sp.' and 'sensu stricto'" );
    
    foreach my $name ( sort keys %error )
    {
	is( parse($name), "ERROR: $error{$name}", "'$name' was rejected" );
    }
    
    is( parse(''), '|||||||', "an empty name gives empty fields" );
    
    is( parse('Martes caurina cf. vulgaris'), 'Martes||||caurina||vulgaris|cf.',
	"a subspecies that begins like a modifier is not taken as a second modifier" );
    
    foreach my $name ( 'vulgaris', 'formosa', 'mutabilis', 'morio' )
    {
	is( parse("cf. Martes caurina $name"), "Martes|cf.|||caurina||$name|",
	    "the subspecies '$name' is kept whole" );
    }
};


# With 'loose', bad capitalization is corrected, but a species modifier without a species
# name, or a new genus or subgenus without a species, is still an error. With 'wildcards',
# '%' and '_' are accepted, and a genus initial is turned into a wildcard.

subtest 'options' => sub {
    
    my %loose = ( 'tyrannosaurus rex' => 'Tyrannosaurus||||rex|||',
		  'TYRANNOSAURUS rex' => 'Tyrannosaurus||||rex|||',
		  'Tyrannosaurus Rex' => 'Tyrannosaurus||||rex|||',
		  'Tyrannosaurus (dinosaurus) rex' => 'Tyrannosaurus||Dinosaurus||rex|||',
		  'cf. Tyrannosaurus' => 'Tyrannosaurus|cf.||||||' );
    
    foreach my $name ( sort keys %loose )
    {
	is( parse($name, { loose => 1 }), $loose{$name}, "loose parse of '$name'" );
    }
    
    foreach my $name ( 'Novogenus n. gen.', 'Tyrannosaurus (Novosub) n. subgen.', 'Tyrannosaurus cf.',
		       'Tyrannosaurus ?', 'Tyrannosaurus n. sp.' )
    {
	is( parse($name, { loose => 1 }), "ERROR: Missing species name",
	    "'$name' is rejected even with 'loose'" );
    }
    
    is( parse('Tyrann% rex', { wildcards => 1 }), 'Tyrann%||||rex|||', "a wildcard is accepted" );
    is( parse('T. rex', { wildcards => 1 }), 'T%||||rex|||', "a genus initial becomes a wildcard" );
};


# Anything left after the subspecies name is an error.

subtest 'extra text' => sub {
    
    my %extra = ( 'Nosuchtaxon foo bar baz' => 'baz',
		  'Nosuchtaxon foo bar baz qux' => 'baz qux',
		  'Martes caurina <undescribed form> extra' => 'extra' );
    
    foreach my $name ( sort keys %extra )
    {
	is( parse($name), "ERROR: Unexpected text after the subspecies name: '$extra{$name}'",
	    "the extra text in '$name' was reported" );
    }
};


# constructIdentifiedName puts the eight fields back together. Modifiers that begin with
# 'n.' or 'sensu' go at the end, and a name whose every component is quoted is quoted as a
# whole.

subtest 'constructing names' => sub {
    
    my %names = ( 'Tyrannosaurus rex' => 'Tyrannosaurus rex',
		  'cf. Tyrannosaurus aff. rex' => 'cf. Tyrannosaurus aff. rex',
		  'Drepanomeryx (Matthomeryx) matthewi' => 'Drepanomeryx (Matthomeryx) matthewi',
		  '<Tyrannosaur A> rex' => '<Tyrannosaur A> rex',
		  'Tyrannosaurus ("Foo") rex' => 'Tyrannosaurus ("Foo") rex',
		  'Martes caurina forma nobilis' => 'Martes caurina forma nobilis',
		  'Tyrannosaurus n. sp. foo' => 'Tyrannosaurus foo n. sp.',
		  'Novogenus n. gen. novus n. sp.' => 'Novogenus novus n. gen. n. sp.',
		  'Tyrannosaurus rex s.s.' => 'Tyrannosaurus rex sensu stricto',
		  'Tyrannosaurus sp. cf. T. rex' => 'Tyrannosaurus cf. rex',
		  'cf. Tyrannosaurus' => 'cf. Tyrannosaurus',
		  'cf. Drepanomeryx (Matthomeryx) s.l.' => 'cf. Drepanomeryx (Matthomeryx) sensu lato' );
    
    foreach my $name ( sort keys %names )
    {
	is( constructIdentifiedName(parseIdentifiedName($name)), $names{$name},
	    "'$name' was reconstructed" );
    }
    
    is( constructIdentifiedName('Tyrannosaurus', '"', '', '', 'rex', '"'), '"Tyrannosaurus rex"',
	"a name whose components are all quoted is quoted as a whole" );
    
    is( parse('"Martes caurina nobilis"'), 'Martes|"|||caurina|"|nobilis|"',
	"every component of a quoted name has the modifier '\"'" );
    
    is( constructIdentifiedName(parseIdentifiedName('"Martes caurina nobilis"')),
	'"Martes caurina nobilis"', "a quoted name with a subspecies is reconstructed" );
};


# splitTaxonName splits the name of a taxon from the authorities table into genus,
# subgenus, species, and subspecies.

subtest 'splitting taxon names' => sub {
    
    my %split = ( 'Tyrannosaurus' => 'Tyrannosaurus|||',
		  'Tyrannosaurus rex' => 'Tyrannosaurus||rex|',
		  'Drepanomeryx (Matthomeryx) matthewi' => 'Drepanomeryx|Matthomeryx|matthewi|',
		  'Martes caurina nobilis' => 'Martes||caurina|nobilis',
		  'Tyrannosauridae' => 'Tyrannosauridae|||',
		  'tyrannosaurus rex' => 'Tyrannosaurus||rex|',
		  'TYRANNO% REX' => 'Tyranno%||rex|',
		  '' => '|||',
		  'Bad Name Here Too' => '|||' );
    
    foreach my $name ( sort keys %split )
    {
	is( join('|', map { $_ // '' } splitTaxonName($name)), $split{$name}, "split '$name'" );
    }
    
    is( join('|', map { $_ // '' } splitTaxonName('martes caurina nobilis')), 'Martes||caurina|nobilis',
	"a lowercase name with a subspecies is split" );
};


# computeTaxonMatch scores how closely a name matches the name of a taxon: 40 for an exact
# match, 30-39 for a subspecies, 20-29 for a species, 10-19 for a genus or subgenus, and 0
# for no match.

subtest 'match scores' => sub {
    
    my @cases = (
	[ 'Martes||caurina|nobilis', 'Martes||caurina|nobilis', 40, "exact subspecies" ],
	[ 'Drepanomeryx|Matthomeryx|matthewi|', 'Drepanomeryx|Matthomeryx|matthewi|', 30, "exact species" ],
	[ 'Drepanomeryx||matthewi|', 'Drepanomeryx|Matthomeryx|matthewi|', 28, "species without its subgenus" ],
	[ 'Matthomeryx||matthewi|', 'Drepanomeryx|Matthomeryx|matthewi|', 27, "subgenus used as genus" ],
	[ 'Foo|Drepanomeryx|falciformis|', 'Drepanomeryx||falciformis|', 26, "genus used as subgenus" ],
	[ 'Foo|Drepanomeryx|falciformis|', 'Bar|Drepanomeryx|falciformis|', 25, "only the subgenus and species match" ],
	[ 'Drepanomeryx|Matthomeryx||', 'Drepanomeryx|Matthomeryx||', 19, "genus and subgenus" ],
	[ 'Tyrannosaurus|||', 'Tyrannosaurus|||', 18, "genus" ],
	[ 'Matthomeryx|||', 'Drepanomeryx|Matthomeryx||', 17, "subgenus used as genus" ],
	[ 'Foo|Tyrannosaurus||', 'Tyrannosaurus|||', 15, "genus used as subgenus" ],
	[ 'Tyrannosaurus||rex|', 'Albertosaurus||rex|', 0, "different genus" ],
	[ '|||', 'Tyrannosaurus|||', 0, "empty name" ] );
    
    foreach my $c ( @cases )
    {
	my ($occ, $taxon, $score, $label) = @$c;
	
	is( computeTaxonMatch(split(/\|/, $occ, 4), split(/\|/, $taxon, 4)), $score, "score for $label" );
    }
};


# defaultSpeciesName gives the species name of something identified only to a taxon: 'sp.'
# for a genus, a subgenus, or no taxon, and 'indet.' above the genus level.

subtest 'default species names' => sub {
    
    is( defaultSpeciesName($dbh, 38613), 'sp.', "a genus" );
    is( defaultSpeciesName($dbh, 138948), 'sp.', "a subgenus" );
    is( defaultSpeciesName($dbh, 38606), 'indet.', "a family" );
    is( defaultSpeciesName($dbh, 0), 'sp.', "no taxon" );
    is( defaultSpeciesName($dbh, undef), 'sp.', "an undefined taxon" );
    is( defaultSpeciesName($dbh, 999999999), 'sp.', "a taxon that does not exist" );
};


# hashIdentification combines a name and a taxon number into a check value.

subtest 'hash' => sub {
    
    my $h1 = hashIdentification('Tyrannosaurus|||rex', 54833);
    
    is( $h1, (unpack("%32L*", "Tyrannosaurus|||rex\0") + 54833) % $OccurrenceBase::HASH_MODULUS,
	"the hash is computed from the name, padded to a multiple of four bytes, and the number" );
    
    is( hashIdentification('Tyrannosaurus|||rex', 54833), $h1, "the hash is repeatable" );
    
    isnt( hashIdentification('Tyrannosaurus|||rex', 38613), $h1, "a different number gives a different hash" );
    
    isnt( hashIdentification('Albertosaurus|||rex', 54833), $h1, "a different name gives a different hash" );
    
    isnt( hashIdentification('Tyrannosaurus|||foo', 54833), $h1,
	  "names that differ only in their last characters give different hashes" );
};


# matchIdentifiedName finds the taxa that best match a name. An exact match is preferred;
# otherwise a species may match in a different subgenus, a subgenus may be used as a
# genus, and so on. Only the matches in the best class of score are returned.

subtest 'matching names' => sub {
    
    is( match('Drepanomeryx'), 'Drepanomeryx', "a genus" );
    is( match('Drepanomeryx', 'Matthomeryx'), 'Drepanomeryx (Matthomeryx)', "a subgenus" );
    is( match('Drepanomeryx', 'Matthomeryx', 'matthewi'), 'Drepanomeryx (Matthomeryx) matthewi',
	"a species in a subgenus" );
    is( match('Drepanomeryx', '', 'falciformis'), 'Drepanomeryx falciformis',
	"a species without its subgenus" );
    is( match('Drepanomeryx', '', 'matthewi'), 'Drepanomeryx (Matthomeryx) matthewi',
	"a species whose subgenus was left out" );
    is( match('Matthomeryx', '', 'matthewi'), 'Drepanomeryx (Matthomeryx) matthewi',
	"a subgenus used as the genus" );
    is( match('Matthomeryx'), 'Drepanomeryx (Matthomeryx)', "a subgenus by itself" );
    is( match('Foo', 'Drepanomeryx', 'falciformis'),
	'Drepanomeryx (Drepanomeryx) falciformis; Drepanomeryx falciformis',
	"a genus used as the subgenus" );
    is( match('Martes', '', 'caurina', 'nobilis'), 'Martes caurina nobilis', "a subspecies" );
    is( match('Tyrannosaurus', '', 'foo'), 'Tyrannosaurus', "an unknown species of a known genus" );
    is( match('Tyrannosaurus', '', 'rex', 'foo'), 'Tyrannosaurus rex',
	"an unknown subspecies of a known species" );
    is( match('Tyrannosauridae'), 'Tyrannosauridae', "a family" );
    is( match('Nosuch', '', 'thing'), '', "an unknown name" );
    
    is( match('Drepanomeryx', 'Matthomeryx', 'foo'), 'Drepanomeryx (Matthomeryx)',
	"an unknown species of a known subgenus matches the subgenus" );
};


# A taxon named 'Xxx (Xxx)' should be discarded in favor of 'Xxx' only if both are found.
# A made-up taxon 'Fakegenus (Fakegenus)', with no 'Fakegenus', checks that it is kept
# when it is the only match. It is a copy of Drepanomeryx (Drepanomeryx), renamed, and is
# removed afterward.

subtest 'subgenus with the name of its genus' => sub {
    
    sql_command("CREATE TEMPORARY TABLE $TEST_DB.lib01_fake SELECT * FROM <<AUTHORITY_DATA>>
		WHERE taxon_no = 138945");
    sql_command("UPDATE $TEST_DB.lib01_fake SET taxon_no = 900001, orig_no = 900001,
		taxon_name = 'Fakegenus (Fakegenus)', subgenus_index = 'Fakegenus'");
    sql_command("INSERT INTO <<AUTHORITY_DATA>> SELECT * FROM $TEST_DB.lib01_fake");
    sql_command("DROP TEMPORARY TABLE $TEST_DB.lib01_fake");
    
    is( match('Drepanomeryx', '', 'foo'), 'Drepanomeryx',
	"'Drepanomeryx' is preferred to 'Drepanomeryx (Drepanomeryx)'" );
    
    is( match('Fakegenus', '', 'foo'), '900001', "'Fakegenus (Fakegenus)' is kept when it is the only match" );
    is( match('Fakegenus'), '900001', "'Fakegenus (Fakegenus)' matches the genus name by itself" );
    
    sql_command("DELETE FROM <<AUTHORITY_DATA>> WHERE taxon_no = 900001");
};


# A name with a wildcard is matched with patterns instead of exactly. The patterns are
# generated from the wildcard name components. Wildcard components are not used when
# scoring the candidates, because computeTaxonMatch compares each component exactly, so a
# candidate that matches only through a wildcard is discarded.

subtest 'wildcards' => sub {
    
    my $result = eval { match('Drepanomeryx', 'Matth%', 'matthewi') };
    
    ok( ! $@, "a name with a wildcard does not throw an exception" ) || diag("exception: $@");
    
    is( $result, 'Drepanomeryx (Matthomeryx) matthewi', "a species with a '%' in the subgenus" );
    
    is( match('Drepanomeryx', 'M_tthomeryx', 'matthewi'), 'Drepanomeryx (Matthomeryx) matthewi',
	"a species with a '_' in the subgenus" );
    
    is( match('Drepanomeryx', 'Drep%', 'falciformis'),
	'Drepanomeryx (Drepanomeryx) falciformis; Drepanomeryx falciformis',
	"a species with a '%' in the subgenus, which matches it both with and without" );
    
    foreach my $species ( 'matthewi', undef )
    {
	my $collector = SQLCollector->new;
	
	matchIdentifiedName($dbh, $collector, 'Drepanomeryx', 'Matth%', $species, undef);
	
	my $sql = $collector->output;
	my $which = $species ? "with a species" : "without a species";
	
	like( $sql, qr{ [(] \]? Matth\[a-zA-Z\]\* }xs,
	      "the subgenus pattern was generated from the subgenus name, $which" ) || diag($sql);
	
	unlike( $sql, qr{ [(] \]? \\? \[ \) \] }xs,
		"no query has an empty subgenus pattern, $which" ) || diag($sql);
    }
  
  TODO: {
	local $TODO = "computeTaxonMatch does not handle wildcards";
	
	is( match('Drepanomeryx', 'Matth%'), 'Drepanomeryx (Matthomeryx)', "a subgenus with a '%'" );
	
	is( match('Drepano%', '', 'matthewi'), 'Drepanomeryx (Matthomeryx) matthewi',
	    "a species with a '%' in the genus" );
    }
};


# matchExactName is used for new names: it returns the most specific taxon whose name is
# exactly one of the name, its species, its subgenus, or its genus.

subtest 'matching new names' => sub {
    
    my %cases = ( 'Tyrannosaurus||rex|' => 'Tyrannosaurus rex',
		  'Tyrannosaurus||novus|' => 'Tyrannosaurus',
		  'Drepanomeryx|Matthomeryx|novus|' => 'Drepanomeryx (Matthomeryx)',
		  'Martes||caurina|nobilis' => 'Martes caurina nobilis',
		  'Nosuch||novus|' => '' );
    
    foreach my $name ( sort keys %cases )
    {
	my @matches = matchExactName($dbh, undef, split(/\|/, $name, 4));
	
	is( join('; ', map { $TAXON{$_} // $_ } @matches), $cases{$name}, "exact match for '$name'" );
    }
};


# findParentTaxon returns the taxon of the specified name among the three nearest
# ancestors of a taxon in the classification. The expected values are read from the real
# taxon_trees table.

subtest 'parent taxa' => sub {
    
    my (@names, @spellings);
    
    my ($orig_no) = $dbh->selectrow_array("SELECT orig_no FROM $TABLE{AUTHORITY_DATA} WHERE taxon_no = 54833");
    
    foreach my $level ( 1 .. 4 )
    {
	my ($immpar) = $dbh->selectrow_array("SELECT immpar_no FROM $TABLE{TAXON_TREES} WHERE orig_no = $orig_no");
	last unless $immpar;
	my ($name, $spelling) = $dbh->selectrow_array("SELECT name, spelling_no FROM $TABLE{TAXON_TREES}
		WHERE orig_no = $immpar");
	push @names, $name; push @spellings, $spelling;
	$orig_no = $immpar;
    }
    
    ok( @names == 4, "T. rex has at least four ancestors" ) || return;
    
    foreach my $i ( 0 .. 2 )
    {
	is( findParentTaxon($dbh, undef, 54833, $names[$i]), $spellings[$i],
	    "ancestor " . ($i+1) . " of T. rex, $names[$i], was found" );
    }
    
    is( findParentTaxon($dbh, undef, 54833, $names[3]), undef,
	"the fourth ancestor, $names[3], is too far away" );
    
    is( findParentTaxon($dbh, undef, 54833, 'Nosuch'), undef, "an unrelated name is not found" );
};


# matchOrigName returns the orig_no of each taxon whose name is exactly the specified
# name.

subtest 'original names' => sub {
    
    my @result = OccurrenceBase::matchOrigName($dbh, undef, 'Nosuch', '', 'thing');
    
    is( scalar(@result), 0, "an unknown name has no match" );
    
    is( join(',', OccurrenceBase::matchOrigName($dbh, undef, 'Tyrannosaurus', '', 'rex')), '54833',
	"the orig_no of T. rex was returned" );
};


# Remove the test taxa.

subtest 'cleanup' => sub {
    
    clear_table('AUTHORITY_DATA');
    
    pass("the test taxa were removed");
};
