# -*- mode: CPerl -*-
# 
# PBDB 1.2
# --------
# 
# The purpose of this file is to test the /data1.2/taxa/list operation,
# including all of the numerous parameters.
# 

use strict;
use feature 'unicode_strings';

use Test::Most tests => 34;

use lib 't';

use Tester;
use Test::Conditions;

# Start by creating an instance of the Tester class, with which to conduct the
# following tests.

my $T = Tester->new({ prefix => 'data1.2' });


my $TEST_VAR_1 = 'Aceratherinae';
my $TEST_ACC_1 = 'Aceratheriinae';


# AAA

# First we test the basic operation of parameter 'taxon_name', alias 'name'.

subtest 'taxon_name basic' => sub {
    
    my $TEST_NAME_3 = 'Felis';
    my $TEST_NAME_4 = 'Canis';
    
    # First check that the parameter 'name' with no wildcards in the value
    # will return records with the actual names given. 
    
    my %found1 = $T->fetch_record_values("/taxa/list.json?name=$TEST_NAME_3,$TEST_NAME_4",
					'nam', "list by name");
    
    unless ( keys %found1 && ! $found1{NO_RECORDS} )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    cmp_ok(keys %found1, '==', 2, "list by name found 2 records");
    ok($found1{$TEST_NAME_3} && $found1{$TEST_NAME_4}, "list by name found both records");
    
    # Then check that 'taxon_name' will do the same.
    
    my %found2 = $T->fetch_record_values("/taxa/list.json?taxon_name=$TEST_NAME_3,$TEST_NAME_4",
				     'nam', "list by taxon_name");
    
    cmp_ok(keys %found2, '==', 2, "list by taxon_name found 2 records");
    ok($found2{$TEST_NAME_3} && $found2{$TEST_NAME_4}, "list by taxon_name found both records");
};


# Then check that bad names generate the proper errors.

subtest 'taxon_name bad' => sub {
    
    # First try some bad characters.
    
    my $BAD_1 = 'Felis @catus';
    
    my $m1 = $T->fetch_nocheck("/taxa/list.json?name=$BAD_1", "bad character");
    
    $T->ok_response_code( $m1, '200', "bad character got 200 response" );
    $T->ok_warning_like( $m1, qr{invalid character}, "bad character got proper warning" );
    $T->ok_no_records( $m1, "bad response got no records" );
    
    # Then try some badly formatted names.
    
    my $BAD_2a = "(not) a taxon";
    my $BAD_2b = "not (a) taxon";
    my $BAD_2c = "not a (taxon)";
    my $BAD_2d = "not a taxon because it has too many words";
    
    my $m2a = $T->fetch_nocheck("/taxa/list.json?name=$BAD_2a", "bad name a");
    my $m2b = $T->fetch_nocheck("/taxa/list.json?name=$BAD_2b", "bad name b");
    my $m2c = $T->fetch_nocheck("/taxa/list.json?name=$BAD_2c", "bad name c");
    my $m2d = $T->fetch_nocheck("/taxa/list.json?name=$BAD_2d", "bad name d");
    
    $T->ok_response_code( $m2a, '200', "bad name a got 200 response" );
    $T->ok_response_code( $m2b, '200', "bad name b got 200 response" );
    $T->ok_response_code( $m2c, '200', "bad name c got 200 response" );
    $T->ok_response_code( $m2d, '200', "bad name d got 200 response" );
    
    $T->ok_warning_like( $m2a, qr{match the pattern}, "bad name a got proper warning" );
    $T->ok_warning_like( $m2a, qr{not match}, "bad name a got proper warning" );
    $T->ok_warning_like( $m2a, qr{match the pattern}, "bad name a got proper warning" );
    $T->ok_warning_like( $m2a, qr{match the pattern}, "bad name a got proper warning" );
    
    $T->ok_no_records( $m2a, "bad name a got no records" );
    $T->ok_no_records( $m2b, "bad name b got no records" );
    $T->ok_no_records( $m2d, "bad name c got no records" );
    $T->ok_no_records( $m2d, "bad name d got no records" );
    
    # Then add "strict".
    
    my $m3a = $T->fetch_nocheck("/taxa/list.json?name=$BAD_2a&strict");
    my $m3b = $T->fetch_nocheck("/taxa/list.json?name=$BAD_2b&strict");
    
    $T->ok_response_code( $m3a, '400', "bad name a strict got 400 response" );
    $T->ok_response_code( $m3b, '404', "bad name b strict got 404 response" );
    
    # Try a mixture of good and bad names with 'strict=no'.
    
    my $GOOD_1 = 'Felis';
    
    my (@r4a) = $T->fetch_records("/taxa/list.json?name=$BAD_2a,$GOOD_1&strict=no", 
				  "good and bad a", { no_diag => 1 });
    
    cmp_ok( @r4a, '==', 1, "good and bad a found one record" );
    is( $r4a[0]{nam}, $GOOD_1, "good and bad a found proper record" );
    $T->ok_response_code( '200', "good and bad a got 200 response" );
    $T->ok_warning_like( qr{match the pattern}, "good and bad a found proper warning" );
    
    
    my $m4b = $T->fetch_url("/taxa/list.json?name=$BAD_2b,$GOOD_1&strict=no", 
			    "good and bad b", { no_diag => 1 });
    
    my (@r4b) = $T->extract_records($m4b, "good and bad b");
    
    cmp_ok( @r4b, '==', 1, "good and bad b found one record" );
    is( $r4b[0]{nam}, $GOOD_1, "good and bad b found proper record" );
    $T->ok_response_code( $m4b, '200', "good and bad b got 200 response" );
    $T->ok_warning_like( $m4b, qr{not match}, "good and bad b found proper warning" );
    
    # Same with 'strict'.
    
    my $m5a = $T->fetch_nocheck("/taxa/list.json?name=$BAD_2a,$GOOD_1&strict", "good and bad a strict");
    my $m5b = $T->fetch_nocheck("/taxa/list.json?name=$BAD_2b,$GOOD_1&strict", "good and bad b strict");
    
    $T->ok_response_code( $m5a, '400', "good and bad a strict got 400 response" );
    $T->ok_response_code( $m5b, '404', "good and bad a strict got 404 response" );
};


# Then check that wildcards work properly with parameter 'taxon_name', alias 'name'.

subtest 'taxon_name with wildcards' => sub {

    my $TEST_WC_1 = 'Pantherin.';
    my $TEST_WC_2 = 'Pantherin%';
    my $TEST_WC_3 = 'Pant%rinae';
    my $TEST_WC_4 = 'Pant_erinae';
    my $TEST_WC_5 = 'F.catus';
    my $TEST_WC_6 = 'F.    catus';
    
    my $PAT_WC_1 = qr{^Pantherin};
    my $PAT_WC_3 = qr{^Pant.*rinae$};
    my $PAT_WC_4 = qr{^Pant.erinae$};
    my $PAT_WC_5 = qr{^F.* catus$};
    
    # Check that wildcard characters . % _ will actually return matching
    # names, both inline and at the end of the argument.
    
    my @r1 = $T->fetch_records("/taxa/list.json?name=$TEST_WC_1", "wildcard '.'");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    cmp_ok( scalar(@r1), '==', 1, "wildcard '.' found one record" );
    like( $r1[0]{nam}, $PAT_WC_1, "wildcard '.' found matching name" );
    
    my @r2 = $T->fetch_records("/taxa/list.json?name=$TEST_WC_2", "wildcard '%'");
    
    cmp_ok( scalar(@r2), '==', 1, "wildcard '%' found one record" );
    is( $r1[0]{nam}, $r2[0]{nam}, "wildcards '.' and '%' found same record" );
    
    my @r3 = $T->fetch_records("/taxa/list.json?name=$TEST_WC_3", "wildcard '%' inline" );
    
    cmp_ok( scalar(@r3), '==', 1, "wildcard '%' inline found one record" );
    like( $r3[0]{nam}, $PAT_WC_3, "wildcard '%' inline found matching name" );
    
    my @r4 = $T->fetch_records("/taxa/list.json?name=$TEST_WC_4", "wildcard '_'" );
    
    cmp_ok( scalar(@r4), '==', 1, "wildcard '_' found one record" );
    like( $r4[0]{nam}, $PAT_WC_4, "wildcard '_' found matching name" );
    
    my @r5 = $T->fetch_records("/taxa/list.json?name=$TEST_WC_5", "generic abbrev" );
    
    cmp_ok( scalar(@r5), '==', 1, "wildcard generic abbrev found one record" );
    like( $r5[0]{nam}, $PAT_WC_5, "wildcard generic abbrev found matching name" );
    
    # Check that adding whitespace after a period will not change the result.
    
    my @r6 = $T->fetch_records("/taxa/list.json?name=$TEST_WC_6", "generic abbrev with whitespace");
    is( $r6[0]{oid}, $r5[0]{oid}, "generic abbrev with whitespace found same record" );
};


# Check the basic operation of parameter 'match_name', with and without
# wildcards.

subtest 'match_name basic' => sub {

    my $WC_1a = 't.affinis';
    my $WC_1b  = 'T.    AFFINIS';
    my $WC_PAT_1 = qr{^T.* affinis$|^\w+ \(T.*\) affinis$}s;
    
    # First try a generic abbreviation, both lowercase and uppercase, and with
    # and without whitespace.
    
    my @r1 = $T->fetch_records("/taxa/list.json?match_name=$WC_1a&show=class", "generic abbreviation lowercase");
    my @r2 = $T->fetch_records("/taxa/list.json?match_name=$WC_1b&show=class", "generic abbreviation uppercase whitspace");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Make sure that both patterns find the same list of names, and that all
    # names match the proper pattern. This pattern allows for a subgenus
    # match, as well as a genus match. We should also find at least 10 names
    # in at least 4 separate classes.
    
    is_deeply(\@r1, \@r2, "same records regardless of case and whitespace");
    cmp_ok( @r1, '>=', 10, "generic abbreviation found more than 10 records" );
    
    $T->check_values(\@r1, 'nam', $WC_PAT_1, "generic abbrev");
    $T->cmp_distinct_count(\@r1, 'cll', '>=', 4, "generic abbrev found in more than four classes");
    
    # Check wildcards inside the name.  This test will also make sure that no
    # species are returned (we should never get a species back unless the name
    # pattern contains an explicit space).
    
    my $NAME_5 = 'in%ta';
    my $PAT_5 = qr{^In\w+ta$}s;
    my $NAME_6 = 'i____ta';
    my $PAT_6 = qr{^I\w\w\w\wta$}s;
    
    my @r3 = $T->fetch_records("/taxa/list.json?match_name=$NAME_5", "match % inside name");
    my @r4 = $T->fetch_records("/taxa/list.json?match_name=$NAME_6", "match _ inside name");
    
    if ( @r3 )
    {
	cmp_ok( @r3, '>', 10, "match % inside name found more than ten records" );
	$T->check_values(\@r3, 'nam', $PAT_5, "match % inside name found proper names" );
    }
    
    if ( @r4 )
    {
	cmp_ok( @r4, '>', 2, "match _ inside name found more than two records" );
	$T->check_values(\@r4, 'nam', $PAT_6, "match _ inside name found proper names" );
    }
    
    # Now test match_name without wildcards. This should find at least two
    # records, in at least two classes. The names should match the given
    # pattern, which includes a subgenus match.
    
    my $NAME_2 = 'Ficus';
    my $PAT_2 = qr{^Ficus$|^\w+ \(Ficus\)$}s;
    
    my @r5 = $T->fetch_records("/taxa/list.json?match_name=$NAME_2&show=class", "match without wildcards");
    
    cmp_ok( @r5, '>=', 2, "match without wildcards found at least 2 records" );
    
    $T->check_values( \@r5, 'nam', $PAT_2, "match without wildcards" );
    $T->cmp_distinct_count( \@r5, 'cll', '>=', 2, "match without wildcards found names in at least two classes" );
    
    # Now cross-check that taoxn_name finds the one with the most occurrences.
    
    my $max_occs;
    
    foreach my $r (@r5)
    {
	$max_occs = $r->{noc} if ! defined $max_occs || $max_occs < $r->{noc};
    }
    
    my ($r5a) = $T->fetch_records("/taxa/list.json?taxon_name=$NAME_2", "taxon_name '$NAME_2'");
    
    cmp_ok( $r5a->{noc}, '==', $max_occs, "taxon_name '$NAME_2' found max occurrences" );
    
    # Specifically test that a generic abbreviation will match subgenera. We
    # consider this okay if any of our examples match, since future taxonomic
    # opinions may reclassify one or more of them.
    
    my %MATCH_3 = ( 'T. AFFINIS' => qr{^[^T]\w+ \(T\w+\) affinis$}s, 
		    'L. MINOR' => qr{^[^L]\w+ \(L\w+\) minor$}s,
		    'A. ELEGANS' => qr{^[^A]\w+ \(A\w+\) elegans$}s );
    
    my ($found_subgenus, $subgeneric_name);
    
    foreach my $name ( keys %MATCH_3 )
    {
	my ($m) = $T->fetch_nocheck("/taxa/list.json?match_name=$name", "match for subgenus");
	my (@r) = $T->extract_records($m);
	
	foreach my $r (@r)
	{
	    if ( defined $r->{nam} && $r->{nam} =~ $MATCH_3{$name} )
	    {
		$found_subgenus = $name;
		$subgeneric_name = $r->{nam};
	    }
	}
    }
    
    ok( $found_subgenus, "found a subgenus with generic match" );
    diag( "found '$subgeneric_name' with match_name=$found_subgenus" ) if $found_subgenus;
    
    # Now test a name with the pattern '% species'. Check that it finds at
    # least one subgenus too, with the subgenus name starting with a different
    # letter from the genus name. We will use this in subsequent checks. Also
    # keep track how many occurrences each name has for use below.
    
    my $NAME_4 = '% distincta';
    my $NAME_4s = 'distincta';
    my $PAT_4 = qr{^\w+ distincta$}s;
    my $PAT_4s = qr{^(\w)\w+ \((\w)\w+\) distincta$}s;
    my ($found_oid, $max_occs2, %n_occs, $A, $B);
    
    my (@r6) = $T->fetch_records("/taxa/list.json?match_name=$NAME_4", "match with wildcard genus");
    
    foreach my $r (@r6)
    {
	my $name = $r->{nam};
	
	unless ( $name =~ $PAT_4 )
	{
	    if ( $name =~ $PAT_4s )
	    {
		if ( $1 ne $2 )
		{
		    $A = $1; $B = $2;
		    $found_oid = $r->{oid};
		}
	    }
	    
	    else
	    {
		fail("match with wildcard genus found a species in a subgenus with a different letter");
		last;
	    }
	}
	
	$n_occs{$name} = $r->{noc};
	$max_occs2 = $r->{noc} if ! defined $max_occs2 || $max_occs2 < $r->{noc};
    }
    
    ok( $max_occs2, "found at least one name with occurrences" );
    
    # Now check to make sure we can find this record using both the initial
    # letter of the genus and the initial letter of the subgenus.
    
    if ( $A && $B )
    {
	my (%found6a) = $T->fetch_record_values("/taxa/list.json?match_name=$A. $NAME_4s", 'oid',
						"match with genus letter");
	my (%found6b) = $T->fetch_record_values("/taxa/list.json?match_name=$B. $NAME_4s", 'oid',
						"match with subgenus letter");
	
	ok( $found6a{$found_oid}, "found matching record with genus letter" );
	ok( $found6b{$found_oid}, "found matching record with subgenus letter" );
    }
    
    # Now check to make sure that using 'taxon_name' with the same name will
    # return the name with the maximum number of occurrences (or one of them,
    # if there are more than one!)
    
    my ($r7) = $T->fetch_records("/taxa/list.json?taxon_name=$NAME_4", "taxon_name with wildcard genus");
    
    if ( $r7 )
    {
	cmp_ok( $n_occs{$r7->{nam}}, '==', $max_occs2, "taxon_name found name with maximum occurrences" );
	cmp_ok( $r7->{noc}, '==', $max_occs2, "occurrence count matches" );
    }
    
    # Now test match_name with wildcards in a species name and subspecies
    # name. 
    
    my $NAME_8 = 'Conus % s.';
    my $PAT_8 = qr{s\w+$};
    my $RANK_8 = '2';
    
    my (@r8) = $T->fetch_records("/taxa/list.json?match_name=$NAME_8", "match_name with subspecies wildcard");
    
    $T->check_values(\@r8, 'nam', $PAT_8, "match_name with subspecies wildcard found matching names");
    $T->check_values(\@r8, 'rnk', $RANK_8, "match_name with subspecies wildcard found only subspecies");
    
    cmp_ok( @r8, '>', 3, "match_name with subspecies wildcard found at least 3 subspecies" );
};


# Then check 'match_name' with bad names

subtest 'match_name bad' => sub {
    
    my $BAD_1 = 'not @ taxon';
    my $BAD_2 = 'not (a) taxon';
    
    my $m1 = $T->fetch_nocheck("/taxa/list.json?name=$BAD_1", "bad character");
    
    $T->ok_response_code( $m1, '200', "bad character got 200 response" );
    $T->ok_warning_like( $m1, qr{invalid character}, "bad character got proper warning" );
    $T->ok_no_records( $m1, "bad character got no records" );
    
    my $m2 = $T->fetch_nocheck("/taxa/list.json?name=$BAD_2", "unknown name");
    
    $T->ok_response_code( $m2, '200', "unknown name got 200 response" );
    $T->ok_warning_like( $m2, qr{did not match}, "unknown name got proper warning" );
    $T->ok_no_records( $m2, "unknown name got no records" );
};


# Check that 'base_name' works okay, with and without wildcards.

subtest 'base_name basic' => sub {
    
    my $TEST_BASE_1 = 'Dascillidae';
    my $TEST_BASE_2 = 'Dascilloidea';
    my $TEST_BASE_3 = 'Dascill%';
    my $TEST_BASE_SPECIES_1 = 'Dascillus shandongianus';
    
    my $TEST_NAME_1a = 'Felis';
    my $TEST_NAME_1b = 'Canis';
    my $TEST_NAME_1c = 'Canis dirus';
    my $TEST_NAME_1d = 'Felis catus';
    
    my ($base_1_record, $base_2_record, $sub_1_record, $base_1_species, $base_2_species);
    
    # Test two different base_name values, the second of which is a supertaxon
    # of the first.
    
    my @r1 = $T->fetch_records("/taxa/list.json?base_name=$TEST_BASE_1", "base name 1");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    foreach my $r (@r1)
    {
	$base_1_record = $r if $r->{nam} eq $TEST_BASE_1;
	$base_1_species = $r if $r->{nam} eq $TEST_BASE_SPECIES_1;
    }
    
    my @r2 = $T->fetch_records("/taxa/list.json?base_name=$TEST_BASE_2", "base name 2");
    
    foreach my $r (@r2)
    {
	$base_2_record = $r if $r->{nam} eq $TEST_BASE_2;
	$sub_1_record = $r if $r->{nam} eq $TEST_BASE_1;
	$base_2_species = $r if $r->{nam} eq $TEST_BASE_SPECIES_1;
    }
    
    ok( $base_1_record, 'found base 1' ) &&
    ok( $base_1_record->{flg} =~ /B/, "base 1 has flag 'B'" );
    
    ok( $base_2_record, 'found base 2' ) &&
    ok( $base_2_record->{flg} =~ /B/, "base 2 has flag 'B'" );
    
    ok( $sub_1_record, 'found base 1 under base 2' ) &&
    ok( $sub_1_record !~ /B/, "sub 1 does not have flag 'B'" );
    
    # Check that the second one has a higher rank.
    
    cmp_ok( $base_2_record->{rnk}, '>', $base_1_record->{rnk}, "base 2 ranks higher than base 1" );
    
    ok( $base_1_species, 'found test species under base 1' );
    ok( $base_2_species, 'found test species under base 2' );
    
    is_deeply( $base_1_species, $base_2_species, 'two species records are identical' )
	if $base_1_species && $base_2_species;
    
    # Now test a wildcard pattern that should match both. Check that it
    # returns the same result set as base 2, the higher of the two taxa.
    
    my @r3 = $T->fetch_records("/taxa/list.json?base_name=$TEST_BASE_3", "base name with '%'");
    
    cmp_ok( @r2, '==', @r3, "base name with '%' matches result set for base 2" );
    is( $r2[0]{nam}, $r3[0]{nam}, "base name with '%' matches base 2" );
    
    # Now check base_name with more than one name.
    
    my %found = $T->fetch_record_values("/taxa/list.json?base_name=$TEST_NAME_1a,$TEST_NAME_1b", 'nam',
					"base_name multiple" );
    
    ok( $found{$TEST_NAME_1a}, "base_name multiple found first base name" );
    ok( $found{$TEST_NAME_1b}, "base_name multiple found second base name" );
    ok( $found{$TEST_NAME_1c}, "base_name multiple found first species name" );
    ok( $found{$TEST_NAME_1d}, "base_name multiple found second species name" );    
};


# Then check 'base_name' with bad names

subtest 'base_name bad' => sub {
    
    my $BAD_1 = 'not @ taxon';
    my $BAD_2 = 'not (a) taxon';
    
    my $m1 = $T->fetch_nocheck("/taxa/list.json?base_name=$BAD_1", "bad character");
    
    $T->ok_response_code( $m1, '200', "bad character got 200 response" );
    $T->ok_warning_like( $m1, qr{invalid character}, "bad character got proper warning" );
    $T->ok_no_records( $m1, "bad character got no records" );
    
    my $m2 = $T->fetch_nocheck("/taxa/list.json?base_name=$BAD_2", "unknown name");
    
    $T->ok_response_code( $m2, '200', "unknown name got 200 response" );
    $T->ok_warning_like( $m2, qr{did not match}, "unknown name got proper warning" );
    $T->ok_no_records( $m2, "unknown name got no records" );
};


# Check that 'taxon_name', works properly with exclusions and selectors.

subtest 'taxon_name with modifiers' => sub {
    
    my $PREFIX_1a = 'Plant';
    my $PREFIX_1b = 'Animal';
    my $PREFIX_1c = 'Morac';
    my $PREFIX_1d = 'Mollusc';
    my $MOD_NAME_1 = 'Ficus';
    my $MOD_NAME_2 = 'Ficus sphericus';
    my $EX_NAME_2 = 'Ficus ^sphericus';
    my $EX_RESP_2 = 'Ficus:sphericus';
    
    my $PREFIX_2 = 'Gastropoda';
    my $WC_NAME_2 = '% affinis';
    my $WC_PAT_2 = qr{^\w+ affinis$};
    my $TAXON_NAME_2_N_OCCS;
    
    my $CASE_NAME_1u = 'ANIMAL:FICUS';
    my $CASE_NAME_1l = 'animal:ficus';
    my $CASE_MATCH_1 = 'Ficus';
    
    my $PREFIX_3 = 'Insect';
    my $WC_NAME_3 = 'T. rex';
    my $WC_PAT_3 = qr{^T\w+ rex$};
    
    my $EX_NAME_3 = 'INSECTA^DASCILLUS ELONGATUS^COLEOPTERA';
    my %EX_NAME_3 = ('Insecta' => undef, 'Dascillus elongatus' => 'E', 'Coleoptera' => 'E');
    
    my $EX_NAME_4 = 'insecta  ^  coleoptera  ^  hymenoptera';
    my %EX_NAME_4 = ('Insecta' => undef, 'Coleoptera' => 'E', 'Hymenoptera' => 'E');
    
    my $EX_NAME_5 = 'Gastro:Ficus^Diconoficus';
    my %EX_NAME_5 = ('Ficus' => undef, 'Ficus (Diconoficus)' => 'E');
    
    my $EX_NAME_6 = 'Gastro:Ficus ^affinis';
    my %EX_NAME_6 = ('Ficus' => undef, 'Ficus affinis' => 'E');
    
    my $ODL_1a = 'Urticales';
    my $ODL_1b = 'Gastropoda';
    
    # First check 'taxon_name' with a selector.
    
    my (@r_a) = $T->fetch_records("/taxa/list.json?taxon_name=$PREFIX_1a:$MOD_NAME_1&show=class", 'selector a');
    my (@r_b) = $T->fetch_records("/taxa/list.json?taxon_name=$PREFIX_1b:$MOD_NAME_1&show=class", 'selector b');
    my (@r_c) = $T->fetch_records("/taxa/list.json?taxon_name=$PREFIX_1c:$MOD_NAME_1&show=class", 'selector c');
    my (@r_d) = $T->fetch_records("/taxa/list.json?taxon_name=$PREFIX_1d:$MOD_NAME_1&show=class", 'selector d');
    
    unless ( @r_a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    is( $r_a[0]{nam}, $MOD_NAME_1, "selector '$PREFIX_1a' finds 'MOD_NAME_1'" );
    is( $r_b[0]{nam}, $MOD_NAME_1, "selector '$PREFIX_1b' finds 'MOD_NAME_1'" );
    is( $r_c[0]{nam}, $MOD_NAME_1, "selector '$PREFIX_1c' finds 'MOD_NAME_1'" );
    is( $r_d[0]{nam}, $MOD_NAME_1, "selector '$PREFIX_1d' finds 'MOD_NAME_1'" );
    
    is( $r_a[0]{odl}, $ODL_1a, "selector '$PREFIX_1a' classifies '$MOD_NAME_1' as '$ODL_1a'" );
    is( $r_b[0]{cll}, $ODL_1b, "selector '$PREFIX_1b' classifies '$MOD_NAME_1' as '$ODL_1b'" );
    is( $r_c[0]{odl}, $ODL_1a, "selector '$PREFIX_1c' classifies '$MOD_NAME_1' as '$ODL_1a'" );
    is( $r_d[0]{cll}, $ODL_1b, "selector '$PREFIX_1d' classifies '$MOD_NAME_1' as '$ODL_1b'" );
    
    cmp_ok( @r_a, '==', 1, "selector '$PREFIX_1a' found one name" );
    cmp_ok( @r_b, '==', 1, "selector '$PREFIX_1b' found one name" );
    
    # Then check 'taxon_name' with a selector and a species-level name
    
    my (@r2_a) = $T->fetch_records("/taxa/list.json?taxon_name=$PREFIX_1a:$MOD_NAME_2", "selector a sp");
    my ($m2_b) = $T->fetch_nocheck("/taxa/list.json?taxon_name=$PREFIX_1b:$MOD_NAME_2", "selector b sp");
    
    is( $r2_a[0]{nam}, $MOD_NAME_2, "selector '$PREFIX_1a' finds '$MOD_NAME_2'" );
    $T->ok_warning_like($m2_b, qr{$PREFIX_1b:$MOD_NAME_2.*did not match}i,
			"selector '$PREFIX_1b' does not find '$MOD_NAME_2'" );
    cmp_ok( @r2_a, '==', 1, "selector '$PREFIX_1a' sp found one name" );
    
    # Then check 'taxon_name' with a selector and a species-level exclusion
    
    my (@r3_a) = $T->fetch_records("/taxa/list.json?taxon_name=$PREFIX_1a:$EX_NAME_2", "selector a ex");
    my ($m3_b) = $T->fetch_nocheck("/taxa/list.json?taxon_name=$PREFIX_1b:$EX_NAME_2", "selector b ex");
    
    is( $r3_a[0]{nam}, $MOD_NAME_1, "selector '$PREFIX_1a' with exclusion finds '$MOD_NAME_1'" );
    is( $r3_a[1]{nam}, $MOD_NAME_2, "selector '$PREFIX_1a' with exclusion finds '$MOD_NAME_2'" );
    
    unlike( $r3_a[0]{flag}, qr{E}, "selector '$PREFIX_1a' no exclusion on '$MOD_NAME_1'" );
    like( $r3_a[1]{flg}, qr{E}, "selector '$PREFIX_1a' finds '$EX_NAME_2' with 'E'" );
    
    cmp_ok( @r3_a, '==', 2, "selector '$PREFIX_1a' with exclusion finds two names" );
    
    $T->ok_warning_like($m3_b, qr{$PREFIX_1b:$EX_RESP_2.*did not match}i,
			"selector '$PREFIX_1b' finds proper error for '$EX_NAME_2'" );
    
    # Now check the use of wildcards with selectors.
    
    my ($r4) = $T->fetch_records("/taxa/list.json?taxon_name=$PREFIX_2:$WC_NAME_2", "selector with wildcards 2");
    my ($r5) = $T->fetch_records("/taxa/list.json?taxon_name=$PREFIX_3:$WC_NAME_3", "selector with wildcards 3");
    
    if ( $r4 )
    {
	like( $r4->{nam}, $WC_PAT_2, "selector with wildcards 2 found matching name" );

	# Save the n_occs field of this record for the test below. We will use
	# 'match_name' with the same pattern and check that the name that was
	# returned had the highest number of occurrences.
	
	$TAXON_NAME_2_N_OCCS = $r4->{noc};
    }
    
    if ( $r5 )
    {
	like( $r5->{nam}, $WC_PAT_3, "selector with wildcards 3 found matching name" );
    }
    
    # Check names with selectors in all-upper and all-lower case
    
    my ($r6) = $T->fetch_records("/taxa/list.json?taxon_name=$CASE_NAME_1u", "selector all uppercase");
    my ($r7) = $T->fetch_records("/taxa/list.json?taxon_name=$CASE_NAME_1l", "selector all lowercase");
    
    is( $r6->{nam}, $CASE_MATCH_1, "selector all uppercase found proper name" ) if $r6;
    is( $r7->{nam}, $CASE_MATCH_1, "selector all lowercase found proper name" ) if $r7;
    
    # Check names with exclusions in upper and lower case, and also multiple exclusions.
    # Also make sure that extra whitespace around the ^ character is okay.
    
    my (@r8) = $T->fetch_records("/taxa/list.json?taxon_name=$EX_NAME_3", "multiple exclusions uppercase whitespace");
    
    cmp_ok( @r8, '==', 3, "multiple exclusions uppercase whitespace found 3 names" );
    
    foreach my $r ( @r8 )
    {
	ok( exists $EX_NAME_3{$r->{nam}}, "multiple exclusions '$r->{nam}' has proper name" ) &&
	    is( $r->{flg}, $EX_NAME_3{$r->{nam}}, "multiple exclusions '$r->{nam}' has proper flag" );
	delete $EX_NAME_3{$r->{nam}};
    }
    
    cmp_ok( keys %EX_NAME_3, '==', 0, "multiple exclusions uppercase whitespace found all names" );
    
    my (@r9) = $T->fetch_records("/taxa/list.json?taxon_name=$EX_NAME_4", "multiple exclusions lowercase whitespace");
    
    cmp_ok( @r9, '==', 3, "multiple exclusions lowercase whitespace found 3 names" );
    
    foreach my $r ( @r9 )
    {
	ok( exists $EX_NAME_4{$r->{nam}}, "multiple exclusions '$r->{nam}' has proper name" ) &&
	    is( $r->{flg}, $EX_NAME_4{$r->{nam}}, "multiple exclusions '$r->{nam}' has proper flag" );
	delete $EX_NAME_4{$r->{nam}};
    }
    
    cmp_ok( keys %EX_NAME_4, '==', 0, "multiple exclusions lowercase whitespace found all names" );
    
    # Test exclusion of a subgenus and of a species, both with a selector
    
    my (@r10) = $T->fetch_records("/taxa/list.json?taxon_name=$EX_NAME_5", "selector and subgenus exclusion");
    
    cmp_ok( @r10, '==', 2, "selector and subgenus exclusion found 2 names" );
    
    foreach my $r ( @r10 )
    {
	ok( exists $EX_NAME_5{$r->{nam}}, "selector and subgenus exclusion '$r->{nam}' has proper name" ) &&
	    is( $r->{flg}, $EX_NAME_5{$r->{nam}}, "selector and subgenus exclusion '$r->{nam}' has proper flag" );
	delete $EX_NAME_5{$r->{nam}};
    }
    
    cmp_ok( keys %EX_NAME_5, '==', 0, "selector and subgenus exclusion found all names" );
    
    my (@r11) = $T->fetch_records("/taxa/list.json?taxon_name=$EX_NAME_6", "selector and species exclusion");
    
    cmp_ok( @r11, '==', 2, "selector and species exclusion found 2 names" );
    
    foreach my $r ( @r11 )
    {
	ok( exists $EX_NAME_6{$r->{nam}}, "selector and species exclusion '$r->{nam}' has proper name" ) &&
	    is( $r->{flg}, $EX_NAME_6{$r->{nam}}, "selector and species exclusion '$r->{nam}' has proper flag" );
	delete $EX_NAME_6{$r->{nam}};
    }
    
    cmp_ok( keys %EX_NAME_6, '==', 0, "selector and species exclusion found all names" );
     
    # Test a selector chain, and also test whitespace around the selector delimiters.
    
    my $PREFIX_4 = "insecta  :  coleoptera  :  ";
    my $TEST_NAME_4 = 'Dascillidae';
    
    my ($r12) = $T->fetch_records("/taxa/list.json?taxon_name=$PREFIX_4$TEST_NAME_4", "selector chain whitespace");
    
    if ( $r12 )
    {
	is( $r12->{nam}, $TEST_NAME_4, 'selector chain whitespace found proper name' );
    }
};


# More tests on match_name with selectors and wildcards.

subtest 'match_name with modifiers' => sub {

    ok('placeholder');
    
    # Test match_name with and without selectors, no wildcards
    
    my $NAME_1 = 'Wexfordia';
    my %phyla;
    
    my (@r) = $T->fetch_records("/taxa/list.json?match_name=$NAME_1&show=class", "match double genus");
    
    unless ( @r )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Flag all of the different phyla taxonomic classes these records are
    # found under, and then test them one by one as selectors.
    
    foreach my $r (@r)
    {
	$phyla{$r->{phl}} = 1;
    }
    
    cmp_ok( keys %phyla, '>', 1, "no selectors, '$NAME_1' found in more than one phylum" );
    
    # Truncate each selector to 5 characters, and check that all of the
    # results fall into the same class.
    
    foreach my $phylum (keys %phyla)
    {
	my $selector = substr($phylum, 0, 5);
	
	my (%result_phylum) = $T->fetch_record_values("/taxa/list.json?match_name=$selector:$NAME_1&show=class",
						      'phl', "selector check '$selector'");
	
	cmp_ok( keys %result_phylum, '==', 1, "selector check '$selector' found record in proper phylum" );
	ok( $result_phylum{$phylum}, "selector check '$selector' found record in proper phylum" );
    }
    
    # Then check match_name with a selector and a wildcard both.
    
    my $NAME_2 = 'Metazoa:Ficus a.';
    my $FAMILY_2 = 'Ficidae';
    my $NAME_3 = 'Plant:Ficus s.';
    
    my (@r2) = $T->fetch_records("/taxa/list.json?match_name=$NAME_2&show=class", "selector a with sp. wildcard");
    $T->check_values(\@r2, 'fml', $FAMILY_2, "selector a with sp. wildcard found proper family" ) if @r2;
    
    my (@r3) = $T->fetch_records("/taxa/list.json?match_name=$NAME_3&show=class", "selector b with species wildcard");
    $T->cmp_values(\@r3, 'fml', 'ne', $FAMILY_2, "selector b with sp. wildcard found proper family" ) if @r3;
};


# Now test base_name using modifiers.

subtest 'base_name with modifiers' => sub {
    
    # QUESTION: WHAT DOES conus ^ am% do??? does it exclude every species in
    # conus that begins with am??? $$$
    
    # First test base_name with selectors and wildcards.
    
    my $NAME_1 = 'brachi : ling%';
    my $NAME_1a = 'Lingulida';
    my $RANK_1 = 'order';
    
    my %found1 = $T->fetch_record_values("/taxa/list.json?base_name=$NAME_1&rank=$RANK_1", 'nam', 
					 "selector and wildcard");
    
    if ( $found1{NO_RECORDS} )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    ok( $found1{$NAME_1a}, "selector and wildcard found name '$NAME_1a'" );
    
    # Then test base_name with exclusions
    
    my $NAME_2 = 'brachiopoda^lingulata';
    my $NAME_2a = 'Lingulida';
    my $NAME_2b = 'Strophomenida';
    
    my %found2 = $T->fetch_record_values("/taxa/list.json?base_name=$NAME_2&rank=$RANK_1", 'nam', "exclusion");
    
    ok( $found2{$NAME_2b}, "exclusion found '$NAME_2b'" );
    ok( ! $found2{$NAME_2a}, "exclusion did not find '$NAME_2a'" );
    
    my $NAME_3 = 'Felidae';
    my $NAME_4a = 'felidae^  felis';
    my $NAME_4b = 'FELIS';
    my $NAME_5a = 'FELIDAE  ^  F. CATUS  ^  f. Silvestris';
    my $NAME_5b = 'f. Catus   ,F.silvestris';
    
    my @r3 = $T->fetch_records("/taxa/list.json?base_name=$NAME_3", "base 3");
    my @r4a = $T->fetch_records("/taxa/list.json?base_name=$NAME_4a", "genus exclusion");
    my @r4b = $T->fetch_records("/taxa/list.json?base_name=$NAME_4b", "genus subtree");
    my @r5a = $T->fetch_records("/taxa/list.json?base_name=$NAME_5a", "species exclusion");
    my @r5b = $T->fetch_records("/taxa/list.json?base_name=$NAME_5b", "species subtree");
    
    cmp_ok( @r3, '==', @r4a + @r4b, "genus exclusion record counts match" );
    cmp_ok( @r3, '==', @r5a + @r5b, "species exclusion record counts match" );
    
    my %found3 = $T->extract_values(\@r3, 'nam');
    my %found4a = $T->extract_values(\@r4a, 'nam');
    my %found4b = $T->extract_values(\@r4b, 'nam');
    my %found5a = $T->extract_values(\@r5a, 'nam');
    my %found5b = $T->extract_values(\@r5b, 'nam');
    
    check_exclusion(\%found3, \%found4a, \%found4b);
    check_exclusion(\%found3, \%found5a, \%found5b);
    
    # Also check some higher taxa with exclusions
    
    my $NAME_6 = 'Dinosauria';
    my $NAME_6a = 'Dinosauria^Aves';
    my $NAME_6b = 'Aves';
    my $RANK_6 = 'family,superfamily';
    
    my @r6 = $T->fetch_records("/taxa/list.json?base_name=$NAME_6&rank=$RANK_6", "base 6");
    my @r6a = $T->fetch_records("/taxa/list.json?base_name=$NAME_6a&rank=$RANK_6", "remainder 6");
    my @r6b = $T->fetch_records("/taxa/list.json?base_name=$NAME_6b&rank=$RANK_6", "exclusion 6");
    
    cmp_ok( @r6, '==', @r6a + @r6b, "base 6 exclusion record counts match" );
    
    my %found6 = $T->extract_values(\@r6, 'oid');
    my %found6a = $T->extract_values(\@r6a, 'oid');
    my %found6b = $T->extract_values(\@r6b, 'oid');
    
    check_exclusion(\%found6, \%found6a, \%found6b);
};


# Check that the set of values in %$base_ref is the sum of the values in
# %$remainder_ref and %$exclusion_ref, and that the latter two are mutually
# exclusive.

sub check_exclusion {
    
    my ($base_ref, $remainder_ref, $exclusion_ref) = @_;
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    
    foreach my $n ( keys %$base_ref )
    {
	unless ( $remainder_ref->{$n} || $exclusion_ref->{$n} )
	{
	    fail( "found '$n' in remainder or excluded subtree" );
	    return;
	}
    }
    
    foreach my $n ( keys %$remainder_ref )
    {
	unless ( $base_ref->{$n} )
	{
	    fail( "found remainder '$n' in base subtree" );
	    return;
	}
	
	if ( $exclusion_ref->{$n} )
	{
	    fail( "remainder '$n' does not appear in exclusion" );
	    return;
	}
    }
    
    foreach my $n ( keys %$exclusion_ref )
    {
	unless ( $base_ref->{$n} )
	{
	    fail( "found excluded '$n' in base subtree" );
	    return;
	}
	
	if ( $remainder_ref->{$n} )
	{
	    fail( "exclusion '$n' does not appear in remainder" );
	    return;
	}
    }
}


# Check matching for common names, including wildcards and selectors and
# including a bad name.

subtest 'common names' => sub {
    
    my $NAME_1a = 'felidae:% cat';
    my $NAME_1b = 'canidae:% cat';
    my $PAT_1 = qr{ cat$}i;
    
    # First test match_name= with complex features (selector and wildcard) and common.
    
    my @r1a = $T->fetch_records("/taxa/list.json?match_name=$NAME_1a&common=EN&show=common", 
				"match common many");
    
    unless ( @r1a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    cmp_ok( @r1a, '>=', 5, "match common many found at least 5 names" );
    
    foreach my $r (@r1a)
    {
	like($r->{nm2}, $PAT_1, "match common many found names with proper pattern") || last;
    }
    
    my @r1b = $T->fetch_records("/taxa/list.json?match_name=$NAME_1b&common=EN&show=common", 
				"match common none", { no_records_ok => 1, no_diag => 1 });
    
    cmp_ok( @r1b, '==', 0, "match common none found no names" );
    
    # Then test name= with combinations of EN and S.
    
    my $NAME_2a = 'cetacea';
    my $NAME_2b = 'whale';
    my $NAME_2c = 'cetacean';
    
    my @r2a = $T->fetch_records("/taxa/list.json?name=$NAME_2a&common=EN", 
				"scientific with common", { no_records_ok => 1, no_diag => 1 });
    my @r2b = $T->fetch_records("/taxa/list.json?name=$NAME_2b&common=S", 
				"common with scientific", { no_records_ok => 1, no_diag => 1 });
    my @r2c = $T->fetch_records("/taxa/list.json?name=$NAME_2b&common=EN", 
				"common with commmon", { no_records_ok => 1, no_diag => 1 });
    my @r2d = $T->fetch_records("/taxa/list.json?name=$NAME_2a&common=S", 
				"scientific with scientific");
    
    my @r3a = $T->fetch_records("/taxa/list.json?name=$NAME_2c&common=EN",
				"common alt with common", { no_records_ok => 1, no_diag => 1 });
    
    cmp_ok( @r2a, '==', 0, "scientific with common found no names" );
    cmp_ok( @r2b, '==', 0, "common with scientific found no names" );
    ok( @r2c == 1 || @r3a == 1, "common with common found one name" );
    cmp_ok( @r2d, '==', 1, "scientific with scientific found one name" );
    
    # Then test taxon_name= with a name containing a hyphen, and one
    # containing an invalid character.
    
    my $NAME_4a = 'five-lined skink';
    my $NAME_4b = 'not (a common) name';
    
    my @r4a = $T->fetch_records("/taxa/list.json?taxon_name=$NAME_4a&common=EN&show=common",
				"common with hyphen");
    
    cmp_ok( @r4a, '==', 1, "common with hyphen found one record" ) &&
	is( $r4a[0]{nm2}, $NAME_4a, "common with hyphen found proper record" );
    
    my $m4b = $T->fetch_nocheck("/taxa/list.json?taxon_name=$NAME_4b&common=EN&show=common",
				"common with parentheses");
    
    my @r4b = $T->extract_records($m4b, "common with parentheses", 
			          { no_records_ok => 1, no_diag => 1 });
    
    cmp_ok( @r4b, '==', 0, "common with parentheses found no records" );
    $T->ok_warning_like( $m4b, qr{invalid character}s, "common with parentheses got proper warning" );
    
    # Then test base_name= with common name and matching scientific name.
    
    my $NAME_5a = 'Cat';
    
    my ($r5) = $T->fetch_records("/taxa/single.json?name=$NAME_5a&common=EN", "fetch record for '$NAME_5a'");
    
    my $NAME_5b = $r5->{nam};
    
    ok( $NAME_5b, "found scientific name for '$NAME_5a'" );
    
    my @r5a = $T->fetch_records("/taxa/list.json?name=$NAME_5a&common=en", "base_name common");
    my @r5b = $T->fetch_records("/taxa/list.json?name=$NAME_5b", "base_name scientific");
    
    is_deeply( \@r5a, \@r5b, "base_name common and scientific found same records" );
};


# Check the taxon_id parameter

subtest 'taxon_id' => sub {
    
    my $TEST_NAME_1a = 'Felis';
    my $TEST_NAME_1b = 'Canis';
    my $TEST_NAME_1c = 'Canis dirus';
    my $TEST_NAME_1d = 'Felis catus';
    
    my $TEST_TXN_1a = 'txn:41055';
    my $TEST_TXN_1b = 'txn:41198';
    my $TEST_ID_1a = '41055';
    my $TEST_ID_1b = '41198';
    
    # First check 'id'.
    
    my %found1 = $T->fetch_record_values("/taxa/list.json?id=$TEST_TXN_1a,$TEST_TXN_1b", 'nam', 'list by id');
    
    if ( $found1{NO_RECORDS} )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    cmp_ok(keys %found1, '==', 2, "list by id found 2 records");
    ok($found1{$TEST_NAME_1a} && $found1{$TEST_NAME_1b}, "list by id found both records");
    
    # Then check 'taxon_id'.
    
    my %found2 = $T->fetch_record_values("/taxa/list.json?taxon_id=$TEST_TXN_1a,$TEST_TXN_1b", 'nam', 
					 'list by id');
    
    cmp_ok(keys %found2, '==', 2, "list by id found 2 records");
    ok($found2{$TEST_NAME_1a} && $found2{$TEST_NAME_1b}, "list by id found both records");
    
    # Make sure we get the right ids back, and also that we can handle
    # whitespace around commas.
    
    my %found3 = $T->fetch_record_values("/taxa/list.json?taxon_id=$TEST_TXN_1a , $TEST_TXN_1b", 'oid', 
					 'list by id with whitespace');
    
    cmp_ok(keys %found3, '==', 2, "list with whitespace found 2 records");
    ok($found3{$TEST_TXN_1a} && $found3{$TEST_TXN_1b}, "list by id found both records");
    
    # Test numeric identifiers
    
    my %found4 = $T->fetch_record_values("/taxa/list.json?id=$TEST_ID_1a , $TEST_ID_1b", 'oid', 
					 'list by numeric id');
    
    cmp_ok(keys %found4, '==', 2, "list by numeric id found 2 records");
    ok($found4{$TEST_TXN_1a} && $found4{$TEST_TXN_1b}, "list by numeric id found both records");
    
    # Test long-form identifiers
    
    my %found5 = $T->fetch_record_values("/taxa/list.csv?id=paleobiodb.org:$TEST_TXN_1a , " . 
					 "paleobiodb.org:$TEST_TXN_1b", 'orig_no', 'list with paleobiodb.org csv');
    
    cmp_ok(keys %found5, '==', 2, "list with paleobiodb.org found 2 records");
    ok($found5{$TEST_ID_1a} && $found5{$TEST_ID_1b}, "list with paleobiodb.org found both records csv");
};


# Now check the base_id and exclude_id parameters.

subtest 'base_id and exclude_id' => sub {
    
    my $TEST_NAME_1a = 'Felis';
    my $TEST_NAME_1b = 'Canis';
    my $TEST_NAME_1c = 'Canis dirus';
    my $TEST_NAME_1d = 'Felis catus';
    
    my $TEST_TXN_1a = 'txn:41055';
    my $TEST_TXN_1b = 'txn:41198';
    my $TEST_ID_1a = '41055';
    my $TEST_ID_1b = '41198';
    
    my $TEST_NAME_3 = 'Felidae';
    my $TEST_NAME_4 = 'Canidae';
    
    my $TEST_NAME_3a = 'Felinae';
    my $TEST_NAME_3b = 'Machairodontinae';
    my $TEST_NAME_3c = 'Felis catus';
    my $TEST_NAME_3d = 'Dinofelis palaeoonca';
    
    my $TEST_TXN_3 = 'txn:41045';
    my $TEST_TXN_3b = 'txn:65494';
    my $TEST_TXN_3c = 'txn:104159';
    my $TEST_TXN_3d = 'txn:49736';
    
    # Make sure that base_id works.
    
    my (@r) = $T->fetch_records("/taxa/list.json?base_id=$TEST_TXN_1a,$TEST_TXN_1b",
				"base_id multiple");
    
    unless ( @r )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my (%found1, %base1);
    
    foreach my $r ( @r )
    {
	next unless defined $r->{nam};
	$found1{$r->{nam}} = 1;
	$base1{$r->{nam}} = 1 if defined $r->{flg} && $r->{flg} =~ /B/;
    }
    
    ok( $found1{$TEST_NAME_1a}, "base_id multiple found first base name" );
    ok( $found1{$TEST_NAME_1b}, "base_id multiple found second base name" );
    ok( $found1{$TEST_NAME_1c}, "base_id multiple found first species name" );
    ok( $found1{$TEST_NAME_1d}, "base_id multiple found second species name" );
    
    # Make sure that the proper records have the 'B' flag.
    
    cmp_ok( keys %base1, '==', 2, "base_id multiple found 2 'B' flags" );
    ok( $base1{$TEST_NAME_1a} && $base1{$TEST_NAME_1b} , "base_id multiple found proper 'B' flags" );
    
    # Now check the 'exclude_id' parameter.
    
    my (%all) = $T->fetch_record_values("/taxa/list.json?base_id=$TEST_TXN_3", 'nam', "no exclusion");
    
    my (%allbut) = $T->fetch_record_values("/taxa/list.json?base_id=$TEST_TXN_3&exclude_id=" .
					   "$TEST_TXN_3b , $TEST_TXN_3c", 'nam', "exclude by id");
    
    ok ( keys %allbut > 1, "exclude by id found records" );
    
    my (%excl) = $T->fetch_record_values("/taxa/list.json?base_id=$TEST_TXN_3b , $TEST_TXN_3c", 'nam',
					 "excluded subtrees");
    
    if ( keys %allbut > 1 )
    {
	ok( ! $allbut{$TEST_NAME_3b}, "excluded '$TEST_NAME_3b'" );
	ok( ! $allbut{$TEST_NAME_3c}, "excluded '$TEST_NAME_3c'" );
	ok( ! $allbut{$TEST_NAME_3d}, "excluded '$TEST_NAME_3d'" );
    }
    
    if ( keys %all > 1 )
    {
	ok( $all{$TEST_NAME_3b}, "all found '$TEST_NAME_3b'" );
	ok( $all{$TEST_NAME_3c}, "all found '$TEST_NAME_3c'" );
	ok( $all{$TEST_NAME_3d}, "all found '$TEST_NAME_3d'" );
    }
    
    if ( keys %excl > 1 )
    {
	ok( $excl{$TEST_NAME_3b}, "excl found '$TEST_NAME_3b'" );
	ok( $excl{$TEST_NAME_3c}, "excl found '$TEST_NAME_3c'" );
	ok( $excl{$TEST_NAME_3d}, "excl found '$TEST_NAME_3d'" );
    }
    
    cmp_ok( keys(%all), '==', keys(%allbut) + keys(%excl), 
	    'record counts match between base_id with exclusion and exclusion' );
    
    # Now test base_name with exclude_id, to make sure that those two
    # parameters also work with each other.
    
    my (%allbut2) = $T->fetch_record_values("/taxa/list.json?base_name=$TEST_NAME_3&exclude_id=" .
					    "$TEST_TXN_3b,$TEST_TXN_3c", 'nam', "base_name + exclude_id");
    
    if ( keys %allbut2 > 1 )
    {
	ok( ! $allbut2{$TEST_NAME_3b}, "excluded '$TEST_NAME_3b'" );
	ok( ! $allbut2{$TEST_NAME_3c}, "excluded '$TEST_NAME_3c'" );
	ok( ! $allbut2{$TEST_NAME_3d}, "excluded '$TEST_NAME_3d'" );
    }
    
    cmp_ok( keys(%all), '==', keys(%allbut2) + keys(%excl), 
	    'record counts match between base_name with exclusion and exclusion' );    
};


# Check for the proper response to bad ids.

subtest 'bad ids' => sub {
    
    # First check some bad identifiers.  Also check that 'strict=no' does not
    # cause the 'strict' behavior.
    
    my $BAD_1a = 'should be id';
    my $BAD_1b = 'int:123';
    my $BAD_1c = 'txn::69296';
    my $BAD_1d = 'txn:9999999';
    
    my $m1a = $T->fetch_nocheck("/taxa/list.json?id=$BAD_1a", "bad id a");
    my $m1b = $T->fetch_nocheck("/taxa/list.json?id=$BAD_1b", "bad id b");
    my $m1c = $T->fetch_nocheck("/taxa/list.json?id=$BAD_1c&strict=no", "bad id c");
    my $m1d = $T->fetch_nocheck("/taxa/list.json?id=$BAD_1d&strict=no", "bad id d");
    
    $T->ok_response_code( $m1a, '200', "bad id a got 200 response" );
    $T->ok_response_code( $m1b, '200', "bad id b got 200 response" );
    $T->ok_response_code( $m1d, '200', "bad id c got 200 response" );
    $T->ok_response_code( $m1d, '200', "bad id d got 200 response" );
    
    $T->ok_no_records( $m1a, "bad id a found no records" );
    $T->ok_no_records( $m1b, "bad id b found no records" );
    $T->ok_no_records( $m1c, "bad id c found no records" );
    $T->ok_no_records( $m1d, "bad id d found no records" );
    
    $T->ok_warning_like( $m1a, qr{must be.*a valid identifier}i, "bad id a got proper warning" );
    $T->ok_warning_like( $m1b, qr{type 'int' is not allowed}i, "bad id b got proper warning" );
    $T->ok_warning_like( $m1c, qr{must be.*a valid identifier}i, "bad id c got proper warning" );
    $T->ok_warning_like( $m1d, qr{unknown taxon}i, "bad id d got proper warning" );
    
    # Then try some mixed, good and bad ids.
    
    my $GOOD_1 = 'txn:41189';
    
    my $m2a = $T->fetch_nocheck("/taxa/list.json?id=$BAD_1a,$GOOD_1", "good and bad id a");
    my $m2d = $T->fetch_nocheck("/taxa/list.json?id=$BAD_1d,$GOOD_1", "good and bad id d");
    
    $T->ok_response_code( $m2a, '200', "good and bad id a got 200 response" );
    $T->ok_response_code( $m2d, '200', "good and bad id d got 200 response" );
    
    my (@r2a) = $T->extract_records( $m2a, "good and bad id a" );
    my (@r2d) = $T->extract_records( $m2a, "good and bad id d" );
    
    $T->ok_warning_like( $m2a, qr{must be.*a valid identifier}i, "good and bad id a got proper warning" );
    $T->ok_warning_like( $m2d, qr{unknown taxon}i, "good and bad id d got proper warning" );
    
    cmp_ok( @r2a, '==', 1, "good and bad id a got 1 record" );
    cmp_ok( @r2d, '==', 1, "good and bad id d got 1 record" );
    
    is( $r2a[0]{oid}, $GOOD_1, "good and bad id a found proper record" );
    is( $r2d[0]{oid}, $GOOD_1, "good and bad id d found proper record" );
    
    # Then try the same, with 'strict'.
    
    my $m3a = $T->fetch_nocheck("/taxa/list.json?id=$BAD_1a,$GOOD_1&strict", "good and bad id a strict");
    my $m3d = $T->fetch_nocheck("/taxa/list.json?id=$BAD_1d,$GOOD_1&strict", "good and bad id d strict");
    
    $T->ok_response_code( $m3a, '400', "good and bad id a strict got 400 response" );
    $T->ok_response_code( $m3d, '404', "good and bad id d strict got 404 response" );
    
    $T->ok_error_like( $m3a, qr{must be.*a valid identifier}i, "good and bad id a strict got proper warning" );
    $T->ok_error_like( $m3d, qr{unknown taxon}i, "good and bad id d strict got proper warning" );
    
    # Try base_id with a bad numeric value
    
    my $BAD_4 = '9999999999';
    
    my ($m4) = $T->fetch_nocheck("/taxa/list.json?base_id=$BAD_4", "very large numeric id");
    
    $T->ok_response_code( $m4, '200', "very large numeric id got 200 response" );
    $T->ok_warning_like( $m4, qr{unknown taxon}i, "very large numeric id got proper warning" );
    
    my (@r4) = $T->extract_records($m4, "very large numeric id", { no_records_ok => 1 });
    
    cmp_ok( @r4, '==', 0, "very large numeric id found no records");
};


# Check the 'parent' and 'immparent' blocks, and also check that using
# 'rel=parent' returns the same info.

subtest 'parent and immparent' => sub {
    
    my $TEST_BASE_1 = 'Carnivora';
    
    my (@r1) = $T->fetch_records("/taxa/list.json?base_name=$TEST_BASE_1&show=immparent", 
				 "list immparent");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my ($taxon_id, $taxon_name, $parent_id, $parent_name, $immpar_id, $immpar_name);
    
    foreach my $r ( @r1 )
    {
	if ( defined $r->{par} && defined $r->{ipn} &&
	    $r->{par} ne $r->{ipn} )
	{
	    $taxon_id = $r->{oid};
	    $parent_id = $r->{par};
	    $immpar_id = $r->{ipn};
	    $taxon_name = $r->{nam};
	    $parent_name = $r->{prl};
	    $immpar_name = $r->{ipl};
	    last;
	}
    }
    
    ok( $taxon_id, "found at least one taxon with parent <> immparent" );
    
    diag( "taxon = '$taxon_name'  parent = '$parent_name'  immpar = '$immpar_name'" );    
    
    if ( $taxon_id )
    {
	ok( $parent_id, "list immparent found parent id" );
	ok( $immpar_id, "list immparent found immparent id" );
	ok( $taxon_name, "list immparent found taxon name" );
	ok( $parent_name, "list immparent found parent name" );
	ok( $immpar_name, "list immparent found immparent name" );
	
	my ($r2) = $T->fetch_records("/taxa/list.json?id=$taxon_id&show=parent");
	
	is( $r2->{par}, $parent_id, "list show parent found proper parent" );
	is( $r2->{prl}, $parent_name, "list show parent found proper label" );
	ok( ! defined $r2->{ipn} && ! defined $r2->{ipl}, "list show parent did not return immpar" );
	
	my ($par) = $T->fetch_records("/taxa/list.json?id=$taxon_id&rel=parent");
	my ($ipr) = $T->fetch_records("/taxa/list.json?id=$taxon_id&rel=immpar");
	
	is( $par->{oid}, $parent_id, "rel parent found proper id" );
	is( $par->{nam}, $parent_name, "rel parent found proper name" );
	
	is( $ipr->{oid}, $immpar_id, "rel immparent found proper id" );
	is( $ipr->{nam}, $immpar_name, "rel immparent found proper name" );
    }
};


# Test the parameter 'immediate'.  With the 'children' and 'all_children'
# relationships, this selects just the immediate children and not the children
# of synonyms.  With the 'parents' and 'all_parents' relationships, it selects
# the immediate parent rather than the senior synonym of the parent.

# This subtest also checks the relationships 'children', 'all_children' and
# 'all_parents'.

subtest 'immediate' => sub {
    
    # Test that 'immediate' works properly with both the senior and junior
    # synonym. We include the 'immparent' block so that we can check that the
    # parent links match up properly.
    
    my $NAME_1j = 'Capitosauridae';
    my $NAME_1s = 'Mastodonsauridae';
    
    my $ID_1j = 'txn:37093';
    my $ID_1s = 'txn:37107';
    
    # Try the senior synonym first, without 'immediate'.  This will give us
    # the full set of children and all_children.
    
    my @r1sc = $T->fetch_records("/taxa/list.json?id=$ID_1s&rel=children&show=immparent", 
				 "senior children");
    my @r1st = $T->fetch_records("/taxa/list.json?id=$ID_1s&rel=all_children&show=immparent", 
				 "senior subtree");
    
    # Also fetch the list of parents, for use below.
    
    my @r1sp = $T->fetch_records("/taxa/list.json?id=$ID_1s&rel=all_parents", "all parents");
    my %r1sp = $T->extract_values( \@r1sp, 'nam' );
    
    # Then the junior synonym without 'immediate'.  This should give us the
    # very same list.
    
    my @r1jc = $T->fetch_records("/taxa/list.json?id=$ID_1j&rel=children&show=immparent",
				 "junior children");
    
    my @r1jt = $T->fetch_records("/taxa/list.json?id=$ID_1j&rel=all_children&show=immparent",
				 "junior subtree");
    
    # Then grab all synonyms of the senior, and fetch all of those with
    # 'immediate'.  This should give us the very same list.
    
    my %synonyms = $T->fetch_record_values("/taxa/list.json?id=$ID_1s&rel=synonyms", 'oid', "list synonyms");
    my $syn_list = join(q{,}, keys %synonyms);
    
    my @r1cc = $T->fetch_records("/taxa/list.json?id=$syn_list&rel=children&immediate&show=immparent",
				 "both children");
    my @r1ct = $T->fetch_records("/taxa/list.json?id=$syn_list&rel=all_children&immediate&show=immparent",
				 "both subtree");
    
    # Check that these trees are equal
    
    is_deeply( \@r1jc, \@r1sc, "junior children equal to senior" );
    is_deeply( \@r1jt, \@r1st, "junior subtree equal to senior" );
    
    is_deeply( \@r1cc, \@r1sc, "combined children equal to senior" );
    
    # We need to strip the 'flg' values from any junior synonym records before this
    # last test, or it will fail.
    
    foreach my $r ( @r1ct )
    {
	delete $r->{flg} unless $r->{oid} eq $ID_1s;
    }
    
    is_deeply( \@r1ct, \@r1st, "combined subtree equal to senior" );
    
    # Then try both synonyms with 'immediate'. We will then be able to check
    # that the two trees are exclusive add up to the ones fetched above.
    
    # Senior with 'immediate'
    
    my @r1Sc = $T->fetch_records("/taxa/list.json?id=$ID_1s&rel=children&immediate&show=immparent",
				 "senior immediate children");
    my @r1St = $T->fetch_records("/taxa/list.json?id=$ID_1s&rel=all_children&immediate&show=immparent",
				 "senior immediate subtree");
    
    # Junior with 'immediate'
    
    my @r1Jc = $T->fetch_records("/taxa/list.json?id=$ID_1j&rel=children&immediate&show=immparent", 
				 "junior immediate children");
    my @r1Jt = $T->fetch_records("/taxa/list.json?id=$ID_1j&rel=all_children&immediate&show=immparent",
				 "junior immediate subtree");
    
    # Now check to make sure that these last four record sets are exclusive
    # and add up to the first two.
    
    cmp_ok( @r1Jc + @r1Sc, '==', @r1sc, "child record counts add up properly" );
    cmp_ok( @r1Jt + @r1St, '==', @r1st, "subtree record counts add up properly" );
    
    # Then check to make sure that the immediate and senior parent taxa match
    # up properly for the various lists of children.
    
    my (%r1scx, %r1Scx, %r1Jcx);
    
    foreach my $r ( @r1sc )
    {
	$r1scx{good_immpar} = 1 if $r->{ipl} && $r->{ipl} eq $NAME_1j;
	$r1scx{bad_immpar} = $r->{ipl} if $r->{ipl} && $r->{ipl} ne $NAME_1j;
	$r1scx{bad_senpar} = ($r->{prl} || '') if ! $r->{prl} || $r->{prl} ne $NAME_1s;
    }
    
    ok( $r1scx{good_immpar}, "senior children found at least one child of a junior synonym" );
    ok( !defined $r1scx{bad_immpar}, "senior children bad immpar name" ) ||
	diag("    Got: '$r1scx{bad_immpar}'");
    ok( !defined $r1scx{bad_senpar}, "senior children bad senpar name" ) ||
	diag("    Got: '$r1scx{bad_senpar}'");
    
    foreach my $r ( @r1Sc )
    {
	$r1Scx{bad_immpar} = $r->{ipl} if $r->{ipl};
	$r1Scx{bad_senpar} = ($r->{prl} || '') if ! $r->{prl} || $r->{prl} ne $NAME_1s;
    }
    
    ok( !defined $r1Scx{bad_immpar}, "senior immediate children bad immpar name" ) ||
	diag("    Got: '$r1Scx{bad_immpar}'");
    ok( !defined $r1Scx{bad_senpar}, "senior immediate children bad senpar name" ) ||
	diag("    Got: '$r1Scx{bad_senpar}'");
    
    foreach my $r ( @r1Jc )
    {
	$r1Jcx{bad_immpar} = ($r->{ipl} || '') if ! $r->{ipl} || $r->{ipl} ne $NAME_1j;
	$r1Jcx{bad_senpar} = ($r->{prl} || '') if ! $r->{prl} || $r->{prl} ne $NAME_1s;
    }
    
    ok( !defined $r1Jcx{bad_immpar}, "senior immediate children bad immpar name" ) ||
	diag("    Got: '$r1Jcx{bad_immpar}'");
    ok( !defined $r1Jcx{bad_senpar}, "senior immediate children bad senpar name" ) ||
	diag("    Got: '$r1Jcx{bad_senpar}'");
    
    # Then look for a species in each subtree and make sure that the immediate
    # and senior parent lists match properly.
    
    my (%r1Stx, %r1Jtx);
    
    foreach my $r ( @r1St )
    {
	next unless $r->{rnk} eq '3';
	
	my $id = $r->{oid};
	my %immpar = $T->fetch_record_values("/taxa/list.json?id=$id&rel=all_parents&immediate",
					      'nam', "senior subtree species parents");
	
	$r1Stx{good_parent} = 1 if $immpar{$NAME_1s};
	$r1Stx{bad_parent} = $r->{nam} unless $immpar{$NAME_1s};
	
	my %senpar = $T->fetch_record_values("/taxa/list.json?id=$id&rel=all_parents", 'nam',
					     "senior subtree species parents senior");
	$T->ok_is_subset( \%r1sp, \%senpar, "senior subtree found proper parents" ) || last;
    }
    
    ok( $r1Stx{good_parent}, "senior subtree species found good parent" );
    ok( !defined $r1Stx{bad_parent}, "senior subtree species found proper parents" ) ||
	diag("    Name was: '$r1Stx{bad_parent}'");
    
    foreach my $r ( @r1Jt )
    {
	next unless $r->{rnk} eq '3';
	
	my $id = $r->{oid};
	my %parents = $T->fetch_record_values("/taxa/list.json?id=$id&rel=all_parents&immediate",
					      'nam', "junior subtree species");
	
	$r1Jtx{good_parent} = 1 if $parents{$NAME_1j};
	$r1Jtx{bad_parent} = $r->{nam} unless $parents{$NAME_1j};
    }
    
    ok( $r1Jtx{good_parent}, "junior subtree species found good parent" );
    ok( !defined $r1Jtx{bad_parent}, "junior subtree species found proper parents" ) ||
	diag("    Name was: '$r1Jtx{bad_parent}'");
    
    # Now take the junior and senior immediate children, and test that
    # 'immpar' and 'senpar' work properly.
    
    my $junior_list = join q{,}, map { $_->{oid} } @r1Jc;
    my $senior_list = join q{,}, map { $_->{oid} } @r1Sc;
    
    my @r2ji = $T->fetch_records("/taxa/list.json?id=$junior_list&rel=immpar", "junior taxa immpar");
    my @r2si = $T->fetch_records("/taxa/list.json?id=$senior_list&rel=immpar", "senior taxa immpar");
    my @r2js = $T->fetch_records("/taxa/list.json?id=$junior_list&rel=senpar", "junior taxa senpar");
    my @r2ss = $T->fetch_records("/taxa/list.json?id=$senior_list&rel=senpar", "senior taxa senpar");
    
    cmp_ok( @r2ji, '==', 1, "junior taxa immpar found one record" );
    cmp_ok( @r2si, '==', 1, "senior taxa immpar found one record" );
    cmp_ok( @r2js, '==', 1, "junior taxa senpar found one record" );
    cmp_ok( @r2ss, '==', 1, "senior taxa senpar found one record" );
    
    is( $r2ji[0]{oid}, $ID_1j, "junior taxa immpar found proper parent" );
    is( $r2si[0]{oid}, $ID_1s, "senior taxa immpar found proper parent" );
    is( $r2js[0]{oid}, $ID_1s, "junior taxa senpar found proper parent" );
    is( $r2ss[0]{oid}, $ID_1s, "senior taxa senpar found proper parent" );
    
    # Then test that 'parent' = 'senpar' and 'parent&immediate' = 'immpar'
    
    my @r2pi = $T->fetch_records("/taxa/list.json?id=$junior_list&rel=parent&immediate", "parent+immediate");
    my @r2px = $T->fetch_records("/taxa/list.json?id=$junior_list&rel=parent", "parent without immediate");
    
    is_deeply( $r2ji[0], $r2pi[0], "parent+immediate found proper record" );
    is_deeply( $r2js[0], $r2px[0], "parent without immediate found proper record" );
    
    # Test that 'immediate' works properly with one senior and one junior
    # synonym, where there are more junior synonyms.
    
    my $NAME_3a = 'HESPEROSAURUS';
    my $NAME_3b = 'STEGOSAURUS';
    
    my @r3a = $T->fetch_records("/taxa/list.csv?base_name=$NAME_3a&immediate", "name 3a immediate");
    my @r3b = $T->fetch_records("/taxa/list.csv?base_name=$NAME_3b&immediate", "name 3b immediate");
    my @r3c = $T->fetch_records("/taxa/list.csv?base_name=$NAME_3a,$NAME_3b&immediate", 
				"name 3a,b immediate");
    
    cmp_ok( @r3a, '>', 1, "name 3a immediate found more than one record" );
    cmp_ok( @r3b, '>', 1, "name 3b immediate found more than one record" );
    cmp_ok( @r3a + @r3b, '==', @r3c, "name 3a, 3b counts add up properly" );
    
    my %rank3a = $T->extract_values( \@r3a, 'taxon_rank' );
    my %rank3b = $T->extract_values( \@r3b, 'taxon_rank' );
    
    ok( $rank3a{species}, "name 3a immediate found at least one species" );
    ok( $rank3b{species}, "name 3b immediate found at least one species" );
};


# Now we need to explicitly test all of the various taxon relationships.

subtest 'exact, current, variants' => sub {
    
    my $NAME_1a = 'Dascillus';
    my $NAME_1b = 'Dascyllus';
    
    my @r1a = $T->fetch_records("/taxa/list.json?name=$NAME_1a,$NAME_1b", "two names default rel");
    my @r1b = $T->fetch_records("/taxa/list.json?name=$NAME_1a,$NAME_1b&rel=exact", "two names exact");
    my @r1c = $T->fetch_records("/taxa/list.json?name=$NAME_1a,$NAME_1b&rel=current", "two names current");
    
    unless ( @r1a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    cmp_ok( @r1a, '==', 2, "two names default rel found two records" );
    cmp_ok( @r1b, '==', 2, "two names exact found two records" );
    cmp_ok( @r1c, '==', 1, "two names current found one record" );
    
    my %names1a = $T->extract_values( \@r1a, 'nam' );
    my %names1b = $T->extract_values( \@r1b, 'nam' );
    my %names1c = $T->extract_values( \@r1c, 'nam' );
    
    ok( $names1a{$NAME_1a} && $names1a{$NAME_1b}, "two names default rel found both names" );
    ok( $names1b{$NAME_1a} && $names1b{$NAME_1b}, "two names exact found both names" );
    ok( $names1c{$NAME_1a}, "two names exact found current name" );
    
    my $NAME_2 = 'Ornithischia';
    my $OID_2 = 'txn:38712';
    
    my @r2a = $T->fetch_records("/taxa/list.json?match_name=$NAME_2", "match default rel");
    my @r2b = $T->fetch_records("/taxa/list.json?match_name=$NAME_2&rel=exact", "match exact");
    my @r2c = $T->fetch_records("/taxa/list.json?match_name=$NAME_2&rel=current", "match current");
    my @r2d = $T->fetch_records("/taxa/list.json?match_name=$NAME_2&rel=synonyms", "match synonyms");
    
    my %vid2a = $T->extract_values( \@r2a, 'vid' );
    my %vid2b = $T->extract_values( \@r2b, 'vid' );
    my %acc2b = $T->extract_values( \@r2b, 'oid' );
    my %oid2c = $T->extract_values( \@r2c, 'oid' );
    
    is_deeply( \%vid2a, \%vid2b, "match default rel and match exact return same records" );
    is_deeply( \%acc2b, \%oid2c, "match current returns accepted records from match exact" );
    
    cmp_ok( @r2a, '>=', 3, "match default rel found at least 3 records" );
    
    foreach my $r ( @r2d )
    {
	my $acc = $r->{acc} || $r->{oid};
	unless ( $acc2b{$acc} )
	{
	    fail("match synonyms found records with proper acc value");
	    diag("   Value '$acc' was not found");
	    last;
	}
    }
    
    my $NAME_3 = 'Ornithosuchia';
    
    my @r3a = $T->fetch_records("/taxa/list.json?name=$NAME_2&rel=variants", "rel variants");
    my %names3a = $T->extract_values( \@r3a, 'nam' );
    
    cmp_ok( @r3a, '>=', 3, "rel variants found at least 3 names" );
    ok( $names3a{$NAME_3}, "rel variants found test name" );
    
    foreach my $r ( @r3a )
    {
	unless ( ($r->{acc} || $r->{oid}) eq $OID_2 )
	{
	    fail("rel variants found only variants of the base name");
	    diag("   Found '$r->{nam}' ($r->{oid})");
	    last;
	}
    }
    
    # Now test identifiers 'var:nnnn' vs. 'txn:nnnn';
    
    if ( @r1a == 2 )
    {
	my ($TXN_1a, $TXN_1b, $VAR_1a, $VAR_1b, $NUM_1a, $NUM_1b);
	
	foreach my $r ( @r1a )
	{
	    if ( $r->{nam} eq $NAME_1a )
	    {
		$TXN_1a = $r->{oid};
		$VAR_1a = $r->{vid};
		$NUM_1a = $VAR_1a; $NUM_1a =~ s/^\w+://;
		ok( $NUM_1a && $NUM_1a =~ /^\d+$/, "found numeric vid for '$NAME_1a'" );
	    }
	    
	    else
	    {
		$TXN_1b = $r->{oid};
		$VAR_1b = $r->{vid};
		$NUM_1b = $VAR_1b; $NUM_1b =~ s/^\w+://;
		ok( $NUM_1b && $NUM_1b =~ /^\d+$/, "found numeric vid for '$NAME_1b'" );
	    }
	}
	
	is( $TXN_1a, $TXN_1b, "both records have same oid" );
	
	my ($rtxna) = $T->fetch_records("/taxa/list.json?id=$TXN_1a", "fetch '$TXN_1a'");
	my ($rvara) = $T->fetch_records("/taxa/list.json?id=$VAR_1a", "fetch '$VAR_1a'");
	my ($rvarb) = $T->fetch_records("/taxa/list.json?id=$VAR_1b", "fetch '$VAR_1b'");
	my ($rnuma) = $T->fetch_records("/taxa/list.json?id=$NUM_1a", "fetch '$NUM_1a'");
	my ($rnumb) = $T->fetch_records("/taxa/list.json?id=$NUM_1b", "fetch '$NUM_1b'");
	
	is( $rtxna->{nam}, $NAME_1a, "fetch '$TXN_1a' found proper name" );
	is( $rvara->{nam}, $NAME_1a, "fetch '$VAR_1a' found proper name" );
	is( $rvarb->{nam}, $NAME_1b, "fetch '$VAR_1b' found proper name" );
	is( $rnuma->{nam}, $NAME_1a, "fetch '$NUM_1a' found proper name" );
	is( $rnumb->{nam}, $NAME_1b, "fetch '$NUM_1b' found proper name" );
    }
};


subtest 'synonyms, senior, accepted' => sub {

    my $NAME_1a = 'Stegosaurus';
    my $NAME_1b = 'Diracodon';
    
    my @r1a = $T->fetch_records("/taxa/list.json?name=$NAME_1a&rel=synonyms", "synonyms a");
    my @r1b = $T->fetch_records("/taxa/list.json?name=$NAME_1b&rel=synonyms", "synonyms b");
    
    unless ( @r1a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my %names1a = $T->extract_values( \@r1a, 'nam' );
    my %acn1a = $T->extract_values( \@r1a, 'acn' );
    
    my %names1b = $T->extract_values( \@r1b, 'nam' );
    my %acn1b = $T->extract_values( \@r1b, 'acn' );
    
    cmp_ok( keys %names1a, '>=', 3, "synonyms a found at least 3 names" );
    is_deeply( \%names1a, \%names1b, "synonyms a and b found same names" );
    
    cmp_ok( keys %acn1a, '==', 1, "synonyms a found just one accepted name" );
    cmp_ok( keys %acn1b, '==', 1, "synonyms b found just one accepted name" );
    
    ok( $acn1a{$NAME_1a} && $acn1b{$NAME_1a}, "synonyms a and b both found senior synonym" );
    
    my @r2a = $T->fetch_records("/taxa/list.json?name=$NAME_1a&rel=senior", "senior a");
    my @r2b = $T->fetch_records("/taxa/list.json?name=$NAME_1b&rel=senior", "senior b");
    
    cmp_ok( @r2a, '==', 1, "senior a found one record" );
    cmp_ok( @r2b, '==', 1, "senior b found one record" );
    
    is( $r2a[0]{nam}, $NAME_1a, "senior a found proper record" );
    is( $r2b[0]{nam}, $NAME_1a, "senior b found proper record" );
    
    my @r3a = $T->fetch_records("/taxa/list.json?name=$NAME_1a&rel=accepted", "accepted a");
    my @r3b = $T->fetch_records("/taxa/list.json?name=$NAME_1b&rel=accepted", "accepted b");
    
    cmp_ok( @r3a, '==', 1, "senior a found one record" );
    cmp_ok( @r3b, '==', 1, "senior b found one record" );
    
    is( $r3a[0]{nam}, $NAME_1a, "senior a found proper record" );
    is( $r3b[0]{nam}, $NAME_1a, "senior b found proper record" );
    
    # Look for invalid names, and check that the 'accepted' for each one is correct.
    
    my @r4 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1a&taxon_status=invalid", 
			       "list invalid taxa");
    
    my ($invalid_name, $invalid_id);
    
    foreach my $r ( @r4 )
    {
	if ( $r->{tdf} =~ /nomen/ )
	{
	    $invalid_name = $r->{nam};
	    $invalid_id = $r->{oid};
	}
    }
    
    ok( $invalid_name, "list invalid taxa found at least one 'nomen'" );
    
    my @r5a = $T->fetch_records("/taxa/list.json?name=$invalid_name&rel=accepted",
				"invalid accepted" );
    my @r5b = $T->fetch_records("/taxa/list.json?name=$invalid_name&rel=senior",
				"invalid senior" );
    
    is( $r5a[0]{nam}, $NAME_1a, "invalid accepted got proper name" );
    is( $r5b[0]{nam}, $invalid_name, "invalid senior got proper name" );
};


subtest 'children and parents' => sub {
    
    my $NAME_1a = 'Felis';
    my $NAME_1b = 'Canis';
    
    my %base_name = ( $NAME_1a => 1, $NAME_1b => 1 );
    
    my @r1 = $T->fetch_records("/taxa/list.json?name=$NAME_1a,$NAME_1b&show=parent", "two names");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my %prl_value = $T->extract_values( \@r1, 'prl' );
    
    cmp_ok( keys %prl_value, '==', 2, "two names found two parents" );
    
    my @r2 = $T->fetch_records("/taxa/list.json?name=$NAME_1a,$NAME_1b&rel=parent", 
			       "parents of two names");
    
    my %nam_value = $T->extract_values( \@r2, 'nam' );
    
    is_deeply( \%prl_value, \%nam_value, "parent names match up" );
    
    my @r3 = $T->fetch_records("/taxa/list.json?name=$NAME_1a,$NAME_1b&rel=children&show=parent", 
			       "children of two names");
    
    my %child_prl_value = $T->extract_values( \@r3, 'prl' );
    
    is_deeply( \%child_prl_value, \%base_name, "child parents match up" );
};


subtest 'classext and all_parents' => sub {

    my $NAME_1 = 'Canis familiaris';
    
    my ($r1) = $T->fetch_records("/taxa/list.json?name=$NAME_1&show=classext", 
				 "base record with classext");
    
    unless ( $r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my @r2 = $T->fetch_records("/taxa/list.json?name=$NAME_1&rel=all_parents",
			       "base record all parents");
    
    is( $r2[0]{nam}, 'Life', "all parents headed by 'Life'" );
    
    my (%found_parent, @bad, @bad_parent);
    
    foreach my $r ( @r2 )
    {
	fail("parent name '$r->{nam}' ( $r->{oid} )") 
	    if ! $r->{oid} || ! $r->{nam};
	$found_parent{$r->{oid}} = $r->{nam};
    }
    
    foreach my $f ( qw(ph cl od fm gn) )
    {
	my $oid = $r1->{$f . 'n'};
	my $nam = $r1->{$f . 'l'};
	
	push @bad, "missing field '${f}n'" unless $oid;
	push @bad, "missing field '${f}l'" unless $nam;
	
	push @bad_parent, "missing parent '$oid'" 
	    unless ! $oid || $found_parent{$oid};
	push @bad_parent, "found parent name '$found_parent{$oid}' instead of '$nam'" 
	    unless ! $nam || $found_parent{$oid} eq $nam;
    }
    
    unless ( ok( @bad == 0, "classext ok" ) )
    {
	diag("    $_") foreach @bad;
    }
    
    unless ( ok( @bad_parent == 0, "all_parents ok" ) )
    {
	diag("    $_") foreach @bad_parent;
    }
};


subtest 'common' => sub {

    my $NAME_1a = 'Felis';
    my $NAME_1b = 'Canis';
    my $NAME_2 = 'Carnivora';
    
    my @r1 = $T->fetch_records("/taxa/list.json?name=$NAME_1a,$NAME_1b&rel=common",
			       "rel common");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my @r2 = $T->fetch_records("/taxa/list.json?name=$NAME_1a,$NAME_1b&rel=all_parents",
			       "rel all_parents");
    
    cmp_ok( @r1, '==', 1, "rel common found one record" );
    
    my (%children, %name, @has_multiple);
    
    foreach my $r ( @r2 )
    {
	my $pid = $r->{par};
	$children{$pid}++ if $pid;
	$name{$r->{oid}} = $r->{nam};
    }
    
    foreach my $p ( keys %children )
    {
	push @has_multiple, $p if $children{$p} > 1;
    }
    
    if ( cmp_ok( @has_multiple, '==', 1, "only one parent with two children" ) )
    {
	diag("Common ancestor is '$name{$has_multiple[0]}'");
	is( $r1[0]{nam}, $name{$has_multiple[0]}, "rel common found proper record" );
    }
};


# We already tested the output blocks for the operation 'taxa/single', so now we test that
# 'taxa/list' produces the same records for the same names. Also test that a bad value for 'show'
# gets a proper warning.

subtest 'output blocks' => sub {
    
    my $NAME_1a = 'Felis';
    my $NAME_1b = 'Canis';
    
    # Start by comparing the output of taxa/single.json and taxa/list.json
    
    my ($single1a) = $T->fetch_records("/taxa/single.json?name=$NAME_1a&show=full", "single json a");
    my ($single1b) = $T->fetch_records("/taxa/single.json?name=$NAME_1b&show=full", "single json b");
    
    my (@r1) = $T->fetch_records("/taxa/list.json?name=$NAME_1a,$NAME_1b&show=full", "list json a+b");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    cmp_ok( @r1, '==', 2, "list json a+b found two records" );
    
    foreach my $r ( @r1 )
    {
	if ( $r->{nam} eq $single1a->{nam} )
	{
	    is_deeply( $r, $single1a, "list json a matches single json a" );
	}
	
	elsif ( $r->{nam} eq $single1b->{nam} )
	{
	    is_deeply( $r, $single1b, "list json b matches single1 json b" );
	}
	
	else
	{
	    fail("list json a+b found unexpected record '$r->{nam}'");
	}
    }
    
    # Then do the same for taxa/single.csv vs. taxa/list.csv and taxa/list.tsv
    
    my ($single2a) = $T->fetch_records("/taxa/single.csv?name=$NAME_1a&show=full", "single csv a");
    my ($single2b) = $T->fetch_records("/taxa/single.csv?name=$NAME_1b&show=full", "single csv b");
    
    my (@r2) = $T->fetch_records("/taxa/list.csv?name=$NAME_1a,$NAME_1b&show=full", "list csv a+b");
    my (@r3) = $T->fetch_records("/taxa/list.tsv?name=$NAME_1a,$NAME_1b&show=full", "list tsv a+b");
    
    cmp_ok( @r2, '==', 2, "list csv a+b found two records" );
    cmp_ok( @r3, '==', 2, "list tsv a+b found two records" );
    
    foreach my $r ( @r2 )
    {
	if ( $r->{taxon_name} eq $single2a->{taxon_name} )
	{
	    is_deeply( $r, $single2a, "list csv a matches single csv a" );
	}
	
	elsif ( $r->{taxon_name} eq $single2b->{taxon_name} )
	{
	    is_deeply( $r, $single2b, "list csv b matches single csv b" );
	}
	
	else
	{
	    fail("list json a+b csv unexpected record '$r->{taxon_name}'");
	}
    }
    
    foreach my $r ( @r3 )
    {
	if ( $r->{taxon_name} eq $single2a->{taxon_name} )
	{
	    is_deeply( $r, $single2a, "list tsv a matches single json a" );
	}
	
	elsif ( $r->{taxon_name} eq $single2b->{taxon_name} )
	{
	    is_deeply( $r, $single2b, "list tsv b matches single1 json b" );
	}
	
	else
	{
	    fail("list json a+b tsv unexpected record '$r->{taxon_name}'");
	}
    }
    
    # Test bad value for show=
    
    my ($m4) = $T->fetch_url("/taxa/list.json?name=$NAME_1a&show=foo", "bad show=",
			 { no_diag => 1 });
    
    $T->ok_response_code( $m4, '200', "bad show= got 200 response" );
    $T->ok_warning_like( $m4, qr{bad value.*show}i, "bad show= got proper warning" );
    
    my @m4 = $T->extract_records($m4, "bad show=");
    
    cmp_ok( @m4, '==', 1, "bad show= found one record" ) &&
	is( $m4[0]{nam}, $NAME_1a, "bad show= found proper record" );
};


# Test the 'variant' parameter. The default is whichever variants were selected by the other
# parameters, so we just need to test 'variant=all'.

subtest 'variant=all' => sub {
    
    my $NAME_1 = 'DASCILLIDAE';
    
    my @r1c = $T->fetch_records("/taxa/list.json?base_name=$NAME_1", "default variants");
    my @r1v = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&variant=all", "all variants");
    
    unless ( @r1c )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    cmp_ok( @r1v, '>', @r1c, "all variants found more records" );
    
    my %current = $T->extract_values( \@r1c, 'nam' );
    my %variant = $T->extract_values( \@r1v, 'nam' );
    my %accepted = $T->extract_values( \@r1v, 'acn' );
    my %diff = $T->extract_values( \@r1v, 'tdf' );
    
    $T->ok_is_subset( \%current, \%variant, "current names are subset of variant names" );
    $T->ok_is_subset( \%accepted, \%current, "accepted names are subset of current names" );
    
    ok( $diff{'misspelling of'}, "found difference 'misspelling of'" );
    ok( $diff{'recombined as'}, "found difference 'recombined as'" );
};


# Test the 'taxon_status' parameter.

subtest 'taxon status' => sub {
    
    my $NAME_1 = 'dinosauria^AVES';
    my $NAME_2 = 'STEGOSAURIA';
    my $OID_2 = 'txn:38798';
    
    my @r1a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxon_status=all", 
				"taxon_status=all 1");
    
    unless ( @r1a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my @r1v = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxon_status=valid",
				"taxon_status=valid 1");
    my @r1s = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxon_status=accepted",
				"taxon_status=accepted 1");
    my @r1j = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxon_status=junior",
				"taxon_status=junior 1");
    my @r1i = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxon_status=invalid",
				"taxon_status=invalid 1");
    
    cmp_ok( @r1a, '==', @r1v + @r1i, "all taxa = valid + invalid" );
    cmp_ok( @r1v, '==', @r1s + @r1j, "valid taxa = senior + junior" );
    
    my $bad_senior_tdf;
    
    foreach my $r ( @r1s )
    {
	if ( $r->{tdf} )
	{
	    $bad_senior_tdf = 1;
	    diag("    Taxon '$r->{nam}' has tdf '$r->{tdf}'");
	    last;
	}
    }
    
    ok( ! $bad_senior_tdf, "senior taxa have no tdf" );
    
    my %junior_status = ( 'subjective synonym of' => 1, 
			  'objective synonym of' => 1,
			  'replaced by'  => 1 );
    
    my $bad_junior_tdf;
    
    foreach my $r ( @r1j )
    {
	unless ( $r->{tdf} && $junior_status{$r->{tdf}} )
	{
	    $bad_junior_tdf = 1;
	    diag("    Taxon '$r->{nam}' has tdf '$r->{tdf}'");
	    last;
	}
    }
    
    ok( ! $bad_junior_tdf, "junior taxa have proper tdf" );
    
    my %invalid_status = ( 'nomen dubium' => 1,
			   'nomen nudum' => 1,
			   'nomen vanum' => 1,
			   'nomen oblitum' => 1,
			   'invalid subgroup of' => 1 );
    
    my $bad_invalid_tdf;
    
    foreach my $r ( @r1i )
    {
	unless ( $r->{tdf} && $invalid_status{$r->{tdf}} )
	{
	    $bad_invalid_tdf = 1;
	    diag("    Taxon '$r->{nam}' has tdf '$r->{tdf}'");
	    last;
	}
    }
    
    ok( ! $bad_invalid_tdf, "invalid taxa have proper tdf" );
    
    my $bad_valid_tdf;
    
    foreach my $r ( @r1v )
    {
	unless ( ! $r->{tdf} || $junior_status{$r->{tdf}} )
	{
	    $bad_valid_tdf = 1;
	    diag("    Taxon '$r->{nam}' has tdf '$r->{tdf}'");
	    last;
	}
    }
    
    ok( ! $bad_valid_tdf, "valid taxa have proper tdf" );
    
    # Now test with something other than base_name
    
    my @r2 = $T->fetch_records("/taxa/list.json?all_records&taxon_status=invalid&order=created&limit=10",
			       "last 10 invalid taxa entered");
    
    cmp_ok( @r2, '==', 10, "last 10 invalid taxa entered found 10 records" );
    
    my $bad_last_tdf;
    
    foreach my $r ( @r2 )
    {
	unless ( $r->{tdf} && $invalid_status{$r->{tdf}} )
	{
	    $bad_last_tdf = 1;
	    diag("    Taxon '$r->{nam}' has tdf '$r->{tdf}'");
	    last;
	}
    }
    
    ok( ! $bad_last_tdf, "last 10 invalid taxa have proper tdf" );
    
    # Test that the default status is 'all', and that 'status' is an acceptable alias for
    # 'taxon_status'. 
    
    my @r3d = $T->fetch_records("/taxa/list.json?base_name=$NAME_2", "default status 3");
    my @r3a = $T->fetch_records("/taxa/list.json?base_name=$NAME_2&taxon_status=all", "taxon_status=all 3");
    my @r3v = $T->fetch_records("/taxa/list.json?base_name=$NAME_2&taxon_status=valid", "taxon_status=valid 3");
    my @r3s = $T->fetch_records("/taxa/list.json?base_name=$NAME_2&taxon_status=accepted", "taxon_status=accepted 3");
    my @r3sx = $T->fetch_records("/taxa/list.json?base_name=$NAME_2&status=accepted", "status=accepted 3");
    my @r3j = $T->fetch_records("/taxa/list.json?base_name=$NAME_2&status=junior", "status=junior 3");
    my @r3i = $T->fetch_records("/taxa/list.json?base_name=$NAME_2&status=invalid", "status=invalid");
    
    cmp_ok( @r3d, '==', @r3a, "default status is same as taxon_status=all" );
    cmp_ok( @r3s, '==', @r3sx, "status=accepted is same as taxon_status=accepted" );
    
    my %junior_acc = $T->extract_values( \@r3j, 'acc' );
    my %junior_acn = $T->extract_values( \@r3j, 'acn' );
    my %invalid_acc = $T->extract_values( \@r3i, 'acc' );
    my %invalid_acn = $T->extract_values( \@r3i, 'acn' );
    
    my %accepted_oid = $T->extract_values( \@r3s, 'oid' );
    my %accepted_nam = $T->extract_values( \@r3s, 'nam' );
    my %invalid_oid = $T->extract_values( \@r3i, 'oid' );
    
    $T->ok_is_subset( \%junior_acc, \%accepted_oid, "junior 'acc' are subset of accepted 'nam'" );
    $T->ok_is_subset( \%junior_acn, \%accepted_nam, "junior 'acn' are subset of accepted 'oid'" );
    
    $T->ok_is_subset( \%invalid_acc, \%accepted_oid, "invalid 'acc' are subset of accepted 'nam'" );
    $T->ok_is_subset( \%invalid_acn, \%accepted_nam, "invalid 'acn' are subset of accepted 'oid'" );
    
    # Make sure that the parent of each of the valid taxa is in the accepted set, except for the
    # base taxon and any synonyms it may have.  The parent of an invalid taxon may itself be invalid.
    
    my ($bad_parent_oid, $bad_invalid_parent_oid);
    
    foreach my $r ( @r3j, @r3s )
    {
	unless ( $accepted_oid{$r->{par}} || ($r->{acn} && $r->{acn} eq $OID_2) || $r->{oid} eq $OID_2 )
	{
	    $bad_parent_oid = 1;
	    diag("    Found: '$r->{nam}' with parent '$r->{par}'");
	    last;
	}
    }
    
    foreach my $r ( @r3i )
    {
	unless ( $accepted_oid{$r->{par}} || $invalid_oid{$r->{par}} )
	{
	    $bad_invalid_parent_oid = 1;
	    diag("    Found: '$r->{nam}' with parent '$r->{par}'");
	    last;
	}
    }
    
    ok( ! $bad_parent_oid, "parent of each valid taxon is accepted" );
    ok( ! $bad_invalid_parent_oid, "parent of each invalid taxon is either accepted or invalid" );
    
    # Test a bad value for taxon_status
    
    my $m4 = $T->fetch_nocheck("/taxa/list.json?name=$NAME_2&status=foo", "bad status=");
    
    $T->ok_response_code( $m4, '400', "bad status= got 400 response" );
    $T->ok_error_like( $m4, qr{bad value.*status}i, "bad status= got proper error" );
};


# Now we test the 'rank' parameter.  This parameter allows for filtering the results by any set or
# range of ranks.  It is also sophisticated enough to handle unranked clades that fall into
# particular levels of the hierarchy with respect to ranked clades (i.e. above families, below orders).

subtest 'taxon rank' => sub {
    
    my $NAME_1 = "carnivoraMORPHA";
    
    my @r1 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&rank=family,infraorder", "ranks 1");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }    
    
    my @r2 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&rank=above_family-below_order", "ranks 2");
    my @r3 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&rank=min_family-max_order", "ranks 3");
    my @r4 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&rank=family-order", "ranks 4");
    
    my %rank1 = $T->extract_values( \@r1, 'rnk' );
    my %rank2 = $T->extract_values( \@r2, 'rnk' );
    my %rank3 = $T->extract_values( \@r3, 'rnk' );
    
    my %good1 = ( 9 => 1, 11 => 1 );
    my %good2 = ( 10 => 1, 11 => 1, 12 => 1, 25 => 1 );
    my %min2 = ( 11 => 1, 25 => 1 );
    my %good3 = ( 9 => 1, 10 => 1, 11 => 1, 12 => 1, 13 => 1, 25 => 1 );
    my %min3 = ( 9 => 1, 11 => 1, 13 => 1, 25 => 1 );
    
    is_deeply( \%rank1, \%good1, "ranks 1 found proper ranks" );
    $T->ok_is_subset( \%rank2, \%good2, "ranks 2 found proper ranks" );
    $T->ok_is_subset( \%min2, \%rank2, "ranks 2 found selected ranks" );
    $T->ok_is_subset( \%rank3, \%good3, "ranks 3 found proper ranks" );
    $T->ok_is_subset( \%min3, \%rank3, "ranks 3 found selected ranks" );
    
    cmp_ok( @r3, '==', @r4, "ranks 4 found same records as ranks 3" );
    
    # Now test a bad value for rank=
    
    my $BAD_1 = 'speciees';
    
    my $m4 = $T->fetch_nocheck("/taxa/list.json?name=$NAME_1&rank=genus,$BAD_1", "bad rank=");
    
    $T->ok_response_code( $m4, '400', "bad rank= got 400 response" );
    $T->ok_error_like( $m4, qr{invalid.*rank}i, "bad rank= got proper error" );
};


subtest 'extant and pres' => sub {

    my $NAME_1 = 'AVETHEROPODA^aves';
    my $NAME_2 = 'Aves';
    my $NAME_3 = 'Canis';
    
    my @r1j = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&show=pres", "base json");
    my @r1t = $T->fetch_records("/taxa/list.csv?base_name=$NAME_1&show=pres", "base csv");
    
    unless ( @r1j )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my %pres1j = $T->extract_values( \@r1j, 'prs', { count_empty => 1 } );
    my %pres1t = $T->extract_values( \@r1t, 'preservation', { count_empty => 1 } );
    
    ok( $pres1j{F}, "base json includes pres 'F'" );
    ok( $pres1j{IF}, "base json includes pres 'IF'" );
    ok( $pres1j{I}, "base json includes pres 'I'" );
    ok( $pres1j{''}, "base json includes pres ''" );
    
    ok( $pres1t{'form taxon'}, "base csv includes pres 'form taxon'" );
    ok( $pres1t{'ichnotaxon'}, "base csv includes pres 'ichnotaxon'" );
    ok( $pres1t{'ichnotaxon+form taxon'}, "base csv includes pres 'ichno+form'" );
    ok( $pres1t{''}, "base csv includes pres ''" );
    
    my @r3f = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&pres=form&show=pres", "pres=form");
    
    my %pres2f = $T->extract_values( \@r3f, 'prs', { count_empty => 1 } );
    
    ok( $pres2f{F}, "pres=form includes 'F'" );
    ok( $pres2f{IF}, "pres=form includes 'IF'" );

    cmp_ok( keys %pres2f, '==', 2, "pres=form finds proper records" );
    
    my @r3i = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&pres=ichno&show=pres", "pres=ichno");
    
    my %pres3f = $T->extract_values( \@r3i, 'prs', { count_empty => 1 } );
    
    ok( $pres3f{I}, "pres=ichno includes 'I'" );
    ok( $pres3f{IF}, "pres=ichno includes 'IF'" );
    
    cmp_ok( keys %pres3f, '==', 2, "pres=form finds proper records" );
    
    my @r3fr = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&pres=form,regular&show=pres", 
				 "pres=form,regular");
    
    my %pres3fr = $T->extract_values( \@r3fr, 'prs', { count_empty => 1 } );
    
    ok( $pres3fr{F}, "pres=form,regular includes 'F'" );
    ok( $pres3fr{IF}, "pres=form,regular includes 'IF'" );
    ok( $pres3fr{''}, "pres=form,regular includes ''" );
    
    cmp_ok( keys %pres3fr, '==', 3, "pres=form,regular finds proper records" );
    
    my @r3fa = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&pres=form,all&show=pres",
				 "pres=form,all");
    my @r3a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&pres=all&show=pres", "pres=all");
    
    cmp_ok( @r3fa, '==', @r1j, "pres=form,all and base find same number of records" );
    cmp_ok( @r3a, '==', @r1j, "pres=all and base find same number of records" );
    
    my @r4a = $T->fetch_records("/taxa/list.json?base_name=$NAME_2", "all aves");
    my @r4b = $T->fetch_records("/taxa/list.json?base_name=$NAME_2&extant=yes", "extant aves");
    my @r4c = $T->fetch_records("/taxa/list.json?base_name=$NAME_2&extant=no", "extinct aves");
    
    cmp_ok( @r4b, '<', @r4a, "extant aves are subset of all" );
    cmp_ok( @r4c, '<', @r4a, "extinct aves are subset of all" );
    
    my ($bad_extant, $bad_extinct);
    
    foreach my $r ( @r4b )
    {
	$bad_extant ||= $r->{nam} unless defined $r->{ext} && $r->{ext} eq '1';
    }
    
    foreach my $r ( @r4c )
    {
	$bad_extinct ||= $r->{nam} unless defined $r->{ext} && $r->{ext} eq '0';
    }
    
    unless ( ok( ! $bad_extant, "extant aves returned bad value for 'ext'" ) )
    {
	diag("    Found '$bad_extant'" );
    }
    
    unless ( ok( ! $bad_extinct, "extinct aves returned bad value for 'ext'" ) )
    {
	diag("    Found '$bad_extinct'" );
    }
    
    # Now test alternate values 1 and 0 for extant
    
    my @r5b = $T->fetch_records("/taxa/list.json?base_name=$NAME_3&extant=1", "extant=1");
    my @r5c = $T->fetch_records("/taxa/list.json?base_name=$NAME_3&extant=0", "extant=0");
    
    cmp_ok( @r5b, '!=', @r5c, "extant=1 gets different result from extant=0" );
    
    # Also test true, false, on, off
    
    foreach my $v ( qw(true false on off) )
    {
	$T->fetch_records("/taxa/list.json?base_name=$NAME_3&extant=$v", "extant=$v");
    }
    
    # Now test bad values for extant and pres
    
    my $m6 = $T->fetch_nocheck("/taxa/list.json?base_name=$NAME_3&extant=foo", "extant=foo");
    
    $T->ok_response_code($m6, '400', "extant=foo got 400 response");
    
    my $m7 = $T->fetch_nocheck("/taxa/list.json?base_name=$NAME_3&pres=foo", "pres=foo");
    
    $T->ok_response_code($m7, '400', "pres=foo got 400 response");    
};


subtest 'interval and ma bounds' => sub {
    
    my $NAME_1 = 'Dascillidae';
    my $INT_1 = 'Cretaceous';
    my $INT_2 = 'Pleistocene';
    my $INT_3 = 'Miocene';
    
    # First check interval names and ids
    
    my @r1 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&show=app", "list base name");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my @r2 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&interval=$INT_1&show=app",
			       "list with interval '$INT_1'");
    
    my @r3 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&interval=$INT_2&show=app",
			       "list with interval '$INT_2'");
    
    cmp_ok( @r2, '<', @r1, "list with interval '$INT_1' found fewer records" );
    cmp_ok( @r3, '<', @r1, "list with interval '$INT_2' found fewer records" );
    
    my ($int1) = $T->fetch_records("/intervals/single.json?name=$INT_1", "interval '$INT_1'");
    my ($int2) = $T->fetch_records("/intervals/single.json?name=$INT_2", "interval '$INT_2'");
    my ($int3) = $T->fetch_records("/intervals/single.json?name=$INT_3", "interval '$INT_3'");
    
    my $IID_1 = $int1->{oid};
    my $IID_2 = $int2->{oid};
    my $IID_3 = $int3->{oid};
    
    my ($bad_taxon1, $bad_taxon2);
    
    foreach my $r ( @r2 )
    {
	if ( $r->{lla} > ($int1->{eag} + 0) || $r->{fea} < ($int1->{lag} + 0) )
	{
	    $bad_taxon1 = $r->{nam};
	}
    }
    
    ok( ! $bad_taxon1, "list taxa with interval '$INT_1' returns taxa within interval" ) ||
	diag("    Found: '$bad_taxon1'");
    
    foreach my $r ( @r3 )
    {
	if ( $r->{lla} > ($int2->{eag} + 0) || $r->{fea} < ($int2->{lag} + 0) )
	{
	    $bad_taxon2 = $r->{nam};
	}
    }

    ok( ! $bad_taxon2, "list taxa with interval '$INT_2' returns taxa within interval" ) ||
	diag("    Found: '$bad_taxon2'");
    
    my @r4 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&interval_id=$IID_1&show=app",
			       "list with interval id '$IID_1'");
    
    cmp_ok( @r2, '==', @r4, "list with interval name and id found same record count" );
    
    # Check to make sure that numeric identifiers work too.
    
    my $NUM_1;
    
    if ( $IID_1 =~ qr{^int:(\d+)$} )
    {
	$NUM_1 = $1;
	
	my @r4a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&interval_id=$NUM_1&show=app",
				    "list with interval id '$NUM_1'");
	
	is_deeply( \@r4, \@r4a, "numeric interval id gets same result as external id");
    }
    
    else
    {
	fail("Interval identifier did not have proper form");
	diag("    Got: $IID_1");
	diag("    Expected: txn:nnnn");
    }
    
    # Now check age bounds.
    
    my $AGE_1 = '30';
    my $AGE_2 = '50';
    
    my @r5 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&max_ma=$AGE_1&show=app",
			       "list with max_ma '$AGE_1'");
    
    my @r6 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&min_ma=$AGE_1&show=app",
			       "list with min_ma '$AGE_1'");
    
    my @r7 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&min_ma=$AGE_1&max_ma=$AGE_2&show=app",
			       "list with min_ma '$AGE_1' and max_ma '$AGE_2'");
    
    cmp_ok( @r5, '<', @r1, "list with max_ma '$AGE_1' found fewer records" );
    cmp_ok( @r6, '<', @r1, "list with min_ma '$AGE_1' found fewer records" );
    cmp_ok( @r7, '<', @r1, "list with age range found fewer records" );
    
    my ($bad_taxon3, $bad_taxon4, $bad_taxon5);
    
    foreach my $r ( @r5 )
    {
	if ( $r->{lla} > $AGE_1 )
	{
	    $bad_taxon3 = $r->{nam};
	}
    }
    
    ok( ! $bad_taxon3, "list taxa with max_ma '$AGE_1' returns taxa within interval" ) ||
	diag("    Found: '$bad_taxon3'");
    
    foreach my $r ( @r6 )
    {
	if ( $r->{fea} < $AGE_1 )
	{
	    $bad_taxon4 = $r->{nam};
	}
    }
    
    ok( ! $bad_taxon4, "list taxa with min_ma '$AGE_1' returns taxa within interval" ) ||
	diag("    Found: '$bad_taxon4'");
    
    foreach my $r ( @r7 )
    {
	if ( $r->{lla} > $AGE_2 || $r->{fea} < $AGE_1 )
	{
	    $bad_taxon5 = $r->{nam};
	}
    }
    
    ok( ! $bad_taxon5, "list taxa with age range returns taxa within interval" ) ||
	diag("    Found: '$bad_taxon5'");
    
    # Test multiple names, ids
    
    my @r8 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&interval=$INT_2 , $INT_3&show=app",
			       "list with two intervals");
    
    my $max = $int2->{eag} > $int3->{eag} ? $int2->{eag} : $int3->{eag};
    my $min = $int2->{lag} < $int3->{lag} ? $int2->{lag} : $int3->{lag};
    
    my ($bad_taxon6, $bad_taxon7);
    
    foreach my $r ( @r8 )
    {
	if ( $r->{lla} > $max || $r->{fea} < $min )
	{
	    $bad_taxon6 = $r->{nam};
	}
    }
    
    ok( ! $bad_taxon6, "list with two intervals returns taxa within interval" ) ||
	diag("    Found: '$bad_taxon6'");
    
    my @r9 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&interval_id=$IID_2 ,$IID_3&show=app",
			       "list with two interval ids");
    
    foreach my $r ( @r9 )
    {
	if ( $r->{lla} > $max || $r->{fea} < $min )
	{
	    $bad_taxon7 = $r->{nam};
	}
    }
    
    ok( ! $bad_taxon7, "list with two interval ids returns taxa within interval" ) ||
	diag("    Found: '$bad_taxon7'");
    
    # Bad name, bad id
    
    my $m1 = $T->fetch_nocheck("/taxa/list.json?base_name=$NAME_1&interval=$INT_1,foo",
			       "bad interval name");
    
    $T->ok_response_code($m1, '400', "bad interval name got proper response");
    
    my $m2 = $T->fetch_nocheck("/taxa/list.json?base_name=$NAME_1&interval_id=bar,$IID_1",
			       "bad interval id");
    
    $T->ok_response_code($m2, '400', "bad interval id got proper response");
};



# $$$ does 'base_name basic' test connectedness of result set?

# Test the 'depth' parameter.  This test also checks that if all taxa in a subtree are retrieved
# then the parent links will all match up except for the top one.

subtest 'tree depth' => sub {
    
    my $TID_1 = 'txn:41189'; # Canidae
    my $DEPTH_1 = 3;
    
    # Do a basic check on the depth= parameter
    
    my @r1 = $T->fetch_records("/taxa/list.json?base_id=$TID_1", "base list");
    my @r2 = $T->fetch_records("/taxa/list.json?base_id=$TID_1&depth=$DEPTH_1", "limited depth '$DEPTH_1'");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    cmp_ok( @r2, '<', @r1, "limited depth retrieves fewer records" );
    
    # Then go through the depth-limited result set, tracing the parent links to make sure that the
    # maximum depth is actually the value specified.  Because the base taxon could have synonyms,
    # we have to check the 'acc' link if 'par' doesn't match.
    
    my %depth;
    my $max_depth;
    my $bad_taxon;
    
    foreach my $r ( @r2 )
    {
	my $oid = $r->{oid}; # id of this taxon
	my $pid = $r->{par}; # id of parent taxon
	my $sid = $r->{acc}; # id of senior synonym if it is a junior
	
	# If this taxon is the base, then set its depth to 0.
	
	if ( $oid eq $TID_1 )
	{
	    $depth{$oid} = 0;
	}
	
	# If we know the depth of this taxon's parent, then set its depth to one greater.  Keep
	# track of the maximum.
	
	elsif ( defined $depth{$pid} )
	{
	    $depth{$oid} = $depth{$pid} + 1;
	    $max_depth = $depth{$oid} if ! defined $max_depth || $depth{$oid} > $max_depth;
	}
	
	# If we know the depth of this taxon's accepted taxon but not of its parent, then it is a
	# synonym of the base taxon.  So set its depth to 0.
	
	elsif ( defined $depth{$sid} )
	{
	    $depth{$oid} = 0;
	}
	
	# Otherewise, we've found an error in the result set.
	
	else
	{
	    $bad_taxon = $r->{nam};
	    last;
	}
    }
    
    ok( ! defined $bad_taxon, "limited depth found a connected set of taxa" ) ||
	diag("    Found: '$bad_taxon' whose parent did not appear in the result set'");
    
    cmp_ok( $max_depth, '==', $DEPTH_1, "limited depth found the proper maximum subtree depth" );
    
    # Test depth with a bad parameter value.
    
    my $m3 = $T->fetch_nocheck("/taxa/list.json?base_name=$TID_1&depth=-1", "negative depth");
    my $m4 = $T->fetch_nocheck("/taxa/list.json?base_name=$TID_1&depth=foo", "bad depth");
    
    $T->ok_response_code( $m3, '400', "negative depth got 400 result" );
    $T->ok_response_code( $m4, '400', "bad depth got 400 result" );
};


# Test 'all_taxa' and 'all_records' with 'limit' and 'offset'. We are not
# going to test all_records without a limit, because that would involve
# fetching a 30+ MB result.

subtest 'all_taxa, all_records, limit and offset' => sub {
    
    # Fetch two overlapping sections. Make sure they overlap properly, and also check that the
    # oids are in numerical order. (The default order should be the order in which the entries
    # occur in the taxon_trees table, which is in order by orig_no).
    
    my @r1 = $T->fetch_records("/taxa/list.csv?all_taxa&rowcount&offset=1000&limit=100", "offset 1000, limit 100");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    $T->cmp_ok_meta( 'record_offset', '==', '1000', "offset 1000, limit 100 got proper record_offset" );
    $T->cmp_ok_meta( 'records_found', '>', 260000, "offset 1000, limit 100 got proper records_found" );
    $T->cmp_ok_meta( 'records_returned', '==', '100', "offset 1000, limit 100 got proper records_returned" );
    cmp_ok( @r1, '==', 100, "offset 1000, limit 100 returned 100 records" );
    
    my @r2 = $T->fetch_records("/taxa/list.csv?all_taxa&rowcount&offset=1050&limit=100", "offset 1050, limit 100");
    
    $T->cmp_ok_meta( 'record_offset', '==', '1050', "offset 1050, limit 100 got proper record_offset" );
    
    my $bad_index;
    
    foreach my $i ( 0..49 )
    {
	unless ( $r2[$i]{orig_no} && $r1[$i+50]{orig_no} && $r2[$i]{orig_no} eq $r1[$i+50]{orig_no} )
	{
	    $bad_index = $i;
	    last;
	}
    }
    
    ok( ! $bad_index, "records from overlapping ranges matched up" ) ||
	diag("    Difference started at index $bad_index");
    
    $T->check_order( \@r1, 'orig_no', '<', 'taxon_name', 
		     "offset 1000 returned records in order by orig_no" );
    
    $T->check_order( \@r2, 'orig_no', '<', 'taxon_name',
		     "offset 1050 returned records in order by orig_no" );
    
    # Get just the number of taxa without actually returning any records.
    
    my $m3 = $T->fetch_nocheck("/taxa/list.json?all_taxa&limit=0&rowcount", "all taxa count");
    
    my @r3 = $T->extract_records($m3, "all records count", { no_records_ok => 1 });
    
    $T->cmp_ok_meta( $m3, 'records_found', '>', 260000, "all records count got propr records_found" );
    $T->cmp_ok_meta( $m3, 'records_returned', 'eq', '0', "all records count found got proper records_returned" );
    cmp_ok( @r3, '==', 0, "all records count returned no records" );
    
    my $count_taxa = $T->get_meta( $m3, 'records_found' );
    
    # Now try 'all_records' instead of 'all_taxa'.  This will get all name
    # variants. 
    
    my $m4 = $T->fetch_nocheck("/taxa/list.json?all_records&limit=0&rowcount", "all records count");
    
    my $count_records = $T->get_meta( $m4, 'records_found' );
    
    cmp_ok( $count_records, '>', $count_taxa, "all_records found more records than all_taxa" );
    
    my @r5 = $T->fetch_records("/taxa/list.json?all_records&offset=10000&limit=200", 
			       "all_records offset 10000");
    
    my $tc = Test::Conditions->new;
    
    $tc->set("no record with 'V' flag");
    
    foreach my $r ( @r5 )
    {
	$tc->clear("no record with 'V' flag") if $r->{flg} && $r->{flg} =~ /V/;
    }
    
    $tc->ok_all("all_records found at least one 'V' flag");
    
    my @r6 = $T->fetch_records("/taxa/list.json?all_taxa&offset=10000&limit=200",
			       "all_taxa offset offset 10000");
    
    foreach my $r ( @r6 )
    {
	$tc->flag("found record with flag(s)", 'nam') if $r->{flg};
    }
    
    $tc->ok_all("all_taxa no records have flags");
};


# Test that the various sort orders work properly.

subtest 'sort order' => sub {
    
    my $NAME_1 = 'Dascillidae';
    my $NAME_2 = 'Canidae';
    
    # Start with the default, which should be 'hierarchy';
    
    my @r1a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&show=seq", "order default");
    
    unless ( @r1a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my @r1b = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=hierarchy&show=seq", 
				"order hierarchy");
    my @r1c = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=hierarchy.desc&show=seq",
				"order hierarchy.desc");
    
    $T->check_order( \@r1b, 'lsq', '<', 'nam', "order hierarchy" );
    
    is_deeply( \@r1a, \@r1b, "default order is 'hierarchy'");
    
    $T->check_order( \@r1c, 'lsq', '>', 'nam', "order hierarchy.desc" );
    
    # Then check 'name'
    
    my @r2a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=name", 
				"order name");
    my @r2b = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=name.desc", 
				"order name.desc");
    
    $T->check_order( \@r2a, 'nam', 'lt', 'nam', "order name" );
    $T->check_order( \@r2b, 'nam', 'gt', 'nam', "order name.desc" );
    
    # Then check 'ref'
    
    my @r3a = $T->fetch_records("/taxa/list.csv?base_name=$NAME_1&order=ref", 
				"order ref");
    my @r3b = $T->fetch_records("/taxa/list.csv?base_name=$NAME_1&order=ref.desc", 
				"order ref.desc");
    
    $T->check_order( \@r3a, 'reference_no', '<=', 'taxon_name', "order ref" );
    $T->check_order( \@r3b, 'reference_no', '>=', 'taxon_name', "order ref.desc" );
    
    # Then check 'firstapp' and 'lastapp'.  We have to skip all records with no occurrences, since
    # they will have no ages of first and last appearance.
    
    my @r4a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=firstapp&show=app",
				"order firstapp");
    my @r4b = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=lastapp&show=app",
				"order lastapp");
    my @r4c = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=firstapp.asc&show=app",
				"order firstapp.asc");
    my @r4d = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=lastapp.asc&show=app",
				"order lastapp.asc");
    
    foreach my $r ( @r4a, @r4b, @r4c, @r4d )
    {
	$r->{SKIP_ORDER} = 1 if defined $r->{noc} && $r->{noc} eq '0';
    }
    
    $T->check_order( \@r4a, 'fea', '>=', 'nam', "order firstapp" );
    $T->check_order( \@r4b, 'lla', '>=', 'nam', "order lastapp" );
    $T->check_order( \@r4c, 'fea', '<=', 'nam', "order firstapp.asc" );
    $T->check_order( \@r4d, 'lla', '<=', 'nam', "order lastapp.asc" );
    
    # Check 'agespan'.
    
    my @r5a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=agespan&show=app",
				"order agespan");
    my @r5b = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=agespan.asc&show=app",
				"order agespan.asc");
    
    foreach my $r ( @r5a, @r5b )
    {
	if ( defined $r->{noc} && $r->{noc} eq '0' )
	{
	    $r->{SKIP_ORDER} = 1;
	} else {
	    $r->{agespan} = $r->{fea} - $r->{lla};
	}
    }
    
    $T->check_order( \@r5a, 'agespan', '>=', 'nam', "order agespan" );
    $T->check_order( \@r5b, 'agespan', '<=', 'nam', "order agespan.asc" );
    
    # Check 'n_occs'.
    
    my @r6a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=n_occs",
				"order n_occs");
    my @r6b = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=n_occs.asc",
				"order n_occs.asc");
    
    $T->check_order( \@r6a, 'noc', '>=', 'nam', "order n_occs" );
    $T->check_order( \@r6b, 'noc', '<=', 'nam', "order n_occs.asc" );
    
    # Check 'author', 'pubyr'
    
    my @r7a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=author&show=attr",
				"order author");
    my @r7b = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=author.desc&show=attr",
				"order author.desc");
    my @r7c = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=pubyr&show=attr",
				"order pubyr");
    my @r7d = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=pubyr.asc&show=attr",
				"order pubyr.asc");
    
    foreach my $r ( @r7a, @r7b )
    {
	$r->{att} =~ s/[()]//g if $r->{att};
	$r->{att} =~ s/\s*\d+//g if $r->{att};
    }
    
    foreach my $r ( @r7c, @r7d )
    {
	if ( $r->{att} && $r->{att} =~ /(\d\d\d\d)/ )
	{
	    $r->{pby} = $1;
	}
    }
    
    $T->check_order( \@r7a, 'att', 'le', 'nam', "order author" );
    $T->check_order( \@r7b, 'att', 'ge', 'nam', "order author.desc" );
    $T->check_order( \@r7c, 'pby', 'ge', 'nam', "order pubyr" );
    $T->check_order( \@r7d, 'pby', 'le', 'nam', "order author.asc" );
    
    # Check 'size', 'extant_size'
    
    my @r8a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&show=size&order=size",
				"order size");
    my @r8b = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&show=size&order=size.asc",
				"order size");
    my @r8c = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&show=size&order=extsize",
				"order size");
    my @r8d = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&show=size&order=extsize.asc",
				"order size");
    
    $T->check_order( \@r8a, 'siz', '>=', 'nam', "order size");
    $T->check_order( \@r8b, 'siz', '<=', 'nam', "order size.asc");
    $T->check_order( \@r8c, 'exs', '>=', 'nam', "order extsize");
    $T->check_order( \@r8d, 'exs', '<=', 'nam', "order extsize.asc");
    
    # Check 'extant'. We need to replace null with a numeric value for the order check to work
    # properly.
    
    my @r9a = $T->fetch_records("/taxa/list.json?base_name=$NAME_2&order=extant",
				"order extant");
    my @r9b = $T->fetch_records("/taxa/list.json?base_name=$NAME_2&order=extant.asc",
				"order extant.asc");
    
    foreach my $r ( @r9a )
    {
	$r->{ext} = -1 unless defined $r->{ext};
    }
    
    foreach my $r ( @r9b )
    {
	$r->{ext} = 2 unless defined $r->{ext};
    }
    
    $T->check_order( \@r9a, 'ext', '>=', 'nam', "order extant");
    $T->check_order( \@r9b, 'ext', '<=', 'nam', "order extant.asc");
    
    # Check 'created', 'modified'
    
    my @r10a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&show=crmod&order=created",
				 "order created");
    my @r10b = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&show=crmod&order=created.asc",
				 "order created.asc");
    my @r10c = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&show=crmod&order=modified",
				 "order modified");
    my @r10d = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&show=crmod&order=modified.asc",
				 "order modified.asc");
    
    $T->check_order( \@r10a, 'dcr', 'ge', 'nam', "order created" );
    $T->check_order( \@r10b, 'dcr', 'le', 'nam', "order created.asc" );
    $T->check_order( \@r10c, 'dmd', 'ge', 'nam', "order modified" );
    $T->check_order( \@r10d, 'dmd', 'le', 'nam', "order modified.asc" );
    
    # Now check order with multiple parameters. We can't possibly check all combinations, so we'll
    # try just a few.
    
    my @r20a = $T->fetch_records("/taxa/list.json?base_name=$NAME_2&order=n_occs,name",
				 "order n_occs, name");
    
    $T->check_order( \@r20a, ['noc', 'nam'], ['>=', 'le'], 'nam', "order n_occs, name");
    
    my @r20b = $T->fetch_records("/taxa/list.json?base_name=$NAME_2&order=n_occs.asc,name.desc",
				 "order n_occs, name reversed");
    
    @r20b = reverse @r20b;
    
    is_deeply( \@r20a, \@r20b, "order multiple reversed reverses result order" );
    
    # Then, we will need to try an order parameter with at least a few of the
    # other methods of selecting base taxa.
    
    my $NAME_20 = "Canis %";
    
    my @r21a = $T->fetch_records("/taxa/list.json?match_name=$NAME_20&show=attr&order=n_occs,author",
				 "match order n_occs, author");
    
    foreach my $r ( @r21a )
    {
	$r->{att} =~ s/[()]//g if $r->{att};
	$r->{att} =~ s/\s*\d+//g if $r->{att};
    }
    
    $T->check_order( \@r21a, ['noc', 'att'], ['>=', 'le'], 'nam', "match order n_occs, author" );
    
    my $NAME_21 = "Canis,Felis,Conus,Ursus";
    
    my @r21b = $T->fetch_records("/taxa/list.csv?taxon_name=$NAME_21&show=app&order=firstapp",
				 "taxon_name order firstapp");
    
    $T->check_order( \@r21b, 'firstapp_max_ma', '>=', 'taxon_name', "taxon_name order firstapp" );
    
    my $NAME_22 = "Stegosaurus";
    
    my @r22a = $T->fetch_records("/taxa/list.json?name=$NAME_22&rel=synonyms&show=attr&order=pubyr.asc",
				"synonyms order pubyr.asc");
    
    foreach my $r ( @r22a )
    {
	$r->{pby} = $1 if $r->{att} && $r->{att} =~ /(\d\d\d\d)/;
    }
    
    $T->check_order( \@r22a, 'pby', '<=', 'nam', "synonyms order pubyr.asc" );
};


# Then test the parameters for listing taxa by created/modified dates.  This will generally be
# used to show the most recently entered taxa.

subtest 'list by crmod and all_records' => sub {
    
    my $NAME_1 = 'Mammalia';
    my $COUNT_1 = '50';
    my $COUNT_2 = '100';
    
    my @r1a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&order=created&show=crmod&limit=$COUNT_1",
				"latest $COUNT_1 taxa from '$NAME_1'");
    
    unless ( @r1a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    cmp_ok( @r1a, '==', $COUNT_1, "latest $COUNT_1 taxa from '$NAME_1' found $COUNT_1 records" );
    $T->check_order( \@r1a, 'dcr', 'ge', 'nam', "latest $COUNT_1 taxa from '$NAME_1'" );
    
    my @r2a = $T->fetch_records("/taxa/list.json?all_records&order=modified&show=crmod&limit=$COUNT_2",
				"latest $COUNT_2 taxa");
    
    cmp_ok( @r2a, '==', $COUNT_2, "latest $COUNT_2 taxa found $COUNT_2 records" );
    $T->check_order( \@r2a, 'dmd', 'ge', 'nam', "latest $COUNT_2 taxa" );
    
    my @r2b = $T->fetch_records("/taxa/list.json?all_records&order=name&taxa_created_before=2003&show=crmod",
				"created before 2003");
    
    cmp_ok( @r2b, '>', 500, "created before 2003 finds at least 500 records" );
    $T->check_order( \@r2b, 'nam', 'le', 'nam', "created before 2003 finds proper record order" );
    
    my $bad2b;
    
    foreach my $r ( @r2b )
    {
	$bad2b = $r->{nam} unless $r->{dcr} && $r->{dcr} lt "2003" && 
	    $r->{dcr} =~ /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/;
    }
    
    ok( ! $bad2b, "created before 2003 finds records with proper creation date" ) ||
	diag( "    Found: '$bad2b' with improper date" );
    
    my @r2c = $T->fetch_records("/taxa/list.json?all_records&taxa_modified_after=2014&show=crmod&limit=100",
				"modified after 2014");
    
    cmp_ok( @r2c, '==', 100, "modified after 2014 finds proper number of records" );
    
    my $bad2c;
    
    foreach my $r ( @r2c )
    {
	$bad2c = $r->{nam} unless $r->{dmd} && $r->{dmd} ge "2014" && 
	    $r->{dmd} =~ /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/;
    }
    
    ok( ! $bad2c, "created after 2014 finds records with proper creation date" ) ||
	diag( "    Found: '$bad2c' with improper date" );
};


# Check that authorizers, enterers and modifiers are reported properly, and that we can select taxa using
# names and identifiers.

subtest 'list by authent' => sub {
    
    my $NAME_1 = 'Felidae';
    
    # First fetch a set of base records regardless of authorizer, enterer or modifier.
    
    my @r1 = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&show=ent,entname", "base records");
    
    unless ( @r1 )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    # Make sure that the name and id fields all have proper values.
    
    my $tc = Test::Conditions->new;
    
 RECORD:
    foreach my $r ( @r1 )
    {
	foreach my $k ( 'ati', 'eni', 'mdi' )
	{
	    next if $k eq 'mdi' && ! defined $r->{mdi};
	    
	    $tc->flag($k, $r->{nam}) unless defined $r->{$k} && $r->{$k} =~ /^prs:\d+$/;
	}
	
	foreach my $k ( 'ath', 'ent', 'mdf' )
	{
	    next if $k eq 'mdf' && ! defined $r->{mdf};
	    
	    $tc->flag($k, $r->{nam}) unless defined $r->{$k} && $r->{$k} ne '' && $r->{$k} !~ /\d/;
	}
    }
    
    $tc->ok_all("base records all have proper authent names and ids");
    
    # Find the person who has authorized the most records, then the non-authorizer who has entered
    # the most records, then the non-enterer who has modified the most records.  Do this for both
    # names and ids.
    
    my %ati = $T->count_values( \@r1, 'ati' );
    my %eni = $T->count_values( \@r1, 'eni' );
    my %mdi = $T->count_values( \@r1, 'mdi' );
    my %ath = $T->count_values( \@r1, 'ath' );
    my %ent = $T->count_values( \@r1, 'ent' );
    my %mdf = $T->count_values( \@r1, 'mdf' );
    
    my ($ath_max, $ath_count) = $T->find_max( \%ath ); delete $ent{$ath_max}; delete $mdf{$ath_max};
    my ($ent_max, $ent_count) = $T->find_max( \%ent ); delete $mdf{$ent_max};
    my ($mdf_max, $mdf_count) = $T->find_max( \%mdf );
    
    diag("   ath: $ath_max  ent: $ent_max  mdf: $mdf_max");
    
    my ($ati_max, $ati_count) = $T->find_max( \%ati ); delete $eni{$ati_max}; delete $mdi{$ati_max};
    my ($eni_max, $eni_count) = $T->find_max( \%eni ); delete $mdi{$eni_max};
    my ($mdi_max, $mdi_count) = $T->find_max( \%mdi );
    
    # Select all the records authorized and not authorized by that person, and make sure the
    # totals add up.
    
    my @r2a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_authorized_by=$ath_max&show=ent,entname",
				"authorized by max name");
    my @r2b = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_authorized_by=!$ath_max&show=ent,entname",
				"not authorized by max name");
    
    cmp_ok( @r2a + @r2b, '==', @r1, "authorized by + not authorized by = all" );
    
    # Same with external identifiers.
    
    my @r2c = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_authorized_by=$ati_max&show=ent,entname",
				"authorized by max id");
    my @r2d = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_authorized_by=!$ati_max&show=ent,entname",
				"not authorized by max id");
    
    cmp_ok( @r2c + @r2d, '==', @r1, "authorized by + not authorized by = all" );
    
    cmp_ok( @r2c, '==', @r2a, "authorized_by ati max = authorized_by ath max" );
    cmp_ok( @r2d, '==', @r2b, "not authorized_by ati max = not authorized_by ath max" );
    
    # Same with numeric identifiers.
    
    $ati_max =~ /(\d+)/; my $ati_num = $1;
    
    my @r2e = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_authorized_by=$ati_num&show=ent,entname",
				"authorized by max num");
    my @r2f = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_authorized_by=!$ati_num&show=ent,entname",
				"not authorized by max num");
    
    cmp_ok( @r2e + @r2f, '==', @r1, "authorized by + not authorized by = all" );
    
    cmp_ok( @r2c, '==', @r2e, "authorized_by ati max = authorized_by ati num max" );
    cmp_ok( @r2d, '==', @r2f, "not authorized_by ati max = not authorized_by ati num max" );
    
    # Make sure that each of the records has the proper identifier and name.
    
    foreach my $r ( @r2a, @r2c, @r2e )
    {
	$tc->flag('ati', $r->{nam}) unless $r->{ati} eq $ati_max;
	$tc->flag('ath', $r->{nam}) unless $r->{ath} eq $ath_max;
    }
    
    $tc->ok_all("authorized by max finds records with proper name and id");
    
    foreach my $r ( @r2b, @r2d, @r2f )
    {
	$tc->flag('ati', $r->{nam}) unless $r->{ati} ne $ati_max;
	$tc->flag('ath', $r->{nam}) unless $r->{ath} ne $ath_max;
    }
    
    $tc->ok_all("not authorized by max finds records with proper name and id");
    
    # Now check enterers in the same way.
    
    my @r3a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_entered_by=$ent_max&show=ent,entname",
				"entered by max name");
    my @r3b = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_entered_by=!$ent_max&show=ent,entname",
				"not entered by max name");
    
    cmp_ok( @r3a + @r3b, '==', @r1, "entered by + not entered by = all" );
    
    # Same with external identifiers.
    
    my @r3c = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_entered_by=$eni_max&show=ent,entname",
				"entered by max id");
    my @r3d = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_entered_by=!$eni_max&show=ent,entname",
				"not entered by max id");
    
    cmp_ok( @r3c + @r3d, '==', @r1, "entered by + not entered by = all" );
    
    # Same with numeric identifiers.
    
    $eni_max =~ /(\d+)/; my $eni_num = $1;
    
    my @r3e = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_entered_by=$eni_num&show=ent,entname",
				"entered by max num");
    my @r3f = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_entered_by=!$eni_num&show=ent,entname",
				"not entered by max num");
    
    cmp_ok( @r3e + @r3f, '==', @r1, "entered by + not entered by = all" );
    
    # Again make sure that each of the records has the proper identifier and name.
    
    foreach my $r ( @r3a, @r3c, @r3e )
    {
	$tc->flag('eni', $r->{nam}) unless $r->{eni} eq $eni_max;
	$tc->flag('ent', $r->{nam}) unless $r->{ent} eq $ent_max;
    }
    
    $tc->ok_all("entered by max finds records with proper name and id");
    
    foreach my $r ( @r3b, @r3d, @r3f )
    {
	$tc->flag('eni', $r->{nam}) unless $r->{eni} ne $eni_max;
	$tc->flag('ent', $r->{nam}) unless $r->{ent} ne $ent_max;
    }
    
    $tc->ok_all("not entered by max finds records with proper name and id");
    
    # Now same for modifiers.  For this, we have to take into account that not every record may
    # have a modifier.
    
    my @r4any = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_modified_by=%&show=ent,entname",
				   "modified by any");
    
    my @r4a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_modified_by=$mdf_max&show=ent,entname",
				"modified by max name");
    my @r4b = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_modified_by=!$mdf_max&show=ent,entname",
				"not modified by max name");
    
    cmp_ok( @r4a + @r4b, '==', @r1, "modified by + not modified by = modified by any (mdf max)" );
    
    # Same with external identifiers.
    
    my @r4c = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_modified_by=$mdi_max&show=ent,entname",
				"modified by max id");
    my @r4d = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_modified_by=!$mdi_max&show=ent,entname",
				"not modified by max id");
    
    cmp_ok( @r4c + @r4d, '==', @r1, "modified by + not modified by = modified by any (mdi max)" );
    
    # Same with numeric identifiers.
    
    $mdi_max =~ /(\d+)/; my $mdi_num = $1;
    
    my @r4e = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_modified_by=$mdi_num&show=ent,entname",
				"modified by max num");
    my @r4f = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_modified_by=!$mdi_num&show=ent,entname",
				"not modified by max num");
    
    cmp_ok( @r4e + @r4f, '==', @r1, "modified by + not modified by = modified by any (mdi num)" );
    
    # Again make sure that each of the records has the proper identifier and name.
    
    foreach my $r ( @r4a, @r4c, @r4e )
    {
	$tc->flag('invalid', $r->{nam}) unless ! $r->{mdi} || $r->{mdi} eq $mdi_max && $r->{mdf} eq $mdf_max;
    }
    
    $tc->ok_all("modified by max finds records with proper name and id");
    
    foreach my $r ( @r4b, @r4d, @r4f )
    {
	$tc->flag('invalid', $r->{nam}) unless ! $r->{mdi} || $r->{mdi} ne $mdi_max && $r->{mdf} ne $mdf_max;
    }
    
    $tc->ok_all("not modified by max finds records with proper name and id");
    
    # Now we need to try the value '!'. This should return no records for 'authorized_by' and
    # 'entered_by', and only taxa that have not been modified for 'modified_by'.
    
    my @r5a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_authorized_by=!&show=ent,entname",
				"authorized by '!'", { no_records_ok => 1 });
    
    cmp_ok( @r5a, '==', 0, "authorized by '!' found no records" );
    
    my @r5b = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_entered_by=!&show=ent,entname",
				"entered by '!'", { no_records_ok => 1 });
    
    cmp_ok( @r5b, '==', 0, "entered by '!' found no records" );
    
    my @r5c = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_modified_by=!&show=ent,entname",
				"modified by '!'");
    
    foreach my $r ( @r5c )
    {
	$tc->flag('invalid', $r->{nam}) if $r->{mdf} || $r->{mdi};
    }
    
    $tc->ok_all("modified by '!' finds records with no modifier");
    
    # Check 'authent_by' using the person who authorized the most records.
    
    my @r6a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_authent_by=$ati_max&show=ent,entname",
				"authent by max");
    my @r6b = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_authent_by=!$ati_max&show=ent,entname",
				"not authent by max");
    
    cmp_ok( @r6a + @r6b, '==', @r1, "authent_by + not authent_by = all (ati max)" );
    
    foreach my $r ( @r6a )
    {
	$tc->flag('invalid', $r->{nam}) unless $r->{ati} eq $ati_max || $r->{eni} eq $ati_max;
    }
    
    $tc->ok_all("authent by max finds records with proper auth/ent identifier");
    
    foreach my $r ( @r6b )
    {
	$tc->flag('invalid', $r->{nam}) if $r->{ati} eq $ati_max || $r->{eni} eq $ati_max;
    }
    
    $tc->ok_all("not authent by max finds records with improper auth/ent identifier");
    
    # Then check to make sure that the two sets of record oids match up.
    
    my @r3x = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_entered_by=$ati_max&show=ent,entname",
				"entered by ati max");
    
    my %authent_oid = $T->extract_values( \@r6a, 'oid' );
    my %auth_by_oid = $T->extract_values( \@r2a, 'oid' );
    my %ent_by_oid = $T->extract_values( \@r3x, 'oid' );
    
    my %check_oid = ( %auth_by_oid, %ent_by_oid );
    
    is_deeply( \%authent_oid, \%check_oid, "authent_by matches authorized_by U entered_by (ati max)" );
    
    # Then do the same check using the person who entered the most records (but did not authorize
    # the most). 
    
    my @r6c = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_authent_by=$eni_max&show=ent,entname",
				"authent_by eni max");
    my @r6d = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_authent_by=!$eni_max&show=ent,entname",
				"not authent_by eni max");
    
    my @r2x = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_authorized_by=$eni_max&show=ent,entname",
				"authorized_by eni max");
    
    cmp_ok( @r6c + @r6d, '==', @r1, "authent_by + not authent_by = all (eni max)" );
    
    %authent_oid = $T->extract_values( \@r6c, 'oid' );
    %auth_by_oid = $T->extract_values( \@r3a, 'oid' );
    %ent_by_oid = $T->extract_values( \@r2x, 'oid' );
    
    %check_oid = ( %auth_by_oid, %ent_by_oid );
    
    is_deeply( \%authent_oid, \%check_oid, "authent_by matches authorized_by U entered_by (eni max)" );
    
    # Now do the same for touched_by, but a smaller number of tests.
    
    my @r7a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_touched_by=$ati_max&show=ent,entname",
				"touched_by ati max");
    my @r7b = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_touched_by=!$ati_max&show=ent,entname",
				"not touched_by ati max");
    
    cmp_ok( @r7a + @r7b, '==', @r1, "touched_by + not touched+by = all (ati max)" );
    
    my @r4x = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_modified_by=$ati_max&show=ent,entname",
				"modified_by ati max");
    
    my %touched_oid = $T->extract_values( \@r7a, 'oid' );
    %auth_by_oid = $T->extract_values( \@r2a, 'oid' );
    %ent_by_oid = $T->extract_values( \@r3x, 'oid' );
    my %mod_by_oid = $T->extract_values( \@r4x, 'oid' );
    
    %check_oid = ( %auth_by_oid, %ent_by_oid, %mod_by_oid );
    
    is_deeply( \%touched_oid, \%check_oid, "touched_by matches auth U entered U mod (ati max)" );
    
    my @r7c = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_touched_by=$mdi_max&show=ent,entname",
				"touched_by mdi max");
    my @r7d = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_touched_by=!$mdi_max&show=ent,entname",
				"not touched_by mdi max");
    
    cmp_ok( @r7c + @r7d, '==', @r1, "touched_by + not touched+by = all (mdi max)" );    
    
    # Then we check two different parameters together.  We can't possibly test all combinations,
    # but we can at least check one.
    
    my @r10a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_authorized_by=$ati_max&" .
				 "taxa_modified_by=!$mdi_max&show=ent,entname", "auth_by and not mod_by");
    
    my %combo_oid = $T->extract_values( \@r10a, 'oid' );
    %check_oid = $T->extract_values( \@r2a, 'oid' );
    %mod_by_oid = $T->extract_values( \@r4c, 'oid' );
    # my %no_mod_oid = $T->extract_values( \@r5c, 'oid' );
    
    # subtract %mod_by_oid from %check_oid, then test.
    
    delete $check_oid{$_} foreach keys %mod_by_oid; # , keys %no_mod_oid;
    
    $T->cmp_sets_ok( \%combo_oid, '==', \%check_oid, "auth_by and not mod_by returns proper records" );
    
    # Then we try a parameter with multiple values.
    
    my @r11a = $T->fetch_records("/taxa/list.json?base_name=$NAME_1&taxa_entered_by=$ati_max,$eni_max&" .
				 "show=ent,entname", "entered_by multiple");
    
    my %eni_count = $T->count_values( \@r11a, 'eni' );
    
    cmp_ok( $eni_count{$ati_max} + $eni_count{$eni_max}, '==', @r11a, 
	    "entered_by multiple gets records with proper eni" );
    cmp_ok( $eni_count{$ati_max}, '>', 0, "entered_by multiple gets at least one entered by ati_max" );
    cmp_ok( $eni_count{$eni_max}, '>', 0, "entered_by multiple gets at least one entered by eni_max" );
};

# Now we test all of the output blocks that haven't been tested above, to make
# sure they produce the proper results under the compact vocabulary.

subtest 'output blocks 1 com' => sub {
    
    my $NAME_1 = 'Marsupialia';
    my $OID_1 = 'txn:39937';
    
    # We have already checked common, parent, immparent, class, classext, crmod, ent, entname above.
    
    # First check basic fields + app, size, subcounts, seq
    
    my @r1a = $T->fetch_records("/taxa/list.json?base_id=$OID_1&show=app,size,subcounts,seq",
				"output blocks 1 json");
    
    unless ( @r1a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my (%record, $base_flag);
    my $tc = Test::Conditions->new;
    
    foreach my $r ( @r1a )
    {
	if ( $r->{oid} && $r->{oid} =~ /^txn:\d+$/ ) 
	{ 
	    $record{$r->{oid}} = $r;
	    
	    $base_flag = $r->{flg} if $r->{oid} eq $OID_1;
	}
	
	else
	{
	    $tc->flag('oid', $r->{nam});
	}
	
	$tc->flag('typ', $r->{nam}) unless defined $r->{typ} && $r->{typ} eq 'txn';
	$tc->flag('rnk', $r->{nam}) unless defined $r->{rnk} && $r->{rnk} =~ /^\d+$/;
	$tc->flag('noc', $r->{nam}) unless defined $r->{noc} && $r->{noc} =~ /^\d+$/;
	$tc->flag('rid', $r->{nam}) unless defined $r->{rid} && $r->{rid} =~ /^ref:\d+$/;
	$tc->flag('ext', $r->{nam}) unless ! defined $r->{ext} || $r->{ext} =~ /^[01]$/;
	$tc->flag('flg', $r->{nam}) if defined $r->{flg} && $r->{flg} =~ /[BVE]/ && $r->{oid} ne $OID_1;
    }
    
    $tc->ok_all("all records have proper values for basic fields");
    
    ok( $base_flag && $base_flag =~ /B/, "base record has flag 'B'" );
    
    # Now, for each record, compare its field values to those of its parent.  Ignore the base
    # record, and compare synonyms of the base to the base (using the 'acc' field).  Ignore this
    # test if we have one or more records with improper oids, which will have alreayd been flagged
    # and is much more severe.
    
    unless ( $tc->is_set('oid') )
    {
	foreach my $r ( @r1a )
	{
	    next if $r->{oid} eq $OID_1;
	    my $p = $record{$r->{par}} ? $record{$r->{par}} : $record{$r->{acc}};
	    
	    # Check sequence numbers against the parent record
	    
	    $tc->flag('lsq', $r->{nam}) unless $r->{lsq} > $p->{lsq};
	    $tc->flag('rsq', $r->{nam}) unless $r->{rsq} <= $p->{rsq};
	    
	    # Check subcounts for proper values and against the parent record
	    
	    if ( $r->{rnk} > 3 )
	    {
		$tc->flag('spc', $r->{nam}) unless defined $r->{spc} && $r->{spc} =~ /^\d+$/;
		    # && defined $p->{spc} && $r->{spc} <= $p->{spc};
		
		if ( $r->{rnk} > 5 )
		{   
		    $tc->flag('gnc', $r->{nam}) unless defined $r->{gnc} && $r->{gnc} =~ /^\d+$/;
			# && defined $p->{gnc} && $r->{gnc} <= $p->{gnc};
		}
		
		else
		{
		    $tc->flag('gnc', $r->{nam}) if defined $r->{gnc} && ! $r->{tdf};
		}
	    }
	    
	    else
	    {
		$tc->flag('gnc', $r->{nam}) if defined $r->{gnc};
		$tc->flag('spc', $r->{nam}) if defined $r->{spc};
	    }
	    
	    # Check size and extsize for proper values and against the parent record
	    
	    $tc->flag('siz', $r->{nam}) unless defined $r->{siz} && $r->{siz} =~ /^\d+$/;
		# && defined $p->{siz} && $r->{siz} <= $p->{siz};
	    $tc->flag('exs', $r->{nam}) unless defined $r->{exs} && $r->{exs} =~ /^\d+$/;
	        # && defined $p->{exs} && $r->{exs} <= $p->{exs};
	    
	    # $tc->flag('exs', $r->{nam}) if $r->{ext} && $r->{exs} eq '0' && ! $r->{tdf};
	    # $tc->flag('exs', $r->{nam}) if defined $r->{ext} && $r->{ext} eq '0' && $r->{exs} ne '0';
	    
	    # Check first and last appearances for proper values and against the parent record,
	    # unless n_occs is zero in which case there should be no values for those fields.
	    
	    if ( $r->{noc} )
	    {
		# $tc->flag('noc', $r->{nam}) unless defined $p->{noc} && $p->{noc} >= $r->{noc};
		
		$tc->flag('fea', $r->{nam}) unless defined $r->{fea} && $r->{fea} =~ /^\d+(?:[.]\d+)?$/;
		    # && defined $p->{fea} && $p->{fea} >= $r->{fea};
		$tc->flag('fla', $r->{nam}) unless defined $r->{fla} && $r->{fla} =~ /^\d+(?:[.]\d+)?$/;
		    # && defined $r->{fea} && $r->{fea} > $r->{fla};
		
		$tc->flag('lla', $r->{nam}) unless defined $r->{lla} && $r->{lla} =~ /^\d+(?:[.]\d+)?$/;
		    # && defined $p->{lla} && $p->{lla} <= $r->{lla};
		$tc->flag('lea', $r->{nam}) unless defined $r->{lea} && $r->{lea} =~ /^\d+(?:[.]\d+)?$/;
		    # && defined $r->{lla} && $r->{lea} > $r->{lla};
	    }
	    
	    else
	    {
		# $tc->flag('fea', $r->{nam}) if defined $r->{fea} && $r->{fea} ne '';
		# $tc->flag('fla', $r->{nam}) if defined $r->{fea} && $r->{fla} ne '';
		# $tc->flag('lea', $r->{nam}) if defined $r->{fea} && $r->{lea} ne '';
		# $tc->flag('lla', $r->{nam}) if defined $r->{fea} && $r->{lla} ne '';
	    }
	}

	$tc->ok_all("all records have proper value for blocks 'app', 'size', 'subcounts', 'seq'" );
    }
};

# AAA


# Now test the rest of the output blocks to make sure they produce the proper
# results under the compact vocabulary.

subtest 'output blocks 2 json' => sub {
    
    my $NAME_1 = 'Mammalia';
    my $OID_1 = 'txn:36651';
    
    # Test 'attr,img,ref,refattr'
    
    my @r2a = $T->fetch_records("/taxa/list.json?base_id=$OID_1&show=img,attr,ref,refattr",
				"output blocks 2 json");
    
    unless ( @r2a )
    {
	diag("skipping remainder of subtest");
	return;
    }
    
    my $tc = Test::Conditions->new;
    $tc->set_limit( DEFAULT => 10, att => 1000 );
    
    foreach my $r ( @r2a )
    {
	$tc->flag('img', $r->{nam}) unless $r->{img} && $r->{img} =~ /^php:\d+$/;
	
	$tc->flag('att', $r->{nam}) unless $r->{att} && $r->{att} =~ /\w\w/ && $r->{att} =~ /\d\d\d\d/;
	
	$tc->flag('pby', $r->{nam}) unless $r->{pby} && $r->{pby} =~ /^\d\d\d\d$/;
	
	$tc->flag('aut', $r->{nam}) unless $r->{aut} && $r->{aut} =~ /\w\w/;
	
	$tc->flag('ref', $r->{nam}) unless $r->{ref} && $r->{ref} =~ /\w\w\w\w/ && $r->{ref} =~ /\d\d\d\d/;
    }

    $tc->ok_all("all records have proper value for 'img', 'attr', 'ref', 'refattr'");
};

__END__


# Then test taxon references, both json and ris formats.

subtest 'list refs json' => sub {

    my $response = $T->fetch_url("/data1.2/taxa/refs.json?base_name=$TEST_NAME_7",
				 "list refs json request OK") || return;
    
    my %found = $T->scan_records($response, 'al1', "list refs json extract records");
    
    return if $found{NO_RECORDS};
    
    ok($T->found_all(\%found, @TEST_AUTHOR_7a), "list refs json found a sample of records");
};


subtest 'list refs ris' => sub {

    my $response = $T->fetch_url("/data1.2/taxa/refs.ris?base_name=$TEST_NAME_7&datainfo",
				 "list refs ris request OK") || return;
    
    my $body = $response->content;
    
    ok($body =~ qr{^UR  - http://.+/data1.2/taxa/refs.ris\?base_name=$TEST_NAME_7&datainfo}m,
       "list refs ris has datainfo UR line");
    ok($body =~ qr{^KW  - base_name = $TEST_NAME_7}m, "list refs ris has datasource KW line");
    ok($body =~ qr{^T2  - $TEST_TITLE_7a}m, "list refs ris found at least one of the proper records");
};


subtest 'auto json' => sub {

    my $TEST_AUTO_1 = 'cani';
    my @TEST_AUTO_1a = ("Caniformia", "canine");
    my $TEST_AUTO_2 = 't.rex';
    my @TEST_AUTO_2a = ("Tyrannosaurus rex", "Telmatornis rex");
    
    my $cani = $T->fetch_url("/data1.2/taxa/auto.json?name=$TEST_AUTO_1&limit=10",
			     "auto json '$TEST_AUTO_1' request OK") || return;
    
    my %found = $T->scan_records($cani, 'nam', "auto json extract records");
    
    return if $found{NO_RECORDS};
    
    ok($T->found_all(\%found, @TEST_AUTO_1a), "auto json found a sample of records");
    
    my $trex = $T->fetch_url("/data1.2/taxa/auto.json?name=$TEST_AUTO_2&limit=10",
			     "auto json $TEST_AUTO_2' request OK") || return;
    
    %found = $T->scan_records($trex, 'nam', "auto json extract records");
    
    return if $found{NO_RECORDS};
    
    ok($T->found_all(\%found, @TEST_AUTO_2a), "auto json found a sample of records");
};


subtest 'images' => sub {
    
    my $thumb = $T->fetch_url("/data1.2/taxa/thumb.png?id=$TEST_IMAGE_1",
			      "image thumb request OK") || return;
    
    my $thumb_length = length($thumb->content) || 0;
    
    cmp_ok($thumb_length, '==', $TEST_IMAGE_SIZE_1a, 'image thumb size');
    
    my $icon = $T->fetch_url("/data1.2/taxa/icon.png?id=910",
			     "image icon request OK") || return;
    
    my $icon_length = length($icon->content) || 0;
    
    cmp_ok($icon_length, '==', $TEST_IMAGE_SIZE_1b, 'image icon size');
};

